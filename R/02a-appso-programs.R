# =============================================================================
# R/02a-appso-programs.R
# =============================================================================
#
# WHAT:
# Creates APPSO (Apprenticeship) CIP records by mapping credential CIP codes
# from the STP (Student Tracking Program) to standardized 4-digit and 2-digit
# CIP codes using INFOWARE lookup tables.
#
# WHY:
# APPSO survey records contain raw CIP codes from institutions. These need to be
# standardized to enable aggregation across institutions and comparison with other
# surveys (BGS, DACSO, GRAD). The INFOWARE tables provide the mapping from raw
# codes to CIP classification hierarchy.
#
# HOW:
# 1. Collect APPSO records from credential_non_dup table
# 2. Clean CIP codes (add leading zeros, trailing zeros)
# 3. Progressive matching strategy:
#    - Exact match on full CIP code
#    - Fallback match on first 5 characters
#    - General program recode (e.g., "11.00" -> "11.01")
#    - Fallback match on first 2 characters
# 4. Add CIP names from lookup tables
# 5. Join back to credential records and save
#
# OUTPUT: Credential_Non_Dup_APPSO_IDs
#
# TODO:
# - [ ] Extract CIP cleaning logic into reusable function (shared with BGS/GRAD)
# - [ ] Parameterize the general_cips vector for easier maintenance
# - [ ] Add logging for match rates (how many matched at each step)
# - [ ] Consider using fuzzy matching for program name similarity

library(arrow)
library(tidyverse)
library(odbc)
library(DBI)
library(dbplyr)

# =============================================================================
# SECTION 1: CONFIGURATION AND CONNECTION
# =============================================================================
# WHAT: Establish database connection using config settings
#
# WHY: Need to read from and write to the decimal database
#
# HOW: Use config::get() for credentials, dbConnect() for connection

lan <- config::get("lan")
db_config <- config::get("decimal")
my_schema <- config::get("myschema")

con <- dbConnect(
  odbc(),
  Driver = db_config$driver,
  Server = db_config$server,
  Database = db_config$database,
  Trusted_Connection = "True"
)

# =============================================================================
# SECTION 2: TABLE REFERENCES
# =============================================================================
# WHAT: Define lazy database table references for source and lookup tables
#
# WHY: Using dbplyr's tbl() creates lazy references that defer execution
#
# HOW:
#   - credential_non_dup_tbl: Source of APPSO credential records
#   - cip_*_tbl: INFOWARE lookup tables for CIP classification

credential_non_dup_tbl <- tbl(con, in_schema(my_schema, "credential_non_dup"))
cip_6_tbl <- tbl(con, in_schema(my_schema, "INFOWARE_L_CIP_6DIGITS_CIP2016"))
cip_4_tbl <- tbl(con, in_schema(my_schema, "INFOWARE_L_CIP_4DIGITS_CIP2016"))
cip_2_tbl <- tbl(con, in_schema(my_schema, "INFOWARE_L_CIP_2DIGITS_CIP2016"))

# =============================================================================
# SECTION 3: CIP CLEANING AND PREPARATION
# =============================================================================
#
# WHAT:
# Extract APPSO records and clean the PSI_CREDENTIAL_CIP field.
# The raw codes may be missing leading zeros or trailing zeros.
#
# WHY:
# CIP codes should follow format "XX.XXXX" but institutions may submit
# "XXXXXX" or "XX.XXX0". We need consistent format for matching.
#
# HOW:
#   - Filter to OUTCOMES_CRED == "APPSO"
#   - Group by CIP code (not per-record) for efficiency
#   - Add leading zero if 6 chars and no period in first 2 positions
#   - Add trailing zero if 6 chars (assumes truncated)
#   - Create helper columns for 5-char and 2-char prefix matching

# Step 1: Collect and clean APPSO CIP codes
appso_cleaning <- credential_non_dup_tbl %>%
  filter(OUTCOMES_CRED == "APPSO") %>%
  group_by(PSI_CREDENTIAL_CIP, OUTCOMES_CRED) %>%
  summarize(Expr1 = n(), .groups = "drop") %>%
  mutate(PSI_CREDENTIAL_CIP_orig = PSI_CREDENTIAL_CIP) %>%
  mutate(
    PSI_CREDENTIAL_CIP = case_when(
      # "123456" -> "0123456" (add leading zero)
      nchar(PSI_CREDENTIAL_CIP) == 6 &
        !str_detect(substr(PSI_CREDENTIAL_CIP, 1, 2), "\\.") ~
        paste0(PSI_CREDENTIAL_CIP, "0"),
      # "12.345" -> "012.345" (add leading zero)
      nchar(PSI_CREDENTIAL_CIP) == 6 ~ paste0("0", PSI_CREDENTIAL_CIP),
      TRUE ~ PSI_CREDENTIAL_CIP
    )
  ) %>%
  mutate(
    PSI_CREDENTIAL_CIP_5 = substr(PSI_CREDENTIAL_CIP, 1, 5),
    PSI_CREDENTIAL_CIP_2 = substr(PSI_CREDENTIAL_CIP, 1, 2)
  )

# TODO: [MEDIUM] Add validation that cleaned CIPs are in valid format
# TODO: [LOW] Log how many records needed each type of cleaning

# =============================================================================
# SECTION 4: PROGRESSIVE CIP MATCHING
# =============================================================================
#
# WHAT:
# Match cleaned CIP codes to standardized 4-digit and 2-digit CIP codes
# using a progressive fallback strategy.
#
# WHY:
# Direct exact matching may fail for similar programs. The progressive
# strategy recovers more matches by relaxing constraints.
#
# HOW:
# Step a: Exact match on full 6-digit code with periods
# Step b: Match on first 5 characters (handles trailing digit variations)
# Step c: General program recode (e.g., "11.00" is a placeholder for "11.01")
# Step d: Match on first 2 characters for 2-digit code only

# Step 2a: Exact match on full CIP code
appso_cleaning <- appso_cleaning %>%
  left_join(
    cip_6_tbl %>% select(LCIP_CD_WITH_PERIOD, LCIP_LCP4_CD, LCIP_LCP2_CD),
    by = c("PSI_CREDENTIAL_CIP" = "LCIP_CD_WITH_PERIOD")
  ) %>%
  rename(STP_CIP_CODE_4 = LCIP_LCP4_CD, STP_CIP_CODE_2 = LCIP_LCP2_CD)

# Step 2b: Match on first 5 characters (fallback)
appso_cleaning <- appso_cleaning %>%
  left_join(
    cip_6_tbl %>%
      mutate(CIP_5 = substr(LCIP_CD_WITH_PERIOD, 1, 5)) %>%
      select(
        CIP_5,
        LCIP_LCP4_CD_alt = LCIP_LCP4_CD,
        LCIP_LCP2_CD_alt = LCIP_LCP2_CD
      ) %>%
      distinct(),
    by = c("PSI_CREDENTIAL_CIP_5" = "CIP_5")
  ) %>%
  mutate(
    STP_CIP_CODE_4 = coalesce(STP_CIP_CODE_4, LCIP_LCP4_CD_alt),
    STP_CIP_CODE_2 = coalesce(STP_CIP_CODE_2, LCIP_LCP2_CD_alt)
  ) %>%
  select(-LCIP_LCP4_CD_alt, -LCIP_LCP2_CD_alt)

# Step 2c: General program recode (e.g., 11.00 -> 11.01)
# These are placeholder codes that need standard assignment
general_cips <- c(
  "11.00",
  "13.00",
  "14.00",
  "19.00",
  "23.00",
  "24.00",
  "26.00",
  "40.00",
  "42.00",
  "45.00",
  "50.00",
  "52.00",
  "55.00"
)
# TODO: [LOW] Move general_cips to a config/lookup table for easier updates

appso_cleaning <- appso_cleaning %>%
  mutate(
    STP_CIP_CODE_4 = case_when(
      is.na(STP_CIP_CODE_4) & PSI_CREDENTIAL_CIP_5 %in% general_cips ~
        paste0(substr(PSI_CREDENTIAL_CIP, 1, 2), "01"),
      TRUE ~ STP_CIP_CODE_4
    )
  )

# Step 2d: Match on first 2 characters for 2-digit code (final fallback)
appso_cleaning <- appso_cleaning %>%
  left_join(
    cip_6_tbl %>%
      mutate(CIP_2 = substr(LCIP_CD_WITH_PERIOD, 1, 2)) %>%
      select(CIP_2, LCIP_LCP2_CD_alt2 = LCIP_LCP2_CD) %>%
      distinct(),
    by = c("PSI_CREDENTIAL_CIP_2" = "CIP_2")
  ) %>%
  mutate(
    STP_CIP_CODE_2 = coalesce(STP_CIP_CODE_2, LCIP_LCP2_CD_alt2)
  ) %>%
  select(-LCIP_LCP2_CD_alt2)

# =============================================================================
# SECTION 5: ADD CIP NAMES
# =============================================================================
#
# WHAT:
# Add human-readable names for the matched CIP codes
#
# WHY:
# The projection models use codes, but names are needed for reporting
# and validation
#
# HOW: Join with CIP name lookup tables

# Step 3: Add 4-digit CIP names
appso_cleaning <- appso_cleaning %>%
  left_join(
    cip_4_tbl %>% select(LCP4_CD, LCP4_CIP_4DIGITS_NAME),
    by = c("STP_CIP_CODE_4" = "LCP4_CD")
  ) %>%
  rename(STP_CIP_CODE_4_NAME = LCP4_CIP_4DIGITS_NAME)

# Step 4: Add 2-digit CIP names
appso_cleaning <- appso_cleaning %>%
  left_join(
    cip_2_tbl %>% select(LCP2_CD, LCP2_DIGITS_NAME),
    by = c("STP_CIP_CODE_2" = "LCP2_CD")
  ) %>%
  rename(STP_CIP_CODE_2_NAME = LCP2_DIGITS_NAME)

# Step 5: Handle invalid CIP names
appso_cleaning <- appso_cleaning %>%
  mutate(
    STP_CIP_CODE_4_NAME = if_else(
      is.na(STP_CIP_CODE_4_NAME),
      "Invalid 4-digit CIP",
      STP_CIP_CODE_4_NAME
    )
  )

# =============================================================================
# SECTION 6: FINAL JOIN AND OUTPUT
# =============================================================================
#
# WHAT:
# Join cleaned CIP data back to original credential records and save
#
# WHY:
# Need to update the credential records with standardized CIP codes
#
# HOW:
#   - Inner join on original CIP and credential type
#   - Select final output columns
#   - Handle "(Unspecified)" program codes
#   - Write to database

# Step 6: Join back to credentials and select final columns
final_appso_ids <- credential_non_dup_tbl %>%
  filter(OUTCOMES_CRED == "APPSO") %>%
  inner_join(
    appso_cleaning,
    by = c(
      "PSI_CREDENTIAL_CIP" = "PSI_CREDENTIAL_CIP_orig",
      "OUTCOMES_CRED" = "OUTCOMES_CRED"
    )
  ) %>%
  select(
    ID,
    PSI_CODE,
    PSI_PROGRAM_CODE,
    PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    PSI_CREDENTIAL_CIP,
    PSI_AWARD_SCHOOL_YEAR,
    OUTCOMES_CRED,
    FINAL_CIP_CODE_4 = STP_CIP_CODE_4,
    FINAL_CIP_CODE_4_NAME = STP_CIP_CODE_4_NAME,
    FINAL_CIP_CODE_2 = STP_CIP_CODE_2,
    FINAL_CIP_CODE_2_NAME = STP_CIP_CODE_2_NAME
  ) %>%
  mutate(
    PSI_PROGRAM_CODE = if_else(
      PSI_PROGRAM_CODE == "(Unspecified)",
      NA_character_,
      PSI_PROGRAM_CODE
    )
  )

# Step 7: Save to database
if (
  dbExistsTable(
    con,
    Id(schema = my_schema, table = "Credential_Non_Dup_APPSO_IDs")
  )
) {
  dbRemoveTable(
    con,
    Id(schema = my_schema, table = "Credential_Non_Dup_APPSO_IDs")
  )
}

final_appso_ids %>%
  compute(
    name = "Credential_Non_Dup_APPSO_IDs",
    temporary = FALSE,
    schema = my_schema
  )

# =============================================================================
# SECTION 7: CLEANUP
# =============================================================================
dbDisconnect(con)
