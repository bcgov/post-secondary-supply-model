# Create APPSO CIP Records — dplyr Translation
# Original: R/02a-appso-programs.R
#
# Pipeline context:
#   APPSO (Apprentice outcomes) represent apprenticeship credentials reported by
#   institutions. Their CIP codes are institution-reported and may not conform to the
#   standard INFOWARE taxonomy. This script cleans those CIP codes and matches them
#   to the official taxonomy so they can be used in downstream program/occupation matching.
#
#   This is one of four program matching scripts that each produce a CIP-matched ID table:
#     02a-dacso-program-matching  → Credential_Non_Dup_Programs_DACSO_FinalCIPs
#     02a-bgs-program-matching    → Credential_Non_Dup_BGS_IDs
#     (GRAD matching)             → Credential_Non_Dup_GRAD_IDs
#     02a-appso-programs          → Credential_Non_Dup_APPSO_IDs  ← THIS SCRIPT
#   All four are consumed by 02a-update-cred-non-dup to populate final CIP codes.
#
# Input tables:
#   - credential_non_dup — main credential table (from 01b-credential-preprocessing)
#   - INFOWARE_L_CIP_6/4/2DIGITS_CIP2016 — official CIP taxonomy lookups
#
# Output table:
#   - Credential_Non_Dup_APPSO_IDs — consumed by 02a-update-cred-non-dup (Step 5)

library(arrow)
library(tidyverse)
library(dbplyr)
library(odbc)
library(DBI)

# ---- Configure LAN Paths and DB Connection -----
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

# Helper: reference a table in the user's schema
sch_tbl <- function(name) {
  tbl(con, dbplyr::in_schema(my_schema, name))
}

# ---- Check Required Tables ----
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."credential_non_dup"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_2DIGITS_CIP2016"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_4DIGITS_CIP2016"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_6DIGITS_CIP2016"')))

# ==============================================================================
# START QUERIES — Clean APPSO CIP Codes
# ==============================================================================

# Pull reference data from INFOWARE lookup tables.
# These define the official CIP hierarchy: 6-digit → 4-digit → 2-digit.
cip6 <- sch_tbl("INFOWARE_L_CIP_6DIGITS_CIP2016") %>%
  select(LCIP_CD_WITH_PERIOD, LCIP_LCP4_CD, LCIP_LCP2_CD) %>%
  collect() |>
  rename_with(toupper)

cip4 <- sch_tbl("INFOWARE_L_CIP_4DIGITS_CIP2016") %>%
  select(LCP4_CD, LCP4_CIP_4DIGITS_NAME) %>%
  collect() |>
  rename_with(toupper)

cip2 <- sch_tbl("INFOWARE_L_CIP_2DIGITS_CIP2016") %>%
  select(LCP2_CD, LCP2_DIGITS_NAME) %>%
  collect() |>
  rename_with(toupper)

# We need a distinct list of APPSO CIP codes to clean, rather than processing
# every individual credential record. Grouping by CIP + outcome type lets us clean
# each unique CIP code once and then join the results back to all matching records.
# ---- Step 1: Create cleaning table from APPSO records ----

cred_non_dup <- sch_tbl("credential_non_dup") %>% collect()
cred_non_dup <- cred_non_dup |>
  rename_with(toupper) # Ensure column names are uppercase for consistency with original SQL

appso_cleaning <- cred_non_dup %>%
  filter(OUTCOMES_CRED == "APPSO") %>%
  count(PSI_CREDENTIAL_CIP, OUTCOMES_CRED, name = "Expr1") %>%
  mutate(
    # Save original CIP before cleaning so we can join back later (Step 5)
    PSI_CREDENTIAL_CIP_orig = PSI_CREDENTIAL_CIP,
    # Initialize columns that will be filled by the matching steps below
    STP_CIP_CODE_4 = NA_character_,
    STP_CIP_CODE_4_NAME = NA_character_,
    STP_CIP_CODE_2 = NA_character_,
    STP_CIP_CODE_2_NAME = NA_character_
  )

# Some institutions report CIP codes without the standard 7-character format
# (e.g., missing leading zeros or trailing digits). These won't match the INFOWARE
# lookup tables unless we normalize them first.
# ---- Step 2: Fix CIP codes with wrong length ----

appso_cleaning <- appso_cleaning %>%
  mutate(
    PSI_CREDENTIAL_CIP = case_when(
      nchar(PSI_CREDENTIAL_CIP) == 6 &
        !grepl("\\.", substr(PSI_CREDENTIAL_CIP, 1, 2)) ~ paste0(
        PSI_CREDENTIAL_CIP,
        "0"
      ),
      TRUE ~ PSI_CREDENTIAL_CIP
    ),
    PSI_CREDENTIAL_CIP = case_when(
      nchar(PSI_CREDENTIAL_CIP) == 6 ~ paste0("0", PSI_CREDENTIAL_CIP),
      TRUE ~ PSI_CREDENTIAL_CIP
    )
  )

# Each credential needs both a 4-digit and 2-digit CIP code for downstream
# program and occupation matching (scripts 06 and 07). The INFOWARE tables define
# the official hierarchy. We try progressively shorter matches because some
# institution CIPs don't have an exact 6-digit match in the taxonomy.
# ---- Step 3: Match CIP codes to INFOWARE taxonomy ----

# Step 3a: Exact match on full 6-digit CIP → gives us both 4-digit and 2-digit
appso_cleaning <- appso_cleaning %>%
  left_join(cip6, by = c("PSI_CREDENTIAL_CIP" = "LCIP_CD_WITH_PERIOD")) %>%
  mutate(
    STP_CIP_CODE_4 = coalesce(LCIP_LCP4_CD, STP_CIP_CODE_4),
    STP_CIP_CODE_2 = coalesce(LCIP_LCP2_CD, STP_CIP_CODE_2)
  ) %>%
  select(-LCIP_LCP4_CD, -LCIP_LCP2_CD)

# Step 3b: Partial match on first 5 digits for CIPs that didn't match exactly.
# Some valid CIPs differ only in the last digit from a known code.
appso_cleaning <- appso_cleaning %>%
  mutate(PSI_CIP_5 = substr(PSI_CREDENTIAL_CIP, 1, 5)) %>%
  left_join(
    cip6 %>%
      mutate(PSI_CIP_5 = substr(LCIP_CD_WITH_PERIOD, 1, 5)) %>%
      filter(!duplicated(PSI_CIP_5)),
    by = "PSI_CIP_5"
  ) %>%
  mutate(
    STP_CIP_CODE_4 = coalesce(STP_CIP_CODE_4, LCIP_LCP4_CD),
    STP_CIP_CODE_2 = coalesce(STP_CIP_CODE_2, LCIP_LCP2_CD)
  ) %>%
  select(-PSI_CIP_5, -LCIP_CD_WITH_PERIOD, -LCIP_LCP4_CD, -LCIP_LCP2_CD)

# Step 3c: General programs (e.g., "Computer Science" XX.00) → default to XX.01.
# WHY: Some CIP families have a "general" code (XX.00) that doesn't exist in INFOWARE.
# We map these to the first specific sub-category (XX.01) as a reasonable default.
general_programs <- c(
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

appso_cleaning <- appso_cleaning %>%
  mutate(
    STP_CIP_CODE_4 = case_when(
      substr(PSI_CREDENTIAL_CIP, 1, 5) %in%
        general_programs &
        is.na(STP_CIP_CODE_4) ~ paste0(substr(PSI_CREDENTIAL_CIP, 1, 2), "01"),
      TRUE ~ STP_CIP_CODE_4
    )
  )

# Step 3d: Fall back to first 2 digits for any still-unmatched 2-digit CIP codes.
appso_cleaning <- appso_cleaning %>%
  mutate(PSI_CIP_2 = substr(PSI_CREDENTIAL_CIP, 1, 2)) %>%
  left_join(
    cip6 %>%
      mutate(PSI_CIP_2 = substr(LCIP_CD_WITH_PERIOD, 1, 2)) %>%
      filter(!duplicated(PSI_CIP_2)),
    by = "PSI_CIP_2"
  ) %>%
  mutate(STP_CIP_CODE_2 = coalesce(STP_CIP_CODE_2, LCIP_LCP2_CD)) %>%
  select(-PSI_CIP_2, -LCIP_CD_WITH_PERIOD, -LCIP_LCP4_CD, -LCIP_LCP2_CD)

# The final output needs both CIP codes and their names for reporting and for
# analysts to verify the matches are sensible. These names come from the INFOWARE
# lookup tables at the 4-digit and 2-digit levels.
# ---- Step 4: Add human-readable CIP names ----

# 4-digit CIP names
appso_cleaning <- appso_cleaning %>%
  left_join(cip4, by = c("STP_CIP_CODE_4" = "LCP4_CD")) %>%
  mutate(
    STP_CIP_CODE_4_NAME = coalesce(LCP4_CIP_4DIGITS_NAME, STP_CIP_CODE_4_NAME)
  ) %>%
  select(-LCP4_CIP_4DIGITS_NAME)

# 2-digit CIP names
appso_cleaning <- appso_cleaning %>%
  left_join(cip2, by = c("STP_CIP_CODE_2" = "LCP2_CD")) %>%
  mutate(
    STP_CIP_CODE_2_NAME = coalesce(LCP2_DIGITS_NAME, STP_CIP_CODE_2_NAME)
  ) %>%
  select(-LCP2_DIGITS_NAME)

# Flag unmatched 4-digit CIPs so analysts can investigate
appso_cleaning <- appso_cleaning %>%
  mutate(
    STP_CIP_CODE_4_NAME = ifelse(
      is.na(STP_CIP_CODE_4_NAME),
      "Invalid 4-digit CIP",
      STP_CIP_CODE_4_NAME
    )
  )

# Write intermediate cleaning table (kept for debugging; dropped at end of script)
dbWriteTable(
  con,
  "Credential_Non_Dup_STP_APPSO_Cleaning",
  appso_cleaning,
  overwrite = TRUE
)

# This is the final output consumed by 02a-update-cred-non-dup (Step 5).
# It joins the cleaned CIP codes back to the original credential records, giving
# each APPSO credential a standardized 4-digit and 2-digit CIP code. Join on the
# original (pre-cleaning) CIP code and OUTCOMES_CRED to match the cleaned results
# back to the right credential records.
# ---- Step 5: Create APPSO IDs table ----

appso_ids <- cred_non_dup %>%
  filter(OUTCOMES_CRED == "APPSO") %>%
  select(
    ID,
    PSI_CODE,
    PSI_PROGRAM_CODE,
    PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    PSI_CREDENTIAL_CIP,
    PSI_AWARD_SCHOOL_YEAR,
    OUTCOMES_CRED
  ) %>%
  inner_join(
    appso_cleaning %>%
      select(
        PSI_CREDENTIAL_CIP_orig,
        OUTCOMES_CRED,
        STP_CIP_CODE_4,
        STP_CIP_CODE_4_NAME,
        STP_CIP_CODE_2,
        STP_CIP_CODE_2_NAME
      ),
    by = c("PSI_CREDENTIAL_CIP" = "PSI_CREDENTIAL_CIP_orig", "OUTCOMES_CRED")
  ) %>%
  transmute(
    ID,
    PSI_CODE,
    PSI_PROGRAM_CODE = ifelse(
      PSI_PROGRAM_CODE == "(Unspecified)",
      NA_character_,
      PSI_PROGRAM_CODE
    ),
    PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    PSI_CREDENTIAL_CIP,
    PSI_AWARD_SCHOOL_YEAR,
    OUTCOMES_CRED,
    FINAL_CIP_CODE_4 = STP_CIP_CODE_4,
    FINAL_CIP_CODE_4_NAME = STP_CIP_CODE_4_NAME,
    FINAL_CIP_CODE_2 = STP_CIP_CODE_2,
    FINAL_CIP_CODE_2_NAME = STP_CIP_CODE_2_NAME
  )

dbWriteTable(con, "Credential_Non_Dup_APPSO_IDs", appso_ids, overwrite = TRUE)

# ---- Clean up ----
dbExecute(con, "DROP TABLE Credential_Non_Dup_STP_APPSO_Cleaning")
dbDisconnect(con)
