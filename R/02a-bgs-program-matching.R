# Copyright 2024 Province of British Columbia
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations under the License.

# ==============================================================================
# BGS program matching and CIP alignment
#
# Purpose:
# Align program CIP codes between Baccalaureate Graduate Survey (BGS) outcomes
# data and Student Transitions Project (STP) credential data.
#
# Why this script exists:
# BGS and STP often describe the same program differently and may assign different
# CIP codes. This script compares records at the student level and applies
# business rules to decide whether the final CIP should come from BGS or STP.
#
# High-level workflow:
# 1. Build the BGS outcomes table used for matching.
# 2. Standardize STP CIP codes to 4-digit and 2-digit forms.
# 3. Match BGS and STP records by PEN.
# 4. Apply automatic match rules for high-confidence cases.
# 5. Apply program-level decision rules for 2-digit matches.
# 6. Export borderline cases for manual review.
# 7. Update the STP credential table with final CIP values.
# 8. Update the BGS outcomes table with final CIP values.
#
# Key business rule:
# When BGS and STP can be confidently linked, use the source judged to have the
# most appropriate CIP for that program. When no confident match is available,
# keep the source-specific CIP and flag that no direct match was found.
#
# Main outputs:
# - T_BGS_Data_Final_for_OutcomesMatching
# - Credential_Non_Dup_BGS_IDs
# - Credential_Non_Dup_GRAD_IDs
# - BGS_Matching_STP_Credential_PEN
#
# Important:
# This script contains both automated matching and a manual review workflow.
# Read the comments in Part 3C before rerunning the manual review section.
# ==============================================================================
#

############################################################################
# ------------------------------------------------------------------------------
# Setup: connections and required lookup tables
#
# Purpose:
# Connect to the working database and confirm that all lookup and source tables
# needed by this script are available.
#
# Notes:
# - Oracle connection code is kept below as a one-time setup reference for
#   loading INFOWARE tables into Decimal.
# - In normal use, this script expects those tables to already exist in the
#   target schema.
# - If required tables are missing, stop early rather than failing later in the
#   matching process.
# ------------------------------------------------------------------------------

# oracle connection instruction
# Follow this solution: [\[oracle.com\]](https://www.oracle.com/database/technologies/releasenote-odbc-ic.html), [\[oracle.com\]](https://www.oracle.com/database/technologies/instant-client/winx64-64-downloads.html)

# 1.  Install **Oracle Instant Client 19c Basic** and **19c ODBC** into something like `C:\Oracle\instantclient_19_30`. [\[oracle.com\]](https://www.oracle.com/database/technologies/releasenote-odbc-ic.html), [\[oracle.com\]](https://www.oracle.com/database/technologies/instant-client/winx64-64-downloads.html)
# 2.  Add that folder to **PATH** and put it **before** old Oracle 11g folders. [\[oracle.com\]](https://www.oracle.com/database/technologies/instant-client/winx64-64-downloads.html)
# 3.  Set **`TNS_ADMIN`** to the folder containing your `tnsnames.ora`. [\[rdrr.io\]](https://rdrr.io/cran/DBI/man/dbBind.html), [\[stackoverflow.com\]](https://stackoverflow.com/questions/50750812/how-to-get-sid-service-name-and-port-for-oracle-database)
# 4.  Run **`odbc_install.exe`**. [\[oracle.com\]](https://www.oracle.com/database/technologies/releasenote-odbc-ic.html)
# 5.  Restart RStudio. [\[oracle.com\]](https://www.oracle.com/database/technologies/instant-client/winx64-64-downloads.html)
# 6.  Run `odbcListDrivers()` and copy the exact driver name into your R code. [\[docs.oracle.com\]](https://docs.oracle.com/en/database/oracle/oracle-database/23/odbcd/basic-programming-oracle-odbc.html), [\[quantargo.com\]](https://www.quantargo.com/help/r/latest/packages/DBI/html/transactions)
# ############################################################################

options(java.parameters = " -Xmx102400m") ## For reading oracle tables: increase amount of memory java is allowed to use

library(tidyverse)
library(RODBC)
library(odbc)
library(DBI)
library(glue)
library(RJDBC)
library(dbplyr)
library(futile.logger)

## -------------------------- Logging Setup -------------------------------------------------------
## -----------------------------------------------------------------------------------------------
log_file <- "./R/execution_log.txt"
flog.appender(appender.file(log_file), name = "file_logger")
flog.threshold(INFO, name = "file_logger")

log_info <- function(msg) {
  flog.info(msg, name = "file_logger")
  print(paste(Sys.time(), "|", msg))
}

log_info("==== 02a-bgs-program-matching.R START ====")

# ---- Configure LAN Paths and DB Connection -----
lan <- config::get("lan")
db_config <- config::get("decimal")
my_schema <- config::get("myschema")

# Connect to Decimal
con <- dbConnect(
  odbc(),
  Driver = db_config$driver,
  Server = db_config$server,
  Database = db_config$database,
  Trusted_Connection = "TRUE"
)
log_info("Connected to SQL Server database")


# ---- Read in INFOWARE tables ----
# Note: These tables should be loaded by 'R/load-infoware-lookups.R'
# source("R/load-infoware-lookups.R")
# We check for their existence and proceed.

required_tables <- c(
  "INFOWARE_BGS_DIST_19_23",
  "INFOWARE_BGS_DIST_18_22",
  "INFOWARE_BGS_COHORT_INFO",
  "INFOWARE_L_CIP_6DIGITS_CIP2016",
  "INFOWARE_L_CIP_4DIGITS_CIP2016",
  "INFOWARE_L_CIP_2DIGITS_CIP2016"
)

missing_tables <- required_tables[
  !map_lgl(
    required_tables,
    ~ dbExistsTable(con, Id(schema = my_schema, table = .x))
  )
]

if (length(missing_tables) > 0) {
  stop(glue::glue(
    "The following required tables are missing in schema '{my_schema}': {paste(missing_tables, collapse = ', ')}. Please run 'R/load-infoware-lookups.R' first."
  ))
}
log_info("All required INFOWARE tables present in database")

# ---- Table References ----
infoware_bgs_19_23 <- tbl(con, in_schema(my_schema, "INFOWARE_BGS_DIST_19_23"))
infoware_bgs_18_22 <- tbl(con, in_schema(my_schema, "INFOWARE_BGS_DIST_18_22"))
infoware_cohort_info <- tbl(
  con,
  in_schema(my_schema, "INFOWARE_BGS_COHORT_INFO")
)

cip_6_tbl <- tbl(con, in_schema(my_schema, "INFOWARE_L_CIP_6DIGITS_CIP2016"))
cip_4_tbl <- tbl(con, in_schema(my_schema, "INFOWARE_L_CIP_4DIGITS_CIP2016"))
cip_2_tbl <- tbl(con, in_schema(my_schema, "INFOWARE_L_CIP_2DIGITS_CIP2016"))

credential_non_dup_tbl <- tbl(con, in_schema(my_schema, "credential_non_dup"))
stp_credential_tbl <- tbl(con, in_schema(my_schema, "STP_Credential"))
log_info(
  "Loaded lazy table references: INFOWARE BGS/CIP tables, credential_non_dup, STP_Credential"
)

# # id should be unique for updates to be reliable.
# infoware_cohort_info |> tally()
# # 290758
# infoware_cohort_info %>%
#   count(PEN) %>%
#   filter(n > 1) %>%
#   tally()
# # NO
# infoware_cohort_info %>%
#   count(STUDID) %>%
#   filter(n > 1) %>%
#   tally()
# # NO
# infoware_cohort_info %>%
#   count(STQU_ID) %>%
#   filter(n > 1) %>%
#   tally()
# # YES
# credential_non_dup_tbl |>
#   count(psi_student_number) %>%
#   filter(n > 1) %>%
#   tally()
# credential_non_dup_tbl |>
#   count(id) %>%
#   filter(n > 1) %>%
#   tally()
# # yes
# stp_credential_tbl |>
#   count(PSI_STUDENT_NUMBER) %>%
#   filter(n > 1) %>%
#   tally()
# stp_credential_tbl |>
#   count(PSI_PEN) %>%
#   filter(n > 1) %>%
#   tally()
# stp_credential_tbl |>
#   count(ID) %>%
#   filter(n > 1) %>%
#   tally()
# # yes

# ------------------------------------------------------------------------------
# Part 1: Build BGS outcomes data used for matching
#
# Purpose:
# Create one combined BGS outcomes table covering the years used in the current
# matching cycle.
#
# Why this step exists:
# The BGS outcomes data is split across multiple INFOWARE delivery tables.
# We need one consistent table with common field names before matching to STP.
#
# Inputs:
# - INFOWARE_BGS_DIST_19_23
# - INFOWARE_BGS_DIST_18_22
# - INFOWARE_BGS_COHORT_INFO
#
# Output:
# - T_BGS_Data_Final_for_OutcomesMatching
#
# Notes:
# - The current script takes all years from the 2019–2023 table and only 2018
#   from the 2018–2022 table.
# - STQU_ID is the safest row-level key for later updates in this table.
# ------------------------------------------------------------------------------

# Step 1: 2020 Outcomes (from 19_23 table)
t_bgs_step1 <- infoware_bgs_19_23 %>%
  select(
    STQU_ID,
    RESPONDENT,
    YEAR,
    INSTITUTION_CODE,
    INSTITUTION
  ) |>
  inner_join(
    infoware_cohort_info |>
      select(
        PEN,
        STUDID,
        STQU_ID,
        SRV_Y_N,
        SUBM_CD,
        CIP2DIG,
        CIP2DIG_NAME,
        CIP4DIG,
        CIP_4DIGIT_NO_PERIOD,
        CIP4DIG_NAME,
        CIP_6DIGIT_1,
        CIP_6DIGIT_NO_PERIOD,
        CIP6DIG_NAME,
        PROGRAM,
        DASHBOARD_PROGRAM,
        CPC
      ),
    by = "STQU_ID" # unique id as key
  ) %>%
  select(
    PEN,
    STUDID,
    STQU_ID,
    SRV_Y_N,
    RESPONDENT,
    YEAR,
    INSTITUTION_CODE,
    INSTITUTION,
    SUBM_CD,
    CIP2DIG,
    CIP2DIG_NAME,
    CIP4DIG,
    CIP_4DIGIT_NO_PERIOD,
    CIP4DIG_NAME,
    CIP_6DIGIT_1,
    CIP_6DIGIT_NO_PERIOD,
    CIP6DIG_NAME,
    PROGRAM,
    DASHBOARD_PROGRAM,
    CPC
  )

# Step 2: 2018 Outcomes (from 18_22 table, filtered for Year 2018)
t_bgs_step2 <- infoware_bgs_18_22 %>%
  filter(YEAR == 2018) %>%
  select(
    STQU_ID,
    RESPONDENT,
    YEAR,
    INSTITUTION_CODE,
    INSTITUTION
  ) |>
  inner_join(
    infoware_cohort_info |>
      select(
        PEN,
        STUDID,
        STQU_ID,
        SRV_Y_N,
        SUBM_CD,
        CIP2DIG,
        CIP2DIG_NAME,
        CIP4DIG,
        CIP_4DIGIT_NO_PERIOD,
        CIP4DIG_NAME,
        CIP_6DIGIT_1,
        CIP_6DIGIT_NO_PERIOD,
        CIP6DIG_NAME,
        PROGRAM,
        DASHBOARD_PROGRAM,
        CPC
      ),
    by = "STQU_ID"
  ) %>%
  select(
    PEN,
    STUDID,
    STQU_ID,
    SRV_Y_N,
    RESPONDENT,
    YEAR,
    SUBM_CD,
    INSTITUTION_CODE,
    INSTITUTION,
    CIP2DIG,
    CIP2DIG_NAME,
    CIP4DIG,
    CIP_4DIGIT_NO_PERIOD,
    CIP4DIG_NAME,
    CIP_6DIGIT_1,
    CIP_6DIGIT_NO_PERIOD,
    CIP6DIG_NAME,
    PROGRAM,
    DASHBOARD_PROGRAM,
    CPC
  )

# Combine and Add PSSM_CREDENTIAL

t_bgs_final <- union_all(t_bgs_step1, t_bgs_step2) %>%
  mutate(PSSM_CREDENTIAL = "BACH")


t_bgs_final <- t_bgs_final %>%
  {
    if (
      dbExistsTable(
        con,
        Id(
          schema = my_schema,
          table = "T_BGS_Data_Final_for_OutcomesMatching_r"
        )
      )
    ) {
      dbRemoveTable(
        con,
        Id(
          schema = my_schema,
          table = "T_BGS_Data_Final_for_OutcomesMatching_r"
        )
      )
    }
    .
  } %>%
  compute(
    name = Id(
      schema = my_schema,
      table = "T_BGS_Data_Final_for_OutcomesMatching_r"
    ),
    temporary = FALSE
  )

# id should be unique for updates to be reliable.
t_bgs_final |> tally()
# 143811
log_info(glue::glue(
  "Part 1: Built T_BGS_Data_Final_for_OutcomesMatching_r: {t_bgs_final %>% tally() %>% pull()} rows"
))

# ------------------------------------------------------------------------------
# Part 2: Standardize STP CIP codes to match BGS structure
#
# Purpose:
# Convert STP credential CIPs into comparable 4-digit and 2-digit CIP codes so
# they can be matched to BGS records.
#
# Why this step exists:
# STP stores CIP values in 6-digit form with periods, while BGS matching is
# done mainly at the 4-digit and 2-digit level. We need a consistent structure
# before comparing the two sources.
#
# Output tables:
# - Credential_Non_Dup_STP_CIP4_Cleaning
# - Credential_Non_Dup_BGS_IDs
# - Credential_Non_Dup_GRAD_IDs
#
# Key rules:
# - First try an exact 6-digit lookup.
# - If that fails, try partial matching using the first 5 characters.
# - For selected "general program" CIPs, default the 4-digit code to the
#   corresponding general 01 category.
# - Derive 2-digit CIPs and descriptive labels from the official lookup tables.
# ------------------------------------------------------------------------------

# 1. Create cleaning table (collect STP BGS/GRAD data)
stp_cip_cleaning <- credential_non_dup_tbl |>
  rename_with(toupper) |>
  filter(OUTCOMES_CRED %in% c("BGS", "GRAD")) |>
  group_by(PSI_CREDENTIAL_CIP, OUTCOMES_CRED) |>
  summarize(Expr1 = n(), .groups = "drop") |>
  mutate(PSI_CREDENTIAL_CIP_orig = PSI_CREDENTIAL_CIP) |>
  mutate(
    CIP_5 = substr(PSI_CREDENTIAL_CIP, 1, 5),
    CIP_2 = substr(PSI_CREDENTIAL_CIP, 1, 2)
  )

# 2. Add 4 and 2D CIP codes from INFOWARE
# Match on full CIP
stp_cip_cleaning <- stp_cip_cleaning %>%
  left_join(
    cip_6_tbl %>% select(LCIP_CD_WITH_PERIOD, LCIP_LCP4_CD, LCIP_LCP2_CD),
    by = c("PSI_CREDENTIAL_CIP" = "LCIP_CD_WITH_PERIOD")
  ) %>%
  rename(STP_CIP_CODE_4 = LCIP_LCP4_CD, STP_CIP_CODE_2 = LCIP_LCP2_CD)

# Match on first 5 chars
stp_cip_cleaning <- stp_cip_cleaning %>%
  left_join(
    cip_6_tbl %>%
      mutate(CIP_5_lookup = substr(LCIP_CD_WITH_PERIOD, 1, 5)) %>%
      select(CIP_5_lookup, LCP4_alt = LCIP_LCP4_CD, LCP2_alt = LCIP_LCP2_CD) %>%
      distinct(),
    by = c("CIP_5" = "CIP_5_lookup")
  ) %>%
  mutate(
    STP_CIP_CODE_4 = coalesce(STP_CIP_CODE_4, LCP4_alt),
    STP_CIP_CODE_2 = coalesce(STP_CIP_CODE_2, LCP2_alt)
  ) %>%
  select(-LCP4_alt, -LCP2_alt)

## New: Add 4D CIP codes for general programs (if 00 change to 01)
## Check which CIPs have general programs here: https://www.statcan.gc.ca/en/subjects/standard/cip/2021/index
# Recode general programs
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
stp_cip_cleaning <- stp_cip_cleaning %>%
  mutate(
    STP_CIP_CODE_4 = case_when(
      is.na(STP_CIP_CODE_4) & CIP_5 %in% general_cips ~ paste0(CIP_2, "01"),
      TRUE ~ STP_CIP_CODE_4
    )
  )

# Match on first 2 digits for 2D code
stp_cip_cleaning <- stp_cip_cleaning %>%
  left_join(
    cip_6_tbl %>%
      mutate(CIP_2_lookup = substr(LCIP_CD_WITH_PERIOD, 1, 2)) %>%
      select(CIP_2_lookup, LCP2_alt2 = LCIP_LCP2_CD) %>%
      distinct(),
    by = c("CIP_2" = "CIP_2_lookup")
  ) %>%
  mutate(
    STP_CIP_CODE_2 = coalesce(STP_CIP_CODE_2, LCP2_alt2)
  ) %>%
  select(-LCP2_alt2)

# 3. Add 4D and 2D CIP names
stp_cip_cleaning <- stp_cip_cleaning %>%
  left_join(
    cip_4_tbl %>% select(LCP4_CD, LCP4_CIP_4DIGITS_NAME),
    by = c("STP_CIP_CODE_4" = "LCP4_CD")
  ) %>%
  left_join(
    cip_2_tbl %>% select(LCP2_CD, LCP2_DIGITS_NAME),
    by = c("STP_CIP_CODE_2" = "LCP2_CD")
  ) %>%
  rename(
    STP_CIP_CODE_4_NAME = LCP4_CIP_4DIGITS_NAME,
    STP_CIP_CODE_2_NAME = LCP2_DIGITS_NAME
  ) %>%
  mutate(
    STP_CIP_CODE_4_NAME = coalesce(STP_CIP_CODE_4_NAME, "Invalid 4-digit CIP")
  )

stp_cip_cleaning <- stp_cip_cleaning %>%
  {
    if (
      dbExistsTable(
        con,
        Id(schema = my_schema, table = "Credential_Non_Dup_STP_CIP4_Cleaning_r")
      )
    ) {
      dbRemoveTable(
        con,
        Id(schema = my_schema, table = "Credential_Non_Dup_STP_CIP4_Cleaning_r")
      )
    }
    .
  } %>%
  compute(
    name = Id(
      schema = my_schema,
      table = "Credential_Non_Dup_STP_CIP4_Cleaning_r"
    ),
    temporary = FALSE
  )


stp_cip_cleaning |> tally()
# 681
log_info(glue::glue(
  "Part 2: Materialized Credential_Non_Dup_STP_CIP4_Cleaning_r: {stp_cip_cleaning %>% tally() %>% pull()} rows"
))

# ---- Part 2 (continued): Create BGS and GRAD credential ID tables ----
# WHAT: Splits STP credential data into two separate tables: one for BGS credentials (which will undergo
#       matching to BGS survey outcomes), and one for GRAD credentials (which use STP CIPs directly).
# WHY: BGS and GRAD credentials follow different downstream processing paths. BGS credentials need to be
#      matched to survey outcomes to determine whether to use BGS or STP CIP codes. GRAD credentials
#      are finalized immediately with STP CIP codes (no BGS survey matching available).
# HOW: 1) Join cleaned STP CIP codes (from stp_cip_cleaning) back to credential base data
#      2) Filter for BGS vs GRAD credential types
#      3) For BGS: add PSI_PEN from STP_Credential table for later PEN-based matching to survey data
#      4) For GRAD: finalize CIP columns immediately (STP CIP becomes FINAL_CIP) and later used in 02a-update-cred-non-dup.R
#      5) Materialize both tables as persistent SQL tables for downstream use
#

# Create base table: join cleaned CIP codes to credential data
# This combines the 4D and 2D normalized STP CIP codes with institution, program, and award year info
stp_cip_ids <- credential_non_dup_tbl %>%
  rename_with(toupper) |>
  filter(OUTCOMES_CRED %in% c("BGS", "GRAD")) %>%
  select(
    ID,
    PSI_CODE,
    PSI_PROGRAM_CODE,
    PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    PSI_CREDENTIAL_CIP,
    PSI_AWARD_SCHOOL_YEAR,
    OUTCOMES_CRED
  ) |>
  inner_join(
    stp_cip_cleaning |> # with cleaned CIP codes in STP credential table
      select(
        PSI_CREDENTIAL_CIP_orig,
        OUTCOMES_CRED,
        STP_CIP_CODE_4,
        STP_CIP_CODE_4_NAME,
        STP_CIP_CODE_2,
        STP_CIP_CODE_2_NAME
      ),
    by = c("PSI_CREDENTIAL_CIP" = "PSI_CREDENTIAL_CIP_orig", "OUTCOMES_CRED")
  )


# ---- BGS Credentials: Credential_Non_Dup_BGS_IDs ----
# Create a table with only BGS credentials and normalized STP CIP codes (485925 rows in 2023)
# These records will later be matched to BGS survey outcomes (T_BGS_Data_Final_for_OutcomesMatching)
# using PEN as the primary key. The matching process determines whether to use BGS or STP CIP codes.

bgs_ids_base <- stp_cip_ids %>%
  filter(OUTCOMES_CRED == "BGS") %>%
  select(
    ID,
    PSI_CODE,
    PSI_PROGRAM_CODE,
    PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    PSI_CREDENTIAL_CIP,
    PSI_AWARD_SCHOOL_YEAR,
    OUTCOMES_CRED,
    STP_CIP_CODE_4,
    STP_CIP_CODE_4_NAME,
    STP_CIP_CODE_2,
    STP_CIP_CODE_2_NAME
  ) %>%
  # Clean up NULL program codes that were imported as "(Unspecified)"
  mutate(
    PSI_PROGRAM_CODE = if_else(
      PSI_PROGRAM_CODE == "(Unspecified)",
      NA_character_,
      PSI_PROGRAM_CODE
    )
  )

bgs_ids_base |> tally() # verify count matches expected from documentation
# 485925 matching the number of records in 2023 according to the documentation
log_info(glue::glue(
  "Part 2: BGS credentials base (bgs_ids_base): {bgs_ids_base %>% tally() %>% pull()} rows"
))

# Add PSI_PEN (Personal Education Number) from STP_Credential table
# PSI_PEN is the linking key between STP credentials and BGS survey outcomes.
# This join brings in the PEN identifier needed for Part 3 (matching BGS survey data to credentials)
credential_bgs_ids <- bgs_ids_base %>%
  left_join(stp_credential_tbl %>% select(ID, PSI_PEN), by = "ID")


credential_bgs_ids <- credential_bgs_ids %>%
  # Materialize as persistent table for use in later steps (Part 3: matching to BGS outcomes)
  {
    if (
      dbExistsTable(
        con,
        Id(schema = my_schema, table = "Credential_Non_Dup_BGS_IDs_r")
      )
    ) {
      dbRemoveTable(
        con,
        Id(schema = my_schema, table = "Credential_Non_Dup_BGS_IDs_r")
      )
    }
    .
  } %>%
  compute(
    name = Id(schema = my_schema, table = "Credential_Non_Dup_BGS_IDs_r"),
    temporary = FALSE
  )
log_info("Part 2: Materialized Credential_Non_Dup_BGS_IDs_r to SQL Server")


# ---- GRAD Credentials: Credential_Non_Dup_GRAD_IDs ----
# Create a table with only GRAD credentials and finalized CIP codes (133844 rows in 2023)
# GRAD credentials are finalized immediately with STP CIP codes as the final CIP.
# No further matching to survey outcomes is needed (no BGS survey data exists for GRAD).
# These records bypass the matching logic and proceed directly to supply modeling.

# TODO [LOW]:
# Confirm whether this table is used by downstream supply modelling.

credential_grad_ids <- stp_cip_ids %>%
  filter(OUTCOMES_CRED == "GRAD") %>%
  select(
    ID,
    PSI_CODE,
    PSI_PROGRAM_CODE,
    PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    PSI_CREDENTIAL_CIP,
    PSI_AWARD_SCHOOL_YEAR,
    OUTCOMES_CRED,
    # Immediately finalize GRAD CIPs to STP values (no BGS survey matching available)
    FINAL_CIP_CODE_4 = STP_CIP_CODE_4,
    FINAL_CIP_CODE_4_NAME = STP_CIP_CODE_4_NAME,
    FINAL_CIP_CODE_2 = STP_CIP_CODE_2,
    FINAL_CIP_CODE_2_NAME = STP_CIP_CODE_2_NAME
  )


credential_grad_ids <- credential_grad_ids %>%
  # Materialize as persistent table for use in later supply modeling steps
  {
    if (
      dbExistsTable(
        con,
        Id(schema = my_schema, table = "Credential_Non_Dup_GRAD_IDs_r")
      )
    ) {
      dbRemoveTable(
        con,
        Id(schema = my_schema, table = "Credential_Non_Dup_GRAD_IDs_r")
      )
    }
    .
  } %>%
  compute(
    name = Id(schema = my_schema, table = "Credential_Non_Dup_GRAD_IDs_r"),
    temporary = FALSE
  )

credential_grad_ids |> tally() # verify count matches expected from documentation
# 133844 matching the number of records in 2023 according to the documentation
# later used in the 02a-update-cred-non-dup.R script
log_info(glue::glue(
  "Part 2: Materialized Credential_Non_Dup_GRAD_IDs_r: {credential_grad_ids %>% tally() %>% pull()} rows"
))

## check Credential_Non_Dup_BGS_IDs_r for (Unspecified) - when Credential_Non_Dup loaded NULLs changed to (Unspecified)
# {

#   ## 2023: only PSI_PROGRAM_CODE had (Unspecified) - replace with NULLs, which is already done.
#   # dbGetQuery(
#   #   con,
#   #   "
#   #            Update Credential_Non_Dup_BGS_IDs_r
#   #            SET PSI_PROGRAM_CODE = NULL
#   #            WHERE PSI_PROGRAM_CODE = '(Unspecified)'"
#   # )

#   tbl(con, "Credential_Non_Dup_BGS_IDs_r") %>%
#     filter(is.na(PSI_PROGRAM_CODE)) %>%
#     tally()
#   # still 364449 rows with null PSI_PROGRAM_CODE which are converted from '(Unspecified)'?

#   rm(chk)
# }

# ------------------------------------------------------------------------------
# Part 3: Match BGS survey outcomes to STP credentials
#
# Purpose:
# Link BGS survey records to STP credential records and decide whether the final
# CIP should come from BGS or STP.
#
# Why this step exists:
# BGS and STP may refer to the same graduate and program but assign different
# CIP codes. This part of the script applies the matching rules that determine
# which source should be trusted for each linked case.
#
# Main stages:
# - Part 3A: Create the case-level crosswalk using PEN.
# - Part 3B: Auto-resolve exact and near-exact matches using rules.
# - Part 3C: Export remaining borderline matches for manual review.
# - Part 3D: Fill in final names and cluster values after the CIP choice is made.
#
# Important:
# The PEN join is expected to be many-to-many. A student may have more than one
# credential or more than one survey-related record across the matching period.
# Do not assume PEN, ID, or STQU_ID is unique in the crosswalk table unless a
# specific validation step confirms the correct key for that update.
# ------------------------------------------------------------------------------

### Part 3A: Initial XWALK ----

# Verify PSI_PEN column exists in STP credentials table (added in Part 2)
colnames(tbl(con, "Credential_Non_Dup_BGS_IDs_r"))

## Create BGS_Matching_STP_Credential_PEN by performing inner join on PEN
## This table combines BGS survey outcomes with matched STP credentials.
## Each row represents a potential match between a BGS survey record and a STP credential.
## Many-to-many relationships are expected (e.g., student with multiple credentials or survey years).

# id should be unique for updates to be reliable.

# 1. Match BGS and STP on PEN (Personal Education Number)
bgs_matching <- t_bgs_final %>%
  # Filter for valid PEN values (exclude blank, missing, or zero values)
  filter(PEN != "", !is.na(PEN), PEN != "0") %>%
  # Inner join: only keep records where PEN exists in both tables
  inner_join(
    credential_bgs_ids,
    by = c("PEN" = "PSI_PEN") # many to many relationship: BGS STQU_ID and STP Credential ID could appear multiple times in this table.
  ) %>%
  # Select and rename columns for clarity in downstream processing
  select(
    STQU_ID, # BGS survey record ID
    ID, # STP credential record ID
    PEN, # Personal Education Number (linking key)
    OUTCOMES_CRED, # Credential type (should be "BGS" for this table)
    INSTITUTION_CODE, # BGS institution code
    PSI_CODE, # STP institution code
    YEAR, # BGS survey year
    PSI_AWARD_SCHOOL_YEAR, # STP credential award year
    BGS_FINAL_CIP_CODE_4 = CIP_4DIGIT_NO_PERIOD, # BGS 4-digit CIP (no periods)
    BGS_FINAL_CIP_CODE_4_NAME = CIP4DIG_NAME, # BGS 4-digit CIP description
    STP_FINAL_CIP_CODE_4 = STP_CIP_CODE_4, # STP normalized 4-digit CIP
    STP_FINAL_CIP_CODE_4_NAME = STP_CIP_CODE_4_NAME, # STP 4-digit CIP description
    BGS_FINAL_CIP_CODE_2 = CIP2DIG, # BGS 2-digit CIP
    BGS_FINAL_CIP_CODE_2_NAME = CIP2DIG_NAME, # BGS 2-digit CIP description
    STP_FINAL_CIP_CODE_2 = STP_CIP_CODE_2, # STP normalized 2-digit CIP
    STP_FINAL_CIP_CODE_2_NAME = STP_CIP_CODE_2_NAME, # STP 2-digit CIP description
    BGS_PROGRAM_CODE = CPC, # BGS program code (College Program Code)
    BGS_PROGRAM_DESC = PROGRAM, # BGS program description
    STP_PROGRAM_CODE = PSI_PROGRAM_CODE, # STP program code
    STP_PROGRAM_DESC = PSI_CREDENTIAL_PROGRAM_DESCRIPTION # STP program description
  ) |>
  distinct()

# id should be unique for updates to be reliable.

# credential_bgs_ids |>
#   count(PSI_PEN) %>%
#   filter(n > 1) %>%
#   tally()
# # 21826
# t_bgs_final |>
#   count(PEN) %>%
#   filter(n > 1) %>%
#   tally()
# # 2904

# bgs_matching %>%
#   count(ID) %>%
#   filter(n > 1) %>%
#   tally()
# #  5303

# bgs_matching %>%
#   count(PEN) %>%
#   filter(n > 1) %>%
#   tally()
# # 9537
# bgs_matching %>%
#   count(STQU_ID) %>%
#   filter(n > 1) %>%
#   tally()
# # 11511

bgs_matching %>%
  count(STQU_ID, ID) %>% # STQU from survey, ID from STP credential
  filter(n > 1) %>%
  tally()
# 0

# t_bgs_final %>%
#     filter(PEN == "110832342")
# # two survey years
# credential_bgs_ids |>
#   filter(PSI_PEN == "110832342")
# # three credentials, two bachelor, one doctor

# bgs_matching |>
#   filter(PEN == "110832342")
# two rows with different BGS CIPs but the same STP CIPs. One of the BGS CIP is the same as the STP CIP.
# after inner join, total six rows for it, after distinct still SIX rows for it

########################################################################################################
# So actuall only three or two rows are really needed. Many dulicated rows are needed to remove.
#########################################################################################################

# bgs_matching |>
#   group_by(ID) |>
#   mutate(n = n()) |>
#   filter(n > 1) |>
#   glimpse()
#

# STQU_ID, ID should be unique

## Validate: Check row count matches expected from documentation
{
  # Expected count: inner join of BGS records with valid PEN to STP BGS credentials
  tbl(con, "T_BGS_Data_Final_for_OutcomesMatching_r") %>%
    select(STQU_ID, PEN) %>%
    filter(!is.na(PEN) & PEN != "" & PEN != "0") %>%
    inner_join(
      tbl(con, "Credential_Non_Dup_BGS_IDs_r") %>% select(ID, PSI_PEN),
      by = c("PEN" = "PSI_PEN")
    ) %>%
    tally() # Expected: 133,952 (2023 data)

  bgs_matching %>% tally() # Verify actual matches expected count: the same
}
log_info(glue::glue(
  "Part 3A: Created bgs_matching (PEN join): {bgs_matching %>% tally() %>% pull()} rows"
))


### Part 3B: Auto matching using flags ----
### Apply business logic to flag matches on institution, award year, and CIP codes

## Add flag columns to track matching criteria
## These flags will be used to determine which CIP source (BGS or STP) is most reliable.

# 2. Add Match Flags
# Define Institution Match Logic:
# Some institutions have different codes in BGS vs STP. This logic handles known aliases.
# For example: CAPU (BGS) = CAP (STP), UBCO/UBCV (BGS) = UBC (STP)
bgs_matching_flagged <- bgs_matching %>%
  mutate(
    # MATCH_INST: Flag records where BGS and STP institution codes align or match known aliases
    MATCH_INST = case_when(
      PSI_CODE == INSTITUTION_CODE ~ "Yes",
      PSI_CODE %in%
        c("CAPU", "CAP") &
        INSTITUTION_CODE %in% c("CAP", "CAPU") ~ "Yes",
      PSI_CODE == "DOUG" & INSTITUTION_CODE == "DGL" ~ "Yes",
      PSI_CODE == "UCC" & INSTITUTION_CODE == "TRU" ~ "Yes",
      PSI_CODE %in%
        c("ECIAD", "ECU") &
        INSTITUTION_CODE %in% c("ECU", "ECUAD", "ECIAD") ~
        "Yes",
      PSI_CODE %in% c("KWAN", "KPU") & INSTITUTION_CODE %in% c("KPU", "KWN") ~
        "Yes",
      PSI_CODE %in% c("MALA", "MAL") & INSTITUTION_CODE %in% c("VIU", "MAL") ~
        "Yes",
      PSI_CODE %in%
        c("OUC", "OKAN") &
        INSTITUTION_CODE %in% c("OKAN", "OKN", "OUC") ~
        "Yes",
      PSI_CODE == "OLA" & INSTITUTION_CODE == "TRUOL" ~ "Yes",
      PSI_CODE %in%
        c("UCFV", "UFV") &
        INSTITUTION_CODE %in% c("UFV", "FVAL", "UCFV") ~
        "Yes",
      PSI_CODE %in% c("UBCO", "UBCV") & INSTITUTION_CODE == "UBC" ~ "Yes",
      TRUE ~ NA_character_
    )
  )

bgs_matching_flagged <- bgs_matching_flagged |>
  mutate(
    # MATCH_AWARD_SCHOOL_YEAR: Flag records where BGS and STP award years align within 2-year lag
    ## BGS cohort is surveyed 2 years after STP credential award, so a 2-year lag is expected.
    ## Example: BGS 2023 survey should match STP 2020/2021 or 2021/2022 award years.
    ## This case_when must be updated annually as new survey years are added. [YSC11.1][AL11.2]
    MATCH_AWARD_SCHOOL_YEAR = case_when(
      (YEAR == 2000 & PSI_AWARD_SCHOOL_YEAR %in% c("1997/1998", "1998/1999")) |
        (YEAR == 2002 &
          PSI_AWARD_SCHOOL_YEAR %in% c("1999/2000", "2000/2001")) |
        (YEAR == 2004 &
          PSI_AWARD_SCHOOL_YEAR %in% c("2001/2002", "2002/2003")) |
        (YEAR == 2006 &
          PSI_AWARD_SCHOOL_YEAR %in% c("2003/2004", "2004/2005")) |
        (YEAR == 2008 &
          PSI_AWARD_SCHOOL_YEAR %in% c("2005/2006", "2006/2007")) |
        (YEAR == 2009 &
          PSI_AWARD_SCHOOL_YEAR %in% c("2006/2007", "2007/2008")) |
        (YEAR == 2010 &
          PSI_AWARD_SCHOOL_YEAR %in% c("2007/2008", "2008/2009")) |
        (YEAR == 2011 &
          PSI_AWARD_SCHOOL_YEAR %in% c("2008/2009", "2009/2010")) |
        (YEAR == 2012 &
          PSI_AWARD_SCHOOL_YEAR %in% c("2009/2010", "2010/2011")) |
        (YEAR == 2013 &
          PSI_AWARD_SCHOOL_YEAR %in% c("2010/2011", "2011/2012")) |
        (YEAR == 2014 &
          PSI_AWARD_SCHOOL_YEAR %in% c("2011/2012", "2012/2013")) |
        (YEAR == 2015 &
          PSI_AWARD_SCHOOL_YEAR %in% c("2012/2013", "2013/2014")) |
        (YEAR == 2016 &
          PSI_AWARD_SCHOOL_YEAR %in% c("2013/2014", "2014/2015")) |
        (YEAR == 2017 &
          PSI_AWARD_SCHOOL_YEAR %in% c("2014/2015", "2015/2016")) |
        (YEAR == 2018 &
          PSI_AWARD_SCHOOL_YEAR %in% c("2015/2016", "2016/2017")) |
        (YEAR == 2019 &
          PSI_AWARD_SCHOOL_YEAR %in% c("2016/2017", "2017/2018")) |
        (YEAR == 2020 &
          PSI_AWARD_SCHOOL_YEAR %in% c("2017/2018", "2018/2019")) |
        (YEAR == 2021 &
          PSI_AWARD_SCHOOL_YEAR %in% c("2018/2019", "2019/2020")) |
        (YEAR == 2022 &
          PSI_AWARD_SCHOOL_YEAR %in% c("2019/2020", "2020/2021")) |
        (YEAR == 2023 &
          PSI_AWARD_SCHOOL_YEAR %in% c("2020/2021", "2021/2022")) ~
        "Yes",
      TRUE ~ NA_character_
    )
  )


bgs_matching_flagged <- bgs_matching_flagged |>
  mutate(
    # Match_CIP_CODE_4: Flag records where BGS and STP 4-digit CIPs are identical
    ## Exact match on specific program classification = high confidence
    Match_CIP_CODE_4 = if_else(
      BGS_FINAL_CIP_CODE_4 == STP_FINAL_CIP_CODE_4,
      "Yes",
      NA_character_
    ),

    # Match_CIP_CODE_2: Flag records where BGS and STP 2-digit CIPs are identical
    ## Match on broader program category (less specific than 4-digit)
    ## Used as fallback when 4-digit codes differ but broad categories align
    Match_CIP_CODE_2 = if_else(
      BGS_FINAL_CIP_CODE_2 == STP_FINAL_CIP_CODE_2,
      "Yes",
      NA_character_
    )
  )

bgs_matching_flagged <- bgs_matching_flagged |>
  ## Create compound flags combining multiple match criteria
  ## These determine the "confidence level" of each match.
  mutate(
    # MATCH_ALL_3_CIP4_FLAG: Highest confidence match
    ## Requires: institution match AND award year match AND 4-digit CIP match
    ## These records need no further review - use BGS CIP (which equals STP CIP for these matches)
    MATCH_ALL_3_CIP4_FLAG = if_else(
      Match_CIP_CODE_4 == "Yes" &
        MATCH_AWARD_SCHOOL_YEAR == "Yes" &
        MATCH_INST == "Yes",
      "Yes",
      NA_character_
    ),

    # MATCH_ALL_3_CIP2_FLAG: Medium confidence match
    ## Requires: institution match AND award year match AND 2-digit CIP match
    ## These records matched on broader program category but 4-digit codes differ
    ## Require manual review or secondary logic to decide which CIP source to use (Part 3B extended)
    MATCH_ALL_3_CIP2_FLAG = if_else(
      Match_CIP_CODE_2 == "Yes" &
        MATCH_AWARD_SCHOOL_YEAR == "Yes" &
        MATCH_INST == "Yes",
      "Yes",
      NA_character_
    )
  )


## Initialize Final CIP columns based on high-confidence matches
## Records not flagged here will be processed in Parts 3B extended/3C

# Set FINAL_CIP values for highest-confidence matches (MATCH_ALL_3_CIP4_FLAG = "Yes")
## For these records, BGS CIP = STP CIP so use BGS as final (no CIP change needed)
bgs_matching_flagged <- bgs_matching_flagged |>
  mutate(
    FINAL_CIP_CODE_4 = if_else(
      MATCH_ALL_3_CIP4_FLAG == "Yes",
      BGS_FINAL_CIP_CODE_4,
      NA_character_
    ),
    FINAL_CIP_CODE_2 = if_else(
      MATCH_ALL_3_CIP4_FLAG == "Yes", # Align with 4-digit logic for consistency
      BGS_FINAL_CIP_CODE_2,
      NA_character_
    ),
    # Track which CIP source was selected for this record
    ## "Yes" = BGS CIP was selected, "No" = STP CIP will be selected (in Parts 3B/3C)
    USE_BGS_CIP = if_else(MATCH_ALL_3_CIP4_FLAG == "Yes", "Yes", NA_character_),
    # Flag records that have been finalized with high confidence (no manual review needed)
    FINAL_CONSIDER_A_MATCH = if_else(
      MATCH_ALL_3_CIP4_FLAG == "Yes",
      "Yes",
      NA_character_
    )
  )

bgs_matching_flagged <- bgs_matching_flagged |>
  mutate(
    # Placeholder for records finalized through manual review in Part 3C
    # The reason: dbplyr translates NA_character_ → SQL NULL (no type). SQL Server then defaults untyped NULL to int. Using sql("CAST(NULL AS VARCHAR(255))") forces the database to treat it as a character column.
    FINAL_CIP_CODE_4_NAME = sql("CAST(NULL AS VARCHAR(255))"),
    FINAL_CIP_CODE_2_NAME = sql("CAST(NULL AS VARCHAR(255))"),
    FINAL_CIP_CLUSTER_CODE = sql("CAST(NULL AS VARCHAR(255))"),
    FINAL_CIP_CLUSTER_NAME = sql("CAST(NULL AS VARCHAR(255))"),
    FINAL_PROBABLE_MATCH = sql("CAST(NULL AS VARCHAR(255))")
  )

bgs_matching_flagged |> tally() # Verify row count: 133,952 (2023)
bgs_matching_flagged |> glimpse() # Review structure

## Materialize as persistent SQL table for use in Parts 3B extended/3C
{
  if (
    dbExistsTable(
      con,
      Id(schema = my_schema, table = "BGS_Matching_STP_Credential_PEN_r")
    )
  ) {
    dbRemoveTable(
      con,
      Id(schema = my_schema, table = "BGS_Matching_STP_Credential_PEN_r")
    )
  }
}

bgs_matching_flagged <- bgs_matching_flagged |>
  compute(
    name = Id(schema = my_schema, table = "BGS_Matching_STP_Credential_PEN_r"),
    temporary = FALSE
  )
log_info(glue::glue(
  "Part 3B: Materialized BGS_Matching_STP_Credential_PEN_r with match flags: {bgs_matching_flagged %>% tally() %>% pull()} rows"
))

# bgs_matching_flagged |> tally() # Verify: 133,952 (2023)
# bgs_matching_flagged |> glimpse() # Review structure

# id should be unique for updates to be reliable.

# bgs_matching_flagged %>%
#   count(ID) %>%
#   filter(n > 1) %>%
#   tally()

bgs_matching_flagged |>
  filter(PEN == "110832342")
# still six rows, but only need three rows or one row.
# who has two surveys after undergrad, and three credentials. two bachelors and one doctor.

## Validation: Check for any new institution codes that need mapping
## If you see unmatched PSI_CODEs/INSTITUTION_CODE pairs, add them to the MATCH_INST case_when above
{
  # Find all institution codes that successfully matched
  table <- bgs_matching_flagged %>%
    select(PSI_CODE, INSTITUTION_CODE, MATCH_INST)

  # Get codes that have at least one match (these are working)
  codes <- table %>%
    filter(!is.na(MATCH_INST)) %>%
    distinct(PSI_CODE, INSTITUTION_CODE) %>%
    collect()

  # Show any PSI codes without any successful matches (potential new aliases needed)
  table %>%
    filter(is.na(MATCH_INST) & !PSI_CODE %in% codes$PSI_CODE) %>%
    count(PSI_CODE, INSTITUTION_CODE) %>%
    arrange(PSI_CODE) %>%
    collect()

  rm(table, codes)
}

## Summary statistics: Review match flag distributions
## These counts help validate the matching logic and identify if adjustments are needed
{
  # Institution match rates
  bgs_matching_flagged %>%
    group_by(MATCH_INST) %>%
    tally()

  # Award year match rates (should be high due to structured logic)
  bgs_matching_flagged %>%
    group_by(MATCH_AWARD_SCHOOL_YEAR) %>%
    tally()

  # 4-digit CIP match rates (0 = all codes differ, 1 = some match)
  bgs_matching_flagged %>%
    group_by(MATCH_ALL_3_CIP4_FLAG) %>%
    tally()
  # 105910, but in the documentation 2023, it is 107135. The reason could be we did the distinct operation at the end of the join in line 858

  # 2-digit CIP match rates (broader category matches)
  bgs_matching_flagged %>%
    group_by(MATCH_ALL_3_CIP2_FLAG) %>%
    tally()
  # 114690, but in the documentation 2023, it is 115755

  ## Detailed breakdown: 4-digit CIP matches by institution
  ## Shows which institutions have high/low match rates (potential problem areas)
  table <- bgs_matching_flagged %>%
    filter(MATCH_INST == "Yes" & MATCH_AWARD_SCHOOL_YEAR == "Yes") %>%
    group_by(INSTITUTION_CODE)

  # Summary: matched vs unmatched record counts and match percentage by institution
  # Institutions with low match percentages may indicate CIP coding alignment issues
  # INSTITUTION_CODE is STP institution code

  chk <- table %>%
    # Count records that matched on all three criteria (high confidence)
    filter(!is.na(MATCH_ALL_3_CIP4_FLAG)) %>%
    tally() %>%
    # Count records that matched on institution/year but NOT 4-digit CIP (need manual review)
    full_join(
      table %>%
        filter(is.na(MATCH_ALL_3_CIP4_FLAG)) %>%
        tally(),
      by = "INSTITUTION_CODE",
      suffix = c("_matched", "_unmatched")
    ) %>%
    # Total record count per institution
    full_join(
      table %>% tally(),
      by = "INSTITUTION_CODE"
    ) %>%
    # Calculate percentage of records requiring further review
    mutate(perc_unmatched = n_unmatched * 100 / n) %>%
    collect() %>%
    # Sort by unmatched percentage to identify institutions needing attention
    arrange(desc(perc_unmatched))

  rm(table, chk)
}
# many unmatched rows

# ------------------------------------------------------------------------------
# Part 3B plus: Apply automatic decision rules; only in R from 2023 onwards
#
#  notes from 2023 documentation:
#  •	qry_Check_BGS_Match_All3_CIP2  [AL13.1][AL13.2]to look at the  records that matched on institution, year and 2-digit CIP (but did not match on 4-digit CIP). Confirm these programs look like matches.
# •	Choose which CIP to use for CIP2 matches. In 2023, rather than using all STP CIPs followed the following steps:
# o	If the BGS CIP is in a “general program” (the 4-digit CIP category is the 2-digit CIP category followed by “general”) – then use the STP CIP
# o	If the STP CIP is in a “general program” – then use the BGS CIP
# o	Then using the 4-digit matches, if one of the 2-digit program matches can be linked to the 4-digit matches on STP institution, program, and CIP – then use the STP CIP
# o	If on of the 2-digit program matches can be linked to the 4-digit matches on BGS institution, program and CIP – then use the BGS CIP
# o	Some additional custom CIP choices were made
# o	The remaining were mostly double majors – used STP CIP for these
# o	In 2023,resulted in 1477 programs using STP and 82 using BGS – or – 8405 records using STP and 235 using BGS
# •	Update the FINAL CIP columns for the 2-digit matches – R code provided.

# Purpose:
# Resolve as many matched BGS/STP pairs as possible without manual review.
#
# Decision order:
# 1. Exact institution + year + 4-digit CIP match:
#    Treat as high confidence and keep the aligned CIP.
# 2. Institution + year + 2-digit CIP match:
#    Apply program-level rules to decide whether BGS or STP is more reliable.
# 3. Remaining institution/year matches with differing CIPs:
#    Send to manual review in Part 3C.
#
# Notes:
# - Institution aliases are handled here because the two sources do not always
#   use the same institution codes.
# - This section reduces the number of records that require manual review.
# ------------------------------------------------------------------------------

# ---- Step 1: Prepare 2-digit CIP match candidates for algorithmic review ----
# Aggregate all 2-digit CIP matches (institution + year + 2D CIP match, but no 4D match)
# into unique program combinations for systematic decision-making.
# This reduces 133,952 individual records to ~1,600 unique program decision points.

## t1: Aggregate 2-digit CIP matches for program-level decision making
## Filters to records where institution + year + 2D CIP match, but 4D CIP differs
## Groups by all relevant program and CIP identifiers to create unique decision points
## This reduces 133,952 individual records to ~1,600 unique program combinations
## that require algorithmic or manual review to determine which CIP source to use.
## The resulting table serves as reference data for Steps 2A/2B cross-validation logic.

t1 <- bgs_matching_flagged |>
  rename_with(toupper) |>
  group_by(
    INSTITUTION_CODE,
    PSI_CODE,
    YEAR,
    PSI_AWARD_SCHOOL_YEAR,
    BGS_PROGRAM_CODE,
    STP_PROGRAM_CODE,
    BGS_PROGRAM_DESC,
    STP_PROGRAM_DESC,
    BGS_FINAL_CIP_CODE_4,
    BGS_FINAL_CIP_CODE_4_NAME,
    STP_FINAL_CIP_CODE_4,
    STP_FINAL_CIP_CODE_4_NAME,
    BGS_FINAL_CIP_CODE_2,
    BGS_FINAL_CIP_CODE_2_NAME,
    STP_FINAL_CIP_CODE_2,
    STP_FINAL_CIP_CODE_2_NAME,
    MATCH_INST,
    MATCH_AWARD_SCHOOL_YEAR,
    MATCH_CIP_CODE_4,
    MATCH_ALL_3_CIP4_FLAG,
    MATCH_CIP_CODE_2,
    MATCH_ALL_3_CIP2_FLAG
  ) |>
  summarise(
    Expr1 = n(),
    .groups = "drop"
  ) |>
  filter(
    is.na(MATCH_ALL_3_CIP4_FLAG), # Exclude 4-digit exact matches (already high-confidence)
    MATCH_ALL_3_CIP2_FLAG == "Yes" # Include only 2-digit CIP matches (broader program category)
  )

t1 |> tally() # Should be ~1,593 unique combinations (varies by year)
log_info(glue::glue(
  "Part 3B+: Aggregated 2-digit CIP match candidates (t1): {t1 %>% tally() %>% pull()} unique combinations"
))

## t2: Aggregated view of 2-digit CIP matches for program-level decision making
## Groups individual records by institution, programs, and CIP codes to create
## unique decision points (program pairs where only 2-digit CIPs match)
t2 <- bgs_matching_flagged %>%
  rename_with(toupper) |>
  filter(MATCH_ALL_3_CIP2_FLAG == "Yes" & is.na(MATCH_ALL_3_CIP4_FLAG)) %>%
  group_by(
    INSTITUTION_CODE,
    PSI_CODE,
    YEAR,
    PSI_AWARD_SCHOOL_YEAR,
    BGS_PROGRAM_CODE,
    STP_PROGRAM_CODE,
    BGS_PROGRAM_DESC,
    STP_PROGRAM_DESC,
    BGS_FINAL_CIP_CODE_4,
    BGS_FINAL_CIP_CODE_4_NAME,
    STP_FINAL_CIP_CODE_4,
    STP_FINAL_CIP_CODE_4_NAME,
    MATCH_ALL_3_CIP2_FLAG
  ) %>%
  summarize(Expr1 = n(), .groups = "drop") %>%
  collect()

t2 |> tally() # ~1593 unique program decision points
log_info(glue::glue(
  "Part 3B+: Collected t2 program decision points: {nrow(t2)} unique combinations"
))

# ---- Step 2: Apply multi-step decision tree to assign CIP_TO_USE ----
# This decision tree prioritizes data quality and consistency:
# - Prefer more specific programs over generic programs
# - Leverage 4-digit exact matches as evidence of reliable coding
# - Use custom rules for known program mapping issues
# - Default to STP for remaining cases (historical consistency)

## Decision Step 1: General Program Logic
## Some CIP codes represent "general" programs (e.g., 1101 = General Agriculture).
## These are less specific than their counterparts. If one source uses a general code,
## prefer the more specific code from the other source. This assumes the more specific
## code better describes the student's actual program of study.
## Note: This logic removes many unassigned cases from ~1,600 to ~300 needing further review.
matched_2d_cips <- t2 %>%
  mutate(
    CIP_TO_USE = case_when(
      # BGS uses a general program - defer to STP's more specific code
      BGS_FINAL_CIP_CODE_4 %in%
        c(
          "1101", # General Agriculture
          "1301", # General Engineering
          "1401", # General Engineering-related fields
          "1901", # General Family and Consumer Sciences
          "2301", # General Biological Sciences
          "2401", # General Biophysics
          "2601", # General Health Sciences
          "4001", # General Public Administration
          "4201", # General Social Sciences
          "4501", # General Area/Ethnic/Cultural Studies
          "5001", # General Humanities
          "5201", # General Visual/Performing Arts
          "5501" # General Business/Commerce
        ) ~ "STP",
      # STP uses a general program - defer to BGS's more specific code
      STP_FINAL_CIP_CODE_4 %in%
        c(
          "1101",
          "1301",
          "1401",
          "1901",
          "2301",
          "2401",
          "2601",
          "4001",
          "4201",
          "4501",
          "5001",
          "5201",
          "5501"
        ) ~ "BGS" # ? even BGS_CIP is na, we still choose BGS over STP here. Why?
    )
  )


matched_2d_cips |> tally() # ~1,600 rows total
count(matched_2d_cips, CIP_TO_USE) # ~1,300 resolved to BGS/STP, ~300 still unassigned (NA)
# most STP due to the previous statement, small set of BGS, and some are NAs.
#   CIP_TO_USE     n
#   <chr>      <int>
# 1 BGS           73
# 2 STP         1218
# 3 NA           302

chk_cips_review1 <- t2 %>%
  inner_join(
    matched_2d_cips %>%
      select(
        INSTITUTION_CODE,
        PSI_CODE,
        YEAR,
        PSI_AWARD_SCHOOL_YEAR,
        BGS_PROGRAM_CODE,
        BGS_PROGRAM_DESC,
        STP_PROGRAM_CODE,
        STP_PROGRAM_DESC,
        BGS_FINAL_CIP_CODE_4_NAME,
        STP_FINAL_CIP_CODE_4_NAME,
        CIP_TO_USE
      ) %>%
      filter(!is.na(CIP_TO_USE))
  )

chk_cips_review2 <- matched_2d_cips %>% filter(is.na(CIP_TO_USE))

## Decision Step 2A: Cross-validate with 4-digit exact matches (STP perspective)
## Strategy: If a program combination (institution + STP program code + STP CIP) appears
## in the only 2-digit exact match records (t1), it indicates STP's CIP coding was used in
## a high-confidence match elsewhere. Assign these records to use STP CIP for consistency.
## This leverages existing reliable matches: if STP's CIP aligned perfectly in one case,
## it's likely reliable for this program type at this institution.
matched_2d_cips <- matched_2d_cips %>% # this one is from t2 which is a subset of aggregated program-level bgs_matching_flagged where is.na(MATCH_ALL_3_CIP4_FLAG), but MATCH_ALL_3_CIP2_FLAG
  left_join(
    t1 %>% # this is another subset of  program-level aggregated bgs_matching_flagged where is.na(MATCH_ALL_3_CIP4_FLAG), but MATCH_ALL_3_CIP2_FLAG
      distinct(
        INSTITUTION_CODE,
        STP_PROGRAM_CODE,
        STP_PROGRAM_DESC,
        CIP = BGS_FINAL_CIP_CODE_4, # CIP from 4-digit BGS survey records.
        STP_FINAL_CIP_CODE_4
      ) |>
      collect(),
    by = c(
      "INSTITUTION_CODE",
      "STP_PROGRAM_CODE",
      "STP_PROGRAM_DESC",
      "STP_FINAL_CIP_CODE_4" # join by the STP CIP CODE which can be used for exact matches
    ) # Detected an unexpected many-to-many relationship between `x` and `y`.? due to different  CIP = BGS_FINAL_CIP_CODE_4 which is not the join key. from 1593 -> 2018
  ) %>%
  mutate(
    # Keep existing decision, or assign STP if program found in 4-digit exact matches
    CIP_TO_USE = case_when(
      !is.na(CIP_TO_USE) ~ CIP_TO_USE,
      !is.na(CIP) ~ "STP" # Program found in 4-digit exact matches - STP CIP proved reliable. Why is it is.na(CIP)? Why we need the BGS_CIP is not na.
    )
  ) %>%
  select(-CIP) |>
  distinct() # after distinct, go back to 1593 rows

matched_2d_cips |> tally() # Expanded due to many-to-many relationships from 4-digit matches
count(matched_2d_cips, CIP_TO_USE) # Most cases now assigned; fewer unassigned (NA) cases remain
# no na anymore
#   CIP_TO_USE     n
#   <chr>      <int>
# 1 BGS           73
# 2 STP         1520

## Decision Step 2B: Cross-validate with 4-digit exact matches (BGS perspective)
## Mirror logic to Step 2A from BGS perspective: if a program combination (institution +
## BGS program code + BGS CIP) appears in 2-digit exact match records (t1), it indicates
## BGS's CIP coding was used in a high-confidence match. Assign these to use BGS CIP
## for consistency. This ensures programs matching on 4-digit BGS CIP elsewhere are
## treated the same way here.
## Warning: This join introduces a many-to-many relationship because a single BGS program
## at an institution may have matched to multiple different STP CIPs in 4-digit exact matches.
## This expands row count significantly (from ~1,600 to ~7,500) but correctly represents
## the multiple program combinations that need individual decisions.
matched_2d_cips <- matched_2d_cips %>%
  left_join(
    t1 %>%
      distinct(
        INSTITUTION_CODE,
        BGS_PROGRAM_CODE,
        BGS_PROGRAM_DESC,
        BGS_FINAL_CIP_CODE_4,
        CIP = STP_FINAL_CIP_CODE_4 # CIP from 4-digit STP records.
      ) |>
      collect(),
    by = c(
      "INSTITUTION_CODE",
      "BGS_PROGRAM_CODE",
      "BGS_PROGRAM_DESC",
      "BGS_FINAL_CIP_CODE_4"
    ),
    relationship = "many-to-many" # Expected: BGS program may have matched to multiple STP CIPs
  ) %>%
  mutate(
    # Keep existing decision, or assign BGS if program found in 2-digit exact matches
    CIP_TO_USE = case_when(
      !is.na(CIP_TO_USE) ~ CIP_TO_USE,
      !is.na(CIP) ~ "BGS" # Program found in 2-digit exact matches - BGS CIP proved reliable
    )
  ) %>%
  select(-CIP) |>
  distinct()

matched_2d_cips |> tally() # ~7,500 rows after many-to-many join expansion before `distinct` call
count(matched_2d_cips, CIP_TO_USE) # All rows should now have CIP_TO_USE assigned (no NA values)
# Nothing changed. why bother?

## Decision Step 3: Custom mappings for known program pair misalignments - may not be applicable every year
## Some program pairs have systematic misalignments due to historical CIP coding
## differences between BGS INFOWARE and STP systems. These custom rules apply
## institutional knowledge to override the algorithmic decisions when appropriate.

# Math/Applied Math distinction: BGS uses 2701 (Math), STP uses 2703 (Applied Math)
# Programs labeled "operations research" or "mash" use 2703 - follow STP
matched_2d_cips <- matched_2d_cips %>%
  mutate(
    CIP_TO_USE = case_when(
      !is.na(CIP_TO_USE) ~ CIP_TO_USE,
      BGS_FINAL_CIP_CODE_4 == "2701" & STP_FINAL_CIP_CODE_4 == "2703" ~ "STP"
    )
  )

count(matched_2d_cips, CIP_TO_USE)

# Bioengineering vs Chemical Engineering: BGS uses 1405, STP uses 1407
# Programs all say "Chemical Engineering" - follow STP
matched_2d_cips <- matched_2d_cips %>%
  mutate(
    CIP_TO_USE = case_when(
      !is.na(CIP_TO_USE) ~ CIP_TO_USE,
      BGS_FINAL_CIP_CODE_4 == "1405" & STP_FINAL_CIP_CODE_4 == "1407" ~ "STP"
    )
  )

count(matched_2d_cips, CIP_TO_USE)

## Decision Step 4: Default to STP for all remaining cases
## Remaining unresolved cases are predominantly double majors where BGS and STP
## recorded the programs in different orders (e.g., BGS: Business+Engineering,
## STP: Engineering+Business). Default to STP to maintain consistency with
## historical supply modeling practices.
matched_2d_cips <- matched_2d_cips %>%
  mutate(
    CIP_TO_USE = case_when(!is.na(CIP_TO_USE) ~ CIP_TO_USE, TRUE ~ "STP")
  )

count(matched_2d_cips, CIP_TO_USE) # All rows should have a CIP_TO_USE assignment now
log_info(
  "Part 3B+: All CIP_TO_USE decisions assigned (general program, cross-validation, custom, default STP)"
)

matched_2d_cips |> tally()
matched_2d_cips |> glimpse()
matched_2d_cips |> str()

# ---- Stage 2-digit CIP decisions into SQL for main table update ----
# WHAT: Materialize the matched_2d_cips decision table into SQL for efficient joining
#       back to the main BGS_Matching_STP_Credential_PEN table (133,952 records).
# WHY: Joining in-memory R dataframes to SQL tables is inefficient at this scale.
#      Staging the decisions as a SQL table enables fast, database-side joins.
# HOW: 1) Select only the join keys and CIP_TO_USE decision
#      2) Copy to SQL temporary table
#      3) Later: join back to main table and apply decisions (Part 3B.2)

join_keys <- c(
  "INSTITUTION_CODE",
  "PSI_CODE",
  "YEAR",
  "PSI_AWARD_SCHOOL_YEAR",
  "BGS_PROGRAM_CODE",
  "STP_PROGRAM_CODE",
  "BGS_PROGRAM_DESC",
  "STP_PROGRAM_DESC",
  "BGS_FINAL_CIP_CODE_4",
  "STP_FINAL_CIP_CODE_4",
  "MATCH_ALL_3_CIP2_FLAG"
)

stage_cols <- c(
  join_keys,
  "CIP_TO_USE"
)

# Copy decision table to SQL for fast joining
copy_to(
  con,
  matched_2d_cips |> select(stage_cols),
  name = "matched_2d_cips_r",
  temporary = FALSE,
  overwrite = TRUE
)

src_tbl <- tbl(con, "matched_2d_cips_r")
log_info("Part 3B+: Staged matched_2d_cips_r decision table to SQL Server")
src_tbl |> glimpse()
src_tbl |> tally()
# 1593

# ---- Apply 2-digit CIP decisions to main matching table ----
# WHAT: Join the 2-digit CIP decisions back to the full BGS_Matching_STP_Credential_PEN
#       table (133,952 records) and populate FINAL_CIP and USE_BGS_CIP columns for all
#       records that were flagged as MATCH_ALL_3_CIP2_FLAG (but not MATCH_ALL_3_CIP4_FLAG).
# WHY: Updates the main matching table with decisions about which CIP source to use,
#      marking these records as "FINAL_CONSIDER_A_MATCH" so they skip manual review.
# HOW: 1) Left join on multi-key match (institution, programs, years, CIPs)
#      2) Populate FINAL_CIP_CODE_4/2 with decision from matched_2d_cips lookup
#      3) Set USE_BGS_CIP based on CIP_TO_USE (Yes/No)
#      4) Flag as FINAL_CONSIDER_A_MATCH = "Yes" (no more review needed)
#      5) Materialize as updated BGS_Matching_STP_Credential_PEN table
# This left_join behaviours differently in dataframe, and in MSSQL server using dbplyr:
# local dplyr join matches NA to NA
# SQL Server join does not match NULL to NULL
# 1. dataframe:
# When you do a normal dplyr::left_join() on data frames, the default is:
#  `na_matches = "na"`
# NA can match NA
# So if both tables have missing values in one or more join columns, R will treat them as equal and join those rows.
# 2. dbplyr
# When dbplyr translates your join to SQL Server:
# NA becomes NULL
# in SQL, NULL = NULL is not TRUE
# so rows with missing values in join keys do not match
# should specifically add: na_matches = "na" in left join

# Important: use na_matches = "na" so SQL join matches NA-to-NA
# the same way as the local R join

bgs_matching_flagged <- bgs_matching_flagged %>%
  left_join(src_tbl, by = join_keys, na_matches = "na")
bgs_matching_flagged |> glimpse()
# src_tbl brings "CIP_TO_USE" column created by auto-matching

bgs_matching_flagged <- bgs_matching_flagged %>%
  mutate(
    # Use coalesce to keep already-assigned FINAL_CIPs (4-digit exact
    # matches), otherwise apply the 2-digit CIP decision from CIP_TO_USE
    FINAL_CIP_CODE_4 = coalesce(
      FINAL_CIP_CODE_4,
      case_when(
        CIP_TO_USE == "BGS" ~ BGS_FINAL_CIP_CODE_4,
        CIP_TO_USE == "STP" ~ STP_FINAL_CIP_CODE_4
      )
    ),
    #  Align 2-digit CIP with 4-digit decision for consistency
    FINAL_CIP_CODE_2 = coalesce(
      FINAL_CIP_CODE_2,
      case_when(
        CIP_TO_USE == "BGS" ~ BGS_FINAL_CIP_CODE_2,
        CIP_TO_USE == "STP" ~ STP_FINAL_CIP_CODE_2
      )
    ),

    # Track which CIP source was selected (Yes = BGS, No = STP)
    USE_BGS_CIP = coalesce(
      USE_BGS_CIP,
      case_when(
        CIP_TO_USE == "BGS" ~ "Yes",
        CIP_TO_USE == "STP" ~ "No"
      )
    ),

    # Mark records as finalized if a CIP decision was made
    FINAL_CONSIDER_A_MATCH = case_when(
      !is.na(
        coalesce(
          USE_BGS_CIP,
          case_when(
            CIP_TO_USE == "BGS" ~ "Yes",
            CIP_TO_USE == "STP" ~ "No"
          )
        )
      ) ~ "Yes",
      TRUE ~ FINAL_CONSIDER_A_MATCH
    )
  )

bgs_matching_flagged |> glimpse()

bgs_matching_flagged |> tally()
# 133952
log_info(glue::glue(
  "Part 3B+: Applied 2-digit CIP decisions to main matching table: {bgs_matching_flagged %>% tally() %>% pull()} rows"
))

# id should be unique for updates to be reliable.

# ---- Materialize updated matching table ----
# Replace the old BGS_Matching_STP_Credential_PEN with this version that includes
# the 2-digit CIP match decisions. This table will now be passed to Part 3C for
# manual review of remaining unmatched records.

# Update reference to point to final table
bgs_matching_tbl <- bgs_matching_flagged


# ---- Validation: Verify 2-digit CIP decisions were applied correctly ----
# Check distribution of USE_BGS_CIP by match type to ensure the decision logic worked

bgs_matching_tbl %>%
  count(
    MATCH_ALL_3_CIP4_FLAG,
    MATCH_ALL_3_CIP2_FLAG,
    USE_BGS_CIP,
    CIP_TO_USE
  ) %>%
  collect()
# 19262 rows have unknown/na values for 'USE_BGS_CIP'
bgs_matching_tbl %>%
  count(FINAL_CONSIDER_A_MATCH, USE_BGS_CIP, CIP_TO_USE) %>%
  collect()
# 19262 rows have na values for 'FINAL_CONSIDER_A_MATCH'

# id should be unique for updates to be reliable.

bgs_matching_tbl %>%
  count(ID) %>%
  filter(n > 1) %>%
  tally()
# 5303 rows which could be an issue.
# ------------------------------------------------------------------------------
# Part 3C: Manual Review for Institution/Year Matches with Different CIPs
#
# Purpose:
# Handle records that appear to be genuine BGS/STP matches based on institution
# and year, but where the program CIP codes differ, requiring human judgment
# for final CIP selection.
#
# Workflow:
# 1. Extract candidate records needing manual review (institution and year match,
#    but CIPs differ and no automatic decision was made).
# 2. Aggregate to program-level combinations to reduce review effort from
#    thousands of individual rows to hundreds of unique program decisions.
# 3. Export the aggregated program combinations to CSV for expert review.
# 4. Experts review and mark each program: USE_BGS_CIP = "Yes" (use BGS CIP) or
#    "No" (use STP CIP).
# 5. Re-import the reviewed CSV and apply decisions to all matching records.
#
# Output:
# - BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly (review input/output table)
#
# "lan\reports-final\internal_use_PSSM_2023-24_to_2034-35_20241220.xlsx"
# "lan\development\work\02a-program-matching\BGS\prod on 2023 data/BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_ProgramCombos_orig.csv"
# "lan\development\work\02a-program-matching\BGS\prod on 2023 data\BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_ProgramCombos.csv"
lan
# Important:
# This section requires a manual step outside R. The script expects the reviewed
# CSV to be returned with a populated USE_BGS_CIP field. If the file is missing,
# the script will default to STP CIP for unreviewed candidates.
# ------------------------------------------------------------------------------

# ---- Part 3C.1: Extract Candidates for Manual Review ----
# Filter to "borderline" matches: institution and award year match, but 4-digit
# CIPs differ, and no automatic decision was applied. These require human judgment.

# In an interactive session, you would:
# - Export manual_candidates to CSV.
# - Share with subject matter experts for review.
# - Experts mark each row: USE_BGS_CIP = "Yes" (use BGS CIP) or "No" (use STP CIP).
# - Re-import the marked CSV.

# ---- Part 3C Extended: Aggregate for Manual Review Workflow ----
# Extract institution/year matches with CIP divergence into a program-level view.
# Aggregating reduces manual review workload from thousands of individual records
# to hundreds of unique program decisions.
#
# Workflow:
# 1. Extract: Pull all institution+year matches with differing CIPs.
# 2. Aggregate: Group by program identifiers (institution, codes, names, CIPs).
# 3. Export: Save aggregated unique program combinations to CSV.
# 4. Manual Review: Experts edit CSV, adding USE_BGS_CIP column ("Yes"/"No").
# 5. Re-import: Read marked CSV back into R.
# 6. Join: Apply decisions to all individual records matching those program pairs.

# ---- Part 3C.1a: Create Initial Dataset for Manual Review ----
# Query all institution/year matches with CIP divergence, formatted for export.

BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_orig <- bgs_matching_tbl %>%
  filter(
    MATCH_INST == "Yes",
    MATCH_AWARD_SCHOOL_YEAR == "Yes",
    is.na(FINAL_CONSIDER_A_MATCH)
  ) %>%
  select(
    STQU_ID,
    ID,
    PEN,
    INSTITUTION_CODE,
    PSI_CODE,
    YEAR,
    PSI_AWARD_SCHOOL_YEAR,
    MATCH_INST,
    MATCH_AWARD_SCHOOL_YEAR,
    MATCH_ALL_3_CIP4_FLAG,
    MATCH_ALL_3_CIP2_FLAG,
    FINAL_CONSIDER_A_MATCH,
    BGS_FINAL_CIP_CODE_4,
    BGS_FINAL_CIP_CODE_4_NAME,
    STP_FINAL_CIP_CODE_4,
    STP_FINAL_CIP_CODE_4_NAME,
    BGS_FINAL_CIP_CODE_2,
    BGS_FINAL_CIP_CODE_2_NAME,
    STP_FINAL_CIP_CODE_2,
    STP_FINAL_CIP_CODE_2_NAME,
    BGS_PROGRAM_CODE,
    BGS_PROGRAM_DESC,
    STP_PROGRAM_CODE,
    STP_PROGRAM_DESC,
    USE_BGS_CIP
  ) %>%
  collect()

BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_orig %>% glimpse()
BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_orig %>% tally()
# ~6000 records

# Creates: BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly table in SQL
# This is the full row-level data for the manual review set.

# ---- Part 3C.3b: Aggregate to Program Level for Manual Review ----
# Reduce row-level data (~6000 records) to unique program combinations (~1700).
# Each row represents one unique program pair at one institution where CIP codes differ.

BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_orig_group <- BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_orig %>%
  mutate(across(everything(), trimws)) %>% # Remove excess whitespace
  group_by(
    INSTITUTION_CODE,
    PSI_CODE,
    BGS_FINAL_CIP_CODE_4,
    BGS_FINAL_CIP_CODE_4_NAME,
    STP_FINAL_CIP_CODE_4,
    STP_FINAL_CIP_CODE_4_NAME,
    BGS_PROGRAM_CODE,
    BGS_PROGRAM_DESC,
    STP_PROGRAM_CODE,
    STP_PROGRAM_DESC,
    USE_BGS_CIP # Will be NA at this stage (no decisions yet)
  ) %>%
  summarize(Count = n(), .groups = "drop")

BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_orig_group %>% glimpse()
BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_orig_group %>% tally()
# ~1700 unique combinations

BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_orig_group %>%
  collect() %>% # Collect for CSV export
  write_csv(
    "BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_ProgramCombos_orig.csv"
  )
# NEXT STEP (manual):
# - Open CSV in Excel.
# - For each row, add USE_BGS_CIP = "Yes" (use BGS CIP) or "No" (use STP CIP).
# - Save as: BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_ProgramCombos.csv.
# - Return to this script.

# ---- Part 3C.1c: Re-import Manual Decisions ----
# Read back the CSV with manual USE_BGS_CIP decisions from subject matter experts.

BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_ProgramCombos <- read_csv(
  glue::glue(
    "{lan}\\development\\work\\02a-program-matching\\BGS\\prod on 2023 data\\BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_ProgramCombos.csv"
  )
)

# ---- Part 3C.1d: Broadcast Manual Decisions to All Matching Records ----
# Join aggregated program-level decisions back to individual records.
# This propagates the manual decision for a program pair to all student records
# matching that program pair at that institution.

BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_orig %>% count(USE_BGS_CIP)
BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_ProgramCombos %>%
  count(USE_BGS_CIP)

## join manual work back to row-level data
## have to alter the tables since reading it in from CSV removes excess whitespace and changes empty strings to NA
BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly <- BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_orig %>%
  mutate(across(everything(), trimws)) %>% # Normalize whitespace from CSV round-trip
  select(-USE_BGS_CIP) %>% # Remove NA values from initial extraction
  left_join(
    # Join manual decisions by all program identifiers to match the aggregation key
    BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_ProgramCombos %>%
      mutate(across(everything(), trimws)) %>%
      mutate(across(everything(), as.character)),
    by = c(
      "INSTITUTION_CODE",
      "PSI_CODE",
      "BGS_FINAL_CIP_CODE_4",
      "BGS_FINAL_CIP_CODE_4_NAME",
      "STP_FINAL_CIP_CODE_4",
      "STP_FINAL_CIP_CODE_4_NAME",
      "BGS_PROGRAM_CODE",
      "BGS_PROGRAM_DESC",
      "STP_PROGRAM_CODE",
      "STP_PROGRAM_DESC"
    )
  )

BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly %>% glimpse() # 5945 rows

# Note: Some NAs may remain if join keys do not match exactly.

# ---- Part 3C.1e: Validate Manual Decisions Were Applied to All Records ----
# Check that every record has a USE_BGS_CIP decision (no NAs).
# If NAs remain, it indicates a mismatch in the join keys between tables.

# {
#   # Count records by USE_BGS_CIP decision
#   BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly %>%
#     count(USE_BGS_CIP)

#   # If NAs detected, debug by comparing STP_PROGRAM_CODE values in both tables
#   if (
#     BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly %>%
#       filter(is.na(USE_BGS_CIP)) %>%
#       nrow() >
#       0
#   ) {
#     # Check for mismatches in program codes
#     chk1 <- BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_ProgramCombos %>%
#       count(STP_PROGRAM_CODE)
#     chk2 <- BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_orig %>%
#       count(STP_PROGRAM_CODE)

#     # Rows in orig but not in manual decisions would cause NAs after join
#     anti_join(chk2, chk1, by = "STP_PROGRAM_CODE")

#     rm(chk1, chk2)
#   }
# }

# ---- Update BGS_Matching_STP_Credential_PEN with Final CIPs Chosen Manually ----
{
  # Optional: Save a backup copy of BGS_Matching_STP_Credential_PEN before updating
  # in case you want to make changes to the manual matching.
  if (
    dbExistsTable(
      con,
      name = Id(
        schema = my_schema,
        table = "BGS_Matching_STP_Credential_PEN_bu_r"
      )
    )
  ) {
    dbRemoveTable(
      con,
      name = Id(
        schema = my_schema,
        table = "BGS_Matching_STP_Credential_PEN_bu_r"
      )
    )
  }
  dbExecute(
    con,
    glue::glue(
      "SELECT * INTO [{my_schema}].BGS_Matching_STP_Credential_PEN_bu_r 
      FROM [{my_schema}].BGS_Matching_STP_Credential_PEN_r"
    )
  )
}

# ---- Part 3C.2: Apply Default Logic for Unreviewed Candidates ----
# For any records without explicit manual decision, default to STP CIP.
# This fallback ensures all candidates get a final CIP assignment.

# BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly %>% glimpse()
# BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly %>% tally()

BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly <- BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly %>%
  mutate(
    # Default to "No" (use STP) if manual review didn't provide a decision
    # USE_BGS_CIP = coalesce(USE_BGS_CIP, "No"),

    # Assign final 4-digit CIP based on decision
    FINAL_CIP_CODE_4 = if_else(
      USE_BGS_CIP == "Yes",
      BGS_FINAL_CIP_CODE_4, # Use BGS CIP if marked during manual review
      STP_FINAL_CIP_CODE_4 # Use STP CIP by default or if "No" marked
    ),

    # Align 2-digit CIP with 4-digit decision for consistency
    FINAL_CIP_CODE_2 = if_else(
      USE_BGS_CIP == "Yes",
      BGS_FINAL_CIP_CODE_2,
      STP_FINAL_CIP_CODE_2
    )
  ) %>%
  # Keep only columns needed to update the main matching table
  select(
    STQU_ID,
    ID,
    FINAL_CIP_CODE_4,
    FINAL_CIP_CODE_2,
    USE_BGS_CIP
  )

# BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly %>% glimpse()

# ---- Part 3C.2b: Validate Final CIPs Are Populated for All Decisions ----
# Verify that every record now has FINAL_CIP_CODE_4 and FINAL_CIP_CODE_2 values.
# Any remaining NAs indicate missing manual review decisions.

{
  # Count records with blank final CIPs (should be zero)
  BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly %>%
    filter(is.na(FINAL_CIP_CODE_4)) %>%
    count(USE_BGS_CIP)

  BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly %>%
    filter(is.na(FINAL_CIP_CODE_2)) %>%
    count(USE_BGS_CIP)
}

# ---- Part 3C.3: Upload Manual Decisions and Update Main Matching Table ----
# Apply the manual decisions back to BGS_Matching_STP_Credential_PEN table,
# replacing any placeholder values with finalized CIP assignments.

if (nrow(BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly) > 0) {
  # Stage manual updates to temporary SQL table for efficient database joining

  dbWriteTable(
    con,
    Id(
      schema = my_schema,
      table = "BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_r"
    ),
    BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly,
    overwrite = TRUE
  )

  # Join manual decisions back to main matching table

  # Source table
  source_tbl <- tbl(
    con,
    DBI::Id(
      schema = my_schema,
      table = "BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_r"
    )
  )
  # source_tbl %>% glimpse()
  # source_tbl %>% tally()
  # # Check unique keys
  # source_tbl %>% count(STQU_ID, ID) %>% collect() %>% filter(n > 1) %>% tally()
  # # Good: No duplicates in either table

  # target table
  # bgs_matching_tbl %>% glimpse()
  # bgs_matching_tbl %>% tally() # 133592 rows
  # # Check unique keys
  # bgs_matching_tbl %>%
  #   count(STQU_ID, ID) %>%
  #   collect() %>%
  #   filter(n > 1) %>%
  #   tally()
  # Good: No duplicates in either table

  # Build the rows that should update
  # qry_update_CIP_for_MatchingYearInstOnly_step1 ----
  bgs_matching_updated <- bgs_matching_tbl %>%
    left_join(
      source_tbl %>%
        transmute(
          ID,
          STQU_ID,
          src_FINAL_CIP_CODE_4 = FINAL_CIP_CODE_4,
          src_FINAL_CIP_CODE_2 = FINAL_CIP_CODE_2,
          src_USE_BGS_CIP = USE_BGS_CIP
        ),
      by = c("ID", "STQU_ID"),
      na_matches = "never"
    ) %>%
    mutate(
      needs_update = is.na(FINAL_PROBABLE_MATCH) &
        is.na(FINAL_CIP_CODE_4) &
        is.na(FINAL_CIP_CODE_2) &
        !is.na(src_FINAL_CIP_CODE_4)
    ) %>%
    mutate(
      FINAL_PROBABLE_MATCH = case_when(
        needs_update == TRUE ~ "Yes",
        TRUE ~ FINAL_PROBABLE_MATCH
      ),
      FINAL_CIP_CODE_4 = case_when(
        needs_update == TRUE ~ src_FINAL_CIP_CODE_4,
        TRUE ~ FINAL_CIP_CODE_4
      ),
      FINAL_CIP_CODE_2 = case_when(
        needs_update == TRUE ~ src_FINAL_CIP_CODE_2,
        TRUE ~ FINAL_CIP_CODE_2
      ),
      USE_BGS_CIP = case_when(
        needs_update == TRUE ~ src_USE_BGS_CIP,
        TRUE ~ USE_BGS_CIP
      )
    ) %>%
    select(-needs_update, -starts_with("src_"))
} else {
  # No manual candidates - use existing matched table
  bgs_matching_updated <- bgs_matching_tbl
}

# bgs_matching_updated %>% show_query()
bgs_matching_updated %>% count(FINAL_PROBABLE_MATCH) %>% collect()

bgs_matching_updated %>% glimpse()
bgs_matching_updated %>% tally()


# qry_update_CIP_for_MatchingYearInstOnly_step2 ----
# Update the rest of the records to use the STP CIPs as final if no match was found
bgs_matching_updated <- bgs_matching_updated %>%
  mutate(
    # Identify records where all target columns are still NULL
    needs_stp_fallback = is.na(FINAL_CIP_CODE_4) &
      is.na(FINAL_CIP_CODE_4_NAME) &
      is.na(FINAL_CIP_CODE_2) &
      is.na(FINAL_CIP_CODE_2_NAME)
  ) %>%
  mutate(
    # Apply fallback values from STP source columns
    FINAL_CIP_CODE_4 = if_else(
      needs_stp_fallback == TRUE,
      STP_FINAL_CIP_CODE_4,
      FINAL_CIP_CODE_4
    ),
    FINAL_CIP_CODE_4_NAME = if_else(
      needs_stp_fallback == TRUE,
      STP_FINAL_CIP_CODE_4_NAME,
      FINAL_CIP_CODE_4_NAME
    ),
    FINAL_CIP_CODE_2 = if_else(
      needs_stp_fallback == TRUE,
      STP_FINAL_CIP_CODE_2,
      FINAL_CIP_CODE_2
    ),
    FINAL_CIP_CODE_2_NAME = if_else(
      needs_stp_fallback == TRUE,
      STP_FINAL_CIP_CODE_2_NAME,
      FINAL_CIP_CODE_2_NAME
    ),
    USE_BGS_CIP = if_else(needs_stp_fallback == TRUE, "No", USE_BGS_CIP)
  ) %>%
  select(-needs_stp_fallback)

# bgs_matching_updated %>% show_query()
# bgs_matching_updated %>% glimpse()
# bgs_matching_updated %>% tally()

# Check remaining non-matches: Compare program descriptions to ensure they are truly non-matches
# {
#   # Get IDs of matches
#   ids_exact <- bgs_matching_updated %>%
#     filter(FINAL_CONSIDER_A_MATCH == "Yes") %>%
#     distinct(ID) %>%
#     collect()
#   ids_probable <- bgs_matching_updated %>%
#     filter(FINAL_PROBABLE_MATCH == "Yes") %>%
#     distinct(ID) %>%
#     collect()

#   # Filter out non-matches for students that have an existing matched program
#   # Review non-matches to see if any should be matched - if so, redo Part 3C to this point
#   chk <- bgs_matching_updated %>%
#     filter(is.na(FINAL_CIP_CODE_4)) %>% # Filter on empty FINAL CIP
#     filter(!is.na(MATCH_INST) & !is.na(MATCH_AWARD_SCHOOL_YEAR)) %>% # Remove records that don't match on institution or year
#     collect() %>%
#     anti_join(ids_exact, by = "ID") %>% # Remove records that already have a match (from flags)
#     anti_join(ids_probable, by = "ID") %>% # Remove records that already have a match (from manual)
#     group_by(
#       INSTITUTION_CODE,
#       BGS_FINAL_CIP_CODE_4,
#       BGS_FINAL_CIP_CODE_4_NAME,
#       STP_FINAL_CIP_CODE_4,
#       STP_FINAL_CIP_CODE_4_NAME,
#       BGS_PROGRAM_CODE,
#       BGS_PROGRAM_DESC,
#       STP_PROGRAM_CODE,
#       STP_PROGRAM_DESC,
#       USE_BGS_CIP
#     ) %>%
#     summarize(Count = n(), .groups = "drop")

#   rm(chk, ids_exact, ids_probable)
# }

# bgs_matching_updated %>% count(FINAL_CONSIDER_A_MATCH) %>% collect()
# bgs_matching_updated %>% count(FINAL_PROBABLE_MATCH) %>% collect()
# bgs_matching_updated %>%
#   count(FINAL_CONSIDER_A_MATCH, FINAL_PROBABLE_MATCH) %>%
#   collect()

# TODO [HIGH]:
# Investigate why a large number of rows still have no match after the current
# auto-match and manual-review steps.

# Validation check:
# Confirm whether this field is safe as a join key for the next update.
# In this workflow, ID is unique in the credential table, but not always unique
# in the BGS/STP crosswalk after matching on PEN.

# bgs_matching_updated %>% tally()
# # 133952 rows

# bgs_matching_updated %>%
#   count(ID) %>%
#   filter(n > 1) %>%
#   tally()
# # Still ~5000 have at least two rows

# bgs_matching_updated %>%
#   count(STQU_ID) %>%
#   filter(n > 1) %>%
#   tally()
# # ~11,000

# bgs_matching_updated %>%
#   count(STQU_ID, ID) %>%
#   filter(n > 1) %>%
#   tally()
# Zero rows

# Check consistency of USE_BGS_CIP with CIP sources
{
  bgs_matching_updated %>%
    count(
      USE_BGS_CIP,
      FINAL_CIP_CODE_4 == STP_FINAL_CIP_CODE_4,
      FINAL_CIP_CODE_4 == BGS_FINAL_CIP_CODE_4
    )
}
# Result may be confusing due to NA values

# Remove local tables
rm(
  BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly,
  BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_orig,
  BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_ProgramCombos
)

### Part 3D: Fill in Final Columns ----

# ---- Part 3D: Final Fill (CIP Names and Clusters) ----
#
# WHAT: Finalizes CIP codes for all records and enriches with names and cluster assignments.
# WHY: Some records may still have NULL CIP codes after matching. We need to ensure complete coverage
#      and add human-readable descriptions for reporting.
# HOW: 1) Default remaining NULL CIP codes to STP values
#      2) Join to 4-digit CIP names table
#      3) Join to 2-digit CIP names and cluster tables
#      4) Materialize final matching table
## Add in FINAL_CIP_CODE_4_NAME
# dbGetQuery(con, qry_fill_final_CIP4_NAME)

bgs_matching_final <- bgs_matching_updated %>%
  # Add Names
  ## qry_fill_final_CIP4_NAME ----
  ## New: fill in CIP4 NAME by linking to infoware table
  left_join(
    cip_4_tbl %>% select(LCP4_CD, LCP4_CIP_4DIGITS_NAME),
    by = c("FINAL_CIP_CODE_4" = "LCP4_CD")
  ) %>%

  ## qry_fill_final_CIP2_NAME_and_CLUSTER ----
  ## New: fill in CIP2 NAME and cluster code/name from infoware table
  left_join(
    cip_2_tbl %>%
      select(LCP2_CD, LCP2_DIGITS_NAME, LCP2_LCIPPC_CD, LCP2_LCIPPC_NAME),
    by = c("FINAL_CIP_CODE_2" = "LCP2_CD")
  ) %>%

  # TODO [MEDIUM]:
  # Confirm whether resetting these name/cluster fields is still required after
  # the current refactor. If yes, explain why we intentionally rebuild them later.

  mutate(
    FINAL_CIP_CODE_4_NAME = LCP4_CIP_4DIGITS_NAME,
    FINAL_CIP_CODE_2_NAME = LCP2_DIGITS_NAME,
    # dbGetQuery(con, qry_fill_final_CIP2_NAME_and_CLUSTER)
    ## New: fill in CIP2 NAME and cluster code/name from infoware table
    FINAL_CIP_CLUSTER_CODE = LCP2_LCIPPC_CD,
    FINAL_CIP_CLUSTER_NAME = LCP2_LCIPPC_NAME
  ) %>%
  # # Default remaining cluster info
  # mutate(
  #   FINAL_CIP_CLUSTER_CODE = coalesce(FINAL_CIP_CLUSTER_CODE, LCP2_LCIPPC_CD),
  #   FINAL_CIP_CLUSTER_NAME = coalesce(FINAL_CIP_CLUSTER_NAME, LCP2_LCIPPC_NAME)
  # ) %>%
  select(-LCP4_CIP_4DIGITS_NAME, -LCP2_LCIPPC_CD, -LCP2_LCIPPC_NAME)

# bgs_matching_final |> glimpse()
# bgs_matching_final |> tally()

# TODO [MEDIUM]:
# Review this summary output. NA values may be making the comparison hard to read.

# Materialize the update to the physical database

output_name <- "BGS_Matching_STP_Credential_PEN_r"
temp_name <- "BGS_Matching_STP_Credential_PEN_temp_r"

# Compute to temporary table first to ensure success before modifying the original
bgs_matching_final <- bgs_matching_final |>
  compute(
    name = Id(schema = my_schema, table = temp_name),
    temporary = FALSE
  )

if (
  dbExistsTable(
    con,
    name = Id(schema = my_schema, table = output_name)
  )
) {
  dbRemoveTable(
    con,
    name = Id(schema = my_schema, table = output_name)
  )
}

# Now rename temporary table to final name using SQL Server system procedure
dbExecute(
  con,
  glue::glue("EXEC sp_rename '{my_schema}.{temp_name}', '{output_name}'")
)

# Update reference to point to the finalized SQL table
bgs_matching_final <- tbl(
  con,
  Id(schema = my_schema, table = output_name)
)

## Validation check: look for any remaining missing values in the final output.
{
  bgs_matching_final %>%
    filter(is.na(FINAL_CIP_CLUSTER_CODE)) %>%
    tally()

  bgs_matching_final %>%
    count(FINAL_CONSIDER_A_MATCH, FINAL_PROBABLE_MATCH) |>
    collect()
}
log_info(glue::glue(
  "Part 3C/3D: Materialized final BGS_Matching_STP_Credential_PEN_r with manual review + final CIP names/clusters"
))
# ? still ~14000 NAs

# ------------------------------------------------------------------------------
# Part 4: Update the BGS credential table with final CIP decisions
#
# Purpose:
# Push the CIP decisions from the BGS/STP matching process back into the
# credential table used downstream.
#
# Why this step exists:
# The crosswalk table is the decision engine, but downstream modelling uses the
# credential table. This step makes the final CIP choice available there.
#
# Main stages:
# - Part 4A: Apply CIP decisions from the BGS/STP matching table.
# - Part 4B: Review unmatched credential programs and apply selected manual
#   overrides for consistency across similar programs.
#
# Output:
# - Credential_Non_Dup_BGS_IDs (updated)
# ------------------------------------------------------------------------------

### Part 4A: Update with XWALK ----
log_info("Part 4: Updating BGS credential table with final CIP decisions")

{
  ## may want to make a backup copy of Credential_Non_Dup_BGS_IDs
  ## in case you want to make changes to the manual matching
  if (
    dbExistsTable(
      con,
      name = Id(schema = my_schema, table = "Credential_Non_Dup_BGS_IDs_bu_r")
    )
  ) {
    dbRemoveTable(
      con,
      name = Id(schema = my_schema, table = "Credential_Non_Dup_BGS_IDs_bu_r")
    )
  }
  dbExecute(
    con,
    glue::glue(
      "select * into [{my_schema}].Credential_Non_Dup_BGS_IDs_bu_r from [{my_schema}].Credential_Non_Dup_BGS_IDs_r"
    )
  )
}

## Fill in final CIPS with BGS_Matching_STP_Credential_PEN
# id should be unique for updates to be reliable.

# bgs_matching_final %>%
#   count(ID, STQU_ID) %>%
#   filter(n > 1) %>%
#   tally()

# bgs_matching_final %>%
#   count(ID) %>%
#   filter(n > 1) %>%
#   tally()
# over ~ 5000

# bgs_matching_final %>%
#   filter(!is.na(FINAL_CONSIDER_A_MATCH) | !is.na(FINAL_PROBABLE_MATCH)) %>%
#   select(
#     ID,
#     MATCH_FINAL_CIP_4 = FINAL_CIP_CODE_4,
#     MATCH_FINAL_CIP_4_NAME = FINAL_CIP_CODE_4_NAME,
#     MATCH_FINAL_CIP_2 = FINAL_CIP_CODE_2,
#     MATCH_FINAL_CIP_2_NAME = FINAL_CIP_CODE_2_NAME,
#     MATCH_FINAL_CLUSTER_CODE = FINAL_CIP_CLUSTER_CODE,
#     MATCH_FINAL_CLUSTER_NAME = FINAL_CIP_CLUSTER_NAME,
#     MATCH_USE_BGS = USE_BGS_CIP,
#     MATCH_BGS_CIP_4 = BGS_FINAL_CIP_CODE_4,
#     MATCH_BGS_CIP_4_NAME = BGS_FINAL_CIP_CODE_4_NAME,
#     FINAL_CONSIDER_A_MATCH,
#     FINAL_PROBABLE_MATCH
#   ) %>%
#   distinct() |> # only remove 10 rows
#   count(ID) %>%
#   filter(n > 1) %>%
#   tally()
# # over ~ 300, left join with ID will create over 300 duplications.

# bgs_matching_final %>%
#   filter(!is.na(FINAL_CONSIDER_A_MATCH) | !is.na(FINAL_PROBABLE_MATCH)) %>%
#   select(
#     ID,
#     MATCH_FINAL_CIP_4 = FINAL_CIP_CODE_4,
#     MATCH_FINAL_CIP_4_NAME = FINAL_CIP_CODE_4_NAME,
#     MATCH_FINAL_CIP_2 = FINAL_CIP_CODE_2,
#     MATCH_FINAL_CIP_2_NAME = FINAL_CIP_CODE_2_NAME,
#     MATCH_FINAL_CLUSTER_CODE = FINAL_CIP_CLUSTER_CODE,
#     MATCH_FINAL_CLUSTER_NAME = FINAL_CIP_CLUSTER_NAME,
#     MATCH_USE_BGS = USE_BGS_CIP,
#     MATCH_BGS_CIP_4 = BGS_FINAL_CIP_CODE_4,
#     MATCH_BGS_CIP_4_NAME = BGS_FINAL_CIP_CODE_4_NAME,
#     FINAL_CONSIDER_A_MATCH,
#     FINAL_PROBABLE_MATCH
#   ) %>%
#   distinct() |> # only remove 10 rows
#   group_by(ID) %>%
#   mutate(n = n()) %>%
#   filter(n > 1) %>%
#   glimpse()

# ------------------------------------------------------------------------------
# Prepare Step 1 source: rows with FINAL_CONSIDER_A_MATCH
#
# SQL equivalent:
# qry_update_Credential_Non_Dup_BGS_IDS_CIP_matches_step1
#
# Business rule:
# If the crosswalk says this is a confirmed "consider a match" case, copy the
# final CIP fields and match metadata into the credential table.
#
# One row per ID is required before joining. If multiple crosswalk rows for the
# same ID carry the same final decision, distinct() will collapse them safely.
# If conflicting decisions remain, stop and review before updating.
# ------------------------------------------------------------------------------

cred_step1_src <- bgs_matching_final %>%
  filter(
    !is.na(FINAL_CONSIDER_A_MATCH),
    FINAL_CONSIDER_A_MATCH != ""
  ) %>%
  transmute(
    ID,
    STEP1_FINAL_CIP_CODE_4 = FINAL_CIP_CODE_4,
    STEP1_FINAL_CIP_CODE_4_NAME = FINAL_CIP_CODE_4_NAME,
    STEP1_FINAL_CIP_CODE_2 = FINAL_CIP_CODE_2,
    STEP1_FINAL_CIP_CODE_2_NAME = FINAL_CIP_CODE_2_NAME,
    STEP1_FINAL_CIP_CLUSTER_CODE = FINAL_CIP_CLUSTER_CODE,
    STEP1_FINAL_CIP_CLUSTER_NAME = FINAL_CIP_CLUSTER_NAME,
    STEP1_USE_BGS_CIP = USE_BGS_CIP,
    STEP1_OUTCOMES_CIP_CODE_4 = BGS_FINAL_CIP_CODE_4,
    STEP1_OUTCOMES_CIP_CODE_4_NAME = BGS_FINAL_CIP_CODE_4_NAME,
    STEP1_FINAL_CONSIDER_A_MATCH = FINAL_CONSIDER_A_MATCH,
    STEP1_FINAL_PROBABLE_MATCH = FINAL_PROBABLE_MATCH
  ) %>%
  distinct()

# Optional validation: if an ID still appears more than once here, the crosswalk
# has conflicting step-1 decisions and should be reviewed before updating.
# cred_step1_dups <- cred_step1_src %>%
#   count(ID) %>%
#   filter(n > 1) |>
#   collect()
# TODO: 9 IDs have conflicting step-1 decisions. This should be investigated before updating.
# if (nrow(cred_step1_dups) > 0) {
#   stop(
#     "Step 1 source has duplicate IDs. Review bgs_matching_final before updating Credential_Non_Dup_BGS_IDs."
#   )
# }

new_cols <- c(
  "OUTCOMES_CIP_CODE_4", # 4-digit STP CIP code for comparison
  "OUTCOMES_CIP_CODE_4_NAME", # 4-digit STP CIP name for comparison
  "FINAL_CONSIDER_A_MATCH", # Flag indicating auto-matched records (high confidence)
  "FINAL_PROBABLE_MATCH", # Flag indicating manually reviewed matches (medium confidence)
  "USE_BGS_CIP", # Flag: "Yes" if BGS CIP was selected as final, "No" if STP CIP was selected
  "FINAL_CIP_CODE_4", # Final 4-digit CIP code (either BGS or STP based on matching logic)
  "FINAL_CIP_CODE_4_NAME", # Final 4-digit CIP name
  "FINAL_CIP_CODE_2", # Final 2-digit CIP code (aligned with 4-digit choice)
  "FINAL_CIP_CODE_2_NAME", # Final 2-digit CIP name
  "FINAL_CIP_CLUSTER_CODE", # Final CIP cluster code (from CIP2016 taxonomy)
  "FINAL_CIP_CLUSTER_NAME" # Final CIP cluster name (from CIP2016 taxonomy)
)

# Add new columns to the table
for (col in new_cols) {
  dbExecute(
    con,
    glue::glue(
      "ALTER TABLE [{my_schema}].Credential_Non_Dup_BGS_IDs_r ADD [{col}] VARCHAR(255) NULL"
    )
  )
}

credential_bgs_updated <- tbl(
  con,
  in_schema(my_schema, "Credential_Non_Dup_BGS_IDs_r")
)
# Validation check: the credential table should still be one row per ID.
# credential_bgs_updated %>%
#   count(ID) %>%
#   filter(n > 1) %>%
#   tally()
# Expected: zero duplicate IDs

# ------------------------------------------------------------------------------
# Step 1 update: apply FINAL_CONSIDER_A_MATCH results
#
# This mirrors the SQL UPDATE ... JOIN for exact / high-confidence matches.
# ------------------------------------------------------------------------------

credential_bgs_updated <- credential_bgs_updated %>%
  left_join(cred_step1_src, by = "ID") %>%
  mutate(
    step1_apply = !is.na(STEP1_FINAL_CONSIDER_A_MATCH) &
      STEP1_FINAL_CONSIDER_A_MATCH != ""
  ) %>%
  mutate(
    FINAL_CIP_CODE_4 = if_else(
      step1_apply == TRUE,
      STEP1_FINAL_CIP_CODE_4,
      FINAL_CIP_CODE_4
    ),
    FINAL_CIP_CODE_4_NAME = if_else(
      step1_apply == TRUE,
      STEP1_FINAL_CIP_CODE_4_NAME,
      FINAL_CIP_CODE_4_NAME
    ),
    FINAL_CIP_CODE_2 = if_else(
      step1_apply == TRUE,
      STEP1_FINAL_CIP_CODE_2,
      FINAL_CIP_CODE_2
    ),
    FINAL_CIP_CODE_2_NAME = if_else(
      step1_apply == TRUE,
      STEP1_FINAL_CIP_CODE_2_NAME,
      FINAL_CIP_CODE_2_NAME
    ),
    FINAL_CIP_CLUSTER_CODE = if_else(
      step1_apply == TRUE,
      STEP1_FINAL_CIP_CLUSTER_CODE,
      FINAL_CIP_CLUSTER_CODE
    ),
    FINAL_CIP_CLUSTER_NAME = if_else(
      step1_apply == TRUE,
      STEP1_FINAL_CIP_CLUSTER_NAME,
      FINAL_CIP_CLUSTER_NAME
    ),
    USE_BGS_CIP = if_else(
      step1_apply == TRUE,
      STEP1_USE_BGS_CIP,
      USE_BGS_CIP
    ),
    OUTCOMES_CIP_CODE_4 = if_else(
      step1_apply == TRUE,
      STEP1_OUTCOMES_CIP_CODE_4,
      OUTCOMES_CIP_CODE_4
    ),
    OUTCOMES_CIP_CODE_4_NAME = if_else(
      step1_apply == TRUE,
      STEP1_OUTCOMES_CIP_CODE_4_NAME,
      OUTCOMES_CIP_CODE_4_NAME
    ),
    FINAL_CONSIDER_A_MATCH = if_else(
      step1_apply == TRUE,
      STEP1_FINAL_CONSIDER_A_MATCH,
      FINAL_CONSIDER_A_MATCH
    ),
    FINAL_PROBABLE_MATCH = if_else(
      step1_apply == TRUE,
      STEP1_FINAL_PROBABLE_MATCH,
      FINAL_PROBABLE_MATCH
    )
  ) %>%
  select(-starts_with("STEP1_"), -step1_apply)

# ------------------------------------------------------------------------------
# Prepare Step 2 source: rows with FINAL_PROBABLE_MATCH
#
# SQL equivalent:
# qry_update_Credential_Non_Dup_BGS_IDS_CIP_matches_step2
#
# Business rule:
# Use probable-match decisions only for rows that are still completely empty
# in the target match-derived fields after Step 1.
# ------------------------------------------------------------------------------

cred_step2_src <- bgs_matching_final %>%
  filter(
    !is.na(FINAL_PROBABLE_MATCH),
    FINAL_PROBABLE_MATCH != ""
  ) %>%
  transmute(
    ID,
    STEP2_FINAL_CIP_CODE_4 = FINAL_CIP_CODE_4,
    STEP2_FINAL_CIP_CODE_4_NAME = FINAL_CIP_CODE_4_NAME,
    STEP2_FINAL_CIP_CODE_2 = FINAL_CIP_CODE_2,
    STEP2_FINAL_CIP_CODE_2_NAME = FINAL_CIP_CODE_2_NAME,
    STEP2_FINAL_CIP_CLUSTER_CODE = FINAL_CIP_CLUSTER_CODE,
    STEP2_FINAL_CIP_CLUSTER_NAME = FINAL_CIP_CLUSTER_NAME,
    STEP2_USE_BGS_CIP = USE_BGS_CIP,
    STEP2_OUTCOMES_CIP_CODE_4 = BGS_FINAL_CIP_CODE_4,
    STEP2_OUTCOMES_CIP_CODE_4_NAME = BGS_FINAL_CIP_CODE_4_NAME,
    STEP2_FINAL_PROBABLE_MATCH = FINAL_PROBABLE_MATCH
  ) %>%
  distinct()

# cred_step2_dups <- cred_step2_src %>%
#   count(ID) %>%
#   filter(n > 1) |>
#   collect()
#  7 IDs have conflicting step-2 decisions. This should be investigated before updating.
# if (nrow(cred_step2_dups) > 0) {
#   stop(
#     "Step 2 source has duplicate IDs. Review bgs_matching_final before updating Credential_Non_Dup_BGS_IDs."
#   )
# }

# ------------------------------------------------------------------------------
# Step 2 update: apply FINAL_PROBABLE_MATCH only where the target fields are empty
#
# This mirrors the SQL WHERE clause that requires all target match-derived fields
# to still be NULL before the probable match is applied.
# ------------------------------------------------------------------------------

credential_bgs_updated <- credential_bgs_updated %>%
  left_join(cred_step2_src, by = "ID") %>%
  mutate(
    step2_target_is_empty = is.na(FINAL_CIP_CODE_4) &
      is.na(FINAL_CIP_CODE_4_NAME) &
      is.na(FINAL_CIP_CODE_2) &
      is.na(FINAL_CIP_CODE_2_NAME) &
      is.na(FINAL_CIP_CLUSTER_CODE) &
      is.na(FINAL_CIP_CLUSTER_NAME) &
      is.na(USE_BGS_CIP) &
      is.na(OUTCOMES_CIP_CODE_4) &
      is.na(OUTCOMES_CIP_CODE_4_NAME) &
      is.na(FINAL_PROBABLE_MATCH),

    step2_source_ok = !is.na(STEP2_FINAL_PROBABLE_MATCH) &
      STEP2_FINAL_PROBABLE_MATCH != "", # this may be redundant

    step2_update = step2_target_is_empty == TRUE & step2_source_ok == TRUE
  ) %>%
  mutate(
    FINAL_CIP_CODE_4 = if_else(
      step2_update == TRUE,
      STEP2_FINAL_CIP_CODE_4,
      FINAL_CIP_CODE_4
    ),
    FINAL_CIP_CODE_4_NAME = if_else(
      step2_update == TRUE,
      STEP2_FINAL_CIP_CODE_4_NAME,
      FINAL_CIP_CODE_4_NAME
    ),
    FINAL_CIP_CODE_2 = if_else(
      step2_update == TRUE,
      STEP2_FINAL_CIP_CODE_2,
      FINAL_CIP_CODE_2
    ),
    FINAL_CIP_CODE_2_NAME = if_else(
      step2_update == TRUE,
      STEP2_FINAL_CIP_CODE_2_NAME,
      FINAL_CIP_CODE_2_NAME
    ),
    FINAL_CIP_CLUSTER_CODE = if_else(
      step2_update == TRUE,
      STEP2_FINAL_CIP_CLUSTER_CODE,
      FINAL_CIP_CLUSTER_CODE
    ),
    FINAL_CIP_CLUSTER_NAME = if_else(
      step2_update == TRUE,
      STEP2_FINAL_CIP_CLUSTER_NAME,
      FINAL_CIP_CLUSTER_NAME
    ),
    USE_BGS_CIP = if_else(
      step2_update == TRUE,
      STEP2_USE_BGS_CIP,
      USE_BGS_CIP
    ),
    OUTCOMES_CIP_CODE_4 = if_else(
      step2_update == TRUE,
      STEP2_OUTCOMES_CIP_CODE_4,
      OUTCOMES_CIP_CODE_4
    ),
    OUTCOMES_CIP_CODE_4_NAME = if_else(
      step2_update == TRUE,
      STEP2_OUTCOMES_CIP_CODE_4_NAME,
      OUTCOMES_CIP_CODE_4_NAME
    ),
    FINAL_PROBABLE_MATCH = if_else(
      step2_update == TRUE,
      STEP2_FINAL_PROBABLE_MATCH,
      FINAL_PROBABLE_MATCH
    )
  ) %>%
  select(
    -starts_with("STEP2_"),
    -step2_target_is_empty,
    -step2_source_ok,
    -step2_update
  )

# ------------------------------------------------------------------------------
# Step 3 fallback: for remaining unresolved rows, use STP CIP
#
# SQL equivalent:
# qry_update_remaining_BGS_CIPs_in_Cred_Non_Dup_BGS_IDS_step1
#
# Business rule:
# If no exact/probable match decision was applied, use the credential-side STP
# CIP as the final CIP and mark USE_BGS_CIP = "No because no match".
# ------------------------------------------------------------------------------

credential_bgs_updated <- credential_bgs_updated %>%
  mutate(
    needs_stp_fallback = is.na(FINAL_CIP_CODE_4) &
      is.na(FINAL_CIP_CODE_2) &
      is.na(FINAL_CONSIDER_A_MATCH) &
      is.na(FINAL_PROBABLE_MATCH)
  ) %>%
  mutate(
    FINAL_CIP_CODE_4 = if_else(
      needs_stp_fallback == TRUE,
      STP_CIP_CODE_4,
      FINAL_CIP_CODE_4
    ),
    FINAL_CIP_CODE_4_NAME = if_else(
      needs_stp_fallback == TRUE,
      STP_CIP_CODE_4_NAME,
      FINAL_CIP_CODE_4_NAME
    ),
    FINAL_CIP_CODE_2 = if_else(
      needs_stp_fallback == TRUE,
      STP_CIP_CODE_2,
      FINAL_CIP_CODE_2
    ),
    FINAL_CIP_CODE_2_NAME = if_else(
      needs_stp_fallback == TRUE,
      STP_CIP_CODE_2_NAME,
      FINAL_CIP_CODE_2_NAME
    ),
    USE_BGS_CIP = if_else(
      needs_stp_fallback == TRUE,
      "No because no match",
      USE_BGS_CIP
    )
  ) %>%
  select(-needs_stp_fallback)

# ------------------------------------------------------------------------------
# Step 4 fill cluster fields from the official 2-digit CIP lookup
#
# SQL equivalent:
# qry_update_remaining_BGS_CIPs_in_Cred_Non_Dup_BGS_IDS_step2
#
# Business rule:
# If final cluster fields are still missing, fill them from the CIP 2-digit
# lookup using FINAL_CIP_CODE_2.
# ------------------------------------------------------------------------------

credential_bgs_updated <- credential_bgs_updated %>%
  left_join(
    cip_2_tbl %>%
      select(LCP2_CD, LCP2_LCIPPC_CD, LCP2_LCIPPC_NAME),
    by = c("FINAL_CIP_CODE_2" = "LCP2_CD")
  ) %>%
  mutate(
    fill_cluster = is.na(FINAL_CIP_CLUSTER_CODE) &
      is.na(FINAL_CIP_CLUSTER_NAME),

    FINAL_CIP_CLUSTER_CODE = if_else(
      fill_cluster == TRUE,
      LCP2_LCIPPC_CD,
      FINAL_CIP_CLUSTER_CODE
    ),
    FINAL_CIP_CLUSTER_NAME = if_else(
      fill_cluster == TRUE,
      LCP2_LCIPPC_NAME,
      FINAL_CIP_CLUSTER_NAME
    )
  ) %>%
  select(-LCP2_LCIPPC_CD, -LCP2_LCIPPC_NAME, -fill_cluster)

# ------------------------------------------------------------------------------
# Optional validation checks
# ------------------------------------------------------------------------------

# Check that the credential table is still one row per ID after the update logic.
# credential_bgs_updated %>%
#   count(ID) %>%
#   filter(n > 1) %>%
#   tally()
# #  16 IDs have duplicate rows after the update. This should be investigated to ensure data integrity.
# # Quick summary of how rows were resolved.
# credential_bgs_updated %>%
#   count(FINAL_CONSIDER_A_MATCH, FINAL_PROBABLE_MATCH, USE_BGS_CIP)
# 70% fall back to STP cip
# ------------------------------------------------------------------------------

# Part 4B: Update unmatched credential programs using approved BGS CIP overrides
#
# Purpose:
# Improve consistency for unmatched credential records that defaulted to STP CIP
# because no case-level BGS/STP match was found.
#
# Why this step exists:
# Some unmatched credential programs appear elsewhere in matched records where
# BGS CIP was clearly selected as the better final CIP. This section identifies
# those patterns, supports manual review, and then applies approved overrides to
# unmatched credential rows with the same program description.
#
# Workflow:
# 1. Build an evidence table of matched credential programs where BGS CIP was
#    selected as the final CIP.
# 2. Build an audit table of unmatched credential programs that still show
#    USE_BGS_CIP = "No because no match".
# 3. Link unmatched programs to matched BGS-CIP evidence using credential-side
#    program identifiers.
# 4. Summarize candidate overrides for analyst review.
# 5. Apply the approved manual override table.
# 6. Refill final CIP names and cluster fields from the official CIP lookups.
# ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# 4B.1 Evidence table: matched credential programs where BGS CIP was used
#
# Purpose:
# Capture credential program combinations where the final decision was to use
# BGS CIP instead of STP CIP.
#
# Why this matters:
# These matched cases are the evidence base for deciding whether similar
# unmatched programs should also switch to BGS CIP for consistency.
# ------------------------------------------------------------------------------

credential_matched_cips_using_bgs <- credential_bgs_updated %>%
  filter(USE_BGS_CIP == "Yes") %>%
  group_by(
    PSI_CODE,
    PSI_PROGRAM_CODE,
    PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    OUTCOMES_CIP_CODE_4,
    OUTCOMES_CIP_CODE_4_NAME,
    STP_CIP_CODE_4,
    STP_CIP_CODE_4_NAME,
    STP_CIP_CODE_2,
    STP_CIP_CODE_2_NAME,
    FINAL_CIP_CODE_4,
    FINAL_CIP_CODE_4_NAME,
    FINAL_CIP_CODE_2,
    FINAL_CIP_CODE_2_NAME,
    FINAL_CIP_CLUSTER_CODE,
    FINAL_CIP_CLUSTER_NAME,
    FINAL_CONSIDER_A_MATCH,
    FINAL_PROBABLE_MATCH,
    USE_BGS_CIP
  ) %>%
  summarise(n = n(), .groups = "drop")

# ------------------------------------------------------------------------------
# 4B.2 Audit table: unmatched credential programs
#
# Purpose:
# Capture credential program combinations that did not receive a BGS/STP match
# decision and therefore kept the fallback value:
#   USE_BGS_CIP = "No because no match"
# ------------------------------------------------------------------------------

credential_unmatched_cips <- credential_bgs_updated %>%
  filter(USE_BGS_CIP == "No because no match") %>%
  group_by(
    PSI_CODE,
    PSI_PROGRAM_CODE,
    PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    OUTCOMES_CIP_CODE_4,
    OUTCOMES_CIP_CODE_4_NAME,
    STP_CIP_CODE_4,
    STP_CIP_CODE_4_NAME,
    STP_CIP_CODE_2,
    STP_CIP_CODE_2_NAME,
    FINAL_CIP_CODE_4,
    FINAL_CIP_CODE_4_NAME,
    FINAL_CIP_CODE_2,
    FINAL_CIP_CODE_2_NAME,
    FINAL_CIP_CLUSTER_CODE,
    FINAL_CIP_CLUSTER_NAME,
    FINAL_CONSIDER_A_MATCH,
    FINAL_PROBABLE_MATCH,
    USE_BGS_CIP
  ) %>%
  summarise(n = n(), .groups = "drop")

# ------------------------------------------------------------------------------
# 4B.3 Candidate review table
#
# Purpose:
# Find unmatched credential programs that appear elsewhere in matched records
# where BGS CIP was selected as the final CIP.
#
# Join keys:
# Use PSI_CODE, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION, and
# STP_CIP_CODE_4 to find closely matching program definitions.
#
# Interpretation:
# - MATCHED_BGS_EXAMPLE = "Yes" means the unmatched program has a matched
#   counterpart where BGS CIP was used.
# - BGS_CIP_DIFFERS_FROM_STP = "Yes" means the suggested BGS CIP is actually
#   different from the original STP CIP, so the program is a real override
#   candidate rather than a no-op.
# ------------------------------------------------------------------------------
## Combine the lists to find any unmatched programs that were matched to outcomes for different records
## Filter where the BGS and STP CIPs differ

credential_unmatched_cips_to_review <- credential_unmatched_cips %>%
  select(-OUTCOMES_CIP_CODE_4, -OUTCOMES_CIP_CODE_4_NAME) %>%
  left_join(
    credential_matched_cips_using_bgs %>%
      distinct(
        PSI_CODE,
        PSI_PROGRAM_CODE,
        PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
        STP_CIP_CODE_4,
        OUTCOMES_CIP_CODE_4,
        OUTCOMES_CIP_CODE_4_NAME
      ),
    by = c(
      "PSI_CODE",
      "PSI_PROGRAM_CODE",
      "PSI_CREDENTIAL_PROGRAM_DESCRIPTION",
      "STP_CIP_CODE_4"
    )
  ) %>%
  mutate(
    MATCHED_BGS_EXAMPLE = case_when(
      !is.na(OUTCOMES_CIP_CODE_4) ~ "Yes",
      TRUE ~ NA_character_
    ),
    BGS_CIP_DIFFERS_FROM_STP = case_when(
      # !is.na(OUTCOMES_CIP_CODE_4) &
      OUTCOMES_CIP_CODE_4 != STP_CIP_CODE_4 ~ "Yes",
      TRUE ~ NA_character_
    )
  ) %>%
  filter(
    MATCHED_BGS_EXAMPLE == "Yes",
    BGS_CIP_DIFFERS_FROM_STP == "Yes"
  )
# %>%
#   arrange(FINAL_CIP_CODE_4)

credential_unmatched_cips_to_review %>% glimpse()

log_info("Test credential_unmatched_cips_to_review")
credential_unmatched_cips_to_review %>% tally()

# ------------------------------------------------------------------------------
# 4B.4 Program-level summary for analyst review
#
# Purpose:
# Reduce the row-level candidate table to one row per credential program
# description and STP CIP combination for easier review.
#
# Why this matters:
# Analysts usually review program-level patterns, not every individual row.
# Keeping only count == 1 avoids cases where one program maps to multiple
# possible BGS CIPs and needs more careful review.
# ------------------------------------------------------------------------------

credential_unmatched_cips_review_summary <- credential_unmatched_cips_to_review %>%
  group_by(
    PSI_CODE,
    PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    STP_CIP_CODE_4,
    STP_CIP_CODE_4_NAME
  ) %>%
  summarise(
    OUTCOMES_CIP_NAME = str_flatten(OUTCOMES_CIP_CODE_4_NAME, collapse = "\n "),
    OUTCOMES_CIP_CODE = str_flatten(OUTCOMES_CIP_CODE_4, collapse = "\n "),
    count = n(),
    .groups = "drop"
  ) %>%
  filter(count == 1)

credential_unmatched_cips_review_summary %>% glimpse()
credential_unmatched_cips_review_summary %>% tally()

# ------------------------------------------------------------------------------
# 4B.5 Approved manual override table
#
# Purpose:
# Store the final list of unmatched credential programs that should use BGS CIP
# instead of their default STP CIP fallback.
#
# Notes:
# - This is a manual, analyst-approved lookup table.
# - The join key used later is PSI_CREDENTIAL_PROGRAM_DESCRIPTION.
# - Keep one row per program description.
# ------------------------------------------------------------------------------

credential_unmatched_cips_to_update <- tibble::tribble(
  ~PSI_CREDENTIAL_PROGRAM_DESCRIPTION                                         , ~FINAL_CIP_CODE_4 , ~FINAL_CIP_CODE_2 ,
  "Bachelor Of Applied Science In Mechatronic Systems Engineering"            ,              1442 ,                14 ,
  "Bachelor Of Athletic And Exercise Therapy"                                 ,              5123 ,                51 ,
  "Bachelor Of Fine Arts In Dance"                                            ,              5003 ,                50 ,
  "Bachelor Of Fine Arts In Film"                                             ,              5006 ,                50 ,
  "Bachelor Of Fine Arts In Music - Composition"                              ,              5009 ,                50 ,
  "Bachelor Of Fine Arts In Music - Electroacoustic"                          ,              5009 ,                50 ,
  "Bachelor Of Fine Arts In Theatre - Performance"                            ,              5005 ,                50 ,
  "Bachelor Of Fine Arts In Theatre - Production And Design"                  ,              5005 ,                50 ,
  "Bachelor Of Science In Geographic Information Science"                     ,              4507 ,                45 ,
  "Bachelor Of Social Work In Indigenous Child Welfare"                       ,              4407 ,                44 ,
  "Bachelor Of Social Work In Indigenous Social Work"                         ,              4407 ,                44 ,
  "Bachelor Of Child & Youth Care In Child & Youth Care"                      ,              1907 ,                19 ,
  "Bachelor Of Child & Youth Care In Child & Youth Care - Child Life Stream"  ,              1907 ,                19 ,
  "Bachelor Of Child & Youth Care In Child & Youth Care - Early Years Stream" ,              1907 ,                19 ,
  "Bachelor Of Child & Youth Care In Child & Youth Care - Child Protection"   ,              1907 ,                19 ,
  "Bachelor Of Child & Youth Care In Child & Youth Care - Indigenous Stream"  ,              1907 ,                19
)

# Validation check: the manual override table must be unique by program description.
dup_override_programs <- credential_unmatched_cips_to_update %>%
  count(PSI_CREDENTIAL_PROGRAM_DESCRIPTION) %>%
  filter(n > 1)

if (nrow(dup_override_programs) > 0) {
  stop(
    "credential_unmatched_cips_to_update has duplicate PSI_CREDENTIAL_PROGRAM_DESCRIPTION values."
  )
}

# Write the approved override table to SQL for database-side joins.
dbWriteTable(
  con,
  "Credential_Unmatched_CIPS_to_update_r",
  credential_unmatched_cips_to_update,
  overwrite = TRUE
)

# Reload as a dbplyr table reference.
credential_unmatched_cips_to_update <- tbl(
  con,
  in_schema(my_schema, "Credential_Unmatched_CIPS_to_update_r")
)

# ------------------------------------------------------------------------------
# 4B.6 Apply approved overrides to unmatched credential rows
#
# Business rule:
# Only override rows that were never matched earlier in the workflow:
#   - FINAL_CONSIDER_A_MATCH is NA
#   - FINAL_PROBABLE_MATCH is NA
#
# After replacing the final CIP codes, clear the dependent name and cluster
# fields so they can be rebuilt from the official lookup tables.
# ------------------------------------------------------------------------------

credential_bgs_updated <- credential_bgs_updated %>%
  left_join(
    credential_unmatched_cips_to_update %>%
      select(
        PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
        FINAL_CIP_CODE_4,
        FINAL_CIP_CODE_2
      ) %>%
      rename(
        upd_FINAL_CIP_CODE_4 = FINAL_CIP_CODE_4,
        upd_FINAL_CIP_CODE_2 = FINAL_CIP_CODE_2
      ),
    by = "PSI_CREDENTIAL_PROGRAM_DESCRIPTION"
  ) %>%
  mutate(
    apply_unmatched_override = is.na(FINAL_CONSIDER_A_MATCH) &
      is.na(FINAL_PROBABLE_MATCH) &
      (!is.na(upd_FINAL_CIP_CODE_4) | !is.na(upd_FINAL_CIP_CODE_2))
  ) %>%
  mutate(
    FINAL_CIP_CODE_4 = if_else(
      apply_unmatched_override == TRUE & !is.na(upd_FINAL_CIP_CODE_4),
      upd_FINAL_CIP_CODE_4,
      FINAL_CIP_CODE_4
    ),
    FINAL_CIP_CODE_4_NAME = if_else(
      apply_unmatched_override == TRUE & !is.na(upd_FINAL_CIP_CODE_4),
      NA_character_,
      FINAL_CIP_CODE_4_NAME
    ),
    FINAL_CIP_CODE_2 = if_else(
      apply_unmatched_override == TRUE & !is.na(upd_FINAL_CIP_CODE_2),
      upd_FINAL_CIP_CODE_2,
      FINAL_CIP_CODE_2
    ),
    FINAL_CIP_CODE_2_NAME = if_else(
      apply_unmatched_override == TRUE & !is.na(upd_FINAL_CIP_CODE_2),
      NA_character_,
      FINAL_CIP_CODE_2_NAME
    ),
    FINAL_CIP_CLUSTER_CODE = if_else(
      apply_unmatched_override == TRUE,
      NA_character_,
      FINAL_CIP_CLUSTER_CODE
    ),
    FINAL_CIP_CLUSTER_NAME = if_else(
      apply_unmatched_override == TRUE,
      NA_character_,
      FINAL_CIP_CLUSTER_NAME
    )
  ) %>%
  select(
    -upd_FINAL_CIP_CODE_4,
    -upd_FINAL_CIP_CODE_2,
    -apply_unmatched_override
  )

# ------------------------------------------------------------------------------
# 4B.7 Validation checks after override
# ------------------------------------------------------------------------------

# Review remaining rows with missing 4-digit CIP names.
credential_bgs_updated %>%
  filter(is.na(FINAL_CIP_CODE_4_NAME)) %>%
  count(FINAL_CONSIDER_A_MATCH, FINAL_PROBABLE_MATCH)

credential_bgs_updated %>%
  filter(is.na(FINAL_CIP_CODE_4_NAME)) %>%
  count(FINAL_CIP_CODE_4)
# so we still have 9 rows with missing CIP names, but they all have the CIP code which is in the override table. This suggests the override was applied but the name needs to be refilled from the official lookup.
# ------------------------------------------------------------------------------
# 4B.8 Refill missing 4-digit CIP names from official lookup
# ------------------------------------------------------------------------------

credential_bgs_updated <- credential_bgs_updated %>%
  left_join(
    cip_4_tbl %>%
      select(LCP4_CD, LCP4_CIP_4DIGITS_NAME),
    by = c("FINAL_CIP_CODE_4" = "LCP4_CD")
  ) %>%
  mutate(
    FINAL_CIP_CODE_4_NAME = if_else(
      is.na(FINAL_CIP_CODE_4_NAME),
      LCP4_CIP_4DIGITS_NAME,
      FINAL_CIP_CODE_4_NAME
    )
  ) %>%
  select(-LCP4_CIP_4DIGITS_NAME)

# ------------------------------------------------------------------------------
# 4B.9 Refill missing 2-digit CIP names and cluster fields from official lookup
#
# Use a helper flag so all related fields are refilled consistently based on
# whether FINAL_CIP_CODE_2_NAME was missing before this refill step.
# ------------------------------------------------------------------------------

credential_bgs_updated <- credential_bgs_updated %>%
  left_join(
    cip_2_tbl %>%
      select(
        LCP2_CD,
        LCP2_DIGITS_NAME,
        LCP2_LCIPPC_CD,
        LCP2_LCIPPC_NAME
      ),
    by = c("FINAL_CIP_CODE_2" = "LCP2_CD")
  ) %>%
  mutate(
    fill_cip2_fields = is.na(FINAL_CIP_CODE_2_NAME),

    FINAL_CIP_CODE_2_NAME = if_else(
      fill_cip2_fields == TRUE,
      LCP2_DIGITS_NAME,
      FINAL_CIP_CODE_2_NAME
    ),
    FINAL_CIP_CLUSTER_CODE = if_else(
      fill_cip2_fields == TRUE,
      LCP2_LCIPPC_CD,
      FINAL_CIP_CLUSTER_CODE
    ),
    FINAL_CIP_CLUSTER_NAME = if_else(
      fill_cip2_fields == TRUE,
      LCP2_LCIPPC_NAME,
      FINAL_CIP_CLUSTER_NAME
    )
  ) %>%
  select(
    -LCP2_DIGITS_NAME,
    -LCP2_LCIPPC_CD,
    -LCP2_LCIPPC_NAME,
    -fill_cip2_fields
  )

# Preview the updated credentials table structure
credential_bgs_updated |> glimpse()
credential_bgs_updated_df <- credential_bgs_updated %>%
  as_tibble()


# str_pad function does not support tbl table.
credential_bgs_updated_df <- credential_bgs_updated_df %>%
  mutate(
    FINAL_CIP_CODE_4 = str_pad(
      FINAL_CIP_CODE_4,
      width = 4,
      side = "left",
      pad = "0"
    ),
    FINAL_CIP_CODE_2 = str_pad(
      FINAL_CIP_CODE_2,
      width = 2,
      side = "left",
      pad = "0"
    )
  )
# write it back to SQL after all the updates and collected.

dbWriteTable(
  con,
  SQL(glue::glue('"{my_schema}"."Credential_Non_Dup_BGS_IDs_r"')),
  credential_bgs_updated_df,
  overwrite = TRUE
)

credential_bgs_updated <- tbl(
  con,
  in_schema(my_schema, "Credential_Non_Dup_BGS_IDs_r")
)


credential_bgs_updated |> glimpse()
credential_bgs_updated |> tally()

## Validation check: look for any remaining missing values in the final output.
{
  tbl(con, "Credential_Non_Dup_BGS_IDs_r") %>%
    filter(is.na(FINAL_CIP_CODE_4_NAME))
  tbl(con, "Credential_Non_Dup_BGS_IDs_r") %>%
    filter(is.na(FINAL_CIP_CLUSTER_CODE))
}
# passed: no missing CIP names or cluster codes remain after the override and refill steps.
log_info(glue::glue(
  "Part 4: Updated Credential_Non_Dup_BGS_IDs_r with final CIPs: {credential_bgs_updated %>% tally() %>% pull()} rows"
))

## remove local tables
rm(
  credential_matched_cips_using_bgs,
  credential_unmatched_cips,
  credential_unmatched_cips_to_review,
  chk,
  credential_unmatched_cips_to_update
)

# ------------------------------------------------------------------------------
# Part 5: Update the BGS outcomes table with final CIP values
#
# Purpose:
# Write the final CIP decision back to the BGS outcomes table so the BGS source
# also reflects the outcome of the BGS/STP matching process.
#
# Why this step exists:
# The matching logic may decide that the final CIP should come from STP rather
# than the original BGS outcome record. This step stores that final choice,
# preserves comparison fields, and fills missing descriptive metadata.
#
# Main stages:
# - Part 5A: Join match results back to the BGS outcomes table.
# - Part 5B: Review unmatched programs and apply manual CIP overrides where
#   needed for consistency.
#
# Output:
# - T_BGS_Data_Final_for_OutcomesMatching (updated)
# ------------------------------------------------------------------------------

### Part 5A: Update with XWALK ----
log_info("Part 5: Updating BGS outcomes table with final CIP values")

{
  ## may want to make a backup copy of T_BGS_Data_Final_for_OutcomesMatching
  ## in case you want to make changes to the manual matching
  if (
    dbExistsTable(
      con,
      name = Id(
        schema = my_schema,
        table = "T_BGS_Data_Final_for_OutcomesMatching_bu_r"
      )
    )
  ) {
    dbRemoveTable(
      con,
      name = Id(
        schema = my_schema,
        table = "T_BGS_Data_Final_for_OutcomesMatching_bu_r"
      )
    )
  }
  dbExecute(
    con,
    glue::glue(
      "select * into [{my_schema}].T_BGS_Data_Final_for_OutcomesMatching_bu_r from [{my_schema}].T_BGS_Data_Final_for_OutcomesMatching_r"
    )
  )
}

## Fill in final CIPs with BGS_Matching_STP_Credential_PEN

## qry_T_BGS_Data_add_columns ----
## New: mimicking Credential_Non_Dup update code
## Add new columns to T_BGS_Data_Final_for_OutcomesMatching to store final CIP codes, STP CIPs, and matching metadata.
## These columns will be populated based on the matching results from BGS_Matching_STP_Credential_PEN.

new_cols <- c(
  "STP_CIP_CODE_4", # 4-digit STP CIP code for comparison
  "STP_CIP_CODE_4_NAME", # 4-digit STP CIP name for comparison
  "FINAL_CONSIDER_A_MATCH", # Flag indicating auto-matched records (high confidence)
  "FINAL_PROBABLE_MATCH", # Flag indicating manually reviewed matches (medium confidence)
  "USE_BGS_CIP", # Flag: "Yes" if BGS CIP was selected as final, "No" if STP CIP was selected
  "USE_STP_CIP", # Flag: "Yes" if STP CIP was selected as final (inverse of USE_BGS_CIP)
  "FINAL_CIP_CODE_4", # Final 4-digit CIP code (either BGS or STP based on matching logic)
  "FINAL_CIP_CODE_4_NAME", # Final 4-digit CIP name
  "FINAL_CIP_CODE_2", # Final 2-digit CIP code (aligned with 4-digit choice)
  "FINAL_CIP_CODE_2_NAME", # Final 2-digit CIP name
  "FINAL_CIP_CLUSTER_CODE", # Final CIP cluster code (from CIP2016 taxonomy)
  "FINAL_CIP_CLUSTER_NAME" # Final CIP cluster name (from CIP2016 taxonomy)
)

# Add new columns to the table
for (col in new_cols) {
  dbExecute(
    con,
    glue::glue(
      "ALTER TABLE [{my_schema}].T_BGS_Data_Final_for_OutcomesMatching_r ADD [{col}] VARCHAR(255) NULL"
    )
  )
}

# Load the updated table reference
t_bgs_updated <- tbl(
  con,
  in_schema(my_schema, "T_BGS_Data_Final_for_OutcomesMatching_r")
)
t_bgs_updated |> glimpse()

# Join matching results from BGS_Matching_STP_Credential_PEN to bring in final CIP decisions
t_bgs_updated <- t_bgs_updated %>%
  # Left join on STQU_ID to bring in matched CIP codes and flags
  # Rename source columns with "src_" prefix to avoid naming conflicts during updates
  left_join(
    bgs_matching_final %>%
      filter(!is.na(FINAL_CONSIDER_A_MATCH) & FINAL_CONSIDER_A_MATCH != "") %>%
      group_by(STQU_ID) %>%
      slice_min(order_by = YEAR, n = 1, with_ties = FALSE) %>%
      select(
        STQU_ID,
        FINAL_CIP_CODE_4,
        FINAL_CIP_CODE_4_NAME,
        FINAL_CIP_CODE_2,
        FINAL_CIP_CODE_2_NAME,
        FINAL_CIP_CLUSTER_CODE,
        FINAL_CIP_CLUSTER_NAME,
        USE_BGS_CIP,
        STP_FINAL_CIP_CODE_4,
        STP_FINAL_CIP_CODE_4_NAME,
        FINAL_CONSIDER_A_MATCH,
        FINAL_PROBABLE_MATCH
      ) %>%
      rename(
        src_FINAL_CIP_CODE_4 = FINAL_CIP_CODE_4,
        src_FINAL_CIP_CODE_4_NAME = FINAL_CIP_CODE_4_NAME,
        src_FINAL_CIP_CODE_2 = FINAL_CIP_CODE_2,
        src_FINAL_CIP_CODE_2_NAME = FINAL_CIP_CODE_2_NAME,
        src_FINAL_CIP_CLUSTER_CODE = FINAL_CIP_CLUSTER_CODE,
        src_FINAL_CIP_CLUSTER_NAME = FINAL_CIP_CLUSTER_NAME,
        src_USE_BGS_CIP = USE_BGS_CIP,
        src_STP_CIP_CODE_4 = STP_FINAL_CIP_CODE_4,
        src_STP_CIP_CODE_4_NAME = STP_FINAL_CIP_CODE_4_NAME,
        src_FINAL_CONSIDER_A_MATCH = FINAL_CONSIDER_A_MATCH,
        src_FINAL_PROBABLE_MATCH = FINAL_PROBABLE_MATCH
      ),
    by = c("STQU_ID" = "STQU_ID")
  )

## qry_update_T_BGS_Data_CIP_matches_step1 ----
## New: mimicking Credential_Non_Dup update code
## Step 1: Update records that were auto-matched (FINAL_CONSIDER_A_MATCH = "Yes")
## These are high-confidence matches where institution, year, and CIP aligned perfectly.
## Populate final CIP columns with the matched values from the source table.
t_bgs_updated <- t_bgs_updated %>%
  # Identify records to update: those with non-empty FINAL_CONSIDER_A_MATCH from source
  mutate(
    step1_update = !is.na(src_FINAL_CONSIDER_A_MATCH) &
      src_FINAL_CONSIDER_A_MATCH != "",

    # Update final CIP columns with matched values for auto-matched records
    FINAL_CIP_CODE_4 = if_else(
      step1_update == TRUE,
      src_FINAL_CIP_CODE_4,
      FINAL_CIP_CODE_4
    ),
    FINAL_CIP_CODE_4_NAME = if_else(
      step1_update == TRUE,
      src_FINAL_CIP_CODE_4_NAME,
      FINAL_CIP_CODE_4_NAME
    ),
    FINAL_CIP_CODE_2 = if_else(
      step1_update == TRUE,
      src_FINAL_CIP_CODE_2,
      FINAL_CIP_CODE_2
    ),
    FINAL_CIP_CODE_2_NAME = if_else(
      step1_update == TRUE,
      src_FINAL_CIP_CODE_2_NAME,
      FINAL_CIP_CODE_2_NAME
    ),
    FINAL_CIP_CLUSTER_CODE = if_else(
      step1_update == TRUE,
      src_FINAL_CIP_CLUSTER_CODE,
      FINAL_CIP_CLUSTER_CODE
    ),
    FINAL_CIP_CLUSTER_NAME = if_else(
      step1_update == TRUE,
      src_FINAL_CIP_CLUSTER_NAME,
      FINAL_CIP_CLUSTER_NAME
    ),
    # Set flags indicating CIP source and match type
    USE_BGS_CIP = if_else(step1_update == TRUE, src_USE_BGS_CIP, USE_BGS_CIP),
    STP_CIP_CODE_4 = if_else(
      step1_update == TRUE,
      src_STP_CIP_CODE_4,
      STP_CIP_CODE_4
    ),
    STP_CIP_CODE_4_NAME = if_else(
      step1_update == TRUE,
      src_STP_CIP_CODE_4_NAME,
      STP_CIP_CODE_4_NAME
    ),
    FINAL_CONSIDER_A_MATCH = if_else(
      step1_update == TRUE,
      src_FINAL_CONSIDER_A_MATCH,
      FINAL_CONSIDER_A_MATCH
    ),
    FINAL_PROBABLE_MATCH = if_else(
      step1_update == TRUE,
      src_FINAL_PROBABLE_MATCH,
      FINAL_PROBABLE_MATCH
    )
  )

t_bgs_updated |> glimpse()

## qry_update_T_BGS_Data_CIP_matches_step2 ----
## New: mimicking Credential_Non_Dup update code
## Step 2: Update records that were manually reviewed (FINAL_PROBABLE_MATCH = "Yes")
## These are medium-confidence matches where institution and year matched but CIPs differed.
## Only update records that are still completely empty (to avoid overwriting auto-matches).
## This ensures auto-matches take priority over manual reviews in case of conflicts.
t_bgs_updated <- t_bgs_updated %>%
  # Identify target records: those still completely empty in final columns
  mutate(
    step2_target_empty = is.na(FINAL_CIP_CODE_4) &
      is.na(FINAL_CIP_CODE_4_NAME) &
      is.na(FINAL_CIP_CODE_2) &
      is.na(FINAL_CIP_CODE_2_NAME) &
      is.na(FINAL_CIP_CLUSTER_CODE) &
      is.na(FINAL_CIP_CLUSTER_NAME) &
      is.na(USE_BGS_CIP) &
      is.na(STP_CIP_CODE_4) &
      is.na(STP_CIP_CODE_4_NAME) &
      is.na(FINAL_PROBABLE_MATCH),

    # Identify source records: those with non-empty FINAL_PROBABLE_MATCH
    step2_source_ok = !is.na(src_FINAL_PROBABLE_MATCH) &
      src_FINAL_PROBABLE_MATCH != "",
    step2_update = step2_target_empty == TRUE & step2_source_ok == TRUE,

    # Update final CIP columns with manually reviewed values for eligible records
    FINAL_CIP_CODE_4 = if_else(
      step2_update == TRUE,
      src_FINAL_CIP_CODE_4,
      FINAL_CIP_CODE_4
    ),
    FINAL_CIP_CODE_4_NAME = if_else(
      step2_update == TRUE,
      src_FINAL_CIP_CODE_4_NAME,
      FINAL_CIP_CODE_4_NAME
    ),
    FINAL_CIP_CODE_2 = if_else(
      step2_update == TRUE,
      src_FINAL_CIP_CODE_2,
      FINAL_CIP_CODE_2
    ),
    FINAL_CIP_CODE_2_NAME = if_else(
      step2_update == TRUE,
      src_FINAL_CIP_CODE_2_NAME,
      FINAL_CIP_CODE_2_NAME
    ),
    FINAL_CIP_CLUSTER_CODE = if_else(
      step2_update == TRUE,
      src_FINAL_CIP_CLUSTER_CODE,
      FINAL_CIP_CLUSTER_CODE
    ),
    FINAL_CIP_CLUSTER_NAME = if_else(
      step2_update == TRUE,
      src_FINAL_CIP_CLUSTER_NAME,
      FINAL_CIP_CLUSTER_NAME
    ),
    # Set flags for manually reviewed records
    USE_BGS_CIP = if_else(step2_update == TRUE, src_USE_BGS_CIP, USE_BGS_CIP),
    STP_CIP_CODE_4 = if_else(
      step2_update == TRUE,
      src_STP_CIP_CODE_4,
      STP_CIP_CODE_4
    ),
    STP_CIP_CODE_4_NAME = if_else(
      step2_update == TRUE,
      src_STP_CIP_CODE_4_NAME,
      STP_CIP_CODE_4_NAME
    ),
    FINAL_PROBABLE_MATCH = if_else(
      step2_update == TRUE,
      src_FINAL_PROBABLE_MATCH,
      FINAL_PROBABLE_MATCH
    )
  )

## qry_update_T_BGS_Data_CIP_matches_step3 ----
## New: update USE_STP_CIP with USE_BGS_CIP
## Step 3: Set USE_STP_CIP flag as the inverse of USE_BGS_CIP
## This provides a clear indicator of whether STP CIP was selected as the final code.
t_bgs_updated <- t_bgs_updated %>%
  mutate(
    USE_STP_CIP = case_when(
      USE_BGS_CIP == "Yes" ~ "No", # If BGS CIP was used, STP was not
      USE_BGS_CIP == "No" ~ "Yes", # If STP CIP was used, BGS was not
      TRUE ~ USE_STP_CIP # Keep existing value if USE_BGS_CIP is NA or other
    )
  )

## qry_update_T_BGS_Data_CIP_matches_step4 ----
## New: drop USE_BGS_CIP
## Step 4: Clean up temporary columns used during updates
## Remove USE_BGS_CIP (since USE_STP_CIP provides the inverse), source columns, and helper flags.
t_bgs_updated <- t_bgs_updated %>%
  select(
    -USE_BGS_CIP, # No longer needed after setting USE_STP_CIP
    -starts_with("src_"), # Temporary source columns from join
    -step1_update, # Helper flag from step 1
    -step2_target_empty, # Helper flags from step 2
    -step2_source_ok,
    -step2_update
  )

### Part 5B: Update unmatched CIPs ----
## Fill in remaining final CIPs with BGS CIP from T_BGS_Data_Final_for_OutcomesMatching
## qry_update_remaining_BGS_CIPs_in_T_BGS_Data_step1 ----
## New: mimicking Credential_Non_Dup update code
## Remaining Step 1: Fallback to original BGS CIPs for records that were not matched
## For unmatched records (no FINAL_CONSIDER_A_MATCH or FINAL_PROBABLE_MATCH), use the original BGS CIP codes as final.
## This ensures all records have final CIP assignments, defaulting to BGS when no STP match exists.
t_bgs_updated <- t_bgs_updated %>%
  mutate(
    fill_remaining = is.na(FINAL_CIP_CODE_4) &
      is.na(FINAL_CIP_CODE_2) &
      is.na(FINAL_CONSIDER_A_MATCH) &
      is.na(FINAL_PROBABLE_MATCH),

    # Use original BGS CIP codes from this table as final for unmatched records
    FINAL_CIP_CODE_4 = if_else(
      fill_remaining == TRUE,
      CIP_4DIGIT_NO_PERIOD,
      FINAL_CIP_CODE_4
    ),
    FINAL_CIP_CODE_4_NAME = if_else(
      fill_remaining == TRUE,
      CIP4DIG_NAME,
      FINAL_CIP_CODE_4_NAME
    ),
    FINAL_CIP_CODE_2 = if_else(
      fill_remaining == TRUE,
      CIP2DIG,
      FINAL_CIP_CODE_2
    ),
    FINAL_CIP_CODE_2_NAME = if_else(
      fill_remaining == TRUE,
      CIP2DIG_NAME,
      FINAL_CIP_CODE_2_NAME
    ),
    # Indicate that STP CIP was not used due to no match
    USE_STP_CIP = if_else(
      fill_remaining == TRUE,
      "No because no match",
      USE_STP_CIP
    )
  ) %>%
  select(-fill_remaining)

## qry_update_remaining_BGS_CIPs_in_T_BGS_Data_step2 ----
## Remaining Step 2: Fill missing cluster information for all records
## Join to CIP_2 lookup table to populate FINAL_CIP_CLUSTER_CODE and FINAL_CIP_CLUSTER_NAME
## based on the final 2-digit CIP code. This ensures complete CIP taxonomy information.
t_bgs_updated <- t_bgs_updated %>%
  left_join(
    cip_2_tbl %>%
      select(
        LCP2_CD,
        LCP2_LCIPPC_CD,
        LCP2_LCIPPC_NAME
      ),
    by = c("FINAL_CIP_CODE_2" = "LCP2_CD")
  ) %>%
  mutate(
    # Only fill cluster info where it's still missing
    fill_cluster = is.na(FINAL_CIP_CLUSTER_CODE) &
      is.na(FINAL_CIP_CLUSTER_NAME),
    FINAL_CIP_CLUSTER_CODE = if_else(
      fill_cluster == TRUE,
      LCP2_LCIPPC_CD,
      FINAL_CIP_CLUSTER_CODE
    ),
    FINAL_CIP_CLUSTER_NAME = if_else(
      fill_cluster == TRUE,
      LCP2_LCIPPC_NAME,
      FINAL_CIP_CLUSTER_NAME
    )
  ) %>%
  select(
    -fill_cluster, # Remove helper flag
    -LCP2_LCIPPC_CD, # Remove temporary lookup columns
    -LCP2_LCIPPC_NAME
  )

t_bgs_updated |> glimpse()

# ------------------------------------------------------------------------------
# Part 5B: Review and update unmatched BGS programs using STP-linked CIP values
#
# Purpose:
# Handle BGS records that did not receive a case-level match in the earlier
# BGS/STP matching workflow. For selected unmatched programs, use evidence from
# matched records to assign a more appropriate final CIP.
#
# Why this step exists:
# Some BGS programs remain unmatched and therefore keep their original BGS CIP.
# However, the same PROGRAM may appear elsewhere in matched cases where STP CIP
# was clearly the better final choice. This step identifies those cases,
# exports them for optional analyst review, and applies approved updates.
#
# Workflow:
# 1. Build an audit table of matched BGS program combinations that already use
#    STP CIP.
# 2. Build an audit table of unmatched BGS program combinations.
# 3. Create a review table showing unmatched programs that have evidence from
#    matched cases suggesting a replacement final CIP.
# 4. Export that review table to CSV.
# 5. If a reviewed CSV exists, read it back and use it as the update table.
#    Otherwise, build a draft update table automatically for PROGRAM values that
#    map to one unique replacement CIP.
# 6. Update unmatched rows in t_bgs_updated and refill descriptive fields from
#    the official CIP lookup tables.
#
# Important assumptions:
# - The final update step joins by PROGRAM only.
# - Because of that, the update table must contain at most one row per PROGRAM.
# - If one PROGRAM maps to multiple possible replacement CIPs, that PROGRAM
#   must be reviewed manually before it can be updated safely.
# ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# 5B.1 Build audit table: matched BGS program combinations that use STP CIP
#
# This is the BGS-side equivalent of the matched-program evidence used in Part 4B.
# It identifies programs where the final decision was already to use STP CIP.
# Those matched examples are the evidence base for updating similar unmatched rows.
# ------------------------------------------------------------------------------

T_BGS_Data_Matched_CIPS_Using_STP <- t_bgs_updated %>%
  group_by(
    INSTITUTION_CODE,
    CPC,
    PROGRAM,
    STP_CIP_CODE_4,
    STP_CIP_CODE_4_NAME,
    CIP_4DIGIT_NO_PERIOD,
    CIP4DIG_NAME,
    CIP2DIG,
    CIP2DIG_NAME,
    FINAL_CIP_CODE_4,
    FINAL_CIP_CODE_4_NAME,
    FINAL_CIP_CODE_2,
    FINAL_CIP_CODE_2_NAME,
    FINAL_CIP_CLUSTER_CODE,
    FINAL_CIP_CLUSTER_NAME,
    FINAL_CONSIDER_A_MATCH,
    FINAL_PROBABLE_MATCH,
    USE_STP_CIP
  ) %>%
  summarise(n = n(), .groups = "drop") %>%
  filter(USE_STP_CIP == "Yes")

# ------------------------------------------------------------------------------
# 5B.2 Build audit table: unmatched BGS program combinations
#
# These are records that did not receive a match-based CIP decision earlier in
# the workflow and therefore still show USE_STP_CIP = "No because no match".
# ------------------------------------------------------------------------------

T_BGS_Data_Unmatched_CIPS <- t_bgs_updated %>%
  group_by(
    INSTITUTION_CODE,
    CPC,
    PROGRAM,
    CIP_4DIGIT_NO_PERIOD,
    STP_CIP_CODE_4,
    STP_CIP_CODE_4_NAME,
    CIP4DIG_NAME,
    CIP2DIG,
    CIP2DIG_NAME,
    FINAL_CIP_CODE_4,
    FINAL_CIP_CODE_4_NAME,
    FINAL_CIP_CODE_2,
    FINAL_CIP_CODE_2_NAME,
    FINAL_CIP_CLUSTER_CODE,
    FINAL_CIP_CLUSTER_NAME,
    FINAL_CONSIDER_A_MATCH,
    FINAL_PROBABLE_MATCH,
    USE_STP_CIP
  ) %>%
  summarise(n = n(), .groups = "drop") %>%
  filter(USE_STP_CIP == "No because no match")

# ------------------------------------------------------------------------------
# 5B.3 Create review table for unmatched BGS programs
#
# Strategy:
# Join unmatched programs to matched programs that already use STP CIP.
# At the review stage, use a safer program identity:
#   INSTITUTION_CODE + CPC + PROGRAM
# rather than PROGRAM alone.
#
# This helps avoid accidental cross-institution or cross-CPC reuse of a CIP.
# The final update table is collapsed to PROGRAM only later, but only if the
# recommended CIP is unique for that PROGRAM.
# ------------------------------------------------------------------------------

T_BGS_Data_Unmatched_CIPS_to_review <- T_BGS_Data_Unmatched_CIPS %>%
  select(-STP_CIP_CODE_4, -STP_CIP_CODE_4_NAME) %>%
  left_join(
    T_BGS_Data_Matched_CIPS_Using_STP %>%
      distinct(
        INSTITUTION_CODE,
        CPC,
        PROGRAM,
        CIP_4DIGIT_NO_PERIOD,
        MATCHED_STP_CIP_CODE_4 = STP_CIP_CODE_4,
        MATCHED_STP_CIP_CODE_4_NAME = STP_CIP_CODE_4_NAME,
        MATCHED_FINAL_CIP_4 = FINAL_CIP_CODE_4,
        MATCHED_FINAL_CIP_4_NAME = FINAL_CIP_CODE_4_NAME,
        MATCHED_FINAL_CIP_2 = FINAL_CIP_CODE_2,
        MATCHED_FINAL_CIP_2_NAME = FINAL_CIP_CODE_2_NAME
      ),
    by = c("INSTITUTION_CODE", "CPC", "PROGRAM", "CIP_4DIGIT_NO_PERIOD")
  ) %>%
  mutate(
    FOUND_MATCHED_STP_EXAMPLE = if_else(
      !is.na(MATCHED_STP_CIP_CODE_4),
      "Yes",
      NA_character_
    ),
    REPLACEMENT_DIFFERS_FROM_CURRENT_BGS = if_else(
      MATCHED_STP_CIP_CODE_4 != CIP_4DIGIT_NO_PERIOD, #? WHY
      "Yes",
      NA_character_
    )
  ) %>%
  filter(
    FOUND_MATCHED_STP_EXAMPLE == "Yes",
    REPLACEMENT_DIFFERS_FROM_CURRENT_BGS == "Yes"
  ) %>%
  select(-PROGRAM, everything(), PROGRAM) %>%
  arrange(FINAL_CIP_CODE_4) # WHY?


## review the outcomes credentials matched to the unmatched programs
## filter out programs with more than one match
## if any should be updated - update the custom query below
chk <- T_BGS_Data_Unmatched_CIPS_to_review %>%
  group_by(
    INSTITUTION_CODE,
    CPC,
    PROGRAM,
    CIP_4DIGIT_NO_PERIOD,
    CIP4DIG_NAME
  ) %>%
  summarize(
    STP_CIP_NAME = paste(MATCHED_STP_CIP_CODE_4_NAME, collapse = "\n "),
    STP_CIP_CODE = paste(MATCHED_STP_CIP_CODE_4, collapse = "\n "),
    count = n()
  ) %>%
  filter(count == 1)

# ------------------------------------------------------------------------------
# 5B.4 Export review table for analyst review
#
# Analysts can inspect this file and optionally create a reviewed version with
# exactly these columns:
#   PROGRAM, FINAL_CIP_CODE_4, FINAL_CIP_CODE_2
#
# Suggested manual workflow:
# - Review whether the matched STP-linked CIP should be applied to unmatched
#   rows for the same PROGRAM.
# - Keep only approved rows in the final reviewed file.
# ------------------------------------------------------------------------------

# review_file <- "T_BGS_Data_Unmatched_CIPS_to_review.csv"
# update_file <- "T_BGS_Data_Unmatched_CIPS_to_update.csv"

# T_BGS_Data_Unmatched_CIPS_to_review <- T_BGS_Data_Unmatched_CIPS_to_review |>
#   collect()
# # 320 rows
# write_csv(T_BGS_Data_Unmatched_CIPS_to_review, review_file)

# # ------------------------------------------------------------------------------
# # 5B.5 Read reviewed update table if available; otherwise build a safe draft
# #
# # If the reviewed update file exists, use it.
# # Otherwise, automatically draft an update table for PROGRAM values that map to
# # exactly one unique replacement 4-digit and 2-digit CIP.
# #
# # This keeps the script runnable even before manual review is complete, while
# # avoiding unsafe many-to-one PROGRAM mappings.
# # ------------------------------------------------------------------------------

# if (file.exists(update_file)) {
#   # --------------------------------------------------------------------------
#   # Preferred path: use the reviewed update file
#   # --------------------------------------------------------------------------
#   T_BGS_Data_Unmatched_CIPS_to_update <- read_csv(
#     update_file,
#     show_col_types = FALSE
#   ) %>%
#     transmute(
#       PROGRAM = as.character(PROGRAM),
#       FINAL_CIP_CODE_4 = as.character(FINAL_CIP_CODE_4),
#       FINAL_CIP_CODE_2 = as.character(FINAL_CIP_CODE_2)
#     )

#   dup_programs <- T_BGS_Data_Unmatched_CIPS_to_update %>%
#     count(PROGRAM) %>%
#     filter(n > 1)

#   if (nrow(dup_programs) > 0) {
#     T_BGS_Data_Unmatched_CIPS_to_update <- T_BGS_Data_Unmatched_CIPS_to_update %>%
#       group_by(PROGRAM) %>%
#       summarise(
#         FINAL_CIP_CODE_4 = first(na.omit(MATCHED_FINAL_CIP_4)),
#         FINAL_CIP_CODE_2 = first(na.omit(MATCHED_FINAL_CIP_2)),
#         n_cip4 = n_distinct(MATCHED_FINAL_CIP_4, na.rm = TRUE),
#         n_cip2 = n_distinct(MATCHED_FINAL_CIP_2, na.rm = TRUE),
#         .groups = "drop"
#       ) %>%
#       filter(n_cip4 == 1, n_cip2 == 1) %>%
#       select(PROGRAM, FINAL_CIP_CODE_4, FINAL_CIP_CODE_2)
#   }
# } else {
#   # --------------------------------------------------------------------------
#   # Fallback path: build an automatic draft update table
#   #
#   # Only keep PROGRAM values that map to one unique replacement CIP pair.
#   # If a PROGRAM maps to more than one candidate CIP, do not auto-update it.
#   # Those cases should be handled through the reviewed CSV path.
#   # --------------------------------------------------------------------------

#   T_BGS_Data_Unmatched_CIPS_to_update <- T_BGS_Data_Unmatched_CIPS_to_review %>%
#     group_by(PROGRAM) %>%
#     summarise(
#       FINAL_CIP_CODE_4 = first(na.omit(MATCHED_FINAL_CIP_4)),
#       FINAL_CIP_CODE_2 = first(na.omit(MATCHED_FINAL_CIP_2)),
#       n_cip4 = n_distinct(MATCHED_FINAL_CIP_4, na.rm = TRUE),
#       n_cip2 = n_distinct(MATCHED_FINAL_CIP_2, na.rm = TRUE),
#       .groups = "drop"
#     ) %>%
#     filter(n_cip4 == 1, n_cip2 == 1) %>%
#     select(PROGRAM, FINAL_CIP_CODE_4, FINAL_CIP_CODE_2)

#   # Save the automatic draft so analysts can review or override it later.
#   write_csv(T_BGS_Data_Unmatched_CIPS_to_update, update_file)
# }

## make a table with PROGRAM_DESCRIPTIONs decided to update
T_BGS_Data_Unmatched_CIPS_to_update <- tibble::tribble(
  ~PROGRAM                                                                                             , ~FINAL_CIP_CODE_4 , ~FINAL_CIP_CODE_2 ,
  "Bachelor of Applied Science - Mechatronic Systems Engineering Major"                                , "1442"            , "14"              ,
  "Bachelor of Applied Science In Chemical Engineering"                                                , "1407"            , "14"              ,
  "Bachelor of Applied Science In Chemical Engineering Minor In Commerce"                              , "1407"            , "14"              ,
  "Bachelor of Applied Science In Chemical Engineering Option in Biology"                              , "1407"            , "14"              ,
  "Bachelor of Environment - Resource and Environmental Management Major"                              , "0301"            , "03"              ,
  "Bachelor of Environment - Resource and Environmental Management Major, First Nations Studies Minor" , "0301"            , "03"              ,
  "Bachelor of Environment - Resource and Environmental Management Major, Geography Minor"             , "0301"            , "03"              ,
  "Bachelor of Science - Biomedical Physiology Major"                                                  , "2609"            , "26"              ,
  "Bachelor of Science in Applied Psychology"                                                          , "4228"            , "42"
)

# Write the update table to SQL for efficient joining

dbWriteTable(
  con,
  "T_BGS_Data_Unmatched_CIPS_to_update_r",
  T_BGS_Data_Unmatched_CIPS_to_update,
  overwrite = TRUE
)

T_BGS_Data_Unmatched_CIPS_to_update <- tbl(
  con,
  in_schema(my_schema, "T_BGS_Data_Unmatched_CIPS_to_update_r")
)


# ------------------------------------------------------------------------------
# 5B.6 Validate update table
#
# The later join uses PROGRAM only, so PROGRAM must be unique in the final
# update table. Stop if duplicates remain.
# ------------------------------------------------------------------------------

dup_programs <- T_BGS_Data_Unmatched_CIPS_to_update %>%
  count(PROGRAM) %>%
  filter(n > 1) |>
  collect()

if (nrow(dup_programs) > 0) {
  stop(
    "T_BGS_Data_Unmatched_CIPS_to_update has duplicate PROGRAM values. Resolve before updating."
  )
}

# Optional check: review programs that were excluded from the automatic draft
# because they mapped to multiple possible replacement CIPs.
T_BGS_Data_Unmatched_CIPS_ambiguous <- T_BGS_Data_Unmatched_CIPS_to_review %>%
  group_by(PROGRAM) %>%
  summarise(
    n_cip4 = n_distinct(MATCHED_FINAL_CIP_4, na.rm = TRUE),
    n_cip2 = n_distinct(MATCHED_FINAL_CIP_2, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  filter(n_cip4 > 1 | n_cip2 > 1)

# ------------------------------------------------------------------------------
# 5B.7 Apply unmatched-program CIP updates
#
# Business rule:
# Only update rows that were never resolved in the earlier matching workflow:
#   - FINAL_CONSIDER_A_MATCH is NA
#   - FINAL_PROBABLE_MATCH is NA
#
# After replacing the final CIP codes, clear the name and cluster fields so
# they can be refilled from the official lookup tables below.
# ------------------------------------------------------------------------------

t_bgs_updated <- t_bgs_updated %>%
  left_join(
    T_BGS_Data_Unmatched_CIPS_to_update %>%
      select(
        PROGRAM,
        upd_FINAL_CIP_CODE_4 = FINAL_CIP_CODE_4,
        upd_FINAL_CIP_CODE_2 = FINAL_CIP_CODE_2
      ),
    by = "PROGRAM"
  )

t_bgs_updated <- t_bgs_updated %>%
  mutate(
    needs_unmatched_update = is.na(FINAL_CONSIDER_A_MATCH) &
      is.na(FINAL_PROBABLE_MATCH),

    FINAL_CIP_CODE_4 = if_else(
      needs_unmatched_update == TRUE & !is.na(upd_FINAL_CIP_CODE_4),
      upd_FINAL_CIP_CODE_4,
      FINAL_CIP_CODE_4
    ),
    FINAL_CIP_CODE_2 = if_else(
      needs_unmatched_update == TRUE & !is.na(upd_FINAL_CIP_CODE_2),
      upd_FINAL_CIP_CODE_2,
      FINAL_CIP_CODE_2
    ),

    # Clear descriptive fields so they can be rebuilt from official lookups.
    FINAL_CIP_CODE_4_NAME = if_else(
      needs_unmatched_update == TRUE & !is.na(upd_FINAL_CIP_CODE_4),
      NA_character_,
      FINAL_CIP_CODE_4_NAME
    ),
    FINAL_CIP_CODE_2_NAME = if_else(
      needs_unmatched_update == TRUE & !is.na(upd_FINAL_CIP_CODE_2),
      NA_character_,
      FINAL_CIP_CODE_2_NAME
    ),
    FINAL_CIP_CLUSTER_CODE = if_else(
      needs_unmatched_update == TRUE & !is.na(upd_FINAL_CIP_CODE_2),
      NA_character_,
      FINAL_CIP_CLUSTER_CODE
    ),
    FINAL_CIP_CLUSTER_NAME = if_else(
      needs_unmatched_update == TRUE & !is.na(upd_FINAL_CIP_CODE_2),
      NA_character_,
      FINAL_CIP_CLUSTER_NAME
    )
  ) %>%
  select(-upd_FINAL_CIP_CODE_4, -upd_FINAL_CIP_CODE_2, -needs_unmatched_update)

# ------------------------------------------------------------------------------
# 5B.8 Refill final 4-digit CIP name from official lookup
# ------------------------------------------------------------------------------

t_bgs_updated <- t_bgs_updated %>%
  left_join(
    cip_4_tbl %>%
      select(LCP4_CD, LCP4_CIP_4DIGITS_NAME),
    by = c("FINAL_CIP_CODE_4" = "LCP4_CD")
  ) %>%
  mutate(
    FINAL_CIP_CODE_4_NAME = if_else(
      is.na(FINAL_CIP_CODE_4_NAME),
      LCP4_CIP_4DIGITS_NAME,
      FINAL_CIP_CODE_4_NAME
    )
  ) %>%
  select(-LCP4_CIP_4DIGITS_NAME)

# ------------------------------------------------------------------------------
# 5B.9 Refill final 2-digit CIP name and cluster fields from official lookup
#
# Use a helper flag so the same "was missing before refill" condition is applied
# consistently to all related fields.
# ------------------------------------------------------------------------------

t_bgs_updated <- t_bgs_updated %>%
  left_join(
    cip_2_tbl %>%
      select(
        LCP2_CD,
        LCP2_DIGITS_NAME,
        LCP2_LCIPPC_CD,
        LCP2_LCIPPC_NAME
      ),
    by = c("FINAL_CIP_CODE_2" = "LCP2_CD")
  ) %>%
  mutate(
    fill_cip2_fields = is.na(FINAL_CIP_CODE_2_NAME),

    FINAL_CIP_CODE_2_NAME = if_else(
      fill_cip2_fields == TRUE,
      LCP2_DIGITS_NAME,
      FINAL_CIP_CODE_2_NAME
    ),
    FINAL_CIP_CLUSTER_CODE = if_else(
      fill_cip2_fields == TRUE,
      LCP2_LCIPPC_CD,
      FINAL_CIP_CLUSTER_CODE
    ),
    FINAL_CIP_CLUSTER_NAME = if_else(
      fill_cip2_fields == TRUE,
      LCP2_LCIPPC_NAME,
      FINAL_CIP_CLUSTER_NAME
    )
  ) %>%
  select(
    -LCP2_DIGITS_NAME,
    -LCP2_LCIPPC_CD,
    -LCP2_LCIPPC_NAME,
    -fill_cip2_fields
  )

# ------------------------------------------------------------------------------
# 5B.10 Optional summary checks
#
# These checks help analysts see:
# - how many candidate review rows were exported,
# - how many PROGRAM values made it into the final update table,
# - and how many ambiguous PROGRAM values still need manual review.
# ------------------------------------------------------------------------------

list(
  n_review_rows = nrow(T_BGS_Data_Unmatched_CIPS_to_review),
  n_update_programs = nrow(T_BGS_Data_Unmatched_CIPS_to_update),
  n_ambiguous_programs = nrow(T_BGS_Data_Unmatched_CIPS_ambiguous)
)

target_name <- "T_BGS_Data_Final_for_OutcomesMatching_r"
temp_name <- "T_BGS_Data_Final_for_OutcomesMatching_temp"

if (dbExistsTable(con, Id(schema = my_schema, table = temp_name))) {
  dbRemoveTable(con, Id(schema = my_schema, table = temp_name))
}

t_bgs_updated <- t_bgs_updated %>%
  compute(
    name = Id(schema = my_schema, table = temp_name),
    temporary = FALSE
  )

if (dbExistsTable(con, Id(schema = my_schema, table = target_name))) {
  dbRemoveTable(con, Id(schema = my_schema, table = target_name))
}
dbExecute(
  con,
  glue::glue("EXEC sp_rename '{my_schema}.{temp_name}', '{target_name}'")
)

t_bgs_updated <- tbl(con, in_schema(my_schema, target_name))

## check for blanks
{
  t_bgs_updated %>%
    filter(is.na(FINAL_CIP_CODE_4_NAME))
  t_bgs_updated %>%
    filter(is.na(FINAL_CIP_CLUSTER_CODE))
}
# passed - no blanks in final CIP name or cluster code
log_info(glue::glue(
  "Part 5: Updated T_BGS_Data_Final_for_OutcomesMatching_r: {t_bgs_updated %>% tally() %>% pull()} rows"
))

# ---- Clean up ----

## remove backup tables
dbRemoveTable(con, "BGS_Matching_STP_Credential_PEN_bu_r")
dbRemoveTable(con, "Credential_Non_Dup_BGS_IDs_bu_r")
dbRemoveTable(con, "T_BGS_Data_Final_for_OutcomesMatching_bu_r")
log_info("Removed backup tables")

dbDisconnect(con)
log_info("Disconnected from SQL Server")

log_info("==== 02a-bgs-program-matching.R COMPLETE ====")
