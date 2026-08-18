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
# APPSO program CIP alignment
#
# Purpose:
# Standardize and clean the CIP codes for APPSO (Apprenticeship) credentials
# in the Credential_Non_Dup table. APPSO credentials use STP CIP codes directly
# (no BGS survey matching is available for apprenticeship records), so the
# workflow here is a one-way normalization from raw 6-digit STP CIPs to
# validated 4-digit and 2-digit codes using INFOWARE lookup tables.
#
# High-level workflow:
# 1. Connect to the database and load source/lookup table references.
# 2. Create a cleaning table grouped by distinct STP CIP values for APPSO records.
# 3. Fix malformed CIP values (missing trailing/leading zeros).
# 4. Match CIPs to INFOWARE 6-digit lookup (exact, then partial fallbacks).
# 5. Apply "general program" rule (.00 -> 01) for unmatched 4-digit codes.
# 6. Add 4-digit and 2-digit CIP descriptive names.
# 7. Join cleaned CIPs back to the full APPSO credential data.
# 8. Materialize the final tables to SQL Server.
#
# Main outputs:
# - Credential_Non_Dup_APPSO_IDs_r
# - Credential_Non_Dup_STP_APPSO_Cleaning_r
#
# Notes
# -----
# - This script assumes the source tables already exist in the database.
# - The objects below are lazy dbplyr tables unless collect() or compute() is used.
# - Where SQL used UPDATE statements, this translation uses sequential mutate() /
#   join() steps to create the same result set.
# - Some remote databases need compute() between steps for performance or to avoid
#   repeating large subqueries. Those calls are left commented for now.
# ==============================================================================

library(dplyr)
library(dbplyr)
library(DBI)
library(odbc)
library(config)
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

## ----------------------------------------------------------
## Reasons for change, other notes
## ----------------------------------------------------------
## compute() with an in_schema()/Id() target does not honour
## overwrite = TRUE, so a rerun after a completed (or partially
## completed) run fails with "There is already an object named ...".
## This helper centralizes the drop-first guard the
## materializations in this script use: pre-check that the target
## exists, drop it if it does, then compute.
compute_overwrite <- function(data, con, schema, table) {
  target <- Id(schema = schema, table = table)
  if (dbExistsTable(con, target)) {
    dbRemoveTable(con, target)
    log_info(glue::glue("Dropped existing {schema}.{table} before compute"))
  }
  compute(data, name = target, temporary = FALSE)
}

log_info("==== 02a-appso-programs.R START ====")

# ---- Configure database connection -------------------------------------------
db_config <- config::get("decimal")
my_schema <- config::get("myschema")

con <- dbConnect(
  odbc(),
  Driver = db_config$driver,
  Server = db_config$server,
  Database = db_config$database,
  Trusted_Connection = "True"
)
log_info("Connected to SQL Server database")


# ---- Read in INFOWARE tables ----
# Note: These tables should be loaded by 'R/load-infoware-lookups.R'
# source("R/load-infoware-lookups.R")
# We check for their existence and proceed.

# ---- Reference source tables -------------------------------------------------
# Adjust schema/table resolution if your environment stores these elsewhere.
# All tables below are lazy dbplyr references; queries are executed only when
# collect() or compute() is called.

credential_non_dup <- tbl(con, in_schema(my_schema, "Credential_Non_Dup_r")) %>%
  rename_with(toupper)
# the following steps need 'load-infoware-lookup.R'
infoware_l_cip_2digits_cip2016 <- tbl(
  con,
  in_schema(my_schema, "INFOWARE_L_CIP_2DIGITS_CIP2016")
)
infoware_l_cip_4digits_cip2016 <- tbl(
  con,
  in_schema(my_schema, "INFOWARE_L_CIP_4DIGITS_CIP2016")
)
infoware_l_cip_6digits_cip2016 <- tbl(
  con,
  in_schema(my_schema, "INFOWARE_L_CIP_6DIGITS_CIP2016")
)
log_info(
  "Loaded lazy table references: Credential_Non_Dup_r, INFOWARE CIP lookup tables (2/4/6-digit)"
)

# ---- 1) SQL: create Credential_Non_Dup_STP_APPSO_Cleaning -------------------
# WHAT: Aggregate APPSO credentials by distinct CIP code to create a compact
#       cleaning table. This reduces the number of rows to process from the full
#       credential table to just the unique CIP values.
# WHY:  The cleaning logic (steps 2-10) operates on CIP codes, not individual
#       records. Working on distinct CIPs is much faster and the results are
#       joined back to the full table in step 11.

credential_non_dup_stp_appso_cleaning <- credential_non_dup %>%
  filter(OUTCOMES_CRED == "APPSO") %>%
  count(PSI_CREDENTIAL_CIP, OUTCOMES_CRED, name = "Expr1")

log_info(glue::glue(
  "Step 1: Created APPSO cleaning table (distinct CIPs): {credential_non_dup_stp_appso_cleaning %>% tally() %>% pull()} rows"
))

# ---- 2) SQL: alter table + update original CIP -------------------------------
# WHAT: Add empty columns for the cleaned 4-digit and 2-digit CIP codes and
#       their names. Also preserve the original CIP value for later joins.
# WHY:  The cleaning steps below populate STP_CIP_CODE_4/2 progressively using
#       coalesce(). PSI_CREDENTIAL_CIP_orig is needed because step 3 modifies
#       PSI_CREDENTIAL_CIP in-place, and the join in step 11 must use the
#       original value.
# Note: dbplyr translates NA_character_ to SQL NULL (no type). SQL Server then
#       defaults untyped NULL to int. Using sql("CAST(NULL AS VARCHAR(255))")
#       forces the database to treat it as a character column.

credential_non_dup_stp_appso_cleaning <- credential_non_dup_stp_appso_cleaning %>%
  mutate(
    STP_CIP_CODE_4 = sql("CAST(NULL AS VARCHAR(255))"),
    STP_CIP_CODE_4_NAME = sql("CAST(NULL AS VARCHAR(255))"),
    STP_CIP_CODE_2 = sql("CAST(NULL AS VARCHAR(255))"),
    STP_CIP_CODE_2_NAME = sql("CAST(NULL AS VARCHAR(255))"),
    PSI_CREDENTIAL_CIP_orig = PSI_CREDENTIAL_CIP
  )

# ---- 3) SQL: clean malformed CIP values --------------------------------------
# WHAT: Fix two common formatting issues in STP CIP codes:
#   3a) 6-character CIPs missing a trailing zero (e.g., "11.010" should be "11.0100")
#   3b) 6-character CIPs missing a leading zero (e.g., "1.0100" should be "01.0100")
# WHY:  INFOWARE lookup tables expect 7-character CIP codes (e.g., "01.0100").
#       Malformed values would fail exact and partial matching in steps 4-7.

# Step 3a: append trailing zero for 6-character values whose second character
# is not a period (i.e., the value is missing its fourth digit).
credential_non_dup_stp_appso_cleaning <- credential_non_dup_stp_appso_cleaning %>%
  mutate(
    PSI_CREDENTIAL_CIP = case_when(
      nchar(PSI_CREDENTIAL_CIP) == 6 &
        substr(PSI_CREDENTIAL_CIP, 2, 2) != "\\." ~
        paste0(PSI_CREDENTIAL_CIP, "0"),
      TRUE ~ PSI_CREDENTIAL_CIP
    )
  )

# Step 3b: prepend leading zero for any value still length 6.
credential_non_dup_stp_appso_cleaning <- credential_non_dup_stp_appso_cleaning %>%
  mutate(
    PSI_CREDENTIAL_CIP = case_when(
      nchar(PSI_CREDENTIAL_CIP) == 6 ~ paste0("0", PSI_CREDENTIAL_CIP),
      TRUE ~ PSI_CREDENTIAL_CIP
    )
  )

log_info("Step 3: Cleaned malformed CIP values (trailing/leading zero fixes)")

# ---- 4) SQL Step1_a: exact match to INFOWARE 6-digit CIP table --------------
# WHAT: Attempt an exact match of the cleaned CIP code against the INFOWARE
#       6-digit lookup table to derive both 4-digit and 2-digit CIP codes.
# WHY:  Exact matching is the most reliable way to resolve CIP codes. If the
#       full 7-character code (e.g., "01.0100") exists in INFOWARE, we can
#       confidently extract the corresponding 4-digit (e.g., "0101") and
#       2-digit (e.g., "01") codes.

exact_match_6 <- infoware_l_cip_6digits_cip2016 %>%
  select(
    LCIP_CD_WITH_PERIOD,
    exact_STP_CIP_CODE_4 = LCIP_LCP4_CD,
    exact_STP_CIP_CODE_2 = LCIP_LCP2_CD
  )

credential_non_dup_stp_appso_cleaning <- credential_non_dup_stp_appso_cleaning %>%
  left_join(
    exact_match_6,
    by = c("PSI_CREDENTIAL_CIP" = "LCIP_CD_WITH_PERIOD")
  ) %>%
  mutate(
    STP_CIP_CODE_4 = coalesce(exact_STP_CIP_CODE_4, STP_CIP_CODE_4),
    STP_CIP_CODE_2 = coalesce(exact_STP_CIP_CODE_2, STP_CIP_CODE_2)
  ) %>%
  select(-exact_STP_CIP_CODE_4, -exact_STP_CIP_CODE_2)

log_info(glue::glue(
  "Step 4: Exact 6-digit CIP match complete. Remaining unmatched 4-digit: {credential_non_dup_stp_appso_cleaning %>% filter(is.na(STP_CIP_CODE_4)) %>% tally() %>% pull()}"
))

# ---- 5) SQL Step1_b: fallback match on first 5 characters --------------------
# WHAT: For CIPs that failed the exact 6-digit match, try matching on the first
#       5 characters of the CIP code (e.g., "01.01" matches "01.0100").
# WHY:  Some STP CIP codes may have minor formatting differences or trailing
#       digits that prevent an exact match but still correspond to a valid
#       INFOWARE entry at the 5-character level.

fallback_match_5 <- credential_non_dup_stp_appso_cleaning %>%
  filter(is.na(STP_CIP_CODE_4)) %>%
  mutate(join_key_5 = substr(PSI_CREDENTIAL_CIP, 1, 5)) %>%
  select(PSI_CREDENTIAL_CIP, OUTCOMES_CRED, join_key_5)

infoware_join_5 <- infoware_l_cip_6digits_cip2016 %>%
  mutate(join_key_5 = substr(LCIP_CD_WITH_PERIOD, 1, 5)) %>%
  select(
    join_key_5,
    fallback_STP_CIP_CODE_4 = LCIP_LCP4_CD,
    fallback_STP_CIP_CODE_2 = LCIP_LCP2_CD
  )

fallback_updates_5 <- fallback_match_5 %>%
  inner_join(infoware_join_5, by = "join_key_5") %>%
  select(
    PSI_CREDENTIAL_CIP,
    OUTCOMES_CRED,
    fallback_STP_CIP_CODE_4,
    fallback_STP_CIP_CODE_2
  )

credential_non_dup_stp_appso_cleaning <- credential_non_dup_stp_appso_cleaning %>%
  left_join(
    fallback_updates_5,
    by = c("PSI_CREDENTIAL_CIP", "OUTCOMES_CRED")
  ) %>%
  mutate(
    STP_CIP_CODE_4 = coalesce(STP_CIP_CODE_4, fallback_STP_CIP_CODE_4),
    STP_CIP_CODE_2 = coalesce(STP_CIP_CODE_2, fallback_STP_CIP_CODE_2)
  ) %>%
  select(-fallback_STP_CIP_CODE_4, -fallback_STP_CIP_CODE_2)

log_info(glue::glue(
  "Step 5: 5-char fallback match complete. Remaining unmatched 4-digit: {credential_non_dup_stp_appso_cleaning %>% filter(is.na(STP_CIP_CODE_4)) %>% tally() %>% pull()}"
))

# ---- 6) SQL Step1_c: general program rule (.00 -> 01) ------------------------
# WHAT: For CIPs still unmatched at the 4-digit level, apply a "general program"
#       rule: if the first 5 characters match a known general-program prefix
#       (e.g., "11.00"), default the 4-digit code to the "01" variant
#       (e.g., "1101" = General Agriculture).
# WHY:  General program CIPs (ending in .00) are not valid 4-digit codes in
#       INFOWARE. The convention is to map them to the corresponding "01"
#       general category, which is the closest valid match.

general_program_prefixes <- c(
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

credential_non_dup_stp_appso_cleaning <- credential_non_dup_stp_appso_cleaning %>%
  mutate(
    STP_CIP_CODE_4 = case_when(
      is.na(STP_CIP_CODE_4) &
        substr(PSI_CREDENTIAL_CIP, 1, 5) %in% general_program_prefixes ~
        paste0(substr(PSI_CREDENTIAL_CIP, 1, 2), "01"),
      TRUE ~ STP_CIP_CODE_4
    )
  )

log_info(
  "Step 6: Applied general program rule (.00 -> 01) for unmatched 4-digit CIPs"
)

# ---- 7) SQL Step1_d: fallback 2-digit match on first 2 characters ------------
# WHAT: For CIPs where the 2-digit code is still missing, try matching on the
#       first 2 characters of the CIP code against the INFOWARE 6-digit table.
# WHY:  The 2-digit CIP code represents the broadest program category. Even when
#       exact and 5-char matching fail, the first 2 digits should resolve to a
#       valid 2-digit category in most cases.

fallback_match_2 <- credential_non_dup_stp_appso_cleaning %>%
  filter(is.na(STP_CIP_CODE_2)) %>%
  mutate(join_key_2 = substr(PSI_CREDENTIAL_CIP, 1, 2)) %>%
  select(PSI_CREDENTIAL_CIP, OUTCOMES_CRED, join_key_2)

infoware_join_2 <- infoware_l_cip_6digits_cip2016 %>%
  mutate(join_key_2 = substr(LCIP_CD_WITH_PERIOD, 1, 2)) %>%
  select(join_key_2, fallback_STP_CIP_CODE_2 = LCIP_LCP2_CD)

fallback_updates_2 <- fallback_match_2 %>%
  inner_join(infoware_join_2, by = "join_key_2") %>%
  select(PSI_CREDENTIAL_CIP, OUTCOMES_CRED, fallback_STP_CIP_CODE_2)

credential_non_dup_stp_appso_cleaning <- credential_non_dup_stp_appso_cleaning %>%
  left_join(
    fallback_updates_2,
    by = c("PSI_CREDENTIAL_CIP", "OUTCOMES_CRED")
  ) %>%
  mutate(
    STP_CIP_CODE_2 = coalesce(STP_CIP_CODE_2, fallback_STP_CIP_CODE_2)
  ) %>%
  select(-fallback_STP_CIP_CODE_2)

log_info(glue::glue(
  "Step 7: 2-char fallback match complete. Remaining unmatched 2-digit: {credential_non_dup_stp_appso_cleaning %>% filter(is.na(STP_CIP_CODE_2)) %>% tally() %>% pull()}"
))

# ---- 8) SQL Step2: add 4-digit CIP names -------------------------------------
# WHAT: Join the cleaned 4-digit CIP codes to the INFOWARE 4-digit lookup table
#       to add human-readable CIP program names (e.g., "0101" -> "Agriculture").
# WHY:  The 4-digit CIP name is needed for downstream reporting and for
#       validating that the CIP code makes sense in context.

credential_non_dup_stp_appso_cleaning <- credential_non_dup_stp_appso_cleaning %>%
  left_join(
    infoware_l_cip_4digits_cip2016 %>%
      select(
        STP_CIP_CODE_4 = LCP4_CD,
        STP_CIP_CODE_4_NAME_lookup = LCP4_CIP_4DIGITS_NAME
      ),
    by = "STP_CIP_CODE_4"
  ) %>%
  mutate(
    STP_CIP_CODE_4_NAME = coalesce(
      STP_CIP_CODE_4_NAME_lookup,
      STP_CIP_CODE_4_NAME
    )
  ) %>%
  select(-STP_CIP_CODE_4_NAME_lookup)

# ---- 9) SQL Step3: add 2-digit CIP names -------------------------------------
# WHAT: Join the cleaned 2-digit CIP codes to the INFOWARE 2-digit lookup table
#       to add human-readable broad category names (e.g., "01" -> "Agriculture").

credential_non_dup_stp_appso_cleaning <- credential_non_dup_stp_appso_cleaning %>%
  left_join(
    infoware_l_cip_2digits_cip2016 %>%
      select(
        STP_CIP_CODE_2 = LCP2_CD,
        STP_CIP_CODE_2_NAME_lookup = LCP2_DIGITS_NAME
      ),
    by = "STP_CIP_CODE_2"
  ) %>%
  mutate(
    STP_CIP_CODE_2_NAME = coalesce(
      STP_CIP_CODE_2_NAME_lookup,
      STP_CIP_CODE_2_NAME
    )
  ) %>%
  select(-STP_CIP_CODE_2_NAME_lookup)

# ---- 10) SQL Step4: mark missing 4-digit names as invalid --------------------
# WHAT: For any 4-digit CIP codes that still have no name after all matching
#       steps, mark them as "Invalid 4-digit CIP" so they can be flagged in
#       downstream quality checks.
# WHY:  A missing name indicates the CIP code could not be resolved to a valid
#       INFOWARE entry. Flagging it explicitly is better than leaving it NULL.

credential_non_dup_stp_appso_cleaning <- credential_non_dup_stp_appso_cleaning %>%
  mutate(
    STP_CIP_CODE_4_NAME = coalesce(STP_CIP_CODE_4_NAME, "Invalid 4-digit CIP")
  )

log_info(
  "Steps 8-10: Added 4-digit and 2-digit CIP names. Marked unresolvable 4-digit names as 'Invalid 4-digit CIP'"
)

# ---- 11) SQL: create Credential_Non_Dup_APPSO_IDs ----------------------------
# WHAT: Join the cleaned CIP codes back to the full APPSO credential data to
#       create the final APPSO IDs table with normalized FINAL_CIP columns.
# WHY:  The cleaning table (step 1-10) operates on distinct CIP values only.
#       This step expands the results back to every individual APPSO credential
#       record, assigning FINAL_CIP_CODE_4/2 and their names.
# HOW:  Inner join on the original (pre-cleaning) CIP value so that each
#       credential record inherits the cleaned codes from its matching CIP.
#       The join key uses PSI_CREDENTIAL_CIP_orig (preserved in step 2).

credential_non_dup_appso_ids <- credential_non_dup %>%
  select(
    ID,
    PSI_CODE,
    PSI_PROGRAM_CODE,
    PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    PSI_CREDENTIAL_CIP,
    PSI_AWARD_SCHOOL_YEAR,
    OUTCOMES_CRED,
  ) |>
  filter(OUTCOMES_CRED == "APPSO") %>%
  inner_join(
    credential_non_dup_stp_appso_cleaning,
    by = c(
      "PSI_CREDENTIAL_CIP" = "PSI_CREDENTIAL_CIP_orig",
      "OUTCOMES_CRED" = "OUTCOMES_CRED"
    )
  ) %>%
  transmute(
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
  )

log_info(glue::glue(
  "Step 11: Created Credential_Non_Dup_APPSO_IDs: {credential_non_dup_appso_ids %>% tally() %>% pull()} rows"
))

# ---- 12) SQL: replace '(Unspecified)' with NULL ------------------------------
# WHAT: Clean up PSI_PROGRAM_CODE values that were imported as "(Unspecified)"
#       by converting them to NULL.
# WHY:  NULL is the correct representation for missing program codes. The
#       "(Unspecified)" string is an artifact of the SQL Server import process
#       and can cause issues in downstream joins and filters.

credential_non_dup_appso_ids <- credential_non_dup_appso_ids %>%
  mutate(
    PSI_PROGRAM_CODE = na_if(PSI_PROGRAM_CODE, "(Unspecified)")
  )

log_info("Step 12: Replaced '(Unspecified)' PSI_PROGRAM_CODE values with NULL")

# ---- Optional materialization -------------------------------------------------
# WHAT: Materialize both lazy dbplyr tables as persistent SQL Server tables so
#       they can be used by downstream scripts without re-running the cleaning
#       pipeline.
# WHY:  compute() writes the lazy query result to a physical table in the
#       database, making it available for direct querying and joins in
#       subsequent scripts (e.g., 02a-update-cred-non-dup.R).
# Note: compute_overwrite() pre-checks the target and drops it before
#       computing -- compute()/copy_to() with an in_schema()/Id() target
#       does not honour overwrite = TRUE on reruns.

credential_non_dup_appso_ids <- compute_overwrite(
  credential_non_dup_appso_ids,
  con = con,
  schema = my_schema,
  table = "Credential_Non_Dup_APPSO_IDs_r"
)
log_info(glue::glue(
  "Materialized Credential_Non_Dup_APPSO_IDs_r to SQL Server"
))


credential_non_dup_stp_appso_cleaning <- compute_overwrite(
  credential_non_dup_stp_appso_cleaning,
  con = con,
  schema = my_schema,
  table = "Credential_Non_Dup_STP_APPSO_Cleaning_r"
)
log_info(glue::glue(
  "Materialized Credential_Non_Dup_STP_APPSO_Cleaning_r to SQL Server"
))

# ---- Disconnect ---------------------------------------------------------------
dbDisconnect(con)
log_info("Disconnected from SQL Server")

log_info("==== 02a-appso-programs.R COMPLETE ====")
