# Update Credential Non Dup (Refactored to dplyr)
#
# PSSM Model Context:
# This script represents the consolidation phase of the "Program Matching" workflow.
# In previous steps (02a-appso, 02a-bgs, 02a-dacso), we analyzed specific sub-sectors of
# the post-secondary system to map institutional programs to standardized CIP codes.
#
# This script integrates those findings back into the central 'Credential_Non_Dup' table.
# It ensures that every credential record has the most accurate 'FINAL_CIP_CODE' possible,
# enabling the Graduation and Occupation Projection models to function correctly.
#
# Required Tables
#   credential_non_dup - Base table to update
#   Credential_Non_Dup_Programs_DACSO_FinalCIPS - DACSO matching results
#   Credential_Non_Dup_BGS_IDs - BGS matching results
#   Credential_Non_Dup_GRAD_IDs - GRAD matching results
#   Credential_Non_Dup_APPSO_IDs - APPSO matching results
#   INFOWARE_L_CIP_6DIGITS_CIP2016 - CIP lookup for recovery
#   INFOWARE_L_CIP_4DIGITS_CIP2016 - 4-digit CIP names
#   INFOWARE_L_CIP_2DIGITS_CIP2016 - 2-digit CIP names and clusters
#
# Resulting Tables
#   Credential_Non_Dup_Updated - Final consolidated table
#
# WHAT: Consolidates program matching results from all outcomes sources into a single updated table.
# WHY: Each outcomes source (DACSO, BGS, GRAD, APPSO) uses different matching strategies.
#      This script applies those results in priority order and cleans up remaining NULLs.
# HOW: 1) Join all source tables to base credential table
#      2) Apply case_when logic to select appropriate CIP based on OUTCOMES_CRED type
#      3) Clean remaining NULL CIPs using Infoware lookup cascade
#      4) Apply cluster information and fallback values
#      5) Handle special cases (Undeclared activity for code 99)
#
# TODO [HIGH]: Add comprehensive column preservation from original credential table
# TODO [MEDIUM]: Add validation with before/after NULL counts
# TODO [LOW]: Add option to update Credential_Non_Dup in place after verification

library(arrow)
library(tidyverse)
library(odbc)
library(DBI)
library(dbplyr)
library(config)

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

# ---- Table References ----

# Base Table
credential_tbl <- tbl(con, in_schema(my_schema, "credential_non_dup"))

# Source of Truth Tables (Created in previous steps)
dacso_matches_tbl <- tbl(
  con,
  in_schema(my_schema, "Credential_Non_Dup_Programs_DACSO_FinalCIPS")
)
bgs_ids_tbl <- tbl(con, in_schema(my_schema, "Credential_Non_Dup_BGS_IDs"))
grad_ids_tbl <- tbl(con, in_schema(my_schema, "Credential_Non_Dup_GRAD_IDs"))
appso_ids_tbl <- tbl(con, in_schema(my_schema, "Credential_Non_Dup_APPSO_IDs"))

# Infoware Reference Tables (For cleaning NULLs)
cip_6_tbl <- tbl(con, in_schema(my_schema, "INFOWARE_L_CIP_6DIGITS_CIP2016"))
cip_4_tbl <- tbl(con, in_schema(my_schema, "INFOWARE_L_CIP_4DIGITS_CIP2016"))
cip_2_tbl <- tbl(con, in_schema(my_schema, "INFOWARE_L_CIP_2DIGITS_CIP2016"))

# ---- Logic Implementation ----
#
# WHAT: Prepares update columns from each matching source and applies them to the base table.
# WHY: Each outcomes source has different join keys (ID for BGS/GRAD/APPSO, attributes for DACSO).
#      We need clean update sets before applying to the base table.
# HOW: 1) Select and rename columns from each matching source table
#      2) Left join all update sets to the base credential table
#      3) Apply case_when logic to select values based on OUTCOMES_CRED type
#      4) Use coalesce to prefer new values, fallback to existing
#
# TODO [HIGH]: Consolidate the 4 identical case_when patterns for CIP columns into a function
# TODO [MEDIUM]: Add debug output for match coverage by outcomes source

# 1. Prepare Updates from Sources
# We select only the columns needed for updates to keep joins clean.

# DACSO (Joined by Program Attributes)
# Note: SQL joins on PSI_CODE, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION, etc.
dacso_updates <- dacso_matches_tbl %>%
  select(
    PSI_CODE,
    PSI_PROGRAM_CODE,
    PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    PSI_CREDENTIAL_CIP,
    PSI_CREDENTIAL_LEVEL,
    PSI_CREDENTIAL_CATEGORY,
    OUTCOMES_CRED,
    D_FINAL_CIP_4 = FINAL_CIP_CODE_4,
    D_FINAL_CIP_4_NAME = FINAL_CIP_CODE_4_NAME,
    D_FINAL_CIP_2 = FINAL_CIP_CODE_2,
    D_FINAL_CIP_2_NAME = FINAL_CIP_CODE_2_NAME,
    D_FINAL_CLUSTER_CODE = FINAL_CIP_CLUSTER_CODE,
    D_FINAL_CLUSTER_NAME = FINAL_CIP_CLUSTER_NAME,
    D_OUTCOMES_CIP_4 = OUTCOMES_CIP_CODE_4,
    D_OUTCOMES_CIP_4_NAME = OUTCOMES_CIP_CODE_4_NAME,
    D_STP_CIP_4 = STP_CIP_CODE_4,
    D_STP_CIP_4_NAME = STP_CIP_CODE_4_NAME
  )

# BGS (Joined by ID)
bgs_updates <- bgs_ids_tbl %>%
  select(
    ID,
    B_FINAL_CIP_4 = FINAL_CIP_CODE_4,
    B_FINAL_CIP_4_NAME = FINAL_CIP_CODE_4_NAME,
    B_FINAL_CIP_2 = FINAL_CIP_CODE_2,
    B_FINAL_CIP_2_NAME = FINAL_CIP_CODE_2_NAME,
    B_FINAL_CLUSTER_CODE = FINAL_CIP_CLUSTER_CODE,
    B_FINAL_CLUSTER_NAME = FINAL_CIP_CLUSTER_NAME
  )

# GRAD (Joined by ID)
grad_updates <- grad_ids_tbl %>%
  select(
    ID,
    G_FINAL_CIP_4 = FINAL_CIP_CODE_4,
    G_FINAL_CIP_4_NAME = FINAL_CIP_CODE_4_NAME,
    G_FINAL_CIP_2 = FINAL_CIP_CODE_2,
    G_FINAL_CIP_2_NAME = FINAL_CIP_CODE_2_NAME
  )

# APPSO (Joined by ID)
appso_updates <- appso_ids_tbl %>%
  select(
    ID,
    A_FINAL_CIP_4 = FINAL_CIP_CODE_4,
    A_FINAL_CIP_4_NAME = FINAL_CIP_CODE_4_NAME,
    A_FINAL_CIP_2 = FINAL_CIP_CODE_2,
    A_FINAL_CIP_2_NAME = FINAL_CIP_CODE_2_NAME
  )

# 2. Apply Updates to Base Table
# We perform Left Joins and use coalesce/case_when to prioritize the new values.

updated_credential <- credential_tbl %>%
  # Join DACSO
  left_join(
    dacso_updates,
    by = c(
      "PSI_CODE",
      "PSI_PROGRAM_CODE",
      "PSI_CREDENTIAL_PROGRAM_DESCRIPTION",
      "PSI_CREDENTIAL_CIP",
      "PSI_CREDENTIAL_LEVEL",
      "PSI_CREDENTIAL_CATEGORY",
      "OUTCOMES_CRED"
    )
  ) %>%
  # Join BGS
  left_join(bgs_updates, by = "ID") %>%
  # Join GRAD
  left_join(grad_updates, by = "ID") %>%
  # Join APPSO
  left_join(appso_updates, by = "ID") %>%
  mutate(
    # --- Logic for FINAL_CIP_CODE_4 ---
    FINAL_CIP_CODE_4 = case_when(
      OUTCOMES_CRED == 'DACSO' ~ coalesce(D_FINAL_CIP_4, FINAL_CIP_CODE_4),
      OUTCOMES_CRED == 'BGS' ~ coalesce(B_FINAL_CIP_4, FINAL_CIP_CODE_4),
      OUTCOMES_CRED == 'GRAD' ~ coalesce(G_FINAL_CIP_4, FINAL_CIP_CODE_4),
      OUTCOMES_CRED == 'APPSO' ~ coalesce(A_FINAL_CIP_4, FINAL_CIP_CODE_4),
      TRUE ~ FINAL_CIP_CODE_4
    ),

    # --- Logic for FINAL_CIP_CODE_4_NAME ---
    FINAL_CIP_CODE_4_NAME = case_when(
      OUTCOMES_CRED == 'DACSO' ~
        coalesce(D_FINAL_CIP_4_NAME, FINAL_CIP_CODE_4_NAME),
      OUTCOMES_CRED == 'BGS' ~
        coalesce(B_FINAL_CIP_4_NAME, FINAL_CIP_CODE_4_NAME),
      OUTCOMES_CRED == 'GRAD' ~
        coalesce(G_FINAL_CIP_4_NAME, FINAL_CIP_CODE_4_NAME),
      OUTCOMES_CRED == 'APPSO' ~
        coalesce(A_FINAL_CIP_4_NAME, FINAL_CIP_CODE_4_NAME),
      TRUE ~ FINAL_CIP_CODE_4_NAME
    ),

    # --- Logic for FINAL_CIP_CODE_2 ---
    FINAL_CIP_CODE_2 = case_when(
      OUTCOMES_CRED == 'DACSO' ~ coalesce(D_FINAL_CIP_2, FINAL_CIP_CODE_2),
      OUTCOMES_CRED == 'BGS' ~ coalesce(B_FINAL_CIP_2, FINAL_CIP_CODE_2),
      OUTCOMES_CRED == 'GRAD' ~ coalesce(G_FINAL_CIP_2, FINAL_CIP_CODE_2),
      OUTCOMES_CRED == 'APPSO' ~ coalesce(A_FINAL_CIP_2, FINAL_CIP_CODE_2),
      TRUE ~ FINAL_CIP_CODE_2
    ),

    # --- Logic for FINAL_CIP_CODE_2_NAME ---
    FINAL_CIP_CODE_2_NAME = case_when(
      OUTCOMES_CRED == 'DACSO' ~
        coalesce(D_FINAL_CIP_2_NAME, FINAL_CIP_CODE_2_NAME),
      OUTCOMES_CRED == 'BGS' ~
        coalesce(B_FINAL_CIP_2_NAME, FINAL_CIP_CODE_2_NAME),
      OUTCOMES_CRED == 'GRAD' ~
        coalesce(G_FINAL_CIP_2_NAME, FINAL_CIP_CODE_2_NAME),
      OUTCOMES_CRED == 'APPSO' ~
        coalesce(A_FINAL_CIP_2_NAME, FINAL_CIP_CODE_2_NAME),
      TRUE ~ FINAL_CIP_CODE_2_NAME
    ),

    # --- Logic for FINAL_CIP_CLUSTER_CODE ---
    # Note: GRAD and APPSO cluster codes are updated in a later step in original SQL,
    # but we can do it here if we join the reference table later.
    FINAL_CIP_CLUSTER_CODE = case_when(
      OUTCOMES_CRED == 'DACSO' ~
        coalesce(D_FINAL_CLUSTER_CODE, FINAL_CIP_CLUSTER_CODE),
      OUTCOMES_CRED == 'BGS' ~
        coalesce(B_FINAL_CLUSTER_CODE, FINAL_CIP_CLUSTER_CODE),
      TRUE ~ FINAL_CIP_CLUSTER_CODE
    ),

    # --- Logic for FINAL_CIP_CLUSTER_NAME ---
    FINAL_CIP_CLUSTER_NAME = case_when(
      OUTCOMES_CRED == 'DACSO' ~
        coalesce(D_FINAL_CLUSTER_NAME, FINAL_CIP_CLUSTER_NAME),
      OUTCOMES_CRED == 'BGS' ~
        coalesce(B_FINAL_CLUSTER_NAME, FINAL_CIP_CLUSTER_NAME),
      TRUE ~ FINAL_CIP_CLUSTER_NAME
    ),

    # --- Logic for OUTCOMES_CIP_CODE_4 (Mainly DACSO) ---
    OUTCOMES_CIP_CODE_4 = if_else(
      OUTCOMES_CRED == 'DACSO',
      coalesce(D_OUTCOMES_CIP_4, OUTCOMES_CIP_CODE_4),
      OUTCOMES_CIP_CODE_4
    ),
    OUTCOMES_CIP_CODE_4_NAME = if_else(
      OUTCOMES_CRED == 'DACSO',
      coalesce(D_OUTCOMES_CIP_4_NAME, OUTCOMES_CIP_CODE_4_NAME),
      OUTCOMES_CIP_CODE_4_NAME
    ),

    # --- Logic for STP_CIP_CODE_4 (Mainly DACSO) ---
    STP_CIP_CODE_4 = if_else(
      OUTCOMES_CRED == 'DACSO',
      coalesce(D_STP_CIP_4, STP_CIP_CODE_4),
      STP_CIP_CODE_4
    ),
    STP_CIP_CODE_4_NAME = if_else(
      OUTCOMES_CRED == 'DACSO',
      coalesce(D_STP_CIP_4_NAME, STP_CIP_CODE_4_NAME),
      STP_CIP_CODE_4_NAME
    )
  )

# 3. Clean Leftover NULLs (Refactoring qry_NULL_STP_CIP_Cleaning)
# Logic: If FINAL_CIP_CODE_4 is still NA, try to recover it from PSI_CREDENTIAL_CIP using Infoware logic.
#
# WHAT: Recovers CIP codes for records that still have NULL after matching.
# WHY: Some records may not have matched in previous steps. We attempt to recover CIP codes
#      by normalizing the raw PSI credential CIP and joining to Infoware lookup tables.
# HOW: 1) Clean PSI_CREDENTIAL_CIP string (add periods/zeros as needed)
#      2) Apply cascade join strategy:
#         - Match on full cleaned CIP code
#         - Fallback to first 5 characters match
#         - Fallback to first 2 characters match
#      3) Apply general program recode (e.g., "11.00" -> "1101")
#      4) Use coalesce to select first successful match
#      5) Apply recovered CIPs only to records that were NULL
#
# TODO [MEDIUM]: Add logging for recovery success rate by CIP level
# TODO [LOW]: Extract general_cips vector to configuration

# First, define the cleaned CIP (Add periods/zeros)
updated_credential <- updated_credential %>%
  mutate(
    # Temporary column for cleaning
    PSI_CIP_CLEAN = case_when(
      nchar(PSI_CREDENTIAL_CIP) == 6 &
        !like(substr(PSI_CREDENTIAL_CIP, 1, 2), '%.%') ~
        paste0(PSI_CREDENTIAL_CIP, "0"),
      nchar(PSI_CREDENTIAL_CIP) == 6 ~ paste0("0", PSI_CREDENTIAL_CIP),
      TRUE ~ PSI_CREDENTIAL_CIP
    ),
    CIP_5 = substr(PSI_CIP_CLEAN, 1, 5),
    CIP_2 = substr(PSI_CIP_CLEAN, 1, 2)
  )

# Join with Infoware to find matches for these NULL records
# Note: In dplyr/SQL translation, it's efficient to join and then use coalesce
# We assume cip_6_tbl, cip_4_tbl, cip_2_tbl are available

updated_credential <- updated_credential %>%
  # Join CIP 6 digit
  left_join(
    cip_6_tbl %>%
      select(
        LCIP_CD_WITH_PERIOD,
        NULL_CIP4_A = LCIP_LCP4_CD,
        NULL_CIP2_A = LCIP_LCP2_CD
      ),
    by = c("PSI_CIP_CLEAN" = "LCIP_CD_WITH_PERIOD")
  ) %>%
  # Join CIP 6 digit (Match first 5 chars)
  left_join(
    cip_6_tbl %>%
      mutate(LCIP_5 = substr(LCIP_CD_WITH_PERIOD, 1, 5)) %>%
      select(LCIP_5, NULL_CIP4_B = LCIP_LCP4_CD, NULL_CIP2_B = LCIP_LCP2_CD) %>%
      distinct(),
    by = c("CIP_5" = "LCIP_5")
  ) %>%
  # Join CIP 6 digit (Match first 2 chars)
  left_join(
    cip_6_tbl %>%
      mutate(LCIP_2_SUB = substr(LCIP_CD_WITH_PERIOD, 1, 2)) %>%
      select(LCIP_2_SUB, NULL_CIP2_C = LCIP_LCP2_CD) %>%
      distinct(),
    by = c("CIP_2" = "LCIP_2_SUB")
  ) %>%
  # General Program Recode Logic (11.00 -> 11.01 etc)
  mutate(
    NULL_CIP4_C = case_when(
      CIP_5 %in%
        c(
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
        ) ~
        paste0(CIP_2, "01"),
      TRUE ~ NA_character_
    )
  ) %>%
  mutate(
    # Determine the "Recovered" CIPs
    RECOVERED_CIP_4 = coalesce(NULL_CIP4_A, NULL_CIP4_B, NULL_CIP4_C),
    RECOVERED_CIP_2 = coalesce(NULL_CIP2_A, NULL_CIP2_B, NULL_CIP2_C)
  ) %>%
  # Get Names for Recovered CIPs
  left_join(
    cip_4_tbl %>% select(LCP4_CD, RECOVERED_CIP_4_NAME = LCP4_CIP_4DIGITS_NAME),
    by = c("RECOVERED_CIP_4" = "LCP4_CD")
  ) %>%
  left_join(
    cip_2_tbl %>% select(LCP2_CD, RECOVERED_CIP_2_NAME = LCP2_DIGITS_NAME),
    by = c("RECOVERED_CIP_2" = "LCP2_CD")
  ) %>%
  # Apply Recovery to NULL records
  mutate(
    FINAL_CIP_CODE_4 = case_when(
      is.na(FINAL_CIP_CODE_4) ~ RECOVERED_CIP_4,
      TRUE ~ FINAL_CIP_CODE_4
    ),
    FINAL_CIP_CODE_4_NAME = case_when(
      is.na(FINAL_CIP_CODE_4_NAME) ~
        coalesce(RECOVERED_CIP_4_NAME, "Invalid 4-digit CIP"),
      TRUE ~ FINAL_CIP_CODE_4_NAME
    ),
    FINAL_CIP_CODE_2 = case_when(
      is.na(FINAL_CIP_CODE_2) ~ RECOVERED_CIP_2,
      TRUE ~ FINAL_CIP_CODE_2
    ),
    FINAL_CIP_CODE_2_NAME = case_when(
      is.na(FINAL_CIP_CODE_2_NAME) ~ RECOVERED_CIP_2_NAME,
      TRUE ~ FINAL_CIP_CODE_2_NAME
    )
  )

# 4. Final Updates (Clusters and Cleanup)
# Update Cluster info for all records based on FINAL_CIP_CODE_2 (Ensure consistency)
# Also handles the "Update Cluster for GRAD/APPSO" step
# Also handles SQLQuery4 (Undeclared Activity for 99)
#
# WHAT: Final cleanup and enrichment of the updated credential table.
# WHY: After applying matching results and cleaning NULLs, we need to:
#      - Ensure all records have cluster information
#      - Apply final fallbacks to STP values
#      - Handle special cases (Undeclared activity for CIP code 99)
# HOW: 1) Left join cluster information from 2-digit CIP lookup
#      2) Apply fallback to STP CIP values if still NULL
#      3) Set Undeclared activity labels for CIP code 99 in BGS/GRAD
#      4) Select final output columns
#
# TODO [MEDIUM]: Add validation that all FINAL columns are populated
# TODO [LOW]: Consider adding audit columns for tracking updates

updated_credential <- updated_credential %>%
  # Join Cluster Info
  left_join(
    cip_2_tbl %>% select(LCP2_CD, LCP2_LCIPPC_CD, LCP2_LCIPPC_NAME),
    by = c("FINAL_CIP_CODE_2" = "LCP2_CD")
  ) %>%
  mutate(
    FINAL_CIP_CLUSTER_CODE = coalesce(FINAL_CIP_CLUSTER_CODE, LCP2_LCIPPC_CD),
    FINAL_CIP_CLUSTER_NAME = coalesce(FINAL_CIP_CLUSTER_NAME, LCP2_LCIPPC_NAME)
  ) %>%
  # Fallback: If FINAL_CIP_CODE_4 is still missing, use STP_CIP_CODE_4
  mutate(
    FINAL_CIP_CODE_4 = coalesce(FINAL_CIP_CODE_4, STP_CIP_CODE_4),
    FINAL_CIP_CODE_4_NAME = coalesce(
      FINAL_CIP_CODE_4_NAME,
      STP_CIP_CODE_4_NAME
    ),
    FINAL_CIP_CODE_2 = coalesce(FINAL_CIP_CODE_2, STP_CIP_CODE_2),
    FINAL_CIP_CODE_2_NAME = coalesce(FINAL_CIP_CODE_2_NAME, STP_CIP_CODE_2_NAME)
  ) %>%
  # Specific Fixes (SQLQuery4, SQLQuery7)
  mutate(
    FINAL_CIP_CODE_2_NAME = if_else(
      FINAL_CIP_CODE_2 == '99' & OUTCOMES_CRED == 'BGS',
      'Undeclared activity',
      FINAL_CIP_CODE_2_NAME
    ),
    FINAL_CIP_CLUSTER_CODE = if_else(
      FINAL_CIP_CODE_2 == '99' &
        (OUTCOMES_CRED == 'BGS' | OUTCOMES_CRED == 'GRAD'),
      '99',
      FINAL_CIP_CLUSTER_CODE
    ),
    FINAL_CIP_CLUSTER_NAME = if_else(
      FINAL_CIP_CODE_2 == '99' &
        (OUTCOMES_CRED == 'BGS' | OUTCOMES_CRED == 'GRAD'),
      'Undeclared activity',
      FINAL_CIP_CLUSTER_NAME
    )
  ) %>%
  # Select final columns to clean up intermediate join columns
  select(
    # ID & Key Columns
    ID,
    PSI_CODE,
    PSI_PROGRAM_CODE,
    PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    PSI_CREDENTIAL_CIP,
    PSI_CREDENTIAL_LEVEL,
    PSI_CREDENTIAL_CATEGORY,
    OUTCOMES_CRED,
    # Standard PSSM Columns (Preserve or Update)
    PSI_AWARD_SCHOOL_YEAR,
    # ... include other original columns from credential_tbl if needed ...
    # Updated CIP Columns
    OUTCOMES_CIP_CODE_4,
    OUTCOMES_CIP_CODE_4_NAME,
    FINAL_CIP_CODE_4,
    FINAL_CIP_CODE_4_NAME,
    FINAL_CIP_CODE_2,
    FINAL_CIP_CODE_2_NAME,
    FINAL_CIP_CLUSTER_CODE,
    FINAL_CIP_CLUSTER_NAME,
    STP_CIP_CODE_4,
    STP_CIP_CODE_4_NAME,
    STP_CIP_CODE_2,
    STP_CIP_CODE_2_NAME
  )

# 5. Save the Result
# We create a new table "Credential_Non_Dup_Updated" to hold the result.
# In a full production run, this might replace "Credential_Non_Dup" after verification.
#
# WHAT: Persists the updated credential table and validates results.
# WHY: The updated table needs to be materialized in the database for downstream scripts.
#      Validation ensures the update process was successful.
# HOW: 1) Drop existing updated table if present
#      2) Compute new table with all transformations applied
#      3) Query for remaining NULL CIPs by outcomes credential
#      4) Print validation results for review

if (
  dbExistsTable(
    con,
    Id(schema = my_schema, table = "Credential_Non_Dup_Updated")
  )
) {
  dbRemoveTable(
    con,
    Id(schema = my_schema, table = "Credential_Non_Dup_Updated")
  )
}

updated_credential %>%
  compute(
    name = "Credential_Non_Dup_Updated",
    temporary = FALSE,
    schema = my_schema
  )

# ---- Validation ----
# Check if there are still any NULL final CIPs
validation <- tbl(con, in_schema(my_schema, "Credential_Non_Dup_Updated")) %>%
  filter(is.na(FINAL_CIP_CODE_4)) %>%
  count(OUTCOMES_CRED, FINAL_CIP_CODE_4) %>%
  collect()

print("Remaining NULL CIPs by Outcome Credential:")
print(validation)

# Clean up
dbDisconnect(con)
