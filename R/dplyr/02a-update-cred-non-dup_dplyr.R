# Update Credential Non Dup — dplyr Translation
# Original: R/02a-update-cred-non-dup.R
#
# Pipeline context:
#   This script is the merge point for all four CIP code matching sources. After the
#   program matching scripts (02a-dacso, 02a-bgs, 02a-appso, and GRAD matching) have
#   each produced a table of matched CIP codes, this script merges them into the main
#   Credential_Non_Dup table. It then handles remaining unmatched records by falling
#   back to institution-reported (STP) CIP codes.
#
#   The priority order for CIP sources is:
#     1. DACSO (richest matching — joins on 7 columns)
#     2. BGS (matched by ID from 02a-bgs)
#     3. GRAD (matched by ID from GRAD matching)
#     4. APPSO (matched by ID from 02a-appso)
#     5. STP fallback (institution-reported, cleaned via INFOWARE lookup)
#
#   The output Credential_Non_Dup table with final CIP codes feeds into:
#     - 02b-1-pssm-cohorts (cohort creation)
#     - 03-near-completers-ttrain
#     - 05-ptib-analysis
#     - 06-program-projections
#
# Input tables:
#   - credential_non_dup — main credential table (from 01b)
#   - Credential_Non_Dup_Programs_DACSO_FinalCIPs — from 02a-dacso
#   - Credential_Non_Dup_BGS_IDs — from 02a-bgs
#   - Credential_Non_Dup_GRAD_IDs — from GRAD matching
#   - Credential_Non_Dup_APPSO_IDs — from 02a-appso
#   - INFOWARE_L_CIP_* — CIP taxonomy lookups (for fallback matching)

library(arrow)
library(tidyverse)
library(dbplyr)
library(odbc)
library(DBI)

# ---- Configure LAN Paths and DB Connection -----
lan <- config::get("lan")
db_config <- config::get("decimal")
my_schema <- config::get("myschema")

con <- dbConnect(odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")

# Helper: reference a table in the user's schema
sch_tbl <- function(name) {
  tbl(con, dbplyr::in_schema(my_schema, name))
}

# ---- Check Required Tables ----
# main table
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."credential_non_dup"')))

# tables with CIP updates (produced by the 4 program matching scripts)
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."Credential_Non_Dup_Programs_DACSO_FinalCIPs"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."Credential_Non_Dup_BGS_IDs"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."Credential_Non_Dup_GRAD_IDs"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."Credential_Non_Dup_APPSO_IDs"')))

# reference tables
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_2DIGITS_CIP2016"')))

# ==============================================================================
# START QUERIES
# ==============================================================================

# The credential table doesn't yet have columns for the final matched CIP codes.
# This step adds them. They will be populated by the four CIP sources (Steps 2–5)
# and the STP fallback (Steps 7–13).
# KEPT AS SQL: ALTER TABLE is DDL — no dplyr equivalent
# ---- Step 1: Add CIP columns to Credential_Non_Dup ----
dbExecute(con, "ALTER TABLE Credential_Non_Dup
ADD         OUTCOMES_CIP_CODE_4 varchar(4),
            OUTCOMES_CIP_CODE_4_NAME varchar(255),
            FINAL_CIP_CODE_4 varchar(4),
            FINAL_CIP_CODE_4_NAME varchar(255),
            FINAL_CIP_CODE_2 varchar(2),
            FINAL_CIP_CODE_2_NAME varchar(255),
            FINAL_CIP_CLUSTER_CODE varchar(10),
            FINAL_CIP_CLUSTER_NAME varchar(255),
            STP_CIP_CODE_4 varchar(4),
            STP_CIP_CODE_4_NAME varchar(255),
            STP_CIP_CODE_2 varchar(2),
            STP_CIP_CODE_2_NAME varchar(255);")

# DACSO provides the richest matching — it joins on 7 columns (institution,
# program code, description, CIP, credential level, category, and outcome type).
# It is applied first so its matches take priority over the simpler ID-based matches
# from BGS, GRAD, and APPSO in Steps 3–5. The resulting CIP columns will be
# populated in cred_non_dup for DACSO-matched records and remain NA for others.
# ---- Step 2: Update CIP codes from DACSO (primary source) ----

cred_non_dup <- sch_tbl("credential_non_dup") %>% collect()
cred_non_dup <- cred_non_dup |> rename_with(toupper)

dacso_cips <- sch_tbl("Credential_Non_Dup_Programs_DACSO_FinalCIPs") %>% collect()
dacso_cips <- dacso_cips |> rename_with(toupper)

# Drop the empty CIP columns (just added by ALTER TABLE) before joining so the
# CIP columns only come from dacso_cips — no .x/.y suffixes needed.
dacso_join <- dacso_cips %>%
  select(
    PSI_CODE, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    PSI_CREDENTIAL_CIP, PSI_CREDENTIAL_LEVEL, PSI_CREDENTIAL_CATEGORY, OUTCOMES_CRED,
    OUTCOMES_CIP_CODE_4, OUTCOMES_CIP_CODE_4_NAME,
    FINAL_CIP_CODE_4, FINAL_CIP_CODE_4_NAME,
    FINAL_CIP_CODE_2, FINAL_CIP_CODE_2_NAME,
    FINAL_CIP_CLUSTER_CODE, FINAL_CIP_CLUSTER_NAME,
    STP_CIP_CODE_4, STP_CIP_CODE_4_NAME
  )

cred_non_dup <- cred_non_dup %>%
  select(-any_of(c("OUTCOMES_CIP_CODE_4", "OUTCOMES_CIP_CODE_4_NAME",
                    "FINAL_CIP_CODE_4", "FINAL_CIP_CODE_4_NAME",
                    "FINAL_CIP_CODE_2", "FINAL_CIP_CODE_2_NAME",
                    "FINAL_CIP_CLUSTER_CODE", "FINAL_CIP_CLUSTER_NAME",
                    "STP_CIP_CODE_4", "STP_CIP_CODE_4_NAME"))) %>%
  left_join(
    dacso_join,
    by = c("PSI_CODE", "PSI_PROGRAM_CODE", "PSI_CREDENTIAL_PROGRAM_DESCRIPTION",
           "PSI_CREDENTIAL_CIP", "PSI_CREDENTIAL_LEVEL", "PSI_CREDENTIAL_CATEGORY", "OUTCOMES_CRED")
  )

# BGS (BC Government Student outcomes) records weren't matched by DACSO.
# These are matched by a simple ID lookup from the BGS program matching script
# (02a-bgs-program-matching). rows_update only overwrites NA values where IDs match.
# ---- Step 3: Update CIP codes from BGS program matching ----

bgs_cips <- sch_tbl("Credential_Non_Dup_BGS_IDs") %>% collect()
bgs_cips <- bgs_cips |> rename_with(toupper)

bgs_updates <- bgs_cips %>%
  select(ID, FINAL_CIP_CODE_4, FINAL_CIP_CODE_4_NAME,
         FINAL_CIP_CODE_2, FINAL_CIP_CODE_2_NAME,
         FINAL_CIP_CLUSTER_CODE, FINAL_CIP_CLUSTER_NAME)

cred_non_dup <- cred_non_dup %>%
  rows_update(bgs_updates, by = "ID", unmatched = "ignore")

# GRAD (graduate outcomes) records get their CIP codes from the GRAD matching.
# Like BGS, this is a simple ID-based lookup applied after DACSO.
# ---- Step 4: Update CIP codes from GRAD program matching ----

grad_cips <- sch_tbl("Credential_Non_Dup_GRAD_IDs") %>% collect()
grad_cips <- grad_cips |> rename_with(toupper)

grad_updates <- grad_cips %>%
  select(ID, FINAL_CIP_CODE_4, FINAL_CIP_CODE_4_NAME,
         FINAL_CIP_CODE_2, FINAL_CIP_CODE_2_NAME)

cred_non_dup <- cred_non_dup %>%
  rows_update(grad_updates, by = "ID", unmatched = "ignore")

# APPSO (Apprentice outcomes) records get their CIP codes from the APPSO
# cleaning script (02a-appso-programs). Like BGS/GRAD, simple ID-based lookup.
# ---- Step 5: Update CIP codes from APPSO program matching ----

appso_cips <- sch_tbl("Credential_Non_Dup_APPSO_IDs") %>% collect()
appso_cips <- appso_cips |> rename_with(toupper)

appso_updates <- appso_cips %>%
  select(ID, FINAL_CIP_CODE_4, FINAL_CIP_CODE_4_NAME,
         FINAL_CIP_CODE_2, FINAL_CIP_CODE_2_NAME)

cred_non_dup <- cred_non_dup %>%
  rows_update(appso_updates, by = "ID", unmatched = "ignore")

# GRAD and APPSO records need cluster codes (broader career groupings) that
# map from their 2-digit CIP codes. These clusters are used in downstream occupation
# matching (script 07) to group CIP programs into occupational categories.
# ---- Step 6: Populate cluster codes for GRAD and APPSO records ----

cip2_lookup <- sch_tbl("INFOWARE_L_CIP_2DIGITS_CIP2016") %>%
  select(LCP2_CD, LCP2_LCIPPC_CD, LCP2_LCIPPC_NAME) %>%
  collect()
cip2_lookup <- cip2_lookup |> rename_with(toupper)

cred_non_dup <- cred_non_dup %>%
  left_join(cip2_lookup, by = c("FINAL_CIP_CODE_2" = "LCP2_CD")) %>%
  mutate(
    FINAL_CIP_CLUSTER_CODE = case_when(
      OUTCOMES_CRED %in% c("GRAD", "APPSO") & !is.na(LCP2_LCIPPC_CD) ~ LCP2_LCIPPC_CD,
      TRUE ~ FINAL_CIP_CLUSTER_CODE
    ),
    FINAL_CIP_CLUSTER_NAME = case_when(
      OUTCOMES_CRED %in% c("GRAD", "APPSO") & !is.na(LCP2_LCIPPC_NAME) ~ LCP2_LCIPPC_NAME,
      TRUE ~ FINAL_CIP_CLUSTER_NAME
    )
  ) %>%
  select(-LCP2_LCIPPC_CD, -LCP2_LCIPPC_NAME)

# Write updated Credential_Non_Dup back to database
dbWriteTable(con, SQL(glue::glue('"{my_schema}"."Credential_Non_Dup"')),
             cred_non_dup, overwrite = TRUE)

# ---- Check for leftover NULLs in final CIP 4 column ----
# WHY: This diagnostic check shows how many records still need CIP codes after the
# four primary sources. These NULLs will be filled by the STP fallback below.
tbl(con, "Credential_Non_Dup") %>% filter(is.na(FINAL_CIP_CODE_4)) %>% count(OUTCOMES_CRED, FINAL_CIP_CODE_4)

# ==============================================================================
# CLEAN UP NULLS — Match leftover NULLs using STP (institution-reported) CIP codes
# ==============================================================================

# Some credentials weren't matched by any of the four CIP sources above (DACSO,
# BGS, GRAD, APPSO). As a last resort, we use the institution's own reported CIP code
# (STP_CIP), clean it against the INFOWARE taxonomy (same cleaning logic as in
# 02a-appso-programs), and use the result. This ensures every record has a CIP code
# for downstream processing.

# Extract distinct CIP codes from unmatched records, clean them, then join back.
# ---- Step 7: Create cleaning table for NULL CIP records ----

null_cleaning <- cred_non_dup %>%
  filter(is.na(FINAL_CIP_CODE_4)) %>%
  count(PSI_CREDENTIAL_CIP, OUTCOMES_CRED, name = "Expr1")

dbWriteTable(con, "Credential_Non_Dup_STP_NULL_Cleaning", null_cleaning, overwrite = TRUE)

# KEPT AS SQL: ALTER TABLE (DDL)
dbExecute(con, "ALTER TABLE Credential_Non_Dup_STP_NULL_Cleaning
ADD STP_CIP_CODE_4 varchar (255),
STP_CIP_CODE_4_NAME varchar (255),
STP_CIP_CODE_2 varchar (255),
STP_CIP_CODE_2_NAME varchar (255),
STP_CIP_CLUSTER_CODE varchar(10),
STP_CIP_CLUSTER_NAME varchar(255),
PSI_CREDENTIAL_CIP_orig varchar (255)")

# We'll modify PSI_CREDENTIAL_CIP during cleaning, so we preserve the original
# to use as a join key when matching the cleaned results back to credential records
# (Step 12).
# ---- Step 8: Save original CIP before cleaning ----

null_cleaning <- null_cleaning %>%
  mutate(PSI_CREDENTIAL_CIP_orig = PSI_CREDENTIAL_CIP)

# Same cleaning logic as in 02a-appso-programs Step 2 — institution CIP codes
# may be missing leading zeros or have the wrong number of digits, which prevents
# matching against the INFOWARE lookup tables.
# ---- Step 9: Fix CIP codes with wrong length ----

null_cleaning <- null_cleaning %>%
  mutate(
    PSI_CREDENTIAL_CIP = case_when(
      nchar(PSI_CREDENTIAL_CIP) == 6 & !grepl("\\.", substring(PSI_CREDENTIAL_CIP, 1, 2))
        ~ paste0(PSI_CREDENTIAL_CIP, "0"),
      TRUE ~ PSI_CREDENTIAL_CIP
    ),
    PSI_CREDENTIAL_CIP = case_when(
      nchar(PSI_CREDENTIAL_CIP) == 6
        ~ paste0("0", PSI_CREDENTIAL_CIP),
      TRUE ~ PSI_CREDENTIAL_CIP
    )
  )

# Try to match each cleaned CIP code to the official INFOWARE taxonomy using
# progressively shorter matches (exact 6-digit → 5-digit → general → 2-digit),
# same strategy as 02a-appso-programs Step 3.
# ---- Step 10: Match CIP codes from INFOWARE lookup tables ----

cip6 <- sch_tbl("INFOWARE_L_CIP_6DIGITS_CIP2016") %>%
  select(LCIP_CD_WITH_PERIOD, LCIP_LCP4_CD, LCIP_LCP2_CD) %>%
  collect()
cip6 <- cip6 |> rename_with(toupper)

cip4 <- sch_tbl("INFOWARE_L_CIP_4DIGITS_CIP2016") %>%
  select(LCP4_CD, LCP4_CIP_4DIGITS_NAME) %>%
  collect()
cip4 <- cip4 |> rename_with(toupper)

cip2 <- sch_tbl("INFOWARE_L_CIP_2DIGITS_CIP2016") %>%
  select(LCP2_CD, LCP2_DIGITS_NAME, LCP2_LCIPPC_CD, LCP2_LCIPPC_NAME) %>%
  collect()
cip2 <- cip2 |> rename_with(toupper)

# Step 10a: Exact match on full 6-digit CIP
null_cleaning <- null_cleaning %>%
  left_join(cip6, by = c("PSI_CREDENTIAL_CIP" = "LCIP_CD_WITH_PERIOD")) %>%
  rename(STP_CIP_CODE_4 = LCIP_LCP4_CD, STP_CIP_CODE_2 = LCIP_LCP2_CD)

# Step 10b: Partial match on first 5 digits (for those still NULL)
# Some valid CIPs differ only in the last digit from a known code
cip6_partial <- cip6 %>%
  mutate(PSI_CIP_5 = substr(LCIP_CD_WITH_PERIOD, 1, 5))

null_cleaning <- null_cleaning %>%
  mutate(PSI_CIP_5 = substr(PSI_CREDENTIAL_CIP, 1, 5)) %>%
  left_join(
    cip6_partial %>% filter(!duplicated(PSI_CIP_5)),
    by = "PSI_CIP_5"
  ) %>%
  mutate(
    STP_CIP_CODE_4 = coalesce(STP_CIP_CODE_4, LCIP_LCP4_CD),
    STP_CIP_CODE_2 = coalesce(STP_CIP_CODE_2, LCIP_LCP2_CD)
  ) %>%
  select(-PSI_CIP_5, -LCIP_CD_WITH_PERIOD, -LCIP_LCP4_CD, -LCIP_LCP2_CD)

# Step 10c: General program CIPs (XX.00 → XX.01)
# WHY: Some CIP families have a "general" code (XX.00) that doesn't exist in INFOWARE.
# Map these to the first specific sub-category as a reasonable default.
general_programs <- c("11.00", "13.00", "14.00", "19.00", "23.00", "24.00",
                       "26.00", "40.00", "42.00", "45.00", "50.00", "52.00", "55.00")

null_cleaning <- null_cleaning %>%
  mutate(
    STP_CIP_CODE_4 = case_when(
      substr(PSI_CREDENTIAL_CIP, 1, 5) %in% general_programs & is.na(STP_CIP_CODE_4)
        ~ paste0(substr(PSI_CREDENTIAL_CIP, 1, 2), "01"),
      TRUE ~ STP_CIP_CODE_4
    )
  )

# Step 10d: Fall back to first 2 digits for any still-unmatched 2-digit CIP codes
null_cleaning <- null_cleaning %>%
  mutate(PSI_CIP_2 = substr(PSI_CREDENTIAL_CIP, 1, 2)) %>%
  left_join(
    cip6 %>% mutate(PSI_CIP_2 = substr(LCIP_CD_WITH_PERIOD, 1, 2)) %>% filter(!duplicated(PSI_CIP_2)),
    by = "PSI_CIP_2"
  ) %>%
  mutate(STP_CIP_CODE_2 = coalesce(STP_CIP_CODE_2, LCIP_LCP2_CD)) %>%
  select(-PSI_CIP_2, -LCIP_CD_WITH_PERIOD, -LCIP_LCP4_CD, -LCIP_LCP2_CD)

# Add human-readable names for the matched CIP codes, needed for reporting and
# for analysts to verify that the CIP matches are sensible.
# ---- Step 11: Add CIP names from lookup tables ----

null_cleaning <- null_cleaning %>%
  left_join(cip4, by = c("STP_CIP_CODE_4" = "LCP4_CD")) %>%
  rename(STP_CIP_CODE_4_NAME = LCP4_CIP_4DIGITS_NAME)

null_cleaning <- null_cleaning %>%
  left_join(cip2, by = c("STP_CIP_CODE_2" = "LCP2_CD")) %>%
  rename(
    STP_CIP_CODE_2_NAME = LCP2_DIGITS_NAME,
    STP_CIP_CLUSTER_CODE = LCP2_LCIPPC_CD,
    STP_CIP_CLUSTER_NAME = LCP2_LCIPPC_NAME
  )

# Flag unmatched 4-digit CIPs so analysts can investigate
null_cleaning <- null_cleaning %>%
  mutate(STP_CIP_CODE_4_NAME = ifelse(is.na(STP_CIP_CODE_4_NAME), "Invalid 4-digit CIP", STP_CIP_CODE_4_NAME))

# Write cleaned NULL table back for the join step
dbWriteTable(con, "Credential_Non_Dup_STP_NULL_Cleaning",
             null_cleaning %>% select(-any_of(c("PSI_CIP_5"))), overwrite = TRUE)

# Join the cleaned CIP results back to the original credential records that had
# NULL final CIP codes, creating a lookup table for the final update in Step 13.
# Match on the original (pre-cleaning) CIP code and OUTCOMES_CRED to ensure
# each cleaned CIP maps to the correct credential records.
# ---- Step 12: Create ID list of NULL records with matched STP CIPs ----

null_ids <- cred_non_dup %>%
  filter(is.na(FINAL_CIP_CODE_4)) %>%
  inner_join(
    null_cleaning %>% select(PSI_CREDENTIAL_CIP_orig, OUTCOMES_CRED,
                              STP_CIP_CODE_4, STP_CIP_CODE_4_NAME,
                              STP_CIP_CODE_2, STP_CIP_CODE_2_NAME,
                              STP_CIP_CLUSTER_CODE, STP_CIP_CLUSTER_NAME),
    by = c("PSI_CREDENTIAL_CIP" = "PSI_CREDENTIAL_CIP_orig", "OUTCOMES_CRED")
  ) %>%
  mutate(
    FINAL_CIP_CODE_4 = STP_CIP_CODE_4,
    FINAL_CIP_CODE_4_NAME = STP_CIP_CODE_4_NAME,
    FINAL_CIP_CODE_2 = STP_CIP_CODE_2,
    FINAL_CIP_CODE_2_NAME = STP_CIP_CODE_2_NAME,
    FINAL_CIP_CLUSTER_CODE = STP_CIP_CLUSTER_CODE,
    FINAL_CIP_CLUSTER_NAME = STP_CIP_CLUSTER_NAME,
    PSI_PROGRAM_CODE = ifelse(PSI_PROGRAM_CODE == "(Unspecified)", NA_character_, PSI_PROGRAM_CODE)
  ) %>%
  select(ID, PSI_CODE, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
         PSI_CREDENTIAL_CIP, PSI_AWARD_SCHOOL_YEAR, OUTCOMES_CRED,
         FINAL_CIP_CODE_4, FINAL_CIP_CODE_4_NAME,
         FINAL_CIP_CODE_2, FINAL_CIP_CODE_2_NAME,
         FINAL_CIP_CLUSTER_CODE, FINAL_CIP_CLUSTER_NAME)

dbWriteTable(con, "Credential_Non_Dup_NULL_IDs", null_ids, overwrite = TRUE)

# This is the final merge — update the main credential table with the STP-derived
# CIP codes for records that weren't matched by any of the four primary sources.
# After this step, every record should have a FINAL_CIP_CODE_4 value.
# ---- Step 13: Apply the NULL CIP fallback to Credential_Non_Dup ----

null_updates <- null_ids %>%
  select(ID, FINAL_CIP_CODE_4, FINAL_CIP_CODE_4_NAME,
         FINAL_CIP_CODE_2, FINAL_CIP_CODE_2_NAME,
         FINAL_CIP_CLUSTER_CODE, FINAL_CIP_CLUSTER_NAME)

cred_non_dup <- cred_non_dup %>%
  rows_update(null_updates, by = "ID", unmatched = "ignore")

# Write final updated table
dbWriteTable(con, SQL(glue::glue('"{my_schema}"."Credential_Non_Dup"')),
             cred_non_dup, overwrite = TRUE)

# ---- Check for remaining NULLs ----
tbl(con, "Credential_Non_Dup") %>% filter(is.na(FINAL_CIP_CODE_4)) %>% count(OUTCOMES_CRED, FINAL_CIP_CODE_4)

# BGS records with CIP code "99" represent "undeclared activity" — they need
# their cluster names set explicitly. Also fill any remaining NULL FINAL_CIPs with
# the institution-reported STP values as an absolute last resort.
# ---- Step 14: Final cleanup of edge cases ----

cred_non_dup <- cred_non_dup %>%
  mutate(
    FINAL_CIP_CODE_2_NAME = case_when(
      OUTCOMES_CRED == "BGS" & FINAL_CIP_CODE_2 == "99" ~ "Undeclared activity",
      TRUE ~ FINAL_CIP_CODE_2_NAME
    ),
    FINAL_CIP_CLUSTER_CODE = case_when(
      OUTCOMES_CRED == "BGS" & FINAL_CIP_CODE_2 == "99" ~ "99",
      TRUE ~ FINAL_CIP_CLUSTER_CODE
    ),
    FINAL_CIP_CLUSTER_NAME = case_when(
      OUTCOMES_CRED == "BGS" & FINAL_CIP_CODE_2 == "99" ~ "Undeclared activity",
      TRUE ~ FINAL_CIP_CLUSTER_NAME
    )
  )

# Fall back to STP CIP for any records where FINAL_CIP is still NULL or blank
cred_non_dup <- cred_non_dup %>%
  mutate(
    FINAL_CIP_CODE_4 = case_when(
      is.na(FINAL_CIP_CODE_4) | FINAL_CIP_CODE_4 == " " ~ STP_CIP_CODE_4,
      TRUE ~ FINAL_CIP_CODE_4
    ),
    FINAL_CIP_CODE_4_NAME = case_when(
      is.na(FINAL_CIP_CODE_4) | FINAL_CIP_CODE_4 == " " ~ STP_CIP_CODE_4_NAME,
      TRUE ~ FINAL_CIP_CODE_4_NAME
    ),
    FINAL_CIP_CODE_2 = case_when(
      is.na(FINAL_CIP_CODE_4) | FINAL_CIP_CODE_4 == " " ~ STP_CIP_CODE_2,
      TRUE ~ FINAL_CIP_CODE_2
    ),
    FINAL_CIP_CODE_2_NAME = case_when(
      is.na(FINAL_CIP_CODE_4) | FINAL_CIP_CODE_4 == " " ~ STP_CIP_CODE_2_NAME,
      TRUE ~ FINAL_CIP_CODE_2_NAME
    )
  )

# Set cluster to '99'/'Undeclared activity' for GRAD records with null clusters and CIP 99
cred_non_dup <- cred_non_dup %>%
  mutate(
    FINAL_CIP_CLUSTER_CODE = case_when(
      OUTCOMES_CRED == "GRAD" & is.na(FINAL_CIP_CLUSTER_CODE) &
        is.na(FINAL_CIP_CLUSTER_NAME) & FINAL_CIP_CODE_2 == "99" ~ "99",
      TRUE ~ FINAL_CIP_CLUSTER_CODE
    ),
    FINAL_CIP_CLUSTER_NAME = case_when(
      OUTCOMES_CRED == "GRAD" & is.na(FINAL_CIP_CLUSTER_CODE) &
        is.na(FINAL_CIP_CLUSTER_NAME) & FINAL_CIP_CODE_2 == "99" ~ "Undeclared activity",
      TRUE ~ FINAL_CIP_CLUSTER_NAME
    )
  )

# Write final table
dbWriteTable(con, SQL(glue::glue('"{my_schema}"."Credential_Non_Dup"')),
             cred_non_dup, overwrite = TRUE)

# ---- Clean up ----
dbExecute(con, "DROP TABLE Credential_Non_Dup_STP_NULL_Cleaning")
dbDisconnect(con)
