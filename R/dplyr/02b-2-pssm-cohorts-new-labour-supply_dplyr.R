# New Labour Supply Distributions — dplyr Translation
# Original: R/02b-2-pssm-cohorts-new-labour-supply.R
#
# Pipeline context:
#   Processes cohort data from student outcomes and creates new labour supply
#   distributions. Runs after 02b-1-pssm-cohorts (which builds T_Cohorts_Recoded).
#
#   At a high level:
#     1. Updates invalid NOC codes in T_Cohorts_Recoded
#     2. Recodes NLS for records with NLS-2 but no NLS-1 (anti-join pattern)
#     3. Creates weights for new labour supply (Weight_NLS)
#     4. Builds 4 labour supply distribution tables (with/without TTRAIN, 4D/2D)
#     5. Appends StatCan census data
#
# Input tables:
#   - T_Cohorts_Recoded — unified cohort table (from 02b-1)
#   - T_Current_Region_PSSM_Codes / T_Current_Region_PSSM_Rollup_Codes — region lookups
#   - T_NOC_Broad_Categories — NOC validation lookup
#   - Labour_Supply_Distribution_Stat_Can — census data
#
# Output tables (4 variants + 1 weight table):
#   - Labour_Supply_Distribution — 4D CIP, with TTRAIN
#   - Labour_Supply_Distribution_No_TT — 4D CIP, without TTRAIN
#   - Labour_Supply_Distribution_LCP2 — 2D CIP, with TTRAIN
#   - Labour_Supply_Distribution_LCP2_No_TT — 2D CIP, without TTRAIN
#   - tmp_tbl_Weights_NLS — weight lookup kept for downstream use

library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(dbplyr)

# ---- Configure LAN and file paths ----
db_config <- config::get("decimal")
lan <- config::get("lan")
my_schema <- config::get("myschema")

decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")

# Helper: reference a table in the user's schema
sch_tbl <- function(name) {
  tbl(decimal_con, dbplyr::in_schema(my_schema, name))
}

# Helper: produce grad status prefix for composite keys.
# WHY: LCIP4_CRED and LCIP2_CRED conditionally prepend grad status with " - ".
# When grad status is NULL the prefix is empty; otherwise "status - ".
grad_prefix <- function(status) {
  if_else(is.na(status), "", paste0(status, " - "))
}

# ---- Check required tables ----
required_tables <- c(
  "t_cohorts_recoded",
  "t_current_region_pssm_rollup_codes",
  "t_current_region_pssm_codes",
  "T_NOC_Broad_Categories",
  "Labour_Supply_Distribution_Stat_Can"
)

for (table_name in required_tables) {
  full_table_name <- SQL(glue::glue('"{my_schema}"."{table_name}"'))
  assertthat::assert_that(
    dbExistsTable(decimal_con, full_table_name),
    msg = paste("Error:", table_name, "does not exist in schema", my_schema)
  )
}


# ******************************************************************************
# Part 0: Pull source data into R
# ******************************************************************************
# WHY: The entire pipeline operates on T_Cohorts_Recoded plus lookup tables.
# Pulling them into R once avoids repeated database round-trips.

cohorts <- sch_tbl("T_Cohorts_Recoded") %>%
  collect() |> rename_with(toupper)

region_codes <- sch_tbl("T_Current_Region_PSSM_Codes") %>%
  collect() |> rename_with(toupper)

region_rollup <- sch_tbl("T_Current_Region_PSSM_Rollup_Codes") %>%
  collect() |> rename_with(toupper)

noc_broad <- sch_tbl("T_NOC_Broad_Categories") %>%
  collect() |> rename_with(toupper)


# ******************************************************************************
# Part 1: Handle invalid NOC codes
# ******************************************************************************
# WHY: Some NOC codes in the cohort data are invalid (e.g., '4122X') and need to
# be set to '99999' (unknown). Also diagnostic checks for other invalid NOCs.

# Check base weights
cohorts %>%
  count(SURVEY, SURVEY_YEAR, WEIGHT)

# Find invalid NOC codes: records with a NOC that doesn't exist in the
# official NOC broad categories lookup, filtered to recent survey years.
# WHY: Identifies data quality issues that may need manual correction.
invalid_nocs <- cohorts %>%
  filter(!is.na(AGE_GROUP_ROLLUP),
         CURRENT_REGION_PSSM_CODE != -1,
         SURVEY_YEAR %in% c("2019", "2020", "2021", "2022", "2023"),
         !is.na(NOC_CD),
         NOC_CD != "") %>%
  anti_join(noc_broad, by = c("NOC_CD" = "UNIT_GROUP_CODE")) %>%
  distinct(STQU_ID, SURVEY, SURVEY_YEAR, NOC_CD)

# Fix known invalid NOC code 4122X → 99999
cohorts <- cohorts %>%
  mutate(NOC_CD = if_else(NOC_CD == "4122X", "99999", NOC_CD))


# ******************************************************************************
# Part 2: Recode NLS for records with NLS-2 but no NLS-1
# ******************************************************************************
# WHY: If a cohort member has a new labour supply value of 2 (NLS-2) but there's
# no matching NLS-1 record with the same survey/region/age/institution/credential,
# the NLS is recoded to 3 (indicating NLS-2 without NLS-1).
# This uses an anti-join pattern: find NLS-2 records that have no NLS-1 match.

# Common filter for both NLS-1 and NLS-2 queries: weight > 0, valid age, grad or non-completer
nls_filter <- function(df) {
  df %>%
    filter(as.numeric(WEIGHT) > 0,
           !is.na(AGE_GROUP_ROLLUP),
           GRAD_STATUS %in% c("1", "3"))
}

# NLS-1 records: group by key dimensions, filter to NLS=1
# WHY: We join with region rollup to get the rollup code for matching.
cohorts_with_region <- cohorts %>%
  inner_join(region_codes %>% select(CURRENT_REGION_PSSM_CODE, CURRENT_REGION_PSSM_CODE_ROLLOUP),
             by = "CURRENT_REGION_PSSM_CODE") %>%
  inner_join(region_rollup %>% select(CURRENT_REGION_PSSM_CODE_ROLLOUP),
             by = "CURRENT_REGION_PSSM_CODE_ROLLOUP")

nls1_keys <- cohorts_with_region %>%
  nls_filter() %>%
  filter(as.numeric(NEW_LABOUR_SUPPLY) == 1) %>%
  distinct(SURVEY, CURRENT_REGION_PSSM_CODE_ROLLOUP, AGE_GROUP_ROLLUP,
           INST_CD, LCIP4_CRED)

# NLS-2 records: find those with no matching NLS-1 record
nls2_no_nls1 <- cohorts_with_region %>%
  nls_filter() %>%
  filter(as.numeric(NEW_LABOUR_SUPPLY) == 2) %>%
  anti_join(nls1_keys,
            by = c("SURVEY", "CURRENT_REGION_PSSM_CODE_ROLLOUP",
                   "AGE_GROUP_ROLLUP", "INST_CD", "LCIP4_CRED"))

# Recode NLS from 2 → 3 for those without NLS-1
# WHY: The anti-join identifies exactly which STQU_IDs need recoding.
nls2_ids <- nls2_no_nls1 %>% distinct(STQU_ID)

cohorts <- cohorts %>%
  mutate(NEW_LABOUR_SUPPLY = if_else(
    STQU_ID %in% nls2_ids$STQU_ID & as.numeric(NEW_LABOUR_SUPPLY) == 2,
    3,
    NEW_LABOUR_SUPPLY
  ))


# ******************************************************************************
# Part 3: Count respondents by region (diagnostic)
# ******************************************************************************
# WHY: Provides a diagnostic view of cohort respondent counts by survey,
# year, and region for verification purposes.

cohorts_by_region <- cohorts %>%
  inner_join(region_codes %>% select(CURRENT_REGION_PSSM_CODE, CURRENT_REGION_PSSM_NAME),
             by = "CURRENT_REGION_PSSM_CODE") %>%
  filter(!is.na(AGE_GROUP_ROLLUP), RESPONDENT == "1", as.numeric(WEIGHT) > 0) %>%
  count(SURVEY, SURVEY_YEAR, CURRENT_REGION_PSSM_CODE, CURRENT_REGION_PSSM_NAME,
        AGE_GROUP_ROLLUP, name = "N", sort = TRUE)


# ******************************************************************************
# Part 4: Create NLS weights (Z01 → Z08)
# ******************************************************************************
# WHY: The NLS weight adjusts for non-response bias. It's computed as:
#   Weight_Prob = Count / Respondents (probability of selection)
#   Weight = Weight_Prob * Weight_Year (combine with year weight)
#   Weight_Adj_Fac = Base / Weighted (adjustment to match cohort totals)
#   Weight_NLS = Weight * Weight_Adj_Fac (final NLS weight)
# In the original, this was 8 temp tables. Here it's a single pipeline.

# ---- Z01: Base NLS records ----
# Filter to NLS in {0,1,2,3}, weight > 0, valid age, grad status 1 or 3
# WHY: These are the cohort records eligible for NLS weighting.
base_nls <- cohorts %>%
  filter(as.numeric(NEW_LABOUR_SUPPLY) %in% c(0, 1, 2, 3),
         as.numeric(WEIGHT) > 0,
         !is.na(AGE_GROUP_ROLLUP),
         GRAD_STATUS %in% c("1", "3"))

# ---- Z02c: Weight_tmp ----
# Count records and respondents by survey/year/institution/age/grad_status/ttrain/lcip4_cred
# WHY: Respondents are those who responded AND have a valid region code (not -1).
weight_tmp <- base_nls %>%
  group_by(SURVEY, SURVEY_YEAR, INST_CD, AGE_GROUP_ROLLUP, GRAD_STATUS,
           TTRAIN, LCIP4_CRED, WEIGHT) %>%
  summarise(
    COUNT = n(),
    RESPONDENTS = sum(if_else(RESPONDENT == "1" & CURRENT_REGION_PSSM_CODE != -1, 1, 0)),
    .groups = "drop"
  ) %>%
  rename(WEIGHT_YEAR = WEIGHT)

# ---- Z02c → Z04: Compute Weight_Prob, Weight, Weighted, then aggregate ----
# WHY: Weight_Prob = Count/Respondents adjusts for non-response. When Respondents=0
# (no respondents for that cell), Weight_Prob is set to 1 (no adjustment).
weight_full <- weight_tmp %>%
  mutate(
    WEIGHT_PROB = if_else(RESPONDENTS == 0, 1, as.numeric(COUNT) / as.numeric(RESPONDENTS)),
    WEIGHT = WEIGHT_PROB * as.numeric(WEIGHT_YEAR),
    WEIGHTED = as.numeric(RESPONDENTS) * WEIGHT_PROB * as.numeric(WEIGHT_YEAR)
  )

# ---- Z03: Weight_Total ----
# Aggregate weighted counts to the grouping level used for adjustment.
weight_total <- weight_full %>%
  group_by(SURVEY, INST_CD, AGE_GROUP_ROLLUP, GRAD_STATUS, TTRAIN, LCIP4_CRED) %>%
  summarise(
    BASE = sum(COUNT),
    WEIGHTED_TOTAL = sum(WEIGHTED),
    .groups = "drop"
  )

# ---- Z04: Weight_Adj_Fac ----
# WHY: The adjustment factor scales the weights so that the weighted total
# matches the base (unweighted) count. This corrects for over/under-representation.
weight_adj_fac <- weight_total %>%
  mutate(WEIGHT_ADJ_FAC = if_else(WEIGHTED_TOTAL == 0, 0,
                                   as.numeric(BASE) / as.numeric(WEIGHTED_TOTAL)))

# ---- Z05: tmp_tbl_weights_nls ----
# Join weight_full with adjustment factor and compute final Weight_NLS.
# WHY: This is the final weight table that gets applied to T_Cohorts_Recoded.
tmp_tbl_weights_nls <- weight_full %>%
  inner_join(
    weight_adj_fac %>% select(SURVEY, INST_CD, AGE_GROUP_ROLLUP, GRAD_STATUS,
                               TTRAIN, LCIP4_CRED, WEIGHT_ADJ_FAC),
    by = c("SURVEY", "INST_CD", "AGE_GROUP_ROLLUP", "GRAD_STATUS",
           "TTRAIN", "LCIP4_CRED")
  ) %>%
  mutate(WEIGHT_NLS = WEIGHT * WEIGHT_ADJ_FAC)

# ---- Z06-Z07: Add Weight_NLS column to T_Cohorts_Recoded ----
# KEPT AS SQL: ALTER TABLE to add/modify the Weight_NLS column
dbExecute(decimal_con, "ALTER TABLE T_Cohorts_Recoded ALTER COLUMN Weight_NLS Float NULL;")

# ---- Z07: Null out Weight_NLS (in R memory) ----
cohorts <- cohorts %>%
  mutate(WEIGHT_NLS = NA_real_)

# ---- Z08: Update Weight_NLS from tmp_tbl_weights_nls ----
# WHY: Only update records with valid region (not -1) that are in the base_nls set.
# Match on survey/year/inst/age/grad_status/ttrain/lcip4_cred.
base_nls_ids <- base_nls %>%
  distinct(STQU_ID)

weight_nls_lookup <- tmp_tbl_weights_nls %>%
  select(SURVEY, SURVEY_YEAR, INST_CD, AGE_GROUP_ROLLUP, GRAD_STATUS,
         TTRAIN, LCIP4_CRED, WEIGHT_NLS)

# Build the update: for each record in cohorts that is in base_nls and has
# valid region, look up the Weight_NLS from the weight table.
cohorts_to_update <- cohorts %>%
  semi_join(base_nls_ids, by = "STQU_ID") %>%
  filter(CURRENT_REGION_PSSM_CODE != -1) %>%
  inner_join(weight_nls_lookup,
             by = c("SURVEY", "SURVEY_YEAR", "INST_CD", "AGE_GROUP_ROLLUP",
                    "GRAD_STATUS", "TTRAIN", "LCIP4_CRED")) %>%
  select(STQU_ID, WEIGHT_NLS)

# Apply the update: replace Weight_NLS for matched records
cohorts <- cohorts %>%
  left_join(cohorts_to_update %>% rename(WEIGHT_NLS_NEW = WEIGHT_NLS),
            by = "STQU_ID") %>%
  mutate(WEIGHT_NLS = if_else(!is.na(WEIGHT_NLS_NEW), WEIGHT_NLS_NEW, WEIGHT_NLS)) %>%
  select(-WEIGHT_NLS_NEW)

# ---- Z09: Check weights (diagnostic) ----
# WHY: Verify that Weight_NLS values are reasonable. Records with Weight_NLS=0 or NULL
# and valid region should be investigated (may indicate missing data).
check_no_weight <- cohorts %>%
  semi_join(base_nls_ids, by = "STQU_ID") %>%
  filter(as.numeric(NEW_LABOUR_SUPPLY) %in% c(0, 1, 2, 3),
         as.numeric(WEIGHT) > 0,
         (is.na(WEIGHT_NLS) | WEIGHT_NLS == 0),
         CURRENT_REGION_PSSM_CODE != -1,
         !is.na(AGE_GROUP_ROLLUP),
         GRAD_STATUS %in% c("1", "2", "3")) %>%
  count(SURVEY, INST_CD, AGE_GROUP_ROLLUP, TTRAIN, LCIP4_CRED, GRAD_STATUS, name = "BASE")


# ******************************************************************************
# Part 5: Weighted labour supply distributions (Q006a → Q007a)
# ******************************************************************************
# WHY: This is the core computation. We compute weighted counts of respondents
# by NLS status across various grouping dimensions, then compute percentages.
# The result feeds 4 output tables:
#   - 4D CIP with TTRAIN
#   - 4D CIP without TTRAIN
#   - 2D CIP with TTRAIN
#   - 2D CIP without TTRAIN

# ---- Q006a: Weight respondents by NLS ----
# Join cohorts with region rollup, filter to respondents with valid weight.
# WHY: Only respondents (RESPONDENT='1') with positive weight contribute to
# the distribution. Region rollup code 9999 is excluded (unknown region).
weight_new_ls <- cohorts %>%
  inner_join(region_codes %>% select(CURRENT_REGION_PSSM_CODE, CURRENT_REGION_PSSM_CODE_ROLLOUP),
             by = "CURRENT_REGION_PSSM_CODE") %>%
  inner_join(region_rollup %>% select(CURRENT_REGION_PSSM_CODE_ROLLOUP),
             by = "CURRENT_REGION_PSSM_CODE_ROLLOUP") %>%
  filter(RESPONDENT == "1",
         as.numeric(WEIGHT) > 0,
         CURRENT_REGION_PSSM_CODE_ROLLOUP != 9999,
         !is.na(AGE_GROUP_ROLLUP),
         GRAD_STATUS %in% c("1", "3"),
         as.numeric(NEW_LABOUR_SUPPLY) %in% c(0, 1, 2, 3)) %>%
  mutate(WEIGHT_NLS = as.numeric(WEIGHT_NLS)) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
           SURVEY_YEAR, INST_CD, AGE_GROUP_ROLLUP, GRAD_STATUS,
           LCP4_CD, TTRAIN, LCIP4_CRED, LCIP2_CRED,
           NEW_LABOUR_SUPPLY, WEIGHT_NLS) %>%
  summarise(COUNT = n(), .groups = "drop") %>%
  mutate(WEIGHTED = COUNT * WEIGHT_NLS)

# ---- Q006b variants: Aggregate weighted counts by NLS status ----
# WHY: We need separate aggregations for NLS=1,2,3 (new labour supply) and NLS=0
# (no new labour supply), across 4 grouping variants.

# Variant A: NLS=1,2,3 with TTRAIN (4D CIP)
q006b_nls123 <- weight_new_ls %>%
  filter(as.numeric(NEW_LABOUR_SUPPLY) %in% c(1, 2, 3)) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
           AGE_GROUP_ROLLUP, LCP4_CD, TTRAIN, LCIP4_CRED, LCIP2_CRED) %>%
  summarise(COUNT = sum(WEIGHTED), UNWEIGHTED_COUNT = sum(COUNT), .groups = "drop")

# Variant B: NLS=0 with TTRAIN (4D CIP)
q006b_nls0 <- weight_new_ls %>%
  filter(as.numeric(NEW_LABOUR_SUPPLY) == 0) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
           AGE_GROUP_ROLLUP, LCP4_CD, TTRAIN, LCIP4_CRED, LCIP2_CRED) %>%
  summarise(COUNT = sum(WEIGHTED), .groups = "drop")

# Variant C: NLS=0 without TTRAIN (4D CIP, no TTRAIN dimension)
# WHY: No-TT variants drop TTRAIN from grouping and reconstruct LCIP4_CRED/LCIP2_CRED
# without TTRAIN but still including grad_status prefix.
q006b_nls0_no_tt <- weight_new_ls %>%
  filter(as.numeric(NEW_LABOUR_SUPPLY) == 0) %>%
  mutate(
    LCIP4_CRED_NT = paste0(grad_prefix(GRAD_STATUS), LCP4_CD, " - ", PSSM_CREDENTIAL),
    LCIP2_CRED_NT = paste0(grad_prefix(GRAD_STATUS), str_sub(LCP4_CD, 1, 2), " - ", PSSM_CREDENTIAL)
  ) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
           AGE_GROUP_ROLLUP, LCP4_CD, LCIP4_CRED_NT, LCIP2_CRED_NT) %>%
  summarise(COUNT = sum(WEIGHTED), .groups = "drop") %>%
  rename(LCIP4_CRED = LCIP4_CRED_NT, LCIP2_CRED = LCIP2_CRED_NT)

# Variant D: NLS=1,2,3 without TTRAIN (4D CIP, no TTRAIN)
q006b_nls123_no_tt <- weight_new_ls %>%
  filter(as.numeric(NEW_LABOUR_SUPPLY) %in% c(1, 2, 3)) %>%
  mutate(
    LCIP4_CRED_NT = paste0(grad_prefix(GRAD_STATUS), LCP4_CD, " - ", PSSM_CREDENTIAL),
    LCIP2_CRED_NT = paste0(grad_prefix(GRAD_STATUS), str_sub(LCP4_CD, 1, 2), " - ", PSSM_CREDENTIAL)
  ) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
           AGE_GROUP_ROLLUP, LCP4_CD, LCIP4_CRED_NT, LCIP2_CRED_NT) %>%
  summarise(COUNT = sum(WEIGHTED), UNWEIGHTED_COUNT = sum(COUNT), .groups = "drop") %>%
  rename(LCIP4_CRED = LCIP4_CRED_NT, LCIP2_CRED = LCIP2_CRED_NT)

# Variant E: NLS=0 with TTRAIN (2D CIP)
q006b_nls0_2d <- weight_new_ls %>%
  filter(as.numeric(NEW_LABOUR_SUPPLY) == 0) %>%
  mutate(LCP2_CD = str_sub(LCP4_CD, 1, 2)) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
           AGE_GROUP_ROLLUP, LCP2_CD, TTRAIN, LCIP2_CRED) %>%
  summarise(COUNT = sum(WEIGHTED), .groups = "drop") %>%
  rename(LCP2_CRED = LCIP2_CRED)

# Variant F: NLS=0 without TTRAIN (2D CIP)
q006b_nls0_2d_no_tt <- weight_new_ls %>%
  filter(as.numeric(NEW_LABOUR_SUPPLY) == 0) %>%
  mutate(
    LCP2_CD = str_sub(LCP4_CD, 1, 2),
    LCP2_CRED_NT = paste0(
      if_else(str_sub(PSSM_CRED, 1, 1) %in% c("1", "3"),
              paste0(str_sub(PSSM_CRED, 1, 1), " - "), ""),
      str_sub(LCP4_CD, 1, 2), " - ", PSSM_CREDENTIAL
    )
  ) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
           AGE_GROUP_ROLLUP, LCP2_CD, LCP2_CRED_NT) %>%
  summarise(COUNT = sum(WEIGHTED), .groups = "drop") %>%
  rename(LCP2_CRED = LCP2_CRED_NT)

# Variant G: NLS=1,2,3 with TTRAIN (2D CIP)
q006b_nls123_2d <- weight_new_ls %>%
  filter(as.numeric(NEW_LABOUR_SUPPLY) %in% c(1, 2, 3)) %>%
  mutate(LCP2_CD = str_sub(LCP4_CD, 1, 2)) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
           AGE_GROUP_ROLLUP, LCP2_CD, TTRAIN, LCIP2_CRED) %>%
  summarise(COUNT = sum(WEIGHTED), .groups = "drop") %>%
  rename(LCP2_CRED = LCIP2_CRED)

# Variant H: NLS=1,2,3 without TTRAIN (2D CIP)
q006b_nls123_2d_no_tt <- weight_new_ls %>%
  filter(as.numeric(NEW_LABOUR_SUPPLY) %in% c(1, 2, 3)) %>%
  mutate(
    LCP2_CD = str_sub(LCP4_CD, 1, 2),
    LCP2_CRED_NT = paste0(
      if_else(str_sub(PSSM_CRED, 1, 1) %in% c("1", "3"),
              paste0(str_sub(PSSM_CRED, 1, 1), " - "), ""),
      str_sub(LCP4_CD, 1, 2), " - ", PSSM_CREDENTIAL
    )
  ) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
           AGE_GROUP_ROLLUP, LCP2_CD, LCP2_CRED_NT) %>%
  summarise(COUNT = sum(WEIGHTED), .groups = "drop") %>%
  rename(LCP2_CRED = LCP2_CRED_NT)

# ---- Q006b Totals: Denominators for percentage computation ----
# WHY: Each variant needs a matching total (denominator). Totals include ALL
# NLS values (0,1,2,3) grouped at the same level but without region.

# Total with TTRAIN (4D CIP)
total_4d <- weight_new_ls %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP_ROLLUP,
           LCP4_CD, TTRAIN, LCIP4_CRED, LCIP2_CRED) %>%
  summarise(TOTAL = sum(WEIGHTED), .groups = "drop")

# Total without TTRAIN (4D CIP)
total_4d_no_tt <- weight_new_ls %>%
  mutate(
    LCIP4_CRED_NT = paste0(grad_prefix(GRAD_STATUS), LCP4_CD, " - ", PSSM_CREDENTIAL),
    LCIP2_CRED_NT = paste0(grad_prefix(GRAD_STATUS), str_sub(LCP4_CD, 1, 2), " - ", PSSM_CREDENTIAL)
  ) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP_ROLLUP,
           LCP4_CD, LCIP4_CRED_NT, LCIP2_CRED_NT) %>%
  summarise(TOTAL = sum(WEIGHTED), .groups = "drop") %>%
  rename(LCIP4_CRED = LCIP4_CRED_NT, LCIP2_CRED = LCIP2_CRED_NT)

# Total with TTRAIN (2D CIP)
total_2d <- weight_new_ls %>%
  mutate(LCP2_CD = str_sub(LCP4_CD, 1, 2)) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP_ROLLUP,
           LCP2_CD, TTRAIN, LCIP2_CRED) %>%
  summarise(TOTAL = sum(WEIGHTED), .groups = "drop") %>%
  rename(LCP2_CRED = LCIP2_CRED)

# Total without TTRAIN (2D CIP)
total_2d_no_tt <- weight_new_ls %>%
  mutate(
    LCP2_CD = str_sub(LCP4_CD, 1, 2),
    LCP2_CRED_NT = paste0(
      if_else(str_sub(PSSM_CRED, 1, 1) %in% c("1", "3"),
              paste0(str_sub(PSSM_CRED, 1, 1), " - "), ""),
      str_sub(LCP4_CD, 1, 2), " - ", PSSM_CREDENTIAL
    )
  ) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP_ROLLUP,
           LCP2_CD, LCP2_CRED_NT) %>%
  summarise(TOTAL = sum(WEIGHTED), .groups = "drop") %>%
  rename(LCP2_CRED = LCP2_CRED_NT)


# ---- Q007a: Compute percentages (count / total) ----
# WHY: The percentage represents the share of new labour supply within each
# program/age group. For NLS=1,2,3 it's count/total (direct percentage).
# For NLS=0 it's 1 - count/total (complement: percentage NOT in NLS=0).

# 4D with TTRAIN: NLS=1,2,3 percentage
q007a_4d <- total_4d %>%
  left_join(q006b_nls123 %>% select(PSSM_CREDENTIAL, PSSM_CRED,
             CURRENT_REGION_PSSM_CODE_ROLLOUP, AGE_GROUP_ROLLUP,
             LCIP4_CRED, COUNT),
    by = c("PSSM_CREDENTIAL", "PSSM_CRED", "AGE_GROUP_ROLLUP", "LCIP4_CRED")
  ) %>%
  filter(!is.na(CURRENT_REGION_PSSM_CODE_ROLLOUP)) %>%
  mutate(PERC = if_else(is.na(COUNT), 0, COUNT) / TOTAL)

# 4D with TTRAIN: NLS=0 percentage (1 - count/total, only where result=0)
# WHY: This captures cases where NLS=0 accounts for 100% (no new labour supply).
q007a_4d_nls0 <- total_4d %>%
  left_join(q006b_nls0 %>% select(PSSM_CREDENTIAL, PSSM_CRED,
             CURRENT_REGION_PSSM_CODE_ROLLOUP, AGE_GROUP_ROLLUP,
             LCIP4_CRED, COUNT),
    by = c("PSSM_CREDENTIAL", "PSSM_CRED", "AGE_GROUP_ROLLUP", "LCIP4_CRED")
  ) %>%
  filter(!is.na(COUNT), COUNT > 0) %>%
  mutate(PERC = 1 - (if_else(is.na(COUNT), 0, COUNT) / TOTAL)) %>%
  filter(PERC == 0)

# 4D no TTRAIN: NLS=1,2,3 percentage
q007a_4d_no_tt <- total_4d_no_tt %>%
  left_join(q006b_nls123_no_tt %>% select(PSSM_CREDENTIAL, PSSM_CRED,
             CURRENT_REGION_PSSM_CODE_ROLLOUP, AGE_GROUP_ROLLUP,
             LCIP4_CRED, COUNT),
    by = c("PSSM_CREDENTIAL", "PSSM_CRED", "AGE_GROUP_ROLLUP", "LCIP4_CRED")
  ) %>%
  filter(!is.na(CURRENT_REGION_PSSM_CODE_ROLLOUP)) %>%
  mutate(PERC = if_else(is.na(COUNT), 0, COUNT) / TOTAL)

# 4D no TTRAIN: NLS=0 percentage
q007a_4d_nls0_no_tt <- total_4d_no_tt %>%
  left_join(q006b_nls0_no_tt %>% select(PSSM_CREDENTIAL, PSSM_CRED,
             CURRENT_REGION_PSSM_CODE_ROLLOUP, AGE_GROUP_ROLLUP,
             LCIP4_CRED, COUNT),
    by = c("PSSM_CREDENTIAL", "PSSM_CRED", "AGE_GROUP_ROLLUP", "LCIP4_CRED")
  ) %>%
  filter(!is.na(COUNT), COUNT > 0) %>%
  mutate(PERC = 1 - (if_else(is.na(COUNT), 0, COUNT) / TOTAL)) %>%
  filter(PERC == 0)

# 2D with TTRAIN: NLS=1,2,3 percentage
q007a_2d <- total_2d %>%
  left_join(q006b_nls123_2d %>% select(PSSM_CREDENTIAL, PSSM_CRED,
             CURRENT_REGION_PSSM_CODE_ROLLOUP, AGE_GROUP_ROLLUP,
             LCP2_CRED, COUNT),
    by = c("PSSM_CREDENTIAL", "PSSM_CRED", "AGE_GROUP_ROLLUP", "LCP2_CRED")
  ) %>%
  filter(!is.na(CURRENT_REGION_PSSM_CODE_ROLLOUP)) %>%
  mutate(PERC = if_else(is.na(COUNT), 0, COUNT) / TOTAL)

# 2D no TTRAIN: NLS=1,2,3 percentage
q007a_2d_no_tt <- total_2d_no_tt %>%
  left_join(q006b_nls123_2d_no_tt %>% select(PSSM_CREDENTIAL, PSSM_CRED,
             CURRENT_REGION_PSSM_CODE_ROLLOUP, AGE_GROUP_ROLLUP,
             LCP2_CRED, COUNT),
    by = c("PSSM_CREDENTIAL", "PSSM_CRED", "AGE_GROUP_ROLLUP", "LCP2_CRED")
  ) %>%
  filter(!is.na(CURRENT_REGION_PSSM_CODE_ROLLOUP)) %>%
  mutate(PERC = if_else(is.na(COUNT), 0, COUNT) / TOTAL)

# 2D with TTRAIN: NLS=0 percentage
q007a_2d_nls0 <- total_2d %>%
  left_join(q006b_nls0_2d %>% select(PSSM_CREDENTIAL, PSSM_CRED,
             CURRENT_REGION_PSSM_CODE_ROLLOUP, AGE_GROUP_ROLLUP,
             LCP2_CRED, COUNT),
    by = c("PSSM_CREDENTIAL", "PSSM_CRED", "AGE_GROUP_ROLLUP", "LCP2_CRED")
  ) %>%
  filter(!is.na(COUNT), COUNT > 0) %>%
  mutate(PERC = 1 - (if_else(is.na(COUNT), 0, COUNT) / TOTAL)) %>%
  filter(PERC == 0)

# 2D no TTRAIN: NLS=0 percentage
q007a_2d_nls0_no_tt <- total_2d_no_tt %>%
  left_join(q006b_nls0_2d_no_tt %>% select(PSSM_CREDENTIAL, PSSM_CRED,
             CURRENT_REGION_PSSM_CODE_ROLLOUP, AGE_GROUP_ROLLUP,
             LCP2_CRED, COUNT),
    by = c("PSSM_CREDENTIAL", "PSSM_CRED", "AGE_GROUP_ROLLUP", "LCP2_CRED")
  ) %>%
  filter(!is.na(COUNT), COUNT > 0) %>%
  mutate(PERC = 1 - (if_else(is.na(COUNT), 0, COUNT) / TOTAL)) %>%
  filter(PERC == 0)


# ******************************************************************************
# Part 6: Build and write final distribution tables
# ******************************************************************************
# WHY: Each of the 4 output tables gets rows from both NLS=1,2,3 (direct %)
# and NLS=0 (complement %). In the original, this was DELETE + INSERT INTO.
# Here we filter existing data, bind new rows, and overwrite.

# Standardize column names for output.
# The 4D tables have: SURVEY, PSSM_CREDENTIAL, PSSM_CRED,
#   CURRENT_REGION_PSSM_CODE_ROLLOUP, AGE_GROUP_ROLLUP, LCP4_CD,
#   TTRAIN (or not for No_TT), LCIP4_CRED, LCIP2_CRED, COUNT, TOTAL, PERC
# The 2D tables have similar but use LCP2_CD/LCP2_CRED instead of LCP4/LCIP4.

# ---- Labour_Supply_Distribution (4D, with TTRAIN) ----
ls_4d_nls123 <- q007a_4d %>%
  mutate(SURVEY = "Student Outcomes") %>%
  select(SURVEY, PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
         AGE_GROUP_ROLLUP, LCP4_CD, TTRAIN, LCIP4_CRED, LCIP2_CRED,
         COUNT, TOTAL, NEW_LABOUR_SUPPLY = PERC)

ls_4d_nls0 <- q007a_4d_nls0 %>%
  mutate(SURVEY = "Student Outcomes") %>%
  select(SURVEY, PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
         AGE_GROUP_ROLLUP, LCP4_CD, TTRAIN, LCIP4_CRED, LCIP2_CRED,
         COUNT, TOTAL, NEW_LABOUR_SUPPLY = PERC)

ls_new <- bind_rows(ls_4d_nls123, ls_4d_nls0)

# Read existing data, remove old "Student Outcomes" rows, append new
if (dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Labour_Supply_Distribution"')))) {
  ls_existing <- sch_tbl("Labour_Supply_Distribution") %>%
    collect() |> rename_with(toupper) %>%
    filter(SURVEY != "Student Outcomes")
  ls_final <- bind_rows(ls_existing, ls_new)
} else {
  ls_final <- ls_new
}

dbWriteTable(decimal_con, SQL(glue::glue('"{my_schema}"."Labour_Supply_Distribution"')),
             ls_final, overwrite = TRUE)


# ---- Labour_Supply_Distribution_No_TT (4D, no TTRAIN) ----
ls_4d_no_tt_nls123 <- q007a_4d_no_tt %>%
  mutate(SURVEY = "Student Outcomes") %>%
  select(SURVEY, PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
         AGE_GROUP_ROLLUP, LCP4_CD, LCIP4_CRED, LCIP2_CRED,
         COUNT, TOTAL, NEW_LABOUR_SUPPLY = PERC)

ls_4d_no_tt_nls0 <- q007a_4d_nls0_no_tt %>%
  mutate(SURVEY = "Student Outcomes") %>%
  select(SURVEY, PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
         AGE_GROUP_ROLLUP, LCP4_CD, LCIP4_CRED, LCIP2_CRED,
         COUNT, TOTAL, NEW_LABOUR_SUPPLY = PERC)

ls_no_tt_new <- bind_rows(ls_4d_no_tt_nls123, ls_4d_no_tt_nls0)

if (dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Labour_Supply_Distribution_No_TT"')))) {
  ls_no_tt_existing <- sch_tbl("Labour_Supply_Distribution_No_TT") %>%
    collect() |> rename_with(toupper) %>%
    filter(SURVEY != "Student Outcomes")
  ls_no_tt_final <- bind_rows(ls_no_tt_existing, ls_no_tt_new)
} else {
  ls_no_tt_final <- ls_no_tt_new
}

dbWriteTable(decimal_con, SQL(glue::glue('"{my_schema}"."Labour_Supply_Distribution_No_TT"')),
             ls_no_tt_final, overwrite = TRUE)


# ---- Labour_Supply_Distribution_LCP2 (2D, with TTRAIN) ----
ls_2d_nls123 <- q007a_2d %>%
  mutate(SURVEY = "Student Outcomes") %>%
  select(SURVEY, PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
         AGE_GROUP_ROLLUP, LCP2_CD, TTRAIN, LCP2_CRED,
         COUNT, TOTAL, NEW_LABOUR_SUPPLY = PERC)

ls_2d_nls0 <- q007a_2d_nls0 %>%
  mutate(SURVEY = "Student Outcomes") %>%
  select(SURVEY, PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
         AGE_GROUP_ROLLUP, LCP2_CD, TTRAIN, LCP2_CRED,
         COUNT, TOTAL, NEW_LABOUR_SUPPLY = PERC)

ls_2d_new <- bind_rows(ls_2d_nls123, ls_2d_nls0)

if (dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Labour_Supply_Distribution_LCP2"')))) {
  ls_2d_existing <- sch_tbl("Labour_Supply_Distribution_LCP2") %>%
    collect() |> rename_with(toupper) %>%
    filter(SURVEY != "Student Outcomes")
  ls_2d_final <- bind_rows(ls_2d_existing, ls_2d_new)
} else {
  ls_2d_final <- ls_2d_new
}

dbWriteTable(decimal_con, SQL(glue::glue('"{my_schema}"."Labour_Supply_Distribution_LCP2"')),
             ls_2d_final, overwrite = TRUE)


# ---- Labour_Supply_Distribution_LCP2_No_TT (2D, no TTRAIN) ----
ls_2d_no_tt_nls123 <- q007a_2d_no_tt %>%
  mutate(SURVEY = "Student Outcomes") %>%
  select(SURVEY, PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
         AGE_GROUP_ROLLUP, LCP2_CD, LCP2_CRED,
         COUNT, TOTAL, NEW_LABOUR_SUPPLY = PERC)

ls_2d_no_tt_nls0 <- q007a_2d_nls0_no_tt %>%
  mutate(SURVEY = "Student Outcomes") %>%
  select(SURVEY, PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
         AGE_GROUP_ROLLUP, LCP2_CD, LCP2_CRED,
         COUNT, TOTAL, NEW_LABOUR_SUPPLY = PERC)

ls_2d_no_tt_new <- bind_rows(ls_2d_no_tt_nls123, ls_2d_no_tt_nls0)

if (dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Labour_Supply_Distribution_LCP2_No_TT"')))) {
  ls_2d_no_tt_existing <- sch_tbl("Labour_Supply_Distribution_LCP2_No_TT") %>%
    collect() |> rename_with(toupper) %>%
    filter(SURVEY != "Student Outcomes")
  ls_2d_no_tt_final <- bind_rows(ls_2d_no_tt_existing, ls_2d_no_tt_new)
} else {
  ls_2d_no_tt_final <- ls_2d_no_tt_new
}

dbWriteTable(decimal_con, SQL(glue::glue('"{my_schema}"."Labour_Supply_Distribution_LCP2_No_TT"')),
             ls_2d_no_tt_final, overwrite = TRUE)


# ******************************************************************************
# Part 7: Append StatCan census data
# ******************************************************************************
# WHY: The Labour_Supply_Distribution table also includes census data from
# Statistics Canada. We remove any existing census rows and re-append.
# KEPT AS SQL: INSERT INTO...SELECT (cross-schema append with column mapping)

# Remove existing StatCan rows from Labour_Supply_Distribution
ls_final <- ls_final %>%
  filter(SURVEY != "2021 Census PSSM 2023-2024")

# Append StatCan data
stat_can <- sch_tbl("Labour_Supply_Distribution_Stat_Can") %>%
  collect() |> rename_with(toupper) %>%
  mutate(SURVEY = "2021 Census PSSM 2023-2024") %>%
  select(SURVEY, PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD, LCIP4_CRED,
         CURRENT_REGION_PSSM_CODE_ROLLOUP, AGE_GROUP_ROLLUP,
         COUNT, TOTAL, NEW_LABOUR_SUPPLY)

ls_final <- bind_rows(ls_final, stat_can)

dbWriteTable(decimal_con, SQL(glue::glue('"{my_schema}"."Labour_Supply_Distribution"')),
             ls_final, overwrite = TRUE)


# ******************************************************************************
# Part 8: Write Weight_NLS back to T_Cohorts_Recoded and tmp_tbl_weights_nls
# ******************************************************************************
# WHY: The updated cohorts table (with Weight_NLS) needs to be written back
# so downstream scripts can use it. The weight lookup table is also kept.

# Write tmp_tbl_weights_nls for downstream reference
dbWriteTable(decimal_con, SQL(glue::glue('"{my_schema}"."tmp_tbl_Weights_NLS"')),
             tmp_tbl_weights_nls, overwrite = TRUE)


# ******************************************************************************
# Part 9: Cleanup and verification
# ******************************************************************************

# Verify tables exist
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Labour_Supply_Distribution"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Labour_Supply_Distribution_No_TT"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Labour_Supply_Distribution_LCP2"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Labour_Supply_Distribution_LCP2_No_TT"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."tmp_tbl_Weights_NLS"')))

# Final weight check
cohorts %>%
  count(SURVEY, SURVEY_YEAR, WEIGHT)

# ---- Disconnect ----
dbDisconnect(decimal_con)
