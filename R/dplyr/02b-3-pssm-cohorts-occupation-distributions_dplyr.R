# Occupation Distributions — dplyr Translation
# Original: R/02b-3-pssm-cohorts-occupation-distributions.R
#
# Pipeline context:
#   Processes cohort data to derive occupation distributions. Runs after
#   02b-2-pssm-cohorts-new-labour-supply (which computed Weight_NLS).
#
#   Creates occupation weights (Weight_OCC), then builds 6 distribution tables:
#     - Occupation_Distributions (4D, with TTRAIN, by region)
#     - Occupation_Distributions_No_TT (4D, no TTRAIN, by region)
#     - Occupation_Distributions_LCP2 (2D, with TTRAIN, by region)
#     - Occupation_Distributions_LCP2_No_TT (2D, no TTRAIN, by region)
#     - Occupation_Distributions_LCP2_BC (2D, with TTRAIN, BC-wide)
#     - Occupation_Distributions_LCP2_BC_No_TT (2D, no TTRAIN, BC-wide)
#   Plus PDEG Law modifications and StatCan census append.
#
# Input tables:
#   - T_Cohorts_Recoded — unified cohort table (from 02b-1)
#   - T_Current_Region_PSSM_Codes / T_Current_Region_PSSM_Rollup_Codes
#   - T_Current_Region_PSSM_Rollup_Codes_BC — BC-wide rollup
#   - tmp_tbl_Weights_NLS — NLS weights (from 02b-2)
#   - T_NOC_Broad_Categories — NOC validation lookup
#   - Occupation_Distributions_Stat_Can — census data
#
# Output tables:
#   - 6 Occupation_Distributions variants
#   - tmp_tbl_Weights_OCC — weight lookup for downstream use

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

grad_prefix <- function(status) {
  if_else(is.na(status), "", paste0(status, " - "))
}

# ---- Check required tables ----
required_tables <- c(
  "t_cohorts_recoded",
  "t_current_region_pssm_codes",
  "t_current_region_pssm_rollup_codes",
  "tmp_tbl_Weights_NLS",
  "T_NOC_Broad_Categories",
  "Occupation_Distributions_Stat_Can"
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
cohorts <- sch_tbl("T_Cohorts_Recoded") %>%
  collect() |> rename_with(toupper)

region_codes <- sch_tbl("T_Current_Region_PSSM_Codes") %>%
  collect() |> rename_with(toupper)

region_rollup <- sch_tbl("T_Current_Region_PSSM_Rollup_Codes") %>%
  collect() |> rename_with(toupper)

# BC-wide rollup (aggregates regions to province-level)
region_rollup_bc <- sch_tbl("T_Current_Region_PSSM_Rollup_Codes_BC") %>%
  collect() |> rename_with(toupper)

tmp_weights_nls <- sch_tbl("tmp_tbl_Weights_NLS") %>%
  collect() |> rename_with(toupper)

# Join cohorts with region rollup
cohorts_with_region <- cohorts %>%
  inner_join(region_codes %>% select(CURRENT_REGION_PSSM_CODE, CURRENT_REGION_PSSM_CODE_ROLLOUP),
             by = "CURRENT_REGION_PSSM_CODE") %>%
  inner_join(region_rollup %>% select(CURRENT_REGION_PSSM_CODE_ROLLOUP),
             by = "CURRENT_REGION_PSSM_CODE_ROLLOUP")


# ******************************************************************************
# Part 1: Weight_OCC creation (Z01 → Z08)
# ******************************************************************************
# WHY: The OCC weight adjusts for non-response in occupation reporting.
# It's computed similarly to Weight_NLS but focuses on respondents with valid
# NOC codes. The pipeline: base → respondents → weight → total → adj_fac → Weight_OCC.

# ---- Z01: Base_OCC records ----
# WHY: Records with NLS=1,2,3 and valid region, for building the OCC weight base.
base_occ <- cohorts %>%
  filter(as.numeric(WEIGHT) > 0,
         CURRENT_REGION_PSSM_CODE != -1,
         !is.na(AGE_GROUP_ROLLUP),
         as.numeric(NEW_LABOUR_SUPPLY) %in% c(1, 2, 3),
         GRAD_STATUS %in% c("1", "3")) %>%
  select(STQU_ID, SURVEY, INST_CD, AGE_GROUP_ROLLUP, TTRAIN, LCIP4_CRED,
         SURVEY_YEAR, GRAD_STATUS, NEW_LABOUR_SUPPLY, WEIGHT_NLS)

# ---- Z02a: Base with region rollup, grouped by year ----
# WHY: Counts records and computes weighted base by survey/year/region/age/cred.
z02a_base <- cohorts_with_region %>%
  filter(as.numeric(NEW_LABOUR_SUPPLY) %in% c(1, 2, 3),
         RESPONDENT == "1",
         as.numeric(WEIGHT) > 0,
         CURRENT_REGION_PSSM_CODE_ROLLOUP != 9999,
         !is.na(AGE_GROUP_ROLLUP),
         GRAD_STATUS %in% c("1", "3")) %>%
  mutate(WEIGHT_NLS = as.numeric(WEIGHT_NLS)) %>%
  group_by(SURVEY, CURRENT_REGION_PSSM_CODE_ROLLOUP, SURVEY_YEAR, INST_CD,
           AGE_GROUP_ROLLUP, GRAD_STATUS, TTRAIN, LCIP4_CRED, WEIGHT_NLS) %>%
  summarise(
    COUNT = n(),
    BASE = n() * WEIGHT_NLS,
    .groups = "drop"
  )

# ---- Z02b: Respondents with valid NOC (not 99999) ----
z02b_valid_noc <- cohorts_with_region %>%
  filter(!is.na(NOC_CD), NOC_CD != "99999",
         as.numeric(NEW_LABOUR_SUPPLY) %in% c(1, 3),
         as.numeric(WEIGHT) > 0,
         !is.na(AGE_GROUP_ROLLUP),
         GRAD_STATUS %in% c("1", "3")) %>%
  group_by(SURVEY, CURRENT_REGION_PSSM_CODE_ROLLOUP, SURVEY_YEAR, INST_CD,
           AGE_GROUP_ROLLUP, GRAD_STATUS, TTRAIN, LCIP4_CRED) %>%
  summarise(
    RESPONDENTS = sum(if_else(RESPONDENT == "1" & CURRENT_REGION_PSSM_CODE != -1, 1, 0)),
    .groups = "drop"
  )

# ---- Z02b: Respondents with NOC=99999 ----
z02b_noc_99999 <- cohorts_with_region %>%
  filter(NOC_CD == "99999",
         as.numeric(NEW_LABOUR_SUPPLY) %in% c(1, 3),
         as.numeric(WEIGHT) > 0,
         !is.na(AGE_GROUP_ROLLUP),
         GRAD_STATUS %in% c("1", "3")) %>%
  group_by(SURVEY, CURRENT_REGION_PSSM_CODE_ROLLOUP, SURVEY_YEAR, INST_CD,
           AGE_GROUP_ROLLUP, GRAD_STATUS, TTRAIN, LCIP4_CRED) %>%
  summarise(
    RESPONDENTS = sum(if_else(RESPONDENT == "1" & CURRENT_REGION_PSSM_CODE != -1, 1, 0)),
    .groups = "drop"
  )

# ---- Z02b: NOC 99999 where 100% of records have that NOC ----
# WHY: If ALL records in a cell have NOC=99999 (respondents/count = 1),
# those records should still get a weight (using the valid-NOC weight).
z02b_noc_99999_100 <- z02a_base %>%
  inner_join(z02b_noc_99999,
             by = c("SURVEY", "CURRENT_REGION_PSSM_CODE_ROLLOUP", "SURVEY_YEAR",
                    "INST_CD", "AGE_GROUP_ROLLUP", "GRAD_STATUS", "TTRAIN", "LCIP4_CRED")) %>%
  filter(RESPONDENTS / COUNT == 1)

# ---- Z02b: Union of respondents ----
z02b_union <- bind_rows(
  z02b_valid_noc,
  z02b_noc_99999_100 %>% select(SURVEY, CURRENT_REGION_PSSM_CODE_ROLLOUP, SURVEY_YEAR,
                                 INST_CD, AGE_GROUP_ROLLUP, GRAD_STATUS, TTRAIN, LCIP4_CRED, RESPONDENTS)
)

# ---- Z02c: Weight computation ----
# WHY: Weight_NLS_Base = Base/Respondents adjusts for non-response.
z02c_weight <- z02a_base %>%
  left_join(z02b_union,
            by = c("SURVEY", "CURRENT_REGION_PSSM_CODE_ROLLOUP", "SURVEY_YEAR",
                   "INST_CD", "AGE_GROUP_ROLLUP", "GRAD_STATUS", "TTRAIN", "LCIP4_CRED")) %>%
  mutate(
    RESPONDENTS = coalesce(RESPONDENTS, 0),
    WEIGHT_NLS_BASE = if_else(RESPONDENTS == 0, 1, as.numeric(COUNT) / as.numeric(RESPONDENTS)),
    WEIGHTED = as.numeric(RESPONDENTS) * if_else(RESPONDENTS == 0, 1, as.numeric(COUNT) / as.numeric(RESPONDENTS))
  )

# ---- Z03: Weight total ----
z03_total <- z02c_weight %>%
  group_by(SURVEY, CURRENT_REGION_PSSM_CODE_ROLLOUP, INST_CD, AGE_GROUP_ROLLUP,
           GRAD_STATUS, TTRAIN, LCIP4_CRED) %>%
  summarise(BASE = sum(COUNT), WEIGHTED = sum(WEIGHTED), .groups = "drop")

# ---- Z04: Weight adjustment factor ----
z04_adj_fac <- z03_total %>%
  mutate(WEIGHT_ADJ_FAC = if_else(WEIGHTED == 0, 0, as.numeric(BASE) / as.numeric(WEIGHTED)))

# ---- Z05: Final Weight_OCC ----
# WHY: Weight_OCC = Weight_NLS_Base * Weight_Adj_Fac. Joins with region codes
# to get the individual region code back from the rollup.
tmp_tbl_weights_occ <- z02c_weight %>%
  inner_join(z04_adj_fac %>% select(SURVEY, CURRENT_REGION_PSSM_CODE_ROLLOUP, INST_CD,
                                     AGE_GROUP_ROLLUP, GRAD_STATUS, TTRAIN, LCIP4_CRED, WEIGHT_ADJ_FAC),
             by = c("SURVEY", "CURRENT_REGION_PSSM_CODE_ROLLOUP", "INST_CD",
                    "AGE_GROUP_ROLLUP", "GRAD_STATUS", "TTRAIN", "LCIP4_CRED")) %>%
  mutate(WEIGHT_OCC = WEIGHT_NLS_BASE * WEIGHT_ADJ_FAC) %>%
  inner_join(
    region_rollup %>% select(CURRENT_REGION_PSSM_CODE_ROLLOUP) %>%
      inner_join(region_codes %>% select(CURRENT_REGION_PSSM_CODE, CURRENT_REGION_PSSM_CODE_ROLLOUP),
                 by = "CURRENT_REGION_PSSM_CODE_ROLLOUP"),
    by = "CURRENT_REGION_PSSM_CODE_ROLLOUP"
  )


# ******************************************************************************
# Part 2: Apply Weight_OCC to T_Cohorts_Recoded
# ******************************************************************************
# WHY: Update Weight_OCC on cohort records. Two passes: valid NOC and NOC=99999 100%.

# KEPT AS SQL: ALTER TABLE for Weight_OCC column
dbExecute(decimal_con, "ALTER TABLE T_Cohorts_Recoded ALTER COLUMN Weight_OCC FLOAT NULL")
dbExecute(decimal_con, "ALTER TABLE T_Cohorts_Recoded ALTER COLUMN Weight_Age FLOAT NULL")

# Null out Weight_OCC in R memory
cohorts <- cohorts %>%
  mutate(WEIGHT_OCC = NA_real_)

# Update for records with valid NOC (not 99999, not null) and valid region
weight_occ_lookup <- tmp_tbl_weights_occ %>%
  select(SURVEY, SURVEY_YEAR, INST_CD, AGE_GROUP_ROLLUP, GRAD_STATUS,
         TTRAIN, LCIP4_CRED, CURRENT_REGION_PSSM_CODE, WEIGHT_OCC)

base_occ_ids <- base_occ %>% distinct(STQU_ID)

cohorts_valid_noc <- cohorts %>%
  semi_join(base_occ_ids, by = "STQU_ID") %>%
  filter(CURRENT_REGION_PSSM_CODE != -1,
         !is.na(NOC_CD), NOC_CD != "99999") %>%
  inner_join(weight_occ_lookup,
             by = c("SURVEY", "SURVEY_YEAR", "INST_CD", "AGE_GROUP_ROLLUP",
                    "GRAD_STATUS", "TTRAIN", "LCIP4_CRED", "CURRENT_REGION_PSSM_CODE")) %>%
  select(STQU_ID, WEIGHT_OCC)

# Update for NOC=99999 records that are in the 100% NOC-99999 cells
noc_99999_ids <- z02b_noc_99999_100 %>%
  select(SURVEY, CURRENT_REGION_PSSM_CODE_ROLLOUP, SURVEY_YEAR, INST_CD,
         AGE_GROUP_ROLLUP, GRAD_STATUS, TTRAIN, LCIP4_CRED) %>%
  inner_join(tmp_tbl_weights_occ %>% select(SURVEY, CURRENT_REGION_PSSM_CODE_ROLLOUP,
                                             SURVEY_YEAR, INST_CD, AGE_GROUP_ROLLUP,
                                             GRAD_STATUS, TTRAIN, LCIP4_CRED,
                                             CURRENT_REGION_PSSM_CODE, WEIGHT_OCC),
             by = c("SURVEY", "CURRENT_REGION_PSSM_CODE_ROLLOUP", "SURVEY_YEAR",
                    "INST_CD", "AGE_GROUP_ROLLUP", "GRAD_STATUS", "TTRAIN", "LCIP4_CRED"))

cohorts_noc_99999 <- cohorts %>%
  semi_join(base_occ_ids, by = "STQU_ID") %>%
  filter(CURRENT_REGION_PSSM_CODE != -1, !is.na(NOC_CD)) %>%
  inner_join(noc_99999_ids %>% select(CURRENT_REGION_PSSM_CODE, SURVEY_YEAR,
                                       INST_CD, AGE_GROUP_ROLLUP, GRAD_STATUS,
                                       TTRAIN, LCIP4_CRED, WEIGHT_OCC),
             by = c("CURRENT_REGION_PSSM_CODE", "SURVEY_YEAR", "INST_CD",
                    "AGE_GROUP_ROLLUP", "GRAD_STATUS", "TTRAIN", "LCIP4_CRED")) %>%
  select(STQU_ID, WEIGHT_OCC)

# Apply both updates
all_weight_occ_updates <- bind_rows(cohorts_valid_noc, cohorts_noc_99999)

cohorts <- cohorts %>%
  left_join(all_weight_occ_updates %>% rename(WEIGHT_OCC_NEW = WEIGHT_OCC),
            by = "STQU_ID") %>%
  mutate(WEIGHT_OCC = if_else(!is.na(WEIGHT_OCC_NEW), WEIGHT_OCC_NEW, WEIGHT_OCC)) %>%
  select(-WEIGHT_OCC_NEW)

# Write Weight_OCC lookup table for downstream
dbWriteTable(decimal_con, SQL(glue::glue('"{my_schema}"."tmp_tbl_Weights_OCC"')),
             tmp_tbl_weights_occ, overwrite = TRUE)


# ******************************************************************************
# Part 3: Weighted occupation counts (Q009)
# ******************************************************************************
# WHY: Count respondents by occupation code, weighted by Weight_OCC. The NOC code
# XXXXX is recoded to 99999. Multiple variants are needed for the 6 output tables.

# Base weighted occupation counts
q009_weight_occs <- cohorts_with_region %>%
  filter(as.numeric(NEW_LABOUR_SUPPLY) %in% c(1, 3),
         as.numeric(WEIGHT) > 0) %>%
  mutate(
    NOC_CD = if_else(NOC_CD == "XXXXX", "99999", NOC_CD),
    WEIGHT_OCC = as.numeric(WEIGHT_OCC)
  ) %>%
  filter(CURRENT_REGION_PSSM_CODE_ROLLOUP != 9999,
         !is.na(AGE_GROUP_ROLLUP),
         GRAD_STATUS %in% c("1", "3"),
         !is.na(NOC_CD),
         !is.na(WEIGHT_OCC)) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
           SURVEY_YEAR, INST_CD, AGE_GROUP_ROLLUP, GRAD_STATUS, LCP4_CD,
           TTRAIN, LCIP4_CRED, LCIP2_CRED, NOC_CD, WEIGHT_OCC) %>%
  summarise(COUNT = n(), .groups = "drop") %>%
  mutate(WEIGHTED = COUNT * WEIGHT_OCC)


# ******************************************************************************
# Part 4: Build distribution variants (Q009b totals + Q010 percentages)
# ******************************************************************************
# WHY: Same pattern as Labour_Supply_Distribution: compute weighted counts and
# totals for different grouping dimensions, then compute percentages.

# Helper: compute percentage = count / total
compute_dist <- function(counts, totals, join_cols) {
  totals %>%
    left_join(counts %>% select(all_of(c(join_cols, "NOC_CD", "COUNT"))), by = join_cols) %>%
    mutate(PERC_DIST = if_else(is.na(COUNT), 0, COUNT) / TOTAL)
}

# ---- Variant 1: 4D with TTRAIN (by region) ----
q009b_counts_4d <- q009_weight_occs %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
           AGE_GROUP_ROLLUP, LCP4_CD, TTRAIN, LCIP4_CRED, LCIP2_CRED, NOC_CD) %>%
  summarise(COUNT = sum(WEIGHTED), .groups = "drop")

q009b_totals_4d <- q009_weight_occs %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
           AGE_GROUP_ROLLUP, LCP4_CD, TTRAIN, LCIP4_CRED, LCIP2_CRED) %>%
  summarise(TOTAL = sum(WEIGHTED), .groups = "drop")

q010_4d <- compute_dist(q009b_counts_4d, q009b_totals_4d,
                         c("PSSM_CREDENTIAL", "PSSM_CRED", "CURRENT_REGION_PSSM_CODE_ROLLOUP",
                           "AGE_GROUP_ROLLUP", "LCP4_CD", "TTRAIN", "LCIP4_CRED", "LCIP2_CRED"))

# ---- Variant 2: 4D no TTRAIN (by region) ----
q009b_counts_4d_no_tt <- q009_weight_occs %>%
  mutate(
    LCIP4_CRED_NT = paste0(grad_prefix(GRAD_STATUS), LCP4_CD, " - ", PSSM_CREDENTIAL),
    LCIP2_CRED_NT = paste0(grad_prefix(GRAD_STATUS), str_sub(LCP4_CD, 1, 2), " - ", PSSM_CREDENTIAL)
  ) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
           AGE_GROUP_ROLLUP, LCP4_CD, LCIP4_CRED_NT, LCIP2_CRED_NT, NOC_CD) %>%
  summarise(COUNT = sum(WEIGHTED), .groups = "drop") %>%
  rename(LCIP4_CRED = LCIP4_CRED_NT, LCIP2_CRED = LCIP2_CRED_NT)

q009b_totals_4d_no_tt <- q009_weight_occs %>%
  mutate(
    LCIP4_CRED_NT = paste0(grad_prefix(GRAD_STATUS), LCP4_CD, " - ", PSSM_CREDENTIAL),
    LCIP2_CRED_NT = paste0(grad_prefix(GRAD_STATUS), str_sub(LCP4_CD, 1, 2), " - ", PSSM_CREDENTIAL)
  ) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
           AGE_GROUP_ROLLUP, LCP4_CD, LCIP4_CRED_NT, LCIP2_CRED_NT) %>%
  summarise(TOTAL = sum(WEIGHTED), .groups = "drop") %>%
  rename(LCIP4_CRED = LCIP4_CRED_NT, LCIP2_CRED = LCIP2_CRED_NT)

q010_4d_no_tt <- compute_dist(q009b_counts_4d_no_tt, q009b_totals_4d_no_tt,
                               c("PSSM_CREDENTIAL", "PSSM_CRED", "CURRENT_REGION_PSSM_CODE_ROLLOUP",
                                 "AGE_GROUP_ROLLUP", "LCP4_CD", "LCIP4_CRED", "LCIP2_CRED"))

# ---- Variant 3: 2D with TTRAIN (by region) ----
q009b_counts_2d <- q009_weight_occs %>%
  mutate(LCP2_CD = str_sub(LCP4_CD, 1, 2)) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
           AGE_GROUP_ROLLUP, LCP2_CD, TTRAIN, LCIP2_CRED, NOC_CD) %>%
  summarise(COUNT = sum(WEIGHTED), .groups = "drop")

q009b_totals_2d <- q009_weight_occs %>%
  mutate(LCP2_CD = str_sub(LCP4_CD, 1, 2)) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
           AGE_GROUP_ROLLUP, LCP2_CD, TTRAIN, LCIP2_CRED) %>%
  summarise(TOTAL = sum(WEIGHTED), .groups = "drop")

q010_2d <- compute_dist(q009b_counts_2d, q009b_totals_2d,
                         c("PSSM_CREDENTIAL", "PSSM_CRED", "CURRENT_REGION_PSSM_CODE_ROLLOUP",
                           "AGE_GROUP_ROLLUP", "LCP2_CD", "TTRAIN", "LCIP2_CRED"))

# ---- Variant 4: 2D no TTRAIN (by region) ----
q009b_counts_2d_no_tt <- q009_weight_occs %>%
  mutate(
    LCP2_CD = str_sub(LCP4_CD, 1, 2),
    LCP2_CRED_NT = paste0(
      if_else(str_sub(PSSM_CRED, 1, 1) %in% c("1", "3"),
              paste0(str_sub(PSSM_CRED, 1, 1), " - "), ""),
      str_sub(LCP4_CD, 1, 2), " - ", PSSM_CREDENTIAL
    )
  ) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
           AGE_GROUP_ROLLUP, LCP2_CD, LCP2_CRED_NT, NOC_CD) %>%
  summarise(COUNT = sum(WEIGHTED), .groups = "drop") %>%
  rename(LCIP2_CRED = LCP2_CRED_NT)

q009b_totals_2d_no_tt <- q009_weight_occs %>%
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
  summarise(TOTAL = sum(WEIGHTED), .groups = "drop") %>%
  rename(LCIP2_CRED = LCP2_CRED_NT)

q010_2d_no_tt <- compute_dist(q009b_counts_2d_no_tt, q009b_totals_2d_no_tt,
                               c("PSSM_CREDENTIAL", "PSSM_CRED", "CURRENT_REGION_PSSM_CODE_ROLLOUP",
                                 "AGE_GROUP_ROLLUP", "LCP2_CD", "LCIP2_CRED"))

# ---- Variant 5: 2D with TTRAIN (BC-wide) ----
# WHY: BC-wide variants aggregate across all regions within BC.
q009b_counts_2d_bc <- q009_weight_occs %>%
  inner_join(region_rollup_bc %>% select(CURRENT_REGION_PSSM_CODE_ROLLOUP),
             by = "CURRENT_REGION_PSSM_CODE_ROLLOUP") %>%
  mutate(LCP2_CD = str_sub(LCP4_CD, 1, 2)) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, LCP2_CD, TTRAIN, LCIP2_CRED, NOC_CD) %>%
  summarise(COUNT = sum(WEIGHTED), .groups = "drop")

q009b_totals_2d_bc <- q009_weight_occs %>%
  inner_join(region_rollup_bc %>% select(CURRENT_REGION_PSSM_CODE_ROLLOUP),
             by = "CURRENT_REGION_PSSM_CODE_ROLLOUP") %>%
  mutate(LCP2_CD = str_sub(LCP4_CD, 1, 2)) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, LCP2_CD, TTRAIN, LCIP2_CRED) %>%
  summarise(TOTAL = sum(WEIGHTED), .groups = "drop")

q010_2d_bc <- compute_dist(q009b_counts_2d_bc, q009b_totals_2d_bc,
                            c("PSSM_CREDENTIAL", "PSSM_CRED", "LCP2_CD", "TTRAIN", "LCIP2_CRED"))

# ---- Variant 6: 2D no TTRAIN (BC-wide) ----
q009b_counts_2d_bc_no_tt <- q009_weight_occs %>%
  inner_join(region_rollup_bc %>% select(CURRENT_REGION_PSSM_CODE_ROLLOUP),
             by = "CURRENT_REGION_PSSM_CODE_ROLLOUP") %>%
  mutate(
    LCP2_CD = str_sub(LCP4_CD, 1, 2),
    LCP2_CRED_NT = paste0(
      if_else(str_sub(PSSM_CRED, 1, 1) %in% c("1", "3"),
              paste0(str_sub(PSSM_CRED, 1, 1), " - "), ""),
      str_sub(LCP4_CD, 1, 2), " - ", PSSM_CREDENTIAL
    )
  ) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, LCP2_CD, LCP2_CRED_NT, NOC_CD) %>%
  summarise(COUNT = sum(WEIGHTED), .groups = "drop") %>%
  rename(LCIP2_CRED = LCP2_CRED_NT)

q009b_totals_2d_bc_no_tt <- q009_weight_occs %>%
  inner_join(region_rollup_bc %>% select(CURRENT_REGION_PSSM_CODE_ROLLOUP),
             by = "CURRENT_REGION_PSSM_CODE_ROLLOUP") %>%
  mutate(
    LCP2_CD = str_sub(LCP4_CD, 1, 2),
    LCP2_CRED_NT = paste0(
      if_else(str_sub(PSSM_CRED, 1, 1) %in% c("1", "3"),
              paste0(str_sub(PSSM_CRED, 1, 1), " - "), ""),
      str_sub(LCP4_CD, 1, 2), " - ", PSSM_CREDENTIAL
    )
  ) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, LCP2_CD, LCP2_CRED_NT) %>%
  summarise(TOTAL = sum(WEIGHTED), .groups = "drop") %>%
  rename(LCIP2_CRED = LCP2_CRED_NT)

q010_2d_bc_no_tt <- compute_dist(q009b_counts_2d_bc_no_tt, q009b_totals_2d_bc_no_tt,
                                  c("PSSM_CREDENTIAL", "PSSM_CRED", "LCP2_CD", "LCIP2_CRED"))


# ******************************************************************************
# Part 5: Write occupation distribution tables
# ******************************************************************************
# WHY: Each of the 6 output tables gets "Student Outcomes" rows replaced.
# Pattern: filter existing to keep non-Student-Outcomes, bind new rows, overwrite.

write_occ_table <- function(table_name, new_data, survey_col = "SURVEY") {
  full_name <- SQL(glue::glue('"{my_schema}"."{table_name}"'))
  new_rows <- new_data %>% mutate(!!survey_col := "Student Outcomes")

  if (dbExistsTable(decimal_con, full_name)) {
    existing <- sch_tbl(table_name) %>%
      collect() |> rename_with(toupper) %>%
      filter(SURVEY != "Student Outcomes")
    final <- bind_rows(existing, new_rows)
  } else {
    final <- new_rows
  }
  dbWriteTable(decimal_con, full_name, final, overwrite = TRUE)
}

# 4D with TTRAIN
write_occ_table("Occupation_Distributions",
  q010_4d %>% select(PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
                     AGE_GROUP_ROLLUP, LCP4_CD, TTRAIN, LCIP4_CRED, LCIP2_CRED,
                     NOC = NOC_CD, COUNT, TOTAL, PERCENT = PERC_DIST)
)

# 4D no TTRAIN
write_occ_table("Occupation_Distributions_No_TT",
  q010_4d_no_tt %>% select(PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
                            AGE_GROUP_ROLLUP, LCP4_CD, LCIP4_CRED, LCIP2_CRED,
                            NOC = NOC_CD, COUNT, TOTAL, PERCENT = PERC_DIST)
)

# 2D with TTRAIN
write_occ_table("Occupation_Distributions_LCP2",
  q010_2d %>% select(PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
                     AGE_GROUP_ROLLUP, LCP2_CD, TTRAIN, LCIP2_CRED,
                     NOC = NOC_CD, COUNT, TOTAL, PERCENT = PERC_DIST)
)

# 2D no TTRAIN
write_occ_table("Occupation_Distributions_LCP2_No_TT",
  q010_2d_no_tt %>% select(PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
                            AGE_GROUP_ROLLUP, LCP2_CD, LCIP2_CRED,
                            NOC = NOC_CD, COUNT, TOTAL, PERCENT = PERC_DIST)
)

# 2D with TTRAIN BC-wide
write_occ_table("Occupation_Distributions_LCP2_BC",
  q010_2d_bc %>% select(PSSM_CREDENTIAL, PSSM_CRED, LCP2_CD, TTRAIN, LCIP2_CRED,
                        NOC = NOC_CD, COUNT, TOTAL, PERCENT = PERC_DIST)
)

# 2D no TTRAIN BC-wide
write_occ_table("Occupation_Distributions_LCP2_BC_No_TT",
  q010_2d_bc_no_tt %>% select(PSSM_CREDENTIAL, PSSM_CRED, LCP2_CD, LCIP2_CRED,
                               NOC = NOC_CD, COUNT, TOTAL, PERCENT = PERC_DIST)
)


# ******************************************************************************
# Part 6: PDEG Law modifications (Q010d → Q010e)
# ******************************************************************************
# WHY: PDEG (post-degree) credentials in CIP cluster 07 (Law) need special handling.
# The occupation distribution for PDEG Law is derived from BACH Law (CIP 22)
# graduates, since law graduates typically have a bachelor's degree followed by
# a law degree. This replaces the default PDEG Law distribution.

# ---- NLS: PDEG 07 from BACH 22 ----
# Remove existing PDEG 07 rows from Labour_Supply_Distribution
ls_dist <- sch_tbl("Labour_Supply_Distribution") %>%
  collect() |> rename_with(toupper)

ls_dist <- ls_dist %>%
  filter(!(SURVEY == "2021 Census PSSM 2023-2024" & PSSM_CREDENTIAL == "PDEG" & LCP4_CD == "07"))

# Compute PDEG 07 NLS from BACH 22 data
pdeg_07_count <- ls_dist %>%
  filter(SURVEY == "Student Outcomes", PSSM_CREDENTIAL == "BACH",
         str_sub(LCP4_CD, 1, 2) == "22") %>%
  group_by(SURVEY, TTRAIN, CURRENT_REGION_PSSM_CODE_ROLLOUP, AGE_GROUP_ROLLUP) %>%
  summarise(COUNT = sum(COUNT), .groups = "drop") %>%
  mutate(PSSM_CREDENTIAL = "PDEG", PSSM_CRED = "PDEG", LCP4_CD = "07",
         LCIP4_CRED = "07 - PDEG")

pdeg_07_total <- ls_dist %>%
  filter(SURVEY == "Student Outcomes", PSSM_CREDENTIAL == "BACH",
         str_sub(LCP4_CD, 1, 2) == "22") %>%
  group_by(SURVEY, TTRAIN, AGE_GROUP_ROLLUP, TOTAL) %>%
  summarise(.groups = "drop") %>%
  mutate(PSSM_CREDENTIAL = "PDEG", PSSM_CRED = "PDEG", LCP4_CD = "07",
         LCIP4_CRED = "07 - PDEG") %>%
  group_by(SURVEY, PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD, TTRAIN, LCIP4_CRED, AGE_GROUP_ROLLUP) %>%
  summarise(TOTAL = sum(TOTAL), .groups = "drop")

pdeg_07_nls <- pdeg_07_total %>%
  left_join(pdeg_07_count %>% select(SURVEY, TTRAIN, CURRENT_REGION_PSSM_CODE_ROLLOUP,
                                      AGE_GROUP_ROLLUP, COUNT),
            by = c("SURVEY", "TTRAIN", "AGE_GROUP_ROLLUP")) %>%
  filter(!is.na(CURRENT_REGION_PSSM_CODE_ROLLOUP)) %>%
  mutate(NEW_LABOUR_SUPPLY = if_else(is.na(COUNT), 0, COUNT) / TOTAL)

ls_dist <- bind_rows(ls_dist, pdeg_07_nls)
dbWriteTable(decimal_con, SQL(glue::glue('"{my_schema}"."Labour_Supply_Distribution"')),
             ls_dist, overwrite = TRUE)


# ---- Occupation: PDEG 07 from BACH 22 ----
occ_dist <- sch_tbl("Occupation_Distributions") %>%
  collect() |> rename_with(toupper)

# Remove old PDEG 07 rows
occ_dist <- occ_dist %>%
  filter(!(SURVEY == "2021 Census PSSM 2022-2023" & PSSM_CREDENTIAL == "PDEG" & LCP4_CD == "07"))

# Compute PDEG 07 occupation distribution from BACH 22
pdeg_07_occ_count <- occ_dist %>%
  filter(SURVEY == "Student Outcomes", PSSM_CREDENTIAL == "BACH",
         str_sub(LCP4_CD, 1, 2) == "22") %>%
  group_by(SURVEY, TTRAIN, CURRENT_REGION_PSSM_CODE_ROLLOUP, AGE_GROUP_ROLLUP, NOC) %>%
  summarise(COUNT = sum(COUNT), .groups = "drop") %>%
  mutate(PSSM_CREDENTIAL = "PDEG", PSSM_CRED = "PDEG", LCP4_CD = "07",
         LCIP4_CRED = "07 - PDEG")

pdeg_07_occ_total <- pdeg_07_occ_count %>%
  group_by(SURVEY, PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
           AGE_GROUP_ROLLUP, LCP4_CD, TTRAIN, LCIP4_CRED) %>%
  summarise(TOTAL = sum(COUNT), .groups = "drop")

pdeg_07_occ_dist <- pdeg_07_occ_total %>%
  left_join(pdeg_07_occ_count %>% select(SURVEY, CURRENT_REGION_PSSM_CODE_ROLLOUP,
                                          AGE_GROUP_ROLLUP, LCIP4_CRED, NOC, COUNT),
            by = c("SURVEY", "CURRENT_REGION_PSSM_CODE_ROLLOUP", "AGE_GROUP_ROLLUP", "LCIP4_CRED")) %>%
  mutate(PERCENT = COUNT / TOTAL)

occ_dist <- bind_rows(occ_dist, pdeg_07_occ_dist)
dbWriteTable(decimal_con, SQL(glue::glue('"{my_schema}"."Occupation_Distributions"')),
             occ_dist, overwrite = TRUE)


# ******************************************************************************
# Part 7: End date imputation + DACSO ENDDT fix
# ******************************************************************************
# WHY: DACSO records with null ENDDT get a default end date of Dec of (survey_year - 2).
cohorts <- cohorts %>%
  mutate(ENDDT = if_else(is.na(ENDDT) & SURVEY == "DACSO",
                          paste0(as.numeric(SURVEY_YEAR) - 2, "-12"), ENDDT))

# Suppression table for public release (only for regular_run)
if (exists("regular_run") && regular_run == TRUE) {
  tryCatch({
    age_groups_rollup <- sch_tbl("tbl_Age_Groups_Rollup") %>%
      collect() |> rename_with(toupper)

    suppression_noc <- cohorts %>%
      inner_join(age_groups_rollup %>% select(AGE_GROUP_ROLLUP, AGE_GROUP_ROLLUP_LABEL),
                 by = "AGE_GROUP_ROLLUP") %>%
      filter(as.numeric(WEIGHT) > 0, !is.na(AGE_GROUP_ROLLUP)) %>%
      count(AGE_GROUP_ROLLUP, AGE_GROUP_ROLLUP_LABEL, NOC_CD, name = "Expr1") %>%
      filter(Expr1 < 5) %>%
      arrange(Expr1)

    dbWriteTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_Suppression_Public_Release_NOC"')),
                 suppression_noc, overwrite = TRUE)
  }, error = function(e) {
    print(paste("Error:", e$message))
    stop()
  })
}


# ******************************************************************************
# Part 8: Append StatCan census data
# ******************************************************************************
# WHY: Occupation_Distributions also includes census data from Statistics Canada.
occ_dist_final <- occ_dist %>%
  filter(SURVEY != "2021 Census PSSM 2023-2024")

stat_can_occ <- sch_tbl("Occupation_Distributions_Stat_Can") %>%
  collect() |> rename_with(toupper) %>%
  mutate(SURVEY = "2021 Census PSSM 2023-2024")

occ_dist_final <- bind_rows(occ_dist_final, stat_can_occ)

dbWriteTable(decimal_con, SQL(glue::glue('"{my_schema}"."Occupation_Distributions"')),
             occ_dist_final, overwrite = TRUE)


# ******************************************************************************
# Part 9: Cleanup and verification
# ******************************************************************************/
# Drop lookup tables no longer needed
# KEPT AS SQL: DROP TABLE (cleanup of tables loaded by earlier scripts)
for (tbl_name in c("tmp_tbl_Weights_OCC", "tmp_tbl_Weights_NLS", "tbl_Age_Groups",
                    "tbl_Age_Groups_Rollup", "tbl_Age", "T_PSSM_Credential_Grouping",
                    "T_Weights", "t_year_survey_year", "t_current_region_pssm_codes",
                    "t_current_region_pssm_rollup_codes", "t_current_region_pssm_rollup_codes_bc")) {
  full_name <- SQL(glue::glue('"{my_schema}"."{tbl_name}"'))
  if (dbExistsTable(decimal_con, full_name)) {
    dbExecute(decimal_con, glue::glue("DROP TABLE {tbl_name}"))
  }
}

# Verify tables exist
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Occupation_Distributions"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Occupation_Distributions_No_TT"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Occupation_Distributions_LCP2"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Occupation_Distributions_LCP2_No_TT"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Occupation_Distributions_LCP2_BC"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Occupation_Distributions_LCP2_BC_No_TT"')))

# ---- Disconnect ----
dbDisconnect(decimal_con)
