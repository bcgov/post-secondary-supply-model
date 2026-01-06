# This script processes cohort data from student outcomes and creates new labour supply distributions.
# Outcomes data has been standardized so all cohorts/surveys are combined in a single dataset.
#
# PSSM Model Context:
# This script calculates weights and distributions for the New Labour Supply (NLS) component of the model.
# NLS represents the number of graduates entering the labour market, weighted by response rates and
# adjusted for various demographic and geographic factors.
#
# Required Source Tables
#   T_Cohorts_Recoded - Unified cohort table from 02b-1-pssm-cohorts.R
#
# Required Lookup Tables
#   t_current_region_pssm_codes, t_current_region_pssm_rollup_codes
#   tbl_noc_skill_level_aged_17_34
#
# Resulting Tables
#   Labour_Supply_Distribution - Main NLS distribution (4-digit CIP, with ttrain)
#   Labour_Supply_Distribution_No_TT - NLS distribution without ttrain dimension
#   Labour_Supply_Distribution_LCP2 - NLS distribution (2-digit CIP, with ttrain)
#   Labour_Supply_Distribution_LCP2_No_TT - NLS distribution (2-digit CIP, without ttrain)
#
# WHAT: Calculates weighted new labour supply distributions for all cohort records.
# WHY: The PSSM model requires calibrated weights that account for survey response rates,
#      year effects, and demographic factors to produce accurate supply projections.
# HOW: 1) Validate and fix invalid NOC codes (e.g., 403X -> 4031)
#      2) Identify NLS-1 (employed) and NLS-2 (part-time) records
#      3) Recode NLS-2 to NLS-3 if no matching NLS-1 exists for the same cohort
#      4) Calculate probability weights (Count / Respondents)
#      5) Apply year weights and adjustment factors
#      6) Create Weight_NLS for each cohort record
#      7) Aggregate into distribution tables by various dimensions
#
# Key Concepts:
#   - NLS-0: Not in labour force
#   - NLS-1: Employed full-time
#   - NLS-2: Employed part-time
#   - NLS-3: Part-time without matching NLS-1 (treated as not in labour force)
#
# Weight Formula:
#   Weight_Prob = Count / Respondents
#   Weighted = Respondents * Weight_Prob * Weight_Year
#   Weight_NLS = weight * Weight_Adj_Fac
#
# TODO [HIGH]: Add validation for NOC skill level consistency
# TODO [MEDIUM]: Extract weight calculation logic into reusable function
# TODO [LOW]: Add parallel processing for distribution aggregations

library(tidyverse)
library(arrow)
library(odbc)
library(DBI)
library(dbplyr)
library(config)
library(glue)

# ---- Configure LAN and DB Connection ----
db_config <- config::get("decimal")
my_schema <- config::get("myschema")

con <- dbConnect(
  odbc(),
  Driver = db_config$driver,
  Server = db_config$server,
  Database = db_config$database,
  Trusted_Connection = "True"
)

# ---- Read Lookup Tables ----
t_current_region_pssm_codes <- tbl(
  con,
  in_schema(my_schema, "t_current_region_pssm_codes")
)
t_current_region_pssm_rollup_codes <- tbl(
  con,
  in_schema(my_schema, "t_current_region_pssm_rollup_codes")
)
tbl_noc_skill_level_aged_17_34 <- tbl(
  con,
  in_schema(my_schema, "tbl_noc_skill_level_aged_17_34")
)

# =============================================================================
# NOC Code Validation and Recoding
# =============================================================================
#
# WHAT: Validates and corrects invalid NOC codes before weight calculations.
# WHY: Invalid NOC codes can cause downstream aggregation errors and should be standardized.
# HOW: 1) Check weight distribution by survey/year for data quality
#      2) Apply specific NOC code corrections (e.g., 403X -> 4031)
#
# Known Corrections:
#   - 403X -> 4031 (child development specialist standardization)

## ---- Check Weights ----
# Refactored from: DACSO_Q005_DACSO_DATA_Part_1b3_Check_Weights
cohorts_base <- tbl(con, in_schema(my_schema, "T_Cohorts_Recoded"))

weight_check <- cohorts_base %>%
  group_by(survey, survey_year, weight) %>%
  summarize(count = n(), .groups = "drop") %>%
  collect()

message("Weight check results:")
print(weight_check)

## ---- NOC Code Recoding ----
# Refactored from: DACSO_Q99A_STQUI_ID, DACSO_Q005_DACSO_DATA_Part_1b4_Check_NOC_Valid
# Note: Setting all 403X to 4031 for now (per original comment)

cohorts_with_noc_fix <- cohorts_base %>%
  mutate(
    noc_cd = if_else(noc_cd == "403X", "4031", noc_cd)
  ) %>%
  compute(name = "T_Cohorts_Recoded_NOC_Fixed", temporary = FALSE)

# =============================================================================
# NLS Recoding (NLS-1 and NLS-2)
# =============================================================================
#
# WHAT: Separates and validates New Labour Supply categories for weight calculation.
# WHY: NLS categories determine how graduates are counted in labour market supply.
#      NLS-2 records without matching NLS-1 need special handling.
# HOW: 1) NLS1: Filter records with new_labour_supply = 1 and positive weight
#      2) NLS2: Collect records with new_labour_supply = 2
#      3) Recode: Anti-join NLS2 to NLS1, recode unmatched NLS2 to NLS3
#
# Key Filters:
#   - as.numeric(weight) > 0 (positive response weight)
#   - !is.na(noc_cd) (valid occupation code)
#   - grad_status %in% c("1", "3") (graduates only)
#   - new_labour_supply %in% c(1, 2) (employed categories)

## ---- NLS1: Base counts for new labour supply = 1 ----
# Refactored from: DACSO_Q005_DACSO_DATA_Part_1c_NLS1
nls1_base <- cohorts_with_noc_fix %>%
  inner_join(
    t_current_region_pssm_codes %>%
      inner_join(
        t_current_region_pssm_rollup_codes,
        by = "current_region_pssm_code_rollup"
      ),
    by = c("current_region_pssm_code" = "current_region_pssm_code")
  ) %>%
  filter(
    as.numeric(weight) > 0,
    !is.na(noc_cd),
    !is.na(age_group_rollup),
    grad_status %in% c("1", "3"),
    new_labour_supply == 1
  ) %>%
  group_by(
    survey,
    current_region_pssm_code_rollup,
    age_group_rollup,
    inst_cd,
    lcip4_cred,
    grad_status,
    new_labour_supply
  ) %>%
  summarize(Base = n(), .groups = "drop") %>%
  compute(name = "DACSO_Q005_DACSO_DATA_Part_1c_NLS1", temporary = FALSE)

## ---- NLS2: Records with new labour supply = 2 ----
# Refactored from: DACSO_Q005_DACSO_DATA_Part_1c_NLS2
nls2_base <- cohorts_with_noc_fix %>%
  inner_join(
    t_current_region_pssm_codes %>%
      inner_join(
        t_current_region_pssm_rollup_codes,
        by = "current_region_pssm_code_rollup"
      ),
    by = c("current_region_pssm_code" = "current_region_pssm_code")
  ) %>%
  filter(
    as.numeric(weight) > 0,
    !is.na(age_group_rollup),
    grad_status %in% c("1", "3"),
    new_labour_supply == 2
  ) %>%
  select(
    survey,
    current_region_pssm_code_rollup,
    age_group_rollup,
    inst_cd,
    lcip4_cred,
    grad_status,
    new_labour_supply,
    stqu_id
  ) %>%
  compute(name = "DACSO_Q005_DACSO_DATA_Part_1c_NLS2", temporary = FALSE)

## ---- Recode NLS-2 to NLS-3 where no matching NLS-1 ----
# Refactored from: DACSO_Q005_DACSO_DATA_Part_1c_NLS2_Recode
# If an NLS-2 record has no corresponding NLS-1 record with same characteristics,
# recode it to NLS-3

# Get list of NLS-2 records without matching NLS-1
nls2_without_match <- nls2_base %>%
  anti_join(
    nls1_base %>%
      select(
        survey,
        current_region_pssm_code_rollup,
        age_group_rollup,
        inst_cd,
        lcip4_cred
      ) %>%
      distinct(),
    by = c(
      "survey",
      "current_region_pssm_code_rollup",
      "age_group_rollup",
      "inst_cd",
      "lcip4_cred"
    )
  ) %>%
  select(stqu_id) %>%
  collect()

# Update cohorts if any NLS-2 records need recoding
if (nrow(nls2_without_match) > 0) {
  stqu_ids_to_update <- nls2_without_match$stqu_id
  update_query <- glue::glue(
    "UPDATE {my_schema}.T_Cohorts_Recoded
     SET new_labour_supply = 3
     WHERE stqu_id IN ({paste(sQuote(stqu_ids_to_update), collapse = ',')})"
  )
  dbExecute(con, update_query)
  message(glue::glue(
    "Recoded {length(stqu_ids_to_update)} NLS-2 records to NLS-3"
  ))
}

# Clean up intermediate NLS tables
dbRemoveTable(
  con,
  Id(schema = my_schema, table = "DACSO_Q005_DACSO_DATA_Part_1c_NLS1")
)
dbRemoveTable(
  con,
  Id(schema = my_schema, table = "DACSO_Q005_DACSO_DATA_Part_1c_NLS2")
)

# =============================================================================
# Weight Calculations (Z01 - Z09)
# =============================================================================
#
# WHAT: Calculates probability weights and adjustment factors for labour supply weighting.
# WHY: Raw counts need to be adjusted for survey response rates and year effects to produce
#      accurate supply estimates that can be generalized to the population.
# HOW: 1) Z01: Create base NLS table filtered for valid records
#      2) Z02: Calculate Weight_Prob = Count / Respondents
#      3) Z03: Aggregate to get weighted totals by dimensions
#      4) Z04: Calculate adjustment factor = Base / Weighted
#      5) Z05: Calculate Weight_NLS = weight * Weight_Adj_Fac
#      6) Z06-Z08: Update T_Cohorts_Recoded with Weight_NLS
#      7) Z09: Validate weights by re-calculating expected values
#
# Weight Components:
#   - Count: Number of records in the group
#   - Respondents: Number of survey respondents (respondent == "1")
#   - Weight_Prob: Response rate (Count / Respondents)
#   - Weight_Year: Year-specific weight from T_Weights
#   - Weight_Adj_Fac: Adjustment for over/under estimation
#   - Weight_NLS: Final calibrated weight for supply calculations

## ---- Cohort Response by Region ----
# Refactored from: DACSO_Q005_Z_Cohort_Resp_by_Region
cohort_resp_by_region <- cohorts_base %>%
  inner_join(
    t_current_region_pssm_codes,
    by = c("current_region_pssm_code" = "current_region_pssm_code")
  ) %>%
  filter(
    !is.na(age_group_rollup),
    respondent == "1",
    weight > 0
  ) %>%
  group_by(
    survey,
    survey_year,
    current_region_pssm_code,
    current_region_pssm_name,
    age_group_rollup
  ) %>%
  summarize(N = n(), .groups = "drop") %>%
  arrange(survey, survey_year, current_region_pssm_code) %>%
  collect()

message("Cohort response by region:")
print(cohort_resp_by_region)

## ---- Z01: Base NLS ----
# Refactored from: DACSO_Q005_Z01_Base_NLS
z01_base_nls <- cohorts_base %>%
  filter(
    new_labour_supply %in% c(0, 1, 2, 3),
    weight > 0,
    !is.na(age_group_rollup),
    grad_status %in% c("1", "3")
  ) %>%
  select(
    survey,
    survey_year,
    inst_cd,
    age_group_rollup,
    grad_status,
    ttrain,
    lcip4_cred,
    stqu_id
  ) %>%
  compute(name = "DACSO_Q005_Z01_Base_NLS", temporary = FALSE)

## ---- Z02: Weight Calculations ----
# Refactored from: DACSO_Q005_Z02c_Weight_tmp, DACSO_Q005_Z02c_Weight, DACSO_Q005_Z03_Weight_Total
# Combine multiple steps into one dplyr chain

# Step Z02c_Weight_tmp: Aggregate by survey, year, institution, age group
z02_weight_tmp <- cohorts_base %>%
  filter(
    new_labour_supply %in% c(0, 1, 2, 3),
    weight > 0,
    !is.na(age_group_rollup),
    grad_status %in% c("1", "3")
  ) %>%
  group_by(
    survey,
    survey_year,
    inst_cd,
    age_group_rollup,
    grad_status,
    ttrain,
    lcip4_cred
  ) %>%
  summarize(
    Count = n(),
    Respondents = sum(
      respondent == "1" & current_region_pssm_code != -1,
      na.rm = TRUE
    ),
    .groups = "drop"
  ) %>%
  compute(name = "DACSO_Q005_Z02c_Weight_tmp", temporary = FALSE)

# Step Z02c_Weight: Calculate weight probability and weighted values
z02_weight <- z02_weight_tmp %>%
  mutate(
    Weight_Prob = if_else(
      Respondents == 0,
      1,
      as.numeric(Count) / as.numeric(Respondents)
    ),
    Weight_Year = weight, # weight from cohorts_base
    Weighted = Respondents * Weight_Prob * Weight_Year
  ) %>%
  compute(name = "DACSO_Q005_Z02c_Weight", temporary = FALSE)

# Step Z03: Weight totals by key dimensions
z03_weight_total <- z02_weight %>%
  group_by(
    survey,
    inst_cd,
    age_group_rollup,
    grad_status,
    ttrain,
    lcip4_cred
  ) %>%
  summarize(
    Base = sum(Count, na.rm = TRUE),
    Weighted = sum(Weighted, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  compute(name = "DACSO_Q005_Z03_Weight_Total", temporary = FALSE)

# Step Z04: Adjustment factor
z04_weight_adj <- z03_weight_total %>%
  mutate(
    Weight_Adj_Fac = if_else(
      Weighted == 0,
      0,
      as.numeric(Base) / as.numeric(Weighted)
    )
  ) %>%
  compute(name = "DACSO_Q005_Z04_Weight_Adj_Fac", temporary = FALSE)

# Step Z05: Calculate Weight_NLS
z05_weight_nls <- z02_weight %>%
  inner_join(
    z04_weight_adj,
    by = c(
      "survey",
      "inst_cd",
      "age_group_rollup",
      "grad_status",
      "ttrain",
      "lcip4_cred"
    )
  ) %>%
  mutate(
    Weight_NLS = weight * Weight_Adj_Fac
  ) %>%
  select(
    survey,
    survey_year,
    inst_cd,
    age_group_rollup,
    grad_status,
    ttrain,
    lcip4_cred,
    count = Count,
    respondents = Respondents,
    weight_prob = Weight_Prob,
    weight_year = Weight_Year,
    weight = Weight,
    weighted = Weighted,
    weight_adj_fac = Weight_Adj_Fac,
    weight_nls = Weight_NLS
  ) %>%
  compute(name = "tmp_tbl_Weights_NLS", temporary = FALSE)

# Step Z06-Z08: Update T_Cohorts_Recoded with Weight_NLS
# This requires a join-based update, which we do via direct SQL for the UPDATE pattern
# (Alternatively, we could collect and re-write, but UPDATE is more efficient here)

dbExecute(
  con,
  glue::glue(
    "ALTER TABLE {my_schema}.T_Cohorts_Recoded ADD Weight_NLS FLOAT NULL"
  )
)
dbExecute(
  con,
  glue::glue("UPDATE {my_schema}.T_Cohorts_Recoded SET Weight_NLS = NULL")
)

# Update using SQL (most efficient for this pattern)
update_nls_query <- glue::glue(
  "
  UPDATE tcr
  SET tcr.Weight_NLS = z05.weight_nls
  FROM {my_schema}.T_Cohorts_Recoded AS tcr
  INNER JOIN {my_schema}.DACSO_Q005_Z01_Base_NLS AS z01
    ON tcr.stqu_id = z01.stqu_id
  INNER JOIN {my_schema}.tmp_tbl_Weights_NLS AS z05
    ON tcr.lcip4_cred = z05.lcip4_cred
    AND tcr.grad_status = z05.grad_status
    AND tcr.age_group_rollup = z05.age_group_rollup
    AND tcr.inst_cd = z05.inst_cd
    AND tcr.survey_year = z05.survey_year
    AND tcr.survey = z05.survey
  WHERE tcr.current_region_pssm_code <> -1
"
)
dbExecute(con, update_nls_query)

# =============================================================================
# Weight Validation (Z09)
# =============================================================================

# Refactored from: DACSO_Q005_Z09_Check_Weights
weight_validation <- cohorts_base %>%
  inner_join(
    tbl(con, in_schema(my_schema, "DACSO_Q005_Z01_Base_NLS")),
    by = "stqu_id"
  ) %>%
  filter(current_region_pssm_code != -1) %>%
  group_by(
    survey_year,
    inst_cd,
    age_group_rollup,
    grad_status,
    ttrain,
    lcip4_cred,
    weight_nls
  ) %>%
  summarize(
    Respondents = sum(
      respondent == "1" & current_region_pssm_code != -1,
      na.rm = TRUE
    ),
    Weighted = sum(
      respondent == "1" & current_region_pssm_code != -1,
      na.rm = TRUE
    ) *
      as.numeric(weight_nls),
    Base = sum(base),
    .groups = "drop"
  ) %>%
  arrange(survey_year, weight_nls) %>%
  collect()

message("Weight validation:")
print(weight_validation)

# =============================================================================
# Labour Supply Distributions (Q006)
# =============================================================================
#
# WHAT: Creates weighted aggregations of labour supply by various dimensions.
# WHY: The PSSM model requires distribution tables showing the proportion of graduates
#      in each labour supply category, stratified by credential, CIP, region, and age.
# HOW: 1) Filter cohorts with valid weights and respondent status
#      2) Aggregate by multiple dimension combinations:
#         - By 4-digit CIP with/without ttrain
#         - By 2-digit CIP with/without ttrain
#         - With/without labour supply category (0 vs 1,2,3)
#      3) Calculate weighted counts using Weight_NLS
#
# Output Distribution Tables:
#   - q006b_weighted_nls: NLS 1,2,3 by 4-digit CIP (with ttrain)
#   - q006b_weighted_nls_0: NLS 0 by 4-digit CIP (with ttrain)
#   - q006b_weighted_nls_2d: NLS 1,2,3 by 2-digit CIP (with ttrain)
#   - q006b_weighted_nls_2d_no_tt: NLS 1,2,3 by 2-digit CIP (no ttrain)
#   - Plus variants for totals and no-ttrain configurations

## ---- Q006a: Weight New Labour Supply by region rollup ----
# Refactored from: DACSO_Q006a_Weight_New_Labour_Supply
q006a_weight_nls <- cohorts_base %>%
  inner_join(
    t_current_region_pssm_codes %>%
      inner_join(
        t_current_region_pssm_rollup_codes,
        by = "current_region_pssm_code_rollup"
      ),
    by = c("current_region_pssm_code" = "current_region_pssm_code")
  ) %>%
  filter(
    respondent == "1",
    weight > 0,
    !is.na(age_group_rollup),
    grad_status %in% c("1", "3"),
    new_labour_supply %in% c(0, 1, 2, 3),
    current_region_pssm_code_rollup != 9999
  ) %>%
  mutate(Weighted = weight_nls) %>%
  compute(name = "DACSO_Q006a_Weight_New_Labour_Supply", temporary = FALSE)

# =============================================================================
# Labour Supply Distribution Aggregations (Q006b Series)
# =============================================================================
#
# WHAT: Generates multiple aggregation variants of labour supply distributions.
# WHY: Different downstream processes require different dimensional views of the same data.
#      Some need ttrain dimension, some don't. Some aggregate to 2-digit CIP.
# HOW: 1) Filter data by NLS category (0, 1-3, or all)
#      2) Create lcip2_cred and lcp2_cd columns as needed
#      3) Aggregate by specified dimensions
#      4) Calculate Count (weighted sum) and Total (for percentage calculation)
#
# Aggregation Dimensions:
#   - lcp4_cd: 4-digit CIP code
#   - lcp2_cd: 2-digit CIP code (derived from lcp4_cd)
#   - ttrain: Training type (1 = trades, 0 = other)
#   - lcip4_cred: Credential grouping with 4-digit CIP
#   - lcip2_cred: Credential grouping with 2-digit CIP
#   - lcp2_cred: Credential grouping with 2-digit CIP (alternate format)
#
# TODO [MEDIUM]: Consolidate similar aggregation patterns into helper function

## Helper function for Q006b aggregations
create_q006b_aggregation <- function(
  data,
  new_labour_supply_values = NULL,
  no_tt = FALSE,
  lcp2 = FALSE
) {
  result <- data

  if (!is.null(new_labour_supply_values)) {
    result <- result %>% filter(new_labour_supply %in% new_labour_supply_values)
  }

  if (no_tt) {
    result <- result %>%
      mutate(
        lcp4_cd = lcp4_cd,
        lcip4_cred = if_else(
          is.na(grad_status),
          NA_character_,
          paste0(grad_status, " - ", lcp4_cd, " - ", pssm_credential)
        ),
        lcip2_cred = if_else(
          is.na(grad_status),
          NA_character_,
          paste0(
            grad_status,
            " - ",
            substr(lcp4_cd, 1, 2),
            " - ",
            pssm_credential
          )
        )
      )
  }

  if (lcp2) {
    result <- result %>%
      mutate(
        lcp2_cd = substr(lcp4_cd, 1, 2),
        lcp2_cred = if_else(
          substr(pssm_cred, 1, 1) %in% c("1", "3"),
          paste0(
            substr(pssm_cred, 1, 1),
            " - ",
            substr(lcp4_cd, 1, 2),
            " - ",
            pssm_credential
          ),
          paste0(substr(lcp4_cd, 1, 2), " - ", pssm_credential)
        )
      )
  }

  result %>%
    group_by(
      survey,
      pssm_credential,
      pssm_cred,
      current_region_pssm_code_rollup,
      age_group_rollup,
      lcp4_cd,
      lcp2_cd,
      ttrain,
      lcip4_cred,
      lcip2_cred,
      lcp2_cred
    ) %>%
    summarize(
      Count = sum(Weighted, na.rm = TRUE),
      Unweighted_Count = n(),
      Total = sum(weight_nls, na.rm = TRUE),
      .groups = "drop"
    )
}

# Q006b_Weighted_New_Labour_Supply (new_labour_supply in 1,2,3)
q006b_weighted_nls <- q006a_weight_nls %>%
  filter(new_labour_supply %in% c(1, 2, 3)) %>%
  group_by(
    survey,
    pssm_credential,
    pssm_cred,
    current_region_pssm_code_rollup,
    age_group_rollup,
    lcp4_cd,
    ttrain,
    lcip4_cred,
    lcip2_cred
  ) %>%
  summarize(
    Count = sum(Weighted, na.rm = TRUE),
    Unweighted_Count = n(),
    .groups = "drop"
  ) %>%
  compute(name = "dacso_q006b_weighted_new_labour_supply", temporary = FALSE)

# Q006b_Weighted_New_Labour_Supply_0 (new_labour_supply = 0)
q006b_weighted_nls_0 <- q006a_weight_nls %>%
  filter(new_labour_supply == 0) %>%
  group_by(
    survey,
    pssm_credential,
    pssm_cred,
    current_region_pssm_code_rollup,
    age_group_rollup,
    lcp4_cd,
    ttrain,
    lcip4_cred,
    lcip2_cred
  ) %>%
  summarize(Count = sum(Weighted, na.rm = TRUE), .groups = "drop") %>%
  compute(name = "dacso_q006b_weighted_new_labour_supply_0", temporary = FALSE)

# Q006b_Weighted_New_Labour_Supply_2D (LCP2, new_labour_supply in 1,2,3)
q006b_weighted_nls_2d <- q006a_weight_nls %>%
  filter(new_labour_supply %in% c(1, 2, 3)) %>%
  mutate(lcp2_cd = substr(lcp4_cd, 1, 2)) %>%
  group_by(
    survey,
    pssm_credential,
    pssm_cred,
    current_region_pssm_code_rollup,
    age_group_rollup,
    lcp2_cd,
    ttrain,
    lcip2_cred
  ) %>%
  summarize(Count = sum(Weighted, na.rm = TRUE), .groups = "drop") %>%
  compute(name = "dacso_q006b_weighted_new_labour_supply_2d", temporary = FALSE)

# Q006b_Weighted_New_Labour_Supply_2D_No_TT
q006b_weighted_nls_2d_no_tt <- q006a_weight_nls %>%
  filter(new_labour_supply %in% c(1, 2, 3)) %>%
  mutate(
    lcp2_cd = substr(lcp4_cd, 1, 2),
    lcp2_cred = case_when(
      substr(pssm_cred, 1, 1) %in% c("1", "3") ~
        paste0(
          substr(pssm_cred, 1, 1),
          " - ",
          substr(lcp4_cd, 1, 2),
          " - ",
          pssm_credential
        ),
      TRUE ~ paste0(substr(lcp4_cd, 1, 2), " - ", pssm_credential)
    )
  ) %>%
  group_by(
    survey,
    pssm_credential,
    pssm_cred,
    current_region_pssm_code_rollup,
    age_group_rollup,
    lcp2_cd,
    lcp2_cred
  ) %>%
  summarize(Count = sum(Weighted, na.rm = TRUE), .groups = "drop") %>%
  compute(
    name = "dacso_q006b_weighted_new_labour_supply_2d_no_tt",
    temporary = FALSE
  )

# Q006b_Weighted_New_Labour_Supply_0_2D
q006b_weighted_nls_0_2d <- q006a_weight_nls %>%
  filter(new_labour_supply == 0) %>%
  mutate(lcp2_cd = substr(lcp4_cd, 1, 2)) %>%
  group_by(
    survey,
    pssm_credential,
    pssm_cred,
    current_region_pssm_code_rollup,
    age_group_rollup,
    lcp2_cd,
    ttrain,
    lcip2_cred
  ) %>%
  summarize(Count = sum(Weighted, na.rm = TRUE), .groups = "drop") %>%
  compute(
    name = "dacso_q006b_weighted_new_labour_supply_0_2d",
    temporary = FALSE
  )

# Q006b_Weighted_New_Labour_Supply_0_2D_No_TT
q006b_weighted_nls_0_2d_no_tt <- q006a_weight_nls %>%
  filter(new_labour_supply == 0) %>%
  mutate(
    lcp2_cd = substr(lcp4_cd, 1, 2),
    lcp2_cred = case_when(
      substr(pssm_cred, 1, 1) %in% c("1", "3") ~
        paste0(
          substr(pssm_cred, 1, 1),
          " - ",
          substr(lcp4_cd, 1, 2),
          " - ",
          pssm_credential
        ),
      TRUE ~ paste0(substr(lcp4_cd, 1, 2), " - ", pssm_credential)
    )
  ) %>%
  group_by(
    survey,
    pssm_credential,
    pssm_cred,
    current_region_pssm_code_rollup,
    age_group_rollup,
    lcp2_cd,
    lcp2_cred
  ) %>%
  summarize(Count = sum(Weighted, na.rm = TRUE), .groups = "drop") %>%
  compute(
    name = "dacso_q006b_weighted_new_labour_supply_0_2d_no_tt",
    temporary = FALSE
  )

# Q006b_Weighted_New_Labour_Supply_No_TT (new_labour_supply in 1,2,3, no ttrain)
q006b_weighted_nls_no_tt <- q006a_weight_nls %>%
  filter(new_labour_supply %in% c(1, 2, 3)) %>%
  mutate(
    lcip4_cred = if_else(
      is.na(grad_status),
      NA_character_,
      paste0(grad_status, " - ", lcp4_cd, " - ", pssm_credential)
    ),
    lcip2_cred = if_else(
      is.na(grad_status),
      NA_character_,
      paste0(grad_status, " - ", substr(lcp4_cd, 1, 2), " - ", pssm_credential)
    )
  ) %>%
  group_by(
    survey,
    pssm_credential,
    pssm_cred,
    current_region_pssm_code_rollup,
    age_group_rollup,
    lcp4_cd,
    lcip4_cred,
    lcip2_cred
  ) %>%
  summarize(
    Count = sum(Weighted, na.rm = TRUE),
    Unweighted_Count = n(),
    .groups = "drop"
  ) %>%
  compute(
    name = "dacso_q006b_weighted_new_labour_supply_no_tt",
    temporary = FALSE
  )

# Q006b_Weighted_New_Labour_Supply_0_No_TT
q006b_weighted_nls_0_no_tt <- q006a_weight_nls %>%
  filter(new_labour_supply == 0) %>%
  mutate(
    lcip4_cred = if_else(
      is.na(grad_status),
      NA_character_,
      paste0(grad_status, " - ", lcp4_cd, " - ", pssm_credential)
    ),
    lcip2_cred = if_else(
      is.na(grad_status),
      NA_character_,
      paste0(grad_status, " - ", substr(lcp4_cd, 1, 2), " - ", pssm_credential)
    )
  ) %>%
  group_by(
    survey,
    pssm_credential,
    pssm_cred,
    current_region_pssm_code_rollup,
    age_group_rollup,
    lcp4_cd,
    lcip4_cred,
    lcip2_cred
  ) %>%
  summarize(Count = sum(Weighted, na.rm = TRUE), .groups = "drop") %>%
  compute(
    name = "dacso_q006b_weighted_new_labour_supply_0_no_tt",
    temporary = FALSE
  )

# Q006b_Weighted_New_Labour_Supply_Total (all new_labour_supply values)
q006b_weighted_nls_total <- q006a_weight_nls %>%
  filter(new_labour_supply %in% c(0, 1, 2, 3)) %>%
  group_by(
    survey,
    pssm_credential,
    pssm_cred,
    age_group_rollup,
    lcp4_cd,
    ttrain,
    lcip4_cred,
    lcip2_cred
  ) %>%
  summarize(Total = sum(Weighted, na.rm = TRUE), .groups = "drop") %>%
  compute(
    name = "dacso_q006b_weighted_new_labour_supply_total",
    temporary = FALSE
  )

# Q006b_Weighted_New_Labour_Supply_Total_2D
q006b_weighted_nls_total_2d <- q006a_weight_nls %>%
  filter(new_labour_supply %in% c(0, 1, 2, 3)) %>%
  mutate(lcp2_cd = substr(lcp4_cd, 1, 2)) %>%
  group_by(
    survey,
    pssm_credential,
    pssm_cred,
    age_group_rollup,
    lcp2_cd,
    ttrain,
    lcip2_cred
  ) %>%
  summarize(Total = sum(Weighted, na.rm = TRUE), .groups = "drop") %>%
  compute(
    name = "dacso_q006b_weighted_new_labour_supply_total_2d",
    temporary = FALSE
  )

# Q006b_Weighted_New_Labour_Supply_Total_2D_No_TT
q006b_weighted_nls_total_2d_no_tt <- q006a_weight_nls %>%
  filter(new_labour_supply %in% c(0, 1, 2, 3)) %>%
  mutate(
    lcp2_cd = substr(lcp4_cd, 1, 2),
    lcp2_cred = case_when(
      substr(pssm_cred, 1, 1) %in% c("1", "3") ~
        paste0(
          substr(pssm_cred, 1, 1),
          " - ",
          substr(lcp4_cd, 1, 2),
          " - ",
          pssm_credential
        ),
      TRUE ~ paste0(substr(lcp4_cd, 1, 2), " - ", pssm_credential)
    )
  ) %>%
  group_by(
    survey,
    pssm_credential,
    pssm_cred,
    age_group_rollup,
    lcp2_cd,
    lcp2_cred
  ) %>%
  summarize(Total = sum(Weighted, na.rm = TRUE), .groups = "drop") %>%
  compute(
    name = "dacso_q006b_weighted_new_labour_supply_total_2d_no_tt",
    temporary = FALSE
  )

# Q006b_Weighted_New_Labour_Supply_Total_No_TT
q006b_weighted_nls_total_no_tt <- q006a_weight_nls %>%
  filter(new_labour_supply %in% c(0, 1, 2, 3)) %>%
  mutate(
    lcip4_cred = if_else(
      is.na(grad_status),
      NA_character_,
      paste0(grad_status, " - ", lcp4_cd, " - ", pssm_credential)
    ),
    lcip2_cred = if_else(
      is.na(grad_status),
      NA_character_,
      paste0(grad_status, " - ", substr(lcp4_cd, 1, 2), " - ", pssm_credential)
    )
  ) %>%
  group_by(
    survey,
    pssm_credential,
    pssm_cred,
    age_group_rollup,
    lcp4_cd,
    lcip4_cred,
    lcip2_cred
  ) %>%
  summarize(Total = sum(Weighted, na.rm = TRUE), .groups = "drop") %>%
  compute(
    name = "dacso_q006b_weighted_new_labour_supply_total_no_tt",
    temporary = FALSE
  )

# =============================================================================
# Calculate Percentages (Q007a Series)
# =============================================================================
#
# WHAT: Calculates the percentage of labour supply for each category.
# WHY: The model needs proportions, not just counts, to apply supply rates consistently.
# HOW: 1) Join count data to total data by common dimensions
#      2) Calculate percentage = Count / Total
#      3) Handle edge cases (zero totals, NULL values)
#
# Percentage Formulas:
#   - NLS 1,2,3: perc = Count / Total
#   - NLS 0: perc = 1 - Count / Total (complement of employed)
#
# Output Tables:
#   - q007a_weighted_nls: Percentages for NLS 1,2,3 (4-digit CIP, with ttrain)
#   - q007a_weighted_nls_0: Percentages for NLS 0 (4-digit CIP, with ttrain)
#   - Plus variants for 2-digit and no-ttrain configurations

## Helper to calculate percentages
calculate_nls_percentages <- function(
  count_data,
  total_data,
  count_col = "Count",
  total_col = "Total"
) {
  count_data %>%
    left_join(
      total_data %>%
        select(
          survey,
          pssm_credential,
          pssm_cred,
          current_region_pssm_code_rollup,
          age_group_rollup,
          lcp4_cd,
          ttrain,
          lcp2_cd,
          lcip4_cred,
          lcip2_cred,
          lcp2_cred,
          !!total_col
        ),
      by = c(
        "survey",
        "pssm_credential",
        "pssm_cred",
        "current_region_pssm_code_rollup",
        "age_group_rollup",
        "lcp4_cd",
        "ttrain",
        "lcp2_cd",
        "lcip4_cred",
        "lcip2_cred",
        "lcp2_cred"
      )
    ) %>%
    mutate(
      perc = if_else(
        !is.na(!!sym(count_col)) & !!sym(total_col) > 0,
        as.numeric(!!sym(count_col)) / as.numeric(!!sym(total_col)),
        NA_real_
      )
    )
}

# Q007a_Weighted_New_Labour_Supply (1,2,3 / Total)
q007a_weighted_nls <- q006b_weighted_nls %>%
  left_join(
    q006b_weighted_nls_total %>%
      select(
        survey,
        pssm_credential,
        pssm_cred,
        age_group_rollup,
        lcp4_cd,
        ttrain,
        lcip4_cred,
        lcip2_cred,
        Total
      ),
    by = c(
      "survey",
      "pssm_credential",
      "pssm_cred",
      "age_group_rollup",
      "lcp4_cd",
      "ttrain",
      "lcip4_cred",
      "lcip2_cred"
    )
  ) %>%
  mutate(
    perc = if_else(
      !is.na(Count) & Total > 0,
      as.numeric(Count) / as.numeric(Total),
      NA_real_
    )
  ) %>%
  compute(name = "DACSO_Q007a_Weighted_New_Labour_Supply", temporary = FALSE)

# Q007a_Weighted_New_Labour_Supply_0 (0 / Total, then 1-perc)
q007a_weighted_nls_0 <- q006b_weighted_nls_0 %>%
  left_join(
    q006b_weighted_nls_total %>%
      select(
        survey,
        pssm_credential,
        pssm_cred,
        age_group_rollup,
        lcp4_cd,
        ttrain,
        lcip4_cred,
        lcip2_cred,
        Total
      ),
    by = c(
      "survey",
      "pssm_credential",
      "pssm_cred",
      "age_group_rollup",
      "lcp4_cd",
      "ttrain",
      "lcip4_cred",
      "lcip2_cred"
    )
  ) %>%
  mutate(
    perc = if_else(
      Count > 0 & Total > 0,
      1 - as.numeric(Count) / as.numeric(Total),
      NA_real_
    )
  ) %>%
  compute(name = "DACSO_Q007a_Weighted_New_Labour_Supply_0", temporary = FALSE)

# Q007a_Weighted_New_Labour_Supply_2D
q007a_weighted_nls_2d <- q006b_weighted_nls_2d %>%
  left_join(
    q006b_weighted_nls_total_2d %>%
      select(
        survey,
        pssm_credential,
        pssm_cred,
        age_group_rollup,
        lcp2_cd,
        ttrain,
        lcp2_cred,
        Total
      ),
    by = c(
      "survey",
      "pssm_credential",
      "pssm_cred",
      "age_group_rollup",
      "lcp2_cd",
      "ttrain",
      "lcp2_cred"
    )
  ) %>%
  mutate(
    perc = if_else(
      !is.na(Count) & Total > 0,
      as.numeric(Count) / as.numeric(Total),
      NA_real_
    )
  ) %>%
  filter(!is.na(current_region_pssm_code_rollup)) %>%
  compute(name = "DACSO_Q007a_Weighted_New_Labour_Supply_2D", temporary = FALSE)

# Q007a_Weighted_New_Labour_Supply_2D_No_TT
q007a_weighted_nls_2d_no_tt <- q006b_weighted_nls_2d_no_tt %>%
  left_join(
    q006b_weighted_nls_total_2d_no_tt %>%
      select(
        survey,
        pssm_credential,
        pssm_cred,
        age_group_rollup,
        lcp2_cd,
        lcp2_cred,
        Total
      ),
    by = c(
      "survey",
      "pssm_credential",
      "pssm_cred",
      "age_group_rollup",
      "lcp2_cd",
      "lcp2_cred"
    )
  ) %>%
  mutate(
    perc = if_else(
      !is.na(Count) & Total > 0,
      as.numeric(Count) / as.numeric(Total),
      NA_real_
    )
  ) %>%
  filter(!is.na(current_region_pssm_code_rollup)) %>%
  compute(
    name = "DACSO_Q007a_Weighted_New_Labour_Supply_2D_No_TT",
    temporary = FALSE
  )

# Q007a_Weighted_New_Labour_Supply_0_2D
q007a_weighted_nls_0_2d <- q006b_weighted_nls_0_2d %>%
  left_join(
    q006b_weighted_nls_total_2d %>%
      select(
        survey,
        pssm_credential,
        pssm_cred,
        age_group_rollup,
        lcp2_cd,
        ttrain,
        lcp2_cred,
        Total
      ),
    by = c(
      "survey",
      "pssm_credential",
      "pssm_cred",
      "age_group_rollup",
      "lcp2_cd",
      "ttrain",
      "lcp2_cred"
    )
  ) %>%
  mutate(
    perc = if_else(
      Count > 0 & Total > 0,
      1 - as.numeric(Count) / as.numeric(Total),
      NA_real_
    )
  ) %>%
  compute(
    name = "DACSO_Q007a_Weighted_New_Labour_Supply_0_2D",
    temporary = FALSE
  )

# Q007a_Weighted_New_Labour_Supply_0_2D_No_TT
q007a_weighted_nls_0_2d_no_tt <- q006b_weighted_nls_0_2d_no_tt %>%
  left_join(
    q006b_weighted_nls_total_2d_no_tt %>%
      select(
        survey,
        pssm_credential,
        pssm_cred,
        age_group_rollup,
        lcp2_cd,
        lcp2_cred,
        Total
      ),
    by = c(
      "survey",
      "pssm_credential",
      "pssm_cred",
      "age_group_rollup",
      "lcp2_cd",
      "lcp2_cred"
    )
  ) %>%
  mutate(
    perc = if_else(
      Count > 0 & Total > 0,
      1 - as.numeric(Count) / as.numeric(Total),
      NA_real_
    )
  ) %>%
  compute(
    name = "DACSO_Q007a_Weighted_New_Labour_Supply_0_2D_No_TT",
    temporary = FALSE
  )

# Q007a_Weighted_New_Labour_Supply_No_TT
q007a_weighted_nls_no_tt <- q006b_weighted_nls_no_tt %>%
  left_join(
    q006b_weighted_nls_total_no_tt %>%
      select(
        survey,
        pssm_credential,
        pssm_cred,
        age_group_rollup,
        lcp4_cd,
        lcip4_cred,
        lcip2_cred,
        Total
      ),
    by = c(
      "survey",
      "pssm_credential",
      "pssm_cred",
      "age_group_rollup",
      "lcp4_cd",
      "lcip4_cred",
      "lcip2_cred"
    )
  ) %>%
  mutate(
    perc = if_else(
      !is.na(Count) & Total > 0,
      as.numeric(Count) / as.numeric(Total),
      NA_real_
    )
  ) %>%
  filter(!is.na(current_region_pssm_code_rollup)) %>%
  compute(
    name = "DACSO_Q007a_Weighted_New_Labour_Supply_No_TT",
    temporary = FALSE
  )

# Q007a_Weighted_New_Labour_Supply_0_No_TT
q007a_weighted_nls_0_no_tt <- q006b_weighted_nls_0_no_tt %>%
  left_join(
    q006b_weighted_nls_total_no_tt %>%
      select(
        survey,
        pssm_credential,
        pssm_cred,
        age_group_rollup,
        lcp4_cd,
        lcip4_cred,
        lcip2_cred,
        Total
      ),
    by = c(
      "survey",
      "pssm_credential",
      "pssm_cred",
      "age_group_rollup",
      "lcp4_cd",
      "lcip4_cred",
      "lcip2_cred"
    )
  ) %>%
  mutate(
    perc = if_else(
      Count > 0 & Total > 0,
      1 - as.numeric(Count) / as.numeric(Total),
      NA_real_
    )
  ) %>%
  compute(
    name = "DACSO_Q007a_Weighted_New_Labour_Supply_0_No_TT",
    temporary = FALSE
  )

# =============================================================================
# Create Labour Supply Distribution Tables
# =============================================================================
#
# WHAT: Persists the calculated distributions to database tables for downstream use.
# WHY: The distribution tables are used by the occupation projections model to calculate
#      supply by occupation category.
# HOW: 1) Delete existing "Student Outcomes" records from distribution tables
#      2) Select and rename columns to match output schema
#      3) Collect data to R (required for dbAppendTable)
#      4) Append new records to distribution tables
#
# Output Tables:
#   - Labour_Supply_Distribution: 4-digit CIP, with ttrain
#   - Labour_Supply_Distribution_No_TT: 4-digit CIP, without ttrain
#   - Labour_Supply_Distribution_LCP2: 2-digit CIP, with ttrain
#   - Labour_Supply_Distribution_LCP2_No_TT: 2-digit CIP, without ttrain
#
# Schema Columns:
#   Survey, PSSM_Credential, PSSM_CRED, LCP4_CD/LCP2_CD, TTRAIN (optional),
#   LCIP4_CRED/LCIP2_CRED/LCP2_CRED, Current_Region_PSSM_Code_Rollup,
#   Age_Group_Rollup, Count, Total, New_Labour_Supply (percentage)

## ---- Define output table schema ----
nls_def <- c(
  Survey = "nvarchar(50)",
  PSSM_Credential = "nvarchar(50)",
  PSSM_CRED = "nvarchar(50)",
  LCP4_CD = "nvarchar(50)",
  TTRAIN = "nvarchar(50)",
  LCIP4_CRED = "nvarchar(50)",
  LCIP2_CRED = "nvarchar(50)",
  Current_Region_PSSM_Code_Rollup = "integer",
  Age_Group_Rollup = "integer",
  Count = "float",
  Total = "float",
  New_Labour_Supply = "float"
)

nls_lcp2_def <- c(
  Survey = "nvarchar(50)",
  PSSM_Credential = "nvarchar(50)",
  PSSM_CRED = "nvarchar(50)",
  LCP2_CD = "nvarchar(50)",
  TTRAIN = "nvarchar(50)",
  LCP2_CRED = "nvarchar(50)",
  Current_Region_PSSM_Code_Rollup = "integer",
  Age_Group_Rollup = "integer",
  Count = "float",
  Total = "float",
  New_Labour_Supply = "float"
)

## ---- Labour_Supply_Distribution ----
# Delete existing 'Student Outcomes' records
dbExecute(
  con,
  glue::glue(
    "DELETE FROM {my_schema}.Labour_Supply_Distribution WHERE Survey = 'Student Outcomes'"
  )
)
dbExecute(
  con,
  glue::glue(
    "DELETE FROM {my_schema}.Labour_Supply_Distribution_No_TT WHERE Survey = 'Student Outcomes'"
  )
)

# Append new data
nls_dist_data <- q007a_weighted_nls %>%
  mutate(Survey = "Student Outcomes") %>%
  select(
    Survey,
    PSSM_Credential = pssm_credential,
    PSSM_CRED = pssm_cred,
    Current_Region_PSSM_Code_Rollup = current_region_pssm_code_rollup,
    Age_Group_Rollup = age_group_rollup,
    LCP4_CD = lcp4_cd,
    TTRAIN = ttrain,
    LCIP4_CRED = lcip4_cred,
    LCIP2_CRED = lcip2_cred,
    Count,
    Total,
    New_Labour_Supply = perc
  ) %>%
  collect()

dbAppendTable(
  con,
  Id(schema = my_schema, table = "Labour_Supply_Distribution"),
  nls_dist_data
)

# Labour_Supply_Distribution_No_TT
nls_dist_no_tt_data <- q007a_weighted_nls_no_tt %>%
  mutate(Survey = "Student Outcomes") %>%
  select(
    Survey,
    PSSM_Credential = pssm_credential,
    PSSM_CRED = pssm_cred,
    Current_Region_PSSM_Code_Rollup = current_region_pssm_code_rollup,
    Age_Group_Rollup = age_group_rollup,
    LCP4_CD = lcp4_cd,
    LCIP4_CRED = lcip4_cred,
    LCIP2_CRED = lcip2_cred,
    Count,
    Total,
    New_Labour_Supply = perc
  ) %>%
  collect()

dbAppendTable(
  con,
  Id(schema = my_schema, table = "Labour_Supply_Distribution_No_TT"),
  nls_dist_no_tt_data
)

# Labour_Supply_Distribution_LCP2
dbExecute(
  con,
  glue::glue(
    "DELETE FROM {my_schema}.Labour_Supply_Distribution_LCP2 WHERE Survey = 'Student Outcomes'"
  )
)
dbExecute(
  con,
  glue::glue(
    "DELETE FROM {my_schema}.Labour_Supply_Distribution_LCP2_No_TT WHERE Survey = 'Student Outcomes'"
  )
)

nls_lcp2_dist_data <- q007a_weighted_nls_2d %>%
  mutate(Survey = "Student Outcomes") %>%
  select(
    Survey,
    PSSM_Credential = pssm_credential,
    PSSM_CRED = pssm_cred,
    Current_Region_PSSM_Code_Rollup = current_region_pssm_code_rollup,
    Age_Group_Rollup = age_group_rollup,
    LCP2_CD = lcp2_cd,
    TTRAIN = ttrain,
    LCP2_CRED = lcp2_cred,
    Count,
    Total,
    New_Labour_Supply = perc
  ) %>%
  collect()

dbAppendTable(
  con,
  Id(schema = my_schema, table = "Labour_Supply_Distribution_LCP2"),
  nls_lcp2_dist_data
)

# Labour_Supply_Distribution_LCP2_No_TT
nls_lcp2_dist_no_tt_data <- q007a_weighted_nls_2d_no_tt %>%
  mutate(Survey = "Student Outcomes") %>%
  select(
    Survey,
    PSSM_Credential = pssm_credential,
    PSSM_CRED = pssm_cred,
    Current_Region_PSSM_Code_Rollup = current_region_pssm_code_rollup,
    Age_Group_Rollup = age_group_rollup,
    LCP2_CD = lcp2_cd,
    LCP2_CRED = lcp2_cred,
    Count,
    Total,
    New_Labour_Supply = perc
  ) %>%
  collect()

dbAppendTable(
  con,
  Id(schema = my_schema, table = "Labour_Supply_Distribution_LCP2_No_TT"),
  nls_lcp2_dist_no_tt_data
)

# =============================================================================
# Cleanup
# =============================================================================

# Remove computed tables
computed_cleanup <- c(
  "T_Cohorts_Recoded_NOC_Fixed",
  "DACSO_Q005_Z01_Base_NLS",
  "DACSO_Q005_Z02c_Weight_tmp",
  "DACSO_Q005_Z02c_Weight",
  "DACSO_Q005_Z03_Weight_Total",
  "DACSO_Q005_Z04_Weight_Adj_Fac",
  "tmp_tbl_Weights_NLS",
  "DACSO_Q006a_Weight_New_Labour_Supply",
  "dacso_q006b_weighted_new_labour_supply",
  "dacso_q006b_weighted_new_labour_supply_0",
  "dacso_q006b_weighted_new_labour_supply_2d",
  "dacso_q006b_weighted_new_labour_supply_2d_no_tt",
  "dacso_q006b_weighted_new_labour_supply_0_2d",
  "dacso_q006b_weighted_new_labour_supply_0_2d_no_tt",
  "dacso_q006b_weighted_new_labour_supply_no_tt",
  "dacso_q006b_weighted_new_labour_supply_0_no_tt",
  "dacso_q006b_weighted_new_labour_supply_total",
  "dacso_q006b_weighted_new_labour_supply_total_2d",
  "dacso_q006b_weighted_new_labour_supply_total_2d_no_tt",
  "dacso_q006b_weighted_new_labour_supply_total_no_tt",
  "DACSO_Q007a_Weighted_New_Labour_Supply",
  "DACSO_Q007a_Weighted_New_Labour_Supply_0",
  "DACSO_Q007a_Weighted_New_Labour_Supply_2D",
  "DACSO_Q007a_Weighted_New_Labour_Supply_2D_No_TT",
  "DACSO_Q007a_Weighted_New_Labour_Supply_0_2D",
  "DACSO_Q007a_Weighted_New_Labour_Supply_0_2D_No_TT",
  "DACSO_Q007a_Weighted_New_Labour_Supply_No_TT",
  "DACSO_Q007a_Weighted_New_Labour_Supply_0_No_TT"
)

for (table in computed_cleanup) {
  if (dbExistsTable(con, Id(schema = my_schema, table = table))) {
    dbRemoveTable(con, Id(schema = my_schema, table = table))
  }
}

# =============================================================================
# Verification
# =============================================================================

message("Verifying output tables...")
dbExistsTable(con, "Labour_Supply_Distribution")
dbExistsTable(con, "Labour_Supply_Distribution_No_TT")
dbExistsTable(con, "Labour_Supply_Distribution_LCP2")
dbExistsTable(con, "Labour_Supply_Distribution_LCP2_No_TT")

dbDisconnect(con)

message("02b-2-pssm-cohorts-new-labour-supply.R refactoring complete.")
