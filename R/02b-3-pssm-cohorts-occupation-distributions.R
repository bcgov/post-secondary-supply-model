# =============================================================================
# R/02b-3-pssm-cohorts-occupation-distributions.R
# =============================================================================
#
# PSSM Model Context:
# This script calculates occupational distributions for graduates from the
# Student Outcomes surveys (TRD, APPSO, BGS, DACSO). It builds on the labour
# supply weights calculated in 02b-2 and applies them to NOC (National
# Occupational Classification) codes to determine where graduates work.
#
# Required Source Tables
#   T_Cohorts_Recoded - Unified cohort table with NOC codes
#   Labour_Supply_Distribution - NLS distribution table (for PDEG estimates)
#   Occupation_Distributions - Previous occupation data (for PDEG estimates)
#
# Required Lookup Tables
#   t_current_region_pssm_codes, t_current_region_pssm_rollup_codes
#   t_current_region_pssm_rollup_codes_bc, tbl_noc_skill_level_aged_17_34
#   tbl_age_groups
#
# Resulting Tables
#   Occupation_Distributions - LCP4 level, with TTRAIN
#   Occupation_Distributions_No_TT - LCP4 level, without TTRAIN
#   Occupation_Distributions_LCP2 - LCP2 level, with TTRAIN
#   Occupation_Distributions_LCP2_No_TT - LCP2 level, without TTRAIN
#   Occupation_Distributions_LCP2_BC - BC-only regional rollup
#   Occupation_Distributions_LCP2_BC_No_TT - BC-only, no TTRAIN
#
# WHAT: Calculates weighted occupation distributions for all cohort records.
# WHY: The PSSM model needs to project future workforce by occupation. This script
#      creates the bridge between education credentials and occupational outcomes.
#      We calculate weighted occupation distributions that account for:
#        - Survey weighting (respondents vs population)
#        - Regional differences (via region rollup codes)
#        - Age group variations
#        - Credential type (BACH, etc.)
#        - Training type (trade vs non-trade)
#
# HOW:
#   1) Calculate occupation weights (Weight_OCC) similar to labour supply weights
#      - Z01-Z08 series: Base weights -> Respondent counts -> Adjustment factors
#   2) Aggregate occupation counts by NOC code with weights applied
#      - Q009 series: Weight occupations by NOC and credential combinations
#   3) Calculate percentages (count / total for each group)
#      - Q010 series: Distribution tables with percent breakdowns
#   4) Handle special PDEG (Post-Secondary Degree Equivalent Graduate) cases
#      - Q010d-e: Law cluster (CIP 07) special handling using BACH/CIP 22 data
#   5) Append results to final distribution tables
#
# Weight Formula:
#   Weight_NLS_Base = Count / Respondents
#   Weighted = Respondents * Weight_NLS_Base
#   Weight_Adj_Fac = Base / Weighted
#   Weight_OCC = Weight_NLS_Base * Weight_Adj_Fac
#
# TODO [HIGH]: Add validation for NOC skill level consistency
# TODO [MEDIUM]: Consolidate similar aggregation patterns into helper functions
# TODO [LOW]: Add parallel processing for distribution aggregations

library(tidyverse)
library(arrow)
library(odbc)
library(DBI)
library(dbplyr)
library(config)
library(glue)

# =============================================================================
# SECTION 1: SETUP AND CONFIGURATION
# =============================================================================
# WHAT: Establish database connection and load lookup tables needed for
#       occupation weight calculations.
#
# WHY: Need access to cohort data (with NOC codes) and geographic rollup
#      information to properly weight and aggregate occupations.
#
# HOW:
#   - Connect to the decimal database using config settings
#   - Load lookup tables: region codes, age groups, NOC skill levels
#   - These tables are used throughout the weighting calculations

db_config <- config::get("decimal")
my_schema <- config::get("myschema")

con <- dbConnect(
  odbc(),
  Driver = db_config$driver,
  Server = db_config$server,
  Database = db_config$database,
  Trusted_Connection = "True"
)

# Load lookup tables
t_current_region_pssm_codes <- tbl(
  con,
  in_schema(my_schema, "t_current_region_pssm_codes")
)
t_current_region_pssm_rollup_codes <- tbl(
  con,
  in_schema(my_schema, "t_current_region_pssm_rollup_codes")
)
t_current_region_pssm_rollup_codes_bc <- tbl(
  con,
  in_schema(my_schema, "t_current_region_pssm_rollup_codes_bc")
)
tbl_noc_skill_level_aged_17_34 <- tbl(
  con,
  in_schema(my_schema, "tbl_noc_skill_level_aged_17_34")
)
tbl_age_groups <- tbl(con, in_schema(my_schema, "tbl_age_groups"))

# Base cohorts table - contains all survey data with NOC codes
cohorts_base <- tbl(con, in_schema(my_schema, "T_Cohorts_Recoded"))

# =============================================================================
# SECTION 2: OCCUPATION WEIGHT CALCULATIONS (Q008 Z-Series)
# =============================================================================
#
# WHAT: Calculate Weight_OCC for each cohort record. This weight accounts for
#       survey sampling and adjusts for non-response to represent the full
#       population of graduates.
#
# WHY: Raw survey counts don't represent the full population. We need to
#       weight respondents to account for:
#       - Different survey weights by year (t_weights)
#       - Regional representation differences
#       - Non-response patterns (not all sampled students respond)
#
# HOW: Multi-step process:
#   Z01: Create base record for occupation calculations (respondents only)
#   Z02a: Aggregate base counts by key dimensions with weights
#   Z02b: Count actual respondents in each group
#   Z02c: Calculate weight probability (base / respondents)
#   Z03: Sum totals across dimensions
#   Z04: Calculate adjustment factors
#   Z05: Apply adjustment to get final Weight_OCC

# -----------------------------------------------------------------------------
# STEP Z01: Base Occupation Records
# WHAT: Filter cohorts to valid occupation records with respondents.
#
# WHY: We only want to include graduates who:
#       - Have valid NOC codes (not null, not '9999')
#       - Responded to the survey (respondent = '1')
#       - Are in the labour force (new_labour_supply = 1 or 3)
#       - Have valid age groups and graduation status
#
# HOW: Filter the cohorts table and create base counts grouped by
#       key demographic dimensions for weighting.

z01_base_occ <- cohorts_base %>%
  filter(
    weight > 0,
    current_region_pssm_code != -1,
    !is.na(age_group_rollup),
    new_labour_supply %in% c(1, 3),
    grad_status %in% c("1", "3")
  ) %>%
  select(
    survey,
    inst_cd,
    age_group_rollup,
    ttrain,
    lcip4_cred,
    stqu_id,
    new_labour_supply,
    grad_status
  ) %>%
  compute(name = "DACSO_Q008_Z01_Base_OCC", temporary = FALSE)

# -----------------------------------------------------------------------------
# STEP Z02: Weight Calculation Pipeline
# WHAT: Calculate weights accounting for sampling probability and non-response.
#
# WHY: Different demographic groups have different response rates. We need
#       to inflate weights for groups with lower response rates to ensure
#       the final distribution represents the true population.
#
# HOW:
#   Z02a: Aggregate by survey/year/institution/age/credential with weight_nls
#   Z02b: Count actual respondents (filtering for valid NOC codes)
#   Z02b_NOC_9999: Special handling for records where NOC is '9999'
#   Z02c: Calculate weight = base / respondents (inverse of response rate)

# Z02a: Base aggregation with weights
z02a_base <- cohorts_base %>%
  inner_join(
    t_current_region_pssm_codes %>%
      inner_join(
        t_current_region_pssm_rollup_codes,
        by = "current_region_pssm_code_rollup"
      ),
    by = c("current_region_pssm_code" = "current_region_pssm_code")
  ) %>%
  filter(
    new_labour_supply %in% c(1, 2, 3),
    respondent == "1",
    weight > 0,
    !is.na(age_group_rollup),
    grad_status %in% c("1", "3"),
    current_region_pssm_code_rollup != 9999
  ) %>%
  mutate(Base = weight_nls) %>%
  select(
    survey,
    current_region_pssm_code_rollup,
    survey_year,
    inst_cd,
    age_group_rollup,
    grad_status,
    ttrain,
    lcip4_cred,
    weight_nls,
    Base
  ) %>%
  compute(name = "DACSO_Q008_Z02a_Base", temporary = FALSE)

# Z02b: Count respondents with valid NOC codes (not 9999)
z02b_respondents <- cohorts_base %>%
  inner_join(
    t_current_region_pssm_codes %>%
      inner_join(
        t_current_region_pssm_rollup_codes,
        by = "current_region_pssm_code_rollup"
      ),
    by = c("current_region_pssm_code" = "current_region_pssm_code")
  ) %>%
  filter(
    !is.na(noc_cd),
    noc_cd != "9999",
    new_labour_supply %in% c(1, 3),
    weight > 0,
    !is.na(age_group_rollup),
    grad_status %in% c("1", "3")
  ) %>%
  group_by(
    survey,
    current_region_pssm_code_rollup,
    survey_year,
    inst_cd,
    age_group_rollup,
    grad_status,
    ttrain,
    lcip4_cred
  ) %>%
  summarize(
    Respondents = sum(
      respondent == "1" & current_region_pssm_code != -1,
      na.rm = TRUE
    ),
    .groups = "drop"
  ) %>%
  compute(name = "DACSO_Q008_Z02b_Respondents", temporary = FALSE)

# Z02b_NOC_9999: Count respondents where NOC = 9999 (100% invalid)
z02b_respondents_9999 <- cohorts_base %>%
  inner_join(
    t_current_region_pssm_codes %>%
      inner_join(
        t_current_region_pssm_rollup_codes,
        by = "current_region_pssm_code_rollup"
      ),
    by = c("current_region_pssm_code" = "current_region_pssm_code")
  ) %>%
  filter(
    noc_cd == "9999",
    new_labour_supply %in% c(1, 3),
    weight > 0,
    !is.na(age_group_rollup),
    grad_status %in% c("1", "3")
  ) %>%
  group_by(
    survey,
    current_region_pssm_code_rollup,
    survey_year,
    inst_cd,
    age_group_rollup,
    grad_status,
    ttrain,
    lcip4_cred
  ) %>%
  summarize(
    Respondents = sum(
      respondent == "1" & current_region_pssm_code != -1,
      na.rm = TRUE
    ),
    .groups = "drop"
  ) %>%
  compute(name = "DACSO_Q008_Z02b_Respondents_NOC_9999", temporary = FALSE)

# Z02b_9999_100_perc: Identify groups where ALL respondents have NOC = 9999
z02b_9999_100_perc <- z02a_base %>%
  inner_join(
    z02b_respondents_9999,
    by = c(
      "survey",
      "current_region_pssm_code_rollup",
      "survey_year",
      "inst_cd",
      "age_group_rollup",
      "grad_status",
      "ttrain",
      "lcip4_cred"
    )
  ) %>%
  mutate(respondent_ratio = Respondents / count) %>%
  filter(respondent_ratio == 1) %>%
  select(
    survey,
    current_region_pssm_code_rollup,
    survey_year,
    inst_cd,
    age_group_rollup,
    grad_status,
    ttrain,
    lcip4_cred,
    count,
    Base,
    Respondents
  ) %>%
  compute(
    name = "DACSO_Q008_Z02b_Respondents_NOC_9999_100_perc",
    temporary = FALSE
  )

# Z02c: Calculate weights (Base / Respondents)
# NOTE: If Respondents = 0, weight defaults to 1 (full population weight)
z02c_weight <- z02a_base %>%
  left_join(
    z02b_respondents %>%
      select(
        survey,
        current_region_pssm_code_rollup,
        survey_year,
        inst_cd,
        age_group_rollup,
        grad_status,
        ttrain,
        lcip4_cred,
        Respondents
      ),
    by = c(
      "survey",
      "current_region_pssm_code_rollup",
      "survey_year",
      "inst_cd",
      "age_group_rollup",
      "grad_status",
      "ttrain",
      "lcip4_cred"
    )
  ) %>%
  mutate(
    Weight_NLS_Base = if_else(
      is.na(Respondents) | Respondents == 0,
      1,
      as.numeric(count) / as.numeric(Respondents)
    ),
    Weighted = if_else(
      is.na(Respondents) | Respondents == 0,
      as.numeric(Base),
      as.numeric(Respondents) * Weight_NLS_Base
    )
  ) %>%
  select(
    survey,
    current_region_pssm_code_rollup,
    survey_year,
    inst_cd,
    age_group_rollup,
    grad_status,
    ttrain,
    lcip4_cred,
    weight_nls,
    Base,
    Respondents,
    Weight_NLS_Base,
    Weighted
  ) %>%
  compute(name = "DACSO_Q008_Z02c_Weight", temporary = FALSE)

# -----------------------------------------------------------------------------
# STEP Z03: Weight Totals
# WHAT: Sum weighted and base counts across dimensions.
#
# WHY: Need totals by key demographic groups to calculate adjustment factors.
#
# HOW: Group by survey and credential dimensions, sum the base and weighted counts.

z03_weight_total <- z02c_weight %>%
  group_by(
    survey,
    current_region_pssm_code_rollup,
    inst_cd,
    age_group_rollup,
    grad_status,
    ttrain,
    lcip4_cred
  ) %>%
  summarize(
    Base = sum(Base, na.rm = TRUE),
    Weighted = sum(Weighted, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  compute(name = "DACSO_Q008_Z03_Weight_Total", temporary = FALSE)

# -----------------------------------------------------------------------------
# STEP Z04: Adjustment Factor
# WHAT: Calculate adjustment factor = Base / Weighted.
#
# WHY: This factor accounts for the difference between expected respondents
#      and actual respondents in each group. Used to adjust weights.
#
# HOW: Simple calculation: if Weighted = 0, factor = 0; otherwise factor = Base/Weighted

z04_weight_adj <- z03_weight_total %>%
  mutate(
    Weight_Adj_Fac = if_else(
      Weighted == 0,
      0,
      as.numeric(Base) / as.numeric(Weighted)
    )
  ) %>%
  compute(name = "DACSO_Q008_Z04_Weight_Adj_Fac", temporary = FALSE)

# -----------------------------------------------------------------------------
# STEP Z05: Final Occupation Weight (Weight_OCC)
# WHAT: Calculate the final weight to apply to occupation counts.
#
# WHY: Weight_OCC represents the population weight for each respondent,
#      accounting for both survey sampling and non-response adjustment.
#
# HOW: Multiply Weight_NLS_Base by the adjustment factor.
#      This gives us the final weight to apply to occupation distributions.

z05_weight_occ <- z02c_weight %>%
  inner_join(
    z04_weight_adj,
    by = c(
      "survey",
      "current_region_pssm_code_rollup",
      "inst_cd",
      "age_group_rollup",
      "grad_status",
      "ttrain",
      "lcip4_cred"
    )
  ) %>%
  mutate(Weight_OCC = Weight_NLS_Base * Weight_Adj_Fac) %>%
  select(
    survey,
    current_region_pssm_code_rollup,
    survey_year,
    inst_cd,
    age_group_rollup,
    grad_status,
    ttrain,
    lcip4_cred,
    Base,
    Respondents,
    Weight_NLS_Base,
    Weight_Adj_Fac,
    Weight_OCC
  ) %>%
  compute(name = "tmp_tbl_Weights_OCC", temporary = FALSE)

# -----------------------------------------------------------------------------
# STEP Z06-Z08: Update T_Cohorts_Recoded with Weight_OCC
# WHAT: Apply the calculated weights back to the cohort records.
#
# WHY: Need Weight_OCC available on each record for occupation aggregation.
#
# HOW:
#   Z06: Add new column to table
#   Z07: Initialize to NULL
#   Z08: Update via JOIN from weight table (two versions for NOC=9999 handling)

# Add Weight_OCC column to T_Cohorts_Recoded
dbExecute(
  con,
  glue::glue(
    "ALTER TABLE {my_schema}.T_Cohorts_Recoded ADD Weight_OCC FLOAT NULL"
  )
)
dbExecute(
  con,
  glue::glue("UPDATE {my_schema}.T_Cohorts_Recoded SET Weight_OCC = NULL")
)

# Update Weight_OCC via JOIN
# This matches records by demographic dimensions and updates from weight table
update_occ_query <- glue::glue(
  "
  UPDATE tcr
  SET tcr.Weight_OCC = z05.Weight_OCC
  FROM {my_schema}.T_Cohorts_Recoded AS tcr
  INNER JOIN {my_schema}.DACSO_Q008_Z01_Base_OCC AS z01
    ON tcr.stqu_id = z01.stqu_id
  INNER JOIN {my_schema}.tmp_tbl_Weights_OCC AS z05
    ON tcr.lcip4_cred = z05.lcip4_cred
    AND tcr.grad_status = z05.grad_status
    AND tcr.age_group_rollup = z05.age_group_rollup
    AND tcr.inst_cd = z05.inst_cd
    AND tcr.survey_year = z05.survey_year
    AND tcr.survey = z05.survey
  WHERE tcr.current_region_pssm_code <> -1
    AND tcr.noc_cd IS NOT NULL
    AND tcr.noc_cd <> '9999'
"
)
dbExecute(con, update_occ_query)

# -----------------------------------------------------------------------------
# STEP Z09: Weight Validation
# WHAT: Verify that weights are being applied correctly.
#
# WHY: Quality check to ensure the weighting is working as expected.
#
# HOW: Sum weighted counts and compare to base counts.

weight_validation <- cohorts_base %>%
  inner_join(
    tbl(con, in_schema(my_schema, "DACSO_Q008_Z01_Base_OCC")),
    by = "stqu_id"
  ) %>%
  filter(
    respondent == "1",
    current_region_pssm_code != -1,
    new_labour_supply %in% c(1, 3)
  ) %>%
  group_by(
    survey_year,
    inst_cd,
    age_group_rollup,
    ttrain,
    lcip4_cred,
    weight_occ
  ) %>%
  summarize(
    Respondents = n(),
    Weighted = n() * as.numeric(weight_occ),
    Base = sum(base),
    .groups = "drop"
  ) %>%
  arrange(survey_year, weight_occ) %>%
  collect()

message("Occupation weight validation:")
print(weight_validation)

# =============================================================================
# SECTION 3: OCCUPATION DISTRIBUTIONS (Q009 Series)
# =============================================================================
#
# WHAT: Aggregate occupation counts by NOC code, weighted by occupation weight.
#
# WHY: Need to know what proportion of graduates in each credential/region/age
#      group work in each occupation (NOC code).
#
# HOW:
#   Q009: Calculate weighted counts by NOC for LCP4 level
#   Q009_2D: Aggregate to LCP2 level (2-digit CIP)
#   Q009_BC: Filter to BC regions only
#   Q009_No_TT: Remove training type dimension

# -----------------------------------------------------------------------------
# Q009: Weighted Occupation Counts by NOC (LCP4 level)
q009_weight_occs <- cohorts_base %>%
  inner_join(
    t_current_region_pssm_codes %>%
      inner_join(
        t_current_region_pssm_rollup_codes,
        by = "current_region_pssm_code_rollup"
      ),
    by = c("current_region_pssm_code" = "current_region_pssm_code")
  ) %>%
  filter(
    new_labour_supply %in% c(1, 3),
    weight > 0,
    current_region_pssm_code_rollup != 9999,
    !is.na(age_group_rollup),
    grad_status %in% c("1", "3"),
    !is.na(noc_cd),
    !is.na(weight_occ)
  ) %>%
  mutate(
    NOC_CD = if_else(noc_cd == "XXXX", "9999", noc_cd),
    Weighted = weight_occ
  ) %>%
  group_by(
    survey,
    pssm_credential,
    pssm_cred,
    current_region_pssm_code_rollup,
    survey_year,
    inst_cd,
    age_group_rollup,
    grad_status,
    lcp4_cd,
    ttrain,
    lcip4_cred,
    lcip2_cred,
    NOC_CD,
    weight_occ
  ) %>%
  summarize(
    Count = n(),
    Weighted = sum(Weighted, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  compute(name = "DACSO_Q009_Weight_Occs", temporary = FALSE)

# -----------------------------------------------------------------------------
# Q009_2D: Aggregate to LCP2 level (2-digit CIP codes)
q009_weighted_occs_2d <- q009_weight_occs %>%
  mutate(LCP2_CD = substr(lcp4_cd, 1, 2)) %>%
  group_by(
    survey,
    pssm_credential,
    pssm_cred,
    current_region_pssm_code_rollup,
    age_group_rollup,
    LCP2_CD,
    ttrain,
    lcip2_cred,
    NOC_CD
  ) %>%
  summarize(Count = sum(Weighted, na.rm = TRUE), .groups = "drop") %>%
  compute(name = "DACSO_Q009_Weighted_Occs_2D", temporary = FALSE)

# -----------------------------------------------------------------------------
# Q009_2D_BC: BC-only regional aggregation
q009_weighted_occs_2d_bc <- q009_weight_occs %>%
  inner_join(
    t_current_region_pssm_rollup_codes_bc,
    by = "current_region_pssm_code_rollup"
  ) %>%
  filter(!is.na(current_region_pssm_code_rollup_bc)) %>%
  mutate(LCP2_CD = substr(lcp4_cd, 1, 2)) %>%
  group_by(
    survey,
    pssm_credential,
    pssm_cred,
    LCP2_CD,
    ttrain,
    lcip2_cred,
    NOC_CD
  ) %>%
  summarize(Count = sum(Weighted, na.rm = TRUE), .groups = "drop") %>%
  compute(name = "DACSO_Q009_Weighted_Occs_2D_BC", temporary = FALSE)

# -----------------------------------------------------------------------------
# Q009_2D_No_TT: LCP2 level without training type
q009_weighted_occs_2d_no_tt <- q009_weight_occs %>%
  mutate(
    LCP2_CD = substr(lcp4_cd, 1, 2),
    LCIP2_CRED = case_when(
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
    LCP2_CD,
    LCIP2_CRED,
    NOC_CD
  ) %>%
  summarize(Count = sum(Weighted, na.rm = TRUE), .groups = "drop") %>%
  compute(name = "DACSO_Q009_Weighted_Occs_2D_No_TT", temporary = FALSE)

# -----------------------------------------------------------------------------
# Q009_2D_BC_No_TT: BC-only, no training type
q009_weighted_occs_2d_bc_no_tt <- q009_weight_occs %>%
  inner_join(
    t_current_region_pssm_rollup_codes_bc,
    by = "current_region_pssm_code_rollup"
  ) %>%
  filter(!is.na(current_region_pssm_code_rollup_bc)) %>%
  mutate(
    LCP2_CD = substr(lcp4_cd, 1, 2),
    LCIP2_CRED = case_when(
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
  group_by(survey, pssm_credential, pssm_cred, LCP2_CD, LCIP2_CRED, NOC_CD) %>%
  summarize(Count = sum(Weighted, na.rm = TRUE), .groups = "drop") %>%
  compute(name = "DACSO_Q009_Weighted_Occs_2D_BC_No_TT", temporary = FALSE)

# -----------------------------------------------------------------------------
# Q009b: Aggregate weighted counts (LCP4 level)
q009b_weighted_occs <- q009_weight_occs %>%
  group_by(
    survey,
    pssm_credential,
    pssm_cred,
    current_region_pssm_code_rollup,
    age_group_rollup,
    lcp4_cd,
    ttrain,
    lcip4_cred,
    lcip2_cred,
    NOC_CD
  ) %>%
  summarize(Count = sum(Weighted, na.rm = TRUE), .groups = "drop") %>%
  compute(name = "DACSO_Q009b_Weighted_Occs", temporary = FALSE)

# -----------------------------------------------------------------------------
# Q009b_No_TT: Without training type
q009b_weighted_occs_no_tt <- q009_weight_occs %>%
  mutate(
    LCIP4_CRED = if_else(
      is.na(grad_status),
      NA_character_,
      paste0(grad_status, " - ", lcp4_cd, " - ", pssm_credential)
    ),
    LCIP2_CRED = if_else(
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
    LCIP4_CRED,
    LCIP2_CRED,
    NOC_CD
  ) %>%
  summarize(Count = sum(Weighted, na.rm = TRUE), .groups = "drop") %>%
  compute(name = "dacso_q009b_weighted_occs_no_tt", temporary = FALSE)

# =============================================================================
# SECTION 4: CALCULATE TOTALS FOR PERCENTAGES (Q009 Series Continued)
# =============================================================================
#
# WHAT: Calculate totals for each demographic group to enable percentage
#       calculation (Count / Total = Percent).
#
# WHY: Need to know the total number of graduates in each group to calculate
#       what proportion work in each occupation.

# Q009_Total_2D: Totals by LCP2 group
q009_total_2d <- q009_weight_occs %>%
  mutate(LCP2_CD = substr(lcp4_cd, 1, 2)) %>%
  group_by(
    survey,
    pssm_credential,
    pssm_cred,
    current_region_pssm_code_rollup,
    age_group_rollup,
    LCP2_CD,
    ttrain,
    lcip2_cred
  ) %>%
  summarize(Total = sum(Weighted, na.rm = TRUE), .groups = "drop") %>%
  compute(name = "DACSO_Q009_Weighted_Occs_Total_2D", temporary = FALSE)

# Q009_Total_2D_BC: BC-only totals
q009_total_2d_bc <- q009_weight_occs %>%
  inner_join(
    t_current_region_pssm_rollup_codes_bc,
    by = "current_region_pssm_code_rollup"
  ) %>%
  filter(!is.na(current_region_pssm_code_rollup_bc)) %>%
  mutate(LCP2_CD = substr(lcp4_cd, 1, 2)) %>%
  group_by(survey, pssm_credential, pssm_cred, LCP2_CD, ttrain, lcip2_cred) %>%
  summarize(Total = sum(Weighted, na.rm = TRUE), .groups = "drop") %>%
  compute(name = "DACSO_Q009_Weighted_Occs_Total_2D_BC", temporary = FALSE)

# Q009_Total_2D_BC_No_TT: BC-only, no training type
q009_total_2d_bc_no_tt <- q009_weight_occs %>%
  inner_join(
    t_current_region_pssm_rollup_codes_bc,
    by = "current_region_pssm_code_rollup"
  ) %>%
  filter(!is.na(current_region_pssm_code_rollup_bc)) %>%
  mutate(
    LCP2_CD = substr(lcp4_cd, 1, 2),
    LCIP2_CRED = case_when(
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
  group_by(survey, pssm_credential, pssm_cred, LCP2_CD, LCIP2_CRED) %>%
  summarize(Total = sum(Weighted, na.rm = TRUE), .groups = "drop") %>%
  compute(
    name = "DACSO_Q009_Weighted_Occs_Total_2D_BC_No_TT",
    temporary = FALSE
  )

# Q009_Total_2D_No_TT: LCP2 totals without training type
q009_total_2d_no_tt <- q009_weight_occs %>%
  mutate(
    LCP2_CD = substr(lcp4_cd, 1, 2),
    LCIP2_CRED = case_when(
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
    LCP2_CD,
    LCIP2_CRED
  ) %>%
  summarize(Total = sum(Weighted, na.rm = TRUE), .groups = "drop") %>%
  compute(name = "DACSO_Q009_Weighted_Occs_Total_2D_No_TT", temporary = FALSE)

# Q009b_Total: Totals at LCP4 level
q009b_total <- q009_weight_occs %>%
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
  summarize(Total = sum(Weighted, na.rm = TRUE), .groups = "drop") %>%
  compute(name = "DACSO_Q009b_Weighted_Occs_Total", temporary = FALSE)

# Q009b_Total_No_TT: LCP4 totals without training type
q009b_total_no_tt <- q009_weight_occs %>%
  mutate(
    LCIP4_CRED = if_else(
      is.na(grad_status),
      NA_character_,
      paste0(grad_status, " - ", lcp4_cd, " - ", pssm_credential)
    ),
    LCIP2_CRED = if_else(
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
    LCIP4_CRED,
    LCIP2_CRED
  ) %>%
  summarize(Total = sum(Weighted, na.rm = TRUE), .groups = "drop") %>%
  compute(name = "dacso_q009b_weighted_occs_total_no_tt", temporary = FALSE)

# =============================================================================
# SECTION 5: CALCULATE PERCENTAGES AND CREATE DISTRIBUTION TABLES (Q010 Series)
# =============================================================================
#
# WHAT: Calculate occupation percentages (Count / Total) and prepare final
#       distribution tables for output.
#
# WHY: The model needs occupation distributions as percentages to project
#       future workforce by occupation.
#
# HOW: Join counts with totals, calculate percentages, append to output tables

# -----------------------------------------------------------------------------
# Helper function for percentage calculation
calculate_occ_percent <- function(
  count_data,
  total_data,
  join_cols,
  count_col = "Count"
) {
  count_data %>%
    left_join(
      total_data %>% select(all_of(c(join_cols, "Total"))),
      by = join_cols
    ) %>%
    mutate(
      Percent = if_else(
        !is.na(!!sym(count_col)) & Total > 0,
        as.numeric(!!sym(count_col)) / as.numeric(Total),
        NA_real_
      )
    )
}

# -----------------------------------------------------------------------------
# Q010_Dist: Occupation distributions at LCP4 level
q010_dist <- q009b_weighted_occs %>%
  left_join(
    q009b_total %>%
      select(
        survey,
        pssm_credential,
        pssm_cred,
        current_region_pssm_code_rollup,
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
      "current_region_pssm_code_rollup",
      "age_group_rollup",
      "lcp4_cd",
      "ttrain",
      "lcip4_cred",
      "lcip2_cred"
    )
  ) %>%
  mutate(
    Percent = if_else(
      !is.na(Count) & Total > 0,
      as.numeric(Count) / as.numeric(Total),
      NA_real_
    )
  ) %>%
  compute(name = "DACSO_Q010_Weighted_Occs_Dist", temporary = FALSE)

# -----------------------------------------------------------------------------
# Q010_Dist_No_TT: LCP4 level without training type
q010_dist_no_tt <- q009b_weighted_occs_no_tt %>%
  left_join(
    q009b_total_no_tt %>%
      select(
        survey,
        pssm_credential,
        pssm_cred,
        current_region_pssm_code_rollup,
        age_group_rollup,
        lcp4_cd,
        LCIP4_CRED,
        LCIP2_CRED,
        Total
      ),
    by = c(
      "survey",
      "pssm_credential",
      "pssm_cred",
      "current_region_pssm_code_rollup",
      "age_group_rollup",
      "lcp4_cd",
      "LCIP4_CRED",
      "LCIP2_CRED"
    )
  ) %>%
  mutate(
    Percent = if_else(
      !is.na(Count) & Total > 0,
      as.numeric(Count) / as.numeric(Total),
      NA_real_
    )
  ) %>%
  compute(name = "DACSO_Q010_Weighted_Occs_Dist_No_TT", temporary = FALSE)

# -----------------------------------------------------------------------------
# Q010_Dist_2D: LCP2 level distributions
q010_dist_2d <- q009_weighted_occs_2d %>%
  left_join(
    q009_total_2d %>%
      select(
        survey,
        pssm_credential,
        pssm_cred,
        current_region_pssm_code_rollup,
        age_group_rollup,
        LCP2_CD,
        ttrain,
        lcip2_cred,
        Total
      ),
    by = c(
      "survey",
      "pssm_credential",
      "pssm_cred",
      "current_region_pssm_code_rollup",
      "age_group_rollup",
      "LCP2_CD",
      "ttrain",
      "lcip2_cred"
    )
  ) %>%
  mutate(
    Percent = if_else(
      !is.na(Count) & Total > 0,
      as.numeric(Count) / as.numeric(Total),
      NA_real_
    )
  ) %>%
  compute(name = "DACSO_Q010_Weighted_Occs_Dist_2D", temporary = FALSE)

# -----------------------------------------------------------------------------
# Q010_Dist_2D_BC: BC-only LCP2 distributions
q010_dist_2d_bc <- q009_weighted_occs_2d_bc %>%
  left_join(
    q009_total_2d_bc %>%
      select(
        survey,
        pssm_credential,
        pssm_cred,
        LCP2_CD,
        ttrain,
        lcip2_cred,
        Total
      ),
    by = c(
      "survey",
      "pssm_credential",
      "pssm_cred",
      "LCP2_CD",
      "ttrain",
      "lcip2_cred"
    )
  ) %>%
  mutate(
    Percent = if_else(
      !is.na(Count) & Total > 0,
      as.numeric(Count) / as.numeric(Total),
      NA_real_
    )
  ) %>%
  compute(name = "DACSO_Q010_Weighted_Occs_Dist_2D_BC", temporary = FALSE)

# -----------------------------------------------------------------------------
# Q010_Dist_2D_BC_No_TT: BC-only, no training type
q010_dist_2d_bc_no_tt <- q009_weighted_occs_2d_bc_no_tt %>%
  left_join(
    q009_total_2d_bc_no_tt %>%
      select(survey, pssm_credential, pssm_cred, LCP2_CD, LCIP2_CRED, Total),
    by = c("survey", "pssm_credential", "pssm_cred", "LCP2_CD", "LCIP2_CRED")
  ) %>%
  mutate(
    Percent = if_else(
      !is.na(Count) & Total > 0,
      as.numeric(Count) / as.numeric(Total),
      NA_real_
    )
  ) %>%
  compute(name = "DACSO_Q010_Weighted_Occs_Dist_2D_BC_No_TT", temporary = FALSE)

# -----------------------------------------------------------------------------
# Q010_Dist_2D_No_TT: LCP2 without training type
q010_dist_2d_no_tt <- q009_weighted_occs_2d_no_tt %>%
  left_join(
    q009_total_2d_no_tt %>%
      select(
        survey,
        pssm_credential,
        pssm_cred,
        current_region_pssm_code_rollup,
        age_group_rollup,
        LCP2_CD,
        LCIP2_CRED,
        Total
      ),
    by = c(
      "survey",
      "pssm_credential",
      "pssm_cred",
      "current_region_pssm_code_rollup",
      "age_group_rollup",
      "LCP2_CD",
      "LCIP2_CRED"
    )
  ) %>%
  mutate(
    Percent = if_else(
      !is.na(Count) & Total > 0,
      as.numeric(Count) / as.numeric(Total),
      NA_real_
    )
  ) %>%
  compute(name = "dacso_q010_weighted_occs_dist_2d_no_tt", temporary = FALSE)

# =============================================================================
# SECTION 6: PDEG SPECIAL HANDLING (Q010d-e Series)
# =============================================================================
#
# WHAT: Handle PDEG (Post-Secondary Degree Equivalent Graduate) records,
#       particularly for the Law cluster (CIP codes starting with '07').
#
# WHY: PDEG represents a special population (non-outcomes graduates) that
#       needs to be estimated from related data. The Law cluster (07) has
#       specific handling due to data quality issues.
#
# HOW:
#   Q010d: Calculate labour supply for PDEG Law cluster from BACH data
#   Q010e: Calculate occupation distributions for PDEG Law cluster

# -----------------------------------------------------------------------------
# Q010d: PDEG Labour Supply (Law Cluster)
# WHAT: Estimate labour supply for PDEG Law (CIP 07) from BACH data (CIP 22).
#
# WHY: No direct outcomes data for PDEG Law, so we estimate from the
#      most similar credential (BACH with CIP 22).
#
# HOW: Sum labour supply counts for BACH credentials where CIP starts with 22,
#      then re-assign to PDEG/Law cluster.

pdeg_law_nls <- tbl(con, in_schema(my_schema, "Labour_Supply_Distribution")) %>%
  filter(
    Survey == "Student Outcomes",
    PSSM_Credential == "BACH",
    substr(LCP4_CD, 1, 2) == "22"
  ) %>%
  mutate(
    PSSM_Credential = "PDEG",
    PSSM_CRED = "PDEG",
    LCP4_CD = "07",
    LCIP4_CRED = "07 - PDEG"
  ) %>%
  group_by(
    Survey,
    PSSM_Credential,
    PSSM_CRED,
    LCP4_CD,
    TTRAIN,
    LCIP4_CRED,
    Current_Region_PSSM_Code_Rollup,
    Age_Group_Rollup
  ) %>%
  summarize(
    Count = sum(Count, na.rm = TRUE),
    Total = sum(Total, na.rm = TRUE),
    New_Labour_Supply = if_else(
      Total > 0,
      as.numeric(Count) / as.numeric(Total),
      NA_real_
    ),
    .groups = "drop"
  ) %>%
  collect()

# Delete existing PDEG Law records and append new estimates
dbExecute(
  con,
  glue::glue(
    "DELETE FROM {my_schema}.Labour_Supply_Distribution
   WHERE Survey = 'Student Outcomes' AND PSSM_Credential = 'PDEG' AND LCP4_CD = '07'"
  )
)

dbAppendTable(
  con,
  Id(schema = my_schema, table = "Labour_Supply_Distribution"),
  pdeg_law_nls
)

# -----------------------------------------------------------------------------
# Q010e: PDEG Occupation Distribution (Law Cluster)
# WHAT: Estimate occupation distribution for PDEG Law from BACH data.
#
# WHY: Similar to labour supply, we estimate occupations for PDEG Law
#      from the most similar available data (BACH credentials).
#
# HOW: Sum occupation counts for BACH credentials where CIP starts with 22,
#      then re-assign to PDEG/Law cluster.

pdeg_law_occ <- tbl(con, in_schema(my_schema, "Occupation_Distributions")) %>%
  filter(
    Survey == "Student Outcomes",
    PSSM_Credential == "BACH",
    substr(LCP4_CD, 1, 2) == "22"
  ) %>%
  mutate(
    PSSM_Credential = "PDEG",
    PSSM_CRED = "PDEG",
    LCP4_CD = "07",
    LCIP4_CRED = "07 - PDEG"
  ) %>%
  group_by(
    Survey,
    PSSM_Credential,
    PSSM_CRED,
    Current_Region_PSSM_Code_Rollup,
    Age_Group_Rollup,
    LCP4_CD,
    TTRAIN,
    LCIP4_CRED,
    NOC
  ) %>%
  summarize(
    Count = sum(Count, na.rm = TRUE),
    Total = sum(Total, na.rm = TRUE),
    Percent = if_else(
      Total > 0,
      as.numeric(Count) / as.numeric(Total),
      NA_real_
    ),
    .groups = "drop"
  ) %>%
  collect()

# Delete existing PDEG Law records and append new estimates
dbExecute(
  con,
  glue::glue(
    "DELETE FROM {my_schema}.Occupation_Distributions
   WHERE Survey = 'Student Outcomes' AND PSSM_Credential = 'PDEG' AND LCP4_CD = '07'"
  )
)

dbAppendTable(
  con,
  Id(schema = my_schema, table = "Occupation_Distributions"),
  pdeg_law_occ
)

# =============================================================================
# SECTION 7: APPEND TO OUTPUT TABLES
# =============================================================================
#
# WHAT: Load final distribution calculations into the permanent database tables.
#
# WHY: These tables are the final output of the occupation distribution
#      calculation and are used by downstream projection scripts.
#
# HOW: Delete existing 'Student Outcomes' records and append new calculations.

# Define table schemas
occ_def <- c(
  Survey = "nvarchar(50)",
  PSSM_Credential = "nvarchar(50)",
  PSSM_CRED = "nvarchar(50)",
  LCP4_CD = "nvarchar(50)",
  TTRAIN = "nvarchar(50)",
  LCIP4_CRED = "nvarchar(50)",
  LCIP2_CRED = "nvarchar(50)",
  NOC = "nvarchar(50)",
  Current_Region_PSSM_Code_Rollup = "integer",
  Age_Group_Rollup = "integer",
  Count = "float",
  Total = "float",
  Percent = "float"
)

occ_lcp2_def <- c(
  Survey = "nvarchar(50)",
  PSSM_Credential = "nvarchar(50)",
  PSSM_CRED = "nvarchar(50)",
  LCP2_CD = "nvarchar(50)",
  TTRAIN = "nvarchar(50)",
  LCIP2_CRED = "nvarchar(50)",
  NOC = "nvarchar(50)",
  Current_Region_PSSM_Code_Rollup = "integer",
  Age_Group_Rollup = "integer",
  Count = "float",
  Total = "float",
  Percent = "float"
)

occ_bc_def <- c(
  Survey = "nvarchar(50)",
  PSSM_Credential = "nvarchar(50)",
  PSSM_CRED = "nvarchar(50)",
  LCP2_CD = "nvarchar(50)",
  TTRAIN = "nvarchar(50)",
  LCIP2_CRED = "nvarchar(50)",
  NOC = "nvarchar(50)",
  Count = "float",
  Total = "float",
  Percent = "float"
)

# -----------------------------------------------------------------------------
# Occupation_Distributions (LCP4 level)
dbExecute(
  con,
  glue::glue(
    "DELETE FROM {my_schema}.Occupation_Distributions WHERE Survey = 'Student Outcomes'"
  )
)
dbExecute(
  con,
  glue::glue(
    "DELETE FROM {my_schema}.Occupation_Distributions_No_TT WHERE Survey = 'Student Outcomes'"
  )
)

occ_dist_data <- q010_dist %>%
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
    NOC = NOC_CD,
    Count,
    Total,
    Percent
  ) %>%
  collect()

dbAppendTable(
  con,
  Id(schema = my_schema, table = "Occupation_Distributions"),
  occ_dist_data
)

# Occupation_Distributions_No_TT
occ_dist_no_tt_data <- q010_dist_no_tt %>%
  mutate(Survey = "Student Outcomes") %>%
  select(
    Survey,
    PSSM_Credential = pssm_credential,
    PSSM_CRED = pssm_cred,
    Current_Region_PSSM_Code_Rollup = current_region_pssm_code_rollup,
    Age_Group_Rollup = age_group_rollup,
    LCP4_CD = lcp4_cd,
    LCIP4_CRED = LCIP4_CRED,
    LCIP2_CRED = LCIP2_CRED,
    NOC = NOC_CD,
    Count,
    Total,
    Percent
  ) %>%
  collect()

dbAppendTable(
  con,
  Id(schema = my_schema, table = "Occupation_Distributions_No_TT"),
  occ_dist_no_tt_data
)

# -----------------------------------------------------------------------------
# Occupation_Distributions_LCP2
dbExecute(
  con,
  glue::glue(
    "DELETE FROM {my_schema}.Occupation_Distributions_LCP2 WHERE Survey = 'Student Outcomes'"
  )
)
dbExecute(
  con,
  glue::glue(
    "DELETE FROM {my_schema}.Occupation_Distributions_LCP2_No_TT WHERE Survey = 'Student Outcomes'"
  )
)

occ_lcp2_dist_data <- q010_dist_2d %>%
  mutate(Survey = "Student Outcomes") %>%
  select(
    Survey,
    PSSM_Credential = pssm_credential,
    PSSM_CRED = pssm_cred,
    Current_Region_PSSM_Code_Rollup = current_region_pssm_code_rollup,
    Age_Group_Rollup = age_group_rollup,
    LCP2_CD = LCP2_CD,
    TTRAIN = ttrain,
    LCIP2_CRED = lcip2_cred,
    NOC = NOC_CD,
    Count,
    Total,
    Percent
  ) %>%
  collect()

dbAppendTable(
  con,
  Id(schema = my_schema, table = "Occupation_Distributions_LCP2"),
  occ_lcp2_dist_data
)

# Occupation_Distributions_LCP2_No_TT
occ_lcp2_dist_no_tt_data <- q010_dist_2d_no_tt %>%
  mutate(Survey = "Student Outcomes") %>%
  select(
    Survey,
    PSSM_Credential = pssm_credential,
    PSSM_CRED = pssm_cred,
    Current_Region_PSSM_Code_Rollup = current_region_pssm_code_rollup,
    Age_Group_Rollup = age_group_rollup,
    LCP2_CD = LCP2_CD,
    LCIP2_CRED = LCIP2_CRED,
    NOC = NOC_CD,
    Count,
    Total,
    Percent
  ) %>%
  collect()

dbAppendTable(
  con,
  Id(schema = my_schema, table = "Occupation_Distributions_LCP2_No_TT"),
  occ_lcp2_dist_no_tt_data
)

# -----------------------------------------------------------------------------
# Occupation_Distributions_LCP2_BC
dbExecute(
  con,
  glue::glue(
    "DELETE FROM {my_schema}.Occupation_Distributions_LCP2_BC WHERE Survey = 'Student Outcomes'"
  )
)
dbExecute(
  con,
  glue::glue(
    "DELETE FROM {my_schema}.Occupation_Distributions_LCP2_BC_No_TT WHERE Survey = 'Student Outcomes'"
  )
)

occ_bc_dist_data <- q010_dist_2d_bc %>%
  mutate(Survey = "Student Outcomes") %>%
  select(
    Survey,
    PSSM_Credential = pssm_credential,
    PSSM_CRED = pssm_cred,
    LCP2_CD = LCP2_CD,
    TTRAIN = ttrain,
    LCIP2_CRED = lcip2_cred,
    NOC = NOC_CD,
    Count,
    Total,
    Percent
  ) %>%
  collect()

dbAppendTable(
  con,
  Id(schema = my_schema, table = "Occupation_Distributions_LCP2_BC"),
  occ_bc_dist_data
)

# Occupation_Distributions_LCP2_BC_No_TT
occ_bc_dist_no_tt_data <- q010_dist_2d_bc_no_tt %>%
  mutate(Survey = "Student Outcomes") %>%
  select(
    Survey,
    PSSM_Credential = pssm_credential,
    PSSM_CRED = pssm_cred,
    LCP2_CD = LCP2_CD,
    LCIP2_CRED = LCIP2_CRED,
    NOC = NOC_CD,
    Count,
    Total,
    Percent
  ) %>%
  collect()

dbAppendTable(
  con,
  Id(schema = my_schema, table = "Occupation_Distributions_LCP2_BC_No_TT"),
  occ_bc_dist_no_tt_data
)

# =============================================================================
# SECTION 8: CLEANUP
# =============================================================================
#
# WHAT: Remove intermediate computed tables to free database resources.
#
# WHY: The refactored script creates many intermediate tables (z01, z02, q009, etc.)
#      that are only needed during processing. Clean them up after use.
#
# HOW: Iterate through list of known computed tables and remove if exists.

computed_cleanup <- c(
  "DACSO_Q008_Z01_Base_OCC",
  "DACSO_Q008_Z02a_Base",
  "DACSO_Q008_Z02b_Respondents",
  "DACSO_Q008_Z02b_Respondents_NOC_9999",
  "DACSO_Q008_Z02b_Respondents_NOC_9999_100_perc",
  "DACSO_Q008_Z02c_Weight",
  "DACSO_Q008_Z03_Weight_Total",
  "DACSO_Q008_Z04_Weight_Adj_Fac",
  "tmp_tbl_Weights_OCC",
  "DACSO_Q009_Weight_Occs",
  "DACSO_Q009_Weighted_Occs_2D",
  "DACSO_Q009_Weighted_Occs_2D_BC",
  "DACSO_Q009_Weighted_Occs_2D_BC_No_TT",
  "DACSO_Q009_Weighted_Occs_2D_No_TT",
  "DACSO_Q009_Weighted_Occs_Total_2D",
  "DACSO_Q009_Weighted_Occs_Total_2D_BC",
  "DACSO_Q009_Weighted_Occs_Total_2D_BC_No_TT",
  "DACSO_Q009_Weighted_Occs_Total_2D_No_TT",
  "DACSO_Q009b_Weighted_Occs",
  "dacso_q009b_weighted_occs_no_tt",
  "DACSO_Q009b_Weighted_Occs_Total",
  "dacso_q009b_weighted_occs_total_no_tt",
  "DACSO_Q010_Weighted_Occs_Dist",
  "DACSO_Q010_Weighted_Occs_Dist_No_TT",
  "DACSO_Q010_Weighted_Occs_Dist_2D",
  "DACSO_Q010_Weighted_Occs_Dist_2D_BC",
  "DACSO_Q010_Weighted_Occs_Dist_2D_BC_No_TT",
  "dacso_q010_weighted_occs_dist_2d_no_tt"
)

for (table in computed_cleanup) {
  if (dbExistsTable(con, Id(schema = my_schema, table = table))) {
    dbRemoveTable(con, Id(schema = my_schema, table = table))
  }
}

# Also clean up temp tables from 02b-2
tmp_cleanup <- c("tmp_tbl_Weights_NLS")
for (table in tmp_cleanup) {
  if (dbExistsTable(con, Id(schema = my_schema, table = table))) {
    dbRemoveTable(con, Id(schema = my_schema, table = table))
  }
}

# =============================================================================
# SECTION 9: VERIFICATION
# =============================================================================
#
# WHAT: Confirm output tables exist and contain expected data.
#
# WHY: Final quality check before completing the script.
#
# HOW: Check table existence and report counts.

message("Verifying output tables...")
message(
  "Occupation_Distributions: ",
  dbExistsTable(con, "Occupation_Distributions")
)
message(
  "Occupation_Distributions_No_TT: ",
  dbExistsTable(con, "Occupation_Distributions_No_TT")
)
message(
  "Occupation_Distributions_LCP2: ",
  dbExistsTable(con, "Occupation_Distributions_LCP2")
)
message(
  "Occupation_Distributions_LCP2_No_TT: ",
  dbExistsTable(con, "Occupation_Distributions_LCP2_No_TT")
)
message(
  "Occupation_Distributions_LCP2_BC: ",
  dbExistsTable(con, "Occupation_Distributions_LCP2_BC")
)
message(
  "Occupation_Distributions_LCP2_BC_No_TT: ",
  dbExistsTable(con, "Occupation_Distributions_LCP2_BC_No_TT")
)

dbDisconnect(con)

message("02b-3-pssm-cohorts-occupation-distributions.R refactoring complete.")
