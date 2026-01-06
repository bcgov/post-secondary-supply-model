# This script prepares student outcomes data for the following student surveys: TRD, APP, DACSO, BGS
#
# PSSM Model Context:
# This script is the core cohort building step that unifies all survey data into a common format
# for the PSSM supply model. Each survey (TRD, APPSO, BGS, DACSO) has different source data structures
# but needs to be transformed into T_Cohorts_Recoded with consistent columns.
#
# Required Source Tables (pre-requisites)
#   TRD: Q000_TRD_Graduates, T_TRD_DATA
#   APPSO: T_APPSO_DATA_Final, APPSO_Graduates
#   BGS: T_BGS_Data_Final, T_BGS_INST_Recode, T_bgs_data_final_for_outcomesmatching
#   DACSO: t_dacso_data_part_1_stepa, infoware_c_outc_clean_short_resp
#
# Required Lookup Tables
#   tbl_age, tbl_age_groups, T_Weights, t_year_survey_year
#   t_current_region_pssm_codes, t_current_region_pssm_rollup_codes
#   t_current_region_pssm_rollup_codes_bc, t_pssm_credential_grouping
#
# Resulting Tables
#   T_Cohorts_Recoded - Unified cohort table for all surveys
#   T_TRD_DATA_Updated, T_APPSO_DATA_Updated, T_BGS_DATA_Final_Updated - Intermediate updates
#
# WHAT: Transforms survey data from TRD, APPSO, BGS, and DACSO into a unified cohort format.
# WHY: The PSSM model requires consistent data structure across all data sources for supply projections.
#      Each survey has different columns and formats that need standardization.
# HOW: 1) For each survey (TRD, APPSO, BGS, DACSO):
#        - Apply year-specific weights
#        - Derive new_labour_supply flag based on employment status
#        - Join age and age group lookups
#        - Create credential grouping columns (lcip4_cred, pssm_cred, lcip2_cred)
#      2) Recode institution codes (BGS only) for consistency
#      3) Update CIP codes from program matching results
#      4) Delete existing records from T_Cohorts_Recoded for each survey
#      5) Append transformed records to T_Cohorts_Recoded
#
# TODO [HIGH]: Add validation for NOC code patterns (see documentation for invalid codes)
# TODO [MEDIUM]: Extract model year "2022-2023" to configuration variable
# TODO [LOW]: Consolidate duplicate code patterns across surveys into functions
# TODO [LOW]: Add before/after row counts for validation

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
tbl_age <- tbl(con, in_schema(my_schema, "tbl_age"))
tbl_age_groups <- tbl(con, in_schema(my_schema, "tbl_age_groups"))
t_weights <- tbl(con, in_schema(my_schema, "T_Weights"))
t_year_survey_year <- tbl(con, in_schema(my_schema, "t_year_survey_year"))
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
t_pssm_credential_grouping <- tbl(
  con,
  in_schema(my_schema, "t_pssm_credential_grouping")
)

# ---- Check for required data tables ----
required_tables <- list(
  "TRD" = c("Q000_TRD_Graduates", "T_TRD_DATA"),
  "APP" = c("T_APPSO_DATA_Final", "APPSO_Graduates"),
  "BGS" = c(
    "T_BGS_Data_Final",
    "T_BGS_INST_Recode",
    "T_bgs_data_final_for_outcomesmatching",
    "T_Weights"
  ),
  "DACSO" = c("t_dacso_data_part_1_stepa", "infoware_c_outc_clean_short_resp"),
  "Lookups" = c(
    "t_current_region_pssm_codes",
    "t_current_region_pssm_rollup_codes",
    "t_current_region_pssm_rollup_codes_bc",
    "tbl_age",
    "tbl_age_groups",
    "t_pssm_credential_grouping",
    "t_year_survey_year"
  )
)

for (survey in names(required_tables)) {
  for (table in required_tables[[survey]]) {
    exists <- dbExistsTable(con, Id(schema = my_schema, table = table))
    if (!exists) {
      warning(glue::glue("Table '{table}' not found for {survey}"))
    }
  }
}

# =============================================================================
# TRD Processing
# =============================================================================
#
# WHAT: Transforms TRD (Trades) survey data into cohort format.
# WHY: TRD data requires age lookup, weight application, and labour supply derivation.
# HOW: 1) Join weights filtered by model and survey
#      2) Join age and age_group lookups
#      3) Derive new_labour_supply from employment status columns
#      4) Select final columns and materialize
#      5) Delete existing TRD records and append transformed records
#
# Key Transformations:
#   - new_labour_supply: 1 if employed OR in labour market but not employed, 0 otherwise
#   - ttrain: converts 2 to 1 for cohort records
#   - noc_cd: converts "XXXX" to "9999"
#   - stqu_id: creates unique identifier "TRD - {KEY}"

## ---- TRD: Apply weights, derive new labour supply, add age groups ----
# Refactored from: Q000_TRD_Q003c_Derived_And_Weights
trd_data_updated <- tbl(con, in_schema(my_schema, "T_TRD_DATA")) %>%
  # Join weights (model year and survey specific)
  left_join(
    t_weights %>%
      filter(model == "2022-2023", survey == "TRD"),
    by = "subm_cd"
  ) %>%
  # Join age lookup
  left_join(
    tbl_age,
    by = c("TRD_AGE_AT_SURVEY" = "age")
  ) %>%
  # Join age groups
  left_join(
    tbl_age_groups,
    by = "age_group"
  ) %>%
  mutate(
    # Derive new_labour_supply (from Q000_TRD_Q003c_Derived_And_Weights)
    new_labour_supply = case_when(
      TRD_LABR_EMPLOYED == 1 ~ 1,
      TRD_LABR_IN_LABOUR_MARKET == 1 & TRD_LABR_EMPLOYED == 0 ~ 1,
      TRD_LABR_EMPLOYED == 0 ~ 0,
      RESPONDENT == "1" ~ 0,
      TRUE ~ 0
    ),
    # Apply weight
    weight = weight,
    # Add age group columns inline (replaces ALTER TABLE ADD)
    age_group = age_group,
    age_group_rollup = age_group_rollup
  ) %>%
  select(
    pen,
    KEY,
    subm_cd,
    inst,
    lcip_cd,
    lcip_lcp4_cd,
    ttrain,
    noc_cd,
    TRD_AGE_AT_SURVEY,
    gradstat_group,
    respondent,
    new_labour_supply,
    weight,
    age_group,
    age_group_rollup,
    pssm_credential,
    current_region_pssm_code
  ) %>%
  compute(name = "T_TRD_DATA_Updated", temporary = FALSE)

## ---- TRD: Refresh records in T_Cohorts_Recoded ----
# Refactored from: Q000_TRD_Q005_1b1_Delete_Cohort + Q000_TRD_Q005_DACSO_DATA_Part_1b2_Cohort_Recoded

# First, remove existing TRD records from T_Cohorts_Recoded
dbExecute(
  con,
  glue::glue("DELETE FROM {my_schema}.T_Cohorts_Recoded WHERE Survey = 'TRD'")
)

# Create TRD cohort records using dplyr
trd_cohort_records <- trd_data_updated %>%
  left_join(
    t_year_survey_year,
    by = "subm_cd"
  ) %>%
  mutate(
    stqu_id = glue::glue("TRD - {KEY}"),
    survey = "TRD",
    survey_year = survey_year,
    inst_cd = inst,
    lcp4_cd = lcip_lcp4_cd,
    ttrain = if_else(ttrain == 2, 1, ttrain),
    noc_cd = if_else(noc_cd == "XXXX", "9999", noc_cd),
    age_at_survey = TRD_AGE_AT_SURVEY,
    grad_status = gradstat_group,
    respondent = respondent,
    pssm_cred = glue::glue("{gradstat_group} - {pssm_credential}"),
    lcip4_cred = glue::glue(
      "{gradstat_group} - {lcip_lcp4_cd} - {if_else(ttrain == 2, '1', as.character(ttrain))} - {pssm_credential}"
    ),
    lcip2_cred = glue::glue(
      "{gradstat_group} - {substr(lcip_lcp4_cd, 1, 2)} - {if_else(ttrain == 2, '1', as.character(ttrain))} - {pssm_credential}"
    )
  ) %>%
  select(
    pen,
    stqu_id,
    survey,
    survey_year,
    inst_cd,
    lcip_cd,
    lcp4_cd,
    ttrain,
    noc_cd,
    age_at_survey,
    age_group,
    age_group_rollup,
    grad_status,
    respondent,
    new_labour_supply,
    weight,
    pssm_credential,
    pssm_cred,
    lcip4_cred,
    lcip2_cred,
    current_region_pssm_code
  )

# Append to T_Cohorts_Recoded
dbAppendTable(
  con,
  Id(schema = my_schema, table = "T_Cohorts_Recoded"),
  trd_cohort_records
)

# =============================================================================
# APPSO Processing
# =============================================================================
#
# WHAT: Transforms APPSO (Apprenticeship) survey data into cohort format.
# WHY: APPSO has similar structure to TRD but lacks ttrain column and has different labour status columns.
# HOW: 1) Join weights filtered by model and survey
#      2) Join age and age_group lookups
#      3) Derive new_labour_supply from employment status columns
#      4) Create credential grouping columns
#      5) Delete existing APPSO records and append transformed records
#
# Key Differences from TRD:
#   - ttrain set to NA (not tracked in APPSO)
#   - grad_status hardcoded to "1" (all APPSO are graduates)
#   - lcip2_cred derived from lcip_lcp4_cd directly

## ---- APPSO: Apply weights, derive new labour supply, add age groups ----
# Refactored from: APPSO_Q003c_Derived_And_Weights
appso_data_updated <- tbl(con, in_schema(my_schema, "T_APPSO_DATA_Final")) %>%
  left_join(
    t_weights %>%
      filter(model == "2022-2023", survey == "APPSO"),
    by = "subm_cd"
  ) %>%
  left_join(
    tbl_age,
    by = c("APP_AGE_AT_SURVEY" = "age")
  ) %>%
  left_join(
    tbl_age_groups,
    by = "age_group"
  ) %>%
  mutate(
    new_labour_supply = case_when(
      APP_LABR_EMPLOYED == 1 ~ 1,
      APP_LABR_IN_LABOUR_MARKET == 1 & APP_LABR_EMPLOYED == 0 ~ 1,
      APP_LABR_EMPLOYED == 0 ~ 0,
      RESPONDENT == "1" ~ 0,
      TRUE ~ 0
    ),
    weight = weight,
    age_group = age_group,
    age_group_rollup = age_group_rollup
  ) %>%
  select(
    pen,
    KEY,
    subm_cd,
    inst,
    lcip_cd,
    lcip_lcp4_cd,
    noc_cd,
    respondent,
    APP_AGE_AT_SURVEY,
    pssm_credential,
    lcip4_cred,
    new_labour_supply,
    weight,
    age_group,
    age_group_rollup,
    current_region_pssm_code
  ) %>%
  compute(name = "T_APPSO_DATA_Updated", temporary = FALSE)

## ---- APPSO: Refresh records in T_Cohorts_Recoded ----
# Refactored from: APPSO_Q005_1b1_Delete_Cohort + APPSO_Q005_DACSO_DATA_Part_1b2_Cohort_Recoded
dbExecute(
  con,
  glue::glue("DELETE FROM {my_schema}.T_Cohorts_Recoded WHERE Survey = 'APPSO'")
)

appso_cohort_records <- appso_data_updated %>%
  left_join(
    t_year_survey_year,
    by = "subm_cd"
  ) %>%
  mutate(
    stqu_id = glue::glue("APPSO - {KEY}"),
    survey = survey,
    survey_year = survey_year,
    lcp4_cd = lcip_lcp4_cd,
    ttrain = NA_integer_, # APPSO doesn't have ttrain column
    noc_cd = if_else(noc_cd == "xxxx", "9999", noc_cd),
    age_at_survey = APP_AGE_AT_SURVEY,
    grad_status = "1",
    pssm_cred = pssm_credential,
    lcip2_cred = glue::glue("{substr(lcip_lcp4_cd, 1, 2)} - {pssm_credential}")
  ) %>%
  select(
    pen,
    stqu_id,
    survey,
    survey_year,
    inst_cd,
    lcip_cd,
    lcp4_cd,
    ttrain,
    noc_cd,
    age_at_survey,
    age_group,
    age_group_rollup,
    grad_status,
    respondent,
    new_labour_supply,
    weight,
    pssm_credential,
    pssm_cred,
    lcip4_cred,
    lcip2_cred,
    current_region_pssm_code
  )

dbAppendTable(
  con,
  Id(schema = my_schema, table = "T_Cohorts_Recoded"),
  appso_cohort_records
)

# =============================================================================
# BGS Processing
# =============================================================================
#
# WHAT: Transforms BGS (Baccalaureate Graduate Survey) data into cohort format.
# WHY: BGS requires institution recoding, CIP updates from matching, and has different labour status logic.
# HOW: 1) Recode institution codes using T_BGS_INST_Recode lookup
#      2) Update CIP codes from T_bgs_data_final_for_outcomesmatching
#      3) Apply weights and derive new_labour_supply
#      4) Join age and age_group lookups
#      5) Delete existing BGS records and append transformed records
#
# Key Transformations:
#   - Institution recode: handles institution code variations
#   - CIP update: applies matched CIPs from program matching workflow
#   - new_labour_supply: complex logic based on CURRENT_ACTIVITY, FULL_TM_WRK, IN_LBR_FRC
#   - lcip4_cred: formatted as "{cip_code_4} - BACH"

## ---- BGS: Recode institution codes ----
# Refactored from: BGS_Q001b_INST_Recode
t_bgs_inst_recode <- tbl(con, in_schema(my_schema, "T_BGS_INST_Recode"))

bgs_data_with_inst_recode <- tbl(
  con,
  in_schema(my_schema, "T_BGS_Data_Final")
) %>%
  left_join(
    t_bgs_inst_recode,
    by = c("inst" = "inst")
  ) %>%
  mutate(
    inst = coalesce(inst_recode, inst)
  ) %>%
  select(-inst_recode) %>%
  compute(name = "T_BGS_Data_Inst_Recoded", temporary = FALSE)

## ---- BGS: Update CIPS after program matching ----
# Refactored from: BGS_Q001c_Update_CIPs_After_Program_Matching
t_bgs_outcomes_matching <- tbl(
  con,
  in_schema(my_schema, "T_bgs_data_final_for_outcomesmatching")
)

bgs_data_with_cips <- bgs_data_with_inst_recode %>%
  left_join(
    t_bgs_outcomes_macking %>%
      select(
        stqu_id,
        final_cip_code_4,
        final_cip_code_2,
        final_cip_cluster_code
      ),
    by = "stqu_id"
  ) %>%
  mutate(
    cip_code_4 = coalesce(final_cip_code_4, cip_code_4),
    cip_code_2 = coalesce(final_cip_code_2, cip_code_2),
    lcip_lcippc_cd = coalesce(final_cip_cluster_code, lcip_lcippc_cd)
  ) %>%
  select(-final_cip_code_4, -final_cip_code_2, -final_cip_cluster_code) %>%
  compute(name = "T_BGS_Data_CIPs_Updated", temporary = FALSE)

## ---- BGS: Update LCIP4_CRED and PSSM_Credential ----
# Refactored from: BGS_Q002_LCP4_CRED
bgs_data_final <- bgs_data_with_cips %>%
  mutate(
    lcip4_cred = glue::glue("{cip_code_4} - BACH"),
    pssm_credential = "BACH"
  )

## ---- BGS: Apply weights, derive new labour supply, add age groups ----
# Refactored from: BGS_Q003c_Derived_And_Weights
bgs_data_updated <- bgs_data_final %>%
  left_join(
    t_weights %>%
      filter(model == "2022-2023", survey == "BGS"),
    by = c("survey_year" = "survey_year")
  ) %>%
  left_join(
    tbl_age,
    by = c("age" = "age")
  ) %>%
  left_join(
    tbl_age_groups,
    by = "age_group"
  ) %>%
  mutate(
    BGS_New_Labour_Supply = case_when(
      CURRENT_ACTIVITY == 1 ~ 1,
      CURRENT_ACTIVITY == 4 & FULL_TM_WRK == 1 ~ 1,
      CURRENT_ACTIVITY == 4 & FULL_TM_WRK == 0 ~ 2,
      CURRENT_ACTIVITY == 3 & IN_LBR_FRC == 1 ~ 1,
      is.na(CURRENT_ACTIVITY) & is.na(FULL_TM_WRK) & IN_LBR_FRC == 1 ~ 1,
      is.na(CURRENT_ACTIVITY) & IN_LBR_FRC == 1 ~ 1,
      srv_y_n == 0 ~ 0,
      TRUE ~ 0
    ),
    weight = weight,
    age_group = age_group,
    age_group_rollup = age_group_rollup
  ) %>%
  select(
    pen,
    stqu_id,
    survey_year,
    inst,
    cip_code_4,
    noc,
    age,
    bgs_new_labour_supply,
    old_labour_supply,
    weight,
    pssm_credential,
    lcip4_cred,
    age_group,
    age_group_rollup,
    current_region_pssm_code
  ) %>%
  compute(name = "T_BGS_DATA_Final_Updated", temporary = FALSE)

## ---- BGS: Refresh records in T_Cohorts_Recoded ----
# Refactored from: BGS_Q005_1b1_Delete_Cohort + BGS_Q005_1b2_Cohort_Recoded
dbExecute(
  con,
  glue::glue("DELETE FROM {my_schema}.T_Cohorts_Recoded WHERE Survey = 'BGS'")
)

bgs_cohort_records <- bgs_data_updated %>%
  mutate(
    stqu_id = glue::glue("BGS - {as.integer(stqu_id)}"),
    survey = "BGS",
    lcp4_cd = cip_code_4,
    noc_cd = if_else(noc == "XXXX", "9999", noc),
    age_at_survey = age,
    grad_status = "1",
    new_labour_supply = bgs_new_labour_supply,
    lcip_cd = NA_character_, # BGS doesn't have lcip_cd
    ttrain = NA_integer_, # BGS doesn't have ttrain
    pssm_cred = pssm_credential,
    lcip2_cred = glue::glue("{substr(cip_code_4, 1, 2)} - BACH")
  ) %>%
  select(
    pen,
    stqu_id,
    survey,
    survey_year,
    inst_cd,
    lcip_cd,
    lcp4_cd,
    ttrain,
    noc_cd,
    age_at_survey,
    age_group,
    age_group_rollup,
    grad_status,
    respondent,
    new_labour_supply,
    old_labour_supply,
    weight,
    pssm_credential,
    pssm_cred,
    lcip4_cred,
    lcip2_cred,
    current_region_pssm_code
  )

dbAppendTable(
  con,
  Id(schema = my_schema, table = "T_Cohorts_Recoded"),
  bgs_cohort_records
)

# =============================================================================
# DACSO Processing
# =============================================================================
#
# WHAT: Transforms DACSO (Diploma/Certificate Outcomes Survey) data into cohort format.
# WHY: DACSO has the most complex processing including credential grouping, further education tracking,
#      and invalid credential filtering.
# HOW: 1) Join credential grouping to map program credentials to PSSM categories
#      2) Add age and age group
#      3) Update further education fields from infoware_c_outc
#      4. Delete invalid credentials (those marked NA in dacso_include_in_model)
#      5) Apply weights and derive new_labour_supply
#      6) Delete existing DACSO records and append transformed records
#
# Key Transformations:
#   - LCIP4_CRED: formatted credential combining grad status, CIP, ttrain, and PSSM credential
#   - new_labour_supply: based on PFST_CURRENT_ACTIVITY and LABR_EMPLOYED_FULL_PART_TIME
#   - Invalid credential removal: filters out credentials not included in model
#
# TODO [MEDIUM]: Document the 2006 DACSO NULL lcip-4-creds issue mentioned in original comments

## ---- DACSO: Add age, update credential, create new LCIP4_CRED variable ----
# Refactored from: DACSO_Q003_DACSO_Data_Part_1_stepB (SELECT INTO -> compute)
dacso_data_part_1 <- tbl(
  con,
  in_schema(my_schema, "t_dacso_data_part_1_stepa")
) %>%
  inner_join(
    t_pssm_credential_grouping,
    by = c("prgm_credential" = "prgm_credential_awarded")
  ) %>%
  left_join(
    tbl_age,
    by = c("coci_age_at_survey" = "age")
  ) %>%
  left_join(
    tbl_age_groups,
    by = "age_group"
  ) %>%
  mutate(
    LCIP4_CRED = glue::glue(
      "{cosc_grad_status_lgds_cd_group} - {lcp4_cd} - {if_else(ttrain == 2, '1', as.character(ttrain))} - {pssm_credential}"
    )
  ) %>%
  select(
    coci_pen,
    coci_stqu_id,
    coci_subm_cd,
    coci_lrst_cd,
    coci_inst_cd,
    pfst_current_activity,
    lcip_lcippc_name,
    lcip_cd,
    lcp4_cd,
    current_region_pssm_code,
    lcp4_cip_4digits_name,
    ttrain,
    tpid_lgnd_cd,
    labr_in_labour_market,
    labr_employed,
    labr_unemployed,
    labr_employed_full_part_time,
    labr_job_search_time_gp,
    labr_job_training_related,
    labr_occupation_lnoc_cd,
    coci_age_at_survey,
    age_group,
    age_group_rollup,
    cosc_grad_status_lgds_cd_group,
    respondent,
    new_labour_supply,
    old_labour_supply,
    weight,
    had_previous_credential,
    pfst_in_post_sec_before,
    pfst_had_previous_cdtl,
    pfst_furstdy_incl_still_attd,
    prgm_credential_awarded,
    pssm_credential,
    pssm_credential_name,
    LCIP4_CRED
  ) %>%
  compute(name = "t_dacso_data_part_1", temporary = FALSE)

## ---- DACSO: Update further education fields ----
# Refactored from: DACSO_Q003b_DACSO_DATA_Part_1_Further_Ed
infoware_c_outc <- tbl(
  con,
  in_schema(my_schema, "infoware_c_outc_clean_short_resp")
)

dacso_data_further_ed <- dacso_data_part_1 %>%
  left_join(
    infoware_c_outc %>%
      select(
        stqu_id,
        q08,
        pfst_had_previous_cdtl,
        pfst_furstdy_incl_still_attd
      ),
    by = c("coci_stqu_id" = "stqu_id")
  ) %>%
  mutate(
    had_previous_credential = if_else(q08 == "1", pfst_had_previous_cdtl, q08),
    pfst_in_post_sec_before = q08,
    pfst_had_previous_cdtl = pfst_had_previous_cdtl,
    pfst_furstdy_incl_still_attd = pfst_furstdy_incl_still_attd
  ) %>%
  select(-q08) %>%
  compute(name = "t_dacso_data_part_1_FurtherEd", temporary = FALSE)

## ---- DACSO: Delete invalid credentials ----
# Refactored from: DACSO_Q004_DACSO_DATA_Part_1_Delete_Credentials
dacso_data_valid <- dacso_data_further_ed %>%
  anti_join(
    t_pssm_credential_grouping %>%
      filter(is.na(dacso_include_in_model)) %>%
      select(prgm_credential_awarded),
    by = c("prgm_credential_awarded" = "prgm_credential_awarded")
  ) %>%
  compute(name = "t_dacso_data_part_1_Valid", temporary = FALSE)

## ---- DACSO: Apply weights and derive new labour supply ----
# Refactored from: DACSO_Q005_DACSO_DATA_Part_1a_Derived
dacso_data_weighted <- dacso_data_valid %>%
  left_join(
    t_weights %>%
      filter(model == "2022-2023", survey == "DACSO"),
    by = c("coci_subm_cd" = "subm_cd")
  ) %>%
  mutate(
    new_labour_supply = case_when(
      PFST_CURRENT_ACTIVITY == 3 ~ 1,
      PFST_CURRENT_ACTIVITY == 2 & LABR_EMPLOYED_FULL_PART_TIME == 1 ~ 1,
      PFST_CURRENT_ACTIVITY == 2 & LABR_EMPLOYED_FULL_PART_TIME == 0 ~ 2,
      PFST_CURRENT_ACTIVITY == 4 & LABR_IN_LABOUR_MARKET == 1 ~ 1,
      RESPONDENT == "1" ~ 0,
      TRUE ~ 0
    ),
    weight = weight
  ) %>%
  select(
    coci_pen,
    coci_stqu_id,
    coci_subm_cd,
    coci_lrst_cd,
    coci_inst_cd,
    pfst_current_activity,
    lcip_lcippc_name,
    lcip_cd,
    lcp4_cd,
    current_region_pssm_code,
    lcp4_cip_4digits_name,
    ttrain,
    tpid_lgnd_cd,
    labr_in_labour_market,
    labr_employed,
    labr_unemployed,
    labr_employed_full_part_time,
    labr_job_search_time_gp,
    labr_job_training_related,
    labr_occupation_lnoc_cd,
    coci_age_at_survey,
    age_group,
    age_group_rollup,
    cosc_grad_status_lgds_cd_group,
    respondent,
    new_labour_supply,
    old_labour_supply,
    weight,
    had_previous_credential,
    pfst_in_post_sec_before,
    pfst_had_previous_cdtl,
    pfst_furstdy_incl_still_attd,
    prgm_credential_awarded,
    pssm_credential,
    pssm_credential_name,
    LCIP4_CRED
  ) %>%
  compute(name = "t_dacso_data_part_1_Weighted", temporary = FALSE)

## ---- DACSO: Refresh records in T_Cohorts_Recoded ----
# Refactored from: DACSO_Q005_DACSO_DATA_Part_1b1_Delete_Cohort + DACSO_Q005_DACSO_DATA_Part_1b2_Cohort_Recoded
dbExecute(
  con,
  glue::glue("DELETE FROM {my_schema}.T_Cohorts_Recoded WHERE Survey = 'DACSO'")
)

dacso_cohort_records <- dacso_data_weighted %>%
  left_join(
    t_year_survey_year,
    by = c("coci_subm_cd" = "subm_cd")
  ) %>%
  mutate(
    stqu_id = glue::glue("DACSO - {coci_stqu_id}"),
    survey = survey,
    survey_year = survey_year,
    inst_cd = coci_inst_cd,
    ttrain = if_else(ttrain == 2, 1, ttrain),
    noc_cd = if_else(
      labr_occupation_lnoc_cd == "XXXX",
      "9999",
      labr_occupation_lnoc_cd
    ),
    age_at_survey = coci_age_at_survey,
    grad_status = cosc_grad_status_lgds_cd_group,
    pssm_cred = glue::glue(
      "{cosc_grad_status_lgds_cd_group} - {pssm_credential}"
    ),
    lcip4_cred = LCIP4_CRED,
    lcip2_cred = glue::glue(
      "{cosc_grad_status_lgds_cd_group} - {substr(lcp4_cd, 1, 2)} - {if_else(ttrain == 2, '1', as.character(ttrain))} - {pssm_credential}"
    )
  ) %>%
  select(
    pen = coci_pen,
    stqu_id,
    survey,
    survey_year,
    inst_cd,
    lcip_cd,
    lcp4_cd,
    ttrain,
    noc_cd,
    age_at_survey,
    age_group,
    age_group_rollup,
    grad_status,
    respondent,
    new_labour_supply,
    old_labour_supply,
    weight,
    pssm_credential,
    pssm_cred,
    lcip4_cred,
    lcip2_cred,
    current_region_pssm_code
  )

dbAppendTable(
  con,
  Id(schema = my_schema, table = "T_Cohorts_Recoded"),
  dacso_cohort_records
)

# =============================================================================
# Validation and Cleanup
# =============================================================================
#
# WHAT: Verifies output tables exist and cleans up intermediate tables.
# WHY: Validation ensures the pipeline completed successfully. Cleanup removes temporary tables.
# HOW: 1) Check existence of key output tables
#      2) Remove source/intermediate tables that are no longer needed
#      3) Remove computed temporary tables from the workflow

# ---- Verify final tables ----
message("Verifying output tables...")
dbExistsTable(con, "APPSO_Graduates")
dbExistsTable(con, "t_dacso_data_part_1")
dbExistsTable(con, "T_Cohorts_Recoded")

# ---- Clean up intermediate tables ----
# Note: In production, you may want to keep some of these for debugging
cleanup_tables <- c(
  "T_TRD_DATA",
  "T_APPSO_DATA_Final",
  "T_BGS_Data_Final",
  "t_dacso_data_part_1_stepa",
  "T_BGS_INST_Recode",
  "Q000_TRD_Graduates",
  "tbl_Age_Groups",
  "tbl_Age",
  "T_PSSM_Credential_Grouping",
  "t_year_survey_year",
  "t_current_region_pssm_codes",
  "t_current_region_pssm_rollup_codes",
  "t_current_region_pssm_rollup_codes_bc"
)

for (table in cleanup_tables) {
  if (dbExistsTable(con, Id(schema = my_schema, table = table))) {
    dbRemoveTable(con, Id(schema = my_schema, table = table))
  }
}

# Remove computed temp tables
computed_cleanup <- c(
  "T_TRD_DATA_Updated",
  "T_APPSO_DATA_Updated",
  "T_BGS_Data_Inst_Recoded",
  "T_BGS_Data_CIPs_Updated",
  "T_BGS_DATA_Final_Updated",
  "t_dacso_data_part_1",
  "t_dacso_data_part_1_FurtherEd",
  "t_dacso_data_part_1_Valid",
  "t_dacso_data_part_1_Weighted"
)

for (table in computed_cleanup) {
  if (dbExistsTable(con, Id(schema = my_schema, table = table))) {
    dbRemoveTable(con, Id(schema = my_schema, table = table))
  }
}

dbDisconnect(con)

message("02b-1-pssm-cohorts.R refactoring complete.")
