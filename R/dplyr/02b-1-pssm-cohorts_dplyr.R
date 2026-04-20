# PSSM Cohorts — dplyr Translation
# Original: R/02b-1-pssm-cohorts.R
#
# Pipeline context:
#   Processes 4 student outcome surveys (TRD, APPSO, BGS, DACSO) into a unified
#   T_Cohorts_Recoded table used by all downstream model steps (02b-2 through 08).
#   Each survey requires different preprocessing:
#     TRD:   Apply weights, derive labour supply, add age groups
#     APPSO: Data is already preprocessed; just reshape for T_Cohorts_Recoded
#     BGS:   Recode institutions, update CIPs, apply weights, derive labour supply
#     DACSO: Join with credential grouping, update fields, filter, apply weights
#
#   All surveys share a common output schema (T_Cohorts_Recoded) with columns:
#     pen, stqu_id, survey, survey_year, inst_cd, lcp4_cd, ttrain, noc_cd,
#     age_at_survey, age_group, age_group_rollup, grad_status, respondent,
#     new_labour_supply, old_labour_supply, weight, pssm_credential, pssm_cred,
#     lcip4_cred, lcip2_cred, current_region_pssm_code
#
# Input tables:
#   - TRD: t_trd_data, t_weights, t_year_survey_year, tbl_age, tbl_age_groups
#   - APPSO: t_appso_data_final, t_year_survey_year, tbl_age, tbl_age_groups
#   - BGS: t_bgs_data_final, t_bgs_inst_recode, t_bgs_data_final_for_outcomesmatching,
#           t_weights, tbl_age, tbl_age_groups
#   - DACSO: t_dacso_data_part_1_stepa, t_pssm_credential_grouping,
#            infoware_c_outc_clean_short_resp, t_weights, t_year_survey_year,
#            tbl_age, tbl_age_groups
#
# Output:
#   - T_Cohorts_Recoded — unified cohort table (replaces old survey records)
#   - t_dacso_data_part_1 — intermediate DACSO table (kept for downstream use)

library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(RJDBC)
library(dbplyr)
library(glue)
library(assertthat)

# ---- Configure LAN and file paths ----
db_config <- config::get("decimal")
lan <- config::get("lan")
my_schema <- config::get("myschema")

# ---- Connection to decimal ----
decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")

# Helper: reference a table in the user's schema
sch_tbl <- function(name) {
  tbl(decimal_con, dbplyr::in_schema(my_schema, name))
}

# List of required tables with categories
required_tables <- list(
  TRD = c("TRD_Graduates", "T_TRD_DATA"),
  APP = c("T_APPSO_DATA_Final", "APPSO_Graduates"),
  BGS = c("T_BGS_Data_Final", "T_BGS_INST_Recode", "T_bgs_data_final_for_outcomesmatching", "T_Weights"),
  DACSO = c("t_dacso_data_part_1_stepa", "infoware_c_outc_clean_short_resp"),
  Lookups = c("t_current_region_pssm_codes", "t_current_region_pssm_rollup_codes",
              "t_current_region_pssm_rollup_codes_bc", "tbl_age", "tbl_age_groups",
              "t_pssm_credential_grouping", "t_year_survey_year")
)

# Check for required data tables in the database
for (category in names(required_tables)) {
  for (table_name in required_tables[[category]]) {
    full_table_name <- SQL(glue::glue('"{my_schema}"."{table_name}"'))
    assert_that(
      dbExistsTable(decimal_con, full_table_name),
      msg = paste("Error:", table_name, "does not exist in schema", my_schema)
    )
  }
}


# ---- Pull all shared lookup tables ----
# These lookups are used by multiple surveys, so we pull them once at the start.
# After collect(), all column names are uppercased for consistency with SQL Server.

tbl_age <- sch_tbl("tbl_age") %>%
  select(AGE, AGE_GROUP) %>%
  collect() |> rename_with(toupper)

tbl_age_groups <- sch_tbl("tbl_age_groups") %>%
  select(AGE_GROUP, AGE_GROUP_ROLLUP) %>%
  collect() |> rename_with(toupper)

year_survey <- sch_tbl("t_year_survey_year") %>%
  collect() |> rename_with(toupper)

t_weights <- sch_tbl("T_Weights") %>%
  collect() |> rename_with(toupper)

pssm_cred_grouping <- sch_tbl("t_pssm_credential_grouping") %>%
  collect() |> rename_with(toupper)

# Determine weight column based on run type.
# WHY: Regular/PTIB runs use the standard Weight, QI runs use Weight_QI.
# Each model run type sets different flags (only one of regular_run, qi_run, ptib_run is TRUE).
if (regular_run | ptib_run) {
  weight_var <- "WEIGHT"
} else if (qi_run) {
  weight_var <- "WEIGHT_QI"
}


# ******************************************************************************
# ---- TRD Queries ----
# Applies weight for model year and derives New Labour Supply
# ******************************************************************************

# Pull TRD data with only the columns needed for cohort building.
trd_data <- sch_tbl("t_trd_data") %>%
  select(PEN, KEY, SUBM_CD, INST, LCIP_CD, LCIP_LCP4_CD, TTRAIN, NOC_CD,
         TRD_AGE_AT_SURVEY, GRADSTAT_GROUP, RESPONDENT,
         TRD_LABR_EMPLOYED, TRD_LABR_IN_LABOUR_MARKET,
         PSSM_CREDENTIAL, CURRENT_REGION_PSSM_CODE) %>%
  collect() |> rename_with(toupper)

# Compute new_labour_supply from labour market response codes.
# WHY: The labour supply indicator determines whether a graduate is counted as
# contributing to the labour supply (1 = yes, 0 = no). Different surveys use
# different questions, so the logic varies by survey type.
# NOTE: In the original, this was an UPDATE...FROM SQL. Here we compute in R.
trd_data <- trd_data %>%
  mutate(NEW_LABOUR_SUPPLY = case_when(
    TRD_LABR_EMPLOYED == 1 ~ 1,
    TRD_LABR_IN_LABOUR_MARKET == 1 & TRD_LABR_EMPLOYED == 0 ~ 1,
    TRD_LABR_EMPLOYED == 0 ~ 0,
    RESPONDENT == "1" ~ 0,
    TRUE ~ 0
  ))

# Join with weights table (inner join — only matching rows get a weight value).
# The weight adjusts for survey non-response and ensures the sample is representative.
trd_weights <- t_weights %>%
  filter(MODEL == "2022-2023", SURVEY == "TRD") %>%
  select(SUBM_CD, all_of(weight_var)) %>%
  rename(WEIGHT = !!sym(weight_var))

trd_data <- trd_data %>%
  inner_join(trd_weights, by = "SUBM_CD")

# Build TRD cohort records for T_Cohorts_Recoded.
# WHY: This transforms raw TRD survey data into the unified cohort schema used by
# all downstream model steps. The CASE WHEN / string concatenation logic for composite
# keys (PSSM_CRED, LCIP4_CRED, LCIP2_CRED) is more readable in dplyr than in SQL.
# NOTE: ttrain = 2 is recoded to 1 for consistency with other surveys.
trd_cohort <- trd_data %>%
  inner_join(year_survey %>% filter(SURVEY == "TRD"), by = "SUBM_CD") %>%
  left_join(tbl_age, by = c("TRD_AGE_AT_SURVEY" = "AGE")) %>%
  left_join(tbl_age_groups, by = "AGE_GROUP") %>%
  mutate(
    TTRAIN_ADJ = if_else(TTRAIN == 2, 1, TTRAIN),
    TTRAIN_STR = if_else(TTRAIN == 2, "1", as.character(TTRAIN)),
    NOC_CD = if_else(NOC_CD == "XXXXX", "99999", NOC_CD)
  ) %>%
  transmute(
    PEN = PEN,
    STQU_ID = paste0("TRD - ", KEY),
    SURVEY = SURVEY.y,
    SURVEY_YEAR = SURVEY_YEAR,
    INST_CD = INST,
    LCIP_CD = LCIP_CD,
    LCP4_CD = LCIP_LCP4_CD,
    TTRAIN = TTRAIN_ADJ,
    NOC_CD = NOC_CD,
    AGE_AT_SURVEY = TRD_AGE_AT_SURVEY,
    AGE_GROUP = AGE_GROUP,
    AGE_GROUP_ROLLUP = AGE_GROUP_ROLLUP,
    GRAD_STATUS = GRADSTAT_GROUP,
    RESPONDENT = RESPONDENT,
    NEW_LABOUR_SUPPLY = NEW_LABOUR_SUPPLY,
    OLD_LABOUR_SUPPLY = NA_real_,
    WEIGHT = WEIGHT,
    PSSM_CREDENTIAL = PSSM_CREDENTIAL,
    PSSM_CRED = paste0(GRADSTAT_GROUP, " - ", PSSM_CREDENTIAL),
    LCIP4_CRED = paste0(GRADSTAT_GROUP, " - ", LCIP_LCP4_CD, " - ", TTRAIN_STR, " - ", PSSM_CREDENTIAL),
    LCIP2_CRED = paste0(GRADSTAT_GROUP, " - ", str_sub(LCIP_LCP4_CD, 1, 2), " - ", TTRAIN_STR, " - ", PSSM_CREDENTIAL),
    CURRENT_REGION_PSSM_CODE = CURRENT_REGION_PSSM_CODE
  )


# ******************************************************************************
# ---- APPSO Queries ----
# Refresh survey records in T_Cohorts_Recoded
# ******************************************************************************

# APPSO data is already preprocessed by earlier steps. It already has new_labour_supply,
# weight, pssm_credential, and lcip4_cred. We just need to reshape it into the
# T_Cohorts_Recoded schema and join with age lookups.
appso_data <- sch_tbl("t_appso_data_final") %>%
  select(PEN, KEY, SUBM_CD, INST, LCIP_CD, LCIP_LCP4_CD, NOC_CD,
         APP_AGE_AT_SURVEY, RESPONDENT,
         NEW_LABOUR_SUPPLY, WEIGHT,
         PSSM_CREDENTIAL, LCIP4_CRED, CURRENT_REGION_PSSM_CODE) %>%
  collect() |> rename_with(toupper)

# Build APPSO cohort records. Note the differences from TRD:
#   - All APPSO records have grad_status = '1' (they are all graduates)
#   - LCIP2_CRED is derived differently (no grad_status prefix)
#   - ttrain is not in APPSO data (not applicable to apprenticeship)
appso_cohort <- appso_data %>%
  inner_join(year_survey %>% filter(SURVEY == "appso"), by = "SUBM_CD") %>%
  left_join(tbl_age, by = c("APP_AGE_AT_SURVEY" = "AGE")) %>%
  left_join(tbl_age_groups, by = "AGE_GROUP") %>%
  mutate(
    NOC_CD = if_else(NOC_CD == "xxxxx", "99999", NOC_CD)
  ) %>%
  transmute(
    PEN = PEN,
    STQU_ID = paste0("APPSO - ", as.integer(KEY)),
    SURVEY = SURVEY.y,
    SURVEY_YEAR = SURVEY_YEAR,
    INST_CD = INST,
    LCIP_CD = LCIP_CD,
    LCP4_CD = LCIP_LCP4_CD,
    TTRAIN = NA_real_,
    NOC_CD = NOC_CD,
    AGE_AT_SURVEY = APP_AGE_AT_SURVEY,
    AGE_GROUP = AGE_GROUP,
    AGE_GROUP_ROLLUP = AGE_GROUP_ROLLUP,
    GRAD_STATUS = "1",
    RESPONDENT = RESPONDENT,
    NEW_LABOUR_SUPPLY = NEW_LABOUR_SUPPLY,
    OLD_LABOUR_SUPPLY = NA_real_,
    WEIGHT = WEIGHT,
    PSSM_CREDENTIAL = PSSM_CREDENTIAL,
    PSSM_CRED = PSSM_CREDENTIAL,
    LCIP4_CRED = LCIP4_CRED,
    LCIP2_CRED = paste0(str_sub(LCIP_LCP4_CD, 1, 2), " - ", PSSM_CREDENTIAL),
    CURRENT_REGION_PSSM_CODE = CURRENT_REGION_PSSM_CODE
  )


# ******************************************************************************
# ---- BGS Queries ----
# Recode institution codes, update CIPs, derive labour supply, apply weights
# ******************************************************************************

# Pull BGS data and related lookup tables.
bgs_data <- sch_tbl("t_bgs_data_final") %>%
  collect() |> rename_with(toupper)

bgs_inst_recode <- sch_tbl("T_BGS_INST_Recode") %>%
  collect() |> rename_with(toupper)

bgs_outcomes <- sch_tbl("T_bgs_data_final_for_outcomesmatching") %>%
  select(STQU_ID, FINAL_CIP_CODE_4, FINAL_CIP_CODE_2, FINAL_CIP_CLUSTER_CODE) %>%
  collect() |> rename_with(toupper)

# ---- BGS_Q001b: Recode institution codes ----
# WHY: Some BGS institutions have codes that changed over time. The recode table maps
# old codes to the current standard so that weight adjustments across years by program
# can be applied consistently.
bgs_data <- bgs_data %>%
  left_join(bgs_inst_recode %>% select(INST, INST_RECODE), by = "INST") %>%
  mutate(INST = coalesce(INST_RECODE, INST)) %>%
  select(-INST_RECODE)

# ---- BGS_Q001c: Update CIPs after program matching ----
# WHY: The outcomes matching step determines the correct CIP code for each record.
# We update the main BGS table with these matched CIP codes.
bgs_data <- bgs_data %>%
  left_join(bgs_outcomes, by = "STQU_ID") %>%
  mutate(
    CIP_CODE_4 = coalesce(FINAL_CIP_CODE_4, CIP_CODE_4),
    CIP_CODE_2 = coalesce(FINAL_CIP_CODE_2, CIP_CODE_2),
    LCIP_LCIPPC_CD = coalesce(FINAL_CIP_CLUSTER_CODE, LCIP_LCIPPC_CD)
  ) %>%
  select(-FINAL_CIP_CODE_4, -FINAL_CIP_CODE_2, -FINAL_CIP_CLUSTER_CODE)

# ---- BGS_Q002: Set lcip4_cred and pssm_credential ----
# WHY: BGS records are all bachelor's degrees, so the credential is always 'BACH'.
# The LCIP4_CRED composite key is built from CIP code + credential type.
bgs_data <- bgs_data %>%
  mutate(
    LCIP4_CRED = paste0(CIP_CODE_4, " - BACH"),
    PSSM_CREDENTIAL = "BACH"
  )

# Compute BGS labour supply from activity and labour force response codes.
# WHY: BGS uses different survey questions than TRD/DACSO to determine labour supply.
# The logic handles multiple response combinations for current activity and work status.
bgs_data <- bgs_data %>%
  mutate(BGS_NEW_LABOUR_SUPPLY = case_when(
    CURRENT_ACTIVITY == 1 ~ 1,
    CURRENT_ACTIVITY == 4 & FULL_TM_WRK == 1 ~ 1,
    CURRENT_ACTIVITY == 4 & FULL_TM_WRK == 0 ~ 2,
    CURRENT_ACTIVITY == 3 & IN_LBR_FRC == 1 ~ 1,
    is.na(CURRENT_ACTIVITY) & is.na(FULL_TM_WRK) & IN_LBR_FRC == 1 ~ 1,
    is.na(CURRENT_ACTIVITY) & IN_LBR_FRC == 1 ~ 1,
    SRV_Y_N == 0 ~ 0,
    TRUE ~ 0
  ))

# Join with weights (BGS joins on survey_year, not subm_cd like TRD/DACSO).
bgs_weights <- t_weights %>%
  filter(MODEL == "2022-2023", SURVEY == "BGS") %>%
  select(SURVEY_YEAR, all_of(weight_var)) %>%
  rename(WEIGHT = !!sym(weight_var))

bgs_data <- bgs_data %>%
  inner_join(bgs_weights, by = "SURVEY_YEAR")

# Build BGS cohort records.
# WHY: BGS records are all graduates (grad_status = '1'). The LCIP2_CRED uses a
# simpler format than TRD/DACSO since BGS doesn't have ttrain or grad_status grouping.
bgs_cohort <- bgs_data %>%
  mutate(
    NOC_CD = if_else(NOC == "XXXXX", "99999", NOC)
  ) %>%
  transmute(
    PEN = PEN,
    STQU_ID = paste0("BGS - ", as.integer(STQU_ID)),
    SURVEY = "BGS",
    SURVEY_YEAR = SURVEY_YEAR,
    INST_CD = INST,
    LCP4_CD = CIP_CODE_4,
    TTRAIN = NA_real_,
    NOC_CD = NOC_CD,
    AGE_AT_SURVEY = AGE,
    AGE_GROUP = AGE_GROUP,
    AGE_GROUP_ROLLUP = AGE_GROUP_ROLLUP,
    GRAD_STATUS = "1",
    RESPONDENT = SRV_Y_N,
    NEW_LABOUR_SUPPLY = BGS_NEW_LABOUR_SUPPLY,
    OLD_LABOUR_SUPPLY = OLD_LABOUR_SUPPLY,
    WEIGHT = WEIGHT,
    PSSM_CREDENTIAL = PSSM_CREDENTIAL,
    PSSM_CRED = PSSM_CREDENTIAL,
    LCIP4_CRED = LCIP4_CRED,
    LCIP2_CRED = paste0(str_sub(CIP_CODE_4, 2, 3), " - BACH"),
    CURRENT_REGION_PSSM_CODE = CURRENT_REGION_PSSM_CODE
  )


# ******************************************************************************
# ---- DACSO Queries ----
# Adds age, updates credential, creates new LCIP4_CRED variable
# ******************************************************************************

# Pull DACSO source data and infoware outcomes reference.
dacso_stepa <- sch_tbl("t_dacso_data_part_1_stepa") %>%
  collect() |> rename_with(toupper)

infoware_outc <- sch_tbl("infoware_c_outc_clean_short_resp") %>%
  select(STQU_ID, Q08, PFST_HAD_PREVIOUS_CDTL, PFST_FURSTDY_INCL_STILL_ATTD) %>%
  collect() |> rename_with(toupper)

# ---- DACSO_Q003: Build DACSO data part 1 ----
# Join stepa data with credential grouping to add PSSM credential categories,
# and with age tables to add age groups. Only include credentials that are in the
# model (dacso_include_in_model IS NOT NULL).
# WHY: DACSO covers many credential types, but only some are relevant for the PSSM
# model. The grouping table maps each credential to a PSSM category and flags which
# ones to include.
dacso_part1 <- dacso_stepa %>%
  inner_join(
    pssm_cred_grouping %>%
      filter(!is.na(DACSO_INCLUDE_IN_MODEL)) %>%
      select(PRGM_CREDENTIAL_AWARDED, PRGM_CREDENTIAL_AWARDED_NAME,
             PSSM_CREDENTIAL, PSSM_CREDENTIAL_NAME, DACSO_INCLUDE_IN_MODEL),
    by = c("PRGM_CREDENTIAL" = "PRGM_CREDENTIAL_AWARDED")
  ) %>%
  left_join(tbl_age, by = c("COCI_AGE_AT_SURVEY" = "AGE")) %>%
  left_join(tbl_age_groups, by = "AGE_GROUP") %>%
  mutate(
    LCIP4_CRED = paste0(
      as.character(COSC_GRAD_STATUS_LGDS_CD_GROUP), " - ", LCP4_CD, " - ",
      if_else(as.character(TTRAIN) == "2", "1", as.character(TTRAIN)), " - ",
      PSSM_CREDENTIAL
    )
  )

# ---- DACSO_Q003b: Update fields from infoware outcomes ----
# WHY: Some DACSO fields need correction based on the cleaned outcomes data from
# INFOWARE. The had_previous_credential field uses a conditional: if q08 is '1',
# use the pfst value; otherwise use q08 directly.
dacso_part1 <- dacso_part1 %>%
  left_join(infoware_outc, by = c("COCI_STQU_ID" = "STQU_ID")) %>%
  mutate(
    HAD_PREVIOUS_CREDENTIAL = if_else(
      Q08 == "1", PFST_HAD_PREVIOUS_CDTL, Q08
    ),
    PFST_IN_POST_SEC_BEFORE = coalesce(Q08, PFST_IN_POST_SEC_BEFORE),
    PFST_HAD_PREVIOUS_CDTL = coalesce(PFST_HAD_PREVIOUS_CDTL, PFST_HAD_PREVIOUS_CDTL),
    PFST_FURSTDY_INCL_STILL_ATTD = coalesce(PFST_FURSTDY_INCL_STILL_ATTD, PFST_FURSTDY_INCL_STILL_ATTD)
  ) %>%
  select(-Q08, -PFST_HAD_PREVIOUS_CDTL.y, -PFST_FURSTDY_INCL_STILL_ATTD.y) %>%
  rename(
    PFST_HAD_PREVIOUS_CDTL = PFST_HAD_PREVIOUS_CDTL.x,
    PFST_FURSTDY_INCL_STILL_ATTD = PFST_FURSTDY_INCL_STILL_ATTD.x
  )

# ---- DACSO_Q005: Compute labour supply and weights ----
# WHY: DACSO uses different survey questions than TRD/BGS for labour supply.
# The logic handles current activity, employment status, and labour market participation.
dacso_part1 <- dacso_part1 %>%
  mutate(NEW_LABOUR_SUPPLY = case_when(
    PFST_CURRENT_ACTIVITY == 3 ~ 1,
    PFST_CURRENT_ACTIVITY == 2 & LABR_EMPLOYED_FULL_PART_TIME == 1 ~ 1,
    PFST_CURRENT_ACTIVITY == 2 & LABR_EMPLOYED_FULL_PART_TIME == 0 ~ 2,
    PFST_CURRENT_ACTIVITY == 4 & LABR_IN_LABOUR_MARKET == 1 ~ 1,
    RESPONDENT == "1" ~ 0,
    TRUE ~ 0
  ))

# Join with weights (DACSO joins on subm_cd).
dacso_weights <- t_weights %>%
  filter(MODEL == "2022-2023", SURVEY == "DACSO") %>%
  select(SUBM_CD, all_of(weight_var)) %>%
  rename(WEIGHT = !!sym(weight_var))

dacso_part1 <- dacso_part1 %>%
  inner_join(dacso_weights, by = c("COCI_SUBM_CD" = "SUBM_CD"))

# Write t_dacso_data_part_1 to database — this intermediate table is kept for
# downstream use and verification.
dbWriteTable(decimal_con, SQL(glue::glue('"{my_schema}"."t_dacso_data_part_1"')),
             dacso_part1, overwrite = TRUE)

# Build DACSO cohort records.
# WHY: DACSO has the most complex cohort record structure due to grad_status grouping,
# ttrain recoding, and the multiple composite keys (PSSM_CRED, LCIP4_CRED, LCIP2_CRED).
# Each key combines different dimensions for different levels of aggregation.
dacso_cohort <- dacso_part1 %>%
  inner_join(year_survey %>% filter(SURVEY == "DACSO"), by = c("COCI_SUBM_CD" = "SUBM_CD")) %>%
  mutate(
    TTRAIN_ADJ = if_else(TTRAIN == 2, 1, TTRAIN),
    TTRAIN_STR = if_else(TTRAIN == 2, "1", as.character(TTRAIN)),
    NOC_CD = if_else(LABR_OCCUPATION_LNOC_CD == "XXXXX", "99999", LABR_OCCUPATION_LNOC_CD)
  ) %>%
  transmute(
    PEN = COCI_PEN,
    STQU_ID = paste0("DACSO - ", COCI_STQU_ID),
    SURVEY = SURVEY.y,
    SURVEY_YEAR = SURVEY_YEAR,
    INST_CD = COCI_INST_CD,
    LCP4_CD = LCP4_CD,
    TTRAIN = TTRAIN_ADJ,
    NOC_CD = NOC_CD,
    AGE_AT_SURVEY = COCI_AGE_AT_SURVEY,
    AGE_GROUP = AGE_GROUP,
    AGE_GROUP_ROLLUP = AGE_GROUP_ROLLUP,
    GRAD_STATUS = COSC_GRAD_STATUS_LGDS_CD_GROUP,
    RESPONDENT = RESPONDENT,
    NEW_LABOUR_SUPPLY = NEW_LABOUR_SUPPLY,
    OLD_LABOUR_SUPPLY = OLD_LABOUR_SUPPLY,
    WEIGHT = WEIGHT,
    PSSM_CREDENTIAL = PSSM_CREDENTIAL,
    PSSM_CRED = paste0(COSC_GRAD_STATUS_LGDS_CD_GROUP, " - ", PSSM_CREDENTIAL),
    LCIP4_CRED = paste0(COSC_GRAD_STATUS_LGDS_CD_GROUP, " - ", LCP4_CD, " - ", TTRAIN_STR, " - ", PSSM_CREDENTIAL),
    LCIP2_CRED = paste0(COSC_GRAD_STATUS_LGDS_CD_GROUP, " - ", str_sub(LCP4_CD, 1, 2), " - ", TTRAIN_STR, " - ", PSSM_CREDENTIAL),
    CURRENT_REGION_PSSM_CODE = CURRENT_REGION_PSSM_CODE
  )


# ******************************************************************************
# ---- T_Cohorts_Recoded ----
# Refresh all survey records in T_Cohorts_Recoded
# ******************************************************************************

# Pull existing T_Cohorts_Recoded and remove old records for all 4 surveys.
# WHY: Each model run refreshes the survey data from scratch. The DELETE + INSERT
# pattern ensures no stale records remain. In dplyr, we filter out old survey records
# and bind the new ones.
cohorts <- sch_tbl("T_Cohorts_Recoded") %>%
  collect() |> rename_with(toupper)

# Remove old survey records for all 4 surveys
cohorts <- cohorts %>%
  filter(!SURVEY %in% c("TRD", "APPSO", "BGS", "DACSO"))

# Add new records from all 4 surveys
cohorts <- bind_rows(cohorts, trd_cohort, appso_cohort, bgs_cohort, dacso_cohort)

# Write the updated T_Cohorts_Recoded back to the database
dbWriteTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_Cohorts_Recoded"')),
             cohorts, overwrite = TRUE)


# ---- Keep ----
dbExistsTable(decimal_con, "APPSO_Graduates")
dbExistsTable(decimal_con, "TRD_Graduates")
dbExistsTable(decimal_con, "t_dacso_data_part_1")
dbExistsTable(decimal_con, "T_Cohorts_Recoded")

# ---- Clean Up Lookups (if desired, not a needed step) ----
# dbExecute(decimal_con, "DROP TABLE T_BGS_INST_Recode;")
# dbExecute(decimal_con, "DROP TABLE T_PSSM_Credential_Grouping")
# dbExecute(decimal_con, "DROP TABLE t_year_survey_year")
# dbExecute(decimal_con, "DROP TABLE t_current_region_pssm_codes")
# dbExecute(decimal_con, "DROP TABLE t_current_region_pssm_rollup_codes")
# dbExecute(decimal_con, "DROP TABLE t_current_region_pssm_rollup_codes_bc")

dbDisconnect(decimal_con)
# rm(list=ls())
