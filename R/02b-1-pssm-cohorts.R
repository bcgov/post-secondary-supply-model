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

# This script prepares student outcomes data for the following student surveys: TRD, APP, DACSO, BGS
# need tables: load-cohort-tablename.r
#
# APP:
#     Assumes geocoding has been done, and CURRENT_REGION_PSSM_CODE contains final region code to use and
#       year weights for model have been added.  New Labour Supply has been calculated and
#       age and age group have been added + a new student id
#     Refreshes survey records in T_Cohorts_Recoded
#
# TRD:
#     Assumes geocoding has been done, and CURRENT_REGION_PSSM_CODE contains final region code to use and
#       New Labour Supply has been calculated
#     Refreshes survey records in T_Cohorts_Recoded
#     Adds year weights for model
#     Adds Age and age groups + a new student id
#
# DACSO:
#     Assumes geocoding has been done, and CURRENT_REGION_PSSM_CODE contains final region code to use
#     Recodes institution codes to be consistent to STP file
#     Update CIPS after program matching.
#     Applies weight for model year and derives New Labour Supply
#     Adds age and age group, a new student id
#     Refresh survey records in T_Cohorts_Recoded
#
# BGS:
#     Assumes geocoding has been done, and CURRENT_REGION_PSSM_CODE contains final region code to use
#     Recodes institution codes to be consistent to STP file
#     Updates CIPS after program matching.
#     Applies weight for model year and derives New Labour Supply
#     Adds age and age group, a new student id
#     Refreshes survey records in T_Cohorts_Recoded
#
#     Notes: double check method for updating CIP codes after program matching.
#     There is a query to check for invalid NOC codes (see documentation).
#     Update T-Year_Survey_Year and T_weights (for all cohorts)
#     2006 dacso all NULL lcip-4-creds (remove 2006)
#

library(tidyverse)
library(config)
library(DBI)
library(odbc)
library(glue)
library(assertthat)
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

log_info("==== 02b-1-pssm-cohorts.R START ====")

# regular_run <- T
# qi_run <- F
# ptib_run <- F

## -------------------------- Configure LAN Paths and DB Connection ------------------------------
## -----------------------------------------------------------------------------------------------

my_schema <- config::get("myschema")

db_config <- config::get("decimal")
con <- dbConnect(
  odbc::odbc(),
  Driver = db_config$driver,
  Server = db_config$server,
  Database = db_config$database,
  Trusted_Connection = "True"
)

lan <- config::get("lan")

log_info("Connected to SQL Server database")


## --------------------------------------Required Tables------------------------------------------
## -----------------------------------------------------------------------------------------------

# List of required tables with categories
# these to be renamed in load scripts
appso_data_final <- t_appso_data_final
bgs_data_final <- t_bgs_data_final
bgs_inst_recode <- t_bgs_inst_recode

t_cohorts_recoded <- t_cohorts_recoded |> filter(FALSE)

required_tables <- c(
  "trd_graduates",
  "trd_data",
  "appso_data_final",
  "appso_graduates",
  "bgs_data_final",
  "bgs_inst_recode",
  "t_bgs_data_final_for_outcomesmatching",
  "t_weights",
  "t_dacso_data_part_1_stepa",
  "infoware_c_outc_clean_short_resp",
  "t_current_region_pssm_codes",
  "t_current_region_pssm_rollup_codes",
  "t_current_region_pssm_rollup_codes_bc",
  "tbl_age",
  "tbl_age_groups",
  "t_pssm_credential_grouping",
  "t_year_survey_year"
)

# Check for required data tables in the database
missing <- required_tables[!sapply(required_tables, exists, where = .GlobalEnv)]

if (length(missing) > 0) {
  stop(paste(
    "The following required tables are missing from the environment:",
    paste(missing, collapse = ", ")
  ))
} else {
  log_info(glue::glue(
    "All {length(required_tables)} required tables found in environment"
  ))
}


# Set weight for model year
target_weight <- if (qi_run) "WEIGHT_QI" else "WEIGHT"

tbl_age <- tbl_age |> janitor::clean_names("all_caps") |> select(AGE, AGE_GROUP)

# ---- TRD Queries ----
log_info("Processing TRD cohort: applying weights, age groups, labour supply")
# Applies weight for model year and derives New Labour Supply
trd_data <- trd_data |>
  select(-WEIGHT) |>
  inner_join(
    t_weights |>
      filter(MODEL == "2024-2025", SURVEY == "TRD") |>
      select(SUBM_CD, WEIGHT = any_of(target_weight)),
    by = "SUBM_CD"
  ) |>
  left_join(
    tbl_age,
    by = c("TRD_AGE_AT_SURVEY" = "AGE")
  ) |>
  left_join(
    tbl_age_groups |> select(AGE_GROUP, AGE_GROUP_ROLLUP),
    by = "AGE_GROUP"
  ) |>
  mutate(
    NEW_LABOUR_SUPPLY = case_when(
      TRD_LABR_EMPLOYED == 1 ~ 1,
      TRD_LABR_IN_LABOUR_MARKET == 1 & TRD_LABR_EMPLOYED == 0 ~ 1,
      TRD_LABR_EMPLOYED == 0 ~ 0,
      RESPONDENT == "1" ~ 0,
      TRUE ~ 0
    )
  )

trd_data <-
  trd_data |>
  inner_join(
    t_year_survey_year |>
      filter(SURVEY == "TRD") |>
      select(SURVEY_YEAR, SUBM_CD),
    by = "SUBM_CD"
  ) |>
  transmute(
    PEN = PEN,
    STQU_ID = paste0("TRD - ", as.character(KEY)),
    SURVEY = SURVEY,
    SURVEY_YEAR = SURVEY_YEAR,
    INST_CD = INST,
    LCIP_CD = LCIP_CD,
    LCP4_CD = LCIP_LCP4_CD,
    TTRAIN = if_else(TTRAIN == 2, 1, as.numeric(TTRAIN)),
    NOC_CD = if_else(NOC_CD == "XXXXX", "99999", NOC_CD),
    AGE_AT_SURVEY = TRD_AGE_AT_SURVEY,
    AGE_GROUP = AGE_GROUP,
    AGE_GROUP_ROLLUP = AGE_GROUP_ROLLUP,
    GRAD_STATUS = GRADSTAT_GROUP,
    RESPONDENT = RESPONDENT,
    NEW_LABOUR_SUPPLY = NEW_LABOUR_SUPPLY,
    WEIGHT = WEIGHT,
    PSSM_CREDENTIAL = PSSM_CREDENTIAL,
    PSSM_CRED = paste0(GRADSTAT_GROUP, " - ", PSSM_CREDENTIAL),
    LCIP4_CRED = paste(
      GRADSTAT_GROUP,
      LCIP_LCP4_CD,
      if_else(TTRAIN == 2, "1", as.character(TTRAIN)),
      PSSM_CREDENTIAL,
      sep = " - "
    ),
    LCIP2_CRED = paste(
      GRADSTAT_GROUP,
      substr(LCIP_LCP4_CD, 1, 2),
      if_else(TTRAIN == 2, "1", as.character(TTRAIN)),
      PSSM_CREDENTIAL,
      sep = " - "
    ),
    CURRENT_REGION_PSSM_CODE = CURRENT_REGION_PSSM_CODE
  )

trd_data[setdiff(names(t_cohorts_recoded), names(trd_data))] <- NA
t_cohorts_recoded <- t_cohorts_recoded |> rbind(trd_data)
log_info(glue::glue(
  "TRD cohort added: {nrow(trd_data)} records. T_Cohorts_Recoded now has {nrow(t_cohorts_recoded)} records"
))

# ---- APP Queries ----
# Process APPSO data into the cohorts recoded table
appso_data_final <- appso_data_final |>
  select(-AGE_GROUP, -AGE_GROUP_LABEL) |>
  inner_join(
    t_year_survey_year |>
      filter(SURVEY == "APPSO") |>
      select(SURVEY_YEAR, SUBM_CD),
    by = "SUBM_CD"
  ) |>
  left_join(
    tbl_age |> inner_join(tbl_age_groups, by = "AGE_GROUP"),
    by = c("APP_AGE_AT_SURVEY" = "AGE")
  ) |>
  transmute(
    PEN = PEN,
    STQU_ID = paste0("APPSO - ", as.character(as.integer(KEY))),
    SURVEY = SURVEY,
    SURVEY_YEAR = SURVEY_YEAR,
    INST_CD = INST,
    LCIP_CD = LCIP_CD,
    LCP4_CD = LCIP_LCP4_CD,
    NOC_CD = if_else(NOC_CD == "XXXXX", "99999", NOC_CD),
    AGE_AT_SURVEY = APP_AGE_AT_SURVEY,
    AGE_GROUP = AGE_GROUP,
    AGE_GROUP_ROLLUP = AGE_GROUP_ROLLUP,
    GRAD_STATUS = "1",
    RESPONDENT = RESPONDENT,
    NEW_LABOUR_SUPPLY = NEW_LABOUR_SUPPLY,
    WEIGHT = WEIGHT,
    PSSM_CREDENTIAL = PSSM_CREDENTIAL,
    PSSM_CRED = PSSM_CREDENTIAL, # In your SQL, this was mapped to pssm_credential
    LCIP4_CRED = LCIP4_CRED,
    LCIP2_CRED = paste0(substr(LCIP_LCP4_CD, 1, 2), " - ", PSSM_CREDENTIAL),
    CURRENT_REGION_PSSM_CODE = CURRENT_REGION_PSSM_CODE
  )

appso_data_final[setdiff(
  names(t_cohorts_recoded),
  names(appso_data_final)
)] <- NA
t_cohorts_recoded <- t_cohorts_recoded |> rbind(appso_data_final)
log_info(glue::glue(
  "APPSO cohort added: {nrow(appso_data_final)} records. T_Cohorts_Recoded now has {nrow(t_cohorts_recoded)} records"
))

# ---- BGS Queries ----
log_info(
  "Processing BGS cohort: institution recode, CIP update, weights, labour supply"
)
t_bgs_data_final <- t_bgs_data_final |>
  left_join(
    t_bgs_inst_recode,
    by = "INST"
  ) |>
  mutate(
    INST = coalesce(INST_RECODE, INST)
  ) |>
  select(-INST_RECODE)

# update cips after program matching
t_bgs_data_final <- t_bgs_data_final |>
  left_join(
    t_bgs_data_final_for_outcomesmatching |>
      select(
        STQU_ID,
        FINAL_CIP_CODE_4,
        FINAL_CIP_CODE_2,
        FINAL_CIP_CLUSTER_CODE
      ),
    by = "STQU_ID"
  ) |>
  mutate(
    CIP_CODE_4 = coalesce(FINAL_CIP_CODE_4, CIP_CODE_4),
    CIP_CODE_2 = coalesce(FINAL_CIP_CODE_2, CIP_CODE_2),
    LCIP_LCIPPC_CD = coalesce(
      FINAL_CIP_CLUSTER_CODE,
      as.character(LCIP_LCIPPC_CD)
    )
  ) |>
  select(-FINAL_CIP_CODE_4, -FINAL_CIP_CODE_2, -FINAL_CIP_CLUSTER_CODE)

t_bgs_data_final <- t_bgs_data_final |>
  mutate(
    LCIP4_CRED = paste0(CIP_CODE_4, " - ", "BACH"),
    PSSM_CREDENTIAL = "BACH"
  )

t_bgs_data_final <- t_bgs_data_final |>
  select(-WEIGHT, -AGE_GROUP, -AGE_GROUP_ROLLUP) |>
  inner_join(
    t_weights |>
      filter(MODEL == "2024-2025", SURVEY == "BGS") |>
      mutate(SURVEY_YEAR = as.double(SURVEY_YEAR)) |>
      select(SURVEY_YEAR, WEIGHT = any_of(target_weight)),
    by = "SURVEY_YEAR"
  ) |>
  left_join(
    tbl_age,
    by = "AGE"
  ) |>
  left_join(
    tbl_age_groups |> select(-AGE_GROUP_LABEL),
    by = "AGE_GROUP"
  ) |>
  mutate(
    BGS_NEW_LABOUR_SUPPLY = case_when(
      CURRENT_ACTIVITY == 1 ~ 1,
      CURRENT_ACTIVITY == 4 & FULL_TM_WRK == 1 ~ 1,
      CURRENT_ACTIVITY == 4 & FULL_TM_WRK == 0 ~ 2,
      CURRENT_ACTIVITY == 3 & IN_LBR_FRC == 1 ~ 1,
      is.na(CURRENT_ACTIVITY) & is.na(FULL_TM_WRK) & IN_LBR_FRC == 1 ~ 1,
      is.na(CURRENT_ACTIVITY) & IN_LBR_FRC == 1 ~ 1,
      SRV_Y_N == 0 ~ 0,
      TRUE ~ 0
    )
  )
# Refresh bgs survey records in T_Cohorts_Recoded
bgs_update <- t_bgs_data_final |>
  transmute(
    PEN = PEN,
    STQU_ID = paste0("BGS - ", as.character(as.integer(STQU_ID))),
    SURVEY = "BGS",
    SURVEY_YEAR = SURVEY_YEAR,
    INST_CD = INST,
    LCP4_CD = CIP_CODE_4,
    NOC_CD = if_else(NOC == "XXXXX", "99999", NOC),
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
    LCIP2_CRED = paste0(substr(CIP_CODE_4, 1, 2), " - ", "BACH"),
    CURRENT_REGION_PSSM_CODE = CURRENT_REGION_PSSM_CODE
  )

bgs_update[setdiff(
  names(t_cohorts_recoded),
  names(bgs_update)
)] <- NA

t_cohorts_recoded <-
  t_cohorts_recoded |>
  filter(SURVEY != "BGS")

t_cohorts_recoded <- t_cohorts_recoded |> rbind(bgs_update)
log_info(glue::glue(
  "BGS cohort added: {nrow(bgs_update)} records. T_Cohorts_Recoded now has {nrow(t_cohorts_recoded)} records"
))

# ----DACSO Queries ----
log_info(
  "Processing DACSO cohort: credential grouping, age, CIP update, weights, labour supply"
)
# NA (why)?  This forces  LCIP4_CRED to include NA's in the concatenated parts
# but SQL coerces the entire variable to NA

# adds age, updates credential, creates new LCIP4_CRED variable
t_dacso_data_part_1 <- t_dacso_data_part_1_stepa |>
  select(-AGE_GROUP, -AGE_GROUP_ROLLUP) |>
  inner_join(
    t_pssm_credential_grouping,
    by = c("PRGM_CREDENTIAL" = "PRGM_CREDENTIAL_AWARDED")
  ) |>
  left_join(
    tbl_age,
    by = c("COCI_AGE_AT_SURVEY" = "AGE")
  ) |>
  left_join(
    tbl_age_groups,
    by = "AGE_GROUP"
  ) |>
  mutate(
    LCIP4_CRED = paste(
      as.character(COSC_GRAD_STATUS_LGDS_CD_GROUP),
      LCP4_CD,
      if_else(as.character(TTRAIN) == "2", "1", as.character(TTRAIN)),
      PSSM_CREDENTIAL,
      sep = " - "
    )
  ) |>
  rename(PRGM_CREDENTIAL_AWARDED = PRGM_CREDENTIAL) |>
  select(
    -TPID_CURRENT_REGION1,
    -TPID_CURRENT_REGION4,
    -COSC_GRAD_STATUS_LGDS_CD,
    -DACSO_INCLUDE_IN_MODEL,
    -AGE_GROUP_LABEL
  )

# Recode institution codes for CIP-NOC work
t_dacso_data_part_1 <- t_dacso_data_part_1 |>
  select(-PFST_HAD_PREVIOUS_CDTL, -PFST_FURSTDY_INCL_STILL_ATTD) |>
  inner_join(
    infoware_c_outc_clean_short_resp |>
      select(
        STQU_ID,
        Q08,
        PFST_HAD_PREVIOUS_CDTL,
        PFST_FURSTDY_INCL_STILL_ATTD
      ),
    by = c("COCI_STQU_ID" = "STQU_ID")
  ) |>
  mutate(
    HAD_PREVIOUS_CREDENTIAL = if_else(
      Q08 == "1",
      PFST_HAD_PREVIOUS_CDTL,
      Q08
    ),
    PFST_IN_POST_SEC_BEFORE = Q08,
    PFST_HAD_PREVIOUS_CDTL = PFST_HAD_PREVIOUS_CDTL,
    PFST_FURSTDY_INCL_STILL_ATTD = PFST_FURSTDY_INCL_STILL_ATTD
  ) |>
  select(-Q08)

# Deletes other, none, invalid etc. credentials that are not part of the PSSM
t_dacso_data_part_1 <- t_dacso_data_part_1 |>
  anti_join(
    t_pssm_credential_grouping |>
      filter(is.na(DACSO_INCLUDE_IN_MODEL)),
    by = "PRGM_CREDENTIAL_AWARDED"
  )

# Recodes all the old institution codes to the current code so that weight adjustments across years by program can be applied.
# This step skipped as not needed, but could add as a check at some point.
# dbExecute(decimal_con, DACSO_Q004b_INST_Recode)

# Applies weight for model year and derives New Labour Supply - re-run if changing model years or grouping geographies
t_dacso_data_part_1 <- t_dacso_data_part_1 |>
  select(-WEIGHT) |>
  inner_join(
    t_weights |>
      filter(MODEL == "2024-2025", SURVEY == "DACSO") |>
      select(SUBM_CD, WEIGHT = any_of(target_weight)),
    by = c("COCI_SUBM_CD" = "SUBM_CD")
  ) |>
  mutate(
    NEW_LABOUR_SUPPLY = case_when(
      PFST_CURRENT_ACTIVITY == 3 ~ 1,
      PFST_CURRENT_ACTIVITY == 2 & LABR_EMPLOYED_FULL_PART_TIME == 1 ~ 1,
      PFST_CURRENT_ACTIVITY == 2 & LABR_EMPLOYED_FULL_PART_TIME == 0 ~ 2,
      PFST_CURRENT_ACTIVITY == 4 & LABR_IN_LABOUR_MARKET == 1 ~ 1,
      RESPONDENT == "1" ~ 0,
      TRUE ~ 0
    )
  )

# Refresh dacso survey records in t_cohorts_recoded
dacso_update <- t_dacso_data_part_1 |>
  inner_join(
    t_year_survey_year |> filter(SURVEY == "DACSO"),
    by = c("COCI_SUBM_CD" = "SUBM_CD")
  ) |>
  mutate(
    PEN = COCI_PEN,
    STQU_ID = paste0("DACSO - ", as.character(COCI_STQU_ID)),
    INST_CD = COCI_INST_CD,
    TTRAIN_VAL = if_else(TTRAIN == 2, 1, as.numeric(TTRAIN)),
    NOC_CD = if_else(
      LABR_OCCUPATION_LNOC_CD == "XXXXX",
      "99999",
      LABR_OCCUPATION_LNOC_CD
    ),
    AGE_AT_SURVEY = COCI_AGE_AT_SURVEY,
    GRAD_STATUS = COSC_GRAD_STATUS_LGDS_CD_GROUP,
    PSSM_CREDENTIAL = PSSM_CREDENTIAL,
    PSSM_CRED = paste(
      COSC_GRAD_STATUS_LGDS_CD_GROUP,
      PSSM_CREDENTIAL,
      sep = " - "
    ),
    LCIP4_CRED = paste(
      COSC_GRAD_STATUS_LGDS_CD_GROUP,
      LCP4_CD,
      if_else(TTRAIN == 2, "1", as.character(TTRAIN)),
      PSSM_CREDENTIAL,
      sep = " - "
    ),
    LCIP2_CRED = paste(
      COSC_GRAD_STATUS_LGDS_CD_GROUP,
      substr(LCP4_CD, 1, 2),
      if_else(TTRAIN == 2, "1", as.character(TTRAIN)),
      PSSM_CREDENTIAL,
      sep = " - "
    )
  )

t_cohorts_recoded <-
  t_cohorts_recoded |>
  filter(SURVEY != "DACSO")

dacso_update[setdiff(
  names(t_cohorts_recoded),
  names(dacso_update)
)] <- NA

t_cohorts_recoded <- t_cohorts_recoded |>
  rbind(dacso_update |> select(any_of(names(t_cohorts_recoded))))


## ------------------------------------ Clean Up --------------------------------------------------
# Current workflow:
#  - Write key tables back to sql server.  These are tables needed for downstream work, or tables
# that might be needed for later reference outside of this analysis.
#  - Close DB connections
#  - Remove all other objects at the end of each script.
## ------------------------------------------------------------------------------------------------

tables_to_keep <- c(
  "t_dacso_data_part_1",
  "t_cohorts_recoded"
)

write_table_to_db <- function(table_name, schema, con) {
  db_name <- paste0(table_name, "_r")
  dbWriteTable(
    con,
    SQL(glue::glue('"{schema}"."{db_name}"')),
    base::get(table_name, envir = .GlobalEnv),
    overwrite = TRUE
  )
}

walk(tables_to_keep, write_table_to_db, schema = my_schema, con = con)
log_info(glue::glue(
  "Wrote {length(tables_to_keep)} tables to DB: {paste(tables_to_keep, collapse=', ')}"
))

log_info("==== 02b-1-pssm-cohorts.R COMPLETE ====")
