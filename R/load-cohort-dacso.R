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

# This script loads student outcomes data for students who recently graduated after
# completing programs at public colleges, institutes, and teaching-intensive universities (~18 months prior)
#
# DACSO = Diploma / Associate Certificate Student Outcomes.  Survey of former
# students of public colleges, institutes and teaching-intensive universities,
# conducted roughly 18 months after graduation.
#
# This script reads survey responses + all the look-ups they need, prepares a
# first-cut DACSO table and writes everything to SQL Server
# (schema = write_schema, database = database/PSSM2025) as <name>_r tables.
# Downstream, 02b-1-pssm-cohorts.R builds the full DACSO cohort from these
# tables and refreshes the DACSO records in T_Cohorts_Recoded (the master
# cohort table feeding credential analysis 01c-, labour supply 02b-2 and
# projections 04-/06-).
#
# The following data-set is read from SQL server database:
#   t_dacso_data_part_1_stepa: unique survey responses for each person/survey year (for years since last model run)
#   infoware_c_outc_clean_short_resp:
#
# The following data-sets are read into SQL server from the LAN:
#   tbl_Age: bins ages into groups (1-10)
#   tbl_Age_Groups: used to assign a label to each age group.
#   t_current_region_pssm_rollup_codes_bc: loo-up re-codes maps regions
#   t_current_region_pssm_rollup_codes: look-up re-codes maps regions
#   t_current_region_pssm_codes: look-up re-codes maps regions
#   tbl_noc_skill_level_aged_17_34: used SQL server for upload as this file contains non-supported type characters
#   T_PSSM_Credential_Grouping: a static table for relabeling credential names
#   T_year_survey_year: TO DO
#   T_Cohorts_Recoded:  this contains survey records for all years.  The table is refreshed in the workflow.
#
# Notes: tbl_noc_skill_level_aged_17_34: used SQL server for upload as this file contains 'non-supported' type characters.
#   T_year_survey_year is carried forward from last models run and updated with new data.
#   T_Cohorts_Recoded: can instead just be created each year.  See comments in 02b-pssm-cohots-dacso.R
#   Age group labels are assigned in the script.  There are two different groupings used to group students by age in the model,
#    check the groupings are the same in DACSO, APPSO etc cohorts

library(tidyverse)
library(config)
library(DBI)
library(odbc)
library(futile.logger)

## -------------------------- Logging Setup ------------------------------------------------------
## -----------------------------------------------------------------------------------------------
log_file <- "./R/execution_log.txt"
flog.appender(appender.file(log_file), name = "file_logger")
flog.threshold(INFO, name = "file_logger")

log_info <- function(msg) {
  flog.info(msg, name = "file_logger")
  print(paste(Sys.time(), "|", msg))
}

log_info("==== load-cohort-dacso.R START ====")

## -------------------------- Configure LAN Paths and DB Connection ------------------------------
## -----------------------------------------------------------------------------------------------

write_schema <- config::get("shareschema")
db_config <- config::get("decimal")
con <- dbConnect(
  odbc::odbc(),
  Driver = db_config$driver,
  Server = db_config$server,
  Database = db_config$database,
  Trusted_Connection = "True"
)

lan <- config::get("lan")

log_info("Connected to SQL Server database (decimal)")

## --------------------------------------Required Tables------------------------------------------
## -----------------------------------------------------------------------------------------------

# Survey responses from INFOWARE (Oracle), one row per respondent per survey
# cycle.  Carried through to 02b-1-pssm-cohorts.R where it supplies follow-up
# flags (Q08 = had post-secondary training before this program;
# PFST_HAD_PREVIOUS_CDTL, PFST_FURSTDY_INCL_STILL_ATTD) used to derive
# HAD_PREVIOUS_CREDENTIAL / PFST_IN_POST_SEC_BEFORE for the DACSO cohort.
infoware_c_outc_clean_short_resp <- read_oracle_csv_auto(glue::glue(
  "{lan}/data/student-outcomes/csv/infoware_c_outc_clean_short_resp.csv"
))
log_info(glue::glue(
  "Read infoware_c_outc_clean_short_resp.csv: {nrow(infoware_c_outc_clean_short_resp)} rows"
))

# Age look-ups: TBL_AGE assigns each age to an AGE_GROUP code and
# TBL_AGE_GROUPS adds the group label + AGE_GROUP_ROLLUP.  Used in
# 02b-1-pssm-cohorts.R for every survey type (DACSO, APPSO, BGS) so age is
# grouped identically across cohorts.  TBL_AGE_GROUPS_ROLLUP is the
# coarser rollup used for reporting.
tbl_age_groups <-
  readr::read_csv(
    glue::glue("{lan}/development/csv/gh-source/lookups/02/tbl_Age_Groups.csv"),
    col_types = cols(.default = col_guess())
  ) %>%
  janitor::clean_names(case = "all_caps")

tbl_age_groups_rollup <-
  readr::read_csv(
    glue::glue(
      "{lan}/development/csv/gh-source/lookups/02/tbl_Age_Groups_Rollup.csv"
    ),
    col_types = cols(.default = col_guess())
  ) %>%
  janitor::clean_names(case = "all_caps")

tbl_age <-
  readr::read_csv(
    glue::glue("{lan}/development/csv/gh-source/lookups/02/tbl_Age.csv"),
    col_types = cols(.default = col_guess())
  ) %>%
  janitor::clean_names(case = "all_caps")

# Static look-up that maps each institution's program credential
# (PRGM_CREDENTIAL_AWARDED) to the standard PSSM credential.  Used in
# 02b-1-pssm-cohorts.R to label credentials and to drop credentials excluded
# from the model (DACSO_INCLUDE_IN_MODEL = NA).
t_pssm_credential_grouping <-
  readr::read_csv(
    glue::glue(
      "{lan}/development/csv/gh-source/lookups/02/T_PSSM_Credential_Grouping.csv"
    ),
    col_types = cols(.default = col_guess())
  ) %>%
  janitor::clean_names(case = "all_caps")

# Maps each survey cycle code (SUBM_CD) to the calendar year it surveyed
# (SURVEY_YEAR), per survey type (SURVEY = DACSO/APPSO/BGS).  Used in
# 02b-1-pssm-cohorts.R to date every cohort record before it enters
# T_Cohorts_Recoded.
t_year_survey_year <-
  readr::read_csv(
    glue::glue(
      "{lan}/development/csv/gh-source/lookups/02/T_Year_Survey_Year.csv"
    ),
    col_types = cols(.default = col_guess())
  ) %>%
  janitor::clean_names(case = "all_caps")

# Empty column template (n_max = 1 row, then dropped) with the exact column
# names/types of the master T_Cohorts_Recoded table.  Used by downstream
# modules as the structure to rbind each cohort onto.
t_cohorts_recoded <-
  readr::read_csv(
    glue::glue(
      "{lan}/development/csv/gh-source/rollover/02/T_Cohorts_Recoded.csv"
    ),
    col_types = cols(
      PEN = "c",
      STQU_ID = "c",
      Survey = "c",
      LCIP_CD = "c",
      LCP4_CD = "c",
      NOC_CD = "c",
      INST_CD = "c",
      PSSM_Credential = "c",
      PSSM_CRED = "c",
      LCIP4_CRED = "c",
      LCIP2_CRED = "c",
      .default = col_number()
    ),
    n_max = 1
  ) %>%
  janitor::clean_names(case = "all_caps") |>
  filter(FALSE)

# Region look-ups: re-code the survey's current-region fields to the standard
# PSSM region codes (T_Current_Region_PSSM_Codes), then roll them up
# (Rollup_Codes) and to the BC level (Rollup_Codes_BC).  Used throughout
# 02b-1/02b-2 for region-level outputs.
t_current_region_pssm_codes <-
  readr::read_csv(
    glue::glue(
      "{lan}/development/csv/gh-source/lookups/02/T_Current_Region_PSSM_Codes.csv"
    ),
    col_types = cols(.default = col_guess())
  ) %>%
  janitor::clean_names(case = "all_caps")

t_current_region_pssm_rollup_codes <-
  readr::read_csv(
    glue::glue(
      "{lan}/development/csv/gh-source/lookups/02/T_Current_Region_PSSM_Rollup_Codes.csv"
    ),
    col_types = cols(.default = col_guess())
  ) %>%
  janitor::clean_names(case = "all_caps")

t_current_region_pssm_rollup_codes_bc <- read_oracle_csv_auto(
  glue::glue(
    "{lan}/development/csv/gh-source/lookups/02/T_Current_Region_PSSM_Rollup_Codes_BC.csv"
  )
) %>%
  janitor::clean_names(case = "all_caps")

# NOC (National Occupational Classification) broad-category look-up.  Used in
# 02b-2-pssm-cohorts-new-labour-supply.R to build the labour supply
# distribution by broad occupation group.
t_noc_broad_categories <-
  read_oracle_csv_auto(
    glue::glue(
      "{lan}/development/csv/gh-source/lookups/02/T_NOC_Broad_Categories_Updated.csv"
    )
  ) %>%
  janitor::clean_names(case = "all_caps")


# Main DACSO survey responses, one row per respondent per survey cycle.
# Sub-cycle is COCI_SUBM_CD (C_Outc..); COCI_STQU_ID identifies the survey
# submission.  In 02b-1-pssm-cohorts.R this is joined to the credential,
# age and weight look-ups and becomes the DACSO block of T_Cohorts_Recoded.
if (regular_run == T | ptib_run == T) {
  t_dacso_data_part_1_stepa <- read_oracle_csv_auto(
    glue::glue(
      "{lan}/data/student-outcomes/csv/DACSO_Q003_DACSO_DATA_Part_1_stepA.csv"
    )
  )
  log_info(glue::glue(
    "Read DACSO_Q003_DACSO_DATA_Part_1_stepA.csv: {nrow(t_dacso_data_part_1_stepa)} rows"
  ))

  # Recode the survey's current-region fields into the standard PSSM region
  # codes (same scheme as APPSO/BGS so regions are comparable across cohorts).
  t_dacso_data_part_1_stepa <- t_dacso_data_part_1_stepa |>
    mutate(
      CURRENT_REGION_PSSM_CODE = case_when(
        TPID_CURRENT_REGION1 %in%
          c(1, 2, 3, 4, 5, 6, 7, 8) ~ TPID_CURRENT_REGION1,
        TPID_CURRENT_REGION4 == 5 ~ 9,
        TPID_CURRENT_REGION4 == 6 ~ 10,
        TPID_CURRENT_REGION4 == 7 ~ 11,
        TPID_CURRENT_REGION4 == 8 ~ -1,
        TRUE ~ NA_integer_
      )
    )
  log_info(glue::glue(
    "CURRENT_REGION_PSSM_CODE assigned: {nrow(t_dacso_data_part_1_stepa)} rows"
  ))
  # commenting these out for now - see PR
  #|>
  #mutate(
  #  TTRAIN = NA_integer_,
  #  LABR_EMPLOYED = NA_integer_,
  #  COSC_GRAD_STATUS_LGDS_CD = NA_integer_,
  #  COSC_GRAD_STATUS_LGDS_CD_GROUP = NA_integer_,
  #  RESPONDENT = NA_integer_,
  #)
}
## ------------------------------------ Clean Up --------------------------------------------------
# Current workflow:
#  - Write key tables back to sql server.  These are tables needed for downstream work, or tables
# that might be needed for later reference outside of this analysis.
#  - Close DB connections
#  - Remove all objects at the end of each script.
## ------------------------------------------------------------------------------------------------

tables_to_keep <- c(
  "t_dacso_data_part_1_stepa",
  "infoware_c_outc_clean_short_resp",
  "tbl_age_groups",
  "tbl_age_groups_rollup",
  "tbl_age",
  "t_pssm_credential_grouping",
  "t_year_survey_year",
  "t_cohorts_recoded",
  "t_current_region_pssm_codes",
  "t_current_region_pssm_rollup_codes",
  "t_current_region_pssm_rollup_codes_bc",
  "t_noc_broad_categories"
)

# Write each kept table to SQL Server as <name>_r.  write_table_to_db lives in
# R/utils.R (sourced by run-data-loading.R / the calling runner).
walk(tables_to_keep, write_table_to_db, schema = write_schema, con = con)
log_info(glue::glue(
  "Written to SQL Server ({write_schema}): {paste0(tables_to_keep, '_r', collapse = ', ')}"
))

dbDisconnect(con)
log_info("Disconnected from SQL Server database")
log_info("==== load-cohort-dacso.R END ====")
