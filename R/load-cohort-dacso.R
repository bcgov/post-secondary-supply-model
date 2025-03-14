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



# This script loads student outcomes data for students who students who recently graduated after
# completing programs at public colleges, institutes, and teaching-intensive universities (~18 months prior)
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
library(RODBC)
library(config)
library(DBI)

# ---- Configure LAN and file paths ----
lan <- config::get("lan")

# ---- Connection to decimal ----
db_config <- config::get("decimal")
decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")
# should specify the DBO schema for final run, individual IDIRS for testing
my_schema <- config::get("myschema")

# ---- retrieve data from decimal ----
infoware_c_outc_clean_short_resp_dat <- dbReadTable(decimal_con, SQL(glue::glue('"{my_schema}"."infoware_c_outc_clean_short_resp_raw"')))

# ---- Read raw data from LAN ----
tbl_Age_Groups <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/02/tbl_Age_Groups.csv"), col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")
tbl_Age_Groups_Rollup <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/02/tbl_Age_Groups_Rollup.csv"), col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")
tbl_Age <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/02/tbl_Age.csv"), col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")
T_PSSM_Credential_Grouping <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/02/T_PSSM_Credential_Grouping.csv"), col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")
t_year_survey_year <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/02/t_year_survey_year.csv"), col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")
t_cohorts_recoded <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/rollover/02/T_Cohorts_Recoded.csv"), 
                  col_types = cols(PEN = "c", STQU_ID = "c", Survey = "c", LCIP_CD = "c", LCP4_CD = "c", NOC_CD = "c", INST_CD = "c",
                                   PSSM_Credential = "c",  PSSM_CRED = "c", LCIP4_CRED= "c",  LCIP2_CRED = "c", .default = col_number()), n_max = 1) %>%
  janitor::clean_names(case = "all_caps")
t_current_region_pssm_codes <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/02/T_Current_Region_PSSM_Codes.csv"), col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")
t_current_region_pssm_rollup_codes <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/02/T_Current_Region_PSSM_Rollup_Codes.csv"), col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")
t_current_region_pssm_rollup_codes_bc <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/02/T_Current_Region_PSSM_Rollup_Codes_BC.csv"), col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")
T_NOC_Broad_Categories <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/02/T_NOC_Broad_Categories_Updated.csv"), col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps") 

# ---- Write LAN data to decimal ----
# Note: may want to check if table exists instead of using overwrite = TRUE
dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."tbl_Age_Groups"')), value = tbl_Age_Groups, overwrite = TRUE)
dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."tbl_Age_Groups_Rollup"')), value = tbl_Age_Groups_Rollup, overwrite = TRUE)
dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."tbl_Age"')), value = tbl_Age, overwrite = TRUE)
dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."T_PSSM_Credential_Grouping"')), value = T_PSSM_Credential_Grouping, overwrite = TRUE)

dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."T_NOC_Broad_Categories"')), value = T_NOC_Broad_Categories, overwrite = TRUE) 
dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."t_year_survey_year"')), value = t_year_survey_year, overwrite = TRUE)
dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."t_cohorts_recoded"')), value = t_cohorts_recoded, overwrite = TRUE)
dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."t_current_region_pssm_codes"')), value = t_current_region_pssm_codes, overwrite = TRUE)
dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."t_current_region_pssm_rollup_codes"')), value = t_current_region_pssm_rollup_codes, overwrite = TRUE)
dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."t_current_region_pssm_rollup_codes_bc"')), value = t_current_region_pssm_rollup_codes_bc, overwrite = TRUE)

# --- Read SO DACSO data and write to decimal ----

if (regular_run == T | ptib_run == T) {
t_dacso_data_part_1_stepa <- dbReadTable(decimal_con, SQL(glue::glue('"{my_schema}"."DACSO_Q003_DACSO_DATA_Part_1_stepA_raw"')))
dbWriteTableArrow(decimal_con, name = SQL(glue::glue('"{my_schema}"."infoware_c_outc_clean_short_resp"')), infoware_c_outc_clean_short_resp_dat, overwrite = TRUE)

dbWriteTableArrow(decimal_con, name = SQL(glue::glue('"{my_schema}"."t_dacso_data_part_1_stepa"')), value = t_dacso_data_part_1_stepa, overwrite = TRUE)

dbExecute(decimal_con, "ALTER TABLE t_dacso_data_part_1_stepa ADD CURRENT_REGION_PSSM_CODE FLOAT NULL")
dbExecute(decimal_con,"UPDATE t_dacso_data_part_1_stepa
                       SET CURRENT_REGION_PSSM_CODE =  
                          CASE
                            WHEN TPID_CURRENT_REGION1 IN (1,2,3,4,5,6,7,8) THEN TPID_CURRENT_REGION1
                            WHEN TPID_CURRENT_REGION4 = 5 THEN 9
                            WHEN TPID_CURRENT_REGION4 = 6 THEN 10
                            WHEN TPID_CURRENT_REGION4 = 7 THEN 11
                            WHEN TPID_CURRENT_REGION4 = 8 THEN -1
                            ELSE NULL
                            END;")

dbExecute(decimal_con, "ALTER TABLE t_dacso_data_part_1_stepa ALTER COLUMN TTRAIN INT NULL")
dbExecute(decimal_con, "ALTER TABLE t_dacso_data_part_1_stepa ALTER COLUMN LABR_EMPLOYED INT NULL")
dbExecute(decimal_con, "ALTER TABLE t_dacso_data_part_1_stepa ALTER COLUMN COSC_GRAD_STATUS_LGDS_CD INT NULL") # check these
dbExecute(decimal_con, "ALTER TABLE t_dacso_data_part_1_stepa ALTER COLUMN COSC_GRAD_STATUS_LGDS_CD_GROUP INT NULL") # check these
dbExecute(decimal_con, "ALTER TABLE t_dacso_data_part_1_stepa ALTER COLUMN RESPONDENT INT NULL")
rm(t_dacso_data_part_1_stepa)
gc()
}

dbExecute(decimal_con, "ALTER TABLE T_NOC_Broad_Categories ALTER COLUMN BROAD_CATEGORY_CODE NVARCHAR(50) NULL;")
dbExecute(decimal_con, "ALTER TABLE T_NOC_Broad_Categories ALTER COLUMN MAJOR_GROUP_CODE NVARCHAR(50) NULL;")
dbExecute(decimal_con, "ALTER TABLE T_NOC_Broad_Categories ALTER COLUMN SUB_MAJOR_GROUP_CODE NVARCHAR(50) NULL;")
dbExecute(decimal_con, "ALTER TABLE T_NOC_Broad_Categories ALTER COLUMN MINOR_GROUP_CODE NVARCHAR(50) NULL;")
dbExecute(decimal_con, "ALTER TABLE T_NOC_Broad_Categories ALTER COLUMN UNIT_GROUP_CODE NVARCHAR(50) NULL;")


# ---- Clean Up ---
dbDisconnect(decimal_con)
# rm(list = ls())




                