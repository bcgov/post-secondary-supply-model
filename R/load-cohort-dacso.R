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
# The following data-set is read into SQL server from the student outcomes survey database:
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
library(RJDBC)


# ---- Configure LAN and file paths ----
db_config <- config::get("pdbtrn")
jdbc_driver_config <- config::get("jdbc")
lan <- config::get("lan")
source(glue::glue("./sql/02b-pssm-cohorts/dacso-data.sql"))

# ---- Connection to outcomes ----
jdbcDriver <- JDBC(driverClass = jdbc_driver_config$class,
                   classPath = jdbc_driver_config$path)

outcomes_con <- dbConnect(drv = jdbcDriver, 
                          url = db_config$url,
                          user = db_config$user,
                          password = db_config$password)

# ---- Connection to decimal ----
db_config <- config::get("decimal")
decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")

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

# --- Required lookup - read directly from SSMS 
#tbl_noc_skill_level_aged_17_34 <- 
#  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/02/tbl_NOC_Skill_Level_Aged_17_34.csv"), col_types = cols(.default = col_guess())) %>%
#  janitor::clean_names(case = "all_caps")


# ---- Write LAN data to decimal ----
# Note: may want to check if table exists instead of using overwrite = TRUE
dbWriteTable(decimal_con, name = "tbl_Age_Groups", value = tbl_Age_Groups)
dbWriteTable(decimal_con, name = "tbl_Age_Groups_Rollup", value = tbl_Age_Groups_Rollup)
dbWriteTable(decimal_con, name = "tbl_Age", value = tbl_Age)
dbWriteTable(decimal_con, name = "T_PSSM_Credential_Grouping", value = T_PSSM_Credential_Grouping)

# load via SQL Server: 
# dbWriteTable(decimal_con, name = "tbl_noc_skill_level_aged_17_34", value = tbl_noc_skill_level_aged_17_34) 
dbWriteTable(decimal_con, name = "t_year_survey_year", value = t_year_survey_year)
dbWriteTable(decimal_con, name = "t_cohorts_recoded", value = t_cohorts_recoded)
dbWriteTable(decimal_con, name = "t_current_region_pssm_codes", value = t_current_region_pssm_codes)
dbWriteTable(decimal_con, name = "t_current_region_pssm_rollup_codes", value = t_current_region_pssm_rollup_codes)
dbWriteTable(decimal_con, name = "t_current_region_pssm_rollup_codes_bc", value = t_current_region_pssm_rollup_codes_bc)

# --- Read SO dacso data and write to decimal ----
t_dacso_data_part_1_stepa <- dbGetQueryArrow(outcomes_con, DACSO_Q003_DACSO_DATA_Part_1_stepA)
dbWriteTableArrow(decimal_con, name = "t_dacso_data_part_1_stepa", value = t_dacso_data_part_1_stepa)
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

# tictoc::tic()
# infoware_c_outc_clean_short_resp_dat <- dbGetQueryArrow(outcomes_con, infoware_c_outc_clean_short_resp)
# tictoc::toc()
# 
# tictoc::tic()
# dbWriteTableArrow(decimal_con, name = "infoware_c_outc_clean_short_resp", value = infoware_c_outc_clean_short_resp_dat)
# tictoc::toc()

## year by year? 
year_start <- 18
year_end <- 23
for (year in year_start:year_end){
  print(glue::glue('Starting year {year}'))
  sql_by_year <- glue::glue("
  SELECT *
  FROM c_outc_clean_short_resp
  WHERE subm_cd = 'C_Outc{year}'
  ")
  tictoc::tic()
  tmp <- dbGetQueryArrow(outcomes_con, sql_by_year)
  print('Loaded year to R.')
  tictoc::toc()
  
  tictoc::tic()
  dbWriteTableArrow(decimal_con, name = glue::glue("infoware_by_year_{year}"), value = tmp)
  print('Loaded year to MS SQL.')
  tictoc::toc()
  print('')
  print('')
  
  rm(tmp)
  gc()
}

full_sql <- "
SELECT *
INTO infoware_c_outc_clean_short_resp 
FROM (
"

for (year in year_start:year_end){
  full_sql <- paste0(full_sql, glue::glue("SELECT * FROM infoware_by_year_{year}
"
  ))
  
  if (year<year_end){
    full_sql <- paste0(full_sql, "
UNION ALL
")
  } else{
    full_sql <- paste0(full_sql, "
) t1")
  }
}

cat(full_sql)

# combine
dbExecute(decimal_con, full_sql)

# confirm that the count by year is the same in both databases
dbGetQuery(
  outcomes_con,
  "SELECT subm_cd, count(*)
  FROM c_outc_clean_short_resp
  WHERE subm_cd IN ('C_Outc18', 'C_Outc19','C_Outc20','C_Outc21','C_Outc22','C_Outc23')
  GROUP BY subm_cd
  ORDER BY 1"
)

dbGetQuery(
  decimal_con,
  "SELECT subm_cd, count(*)
  FROM infoware_c_outc_clean_short_resp
  GROUP BY subm_cd
  ORDER BY 1"
)

# drop tmp
for (year in year_start:year_end){
  dbExecute(decimal_con, glue::glue("DROP TABLE infoware_by_year_{year}"))
}


# ---- Clean Up ---
dbDisconnect(outcomes_con)
dbDisconnect(decimal_con)




                