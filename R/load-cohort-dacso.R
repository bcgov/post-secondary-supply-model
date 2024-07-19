library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(RJDBC)


# ---- Configure LAN and file paths ----
db_config <- config::get("pdbtrn")
jdbc_driver_config <- config::get("jdbc")
lan <- config::get("lan")
source(glue::glue("{lan}/data/student-outcomes/sql/dacso-data.sql"))

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
# Note: some of these tables are derived and we need to figure out where they come from.  Are they rolled over year after year?
# tbl_Age_Groups: a static crosswalk for aligning ages, groups and labels across queries and datasets
# tbl_Age: static lookup maps age to age group in table tbl_Age_Groups
# T_PSSM_Credential_Grouping: a static table for relabeling credential names
# T_year_survey_year: carried forward from last models run and updated with new data. Will need to add a step to update this somewhere in the workflow.
# dacso_current_region_data: derived in geocoding workflow and will be saved to db in future.
# T_Cohorts_Recoded:  initially uploaded in this script, it contains survey records for all years.  But the table is refreshed in the workflow
# so can instead just be created each year.  See comments in 02b-pssm-cohots-dacso.R

tbl_Age_Groups <- 
  readr::read_csv(glue::glue("{lan}/data/student-outcomes/csv/tbl_Age_Groups.csv"), col_types = cols(.default = col_character())) %>%
  janitor::clean_names(case = "all_caps")
tbl_Age <- 
  readr::read_csv(glue::glue("{lan}/data/student-outcomes/csv/tbl_Age.csv"), col_types = cols(.default = col_character())) %>%
  janitor::clean_names(case = "all_caps")
T_PSSM_Credential_Grouping <- 
  readr::read_csv(glue::glue("{lan}/data/student-outcomes/csv/T_PSSM_Credential_Grouping.csv"), col_types = cols(.default = col_character())) %>%
  janitor::clean_names(case = "all_caps")
dacso_current_region_data <- 
  readr::read_csv(glue::glue("{lan}/data/student-outcomes/csv/dacso_current_region_data.csv"), col_types = cols(.default = col_character())) %>%
  janitor::clean_names(case = "all_caps")
t_year_survey_year <- 
  readr::read_csv(glue::glue("{lan}/data/student-outcomes/csv/t_year_survey_year.csv"), col_types = cols(.default = col_character())) %>%
  janitor::clean_names(case = "all_caps")
t_cohorts_recoded <- 
  readr::read_csv(glue::glue("{lan}/data/student-outcomes/csv/T_Cohorts_Recoded.csv"), col_types = cols(.default = col_character())) %>%
  janitor::clean_names(case = "all_caps")

# ---- Write LAN data to decimal ----
# Note: may want to check if table exists instead of using overwrite = TRUE
dbWriteTable(decimal_con, name = "tbl_Age_Groups", value = tbl_Age_Groups, overwrite = TRUE)
dbWriteTable(decimal_con, name = "tbl_Age", value = tbl_Age, overwrite = TRUE)
dbWriteTable(decimal_con, name = "T_PSSM_Credential_Grouping", value = T_PSSM_Credential_Grouping, overwrite = TRUE)
dbWriteTable(decimal_con, name = "dacso_current_region_data", value = dacso_current_region_data, overwrite = TRUE)
dbWriteTable(decimal_con, name = "t_year_survey_year", value = t_year_survey_year, overwrite = TRUE)
dbWriteTable(decimal_con, name = "t_cohorts_recoded", value = t_cohorts_recoded, overwrite = TRUE)

# --- Read SO dacso data and write to decimal ----
t_dacso_data_part_1_stepa <- dbGetQueryArrow(outcomes_con, DACSO_Q003_DACSO_DATA_Part_1_stepA)
dbWriteTableArrow(decimal_con, name = "t_dacso_data_part_1_stepa", value = t_dacso_data_part_1_stepa)
rm(t_dacso_data_part_1_stepa)
gc()

infoware_c_outc_clean_short_resp <- dbGetQuery(outcomes_con, infoware_c_outc_clean_short_resp)
dbWriteTable(decimal_con, name = "infoware_c_outc_clean_short_resp", value = infoware_c_outc_clean_short_resp)
rm(t_dacso_data_part_1_stepa)
gc()

# ---- Clean Up ---
dbDisconnect(outcomes_con)
dbDisconnect(decimal_con)


                