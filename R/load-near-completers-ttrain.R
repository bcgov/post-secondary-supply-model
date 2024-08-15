library(tidyverse)
library(RODBC)
library(config)
library(glue)
library(DBI)
library(RJDBC)


# ---- Configure LAN and file paths ----
db_config <- config::get("pdbtrn")
jdbc_driver_config <- config::get("jdbc")
lan <- config::get("lan")

source(glue::glue("{lan}/data/student-outcomes/sql/qry_make_tmp_table_Age_step1.sql"))

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


# ---- Read LAN data ----
tmp_tbl_Age <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/testing/03/tmp_tbl_Age.csv"), col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")
stp_dacso_prgm_credential_lookup <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/STP_DACSO_PRGM_CREDENTIAL_LOOKUP.csv"), col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")
combine_creds <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/combine_creds.csv"), col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")
age_group_lookup <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/AgeGroupLookup.csv"), col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")
t_pssm_projection_cred_grp <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/T_PSSM_Projection_Cred_Grp.csv"), col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps") %>% 
  add_case(PSSM_PROJECTION_CREDENTIAL = 'UNIVERSITY TRANSFER', 
           PSSM_CREDENTIAL = 'ADGR OR UT', 
           PSSM_CREDENTIAL_NAME = 'Associate degree/University transfer', 
           COSC_GRAD_STATUS_LGDS_CD = 1)
t_dacso_data_part_1 <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/testing/03/t_dacso_data_part_1.csv"), col_types = cols(.default = col_guess())) %>%
                  janitor::clean_names(case = "all_caps")
tbl_Age <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/tbl_Age.csv"), col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")
tbl_Age_Groups <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/tbl_Age_Groups.csv"), col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")


# ---- Read SO data ----
tmp_tbl_Age_AppendNewYears <- dbGetQuery(outcomes_con, qry_make_tmp_table_Age_step1) # adjust query for correct year

# ---- Write to decimal ----
dbWriteTable(decimal_con, name = "tmp_tbl_Age_AppendNewYears", value = tmp_tbl_Age_AppendNewYears)
dbWriteTable(decimal_con, name = "tmp_tbl_Age", value = tmp_tbl_Age )
dbWriteTable(decimal_con, name = "tbl_Age", value = tbl_Age )
dbWriteTable(decimal_con, name = "tbl_Age_Groups", value = tbl_Age_Groups)
dbWriteTable(decimal_con, name = "combine_creds", value = combine_creds )
dbWriteTable(decimal_con, name = "stp_dacso_prgm_credential_lookup", value = stp_dacso_prgm_credential_lookup)
dbWriteTable(decimal_con, name = "t_pssm_projection_cred_grp", value = t_pssm_projection_cred_grp)
dbWriteTable(decimal_con, name = "t_dacso_data_part_1", value = t_dacso_data_part_1)

# ---- Clean up and disconnect ----
dbDisconnect(decimal_con)
dbDisconnect(outcomes_con)
gc()
rm(list = ls())




