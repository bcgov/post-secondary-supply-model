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

stp_dacso_prgm_credential_lookup <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/STP_DACSO_PRGM_CREDENTIAL_LOOKUP.csv"), col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")
combine_creds <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/combine_creds.csv"), col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")

t_pssm_projection_cred_grp <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/T_PSSM_Projection_Cred_Grp.csv"), col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps") %>% 
  add_case(PSSM_PROJECTION_CREDENTIAL = 'UNIVERSITY TRANSFER', 
           PSSM_CREDENTIAL = 'ADGR OR UT', 
           PSSM_CREDENTIAL_NAME = 'Associate degree/University transfer', 
           COSC_GRAD_STATUS_LGDS_CD = 1)
tbl_Age <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/tbl_Age.csv"), col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps") %>%
  mutate(AGE_GROUP= AGE_GROUP-1) %>%
  mutate(AGE_GROUP = if_else(AGE %in% 35:64, 5, AGE_GROUP))

age_group_lookup <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/AgeGroupLookup.csv"), col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps") %>% 
  filter(AGE_INDEX %in% 2:5) %>% 
  mutate(AGE_INDEX = AGE_INDEX -1) %>%
  add_case(AGE_INDEX = 5, AGE_GROUP = "35 to 64", LOWER_BOUND = 35, UPPER_BOUND = 64)


# ---- Testing Only ---- 
t_dacso_data_part_1 <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/testing/03/t_dacso_data_part_1.csv"), col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")
tmp_tbl_Age <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/testing/03/tmp_tbl_Age.csv"), col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")

# ---- Read SO data ----
tmp_tbl_Age_AppendNewYears <- dbGetQuery(outcomes_con, qry_make_tmp_table_Age_step1) # adjust query for correct year

# ---- Write to decimal ----
dbWriteTable(decimal_con, name = "tmp_tbl_Age_AppendNewYears", value = tmp_tbl_Age_AppendNewYears)
dbWriteTable(decimal_con, name = "tmp_tbl_Age", value = tmp_tbl_Age)
dbWriteTable(decimal_con, name = "tbl_Age", value = tbl_Age, overwrite = TRUE)
dbWriteTable(decimal_con, name = "combine_creds", value = combine_creds )
dbWriteTable(decimal_con, name = "stp_dacso_prgm_credential_lookup", value = stp_dacso_prgm_credential_lookup)
dbWriteTable(decimal_con, name = "t_pssm_projection_cred_grp", value = t_pssm_projection_cred_grp)
dbWriteTable(decimal_con, name = "AgeGroupLookup", age_group_lookup, overwrite = TRUE)


t_dacso_data_part_1_1 <- t_dacso_data_part_1 %>% slice(1:200000)
t_dacso_data_part_1_2 <- t_dacso_data_part_1 %>% slice(200001:376895)
dbWriteTable(decimal_con, name = "t_dacso_data_part_1", value = t_dacso_data_part_1_1)
dbWriteTable(decimal_con, name = "t_dacso_data_part_1_2", value = t_dacso_data_part_1_2)
dbExecute(decimal_con, "INSERT INTO t_dacso_data_part_1 SELECT * FROM t_dacso_data_part_1_2")
dbExecute(decimal_con, "DROP TABLE t_dacso_data_part_1_2")
dbExecute(decimal_con, "DELETE FROM t_dacso_data_part_1 WHERE COCI_SUBM_CD IN ('C_Outc19','C_Outc18','C_Outc06')")

# ---- Clean up and disconnect ----
dbDisconnect(decimal_con)
dbDisconnect(outcomes_con)
gc()
rm(list = ls())




