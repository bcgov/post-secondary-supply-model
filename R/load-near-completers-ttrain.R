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

# ---- Connection to outcomes ----
jdbcDriver <- JDBC(driverClass = jdbc_driver_config$class,
                   classPath = jdbc_driver_config$path)

outcomes_con <- dbConnect(drv = jdbcDriver, 
                          url = db_config$url,
                          user = db_config$user,
                          password = db_config$password)

# ---- Read raw data from LAN ----
# Note: tmp_tbl_Age is rolled over year after year and updated from SO tables. 
tmp_tbl_Age <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/03-tmp_tbl_Age.csv"), col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")
# stp_dacso_prgm_credential_lookup and combine_creds are lookups that don't change year to year
stp_dacso_prgm_credential_lookup <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/STP_DACSO_PRGM_CREDENTIAL_LOOKUP.csv"), col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")
combine_creds <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/combine_creds.csv"), col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")

# for testing of 2019 data only - remove as this table will be made in earlier workflow
t_dacso_data_part_1 <- 
  arrow::read_csv_arrow(glue::glue("{lan}/development/csv/gh-source/03-t_dacso_data_part_1.csv"), 
                  col_types = cols(COCI_PEN = "c", COSC_GRAD_STATUS_LGDS_CD_Group = "c", Respondent = "c", .default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")

# ---- Read SO data and disconnect ----
source(glue::glue("{lan}/data/student-outcomes/sql/qry_make_tmp_table_Age_step1.sql"))

tmp_tbl_Age_AppendNewYears <- dbGetQuery(outcomes_con, qry_make_tmp_table_Age_step1)

dbDisconnect(outcomes_con)

# ---- Connection to decimal ----
db_config <- config::get("decimal")
decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")

dbWriteTable(decimal_con, name = "tmp_tbl_Age_AppendNewYears", value = tmp_tbl_Age_AppendNewYears)
dbWriteTable(decimal_con, name = "tmp_tbl_Age", value = tmp_tbl_Age )
dbWriteTable(decimal_con, name = "combine_creds", value = combine_creds )
dbWriteTable(decimal_con, name = "stp_dacso_prgm_credential_lookup", value = stp_dacso_prgm_credential_lookup)
dbWriteTableArrow(decimal_con, name = "t_dacso_data_part_1", nanoarrow::as_nanoarrow_array(t_dacso_data_part_1arr))
dbDisconnect(decimal_con)




