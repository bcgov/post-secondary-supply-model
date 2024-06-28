library(tidyverse)
library(RODBC)
library(config)
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

# ---- Read raw data ----
source(glue::glue("{lan}/data/student-outcomes/sql/dacso-data.sql"))

tbl_Age_Groups <- readr::read_csv(glue::glue("{lan}/data/student-outcomes/csv/tbl_Age_Groups.csv"), col_types = cols(.default = col_character())) %>%
  janitor::clean_names(case = "all_caps")
tbl_Age <- readr::read_csv(glue::glue("{lan}/data/student-outcomes/csv/tbl_Age.csv"), col_types = cols(.default = col_character())) %>%
  janitor::clean_names(case = "all_caps")
T_PSSM_Credential_Grouping <- readr::read_csv(glue::glue("{lan}/data/student-outcomes/csv/T_PSSM_Credential_Grouping.csv"), col_types = cols(.default = col_character())) %>%
  janitor::clean_names(case = "all_caps")

# dacso data from primary tables
t_dacso_data_part_1_stepa <- dbGetQuery(outcomes_con, DACSO_Q003_DACSO_DATA_Part_1_stepA)
infoware_c_outc_clean_short_resp <- dbGetQueryArrow(outcomes_con, "SELECT * FROM c_outc_clean_short_resp")
infoware_c_outc_clean2 <- dbGetQueryArrow(outcomes_con, "SELECT * FROM c_outc_clean2")

# ---- Connection to decimal and load data ----
db_config <- config::get("decimal")
decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")
dbWriteTableArrow(decimal_con, name = "infoware_c_outc_clean2", value = infoware_c_outc_clean2)
dbWriteTable(decimal_con, name = "infoware_c_outc_clean_short_resp", value = infoware_c_outc_clean_short_resp)

dbDisconnect(decimal_con)


                