# This script loads student outcomes data for students who students who recently graduated with a 
# Baccalaureate degree (Baccalaureate students are surveyed two years after graduation)
#
# The following data set is read into SQL server from the student outcomes survey database:
#   bgs_current_region_data: postal and region codes, unique for each person/survey year
#   appso_current_region_data: postal and region codes, unique for each person/survey year
#   trd_current_region_data: postal and region codes, unique for each person/survey year
#   dacso_current_region_data: postal and region codes, unique for each person/survey year
#
# The following data sets are read into SQL server from the LAN:
#   tmp_bgs_inst_region_cds: look-up used to re-code several institution codes
#
# Notes: ** Adjust years to include 2022 and 2023 when engineering 2023 data **
# trd_current_region_data: included all years for 2019 model run as this was the first year geocoding was done  
# bgs_current_region_data: no longer have a region3 code in SO tables so handled a little differently

library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(RJDBC)


# ---- Configure LAN and file paths ----
db_config <- config::get("pdbtrn")
jdbc_driver_config <- config::get("jdbc")
lan <- config::get("lan")
source(glue::glue("{lan}/development/sql/gh-source/02b-pssm-cohorts/geocoding.R"))

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
tmp_bgs_inst_region_cds <- 
  readr::read_csv(glue::glue("{lan}/data/student-outcomes/csv/tmp_BGS_INST_REGION_CDS.csv"), 
                  col_types = cols(Current_Region_PSSM = "d", .default = col_character())) %>%
  janitor::clean_names(case = "all_caps")
dbWriteTable(decimal_con, name = "tmp_bgs_inst_region_cds", value = tmp_bgs_inst_region_cds)

# --- Read Geoding data from SO and write to decimal ----
# trd geocoding 
trd_current_region_data <- dbGetQuery(outcomes_con, qry_000_TRD_Current_Region_Data)
dbWriteTable(decimal_con, name = "trd_current_region_data", value = trd_current_region_data)

# appso geocoding
appso_current_region_data <- dbGetQuery(outcomes_con, qry_APPSO_Current_Region_Data)
dbWriteTable(decimal_con, name = "appso_current_region_data_update", value = appso_current_region_data)
dbExecute(decimal_con, "ALTER TABLE appso_current_region_data_update ALTER COLUMN RESPONDENT FLOAT NULL")

# dacso geocoding
dacso_current_region_data <- dbGetQuery(outcomes_con, qry_DACSO_Current_Region_Data)
dbWriteTable(decimal_con, name = "dacso_current_region_data", value = dacso_current_region_data)

# bgs geocoding
bgs_current_region_data <- dbGetQuery(outcomes_con, qry_BGS_00_Append)
new_postal <- dbGetQuery(outcomes_con, qry_BGS_00_NEW_POSTAL)
bgs_current_region_data <- bgs_current_region_data %>% 
  left_join(new_postal, by = join_by(STQU_ID, SURVEY_YEAR)) %>% 
  select(-NEW_POST)
dbWriteTable(decimal_con, name = "bgs_current_region_data_update", value = bgs_current_region_data)

# ---- Clean Up ---
dbDisconnect(outcomes_con)
dbDisconnect(decimal_con)


                