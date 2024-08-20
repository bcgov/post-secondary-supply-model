# This script loads student outcomes data for students who students who have completed the 
# final year of their apprenticeship technical training within the first year of graduation.
# 
# The following data is read into SQL server from the student outcomes survey database:
#   T_APPSO_DATA_Final: unique survey responses for each person/survey year  (a few duplicates)
#   APPSO_Graduates: a count of graduates by credential type, age and survey year
# 
# The following data are look-ups read into SQL Server from the LAN
#   tbl_Age: bins ages into groups (1-10)
#   tbl_Age_Groups2: used to assign a label to each age group.  
#
# Notes: Age group labels are assigned in the script.  There are two different groupings used to group students by age in the model.
#   but I think they are not needed?
  
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

# ---- Read outcomes data ----
source(glue::glue("{lan}/data/student-outcomes/sql/appso-data.sql"))

T_APPSO_DATA_Final <- dbGetQuery(outcomes_con, APPSO_DATA_01_Final)
APPSO_Graduates <- dbGetQuery(outcomes_con, APPSO_Graduates)

# Convert some variables that should be numeric
T_APPSO_DATA_Final <- T_APPSO_DATA_Final %>% 
  mutate(TTRAIN = as.numeric(TTRAIN))

# ---- Read LAN data ----
# Lookups
tbl_Age <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/02/tbl_Age.csv"), col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")

tbl_Age_Groups <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/02/tbl_Age_Groups.csv"), col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")

# prepare graduate dataset
APPSO_Graduates  %>%
  mutate(AGE_GROUP_LABEL = case_when (
    APP_AGE_AT_SURVEY %in% 15:16 ~ "15 to 16",
    APP_AGE_AT_SURVEY %in% 17:19 ~ "17 to 19",
    APP_AGE_AT_SURVEY %in% 20:24 ~ "20 to 24",
    APP_AGE_AT_SURVEY %in% 25:29 ~ "25 to 29",
    APP_AGE_AT_SURVEY %in% 30:34 ~ "30 to 34",
    APP_AGE_AT_SURVEY %in% 35:44 ~ "35 to 44",
    APP_AGE_AT_SURVEY %in% 45:54 ~ "45 to 54",
    APP_AGE_AT_SURVEY %in% 55:64 ~ "55 to 64",
    APP_AGE_AT_SURVEY %in% 65:89 ~ "65 to 89",
    TRUE ~ NA)) -> APPSO_Graduates 


# ---- Connection to decimal ----
db_config <- config::get("decimal")
decimal_con <- dbConnect(odbc::odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")

dbWriteTable(decimal_con, name = "T_APPSO_DATA_Final", value = T_APPSO_DATA_Final)
dbWriteTable(decimal_con, name = "APPSO_Graduates", value = APPSO_Graduates)

dbDisconnect(decimal_con)
dbDisconnect(outcomes_con)

