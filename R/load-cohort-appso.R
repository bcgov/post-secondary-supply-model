# This script loads student outcomes data for students who students who have completed the 
# final year of their apprenticeship technical training within the first year of graduation.
# 
# The following data is read into SQL server from the student outcomes survey database:
#   T_APPSO_DATA_Final: unique survey responses for each person/survey year  (a few duplicates)
#   APPSO_Graduates: a count of graduates by credential type, age and survey year
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
my_schema <- config::get("myschema")

# ---- Connection to outcomes ----
jdbcDriver <- JDBC(driverClass = jdbc_driver_config$class,
                   classPath = jdbc_driver_config$path)

outcomes_con <- dbConnect(drv = jdbcDriver, 
                 url = db_config$url,
                 user = db_config$user,
                 password = db_config$password)

# ---- Read outcomes data ----
source(glue::glue("./sql/02b-pssm-cohorts/appso-data.sql"))

T_APPSO_DATA_Final <- dbGetQuery(outcomes_con, APPSO_DATA_01_Final)
APPSO_Graduates_dat <- dbGetQuery(outcomes_con, APPSO_Graduates)

# Convert some variables that should be numeric
T_APPSO_DATA_Final <- T_APPSO_DATA_Final %>% 
  mutate(TTRAIN = as.numeric(TTRAIN))

# Make sure this is updated to only the last 6 years of data 
T_APPSO_DATA_Final <-
  T_APPSO_DATA_Final %>% 
  mutate(CURRENT_REGION_PSSM_CODE =  case_when (
    CURRENT_REGION1 %in% 1:8 ~ CURRENT_REGION1, 
    CURRENT_REGION4 == 5 ~ 9,
    CURRENT_REGION4 == 6 ~ 10,
    CURRENT_REGION4 == 7 ~ 11,
    CURRENT_REGION4 == 8 ~ -1,
    TRUE ~ NA)) %>%
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
    TRUE ~ NA)) %>%
  mutate(AGE_GROUP = case_when (
    APP_AGE_AT_SURVEY %in% 17:19 ~ 2,
    APP_AGE_AT_SURVEY %in% 20:24 ~ 3,
    APP_AGE_AT_SURVEY %in% 25:29 ~ 4,
    APP_AGE_AT_SURVEY %in% 30:34 ~ 5,
    APP_AGE_AT_SURVEY %in% 35:44 ~ 6,
    APP_AGE_AT_SURVEY %in% 45:54 ~ 7,
    APP_AGE_AT_SURVEY %in% 55:64 ~ 8,
    TRUE ~ NA)) %>%
  # check that these years are correct
  # TODO: this moved out of query for derived weights  but means an extra step for QI - move back to query design?
  mutate(WEIGHT = case_when (
    SUBM_CD == 'C_Outc19' ~ 2,
    SUBM_CD == 'C_Outc20' ~ 3,
    SUBM_CD == 'C_Outc21' ~ 4,
    SUBM_CD == 'C_Outc22' ~ 5,
    SUBM_CD == 'C_Outc23' ~ 0,
    TRUE ~ 0)) %>%
  mutate(NEW_LABOUR_SUPPLY = case_when(
    APP_LABR_EMPLOYED == 1 ~ 1,
    APP_LABR_IN_LABOUR_MARKET == 1 & APP_LABR_EMPLOYED == 0 ~ 1,
    APP_LABR_EMPLOYED == 0 ~ 0,
    RESPONDENT == '1' ~ 0,
    TRUE ~ 0))

# prepare graduate dataset
APPSO_Graduates_dat  %>%
  mutate(AGE_GROUP = case_when (
    APP_AGE_AT_SURVEY %in% 15:16 ~ "15 to 16",
    APP_AGE_AT_SURVEY %in% 17:19 ~ "17 to 19",
    APP_AGE_AT_SURVEY %in% 20:24 ~ "20 to 24",
    APP_AGE_AT_SURVEY %in% 25:29 ~ "25 to 29",
    APP_AGE_AT_SURVEY %in% 30:34 ~ "30 to 34",
    APP_AGE_AT_SURVEY %in% 35:44 ~ "35 to 44",
    APP_AGE_AT_SURVEY %in% 45:54 ~ "45 to 54",
    APP_AGE_AT_SURVEY %in% 55:64 ~ "55 to 64",
    APP_AGE_AT_SURVEY %in% 65:89 ~ "65 to 89",
    TRUE ~ NA)) -> APPSO_Graduates_dat 

# ---- Connection to decimal ----
db_config <- config::get("decimal")
decimal_con <- dbConnect(odbc::odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")


dbWriteTable(decimal_con, 
             name = SQL(glue::glue('"{my_schema}"."T_APPSO_DATA_Final"')), 
             value = T_APPSO_DATA_Final,
             overwrite = TRUE)


dbWriteTable(decimal_con, 
             name = SQL(glue::glue('"{my_schema}"."APPSO_Graduates"')),
             value = APPSO_Graduates_dat, 
             overwrite = TRUE)


dbDisconnect(decimal_con)
dbDisconnect(outcomes_con)

