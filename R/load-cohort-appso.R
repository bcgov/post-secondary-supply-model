# ---- Required Tables ----
# Primary Outcomes tables: See raw data documentation
# tbl_age
# tbl_age_groups
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
source(glue("{lan}/data/student-outcomes/sql/appso-data.sql"))

T_APPSO_DATA_Final <- dbGetQuery(outcomes_con, APPSO_DATA_01_Final)
APPSO_Graduates <- dbGetQuery(outcomes_con, APPSO_Graduates)

# Convert some variables that should be numeric
T_APPSO_DATA_Final <- T_APPSO_DATA_Final %>% 
  mutate(TTRAIN = as.numeric(TTRAIN))

# ---- Read LAN data ----
# Lookups
tbl_Age <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/tbl_Age.csv"), col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")

tbl_Age_Groups2 <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/tbl_Age_Groups2.csv"), col_types = cols(.default = col_guess())) %>%
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

