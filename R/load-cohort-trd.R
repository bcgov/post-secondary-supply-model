# This script loads student outcomes data for students who students who were formerly enrolled in 
# a trades program (i.e. an apprenticeship, trades foundation program or trades-related vocational program)
#
# The following data sets are read into SQL server from the student outcomes survey database:
#   Q000_TRD_DATA_01: unique survey responses for each person/survey year (a few duplicates)
#   Q000_TRD_Graduates: a count of graduates by credential type, age and survey year
#
# Notes: Age group labels are assigned.  Note there are two different groupings used to group students by age in the model.

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

# ---- Read raw data and disconnect ----
source(glue("{lan}/data/student-outcomes/sql/trd-data.sql"))

Q000_TRD_DATA_01 <- dbGetQuery(outcomes_con, Q000_TRD_DATA_01)
Q000_TRD_Graduates <- dbGetQuery(outcomes_con, Q000_TRD_Graduates)

# Convert some variables that should be numeric
Q000_TRD_DATA_01 <- Q000_TRD_DATA_01 %>% 
  mutate(GRADSTAT = as.numeric(GRADSTAT), 
         KEY = as.numeric(KEY),  
         TTRAIN = as.numeric(TTRAIN))

# Gradstat group : couldn't find in outcomes data so defining here.
Q000_TRD_DATA_01 <- Q000_TRD_DATA_01 %>% 
  mutate(LCIP4_CRED = paste0(GRADSTAT_GROUP, ' - ' , LCIP_LCP4_CD , ' - ' , TTRAIN , ' - ' , PSSM_CREDENTIAL))

# prepare graduate dataset
Q000_TRD_Graduates   %>%
  mutate(AGE_GROUP_LABEL = case_when (
    TRD_AGE_AT_SURVEY %in% 15:16 ~ "15 to 16",
    TRD_AGE_AT_SURVEY %in% 17:19 ~ "17 to 19",
    TRD_AGE_AT_SURVEY %in% 20:24 ~ "20 to 24",
    TRD_AGE_AT_SURVEY %in% 25:29 ~ "25 to 29",
    TRD_AGE_AT_SURVEY %in% 30:34 ~ "30 to 34",
    TRD_AGE_AT_SURVEY %in% 35:44 ~ "35 to 44",
    TRD_AGE_AT_SURVEY %in% 45:54 ~ "45 to 54",
    TRD_AGE_AT_SURVEY %in% 55:64 ~ "55 to 64",
    TRD_AGE_AT_SURVEY %in% 65:89 ~ "65 to 89",
    TRUE ~ NA)) -> Q000_TRD_Graduates 


# ---- Connection to decimal ----
db_config <- config::get("decimal")
decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")
dbWriteTable(decimal_con, name = "T_TRD_DATA", value = Q000_TRD_DATA_01)
dbWriteTable(decimal_con, name = "Q000_TRD_Graduates", value = Q000_TRD_Graduates)

dbDisconnect(decimal_con)
dbDisconnect(outcomes_con)
