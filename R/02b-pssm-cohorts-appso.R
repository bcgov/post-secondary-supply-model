# This script prepares student outcomes data for the following student surveys:
#  APP: students who have completed the final year of their apprenticeship technical training within the first year of graduation.
# 
#  APP:
#     Assumes - geocoding has been done, and CURRENT_REGION_PSSM_CODE contains final region code to use
#             - year weights for model have been added
#             - New Labour Supply has been calculated
#             - Age and age group have been added + a new student id
#     Refreshes survey records in T_Cohorts_Recoded


library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(RJDBC)

# ---- Configure LAN and file paths ----
db_config <- config::get("decimal")
lan <- config::get("lan")
my_schema <- config::get("myschema")
source(glue::glue("{lan}/development/sql/gh-source/02b-pssm-cohorts/02b-pssm-cohorts-appso.R"))

# ---- Connection to decimal ----
db_config <- config::get("decimal")
decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")

# ---- Check for required data tables ----
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_APPSO_DATA_Final"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."APPSO_Graduates"')))

# ---- Execute SQL ----
# Refresh survey records in T_Cohorts_Recoded
dbExecute(decimal_con, APPSO_Q005_1b1_Delete_Cohort)
dbExecute(decimal_con, APPSO_Q005_DACSO_DATA_Part_1b2_Cohort_Recoded)

# ---- Clean Up ----
dbDisconnect(decimal_con)
dbExecute(decimal_con, "DROP TABLE T_APPSO_DATA_Final")

# ---- For future workflow ----
dbExistsTable(decimal_con, "APPSO_Graduates")


