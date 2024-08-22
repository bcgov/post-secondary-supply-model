# This script loads student outcomes data for students who students who have completed the 
# final year of their apprenticeship technical training within the first year of graduation.
# 
# Generally, the script prepares survey data by:
#     Updating CURRENT_REGION_PSSM_CODE after the geocoding
#     Applies weight for model year and derives New Labour Supply
#     Refresh survey records in T_Cohorts_Recoded
#     adds age and age group, a new student id


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
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."appso_current_region_data"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."tbl_Age"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."tbl_Age_Groups"')))

# ---- Execute SQL ----
# Refresh survey records in T_Cohorts_Recoded
dbExecute(decimal_con, APPSO_Q005_1b1_Delete_Cohort)
dbExecute(decimal_con, APPSO_Q005_DACSO_DATA_Part_1b2_Cohort_Recoded)

# ---- Clean Up ----
dbDisconnect(decimal_con)
dbExecute(decimal_con, "DROP TABLE T_APPSO_DATA_Final")

# ---- For future workflow ----
dbExistsTable(decimal_con, "APPSO_Graduates")


