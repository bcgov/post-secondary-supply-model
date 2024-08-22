# This script prepares student outcomes data for students who were formerly enrolled in 
# a trades program (i.e. an apprenticeship, trades foundation program or trades-related vocational program)
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
source(glue::glue("{lan}/development/sql/gh-source/02b-pssm-cohorts/02b-pssm-cohorts-trd.R"))

# ---- Connection to decimal ----
db_config <- config::get("decimal")
decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")

# ---- Check for required data tables ----
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Q000_TRD_Graduates"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_TRD_DATA"')))

# ---- Execute SQL ----
# Applies weight for model year and derives New Labour Supply
dbExecute(decimal_con, "ALTER TABLE t_TRD_data ADD Age_Group FLOAT NULL;")
dbExecute(decimal_con, "ALTER TABLE t_TRD_data ADD Age_Group_Rollup FLOAT NULL;")
dbExecute(decimal_con, Q000_TRD_Q003c_Derived_And_Weights)

# Refresh trd survey records in T_Cohorts_Recoded
dbExecute(decimal_con, Q000_TRD_Q005_1b1_Delete_Cohort)
dbExecute(decimal_con, Q000_TRD_Q005_DACSO_DATA_Part_1b2_Cohort_Recoded)

# ---- Clean Up ----
dbDisconnect(decimal_con)
dbExecute(decimal_con, "DROP TABLE T_TRD_DATA")

# ---- For future workflow ----
dbExists(decimal_con, "Q000_TRD_Graduates")



