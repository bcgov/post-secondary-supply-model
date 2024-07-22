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
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."APPSO_DATA_01_Final"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."APPSO_Graduates"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."appso_current_region_data"')))

# ---- Execute SQL ----
# updates CURRENT_REGION_PSSM_CODE after the geocoding.
dbExecute(decimal_con, APPSO_Q003b_Add_CURRENT_REGION_PSSM) # Not sure we need this
dbExecute(decimal_con, APPSO_Q003b_Add_CURRENT_REGION_PSSM2)

# Applies weight for model year and derives New Labour Supply
dbExecute(decimal_con, "ALTER TABLE t_appso_data_final ADD New_Labour_Supply INT NULL;")
dbExecute(decimal_con, APPSO_Q003c_Derived_And_Weights)

# Refresh bgs survey records in T_Cohorts_Recoded
dbExecute(decimal_con, APPSO_Q005_1b1_Delete_Cohort)
dbExecute(decimal_con, APPSO_Q005_DACSO_DATA_Part_1b2_Cohort_Recoded)

# ---- Clean Up ----
dbDisconnect(decimal_con)



