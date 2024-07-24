# Notes: watch for Age_Grouping variable, documentation mentions having removed it from earlier queries and linked later.  not sure what this means.

library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(RJDBC)

# ---- Configure LAN and file paths ----
db_config <- config::get("decimal")
lan <- config::get("lan")
my_schema <- config::get("myschema")
source(glue::glue("{lan}/development/sql/gh-source/02b-pssm-cohorts/02b-pssm-cohorts-bgs.R"))

# ---- Connection to decimal ----
db_config <- config::get("decimal")
decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")

# ---- Check for required data tables ----
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_BGS_Data_Final"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_BGS_INST_Recode"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_bgs_data_final_for_outcomesmatching2020"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_Weights"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."bgs_current_region_data"'))) 

# ---- Execute SQL ----
# Recode institution codes to be consistent to STP file
dbExecute(decimal_con, BGS_Q001b_INST_Recode)

# Note: update CIPS after program matching.  Some eyes needed to double check method.
dbExecute(decimal_con, BGS_Q001c_Update_CIPs_After_Program_Matching)
dbExecute(decimal_con, BGS_Q002_LCP4_CRED)

# updates CURRENT_REGION_PSSM_CODE after the geocoding.
# dbExecute(decimal_con, BGS_Q003b_Add_CURRENT_REGION_PSSM) # Not sure we need this
dbExecute(decimal_con, BGS_Q003b_Add_CURRENT_REGION_PSSM2)

# Applies weight for model year and derives New Labour Supply
dbExecute(decimal_con, "ALTER TABLE T_BGS_Data_Final ADD BGS_New_Labour_Supply INT NULL;")
dbExecute(decimal_con, BGS_Q003c_Derived_And_Weights)

# Refresh bgs survey records in T_Cohorts_Recoded
dbExecute(decimal_con, BGS_Q005_1b1_Delete_Cohort)
dbExecute(decimal_con, BGS_Q005_1b2_Cohort_Recoded)

# ---- Clean Up ----
dbDisconnect(decimal_con)
dbExecute(decimal_con, "DROP TABLE T_BGS_Data_Final")
dbExecute(decimal_con, "DROP TABLE bgs_current_region_data")



