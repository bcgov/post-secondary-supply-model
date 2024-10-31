# This script prepares student outcomes data for the following student surveys: TRD, APP, DACSO, BGS
#
# APP:
#     Assumes geocoding has been done, and CURRENT_REGION_PSSM_CODE contains final region code to use and
#       year weights for model have been added.  New Labour Supply has been calculated and
#       age and age group have been added + a new student id
#     Refreshes survey records in T_Cohorts_Recoded
#
# TRD:
#     Assumes geocoding has been done, and CURRENT_REGION_PSSM_CODE contains final region code to use and
#       New Labour Supply has been calculated
#     Refreshes survey records in T_Cohorts_Recoded
#     Adds year weights for model
#     Adds Age and age groups + a new student id
#
# DACSO:
#     Assumes geocoding has been done, and CURRENT_REGION_PSSM_CODE contains final region code to use
#     Recodes institution codes to be consistent to STP file
#     Update CIPS after program matching.
#     Applies weight for model year and derives New Labour Supply
#     Adds age and age group, a new student id
#     Refresh survey records in T_Cohorts_Recoded
#
# BGS:
#     Assumes geocoding has been done, and CURRENT_REGION_PSSM_CODE contains final region code to use
#     Recodes institution codes to be consistent to STP file
#     Updates CIPS after program matching.
#     Applies weight for model year and derives New Labour Supply
#     Adds age and age group, a new student id
#     Refreshes survey records in T_Cohorts_Recoded
#
#     Notes: double check method for updating CIP codes after program matching.
#     There is a query to check for invalid NOC codes (see documentation).
#     Update T-Year_Survey_Year and T_weights (for all cohorts)
#     2006 dacso all NULL lcip-4-creds (remove 2006)
# 


library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(RJDBC)

# ---- Configure LAN and file paths ----
db_config <- config::get("decimal")
lan <- config::get("lan")
my_schema <- config::get("myschema")


# ---- Query Defs ----
source("./sql/02b-pssm-cohorts/02b-pssm-cohorts-trd.R")
source("./sql/02b-pssm-cohorts/02b-pssm-cohorts-appso.R")
source("./sql/02b-pssm-cohorts/02b-pssm-cohorts-bgs.R")
source("./sql/02b-pssm-cohorts/02b-pssm-cohorts-dacso.R")

# ---- Connection to decimal ----
db_config <- config::get("decimal")
decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")

# ---- Check for required data tables ----
# TRD
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."TRD_Graduates"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_TRD_DATA"')))

# APP
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_APPSO_DATA_Final"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."APPSO_Graduates"')))

# BGS
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_BGS_Data_Final"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_BGS_INST_Recode"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_bgs_data_final_for_outcomesmatching"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_Weights"')))

# DACSO
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."t_dacso_data_part_1_stepa"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."infoware_c_outc_clean_short_resp"')))

# Lookups
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."t_current_region_pssm_codes"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."t_current_region_pssm_rollup_codes"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."t_current_region_pssm_rollup_codes_bc"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."tbl_age"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."tbl_age_groups"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."t_pssm_credential_grouping"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."t_year_survey_year"')))

# ---- TRD Queries ----
# Applies weight for model year and derives New Labour Supply
if (regular_run == T & ptib_flag != T){
  dbExecute(decimal_con, "ALTER TABLE t_TRD_data ADD Age_Group FLOAT NULL;")
  dbExecute(decimal_con, "ALTER TABLE t_TRD_data ADD Age_Group_Rollup FLOAT NULL;")
  dbExecute(decimal_con, Q000_TRD_Q003c_Derived_And_Weights)
} 

if (regular_run == T & ptib_flag == T){
  dbExecute(decimal_con, Q000_TRD_Q003c_Derived_And_Weights)
}  

if (regular_run != T & ptib_flag != T) {
  dbExecute(decimal_con, Q000_TRD_Q003c_Derived_And_Weights_QI)
}


# Refresh trd survey records in T_Cohorts_Recoded
dbExecute(decimal_con, Q000_TRD_Q005_1b1_Delete_Cohort)
dbExecute(decimal_con, Q000_TRD_Q005_DACSO_DATA_Part_1b2_Cohort_Recoded)

# ---- APP Queries ----
# Refresh survey records in T_Cohorts_Recoded
dbExecute(decimal_con, APPSO_Q005_1b1_Delete_Cohort)
dbExecute(decimal_con, APPSO_Q005_DACSO_DATA_Part_1b2_Cohort_Recoded)

# ---- BGS Queries ----
# Recode institution codes to be consistent to STP file
dbExecute(decimal_con, BGS_Q001b_INST_Recode)

# Note: update CIPS after program matching. 
dbExecute(decimal_con, BGS_Q001c_Update_CIPs_After_Program_Matching)
dbExecute(decimal_con, BGS_Q002_LCP4_CRED)

# Applies weight for model year and derives New Labour Supply
if (regular_run == T | ptib_flag == T){
  dbExecute(decimal_con, "ALTER TABLE T_BGS_Data_Final ADD BGS_New_Labour_Supply FLOAT NULL;")
  dbExecute(decimal_con, BGS_Q003c_Derived_And_Weights)
}  else {
  dbExecute(decimal_con, BGS_Q003c_Derived_And_Weights_QI)
}

# Refresh bgs survey records in T_Cohorts_Recoded
dbExecute(decimal_con, BGS_Q005_1b1_Delete_Cohort)
dbExecute(decimal_con, BGS_Q005_1b2_Cohort_Recoded)

# ----DACSO Queries ----
# adds age, updates credential, creates new LCIP4_CRED variable 
if (regular_run == T | ptib_flag == T){ dbExecute(decimal_con, DACSO_Q003_DACSO_Data_Part_1_stepB) }

# Recode institution codes for CIP-NOC work
dbExecute(decimal_con, DACSO_Q003b_DACSO_DATA_Part_1_Further_Ed)

# Deletes other, none, invalid etc. credentials that are not part of the PSSM
dbExecute(decimal_con, DACSO_Q004_DACSO_DATA_Part_1_Delete_Credentials)

# Recodes all the old institution codes to the current code so that weight adjustments across years by program can be applied.
# This step skipped as not needed, but could add as a check at some point.
# dbExecute(decimal_con, DACSO_Q004b_INST_Recode)

# Applies weight for model year and derives New Labour Supply - re-run if changing model years or grouping geographies
if (regular_run == T | ptib_flag == T){
  dbExecute(decimal_con, DACSO_Q005_DACSO_DATA_Part_1a_Derived)
}  else {
  dbExecute(decimal_con, DACSO_Q005_DACSO_DATA_Part_1a_Derived_QI)
}

# Refresh dacso survey records in T_Cohorts_Recoded
dbExecute(decimal_con, DACSO_Q005_DACSO_DATA_Part_1b1_Delete_Cohort)
dbExecute(decimal_con, DACSO_Q005_DACSO_DATA_Part_1b2_Cohort_Recoded)

# ---- Keep  ----
dbExistsTable(decimal_con, "APPSO_Graduates")
dbExistsTable(decimal_con, "TRD_Graduates")
dbExistsTable(decimal_con, "t_dacso_data_part_1")
dbExistsTable(decimal_con, "T_Cohorts_Recoded")

# ---- Clean Up Lookups (if desired, not a needed step) ----
# dbExecute(decimal_con, "DROP TABLE T_BGS_INST_Recode;")
# dbExecute(decimal_con, "DROP TABLE T_PSSM_Credential_Grouping")
# dbExecute(decimal_con, "DROP TABLE t_year_survey_year")
# dbExecute(decimal_con, "DROP TABLE t_current_region_pssm_codes")
# dbExecute(decimal_con, "DROP TABLE t_current_region_pssm_rollup_codes")
# dbExecute(decimal_con, "DROP TABLE t_current_region_pssm_rollup_codes_bc")

dbDisconnect(decimal_con)
# rm(list=ls())

