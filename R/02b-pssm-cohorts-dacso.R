# This script prepares student outcomes data for studentswho recently graduated after
# completing programs at public colleges, institutes, and teaching-intensive universities (~18 months prior)
# 
# Generally, the script prepares survey data by:
#   Recode institution codes to be consistent to STP file
#   update CIPS after program matching.
#   Updating CURRENT_REGION_PSSM_CODE after the geocoding
#   Applies weight for model year and derives New Labour Supply
#   Refresh survey records in T_Cohorts_Recoded
#   adds age and age group, a new student id
#
# Notes: double check method for updating CIPc codes after program matching. 
#  There is a query to check for invalid NOC codes (see documentation).
#  update T-Year_Survey_Year and T_weights (for all cohorts)
#  2006 dacso all NULL lcip-4-creds (remove 2006)

library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(RJDBC)

# ---- Configure LAN and file paths ----
db_config <- config::get("decimal")
lan <- config::get("lan")
my_schema <- config::get("myschema")

# ---- Connection to database ----
db_config <- config::get("decimal")
decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")

# ---- Read raw data ----
source(glue::glue("{lan}/development/sql/gh-source/02b-pssm-cohorts/02b-pssm-cohorts-dacso.R"))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."tbl_age"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."tbl_age_groups"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."t_pssm_credential_grouping"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."infoware_c_outc_clean_short_resp"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."t_current_region_pssm_codes"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."t_current_region_pssm_rollup_codes"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."t_current_region_pssm_rollup_codes_bc"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."t_dacso_data_part_1_stepa"')))

# ---- Execute SQL ----
# adds age, updates credential, creates new LCIP4_CRED variable 
dbExecute(decimal_con, DACSO_Q003_DACSO_Data_Part_1_stepB)

# Recode institution codes for CIP-NOC work
dbExecute(decimal_con, DACSO_Q003b_DACSO_DATA_Part_1_Further_Ed)

# Deletes other, none, invalid etc. credentials that are not part of the PSSM
dbExecute(decimal_con, DACSO_Q004_DACSO_DATA_Part_1_Delete_Credentials)

# Recodes all the old institution codes to the current code so that weight adjustments across years by program can be applied.
# This step skipped as not needed, but could add as a check at some point.
# dbExecute(decimal_con, DACSO_Q004b_INST_Recode)

# Applies weight for model year and derives New Labour Supply - re-run if changing model years or grouping geographies
dbExecute(decimal_con, DACSO_Q005_DACSO_DATA_Part_1a_Derived)

# Refresh dacso survey records in T_Cohorts_Recoded
dbExecute(decimal_con, DACSO_Q005_DACSO_DATA_Part_1b1_Delete_Cohort)
dbExecute(decimal_con, DACSO_Q005_DACSO_DATA_Part_1b2_Cohort_Recoded)

# ---- Clean Up ----
dbDisconnect(decimal_con)
dbExecute(decimal_con, "DROP TABLE t_dacso_data_part_1_stepa;")