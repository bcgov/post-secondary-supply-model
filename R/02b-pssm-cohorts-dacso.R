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
source(glue::glue("{lan}/data/student-outcomes/sql/dacso-data.sql"))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."tbl_age"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."tbl_age_groups"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."t_pssm_credential_grouping"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."infoware_c_outc_clean_short_resp"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."infoware_c_outc_clean2"')))

# Notes: watch for Age_Grouping variable, documentation mentions having removed it from earlier queries and linked later.  not sure what this means.

# Recodes CIP codes
dbExecute(decimal_con, DACSO_Q003_DACSO_Data_Part_1_stepB)

# Recode institution codes for CIP-NOC work
dbExecute(decimal_con, DACSO_Q003b_DACSO_DATA_Part_1_Further_Ed)

# Deletes other, none, invalid etc. credentials that are not part of the PSSM
dbExcute(decimal_con, DACSO_Q004_DACSO_DATA_Part_1_Delete_Credentials)

# Recodes all the old institution codes to the current code so that weight adjustments across years by program can be applied
dbExcute(decimal_con, DACSO_Q004b_INST_Recode)

# updates CURRENT_REGION_PSSM_CODE after the geocoding.
dbExcute(decimal_con, DACSO_Q005_DACSO_DATA_Part_1_Add_CURRENT_REGION_PSSM)
dbExcute(decimal_con, DACSO_Q005_DACSO_DATA_Part_1_Add_CURRENT_REGION_PSSM2)

# Applies weight for model year and derives New Labour Supply - re-run if changing model years or grouping geographies
# Note: SQl may need to be updated - looks like the query needs a case_when added
dbExcute(decimal_con, DACSO_Q005_DACSO_DATA_Part_1a_Derive)

# # Refresh bgs survey records in T_Cohorts_Recoded
dbExcute(decimal_con, DACSO_Q005_DACSO_DATA_Part_1b1_Delete_Cohort)
dbExcute(decimal_con, DACSO_Q005_DACSO_DATA_Part_1b2_Cohort_Recoded)

# Check weights
dbExcute(decimal_con, DACSO_Q005_DACSO_DATA_Part_1b3_Check_Weights)




# ---- Clean Up ----
dbDisconnect(decimal_con)
