# ******************************************************************************
# This script prepares for the CIP-NOC and NOC-CIP transformation reports
# 
# Required Tables
#   T_Cohorts_Recoded
#   T_Current_Region_PSSM_Codes
#   T_Current_Region_PSSM_Rollup_Codes
#
# ******************************************************************************

library(tidyverse)
library(RODBC)
library(config)
library(DBI)

# ---- Configure LAN and file paths ----
db_config <- config::get("decimal")
lan <- config::get("lan")
my_schema <- config::get("myschema")

# ---- Connection to decimal ----
db_config <- config::get("decimal")
decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")

# ---- Check for/read in required data tables ----
## Look up tables
# run one time to create in schema
t_current_region_pssm_codes <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/02/T_Current_Region_PSSM_Codes.csv"), col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")
dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."t_current_region_pssm_codes"')), value = t_current_region_pssm_codes)

# import sql queries
source("./sql/09-cip-noc-transformations/cip-noc-transformations.R")

# CIP-NOC work ----
## Create T_Cohorts_Recoded_CIP_NOC from T_Cohorts_Recoded for desired years
dbExecute(decimal_con, qry_Make_T_Cohorts_Recoded_for_CIP_NOC)

# Add NLS to NLS_CIP_NOC column
dbExecute(decimal_con, CIP_NOC_Update_NewLabourSupply_CIP_NOC)

# recode new labour supply cip-noc for those with an NLS-2 record and no NLS1
dbExecute(decimal_con, DACSO_Q005_DACSO_DATA_Part_1c_NLS1_CIP_NOC)
dbExecute(decimal_con, DACSO_Q005_DACSO_DATA_Part_1c_NLS2_CIP_NOC)
