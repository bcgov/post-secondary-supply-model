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

## Prepare Labour Supply table ----
# Add NLS to NLS_CIP_NOC column
dbExecute(decimal_con, CIP_NOC_Update_NewLabourSupply_CIP_NOC)

# recode new labour supply cip-noc for those with an NLS-2 record and no NLS1
dbExecute(decimal_con, DACSO_Q005_DACSO_DATA_Part_1c_NLS1_CIP_NOC)
dbExecute(decimal_con, DACSO_Q005_DACSO_DATA_Part_1c_NLS2_CIP_NOC)
dbExecute(decimal_con, DACSO_Q005_DACSO_DATA_Part_1c_NLS2_Recode_CIP_NOC)

# create base weights for the full cohort
dbExecute(decimal_con, DACSO_Q005_Z01_Base_NLS_CIP_NOC)

# create nls weights
dbExecute(decimal_con, DACSO_Q005_Z02_Weight_CIP_NOC_tmp)
dbExecute(decimal_con, DACSO_Q005_Z02_Weight_CIP_NOC)
dbExecute(decimal_con, DACSO_Q005_Z03_Weight_Total_CIP_NOC)
dbExecute(decimal_con, DACSO_Q005_Z04_Weight_Adj_Fac_CIP_NOC)
dbExecute(decimal_con, DACSO_Q005_Z05_Weight_NLS_CIP_NOC)

# null and update weight_nls_cip_noc in T_Cohorts_Recoded_CIP_NOC
dbExecute(decimal_con, DACSO_Q005_Z07_Weight_NLS_Null_CIP_NOC)
dbExecute(decimal_con, DACSO_Q005_Z08_Weight_NLS_Update_CIP_NOC)

# check weights?
dbExecute(decimal_con, DACSO_Q005_Z09_Check_Weights_CIP_NOC)
dbExecute(decimal_con, DACSO_Q005_Z09_Check_Weights_No_Weight_CIP_NOC)

# apply nls weights to group totals
dbExecute(decimal_con, DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC)

# calculate weighted new labor supply - various distribution
# from documentation: Note that the queries with _2D in the name are not required for the CIP-NOC work. 
# have excluded them from this script
dbExecute(decimal_con, DACSO_Q006b_Weighted_New_Labour_Supply_CIP_NOC)
dbExecute(decimal_con, DACSO_Q006b_Weighted_New_Labour_Supply_0_CIP_NOC)
dbExecute(decimal_con, DACSO_Q006b_Weighted_New_Labour_Supply_Total_CIP_NOC)
dbExecute(decimal_con, DACSO_Q007a_Weighted_New_Labour_Supply_CIP_NOC)
dbExecute(decimal_con, DACSO_Q007a_Weighted_New_Labour_Supply_0_CIP_NOC)

dbExecute(decimal_con, "DROP TABLE DACSO_Q005_DACSO_DATA_Part_1c_NLS1_CIP_NOC")
dbExecute(decimal_con, "DROP TABLE DACSO_Q005_DACSO_DATA_Part_1c_NLS2_CIP_NOC")
dbExecute(decimal_con, "DROP TABLE DACSO_Q005_Z01_Base_NLS_CIP_NOC")
dbExecute(decimal_con, "DROP TABLE DACSO_Q005_Z02_Weight_CIP_NOC_tmp")
dbExecute(decimal_con, "DROP TABLE DACSO_Q005_Z02_Weight_CIP_NOC")
dbExecute(decimal_con, "DROP TABLE DACSO_Q005_Z03_Weight_Total_CIP_NOC")
dbExecute(decimal_con, "DROP TABLE DACSO_Q005_Z04_Weight_Adj_Fac_CIP_NOC")
dbExecute(decimal_con, "DROP TABLE DACSO_Q006b_Weighted_New_Labour_Supply_Total_CIP_NOC")
dbExecute(decimal_con, "DROP TABLE DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC")
dbExecute(decimal_con, "DROP TABLE DACSO_Q006b_Weighted_New_Labour_Supply_CIP_NOC")
dbExecute(decimal_con, "DROP TABLE DACSO_Q006b_Weighted_New_Labour_Supply_0_CIP_NOC")
dbExecute(decimal_con, "DROP TABLE DACSO_Q006b_Weighted_New_Labour_Supply_Total_CIP_NOC")

# append to labour supply distribution table
nls_def <- c(Survey = "nvarchar(50)", PSSM_Credential  = "nvarchar(50)", Current_Region_PSSM_Code_Rollup = "integer",   
             Age_Group_Rollup = "integer", LCP4_CD = "nvarchar(50)", LCIP4_CRED = "nvarchar(50)",  
             Count = "float", Total = "float", New_Labour_Supply_CIP_NOC = "float")

if(!dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Labour_Supply_Distribution_CIP_NOC"')))){
  dbCreateTable(decimal_con, SQL(glue::glue('"{my_schema}"."Labour_Supply_Distribution_CIP_NOC"')),  nls_def)
} 

dbExecute(decimal_con,"DELETE FROM labour_Supply_Distribution_cip_noc")
dbExecute(decimal_con, DACSO_Q007b1_Append_New_Labour_Supply_CIP_NOC)
dbExecute(decimal_con, DACSO_Q007b2_Append_New_Labour_Supply_0_CIP_NOC)

dbExecute(decimal_con, "DROP TABLE DACSO_Q007a_Weighted_New_Labour_Supply_CIP_NOC")
dbExecute(decimal_con, "DROP TABLE DACSO_Q007a_Weighted_New_Labour_Supply_0_CIP_NOC")
dbExecute(decimal_con, "DROP TABLE tmp_tbl_Weights_NLS_CIP_NOC")

## Prepare Occupation Distribution table ----

# create base weights
dbExecute(decimal_con, DACSO_Q008_Z01_Base_OCC_CIP_NOC)

# weight calculations
dbExecute(decimal_con, DACSO_Q008_Z02a_Base_CIP_NOC)
dbExecute(decimal_con, DACSO_Q008_Z02b_Respondents_CIP_NOC)
dbExecute(decimal_con, DACSO_Q008_Z02c_Weight_CIP_NOC)
dbExecute(decimal_con, DACSO_Q008_Z03_Weight_Total_CIP_NOC)
dbExecute(decimal_con, DACSO_Q008_Z04_Weight_Adj_Fac_CIP_NOC)
dbExecute(decimal_con, DACSO_Q008_Z05_Weight_OCC_CIP_NOC)

# placeholder for some queries - investigate if needed
# DACSO_Q008_Z05b_CIP2D_NLS_XTab
# DACSO_Q008_Z05b_CIP4D_NLS_XTab
# DACSO_Q008_Z05b_Finding_NLS2_Missing
# DACSO_Q008_Z05b_NOC4D_NLS_XTab
# DACSO_Q008_Z05b_Weight_Comparison 

# null and update weight_occ_cip_noc in T_Cohorts_Recoded_CIP_NOC
dbExecute(decimal_con, DACSO_Q008_Z07_Weight_OCC_Null_CIP_NOC)
dbExecute(decimal_con, DACSO_Q008_Z08_Weight_OCC_Update_CIP_NOC)

# check weights?
dbExecute(decimal_con, DACSO_Q008_Z09_Check_Weights_CIP_NOC)

# apply weight occs by region 
dbExecute(decimal_con, DACSO_Q009_Weight_Occs_CIP_NOC)

# calculate weighted occs
# from documentation: Note that the queries with _2D in the name are not required for the CIP-NOC work. 
# have excluded them from this script
dbExecute(decimal_con, DACSO_Q009b_Weighted_Occs_CIP_NOC)
dbExecute(decimal_con, DACSO_Q009b_Weighted_Occs_Total_CIP_NOC)
dbExecute(decimal_con, DACSO_Q010_Weighted_Occs_Dist_CIP_NOC)

dbExecute(decimal_con, "DROP TABLE DACSO_Q008_Z01_Base_OCC_CIP_NOC")
dbExecute(decimal_con, "DROP TABLE DACSO_Q008_Z02a_Base_CIP_NOC")
dbExecute(decimal_con, "DROP TABLE DACSO_Q008_Z02b_Respondents_CIP_NOC")
dbExecute(decimal_con, "DROP TABLE DACSO_Q008_Z02c_Weight_CIP_NOC")
dbExecute(decimal_con, "DROP TABLE DACSO_Q008_Z03_Weight_Total_CIP_NOC")
dbExecute(decimal_con, "DROP TABLE DACSO_Q008_Z04_Weight_Adj_Fac_CIP_NOC")
dbExecute(decimal_con, "DROP TABLE tmp_tbl_Weights_OCC_CIP_NOC")
dbExecute(decimal_con, "DROP TABLE DACSO_Q009_Weight_Occs_CIP_NOC")
dbExecute(decimal_con, "DROP TABLE DACSO_Q009b_Weighted_Occs_CIP_NOC")
dbExecute(decimal_con, "DROP TABLE DACSO_Q009b_Weighted_Occs_Total_CIP_NOC")


# append to occupation distributions table
occs_def <- c(Survey = "nvarchar(50)", PSSM_Credential  = "nvarchar(50)", Current_Region_PSSM_Code_Rollup = "integer",
              Age_Group_Rollup = "integer",  LCP4_CD = "nvarchar(50)", LCIP4_CRED = "nvarchar(50)", NOC = "nvarchar(50)" , 
              Unweighted_Count = "float", Unweighted_Total = "float", Unweighted_Percent = "float",
              Count = "float",Total = "float", Percent = "float")


if(!dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Occupation_Distributions_CIP_NOC"')))){
  dbCreateTable(decimal_con, SQL(glue::glue('"{my_schema}"."Occupation_Distributions_CIP_NOC"')),  occs_def)
} 

dbExecute(decimal_con,"DELETE FROM Occupation_Distributions_CIP_NOC")
dbExecute(decimal_con, DACSO_Q010b_Append_Occupational_Distribution_CIP_NOC)

dbExecute(decimal_con, "DROP TABLE DACSO_Q010_Weighted_Occs_Dist_CIP_NOC")

