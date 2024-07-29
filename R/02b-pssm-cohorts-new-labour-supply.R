# Notes: watch for Age_Grouping variable, documentation mentions having removed it from earlier queries and linked later.  not sure what this means.
# also, need to update T-Year_Survey_Year as is a dependency in DACSO_Q005_DACSO_DATA_Part_1b2_Cohort_Recoded.  The pattern to update is obvious from prior
# year's entries, but some rationale would be helpful.

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
source(glue::glue("{lan}/development/sql/gh-source/02b-pssm-cohorts/02b-pssm-cohorts-new-labour-supply.R"))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."t_cohorts_recoded"')))
dbExistsTable(decimal_con, "t_current_region_pssm_rollup_codes")
dbExistsTable(decimal_con, "t_current_region_pssm_codes")
dbExistsTable(decimal_con, "tbl_noc_skill_level_aged_17_34")

# ---- Execute SQL ----
# ---- look for invalid NOC codes and recode bgs and dacso tables ----  
# Note: this step should probably be done earlier, bc if invalid nocs found, they 
# are fixed and then I believe T_Cohorts_recoded needs to be updated.
dbExecute(decimal_con, DACSO_Q99A_STQUI_ID)
dbGetQuery(decimal_con, DACSO_Q005_DACSO_DATA_Part_1b4_Check_NOC_Valid)

# If invalid nocs are found, run the following queries.  Looks like 2 tmp tables needs to be made.
dbExecute(decimal_con, DACSO_Q005_DACSO_Data_Part_1b7_Update_After_Recoding)
dbExecute(decimal_con, DACSO_Q005_DACSO_Data_Part_1b8_Update_After_Recoding)
dbExecute(decimal_con, "DROP TABLE DACSO_Q99A_STQUI_ID")

# recodes new labour supply for those with an NLS-2 record and no NLS1
# However, original query used a distinctrow and so this section needs a re-look  
# Need to rewrite the distinctrow part
dbExecute(decimal_con, DACSO_Q005_DACSO_DATA_Part_1c_NLS1)
dbExecute(decimal_con, DACSO_Q005_DACSO_DATA_Part_1c_NLS2)
dbExecute(decimal_con, DACSO_Q005_DACSO_DATA_Part_1c_NLS2_Recode) 
dbExecute(decimal_con, "DROP TABLE DACSO_Q005_DACSO_DATA_Part_1c_NLS1") 
dbExecute(decimal_con, "DROP TABLE DACSO_Q005_DACSO_DATA_Part_1c_NLS2")

# counts the number of records in the cohort for the years included
dbGetQuery(decimal_con, DACSO_Q005_Z_Cohort_Resp_by_Region)
dbGetQuery(decimal_con, DACSO_Q005_Z01_Base_NLS)

# not used
# find instances of where the PSSM region code is unknown for a program at an institution across all years
# if implemented it would include these records with an unknown region in the model
#dbExecute(decimal_con, DACSO_Q005_Z02a_Base)
#dbExecute(decimal_con, DACSO_Q005_Z02b_Respondents)
#dbExecute(decimal_con, DACSO_Q005_Z02b_Respondents_Region_9999)
#dbExecute(decimal_con, DACSO_Q005_Z02b_Respondents_Union)

dbExecute(decimal_con, DACSO_Q005_Z02c_Weight_tmp)
dbExecute(decimal_con, DACSO_Q005_Z02c_Weight)
dbExecute(decimal_con, DACSO_Q005_Z03_Weight_Total)
dbExecute(decimal_con, DACSO_Q005_Z04_Weight_Adj_Fac)
dbExecute(decimal_con, DACSO_Q005_Z05_Weight_NLS)
dbExecute(decimal_con, DACSO_Q005_Z06_Add_Weight_NLS_Field) # add the Weight_NLS field to T_Cohorts_Recoded
dbExecute(decimal_con, DACSO_Q005_Z07_Weight_NLS_Null) # nulls Weight_NLS field if you’ve been messing with iterations
dbExecute(decimal_con, DACSO_Q005_Z08_Weight_NLS_Update)
dbGetQuery(decimal_con, DACSO_Q005_Z09_Check_Weights)
dbGetQuery(decimal_con, DACSO_Q005_Z09_Check_Weights_No_Weight_CIP)

dbExecute(decimal_con, DACSO_Q006a_Weight_New_Labour_Supply)
dbExecute(decimal_con, DACSO_Q006b_Weighted_New_Labour_Supply)
dbExecute(decimal_con, DACSO_Q006b_Weighted_New_Labour_Supply_0)
dbExecute(decimal_con, DACSO_Q006b_Weighted_New_Labour_Supply_0_2D)
dbExecute(decimal_con, DACSO_Q006b_Weighted_New_Labour_Supply_0_2D_No_TT)
dbExecute(decimal_con, DACSO_Q006b_Weighted_New_Labour_Supply_0_No_TT)
dbExecute(decimal_con, DACSO_Q006b_Weighted_New_Labour_Supply_2D)
dbExecute(decimal_con, DACSO_Q006b_Weighted_New_Labour_Supply_2D_No_TT)
dbExecute(decimal_con, DACSO_Q006b_Weighted_New_Labour_Supply_No_TT)
dbExecute(decimal_con, DACSO_Q006b_Weighted_New_Labour_Supply_Total)
dbExecute(decimal_con, DACSO_Q006b_Weighted_New_Labour_Supply_Total_2D)
dbExecute(decimal_con, DACSO_Q006b_Weighted_New_Labour_Supply_Total_2D_No_TT)
dbExecute(decimal_con, DACSO_Q006b_Weighted_New_Labour_Supply_Total_No_TT)

dbExecute(decimal_con, "DROP TABLE DACSO_Q005_Z02c_Weight_tmp")
dbExecute(decimal_con, "DROP TABLE DACSO_Q005_Z03_Weight_Total")
dbExecute(decimal_con, "DROP TABLE DACSO_Q005_Z04_Weight_Adj_Fac")
dbExecute(decimal_con, "DROP TABLE DACSO_Q005_Z02c_Weight")
dbExecute(decimal_con, "DROP TABLE DACSO_Q005_Z01_Base_NLS")

dbExecute(decimal_con, DACSO_Q007a_Weighted_New_Labour_Supply)
dbExecute(decimal_con, DACSO_Q007a_Weighted_New_Labour_Supply_0)
dbExecute(decimal_con, DACSO_Q007a_Weighted_New_Labour_Supply_0_2D)
dbExecute(decimal_con, DACSO_Q007a_Weighted_New_Labour_Supply_0_2D_No_TT)
dbExecute(decimal_con, DACSO_Q007a_Weighted_New_Labour_Supply_0_No_TT)
dbExecute(decimal_con, DACSO_Q007a_Weighted_New_Labour_Supply_2D)
dbExecute(decimal_con, DACSO_Q007a_Weighted_New_Labour_Supply_2D_No_TT)
dbExecute(decimal_con, DACSO_Q007a_Weighted_New_Labour_Supply_No_TT)

dbExecute(decimal_con, "DROP TABLE DACSO_Q006a_Weight_New_Labour_Supply")
dbExecute(decimal_con, "DROP TABLE DACSO_Q006b_Weighted_New_Labour_Supply")
dbExecute(decimal_con, "DROP TABLE DACSO_Q006b_Weighted_New_Labour_Supply_0")
dbExecute(decimal_con, "DROP TABLE DACSO_Q006b_Weighted_New_Labour_Supply_0_2D")
dbExecute(decimal_con, "DROP TABLE dacso_q006b_weighted_new_labour_supply_0_2d_no_tt")
dbExecute(decimal_con, "DROP TABLE dacso_q006b_weighted_new_labour_supply_0_no_tt")
dbExecute(decimal_con, "DROP TABLE DACSO_Q006b_Weighted_New_Labour_Supply_2D")
dbExecute(decimal_con, "DROP TABLE dacso_q006b_weighted_new_labour_supply_2d_no_tt")
dbExecute(decimal_con, "DROP TABLE DACSO_Q006b_Weighted_New_Labour_Supply_Total")
dbExecute(decimal_con, "DROP TABLE DACSO_Q006b_Weighted_New_Labour_Supply_Total_2D")
dbExecute(decimal_con, "DROP TABLE DACSO_Q006b_Weighted_New_Labour_Supply_Total_2D_No_TT")
dbExecute(decimal_con, "DROP TABLE dacso_q006b_weighted_new_labour_supply_total_no_tt")
dbExecute(decimal_con, "DROP TABLE dacso_q006b_weighted_new_labour_supply_no_tt")

nls_def <- c(Survey = "nvarchar(50)", PSSM_Credential  = "nvarchar(50)", PSSM_CRED  = "nvarchar(50)",  LCP4_CD = "nvarchar(50)", 
              TTRAIN = "nvarchar(50)", LCIP4_CRED = "nvarchar(50)", LCIP2_CRED = "nvarchar(50)", 
              Current_Region_PSSM_Code_Rollup = "integer", Age_Group_Rollup = "integer", Count = "float", Total = "float", New_Labour_Supply = "float")

if(!dbExistsTable(decimal_con, "Labour_Supply_Distribution")){
  dbCreateTable(decimal_con, "Labour_Supply_Distribution",  nls_def)
} 
if(!dbExistsTable(decimal_con, "Labour_Supply_Distribution_No_TT")){
  dbCreateTable(decimal_con, "Labour_Supply_Distribution_No_TT",  nls_def)
}

dbExecute(decimal_con, DACSO_Q007b0_Delete_New_Labour_Supply)
dbExecute(decimal_con, DACSO_Q007b0_Delete_New_Labour_Supply_No_TT)
# dbExecute(decimal_con, DACSO_Q007b0_Delete_New_Labour_Supply_No_TT_QI)
# dbExecute(decimal_con, DACSO_Q007b0_Delete_New_Labour_Supply_QI)
dbExecute(decimal_con, DACSO_Q007b1_Append_New_Labour_Supply)
dbExecute(decimal_con, DACSO_Q007b1_Append_New_Labour_Supply_No_TT)
dbExecute(decimal_con, DACSO_Q007b2_Append_New_Labour_Supply_0)
dbExecute(decimal_con, DACSO_Q007b2_Append_New_Labour_Supply_0_No_TT)

nls_def <- c(Survey = "nvarchar(50)", PSSM_Credential  = "nvarchar(50)", PSSM_CRED  = "nvarchar(50)",  LCP2_CD = "nvarchar(50)", 
             TTRAIN = "nvarchar(50)", LCP2_CRED = "nvarchar(50)", 
             Current_Region_PSSM_Code_Rollup = "integer", Age_Group_Rollup = "integer", Count = "float", Total = "float", New_Labour_Supply = "float")

if(!dbExistsTable(decimal_con, "Labour_Supply_Distribution_LCP2")){
  dbCreateTable(decimal_con, "Labour_Supply_Distribution_LCP2",  nls_def)
} 
if(!dbExistsTable(decimal_con, "Labour_Supply_Distribution_LCP2_No_TT")){
  dbCreateTable(decimal_con, "Labour_Supply_Distribution_LCP2_No_TT",  nls_def)
}

dbExecute(decimal_con, DACSO_Q007c0_Delete_New_Labour_Supply_2D)
dbExecute(decimal_con, DACSO_Q007c0_Delete_New_Labour_Supply_2D_No_TT)
# dbExecute(decimal_con, DACSO_Q007c0_Delete_New_Labour_Supply_2D_No_TT_QI)
# dbExecute(decimal_con, DACSO_Q007c0_Delete_New_Labour_Supply_2D_QI)

dbExecute(decimal_con, DACSO_Q007c1_Append_New_Labour_Supply_2D)
dbExecute(decimal_con, DACSO_Q007c1_Append_New_Labour_Supply_2D_No_TT)
dbExecute(decimal_con, DACSO_Q007c2_Append_New_Labour_Supply_0_2D)
dbExecute(decimal_con, DACSO_Q007c2_Append_New_Labour_Supply_0_2D_No_TT)

dbExecute(decimal_con, "DROP TABLE DACSO_Q007a_Weighted_New_Labour_Supply")
dbExecute(decimal_con, "DROP TABLE DACSO_Q007a_Weighted_New_Labour_Supply_0")
dbExecute(decimal_con, "DROP TABLE DACSO_Q007a_Weighted_New_Labour_Supply_0_2D")
dbExecute(decimal_con, "DROP TABLE DACSO_Q007a_Weighted_New_Labour_Supply_0_2D_No_TT")
dbExecute(decimal_con, "DROP TABLE DACSO_Q007a_Weighted_New_Labour_Supply_0_No_TT")
dbExecute(decimal_con, "DROP TABLE DACSO_Q007a_Weighted_New_Labour_Supply_2D")
dbExecute(decimal_con, "DROP TABLE DACSO_Q007a_Weighted_New_Labour_Supply_2D_No_TT")
dbExecute(decimal_con, "DROP TABLE DACSO_Q007a_Weighted_New_Labour_Supply_No_TT")

# ---- Clean Up ----
dbDisconnect(decimal_con)

# --- just for testing - do not run as part of the workflow
dbExecute(decimal_con, "DROP TABLE tmp_tbl_Weights_NLS")
dbExecute(decimal_con, "DROP TABLE Labour_Supply_Distribution")
dbExecute(decimal_con, "DROP TABLE Labour_Supply_Distribution_No_TT")
dbExecute(decimal_con, "DROP TABLE Labour_Supply_Distribution_LCP2")
dbExecute(decimal_con, "DROP TABLE Labour_Supply_Distribution_LCP2_No_TT")

# ---- old clean up - move these ----
dbExecute(decimal_con, "DROP TABLE T_TRD_DATA")
dbExecute(decimal_con, "DROP TABLE T_APPSO_DATA_Final")
dbExecute(decimal_con, "DROP TABLE T_BGS_Data_Final")
dbExecute(decimal_con, "DROP TABLE appso_current_region_data")
dbExecute(decimal_con, "DROP TABLE dacso_current_region_data")
dbExecute(decimal_con, "DROP TABLE bgs_current_region_data")
dbExecute(decimal_con, "DROP TABLE trd_current_region_data")
dbExecute(decimal_con, "DROP TABLE t_dacso_data_part_1_stepa")
dbExecute(decimal_con, "DROP TABLE t_dacso_data_part_1")
