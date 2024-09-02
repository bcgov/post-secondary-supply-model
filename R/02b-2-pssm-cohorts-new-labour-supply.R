# This script processes cohort data from student outcomes and creates new labour supply distributions.
# Outcomes data has been standardized so all cohorts/surveys are combined in a single dataset. 
#
# At a high level, the script:
#   Searches and updates invalid NOC codes in bgs and dacso tables
#   recodes the new labour supply for those with an NLS-2 record and no NLS-1
#   Weights each year up to the cohort (Prob_Weight) and apply year weights 
#     (1,2,3,4,5) and adjust to the cohort. 
#   Create weights for new labour supply (Weight_NLS)
#   Create weights for occupational distribution (Weight_OCC). 
#
# Includes records with a labour force status for those aged 17 to 64, 
# Includes those with an invalid NOC where 100% of CIP is invalid, as the cohort number. 

# Notes:  create Weight_Age is used to calculate the age for the private institution credentials 
# and needed if the data set doesn’t have age. Some invalid NOC codes (see documentation)
#         PDEG included at the end of occupation_distribution scripts.   
#
# FIXME Labour_Supply_Distribution_LCP2/LCP_No_TT have LCP2_CRED not LCIP2_CRED
# FIXME Missing Graduate Distributions (Non-Student Outcomes)

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
# ---- Source Queries ----
source(glue::glue("./sql/02b-pssm-cohorts/02b-pssm-cohorts-new-labour-supply.R"))
source(glue::glue("./sql/02b-pssm-cohorts/02b-pssm-cohorts-dacso.R"))

# ---- Check for required data tables ----
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."t_cohorts_recoded"')))
dbExistsTable(decimal_con, "t_current_region_pssm_rollup_codes")
dbExistsTable(decimal_con, "t_current_region_pssm_codes")
dbExistsTable(decimal_con, "tbl_noc_skill_level_aged_17_34")

# ---- Execute SQL ----
# TODO: would move this out of the dacso script as it's not DACSO specific
# and would remove the need to source it above. 
dbGetQuery(decimal_con, DACSO_Q005_DACSO_DATA_Part_1b3_Check_Weights) # Check base weights

# handle invalid noc codes
dbExecute(decimal_con, DACSO_Q99A_STQUI_ID)
dbGetQuery(decimal_con, DACSO_Q005_DACSO_DATA_Part_1b4_Check_NOC_Valid)

# No invalid nocs in dacso survey data
#dbExecute(decimal_con, DACSO_Q005_DACSO_Data_Part_1b7_Update_After_Recoding)

# NOTE: setting all 403X to 4031 for now, but these need would need to be
# imputed to 4031, 4032, 9999 to be accurate.  move qry to 02b1
#dbExecute(decimal_con, DACSO_Q005_DACSO_Data_Part_1b8_Update_After_Recoding)
dbExecute(decimal_con, "UPDATE T_Cohorts_Recoded
                        SET    T_Cohorts_Recoded.noc_cd = '4031'
                        WHERE T_Cohorts_Recoded.noc_cd = '403X'")
dbExecute(decimal_con, "DROP TABLE DACSO_Q99A_STQUI_ID")


# recode new labour supply for those with an NLS-2 record and no NLS1
dbExecute(decimal_con, DACSO_Q005_DACSO_DATA_Part_1c_NLS1)
dbExecute(decimal_con, DACSO_Q005_DACSO_DATA_Part_1c_NLS2)
dbExecute(decimal_con, DACSO_Q005_DACSO_DATA_Part_1c_NLS2_Recode) 
dbExecute(decimal_con, "DROP TABLE DACSO_Q005_DACSO_DATA_Part_1c_NLS1") 
dbExecute(decimal_con, "DROP TABLE DACSO_Q005_DACSO_DATA_Part_1c_NLS2")

# count the number of records in the cohort for the years included
dbGetQuery(decimal_con, DACSO_Q005_Z_Cohort_Resp_by_Region)

# create base weights for the full cohort
dbExecute(decimal_con, DACSO_Q005_Z01_Base_NLS)

# not used (see documentation)
#dbExecute(decimal_con, DACSO_Q005_Z02a_Base)
#dbExecute(decimal_con, DACSO_Q005_Z02b_Respondents)
#dbExecute(decimal_con, DACSO_Q005_Z02b_Respondents_Region_9999)
#dbExecute(decimal_con, DACSO_Q005_Z02b_Respondents_Union)

# create base and nls weights
dbExecute(decimal_con, DACSO_Q005_Z02c_Weight_tmp)
dbExecute(decimal_con, DACSO_Q005_Z02c_Weight)
dbExecute(decimal_con, DACSO_Q005_Z03_Weight_Total)
dbExecute(decimal_con, DACSO_Q005_Z04_Weight_Adj_Fac)
dbExecute(decimal_con, DACSO_Q005_Z05_Weight_NLS)
dbExecute(decimal_con, DACSO_Q005_Z06_Add_Weight_NLS_Field) 
dbExecute(decimal_con, "ALTER TABLE T_Cohorts_Recoded ALTER COLUMN Weight_NLS Float NULL;")

# null Weight_NLS field and update (if you’ve been messing with iterations)
dbExecute(decimal_con, DACSO_Q005_Z07_Weight_NLS_Null) 
dbExecute(decimal_con, DACSO_Q005_Z08_Weight_NLS_Update)

# check weights
dbGetQuery(decimal_con, DACSO_Q005_Z09_Check_Weights)
dbGetQuery(decimal_con, DACSO_Q005_Z09_Check_Weights_No_Weight_CIP)

dbExecute(decimal_con, "DROP TABLE DACSO_Q005_Z02c_Weight_tmp")
dbExecute(decimal_con, "DROP TABLE DACSO_Q005_Z03_Weight_Total")
dbExecute(decimal_con, "DROP TABLE DACSO_Q005_Z04_Weight_Adj_Fac")
dbExecute(decimal_con, "DROP TABLE DACSO_Q005_Z02c_Weight")
dbExecute(decimal_con, "DROP TABLE DACSO_Q005_Z01_Base_NLS")

# apply nls weights to group totals
dbExecute(decimal_con, DACSO_Q006a_Weight_New_Labour_Supply)
# calculate weighted new labor supply - various distribution
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

# ---- Final Distributions ----
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

# ---- Keep ----
dbExistsTable(decimal_con, "Labour_Supply_Distribution")
dbExistsTable(decimal_con, "Labour_Supply_Distribution_No_TT")
dbExistsTable(decimal_con, "Labour_Supply_Distribution_LCP2")
dbExistsTable(decimal_con, "Labour_Supply_Distribution_LCP2_No_TT")
dbExistsTable(decimal_con, "tmp_tbl_Weights_NLS")

# ---- Just for testing ----
# dbExecute(decimal_con, "DROP TABLE Labour_Supply_Distribution")
# dbExecute(decimal_con, "DROP TABLE Labour_Supply_Distribution_No_TT")
# dbExecute(decimal_con, "DROP TABLE Labour_Supply_Distribution_LCP2")
# dbExecute(decimal_con, "DROP TABLE Labour_Supply_Distribution_LCP2_No_TT")


