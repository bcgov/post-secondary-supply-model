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

# ---- Execute SQL ----
# looks for invalid NOC codes and recode.  I haven't recreated this yet as dependent on other tables which I am not sure how they are derived.
dbGetResults(decimal_con, DACSO_Q005_DACSO_DATA_Part_1b4_Check_NOC_Valid)
dbExecute(decimal_con, DACSO_Q005_DACSO_Data_Part_1b7_Update_After_Recoding)
dbExecute(decimal_con, DACSO_Q005_DACSO_Data_Part_1b8_Update_After_Recoding)

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
dbExecute(decimal_con, DACSO_Q005_Z08_Weight_NLS_Update) # removed distinctrow 

dbGetQuery(decimal_con, DACSO_Q005_Z09_Check_Weights)

dbExecute(decimal_con, DACSO_Q005_Z09_Check_Weights_No_Weight_CIP)

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

# drop these before the 006a series?
dbExecute(decimal_con, "DROP TABLE DACSO_Q005_Z02c_Weight_tmp")
dbExecute(decimal_con, "DROP TABLE DACSO_Q005_Z03_Weight_Total")
dbExecute(decimal_con, "DROP TABLE DACSO_Q005_Z04_Weight_Adj_Fac")
dbExecute(decimal_con, "DROP TABLE DACSO_Q005_Z02c_Weight")
dbExecute(decimal_con, "DROP TABLE tmp_tbl_Weights_NLS")
dbExecute(decimal_con, "DROP TABLE DACSO_Q005_Z01_Base_NLS")
dbExecute(decimal_con, "DROP TABLE DACSO_Q006a_Weight_New_Labour_Supply")


dbExecute(decimal_con, DACSO_Q007a_Weighted_New_Labour_Supply)
dbExecute(decimal_con, DACSO_Q007a_Weighted_New_Labour_Supply_0)
dbExecute(decimal_con, DACSO_Q007a_Weighted_New_Labour_Supply_0_2D)
dbExecute(decimal_con, DACSO_Q007a_Weighted_New_Labour_Supply_0_2D_No_TT)
dbExecute(decimal_con, DACSO_Q007a_Weighted_New_Labour_Supply_0_No_TT)
dbExecute(decimal_con, DACSO_Q007a_Weighted_New_Labour_Supply_2D)
dbExecute(decimal_con, DACSO_Q007a_Weighted_New_Labour_Supply_2D_No_TT)
dbExecute(decimal_con, DACSO_Q007a_Weighted_New_Labour_Supply_No_TT)
dbExecute(decimal_con, DACSO_Q007b0_Delete_New_Labour_Supply)
dbExecute(decimal_con, DACSO_Q007b0_Delete_New_Labour_Supply_No_TT)
dbExecute(decimal_con, DACSO_Q007b0_Delete_New_Labour_Supply_No_TT_QI)
dbExecute(decimal_con, DACSO_Q007b0_Delete_New_Labour_Supply_QI)
dbExecute(decimal_con, DACSO_Q007b1_Append_New_Labour_Supply)
dbExecute(decimal_con, DACSO_Q007b1_Append_New_Labour_Supply_No_TT)
dbExecute(decimal_con, DACSO_Q007b2_Append_New_Labour_Supply_0)
dbExecute(decimal_con, DACSO_Q007b2_Append_New_Labour_Supply_0_No_TT)

dbExecute(decimal_con, DACSO_Q007c0_Delete_New_Labour_Supply_2D)

dbExecute(decimal_con, DACSO_Q007c0_Delete_New_Labour_Supply_2D_No_TT)

dbExecute(decimal_con, DACSO_Q007c0_Delete_New_Labour_Supply_2D_No_TT_QI)

dbExecute(decimal_con, DACSO_Q007c0_Delete_New_Labour_Supply_2D_QI)

dbExecute(decimal_con, DACSO_Q007c1_Append_New_Labour_Supply_2D)

dbExecute(decimal_con, DACSO_Q007c1_Append_New_Labour_Supply_2D_No_TT)

dbExecute(decimal_con, DACSO_Q007c2_Append_New_Labour_Supply_0_2D)

dbExecute(decimal_con, DACSO_Q007c2_Append_New_Labour_Supply_0_2D_No_TT)


# ---- Clean Up ----
dbDisconnect(decimal_con)
dbExecute(decimal_con, "DROP TABLE T_Cohorts_Recoded")
dbExecute(decimal_con, "DROP TABLE tmp_bgs_inst_region_cds")
