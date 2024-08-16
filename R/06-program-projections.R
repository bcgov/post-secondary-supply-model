library(tidyverse)
library(RODBC)
library(config)
library(DBI)

# ---- Configure LAN and file paths ----
db_config <- config::get("decimal")
lan <- config::get("lan")
my_schema <- config::get("myschema")

source(glue::glue("{lan}/development/sql/gh-source/06-program-projections/06-program-projections.R"))

# ---- Connection to decimal ----
db_config <- config::get("decimal")
decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")

# ---- Check for required data tables ----
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN"'))) 
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Cohort_Program_Distributions_Projected"')))
# tbl_Program_Projection needs to be built.  Definiton saved in Development\SQL Server\Views
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."tbl_Program_Projection_Input"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_4DIGITS_CIP2016"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_PSSM_Projection_Cred_Grp"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_Weights_STP"')))


# ---- Near Completers ----
# computes distribution of near completers
dbExecute(decimal_con, qry_13a0_Delete_Near_Completers_Projected) 
dbExecute(decimal_con, qry_13a0_Delete_Near_Completers_Static) 
dbExecute(decimal_con, qry_13a1_Near_completers) 
dbExecute(decimal_con, qry_13b_Near_Completers_Total) 
dbExecute(decimal_con, qry_13d_Append_Near_Completers_Program_Dist_Projected_TTRAIN) 
dbExecute(decimal_con, qry_13d_Append_Near_Completers_Program_Dist_Static_TTRAIN) 

# ---- Static Program Distributions ----
# Note:	Update Survey and Projection_year for all queries

dbGetQuery(decimal_con, Q012a_Check_Total_for_Invalid_CIPs) 
dbExecute(decimal_con, Q012b_Weight_Cohort_Dist) 
dbExecute(decimal_con, Q012c_Weighted_Cohort_Dist) 
dbExecute(decimal_con, Q012c1_Weighted_Cohort_Dist_TTRAIN) 
dbExecute(decimal_con, Q012c2_Weighted_Cohort_Dist) 
dbExecute(decimal_con, Q012c3_Weighted_Cohort_Dist_Total) 
dbExecute(decimal_con, Q012c4_Weighted_Cohort_Distribution_Projected) 
dbExecute(decimal_con, Q012c5_Weighted_Cohort_Dist_TTRAIN) 
dbExecute(decimal_con, Q012d_Weighted_Cohort_Dist_Total) 
dbExecute(decimal_con, Q012e_Delete_Weighted_Cohort_Distribution) 
dbExecute(decimal_con, Q012e_Weighted_Cohort_Distribution) 

dbExecute(decimal_con, qry_12_LCP4_LCIPPC_Recode_9999)
dbExecute(decimal_con, Q013a_Check_PDEG_CLP_07_Only_CIP_22)
dbExecute(decimal_con, Q013b_Weight_Cohort_Dist_MAST_DOCT_Others)
dbExecute(decimal_con, Q013c_Weighted_Cohort_Dist)
dbExecute(decimal_con, Q013d_Weighted_Cohort_Dist_Total)
dbExecute(decimal_con, Q013e_Weighted_Cohort_Distribution)

dbExecute(decimal_con, Q014b_Weighted_Cohort_Dist_APPR)
dbExecute(decimal_con, Q014c_Weighted_Cohort_Dist)
dbExecute(decimal_con, Q014d_Weighted_Cohort_Dist_Total)
dbExecute(decimal_con, Q014e_Weighted_Cohort_Distribution_Projected)
dbExecute(decimal_con, Q014e_Weighted_Cohort_Distribution_Static )

dbExecute(decimal_con, Q015e21_Append_Selected_Static_Distribution_Y2_to_Y12_Projected)
dbExecute(decimal_con, Q015e22_Append_Distribution_Y2_to_Y12_Static)

# ----  Run Werner Program ----
# pull tbl_Program_Projections into R
# pivot to wide format
# pivoted data as input to program

# input + output (cbind or ...) = T_Predict_CIP_CRED_AGE
file = glue::glue("{lan}/development/csv/gh-source/testing/cip-cred-age.csv")
T_Predict_CIP_CRED_AGE <-read_csv(file)
dbWriteTable(decimal_con, "T_Predict_CIP_CRED_AGE", T_Predict_CIP_CRED_AGE)

dbExecute(decimal_con, qry_05_Flip_T_Predict_CIP_CRED_AGE_1)
dbExecute(decimal_con, qry_05_Flip_T_Predict_CIP_CRED_AGE_2)
dbExecute(decimal_con, qry_09_Delete_Selected_Static_Cohort_Dist_from_Projected)

dbExecute(decimal_con, qry_10a_Program_Dist_Count)
dbExecute(decimal_con, qry_10b_Program_Dist_Total)
dbExecute(decimal_con, qry_10c_Program_Dist_Distribution)
# check that the changes from Q013 to qry_10c don't affect next query (it was run earlier)
dbExecute(decimal_con, qry_12_LCP4_LCIPPC_Recode_9999)
dbExecute(decimal_con, qry_12a_Program_Dist_Count)
dbExecute(decimal_con, qry_12b_Program_Dist_Total)
dbExecute(decimal_con, qry_12c_Program_Dist_Distribution)
dbGetQuery(decimal_con, qry_12d_Check_Missing)

# ---- Apprenticeship Graduates ----

dbExecute(decimal_con, "drop table Q013b_Weight_Cohort_Dist_MAST_DOCT_Others")
dbExecute(decimal_con, "drop table Q013c_Weighted_Cohort_Dist")
dbExecute(decimal_con, "drop table Q013d_Weighted_Cohort_Dist_Total")
dbExecute(decimal_con, "drop table Q013e_Weighted_Cohort_Distribution")
dbExecute(decimal_con, "drop table Q012b_Weight_Cohort_Dist") 
dbExecute(decimal_con, "drop table Q012c_Weighted_Cohort_Dist") 
dbExecute(decimal_con, "drop table Q012c1_Weighted_Cohort_Dist_TTRAIN") 
dbExecute(decimal_con, "drop table Q012c2_Weighted_Cohort_Dist") 
dbExecute(decimal_con, "drop table Q012c3_Weighted_Cohort_Dist_Total") 
dbExecute(decimal_con, "drop table Q012c4_Weighted_Cohort_Distribution_Projected") 
dbExecute(decimal_con, "drop table Q012c5_Weighted_Cohort_Dist_TTRAIN") 
dbExecute(decimal_con, "drop table Q012d_Weighted_Cohort_Dist_Total") 



