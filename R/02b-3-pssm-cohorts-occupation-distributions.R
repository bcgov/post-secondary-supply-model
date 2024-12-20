# This script processes cohort data from student outcomes and derives occupation distributions.
# Outcomes data has been standardized so all cohorts/surveys are combined in a single dataset before
# processing.
#
#   Create weights for occupational distribution (Weight_OCC). 
#
# Includes records with a labour force status for those aged 17 to 64, 
# Includes those with an invalid NOC where 100% of CIP is invalid, as the cohort number.

# Note:  create Weight_Age is used to calculate the age for the private institution credentials 
# and needed if the data set doesn’t have age.
# Check PDCT or PDDP	
# DACSO_Q008_Z01_Base_OCC documentation - records should be the same as equivalent NLS query


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
source(glue::glue("./sql/02b-pssm-cohorts/02b-pssm-cohorts-occupation-distributions.R"))

# ---- Check for required data tables ----
# Load necessary libraries
library(DBI)
library(glue)
library(assertthat)

# List of required tables
required_tables <- c(
  "t_cohorts_recoded",
  "t_current_region_pssm_codes",
  "t_current_region_pssm_rollup_codes",
  "tmp_tbl_Weights_NLS",
  "T_NOC_Broad_Categories",
  "Occupation_Distributions_Stat_Can"
)

# Check for required data tables in the database
for (table_name in required_tables) {
  # Build SQL statement
  full_table_name <- SQL(glue::glue('"{my_schema}"."{table_name}"'))
  
  # Assert that the table exists in the database
  assert_that(
    dbExistsTable(decimal_con, full_table_name),
    msg = paste("Error:", table_name, "does not exist in schema", my_schema)
  )
}


# ---- Execute SQL ----
dbExecute(decimal_con, DACSO_Q008_Z01_Base_OCC) 
dbExecute(decimal_con, DACSO_Q008_Z02a_Base)
dbExecute(decimal_con, DACSO_Q008_Z02b_Respondents)
dbExecute(decimal_con, DACSO_Q008_Z02b_Respondents_NOC_99999)
dbExecute(decimal_con, DACSO_Q008_Z02b_Respondents_NOC_99999_100_perc)
dbExecute(decimal_con, DACSO_Q008_Z02b_Respondents_Union)
dbExecute(decimal_con, DACSO_Q008_Z02c_Weight)
dbExecute(decimal_con, DACSO_Q008_Z03_Weight_Total)
dbExecute(decimal_con, DACSO_Q008_Z04_Weight_Adj_Fac)
dbExecute(decimal_con, DACSO_Q008_Z05_Weight_OCC)
dbExecute(decimal_con, DACSO_Q008_Z05b_Finding_NLS2_Missing)
dbExecute(decimal_con, DACSO_Q008_Z05b_NOC4D_NLS_XTab)
dbExecute(decimal_con, DACSO_Q008_Z05b_Weight_Comparison)

# dbExecute(decimal_con, DACSO_Q008_Z06_Add_Weight_OCC_Field)
dbExecute(decimal_con, DACSO_Q008_Z07_Weight_OCC_Null)
dbExecute(decimal_con, "ALTER TABLE T_Cohorts_Recoded ALTER COLUMN Weight_OCC FLOAT NULL")
dbExecute(decimal_con, "ALTER TABLE T_Cohorts_Recoded ALTER COLUMN Weight_Age FLOAT NULL")
dbExecute(decimal_con, DACSO_Q008_Z08_Weight_OCC_Update) 
dbExecute(decimal_con, DACSO_Q008_Z08_Weight_OCC_Update_NOC_99999_100_perc)
dbGetQuery(decimal_con, DACSO_Q008_Z09_Check_Weights)
dbExecute(decimal_con, "DROP TABLE DACSO_Q008_Z01_Base_OCC")
dbExecute(decimal_con, "DROP TABLE DACSO_Q008_Z02a_Base")
dbExecute(decimal_con, "DROP TABLE DACSO_Q008_Z02b_Respondents")
dbExecute(decimal_con, "DROP TABLE DACSO_Q008_Z02b_Respondents_NOC_99999")
dbExecute(decimal_con, "DROP TABLE DACSO_Q008_Z02b_Respondents_NOC_99999_100_perc")
dbExecute(decimal_con, "DROP TABLE DACSO_Q008_Z02b_Respondents_Union")
dbExecute(decimal_con, "DROP TABLE DACSO_Q008_Z02c_Weight")
dbExecute(decimal_con, "DROP TABLE DACSO_Q008_Z03_Weight_Total")
dbExecute(decimal_con, "DROP TABLE DACSO_Q008_Z04_Weight_Adj_Fac")
dbExecute(decimal_con, "DROP TABLE DACSO_Q008_Z05b_Finding_NLS2_Missing")
dbExecute(decimal_con, "DROP TABLE DACSO_Q008_Z05b_NOC4D_NLS_XTab")
dbExecute(decimal_con, "DROP TABLE DACSO_Q008_Z05b_Weight_Comparison")

dbExecute(decimal_con, DACSO_Q009_Weight_Occs)
dbExecute(decimal_con, DACSO_Q009_Weighted_Occs_2D)
dbExecute(decimal_con, DACSO_Q009_Weighted_Occs_2D_BC)
dbExecute(decimal_con, DACSO_Q009_Weighted_Occs_2D_BC_No_TT)
dbExecute(decimal_con, DACSO_Q009_Weighted_Occs_2D_No_TT)
dbExecute(decimal_con, DACSO_Q009_Weighted_Occs_Total_2D)
dbExecute(decimal_con, DACSO_Q009_Weighted_Occs_Total_2D_BC)
dbExecute(decimal_con, DACSO_Q009_Weighted_Occs_Total_2D_BC_No_TT)
dbExecute(decimal_con, DACSO_Q009_Weighted_Occs_Total_2D_No_TT)
dbExecute(decimal_con, DACSO_Q009b_Weighted_Occs)
dbExecute(decimal_con, DACSO_Q009b_Weighted_Occs_No_TT)
dbExecute(decimal_con, DACSO_Q009b_Weighted_Occs_Total)
dbExecute(decimal_con, DACSO_Q009b_Weighted_Occs_Total_No_TT)

dbExecute(decimal_con, DACSO_Q010_Weighted_Occs_Dist)
dbExecute(decimal_con, DACSO_Q010_Weighted_Occs_Dist_2D)
dbExecute(decimal_con, DACSO_Q010_Weighted_Occs_Dist_2D_BC)
dbExecute(decimal_con, DACSO_Q010_Weighted_Occs_Dist_2D_BC_No_TT)
dbExecute(decimal_con, DACSO_Q010_Weighted_Occs_Dist_2D_No_TT)
dbExecute(decimal_con, DACSO_Q010_Weighted_Occs_Dist_No_TT)

dbExecute(decimal_con, "DROP TABLE DACSO_Q009_Weight_Occs")
dbExecute(decimal_con, "DROP TABLE DACSO_Q009_Weighted_Occs_2D")
dbExecute(decimal_con, "DROP TABLE DACSO_Q009_Weighted_Occs_2D_BC")
dbExecute(decimal_con, "DROP TABLE DACSO_Q009_Weighted_Occs_2D_BC_No_TT")
dbExecute(decimal_con, "DROP TABLE DACSO_Q009_Weighted_Occs_2D_No_TT")
dbExecute(decimal_con, "DROP TABLE DACSO_Q009_Weighted_Occs_Total_2D")
dbExecute(decimal_con, "DROP TABLE DACSO_Q009_Weighted_Occs_Total_2D_BC")
dbExecute(decimal_con, "DROP TABLE DACSO_Q009_Weighted_Occs_Total_2D_BC_No_TT")
dbExecute(decimal_con, "DROP TABLE DACSO_Q009_Weighted_Occs_Total_2D_No_TT")
dbExecute(decimal_con, "DROP TABLE DACSO_Q009b_Weighted_Occs")
dbExecute(decimal_con, "DROP TABLE dacso_q009b_weighted_occs_no_tt")
dbExecute(decimal_con, "DROP TABLE DACSO_Q009b_Weighted_Occs_Total")
dbExecute(decimal_con, "DROP TABLE dacso_q009b_weighted_occs_total_no_tt")

occs_def <- c(Survey = "nvarchar(50)", PSSM_Credential  = "nvarchar(50)", PSSM_CRED  = "nvarchar(50)",  LCP4_CD = "nvarchar(50)", 
              TTRAIN = "nvarchar(50)", LCIP4_CRED = "nvarchar(50)", LCIP2_CRED = "nvarchar(50)", NOC = "nvarchar(50)" , 
              Current_Region_PSSM_Code_Rollup = "integer", Age_Group_Rollup = "integer", Count = "float", Total = "float", Percent = "float")


if(!dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Occupation_Distributions"')))){
  dbCreateTable(decimal_con, SQL(glue::glue('"{my_schema}"."Occupation_Distributions"')),  occs_def)
} 
if(!dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Occupation_Distributions_No_TT"')))){
  dbCreateTable(decimal_con, SQL(glue::glue('"{my_schema}"."Occupation_Distributions_No_TT"')),  occs_def)
}

dbExecute(decimal_con, DACSO_Q010a0_Delete_Occupational_Distribution)
dbExecute(decimal_con, DACSO_Q010a0_Delete_Occupational_Distribution_No_TT)
#dbExecute(decimal_con, DACSO_Q010a0_Delete_Occupational_Distribution_No_TT_QI)
#dbExecute(decimal_con, DACSO_Q010a0_Delete_Occupational_Distribution_QI)
dbExecute(decimal_con, DACSO_Q010a1_Append_Occupational_Distribution)
dbExecute(decimal_con, DACSO_Q010a1_Append_Occupational_Distribution_No_TT)

occs_def <- c(Survey = "nvarchar(50)", PSSM_Credential  = "nvarchar(50)", PSSM_CRED  = "nvarchar(50)",  LCP2_CD = "nvarchar(50)", 
              TTRAIN = "nvarchar(50)", LCIP2_CRED = "nvarchar(50)", NOC = "nvarchar(50)" , 
              Current_Region_PSSM_Code_Rollup = "integer", Age_Group_Rollup = "integer", Count = "float", Total = "float", Percent = "float")



if(!dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Occupation_Distributions_LCP2"')))){
  dbCreateTable(decimal_con, SQL(glue::glue('"{my_schema}"."Occupation_Distributions_LCP2"')),  occs_def)
} 
if(!dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Occupation_Distributions_LCP2_No_TT"')))){
  dbCreateTable(decimal_con, SQL(glue::glue('"{my_schema}"."Occupation_Distributions_LCP2_No_TT"')),  occs_def)
}

dbExecute(decimal_con, DACSO_Q010b0_Delete_Occupational_Distribution_LCP2)
dbExecute(decimal_con, DACSO_Q010b0_Delete_Occupational_Distribution_LCP2_No_TT)
# dbExecute(decimal_con, DACSO_Q010b0_Delete_Occupational_Distribution_LCP2_No_TT_QI)
# dbExecute(decimal_con, DACSO_Q010b0_Delete_Occupational_Distribution_LCP2_QI)
dbExecute(decimal_con, DACSO_Q010b1_Append_Occupational_Distribution_LCP2)
dbExecute(decimal_con, DACSO_Q010b1_Append_Occupational_Distribution_LCP2_No_TT)

occs_def <- c(Survey = "nvarchar(50)", PSSM_Credential  = "nvarchar(50)", PSSM_CRED  = "nvarchar(50)",  LCP2_CD = "nvarchar(50)", 
              TTRAIN = "nvarchar(50)", LCIP2_CRED = "nvarchar(50)", NOC = "nvarchar(50)" ,  Count = "float", Total = "float", Percent = "float")

if(!dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Occupation_Distributions_LCP2_BC"')))){
  dbCreateTable(decimal_con, SQL(glue::glue('"{my_schema}"."Occupation_Distributions_LCP2_BC"')),  occs_def)
}

if(!dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Occupation_Distributions_LCP2_BC_No_TT"')))){
  dbCreateTable(decimal_con, "Occupation_Distributions_LCP2_BC_No_TT",  occs_def)
}

dbExecute(decimal_con, DACSO_Q010c0_Delete_Occupational_Distribution_LCP2_BC)
dbExecute(decimal_con, DACSO_Q010c0_Delete_Occupational_Distribution_LCP2_BC_No_TT)
# dbExecute(decimal_con, DACSO_Q010c0_Delete_Occupational_Distribution_LCP2_BC_No_TT_QI)
# dbExecute(decimal_con, DACSO_Q010c0_Delete_Occupational_Distribution_LCP2_BC_QI)
dbExecute(decimal_con, DACSO_Q010c1_Append_Occupational_Distribution_LCP2_BC)
dbExecute(decimal_con, DACSO_Q010c1_Append_Occupational_Distribution_LCP2_BC_No_TT)

dbExecute(decimal_con, "DROP TABLE DACSO_Q010_Weighted_Occs_Dist_2D_BC_No_TT")
dbExecute(decimal_con, "DROP TABLE dacso_q010_weighted_occs_dist_2d_no_tt")
dbExecute(decimal_con, "DROP TABLE DACSO_Q010_Weighted_Occs_Dist_No_TT")
dbExecute(decimal_con, "DROP TABLE DACSO_Q010_Weighted_Occs_Dist")
dbExecute(decimal_con, "DROP TABLE DACSO_Q010_Weighted_Occs_Dist_2D")
dbExecute(decimal_con, "DROP TABLE DACSO_Q010_Weighted_Occs_Dist_2D_BC")

dbExecute(decimal_con, DACSO_Q010d1_Delete_PDEG_CIP_Cluster_07_Law_New_Labour_Supply)
#dbExecute(decimal_con, DACSO_Q010d1_Delete_PDEG_CIP_Cluster_07_Law_New_Labour_Supply_QI)
dbExecute(decimal_con, DACSO_Q010d2_NLS_PDEG_07_Count)
dbExecute(decimal_con, DACSO_Q010d3_NLS_PDEG_07_Subtotal)
dbExecute(decimal_con, DACSO_Q010d4_NLS_PDEG_07_Total)
dbExecute(decimal_con, DACSO_Q010d5_NLS_PDEG_07_Weighted_New_Labour_Supply)
dbExecute(decimal_con, DACSO_Q010d6_Append_NLS_PDEG_07_New_Labour_Supply)

dbExecute(decimal_con, "DROP TABLE DACSO_Q010d5_NLS_PDEG_07_Weighted_New_Labour_Supply")
dbExecute(decimal_con, "DROP TABLE DACSO_Q010d2_NLS_PDEG_07_Count")
dbExecute(decimal_con, "DROP TABLE DACSO_Q010d3_NLS_PDEG_07_Subtotal")
dbExecute(decimal_con, "DROP TABLE DACSO_Q010d4_NLS_PDEG_07_Total")
          
dbExecute(decimal_con, DACSO_Q010e1_Delete_PDEG_CIP_Cluster_07_Law_Occupation_Dist)
#dbExecute(decimal_con, DACSO_Q010e1_Delete_PDEG_CIP_Cluster_07_Law_Occupation_Dist_QI)
dbExecute(decimal_con, DACSO_Q010e2_Weighted_Occs_PDEG_07)
dbExecute(decimal_con, DACSO_Q010e3_Weighted_Occs_Total_PDEG_07)
dbExecute(decimal_con, DACSO_Q010e4_Weighted_Occs_Dist_PDEG_07)
dbExecute(decimal_con, DACSO_Q010e5_Append_Occupational_Distribution_PDEG_07)

dbExecute(decimal_con, "DROP TABLE DACSO_Q010e2_Weighted_Occs_PDEG_07")
dbExecute(decimal_con, "DROP TABLE DACSO_Q010e3_Weighted_Occs_Total_PDEG_07")
dbExecute(decimal_con, "DROP TABLE DACSO_Q010e4_Weighted_Occs_Dist_PDEG_07")

dbExecute(decimal_con, DACSO_Q99A_ENDDT_IMPUTED)

if (qi_run == TRUE | ptib_run == TRUE) {
  # do  nothing
} 
if (regular_run == TRUE){
  tryCatch({
    dbExecute(decimal_con, DACSO_qry99_Suppression_Public_Release_NOC)
  }, error = function(e) {
    print(paste("Error:", e$message))
    stop()
  })
}

dbExecute(decimal_con, "DELETE 
                        FROM Occupation_Distributions
                        WHERE (((Occupation_Distributions.Survey)='2021 Census PSSM 2023-2024'));")

# uncomment if running for the first time
# dbExecute(decimal_con, "ALTER TABLE Occupation_Distributions_Stat_Can ADD TTRAIN NVARCHAR(50)")
# dbExecute(decimal_con, "ALTER TABLE Occupation_Distributions_Stat_Can ADD LCIP2_CRED NVARCHAR(50)")
dbExecute(decimal_con, "INSERT INTO Occupation_Distributions ([Survey]
      ,[PSSM_Credential]
      ,[PSSM_CRED]
      ,[LCP4_CD]
      ,[TTRAIN]
      ,[LCIP4_CRED]
      ,[LCIP2_CRED]
      ,[NOC]
      ,[Current_Region_PSSM_Code_Rollup]
      ,[Age_Group_Rollup]
      ,[Count]
      ,[Total]
      ,[Percent])
          SELECT '2021 Census PSSM 2023-2024' as Survey
      ,[PSSM_Credential]
      ,[PSSM_CRED]
      ,[LCP4_CD]
      ,[TTRAIN]
      ,[LCIP4_CRED]
      ,[LCIP2_CRED]
      ,[NOC]
      ,[Current_Region_PSSM_Code_Rollup]
      ,[Age_Group_Rollup]
      ,[Count]
      ,[Total]
      ,[Percent] FROM Occupation_Distributions_Stat_Can")

# ---- Clean Up ----
dbExecute(decimal_con, "DROP TABLE tmp_tbl_Weights_OCC")
dbExecute(decimal_con, "DROP TABLE tmp_tbl_Weights_NLS")
dbExecute(decimal_con, "DROP TABLE tbl_Age_Groups")
dbExecute(decimal_con, "DROP TABLE tbl_Age_Groups_Rollup")
dbExecute(decimal_con, "DROP TABLE tbl_Age")
dbExecute(decimal_con, "DROP TABLE T_PSSM_Credential_Grouping")
dbExecute(decimal_con, "DROP TABLE T_Weights")
dbExecute(decimal_con, "DROP TABLE t_year_survey_year")
dbExecute(decimal_con, "DROP TABLE t_current_region_pssm_codes")
dbExecute(decimal_con, "DROP TABLE t_current_region_pssm_rollup_codes")
dbExecute(decimal_con, "DROP TABLE t_current_region_pssm_rollup_codes_bc")

# ---- Keep ----
dbExistsTable(decimal_con, "Occupation_Distributions")
dbExistsTable(decimal_con, "Occupation_Distributions_No_TT")
dbExistsTable(decimal_con, "Occupation_Distributions_LCP2")
dbExistsTable(decimal_con, "Occupation_Distributions_LCP2_No_TT")
dbExistsTable(decimal_con, "Occupation_Distributions_LCP2_BC")
dbExistsTable(decimal_con, "Occupation_Distributions_LCP2_BC_No_TT")


dbDisconnect(decimal_con)



