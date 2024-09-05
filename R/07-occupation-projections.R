# This script computes the final NLS distributions based on program projections, labour supply distributions
# and occupation distributions.  

# Tables were created in the cohorts database process and populated here
# Then all of the labour supply and occ queries were moved around a bit to 
# incorporate looking for unknown labour supply and unknown occs in more steps 
# than was previously necessary for the LCIP2 and private institution proxies.

# QI: The model is rerun a second time and all of these queries are also re-run
# to create an indicator which measures the quality of predictability for each NOC projection.

# Age groups: 17 to 19, 20 to 24, 25 to 29, and 30 to 34, and 35 to 64
# Credentials: From Diploma, Associate Degree, and Certificate Outcomes Survey cohorts. 
# Survey years: 2019/2020 to 2030/2031 for PSSM 2019

#  Note: Q_1_Grad_Projections_by_Age_by_Program links to the following tables to exclude programs 
#  where Student Outcomes results not available or inappropriate 
#   - T_Exclude_from_Projections_LCIP4_CRED
#	  - T_Exclude_from_Projections_LCP4_CD
#	  - T_Exclude_from_Projections_PSSM_Credential
#
# When switching between models, copy Static/Projected into Cohort_Program_Distributions

library(tidyverse)
library(RODBC)
library(config)
library(DBI)

# ---- Configure LAN and file paths ----
db_config <- config::get("decimal")
lan <- config::get("lan")
my_schema <- config::get("myschema")

source("./sql/07-occupation-projections/occupation-projections.R")

# ---- Connection to decimal ----
db_config <- config::get("decimal")
decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")

# ---- Check for required data tables ----
# Derived tables
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Labour_Supply_Distribution"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Labour_Supply_Distribution_LCP2"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Labour_Supply_Distribution_No_TT"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Labour_Supply_Distribution_LCP2_No_TT"')))

dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Occupation_Distributions"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Occupation_Distributions_No_TT"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Occupation_Distributions_LCP2"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Occupation_Distributions_LCP2_No_TT"')))

dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Cohort_Program_Distributions_Projected"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Cohort_Program_Distributions_Static"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Cohort_Program_Distributions"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Graduate_Projections"')))

# Lookups
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_4DIGITS_CIP2016"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_6DIGITS_CIP2016"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_Exclude_from_Projections_LCP4_CD"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_Exclude_from_Projections_LCIP4_CRED"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_Exclude_from_Projections_PSSM_Credential"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_Exclude_from_Labour_Supply_Unknown_LCP2_Proxy"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."tbl_Age_Groups"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."tbl_Age_Groups_Rollup"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."tbl_NOC_Skill_Level_Aged_17_34"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_NOC_Skill_Type"')))

# ---- SQL Commands ----
#dbExecute(decimal_con, "DELETE FROM Cohort_Program_Distributions")
#dbExecute(decimal_con, "INSERT INTO Cohort_Program_Distributions 
#                        SELECT * FROM Cohort_Program_Distributions_Projected;")

dbExecute(decimal_con, "SELECT * INTO Cohort_Program_Distributions 
                        FROM Cohort_Program_Distributions_Projected;")

# Checks
dbGetQuery(decimal_con, Count_Cohort_Program_Distributions) 
dbGetQuery(decimal_con, Count_Labour_Supply_Distribution1) 
dbGetQuery(decimal_con, Count_Labour_Supply_Distribution2) 
dbGetQuery(decimal_con, Count_Occupation_Distributions1) # We want this to contain all of our PSSM Credentials
dbGetQuery(decimal_con, Count_Occupation_Distributions2) 
dbGetQuery(decimal_con, Occupation_Unknown) 

# creates mapping for LCIP4 to LCIP2 
dbExecute(decimal_con, Q_0_LCP2_LCP4) 

# use these to delete PTIB for running a model without private institutions
#dbExecute(decimal_con, Q_0a_Delete_Private_Inst_Labour_Supply_Distribution) 
#dbExecute(decimal_con, Q_0a_Delete_Private_Inst_Labour_Supply_Distribution_LCP2) 
#dbExecute(decimal_con, Q_0a_Delete_Private_Inst_Occupation_Distribution) 
#dbExecute(decimal_con, Q_0a_Delete_Private_Inst_Occupation_Distribution_LCP2) 

# essentially duplicates records (as a placeholder to insert graduate records for ptib later?)  
#dbExecute(decimal_con, Q_0b_Append_Private_Institution_Labour_Supply_Distribution) 
#dbExecute(decimal_con, Q_0b_Append_Private_Institution_Labour_Supply_Distribution_2D) 
#dbExecute(decimal_con, Q_0c_Append_Private_Institution_Occupation_Distribution) 
#dbExecute(decimal_con, Q_0c_Append_Private_Institution_Occupation_Distribution_2D) 

# ---- Q_1 Series ---- 
dbExecute(decimal_con, Q_1_Grad_Projections_by_Age_by_Program) 
dbExecute(decimal_con, Q_1_Grad_Projections_by_Age_by_Program_Static) 
dbGetQuery(decimal_con, Q_1b_Checking_Grads_by_Year_Excludes_CIPs)
dbExecute(decimal_con, Q_1c_Grad_Projections_by_Program) 
dbExecute(decimal_con, Q_1c_Grad_Projections_by_Program_LCP2)



# ---- Q_2 Series ---- 
dbExecute(decimal_con, Q_2_Labour_Supply_by_LCIP4_CRED)
dbExecute(decimal_con, Q_2a_Labour_Supply_Unknown) 
dbExecute(decimal_con, Q_2a2_Labour_Supply_Unknown_No_TT_Proxy) 
dbExecute(decimal_con, Q_2a3_Labour_Supply_by_LCIP4_CRED_No_TT_Proxy_Union) 
dbExecute(decimal_con, Q_2a4_Labour_Supply) 
dbExecute(decimal_con, Q_2b_Labour_Supply_Unknown) 
dbExecute(decimal_con, Q_2b2_Labour_Supply_Unknown_Private_Cred_Proxy) 
dbExecute(decimal_con, Q_2b3_Labour_Supply_by_LCIP4_CRED_Private_Cred_Proxy_Union) 
dbExecute(decimal_con, Q_2b4_Labour_Supply_Unknown) 
dbExecute(decimal_con, Q_2c_Labour_Supply_Unknown_LCP2_Proxy) 
dbExecute(decimal_con, Q_2c2_Labour_Supply_Unknown_LCP2_Proxy_Union) 
dbExecute(decimal_con, Q_2c3_Labour_Supply_Unknown) 
dbExecute(decimal_con, Q_2c4_Labour_Supply_Unknown_LCP2_Proxy_No_TT) 
dbExecute(decimal_con, Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union) 
dbExecute(decimal_con, Q_2d2_Labour_Supply) 
dbExecute(decimal_con, Q_2d2_Labour_Supply_Unknown) 
dbExecute(decimal_con, Q_2d3_Labour_Supply_Unknown_LCP2_Private_Cred_Proxy) 
dbExecute(decimal_con, Q_2d4_Labour_Supply_by_LCIP4_CRED_LCP2_LCP2_Private_Union) 
dbExecute(decimal_con, Q_2f_Labour_Supply) 
dbExecute(decimal_con, Q_2f2_Labour_Supply_Unknown) # numbers are low

dbExecute(decimal_con, "DROP TABLE Q_1c_Grad_Projections_by_Program")
dbExecute(decimal_con, "DROP TABLE Q_2_Labour_Supply_by_LCIP4_CRED")
dbExecute(decimal_con, "DROP TABLE Q_2a_Labour_Supply_Unknown")
dbExecute(decimal_con, "DROP TABLE Q_2a2_Labour_Supply_Unknown_No_TT_Proxy")
dbExecute(decimal_con, "DROP TABLE Q_2a3_Labour_Supply_by_LCIP4_CRED_No_TT_Proxy_Union")
dbExecute(decimal_con, "DROP TABLE Q_2b_Labour_Supply_Unknown")
dbExecute(decimal_con, "DROP TABLE Q_2b2_Labour_Supply_Unknown_Private_Cred_Proxy")
dbExecute(decimal_con, "DROP TABLE Q_2b3_Labour_Supply_by_LCIP4_CRED_Private_Cred_Proxy_Union")
dbExecute(decimal_con, "DROP TABLE Q_2b4_Labour_Supply_Unknown")
dbExecute(decimal_con, "DROP TABLE Q_2c_Labour_Supply_Unknown_LCP2_Proxy")
dbExecute(decimal_con, "DROP TABLE Q_2c2_Labour_Supply_Unknown_LCP2_Proxy_Union")
dbExecute(decimal_con, "DROP TABLE Q_2c3_Labour_Supply_Unknown")
dbExecute(decimal_con, "DROP TABLE Q_2c4_Labour_Supply_Unknown_LCP2_Proxy_No_TT")
dbExecute(decimal_con, "DROP TABLE Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union")
dbExecute(decimal_con, "DROP TABLE Q_2d2_Labour_Supply_Unknown")
dbExecute(decimal_con, "DROP TABLE Q_2d3_Labour_Supply_Unknown_LCP2_Private_Cred_Proxy")
dbExecute(decimal_con, "DROP TABLE Q_2d4_Labour_Supply_by_LCIP4_CRED_LCP2_LCP2_Private_Union")
dbExecute(decimal_con, "DROP TABLE Q_2f_Labour_Supply")
#dbExecute(decimal_con, "DROP TABLE tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union_tmp")
#dbExecute(decimal_con, "DROP TABLE tmp_tbl_Q_2a4_Labour_Supply_by_LCIP4_CRED_No_TT_Union_tmp")

# ---- Q_3 Series ---- 
dbExecute(decimal_con, Q_3_Occupations_by_LCIP4_CRED) 
dbExecute(decimal_con, Q_3b_Occupations_Unknown) # numbers too high
dbExecute(decimal_con, Q_3b11_Ocupations_Unknown_No_TT_Proxy) # numbers too high
dbExecute(decimal_con, q_3b12_Occupations_by_LCIP4_CRED_No_TT_Proxy_Union) 
dbExecute(decimal_con, Q_3b13_Occupations) 
dbExecute(decimal_con, Q_3b14_Occupations_Unknown) 
dbExecute(decimal_con, Q_3b2_Occupations_Unknown_Private_Cred_Proxy) 
dbExecute(decimal_con, Q_3b3_Occupations_by_LCIP4_CRED_Private_Cred_Proxy_Union) 
dbExecute(decimal_con, Q_3b4_Occupations_Unknown) 

dbExecute(decimal_con, Q_3c_Occupations_Unknown_LCP2_Proxy) 

dbExecute(decimal_con, Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union) 
dbExecute(decimal_con, Q_3d2_Occupations) 
dbExecute(decimal_con, Q_3d2_Occupations_Unknown) 
dbExecute(decimal_con, Q_3d21_Occupations_Unknown_LCP2_Proxy_No_TT) 
dbExecute(decimal_con, Q_3d22_Occupations_by_LCIP4_CRED_LCP2_No_T_Proxy_Union) 
dbExecute(decimal_con, Q_3d24_Occupations_Unknown) 
dbExecute(decimal_con, Q_3d3_Occupations_Unknown_LCP2_Private_Cred_Proxy) 
dbExecute(decimal_con, Q_3d4_Occupations_by_LCIP4_CRED_LCP2_LCP2_Private_Union) 

dbExecute(decimal_con, Q_3e_Occupations_Unknown) 
dbExecute(decimal_con, Q_3e2_Occupations_Unknown) 
dbExecute(decimal_con, Q_3e3_Occupations_by_LCIP4_CRED_LCP2_Union)
dbExecute(decimal_con, Q_3f_Occupations) 

dbExecute(decimal_con, "DROP TABLE Q_3_Occupations_by_LCIP4_CRED")
dbExecute(decimal_con, "DROP TABLE Q_3b_Occupations_Unknown")
dbExecute(decimal_con, "DROP TABLE Q_3b11_Ocupations_Unknown_No_TT_Proxy")
dbExecute(decimal_con, "DROP TABLE q_3b12_Occupations_by_LCIP4_CRED_No_TT_Proxy_Union")
dbExecute(decimal_con, "DROP TABLE Q_3b14_Occupations_Unknown")
dbExecute(decimal_con, "DROP TABLE Q_3b2_Occupations_Unknown_Private_Cred_Proxy")
dbExecute(decimal_con, "DROP TABLE Q_3b3_Occupations_by_LCIP4_CRED_Private_Cred_Proxy_Union")
dbExecute(decimal_con, "DROP TABLE Q_3b4_Occupations_Unknown")
dbExecute(decimal_con, "DROP TABLE Q_3c_Occupations_Unknown_LCP2_Proxy")
dbExecute(decimal_con, "DROP TABLE Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union")
dbExecute(decimal_con, "DROP TABLE Q_3d2_Occupations_Unknown")
dbExecute(decimal_con, "DROP TABLE Q_3d21_Occupations_Unknown_LCP2_Proxy_No_TT")
dbExecute(decimal_con, "DROP TABLE Q_3d22_Occupations_by_LCIP4_CRED_LCP2_No_T_Proxy_Union")
dbExecute(decimal_con, "DROP TABLE Q_3d24_Occupations_Unknown")
dbExecute(decimal_con, "DROP TABLE Q_3d3_Occupations_Unknown_LCP2_Private_Cred_Proxy")
dbExecute(decimal_con, "DROP TABLE Q_3d4_Occupations_by_LCIP4_CRED_LCP2_LCP2_Private_Union")
dbExecute(decimal_con, "DROP TABLE Q_3e_Occupations_Unknown")
dbExecute(decimal_con, "DROP TABLE Q_3e2_Occupations_Unknown")
dbExecute(decimal_con, "DROP TABLE Q_3e3_Occupations_by_LCIP4_CRED_LCP2_Union")
dbExecute(decimal_con, "DROP TABLE tmp_tbl_Q3b12_Occupations_by_LCIP4_CRED_No_TT_Union_tmp")
#dbExecute(decimal_con, "DROP TABLE tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union")

# ---- Q_4_NOC_D Series ---- Q_3e2_Occupations_Unknown 
dbExecute(decimal_con, Q_4_NOC_1D_Totals_by_PSSM_CRED) 
dbExecute(decimal_con, Q_4_NOC_1D_Totals_by_Year) 
dbExecute(decimal_con, Q_4_NOC_2D_Totals_by_PSSM_CRED) 
dbExecute(decimal_con, Q_4_NOC_2D_Totals_by_PSSM_CRED_Appendix) 
dbExecute(decimal_con, Q_4_NOC_2D_Totals_by_Year) 
dbExecute(decimal_con, Q_4_NOC_3D_Totals_by_PSSM_CRED) 
dbExecute(decimal_con, Q_4_NOC_3D_Totals_by_Year) 
dbExecute(decimal_con, Q_4_NOC_4D_Totals_by_PSSM_CRED) 
dbExecute(decimal_con, Q_4_NOC_4D_Totals_by_Year) 
dbExecute(decimal_con, Q_4_NOC_4D_Totals_by_Year_Input_for_Rounding) 

# ---- Q_4_NOC_Totals Series ---- 
dbGetQuery(decimal_con, Q_4_NOC_Totals_by_Year_and_PSSM_CRED) # FIXME Broken SQL
dbExecute(decimal_con, Q_4_NOC_Totals_by_Year) 
dbExecute(decimal_con, Q_4_NOC_Totals_by_Year_BC) 
dbExecute(decimal_con, Q_4_NOC_Totals_by_Year_Total)

dbExecute(decimal_con, "DROP TABLE Q_4_NOC_1D_Totals_by_PSSM_CRED")
dbExecute(decimal_con, "DROP TABLE Q_4_NOC_1D_Totals_by_Year")
dbExecute(decimal_con, "DROP TABLE Q_4_NOC_2D_Totals_by_PSSM_CRED")
dbExecute(decimal_con, "DROP TABLE Q_4_NOC_2D_Totals_by_PSSM_CRED_Appendix")
dbExecute(decimal_con, "DROP TABLE Q_4_NOC_2D_Totals_by_Year")
dbExecute(decimal_con, "DROP TABLE Q_4_NOC_3D_Totals_by_PSSM_CRED")
dbExecute(decimal_con, "DROP TABLE Q_4_NOC_3D_Totals_by_Year")
dbExecute(decimal_con, "DROP TABLE Q_4_NOC_4D_Totals_by_PSSM_CRED")
dbExecute(decimal_con, "DROP TABLE Q_4_NOC_4D_Totals_by_Year")
dbExecute(decimal_con, "DROP TABLE Q_4_NOC_4D_Totals_by_Year_Input_for_Rounding")

# ---- Q_5 Series ---- 
dbExecute(decimal_con, Q_5_NOC_Totals_by_Year_and_BC) 
dbExecute(decimal_con, Q_5_NOC_Totals_by_Year_and_BC_and_Total)
dbExecute(decimal_con, "DROP TABLE Q_4_NOC_Totals_by_Year")
dbExecute(decimal_con, "DROP TABLE Q_4_NOC_Totals_by_Year_BC")
dbExecute(decimal_con, "DROP TABLE Q_4_NOC_Totals_by_Year_Total")

# ---- Q_6 Series ---- 
dbExecute(decimal_con, Q_6_tmp_tbl_Model) 
#dbExecute(decimal_con, Q_6_tmp_tbl_Model_QI) 
dbExecute(decimal_con, Q_6_tmp_tbl_Model_Inc_Private_Inst) 
dbExecute(decimal_con, Q_6_tmp_tbl_Model_Program_Projection) 

dbExecute(decimal_con, "DROP TABLE Q_5_NOC_Totals_by_Year_and_BC")
dbExecute(decimal_con, "DROP TABLE Q_5_NOC_Totals_by_Year_and_BC_and_Total")

# ---- model with QI ----
dbExecute(decimal_con, Q_7_QI) 
dbGetQuery(decimal_con, Q_8_Labour_Supply_Total_by_Year) 
dbExecute(decimal_con, qry_10a_Model) 

# ---- public release ----
dbGetQuery(decimal_con, "SELECT * FROM qry_10a_Model_Public_Release_Union") %>% 
  write_csv(glue::glue("{lan}/reports-final/drafts/occupation_projections_public_release.csv"))
dbExecute(decimal_con, qry_10a_Model_Public_Release) 
dbExecute(decimal_con, qry_10a_Model_Public_Release_Suppressed) 
dbExecute(decimal_con, qry_10a_Model_Public_Release_Suppressed_Total) 
dbExecute(decimal_con, qry_10a_Model_Public_Release_Union) 


# ---- internal release ----
 dbExecute(decimal_con, qry_10a_Model_QI_PPCI) 
 dbExecute(decimal_con, qry_10a_Model_QI_PPCI_No_Supp) 
# dbExecute(decimal_con, qry_10a_Model_QI_PPCI_Suppressed) 
# dbExecute(decimal_con, qry_10a_Model_QI_PPCI_Suppressed_Total) 
dbGetQuery(decimal_con, "SELECT * FROM qry_10a_Model_QI_PPCI_No_Supp") %>% 
   write_csv(glue::glue("{lan}/reports-final/drafts/occupation_projections_internal_only.csv"))
 
dbExecute(decimal_con, qry_10b_Quality_Indicator) 
dbExecute(decimal_con, qry_10c_Coverage_Indicator) 
# dbExecute(decimal_con, qry_10d_tmp_No_Near_Completers) 

 
dbGetQuery(decimal_con, qry_LCIP4_CRED) 
#dbGetQuery(decimal_con, qry_LCIP4_CRED_Filtered_NOC) 
dbGetQuery(decimal_con, qry_LCIP4_CRED_NOC) 
# dbExecute(decimal_con, qry100_Grad_Skill_Level) 

# ---- public release ----
dbGetQuery(decimal_con, qry99_Presentations_Graduates_Appendix) %>% 
  mutate(across(where(is.numeric), round)) %>%
  write_csv(glue::glue("{lan}/reports-final/drafts/graduate_projections.csv"))


dbGetQuery(decimal_con, qry99_Presentations_Graduates_Appendix_by_Age_Group_Totals) 
# dbExecute(decimal_con, qry99_Presentations_Graduates_Appendix_Unrounded) 
# dbExecute(decimal_con, qry99_Presentations_Graduates_Including_those_not_projected) 
# dbExecute(decimal_con, qry99_Presentations_Labour_Force) 
# dbExecute(decimal_con, qry99_Presentations_Labour_Force_BC) 
# dbExecute(decimal_con, qry99_Presentations_Labour_Force_Overall) 
# dbExecute(decimal_con, qry99_Presentations_Occs) 
# dbExecute(decimal_con, qry99_Presentations_PPSCI_Graduates) 
# dbExecute(decimal_con, qry9999_NOC_4031_4032) 

# ---- Clean Up ----
dbExecute(decimal_con, "DROP TABLE tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union")
dbExecute(decimal_con, "DROP TABLE tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union")
dbExecute(decimal_con, "DROP TABLE Q_1_Grad_Projections_by_Age_by_Program")
dbExecute(decimal_con, "DROP TABLE Q_1_Grad_Projections_by_Age_by_Program_Static")
dbExecute(decimal_con, "DROP TABLE Q_1c_Grad_Projections_by_Program_LCP2")

# Lookups
dbExecute(decimal_con, "drop table INFOWARE_L_CIP_4DIGITS_CIP2016")
dbExecute(decimal_con, "drop table INFOWARE_L_CIP_6DIGITS_CIP2016")
dbExecute(decimal_con, "DROP TABLE T_NOC_Skill_Type")
dbExecute(decimal_con, "DROP TABLE tbl_NOC_Skill_Level_Aged_17_34")
dbExecute(decimal_con, "DROP TABLE T_Current_Region_PSSM_Rollup_Codes")
dbExecute(decimal_con, "DROP TABLE T_PSSM_CRED_RECODE")
dbExecute(decimal_con, "DROP TABLE INFOWARE_L_CIP_4DIGITS_CIP2016")
dbExecute(decimal_con, "DROP TABLE INFOWARE_L_CIP_6DIGITS_CIP2016")
dbExecute(decimal_con, "DROP TABLE T_Exclude_from_Projections_LCP4_CD")
dbExecute(decimal_con, "DROP TABLE T_Exclude_from_Projections_LCIP4_CRED")
dbExecute(decimal_con, "DROP TABLE T_Exclude_from_Projections_PSSM_Credential")
dbExecute(decimal_con, "DROP TABLE tbl_Age_Groups")
dbExecute(decimal_con, "DROP TABLE tbl_Age_Groups_Rollup")
dbExecute(decimal_con, "DROP TABLE T_Exclude_from_Labour_Supply_Unknown_LCP2_Proxy")

# Keep 
dbExists(decimal_con, "")
dbExists(decimal_con, "")














