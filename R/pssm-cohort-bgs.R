# ---- Required Tables ----
# See outcomes surveys raw data documentation
# T_BGS_Data_Final (from last runs PSSM)

library(tidyverse)
library(RODBC)
library(config)
library(glue)
library(DBI)

#lan <- safepaths::get_network_path()
source(glue("{lan}/development/sql/gh-source/02b-pssm-cohort-bgs.R"))

#---- Connect to Outcomes Database ----
connection <- config::get("connection")$outcomes_cohorts
con <- odbcDriverConnect(connection)

sqlQuery(con, BGS_Q001_BGS_Data_2018_2019)
sqlQuery(con, BGS_Q001b_INST_Recode)
sqlQuery(con, BGS_Q001c_Update_CIPs_After_Program_Matching)
sqlQuery(con, BGS_Q002_LCP4_CRED)
sqlQuery(con, BGS_Q003b_Add_CURRENT_REGION_PSSM)
sqlQuery(con, BGS_Q003b_Add_CURRENT_REGION_PSSM2)
sqlQuery(con, BGS_Q003c_Derived_And_Weights)
sqlQuery(con, BGS_Q005_1b1_Delete_Cohort)
sqlQuery(con, BGS_Q005_1b2_Cohort_Recoded)
sqlQuery(con, BGS_Q99A_ENDDT_IMPUTED)
                
close(con)