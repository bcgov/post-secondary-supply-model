# ---- Required Tables (Access) ----
# INFOWARE_PROGRAMS_HIST_PRGMID_XREF
# INFOWARE_L_CIP_2DIGITS_CIP2016
# INFOWARE_L_CIP_4DIGITS_CIP2016
# INFOWARE_L_CIP_6DIGITS_CIP2016
# INFOWARE_PROGRAMS
# DACSO_STP_ProgramsCIP4_XWALK_ALL_2018

library(tidyverse)
library(RODBC)
library(config)

source("./sql/02a-bgs-program-matching.R")

#---- Connect to Outcomes Database ----
connection <- config::get("connection")$outcomes_bgs
con <- odbcDriverConnect(connection)

sqlQuery(con, "SELECT COUNT(*) FROM INFOWARE_BGS_DIST_12_161")
sqlQuery(con, "SELECT COUNT(*) FROM INFOWARE_BGS_DIST_12_16")

# ----  ----

sqlQuery(con, qry_Make_T_BGS_Data_for_OutcomesMatching_step1 )
sqlQuery(con, qry_Make_T_BGS_Data_for_OutcomesMatching_step2)
sqlQuery(con, qry_Update_PSSM_CREDENTIAL)
sqlQuery(con, qry_Check_BGS_CIP_Data)


# Export NEW_T_BGS_DATA_FINAL_for_OutcomesMatching2018 to SSMS
# ----  ----


# ----  ----


close(con)
