# ---- Required INFOWARE TABLES
# INFOWARE_PROGRAMS_HIST_PRGMID_XREF
# INFOWARE_C_OUTC_CLEAN_SHORT_RESP - I don't think we need this one
# INFOWARE_L_CIP_2DIGITS_CIP2016
# INFOWARE_L_CIP_4DIGITS_CIP2016
# INFOWARE_L_CIP_6DIGITS_CIP2016
# INFOWARE_PROGRAMS
# DACSO_STP_ProgramsCIP4_XWALK_ALL_2018

# DACSO_Programs_Seen_02_to_18                           
# dbo_STP_Credential_Non_Dup_Programs_DACSO               
# STP_Credential_Non_Dup_Programs_DACSO_CIPS_CHANGED_2018 
# tbl_STP_PSI_CODE_INST_CD_Lookup    
# tbl_SubmCd_Lookup  

library(tidyverse)
library(RODBC)
library(config)

source("./sql/dacso_program_matching.R")

#---- Connect to Outcomes Database ----
connection <- config::get("connection")$outcomes_dacso
con <- odbcDriverConnect(connection)

sqlTables(con,  tableType = c("TABLE")) %>% pull(TABLE_NAME) # tableType = "SYNONYM" gives tables linked to ORACLE