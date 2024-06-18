# ******************************************************************************
# Aligns CIP codes between DAC survey and STP data
# Required Tables
#   dbo_STP_Credential_Non_Dup_Programs_DACSO
#   tbl_STP_PSI_CODE_INST_CD_Lookup 
#   DACSO_STP_ProgramsCIP4_XWALK_ALL_2018
# ******************************************************************************
library(tidyverse)
library(RODBC)
library(config)

lan < - config::get("lan")
source(glue("{lan}/development/sql/git-to-source/02a-dacso-program-matching.R"))

#---- Connect to Outcomes Database ----
connection <- config::get("connection")$outcomes_dacso
con <- odbcDriverConnect(connection)

# readr::read_delim(file = "./cred_non_dup.txt", delim = "\t", col_names = TRUE)

# ---- Restructure and populate imported STP data ---- 
sqlQuery(con, "ALTER TABLE STP_Credential_Non_Dup_Programs_DACSO 
               ADD COLUMN Already_Matched VARCHAR(255), New_Auto_Match VARCHAR(255), New_Manual_Match VARCHAR(255);")

sqlQuery(con, "UPDATE dbo_STP_Credential_Non_Dup_Programs_DACSO 
INNER JOIN tbl_STP_PSI_CODE_INST_CD_Lookup ON dbo_STP_Credential_Non_Dup_Programs_DACSO.PSI_CODE = tbl_STP_PSI_CODE_INST_CD_Lookup.PSI_CODE 
SET dbo_STP_Credential_Non_Dup_Programs_DACSO.COCI_INST_CD = [tbl_STP_PSI_CODE_INST_CD_Lookup].[COCI_INST_CD];")

# ---- Matched programs with new CIPs ----
sqlQuery(con, qry_STP_Credential_DACSO_Programs_AlreadyMatched)
sqlQuery(con, qry_STP_Credential_DACSO_Programs_AlreadyMatched_b)

# ---- Newly matched programs with new CIPS ----
sqlQuery(con, qry_STP_Credential_DACSO_Programs_NewMatches_a)
sqlQuery(con, qry_STP_Credential_DACSO_Programs_NewMatches_a_step2)
sqlQuery(con, qry_STP_Credential_DACSO_Programs_NewMatches_b)
sqlQuery(con, qry_STP_Credential_DACSO_Programs_NewMatches_b_step2)

# ---- Search for programs requiring manual work ----
# This query is missing and needs to be created - see documentation
sqlQuery(con, qry_STP_Cred_DACSO_Programs_to_Match)
sqlQuery(con, qry_Make_DACSO_Programs_Seen)



