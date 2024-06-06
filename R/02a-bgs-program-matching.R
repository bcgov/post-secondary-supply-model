# ---- Required Tables (Access) ----
# infoware_bgs_dist_15_19
# infoware_bgs_dist_13_17
# infoware_bgs_dist_20_22 (or one with later years)
# infoware_bgs_cohort_info
# Credential_non_Dup 

# DACSO_STP_ProgramsCIP4_XWALK_ALL_2018
# INFOWARE_PROGRAMS_HIST_PRGMID_XREF

library(tidyverse)
library(RODBC)
library(config)
library(odbc)
library(DBI)

source("./sql/02a-bgs-program-matching.R")

#---- Connect to Outcomes Database ----
connection <- config::get("connection")$outcomes_bgs
con <- odbcDriverConnect(connection)
sqlTables(con, tableType = c("TABLE", "SYNONYM"))

# ---- Build T_DATA_FINAL table with all years ----
sqlQuery(con, qry_Make_T_BGS_Data_for_OutcomesMatching_step1)
sqlQuery(con, qry_Make_T_BGS_Data_for_OutcomesMatching_step2)
sqlQuery(con, "ALTER TABLE T_BGS_Data_Final_for_OutcomesMatching2020 ADD COLUMN PSSM_CREDENTIAL VARCHAR(255);")
sqlQuery(con, qry_Update_PSSM_CREDENTIAL)

# if this query produces funny results, check out the 
sqlQuery(con, qry_Check_BGS_CIP_Data)

# ---- Export and flip to decimal  ----
# Export NEW_T_BGS_DATA_FINAL_for_OutcomesMatching2020 to SSMS
connection <- config::get("decimal2019")
con <- dbConnect(odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")

dbGetQuery(con, qry_BGS_STP_Credential_Programs)
dbGetQuery(con, "SELECT PSI_CREDENTIAL_CIP, OUTCOMES_CRED, OUTCOMES_CIP_CODE_4, OUTCOMES_CIP_CODE_4_NAME, FINAL_CIP_CODE_4, FINAL_CIP_CODE_4_NAME, 
                  FINAL_CIP_CODE_2, FINAL_CIP_CODE_2_NAME, FINAL_CIP_CLUSTER_CODE, FINAL_CIP_CLUSTER_NAME, STP_CIP_CODE_4, STP_CIP_CODE_4_NAME, COUNT(*) AS Expr1
                  INTO  Credential_Non_Dup_BGS_STP_CIP4_Cleaning
                  FROM  Credential_Non_Dup
                  GROUP BY PSI_CREDENTIAL_CIP, OUTCOMES_CRED, OUTCOMES_CIP_CODE_4, OUTCOMES_CIP_CODE_4_NAME, FINAL_CIP_CODE_4, FINAL_CIP_CODE_4_NAME, 
                         FINAL_CIP_CODE_2, FINAL_CIP_CODE_2_NAME, FINAL_CIP_CLUSTER_CODE, FINAL_CIP_CLUSTER_NAME, STP_CIP_CODE_4, STP_CIP_CODE_4_NAME
                  HAVING OUTCOMES_CRED = 'BGS';")

# ---- Export and flip to Outcomes  ----
# Export Credential_Non_Dup_BGS_STP_CIP4_Cleaning to Access
connection <- config::get("connection")$outcomes_bgs
con <- odbcDriverConnect(connection)

sqlQuery(con, qry_Clean_BGS_STP_CIP_Step1)
# Manual work here to populate blank CIP4s and CIP2.  See documentation.
sqlQuery(con, qry_Clean_BGS_STP_CIP_Step2)
sqlQuery(con, qry_Clean_BGS_STP_CIP_Step3)
# Manual work here to mark “Invalid 4-digit CIP” for remaining blanks.  See documentation.

# ---- Export and flip to decimal  ----
#Export Credential_Non_Dup_BGS_STP_CIP4_Cleaning table to SSMS
con <- dbConnect(odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")

dbGetQuery(con, "UPDATE Credential_Non_Dup
                 SET STP_CIP_CODE_4 = Credential_Non_Dup_BGS_STP_CIP4_Cleaning.STP_CIP_CODE_4, STP_CIP_CODE_4_NAME = Credential_Non_Dup_BGS_STP_CIP4_Cleaning.STP_CIP_CODE_4_NAME,
				             STP_CIP_CODE_2 = Credential_Non_Dup_BGS_STP_CIP4_Cleaning.STP_CIP_CODE_2, STP_CIP_CODE_2_NAME = Credential_Non_Dup_BGS_STP_CIP4_Cleaning.STP_CIP_CODE_2_NAME
                 FROM Credential_Non_Dup 
                 INNER JOIN Credential_Non_Dup_BGS_STP_CIP4_Cleaning 
                  ON Credential_Non_Dup.PSI_CREDENTIAL_CIP = Credential_Non_Dup_BGS_STP_CIP4_Cleaning.PSI_CREDENTIAL_CIP 
                  AND Credential_Non_Dup.OUTCOMES_CRED = Credential_Non_Dup_BGS_STP_CIP4_Cleaning.OUTCOMES_CRED;")

# RUN THIS FIRST if missing Credential_Non_Dup.PSI_PEN
dbgetQuery(con, "UPDATE dbo.Credential_Non_Dup SET PSI_PEN = STP_Credential.PSI_PEN
                 FROM dbo.Credential_Non_Dup 
                 JOIN STP_Credential ON STP_Credential.ID = dbo.Credential_Non_Dup.id;")

dbgetQuery(con, qry01_Match_BGS_STP_Credential_on_PEN)
dbgetQuery(con, qry01b_Match_BGS_STP_Credential_Add_Cols)
dbgetQuery(con, qry01c_Match_BGS_STP_Credential_UpdateVisaStatus)
dbgetQuery(con, qry02_Match_BGS_STP_Credential_Match_Inst)
dbgetQuery(con, qry03_Match_BGS_STP_Credential_Match_AwardYear)
dbgetQuery(con, qry04_Match_BGS_STP_Credential_Match_CIPCODE4)
dbgetQuery(con, qry05_Match_BGS_STP_Credential_Match_CIPCODE2)
dbgetQuery(con, qry06_Match_BGS_STP_Credential_MatchAll3_CIP4Flag)
dbgetQuery(con, qry07_Match_BGS_STP_Credential_MatchAll3_CIP2Flag)

# ---- Export and flip to Outcomes  ----
# Export BGS_Matching_STP_Credential_PEN to Access
# Export a copy of Credential_non_Dup, or create a link to it
connection <- config::get("connection")$outcomes_bgs
con <- odbcDriverConnect(connection)

sqlQuery(con,"ALTER BGS_Matching_STP_Credential_PEN TABLE ADD COLUMN BGS_program Varchar(255);")
sqlQuery(con, qry_update_BGS_program)
sqlQuery(con, qry_Make_Credential_Non_Dup_BGS_IDs)
sqlQuery(con,"ALTER TABLE Credential_Non_Dup_BGS_Ids ADD COLUMN Final_Consider_A_Match VARCHAR(255), Final_Probable_Match VARCHAR(255), USE_BGS_CIP VARCHAR(255);")
sqlQuery(con,"ALTER TABLE BGS_Matching_STP_Credential_PEN ADD COLUMN PSI_PROGRAM_CODE VARCHAR(255), PSI_CREDENTIAL_PROGRAM_DESC VARCHAR(255);")
sqlQuery(con, qry_Update_PSI_Program)
sqlQuery(con,"ALTER TABLE BGS_Matching_STP_Credential_PEN ADD COLUMN FINAL_CIP_CODE_4 VARCHAR(255), FINAL_CIP_CODE_2 VARCHAR(255), USE_BGS_CIP VARCHAR(255);")

SQLqUERY(con, qry_Make_STP_BGS_CIP4_Program_Matching)
SQLqUERY(con, qry_Make_BGS_Outcomes_CIP4_Program_Matching)
SQLqUERY(con, qry_Find_STP_BGS_Program_Matches_step1)
SQLqUERY(con, qry_Find_STP_BGS_Program_Matches_step2)
SQLqUERY(con, qry_Find_STP_BGS_Program_Matches_step3)

close(con)
