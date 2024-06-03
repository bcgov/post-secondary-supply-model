# ---- Required INFOWARE TABLES
# INFOWARE_PROGRAMS_HIST_PRGMID_XREF
# INFOWARE_C_OUTC_CLEAN_SHORT_RESP
# INFOWARE_L_CIP_2DIGITS_CIP2016
# INFOWARE_L_CIP_4DIGITS_CIP2016
# INFOWARE_L_CIP_6DIGITS_CIP2016
# INFOWARE_PROGRAMS
# DACSO_STP_ProgramsCIP4_XWALK_ALL_2018

library(tidyverse)
library(RODBC)
library(config)

source("./sql/dacso_program_matching.R")

#---- Connect to Outcomes Database ----
connection <- config::get("connection")$outcomes_dacso
con <- odbcDriverConnect(connection)

sqlTables(con,  tableType = c("TABLE")) # tableType = "SYNONYM" gives tables linked to ORACLE

# ---- Update XWALK ----
sqlQuery(con, "SELECT * INTO DACSO_STP_ProgramsCIP4_XWALK_ALL_2020 FROM DACSO_STP_ProgramsCIP4_XWALK_ALL_2018;")
sqlQuery(con, "ALTER TABLE DACSO_STP_ProgramsCIP4_XWALK_ALL_2020 ADD COLUMN New_DACSO_2020 VARCHAR(255), New_STP_2020 VARCHAR(255), Updated_DACSO_CPC_2020 VARCHAR(255), Updated_DACSO_CIP_2020 VARCHAR(255), Updated_DACSO_CDTL_2020 VARCHAR(255);")

# ---- New DACSO programs with no historical linkages ----
# Note: new since the last PSSM run

sqlQuery(con, qry_DACSO_new_programs)
sqlQuery(con, qry_add_new_DACSO_program_to_XWALK)

# ---- New DACSO programs with historical program linkages ----
# Note: (Updated CPCs) and were previously mapped to a STP program

sqlQuery(con, qry_DACSO_updated_programs)
sqlQuery(con, qry_DACSO_updated_programs_links_a)

# only run next line if SO team has not yet added the latest year’s historical linkages to the Infoware Historical Program ID XWALK table. (*reminder to update year)
sqlQuery(con, qry_DACSO_updated_programs_links_a_step2)

# ... and continue
sqlQuery(con, "ALTER TABLE updated_dacso_programs_in_2020_with_links ADD COLUMN HISTORICAL_CPC_CD VARCHAR(255), HISTORICAL_PROGRAM_NAME VARCHAR(255), HISTORICAL_CIP4_CD VARCHAR(255)")
sqlQuery(con, qry_DACSO_updated_program_links_b)
sqlQuery(con, qry_DACSO_updated_programs_linked_STP_a)
sqlQuery(con, qry_DACSO_updated_programs_linked_STP_b)
sqlQuery(con, "ALTER TABLE Updated_DACSO_Programs_in_2020_with_links ADD COLUMN PSI_CODE VARCHAR(255), OUTCOMES_CRED VARCHAR(255), PSI_PROGRAM_CODE VARCHAR(255), PSI_CREDENTIAL_PROGRAM_DESC VARCHAR(255), STP_CIP4_CODE VARCHAR(255), STP_CIP4_NAME VARCHAR(255), One_To_One_Match VARCHAR(255), One_to_Many_Match VARCHAR(255), Probable_Match VARCHAR(255), Manual_Match VARCHAR(255), Extra_Flag VARCHAR(255);")
sqlQuery(con, qry_DACSO_updated_programs_linked_STP_c)
sqlQuery(con, "ALTER TABLE Updated_DACSO_Programs_in_2020_with_links ADD COLUMN Updated_CIP_Flag VARCHAR(255), Updated_CPC_Flag VARCHAR(255);")
sqlQuery(con, qry_DACSO_updated_programs_linked_STP_d)
sqlQuery(con, qry_DACSO_updated_programs_linked_STP_e)

# update STP xwalk with updated CPC info
sqlQuery(con, qry_Update_DACSO_STP_XWALK_Updated_CPCS)

# ---- check remaining DACSO updated CPCS for match to STP program
sqlQuery(con, qry_make_Remaining_DACSO_Updated_CPCS)
sqlQuery(con, qry_append_updated_cpcs_not_previously_matched)

# ---- DACSO programs with updated CIPS ----
# Open up the Infoware_Programs table to see which CPCs had an updated CIP in the latest year 
# Check the NOTES column to find out because this is where Sheila makes a note if the CIP was updated for a program.
# Manual Method: search on the CPC nad update the CIPs manually in the XWALK table and put a “Yes” in the Updated_DACSO_CIP2015 column in the XWALK table
# Alternativly something like: 
sqlQuery(con, qry_Check_InfowarePrograms_Updated_CIPS_a)


sqlQuery(con, "SELECT * FROM infoware_programs_updatecips_2020")

close(con)
