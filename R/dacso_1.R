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

source("./sql/stp-dacso_program_matching_2019_20.R")

#---- Connect to Outcomes Database
connection <- config::get("connection")$outcomes_dacso
con <- odbcDriverConnect(connection)

sqlTables(con,  tableType = c("TABLE")) # tableType = "SYNONYM" gives tables linked to ORACLE

# Update last runs XWALK
sqlQuery(con, "SELECT * INTO DACSO_STP_ProgramsCIP4_XWALK_ALL_2020 FROM DACSO_STP_ProgramsCIP4_XWALK_ALL_2018;")
sqlQuery(con, "ALTER TABLE DACSO_STP_ProgramsCIP4_XWALK_ALL_2020 ADD COLUMN New_DACSO_2020 VARCHAR(50), New_STP_2020 VARCHAR(50), Updated_DACSO_CPC_2020 VARCHAR(50), Updated_DACSO_CIP_2020 VARCHAR(50), Updated_DACSO_CDTL_2020 VARCHAR(50);")

# new DACSO programs with no historical linkages (new since the last PSSM run)
sqlQuery(con, qry_DACSO_new_programs)
sqlQuery(con, qry_add_new_DACSO_program_to_XWALK)

# New DACSO programs with historical program linkages (updated CPCs) and were previously mapped to a STP program
sqlQuery(con, qry_DACSO_updated_programs)

close(con)
