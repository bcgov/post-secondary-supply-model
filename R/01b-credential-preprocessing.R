# Workflow #2
# Credential Preprocessing 
# Description: 
# Relies on STP_Credential, STP_Enrolment_Record_Type, STP_Enrolment_Valid, STP_Enrolment data tables
# Creates tables _____ which are used in subsequent workflows

library(arrow)
library(tidyverse)
library(odbc)
library(DBI)

# ---- Configure LAN Paths and DB Connection -----
lan <- config::get("lan")
source("./sql/01-credential-preprocessing/01a-credential-preprocessing.R")
source("./sql/01-credential-preprocessing/convert_date_scripts.R")

# set connection string to decimal
db_config <- config::get("decimal")
my_schema <- config::get("myschema")

con <- dbConnect(odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")

# ---- Check Required Tables etc. ----
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Credential"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Enrolment_Record_Type"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Enrolment"')))

dbGetQuery(con, qry00a_check_null_epens)
dbGetQuery(con, qry00b_check_unique_epens)

# ---- Add primary key ----
dbExecute(con, qry00c_CreateIDinSTPCredential)
dbExecute(con, qry00d_SetPKeyinSTPCredential)

# ---- Reformat yy-mm-dd to yyyy-mm-dd ----
# check date variable format here
dbGetQuery(con, "SELECT TOP 100 CREDENTIAL_AWARD_DATE, PSI_PROGRAM_EFFECTIVE_DATE FROM STP_Credential;")

# if in format yy-mm-dd then run the following queries to convert from yy-mm-dd to yyyy-mm-dd
dbExecute(con, qrydates_create_tmp_table)
dbExecute(con, qrydates_add_cols)
dbExecute(con, qrydates_convert1)
dbExecute(con, qrydates_convert2)
dbExecute(con, qrydates_convert3)
dbExecute(con, qrydates_convert4)
dbExecute(con, qrydates_convert5)
dbExecute(con, qrydates_convert6)
dbExecute(con, qrydates_update_stp_credential1)
dbExecute(con, qrydates_update_stp_credential2)
dbExecute(con, "DROP TABLE tmp_ConvertDateFormatCredential")

# ---- Process by Record Type ----
# Record Status codes:
# 0 = Good
# 1 = Missing Student Number
# 2 = Developmental
# 3 = No PSI Transition
# 4 = Credential Only (No Enrolment Record)
# 5 = PSI_Outside_BC 
# 6 = Skills Based
# 7 = Developmental CIP
# 8 = Recommendation for Certification 


# ---- Create lookup table for ID/Record Status and populate with ID column and EPEN ----
dbExecute(con, qry01_ExtractAllID_into_STP_Credential_Record_Type)


# ---- Find records with Record_Status = 1  ----
dbExecute(con, qry02a_Record_With_PEN_Or_STUID)
dbExecute(con, qry02b_Drop_No_PEN_Or_No_STUID)
dbExecute(con, qry02c_Update_Drop_No_PEN_or_No_STUID)
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[tmp_tbl_qry02a_Cred_Record_With_PEN_or_STUID];"))
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[Drop_Cred_No_PEN_or_No_STUID];")) 

# ---- Find records with Record_Status = 2  ----
dbExecute(con, qry03a_Drop_Record_Developmental)
dbExecute(con, qry03b_Update_Drop_Record_Developmental)
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[Drop_Cred_Developmental];")) 

# ---- Find records with Record_Status = 6  ----
dbExecute(con, qry03c_create_table_EnrolmentSkillsBasedCourse)
dbExecute(con, qry03d_create_table_Suspect_Skills_Based)
dbExecute(con, qry03e_Find_Suspect_Skills_Based)
dbExecute(con, qry03f_Update_Suspect_Skills_Based)
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[tmp_tbl_Cred_Suspect_Skills_Based];")) 
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[Cred_Suspect_Skills_Based];")) 
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[tmp_tbl_EnrolmentSkillsBasedCourses];")) 

# ---- Find records with Record_Status = 7 and update look up table ----
dbExecute(con, qry03g_Drop_Developmental_Credential_CIPS)
dbExecute(con, "ALTER TABLE Drop_Developmental_PSI_CREDENTIAL_CIPS ADD Keep NVARCHAR(2)")

###  ---- ** Manual **  ----
# Check against the outcomes programs table to see if some are non-developmental CIP. If so, set keep = 'Y'.
data <- dbReadTable(con, "Drop_Developmental_PSI_CREDENTIAL_CIPS", col_types = cols(.default = col_character()))
data.entry(data)
dbWriteTable(con, name = "Drop_Developmental_PSI_CREDENTIAL_CIPS", as.data.frame(data), overwrite = TRUE)

dbExecute(con, qry03h_Update_Developmental_CIPs)
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[Drop_Developmental_PSI_CREDENTIAL_CIPS];")) 

# ---- Find records with Record_Status = 8 and update look up table ----
dbExecute(con, qry03i_Drop_RecommendationForCert)
dbExecute(con, qry03j_Update_RecommendationForCert)
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[Drop_Cred_RecommendForCert];")) 

dbExecute(con, qry04_Update_RecordStatus_Not_Dropped)
dbGetQuery(con, RecordTypeSummary)

# ---- Clean Up and check tables to keep ----
dbExistsTable(con, glue::glue("'{my_schema}.STP_Enrolment_Record_Type;'"))  
dbExistsTable(con, glue::glue("'{my_schema}.STP_Enrolment_Valid;'"))  
dbExistsTable(con, glue::glue("'{my_schema}.STP_Enrolment;'"))  
dbExistsTable(con, glue::glue("'{my_schema}.STP_Credential;'"))  
dbExistsTable(con, glue::glue("'{my_schema}.STP_Credential_Record_Type;'")) 

dbDisconnect(con)

