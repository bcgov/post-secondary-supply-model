library(arrow)
library(tidyverse)
library(odbc)
library(DBI)

# ---- Configure LAN Paths and DB Connection -----
lan <- config::get("lan")
source(glue::glue("{lan}/development/sql/gh-source/01a-credential-preprocessing/01a-credential-preprocessing.R"))
source(glue::glue("{lan}/development/sql/gh-source/01a-credential-preprocessing/convert_date_scripts.R"))

# set connection string to decimal
db_config <- config::get("decimal")
db_schema <- config::get("dbschema")
my_schema <- config::get("myschema")

con <- dbConnect(odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")

# ---- Check Required Tables etc. ----
dbExistsTable(con, SQL(glue::glue('"{db_schema}"."STP_CREDENTIAL"')))
dbExistsTable(con, SQL(glue::glue('"{db_schema}"."STP_Enrolment_Record_Type"')))
dbExistsTable(con, SQL(glue::glue('"{db_schema}"."STP_Enrolment"')))
dbGetQuery(con, qry00a_check_null_epens)
dbGetQuery(con, qry00b_check_unique_epens)

# ---- Add primary key ----
dbExecute(con, qry00c_CreateIDinSTPCredential)
dbExecute(con, qry00d_SetPKeyinSTPCredential)

# ---- Reformat yy-mm-dd to yyyy-mm-dd
# Create temporary table
dbExecute(con, qrydates_create_tmp_table)
dbExecute(con, qrydates_add_cols)

# Add first two digits to dates in the convert variables
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

# ---- Find records with Record_Status = 1 and update look up table ----
dbExecute(con, qry02a_Record_With_PEN_Or_STUID)
dbExecute(con, qry02b_Drop_No_PEN_Or_No_STUID)
dbExecute(con, qry02c_Update_Drop_No_PEN_or_No_STUID)
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[tmp_tbl_qry02a_Cred_Record_With_PEN_or_STUID];"))
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[Drop_Cred_No_PEN_or_No_STUID];")) 

# Find records with Record_Status = 2 and update look up table
dbExecute(con, qry03a_Drop_Record_Developmental)
dbExecute(con, qry03b_Update_Drop_Record_Developmental)
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[Drop_Cred_Developmental];")) 

# Create a subset of potential Record_Status = 6 records that have not already assigned a record status
dbExecute(con, qry03c_create_table_EnrolmentSkillsBasedCourse)
dbExecute(con, qry03c_Drop_Skills_Based)
dbExecute(con, qry03d_Update_Drop_Record_Skills_Based)
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[Drop_Skills_Based];"))

dbExecute(con, qry03d_create_table_Suspect_Skills_Based)
dbExecute(con, qry03e_Find_Suspect_Skills_Based)
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[tmp_tbl_Cred_Suspect_Skills_Based];")) 
dbExecute(con, qry03e_Keep_Skills_Based)
dbExecute(con, qry03f_Update_Keep_Record_Skills_Based)
dbExecute(con, qry03f_Update_Suspect_Skills_Based)
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[Keep_Skills_Based];"))
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[Cred_Suspect_Skills_Based];")) 
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[tmp_tbl_EnrolmentSkillsBasedCourses];")) 
 
# ---- Find records with Record_Status = 7 and update look up table ----
dbExecute(con, qry03g_Drop_Developmental_Credential_CIPS)
dbExecute(con, "ALTER TABLE Drop_Developmental_PSI_CREDENTIAL_CIPS ADD KEEP NVARCHAR(2)")
## ---- Manual Work ----
data <- dbReadTable(con, "Drop_Developmental_PSI_CREDENTIAL_CIPS", col_types = cols(.default = col_character()))
data <- data %>% mutate(KEEP = NA) # or use excel, utils::data.entry or whatever
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[Drop_Developmental_PSI_CREDENTIAL_CIPS];")) 
dbWriteTable(con, name = "Drop_Developmental_PSI_CREDENTIAL_CIPS", data)

## ---- Back to Workflow ----
dbExecute(con, qry03h_Update_Developmental_CIPs)
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[Drop_Developmental_PSI_CREDENTIAL_CIPS];")) 

# ---- Find records with Record_Status = 8 and update look up table ----
dbExecute(con, qry03i_Drop_RecommendationForCert)
dbExecute(con, qry03j_Update_RecommendationForCert)
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[Drop_Cred_RecommendForCert];")) 

# ---- Investigate This Query ----
# dbExecute(con, qry03k_Drop_Developmental_CIPS) # are these run?
# dbExecute(con, qry03k_Update_ID_for_Drop_Dev_Credential_CIP) # are these run?
# dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[];")) 

dbExecute(con, qry04_Update_RecordStatus_Not_Dropped)
