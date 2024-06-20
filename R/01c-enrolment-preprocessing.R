library(arrow)
library(tidyverse)
library(odbc)
library(DBI)

# ---- Configure LAN Paths and DB Connection -----
lan <- config::get("lan")
# invisible(lapply(list.files(glue::glue("{lan}/development/sql/gh-source/01c-enrolment-preprocessing"), full.names = TRUE), source))
source(glue::glue("{lan}/development/sql/gh-source/01c-enrolment-preprocessing/01c-enrolment-preprocessing-sql.R"))
source(glue::glue("{lan}/development/sql/gh-source/01c-enrolment-preprocessing/convert-date-scripts.R"))

# set connection string to decimal
db_config <- config::get("decimal")
my_schema <- config::get("myschema")
db_schema <- config::get("dbschema")

con <- dbConnect(odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")

# ---- A few checks ----
# required tables
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Enrolment"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."AgeGroupLookup"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."CredentialGrouping"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."CredentialRank"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."OutcomeCredential"')))

# null values
dbGetQuery(con, qry00a_check_null_epens)
dbGetQuery(con, qry00b_check_unique_epens)

# ---- Add primary key ----
dbExecute(con, qry00c_CreateIDinSTPEnrolment)
dbExecute(con, qry00d_SetPKeyinSTPEnrolment)

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
dbExecute(con, qrydates_convert7)
dbExecute(con, qrydates_convert8)
dbExecute(con, qrydates_convert9)
dbExecute(con, qrydates_convert10)
dbExecute(con, qrydates_convert11)
dbExecute(con, qrydates_convert12)

dbExecute(con, qrydates_update1)
dbExecute(con, qrydates_update2)
dbExecute(con, qrydates_update3)
dbExecute(con, qrydates_update4)
dbExecute(con, "DROP TABLE tmp_ConvertDateFormat")

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

# Create lookup table for ID/Record Status and populate with ID column and EPEN 
dbExecute(con, qry01_ExtractAllID_into_STP_Enrolment_Record_Type)

# Find records with Record_Status = 0 and update look up table
dbExecute(con, qry02a_Record_With_PEN_Or_STUID)

# Find records with Record_Status = 1 and update look up table
dbExecute(con, qry02b_Drop_No_PEN_Or_No_STUID)
dbExecute(con, qry02c_Update_Drop_No_PEN_or_No_STUID)
