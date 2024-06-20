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

# Create lookup table for ID/Record Status and populate with ID column and EPEN 
