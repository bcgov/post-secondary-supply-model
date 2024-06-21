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

# ----- Find records with Record_Status = 0 and update look up table -----
dbExecute(con, qry02a_Record_With_PEN_Or_STUID)

# ----- Find records with Record_Status = 1 and update look up table -----
dbExecute(con, qry02b_Drop_No_PEN_Or_No_STUID)
dbExecute(con, qry02c_Update_Drop_No_PEN_Or_No_STUID)

# ----- Find records with Record_Status = 2 and update look up table -----
dbExecute(con, qry03a_Drop_Record_Developmental)
dbExecute(con, qry03b_Update_Drop_Record_Developmental)

# ----- Find records with Record_Status = 6 and update look up table -----
dbExecute(con, qry03c_Drop_Skills_Based)

# a manual check required of programs that are considered skills based.  Overall this list should make sense.
res <- dbGetQuery(con, "
                SELECT PSI_CODE, PSI_CE_CRS_ONLY, CIP2, PSI_PROGRAM_CODE, 
                       PSI_CREDENTIAL_PROGRAM_DESC, PSI_STUDY_LEVEL, PSI_CREDENTIAL_CATEGORY
                FROM  Drop_Skills_Based
                GROUP BY PSI_CODE, PSI_CE_CRS_ONLY, CIP2, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESC, PSI_STUDY_LEVEL, PSI_CREDENTIAL_CATEGORY;")

dbExecute(con, "ALTER TABLE Drop_Skills_Based ADD KEEP nvarchar(2) NULL;")
dbExecute(con, qry03d_Update_Drop_Record_Skills_Based)
dbExecute(con, qry03da_Keep_TeachEd)
dbExecute(con, qry03d_1_Drop_Continuing_Ed)
dbExecute(con, qry03d_2_Update_Drop_Continuing_Ed)
dbExecute(con, qry03d_3_Drop_More_Continuing_Ed) # <-- check this as notes from last run show much greater number of affected rows (10x)
dbExecute(con, qry03d_4_Updated_Drop_ContinuingEdMore)
dbExecute(con, qry03e_Keep_Skills_Based)
dbExecute(con, "ALTER TABLE Keep_Skills_Based ADD EXCLUDE nvarchar(2) NULL;")
dbExecute(con, qry03ea_Exclude_Skills_Based_Programs)
dbExecute(con, qry03f_Update_Keep_Record_Skills_Based)
dbExecute(con, qry03fb_Update_Keep_Record_Skills_Based)
dbExecute(con, qry03g_create_table_SkillsBasedCourses)
dbExecute(con, "ALTER TABLE tmp_tbl_SkillsBasedCourses ADD KEEP nvarchar(2) NULL;")
dbExecute(con, qry03g_b_Keep_More_Skills_Based) # <-- documentation suggests investigation but discovered zero records in both past two model runs. 
dbExecute(con, qry03g_c_Update_Keep_More_Skills_Based) # <-- documentation suggests investigation but discovered zero records in both past two model runs. 
dbExecute(con, qry03g_c2_Update_More_Selkirk)
dbExecute(con, qry03g_d_EnrolCoursesSeen)
dbExecute(con, qry03h_create_table_Suspect_Skills_Based) # <-- returns fewer records than documented (for 2019).  
dbExecute(con, qry03i_Find_Suspect_Skills_Based) # <-- returns fewer records than documented (for 2019).  
dbExecute(con, qry03i2_Drop_Suspect_Skills_Based) # <-- returns fewer records than documented (for 2019).  
dbExecute(con, qry03j_Update_Suspect_Skills_Based) # <-- returns fewer records than documented (for 2019).  










