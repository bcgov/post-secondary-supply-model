# Workflow #1 (noting here for now)
# Enrolment Preprocessing 
# Description: 
# Relies on STP_Enrolment data table
# Creates tables STP_Enrolment_Record_Type, STP_Enrolment_Valid, STP_Enrolment used in subsequent workflows

library(arrow)
library(tidyverse)
library(odbc)
library(DBI)

# ---- Configure LAN Paths and DB Connection -----
lan <- config::get("lan")
source(glue::glue("{lan}/development/sql/gh-source/01c-enrolment-preprocessing/01c-enrolment-preprocessing-sql.R"))
source(glue::glue("{lan}/development/sql/gh-source/01c-enrolment-preprocessing/convert-date-scripts.R"))
source(glue::glue("{lan}/development/sql/gh-source/01c-enrolment-preprocessing/pssm-birthdate-cleaning.R"))

db_config <- config::get("decimal")
my_schema <- config::get("myschema")
db_schema <- config::get("dbschema")

con <- dbConnect(odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")

# ---- Check Required Tables etc. ----
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Enrolment"')))

## ---- Null values ----
dbGetQuery(con, qry00a_check_null_epens)
dbGetQuery(con, qry00b_check_unique_epens)

## ---- Primary key and nulls ----
dbExecute(con, qry00c_CreateIDinSTPEnrolment)
dbExecute(con, qry00d_SetPKeyinSTPEnrolment)


# ---- Reformat yy-mm-dd to yyyy-mm-dd ----
# check date variable format here
dbGetQuery(con, "SELECT TOP 100 PSI_BIRTHDATE, 
                  LAST_SEEN_BIRTHDATE, 
                  PSI_PROGRAM_EFFECTIVE_DATE, 
                  PSI_MIN_START_DATE 
                 FROM STP_Enrolment;")

# if in format yy-mm-dd then run the following queries to convert from yy-mm-dd to yyyy-mm-dd
dbExecute(con, qrydates_create_tmp_table)
dbExecute(con, qrydates_add_cols)
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
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[tmp_ConvertDateFormat];"))  

# ---- Create Record Type Table ----

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


# ---- Define lookup table for ID/Record Status and populate with ID column and EPEN 
dbExecute(con, qry01_ExtractAllID_into_STP_Enrolment_Record_Type)

# ----- Find records with Record_Status = 1 and update look up table -----
dbExecute(con, qry02a_Record_With_PEN_Or_STUID)
dbExecute(con, qry02b_Drop_No_PEN_Or_No_STUID)
dbExecute(con, qry02c_Update_Drop_No_PEN_Or_No_STUID)
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[tmp_tbl_qry02a_Record_With_PEN_Or_STUID];"))
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[Drop_No_PEN_or_No_STUID];"))   

# ----- Find records with Record_Status = 2 and update look up table -----
dbExecute(con, qry03a_Drop_Record_Developmental)
dbExecute(con, qry03b_Update_Drop_Record_Developmental)
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[Drop_Developmental];"))

# ----- Find records with Record_Status = 6 and update look up table -----
dbExecute(con, qry03c_Drop_Skills_Based)

## ---- Manual Work
# a manual check required of programs that are considered skills based.  Overall this list should make sense.
res <- dbGetQuery(con, "
                SELECT PSI_CODE, PSI_CE_CRS_ONLY, CIP2, PSI_PROGRAM_CODE, 
                       PSI_CREDENTIAL_PROGRAM_DESC, PSI_STUDY_LEVEL, PSI_CREDENTIAL_CATEGORY
                FROM  Drop_Skills_Based
                GROUP BY PSI_CODE, PSI_CE_CRS_ONLY, CIP2, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESC, PSI_STUDY_LEVEL, PSI_CREDENTIAL_CATEGORY;")

dbExecute(con, "ALTER TABLE Drop_Skills_Based ADD KEEP nvarchar(2) NULL;")
dbExecute(con, qry03da_Keep_TeachEd)
dbExecute(con, qry03d_Update_Drop_Record_Skills_Based)
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[Drop_Skills_Based];")) 

dbExecute(con, qry03d_1_Drop_Continuing_Ed)
dbExecute(con, qry03d_2_Update_Drop_Continuing_Ed)
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[Drop_ContinuingEd];"))

dbExecute(con, qry03d_3_Drop_More_Continuing_Ed) 
dbExecute(con, qry03d_4_Updated_Drop_ContinuingEdMore)

## Review ----
## This table is deleted before it gets used? 
dbExecute(con, qry03e_Keep_Skills_Based)
dbExecute(con, "ALTER TABLE Keep_Skills_Based ADD EXCLUDE nvarchar(2) NULL;")
dbExecute(con, qry03ea_Exclude_Skills_Based_Programs)

## ---- Manual work ----
# investigate programs that are considered skills based
keep_skills_based <- dbReadTable(con, "Keep_Skills_Based")

## This table uses IDs defined earlier in this workflow.  Manually updating is only valid in the context of a single run.
# readr::write_csv(glue::glue("{lan}/development/csv/gh-source/tmp/keep-skills-based-with-exclusions-for-review.csv"))
# keep_skills_based <- readr::read_csv(glue::glue("{lan}/development/csv/gh-source/tmp/01c-keep-skills-based-with-exclusions.csv"), 
#                              col_types = cols(.default = col_character()), 
#                              na = c("", "NA", "NULL"))
# dbWriteTable(con, name = "Keep_Skills_Based", keep_skills_based, overwrite = TRUE)

dbExecute(con, qry03f_Update_Keep_Record_Skills_Based) 
dbExecute(con, qry03fb_Update_Keep_Record_Skills_Based) 


# ---- Manual work ----
# documentation suggests investigation but discovered zero records in both past two model runs. 
dbExecute(con, qry03g_create_table_SkillsBasedCourses) 
dbExecute(con, "ALTER TABLE tmp_tbl_SkillsBasedCourses ADD KEEP nvarchar(2) NULL;")
dbExecute(con, qry03g_b_Keep_More_Skills_Based) 
dbExecute(con, qry03g_c_Update_Keep_More_Skills_Based)
dbExecute(con, qry03g_c2_Update_More_Selkirk)
dbGetQuery(con, qry03g_d_EnrolCoursesSeen)
dbExecute(con, qry03h_create_table_Suspect_Skills_Based) 
dbExecute(con, qry03i_Find_Suspect_Skills_Based) 
dbExecute(con, qry03i2_Drop_Suspect_Skills_Based)  
dbExecute(con, qry03j_Update_Suspect_Skills_Based) 

dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[Drop_ContinuingEd_More];"))
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[Keep_Skills_Based];"))
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[tmp_MoreSkillsBased_to_Keep];"))  
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[tmp_tbl_EnrolCoursesSeen];"))  
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[tmp_tbl_Suspect_Skills_Based];")) 
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[tmp_tbl_SkillsBasedCourses];"))  
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[Suspect_Skills_Based];"))                

# ---- Find records with Record_Status = 7 and update look up table ----
# NOTE: Check this query as was mixed up with same credentials query.  There are 
# three versions of qry03k??
# dbExecute(con, qry03k_Drop_Developmental_CIPS)        
dbExecute(con, "ALTER TABLE Drop_Developmental_CIPS 
          ADD ID INT NULL, 
          DO_NOT_EXCLUDE nvarchar(2) NULL;")
dbExecute(con, qry03l_Update_Developmental_CIPs)

dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[Drop_Developmental_CIPS];"))

# ---- Find records with Record_Status = 5 and update look up table ----
dbExecute(con, qry04a_Drop_No_PSI_Transition)
dbExecute(con, qry04b_Update_Drop_No_PSI_Transition)

dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[Drop_No_Transition];"))

dbExecute(con, qry06a_Drop_PSI_Outside_BC)
dbExecute(con, qry06b_Update_Drop_PSI_Outside_BC)

dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[Drop_PSI_Outside_BC];"))

# ---- Update remaining records and fix cols ----
dbExecute(con, qry07_Update_RecordStatus_No_Dropped)
dbExecute(con, qry08a_Create_Table_STP_Enrolment_Valid)
dbExecute(con, "ALTER TABLE [STP_Enrolment_Valid] 
                ADD CONSTRAINT ValidEnrolmentPK_ID
                PRIMARY KEY (ID);")

# check count of records in STP_Enrolment_Valid associated with > 1 EPEN or those missing a pen.  
cat("Records associated with > 1 EPEN")
dbGetQuery(con, "SELECT  T.PSI_CODE, T.PSI_STUDENT_NUMBER, COUNT(*)
                FROM (
	              SELECT PSI_CODE, PSI_STUDENT_NUMBER, ENCRYPTED_TRUE_PEN
	                FROM  STP_Enrolment_Valid
	                GROUP BY  PSI_CODE, PSI_STUDENT_NUMBER, ENCRYPTED_TRUE_PEN) T
                GROUP BY  T.PSI_CODE, T.PSI_STUDENT_NUMBER
                HAVING COUNT(*) <> 1")


# ---- Min Enrolment ----
# Find record with minimum enrollment sequence for each student per school year 
# by ENCRYPTED_TRUE_PEN
dbExecute(con, qry09a_MinEnrolmentPEN)
dbExecute(con, qry09b_MinEnrolmentPEN)
dbExecute(con, qry09c_MinEnrolmentPEN)
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[tmp_tbl_qry09a_MinEnrolmentPEN];"))
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[tmp_tbl_qry09b_MinEnrolmentPEN];"))

# by PSI_CODE/PSI_STUDENT_NUMBER combo for students records with null ENCRYPTED_TRUE_PEN's
dbExecute(con, qry10a_MinEnrolmentSTUID)
dbExecute(con, qry10b_MinEnrolmentSTUID)
dbExecute(con, qry10c_MinEnrolmentSTUID)
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[tmp_tbl_qry10b_MinEnrolmentSTUID];"))
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[tmp_tbl_qry10a_MinEnrolmentSTUID];"))

# Flag each record in STP_Enrolment_Record_Type as min enrollment (TRUE = 1, FALSE  = 0)
dbExecute(con, qry11a_Update_MinEnrolmentPEN)
dbExecute(con, qry11b_Update_MinEnrolmentSTUID)
dbExecute(con, qry11c_Update_MinEnrolment_NA)

dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[MinEnrolment_ID_PEN];"))
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[MinEnrolment_ID_STUID];"))

# ---- First Enrollment Date ---- 
# Find earliest enrollment record for each student per school year
# by ENCRYPTED_TRUE_PEN
dbExecute(con, qry12a_FirstEnrolmentPEN)
dbExecute(con, qry12b_FirstEnrolmentPEN)
dbExecute(con, qry12c_FirstEnrolmentPEN)
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[tmp_tbl_qry12a_FirstEnrolmentPEN];"))
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[tmp_tbl_qry12b_FirstEnrolmentPEN];"))

# by PSI_CODE/PSI_STUDENT_NUMBER combo for students records with null ENCRYPTED_TRUE_PEN's
dbExecute(con, qry13a_FirstEnrolmentSTUID)
dbExecute(con, qry13b_FirstEnrolmentSTUID)
dbExecute(con, qry13c_FirstEnrolmentSTUID)
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[tmp_tbl_qry13a_FirstEnrolment_STUID];"))
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[tmp_tbl_qry13b_FirstEnrolment_STUID];"))

# Flag each record in STP_Enrolment_Record_Type as first enrollment (TRUE = 1, FALSE  = 0)
dbExecute(con, qry14a_Update_FirstEnrolmentPEN)
dbExecute(con, qry14b_Update_FirstEnrolmentSTUID)
dbExecute(con, qry14c_Update_FirstEnrolmentNA)

dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[FirstEnrolment_ID_PEN];"))
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[FirstEnrolment_ID_STUID];"))

# ---- Clean Birthdates ----
## Documentation: development\documentation\01-pssm-2020-2021-notes-on-enrolment-and-graduate-projections\birthdates-and-gender.docx
# Creates some temp tables to flag if each record represents the min or max birthdate
dbExecute(con, qry01_BirthdateCleaning) 
dbExecute(con, qry02_BirthdateCleaning)
dbExecute(con, qry03_BirthdateCleaning)
dbExecute(con, qry04_BirthdateCleaning)
dbExecute(con, "ALTER table tmp_MaxPSIBirthdate ADD NumBirthdateRecords INT NULL")
dbExecute(con, "ALTER table tmp_MinPSIBirthdate ADD NumBirthdateRecords INT NULL")
dbExecute(con, qry05_BirthdateCleaning)
dbExecute(con, qry06_BirthdateCleaning)
dbExecute(con, "ALTER table tmp_MoreThanOne_Birthdate 
                ADD MinPSIBirthdate NVARCHAR(50) NULL,
                    NumMinBirthdateRecords INT NULL,
                    MaxPSIBirthdate NVARCHAR(50) NULL,
                    NumMaxBirthdateRecords INT NULL,
                    LastSeenBirthdate NVARCHAR(50) NULL,
                    MaxOrMin_MostCommon NVARCHAR(50) NULL,
                    UseMaxOrMin_FINAL NVARCHAR(50) NULL,
                    psi_birthdate_cleaned NVARCHAR(50) NULL")
dbExecute(con, qry07a_BirthdateCleaning)
dbExecute(con, qry07b_BirthdateCleaning)

# pull in LAST_SEEN_BIRTHDATE from STP_ENROLMENT
dbExecute(con, qry08_BirthdateCleaning)

# ----- Manual Cleaning -----
## TO DO: code this manual step. See documentation for methodology
#  tmp_MoreThanOne_Birthdate is exported for manual review, then imported back as tmp_Clean_MaxMinBirthDate
orig.data <- dbGetQuery(con, "SELECT * FROM tmp_MoreThanOne_Birthdate")
data <- readr::read_csv(glue::glue("{lan}/development/csv/gh-source/testing/tmp_Clean_MaxMinBirthDate.csv"), 
                        col_types = cols(.default = col_character()))
dbWriteTable(con, "tmp_Clean_MaxMinBirthDate", data)
dbExecute(con, qry09_BirthdateCleaning)
dbExecute(con, qry10_BirthdateCleaning)
dbExecute(con, qry11_BirthdateCleaning)
dbExecute(con, "DROP TABLE tmp_Clean_MaxMinBirthDate")

dbExecute(con, "ALTER TABLE STP_Enrolment ADD psi_birthdate_cleaned NVARCHAR(50) NULL")
# update psi_birthdate_cleaned in STP_Enrolment with birthdate to use
dbExecute(con, qry12_BirthdateCleaning)
dbExecute(con, "DROP TABLE tmp_MinPSIBirthdate")
dbExecute(con, "DROP TABLE tmp_MaxPSIBirthdate")

# some records have a null PSI_BIRTHDATE, search for non-null PSI_BIRTHDATE for these EPENS
dbExecute(con, qry13_BirthdateCleaning)
dbExecute(con, qry14_BirthdateCleaning)
dbExecute(con, qry15_BirthdateCleaning)
dbExecute(con, "ALTER TABLE tmp_NullBirthdateCleaned ADD psi_birthdate_cleaned NVARCHAR(50) NULL")
dbExecute(con, qry16_BirthdateCleaning)
dbExecute(con, qry17_BirthdateCleaning)

# update STP_Enrolment with cleaned birthdates + those that didn't need cleaning. 
# psi_birthdate_cleaned will contain the correct birthdate
dbExecute(con, qry18_BirthdateCleaning)
dbExecute(con, qry19_BirthdateCleaning)

# sanity check on psi_birthdate_cleaned - finish this and save report
dbExecute(con, qry20_BirthdateCleaning)
dbGetQuery(con, qry21_BirthdateCleaning)

dbExecute(con, "DROP TABLE tmp_BirthDate")
dbExecute(con, "DROP TABLE tmp_MoreThanOne_Birthdate")
dbExecute(con, "DROP TABLE tmp_NullBirthdate")
dbExecute(con, "DROP TABLE tmp_NonNullBirthdate")
dbExecute(con, "DROP TABLE tmp_NullBirthdateCleaned")
dbExecute(con, "DROP TABLE tmp_TEST_multi_birthdate")


# ---- Clean Up and check tables to keep ----
dbExists(con, glue::glue("DROP TABLE [{my_schema}].[STP_Enrolment_Record_Type];"))  
dbExists(con, glue::glue("DROP TABLE [{my_schema}].[STP_Enrolment_Valid];"))  
dbExists(con, glue::glue("DROP TABLE [{my_schema}].[STP_Enrolment];"))  
dbDisconnect(con)






