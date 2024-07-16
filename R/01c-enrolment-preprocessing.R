library(arrow)
library(tidyverse)
library(odbc)
library(DBI)

# ---- Configure LAN Paths and DB Connection -----
lan <- config::get("lan")
lan_2019 <- config::get("lan_2019")
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
dbExecute(con, qry03e_Keep_Skills_Based)
dbExecute(con, "ALTER TABLE Keep_Skills_Based ADD EXCLUDE nvarchar(2) NULL;")
dbExecute(con, qry03ea_Exclude_Skills_Based_Programs)

## ---- Manual work ----
# investigate programs that are considered skills based - requires a think as to how to include in a pipeline
# keep_skills_based <- dbReadTable(con, "Keep_Skills_Based")
# readr::write_csv(glue::glue("{lan}/development/csv/gh-source/01c-keep-skills-based-with-exclusions.csv"))
keep_skills_based <- readr::read_csv(glue::glue("{lan}/development/csv/gh-source/01c-keep-skills-based-with-exclusions.csv"), 
                              col_types = cols(.default = col_character()), 
                              na = c("", "NA", "NULL"))

dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[Keep_Skills_Based];"))
dbWriteTable(con, name = "Keep_Skills_Based", keep_skills_based, overwrite = TRUE)

dbExecute(con, qry03f_Update_Keep_Record_Skills_Based) # counts differ slightly from documentation (2019) see documentation for rationale
dbExecute(con, qry03fb_Update_Keep_Record_Skills_Based) # counts differ slightly from documentation (2019) see documentation for rationale

dbExecute(con, qry03g_create_table_SkillsBasedCourses) # counts differ a bit from documentation (2019)
dbExecute(con, "ALTER TABLE tmp_tbl_SkillsBasedCourses ADD KEEP nvarchar(2) NULL;")

# ---- Manual work ----
# This section is questionable as it finds no records. Noted in 2019 documentation
dbExecute(con, qry03g_b_Keep_More_Skills_Based) # documentation suggests investigation but discovered zero records in both past two model runs. 
dbExecute(con, qry03g_c_Update_Keep_More_Skills_Based) # documentation suggests investigation but discovered zero records in both past two model runs. 
dbExecute(con, qry03g_c2_Update_More_Selkirk)
dbExecute(con, qry03g_d_EnrolCoursesSeen) # The data in this table doesn't appear to be used for anything.

# ---- Error check needed here ----
# counts differ significantly from documentation (2019) but I think we are fine here as we should be filtering out these records. 
# The set difference is the result set from  qry03c_Drop_Skills_Based.
# The update query may have been run later, or the courses may be filtered later.
dbExecute(con, qry03h_create_table_Suspect_Skills_Based) 
dbExecute(con, qry03i_Find_Suspect_Skills_Based) # counts differ significantly from documentation (2019)
dbExecute(con, qry03i2_Drop_Suspect_Skills_Based) # affects 0 rows??  
dbExecute(con, qry03j_Update_Suspect_Skills_Based) # # counts differ from documentation (2019)

dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[Drop_ContinuingEd_More];"))
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[Keep_Skills_Based];"))
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[tmp_MoreSkillsBased_to_Keep];"))  
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[tmp_tbl_EnrolCoursesSeen];"))  
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[tmp_tbl_Suspect_Skills_Based];")) 
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[tmp_tbl_SkillsBasedCourses];"))  
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[Suspect_Skills_Based];"))                

# ---- Find records with Record_Status = 7 and update look up table ----
## ---- Manual Work ----
dbExecute(con, qry03k_Drop_Developmental_Credential_CIPS)        
dbExecute(con, "ALTER TABLE Drop_Developmental_CIPS 
          ADD ID INT NULL, 
          DO_NOT_EXCLUDE nvarchar(2) NULL;")
dbExecute(con, qry03k_Update_ID_for_Drop_Dev_Credential_CIP)
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

# Manual Work
# check for records in STP_Enrolment_Valid associated with > 1 EPEN or those missing a pen and fix.
dbExecute(con, "SELECT  T.PSI_CODE, T.PSI_STUDENT_NUMBER, COUNT(*)
                FROM (
	              SELECT PSI_CODE, PSI_STUDENT_NUMBER, ENCRYPTED_TRUE_PEN
	                FROM  STP_Enrolment_Valid
	                GROUP BY  PSI_CODE, PSI_STUDENT_NUMBER, ENCRYPTED_TRUE_PEN) T
                GROUP BY  T.PSI_CODE, T.PSI_STUDENT_NUMBER
                HAVING COUNT(*) <> 1")

# ---- Min Enrolment ----
# Find first enrollment record for each student per school year (by ENCRYPTED_TRUE_PEN)
dbExecute(con, qry09a_MinEnrolmentPEN)
dbExecute(con, qry09b_MinEnrolmentPEN)
dbExecute(con, qry09c_MinEnrolmentPEN)
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[tmp_tbl_qry09a_MinEnrolmentPEN];"))
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[tmp_tbl_qry09b_MinEnrolmentPEN];"))

# Find first enrollment record for each student per school year,(PSI_CODE/PSI_STUDENT_NUMBER combo for students records with null ENCRYPTED_TRUE_PEN's)
dbExecute(con, qry10a_MinEnrolmentSTUID)
dbExecute(con, qry10b_MinEnrolmentSTUID)
dbExecute(con, qry10c_MinEnrolmentSTUID)
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[tmp_tbl_qry10b_MinEnrolmentSTUID];"))
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[tmp_tbl_qry10a_MinEnrolmentSTUID];"))

dbExecute(con, qry11a_Update_MinEnrolmentPEN)
dbExecute(con, qry11b_Update_MinEnrolmentSTUID)
dbExecute(con, qry11c_Update_MinEnrolment_NA)
#dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[qry11a_Update_MinEnrolmentPEN];"))
#dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[qry11b_Update_MinEnrolmentPEN];"))
#dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[qry11c_Update_MinEnrolmentPEN];"))

dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[MinEnrolment_ID_PEN];"))
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[MinEnrolment_ID_STUID];"))

# ---- First Enrollment Date ---- 
dbExecute(con, qry12a_FirstEnrolmentPEN)
dbExecute(con, qry12b_FirstEnrolmentPEN)
dbExecute(con, qry12c_FirstEnrolmentPEN)
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[tmp_tbl_qry12a_FirstEnrolmentPEN];"))
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[tmp_tbl_qry12b_FirstEnrolmentPEN];"))

dbExecute(con, qry13a_FirstEnrolmentSTUID)
dbExecute(con, qry13b_FirstEnrolmentSTUID)
dbExecute(con, qry13c_FirstEnrolmentSTUID)
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[tmp_tbl_qry13a_FirstEnrolment_STUID];"))
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[tmp_tbl_qry13b_FirstEnrolment_STUID];"))

dbExecute(con, qry14a_Update_FirstEnrolmentPEN)
dbExecute(con, qry14b_Update_FirstEnrolmentSTUID)

dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[FirstEnrolment_ID_PEN];"))
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[FirstEnrolment_ID_STUID];"))

# ---- Clean Birthdates ----
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
dbExecute(con, qry08_BirthdateCleaning)

# ----- Manual Cleaning -----
data <- readr::read_csv(glue::glue("{lan_2019}/Development/SQL Server/Preprocessing/tmp_Clean_MaxMinBirthDate.csv"), col_types = cols(.default = col_character()))
dbWriteTable(con, "tmp_Clean_MaxMinBirthDate", data)
dbExecute(con, qry09_BirthdateCleaning)
dbExecute(con, qry10_BirthdateCleaning)
dbExecute(con, qry11_BirthdateCleaning)
#dbExecute(con, "ALTER TABLE STP_Enrolment ADD psi_birthdate_cleaned NVARCHAR(50) NULL")
dbExecute(con, qry12_BirthdateCleaning)
dbExecute(con, "DROP TABLE tmp_MinPSIBirthdate")
dbExecute(con, "DROP TABLE tmp_MaxPSIBirthdate")
dbExecute(con, qry13_BirthdateCleaning)
dbExecute(con, qry14_BirthdateCleaning)
dbExecute(con, qry15_BirthdateCleaning)
dbExecute(con, "ALTER TABLE tmp_NullBirthdateCleaned ADD psi_birthdate_cleaned NVARCHAR(50) NULL")
dbExecute(con, qry16_BirthdateCleaning)
dbExecute(con, qry17_BirthdateCleaning)
dbExecute(con, qry18_BirthdateCleaning)
dbExecute(con, qry19_BirthdateCleaning)
dbExecute(con, qry20_BirthdateCleaning)
dbGetQuery(con, qry21_BirthdateCleaning)
dbExecute(con, "DROP TABLE tmp_BirthDate")
dbExecute(con, "DROP TABLE tmp_MoreThanOne_Birthdate")
dbExecute(con, "DROP TABLE tmp_Clean_MaxMinBirthDate")
dbExecute(con, "DROP TABLE tmp_NullBirthdate")
dbExecute(con, "DROP TABLE tmp_NonNullBirthdate")
dbExecute(con, "DROP TABLE tmp_NullBirthdateCleaned")
dbExecute(con, "DROP TABLE tmp_TEST_multi_birthdate")
# TO DO: a large number of null psi_birthdates at this point.

# ---- Clean Up ----
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[STP_Enrolment_Record_Type];"))  
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[STP_Enrolment_Valid];"))   
dbDisconnect(con)






