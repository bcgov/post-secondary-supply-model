# Copyright 2024 Province of British Columbia
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations under the License.

# STP Enrolment Preprocessing: Workflow #1
# Description: 
# Relies on: STP_Enrolment data table
# Creates tables: STP_Enrolment_Record_Type, STP_Enrolment_Valid, STP_Enrolment

library(arrow)
library(tidyverse)
library(odbc)
library(DBI)

# ---- Configure LAN Paths and DB Connection -----
lan <- config::get("lan")
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

# [SQL]
# ---- Checks ----
qry00a_check_null_epens <- "
SELECT COUNT (*) AS n_null_epens FROM STP_Enrolment
WHERE STP_Enrolment.ENCRYPTED_TRUE_PEN IN ('', ' ', '(Unspecified)')
OR STP_Enrolment.ENCRYPTED_TRUE_PEN IS NULL;"
dbGetQuery(con, qry00a_check_null_epens)

# [SQL]

qry00b_check_unique_epens <- "
  SELECT COUNT (DISTINCT ENCRYPTED_TRUE_PEN) AS n_epens
  FROM STP_Enrolment"
dbGetQuery(con, qry00b_check_unique_epens)

## ---- Primary key and nulls ----

# [ALTER TABLE] STP_Enrolment

# ---- create id field as primary key ----
qry00c_CreateIDinSTPEnrolment <- "
  ALTER TABLE STP_Enrolment
  ADD ID INT IDENTITY(1,1) NOT NULL"
dbExecute(con, qry00c_CreateIDinSTPEnrolment)

# [ALTER TABLE] STP_Enrolment

qry00d_SetPKeyinSTPEnrolment <- "
  ALTER TABLE STP_Enrolment
  ADD CONSTRAINT STP_Enrolment_PK_ID PRIMARY KEY (ID)"
dbExecute(con, qry00d_SetPKeyinSTPEnrolment)


# ---- Reformat yy-mm-dd to yyyy-mm-dd ----
# check date variable format here
dbGetQuery(con, "SELECT TOP 100 PSI_BIRTHDATE, 
                  LAST_SEEN_BIRTHDATE, 
                  PSI_PROGRAM_EFFECTIVE_DATE, 
                  PSI_MIN_START_DATE 
                 FROM STP_Enrolment;")

# if in format yy-mm-dd then run the following queries to convert from yy-mm-dd to yyyy-mm-dd

# [SELECT INTO] Create tmp_ConvertDateFormat from STP_Enrolment
# ---- qrydates_create_tmp_table ----
qrydates_create_tmp_table <- "
SELECT ID,
       PSI_BIRTHDATE,
       LAST_SEEN_BIRTHDATE,
       PSI_MIN_START_DATE,
       PSI_PROGRAM_EFFECTIVE_DATE
  INTO tmp_ConvertDateFormat
  FROM STP_Enrolment;"
dbExecute(con, qrydates_create_tmp_table)

# [ALTER TABLE] tmp_ConvertDateFormat

# ---- qrydates_add_cols ----
qrydates_add_cols <- "
ALTER TABLE tmp_ConvertDateFormat
  ADD PSI_BIRTHDATE_convert varchar(50),
      LAST_SEEN_BIRTHDATE_convert varchar(50),
      PSI_MIN_START_DATE_convert varchar(50),
      PSI_PROGRAM_EFFECTIVE_DATE_convert varchar(50);"
dbExecute(con, qrydates_add_cols)

# [UPDATE] Tmp_ConvertDateFormat

# ---- qrydates_convert1 ----
qrydates_convert1 <- "
UPDATE Tmp_ConvertDateFormat
SET Tmp_ConvertDateFormat.PSI_BIRTHDATE_CONVERT = '20'+PSI_BIRTHDATE
WHERE ((Left(PSI_BIRTHDATE,2)<24));"
dbExecute(con, qrydates_convert1)

# [UPDATE] Tmp_ConvertDateFormat

# ---- qrydates_convert2 ----
qrydates_convert2 <- "
UPDATE Tmp_ConvertDateFormat
SET Tmp_ConvertDateFormat.PSI_BIRTHDATE_CONVERT = '19'+PSI_BIRTHDATE
WHERE ((Left(PSI_BIRTHDATE,2)>23));"
dbExecute(con, qrydates_convert2)

# [UPDATE] Tmp_ConvertDateFormat

# ---- qrydates_convert3 ----
qrydates_convert3 <- "
UPDATE Tmp_ConvertDateFormat
SET Tmp_ConvertDateFormat.PSI_BIRTHDATE_CONVERT = ''
WHERE ((Left(PSI_BIRTHDATE,2)='  '));"
dbExecute(con, qrydates_convert3)

# [UPDATE] Tmp_ConvertDateFormat

# ---- qrydates_convert4 ----
qrydates_convert4 <- "
UPDATE Tmp_ConvertDateFormat
SET Tmp_ConvertDateFormat.LAST_SEEN_BIRTHDATE_CONVERT = '20'+LAST_SEEN_BIRTHDATE
WHERE ((Left(LAST_SEEN_BIRTHDATE,2)<24));"
dbExecute(con, qrydates_convert4)

# [UPDATE] Tmp_ConvertDateFormat

# ---- qrydates_convert5 ----
qrydates_convert5 <- "
UPDATE Tmp_ConvertDateFormat
SET Tmp_ConvertDateFormat.LAST_SEEN_BIRTHDATE_CONVERT = '19'+LAST_SEEN_BIRTHDATE
WHERE ((Left(LAST_SEEN_BIRTHDATE,2)>23));"
dbExecute(con, qrydates_convert5)

# [UPDATE] Tmp_ConvertDateFormat

# ---- qrydates_convert6 ----
qrydates_convert6 <- "
UPDATE Tmp_ConvertDateFormat
SET Tmp_ConvertDateFormat.LAST_SEEN_BIRTHDATE_CONVERT = ''
WHERE ((Left(LAST_SEEN_BIRTHDATE,2)='  '));"
dbExecute(con, qrydates_convert6)

# [UPDATE] Tmp_ConvertDateFormat

# ---- qrydates_convert7 ----
qrydates_convert7 <- "UPDATE Tmp_ConvertDateFormat
SET Tmp_ConvertDateFormat.PSI_MIN_START_DATE_CONVERT = '20'+PSI_MIN_START_DATE
WHERE ((Left(PSI_MIN_START_DATE,2)<24));"
dbExecute(con, qrydates_convert7)

# [UPDATE] Tmp_ConvertDateFormat

# ---- qrydates_convert8 ----
qrydates_convert8 <- "
UPDATE Tmp_ConvertDateFormat
SET Tmp_ConvertDateFormat.PSI_MIN_START_DATE_CONVERT = '19'+PSI_MIN_START_DATE
WHERE ((Left(PSI_MIN_START_DATE,2)>23));"
dbExecute(con, qrydates_convert8)

# [UPDATE] Tmp_ConvertDateFormat

# ---- qrydates_convert9 ----
qrydates_convert9 <- "
UPDATE Tmp_ConvertDateFormat
SET Tmp_ConvertDateFormat.PSI_MIN_START_DATE_CONVERT = ''
WHERE ((Left(PSI_MIN_START_DATE,2)='  '));"
dbExecute(con, qrydates_convert9)

# [UPDATE] Tmp_ConvertDateFormat

# ---- qrydates_convert10 ----
qrydates_convert10 <- "UPDATE Tmp_ConvertDateFormat SET Tmp_ConvertDateFormat.PSI_PROGRAM_EFFECTIVE_DATE_CONVERT = '20'+PSI_PROGRAM_EFFECTIVE_DATE
WHERE ((Left(PSI_PROGRAM_EFFECTIVE_DATE,2)<24));"
dbExecute(con, qrydates_convert10)

# [UPDATE] Tmp_ConvertDateFormat

# ---- qrydates_convert11 ----
qrydates_convert11 <- "
UPDATE Tmp_ConvertDateFormat
SET Tmp_ConvertDateFormat.PSI_PROGRAM_EFFECTIVE_DATE_CONVERT = '19'+PSI_PROGRAM_EFFECTIVE_DATE
WHERE ((Left(PSI_PROGRAM_EFFECTIVE_DATE,2)>23));"
dbExecute(con, qrydates_convert11)

# [UPDATE] Tmp_ConvertDateFormat

# ---- qrydates_convert12 ----
qrydates_convert12 <- "
UPDATE Tmp_ConvertDateFormat
SET Tmp_ConvertDateFormat.PSI_PROGRAM_EFFECTIVE_DATE_CONVERT = ''
WHERE ((Left(PSI_PROGRAM_EFFECTIVE_DATE,2)='  '));"
dbExecute(con, qrydates_convert12)

# [UPDATE] STP_Enrolment

# ---- qrydates_update1 ----
qrydates_update1 <- "
UPDATE STP_Enrolment
SET STP_Enrolment.PSI_BIRTHDATE = tmp_ConvertDateFormat.PSI_BIRTHDATE_CONVERT
FROM tmp_ConvertDateFormat, STP_Enrolment
WHERE STP_Enrolment.ID = tmp_ConvertDateFormat.ID;"
dbExecute(con, qrydates_update1)

# [UPDATE] STP_Enrolment

# ---- qrydates_update2 ----
qrydates_update2 <- "
UPDATE STP_Enrolment
SET STP_Enrolment.LAST_SEEN_BIRTHDATE = tmp_ConvertDateFormat.LAST_SEEN_BIRTHDATE_CONVERT
FROM tmp_ConvertDateFormat, STP_Enrolment
WHERE STP_Enrolment.ID = tmp_ConvertDateFormat.ID;"
dbExecute(con, qrydates_update2)

# [UPDATE] STP_Enrolment

# ---- qrydates_update3 ----
qrydates_update3 <- "
UPDATE STP_Enrolment
SET STP_Enrolment.PSI_MIN_START_DATE = tmp_ConvertDateFormat.PSI_MIN_START_DATE_CONVERT
FROM tmp_ConvertDateFormat, STP_Enrolment
WHERE STP_Enrolment.ID = tmp_ConvertDateFormat.ID;"
dbExecute(con, qrydates_update3)

# [UPDATE] STP_Enrolment

# ---- qrydates_update4 ----
qrydates_update4 <- "
UPDATE STP_Enrolment
SET STP_Enrolment.PSI_PROGRAM_EFFECTIVE_DATE = tmp_ConvertDateFormat.PSI_PROGRAM_EFFECTIVE_DATE_CONVERT
FROM tmp_ConvertDateFormat, STP_Enrolment
WHERE STP_Enrolment.ID = tmp_ConvertDateFormat.ID;"
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

# [SQL]


# ---- qry01_ExtractAllID_into_STP_Enrolment_Record_Type ----
qry01_ExtractAllID_into_STP_Enrolment_Record_Type <- "
   CREATE TABLE STP_Enrolment_Record_Type (
   [ID] int NOT NULL,
   [RecordStatus] smallint,
   [MinEnrolment] smallint,
   [FirstEnrolment] smallint
   );

   INSERT INTO STP_Enrolment_Record_Type (ID)
   SELECT STP_Enrolment.ID
   FROM STP_Enrolment;"
dbExecute(con, qry01_ExtractAllID_into_STP_Enrolment_Record_Type)

# ----- Find records with Record_Status = 1 and update look up table -----

# [SELECT INTO] Create tmp_tbl_qry02a_Record_With_PEN_Or_STUID from STP_Enrolment


# ---- qry02a_Record_With_PEN_Or_STUID ----
qry02a_Record_With_PEN_Or_STUID <- "
SELECT     id, PSI_STUDENT_NUMBER, PSI_CODE, ENCRYPTED_TRUE_PEN
INTO       tmp_tbl_qry02a_Record_With_PEN_Or_STUID
FROM       STP_Enrolment
WHERE     (PSI_STUDENT_NUMBER NOT IN('',' ','(Unspecified)')
AND        PSI_CODE NOT IN('',' ','(Unspecified)'))
OR         (ENCRYPTED_TRUE_PEN NOT IN('',' ','(Unspecified)'));"
dbExecute(con, qry02a_Record_With_PEN_Or_STUID)

# [SELECT INTO] Create Drop_No_PEN_or_No_STUID from tmp_tbl_qry02a_Record_With_PEN_Or_STUID


# ---- qry02b_Drop_No_PEN_Or_No_STUID ----
qry02b_Drop_No_PEN_Or_No_STUID <- "
SELECT      STP_Enrolment.ID, STP_Enrolment.ENCRYPTED_TRUE_PEN, STP_Enrolment.PSI_STUDENT_NUMBER, STP_Enrolment.PSI_CODE
INTO        Drop_No_PEN_or_No_STUID
FROM        tmp_tbl_qry02a_Record_With_PEN_Or_STUID
RIGHT JOIN  STP_Enrolment
  ON        tmp_tbl_qry02a_Record_With_PEN_Or_STUID.ID = STP_Enrolment.ID
WHERE       ((tmp_tbl_qry02a_Record_With_PEN_Or_STUID.ID) Is Null);"
dbExecute(con, qry02b_Drop_No_PEN_Or_No_STUID)

# [UPDATE] STP_Enrolment_Record_Type


# ---- qry02c_Update_Drop_No_PEN_Or_No_STUID.SQL ----
qry02c_Update_Drop_No_PEN_Or_No_STUID <- "
UPDATE    STP_Enrolment_Record_Type
SET       RecordStatus = 1
FROM      STP_Enrolment_Record_Type INNER JOIN
          Drop_No_PEN_or_No_STUID ON STP_Enrolment_Record_Type.ID = Drop_No_PEN_or_No_STUID.ID;"
dbExecute(con, qry02c_Update_Drop_No_PEN_Or_No_STUID)
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[tmp_tbl_qry02a_Record_With_PEN_Or_STUID];"))
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[Drop_No_PEN_or_No_STUID];"))   

# ----- Find records with Record_Status = 2 and update look up table -----

# [SELECT INTO] Create Drop_Developmental from STP_Enrolment

# ---- qry03a_Drop_Record_Developmental ----
qry03a_Drop_Record_Developmental <- "
SELECT    ID, ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, PSI_CONTINUING_EDUCATION_COURSE_ONLY, LEFT(STP_Enrolment.PSI_CIP_CODE, 2) AS CIP2, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION, PSI_STUDY_LEVEL
INTO      Drop_Developmental
FROM      STP_Enrolment
WHERE     PSI_STUDY_LEVEL = 'DEVELOPMENTAL';"
dbExecute(con, qry03a_Drop_Record_Developmental)

# [UPDATE] STP_Enrolment_Record_Type


# ---- qry03b_Update_Drop_Record_Developmental ----
qry03b_Update_Drop_Record_Developmental <- "
UPDATE    STP_Enrolment_Record_Type
SET       RecordStatus = 2
FROM      STP_Enrolment_Record_Type INNER JOIN
          Drop_Developmental ON STP_Enrolment_Record_Type.ID = Drop_Developmental.ID;"
dbExecute(con, qry03b_Update_Drop_Record_Developmental)
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[Drop_Developmental];"))

# ----- Find records with Record_Status = 6 and update look up table -----

# [SELECT INTO] Create Drop_Skills_Based from STP_Enrolment

# ---- qry03c_Drop_Skills_Based ----
qry03c_Drop_Skills_Based <- "
SELECT    ID, ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, PSI_CONTINUING_EDUCATION_COURSE_ONLY, LEFT(STP_Enrolment.PSI_CIP_CODE, 2) AS CIP2,
          PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION, PSI_STUDY_LEVEL, PSI_CREDENTIAL_CATEGORY
INTO      Drop_Skills_Based
FROM      STP_Enrolment
WHERE     PSI_CONTINUING_EDUCATION_COURSE_ONLY = 'SKILLS CRS ONLY'
  AND     PSI_STUDY_LEVEL <>'DEVELOPMENTAL'
  AND     PSI_CREDENTIAL_CATEGORY IN ('NONE','OTHER');"
dbExecute(con, qry03c_Drop_Skills_Based)

# [SQL]

# ---- CheckSkillsBased ---
CheckSkillsBased <-
"SELECT PSI_CODE, PSI_CONTINUING_EDUCATION_COURSE_ONLY, CIP2, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION, PSI_STUDY_LEVEL, PSI_CREDENTIAL_CATEGORY
FROM  Drop_Skills_Based
GROUP BY PSI_CODE, PSI_CONTINUING_EDUCATION_COURSE_ONLY, CIP2, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION, PSI_STUDY_LEVEL, PSI_CREDENTIAL_CATEGORY;"
dbGetQuery(con, CheckSkillsBased) # check list of programs considered skills based.

dbExecute(con, "ALTER TABLE Drop_Skills_Based ADD KEEP nvarchar(2) NULL;")

# [UPDATE] Drop_Skills_Based

# ---- qry03da_Keep_TeachEd ----
qry03da_Keep_TeachEd <- "
UPDATE    Drop_Skills_Based
SET       KEEP = 'Y'
WHERE     (PSI_CODE = 'UFV') AND (PSI_PROGRAM_CODE = 'TEACH ED')
  OR      (PSI_CODE = 'UCFV') AND (PSI_PROGRAM_CODE = 'TEACH ED');"
dbExecute(con, qry03da_Keep_TeachEd) 

# [UPDATE] STP_Enrolment_Record_Type

# ---- qry03d_Update_Drop_Record_Skills_Based ----
qry03d_Update_Drop_Record_Skills_Based <- "
UPDATE    STP_Enrolment_Record_Type
SET       RecordStatus = 6
FROM      STP_Enrolment_Record_Type
INNER JOIN Drop_Skills_Based
  ON STP_Enrolment_Record_Type.ID = Drop_Skills_Based.ID
WHERE     RecordStatus is NULL and KEEP IS NULL;"
dbExecute(con, qry03d_Update_Drop_Record_Skills_Based) 
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[Drop_Skills_Based];")) 


# [SELECT INTO] Create Drop_ContinuingEd from STP_Enrolment

# ---- qry03d_1_Drop_Continuing_Ed ----
qry03d_1_Drop_Continuing_Ed <- "
SELECT    ID, ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, PSI_CONTINUING_EDUCATION_COURSE_ONLY, LEFT(STP_Enrolment.PSI_CIP_CODE, 2) AS CIP2, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION, PSI_STUDY_LEVEL
INTO      Drop_ContinuingEd
FROM      STP_Enrolment
WHERE     (PSI_STUDY_LEVEL <> 'DEVELOPMENTAL'
  AND     PSI_CONTINUING_EDUCATION_COURSE_ONLY <> 'SKILLS CRS ONLY'
  AND     PSI_CREDENTIAL_CATEGORY IN ('NONE','OTHER')
  AND     (Left(PSI_CIP_CODE,2) IN ('21', '32', '33', '34', '35', '36','37', '53', '89')));"
dbExecute(con, qry03d_1_Drop_Continuing_Ed)

# [UPDATE] STP_Enrolment_Record_Type

# ---- qry03d_2_Update_Drop_Continuing_Ed ----
qry03d_2_Update_Drop_Continuing_Ed <- "
UPDATE
STP_Enrolment_Record_Type
SET       RecordStatus = 6
FROM      STP_Enrolment_Record_Type
INNER JOIN Drop_ContinuingEd
ON        STP_Enrolment_Record_Type.ID = Drop_ContinuingEd.ID
WHERE     RecordStatus is NULL;"
dbExecute(con, qry03d_2_Update_Drop_Continuing_Ed)
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[Drop_ContinuingEd];"))


# [SELECT INTO] Create Drop_ContinuingEd_More from STP_Enrolment

# ---- qry03d_3_Drop_More_Continuing_Ed ----
qry03d_3_Drop_More_Continuing_Ed <- "
SELECT    STP_Enrolment.ID, STP_Enrolment.ENCRYPTED_TRUE_PEN, STP_Enrolment.PSI_STUDENT_NUMBER, STP_Enrolment.PSI_CODE,
          STP_Enrolment.PSI_CONTINUING_EDUCATION_COURSE_ONLY, LEFT(STP_Enrolment.PSI_CIP_CODE, 2) AS CIP2, STP_Enrolment.PSI_PROGRAM_CODE,
          STP_Enrolment.PSI_CREDENTIAL_PROGRAM_DESCRIPTION, STP_Enrolment.PSI_STUDY_LEVEL, STP_Enrolment_Record_Type.RecordStatus
INTO      Drop_ContinuingEd_More
FROM      STP_Enrolment
LEFT OUTER JOIN STP_Enrolment_Record_Type
  ON      STP_Enrolment.ID = STP_Enrolment_Record_Type.ID
WHERE     ((STP_Enrolment_Record_Type.RecordStatus IS NULL)
  AND     ((STP_Enrolment.PSI_CREDENTIAL_PROGRAM_DESCRIPTION LIKE '%Continuing Education')
  OR      (STP_Enrolment.PSI_CREDENTIAL_PROGRAM_DESCRIPTION LIKE '%Continuing Studies')
  OR      (STP_Enrolment.PSI_CREDENTIAL_PROGRAM_DESCRIPTION LIKE '%Audit%')
  OR      (STP_Enrolment.PSI_CREDENTIAL_PROGRAM_DESCRIPTION LIKE 'CE %')));"
dbExecute(con, qry03d_3_Drop_More_Continuing_Ed) 

# [UPDATE] STP_Enrolment_Record_Type


# ---- qry03d_4_Updated_Drop_ContinuingEdMore ----
qry03d_4_Updated_Drop_ContinuingEdMore <- "
UPDATE      STP_Enrolment_Record_Type
SET         STP_Enrolment_Record_Type.RecordStatus = 6
FROM        STP_Enrolment_Record_Type
INNER JOIN  Drop_ContinuingEd_More
  ON        STP_Enrolment_Record_Type.ID = Drop_ContinuingEd_More.ID
WHERE       STP_Enrolment_Record_Type.RecordStatus is NULL;"
dbExecute(con, qry03d_4_Updated_Drop_ContinuingEdMore)


# [SELECT INTO] Create Keep_Skills_Based from STP_Enrolment


# ---- qry03e_Keep_Skills_Based ----
qry03e_Keep_Skills_Based <- "
SELECT      ID, ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, PSI_CONTINUING_EDUCATION_COURSE_ONLY, LEFT(PSI_CIP_CODE, 2) AS CIP2, PSI_PROGRAM_CODE,
            PSI_CREDENTIAL_PROGRAM_DESCRIPTION, PSI_STUDY_LEVEL, PSI_CREDENTIAL_CATEGORY
INTO        Keep_Skills_Based
FROM        STP_Enrolment
WHERE       (PSI_CONTINUING_EDUCATION_COURSE_ONLY = 'SKILLS CRS ONLY')
AND (PSI_STUDY_LEVEL <> 'DEVELOPMENTAL')
AND (PSI_CREDENTIAL_CATEGORY NOT IN ('NONE', 'OTHER', 'SHORT CERTIFICATE'))
AND (NOT (PSI_CREDENTIAL_PROGRAM_DESCRIPTION LIKE '%Continuing Studies'))
AND (NOT (PSI_CREDENTIAL_PROGRAM_DESCRIPTION LIKE '%Audit%'))
AND (NOT (PSI_CREDENTIAL_PROGRAM_DESCRIPTION LIKE '%Continuing Education'))
AND (NOT (PSI_CREDENTIAL_PROGRAM_DESCRIPTION LIKE 'CE %'));"
dbExecute(con, qry03e_Keep_Skills_Based)
dbExecute(con, "ALTER TABLE Keep_Skills_Based ADD EXCLUDE nvarchar(2) NULL;")

# [UPDATE] Keep_Skills_Based

# ---- qry03ea_Exclude_Skills_Based_Programs ----
qry03ea_Exclude_Skills_Based_Programs <- "
UPDATE      Keep_Skills_Based
SET         Exclude = 'Y'
WHERE      (PSI_CODE = 'SEL'
  AND       PSI_CREDENTIAL_PROGRAM_DESCRIPTION = 'COMMUNITY, CORPORATE & INTERNATIONAL DEVELOPMENT')
  OR (PSI_CODE = 'NIC' AND CIP2 IN ('21', '32', '33', '34', '35', '36', '37', '53', '89'));"
dbExecute(con, qry03ea_Exclude_Skills_Based_Programs)


# [UPDATE] STP_Enrolment_Record_Type


# ---- qry03f_Update_Keep_Record_Skills_Based ----
qry03f_Update_Keep_Record_Skills_Based <- "
UPDATE      STP_Enrolment_Record_Type
SET         RecordStatus = 0
FROM        STP_Enrolment_Record_Type
INNER JOIN  Keep_Skills_Based
ON          STP_Enrolment_Record_Type.ID = Keep_Skills_Based.ID
WHERE       STP_Enrolment_Record_Type.RecordStatus IS NULL
AND         Keep_Skills_Based.Exclude IS NULL;"
dbExecute(con, qry03f_Update_Keep_Record_Skills_Based) 

# [UPDATE] STP_Enrolment_Record_Type


# ---- qry03fb_Update_Keep_Record_Skills_Based ----
qry03fb_Update_Keep_Record_Skills_Based <- "
UPDATE      STP_Enrolment_Record_Type
SET         RecordStatus = 6
FROM        STP_Enrolment_Record_Type
INNER JOIN  Keep_Skills_Based
  ON        STP_Enrolment_Record_Type.ID = Keep_Skills_Based.ID
WHERE       STP_Enrolment_Record_Type.RecordStatus IS NULL
  AND       Keep_Skills_Based.Exclude ='Y';"
dbExecute(con, qry03fb_Update_Keep_Record_Skills_Based) 

# manual investigation done here in the past and requires a review
# leaving for now as has minimal impact on final distributions

# [SELECT INTO] Create tmp_tbl_SkillsBasedCourses from STP_Enrolment_Record_Type


# ---- qry03g_create_table_SkillsBasedCourses ----
qry03g_create_table_SkillsBasedCourses <- "
SELECT    STP_Enrolment.PSI_CODE,
          STP_Enrolment.PSI_PROGRAM_CODE,
          STP_Enrolment.PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
          LEFT(STP_Enrolment.PSI_CIP_CODE, 2) AS CIP2,
          STP_Enrolment.PSI_CREDENTIAL_CATEGORY,
          STP_Enrolment.PSI_STUDY_LEVEL,
          STP_Enrolment.PSI_CONTINUING_EDUCATION_COURSE_ONLY,
          COUNT(*) AS Count
INTO      tmp_tbl_SkillsBasedCourses
FROM      STP_Enrolment_Record_Type
INNER JOIN STP_Enrolment
  ON STP_Enrolment_Record_Type.ID = STP_Enrolment.ID
WHERE     STP_Enrolment_Record_Type.RecordStatus = 6
GROUP BY STP_Enrolment.PSI_CODE,
          STP_Enrolment.PSI_PROGRAM_CODE,
          STP_Enrolment.PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
          LEFT(STP_Enrolment.PSI_CIP_CODE, 2),
          STP_Enrolment.PSI_CREDENTIAL_CATEGORY,
          STP_Enrolment.PSI_STUDY_LEVEL,
          STP_Enrolment.PSI_CONTINUING_EDUCATION_COURSE_ONLY;"
dbExecute(con, qry03g_create_table_SkillsBasedCourses) 
dbExecute(con, "ALTER TABLE tmp_tbl_SkillsBasedCourses ADD KEEP nvarchar(2) NULL;")

# [SELECT INTO] Create tmp_MoreSkillsBased_to_Keep from STP_Enrolment


# ---- qry03g_b_Keep_More_Skills_Based ----
qry03g_b_Keep_More_Skills_Based <- "
SELECT        STP_Enrolment.ID,
              tmp_tbl_SkillsBasedCourses.PSI_CODE,
              tmp_tbl_SkillsBasedCourses.PSI_PROGRAM_CODE, tmp_tbl_SkillsBasedCourses.CIP2,
              tmp_tbl_SkillsBasedCourses.PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
              tmp_tbl_SkillsBasedCourses.PSI_CREDENTIAL_CATEGORY,
              tmp_tbl_SkillsBasedCourses.PSI_STUDY_LEVEL,
              tmp_tbl_SkillsBasedCourses.PSI_CONTINUING_EDUCATION_COURSE_ONLY
INTO          tmp_MoreSkillsBased_to_Keep
FROM          STP_Enrolment
INNER JOIN    tmp_tbl_SkillsBasedCourses
  ON          STP_Enrolment.PSI_CODE = tmp_tbl_SkillsBasedCourses.PSI_CODE
  AND         STP_Enrolment.PSI_PROGRAM_CODE = tmp_tbl_SkillsBasedCourses.PSI_PROGRAM_CODE
  AND         STP_Enrolment.PSI_CREDENTIAL_PROGRAM_DESCRIPTION = tmp_tbl_SkillsBasedCourses.PSI_CREDENTIAL_PROGRAM_DESCRIPTION
  AND         STP_Enrolment.PSI_CREDENTIAL_CATEGORY = tmp_tbl_SkillsBasedCourses.PSI_CREDENTIAL_CATEGORY
  AND         STP_Enrolment.PSI_STUDY_LEVEL = tmp_tbl_SkillsBasedCourses.PSI_STUDY_LEVEL
  AND         STP_Enrolment.PSI_CONTINUING_EDUCATION_COURSE_ONLY = tmp_tbl_SkillsBasedCourses.PSI_CONTINUING_EDUCATION_COURSE_ONLY
WHERE        (tmp_tbl_SkillsBasedCourses.Keep = 'Yes');"
dbExecute(con, qry03g_b_Keep_More_Skills_Based) 

# [UPDATE] STP_Enrolment_Record_Type


# ---- qry03g_c_Update_Keep_More_Skills_Based ----
qry03g_c_Update_Keep_More_Skills_Based <- "
UPDATE        STP_Enrolment_Record_Type
SET           RecordStatus = 0
FROM          STP_Enrolment_Record_Type
INNER JOIN    tmp_MoreSkillsBased_to_Keep
  ON STP_Enrolment_Record_Type.ID = tmp_MoreSkillsBased_to_Keep.ID
WHERE        (STP_Enrolment_Record_Type.RecordStatus=6);"
dbExecute(con, qry03g_c_Update_Keep_More_Skills_Based)

# [UPDATE] STP_Enrolment_Record_Type


# ---- qry03g_c2_Update_More_Selkirk ----
qry03g_c2_Update_More_Selkirk <- "
UPDATE STP_Enrolment_Record_Type
SET RecordStatus = 6
FROM STP_Enrolment_Record_Type
INNER JOIN STP_Enrolment
  ON STP_Enrolment_Record_Type.ID = STP_Enrolment.ID
WHERE (STP_Enrolment.PSI_CODE = 'SEL')
  AND (STP_Enrolment.PSI_CREDENTIAL_PROGRAM_DESCRIPTION = 'COMMUNITY, CORPORATE & INTERNATIONAL DEVELOPMENT')
  AND (STP_Enrolment_Record_Type.RecordStatus IS NULL)"
dbExecute(con, qry03g_c2_Update_More_Selkirk)

# [SELECT INTO] Create tmp_tbl_EnrolCoursesSeen from STP_Enrolment


# ---- qry03g_d_EnrolCoursesSeen ----
qry03g_d_EnrolCoursesSeen <- "
SELECT  STP_Enrolment.PSI_CODE,
        STP_Enrolment.PSI_PROGRAM_CODE,
        STP_Enrolment.PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
        LEFT(STP_Enrolment.PSI_CIP_CODE, 2) AS CIP2,
        STP_Enrolment.PSI_CREDENTIAL_CATEGORY,
        STP_Enrolment.PSI_STUDY_LEVEL,
        STP_Enrolment.PSI_CONTINUING_EDUCATION_COURSE_ONLY,
        COUNT(*) AS Count
INTO    tmp_tbl_EnrolCoursesSeen
FROM    STP_Enrolment
GROUP BY STP_Enrolment.PSI_CODE,
        STP_Enrolment.PSI_PROGRAM_CODE,
        STP_Enrolment.PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
        LEFT(STP_Enrolment.PSI_CIP_CODE, 2),
        STP_Enrolment.PSI_CREDENTIAL_CATEGORY,
        STP_Enrolment.PSI_STUDY_LEVEL,
        STP_Enrolment.PSI_CONTINUING_EDUCATION_COURSE_ONLY;"
dbExecute(con, qry03g_d_EnrolCoursesSeen)

# [SELECT INTO] Create tmp_tbl_Suspect_Skills_Based from STP_Enrolment_Record_Type


# ---- qry03h_create_table_Suspect_Skills_Based ----
qry03h_create_table_Suspect_Skills_Based <- "
SELECT      STP_Enrolment.ID, STP_Enrolment.ENCRYPTED_TRUE_PEN,   STP_Enrolment.PSI_STUDENT_NUMBER, STP_Enrolment.PSI_STUDENT_POSTAL_CODE_CURRENT, STP_Enrolment.PSI_SCHOOL_YEAR, STP_Enrolment.PSI_REGISTRATION_TERM,
            STP_Enrolment.PSI_CODE, STP_Enrolment.PSI_PROGRAM_CODE, STP_Enrolment.PSI_CREDENTIAL_PROGRAM_DESCRIPTION, LEFT(STP_Enrolment.PSI_CIP_CODE,2) AS CIP2, STP_Enrolment.PSI_CREDENTIAL_CATEGORY,
            STP_Enrolment.PSI_STUDY_LEVEL, STP_Enrolment.PSI_ENTRY_STATUS, STP_Enrolment.PSI_BIRTHDATE, STP_Enrolment.PSI_GENDER, STP_Enrolment.PSI_MIN_START_DATE, STP_Enrolment.PSI_CONTINUING_EDUCATION_COURSE_ONLY
INTO        tmp_tbl_Suspect_Skills_Based
FROM        STP_Enrolment_Record_Type
INNER JOIN  STP_Enrolment
  ON        STP_Enrolment_Record_Type.ID = STP_Enrolment.ID
WHERE    (STP_Enrolment_Record_Type.RecordStatus IS NULL);"
dbExecute(con, qry03h_create_table_Suspect_Skills_Based) 

# [SELECT INTO] Create Suspect_Skills_Based from tmp_tbl_Suspect_Skills_Based


# ---- qry03i_Find_Suspect_Skills_Based ----
qry03i_Find_Suspect_Skills_Based <- "
SELECT    tmp_tbl_Suspect_Skills_Based.ID, tmp_tbl_Suspect_Skills_Based.ENCRYPTED_TRUE_PEN, tmp_tbl_Suspect_Skills_Based.PSI_STUDENT_NUMBER,
          tmp_tbl_Suspect_Skills_Based.PSI_CODE, tmp_tbl_Suspect_Skills_Based.PSI_PROGRAM_CODE,
          tmp_tbl_Suspect_Skills_Based.PSI_CREDENTIAL_PROGRAM_DESCRIPTION, tmp_tbl_Suspect_Skills_Based.CIP2,
          tmp_tbl_Suspect_Skills_Based.PSI_CREDENTIAL_CATEGORY, tmp_tbl_Suspect_Skills_Based.PSI_STUDY_LEVEL,
          tmp_tbl_Suspect_Skills_Based.PSI_CONTINUING_EDUCATION_COURSE_ONLY, tmp_tbl_SkillsBasedCourses.Keep
INTO      Suspect_Skills_Based
FROM      tmp_tbl_Suspect_Skills_Based
INNER JOIN  tmp_tbl_SkillsBasedCourses
  ON      tmp_tbl_Suspect_Skills_Based.PSI_CODE = tmp_tbl_SkillsBasedCourses.PSI_CODE
  AND     tmp_tbl_Suspect_Skills_Based.PSI_PROGRAM_CODE = tmp_tbl_SkillsBasedCourses.PSI_PROGRAM_CODE
  AND     tmp_tbl_Suspect_Skills_Based.PSI_CREDENTIAL_PROGRAM_DESCRIPTION = tmp_tbl_SkillsBasedCourses.PSI_CREDENTIAL_PROGRAM_DESCRIPTION
  AND     tmp_tbl_Suspect_Skills_Based.CIP2 = tmp_tbl_SkillsBasedCourses.CIP2
  AND     tmp_tbl_Suspect_Skills_Based.PSI_CREDENTIAL_CATEGORY = tmp_tbl_SkillsBasedCourses.PSI_CREDENTIAL_CATEGORY
  AND     tmp_tbl_Suspect_Skills_Based.PSI_STUDY_LEVEL = tmp_tbl_SkillsBasedCourses.PSI_STUDY_LEVEL
WHERE (tmp_tbl_SkillsBasedCourses.Keep IS NULL);"
dbExecute(con, qry03i_Find_Suspect_Skills_Based) 

# [UPDATE] Suspect_Skills_Based


# ---- qry03i2_Drop_Suspect_Skills_Based ----
qry03i2_Drop_Suspect_Skills_Based <-
"UPDATE  Suspect_Skills_Based
SET  Keep = 'Y'
FROM  Suspect_Skills_Based
INNER JOIN tmp_tbl_SkillsBasedCourses
  ON Suspect_Skills_Based.PSI_CODE = tmp_tbl_SkillsBasedCourses.PSI_CODE
  AND Suspect_Skills_Based.PSI_PROGRAM_CODE = tmp_tbl_SkillsBasedCourses.PSI_PROGRAM_CODE
  AND Suspect_Skills_Based.PSI_CREDENTIAL_PROGRAM_DESCRIPTION = tmp_tbl_SkillsBasedCourses.PSI_CREDENTIAL_PROGRAM_DESCRIPTION
  AND Suspect_Skills_Based.CIP2 = tmp_tbl_SkillsBasedCourses.CIP2
  AND Suspect_Skills_Based.PSI_CREDENTIAL_CATEGORY = tmp_tbl_SkillsBasedCourses.PSI_CREDENTIAL_CATEGORY
  AND Suspect_Skills_Based.PSI_STUDY_LEVEL = tmp_tbl_SkillsBasedCourses.PSI_STUDY_LEVEL
WHERE (tmp_tbl_SkillsBasedCourses.KEEP = 'Y')"
dbExecute(con, qry03i2_Drop_Suspect_Skills_Based)   #see documentation, this is related to some manula work that wasn't done in 2023

# [UPDATE] STP_Enrolment_Record_Type

# ---- qry03j_Update_Suspect_Skills_Based ----
qry03j_Update_Suspect_Skills_Based <- "
UPDATE    STP_Enrolment_Record_Type
SET       RecordStatus = 6
FROM      Suspect_Skills_Based INNER JOIN STP_Enrolment_Record_Type
  ON        Suspect_Skills_Based.ID = STP_Enrolment_Record_Type.ID
WHERE STP_Enrolment_Record_Type.RecordStatus IS NULL AND Suspect_Skills_Based.Keep IS NULL;"
dbExecute(con, qry03j_Update_Suspect_Skills_Based) 

# [SQL]

# ---- RecordTypeSummary ----
RecordTypeSummary <-
"SELECT RecordStatus, COUNT(*) AS Expr1
FROM  STP_Enrolment_Record_Type
GROUP BY RecordStatus
"
dbGetQuery(con, RecordTypeSummary)

dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[Drop_ContinuingEd_More];"))
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[Keep_Skills_Based];"))
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[tmp_MoreSkillsBased_to_Keep];"))  
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[tmp_tbl_EnrolCoursesSeen];"))  
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[tmp_tbl_Suspect_Skills_Based];")) 
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[tmp_tbl_SkillsBasedCourses];"))  
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[Suspect_Skills_Based];"))                

# ---- Find records with Record_Status = 7 and update look up table ----

# [SELECT INTO] Create Drop_Developmental_CIPS from STP_Enrolment_Record_Type


# ---- qry03k_Drop_Developmental_CIPS ----
qry03k_Drop_Developmental_CIPS <- "SELECT
 STP_Enrolment.ID,   STP_Enrolment.ENCRYPTED_TRUE_PEN,   STP_Enrolment.PSI_STUDENT_NUMBER,   STP_Enrolment.PSI_CODE,
 STP_Enrolment.PSI_PROGRAM_CODE, STP_Enrolment.PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
 LEFT(STP_Enrolment.PSI_CIP_CODE, 2) AS CIP2, STP_Enrolment.PSI_CREDENTIAL_CATEGORY,
 STP_Enrolment.PSI_CONTINUING_EDUCATION_COURSE_ONLY
INTO  Drop_Developmental_CIPS
FROM  STP_Enrolment_Record_Type
INNER JOIN STP_Enrolment ON STP_Enrolment_Record_Type.ID = STP_Enrolment.ID
WHERE     (STP_Enrolment.PSI_CONTINUING_EDUCATION_COURSE_ONLY = 'NOT SKILLS CRS ONLY') AND
(STP_Enrolment_Record_Type.RecordStatus IS NULL) AND
(LEFT(STP_Enrolment.PSI_CIP_CODE, 2) IN ('21', '32','33', '34', '35', '36', '37', '53', '89'));"
dbExecute(con, qry03k_Drop_Developmental_CIPS)        
dbExecute(con, "ALTER TABLE Drop_Developmental_CIPS 
          ADD DO_NOT_EXCLUDE nvarchar(2) NULL;")

# [UPDATE] STP_Enrolment_Record_Type


# ---- qry03l_Update_Developmental_CIPs ----
qry03l_Update_Developmental_CIPs <- "
UPDATE  STP_Enrolment_Record_Type
SET     RecordStatus = 7
FROM    STP_Enrolment_Record_Type
INNER JOIN Drop_Developmental_CIPS
  ON    STP_Enrolment_Record_Type.ID = Drop_Developmental_CIPS.ID
WHERE   STP_Enrolment_Record_Type.RecordStatus IS NULL
  AND   Drop_Developmental_CIPS.DO_NOT_EXCLUDE IS Null;"
dbExecute(con, qry03l_Update_Developmental_CIPs)
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[Drop_Developmental_CIPS];"))

# ---- Find records with Record_Status = 5 and update look up table ----

# [SELECT INTO] Create Drop_No_Transition from STP_Enrolment


# ---- qry04a_Drop_No_PSI_Transition ----
qry04a_Drop_No_PSI_Transition <- "
SELECT  ID, ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, PSI_ENTRY_STATUS
INTO    Drop_No_Transition
FROM    STP_Enrolment
WHERE   PSI_ENTRY_STATUS = 'No Transition';"
dbExecute(con, qry04a_Drop_No_PSI_Transition)

# [UPDATE] STP_Enrolment_Record_Type


# ---- qry04b_Update_Drop_No_PSI_Transition ----
qry04b_Update_Drop_No_PSI_Transition <- "
UPDATE  STP_Enrolment_Record_Type
SET     RecordStatus = 3
FROM    STP_Enrolment_Record_Type
INNER JOIN Drop_No_Transition
  ON    STP_Enrolment_Record_Type.ID = Drop_No_Transition.ID
WHERE   STP_Enrolment_Record_Type.RecordStatus is NULL;"
dbExecute(con, qry04b_Update_Drop_No_PSI_Transition)
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[Drop_No_Transition];"))


# [SELECT INTO] Create Drop_PSI_Outside_BC from STP_Enrolment


# ---- qry06a_Drop_PSI_Outside_BC ----
qry06a_Drop_PSI_Outside_BC <- "
SELECT  ID, ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, ATTENDING_PSI_OUTSIDE_BC
INTO    Drop_PSI_Outside_BC
FROM    STP_Enrolment
WHERE   ATTENDING_PSI_OUTSIDE_BC = 'Y';"
dbExecute(con, qry06a_Drop_PSI_Outside_BC)

# [UPDATE] STP_Enrolment_Record_Type


# ---- qry06b_Update_Drop_PSI_Outside_BC ----
qry06b_Update_Drop_PSI_Outside_BC <- "
UPDATE    STP_Enrolment_Record_Type
SET       RecordStatus = 5
FROM      STP_Enrolment_Record_Type INNER JOIN
          Drop_PSI_Outside_BC ON STP_Enrolment_Record_Type.ID = Drop_PSI_Outside_BC.ID
WHERE STP_Enrolment_Record_Type.RecordStatus is NULL;"
dbExecute(con, qry06b_Update_Drop_PSI_Outside_BC)
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[Drop_PSI_Outside_BC];"))

# ---- Set Remaining Records to Record_Status = 0 ----

# [UPDATE] STP_Enrolment_Record_Type

# ---- qry07_Update_RecordStatus_No_Dropped ----
qry07_Update_RecordStatus_No_Dropped <- "
UPDATE  STP_Enrolment_Record_Type
SET     STP_Enrolment_Record_Type.RecordStatus = 0
WHERE   STP_Enrolment_Record_Type.RecordStatus Is Null;"
dbExecute(con, qry07_Update_RecordStatus_No_Dropped)

# [SELECT INTO] Create STP_Enrolment_Valid from STP_Enrolment


# ---- qry08a_Create_Table_STP_Enrolment_Valid ----
qry08a_Create_Table_STP_Enrolment_Valid <- "
SELECT  STP_Enrolment.ID, STP_Enrolment.PSI_STUDENT_NUMBER, STP_Enrolment.ENCRYPTED_TRUE_PEN, STP_Enrolment.PSI_SCHOOL_YEAR,
        STP_Enrolment.PSI_STUDENT_POSTAL_CODE_CURRENT, STP_Enrolment.PSI_ENROLMENT_SEQUENCE, STP_Enrolment.PSI_CODE,
        STP_Enrolment.PSI_MIN_START_DATE
INTO    STP_Enrolment_Valid
FROM    STP_Enrolment
INNER JOIN STP_Enrolment_Record_Type
        ON STP_Enrolment.ID = STP_Enrolment_Record_Type.ID
WHERE     (STP_Enrolment_Record_Type.RecordStatus = 0);"
dbExecute(con, qry08a_Create_Table_STP_Enrolment_Valid)
dbExecute(con, "ALTER TABLE [STP_Enrolment_Valid] 
                ADD CONSTRAINT ValidEnrolmentPK_ID
                PRIMARY KEY (ID);")

# check count of records in STP_Enrolment_Valid associated with > 1 EPEN.  
cat("Records associated with > 1 EPEN:")
print(dbGetQuery(con, "SELECT  T.PSI_CODE, T.PSI_STUDENT_NUMBER, COUNT(*) 
                FROM (
	              SELECT PSI_CODE, PSI_STUDENT_NUMBER, ENCRYPTED_TRUE_PEN
	                FROM  STP_Enrolment_Valid
	                GROUP BY  PSI_CODE, PSI_STUDENT_NUMBER, ENCRYPTED_TRUE_PEN) T
                GROUP BY  T.PSI_CODE, T.PSI_STUDENT_NUMBER
                HAVING COUNT(*) <> 1"))

dbGetQuery(con, RecordTypeSummary)

# ---- Min Enrolment ----
# Find record with minimum enrollment sequence for each student per school year 
# by ENCRYPTED_TRUE_PEN

# [SELECT INTO] Create tmp_tbl_qry09a_MinEnrolmentPEN from STP_Enrolment_Valid

# ---- qry09a_MinEnrolmentPEN ----
qry09a_MinEnrolmentPEN <- "
SELECT    ENCRYPTED_TRUE_PEN, PSI_SCHOOL_YEAR, MIN(PSI_ENROLMENT_SEQUENCE) AS MinPSIEnrolmentSequence
INTO      tmp_tbl_qry09a_MinEnrolmentPEN
FROM      STP_Enrolment_Valid
GROUP BY  ENCRYPTED_TRUE_PEN, PSI_SCHOOL_YEAR
HAVING    ENCRYPTED_TRUE_PEN NOT IN('',' ','(Unspecified)');"
dbExecute(con, qry09a_MinEnrolmentPEN)

# [SELECT INTO] Create tmp_tbl_qry09b_MinEnrolmentPEN from tmp_tbl_qry09a_MinEnrolmentPEN

# ---- qry09b_MinEnrolmentPEN ----
qry09b_MinEnrolmentPEN <- "
SELECT    STP_Enrolment_Valid.ENCRYPTED_TRUE_PEN,
          STP_Enrolment_Valid.PSI_SCHOOL_YEAR,
          STP_Enrolment_Valid.PSI_ENROLMENT_SEQUENCE,
          MIN(STP_Enrolment_Valid.ID) AS MinID
INTO      tmp_tbl_qry09b_MinEnrolmentPEN
FROM      tmp_tbl_qry09a_MinEnrolmentPEN
INNER JOIN STP_Enrolment_Valid
  ON      tmp_tbl_qry09a_MinEnrolmentPEN.ENCRYPTED_TRUE_PEN = STP_Enrolment_Valid.ENCRYPTED_TRUE_PEN
  AND     tmp_tbl_qry09a_MinEnrolmentPEN.PSI_SCHOOL_YEAR = STP_Enrolment_Valid.PSI_SCHOOL_YEAR
  AND     tmp_tbl_qry09a_MinEnrolmentPEN.MinPSIEnrolmentSequence = STP_Enrolment_Valid.PSI_ENROLMENT_SEQUENCE
GROUP BY STP_Enrolment_Valid.ENCRYPTED_TRUE_PEN, STP_Enrolment_Valid.PSI_SCHOOL_YEAR, STP_Enrolment_Valid.PSI_ENROLMENT_SEQUENCE;"
dbExecute(con, qry09b_MinEnrolmentPEN)

# [SELECT INTO] Create MinEnrolment_ID_PEN from tmp_tbl_qry09b_MinEnrolmentPEN

# ---- qry09c_MinEnrolmentPEN ----
qry09c_MinEnrolmentPEN <- "
SELECT    MinID AS MinOfID
INTO      MinEnrolment_ID_PEN
FROM      tmp_tbl_qry09b_MinEnrolmentPEN;"
dbExecute(con, qry09c_MinEnrolmentPEN)
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[tmp_tbl_qry09a_MinEnrolmentPEN];"))
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[tmp_tbl_qry09b_MinEnrolmentPEN];"))

# by PSI_CODE/PSI_STUDENT_NUMBER combo for students records with null ENCRYPTED_TRUE_PEN's

# [SELECT INTO] Create tmp_tbl_qry10a_MinEnrolmentSTUID from STP_Enrolment_Valid

# ---- qry10a_MinEnrolmentSTUID ----
qry10a_MinEnrolmentSTUID <- "
SELECT    PSI_STUDENT_NUMBER, PSI_CODE, PSI_SCHOOL_YEAR, MIN(PSI_ENROLMENT_SEQUENCE) AS MinPSIEnrolmentSequence, ENCRYPTED_TRUE_PEN
INTO      tmp_tbl_qry10a_MinEnrolmentSTUID
FROM      STP_Enrolment_Valid
GROUP BY  PSI_STUDENT_NUMBER, PSI_CODE, PSI_SCHOOL_YEAR, ENCRYPTED_TRUE_PEN
HAVING    ENCRYPTED_TRUE_PEN IN('',' ','(Unspecified)');"
dbExecute(con, qry10a_MinEnrolmentSTUID)

# [SELECT INTO] Create tmp_tbl_qry10b_MinEnrolmentSTUID from tmp_tbl_qry10a_MinEnrolmentSTUID

# ---- qry10b_MinEnrolmentSTUID ----
qry10b_MinEnrolmentSTUID <- "
SELECT    STP_Enrolment_Valid.PSI_STUDENT_NUMBER, STP_Enrolment_Valid.PSI_CODE , STP_Enrolment_Valid.PSI_SCHOOL_YEAR, STP_Enrolment_Valid.PSI_ENROLMENT_SEQUENCE, MIN(STP_Enrolment_Valid.ID) AS MinID
INTO      tmp_tbl_qry10b_MinEnrolmentSTUID
FROM      tmp_tbl_qry10a_MinEnrolmentSTUID
INNER JOIN STP_Enrolment_Valid
  ON tmp_tbl_qry10a_MinEnrolmentSTUID.PSI_STUDENT_NUMBER = STP_Enrolment_Valid.PSI_STUDENT_NUMBER
  AND tmp_tbl_qry10a_MinEnrolmentSTUID.PSI_SCHOOL_YEAR = STP_Enrolment_Valid.PSI_SCHOOL_YEAR
  AND tmp_tbl_qry10a_MinEnrolmentSTUID.PSI_CODE = STP_Enrolment_Valid.PSI_CODE
  AND tmp_tbl_qry10a_MinEnrolmentSTUID.MinPSIEnrolmentSequence = STP_Enrolment_Valid.PSI_ENROLMENT_SEQUENCE
GROUP BY STP_Enrolment_Valid.PSI_STUDENT_NUMBER, STP_Enrolment_Valid.PSI_CODE, STP_Enrolment_Valid.PSI_SCHOOL_YEAR, STP_Enrolment_Valid.PSI_ENROLMENT_SEQUENCE;"
dbExecute(con, qry10b_MinEnrolmentSTUID)

# [SELECT INTO] Create MinEnrolment_ID_STUID from tmp_tbl_qry10b_MinEnrolmentSTUID

# ---- qry10c_MinEnrolmentSTUID ----
qry10c_MinEnrolmentSTUID <- "
SELECT    MinID AS MinOfID
INTO      MinEnrolment_ID_STUID
FROM      tmp_tbl_qry10b_MinEnrolmentSTUID;"
dbExecute(con, qry10c_MinEnrolmentSTUID)
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[tmp_tbl_qry10b_MinEnrolmentSTUID];"))
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[tmp_tbl_qry10a_MinEnrolmentSTUID];"))

# Flag each record in STP_Enrolment_Record_Type as min enrollment (TRUE = 1, FALSE  = 0)

# [UPDATE] STP_Enrolment_Record_Type

# ---- qry11a_Update_MinEnrolmentPEN ----
qry11a_Update_MinEnrolmentPEN <- "
UPDATE    STP_Enrolment_Record_Type
SET       MinEnrolment = 1
FROM      MinEnrolment_ID_PEN
INNER JOIN STP_Enrolment_Record_Type
  ON MinEnrolment_ID_PEN.MinOfID = STP_Enrolment_Record_Type.ID;"
dbExecute(con, qry11a_Update_MinEnrolmentPEN)

# [UPDATE] STP_Enrolment_Record_Type

# ---- qry11b_Update_MinEnrolmentSTUID ----
qry11b_Update_MinEnrolmentSTUID <- "
UPDATE    STP_Enrolment_Record_Type
SET       MinEnrolment = 1
FROM      MinEnrolment_ID_STUID
INNER JOIN STP_Enrolment_Record_Type
  ON MinEnrolment_ID_STUID.MinOfID = STP_Enrolment_Record_Type.ID
WHERE STP_Enrolment_Record_Type.MinEnrolment = 0
  OR  STP_Enrolment_Record_Type.MinEnrolment IS NULL;"
dbExecute(con, qry11b_Update_MinEnrolmentSTUID)

# [UPDATE] STP_Enrolment_Record_Type

# ---- qry11c_Update_MinEnrolment_NA ----
qry11c_Update_MinEnrolment_NA <- "
UPDATE STP_Enrolment_Record_Type
SET STP_Enrolment_Record_Type.MinEnrolment = 0
WHERE (((STP_Enrolment_Record_Type.MinEnrolment) Is Null));"
dbExecute(con, qry11c_Update_MinEnrolment_NA)

dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[MinEnrolment_ID_PEN];"))
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[MinEnrolment_ID_STUID];"))

# ---- First Enrollment Date ---- 
# Find earliest enrollment record for each student per school year
# by ENCRYPTED_TRUE_PEN

# [SELECT INTO] Create tmp_tbl_qry12a_FirstEnrolmentPEN from STP_Enrolment_Valid

# ---- qry12a_FirstEnrolmentPEN ----
qry12a_FirstEnrolmentPEN <- "
SELECT    ENCRYPTED_TRUE_PEN, MIN(PSI_MIN_START_DATE) AS MIN_PSI_MIN_START_DATE
INTO      tmp_tbl_qry12a_FirstEnrolmentPEN
FROM      STP_Enrolment_Valid
GROUP BY  ENCRYPTED_TRUE_PEN
HAVING    ENCRYPTED_TRUE_PEN NOT IN('',' ','(Unspecified)');"
dbExecute(con, qry12a_FirstEnrolmentPEN)

# [SELECT INTO] Create tmp_tbl_qry12b_FirstEnrolmentPEN from tmp_tbl_qry12a_FirstEnrolmentPEN

# ---- qry12b_FirstEnrolmentPEN ----
qry12b_FirstEnrolmentPEN <- "
SELECT    STP_Enrolment_Valid.ENCRYPTED_TRUE_PEN, tmp_tbl_qry12a_FirstEnrolmentPEN.MIN_PSI_MIN_START_DATE, MIN(STP_Enrolment_Valid.PSI_ENROLMENT_SEQUENCE) AS MinPSI_Enrolment_Sequence
INTO      tmp_tbl_qry12b_FirstEnrolmentPEN
FROM      tmp_tbl_qry12a_FirstEnrolmentPEN
INNER JOIN STP_Enrolment_Valid
  ON tmp_tbl_qry12a_FirstEnrolmentPEN.ENCRYPTED_TRUE_PEN = STP_Enrolment_Valid.ENCRYPTED_TRUE_PEN
  AND tmp_tbl_qry12a_FirstEnrolmentPEN.MIN_PSI_MIN_START_DATE = STP_Enrolment_Valid.PSI_MIN_START_DATE
GROUP BY STP_Enrolment_Valid.ENCRYPTED_TRUE_PEN, tmp_tbl_qry12a_FirstEnrolmentPEN.MIN_PSI_MIN_START_DATE;"
dbExecute(con, qry12b_FirstEnrolmentPEN)

# [SELECT INTO] Create FirstEnrolment_ID_PEN from tmp_tbl_qry12b_FirstEnrolmentPEN

# ---- qry12c_FirstEnrolmentPEN ----
qry12c_FirstEnrolmentPEN <- "
SELECT    MIN(STP_Enrolment_Valid.ID) AS MinID, STP_Enrolment_Valid.ENCRYPTED_TRUE_PEN, STP_Enrolment_Valid.PSI_MIN_START_DATE,  STP_Enrolment_Valid.PSI_ENROLMENT_SEQUENCE
INTO      FirstEnrolment_ID_PEN
FROM      tmp_tbl_qry12b_FirstEnrolmentPEN
INNER JOIN STP_Enrolment_Valid
ON tmp_tbl_qry12b_FirstEnrolmentPEN.ENCRYPTED_TRUE_PEN = STP_Enrolment_Valid.ENCRYPTED_TRUE_PEN
AND tmp_tbl_qry12b_FirstEnrolmentPEN.MIN_PSI_MIN_START_DATE = STP_Enrolment_Valid.PSI_MIN_START_DATE
AND tmp_tbl_qry12b_FirstEnrolmentPEN.MinPSI_Enrolment_Sequence = STP_Enrolment_Valid.PSI_ENROLMENT_SEQUENCE
GROUP BY STP_Enrolment_Valid.ENCRYPTED_TRUE_PEN, STP_Enrolment_Valid.PSI_MIN_START_DATE, STP_Enrolment_Valid.PSI_ENROLMENT_SEQUENCE;"
dbExecute(con, qry12c_FirstEnrolmentPEN)
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[tmp_tbl_qry12a_FirstEnrolmentPEN];"))
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[tmp_tbl_qry12b_FirstEnrolmentPEN];"))

# by PSI_CODE/PSI_STUDENT_NUMBER combo for students records with null ENCRYPTED_TRUE_PEN's

# [SELECT INTO] Create tmp_tbl_qry13a_FirstEnrolment_STUID from STP_Enrolment_Valid


# ---- qry13a_FirstEnrolmentSTUID ----
qry13a_FirstEnrolmentSTUID <- "
SELECT    PSI_STUDENT_NUMBER, PSI_CODE, MIN(PSI_MIN_START_DATE) AS Min_PSI_Min_Start_Date
INTO      tmp_tbl_qry13a_FirstEnrolment_STUID
FROM      STP_Enrolment_Valid
WHERE     ENCRYPTED_TRUE_PEN IN('',' ','(Unspecified)')
GROUP BY PSI_STUDENT_NUMBER, PSI_CODE;"
dbExecute(con, qry13a_FirstEnrolmentSTUID)

# [SELECT INTO] Create tmp_tbl_qry13b_FirstEnrolment_STUID from tmp_tbl_qry13a_FirstEnrolment_STUID

# ---- qry13b_FirstEnrolmentSTUID ----
qry13b_FirstEnrolmentSTUID <- "
SELECT    STP_Enrolment_Valid.PSI_STUDENT_NUMBER, STP_Enrolment_Valid.PSI_CODE, tmp_tbl_qry13a_FirstEnrolment_STUID.Min_PSI_Min_Start_Date,
          MIN(STP_Enrolment_Valid.PSI_ENROLMENT_SEQUENCE) AS Min_PSI_Enrolment_Sequence
INTO      tmp_tbl_qry13b_FirstEnrolment_STUID
FROM      tmp_tbl_qry13a_FirstEnrolment_STUID
INNER JOIN STP_Enrolment_Valid
  ON tmp_tbl_qry13a_FirstEnrolment_STUID.PSI_STUDENT_NUMBER = STP_Enrolment_Valid.PSI_STUDENT_NUMBER
  AND tmp_tbl_qry13a_FirstEnrolment_STUID.PSI_CODE = STP_Enrolment_Valid.PSI_CODE
  AND tmp_tbl_qry13a_FirstEnrolment_STUID.Min_PSI_Min_Start_Date = STP_Enrolment_Valid.PSI_MIN_START_DATE
GROUP BY STP_Enrolment_Valid.PSI_STUDENT_NUMBER, STP_Enrolment_Valid.PSI_CODE, tmp_tbl_qry13a_FirstEnrolment_STUID.Min_PSI_Min_Start_Date;"
dbExecute(con, qry13b_FirstEnrolmentSTUID)

# [SELECT INTO] Create FirstEnrolment_ID_STUID from tmp_tbl_qry13b_FirstEnrolment_STUID

# ---- qry13c_FirstEnrolmentSTUID ----
qry13c_FirstEnrolmentSTUID <- "
SELECT    MIN(STP_Enrolment_Valid.ID) AS MinID, STP_Enrolment_Valid.ENCRYPTED_TRUE_PEN, STP_Enrolment_Valid.PSI_STUDENT_NUMBER, STP_Enrolment_Valid.PSI_CODE
INTO      FirstEnrolment_ID_STUID
FROM      tmp_tbl_qry13b_FirstEnrolment_STUID
INNER JOIN STP_Enrolment_Valid
  ON tmp_tbl_qry13b_FirstEnrolment_STUID.PSI_STUDENT_NUMBER = STP_Enrolment_Valid.PSI_STUDENT_NUMBER
  AND tmp_tbl_qry13b_FirstEnrolment_STUID.PSI_CODE = STP_Enrolment_Valid.PSI_CODE
  AND tmp_tbl_qry13b_FirstEnrolment_STUID.Min_PSI_Enrolment_Sequence = STP_Enrolment_Valid.PSI_ENROLMENT_SEQUENCE
  AND tmp_tbl_qry13b_FirstEnrolment_STUID.Min_PSI_Min_Start_Date = STP_Enrolment_Valid.PSI_MIN_START_DATE
GROUP BY STP_Enrolment_Valid.ENCRYPTED_TRUE_PEN, STP_Enrolment_Valid.PSI_STUDENT_NUMBER, STP_Enrolment_Valid.PSI_CODE;"
dbExecute(con, qry13c_FirstEnrolmentSTUID)
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[tmp_tbl_qry13a_FirstEnrolment_STUID];"))
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[tmp_tbl_qry13b_FirstEnrolment_STUID];"))

# Flag each record in STP_Enrolment_Record_Type as first enrollment (TRUE = 1, FALSE  = 0)

# [UPDATE] STP_Enrolment_Record_Type

# ---- qry14a_Update_FirstEnrolmentPEN ----
qry14a_Update_FirstEnrolmentPEN <- "
UPDATE    STP_Enrolment_Record_Type
SET       FirstEnrolment = 1
FROM      FirstEnrolment_ID_PEN
INNER JOIN STP_Enrolment_Record_Type
  ON FirstEnrolment_ID_PEN.MinID = STP_Enrolment_Record_Type.ID
WHERE     STP_Enrolment_Record_Type.FirstEnrolment IS NULL
    OR    STP_Enrolment_Record_Type.FirstEnrolment = 0;"
dbExecute(con, qry14a_Update_FirstEnrolmentPEN)

# [UPDATE] STP_Enrolment_Record_Type

# ---- qry14b_Update_FirstEnrolmentSTUID ----
qry14b_Update_FirstEnrolmentSTUID <- "
UPDATE    STP_Enrolment_Record_Type
SET       FirstEnrolment = 1
FROM      STP_Enrolment_Record_Type
INNER JOIN FirstEnrolment_ID_STUID
  ON STP_Enrolment_Record_Type.ID = FirstEnrolment_ID_STUID.MinID
WHERE STP_Enrolment_Record_Type.FirstEnrolment IS NULL;"
dbExecute(con, qry14b_Update_FirstEnrolmentSTUID)

# [UPDATE] STP_Enrolment_Record_Type

# ---- qry14c_Update_FirstEnrolmentNA ----
qry14c_Update_FirstEnrolmentNA <- "
UPDATE    STP_Enrolment_Record_Type
SET       FirstEnrolment = 0
WHERE     FirstEnrolment IS NULL;"
dbExecute(con, qry14c_Update_FirstEnrolmentNA)
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[FirstEnrolment_ID_PEN];"))
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[FirstEnrolment_ID_STUID];"))

# ---- Clean Birthdates ----

# [SELECT INTO] Create tmp_BirthDate from STP_Enrolment

# ---- ----
qry01_BirthdateCleaning <-  "
SELECT     ENCRYPTED_TRUE_PEN, PSI_BIRTHDATE, count(*) as NumBirthdateRecords
INTO        tmp_BirthDate
FROM        STP_Enrolment
GROUP BY    ENCRYPTED_TRUE_PEN, PSI_BIRTHDATE"
dbExecute(con, qry01_BirthdateCleaning) 

# [SELECT INTO] Create tmp_MoreThanOne_Birthdate from tmp_BirthDate

# ---- ----
qry02_BirthdateCleaning <- "
SELECT     ENCRYPTED_TRUE_PEN, COUNT(*) AS N_Birthdates
INTO        tmp_MoreThanOne_Birthdate
FROM        tmp_BirthDate
WHERE       PSI_BIRTHDATE NOT IN('',' ', '(Unspecified)')
  AND       ENCRYPTED_TRUE_PEN NOT IN('',' ', '(Unspecified)')
GROUP BY    ENCRYPTED_TRUE_PEN
HAVING      COUNT(*) > 1"
dbExecute(con, qry02_BirthdateCleaning)

# [SELECT INTO] Create tmp_MinPSIBirthdate from tmp_BirthDate

# ---- ----
qry03_BirthdateCleaning <-
"SELECT     ENCRYPTED_TRUE_PEN, MIN(PSI_BIRTHDATE) AS MinPSIBirthdate
INTO        tmp_MinPSIBirthdate
FROM        tmp_BirthDate
WHERE       PSI_BIRTHDATE NOT IN('',' ', '(Unspecified)')
GROUP BY    ENCRYPTED_TRUE_PEN"
dbExecute(con, qry03_BirthdateCleaning)

# [SELECT INTO] Create tmp_MaxPSIBirthdate from tmp_BirthDate

# ---- ----
qry04_BirthdateCleaning <-"
SELECT     ENCRYPTED_TRUE_PEN, MAX(PSI_BIRTHDATE) AS MaxPSIBirthdate
INTO        tmp_MaxPSIBirthdate
FROM        tmp_BirthDate
WHERE       PSI_BIRTHDATE NOT IN('',' ', '(Unspecified)')
AND         ENCRYPTED_TRUE_PEN NOT IN('',' ', '(Unspecified)')
GROUP BY    ENCRYPTED_TRUE_PEN"
dbExecute(con, qry04_BirthdateCleaning)
dbExecute(con, "ALTER table tmp_MaxPSIBirthdate ADD NumBirthdateRecords INT NULL")
dbExecute(con, "ALTER table tmp_MinPSIBirthdate ADD NumBirthdateRecords INT NULL")

# [UPDATE] tmp_MinPSIBirthdate

# ---- ----
qry05_BirthdateCleaning <-
"UPDATE       tmp_MinPSIBirthdate
SET           NumBirthdateRecords = tmp_BirthDate.NumBirthdateRecords
FROM          tmp_MinPSIBirthdate
INNER JOIN    tmp_BirthDate
  ON          tmp_MinPSIBirthdate.ENCRYPTED_TRUE_PEN = tmp_BirthDate.ENCRYPTED_TRUE_PEN
  AND         tmp_MinPSIBirthdate.MinPSIBirthdate = tmp_BirthDate.PSI_BIRTHDATE"
dbExecute(con, qry05_BirthdateCleaning)

# [UPDATE] tmp_MaxPSIBirthdate

# ---- ----
qry06_BirthdateCleaning <-  "
UPDATE        tmp_MaxPSIBirthdate
SET           NumBirthdateRecords = tmp_BirthDate.NumBirthdateRecords
FROM          tmp_MaxPSIBirthdate
INNER JOIN    tmp_BirthDate
  ON          tmp_MaxPSIBirthdate.ENCRYPTED_TRUE_PEN = tmp_BirthDate.ENCRYPTED_TRUE_PEN
  AND         tmp_MaxPSIBirthdate.MaxPSIBirthdate = tmp_BirthDate.PSI_BIRTHDATE"
dbExecute(con, qry06_BirthdateCleaning)
dbExecute(con, "ALTER table tmp_MoreThanOne_Birthdate 
                ADD MinPSIBirthdate NVARCHAR(50) NULL,
                    NumMinBirthdateRecords INT NULL,
                    MaxPSIBirthdate NVARCHAR(50) NULL,
                    NumMaxBirthdateRecords INT NULL")

# [UPDATE] tmp_MoreThanOne_Birthdate

# ---- ----
qry07a_BirthdateCleaning <- "
UPDATE        tmp_MoreThanOne_Birthdate
SET           MinPSIBirthdate = tmp_MinPSIBirthdate.MinPSIBirthdate,
              NumMinBirthdateRecords = tmp_MinPSIBirthdate.NumBirthdateRecords
FROM          tmp_MoreThanOne_Birthdate
INNER JOIN    tmp_MinPSIBirthdate
  ON          tmp_MoreThanOne_Birthdate.ENCRYPTED_TRUE_PEN = tmp_MinPSIBirthdate.ENCRYPTED_TRUE_PEN"
dbExecute(con, qry07a_BirthdateCleaning)

# [UPDATE] tmp_MoreThanOne_Birthdate

# ---- ----
qry07b_BirthdateCleaning <- "
UPDATE        tmp_MoreThanOne_Birthdate
SET           MaxPSIBirthdate = tmp_MaxPSIBirthdate.MaxPSIBirthdate,
              NumMaxBirthdateRecords = tmp_MaxPSIBirthdate.NumBirthdateRecords
FROM          tmp_MoreThanOne_Birthdate
INNER JOIN    tmp_MaxPSIBirthdate
  ON          tmp_MoreThanOne_Birthdate.ENCRYPTED_TRUE_PEN = tmp_MaxPSIBirthdate.ENCRYPTED_TRUE_PEN"
dbExecute(con, qry07b_BirthdateCleaning)
dbExecute(con, "DROP TABLE tmp_MinPSIBirthdate")
dbExecute(con, "DROP TABLE tmp_MaxPSIBirthdate")

dbExecute(con, "ALTER table tmp_MoreThanOne_Birthdate 
                ADD LastSeenBirthdate NVARCHAR(50) NULL;")

# [UPDATE] tmp_MoreThanOne_Birthdate

# ---- ----
qry08_BirthdateCleaning <-
"UPDATE       tmp_MoreThanOne_Birthdate
SET           LastSeenBirthdate = STP_Enrolment.LAST_SEEN_BIRTHDATE
FROM          tmp_MoreThanOne_Birthdate
INNER JOIN    STP_Enrolment
ON            tmp_MoreThanOne_Birthdate.ENCRYPTED_TRUE_PEN = STP_Enrolment.ENCRYPTED_TRUE_PEN"
dbExecute(con, qry08_BirthdateCleaning)
dbExecute(con, "ALTER table tmp_MoreThanOne_Birthdate 
                ADD UseMaxOrMin_FINAL NVARCHAR(50) NULL;")

# [UPDATE] tmp_MoreThanOne_Birthdate

# ---- ----
qry09_BirthdateCleaning <-  "
UPDATE      tmp_MoreThanOne_Birthdate
SET         UseMaxOrMin_FINAL = CASE
              WHEN MaxPSIBirthdate = LastSeenBirthdate THEN 'MAX'
              WHEN NumMaxBirthdateRecords > NumMinBirthdateRecords THEN 'MAX'
              WHEN NumMaxBirthdateRecords < NumMinBirthdateRecords THEN 'MIN'
              ELSE 'MIN' END
FROM        tmp_MoreThanOne_Birthdate"
dbExecute(con, qry09_BirthdateCleaning)
dbExecute(con, "ALTER table tmp_MoreThanOne_Birthdate 
                ADD psi_birthdate_cleaned NVARCHAR(50) NULL;")

# [UPDATE] tmp_MoreThanOne_Birthdate

# ---- ----
qry10_BirthdateCleaning <-
"UPDATE       tmp_MoreThanOne_Birthdate
SET                psi_birthdate_cleaned = MinPSIBirthdate
WHERE        (USEMAXORMIN_FINAL = 'MIN')"
dbExecute(con, qry10_BirthdateCleaning)

# [UPDATE] tmp_MoreThanOne_Birthdate

# ---- ----
qry11_BirthdateCleaning <-
"UPDATE       tmp_MoreThanOne_Birthdate
SET                psi_birthdate_cleaned = MaxPSIBirthdate
WHERE        (USEMAXORMIN_FINAL = 'MAX')"
dbExecute(con, qry11_BirthdateCleaning)

dbExecute(con, "ALTER TABLE STP_Enrolment ADD psi_birthdate_cleaned NVARCHAR(50) NULL")

#Update STP Enrolment with birthdates for those EPENS which have > 1 birthdate records 

# [UPDATE] STP_Enrolment

# ---- ----
qry12_BirthdateCleaning <-
"UPDATE    STP_Enrolment
SET              psi_birthdate_cleaned = tmp_MoreThanOne_Birthdate.psi_birthdate_cleaned
FROM         STP_Enrolment INNER JOIN
                      tmp_MoreThanOne_Birthdate ON STP_Enrolment.ENCRYPTED_TRUE_PEN = tmp_MoreThanOne_Birthdate.ENCRYPTED_TRUE_PEN"
dbExecute(con, qry12_BirthdateCleaning)

# some records have a null PSI_BIRTHDATE, search for non-null PSI_BIRTHDATE for these EPENS

# [SELECT INTO] Create tmp_NullBirthdate from tmp_BirthDate
# ---- ----
qry13_BirthdateCleaning <-
"SELECT     ENCRYPTED_TRUE_PEN, PSI_BIRTHDATE
 INTO            tmp_NullBirthdate
 FROM         tmp_BirthDate
 GROUP BY ENCRYPTED_TRUE_PEN, PSI_BIRTHDATE
HAVING      (ENCRYPTED_TRUE_PEN NOT IN('',' ', '(Unspecified)')) AND (PSI_BIRTHDATE IN('', ' ', '(Unspecified)'))"
dbExecute(con, qry13_BirthdateCleaning)

# [SELECT INTO] Create tmp_NonNullBirthdate from tmp_BirthDate

# ---- ----
qry14_BirthdateCleaning <- "
 SELECT     ENCRYPTED_TRUE_PEN, PSI_BIRTHDATE
 INTO        tmp_NonNullBirthdate
 FROM       tmp_BirthDate
 GROUP BY ENCRYPTED_TRUE_PEN, PSI_BIRTHDATE
HAVING      (ENCRYPTED_TRUE_PEN NOT IN('', ' ', '(Unspecified)')) AND (PSI_BIRTHDATE NOT IN('',' ', '(Unspecified)'))"
dbExecute(con, qry14_BirthdateCleaning)

# [SELECT INTO] Create tmp_NullBirthdateCleaned from tmp_NonNullBirthdate

# ---- ----
qry15_BirthdateCleaning <-
"SELECT     tmp_NonNullBirthdate.ENCRYPTED_TRUE_PEN, tmp_NonNullBirthdate.PSI_BIRTHDATE
INTO        tmp_NullBirthdateCleaned
FROM        tmp_NonNullBirthdate
INNER JOIN  tmp_NullBirthdate
  ON tmp_NonNullBirthdate.ENCRYPTED_TRUE_PEN = tmp_NullBirthdate.ENCRYPTED_TRUE_PEN"
dbExecute(con, qry15_BirthdateCleaning)
dbExecute(con, "ALTER TABLE tmp_NullBirthdateCleaned ADD psi_birthdate_cleaned NVARCHAR(50) NULL")

# [UPDATE] tmp_NullBirthdateCleaned

# ---- ----
qry16_BirthdateCleaning <- "
UPDATE    tmp_NullBirthdateCleaned
SET       psi_birthdate_cleaned = tmp_MoreThanOne_Birthdate.psi_birthdate_cleaned
FROM      tmp_NullBirthdateCleaned
INNER JOIN tmp_MoreThanOne_Birthdate
ON tmp_NullBirthdateCleaned.ENCRYPTED_TRUE_PEN = tmp_MoreThanOne_Birthdate.ENCRYPTED_TRUE_PEN"
dbExecute(con, qry16_BirthdateCleaning)

# [UPDATE] tmp_NullBirthdateCleaned

# ---- ----
qry17_BirthdateCleaning <-
"UPDATE    tmp_NullBirthdateCleaned
SET              psi_birthdate_cleaned = PSI_BIRTHDATE
WHERE     (psi_birthdate_cleaned IS NULL)"
dbExecute(con, qry17_BirthdateCleaning)

# Update STP_Enrolment with birthdates found in non-null records 

# [UPDATE] STP_Enrolment

# ---- ----
qry18_BirthdateCleaning <-  "
UPDATE    STP_Enrolment
SET       psi_birthdate_cleaned = tmp_NullBirthdateCleaned.psi_birthdate_cleaned
FROM      STP_Enrolment
INNER JOIN  tmp_NullBirthdateCleaned
  ON STP_Enrolment.ENCRYPTED_TRUE_PEN = tmp_NullBirthdateCleaned.ENCRYPTED_TRUE_PEN
WHERE     (STP_Enrolment.psi_birthdate_cleaned IS NULL)
OR        (STP_Enrolment.psi_birthdate_cleaned IN('', ' ', '(Unspecified)'))"
dbExecute(con, qry18_BirthdateCleaning)

# [UPDATE] STP_Enrolment

# ---- ----
qry19_BirthdateCleaning <-
 "UPDATE    STP_Enrolment
 SET         psi_birthdate_cleaned = PSI_BIRTHDATE
WHERE       ((psi_birthdate_cleaned IS NULL) AND (NOT (PSI_BIRTHDATE IS NULL)))
OR          ((psi_birthdate_cleaned IN( '',' ', '(Unspecified)'))   AND (NOT (PSI_BIRTHDATE IS NULL)))
OR          ((psi_birthdate_cleaned IS NULL) AND (PSI_BIRTHDATE NOT IN( '',' ', '(Unspecified)')))
OR          ((psi_birthdate_cleaned IN( '',' ', '(Unspecified)'))   AND (PSI_BIRTHDATE NOT IN( '',' ', '(Unspecified)')))"
dbExecute(con, qry19_BirthdateCleaning)

# sanity check on psi_birthdate_cleaned - finish this and save report

# [SELECT INTO] Create tmp_TEST_multi_birthdate from STP_Enrolment


# ---- ----
qry20_BirthdateCleaning <-
"SELECT     PSI_STUDENT_NUMBER, PSI_BIRTHDATE, psi_birthdate_cleaned, PSI_CODE, COUNT(*) AS Expr1
INTO            tmp_TEST_multi_birthdate
FROM         STP_Enrolment
WHERE     (ENCRYPTED_TRUE_PEN IN( ' ', '(Unspecified)'))
GROUP BY PSI_STUDENT_NUMBER, PSI_BIRTHDATE, psi_birthdate_cleaned, PSI_CODE
HAVING      (PSI_BIRTHDATE NOT IN( '',' ', '(Unspecified)')) AND (PSI_STUDENT_NUMBER NOT IN( '',' ', '(Unspecified)')) AND (PSI_CODE NOT IN( '',' ', '(Unspecified)'))"
dbExecute(con, qry20_BirthdateCleaning)

# [SQL]

# ---- ----
qry21_BirthdateCleaning <-
"SELECT     PSI_STUDENT_NUMBER, PSI_CODE, COUNT(*) AS Expr1
FROM         tmp_TEST_multi_birthdate
GROUP BY    PSI_STUDENT_NUMBER, PSI_CODE
HAVING      (COUNT(*) > 1)"
dbGetQuery(con, qry21_BirthdateCleaning)

# ---- Clean Up and check tables to keep ----
dbExecute(con, "DROP TABLE tmp_BirthDate")
dbExecute(con, "DROP TABLE tmp_MoreThanOne_Birthdate")
dbExecute(con, "DROP TABLE tmp_NullBirthdate")
dbExecute(con, "DROP TABLE tmp_NonNullBirthdate")
dbExecute(con, "DROP TABLE tmp_NullBirthdateCleaned")

dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Enrolment_Record_Type"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Enrolment"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Enrolment_Valid"')))

dbDisconnect(con)


