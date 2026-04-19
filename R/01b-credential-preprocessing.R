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

# Workflow #2
# Credential Preprocessing
# Description:
# Relies on STP_Credential, STP_Enrolment_Record_Type, STP_Enrolment_Valid, STP_Enrolment data tables
# Creates STP_Credential_Record_Type table with record status classification

library(arrow)
library(tidyverse)
library(odbc)
library(DBI)

# ---- Configure LAN Paths and DB Connection -----
lan <- config::get("lan")
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


# [SQL]
# ---- Checks ----
qry00a_check_null_epens <- "SELECT COUNT (*) AS n_null_epens FROM STP_Credential
  WHERE STP_Credential.ENCRYPTED_TRUE_PEN IN ('', ' ', '(Unspecified)')
OR STP_Credential.ENCRYPTED_TRUE_PEN IS NULL;"
dbGetQuery(con, qry00a_check_null_epens)

# [SQL]

qry00b_check_unique_epens <- "
  SELECT COUNT (DISTINCT ENCRYPTED_TRUE_PEN) AS n_epens
  FROM STP_Credential"
dbGetQuery(con, qry00b_check_unique_epens)

# ---- Add primary key ----

# [ALTER TABLE] STP_Credential

qry00c_CreateIDinSTPCredential <- "
  ALTER TABLE STP_Credential
  ADD ID INT IDENTITY(1,1) NOT NULL"
dbExecute(con, qry00c_CreateIDinSTPCredential)

# [ALTER TABLE] STP_Credential

qry00d_SetPKeyinSTPCredential <- "
  ALTER TABLE STP_Credential
  ADD CONSTRAINT STP_Credential_PK_ID PRIMARY KEY (ID)"
dbExecute(con, qry00d_SetPKeyinSTPCredential)

# ---- Reformat yy-mm-dd to yyyy-mm-dd ----
# check date variable format here
dbGetQuery(con, "SELECT TOP 100 CREDENTIAL_AWARD_DATE, PSI_PROGRAM_EFFECTIVE_DATE FROM STP_Credential;")

# if in format yy-mm-dd then run the following queries to convert from yy-mm-dd to yyyy-mm-dd

# [SELECT INTO] Create tmp_ConvertDateFormatCredential from STP_Credential
qrydates_create_tmp_table <- "SELECT CREDENTIAL_AWARD_DATE
      ,PSI_PROGRAM_EFFECTIVE_DATE
      ,ID
INTO tmp_ConvertDateFormatCredential
FROM STP_Credential;"
dbExecute(con, qrydates_create_tmp_table)

# [ALTER TABLE] tmp_ConvertDateFormatCredential

qrydates_add_cols <- "
  ALTER TABLE tmp_ConvertDateFormatCredential
  ADD CREDENTIAL_AWARD_DATE_convert varchar(50),
      PSI_PROGRAM_EFFECTIVE_DATE_convert varchar(50);"
dbExecute(con, qrydates_add_cols)

# [UPDATE] Tmp_ConvertDateFormatCredential

qrydates_convert1 <- "UPDATE Tmp_ConvertDateFormatCredential SET Tmp_ConvertDateFormatCredential.CREDENTIAL_AWARD_DATE_convert = '20'+CREDENTIAL_AWARD_DATE
WHERE ((Left(CREDENTIAL_AWARD_DATE,2)<24));"
dbExecute(con, qrydates_convert1)

# [UPDATE] Tmp_ConvertDateFormatCredential

qrydates_convert2 <- "UPDATE Tmp_ConvertDateFormatCredential SET Tmp_ConvertDateFormatCredential.CREDENTIAL_AWARD_DATE_convert = '19'+CREDENTIAL_AWARD_DATE
WHERE ((Left(CREDENTIAL_AWARD_DATE,2)>23));"
dbExecute(con, qrydates_convert2)

# [UPDATE] Tmp_ConvertDateFormatCredential

qrydates_convert3 <- "UPDATE Tmp_ConvertDateFormatCredential SET Tmp_ConvertDateFormatCredential.CREDENTIAL_AWARD_DATE_convert = ''
WHERE ((Left(CREDENTIAL_AWARD_DATE,2)='  '));"
dbExecute(con, qrydates_convert3)

# [UPDATE] Tmp_ConvertDateFormatCredential

qrydates_convert4 <- "UPDATE Tmp_ConvertDateFormatCredential SET Tmp_ConvertDateFormatCredential.PSI_PROGRAM_EFFECTIVE_DATE_convert = '20'+PSI_PROGRAM_EFFECTIVE_DATE
WHERE ((Left(PSI_PROGRAM_EFFECTIVE_DATE,2)<24));"
dbExecute(con, qrydates_convert4)

# [UPDATE] Tmp_ConvertDateFormatCredential

qrydates_convert5 <- "UPDATE Tmp_ConvertDateFormatCredential SET Tmp_ConvertDateFormatCredential.PSI_PROGRAM_EFFECTIVE_DATE_convert = '19'+PSI_PROGRAM_EFFECTIVE_DATE
WHERE ((Left(PSI_PROGRAM_EFFECTIVE_DATE,2)>23));"
dbExecute(con, qrydates_convert5)

# [UPDATE] Tmp_ConvertDateFormatCredential

qrydates_convert6 <- "UPDATE Tmp_ConvertDateFormatCredential SET Tmp_ConvertDateFormatCredential.PSI_PROGRAM_EFFECTIVE_DATE_convert = ''
WHERE ((Left(PSI_PROGRAM_EFFECTIVE_DATE,2)='  '));"
dbExecute(con, qrydates_convert6)

# [UPDATE] STP_Credential

qrydates_update_stp_credential1 <-
"UPDATE STP_Credential
SET STP_Credential.CREDENTIAL_AWARD_DATE = Tmp_ConvertDateFormatCredential.CREDENTIAL_AWARD_DATE_convert
FROM Tmp_ConvertDateFormatCredential, STP_Credential
WHERE STP_Credential.ID = Tmp_ConvertDateFormatCredential.ID;"
dbExecute(con, qrydates_update_stp_credential1)

# [UPDATE] STP_Credential

qrydates_update_stp_credential2 <-
"UPDATE STP_Credential
SET STP_Credential.PSI_PROGRAM_EFFECTIVE_DATE = Tmp_ConvertDateFormatCredential.PSI_PROGRAM_EFFECTIVE_DATE_convert
FROM Tmp_ConvertDateFormatCredential, STP_Credential
WHERE STP_Credential.ID = Tmp_ConvertDateFormatCredential.ID;"
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

# [SQL]


# ---- qry01_ExtractAllID_into_STP_Credential_Record_Type ----
qry01_ExtractAllID_into_STP_Credential_Record_Type <-"
  CREATE TABLE [STP_Credential_Record_Type] (
  [ID] int NOT NULL,
  [ENCRYPTED_TRUE_PEN] varchar(50),
  [RecordStatus] smallint,
  [MinEnrolment] smallint,
  [FirstEnrolment] smallint);

  INSERT INTO STP_Credential_Record_Type (ID, ENCRYPTED_TRUE_PEN)
  SELECT STP_Credential.ID, STP_Credential.ENCRYPTED_TRUE_PEN
  FROM STP_Credential;"
dbExecute(con, qry01_ExtractAllID_into_STP_Credential_Record_Type)


# ---- Find records with Record_Status = 1  ----

# [SELECT INTO] Create tmp_tbl_qry02a_Cred_Record_With_PEN_or_STUID from STP_Credential


# ---- qry02a_Record_With_PEN_Or_STUID ----
qry02a_Record_With_PEN_Or_STUID <- "SELECT      id, PSI_STUDENT_NUMBER, PSI_CODE, ENCRYPTED_TRUE_PEN
INTO       tmp_tbl_qry02a_Cred_Record_With_PEN_or_STUID
FROM       STP_Credential
WHERE     (PSI_STUDENT_NUMBER NOT IN ('', ' ', '(Unspecified)')
AND        PSI_CODE NOT IN ('', ' ', '(Unspecified)'))
OR (ENCRYPTED_TRUE_PEN NOT IN ('', ' ', '(Unspecified)'));"
dbExecute(con, qry02a_Record_With_PEN_Or_STUID)

# [SELECT INTO] Create Drop_Cred_No_PEN_or_No_STUID from tmp_tbl_qry02a_Cred_Record_With_PEN_or_STUID


# ---- qry02b_Drop_No_PEN_Or_No_STUID ----
qry02b_Drop_No_PEN_Or_No_STUID <- "
SELECT STP_Credential.ID, STP_Credential.ENCRYPTED_TRUE_PEN, STP_Credential.PSI_CODE, STP_Credential.PSI_STUDENT_NUMBER
INTO Drop_Cred_No_PEN_or_No_STUID
FROM tmp_tbl_qry02a_Cred_Record_With_PEN_or_STUID
RIGHT JOIN STP_Credential
ON tmp_tbl_qry02a_Cred_Record_With_PEN_or_STUID.ID = STP_Credential.ID
WHERE (((tmp_tbl_qry02a_Cred_Record_With_PEN_or_STUID.ID) Is Null));"
dbExecute(con, qry02b_Drop_No_PEN_Or_No_STUID)

# [UPDATE] STP_Credential_Record_Type


# ---- qry02c_Update_Drop_No_PEN_or_No_STUID.SQL ----
qry02c_Update_Drop_No_PEN_or_No_STUID <- "
UPDATE    STP_Credential_Record_Type
SET       RecordStatus = 1
FROM      STP_Credential_Record_Type
INNER JOIN Drop_Cred_No_PEN_or_No_STUID
ON STP_Credential_Record_Type.ID = Drop_Cred_No_PEN_or_No_STUID.ID;"
dbExecute(con, qry02c_Update_Drop_No_PEN_or_No_STUID)
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[tmp_tbl_qry02a_Cred_Record_With_PEN_or_STUID];"))
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[Drop_Cred_No_PEN_or_No_STUID];")) 

# ---- Find records with Record_Status = 2  ----

# [SELECT INTO] Create Drop_Cred_Developmental from STP_Credential


# ---- qry03a_Drop_Record_Developmental ----
qry03a_Drop_Record_Developmental <- "
SELECT    ID, ENCRYPTED_TRUE_PEN,  PSI_CODE, PSI_STUDENT_NUMBER, PSI_CREDENTIAL_CATEGORY, LEFT(STP_CREDENTIAL.PSI_CREDENTIAL_CIP, 2) AS CIP2, PSI_CREDENTIAL_LEVEL
INTO      Drop_Cred_Developmental
FROM      STP_Credential
WHERE     PSI_CREDENTIAL_LEVEL = 'DEVELOPMENTAL';"
dbExecute(con, qry03a_Drop_Record_Developmental)

# [UPDATE] STP_Credential_Record_Type


# ---- qry03b_Update_Drop_Record_Developmental ----
qry03b_Update_Drop_Record_Developmental <- "
UPDATE    STP_Credential_Record_Type
SET       RecordStatus = 2
FROM      STP_Credential_Record_Type
INNER JOIN Drop_Cred_Developmental
  ON STP_Credential_Record_Type.ID = Drop_Cred_Developmental.ID
WHERE STP_Credential_Record_Type.RecordStatus is null;"
dbExecute(con, qry03b_Update_Drop_Record_Developmental)
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[Drop_Cred_Developmental];")) 

# ---- Find records with Record_Status = 6  ----

# [SELECT INTO] Create tmp_tbl_EnrolmentSkillsBasedCourses from STP_Enrolment_Record_Type

# ---- qry03c_create_table_EnrolmentSkillsBasedCourse ----
qry03c_create_table_EnrolmentSkillsBasedCourse <- "
SELECT    STP_Enrolment.PSI_CODE, STP_Enrolment.PSI_PROGRAM_CODE, STP_Enrolment.PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
          LEFT(STP_Enrolment.PSI_CIP_CODE, 2) AS CIP2, STP_Enrolment.PSI_CREDENTIAL_CATEGORY,
          STP_Enrolment.PSI_STUDY_LEVEL, STP_Enrolment.PSI_CONTINUING_EDUCATION_COURSE_ONLY, COUNT(*) AS Count
INTO      tmp_tbl_EnrolmentSkillsBasedCourses
FROM      STP_Enrolment_Record_Type
INNER JOIN STP_Enrolment
  ON STP_Enrolment_Record_Type.ID = STP_Enrolment.ID
WHERE     (STP_Enrolment_Record_Type.RecordStatus = 6)
GROUP BY STP_Enrolment.PSI_CODE, STP_Enrolment.PSI_PROGRAM_CODE, STP_Enrolment.PSI_CREDENTIAL_PROGRAM_DESCRIPTION, LEFT(STP_Enrolment.PSI_CIP_CODE, 2),
                      STP_Enrolment.PSI_CREDENTIAL_CATEGORY, STP_Enrolment.PSI_STUDY_LEVEL, STP_Enrolment.PSI_CONTINUING_EDUCATION_COURSE_ONLY;"
dbExecute(con, qry03c_create_table_EnrolmentSkillsBasedCourse)

# [SELECT INTO] Create tmp_tbl_Cred_Suspect_Skills_Based from STP_Credential_Record_Type


# ---- qry03d_create_table_Suspect_Skills_Based ----
qry03d_create_table_Suspect_Skills_Based <- "
SELECT    STP_Credential.ID, STP_Credential.ENCRYPTED_TRUE_PEN, STP_Credential.PSI_STUDENT_NUMBER, STP_Credential.PSI_SCHOOL_YEAR,
          STP_Credential.PSI_CODE, STP_Credential.PSI_CREDENTIAL_PROGRAM_DESCRIPTION, LEFT(STP_Credential.PSI_CREDENTIAL_CIP,2) AS CIP2, STP_Credential.PSI_CREDENTIAL_CATEGORY,
          STP_Credential.PSI_CREDENTIAL_LEVEL
INTO      tmp_tbl_Cred_Suspect_Skills_Based
FROM      STP_Credential_Record_Type
INNER JOIN STP_Credential
  ON STP_Credential_Record_Type.ID = STP_Credential.ID
WHERE    (STP_Credential_Record_Type.RecordStatus IS NULL);"
dbExecute(con, qry03d_create_table_Suspect_Skills_Based)

# [SELECT INTO] Create Cred_Suspect_Skills_Based from tmp_tbl_Cred_Suspect_Skills_Based

# ---- qry03e_Find_Suspect_Skills_Based ----
qry03e_Find_Suspect_Skills_Based <- "
SELECT    tmp_tbl_Cred_Suspect_Skills_Based.ID, tmp_tbl_Cred_Suspect_Skills_Based.ENCRYPTED_TRUE_PEN, tmp_tbl_Cred_Suspect_Skills_Based.PSI_CODE,  tmp_tbl_Cred_Suspect_Skills_Based.PSI_STUDENT_NUMBER,
          tmp_tbl_Cred_Suspect_Skills_Based.PSI_CREDENTIAL_PROGRAM_DESCRIPTION, tmp_tbl_Cred_Suspect_Skills_Based.CIP2,
          tmp_tbl_Cred_Suspect_Skills_Based.PSI_CREDENTIAL_CATEGORY, tmp_tbl_Cred_Suspect_Skills_Based.PSI_CREDENTIAL_LEVEL
INTO      Cred_Suspect_Skills_Based
FROM      tmp_tbl_Cred_Suspect_Skills_Based
INNER JOIN tmp_tbl_EnrolmentSkillsBasedCourses
  ON tmp_tbl_Cred_Suspect_Skills_Based.PSI_CODE = tmp_tbl_EnrolmentSkillsBasedCourses.PSI_CODE
  AND tmp_tbl_Cred_Suspect_Skills_Based.PSI_CREDENTIAL_PROGRAM_DESCRIPTION = tmp_tbl_EnrolmentSkillsBasedCourses.PSI_CREDENTIAL_PROGRAM_DESCRIPTION
  AND tmp_tbl_Cred_Suspect_Skills_Based.CIP2 = tmp_tbl_EnrolmentSkillsBasedCourses.CIP2
  AND tmp_tbl_Cred_Suspect_Skills_Based.PSI_CREDENTIAL_CATEGORY = tmp_tbl_EnrolmentSkillsBasedCourses.PSI_CREDENTIAL_CATEGORY
  AND tmp_tbl_Cred_Suspect_Skills_Based.PSI_CREDENTIAL_LEVEL = tmp_tbl_EnrolmentSkillsBasedCourses.PSI_STUDY_LEVEL;"
dbExecute(con, qry03e_Find_Suspect_Skills_Based)

# [UPDATE] STP_Credential_Record_Type


# ---- qry03f_Update_Suspect_Skills_Based ----
qry03f_Update_Suspect_Skills_Based <- "
UPDATE    STP_Credential_Record_Type
SET       RecordStatus = 6
FROM      Cred_Suspect_Skills_Based
INNER JOIN STP_Credential_Record_Type
  ON Cred_Suspect_Skills_Based.ID = STP_Credential_Record_Type.ID
WHERE STP_Credential_Record_Type.RecordStatus IS NULL;"
dbExecute(con, qry03f_Update_Suspect_Skills_Based)
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[tmp_tbl_Cred_Suspect_Skills_Based];")) 
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[Cred_Suspect_Skills_Based];")) 
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[tmp_tbl_EnrolmentSkillsBasedCourses];")) 

# ---- Find records with Record_Status = 7 and update look up table ----

# [SELECT INTO] Create Drop_Developmental_PSI_CREDENTIAL_CIPS from STP_Credential

# ---- qry03g_Drop_Developmental_Credential_CIPS ----
qry03g_Drop_Developmental_Credential_CIPS <- "
SELECT    STP_Credential.ID, STP_Credential.ENCRYPTED_TRUE_PEN, STP_Credential.PSI_CODE, STP_Credential.PSI_STUDENT_NUMBER, STP_Credential.PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
          LEFT(STP_Credential.PSI_CREDENTIAL_CIP, 2) AS CIP2, STP_Credential.PSI_CREDENTIAL_CATEGORY, STP_Credential_Record_Type.RecordStatus
INTO      Drop_Developmental_PSI_CREDENTIAL_CIPS
FROM      STP_Credential
INNER JOIN STP_Credential_Record_Type
  ON STP_Credential.ID = STP_Credential_Record_Type.ID
WHERE     (LEFT(STP_Credential.PSI_CREDENTIAL_CIP, 2) IN ('21', '32', '33', '34', '35', '36', '37', '53', '89'))
  AND (STP_Credential_Record_Type.RecordStatus IS NULL);"
dbExecute(con, qry03g_Drop_Developmental_Credential_CIPS)
dbExecute(con, "ALTER TABLE Drop_Developmental_PSI_CREDENTIAL_CIPS ADD Keep NVARCHAR(2)")

###  ---- ** Manual **  ----
# Check against the outcomes programs table to see if some are non-developmental CIP. If so, set keep = 'Y'.
data <- dbReadTable(con, "Drop_Developmental_PSI_CREDENTIAL_CIPS", col_types = cols(.default = col_character()))
data.entry(data)
dbWriteTable(con, name = "Drop_Developmental_PSI_CREDENTIAL_CIPS", as.data.frame(data), overwrite = TRUE)


# [UPDATE] STP_Credential_Record_Type

# ---- qry03h_Update_Developmental_CIPs ----
qry03h_Update_Developmental_CIPs <- "
UPDATE    STP_Credential_Record_Type
SET       RecordStatus = 7
FROM      STP_Credential_Record_Type
INNER JOIN Drop_Developmental_PSI_CREDENTIAL_CIPS
  ON STP_Credential_Record_Type.ID = Drop_Developmental_PSI_CREDENTIAL_CIPS.ID
WHERE STP_Credential_Record_Type.RecordStatus IS NULL and Drop_Developmental_PSI_CREDENTIAL_CIPS.Keep IS Null;"
dbExecute(con, qry03h_Update_Developmental_CIPs)
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[Drop_Developmental_PSI_CREDENTIAL_CIPS];")) 

# ---- Find records with Record_Status = 8 and update look up table ----

# [SELECT INTO] Create Drop_Cred_RecommendForCert from STP_Credential


# ---- qry03i_Drop_RecommendationForCert ----
qry03i_Drop_RecommendationForCert <- "
SELECT      STP_Credential.ID, STP_Credential.ENCRYPTED_TRUE_PEN, STP_Credential.PSI_CODE, STP_Credential.PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
                         LEFT(STP_Credential.PSI_CREDENTIAL_CIP, 2) AS CIP2, STP_Credential.PSI_CREDENTIAL_CATEGORY, STP_Credential_Record_Type.RecordStatus
INTO        Drop_Cred_RecommendForCert
FROM        STP_Credential
INNER JOIN STP_Credential_Record_Type
  ON STP_Credential.ID = STP_Credential_Record_Type.ID
WHERE        (PSI_CREDENTIAL_CATEGORY = 'RECOMMENDATION FOR CERTIFICATION') AND (STP_Credential_Record_Type.RecordStatus IS NULL);"
dbExecute(con, qry03i_Drop_RecommendationForCert)

# [UPDATE] STP_Credential_Record_Type


# ---- qry03j_Update_RecommendationForCert  ----
qry03j_Update_RecommendationForCert  <- "
UPDATE    STP_Credential_Record_Type
SET       RecordStatus = 8
FROM      STP_Credential_Record_Type
INNER JOIN Drop_Cred_RecommendForCert
  ON STP_Credential_Record_Type.ID = Drop_Cred_RecommendForCert.ID
WHERE STP_Credential_Record_Type.RecordStatus IS NULL;"
dbExecute(con, qry03j_Update_RecommendationForCert)
dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[Drop_Cred_RecommendForCert];")) 


# [UPDATE] STP_Credential_Record_Type


# ---- qry04_Update_RecordStatus_Not_Dropped ----
qry04_Update_RecordStatus_Not_Dropped <- "
UPDATE STP_Credential_Record_Type
SET STP_Credential_Record_Type.RecordStatus = 0
WHERE (((STP_Credential_Record_Type.RecordStatus) Is Null));;"
dbExecute(con, qry04_Update_RecordStatus_Not_Dropped)

# [SQL]

# ---- RecordTypeSummary ----
RecordTypeSummary <-
  "SELECT RecordStatus, COUNT(*) AS Expr1
FROM  STP_Credential_Record_Type
GROUP BY RecordStatus"
dbGetQuery(con, RecordTypeSummary)

# ---- Clean Up and check tables to keep ----
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Credential"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Credential_Record_Type"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Enrolment_Record_Type"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Enrolment"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Enrolment_Valid"')))

dbDisconnect(con)

