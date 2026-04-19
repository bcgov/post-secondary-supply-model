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

# Workflow #3 (noting here for now)
# Credential Analysis
# Description: 
# Relies on STP_Enrolment, STP_Credential, STP_Credential_Record_Type, STP_Enrolment_Valid
# Lookups OutcomeCredential, AgeGroup
# Creates table to be used for grad projections:
#   qry20a_4Credential_By_Year_CIP4_Gender_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs
# Notes: Line 100 # flag records on CREDENTIAL_AWARD_DATE >= '2019-09-01'.  Change to 2023-09-01 for 2023 run.

library(arrow)
library(tidyverse)
library(odbc)
library(DBI)

# ---- Configure LAN Paths and DB Connection -----
lan <- config::get("lan")


# ---- qry03fCredential_SupVarsGenderCleaning1 ----
qry03fCredential_SupVarsGenderCleaning1 <-
  "SELECT [ENCRYPTED_TRUE_PEN], COUNT(*) AS GenderCount
INTO CredentialSupVars_MultiGenderCounter
FROM [CredentialSupVars_Gender]
GROUP BY [ENCRYPTED_TRUE_PEN];"

# ---- qry03fCredential_SupVarsGenderCleaning2 ----
qry03fCredential_SupVarsGenderCleaning2<-
  "SELECT [ENCRYPTED_TRUE_PEN]
INTO CredentialSupVars_MultiGender
FROM [CredentialSupVars_MultiGenderCounter]
WHERE [GenderCount]>1
GROUP BY [ENCRYPTED_TRUE_PEN];"


# --------------------------------------------------------------------------------------------------------------------------

"SELECT   CredentialSupVarsFromEnrolment_MultiGender.ENCRYPTED_TRUE_PEN, CredentialSupVarsFromEnrolment_MultiGender.PSI_GENDER,
          MAX(CredentialSupVarsFromEnrolment_MultiGender.PSI_SCHOOL_YEAR) AS MAX_PSI_SCHOOL_YEAR
INTO      tmp_CredentialGenderCleaning_Step1
FROM      CredentialSupVarsFromEnrolment_MultiGender
GROUP BY  CredentialSupVarsFromEnrolment_MultiGender.ENCRYPTED_TRUE_PEN, CredentialSupVarsFromEnrolment_MultiGender.PSI_GENDER
HAVING    (CredentialSupVarsFromEnrolment_MultiGender.PSI_GENDER IS NOT NULL AND CredentialSupVarsFromEnrolment_MultiGender.PSI_GENDER <> ' ')"


#For updating psi_gender_cleaned_for_records_with_nullEPEN or unmatched EPEN, but matched on PSI_STUDENT_NUMBER/PSI_CODE
"UPDATE       CredentialSupVars
SET                psi_gender_cleaned = tmp_CredentialGenderCleaning_Step10.psi_gender_cleaned
FROM            CredentialSupVars INNER JOIN
                         tmp_CredentialGenderCleaning_Step10 ON CredentialSupVars.ENCRYPTED_TRUE_PEN = tmp_CredentialGenderCleaning_Step10.ENCRYPTED_TRUE_PEN
WHERE        (CredentialSupVars.psi_gender_cleaned IS NULL) AND (CredentialSupVars.ENCRYPTED_TRUE_PEN IS NOT NULL AND
                         CredentialSupVars.ENCRYPTED_TRUE_PEN NOT IN ('',' ','(Unspecified)'))"


"UPDATE       CredentialSupVars
SET                psi_gender_cleaned = tmp_CredentialGenderCleaning_Step10.psi_gender_cleaned
FROM            CredentialSupVars INNER JOIN
                         tmp_CredentialGenderCleaning_Step10 ON CredentialSupVars.PSI_STUDENT_NUMBER = tmp_CredentialGenderCleaning_Step10.PSI_STUDENT_NUMBER AND
                         CredentialSupVars.PSI_CODE = tmp_CredentialGenderCleaning_Step10.PSI_CODE
WHERE        (CredentialSupVars.psi_gender_cleaned IS NULL) OR
                         (CredentialSupVars.psi_gender_cleaned = '')"

db_config <- config::get("decimal")
my_schema <- config::get("myschema")

con <- dbConnect(odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")

# ---- Check Required Tables ----
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Enrolment"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Credential"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Enrolment_Record_Type"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Credential_Record_Type"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Enrolment_Valid"')))

# Lookup
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."OutcomeCredential"'))) 
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."AgeGroupLookup"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."CredentialRank"')))

# ---- Create a view with STP_Credential data with record_type == 0 and a non-blank award date ----

# [SQL]

# ---- qry_Credential_view_initial ---- 
qry_Credential_view_initial <- "
CREATE VIEW Credential 
AS
SELECT        STP_Credential.ID,STP_Credential.ENCRYPTED_TRUE_PEN, 
              STP_Credential.PSI_SCHOOL_YEAR, 
              STP_Credential.PSI_STUDENT_NUMBER, 
              STP_Credential.PSI_CODE, 
              STP_Credential.CREDENTIAL_AWARD_DATE, 
              STP_Credential_Record_Type.RecordStatus, 
              STP_Credential.PSI_PROGRAM_CODE, 
              STP_Credential.PSI_CREDENTIAL_PROGRAM_DESCRIPTION, 
              STP_Credential.PSI_CREDENTIAL_CIP, 
              STP_Credential.PSI_CREDENTIAL_LEVEL, 
              STP_Credential.PSI_CREDENTIAL_CATEGORY
FROM          STP_Credential 
INNER JOIN    STP_Credential_Record_Type 
  ON          STP_Credential.ID = STP_Credential_Record_Type.ID
WHERE        (STP_Credential.CREDENTIAL_AWARD_DATE NOT IN ('', ' ', '(Unspecified)')) 
  AND (STP_Credential_Record_Type.RecordStatus = 0);"
dbExecute(con, qry_Credential_view_initial) 

# ---- Make Credential Sup Vars Enrolment ----
# Create a list of EPENs/max school year/enrolment ID's from the Enrolment_valid table 

# [SELECT INTO] Create tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step1 from STP_Enrolment_Valid
# ---- qry01_CredentialSupVars_From_Enrolment ----
qry01_CredentialSupVars_From_Enrolment <- "
SELECT     ENCRYPTED_TRUE_PEN, MAX(PSI_SCHOOL_YEAR) AS MaxSchoolYear
INTO       tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step1
FROM       STP_Enrolment_Valid
GROUP BY   ENCRYPTED_TRUE_PEN
HAVING     (ENCRYPTED_TRUE_PEN IS NOT NULL) AND (ENCRYPTED_TRUE_PEN NOT IN ('', ' ', '(Unspecified)'))"
dbExecute(con, qry01_CredentialSupVars_From_Enrolment)  

# [SELECT INTO] Create tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step2 from tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step1

# ---- qry02_CredentialSupVars_From_Enrolment ----
qry02_CredentialSupVars_From_Enrolment <- "
SELECT      STP_Enrolment_Valid.ID, STP_Enrolment_Valid.ENCRYPTED_TRUE_PEN, STP_Enrolment_Valid.PSI_STUDENT_NUMBER,
            STP_Enrolment_Valid.PSI_CODE, tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step1.MaxSchoolYear
INTO        tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step2
FROM        tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step1
INNER JOIN  STP_Enrolment_Valid
  ON        tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step1.ENCRYPTED_TRUE_PEN = STP_Enrolment_Valid.ENCRYPTED_TRUE_PEN
  AND       tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step1.MaxSchoolYear = STP_Enrolment_Valid.PSI_SCHOOL_YEAR
GROUP BY    STP_Enrolment_Valid.ID, STP_Enrolment_Valid.ENCRYPTED_TRUE_PEN, STP_Enrolment_Valid.PSI_STUDENT_NUMBER,
            STP_Enrolment_Valid.PSI_CODE, tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step1.MaxSchoolYear"
dbExecute(con, qry02_CredentialSupVars_From_Enrolment)  

# [ALTER TABLE] tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step2

# ---- qry03_CredentialSupVars_From_Enrolment ----
qry03_CredentialSupVars_From_Enrolment <- "
ALTER TABLE tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step2
ADD CONSTRAINT tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step2_PK_ID
PRIMARY KEY (ID);"
dbExecute(con, qry03_CredentialSupVars_From_Enrolment)  

# [SELECT INTO] Create tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step3 from tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step2

# ---- qry04_CredentialSupVars_From_Enrolment ----
qry04_CredentialSupVars_From_Enrolment <- "
SELECT  STP_Enrolment_Valid.ID, STP_Enrolment_Valid.PSI_STUDENT_NUMBER, STP_Enrolment_Valid.ENCRYPTED_TRUE_PEN,
        STP_Enrolment_Valid.PSI_SCHOOL_YEAR, STP_Enrolment_Valid.PSI_STUDENT_POSTAL_CODE_CURRENT, STP_Enrolment_Valid.PSI_ENROLMENT_SEQUENCE,
        STP_Enrolment_Valid.PSI_CODE, STP_Enrolment_Valid.PSI_MIN_START_DATE
INTO    tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step3
FROM    tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step2
INNER JOIN STP_Enrolment_Valid
  ON tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step2.ID = STP_Enrolment_Valid.ID"
dbExecute(con, qry04_CredentialSupVars_From_Enrolment)  

# [ALTER TABLE] tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step3

# ---- qry05_CredentialSupVars_From_Enrolment ----
qry05_CredentialSupVars_From_Enrolment <- "
ALTER TABLE tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step3
ADD CONSTRAINT tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step3_PK_ID
PRIMARY KEY (ID);"
dbExecute(con, qry05_CredentialSupVars_From_Enrolment) 

# [SELECT INTO] Create CredentialSupVarsFromEnrolment from tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step3

# ---- qry06_CredentialSupVars_From_Enrolment ----
qry06_CredentialSupVars_From_Enrolment <- "
SELECT        tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step3.ID AS EnrolmentID,
              tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step3.ENCRYPTED_TRUE_PEN,
              tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step3.PSI_MIN_START_DATE,
              Credential.RecordStatus AS CredentialRecordStatus,
              tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step3.PSI_STUDENT_POSTAL_CODE_CURRENT,
              tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step3.PSI_SCHOOL_YEAR,
              tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step3.PSI_CODE,
              tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step3.PSI_STUDENT_NUMBER,
              tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step3.PSI_ENROLMENT_SEQUENCE
INTO          CredentialSupVarsFromEnrolment
FROM          tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step3
INNER JOIN    Credential
  ON          tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step3.ENCRYPTED_TRUE_PEN = Credential.ENCRYPTED_TRUE_PEN
GROUP BY      tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step3.ID,
              tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step3.ENCRYPTED_TRUE_PEN,
              tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step3.PSI_MIN_START_DATE,
              Credential.RecordStatus,
              tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step3.PSI_STUDENT_POSTAL_CODE_CURRENT,
              tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step3.PSI_SCHOOL_YEAR,
              tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step3.PSI_CODE,
              tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step3.PSI_STUDENT_NUMBER,
              tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step3.PSI_ENROLMENT_SEQUENCE"
dbExecute(con, qry06_CredentialSupVars_From_Enrolment)  
dbExecute(con, "ALTER TABLE CredentialSupVarsFromEnrolment ADD CONSTRAINT PK_CredSupVarsfromEnrol_ID PRIMARY KEY (EnrolmentID);")

dbExecute(con, "SELECT PSI_CODE, PSI_STUDENT_NUMBER 
                INTO RW_TEST_CRED_EPENS_NOT_MATCHED_ID_PSICODE 
                from Credential
                WHERE ENCRYPTED_TRUE_PEN NOT IN (
	              SELECT ENCRYPTED_TRUE_PEN
	              FROM CredentialSupVarsFromEnrolment);")
dbExecute(con, "SELECT ID, PSI_CODE, PSI_STUDENT_NUMBER 
                INTO RW_TEST_CRED_NULLEPENS_TO_MATCH 
                FROM Credential 
                WHERE ENCRYPTED_TRUE_PEN = '';")


# [SELECT INTO] Create tmp_tbl_Enrol_ID_EPEN_for_Cred_Join_step4 from tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step3


# ---- qry07_CredentialSupVars_From_Enrolment ----
qry07_CredentialSupVars_From_Enrolment <- "
SELECT          tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step3.ID,
                tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step3.PSI_STUDENT_NUMBER,
                tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step3.ENCRYPTED_TRUE_PEN,
                tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step3.PSI_SCHOOL_YEAR,
                tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step3.PSI_STUDENT_POSTAL_CODE_CURRENT,
                tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step3.PSI_ENROLMENT_SEQUENCE,
                tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step3.PSI_CODE,
                tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step3.PSI_MIN_START_DATE
INTO            tmp_tbl_Enrol_ID_EPEN_for_Cred_Join_step4
FROM            tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step3
INNER JOIN      RW_TEST_CRED_EPENS_NOT_MATCHED_ID_PSICODE
  ON            tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step3.PSI_STUDENT_NUMBER = RW_TEST_CRED_EPENS_NOT_MATCHED_ID_PSICODE.PSI_STUDENT_NUMBER
  AND           tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step3.PSI_CODE = RW_TEST_CRED_EPENS_NOT_MATCHED_ID_PSICODE.PSI_CODE"
#dbExecute(con, qry07_CredentialSupVars_From_Enrolment) 

# [SELECT INTO] Create RW_TEST_CRED_NULLEPENS_MATCHED from tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step3


# ---- qry08_CredentialSupVars_From_Enrolment ----
qry08_CredentialSupVars_From_Enrolment <- "
SELECT        tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step3.ID AS EnrolmentID,
              tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step3.PSI_STUDENT_NUMBER,
              tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step3.ENCRYPTED_TRUE_PEN AS EnrolEPEN,
              tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step3.PSI_SCHOOL_YEAR,
              tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step3.PSI_STUDENT_POSTAL_CODE_CURRENT,
              tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step3.PSI_ENROLMENT_SEQUENCE,
              tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step3.PSI_CODE,
              tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step3.PSI_MIN_START_DATE,
              RW_TEST_CRED_NULLEPENS_TO_MATCH.ID AS CredentialID
INTO          RW_TEST_CRED_NULLEPENS_MATCHED
FROM          tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step3
INNER JOIN    RW_TEST_CRED_NULLEPENS_TO_MATCH
  ON          tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step3.PSI_STUDENT_NUMBER = RW_TEST_CRED_NULLEPENS_TO_MATCH.PSI_STUDENT_NUMBER
  AND         tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step3.PSI_CODE = RW_TEST_CRED_NULLEPENS_TO_MATCH.PSI_CODE"
dbExecute(con, qry08_CredentialSupVars_From_Enrolment) 

# [SELECT INTO] Create tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step4 from STP_Enrolment_Valid

# ---- qry09_CredentialSupVars_From_Enrolment ----
qry09_CredentialSupVars_From_Enrolment <- "
SELECT     ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, MAX(PSI_SCHOOL_YEAR) AS MaxSchoolYear
INTO       tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step4
FROM       STP_Enrolment_Valid
GROUP BY   ENCRYPTED_TRUE_PEN, PSI_CODE, PSI_STUDENT_NUMBER
HAVING     (ENCRYPTED_TRUE_PEN IS NULL) OR (ENCRYPTED_TRUE_PEN IN ('', ' ', '(Unspecified)'));"
dbExecute(con, qry09_CredentialSupVars_From_Enrolment) 

# [SELECT INTO] Create tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step5 from tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step4

# ---- qry10_CredentialSupVars_From_Enrolment ----
qry10_CredentialSupVars_From_Enrolment <- "
SELECT      STP_Enrolment_Valid.ID,
            STP_Enrolment_Valid.ENCRYPTED_TRUE_PEN,
            STP_Enrolment_Valid.PSI_STUDENT_NUMBER,
            STP_Enrolment_Valid.PSI_CODE,
            tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step4.MaxSchoolYear
INTO        tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step5
FROM        tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step4
INNER JOIN  STP_Enrolment_Valid
  ON        tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step4.PSI_CODE = STP_Enrolment_Valid.PSI_CODE
	AND       tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step4.MaxSchoolYear = STP_Enrolment_Valid.PSI_SCHOOL_YEAR
	AND       tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step4.PSI_STUDENT_NUMBER = STP_Enrolment_Valid.PSI_STUDENT_NUMBER
GROUP BY    STP_Enrolment_Valid.ID,
            STP_Enrolment_Valid.ENCRYPTED_TRUE_PEN,
            STP_Enrolment_Valid.PSI_STUDENT_NUMBER,
            STP_Enrolment_Valid.PSI_CODE,
            tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step4.MaxSchoolYear;"
dbExecute(con, qry10_CredentialSupVars_From_Enrolment) 

# [ALTER TABLE] table

# ---- qry11_CredentialSupVars_From_Enrolment ----
qry11_CredentialSupVars_From_Enrolment <- "
ALTER TABLE [tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step5]
ADD CONSTRAINT tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step5_PK_ID
PRIMARY KEY (ID);"
dbExecute(con, qry11_CredentialSupVars_From_Enrolment) 

# [SELECT INTO] Create tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step6 from tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step5

# ---- qry12_CredentialSupVars_From_Enrolment ----
qry12_CredentialSupVars_From_Enrolment <- "
SELECT        STP_Enrolment_Valid.ID,
              STP_Enrolment_Valid.PSI_STUDENT_NUMBER,
              STP_Enrolment_Valid.ENCRYPTED_TRUE_PEN,
              STP_Enrolment_Valid.PSI_SCHOOL_YEAR,
              STP_Enrolment_Valid.PSI_STUDENT_POSTAL_CODE_CURRENT,
              STP_Enrolment_Valid.PSI_ENROLMENT_SEQUENCE,
              STP_Enrolment_Valid.PSI_CODE,
              STP_Enrolment_Valid.PSI_MIN_START_DATE
INTO          tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step6
FROM          tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step5
INNER JOIN    STP_Enrolment_Valid
  ON          tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step5.ID = STP_Enrolment_Valid.ID;"
dbExecute(con, qry12_CredentialSupVars_From_Enrolment) 

# [ALTER TABLE] table

# ---- qry12b_CredentialSupVars_From_Enrolment ----
qry12b_CredentialSupVars_From_Enrolment <- "
ALTER TABLE [tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step6]
ADD CONSTRAINT tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step6_PK_ID
PRIMARY KEY (ID);"
dbExecute(con, qry12b_CredentialSupVars_From_Enrolment)

# [INSERT INTO] CredentialSupVarsFromEnrolment

# ---- qry13_CredentialSupVars_From_Enrolment ----
qry13_CredentialSupVars_From_Enrolment <- "
INSERT INTO CredentialSupVarsFromEnrolment
SELECT        tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step6.ID AS EnrolmentID,
					      tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step6.ENCRYPTED_TRUE_PEN,
					      tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step6.PSI_MIN_START_DATE,
              Credential.RecordStatus AS CredentialRecordStatus,
					      tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step6.PSI_STUDENT_POSTAL_CODE_CURRENT,
					      tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step6.PSI_SCHOOL_YEAR,
              tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step6.PSI_CODE,
              tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step6.PSI_STUDENT_NUMBER,
              tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step6.PSI_ENROLMENT_SEQUENCE
FROM          tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step6
INNER JOIN    Credential
  ON          tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step6.PSI_CODE = Credential.PSI_CODE
	AND         tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step6.PSI_STUDENT_NUMBER = Credential.PSI_STUDENT_NUMBER
GROUP BY      tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step6.ID,
              tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step6.ENCRYPTED_TRUE_PEN,
              tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step6.PSI_MIN_START_DATE, Credential.RecordStatus,
              tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step6.PSI_STUDENT_POSTAL_CODE_CURRENT,
              tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step6.PSI_SCHOOL_YEAR,
						  tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step6.PSI_CODE,
              tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step6.PSI_STUDENT_NUMBER,
              tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step6.PSI_ENROLMENT_SEQUENCE"
dbExecute(con, qry13_CredentialSupVars_From_Enrolment) 

dbExecute(con, "DROP TABLE tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step1")
dbExecute(con, "DROP TABLE tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step2")
dbExecute(con, "DROP TABLE tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step3")
dbExecute(con, "DROP TABLE tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step4")
dbExecute(con, "DROP TABLE tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step5")
dbExecute(con, "DROP TABLE tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step6")
dbExecute(con, "DROP TABLE RW_TEST_CRED_NULLEPENS_MATCHED") 
dbExecute(con, "DROP TABLE RW_TEST_CRED_NULLEPENS_TO_MATCH")
dbExecute(con, "DROP TABLE RW_TEST_CRED_EPENS_NOT_MATCHED_ID_PSICODE") 

# ---- 01 Make Credential Sup Vars ----

# [SELECT INTO] Create CredentialSupVars from Credential


# ---- qry01a_CredentialSupVars ---- 
qry01a_CredentialSupVars <- "
SELECT      ID, 
            ENCRYPTED_TRUE_PEN, 
            PSI_STUDENT_NUMBER, 
            PSI_CODE, 
            PSI_SCHOOL_YEAR, 
            CREDENTIAL_AWARD_DATE, 
            RecordStatus AS CredentialRecordStatus, 
            PSI_PROGRAM_CODE, 
            PSI_CREDENTIAL_PROGRAM_DESCRIPTION, 
            PSI_CREDENTIAL_CIP, 
            PSI_CREDENTIAL_LEVEL, 
            PSI_CREDENTIAL_CATEGORY
INTO        CredentialSupVars
FROM        Credential;"
dbExecute(con, qry01a_CredentialSupVars) # select key columns from Credential View into a new table called CredentialSupVars

# [ALTER TABLE] CredentialSupVars


# ---- qry01b_CredentialSupVars ---- 
qry01b_CredentialSupVars <- "
ALTER TABLE CredentialSupVars 
ADD         CREDENTIAL_AWARD_DATE_D [date] NULL,
	          PSI_AWARD_SCHOOL_YEAR [varchar](50) NULL,
	          RECORD_TO_DELETE [int] NULL,
	          psi_birthdate_cleaned_D [date] NULL,
	          psi_birthdate_cleaned [date] NULL,
	          Last_Date_Highest_Cred [varchar](255) NULL,
	          Highest_Cred_by_Date [varchar](255) NULL,
	          Highest_Cred_by_Rank [varchar](255) NULL,
	          Highest_Cred_by_School_Year [varchar](255) NULL,
	          OUTCOMES_CRED [varchar](255) NULL,
	          RESEARCH_UNIVERSITY [int] NULL,
	          CREDENTIAL_AWARD_DATE_D_DELAYED [date] NULL,
	          PSI_AWARD_SCHOOL_YEAR_DELAYED [varchar](50) NULL,
            AGE_AT_GRAD [int] NULL,
            AGE_GROUP_AT_GRAD [int] NULL, 
            psi_gender_cleaned NVARCHAR(10) NULL;"
dbExecute(con, qry01b_CredentialSupVars) # add some more columns to be filled in later
dbExecute(con, "ALTER TABLE [CredentialSupVars] ADD CONSTRAINT PK_CredSupVars_ID PRIMARY KEY (ID);")

# [ALTER TABLE] CredentialSupVarsFromEnrolment


# ---- qry01b_CredentialSupVarsFromEnrol ---- 
qry01b_CredentialSupVarsFromEnrol_1 <- "
ALTER TABLE CredentialSupVarsFromEnrolment 
ADD         PSI_MIN_START_DATE_D [date] NULL,
	          AGE_AT_GRAD [numeric](18, 0) NULL,
	          AGE_GROUP_AT_GRAD [numeric](18, 0) NULL,
	          PSI_BIRTHDATE_D [date] NULL,
	          RECORD_TO_DELETE [int] NULL,
	          psi_birthdate_cleaned_D [date] NULL,
	          PSI_VISA_STATUS [varchar](50) NULL,
	          PSI_BIRTHDATE  [varchar](50) NULL,
	          PSI_PROGRAM_CODE [varchar](500) NULL,
	          PSI_CREDENTIAL_PROGRAM_DESCRIPTION [varchar](500) NULL,
	          PSI_CIP_CODE [varchar](50) NULL,
	          PSI_CONTINUING_EDUCATION_COURSE_ONLY [varchar](50) NULL,
	          PSI_GENDER [varchar](50) NULL,
	          psi_birthdate_cleaned [date] NULL;"
dbExecute(con, qry01b_CredentialSupVarsFromEnrol_1) # add some columns to CredSupVarsEnrol

# [UPDATE] CredentialSupVarsFromEnrolment

qry01b_CredentialSupVarsFromEnrol_2 <- "	
UPDATE      CredentialSupVarsFromEnrolment
SET         psi_birthdate_cleaned = STP_Enrolment.psi_birthdate_cleaned, 
            PSI_VISA_STATUS = STP_Enrolment.PSI_VISA_STATUS, 
            PSI_BIRTHDATE = STP_Enrolment.PSI_BIRTHDATE, 
            PSI_PROGRAM_CODE = STP_Enrolment.PSI_PROGRAM_CODE, 
            PSI_CREDENTIAL_PROGRAM_DESCRIPTION = STP_Enrolment.PSI_CREDENTIAL_PROGRAM_DESCRIPTION, 
            PSI_CIP_CODE = STP_Enrolment.PSI_CIP_CODE, 
            PSI_CONTINUING_EDUCATION_COURSE_ONLY = STP_Enrolment.PSI_CONTINUING_EDUCATION_COURSE_ONLY, 
            PSI_GENDER = STP_Enrolment.PSI_GENDER
FROM        CredentialSupVarsFromEnrolment 
INNER JOIN  STP_Enrolment 
  ON        CredentialSupVarsFromEnrolment.EnrolmentID = STP_Enrolment.ID;"
dbExecute(con, qry01b_CredentialSupVarsFromEnrol_2) # bring in data from STP_Enrolment 

# [UPDATE] CredentialSupVarsFromEnrolment

qry01b_CredentialSupVarsFromEnrol_3 <- "
UPDATE      CredentialSupVarsFromEnrolment
SET         psi_birthdate_cleaned = NULL
WHERE       psi_birthdate_cleaned = '1900-01-01';"
dbExecute(con, qry01b_CredentialSupVarsFromEnrol_3) # Empty strings ' ' in psi_birthdate_cleaned were cast to 1900-01-01 in date format. 

# ---- 02 Developmental Records ----
# flag STP_Credential_Record_Type records with PSI_CREDENTIAL_CATEGORY = 'DEVELOPMENTAL CREDENTIAL' 'OTHER' 'NONE' 'SHORT CERTIFICATE'

# [SELECT INTO] Create Drop_Credential_Category from Credential


# ---- qry02a_DropCredCategory ---- 
qry02a_DropCredCategory <- "
SELECT     id, PSI_CODE, PSI_CREDENTIAL_CATEGORY, ENCRYPTED_TRUE_PEN, PSI_SCHOOL_YEAR, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION
INTO       Drop_Credential_Category
FROM       Credential
WHERE      PSI_CREDENTIAL_CATEGORY = 'DEVELOPMENTAL CREDENTIAL'
  OR       PSI_CREDENTIAL_CATEGORY = 'OTHER'
  OR       PSI_CREDENTIAL_CATEGORY = 'NONE'
  OR       PSI_CREDENTIAL_CATEGORY = 'SHORT CERTIFICATE';"
dbExecute(con, qry02a_DropCredCategory) 
dbExecute(con, "ALTER TABLE  STP_Credential_Record_Type ADD DropCredCategory NVARCHAR(50) NULL")

# [UPDATE] STP_Credential_Record_Type

# ---- qry02b_DeleteCredCategory ---- 
qry02b_DeleteCredCategory <- "
UPDATE    STP_Credential_Record_Type
SET       DropCredCategory = 'Yes'
FROM      Drop_Credential_Category 
INNER JOIN STP_Credential_Record_Type 
ON  Drop_Credential_Category.id =  STP_Credential_Record_Type.id;"
dbExecute(con, qry02b_DeleteCredCategory)
dbExecute(con, "DROP TABLE Drop_Credential_Category")

# ---- 03 Miscellaneous ----
## ---- ** Manual **  ----
# change date in qry03c_DeletePartialYear
# that query flags STP_Credential_Record_Type records whose CREDENTIAL_AWARD_DATE >= '<model-year>-09-01'

# [UPDATE] CredentialSupVars

# ---- qry03a1_ConvertAwardDate ---- 
qry03a1_ConvertAwardDate <- "
UPDATE CredentialSupVars
SET    CREDENTIAL_AWARD_DATE_D = CREDENTIAL_AWARD_DATE;"
dbExecute(con, qry03a1_ConvertAwardDate) # data type conversion

# [SELECT INTO] Create Drop_Partial_Year from CredentialSupVars

# ---- qry03b_DropPartialYear ---- 
qry03b_DropPartialYear <- "
SELECT     id, CREDENTIAL_AWARD_DATE_D
INTO            Drop_Partial_Year
FROM         CredentialSupVars
WHERE     (CREDENTIAL_AWARD_DATE_D >= '2023-09-01');"
dbExecute(con, qry03b_DropPartialYear) 
dbExecute(con, "ALTER TABLE  STP_Credential_Record_Type ADD DropPartialYear NVARCHAR(50) NULL")

# [UPDATE] STP_Credential_Record_Type

# ---- qry03c_DeletePartialYear ---- 
qry03c_DeletePartialYear <- "
UPDATE    STP_Credential_Record_Type 
SET       DropPartialYear = 'Yes'
FROM      STP_Credential_Record_Type  
INNER JOIN Drop_Partial_Year 
ON        STP_Credential_Record_Type.id = Drop_Partial_Year.id;"
dbExecute(con, qry03c_DeletePartialYear)
dbExecute(con, "DROP TABLE Drop_Partial_Year")


# [SELECT INTO] Create CredentialSupVars_BirthdateClean from CredentialSupVarsFromEnrolment

# ---- qry03d_CredentialSupVarsBirthdate ---- 
qry03d_CredentialSupVarsBirthdate <- "
SELECT        ENCRYPTED_TRUE_PEN, psi_birthdate_cleaned, psi_birthdate_cleaned_D, PSI_STUDENT_NUMBER, PSI_CODE
INTO          CredentialSupVars_BirthdateClean
FROM          CredentialSupVarsFromEnrolment
GROUP BY      ENCRYPTED_TRUE_PEN, psi_birthdate_cleaned, psi_birthdate_cleaned_D, PSI_STUDENT_NUMBER, PSI_CODE"
dbExecute(con, qry03d_CredentialSupVarsBirthdate) # create a table with unique EPEN/birthdates from CredentialSupVarsFromEnrolment
dbExecute(con, "UPDATE  CredentialSupVars_BirthdateClean 
                SET psi_birthdate_cleaned_D = cast(psi_birthdate_cleaned as date)
                WHERE psi_birthdate_cleaned is not null AND psi_birthdate_cleaned NOT IN ('', ' ')")

# ---- 03 Gender Cleaning ---- 

# [SELECT INTO] Create CredentialSupVars_Gender from CredentialSupVarsFromEnrolment


# ---- qry03e_CredentialSupVarsGender ---- 
qry03e_CredentialSupVarsGender <- "
SELECT        ENCRYPTED_TRUE_PEN, PSI_GENDER
INTO          CredentialSupVars_Gender
FROM          CredentialSupVarsFromEnrolment
GROUP BY      ENCRYPTED_TRUE_PEN, PSI_GENDER;"
dbExecute(con, qry03e_CredentialSupVarsGender) # create a table with unique EPEN/gender from CredentialSupVarsFromEnrolment

# [SELECT INTO] Create CredentialSupVars_MultiGenderCounter
# ---- qry03fCredential_SupVarsGenderCleaning1 ----
qry03fCredential_SupVarsGenderCleaning1 <-
  "SELECT [ENCRYPTED_TRUE_PEN], COUNT(*) AS GenderCount
INTO CredentialSupVars_MultiGenderCounter
FROM [CredentialSupVars_Gender]
GROUP BY [ENCRYPTED_TRUE_PEN];"
dbExecute(con, qry03fCredential_SupVarsGenderCleaning1)

# [SELECT INTO] Create CredentialSupVars_MultiGender

# ---- qry03fCredential_SupVarsGenderCleaning2 ----
qry03fCredential_SupVarsGenderCleaning2<-
  "SELECT [ENCRYPTED_TRUE_PEN]
INTO CredentialSupVars_MultiGender
FROM [CredentialSupVars_MultiGenderCounter]
WHERE [GenderCount]>1
GROUP BY [ENCRYPTED_TRUE_PEN];"
dbExecute(con, qry03fCredential_SupVarsGenderCleaning2)
dbExecute(con, "DROP TABLE CredentialSupVars_MultiGenderCounter")

# [SELECT INTO] Create tmp_CredentialGenderCleaning_Step1 from CredentialSupVars_MultiGender

# ---- qry03fCredential_SupVarsGenderCleaning3 ----
qry03fCredential_SupVarsGenderCleaning3 <-
  "SELECT CredentialSupVars_MultiGender.ENCRYPTED_TRUE_PEN, CredentialSupVarsFromEnrolment.PSI_GENDER,
       MAX(CredentialSupVarsFromEnrolment.PSI_SCHOOL_YEAR) AS MAX_PSI_SCHOOL_YEAR,
       MAX(CredentialSupVarsFromEnrolment.PSI_ENROLMENT_SEQUENCE) AS MAX_PSI_ENROLMENT_SEQUENCE
INTO tmp_CredentialGenderCleaning_Step1
FROM CredentialSupVars_MultiGender
INNER JOIN CredentialSupVarsFromEnrolment
ON CredentialSupVarsFromEnrolment.ENCRYPTED_TRUE_PEN = CredentialSupVars_MultiGender.ENCRYPTED_TRUE_PEN
GROUP BY CredentialSupVars_MultiGender.ENCRYPTED_TRUE_PEN, CredentialSupVarsFromEnrolment.PSI_GENDER;"
dbExecute(con, qry03fCredential_SupVarsGenderCleaning3)

# [SELECT INTO] Create tmp_CredentialGenderCleaning_Step2 from tmp_CredentialGenderCleaning_Step1

# ---- qry03fCredential_SupVarsGenderCleaning4 ----
qry03fCredential_SupVarsGenderCleaning4 <-
  "SELECT ENCRYPTED_TRUE_PEN, MAX(MAX_PSI_SCHOOL_YEAR) AS MAX_MAX_PSI_SCHOOL_YEAR,
       MAX(MAX_PSI_ENROLMENT_SEQUENCE) AS MAX_MAX_PSI_ENROLMENT_SEQUENCE
INTO tmp_CredentialGenderCleaning_Step2
FROM tmp_CredentialGenderCleaning_Step1
GROUP BY ENCRYPTED_TRUE_PEN;"
dbExecute(con, qry03fCredential_SupVarsGenderCleaning4)

# [SELECT INTO] Create tmp_CredentialGenderCleaning_Step3 from tmp_CredentialGenderCleaning_Step2

# ---- qry03fCredential_SupVarsGenderCleaning5 ----
qry03fCredential_SupVarsGenderCleaning5 <-
  "SELECT tmp_CredentialGenderCleaning_Step2.ENCRYPTED_TRUE_PEN,
   tmp_CredentialGenderCleaning_Step1.PSI_GENDER AS PSI_GENDER_To_Use
INTO tmp_CredentialGenderCleaning_Step3
FROM tmp_CredentialGenderCleaning_Step2
INNER JOIN tmp_CredentialGenderCleaning_Step1 ON
   tmp_CredentialGenderCleaning_Step2.ENCRYPTED_TRUE_PEN = tmp_CredentialGenderCleaning_Step1.ENCRYPTED_TRUE_PEN AND
   tmp_CredentialGenderCleaning_Step2.MAX_MAX_PSI_SCHOOL_YEAR = tmp_CredentialGenderCleaning_Step1.MAX_PSI_SCHOOL_YEAR AND
   tmp_CredentialGenderCleaning_Step2.MAX_MAX_PSI_ENROLMENT_SEQUENCE = tmp_CredentialGenderCleaning_Step1.MAX_PSI_ENROLMENT_SEQUENCE
WHERE tmp_CredentialGenderCleaning_Step2.ENCRYPTED_TRUE_PEN NOT IN ('',' ','(Unspecified)') AND tmp_CredentialGenderCleaning_Step2.ENCRYPTED_TRUE_PEN IS NOT NULL
GROUP BY tmp_CredentialGenderCleaning_Step2.ENCRYPTED_TRUE_PEN, tmp_CredentialGenderCleaning_Step1.PSI_GENDER;"
dbExecute(con, qry03fCredential_SupVarsGenderCleaning5)
dbExecute(con, "DROP TABLE tmp_CredentialGenderCleaning_Step1")
dbExecute(con, "DROP TABLE tmp_CredentialGenderCleaning_Step2")

# [SELECT INTO] Create RW_TEST_ENROL_GENDER_morethanone_list_stepa from CredentialSupVarsFromEnrolment


# ---- qry03fCredential_SupVars_Enrol_GenderCleaning6 ----
#  Find all the enrolment IDs for EPENS in credential and enrolment that match and have more than one gender in enrolment data:
qry03fCredential_SupVars_Enrol_GenderCleaning6 <-
  "SELECT T.ENCRYPTED_TRUE_PEN, COUNT(*) AS CountOfGender
INTO RW_TEST_ENROL_GENDER_morethanone_list_stepa
FROM (
    SELECT ENCRYPTED_TRUE_PEN, PSI_GENDER
    FROM CredentialSupVarsFromEnrolment
    GROUP BY ENCRYPTED_TRUE_PEN, PSI_GENDER
) T
GROUP BY T.ENCRYPTED_TRUE_PEN
HAVING COUNT(*) > 1;"
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning6)

# [SELECT INTO] Create RW_TEST_ENROL_GENDER_morethanone_list from RW_TEST_ENROL_GENDER_morethanone_list_stepa

# ---- qry03fCredential_SupVars_Enrol_GenderCleaning7 ----
qry03fCredential_SupVars_Enrol_GenderCleaning7 <-
  "SELECT R.*, IN_CREDENTIALSUPVARS = 'T'
INTO RW_TEST_ENROL_GENDER_morethanone_list
FROM RW_TEST_ENROL_GENDER_morethanone_list_stepa R
INNER JOIN (
    SELECT DISTINCT ENCRYPTED_TRUE_PEN
    FROM CredentialSupVars
) C
ON C.ENCRYPTED_TRUE_PEN = R.ENCRYPTED_TRUE_PEN;"
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning7)

# [SELECT INTO] Create RW_TEST_ENROL_GENDER_morethanone_listIDS from RW_TEST_ENROL_GENDER_morethanone_list

# ---- qry03fCredential_SupVars_Enrol_GenderCleaning8 ----
qry03fCredential_SupVars_Enrol_GenderCleaning8 <- "
SELECT RW_TEST_ENROL_GENDER_morethanone_list.ENCRYPTED_TRUE_PEN, RW_TEST_ENROL_GENDER_morethanone_list.CountOfGender,
  RW_TEST_ENROL_GENDER_morethanone_list.IN_CREDENTIALSUPVARS, STP_Enrolment_Valid.ID AS EnrolmentID
INTO RW_TEST_ENROL_GENDER_morethanone_listIDS
FROM RW_TEST_ENROL_GENDER_morethanone_list
INNER JOIN STP_Enrolment_Valid
  ON RW_TEST_ENROL_GENDER_morethanone_list.ENCRYPTED_TRUE_PEN = STP_Enrolment_Valid.ENCRYPTED_TRUE_PEN
WHERE RW_TEST_ENROL_GENDER_morethanone_list.ENCRYPTED_TRUE_PEN IS NOT NULL AND
  RW_TEST_ENROL_GENDER_morethanone_list.ENCRYPTED_TRUE_PEN NOT IN ('',' ','(Unspecified)')
"
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning8)

# [SELECT INTO] Create CredentialSupVarsFromEnrolment_MultiGender from RW_TEST_ENROL_GENDER_morethanone_listIDS

# ---- qry03fCredential_SupVars_Enrol_GenderCleaning9 ----
qry03fCredential_SupVars_Enrol_GenderCleaning9 <- "
SELECT  STP_Enrolment.ID AS EnrolmentID, STP_Enrolment.ENCRYPTED_TRUE_PEN, STP_Enrolment.PSI_BIRTHDATE, STP_Enrolment.PSI_MIN_START_DATE,
        STP_Enrolment.psi_birthdate_cleaned, STP_Enrolment.PSI_VISA_STATUS, STP_Enrolment.PSI_STUDENT_POSTAL_CODE_CURRENT, STP_Enrolment.PSI_SCHOOL_YEAR,
        STP_Enrolment.PSI_PROGRAM_CODE, STP_Enrolment.PSI_CREDENTIAL_PROGRAM_DESCRIPTION, STP_Enrolment.PSI_ENROLMENT_SEQUENCE,
        STP_Enrolment.PSI_CIP_CODE, STP_Enrolment.PSI_CONTINUING_EDUCATION_COURSE_ONLY, STP_Enrolment.PSI_GENDER
INTO    CredentialSupVarsFromEnrolment_MultiGender
FROM    RW_TEST_ENROL_GENDER_morethanone_listIDS
INNER JOIN STP_Enrolment
  ON    RW_TEST_ENROL_GENDER_morethanone_listIDS.EnrolmentID = STP_Enrolment.ID
"
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning9)
dbExecute(con, "DROP TABLE RW_TEST_ENROL_GENDER_morethanone_list_stepa")
dbExecute(con, "DROP TABLE RW_TEST_ENROL_GENDER_morethanone_list")
dbExecute(con, "DROP TABLE RW_TEST_ENROL_GENDER_morethanone_listIDS") 

# [ALTER TABLE] CredentialSupVarsFromEnrolment_MultiGender

# ---- qry03fCredential_SupVars_Enrol_GenderCleaning10 ----
qry03fCredential_SupVars_Enrol_GenderCleaning10 <- "
ALTER TABLE CredentialSupVarsFromEnrolment_MultiGender
ADD psi_gender_cleaned NVARCHAR(50)"
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning10)

# [UPDATE] CredentialSupVarsFromEnrolment_MultiGender

# ---- qry03fCredential_SupVars_Enrol_GenderCleaning11 ----
qry03fCredential_SupVars_Enrol_GenderCleaning11 <- "
UPDATE      CredentialSupVarsFromEnrolment_MultiGender
SET                psi_gender_cleaned = tmp_CredentialGenderCleaning_Step3.PSI_GENDER_To_Use
FROM            CredentialSupVarsFromEnrolment_MultiGender INNER JOIN
tmp_CredentialGenderCleaning_Step3 ON
CredentialSupVarsFromEnrolment_MultiGender.ENCRYPTED_TRUE_PEN = tmp_CredentialGenderCleaning_Step3.ENCRYPTED_TRUE_PEN"
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning11)
dbExecute(con, "DROP TABLE tmp_CredentialGenderCleaning_Step3")

# [ALTER TABLE] CredentialSupVars_Gender

# ---- qry03fCredential_SupVars_Enrol_GenderCleaning12 ----
qry03fCredential_SupVars_Enrol_GenderCleaning12 <- "
ALTER TABLE CredentialSupVars_Gender
ADD psi_gender_cleaned NVARCHAR(50),
psi_gender_cleaned_flag NVARCHAR(50)"
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning12)

# [UPDATE] CredentialSupVars_Gender

# ---- qry03fCredential_SupVars_Enrol_GenderCleaning13 ----
qry03fCredential_SupVars_Enrol_GenderCleaning13 <- "
UPDATE       CredentialSupVars_Gender
SET                psi_gender_cleaned = CredentialSupVarsFromEnrolment_MultiGender.psi_gender_cleaned, psi_gender_cleaned_flag=  'Yes'
FROM            CredentialSupVars_Gender INNER JOIN
                         CredentialSupVarsFromEnrolment_MultiGender ON
                         CredentialSupVars_Gender.ENCRYPTED_TRUE_PEN = CredentialSupVarsFromEnrolment_MultiGender.ENCRYPTED_TRUE_PEN"
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning13)

# [UPDATE] CredentialSupVars_Gender

# ---- qry03fCredential_SupVars_Enrol_GenderCleaning14 ----
qry03fCredential_SupVars_Enrol_GenderCleaning14 <- "
UPDATE       CredentialSupVars_Gender
SET                psi_gender_cleaned = PSI_GENDER
WHERE        (psi_gender_cleaned IS NULL)
"
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning14)

# [SELECT INTO] Create tmp_CredentialSupVars_Gender_CleanUnknowns from CredentialSupVars_Gender

# ---- qry03fCredential_SupVars_Enrol_GenderCleaning15 ----
qry03fCredential_SupVars_Enrol_GenderCleaning15 <- "
SELECT  ENCRYPTED_TRUE_PEN, psi_gender_cleaned, psi_gender_cleaned_flag, PSI_GENDER
INTO    tmp_CredentialSupVars_Gender_CleanUnknowns
FROM    CredentialSupVars_Gender
WHERE   (psi_gender_cleaned = 'U')
OR (psi_gender_cleaned = 'Unknown')
"
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning15)

# [ALTER TABLE] tmp_CredentialSupVars_Gender_CleanUnknowns

# ---- qry03fCredential_SupVars_Enrol_GenderCleaning16 ----
qry03fCredential_SupVars_Enrol_GenderCleaning16 <- "
ALTER TABLE tmp_CredentialSupVars_Gender_CleanUnknowns
ADD psi_gender_cleaned_NEW NVARCHAR(50)
"
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning16)

# [SELECT INTO] Create tmp_CredentialSupVars_Gender_CleanUnknowns_Step2 from tmp_CredentialSupVars_Gender_CleanUnknowns

# ---- qry03fCredential_SupVars_Enrol_GenderCleaning17 ----
qry03fCredential_SupVars_Enrol_GenderCleaning17 <- "
SELECT  tmp_CredentialSupVars_Gender_CleanUnknowns.ENCRYPTED_TRUE_PEN, tmp_CredentialSupVars_Gender_CleanUnknowns.psi_gender_cleaned,
        tmp_CredentialSupVars_Gender_CleanUnknowns.psi_gender_cleaned_flag, tmp_CredentialSupVars_Gender_CleanUnknowns.PSI_GENDER,
        tmp_CredentialSupVars_Gender_CleanUnknowns.psi_gender_cleaned_NEW, CredentialSupVarsFromEnrolment.PSI_GENDER AS Expr1
INTO    tmp_CredentialSupVars_Gender_CleanUnknowns_Step2
FROM    tmp_CredentialSupVars_Gender_CleanUnknowns
INNER JOIN CredentialSupVarsFromEnrolment ON
        tmp_CredentialSupVars_Gender_CleanUnknowns.ENCRYPTED_TRUE_PEN = CredentialSupVarsFromEnrolment.ENCRYPTED_TRUE_PEN
WHERE   tmp_CredentialSupVars_Gender_CleanUnknowns.ENCRYPTED_TRUE_PEN IS NOT NULL
        AND tmp_CredentialSupVars_Gender_CleanUnknowns.ENCRYPTED_TRUE_PEN NOT IN ('',' ','(Unspecified)')
ORDER BY tmp_CredentialSupVars_Gender_CleanUnknowns.PSI_GENDER DESC
"
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning17)

# [SELECT INTO] Create tmp_CredentialSupVars_Gender_CleanUnknowns_Step3 from tmp_CredentialSupVars_Gender_CleanUnknowns_Step2

# ---- qry03fCredential_SupVars_Enrol_GenderCleaning18 ----
qry03fCredential_SupVars_Enrol_GenderCleaning18 <- "
SELECT  ENCRYPTED_TRUE_PEN, PSI_GENDER AS GenderToUse
INTO    tmp_CredentialSupVars_Gender_CleanUnknowns_Step3
FROM    tmp_CredentialSupVars_Gender_CleanUnknowns_Step2
GROUP BY ENCRYPTED_TRUE_PEN, PSI_GENDER
HAVING  ((PSI_GENDER <> 'U') AND (PSI_GENDER <> 'Unknown'))
"
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning18)

# [UPDATE] tmp_CredentialSupVars_Gender_CleanUnknowns

# ---- qry03fCredential_SupVars_Enrol_GenderCleaning19 ----
qry03fCredential_SupVars_Enrol_GenderCleaning19 <- "
UPDATE       tmp_CredentialSupVars_Gender_CleanUnknowns
SET          psi_gender_cleaned_NEW = tmp_CredentialSupVars_Gender_CleanUnknowns_Step3.GenderToUse
FROM         tmp_CredentialSupVars_Gender_CleanUnknowns_Step3
INNER JOIN   tmp_CredentialSupVars_Gender_CleanUnknowns ON
             tmp_CredentialSupVars_Gender_CleanUnknowns_Step3.ENCRYPTED_TRUE_PEN = tmp_CredentialSupVars_Gender_CleanUnknowns.ENCRYPTED_TRUE_PEN
"
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning19)
dbExecute(con, "DROP TABLE tmp_CredentialSupVars_Gender_CleanUnknowns_Step2")
dbExecute(con, "DROP TABLE tmp_CredentialSupVars_Gender_CleanUnknowns_Step3")

# [UPDATE] CredentialSupVarsFromEnrolment_MultiGender

# ---- qry03fCredential_SupVars_Enrol_GenderCleaning20 ----
qry03fCredential_SupVars_Enrol_GenderCleaning20 <- "
UPDATE       CredentialSupVarsFromEnrolment_MultiGender
SET          psi_gender_cleaned = tmp_CredentialSupVars_Gender_CleanUnknowns.psi_gender_cleaned_NEW
FROM         CredentialSupVarsFromEnrolment_MultiGender
INNER JOIN   tmp_CredentialSupVars_Gender_CleanUnknowns ON
             CredentialSupVarsFromEnrolment_MultiGender.ENCRYPTED_TRUE_PEN = tmp_CredentialSupVars_Gender_CleanUnknowns.ENCRYPTED_TRUE_PEN
"
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning20)

# [UPDATE] credentialsupvars_gender

# ---- qry03fCredential_SupVars_Enrol_GenderCleaning21 ----
qry03fCredential_SupVars_Enrol_GenderCleaning21<-"
UPDATE credentialsupvars_gender
SET    psi_gender_cleaned =
       tmp_credentialsupvars_gender_cleanunknowns.psi_gender_cleaned_new
FROM   credentialsupvars_gender
       INNER JOIN tmp_credentialsupvars_gender_cleanunknowns
               ON credentialsupvars_gender.encrypted_true_pen =
                  tmp_credentialsupvars_gender_cleanunknowns.encrypted_true_pen"
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning21)
dbExecute(con, "DROP TABLE tmp_CredentialSupVars_Gender_CleanUnknowns")
dbExecute(con, "ALTER TABLE  credentialsupvars ALTER COLUMN psi_gender_cleaned NVARCHAR(50) NULL")

# [UPDATE] credentialsupvars

# ---- qry03fCredential_SupVars_Enrol_GenderCleaning22 ----
qry03fCredential_SupVars_Enrol_GenderCleaning22<-
"UPDATE credentialsupvars
SET    psi_gender_cleaned = credentialsupvars_gender.psi_gender_cleaned
FROM   credentialsupvars
       INNER JOIN credentialsupvars_gender
               ON credentialsupvars.encrypted_true_pen =
                  credentialsupvars_gender.encrypted_true_pen
WHERE  credentialsupvars_gender.encrypted_true_pen IS NOT NULL
       AND credentialsupvars_gender.encrypted_true_pen NOT IN ('',' ','(Unspecified)')"
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning22)

# [SQL]

# ---- qry03fCredential_SupVars_Enrol_GenderCleaning23 ----
qry03fCredential_SupVars_Enrol_GenderCleaning23<-"
SELECT *
FROM   credentialsupvars
WHERE  psi_gender_cleaned IS NULL"
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning23)

# [SELECT INTO] Create tmp_credentialgendercleaning_step5 from credentialsupvars

# ---- qry03fCredential_SupVars_Enrol_GenderCleaning24 ----
qry03fCredential_SupVars_Enrol_GenderCleaning24<-"
SELECT encrypted_true_pen,
       psi_student_number,
       psi_code,
       psi_gender_cleaned
INTO   tmp_credentialgendercleaning_step5
FROM   credentialsupvars
WHERE  psi_gender_cleaned IS NULL;"
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning24)

# [SELECT INTO] Create tmp_credentialgendercleaning_step6 from tmp_credentialgendercleaning_step5

# ---- qry03fCredential_SupVars_Enrol_GenderCleaning25 ----
qry03fCredential_SupVars_Enrol_GenderCleaning25<-"
SELECT DISTINCT tmp_credentialgendercleaning_step5.encrypted_true_pen,
                stp_enrolment.psi_student_number,
                stp_enrolment.psi_code,
                psi_gender_cleaned,
                stp_enrolment.psi_gender
INTO   tmp_credentialgendercleaning_step6
FROM   tmp_credentialgendercleaning_step5
       INNER JOIN stp_enrolment
               ON tmp_credentialgendercleaning_step5.psi_student_number =
                             stp_enrolment.psi_student_number
                  AND tmp_credentialgendercleaning_step5.psi_code =
                      stp_enrolment.psi_code;"
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning25)
dbExecute(con, "DROP TABLE tmp_CredentialGenderCleaning_Step5")

# [SELECT INTO] Create CredentialSupVars_MultiGenderCounterForNULLS from tmp_CredentialGenderCleaning_Step6

# ---- qry03fCredential_SupVars_Enrol_GenderCleaning26a ----
qry03fCredential_SupVars_Enrol_GenderCleaning26a<-"
SELECT ENCRYPTED_TRUE_PEN,
       PSI_STUDENT_NUMBER,
       psi_code,
       COUNT(*) AS GenderCount
INTO CredentialSupVars_MultiGenderCounterForNULLS
FROM tmp_CredentialGenderCleaning_Step6
GROUP BY ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, psi_code"
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning26a)

# [SELECT INTO] Create credentialsupvars_multigenderfornulls from credentialsupvars_multigendercounterfornulls

# ---- qry03fCredential_SupVars_Enrol_GenderCleaning26b ----
qry03fCredential_SupVars_Enrol_GenderCleaning26b<-"
SELECT encrypted_true_pen,
       psi_student_number,
       psi_code,
       Count(*) AS GenderCount
INTO   credentialsupvars_multigenderfornulls
FROM   credentialsupvars_multigendercounterfornulls
GROUP  BY encrypted_true_pen,
          psi_student_number,
          psi_code
HAVING Count(*) > 1 "
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning26b)

# [SELECT INTO] Create tmp_CredentialGenderCleaning_Step7 from CredentialSupVars_MultiGenderForNULLS


# ---- qry03fCredential_SupVars_Enrol_GenderCleaning27 ----
qry03fCredential_SupVars_Enrol_GenderCleaning27<-"
SELECT      CredentialSupVars_MultiGenderForNULLS.ENCRYPTED_TRUE_PEN, CredentialSupVars_MultiGenderForNULLS.PSI_STUDENT_NUMBER,
            CredentialSupVars_MultiGenderForNULLS.psi_code,
            CredentialSupVarsFromEnrolment.PSI_GENDER, MAX(CredentialSupVarsFromEnrolment.PSI_SCHOOL_YEAR) AS MAX_PSI_SCHOOL_YEAR,
            MAX(CredentialSupVarsFromEnrolment.PSI_ENROLMENT_SEQUENCE) AS MAX_PSI_ENROLMENT_SEQUENCE
INTO        tmp_CredentialGenderCleaning_Step7
FROM        CredentialSupVars_MultiGenderForNULLS
INNER JOIN  CredentialSupVarsFromEnrolment
  ON        CredentialSupVarsFromEnrolment.ENCRYPTED_TRUE_PEN = CredentialSupVars_MultiGenderForNULLS.ENCRYPTED_TRUE_PEN
GROUP BY    CredentialSupVars_MultiGenderForNULLS.ENCRYPTED_TRUE_PEN, CredentialSupVars_MultiGenderForNULLS.PSI_STUDENT_NUMBER,
            CredentialSupVars_MultiGenderForNULLS.psi_code, CredentialSupVarsFromEnrolment.PSI_GENDER"
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning27)
dbExecute(con, "DROP TABLE CredentialSupVars_MultiGenderForNULLS")
dbExecute(con, "DROP TABLE CredentialSupVars_MultiGenderCounterForNULLS")
dbExecute(con, "ALTER TABLE tmp_credentialgendercleaning_step6 ADD psi_gender_cleaned_flag nvarchar(50)")
dbExecute(con, "ALTER TABLE tmp_credentialgendercleaning_step7 ADD psi_gender_cleaned nvarchar(50)")

# [UPDATE] tmp_credentialgendercleaning_step6

# ---- qry03fCredential_SupVars_Enrol_GenderCleaning28 ----
qry03fCredential_SupVars_Enrol_GenderCleaning28 <- "
UPDATE tmp_credentialgendercleaning_step6
SET    tmp_credentialgendercleaning_step6.psi_gender_cleaned = tmp_credentialgendercleaning_step7.psi_gender_cleaned,
       tmp_credentialgendercleaning_step6.psi_gender_cleaned_flag = 'Yes'
FROM   tmp_credentialgendercleaning_step6
       INNER JOIN tmp_credentialgendercleaning_step7
               ON tmp_credentialgendercleaning_step6.psi_student_number = tmp_credentialgendercleaning_step7.psi_student_number
              AND tmp_credentialgendercleaning_step6.psi_code = tmp_credentialgendercleaning_step7.psi_code"
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning28) 

# [SELECT INTO] Create tmp_credentialsupvars_gender_cleanunknownsfornulls_step1 from tmp_credentialgendercleaning_step6

# ---- qry03fCredential_SupVars_Enrol_GenderCleaning29  ----
qry03fCredential_SupVars_Enrol_GenderCleaning29 <- "
SELECT encrypted_true_pen,
       psi_student_number,
       psi_code,
       psi_gender_cleaned,
       psi_gender_cleaned_flag,
       psi_gender
INTO   tmp_credentialsupvars_gender_cleanunknownsfornulls_step1
FROM   tmp_credentialgendercleaning_step6
WHERE  ( psi_gender = 'U' OR psi_gender = 'Unknown' OR psi_gender = '(Unspecified)')
       AND psi_gender_cleaned_flag IS NULL"
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning29)

# [SELECT INTO] Create tmp_credentialsupvars_gender_cleanunknownsfornulls_step2 from tmp_credentialsupvars_gender_cleanunknownsfornulls_step1

# ---- qry03fCredential_SupVars_Enrol_GenderCleaning30 ----
qry03fCredential_SupVars_Enrol_GenderCleaning30 <- "
SELECT
       tmp_credentialsupvars_gender_cleanunknownsfornulls_step1.encrypted_true_pen,
       tmp_credentialsupvars_gender_cleanunknownsfornulls_step1.psi_student_number,
       tmp_credentialsupvars_gender_cleanunknownsfornulls_step1.psi_code,
       tmp_credentialsupvars_gender_cleanunknownsfornulls_step1.psi_gender_cleaned,
       tmp_credentialsupvars_gender_cleanunknownsfornulls_step1.psi_gender_cleaned_flag,
       tmp_credentialsupvars_gender_cleanunknownsfornulls_step1.psi_gender,
       credentialsupvarsfromenrolment.psi_gender AS Expr1
INTO   tmp_credentialsupvars_gender_cleanunknownsfornulls_step2
FROM   tmp_credentialsupvars_gender_cleanunknownsfornulls_step1
       INNER JOIN credentialsupvarsfromenrolment
               ON tmp_credentialsupvars_gender_cleanunknownsfornulls_step1.encrypted_true_pen = credentialsupvarsfromenrolment.encrypted_true_pen
              AND tmp_credentialsupvars_gender_cleanunknownsfornulls_step1.psi_student_number = credentialsupvarsfromenrolment.psi_student_number
              AND tmp_credentialsupvars_gender_cleanunknownsfornulls_step1.psi_code = credentialsupvarsfromenrolment.psi_code
ORDER  BY tmp_credentialsupvars_gender_cleanunknownsfornulls_step1.psi_student_number DESC"
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning30)

# [SELECT INTO] Create tmp_credentialsupvars_gender_cleanunknownsfornulls_step3 from tmp_credentialsupvars_gender_cleanunknownsfornulls_step2


# ---- qry03fCredential_SupVars_Enrol_GenderCleaning31  ----
qry03fCredential_SupVars_Enrol_GenderCleaning31 <- "
SELECT encrypted_true_pen,
       psi_student_number,
       psi_code,
       psi_gender AS GenderToUse
INTO   tmp_credentialsupvars_gender_cleanunknownsfornulls_step3
FROM   tmp_credentialsupvars_gender_cleanunknownsfornulls_step2
GROUP  BY encrypted_true_pen,
          psi_student_number,
          psi_code,
          psi_gender
HAVING ( psi_gender <> 'U' AND psi_gender <> 'Unknown' AND psi_gender <> '(Unspecified)')"
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning31)

# [UPDATE] tmp_CredentialGenderCleaning_Step6

# ---- qry03fCredential_SupVars_Enrol_GenderCleaning32 ----
qry03fCredential_SupVars_Enrol_GenderCleaning32 <- "
UPDATE       tmp_CredentialGenderCleaning_Step6
SET                psi_gender_cleaned_flag = 'Yes'
WHERE (PSI_GENDER = 'U' OR PSI_GENDER = 'Unknown' OR psi_gender = '(Unspecified)')"
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning32)
dbExecute(con, "DROP TABLE tmp_CredentialSupVars_Gender_CleanUnknownsforNULLS_Step1")
dbExecute(con, "DROP TABLE tmp_CredentialSupVars_Gender_CleanUnknownsforNULLS_Step2")
dbExecute(con, "DROP TABLE tmp_CredentialSupVars_Gender_CleanUnknownsforNULLS_Step3")

# [UPDATE] tmp_CredentialGenderCleaning_Step6

qry03fCredential_SupVars_Enrol_GenderCleaning33 <- "
UPDATE       tmp_CredentialGenderCleaning_Step6
SET                psi_gender_cleaned = tmp_CredentialGenderCleaning_Step7.psi_gender_cleaned, psi_gender_cleaned_flag=  'Yes'
FROM            tmp_CredentialGenderCleaning_Step6 INNER JOIN
                         tmp_CredentialGenderCleaning_Step7 ON
                         tmp_CredentialGenderCleaning_Step6.PSI_STUDENT_NUMBER = tmp_CredentialGenderCleaning_Step7.PSI_STUDENT_NUMBER
					 AND tmp_CredentialGenderCleaning_Step6.psi_code = tmp_CredentialGenderCleaning_Step7.psi_code"
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning33)
dbExecute(con, "DROP TABLE tmp_CredentialGenderCleaning_Step7")

# [UPDATE] tmp_CredentialGenderCleaning_Step6

# ---- qry03fCredential_SupVars_Enrol_GenderCleaning34 ----
qry03fCredential_SupVars_Enrol_GenderCleaning34 <- "
UPDATE       tmp_CredentialGenderCleaning_Step6
SET                psi_gender_cleaned_flag = 'Yes', psi_gender_cleaned = PSI_GENDER
WHERE psi_gender_cleaned_flag IS NULL"
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning34)

# [UPDATE] CredentialSupVars

# ---- qry03fCredential_SupVars_Enrol_GenderCleaning35 ----
qry03fCredential_SupVars_Enrol_GenderCleaning35 <- "
UPDATE       CredentialSupVars
SET          psi_gender_cleaned = tmp_CredentialGenderCleaning_Step6.psi_gender_cleaned
FROM         tmp_CredentialGenderCleaning_Step6 INNER JOIN
                         CredentialSupVars ON
                         tmp_CredentialGenderCleaning_Step6.PSI_STUDENT_NUMBER = CredentialSupVars.PSI_STUDENT_NUMBER
					 AND tmp_CredentialGenderCleaning_Step6.psi_code = CredentialSupVars.psi_code
WHERE tmp_CredentialGenderCleaning_Step6.psi_gender_cleaned_flag='Yes' AND CredentialSupVars.psi_gender_cleaned IS NULL"
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning35)
dbExecute(con, "DROP TABLE tmp_CredentialGenderCleaning_Step6")
dbExecute(con, "DROP TABLE CredentialSupVars_MultiGender")
dbExecute(con, "DROP TABLE CredentialSupVarsFromEnrolment_MultiGender")

dbGetQuery(con, "
SELECT T.ENCRYPTED_TRUE_PEN FROM (
SELECT DISTINCT ENCRYPTED_TRUE_PEN, psi_gender_cleaned
FROM CredentialSupVars
) T
GROUP BY T.ENCRYPTED_TRUE_PEN
HAVING COUNT(*) > 1")

dbGetQuery(con, "
SELECT T.PSI_CODE, T.PSI_STUDENT_NUMBER FROM (
SELECT DISTINCT PSI_CODE, PSI_STUDENT_NUMBER, psi_gender_cleaned
FROM CredentialSupVars
) T
GROUP BY T.PSI_CODE, T.PSI_STUDENT_NUMBER
HAVING COUNT(*) > 1")

# ---- 04 Birthdate cleaning (last seen birthdate) ----
# the biggest issue in this section is there are too many psi_birthdate_cleaned cols. 

# [UPDATE] CredentialSupVars


# ---- qry04a_UpdateCredentialSupVarsBirthdate ---- 
qry04a_UpdateCredentialSupVarsBirthdate <- "
UPDATE        CredentialSupVars
SET           psi_birthdate_cleaned = CredentialSupVars_BirthdateClean.psi_birthdate_cleaned, 
              psi_birthdate_cleaned_D = CredentialSupVars_BirthdateClean.psi_birthdate_cleaned_D
FROM          CredentialSupVars 
INNER JOIN    CredentialSupVars_BirthdateClean 
  ON          CredentialSupVars.ENCRYPTED_TRUE_PEN = CredentialSupVars_BirthdateClean.ENCRYPTED_TRUE_PEN
WHERE        (CredentialSupVars.ENCRYPTED_TRUE_PEN IS NOT NULL AND CredentialSupVars.ENCRYPTED_TRUE_PEN NOT IN ('', ' ', '(Unspecified)'))"
dbExecute(con, qry04a_UpdateCredentialSupVarsBirthdate) # run for the records that matched on ENCRYPTED_TRUE_PEN (non-null/blank)

# [UPDATE] CredentialSupVars

qry04a_UpdateCredentialSupVarsBirthdate2 <- "
UPDATE      CredentialSupVars
SET         psi_birthdate_cleaned = CredentialSupVars_BirthdateClean.psi_birthdate_cleaned, 
            psi_birthdate_cleaned_D = CredentialSupVars_BirthdateClean.psi_birthdate_cleaned_D
FROM        CredentialSupVars_BirthdateClean 
INNER JOIN  CredentialSupVars 
  ON        CredentialSupVars_BirthdateClean.PSI_STUDENT_NUMBER = CredentialSupVars.PSI_STUDENT_NUMBER 
  AND       CredentialSupVars_BirthdateClean.PSI_CODE = CredentialSupVars.PSI_CODE
WHERE       (CredentialSupVars.ENCRYPTED_TRUE_PEN IS NULL OR CredentialSupVars.ENCRYPTED_TRUE_PEN IN ('', ' ', '(Unspecified)'))
AND         (CredentialSupVars.PSI_CODE IS NOT NULL AND CredentialSupVars.PSI_CODE NOT IN ('', ' ', '(Unspecified)')) 
AND         (CredentialSupVars.PSI_STUDENT_NUMBER IS NOT NULL AND CredentialSupVars.PSI_STUDENT_NUMBER NOT IN ('', ' ', '(Unspecified)'));"
dbExecute(con, qry04a_UpdateCredentialSupVarsBirthdate2) # supports the PSI_CODE/PSI_STUDENT number combos
dbExecute(con, "ALTER TABLE CredentialSupVars ADD LAST_SEEN_BIRTHDATE DATE")
dbExecute(con, "ALTER TABLE CredentialSupVarsFromEnrolment ADD LAST_SEEN_BIRTHDATE DATE")

# [UPDATE] CredentialSupVarsFromEnrolment


qry04a1_UpdateCredentialSupVarsBirthdate <- "
UPDATE        CredentialSupVarsFromEnrolment
SET           LAST_SEEN_BIRTHDATE = STP_Enrolment.LAST_SEEN_BIRTHDATE
FROM          CredentialSupVarsFromEnrolment 
INNER JOIN    STP_Enrolment 
ON CredentialSupVarsFromEnrolment.EnrolmentID = STP_Enrolment.ID"
dbExecute(con, qry04a1_UpdateCredentialSupVarsBirthdate) 

# [UPDATE] CredentialSupVars

qry04a2_UpdateCredentialSupVarsBirthdate <- "
UPDATE        CredentialSupVars
SET           LAST_SEEN_BIRTHDATE = CredentialSupVarsFromEnrolment.LAST_SEEN_BIRTHDATE
FROM          CredentialSupVarsFromEnrolment 
INNER JOIN    CredentialSupVars 
ON CredentialSupVarsFromEnrolment.ENCRYPTED_TRUE_PEN = CredentialSupVars.ENCRYPTED_TRUE_PEN"
dbExecute(con, qry04a2_UpdateCredentialSupVarsBirthdate) 

# [UPDATE] CredentialSupVars

qry04a3_UpdateCredentialSupVarsBirthdate <- "
UPDATE       CredentialSupVars
SET          CredentialSupVars.psi_birthdate_cleaned = LAST_SEEN_BIRTHDATE
WHERE        ((LAST_SEEN_BIRTHDATE IS NOT NULL AND LAST_SEEN_BIRTHDATE NOT IN ('', ' ')) 
AND           (psi_birthdate_cleaned IS NULL))
OR           ((LAST_SEEN_BIRTHDATE IS NOT NULL AND LAST_SEEN_BIRTHDATE NOT IN ('', ' ')) 
AND           (psi_birthdate_cleaned IN ('', ' ')))"
dbExecute(con, qry04a3_UpdateCredentialSupVarsBirthdate) 
dbExecute(con, "DROP TABLE CredentialSupVars_BirthdateClean")


# [UPDATE] CredentialSupVars

# ---- qry04b_UpdateCredentiaSupVarsGender  ---- 
qry04b_UpdateCredentiaSupVarsGender <- "
UPDATE       CredentialSupVars
SET          psi_gender_cleaned = CredentialSupVars_Gender.psi_gender_cleaned
FROM         CredentialSupVars 
INNER JOIN   CredentialSupVars_Gender 
  ON         CredentialSupVars.ENCRYPTED_TRUE_PEN = CredentialSupVars_Gender.ENCRYPTED_TRUE_PEN;"
dbExecute(con, qry04b_UpdateCredentiaSupVarsGender)
dbExecute(con, "DROP TABLE CredentialSupVars_Gender")
dbExecute(con, "DROP VIEW Credential")

# [SQL]


# ---- qry04c_RecreateCredentialViewWithSupVars  ---- 
qry04c_RecreateCredentialViewWithSupVars <- "
CREATE VIEW Credential AS
SELECT        STP_Credential.ID, STP_Credential.ENCRYPTED_TRUE_PEN,  STP_Credential.PSI_STUDENT_NUMBER,
              STP_Credential.PSI_CODE, STP_Credential.PSI_FULL_NAME, STP_Credential.PSI_SCHOOL_YEAR, 
              STP_Credential.PSI_PROGRAM_CODE, STP_Credential.PSI_CREDENTIAL_PROGRAM_DESCRIPTION, 
              STP_Credential.PSI_CREDENTIAL_CATEGORY, STP_Credential.PSI_CREDENTIAL_LEVEL, 
              STP_Credential.PSI_CREDENTIAL_CIP, STP_Credential.CREDENTIAL_AWARD_DATE, 
              CredentialSupVars.CREDENTIAL_AWARD_DATE_D, CredentialSupVars.AGE_AT_GRAD, CredentialSupVars.AGE_GROUP_AT_GRAD, 
              CredentialSupVars.PSI_AWARD_SCHOOL_YEAR, CredentialSupVars.RECORD_TO_DELETE, CredentialSupVars.Last_Date_Highest_Cred, 
              CredentialSupVars.Highest_Cred_by_Date, CredentialSupVars.Highest_Cred_by_Rank, CredentialSupVars.Highest_Cred_by_School_Year, 
              CredentialSupVars.OUTCOMES_CRED, CredentialSupVars.RESEARCH_UNIVERSITY, CredentialSupVars.CREDENTIAL_AWARD_DATE_D_DELAYED, 
              CredentialSupVars.PSI_AWARD_SCHOOL_YEAR_DELAYED, 
						  CredentialSupVars.psi_birthdate_cleaned,
						  CredentialSupVars.psi_birthdate_cleaned_D, 
						  CredentialSupVars.psi_gender_cleaned,
						  STP_Credential_Record_Type.RecordStatus, STP_Credential_Record_Type.DropCredCategory, 
              STP_Credential_Record_Type.DropPartialYear
FROM          STP_Credential 
INNER JOIN    CredentialSupVars 
  ON          STP_Credential.ID = CredentialSupVars.ID 
  INNER JOIN  STP_Credential_Record_Type ON STP_Credential.ID = STP_Credential_Record_Type.ID
WHERE        (STP_Credential_Record_Type.RecordStatus = 0) 
AND           (STP_Credential_Record_Type.DropCredCategory IS NULL)
AND           (STP_Credential_Record_Type.DropPartialYear IS NULL);"
dbExecute(con, qry04c_RecreateCredentialViewWithSupVars)

# ---- 05 Age and Credential Update ----

# [SQL]


# ---- qry05a_FindDistinctCredentials_CreateViewCredentialRemoveDup ---- 
qry05a_FindDistinctCredentials_CreateViewCredentialRemoveDup <- "
CREATE VIEW     Credential_Remove_Dup AS
SELECT DISTINCT ENCRYPTED_TRUE_PEN, PSI_CODE, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION, PSI_CREDENTIAL_CIP, PSI_CREDENTIAL_LEVEL, 
                PSI_CREDENTIAL_CATEGORY, CREDENTIAL_AWARD_DATE_D, MAX(DISTINCT id) AS ID
FROM            Credential
GROUP BY        ENCRYPTED_TRUE_PEN, PSI_CODE, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION, PSI_CREDENTIAL_CIP, PSI_CREDENTIAL_LEVEL, 
                PSI_CREDENTIAL_CATEGORY, CREDENTIAL_AWARD_DATE_D;"
dbExecute(con, qry05a_FindDistinctCredentials_CreateViewCredentialRemoveDup) 
# NOTE: check that the birthdate cleaned_D column populated correctly 
dbExecute(con, "UPDATE Credential SET psi_birthdate_cleaned_D = psi_birthdate_cleaned where psi_birthdate_cleaned is not null")

# [UPDATE] Credential


# ---- qry05c_UpdateAgeAtGrad ---- 
qry05c_UpdateAgeAtGrad <- "
UPDATE    Credential
SET       AGE_AT_GRAD = 
              CASE WHEN dateadd(year, datediff(year, psi_birthdate_cleaned_d, CREDENTIAL_AWARD_DATE_D), psi_birthdate_cleaned_d) > CREDENTIAL_AWARD_DATE_D 
              THEN datediff(year, psi_birthdate_cleaned_d, CREDENTIAL_AWARD_DATE_D) - 1 
              ELSE datediff(year, psi_birthdate_cleaned_d,  CREDENTIAL_AWARD_DATE_D) 
              END
WHERE     (psi_birthdate_cleaned IS NOT NULL) AND (psi_birthdate_cleaned NOT IN ('', ' '));"
dbExecute(con, qry05c_UpdateAgeAtGrad)

# [UPDATE] Credential


# ---- qry05d_UpdateAGAtGrad ---- 
qry05d_UpdateAgeGroupAtGrad <- "
UPDATE    Credential
SET       AGE_GROUP_AT_GRAD = AgeGroupLookup.AgeIndex
FROM      Credential CROSS JOIN AgeGroupLookup
WHERE     (AgeGroupLookup.LowerBound <= Credential.AGE_AT_GRAD) AND (AgeGroupLookup.UpperBound >= Credential.AGE_AT_GRAD);"
dbExecute(con, qry05d_UpdateAgeGroupAtGrad)

# [UPDATE] Credential


# ---- qry06e_UpdateAwardSchoolYear ---- 
qry06e_UpdateAwardSchoolYear <- "
UPDATE Credential
SET    PSI_AWARD_SCHOOL_YEAR = CASE
	WHEN (Month(Credential.CREDENTIAL_AWARD_DATE_D) >= 9) THEN LTrim(Str(Year(Credential.CREDENTIAL_AWARD_DATE_D))) + '/' + LTrim(Str(Year(Credential.CREDENTIAL_AWARD_DATE_D)+1))
	ELSE LTrim(Str(Year(Credential.CREDENTIAL_AWARD_DATE_D)-1)) + '/' + LTrim(Str(Year(Credential.CREDENTIAL_AWARD_DATE_D)))
	END
WHERE (((Credential.PSI_AWARD_SCHOOL_YEAR) Is Null));"
dbExecute(con, qry06e_UpdateAwardSchoolYear)

# ---- 07 Credential Cleaning ----
## ---- ** Create NON DUP ** ----

# [UPDATE] Credential


# ---- qry07a1a_UpdateGender ---- 
qry07a1a_UpdateGender <- "
UPDATE    Credential
SET       Credential.PSI_GENDER_cleaned = STP_Enrolment.PSI_GENDER
FROM      STP_Enrolment 
INNER JOIN Credential ON STP_Enrolment.ENCRYPTED_TRUE_PEN = Credential.ENCRYPTED_TRUE_PEN
 AND STP_Enrolment.PSI_STUDENT_NUMBER = Credential.PSI_STUDENT_NUMBER
 AND STP_Enrolment.PSI_code = Credential.PSI_code
WHERE     (Credential.PSI_GENDER_cleaned IN ('', ' ', '(Unspecified)') 
OR         Credential.PSI_GENDER_cleaned IS NULL) 
AND       (STP_Enrolment.PSI_GENDER IN ('Female', 'Male', 'Gender Diverse'));"
dbExecute(con, qry07a1a_UpdateGender)

# [SELECT INTO] Create credential_non_dup from credential


# ---- qry07a1b_Create_Credential_Non_Dup ---- 
qry07a1b_Create_Credential_Non_Dup <- "
SELECT credential.id,
       credential.psi_student_number,
       credential.psi_birthdate_cleaned,
       credential.psi_gender_cleaned,
       credential.encrypted_true_pen,
       credential.psi_school_year,
       credential.psi_code,
       credential.credential_award_date,
       credential.recordstatus,
       credential.psi_program_code,
       credential.PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
       credential.psi_credential_cip,
       credential.psi_credential_level,
       credential.psi_credential_category,
       credential.credential_award_date_d,
       credential.age_at_grad,
       credential.age_group_at_grad,
       credential.psi_birthdate_cleaned_d,
       credential.psi_award_school_year,
       credential.record_to_delete,
       credential.last_date_highest_cred,
       credential.highest_cred_by_date,
       credential.highest_cred_by_rank,
       credential.outcomes_cred,
       credential.highest_cred_by_school_year,
       credential.research_university
INTO   credential_non_dup
FROM   credential
INNER JOIN credential_remove_dup
ON credential.id = credential_remove_dup.id; "
dbExecute(con, qry07a1b_Create_Credential_Non_Dup) 

# [SELECT INTO] Create tmp_credential_epen_gender from credential

# ---- qry07a1c_tmp_Credential_Gender ---- 
qry07a1c_tmp_Credential_Gender <- "
SELECT DISTINCT encrypted_true_pen,
                psi_student_number,
                psi_code,
                psi_gender_cleaned
INTO   tmp_credential_epen_gender
FROM   credential;"
dbExecute(con, qry07a1c_tmp_Credential_Gender)

# [SELECT INTO] Create tmp_dup_credential_epen_gender from tmp_credential_epen_gender

# ---- qry07a1d_tmp_Credential_GenderDups ---- 
qry07a1d_tmp_Credential_GenderDups <- "
SELECT encrypted_true_pen,
       psi_student_number,
       psi_code,
       Count(*) AS Expr1
INTO   tmp_dup_credential_epen_gender
FROM   tmp_credential_epen_gender
GROUP  BY encrypted_true_pen,
          psi_student_number,
          psi_code
HAVING ( Count(*) > 1 );"
dbExecute(con, qry07a1d_tmp_Credential_GenderDups)

# [SELECT INTO] Create tmp_dup_credential_epen_gender_maxcreddate from credential_non_dup


# ---- qry07a1e_tmp_Credential_GenderDups_FindMaxCredDate ---- 
qry07a1e_tmp_Credential_GenderDups_FindMaxCredDate <- "
SELECT tmp_dup_credential_epen_gender.encrypted_true_pen,
       tmp_dup_credential_epen_gender.psi_student_number,
       tmp_dup_credential_epen_gender.psi_code,
       Max(credential_non_dup.credential_award_date_d) AS
       Max_Credential_Award_Date
INTO   tmp_dup_credential_epen_gender_maxcreddate
FROM   credential_non_dup
       INNER JOIN tmp_dup_credential_epen_gender
               ON credential_non_dup.encrypted_true_pen =
                             tmp_dup_credential_epen_gender.encrypted_true_pen
                  AND credential_non_dup.psi_student_number =
                      tmp_dup_credential_epen_gender.psi_student_number
                  AND credential_non_dup.psi_code =
                      tmp_dup_credential_epen_gender.psi_code
GROUP  BY tmp_dup_credential_epen_gender.encrypted_true_pen,
          tmp_dup_credential_epen_gender.psi_student_number,
          tmp_dup_credential_epen_gender.psi_code;"
dbExecute(con, qry07a1e_tmp_Credential_GenderDups_FindMaxCredDate)

dbExecute(con, "ALTER TABLE tmp_Dup_Credential_EPEN_Gender_MaxCredDate ADD PSI_GENDER varchar(10)")

# [UPDATE] tmp_Dup_Credential_EPEN_Gender_MaxCredDate


# ---- qry07a1f_tmp_Credential_GenderDups_PickGender ---- 
qry07a1f_tmp_Credential_GenderDups_PickGender <- "
UPDATE      tmp_Dup_Credential_EPEN_Gender_MaxCredDate
SET         PSI_GENDER = Credential_Non_Dup.PSI_GENDER_cleaned
FROM        tmp_Dup_Credential_EPEN_Gender_MaxCredDate 
INNER JOIN  Credential_Non_Dup 
ON          tmp_Dup_Credential_EPEN_Gender_MaxCredDate.ENCRYPTED_TRUE_PEN = Credential_Non_Dup.ENCRYPTED_TRUE_PEN 
AND         tmp_Dup_Credential_EPEN_Gender_MaxCredDate.Max_Credential_Award_Date = Credential_Non_Dup.CREDENTIAL_AWARD_DATE_D"
dbExecute(con, qry07a1f_tmp_Credential_GenderDups_PickGender)                

# [UPDATE] Credential_Non_Dup


# ---- qry07a1g_Update_Credential_Non_Dup_GenderDups ---- 
qry07a1g_Update_Credential_Non_Dup_GenderDups <- "
UPDATE    Credential_Non_Dup
SET       PSI_GENDER_CLEANED = tmp_Dup_Credential_EPEN_Gender_MaxCredDate.PSI_GENDER
FROM      tmp_Dup_Credential_EPEN_Gender_MaxCredDate 
INNER JOIN  Credential_Non_Dup 
ON        tmp_Dup_Credential_EPEN_Gender_MaxCredDate.ENCRYPTED_TRUE_PEN = Credential_Non_Dup.ENCRYPTED_TRUE_PEN 
AND       Credential_Non_Dup.PSI_GENDER_CLEANED <> tmp_Dup_Credential_EPEN_Gender_MaxCredDate.PSI_GENDER;"
dbExecute(con, qry07a1g_Update_Credential_Non_Dup_GenderDups)  

# [UPDATE] Credential


# ---- qry07a1h_Update_Credential_GenderDups ---- 
qry07a1h_Update_Credential_GenderDups <- "
UPDATE    Credential
SET       PSI_GENDER_CLEANED = tmp_Dup_Credential_EPEN_Gender_MaxCredDate.PSI_GENDER
FROM      Credential 
INNER JOIN tmp_Dup_Credential_EPEN_Gender_MaxCredDate 
ON        Credential.ENCRYPTED_TRUE_PEN = tmp_Dup_Credential_EPEN_Gender_MaxCredDate.ENCRYPTED_TRUE_PEN 
AND       Credential.PSI_GENDER_CLEANED <> tmp_Dup_Credential_EPEN_Gender_MaxCredDate.PSI_GENDER;"
dbExecute(con, qry07a1h_Update_Credential_GenderDups) 

# [SELECT INTO] Create CRED_Extract_No_Gender from Credential_Non_Dup


# ---- qry07a2a_ExtractNoGender ---- 
qry07a2a_ExtractNoGender <- "
SELECT    id, ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, psi_gender_cleaned, PSI_CREDENTIAL_CATEGORY 
INTO      CRED_Extract_No_Gender
FROM      Credential_Non_Dup
WHERE     psi_gender_cleaned IN ('', ' ', '(Unspecified)') OR psi_gender_cleaned IS NULL;"
dbExecute(con, qry07a2a_ExtractNoGender)

# [SELECT INTO] Create CRED_Extract_No_Gender_Unique from CRED_Extract_No_Gender


# ---- qry07a2b_ExtractNoGenderUnique ---- 
qry07a2b_ExtractNoGenderUnique <- "
SELECT    DISTINCT ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, psi_gender_cleaned, PSI_CREDENTIAL_CATEGORY
INTO      CRED_Extract_No_Gender_Unique
FROM      CRED_Extract_No_Gender;"
dbExecute(con, qry07a2b_ExtractNoGenderUnique)

# [SELECT INTO] Create CRED_Extract_No_Gender_EPEN_with_MultiCred from CRED_Extract_No_Gender_Unique

# ---- qry07a2c_Create_CRED_Extract_No_Gender_EPEN_with_MultiCred ---- 
qry07a2c_Create_CRED_Extract_No_Gender_EPEN_with_MultiCred <- "
SELECT    ENCRYPTED_TRUE_PEN, psi_gender_cleaned, COUNT(*) AS Expr1
INTO      CRED_Extract_No_Gender_EPEN_with_MultiCred
FROM      CRED_Extract_No_Gender_Unique
GROUP BY  ENCRYPTED_TRUE_PEN, psi_gender_cleaned
HAVING    COUNT(*) > 1;"
dbExecute(con, qry07a2c_Create_CRED_Extract_No_Gender_EPEN_with_MultiCred)
dbExecute(con, "ALTER TABLE CRED_Extract_No_Gender_Unique ADD MultiCredFlag varchar(2)")

# [UPDATE] CRED_Extract_No_Gender_Unique

# ---- qry07a2d_Update_MultiCredFlag ---- 
qry07a2d_Update_MultiCredFlag <- "
UPDATE    CRED_Extract_No_Gender_Unique
SET       MultiCredFlag = 'Y'
FROM      CRED_Extract_No_Gender_Unique
INNER JOIN CRED_Extract_No_Gender_EPEN_with_MultiCred 
ON        CRED_Extract_No_Gender_Unique.ENCRYPTED_TRUE_PEN = CRED_Extract_No_Gender_EPEN_with_MultiCred.ENCRYPTED_TRUE_PEN;"
dbExecute(con, qry07a2d_Update_MultiCredFlag)
dbExecute(con, "DROP TABLE CRED_Extract_No_Gender_EPEN_with_MultiCred")

## ---- Impute Missing Gender ----

# [SQL]

# ---- qry07b_GenderDistribution ---- 
qry07b_GenderDistribution <- "
SELECT psi_gender_cleaned AS PSI_GENDER, PSI_CREDENTIAL_CATEGORY, COUNT(*) AS Expr1
FROM Credential_Non_Dup
GROUP BY psi_gender_cleaned, PSI_CREDENTIAL_CATEGORY;"
d <- dbGetQuery(con, qry07b_GenderDistribution) %>% 
  mutate(Expr1 = replace_na(Expr1, 0))

nulls <- d %>% 
  filter(is.na(PSI_GENDER)) %>% 
  select(-PSI_GENDER)

f_d <- d %>% 
  filter(!is.na(PSI_GENDER)) %>% 
  group_by(PSI_CREDENTIAL_CATEGORY) %>% 
  mutate(p = Expr1/sum(Expr1)) %>% filter(PSI_GENDER == 'Female') %>% 
  select (-c(PSI_GENDER, Expr1))

m_d <- d %>% 
  filter(!is.na(PSI_GENDER)) %>% 
  group_by(PSI_CREDENTIAL_CATEGORY) %>% 
  mutate(p = Expr1/sum(Expr1)) %>% filter(PSI_GENDER == 'Male') %>% 
  select (-c(PSI_GENDER, Expr1))

top_nf <- inner_join(f_d, nulls) %>% 
  mutate(n = round(Expr1*p)) %>%
  select(PSI_CREDENTIAL_CATEGORY, n)

top_nm <- inner_join(m_d, nulls) %>% 
  mutate(n = round(Expr1*p)) %>%
  select(PSI_CREDENTIAL_CATEGORY, n)

top_nf
top_nm


## ---- STOP !! manually add top_nf to queries below ----
# then change queries and do the same for top_mf
# Code later: https://github.com/r-dbi/DBI/issues/193

# [UPDATE] TOP

# ---- qry07c10_Assign_TopID_GenderF_GradCert ---- 
qry07c10_Assign_TopID_GenderF_GradCert <- "
UPDATE TOP (1) CRED_Extract_No_Gender_Unique
SET PSI_GENDER_CLEANED = 'Female'
WHERE PSI_CREDENTIAL_CATEGORY = 'GRADUATE CERTIFICATE';
"
dbExecute(con, qry07c10_Assign_TopID_GenderF_GradCert)

# [UPDATE] TOP

# ---- qry07c11_Assign_TopID_GenderF_GradDipl ---- 
qry07c11_Assign_TopID_GenderF_GradDipl <- "
UPDATE TOP (101) CRED_Extract_No_Gender_Unique
SET PSI_GENDER_CLEANED = 'Female'
WHERE PSI_CREDENTIAL_CATEGORY = 'GRADUATE DIPLOMA';
"
dbExecute(con, qry07c11_Assign_TopID_GenderF_GradDipl)

# [UPDATE] TOP

# ---- qry07c12_Assign_TopID_GenderF_Masters ---- 
qry07c12_Assign_TopID_GenderF_Masters <- "
UPDATE TOP (457) CRED_Extract_No_Gender_Unique
SET PSI_GENDER_CLEANED = 'Female'
WHERE PSI_CREDENTIAL_CATEGORY = 'MASTERS DEGREE';
"
dbExecute(con, qry07c12_Assign_TopID_GenderF_Masters)

# [UPDATE] TOP

# ---- qry07c13_Assign_TopID_GenderF_PostDegCert ---- 
qry07c13_Assign_TopID_GenderF_PostDegCert <- "
UPDATE TOP (40) CRED_Extract_No_Gender_Unique
SET PSI_GENDER_CLEANED = 'Female'
WHERE PSI_CREDENTIAL_CATEGORY = 'POST-DEGREE CERTIFICATE';
"
dbExecute(con, qry07c13_Assign_TopID_GenderF_PostDegCert)

# [UPDATE] TOP

# ---- qry07c14_Assign_TopID_GenderF_PostDegDipl ---- 
qry07c14_Assign_TopID_GenderF_PostDegDipl <- "
UPDATE TOP (248) CRED_Extract_No_Gender_Unique
SET PSI_GENDER_CLEANED = 'Female'
WHERE PSI_CREDENTIAL_CATEGORY = 'POST-DEGREE DIPLOMA';
"
dbExecute(con, qry07c14_Assign_TopID_GenderF_PostDegDipl)

# [UPDATE] TOP

# ---- qry07c1_Assign_TopID_GenderF_AdvancedCert ---- 
qry07c1_Assign_TopID_GenderF_AdvancedCert <- "
UPDATE TOP (26) CRED_Extract_No_Gender_Unique
SET PSI_GENDER_CLEANED = 'Female'
WHERE PSI_CREDENTIAL_CATEGORY = 'ADVANCED CERTIFICATE';"
dbExecute(con, qry07c1_Assign_TopID_GenderF_AdvancedCert)

# [UPDATE] TOP

# ---- qry07c2_Assign_TopID_GenderF_AdvancedDip ---- 
qry07c2_Assign_TopID_GenderF_AdvancedDip <- "
UPDATE TOP (18) CRED_Extract_No_Gender_Unique
SET PSI_GENDER_CLEANED = 'Female'
WHERE PSI_CREDENTIAL_CATEGORY = 'ADVANCED DIPLOMA';
"
dbExecute(con, qry07c2_Assign_TopID_GenderF_AdvancedDip)

# [UPDATE] TOP

# ---- qry07c3_Assign_TopID_GenderF_Apprenticeship ---- 
qry07c3_Assign_TopID_GenderF_Apprenticeship <- "
UPDATE TOP (1) CRED_Extract_No_Gender_Unique
SET PSI_GENDER_CLEANED = 'Female'
WHERE PSI_CREDENTIAL_CATEGORY = 'APPRENTICESHIP';
"
dbExecute(con, qry07c3_Assign_TopID_GenderF_Apprenticeship)

# [UPDATE] TOP

# ---- qry07c4_Assign_TopID_GenderF_AssocDegree ---- 
qry07c4_Assign_TopID_GenderF_AssocDegree <- "
UPDATE TOP (30) CRED_Extract_No_Gender_Unique
SET PSI_GENDER_CLEANED = 'Female'
WHERE PSI_CREDENTIAL_CATEGORY = 'ASSOCIATE DEGREE';
"
dbExecute(con, qry07c4_Assign_TopID_GenderF_AssocDegree)

# [UPDATE] TOP

# ---- qry07c5_Assign_TopID_GenderF_Bachelor ---- 
qry07c5_Assign_TopID_GenderF_Bachelor <- "
UPDATE TOP (1643) CRED_Extract_No_Gender_Unique
SET PSI_GENDER_CLEANED = 'Female'
WHERE PSI_CREDENTIAL_CATEGORY = 'BACHELORS DEGREE';
"
dbExecute(con, qry07c5_Assign_TopID_GenderF_Bachelor)

# [UPDATE] TOP

# ---- qry07c6_Assign_TopID_GenderF_Certificate ---- 
qry07c6_Assign_TopID_GenderF_Certificate <- "
UPDATE TOP (795) CRED_Extract_No_Gender_Unique
SET PSI_GENDER_CLEANED = 'Female'
WHERE PSI_CREDENTIAL_CATEGORY = 'CERTIFICATE';
"
dbExecute(con, qry07c6_Assign_TopID_GenderF_Certificate)

# [UPDATE] TOP

# ---- qry07c7_Assign_TopID_GenderF_Diploma ---- 
qry07c7_Assign_TopID_GenderF_Diploma <- "
UPDATE TOP (465) CRED_Extract_No_Gender_Unique
SET PSI_GENDER_CLEANED = 'Female'
WHERE PSI_CREDENTIAL_CATEGORY = 'DIPLOMA';
"
dbExecute(con, qry07c7_Assign_TopID_GenderF_Diploma)

# [UPDATE] TOP

# ---- qry07c8_Assign_TopID_GenderF_Doctorate ---- 
qry07c8_Assign_TopID_GenderF_Doctorate <- "
UPDATE TOP (63) CRED_Extract_No_Gender_Unique
SET PSI_GENDER_CLEANED = 'Female'
WHERE PSI_CREDENTIAL_CATEGORY = 'DOCTORATE';
"
dbExecute(con, qry07c8_Assign_TopID_GenderF_Doctorate)

# [UPDATE] TOP

# ---- qry07c9_Assign_TopID_GenderF_FirstProfDeg ---- 
qry07c9_Assign_TopID_GenderF_FirstProfDeg <- "
UPDATE TOP (26) CRED_Extract_No_Gender_Unique
SET PSI_GENDER_CLEANED = 'Female'
WHERE PSI_CREDENTIAL_CATEGORY = 'FIRST PROFESSIONAL DEGREE';"
dbExecute(con, qry07c9_Assign_TopID_GenderF_FirstProfDeg)

# [UPDATE] CRED_Extract_No_Gender_Unique


# ---- qry07c_Assign_TopID_GenderM ---- 
qry07c_Assign_TopID_GenderM <- "
UPDATE CRED_Extract_No_Gender_Unique
SET PSI_GENDER_CLEANED = 'Gender Diverse'
WHERE PSI_GENDER_CLEANED NOT IN('Female','Male') OR PSI_GENDER_CLEANED IS NULL;
"
dbExecute(con, qry07c_Assign_TopID_GenderM)

# [UPDATE] CRED_Extract_No_Gender


# ---- qry07d_CorrectGender1 ---- 
qry07d_CorrectGender1 <- "
UPDATE CRED_Extract_No_Gender
SET PSI_GENDER_CLEANED = CRED_Extract_No_Gender_Unique.PSI_GENDER_CLEANED
FROM CRED_Extract_No_Gender_Unique
INNER JOIN CRED_Extract_No_Gender ON CRED_Extract_No_Gender_Unique.ENCRYPTED_TRUE_PEN = CRED_Extract_No_Gender.ENCRYPTED_TRUE_PEN;"
dbExecute(con, qry07d_CorrectGender1)

# [UPDATE] Credential_Non_Dup


# ---- qry07d_CorrectGender2 ---- 
qry07d_CorrectGender2 <- "
UPDATE    Credential_Non_Dup
SET       PSI_GENDER_CLEANED = CRED_Extract_No_Gender.PSI_GENDER_CLEANED
FROM      CRED_Extract_No_Gender 
INNER JOIN Credential_Non_Dup ON CRED_Extract_No_Gender.id = Credential_Non_Dup.id;"
dbExecute(con, qry07d_CorrectGender2)
dbExecute(con, "DROP TABLE CRED_Extract_No_Gender")
dbExecute(con, "DROP TABLE CRED_Extract_No_Gender_Unique")
dbExecute(con, "DROP VIEW Credential_Remove_Dup")
dbExecute(con, "DROP TABLE tmp_credential_epen_gender")
dbExecute(con, "DROP TABLE tmp_dup_credential_epen_gender")
dbExecute(con, "DROP TABLE tmp_dup_credential_epen_gender_maxcreddate")

# ---- 08 Credential Ranking ----

# [SELECT INTO] Create tmp_Credential_Ranking_step1 from Credential_Non_Dup


# ---- qry08_Create_Credential_Ranking_View a ---- 
qry08_Create_Credential_Ranking_View_a <-  
"SELECT        a.id, a.ENCRYPTED_TRUE_PEN, 
a.CREDENTIAL_AWARD_DATE_D, 
CredentialRank.RANK, 
a.Highest_Cred_by_Date, 
a.Highest_Cred_by_Rank, 
a.Highest_Cred_by_School_Year
INTO              tmp_Credential_Ranking_step1
FROM            Credential_Non_Dup AS a INNER JOIN
                         CredentialRank ON a.PSI_CREDENTIAL_CATEGORY = CredentialRank.PSI_CREDENTIAL_CATEGORY
WHERE        (a.ENCRYPTED_TRUE_PEN IN
                             (SELECT        ENCRYPTED_TRUE_PEN
                               FROM            Credential_Non_Dup AS b
                               GROUP BY ENCRYPTED_TRUE_PEN
                               HAVING         (COUNT(ENCRYPTED_TRUE_PEN) > 1) 
                               AND (ENCRYPTED_TRUE_PEN IS NOT NULL) 
AND (ENCRYPTED_TRUE_PEN NOT IN ('', ' ', '(Unspecified)'))))"
dbExecute(con, qry08_Create_Credential_Ranking_View_a)

# [SELECT INTO] Create tmp_CredentialNonDup_STUD_NUM_PSI_CODE_MoreThanOne from Credential_Non_Dup

# ---- qry08_Create_Credential_Ranking_View b ---- 
qry08_Create_Credential_Ranking_View_b <-  
"SELECT        a.id, a.ENCRYPTED_TRUE_PEN, a.PSI_STUDENT_NUMBER, a.psi_code, a.CREDENTIAL_AWARD_DATE_D, CredentialRank.RANK, a.Highest_Cred_by_Date, a.Highest_Cred_by_Rank, 
                         a.Highest_Cred_by_School_Year
INTO              tmp_CredentialNonDup_STUD_NUM_PSI_CODE_MoreThanOne
FROM            Credential_Non_Dup AS a INNER JOIN
                         CredentialRank ON a.PSI_CREDENTIAL_CATEGORY = CredentialRank.PSI_CREDENTIAL_CATEGORY
WHERE        (a.PSI_STUDENT_NUMBER IN
                             (SELECT        PSI_STUDENT_NUMBER
                               FROM            Credential_Non_Dup AS b
                               GROUP BY ENCRYPTED_TRUE_PEN,PSI_STUDENT_NUMBER
                               HAVING         (COUNT(PSI_STUDENT_NUMBER) > 1) AND ((ENCRYPTED_TRUE_PEN IS NULL) OR (ENCRYPTED_TRUE_PEN IN ('', ' ', '(Unspecified)')))))"
dbExecute(con, qry08_Create_Credential_Ranking_View_b)

# [SELECT INTO] Create tmp_Credential_Ranking_step2 from Credential_Non_Dup


# ---- qry08_Create_Credential_Ranking_View c ---- 
qry08_Create_Credential_Ranking_View_c <-    
"SELECT        a.id, a.ENCRYPTED_TRUE_PEN, a.PSI_STUDENT_NUMBER, a.PSI_CODE, a.CREDENTIAL_AWARD_DATE_D, CredentialRank.RANK, a.Highest_Cred_by_Date, 
                         a.Highest_Cred_by_Rank, a.Highest_Cred_by_School_Year
INTO              tmp_Credential_Ranking_step2
FROM            Credential_Non_Dup AS a INNER JOIN
                         CredentialRank ON a.PSI_CREDENTIAL_CATEGORY = CredentialRank.PSI_CREDENTIAL_CATEGORY INNER JOIN
                         tmp_CredentialNonDup_STUD_NUM_PSI_CODE_MoreThanOne ON 
                         a.PSI_STUDENT_NUMBER = tmp_CredentialNonDup_STUD_NUM_PSI_CODE_MoreThanOne.PSI_STUDENT_NUMBER AND 
                         a.PSI_CODE = tmp_CredentialNonDup_STUD_NUM_PSI_CODE_MoreThanOne.PSI_CODE AND 
                         a.ENCRYPTED_TRUE_PEN = tmp_CredentialNonDup_STUD_NUM_PSI_CODE_MoreThanOne.ENCRYPTED_TRUE_PEN"
dbExecute(con, qry08_Create_Credential_Ranking_View_c)

# [SELECT INTO] Create tmp_Credential_Ranking_step3 from tmp_Credential_Ranking_step1
                         
# ---- qry08_Create_Credential_Ranking_View d ---- 
qry08_Create_Credential_Ranking_View_d <-                            
"SELECT        id, ENCRYPTED_TRUE_PEN, CREDENTIAL_AWARD_DATE_D, RANK, Highest_Cred_by_Date, Highest_Cred_by_Rank, Highest_Cred_by_School_Year
INTO              tmp_Credential_Ranking_step3
FROM            tmp_Credential_Ranking_step1"
dbExecute(con, qry08_Create_Credential_Ranking_View_d)
dbExecute(con, "ALTER TABLE tmp_Credential_Ranking_step3 ADD PSI_STUDENT_NUMBER varchar(50)")
dbExecute(con, "ALTER TABLE tmp_Credential_Ranking_step3 ADD PSI_CODE varchar(50)")

# [INSERT INTO] tmp_Credential_Ranking_step3

# ---- qry08_Create_Credential_Ranking_View e ---- 
qry08_Create_Credential_Ranking_View_e <-   
"INSERT INTO tmp_Credential_Ranking_step3
                         (id, ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, CREDENTIAL_AWARD_DATE_D, RANK, Highest_Cred_by_Date, Highest_Cred_by_Rank, 
                         Highest_Cred_by_School_Year)
SELECT        id, ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, CREDENTIAL_AWARD_DATE_D, RANK, Highest_Cred_by_Date, Highest_Cred_by_Rank, 
                         Highest_Cred_by_School_Year
FROM            tmp_Credential_Ranking_step2"
dbExecute(con, qry08_Create_Credential_Ranking_View_e)

# [SQL]

# ---- qry08_Create_Credential_Ranking_View f ---- 
qry08_Create_Credential_Ranking_View_f <-   
"SELECT        id, ENCRYPTED_TRUE_PEN, CREDENTIAL_AWARD_DATE_D, RANK, Highest_Cred_by_Date, Highest_Cred_by_Rank, Highest_Cred_by_School_Year, 
                         PSI_STUDENT_NUMBER, PSI_CODE
FROM            tmp_Credential_Ranking_step3;"
dbExecute(con, qry08_Create_Credential_Ranking_View_f) 
dbExecute(con, "DROP TABLE tmp_Credential_Ranking_step1")
dbExecute(con, "DROP TABLE tmp_Credential_Ranking_step2")
dbExecute(con, "DROP TABLE tmp_CredentialNonDup_STUD_NUM_PSI_CODE_MoreThanOne")

# [SQL]

# ---- qry08_Create_Credential_Ranking_View_g----
qry08_Create_Credential_Ranking_View_g <- "
CREATE VIEW Credential_Ranking 
AS
SELECT  id, 
        ENCRYPTED_TRUE_PEN, 
        PSI_STUDENT_NUMBER, 
        PSI_CODE,
        CREDENTIAL_AWARD_DATE_D, 
        RANK, 
        Highest_Cred_by_Date, 
        Highest_Cred_by_Rank, 
        Highest_Cred_by_School_Year
FROM    tmp_Credential_Ranking_step3;"
dbExecute(con, qry08_Create_Credential_Ranking_View_g) 

res <- dbGetQuery(con, "SELECT DISTINCT id,
                        credential_ranking.encrypted_true_pen,
                        credential_ranking.psi_student_number,
                        credential_ranking.psi_code,
                        [encrypted_true_pen]+[psi_student_number] AS concatenated_id,
                        credential_ranking.credential_award_date_d,
                        credential_ranking.rank,
                        credential_ranking.highest_cred_by_date, 
                        credential_ranking.highest_cred_by_rank FROM credential_ranking")
names(res) <- tolower(names(res))

res <- res %>%  
  mutate(highest_cred_by_rank = NA) %>%  
  mutate(highest_cred_by_date = NA)

res <- res %>% 
  group_by(encrypted_true_pen, psi_student_number) %>% 
  arrange(encrypted_true_pen, psi_student_number, psi_code, desc(credential_award_date_d), rank, .by_group = TRUE) %>%
  mutate(highest_cred_by_date = replace(highest_cred_by_date, 1, 'Yes')) %>% 
  ungroup()

res <- res %>%  
  group_by(encrypted_true_pen, psi_student_number) %>% 
  arrange(encrypted_true_pen, psi_student_number, psi_code, rank, desc(credential_award_date_d), .by_group = TRUE) %>%
  mutate(highest_cred_by_rank = replace(highest_cred_by_rank, 1, 'Yes')) %>% 
  ungroup()

dbWriteTable(con, name = 'tmp_Credential_Ranking', res, overwrite = TRUE)

dbExecute(con, "ALTER TABLE tmp_credential_Ranking ALTER COLUMN id INT NOT NULL;")


# [UPDATE] Credential_Non_Dup


# ---- qry08a1_Update_CredentialNonDup_with_highestDate_Rank ----
qry08a1_Update_CredentialNonDup_with_highestDate_Rank <- "
UPDATE  Credential_Non_Dup
SET     Highest_Cred_by_Date = tmp_Credential_Ranking.Highest_Cred_by_Date, 
        Highest_Cred_by_Rank = tmp_Credential_Ranking.Highest_Cred_by_Rank
FROM    Credential_Non_Dup
INNER JOIN tmp_Credential_Ranking ON Credential_Non_Dup.id = tmp_Credential_Ranking.id;
"
dbExecute(con, qry08a1_Update_CredentialNonDup_with_highestDate_Rank)

# [UPDATE] Credential_Ranking

# ---- qry08a_Run_after_Credential_Ranking ----
qry08a_Run_after_Credential_Ranking <- "
UPDATE  Credential_Ranking
SET     Highest_Cred_by_Date = tmp_Credential_Ranking.Highest_Cred_by_Date, 
        Highest_Cred_by_Rank = tmp_Credential_Ranking.Highest_Cred_by_Rank
FROM    tmp_Credential_Ranking
INNER JOIN Credential_Ranking ON tmp_Credential_Ranking.id = Credential_Ranking.id;
"
dbExecute(con, qry08a_Run_after_Credential_Ranking)

# [UPDATE] Credential_Non_Dup

# ---- qry08b_Rank_non_multi_cred ----
qry08b_Rank_non_multi_cred <- "
UPDATE Credential_Non_Dup
SET Credential_Non_Dup.Highest_Cred_by_Date = 'Yes', 
    Credential_Non_Dup.Highest_Cred_by_Rank = 'Yes'
WHERE NOT EXISTS (
SELECT * FROM tmp_Credential_Ranking
WHERE tmp_Credential_Ranking.ID = Credential_Non_Dup.ID)"
dbExecute(con, qry08b_Rank_non_multi_cred)
dbExecute(con, "DROP TABLE tmp_Credential_Ranking")
dbExecute(con, "DROP TABLE tmp_Credential_Ranking_step3")
dbExecute(con, "DROP VIEW Credential_Ranking")

# ---- 09 Age Gender Distributions ----

# [SELECT INTO] Create CRED_Extract_No_Age from Credential_Non_Dup


# ---- qry09a_ExtractNoAge ----
qry09a_ExtractNoAge <- "
SELECT  id, ENCRYPTED_TRUE_PEN, AGE_AT_GRAD, psi_gender_cleaned, PSI_AWARD_SCHOOL_YEAR, 
PSI_CREDENTIAL_CATEGORY, CREDENTIAL_AWARD_DATE_D, 0 AS LASTCRED, HIGHEST_CRED_BY_DATE
INTO    CRED_Extract_No_Age
FROM    Credential_Non_Dup
WHERE   (AGE_AT_GRAD IS NULL);
"
dbExecute(con, qry09a_ExtractNoAge) 
dbExecute(con, "ALTER TABLE CRED_Extract_No_Age ADD PRIMARY KEY (id);")

# [SELECT INTO] Create CRED_Extract_No_Age_Unique from Credential_Non_Dup

# ---- qry09b_ExtractNoAgeUnique ----
qry09b_ExtractNoAgeUnique <- "
SELECT  id, ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, AGE_AT_GRAD, PSI_GENDER_CLEANED, 
PSI_CREDENTIAL_CATEGORY, CREDENTIAL_AWARD_DATE_D, PSI_AWARD_SCHOOL_YEAR
INTO    CRED_Extract_No_Age_Unique
FROM    Credential_Non_Dup
WHERE   (AGE_AT_GRAD IS NULL) AND (Highest_Cred_by_Date = 'Yes');
"
dbExecute(con, qry09b_ExtractNoAgeUnique)
dbExecute(con, "ALTER TABLE CRED_Extract_No_Age_Unique ADD PRIMARY KEY (id);")

# [SQL]

sql <- "SELECT PSI_GENDER_CLEANED, PSI_CREDENTIAL_CATEGORY, COUNT(*) AS NumWithNullAge
FROM CRED_Extract_No_Age_Unique GROUP BY PSI_GENDER_CLEANED, PSI_CREDENTIAL_CATEGORY"
CRED_Extract_No_Age_Unique <- dbGetQuery(con, sql)

# [SQL]

# ---- qry09d_ShowAgeGenderDistribution ---- 
qry09d_ShowAgeGenderDistribution <- "
SELECT     PSI_GENDER_CLEANED, AGE_AT_GRAD, PSI_CREDENTIAL_CATEGORY, COUNT(*) AS NumGrads
FROM         Credential_Non_Dup
WHERE     (AGE_GROUP_AT_GRAD IS NOT NULL) AND (Highest_Cred_by_Date = 'Yes')
GROUP BY   PSI_GENDER_CLEANED, AGE_AT_GRAD, PSI_CREDENTIAL_CATEGORY;"
CREDAgeDistributionbyGender <- dbGetQuery(con, qry09d_ShowAgeGenderDistribution)

d <- CREDAgeDistributionbyGender %>% 
  group_by(PSI_GENDER_CLEANED, PSI_CREDENTIAL_CATEGORY) %>%
  mutate(p = NumGrads/sum(NumGrads)) %>%
  left_join(CRED_Extract_No_Age_Unique, 
            by = join_by(PSI_GENDER_CLEANED, PSI_CREDENTIAL_CATEGORY)) %>%
  mutate(n = round(p*NumWithNullAge)) %>% 
  arrange(PSI_GENDER_CLEANED, PSI_CREDENTIAL_CATEGORY, AGE_AT_GRAD) %>%
  filter(!is.na(NumWithNullAge))

# consider sampling instead to ensure randomness and give full coverage
print("imputing missing age_at_grad ....")
for (i in 1:nrow(d)) {
  sql <- "UPDATE TOP(?n) CRED_Extract_No_Age_Unique
          SET AGE_AT_GRAD = ?age 
          WHERE PSI_GENDER_CLEANED  = ?gender
            AND PSI_CREDENTIAL_CATEGORY = ?cred
            AND (AGE_AT_GRAD IS NULL OR AGE_AT_GRAD = ' ');"
  sql <- sqlInterpolate(con, sql, 
                        n = as.numeric(d[i,"n"]), 
                        age = as.numeric(d[i,"AGE_AT_GRAD"]), 
                        gender = as.character(d[i,"PSI_GENDER_CLEANED"]), 
                        cred = as.character(d[i,"PSI_CREDENTIAL_CATEGORY"]))
  dbExecute(con, sql)
}
print("....done")

# assign a random age between 19 and 54 to any remaining nulls. 
dbExecute(con, "UPDATE CRED_Extract_No_Age_Unique
                SET AGE_AT_GRAD = (ABS(CHECKSUM(NewId())) % 35) + 19
                WHERE AGE_AT_GRAD IS NULL OR AGE_AT_GRAD = ' '")

# See documentation at this point for further processing of multiple credentials


# [UPDATE] CRED_Extract_No_Age


# ---- qry10_Update_Extract_No_Age ---- 
qry10_Update_Extract_No_Age <- "
UPDATE    CRED_Extract_No_Age
SET       AGE_AT_GRAD = CRED_Extract_No_Age_Unique.AGE_AT_GRAD
FROM      CRED_Extract_No_Age_Unique
INNER JOIN CRED_Extract_No_Age 
ON CRED_Extract_No_Age_Unique.id = CRED_Extract_No_Age.id;"
dbExecute(con, qry10_Update_Extract_No_Age)

# [UPDATE] Credential_Non_Dup

# ---- qry11a_UpdateAgeAtGrad ---- 
qry11a_UpdateAgeAtGrad <- "
UPDATE    Credential_Non_Dup
SET       AGE_AT_GRAD = CRED_Extract_No_Age.AGE_AT_GRAD
FROM      CRED_Extract_No_Age INNER JOIN
          Credential_Non_Dup ON CRED_Extract_No_Age.id = Credential_Non_Dup.id;"
dbExecute(con, qry11a_UpdateAgeAtGrad)

# [UPDATE] Credential_Non_Dup

# ---- qry11b_UpdateAGAtGrad ---- 
qry11b_UpdateAGAtGrad <- "
UPDATE    Credential_Non_Dup
SET       AGE_GROUP_AT_GRAD = AgeGroupLookup.AgeIndex
FROM      Credential_Non_Dup 
INNER JOIN AgeGroupLookup 
ON Credential_Non_Dup.AGE_AT_GRAD >= AgeGroupLookup.LowerBound AND Credential_Non_Dup.AGE_AT_GRAD <= AgeGroupLookup.UpperBound;"
dbExecute(con, qry11b_UpdateAGAtGrad)
dbExecute(con, "DROP TABLE CRED_Extract_No_Age")
dbExecute(con, "DROP TABLE CRED_Extract_No_Age_Unique")
#dbExecute(con, "DROP TABLE CREDAgeDistributionbyGender")

# ---- VISA Status ----
dbExecute(con, "ALTER TABLE CredentialSupVars ADD PSI_VISA_STATUS varchar(50)")
dbExecute(con, "ALTER TABLE Credential_Non_Dup ADD PSI_VISA_STATUS varchar(50)")

# [SQL]

# ---- CredentialSupVars_VisaStatus_Cleaning_check ----
CredentialSupVars_VisaStatus_Cleaning_check <-"
SELECT PSI_VISA_STATUS, count(*) FROM CredentialSupVars GROUP BY PSI_VISA_STATUS"
dbGetQuery(con, CredentialSupVars_VisaStatus_Cleaning_check)

# [SELECT INTO] Create credential_non_dup_visastatus_cleaning_step1 from credentialsupvars

# ---- CredentialSupVars_VisaStatus_1 ----
CredentialSupVars_VisaStatus_Cleaning_1 <-
"SELECT credential_non_dup.id,
       credential_non_dup.encrypted_true_pen,
       credential_non_dup.psi_student_number,
       credential_non_dup.psi_school_year,
       credential_non_dup.psi_code,
       credential_non_dup.credential_award_date,
       credential_non_dup.psi_program_code,
       credential_non_dup.psi_credential_program_description,
       credential_non_dup.psi_credential_category,
       credential_non_dup.psi_credential_level,
       credential_non_dup.psi_credential_cip,
       credential_non_dup.psi_award_school_year,
       credentialsupvars.psi_visa_status
INTO   credential_non_dup_visastatus_cleaning_step1
FROM   credentialsupvars
       INNER JOIN credential_non_dup
               ON credentialsupvars.id = credential_non_dup.id "
dbExecute(con, CredentialSupVars_VisaStatus_Cleaning_1)

# [UPDATE] Credential_Non_Dup_VisaStatus_Cleaning_Step1

# ---- CredentialSupVars_VisaStatus_2 ----
CredentialSupVars_VisaStatus_Cleaning_2 <-"
UPDATE  Credential_Non_Dup_VisaStatus_Cleaning_Step1
SET     PSI_VISA_STATUS = CredentialSupVarsFromEnrolment.PSI_VISA_STATUS
FROM    Credential_Non_Dup_VisaStatus_Cleaning_Step1
INNER JOIN CredentialSupVarsFromEnrolment
		ON  Credential_Non_Dup_VisaStatus_Cleaning_Step1.ENCRYPTED_TRUE_PEN = CredentialSupVarsFromEnrolment.ENCRYPTED_TRUE_PEN
AND     Credential_Non_Dup_VisaStatus_Cleaning_Step1.PSI_CODE = CredentialSupVarsFromEnrolment.PSI_CODE
AND     Credential_Non_Dup_VisaStatus_Cleaning_Step1.PSI_STUDENT_NUMBER = CredentialSupVarsFromEnrolment.PSI_STUDENT_NUMBER
AND     Credential_Non_Dup_VisaStatus_Cleaning_Step1.PSI_PROGRAM_CODE = CredentialSupVarsFromEnrolment.PSI_PROGRAM_CODE
AND     Credential_Non_Dup_VisaStatus_Cleaning_Step1.PSI_CREDENTIAL_PROGRAM_DESCRIPTION = CredentialSupVarsFromEnrolment.PSI_CREDENTIAL_PROGRAM_DESCRIPTION
AND     Credential_Non_Dup_VisaStatus_Cleaning_Step1.PSI_SCHOOL_YEAR = CredentialSupVarsFromEnrolment.PSI_SCHOOL_YEAR"
dbExecute(con, CredentialSupVars_VisaStatus_Cleaning_2)

# [UPDATE] credential_non_dup_visastatus_cleaning_step1

# ---- CredentialSupVars_VisaStatus_3 ----
CredentialSupVars_VisaStatus_Cleaning_3 <-"
UPDATE  credential_non_dup_visastatus_cleaning_step1
SET     psi_visa_status = credentialsupvarsfromenrolment.psi_visa_status
FROM    credential_non_dup_visastatus_cleaning_step1
INNER JOIN credentialsupvarsfromenrolment
ON      credential_non_dup_visastatus_cleaning_step1.encrypted_true_pen = credentialsupvarsfromenrolment.encrypted_true_pen
AND     credential_non_dup_visastatus_cleaning_step1.psi_code = credentialsupvarsfromenrolment.psi_code
AND     credential_non_dup_visastatus_cleaning_step1.psi_student_number = credentialsupvarsfromenrolment.psi_student_number
AND     credential_non_dup_visastatus_cleaning_step1.psi_program_code = credentialsupvarsfromenrolment.psi_program_code
AND     credential_non_dup_visastatus_cleaning_step1.psi_credential_program_description = credentialsupvarsfromenrolment.psi_credential_program_description
AND     credential_non_dup_visastatus_cleaning_step1.psi_school_year = credentialsupvarsfromenrolment.psi_school_year "
dbExecute(con, CredentialSupVars_VisaStatus_Cleaning_3)

# [UPDATE] Credential_Non_Dup_VisaStatus_Cleaning_Step1


# ---- CredentialSupVars_VisaStatus_4 ----
CredentialSupVars_VisaStatus_Cleaning_4 <-"
UPDATE    Credential_Non_Dup_VisaStatus_Cleaning_Step1
SET       PSI_VISA_STATUS = CredentialSupVarsFromEnrolment.PSI_VISA_STATUS
FROM      Credential_Non_Dup_VisaStatus_Cleaning_Step1
INNER JOIN CredentialSupVarsFromEnrolment
ON        Credential_Non_Dup_VisaStatus_Cleaning_Step1.ENCRYPTED_TRUE_PEN = CredentialSupVarsFromEnrolment.ENCRYPTED_TRUE_PEN AND
          Credential_Non_Dup_VisaStatus_Cleaning_Step1.PSI_CODE = CredentialSupVarsFromEnrolment.PSI_CODE AND
          Credential_Non_Dup_VisaStatus_Cleaning_Step1.PSI_STUDENT_NUMBER= CredentialSupVarsFromEnrolment.PSI_STUDENT_NUMBER AND
          Credential_Non_Dup_VisaStatus_Cleaning_Step1.PSI_SCHOOL_YEAR = CredentialSupVarsFromEnrolment.PSI_SCHOOL_YEAR
WHERE     (Credential_Non_Dup_VisaStatus_Cleaning_Step1.PSI_VISA_STATUS IS NULL)"
dbExecute(con, CredentialSupVars_VisaStatus_Cleaning_4)

# [UPDATE] CredentialSupVars

# ---- CredentialSupVars_VisaStatus_5 ----
CredentialSupVars_VisaStatus_Cleaning_5 <-"
UPDATE    CredentialSupVars
SET       PSI_VISA_STATUS = Credential_Non_Dup_VisaStatus_Cleaning_Step1.PSI_VISA_STATUS
FROM      CredentialSupVars
INNER JOIN Credential_Non_Dup_VisaStatus_Cleaning_Step1 ON CredentialSupVars.ID = Credential_Non_Dup_VisaStatus_Cleaning_Step1.id
WHERE     (CredentialSupVars.PSI_VISA_STATUS IS NULL)
OR        (CredentialSupVars.PSI_VISA_STATUS IN ('', ' ', '(Unspecified)'))"
dbExecute(con, CredentialSupVars_VisaStatus_Cleaning_5)

# [UPDATE] Credential_Non_Dup


# ---- CredentialSupVars_VisaStatus_6 ----
CredentialSupVars_VisaStatus_Cleaning_6 <-"
UPDATE      Credential_Non_Dup
SET         PSI_VISA_STATUS = CredentialSupVars.PSI_VISA_STATUS
FROM        Credential_Non_Dup
INNER JOIN  CredentialSupVars ON Credential_Non_Dup.id = CredentialSupVars.ID"
dbExecute(con, CredentialSupVars_VisaStatus_Cleaning_6)
dbGetQuery(con, CredentialSupVars_VisaStatus_Cleaning_check)
dbExecute(con, "DROP TABLE CredentialSupVars_VisaStatus_Cleaning_Step2")
dbExecute(con, "DROP TABLE Credential_Non_Dup_VisaStatus_Cleaning_Step1")
dbExecute(con, "DROP TABLE CredentialSupVars_VisaStatus_Cleaning_Step1")

# ---- Highest Rank ----
dbExecute(con, "ALTER TABLE Credential_Non_Dup ADD CONCATENATED_ID VARCHAR(255) NULL")
dbExecute(con, "UPDATE Credential_Non_Dup SET CONCATENATED_ID = ENCRYPTED_TRUE_PEN 
                 WHERE (ENCRYPTED_TRUE_PEN IS NOT NULL AND ENCRYPTED_TRUE_PEN NOT IN ('', ' ', '(Unspecified)'))")
dbExecute(con, "UPDATE Credential_Non_Dup SET CONCATENATED_ID = PSI_STUDENT_NUMBER + PSI_CODE 
                WHERE (ENCRYPTED_TRUE_PEN IS NULL) OR (ENCRYPTED_TRUE_PEN IN ('', ' ', '(Unspecified)'))")

# [SQL]

# ---- qry12_Create_View_tblCredentialHighestRank ----
qry12_Create_View_tblCredentialHighestRank <- "
CREATE VIEW tblCredential_HighestRank AS
SELECT    Credential_Non_Dup.id, 
          -- Credential_Non_Dup.PSI_PEN,
          Credential_Non_Dup.psi_birthdate_cleaned, 
          Credential_Non_Dup.psi_gender_cleaned, 
          Credential_Non_Dup.ENCRYPTED_TRUE_PEN, 
          Credential_Non_Dup.PSI_STUDENT_NUMBER,
          Credential_Non_Dup.PSI_SCHOOL_YEAR,
          Credential_Non_Dup.PSI_CODE, 
          Credential_Non_Dup.CREDENTIAL_AWARD_DATE, 
          Credential_Non_Dup.RecordStatus, 
          Credential_Non_Dup.PSI_PROGRAM_CODE, 
          Credential_Non_Dup.PSI_CREDENTIAL_PROGRAM_DESCRIPTION, 
          Credential_Non_Dup.PSI_CREDENTIAL_CIP, 
          Credential_Non_Dup.PSI_CREDENTIAL_LEVEL, 
          Credential_Non_Dup.PSI_CREDENTIAL_CATEGORY, 
          Credential_Non_Dup.CREDENTIAL_AWARD_DATE_D, 
          Credential_Non_Dup.AGE_AT_GRAD, Credential_Non_Dup.AGE_GROUP_AT_GRAD, 
          Credential_Non_Dup.psi_birthdate_cleaned_D,
		      Credential_Non_Dup.PSI_AWARD_SCHOOL_YEAR, 
          Credential_Non_Dup.RECORD_TO_DELETE, 
          Credential_Non_Dup.Last_Date_Highest_Cred, 
          Credential_Non_Dup.Highest_Cred_by_Date, 
          Credential_Non_Dup.Highest_Cred_by_Rank, 
          Credential_Non_Dup.OUTCOMES_CRED, 
          Credential_Non_Dup.Highest_Cred_by_School_Year, 
          Credential_Non_Dup.RESEARCH_UNIVERSITY, 
          Credential_Non_Dup.CONCATENATED_ID,
          CredentialSupVars.CREDENTIAL_AWARD_DATE_D_DELAYED, 
          CredentialSupVars.PSI_AWARD_SCHOOL_YEAR_DELAYED, 
          CredentialSupVars.PSI_VISA_STATUS
FROM      Credential_Non_Dup INNER JOIN CredentialSupVars 
  ON      Credential_Non_Dup.id = CredentialSupVars.ID
WHERE     Credential_Non_Dup.Highest_Cred_by_Rank = 'Yes'"
dbExecute(con, qry12_Create_View_tblCredentialHighestRank)


# [SELECT INTO] Create tblcredential_laterawarded


# ---- qry18a_ExtrLaterAwarded ----
qry18a_ExtrLaterAwarded <-
  "SELECT DISTINCT credential_non_dup.id  AS LID,
                tblcredential_highestrank.id AS HID,
                credential_non_dup.concatenated_id,
                credential_non_dup.credential_award_date_d AS LATER_AWARD_DATE,
                credential_non_dup.highest_cred_by_date,
                credential_non_dup.psi_award_school_year,
                tblcredential_highestrank.credential_award_date_d AS HIGHEST_AWARD_DATE,
                credential_non_dup.psi_credential_category,
                credentialrank.rank
INTO   tblcredential_laterawarded
FROM   (credential_non_dup
        INNER JOIN credentialrank
                ON credential_non_dup.psi_credential_category =
                   credentialrank.psi_credential_category)
       INNER JOIN tblcredential_highestrank
               ON credential_non_dup.concatenated_id = tblcredential_highestrank.concatenated_id
WHERE  (( ( credential_non_dup.credential_award_date_d ) >
                  [tblcredential_highestrank].[credential_award_date_d]))"
dbExecute(con, qry18a_ExtrLaterAwarded)

# [SELECT INTO] Create tmp_qry18b_ExtrLaterAwarded from tblCredential_HighestRank

# ---- qry18b_ExtrLaterAwarded ----
qry18b_ExtrLaterAwarded <-
  "SELECT tblCredential_LaterAwarded.LID,
        tblCredential_LaterAwarded.HID,
        tblCredential_LaterAwarded.concatenated_id,
        tblCredential_LaterAwarded.LATER_AWARD_DATE,
        tblCredential_LaterAwarded.PSI_AWARD_SCHOOL_YEAR
INTO tmp_qry18b_ExtrLaterAwarded
FROM tblCredential_HighestRank
INNER JOIN tblCredential_LaterAwarded
ON tblCredential_HighestRank.concatenated_id = tblCredential_LaterAwarded.concatenated_id
WHERE (((tblCredential_LaterAwarded.PSI_CREDENTIAL_CATEGORY)='APPRENTICESHIP'
     Or (tblCredential_LaterAwarded.PSI_CREDENTIAL_CATEGORY)='BACHELORS DEGREE'
     Or (tblCredential_LaterAwarded.PSI_CREDENTIAL_CATEGORY)='FIRST PROFESSIONAL DEGREE'))
OR (((tblCredential_LaterAwarded.PSI_CREDENTIAL_CATEGORY)='ADVANCED DIPLOMA'
  Or (tblCredential_LaterAwarded.PSI_CREDENTIAL_CATEGORY)='ADVANCED CERTIFICATE')
AND ((DateDiff(month,[tblCredential_HighestRank].[CREDENTIAL_AWARD_DATE_D],[tblCredential_LaterAwarded].[LATER_AWARD_DATE]))<=36))
OR (((tblCredential_LaterAwarded.PSI_CREDENTIAL_CATEGORY)='ASSOCIATE DEGREE')
AND ((DateDiff(month,[tblCredential_HighestRank].[CREDENTIAL_AWARD_DATE_D],[tblCredential_LaterAwarded].[LATER_AWARD_DATE]))<=18))
OR (((tblCredential_LaterAwarded.PSI_CREDENTIAL_CATEGORY)='CERTIFICATE')
AND ((DateDiff(month,[tblCredential_HighestRank].[CREDENTIAL_AWARD_DATE_D],[tblCredential_LaterAwarded].[LATER_AWARD_DATE]))<=18))
OR (((tblCredential_LaterAwarded.PSI_CREDENTIAL_CATEGORY)='DIPLOMA')
AND ((DateDiff(month,[tblCredential_HighestRank].[CREDENTIAL_AWARD_DATE_D],[tblCredential_LaterAwarded].[LATER_AWARD_DATE]))<=30))
OR (((tblCredential_LaterAwarded.PSI_CREDENTIAL_CATEGORY)='MASTERS DEGREE')
AND ((DateDiff(month,[tblCredential_HighestRank].[CREDENTIAL_AWARD_DATE_D],[tblCredential_LaterAwarded].[LATER_AWARD_DATE]))<=30))
OR (((tblCredential_LaterAwarded.PSI_CREDENTIAL_CATEGORY)='GRADUATE CERTIFICATE')
AND ((DateDiff(month,[tblCredential_HighestRank].[CREDENTIAL_AWARD_DATE_D],[tblCredential_LaterAwarded].[LATER_AWARD_DATE]))<=18))
OR (((tblCredential_LaterAwarded.PSI_CREDENTIAL_CATEGORY)='GRADUATE DIPLOMA')
AND ((DateDiff(month,[tblCredential_HighestRank].[CREDENTIAL_AWARD_DATE_D],[tblCredential_LaterAwarded].[LATER_AWARD_DATE]))<=30))
OR (((tblCredential_LaterAwarded.PSI_CREDENTIAL_CATEGORY)='POST-DEGREE CERTIFICATE')
AND ((DateDiff(month,[tblCredential_HighestRank].[CREDENTIAL_AWARD_DATE_D],[tblCredential_LaterAwarded].[LATER_AWARD_DATE]))<=18))
OR (((tblCredential_LaterAwarded.PSI_CREDENTIAL_CATEGORY)='POST-DEGREE DIPLOMA')
AND ((DateDiff(month,[tblCredential_HighestRank].[CREDENTIAL_AWARD_DATE_D],[tblCredential_LaterAwarded].[LATER_AWARD_DATE]))<=30));"
dbExecute(con, qry18b_ExtrLaterAwarded)

# [SELECT INTO] Create tmp_qry18c_ExtrLaterAwarded from tmp_qry18b_ExtrLaterAwarded

# ---- qry18c_ExtrLaterAwarded ----
qry18c_ExtrLaterAwarded <-
  "SELECT tmp_qry18b_ExtrLaterAwarded.concatenated_id,
Max(tmp_qry18b_ExtrLaterAwarded.LATER_AWARD_DATE) AS MaxOfLATER_AWARD_DATE
INTO tmp_qry18c_ExtrLaterAwarded
FROM tmp_qry18b_ExtrLaterAwarded
GROUP BY tmp_qry18b_ExtrLaterAwarded.concatenated_id;"
dbExecute(con, qry18c_ExtrLaterAwarded)

# [SELECT INTO] Create tblCredential_DelayEffect from tmp_qry18b_ExtrLaterAwarded

# ---- qry18d_ExtrLaterAwarded ----
qry18d_ExtrLaterAwarded <-
  "SELECT DISTINCT Min(tmp_qry18b_ExtrLaterAwarded.LID) AS LID,
        tmp_qry18b_ExtrLaterAwarded.HID,
        tmp_qry18b_ExtrLaterAwarded.concatenated_id,
        tmp_qry18b_ExtrLaterAwarded.LATER_AWARD_DATE,
        tmp_qry18b_ExtrLaterAwarded.PSI_AWARD_SCHOOL_YEAR
INTO    tblCredential_DelayEffect
FROM    tmp_qry18b_ExtrLaterAwarded
INNER JOIN tmp_qry18c_ExtrLaterAwarded
  ON    tmp_qry18b_ExtrLaterAwarded.LATER_AWARD_DATE = tmp_qry18c_ExtrLaterAwarded.MaxOfLATER_AWARD_DATE
  AND   tmp_qry18b_ExtrLaterAwarded.concatenated_id = tmp_qry18c_ExtrLaterAwarded.concatenated_id
GROUP BY
        tmp_qry18b_ExtrLaterAwarded.HID,
        tmp_qry18b_ExtrLaterAwarded.concatenated_id,
        tmp_qry18b_ExtrLaterAwarded.LATER_AWARD_DATE,
        tmp_qry18b_ExtrLaterAwarded.PSI_AWARD_SCHOOL_YEAR;"
dbExecute(con, qry18d_ExtrLaterAwarded)
dbExecute(con, "DROP TABLE tmp_qry18b_ExtrLaterAwarded_2")
dbExecute(con, "DROP TABLE tmp_qry18c_ExtrLaterAwarded_3")
dbExecute(con, "DROP TABLE tblcredential_laterawarded")

# ---- 13 Delay Date ----

# [UPDATE] tblCredential_HighestRank

# ---- qry19_UpdateDelayDate ----
qry19_UpdateDelayDate <- "
UPDATE  tblCredential_HighestRank
SET     tblCredential_HighestRank.CREDENTIAL_AWARD_DATE_D_DELAYED = tblCredential_DelayEffect.LATER_AWARD_DATE,
        tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED = tblCredential_DelayEffect.PSI_AWARD_SCHOOL_YEAR
FROM    tblCredential_HighestRank
INNER JOIN tblCredential_DelayEffect
ON tblCredential_HighestRank.ID = tblCredential_DelayEffect.HID;"
dbExecute(con, qry19_UpdateDelayDate)
dbExecute(con, "DROP TABLE tblCredential_DelayEffect")

dbExecute(con, "ALTER TABLE Credential_Non_Dup 
                ADD CREDENTIAL_AWARD_DATE_D_DELAYED date, 
                PSI_AWARD_SCHOOL_YEAR_DELAYED varchar(50);")


# [UPDATE] Credential_Non_Dup


# ---- qry13a_UpdateDelayedCredDate ---- 
qry13a_UpdateDelayedCredDate <- "
UPDATE    Credential_Non_Dup
SET              CREDENTIAL_AWARD_DATE_D_DELAYED = tblCredential_HighestRank.CREDENTIAL_AWARD_DATE_D_DELAYED, 
                      PSI_AWARD_SCHOOL_YEAR_DELAYED = tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED
FROM         tblCredential_HighestRank INNER JOIN
                      Credential_Non_Dup ON tblCredential_HighestRank.id = Credential_Non_Dup.id
WHERE     (tblCredential_HighestRank.CREDENTIAL_AWARD_DATE_D_DELAYED IS NOT NULL) AND 
                      (tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED IS NOT NULL);"
dbExecute(con, qry13a_UpdateDelayedCredDate)

# [UPDATE] Credential_Non_Dup

# ---- qry13b_UpdateDelayedCredDate ---- 
qry13b_UpdateDelayedCredDate <- "
UPDATE    Credential_Non_Dup
SET       CREDENTIAL_AWARD_DATE_D_DELAYED = CREDENTIAL_AWARD_DATE_D, 
          PSI_AWARD_SCHOOL_YEAR_DELAYED = PSI_AWARD_SCHOOL_YEAR
WHERE     (CREDENTIAL_AWARD_DATE_D_DELAYED IS NULL);"
dbExecute(con, qry13b_UpdateDelayedCredDate)

# [UPDATE] tblCredential_HighestRank

# ---- qry13_UpdateDelayedCredDate ---- 
qry13_UpdateDelayedCredDate <- "
UPDATE  tblCredential_HighestRank
SET     CREDENTIAL_AWARD_DATE_D_DELAYED = CREDENTIAL_AWARD_DATE_D, 
        PSI_AWARD_SCHOOL_YEAR_DELAYED = PSI_AWARD_SCHOOL_YEAR
WHERE     (CREDENTIAL_AWARD_DATE_D_DELAYED IS NULL);"
dbExecute(con, qry13_UpdateDelayedCredDate)

# ---- 14-15 research University + Outcomes Credential ----

# [UPDATE] Credential_Non_Dup


# ---- qry14_ResearchUniversity ---- 
qry14_ResearchUniversity <- "UPDATE    Credential_Non_Dup
SET              RESEARCH_UNIVERSITY = 1
WHERE     (PSI_CODE = 'SFU') OR
                      (PSI_CODE = 'UBC') OR
					  (PSI_CODE = 'UBCV') OR
                      (PSI_CODE = 'UBCO') OR
                      (PSI_CODE = 'UNBC') OR
                      (PSI_CODE = 'UVIC') OR
                      (PSI_CODE = 'RRU');"
dbExecute(con, qry14_ResearchUniversity)

# [UPDATE] Credential_Non_Dup


# ---- qry15_OutcomeCredential ---- 
qry15_OutcomeCredential <- "UPDATE    Credential_Non_Dup
SET              OUTCOMES_CRED = OutcomeCredential.Outcomes_CRED
FROM         Credential_Non_Dup INNER JOIN
                      OutcomeCredential ON Credential_Non_Dup.PSI_CREDENTIAL_CATEGORY = OutcomeCredential.PSI_CREDENTIAL_CATEGORY;"
dbExecute(con, qry15_OutcomeCredential)

# update non-dup table here

# ---- Break and do Program Matching ----
# IMPORTANT!!! THIS SECTION CAN ONLY BE RUN AFTER THE PROGRAM MATCHING WORK HAS BEEN DONE 
# TODO: This will later be moved to a different script. 
# Tables that are needed at this point include:

# ---- Check Required Tables ----
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."credential_non_dup"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."AgeGroupLookup"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."tblCredential_HighestRank"')))

# ---- 20 Final Distributions ----
# NOTE: Exclude_CIPs queries end up with Invalid column name 'FINAL_CIP_CLUSTER_CODE'. 

# [SELECT INTO] Create Credential_By_Year_AgeGroup from tblCredential_HighestRank


# ---- qry20a_1Credential_By_Year_AgeGroup ---- 
qry20a_1Credential_By_Year_AgeGroup <- "
SELECT        AgeGroupLookup.AgeGroup, tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY, 
                         tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY + AgeGroupLookup.AgeGroup AS Expr1, 
                         tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED, COUNT(*) AS Count
INTO Credential_By_Year_AgeGroup
FROM            tblCredential_HighestRank INNER JOIN
                         AgeGroupLookup ON tblCredential_HighestRank.AGE_GROUP_AT_GRAD = AgeGroupLookup.AgeIndex
GROUP BY AgeGroupLookup.AgeGroup, tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY, tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED
HAVING        (tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY <> 'APPRENTICESHIP')
ORDER BY AgeGroupLookup.AgeGroup, tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY, tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED;"
dbExecute(con, qry20a_1Credential_By_Year_AgeGroup)

# [SELECT INTO] Create Credential_By_Year_AgeGroup_Exclude_CIPs from tblCredential_HighestRank


# ---- qry20a_1Credential_By_Year_AgeGroup_Exclude_CIPs ---- 
qry20a_1Credential_By_Year_AgeGroup_Exclude_CIPs <- "
SELECT        AgeGroupLookup.AgeGroup, tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY, 
                         tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY + AgeGroupLookup.AgeGroup AS Expr1, 
                         tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED, COUNT(*) AS Count
INTO Credential_By_Year_AgeGroup_Exclude_CIPs
FROM            tblCredential_HighestRank INNER JOIN
                         AgeGroupLookup ON tblCredential_HighestRank.AGE_GROUP_AT_GRAD = AgeGroupLookup.AgeIndex INNER JOIN
                         Credential_Non_Dup ON tblCredential_HighestRank.id = Credential_Non_Dup.id
WHERE        (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '09' AND Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '10')
GROUP BY AgeGroupLookup.AgeGroup, tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY, tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED
HAVING        (tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY <> 'APPRENTICESHIP')
ORDER BY AgeGroupLookup.AgeGroup, tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY, tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED;"
dbExecute(con, qry20a_1Credential_By_Year_AgeGroup_Exclude_CIPs)

# [SELECT INTO] Create Credential_By_Year_AgeGroup_Domestic from tblCredential_HighestRank


# ---- qry20a_2Credential_By_Year_AgeGroup_Domestic ---- 
qry20a_2Credential_By_Year_AgeGroup_Domestic <- "
SELECT        AgeGroupLookup.AgeGroup, tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY, 
                         tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY + AgeGroupLookup.AgeGroup AS Expr1, 
                         tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED, COUNT(*) AS Count
INTO Credential_By_Year_AgeGroup_Domestic
FROM            tblCredential_HighestRank INNER JOIN
                         AgeGroupLookup ON tblCredential_HighestRank.AGE_GROUP_AT_GRAD = AgeGroupLookup.AgeIndex
WHERE        (tblCredential_HighestRank.PSI_VISA_STATUS = 'DOMESTIC') OR
                         (tblCredential_HighestRank.PSI_VISA_STATUS IS NULL)
GROUP BY AgeGroupLookup.AgeGroup, tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY, tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED
HAVING        (tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY <> 'APPRENTICESHIP')
ORDER BY AgeGroupLookup.AgeGroup, tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY, tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED;"
dbExecute(con, qry20a_2Credential_By_Year_AgeGroup_Domestic)

# [SELECT INTO] Create Credential_By_Year_AgeGroup_Domestic_Exclude_CIPs from tblCredential_HighestRank


# ---- qry20a_2Credential_By_Year_AgeGroup_Domestic_Exclude_CIPs ---- 
qry20a_2Credential_By_Year_AgeGroup_Domestic_Exclude_CIPs <- "
SELECT        AgeGroupLookup.AgeGroup, tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY, 
                         tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY + AgeGroupLookup.AgeGroup AS Expr1, 
                         tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED, COUNT(*) AS Count
INTO Credential_By_Year_AgeGroup_Domestic_Exclude_CIPs
FROM            tblCredential_HighestRank INNER JOIN
                         AgeGroupLookup ON tblCredential_HighestRank.AGE_GROUP_AT_GRAD = AgeGroupLookup.AgeIndex INNER JOIN
                         Credential_Non_Dup ON tblCredential_HighestRank.id = Credential_Non_Dup.id
WHERE        (tblCredential_HighestRank.PSI_VISA_STATUS = 'DOMESTIC') AND (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '09' AND 
                         Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '10') OR
                         (tblCredential_HighestRank.PSI_VISA_STATUS IS NULL) AND (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '09' AND 
                         Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '10')
GROUP BY AgeGroupLookup.AgeGroup, tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY, tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED
HAVING        (tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY <> 'APPRENTICESHIP')
ORDER BY AgeGroupLookup.AgeGroup, tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY, tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED;"
dbExecute(con, qry20a_2Credential_By_Year_AgeGroup_Domestic_Exclude_CIPs)

# [SELECT INTO] Create Credential_By_Year_AgeGroup_Domestic_Exclude_RU_DACSO from tblCredential_HighestRank


# ---- qry20a_3Credential_By_Year_AgeGroup_Domestic_Exclude_RU_DACSO ---- 
qry20a_3Credential_By_Year_AgeGroup_Domestic_Exclude_RU_DACSO <- "
SELECT        AgeGroupLookup.AgeGroup, tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY, 
                         tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY + AgeGroupLookup.AgeGroup AS Expr1, 
                         tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED, COUNT(*) AS Count
INTO Credential_By_Year_AgeGroup_Domestic_Exclude_RU_DACSO
FROM            tblCredential_HighestRank INNER JOIN
                         AgeGroupLookup ON tblCredential_HighestRank.AGE_GROUP_AT_GRAD = AgeGroupLookup.AgeIndex
WHERE        (tblCredential_HighestRank.PSI_VISA_STATUS = 'DOMESTIC') AND (tblCredential_HighestRank.RESEARCH_UNIVERSITY = 1) AND 
                         (tblCredential_HighestRank.OUTCOMES_CRED <> 'DACSO') OR
                         (tblCredential_HighestRank.PSI_VISA_STATUS IS NULL) AND (tblCredential_HighestRank.RESEARCH_UNIVERSITY = 1) AND 
                         (tblCredential_HighestRank.OUTCOMES_CRED <> 'DACSO') OR
                         (tblCredential_HighestRank.PSI_VISA_STATUS = 'DOMESTIC') AND (tblCredential_HighestRank.RESEARCH_UNIVERSITY IS NULL) OR
                         (tblCredential_HighestRank.PSI_VISA_STATUS IS NULL) AND (tblCredential_HighestRank.RESEARCH_UNIVERSITY IS NULL)
GROUP BY AgeGroupLookup.AgeGroup, tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY, tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED
HAVING        (tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY <> 'APPRENTICESHIP')
ORDER BY AgeGroupLookup.AgeGroup, tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY, tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED;"
dbExecute(con, qry20a_3Credential_By_Year_AgeGroup_Domestic_Exclude_RU_DACSO)

# [SELECT INTO] Create Credential_By_Year_CIP4_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs from tblCredential_HighestRank


# ---- qry20a_4Credential_By_Year_CIP4_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs ---- 
qry20a_4Credential_By_Year_CIP4_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs <- "
SELECT        AgeGroupLookup.AgeGroup, tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY, 
                         tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY + AgeGroupLookup.AgeGroup AS Expr1, Credential_Non_Dup.FINAL_CIP_CODE_4, 
                         tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED, COUNT(*) AS Count
INTO Credential_By_Year_CIP4_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs
FROM            tblCredential_HighestRank INNER JOIN
                         AgeGroupLookup ON tblCredential_HighestRank.AGE_GROUP_AT_GRAD = AgeGroupLookup.AgeIndex INNER JOIN
                         Credential_Non_Dup ON tblCredential_HighestRank.id = Credential_Non_Dup.id
WHERE        (tblCredential_HighestRank.PSI_VISA_STATUS = 'DOMESTIC') AND (tblCredential_HighestRank.RESEARCH_UNIVERSITY = 1) AND 
                         (tblCredential_HighestRank.OUTCOMES_CRED <> 'DACSO') AND (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '09') AND 
                         (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '10') OR
                         (tblCredential_HighestRank.PSI_VISA_STATUS IS NULL) AND (tblCredential_HighestRank.RESEARCH_UNIVERSITY = 1) AND 
                         (tblCredential_HighestRank.OUTCOMES_CRED <> 'DACSO') AND (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '09') AND 
                         (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '10') OR
                         (tblCredential_HighestRank.PSI_VISA_STATUS = 'DOMESTIC') AND (tblCredential_HighestRank.RESEARCH_UNIVERSITY IS NULL) AND 
                         (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '09') AND (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '10') OR
                         (tblCredential_HighestRank.PSI_VISA_STATUS IS NULL) AND (tblCredential_HighestRank.RESEARCH_UNIVERSITY IS NULL) AND 
                         (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '09') AND (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '10')
GROUP BY AgeGroupLookup.AgeGroup, tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY, tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED, 
                         Credential_Non_Dup.FINAL_CIP_CODE_4
HAVING        (tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY <> 'APPRENTICESHIP')
ORDER BY AgeGroupLookup.AgeGroup, tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY, Credential_Non_Dup.FINAL_CIP_CODE_4, 
                         tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED;"
dbExecute(con, qry20a_4Credential_By_Year_CIP4_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs)

# [SELECT INTO] Create Credential_By_Year_CIP4_Gender_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs from tblCredential_HighestRank


# ---- qry20a_4Credential_By_Year_CIP4_Gender_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs ---- 
qry20a_4Credential_By_Year_CIP4_Gender_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs <- "SELECT        tblCredential_HighestRank.psi_gender_cleaned, AgeGroupLookup.AgeGroup, tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY, 
                         tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY + AgeGroupLookup.AgeGroup + tblCredential_HighestRank.psi_gender_cleaned AS Expr1, 
                         Credential_Non_Dup.FINAL_CIP_CODE_4, tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED, COUNT(*) AS Count
INTO Credential_By_Year_CIP4_Gender_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs
FROM            tblCredential_HighestRank INNER JOIN
                         AgeGroupLookup ON tblCredential_HighestRank.AGE_GROUP_AT_GRAD = AgeGroupLookup.AgeIndex INNER JOIN
                         Credential_Non_Dup ON tblCredential_HighestRank.id = Credential_Non_Dup.id
WHERE        (tblCredential_HighestRank.PSI_VISA_STATUS = 'DOMESTIC') AND (tblCredential_HighestRank.RESEARCH_UNIVERSITY = 1) AND 
                         (tblCredential_HighestRank.OUTCOMES_CRED <> 'DACSO') AND (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '09') AND 
                         (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '10') OR
                         (tblCredential_HighestRank.PSI_VISA_STATUS IS NULL) AND (tblCredential_HighestRank.RESEARCH_UNIVERSITY = 1) AND 
                         (tblCredential_HighestRank.OUTCOMES_CRED <> 'DACSO') AND (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '09') AND 
                         (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '10') OR
                         (tblCredential_HighestRank.PSI_VISA_STATUS = 'DOMESTIC') AND (tblCredential_HighestRank.RESEARCH_UNIVERSITY IS NULL) AND 
                         (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '09') AND (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '10') OR
                         (tblCredential_HighestRank.PSI_VISA_STATUS IS NULL) AND (tblCredential_HighestRank.RESEARCH_UNIVERSITY IS NULL) AND 
                         (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '09') AND (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '10')
GROUP BY AgeGroupLookup.AgeGroup, tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY, tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED, 
                         tblCredential_HighestRank.psi_gender_cleaned, Credential_Non_Dup.FINAL_CIP_CODE_4
HAVING        (tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY <> 'APPRENTICESHIP')
ORDER BY AgeGroupLookup.AgeGroup, tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY, tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED, 
                         tblCredential_HighestRank.psi_gender_cleaned DESC;"
dbExecute(con, qry20a_4Credential_By_Year_CIP4_Gender_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs) # 

# [SELECT INTO] Create Credential_By_Year_Gender_AgeGroup_Domestic_Exclude_CIPs from tblCredential_HighestRank


# ---- qry20a_4Credential_By_Year_Gender_AgeGroup_Domestic_Exclude_CIPs ---- 
qry20a_4Credential_By_Year_Gender_AgeGroup_Domestic_Exclude_CIPs <- "SELECT        tblCredential_HighestRank.psi_gender_cleaned, AgeGroupLookup.AgeGroup, tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY, 
                         tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY + AgeGroupLookup.AgeGroup + tblCredential_HighestRank.psi_gender_cleaned AS Expr1, 
                         tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED, COUNT(*) AS Count
INTO Credential_By_Year_Gender_AgeGroup_Domestic_Exclude_CIPs
FROM            tblCredential_HighestRank INNER JOIN
                         AgeGroupLookup ON tblCredential_HighestRank.AGE_GROUP_AT_GRAD = AgeGroupLookup.AgeIndex INNER JOIN
                         Credential_Non_Dup ON tblCredential_HighestRank.id = Credential_Non_Dup.id
WHERE        (tblCredential_HighestRank.PSI_VISA_STATUS = 'DOMESTIC') AND (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '09') AND 
                         (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '10') OR
                         (tblCredential_HighestRank.PSI_VISA_STATUS IS NULL) AND (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '09') AND 
                         (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '10') OR
                         (tblCredential_HighestRank.PSI_VISA_STATUS = 'DOMESTIC') AND (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '09') AND 
                         (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '10') OR
                         (tblCredential_HighestRank.PSI_VISA_STATUS IS NULL) AND (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '09') AND 
                         (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '10')
GROUP BY AgeGroupLookup.AgeGroup, tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY, tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED, 
                         tblCredential_HighestRank.psi_gender_cleaned
HAVING        (tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY <> 'APPRENTICESHIP')
ORDER BY AgeGroupLookup.AgeGroup, tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY, tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED, 
                         tblCredential_HighestRank.psi_gender_cleaned DESC;"
dbExecute(con, qry20a_4Credential_By_Year_Gender_AgeGroup_Domestic_Exclude_CIPs) 

# [SELECT INTO] Create Credential_By_Year_Gender_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs from tblCredential_HighestRank


# ---- qry20a_4Credential_By_Year_Gender_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs ---- 
qry20a_4Credential_By_Year_Gender_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs <- "SELECT        tblCredential_HighestRank.psi_gender_cleaned, AgeGroupLookup.AgeGroup, tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY, 
                         tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY + AgeGroupLookup.AgeGroup + tblCredential_HighestRank.psi_gender_cleaned AS Expr1, tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED, COUNT(*) 
                         AS Count
INTO Credential_By_Year_Gender_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs
FROM            tblCredential_HighestRank INNER JOIN
                         AgeGroupLookup ON tblCredential_HighestRank.AGE_GROUP_AT_GRAD = AgeGroupLookup.AgeIndex INNER JOIN
                         Credential_Non_Dup ON tblCredential_HighestRank.id = Credential_Non_Dup.id
WHERE        (tblCredential_HighestRank.PSI_VISA_STATUS = 'DOMESTIC') AND (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '09') AND (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '10') AND 
                         (Credential_Non_Dup.RESEARCH_UNIVERSITY = 1) AND (Credential_Non_Dup.OUTCOMES_CRED <> 'DACSO') OR
                         (tblCredential_HighestRank.PSI_VISA_STATUS IS NULL) AND (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '09') AND (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '10') AND 
                         (Credential_Non_Dup.RESEARCH_UNIVERSITY = 1) AND (Credential_Non_Dup.OUTCOMES_CRED <> 'DACSO') OR
                         (tblCredential_HighestRank.PSI_VISA_STATUS = 'DOMESTIC') AND (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '09') AND (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '10') AND 
                         (Credential_Non_Dup.RESEARCH_UNIVERSITY IS NULL) OR
                         (tblCredential_HighestRank.PSI_VISA_STATUS IS NULL) AND (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '09') AND (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '10') AND 
                         (Credential_Non_Dup.RESEARCH_UNIVERSITY IS NULL)
GROUP BY AgeGroupLookup.AgeGroup, tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY, tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED, tblCredential_HighestRank.psi_gender_cleaned
HAVING        (tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY <> 'APPRENTICESHIP')
ORDER BY AgeGroupLookup.AgeGroup, tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY, tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED, tblCredential_HighestRank.psi_gender_cleaned DESC;"
dbExecute(con, qry20a_4Credential_By_Year_Gender_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs) 

# these two need a table we don't have - ignore for now 

# [SELECT INTO] Create Credential_By_Year_PSI_TYPE_Domestic_Exclude_RU_DACSO_Exclude_CIPs from tblCredential_HighestRank


# ---- qry20a_4Credential_By_Year_PSI_TYPE_Domestic_Exclude_RU_DACSO_Exclude_CIPs ---- 
qry20a_4Credential_By_Year_PSI_TYPE_Domestic_Exclude_RU_DACSO_Exclude_CIPs <- "
SELECT        PSI_CODE_RECODE.PSI_TYPE_RECODE, tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY, 
                         tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY AS Expr1, 
                         tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED, COUNT(*) AS Count
INTO Credential_By_Year_PSI_TYPE_Domestic_Exclude_RU_DACSO_Exclude_CIPs
FROM            tblCredential_HighestRank INNER JOIN
                         AgeGroupLookup ON tblCredential_HighestRank.AGE_GROUP_AT_GRAD = AgeGroupLookup.AgeIndex INNER JOIN
                         Credential_Non_Dup ON tblCredential_HighestRank.id = Credential_Non_Dup.id INNER JOIN
                         PSI_CODE_RECODE ON tblCredential_HighestRank.PSI_CODE = PSI_CODE_RECODE.PSI_CODE
WHERE        (tblCredential_HighestRank.PSI_VISA_STATUS = 'DOMESTIC') AND (tblCredential_HighestRank.RESEARCH_UNIVERSITY = 1) AND 
                         (tblCredential_HighestRank.OUTCOMES_CRED <> 'DACSO') AND (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '09') AND 
                         (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '10') AND (AgeGroupLookup.AgeIndex <> 1) AND (AgeGroupLookup.AgeIndex <> 9) OR
                         (tblCredential_HighestRank.PSI_VISA_STATUS IS NULL) AND (tblCredential_HighestRank.RESEARCH_UNIVERSITY = 1) AND 
                         (tblCredential_HighestRank.OUTCOMES_CRED <> 'DACSO') AND (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '09') AND 
                         (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '10') AND (AgeGroupLookup.AgeIndex <> 1) AND (AgeGroupLookup.AgeIndex <> 9) OR
                         (tblCredential_HighestRank.PSI_VISA_STATUS = 'DOMESTIC') AND (tblCredential_HighestRank.RESEARCH_UNIVERSITY IS NULL) AND 
                         (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '09') AND (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '10') AND (AgeGroupLookup.AgeIndex <> 1) 
                         AND (AgeGroupLookup.AgeIndex <> 9) OR
                         (tblCredential_HighestRank.PSI_VISA_STATUS IS NULL) AND (tblCredential_HighestRank.RESEARCH_UNIVERSITY IS NULL) AND 
                         (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '09') AND (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '10') AND (AgeGroupLookup.AgeIndex <> 1) 
                         AND (AgeGroupLookup.AgeIndex <> 9)
GROUP BY tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY, tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED, 
                         PSI_CODE_RECODE.PSI_TYPE_RECODE
HAVING        (tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY <> 'APPRENTICESHIP')
ORDER BY tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY, tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED;"
# dbGetQuery(con, qry20a_4Credential_By_Year_PSI_TYPE_Domestic_Exclude_RU_DACSO_Exclude_CIPs)

# [SELECT INTO] Create Credential_By_Year_PSI_TYPE_Domestic_Exclude_RU_DACSO_Exclude_CIPs_Not_Highest from AgeGroupLookup


# ---- qry20a_4Credential_By_Year_PSI_TYPE_Domestic_Exclude_RU_DACSO_Exclude_CIPs_Not_Highest ---- 
qry20a_4Credential_By_Year_PSI_TYPE_Domestic_Exclude_RU_DACSO_Exclude_CIPs_Not_Highest <- "
SELECT        PSI_CODE_RECODE.PSI_TYPE_RECODE, Credential_Non_Dup.PSI_CREDENTIAL_CATEGORY, Credential_Non_Dup.PSI_CREDENTIAL_CATEGORY AS Expr1, 
                         Credential_Non_Dup.PSI_AWARD_SCHOOL_YEAR_DELAYED, COUNT(*) AS Count
INTO Credential_By_Year_PSI_TYPE_Domestic_Exclude_RU_DACSO_Exclude_CIPs_Not_Highest
FROM            AgeGroupLookup INNER JOIN
                         Credential_Non_Dup ON AgeGroupLookup.AgeIndex = Credential_Non_Dup.AGE_GROUP_AT_GRAD INNER JOIN
                         PSI_CODE_RECODE ON Credential_Non_Dup.PSI_CODE = PSI_CODE_RECODE.PSI_CODE INNER JOIN
                         CredentialSupVars ON Credential_Non_Dup.id = CredentialSupVars.ID
WHERE        (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '09') AND (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '10') AND (AgeGroupLookup.AgeIndex <> 1) 
                         AND (AgeGroupLookup.AgeIndex <> 9) AND (CredentialSupVars.PSI_VISA_STATUS = 'DOMESTIC') AND (Credential_Non_Dup.RESEARCH_UNIVERSITY = 1) AND 
                         (Credential_Non_Dup.OUTCOMES_CRED <> 'DACSO') OR
                         (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '09') AND (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '10') AND (AgeGroupLookup.AgeIndex <> 1) 
                         AND (AgeGroupLookup.AgeIndex <> 9) AND (CredentialSupVars.PSI_VISA_STATUS IS NULL) AND (Credential_Non_Dup.RESEARCH_UNIVERSITY = 1) AND 
                         (Credential_Non_Dup.OUTCOMES_CRED <> 'DACSO') OR
                         (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '09') AND (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '10') AND (AgeGroupLookup.AgeIndex <> 1) 
                         AND (AgeGroupLookup.AgeIndex <> 9) AND (CredentialSupVars.PSI_VISA_STATUS = 'DOMESTIC') AND (Credential_Non_Dup.RESEARCH_UNIVERSITY IS NULL) OR
                         (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '09') AND (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '10') AND (AgeGroupLookup.AgeIndex <> 1) 
                         AND (AgeGroupLookup.AgeIndex <> 9) AND (CredentialSupVars.PSI_VISA_STATUS IS NULL) AND (Credential_Non_Dup.RESEARCH_UNIVERSITY IS NULL)
GROUP BY Credential_Non_Dup.PSI_CREDENTIAL_CATEGORY, Credential_Non_Dup.PSI_AWARD_SCHOOL_YEAR_DELAYED, PSI_CODE_RECODE.PSI_TYPE_RECODE
HAVING        (Credential_Non_Dup.PSI_CREDENTIAL_CATEGORY <> 'APPRENTICESHIP')
ORDER BY Credential_Non_Dup.PSI_CREDENTIAL_CATEGORY, Credential_Non_Dup.PSI_AWARD_SCHOOL_YEAR_DELAYED;"
# dbGetQuery(con, qry20a_4Credential_By_Year_PSI_TYPE_Domestic_Exclude_RU_DACSO_Exclude_CIPs_Not_Highest)


# [SELECT INTO] Create Checking_Excluding_RU_DACSO_Variables from Credential_Non_Dup


# ---- qry20a_99_Checking_Excluding_RU_DACSO_Variables ---- 
qry20a_99_Checking_Excluding_RU_DACSO_Variables <- "
SELECT        RESEARCH_UNIVERSITY, OUTCOMES_CRED, PSI_CODE, PSI_CREDENTIAL_CATEGORY, PSI_AWARD_SCHOOL_YEAR_DELAYED, COUNT(*) AS Expr1
INTO Checking_Excluding_RU_DACSO_Variables
FROM            Credential_Non_Dup
GROUP BY RESEARCH_UNIVERSITY, PSI_CODE, OUTCOMES_CRED, PSI_CREDENTIAL_CATEGORY, PSI_AWARD_SCHOOL_YEAR_DELAYED
HAVING        (RESEARCH_UNIVERSITY = 1) AND (OUTCOMES_CRED = 'DACSO') AND (PSI_AWARD_SCHOOL_YEAR_DELAYED = '2018/2019')
ORDER BY OUTCOMES_CRED, RESEARCH_UNIVERSITY;"
dbExecute(con, qry20a_99_Checking_Excluding_RU_DACSO_Variables)

# not used?
# dbGetQuery(con, qryCreateIDinSTPCredential)


# [UPDATE] CredentialSupVars


# ---- qry_Update_Cdtl_Sup_Vars_InternationalFlag ---- 
qry_Update_Cdtl_Sup_Vars_InternationalFlag <- "UPDATE    CredentialSupVars SET  International_Include_Flag = 
tbl_CredentialHighestRank_International.International_Include_Flag 
FROM         CredentialSupVars INNER JOIN 
tbl_CredentialHighestRank_International ON 
CredentialSupVars.ID = tbl_CredentialHighestRank_International.id;"
dbExecute(con, qry_Update_Cdtl_Sup_Vars_InternationalFlag)

# ---- Clean Up ----
dbExecute(con, "DROP TABLE CredentialSupVarsFromEnrolment")
dbExecute(con, "DROP TABLE CredentialSupVars")
dbExecute(con, "DROP TABLE CredentialSupVars_BirthdateClean")
dbExecute(con, "DROP VIEW Credential")
dbExecute(con, "DROP VIEW Credential_Ranking")
dbDisconnect(con)

# ---- These tables used later ----
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."tblCredential_HighestRank"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."Credential_Non_Dup"')))


