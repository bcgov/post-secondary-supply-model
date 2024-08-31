# ---- Checks ----
qry00a_check_null_epens <- "SELECT COUNT (*) AS n_null_epens FROM STP_Credential
  WHERE STP_Credential.ENCRYPTED_TRUE_PEN IN ('', ' ', '(Unspecified)') 
OR STP_Credential.ENCRYPTED_TRUE_PEN IS NULL;"

qry00b_check_unique_epens <- "
  SELECT COUNT (DISTINCT ENCRYPTED_TRUE_PEN) AS n_epens
  FROM STP_Credential"

qry00c_CreateIDinSTPCredential <- "
  ALTER TABLE STP_Credential
  ADD ID INT IDENTITY(1,1) NOT NULL"

qry00d_SetPKeyinSTPCredential <- "
  ALTER TABLE STP_Credential
  ADD CONSTRAINT STP_Credential_PK_ID PRIMARY KEY (ID)"


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


# ---- qry02a_Record_With_PEN_Or_STUID ---- 
qry02a_Record_With_PEN_Or_STUID <- "SELECT      id, PSI_STUDENT_NUMBER, PSI_CODE, ENCRYPTED_TRUE_PEN
INTO       tmp_tbl_qry02a_Cred_Record_With_PEN_or_STUID
FROM       STP_Credential
WHERE     (PSI_STUDENT_NUMBER NOT IN ('', ' ', '(Unspecified)') 
AND        PSI_CODE NOT IN ('', ' ', '(Unspecified)')) 
OR (ENCRYPTED_TRUE_PEN NOT IN ('', ' ', '(Unspecified)'));"


# ---- qry02b_Drop_No_PEN_Or_No_STUID ---- 
qry02b_Drop_No_PEN_Or_No_STUID <- "
SELECT STP_Credential.ID, STP_Credential.ENCRYPTED_TRUE_PEN, STP_Credential.PSI_CODE, STP_Credential.PSI_STUDENT_NUMBER
INTO Drop_Cred_No_PEN_or_No_STUID
FROM tmp_tbl_qry02a_Cred_Record_With_PEN_or_STUID 
RIGHT JOIN STP_Credential 
ON tmp_tbl_qry02a_Cred_Record_With_PEN_or_STUID.ID = STP_Credential.ID
WHERE (((tmp_tbl_qry02a_Cred_Record_With_PEN_or_STUID.ID) Is Null));"


# ---- qry02c_Update_Drop_No_PEN_or_No_STUID.SQL ---- 
qry02c_Update_Drop_No_PEN_or_No_STUID <- "
UPDATE    STP_Credential_Record_Type
SET       RecordStatus = 1
FROM      STP_Credential_Record_Type 
INNER JOIN Drop_Cred_No_PEN_or_No_STUID 
ON STP_Credential_Record_Type.ID = Drop_Cred_No_PEN_or_No_STUID.ID;"


# ---- qry03a_Drop_Record_Developmental ---- 
qry03a_Drop_Record_Developmental <- "
SELECT    ID, ENCRYPTED_TRUE_PEN,  PSI_CODE, PSI_STUDENT_NUMBER, PSI_CREDENTIAL_CATEGORY, LEFT(STP_CREDENTIAL.PSI_CREDENTIAL_CIP, 2) AS CIP2, PSI_CREDENTIAL_LEVEL
INTO      Drop_Cred_Developmental
FROM      STP_Credential
WHERE     PSI_CREDENTIAL_LEVEL = 'DEVELOPMENTAL';"


# ---- qry03b_Update_Drop_Record_Developmental ---- 
qry03b_Update_Drop_Record_Developmental <- "
UPDATE    STP_Credential_Record_Type
SET       RecordStatus = 2
FROM      STP_Credential_Record_Type 
INNER JOIN Drop_Cred_Developmental 
  ON STP_Credential_Record_Type.ID = Drop_Cred_Developmental.ID
WHERE STP_Credential_Record_Type.RecordStatus is null;"

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


# ---- qry03f_Update_Suspect_Skills_Based ---- 
qry03f_Update_Suspect_Skills_Based <- "
UPDATE    STP_Credential_Record_Type
SET       RecordStatus = 6
FROM      Cred_Suspect_Skills_Based 
INNER JOIN STP_Credential_Record_Type 
  ON Cred_Suspect_Skills_Based.ID = STP_Credential_Record_Type.ID
WHERE STP_Credential_Record_Type.RecordStatus IS NULL;"

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

# ---- qry03g2_Drop_Developmental_Credential_CIPS ---- 
qry03g2_Drop_Developmental_Credential_CIPS <- "
UPDATE Drop_Developmental_PSI_CREDENTIAL_CIPS
   SET Keep = 'Yes'
 WHERE (PSI_CODE = 'UVIC' AND PSI_CREDENTIAL_PROGRAM_DESCRIPTION = 'PROF SPEC CERTIFICATE IN MIDDLE YEARS LANG AND LITERACY')
    OR (PSI_CODE = 'NIC' AND PSI_CREDENTIAL_PROGRAM_DESCRIPTION = 'Aquaculture Technician 1')
    OR (PSI_CODE = 'NIC' AND PSI_CREDENTIAL_PROGRAM_DESCRIPTION = 'Coastal Forest Resource')
    OR (PSI_CODE = 'NIC' AND PSI_CREDENTIAL_PROGRAM_DESCRIPTION = 'Underground Mining Essentials');"

# ---- qry03h_Update_Developmental_CIPs ---- 
qry03h_Update_Developmental_CIPs <- "
UPDATE    STP_Credential_Record_Type
SET       RecordStatus = 7
FROM      STP_Credential_Record_Type 
INNER JOIN Drop_Developmental_PSI_CREDENTIAL_CIPS 
  ON STP_Credential_Record_Type.ID = Drop_Developmental_PSI_CREDENTIAL_CIPS.ID
WHERE STP_Credential_Record_Type.RecordStatus IS NULL and Drop_Developmental_PSI_CREDENTIAL_CIPS.Keep IS Null;"


# ---- qry03i_Drop_RecommendationForCert ---- 
qry03i_Drop_RecommendationForCert <- "
SELECT      STP_Credential.ID, STP_Credential.ENCRYPTED_TRUE_PEN, STP_Credential.PSI_CODE, STP_Credential.PSI_CREDENTIAL_PROGRAM_DESCRIPTION, 
                         LEFT(STP_Credential.PSI_CREDENTIAL_CIP, 2) AS CIP2, STP_Credential.PSI_CREDENTIAL_CATEGORY, STP_Credential_Record_Type.RecordStatus
INTO        Drop_Cred_RecommendForCert
FROM        STP_Credential 
INNER JOIN STP_Credential_Record_Type 
  ON STP_Credential.ID = STP_Credential_Record_Type.ID
WHERE        (PSI_CREDENTIAL_CATEGORY = 'RECOMMENDATION FOR CERTIFICATION') AND (STP_Credential_Record_Type.RecordStatus IS NULL);"


# ---- qry03j_Update_RecommendationForCert  ---- 
qry03j_Update_RecommendationForCert  <- "
UPDATE    STP_Credential_Record_Type
SET       RecordStatus = 8
FROM      STP_Credential_Record_Type 
INNER JOIN Drop_Cred_RecommendForCert 
  ON STP_Credential_Record_Type.ID = Drop_Cred_RecommendForCert.ID
WHERE STP_Credential_Record_Type.RecordStatus IS NULL;"


# ---- qry04_Update_RecordStatus_Not_Dropped ---- 
qry04_Update_RecordStatus_Not_Dropped <- "
UPDATE STP_Credential_Record_Type 
SET STP_Credential_Record_Type.RecordStatus = 0
WHERE (((STP_Credential_Record_Type.RecordStatus) Is Null));;"

# ---- RecordTypeSummary ----
RecordTypeSummary <-
  "SELECT RecordStatus, COUNT(*) AS Expr1
FROM  STP_Credential_Record_Type
GROUP BY RecordStatus"


