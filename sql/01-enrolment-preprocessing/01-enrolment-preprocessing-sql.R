# ---- Checks ----
qry00a_check_null_epens <- "
SELECT COUNT (*) AS n_null_epens FROM STP_Enrolment
WHERE STP_Enrolment.ENCRYPTED_TRUE_PEN IN ('', ' ', '(Unspecified)') 
OR STP_Enrolment.ENCRYPTED_TRUE_PEN IS NULL;"

qry00b_check_unique_epens <- "
  SELECT COUNT (DISTINCT ENCRYPTED_TRUE_PEN) AS n_epens
  FROM STP_Enrolment"

# ---- create id field as primary key ----
qry00c_CreateIDinSTPEnrolment <- "
  ALTER TABLE STP_Enrolment
  ADD ID INT IDENTITY(1,1) NOT NULL"

qry00d_SetPKeyinSTPEnrolment <- "
  ALTER TABLE STP_Enrolment
  ADD CONSTRAINT STP_Enrolment_PK_ID PRIMARY KEY (ID)"


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


# ---- qry02a_Record_With_PEN_Or_STUID ---- 
qry02a_Record_With_PEN_Or_STUID <- "
SELECT     id, PSI_STUDENT_NUMBER, PSI_CODE, ENCRYPTED_TRUE_PEN
INTO       tmp_tbl_qry02a_Record_With_PEN_Or_STUID
FROM       STP_Enrolment
WHERE     (PSI_STUDENT_NUMBER NOT IN('',' ','(Unspecified)')
AND        PSI_CODE NOT IN('',' ','(Unspecified)'))
OR         (ENCRYPTED_TRUE_PEN NOT IN('',' ','(Unspecified)'));"


# ---- qry02b_Drop_No_PEN_Or_No_STUID ---- 
qry02b_Drop_No_PEN_Or_No_STUID <- "
SELECT      STP_Enrolment.ID, STP_Enrolment.ENCRYPTED_TRUE_PEN, STP_Enrolment.PSI_STUDENT_NUMBER, STP_Enrolment.PSI_CODE 
INTO        Drop_No_PEN_or_No_STUID
FROM        tmp_tbl_qry02a_Record_With_PEN_Or_STUID 
RIGHT JOIN  STP_Enrolment 
  ON        tmp_tbl_qry02a_Record_With_PEN_Or_STUID.ID = STP_Enrolment.ID
WHERE       ((tmp_tbl_qry02a_Record_With_PEN_Or_STUID.ID) Is Null);"


# ---- qry02c_Update_Drop_No_PEN_Or_No_STUID.SQL ---- 
qry02c_Update_Drop_No_PEN_Or_No_STUID <- "
UPDATE    STP_Enrolment_Record_Type
SET       RecordStatus = 1
FROM      STP_Enrolment_Record_Type INNER JOIN
          Drop_No_PEN_or_No_STUID ON STP_Enrolment_Record_Type.ID = Drop_No_PEN_or_No_STUID.ID;"

# ---- qry03a_Drop_Record_Developmental ---- 
qry03a_Drop_Record_Developmental <- "
SELECT    ID, ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, PSI_CONTINUING_EDUCATION_COURSE_ONLY, LEFT(STP_Enrolment.PSI_CIP_CODE, 2) AS CIP2, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION, PSI_STUDY_LEVEL
INTO      Drop_Developmental
FROM      STP_Enrolment
WHERE     PSI_STUDY_LEVEL = 'DEVELOPMENTAL';"


# ---- qry03b_Update_Drop_Record_Developmental ---- 
qry03b_Update_Drop_Record_Developmental <- "
UPDATE    STP_Enrolment_Record_Type
SET       RecordStatus = 2
FROM      STP_Enrolment_Record_Type INNER JOIN
          Drop_Developmental ON STP_Enrolment_Record_Type.ID = Drop_Developmental.ID;"

# ---- qry03c_Drop_Skills_Based ---- 
qry03c_Drop_Skills_Based <- "
SELECT    ID, ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, PSI_CONTINUING_EDUCATION_COURSE_ONLY, LEFT(STP_Enrolment.PSI_CIP_CODE, 2) AS CIP2, 
          PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION, PSI_STUDY_LEVEL, PSI_CREDENTIAL_CATEGORY
INTO      Drop_Skills_Based
FROM      STP_Enrolment
WHERE     PSI_CONTINUING_EDUCATION_COURSE_ONLY = 'SKILLS CRS ONLY'
  AND     PSI_STUDY_LEVEL <>'DEVELOPMENTAL'
  AND     PSI_CREDENTIAL_CATEGORY IN ('NONE','OTHER');"

# ---- qry03da_Keep_TeachEd ---- 
qry03da_Keep_TeachEd <- "
UPDATE    Drop_Skills_Based
SET       KEEP = 'Y'
WHERE     (PSI_CODE = 'UFV') AND (PSI_PROGRAM_CODE = 'TEACH ED') 
  OR      (PSI_CODE = 'UCFV') AND (PSI_PROGRAM_CODE = 'TEACH ED');"

# ---- qry03d_Update_Drop_Record_Skills_Based ---- 
qry03d_Update_Drop_Record_Skills_Based <- "
UPDATE    STP_Enrolment_Record_Type
SET       RecordStatus = 6
FROM      STP_Enrolment_Record_Type 
INNER JOIN Drop_Skills_Based 
  ON STP_Enrolment_Record_Type.ID = Drop_Skills_Based.ID
WHERE     RecordStatus is NULL and KEEP IS NULL;"

# ---- qry03d_1_Drop_Continuing_Ed ---- 
qry03d_1_Drop_Continuing_Ed <- "
SELECT    ID, ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, PSI_CONTINUING_EDUCATION_COURSE_ONLY, LEFT(STP_Enrolment.PSI_CIP_CODE, 2) AS CIP2, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION, PSI_STUDY_LEVEL
INTO      Drop_ContinuingEd
FROM      STP_Enrolment
WHERE     (PSI_STUDY_LEVEL <> 'DEVELOPMENTAL'
  AND     PSI_CONTINUING_EDUCATION_COURSE_ONLY <> 'SKILLS CRS ONLY'
  AND     PSI_CREDENTIAL_CATEGORY IN ('NONE','OTHER')
  AND     (Left(PSI_CIP_CODE,2) IN ('21', '32', '33', '34', '35', '36','37', '53', '89')));"

# ---- qry03d_2_Update_Drop_Continuing_Ed ---- 
qry03d_2_Update_Drop_Continuing_Ed <- "
UPDATE    
STP_Enrolment_Record_Type
SET       RecordStatus = 6
FROM      STP_Enrolment_Record_Type 
INNER JOIN Drop_ContinuingEd 
ON        STP_Enrolment_Record_Type.ID = Drop_ContinuingEd.ID
WHERE     RecordStatus is NULL;"

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


# ---- qry03d_4_Updated_Drop_ContinuingEdMore ---- 
qry03d_4_Updated_Drop_ContinuingEdMore <- "
UPDATE      STP_Enrolment_Record_Type
SET         STP_Enrolment_Record_Type.RecordStatus = 6
FROM        STP_Enrolment_Record_Type 
INNER JOIN  Drop_ContinuingEd_More 
  ON        STP_Enrolment_Record_Type.ID = Drop_ContinuingEd_More.ID
WHERE       STP_Enrolment_Record_Type.RecordStatus is NULL;"


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

# ---- qry03ea_Exclude_Skills_Based_Programs ----
qry03ea_Exclude_Skills_Based_Programs <- "
UPDATE      Keep_Skills_Based
SET         Exclude = 'Y'
WHERE      (PSI_CODE = 'SEL'
  AND       PSI_CREDENTIAL_PROGRAM_DESCRIPTION = 'COMMUNITY, CORPORATE & INTERNATIONAL DEVELOPMENT')
  OR (PSI_CODE = 'NIC' AND CIP2 IN ('21', '32', '33', '34', '35', '36', '37', '53', '89'));"


# ---- qry03f_Update_Keep_Record_Skills_Based ---- 
qry03f_Update_Keep_Record_Skills_Based <- "
UPDATE      STP_Enrolment_Record_Type
SET         RecordStatus = 0
FROM        STP_Enrolment_Record_Type 
INNER JOIN  Keep_Skills_Based 
ON          STP_Enrolment_Record_Type.ID = Keep_Skills_Based.ID
WHERE       STP_Enrolment_Record_Type.RecordStatus IS NULL
AND         Keep_Skills_Based.Exclude IS NULL;"


# ---- qry03fb_Update_Keep_Record_Skills_Based ---- 
qry03fb_Update_Keep_Record_Skills_Based <- "
UPDATE      STP_Enrolment_Record_Type
SET         RecordStatus = 6
FROM        STP_Enrolment_Record_Type 
INNER JOIN  Keep_Skills_Based 
  ON        STP_Enrolment_Record_Type.ID = Keep_Skills_Based.ID
WHERE       STP_Enrolment_Record_Type.RecordStatus IS NULL
  AND       Keep_Skills_Based.Exclude ='Y';"


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


# ---- qry03g_c_Update_Keep_More_Skills_Based ---- 
qry03g_c_Update_Keep_More_Skills_Based <- "
UPDATE        STP_Enrolment_Record_Type
SET           RecordStatus = 0
FROM          STP_Enrolment_Record_Type 
INNER JOIN    tmp_MoreSkillsBased_to_Keep 
  ON STP_Enrolment_Record_Type.ID = tmp_MoreSkillsBased_to_Keep.ID
WHERE        (STP_Enrolment_Record_Type.RecordStatus=6);"


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

# ---- qry03j_Update_Suspect_Skills_Based ---- 
qry03j_Update_Suspect_Skills_Based <- "
UPDATE    STP_Enrolment_Record_Type
SET       RecordStatus = 6
FROM      Suspect_Skills_Based INNER JOIN STP_Enrolment_Record_Type 
  ON        Suspect_Skills_Based.ID = STP_Enrolment_Record_Type.ID
WHERE STP_Enrolment_Record_Type.RecordStatus IS NULL AND Suspect_Skills_Based.Keep IS NULL;"


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


# ---- qry03k_Update_ID_for_Drop_Dev_Credential_CIP ---- 
qry03k_Update_ID_for_Drop_Dev_Credential_CIP <- "
UPDATE  Drop_Developmental_CIPS
SET     ID = STP_Enrolment.ID
FROM    Drop_Developmental_CIPS 
INNER JOIN STP_Enrolment 
  ON    Drop_Developmental_CIPS.ENCRYPTED_TRUE_PEN = STP_Enrolment.ENCRYPTED_TRUE_PEN;"


# ---- qry03l_Update_Developmental_CIPs ---- 
qry03l_Update_Developmental_CIPs <- "
UPDATE  STP_Enrolment_Record_Type
SET     RecordStatus = 7
FROM    STP_Enrolment_Record_Type 
INNER JOIN Drop_Developmental_CIPS 
  ON    STP_Enrolment_Record_Type.ID = Drop_Developmental_CIPS.ID
WHERE   STP_Enrolment_Record_Type.RecordStatus IS NULL 
  AND   Drop_Developmental_CIPS.DO_NOT_EXCLUDE IS Null;"


# ---- qry04a_Drop_No_PSI_Transition ---- 
qry04a_Drop_No_PSI_Transition <- "
SELECT  ID, ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, PSI_ENTRY_STATUS
INTO    Drop_No_Transition
FROM    STP_Enrolment
WHERE   PSI_ENTRY_STATUS = 'No Transition';"


# ---- qry04b_Update_Drop_No_PSI_Transition ---- 
qry04b_Update_Drop_No_PSI_Transition <- "
UPDATE  STP_Enrolment_Record_Type
SET     RecordStatus = 3
FROM    STP_Enrolment_Record_Type
INNER JOIN Drop_No_Transition 
  ON    STP_Enrolment_Record_Type.ID = Drop_No_Transition.ID
WHERE   STP_Enrolment_Record_Type.RecordStatus is NULL;"


# ---- qry05a_Drop_Credential_Only ---- 
qry05a_Drop_Credential_Only <- "
SELECT ID, ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, PSI_ENTRY_STATUS, PSI_MIN_START_DATE, CREDENTIAL_AWARD_DATE
INTO Drop_Credential_Only
FROM STP_Enrolment
WHERE (PSI_MIN_START_DATE IN('',' ','(Unspecified)'))
  AND (PSI_ENTRY_STATUS IN('',' ','(Unspecified)')) 
  AND (CREDENTIAL_AWARD_DATE NOT IN('',' ','(Unspecified)'));"


# ---- qry05b_Update_Drop_Credential_Only ---- 
qry05b_Update_Drop_Credential_Only <- "
UPDATE  STP_Enrolment_Record_Type
SET     RecordStatus = 4
FROM    STP_Enrolment_Record_Type INNER JOIN
        Drop_Credential_Only ON STP_Enrolment_Record_Type.ID = Drop_Credential_Only.ID
WHERE   STP_Enrolment_Record_Type.RecordStatus is NULL;"


# ---- qry06a_Drop_PSI_Outside_BC ---- 
qry06a_Drop_PSI_Outside_BC <- "
SELECT  ID, ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, ATTENDING_PSI_OUTSIDE_BC
INTO    Drop_PSI_Outside_BC
FROM    STP_Enrolment
WHERE   ATTENDING_PSI_OUTSIDE_BC = 'Y';"


# ---- qry06b_Update_Drop_PSI_Outside_BC ---- 
qry06b_Update_Drop_PSI_Outside_BC <- "
UPDATE    STP_Enrolment_Record_Type
SET       RecordStatus = 5
FROM      STP_Enrolment_Record_Type INNER JOIN
          Drop_PSI_Outside_BC ON STP_Enrolment_Record_Type.ID = Drop_PSI_Outside_BC.ID
WHERE STP_Enrolment_Record_Type.RecordStatus is NULL;"

# ---- qry07_Update_RecordStatus_No_Dropped ---- 
qry07_Update_RecordStatus_No_Dropped <- "
UPDATE  STP_Enrolment_Record_Type 
SET     STP_Enrolment_Record_Type.RecordStatus = 0
WHERE   STP_Enrolment_Record_Type.RecordStatus Is Null;"


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


# ---- qry08b_Fix_EPEN_in_STP_Enrolment_Valid ---- 
qry08b_Fix_EPEN_in_STP_Enrolment_Valid <- "
UPDATE    STP_Enrolment_Valid
SET       ENCRYPTED_TRUE_PEN = pssm2019.dbo.EPEN_ENRO_FIXES_2019_20.EPEN_FIXED
FROM      STP_Enrolment_Valid 
INNER JOIN pssm2019.dbo.EPEN_ENRO_FIXES_2019_20
  ON STP_Enrolment_Valid.ID = pssm2019.dbo.EPEN_ENRO_FIXES_2019_20.ID
WHERE     pssm2019.dbo.EPEN_ENRO_FIXES_2019_20.EPEN_FIXED_FLAG = 1;"


# ---- qry08c_Fix_Enrol_Sequence_in_STP_Enrolment_Valid ---- 
qry08c_Fix_Enrol_Sequence_in_STP_Enrolment_Valid <- "
UPDATE    STP_Enrolment_Valid
SET       PSI_ENROLMENT_SEQUENCE = pssm2019.dbo.EPEN_ENRO_FIXES_2019_20.PSI_ENROLMENT_SEQUENCE_FIX
FROM      STP_Enrolment_Valid 
INNER JOIN pssm2019.dbo.EPEN_ENRO_FIXES_2019_20
  ON STP_Enrolment_Valid.ID = pssm2019.dbo.EPEN_ENRO_FIXES_2019_20.ID
WHERE     pssm2019.dbo.EPEN_ENRO_FIXES_2019_20.PSI_ENROLMENT_SEQUENCE_FIX <> 'NULL';"

# ---- qry09a_MinEnrolmentPEN ---- 
qry09a_MinEnrolmentPEN <- "
SELECT    ENCRYPTED_TRUE_PEN, PSI_SCHOOL_YEAR, MIN(PSI_ENROLMENT_SEQUENCE) AS MinPSIEnrolmentSequence
INTO      tmp_tbl_qry09a_MinEnrolmentPEN
FROM      STP_Enrolment_Valid
GROUP BY  ENCRYPTED_TRUE_PEN, PSI_SCHOOL_YEAR
HAVING    ENCRYPTED_TRUE_PEN NOT IN('',' ','(Unspecified)');"

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

# ---- qry09c_MinEnrolmentPEN ---- 
qry09c_MinEnrolmentPEN <- "
SELECT    MinID AS MinOfID
INTO      MinEnrolment_ID_PEN
FROM      tmp_tbl_qry09b_MinEnrolmentPEN;"

# ---- qry10a_MinEnrolmentSTUID ---- 
qry10a_MinEnrolmentSTUID <- "
SELECT    PSI_STUDENT_NUMBER, PSI_CODE, PSI_SCHOOL_YEAR, MIN(PSI_ENROLMENT_SEQUENCE) AS MinPSIEnrolmentSequence, ENCRYPTED_TRUE_PEN
INTO      tmp_tbl_qry10a_MinEnrolmentSTUID
FROM      STP_Enrolment_Valid
GROUP BY  PSI_STUDENT_NUMBER, PSI_CODE, PSI_SCHOOL_YEAR, ENCRYPTED_TRUE_PEN
HAVING    ENCRYPTED_TRUE_PEN IN('',' ','(Unspecified)');"

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

# ---- qry10c_MinEnrolmentSTUID ---- 
qry10c_MinEnrolmentSTUID <- "
SELECT    MinID AS MinOfID
INTO      MinEnrolment_ID_STUID
FROM      tmp_tbl_qry10b_MinEnrolmentSTUID;"

# ---- qry11a_Update_MinEnrolmentPEN ---- 
qry11a_Update_MinEnrolmentPEN <- "
UPDATE    STP_Enrolment_Record_Type
SET       MinEnrolment = 1
FROM      MinEnrolment_ID_PEN 
INNER JOIN STP_Enrolment_Record_Type 
  ON MinEnrolment_ID_PEN.MinOfID = STP_Enrolment_Record_Type.ID;"

# ---- qry11b_Update_MinEnrolmentSTUID ---- 
qry11b_Update_MinEnrolmentSTUID <- "
UPDATE    STP_Enrolment_Record_Type
SET       MinEnrolment = 1
FROM      MinEnrolment_ID_STUID 
INNER JOIN STP_Enrolment_Record_Type 
  ON MinEnrolment_ID_STUID.MinOfID = STP_Enrolment_Record_Type.ID
WHERE STP_Enrolment_Record_Type.MinEnrolment = 0
  OR  STP_Enrolment_Record_Type.MinEnrolment IS NULL;"

# ---- qry11c_Update_MinEnrolment_NA ---- 
qry11c_Update_MinEnrolment_NA <- "
UPDATE STP_Enrolment_Record_Type 
SET STP_Enrolment_Record_Type.MinEnrolment = 0
WHERE (((STP_Enrolment_Record_Type.MinEnrolment) Is Null));"

# ---- qry12a_FirstEnrolmentPEN ---- 
qry12a_FirstEnrolmentPEN <- "
SELECT    ENCRYPTED_TRUE_PEN, MIN(PSI_MIN_START_DATE) AS MIN_PSI_MIN_START_DATE
INTO      tmp_tbl_qry12a_FirstEnrolmentPEN
FROM      STP_Enrolment_Valid
GROUP BY  ENCRYPTED_TRUE_PEN
HAVING    ENCRYPTED_TRUE_PEN NOT IN('',' ','(Unspecified)');"

# ---- qry12b_FirstEnrolmentPEN ---- 
qry12b_FirstEnrolmentPEN <- "
SELECT    STP_Enrolment_Valid.ENCRYPTED_TRUE_PEN, tmp_tbl_qry12a_FirstEnrolmentPEN.MIN_PSI_MIN_START_DATE, MIN(STP_Enrolment_Valid.PSI_ENROLMENT_SEQUENCE) AS MinPSI_Enrolment_Sequence
INTO      tmp_tbl_qry12b_FirstEnrolmentPEN
FROM      tmp_tbl_qry12a_FirstEnrolmentPEN 
INNER JOIN STP_Enrolment_Valid 
  ON tmp_tbl_qry12a_FirstEnrolmentPEN.ENCRYPTED_TRUE_PEN = STP_Enrolment_Valid.ENCRYPTED_TRUE_PEN 
  AND tmp_tbl_qry12a_FirstEnrolmentPEN.MIN_PSI_MIN_START_DATE = STP_Enrolment_Valid.PSI_MIN_START_DATE
GROUP BY STP_Enrolment_Valid.ENCRYPTED_TRUE_PEN, tmp_tbl_qry12a_FirstEnrolmentPEN.MIN_PSI_MIN_START_DATE;"

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


# ---- qry13a_FirstEnrolmentSTUID ---- 
qry13a_FirstEnrolmentSTUID <- "
SELECT    PSI_STUDENT_NUMBER, PSI_CODE, MIN(PSI_MIN_START_DATE) AS Min_PSI_Min_Start_Date
INTO      tmp_tbl_qry13a_FirstEnrolment_STUID
FROM      STP_Enrolment_Valid
WHERE     ENCRYPTED_TRUE_PEN IN('',' ','(Unspecified)')
GROUP BY PSI_STUDENT_NUMBER, PSI_CODE;"

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

# ---- qry14a_Update_FirstEnrolmentPEN ---- 
qry14a_Update_FirstEnrolmentPEN <- "
UPDATE    STP_Enrolment_Record_Type
SET       FirstEnrolment = 1
FROM      FirstEnrolment_ID_PEN 
INNER JOIN STP_Enrolment_Record_Type 
  ON FirstEnrolment_ID_PEN.MinID = STP_Enrolment_Record_Type.ID
WHERE     STP_Enrolment_Record_Type.FirstEnrolment IS NULL
    OR    STP_Enrolment_Record_Type.FirstEnrolment = 0;"

# ---- qry14b_Update_FirstEnrolmentSTUID ---- 
qry14b_Update_FirstEnrolmentSTUID <- "
UPDATE    STP_Enrolment_Record_Type
SET       FirstEnrolment = 1
FROM      STP_Enrolment_Record_Type 
INNER JOIN FirstEnrolment_ID_STUID 
  ON STP_Enrolment_Record_Type.ID = FirstEnrolment_ID_STUID.MinID
WHERE STP_Enrolment_Record_Type.FirstEnrolment IS NULL;"

# ---- qry14c_Update_FirstEnrolmentNA ---- 
qry14c_Update_FirstEnrolmentNA <- "
UPDATE    STP_Enrolment_Record_Type
SET       FirstEnrolment = 0
WHERE     FirstEnrolment IS NULL;"

# ---- RecordTypeSummary ----
RecordTypeSummary <-
"SELECT RecordStatus, COUNT(*) AS Expr1
FROM  STP_Enrolment_Record_Type
GROUP BY RecordStatus
"

# ---- CheckSkillsBased ---
CheckSkillsBased <-
"SELECT PSI_CODE, PSI_CONTINUING_EDUCATION_COURSE_ONLY, CIP2, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION, PSI_STUDY_LEVEL, PSI_CREDENTIAL_CATEGORY
FROM  Drop_Skills_Based
GROUP BY PSI_CODE, PSI_CONTINUING_EDUCATION_COURSE_ONLY, CIP2, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION, PSI_STUDY_LEVEL, PSI_CREDENTIAL_CATEGORY;"