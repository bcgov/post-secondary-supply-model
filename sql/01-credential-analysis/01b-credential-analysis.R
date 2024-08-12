
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

qry01b_CredentialSupVarsFromEnrol_3 <- "
UPDATE      CredentialSupVarsFromEnrolment
SET         psi_birthdate_cleaned = NULL
WHERE       psi_birthdate_cleaned = '1900-01-01';"


# ---- qry02a_DropCredCategory ---- 
qry02a_DropCredCategory <- "
SELECT     id, PSI_CODE, PSI_CREDENTIAL_CATEGORY, ENCRYPTED_TRUE_PEN, PSI_SCHOOL_YEAR, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION
INTO       Drop_Credential_Category
FROM       Credential
WHERE      PSI_CREDENTIAL_CATEGORY = 'DEVELOPMENTAL CREDENTIAL'
  OR       PSI_CREDENTIAL_CATEGORY = 'OTHER'
  OR       PSI_CREDENTIAL_CATEGORY = 'NONE'
  OR       PSI_CREDENTIAL_CATEGORY = 'SHORT CERTIFICATE';"

# ---- qry02b_DeleteCredCategory ---- 
qry02b_DeleteCredCategory <- "
UPDATE    STP_Credential_Record_Type
SET       DropCredCategory = 'Yes'
FROM      Drop_Credential_Category 
INNER JOIN STP_Credential_Record_Type 
ON  Drop_Credential_Category.id =  STP_Credential_Record_Type.id;"

# ---- qry03a1_ConvertAwardDate ---- 
qry03a1_ConvertAwardDate <- "
UPDATE CredentialSupVars
SET    CREDENTIAL_AWARD_DATE_D = CREDENTIAL_AWARD_DATE;"

# ---- qry03b_DropPartialYear ---- 
qry03b_DropPartialYear <- "
SELECT     id, CREDENTIAL_AWARD_DATE_D
INTO            Drop_Partial_Year
FROM         CredentialSupVars
WHERE     (CREDENTIAL_AWARD_DATE_D >= '2023-09-01');"

# ---- qry03c_DeletePartialYear ---- 
qry03c_DeletePartialYear <- "
UPDATE    STP_Credential_Record_Type 
SET       DropPartialYear = 'Yes'
FROM      STP_Credential_Record_Type  
INNER JOIN Drop_Partial_Year 
ON        STP_Credential_Record_Type.id = Drop_Partial_Year.id;"

# ---- qry03d_CredentialSupVarsBirthdate ---- 
qry03d_CredentialSupVarsBirthdate <- "
SELECT        ENCRYPTED_TRUE_PEN, psi_birthdate_cleaned, psi_birthdate_cleaned_D, PSI_STUDENT_NUMBER, PSI_CODE
INTO          CredentialSupVars_BirthdateClean
FROM          CredentialSupVarsFromEnrolment
GROUP BY      ENCRYPTED_TRUE_PEN, psi_birthdate_cleaned, psi_birthdate_cleaned_D, PSI_STUDENT_NUMBER, PSI_CODE"


# ---- qry03e_CredentialSupVarsGender ---- 
qry03e_CredentialSupVarsGender <- "
SELECT        ENCRYPTED_TRUE_PEN, PSI_GENDER
INTO          CredentialSupVars_Gender
FROM          CredentialSupVarsFromEnrolment
GROUP BY      ENCRYPTED_TRUE_PEN, PSI_GENDER;"


# ---- qry04a_UpdateCredentialSupVarsBirthdate ---- 
qry04a_UpdateCredentialSupVarsBirthdate <- "
UPDATE        CredentialSupVars
SET           psi_birthdate_cleaned = CredentialSupVars_BirthdateClean.psi_birthdate_cleaned, 
              psi_birthdate_cleaned_D = CredentialSupVars_BirthdateClean.psi_birthdate_cleaned_D
FROM          CredentialSupVars 
INNER JOIN    CredentialSupVars_BirthdateClean 
  ON          CredentialSupVars.ENCRYPTED_TRUE_PEN = CredentialSupVars_BirthdateClean.ENCRYPTED_TRUE_PEN
WHERE        (CredentialSupVars.ENCRYPTED_TRUE_PEN IS NOT NULL AND CredentialSupVars.ENCRYPTED_TRUE_PEN NOT IN ('', ' ', '(Unspecified)'))"

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


qry04a1_UpdateCredentialSupVarsBirthdate <- "
UPDATE        CredentialSupVarsFromEnrolment
SET           LAST_SEEN_BIRTHDATE = STP_Enrolment.LAST_SEEN_BIRTHDATE
FROM          CredentialSupVarsFromEnrolment 
INNER JOIN    STP_Enrolment 
ON CredentialSupVarsFromEnrolment.EnrolmentID = STP_Enrolment.ID"

qry04a2_UpdateCredentialSupVarsBirthdate <- "
UPDATE        CredentialSupVars
SET           LAST_SEEN_BIRTHDATE = CredentialSupVarsFromEnrolment.LAST_SEEN_BIRTHDATE
FROM          CredentialSupVarsFromEnrolment 
INNER JOIN    CredentialSupVars 
ON CredentialSupVarsFromEnrolment.ENCRYPTED_TRUE_PEN = CredentialSupVars.ENCRYPTED_TRUE_PEN"

qry04a3_UpdateCredentialSupVarsBirthdate <- "
UPDATE       CredentialSupVars
SET          CredentialSupVars.psi_birthdate_cleaned = LAST_SEEN_BIRTHDATE
WHERE        ((LAST_SEEN_BIRTHDATE IS NOT NULL AND LAST_SEEN_BIRTHDATE NOT IN ('', ' ')) 
AND           (psi_birthdate_cleaned IS NULL))
OR           ((LAST_SEEN_BIRTHDATE IS NOT NULL AND LAST_SEEN_BIRTHDATE NOT IN ('', ' ')) 
AND           (psi_birthdate_cleaned IN ('', ' ')))"

# ---- qry04b_UpdateCredentiaSupVarsGender  ---- 
qry04b_UpdateCredentiaSupVarsGender <- "
UPDATE       CredentialSupVars
SET          psi_gender_cleaned = CredentialSupVars_Gender.psi_gender_cleaned
FROM         CredentialSupVars 
INNER JOIN   CredentialSupVars_Gender 
  ON         CredentialSupVars.ENCRYPTED_TRUE_PEN = CredentialSupVars_Gender.ENCRYPTED_TRUE_PEN;"


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


# ---- qry05a_FindDistinctCredentials_CreateViewCredentialRemoveDup ---- 
qry05a_FindDistinctCredentials_CreateViewCredentialRemoveDup <- "
CREATE VIEW     Credential_Remove_Dup AS
SELECT DISTINCT ENCRYPTED_TRUE_PEN, PSI_CODE, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION, PSI_CREDENTIAL_CIP, PSI_CREDENTIAL_LEVEL, 
                PSI_CREDENTIAL_CATEGORY, CREDENTIAL_AWARD_DATE_D, MAX(DISTINCT id) AS ID
FROM            Credential
GROUP BY        ENCRYPTED_TRUE_PEN, PSI_CODE, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION, PSI_CREDENTIAL_CIP, PSI_CREDENTIAL_LEVEL, 
                PSI_CREDENTIAL_CATEGORY, CREDENTIAL_AWARD_DATE_D;"



# ---- qry05b_Lookingatdups ---- 
qry05b_Lookingatdups <- "
SELECT        Credential.ENCRYPTED_TRUE_PEN AS Expr1, Credential.PSI_STUDENT_NUMBER, Credential.PSI_CODE AS Expr2, Credential.PSI_PROGRAM_CODE, Credential.PSI_CREDENTIAL_PROGRAM_DESCRIPTION, 
              Credential.PSI_CREDENTIAL_CIP, Credential.PSI_CREDENTIAL_LEVEL, Credential.PSI_CREDENTIAL_CATEGORY, Credential.CREDENTIAL_AWARD_DATE_D, 
              STP_Credential_Record_Type.ID AS Expr8, STP_Credential_Record_Type.ID, STP_Credential_Record_Type.ENCRYPTED_TRUE_PEN, 
              STP_Credential_Record_Type.RecordStatus, STP_Credential_Record_Type.MinEnrolment, STP_Credential_Record_Type.FirstEnrolment, 
              STP_Credential_Record_Type.DropCredCategory, STP_Credential_Record_Type.DropPartialYear
FROM          Credential INNER JOIN
                         STP_Credential_Record_Type ON Credential.ID = STP_Credential_Record_Type.ID
WHERE        (STP_Credential_Record_Type.DropPartialYear IS NULL) AND (STP_Credential_Record_Type.DropCredCategory IS NULL) AND 
                         (STP_Credential_Record_Type.RecordStatus = 0)
ORDER BY Expr1;"


# ---- qry05c_UpdateAgeAtGrad ---- 
qry05c_UpdateAgeAtGrad <- "
UPDATE    Credential
SET       AGE_AT_GRAD = 
              CASE WHEN dateadd(year, datediff(year, psi_birthdate_cleaned_d, CREDENTIAL_AWARD_DATE_D), psi_birthdate_cleaned_d) > CREDENTIAL_AWARD_DATE_D 
              THEN datediff(year, psi_birthdate_cleaned_d, CREDENTIAL_AWARD_DATE_D) - 1 
              ELSE datediff(year, psi_birthdate_cleaned_d,  CREDENTIAL_AWARD_DATE_D) 
              END
WHERE     (psi_birthdate_cleaned IS NOT NULL) AND (psi_birthdate_cleaned NOT IN ('', ' '));"


# ---- qry05d_UpdateAGAtGrad ---- 
qry05d_UpdateAgeGroupAtGrad <- "
UPDATE    Credential
SET       AGE_GROUP_AT_GRAD = AgeGroupLookup.AgeIndex
FROM      Credential CROSS JOIN AgeGroupLookup
WHERE     (AgeGroupLookup.LowerBound <= Credential.AGE_AT_GRAD) AND (AgeGroupLookup.UpperBound >= Credential.AGE_AT_GRAD);"


# ---- qry06e_UpdateAwardSchoolYear ---- 
qry06e_UpdateAwardSchoolYear <- "
UPDATE Credential
SET    PSI_AWARD_SCHOOL_YEAR = CASE
	WHEN (Month(Credential.CREDENTIAL_AWARD_DATE_D) >= 9) THEN LTrim(Str(Year(Credential.CREDENTIAL_AWARD_DATE_D))) + '/' + LTrim(Str(Year(Credential.CREDENTIAL_AWARD_DATE_D)+1))
	ELSE LTrim(Str(Year(Credential.CREDENTIAL_AWARD_DATE_D)-1)) + '/' + LTrim(Str(Year(Credential.CREDENTIAL_AWARD_DATE_D)))
	END
WHERE (((Credential.PSI_AWARD_SCHOOL_YEAR) Is Null));"


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

# ---- qry07a1c_tmp_Credential_Gender ---- 
qry07a1c_tmp_Credential_Gender <- "
SELECT DISTINCT encrypted_true_pen,
                psi_student_number,
                psi_code,
                psi_gender_cleaned
INTO   tmp_credential_epen_gender
FROM   credential;"

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




# ---- qry07a1f_tmp_Credential_GenderDups_PickGender ---- 
qry07a1f_tmp_Credential_GenderDups_PickGender <- "
UPDATE      tmp_Dup_Credential_EPEN_Gender_MaxCredDate
SET         PSI_GENDER = Credential_Non_Dup.PSI_GENDER_cleaned
FROM        tmp_Dup_Credential_EPEN_Gender_MaxCredDate 
INNER JOIN  Credential_Non_Dup 
ON          tmp_Dup_Credential_EPEN_Gender_MaxCredDate.ENCRYPTED_TRUE_PEN = Credential_Non_Dup.ENCRYPTED_TRUE_PEN 
AND         tmp_Dup_Credential_EPEN_Gender_MaxCredDate.Max_Credential_Award_Date = Credential_Non_Dup.CREDENTIAL_AWARD_DATE_D"



# ---- qry07a1g_Update_Credential_Non_Dup_GenderDups ---- 
qry07a1g_Update_Credential_Non_Dup_GenderDups <- "
UPDATE    Credential_Non_Dup
SET       PSI_GENDER_CLEANED = tmp_Dup_Credential_EPEN_Gender_MaxCredDate.PSI_GENDER
FROM      tmp_Dup_Credential_EPEN_Gender_MaxCredDate 
INNER JOIN  Credential_Non_Dup 
ON        tmp_Dup_Credential_EPEN_Gender_MaxCredDate.ENCRYPTED_TRUE_PEN = Credential_Non_Dup.ENCRYPTED_TRUE_PEN 
AND       Credential_Non_Dup.PSI_GENDER_CLEANED <> tmp_Dup_Credential_EPEN_Gender_MaxCredDate.PSI_GENDER;"


# ---- qry07a1h_Update_Credential_GenderDups ---- 
qry07a1h_Update_Credential_GenderDups <- "
UPDATE    Credential
SET       PSI_GENDER_CLEANED = tmp_Dup_Credential_EPEN_Gender_MaxCredDate.PSI_GENDER
FROM      Credential 
INNER JOIN tmp_Dup_Credential_EPEN_Gender_MaxCredDate 
ON        Credential.ENCRYPTED_TRUE_PEN = tmp_Dup_Credential_EPEN_Gender_MaxCredDate.ENCRYPTED_TRUE_PEN 
AND       Credential.PSI_GENDER_CLEANED <> tmp_Dup_Credential_EPEN_Gender_MaxCredDate.PSI_GENDER;"


# ---- qry07a2a_ExtractNoGender ---- 
qry07a2a_ExtractNoGender <- "
SELECT    id, ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, psi_gender_cleaned, PSI_CREDENTIAL_CATEGORY 
INTO      CRED_Extract_No_Gender
FROM      Credential_Non_Dup
WHERE     psi_gender_cleaned IN ('', ' ', '(Unspecified)') OR psi_gender_cleaned IS NULL;"


# ---- qry07a2b_ExtractNoGenderUnique ---- 
qry07a2b_ExtractNoGenderUnique <- "
SELECT    DISTINCT ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, psi_gender_cleaned, PSI_CREDENTIAL_CATEGORY
INTO      CRED_Extract_No_Gender_Unique
FROM      CRED_Extract_No_Gender;"

# ---- qry07a2c_Create_CRED_Extract_No_Gender_EPEN_with_MultiCred ---- 
qry07a2c_Create_CRED_Extract_No_Gender_EPEN_with_MultiCred <- "
SELECT    ENCRYPTED_TRUE_PEN, psi_gender_cleaned, COUNT(*) AS Expr1
INTO      CRED_Extract_No_Gender_EPEN_with_MultiCred
FROM      CRED_Extract_No_Gender_Unique
GROUP BY  ENCRYPTED_TRUE_PEN, psi_gender_cleaned
HAVING    COUNT(*) > 1;"

# ---- qry07a2d_Update_MultiCredFlag ---- 
qry07a2d_Update_MultiCredFlag <- "
UPDATE    CRED_Extract_No_Gender_Unique
SET       MultiCredFlag = 'Y'
FROM      CRED_Extract_No_Gender_Unique
INNER JOIN CRED_Extract_No_Gender_EPEN_with_MultiCred 
ON        CRED_Extract_No_Gender_Unique.ENCRYPTED_TRUE_PEN = CRED_Extract_No_Gender_EPEN_with_MultiCred.ENCRYPTED_TRUE_PEN;"

# ---- qry07b_GenderDistribution ---- 
qry07b_GenderDistribution <- "
SELECT psi_gender_cleaned AS PSI_GENDER, PSI_CREDENTIAL_CATEGORY, COUNT(*) AS Expr1
FROM Credential_Non_Dup
GROUP BY psi_gender_cleaned, PSI_CREDENTIAL_CATEGORY;"

# ---- qry07c1_Assign_TopID_GenderF_AdvancedCert ---- 
qry07c1_Assign_TopID_GenderF_AdvancedCert <- "
UPDATE TOP (5) CRED_Extract_No_Gender_Unique
SET PSI_GENDER_CLEANED = 'Male'
WHERE PSI_CREDENTIAL_CATEGORY = 'ADVANCED CERTIFICATE';"

# ---- qry07c2_Assign_TopID_GenderF_AdvancedDip ---- 
qry07c2_Assign_TopID_GenderF_AdvancedDip <- "
UPDATE TOP (19) CRED_Extract_No_Gender_Unique
SET PSI_GENDER_CLEANED = 'Male'
WHERE PSI_CREDENTIAL_CATEGORY = 'ADVANCED DIPLOMA';
"

# ---- qry07c3_Assign_TopID_GenderF_Apprenticeship ---- 
qry07c3_Assign_TopID_GenderF_Apprenticeship <- "
UPDATE TOP (16) CRED_Extract_No_Gender_Unique
SET PSI_GENDER_CLEANED = 'Male'
WHERE PSI_CREDENTIAL_CATEGORY = 'APPRENTICESHIP';
"

# ---- qry07c4_Assign_TopID_GenderF_AssocDegree ---- 
qry07c4_Assign_TopID_GenderF_AssocDegree <- "
UPDATE TOP (16) CRED_Extract_No_Gender_Unique
SET PSI_GENDER_CLEANED = 'Male'
WHERE PSI_CREDENTIAL_CATEGORY = 'ASSOCIATE DEGREE';
"

# ---- qry07c5_Assign_TopID_GenderF_Bachelor ---- 
qry07c5_Assign_TopID_GenderF_Bachelor <- "
UPDATE TOP (1092) CRED_Extract_No_Gender_Unique
SET PSI_GENDER_CLEANED = 'Male'
WHERE PSI_CREDENTIAL_CATEGORY = 'BACHELORS DEGREE';
"

# ---- qry07c6_Assign_TopID_GenderF_Certificate ---- 
qry07c6_Assign_TopID_GenderF_Certificate <- "
UPDATE TOP (724) CRED_Extract_No_Gender_Unique
SET PSI_GENDER_CLEANED = 'Male'
WHERE PSI_CREDENTIAL_CATEGORY = 'CERTIFICATE';
"

# ---- qry07c7_Assign_TopID_GenderF_Diploma ---- 
qry07c7_Assign_TopID_GenderF_Diploma <- "
UPDATE TOP (427) CRED_Extract_No_Gender_Unique
SET PSI_GENDER_CLEANED = 'Male'
WHERE PSI_CREDENTIAL_CATEGORY = 'DIPLOMA';
"

# ---- qry07c8_Assign_TopID_GenderF_Doctorate ---- 
qry07c8_Assign_TopID_GenderF_Doctorate <- "
UPDATE TOP (73) CRED_Extract_No_Gender_Unique
SET PSI_GENDER_CLEANED = 'Male'
WHERE PSI_CREDENTIAL_CATEGORY = 'DOCTORATE';
"

# ---- qry07c9_Assign_TopID_GenderF_FirstProfDeg ---- 
qry07c9_Assign_TopID_GenderF_FirstProfDeg <- "
UPDATE TOP (22) CRED_Extract_No_Gender_Unique
SET PSI_GENDER_CLEANED = 'Male'
WHERE PSI_CREDENTIAL_CATEGORY = 'FIRST PROFESSIONAL DEGREE';"

# ---- qry07c10_Assign_TopID_GenderF_GradCert ---- 
qry07c10_Assign_TopID_GenderF_GradCert <- "
UPDATE TOP (0) CRED_Extract_No_Gender_Unique
SET PSI_GENDER_CLEANED = 'Male'
WHERE PSI_CREDENTIAL_CATEGORY = 'GRADUATE CERTIFICATE';
"

# ---- qry07c11_Assign_TopID_GenderF_GradDipl ---- 
qry07c11_Assign_TopID_GenderF_GradDipl <- "
UPDATE TOP (51) CRED_Extract_No_Gender_Unique
SET PSI_GENDER_CLEANED = 'Male'
WHERE PSI_CREDENTIAL_CATEGORY = 'GRADUATE DIPLOMA';
"

# ---- qry07c12_Assign_TopID_GenderF_Masters ---- 
qry07c12_Assign_TopID_GenderF_Masters <- "
UPDATE TOP (314) CRED_Extract_No_Gender_Unique
SET PSI_GENDER_CLEANED = 'Male'
WHERE PSI_CREDENTIAL_CATEGORY = 'MASTERS DEGREE';
"

# ---- qry07c13_Assign_TopID_GenderF_PostDegCert ---- 
qry07c13_Assign_TopID_GenderF_PostDegCert <- "
UPDATE TOP (28) CRED_Extract_No_Gender_Unique
SET PSI_GENDER_CLEANED = 'Male'
WHERE PSI_CREDENTIAL_CATEGORY = 'POST-DEGREE CERTIFICATE';
"

# ---- qry07c14_Assign_TopID_GenderF_PostDegDipl ---- 
qry07c14_Assign_TopID_GenderF_PostDegDipl <- "
UPDATE TOP (175) CRED_Extract_No_Gender_Unique
SET PSI_GENDER_CLEANED = 'Male'
WHERE PSI_CREDENTIAL_CATEGORY = 'POST-DEGREE DIPLOMA';
"


# ---- qry07c_Assign_TopID_GenderM ---- 
qry07c_Assign_TopID_GenderM <- "
UPDATE CRED_Extract_No_Gender_Unique
SET PSI_GENDER_CLEANED = 'Gender Diverse'
WHERE PSI_GENDER_CLEANED NOT IN('Female','Male') OR PSI_GENDER_CLEANED IS NULL;
"


# ---- qry07d_CorrectGender1 ---- 
qry07d_CorrectGender1 <- "
UPDATE CRED_Extract_No_Gender
SET PSI_GENDER_CLEANED = CRED_Extract_No_Gender_Unique.PSI_GENDER_CLEANED
FROM CRED_Extract_No_Gender_Unique
INNER JOIN CRED_Extract_No_Gender ON CRED_Extract_No_Gender_Unique.ENCRYPTED_TRUE_PEN = CRED_Extract_No_Gender.ENCRYPTED_TRUE_PEN;"


# ---- qry07d_CorrectGender2 ---- 
qry07d_CorrectGender2 <- "
UPDATE    Credential_Non_Dup
SET       PSI_GENDER_CLEANED = CRED_Extract_No_Gender.PSI_GENDER_CLEANED
FROM      CRED_Extract_No_Gender 
INNER JOIN Credential_Non_Dup ON CRED_Extract_No_Gender.id = Credential_Non_Dup.id;"


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
                         
# ---- qry08_Create_Credential_Ranking_View d ---- 
qry08_Create_Credential_Ranking_View_d <-                            
"SELECT        id, ENCRYPTED_TRUE_PEN, CREDENTIAL_AWARD_DATE_D, RANK, Highest_Cred_by_Date, Highest_Cred_by_Rank, Highest_Cred_by_School_Year
INTO              tmp_Credential_Ranking_step3
FROM            tmp_Credential_Ranking_step1"

# ---- qry08_Create_Credential_Ranking_View e ---- 
qry08_Create_Credential_Ranking_View_e <-   
"INSERT INTO tmp_Credential_Ranking_step3
                         (id, ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, CREDENTIAL_AWARD_DATE_D, RANK, Highest_Cred_by_Date, Highest_Cred_by_Rank, 
                         Highest_Cred_by_School_Year)
SELECT        id, ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, CREDENTIAL_AWARD_DATE_D, RANK, Highest_Cred_by_Date, Highest_Cred_by_Rank, 
                         Highest_Cred_by_School_Year
FROM            tmp_Credential_Ranking_step2"

# ---- qry08_Create_Credential_Ranking_View f ---- 
qry08_Create_Credential_Ranking_View_f <-   
"SELECT        id, ENCRYPTED_TRUE_PEN, CREDENTIAL_AWARD_DATE_D, RANK, Highest_Cred_by_Date, Highest_Cred_by_Rank, Highest_Cred_by_School_Year, 
                         PSI_STUDENT_NUMBER, PSI_CODE
FROM            tmp_Credential_Ranking_step3;"

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



# ---- qry08a1_Update_CredentialNonDup_with_highestDate_Rank ----
qry08a1_Update_CredentialNonDup_with_highestDate_Rank <- "
UPDATE  Credential_Non_Dup
SET     Highest_Cred_by_Date = tmp_Credential_Ranking.Highest_Cred_by_Date, 
        Highest_Cred_by_Rank = tmp_Credential_Ranking.Highest_Cred_by_Rank
FROM    Credential_Non_Dup
INNER JOIN tmp_Credential_Ranking ON Credential_Non_Dup.id = tmp_Credential_Ranking.id;
"

# ---- qry08a_Run_after_Credential_Ranking ----
qry08a_Run_after_Credential_Ranking <- "
UPDATE  Credential_Ranking
SET     Highest_Cred_by_Date = tmp_Credential_Ranking.Highest_Cred_by_Date, 
        Highest_Cred_by_Rank = tmp_Credential_Ranking.Highest_Cred_by_Rank
FROM    tmp_Credential_Ranking
INNER JOIN Credential_Ranking ON tmp_Credential_Ranking.id = Credential_Ranking.id;
"

# ---- qry08b_Rank_non_multi_cred ----
qry08b_Rank_non_multi_cred <- "
UPDATE Credential_Non_Dup
SET Credential_Non_Dup.Highest_Cred_by_Date = 'Yes', 
    Credential_Non_Dup.Highest_Cred_by_Rank = 'Yes'
WHERE NOT EXISTS (
SELECT * FROM tmp_Credential_Ranking
WHERE tmp_Credential_Ranking.ID = Credential_Non_Dup.ID)"


# ---- qry09a_ExtractNoAge ----
qry09a_ExtractNoAge <- "
SELECT  id, ENCRYPTED_TRUE_PEN, AGE_AT_GRAD, psi_gender_cleaned, PSI_AWARD_SCHOOL_YEAR, 
PSI_CREDENTIAL_CATEGORY, CREDENTIAL_AWARD_DATE_D, 0 AS LASTCRED, HIGHEST_CRED_BY_DATE
INTO    CRED_Extract_No_Age
FROM    Credential_Non_Dup
WHERE   (AGE_AT_GRAD IS NULL);
"

# ---- qry09b_ExtractNoAgeUnique ----
qry09b_ExtractNoAgeUnique <- "
SELECT  id, ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, AGE_AT_GRAD, PSI_GENDER_CLEANED, 
PSI_CREDENTIAL_CATEGORY, CREDENTIAL_AWARD_DATE_D, PSI_AWARD_SCHOOL_YEAR
INTO    CRED_Extract_No_Age_Unique
FROM    Credential_Non_Dup
WHERE   (AGE_AT_GRAD IS NULL) AND (Highest_Cred_by_Date = 'Yes');
"


# ---- qry09c_Create_CREDAgeDistributionGender ---- 
qry09c_Create_CREDAgeDistributionGender <- "CREATE TABLE CREDAgeDistributionbyGender(
	[PSI_GENDER_CLEANED] [varchar](10) NULL,
	[AGE_AT_GRAD] [numeric](18, 0) NULL,
	[PSI_CREDENTIAL_CATEGORY] [varchar](50) NULL,
	[NumGrads] [int] NULL,
	[PropGrads] [decimal](18, 5) NULL,
	[NumDistribution] [int] NULL
) ON [PRIMARY]
;"

# ---- qry09d_ShowAgeGenderDistribution ---- 
qry09d_ShowAgeGenderDistribution <- "
SELECT     PSI_GENDER_CLEANED, AGE_AT_GRAD, PSI_CREDENTIAL_CATEGORY, COUNT(*) AS NumGrads
FROM         Credential_Non_Dup
WHERE     (AGE_GROUP_AT_GRAD IS NOT NULL) AND (Highest_Cred_by_Date = 'Yes')
GROUP BY   PSI_GENDER_CLEANED, AGE_AT_GRAD, PSI_CREDENTIAL_CATEGORY;"


# ---- qry10_Update_Extract_No_Age ---- 
qry10_Update_Extract_No_Age <- "
UPDATE    CRED_Extract_No_Age
SET       AGE_AT_GRAD = CRED_Extract_No_Age_Unique.AGE_AT_GRAD
FROM      CRED_Extract_No_Age_Unique
INNER JOIN CRED_Extract_No_Age 
ON CRED_Extract_No_Age_Unique.id = CRED_Extract_No_Age.id;"

# ---- qry11a_UpdateAgeAtGrad ---- 
qry11a_UpdateAgeAtGrad <- "
UPDATE    Credential_Non_Dup
SET       AGE_AT_GRAD = CRED_Extract_No_Age.AGE_AT_GRAD
FROM      CRED_Extract_No_Age INNER JOIN
          Credential_Non_Dup ON CRED_Extract_No_Age.id = Credential_Non_Dup.id;"

# ---- qry11b_UpdateAGAtGrad ---- 
qry11b_UpdateAGAtGrad <- "
UPDATE    Credential_Non_Dup
SET       AGE_GROUP_AT_GRAD = AgeGroupLookup.AgeIndex
FROM      Credential_Non_Dup 
INNER JOIN AgeGroupLookup 
ON Credential_Non_Dup.AGE_AT_GRAD >= AgeGroupLookup.LowerBound AND Credential_Non_Dup.AGE_AT_GRAD <= AgeGroupLookup.UpperBound;"

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

# ---- qry13_UpdateDelayedCredDate ---- 
qry13_UpdateDelayedCredDate <- "
UPDATE  tblCredential_HighestRank
SET     CREDENTIAL_AWARD_DATE_D_DELAYED = CREDENTIAL_AWARD_DATE_D, 
        PSI_AWARD_SCHOOL_YEAR_DELAYED = PSI_AWARD_SCHOOL_YEAR
WHERE     (CREDENTIAL_AWARD_DATE_D_DELAYED IS NULL);"


# ---- qry13a_UpdateDelayedCredDate ---- 
qry13a_UpdateDelayedCredDate <- "
UPDATE    Credential_Non_Dup
SET              CREDENTIAL_AWARD_DATE_D_DELAYED = tblCredential_HighestRank.CREDENTIAL_AWARD_DATE_D_DELAYED, 
                      PSI_AWARD_SCHOOL_YEAR_DELAYED = tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED
FROM         tblCredential_HighestRank INNER JOIN
                      Credential_Non_Dup ON tblCredential_HighestRank.id = Credential_Non_Dup.id
WHERE     (tblCredential_HighestRank.CREDENTIAL_AWARD_DATE_D_DELAYED IS NOT NULL) AND 
                      (tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED IS NOT NULL);"

# ---- qry13b_UpdateDelayedCredDate ---- 
qry13b_UpdateDelayedCredDate <- "
UPDATE    Credential_Non_Dup
SET       CREDENTIAL_AWARD_DATE_D_DELAYED = CREDENTIAL_AWARD_DATE_D, 
          PSI_AWARD_SCHOOL_YEAR_DELAYED = PSI_AWARD_SCHOOL_YEAR
WHERE     (CREDENTIAL_AWARD_DATE_D_DELAYED IS NULL);"


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




# ---- qry14_ResearchUniversity_Exclude_LatestYr ---- 
qry14_ResearchUniversity_Exclude_LatestYr <- "
UPDATE    Credential_Non_Dup_Exclude_LatestYr
SET              RESEARCH_UNIVERSITY = 1
WHERE     (PSI_CODE = 'SFU') OR
                      (PSI_CODE = 'UBC') OR
                      (PSI_CODE = 'UBCO') OR
                      (PSI_CODE = 'UNBC') OR
                      (PSI_CODE = 'UVIC') OR
                      (PSI_CODE = 'RRU');"




# ---- qry15_OutcomeCredential ---- 
qry15_OutcomeCredential <- "UPDATE    Credential_Non_Dup
SET              OUTCOMES_CRED = OutcomeCredential.Outcomes_CRED
FROM         Credential_Non_Dup INNER JOIN
                      OutcomeCredential ON Credential_Non_Dup.PSI_CREDENTIAL_CATEGORY = OutcomeCredential.PSI_CREDENTIAL_CATEGORY;"


# ---- qry15_OutcomeCredential_Exclude_LatestYr ---- 
qry15_OutcomeCredential_Exclude_LatestYr <- "UPDATE    Credential_Non_Dup_Exclude_LatestYr
SET              OUTCOMES_CRED = OutcomeCredential.Outcomes_CRED
FROM         Credential_Non_Dup_Exclude_LatestYr INNER JOIN
                      OutcomeCredential ON Credential_Non_Dup_Exclude_LatestYr.PSI_CREDENTIAL_CATEGORY = OutcomeCredential.PSI_CREDENTIAL_CATEGORY;"


# ---- qry20a_1Credential_By_Year_AgeGroup ---- 
qry20a_1Credential_By_Year_AgeGroup <- "SELECT        AgeGroupLookup.AgeGroup, tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY, 
                         tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY + AgeGroupLookup.AgeGroup AS Expr1, 
                         tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED, COUNT(*) AS Count
FROM            tblCredential_HighestRank INNER JOIN
                         AgeGroupLookup ON tblCredential_HighestRank.AGE_GROUP_AT_GRAD = AgeGroupLookup.AgeIndex
GROUP BY AgeGroupLookup.AgeGroup, tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY, tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED
HAVING        (tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY <> 'APPRENTICESHIP')
ORDER BY AgeGroupLookup.AgeGroup, tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY, tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED;"


# ---- qry20a_1Credential_By_Year_AgeGroup_Exclude_CIPs ---- 
qry20a_1Credential_By_Year_AgeGroup_Exclude_CIPs <- "SELECT        AgeGroupLookup.AgeGroup, tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY, 
                         tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY + AgeGroupLookup.AgeGroup AS Expr1, 
                         tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED, COUNT(*) AS Count
FROM            tblCredential_HighestRank INNER JOIN
                         AgeGroupLookup ON tblCredential_HighestRank.AGE_GROUP_AT_GRAD = AgeGroupLookup.AgeIndex INNER JOIN
                         Credential_Non_Dup ON tblCredential_HighestRank.id = Credential_Non_Dup.id
WHERE        (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '09' AND Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '10')
GROUP BY AgeGroupLookup.AgeGroup, tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY, tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED
HAVING        (tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY <> 'APPRENTICESHIP')
ORDER BY AgeGroupLookup.AgeGroup, tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY, tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED;"


# ---- qry20a_2Credential_By_Year_AgeGroup_Domestic ---- 
qry20a_2Credential_By_Year_AgeGroup_Domestic <- "SELECT        AgeGroupLookup.AgeGroup, tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY, 
                         tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY + AgeGroupLookup.AgeGroup AS Expr1, 
                         tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED, COUNT(*) AS Count
FROM            tblCredential_HighestRank INNER JOIN
                         AgeGroupLookup ON tblCredential_HighestRank.AGE_GROUP_AT_GRAD = AgeGroupLookup.AgeIndex
WHERE        (tblCredential_HighestRank.PSI_VISA_STATUS = 'DOMESTIC') OR
                         (tblCredential_HighestRank.PSI_VISA_STATUS IS NULL)
GROUP BY AgeGroupLookup.AgeGroup, tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY, tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED
HAVING        (tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY <> 'APPRENTICESHIP')
ORDER BY AgeGroupLookup.AgeGroup, tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY, tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED;"



# ---- qry20a_2Credential_By_Year_AgeGroup_Domestic_Exclude_CIPs ---- 
qry20a_2Credential_By_Year_AgeGroup_Domestic_Exclude_CIPs <- "SELECT        AgeGroupLookup.AgeGroup, tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY, 
                         tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY + AgeGroupLookup.AgeGroup AS Expr1, 
                         tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED, COUNT(*) AS Count
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




# ---- qry20a_3Credential_By_Year_AgeGroup_Domestic_Exclude_RU_DACSO ---- 
qry20a_3Credential_By_Year_AgeGroup_Domestic_Exclude_RU_DACSO <- "SELECT        AgeGroupLookup.AgeGroup, tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY, 
                         tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY + AgeGroupLookup.AgeGroup AS Expr1, 
                         tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED, COUNT(*) AS Count
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


# ---- qry20a_4Credential_By_Year_CIP4_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs ---- 
qry20a_4Credential_By_Year_CIP4_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs <- "
SELECT        AgeGroupLookup.AgeGroup, tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY, 
                         tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY + AgeGroupLookup.AgeGroup AS Expr1, Credential_Non_Dup.FINAL_CIP_CODE_4, 
                         tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED, COUNT(*) AS Count
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



# ---- qry20a_4Credential_By_Year_CIP4_Gender_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs ---- 
qry20a_4Credential_By_Year_CIP4_Gender_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs <- "SELECT        tblCredential_HighestRank.psi_gender_cleaned, AgeGroupLookup.AgeGroup, tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY, 
                         tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY + AgeGroupLookup.AgeGroup + tblCredential_HighestRank.psi_gender_cleaned AS Expr1, 
                         Credential_Non_Dup.FINAL_CIP_CODE_4, tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED, COUNT(*) AS Count
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


# ---- qry20a_4Credential_By_Year_Gender_AgeGroup_Domestic_Exclude_CIPs ---- 
qry20a_4Credential_By_Year_Gender_AgeGroup_Domestic_Exclude_CIPs <- "SELECT        tblCredential_HighestRank.psi_gender_cleaned, AgeGroupLookup.AgeGroup, tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY, 
                         tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY + AgeGroupLookup.AgeGroup + tblCredential_HighestRank.psi_gender_cleaned AS Expr1, 
                         tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED, COUNT(*) AS Count
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


# ---- qry20a_4Credential_By_Year_Gender_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs ---- 
qry20a_4Credential_By_Year_Gender_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs <- "SELECT        tblCredential_HighestRank.psi_gender_cleaned, AgeGroupLookup.AgeGroup, tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY, 
                         tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY + AgeGroupLookup.AgeGroup + tblCredential_HighestRank.psi_gender_cleaned AS Expr1, tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED, COUNT(*) 
                         AS Count
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


# ---- qry20a_4Credential_By_Year_PSI_TYPE_Domestic_Exclude_RU_DACSO_Exclude_CIPs ---- 
qry20a_4Credential_By_Year_PSI_TYPE_Domestic_Exclude_RU_DACSO_Exclude_CIPs <- "SELECT        PSI_CODE_RECODE.PSI_TYPE_RECODE, tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY, 
                         tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY AS Expr1, tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED, COUNT(*) AS Count
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


# ---- qry20a_4Credential_By_Year_PSI_TYPE_Domestic_Exclude_RU_DACSO_Exclude_CIPs_Not_Highest ---- 
qry20a_4Credential_By_Year_PSI_TYPE_Domestic_Exclude_RU_DACSO_Exclude_CIPs_Not_Highest <- "SELECT        PSI_CODE_RECODE.PSI_TYPE_RECODE, Credential_Non_Dup.PSI_CREDENTIAL_CATEGORY, Credential_Non_Dup.PSI_CREDENTIAL_CATEGORY AS Expr1, 
                         Credential_Non_Dup.PSI_AWARD_SCHOOL_YEAR_DELAYED, COUNT(*) AS Count
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


# ---- qry20a_99_Checking_Excluding_RU_DACSO_Variables ---- 
qry20a_99_Checking_Excluding_RU_DACSO_Variables <- "SELECT        RESEARCH_UNIVERSITY, OUTCOMES_CRED, PSI_CODE, PSI_CREDENTIAL_CATEGORY, PSI_AWARD_SCHOOL_YEAR_DELAYED, COUNT(*) AS Expr1
FROM            Credential_Non_Dup
GROUP BY RESEARCH_UNIVERSITY, PSI_CODE, OUTCOMES_CRED, PSI_CREDENTIAL_CATEGORY, PSI_AWARD_SCHOOL_YEAR_DELAYED
HAVING        (RESEARCH_UNIVERSITY = 1) AND (OUTCOMES_CRED = 'DACSO') AND (PSI_AWARD_SCHOOL_YEAR_DELAYED = '2018/2019')
ORDER BY OUTCOMES_CRED, RESEARCH_UNIVERSITY;"



# ---- qry_Update_Cdtl_Sup_Vars_InternationalFlag ---- 
qry_Update_Cdtl_Sup_Vars_InternationalFlag <- "UPDATE    CredentialSupVars SET  International_Include_Flag = 
tbl_CredentialHighestRank_International.International_Include_Flag 
FROM         CredentialSupVars INNER JOIN 
tbl_CredentialHighestRank_International ON 
CredentialSupVars.ID = tbl_CredentialHighestRank_International.id;"


# ---- NOT USED ------

#  ---- qry04a_UpdateCredentialSupVarsRecordStatus  ---- 
qry04a_UpdateCredentialSupVarsRecordStatus <- "
UPDATE    CredentialSupVars
SET       CredentialRecordStatus = 0
WHERE     (CredentialRecordStatus = 4);" 





# ---- qry08_Create_Credential_Ranking_View_Exclude_LatestYr ----
qry08_Create_Credential_Ranking_View_Exclude_LatestYr <- "
SELECT  a.id, 
        a.ENCRYPTED_TRUE_PEN, 
        a.CREDENTIAL_AWARD_DATE_D, 
        CredentialRank.RANK, 
        a.Highest_Cred_by_Date, 
        a.Highest_Cred_by_Rank, 
        a.Highest_Cred_by_School_Year, 
        a.PSI_AWARD_SCHOOL_YEAR
FROM    Credential_Non_Dup AS a 
INNER JOIN CredentialRank ON a.PSI_CREDENTIAL_CATEGORY = CredentialRank.PSI_CREDENTIAL_CATEGORY
WHERE   (a.ENCRYPTED_TRUE_PEN IN (
            SELECT  ENCRYPTED_TRUE_PEN
            FROM    Credential_Non_Dup AS b
            GROUP BY ENCRYPTED_TRUE_PEN
            HAVING  (COUNT(ENCRYPTED_TRUE_PEN) > 1)
        )) 
        AND (a.PSI_AWARD_SCHOOL_YEAR <> '2011/2012');"


# ---- qry08a_Run_after_Credential_Ranking_Exclude_LatestYr ----
qry08a_Run_after_Credential_Ranking_Exclude_LatestYr <- "
UPDATE  Credential_Ranking_Exclude_LatestYr
SET     Highest_Cred_by_Date = tmp_Credential_Ranking_Exclude_LatestYr.Highest_Cred_by_Date, 
        Highest_Cred_by_Rank = tmp_Credential_Ranking_Exclude_LatestYr.Highest_Cred_by_Rank
        --Highest_Cred_by_School_Year = tmp_Credential_Ranking_Exclude_LatestYr.Highest_Cred_by_School_Year
FROM    tmp_Credential_Ranking_Exclude_LatestYr
INNER JOIN Credential_Ranking_Exclude_LatestYr ON tmp_Credential_Ranking_Exclude_LatestYr.id = Credential_Ranking_Exclude_LatestYr.id;
"

# ---- qry12_Create_View_tblCredentialHighestRank_Exclude_LatestYr ---- 
qry12_Create_View_tblCredentialHighestRank_Exclude_LatestYr <- 
  "SELECT     Credential_Non_Dup_Exclude_LatestYr.id, 
            Credential_Non_Dup_Exclude_LatestYr.PSI_PEN, Credential_Non_Dup_Exclude_LatestYr.PSI_BIRTHDATE, 
            Credential_Non_Dup_Exclude_LatestYr.psi_birthdate_cleaned, Credential_Non_Dup_Exclude_LatestYr.PSI_GENDER, 
            Credential_Non_Dup_Exclude_LatestYr.PSI_STUDENT_NUMBER, Credential_Non_Dup_Exclude_LatestYr.ENCRYPTED_TRUE_PEN, 
            Credential_Non_Dup_Exclude_LatestYr.PSI_SCHOOL_YEAR, Credential_Non_Dup_Exclude_LatestYr.PSI_STUD_POSTAL_CD_CURR, 
            Credential_Non_Dup_Exclude_LatestYr.PSI_ENROLMENT_SEQUENCE, Credential_Non_Dup_Exclude_LatestYr.PSI_CODE, 
            Credential_Non_Dup_Exclude_LatestYr.PSI_MIN_START_DATE, Credential_Non_Dup_Exclude_LatestYr.CREDENTIAL_AWARD_DATE, 
            Credential_Non_Dup_Exclude_LatestYr.RecordStatus, Credential_Non_Dup_Exclude_LatestYr.PSI_PROGRAM_CODE, 
            Credential_Non_Dup_Exclude_LatestYr.PSI_CREDENTIAL_PROGRAM_DESCRIPTION, Credential_Non_Dup_Exclude_LatestYr.PSI_CIP_CODE, 
            Credential_Non_Dup_Exclude_LatestYr.PSI_CREDENTIAL_CIP, Credential_Non_Dup_Exclude_LatestYr.PSI_CONTINUING_EDUCATION_COURSE_ONLY, 
            Credential_Non_Dup_Exclude_LatestYr.PSI_CREDENTIAL_LEVEL, Credential_Non_Dup_Exclude_LatestYr.PSI_CREDENTIAL_CATEGORY, 
            Credential_Non_Dup_Exclude_LatestYr.PSI_MIN_START_DATE_D, Credential_Non_Dup_Exclude_LatestYr.CREDENTIAL_AWARD_DATE_D, 
            Credential_Non_Dup_Exclude_LatestYr.AGE_AT_GRAD, Credential_Non_Dup_Exclude_LatestYr.AGE_GROUP_AT_GRAD, 
            Credential_Non_Dup_Exclude_LatestYr.PSI_BIRTHDATE_D, Credential_Non_Dup_Exclude_LatestYr.psi_birthdate_cleaned_D, 
            Credential_Non_Dup_Exclude_LatestYr.PSI_AWARD_SCHOOL_YEAR, Credential_Non_Dup_Exclude_LatestYr.RECORD_TO_DELETE, 
            Credential_Non_Dup_Exclude_LatestYr.Last_Date_Highest_Cred, Credential_Non_Dup_Exclude_LatestYr.Highest_Cred_by_Date, 
            Credential_Non_Dup_Exclude_LatestYr.Highest_Cred_by_Rank, Credential_Non_Dup_Exclude_LatestYr.OUTCOMES_CRED, 
            Credential_Non_Dup_Exclude_LatestYr.Highest_Cred_by_School_Year, Credential_Non_Dup_Exclude_LatestYr.RESEARCH_UNIVERSITY, 
            Credential_Non_Dup_Exclude_LatestYr.CredentialRecordStatus, CredentialSupVars.CREDENTIAL_AWARD_DATE_D_DELAYED, 
            CredentialSupVars.PSI_AWARD_SCHOOL_YEAR_DELAYED, CredentialSupVars.PSI_VISA_STATUS
FROM        Credential_Non_Dup_Exclude_LatestYr INNER JOIN
CredentialSupVars ON Credential_Non_Dup_Exclude_LatestYr.id = CredentialSupVars.ID
WHERE     (Credential_Non_Dup_Exclude_LatestYr.Highest_Cred_by_Rank = 'Yes')"







