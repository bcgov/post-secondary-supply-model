# ---- qry01a_MinEnrolmentSupVar ---- 
qry01a_MinEnrolmentSupVar <- "
SELECT    STP_Enrolment.ID, STP_Enrolment.psi_birthdate_cleaned, STP_Enrolment.PSI_MIN_START_DATE, 
          STP_Enrolment_Record_Type.MinEnrolment, STP_Enrolment_Record_Type.FirstEnrolment, STP_Enrolment_Record_Type.RecordStatus
INTO      MinEnrolmentSupVar
FROM      STP_Enrolment 
INNER JOIN STP_Enrolment_Record_Type 
  ON      STP_Enrolment.ID = STP_Enrolment_Record_Type.ID;"


# ---- qry01b_MinEnrolmentSupVar ---- 
qry01b_MinEnrolmentSupVar <- "ALTER TABLE MinEnrolmentSupVar 
ADD
	psi_birthdate_cleaned_D date NULL,
	PSI_MIN_START_DATE_D date NULL,
	AGE_AT_ENROL_DATE numeric(18, 0) NULL,
	AGE_GROUP_ENROL_DATE numeric(18, 0) NULL,
	AGE_AT_CENSUS_2016 numeric(18, 0) NULL,
	AGE_GROUP_CENSUS_2016 numeric(18, 0) NULL,
	IS_FIRST_ENROLMENT varchar(255) NULL,
	IS_SKILLS_BASED int NULL;"


# ---- qry01c_MinEnrolmentSupVar ---- 
qry01c_MinEnrolmentSupVar <- "
UPDATE    MinEnrolmentSupvar
SET       psi_birthdate_cleaned_D = psi_birthdate_cleaned
WHERE     psi_birthdate_cleaned is not null;"


# ---- qry01d_MinEnrolmentSupVar ---- --- 
qry01d1_MinEnrolmentSupVar <- "
UPDATE MinEnrolmentSupvar
SET    PSI_MIN_START_DATE_D = PSI_MIN_START_DATE
WHERE     (PSI_MIN_START_DATE <> '' AND PSI_MIN_START_DATE <> '(Unspecified)';"

# ---- qry01d_MinEnrolmentSupVar ---- 
qry01d2_MinEnrolmentSupVar <- "
UPDATE      MinEnrolmentSupVar
SET         psi_birthdate_cleaned_D = NULL
WHERE       psi_birthdate_cleaned_D = CONVERT(DATETIME, '1900-01-01 00:00:00', 102)
  AND       (
              psi_birthdate_cleaned IS NULL 
              OR psi_birthdate_cleaned = '' 
              OR psi_birthdate_cleaned = '(Unspecified)'
);"


# ---- qry01e_MinEnrolmentSupVar ---- 
qry01e_MinEnrolmentSupVar <- "
UPDATE    MinEnrolmentSupVar
SET       IS_FIRST_ENROLMENT = 'Yes'
WHERE     FirstEnrolment = 1;"


# ---- qry02a_UpdateAgeAtEnrol ---- 
qry02a_UpdateAgeAtEnrol <- "
UPDATE    MinEnrolment
SET       AGE_AT_ENROL_DATE = 
            CASE WHEN dateadd(year, datediff (year, psi_birthdate_cleaned_D, PSI_MIN_START_DATE_D), psi_birthdate_cleaned_D) > PSI_MIN_START_DATE_D
		             THEN datediff (year, psi_birthdate_cleaned_D, PSI_MIN_START_DATE_D) - 1
		             ELSE datediff (year, psi_birthdate_cleaned_D, PSI_MIN_START_DATE_D)
		        END
WHERE     psi_birthdate_cleaned_D IS NOT NULL
;"


# ---- qry02b_UpdateAGAtEnrol ---- 
qry02b_UpdateAGAtEnrol <- "
UPDATE    MinEnrolment
SET       AGE_GROUP_ENROL_DATE = AgeGroupLookup.AgeIndex
FROM      MinEnrolment 
CROSS JOIN AgeGroupLookup
WHERE     AgeGroupLookup.LowerBound <= MinEnrolment.AGE_AT_ENROL_DATE
  AND     AgeGroupLookup.UpperBound >= MinEnrolment.AGE_AT_ENROL_DATE;"


# ---- qry04a*_UpdateMinEnrolment_Gender ---- 
qry04a1_UpdateMinEnrolment_Gender <- "
UPDATE      MinEnrolment
SET         PSI_GENDER = Credential.PSI_GENDER_CLEANED
FROM        MinEnrolment 
INNER JOIN  Credential
  ON        MinEnrolment.ENCRYPTED_TRUE_PEN = Credential.ENCRYPTED_TRUE_PEN 
  AND       MinEnrolment.PSI_GENDER <> Credential.PSI_GENDER_CLEANED
WHERE       MinEnrolment.ENCRYPTED_TRUE_PEN <> ''
  AND       MinEnrolment.ENCRYPTED_TRUE_PEN IS NOT NULL
  AND       MinEnrolment.ENCRYPTED_TRUE_PEN <> '(Unspecified)'
;"

qry04a2_UpdateMinEnrolment_Gender <- "
UPDATE      MinEnrolment
SET         PSI_GENDER = Credential.psi_gender_cleaned
FROM        MinEnrolment 
INNER JOIN  Credential 
  ON        MinEnrolment.PSI_STUDENT_NUMBER = Credential.PSI_STUDENT_NUMBER 
  AND       MinEnrolment.PSI_CODE = Credential.PSI_CODE 
  AND       MinEnrolment.PSI_GENDER <> Credential.psi_gender_cleaned
WHERE       MinEnrolment.ENCRYPTED_TRUE_PEN IS NULL
  OR        MinEnrolment.ENCRYPTED_TRUE_PEN = ''
  OR        MinEnrolment.ENCRYPTED_TRUE_PEN = '(Unspecified)'
;"


# ---- qry04b*_tmp_MinEnrolment_Gender ---- 
qry04b1_tmp_MinEnrolment_Gender <- "
SELECT    DISTINCT PSI_GENDER, ENCRYPTED_TRUE_PEN
INTO      tmp_MinEnrolment_EPEN_Gender_step1
FROM      MinEnrolment
WHERE     ENCRYPTED_TRUE_PEN <> '' 
AND       ENCRYPTED_TRUE_PEN IS NOT NULL
AND       ENCRYPTED_TRUE_PEN <> '(Unspecified)'
;"

qry04b2_tmp_MinEnrolment_Gender <-"
SELECT    DISTINCT PSI_GENDER, PSI_STUDENT_NUMBER, PSI_CODE
INTO      tmp_MinEnrolment_STUDNUM_PSICODE_Gender_step1
FROM      MinEnrolment
WHERE     ENCRYPTED_TRUE_PEN = '' 
OR        ENCRYPTED_TRUE_PEN IS NULL
OR        ENCRYPTED_TRUE_PEN = '(Unspecified)'
;"

# Make a new table called tmp_MinEnrolment_EPEN_Gender and append the tmp_MinEnrolment_EPEN_Gender_step1 records to it. */
qry04b3_tmp_MinEnrolment_Gender <-"
SELECT  tmp_MinEnrolment_EPEN_Gender_step1.* 
INTO    tmp_MinEnrolment_EPEN_Gender
FROM    tmp_MinEnrolment_EPEN_Gender_step1;"

# In the tmp_MinEnrolment_EPEN_Gender add cols for PSI_STUDENT_NUMBER, PSI_CODE and CONCATENATED_ID */
qry04b4_tmp_MinEnrolment_Gender <-"
ALTER TABLE tmp_MinEnrolment_EPEN_Gender
  ADD   PSI_STUDENT_NUMBER varchar(50), 
        PSI_CODE varchar(50),
        CONCATENATED_ID varchar(50);"

# Append the tmp_MinEnrolment_STUDNUM_PSICODE_Gender_step1 records to the tmp_MinEnrolment_EPEN_Gender */
qry04b5_tmp_MinEnrolment_Gender <-"
INSERT INTO tmp_MinEnrolment_EPEN_Gender (PSI_GENDER, PSI_STUDENT_NUMBER, PSI_CODE)
SELECT      PSI_GENDER, PSI_STUDENT_NUMBER, PSI_CODE 
FROM        tmp_MinEnrolment_STUDNUM_PSICODE_Gender_step1;"

qry04b6_tmp_MinEnrolment_Gender <- "
UPDATE      tmp_MinEnrolment_EPEN_Gender
SET         CONCATENATED_ID = ENCRYPTED_TRUE_PEN
WHERE       ENCRYPTED_TRUE_PEN IS NOT NULL
AND         ENCRYPTED_TRUE_PEN <> ''
AND         ENCRYPTED_TRUE_PEN <> '(Unspecified)'
;"

qry04b7_tmp_MinEnrolment_Gender <- "
UPDATE      tmp_MinEnrolment_EPEN_Gender
SET         CONCATENATED_ID = PSI_STUDENT_NUMBER + PSI_CODE
WHERE       (ENCRYPTED_TRUE_PEN IS NULL) 
OR          (ENCRYPTED_TRUE_PEN = '')
OR          (ENCRYPTED_TRUE_PEN = '(Unspecified)')
;"

# ---- qry04c_tmp_MinEnrolment_GenderDups ---- 
qry04c_tmp_MinEnrolment_GenderDups <- "
SELECT     CONCATENATED_ID, COUNT(*) AS Expr1
INTO       tmp_Dup_MinEnrolment_EPEN_Gender
FROM       tmp_MinEnrolment_EPEN_Gender
GROUP BY CONCATENATED_ID
HAVING      (COUNT(*) > 1);

ALTER TABLE tmp_Dup_MinEnrolment_EPEN_Gender
ADD PSI_GENDER_FirstEnrolment varchar(50);
;"

# ---- qry04d*_tmp_MinEnrolment_GenderDups_PickGender ---- 
qry04d1_tmp_MinEnrolment_GenderDups_PickGender <- "
UPDATE    tmp_Dup_MinEnrolment_EPEN_Gender
SET       PSI_GENDER_FirstEnrolment = MinEnrolment.PSI_GENDER
FROM      tmp_Dup_MinEnrolment_EPEN_Gender 
INNER JOIN MinEnrolment 
ON        tmp_Dup_MinEnrolment_EPEN_Gender.CONCATENATED_ID = MinEnrolment.ENCRYPTED_TRUE_PEN
WHERE     (MinEnrolment.IS_FIRST_ENROLMENT = 'Yes');"


qry04d2_tmp_MinEnrolment_GenderDups_PickGender <- "
UPDATE    tmp_Dup_MinEnrolment_EPEN_Gender
SET              PSI_GENDER_FirstEnrolment = MinEnrolment.PSI_GENDER
FROM         tmp_Dup_MinEnrolment_EPEN_Gender 
INNER JOIN MinEnrolment 
ON tmp_Dup_MinEnrolment_EPEN_Gender.CONCATENATED_ID = (MinEnrolment.PSI_STUDENT_NUMBER+MinEnrolment.PSI_CODE)
WHERE     (MinEnrolment.IS_FIRST_ENROLMENT = 'Yes');"

# Select records with 'U' to tmp table 
qry04d3_tmp_MinEnrolment_GenderDups_PickGender <- "
SELECT *   
INTO  tmp_Dup_MinEnrolment_EPEN_Gender_Unknowns
FROM         tmp_Dup_MinEnrolment_EPEN_Gender
WHERE PSI_GENDER_FirstEnrolment <> 'Male' 
AND PSI_GENDER_FirstEnrolment <> 'Female'
AND PSI_GENDER_FirstEnrolment <> 'Gender Diverse'
"

# Alter table to add other gender variable 
qry04d4_tmp_MinEnrolment_GenderDups_PickGender <- "
ALTER TABLE tmp_Dup_MinEnrolment_EPEN_Gender_Unknowns
ADD PSI_GENDER_FinalUnknowns VARCHAR(50)"

# Update new gender variable with gender <> 'U' 
qry04d5_tmp_MinEnrolment_GenderDups_PickGender <- "
UPDATE       tmp_Dup_MinEnrolment_EPEN_Gender_Unknowns
SET                PSI_GENDER_FinalUnknowns = tmp_MinEnrolment_EPEN_Gender.psi_gender
FROM            tmp_Dup_MinEnrolment_EPEN_Gender_Unknowns 
INNER JOIN      tmp_MinEnrolment_EPEN_Gender 
ON tmp_Dup_MinEnrolment_EPEN_Gender_Unknowns.CONCATENATED_ID = tmp_MinEnrolment_EPEN_Gender.CONCATENATED_ID
WHERE        tmp_MinEnrolment_EPEN_Gender.psi_gender <> 'Unknown'"

# Update original table with non-U gender 
qry04d6_tmp_MinEnrolment_GenderDups_PickGender <- "
UPDATE       tmp_Dup_MinEnrolment_EPEN_Gender
SET                PSI_GENDER_FirstEnrolment = tmp_Dup_MinEnrolment_EPEN_Gender_Unknowns.PSI_GENDER_FinalUnknowns
FROM            tmp_Dup_MinEnrolment_EPEN_Gender
INNER JOIN      tmp_Dup_MinEnrolment_EPEN_Gender_Unknowns
ON tmp_Dup_MinEnrolment_EPEN_Gender.CONCATENATED_ID = tmp_Dup_MinEnrolment_EPEN_Gender_Unknowns.CONCATENATED_ID"


# ---- qry04e_UpdateMinEnrolment_EPEN_GenderDups ---- 
qry04e1_UpdateMinEnrolment_EPEN_GenderDups <- "
UPDATE       MinEnrolment
SET          PSI_GENDER = tmp_Dup_MinEnrolment_EPEN_Gender.PSI_GENDER_FirstEnrolment
FROM         MinEnrolment 
INNER JOIN   tmp_Dup_MinEnrolment_EPEN_Gender 
ON           tmp_Dup_MinEnrolment_EPEN_Gender.CONCATENATED_ID = MinEnrolment.ENCRYPTED_TRUE_PEN;"

qry04e2_UpdateMinEnrolment_EPEN_GenderDups <- "
UPDATE       MinEnrolment
SET          PSI_GENDER = tmp_Dup_MinEnrolment_EPEN_Gender.PSI_GENDER_FirstEnrolment
FROM         MinEnrolment 
INNER JOIN   tmp_Dup_MinEnrolment_EPEN_Gender 
ON           tmp_Dup_MinEnrolment_EPEN_Gender.CONCATENATED_ID = (MinEnrolment.PSI_STUDENT_NUMBER+MinEnrolment.PSI_CODE);"

# ---- qry05a1_Extract_No_Gender ---- 
qry05a1_Extract_No_Gender <- "
SELECT    DISTINCT id, ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, PSI_GENDER 
INTO      Extract_No_Gender
FROM      MinEnrolment
WHERE     PSI_GENDER='U' 
OR        PSI_GENDER = 'Unknown'
OR        PSI_GENDER = '(Unspecified)'
OR        PSI_GENDER = '' 
OR        PSI_GENDER IS NULL;"

# ---- qry05a1_Extract_No_Gender_First_Enrolment ---- 
qry05a1_Extract_No_Gender_First_Enrolment <- "
SELECT    DISTINCT id, ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, PSI_GENDER 
INTO      Extract_No_Gender_First_Enrolment
FROM      MinEnrolment
WHERE     (PSI_GENDER ='Unknown' OR PSI_GENDER = '(Unspecified)' OR PSI_GENDER ='' OR PSI_GENDER IS NULL) 
AND       IS_FIRST_ENROLMENT='Yes';"

# ---- qry05a2_Show_Gender_Distribution ---- 
qry05a2_Show_Gender_Distribution <- "
SELECT     PSI_GENDER, COUNT(*) AS NumEnrolled 
FROM       MinEnrolment
WHERE      PSI_GENDER <> 'Unknown' 
  AND      PSI_GENDER <> '' 
  AND      PSI_GENDER <> '(Unspecified)'
  AND      PSI_GENDER IS NOT NULL 
  AND      IS_FIRST_ENROLMENT='Yes'
GROUP BY   PSI_GENDER;"

# ---- qry06a1_Assign_TopID_Gender ---- 
qry06a1_Assign_TopID_Gender <- "
UPDATE    TOP (5429) Extract_No_Gender_First_Enrolment
SET       PSI_GENDER = 'Female';"

# ---- qry06a2_Assign_TopID_Gender2 ---- 
qry06a2_Assign_TopID_Gender2 <- "
UPDATE    TOP (5177) Extract_No_Gender_First_Enrolment
SET       PSI_GENDER = 'Male'
WHERE     PSI_GENDER = 'U' 
    OR    PSI_GENDER = 'Unknown'
    OR    PSI_GENDER ='(Unspecified)'
    OR    PSI_GENDER='' 
    OR    PSI_GENDER IS NULL;"

# ---- qry06a2_Assign_TopID_Gender3 ---- 
qry06a2_Assign_TopID_Gender3 <- "
UPDATE    Extract_No_Gender_First_Enrolment
SET       PSI_GENDER = 'Gender Diverse'
WHERE     PSI_GENDER = 'U' 
    OR    PSI_GENDER = 'Unknown'
    OR    PSI_GENDER ='(Unspecified)'
    OR    PSI_GENDER='' 
    OR    PSI_GENDER IS NULL;"


# ---- qry06a3*_CorrectGender1 ---- 
qry06a3_CorrectGender1 <- "
UPDATE    Extract_No_Gender
SET       PSI_GENDER = Extract_No_Gender_First_Enrolment.PSI_GENDER
FROM      Extract_No_Gender_First_Enrolment 
INNER JOIN Extract_No_Gender 
    ON    Extract_No_Gender_First_Enrolment.PSI_STUDENT_NUMBER = Extract_No_Gender.PSI_STUDENT_NUMBER 
    AND   Extract_No_Gender_First_Enrolment.PSI_CODE = Extract_No_Gender.PSI_CODE;"

qry06a3_CorrectGender2 <- "
UPDATE    TOP (40) Extract_No_Gender
SET       PSI_GENDER ='Female'
WHERE     PSI_GENDER IS NULL 
OR        PSI_GENDER = ' ' 
OR        PSI_GENDER = 'U'
OR        PSI_GENDER = 'Unknown'
OR        PSI_GENDER = '(Unspecified)'
"

qry06a3_CorrectGender3 <- "
UPDATE    TOP (39) Extract_No_Gender
SET       PSI_GENDER ='Male'
WHERE     PSI_GENDER IS NULL 
OR        PSI_GENDER = ' ' 
OR        PSI_GENDER = 'U'
OR        PSI_GENDER = 'Unknown'
OR        PSI_GENDER = '(Unspecified)'
"

qry06a3_CorrectGender4 <- "
UPDATE    Extract_No_Gender
SET       PSI_GENDER = 'Gender Diverse'
WHERE     PSI_GENDER IS NULL 
OR        PSI_GENDER = ' ' 
OR        PSI_GENDER = 'U'
OR        PSI_GENDER = 'Unknown'
OR        PSI_GENDER = '(Unspecified)'
"


# ---- qry06a4a_ExtractNoGender_DupEPENS ---- 
qry06a4a_ExtractNoGender_DupEPENS <- "
SELECT    ENCRYPTED_TRUE_PEN, PSI_GENDER
INTO      tmp_Extract_No_Gender_EPENS
FROM      Extract_No_Gender
WHERE     ENCRYPTED_TRUE_PEN <> '' 
AND       ENCRYPTED_TRUE_PEN IS NOT NULL
AND       ENCRYPTED_TRUE_PEN <> '(Unspecified)'
GROUP BY  ENCRYPTED_TRUE_PEN, PSI_GENDER;"


# ---- qry06a4b_ExtractNoGender_DupEPENS ---- 
qry06a4b_ExtractNoGender_DupEPENS_1 <- "
SELECT     ENCRYPTED_TRUE_PEN, COUNT(*) AS Expr1
INTO       tmp_Extract_No_Gender_DupEPENS
FROM       tmp_Extract_No_Gender_EPENS
GROUP BY   ENCRYPTED_TRUE_PEN
HAVING     COUNT(*) > 1;"

qry06a4b_ExtractNoGender_DupEPENS_2 <- 
"UPDATE    TOP (10) tmp_Extract_No_Gender_DupEPENS
SET        PSI_GENDER_to_use ='Female'
WHERE      PSI_GENDER_to_use IS NULL;

UPDATE     TOP (0) tmp_Extract_No_Gender_DupEPENS
SET        PSI_GENDER_to_use ='Gender Diverse'
WHERE      PSI_GENDER_to_use IS NULL;

UPDATE     tmp_Extract_No_Gender_DupEPENS
SET        PSI_GENDER_to_use ='Male'
WHERE      PSI_GENDER_to_use IS NULL;
"


# ---- qry06a4c_Update_ExtractNoGender_DupEPENS ---- 
qry06a4c_Update_ExtractNoGender_DupEPENS <- "
UPDATE     Extract_No_Gender
SET        PSI_GENDER = tmp_Extract_No_Gender_DupEPENS.PSI_GENDER_TO_USE
FROM       Extract_No_Gender 
INNER JOIN tmp_Extract_No_Gender_DupEPENS 
  ON       Extract_No_Gender.ENCRYPTED_TRUE_PEN = tmp_Extract_No_Gender_DupEPENS.ENCRYPTED_TRUE_PEN;"

# ---- qry06a4c_Check_Prop  ----
qry06a4c_Check_Prop  <- "
SELECT     PSI_GENDER, COUNT(*) AS NumEnrolled
FROM       MinEnrolment
WHERE      PSI_GENDER <> 'Unknown' 
AND        PSI_GENDER <> '' 
AND        PSI_GENDER <> '(Unspecified)'
AND        PSI_GENDER IS NOT NULL 
AND        IS_FIRST_ENROLMENT='Yes'
GROUP BY   PSI_GENDER
"

# ---- qry06a5_CorrectGender2 ---- 
qry06a5_CorrectGender2 <- "
UPDATE    MinEnrolment
SET       PSI_GENDER = Extract_No_Gender.PSI_GENDER
FROM      Extract_No_Gender 
INNER JOIN MinEnrolment ON Extract_No_Gender.id = MinEnrolment.id;"

# ---- qry07a_Extract_No_Age ---- 
qry07a_Extract_No_Age <- "
SELECT DISTINCT id, ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, AGE_AT_ENROL_DATE, 
PSI_SCHOOL_YEAR, PSI_MIN_START_DATE, PSI_MIN_START_DATE_D
INTO    Extract_No_Age
FROM    MinEnrolment
WHERE   AGE_AT_ENROL_DATE IS NULL;"


# ---- qry07b_Extract_No_Age_First_Enrolment ---- 
qry07b_Extract_No_Age_First_Enrolment <- "
SELECT DISTINCT id, ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_GENDER, PSI_CODE, AGE_AT_ENROL_DATE
INTO            Extract_No_Age_First_Enrolment
FROM         MinEnrolment
WHERE     (AGE_AT_ENROL_DATE IS NULL) AND IS_FIRST_ENROLMENT='Yes';"


# ---- qry07b2_update_Extract_No_Age_IsFirstEnrolment ---- 
qry07b2_update_Extract_No_Age_IsFirstEnrolment <- "
UPDATE    Extract_No_Age
SET       IS_FIRST_ENROLMENT = 'Yes'
FROM         Extract_No_Age 
INNER JOIN Extract_No_Age_First_Enrolment 
ON Extract_No_Age.id = Extract_No_Age_First_Enrolment.id;"




# ---- qry07c_Show_Age_Distribution ---- 
qry07c_Show_Age_Distribution <- "
SELECT     AGE_AT_ENROL_DATE, PSI_GENDER, COUNT(id) AS NumEnrolled 
FROM         MinEnrolment
WHERE     (AGE_GROUP_ENROL_DATE IS NOT NULL) AND (IS_FIRST_ENROLMENT = 'Yes')
GROUP BY AGE_AT_ENROL_DATE, PSI_GENDER;"


# ---- qry07d1_Update_Extract_No_Age ---- 
qry07d1_Update_Extract_No_Age <- "
UPDATE    Extract_No_Age
SET              AGE_AT_ENROL_DATE = Extract_No_Age_First_Enrolment.AGE_AT_ENROL_DATE
FROM         Extract_No_Age_First_Enrolment INNER JOIN
                      Extract_No_Age ON Extract_No_Age_First_Enrolment.id = Extract_No_Age.id;"

# ---- qry02a_Multiple_Enrol ----
qry02a_Multiple_Enrol <- "
SELECT Extract_No_Age.PSI_STUDENT_NUMBER, Extract_No_Age.PSI_CODE
FROM   Extract_No_Age
GROUP BY Extract_No_Age.PSI_STUDENT_NUMBER, Extract_No_Age.PSI_CODE
HAVING (((Count(*))>1));"


# ---- qry02b_Calc_Ages  ----
qry02b_Calc_Ages <- "
SELECT  Extract_No_Age.ID, 
        Extract_No_Age.PSI_STUDENT_NUMBER, Extract_No_Age.PSI_CODE, 
        Extract_No_Age.PSI_SCHOOL_YEAR, Extract_No_Age.PSI_MIN_START_DATE_D, 
        Extract_No_Age.AGE_AT_ENROL_DATE, Extract_No_Age.IS_FIRST_ENROLMENT
FROM    Extract_No_Age
ORDER BY Extract_No_Age.PSI_STUDENT_NUMBER, Extract_No_Age.PSI_CODE, 
        Extract_No_Age.PSI_MIN_START_DATE_D, Extract_No_Age.IS_FIRST_ENROLMENT DESC;"

# ---- qry_Update_Linked_dbo_Extract_No_Age_after_mod2 ----
qry_Update_Linked_dbo_Extract_No_Age_after_mod2 <- "
UPDATE Extract_No_Age
SET   Extract_No_Age.AGE_AT_ENROL_DATE = R_Extract_No_Age.AGE_AT_ENROL_DATE
FROM  Extract_No_Age
INNER JOIN R_Extract_No_Age
  ON  Extract_No_Age.id = R_Extract_No_Age.id 
WHERE Extract_No_Age.AGE_AT_ENROL_DATE Is Null
AND   R_Extract_No_Age.AGE_AT_ENROL_DATE Is Not Null;"


# ---- qry07d_Create_Age_Manual_Fixes View ---- 
qry07d_Create_Age_Manual_Fixes_View <- "CREATE VIEW qry05c_Age_Manual_Fixes_View AS
SELECT     TOP (100) PERCENT Extract_No_Age.AGE_AT_ENROL_DATE AS Expr1, MinEnrolment.PSI_GENDER, MinEnrolment.PSI_STUDENT_NUMBER, MinEnrolment.PSI_CODE, MinEnrolment.id, 
                      MinEnrolment.psi_birthdate_cleaned_D, MinEnrolment.PSI_MIN_START_DATE_D, MinEnrolment.AGE_AT_ENROL_DATE, MinEnrolment.IS_FIRST_ENROLMENT
FROM         Extract_No_Age INNER JOIN
                      MinEnrolment ON Extract_No_Age.PSI_STUDENT_NUMBER = MinEnrolment.PSI_STUDENT_NUMBER AND 
                      Extract_No_Age.PSI_CODE = MinEnrolment.PSI_CODE
WHERE     (Extract_No_Age.AGE_AT_ENROL_DATE IS NULL)
ORDER BY MinEnrolment.PSI_STUDENT_NUMBER;"


# ---- qry07d2_Update_Birthdate ---- 
qry07d2_Update_Birthdate <- "
UPDATE    qry05c_Age_Manual_Fixes_View
SET              PSI_BIRTHDATE_cleaned_D = qry05c_Age_Manual_Fixes_View_1.PSI_BIRTHDATE_cleaned_D
FROM         qry05c_Age_Manual_Fixes_View INNER JOIN
                      qry05c_Age_Manual_Fixes_View AS qry05c_Age_Manual_Fixes_View_1 ON 
                      qry05c_Age_Manual_Fixes_View.PSI_STUDENT_NUMBER = qry05c_Age_Manual_Fixes_View_1.PSI_STUDENT_NUMBER AND 
                      qry05c_Age_Manual_Fixes_View.PSI_CODE = qry05c_Age_Manual_Fixes_View_1.PSI_CODE
WHERE     (qry05c_Age_Manual_Fixes_View.PSI_BIRTHDATE_cleaned_D IS NULL) AND (qry05c_Age_Manual_Fixes_View_1.PSI_BIRTHDATE_cleaned_D IS NOT NULL);"


# ---- qry07d3_Update_Age ---- 
qry07d3_Update_Age <- "UPDATE    qry05C_Age_Manual_Fixes_View
SET     AGE_AT_ENROL_DATE = CASE
		WHEN dateadd(year, datediff (year, PSI_birthdate_cleaned_D, PSI_MIN_START_DATE_D), PSI_birthdate_cleaned_D) > PSI_MIN_START_DATE_D
		THEN datediff (year, PSI_birthdate_cleaned_D, PSI_MIN_START_DATE_D) - 1
		ELSE datediff (year, PSI_birthdate_cleaned_D, PSI_MIN_START_DATE_D)
		END
WHERE     (PSI_birthdate_cleaned_D IS NOT NULL)"


# ---- qry07e_Update_MinEnrolment_With_Age ---- 
qry07e_Update_MinEnrolment_With_Age <- "UPDATE    MinEnrolment
SET              AGE_AT_ENROL_DATE = Extract_No_Age.AGE_AT_ENROL_DATE
FROM         MinEnrolment INNER JOIN
                      Extract_No_Age ON MinEnrolment.id = Extract_No_Age.id;"


# ---- qry08_UpdateAGAtEnrol ---- 
qry08_UpdateAGAtEnrol <- "UPDATE    MinEnrolment
SET              AGE_GROUP_ENROL_DATE = AgeGroupLookup.AgeIndex
FROM         MinEnrolment CROSS JOIN
                      AgeGroupLookup
WHERE     (AgeGroupLookup.LowerBound <= MinEnrolment.AGE_AT_ENROL_DATE) AND (AgeGroupLookup.UpperBound >= MinEnrolment.AGE_AT_ENROL_DATE);"


# ---- qry09c_MinEnrolment by Credential and CIP Code ---- 
qry09c_MinEnrolment_by_Credential_and_CIP_Code <- "
SELECT        PSI_SCHOOL_YEAR, PSI_CREDENTIAL_CATEGORY, PSI_CIP_CODE, COUNT(*) AS Expr1
INTO  qry09c_MinEnrolment_by_Credential_and_CIP_Code
FROM            MinEnrolment
GROUP BY PSI_SCHOOL_YEAR, PSI_CREDENTIAL_CATEGORY, PSI_CIP_CODE
--HAVING        (PSI_SCHOOL_YEAR = '2016/2017')
ORDER BY PSI_CREDENTIAL_CATEGORY, PSI_CIP_CODE;"


# ---- qry09c_MinEnrolment ---- 
qry09c_MinEnrolment <- "
SELECT     MinEnrolment.PSI_GENDER, MinEnrolment.PSI_GENDER + AgeGroupLookup.AgeGroup As Groups, MinEnrolment.PSI_SCHOOL_YEAR, COUNT(*) AS Expr1
INTO    qry09c_MinEnrolment
FROM       MinEnrolment 
INNER JOIN  AgeGroupLookup 
ON  MinEnrolment.AGE_GROUP_ENROL_DATE = AgeGroupLookup.AgeIndex
GROUP BY MinEnrolment.PSI_GENDER, AgeGroupLookup.AgeGroup, MinEnrolment.PSI_SCHOOL_YEAR
HAVING      (MinEnrolment.PSI_SCHOOL_YEAR <> '2023/2024')
ORDER BY MinEnrolment.PSI_GENDER, AgeGroupLookup.AgeGroup, MinEnrolment.PSI_SCHOOL_YEAR
;"


# ---- qry09c_MinEnrolment_Domestic ---- 
qry09c_MinEnrolment_Domestic <- "
SELECT     MinEnrolment.PSI_GENDER, AgeGroupLookup.AgeGroup, MinEnrolment.PSI_SCHOOL_YEAR, COUNT(*) AS Expr1
INTO  qry09c_MinEnrolment_Domestic
FROM         MinEnrolment INNER JOIN
                      AgeGroupLookup ON MinEnrolment.AGE_GROUP_ENROL_DATE = AgeGroupLookup.AgeIndex
WHERE     (MinEnrolment.PSI_VISA_STATUS = 'DOMESTIC')
GROUP BY MinEnrolment.PSI_GENDER, AgeGroupLookup.AgeGroup, MinEnrolment.PSI_SCHOOL_YEAR
--HAVING      (MinEnrolment.PSI_SCHOOL_YEAR <> '2012/2013')
ORDER BY MinEnrolment.PSI_GENDER, AgeGroupLookup.AgeGroup, MinEnrolment.PSI_SCHOOL_YEAR;"



# ---- qry09c_MinEnrolment_PSI_TYPE ---- 
qry09c_MinEnrolment_PSI_TYPE <- "
SELECT        MinEnrolment.PSI_SCHOOL_YEAR, COUNT(*) AS Expr1, PSI_CODE_RECODE.PSI_TYPE_RECODE, PSI_CODE_RECODE.PSI_CODE_RECODE
INTO qry09c_MinEnrolment_PSI_TYPE
FROM            MinEnrolment INNER JOIN
                         AgeGroupLookup ON MinEnrolment.AGE_GROUP_ENROL_DATE = AgeGroupLookup.AgeIndex INNER JOIN
                         PSI_CODE_RECODE ON MinEnrolment.PSI_CODE = PSI_CODE_RECODE.PSI_CODE
WHERE        (AgeGroupLookup.AgeIndex <> 1 AND AgeGroupLookup.AgeIndex <> 9)
GROUP BY MinEnrolment.PSI_SCHOOL_YEAR, PSI_CODE_RECODE.PSI_TYPE_RECODE, PSI_CODE_RECODE.PSI_CODE_RECODE
--HAVING        (MinEnrolment.PSI_SCHOOL_YEAR <> '2015/2016')
ORDER BY MinEnrolment.PSI_SCHOOL_YEAR;"


# ---- qry_CreateMinEnrolmentView ---- 
qry_CreateMinEnrolmentView <- "
CREATE VIEW MinEnrolment
AS
SELECT    STP_Enrolment.ID, 
          STP_Enrolment.PSI_PEN, 
          STP_Enrolment.PSI_BIRTHDATE, 
          STP_Enrolment.psi_birthdate_cleaned, 
          STP_Enrolment.PSI_GENDER, 
          STP_Enrolment.PSI_STUDENT_NUMBER, 
          STP_Enrolment.PSI_STUDENT_POSTAL_CODE_FIRST_CONTACT, 
          STP_Enrolment.TRUE_PEN, 
          STP_Enrolment.ENCRYPTED_TRUE_PEN, 
          STP_Enrolment.PSI_SCHOOL_YEAR, 
          STP_Enrolment.PSI_REGISTRATION_TERM, 
          STP_Enrolment.PSI_STUDENT_POSTAL_CODE_CURRENT, 
          STP_Enrolment.PSI_INDIGENOUS_STATUS, 
          STP_Enrolment.PSI_NEW_STUDENT_FLAG, 
          STP_Enrolment.PSI_ENROLMENT_SEQUENCE, 
          STP_Enrolment.PSI_CODE, 
          STP_Enrolment.PSI_TYPE, 
          STP_Enrolment.PSI_FULL_NAME, 
          STP_Enrolment.PSI_BASIS_OF_ADMISSION, 
          STP_Enrolment.PSI_MIN_START_DATE, 
          STP_Enrolment.PSI_CREDENTIAL_PROGRAM_DESCRIPTION, 
          STP_Enrolment.PSI_PROGRAM_CODE, 
          STP_Enrolment.PSI_CIP_CODE, 
          STP_Enrolment.PSI_PROGRAM_EFFECTIVE_DATE, 
          STP_Enrolment.PSI_FACULTY, 
          STP_Enrolment.PSI_CONTINUING_EDUCATION_COURSE_ONLY, 
          STP_Enrolment.PSI_CREDENTIAL_CATEGORY, 
          STP_Enrolment.PSI_VISA_STATUS, 
          STP_Enrolment.PSI_STUDY_LEVEL, 
          STP_Enrolment.PSI_ENTRY_STATUS,
          STP_Enrolment.OVERALL_INDIGENOUS_STATUS, 
          MinEnrolmentSupVar.psi_birthdate_cleaned_D, 
          MinEnrolmentSupVar.PSI_MIN_START_DATE_D, 
          MinEnrolmentSupVar.AGE_AT_ENROL_DATE, 
          MinEnrolmentSupVar.AGE_GROUP_ENROL_DATE, 
          MinEnrolmentSupVar.AGE_AT_CENSUS_2016, 
          MinEnrolmentSupVar.AGE_GROUP_CENSUS_2016, 
          MinEnrolmentSupVar.IS_FIRST_ENROLMENT, 
          MinEnrolmentSupVar.IS_SKILLS_BASED
FROM            STP_Enrolment INNER JOIN
                         STP_Enrolment_Record_Type ON STP_Enrolment.ID = STP_Enrolment_Record_Type.ID INNER JOIN
                         MinEnrolmentSupVar ON STP_Enrolment.ID = MinEnrolmentSupVar.ID
WHERE        (STP_Enrolment_Record_Type.RecordStatus = 0) AND (STP_Enrolment_Record_Type.MinEnrolment = 1);"




