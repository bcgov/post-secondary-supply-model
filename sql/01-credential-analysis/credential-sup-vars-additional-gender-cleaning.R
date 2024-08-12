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

# ---- qry03fCredential_SupVarsGenderCleaning4 ----
qry03fCredential_SupVarsGenderCleaning4 <- 
  "SELECT ENCRYPTED_TRUE_PEN, MAX(MAX_PSI_SCHOOL_YEAR) AS MAX_MAX_PSI_SCHOOL_YEAR, 
       MAX(MAX_PSI_ENROLMENT_SEQUENCE) AS MAX_MAX_PSI_ENROLMENT_SEQUENCE
INTO tmp_CredentialGenderCleaning_Step2
FROM tmp_CredentialGenderCleaning_Step1
GROUP BY ENCRYPTED_TRUE_PEN;"

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

# ---- qry03fCredential_SupVars_Enrol_GenderCleaning10 ---- 
qry03fCredential_SupVars_Enrol_GenderCleaning10 <- "
ALTER TABLE CredentialSupVarsFromEnrolment_MultiGender
ADD psi_gender_cleaned NVARCHAR(50)"

# ---- qry03fCredential_SupVars_Enrol_GenderCleaning11 ---- 
qry03fCredential_SupVars_Enrol_GenderCleaning11 <- "
UPDATE      CredentialSupVarsFromEnrolment_MultiGender
SET                psi_gender_cleaned = tmp_CredentialGenderCleaning_Step3.PSI_GENDER_To_Use
FROM            CredentialSupVarsFromEnrolment_MultiGender INNER JOIN
tmp_CredentialGenderCleaning_Step3 ON 
CredentialSupVarsFromEnrolment_MultiGender.ENCRYPTED_TRUE_PEN = tmp_CredentialGenderCleaning_Step3.ENCRYPTED_TRUE_PEN"

# ---- qry03fCredential_SupVars_Enrol_GenderCleaning12 ---- 
qry03fCredential_SupVars_Enrol_GenderCleaning12 <- "
ALTER TABLE CredentialSupVars_Gender
ADD psi_gender_cleaned NVARCHAR(50), 
psi_gender_cleaned_flag NVARCHAR(50)"

# ---- qry03fCredential_SupVars_Enrol_GenderCleaning13 ---- 
qry03fCredential_SupVars_Enrol_GenderCleaning13 <- "
UPDATE       CredentialSupVars_Gender
SET                psi_gender_cleaned = CredentialSupVarsFromEnrolment_MultiGender.psi_gender_cleaned, psi_gender_cleaned_flag=  'Yes'
FROM            CredentialSupVars_Gender INNER JOIN
                         CredentialSupVarsFromEnrolment_MultiGender ON 
                         CredentialSupVars_Gender.ENCRYPTED_TRUE_PEN = CredentialSupVarsFromEnrolment_MultiGender.ENCRYPTED_TRUE_PEN"

# ---- qry03fCredential_SupVars_Enrol_GenderCleaning14 ---- 
qry03fCredential_SupVars_Enrol_GenderCleaning14 <- "
UPDATE       CredentialSupVars_Gender
SET                psi_gender_cleaned = PSI_GENDER
WHERE        (psi_gender_cleaned IS NULL)
"

# ---- qry03fCredential_SupVars_Enrol_GenderCleaning15 ----
qry03fCredential_SupVars_Enrol_GenderCleaning15 <- "
SELECT  ENCRYPTED_TRUE_PEN, psi_gender_cleaned, psi_gender_cleaned_flag, PSI_GENDER
INTO    tmp_CredentialSupVars_Gender_CleanUnknowns
FROM    CredentialSupVars_Gender
WHERE   (psi_gender_cleaned = 'U')
OR (psi_gender_cleaned = 'Unknown')
"

# ---- qry03fCredential_SupVars_Enrol_GenderCleaning16 ----
qry03fCredential_SupVars_Enrol_GenderCleaning16 <- "
ALTER TABLE tmp_CredentialSupVars_Gender_CleanUnknowns
ADD psi_gender_cleaned_NEW NVARCHAR(50)
"

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

# ---- qry03fCredential_SupVars_Enrol_GenderCleaning18 ----
qry03fCredential_SupVars_Enrol_GenderCleaning18 <- "
SELECT  ENCRYPTED_TRUE_PEN, PSI_GENDER AS GenderToUse
INTO    tmp_CredentialSupVars_Gender_CleanUnknowns_Step3
FROM    tmp_CredentialSupVars_Gender_CleanUnknowns_Step2
GROUP BY ENCRYPTED_TRUE_PEN, PSI_GENDER
HAVING  ((PSI_GENDER <> 'U') AND (PSI_GENDER <> 'Unknown'))
"

# ---- qry03fCredential_SupVars_Enrol_GenderCleaning19 ----
qry03fCredential_SupVars_Enrol_GenderCleaning19 <- "
UPDATE       tmp_CredentialSupVars_Gender_CleanUnknowns
SET          psi_gender_cleaned_NEW = tmp_CredentialSupVars_Gender_CleanUnknowns_Step3.GenderToUse
FROM         tmp_CredentialSupVars_Gender_CleanUnknowns_Step3
INNER JOIN   tmp_CredentialSupVars_Gender_CleanUnknowns ON 
             tmp_CredentialSupVars_Gender_CleanUnknowns_Step3.ENCRYPTED_TRUE_PEN = tmp_CredentialSupVars_Gender_CleanUnknowns.ENCRYPTED_TRUE_PEN
"

# ---- qry03fCredential_SupVars_Enrol_GenderCleaning20 ----
qry03fCredential_SupVars_Enrol_GenderCleaning20 <- "
UPDATE       CredentialSupVarsFromEnrolment_MultiGender
SET          psi_gender_cleaned = tmp_CredentialSupVars_Gender_CleanUnknowns.psi_gender_cleaned_NEW
FROM         CredentialSupVarsFromEnrolment_MultiGender
INNER JOIN   tmp_CredentialSupVars_Gender_CleanUnknowns ON 
             CredentialSupVarsFromEnrolment_MultiGender.ENCRYPTED_TRUE_PEN = tmp_CredentialSupVars_Gender_CleanUnknowns.ENCRYPTED_TRUE_PEN
"

# ---- qry03fCredential_SupVars_Enrol_GenderCleaning21 ----
qry03fCredential_SupVars_Enrol_GenderCleaning21<-"
UPDATE credentialsupvars_gender
SET    psi_gender_cleaned =
       tmp_credentialsupvars_gender_cleanunknowns.psi_gender_cleaned_new
FROM   credentialsupvars_gender
       INNER JOIN tmp_credentialsupvars_gender_cleanunknowns
               ON credentialsupvars_gender.encrypted_true_pen =
                  tmp_credentialsupvars_gender_cleanunknowns.encrypted_true_pen"

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

# ---- qry03fCredential_SupVars_Enrol_GenderCleaning23 ----
qry03fCredential_SupVars_Enrol_GenderCleaning23<-"
SELECT *
FROM   credentialsupvars
WHERE  psi_gender_cleaned IS NULL"

# ---- qry03fCredential_SupVars_Enrol_GenderCleaning24 ----
qry03fCredential_SupVars_Enrol_GenderCleaning24<-"
SELECT encrypted_true_pen,
       psi_student_number,
       psi_code,
       psi_gender_cleaned
INTO   tmp_credentialgendercleaning_step5
FROM   credentialsupvars
WHERE  psi_gender_cleaned IS NULL;"

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

# ---- qry03fCredential_SupVars_Enrol_GenderCleaning26a ----
qry03fCredential_SupVars_Enrol_GenderCleaning26a<-"
SELECT ENCRYPTED_TRUE_PEN,
       PSI_STUDENT_NUMBER,
       psi_code, 
       COUNT(*) AS GenderCount
INTO CredentialSupVars_MultiGenderCounterForNULLS
FROM tmp_CredentialGenderCleaning_Step6
GROUP BY ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, psi_code"

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

# ---- qry03fCredential_SupVars_Enrol_GenderCleaning28 ----
qry03fCredential_SupVars_Enrol_GenderCleaning28 <- "
UPDATE tmp_credentialgendercleaning_step6
SET    tmp_credentialgendercleaning_step6.psi_gender_cleaned = tmp_credentialgendercleaning_step7.psi_gender_cleaned,
       tmp_credentialgendercleaning_step6.psi_gender_cleaned_flag = 'Yes'
FROM   tmp_credentialgendercleaning_step6
       INNER JOIN tmp_credentialgendercleaning_step7
               ON tmp_credentialgendercleaning_step6.psi_student_number = tmp_credentialgendercleaning_step7.psi_student_number
              AND tmp_credentialgendercleaning_step6.psi_code = tmp_credentialgendercleaning_step7.psi_code"

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

# ---- qry03fCredential_SupVars_Enrol_GenderCleaning32 ----
qry03fCredential_SupVars_Enrol_GenderCleaning32 <- "
UPDATE       tmp_CredentialGenderCleaning_Step6
SET                psi_gender_cleaned_flag = 'Yes'
WHERE (PSI_GENDER = 'U' OR PSI_GENDER = 'Unknown' OR psi_gender = '(Unspecified)')"

qry03fCredential_SupVars_Enrol_GenderCleaning33 <- "
UPDATE       tmp_CredentialGenderCleaning_Step6
SET                psi_gender_cleaned = tmp_CredentialGenderCleaning_Step7.psi_gender_cleaned, psi_gender_cleaned_flag=  'Yes'
FROM            tmp_CredentialGenderCleaning_Step6 INNER JOIN
                         tmp_CredentialGenderCleaning_Step7 ON 
                         tmp_CredentialGenderCleaning_Step6.PSI_STUDENT_NUMBER = tmp_CredentialGenderCleaning_Step7.PSI_STUDENT_NUMBER
						 AND tmp_CredentialGenderCleaning_Step6.psi_code = tmp_CredentialGenderCleaning_Step7.psi_code"

# ---- qry03fCredential_SupVars_Enrol_GenderCleaning34 ----
qry03fCredential_SupVars_Enrol_GenderCleaning34 <- "
UPDATE       tmp_CredentialGenderCleaning_Step6
SET                psi_gender_cleaned_flag = 'Yes', psi_gender_cleaned = PSI_GENDER
WHERE psi_gender_cleaned_flag IS NULL"

# ---- qry03fCredential_SupVars_Enrol_GenderCleaning35 ----
qry03fCredential_SupVars_Enrol_GenderCleaning35 <- "
UPDATE       CredentialSupVars
SET          psi_gender_cleaned = tmp_CredentialGenderCleaning_Step6.psi_gender_cleaned 
FROM         tmp_CredentialGenderCleaning_Step6 INNER JOIN
                         CredentialSupVars ON 
                         tmp_CredentialGenderCleaning_Step6.PSI_STUDENT_NUMBER = CredentialSupVars.PSI_STUDENT_NUMBER
						 AND tmp_CredentialGenderCleaning_Step6.psi_code = CredentialSupVars.psi_code
WHERE tmp_CredentialGenderCleaning_Step6.psi_gender_cleaned_flag='Yes' AND CredentialSupVars.psi_gender_cleaned IS NULL"



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