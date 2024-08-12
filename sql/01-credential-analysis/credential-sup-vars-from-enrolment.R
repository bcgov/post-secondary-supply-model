# ---- qry01_CredentialSupVars_From_Enrolment ----
qry01_CredentialSupVars_From_Enrolment <- "
SELECT     ENCRYPTED_TRUE_PEN, MAX(PSI_SCHOOL_YEAR) AS MaxSchoolYear
INTO       tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step1
FROM       STP_Enrolment_Valid
GROUP BY   ENCRYPTED_TRUE_PEN
HAVING     (ENCRYPTED_TRUE_PEN IS NOT NULL) AND (ENCRYPTED_TRUE_PEN <> '')"

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

# ---- qry03_CredentialSupVars_From_Enrolment ----
qry03_CredentialSupVars_From_Enrolment <- "
ALTER TABLE tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step2
ADD CONSTRAINT tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step2_PK_ID
PRIMARY KEY (ID);"

# ---- qry04_CredentialSupVars_From_Enrolment ----
qry04_CredentialSupVars_From_Enrolment <- "
SELECT  STP_Enrolment_Valid.ID, STP_Enrolment_Valid.PSI_STUDENT_NUMBER, STP_Enrolment_Valid.ENCRYPTED_TRUE_PEN, 
        STP_Enrolment_Valid.PSI_SCHOOL_YEAR, STP_Enrolment_Valid.PSI_STUDENT_POSTAL_CODE_CURRENT, STP_Enrolment_Valid.PSI_ENROLMENT_SEQUENCE, 
        STP_Enrolment_Valid.PSI_CODE, STP_Enrolment_Valid.PSI_MIN_START_DATE
INTO    tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step3
FROM    tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step2 
INNER JOIN STP_Enrolment_Valid 
  ON tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step2.ID = STP_Enrolment_Valid.ID"
                         
# ---- qry05_CredentialSupVars_From_Enrolment ----                    
qry05_CredentialSupVars_From_Enrolment <- "                         
ALTER TABLE tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step3
ADD CONSTRAINT tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step3_PK_ID
PRIMARY KEY (ID);"

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
                    

# check if any unmatched join on student id/psi code combo.
# 2020 NOTE: prior year used a table called [RW_TEST_CRED_EPENS_NOT_MATCHED_ID_PSICODE] which is missing
# but likely pulled in all records from the Credential view which do not exist in CredentialSupVarsFromEnrolment table
#  attempt to replicate process by creating a query to pull EPENs from Credential view with NULL CredentialSupVarsFromEnrolment data
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
    
# ---- qry09_CredentialSupVars_From_Enrolment ----                     
qry09_CredentialSupVars_From_Enrolment <- "
SELECT     ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, MAX(PSI_SCHOOL_YEAR) AS MaxSchoolYear
INTO       tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step4
FROM       STP_Enrolment_Valid
GROUP BY   ENCRYPTED_TRUE_PEN, PSI_CODE, PSI_STUDENT_NUMBER
HAVING     (ENCRYPTED_TRUE_PEN IS NULL) OR (ENCRYPTED_TRUE_PEN = '');"

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

# ---- qry11_CredentialSupVars_From_Enrolment ----
qry11_CredentialSupVars_From_Enrolment <- "
ALTER TABLE [tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step5] 
ADD CONSTRAINT tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step5_PK_ID
PRIMARY KEY (ID);"

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

# ---- qry12b_CredentialSupVars_From_Enrolment ----
qry12b_CredentialSupVars_From_Enrolment <- "                        
ALTER TABLE [tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step6] 
ADD CONSTRAINT tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step6_PK_ID
PRIMARY KEY (ID);"

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
                         