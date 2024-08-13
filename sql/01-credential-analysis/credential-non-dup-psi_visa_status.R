

# ---- CredentialSupVars_VisaStatus_Cleaning_check ----
CredentialSupVars_VisaStatus_Cleaning_check <-"
SELECT PSI_VISA_STATUS, count(*) FROM CredentialSupVars GROUP BY PSI_VISA_STATUS"

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

# ---- CredentialSupVars_VisaStatus_5 ----
CredentialSupVars_VisaStatus_Cleaning_5 <-"
UPDATE    CredentialSupVars
SET       PSI_VISA_STATUS = Credential_Non_Dup_VisaStatus_Cleaning_Step1.PSI_VISA_STATUS
FROM      CredentialSupVars 
INNER JOIN Credential_Non_Dup_VisaStatus_Cleaning_Step1 ON CredentialSupVars.ID = Credential_Non_Dup_VisaStatus_Cleaning_Step1.id
WHERE     (CredentialSupVars.PSI_VISA_STATUS IS NULL) 
OR        (CredentialSupVars.PSI_VISA_STATUS IN ('', ' ', '(Unspecified)'))"

                        
# ---- CredentialSupVars_VisaStatus_6 ----
CredentialSupVars_VisaStatus_Cleaning_6 <-"
UPDATE      Credential_Non_Dup
SET         PSI_VISA_STATUS = CredentialSupVars.PSI_VISA_STATUS
FROM        Credential_Non_Dup 
INNER JOIN  CredentialSupVars ON Credential_Non_Dup.id = CredentialSupVars.ID"






















