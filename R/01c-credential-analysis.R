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
source("./sql/01-credential-analysis/01b-credential-analysis.R")
source("./sql/01-credential-analysis/credential-sup-vars-from-enrolment.R")
source("./sql/01-credential-analysis/credential-sup-vars-additional-gender-cleaning.R")
source("./sql/01-credential-analysis/credential-non-dup-psi_visa_status.R")
source("./sql/01-credential-analysis/credential-ranking.R")

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
dbExecute(con, qry_Credential_view_initial) 

# ---- Make Credential Sup Vars Enrolment ----
# Create a list of EPENs/max school year/enrolment ID's from the Enrolment_valid table 
dbExecute(con, qry01_CredentialSupVars_From_Enrolment) # for valid EPENS pull max school year
dbExecute(con, qry02_CredentialSupVars_From_Enrolment) # bring in more enrolment information for the most recent school year 
dbExecute(con, qry03_CredentialSupVars_From_Enrolment) # add pkey for faster search
dbExecute(con, qry04_CredentialSupVars_From_Enrolment) # ... more enrolment information
dbExecute(con, qry05_CredentialSupVars_From_Enrolment) # add pkey for faster search
dbExecute(con, qry06_CredentialSupVars_From_Enrolment) # bring in credential record status from Credential View
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

#dbExecute(con, qry07_CredentialSupVars_From_Enrolment) Creates a table that isn't used for anything that I can see
dbExecute(con, qry08_CredentialSupVars_From_Enrolment) 
dbExecute(con, qry09_CredentialSupVars_From_Enrolment) # for null/blank EPENS pull max school year
dbExecute(con, qry10_CredentialSupVars_From_Enrolment) # bring in more enrolment information for the most recent school year 
dbExecute(con, qry11_CredentialSupVars_From_Enrolment) # add pky constraint
dbExecute(con, qry12_CredentialSupVars_From_Enrolment) # ...
dbExecute(con, qry12b_CredentialSupVars_From_Enrolment) # add pky constraint
dbExecute(con, qry13_CredentialSupVars_From_Enrolment) # bring in credential record status from Credential View

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
dbExecute(con, qry01a_CredentialSupVars) # select key columns from Credential View into a new table called CredentialSupVars
dbExecute(con, qry01b_CredentialSupVars) # add some more columns to be filled in later
dbExecute(con, "ALTER TABLE [CredentialSupVars] ADD CONSTRAINT PK_CredSupVars_ID PRIMARY KEY (ID);")
dbExecute(con, qry01b_CredentialSupVarsFromEnrol_1) # add some columns to CredSupVarsEnrol
dbExecute(con, qry01b_CredentialSupVarsFromEnrol_2) # bring in data from STP_Enrolment 
dbExecute(con, qry01b_CredentialSupVarsFromEnrol_3) # Empty strings ' ' in psi_birthdate_cleaned were cast to 1900-01-01 in date format. 

# ---- 02 Developmental Records ----
# flag STP_Credential_Record_Type records with PSI_CREDENTIAL_CATEGORY = 'DEVELOPMENTAL CREDENTIAL' 'OTHER' 'NONE' 'SHORT CERTIFICATE'
dbExecute(con, qry02a_DropCredCategory) 
dbExecute(con, "ALTER TABLE  STP_Credential_Record_Type ADD DropCredCategory NVARCHAR(50) NULL")
dbExecute(con, qry02b_DeleteCredCategory)
dbExecute(con, "DROP TABLE Drop_Credential_Category")

# ---- 03 Miscellaneous ----
## ---- ** Manual ** change date in Drop Partial Year ----
# qry03c_DeletePartialYear flags STP_Credential_Record_Type records whose CREDENTIAL_AWARD_DATE >= '<model-year>-09-01'
dbExecute(con, qry03a1_ConvertAwardDate) # data type conversion
dbExecute(con, qry03b_DropPartialYear) 
dbExecute(con, "ALTER TABLE  STP_Credential_Record_Type ADD DropPartialYear NVARCHAR(50) NULL")
dbExecute(con, qry03c_DeletePartialYear)
dbExecute(con, "DROP TABLE Drop_Partial_Year")

dbExecute(con, qry03d_CredentialSupVarsBirthdate) # create a table with unique EPEN/birthdates from CredentialSupVarsFromEnrolment
dbExecute(con, "UPDATE  CredentialSupVars_BirthdateClean 
                SET psi_birthdate_cleaned_D = psi_birthdate_cleaned
                WHERE psi_birthdate_cleaned is not null AND psi_birthdate_cleaned <> ''")

# ---- 03 Gender Cleaning ---- 
dbExecute(con, qry03e_CredentialSupVarsGender) # create a table with unique EPEN/gender from CredentialSupVarsFromEnrolment
dbExecute(con, qry03fCredential_SupVarsGenderCleaning1)
dbExecute(con, qry03fCredential_SupVarsGenderCleaning2)
dbExecute(con, "DROP TABLE CredentialSupVars_MultiGenderCounter")
dbExecute(con, qry03fCredential_SupVarsGenderCleaning3)
dbExecute(con, qry03fCredential_SupVarsGenderCleaning4)
dbExecute(con, qry03fCredential_SupVarsGenderCleaning5)
dbExecute(con, "DROP TABLE tmp_CredentialGenderCleaning_Step1")
dbExecute(con, "DROP TABLE tmp_CredentialGenderCleaning_Step2")
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning6)
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning7)
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning8)
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning9)
dbExecute(con, "DROP TABLE RW_TEST_ENROL_GENDER_morethanone_list_stepa")
dbExecute(con, "DROP TABLE RW_TEST_ENROL_GENDER_morethanone_list")
dbExecute(con, "DROP TABLE RW_TEST_ENROL_GENDER_morethanone_listIDS") 
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning10)
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning11)
dbExecute(con, "DROP TABLE tmp_CredentialGenderCleaning_Step3")
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning12)
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning13)
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning14)
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning15)
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning16)
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning17)
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning18)
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning19)
dbExecute(con, "DROP TABLE tmp_CredentialSupVars_Gender_CleanUnknowns_Step2")
dbExecute(con, "DROP TABLE tmp_CredentialSupVars_Gender_CleanUnknowns_Step3")
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning20)
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning21)
dbExecute(con, "DROP TABLE tmp_CredentialSupVars_Gender_CleanUnknowns")
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning22)
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning23)
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning24)
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning25)
dbExecute(con, "DROP TABLE tmp_CredentialGenderCleaning_Step5")
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning26a)
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning26b)
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning27)
dbExecute(con, "DROP TABLE CredentialSupVars_MultiGenderForNULLS")
dbExecute(con, "DROP TABLE CredentialSupVars_MultiGenderCounterForNULLS")
dbExecute(con, "ALTER TABLE tmp_credentialgendercleaning_step6 ADD psi_gender_cleaned_flag nvarchar(10)")
dbExecute(con, "ALTER TABLE tmp_credentialgendercleaning_step7 ADD psi_gender_cleaned nvarchar(10)")
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning28) 
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning29)
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning30)
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning31)
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning32)
dbExecute(con, "DROP TABLE tmp_CredentialSupVars_Gender_CleanUnknownsforNULLS_Step1")
dbExecute(con, "DROP TABLE tmp_CredentialSupVars_Gender_CleanUnknownsforNULLS_Step2")
dbExecute(con, "DROP TABLE tmp_CredentialSupVars_Gender_CleanUnknownsforNULLS_Step3")
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning33)
dbExecute(con, "DROP TABLE tmp_CredentialGenderCleaning_Step7")
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning34)
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
dbExecute(con, qry04a_UpdateCredentialSupVarsBirthdate) # run for the records that matched on ENCRYPTED_TRUE_PEN (non-null/blank)
dbExecute(con, qry04a_UpdateCredentialSupVarsBirthdate2) # supports the PSI_CODE/PSI_STUDENT number combos
dbExecute(con, "ALTER TABLE CredentialSupVars ADD LAST_SEEN_BIRTHDATE DATE")
dbExecute(con, "ALTER TABLE CredentialSupVarsFromEnrolment ADD LAST_SEEN_BIRTHDATE DATE")
dbExecute(con, qry04a1_UpdateCredentialSupVarsBirthdate) 
dbExecute(con, qry04a2_UpdateCredentialSupVarsBirthdate) 
dbExecute(con, qry04a3_UpdateCredentialSupVarsBirthdate) 
dbExecute(con, "DROP TABLE CredentialSupVars_BirthdateClean")

dbExecute(con, qry04b_UpdateCredentiaSupVarsGender)
dbExecute(con, "DROP TABLE CredentialSupVars_Gender")
dbExecute(con, "DROP VIEW Credential")
dbExecute(con, qry04c_RecreateCredentialViewWithSupVars)

# ---- 05 Age and Credential Update ----
dbExecute(con, qry05a_FindDistinctCredentials_CreateViewCredentialRemoveDup) 
# check the birthdate cleaned_D column; sometimes it populates, sometimes not.  
# This is something that needs to be removed from workflow completely at a later date
dbExecute(con, "UPDATE Credential SET psi_birthdate_cleaned_D = psi_birthdate_cleaned where psi_birthdate_cleaned is not null")
dbExecute(con, qry05c_UpdateAgeAtGrad)
dbExecute(con, qry05d_UpdateAgeGroupAtGrad)
dbExecute(con, qry06e_UpdateAwardSchoolYear)


# ---- 07 Credential Cleaning ----
## ---- ** Create NON DUP ** ----
dbExecute(con, qry07a1a_UpdateGender)
dbExecute(con, qry07a1b_Create_Credential_Non_Dup) # flagging non dup created here.  OUTCOMES_CRED comes from a lookup later
dbExecute(con, qry07a1c_tmp_Credential_Gender)
dbExecute(con, qry07a1d_tmp_Credential_GenderDups)
dbExecute(con, qry07a1e_tmp_Credential_GenderDups_FindMaxCredDate)

dbExecute(con, "ALTER TABLE tmp_Dup_Credential_EPEN_Gender_MaxCredDate ADD PSI_GENDER varchar(10)")
dbExecute(con, qry07a1f_tmp_Credential_GenderDups_PickGender)                
dbExecute(con, qry07a1g_Update_Credential_Non_Dup_GenderDups)  
dbExecute(con, qry07a1h_Update_Credential_GenderDups) 
dbExecute(con, qry07a2a_ExtractNoGender)
dbExecute(con, qry07a2b_ExtractNoGenderUnique)
dbExecute(con, qry07a2c_Create_CRED_Extract_No_Gender_EPEN_with_MultiCred)
dbExecute(con, "ALTER TABLE CRED_Extract_No_Gender_Unique ADD MultiCredFlag varchar(2)")
dbExecute(con, qry07a2d_Update_MultiCredFlag)
dbExecute(con, "DROP TABLE CRED_Extract_No_Gender_EPEN_with_MultiCred")

## ---- Impute Missing Gender ----
d <- dbGetQuery(con, qry07b_GenderDistribution) %>% 
  mutate(Expr1 = replace_na(Expr1, 0))

nulls <- d %>% 
  filter(is.na(PSI_GENDER)) %>% 
  select(-PSI_GENDER)

d <- d %>% 
  filter(!is.na(PSI_GENDER)) %>% 
  group_by(PSI_CREDENTIAL_CATEGORY) %>% 
  mutate(p = Expr1/sum(Expr1)) %>% filter(PSI_GENDER == 'F') %>% 
  select (-c(PSI_GENDER, Expr1))

top_n <- inner_join(d, nulls) %>% 
  mutate(n = round(Expr1*p)) %>%
  select(PSI_CREDENTIAL_CATEGORY, n)

top_n


## ---- STOP !! manually add top_n to queries below ----
# Code later: https://github.com/r-dbi/DBI/issues/193
dbExecute(con, qry07c10_Assign_TopID_GenderF_GradCert)
dbExecute(con, qry07c11_Assign_TopID_GenderF_GradDipl)
dbExecute(con, qry07c12_Assign_TopID_GenderF_Masters)
dbExecute(con, qry07c13_Assign_TopID_GenderF_PostDegCert)
dbExecute(con, qry07c14_Assign_TopID_GenderF_PostDegDipl)
dbExecute(con, qry07c1_Assign_TopID_GenderF_AdvancedCert)
dbExecute(con, qry07c2_Assign_TopID_GenderF_AdvancedDip)
dbExecute(con, qry07c3_Assign_TopID_GenderF_Apprenticeship)
dbExecute(con, qry07c4_Assign_TopID_GenderF_AssocDegree)
dbExecute(con, qry07c5_Assign_TopID_GenderF_Bachelor)
dbExecute(con, qry07c6_Assign_TopID_GenderF_Certificate)
dbExecute(con, qry07c7_Assign_TopID_GenderF_Diploma)
dbExecute(con, qry07c8_Assign_TopID_GenderF_Doctorate)
dbExecute(con, qry07c9_Assign_TopID_GenderF_FirstProfDeg)
dbExecute(con, qry07c_Assign_TopID_GenderM)
dbExecute(con, qry07d_CorrectGender1)
dbExecute(con, qry07d_CorrectGender2)
dbExecute(con, "DROP TABLE CRED_Extract_No_Gender")
dbExecute(con, "DROP TABLE CRED_Extract_No_Gender_Unique")
dbExecute(con, "DROP VIEW Credential_Remove_Dup")
dbExecute(con, "DROP TABLE tmp_credential_epen_gender")
dbExecute(con, "DROP TABLE tmp_dup_credential_epen_gender")
dbExecute(con, "DROP TABLE tmp_dup_credential_epen_gender_maxcreddate")

# ---- 08 Credential Ranking ----
dbExecute(con, qry08_Create_Credential_Ranking_View_a)
dbExecute(con, qry08_Create_Credential_Ranking_View_b)
dbExecute(con, qry08_Create_Credential_Ranking_View_c)
dbExecute(con, qry08_Create_Credential_Ranking_View_d)
dbExecute(con, "ALTER TABLE tmp_Credential_Ranking_step3 ADD PSI_STUDENT_NUMBER varchar(50)")
dbExecute(con, "ALTER TABLE tmp_Credential_Ranking_step3 ADD PSI_CODE varchar(50)")
dbExecute(con, qry08_Create_Credential_Ranking_View_e)
dbExecute(con, qry08_Create_Credential_Ranking_View_f) 
dbExecute(con, "DROP TABLE tmp_Credential_Ranking_step1")
dbExecute(con, "DROP TABLE tmp_Credential_Ranking_step2")
dbExecute(con, "DROP TABLE tmp_CredentialNonDup_STUD_NUM_PSI_CODE_MoreThanOne")
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

dbExecute(con, qry08a1_Update_CredentialNonDup_with_highestDate_Rank)
dbExecute(con, qry08a_Run_after_Credential_Ranking)
dbExecute(con, qry08b_Rank_non_multi_cred)
dbExecute(con, "DROP TABLE tmp_Credential_Ranking")
dbExecute(con, "DROP TABLE tmp_Credential_Ranking_step3")
dbExecute(con, "DROP VIEW Credential_Ranking")

# ---- 09 Age Gender Distributions ----
dbExecute(con, qry09a_ExtractNoAge) 
dbExecute(con, "ALTER TABLE CRED_Extract_No_Age ADD PRIMARY KEY (id);")
dbExecute(con, qry09b_ExtractNoAgeUnique)
dbExecute(con, "ALTER TABLE CRED_Extract_No_Age_Unique ADD PRIMARY KEY (id);")

sql <- "SELECT PSI_GENDER_CLEANED, PSI_CREDENTIAL_CATEGORY, COUNT(*) AS NumWithNullAge
FROM CRED_Extract_No_Age_Unique GROUP BY PSI_GENDER_CLEANED, PSI_CREDENTIAL_CATEGORY"
CRED_Extract_No_Age_Unique <- dbGetQuery(con, sql)
CREDAgeDistributionbyGender <- dbGetQuery(con, qry09d_ShowAgeGenderDistribution)

d <- CREDAgeDistributionbyGender %>% 
  group_by(PSI_GENDER_CLEANED, PSI_CREDENTIAL_CATEGORY) %>%
  mutate(p = NumGrads/sum(NumGrads)) %>%
  left_join(CRED_Extract_No_Age_Unique, 
            by = join_by(PSI_GENDER_CLEANED, PSI_CREDENTIAL_CATEGORY)) %>%
  mutate(n = round(p*NumWithNullAge)) %>% 
  arrange(PSI_GENDER_CLEANED, PSI_CREDENTIAL_CATEGORY, AGE_AT_GRAD) 

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

}
print("....done")

# assign a random age between 19 and 70 to any remaining nulls. 
dbExecute(con, "UPDATE CRED_Extract_No_Age_Unique
                SET AGE_AT_GRAD = (ABS(CHECKSUM(NewId())) % 35) + 19
                WHERE AGE_AT_GRAD IS NULL OR AGE_AT_GRAD = ' '")

dbExecute(con, qry10_Update_Extract_No_Age)
dbExecute(con, qry11a_UpdateAgeAtGrad)
dbExecute(con, qry11b_UpdateAGAtGrad)
dbExecute(con, "DROP TABLE CRED_Extract_No_Age")
dbExecute(con, "DROP TABLE CRED_Extract_No_Age_Unique")
#dbExecute(con, "DROP TABLE CREDAgeDistributionbyGender")

# ---- VISA Status ----
dbExecute(con, "ALTER TABLE CredentialSupVars ADD PSI_VISA_STATUS varchar(50)")
dbExecute(con, "ALTER TABLE Credential_Non_Dup ADD PSI_VISA_STATUS varchar(50)")
dbGetQuery(con, CredentialSupVars_VisaStatus_Cleaning_check)
dbExecute(con, "ALTER TABLE Credential_Non_Dup ADD PSI_VISA_STATUS varchar(50)")
dbExecute(con, CredentialSupVars_VisaStatus_Cleaning_1)
dbExecute(con, CredentialSupVars_VisaStatus_Cleaning_2)
dbExecute(con, CredentialSupVars_VisaStatus_Cleaning_3)
dbExecute(con, CredentialSupVars_VisaStatus_Cleaning_4)
dbExecute(con, "ALTER TABLE CredentialSupVars ADD PSI_VISA_STATUS varchar(50)")
dbExecute(con, CredentialSupVars_VisaStatus_Cleaning_5)
dbExecute(con, CredentialSupVars_VisaStatus_Cleaning_6)
dbGetQuery(con, CredentialSupVars_VisaStatus_Cleaning_check)
dbExecute(con, "DROP TABLE CredentialSupVars_VisaStatus_Cleaning_Step2")
dbExecute(con, "DROP TABLE Credential_Non_Dup_VisaStatus_Cleaning_Step1")
dbExecute(con, "DROP TABLE CredentialSupVars_VisaStatus_Cleaning_Step1")

# ---- Highest Rank ----
dbExecute(con, "ALTER TABLE Credential_Non_Dup ADD CONCATENATED_ID VARCHAR(255) NULL")
dbExecute(con, "UPDATE Credential_Non_Dup SET CONCATENATED_ID = ENCRYPTED_TRUE_PEN 
                 WHERE (ENCRYPTED_TRUE_PEN IS NOT NULL AND ENCRYPTED_TRUE_PEN <> '')")
dbExecute(con, "UPDATE Credential_Non_Dup SET CONCATENATED_ID = PSI_STUDENT_NUMBER + PSI_CODE 
                WHERE (ENCRYPTED_TRUE_PEN IS NULL) OR (ENCRYPTED_TRUE_PEN = '')")
dbExecute(con, qry12_Create_View_tblCredentialHighestRank)

dbExecute(con, qry18a_ExtrLaterAwarded)
dbExecute(con, qry18b_ExtrLaterAwarded)
dbExecute(con, qry18c_ExtrLaterAwarded)
dbExecute(con, qry18d_ExtrLaterAwarded)
dbExecute(con, "DROP TABLE tmp_qry18b_ExtrLaterAwarded_2")
dbExecute(con, "DROP TABLE tmp_qry18c_ExtrLaterAwarded_3")
dbExecute(con, "DROP TABLE tblcredential_laterawarded")

# ---- 13 Delay Date ----
dbExecute(con, qry19_UpdateDelayDate)
dbExecute(con, "DROP TABLE tblCredential_DelayEffect")

dbExecute(con, "ALTER TABLE Credential_Non_Dup 
                ADD CREDENTIAL_AWARD_DATE_D_DELAYED date, 
                PSI_AWARD_SCHOOL_YEAR_DELAYED varchar(50);")

dbExecute(con, qry13a_UpdateDelayedCredDate)
dbExecute(con, qry13b_UpdateDelayedCredDate)
dbExecute(con, qry13_UpdateDelayedCredDate)

# ---- 14-15 research University + Outcomes Credential ----
dbExecute(con, qry14_ResearchUniversity)
dbExecute(con, qry15_OutcomeCredential)

# ---- Break and do Program Matching ----
# ---- 20 Final Distributions ----
# NOTE: Exclude_CIPs queries end up with Invalid column name 'FINAL_CIP_CLUSTER_CODE'. 
dbGetQuery(con, qry20a_1Credential_By_Year_AgeGroup)
dbGetQuery(con, qry20a_1Credential_By_Year_AgeGroup_Exclude_CIPs)
dbGetQuery(con, qry20a_2Credential_By_Year_AgeGroup_Domestic)
dbGetQuery(con, qry20a_2Credential_By_Year_AgeGroup_Domestic_Exclude_CIPs)
dbGetQuery(con, qry20a_3Credential_By_Year_AgeGroup_Domestic_Exclude_RU_DACSO)
dbGetQuery(con, qry20a_4Credential_By_Year_CIP4_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs)
dbGetQuery(con, qry20a_4Credential_By_Year_CIP4_Gender_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs)
dbGetQuery(con, qry20a_4Credential_By_Year_Gender_AgeGroup_Domestic_Exclude_CIPs) 
dbGetQuery(con, qry20a_4Credential_By_Year_Gender_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs) 
dbGetQuery(con, qry20a_4Credential_By_Year_PSI_TYPE_Domestic_Exclude_RU_DACSO_Exclude_CIPs)
dbGetQuery(con, qry20a_4Credential_By_Year_PSI_TYPE_Domestic_Exclude_RU_DACSO_Exclude_CIPs_Not_Highest)
dbGetQuery(con, qry20a_99_Checking_Excluding_RU_DACSO_Variables)
dbGetQuery(con, qryCreateIDinSTPCredential)
dbGetQuery(con, qry_Update_Cdtl_Sup_Vars_InternationalFlag)


# ---- Clean Up ----
dbExecute(con, "DROP VIEW tblCredential_HighestRank")
dbExecute(con, "DROP TABLE CredentialSupVarsFromEnrolment")
dbExecute(con, "DROP TABLE CredentialSupVars")
dbExecute(con, "DROP TABLE CredentialSupVars_BirthdateClean")
dbExecute(con, "DROP VIEW Credential")
dbExecute(con, "DROP VIEW Credential_Ranking")
dbDisconnect(con)




