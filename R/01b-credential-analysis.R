library(arrow)
library(tidyverse)
library(odbc)
library(DBI)

# ---- Configure LAN Paths and DB Connection -----
lan <- config::get("lan")
source(glue::glue("{lan}/development/sql/gh-source/01b-credential-analysis/01b-credential-analysis.R"))
source(glue::glue("{lan}/development/sql/gh-source/01b-credential-analysis/credential-sup-vars-from-enrolment.R"))

db_config <- config::get("decimal")
my_schema <- config::get("myschema")
db_schema <- config::get("dbschema")

con <- dbConnect(odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")

# ---- Check Required Tables etc. ----
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Credential"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Credential_Record_Type"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Enrolment"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Enrolment_Record_Type"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Enrolment_Valid"')))

# ---- Create a view with STP_Credential data with record_type == 0 and a non-blank award date ----
dbExecute(con, qry_Credential_view_initial) 

# ---- Create a list of EPENs/max school year/enrolment ID's from the Enrolment_valid table ----
dbExecute(con, qry01_CredentialSupVars_From_Enrolment) # for non-(null/blank) EPENS pull max school year
dbExecute(con, qry02_CredentialSupVars_From_Enrolment) # bring in more enrolment information for the most recent school year 
dbExecute(con, qry03_CredentialSupVars_From_Enrolment) # ... more enrolment information
dbExecute(con, qry04_CredentialSupVars_From_Enrolment) #... more enrolment information
dbExecute(con, qry05_CredentialSupVars_From_Enrolment) # bring in credential record status from Credential View
dbExecute(con, qry06_CredentialSupVars_From_Enrolment)
#dbExecute(con, qry07_CredentialSupVars_From_Enrolment  # we can't do these checks for extra unmatched records
#dbExecute(con, qry08_CredentialSupVars_From_Enrolment) # we can't do these checks for extra unmatched records
dbExecute(con, qry09_CredentialSupVars_From_Enrolment) # for (null/blank) EPENS pull max school year
dbExecute(con, qry10_CredentialSupVars_From_Enrolment) # bring in more enrolment information for the most recent school year 
dbExecute(con, qry11_CredentialSupVars_From_Enrolment) # add pky constraint
dbExecute(con, qry12_CredentialSupVars_From_Enrolment) # ...
dbExecute(con, qry12b_CredentialSupVars_From_Enrolment) # add pky constraint
dbExecute(con, qry13_CredentialSupVars_From_Enrolment) # # bring in credential record status from Credential View
dbExecute(con, paste0("DROP TABLE ", paste0("tmp_tbl_Enrol_ID_EPEN_For_Cred_Join_step", 1:6, collapse = ", ")))  # clean up

dbExecute(con, qry01a_CredentialSupVars) # select key columns from Credential View into a new table called CredentialSupVars
dbExecute(con, qry01b_CredentialSupVars) # add some more columns to be filled in later
dbExecute(con, "ALTER TABLE [CredentialSupVars] ADD CONSTRAINT PK_CredSupVars_ID PRIMARY KEY (ID);")

dbExecute(con, qry01b_CredentialSupVarsFromEnrol_1) # add some columns to CredSupVarsEnrol
dbExecute(con, qry01b_CredentialSupVarsFromEnrol_2) # bring in data from STP_Enrolment 
dbExecute(con, qry01b_CredentialSupVarsFromEnrol_3) # Empty strings ' ' in psi_birthdate_cleaned were cast to 1900-01-01 in date format. 

# flag records in STP_Credential_Record_Type (for removal?) with PSI_CREDENTIAL_CATEGORY = 'DEVELOPMENTAL CREDENTIAL' 'OTHER' 'NONE' 'SHORT CERTIFICATE'
dbExecute(con, qry02a_DropCredCategory) 
dbExecute(con, "Alter Table STP_Credential_Record_Type ADD DropCredCategory [varchar](50) NULL;")
dbExecute(con, qry02b_DeleteCredCategory)
dbExecute(con, "DROP TABLE Drop_Credential_Category")

# flag records in STP_Credential_Record_Type whose CREDENTIAL_AWARD_DATE >= '2019-09-01'
# this will have to be changed to 2023-09-01
dbExecute(con, qry03a1_ConvertAwardDate) # data type conversion
dbExecute(con, qry03b_DropPartialYear) 
dbExecute(con, "Alter Table STP_Credential_Record_Type ADD DropPartialYear [varchar](50) NULL;") 
dbExecute(con, qry03c_DeletePartialYear)
dbExecute(con, "DROP TABLE Drop_Partial_Year")

dbExecute(con, qry03d_CredentialSupVarsBirthdate) # create a table with unique EPEN/birthdates from CredentialSupVarsFromEnrolment
dbExecute(con, "UPDATE  CredentialSupVars_BirthdateClean 
                SET psi_birthdate_cleaned_D = psi_birthdate_cleaned
                WHERE psi_birthdate_cleaned is not null AND psi_birthdate_cleaned <> ''") # I think this is just data type conversion
dbExecute(con, qry03e_CredentialSupVarsGender) # create a table with unique EPEN/gender from CredentialSupVarsFromEnrolment

# ---- Gender Cleaning Queries Here ---- #
dbExecute(con, qry03fCredential_SupVarsGenderCleaning1)
dbExecute(con, qry03fCredential_SupVarsGenderCleaning2)
dbExecute(con, qry03fCredential_SupVarsGenderCleaning3)
dbExecute(con, qry03fCredential_SupVarsGenderCleaning4)
dbExecute(con, qry03fCredential_SupVarsGenderCleaning5)
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning6)
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning7)
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning8)
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning9)
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning10)
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning11)
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning12)
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning13)
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning14)
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning15)
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning16)
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning17)
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning18)
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning19)
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning20)
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning21)
dbExecute(con, qry03fCredential_SupVars_Enrol_GenderCleaning22)

# ---- Finished Gender Cleaning ----# 
dbExecute(con, qry04a_UpdateCredentialSupVarsBirthdate1) #  run for the records that matched on ENCRYPTED_TRUE_PEN (non-null/blank) - I think the logic is wrong here though
dbExecute(con, qry04a_UpdateCredentialSupVarsBirthdate2)# Run for the records that matched on PSI_STUDENT_NUMBER/PSI_CODE (non-null/blank)

# ---- Clean Up ----
dbExecute(con, "DROP TABLE CredentialSupVarsFromEnrolment_MultiGender")
dbExecute(con, "DROP TABLE MinEnrolmentSupVar")
dbExecute(con, "DROP VIEW MinEnrolment")
dbExecute(con, "DROP VIEW Credential")
dbExecute(con, "DROP TABLE CredentialSupVarsFromEnrolment")
dbExecute(con, "DROP TABLE CredentialSupVars")
dbExecute(con, "DROP TABLE CredentialSupVars_BirthdateClean")
dbExecute(con, "DROP TABLE CredentialSupVars_Gender")
dbExecute(con, "DROP TABLE CredentialSupVars_MultiGender")
dbExecute(con, "DROP TABLE tmp_CredentialGenderCleaning_Step1")
dbExecute(con, "DROP TABLE tmp_CredentialGenderCleaning_Step2")
dbExecute(con, "DROP TABLE tmp_CredentialGenderCleaning_Step3")
dbExecute(con, "DROP TABLE RW_TEST_ENROL_GENDER_morethanone_list_stepa")
dbExecute(con, "DROP TABLE RW_TEST_ENROL_GENDER_morethanone_list")
dbExecute(con, "DROP TABLE RW_TEST_ENROL_GENDER_morethanone_listIDS")
dbExecute(con, "DROP TABLE CredentialSupVars_MultiGenderCounter")
dbExecute(con, "DROP TABLE RW_TEST_ENROL_GENDER_morethanone_listIDS")
dbExecute(con, "DROP TABLE tmp_CredentialSupVars_Gender_CleanUnknowns")
dbExecute(con, "DROP TABLE tmp_CredentialSupVars_Gender_CleanUnknowns_Step2")
dbExecute(con, "DROP TABLE tmp_CredentialSupVars_Gender_CleanUnknowns_Step3")



dbDisconnect(con)
