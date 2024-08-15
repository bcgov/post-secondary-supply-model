library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(RJDBC)

# ---- Configure LAN and file paths ----
db_config <- config::get("decimal")
lan <- config::get("lan")
my_schema <- config::get("myschema")

# ---- Connection to database ----
db_config <- config::get("decimal")
decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")

# ---- Required data tables and SQL ----
source(glue::glue("{lan}/development/sql/gh-source/03-near-completers-ttrain/near-completers-investigation-ttrain.R"))
source(glue::glue("{lan}/development/sql/gh-source/03-near-completers-ttrain/dacso-near-completers.R"))

# tmp tables made in earlier part of workflow
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."t_dacso_data_part_1"'))) 
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Credential_Non_Dup"'))) 

# carry over from last model's run
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."tmp_tbl_Age"')))

# new data - see load-near-completers-ttrain.R
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."tmp_tbl_Age_AppendNewYears"')))

# lookups - see load-near-completers-ttrain.R
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."AgeGroupLookup"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."t_pssm_projection_cred_grp"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."combine_creds"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."stp_dacso_prgm_credential_lookup"')))

# ---- Execute SQL ----
dbExecute(decimal_con, "ALTER TABLE tmp_tbl_Age_AppendNewYears ADD BTHDT_CLEANED NVARCHAR(20) NULL")
dbExecute(decimal_con, "ALTER TABLE tmp_tbl_Age_AppendNewYears ADD ENDDT_CLEANED NVARCHAR(20) NULL")
dbExecute(decimal_con, qry_make_tmp_table_Age_step2)
dbExecute(decimal_con, "UPDATE tmp_tbl_Age_AppendNewYears SET ENDDT_CLEANED = '' WHERE ENDDT_CLEANED = '00/1/0000'")
dbExecute(decimal_con, "ALTER TABLE tmp_tbl_Age_AppendNewYears ADD BTHDT_DATE NVARCHAR(20) NULL")
dbExecute(decimal_con, "ALTER TABLE tmp_tbl_Age_AppendNewYears ADD ENDDT_DATE NVARCHAR(20) NULL")
dbExecute(decimal_con, qry_make_tmp_table_Age_step3)
dbExecute(decimal_con, "UPDATE tmp_tbl_Age_AppendNewYears SET ENDDT_DATE = NULL WHERE ENDDT_DATE = '1900-01-01'")
dbExecute(decimal_con, qry_make_tmp_table_Age_step4)

dbExecute(decimal_con, "ALTER TABLE T_DACSO_Data_Part_1 ADD Age_At_Grad FLOAT NULL")
dbExecute(decimal_con, "ALTER TABLE tmp_tbl_age ADD Age_At_Grad FLOAT NULL")
dbExecute(decimal_con, qry99_Update_Age_At_Grad)
dbExecute(decimal_con, "ALTER TABLE T_DACSO_Data_Part_1 ADD Grad_Status_Factoring_in_STP nvarchar(2) NULL")
dbExecute(decimal_con, qry99a_Update_Age_At_Grad)

# Note: possibly want to edit this to include only age groups up to age 64 
dbExecute(decimal_con, qry_make_T_DACSO_DATA_Part_1_TempSelection)

# do some dacso-stp matching
dbExecute(decimal_con, qry01_Match_DACSO_to_STP_Credential_Non_DUP_on_PEN)
dbExecute(decimal_con, "ALTER TABLE dacso_matching_stp_credential_pen ADD stp_prgm_credential_awarded_name nvarchar(50) NULL")
dbExecute(decimal_con, qry_Update_STP_PRGM_Credential_Awarded_Name)

# How many PEN matched records also match STP on credential category
dbExecute(decimal_con, "ALTER TABLE dacso_matching_stp_credential_pen ADD match_credential nvarchar(10) NULL")
dbExecute(decimal_con, qry02_Match_DACSO_STP_Credential_PSI_CRED_Category)

# How many PEN matched records also match STP on CIP4
dbExecute(decimal_con, "ALTER TABLE dacso_matching_stp_credential_pen ADD match_cip_code_4 nvarchar(10) NULL")
dbExecute(decimal_con, qry03_Match_DACSO_STP_Credential_CIPCODE4)

# How many PEN matched records also match STP on CIP2
dbExecute(decimal_con, "ALTER TABLE dacso_matching_stp_credential_pen ADD match_CIP_CODE_2 nvarchar(10) NULL")
dbExecute(decimal_con, qry03b_Match_DACSO_STP_Credential_CIPCODE2)

# How many PEN matched records also match STP on Award Year. 
#Add the new year combinations required in the query design first 
dbExecute(decimal_con, "ALTER TABLE dacso_matching_stp_credential_pen ADD match_award_school_year nvarchar(10) NULL")
dbExecute(decimal_con, qry04_Match_DACSO_STP_Credential_AwardYear)

# How many PEN matched records also match STP on Inst code
dbExecute(decimal_con, "ALTER TABLE dacso_matching_stp_credential_pen ADD match_inst nvarchar(10) NULL")
dbExecute(decimal_con, qry05_Match_DACSO_STP_Credential_Inst)

# Print summary of the matching results.
dbGetQuery(decimal_con, qry06_Match_DACSO_STP_Credential_Summary)

#  These are considered final matches to STP credential.
dbExecute(decimal_con, "ALTER TABLE dacso_matching_stp_credential_pen ADD final_consider_a_match nvarchar(10) NULL")
dbExecute(decimal_con, "ALTER TABLE dacso_matching_stp_credential_pen ADD match_all_4_flag nvarchar(10) NULL")
dbExecute(decimal_con, qry07_DACSO_STP_Credential_MatchAll4_Flag)

#  flags the records that match on inst, award year, credential, and CIP 2 (but not CIP 4) as final matches too. 
dbExecute(decimal_con, qry08_DACSO_STP_Credential_Final_Match_Flag)

# ---- Flag the DACSO near completer records that have an earlier or later credential in the STP Credential file ----
dbExecute(decimal_con, qry_Find_NearCompleters_in_STP_Credential_Step1)
dbExecute(decimal_con, "ALTER TABLE nearcompleters_in_stp_credential_step1 ADD STP_Credential_Awarded_Before_DACSO NVARCHAR(10) NULL")
dbExecute(decimal_con, "ALTER TABLE nearcompleters_in_stp_credential_step1 ADD STP_Credential_Awarded_After_DACSO NVARCHAR(10) NULL")
dbExecute(decimal_con, "ALTER TABLE nearcompleters_in_stp_credential_step1 ADD Has_Multiple_STP_Credentials NVARCHAR(10) NULL")
dbExecute(decimal_con, qry_Update_STP_Credential_Awarded_Before_DACSO)
dbExecute(decimal_con, qry_Update_STP_Credential_Awarded_After_DACSO)

dbExecute(decimal_con, qry_make_table_NearCompleters)
dbExecute(decimal_con, "ALTER TABLE T_DACSO_NearCompleters ADD STP_Credential_Awarded_Before_DACSO NVARCHAR(10) NULL")
dbExecute(decimal_con, "ALTER TABLE T_DACSO_NearCompleters ADD STP_Credential_Awarded_After_DACSO NVARCHAR(10) NULL")
dbExecute(decimal_con, "ALTER TABLE T_DACSO_NearCompleters ADD Has_Multiple_STP_Credentials NVARCHAR(10) NULL")
dbExecute(decimal_con, qry_update_T_DACSO_Near_Completers_step1)
dbExecute(decimal_con, qry_update_T_DACSO_Near_Completers_step2)

# ---- Flag near-completers with multiple credentials----
dbExecute(decimal_con, qry_NearCompleters_With_More_Than_One_Cdtl)
dbExecute(decimal_con, qry_Update_T_NearCompleters_HasMultipleCdtls)

# This takes some thought - I haven't convinced myself that having one credential is any different than
# many credentials, since in the end it seems to only matter if there is a credential.  I may be proven wrong
# so have flagged each near completer as Yes if have multi's and will rewrite this bit if it turns out to be
# important.

# dbExecute(decimal_con, qry_Clean_NearCompleters_MultiCdtls_Step1)
# dbExecute(decimal_con, qry_NearCompleters_MultiCdtls_Cleaning_Step2)
# 
# # Find record which represents the max psi award year
# dbExecute(decimal_con, qry_PickMaxYear_step1)
# dbExecute(decimal_con, "ALTER TABLE tmp_NearCompletersWithMultiCredentials_Cleaning ADD Max_Award_School_Year NVARCHAR(10) NULL")
# dbExecute(decimal_con, qry_NearCompleters_MultiCdtls_Cleaning_Step3)
# 
# dbExecute(decimal_con, "ALTER TABLE NearCompleters_in_STP_Credential_Step1 ADD Dup_STQUID_UseThisRecord NVARCHAR(10) NULL")
# dbExecute(decimal_con, qry_NearCompleters_MultiCdtls_Cleaning_Step4)
# dbExecute(decimal_con, qry_NearCompleters_MultiCdtls_Cleaning_Step5)
# dbExecute(decimal_con, qry_NearCompleters_MultiCdtls_Cleaning_Step6)
# dbExecute(decimal_con, qry_PickMaxYear_Step2)
# dbExecute(decimal_con, "ALTER TABLE tmp_NearCompletersWithMultiCredentials_MaxYearCleaning ADD Final_Record_To_Use NVARCHAR(10) NULL")
# dbExecute(decimal_con, qry_PickMaxYear_Step3)
# dbExecute(decimal_con, "ALTER TABLE NearCompleters_in_STP_Credential_Step1 ADD Final_Record_To_Use NVARCHAR(10) NULL")
# dbExecute(decimal_con, qry_NearCompleters_MultiCdtls_Cleaning_Step10)
# dbExecute(decimal_con, qry_NearCompleters_MultiCdtls_Cleaning_Step13)
# 
# dbExecute(decimal_con, "ALTER TABLE DACSO_Matching_STP_Credential_PEN ADD Dup_STQUID_UseThisRecord NVARCHAR(10) NULL")
# dbExecute(decimal_con, qry_Update_DupStqu_ID_UseThisRecord2)
# dbExecute(decimal_con, "ALTER TABLE NearCompleters_in_STP_Credential_Step1 ADD Final_Record_To_Use NVARCHAR(10) NULL")
# dbExecute(decimal_con, qry_Update_Final_Record_To_Use_NearCompletersDups)
# dbExecute(decimal_con, qry_NearCompleters_MultiCdtls_Cleaning_Step12)
# dbExecute(decimal_con, "ALTER TABLE T_DACSO_NearCompleters ADD STP_Credential_Awarded_Before_DACSO_Final NVARCHAR(10) NULL")
# dbExecute(decimal_con, "ALTER TABLE T_DACSO_NearCompleters ADD STP_Credential_Awarded_After_DACSO_Final NVARCHAR(10) NULL")
# dbExecute(decimal_con, qry_Update_Final_STP_Cred_Before_or_After_Step1)

dbExecute(decimal_con, "ALTER TABLE T_DACSO_DATA_Part_1_TempSelection ADD Has_STP_Credential NVARCHAR(10) NULL")
dbExecute(decimal_con, qry_update_Has_STP_Credential)

#dbExecute(decimal_con, "DROP TABLE tmp_NearCompletersWithMultiCredentials_Cleaning")
dbExecute(decimal_con, "DROP TABLE tmp_MaxAwardYear")
#dbExecute(decimal_con, "DROP TABLE tmp_NearCompletersWithMultiCredentials_MaxYear")
dbExecute(decimal_con, "DROP TABLE tmp_NearCompletersWithMultiCredentials_MaxYearCleaning")
dbExecute(decimal_con, "DROP TABLE T_DACSO_NearCompleters")
dbExecute(decimal_con, "DROP TABLE tmp_DACSO_NearCompleters_with_Multiple_Cdtls")
#dbExecute(decimal_con, "DROP TABLE tmp_MaxAwardYearCleaning_MaxID")
dbExecute(decimal_con, "DROP TABLE DACSO_Matching_STP_Credential_PEN")
dbExecute(decimal_con, "DROP TABLE nearcompleters_in_stp_credential_step1")

# ----- Queries for Near Completers Ratios in Excel Worksheets -----
dbExecute(decimal_con,  "ALTER TABLE T_DACSO_DATA_Part_1_TempSelection ADD Grad_Status_Factoring_in_STP NVARCHAR(10) NULL")
dbExecute(decimal_con,  qry_update_Grad_Status_Factoring_in_STP_step1)
dbExecute(decimal_con,  qry_update_Grad_Status_Factoring_in_STP_step2) 

# The following 4 queries are used to choose which years to base a ratio on.
# Only ages 17-34 included, adjust query to change this.
# Dro pinto Excel NearCompleters_AgeAtGrad17to64 and 17to34
dbGetQuery(decimal_con, qry99_Investigate_Near_Completes_vs_Graduates_by_Year)
dbGetQuery(decimal_con, qry99_GradStatus_Factoring_in_STP_Credential_by_Year)
dbGetQuery(decimal_con, qry99_GradStatus_byCred_by_Year_Age_At_Grad)
dbGetQuery(decimal_con, qry99_GradStatus_Factoring_in_STP_byCred_by_Year_Age_At_Grad)
dbGetQuery(decimal_con, qry_details_of_STP_Credential_Matching)

# Note: The ratios created in this section combine ages 35+ into a single group (35-64).
agegrouplookup <- dbReadTable(decimal_con, "agegrouplookup")
agegroupnearcompleterslookup <- agegrouplookup %>% 
  filter(AgeIndex %in% 2:5) %>% 
  mutate(AgeIndex = AgeIndex -1) %>%
  add_case(AgeIndex = 5, AgeGroup = "35 to 64", LowerBound = 35, UpperBound = 64)
dbWriteTable(decimal_con, "agegroupnearcompleterslookup", agegroupnearcompleterslookup)

dbExecute(decimal_con, qry99_Near_completes_total_by_CIP4)
dbGetQuery(decimal_con, qry_Make_NearCompleters_CIP4_CombinedCred) 
dbExecute(decimal_con, "ALTER TABLE T_DACSO_Data_Part_1 ADD Has_STP_Credential NVARCHAR(10)")
dbExecute(decimal_con, "UPDATE T_DACSO_DATA_Part_1 
                        SET Has_STP_Credential = T_DACSO_DATA_Part_1_TempSelection.Has_STP_Credential,
                            Grad_Status_Factoring_In_STP = T_DACSO_DATA_Part_1_TempSelection.Grad_Status_Factoring_In_STP
                        FROM T_DACSO_DATA_Part_1 INNER JOIN T_DACSO_DATA_Part_1_TempSelection 
                        ON T_DACSO_DATA_Part_1.COCI_STQU_ID = T_DACSO_DATA_Part_1_TempSelection.COCI_STQU_ID")
dbExecute(decimal_con, qry99_Near_completes_total_with_STP_Credential_ByCIP4)
dbGetQuery(decimal_con, qry_Make_NearCompleters_CIP4_With_STP_CombinedCred)


dbExecute(decimal_con, "DROP TABLE NearCompleters_CIP4")
#dbExecute(decimal_con, "DROP TABLE nearcompleters_cip4_combinedcred")
dbExecute(decimal_con, "DROP TABLE NearCompleters_CIP4_with_STP_Credential")
#dbExecute(decimal_con, "DROP TABLE nearcompleters_cip4_combinedcred_with_stp_credential")

dbExecute(decimal_con, qry99_Completers_agg_factoring_in_STP_Credential_by_CIP4)
dbExecute(decimal_con, "alter table completersfactoringinstp_cip4 add lcip4_cred_cleaned nvarchar(50) NULL;")
dbExecute(decimal_con, "update completersfactoringinstp_cip4 
                        set lcip4_cred_cleaned = 
                        	CASE WHEN PATINDEX('%1 - %', lcip4_cred) = 1 THEN REPLACE(lcip4_cred, '1 - ', '3 - ') 
                        	ELSE lcip4_cred
                        	END
                        from completersfactoringinstp_cip4")

# see Excel sheet 
dbGetQuery(decimal_con, qry_Make_CompletersFactoringInSTP_CIP4_CombinedCred)

dbExecute(decimal_con, qry99_Completers_agg_byCIP4)
dbExecute(decimal_con, "alter table completerscip4 add lcip4_cred_cleaned nvarchar(50) NULL;")
dbExecute(decimal_con, "update completerscip4 
                        set lcip4_cred_cleaned = 
                        	CASE WHEN PATINDEX('%1 - %', lcip4_cred) = 1 THEN REPLACE(lcip4_cred, '1 - ', '3 - ') 
                        	ELSE lcip4_cred
                        	END
                        from completerscip4")
dbGetQuery(decimal_con, qry_Make_Completers_CIP4_CombinedCred)

dbExecute(decimal_con, "DROP TABLE completersfactoringinstp_cip4")
dbExecute(decimal_con, "DROP TABLE completersfactoringinstp_cip4_combinedcred")
dbExecute(decimal_con, "DROP TABLE completerscip4")
dbExecute(decimal_con, "DROP TABLE completers_cip4_combinedcred")

near_completer_ratio = dbGetQuery(decimal_con, qry99_Near_completes_total_byGender) %>%
  rename("n_nc" = "Count") %>% 
  inner_join(
    dbGetQuery(decimal_con, qry99_Near_completes_total_with_STP_Credential_by_Gender) %>% 
      rename("n_nc_stp_early_late" = "Count") %>% 
      select(-has_stp_credential)
    ) %>%
  left_join (
    dbGetQuery(decimal_con, qry99_Completers_agg_by_gender) %>%
      rename("n_completers" = "Count")) %>%
  mutate(prgm_credential_awarded_name = 
           if_else(prgm_credential_awarded_name %in% c('Associate Degree','University Transfer'), 
           'Associate Degree/University Transfer', prgm_credential_awarded_name)) %>%
  summarize(n_nc = sum(n_nc, na.rm = TRUE), 
            n_nc_stp_early_late =  sum(n_nc_stp_early_late, na.rm = TRUE),
            n_completers =  sum(n_completers, na.rm = TRUE), 
            .by = c(tpid_lgnd_cd, agegroup, prgm_credential_awarded_name)) %>%
  mutate(n_nc_stp = n_nc - n_nc_stp_early_late) %>%
  mutate(r = if_else(n_completers > 0, n_nc_stp/n_completers, 0))

dbWriteTable(decimal_con, name = "near_completer_ratio", near_completer_ratio)

dbGetQuery(decimal_con, qry99_Near_completes_factoring_in_STP_total)

# ---- TTRAIN tables ----
# Note: the first query filters on cosc_grad_status_lgds_cd_group = '3'
# The second one doesn't
dbExecute(decimal_con, qry99_Near_completes_total_by_CIP4_TTRAIN)
dbExecute(decimal_con, qry99_Near_completes_total_with_STP_Credential_ByCIP4_TTRAIN)
dbExecute(decimal_con, qry99_Near_completes_program_dist_count) # check pssm_credential column - presence of both 'OR' and 'or' creates faux-duplicates

dbExecute(decimal_con, "DROP TABLE Near_completes_total_by_CIP4_TTRAIN")
dbExecute(decimal_con, "DROP TABLE Near_completes_total_with_STP_Credential_ByCIP4_TTRAIN")
dbExecute(decimal_con, "DROP TABLE T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN")


# ---- Clean Up ----
dbExecute(decimal_con, "DROP TABLE stp_dacso_prgm_credential_lookup")
dbExecute(decimal_con, "DROP TABLE tmp_tbl_Age")
dbExecute(decimal_con, "DROP TABLE tmp_tbl_Age_AppendNewYears")
dbExecute(decimal_con, "DROP TABLE AgeGroupLookup")
dbExecute(decimal_con, "DROP TABLE T_DACSO_DATA_Part_1_TempSelection")
dbExecute(decimal_con, "DROP TABLE combine_creds")
dbExecute(decimal_con, "DROP TABLE T_DACSO_DATA_Part_1")
dbExecute(decimal_con, "DROP TABLE agegroupnearcompleterslookup")
dbExecute(decimal_con, "DROP TABLE t_pssm_projection_cred_grp")





