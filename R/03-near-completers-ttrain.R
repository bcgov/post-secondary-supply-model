# Copyright 2024 Province of British Columbia
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations under the License.

# This script computes the ratio of near completers to graduates by age group and credential
# Near completers who later received a credential according to the STP Credential 
# file or had an earlier credential are subtracted from the total of all near completers.
# 
# Age groups: 17 to 19, 20 to 24, 25 to 29, and 35 to 64
# Credentials: From Diploma, Associate Degree, and Certificate Outcomes Survey cohorts. 
# Survey years: 2018, 2019, 2020, 2021, 2022, 2023 for PSSM 2023
# STP Credential years searched: 2002/03 - 2022/23 
#
# Annual ratios are computed for all available years and an average taken of two or three representative years
# (chosen by investigation).  PSSM model 2023 used an average ratio of 2018-2019.
# Notes: Using age at grad (not age at survey) for age groupings.  


library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(RJDBC)
library(assertthat)

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

# ---- Data Requirements and SQL Definitons ----
source("./sql/03-near-completers/near-completers-investigation-ttrain.R")
source("./sql/03-near-completers/dacso-near-completers.R")

# tables made in earlier part of workflow
assert_that(dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."t_dacso_data_part_1"'))))
assert_that(dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Credential_Non_Dup"'))))

# rollover tables - this can be removed later
assert_that(dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."tmp_tbl_Age"'))))

# new data
assert_that(dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."tmp_tbl_Age_AppendNewYears"'))))

# lookups
assert_that(dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."tbl_Age"'))))
assert_that(dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."t_pssm_projection_cred_grp"'))))
assert_that(dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."combine_creds"'))))
assert_that(dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."stp_dacso_prgm_credential_lookup"'))))
assert_that(dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."AgeGroupLookup"'))))

# ---- Derive Age at Grad ----
dbExecute(decimal_con, "ALTER TABLE tmp_tbl_Age_AppendNewYears ADD BTHDT_CLEANED NVARCHAR(20) NULL")
dbExecute(decimal_con, "ALTER TABLE tmp_tbl_Age_AppendNewYears ADD ENDDT_CLEANED NVARCHAR(20) NULL")
dbExecute(decimal_con, "ALTER TABLE tmp_tbl_Age_AppendNewYears ADD BTHDT_DATE NVARCHAR(20) NULL")
dbExecute(decimal_con, "ALTER TABLE tmp_tbl_Age_AppendNewYears ADD ENDDT_DATE NVARCHAR(20) NULL")
dbExecute(decimal_con, qry_make_tmp_table_Age_step2)
dbExecute(decimal_con, "UPDATE tmp_tbl_Age_AppendNewYears SET ENDDT_CLEANED = '' WHERE ENDDT_CLEANED = '00/1/0000'")
dbExecute(decimal_con, qry_make_tmp_table_Age_step3)
dbExecute(decimal_con, "UPDATE tmp_tbl_Age_AppendNewYears SET ENDDT_DATE = NULL WHERE ENDDT_DATE = '1900-01-01'")
dbExecute(decimal_con, qry_make_tmp_table_Age_step4)
dbExecute(decimal_con, "DROP TABLE tmp_tbl_Age_AppendNewYears") # drop the new table

dbExecute(decimal_con, "ALTER TABLE T_DACSO_Data_Part_1 ADD Age_At_Grad FLOAT NULL")
# dbExecute(decimal_con, "ALTER TABLE tmp_tbl_age ADD Age_At_Grad FLOAT NULL") # in 'load-near-completers-ttrain.R', we read the CSV for it and it has the age at grad. 
dbExecute(decimal_con, qry99_Update_Age_At_Grad)
dbExecute(decimal_con, qry99a_Update_Age_At_Grad)

# use a temporary subset of columns from T_DACSO_DATA_Part_1 for selection
dbExecute(decimal_con, qry_make_T_DACSO_DATA_Part_1_TempSelection)
dbGetQuery(decimal_con, qry99_Investigate_Near_Completes_vs_Graduates_by_Year)

# ---- Add PEN to Non-Dup table ----
# Note: Move to earlier workflow - 02 series.  This updates credential non-dup in current schema only
sql <- glue::glue("ALTER TABLE pssm2023.[{my_schema}].credential_non_dup
ADD PSI_PEN NVARCHAR(255) NULL;")
dbExecute(decimal_con, sql)

sql <- glue::glue("UPDATE N
SET N.PSI_PEN = C.PSI_PEN
FROM pssm2023.[{my_schema}].credential_non_dup AS N
INNER JOIN dbo.STP_Credential AS C
ON N.ID = C.ID
")
dbExecute(decimal_con, sql)

# ---- DACSO Matching STP Credential ----
dbExecute(decimal_con, qry01_Match_DACSO_to_STP_Credential_Non_DUP_on_PEN)
dbExecute(decimal_con, "ALTER TABLE dacso_matching_stp_credential_pen ADD stp_prgm_credential_awarded_name nvarchar(50) NULL")
dbExecute(decimal_con, "ALTER TABLE dacso_matching_stp_credential_pen ADD match_credential nvarchar(10) NULL")
dbExecute(decimal_con, "ALTER TABLE dacso_matching_stp_credential_pen ADD match_cip_code_4 nvarchar(10) NULL")
dbExecute(decimal_con, "ALTER TABLE dacso_matching_stp_credential_pen ADD match_CIP_CODE_2 nvarchar(10) NULL")
dbExecute(decimal_con, "ALTER TABLE dacso_matching_stp_credential_pen ADD match_award_school_year nvarchar(10) NULL")
dbExecute(decimal_con, "ALTER TABLE dacso_matching_stp_credential_pen ADD match_inst nvarchar(10) NULL")
dbExecute(decimal_con, qry_Update_STP_PRGM_Credential_Awarded_Name)

# How many PEN matched records also match STP on credential category
dbExecute(decimal_con, qry02_Match_DACSO_STP_Credential_PSI_CRED_Category)
# How many PEN matched records also match STP on CIP4
dbExecute(decimal_con, qry03_Match_DACSO_STP_Credential_CIPCODE4)
# How many PEN matched records also match STP on CIP2
dbExecute(decimal_con, qry03b_Match_DACSO_STP_Credential_CIPCODE2)
# How many PEN matched records also match STP on Award Year. 
dbExecute(decimal_con, qry04_Match_DACSO_STP_Credential_AwardYear) # Manual: Add the new year combinations to query design first 
# How many PEN matched records also match STP on Inst code
dbExecute(decimal_con, qry05_Match_DACSO_STP_Credential_Inst)
# Print summary of the matching results.
dbGetQuery(decimal_con, qry06_Match_DACSO_STP_Credential_Summary)

#  These are considered final matches to STP credential.
dbExecute(decimal_con, "ALTER TABLE dacso_matching_stp_credential_pen ADD final_consider_a_match nvarchar(10) NULL")
dbExecute(decimal_con, "ALTER TABLE dacso_matching_stp_credential_pen ADD match_all_4_flag nvarchar(10) NULL")
dbExecute(decimal_con, qry07_DACSO_STP_Credential_MatchAll4_Flag)

#  Flag records that match on inst, award year, credential, and CIP 2 (but not CIP 4) as final matches too. 
dbExecute(decimal_con, qry08_DACSO_STP_Credential_Final_Match_Flag)

# ---- Flag near-completers with earlier or later credential----
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

dbExecute(decimal_con, qry_Clean_NearCompleters_MultiCdtls_Step1)
dbExecute(decimal_con, qry_NearCompleters_MultiCdtls_Cleaning_Step2)

# Find record with max psi award year
dbExecute(decimal_con, qry_PickMaxYear_step1)
dbExecute(decimal_con, "ALTER TABLE tmp_NearCompletersWithMultiCredentials_Cleaning ADD Max_Award_School_Year NVARCHAR(10) NULL")
dbExecute(decimal_con, qry_NearCompleters_MultiCdtls_Cleaning_Step3)

dbExecute(decimal_con, "ALTER TABLE NearCompleters_in_STP_Credential_Step1 ADD Dup_STQUID_UseThisRecord NVARCHAR(10) NULL")
dbExecute(decimal_con, qry_NearCompleters_MultiCdtls_Cleaning_Step4)
dbExecute(decimal_con, qry_NearCompleters_MultiCdtls_Cleaning_Step5)
dbExecute(decimal_con, qry_NearCompleters_MultiCdtls_Cleaning_Step6)
dbExecute(decimal_con, qry_PickMaxYear_Step2)
dbExecute(decimal_con, "ALTER TABLE tmp_NearCompletersWithMultiCredentials_MaxYearCleaning ADD Final_Record_To_Use NVARCHAR(10) NULL")
dbExecute(decimal_con, qry_PickMaxYear_Step3)
dbExecute(decimal_con, "ALTER TABLE NearCompleters_in_STP_Credential_Step1 ADD Final_Record_To_Use NVARCHAR(10) NULL")
dbExecute(decimal_con, qry_NearCompleters_MultiCdtls_Cleaning_Step10)
dbExecute(decimal_con, qry_NearCompleters_MultiCdtls_Cleaning_Step13)

dbExecute(decimal_con, "ALTER TABLE DACSO_Matching_STP_Credential_PEN ADD Dup_STQUID_UseThisRecord NVARCHAR(10) NULL")
dbExecute(decimal_con, qry_Update_DupStqu_ID_UseThisRecord2)
#dbExecute(decimal_con, "ALTER TABLE NearCompleters_in_STP_Credential_Step1 ADD Final_Record_To_Use NVARCHAR(10) NULL")
dbExecute(decimal_con, qry_Update_Final_Record_To_Use_NearCompletersDups)
dbExecute(decimal_con, qry_NearCompleters_MultiCdtls_Cleaning_Step12)
dbExecute(decimal_con, "ALTER TABLE T_DACSO_NearCompleters ADD STP_Credential_Awarded_Before_DACSO_Final NVARCHAR(10) NULL")
dbExecute(decimal_con, "ALTER TABLE T_DACSO_NearCompleters ADD STP_Credential_Awarded_After_DACSO_Final NVARCHAR(10) NULL")
dbExecute(decimal_con, qry_Update_Final_STP_Cred_Before_or_After_Step1)

dbExecute(decimal_con, "ALTER TABLE T_DACSO_DATA_Part_1_TempSelection ADD Has_STP_Credential NVARCHAR(10) NULL")
dbExecute(decimal_con, "ALTER TABLE T_DACSO_Data_Part_1 ADD Has_STP_Credential NVARCHAR(10)")
dbExecute(decimal_con, qry_update_Has_STP_Credential)

dbExecute(decimal_con, "ALTER TABLE T_DACSO_Data_Part_1 ADD Grad_Status_Factoring_in_STP nvarchar(2) NULL")
dbExecute(decimal_con,"ALTER TABLE T_DACSO_DATA_Part_1_TempSelection ADD Grad_Status_Factoring_in_STP NVARCHAR(10) NULL")
dbExecute(decimal_con,  qry_update_Grad_Status_Factoring_in_STP_step1)
dbExecute(decimal_con,  qry_update_Grad_Status_Factoring_in_STP_step2) 

dbExecute(decimal_con, "UPDATE T_DACSO_DATA_Part_1 
                        SET Has_STP_Credential = T_DACSO_DATA_Part_1_TempSelection.Has_STP_Credential,
                            Grad_Status_Factoring_In_STP = T_DACSO_DATA_Part_1_TempSelection.Grad_Status_Factoring_In_STP
                        FROM T_DACSO_DATA_Part_1 INNER JOIN T_DACSO_DATA_Part_1_TempSelection 
                        ON T_DACSO_DATA_Part_1.COCI_STQU_ID = T_DACSO_DATA_Part_1_TempSelection.COCI_STQU_ID")

dbExecute(decimal_con, "DROP TABLE tmp_NearCompletersWithMultiCredentials_Cleaning")
dbExecute(decimal_con, "DROP TABLE tmp_NearCompletersWithMultiCredentials_MaxYear")
dbExecute(decimal_con, "DROP TABLE tmp_NearCompletersWithMultiCredentials_MaxYearCleaning")
dbExecute(decimal_con, "DROP TABLE T_DACSO_NearCompleters")
dbExecute(decimal_con, "DROP TABLE tmp_MaxAwardYear")
dbExecute(decimal_con, "DROP TABLE tmp_DACSO_NearCompleters_with_Multiple_Cdtls")
dbExecute(decimal_con, "DROP TABLE tmp_MaxAwardYearCleaning_MaxID")
dbExecute(decimal_con, "DROP TABLE DACSO_Matching_STP_Credential_PEN")
dbExecute(decimal_con, "DROP TABLE nearcompleters_in_stp_credential_step1")

# ----- Check Near Completers Ratios -----
dbGetQuery(decimal_con, qry99_Investigate_Near_Completes_vs_Graduates_by_Year)
dbGetQuery(decimal_con, qry99_GradStatus_Factoring_in_STP_Credential_by_Year)
dbGetQuery(decimal_con, qry99_GradStatus_byCred_by_Year_Age_At_Grad) 
dbGetQuery(decimal_con, qry99_GradStatus_Factoring_in_STP_byCred_by_Year_Age_At_Grad)
dbGetQuery(decimal_con, qry_details_of_STP_Credential_Matching) 

# Queries are for Excel: C_Outc12_13_14RatiosAgeGradCIP4
#1 (col H in Excel sheet)
dbExecute(decimal_con, qry99_Near_completes_total_by_CIP4)
dbExecute(decimal_con, qry_Make_NearCompleters_CIP4_CombinedCred) 
NearCompleters_CIP4_CombinedCred <- dbReadTable(decimal_con, "NearCompleters_CIP4_CombinedCred")
NearCompleters_CIP4_CombinedCred$lcip4_cred <- gsub("-\\s(0|1)\\s","", NearCompleters_CIP4_CombinedCred$lcip4_cred)
NearCompleters_CIP4_CombinedCred <- NearCompleters_CIP4_CombinedCred %>% 
  summarise(count = sum(CombinedCredCount, na.rm = TRUE), .by = c(age_group, lcip4_cred, lcp4_cd))

#2 (col I in Excel sheet)
dbExecute(decimal_con, qry99_Near_completes_total_with_STP_Credential_ByCIP4)
dbExecute(decimal_con, qry_Make_NearCompleters_CIP4_With_STP_CombinedCred)
NearCompleters_CIP4_With_STP_CombinedCred <- dbReadTable(decimal_con, "NearCompleters_CIP4_With_STP_CombinedCred")
NearCompleters_CIP4_With_STP_CombinedCred$lcip4_cred <- gsub("-\\s(0|1)\\s","", NearCompleters_CIP4_With_STP_CombinedCred$lcip4_cred)
NearCompleters_CIP4_With_STP_CombinedCred <- NearCompleters_CIP4_With_STP_CombinedCred %>% 
  summarise(nc_with_earlier_or_later = sum(CombinedCredCount, na.rm = TRUE), .by = c(age_group, lcip4_cred, lcp4_cd))

  
#3 (col K in Excel sheet)
dbExecute(decimal_con, qry99_Completers_agg_factoring_in_STP_Credential_by_CIP4)
dbExecute(decimal_con, "alter table completersfactoringinstp_cip4 add lcip4_cred_cleaned nvarchar(50) NULL;")
dbExecute(decimal_con, "update completersfactoringinstp_cip4 
                        set lcip4_cred_cleaned = 
                        	CASE WHEN PATINDEX('%1 - %', lcip4_cred) = 1 THEN STUFF(lcip4_cred, 1, 3,'3 -')  
                        	ELSE lcip4_cred
                        	END
                        from completersfactoringinstp_cip4")

dbExecute(decimal_con, qry_Make_CompletersFactoringInSTP_CIP4_CombinedCred)
CompletersFactoringInSTP_CIP4_CombinedCred <- dbReadTable(decimal_con, "CompletersFactoringInSTP_CIP4_CombinedCred")
CompletersFactoringInSTP_CIP4_CombinedCred$lcip4_cred <- gsub("-\\s(0|1)\\s","", CompletersFactoringInSTP_CIP4_CombinedCred$lcip4_cred_cleaned)
CompletersFactoringInSTP_CIP4_CombinedCred <- CompletersFactoringInSTP_CIP4_CombinedCred %>% 
  summarise(completers = sum(CombinedCredCount, na.rm = TRUE), .by = c(age_group, lcip4_cred, lcp4_cd))


#4 (col M in Excel sheet)
dbExecute(decimal_con, qry99_Completers_agg_byCIP4)
dbExecute(decimal_con, "alter table completerscip4 add lcip4_cred_cleaned nvarchar(50) NULL;")
dbExecute(decimal_con, "update completerscip4 
                        set lcip4_cred_cleaned = 
                        	CASE WHEN PATINDEX('%1 - %', lcip4_cred) = 1 THEN STUFF(lcip4_cred, 1, 3,'3 -') 
                        	ELSE lcip4_cred
                        	END
                        from completerscip4")

dbExecute(decimal_con, qry_Make_Completers_CIP4_CombinedCred) 
Completers_CIP4_CombinedCred <- dbReadTable(decimal_con, "Completers_CIP4_CombinedCred")
Completers_CIP4_CombinedCred$lcip4_cred <- gsub("-\\s(0|1)\\s","", Completers_CIP4_CombinedCred$lcip4_cred_cleaned)
Completers_CIP4_CombinedCred  <- Completers_CIP4_CombinedCred  %>% 
  summarise(c_not_factoring_stp = sum(CombinedCredCount, na.rm = TRUE), .by = c(age_group, lcip4_cred, lcp4_cd))

T_DACSO_Near_Completers_RatioAgeAtGradCIP4 <- NearCompleters_CIP4_CombinedCred %>%
  left_join(NearCompleters_CIP4_With_STP_CombinedCred, by = join_by(age_group, lcip4_cred, lcp4_cd)) %>%
  left_join(CompletersFactoringInSTP_CIP4_CombinedCred, by = join_by(age_group, lcip4_cred, lcp4_cd)) %>%
  left_join(Completers_CIP4_CombinedCred, by = join_by(age_group, lcip4_cred, lcp4_cd)) %>%
  mutate(across(where(is.numeric), ~replace_na(.,0))) %>%
  mutate(near_completers_stp_cred = count-nc_with_earlier_or_later, 
         ratio = near_completers_stp_cred/completers, 
         ratio_not_factoring_stp = near_completers_stp_cred/c_not_factoring_stp) %>%
  mutate(across(where(is.double), ~na_if(., Inf)))%>%
  mutate_all(function(x) ifelse(is.nan(x), NA, x))

dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."T_DACSO_Near_Completers_RatioAgeAtGradCIP4"')), T_DACSO_Near_Completers_RatioAgeAtGradCIP4)
dbExecute(decimal_con, "DROP TABLE NearCompleters_CIP4")
dbExecute(decimal_con, "DROP TABLE NearCompleters_CIP4_with_STP_Credential")
dbExecute(decimal_con, "DROP TABLE completersfactoringinstp_cip4")
dbExecute(decimal_con, "DROP TABLE completerscip4")

# Queries are for Excel: C_Outc12_13_14RatiosByGender
#1: paste to col E
dbExecute(decimal_con, qry99_Near_completes_total_byGender)
Near_completes_total_byGender <-  dbReadTable(decimal_con, "Near_completes_total_byGender")
dbExecute(decimal_con, "DROP TABLE Near_completes_total_byGender")

#2: paste to col F
dbExecute(decimal_con, qry99_Near_completes_total_with_STP_Credential_by_Gender)
Near_completes_total_with_STP_Credential_by_Gender <- dbReadTable(decimal_con, "Near_completes_total_with_STP_Credential_by_Gender") %>% 
  rename("nc_with_early_or_late" = "Count")  %>% 
  select(-has_stp_credential)
dbExecute(decimal_con, "DROP TABLE Near_completes_total_with_STP_Credential_by_Gender")

#3: looks like paste to H (check)
dbExecute(decimal_con, qry99_Completers_agg_by_gender) 
Completers_agg_by_gender <- dbReadTable(decimal_con, "Completers_agg_by_gender") %>%
  rename("completers" = "Count")
dbExecute(decimal_con, "DROP TABLE Completers_agg_by_gender")

ratio.df = Near_completes_total_byGender %>% 
  left_join(Near_completes_total_with_STP_Credential_by_Gender)  %>%
  left_join (Completers_agg_by_gender) %>%
  rename("gender" = "tpid_lgnd_cd")

# we want the adjusted ratio from column L (or just the normal ratio for nc for this year)
ratio.df  <- ratio.df %>%
  mutate(across(where(is.numeric), ~replace_na(.,0))) %>%
  mutate(n_nc_stp = Count - nc_with_early_or_late) %>%
  mutate(ratio = n_nc_stp/completers)

ratio.df2 <- ratio.df %>%
    filter(prgm_credential_awarded_name %in% c("Associate Degree", "University Transfer")) %>%
    mutate(prgm_credential_awarded_name = "Associate Degree") %>%
    summarise(ratio_adgt= sum(n_nc_stp)/sum(completers), .by = c(gender, age_group, prgm_credential_awarded_name))

T_DACSO_Near_Completers_RatioByGender <- 
  ratio.df %>% 
  left_join(ratio.df2) %>%
  mutate(ratio = if_else(prgm_credential_awarded_name %in% c("Associate Degree", "University Transfer"), ratio_adgt, ratio)) %>%
  mutate(across(where(is.double), ~na_if(., Inf))) %>%
  mutate_all(function(x) ifelse(is.nan(x), NA, x)) %>%
  select(-ratio_adgt)

dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."T_DACSO_Near_Completers_RatioByGender"')), T_DACSO_Near_Completers_RatioByGender)

# 4. Same as above (3.) but by year - to get historical 

# 4.1: paste to col E
dbExecute(decimal_con, qry99_Near_completes_total_byGender_year)
Near_completes_total_byGender_year <-  dbReadTable(decimal_con, "Near_completes_total_byGender_year")
dbExecute(decimal_con, "DROP TABLE Near_completes_total_byGender_year")

# 4.2: paste to col F
dbExecute(decimal_con, qry99_Near_completes_total_with_STP_Credential_by_Gender_year)
Near_completes_total_with_STP_Credential_by_Gender_year <- dbReadTable(decimal_con, "Near_completes_total_with_STP_Credential_by_Gender_year") %>% 
  rename("nc_with_early_or_late" = "Count")  %>% 
  select(-has_stp_credential)
dbExecute(decimal_con, "DROP TABLE Near_completes_total_with_STP_Credential_by_Gender_year")

# 4.3 get full ratio 
dbExecute(decimal_con, qry99_Completers_agg_by_gender_age_year) 
Completers_agg_by_gender_age_year <- dbReadTable(decimal_con, "Completers_agg_by_gender_age_year") %>%
  rename("completers" = "Count")
dbExecute(decimal_con, "DROP TABLE Completers_agg_by_gender_age_year")

ratio.df = Near_completes_total_byGender_year %>% 
  left_join(Near_completes_total_with_STP_Credential_by_Gender_year)  %>%
  left_join(Completers_agg_by_gender_age_year) %>%
  rename("gender" = "tpid_lgnd_cd")

# we want the adjusted ratio from column L (or just the normal ratio for nc for this year)
ratio.df  <- ratio.df %>%
  mutate(across(where(is.numeric), ~replace_na(.,0))) %>%
  mutate(n_nc_stp = Count - nc_with_early_or_late) %>%
  mutate(ratio = n_nc_stp/completers)

ratio.df2 <- ratio.df %>%
  filter(prgm_credential_awarded_name %in% c("Associate Degree", "University Transfer")) %>%
  mutate(prgm_credential_awarded_name = "Associate Degree") %>%
  summarise(ratio_adgt= sum(n_nc_stp)/sum(completers), .by = c(gender, age_group, prgm_credential_awarded_name))

# my question here - is this the right year to switch to?
# in lookup table, DACSO data should be sent back by one 
T_DACSO_Near_Completers_RatioByGender_year <- 
  ratio.df %>% 
  left_join(ratio.df2) %>%
  mutate(ratio = if_else(prgm_credential_awarded_name %in% c("Associate Degree", "University Transfer"), ratio_adgt, ratio)) %>%
  mutate(across(where(is.double), ~na_if(., Inf))) %>%
  mutate_all(function(x) ifelse(is.nan(x), NA, x)) %>%
  select(-ratio_adgt) %>% 
  # subtract one here so that it's the first half of the school year
  mutate(
    year = as.numeric(paste0('20', str_sub(coci_subm_cd, 7,8)))-1
  )

dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."T_DACSO_Near_Completers_RatioByGender_year"')), T_DACSO_Near_Completers_RatioByGender_year)

# random query
#dbGetQuery(decimal_con, qry99_Near_completes_factoring_in_STP_total)

# ---- TTRAIN tables ----
# This part is not completed  - see documentation
# Note: the first query filters on cosc_grad_status_lgds_cd_group = '3'
dbExecute(decimal_con, qry99_Near_completes_total_by_CIP4_TTRAIN)
dbExecute(decimal_con, qry99_Near_completes_total_with_STP_Credential_ByCIP4_TTRAIN)
dbExecute(decimal_con, qry99_Near_completes_program_dist_count) 

dbExecute(decimal_con, "DROP TABLE Near_completes_total_by_CIP4_TTRAIN")
dbExecute(decimal_con, "DROP TABLE Near_completes_total_with_STP_Credential_ByCIP4_TTRAIN")

# ---- HISTORICAL TTRAIN queries ----
# note: this uses the same intermediate table names as the above, so make sure the 2 drops are performed
dbExecute(decimal_con, qry99_Near_completes_total_by_CIP4_TTRAIN_history)
dbExecute(decimal_con, qry99_Near_completes_total_with_STP_Credential_ByCIP4_TTRAIN_history)
dbExecute(decimal_con, qry99_Near_completes_program_dist_count_history) 

dbExecute(decimal_con, "DROP TABLE Near_completes_total_by_CIP4_TTRAIN")
dbExecute(decimal_con, "DROP TABLE Near_completes_total_with_STP_Credential_ByCIP4_TTRAIN")


# ---- Clean Up ----
# TODO: clean up this section
dbExecute(decimal_con, "DROP TABLE stp_dacso_prgm_credential_lookup")
dbExecute(decimal_con, "DROP TABLE tmp_tbl_Age")
dbExecute(decimal_con, "DROP TABLE tbl_Age")
dbExecute(decimal_con, "DROP TABLE AgeGroupLookup")
dbExecute(decimal_con, "DROP TABLE T_DACSO_DATA_Part_1_TempSelection")
dbExecute(decimal_con, "DROP TABLE combine_creds")
dbExecute(decimal_con, "DROP TABLE t_pssm_projection_cred_grp")
dbExecute(decimal_con, "drop table nearcompleters_cip4_combinedcred")
dbExecute(decimal_con, "drop table NearCompleters_CIP4_With_STP_CombinedCred")
dbExecute(decimal_con, "drop table CompletersFactoringInSTP_CIP4_CombinedCred")
dbExecute(decimal_con, "drop table Completers_CIP4_CombinedCred")

# ---- Keep for program projections ----
dbExistsTable(decimal_con, "T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN")
dbExistsTable(decimal_con, "T_DACSO_Near_Completers_RatioAgeAtGradCIP4")
dbExistsTable(decimal_con, "T_DACSO_Near_Completers_RatioByGender")



