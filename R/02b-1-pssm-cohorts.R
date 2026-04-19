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

# This script prepares student outcomes data for the following student surveys: TRD, APP, DACSO, BGS
#
# APP:
#     Assumes geocoding has been done, and CURRENT_REGION_PSSM_CODE contains final region code to use and
#       year weights for model have been added.  New Labour Supply has been calculated and
#       age and age group have been added + a new student id
#     Refreshes survey records in T_Cohorts_Recoded
#
# TRD:
#     Assumes geocoding has been done, and CURRENT_REGION_PSSM_CODE contains final region code to use and
#       New Labour Supply has been calculated
#     Refreshes survey records in T_Cohorts_Recoded
#     Adds year weights for model
#     Adds Age and age groups + a new student id
#
# DACSO:
#     Assumes geocoding has been done, and CURRENT_REGION_PSSM_CODE contains final region code to use
#     Recodes institution codes to be consistent to STP file
#     Update CIPS after program matching.
#     Applies weight for model year and derives New Labour Supply
#     Adds age and age group, a new student id
#     Refresh survey records in T_Cohorts_Recoded
#
# BGS:
#     Assumes geocoding has been done, and CURRENT_REGION_PSSM_CODE contains final region code to use
#     Recodes institution codes to be consistent to STP file
#     Updates CIPS after program matching.
#     Applies weight for model year and derives New Labour Supply
#     Adds age and age group, a new student id
#     Refreshes survey records in T_Cohorts_Recoded
#
#     Notes: double check method for updating CIP codes after program matching.
#     There is a query to check for invalid NOC codes (see documentation).
#     Update T-Year_Survey_Year and T_weights (for all cohorts)
#     2006 dacso all NULL lcip-4-creds (remove 2006)
# 


library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(RJDBC)

# ---- Configure LAN and file paths ----
db_config <- config::get("decimal")
lan <- config::get("lan")
my_schema <- config::get("myschema")


# ---- Query Defs ----

# ---- Connection to decimal ----
db_config <- config::get("decimal")
decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")

# Load necessary libraries
library(DBI)
library(glue)
library(assertthat)

# List of required tables with categories
required_tables <- list(
  TRD = c("TRD_Graduates", "T_TRD_DATA"),
  APP = c("T_APPSO_DATA_Final", "APPSO_Graduates"),
  BGS = c("T_BGS_Data_Final", "T_BGS_INST_Recode", "T_bgs_data_final_for_outcomesmatching", "T_Weights"),
  DACSO = c("t_dacso_data_part_1_stepa", "infoware_c_outc_clean_short_resp"),
  Lookups = c("t_current_region_pssm_codes", "t_current_region_pssm_rollup_codes", 
              "t_current_region_pssm_rollup_codes_bc", "tbl_age", "tbl_age_groups", 
              "t_pssm_credential_grouping", "t_year_survey_year")
)

# Check for required data tables in the database
for (category in names(required_tables)) {
  for (table_name in required_tables[[category]]) {
    # Build SQL statement
    full_table_name <- SQL(glue::glue('"{my_schema}"."{table_name}"'))
    
    # Assert that the table exists in the database
    assert_that(
      dbExistsTable(decimal_con, full_table_name),
      msg = paste("Error:", table_name, "does not exist in schema", my_schema)
    )
  }
}


# ---- TRD Queries ----
# Applies weight for model year and derives New Labour Supply
if (regular_run == T | ptib_run == T){
  dbExecute(decimal_con, "ALTER TABLE t_TRD_data ADD Age_Group FLOAT NULL;")
  dbExecute(decimal_con, "ALTER TABLE t_TRD_data ADD Age_Group_Rollup FLOAT NULL;")

# [UPDATE] t_trd_data

Q000_TRD_Q003c_Derived_And_Weights <- "
UPDATE t_trd_data
SET    t_trd_data.new_labour_supply =
       CASE 
          WHEN TRD_LABR_EMPLOYED = 1 THEN 1 
          WHEN TRD_LABR_IN_LABOUR_MARKET = 1 And TRD_LABR_EMPLOYED = 0 THEN 1
          WHEN TRD_LABR_EMPLOYED = 0 THEN 0
          WHEN RESPONDENT = '1' THEN 0
          ELSE 0 
      END, 
      T_TRD_Data.Weight = T_Weights.Weight,
      T_TRD_Data.age_group = tbl_age.age_group,
      T_TRD_Data.age_group_rollup = tbl_age_groups.age_group_rollup
FROM ((t_trd_data
INNER JOIN t_weights
  ON t_trd_data.subm_cd = t_weights.subm_cd)
LEFT JOIN tbl_age
  ON t_trd_data.trd_age_at_survey = tbl_age.age)
LEFT JOIN tbl_age_groups
  ON tbl_age.age_group = tbl_age_groups.age_group
WHERE  t_weights.model = '2022-2023'
  AND t_weights.survey = 'TRD';"
  dbExecute(decimal_con, Q000_TRD_Q003c_Derived_And_Weights)
} 

if (qi_run == T ) {

# [UPDATE] t_trd_data

Q000_TRD_Q003c_Derived_And_Weights_QI <- "
UPDATE t_trd_data
SET    t_trd_data.new_labour_supply =
       CASE 
          WHEN TRD_LABR_EMPLOYED = 1 THEN 1 
          WHEN TRD_LABR_IN_LABOUR_MARKET = 1 And TRD_LABR_EMPLOYED = 0 THEN 1
          WHEN TRD_LABR_EMPLOYED = 0 THEN 0
          WHEN RESPONDENT = '1' THEN 0
          ELSE 0 
      END, 
      T_TRD_Data.Weight = T_Weights.Weight_QI,
      T_TRD_Data.age_group = tbl_age.age_group,
      T_TRD_Data.age_group_rollup = tbl_age_groups.age_group_rollup
FROM ((t_trd_data
INNER JOIN t_weights
  ON t_trd_data.subm_cd = t_weights.subm_cd)
LEFT JOIN tbl_age
  ON t_trd_data.trd_age_at_survey = tbl_age.age)
LEFT JOIN tbl_age_groups
  ON tbl_age.age_group = tbl_age_groups.age_group
WHERE  t_weights.model = '2022-2023'
  AND t_weights.survey = 'TRD';"
  dbExecute(decimal_con, Q000_TRD_Q003c_Derived_And_Weights_QI)
}


# Refresh trd survey records in T_Cohorts_Recoded

# [DELETE] T_Cohorts_Recoded

Q000_TRD_Q005_1b1_Delete_Cohort <- "
DELETE  T_Cohorts_Recoded
FROM T_Cohorts_Recoded
WHERE T_Cohorts_Recoded.Survey='TRD';"
dbExecute(decimal_con, Q000_TRD_Q005_1b1_Delete_Cohort)

# [INSERT INTO] t_cohorts_recoded

Q000_TRD_Q005_DACSO_DATA_Part_1b2_Cohort_Recoded <- "
INSERT INTO t_cohorts_recoded
            (pen,
             stqu_id,
             survey,
             survey_year,
             inst_cd,
             lcip_cd,
             lcp4_cd,
             ttrain,
             noc_cd,
             age_at_survey,
             age_group,
             age_group_rollup,
             grad_status,
             respondent,
             new_labour_supply,
             weight,
             pssm_credential,
             pssm_cred,
             lcip4_cred,
             lcip2_cred,
             current_region_pssm_code)
SELECT t_trd_data.pen,
       'TRD - ' + cast([key] as nvarchar(255)) AS stqu_id,
       t_year_survey_year.survey,
       t_year_survey_year.survey_year,
       t_trd_data.inst,
       t_trd_data.lcip_cd,
       t_trd_data.lcip_lcp4_cd,
       CASE WHEN ttrain = 2 THEN 1 ELSE ttrain END AS ttrain,
       CASE WHEN t_trd_data.noc_cd = 'XXXXX' THEN '99999' ELSE t_trd_data.noc_cd END AS NOC_CD,
       t_trd_data.trd_age_at_survey,
       tbl_age_groups.age_group,
       tbl_age_groups.age_group_rollup,
       t_trd_data.gradstat_group,
       t_trd_data.respondent,
       t_trd_data.new_labour_supply,
       t_trd_data.weight,
       t_trd_data.pssm_credential,
       gradstat_group + ' - ' + pssm_credential  AS PSSM_CRED,
       gradstat_group + ' - ' + lcip_lcp4_cd + ' - ' +
       CASE WHEN ttrain = 2 THEN '1' ELSE cast(ttrain as nvarchar(10)) END + ' - ' + pssm_credential AS lcip4_cred,
       gradstat_group + ' - ' + LEFT(lcip_lcp4_cd, 2) + ' - ' +
       CASE WHEN ttrain = 2 THEN '1' ELSE cast(ttrain as nvarchar(10)) END + ' - ' + pssm_credential AS lcip2_cred,
       t_trd_data.current_region_pssm_code
FROM   t_trd_data
INNER JOIN t_year_survey_year
  ON t_trd_data.subm_cd = t_year_survey_year.subm_cd
  LEFT JOIN (tbl_Age  INNER JOIN tbl_Age_Groups  ON tbl_Age.Age_Group = tbl_Age_Groups.Age_Group)
	        ON tbl_Age.Age =  t_trd_data.TRD_AGE_AT_SURVEY
WHERE  t_year_survey_year.survey = 'TRD';"
dbExecute(decimal_con, Q000_TRD_Q005_DACSO_DATA_Part_1b2_Cohort_Recoded)

# ---- APP Queries ----
# Refresh survey records in T_Cohorts_Recoded

# [DELETE] T_Cohorts_Recoded


APPSO_Q005_1b1_Delete_Cohort <- 
"DELETE T_Cohorts_Recoded
FROM T_Cohorts_Recoded
WHERE T_Cohorts_Recoded.Survey = 'APPSO';"
dbExecute(decimal_con, APPSO_Q005_1b1_Delete_Cohort)

# [INSERT INTO] t_cohorts_recoded

APPSO_Q005_DACSO_DATA_Part_1b2_Cohort_Recoded <- 
"INSERT INTO t_cohorts_recoded
            (pen,
             stqu_id,
             survey,
             survey_year,
             inst_cd,
             lcip_cd,
             lcp4_cd,
             noc_cd,
             age_at_survey,
             age_group,
             age_group_rollup,
             grad_status,
             respondent,
             new_labour_supply,
             weight,
             pssm_credential,
             pssm_cred,
             lcip4_cred,
             lcip2_cred,
             current_region_pssm_code)
SELECT t_appso_data_final.pen,
       'APPSO - ' + cast(cast([key] AS INTEGER) AS NVARCHAR(255)) AS stqu_id,
       t_year_survey_year.survey,
       t_year_survey_year.survey_year,
       t_appso_data_final.inst,
       t_appso_data_final.lcip_cd,
       t_appso_data_final.lcip_lcp4_cd as lcp4_cd,
       CASE WHEN t_appso_data_final.noc_cd = 'xxxxx' THEN '99999' ELSE t_appso_data_final.noc_cd END AS NOC_CD,
       t_appso_data_final.app_age_at_survey,
       tbl_Age_Groups.age_group,
       tbl_Age_Groups.age_group_rollup,
       '1' AS grad_status,
       t_appso_data_final.respondent,
       t_appso_data_final.new_labour_supply,
       t_appso_data_final.weight,
       t_appso_data_final.pssm_credential,
       t_appso_data_final.pssm_credential,
       t_appso_data_final.lcip4_cred,
       LEFT(lcip_lcp4_cd, 2) + ' - ' + pssm_credential AS LCIP2_CRED,
       t_appso_data_final.current_region_pssm_code
FROM   t_appso_data_final
       INNER JOIN t_year_survey_year
               ON t_appso_data_final.subm_cd = t_year_survey_year.subm_cd
       LEFT JOIN (tbl_Age  INNER JOIN tbl_Age_Groups  ON tbl_Age.Age_Group = tbl_Age_Groups.Age_Group)
	        ON tbl_Age.Age =  t_appso_data_final.APP_AGE_AT_SURVEY
WHERE  t_year_survey_year.survey = 'appso';"
dbExecute(decimal_con, APPSO_Q005_DACSO_DATA_Part_1b2_Cohort_Recoded)

# ---- BGS Queries ----
# Recode institution codes to be consistent to STP file

# [UPDATE] t_bgs_data_final


BGS_Q001b_INST_Recode <- "
UPDATE t_bgs_data_final
SET    t_bgs_data_final.inst = t_bgs_inst_recode.inst_recode
FROM    t_bgs_data_final
INNER JOIN t_bgs_inst_recode
  ON t_bgs_data_final.inst = t_bgs_inst_recode.inst;"
dbExecute(decimal_con, BGS_Q001b_INST_Recode)

# Note: update CIPS after program matching. 

# [UPDATE] t_bgs_data_final

BGS_Q001c_Update_CIPs_After_Program_Matching <- "
UPDATE t_bgs_data_final
SET    t_bgs_data_final.cip_code_4 = t_bgs_data_final_for_outcomesmatching.final_cip_code_4,
       t_bgs_data_final.cip_code_2 = t_bgs_data_final_for_outcomesmatching.final_cip_code_2,
       t_bgs_data_final.lcip_lcippc_cd = t_bgs_data_final_for_outcomesmatching.final_cip_cluster_code
FROM   t_bgs_data_final
INNER JOIN t_bgs_data_final_for_outcomesmatching
    ON t_bgs_data_final.stqu_id = t_bgs_data_final_for_outcomesmatching.stqu_id;"
dbExecute(decimal_con, BGS_Q001c_Update_CIPs_After_Program_Matching)

# [UPDATE] t_bgs_data_final

BGS_Q002_LCP4_CRED <- "
UPDATE t_bgs_data_final
SET    t_bgs_data_final.lcip4_cred = cip_code_4 + ' - ' + 'BACH',
       t_bgs_data_final.pssm_credential = 'BACH';"
dbExecute(decimal_con, BGS_Q002_LCP4_CRED)

# Applies weight for model year and derives New Labour Supply
if (regular_run == T | ptib_run == T){
  dbExecute(decimal_con, "ALTER TABLE T_BGS_Data_Final ADD BGS_New_Labour_Supply FLOAT NULL;")

# [UPDATE] t_bgs_data_final
  
BGS_Q003c_Derived_And_Weights <- "
UPDATE t_bgs_data_final
SET    t_bgs_data_final.BGS_New_Labour_Supply =  
        CASE
	        	WHEN CURRENT_ACTIVITY = 1 THEN 1
	        	WHEN CURRENT_ACTIVITY = 4 And FULL_TM_WRK = 1 THEN 1
	        	WHEN CURRENT_ACTIVITY = 4 And FULL_TM_WRK = 0 THEN 2
	        	WHEN CURRENT_ACTIVITY = 3 And IN_LBR_FRC = 1 THEN 1 
	        	WHEN CURRENT_ACTIVITY IS NULL  And  FULL_TM_WRK IS NULL And IN_LBR_FRC = 1 THEN 1 
	        	WHEN CURRENT_ACTIVITY IS NULL And IN_LBR_FRC = 1 THEN 1
	        	WHEN srv_y_n = 0 THEN 0
	        	ELSE 0 
	      END,
       t_bgs_data_final.weight = t_weights.weight,
       t_bgs_data_final.age_group = tbl_age.age_group,
       t_bgs_data_final.age_group_rollup = tbl_age_groups.age_group_rollup
FROM ((t_bgs_data_final
INNER JOIN t_weights
  ON t_bgs_data_final.survey_year = t_weights.survey_year)
LEFT JOIN tbl_age
  ON t_bgs_data_final.age = tbl_age.age)
LEFT JOIN tbl_age_groups
  ON tbl_age.age_group = tbl_age_groups.age_group
WHERE  t_weights.model = '2022-2023'
AND    t_weights.survey = 'BGS';"
  dbExecute(decimal_con, BGS_Q003c_Derived_And_Weights)
}  
if (qi_run == T ) {

# [UPDATE] t_bgs_data_final

BGS_Q003c_Derived_And_Weights_QI <- "
UPDATE t_bgs_data_final
SET    t_bgs_data_final.BGS_New_Labour_Supply =  
        CASE
	        	WHEN CURRENT_ACTIVITY = 1 THEN 1
	        	WHEN CURRENT_ACTIVITY = 4 And FULL_TM_WRK = 1 THEN 1
	        	WHEN CURRENT_ACTIVITY = 4 And FULL_TM_WRK = 0 THEN 2
	        	WHEN CURRENT_ACTIVITY = 3 And IN_LBR_FRC = 1 THEN 1 
	        	WHEN CURRENT_ACTIVITY IS NULL  And  FULL_TM_WRK IS NULL And IN_LBR_FRC = 1 THEN 1 
	        	WHEN CURRENT_ACTIVITY IS NULL And IN_LBR_FRC = 1 THEN 1
	        	WHEN srv_y_n = 0 THEN 0
	        	ELSE 0 
	      END,
       t_bgs_data_final.weight = t_weights.weight_QI,
       t_bgs_data_final.age_group = tbl_age.age_group,
       t_bgs_data_final.age_group_rollup = tbl_age_groups.age_group_rollup
FROM ((t_bgs_data_final
INNER JOIN t_weights
  ON t_bgs_data_final.survey_year = t_weights.survey_year)
LEFT JOIN tbl_age
  ON t_bgs_data_final.age = tbl_age.age)
LEFT JOIN tbl_age_groups
  ON tbl_age.age_group = tbl_age_groups.age_group
WHERE  t_weights.model = '2022-2023'
AND    t_weights.survey = 'BGS';"
  dbExecute(decimal_con, BGS_Q003c_Derived_And_Weights_QI)
}

# Refresh bgs survey records in T_Cohorts_Recoded

# [DELETE] T_Cohorts_Recoded


BGS_Q005_1b1_Delete_Cohort <- "
DELETE T_Cohorts_Recoded
FROM T_Cohorts_Recoded
WHERE (((T_Cohorts_Recoded.Survey)='BGS'));"
dbExecute(decimal_con, BGS_Q005_1b1_Delete_Cohort)

# [INSERT INTO] t_cohorts_recoded

BGS_Q005_1b2_Cohort_Recoded <- "INSERT INTO t_cohorts_recoded
            (pen,
             stqu_id,
             survey,
             survey_year,
             inst_cd,
             lcp4_cd,
             noc_cd,
             age_at_survey,
             age_group,
             age_group_rollup,
             grad_status,
             respondent,
             new_labour_supply,
             old_labour_supply,
             weight,
             pssm_credential,
             pssm_cred,
             lcip4_cred,
             lcip2_cred,
             current_region_pssm_code)
SELECT t_bgs_data_final.pen,
       'BGS - ' + cast(cast(stqu_id as integer) as nvarchar(20)) AS stqu_id,
       'BGS' AS survey,
       t_bgs_data_final.survey_year,
       t_bgs_data_final.inst,
       t_bgs_data_final.cip_code_4,
       CASE 
          WHEN t_bgs_data_final.noc = 'XXXXX' THEN '99999' 
			    ELSE t_bgs_data_final.noc
	     END AS NOC_CD,
       t_bgs_data_final.age,
       t_bgs_data_final.age_group,
       t_bgs_data_final.age_group_rollup,
       '1' AS grad_status,
       t_bgs_data_final.srv_y_n,
       t_bgs_data_final.bgs_new_labour_supply,
       t_bgs_data_final.old_labour_supply,
       t_bgs_data_final.weight,
       t_bgs_data_final.pssm_credential,
       t_bgs_data_final.pssm_credential,
       t_bgs_data_final.lcip4_cred,
       LEFT(cip_code_4, 2) + ' - ' + 'BACH' AS lcip2_cred,
       t_bgs_data_final.current_region_pssm_code
FROM   t_bgs_data_final; "
dbExecute(decimal_con, BGS_Q005_1b2_Cohort_Recoded)

# ----DACSO Queries ----
# adds age, updates credential, creates new LCIP4_CRED variable 

# [SELECT INTO] Create t_dacso_data_part_1
# ---- Q003 - Q005 Pull DACSO Records ----
DACSO_Q003_DACSO_Data_Part_1_stepB <- 
  "SELECT t_dacso_data_part_1_stepa.coci_pen,
       t_dacso_data_part_1_stepa.coci_stqu_id,
       t_dacso_data_part_1_stepa.coci_subm_cd,
       t_dacso_data_part_1_stepa.coci_lrst_cd,
       t_dacso_data_part_1_stepa.coci_inst_cd,
       t_dacso_data_part_1_stepa.pfst_current_activity,
       t_dacso_data_part_1_stepa.lcip_lcippc_name,
       t_dacso_data_part_1_stepa.lcip_cd,
       t_dacso_data_part_1_stepa.lcp4_cd,
       t_dacso_data_part_1_stepa.current_region_pssm_code, 
       t_dacso_data_part_1_stepa.lcp4_cip_4digits_name,
       t_dacso_data_part_1_stepa.ttrain,
       t_dacso_data_part_1_stepa.tpid_lgnd_cd,
       t_dacso_data_part_1_stepa.labr_in_labour_market,
       t_dacso_data_part_1_stepa.labr_employed,
       t_dacso_data_part_1_stepa.labr_unemployed,
       t_dacso_data_part_1_stepa.labr_employed_full_part_time,
       t_dacso_data_part_1_stepa.labr_job_search_time_gp,
       t_dacso_data_part_1_stepa.labr_job_training_related,
       t_dacso_data_part_1_stepa.labr_occupation_lnoc_cd,
       t_dacso_data_part_1_stepa.coci_age_at_survey,
       tbl_age.age_group,
       tbl_age_groups.age_group_rollup,
       t_dacso_data_part_1_stepa.cosc_grad_status_lgds_cd_group,
       t_dacso_data_part_1_stepa.respondent,
       t_dacso_data_part_1_stepa.new_labour_supply,
       t_dacso_data_part_1_stepa.old_labour_supply,
       t_dacso_data_part_1_stepa.weight,
       t_dacso_data_part_1_stepa.had_previous_credential,
       t_dacso_data_part_1_stepa.pfst_in_post_sec_before,
       t_dacso_data_part_1_stepa.pfst_had_previous_cdtl,
       t_dacso_data_part_1_stepa.pfst_furstdy_incl_still_attd,
       t_pssm_credential_grouping.prgm_credential_awarded,
       t_pssm_credential_grouping.prgm_credential_awarded_name,
       t_pssm_credential_grouping.pssm_credential,
       t_pssm_credential_grouping.pssm_credential_name,
       cast(cosc_grad_status_lgds_cd_group as varchar(20)) + ' - ' + lcp4_cd + ' - ' +  
        CASE WHEN cast(ttrain as varchar(10)) = '2' THEN '1' ELSE cast(ttrain as varchar(10)) END + ' - ' + pssm_credential 
        AS LCIP4_CRED
INTO   t_dacso_data_part_1
FROM   ((t_dacso_data_part_1_stepa
         INNER JOIN t_pssm_credential_grouping
                 ON t_dacso_data_part_1_stepa.prgm_credential = t_pssm_credential_grouping.prgm_credential_awarded)
        LEFT JOIN tbl_age
               ON t_dacso_data_part_1_stepa.coci_age_at_survey = tbl_age.age)
       LEFT JOIN tbl_age_groups
              ON tbl_age.age_group = tbl_age_groups.age_group;"
dbExecute(decimal_con, DACSO_Q003_DACSO_Data_Part_1_stepB) 

# Recode institution codes for CIP-NOC work

# [UPDATE] t_dacso_data_part_1

# in 2023 I removed the inner join on c_out_c_clean2 as it seemed to do nothing. 
DACSO_Q003b_DACSO_DATA_Part_1_Further_Ed <- "
UPDATE t_dacso_data_part_1
SET    t_dacso_data_part_1.had_previous_credential =
          CASE WHEN infoware_c_outc_clean_short_resp.q08 = '1' 
          THEN infoware_c_outc_clean_short_resp.pfst_had_previous_cdtl 
          ELSE infoware_c_outc_clean_short_resp.q08 END,
       t_dacso_data_part_1.pfst_in_post_sec_before = infoware_c_outc_clean_short_resp.q08,
       t_dacso_data_part_1.pfst_had_previous_cdtl = infoware_c_outc_clean_short_resp.pfst_had_previous_cdtl,
       t_dacso_data_part_1.pfst_furstdy_incl_still_attd = infoware_c_outc_clean_short_resp.pfst_furstdy_incl_still_attd
FROM   t_dacso_data_part_1
INNER JOIN infoware_c_outc_clean_short_resp  
  ON  infoware_c_outc_clean_short_resp.stqu_id = t_dacso_data_part_1.coci_stqu_id;"
dbExecute(decimal_con, DACSO_Q003b_DACSO_DATA_Part_1_Further_Ed)

# Deletes other, none, invalid etc. credentials that are not part of the PSSM

# [DELETE] t_dacso_data_part_1

DACSO_Q004_DACSO_DATA_Part_1_Delete_Credentials <- "
DELETE t_dacso_data_part_1
FROM       t_dacso_data_part_1
INNER JOIN t_pssm_credential_grouping
ON         t_dacso_data_part_1.prgm_credential_awarded = t_pssm_credential_grouping.prgm_credential_awarded
WHERE      t_pssm_credential_grouping.dacso_include_in_model IS NULL;"
dbExecute(decimal_con, DACSO_Q004_DACSO_DATA_Part_1_Delete_Credentials)

# Recodes all the old institution codes to the current code so that weight adjustments across years by program can be applied.
# This step skipped as not needed, but could add as a check at some point.
# dbExecute(decimal_con, DACSO_Q004b_INST_Recode)

# Applies weight for model year and derives New Labour Supply - re-run if changing model years or grouping geographies
if (regular_run == T | ptib_run == T){

# [UPDATE] t_dacso_data_part_1

DACSO_Q005_DACSO_DATA_Part_1a_Derived <- "
UPDATE t_dacso_data_part_1
SET    t_dacso_data_part_1.new_labour_supply = 
       CASE 
	     		WHEN PFST_CURRENT_ACTIVITY = 3 THEN 1
	     		WHEN PFST_CURRENT_ACTIVITY = 2 And LABR_EMPLOYED_FULL_PART_TIME = 1 THEN 1
	     		WHEN PFST_CURRENT_ACTIVITY = 2 And LABR_EMPLOYED_FULL_PART_TIME = 0 THEN 2
	     		WHEN PFST_CURRENT_ACTIVITY = 4 And LABR_IN_LABOUR_MARKET = 1 THEN 1
	     		WHEN RESPONDENT = '1' THEN 0
	        ELSE 0 
	     END,
       t_dacso_data_part_1.weight = t_weights.weight
FROM t_dacso_data_part_1
INNER JOIN t_weights
  ON t_dacso_data_part_1.coci_subm_cd = t_weights.subm_cd
WHERE  t_weights.model = '2022-2023'
AND    t_weights.survey = 'DACSO';"
  dbExecute(decimal_con, DACSO_Q005_DACSO_DATA_Part_1a_Derived)
}  

if (qi_run == T ) {

# [UPDATE] t_dacso_data_part_1

DACSO_Q005_DACSO_DATA_Part_1a_Derived_QI <- "
UPDATE t_dacso_data_part_1
SET    t_dacso_data_part_1.new_labour_supply = 
       CASE 
	     		WHEN PFST_CURRENT_ACTIVITY = 3 THEN 1
	     		WHEN PFST_CURRENT_ACTIVITY = 2 And LABR_EMPLOYED_FULL_PART_TIME = 1 THEN 1
	     		WHEN PFST_CURRENT_ACTIVITY = 2 And LABR_EMPLOYED_FULL_PART_TIME = 0 THEN 2
	     		WHEN PFST_CURRENT_ACTIVITY = 4 And LABR_IN_LABOUR_MARKET = 1 THEN 1
	     		WHEN RESPONDENT = '1' THEN 0
	        ELSE 0 
	     END,
       t_dacso_data_part_1.weight = t_weights.weight_QI
FROM t_dacso_data_part_1
INNER JOIN t_weights
  ON t_dacso_data_part_1.coci_subm_cd = t_weights.subm_cd
WHERE  t_weights.model = '2022-2023'
AND    t_weights.survey = 'DACSO';"
  dbExecute(decimal_con, DACSO_Q005_DACSO_DATA_Part_1a_Derived_QI)
}

# Refresh dacso survey records in T_Cohorts_Recoded

# [DELETE] t_cohorts_recoded


DACSO_Q005_DACSO_DATA_Part_1b1_Delete_Cohort <- "
DELETE t_cohorts_recoded
FROM   t_cohorts_recoded
WHERE  t_cohorts_recoded.survey = 'DACSO'; "
dbExecute(decimal_con, DACSO_Q005_DACSO_DATA_Part_1b1_Delete_Cohort)

# [INSERT INTO] t_cohorts_recoded

DACSO_Q005_DACSO_DATA_Part_1b2_Cohort_Recoded <- "
INSERT 
INTO t_cohorts_recoded
    (pen,stqu_id,survey,survey_year,inst_cd,lcp4_cd,ttrain,noc_cd,age_at_survey,age_group,age_group_rollup,grad_status,respondent,new_labour_supply,
     old_labour_supply,weight,pssm_credential,pssm_cred,lcip4_cred,lcip2_cred,current_region_pssm_code)
SELECT t_dacso_data_part_1.coci_pen AS pen,
       'DACSO - ' + CAST(coci_stqu_id AS NVARCHAR(100)) AS stqu_id,
       t_year_survey_year.survey,
       t_year_survey_year.survey_year,
       t_dacso_data_part_1.coci_inst_cd,
       t_dacso_data_part_1.lcp4_cd,
       CASE WHEN ttrain = 2 THEN 1 ELSE ttrain END AS TTRAIN,
       CASE WHEN t_dacso_data_part_1.labr_occupation_lnoc_cd = 'XXXXX' THEN '99999' 
			ELSE t_dacso_data_part_1.labr_occupation_lnoc_cd END AS NOC_CD,
       t_dacso_data_part_1.coci_age_at_survey,
       t_dacso_data_part_1.age_group,
       t_dacso_data_part_1.age_group_rollup,
       t_dacso_data_part_1.cosc_grad_status_lgds_cd_group as grad_status,
       t_dacso_data_part_1.respondent,
       t_dacso_data_part_1.new_labour_supply,
       t_dacso_data_part_1.old_labour_supply,
       t_dacso_data_part_1.weight,
       t_dacso_data_part_1.pssm_credential,
       cast(cosc_grad_status_lgds_cd_group as nvarchar(10)) + ' - ' + pssm_credential AS PSSM_CRED,
       cast(cosc_grad_status_lgds_cd_group as nvarchar(10)) + ' - ' + lcp4_cd + ' - ' + 
			CASE WHEN ttrain = 2 THEN '1' ELSE cast(ttrain as nvarchar(10)) END + ' - ' + pssm_credential AS LCIP4_CRED,
       cast(cosc_grad_status_lgds_cd_group as nvarchar(10)) + ' - ' + LEFT(lcp4_cd, 2) + ' - ' + 
            CASE WHEN ttrain = 2 THEN '1' ELSE cast(ttrain as nvarchar(10)) END + ' - ' + pssm_credential AS LCIP2_CRED,
       t_dacso_data_part_1.current_region_pssm_code
FROM   t_year_survey_year
INNER JOIN t_dacso_data_part_1
  ON t_year_survey_year.subm_cd = t_dacso_data_part_1.coci_subm_cd
WHERE  t_year_survey_year.survey = 'DACSO';"
dbExecute(decimal_con, DACSO_Q005_DACSO_DATA_Part_1b2_Cohort_Recoded)

# ---- Keep  ----
dbExistsTable(decimal_con, "APPSO_Graduates")
dbExistsTable(decimal_con, "TRD_Graduates")
dbExistsTable(decimal_con, "t_dacso_data_part_1")
dbExistsTable(decimal_con, "T_Cohorts_Recoded")

# ---- Clean Up Lookups (if desired, not a needed step) ----
# dbExecute(decimal_con, "DROP TABLE T_BGS_INST_Recode;")
# dbExecute(decimal_con, "DROP TABLE T_PSSM_Credential_Grouping")
# dbExecute(decimal_con, "DROP TABLE t_year_survey_year")
# dbExecute(decimal_con, "DROP TABLE t_current_region_pssm_codes")
# dbExecute(decimal_con, "DROP TABLE t_current_region_pssm_rollup_codes")
# dbExecute(decimal_con, "DROP TABLE t_current_region_pssm_rollup_codes_bc")

dbDisconnect(decimal_con)
# rm(list=ls())

