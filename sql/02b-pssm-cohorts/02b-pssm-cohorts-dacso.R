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

DACSO_Q004_DACSO_DATA_Part_1_Delete_Credentials <- "
DELETE t_dacso_data_part_1
FROM       t_dacso_data_part_1
INNER JOIN t_pssm_credential_grouping
ON         t_dacso_data_part_1.prgm_credential_awarded = t_pssm_credential_grouping.prgm_credential_awarded
WHERE      t_pssm_credential_grouping.dacso_include_in_model IS NULL;"

DACSO_Q004b_DACSO_DATA_Part_1_Add_CURRENT_REGION_PSSM <- "
ALTER TABLE T_DACSO_DATA_Part_1
ADD CURRENT_REGION_PSSM_CODE INT NULL;"

DACSO_Q004b_DACSO_DATA_Part_1_Add_CURRENT_REGION_PSSM2 <- "
UPDATE t_dacso_data_part_1
SET    t_dacso_data_part_1.current_region_pssm_code = dacso_current_region_data.current_region_pssm_code
FROM t_dacso_data_part_1
INNER JOIN dacso_current_region_data
  ON t_dacso_data_part_1.coci_stqu_id = dacso_current_region_data.coci_stqu_id;"

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


DACSO_Q005_DACSO_DATA_Part_1b1_Delete_Cohort <- "
DELETE t_cohorts_recoded
FROM   t_cohorts_recoded
WHERE  t_cohorts_recoded.survey = 'DACSO'; "

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
       CASE WHEN t_dacso_data_part_1.labr_occupation_lnoc_cd = 'XXXX' THEN '9999' 
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


DACSO_Q005_DACSO_DATA_Part_1b3_Check_Weights <- "
SELECT t_cohorts_recoded.survey,
       t_cohorts_recoded.survey_year,
       t_cohorts_recoded.weight
FROM   t_cohorts_recoded
GROUP  BY t_cohorts_recoded.survey,
          t_cohorts_recoded.survey_year,
          t_cohorts_recoded.weight;"