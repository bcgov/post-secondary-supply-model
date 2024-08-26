BGS_Q00_Checking_Age <- "
SELECT infoware_bgs_cohort_info.brthmnth,
infoware_bgs_cohort_info.brthyear,
infoware_bgs_cohort_info.subm_cd,
Datevalue([brthdate_string])                       AS Birth_Date,
[infoware_bgs_cohort_info].[brthyear] & '-' &
  [infoware_bgs_cohort_info].[brthmnth] & '-' & '15' AS BRTHDATE_STRING,
'20' & RIGHT([subm_cd], 2) & '-12-01'              AS Survey_Date,
infoware_bgs_cohort_info.subm_cd, Datediff('yyyy', [Birth_Date],
                                           [Survey_Date])+([Survey_Date]<Dateserial(Year([Survey_Date]), Month([Birth_Date]
                                           ), Day([Birth_Date]))) AS expr1, infoware_bgs_dist.age
FROM   infoware_bgs_cohort_info
INNER JOIN infoware_bgs_dist
ON infoware_bgs_cohort_info.stqu_id = infoware_bgs_dist.stqu_id
WHERE  (((infoware_bgs_cohort_info.subm_cd)='C_Outc16' OR (
  infoware_bgs_cohort_info.subm_cd)='C_Outc17'));"

BGS_Q001b_INST_Recode <- "
UPDATE t_bgs_data_final
SET    t_bgs_data_final.inst = t_bgs_inst_recode.inst_recode
FROM    t_bgs_data_final
INNER JOIN t_bgs_inst_recode
  ON t_bgs_data_final.inst = t_bgs_inst_recode.inst;"

BGS_Q001c_Update_CIPs_After_Program_Matching <- "
UPDATE t_bgs_data_final
SET    t_bgs_data_final.cip_code_4 = t_bgs_data_final_for_outcomesmatching2020.final_cip_code_4,
       t_bgs_data_final.cip_code_2 = t_bgs_data_final_for_outcomesmatching2020.final_cip_code_2,
       t_bgs_data_final.lcip_lcippc_cd = t_bgs_data_final_for_outcomesmatching2020.final_cip_cluster_code
FROM   t_bgs_data_final
INNER JOIN t_bgs_data_final_for_outcomesmatching2020
    ON t_bgs_data_final.stqu_id = t_bgs_data_final_for_outcomesmatching2020.stqu_id;"

BGS_Q002_LCP4_CRED <- "
UPDATE t_bgs_data_final
SET    t_bgs_data_final.lcip4_cred = cip_code_4 + ' - ' + 'BACH',
       t_bgs_data_final.pssm_credential = 'BACH';"

BGS_Q003b_Add_CURRENT_REGION_PSSM <- "
ALTER TABLE T_BGS_DATA_Final
ADD CURRENT_REGION_PSSM_CODE INT NULL;"

BGS_Q003b_Add_CURRENT_REGION_PSSM2 <- "
UPDATE      t_bgs_data_final
SET         t_bgs_data_final.current_region_pssm_code =  bgs_current_region_data.current_region_pssm_code
FROM        t_bgs_data_final
INNER JOIN  bgs_current_region_data
    ON      t_bgs_data_final.stqu_id = bgs_current_region_data.stqu_id;"
  
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
WHERE  t_weights.model = '2019-2020'
AND    t_weights.survey = 'BGS';"

BGS_Q005_1b1_Delete_Cohort <- "
DELETE T_Cohorts_Recoded
FROM T_Cohorts_Recoded
WHERE (((T_Cohorts_Recoded.Survey)='BGS'));"

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
          WHEN t_bgs_data_final.noc = 'XXXX' THEN '9999' 
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


BGS_Q99A_ENDDT_IMPUTED <- "
UPDATE t_cohorts_recoded
SET    t_cohorts_recoded.enddt = ( [survey_year] - 2 ) & '-06'
WHERE  ( ( ( t_cohorts_recoded.enddt ) IS NULL )
         AND ( ( t_cohorts_recoded.survey ) = 'BGS' ) ); "

