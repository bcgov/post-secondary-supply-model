SELECT 'BACH' AS PSSM_Credential,
       bgs_cohort_info.pen,
       bgs_cohort_info.studid,
       bgs_cohort_info.stqu_id,
       bgs_cohort_info.srv_y_n,
       bgs_cohort_info.subm_cd,
       bgs_dist_21_25.year AS SURVEY_YEAR,
       bgs_cohort_info.inst,
       bgs_cohort_info.program,
       bgs_cohort_info.cip_4digit_no_period AS CIP_CODE_4,
       bgs_cohort_info.cip2dig AS CIP_CODE_2,
       0 AS LCIP_LCIPPC_CD,
       bgs_cohort_info.cip_4digit_no_period || ' - ' || 'BACH' AS LCIP4_CRED,
       CASE 
       		WHEN lbr_frc_currently_employed = 1 AND pfst_currently_studying = 0 THEN 1 
       		WHEN lbr_frc_currently_employed = 0 AND pfst_currently_studying = 1 THEN 2
       		WHEN lbr_frc_currently_employed = 0 AND pfst_currently_studying = 0 THEN 3
       		WHEN lbr_frc_currently_employed = 1 AND pfst_currently_studying = 1 THEN 4
       END AS CURRENT_ACTIVITY,
       bgs_dist_21_25.full_tm,
       bgs_dist_21_25.lbr_frc_labour_market,
       bgs_dist_21_25.lbr_frc_currently_employed,
       bgs_dist_21_25.lbr_frc_unemployed,
       bgs_dist_21_25.e10_in_training_related_job,
       bgs_dist_21_25.noc_2021 as noc,
       --bgs_dist_21_25.noc,
       bgs_dist_21_25.PFST_CURRENTLY_STUDYING,
       bgs_dist_21_25.age,
       bgs_dist_21_25.region_cd, 
       bgs_dist_21_25.cur_res,
       bgs_dist_21_25.current_region,
       0 AS Age_Group,
       0 AS Age_Group_Rollup,
       0 AS New_Labour_Supply,
       0 AS Weight,
       0 AS Weight_CIP,
       bgs_dist_21_25.d01_r1
FROM   bgs_cohort_info
       INNER JOIN bgs_dist_21_25
               ON bgs_cohort_info.stqu_id = bgs_dist_21_25.stqu_id
WHERE  (bgs_cohort_info.srv_y_n = 0 OR bgs_cohort_info.srv_y_n = 1)