BGS_Q001_BGS_Data_2015_2019 <- "
SELECT 'BACH' AS PSSM_Credential,
       bgs_cohort_info.pen,
       bgs_cohort_info.studid,
       bgs_cohort_info.stqu_id,
       bgs_cohort_info.srv_y_n,
       --'20' || RIGHT(bgs_cohort_info.subm_cd, 2) AS survey_year,
       bgs_cohort_info.subm_cd,
       bgs_dist_15_19.year AS SURVEY_YEAR,
       bgs_cohort_info.inst,
       bgs_cohort_info.program,
       bgs_cohort_info.cip_4digit_no_period AS CIP_CODE_4,
       bgs_cohort_info.cip2dig AS CIP_CODE_2,
       0 AS LCIP_LCIPPC_CD,
       bgs_cohort_info.cip_4digit_no_period || ' - ' || 'BACH' AS LCIP4_CRED,
       CASE 
       		WHEN lbr_frc_currently_employed = 1 AND d02_r1_currently_studying = 0 THEN 1 
       		WHEN lbr_frc_currently_employed = 0 AND d02_r1_currently_studying = 1 THEN 2
       		WHEN lbr_frc_currently_employed = 0 AND d02_r1_currently_studying = 0 THEN 3
       		WHEN lbr_frc_currently_employed = 1 AND d02_r1_currently_studying = 1 THEN 4
       END AS CURRENT_ACTIVITY,
       bgs_dist_15_19.full_tm,
       bgs_dist_15_19.d03_studying_ft,
       bgs_dist_15_19.lbr_frc_labour_market,
       bgs_dist_15_19.d02_r1_currently_studying,
       bgs_dist_15_19.lbr_frc_currently_employed,
       bgs_dist_15_19.lbr_frc_unemployed,
       bgs_dist_15_19.e10_in_training_related_job,
       bgs_dist_15_19.noc,
       bgs_dist_15_19.age,
       bgs_dist_15_19.region_cd, 
       bgs_dist_15_19.cur_res,
       bgs_dist_15_19.current_region,
       0 AS Age_Group,
       0 AS Age_Group_Rollup,
       0 AS New_Labour_Supply,
       0 AS Weight,
       0 AS Weight_CIP,
       bgs_dist_15_19.d01_r1
FROM   bgs_cohort_info
       INNER JOIN bgs_dist_15_19
               ON bgs_cohort_info.stqu_id = bgs_dist_15_19.stqu_id
WHERE  (bgs_cohort_info.srv_y_n = 0 OR bgs_cohort_info.srv_y_n = 1)"

BGS_Q001_BGS_Data_2019_2023 <- "
SELECT 'BACH' AS PSSM_Credential,
       bgs_cohort_info.pen,
       bgs_cohort_info.studid,
       bgs_cohort_info.stqu_id,
       bgs_cohort_info.srv_y_n,
       bgs_cohort_info.subm_cd,
       bgs_dist_19_23.year AS SURVEY_YEAR,
       bgs_cohort_info.inst,
       bgs_cohort_info.program,
       bgs_cohort_info.cip_4digit_no_period AS CIP_CODE_4,
       bgs_cohort_info.cip2dig AS CIP_CODE_2,
       0 AS LCIP_LCIPPC_CD,
       bgs_cohort_info.cip_4digit_no_period || ' - ' || 'BACH' AS LCIP4_CRED,
       CASE 
       		WHEN lbr_frc_currently_employed = 1 AND d02_r1_currently_studying = 0 THEN 1 
       		WHEN lbr_frc_currently_employed = 0 AND d02_r1_currently_studying = 1 THEN 2
       		WHEN lbr_frc_currently_employed = 0 AND d02_r1_currently_studying = 0 THEN 3
       		WHEN lbr_frc_currently_employed = 1 AND d02_r1_currently_studying = 1 THEN 4
       END AS CURRENT_ACTIVITY,
       bgs_dist_19_23.full_tm,
       bgs_dist_19_23.d03_studying_ft,
       bgs_dist_19_23.lbr_frc_labour_market,
       bgs_dist_19_23.lbr_frc_currently_employed,
       bgs_dist_19_23.lbr_frc_unemployed,
       bgs_dist_19_23.e10_in_training_related_job,
       bgs_dist_19_23.noc,
       bgs_dist_19_23.D02_R1_CURRENTLY_STUDYING,
       bgs_dist_19_23.age,
       bgs_dist_19_23.region_cd, 
       bgs_dist_19_23.cur_res,
       bgs_dist_19_23.current_region,
       0 AS Age_Group,
       0 AS Age_Group_Rollup,
       0 AS New_Labour_Supply,
       0 AS Weight,
       0 AS Weight_CIP,
       bgs_dist_19_23.d01_r1
FROM   bgs_cohort_info
       INNER JOIN bgs_dist_19_23
               ON bgs_cohort_info.stqu_id = bgs_dist_19_23.stqu_id
WHERE  (bgs_cohort_info.srv_y_n = 0 OR bgs_cohort_info.srv_y_n = 1)"