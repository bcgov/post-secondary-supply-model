Q000_TRD_Graduates <- 
"SELECT 'TRD' AS Survey,
       0 AS Private,
       programs.prgm_credential AS PSSM_Credential,
       -- tbl_age_groups.age_group_label,
       trades_cohort_info.subm_cd,
       trades_cohort_info.trd_age_at_survey,
       Count(*) AS Expr1  
FROM trades_cohort_info
LEFT JOIN 
    (
        (
            (
            programs
            LEFT JOIN l_cip_6digits_cip2016
            ON programs.lcip_cd_cip2016 = l_cip_6digits_cip2016.lcip_cd
            )
        LEFT JOIN l_cip_4digits_cip2016
        ON l_cip_6digits_cip2016.lcip_lcp4_cd = l_cip_4digits_cip2016.lcp4_cd
        )
    RIGHT JOIN trades_short_resp
    ON programs.prgm_id = trades_short_resp.prgm_id
    )
ON trades_cohort_info.key = trades_short_resp.key
-- grouping by trd_age_at_survey but should be by age groups in Access tables
GROUP BY 'TRD',
          0,
          programs.prgm_credential,
          --tbl_age_groups.age_group_label,
          trades_cohort_info.trd_age_at_survey, 
          trades_cohort_info.subm_cd"

Q000_TRD_DATA_01 <- 
"SELECT 'TRD' AS Survey,
       trades_cohort_info.trd_age_at_survey,
       --tbl_age_groups.age_group,
       --tbl_age_groups.age_group_rollup,
       trades_cohort_info.gradstat,
       CASE gradstat WHEN '2' THEN '3' ELSE gradstat END AS gradstat_group,
       trades_cohort_info.key,
       trades_cohort_info.pen,
       trades_cohort_info.subm_cd,
       trades_short_resp.respondent,
       trades_cohort_info.inst,
       programs.prgm_credential AS PSSM_Credential,
       l_cip_6digits_cip2016.lcip_cd,
       l_cip_6digits_cip2016.lcip_lcp4_cd,
       trades_cohort_info.ttrain,
       trades_cohort_info.current_region1,
       trades_cohort_info.current_region4,
       trades_short_resp.trd_labr_in_labour_market,
       trades_short_resp.q18 AS TRD_LABR_EMPLOYED,
       trades_short_resp.trd_labr_unemployed,
       trades_short_resp.trd_labr_job_search_time_gp,
       trades_short_resp.trd_labr_job_training_related,
       trades_short_resp.noc_cd,
       -- gradstat_group || ' - ' || l_cip_6digits_cip2016.lcip_lcp4_cd || ' - ' || trades_cohort_info.ttrain || ' - ' || PSSM_Credential AS LCIP4_CRED,
       0 AS New_Labour_Supply,
       0 AS Weight
from trades_cohort_info 
LEFT JOIN 
    (
        (
            (
            l_cip_6digits_cip2016
            LEFT JOIN l_cip_4digits_cip2016 
            ON l_cip_6digits_cip2016.lcip_lcp4_cd = l_cip_4digits_cip2016.lcp4_cd
            )
        RIGHT JOIN programs 
        ON l_cip_6digits_cip2016.lcip_cd = programs.lcip_cd_cip2016
        )
    RIGHT JOIN trades_short_resp 
    ON programs.prgm_id = trades_short_resp.prgm_id
    ) 
ON trades_cohort_info.key = trades_short_resp.key
WHERE         trades_cohort_info.subm_cd = 'C_Outc23'
           OR trades_cohort_info.subm_cd = 'C_Outc22'
           OR trades_cohort_info.subm_cd = 'C_Outc21'
           OR trades_cohort_info.subm_cd = 'C_Outc20'
           OR trades_cohort_info.subm_cd = 'C_Outc19'
           OR trades_cohort_info.subm_cd = 'C_Outc18'"
