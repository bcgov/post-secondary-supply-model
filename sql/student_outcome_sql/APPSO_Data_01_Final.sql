SELECT 'APPSO' AS Survey,
       apprentice_cohort_info.app_age_at_survey,
       -- tbl_age_groups.age_group,
       -- tbl_age_groups.age_group_rollup,
       apprentice_cohort_info.KEY,
       apprentice_cohort_info.pen,
       apprentice_cohort_info.subm_cd,
       apprentice_short_resp.respondent,
       apprentice_cohort_info.inst,
       apprentice_cohort_info.current_region1,
       apprentice_cohort_info.current_region4,
        'APPR' || programs.prgm_credential AS PSSM_Credential,
       l_cip_6digits_cip2021.lcp6_cd,
       l_cip_6digits_cip2021.lcip_lcp4_cd,
       apprentice_cohort_info.ttrain,
       apprentice_short_resp.app_labr_in_labour_market,
       apprentice_short_resp.q18 AS APP_LABR_EMPLOYED,
       apprentice_short_resp.app_labr_unemployed,
       apprentice_short_resp.app_time_to_find_employ_mjob,
       apprentice_short_resp.app_labr_job_training_related,
       apprentice_short_resp.noc_cd_2021 as noc_cd,
       --apprentice_short_resp.noc_cd,
       l_cip_6digits_cip2021.lcip_lcp4_cd || ' - APPR' || programs.prgm_credential AS LCIP4_CRED,
       0  AS New_Labour_Supply,
       0  AS Weight
FROM   l_cip_6digits_cip2021
LEFT JOIN l_cip_4digits_cip2021
ON l_cip_6digits_cip2021.lcip_lcp4_cd = l_cip_4digits_cip2021.lcp4_cd
RIGHT JOIN 
    (
        (
        apprentice_short_resp
        RIGHT JOIN apprentice_cohort_info
        ON apprentice_short_resp.KEY = apprentice_cohort_info.KEY
        )
        LEFT JOIN programs
        ON apprentice_short_resp.prgm_id = programs.prgm_id
    )
ON l_cip_6digits_cip2021.lcp6_cd = programs.lcip_cd_cip2021
WHERE  apprentice_cohort_info.subm_cd IN (
  'C_Outc25', 
  'C_Outc24', 
  'C_Outc23', 
  'C_Outc22', 
  'C_Outc21')
ORDER  BY apprentice_cohort_info.subm_cd