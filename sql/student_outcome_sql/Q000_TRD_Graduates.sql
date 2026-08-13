SELECT 'TRD' AS Survey,
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
            LEFT JOIN l_cip_6digits_cip2021
            ON programs.lcip_cd_cip2021 = l_cip_6digits_cip2021.lcp6_cd
            )
        LEFT JOIN l_cip_4digits_cip2021
        ON l_cip_6digits_cip2021.lcip_lcp4_cd = l_cip_4digits_cip2021.lcp4_cd
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
          trades_cohort_info.subm_cd