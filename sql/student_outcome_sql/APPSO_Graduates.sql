SELECT 'APPSO' AS Survey,
       apprentice_short_resp.private,
       'APPR' || programs.prgm_credential AS PSSM_Credential,
       --tbl_age_groups.age_group_label,
       apprentice_cohort_info.subm_cd,
       apprentice_cohort_info.app_age_at_survey,
       Count(*) AS Expr1
FROM  
    (
        (
            (
            apprentice_short_resp
            RIGHT JOIN apprentice_cohort_info
            ON apprentice_short_resp.KEY = apprentice_cohort_info.KEY
            )
        LEFT JOIN programs
        ON apprentice_short_resp.prgm_id = programs.prgm_id
        )
    LEFT JOIN l_cip_6digits_cip2021
    ON programs.lcip_cd_cip2021 = l_cip_6digits_cip2021.lcp6_cd
    )
LEFT JOIN l_cip_4digits_cip2021
ON l_cip_6digits_cip2021.lcip_lcp4_cd = l_cip_4digits_cip2021.lcp4_cd
GROUP  BY 'APPSO',
          apprentice_short_resp.private,
          'APPR' || programs.prgm_credential,
          apprentice_cohort_info.app_age_at_survey,
          -- tbl_age_groups.age_group_label,
          apprentice_cohort_info.subm_cd
ORDER  BY 'APPR' || programs.prgm_credential,
          -- tbl_age_groups.age_group_label,
          apprentice_cohort_info.app_age_at_survey,
          apprentice_cohort_info.subm_cd