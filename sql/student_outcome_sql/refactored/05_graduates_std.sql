-- =====================================================================
-- TRD + APPSO graduate counts, standardized extraction (UNION ALL)
-- Database : INFOWARE Oracle (scratch.bcgov:1521/pdbtrn)
-- Scope    : survey cycles C_Outc21-C_Outc25
-- Replaces : Q000_TRD_Graduates.sql, APPSO_Graduates.sql
-- Standard : see README.md
-- Notes    : EXPR1 (Access auto-name) -> GRAD_COUNT; Private -> PRIVATE_FLAG.
--            AGE_GROUP_LABEL coded with the same bands as the PSSM R
--            pipeline (load-cohort-*.R). Inner subqueries group on raw
--            age; the outer SELECT derives SURVEY_YEAR + AGE_GROUP_LABEL
--            so Oracle's GROUP BY rules are satisfied cleanly.
-- =====================================================================
SELECT
    'TRD'                                            AS SURVEY,
    0                                                AS PRIVATE_FLAG,
    g.pssm_credential                                AS PSSM_CREDENTIAL,
    g.subm_cd                                        AS SUBM_CD,
    CASE g.subm_cd
        WHEN 'C_Outc21' THEN 2021
        WHEN 'C_Outc22' THEN 2022
        WHEN 'C_Outc23' THEN 2023
        WHEN 'C_Outc24' THEN 2024
        WHEN 'C_Outc25' THEN 2025
    END                                              AS SURVEY_YEAR,
    g.age_at_survey                                  AS AGE_AT_SURVEY,
    CASE
        WHEN g.age_at_survey BETWEEN 15 AND 16 THEN '15 to 16'
        WHEN g.age_at_survey BETWEEN 17 AND 19 THEN '17 to 19'
        WHEN g.age_at_survey BETWEEN 20 AND 24 THEN '20 to 24'
        WHEN g.age_at_survey BETWEEN 25 AND 29 THEN '25 to 29'
        WHEN g.age_at_survey BETWEEN 30 AND 34 THEN '30 to 34'
        WHEN g.age_at_survey BETWEEN 35 AND 44 THEN '35 to 44'
        WHEN g.age_at_survey BETWEEN 45 AND 54 THEN '45 to 54'
        WHEN g.age_at_survey BETWEEN 55 AND 64 THEN '55 to 64'
        WHEN g.age_at_survey BETWEEN 65 AND 89 THEN '65 to 89'
    END                                              AS AGE_GROUP_LABEL,
    g.grad_count                                     AS GRAD_COUNT
FROM (
    SELECT
        pr.prgm_credential                           AS pssm_credential,
        ci.subm_cd                                   AS subm_cd,
        ci.trd_age_at_survey                         AS age_at_survey,
        COUNT(*)                                     AS grad_count
    FROM trades_cohort_info ci
    LEFT JOIN trades_short_resp sr
        ON sr.key = ci.key
    LEFT JOIN programs pr
        ON pr.prgm_id = sr.prgm_id
    GROUP BY pr.prgm_credential, ci.subm_cd, ci.trd_age_at_survey
) g

UNION ALL

SELECT
    'APPSO'                                          AS SURVEY,
    g.private_flag                                   AS PRIVATE_FLAG,
    g.pssm_credential                                AS PSSM_CREDENTIAL,
    g.subm_cd                                        AS SUBM_CD,
    CASE g.subm_cd
        WHEN 'C_Outc21' THEN 2021
        WHEN 'C_Outc22' THEN 2022
        WHEN 'C_Outc23' THEN 2023
        WHEN 'C_Outc24' THEN 2024
        WHEN 'C_Outc25' THEN 2025
    END                                              AS SURVEY_YEAR,
    g.age_at_survey                                  AS AGE_AT_SURVEY,
    CASE
        WHEN g.age_at_survey BETWEEN 15 AND 16 THEN '15 to 16'
        WHEN g.age_at_survey BETWEEN 17 AND 19 THEN '17 to 19'
        WHEN g.age_at_survey BETWEEN 20 AND 24 THEN '20 to 24'
        WHEN g.age_at_survey BETWEEN 25 AND 29 THEN '25 to 29'
        WHEN g.age_at_survey BETWEEN 30 AND 34 THEN '30 to 34'
        WHEN g.age_at_survey BETWEEN 35 AND 44 THEN '35 to 44'
        WHEN g.age_at_survey BETWEEN 45 AND 54 THEN '45 to 54'
        WHEN g.age_at_survey BETWEEN 55 AND 64 THEN '55 to 64'
        WHEN g.age_at_survey BETWEEN 65 AND 89 THEN '65 to 89'
    END                                              AS AGE_GROUP_LABEL,
    g.grad_count                                     AS GRAD_COUNT
FROM (
    SELECT
        sr.private                                   AS private_flag,
        'APPR' || pr.prgm_credential                 AS pssm_credential,
        ci.subm_cd                                   AS subm_cd,
        ci.app_age_at_survey                         AS age_at_survey,
        COUNT(*)                                     AS grad_count
    FROM apprentice_cohort_info ci
    LEFT JOIN apprentice_short_resp sr
        ON sr.key = ci.key
    LEFT JOIN programs pr
        ON pr.prgm_id = sr.prgm_id
    GROUP BY sr.private, 'APPR' || pr.prgm_credential, ci.subm_cd, ci.app_age_at_survey
) g
