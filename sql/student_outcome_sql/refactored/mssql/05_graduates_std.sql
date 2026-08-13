-- =====================================================================
-- TRD + APPSO graduate counts, standardized extraction (UNION)
-- Database : PSSM2025 (dbo)
-- Replaces : Q000_TRD_Graduates.sql, APPSO_Graduates.sql
-- Standard : see refactored/README.md
-- Note     : EXPR1 (Access auto-name) -> GRAD_COUNT; APPSO AGE_GROUP_LABEL
--            derived in R originally, here computed with the same bands.
-- =====================================================================
SELECT
    'TRD'                                        AS SURVEY,
    g.PSSM_CREDENTIAL                            AS PSSM_CREDENTIAL,
    g.SUBM_CD                                    AS SUBM_CD,
    ys.SURVEY_YEAR                               AS SURVEY_YEAR,
    g.TRD_AGE_AT_SURVEY                          AS AGE_AT_SURVEY,
    g.EXPR1                                      AS GRAD_COUNT,
    g.PRIVATE                                   AS PRIVATE,
    CASE
        WHEN g.TRD_AGE_AT_SURVEY BETWEEN 15 AND 16 THEN '15 to 16'
        WHEN g.TRD_AGE_AT_SURVEY BETWEEN 17 AND 19 THEN '17 to 19'
        WHEN g.TRD_AGE_AT_SURVEY BETWEEN 20 AND 24 THEN '20 to 24'
        WHEN g.TRD_AGE_AT_SURVEY BETWEEN 25 AND 29 THEN '25 to 29'
        WHEN g.TRD_AGE_AT_SURVEY BETWEEN 30 AND 34 THEN '30 to 34'
        WHEN g.TRD_AGE_AT_SURVEY BETWEEN 35 AND 44 THEN '35 to 44'
        WHEN g.TRD_AGE_AT_SURVEY BETWEEN 45 AND 54 THEN '45 to 54'
        WHEN g.TRD_AGE_AT_SURVEY BETWEEN 55 AND 64 THEN '55 to 64'
        WHEN g.TRD_AGE_AT_SURVEY BETWEEN 65 AND 89 THEN '65 to 89'
    END                                          AS AGE_GROUP_LABEL
FROM dbo.trd_graduates_r AS g
LEFT JOIN dbo.t_year_survey_year_r AS ys
    ON ys.SURVEY = 'TRD' AND ys.SUBM_CD = g.SUBM_CD

UNION ALL

SELECT
    'APPSO'                                      AS SURVEY,
    g.PSSM_CREDENTIAL                            AS PSSM_CREDENTIAL,
    g.SUBM_CD                                    AS SUBM_CD,
    ys.SURVEY_YEAR                               AS SURVEY_YEAR,
    g.APP_AGE_AT_SURVEY                          AS AGE_AT_SURVEY,
    g.EXPR1                                      AS GRAD_COUNT,
    g.PRIVATE                                   AS PRIVATE,
    g.AGE_GROUP                                  AS AGE_GROUP_LABEL
FROM dbo.appso_graduates_r AS g
LEFT JOIN dbo.t_year_survey_year_r AS ys
    ON ys.SURVEY = 'APPSO' AND ys.SUBM_CD = g.SUBM_CD
