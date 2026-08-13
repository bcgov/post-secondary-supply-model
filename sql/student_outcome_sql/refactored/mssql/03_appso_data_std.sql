-- =====================================================================
-- Apprentice (APPSO) data, standardized extraction
-- Database : PSSM2025 (dbo)
-- Scope    : survey cycles C_Outc21-C_Outc25 (SUBM_CD filter)
-- Replaces : APPSO_Data_01_Final.sql
-- Standard : see refactored/README.md
-- Note     : APPSO has no job-search-time question -> NULL
-- =====================================================================
SELECT
    'APPSO'                                        AS SURVEY,
    a.SUBM_CD                                      AS SUBM_CD,
    ys.SURVEY_YEAR                                 AS SURVEY_YEAR,
    a.[KEY]                                        AS STUDENT_KEY,
    a.PEN                                          AS PEN,
    a.RESPONDENT                                   AS RESPONDENT,
    a.INST                                         AS INST,
    a.PSSM_CREDENTIAL                              AS PSSM_CREDENTIAL,
    a.LCP6_CD                                      AS LCP6_CD,
    cip6.LCP6_DIGITS_NAME                          AS LCP6_DIGITS_NAME,
    a.LCIP_LCP4_CD                                 AS LCP4_CD,
    cip4.LCP4_DIGITS_NAME                          AS LCP4_DIGITS_NAME,
    a.TTRAIN                                       AS TTRAIN,
    a.APP_AGE_AT_SURVEY                            AS AGE_AT_SURVEY,
    a.CURRENT_REGION1                              AS CURRENT_REGION1,
    a.CURRENT_REGION4                              AS CURRENT_REGION4,
    a.CURRENT_REGION_PSSM_CODE                     AS CURRENT_REGION_PSSM_CODE,
    a.APP_LABR_IN_LABOUR_MARKET                    AS LABR_IN_LABOUR_MARKET,
    a.APP_LABR_EMPLOYED                            AS LABR_EMPLOYED,
    a.APP_LABR_UNEMPLOYED                          AS LABR_UNEMPLOYED,
    CAST(NULL AS float)                            AS LABR_JOB_SEARCH_TIME_GP,
    a.APP_LABR_JOB_TRAINING_RELATED                AS LABR_JOB_TRAINING_RELATED,
    a.NOC_CD                                       AS NOC_CD,
    a.NEW_LABOUR_SUPPLY                            AS NEW_LABOUR_SUPPLY,
    a.WEIGHT                                       AS WEIGHT,
    a.LCIP4_CRED                                   AS LCIP4_CRED,
    CAST(NULL AS float)                            AS TOOK_FURTH_ED,
    a.APP_TIME_TO_FIND_EMPLOY_MJOB                 AS TIME_TO_FIND_EMPLOY_MJOB,
    a.AGE_GROUP                                    AS AGE_GROUP,
    a.AGE_GROUP_LABEL                              AS AGE_GROUP_LABEL
FROM dbo.t_appso_data_final_r AS a
LEFT JOIN dbo.t_year_survey_year_r AS ys
    ON ys.SURVEY = 'APPSO' AND ys.SUBM_CD = a.SUBM_CD
LEFT JOIN dbo.INFOWARE_L_CIP_6DIGITS_CIP2021 AS cip6
    ON cip6.LCP6_CD = a.LCP6_CD
LEFT JOIN dbo.INFOWARE_L_CIP_4DIGITS_CIP2021 AS cip4
    ON cip4.LCP4_CD = a.LCIP_LCP4_CD
WHERE a.SUBM_CD IN ('C_Outc21','C_Outc22','C_Outc23','C_Outc24','C_Outc25')
