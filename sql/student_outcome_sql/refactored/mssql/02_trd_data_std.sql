-- =====================================================================
-- Trades (TRD) data, standardized extraction
-- Database : PSSM2025 (dbo)
-- Scope    : survey cycles C_Outc21-C_Outc25 (SUBM_CD filter)
-- Replaces : Q000_TRD_DATA_01.sql
-- Standard : see refactored/README.md
-- =====================================================================
SELECT
    'TRD'                                          AS SURVEY,
    t.SUBM_CD                                      AS SUBM_CD,
    ys.SURVEY_YEAR                                 AS SURVEY_YEAR,
    t.[KEY]                                        AS STUDENT_KEY,
    t.PEN                                          AS PEN,
    t.RESPONDENT                                   AS RESPONDENT,
    t.INST                                         AS INST,
    t.PSSM_CREDENTIAL                              AS PSSM_CREDENTIAL,
    t.LCP6_CD                                      AS LCP6_CD,
    cip6.LCP6_DIGITS_NAME                          AS LCP6_DIGITS_NAME,
    t.LCIP_LCP4_CD                                 AS LCP4_CD,
    cip4.LCP4_DIGITS_NAME                          AS LCP4_DIGITS_NAME,
    t.TTRAIN                                       AS TTRAIN,
    t.TRD_AGE_AT_SURVEY                            AS AGE_AT_SURVEY,
    t.CURRENT_REGION1                              AS CURRENT_REGION1,
    t.CURRENT_REGION4                              AS CURRENT_REGION4,
    t.CURRENT_REGION_PSSM_CODE                     AS CURRENT_REGION_PSSM_CODE,
    t.TRD_LABR_IN_LABOUR_MARKET                    AS LABR_IN_LABOUR_MARKET,
    t.TRD_LABR_EMPLOYED                            AS LABR_EMPLOYED,
    t.TRD_LABR_UNEMPLOYED                          AS LABR_UNEMPLOYED,
    t.TRD_LABR_JOB_SEARCH_TIME_GP                  AS LABR_JOB_SEARCH_TIME_GP,
    t.TRD_LABR_JOB_TRAINING_RELATED                AS LABR_JOB_TRAINING_RELATED,
    t.NOC_CD                                       AS NOC_CD,
    t.NEW_LABOUR_SUPPLY                            AS NEW_LABOUR_SUPPLY,
    t.WEIGHT                                       AS WEIGHT,
    t.LCIP4_CRED                                   AS LCIP4_CRED,
    CAST(NULL AS float)                            AS TOOK_FURTH_ED,
    t.GRADSTAT                                     AS GRADSTAT,
    t.GRADSTAT_GROUP                               AS GRADSTAT_GROUP
FROM dbo.trd_data_r AS t
LEFT JOIN dbo.t_year_survey_year_r AS ys
    ON ys.SURVEY = 'TRD' AND ys.SUBM_CD = t.SUBM_CD
LEFT JOIN dbo.INFOWARE_L_CIP_6DIGITS_CIP2021 AS cip6
    ON cip6.LCP6_CD = t.LCP6_CD
LEFT JOIN dbo.INFOWARE_L_CIP_4DIGITS_CIP2021 AS cip4
    ON cip4.LCP4_CD = t.LCIP_LCP4_CD
WHERE t.SUBM_CD IN ('C_Outc21','C_Outc22','C_Outc23','C_Outc24','C_Outc25')
