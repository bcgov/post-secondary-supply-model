-- =====================================================================
-- BGS data (Bachelor's graduates), standardized extraction
-- Database : PSSM2025 (dbo)
-- Scope    : survey cycles 2021-2025 (SURVEY_YEAR filter)
-- Replaces : BGS_Q001_BGS_Data_2021_2025.sql
-- Standard : see refactored/README.md
-- Derived  : CURRENT_ACTIVITY 1-4 from employment/study status
--            LCIP4_CRED       CIP4 + credential composite key
-- =====================================================================
SELECT
    'BGS'                                          AS SURVEY,
    CAST(NULL AS varchar(12))                      AS SUBM_CD,
    b.SURVEY_YEAR                                  AS SURVEY_YEAR,
    b.STQU_ID                                      AS STUDENT_KEY,
    b.PEN                                          AS PEN,
    b.SRV_Y_N                                      AS RESPONDENT,
    b.INST                                         AS INST,
    b.PSSM_CREDENTIAL                              AS PSSM_CREDENTIAL,
    CAST(NULL AS varchar(6))                       AS LCP6_CD,
    CAST(NULL AS varchar(150))                     AS LCP6_DIGITS_NAME,
    b.CIP_CODE_4                                   AS LCP4_CD,
    cip4.LCP4_DIGITS_NAME                          AS LCP4_DIGITS_NAME,
    CAST(NULL AS float)                            AS TTRAIN,
    b.AGE                                          AS AGE_AT_SURVEY,
    CAST(NULL AS float)                            AS CURRENT_REGION1,
    CAST(NULL AS float)                            AS CURRENT_REGION4,
    b.CURRENT_REGION_PSSM_CODE                     AS CURRENT_REGION_PSSM_CODE,
    b.IN_LBR_FRC                                   AS LABR_IN_LABOUR_MARKET,
    b.EMPLOYED                                     AS LABR_EMPLOYED,
    b.UNEMPLOYED                                   AS LABR_UNEMPLOYED,
    CAST(NULL AS float)                            AS LABR_JOB_SEARCH_TIME_GP,
    b.TRAINING_RELATED                             AS LABR_JOB_TRAINING_RELATED,
    b.NOC                                          AS NOC_CD,
    b.NEW_LABOUR_SUPPLY                            AS NEW_LABOUR_SUPPLY,
    b.WEIGHT                                       AS WEIGHT,
    b.LCIP4_CRED                                   AS LCIP4_CRED,
    b.TOOK_FURTH_ED                                AS TOOK_FURTH_ED,
    b.CURRENT_ACTIVITY                             AS CURRENT_ACTIVITY,
    b.FULL_TM_WRK                                  AS FULL_TM_WRK,
    b.AGE_GROUP                                    AS AGE_GROUP,
    b.AGE_GROUP_ROLLUP                             AS AGE_GROUP_ROLLUP,
    b.AGE_17_34                                    AS AGE_17_34,
    b.OLD_LABOUR_SUPPLY                            AS OLD_LABOUR_SUPPLY,
    b.WEIGHT_CIP                                   AS WEIGHT_CIP
FROM dbo.t_bgs_data_final_r AS b
LEFT JOIN dbo.INFOWARE_L_CIP_4DIGITS_CIP2021 AS cip4
    ON cip4.LCP4_CD = b.CIP_CODE_4
WHERE b.SURVEY_YEAR BETWEEN 2021 AND 2025
