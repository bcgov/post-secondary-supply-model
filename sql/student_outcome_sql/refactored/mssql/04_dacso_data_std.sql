-- =====================================================================
-- DACSO data, standardized extraction
-- Database : PSSM2025 (dbo)
-- Scope    : survey cycles C_Outc21-C_Outc25 (COCI_SUBM_CD filter)
-- Replaces : DACSO_Q003_DACSO_DATA_Part_1_stepA.sql
-- Standard : see refactored/README.md
-- Derived  : RESPONDENT = COCI_LRST_CD = '000' (same rule as original)
-- =====================================================================
SELECT
    'DACSO'                                        AS SURVEY,
    d.COCI_SUBM_CD                                 AS SUBM_CD,
    ys.SURVEY_YEAR                                 AS SURVEY_YEAR,
    d.COCI_STQU_ID                                 AS STUDENT_KEY,
    d.COCI_PEN                                     AS PEN,
    d.RESPONDENT                                   AS RESPONDENT,
    d.COCI_INST_CD                                 AS INST,
    d.PRGM_CREDENTIAL                              AS PSSM_CREDENTIAL,
    d.LCP6_CD                                      AS LCP6_CD,
    cip6.LCP6_DIGITS_NAME                          AS LCP6_DIGITS_NAME,
    d.LCP4_CD                                      AS LCP4_CD,
    d.LCP4_DIGITS_NAME                             AS LCP4_DIGITS_NAME,
    d.TTRAIN                                       AS TTRAIN,
    d.COCI_AGE_AT_SURVEY                           AS AGE_AT_SURVEY,
    d.TPID_CURRENT_REGION1                         AS CURRENT_REGION1,
    d.TPID_CURRENT_REGION4                         AS CURRENT_REGION4,
    d.CURRENT_REGION_PSSM_CODE                     AS CURRENT_REGION_PSSM_CODE,
    d.LABR_IN_LABOUR_MARKET                        AS LABR_IN_LABOUR_MARKET,
    d.LABR_EMPLOYED                                AS LABR_EMPLOYED,
    d.LABR_UNEMPLOYED                              AS LABR_UNEMPLOYED,
    d.LABR_JOB_SEARCH_TIME_GP                      AS LABR_JOB_SEARCH_TIME_GP,
    d.LABR_JOB_TRAINING_RELATED                    AS LABR_JOB_TRAINING_RELATED,
    d.LABR_OCCUPATION_LNOC_CD                      AS NOC_CD,
    d.NEW_LABOUR_SUPPLY                            AS NEW_LABOUR_SUPPLY,
    d.WEIGHT                                       AS WEIGHT,
    CAST(NULL AS varchar(50))                      AS LCIP4_CRED,
    d.PFST_FURSTDY_INCL_STILL_ATTD                 AS TOOK_FURTH_ED,
    d.PFST_CURRENT_ACTIVITY                        AS CURRENT_ACTIVITY,
    d.COSC_GRAD_STATUS_LGDS_CD                     AS GRADSTAT,
    d.COSC_GRAD_STATUS_LGDS_CD_GROUP               AS GRADSTAT_GROUP,
    d.AGE_GROUP                                    AS AGE_GROUP,
    d.AGE_GROUP_ROLLUP                             AS AGE_GROUP_ROLLUP,
    d.LABR_EMPLOYED_FULL_PART_TIME                 AS LABR_EMPLOYED_FULL_PART_TIME,
    d.HAD_PREVIOUS_CREDENTIAL                      AS HAD_PREVIOUS_CREDENTIAL,
    d.PFST_IN_POST_SEC_BEFORE                      AS PFST_IN_POST_SEC_BEFORE,
    d.PFST_HAD_PREVIOUS_CDTL                       AS PFST_HAD_PREVIOUS_CDTL,
    d.OLD_LABOUR_SUPPLY                            AS OLD_LABOUR_SUPPLY
FROM dbo.t_dacso_data_part_1_stepa_r AS d
LEFT JOIN dbo.t_year_survey_year_r AS ys
    ON ys.SURVEY = 'DACSO' AND ys.SUBM_CD = d.COCI_SUBM_CD
LEFT JOIN dbo.INFOWARE_L_CIP_6DIGITS_CIP2021 AS cip6
    ON cip6.LCP6_CD = d.LCP6_CD
WHERE d.COCI_SUBM_CD IN ('C_Outc21','C_Outc22','C_Outc23','C_Outc24','C_Outc25')
