-- =====================================================================
-- BGS data (Bachelor's graduates), standardized extraction
-- Database : INFOWARE Oracle (scratch.bcgov:1521/pdbtrn)
-- Scope    : survey cycles 2021-2025 (BGS_DIST_21_25)
-- Replaces : BGS_Q001_BGS_Data_2021_2025.sql
-- Standard : see README.md
-- Notes    : BGS = bachelor's graduates, PSSM_CREDENTIAL constant 'BACH'.
--            BGS has no CIP6; CIP4 name from L_CIP_4DIGITS_CIP2021.
--            Age band: BGS_DIST_21_25.AGE_RNG_AGGR (source-coded).
-- =====================================================================
SELECT
    'BGS'                                            AS SURVEY,
    TO_CHAR(NULL)                                    AS SUBM_CD,
    d.year                                          AS SURVEY_YEAR,
    b.stqu_id                                       AS STUDENT_KEY,
    b.pen                                           AS PEN,
    b.srv_y_n                                       AS RESPONDENT,
    b.inst                                          AS INST,
    b.inst_name                                     AS INST_NAME,
    CAST('BACH' AS VARCHAR2(10))                    AS PSSM_CREDENTIAL,
    TO_CHAR(NULL)                                   AS LCP6_CD,
    TO_CHAR(NULL)                                   AS LCP6_DIGITS_NAME,
    b.cip_4digit_no_period                          AS LCP4_CD,
    cip4.lcp4_digits_name                           AS LCP4_DIGITS_NAME,
    TO_CHAR(NULL)                                   AS TTRAIN,
    d.age                                           AS AGE_AT_SURVEY,
    d.age_rng_aggr                                  AS AGE_GROUP_SRC,
    TO_NUMBER(NULL)                                 AS CURRENT_REGION1,
    TO_NUMBER(NULL)                                 AS CURRENT_REGION4,
    d.lbr_frc_labour_market                         AS LABR_IN_LABOUR_MARKET,
    d.lbr_frc_currently_employed                    AS LABR_EMPLOYED,
    d.lbr_frc_unemployed                            AS LABR_UNEMPLOYED,
    TO_NUMBER(NULL)                                 AS LABR_JOB_SEARCH_TIME_GP,
    d.e10_in_training_related_job                   AS LABR_JOB_TRAINING_RELATED,
    d.noc_2021                                      AS NOC_CD,
    d.noc_2021_name                                 AS NOC_NAME,
    TO_CHAR(NULL)                                   AS GRADSTAT_GROUP,
    d.gender                                        AS GENDER,
    TO_CHAR(d.international)                         AS INTERNATIONAL,
    d.full_tm                                       AS FULL_TM_WRK,
    d.totl_sal                                      AS TOTL_SAL,
    d.income                                        AS INCOME,
    d.cur_res                                       AS CURRENT_RESIDENCE,
    d.current_region                                AS CURRENT_REGION,
    d.region_cd                                     AS REGION_CD,
    d.pfst_currently_studying                       AS PFST_CURRENTLY_STUDYING,
    d.d01_taken_further_studies                     AS TOOK_FURTH_ED
FROM bgs_cohort_info b
INNER JOIN bgs_dist_21_25 d
    ON d.stqu_id = b.stqu_id
LEFT JOIN l_cip_4digits_cip2021 cip4
    ON cip4.lcp4_cd = b.cip_4digit_no_period
WHERE b.srv_y_n IN (0, 1)
