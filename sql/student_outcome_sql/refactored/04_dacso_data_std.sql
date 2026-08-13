-- =====================================================================
-- DACSO data, standardized extraction
-- Database : INFOWARE Oracle (scratch.bcgov:1521/pdbtrn)
-- Scope    : survey cycles C_Outc21-C_Outc25
-- Replaces : DACSO_Q003_DACSO_DATA_Part_1_stepA.sql
-- Standard : see README.md
-- Notes    : frame = SURV_COHORT_COLLECTION_INFO (INNER JOIN programs + CIP);
--            responses = C_OUTC_CLEAN_SHORT_RESP (LEFT JOIN on STQU_ID).
--            C_OUTC_CLEAN2 dropped (redundant, its columns were unused).
--            RESPONDENT derived from COCI_LRST_CD = '000' (same rule as
--            the original). Developmental / Personal-Improvement & Leisure
--            CIP clusters excluded (same filter as the original).
-- =====================================================================
SELECT
    'DACSO'                                          AS SURVEY,
    sc.coci_subm_cd                                  AS SUBM_CD,
    CASE sc.coci_subm_cd
        WHEN 'C_Outc21' THEN 2021
        WHEN 'C_Outc22' THEN 2022
        WHEN 'C_Outc23' THEN 2023
        WHEN 'C_Outc24' THEN 2024
        WHEN 'C_Outc25' THEN 2025
    END                                              AS SURVEY_YEAR,
    sc.coci_stqu_id                                  AS STUDENT_KEY,
    sc.coci_pen                                      AS PEN,
    CASE WHEN sc.coci_lrst_cd = '000' THEN 1 ELSE 0 END AS RESPONDENT,
    sc.coci_inst_cd                                  AS INST,
    sc.coci_inst_name                                AS INST_NAME,
    pr.prgm_credential                               AS PSSM_CREDENTIAL,
    sr.credential_derived                            AS CREDENTIAL_DERIVED,
    cip6.lcp6_cd                                     AS LCP6_CD,
    cip6.lcp6_digits_name                            AS LCP6_DIGITS_NAME,
    cip4.lcp4_cd                                     AS LCP4_CD,
    cip4.lcp4_digits_name                            AS LCP4_DIGITS_NAME,
    sr.ttrain                                        AS TTRAIN,
    sc.coci_age_at_survey                            AS AGE_AT_SURVEY,
    sc.coci_age_group                                AS AGE_GROUP_SRC,
    sc.tpid_current_region1                          AS CURRENT_REGION1,
    sc.tpid_current_region4                          AS CURRENT_REGION4,
    sr.labr_in_labour_market                         AS LABR_IN_LABOUR_MARKET,
    sr.q18                                           AS LABR_EMPLOYED,
    sr.labr_unemployed                               AS LABR_UNEMPLOYED,
    sr.labr_job_search_time_gp                       AS LABR_JOB_SEARCH_TIME_GP,
    sr.labr_job_training_related                     AS LABR_JOB_TRAINING_RELATED,
    sr.labr_employed_full_part_time                  AS LABR_EMPLOYED_FULL_PART_TIME,
    sr.labr_job_permanent                            AS LABR_JOB_PERMANENT,
    sr.labr_occ_lnoc_2021_cd                         AS NOC_CD,
    sr.labr_occ_lnoc_2021_name                       AS NOC_NAME,
    CASE sc.cosc_grad_status_lgds_cd
        WHEN '2' THEN '3'
        ELSE sc.cosc_grad_status_lgds_cd
    END                                              AS GRADSTAT_GROUP,
    sc.cosc_grad_status_lgds_cd                      AS GRADSTAT,
    sc.coci_lrst_cd                                  AS LRST_CD,
    sc.cosc_international                            AS INTERNATIONAL,
    sc.cosc_prgm_id                                  AS PRGM_ID,
    sr.pfst_current_activity                         AS PFST_CURRENT_ACTIVITY,
    sr.pfst_currently_studying                       AS PFST_CURRENTLY_STUDYING,
    sr.pfst_furstdy_incl_still_attd                  AS PFST_FURSTDY_INCL_STILL_ATTD,
    sr.pfst_had_previous_cdtl                        AS PFST_HAD_PREVIOUS_CDTL,
    sc.cosc_completed_prgm_req                       AS COSC_COMPLETED_PRGM_REQ,
    sc.cosc_cum_gpa_group                            AS COSC_CUM_GPA_GROUP,
    sc.tpid_date_of_birth                            AS DATE_OF_BIRTH,
    sc.cosc_enrl_end_date                            AS ENRL_END_DATE,
    sr.startmonth                                    AS STARTMONTH,
    sr.startyear                                     AS STARTYEAR,
    sr.endmonth                                      AS ENDMONTH,
    sr.endyear                                       AS ENDYEAR
FROM surv_cohort_collection_info sc
INNER JOIN programs pr
    ON pr.prgm_id = sc.cosc_prgm_id
INNER JOIN l_cip_6digits_cip2021 cip6
    ON cip6.lcp6_cd = pr.lcip_cd_cip2021
INNER JOIN l_cip_4digits_cip2021 cip4
    ON cip4.lcp4_cd = cip6.lcip_lcp4_cd
LEFT JOIN c_outc_clean_short_resp sr
    ON sr.stqu_id = sc.coci_stqu_id
WHERE sc.coci_subm_cd IN ('C_Outc21','C_Outc22','C_Outc23','C_Outc24','C_Outc25')
  AND cip6.lcip_lcippc_name NOT IN ('Developmental', 'Personal Improvement and Leisure')
