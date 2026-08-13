-- =====================================================================
-- Apprentice (APPSO) data, standardized extraction
-- Database : INFOWARE Oracle (scratch.bcgov:1521/pdbtrn)
-- Scope    : survey cycles C_Outc21-C_Outc25
-- Replaces : APPSO_Data_01_Final.sql
-- Standard : see README.md
-- Notes    : frame = APPRENTICE_COHORT_INFO; responses = APPRENTICE_SHORT_RESP.
--            APPSO has no job-search-time question -> NULL.
-- =====================================================================
SELECT
    'APPSO'                                          AS SURVEY,
    ci.subm_cd                                       AS SUBM_CD,
    CASE ci.subm_cd
        WHEN 'C_Outc21' THEN 2021
        WHEN 'C_Outc22' THEN 2022
        WHEN 'C_Outc23' THEN 2023
        WHEN 'C_Outc24' THEN 2024
        WHEN 'C_Outc25' THEN 2025
    END                                              AS SURVEY_YEAR,
    ci.key                                           AS STUDENT_KEY,
    ci.pen                                           AS PEN,
    sr.respondent                                    AS RESPONDENT,
    ci.inst                                          AS INST,
    ci.inst_name                                     AS INST_NAME,
    'APPR' || pr.prgm_credential                     AS PSSM_CREDENTIAL,
    cip6.lcp6_cd                                     AS LCP6_CD,
    cip6.lcp6_digits_name                            AS LCP6_DIGITS_NAME,
    cip4.lcp4_cd                                     AS LCP4_CD,
    cip4.lcp4_digits_name                            AS LCP4_DIGITS_NAME,
    ci.ttrain                                        AS TTRAIN,
    ci.app_age_at_survey                             AS AGE_AT_SURVEY,
    ci.app_age_at_survey_grp                         AS AGE_GROUP_SRC,
    ci.current_region1                               AS CURRENT_REGION1,
    ci.current_region4                               AS CURRENT_REGION4,
    sr.app_labr_in_labour_market                     AS LABR_IN_LABOUR_MARKET,
    sr.q18                                           AS LABR_EMPLOYED,
    sr.app_labr_unemployed                           AS LABR_UNEMPLOYED,
    TO_NUMBER(NULL)                                  AS LABR_JOB_SEARCH_TIME_GP,
    sr.app_labr_job_training_related                 AS LABR_JOB_TRAINING_RELATED,
    sr.noc_cd_2021                                   AS NOC_CD,
    sr.noc_name_2021                                 AS NOC_NAME,
    TO_CHAR(NULL)                                    AS GRADSTAT_GROUP,
    ci.gradstat                                      AS GRADSTAT,
    ci.gender                                        AS GENDER,
    ci.international                                 AS INTERNATIONAL,
    ci.lrst_cd                                       AS LRST_CD,
    sr.prgm_id                                       AS PRGM_ID,
    sr.app_time_to_find_employ_mjob                  AS TIME_TO_FIND_EMPLOY_MJOB,
    sr.app_labr_employed_ft_mjob                     AS LABR_EMPLOYED_FT_MJOB,
    sr.app_labr_employ_full_part_time                AS LABR_EMPLOY_FULL_PART_TIME,
    sr.app_labr_mjob_hourly_gross_sal                AS LABR_MJOB_HOURLY_GROSS_SAL,
    sr.app_labr_job_permanent                        AS LABR_JOB_PERMANENT,
    sr.private                                       AS PRIVATE_FLAG,
    sr.startmonth                                    AS STARTMONTH,
    sr.startyear                                     AS STARTYEAR,
    sr.endmonth                                      AS ENDMONTH,
    sr.endyear                                       AS ENDYEAR
FROM apprentice_cohort_info ci
LEFT JOIN apprentice_short_resp sr
    ON sr.key = ci.key
LEFT JOIN programs pr
    ON pr.prgm_id = sr.prgm_id
LEFT JOIN l_cip_6digits_cip2021 cip6
    ON cip6.lcp6_cd = pr.lcip_cd_cip2021
LEFT JOIN l_cip_4digits_cip2021 cip4
    ON cip4.lcp4_cd = cip6.lcip_lcp4_cd
WHERE ci.subm_cd IN ('C_Outc21','C_Outc22','C_Outc23','C_Outc24','C_Outc25')
