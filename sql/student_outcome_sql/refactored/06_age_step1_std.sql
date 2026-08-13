-- =====================================================================
-- DACSO age-at-survey per respondent, standardized extraction
-- Database : INFOWARE Oracle (scratch.bcgov:1521/pdbtrn)
-- Scope    : survey cycles C_Outc21-C_Outc25
-- Replaces : qry_make_tmp_table_Age_step1.sql
-- Standard : see README.md
-- Notes    : TPID_DATE_OF_BIRTH and COSC_ENRL_END_DATE are VARCHAR2 dates
--            in Oracle (string form), not numeric as in the MS SQL copy.
-- =====================================================================
SELECT
    coci_stqu_id                                     AS STUDENT_KEY,
    coci_subm_cd                                     AS SUBM_CD,
    CASE coci_subm_cd
        WHEN 'C_Outc21' THEN 2021
        WHEN 'C_Outc22' THEN 2022
        WHEN 'C_Outc23' THEN 2023
        WHEN 'C_Outc24' THEN 2024
        WHEN 'C_Outc25' THEN 2025
    END                                              AS SURVEY_YEAR,
    coci_age_at_survey                               AS AGE_AT_SURVEY,
    tpid_date_of_birth                               AS BIRTH_DATE,
    cosc_enrl_end_date                               AS ENRL_END_DATE,
    cosc_ttrain                                      AS TTRAIN
FROM surv_cohort_collection_info
WHERE coci_subm_cd IN ('C_Outc21','C_Outc22','C_Outc23','C_Outc24','C_Outc25')
ORDER BY cosc_ttrain, coci_stqu_id
