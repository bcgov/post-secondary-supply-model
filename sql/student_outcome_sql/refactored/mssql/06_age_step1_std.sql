-- =====================================================================
-- DACSO age-at-survey per respondent, standardized extraction
-- Database : PSSM2025 (dbo)
-- Replaces : qry_make_tmp_table_Age_step1.sql
-- Standard : see refactored/README.md
-- Note     : BTHDT stored as float (legacy); kept as-is, cast for safety.
-- =====================================================================
SELECT
    COCI_STQU_ID        AS STUDENT_KEY,
    COCI_SUBM_CD        AS SUBM_CD,
    COCI_AGE_AT_SURVEY  AS AGE_AT_SURVEY,
    CAST(BTHDT AS float)  AS BIRTH_DATE_NUM,
    ENDDT               AS END_DATE
FROM dbo.qry_make_tmp_table_Age_step1_raw
WHERE COCI_SUBM_CD IN ('C_Outc21','C_Outc22','C_Outc23','C_Outc24','C_Outc25')
