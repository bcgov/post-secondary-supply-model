-- =====================================================================
-- Lookup tables, standardized extraction
-- Database : INFOWARE Oracle (scratch.bcgov:1521/pdbtrn)
-- Replaces : INFOWARE_L_CIP_4DIGITS_CIP2021.sql,
--            INFOWARE_L_CIP_6DIGITS_CIP2021.sql
-- Standard : see README.md
-- Note     : Oracle table names have NO 'INFOWARE_' prefix (that prefix is
--            the PSSM/MS SQL copy name). T_YEAR_SURVEY_YEAR does not exist
--            in Oracle (it is a PSSM-side table). Run each block separately.
-- =====================================================================

-- ---- CIP 2021 4-digit classification --------------------------------
SELECT
    lcp4_cd              AS LCP4_CD,
    lcp4_digits_name     AS LCP4_DIGITS_NAME,
    lcip4_cd_with_period AS LCP4_CD_WITH_PERIOD
FROM l_cip_4digits_cip2021;

-- ---- CIP 2021 6-digit classification --------------------------------
SELECT
    lcp6_cd                 AS LCP6_CD,
    lcp6_digits_name        AS LCP6_DIGITS_NAME,
    lcip_cd_with_period     AS LCP6_CD_WITH_PERIOD,
    lcip_lcp4_cd            AS LCP4_CD,
    lcip_lcp2_cd            AS LCP2_CD,
    lcip_lcippc_cd          AS LCIP_LCIPPC_CD,
    lcip_lcippc_name        AS LCIP_LCIPPC_NAME,
    lcip_statscan_stem_cd   AS STATSCAN_STEM_CD,
    lcip_statscan_stem_group AS STATSCAN_STEM_GROUP
FROM l_cip_6digits_cip2021;

-- ---- Program master -------------------------------------------------
SELECT
    prgm_id                       AS PRGM_ID,
    prgm_first_seen_subm_cd       AS PRGM_FIRST_SEEN_SUBM_CD,
    prgm_inst_cd                  AS PRGM_INST_CD,
    prgm_inst_program_name        AS PRGM_INST_PROGRAM_NAME,
    prgm_inst_program_name_cleaned AS PRGM_INST_PROGRAM_NAME_CLEANED,
    dashboard_cpc_name            AS DASHBOARD_CPC_NAME,
    prgm_lcpc_cd                  AS PRGM_LCPC_CD,
    app_type                      AS APP_TYPE,
    prgm_ttrain_flag              AS PRGM_TTRAIN_FLAG,
    prgm_trade_program_code       AS PRGM_TRADE_PROGRAM_CODE,
    prgm_trade_program_grouping   AS PRGM_TRADE_PROGRAM_GROUPING,
    prgm_credential               AS PRGM_CREDENTIAL,
    notes                         AS NOTES,
    has_historical_prgm_id_link   AS HAS_HISTORICAL_PRGM_ID_LINK,
    old_prgmid                    AS OLD_PRGMID,
    in_current_data_extract       AS IN_CURRENT_DATA_EXTRACT,
    cip_cluster_arts_applied      AS CIP_CLUSTER_ARTS_APPLIED,
    dacso_old_prgm_id_do_not_use  AS DACSO_OLD_PRGM_ID_DO_NOT_USE,
    dup_program_use_this_prgm_id  AS DUP_PROGRAM_USE_THIS_PRGM_ID,
    sors2009_programid            AS SORS2009_PROGRAMID,
    dacso_prgm_id                 AS DACSO_PRGM_ID,
    lcip_cd_cip2021               AS LCIP_CD_CIP2021,
    lcip_name_cip2021             AS LCIP_NAME_CIP2021,
    lcip_lcippc_cd_2021           AS LCIP_LCIPPC_CD_2021,
    lcip_lcippc_name_2021         AS LCIP_LCIPPC_NAME_2021
FROM programs;
