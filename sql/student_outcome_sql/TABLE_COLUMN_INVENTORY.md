# Student Outcome Survey — Table & Column Inventory

The original extraction queries in this folder run against the **INFOWARE
Oracle database** (`scratch.bcgov:1521/pdbtrn`). The tables below are the
**PSSM2025 MS SQL copies** (loaded by `R/load-cohort-*.R`); Oracle source
table names differ (no `INFOWARE_` prefix; see [`refactored/README.md`](refactored/README.md)
for the Oracle table map). Standardized replacements:
- [`refactored/`](refactored/) — Oracle-native queries (primary replacement)
- [`refactored/mssql/`](refactored/mssql/) — MS SQL port for PSSM2025

## Overview

| Table | Rows | Type | Produced by | Consumed by (R) |
|---|---|---|---|---|
| `t_bgs_data_final_r` | 123,452 | BGS response (final, cleaned) | `load-cohort-bgs.R` from BGS CSV | `load-so-survey-eda-data.R`, `02a-bgs-program-matching.R`, EDA report |
| `INFOWARE_BGS_COHORT_INFO` | 343,315 | BGS sample/cohort master (contact + demographics) | Survey team (Access export) | `load-cohort-bgs.R` (source), BGS extraction SQL |
| `INFOWARE_BGS_DIST_21_25` | 123,452 | BGS survey responses + derived indicators (2021–2025) | Survey team | `load-cohort-bgs.R` (source), BGS extraction SQL |
| `trd_data_r` | 22,066 | Trades (TRD) response, final | `load-cohort-trd.R` from `Q000_TRD_DATA_01` CSV | `load-so-survey-eda-data.R`, `02a-*` |
| `t_appso_data_final_r` | 23,166 | Apprentice (APPSO) response, final | `load-cohort-appso.R` from `APPSO_Data_01` CSV | `load-so-survey-eda-data.R`, `02a-*` |
| `t_dacso_data_part_1_stepa_r` | 147,020 | DACSO response (step A), final | `load-cohort-dacso.R` from `DACSO_Q003` CSV | `load-so-survey-eda-data.R`, `02a-dacso-program-matching.R` |
| `INFOWARE_C_OutC_Clean_Short_Resp_raw` | 147,020 | DACSO short-response rows (source of DACSO join) | Survey team | `load-cohort-dacso.R`, DACSO extraction SQL |
| `INFOWARE_PROGRAMS` | 6,736 | Program master (PRGM_ID → inst/program/credential/CIP) | Survey team | `load-infoware-lookups.R`, all `02a-*` matching |
| `INFOWARE_L_CIP_6DIGITS_CIP2021` | 2,119 | CIP 2021 6-digit classification + names + STEM flags | Survey team | `load-outcomes-data.R`, `load-so-survey-eda-data.R` (CIP6 names) |
| `INFOWARE_L_CIP_4DIGITS_CIP2021` | 454 | CIP 2021 4-digit classification + names | Survey team | `load-so-survey-eda-data.R` (CIP4 names), BGS L tables |
| `t_year_survey_year_r` | 82 | SUBM_CD → survey year / school year / projection year | `load-*` year loader | `load-cohort-*.R`, EDA report |
| `t_noc_broad_categories_r` | 517 | NOC 2021 unit group → broad category / TEER | `load-*` | EDA report section G |
| `t_bgs_inst_recode_r` | 13 | BGS institution recode map (INST → INST_RECODE) | `load-*` | EDA report (institution groups) |
| `trd_graduates_r` | 1,208 | TRD graduate counts by age/credential/cycle | `load-cohort-trd.R` from `Q000_TRD_Graduates` CSV | `load-so-survey-eda-data.R`, EDA report |
| `appso_graduates_r` | 2,468 | APPSO graduate counts by age/credential/cycle | `load-cohort-appso.R` from `APPSO_Graduates` CSV | `load-so-survey-eda-data.R`, EDA report |
| `Q000_TRD_Graduates_raw` | 1,208 | TRD graduates, raw extraction | Survey team SQL `Q000_TRD_Graduates.sql` | `load-cohort-trd.R` (source) |
| `APPSO_Graduates_raw` | 2,468 | APPSO graduates, raw extraction | Survey team SQL `APPSO_Graduates.sql` | `load-cohort-appso.R` (source) |
| `qry_make_tmp_table_Age_step1_raw` | 147,020 | DACSO age-at-survey per respondent | Survey team SQL `qry_make_tmp_table_Age_step1.sql` | DACSO load pipeline |

Notes:
- TRD/APPSO/DACSO flow source tables (`trades_cohort_info`, `trades_short_resp`, `apprentice_cohort_info`, `apprentice_short_resp`, `surv_cohort_collection_info`, `c_outc_clean_short_resp`, `programs`, `l_cip_*` — Access-era names) do **not** exist in `PSSM2025` `dbo`; only materialized results (`trd_data_r`, `t_appso_data_final_r`, `t_dacso_data_part_1_stepa_r`, `*_raw`) are loaded. BGS flow tables (`INFOWARE_BGS_COHORT_INFO`, `INFOWARE_BGS_DIST_*`) do exist.
- Cycle scopes: BGS filtered `SURVEY_YEAR %in% 2024:2025` (new pool) / `2021:2023` (benchmark); TRD/APPSO/DACSO filtered by `SUBM_CD` / `COCI_SUBM_CD` `IN ('C_Outc24','C_Outc25')` (or `C_Outc21..23`).

---

## 1. BGS — `t_bgs_data_final_r` (30 cols, one row per sample record)

| Column | Type | Meaning / downstream use |
|---|---|---|
| `PSSM_CREDENTIAL` | varchar | Credential level (BACH etc.) — used in L tables |
| `PEN` | float | Personal education number (anonymized key) |
| `STUDID` | varchar | Student ID |
| `STQU_ID` | float | Sample/response questionnaire ID (join key to cohort info) |
| `SRV_Y_N` | float | Surveyed flag |
| `SURVEY_YEAR` | float | Cycle year — pool filter, EDA tables |
| `INST` | varchar | Institution code — recoded via `t_bgs_inst_recode_r` |
| `PROGRAM` | varchar | Program name |
| `CIP_CODE_4` | varchar | CIP 2021 4-digit (maps to `INFOWARE_L_CIP_4DIGITS_CIP2021.LCP4_CD`) |
| `CIP_CODE_2` | varchar | CIP 2-digit |
| `LCIP_LCIPPC_CD` | float | CIP post-secondary cluster code |
| `LCIP4_CRED` | varchar | CIP4 + credential composite key |
| `CURRENT_ACTIVITY` | float | Post-survey current activity |
| `FULL_TM_WRK` | float | Full-time work flag |
| `IN_LBR_FRC` | float | In labour force |
| `EMPLOYED` | float | Employed |
| `UNEMPLOYED` | float | Unemployed |
| `TRAINING_RELATED` | float | Training-related employment |
| `NOC` | varchar | NOC 2021 occupation code |
| `AGE` | float | Age at survey |
| `AGE_GROUP` / `AGE_GROUP_ROLLUP` | float | Age bands (counts tables) |
| `NEW_LABOUR_SUPPLY` | float | New labour supply flag (weights) |
| `WEIGHT` | float | Survey weight |
| `WEIGHT_CIP` | float | CIP-level weight |
| `TOOK_FURTH_ED` | float | Took further education |
| `AGE_17_34` | float | Youth age flag |
| `OLD_LABOUR_SUPPLY` | bit | Legacy labour-supply flag |
| `CURRENT_REGION_PSSM_CODE` | float | Standard PSSM region (1–11, -1) |
| `CURRENT_REGION_PSSM` | varchar | Region name |

### `INFOWARE_BGS_COHORT_INFO` (112 cols) — source/master, only a subset loaded downstream

Sample/contact master. Columns used by the project pipeline: `STQU_ID`, `INST`, `INST_NAME`, `INST_SHORT_NAME`, `STUDID`, `PEN`, `CIP_6DIGIT_1`, `CIP6DIG_NAME`, `CIP_4DIGIT_NO_PERIOD`, `CIP4DIG`, `CIP2DIG`, `PROGRAM`, `DASHBOARD_PROGRAM`, `CPC`, `COOP`, `GENDER`, `INTERNATIONAL`, `BRTHMNTH`, `BRTHYEAR`, `STATUS`, `SRV_Y_N`, `S_MODE`, `SURVEY_DATE`, `SUBM_CD`. Remainder are contact/address/phone/email fields (e.g. `MAIL_*`, `PERM_*`, `WORK_*`, `CONT_*`, `EMAIL*`, `*_TEL`) used only for survey administration, not analysis.

### `INFOWARE_BGS_DIST_21_25` (201 cols) — response + derived indicators

Raw responses (`A01_R1`, `B03_R1`, `C01`…`C25B`, `D01_R1`, `D06_R2`, `E01_R1`…`E31`, `FF01/FF02`, `F02_R2`, `F03_R2`, `G02C_*`, `G08A`), demographics (`GENDER`, `AGE`, `AGE_NEW`, `BRTHMNTH/YEAR`), derived flags (`LBR_FRC_LABOUR_MARKET`, `LBR_FRC_CURRENTLY_EMPLOYED`, `LBR_FRC_UNEMPLOYED`, `LBR_FRC_NOT_IN_LABOUR_MARKET`, `E10_IN_TRAINING_RELATED_JOB`, `D01_TAKEN_FURTHER_STUDIES`, `PFST_CURRENTLY_STUDYING`, `*_BASE_VALID_RESPONSES`, `*_YES_NO`), geography (`STUDY_REGION`, `CUR_RES`, `CURRENT_REGION`, `REGION_CD`), NOC 2021 (`NOC_2021`, `NOC_2021_NAME`, `NOC_2021_TEER`, `NOC_2021_SKL_TYPE`), income/employment (`TOTL_SAL`, `INCOME`, `FULL_TM`, `TOTL_SAL_FULL_TM`, `LABR_MJOB_HOURLY_*`, `SAL_RNG_J`), institution/program detail (`DEPT_*`, `FACULTY_*`, `DEGREE_TYPE_*`). `t_bgs_data_final_r` is the R-side cleaned projection of the subset the pipeline uses.

---

## 2. Trades — `trd_data_r` (25 cols)

| Column | Type | Standardized name (refactored) |
|---|---|---|
| `SURVEY` | varchar | `SURVEY` (='TRD') |
| `TRD_AGE_AT_SURVEY` | float | `AGE_AT_SURVEY` |
| `GRADSTAT` / `GRADSTAT_GROUP` | float | `GRADSTAT` / `GRADSTAT_GROUP` |
| `KEY` | float | `STUDENT_KEY` |
| `PEN` | float | `PEN` |
| `SUBM_CD` | varchar | `SUBM_CD` (cycle `C_Outc24/25` = 2024/2025 via `t_year_survey_year_r`) |
| `RESPONDENT` | float | `RESPONDENT` |
| `INST` | varchar | `INST` |
| `PSSM_CREDENTIAL` | varchar | `PSSM_CREDENTIAL` |
| `LCP6_CD` | varchar | `LCP6_CD` (maps to `INFOWARE_L_CIP_6DIGITS_CIP2021.LCP6_CD`) |
| `LCIP_LCP4_CD` | varchar | `LCP4_CD` (maps to `INFOWARE_L_CIP_4DIGITS_CIP2021.LCP4_CD`) |
| `TTRAIN` | float | `TTRAIN` |
| `CURRENT_REGION1` / `CURRENT_REGION4` | float | `CURRENT_REGION1` / `CURRENT_REGION4` |
| `TRD_LABR_IN_LABOUR_MARKET` | float | `LABR_IN_LABOUR_MARKET` |
| `TRD_LABR_EMPLOYED` | float | `LABR_EMPLOYED` |
| `TRD_LABR_UNEMPLOYED` | float | `LABR_UNEMPLOYED` |
| `TRD_LABR_JOB_SEARCH_TIME_GP` | float | `LABR_JOB_SEARCH_TIME_GP` |
| `TRD_LABR_JOB_TRAINING_RELATED` | float | `LABR_JOB_TRAINING_RELATED` |
| `NOC_CD` | varchar | `NOC_CD` |
| `NEW_LABOUR_SUPPLY` | float | `NEW_LABOUR_SUPPLY` |
| `WEIGHT` | float | `WEIGHT` |
| `LCIP4_CRED` | varchar | `LCIP4_CRED` |
| `CURRENT_REGION_PSSM_CODE` | float | `CURRENT_REGION_PSSM_CODE` |

---

## 3. Apprentice — `t_appso_data_final_r` (25 cols)

Same shape as TRD with `APP_*` prefix: `SURVEY`, `APP_AGE_AT_SURVEY`, `KEY`, `PEN`, `SUBM_CD`, `RESPONDENT`, `INST`, `CURRENT_REGION1/4`, `PSSM_CREDENTIAL`, `LCP6_CD`, `LCIP_LCP4_CD`, `TTRAIN`, `APP_LABR_IN_LABOUR_MARKET`, `APP_LABR_EMPLOYED`, `APP_LABR_UNEMPLOYED`, `APP_TIME_TO_FIND_EMPLOY_MJOB` (bit), `APP_LABR_JOB_TRAINING_RELATED`, `NOC_CD`, `LCIP4_CRED`, `NEW_LABOUR_SUPPLY`, `WEIGHT`, `CURRENT_REGION_PSSM_CODE`, `AGE_GROUP_LABEL`, `AGE_GROUP`. No `LABR_JOB_SEARCH_TIME_GP` column (survey difference).

---

## 4. DACSO — `t_dacso_data_part_1_stepa_r` (36 cols)

| Column | Type | Standardized name |
|---|---|---|
| `COCI_PEN` | float | `PEN` |
| `COCI_STQU_ID` | float | `STUDENT_KEY` |
| `COCI_SUBM_CD` | varchar | `SUBM_CD` |
| `COCI_LRST_CD` | varchar | `LRST_CD` (response status; `000` = respondent) |
| `COCI_INST_CD` | varchar | `INST` |
| `PFST_CURRENT_ACTIVITY` | float | `CURRENT_ACTIVITY` |
| `LCIP_LCIPPC_NAME` | varchar | `LCIP_LCIPPC_NAME` |
| `LCP6_CD` | varchar | `LCP6_CD` |
| `LCP4_CD` / `LCP4_DIGITS_NAME` | varchar | `LCP4_CD` / `LCP4_DIGITS_NAME` |
| `TTRAIN` | float | `TTRAIN` |
| `TPID_CURRENT_REGION1/4` | float | `CURRENT_REGION1` / `CURRENT_REGION4` |
| `TPID_LGND_CD` | float | (legal region detail) |
| `LABR_*` (6 cols incl. `LABR_EMPLOYED_FULL_PART_TIME`, `LABR_OCCUPATION_LNOC_CD`) | float/varchar | `LABR_*`, `NOC_CD` |
| `COCI_AGE_AT_SURVEY` | float | `AGE_AT_SURVEY` |
| `AGE_GROUP` / `AGE_GROUP_ROLLUP` | float | as-is |
| `COSC_GRAD_STATUS_LGDS_CD(_GROUP)` | float | `GRADSTAT_GROUP` |
| `RESPONDENT` | float | `RESPONDENT` |
| `NEW_LABOUR_SUPPLY` / `OLD_LABOUR_SUPPLY` / `WEIGHT` | float | as-is |
| `HAD_PREVIOUS_CREDENTIAL`, `PFST_IN_POST_SEC_BEFORE`, `PFST_HAD_PREVIOUS_CDTL`, `PFST_FURSTDY_INCL_STILL_ATTD` | float | further-education flags |
| `PRGM_CREDENTIAL` | varchar | `PSSM_CREDENTIAL` |
| `CURRENT_REGION_PSSM_CODE` | float | as-is |

### `INFOWARE_C_OutC_Clean_Short_Resp_raw` (16 cols)
Short-response side of DACSO (join key `COCI_STQU_ID` = `STQU_ID`): `STQU_ID`, `SUBM_CD`, `INST_CD`, `PRGM_ID`, `LRST_CD`, `TTRAIN`, `Q08`, `FINAL_DISPOSITION`, `RESPONDENT`, `CREDENTIAL_DERIVED`, `STARTMONTH/YEAR`, `ENDMONTH/YEAR`, `PFST_FURSTDY_INCL_STILL_ATTD`, `PFST_HAD_PREVIOUS_CDTL`.

### `qry_make_tmp_table_Age_step1_raw` (5 cols)
`COCI_STQU_ID` (float), `COCI_SUBM_CD` (varchar), `BTHDT` (float — note: birth date stored as float), `ENDDT` (varchar), `COCI_AGE_AT_SURVEY` (float). Feeds age-at-survey derivation for DACSO.

---

## 5. Lookup / supporting tables

### `INFOWARE_L_CIP_6DIGITS_CIP2021` (9 cols)
`LCP6_CD` (key), `LCP6_DIGITS_NAME`, `LCIP_CD_WITH_PERIOD`, `LCIP_LCP4_CD`, `LCIP_LCP2_CD`, `LCIP_LCIPPC_CD`, `LCIP_LCIPPC_NAME`, `LCIP_STATSCAN_STEM_CD`, `LCIP_STATSCAN_STEM_GROUP`.

### `INFOWARE_L_CIP_4DIGITS_CIP2021` (3 cols)
`LCP4_CD` (key), `LCP4_DIGITS_NAME`, `LCIP4_CD_WITH_PERIOD`. Used for BGS CIP4 names (BGS has no CIP6).

### `INFOWARE_PROGRAMS` (25 cols)
`PRGM_ID` (key), `PRGM_FIRST_SEEN_SUBM_CD`, `PRGM_INST_CD`, `PRGM_INST_PROGRAM_NAME(_CLEANED)`, `DASHBOARD_CPC_NAME`, `PRGM_LCPC_CD`, `APP_TYPE`, `PRGM_TTRAIN_FLAG`, `PRGM_TRADE_PROGRAM_CODE(_GROUPING)`, `PRGM_CREDENTIAL`, `NOTES`, `HAS_HISTORICAL_PRGM_ID_LINK`, `OLD_PRGMID`, `IN_CURRENT_DATA_EXTRACT`, `CIP_CLUSTER_ARTS_APPLIED`, `DACSO_OLD_PRGM_ID_DO_NOT_USE`, `DUP_PROGRAM_USE_THIS_PRGM_ID`, `SORS2009_PROGRAMID`, `DACSO_PRGM_ID`, `LCIP_CD_CIP2021`, `LCIP_NAME_CIP2021`, `LCIP_LCIPPC_CD_2021`, `LCIP_LCIPPC_NAME_2021`.

### `t_year_survey_year_r` (6 cols)
`SURVEY`, `YEAR_CODE`, `SUBM_CD`, `SURVEY_YEAR`, `AWARD_SCHOOL_YEAR`, `PROJECTION_YEAR`. Maps `C_Outc21..25` cycle codes to years; also projection-year mapping.

### `t_noc_broad_categories_r` (12 cols)
`UNIT_GROUP_CODE`, `ENGLISH_NAME`, `BROAD_CATEGORY_CODE`, `BROAD_CATEGORY_ENGLISH_NAME`, `TEER_CATEGORY(_DESCRIPTION)`, `MAJOR_GROUP_CODE`, `SUB_MAJOR_GROUP_CODE`, `MINOR_GROUP_CODE`, `MAJOR_GROUP_ENGLISH_NAME`, `SUB_MAJOR_ENGLISH_NAME`, `MINOR_GROUP_ENGLISH_NAME`. Used for occupation category tables.

### `t_bgs_inst_recode_r` (2 cols)
`INST` (code), `INST_RECODE` (standard grouping). 13 institutions.

### `trd_graduates_r` / `appso_graduates_r` (7–8 cols)
`SURVEY`, `PRIVATE` (count flag), `PSSM_CREDENTIAL`, `SUBM_CD`, `TRD_AGE_AT_SURVEY`/`APP_AGE_AT_SURVEY`, `EXPR1` (graduate count — Access placeholder name!), `AGE_GROUP_LABEL` (added in R; TRD) / `AGE_GROUP` (APPSO). Raw twins `Q000_TRD_Graduates_raw` / `APPSO_Graduates_raw` are identical minus the R-added label column.

---

## 6. `INTERNATIONAL` flag (cross-survey)

All four surveys carry an international-student indicator. The column name
and source table differ per survey; the refactored Oracle queries
(`refactored/01..04_*_std.sql`) standardize the output to `INTERNATIONAL`.

| Survey | Oracle source table | Source column | Data type | Standardized name |
|---|---|---|---|---|
| BGS | `BGS_DIST_21_25` | `INTERNATIONAL` | NUMBER | `INTERNATIONAL` |
| TRD | `TRADES_COHORT_INFO` | `INTERNATIONAL` | VARCHAR2 | `INTERNATIONAL` |
| APPSO | `APPRENTICE_COHORT_INFO` | `INTERNATIONAL` | VARCHAR2 | `INTERNATIONAL` |
| DACSO | `SURV_COHORT_COLLECTION_INFO` | `COSC_INTERNATIONAL` | VARCHAR2 | `INTERNATIONAL` |

Note: the BGS column also exists on `BGS_COHORT_INFO` (NUMBER) and
`BGS_DIST_21_25` (NUMBER + `INTERNATIONAL_NAME` VARCHAR2 for the label).
The TRD/APPSO `INTERNATIONAL` columns live on the cohort (frame) tables, not
the short-response tables. The DACSO equivalent is prefixed `COSC_` on the
surv-cohort table. Neither the PSSM2025 `_r` tables nor the original
extraction queries carried this column — it is newly added to the refactored
queries for downstream analysis.

---

## 7. Legacy issues found in original SQL (fixed in `refactored/`)

- Access-style unqualified table names (`trades_cohort_info`, `l_cip_*`, …) — not resolvable in `PSSM2025`; refactored queries target the materialized tables that exist.
- Placeholder columns `0 AS ...` (e.g. `0 AS New_Labour_Supply`, `0 AS Weight`, `0 AS Age_Group`) — computed or dropped.
- `Expr1` (Access auto-name) for graduate counts — renamed `GRAD_COUNT`.
- Survey-specific labour prefixes (`TRD_LABR_*`, `APP_LABR_*`) and bare `LABR_*` — unified to `LABR_*`.
- Key joins on floats (`STQU_ID`, `COCI_STQU_ID`) — explicit `CAST` to varchar for deterministic joins.
- `||` concatenation (Access) — replaced with SQL Server `CONCAT()`.
- Per-survey cycle filters differ — standardized `SUBM_CD IN ('C_Outc21'..'C_Outc25')` / `SURVEY_YEAR` filter, joinable to `t_year_survey_year_r` for year-level outputs.
