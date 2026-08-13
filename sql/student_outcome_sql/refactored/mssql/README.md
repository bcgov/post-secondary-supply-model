# Refactored Student Outcome Survey SQL

Standardized extraction queries replacing the legacy Access-style originals in
`../`. All queries run read-only against `PSSM2025` `dbo` and share a common
column schema across the four surveys so results can be `UNION ALL`-ed.

## Naming conventions (standardized schema)

Common core columns (present in every survey output; `NULL` where the survey
has no equivalent):

| Standard name | Meaning | BGS source | TRD source | APPSO source | DACSO source |
|---|---|---|---|---|---|
| `SURVEY` | Survey label | `'BGS'` | `'TRD'` | `'APPSO'` | `'DACSO'` |
| `SUBM_CD` | Cycle code | `NULL` | `SUBM_CD` | `SUBM_CD` | `COCI_SUBM_CD` |
| `SURVEY_YEAR` | Year of cycle | `SURVEY_YEAR` | via `t_year_survey_year_r` | via `t_year_survey_year_r` | via `t_year_survey_year_r` |
| `STUDENT_KEY` | Sample/respondent key | `STQU_ID` | `KEY` | `KEY` | `COCI_STQU_ID` |
| `PEN` | Person number | `PEN` | `PEN` | `PEN` | `COCI_PEN` |
| `RESPONDENT` | Responded flag | `SRV_Y_N` | `RESPONDENT` | `RESPONDENT` | `RESPONDENT` |
| `INST` | Institution code | `INST` | `INST` | `INST` | `COCI_INST_CD` |
| `PSSM_CREDENTIAL` | Credential level | `PSSM_CREDENTIAL` | `PSSM_CREDENTIAL` | `PSSM_CREDENTIAL` | `PRGM_CREDENTIAL` |
| `LCP6_CD` | CIP 2021 6-digit | `NULL` | `LCP6_CD` | `LCP6_CD` | `LCP6_CD` |
| `LCP6_DIGITS_NAME` | CIP 6-digit name | `NULL` | lookup join | lookup join | lookup join |
| `LCP4_CD` | CIP 2021 4-digit | `CIP_CODE_4` | `LCIP_LCP4_CD` | `LCIP_LCP4_CD` | `LCP4_CD` |
| `LCP4_DIGITS_NAME` | CIP 4-digit name | lookup join | lookup join | lookup join | `LCP4_DIGITS_NAME` |
| `TTRAIN` | Training related | — | `TTRAIN` | `TTRAIN` | `TTRAIN` |
| `AGE_AT_SURVEY` | Age at survey | `AGE` | `TRD_AGE_AT_SURVEY` | `APP_AGE_AT_SURVEY` | `COCI_AGE_AT_SURVEY` |
| `CURRENT_REGION1` | Region Q1 | — | `CURRENT_REGION1` | `CURRENT_REGION1` | `TPID_CURRENT_REGION1` |
| `CURRENT_REGION4` | Region Q4 | — | `CURRENT_REGION4` | `CURRENT_REGION4` | `TPID_CURRENT_REGION4` |
| `CURRENT_REGION_PSSM_CODE` | Standard PSSM region | `CURRENT_REGION_PSSM_CODE` | `CURRENT_REGION_PSSM_CODE` | `CURRENT_REGION_PSSM_CODE` | `CURRENT_REGION_PSSM_CODE` |
| `LABR_IN_LABOUR_MARKET` | In labour market | `IN_LBR_FRC` | `TRD_LABR_IN_LABOUR_MARKET` | `APP_LABR_IN_LABOUR_MARKET` | `LABR_IN_LABOUR_MARKET` |
| `LABR_EMPLOYED` | Employed | `EMPLOYED` | `TRD_LABR_EMPLOYED` | `APP_LABR_EMPLOYED` | `LABR_EMPLOYED` |
| `LABR_UNEMPLOYED` | Unemployed | `UNEMPLOYED` | `TRD_LABR_UNEMPLOYED` | `APP_LABR_UNEMPLOYED` | `LABR_UNEMPLOYED` |
| `LABR_JOB_SEARCH_TIME_GP` | Job search time group | `NULL` | `TRD_LABR_JOB_SEARCH_TIME_GP` | `NULL` | `LABR_JOB_SEARCH_TIME_GP` |
| `LABR_JOB_TRAINING_RELATED` | Job training-related | `TRAINING_RELATED` | `TRD_LABR_JOB_TRAINING_RELATED` | `APP_LABR_JOB_TRAINING_RELATED` | `LABR_JOB_TRAINING_RELATED` |
| `NOC_CD` | NOC 2021 occupation | `NOC` | `NOC_CD` | `NOC_CD` | `LABR_OCCUPATION_LNOC_CD` |
| `NEW_LABOUR_SUPPLY` | New labour supply flag | `NEW_LABOUR_SUPPLY` | `NEW_LABOUR_SUPPLY` | `NEW_LABOUR_SUPPLY` | `NEW_LABOUR_SUPPLY` |
| `WEIGHT` | Survey weight | `WEIGHT` | `WEIGHT` | `WEIGHT` | `WEIGHT` |
| `LCIP4_CRED` | CIP4 + credential composite | `LCIP4_CRED` | `LCIP4_CRED` | `LCIP4_CRED` | `LCIP4_CRED` |

Survey-specific columns (activity, further education, graduate counts) are
appended after the core block per query.

## Files

- `01_bgs_data_std.sql` — BGS responses, cycles 2021–2025
- `02_trd_data_std.sql` — Trades responses
- `03_appso_data_std.sql` — Apprentice responses
- `04_dacso_data_std.sql` — DACSO responses
- `05_graduates_std.sql` — TRD + APPSO graduate counts (UNION)
- `06_age_step1_std.sql` — DACSO age-at-survey per respondent
- `07_lookups_std.sql` — CIP 4/6-digit, programs, year–cycle lookup

Cycle filter: BGS `SURVEY_YEAR`; TRD/APPSO `SUBM_CD`; DACSO `COCI_SUBM_CD`;
valid codes `C_Outc21` … `C_Outc25` (see `t_year_survey_year_r`).
