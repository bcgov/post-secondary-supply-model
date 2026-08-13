# Refactored Student Outcome Survey SQL — Oracle (INFOWARE)

Standardized extraction queries that **replace** the legacy originals in
`../`. These run read-only against the **INFOWARE Oracle database**
(`scratch.bcgov:1521/pdbtrn`, user `INFOWARE`, see `config.yml` →
`infoware`). A separate MS SQL port of the same schema lives in
[`mssql/`](mssql/README.md) for PSSM2025.

## Connection

```r
iw_config <- config::get("infoware")
iw_con <- dbConnect(
  odbc::odbc(),
  Driver = "Oracle in instantclient_19c",
  DBQ = iw_config$dbq,   # NOTE: if EZConnect fails, use the full
                         # (DESCRIPTION=...) connect string (see README)
  UID = iw_config$uid,
  PWD = iw_config$pwd
)
```

> **Driver note**: on some clients the EZConnect `//host:port/service`
> form fails with `ORA-12154`; the equivalent full descriptor
> `(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=scratch.bcgov)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=pdbtrn)))`
> works.

## Source tables (Oracle names, INFOWARE schema)

| Oracle table | Rows | Role |
|---|---|---|
| `TRADES_COHORT_INFO` | 68,264 | TRD sample frame (KEY, inst, age, regions, gradstat) |
| `TRADES_SHORT_RESP` | 68,264 | TRD responses (LABR_*, NOC, further-ed) |
| `APPRENTICE_COHORT_INFO` | 101,831 | APPSO sample frame |
| `APPRENTICE_SHORT_RESP` | 101,831 | APPSO responses |
| `SURV_COHORT_COLLECTION_INFO` | 596,293 | DACSO sample frame (all cycles) |
| `C_OUTC_CLEAN_SHORT_RESP` | 534,494 | DACSO responses (all cycles) |
| `C_OUTC_CLEAN2` | 534,494 | DACSO legacy column set (not needed in refactor) |
| `PROGRAMS` | — | program master |
| `L_CIP_6DIGITS_CIP2021` | 2,119 | CIP2021 6-digit + names |
| `L_CIP_4DIGITS_CIP2021` | 454 | CIP2021 4-digit + names |
| `BGS_COHORT_INFO` | 343,315 | BGS sample frame |
| `BGS_DIST_21_25` | 123,452 | BGS responses + derived indicators |
| `L_AGE_GROUPS` | — | age-band labels (not joined; bands coded in queries) |

Note: `INFOWARE_L_CIP_*` / `T_YEAR_SURVEY_YEAR` / `TRD_GRADUATES` /
`APPSO_GRADUATES` do **not** exist in Oracle — those are PSSM-side (MS SQL)
copies. Oracle graduate counts come from the response queries directly.

## Standardized output schema (identical across all four surveys)

| Standard name | Meaning | BGS source | TRD source | APPSO source | DACSO source |
|---|---|---|---|---|---|
| `SURVEY` | Survey label | `'BGS'` | `'TRD'` | `'APPSO'` | `'DACSO'` |
| `SUBM_CD` | Cycle code | `NULL` (uses `SURVEY_YEAR`) | `SUBM_CD` | `SUBM_CD` | `COCI_SUBM_CD` |
| `SURVEY_YEAR` | Year | `BGS_DIST.YEAR` | `CASE SUBM_CD` | `CASE SUBM_CD` | `CASE COCI_SUBM_CD` |
| `STUDENT_KEY` | Frame key | `STQU_ID` | `KEY` | `KEY` | `COCI_STQU_ID` |
| `PEN` | Person number | `PEN` | `PEN` | `PEN` | `COCI_PEN` |
| `RESPONDENT` | Responded | `SRV_Y_N` | `RESPONDENT` | `RESPONDENT` | `RESPONDENT` |
| `INST` | Institution code | `INST` | `INST` | `INST` | `COCI_INST_CD` |
| `INST_NAME` | Institution name | `INST_NAME` | `INST_NAME` | `INST_NAME` | `COCI_INST_NAME` |
| `PSSM_CREDENTIAL` | Credential level | `'BACH'` | `PRGM_CREDENTIAL` | `'APPR'||PRGM_CREDENTIAL` | `PRGM_CREDENTIAL` |
| `LCP6_CD` | CIP2021 6-digit | `NULL` | via `PROGRAMS` | via `PROGRAMS` | via `PROGRAMS` |
| `LCP6_DIGITS_NAME` | CIP6 name | `NULL` | lookup join | lookup join | lookup join |
| `LCP4_CD` | CIP2021 4-digit | `CIP_4DIGIT_NO_PERIOD` | lookup join | lookup join | lookup join |
| `LCP4_DIGITS_NAME` | CIP4 name | lookup join | lookup join | lookup join | lookup join |
| `TTRAIN` | Training related | `NULL` | `TTRAIN` | `TTRAIN` | `TTRAIN` |
| `AGE_AT_SURVEY` | Age at survey | `AGE` | `TRD_AGE_AT_SURVEY` | `APP_AGE_AT_SURVEY` | `COCI_AGE_AT_SURVEY` |
| `AGE_GROUP` | Age band (source-coded) | `AGE_RNG_AGGR` | `TRD_AGE_AT_SURVEY_GRP` | `APP_AGE_AT_SURVEY_GRP` | `COCI_AGE_GROUP` |
| `CURRENT_REGION1` | Region (spring) | `NULL` | `CURRENT_REGION1` | `CURRENT_REGION1` | `TPID_CURRENT_REGION1` |
| `CURRENT_REGION4` | Region (fall) | `NULL` | `CURRENT_REGION4` | `CURRENT_REGION4` | `TPID_CURRENT_REGION4` |
| `LABR_IN_LABOUR_MARKET` | In labour market | `LBR_FRC_LABOUR_MARKET` | `TRD_LABR_IN_LABOUR_MARKET` | `APP_LABR_IN_LABOUR_MARKET` | `LABR_IN_LABOUR_MARKET` |
| `LABR_EMPLOYED` | Employed (Q18) | `LBR_FRC_CURRENTLY_EMPLOYED` | `Q18` | `Q18` | `Q18` |
| `LABR_UNEMPLOYED` | Unemployed | `LBR_FRC_UNEMPLOYED` | `TRD_LABR_UNEMPLOYED` | `APP_LABR_UNEMPLOYED` | `LABR_UNEMPLOYED` |
| `LABR_JOB_SEARCH_TIME_GP` | Job search time | `NULL` | `TRD_LABR_JOB_SEARCH_TIME_GP` | `NULL` | `LABR_JOB_SEARCH_TIME_GP` |
| `LABR_JOB_TRAINING_RELATED` | Training-related job | `E10_IN_TRAINING_RELATED_JOB` | `TRD_LABR_JOB_TRAINING_RELATED` | `APP_LABR_JOB_TRAINING_RELATED` | `LABR_JOB_TRAINING_RELATED` |
| `NOC_CD` | NOC 2021 occupation | `NOC_2021` | `NOC_CD_2021` | `NOC_CD_2021` | `LABR_OCC_LNOC_2021_CD` |
| `GRADSTAT_GROUP` | Graduation status group | `NULL` | `CASE GRADSTAT '2'→'3'` | `NULL` | `CASE COSC_GRAD_STATUS_LGDS_CD '2'→'3'` |

Columns that the PSSM R pipeline derives downstream (weights, new labour
supply, age-band labels) are **not** faked with `0 AS` placeholders —
they are omitted here and documented in
[`../TABLE_COLUMN_INVENTORY.md`](../TABLE_COLUMN_INVENTORY.md).

## Fixes vs originals

- Access-style nested `RIGHT JOIN (...)` chains replaced with flat `LEFT JOIN` order from the frame table (same result set).
- `0 AS New_Labour_Supply / Weight / Age_Group` placeholders dropped (R computes real values).
- `Expr1` (Access auto-name) → `GRAD_COUNT`; `Private` → `PRIVATE_FLAG`.
- Commented-out columns (`noc_cd`, `age_group_label`, `LCIP4_CRED`) either restored or documented.
- Added columns that exist in Oracle but were missing (per-survey extras: `GENDER`, `LRST_CD`, `SURVEY_MODE`, `CURRENT_REGION2/3`, further-ed flags, `NOC_NAME_2021`, salaries).
- Cycle filter standardized: `IN ('C_Outc21'..'C_Outc25')` for TRD/APPSO/DACSO; `YEAR` between 2021 and 2025 for BGS.

## Files

- `01_bgs_data_std.sql` — BGS responses 2021–2025 (BGS_COHORT_INFO × BGS_DIST_21_25 + CIP4 names)
- `02_trd_data_std.sql` — Trades responses (TRADES_COHORT_INFO × TRADES_SHORT_RESP × PROGRAMS × CIP)
- `03_appso_data_std.sql` — Apprentice responses (same shape)
- `04_dacso_data_std.sql` — DACSO responses (SURV_COHORT_COLLECTION_INFO × PROGRAMS × CIP × C_OUTC_CLEAN_SHORT_RESP)
- `05_graduates_std.sql` — TRD + APPSO graduate counts (UNION, age-band labels coded)
- `06_age_step1_std.sql` — DACSO age-at-survey per respondent
- `07_lookups_std.sql` — CIP4 / CIP6 / PROGRAMS lookups (three standalone blocks)

## Validation (read-only against Oracle)

All queries executed successfully against the INFOWARE Oracle database.
Row counts match the PSSM2025 MS SQL copies exactly (confirming data parity):

| Query | Rows | Cols |
|---|---|---|
| `01_bgs_data_std.sql` | 123,452 | 35 |
| `02_trd_data_std.sql` | 22,066 | 41 |
| `03_appso_data_std.sql` | 23,166 | 40 |
| `04_dacso_data_std.sql` | 147,020 | 44 |
| `05_graduates_std.sql` | 1,004 | 8 |
| `06_age_step1_std.sql` | 147,020 | 7 |
| `07_lookups_std.sql` — CIP4 | 454 | 3 |
| `07_lookups_std.sql` — CIP6 | 2,119 | 9 |
| `07_lookups_std.sql` — PROGRAMS | 6,736 | 25 |

> **Graduates note**: the original `Q000_TRD_Graduates.sql` / `APPSO_Graduates.sql`
> returned graduate counts for **all** survey cycles (no `WHERE subm_cd`
> filter). The refactored query filters to `C_Outc21`–`C_Outc25` for
> consistency with the data queries; widen the `IN (...)` list if
> benchmark cycles (2019–2023) are needed.
