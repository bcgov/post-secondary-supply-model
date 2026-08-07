# Student Outcome Survey Data — Schema Diff 2025 (2027 LAN refresh)

Applies to the 2027 data refresh: the student-outcome survey CSVs moved from the 2022-2023 LAN folder (`{lan_2023}`) to the 2027 folder (`{lan}`), and the loading scripts were updated to match. This report documents every intentional column change, old CSV vs new CSV, cross-checked against the new LAN `sql/` query definitions and `data_dictionary_so.csv` as ground truth.

Path shorthand: `{lan}` = new LAN root; `{lan_2023}` = old LAN root. All survey files live at `{lan}/data/student-outcomes/csv/` (the old `so-provision/` subfolder is gone).

## Layout changes (all cohorts)

| Change | Old | New |
|---|---|---|
| Survey CSV folder | `data/student-outcomes/csv/so-provision/` | `data/student-outcomes/csv/` (no subfolder) |
| BGS survey file | `BGS_Q001_BGS_Data_2019_2023.csv` | `BGS_Q001_BGS_Data_2021_2025.csv` |
| CIP lookup version | `INFOWARE_L_CIP_*_CIP2016.csv` | `INFOWARE_L_CIP_*_CIP2021.csv` |
| Age table | per-year `qry_make_tmp_table_Age_step1_20xx.csv` | single `qry_make_tmp_table_Age_step1.csv` |
| Survey year codes (`SUBM_CD`) | `C_Outc19` … `C_Outc23` | `C_Outc21` … `C_Outc25` |

## BGS (Baccalaureate Graduate Survey)

`BGS_Q001_BGS_Data_2021_2025.csv` vs `BGS_Q001_BGS_Data_2019_2023.csv`, per `sql/BGS_Q001_BGS_Data_2021_2025.sql`.

| Column | Change | Action in R |
|---|---|---|
| `D03_STUDYING_FT` | **removed** from new file | drop `FULL_TM_SCHOOL` rename in `load-cohort-bgs.R` |
| `D02_R1_CURRENTLY_STUDYING` | renamed → `PFST_CURRENTLY_STUDYING` | select out `PFST_CURRENTLY_STUDYING` instead |
| `STUDID` | type change: numeric → character (16,090 `V`-prefixed UVIC rows) | none needed (readr infers character) |
| `PEN` | still numeric-parsed; 4 leading-zero rows only | keep existing casts |
| `NOC_CD` | character, `"XXXXX"` × 2,013 | none |
| `AGE` | 210 blanks | none (existing `between(AGE, 17, 34)` handles NA) |
| All other columns | unchanged names/order (32 → 31 cols) | — |

BGS renames (`FULL_TM_WRK`, `IN_LBR_FRC`, `EMPLOYED`, `UNEMPLOYED`, `TRAINING_RELATED`, `TOOK_FURTH_ED`) are **not** pre-applied in the new SQL — the R rename block stays, minus `FULL_TM_SCHOOL`.

Note: `data_dictionary_so.csv` currently has no rows for the 2021_2025 BGS file — dictionary update pending with the survey team.

## TRD (Trades)

`Q000_TRD_DATA_01.csv` + `Q000_TRD_Graduates.csv`, per the matching `sql/` files.

| File | Change |
|---|---|
| `Q000_TRD_DATA_01.csv` | 23 cols, one rename: `LCIP_CD` → `LCP6_CD` (CIP2021). Types unchanged — `GRADSTAT`/`KEY`/`TTRAIN` numeric coercions in R still valid. `SUBM_CD` now `C_Outc21..25` (was 19..23). `PEN` now >1e9 (10-digit, exact as double). |
| `Q000_TRD_Graduates.csv` | identical 6 columns. `SUBM_CD` range `C_Outc13..26` (was 13..24 — adds 25, 26). |

## APPSO (Apprenticeship)

`APPSO_Data_01_Final.csv` + `APPSO_Graduates.csv` (note: filename casing `APPSO_Data_01_Final` vs old `APPSO_DATA_01_Final`).

| File | Change |
|---|---|
| `APPSO_Data_01_Final.csv` | one rename: `LCIP_CD` → `LCP6_CD`. Types unchanged. `SUBM_CD` `C_Outc21..25` (was 19..23); shared years row-identical, minus 19/20 plus 24/25 (+8,869 rows). `RESPONDENT` numeric 0/1 in both. `APP_TIME_TO_FIND_EMPLOY_MJOB` 100% empty (pre-existing). |
| `APPSO_Graduates.csv` | identical columns; adds `C_Outc25`, `C_Outc26` (data file lags one year, as before). |

**Weight mapping** — the hardcoded `SUBM_CD` weight case_whens in `load-cohort-appso.R` were remapped for the new codes: `C_Outc21→1 … C_Outc25→5` (regular) and QI variant shifted `C_Outc21→2 … C_Outc25→0`. Without the remap the new codes hit `TRUE ~ 0` and ~38% of rows would get zero weight. Exact weight values still pending analyst sign-off.

## DACSO (Diploma/Associate/Certificate)

`DACSO_Q003_DACSO_DATA_Part_1_stepA.csv` + `INFOWARE_C_OutC_Clean_Short_Resp.csv`.

| File | Change |
|---|---|
| `DACSO_Q003_DACSO_DATA_Part_1_stepA.csv` | two renames: `LCIP_CD` → `LCP6_CD`, `LCP4_CIP_4DIGITS_NAME` → `LCP4_DIGITS_NAME` (CIP2016 → CIP2021 values). No type drift. Rows 136,675 → 147,020. `SUBM_CD` `C_Outc21..25`. Note: columns `LABR_EMPLOYED`, `COSC_GRAD_STATUS_LGDS_CD(_GROUP)`, `RESPONDENT` exist with real values — the commented-out NA-mutate block stays commented. |
| `INFOWARE_C_OutC_Clean_Short_Resp.csv` | 16 columns, identical. Mixed quoting (75,855 quoted CIP names) — readr-safe. |

## Age table

`qry_make_tmp_table_Age_step1.csv` — single file replacing the per-year glob in `load-outcomes-data.R`. Header: `COCI_STQU_ID, COCI_SUBM_CD, BTHDT, ENDDT, COCI_AGE_AT_SURVEY`; 147,020 rows; `C_Outc21..25`. Column spec `"dcdcd"` remains valid (ENDDT read as character, lossless).

## Lookups (root-swap only)

All lookups referenced by the loading scripts exist at the same relative path on the new LAN (`development/csv/gh-source/lookups/02/`, `rollover/02/`). One case-only rename: `t_year_survey_year.csv` → `T_Year_Survey_Year.csv` (applied in `load-cohort-dacso.R`). Lookup content/schema verification deferred to a later branch.

## Verdict

No unexpected column-type breaks beyond `STUDID` (BGS) and the two CIP2021 renames. All R-side type coercions remain valid; two BGS references (removed column, renamed column) were fixed in `load-cohort-bgs.R`.
