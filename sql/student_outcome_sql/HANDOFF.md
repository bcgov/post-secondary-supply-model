# Handoff: Student Outcome Survey SQL Refactor (INFOWARE Oracle)

> **Scope**: Refactor the 10 legacy Access-style SQL queries in
> `sql/student_outcome_sql/` into standardized, Oracle-native extraction
> queries with consistent cross-survey column naming, plus an R loader
> script wrapping them as dbplyr tbl objects. This is **separate from** the
> EDA wayfinder ("Response rate check: benchmark vs new pool") — the EDA
> reads from PSSM2025 MS SQL copies; this effort targets the INFOWARE
> Oracle source directly.

---

## What's done

### Oracle schema mapped
- Connected to INFOWARE Oracle (`scratch.bcgov:1521/pdbtrn`, user INFOWARE).
  EZConnect `//host:port/service` fails with ORA-12154 on this client; full
  TNS descriptor works (see `refactored/README.md`).
- ALL_TAB_COLUMNS pulled for all 13 source tables (temp:
  `oracle-inv-out.txt`, `oracle-extra-out.txt`). Row counts confirmed:
  TRADES_COHORT_INFO 68,264 / TRADES_SHORT_RESP 68,264 /
  APPRENTICE_COHORT_INFO 101,831 / APPRENTICE_SHORT_RESP 101,831 /
  SURV_COHORT_COLLECTION_INFO 596,293 / C_OUTC_CLEAN_SHORT_RESP 534,494 /
  C_OUTC_CLEAN2 534,494 / BGS_COHORT_INFO 343,315 / BGS_DIST_21_25 123,452.
- Confirmed NOT in Oracle (PSSM/MS SQL only): `INFOWARE_L_CIP_*`,
  `T_YEAR_SURVEY_YEAR`, `TRD_GRADUATES`, `APPSO_GRADUATES`, `TBL_AGE_GROUPS`
  (actual = `L_AGE_GROUPS`).

### 7 Oracle refactored SQL queries written + validated
Files in `sql/student_outcome_sql/refactored/`:

| File | Replaces | Rows | Cols | Notes |
|---|---|---|---|---|
| `01_bgs_data_std.sql` | `BGS_Q001_BGS_Data_2021_2025.sql` | 123,452 | 36 | BGS_COHORT_INFO × BGS_DIST_21_25 + CIP4 names; `TO_CHAR(d.international)`, `CAST('BACH' AS VARCHAR2(10))` for UNION ALL compatibility |
| `02_trd_data_std.sql` | `Q000_TRD_DATA_01.sql` | 22,066 | 42 | TRADES_COHORT_INFO × SHORT_RESP × PROGRAMS × CIP |
| `03_appso_data_std.sql` | `APPSO_Data_01_Final.sql` | 23,166 | 41 | Same shape; `'APPR' \|\| prgm_credential` |
| `04_dacso_data_std.sql` | `DACSO_Q003_DACSO_DATA_Part_1_stepA.sql` | 147,020 | 45 | SURV_COHORT × PROGRAMS × CIP × C_OUTC; C_OUTC_CLEAN2 dropped (redundant) |
| `05_graduates_std.sql` | `Q000_TRD_Graduates.sql` + `APPSO_Graduates.sql` | 1,004 | 8 | TRD+APPSO UNION; age-band labels coded; filters C_Outc21–25 (originals had no filter) |
| `06_age_step1_std.sql` | `qry_make_tmp_table_Age_step1.sql` | 147,020 | 7 | SURV_COHORT; dates are VARCHAR2 in Oracle |
| `07_lookups_std.sql` | `INFOWARE_L_CIP_*.sql` (2 files) | 454/2,119/6,736 | 3/9/25 | Three standalone SELECT blocks (run separately) |

All validated read-only against Oracle. Row counts match PSSM2025 MS SQL
copies exactly (data parity confirmed).

### INTERNATIONAL column added
All four surveys carry an international-student flag; added to all four data
queries (see `TABLE_COLUMN_INVENTORY.md` section 6):

| Survey | Oracle source table | Column | Type |
|---|---|---|---|
| BGS | `BGS_DIST_21_25` | `INTERNATIONAL` | NUMBER → cast to `TO_CHAR()` |
| TRD | `TRADES_COHORT_INFO` | `INTERNATIONAL` | VARCHAR2 |
| APPSO | `APPRENTICE_COHORT_INFO` | `INTERNATIONAL` | VARCHAR2 |
| DACSO | `SURV_COHORT_COLLECTION_INFO` | `COSC_INTERNATIONAL` | VARCHAR2 |

### MS SQL port preserved
`sql/student_outcome_sql/refactored/mssql/` — 7 files + README, validated
against PSSM2025. Reads from the materialized `_r` tables.

### R loader script written
`R/load-so-survey-infoware.R` — sources from repo root, connects to Oracle,
creates lazy dbplyr tbl objects:
- `so_bgs`, `so_trd`, `so_appso`, `so_dacso` (individual survey data, all columns)
- `so_graduates` (TRD+APPSO graduate counts)
- `so_cip4_lookup`, `so_cip6_lookup`, `so_programs` (lookups)
- `so_combined` (four surveys stacked — **see Known Issue below**)

Individual tbls work correctly for lazy dbplyr chains (filter, select,
group_by, collect).

### Inventory doc updated
`sql/student_outcome_sql/TABLE_COLUMN_INVENTORY.md` — updated header to
clarify Oracle as true source; added section 6 (INTERNATIONAL flag).

---

## What's incomplete (Known Issues)

### `so_combined` stacking — Oracle UNION ALL type mismatch (ORA-01790)

**Problem**: Oracle UNION ALL across the four surveys fails because columns
that are logically identical (NUMBER, VARCHAR2) have different internal
subtypes depending on the source table definition:
- NUMBER reported as ODBC type 6 (FLOAT) in BGS_DIST vs type 3 (DECIMAL) in
  TRADES_COHORT_INFO — Oracle treats these as incompatible in UNION ALL.
- VARCHAR2 columns reported as type 12 (VARCHAR) vs -9 (WVARCHAR) depending
  on CHAR-vs-BYTE semantics.
- 15 of 27 common columns have mismatches.

Diagnosis output in `C:\Users\JDUAN\AppData\Local\Temp\opencode\diagnose-types.R`
+ its output.

**Current approach**: `collect()` each survey separately, then `bind_rows()`
in R. Works but is slow — 316k rows total (mainly DACSO 147k with complex
joins); timed out at 30 min in testing. The four individual `collect()`
calls may need to be run with a longer timeout or cached to `.rds`.

**Options for next session**:
1. **Increase timeout** — re-run with `timeout=3600000` (60 min); DACSO is
   the bottleneck (147k rows, 5-table join).
2. **Cache to .rds** — collect each survey once, save to
   `.scratch/cache/so_{survey}_std.rds`, then `bind_rows()` from caches.
   Future sessions load from .rds (seconds, not minutes).
3. **Cast all columns** in the SQL: wrap each common column in
   `CAST(col AS NUMBER)` or `CAST(col AS VARCHAR2(n))` inside each branch of
   the UNION ALL. Verbose (27 casts × 4 branches) but makes Oracle UNION ALL
   work, keeping the result lazy.
4. **Create Oracle view** — `CREATE VIEW so_combined_v AS SELECT ... UNION
   ALL ...` with explicit casts; then `tbl(con, in_schema("INFOWARE",
   "so_combined_v"))`. Requires DB write access (one-time).

### BGS SQL has workaround casts
`01_bgs_data_std.sql` has `CAST('BACH' AS VARCHAR2(10))` and
`TO_CHAR(d.international)` — added to fix UNION ALL type issues. These are
necessary but **not sufficient** (many other columns still mismatch). If
option 3 or 4 above is chosen, review whether these per-column casts should
stay in the SQL file or move to the UNION ALL wrapper.

### Graduates cycle filter
`05_graduates_std.sql` filters to `C_Outc21–C_Outc25` for consistency with
the data queries. Original queries returned **all** cycles (no filter).
Widen the `IN (...)` list if benchmark cycles (2019–2023) are needed.

### dbplyr + Oracle quirks for analysts
- `sum(condition)` doesn't translate (use `sum(if_else(...))` or
  `sum(case_when(...))`).
- `filter(row_number() <= n)` fails (window functions in WHERE not allowed
  in Oracle).
- `mutate()` referencing a column created in the same `summarise()` fails
  (dbplyr limitation — add a separate `mutate()` step).

---

## File map

```
sql/student_outcome_sql/
├── TABLE_COLUMN_INVENTORY.md          ← inventory + standardized name map
├── HANDOFF.md                         ← this file
├── refactored/
│   ├── README.md                      ← Oracle conventions + validation table
│   ├── 01_bgs_data_std.sql            ← Oracle queries (PRIMARY)
│   ├── 02_trd_data_std.sql
│   ├── 03_appso_data_std.sql
│   ├── 04_dacso_data_std.sql
│   ├── 05_graduates_std.sql
│   ├── 06_age_step1_std.sql
│   ├── 07_lookups_std.sql
│   └── mssql/
│       ├── README.md                  ← MS SQL port notes
│       └── 01..07_*.sql              ← PSSM2025 MS SQL versions
├── BGS_Q001_BGS_Data_2021_2025.sql    ← originals (untouched)
├── Q000_TRD_DATA_01.sql
├── ... (10 legacy files)
R/
└── load-so-survey-infoware.R          ← dbplyr wrapper script
```

---

## Oracle connection snippet

```r
iw_config <- config::get("infoware")
# EZConnect form may fail (ORA-12154); full descriptor is the fallback:
desc <- "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=scratch.bcgov)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=pdbtrn)))"
iw_con <- dbConnect(
  odbc::odbc(),
  Driver = "Oracle in instantclient_19c",
  DBQ = desc,        # or iw_config$dbq if EZConnect works on your client
  UID = iw_config$uid,
  PWD = iw_config$pwd
)
```

---

## Standardized output schema (27 common columns)

`SURVEY`, `SUBM_CD`, `SURVEY_YEAR`, `STUDENT_KEY`, `PEN`, `RESPONDENT`,
`INST`, `INST_NAME`, `PSSM_CREDENTIAL`, `LCP6_CD`, `LCP6_DIGITS_NAME`,
`LCP4_CD`, `LCP4_DIGITS_NAME`, `TTRAIN`, `AGE_AT_SURVEY`, `AGE_GROUP`,
`CURRENT_REGION1`, `CURRENT_REGION4`, `LABR_IN_LABOUR_MARKET`,
`LABR_EMPLOYED`, `LABR_UNEMPLOYED`, `LABR_JOB_SEARCH_TIME_GP`,
`LABR_JOB_TRAINING_RELATED`, `NOC_CD`, `NOC_NAME`, `GRADSTAT_GROUP`,
`INTERNATIONAL`

Survey-specific extras (BGS salaries/satisfaction, TRD further-ed, APPSO
time-to-find/private, DACSO PFST_*/credential/cohorts) remain in the
individual `so_{survey}` tbls.

---

## Recommended next steps

1. **Resolve `so_combined`** — pick one of the 4 options above (recommend
   option 2: cache to .rds for speed + reproducibility).
2. **Validate graduates** — re-run with all cycles if benchmark graduates
   are needed; verify the AGE_GROUP_LABEL bands match the R pipeline.
3. **Integration test** — run the R loader, pull `so_combined` (or cached
   version), verify response rates / employment rates match the EDA report
   values.
4. **Doc cleanup** — remove temp investigation scripts from
   `C:\Users\JDUAN\AppData\Local\Temp\opencode\` (inventory scripts, validate
   scripts, diagnose-types.R, oracle-*.R).
