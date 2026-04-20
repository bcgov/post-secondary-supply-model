# dplyr Translations of the PSSM Pipeline

This directory contains dplyr/dbplyr translations of the original R+SQL hybrid pipeline scripts. Each `*_dplyr.R` file is a standalone replacement for the corresponding original file in `R/`.

## Why This Exists

The original pipeline mixed R orchestration with SQL Server queries executed as raw strings. This made the logic harder to follow, debug, and maintain. The translations move all data transformation logic into R using the tidyverse (dplyr, tidyr, stringr, lubridate), keeping only DDL operations (CREATE/DROP/ALTER TABLE) as `dbExecute()` calls.

## Quick Start

1. **Prerequisites**: Same as the original pipeline — R, tidyverse, DBI, odbc, dbplyr, config, and VPN access to the BC Gov network.
2. **Configuration**: Uses the same `config.yml` as the original. No changes needed.
3. **Running the model**: Replace sourced file paths in the prep scripts to point to `R/dplyr/` versions. For example, in `prep-for-fresh-run_dplyr.R`, the file list already references the original paths — change them to `"./R/dplyr/02b-1-pssm-cohorts_dplyr.R"` etc.
4. **Verifying results**: After running both versions, compare output tables with `all.equal()`.

## File Mapping

### Pipeline Scripts (core model, numbered sequence)

| dplyr File | Original | Lines (dplyr / original) | Reduction |
|---|---|---|---|
| `01a-enrolment-preprocessing_dplyr.R` | `01a-enrolment-preprocessing.R` | 579 / 1376 | 58% |
| `01b-credential-preprocessing_dplyr.R` | `01b-credential-preprocessing.R` | 335 / 411 | 18% |
| `01c-credential-analysis_dplyr.R` | `01c-credential-analysis.R` | 1480 / 2595 | 43% |
| `01d-enrolment-analysis_dplyr.R` | `01d-enrolment-analysis.R` | 755 / 1028 | 27% |
| `02a-appso-programs_dplyr.R` | `02a-appso-programs.R` | 281 / 243 | * |
| `02a-bgs-program-matching_dplyr.R` | `02a-bgs-program-matching.R` | 1306 / 2055 | 36% |
| `02a-dacso-program-matching_dplyr.R` | `02a-dacso-program-matching.R` | 702 / 1030 | 32% |
| `02a-update-cred-non-dup_dplyr.R` | `02a-update-cred-non-dup.R` | 467 / 436 | * |
| `02b-1-pssm-cohorts_dplyr.R` | `02b-1-pssm-cohorts.R` | 503 / 661 | 24% |
| `02b-2-pssm-cohorts-new-labour-supply_dplyr.R` | `02b-2-pssm-cohorts-new-labour-supply.R` | 747 / 1239 | 40% |
| `02b-3-pssm-cohorts-occupation-distributions_dplyr.R` | `02b-3-pssm-cohorts-occupation-distributions.R` | 681 / 1664 | 59% |
| `03-near-completers-ttrain_dplyr.R` | `03-near-completers-ttrain.R` | 799 / 2010 | 60% |
| `04-graduate-projections_dplyr.R` | `04-graduate-projections.R` | 546 / 492 | * |
| `05-ptib-analysis_dplyr.R` | `05-ptib-analysis.R` | 336 / 514 | 35% |
| `06-historic-cohort-program-distribution_dplyr.R` | `06-historic-cohort-program-distribution.R` | 334 / 242 | * |
| `06-program-projections_dplyr.R` | `06-program-projections.R` | 566 / 1094 | 48% |
| `07-occupation-projections_dplyr.R` | `07-occupation-projections.R` | 638 / 2949 | 78% |

\* Some files are slightly longer due to added explanatory comments and R-native boilerplate.

### Data Loading Scripts

| dplyr File | Original | Notes |
|---|---|---|
| `load-program-projections_dplyr.R` | `load-program-projections.R` | Mostly CSV reads; 1 complex SQL query translated |
| `load-cohort-dacso_dplyr.R` | `load-cohort-dacso.R` | UPDATE...CASE WHEN + ALTER TABLE replaced by R-native processing |
| `occ-dists-census-data_dplyr.R` | `occ-dists-census-data.R` | SELECT INTO + INSERT INTO replaced by `bind_rows()` |

### Orchestration Scripts

| dplyr File | Original | Notes |
|---|---|---|
| `prep-for-fresh-run_dplyr.R` | `prep-for-fresh-run.R` | DROP TABLE kept as DDL; SELECT INTO → `dbReadTable` + `dbWriteTable` |
| `prep-for-qi-run_dplyr.R` | `prep-for-qi-run.R` | DROP TABLE IF EXISTS kept as DDL |
| `prep-for-ptib-run_dplyr.R` | `prep-for-ptib-run.R` | Same pattern: DDL kept, copies use R-mediated read/write |

### Ad-hoc / Utility Scripts

| dplyr File | Original | Notes |
|---|---|---|
| `zz-adhoc-outputs-lf_dplyr.R` | `zz-adhoc-outputs-lf.R` | Complex analytical SQL with joins/IIf → dplyr `left_join` + `case_when` |
| `zz-graduate-historical-forecasted_dplyr.R` | `zz-graduate-historical-forecasted.R` | SQL queries → dplyr transformations |

### Files Not Translated (already R-native)

These 17 files were assessed and require no translation — they already use dplyr natively or only perform simple CSV loading:

- `08-create-final-reports.R` — reads DB tables with `dbReadTable`, all processing in dplyr
- `labour-supply-dists-census-data.R` — all transformations already in dplyr
- `load-cohort-appso.R`, `load-cohort-bgs.R`, `load-cohort-trd.R` — R-native data loading
- `load-custom-stats-can.R`, `load-graduate-projections.R`, `load-near-completers-ttrain.R`
- `load-occupation-projections.R`, `load-outcomes-data.R`, `load-ptib.R`
- `load-stp-cred.R`, `load-stp-enrol.R`
- `noc-imputation.R` — entirely R-native mathematical operations
- `run-imputation-by-region.R` — orchestrator that sources `noc-imputation.R`
- `utils.R` — `time_execution()` wrapper, no SQL
- `zz-adhoc-outputs.R` — already uses `tbl()` + `collect()` with dplyr

## How to Navigate the Code

### Pipeline Sequence

The scripts run in a specific order, controlled by the prep scripts:

```
01a-01d   Preprocessing (enrolment + credential data cleaning)
02a       Program matching (CIP codes, APPSO/BGS/DACSO datasets)
02b-1     Unified cohort creation (4 surveys merged)
02b-2     New labour supply distributions
02b-3     Occupation distributions
03        Near completers (trades training)
04        Graduate projections
05        PTIB analysis (private institutions)
06        Program projections + historic cohort distributions
07        Occupation projections (final output)
08        Report generation (Excel workbooks)
```

### Reading a dplyr File

Each file follows a consistent structure:

1. **Header comment** — explains the file's role in the pipeline, lists input/output tables, and summarizes key translations from the original SQL.
2. **Boilerplate** — database connection, `sch_tbl()` and `write_schema_table()` helpers.
3. **Data loading** — `sch_tbl("table") %>% collect() |> rename_with(toupper)` pulls data from SQL Server into R.
4. **Sections** — marked with `# ******` banners matching the original section names. Each section has a `# WHY:` comment explaining the business logic.
5. **Output** — `write_schema_table()` writes results back to the database.

### Finding Specific Logic

| What you're looking for | Where to find it |
|---|---|
| How credentials are classified (RecordStatus 0-8) | `01b-credential-preprocessing_dplyr.R` |
| Gender cleaning pipeline (~35 SQL ops consolidated) | `01c-credential-analysis_dplyr.R`, "Gender cleaning" section |
| Credential ranking by date within EPEN groups | `01c-credential-analysis_dplyr.R`, "Credential ranking" section |
| How 4 surveys (TRD, APPSO, BGS, DACSO) merge into cohorts | `02b-1-pssm-cohorts_dplyr.R` |
| Cascading unknown resolution (direct -> No_TT -> Private_Cred -> LCP2) | `07-occupation-projections_dplyr.R`, Q_2 and Q_3 sections |
| NOC pivot tables (1D-5D) | `07-occupation-projections_dplyr.R`, Q_4 section |
| PTIB domestic graduate estimation | `05-ptib-analysis_dplyr.R` |
| BC and Total region rollups | `07-occupation-projections_dplyr.R`, Q_4 BC/Total sections |

## Translation Patterns

These patterns appear repeatedly across the translations. Understanding them will help you read and modify the code.

### SQL to dplyr Mapping

| SQL | dplyr |
|---|---|
| `SELECT * INTO new FROM old` | `new <- old; write_schema_table("new", new)` |
| `UPDATE t SET col = CASE WHEN ... END` | `t <- t %>% mutate(col = case_when(...))` |
| `INSERT INTO t SELECT ... FROM s` | `t <- bind_rows(t, s %>% ...)` |
| `DELETE FROM t WHERE cond` | `t <- t %>% filter(!cond)` |
| `LEFT JOIN` / `INNER JOIN` | `left_join()` / `inner_join()` |
| `UNION ALL` | `bind_rows()` |
| `IIf(cond, a, b)` | `if_else(cond, a, b)` or `case_when()` |
| `LEFT(col, n)` / `SUBSTRING(col, s, n)` | `str_sub(col, 1, n)` |
| `CONCAT(a, b, c)` / `a + b` | `paste0(a, b, c)` or `str_c()` |
| `PIVOT` | `pivot_wider()` |
| `IS NULL` / `IS NOT NULL` | `is.na()` / `!is.na()` |
| `DROP TABLE` | `dbExecute(con, "DROP TABLE ...")` (kept as SQL) |
| `ALTER TABLE` | Type conversion in R before `dbWriteTable` |
| `UPDATE...FROM` (multi-table) | Pull all tables, `left_join() + mutate()`, write back |

### Key Helpers

Every file defines these two helpers at the top:

```r
sch_tbl <- function(name) {
  tbl(decimal_con, dbplyr::in_schema(my_schema, name))
}

write_schema_table <- function(name, data) {
  dbWriteTable(decimal_con, SQL(glue::glue('"{my_schema}"."{name}"')), data, overwrite = TRUE)
}
```

A few files define additional helpers for repeated patterns:

- `grad_prefix(status)` — produces the conditional `"status - "` prefix for composite keys (used in `06-historic-cohort-program-distribution_dplyr.R` and `07-occupation-projections_dplyr.R`)
- `build_noc_pivot_by_cred()` / `build_noc_pivot_by_year()` — NOC pivot table builders (used in `07-occupation-projections_dplyr.R`)

### Column Name Convention

SQL Server column names are uppercase. After every `collect()`, we apply `rename_with(toupper)` to ensure consistency:

```r
data <- sch_tbl("some_table") %>% collect() |> rename_with(toupper)
```

This prevents case-sensitivity issues when joining tables loaded from different sources.

### SQL Operations Kept as `dbExecute`

Some SQL operations cannot be translated to dplyr and are kept as `dbExecute()` calls. These are marked with `# KEPT AS SQL:` comments in the code:

- **DDL operations** — `CREATE TABLE`, `DROP TABLE`, `ALTER TABLE`
- **Cross-schema copies** — `SELECT * INTO ... FROM [other_schema].[table]` (the source table may not be accessible via `sch_tbl()`)
- **Appends to existing tables** — `INSERT INTO ... SELECT` where the target table already contains data from a prior step and we're adding rows

## Caveats and Considerations

### Memory Usage

The original pipeline used SQL Server as intermediate storage — each step wrote results to a database table, and the next step read from that table. The dplyr translations pull data into R memory instead. For this dataset (education enrolment records for a single province), this is manageable on a standard workstation. If the data volume grows significantly, consider:

- Using `dbplyr` lazy evaluation (chain dplyr verbs without `collect()`) for filtering/aggregation steps
- Using `arrow` for larger-than-memory datasets
- Writing intermediate results back to the database with `write_schema_table()` at natural checkpoints

### Temp Tables vs R Variables

The original pipeline created and dropped many temporary SQL tables (e.g., `qry_01a_temp`, `qry_01b_step2`). The dplyr translations replace these with R variables that are automatically cleaned up when the script finishes. This eliminates the need for `DROP TABLE` cleanup at the end of each script.

### `rows_update()` for Simple Updates

For `UPDATE t SET col = val WHERE id = x` patterns, we use `rows_update(by = "ID", unmatched = "ignore")` when the update is a simple key-value replacement. For complex conditional updates (`UPDATE ... SET col = CASE WHEN ... END`), we pull the table, apply `mutate(case_when())`, and write it back.

### Floating Point Differences

SQL Server and R handle floating-point arithmetic differently. You may see small differences in the last decimal places of computed percentages. These are negligible for the model but may cause `all.equal()` to fail. Use `all.equal(tolerance = 1e-6)` or compare rounded values when verifying translations.

### NULL Handling

SQL `NULL` and R `NA` behave differently in some edge cases:

- SQL: `NULL = NULL` is `UNKNOWN` (false). R: `NA == NA` is `NA`.
- SQL: `NULL + 1` is `NULL`. R: `NA + 1` is `NA`.
- SQL: `ISNULL(col, 0)`. R: `coalesce(col, 0)` or `replace_na()`.

The translations use `coalesce()` for `ISNULL()`/`COALESCE()` patterns and `is.na()` for `IS NULL` checks.

### Join Cardinality

SQL's `UPDATE...FROM` with joins can silently produce duplicate rows if the join has multiple matches. The dplyr translations are more explicit — `left_join()` will produce the same duplicates (with a warning in newer dplyr versions), making the issue visible. If you see unexpected row counts, check join cardinality.

### String Encoding

Some lookup CSV files contain non-ASCII characters (e.g., NOC occupation names with accents). The original pipeline loaded these through SQL Server which handled encoding. When reading CSVs directly in R, ensure you use `read_csv()` (from readr) which handles UTF-8 correctly, or specify `locale = locale(encoding = "UTF-8")` if needed.

## Annotation Conventions

Each section in the translated files includes:

- `# WHY:` — explains the business reason for a calculation (why, not what)
- `# KEPT AS SQL:` — marks SQL operations that could not be translated
- `# NOTE:` — flags non-obvious behavior or edge cases
- Section headers (e.g., `# ---- qry_06b: Count grads by CIP ----`) are kept from the original for easy cross-referencing

## Verification

To verify a translation produces identical results:

1. Run the original script against a test schema.
2. Run the dplyr version against a different test schema.
3. Compare key output tables:

```r
original <- dbReadTable(con1, "output_table") %>% arrange(across(everything()))
translated <- dbReadTable(con2, "output_table") %>% arrange(across(everything()))
all.equal(original, translated, tolerance = 1e-6)
```

Some tables may have column ordering differences — use `sort()` on both before comparing.
