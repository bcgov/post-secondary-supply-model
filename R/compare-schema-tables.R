# Copyright 2024 Province of British Columbia
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# ******************************************************************************
# compare-schema-tables.R — compare tables between two SQL Server schemas
# ******************************************************************************
# WHAT:
#   Compares a set of table pairs between a baseline schema and a rebuilt
#   schema on the configured SQL Server database, using READ-ONLY queries:
#     1. structure  — column sets and types (INFORMATION_SCHEMA.COLUMNS)
#     2. row counts — sys.partitions
#     3. content    — two-direction EXCEPT over shared columns, exact match
#                     (zero tolerance; NULL-safe since EXCEPT treats NULLs
#                     as equal)
#     4. drill-down — per-column (value, frequency) distribution diffs +
#                     sample rows from each direction (small tables only)
#
#   Each pair runs on its OWN connection, is wrapped in tryCatch, and writes
#   its report section to a fragment file as soon as it completes — so a
#   network drop or query failure kills at most the pair in flight, and a
#   rerun resumes from the fragments (completed pairs are skipped).
#   Final report = assembled fragments + summary table.
#
# RUN:
#   Rscript R/compare-schema-tables.R
#   (RUN_ALL_PAIRS at the top selects the full pair set or the fast subset;
#    delete a fragment file to force that pair to re-run)
#
# NEXT STEP / context:
#   Built for the STP_* comparison across personal schemas (wayfinder map,
#   plan/wayfinder/). Pair list below mirrors the confirmed inventory
#   (plan/wayfinder/research/stp-inventory.md); extend it when comparing
#   other table families.
# ******************************************************************************

## ----------------------------------------------------------
## Reasons for change, other notes
## ----------------------------------------------------------
## Shape validated 2026-08-18 against the credential pairs
## (prototype + reaction, wayfinder ticket 003):
##   - ID-like columns are EXCLUDED from the content verdict and
##     probed separately as set-equality (info only): both sides
##     regenerate ID as row_number(), so ID equality is meaningless
##     for content but useful as a record-universe check.
##   - Type mismatches escalate the verdict to STRUCTURE-DIFF
##     (soft content compare still reported underneath via
##     nvarchar(max) casts) — the date-vs-varchar finding showed
##     type mismatches can be the story, not a parenthetical.
##   - Sample rows are shown from BOTH directions, ordered by the
##     first shared columns so counterpart rows roughly align
##     (no join key exists; ordering is the honest approximation).
##
## Hardened 2026-08-18 after the first full run died mid-drill-down
## (08S01 network error killed the whole run; nothing persisted):
##   - per-pair connection + tryCatch — one failure no longer halts
##     the run; the failed pair gets a PAIR-ERROR fragment and is
##     retried on the next run
##   - per-pair report fragments under plan/wayfinder/research/
##     fragments/ — completed pairs are skipped on rerun (resume)
##   - sample-row fetch skipped for tables above MAX_ROWS_FOR_SAMPLES
##     (each sample query re-materialises the full EXCEPT; on 20M-row
##     pairs that is ~10 min per direction for nice-to-have rows)
## ----------------------------------------------------------

library(DBI)

## ---- Config ----------------------------------------------------------------
db_config <- config::get("decimal")
my_schema   <- config::get("myschema")      # rebuilt side
second      <- config::get("second_schema") # baseline side
shareschema <- config::get("shareschema")   # dbo raw layer

EXCLUDE_COLS_DEFAULT <- c("ID")  # excluded from content verdict, probed as set-equality
DRILLDOWN_MAX_COLS   <- 40       # per-pair cap for per-column distribution diffs
SAMPLE_ROWS          <- 3        # sample rows shown per direction
MAX_ROWS_FOR_SAMPLES <- 5e6      # skip sample fetch above this row count

RUN_ALL_PAIRS <- TRUE

REPORT_PATH   <- "plan/stp-comparison-report-rerun.md"
FRAGMENT_DIR  <- "plan/wayfinder/02-prep/research/fragments-rerun"

## Comparison set from the confirmed inventory (stp-inventory.md).
## Raw pairs: dbo _2024 tables stand in as the rebuilt side.
pairs_all <- list(
  list(label = "raw credential",
       base_schema = second, base_table = "STP_CREDENTIAL",
       reb_schema  = shareschema, reb_table = "STP_CREDENTIAL_2024"),
  list(label = "raw enrolment",
       base_schema = second, base_table = "STP_ENROLMENT",
       reb_schema  = shareschema, reb_table = "STP_Enrolment_2024"),
  list(label = "01-stage",
       base_schema = second, base_table = "stp_credential_r",
       reb_schema  = my_schema, reb_table = "stp_credential_r"),
  list(label = "01-stage",
       base_schema = second, base_table = "stp_credential_record_type_r",
       reb_schema  = my_schema, reb_table = "stp_credential_record_type_r"),
  list(label = "01-stage",
       base_schema = second, base_table = "stp_enrolment_r",
       reb_schema  = my_schema, reb_table = "stp_enrolment_r"),
  list(label = "01-stage",
       base_schema = second, base_table = "stp_enrolment_record_type_r",
       reb_schema  = my_schema, reb_table = "stp_enrolment_record_type_r"),
  list(label = "01-stage",
       base_schema = second, base_table = "stp_enrolment_valid_r",
       reb_schema  = my_schema, reb_table = "stp_enrolment_valid_r")
)
pairs <- if (RUN_ALL_PAIRS) pairs_all else pairs_all[c(1, 3)]

dir.create(FRAGMENT_DIR, showWarnings = FALSE, recursive = TRUE)

## ---- Helpers ----------------------------------------------------------------
connect_db <- function() {
  dbConnect(
    odbc::odbc(),
    Driver = db_config$driver, Server = db_config$server,
    Database = db_config$database, Trusted_Connection = "True"
  )
}

qt <- function(schema, table) {
  paste0("[", gsub("]", "]]", schema, fixed = TRUE), "].[" ,
         gsub("]", "]]", table, fixed = TRUE), "]")
}

get_columns <- function(con, schema, table) {
  dbGetQuery(con, paste0(
    "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE
     FROM INFORMATION_SCHEMA.COLUMNS
     WHERE TABLE_SCHEMA = '", schema, "' AND TABLE_NAME = '", table, "'
     ORDER BY ORDINAL_POSITION;"))
}

type_str <- function(cols) {
  paste0(cols$DATA_TYPE,
         ifelse(!is.na(cols$CHARACTER_MAXIMUM_LENGTH) & cols$DATA_TYPE %in% c("varchar","nvarchar","char","nchar"),
                paste0("(", cols$CHARACTER_MAXIMUM_LENGTH, ")"), ""),
         ifelse(cols$DATA_TYPE %in% c("decimal","numeric"),
                paste0("(", cols$NUMERIC_PRECISION, ",", cols$NUMERIC_SCALE, ")"), ""))
}

row_count <- function(con, schema, table) {
  dbGetQuery(con, paste0(
    "SELECT SUM(p.rows) AS n FROM sys.partitions p
     JOIN sys.tables t ON t.object_id = p.object_id
     JOIN sys.schemas s ON s.schema_id = t.schema_id
     WHERE s.name = '", schema, "' AND t.name = '", table, "'
       AND p.index_id IN (0,1);"))$n[[1]]
}

## Per-column (value, frequency) distribution diff — keyless attribution signal
column_dist_diff <- function(con, schema, table, col, other_schema, other_table) {
  dbGetQuery(con, paste0(
    "SELECT COUNT(*) AS n_distinct_changed FROM (
       SELECT [", col, "] AS v, COUNT(*) AS freq FROM ", qt(schema, table), " GROUP BY [", col, "]
       EXCEPT
       SELECT [", col, "] AS v, COUNT(*) AS freq FROM ", qt(other_schema, other_table), " GROUP BY [", col, "]
     ) d;"))$n_distinct_changed[[1]]
}

## ---- Run ---------------------------------------------------------------------
cat("=== Schema table comparison |", db_config$database, "|",
    format(Sys.time(), "%Y-%m-%d %H:%M"), "===\n\n")

compare_pair <- function(idx, p) {
  frag_path <- file.path(FRAGMENT_DIR,
                         sprintf("%02d-%s.md", idx, p$base_table))
  if (file.exists(frag_path) &&
      !grepl("PAIR-ERROR", readLines(frag_path, warn = FALSE), fixed = TRUE)) {
    cat("##", p$base_table, "vs", p$reb_table, " — fragment exists, skipping (rerun-safe)\n")
    return(invisible(NULL))
  }
  t0 <- Sys.time()
  cat("##", p$base_table, "vs", p$reb_table, "\n")
  con <- connect_db()
  on.exit(try(dbDisconnect(con), silent = TRUE), add = TRUE)

  result <- tryCatch({
    b <- qt(p$base_schema, p$base_table)
    r <- qt(p$reb_schema, p$reb_table)

    n_b <- row_count(con, p$base_schema, p$base_table)
    n_r <- row_count(con, p$reb_schema, p$reb_table)

    ## 1. structure
    cb <- get_columns(con, p$base_schema, p$base_table)
    cr <- get_columns(con, p$reb_schema, p$reb_table)
    shared   <- intersect(cb$COLUMN_NAME, cr$COLUMN_NAME)
    only_b   <- setdiff(cb$COLUMN_NAME, cr$COLUMN_NAME)
    only_r   <- setdiff(cr$COLUMN_NAME, cb$COLUMN_NAME)
    tb <- setNames(type_str(cb), cb$COLUMN_NAME); tr <- setNames(type_str(cr), cr$COLUMN_NAME)
    type_diffs <- shared[tb[shared] != tr[shared]]

    struct_ok <- length(only_b) == 0 && length(only_r) == 0 && length(type_diffs) == 0

    detail <- c(paste0("## ", p$base_table, "  (", p$base_schema, ")  vs  ",
                       p$reb_table, "  (", p$reb_schema, ")  — ", p$label),
                paste0("rows: ", format(n_b, big.mark = ","),
                       " vs ", format(n_r, big.mark = ",")),
                "")

    if (!struct_ok) {
      detail <- c(detail, "**Structure differences:**", "")
      if (length(only_b)) detail <- c(detail, paste0("- baseline-only columns: ",
        paste(only_b, collapse = ", ")))
      if (length(only_r)) detail <- c(detail, paste0("- rebuilt-only columns: ",
        paste(only_r, collapse = ", ")))
      if (length(type_diffs)) detail <- c(detail, paste0("- type mismatches: ",
        paste0(type_diffs, " (", tb[type_diffs], " vs ", tr[type_diffs], ")", collapse = "; ")), "")
    }

    ## 2-3. content on shared columns, excluding EXCLUDE_COLS
    cmp_cols  <- setdiff(shared, EXCLUDE_COLS_DEFAULT)
    soft <- length(type_diffs) > 0
    cast_wrap <- if (soft) function(x) paste0("CAST([", x, "] AS nvarchar(max)) AS [", x, "]")
                 else function(x) paste0("[", x, "]")
    sel <- paste(vapply(cmp_cols, cast_wrap, ""), collapse = ", ")

    excl_note <- if (length(intersect(EXCLUDE_COLS_DEFAULT, shared)))
      paste0("content compare excludes: ", paste(intersect(EXCLUDE_COLS_DEFAULT, shared), collapse = ", ")) else ""
    soft_note <- if (soft) "SOFT COMPARE: mismatched-type columns cast to nvarchar(max) — date-format artifacts possible" else ""

    base_only <- dbGetQuery(con, paste0("SELECT COUNT(*) AS n FROM (SELECT ", sel, " FROM ", b, " EXCEPT SELECT ", sel, " FROM ", r, ") d;"))$n[[1]]
    reb_only  <- dbGetQuery(con, paste0("SELECT COUNT(*) AS n FROM (SELECT ", sel, " FROM ", r, " EXCEPT SELECT ", sel, " FROM ", b, ") d;"))$n[[1]]

    ## ID set-equality probe (informational only)
    id_note <- ""
    if ("ID" %in% shared) {
      id_b <- dbGetQuery(con, paste0("SELECT COUNT(*) AS n FROM (SELECT [ID] FROM ", b, " EXCEPT SELECT [ID] FROM ", r, ") d;"))$n[[1]]
      id_r <- dbGetQuery(con, paste0("SELECT COUNT(*) AS n FROM (SELECT [ID] FROM ", r, " EXCEPT SELECT [ID] FROM ", b, ") d;"))$n[[1]]
      id_note <- paste0("ID set-equality (info only, excluded from verdict): baseline-only ", format(id_b, big.mark = ","),
                        " / rebuilt-only ", format(id_r, big.mark = ","))
    }

    verdict <- if (!struct_ok) "STRUCTURE-DIFF"
      else if (base_only == 0 && reb_only == 0) "MATCH"
      else "CONTENT-DIFF"

    detail <- c(detail,
      paste0("**Verdict: ", verdict, "** — content: baseline-only ",
             format(base_only, big.mark = ","), " / rebuilt-only ", format(reb_only, big.mark = ",")),
      if (excl_note != "") paste0("(", excl_note, ")"),
      if (soft_note != "") paste0("(", soft_note, ")"),
      if (id_note != "") c("", id_note), "")

    ## 4. drill-down when flagged
    if (verdict != "MATCH") {
      dcols <- head(cmp_cols, DRILLDOWN_MAX_COLS)
      dd <- vapply(dcols, function(cc)
        column_dist_diff(con, p$base_schema, p$base_table, cc, p$reb_schema, p$reb_table), 0)
      dd <- sort(dd[dd > 0], decreasing = TRUE)
      if (length(dd)) {
        detail <- c(detail, paste0("**Columns with differing (value, frequency) distributions** ",
                                   "(capped at first ", DRILLDOWN_MAX_COLS, " columns):"), "")
        for (cc in names(dd)) detail <- c(detail, paste0("- `", cc, "`: ", format(dd[[cc]], big.mark = ","), " distinct values changed"))
        detail <- c(detail, "")
      }
      ## Sample rows from BOTH directions (skipped on big tables — each fetch
      ## re-materialises the full EXCEPT).
      if (max(n_b, n_r) <= MAX_ROWS_FOR_SAMPLES) {
        ord_cols <- head(cmp_cols, 3)
        ord_by <- paste(vapply(ord_cols, function(x) paste0("[", x, "]"), ""), collapse = ", ")
        samp_b <- dbGetQuery(con, paste0("SELECT TOP (", SAMPLE_ROWS, ") * FROM (SELECT ", sel, " FROM ", b, " EXCEPT SELECT ", sel, " FROM ", r, ") d ORDER BY ", ord_by, ";"))
        samp_r <- dbGetQuery(con, paste0("SELECT TOP (", SAMPLE_ROWS, ") * FROM (SELECT ", sel, " FROM ", r, " EXCEPT SELECT ", sel, " FROM ", b, ") d ORDER BY ", ord_by, ";"))
        if (nrow(samp_b)) {
          detail <- c(detail, paste0("**Sample baseline-only rows** (first ", SAMPLE_ROWS, ", first 8 compared columns, ordered by ", paste(ord_cols, collapse = ", "), "):"), "")
          detail <- c(detail, "```", capture.output(print(samp_b[, head(cmp_cols, 8), drop = FALSE], row.names = FALSE)), "```", "")
        }
        if (nrow(samp_r)) {
          detail <- c(detail, paste0("**Sample rebuilt-only rows** (same ordering — compare against the block above):"), "")
          detail <- c(detail, "```", capture.output(print(samp_r[, head(cmp_cols, 8), drop = FALSE], row.names = FALSE)), "```", "")
        }
      } else {
        detail <- c(detail, paste0("(Sample rows skipped: table exceeds ", format(MAX_ROWS_FOR_SAMPLES, big.mark = ","),
                                   " rows — each sample fetch re-materialises the full EXCEPT.)"), "")
      }
    }

    elapsed <- round(as.numeric(difftime(Sys.time(), t0, units = "secs")), 1)
    cat(sprintf("   verdict: %-13s | baseline-only %s | rebuilt-only %s | %.1fs\n",
                verdict, format(base_only, big.mark = ","), format(reb_only, big.mark = ","), elapsed))
    if (id_note != "") cat("  ", id_note, "\n")

    meta <- sprintf("<!-- VERDICT:%s|%s|%d|%d -->", p$base_table, verdict, base_only, reb_only)
    writeLines(c(meta, detail), frag_path)
    NULL
  },
  error = function(e) {
    msg <- conditionMessage(e)
    cat("   PAIR-ERROR:", substr(msg, 1, 160), "\n")
    writeLines(c(sprintf("<!-- VERDICT:%s|PAIR-ERROR|0|0 -->", p$base_table),
                 paste0("## ", p$base_table, " vs ", p$reb_table, " — PAIR-ERROR"),
                 "", "```", substr(msg, 1, 2000), "```", ""),
               frag_path)
    NULL
  })
  invisible(result)
}

for (i in seq_along(pairs)) compare_pair(i, pairs[[i]])

## ---- Assemble report from fragments ------------------------------------------
frags <- sort(list.files(FRAGMENT_DIR, pattern = "\\.md$", full.names = TRUE))
meta_re <- "^<!-- VERDICT:([^|]+)\\|([^|]+)\\|([0-9]+)\\|([0-9]+) -->$"
summary_rows <- data.frame()
body <- c()
for (f in frags) {
  lines <- readLines(f, warn = FALSE)
  m <- regexec(meta_re, lines[1])
  v <- regmatches(lines[1], m)[[1]]
  if (length(v) == 5) {
    summary_rows <- rbind(summary_rows, data.frame(
      table = v[2], verdict = v[3],
      baseline_only = as.numeric(v[4]), rebuilt_only = as.numeric(v[5]),
      stringsAsFactors = FALSE))
  }
  body <- c(body, lines[-1], "")
}

report <- c(
  "# Schema table comparison report",
  "",
  paste0("Assembled ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
         " from ", length(frags), " pair fragments (see ", FRAGMENT_DIR,
         "/). Against ", db_config$server, "/", db_config$database,
         ". Content compare is exact/zero-tolerance, NULL-safe, on shared columns minus ",
         paste(EXCLUDE_COLS_DEFAULT, collapse = ", "), "."),
  "Per-table create/modify timestamps for both sides: see",
  "plan/wayfinder/research/stp-inventory.md (ticket 001 inventory).",
  "",
  "## Summary", "",
  if (nrow(summary_rows)) capture.output(print(summary_rows, row.names = FALSE)) else "(no fragments yet)",
  "",
  body)

writeLines(report, REPORT_PATH)
cat("\n=== SUMMARY (", nrow(summary_rows), " pairs ) ===\n", sep = "")
if (nrow(summary_rows)) print(summary_rows, row.names = FALSE)
cat("\nReport written:", REPORT_PATH, "\n")
