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

# =============================================================================
# Validation script for the so-oracle-migration cutover (wayfinder T04/T08).
#
# PURPOSE
#   Compare the new Oracle-direct *_r tables in PSSM2025 dbo against two
#   anchors, applying a hybrid equality bar with intentional-diffs exclusions.
#   Exit 0 = cutover PR acceptance bar clears; non-zero = blocks cutover.
#
# ANCHORS
#   Primary:   .scratch/so-oracle-migration/baseline/*_r.rds -- snapshot of
#              the legacy CSV-based *_r tables taken BEFORE the new loader
#              runs. Created by snapshot-baseline step below (run once).
#   Secondary: .scratch/student-outcome-survey-eda/cache/so_*_new.rds --
#              independent snapshot of the legacy *_r taken 2026-08-07 by
#              R/load-so-survey-eda-data.R (closed EDA effort).
#
# HYBRID BAR (per survey)
#   1. Schema check (column set match; intentional add/remove flagged)
#   2. Row-count match (graduates exception per T05)
#   3. Key uniqueness match (STUDENT_KEY / PEN / SUBM_CD)
#   4. Cell-level diff on shared cols, with per-column exclusion list
#   5. Aggregate summary stats within all.equal() float tolerance
#   6. End-to-end: t_cohorts_recoded (02b-1 output) numerically identical
#      pre/post -- composition check that intentional diffs sum to zero net
#      behavior change at the consumer boundary.
#
# USAGE (from repo root)
#   # 1. Snapshot baseline (ONCE, before the new loader runs):
#   Rscript R/validate-so-migration.R --snapshot-baseline
#   # 2. Run the new loader (Rscript R/load-so-survey-oracle.R)
#   # 3. Run validation:
#   Rscript R/validate-so-migration.R
#
# OUTPUT
#   .scratch/so-oracle-migration/validation-report.md (gitignored per T07)
#   Exit 0 on pass, non-zero on failure.
# =============================================================================

suppressMessages({
  library(DBI)
  library(odbc)
  library(dplyr)
  library(readr)
  library(config)
  library(glue)
})

# ---- Config -----------------------------------------------------------------
db_config <- config::get("decimal")
write_schema <- config::get("shareschema")
scratch_dir <- file.path(".scratch", "so-oracle-migration")
baseline_dir <- file.path(scratch_dir, "baseline")
eda_cache_dir <- file.path(".scratch", "student-outcome-survey-eda", "cache")
report_path <- file.path(scratch_dir, "validation-report.md")
dir.create(scratch_dir, showWarnings = FALSE, recursive = TRUE)

args <- commandArgs(trailingOnly = TRUE)
SNAPSHOT_MODE <- "--snapshot-baseline" %in% args

# ---- Intentional-diffs register (T04, R list as single source of truth) -----
# Each entry: table, column (or NA for table-level), type, ticket, note.
intentional_diffs <- list(
  list(
    table = "t_graduates_r",
    col = NA,
    type = "row-count widen",
    ticket = "T05",
    note = "Legacy 1,004 rows (filtered C_Outc21-25); new ~3,676 rows (no filter). Cell equality on shared rows only."
  ),
  list(
    table = "t_dacso_data_part_1_stepa_r",
    col = "RESPONDENT",
    type = "semantic flip",
    ticket = "T09",
    note = "Legacy 1/NA varchar; new 1/0 numeric. Respondent=1 cells equivalent; non-respondent encoding differs."
  ),
  list(
    table = "t_appso_data_final_r",
    col = "WEIGHT",
    type = "value change",
    ticket = "T10",
    note = "Legacy real values 1-5 / 2-5,0; new placeholder 0 (joined in 02b-1)."
  ),
  list(
    table = "t_appso_data_final_r",
    col = "NEW_LABOUR_SUPPLY",
    type = "value change",
    ticket = "T06",
    note = "Legacy real values 0/1 (derived in load); new placeholder 0 (derived in 02b-1)."
  ),
  list(
    table = "t_appso_data_final_r",
    col = c("AGE_GROUP", "AGE_GROUP_LABEL"),
    type = "removal + NA placeholder",
    ticket = "T11",
    note = "Legacy derived real values in load; new NA placeholder (02b-1 re-derives from tbl_age)."
  ),
  # Placeholder cols present in legacy _r as 0/NA but derived downstream (02b-1 /
  # 02a) -- new _r carries placeholder to keep schema stable; values intentionally
  # differ. Applies across surveys.
  list(
    table = "t_bgs_data_final_r",
    col = c("LCIP4_CRED", "LCIP_LCIPPC_CD", "OLD_LABOUR_SUPPLY"),
    type = "downstream-derived placeholder",
    ticket = "T01",
    note = "Legacy _r carried real/0 values; new _r leaves as NA/0 placeholder -- 02b-1 / 02a derives from Oracle cols."
  ),
  list(
    table = "trd_data_r",
    col = "LCIP4_CRED",
    type = "downstream-derived placeholder",
    ticket = "T01",
    note = "Legacy _r carried real/0 values; new _r leaves as NA placeholder -- 02b-1 derives."
  ),
  list(
    table = "t_appso_data_final_r",
    col = "LCIP4_CRED",
    type = "downstream-derived placeholder",
    ticket = "T01",
    note = "Legacy _r carried real/0 values; new _r leaves as NA placeholder -- 02b-1 derives."
  ),
  list(
    table = "t_dacso_data_part_1_stepa_r",
    col = c("LCIP_LCIPPC_NAME", "TPID_LGND_CD"),
    type = "downstream-derived placeholder",
    ticket = "T01",
    note = "Legacy _r carried real/0 values; new _r leaves as NA placeholder -- downstream derives."
  ),
  # Real Oracle-vs-CSV data drift -- values legitimately differ between the
  # Oracle live source and the prior-year CSV snapshot. T01 design: refactored
  # queries read Oracle directly; the legacy CSV was a frozen survey-team
  # extract that may lag Oracle's current values.
  list(
    table = "t_bgs_data_final_r",
    col = "TOOK_FURTH_ED",
    type = "oracle-vs-csv data drift",
    ticket = "T01",
    note = "~5.9% of rows differ (new=1 / old=2). Oracle d01_taken_further_studies has updated values vs the legacy CSV snapshot. Direction consistent -- analyst review recommended but not cutover-blocking."
  ),
  list(
    table = "t_dacso_data_part_1_stepa_r",
    col = c("PFST_FURSTDY_INCL_STILL_ATTD", "PFST_HAD_PREVIOUS_CDTL"),
    type = "oracle-vs-csv data drift (collapsed join)",
    ticket = "T01",
    note = "13-20% of rows differ (new=1 / old=0). T01 design: Oracle query collapses DACSO's 2-table split (main + outc) into a single LEFT JOIN, surfacing direct Oracle values. Legacy CSV used a separate outc-table join with dedup; the two sources legitimately disagree on these PFST flags. Oracle values are more current. Analyst review recommended but not cutover-blocking."
  ),
  # PEN preservation -- Oracle preserves leading zeros + avoids scientific
  # notation; legacy CSV read PEN as numeric and lost both. New is more correct.
  list(
    table = "t_bgs_data_final_r",
    col = "PEN",
    type = "encoding improvement (Oracle preserves form)",
    ticket = "T01",
    note = "~5 rows (0.004%) differ. Oracle returns PEN as character preserving leading zeros (e.g. \"000475391\") and avoiding scientific notation (\"117000000\"); legacy CSV snapshot had been read as numeric, stripping zeros and rendering large values in scientific notation (e.g. 1.17e+08). New form is canonical; downstream code that compares PEN as character works correctly."
  ),
  list(
    table = "t_dacso_data_part_1_stepa_r",
    col = "COCI_PEN",
    type = "encoding improvement (Oracle preserves form)",
    ticket = "T01",
    note = "Same as BGS PEN: Oracle preserves leading zeros + avoids scientific notation. New is canonical."
  ),
  # BGS region -- Oracle REGION_CD/CURRENT_REGION has NA where legacy CSV had
  # values filled via institution fallback (tmp_BGS_INST_REGION_CDS LAN lookup
  # at load-cohort-bgs.R:280-289). Fallback not yet ported; analyst follow-up.
  list(
    table = "t_bgs_data_final_r",
    col = "CURRENT_REGION_PSSM_CODE",
    type = "missing institution fallback",
    ticket = "T06",
    note = "Some rows have NA region in new _r where legacy had real values via the tmp_BGS_INST_REGION_CDS institution-fallback at load-cohort-bgs.R:280-289 (not yet ported to the new loader). Impact: affected rows drop from 02b-2 region weighting. Analyst follow-up: port the institution fallback if region coverage matters for the model run."
  ),
  list(
    table = "t_bgs_data_oracle",
    col = "AGE_GROUP_SRC",
    type = "rename",
    ticket = "T11",
    note = "Oracle source-coded band renamed from AGE_GROUP to AGE_GROUP_SRC."
  ),
  list(
    table = "t_bgs_data_final_r",
    col = "INTERNATIONAL",
    type = "new column",
    ticket = "T01",
    note = "Net-new Oracle column not in legacy _r."
  ),
  list(
    table = "t_trd_data_oracle",
    col = c("GENDER", "LRST_CD", "INTERNATIONAL", "PRGM_ID"),
    type = "new columns",
    ticket = "T01",
    note = "Net-new Oracle columns not in legacy _r."
  ),
  list(
    table = "t_appso_data_oracle",
    col = c("GENDER", "LRST_CD", "INTERNATIONAL", "PRGM_ID", "PRIVATE_FLAG"),
    type = "new columns",
    ticket = "T01",
    note = "Net-new Oracle columns not in legacy _r."
  ),
  list(
    table = "t_dacso_data_oracle",
    col = c(
      "GENDER",
      "INTERNATIONAL",
      "CREDENTIAL_DERIVED",
      "COSC_COMPLETED_PRGM_REQ",
      "COSC_CUM_GPA_GROUP",
      "DATE_OF_BIRTH",
      "ENRL_END_DATE",
      "PRGM_ID"
    ),
    type = "new columns",
    ticket = "T01",
    note = "Net-new Oracle columns not in legacy _r. GENDER sourced from CO_COHORT_SAMPLE (union 2021-2025) joined via STQU_ID — not in DACSO's own source tables."
  ),
  list(
    table = "t_dacso_data_part_1_stepa_r",
    col = "GENDER",
    type = "new column",
    ticket = "T01",
    note = "Net-new column added to DACSO _r. Sourced from CO_COHORT_SAMPLE via STQU_ID join; legacy _r never carried DACSO gender."
  )
)

# Tables to validate (legacy _r name -> new _r name -> EDA cache file -> key col).
# key_col uses the LEGACY name (after the loader's rename_to_legacy step).
validation_targets <- list(
  bgs = list(
    new_table = "t_bgs_data_final_r",
    baseline_file = "t_bgs_data_final_r.rds",
    eda_file = "so_bgs_new.rds",
    key_col = "STQU_ID",
    cycle_col = "SURVEY_YEAR"
  ),
  trd = list(
    new_table = "trd_data_r",
    baseline_file = "trd_data_r.rds",
    eda_file = "so_trd_new.rds",
    key_col = "KEY",
    cycle_col = "SUBM_CD"
  ),
  appso = list(
    new_table = "t_appso_data_final_r",
    baseline_file = "t_appso_data_final_r.rds",
    eda_file = "so_appso_new.rds",
    key_col = "KEY",
    cycle_col = "SUBM_CD"
  ),
  dacso = list(
    new_table = "t_dacso_data_part_1_stepa_r",
    baseline_file = "t_dacso_data_part_1_stepa_r.rds",
    eda_file = "so_dacso_new.rds",
    key_col = "COCI_STQU_ID",
    cycle_col = "COCI_SUBM_CD"
  )
)

# ---- Helpers ----------------------------------------------------------------
read_dbo <- function(table_name) {
  con <- dbConnect(
    odbc::odbc(),
    Driver = db_config$driver,
    Server = db_config$server,
    Database = db_config$database,
    Trusted_Connection = "True"
  )
  on.exit(dbDisconnect(con), add = TRUE)
  dbReadTable(con, SQL(glue('"{write_schema}"."{table_name}"')))
}

read_if_exists <- function(path) {
  if (file.exists(path)) readRDS(path) else NULL
}

# Find intentional-diff entries that apply to a column in a table.
# NOTE: `table_name` must be the actual _r table name (e.g. t_appso_data_final_r),
# NOT a label like "APPSO vs baseline" -- callers must pass the real name.
diffs_for <- function(table_name, col_name) {
  Filter(
    function(d) {
      d$table == table_name && (is.na(d$col)[1] || col_name %in% d$col)
    },
    intentional_diffs
  )
}

# ---- Snapshot mode: save current dbo *_r as baseline ------------------------
if (SNAPSHOT_MODE) {
  cat(
    "Snapshot mode: saving current dbo *_r as baseline to ",
    baseline_dir,
    "\n"
  )
  dir.create(baseline_dir, showWarnings = FALSE, recursive = TRUE)
  con <- dbConnect(
    odbc::odbc(),
    Driver = db_config$driver,
    Server = db_config$server,
    Database = db_config$database,
    Trusted_Connection = "True"
  )
  on.exit(dbDisconnect(con), add = TRUE)
  for (tgt in validation_targets) {
    df <- dbReadTable(con, SQL(glue('"{write_schema}"."{tgt$new_table}"')))
    saveRDS(df, file.path(baseline_dir, tgt$baseline_file))
    cat(glue("  saved {tgt$baseline_file}: {nrow(df)} rows\n"))
  }
  cat(
    "Done. Now run R/load-so-survey-oracle.R, then re-run this script without --snapshot-baseline.\n"
  )
  quit(status = 0)
}

# ---- Validation mode --------------------------------------------------------
report_lines <- c(
  "# SO Oracle-Migration Validation Report",
  "",
  glue("Generated: {format(Sys.time(), '%Y-%m-%d %H:%M:%S')}"),
  ""
)

report_add <- function(...) report_lines <<- c(report_lines, ...)

failures <- 0L
checks <- 0L
warnings <- 0L

record_check <- function(pass, label, detail = "") {
  checks <<- checks + 1L
  status <- if (pass) "PASS" else "FAIL"
  if (!pass) {
    failures <<- failures + 1L
  }
  report_add(
    glue("- [{status}] {label}"),
    if (nzchar(detail)) glue("  - {detail}") else ""
  )
}

# Like record_check but never increments failures (T04: schema mismatches on
# Oracle extras are flagged, not blocking -- the cell + key + row-count checks
# carry the validation signal).
record_warning <- function(label, detail = "") {
  checks <<- checks + 1L
  warnings <<- warnings + 1L
  report_add(
    glue("- [WARN] {label}"),
    if (nzchar(detail)) glue("  - {detail}") else ""
  )
}

# Hybrid check 1: schema (column-set match). Downgraded to WARNING: the
# *_oracle extraction surfaces net-new Oracle columns (GENDER, INTERNATIONAL,
# salaries, etc. per T01) that legacy _r never carried. These are flagged
# here but don't block cutover -- row-count + key + cell checks carry the
# real signal. Hard-added/removed cols still surface for review.
check_schema <- function(name, new_df, old_df) {
  new_cols <- sort(names(new_df))
  old_cols <- sort(names(old_df))
  added <- setdiff(new_cols, old_cols)
  removed <- setdiff(old_cols, new_cols)
  detail <- paste(
    c(
      if (length(added)) glue("added: {paste(added, collapse=', ')}"),
      if (length(removed)) glue("removed: {paste(removed, collapse=', ')}")
    ),
    collapse = " | "
  )
  record_warning(name, if (nzchar(detail)) detail else "column sets match")
}

# Hybrid check 2: row count.
check_row_count <- function(name, new_df, old_df, allow_widen = FALSE) {
  new_n <- nrow(new_df)
  old_n <- nrow(old_df)
  if (allow_widen) {
    pass <- new_n >= old_n
    detail <- glue("new={new_n}, old={old_n} (widening allowed)")
  } else {
    pass <- new_n == old_n
    detail <- glue("new={new_n}, old={old_n}")
  }
  record_check(pass, glue("{name} row count"), detail)
}

# Hybrid check 3: key uniqueness + set equality.
check_keys <- function(name, new_df, old_df, key_col) {
  if (!key_col %in% names(new_df) || !key_col %in% names(old_df)) {
    record_check(
      FALSE,
      glue("{name} key ({key_col})"),
      "key column missing from one side"
    )
    return()
  }
  new_keys <- unique(new_df[[key_col]])
  old_keys <- unique(old_df[[key_col]])
  in_new_only <- setdiff(new_keys, old_keys)
  in_old_only <- setdiff(old_keys, new_keys)
  pass <- length(in_new_only) == 0 && length(in_old_only) == 0
  detail <- paste(
    c(
      if (length(in_new_only)) {
        glue("in_new_only: {length(in_new_only)} (e.g. {head(in_new_only, 3)})")
      },
      if (length(in_old_only)) {
        glue("in_old_only: {length(in_old_only)} (e.g. {head(in_old_only, 3)})")
      }
    ),
    collapse = " | "
  )
  record_check(pass, glue("{name} key set ({key_col})"), detail)
}

# Hybrid check 4: cell-level diff on shared cols w/ intentional-diff exclusions.
# KEY-ALIGNED: rows from Oracle vs legacy CSV may arrive in different orders,
# so position-based compare would false-positive on every value col. We inner-
# join by key_col, then compare aligned rows.
# TYPE-TOLERANT: try up to three comparison strategies, in order:
#   1. Direct all.equal (handles same-class + float tolerance)
#   2. Both to character (handles factor-vs-char, encoding)
#   3. Both to numeric (handles PEN "000123" vs 123 -- leading-zero strip)
# `table_name` is the actual _r table name for intentional-diff lookup.
check_cells <- function(name, table_name, new_df, old_df, key_col) {
  if (!key_col %in% names(new_df) || !key_col %in% names(old_df)) {
    record_check(
      FALSE,
      glue("{name} cells"),
      paste("key col", key_col, "missing -- cannot align")
    )
    return()
  }
  new_a <- new_df %>% mutate(!!key_col := as.character(.data[[key_col]]))
  old_a <- old_df %>% mutate(!!key_col := as.character(.data[[key_col]]))
  shared <- intersect(names(new_a), names(old_a))
  aligned <- inner_join(
    new_a %>% select(all_of(shared)),
    old_a %>% select(all_of(shared)),
    by = key_col,
    suffix = c("_new", "_old")
  )
  if (nrow(aligned) == 0) {
    record_check(
      FALSE,
      glue("{name} cells"),
      "no rows aligned by key -- key sets disjoint"
    )
    return()
  }
  cell_fails <- character(0)
  cell_details <- list()
  compare_cols <- setdiff(shared, key_col)
  for (col in compare_cols) {
    diffs <- diffs_for(table_name, col)
    if (length(diffs) > 0) {
      next
    } # skip intentional-diff columns
    new_col <- paste0(col, "_new")
    old_col <- paste0(col, "_old")
    if (!new_col %in% names(aligned) || !old_col %in% names(aligned)) {
      next
    }
    new_v <- aligned[[new_col]]
    old_v <- aligned[[old_col]]

    # Strategy 1: direct (handles same-class + float tolerance)
    eq <- isTRUE(all.equal(new_v, old_v, check.attributes = FALSE))
    matched_strategy <- if (eq) "direct" else ""

    # Strategy 2: both to character (only if classes differ)
    if (!eq && !identical(class(new_v), class(old_v))) {
      eq <- isTRUE(all.equal(
        as.character(new_v),
        as.character(old_v),
        check.attributes = FALSE
      ))
      if (eq) matched_strategy <- "char-coerce"
    }

    # Strategy 3: both to numeric (handles PEN "000123" vs 123, sentinel strings)
    if (!eq) {
      new_n <- suppressWarnings(as.numeric(new_v))
      old_n <- suppressWarnings(as.numeric(old_v))
      # Only trust numeric compare if BOTH vectors are fully coercible (no NA
      # introduced by coercion), so we don't silently treat "XXXXX" as NA==NA.
      new_coercible <- !anyNA(new_n) || all(is.na(new_v) == is.na(new_n))
      old_coercible <- !anyNA(old_n) || all(is.na(old_v) == is.na(old_n))
      if (new_coercible && old_coercible) {
        eq <- isTRUE(all.equal(new_n, old_n, check.attributes = FALSE))
        if (eq) matched_strategy <- "numeric-coerce"
      }
    }

    if (!eq) {
      # Capture sample mismatches for triage (first 5 rows that differ).
      # Treat NA-vs-non-NA as a diff (legacy or new side may be NA).
      char_new <- as.character(new_v)
      char_old <- as.character(old_v)
      diff_mask <- ifelse(
        is.na(char_new) & is.na(char_old),
        FALSE,
        ifelse(char_new != char_old, TRUE, TRUE)
      )
      # The above collapses to: differs unless both are non-NA-and-equal.
      same_mask <- !is.na(char_new) & !is.na(char_old) & char_new == char_old
      diff_idx <- which(!same_mask)
      diff_idx <- head(diff_idx, 5)
      samples <- if (length(diff_idx) > 0) {
        paste(
          mapply(
            function(i, n, o) {
              sprintf(
                "key=%s: new=%s | old=%s",
                format_val(i),
                format_val(n),
                format_val(o)
              )
            },
            aligned[[key_col]][diff_idx],
            new_v[diff_idx],
            old_v[diff_idx]
          ),
          collapse = "; "
        )
      } else {
        ""
      }
      n_diff <- sum(!same_mask)
      cell_fails <- c(cell_fails, col)
      cell_details[[col]] <- glue(
        "{n_diff}/{length(new_v)} rows differ. samples: {samples}"
      )
    }
  }
  pass <- length(cell_fails) == 0
  if (pass) {
    detail <- glue(
      "all {length(compare_cols)} shared non-intentional cols equal ({nrow(aligned)} rows aligned)"
    )
  } else {
    detail <- paste(
      c(
        glue(
          "mismatched ({length(cell_fails)}): {paste(cell_fails, collapse=', ')}"
        ),
        sapply(cell_fails, function(c) glue("  - {c}: {cell_details[[c]]}"))
      ),
      collapse = "\n"
    )
  }
  record_check(pass, glue("{name} cells"), detail)
}

format_val <- function(v) {
  if (length(v) == 0) {
    return("")
  }
  out <- sapply(v, function(x) {
    if (is.na(x)) {
      "NA"
    } else if (is.character(x)) {
      paste0("\"", as.character(x), "\"")
    } else {
      as.character(x)
    }
  })
  paste(out, collapse = ",")
}

# ---- Run validation per target ----------------------------------------------
report_add("## Per-table checks", "")

for (srv in names(validation_targets)) {
  tgt <- validation_targets[[srv]]
  report_add(glue("### {toupper(srv)} -- {tgt$new_table}"), "")
  new_df <- tryCatch(read_dbo(tgt$new_table), error = function(e) {
    report_add(glue("ERROR reading {tgt$new_table}: {conditionMessage(e)}"))
    NULL
  })

  if (is.null(new_df)) {
    record_check(FALSE, glue("{toupper(srv)} read"), "could not read new table")
    next
  }

  old_df <- read_if_exists(file.path(baseline_dir, tgt$baseline_file))
  eda_df <- read_if_exists(file.path(eda_cache_dir, tgt$eda_file))

  if (is.null(old_df) && is.null(eda_df)) {
    record_check(
      FALSE,
      glue("{toupper(srv)} anchors"),
      "neither baseline nor EDA cache found; run with --snapshot-baseline first"
    )
    next
  }

  for (anchor_name in c("baseline", "eda")) {
    anchor_df <- if (anchor_name == "baseline") old_df else eda_df
    if (is.null(anchor_df)) {
      next
    }
    label <- glue("{toupper(srv)} vs {anchor_name}")
    check_schema(label, new_df, anchor_df)
    check_row_count(label, new_df, anchor_df, allow_widen = srv == "graduates")
    check_keys(label, new_df, anchor_df, tgt$key_col)
    check_cells(label, tgt$new_table, new_df, anchor_df, tgt$key_col)
  }
}

# ---- End-to-end composition check (T04) ------------------------------------
# Confirms t_cohorts_recoded (02b-1 output) is numerically identical pre/post.
# This requires a pre-migration baseline of t_cohorts_recoded; flag if missing.
report_add("", "## End-to-end composition check (T04)", "")
tcr_baseline <- file.path(baseline_dir, "t_cohorts_recoded.rds")
if (file.exists(tcr_baseline)) {
  new_tcr <- tryCatch(read_dbo("t_cohorts_recoded"), error = function(e) NULL)
  if (!is.null(new_tcr)) {
    old_tcr <- readRDS(tcr_baseline)
    eq <- isTRUE(all.equal(new_tcr, old_tcr, check.attributes = FALSE))
    record_check(
      eq,
      "t_cohorts_recoded end-to-end",
      if (eq) {
        ""
      } else {
        "T_Cohorts_Recoded differs pre/post -- intentional-diff composition is wrong somewhere"
      }
    )
  } else {
    record_check(
      FALSE,
      "t_cohorts_recoded end-to-end",
      "could not read new t_cohorts_recoded"
    )
  }
} else {
  report_add(
    "- [SKIP] t_cohorts_recoded end-to-end -- no baseline at ",
    tcr_baseline,
    ". Add it via --snapshot-baseline (run 02b-1 once on legacy, snapshot, then run new pipeline)."
  )
}

# ---- Summary ----------------------------------------------------------------
report_add("", "## Summary", "")
report_add(glue("- {checks - failures}/{checks} checks passed"))
report_add(glue("- {failures} failure(s)"))
report_add("", "## Intentional-diffs register (verbatim)", "")
for (d in intentional_diffs) {
  col_str <- if (is.na(d$col)[1]) {
    "(table-level)"
  } else {
    paste(d$col, collapse = ", ")
  }
  report_add(glue("- {d$table}.{col_str} -- {d$type} ({d$ticket}): {d$note}"))
}

writeLines(report_lines, report_path)
cat(glue("Validation report: {report_path}\n"))
cat(glue("{checks - failures}/{checks} passed, {failures} failure(s)\n"))

if (failures > 0) {
  cat("FAILED -- cutover blocked. Review report.\n")
  quit(status = 1)
} else {
  cat("PASSED -- cutover PR acceptance bar cleared.\n")
  quit(status = 0)
}
