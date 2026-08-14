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
# Student Outcome Survey -- Oracle-direct loader (so-oracle-migration build)
#
# PURPOSE
#   Replace the CSV-LAN read paths in load-outcomes-data.R +
#   load-cohort-{bgs,trd,appso,dacso}.R with Oracle-INFOWARE-direct reads.
#   Produces two layers in PSSM2025 dbo per wayfinder tickets T02/T03/T05/
#   T06/T07/T10/T11:
#
#     Layer 1 -- *_oracle (standardized extraction): pure Oracle output
#         from the refactored queries in sql/student_outcome_sql/refactored/.
#         Adds no LAN lookups; Oracle source-coded age band surfaced as
#         AGE_GROUP_SRC (T11 rename).
#     Layer 2 -- *_r (cohort-cleaned): *_oracle + CURRENT_REGION_PSSM_CODE
#         recode + placeholder NA/0 columns (AGE_GROUP, AGE_GROUP_LABEL,
#         NEW_LABOUR_SUPPLY, WEIGHT) that 02b-1 populates from LAN lookups
#         (tbl_age, tbl_age_groups, T_Weights) and APP_LABR_* derivations.
#
#   Also writes per-survey .rds + .parquet snapshots to
#   .scratch/so-oracle-migration/cache/ (T07 cache-also) and the stacked
#   so_combined.rds built from *_r common cols + per-survey extras (T02).
#
# USAGE (from repo root)
#   Rscript R/load-so-survey-oracle.R            # production: fresh extract
#   USE_CACHE=TRUE Rscript R/load-so-survey-oracle.R   # dev: skip Oracle,
#                                                      # load from .rds cache
#
# CAVEAT
#   This loader was written as part of the so-oracle-migration build. The
#   validation script R/validate-so-migration.R is the source of truth for
#   row-for-row equivalence with the legacy CSV-based *_r tables. If
#   validation fails, the column lists / placeholder defaults below are the
#   first place to look.
# =============================================================================

suppressMessages({
  library(DBI)
  library(odbc)
  library(dbplyr)
  library(dplyr)
  library(readr)
  library(config)
  library(futile.logger)
  library(glue)
})

# ---- Logging setup ----------------------------------------------------------
log_file <- "./R/execution_log.txt"
flog.appender(appender.file(log_file), name = "file_logger")
flog.threshold(INFO, name = "file_logger")

log_info <- function(msg) {
  flog.info(msg, name = "file_logger")
  print(paste(Sys.time(), "|", msg))
}

log_info("==== load-so-survey-oracle.R START ====")

# ---- Flags + config ---------------------------------------------------------
# USE_CACHE=TRUE skips Oracle extraction and loads per-survey data from the
# .rds cache instead (T07 dev bypass). Default FALSE = always extract fresh.
USE_CACHE <- tolower(Sys.getenv("USE_CACHE", "FALSE")) %in%
  c("true", "1", "yes")
if (USE_CACHE) {
  log_info("USE_CACHE=TRUE: loading from .rds cache, skipping Oracle")
}

sql_dir <- file.path("sql", "student_outcome_sql", "refactored")
cache_dir <- file.path(".scratch", "so-oracle-migration", "cache")
dir.create(cache_dir, showWarnings = FALSE, recursive = TRUE)
manifest <- list()

iw_config <- config::get("infoware")
db_config <- config::get("decimal")
write_schema <- config::get("shareschema")
lan <- config::get("lan")

# ---- Helpers ----------------------------------------------------------------
read_sql <- function(path) {
  lines <- readLines(path, warn = FALSE)
  lines <- lines[!grepl(r"(^\s*--)", lines)]
  paste(lines[nzchar(trimws(lines))], collapse = "\n")
}

# Parquet writer -- arrow is the default; duckdb is a build-time swap (T07).
write_parquet_safe <- function(data, path) {
  if (requireNamespace("arrow", quietly = TRUE)) {
    arrow::write_parquet(data, path)
  } else if (requireNamespace("duckdb", quietly = TRUE)) {
    duckdb_write_parquet(data, path)
  } else {
    log_info(paste(
      "Neither arrow nor duckdb available; skipping parquet write for",
      basename(path)
    ))
  }
}

duckdb_write_parquet <- function(data, path) {
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  DBI::dbWriteTable(con, "tmp", as.data.frame(data), overwrite = TRUE)
  duckdb::dbExecute(con, glue("COPY tmp TO '{path}' (FORMAT PARQUET);"))
}

record_manifest <- function(name, rows, cols, source) {
  manifest[[length(manifest) + 1]] <<- data.frame(
    artifact = name,
    rows = as.integer(rows),
    cols = as.integer(cols),
    source = source,
    extracted_at = format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  )
}

# Region recode. TRD/APPSO/DACSO carry CURRENT_REGION1/CURRENT_REGION4 (split
# form); BGS carries REGION_CD (primary, 1-8) + CURRENT_REGION (rollup form,
# values 5-10) per load-cohort-bgs.R:266-273.
add_region_recode <- function(data, survey) {
  if (survey == "bgs") {
    # BGS: REGION_CD is primary (1-8); CURRENT_REGION maps rollup values.
    # Mirrors load-cohort-bgs.R:266-273. Institution-based fallback
    # (tmp_BGS_INST_REGION_CDS for non-respondents w/ missing region) is NOT
    # applied here -- add if validation shows it's needed for non-respondent
    # region coverage.
    data %>%
      mutate(
        CURRENT_REGION_PSSM_CODE = case_when(
          REGION_CD %in% 1:8 ~ REGION_CD,
          CURRENT_REGION %in% c(6, 9, 10) ~ 10,
          CURRENT_REGION == 7 ~ 11,
          CURRENT_REGION == 5 ~ 9,
          CURRENT_REGION == 8 ~ -1,
          TRUE ~ NA_real_
        )
      )
  } else {
    # TRD/APPSO/DACSO: CURRENT_REGION1 (1-8) or CURRENT_REGION4 rollup (5-8).
    data %>%
      mutate(
        CURRENT_REGION_PSSM_CODE = case_when(
          CURRENT_REGION1 %in% 1:8 ~ CURRENT_REGION1,
          CURRENT_REGION4 == 5 ~ 9,
          CURRENT_REGION4 == 6 ~ 10,
          CURRENT_REGION4 == 7 ~ 11,
          CURRENT_REGION4 == 8 ~ -1,
          TRUE ~ NA_real_
        )
      )
  }
}

# Trim trailing whitespace from character cols -- Oracle CHAR(N) right-pads
# with spaces (e.g. PEN "141667873           "). Apply post-extract so the
# new _r / _oracle match legacy CSV semantics.
trim_char_cols <- function(data) {
  chr_cols <- names(data)[vapply(data, is.character, logical(1))]
  for (col in chr_cols) {
    data[[col]] <- trimws(data[[col]])
  }
  data
}

# Per-survey placeholder columns written to *_r so the schema matches legacy
# _r exactly. Values are 0 (numeric) or NA (character) to mirror legacy
# `0 AS <col>` placeholders. 02b-1 supplies real values via T_Weights / tbl_age
# / case_on (T06/T10/T11).
placeholders_per_survey <- list(
  bgs = list(
    AGE_GROUP = 0,
    AGE_GROUP_ROLLUP = 0,
    NEW_LABOUR_SUPPLY = 0,
    WEIGHT = 0,
    OLD_LABOUR_SUPPLY = 0,
    WEIGHT_CIP = 0,
    LCIP4_CRED = NA_character_,
    LCIP_LCIPPC_CD = NA_character_
  ),
  trd = list(
    NEW_LABOUR_SUPPLY = 0,
    WEIGHT = 0,
    LCIP4_CRED = NA_character_
  ),
  appso = list(
    AGE_GROUP = 0,
    AGE_GROUP_LABEL = NA_character_,
    NEW_LABOUR_SUPPLY = 0,
    WEIGHT = 0,
    LCIP4_CRED = NA_character_
  ),
  dacso = list(
    AGE_GROUP = 0,
    AGE_GROUP_ROLLUP = 0,
    NEW_LABOUR_SUPPLY = 0,
    WEIGHT = 0,
    OLD_LABOUR_SUPPLY = 0,
    HAD_PREVIOUS_CREDENTIAL = 0,
    PFST_IN_POST_SEC_BEFORE = 0,
    TPID_LGND_CD = NA_real_,
    LCIP_LCIPPC_NAME = NA_character_
  )
)

add_placeholders <- function(data, survey) {
  ph <- placeholders_per_survey[[survey]]
  for (col in names(ph)) {
    if (!col %in% names(data)) data[[col]] <- ph[[col]]
  }
  data
}

# Per-survey rename maps: standardized Oracle name -> legacy _r name.
# Required so 02b-1 (which reads legacy survey-prefixed names) finds columns.
# Derived from validation-report.md schema diffs (post-cutover run).
# The *_oracle extraction layer keeps standardized names; only *_r is renamed.
rename_maps <- list(
  bgs = c(
    STUDENT_KEY = "STQU_ID",
    RESPONDENT = "SRV_Y_N",
    AGE_AT_SURVEY = "AGE",
    LABR_IN_LABOUR_MARKET = "IN_LBR_FRC",
    LABR_EMPLOYED = "EMPLOYED",
    LABR_UNEMPLOYED = "UNEMPLOYED",
    LABR_JOB_TRAINING_RELATED = "TRAINING_RELATED",
    NOC_CD = "NOC",
    LCP4_CD = "CIP_CODE_4"
  ),
  trd = c(
    STUDENT_KEY = "KEY",
    LCP4_CD = "LCIP_LCP4_CD",
    AGE_AT_SURVEY = "TRD_AGE_AT_SURVEY",
    LABR_IN_LABOUR_MARKET = "TRD_LABR_IN_LABOUR_MARKET",
    LABR_EMPLOYED = "TRD_LABR_EMPLOYED",
    LABR_UNEMPLOYED = "TRD_LABR_UNEMPLOYED",
    LABR_JOB_SEARCH_TIME_GP = "TRD_LABR_JOB_SEARCH_TIME_GP",
    LABR_JOB_TRAINING_RELATED = "TRD_LABR_JOB_TRAINING_RELATED"
  ),
  appso = c(
    STUDENT_KEY = "KEY",
    LCP4_CD = "LCIP_LCP4_CD",
    AGE_AT_SURVEY = "APP_AGE_AT_SURVEY",
    LABR_IN_LABOUR_MARKET = "APP_LABR_IN_LABOUR_MARKET",
    LABR_EMPLOYED = "APP_LABR_EMPLOYED",
    LABR_UNEMPLOYED = "APP_LABR_UNEMPLOYED",
    LABR_JOB_TRAINING_RELATED = "APP_LABR_JOB_TRAINING_RELATED",
    TIME_TO_FIND_EMPLOY_MJOB = "APP_TIME_TO_FIND_EMPLOY_MJOB"
  ),
  dacso = c(
    STUDENT_KEY = "COCI_STQU_ID",
    PEN = "COCI_PEN",
    SUBM_CD = "COCI_SUBM_CD",
    INST = "COCI_INST_CD",
    LRST_CD = "COCI_LRST_CD",
    AGE_AT_SURVEY = "COCI_AGE_AT_SURVEY",
    CURRENT_REGION1 = "TPID_CURRENT_REGION1",
    CURRENT_REGION4 = "TPID_CURRENT_REGION4",
    GRADSTAT = "COSC_GRAD_STATUS_LGDS_CD",
    GRADSTAT_GROUP = "COSC_GRAD_STATUS_LGDS_CD_GROUP",
    PSSM_CREDENTIAL = "PRGM_CREDENTIAL",
    NOC_CD = "LABR_OCCUPATION_LNOC_CD"
  )
)

# Per-survey legacy placeholder columns (NA/0) that 02b-1 or downstream might
# read but Oracle doesn't supply. Mirrors legacy _r schema; safe as placeholder.
# (Folded into placeholders_per_survey above; this stub kept for compatibility.)
legacy_placeholders <- list(
  bgs = list(),
  trd = list(),
  appso = list(),
  dacso = list()
)

rename_to_legacy <- function(data, survey) {
  rmap <- rename_maps[[survey]]
  present <- intersect(names(rmap), names(data))
  if (length(present) > 0) {
    names(data)[match(present, names(data))] <- rmap[present]
  }
  ph <- legacy_placeholders[[survey]]
  for (col in names(ph)) {
    if (!col %in% names(data)) data[[col]] <- ph[[col]]
  }
  data
}

# ---- Connect ----------------------------------------------------------------
oracle_con <- if (!USE_CACHE) {
  tryCatch(
    dbConnect(
      odbc::odbc(),
      Driver = "Oracle in instantclient_19c",
      DBQ = iw_config$dbq,
      UID = iw_config$uid,
      PWD = iw_config$pwd
    ),
    error = function(e) {
      log_info(paste(
        "EZConnect failed (ORA-12154 expected); retrying with TNS descriptor"
      ))
      dbConnect(
        odbc::odbc(),
        Driver = "Oracle in instantclient_19c",
        DBQ = iw_config$desc,
        UID = iw_config$uid,
        PWD = iw_config$pwd
      )
    }
  )
} else {
  NULL
}

if (!is.null(oracle_con)) {
  log_info(paste("Connected to INFOWARE Oracle:", dbGetInfo(oracle_con)$dbname))
}

mssql_con <- dbConnect(
  odbc::odbc(),
  Driver = db_config$driver,
  Server = db_config$server,
  Database = db_config$database,
  Trusted_Connection = "True"
)
log_info("Connected to SQL Server (decimal / PSSM2025)")

# ---- Survey spec ------------------------------------------------------------
# Maps survey -> (sql_file, oracle_table, r_table, key_col).
# key_col is used for row-count + uniqueness checks in validation.
surveys <- list(
  bgs = list(
    sql = file.path(sql_dir, "01_bgs_data_std.sql"),
    oracle_tbl = "t_bgs_data_oracle",
    r_tbl = "t_bgs_data_final_r",
    key_col = "STUDENT_KEY"
  ),
  trd = list(
    sql = file.path(sql_dir, "02_trd_data_std.sql"),
    oracle_tbl = "trd_data_oracle",
    r_tbl = "trd_data_r",
    key_col = "STUDENT_KEY"
  ),
  appso = list(
    sql = file.path(sql_dir, "03_appso_data_std.sql"),
    oracle_tbl = "t_appso_data_oracle",
    r_tbl = "t_appso_data_final_r",
    key_col = "STUDENT_KEY"
  ),
  dacso = list(
    sql = file.path(sql_dir, "04_dacso_data_std.sql"),
    oracle_tbl = "t_dacso_data_oracle",
    r_tbl = "t_dacso_data_part_1_stepa_r",
    key_col = "STUDENT_KEY"
  )
)

# ---- Per-survey extract -> *_oracle + *_r + cache ---------------------------
extracted <- list() # holds in-memory copies for the stacked so_combined build

for (srv in names(surveys)) {
  spec <- surveys[[srv]]
  log_info(glue("---- {toupper(srv)} ----"))

  # 1. Get the extraction (fresh from Oracle or from .rds cache).
  rds_path <- file.path(cache_dir, paste0(spec$oracle_tbl, ".rds"))
  pq_path <- file.path(cache_dir, paste0(spec$oracle_tbl, ".parquet"))

  if (USE_CACHE && file.exists(rds_path)) {
    log_info(glue("USE_CACHE: loading {basename(rds_path)}"))
    df <- readRDS(rds_path)
  } else {
    log_info(glue("Extracting from Oracle via {basename(spec$sql)}"))
    df <- dbGetQuery(oracle_con, read_sql(spec$sql))
    saveRDS(df, rds_path)
    write_parquet_safe(df, pq_path)
  }
  log_info(glue("{toupper(srv)} extraction: {nrow(df)} rows x {ncol(df)} cols"))
  record_manifest(
    spec$oracle_tbl,
    nrow(df),
    ncol(df),
    source = if (USE_CACHE) "cache-rds" else "oracle"
  )

  # 2. Write *_oracle to dbo (Layer 1 -- pure extraction).
  dbWriteTable(
    mssql_con,
    name = SQL(glue('"{write_schema}"."{spec$oracle_tbl}"')),
    value = df,
    overwrite = TRUE
  )
  log_info(glue("Wrote {spec$oracle_tbl} to dbo"))

  # 3. Build *_r (Layer 2 -- extraction + trim + region recode + placeholders +
  #    legacy-name renames so 02b-1 finds columns by their survey-prefixed names).
  r_df <- df %>%
    trim_char_cols() %>%
    add_region_recode(srv) %>%
    add_placeholders(srv) %>%
    rename_to_legacy(srv)

  dbWriteTable(
    mssql_con,
    name = SQL(glue('"{write_schema}"."{spec$r_tbl}"')),
    value = r_df,
    overwrite = TRUE
  )
  log_info(glue("Wrote {spec$r_tbl} to dbo"))

  extracted[[srv]] <- r_df
}

# ---- Graduates (T05 widened -- all Oracle cycles) ---------------------------
grad_path <- file.path(cache_dir, "t_graduates_oracle.rds")
if (USE_CACHE && file.exists(grad_path)) {
  graduates <- readRDS(grad_path)
} else {
  log_info(
    "Extracting graduates from Oracle (T05: no cycle filter, all cycles)"
  )
  graduates <- dbGetQuery(
    oracle_con,
    read_sql(file.path(sql_dir, "05_graduates_std.sql"))
  )
  saveRDS(graduates, grad_path)
  write_parquet_safe(
    graduates,
    file.path(cache_dir, "t_graduates_oracle.parquet")
  )
}
dbWriteTable(
  mssql_con,
  name = SQL(glue('"{write_schema}"."t_graduates_oracle"')),
  value = graduates,
  overwrite = TRUE
)
dbWriteTable(
  mssql_con,
  name = SQL(glue('"{write_schema}"."t_graduates_r"')),
  value = graduates,
  overwrite = TRUE
)
record_manifest(
  "t_graduates_oracle",
  nrow(graduates),
  ncol(graduates),
  source = if (USE_CACHE) "cache-rds" else "oracle"
)
log_info(glue("Graduates: {nrow(graduates)} rows"))

# ---- Lookups + age-step + year-cycle (T06: LAN read, dbo *_r write) --------
# These stay LAN-sourced per T06. Re-read every loader run (status quo).
# Mirrors the lookup reads in load-cohort-{bgs,trd,appso,dacso}.R.
source_lookup <- function(csv_rel, out_tbl) {
  path <- glue("{lan}/{csv_rel}")
  if (!file.exists(path)) {
    log_info(glue("SKIP {out_tbl}: {csv_rel} not found on LAN"))
    return(invisible(NULL))
  }
  df <- read_csv(path, show_col_types = FALSE)
  dbWriteTable(
    mssql_con,
    name = SQL(glue('"{write_schema}"."{out_tbl}"')),
    value = df,
    overwrite = TRUE
  )
  log_info(glue("Wrote {out_tbl} ({nrow(df)} rows) from LAN"))
  record_manifest(out_tbl, nrow(df), ncol(df), source = "lan-csv")
}

# T_Weights is the critical one -- loaded by load-cohort-bgs.R today.
source_lookup(
  "development/csv/gh-source/lookups/02/T_Weights.csv",
  "t_weights_r"
)
source_lookup("development/csv/gh-source/lookups/02/tbl_Age.csv", "tbl_age_r")
source_lookup(
  "development/csv/gh-source/lookups/02/tbl_Age_Groups.csv",
  "tbl_age_groups_r"
)
source_lookup(
  "development/csv/gh-source/lookups/02/T_PSSM_Credential_Grouping.csv",
  "t_pssm_credential_grouping_r"
)
source_lookup(
  "development/csv/gh-source/lookups/02/T_Year_Survey_Year.csv",
  "t_year_survey_year_r"
)
source_lookup(
  "development/csv/gh-source/lookups/02/T_NOC_Broad_Categories_Updated.csv",
  "t_noc_broad_categories_r"
)
source_lookup(
  "development/csv/gh-source/lookups/02/T_Current_Region_PSSM_Codes.csv",
  "t_current_region_pssm_codes_r"
)
source_lookup(
  "development/csv/gh-source/lookups/02/T_Current_Region_PSSM_Rollup_Codes.csv",
  "t_current_region_pssm_rollup_codes_r"
)
source_lookup(
  "development/csv/gh-source/lookups/02/T_Current_Region_PSSM_Rollup_Codes_BC.csv",
  "t_current_region_pssm_rollup_codes_bc_r"
)

# ---- Stacked so_combined (T02 -- *_r common 27 cols + per-survey extras) ----
common_cols <- c(
  "SURVEY",
  "SUBM_CD",
  "SURVEY_YEAR",
  "STUDENT_KEY",
  "PEN",
  "RESPONDENT",
  "INST",
  "INST_NAME",
  "PSSM_CREDENTIAL",
  "LCP6_CD",
  "LCP6_DIGITS_NAME",
  "LCP4_CD",
  "LCP4_DIGITS_NAME",
  "TTRAIN",
  "AGE_AT_SURVEY",
  "AGE_GROUP",
  "CURRENT_REGION1",
  "CURRENT_REGION4",
  "LABR_IN_LABOUR_MARKET",
  "LABR_EMPLOYED",
  "LABR_UNEMPLOYED",
  "LABR_JOB_SEARCH_TIME_GP",
  "LABR_JOB_TRAINING_RELATED",
  "NOC_CD",
  "NOC_NAME",
  "GRADSTAT_GROUP",
  "INTERNATIONAL",
  "GENDER"
)

# bind_rows pads per-survey extras with NA automatically (T02).
stackable <- lapply(extracted, function(d) {
  # Include common cols that exist; misspelled/absent cols handled by bind_rows.
  present <- intersect(common_cols, names(d))
  d[, present, drop = FALSE]
})

# Type harmonization: Oracle emits the "same" column with different R types
# across surveys (e.g. LABR_EMPLOYED is numeric for BGS/TRD/APPSO but character
# for DACSO where it carries NULL/'1'/'0'). vctrs::vec_rbind (used by
# bind_rows) refuses to combine mismatched types. Detect cols whose class
# differs across surveys and coerce those to character (preserves data;
# analysts can re-cast downstream).
all_cols <- unique(unlist(lapply(stackable, names)))
class_sets <- lapply(all_cols, function(col) {
  classes <- lapply(stackable, function(d) {
    if (col %in% names(d)) class(d[[col]])[1] else NA_character_
  })
  unique(unlist(classes))
})
problem_cols <- all_cols[lengths(class_sets) > 1]
if (length(problem_cols) > 0) {
  log_info(glue(
    "Type-harmonizing {length(problem_cols)} cols to character (mixed classes across surveys): ",
    "{paste(problem_cols, collapse=', ')}"
  ))
  stackable <- lapply(stackable, function(d) {
    for (col in intersect(problem_cols, names(d))) {
      d[[col]] <- as.character(d[[col]])
    }
    d
  })
}

so_combined <- do.call(dplyr::bind_rows, stackable)

combined_path <- file.path(cache_dir, "so_combined.rds")
saveRDS(so_combined, combined_path)
record_manifest(
  "so_combined",
  nrow(so_combined),
  ncol(so_combined),
  source = "derived"
)
log_info(glue(
  "so_combined: {nrow(so_combined)} rows x {ncol(so_combined)} cols -> {combined_path}"
))

# ---- Manifest ---------------------------------------------------------------
man_df <- do.call(rbind, manifest)
write_csv(man_df, file.path(cache_dir, "manifest.csv"))
log_info(glue("Manifest: {nrow(man_df)} artifacts -> {cache_dir}/manifest.csv"))

# ---- Disconnect -------------------------------------------------------------
if (!is.null(oracle_con)) {
  dbDisconnect(oracle_con)
  log_info("Disconnected from Oracle")
}
dbDisconnect(mssql_con)
log_info("Disconnected from SQL Server")
log_info("==== load-so-survey-oracle.R END ====")

# Optional helper (T02): rebuild so_combined on demand from per-survey caches.
# Provided for analysts who want to rebind after custom filtering.
bind_so_surveys <- function(cache = cache_dir, surveys = names(surveys)) {
  stackable <- lapply(surveys, function(srv) {
    spec <- surveys[[srv]]
    path <- file.path(cache, paste0(spec$oracle_tbl, ".rds"))
    if (!file.exists(path)) {
      stop(glue("Missing {path}; run loader first"))
    }
    d <- readRDS(path)
    present <- intersect(common_cols, names(d))
    d[, present, drop = FALSE]
  })
  do.call(dplyr::bind_rows, stackable)
}
