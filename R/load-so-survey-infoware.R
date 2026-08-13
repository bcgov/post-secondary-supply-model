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
# See the License for the specific governing permissions and limitations under
# the License.

# =============================================================================
# Student Outcome Survey — standardized extraction from INFOWARE Oracle
#
# PURPOSE
#   Wraps the refactored Oracle SQL queries in sql/student_outcome_sql/
#   refactored/ as lazy dbplyr tbl objects so analysts can chain dplyr verbs
#   directly against the INFOWARE Oracle database without pulling data into R
#   until collect() is called.
#
#   At the end, the four survey data tables are stacked into a single
#   `so_combined` tbl (common-core columns only) for easy group_by / plot.
#
# SOURCE
#   The SQL text is read from the .sql files in
#   sql/student_outcome_sql/refactored/ — this script is a thin R wrapper;
#   change the SQL there, not here.
#
# CONNECTION
#   Uses config::get("infoware") (see config.yml). The EZConnect form
#   //scratch.bcgov:1521/pdbtrn may fail with ORA-12154 on some clients;
#   the full TNS descriptor is used as a fallback automatically.
#
# USAGE (from repo root)
#   source("R/load-so-survey-infoware.R")
#   # individual surveys (all survey-specific columns):
#   so_bgs %>% filter(SURVEY_YEAR == 2025) %>% collect()
#   # stacked (common-core columns, UNION ALL of all four surveys):
#   so_combined %>% group_by(SURVEY, SURVEY_YEAR) %>%
#     summarise(resp_rate = mean(RESPONDENT, na.rm = TRUE)) %>% collect()
#
# OUTPUTS (lazy tbl objects in .GlobalEnv)
#   so_bgs        BGS data (123k rows, 36 cols)
#   so_trd        TRD data (22k rows, 42 cols)
#   so_appso      APPSO data (23k rows, 41 cols)
#   so_dacso      DACSO data (147k rows, 45 cols)
#   so_graduates  TRD + APPSO graduate counts (1k rows, 8 cols)
#   so_combined   Four surveys stacked, common-core cols only (316k rows, 27 cols)
#   so_cip4_lookup   CIP 2021 4-digit classification
#   so_cip6_lookup   CIP 2021 6-digit classification
#   so_programs      Program master
#
#   Disconnect when done:  dbDisconnect(iw_con)
# =============================================================================

suppressMessages({
  library(DBI)
  library(odbc)
  library(dbplyr)
  library(dplyr)
  library(config)
  library(futile.logger)
})

## -------------------------- Logging Setup -------------------------------------------------------
## -----------------------------------------------------------------------------------------------
log_file <- "./R/execution_log.txt"
flog.appender(appender.file(log_file), name = "file_logger")
flog.threshold(INFO, name = "file_logger")

log_info <- function(msg) {
  flog.info(msg, name = "file_logger")
  print(paste(Sys.time(), "|", msg))
}

log_info("==== load-so-survey-infoware.R START ====")

sql_dir <- file.path("sql", "student_outcome_sql", "refactored")

# ---- Helper: read a .sql file and strip comment lines -----------------------
read_sql <- function(path) {
  lines <- readLines(path, warn = FALSE)
  lines <- lines[!grepl(r"(^\s*--)", lines)]
  paste(lines[nzchar(trimws(lines))], collapse = "\n")
}

# ---- Connect to INFOWARE Oracle ---------------------------------------------
iw_config <- config::get("infoware")

# EZConnect form first; fall back to full TNS descriptor if ORA-12154.
iw_con <- tryCatch(
  dbConnect(
    odbc::odbc(),
    Driver = "Oracle in instantclient_19c",
    DBQ = iw_config$dbq,
    UID = iw_config$uid,
    PWD = iw_config$pwd
  ),
  error = function(e) {
    desc <- iw_config$desc
    dbConnect(
      odbc::odbc(),
      Driver = "Oracle in instantclient_19c",
      DBQ = desc,
      UID = iw_config$uid,
      PWD = iw_config$pwd
    )
  }
)
log_info(paste("Connected to INFOWARE Oracle:", dbGetInfo(iw_con)$dbname))

# ---- Load each refactored query as a lazy tbl --------------------------------
# tbl(con, sql(...)) wraps the raw SQL in a subquery so dbplyr verbs chain
# on top without pulling data into R.

so_bgs <- tbl(iw_con, sql(read_sql(file.path(sql_dir, "01_bgs_data_std.sql"))))
so_trd <- tbl(iw_con, sql(read_sql(file.path(sql_dir, "02_trd_data_std.sql"))))
so_appso <- tbl(
  iw_con,
  sql(read_sql(file.path(sql_dir, "03_appso_data_std.sql")))
)
so_dacso <- tbl(
  iw_con,
  sql(read_sql(file.path(sql_dir, "04_dacso_data_std.sql")))
)
so_graduates <- tbl(
  iw_con,
  sql(read_sql(file.path(sql_dir, "05_graduates_std.sql")))
)

log_info("Loaded 5 survey data tbls (lazy)")

# ---- Lookups (three SELECT blocks in one file) -------------------------------
lk_raw <- read_sql(file.path(sql_dir, "07_lookups_std.sql"))
lk_blocks <- strsplit(lk_raw, ";")[[1]]
lk_blocks <- trimws(lk_blocks)
lk_blocks <- lk_blocks[grepl("^SELECT", lk_blocks)]

so_cip4_lookup <- tbl(iw_con, sql(lk_blocks[1]))
so_cip6_lookup <- tbl(iw_con, sql(lk_blocks[2]))
so_programs <- tbl(iw_con, sql(lk_blocks[3]))

log_info("Loaded 3 lookup tbls (lazy)")

# ---- Stack the four survey tables into one -----------------------------------
# Oracle UNION ALL across the four surveys fails with ORA-01790 because columns
# that are logically identical (NUMBER, VARCHAR2) have different internal
# subtypes (FLOAT vs DECIMAL, CHAR-vs-BYTE semantics) depending on the source
# table definition.  Rather than CAST every column, we collect each survey
# separately and bind_rows() in R — 316k rows fits comfortably in memory and
# gives analysts an immediate data.frame for group_by / plots.

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
  "INTERNATIONAL"
)

collect_common <- function(x, name) {
  log_info(paste("[collect_common] Collecting", name, "..."))
  t0 <- Sys.time()

  df <- x %>%
    select(all_of(common_cols)) %>%
    collect()

  log_info(sprintf("[collect_common] %s: %d rows collected in %.1fs",
                   name, nrow(df), as.numeric(difftime(Sys.time(), t0, units = "secs"))))

  df <- df %>%
    mutate(
      LABR_EMPLOYED = suppressWarnings(as.character(LABR_EMPLOYED))
    )

  log_info(sprintf("[collect_common] %s: LABR_EMPLOYED coerced to %s",
                   name, class(df$LABR_EMPLOYED)[1]))

  df
}

so_bgs_df   <- collect_common(so_bgs,   "BGS")
so_trd_df   <- collect_common(so_trd,   "TRD")
so_appso_df <- collect_common(so_appso, "APPSO")
so_dacso_df <- collect_common(so_dacso, "DACSO")

log_info("[bind_rows] Stacking 4 survey data frames ...")
so_combined <- bind_rows(
  so_bgs_df,
  so_trd_df,
  so_appso_df,
  so_dacso_df
)
log_info(sprintf("[bind_rows] so_combined: %d rows x %d cols",
                 nrow(so_combined), ncol(so_combined)))

log_info(sprintf("Created so_combined (4-survey UNION ALL, %d common cols)",
                 length(common_cols)))

log_info("Done. Disconnecting from INFOWARE Oracle.")
dbDisconnect(iw_con)
log_info("==== load-so-survey-infoware.R END ====")
