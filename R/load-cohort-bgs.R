# Copyright 2024 Province of British Columbia
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations under the License.

# This script loads student outcomes data for students who recently graduated with a
# Baccalaureate degree (Baccalaureate students are surveyed two years after graduation)
#
# BGS = Baccalaureate Graduate Student Outcomes.  Survey of graduates with a
# bachelor's degree, conducted roughly two years after graduation.
#
# This script does two things:
#
# 1) Copies the INFOWARE (Oracle) BGS distribution/cohort tables into SQL
#    Server (schema = shareschema/dbo, database = decimal/PSSM2025) as
#    INFOWARE_BGS_* tables.  These are the BGS survey records used by
#    02a-bgs-program-matching.R to assign programs (CIP codes) to BGS
#    graduates.
#
# 2) Reads the BGS survey responses + look-ups from the LAN and writes the
#    cleaned tables to SQL Server (dbo):
#
#    t_bgs_data_final -> t_bgs_data_final_r
#        Unique survey responses for each person/survey year (2021-2025
#        cycles for the 2027 model run).  Consumed by
#        02b-1-pssm-cohorts.R: institution re-code, CIP updates from program
#        matching, weights, labour supply, then stacked into
#        T_Cohorts_Recoded (master cohort table feeding credential analysis
#        01c-, labour supply 02b-2 and projections 04-/06-).
#
#    t_weights -> t_weights_r
#        Weights by model run / survey type / survey cycle.  Carried forward
#        from the last model run and updated with new data.  Applied to the
#        TRD, DACSO and BGS cohorts in 02b-1-pssm-cohorts.R.
#
#    t_bgs_inst_recode -> t_bgs_inst_recode_r
#        Look-up used to re-code several institution codes; joined to the
#        BGS cohort in 02b-1-pssm-cohorts.R.
#
# Notes: use query BGS_Q001_BGS_Data_2021_2025 (see LAN sql/ folder) for the 2027 model run.
# Some changes to variable names were done for consistency with the 2021-2025 file.

library(tidyverse)
library(config)
library(DBI)
library(odbc)
library(futile.logger)
# regular_run <- T
# qi_run <- F
# ptib_run <- F
## -------------------------- Logging Setup ------------------------------------------------------
## -----------------------------------------------------------------------------------------------
log_file <- "./R/execution_log.txt"
flog.appender(appender.file(log_file), name = "file_logger")
flog.threshold(INFO, name = "file_logger")

log_info <- function(msg) {
  flog.info(msg, name = "file_logger")
  print(paste(Sys.time(), "|", msg))
}

log_info("==== load-cohort-bgs.R START ====")

## -------------------------- Configure LAN Paths and DB Connection ------------------------------
## -----------------------------------------------------------------------------------------------

write_schema <- config::get("shareschema")

db_config <- config::get("decimal")
con <- dbConnect(
  odbc::odbc(),
  Driver = db_config$driver,
  Server = db_config$server,
  Database = db_config$database,
  Trusted_Connection = "True"
)

lan <- config::get("lan")

log_info("Connected to SQL Server database (decimal)")

#  ---- Connect to INFOWARE (Oracle) ----
# Requires the "Oracle in instantclient_19c" ODBC driver to be installed.
iw_config <- config::get("infoware")
odbcListDrivers() # confirm available drivers / Oracle client is present
iw_con <- dbConnect(
  odbc::odbc(),
  Driver = "Oracle in instantclient_19c",
  DBQ = iw_config$dbq,
  UID = iw_config$uid,
  PWD = iw_config$pwd
)
log_info("Connected to INFOWARE (Oracle)")

## --------------------------------------Required Tables------------------------------------------
## -----------------------------------------------------------------------------------------------

# ---- Load INFOWARE BGS tables to SQL Server (dbo / shareschema) ----
# Read BGS distribution/cohort tables from the INFOWARE (Oracle) database and write
# them to SQL Server. Chunked writes because the full datasets are too large for
# a single dbWriteTable call.
# BGS_DIST_20_24 / 21_25 = rolling six-year windows of
# BGS survey records; BGS_COHORT_INFO = BGS cohort information.  All are used
# by 02a-bgs-program-matching.R (reads them from dbo) to assign programs
# (CIP codes) to BGS graduates.  Source names in INFOWARE = table name minus
# the INFOWARE_ prefix (e.g. INFOWARE_BGS_DIST_20_24 <- INFOWARE.BGS_DIST_20_24).

# Load one INFOWARE table into SQL Server in chunks of chunk_size rows.
# Skips (without overwriting) if the target table already exists.
load_infoware_table_by_chunk <- function(
  iw_con,
  con,
  table_name,
  write_schema,
  chunk_size = 80000
) {
  target <- DBI::Id(schema = write_schema, table = table_name)

  if (dbExistsTable(con, target)) {
    log_info(glue::glue(
      "Table {write_schema}.{table_name} already exists; skipping"
    ))
    return(invisible(FALSE))
  }

  source_table <- sub("^INFOWARE_", "", table_name)
  data <- dbReadTable(
    iw_con,
    DBI::Id(schema = "INFOWARE", table = source_table)
  )

  # Some values in the Oracle source contain invalid UTF-8 byte sequences
  # (e.g. NOC_5_DIGIT_CODE_AND_NAME), which make odbcDataType()/nchar() fail
  # when writing.  Strip invalid bytes from all character columns.
  data <- data %>%
    mutate(across(
      where(is.character),
      ~ iconv(.x, from = "UTF-8", to = "UTF-8", sub = "")
    ))

  n <- nrow(data)
  starts <- seq(1, n, by = chunk_size)
  for (i in seq_along(starts)) {
    rows <- starts[i]:min(starts[i] + chunk_size - 1, n)
    dbWriteTable(
      con,
      target,
      data[rows, ],
      append = i > 1
    )
  }

  log_info(glue::glue(
    "Loaded {n} rows from INFOWARE.{source_table} to {write_schema}.{table_name}"
  ))
  invisible(TRUE)
}

# The INFOWARE BGS tables to copy over (first chunk writes the table,
# subsequent chunks append).  Written to dbo (shareschema) because
# 02a-bgs-program-matching.R reads them from there.
bgs_infoware_tables <- c(
  "INFOWARE_BGS_DIST_20_24",
  "INFOWARE_BGS_DIST_21_25",
  "INFOWARE_BGS_COHORT_INFO"
)
# We load six years of data for the analysis/model run.
walk(
  bgs_infoware_tables,
  load_infoware_table_by_chunk,
  iw_con = iw_con,
  con = con,
  write_schema = write_schema
)

dbDisconnect(iw_con)
log_info("Disconnected from INFOWARE (Oracle)")

# ---- Read LAN Data ----
# Lookups
# Weights by model run / survey type / survey cycle (Weight = regular run,
# WeightQI = QI run).  Carried forward from the last model run and applied to
# the TRD, DACSO and BGS cohorts in 02b-1-pssm-cohorts.R (weights scale
# survey respondents up to population counts for reporting).
# the weights are updated with 24-25 survey data for the 2027 model run, but need to be verified.
t_weights <-
  readr::read_csv(
    glue::glue("{lan}/development/csv/gh-source/lookups/02/T_Weights.csv"),
    col_types = cols(
      Group = "d",
      Weight = "d",
      WeightQI = "d",
      .default = col_character()
    )
  ) %>%
  janitor::clean_names(case = "all_caps") |>
  arrange(MODEL, SURVEY, SURVEY_YEAR)

# Look-up used for region re-coding: maps each institution to its current
# PSSM region (CURRENT_REGION_PSSM).  Used below as a fallback for BGS
# records whose survey region is missing/unknown.
tmp_bgs_inst_cds <-
  readr::read_csv(
    glue::glue(
      "{lan}/development/csv/gh-source/lookups/02/tmp_BGS_INST_REGION_CDS.csv"
    ),
    col_types = cols(.default = col_character())
  ) %>%
  janitor::clean_names(case = "all_caps")

# Look-up used to re-code several BGS institution codes to their current
# code.  Joined in 02b-1-pssm-cohorts.R so older institution codes are
# consolidated before weights are applied.
t_bgs_inst_recode <-
  readr::read_csv(
    glue::glue(
      "{lan}/development/csv/gh-source/lookups/02/T_BGS_INST_Recode.csv"
    ),
    col_types = cols(.default = col_character())
  ) %>%
  janitor::clean_names(case = "all_caps")


# ---- Read Outcomes Data ----
# BGS survey responses, one row per person per survey year (2021-2025
# cycles).  STQU_ID identifies the survey submission; SURVEY_YEAR dates the
# cycle.
bgs_data_update <- read_oracle_csv_auto(glue::glue(
  "{lan}/data/student-outcomes/csv/BGS_Q001_BGS_Data_2021_2025.csv"
))
log_info(glue::glue(
  "Read BGS_Q001_BGS_Data_2021_2025.csv: {nrow(bgs_data_update)} rows"
))

# Rename to the variable names used by the rest of the model (the source
# query used different names for the 2021-2025 file):
#   FULL_TM_WRK = working full-time, IN_LBR_FRC = in the labour force,
#   EMPLOYED / UNEMPLOYED = labour force status, TRAINING_RELATED = job
#   related to the field of study, TOOK_FURTH_ED = took further education.
# AGE_17_34 flags the young worker age band used for labour supply reporting;
# OLD_LABOUR_SUPPLY is a legacy placeholder not used downstream.
# PFST_CURRENTLY_STUDYING and SUBM_CD are dropped as not used downstream.
bgs_data_update <- bgs_data_update %>%
  rename(
    "FULL_TM_WRK" = FULL_TM,
    "IN_LBR_FRC" = LBR_FRC_LABOUR_MARKET,
    "EMPLOYED" = LBR_FRC_CURRENTLY_EMPLOYED,
    "UNEMPLOYED" = LBR_FRC_UNEMPLOYED,
    "TRAINING_RELATED" = E10_IN_TRAINING_RELATED_JOB,
    "TOOK_FURTH_ED" = D01_R1
  ) %>% # this can be added to original query
  mutate(AGE_17_34 = if_else(between(AGE, 17, 34), 1, 0)) %>%
  mutate(OLD_LABOUR_SUPPLY = NA) %>% # I don't think we use this?
  select(-c(PFST_CURRENTLY_STUDYING, SUBM_CD)) # nor these?

# Recode the survey's current-region fields into the standard PSSM region
# codes (same scheme as APPSO/DACSO/TRD so regions are comparable across
# cohorts; used for region-level outputs in 02b-1/02b-2).
bgs_data_update <- bgs_data_update %>%
  mutate(
    CURRENT_REGION_PSSM_CODE = case_when(
      REGION_CD %in% 1:8 ~ REGION_CD,
      CURRENT_REGION %in% c(6, 9, 10) ~ 10,
      CURRENT_REGION == 7 ~ 11,
      CURRENT_REGION == 5 ~ 9,
      CURRENT_REGION == 8 ~ -1,
      TRUE ~ NA
    )
  )

# Fallback: where the survey region is missing/unknown (-1/NA) and the
# graduate was NOT served by the institution (SRV_Y_N == 0), take the
# institution's current PSSM region from the tmp_BGS_INST_REGION_CDS
# look-up instead.
bgs_data_update <- bgs_data_update %>%
  inner_join(tmp_bgs_inst_cds, by = join_by(INST)) %>%
  mutate(
    CURRENT_REGION_PSSM_CODE = if_else(
      (CURRENT_REGION_PSSM_CODE == -1 | is.na(CURRENT_REGION_PSSM_CODE)) &
        (SRV_Y_N == 0 | is.na(SRV_Y_N)),
      as.numeric(CURRENT_REGION_PSSM),
      CURRENT_REGION_PSSM_CODE
    )
  )

# ---- Make T_BGS_Data_Final ----
# Drop the raw region variables now that CURRENT_REGION_PSSM_CODE is set.
# t_bgs_data_final is the BGS cohort consumed by 02b-1-pssm-cohorts.R.
t_bgs_data_final <- bgs_data_update %>%
  select(-c(CUR_RES, REGION_CD, CURRENT_REGION))
log_info(glue::glue(
  "t_bgs_data_final prepared: {nrow(t_bgs_data_final)} rows"
))

## ------------------------------------ Clean Up --------------------------------------------------
# Current workflow:
#  - Write key tables back to sql server.  These are tables needed for downstream work, or tables
# that might be needed for later reference outside of this analysis.
#  - Close DB connections
#  - Remove all objects at the end of each script.
## ------------------------------------------------------------------------------------------------

tables_to_keep <- c(
  "t_bgs_data_final",
  "t_weights",
  "t_bgs_inst_recode" #,
  #"tmp_bgs_inst_cds"
)

# Write each kept table to SQL Server as <name>_r.  write_table_to_db lives in
# R/utils.R (sourced by run-data-loading.R / the calling runner).
walk(tables_to_keep, write_table_to_db, schema = write_schema, con = con)
log_info(glue::glue(
  "Written to SQL Server ({write_schema}): {paste0(tables_to_keep, '_r', collapse = ', ')}"
))

dbDisconnect(con)
log_info("Disconnected from SQL Server database")
log_info("==== load-cohort-bgs.R END ====")
