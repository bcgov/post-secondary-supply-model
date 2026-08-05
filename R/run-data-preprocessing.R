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

# ******************************************************************************
# Run Data Preprocessing Pipeline
# ******************************************************************************
#
# WHAT:
#   Orchestrates the data preprocessing stage of the PSSM pipeline. This is the
#   stage that transforms raw STP enrolment/credential data and matches CIP codes
#   from four outcomes sources (DACSO, BGS, GRAD, APPSO). It must be run once
#   before any model run (regular, QI, or PTIB).
#
# WHY:
#   The model run scripts (prep-for-fresh-run, prep-for-qi-run, prep-for-ptib-run)
#   start from 02b onwards and assume all preprocessing tables already exist in
#   the database. This script produces those tables.
#
# PIPELINE SEQUENCE:
#   01a  Enrolment preprocessing          -- loads & cleans STP enrolment data
#   01b  Credential preprocessing         -- loads & cleans STP credential data
#   01c  Credential analysis              -- creates credential_non_dup, ranking,
#                                            gender/age cleaning, deduplication
#   02a  APPSO program matching           -- matches APPSO programs to CIP codes
#   02a  BGS program matching             -- matches BGS programs to CIP codes
#   02a  DACSO program matching           -- matches DACSO programs to CIP codes
#   02a  Update credential non dup        -- merges all four CIP sources into
#                                            credential_non_dup (DACSO > BGS >
#                                            GRAD > APPSO > STP fallback)
#   01d  Enrolment analysis               -- creates enrolment analysis tables
#   01e  STP distributions                -- creates credential & enrolment
#                                            distribution tables for projections
#
# PREREQUISITES:
#   Before running this script, ensure the following raw data has been loaded
#   into your SQL schema (these are one-time setup steps):
#     - load-stp-enrol.R          (STP enrolment raw data)
#     - load-stp-cred.R           (STP credential raw data)
#     - load-infoware-lookups.R   (INFOWARE CIP taxonomy + outcomes reference tables)
#     - load-outcomes-data.R      (BGS, DACSO, APPSO, TRD survey data)
#
# OUTPUT TABLES (written to SQL with _r suffix):
#   - STP_Enrolment_r, qry09c_minenrolment_r          (from 01a)
#   - STP_Credential_r                                 (from 01b)
#   - Credential_Non_Dup_r, tbl_credential_highest_rank_r  (from 01c)
#   - Various enrolment analysis tables                (from 01d)
#   - Credential_Non_Dup_APPSO_IDs_r                   (from 02a-appso)
#   - Credential_Non_Dup_BGS_IDs_r                     (from 02a-bgs)
#   - STP_Credential_Non_Dup_Programs_DACSO_r, etc.    (from 02a-dacso)
#   - Credential_Non_Dup_r (updated with final CIPs)   (from 02a-update)
#   - Distribution tables for credentials & enrolment  (from 01e)
#
# ******************************************************************************

library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(RJDBC)
library(futile.logger)
library(glue)

# ---- Load shared utilities (provides time_execution) ----
source("./R/utils.R")

# ---- Logging Setup ----
log_file <- "./R/execution_log.txt"
flog.appender(appender.file(log_file), name = "file_logger")
flog.threshold(INFO, name = "file_logger")

log_info <- function(msg) {
  flog.info(msg, name = "file_logger")
  print(paste(Sys.time(), "|", msg))
}

log_info("==========================================================")
log_info("==== DATA PREPROCESSING PIPELINE START ====")
log_info("==========================================================")

# ---- Configuration ----
my_schema <- config::get("myschema")
db_config <- config::get("decimal")

con <- dbConnect(
  odbc::odbc(),
  Driver = db_config$driver,
  Server = db_config$server,
  Database = db_config$database,
  Trusted_Connection = "True"
)

log_info(glue::glue(
  "Connected to SQL Server | Schema: {my_schema} | Database: {db_config$database}"
))

# ---- Preprocessing pipeline scripts (in execution order) ----
# Each script connects to and disconnects from the DB independently.
# They are sourced via time_execution which provides timing and error handling.

preprocessing_files <- c(
  "./R/01a-enrolment-preprocessing.R",
  "./R/01b-credential-preprocessing.R",
  "./R/01c-credential-analysis.R",
  "./R/01d-enrolment-analysis.R",
  "./R/02a-appso-programs.R",
  "./R/02a-bgs-program-matching.R",
  "./R/02a-dacso-program-matching.R",
  "./R/02a-update-cred-non-dup.R",
  "./R/01e-stp-distributions.R"
)

log_info(glue::glue(
  "Pipeline has {length(preprocessing_files)} scripts to execute"
))

# ---- Execute pipeline ----
pipeline_start <- Sys.time()

for (i in seq_along(preprocessing_files)) {
  file_path <- preprocessing_files[[i]]
  script_name <- basename(file_path)
  log_info(glue::glue(
    "[{i}/{length(preprocessing_files)}] Starting: {script_name}"
  ))

  tryCatch(
    {
      time_execution(file_path)
    },
    error = function(e) {
      log_info(glue::glue(
        "PIPELINE FAILED at step {i}/{length(preprocessing_files)}: {script_name}"
      ))
      log_info(glue::glue("Error: {e$message}"))
      stop(paste("Pipeline halted at", script_name))
    }
  )
}

# ---- Summary ----
pipeline_end <- Sys.time()
pipeline_elapsed <- round(
  difftime(pipeline_end, pipeline_start, units = "mins"),
  1
)

log_info("==========================================================")
log_info(glue::glue("==== DATA PREPROCESSING PIPELINE COMPLETE ===="))
log_info(glue::glue(
  "Total elapsed: {pipeline_elapsed} minutes ({length(preprocessing_files)} scripts)"
))
log_info("==========================================================")

# ---- Disconnect ----
dbDisconnect(con)
gc()
