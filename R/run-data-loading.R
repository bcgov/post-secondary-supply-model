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
# Run Data Loading Pipeline
# ******************************************************************************
#
# WHAT:
#   Loads all raw source data into SQL Server before any preprocessing or model
#   run. This is the very first step in the PSSM pipeline and typically only needs
#   to be run once per data refresh cycle (e.g., new STP extract, new survey year).
#
# WHY:
#   The preprocessing scripts (run-data-preprocessing.R) assume that raw STP
#   enrolment/credential data, INFOWARE lookup tables, and student outcomes survey
#   data already exist in your SQL schema. This script ensures they are loaded.
#
# PIPELINE SEQUENCE:
#   load-stp-enrol.R           -- STP enrolment raw data (TSV -> SQL)
#   load-stp-cred.R            -- STP credential raw data (TSV -> SQL)
#   load-infoware-lookups.R    -- INFOWARE CIP taxonomy + outcomes reference tables
#                                 (Oracle INFOWARE -> SQL, one-time per cycle)
#   load-outcomes-data.R       -- BGS, DACSO, APPSO, TRD survey data
#                                 (CSV from LAN -> SQL)
#
# NEXT STEP:
#   After this script completes, run run-data-preprocessing.R to process the raw
#   data into analysis-ready tables.
#
# PREREQUISITES:
#   - VPN must be connected (LAN access required for STP and survey CSV files)
#   - Oracle Instant Client must be installed (for INFOWARE connection)
#   - SQL Server tables for STP data should be empty or dropped before append
#   - config.yml must have correct paths and connection settings
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
log_info("==== DATA LOADING PIPELINE START ====")
log_info("==========================================================")

# ---- Configuration ----
my_schema <- config::get("myschema")
db_config <- config::get("decimal")

log_info(glue::glue(
  "Schema: {my_schema} | Database: {db_config$database}"
))

# ---- Data loading scripts (in execution order) ----
# Each script connects to and disconnects from the DB independently.
# They are sourced via time_execution which provides timing and error handling.

data_loading_files <- c(
  "./R/load-stp-enrol.R",
  "./R/load-stp-cred.R",
  "./R/load-infoware-lookups.R",
  "./R/load-outcomes-data.R"
)

log_info(glue::glue(
  "Pipeline has {length(data_loading_files)} scripts to execute"
))

# ---- Execute pipeline ----
pipeline_start <- Sys.time()

for (i in seq_along(data_loading_files)) {
  file_path <- data_loading_files[[i]]
  script_name <- basename(file_path)
  log_info(glue::glue(
    "[{i}/{length(data_loading_files)}] Starting: {script_name}"
  ))

  tryCatch(
    {
      time_execution(file_path)
    },
    error = function(e) {
      log_info(glue::glue(
        "PIPELINE FAILED at step {i}/{length(data_loading_files)}: {script_name}"
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
log_info(glue::glue("==== DATA LOADING PIPELINE COMPLETE ===="))
log_info(glue::glue(
  "Total elapsed: {pipeline_elapsed} minutes ({length(data_loading_files)} scripts)"
))
log_info(glue::glue("Next step: run run-data-preprocessing.R"))
log_info("==========================================================")
