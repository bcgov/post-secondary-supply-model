# Copyright 2024 Province of British Columbia
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations under the License.

library(tidyverse)
library(odbc)
library(DBI)
library(futile.logger)

## -------------------------- Logging Setup -------------------------------------------------------
## -----------------------------------------------------------------------------------------------
log_file <- "./R/execution_log.txt"
flog.appender(appender.file(log_file), name = "file_logger")
flog.threshold(INFO, name = "file_logger")

log_info <- function(msg) {
  flog.info(msg, name = "file_logger")
  print(paste(Sys.time(), "|", msg))
}

log_info("==== 01b-credential-preprocessing.R START ====")

## -------------------------- Configure LAN Paths and DB Connection ------------------------------
## -----------------------------------------------------------------------------------------------
db_config <- config::get("decimal")
my_schema <- config::get("myschema")

con <- dbConnect(
  odbc(),
  Driver = db_config$driver,
  Server = db_config$server,
  Database = db_config$database,
  Trusted_Connection = "True"
)
log_info("Connected to SQL Server database")

## --------------------------------------Required Tables------------------------------------------
# currently repurposing data from 2023 run.  When running next update, change this code to pull
# from the original raw credential dataset and then untoggle the commented sections below to reformat dates,
# and add an ID field.
## -----------------------------------------------------------------------------------------------

stp_credential <- dbGetQuery(
  con,
  glue::glue(
    'SELECT
    CREDENTIAL_AWARD_DATE,
    ENCRYPTED_TRUE_PEN,
    /*ID,*/
    PSI_CODE,
    PSI_CREDENTIAL_CATEGORY,
    PSI_CREDENTIAL_CIP,
    PSI_CREDENTIAL_LEVEL,
    PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    PSI_FULL_NAME,
    PSI_PEN,
    PSI_PROGRAM_CODE,
    PSI_PROGRAM_EFFECTIVE_DATE,
    PSI_SCHOOL_YEAR,
    PSI_STUDENT_NUMBER
  FROM "STP_Credential_2024"'
  )
)
log_info(glue::glue("Loaded STP_Credential: {nrow(stp_credential)} rows, {ncol(stp_credential)} columns"))

stp_enrolment_record_type <- dbReadTable(
  con,
  SQL(glue::glue('"{my_schema}"."stp_enrolment_record_type_r"'))
)
log_info(glue::glue("Loaded stp_enrolment_record_type_r: {nrow(stp_enrolment_record_type)} rows"))

stp_enrolment <- dbGetQuery(
  con,
  glue::glue(
    'SELECT
    ID,
    PSI_CIP_CODE,
    PSI_CODE,
    PSI_CREDENTIAL_CATEGORY,
    PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    PSI_STUDY_LEVEL
  FROM "{my_schema}"."stp_enrolment_r"'
  )
)
log_info(glue::glue("Loaded stp_enrolment_r: {nrow(stp_enrolment)} rows"))

## --------------------------------------Initial Data Checks--------------------------------------
## reference: source("./sql/01-credential-preprocessing/01-credential-preprocessing-sql.R")
## -----------------------------------------------------------------------------------------------

invalid_pen_count <- stp_credential |>
  filter(
    ENCRYPTED_TRUE_PEN %in%
      c("", " ", "(Unspecified)") |
      is.na(ENCRYPTED_TRUE_PEN)
  ) |>
  nrow()
log_info(glue::glue("Rows with invalid/missing ENCRYPTED_TRUE_PEN: {invalid_pen_count}"))

distinct_pen_count <- stp_credential |> distinct(ENCRYPTED_TRUE_PEN) |> nrow()
log_info(glue::glue("Distinct ENCRYPTED_TRUE_PEN values: {distinct_pen_count}"))

# Untoggle when running new data and/or add a conditional to test for the presence of the ID field.
stp_credential <- stp_credential |>
  mutate(ID = row_number()) |>
  relocate(ID, .before = CREDENTIAL_AWARD_DATE)

# ---- Reformat yy-mm-dd to yyyy-mm-dd ----
date_cols <- c(
  "CREDENTIAL_AWARD_DATE",
  "PSI_PROGRAM_EFFECTIVE_DATE"
)

stp_credential |> select(all_of(date_cols)) |> glimpse()

## --------------------------------------Reformat yy-mm-dd to yyyy-mm-dd---------------------------
## reference: source("./sql/01-enrolment-preprocessing/convert-date-scripts.R")
## adapted from all queries in the file
## -------------------------------------------------------------------------------------------------
convert_date <- function(vec) {
  # Years 26-99 go to 19xx
  # Years 00-25 go to 20xx
  yy <- as.numeric(substr(vec, 1, 2))

  century_prefix <- case_when(
    is.na(yy) ~ NA_character_,
    yy < 24 ~ "20",
    TRUE ~ "19"
  )

  lubridate::ymd(paste0(century_prefix, vec))
}

# Uncomment when running new data and/or add a conditional to test the date format.
# stp_credential <- stp_credential |>
#   mutate(
#     across(all_of(date_cols), .fns = convert_date, .names = "{.col}")
#   )

## --------------------------------------- Create Record Type Table -------------------------------
# reference: source("./sql/01-credential-preprocessing/01a-credential-preprocessing.R")
#
# Create lookup table for ID/Record Status and populate with ID column and EPEN
# Record Status codes:
# 0 = Good
# 1 = Missing Student Number
# 2 = Developmental
# 3 = No PSI Transition
# 4 = Credential Only (No Enrolment Record)
# 5 = PSI_Outside_BC
# 6 = Skills Based
# 7 = Developmental CIP
# 8 = Recommendation for Certification
#
# Notes:
#   1. Cips list may be the same list as defined in enrolement processing
# this could be defined in global file.
#   2. Comment on record-type 7 - SQL version skipped qry03g2 which filters on specific
# PSI_CODE and PSI_CREDENTIAL_PROGRAM_DESCRIPTION combinations. I Commented the code out (below)
# to maintain alignment but also becuase we need more clarification on the rationale for these specific
# filters. There may also be more exceptions to add (in previous years some manual checks were done, too).
## ------------------------------------------------------------------------------------------------

invalid_vals <- c("", " ", "(Unspecified)")
dev_cips <- c("21", "32", "33", "34", "35", "36", "37", "53", "89")


enrol_skills_lookup <- stp_enrolment |>
  # Join with enrolment record type to find the Status 6 records
  inner_join(stp_enrolment_record_type, by = "ID") |>
  filter(RecordStatus == 6) |>
  mutate(CIP2 = substr(PSI_CIP_CODE, 1, 2)) |>
  distinct(
    PSI_CODE,
    PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    CIP2,
    PSI_CREDENTIAL_CATEGORY,
    PSI_STUDY_LEVEL
  ) |>
  mutate(is_skills_match = TRUE)

log_info(glue::glue("Created enrol_skills_lookup: {nrow(enrol_skills_lookup)} distinct skill-based course combinations"))

stp_credential_record_type <- stp_credential |>
  mutate(CIP2 = substr(PSI_CREDENTIAL_CIP, 1, 2)) |>
  left_join(
    enrol_skills_lookup,
    by = c(
      "CIP2" = "CIP2",
      "PSI_CODE" = "PSI_CODE",
      "PSI_CREDENTIAL_PROGRAM_DESCRIPTION" = "PSI_CREDENTIAL_PROGRAM_DESCRIPTION",
      "PSI_CREDENTIAL_CATEGORY" = "PSI_CREDENTIAL_CATEGORY",
      "PSI_CREDENTIAL_LEVEL" = "PSI_STUDY_LEVEL"
    )
  ) |>
  mutate(
    RecordStatus = case_when(
      # --- Record Type 1 ---
      (ENCRYPTED_TRUE_PEN %in% invalid_vals) &
        (PSI_STUDENT_NUMBER %in% invalid_vals | PSI_CODE %in% invalid_vals) ~ 1,
      # --- Record Type 2 ---
      PSI_CREDENTIAL_LEVEL == "Developmental" ~ 2,
      # --- Record Type 6 ---
      is_skills_match == TRUE ~ 6,
      # --- Record Type 7 ---
      (CIP2 %in% dev_cips) ~ 7, # &
      #!((PSI_CODE == "UVIC" &
      #  PSI_CREDENTIAL_PROGRAM_DESCRIPTION ==
      #    "PROF SPEC CERTIFICATE IN MIDDLE YEARS LANG AND LITERACY") |
      #  (PSI_CODE == "NIC" &
      #    PSI_CREDENTIAL_PROGRAM_DESCRIPTION == "Aquaculture Technician 1") |
      #  (PSI_CODE == "NIC" &
      #    PSI_CREDENTIAL_PROGRAM_DESCRIPTION == "Coastal Forest Resource") |
      #  (PSI_CODE == "NIC" &
      #    PSI_CREDENTIAL_PROGRAM_DESCRIPTION ==
      #      "Underground Mining Essentials")) ~ 7,
      # --- Record Type 8 ---
      PSI_CREDENTIAL_CATEGORY == "Recommendation For Certification" ~ 8,
      # Default: leave other records as NA (or 0) for now
      TRUE ~ 0
    )
  ) |>
  select(ID, ENCRYPTED_TRUE_PEN, RecordStatus)

log_info("Credential RecordStatus assignment complete. Counts by status:")
log_info(paste(capture.output(print(stp_credential_record_type |> count(RecordStatus))), collapse = "\n"))


## ------------------------------------ Clean Up --------------------------------------------------
# Current workflow:
#  - Write key tables back to sql server.  These are tables needed for downstream work, or tables
# that might be needed for later reference outside of this analysis.
#  - Close DB connections
#  - Remove all objects at the end of each script.
## ------------------------------------------------------------------------------------------------

tables_to_keep <- c(
  "stp_credential",
  "stp_credential_record_type"
)

write_table_to_db <- function(table_name, schema, con) {
  db_name <- paste0(table_name, "_r")
  dbWriteTable(
    con,
    SQL(glue::glue('"{schema}"."{db_name}"')),
    base::get(table_name, envir = .GlobalEnv),
    overwrite = TRUE
  )
  log_info(glue::glue("Wrote table '{schema}.{db_name}' ({nrow(base::get(table_name, envir = .GlobalEnv))} rows) to SQL Server"))
}

log_info(glue::glue("Writing {length(tables_to_keep)} tables to DB: {paste(tables_to_keep, collapse = ', ')}"))
walk(tables_to_keep, write_table_to_db, schema = my_schema, con = con)

dbDisconnect(con)
log_info("Disconnected from SQL Server")

log_info("==== 01b-credential-preprocessing.R COMPLETE ====")


# rm(list = ls())
