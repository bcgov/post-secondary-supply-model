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
# Load STP enrolment data from staging area in LAN project folder, to decimal.
# Requested data to be in tab-separated text files partitioned by school year.
# The hope if that smaller datasets are more easily processed by R and SMSS
# ******************************************************************************
library(arrow)
library(tidyverse)
library(odbc)
library(DBI)
library(config)
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

log_info("==== load-stp-enrol.R START ====")

# ---- Configure LAN and file paths ----
lan <- config::get("lan")
my_schema <- config::get("myschema")

fls <- list.files(
  glue::glue("{lan}/Data/stp/BCSTATS_STP_ISA_PSSM_JUL13_2026/"),
  full.names = TRUE,
  recursive = FALSE
)
fls <- fls[grepl("STP_ENROLMENT", fls)]

log_info(glue::glue(
  "Found {length(fls)} STP enrolment file(s): {paste(basename(fls), collapse=', ')}"
))


## ----- Connection to decimal ----
db_config <- config::get("decimal")
con <- dbConnect(
  odbc(),
  Driver = db_config$driver,
  Server = db_config$server,
  Database = db_config$database,
  Trusted_Connection = "True"
)
# log_info(glue::glue("Connected to SQL Server | Database: {db_config$database}"))
# schema <-
#   schema(
#     PSI_PEN = string(),
#     PSI_BIRTHDATE = string(),
#     PSI_GENDER = string(),
#     PSI_STUDENT_NUMBER = string(),
#     PSI_STUDENT_POSTAL_CODE_FIRST_CONTACT = string(),
#     TRUE_PEN = string(),
#     ENCRYPTED_TRUE_PEN = string(),
#     STP_ALT_ID = string(),
#     LAST_SEEN_BIRTHDATE = string(),
#     LAST_SEEN_GENDER = string(),
#     BC_K_12_STUDENT_EVER = string(),
#     LAST_SEEN_INDIGENOUS_EVER_BACKDATED = string(), # data codes also different
#     LAST_SEEN_SCHOOL_YEAR = string(),
#     LAST_SEEN_DISTRICT_NUMBER = string(),
#     LAST_SEEN_DISTRICT_NAME = string(),
#     LAST_SEEN_STUDENT_GRADE = string(),
#     LAST_SEEN_SPECIAL_NEED_CODE = string(),
#     GRAD_YEAR_MONTH = string(),
#     AT_GRAD_DISTRICT_NUMBER = string(),
#     AT_GRAD_DISTRICT_NAME = string(),
#     AT_GRAD_CURRENT_COLLEGE_REGION_NAME = string(),
#     AT_GRAD_CURRENT_COLLEGE_REGION_NUMBER = string(),
#     CREDENTIAL_NAME = string(),
#     AGPA_PERCENT = string(),
#     ATTENDING_PSI_OUTSIDE_BC = string(),
#     LAST_SEEN_HOME_LANGUAGE = string(),
#     LAST_SEEN_RESIDENCY = string(),
#     PSI_SCHOOL_YEAR = string(),
#     PSI_REGISTRATION_TERM = string(),
#     PSI_STUDENT_POSTAL_CODE_CURRENT = string(),
#     PSI_INDIGENOUS_STATUS = string(),
#     PSI_NEW_STUDENT_FLAG = string(),
#     PSI_ENROLMENT_SEQUENCE = string(),
#     PSI_CODE = string(),
#     PSI_TYPE = string(),
#     PSI_FULL_NAME = string(),
#     PSI_BASIS_OF_ADMISSION = string(),
#     PSI_MIN_START_DATE = string(),
#     PSI_CREDENTIAL_PROGRAM_DESCRIPTION = string(),
#     PSI_PROGRAM_CODE = string(),
#     PSI_PROGRAM_EFFECTIVE_DATE = string(),
#     PSI_CIP_CODE = string(),
#     PSI_FACULTY = string(),
#     PSI_CONTINUING_EDUCATION_COURSE_ONLY = string(),
#     PSI_CREDENTIAL_CATEGORY = string(),
#     PSI_VISA_STATUS = string(),
#     PSI_STUDY_LEVEL = string(),
#     PSI_ENTRY_STATUS = string(),
#     OVERALL_INDIGENOUS_STATUS = string()
#   )

# ---- Write to decimal ----

# ---- Functions ----
# The refreshed STP exports (2024 cycle) write dates with TWO-DIGIT years,
# and enrolment dates span BOTH centuries (PSI_BIRTHDATE "69-08-09" = 1969
# next to "02-09-03" = 2002). Left as-is, as.Date()'s default parses them as
# years 0002-0025: SQL Server rejects those on insert (datetime range starts
# 1753 -> error 22007) and downstream date logic silently misparses. Expand
# to four-digit years at load -- single point, every consumer (01a/01c)
# receives clean YYYY-MM-DD. Cutoff follows the convert_date convention in
# 01a-enrolment-preprocessing.R (yy < 26 -> 20xx, else 19xx; bump with each
# data cycle -- birthdates/program dates 26-99 are 19xx, recent ones 00-25
# are 20xx). nchar == 8 guard keeps already-expanded values untouched, so
# the fix is idempotent if applied twice. any_of() tolerates a part file
# carrying a different column set.
convert_two_digit_year <- function(x, cutoff = 26) {
  out <- x
  need <- !is.na(x) & x != "" & x != "(Unspecified)" & nchar(x) == 8
  yy <- suppressWarnings(as.integer(substr(x[need], 1, 2)))
  out[need] <- paste0(ifelse(!is.na(yy) & yy < cutoff, "20", "19"), x[need])
  out
}
stp_date_cols <- c(
  "PSI_BIRTHDATE", "LAST_SEEN_BIRTHDATE",
  "PSI_MIN_START_DATE", "PSI_PROGRAM_EFFECTIVE_DATE"
)

write_to_decimal <- function(
  flnm,
  con,
  schema,
  append = FALSE,
  format = "csv"
) {
  tblnm <- tools::file_path_sans_ext(basename(flnm))
  log_info(glue::glue("Processing enrolment file: {basename(flnm)}"))
  cat(glue::glue("Processing {tblnm}: {Sys.time()} ..."))
  cat()

  # data <- open_dataset(
  #   sources = flnm,
  #   format = format,
  #   schema = schema,
  #   skip = 1
  # )

  data <- readr::read_csv(
    flnm,
    locale = locale(encoding = "latin1"),
    col_types = cols(.default = col_character())
  ) %>%
    mutate(across(any_of(stp_date_cols), convert_two_digit_year))
  # sample computed outside glue -- glue parses {} contents as code and
  # complex nested calls there have bitten us twice this cycle
  sample_bd <- paste(
    head(sort(data$PSI_BIRTHDATE[!is.na(data$PSI_BIRTHDATE)]), 2),
    collapse = ", "
  )
  fixed_cols <- paste(intersect(stp_date_cols, names(data)), collapse = ", ")
  log_info(glue::glue(
    "Expanded two-digit years in {fixed_cols}; sample birthdates: {sample_bd}"
  ))

  DBI::dbWriteTableArrow(
    con,
    name = SQL(glue::glue('"{my_schema}"."STP_Enrolment_orig"')),
    nanoarrow::as_nanoarrow_array_stream(data),
    append = append
  )

  cat(glue::glue("...completed {Sys.time()}"))
  cat("\n")
  log_info(glue::glue("Finished writing: {basename(flnm)}"))
}


log_info(glue::glue(
  "Writing {length(fls)} file(s) to STP_Enrolment table | append = TRUE"
))
invisible(lapply(
  fls,
  write_to_decimal,
  con = con,
  schema = schema,
  append = TRUE
))


# ---- Disconnect ----
dbDisconnect(con)
log_info("Disconnected from SQL Server")

log_info("==== load-stp-enrol.R COMPLETE ====")
