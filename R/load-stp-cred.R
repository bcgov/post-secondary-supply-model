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
# Load STP credential data from staging area in LAN project folder, to decimal.
# Requested data to be in tab-separated text file
# STP credential data loads with a few data type conversion problems:
#   - querying SQL data returns PSI_CREDENTIAL_PROGRAM_DESC as a quoted string
#   - dates are uploaded format YY-MM-DD instead of YYYY-MM-DD
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

log_info("==== load-stp-cred.R START ====")

# ---- Configure LAN and file paths ----
lan <- config::get("lan_2027")
my_schema <- config::get("myschema")
raw_data <- glue::glue(
  "{lan}/Data/stp/BCSTATS_STP_ISA_PSSM_JUL13_2026/STP_CREDENTIAL_2024.dsv"
)
log_info(glue::glue("Loading STP credential data from: {raw_data}"))


## ----- Connection to decimal ----
# db_config <- config::get("decimal2026")
# test in 2023 database
db_config <- config::get("decimal")
con <- dbConnect(
  odbc(),
  Driver = db_config$driver,
  Server = db_config$server,
  Database = db_config$database,
  Trusted_Connection = "True"
)

log_info(glue::glue("Connected to SQL Server | Database: {db_config$database}"))
# schema <-
#   schema(
#     CREDENTIAL_AWARD_DATE = string(),
#     PSI_CODE = string(),
#     PSI_FULL_NAME = string(),
#     PSI_PEN = string(),
#     PSI_STUDENT_NUMBER = string(),
#     PSI_SCHOOL_YEAR = string(),
#     PSI_PROGRAM_CODE = string(),
#     PSI_PROGRAM_EFFECTIVE_DATE = string(),
#     PSI_CREDENTIAL_CATEGORY = string(),
#     PSI_CREDENTIAL_LEVEL = string(),
#     PSI_CREDENTIAL_CIP = string(),
#     PSI_CREDENTIAL_PROGRAM_DESCRIPTION = string(),
#     SNAPSHOT_DATE = string(),
#     ENCRYPTED_TRUE_PEN = string(),
#     STP_ALT_ID = string()
#   )

# ---- Write to decimal ----
tblnm <- tools::file_path_sans_ext(basename(raw_data))
log_info(glue::glue("Writing table: {tblnm} | append = TRUE"))


# data <- open_dataset(
#   sources = raw_data,
#   format = "csv", #The DSV file is comma-delimited
#   schema = schema,
#   skip = 1,
# )
# Arrow's CSV reader expects UTF-8 text.
# If your CSV is encoded in Latin-1 or Windows-1252, Arrow doesn't transcode it—it will often infer the column as binary instead of string, exactly as you're seeing.

data <- readr::read_csv(
  raw_data,
  locale = locale(encoding = "latin1"),
  col_types = cols(.default = col_character())
)
# data |> glimpse()

dbWriteTableArrow(
  con,
  name = SQL(glue::glue('"{my_schema}"."{tblnm}"')),
  nanoarrow::as_nanoarrow_array_stream(data),
  append = TRUE
)

log_info(glue::glue(
  "STP_Credential written to SQL Server (table: STP_Credential)"
))
cat(glue::glue("...completed {Sys.time()}"))
cat("\n")


# ---- Disconnect ----
dbDisconnect(con)
log_info("Disconnected from SQL Server")

log_info("==== load-stp-cred.R COMPLETE ====")
