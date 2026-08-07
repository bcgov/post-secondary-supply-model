# Copyright 2024 Province of British Columbia
#
# Licensed under the Apache License, Version 2.0 (the &quot;License&quot;);
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an &quot;AS IS&quot; BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations under the License.

##########################################################################################################################################
# This script loads student outcomes data for students who
# TRD: were formerly enrolled in a trades program (i.e. an apprenticeship, trades foundation program or trades-related vocational program)
# APP: have completed the final year of their apprenticeship technical training within the first year of graduation.
# BGS: recently graduated with a Baccalaureate degree (Baccalaureate students are surveyed two years after graduation)
# DAC: recently graduated after completing programs at public colleges, institutes, and teaching-intensive universities (~18 months prior)

# The following data sets are read into SQL server from a csv housed in the project LAN drive.
# Original data is sourced from the student outcomes survey database, and provisioned to the project by the student outcomes survey team.
#   Q000_TRD_DATA_01: unique survey responses for each person/survey year (a few duplicates)
#   Q000_TRD_Graduates: a count of graduates by credential type, age and survey year
#   T_APPSO_DATA_Final: unique survey responses for each person/survey year  (a few duplicates)
#   APPSO_Graduates: a count of graduates by credential type, age and survey year
#   BGS_Q001_BGS_Data_2021_2025: unique survey responses for each person/survey year (for years since last model run)
#   t_dacso_data_part_1_stepa: unique survey responses for each person/survey year (for years since last model run)
#   infoware_c_outc_clean_short_resp
#   qry_make_tmp_table_Age_step1
#   INFOWARE_L_CIP_4DIGITS_CIP2021
#   INFOWARE_L_CIP_6DIGITS_CIP2021
#
# This script is the bulk raw loader: every CSV in the student-outcomes folder
# is read into memory (one object per file, named after the file) and written
# to SQL Server (schema = use_schema/shareschema, database = database/PSSM2025)
# as <name>_raw.  These _raw tables are the unmodified archive copies of the
# survey files for reference/audit.
#
# The cleaned versions of the survey data are built by the load-cohort-*
# scripts (load-cohort-trd/appso/bgs/dacso.R), which re-read the same CSVs
# from the LAN, add derived fields (region recodes, weights, age groups,
# labour supply flags) and write the *_r tables that feed
# 02b-1-pssm-cohorts.R -> T_Cohorts_Recoded -> 01c-/02b-2/04-/06-.
#
# File map:
#   Q000_TRD_DATA_01 / Q000_TRD_Graduates  -> trades survey (load-cohort-trd.R)
#   T_APPSO_DATA_Final / APPSO_Graduates   -> apprenticeship survey (load-cohort-appso.R)
#   BGS_Q001_BGS_Data_2021_2025            -> baccalaureate survey (load-cohort-bgs.R)
#   DACSO_Q003_DACSO_DATA_Part_1_stepA     -> college/diploma survey (load-cohort-dacso.R)
#   infoware_c_outc_clean_short_resp       -> DACSO follow-up flags (load-cohort-dacso.R)
#   qry_make_tmp_table_Age_step1           -> age-at-survey per submission (SQL query output)
#   INFOWARE_L_CIP_4DIGITS_CIP2021 / INFOWARE_L_CIP_6DIGITS_CIP2021
#                                          -> CIP2021 program classification look-ups,
#                                             used for program matching in the 02a-* scripts
#############################################################################################################################################

#---------------------------------------------------------------------------------------------------------------------------
# notes:
#   (small n) minor differences in code description/name: INFOWARE_L_CIP_4DIGITS_CIP2021, INFOWARE_L_CIP_6DIGITS_CIP2021
#   (small n) differences in prgm_credential: T_DACSO_DATA_Part_1_stepA
#   I've read that using assign() as below is fragile ... it works though, so..?
#   TODO: investigate both tmp_table_Age and the BGS data a little more.
#---------------------------------------------------------------------------------------------------------------------------

library(tidyverse)
library(config)
library(RODBC)
library(DBI)
library(odbc)
library(futile.logger)

## -------------------------- Logging Setup ------------------------------------------------------
## -----------------------------------------------------------------------------------------------
log_file <- "./R/execution_log.txt"
flog.appender(appender.file(log_file), name = "file_logger")
flog.threshold(INFO, name = "file_logger")

log_info <- function(msg) {
  flog.info(msg, name = "file_logger")
  print(paste(Sys.time(), "|", msg))
}

log_info("==== load-outcomes-data.R START ====")

# set up db connection and lan paths
db_config <- config::get("decimal")
decimal_con <- dbConnect(
  odbc::odbc(),
  Driver = db_config$driver,
  Server = db_config$server,
  Database = db_config$database,
  Trusted_Connection = "True"
)
log_info("Connected to SQL Server database (decimal)")
# make sure vpn is on, and the lan is available. Or switch to safepath approach.
use_schema <- config::get("shareschema")
lan <- config::get("lan")
so_lan_path <- glue::glue("{lan}/data/student-outcomes/csv/")

# read csv's into objects in memory.
# List every CSV the student outcomes team dropped in the folder; each file is
# loaded below as an object named after the file (extension removed).
so_data_all_full_pathnames <- list.files(
  so_lan_path,
  pattern = ".csv",
  full.names = TRUE
)
age_file_full_pathname <- glue::glue(
  "{so_lan_path}qry_make_tmp_table_Age_step1.csv"
)
log_info(glue::glue(
  "Found {length(so_data_all_full_pathnames)} CSV files in student-outcomes folder"
))

# read all files into current environment, processing the tmp_table_Age_step1 data set separately.
# (The age file has a fixed column layout and must be read with explicit
# col_types - COCI_STQU_ID/COCI_SUBM_CD/BTHDT/ENDDT/COCI_AGE_AT_SURVEY.)
so_data_full_pathnames <- so_data_all_full_pathnames[
  tolower(basename(so_data_all_full_pathnames)) !=
    "qry_make_tmp_table_age_step1.csv"
]
so_data_full_pathnames %>%
  set_names(tools::file_path_sans_ext(basename(so_data_full_pathnames))) %>%
  map(read_csv, show_col_types = FALSE) %>%
  imap(~ assign(..2, ..1, envir = .GlobalEnv)) %>%
  invisible()
log_info(glue::glue(
  "Read {length(so_data_full_pathnames)} SO csv files into environment"
))

# tmp_table_Age_step1 data set
qry_make_tmp_table_Age_step1 <- read_csv(
  age_file_full_pathname,
  show_col_types = FALSE,
  col_types = "dcdcd"
)
log_info(glue::glue(
  "Read qry_make_tmp_table_Age_step1.csv: {nrow(qry_make_tmp_table_Age_step1)} rows"
))

# sanity check: any datasets missing from current environment?
so_data_expected <- c(
  tools::file_path_sans_ext(basename(so_data_full_pathnames)),
  'qry_make_tmp_table_Age_step1'
)
missing <- !so_data_expected %in% ls()

if (any(missing)) {
  warning("One or more SO datasets were not loaded into current environment.")
  log_info(glue::glue(
    "WARNING: missing datasets: {paste0(so_data_expected[missing], collapse = ', ')}"
  ))
} else {
  log_info("All expected SO datasets loaded into environment")
}

# recast datatypes
# PEN (person education number) must be character - it has leading zeros that
# a numeric read would strip; survey value columns are recast for the
# calculations in the cohort modules.
# Each block is guarded with exists() so a missing CSV (noted in the sanity
# check above) only logs a warning instead of stopping the script.
if (exists("APPSO_Data_01_Final")) {
  APPSO_Data_01_Final$PEN <- as.character(APPSO_Data_01_Final$PEN)
  APPSO_Data_01_Final$APP_TIME_TO_FIND_EMPLOY_MJOB <- as.numeric(
    APPSO_Data_01_Final$APP_TIME_TO_FIND_EMPLOY_MJOB
  )
} else {
  log_info("Skipping APPSO_Data_01_Final recasts: object not loaded")
}

if (exists("BGS_Q001_BGS_Data_2021_2025")) {
  BGS_Q001_BGS_Data_2021_2025$PEN <- as.character(
    BGS_Q001_BGS_Data_2021_2025$PEN
  )
} else {
  log_info("Skipping BGS_Q001_BGS_Data_2021_2025 recasts: object not loaded")
}

if (exists("DACSO_Q003_DACSO_DATA_Part_1_stepA")) {
  DACSO_Q003_DACSO_DATA_Part_1_stepA <- DACSO_Q003_DACSO_DATA_Part_1_stepA %>%
    mutate(across(.cols = c(TPID_LGND_CD, COCI_PEN), .fns = as.character))
} else {
  log_info(
    "Skipping DACSO_Q003_DACSO_DATA_Part_1_stepA recasts: object not loaded"
  )
}

if (exists("Q000_TRD_DATA_01")) {
  Q000_TRD_DATA_01 <- Q000_TRD_DATA_01 %>%
    mutate(across(.cols = c(GRADSTAT_GROUP, PEN), .fns = as.character))
} else {
  log_info("Skipping Q000_TRD_DATA_01 recasts: object not loaded")
}

if (exists("INFOWARE_C_OutC_Clean_Short_Resp")) {
  INFOWARE_C_OutC_Clean_Short_Resp <- INFOWARE_C_OutC_Clean_Short_Resp %>%
    mutate(across(
      .cols = c(TTRAIN, Q08, FINAL_DISPOSITION, RESPONDENT, CREDENTIAL_DERIVED),
      .fns = as.character
    ))
} else {
  log_info(
    "Skipping INFOWARE_C_OutC_Clean_Short_Resp recasts: object not loaded"
  )
}
log_info("Data types recast (PEN/keys as character, survey values as numeric)")

# remove non-standard characters so ssms won't throw an err
# The CIP look-ups contain invalid UTF-8 byte sequences (e.g. accents encoded
# in a legacy codepage); strip invalid bytes so dbWriteTable/odbcDataType do
# not fail on them.
if (exists("INFOWARE_L_CIP_4DIGITS_CIP2021")) {
  INFOWARE_L_CIP_4DIGITS_CIP2021$LCP4_DIGITS_NAME <- iconv(
    INFOWARE_L_CIP_4DIGITS_CIP2021$LCP4_DIGITS_NAME,
    "UTF-8",
    "UTF-8",
    sub = ''
  )
} else {
  log_info("Skipping INFOWARE_L_CIP_4DIGITS_CIP2021 iconv: object not loaded")
}

if (exists("INFOWARE_L_CIP_6DIGITS_CIP2021")) {
  INFOWARE_L_CIP_6DIGITS_CIP2021$LCP6_DIGITS_NAME <- iconv(
    INFOWARE_L_CIP_6DIGITS_CIP2021$LCP6_DIGITS_NAME,
    "UTF-8",
    "UTF-8",
    sub = ''
  )
  INFOWARE_L_CIP_6DIGITS_CIP2021$LCIP_LCIPPC_NAME <- iconv(
    INFOWARE_L_CIP_6DIGITS_CIP2021$LCIP_LCIPPC_NAME,
    "UTF-8",
    "UTF-8",
    sub = ''
  )
} else {
  log_info("Skipping INFOWARE_L_CIP_6DIGITS_CIP2021 iconv: object not loaded")
}
log_info("Invalid UTF-8 bytes stripped from CIP look-up columns")

# load to ssms
# Write each loaded object to SQL Server as <name>_raw - raw archive copy of
# the survey file (unmodified except for the recasts/cleaning above).
# Character columns are sanitized first: some source files contain invalid
# UTF-8 byte sequences which make odbcDataType()/nchar() fail when writing.
so_data_expected[!missing] %>%
  mget(envir = .GlobalEnv) %>%
  imap(
    ~ dbWriteTable(
      decimal_con,
      overwrite = TRUE,
      name = SQL(glue::glue('"{use_schema}"."{..2}_raw"')),
      value = ..1 %>%
        mutate(across(
          where(is.character),
          ~ iconv(.x, from = "UTF-8", to = "UTF-8", sub = "")
        ))
    )
  )
log_info(glue::glue(
  "Written {length(so_data_expected[!missing])} raw tables to SQL Server ({use_schema}): ",
  "{paste0(so_data_expected[!missing], '_raw', collapse = ', ')}"
))

dbDisconnect(decimal_con)
log_info("Disconnected from SQL Server database")
log_info("==== load-outcomes-data.R END ====")
