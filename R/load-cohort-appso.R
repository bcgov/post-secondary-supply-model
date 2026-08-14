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

# ---------------------------------------------------------------------------
# LOAD COHORT - APPSO
#
# APPSO = Apprenticeship Student Outcomes.  Survey of former apprenticeship
# students, conducted roughly two years after they completed (or exited)
# their program.  These data describe what apprentices are doing after
# leaving their training (employed / in the labour market / further study /
# not in the labour force), by training institution, program area (CIP),
# region, age and credential.
#
# This script loads the APPSO survey data from the LAN and writes the cleaned
# tables to SQL Server (schema = write_schema, database = database/PSSM2025):
#
#   t_appso_data_final -> t_appso_data_final_r
#       Survey responses, one row per respondent per survey cycle.
#       Consumed by 02b-1-pssm-cohorts.R: joined to survey-cycle and age
#       look-ups, standardized and stacked into T_Cohorts_Recoded, the master
#       cohort table that feeds credential analysis (01c-), labour supply
#       (02b-2) and projections (04-, 06-).
#
#   appso_graduates   -> appso_graduates_r
#       Graduate counts by training program/credential/region, carried
#       through the cohort modules and used in 04-graduate-projections.R
#       for the 2-year average and historical forecast of apprentice
#       graduates.
#
# Survey cycle is identified by SUBM_CD (C_Outc21 = 2021 cohort up to
# C_Outc25 = 2025 cohort).  Weights derived from SUBM_CD scale the survey
# respondents up to population counts for reporting.
# Variable dictionary: see data_dictionary_so.csv on the LAN.
# ---------------------------------------------------------------------------

library(tidyverse)
library(config)
library(DBI)
library(odbc)
library(futile.logger)
# qi_run <- F
# regular_run <- T
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

log_info("==== load-cohort-appso.R START ====")

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

## --------------------------------------Required Tables------------------------------------------
## -----------------------------------------------------------------------------------------------

# source(glue::glue("./sql/02b-pssm-cohorts/appso-data.sql"))
# Read the APPSO survey responses from the LAN (source: query
# APPSO_Data_01_Final in the LAN sql/ folder).  One row per respondent per
# survey cycle; SUBM_CD gives the cycle (C_Outc21..C_Outc25).
t_appso_data_final <- read_csv(glue::glue(
  "{lan}/data/student-outcomes/csv/APPSO_Data_01_Final.csv"
))
log_info(glue::glue("Read APPSO_Data_01_Final.csv: {nrow(t_appso_data_final)} rows"))
# Read the apprenticeship graduate counts (used in 04-graduate-projections
# for the 2-year average / historical forecast of APPSO graduates).
appso_graduates <- read_csv(glue::glue(
  "{lan}/data/student-outcomes/csv/APPSO_Graduates.csv"
))
log_info(glue::glue("Read APPSO_Graduates.csv: {nrow(appso_graduates)} rows"))

# Convert some variables that should be numeric
# TTRAIN = total months of apprenticeship training; needed as numeric for
# calculations in the labour supply modules.
t_appso_data_final <- t_appso_data_final %>%
  mutate(TTRAIN = as.numeric(TTRAIN))

# Make sure this is updated to only the last 6 years of data
# Recode the survey's current-region fields (CURRENT_REGION1/CURRENT_REGION4)
# into the standard PSSM region codes used across all modules
# (see T_Current_Region_PSSM_Codes / roll-ups in 02b-2).
t_appso_data_final <-
  t_appso_data_final %>%
  mutate(
    CURRENT_REGION_PSSM_CODE = case_when(
      CURRENT_REGION1 %in% 1:8 ~ CURRENT_REGION1,
      CURRENT_REGION4 == 5 ~ 9,
      CURRENT_REGION4 == 6 ~ 10,
      CURRENT_REGION4 == 7 ~ 11,
      CURRENT_REGION4 == 8 ~ -1,
      TRUE ~ NA
    )
  ) %>%
  # AGE_GROUP_LABEL: reporting labels for the standard PSSM age bands.
  mutate(
    AGE_GROUP_LABEL = case_when(
      APP_AGE_AT_SURVEY %in% 15:16 ~ "15 to 16",
      APP_AGE_AT_SURVEY %in% 17:19 ~ "17 to 19",
      APP_AGE_AT_SURVEY %in% 20:24 ~ "20 to 24",
      APP_AGE_AT_SURVEY %in% 25:29 ~ "25 to 29",
      APP_AGE_AT_SURVEY %in% 30:34 ~ "30 to 34",
      APP_AGE_AT_SURVEY %in% 35:44 ~ "35 to 44",
      APP_AGE_AT_SURVEY %in% 45:54 ~ "45 to 54",
      APP_AGE_AT_SURVEY %in% 55:64 ~ "55 to 64",
      APP_AGE_AT_SURVEY %in% 65:89 ~ "65 to 89",
      TRUE ~ NA
    )
  ) %>%
  # AGE_GROUP: numeric code for the same bands (2-8).  In 02b-1-pssm-cohorts.R
  # this key is used to join TBL_AGE_GROUPS so every cohort gets a standard
  # age grouping across survey types.
  mutate(
    AGE_GROUP = case_when(
      APP_AGE_AT_SURVEY %in% 17:19 ~ 2,
      APP_AGE_AT_SURVEY %in% 20:24 ~ 3,
      APP_AGE_AT_SURVEY %in% 25:29 ~ 4,
      APP_AGE_AT_SURVEY %in% 30:34 ~ 5,
      APP_AGE_AT_SURVEY %in% 35:44 ~ 6,
      APP_AGE_AT_SURVEY %in% 45:54 ~ 7,
      APP_AGE_AT_SURVEY %in% 55:64 ~ 8,
      TRUE ~ NA
    )
  ) %>%
  # NEW_LABOUR_SUPPLY: 1 if the respondent is employed or in the labour
  # market, 0 otherwise.  Used in 02b-2-pssm-cohorts-new-labour-supply.R to
  # estimate the share of new graduates entering the labour market.
  mutate(
    NEW_LABOUR_SUPPLY = case_when(
      APP_LABR_EMPLOYED == 1 ~ 1,
      APP_LABR_IN_LABOUR_MARKET == 1 & APP_LABR_EMPLOYED == 0 ~ 1,
      APP_LABR_EMPLOYED == 0 ~ 0,
      RESPONDENT == '1' ~ 0,
      TRUE ~ 0
    )
  )

# When running, make sure to update weights for the regular run.
# Replace the weights in the appropriate area in the code (~lines 71-77):
# Weight values for C_Outc21..C_Outc25 still require analyst sign-off.
# WEIGHT scales each survey cycle's respondents up to population counts for
# reporting (larger cycle number = more recent cycle).  The same weighting
# scheme applies to the other survey types via T_Weights.
t_appso_data_final <-
  t_appso_data_final %>%
  mutate(
    WEIGHT = case_when(
      SUBM_CD == 'C_Outc21' ~ 1,
      SUBM_CD == 'C_Outc22' ~ 2,
      SUBM_CD == 'C_Outc23' ~ 3,
      SUBM_CD == 'C_Outc24' ~ 4,
      SUBM_CD == 'C_Outc25' ~ 5,
      TRUE ~ 0
    )
  )

# update the weights for the QI run.
# Quality Improvement run: re-weight excluding the most recent survey cycle
# (C_Outc25 -> 0) so the QI run only uses the cycles shared with the
# previous model run.
if (exists("qi_run") && qi_run == TRUE) {
  # check that these years are correct
  # TODO: this moved out of query for derived weights  but means an extra step for QI - move back to query design?
  t_appso_data_final <-
    t_appso_data_final %>%
    mutate(
      WEIGHT = case_when(
        SUBM_CD == 'C_Outc21' ~ 2,
        SUBM_CD == 'C_Outc22' ~ 3,
        SUBM_CD == 'C_Outc23' ~ 4,
        SUBM_CD == 'C_Outc24' ~ 5,
        SUBM_CD == 'C_Outc25' ~ 0,
        TRUE ~ 0
      )
    )
  log_info("QI run: QI weights applied to APPSO data")
}

log_info(glue::glue(
  "t_appso_data_final prepared: {nrow(t_appso_data_final)} rows, weights applied"
))

# prepare graduate dataset
# Add the standard PSSM age-band labels to the graduate counts so they can be
# reported consistently; used in 04-graduate-projections.R.
appso_graduates %>%
  mutate(
    AGE_GROUP = case_when(
      APP_AGE_AT_SURVEY %in% 15:16 ~ "15 to 16",
      APP_AGE_AT_SURVEY %in% 17:19 ~ "17 to 19",
      APP_AGE_AT_SURVEY %in% 20:24 ~ "20 to 24",
      APP_AGE_AT_SURVEY %in% 25:29 ~ "25 to 29",
      APP_AGE_AT_SURVEY %in% 30:34 ~ "30 to 34",
      APP_AGE_AT_SURVEY %in% 35:44 ~ "35 to 44",
      APP_AGE_AT_SURVEY %in% 45:54 ~ "45 to 54",
      APP_AGE_AT_SURVEY %in% 55:64 ~ "55 to 64",
      APP_AGE_AT_SURVEY %in% 65:89 ~ "65 to 89",
      TRUE ~ NA
    )
  ) -> appso_graduates
log_info(glue::glue("appso_graduates prepared: {nrow(appso_graduates)} rows"))

## ------------------------------------ Clean Up --------------------------------------------------
# Current workflow:
#  - Write key tables back to sql server.  These are tables needed for downstream work, or tables
# that might be needed for later reference outside of this analysis.
#  - Close DB connections
#  - Remove all other objects at the end of each script.
## ------------------------------------------------------------------------------------------------

tables_to_keep <- c(
  "appso_graduates",
  "t_appso_data_final"
)

# Write each kept table to SQL Server as <name>_r.  Downstream scripts
# (02b-1-pssm-cohorts.R etc.) pick them up by object name from the loading
# sequence in run-data-loading.R / run-data-preprocessing.R.
write_table_to_db <- function(table_name, schema, con) {
  db_name <- paste0(table_name, "_r")
  # Some source files contain invalid UTF-8 byte sequences (e.g. PROGRAM
  # names), which make odbcDataType()/nchar() fail when writing.  Strip
  # invalid bytes from all character columns before writing.
  data <- base::get(table_name, envir = .GlobalEnv) %>%
    mutate(across(
      where(is.character),
      ~ iconv(.x, from = "UTF-8", to = "UTF-8", sub = "")
    ))
  dbWriteTable(
    con,
    SQL(glue::glue('"{schema}"."{db_name}"')),
    data,
    overwrite = TRUE
  )
}

walk(tables_to_keep, write_table_to_db, schema = write_schema, con = con)
log_info(glue::glue(
  "Written to SQL Server ({write_schema}): {paste0(tables_to_keep, '_r', collapse = ', ')}"
))

dbDisconnect(con)
log_info("Disconnected from SQL Server database")
log_info("==== load-cohort-appso.R END ====")
