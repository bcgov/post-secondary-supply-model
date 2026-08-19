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

# This script loads student outcomes data for students who students who were formerly enrolled in
# a trades program (i.e. an apprenticeship, trades foundation program or trades-related vocational program)
#
# TRD = Trades Student Outcomes.  Survey of former students of trades programs
# (apprenticeships, trades foundation and trades-related vocational programs).
#
# This script reads the trades survey data from the LAN and writes the cleaned
# tables to SQL Server (schema = write_schema, database = decimal/PSSM2025):
#
#   q000_trd_data_01 -> trd_data (trd_data_r)
#       Survey responses, one row per respondent per survey cycle (a few
#       duplicates exist).  Consumed by 02b-1-pssm-cohorts.R: weights are
#       applied (T_Weights, SURVEY = TRD), age groups joined from TBL_AGE /
#       TBL_AGE_GROUPS, NEW_LABOUR_SUPPLY derived, and the standardized
#       records are stacked into T_Cohorts_Recoded (master cohort table
#       feeding credential analysis 01c-, labour supply 02b-2 and
#       projections 04-/06-).
#
#   q000_trd_graduates -> trd_graduates (trd_graduates_r)
#       Count of graduates by credential type, age and survey year.  Carried
#       through the cohort modules (02b-1/02b-2) and referenced by
#       04-graduate-projections.R for graduate-count work.
#
# Notes: Age group labels are assigned.  Note there are two different groupings used to group students by age in the model.

library(tidyverse)
library(config)
library(DBI)
library(odbc)
library(futile.logger)
source("R/utils.R")
## -------------------------- Logging Setup ------------------------------------------------------
## -----------------------------------------------------------------------------------------------
log_file <- "./R/execution_log.txt"
flog.appender(appender.file(log_file), name = "file_logger")
flog.threshold(INFO, name = "file_logger")

log_info <- function(msg) {
  flog.info(msg, name = "file_logger")
  print(paste(Sys.time(), "|", msg))
}

log_info("==== load-cohort-trd.R START ====")

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

# Trades survey responses, one row per respondent per survey cycle.  KEY
# identifies the respondent within a cycle (used to build STQU_ID in
# 02b-1-pssm-cohorts.R); SUBM_CD gives the survey cycle (C_Outc..).
q000_trd_data_01 <- read_oracle_csv_auto(glue::glue(
  "{lan}/data/student-outcomes/csv/Q000_TRD_DATA_01.csv"
))
log_info(glue::glue("Read Q000_TRD_DATA_01.csv: {nrow(q000_trd_data_01)} rows"))

# Trades graduate counts by credential type, age and survey year.
q000_trd_graduates <- read_oracle_csv_auto(glue::glue(
  "{lan}/data/student-outcomes/csv/Q000_TRD_Graduates.csv"
))
log_info(glue::glue(
  "Read Q000_TRD_Graduates.csv: {nrow(q000_trd_graduates)} rows"
))

# Convert some variables that should be numeric
# GRADSTAT = graduation status, KEY = respondent key, TTRAIN = total months
# of trades training; all needed as numbers for joins and credential coding.
q000_trd_data_01 <- q000_trd_data_01 %>%
  mutate(
    GRADSTAT = as.numeric(GRADSTAT),
    KEY = as.numeric(KEY),
    TTRAIN = as.numeric(TTRAIN)
  )

# Gradstat group : couldn't find in outcomes data so defining here.
# Build the standard credential key used across all cohorts: grad-status
# group + 4-digit CIP + months of training + PSSM credential.  In
# 02b-1-pssm-cohorts.R this becomes LCIP4_CRED / LCIP2_CRED, the codes by
# which credential-level outputs are grouped.
q000_trd_data_01 <- q000_trd_data_01 %>%
  mutate(
    LCIP4_CRED = paste0(
      GRADSTAT_GROUP,
      ' - ',
      LCIP_LCP4_CD,
      ' - ',
      TTRAIN,
      ' - ',
      PSSM_CREDENTIAL
    )
  )

# Recode the survey's current-region fields into the standard PSSM region
# codes (same scheme as APPSO/DACSO/BGS so regions are comparable across
# cohorts; used for region-level outputs in 02b-1/02b-2).
trd_data <-
  q000_trd_data_01 %>%
  mutate(
    CURRENT_REGION_PSSM_CODE = case_when(
      CURRENT_REGION1 %in% 1:8 ~ CURRENT_REGION1,
      CURRENT_REGION4 == 5 ~ 9,
      CURRENT_REGION4 == 6 ~ 10,
      CURRENT_REGION4 == 7 ~ 11,
      CURRENT_REGION4 == 8 ~ -1,
      TRUE ~ NA
    )
  )
log_info(glue::glue("trd_data prepared: {nrow(trd_data)} rows"))

# prepare graduate dataset
# Add the standard PSSM age-band labels to the trades graduate counts for
# consistent reporting.
trd_graduates <- q000_trd_graduates %>%
  mutate(
    AGE_GROUP_LABEL = case_when(
      TRD_AGE_AT_SURVEY %in% 15:16 ~ "15 to 16",
      TRD_AGE_AT_SURVEY %in% 17:19 ~ "17 to 19",
      TRD_AGE_AT_SURVEY %in% 20:24 ~ "20 to 24",
      TRD_AGE_AT_SURVEY %in% 25:29 ~ "25 to 29",
      TRD_AGE_AT_SURVEY %in% 30:34 ~ "30 to 34",
      TRD_AGE_AT_SURVEY %in% 35:44 ~ "35 to 44",
      TRD_AGE_AT_SURVEY %in% 45:54 ~ "45 to 54",
      TRD_AGE_AT_SURVEY %in% 55:64 ~ "55 to 64",
      TRD_AGE_AT_SURVEY %in% 65:89 ~ "65 to 89",
      TRUE ~ NA
    )
  )
log_info(glue::glue("trd_graduates prepared: {nrow(trd_graduates)} rows"))

## ------------------------------------ Clean Up --------------------------------------------------
# Current workflow:
#  - Write key tables back to sql server.  These are tables needed for downstream work, or tables
# that might be needed for later reference outside of this analysis.
#  - Close DB connections
#  - Remove all objects at the end of each script.
## ------------------------------------------------------------------------------------------------

tables_to_keep <- c(
  "trd_data",
  "trd_graduates"
)

# Write each kept table to SQL Server as <name>_r.  write_table_to_db lives in
# R/utils.R (sourced by run-data-loading.R / the calling runner).
walk(tables_to_keep, write_table_to_db, schema = write_schema, con = con)
log_info(glue::glue(
  "Written to SQL Server ({write_schema}): {paste0(tables_to_keep, '_r', collapse = ', ')}"
))

dbDisconnect(con)
log_info("Disconnected from SQL Server database")
log_info("==== load-cohort-trd.R END ====")
