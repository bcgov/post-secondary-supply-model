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
# The following data sets are read into SQL server from the student outcomes survey database:
#   q000_trd_data_01: unique survey responses for each person/survey year (a few duplicates)
#   q000_trd_graduates: a count of graduates by credential type, age and survey year
#
# Notes: Age group labels are assigned.  Note there are two different groupings used to group students by age in the model.

library(tidyverse)
library(config)
library(DBI)
library(odbc)

qi_run <- F
regular_run <- T
ptib_run <- T

## -------------------------- Configure LAN Paths and DB Connection ------------------------------
## -----------------------------------------------------------------------------------------------

my_schema <- config::get("myschema")
db_config <- config::get("decimal")

con <- dbConnect(
  odbc::odbc(),
  Driver = db_config$driver,
  Server = db_config$server,
  Database = db_config$database,
  Trusted_Connection = "True"
)

lan <- config::get("lan")

## --------------------------------------Required Tables------------------------------------------
## -----------------------------------------------------------------------------------------------

q000_trd_data_01 <- read_csv(glue::glue(
  "{lan}/data/student-outcomes/csv/so-provision/Q000_TRD_DATA_01.csv"
))

q000_trd_graduates <- read_csv(glue::glue(
  "{lan}/data/student-outcomes/csv/so-provision/Q000_TRD_Graduates.csv"
))

# Convert some variables that should be numeric
q000_trd_data_01 <- q000_trd_data_01 %>%
  mutate(
    GRADSTAT = as.numeric(GRADSTAT),
    KEY = as.numeric(KEY),
    TTRAIN = as.numeric(TTRAIN)
  )

# Gradstat group : couldn't find in outcomes data so defining here.
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

# prepare graduate dataset
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

write_table_to_db <- function(table_name, schema, con) {
  db_name <- paste0(table_name, "_r")
  dbWriteTable(
    con,
    SQL(glue::glue('"{schema}"."{db_name}"')),
    base::get(table_name, envir = .GlobalEnv),
    overwrite = TRUE
  )
}

walk(tables_to_keep, write_table_to_db, schema = my_schema, con = con)

dbDisconnect(con)
