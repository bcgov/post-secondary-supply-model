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

# source(glue::glue("./sql/02b-pssm-cohorts/appso-data.sql"))
t_appso_data_final <- read_csv(glue::glue(
  "{lan}/data/student-outcomes/csv/so-provision/APPSO_DATA_01_Final.csv"
))
appso_graduates <- read_csv(glue::glue(
  "{lan}/data/student-outcomes/csv/so-provision/APPSO_Graduates.csv"
))

# Convert some variables that should be numeric
t_appso_data_final <- t_appso_data_final %>%
  mutate(TTRAIN = as.numeric(TTRAIN))

# Make sure this is updated to only the last 6 years of data
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
t_appso_data_final <-
  t_appso_data_final %>%
  mutate(
    WEIGHT = case_when(
      SUBM_CD == 'C_Outc19' ~ 1,
      SUBM_CD == 'C_Outc20' ~ 2,
      SUBM_CD == 'C_Outc21' ~ 3,
      SUBM_CD == 'C_Outc22' ~ 4,
      SUBM_CD == 'C_Outc23' ~ 5,
      TRUE ~ 0
    )
  )

# update the weights for the QI run.
if (qi_run == TRUE) {
  # check that these years are correct
  # TODO: this moved out of query for derived weights  but means an extra step for QI - move back to query design?
  t_appso_data_final <-
    t_appso_data_final %>%
    mutate(
      WEIGHT = case_when(
        SUBM_CD == 'C_Outc19' ~ 2,
        SUBM_CD == 'C_Outc20' ~ 3,
        SUBM_CD == 'C_Outc21' ~ 4,
        SUBM_CD == 'C_Outc22' ~ 5,
        SUBM_CD == 'C_Outc23' ~ 0,
        TRUE ~ 0
      )
    )
}

# prepare graduate dataset
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
