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

# This script loads student outcomes data for students who students who have completed the
# final year of their apprenticeship technical training within the first year of graduation.
#
# The following data is read from SQL server database:
#   t_appso_data_final: unique survey responses for each person/survey year  (a few duplicates)
#   APPSO_Graduates: a count of graduates by credential type, age and survey year

library(tidyverse)
library(config)
library(glue)


# ---- Configure LAN and file paths ----
lan <- config::get("lan")

# ---- Read outcomes data ----
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

if (regular_run == TRUE | ptib_run == T) {
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
}

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
