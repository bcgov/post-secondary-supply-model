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
library(RODBC)
library(config)
library(glue)
library(DBI)

# ---- Configure LAN and file paths ----
lan <- config::get("lan")
my_schema <- config::get("myschema")

# ---- Connection to decimal ----
db_config <- config::get("decimal")
decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")

# ---- Read LAN data ----
stp_dacso_prgm_credential_lookup <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/STP_DACSO_PRGM_CREDENTIAL_LOOKUP.csv"), col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")

combine_creds <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/combine_creds.csv"), col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")

t_pssm_projection_cred_grp <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/T_PSSM_Projection_Cred_Grp.csv"), col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps") %>% 
  add_case(PSSM_PROJECTION_CREDENTIAL = 'UNIVERSITY TRANSFER', 
           PSSM_CREDENTIAL = 'ADGR OR UT', 
           PSSM_CREDENTIAL_NAME = 'Associate Degree/University Transfer', 
           COSC_GRAD_STATUS_LGDS_CD = 1)

tbl_Age <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/tbl_Age.csv"), col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps") %>%
  mutate(AGE_GROUP= AGE_GROUP-1) %>%
  mutate(AGE_GROUP = if_else(AGE %in% 35:64, 5, AGE_GROUP))

age_group_lookup <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/AgeGroupLookup.csv"), col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps") %>% 
  filter(AGE_INDEX %in% 2:5) %>% 
  mutate(AGE_INDEX = AGE_INDEX -1) %>%
  add_case(AGE_INDEX = 5, AGE_GROUP = "35 to 64", LOWER_BOUND = 35, UPPER_BOUND = 64)

# ---- Rollover Tables ---- 
tmp_tbl_Age <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/testing/03/tmp_tbl_Age.csv"), col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")

# ---- Read SO data ----
tmp_tbl_Age_AppendNewYears <- dbReadTable(decimal_con, SQL(glue::glue('"{my_schema}"."qry_make_tmp_table_Age_step1_raw"'))) # adjust query for correct year

# ---- Write to decimal ----
dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."tmp_tbl_Age_AppendNewYears"')), value = tmp_tbl_Age_AppendNewYears, overwrite=TRUE)
dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."tmp_tbl_Age"')), value = tmp_tbl_Age, overwrite=TRUE)
dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."tbl_Age"')), value = tbl_Age, overwrite = TRUE)
dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."combine_creds"')), value = combine_creds , overwrite=TRUE)
dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."stp_dacso_prgm_credential_lookup"')), value = stp_dacso_prgm_credential_lookup, overwrite=TRUE)
dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."t_pssm_projection_cred_grp"')), value = t_pssm_projection_cred_grp, overwrite=TRUE)
dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."AgeGroupLookup"')), age_group_lookup, overwrite = TRUE)

# ---- Clean up and disconnect ----
dbDisconnect(decimal_con)
gc()
# rm(list = ls())




