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

# This script loads student outcomes data for students who students who recently graduated with a
# Baccalaureate degree (Baccalaureate students are surveyed two years after graduation)
#
# The following data set is read from SQL server database:
#   bgs_data_update: unique survey responses for each person/survey year (for years since last model run)
#
# The following data sets are read into SQL server from the LAN:
#   T_BGS_Data: unique survey responses for each person/survey year (last model run)
#   T_weights: carried forward from last models run and updated with new data.  Waiting for confirmation
#   T_BGS_INST_Re-code: look-up used to re-code several institution codes
#
# Notes: T_BGS_Data +  bgs_data_update contain the full set of survey responses used.
# T_BGS_Data_Final_2017.csv used for 2019 model run, T_BGS_Data_Final.csv for 2023 model run (rollover)
# Some changes to variable names were done for consistency and will be needed when using 2023 BGS_Data_Final
# use query BGS_Q001_BGS_Data_2019_2023 for 2023 model run but note overlapping years (2019)

library(tidyverse)
library(config)
library(DBI)
library(odbc)

regular_run <- T
qi_run <- F
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

# ---- Read T_BGS_Data_Final from SQL Server ----
t_bgs_data_final_for_outcomesmatching <- dbReadTable(
  con,
  Id(schema = my_schema, table = "t_bgs_data_final_for_outcomesmatching_r")
)

# ---- Read LAN Data ----
# Lookups
t_weights <-
  readr::read_csv(
    glue::glue("{lan}/development/csv/gh-source/lookups/02/T_Weights.csv"),
    col_types = cols(
      Group = "d",
      Weight = "d",
      WeightQI = "d",
      .default = col_character()
    )
  ) %>%
  janitor::clean_names(case = "all_caps")

tmp_bgs_inst_cds <-
  readr::read_csv(
    glue::glue(
      "{lan}/development/csv/gh-source/lookups/02/tmp_BGS_INST_REGION_CDS.csv"
    ),
    col_types = cols(.default = col_character())
  ) %>%
  janitor::clean_names(case = "all_caps")

t_bgs_inst_recode <-
  readr::read_csv(
    glue::glue(
      "{lan}/development/csv/gh-source/lookups/02/T_BGS_INST_Recode.csv"
    ),
    col_types = cols(.default = col_character())
  ) %>%
  janitor::clean_names(case = "all_caps")


# ---- Read Outcomes Data ----
if (regular_run == T | ptib_run == T) {
  bgs_data_update <- read_csv(glue::glue(
    "{lan}/data/student-outcomes/csv/so-provision/BGS_Q001_BGS_Data_2019_2023.csv"
  ))

  bgs_data_update <- bgs_data_update %>%
    rename(
      "FULL_TM_WRK" = FULL_TM,
      "FULL_TM_SCHOOL" = D03_STUDYING_FT,
      "IN_LBR_FRC" = LBR_FRC_LABOUR_MARKET,
      "EMPLOYED" = LBR_FRC_CURRENTLY_EMPLOYED,
      "UNEMPLOYED" = LBR_FRC_UNEMPLOYED,
      "TRAINING_RELATED" = E10_IN_TRAINING_RELATED_JOB,
      "TOOK_FURTH_ED" = D01_R1
    ) %>% # this can be added to original query
    mutate(AGE_17_34 = if_else(between(AGE, 17, 34), 1, 0)) %>%
    mutate(OLD_LABOUR_SUPPLY = NA) %>% # I don't think we use this?
    select(-c(D02_R1_CURRENTLY_STUDYING, SUBM_CD)) # nor these?

  bgs_data_update <- bgs_data_update %>%
    mutate(
      CURRENT_REGION_PSSM_CODE = case_when(
        REGION_CD %in% 1:8 ~ REGION_CD,
        CURRENT_REGION %in% c(6, 9, 10) ~ 10,
        CURRENT_REGION == 7 ~ 11,
        CURRENT_REGION == 5 ~ 9,
        CURRENT_REGION == 8 ~ -1,
        TRUE ~ NA
      )
    )

  bgs_data_update <- bgs_data_update %>%
    inner_join(tmp_bgs_inst_cds, by = join_by(INST)) %>%
    mutate(
      CURRENT_REGION_PSSM_CODE = if_else(
        (CURRENT_REGION_PSSM_CODE == -1 | is.na(CURRENT_REGION_PSSM_CODE)) &
          (SRV_Y_N == 0 | is.na(SRV_Y_N)),
        as.numeric(CURRENT_REGION_PSSM),
        CURRENT_REGION_PSSM_CODE
      )
    )

  # ---- Make T_BGS_Data_Final ----
  t_bgs_data_final <- bgs_data_update %>%
    select(-c(CUR_RES, REGION_CD, CURRENT_REGION))
}

## ------------------------------------ Clean Up --------------------------------------------------
# Current workflow:
#  - Write key tables back to sql server.  These are tables needed for downstream work, or tables
# that might be needed for later reference outside of this analysis.
#  - Close DB connections
#  - Remove all objects at the end of each script.
## ------------------------------------------------------------------------------------------------

tables_to_keep <- c(
  "t_bgs_data_final",
  "t_bgs_data_final_for_outcomesmatching",
  "t_weights",
  "t_bgs_inst_recode" #,
  #"tmp_bgs_inst_cds"
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
