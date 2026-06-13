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

# This script loads student outcomes data for students who students who recently graduated after
# completing programs at public colleges, institutes, and teaching-intensive universities (~18 months prior)
#
# The following data-set is read from SQL server database:
#   t_dacso_data_part_1_stepa: unique survey responses for each person/survey year (for years since last model run)
#   infoware_c_outc_clean_short_resp:
#
# The following data-sets are read into SQL server from the LAN:
#   tbl_Age: bins ages into groups (1-10)
#   tbl_Age_Groups: used to assign a label to each age group.
#   t_current_region_pssm_rollup_codes_bc: loo-up re-codes maps regions
#   t_current_region_pssm_rollup_codes: look-up re-codes maps regions
#   t_current_region_pssm_codes: look-up re-codes maps regions
#   tbl_noc_skill_level_aged_17_34: used SQL server for upload as this file contains non-supported type characters
#   T_PSSM_Credential_Grouping: a static table for relabeling credential names
#   T_year_survey_year: TO DO
#   T_Cohorts_Recoded:  this contains survey records for all years.  The table is refreshed in the workflow.
#
# Notes: tbl_noc_skill_level_aged_17_34: used SQL server for upload as this file contains 'non-supported' type characters.
#   T_year_survey_year is carried forward from last models run and updated with new data.
#   T_Cohorts_Recoded: can instead just be created each year.  See comments in 02b-pssm-cohots-dacso.R
#   Age group labels are assigned in the script.  There are two different groupings used to group students by age in the model,
#    check the groupings are the same in DACSO, APPSO etc cohorts

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

infoware_c_outc_clean_short_resp <- read_csv(glue::glue(
  "{lan}/data/student-outcomes/csv/so-provision/infoware_c_outc_clean_short_resp.csv"
))

tbl_age_groups <-
  readr::read_csv(
    glue::glue("{lan}/development/csv/gh-source/lookups/02/tbl_Age_Groups.csv"),
    col_types = cols(.default = col_guess())
  ) %>%
  janitor::clean_names(case = "all_caps")

tbl_age_groups_rollup <-
  readr::read_csv(
    glue::glue(
      "{lan}/development/csv/gh-source/lookups/02/tbl_Age_Groups_Rollup.csv"
    ),
    col_types = cols(.default = col_guess())
  ) %>%
  janitor::clean_names(case = "all_caps")

tbl_age <-
  readr::read_csv(
    glue::glue("{lan}/development/csv/gh-source/lookups/02/tbl_Age.csv"),
    col_types = cols(.default = col_guess())
  ) %>%
  janitor::clean_names(case = "all_caps")

t_pssm_credential_grouping <-
  readr::read_csv(
    glue::glue(
      "{lan}/development/csv/gh-source/lookups/02/T_PSSM_Credential_Grouping.csv"
    ),
    col_types = cols(.default = col_guess())
  ) %>%
  janitor::clean_names(case = "all_caps")

t_year_survey_year <-
  readr::read_csv(
    glue::glue(
      "{lan}/development/csv/gh-source/lookups/02/t_year_survey_year.csv"
    ),
    col_types = cols(.default = col_guess())
  ) %>%
  janitor::clean_names(case = "all_caps")

t_cohorts_recoded <-
  readr::read_csv(
    glue::glue(
      "{lan}/development/csv/gh-source/rollover/02/T_Cohorts_Recoded.csv"
    ),
    col_types = cols(
      PEN = "c",
      STQU_ID = "c",
      Survey = "c",
      LCIP_CD = "c",
      LCP4_CD = "c",
      NOC_CD = "c",
      INST_CD = "c",
      PSSM_Credential = "c",
      PSSM_CRED = "c",
      LCIP4_CRED = "c",
      LCIP2_CRED = "c",
      .default = col_number()
    ),
    n_max = 1
  ) %>%
  janitor::clean_names(case = "all_caps") |>
  filter(FALSE)

t_current_region_pssm_codes <-
  readr::read_csv(
    glue::glue(
      "{lan}/development/csv/gh-source/lookups/02/T_Current_Region_PSSM_Codes.csv"
    ),
    col_types = cols(.default = col_guess())
  ) %>%
  janitor::clean_names(case = "all_caps")

t_current_region_pssm_rollup_codes <-
  readr::read_csv(
    glue::glue(
      "{lan}/development/csv/gh-source/lookups/02/T_Current_Region_PSSM_Rollup_Codes.csv"
    ),
    col_types = cols(.default = col_guess())
  ) %>%
  janitor::clean_names(case = "all_caps")

t_current_region_pssm_rollup_codes_bc <-
  readr::read_csv(
    glue::glue(
      "{lan}/development/csv/gh-source/lookups/02/T_Current_Region_PSSM_Rollup_Codes_BC.csv"
    ),
    col_types = cols(.default = col_guess())
  ) %>%
  janitor::clean_names(case = "all_caps")

t_noc_broad_categories <-
  readr::read_csv(
    glue::glue(
      "{lan}/development/csv/gh-source/lookups/02/T_NOC_Broad_Categories_Updated.csv"
    ),
    col_types = cols(.default = col_guess())
  ) %>%
  janitor::clean_names(case = "all_caps")


if (regular_run == T | ptib_run == T) {
  t_dacso_data_part_1_stepa <- readr::read_csv(
    glue::glue(
      "{lan}/data/student-outcomes/csv/so-provision/DACSO_Q003_DACSO_DATA_Part_1_stepA.csv"
    )
  )

  t_dacso_data_part_1_stepa <- t_dacso_data_part_1_stepa |>
    mutate(
      CURRENT_REGION_PSSM_CODE = case_when(
        TPID_CURRENT_REGION1 %in%
          c(1, 2, 3, 4, 5, 6, 7, 8) ~ TPID_CURRENT_REGION1,
        TPID_CURRENT_REGION4 == 5 ~ 9,
        TPID_CURRENT_REGION4 == 6 ~ 10,
        TPID_CURRENT_REGION4 == 7 ~ 11,
        TPID_CURRENT_REGION4 == 8 ~ -1,
        TRUE ~ NA_integer_
      )
    )
  # commenting these out for now - see PR
  #|>
  #mutate(
  #  TTRAIN = NA_integer_,
  #  LABR_EMPLOYED = NA_integer_,
  #  COSC_GRAD_STATUS_LGDS_CD = NA_integer_,
  #  COSC_GRAD_STATUS_LGDS_CD_GROUP = NA_integer_,
  #  RESPONDENT = NA_integer_,
  #)
}

t_noc_broad_categories <- t_noc_broad_categories |>
  mutate(
    BROAD_CATEGORY_CODE = NA_character_,
    MAJOR_GROUP_CODE = NA_character_,
    SUB_MAJOR_GROUP_CODE = NA_character_,
    MINOR_GROUP_CODE = NA_character_,
    UNIT_GROUP_CODE = NA_character_
  )

## ------------------------------------ Clean Up --------------------------------------------------
# Current workflow:
#  - Write key tables back to sql server.  These are tables needed for downstream work, or tables
# that might be needed for later reference outside of this analysis.
#  - Close DB connections
#  - Remove all objects at the end of each script.
## ------------------------------------------------------------------------------------------------

tables_to_keep <- c(
  "t_dacso_data_part_1_stepa",
  "infoware_c_outc_clean_short_resp",
  "tbl_age_groups",
  "tbl_age_groups_rollup",
  "tbl_age",
  "t_pssm_credential_grouping",
  "t_year_survey_year",
  "t_cohorts_recoded",
  "t_current_region_pssm_codes",
  "t_current_region_pssm_rollup_codes",
  "t_current_region_pssm_rollup_codes_bc",
  "t_noc_broad_categories"
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
