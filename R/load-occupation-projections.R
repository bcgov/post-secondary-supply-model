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
# Load datasets required to run program projections step
# ******************************************************************************

library(tidyverse)
library(RODBC)
library(config)
library(DBI)

# ---- Configure LAN and file paths ----
lan <- config::get("lan")
my_schema <- config::get("myschema")
db_schema <- config::get("db_schema")

# ---- Connection to decimal ----
db_config <- config::get("decimal")
decimal_con <- dbConnect(
  odbc::odbc(),
  Driver = db_config$driver,
  Server = db_config$server,
  Database = db_config$database,
  Trusted_Connection = "True"
)

# -------------------- READ DATA FROM SQL SERVER ------------------------
# read the following tables from {my_schema}, noting that we
# might get FALSE that Cohort_Program_Distributions exists during first run-through.
# Use the Static Cohort_Program_Distributions table?

# Labour_Supply_Distribution
labour_supply_distribution <-
  dbReadTable(
    decimal_con,
    SQL(glue::glue('"{my_schema}"."Labour_Supply_Distribution_r"'))
  ) %>%
  janitor::clean_names(case = "all_caps") |>
  mutate(across(
    any_of(c("PSSM_CRED", "PSSM_CREDENTIAL", "LCP2_CRED", "LCIP2_CRED")),
    ~ str_replace(., " OR ", " or ")
  )) |>
  filter(!SURVEY == "PTIB")

# Labour_Supply_Distribution_LCP2
labour_supply_distribution_lcp2 <-
  dbReadTable(
    decimal_con,
    SQL(glue::glue('"{my_schema}"."Labour_Supply_Distribution_LCP2_r"'))
  ) %>%
  janitor::clean_names(case = "all_caps") |>
  mutate(across(
    any_of(c("PSSM_CRED", "PSSM_CREDENTIAL", "LCP2_CRED", "LCIP2_CRED")),
    ~ str_replace(., " OR ", " or ")
  )) |>
  filter(!SURVEY == "PTIB")

# Labour_Supply_Distribution_No_TT
labour_supply_distribution_no_tt <-
  dbReadTable(
    decimal_con,
    SQL(glue::glue('"{my_schema}"."Labour_Supply_Distribution_No_TT_r"'))
  ) %>%
  janitor::clean_names(case = "all_caps") |>
  mutate(across(
    any_of(c("PSSM_CRED", "PSSM_CREDENTIAL", "LCP2_CRED", "LCIP2_CRED")),
    ~ str_replace(., " OR ", " or ")
  )) |>
  filter(!SURVEY == "PTIB")

# Labour_Supply_Distribution_LCP2_No_TT
labour_supply_distribution_lcp2_no_tt <-
  dbReadTable(
    decimal_con,
    SQL(glue::glue('"{my_schema}"."Labour_Supply_Distribution_LCP2_No_TT_r"'))
  ) %>%
  janitor::clean_names(case = "all_caps") |>
  mutate(across(
    any_of(c("PSSM_CRED", "PSSM_CREDENTIAL", "LCP2_CRED", "LCIP2_CRED")),
    ~ str_replace(., " OR ", " or ")
  )) |>
  filter(!SURVEY == "PTIB")

# Occupation_Distributions
occupation_distributions <-
  dbReadTable(
    decimal_con,
    SQL(glue::glue('"{my_schema}"."Occupation_Distributions_r"'))
  ) %>%
  janitor::clean_names(case = "all_caps") |>
  mutate(across(
    any_of(c("PSSM_CRED", "PSSM_CREDENTIAL", "LCP2_CRED", "LCIP2_CRED")),
    ~ str_replace(., " OR ", " or ")
  )) |>
  filter(!SURVEY == "PTIB")


# Occupation_Distributions_No_TT
occupation_distributions_no_tt <-
  dbReadTable(
    decimal_con,
    SQL(glue::glue('"{my_schema}"."Occupation_Distributions_No_TT_r"'))
  ) %>%
  janitor::clean_names(case = "all_caps") |>
  mutate(across(
    any_of(c("PSSM_CRED", "PSSM_CREDENTIAL", "LCP2_CRED", "LCIP2_CRED")),
    ~ str_replace(., " OR ", " or ")
  )) |>
  filter(!SURVEY == "PTIB")

# Occupation_Distributions_LCP2
occupation_distributions_lcp2 <-
  dbReadTable(
    decimal_con,
    SQL(glue::glue('"{my_schema}"."Occupation_Distributions_LCP2_r"'))
  ) %>%
  janitor::clean_names(case = "all_caps") |>
  mutate(across(
    any_of(c("PSSM_CRED", "PSSM_CREDENTIAL", "LCP2_CRED", "LCIP2_CRED")),
    ~ str_replace(., " OR ", " or ")
  )) |>
  filter(!SURVEY == "PTIB")


# Occupation_Distributions_LCP2_No_TT
occupation_distributions_lcp2_no_tt <-
  dbReadTable(
    decimal_con,
    SQL(glue::glue('"{my_schema}"."Occupation_Distributions_LCP2_No_TT_r"'))
  ) %>%
  janitor::clean_names(case = "all_caps") |>
  mutate(across(
    any_of(c("PSSM_CRED", "PSSM_CREDENTIAL", "LCP2_CRED", "LCIP2_CRED")),
    ~ str_replace(., " OR ", " or ")
  )) |>
  filter(!SURVEY == "PTIB")

# Cohort_Program_Distributions_Projected
cohort_program_distributions_projected <-
  dbReadTable(
    decimal_con,
    SQL(glue::glue('"{my_schema}"."Cohort_Program_Distributions_Projected_r"'))
  ) %>%
  janitor::clean_names(case = "all_caps") |>
  mutate(across(
    any_of(c("PSSM_CRED", "PSSM_CREDENTIAL", "LCP2_CRED", "LCIP2_CRED")),
    ~ str_replace(., " OR ", " or ")
  ))

# Cohort_Program_Distributions_Static
cohort_program_distributions_static <-
  dbReadTable(
    decimal_con,
    SQL(glue::glue('"{my_schema}"."Cohort_Program_Distributions_Static_r"'))
  ) %>%
  janitor::clean_names(case = "all_caps") |>
  mutate(across(
    any_of(c("PSSM_CRED", "PSSM_CREDENTIAL", "LCP2_CRED", "LCIP2_CRED")),
    ~ str_replace(., " OR ", " or ")
  ))

# Graduate_Projection
graduate_projections <-
  dbReadTable(
    decimal_con,
    SQL(glue::glue('"{my_schema}"."Graduate_Projections_r"'))
  ) %>%
  janitor::clean_names(case = "all_caps") |>
  mutate(across(
    any_of(c("PSSM_CRED", "PSSM_CREDENTIAL", "LCP2_CRED", "LCIP2_CRED")),
    ~ str_replace(., " OR ", " or ")
  ))

infoware_l_cip_4digits_cip2016 <-
  dbReadTable(
    decimal_con,
    SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_4DIGITS_CIP2016"'))
  ) %>%
  janitor::clean_names(case = "all_caps")

infoware_l_cip_6digits_cip2016 <-
  dbReadTable(
    decimal_con,
    SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_6DIGITS_CIP2016"'))
  ) %>%
  janitor::clean_names(case = "all_caps")

# -------------------- LOAD LOOKUPS ------------------------
t_exclude_from_projections_lcp4_cd <-
  readr::read_csv(
    glue::glue(
      "{lan}/development/csv/gh-source/lookups/07/T_Exclude_from_Projections_LCP4_CD.csv"
    ),
    col_types = cols(.default = col_guess())
  ) %>%
  janitor::clean_names(case = "all_caps") %>%
  mutate(across(everything(), ~ as.character(.)))

# this is an empty table!!
t_exclude_from_projections_lcip4_cred <-
  readr::read_csv(
    glue::glue(
      "{lan}/development/csv/gh-source/lookups/07/T_Exclude_from_Projections_LCIP4_CRED.csv"
    ),
    col_types = cols(.default = col_guess())
  ) %>%
  janitor::clean_names(case = "all_caps") %>%
  mutate(across(everything(), ~ as.character(.)))

# this is an empty table!!
t_exclude_from_projections_pssm_credential <-
  readr::read_csv(
    glue::glue(
      "{lan}/development/csv/gh-source/lookups/07/T_Exclude_from_Projections_PSSM_Credential.csv"
    ),
    col_types = cols(.default = col_guess())
  ) %>%
  janitor::clean_names(case = "all_caps") %>%
  mutate(across(everything(), ~ as.character(.)))

t_exclude_from_labour_supply_unknown_lcp2_proxy <-
  readr::read_csv(
    glue::glue(
      "{lan}/development/csv/gh-source/lookups/07/T_Exclude_from_Labour_Supply_Unknown_LCP2_Proxy.csv"
    ),
    col_types = cols(.default = col_guess())
  ) %>%
  janitor::clean_names(case = "all_caps") %>%
  mutate(across(everything(), ~ as.character(.)))

t_lcp2_lcp4 <-
  readr::read_csv(
    glue::glue("{lan}/development/csv/gh-source/lookups/07/T_LCP2_LCP4.csv"),
    col_types = cols(.default = col_guess())
  ) %>%
  janitor::clean_names(case = "all_caps") %>%
  mutate(across(everything(), ~ as.character(.)))

tbl_age_groups <-
  readr::read_csv(
    glue::glue("{lan}/development/csv/gh-source/lookups/07/tbl_Age_Groups.csv"),
    col_types = cols(.default = col_guess())
  ) %>%
  janitor::clean_names(case = "all_caps")

tbl_age_groups_rollup <-
  readr::read_csv(
    glue::glue(
      "{lan}/development/csv/gh-source/lookups/07/tbl_Age_Groups_Rollup.csv"
    ),
    col_types = cols(.default = col_guess())
  ) %>%
  janitor::clean_names(case = "all_caps")

t_current_region_pssm_rollup_codes <-
  readr::read_csv(
    glue::glue(
      "{lan}/development/csv/gh-source/lookups/07/T_Current_Region_PSSM_Rollup_Codes.csv"
    ),
    col_types = cols(.default = col_guess())
  ) %>%
  janitor::clean_names(case = "all_caps")

t_current_region_pssm_rollup_codes_bc <-
  readr::read_csv(
    glue::glue(
      "{lan}/development/csv/gh-source/lookups/07/T_Current_Region_PSSM_Rollup_Codes_BC.csv"
    ),
    col_types = cols(.default = col_guess())
  ) %>%
  janitor::clean_names(case = "all_caps")

t_pssm_cred_recode <-
  readr::read_csv(
    glue::glue(
      "{lan}/development/csv/gh-source/lookups/07/T_PSSM_CRED_RECODE.csv"
    ),
    col_types = cols(.default = col_guess())
  ) %>%
  janitor::clean_names(case = "all_caps")

t_pssm_credential_grouping_appendix <-
  readr::read_csv(
    glue::glue(
      "{lan}/development/csv/gh-source/lookups/07/T_PSSM_Credential_Grouping_Appendix.csv"
    ),
    col_types = cols(.default = col_guess())
  ) %>%
  janitor::clean_names(case = "all_caps")
# T_NOC_Skill_Type <-
#   readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/07/T_NOC_Skill_Type.csv"),  col_types = cols(.default = col_guess())) %>%
#   janitor::clean_names(case = "all_caps")

t_noc_broad_categories <-
  readr::read_csv(
    glue::glue(
      "{lan}/development/csv/gh-source/lookups/02/T_NOC_Broad_Categories_Updated.csv"
    ),
    col_types = cols(.default = col_guess())
  ) %>%
  janitor::clean_names(case = "all_caps")

# -------------------- DEVELOPMENT ONLY ------------------------
# dbWriteTable(
#   decimal_con,
#   SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_4DIGITS_CIP2016"')),
#   infoware_l_cip_4digits_cip2016,
#   overwrite = T
# )
#
# dbWriteTable(
#   decimal_con,
#   SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_6DIGITS_CIP2016"')),
#   infoware_l_cip_2digits_cip2016,
#   overwrite = T
# )
#
# dbWriteTable(
#   decimal_con,
#   SQL(glue::glue('"{my_schema}"."T_Exclude_from_Projections_LCP4_CD"')),
#   t_exclude_from_projections_lcp4_cd,
#   overwrite = T
# )
#
#
# dbWriteTable(
#   decimal_con,
#   SQL(glue::glue('"{my_schema}"."T_Exclude_from_Projections_LCIP4_CRED"')),
#   t_exclude_from_projections_lcip4_cred,
#   overwrite = T
# )
#
# dbWriteTable(
#   decimal_con,
#   SQL(glue::glue('"{my_schema}"."T_Exclude_from_Projections_PSSM_Credential"')),
#   t_exclude_from_projections_pssm_credential,
#   overwrite = T
# )
#
# dbWriteTable(
#   decimal_con,
#   SQL(glue::glue(
#     '"{my_schema}"."T_Exclude_from_Labour_Supply_Unknown_LCP2_Proxy"'
#   )),
#   t_exclude_from_labour_supply_unknown_lcp2_proxy,
#   overwrite = T
# )
#
# dbWriteTable(
#   decimal_con,
#   SQL(glue::glue('"{my_schema}"."tbl_Age_Groups"')),
#   tbl_age_groups,
#   overwrite = T
# )
#
# dbWriteTable(
#   decimal_con,
#   SQL(glue::glue('"{my_schema}"."tbl_Age_Groups_Rollup"')),
#   tbl_age_groups_rollup,
#   overwrite = T
# )
#
# dbWriteTable(
#   decimal_con,
#   SQL(glue::glue('"{my_schema}"."T_noc_broad_categories"')),
#   t_noc_broad_categories,
#   overwrite = T
# )
#
# dbWriteTable(
#   decimal_con,
#   SQL(glue::glue('"{my_schema}"."T_Current_Region_PSSM_Rollup_Codes"')),
#   t_current_region_pssm_rollup_codes,
#   overwrite = T
# )
#
#
# dbWriteTable(
#   decimal_con,
#   SQL(glue::glue('"{my_schema}"."T_Current_Region_PSSM_Rollup_Codes_BC"')),
#   t_current_region_pssm_rollup_codes_bc,
#   overwrite = T
# )
#
#
# # dbWriteTable(decimal_con, name = "tbl_NOC_Skill_Level_Aged_17_34",  tbl_NOC_Skill_Level_Aged_17_34)
# #dbWriteTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_NOC_Skill_Type"')),  T_NOC_Skill_Type)
#
# dbWriteTable(
#   decimal_con,
#   SQL(glue::glue('"{my_schema}"."T_PSSM_CRED_RECODE"')),
#   t_pssm_cred_recode,
#   overwrite = T
# )
#
# if (regular_run == T | ptib_run == T) {
#   dbWriteTable(
#     decimal_con,
#     SQL(glue::glue('"{my_schema}"."T_PSSM_Credential_Grouping_Appendix"')),
#     t_pssm_credential_grouping_appendix,
#     overwrite = T
#   )
#   dbWriteTable(
#     decimal_con,
#     SQL(glue::glue('"{my_schema}"."T_LCP2_LCP4"')),
#     t_lcp2_lcp4,
#     overwrite = T
#   )
# }
#
# ---- Disconnect ----
# dbDisconnect(decimal_con)
# Retrieve table names from the database
