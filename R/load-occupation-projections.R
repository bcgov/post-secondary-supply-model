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

# ---- Connection to decimal ----
db_config <- config::get("decimal")
con <- dbConnect(
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
    con,
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
    con,
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
    con,
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
    con,
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
    con,
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
    con,
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
    con,
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
    con,
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
    con,
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
    con,
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
    con,
    SQL(glue::glue('"{my_schema}"."Graduate_Projections_r"'))
  ) %>%
  janitor::clean_names(case = "all_caps") |>
  mutate(across(
    any_of(c("PSSM_CRED", "PSSM_CREDENTIAL", "LCP2_CRED", "LCIP2_CRED")),
    ~ str_replace(., " OR ", " or ")
  ))

infoware_l_cip_4digits_cip2016 <-
  dbReadTable(
    con,
    SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_4DIGITS_CIP2016"'))
  ) %>%
  janitor::clean_names(case = "all_caps")

infoware_l_cip_6digits_cip2016 <-
  dbReadTable(
    con,
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

## ------------------------------------ Clean Up --------------------------------------------------
# Current workflow:
#  - Write key tables back to sql server.  These are tables needed for downstream work, or tables
# that might be needed for later reference outside of this analysis.
#  - Close DB connections
#  - Remove all objects at the end of each script.
## ------------------------------------------------------------------------------------------------

tables_to_keep <- c(
  # keep all tables that were read into this script via read_csv, but not the
  # tables read from SQL Server
  "t_exclude_from_projections_lcp4_cd",
  "t_exclude_from_projections_lcip4_cred",
  "t_exclude_from_projections_pssm_credential",
  "t_exclude_from_labour_supply_unknown_lcp2_proxy",
  "t_lcp2_lcp4",
  "tbl_age_groups",
  "tbl_age_groups_rollup",
  "t_current_region_pssm_rollup_codes",
  "t_current_region_pssm_rollup_codes_bc",
  "t_pssm_cred_recode",
  "t_pssm_credential_grouping_appendix",
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
gc()
