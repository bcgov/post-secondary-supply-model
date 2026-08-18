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
library(DBI)
library(assertthat)


# ---- Configure LAN and file paths ----
lan <- config::get("lan")
my_schema <- config::get("myschema")
db_schema <- config::get("dbschema")

# ---- Connection to decimal ----
db_config <- config::get("decimal")
con <- dbConnect(
  odbc::odbc(),
  Driver = db_config$driver,
  Server = db_config$server,
  Database = db_config$database,
  Trusted_Connection = "True"
)

# ---- Read raw data  ----
raw_data_file_path <- glue::glue(
  "{lan}/data/people2020/population_projections.csv",
  overwrite = TRUE
)

population_projections <- readr::read_csv(
  raw_data_file_path,
  col_types = cols(.default = col_guess())
) %>%
  janitor::clean_names(case = "all_caps")

# population_projections arrives WIDE: one column per age band, named like
# "X20_TO_24". Reshape to long, keep only F/M (drops "Gender Diverse"), relabel
# gender to words, and strip the "X" / "_TO_" so AGE_GROUP matches the other
# tables (e.g. "20 to 24").
population_projections <- population_projections |>
  select(-c(REGION, LOCAL_HEALTH_AREA, TOTAL, TYPE)) %>%
  group_by(YEAR, GENDER) |>
  # age groups are in single year increments, one year per column.  We need to pivot longer to get a single column for age group and a single column for population count.
  pivot_longer(
    cols = starts_with("X"),
    names_to = "AGES",
    values_to = "POP"
  ) |>
  mutate(AGE_GROUP = as.numeric(str_remove(AGES, "X"))) |>
  filter(AGE_GROUP >= 15 & AGE_GROUP <= 89) |>
  mutate(
    AGE_GROUP = case_when(
      AGE_GROUP >= 15 & AGE_GROUP <= 16 ~ "15 to 16",
      AGE_GROUP >= 17 & AGE_GROUP <= 19 ~ "17 to 19",
      AGE_GROUP >= 20 & AGE_GROUP <= 24 ~ "20 to 24",
      AGE_GROUP >= 25 & AGE_GROUP <= 29 ~ "25 to 29",
      AGE_GROUP >= 30 & AGE_GROUP <= 34 ~ "30 to 34",
      AGE_GROUP >= 35 & AGE_GROUP <= 44 ~ "35 to 44",
      AGE_GROUP >= 45 & AGE_GROUP <= 54 ~ "45 to 54",
      AGE_GROUP >= 55 & AGE_GROUP <= 64 ~ "55 to 64",
      AGE_GROUP >= 65 & AGE_GROUP <= 89 ~ "65 to 89"
    )
  ) |>
  mutate(
    GENDER = case_when(
      GENDER == "F" ~ "Woman/Girl",
      GENDER == "M" ~ "Man/Boy"
    )
  ) |>
  group_by(YEAR, GENDER, AGE_GROUP) |>
  summarise(POP = sum(POP, na.rm = TRUE)) |>
  ungroup()

min_enrolments <- dbReadTable(
  con,
  SQL(glue::glue('"{my_schema}"."qry09c_minenrolment_r"'))
)

credentials <- dbReadTable(
  con,
  SQL(glue::glue(
    '"{my_schema}"."Credential_By_Year_Gender_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs_r"'
  ))
)

## ------------------------------------ Clean Up --------------------------------------------------
# Current workflow:
#  - Write key tables back to sql server.  These are tables needed for downstream work, or tables
# that might be needed for later reference outside of this analysis.
#  - Close DB connections
#  - Remove all objects at the end of each script.
## ------------------------------------------------------------------------------------------------

tables_to_keep <- c(
  "population_projections"
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


# ---- Disconnect ----
dbDisconnect(con)
