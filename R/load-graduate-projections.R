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
decimal_con <- dbConnect(
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

# ---- Read data from decimal  ----
assert_that(
  dbExistsTable(
    decimal_con,
    SQL(glue::glue('"{my_schema}"."qry09c_MinEnrolment"'))
  ),
  msg = "Import qry09c_MinEnrolment; from dbschema or run 01e-stp-distributions.R before continuing."
)

# this fails but I can still draw from dbo
assert_that(
  dbExistsTable(
    decimal_con,
    SQL(glue::glue(
      '"{my_schema}"."Credential_By_Year_Gender_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs"'
    ))
  ),
  msg = "Import table 'Credential_By_Year_Gender_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs' from dbschema or run 01e-stp-distributions.R before continuing."
)

population_projections <- dbReadTable(decimal_con, "population_projections")

min_enrolments <- dbReadTable(
  decimal_con,
  SQL(glue::glue('"{my_schema}"."qry09c_MinEnrolment"'))
)
credentials <- dbReadTable(
  decimal_con,
  SQL(glue::glue(
    '"{my_schema}"."Credential_By_Year_Gender_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs"'
  ))
)

# ---- Write to decimal ----
dbWriteTable(
  decimal_con,
  name = SQL(glue::glue('"{my_schema}"."population_projections"')),
  population_projections
)

# ---- Disconnect ----
dbDisconnect(decimal_con)
