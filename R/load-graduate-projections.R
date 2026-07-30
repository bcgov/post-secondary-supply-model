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

min_enrolments <- dbReadTable(
  con,
  SQL(glue::glue('"{my_schema}"."qry09c_MinEnrolment_r"'))
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
