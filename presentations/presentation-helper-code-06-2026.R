# Copyright 2026 Province of British Columbia
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations under the License.

library(tidyverse)
library(DBI)
library(odbc)

# ---------------- constants ----------------
MODEL_KEYS <- c(
  "AGE_GROUP_ROLLUP_LABEL",
  "NOC_LEVEL",
  "NOC",
  "ENGLISH_NAME",
  "CURRENT_REGION_PSSM_CODE_ROLLUP",
  "CURRENT_REGION_PSSM_NAME_ROLLUP",
  "YEAR"
)

GRAD_KEYS <- c(
  "PSSM_CRED",
  "YEAR",
  "AGE_GROUP",
  "SURVEY",
  "PSI_CREDENTIAL_CATEGORY",
  "PSSM_CREDENTIAL"
)

# ---------------- helpers ----------------
# Description:Standardize projection-table column names for comparison.
# Inputs:
#   - df (table): projection table in wide format.
# Output:
#   - table: same data with normalized names (upper case, year prefix cleaned,
#     and period separators converted to "/").
clean_projection_names <- function(df) {
  names(df) <- names(df) |>
    toupper() |>
    # remove only leading X (e.g., X2023.2024 -> 2023.2024)
    stringr::str_replace("^X(?=20)", "") |>
    stringr::str_replace_all("\\.", "/")
  df
}

# Description:Calculate symmetric percent difference safely.
# Inputs:
#   - a (numeric vector): first series.
#   - b (numeric vector): second series.
# Output:
#   - numeric vector: percent difference; NA when denominator is 0/NA.
safe_percent_diff <- function(a, b) {
  denom <- (a + b) / 2
  if_else(denom == 0 | is.na(denom), NA_real_, abs(a - b) / denom * 100)
}

# ---------------- connection ----------------
db_config <- config::get("decimal")
my_schema <- config::get("myschema")
db_schema <- config::get("dbschema")

con <- dbConnect(
  odbc(),
  Driver = db_config$driver,
  Server = db_config$server,
  Database = db_config$database,
  Trusted_Connection = "True"
)


# ---------------- load model tables ----------------
sql <- dbReadTable(con, SQL(glue::glue('"{my_schema}"."tmp_tbl_model"'))) |>
  clean_projection_names()

r <- dbReadTable(con, SQL(glue::glue('"{my_schema}"."tmp_tbl_model_r"'))) |>
  clean_projection_names()

dbo <- dbReadTable(con, SQL(glue::glue('"{db_schema}"."tmp_tbl_model"'))) |>
  clean_projection_names()

common <- intersect(names(sql), names(r))
sql <- sql |> select(any_of(common))
r <- r |> select(any_of(common))

common <- intersect(names(sql), names(dbo))
dbo <- dbo |> select(any_of(common))


sql_long <- sql |>
  pivot_longer(
    cols = starts_with("20"),
    names_to = "YEAR",
    values_to = "VAL"
  )

r_long <- r |>
  pivot_longer(
    cols = starts_with("20"),
    names_to = "YEAR",
    values_to = "VAL"
  )

dbo_long <- dbo |>
  pivot_longer(
    cols = starts_with("20"),
    names_to = "YEAR",
    values_to = "VAL"
  )

diff_r_sql <- sql_long |>
  full_join(r_long, by = MODEL_KEYS, suffix = c(".sql", ".r")) |>
  mutate(
    VAL.sql = coalesce(round(VAL.sql, 0), 0),
    VAL.r = coalesce(round(VAL.r, 0), 0),
    diff = abs(VAL.sql - VAL.r),
    p_diff = if_else(
      VAL.sql == 0 & VAL.r == 0,
      0,
      safe_percent_diff(VAL.sql, VAL.r)
    )
  )

diff_r_dbo <- dbo_long |>
  full_join(r_long, by = MODEL_KEYS, suffix = c(".dbo", ".r")) |>
  mutate(
    VAL.dbo = coalesce(round(VAL.dbo, 0), 0),
    VAL.r = coalesce(round(VAL.r, 0), 0),
    diff = abs(VAL.dbo - VAL.r),
    p_diff = if_else(
      VAL.dbo == 0 & VAL.r == 0,
      0,
      safe_percent_diff(VAL.dbo, VAL.r)
    )
  )

# ---------------- graduate projections ----------------
grad_proj_sql <- dbReadTable(
  con,
  SQL(glue::glue('"{my_schema}"."graduate_projections"'))
) |>
  rename_with(toupper)
grad_proj_r <- dbReadTable(
  con,
  SQL(glue::glue('"{my_schema}"."graduate_projections_r"'))
) |>
  rename_with(toupper)
grad_proj_dbo <- dbReadTable(
  con,
  SQL(glue::glue('"{db_schema}"."graduate_projections"'))
) |>
  rename_with(toupper)

grad_proj_sql_r <- grad_proj_sql |>
  full_join(grad_proj_r, by = GRAD_KEYS, suffix = c(".sql", ".r")) |>
  mutate(
    GRADUATES.sql = coalesce(round(GRADUATES.sql, 0), 0),
    GRADUATES.r = coalesce(round(GRADUATES.r, 0), 0),
    diff = abs(GRADUATES.sql - GRADUATES.r),
    p_diff = if_else(
      GRADUATES.sql == 0 & GRADUATES.r == 0,
      0,
      safe_percent_diff(GRADUATES.sql, GRADUATES.r)
    )
  )

grad_proj_dbo_sql <- grad_proj_dbo |>
  full_join(grad_proj_sql, by = GRAD_KEYS, suffix = c(".dbo", ".sql")) |>
  mutate(
    GRADUATES.dbo = coalesce(round(GRADUATES.dbo, 0), 0),
    GRADUATES.sql = coalesce(round(GRADUATES.sql, 0), 0),
    diff = abs(GRADUATES.dbo - GRADUATES.sql),
    p_diff = if_else(
      GRADUATES.dbo == 0 & GRADUATES.sql == 0,
      0,
      safe_percent_diff(GRADUATES.dbo, GRADUATES.sql)
    )
  )

dbDisconnect(con)
