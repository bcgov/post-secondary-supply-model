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

# Description:Read one SQL Server table into R without transforming values.
# Inputs:
#   - con (DBI connection): active SQL Server connection.
#   - schema (character): source schema name.
#   - table (character): source table name.
# Output:
#   - data.frame/tibble: full table contents.
read_table <- function(con, schema, table) {
  dbReadTable(con, SQL(glue::glue('"{schema}"."{table}"')))
}

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

# Description:Keep only shared columns across two tables.
# Inputs:
#   - df1 (table): first table.
#   - df2 (table): second table.
# Output:
#   - list(df1, df2): both tables restricted to their common columns.
select_common <- function(df1, df2) {
  common <- intersect(names(df1), names(df2))
  list(df1 = select(df1, any_of(common)), df2 = select(df2, any_of(common)))
}

# Description:Pivot year columns from wide to long format.
# Inputs:
#   - df (table): table containing year columns starting with "20".
#   - value_name (character): output value column name.
# Output:
#   - tibble: long table with YEAR + value_name columns.
pivot_years_long <- function(df, value_name = "VAL") {
  df |>
    pivot_longer(
      cols = starts_with("20"),
      names_to = "YEAR",
      values_to = value_name
    )
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

# Description:Join two tables and compute absolute/percent differences.
# Inputs:
#   - left, right (tables): tables to compare.
#   - keys (character vector): join keys.
#   - left_value, right_value (character): numeric comparison columns after join.
#   - left_suffix, right_suffix (character): suffixes for overlapping names.
# Output:
#   - tibble: joined table plus diff and p_diff columns.
compare_tables <- function(
  left,
  right,
  keys,
  left_value,
  right_value,
  left_suffix,
  right_suffix
) {
  left |>
    full_join(right, by = keys, suffix = c(left_suffix, right_suffix)) |>
    mutate(
      diff = abs(.data[[left_value]] - .data[[right_value]]),
      p_diff = safe_percent_diff(.data[[left_value]], .data[[right_value]])
    )
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
model_tables <- list(
  sql = read_table(con, my_schema, "tmp_tbl_model"),
  r = read_table(con, my_schema, "tmp_tbl_model_r"),
  dbo = read_table(con, db_schema, "tmp_tbl_model")
) |>
  purrr::map(clean_projection_names)

# align columns to R version once
sql_r_common <- select_common(model_tables$sql, model_tables$r)
dbo_r_common <- select_common(model_tables$dbo, model_tables$r)

tmp_tbl_model_sql_long <- pivot_years_long(sql_r_common$df1)
tmp_tbl_model_r_long <- pivot_years_long(sql_r_common$df2)
tmp_tbl_model_dbo_long <- pivot_years_long(dbo_r_common$df1)
tmp_tbl_model_r_long2 <- pivot_years_long(dbo_r_common$df2)

diff_r_sql <- compare_tables(
  left = tmp_tbl_model_sql_long,
  right = tmp_tbl_model_r_long,
  keys = MODEL_KEYS,
  left_value = "VAL.sql",
  right_value = "VAL.r",
  left_suffix = ".sql",
  right_suffix = ".r"
)

diff_r_dbo <- compare_tables(
  left = tmp_tbl_model_dbo_long,
  right = tmp_tbl_model_r_long2,
  keys = MODEL_KEYS,
  left_value = "VAL.dbo",
  right_value = "VAL.r",
  left_suffix = ".dbo",
  right_suffix = ".r"
)

# ---------------- graduate projections ----------------
grad_proj_sql <- read_table(con, my_schema, "graduate_projections") |>
  rename_with(toupper)
grad_proj_r <- read_table(con, my_schema, "graduate_projections_r") |>
  rename_with(toupper)
grad_proj_dbo <- read_table(con, db_schema, "graduate_projections") |>
  rename_with(toupper)

grad_proj_sql_r <- compare_tables(
  left = grad_proj_sql,
  right = grad_proj_r,
  keys = GRAD_KEYS,
  left_value = "GRADUATES.sql",
  right_value = "GRADUATES.r",
  left_suffix = ".sql",
  right_suffix = ".r"
)

grad_proj_dbo_sql <- grad_proj_dbo |>
  full_join(grad_proj_sql, by = GRAD_KEYS, suffix = c(".dbo", ".r")) |>
  mutate(
    GRADUATES.dbo = coalesce(round(GRADUATES.dbo, 0), 0),
    GRADUATES.r = coalesce(round(GRADUATES.r, 0), 0),
    diff = abs(GRADUATES.dbo - GRADUATES.r),
    p_diff = if_else(GRADUATES.r == 0, NA_real_, diff / GRADUATES.r * 100)
  )
