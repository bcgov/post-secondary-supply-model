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
# Load custom Statistics Canada data from staging area in LAN project folder, to decimal.
# ******************************************************************************

# ---- libraries and global variables
library(tidyverse)
library(janitor)

# ---- Configure LAN Paths ----
lan <- config::get("lan")
raw_data_file <- glue::glue("{lan}/data/statcan/stat-can-data-export.csv")

# ----- Connection to decimal ----
db_config <- config::get("decimal")
con <- dbConnect(
  odbc(),
  Driver = db_config$driver,
  Server = db_config$server,
  Database = db_config$database,
  Trusted_Connection = "True"
)

# ---- Read raw data  ----
raw_data <- read_csv(raw_data_file, locale = locale(encoding = "latin1"))

# ---- Clean data ----
data <- raw_data %>%
  clean_names() %>%
  rename(
    age_group = age,
    occupation_NOC = occupation,
    masters_degree_and_earned_doctorate = master_s_degree_and_earned_doctorate # funky apostrophe header name
  ) %>%
  # fix the geography column en-dashes
  mutate(geography = str_replace(geography, "\u0096", "-"))

# ---- Write to decimal ----
dbWriteTableArrow(
  con,
  name = "STAT_CAN_r",
  nanoarrow::as_nanoarrow_array_stream(data)
)

# ---- Read from decimal ----
dbReadTable(con, "STAT_CAN_r")

# check
stat_can_data |> filter(region == "missing") # expect 0 rows
stat_can_data |> count(region, geography) # review regions

# review age groups and major fields total variable names
stat_can_data |> count(age_group)
stat_can_data |> count(major_field_cip)

# filter out totals from age and study fields
stat_can_data <- stat_can_data |>
  filter(
    age_group != "Total - population 17 to 64 years old" &
      major_field_cip !=
        "Total - Major Field of study (BC Program Cluster aggregation of CIP 2016)"
  )

t_current_region_pssm_rollup_codes_statcan <-
  readr::read_csv(
    glue::glue(
      "{lan}/development/csv/gh-source/lookups/02/T_Current_Region_PSSM_Rollup_Codes_StatCan.csv"
    ),
    col_types = cols(.default = col_guess())
  ) %>%
  janitor::clean_names(case = "all_caps")

tbl_age_groups_rollup <- data.frame(
  Age_Group_Rollup = c(1, 2, 3),
  Age_Group_Rollup_Label = c("17 to 29", "30 to 44", "45 to 64")
)
names(tbl_age_groups_rollup) <- toupper(names(tbl_age_groups_rollup))

tables_to_keep <- c(
  "stat_can_data",
  "t_current_region_pssm_rollup_codes_statcan",
  "tbl_age_groups_rollup"
)

rm(list = setdiff(ls(), tables_to_keep))
gc()
