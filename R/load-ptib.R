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
# Load PTIB enrolment data from staging area in LAN project folder, to decimal.
# Raw data is in excel so some cleaning req'd to handle cip code conversion issues
# ******************************************************************************
library(tidyverse)
library(odbc)
library(DBI)
library(config)
library(readxl)
library(janitor)

# ---- Configure LAN and file paths ----
lan <- config::get("lan")
raw_data_file <- glue::glue(
  "{lan}/data/ptib/PTIB 2021 and 2022 Enrolment Data for BC Stats 2024.05.31.xlsx"
)
my_schema <- config::get("myschema")
db_schema <- config::get("dbschema")

# ---- Connection to decimal ----
db_config <- config::get("decimal")
decimal_con <- dbConnect(
  odbc(),
  Driver = db_config$driver,
  Server = db_config$server,
  Database = db_config$database,
  Trusted_Connection = "True"
)

# ---- Read raw data  ----
raw_data <- read_xlsx(raw_data_file, sheet = 1, skip = 2)

# ---- Clean data ----
# this needs a review - the CIP code rounding
# seems right, but there is at least one CIP that hasn't been
# converted properly
cleaned_data <- raw_data %>%
  clean_names() %>%
  rename(
    year = calendar_year,
    credential = credential_6,
    graduates = credential_8
  ) %>% ## column is total_enrolments - enrolments_not_graduated
  mutate(
    cip1 = str_sub(cip, end = 2) %>%
      str_remove_all("\\.") %>%
      str_pad(width = "2", side = "left", pad = "0"),
    cip2 = ifelse(
      !is.na(str_extract(cip, "(\\.[:digit:]*)+")),
      str_extract(cip, "(\\.[:digit:]*)+"),
      0
    ) %>%
      str_replace_all("(\\.[:digit:]*)\\.", "\\1") %>%
      as.numeric() %>%
      round_half_up(digits = 4) %>%
      as.character() %>%
      str_remove_all("^0\\.") %>%
      str_pad(width = 4, side = "right", pad = "0"),
    cip3 = paste(cip1, cip2, sep = ".")
  )

# ---- Aggregate data ----
ptib_data <- cleaned_data %>%
  group_by(year, credential, cip3, age_group, immigration_status) %>%
  summarize(
    sum_of_graduates = sum(graduates, na.rm = TRUE),
    sum_of_enrolments = sum(enrolments_not_graduated, na.rm = TRUE),
    sum_of_total_enrolments = sum(total_enrolments, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  rename(cip = cip3)

# ---- Read Outcomes Data ----
infoware <- read_csv(
  (glue::glue(
    "{lan}\\development\\csv\\gh-source\\lookups\\05\\INFOWARE_L_CIP_6DIGITS_CIP2016.csv"
  ))
)

# ---- Read LAN data ----
## Lookups
pssm_cred_grps <- read_csv(
  (glue::glue(
    "{lan}\\development\\csv\\gh-source\\lookups\\05\\T_PSSM_Credential_Grouping.csv"
  ))
) %>%
  janitor::clean_names(case = "all_caps")

T_PTIB_Y1_to_Y10 <- read_csv(
  (glue::glue(
    "{lan}\\development\\csv\\gh-source\\lookups\\05\\T_PTIB_Y1_to_Y10.csv"
  ))
) |>
  janitor::clean_names(case = "all_caps")

# other tables should be in the R environment from earlier analysis
grad_proj <- dbReadTable(
  decimal_con,
  SQL(glue::glue('"{db_schema}"."Graduate_Projections"'))
)

cpd_proj <- dbReadTable(
  decimal_con,
  SQL(glue::glue('"{db_schema}"."Cohort_Program_Distributions_Projected"'))
)

cpd_static <- dbReadTable(
  decimal_con,
  SQL(glue::glue('"{db_schema}"."Cohort_Program_Distributions_Static"'))
)

# ---- Write to decimal ----
# Lookups
dbWriteTable(
  decimal_con,
  SQL(glue::glue('"{my_schema}"."T_PSSM_Credential_Grouping"')),
  pssm_cred_grps
)
dbWriteTable(
  decimal_con,
  SQL(glue::glue('"{my_schema}"."T_PTIB_Y1_to_Y10"')),
  T_PTIB_Y1_to_Y10
)
dbWriteTable(
  decimal_con,
  SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_6DIGITS_CIP2016"')),
  infoware
)

# Main dataset
dbWriteTable(
  decimal_con,
  SQL(glue::glue('"{my_schema}"."T_Private_Institutions_Credentials_Raw"')),
  ptib_data,
  overwrite = TRUE
)

# ---- Disconnect ----
dbDisconnect(decimal_con)
# rm(list = ls())
gc()
