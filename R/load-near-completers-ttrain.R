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

# ---- Data Requirements and SQL Definitons ----
# PR Notes for this script:
# 1) several key tables were made in earlier scripts that I assume will be
# written back to decimal for intermediate storage (between script processes).  For the PR, you may need
# to bring them in from the master schema into your schema before running this code.  You'll need the following key
# tables in decimal: STP_Credential, t_dacso_data_part_1, credential_non_dup, and credential_rank as it isn't in the
# SQl load scripts.
# 2) additionally, for the PR only, you'll need a few lookup tables in decimal.  They are on the LAN but some I hard-coded
# here so you can write them to decimal if you like. The LAN versions are in development/csv/gh-source/lookups. I tried to keep the upper/lower case the same as SQL
# so sometimes you'll see a column with mixed types (we can change later).  The exception is t_pssm_projection_cred_grp; I
# updated the hard-coded values in one column so they were comparable across datasets in R.  SQL Server is not case-sensitive
# so those queries should run as expected, the only implication is if you load this table from LAN the R code will be wrong.
# 3) tmp_tbl_age is oddly designed.  Historically, the analyst appended new data onto old data
# but now we should be able to work with SO team to transfer one csv (possibly split across years) to the LAN.
# For development, we currently append new years to historical data (one of the datasets contains duplicates); we write
# the combined table to decimal along with tmp_tbl_age to get the queries to run.

library(tidyverse)
library(RODBC)
library(config)
library(glue)
library(DBI)

## -------------------------- Configure LAN Paths and DB Connection ------------------------------
## -----------------------------------------------------------------------------------------------
lan <- config::get("lan")
my_schema <- config::get("myschema")
db_schema <- config::get("dbschema")

db_config <- config::get("decimal2025")
con <- dbConnect(
  odbc::odbc(),
  Driver = db_config$driver,
  Server = db_config$server,
  Database = db_config$database,
  Trusted_Connection = "True"
)

## --------------------------------------Required Tables------------------------------------------
## -----------------------------------------------------------------------------------------------

stp_dacso_prgm_credential_lookup <-
  readr::read_csv(
    glue::glue(
      "{lan}/development/csv/gh-source/lookups/STP_DACSO_PRGM_CREDENTIAL_LOOKUP.csv"
    ),
    col_types = cols(.default = col_guess())
  ) |>
  setNames(c(
    "PRGRM_Credential_Awarded",
    "PRGM_Credential_Awarded_Name",
    "STP_PRGM_Credential_Awarded_Name"
  )) |>
  mutate(
    PRGM_Credential_Awarded_Name = case_when(
      PRGM_Credential_Awarded_Name ==
        "Post-degree Diploma" ~ "Post-Degree Diploma",
      PRGM_Credential_Awarded_Name ==
        "Post-degree Certificate" ~ "Post-Degree Certificate",
      TRUE ~ PRGM_Credential_Awarded_Name
    )
  )

combine_creds <-
  readr::read_csv(
    glue::glue("{lan}/development/csv/gh-source/lookups/combine_creds.csv"),
    col_types = cols(.default = col_guess())
  ) |>
  setNames(c(
    "id",
    "combined_cred",
    "prgm_credential_awarded_name",
    "combined_cred_name",
    "use_in_pssm_2017_18"
  ))

t_pssm_projection_cred_grp <-
  readr::read_csv(
    glue::glue(
      "{lan}/development/csv/gh-source/lookups/T_PSSM_Projection_Cred_Grp.csv"
    ),
    col_types = cols(.default = col_guess())
  ) |>
  setNames(c(
    "PSSM_Projection_Credential",
    "PSSM_Credential",
    "PSSM_Credential_Name",
    "COSC_GRAD_STATUS_LGDS_CD"
  )) |>
  mutate(
    PSSM_Projection_Credential = stringr::str_to_title(
      PSSM_Projection_Credential
    )
  ) |>
  add_case(
    PSSM_Projection_Credential = 'University Transfer',
    PSSM_Credential = 'ADGR OR UT',
    PSSM_Credential_Name = 'Associate Degree/University Transfer',
    COSC_GRAD_STATUS_LGDS_CD = 1
  ) |>
  mutate(
    PSSM_Credential_Name = case_when(
      PSSM_Credential_Name ==
        "Post-degree certificate/diploma" ~ "Post-Degree certificate/diploma",
      TRUE ~ PSSM_Credential_Name
    )
  )

credential_rank <- tribble(
  ~PSI_CREDENTIAL_CATEGORY    , ~RANK ,
  "ADVANCED CERTIFICATE"      ,    10 ,
  "ADVANCED DIPLOMA"          ,     9 ,
  "APPRENTICESHIP"            ,    14 ,
  "ASSOCIATE DEGREE"          ,    11 ,
  "BACHELORS DEGREE"          ,     8 ,
  "CERTIFICATE"               ,    13 ,
  "DIPLOMA"                   ,    12 ,
  "DOCTORATE"                 ,     1 ,
  "FIRST PROFESSIONAL DEGREE" ,     7 ,
  "GRADUATE CERTIFICATE"      ,     4 ,
  "GRADUATE DIPLOMA"          ,     3 ,
  "MASTERS DEGREE"            ,     2 ,
  "POST-DEGREE CERTIFICATE"   ,     6 ,
  "POST-DEGREE DIPLOMA"       ,     5
)

tbl_age <- tibble(
  Age = 0:150
) |>
  mutate(
    Age_Group = case_when(
      Age >= 15 & Age <= 16 ~ 0,
      Age >= 17 & Age <= 19 ~ 1,
      Age >= 20 & Age <= 24 ~ 2,
      Age >= 25 & Age <= 29 ~ 3,
      Age >= 30 & Age <= 34 ~ 4,
      Age >= 35 & Age <= 64 ~ 5,
      Age >= 65 & Age < 90 ~ 9,
      Age >= 90 ~ NA_real_,
      TRUE ~ NA_real_ # For Age < 15
    )
  )

age_group_lookup <- tibble(
  Age_Index = 1:5,
  Age_Group = c(
    "17 to 19",
    "20 to 24",
    "25 to 29",
    "30 to 34",
    "35 to 64"
  ),
  Lower_Bound = c(17, 20, 25, 30, 35),
  Upper_Bound = c(19, 24, 29, 34, 64)
)

# lookups
#tbl_age <- tibble(
#  Age = 0:150
#) |>
#  mutate(
#    Age_Group = case_when(
#      Age >= 15 & Age <= 16 ~ 1,
#      Age >= 17 & Age <= 19 ~ 2,
#      Age >= 20 & Age <= 24 ~ 3,
#      Age >= 25 & Age <= 29 ~ 4,
#      Age >= 30 & Age <= 34 ~ 5,
#      Age >= 35 & Age <= 44 ~ 6,
#      Age >= 45 & Age <= 54 ~ 7,
#      Age >= 55 & Age <= 64 ~ 8,
#      Age >= 65 & Age < 90 ~ 9,
#      Age >= 90 ~ NA_real_,
#      TRUE ~ NA_real_ # For Age < 15
#    )
#  )

#age_group_lookup <- tibble(
#  Age_Index = 1:9,
#  Age_Group = c(
#    "15 to 16",
#    "17 to 19",
#    "20 to 24",
#    "25 to 29",
#    "30 to 34",
#    "35 to 44",
#    "45 to 54",
#    "55 to 64",
#    "65 to 89"
#  ),
#  Lower_Bound = c(15, 17, 20, 25, 30, 35, 45, 55, 65),
#  Upper_Bound = c(16, 19, 24, 29, 34, 44, 54, 64, 89)
#)

# ---- Rollover Tables ----
tmp_tbl_age <- read_csv(
  glue::glue(
    "{lan}/development/csv/gh-source/testing/03/tmp_tbl_Age.csv"
  ),
  col_types = "dccccdd"
) |>
  rename_with(tolower) |>
  mutate(across(
    c(tpid_date_of_birth, cosc_enrl_end_date, cosc_grad_credential_date),
    as.Date
  )) |>
  distinct()

tmp_tbl_age_append_2018_2020 <- 2018:2020 |>
  purrr::map_dfr(
    ~ {
      file_path <- glue::glue(
        "{lan}/data/student-outcomes/csv/qry_make_tmp_table_Age_step1_{.x}.csv"
      )
      read_csv(file_path, col_types = "dcdcd")
    }
  ) |>
  rename_with(tolower) |>
  distinct() # duplicates in this data

tmp_tbl_age_append_2021_2025 <-
  read_csv(
    glue::glue(
      "{lan}/data/student-outcomes/csv/qry_make_tmp_table_Age_step1.csv"
    ),
    col_types = "dcdcd"
  ) |>
  rename_with(tolower) |>
  distinct()

tmp_tbl_age_append_new_years <- rbind(
  tmp_tbl_age_append_2018_2020,
  tmp_tbl_age_append_2021_2025
)

# combine all age data from previous and new years
tmp_tbl_age_append_new_years <- tmp_tbl_age_append_new_years |>
  rename_with(tolower) |>
  select(
    cosc_stqu_id = coci_stqu_id,
    cosc_subm_cd = coci_subm_cd,
    tpid_date_of_birth = bthdt,
    cosc_enrl_end_date = enddt,
    coci_age_at_survey = coci_age_at_survey
  ) |>
  mutate(
    tpid_date_of_birth = lubridate::ym(tpid_date_of_birth, quiet = TRUE), # implicitly convert "bad" dates to NA
    cosc_enrl_end_date = lubridate::ym(cosc_enrl_end_date, quiet = TRUE), # implicitly convert "bad" dates to NA
    cosc_grad_credential_date = NA_character_,
    age_at_grad = NA_real_
  )

tmp_tbl_age <- tmp_tbl_age |>
  rbind(tmp_tbl_age_append_new_years) |>
  select(-age_at_grad) |>
  distinct() # just in case

# derive age at grad variable
tmp_tbl_age <- tmp_tbl_age |>
  mutate(
    ref_date = coalesce(cosc_grad_credential_date, cosc_enrl_end_date),
    year_diff = year(ref_date) - year(tpid_date_of_birth),
    birthday_ref_year = make_date(
      year(ref_date),
      month(tpid_date_of_birth),
      day(tpid_date_of_birth)
    ),
    age_at_grad = if_else(
      ref_date < birthday_ref_year,
      year_diff - 1,
      year_diff
    )
  ) |>
  select(-ref_date, -year_diff, -birthday_ref_year)

t_dacso_data_part_1 <- dbReadTable(
  con,
  SQL(glue::glue('"{my_schema}"."t_dacso_data_part_1_r"'))
) |>
  rename_with(tolower) |>
  mutate(coci_pen = as.character(coci_pen))

credential_non_dup <- dbReadTable(
  con,
  SQL(glue::glue('"{my_schema}"."Credential_Non_Dup_r"'))
) |>
  rename_with(tolower)

# read columns id and psi_pen from STP_Credential_r table in decimal schema
stp_credential <- dbGetQuery(
  con,
  SQL(glue::glue(' SELECT ID, PSI_PEN FROM "{my_schema}"."STP_Credential_r"'))
)

# ---- Clean up and disconnect ----
dbDisconnect(con)
gc()

# Notes:

# stp_dacso_prgm_credential_lookup
# SQL version has Post-degree Certificate and Post-degree Diploma
# R version has Post-Degree Certificate and Post-Degree Diploma
# Implications: I think this may have inadvertently dropped rows.
# Is this change the "correct" way, and how did it affect the numbers?

# t_pssm_projection_cred_grp
# SQL version PSSM_PROJECTION_CREDENTIAL values are allcaps
#   and has Post-degree Certificate and Post-degree Diploma
# R version PSSM_PROJECTION_CREDENTIAL values are title case
#   and has Post-Degree Certificate and Post-Degree Diploma

# credential_rank - not in the SQL load scripts, so pull in from other schema.

# tbl_age and age_group_lookup
# these were the tables we used last model run.
# The commented ones are others I found that I thought made sense.
# neither version here match the version used in 2019, though. (see the 2017 Near Completers Access DB)
# The sql has been translated correctly so we get the same ratios (R vs SQL),
# but we need to investigate which age group tables are correct to use.
# There is a correction in 04-graduate-projections which adjusts, too.

# tmp_tbl_age:
# SQL version may not load TPID_DATE_OF_BIRTH, COSC_ENRL_END_DATE, COSC_GRAD_CREDENTIAL_DATE
# as date types.
