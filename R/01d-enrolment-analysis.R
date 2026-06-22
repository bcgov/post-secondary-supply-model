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
library(odbc)
library(DBI)
set.seed(123456)

## -------------------------- Configure LAN Paths and DB Connection ------------------------------
## -----------------------------------------------------------------------------------------------
db_config <- config::get("decimal")
my_schema <- config::get("myschema")

con <- dbConnect(
  odbc(),
  Driver = db_config$driver,
  Server = db_config$server,
  Database = db_config$database,
  Trusted_Connection = "True"
)

## --------------------------------------Required Tables------------------------------------------
## -----------------------------------------------------------------------------------------------

age_group_lookup <- dbReadTable(
  con,
  SQL(glue::glue('"{my_schema}"."age_group_lookup_r"'))
)

credential <- dbReadTable(
  con,
  SQL(glue::glue('"{my_schema}"."credential_r"'))
)

stp_enrolment <- dbReadTable(
  con,
  SQL(glue::glue('"{my_schema}"."stp_enrolment_r"'))
)

stp_enrolment_record_type <- dbReadTable(
  con,
  SQL(glue::glue('"{my_schema}"."stp_enrolment_record_type_r"'))
)

## ---------------------------------------Global Variables ---------------------------------------
## -----------------------------------------------------------------------------------------------
na_vals <- c("", " ", "(Unspecified)", NA)

stp_cols <- c(
  "ID",
  "PSI_BIRTHDATE",
  "psi_birthdate_cleaned",
  "PSI_GENDER",
  "PSI_STUDENT_NUMBER",
  "ENCRYPTED_TRUE_PEN",
  "PSI_SCHOOL_YEAR",
  "PSI_CODE",
  "PSI_MIN_START_DATE",
  "PSI_CIP_CODE",
  "PSI_CREDENTIAL_CATEGORY"
)
cred_cols <- c(
  "ENCRYPTED_TRUE_PEN",
  "PSI_CODE",
  "PSI_STUDENT_NUMBER",
  "psi_gender_cleaned"
)

## --------------------------Extract first-time enrolled records----------------------------------
# References:
# qry01a through qry01e (STP Enrolment Analysis), qry_CreateMinEnrolmentView and qry02a-qry02b (except 03 series)
#
# What the code does:
# - Constructs the core 'min_enrolment' dataframe from the STP enrolment data
#   - Keep only those records with RecordStatus == 0, MinEnrolment == 1
#   - Calculates the primary 'AGE_AT_ENROL_DATE' and 'AGE_GROUP_ENROL_DATE' intervals
# BA Notes:
# - Non-essential columns are dropped from STP_Enrolment and Credential tables to speed up processing
# however, this may not be necessary as code has been since added to prune the STP data tables upstream.
# also, do we need the _D columns anymore?
## -----------------------------------------------------------------------------------------------

# create min_enrolment dataframe keeping only valid first enrolment records
min_enrolment <- stp_enrolment |>
  select(all_of(stp_cols)) |>
  inner_join(
    stp_enrolment_record_type |>
      filter(RecordStatus == 0, is_min_enrol == 1) |>
      select(
        ID,
        RecordStatus,
        IS_MIN_ENROL = is_min_enrol,
        IS_FIRST_ENROL = is_first_enrol
      ),
    by = "ID"
  ) |>
  rename_with(toupper)

# clean and format date variables, calculate age at enrolment, and flag first enrolments
min_enrolment <- min_enrolment |>
  mutate(
    PSI_BIRTHDATE_CLEANED_D = if_else(
      PSI_BIRTHDATE_CLEANED %in%
        na_vals |
        PSI_BIRTHDATE_CLEANED == as.Date("1900-01-01"),
      as.Date(NA),
      as.Date(PSI_BIRTHDATE_CLEANED)
    ),
    PSI_MIN_START_DATE_D = if_else(
      PSI_MIN_START_DATE %in% na_vals,
      as.Date(NA),
      as.Date(PSI_MIN_START_DATE)
    ),
    IS_FIRST_ENROLMENT = if_else(IS_FIRST_ENROL == 1, "Yes", NA_character_),
    AGE_AT_ENROL_DATE = NA_real_,
    AGE_GROUP_ENROL_DATE = NA_real_,
    AGE_AT_CENSUS_2016 = NA_real_,
    AGE_GROUP_CENSUS_2016 = NA_real_,
    IS_SKILLS_BASED = NA_integer_
  )

# ---- Create MinEnrolment View ---
# SQL Reference: branch 'main' (line 46)
# Replicates:qry_CreateMinEnrolmentView and qry02a-qry04a2 (missing 03 series)
# What the code does:
#  - Constructs the core 'min_enrolment' dataframe by joining raw STP data with
#  record-type filters and previously initialized supplemental variables.
#  - It also calculates the primary 'AGE_AT_ENROL_DATE' using lubridate
#  intervals and maps age groups via an inequality join.
# BA Notes:
#  - creates min_enrolment (identical db table MinEnrolment)
#  - Column bloat: I’ve retained all 30+ legacy columns to ensure 1:1 parity
#  with the SQL version for QA/Review purposes. Non-essential columns
#  can be dropped in a later 'refine' phase once the logic is validated.
#  - There are some epens with > 1 gender still (in the SQL version) as an original UPDATE query
# appears to not be deterministic; without loss of generality(?) choosing slice_max.
#  - hard to test the SQL version seperatly as min_enrolment is a view to stp_enrolment.  You'd need
# to make sure this script hasn't been run once already which could be tricky or not reasonable.

min_enrolment <- stp_enrolment |>
  select(
    ID,
    PSI_PEN,
    PSI_BIRTHDATE,
    psi_birthdate_cleaned,
    PSI_GENDER,
    PSI_STUDENT_NUMBER,
    PSI_STUDENT_POSTAL_CODE_FIRST_CONTACT,
    TRUE_PEN,
    ENCRYPTED_TRUE_PEN,
    PSI_SCHOOL_YEAR,
    PSI_REGISTRATION_TERM,
    PSI_STUDENT_POSTAL_CODE_CURRENT,
    PSI_INDIGENOUS_STATUS,
    PSI_NEW_STUDENT_FLAG,
    PSI_ENROLMENT_SEQUENCE,
    PSI_CODE,
    PSI_TYPE,
    PSI_FULL_NAME,
    PSI_BASIS_OF_ADMISSION,
    PSI_MIN_START_DATE,
    PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    PSI_PROGRAM_CODE,
    PSI_CIP_CODE,
    PSI_PROGRAM_EFFECTIVE_DATE,
    PSI_FACULTY,
    PSI_CONTINUING_EDUCATION_COURSE_ONLY,
    PSI_CREDENTIAL_CATEGORY,
    PSI_VISA_STATUS,
    PSI_STUDY_LEVEL,
    PSI_ENTRY_STATUS,
    OVERALL_INDIGENOUS_STATUS
  ) |>
  inner_join(
    stp_enrolment_record_type |>
      # filter(RecordStatus == 0, MinEnrolment == 1) |>
      filter(RecordStatus == 0, IS_MIN_ENROL == 1) |> # need attention
      select(ID),
    by = "ID"
  ) |>
  inner_join(
    min_enrolment_sup_var |>
      select(
        ID,
        PSI_BIRTHDATE_CLEANED_D,
        PSI_MIN_START_DATE_D,
        AGE_AT_ENROL_DATE,
        AGE_GROUP_ENROL_DATE,
        AGE_AT_CENSUS_2016,
        AGE_GROUP_CENSUS_2016,
        IS_FIRST_ENROLMENT,
        IS_SKILLS_BASED
      ),
    by = "ID"
  )

min_enrolment <- min_enrolment |>
  mutate(
    AGE_AT_ENROL_DATE = if_else(
      !is.na(PSI_BIRTHDATE_CLEANED_D) & !is.na(PSI_MIN_START_DATE_D),
      floor(interval(PSI_BIRTHDATE_CLEANED_D, PSI_MIN_START_DATE_D) / years(1)),
      NA_real_
    )
  ) |>
  select(-IS_FIRST_ENROL)

# map age groups via an inequality join
min_enrolment <- min_enrolment |>
  left_join(
    age_group_lookup,
    by = join_by(between(AGE_AT_ENROL_DATE, LowerBound, UpperBound))
  ) |>
  mutate(AGE_GROUP_ENROL_DATE = AgeIndex) |>
  select(-AgeIndex, -AgeGroup, -LowerBound, -UpperBound)

## ----------Find gender for distinct non-null EPENs, or non-null PSI_CODE/PSI_NUMBER--------------
# References: qry04a1 through qry04a2
#
# What the code does:
# Identify invalid genders  ("", " ", "(Unspecified)", NA)
# Uses genders from the credential view to impute (backfill) gender into min_enrolment.
# This is accomplished by performing two passes
# - 1) using valid epens, then
# - 2) valid psi code/number combinations for records without valid epens.
#
#
# BA Notes:
# We have 3 quite different methods for imputing invalid genders in the next couple of sections
# We should consider reducing complexity and create a unified approach
# this code mimics SQL; in SQL when an UPDATE...SET returns multiple options, the engine will choose one, usually the first one
# this is unpredictable but at this point we only concerned with generating the same counts as the SQL version.
## -----------------------------------------------------------------------------------------------

# select the first valid gender for each student in credential data - pass #1 valid epens
credential_epen <- credential |>
  select(all_of(cred_cols)) |>
  filter(!ENCRYPTED_TRUE_PEN %in% na_vals, !psi_gender_cleaned %in% na_vals) |>
  select(ENCRYPTED_TRUE_PEN, gender_cred_epen = psi_gender_cleaned) |>
  slice_head(
    by = ENCRYPTED_TRUE_PEN,
    n = 1
  )

# select the first valid gender for each student in credential data - pass #2 invalid epens
credential_no_epen <- credential |>
  select(all_of(cred_cols)) |>
  filter(ENCRYPTED_TRUE_PEN %in% na_vals, !psi_gender_cleaned %in% na_vals) |>
  select(
    PSI_STUDENT_NUMBER,
    PSI_CODE,
    gender_cred_no_epen = psi_gender_cleaned
  ) |>
  slice_head(
    by = c(PSI_STUDENT_NUMBER, PSI_CODE),
    n = 1
  )

# back fill NA genders in min_enrolment
min_enrolment <- min_enrolment |>
  left_join(credential_epen, by = join_by(ENCRYPTED_TRUE_PEN)) |>
  left_join(credential_no_epen, by = join_by(PSI_STUDENT_NUMBER, PSI_CODE)) |>
  mutate(
    gender_cred = coalesce(gender_cred_epen, gender_cred_no_epen)
  ) |>
  mutate(
    PSI_GENDER = case_when(
      is.na(PSI_GENDER) ~ gender_cred,
      is.na(gender_cred) ~ PSI_GENDER,
      TRUE ~ if_else(PSI_GENDER != gender_cred, gender_cred, PSI_GENDER)
    )
  ) |>
  select(-gender_cred_epen, -gender_cred_no_epen, -gender_cred)

## --------------Assign one gender/student and update MinEnrolment table ------------------------
# References: qry04c through qry04e2
#
# What the code does:
# - This code performs a Gender Standardization process based on a student’s earliest recorded data.
# - It is designed to solve the problem of "conflicting records", where the same student might appear
# with different gender labels across different rows in the dataset.
# BA Notes: We use Concatenated_ID instead of EPEN for the next set of queries
# I beleive this would be a useful approach to adopt for many of the other steps.
# - Performs a historic imputation process based on a student’s earliest recorded data in stp_enrolment.
# - Accomplished in one pass by creating a concatenated ID (encrypted true pen where available, and psi code/number where not)
## -----------------------------------------------------------------------------------------------

# select first-time recorded gender from min-enrolment data
first_gender_lookup <- min_enrolment |>
  filter(IS_FIRST_ENROLMENT == "Yes") |>
  mutate(
    CONCATENATED_ID = if_else(
      !ENCRYPTED_TRUE_PEN %in% na_vals,
      ENCRYPTED_TRUE_PEN,
      paste0(PSI_STUDENT_NUMBER, PSI_CODE)
    )
  ) |>
  distinct(CONCATENATED_ID, FIRST_GENDER = PSI_GENDER)

# forward fill NA genders (join on a concatenated ID, instead of 2-passes, epen, no epen)
min_enrolment <- min_enrolment |>
  mutate(
    CONCATENATED_ID = if_else(
      !ENCRYPTED_TRUE_PEN %in% na_vals,
      ENCRYPTED_TRUE_PEN,
      paste0(PSI_STUDENT_NUMBER, PSI_CODE)
    )
  )

first_gender_lookup <- min_enrolment |>
  filter(IS_FIRST_ENROLMENT == "Yes") |>
  distinct(CONCATENATED_ID, FIRST_GENDER = PSI_GENDER)

min_enrolment <- min_enrolment |>
  left_join(first_gender_lookup, by = "CONCATENATED_ID") |>
  mutate(
    PSI_GENDER = coalesce(FIRST_GENDER, PSI_GENDER)
  ) |>
  select(-FIRST_GENDER)

## ---------------------------------impute gender -----------------------------------------------
# References:
# Replicates: qry05a1 through qry06a5 and some R code from lines 101 to 170 (main)
#
# What the code does:
# - Performs a proportional imputation for missing gender data.
# It calculates the distribution of the known population from the set of first enrolment records
#  and applies that same ratio to missing first records.
# Simulataneously performs a historical imputation, where the first seen record is carried forward.
## -----------------------------------------------------------------------------------------------

na_vals <- c("U", "Unknown", "(Unspecified)", "", NA)

# first-time "unknowns"
extract_no_gender_first <- min_enrolment |>
  filter(IS_FIRST_ENROLMENT == "Yes", PSI_GENDER %in% na_vals) |>
  select(ID, ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, PSI_GENDER)

total_unknowns <- nrow(extract_no_gender_first)

# first-time valid genders (Male, Female, Gender Diverse)
gender_weights <- min_enrolment |>
  filter(IS_FIRST_ENROLMENT == "Yes", !PSI_GENDER %in% na_vals) |>
  count(PSI_GENDER) |>
  mutate(PROPORTION = n / sum(n)) |>
  mutate(TARGET_N = round(PROPORTION * total_unknowns))

# resample unknownws
imputed_first_enrolments <- extract_no_gender_first |>
  mutate(
    PSI_GENDER_IMPUTED = sample(
      gender_weights$PSI_GENDER,
      size = n(),
      replace = TRUE,
      prob = gender_weights$PROPORTION
    )
  )

# carry forward first seen gender for records with valid encrypted true pen
extract_no_gender_epen <- min_enrolment |>
  filter(PSI_GENDER %in% na_vals, !ENCRYPTED_TRUE_PEN %in% na_vals) |>
  select(ID, ENCRYPTED_TRUE_PEN) |>
  inner_join(
    imputed_first_enrolments |>
      distinct(ENCRYPTED_TRUE_PEN, PSI_GENDER_IMPUTED),
    by = join_by(ENCRYPTED_TRUE_PEN)
  )

# carry forward first seen gender for records without valid encrypted true pen
extract_no_gender_no_epen <- min_enrolment |>
  filter(PSI_GENDER %in% na_vals, ENCRYPTED_TRUE_PEN %in% na_vals) |>
  filter(!PSI_CODE %in% na_vals, !PSI_STUDENT_NUMBER %in% na_vals) |>
  select(ID, PSI_CODE, PSI_STUDENT_NUMBER) |>
  inner_join(
    imputed_first_enrolments |>
      distinct(PSI_CODE, PSI_STUDENT_NUMBER, PSI_GENDER_IMPUTED),
    by = join_by(PSI_CODE, PSI_STUDENT_NUMBER)
  )

# backfill genders in min_enrolment data
min_enrolment <- min_enrolment |>
  left_join(
    extract_no_gender_epen |> select(ID, PSI_GENDER_IMPUTED),
    by = "ID"
  ) |>
  left_join(
    extract_no_gender_no_epen |> select(ID, PSI_GENDER_IMPUTED),
    by = "ID",
    suffix = c(".pen", ".nopen")
  ) |>
  mutate(
    PSI_GENDER = if_else(PSI_GENDER %in% na_vals, NA_character_, PSI_GENDER),
    PSI_GENDER = coalesce(
      PSI_GENDER,
      PSI_GENDER_IMPUTED.pen,
      PSI_GENDER_IMPUTED.nopen
    )
  ) |>
  select(-PSI_GENDER_IMPUTED.pen, -PSI_GENDER_IMPUTED.nopen)

## --------------------------Create Age and Gender Distrbutions----------------------------------
# AND
## ----------------------Assign age to records with missing age----------------------------------
# Where is the SQL version: Originally sourced from branch 'main' (line 164:281)
# Replicates: qry07a-qry08 and much R code
# What the code does:
# - This code extracts and isolates records where the student's age could not be calculated.
# - Performs a proportional imputation for missing ages.
# - Followed by a temporal (or forward-fill) projection to fill in subsequent records.
# - Updates min_enrolment with imputed ages.
# BA Notes:
# - compare extract_no_age to Extract_No_Age and
# - compare extract_no_age_first_enrol to Extract_No_Age_First_Enrolment
# - distribution of assigned (imputed) ages are similar for this version vs last
## -----------------------------------------------------------------------------------------------

# extract records with missing age at enrolment
extract_no_age <- min_enrolment |>
  filter(is.na(AGE_AT_ENROL_DATE)) |>
  distinct(
    ID,
    ENCRYPTED_TRUE_PEN,
    PSI_STUDENT_NUMBER,
    PSI_CODE,
    AGE_AT_ENROL_DATE,
    PSI_SCHOOL_YEAR,
    PSI_MIN_START_DATE,
    PSI_MIN_START_DATE_D,
    IS_FIRST_ENROLMENT,
    PSI_GENDER
  )

# extract records with missing age at enrolment for first enrolments only (for imputation)
extract_no_age_first_enrol <- min_enrolment |>
  filter(is.na(AGE_AT_ENROL_DATE), IS_FIRST_ENROLMENT == "Yes") |>
  distinct(
    ID,
    ENCRYPTED_TRUE_PEN,
    PSI_STUDENT_NUMBER,
    PSI_GENDER,
    PSI_CODE,
    AGE_AT_ENROL_DATE_first = AGE_AT_ENROL_DATE
  )

# function to impute age by gender
# assumes that wt contains a p for every age
impute_age_by_gender <- function(df, gender_name, wt) {
  # isolate frequency distribution for specified gender
  dist <- wt |> filter(PSI_GENDER == gender_name)

  # if the distribution is empty/missing, use the global age distribution
  if (nrow(dist) == 0) {
    dist <- wt |>
      count(AGE_AT_ENROL_DATE, wt = count) |>
      mutate(p = n / sum(n))
  }

  # sample ages and assign to df
  df$AGE_AT_ENROL_DATE <- sample(
    dist$AGE_AT_ENROL_DATE,
    size = nrow(df),
    replace = TRUE,
    prob = dist$p
  )
  return(df)
}

# calculate natural age by gender distribution from known ages at first enrolment
age_weights <- min_enrolment |>
  filter(!is.na(AGE_AT_ENROL_DATE), IS_FIRST_ENROLMENT == "Yes") |>
  count(PSI_GENDER, AGE_AT_ENROL_DATE, name = "count") |>
  group_by(PSI_GENDER) |>
  mutate(p = count / sum(count)) |>
  ungroup()

# impute ages for records with missing age at first enrolment
# improvement: assumes age_weights includes a p for every age
# but if there are missing ages, this forces sampling with 0 p.
extract_no_age_first_enrol <- extract_no_age_first_enrol |>
  split(~PSI_GENDER) |>
  imap(~ impute_age_by_gender(.x, .y, age_weights)) |>
  list_rbind()

# assign ages to all first enrolment records that are missing ages
extract_no_age <- extract_no_age |>
  select(-AGE_AT_ENROL_DATE) |>
  left_join(
    extract_no_age_first_enrol |>
      distinct(ID, AGE_AT_ENROL_DATE)
  )

# calculate missing ages from first enrolments
calc_ages <- extract_no_age |>
  arrange(PSI_STUDENT_NUMBER, PSI_CODE, PSI_MIN_START_DATE_D) |>
  group_by(PSI_STUDENT_NUMBER, PSI_CODE) |>
  mutate(
    base_date = first(PSI_MIN_START_DATE_D),
    base_age = first(AGE_AT_ENROL_DATE),
    AGE_AT_ENROL_DATE = if_else(
      is.na(AGE_AT_ENROL_DATE) & !is.na(base_age),
      base_age + (year(PSI_MIN_START_DATE_D) - year(base_date)),
      AGE_AT_ENROL_DATE
    )
  ) |>
  ungroup() |>
  select(-base_date, -base_age)

calc_ages <- calc_ages %>% select(ID, AGE_AT_ENROL_DATE)

extract_no_age <- extract_no_age |>
  left_join(
    calc_ages |> rename(AGE_AT_ENROL_DATE_to_update = AGE_AT_ENROL_DATE)
  ) |>
  mutate(
    AGE_AT_ENROL_DATE = coalesce(AGE_AT_ENROL_DATE, AGE_AT_ENROL_DATE_to_update)
  ) |>
  select(-AGE_AT_ENROL_DATE_to_update)

# ---- start manual edits ----
# BA Notes: Some manual updates were made here to remaining missing ages.
# I haven't done the manual fixes as we're getting away from manual work
# ---- end manual edits  ----

# Update Min Enrolment
min_enrolment <- min_enrolment |>
  left_join(
    extract_no_age |>
      distinct(ID, AGE_AT_ENROL_DATE_to_update = AGE_AT_ENROL_DATE)
  ) |>
  mutate(
    AGE_AT_ENROL_DATE = coalesce(AGE_AT_ENROL_DATE, AGE_AT_ENROL_DATE_to_update)
  ) |>
  select(-AGE_AT_ENROL_DATE_to_update)

min_enrolment <- min_enrolment |>
  left_join(
    age_group_lookup |> select(AgeIndex, LowerBound, UpperBound),
    by = join_by(between(AGE_AT_ENROL_DATE, LowerBound, UpperBound))
  ) |>
  mutate(
    AGE_GROUP_ENROL_DATE = AgeIndex
  ) |>
  select(-AgeIndex, -LowerBound, -UpperBound)

## ------------------------------------ Clean Up --------------------------------------------------
# Current workflow:
#  - Write key tables back to sql server.  These are tables needed for downstream work, or tables
# that might be needed for later reference outside of this analysis.
#  - Close DB connections
#  - Remove all objects at the end of each script.
## ------------------------------------------------------------------------------------------------

tryCatch(
  {
    dbWriteTable(
      con,
      Id(schema = my_schema, name = "min_enrolment_r"),
      min_enrolment,
      overwrite = TRUE
    )
  },
  error = function(e) {
    message("Error writing min_enrolment to the database: ", e$message)
    stop("Database write failed. Please check your connection and permissions.")
  }
)


# ---- Final Distributions ----
# This section moved to 01e-stp-distributions

## Review ----
# SQL version starts at line ? on branch main
# Replicates:
# What the code does:
# BA Notes: I get an error in qry09c_:
#   invalid object name 'PSI_CODE_RECODE' is this another table I need to bring in?
# dbExecute(con, qry09c_MinEnrolment_PSI_TYPE)

# ---- Clean Up ----
# SQL version starts at line ? on branch main
# Replicates:
# What the code does:
# BA Notes: refine as needed
tables_to_keep <- c(
  "min_enrolment"
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

dbDisconnect(con)

rm(list = ls())
