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
set.seed(123456)

# ---- Check required Tables etc. ----
# Where is the SQL version: Originally sourced from branch 'main' (line 41)
# What the code does: Ensures all source dataframes
#   and lookup tables are present in global environemnt before processing.
# BA Notes:
# - Required tables, and "tables to keep" at the end of the script, reflect
#  tables used/retained from this script only.  Additional changes will be needed
# if running as a continuous workflow.

required_tables <- c(
  "age_group_lookup",
  "outcome_credential",
  "credential",
  "stp_enrolment",
  "stp_enrolment_record_type"
)

missing <- required_tables[!sapply(required_tables, exists, where = .GlobalEnv)]

if (length(missing) > 0) {
  stop(paste(
    "The following required tables are missing from the environment:",
    paste(missing, collapse = ", ")
  ))
}

# define global variables
na_vals = c("", " ", "(Unspecified)", NA)

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
cred_cols <- c("ENCRYPTED_TRUE_PEN", "PSI_CODE", "PSI_STUDENT_NUMBER", "psi_gender_cleaned")

# ---- Extract first-time enrolled records ----
# Where is the SQL version: Originally sourced from branch 'main' (line 46)
# Replicates: qry01a through qry01e (STP Enrolment Analysis), qry_CreateMinEnrolmentView and qry02a-qry02b (except 03 series)
# What the code does:
# - Constructs the core 'min_enrolment' dataframe by joining raw STP data with
#  record-type filters and previously initialized supplemental variables.
# - Calculates the primary 'AGE_AT_ENROL_DATE' intervals
# - maps age groups via an inequality join.
# BA Notes:
# - Non-essential columns have been dropped from STP_Enrolment and Credential tables to speed up processing



# define min_enrolment dataframe from stp_enrolment and stp_enrolment_record_type
# keeping only valid first enrolment records (RecordStatus == 0, MinEnrolment == 1)
min_enrolment <- stp_enrolment |>
  select(all_of(stp_cols)) |>
  inner_join(
    stp_enrolment_record_type |>
      filter(RecordStatus == 0, MinEnrolment == 1) |>
      select(ID, FirstEnrolment),
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
    IS_FIRST_ENROLMENT = if_else(FIRSTENROLMENT == 1, "Yes", NA_character_),
    AGE_AT_ENROL_DATE = if_else(
      !is.na(PSI_BIRTHDATE_CLEANED_D) & !is.na(PSI_MIN_START_DATE_D),
      floor(interval(PSI_BIRTHDATE_CLEANED_D, PSI_MIN_START_DATE_D) / years(1)),
      NA_real_
    )
  ) |>
  select(-FIRSTENROLMENT)

# map age groups via an inequality join
min_enrolment <- min_enrolment |>
  left_join(
    age_group_lookup,
    by = join_by(between(AGE_AT_ENROL_DATE, LowerBound, UpperBound))
  ) |>
  mutate(AGE_GROUP_ENROL_DATE = AgeIndex) |>
  select(-AgeIndex, -AgeGroup, -LowerBound, -UpperBound)


# ---- Find gender for distinct non-null EPENs, or non-null PSI_CODE/PSI_NUMBER  ----
# Where is the SQL version: Originally sourced from branch 'main' (line 59)
# Replicates: qry04a1 through qry04a2
# What the code does: identifies invalid genders  ("", " ", "(Unspecified)", NA) and 
#   uses genders from the credential view to impute (backfill) gender into min_enrolment.  Accomplished 
#   by performing two passes - 1) using valid epens, then 2) valid psi code/number combinations for records without valid epens.
# BA Notes:
# We have 3 quite different methods for imputing invalid genders in the next couple of sections
# We should consider reducing complexity and create aunified approach

# select the first valid gender for each student in credential data - pass #1 valid epens
# this mimicks SQL - in SQL when an UPDATE...SET returns multiple options, the engine will choose one, usually the first one
# this is unpredictable but at this point we only concerned with generating the same counts as the SQL version.
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
  left_join(credential_epen, by = join_by(ENCRYPTED_TRUE_PEN)) |> # some duplicates being introduced here
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

# ---- Assign one gender/student and update MinEnrolment table ----
# Where is the SQL version: Originally sourced from branch 'main' (line 78)
# Replicates: qry04c through qry04e2
# What the code does:
# - Performs a historic imputation process based on a student’s earliest recorded data in stp_enrolment.
# - Accomplished in one pass by creating a concatenated ID (encrypted true pen where available, and psi code/number where not) 

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
  ) |>
  left_join(first_gender_lookup, by = "CONCATENATED_ID") |>
  mutate(
    PSI_GENDER = coalesce(FIRST_GENDER, PSI_GENDER)
  ) |>
  select(-FIRST_GENDER)

# ---- impute gender  ----
# Where is the SQL version: SQL version starts ~line 101 on branch main
# Replicates: qry05a1 through qry06a5 and some R code from lines 101 to 170 (main)
# What the code does:
# - Performs a proportional imputation for missing gender data.
# It calculates the distribution of the known population from the set of first enrolment records
#  and applies that same ratio to missing first records.
# Simulataneously performs a historical imputation, where the first seen record is carried forward. 

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


# ---- Create Age and Gender Distrbutions ----
# and 
# ----- Assign age to records with missing age -----
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
      base_age +
        (as.POSIXlt(PSI_MIN_START_DATE_D)$year - as.POSIXlt(base_date)$year),
      AGE_AT_ENROL_DATE
    )
  ) |>
  ungroup() |>
  select(-base_date, -base_age)

calc_ages <- calc_ages %>% select(ID, AGE_AT_ENROL_DATE)

# 
extract_no_age <- extract_no_age |>
  left_join(
    calc_ages |> rename(AGE_AT_ENROL_DATE_to_update = AGE_AT_ENROL_DATE)
  ) |>
  mutate(
    AGE_AT_ENROL_DATE = coalesce(AGE_AT_ENROL_DATE, AGE_AT_ENROL_DATE_to_update)
  ) |>
  select(-AGE_AT_ENROL_DATE_to_update)

# ---- some manual edits ----
# BA Notes: Some manual updates were made here to remaining missing ages. 
# I haven't done the manual fixes as we're getting away from manual work

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


# ---- Final Distributions ----
# !! This section moved to 01e-stp-distributions


# ---- Clean Up ----
tables_to_keep <- c(
  "age_group_lookup",
  "min_enrolment"
)

rm(list = setdiff(ls(), tables_to_keep))
