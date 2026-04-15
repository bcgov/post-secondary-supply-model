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
# SQL version: Originally sourced from branch 'main' (line 41)
# What the code does: Acts as a pre-flight "circuit breaker" to ensure all source dataframes
#   and lookup tables are present in global environemnt before processing. Prevents
#   partial execution errors.
# QA/Review Notes:
# - As more queries from SQL are ported and the process is refined,
#   new tables may be added to the 'required_tables' list below.

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

na_vals = c("", " ", "(Unspecified)", NA)

# ---- Extract first-time enrolled records ----
# SQL Reference:
# - Originally sourced from branch 'main' (line 46)
# Replicates:
# - qry01a through qry01e (STP Enrolment Analysis)
# - Replicates:qry_CreateMinEnrolmentView and qry02a-qry04a2 (missing 03 series)
# What the code does:
#  - Constructs the core 'min_enrolment' dataframe by joining raw STP data with
#  record-type filters and previously initialized supplemental variables.
#  - It also calculates the primary 'AGE_AT_ENROL_DATE' using lubridate
#  intervals and maps age groups via an inequality join.
# QA/Review Notes:
# - qry defn for qry01d1_MinEnrolmentSupVar is missing a ")". qry errors - I fixed manually and reran.
#  - creates min_enrolment (identical db table MinEnrolment)
#  - Column bloat: I’ve retained all 30+ legacy columns to ensure 1:1 parity
#  with the SQL version for QA/Review purposes. Non-essential columns
#  can be dropped in a later 'refine' phase once the logic is validated.
#  - There are some epens with > 1 gender still (in the SQL version) as an original UPDATE query
# appears to not be deterministic; without loss of generality(?) choosing slice_max.

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

min_enrolment <- stp_enrolment |>
  select(all_of(stp_cols)) |>
  inner_join(
    stp_enrolment_record_type |>
      filter(RecordStatus == 0, MinEnrolment == 1) |>
      select(ID, FirstEnrolment),
    by = "ID"
  ) |>
  rename_with(toupper)

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

min_enrolment <- min_enrolment |>
  left_join(
    age_group_lookup,
    by = join_by(between(AGE_AT_ENROL_DATE, LowerBound, UpperBound))
  ) |>
  mutate(AGE_GROUP_ENROL_DATE = AgeIndex) |>
  select(-AgeIndex, -AgeGroup, -LowerBound, -UpperBound)


# ---- Find gender for distinct non-null EPENs, or non-null PSI_CODE/PSI_NUMBER  ----
# uses genders from the credential view to impute gender into min_enrolment in the case of NULLS
# !!! what we have going in the code below is 3 quite different methods for imputing invalid genders.
# # We could reduce complexity and created a more unified approach

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
# SQL version starts at line 62 on branch main
# Replicates: qry04b through qry04e2
# What the code does:
# - This code performs a historic imputation process based on a student’s earliest recorded data . This
# solves the problem of the same student appearing with different gender labels across different rows in the dataset.

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
# SQL version starts ~line 101 on branch main
# Replicates: qry05a1 through qry06a5
# What the code does:
# - Perform a Proportional Imputation for missing gender data.
# Instead of leaving "Unknown" genders as blanks or assigning them all to one category,
# it calculates the "natural" distribution of the known population and applies that same ratio to the missing records.
# the distribution is taken from the set of firt enrolment records, effectivly performing a historical imputation,
# where the first seen record is carried forward. (This was what I think SQL versions were doing - we should relook at this)

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
    PSI_GENDER_IMPUTED = sample(rep(
      gender_weights$PSI_GENDER,
      times = gender_weights$TARGET_N
    ))
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
# SQL version starts at line 163 on branch main
# Replicates: qry07a-qry07b2
# What the code does:
# - This code extracts and isolates records where the student's age could not be calculated.
# - It prepares the data for a second round of imputation (on age) by identifying which students are missing an age.
# BA Notes:
# - compare extract_no_age to Extract_No_Age and
# - compare extract_no_age_first_enrol to Extract_No_Age_First_Enrolment
# R version carries the column PSI_GENDER - needed?
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


# ----- Assign age to records with missing age -----
# SQL version starts at line 169 on branch main
# Replicates: the R code from lines 171 to 263 (main)
# What the code does:
# - Performs a Stratified Proportional Imputation for missing ages.
# - Followed by a Temporal Projection to fill in subsequent records.
# - Updates extract_no_age with imputed ages
# - distribution of assigned (imputed) ages are similar for this version vs last

impute_age_by_gender <- function(sub_df, gender_name, lookup_table) {
  # Look for the distribution for this specific gender
  dist <- lookup_table |> filter(PSI_GENDER == gender_name)

  # Fallback: If gender group is empty/missing, use the global age distribution
  if (nrow(dist) == 0) {
    dist <- lookup_table |>
      count(AGE_AT_ENROL_DATE, wt = count) |>
      mutate(prob = n / sum(n))
  }

  # Assign the sampled ages
  sub_df$AGE_AT_ENROL_DATE <- sample(
    dist$AGE_AT_ENROL_DATE,
    size = nrow(sub_df),
    replace = TRUE,
    prob = dist$prob
  )
  return(sub_df)
}

# 1. Prep the weights once
age_weights <- min_enrolment |>
  filter(!is.na(AGE_AT_ENROL_DATE), IS_FIRST_ENROLMENT == "Yes") |>
  count(PSI_GENDER, AGE_AT_ENROL_DATE, name = "count") |>
  group_by(PSI_GENDER) |>
  mutate(prob = count / sum(count)) |>
  ungroup()

# 2. Run the imputation
# We split by gender, apply the function, and bind the results back together
extract_no_age_first_enrol <- extract_no_age_first_enrol |>
  split(~PSI_GENDER) |>
  imap(~ impute_age_by_gender(.x, .y, age_weights)) |>
  list_rbind()


extract_no_age <- extract_no_age |>
  select(-AGE_AT_ENROL_DATE) |>
  left_join(
    extract_no_age_first_enrol |>
      distinct(ID, AGE_AT_ENROL_DATE)
  )

# calculate missing ages from first enrolments
calc_ages <- extract_no_age |>
  # Arrange to ensure the first record (baseline) is chronologically first
  arrange(PSI_STUDENT_NUMBER, PSI_CODE, PSI_MIN_START_DATE_D) |>
  group_by(PSI_STUDENT_NUMBER, PSI_CODE) |>
  mutate(
    # Get the baseline date and age from the first record in the group
    base_date = first(PSI_MIN_START_DATE_D),
    base_age = first(AGE_AT_ENROL_DATE),

    # Only calculate if the first record has an age (as per your 'if' logic)
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

extract_no_age <- extract_no_age |>
  left_join(
    calc_ages |> rename(AGE_AT_ENROL_DATE_to_update = AGE_AT_ENROL_DATE)
  ) |>
  mutate(
    AGE_AT_ENROL_DATE = coalesce(AGE_AT_ENROL_DATE, AGE_AT_ENROL_DATE_to_update)
  ) |>
  select(-AGE_AT_ENROL_DATE_to_update)

# ---- some manual edits ----
# SQL version starts at line ? on branch main
# Replicates:
# What the code does: Some manual updates were made here to remaining missing ages.
# BA Notes: I haven't done the manual fixes as we're getting away from manual work

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
    # Update the target column and clean up the join helpers
    AGE_GROUP_ENROL_DATE = AgeIndex
  ) |>
  select(-AgeIndex, -LowerBound, -UpperBound)


# ---- Final Distributions ----
# This section moved to 01e-stp-distributions

# Test 1
# sql <- "
# SELECT PSI_SCHOOL_YEAR, PSI_CREDENTIAL_CATEGORY, PSI_CIP_CODE, COUNT(*) AS n
# FROM MinEnrolment
# GROUP BY PSI_SCHOOL_YEAR, PSI_CREDENTIAL_CATEGORY, PSI_CIP_CODE
# ORDER BY PSI_CREDENTIAL_CATEGORY, PSI_CIP_CODE;"
# 
# s1 <- dbGetQuery(con, sql)
# r1 <- min_enrolment |> count(PSI_SCHOOL_YEAR, PSI_CREDENTIAL_CATEGORY, PSI_CIP_CODE) |> arrange(PSI_CREDENTIAL_CATEGORY, PSI_CIP_CODE)
# res <- full_join(s1, r1, by = join_by(PSI_SCHOOL_YEAR, PSI_CREDENTIAL_CATEGORY, PSI_CIP_CODE)
# ks.test(res$n.x, res$n.y)
# plot(res$n.x, res$n.y)

# Test 2
# sql <- "
# SELECT MinEnrolment.PSI_GENDER, AgeGroupLookup.AgeGroup, MinEnrolment.PSI_SCHOOL_YEAR, COUNT(*) AS n
# FROM MinEnrolment
# INNER JOIN AgeGroupLookup
# ON  MinEnrolment.AGE_GROUP_ENROL_DATE = AgeGroupLookup.AgeIndex
# GROUP BY MinEnrolment.PSI_GENDER, AgeGroupLookup.AgeGroup, MinEnrolment.PSI_SCHOOL_YEAR
# ORDER BY MinEnrolment.PSI_GENDER, AgeGroupLookup.AgeGroup, MinEnrolment.PSI_SCHOOL_YEAR;"
# s2 <- dbGetQuery(con, sql)
# r2<- min_enrolment |> inner_join(age_group_lookup, by = c("AGE_GROUP_ENROL_DATE" = "AgeIndex")) |>
#   count(PSI_SCHOOL_YEAR, AgeGroup, PSI_GENDER) |> arrange(PSI_GENDER, AgeGroup, PSI_SCHOOL_YEAR)
# res <- full_join(s2, r2, by = join_by(PSI_GENDER, AgeGroup, PSI_SCHOOL_YEAR))
# ks.test(res$n.x, res$n.y)
# plot(res$n.x, res$n.y)

# ---- Clean Up ----
tables_to_keep <- c(
  "age_group_lookup",
  "min_enrolment"
)

rm(list = setdiff(ls(), tables_to_keep))
