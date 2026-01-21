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

# Workflow #4 (noting here for now)
# Enrolment Analysis
# Description:
# Relies on STP_Enrolment data table, STP_Enrolment_Record_Type, Credential View, AgeGroupLookup
# Creates tables qry09c_MinEnrolment (one of them) to be used for grad projections

library(tidyverse)
set.seed(123456)

# ---- Extract first-time enrolled records ----
# qry01a through qry01e
na_vals = c("", " ", "(Unspecified)")
min_enrolment_sup_var <- stp_enrolment |>
  select(ID, psi_birthdate_cleaned, PSI_MIN_START_DATE) |>
  inner_join(
    stp_enrolment_record_type |>
      select(ID, MinEnrolment, FirstEnrolment, RecordStatus),
    by = "ID"
  ) |>
  rename_with(toupper) |>
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
      as.Date(PSI_MIN_START_DATE),
    ),
    IS_FIRST_ENROLMENT = if_else(FIRSTENROLMENT == 1, "Yes", NA_character_),
    AGE_AT_ENROL_DATE = NA_real_,
    AGE_GROUP_ENROL_DATE = NA_real_,
    AGE_AT_CENSUS_2016 = NA_real_,
    AGE_GROUP_CENSUS_2016 = NA_real_,
    IS_SKILLS_BASED = NA_integer_
  )

# ---- Create MinEnrolment View ---
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
      filter(RecordStatus == 0, MinEnrolment == 1) |>
      select(ID), # We only need the ID to facilitate the filter-join
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

# calculate age at enrolement
min_enrolment <- min_enrolment |>
  # ---- qry02a_UpdateAgeAtEnrol ----
  mutate(
    AGE_AT_ENROL_DATE = if_else(
      !is.na(PSI_BIRTHDATE_CLEANED_D) & !is.na(PSI_MIN_START_DATE_D),
      floor(interval(PSI_BIRTHDATE_CLEANED_D, PSI_MIN_START_DATE_D) / years(1)),
      NA_integer_
    )
  ) |>
  # ---- qry02b_UpdateAGAtEnrol ----
  left_join(
    age_group_lookup,
    by = join_by(between(AGE_AT_ENROL_DATE, LowerBound, UpperBound))
  ) |>
  mutate(AGE_GROUP_ENROL_DATE = AgeIndex) |>
  select(-AgeIndex, -AgeGroup, -LowerBound, -UpperBound)

# assign gender to min enrolement
# Note: there are some epens with > 1 gender still (in the SQL version)
# the original UPDATE query appears to not be deterministic, so choice of gender for
# thes duplicates is arbitrary.
# choosing slice(1)
credential_epen <- credential |>
  filter(!ENCRYPTED_TRUE_PEN %in% na_vals, !psi_gender_cleaned %in% na_vals) |>
  select(ENCRYPTED_TRUE_PEN, gender_cred_epen = psi_gender_cleaned) |>
  slice_max(
    by = ENCRYPTED_TRUE_PEN,
    order_by = gender_cred_epen,
    with_ties = FALSE
  )

# no dups here
credential_no_epen <- credential |>
  filter(ENCRYPTED_TRUE_PEN %in% na_vals, !psi_gender_cleaned %in% na_vals) |>
  select(
    PSI_STUDENT_NUMBER,
    PSI_CODE,
    gender_cred_no_epen = psi_gender_cleaned
  ) |>
  slice_max(
    by = c(PSI_STUDENT_NUMBER, PSI_CODE),
    order_by = gender_cred_no_epen,
    with_ties = FALSE
  )

min_enrolment <- min_enrolment |>
  # ---- qry04a_UpdateMinEnrolment_Gender ----
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
  )

# ---- Assign one gender/student and update MinEnrolment table ----
# Using Concatenated_ID instead of EPEN for the next set of queries.
# I feel like we're going in circles...
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

# Perform a Proportional Imputation for missing gender data.
# calculates the existing ratio of Females, Males, and Gender Diverse students in the "known"
# population and then assigns those same proportions to the "unknown" population for their First Enrollment records.
# essentially random assignment based on population weights to keep
# gender statistics remain consistent with the known distribution
na_vals <- c("U", "Unknown", "(Unspecified)", "", NA)

extract_no_gender_first <- min_enrolment |>
  filter(IS_FIRST_ENROLMENT == "Yes", PSI_GENDER %in% na_vals) |>
  select(ID, ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, PSI_GENDER)

total_unknowns <- nrow(extract_no_gender_first)

gender_weights <- min_enrolment |>
  filter(IS_FIRST_ENROLMENT == "Yes", !PSI_GENDER %in% na_vals) |>
  count(PSI_GENDER) |>
  mutate(PROPORTION = n / sum(n)) |>
  mutate(TARGET_N = round(PROPORTION * total_unknowns))

imputed_first_enrolments <- extract_no_gender_first |>
  sample_frac(size = 1, replace = FALSE) |>
  mutate(
    PSI_GENDER = rep(
      gender_weights$PSI_GENDER,
      times = gender_weights$TARGET_N
    ) |>
      head(total_unknowns) |> # handle
      as.character()
  )

extract_no_gender <- min_enrolment |>
  filter(PSI_GENDER %in% na_vals) |>
  select(ID, ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE) |>
  left_join(
    imputed_first_enrolments |>
      distinct(PSI_GENDER, PSI_STUDENT_NUMBER, PSI_CODE),
    by = join_by(PSI_STUDENT_NUMBER, PSI_CODE)
  )

# at this point SQL does some more proportional updates to obtain a gender for a handful of records, followed by
# further processing to handle multiple EPEN-gender combos.
# however, those records all have valid EPENS so I'm doing a second pass and joining by epen.
# The finals distributions are minimally off, but worth noting.
extract_no_gender <- extract_no_gender |>
  left_join(
    extract_no_gender |>
      filter(PSI_GENDER %in% na_vals) |>
      left_join(
        imputed_first_enrolments |>
          distinct(ENCRYPTED_TRUE_PEN, PSI_GENDER_to_update = PSI_GENDER)
      )
  ) |>
  mutate(PSI_GENDER_to_update = coalesce(PSI_GENDER, PSI_GENDER_to_update)) |>
  select(-PSI_GENDER)

min_enrolment <- min_enrolment |>
  left_join(extract_no_gender |> select(ID, PSI_GENDER_to_update)) |>
  mutate(
    PSI_GENDER = if_else(
      PSI_GENDER %in% na_vals,
      PSI_GENDER_to_update,
      PSI_GENDER
    )
  ) |>
  select(-PSI_GENDER_to_update)


# ---- Create Age and Gender Distrbutions ----
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
# impute based on age and gender distribution

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
extract_no_age_first_enrolment <- extract_no_age_first_enrolment |>
  split(~PSI_GENDER) |>
  imap(~ impute_age_by_gender(.x, .y, age_weights)) |>
  list_rbind()


extract_no_age <- extract_no_age |>
  select(-AGE_AT_ENROL_DATE) |>
  left_join(
    extract_no_age_first_enrolment |>
      distinct(ID = id, AGE_AT_ENROL_DATE)
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
# Some manual updates were made here to remaining missing ages.
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
    # Update the target column and clean up the join helpers
    AGE_GROUP_ENROL_DATE = AgeIndex
  ) |>
  select(-AgeIndex, -LowerBound, -UpperBound)


# ---- Final Distributions ----
dbExecute(con, qry09c_MinEnrolment_by_Credential_and_CIP_Code)
dbExecute(con, qry09c_MinEnrolment_Domestic)
dbExecute(con, qry09c_MinEnrolment)

## Review ----
##I get an error here - invalid object name 'PSI_CODE_RECODE'
# is this another table I need to bring in?
# dbExecute(con, qry09c_MinEnrolment_PSI_TYPE)

# ---- Clean Up ----

# ---- Keep ----
dbExistsTable(
  con,
  SQL(glue::glue('"{my_schema}"."STP_Credential_Record_Type;"'))
)
dbExistsTable(
  con,
  SQL(glue::glue('"{my_schema}"."STP_Enrolment_Record_Type;"'))
)
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Enrolment;"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Enrolment_Valid;"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Credential;"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."MinEnrolment"')))

dbDisconnect(con)
