# Copyright 2024 Province of British Columbia
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

# credential_supvars is the only table in the global environment that gets altered by this script.
# toggle to create a convenience backup (development only)
# credential_supvars.bk -> credential_supvars

credential_supvars_gender <- credential_supvars_enrolment |>
  distinct(
    ENCRYPTED_TRUE_PEN,
    PSI_GENDER
  )

# ------------------------------------------------------------------------------------
# MULTIPLE GENDERS
#  - Find all students with multiple recorded genders (in credential_supvars_enrolment).
#  - Choose one (select 1st after arranging by PSI_SCHOOL_YEAR, PSI_ENROLMENT_SEQUENCE)
#  - Map the resolved gender values back to credential_supvars_gender.
#  - Prioritize the corrected value, retain the original entry for all other records.
# ------------------------------------------------------------------------------------
# qry03f Cleaning 1 through 5
supvars_enrol_more_than_1 <- credential_supvars_enrolment |>
  group_by(ENCRYPTED_TRUE_PEN) |>
  filter(
    !is.na(ENCRYPTED_TRUE_PEN),
    !(ENCRYPTED_TRUE_PEN %in% c("", " ", "(Unspecified)"))
  ) |>
  filter(n_distinct(PSI_GENDER) > 1) |>
  slice_max(
    order_by = tibble(PSI_SCHOOL_YEAR, PSI_ENROLMENT_SEQUENCE),
    n = 1,
    with_ties = FALSE
  ) |>
  select(
    ENCRYPTED_TRUE_PEN,
    PSI_GENDER_To_Use = PSI_GENDER
  ) |>
  ungroup()

# qry03f_6 thorough 11 (redundant, removed)

# (qry03f_12, 13, & 14)
credential_supvars_gender <- credential_supvars_gender |>
  left_join(
    supvars_enrol_more_than_1 |>
      select(ENCRYPTED_TRUE_PEN, PSI_GENDER_To_Use),
    by = "ENCRYPTED_TRUE_PEN"
  ) |>
  mutate(
    psi_gender_cleaned_flag = if_else(
      !is.na(PSI_GENDER_To_Use),
      "Yes",
      NA_character_
    ),
    psi_gender_cleaned = coalesce(PSI_GENDER_To_Use, PSI_GENDER)
  ) |>
  select(-PSI_GENDER_To_Use)

rm(supvars_enrol_more_than_1)


# ------------------------------------------------------------------------------------
# UNKNOWNS
#  - Find records with "Unknown" genders remaining in credential_supvars_gender.
#  - Search credential_supvars_gender for another record containing valid gender.
#  - Resolve multiples by choosing the "first" gender (based on alphabetical order).
#  - Map the resolved gender values back to credential_supvars_gender.
#  - Prioritize the corrected value, retain the original entry for all other records.
# ------------------------------------------------------------------------------------
# qry03f 15, 17, 18, 19
gender_recovery_lookup <- credential_supvars_gender |>
  filter(
    psi_gender_cleaned %in% c("U", "Unknown", "Prefer Not To Answer/Unknown")
  ) |>
  inner_join(
    credential_supvars_enrolment |>
      filter(
        !PSI_GENDER %in% c("U", "Unknown", "Prefer Not To Answer/Unknown")
      ) |>
      select(ENCRYPTED_TRUE_PEN, ResolvedGender = PSI_GENDER),
    by = "ENCRYPTED_TRUE_PEN",
    relationship = "many-to-many" # each student can have multiple credentials. Downstream slice_max handles this later.
  ) |>
  filter(
    !is.na(ENCRYPTED_TRUE_PEN),
    !(ENCRYPTED_TRUE_PEN %in% c("", " ", "(Unspecified)"))
  ) |>
  group_by(ENCRYPTED_TRUE_PEN) |>
  slice_max(order_by = ResolvedGender, n = 1, with_ties = FALSE) |>
  ungroup() |>
  select(ENCRYPTED_TRUE_PEN, psi_gender_cleaned_NEW = ResolvedGender)

# qry03f 20 and 21 (redundant, removed)

credential_supvars_gender <- credential_supvars_gender |>
  left_join(gender_recovery_lookup, by = "ENCRYPTED_TRUE_PEN") |>
  mutate(
    psi_gender_cleaned = if_else(
      psi_gender_cleaned %in% c("U", "Unknown", "Prefer Not To Answer/Unknown"),
      psi_gender_cleaned_NEW,
      psi_gender_cleaned
    )
  ) |>
  select(-psi_gender_cleaned_NEW)

credential_supvars <- credential_supvars |>
  left_join(
    credential_supvars_gender |>
      filter(
        !is.na(ENCRYPTED_TRUE_PEN),
        !(ENCRYPTED_TRUE_PEN %in% c("", " ", "(Unspecified)"))
      ) |>
      distinct(ENCRYPTED_TRUE_PEN, psi_gender_cleaned),
    by = "ENCRYPTED_TRUE_PEN",
    relationship = "many-to-many" # each student can have multiple credentials; distinct() handles this downstream.
  )

# ---------------------------------------------------------------------------------------------------------
# NULLS (section needs to be rewritten)
#   - Identify records where the cleaned gender is missing in credential_supvars
#   - Search stp enrolment data to find any recorded gender values based on PSI_STUDENT_NUMBER and PSI_CODE
#   - Multiple resolved genders
#   -   - Identify cases with multiple conflicting genders for the same student/ID
#   -   - Resolve conflicts by selecting the "top" gender in the most recent year from credential_supvars_enrolment
#   - Map the resolved gender values back to the primary dataset, update flags and such, but the code needs to be rewritten.
# ---------------------------------------------------------------------------------------------------------

# qry03f_24 & 25
# create a list of records still missing a gender
credential_supvars_missing <- credential_supvars |>
  filter(is.na(psi_gender_cleaned)) |>
  select(ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, psi_gender_cleaned)

# search stp_enrolment for records that can be used to backfill in the missing genders
credential_supvars_missing_recovered <- credential_supvars_missing |>
  inner_join(
    stp_enrolment |> select(PSI_STUDENT_NUMBER, PSI_CODE, PSI_GENDER),
    by = c("PSI_STUDENT_NUMBER", "PSI_CODE"),
    relationship = "many-to-many"
  ) |>
  distinct()

# qry03f_26 & 27
# isolate records associated with multiple genders per student
missing_recovered_multis <- credential_supvars_missing_recovered |>
  group_by(ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE) |>
  summarise(GenderCount = n_distinct(PSI_GENDER), .groups = "drop") |>
  filter(GenderCount > 1)

# select (resolve) one gender per student for the multi-gender records
missing_recovered_multis_distinct <- missing_recovered_multis |>
  select(ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE) |>
  inner_join(
    credential_supvars_enrolment |>
      select(
        ENCRYPTED_TRUE_PEN,
        PSI_GENDER,
        PSI_SCHOOL_YEAR,
        PSI_ENROLMENT_SEQUENCE
      ),
    by = "ENCRYPTED_TRUE_PEN",
    # each student can have multiple credentials, we want to keep all so we determing the max enrolment downstream
    relationship = "many-to-many"
  ) |>
  group_by(ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, PSI_GENDER) |>
  summarise(
    MAX_PSI_SCHOOL_YEAR = max(PSI_SCHOOL_YEAR, na.rm = TRUE),
    MAX_PSI_ENROLMENT_SEQUENCE = max(PSI_ENROLMENT_SEQUENCE, na.rm = TRUE),
    .groups = "drop"
  ) |>
  group_by(ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE) |>
  slice_max(
    order_by = tibble(MAX_PSI_SCHOOL_YEAR, MAX_PSI_ENROLMENT_SEQUENCE),
    n = 1,
    with_ties = FALSE
  ) |>
  ungroup() |>
  rename(ResolvedGender = PSI_GENDER)


# qry03f_28, 32, 33, & 34
# assign the resolved gender to the multi-gender records
credential_supvars_missing_recovered <- credential_supvars_missing_recovered |>
  left_join(
    missing_recovered_multis_distinct |>
      select(PSI_STUDENT_NUMBER, PSI_CODE, ResolvedGender),
    by = c("PSI_STUDENT_NUMBER", "PSI_CODE")
  ) |>
  mutate(
    PSI_GENDER_CLEANED_FLAG = if_else(
      PSI_GENDER %in%
        c("U", "Unknown", "(Unspecified)", "Prefer Not To Answer/Unknown"),
      "Yes",
      NA_character_
    ),
    PSI_GENDER_CLEANED_FLAG = if_else(
      !is.na(ResolvedGender),
      "Yes",
      PSI_GENDER_CLEANED_FLAG
    ),
    psi_gender_cleaned = coalesce(ResolvedGender, psi_gender_cleaned),
    psi_gender_cleaned = if_else(
      is.na(PSI_GENDER_CLEANED_FLAG),
      PSI_GENDER,
      psi_gender_cleaned
    ),
    PSI_GENDER_CLEANED_FLAG = if_else(
      is.na(PSI_GENDER_CLEANED_FLAG),
      "Yes",
      PSI_GENDER_CLEANED_FLAG
    )
  ) |>
  select(-ResolvedGender)

# qry03f_35
# backfill missing genders in credential_supvars
credential_supvars <- credential_supvars |>
  left_join(
    credential_supvars_missing_recovered |>
      filter(PSI_GENDER_CLEANED_FLAG == "Yes") |>
      distinct(PSI_STUDENT_NUMBER, PSI_CODE, psi_gender_cleaned),
    by = c("PSI_STUDENT_NUMBER", "PSI_CODE")
  ) |>
  mutate(
    psi_gender_cleaned = if_else(
      is.na(psi_gender_cleaned.x),
      psi_gender_cleaned.y,
      psi_gender_cleaned.x
    )
  ) |>
  select(-psi_gender_cleaned.x, -psi_gender_cleaned.y)

rm(
  missing_recovered_multis_distinct,
  credential_supvars_missing_recovered,
  credential_supvars_missing,
  missing_recovered_multis,
  credential_supvars_gender,
  gender_recovery_lookup
)
gc()
