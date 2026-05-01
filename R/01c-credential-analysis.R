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

library(tidyverse)
library(odbc)
library(DBI)

# ---- Configure LAN Paths and DB Connection -----
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

## These should be in the R environment already.  If not, toggle.
stp_enrolment <- dbReadTable(
  con,
  SQL(glue::glue('"{my_schema}"."STP_Enrolment"'))
)
stp_credential <- dbReadTable(
  con,
  SQL(glue::glue('"{my_schema}"."STP_Credential"'))
)

stp_enrolment_record_type <- dbReadTable(
  con,
  SQL(glue::glue('"{my_schema}"."STP_Enrolment_Record_Type"'))
)
stp_credential_record_type <- dbReadTable(
  con,
  SQL(glue::glue('"{my_schema}"."STP_Credential_Record_Type"'))
)
stp_enrolment_valid <- dbReadTable(
  con,
  SQL(glue::glue('"{my_schema}"."STP_Enrolment_Valid"'))
)

# Define lookup tables
outcome_credential <- tibble(
  PSI_CREDENTIAL_CATEGORY = str_to_title(c(
    "ADVANCED CERTIFICATE",
    "ADVANCED DIPLOMA",
    "APPRENTICESHIP",
    "ASSOCIATE DEGREE",
    "BACHELORS DEGREE",
    "CERTIFICATE",
    "DIPLOMA",
    "DOCTORATE",
    "FIRST PROFESSIONAL DEGREE",
    "GRADUATE CERTIFICATE",
    "GRADUATE DIPLOMA",
    "MASTERS DEGREE",
    "POST-DEGREE CERTIFICATE",
    "POST-DEGREE DIPLOMA"
  )),
  Outcomes_Cred = c(
    "DACSO",
    "DACSO",
    "APPSO",
    "DACSO",
    "BGS",
    "DACSO",
    "DACSO",
    "GRAD",
    "BGS",
    "GRAD",
    "GRAD",
    "GRAD",
    "DACSO",
    "DACSO"
  )
)

credential_rank <- tibble::tibble(
  PSI_CREDENTIAL_CATEGORY = str_to_title(c(
    "ADVANCED CERTIFICATE",
    "ADVANCED DIPLOMA",
    "APPRENTICESHIP",
    "ASSOCIATE DEGREE",
    "BACHELORS DEGREE",
    "CERTIFICATE",
    "DIPLOMA",
    "DOCTORATE",
    "FIRST PROFESSIONAL DEGREE",
    "GRADUATE CERTIFICATE",
    "GRADUATE DIPLOMA",
    "MASTERS DEGREE",
    "POST-DEGREE CERTIFICATE",
    "POST-DEGREE DIPLOMA"
  )),
  RANK = c(10, 9, 14, 11, 8, 13, 12, 1, 7, 4, 3, 2, 6, 5)
)

age_group_lookup <- data.frame(
  AgeIndex = 1:9,
  AgeGroup = c(
    "15 to 16",
    "17 to 19",
    "20 to 24",
    "25 to 29",
    "30 to 34",
    "35 to 44",
    "45 to 54",
    "55 to 64",
    "65 to 89"
  ),
  LowerBound = c(15, 17, 20, 25, 30, 35, 45, 55, 65),
  UpperBound = c(16, 19, 24, 29, 34, 44, 54, 64, 89),
  stringsAsFactors = FALSE
)

# ---- Create a view with STP_Credential data with record_type == 0 and a non-blank award date ----
# qry00
credential <- stp_credential |>
  inner_join(
    stp_credential_record_type |> select(ID, RecordStatus),
    by = "ID"
  ) |>
  filter(
    RecordStatus == 0,
    !is.na(CREDENTIAL_AWARD_DATE),
    !CREDENTIAL_AWARD_DATE %in% c("", " ", "(Unspecified)")
  ) |>
  select(
    ID,
    ENCRYPTED_TRUE_PEN,
    PSI_SCHOOL_YEAR,
    PSI_STUDENT_NUMBER,
    PSI_CODE,
    CREDENTIAL_AWARD_DATE,
    PSI_PROGRAM_CODE,
    PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    PSI_CREDENTIAL_CIP,
    PSI_CREDENTIAL_LEVEL,
    PSI_CREDENTIAL_CATEGORY,
    RecordStatus
  )

# -------------------------------------------------------------------------------------------------
# ----------------------------- Make Credential Sup Vars Enrolment Table --------------------------------
## reference: source("./sql/01-credential-analysis/"credential-sup-vars-from-enrolment.R")
# qry01 to qry05
latest_enrolment_epen <- stp_enrolment_valid |>
  filter(
    !is.na(ENCRYPTED_TRUE_PEN),
    !ENCRYPTED_TRUE_PEN %in% c("", " ", "(Unspecified)")
  ) |>
  group_by(ENCRYPTED_TRUE_PEN) |>
  filter(PSI_SCHOOL_YEAR == max(PSI_SCHOOL_YEAR, na.rm = TRUE)) |>
  ungroup() |>
  distinct(
    ID,
    ENCRYPTED_TRUE_PEN,
    PSI_STUDENT_NUMBER,
    PSI_CODE,
    PSI_SCHOOL_YEAR,
    PSI_STUDENT_POSTAL_CODE_CURRENT,
    PSI_ENROLMENT_SEQUENCE,
    PSI_MIN_START_DATE
  )

cred_supvars_enrol_epen <- latest_enrolment_epen |>
  inner_join(
    credential |> select(ENCRYPTED_TRUE_PEN, RecordStatus),
    by = c("ENCRYPTED_TRUE_PEN"),
    relationship = "many-to-many"
  ) |>
  select(
    EnrolmentID = ID,
    ENCRYPTED_TRUE_PEN,
    PSI_MIN_START_DATE,
    CredentialRecordStatus = RecordStatus,
    PSI_STUDENT_POSTAL_CODE_CURRENT,
    PSI_SCHOOL_YEAR = PSI_SCHOOL_YEAR,
    PSI_CODE = PSI_CODE,
    PSI_STUDENT_NUMBER = PSI_STUDENT_NUMBER,
    PSI_ENROLMENT_SEQUENCE
  ) |>
  distinct()


# Match via PSI_CODE/Student Number to recover records missed by PEN join.
# Misses may also occur due to temporal mismatches or students lacking "Valid" enrolment status.
#qry06 to qry12
latest_enrolment_no_epen <- stp_enrolment_valid |>
  filter(
    is.na(ENCRYPTED_TRUE_PEN) |
      ENCRYPTED_TRUE_PEN %in% c("", " ", "(Unspecified)")
  ) |>
  group_by(PSI_CODE, PSI_STUDENT_NUMBER) |>
  filter(PSI_SCHOOL_YEAR == max(PSI_SCHOOL_YEAR, na.rm = TRUE)) |>
  ungroup() |>
  distinct(
    ID,
    ENCRYPTED_TRUE_PEN,
    PSI_STUDENT_NUMBER,
    PSI_CODE,
    PSI_SCHOOL_YEAR,
    PSI_STUDENT_POSTAL_CODE_CURRENT,
    PSI_ENROLMENT_SEQUENCE,
    PSI_MIN_START_DATE
  )

cred_supvars_enrol_no_pen <- latest_enrolment_no_epen |>
  inner_join(
    credential |> select(PSI_CODE, PSI_STUDENT_NUMBER, RecordStatus),
    by = c("PSI_CODE", "PSI_STUDENT_NUMBER"),
    relationship = "many-to-many"
  ) |>
  select(
    EnrolmentID = ID,
    ENCRYPTED_TRUE_PEN = ENCRYPTED_TRUE_PEN,
    PSI_MIN_START_DATE,
    CredentialRecordStatus = RecordStatus,
    PSI_STUDENT_POSTAL_CODE_CURRENT,
    PSI_SCHOOL_YEAR = PSI_SCHOOL_YEAR,
    PSI_CODE = PSI_CODE,
    PSI_STUDENT_NUMBER = PSI_STUDENT_NUMBER,
    PSI_ENROLMENT_SEQUENCE
  ) |>
  distinct()

credential_supvars_enrolment <- rbind(
  cred_supvars_enrol_epen,
  cred_supvars_enrol_no_pen
) |>
  distinct()

# -------------------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------------------
# ---- Make Credential Sup Vars Table ----
# this gets added to later
credential_supvars <- credential |>
  select(
    ID,
    ENCRYPTED_TRUE_PEN,
    PSI_STUDENT_NUMBER,
    PSI_CODE,
    PSI_SCHOOL_YEAR,
    CREDENTIAL_AWARD_DATE,
    CredentialRecordStatus = RecordStatus, # Renaming 'RecordStatus' as in SQL
    PSI_PROGRAM_CODE,
    PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    PSI_CREDENTIAL_CIP,
    PSI_CREDENTIAL_LEVEL,
    PSI_CREDENTIAL_CATEGORY
  ) |>
  mutate(CREDENTIAL_AWARD_DATE_D = as.Date(CREDENTIAL_AWARD_DATE)) # create date format of award date

credential_supvars_enrolment <- credential_supvars_enrolment |>
  left_join(
    stp_enrolment |>
      select(
        ID,
        psi_birthdate_cleaned,
        PSI_VISA_STATUS,
        PSI_BIRTHDATE,
        PSI_PROGRAM_CODE,
        PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
        PSI_CIP_CODE,
        PSI_CONTINUING_EDUCATION_COURSE_ONLY,
        PSI_GENDER
      ),
    by = c("EnrolmentID" = "ID")
  ) |>
  mutate(
    psi_birthdate_cleaned = if_else(
      psi_birthdate_cleaned == "1900-01-01",
      NA,
      psi_birthdate_cleaned
    )
  )

# slight correction needed to align with SQL
credential_supvars_enrolment <- credential_supvars_enrolment |>
  mutate(
    psi_birthdate_cleaned = if_else(
      psi_birthdate_cleaned == "",
      NA_character_,
      psi_birthdate_cleaned
    )
  )


# ---- 02 Developmental Records ----
# add a drop credential flag, presumably for later use
stp_credential_record_type <-
  stp_credential_record_type |>
  left_join(
    (credential |>
      filter(
        PSI_CREDENTIAL_CATEGORY %in%
          c(
            "Developmental Credential",
            "Other",
            "None",
            "Short Certificate"
          )
      ) |>
      mutate(DropCredCategory = "Yes") |>
      select(ID, DropCredCategory)),
    by = "ID"
  )


# ---- 03 Miscellaneous ----
stp_credential_record_type <-
  credential_supvars |>
  filter(CREDENTIAL_AWARD_DATE >= "2023-09-01") |>
  select(ID) |>
  mutate(DropPartialYear = "Yes") |>
  right_join(stp_credential_record_type, by = "ID")

rm(
  latest_enrolment_no_epen,
  latest_enrolment_epen,
  cred_supvars_enrol_no_pen,
  cred_supvars_enrol_epen
)
gc()

# ---- 03 Gender Cleaning ----
# !! Commenting out this entire section, instead run the gender_cleaning.r script
# !! That gender_cleaning.r script reproduces the SQL counts, with the exception of a very few Unknowns.
# let's not push the script to gh yet, clean it up first.

na_vals <- c("U", "Unknown", "(Unspecified)", "", " ", NA_character_)

source("R/gender_cleaning.r")

# Performs a targeted data recovery for missing gender information.
# Essentially, identify students with missing gender values and
# attempt to "backfill" them using their most recent valid record from a lookup table.
# There are two passes:
#     1. looks for most recent gender record from credential_supvars_enrolment
#     2. looks for most recent gender record from stp_enrolment
# within each pass, the data is placed into buckets based on whether ENCRYPTED_TRUE_PEN is available or not
# note: the "not" bucket hasn"t been implemented yet for pass 1, in order to keep in alignment with the SQL environment
# also, this strategy assumes that the most recent record is the most accurate, which may not always be the case.
# we could use a more sophisticated approach if needed, such as considering multiple records or averaging values.

# First pass: find genders from credential_supvars_enrolment
#missing_gender <- credential_supvars |>
#  distinct(
#    ENCRYPTED_TRUE_PEN,
#    PSI_STUDENT_NUMBER,
#    PSI_CODE
#  )
#
#src_gender_lookup <- credential_supvars_enrolment |>
#  filter(!PSI_GENDER %in% na_vals) |>
#  distinct(
#    ENCRYPTED_TRUE_PEN,
#    PSI_STUDENT_NUMBER,
#    PSI_CODE,
#    PSI_GENDER,
#    PSI_ENROLMENT_SEQUENCE,
#    PSI_SCHOOL_YEAR
#  )
#
## implementing only the EPEN bucket for now
#epen_missing_gender <- missing_gender |>
#  filter(!ENCRYPTED_TRUE_PEN %in% na_vals) |>
#  inner_join(
#    src_gender_lookup |>
#      filter(!ENCRYPTED_TRUE_PEN %in% na_vals),
#    by = c("ENCRYPTED_TRUE_PEN")
#  ) |>
#  group_by(ENCRYPTED_TRUE_PEN) |>
#  arrange(desc(PSI_SCHOOL_YEAR), desc(PSI_ENROLMENT_SEQUENCE)) |>
#  slice(1) |>
#  ungroup() |>
#  select(ENCRYPTED_TRUE_PEN, psi_gender_cleaned = PSI_GENDER) |>
#  distinct()
#
## add recovered genders to credential_supvars
#credential_supvars <- credential_supvars |>
#  left_join(
#    epen_missing_gender |>
#      select(
#        ENCRYPTED_TRUE_PEN,
#        psi_gender_cleaned
#      ),
#    suffix = c("", "_y"),
#    # Safety distinct to ensure no row duplication (similar to group_by..slice(1))
#    distinct(ENCRYPTED_TRUE_PEN, .keep_all = TRUE),
#    by = "ENCRYPTED_TRUE_PEN"
#  )
#
## Second pass: find genders from stp_enrolment for those still missing
#still_missing_gender <- credential_supvars |>
#  filter(psi_gender_cleaned %in% na_vals) |> # Select initial subset of columns
#  distinct(
#    ENCRYPTED_TRUE_PEN,
#    PSI_STUDENT_NUMBER,
#    PSI_CODE
#  )
#
#src_gender_lookup <- stp_enrolment |>
#  filter(!PSI_GENDER %in% na_vals) |>
#  distinct(
#    ENCRYPTED_TRUE_PEN,
#    PSI_STUDENT_NUMBER,
#    PSI_CODE,
#    PSI_GENDER,
#    PSI_ENROLMENT_SEQUENCE,
#    PSI_SCHOOL_YEAR
#  )
#
## implementing both buckets this time
## 1. EPEN bucket
#epen_still_missing_gender <- still_missing_gender |>
#  filter(!ENCRYPTED_TRUE_PEN %in% na_vals) |>
#  inner_join(
#    src_gender_lookup |>
#      filter(!ENCRYPTED_TRUE_PEN %in% na_vals),
#    by = c("ENCRYPTED_TRUE_PEN")
#  ) |>
#  group_by(ENCRYPTED_TRUE_PEN) |>
#  arrange(desc(PSI_SCHOOL_YEAR), desc(PSI_ENROLMENT_SEQUENCE)) |>
#  slice(1) |>
#  ungroup() |>
#  select(ENCRYPTED_TRUE_PEN, GENDER_FROM_STP_ENROLMENT = PSI_GENDER) |>
#  distinct()
#
## 2. No EPEN bucket
#no_epen_still_missing_gender <- still_missing_gender |>
#  filter(ENCRYPTED_TRUE_PEN %in% na_vals) |>
#  inner_join(
#    src_gender_lookup |>
#      filter(ENCRYPTED_TRUE_PEN %in% na_vals),
#    by = c("PSI_STUDENT_NUMBER", "PSI_CODE")
#  ) |>
#  group_by(PSI_STUDENT_NUMBER, PSI_CODE) |>
#  arrange(desc(PSI_SCHOOL_YEAR), desc(PSI_ENROLMENT_SEQUENCE)) |>
#  slice(1) |>
#  ungroup() |>
#  select(
#    PSI_STUDENT_NUMBER,
#    PSI_CODE,
#    GENDER_FROM_STP_ENROLMENT = PSI_GENDER
#  ) |>
#  distinct()
#
## add recovered genders back to credential_supvars
#credential_supvars <- credential_supvars |>
#  left_join(
#    epen_still_missing_gender,
#    by = "ENCRYPTED_TRUE_PEN"
#  ) |>
#  left_join(
#    no_epen_still_missing_gender,
#    by = c("PSI_STUDENT_NUMBER", "PSI_CODE")
#  ) |>
#  mutate(
#    psi_gender_cleaned = coalesce(
#      # use lower case to align with SQL versions
#      psi_gender_cleaned,
#      GENDER_FROM_STP_ENROLMENT.x,
#      GENDER_FROM_STP_ENROLMENT.y
#    )
#  ) |>
#  select(-GENDER_FROM_STP_ENROLMENT.x, -GENDER_FROM_STP_ENROLMENT.y)

# ---- 04 Birthdate cleaning (last seen birthdate) ----
# note: check if LAST_SEEN_BIRTHDATE can be included when supvars tables are created (at the top of script)
# note: a handful out but I think the R version is handling the coalesce correctly.
# we're only adding LAST_SEEN_BIRTHDATE for valid epens. Can we add for invalid EPENS?
na_vals <- c("", " ", NA_character_, NA, "(Unspecified)")

credential_supvars_birthdate_clean <- credential_supvars_enrolment |>
  select(
    ENCRYPTED_TRUE_PEN,
    psi_birthdate_cleaned,
    PSI_STUDENT_NUMBER,
    PSI_CODE
  ) |>
  distinct() |>
  mutate(
    # we should handle NA transformation when loading into R.
    psi_birthdate_cleaned = if_else(
      psi_birthdate_cleaned %in% na_vals,
      NA_character_,
      psi_birthdate_cleaned
    )
  ) |>
  mutate(
    psi_birthdate_cleaned_D = as.Date(psi_birthdate_cleaned) # we should be able to just cast this in the beginnning
  )

credential_supvars <- credential_supvars |>
  left_join(
    credential_supvars_birthdate_clean |>
      filter(!ENCRYPTED_TRUE_PEN %in% na_vals) |>
      distinct(
        ENCRYPTED_TRUE_PEN,
        bd_pen = psi_birthdate_cleaned,
        bd_pen_d = psi_birthdate_cleaned_D
      ),
    by = c("ENCRYPTED_TRUE_PEN")
  ) |>
  left_join(
    credential_supvars_birthdate_clean |>
      filter(ENCRYPTED_TRUE_PEN %in% na_vals) |>
      distinct(
        PSI_STUDENT_NUMBER,
        PSI_CODE,
        bd_stu = psi_birthdate_cleaned,
        bd_stu_d = psi_birthdate_cleaned_D
      ),
    by = c("PSI_STUDENT_NUMBER", "PSI_CODE")
  )

credential_supvars <- credential_supvars |>
  mutate(
    psi_birthdate_cleaned = coalesce(bd_pen, bd_stu),
    psi_birthdate_cleaned_D = coalesce(bd_pen_d, bd_stu_d),
  ) |>
  select(-bd_pen, -bd_pen_d, -bd_stu, -bd_stu_d)


credential_supvars_enrolment <- credential_supvars_enrolment |>
  left_join(
    stp_enrolment |> select(ID, LAST_SEEN_BIRTHDATE),
    by = c("EnrolmentID" = "ID")
  )

credential_supvars <- credential_supvars |>
  left_join(
    credential_supvars_enrolment |>
      distinct(ENCRYPTED_TRUE_PEN, LAST_SEEN_BIRTHDATE),
    by = "ENCRYPTED_TRUE_PEN"
  )

credential_supvars <- credential_supvars |>
  mutate(
    psi_birthdate_cleaned_D = coalesce(
      psi_birthdate_cleaned_D,
      as.Date(LAST_SEEN_BIRTHDATE)
    )
  )

credential <- stp_credential |>
  select(
    ID,
    ENCRYPTED_TRUE_PEN,
    PSI_STUDENT_NUMBER,
    PSI_CODE,
    PSI_FULL_NAME,
    PSI_SCHOOL_YEAR,
    PSI_PROGRAM_CODE,
    PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    PSI_CREDENTIAL_CATEGORY,
    PSI_CREDENTIAL_LEVEL,
    PSI_CREDENTIAL_CIP,
    CREDENTIAL_AWARD_DATE
  ) |>
  inner_join(
    credential_supvars |>
      select(
        ID,
        CREDENTIAL_AWARD_DATE_D,
        psi_birthdate_cleaned,
        psi_birthdate_cleaned_D,
        psi_gender_cleaned
      ),
    by = "ID"
  ) |>
  inner_join(
    stp_credential_record_type |>
      select(ID, RecordStatus, DropCredCategory, DropPartialYear),
    by = "ID"
  ) |>
  filter(
    RecordStatus == 0,
    is.na(DropCredCategory),
    is.na(DropPartialYear)
  )

# ---- 05 Age and Credential  ----
credential <- credential |>
  mutate(
    AGE_AT_GRAD = as.integer(floor(
      interval(psi_birthdate_cleaned_D, CREDENTIAL_AWARD_DATE_D) / years(1)
    ))
  ) |>
  left_join(
    age_group_lookup,
    by = join_by(between(AGE_AT_GRAD, LowerBound, UpperBound))
  ) |>
  mutate(AGE_GROUP_AT_GRAD = AgeIndex) |>
  select(-any_of(names(age_group_lookup)))

# calculate credential school year based on award date
credential <- credential |>
  mutate(
    cred_month = lubridate::month(CREDENTIAL_AWARD_DATE_D),
    cred_year = lubridate::year(CREDENTIAL_AWARD_DATE_D)
  ) |>
  mutate(
    cred_year_start = if_else(cred_month < 9, cred_year - 1, cred_year),
    cred_year_end = if_else(cred_month < 9, cred_year, cred_year + 1)
  ) |>
  mutate(
    PSI_AWARD_SCHOOL_YEAR = paste0(
      cred_year_start,
      "/",
      as.character(cred_year_end)
    )
  ) |>
  select(-cred_month, -cred_year, -cred_year_start, -cred_year_end)

valid_genders <- c("Female", "Male", "Gender Diverse")

# pull more genders from the stp_enrolment table to fill in gaps
credential <- credential |>
  left_join(
    stp_enrolment |>
      filter(PSI_GENDER %in% valid_genders) |>
      distinct(
        ENCRYPTED_TRUE_PEN,
        PSI_STUDENT_NUMBER,
        PSI_CODE,
        PSI_GENDER_FROM_ENROLMENT = PSI_GENDER
      ),
    by = c("ENCRYPTED_TRUE_PEN", "PSI_STUDENT_NUMBER", "PSI_CODE"),
    relationship = "many-to-many"
  ) |>
  mutate(
    psi_gender_cleaned = coalesce(psi_gender_cleaned, PSI_GENDER_FROM_ENROLMENT)
  ) |>
  select(-PSI_GENDER_FROM_ENROLMENT)


# --- make non dup table ----
credential_non_dup <- credential |>
  group_by(
    ENCRYPTED_TRUE_PEN,
    PSI_CODE,
    PSI_PROGRAM_CODE,
    PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    PSI_CREDENTIAL_CIP,
    PSI_CREDENTIAL_LEVEL,
    PSI_CREDENTIAL_CATEGORY,
    CREDENTIAL_AWARD_DATE_D
  ) |>
  slice_max(ID, n = 1, with_ties = FALSE) |>
  ungroup() |>
  select(-DropCredCategory, -DropPartialYear, -PSI_FULL_NAME)

credential_non_dup <- credential_non_dup |>
  group_by(ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE) |>
  arrange(CREDENTIAL_AWARD_DATE_D, .by_group = TRUE) |>
  mutate(psi_gender_cleaned = last(psi_gender_cleaned)) |>
  ungroup()


# ---- Impute Missing Gender ----
# This procedure performs proportional stochastic imputation to fill in missing gender data.
# It calculates the existing gender distribution for each credential category and then
# uses those ratios as weights to "flip a coin" for every empty record
# ensuring the final dataset maintains the same statistical balance as the known population.

# 1. Create the probability weights per category
gender_weights <- credential_non_dup |>
  filter(!is.na(psi_gender_cleaned)) |>
  count(PSI_CREDENTIAL_CATEGORY, psi_gender_cleaned) |>
  group_by(PSI_CREDENTIAL_CATEGORY) |>
  mutate(prob = n / sum(n)) |>
  summarise(
    genders = list(psi_gender_cleaned),
    weights = list(prob),
    .groups = "drop"
  )

# 2. Apply the weighted coin flip
set.seed(42)
credential_non_dup <- credential_non_dup |>
  left_join(gender_weights, by = "PSI_CREDENTIAL_CATEGORY") |>
  mutate(
    psi_gender_cleaned = case_when(
      !is.na(psi_gender_cleaned) ~ psi_gender_cleaned,
      TRUE ~ as.character(map2(
        genders,
        weights,
        ~ sample(.x, size = 1, prob = .y)
      ))
    )
  ) |>
  select(-genders, -weights)

rm(
  cred_supvars_enrol_epen,
  cred_supvars_enrol_no_pen,
  credential_supvars_birthdate_clean,
  epen_missing_gender,
  epen_still_missing_gender,
  gender_weights,
  latest_enrolment_epen,
  latest_enrolment_no_epen,
  missing_gender,
  no_epen_still_missing_gender,
  src_gender_lookup,
  still_missing_gender
)


# ---- 08 Credential Ranking ----
# The R version produces similar results to SQL.  Some differences noted
# in how SQL and R handle tie-breaking leads to different row flags (HIGHEST_CRED_BY_DATE/RANK)
# for a handful of records, in almost all cases this is because two credentials of same rank are assigned on the same date
# This will hopefully have minimal impact on overall results.

base_data <- credential_non_dup |>
  left_join(credential_rank, by = c("PSI_CREDENTIAL_CATEGORY"))

pen_group <- base_data |>
  select(
    ID,
    ENCRYPTED_TRUE_PEN,
    PSI_STUDENT_NUMBER,
    PSI_CODE,
    CREDENTIAL_AWARD_DATE_D,
    RANK
  ) |>
  filter(!ENCRYPTED_TRUE_PEN %in% na_vals) |>
  group_by(ENCRYPTED_TRUE_PEN) |>
  arrange(desc(CREDENTIAL_AWARD_DATE_D), RANK) |>
  mutate(
    HIGHEST_CRED_BY_DATE = if_else(row_number() == 1, "Yes", NA_character_)
  ) |>
  arrange(RANK, desc(CREDENTIAL_AWARD_DATE_D)) |>
  mutate(
    HIGHEST_CRED_BY_RANK = if_else(row_number() == 1, "Yes", NA_character_)
  ) |>
  ungroup()

stud_num_group <- base_data |>
  select(
    ID,
    ENCRYPTED_TRUE_PEN,
    PSI_STUDENT_NUMBER,
    PSI_CODE,
    CREDENTIAL_AWARD_DATE_D,
    RANK
  ) |>
  filter(ENCRYPTED_TRUE_PEN %in% na_vals) |>
  group_by(PSI_CODE, PSI_STUDENT_NUMBER) |>
  arrange(desc(CREDENTIAL_AWARD_DATE_D), RANK) |>
  mutate(
    HIGHEST_CRED_BY_DATE = if_else(row_number() == 1, "Yes", NA_character_)
  ) |>
  arrange(RANK, desc(CREDENTIAL_AWARD_DATE_D)) |>
  mutate(
    HIGHEST_CRED_BY_RANK = if_else(row_number() == 1, "Yes", NA_character_)
  ) |>
  ungroup()

credential_ranking <- bind_rows(pen_group, stud_num_group)

credential_non_dup <- credential_non_dup |>
  left_join(
    credential_ranking,
    by = join_by(
      ID,
      ENCRYPTED_TRUE_PEN,
      PSI_STUDENT_NUMBER,
      PSI_CODE,
      CREDENTIAL_AWARD_DATE_D
    )
  ) |>
  select(-RANK)

# ---- 09 Age Gender Distributions ---
age_weights <- credential_non_dup |>
  filter(
    !is.na(AGE_GROUP_AT_GRAD),
    HIGHEST_CRED_BY_DATE == "Yes"
  ) |>
  count(PSI_CREDENTIAL_CATEGORY, psi_gender_cleaned, AGE_AT_GRAD) |>
  complete(
    PSI_CREDENTIAL_CATEGORY = credential_rank$PSI_CREDENTIAL_CATEGORY,
    psi_gender_cleaned = valid_genders,
    AGE_AT_GRAD = full_seq(AGE_AT_GRAD, 1),
    fill = list(n = 0)
  ) |>
  group_by(PSI_CREDENTIAL_CATEGORY, psi_gender_cleaned) |>
  mutate(grp_ttl = sum(n, na.rm = TRUE)) |>
  mutate(prob = if_else(grp_ttl > 0, n / grp_ttl, 0)) |>
  summarise(
    ages = list(AGE_AT_GRAD),
    counts = list(n),
    weights = list(prob),
    .groups = "drop"
  )

set.seed(42)
# verify that these results produce similar distributions, the differences in
# sampling results may be considered insignificant
to_impute <- credential_non_dup |>
  filter(
    is.na(AGE_AT_GRAD),
    HIGHEST_CRED_BY_DATE == "Yes"
  ) |>
  select(
    ID,
    ENCRYPTED_TRUE_PEN,
    PSI_CREDENTIAL_CATEGORY,
    psi_gender_cleaned
  ) |>
  left_join(
    age_weights,
    by = c("PSI_CREDENTIAL_CATEGORY", "psi_gender_cleaned")
  )

imputed_student_ages <- to_impute |>
  mutate(
    IMPUTED_AGE_AT_GRAD = case_when(
      !is.null(ages) ~ as.numeric(map2(
        ages,
        weights,
        ~ sample(.x, size = 1, prob = .y)
      )),
      TRUE ~ sample(19:54, 1)
    )
  ) |>
  select(-ages, -weights, -counts)

credential_non_dup <- credential_non_dup |>
  left_join(
    imputed_student_ages,
    by = join_by(
      ID,
      ENCRYPTED_TRUE_PEN,
      PSI_CREDENTIAL_CATEGORY,
      psi_gender_cleaned
    )
  ) |>
  mutate(
    AGE_AT_GRAD = coalesce(
      as.numeric(AGE_AT_GRAD),
      as.numeric(IMPUTED_AGE_AT_GRAD)
    )
  ) |>
  select(-IMPUTED_AGE_AT_GRAD)

credential_non_dup <- credential_non_dup |>
  left_join(
    age_group_lookup |> select(AgeIndex, LowerBound, UpperBound),
    by = join_by(between(AGE_AT_GRAD, LowerBound, UpperBound))
  ) |>
  mutate(AGE_GROUP_AT_GRAD = AgeIndex) |>
  select(-AgeIndex, -LowerBound, -UpperBound)


# ---- VISA Status ----
cols_specific <- c(
  "ENCRYPTED_TRUE_PEN",
  "PSI_CODE",
  "PSI_STUDENT_NUMBER",
  "PSI_PROGRAM_CODE",
  "PSI_CREDENTIAL_PROGRAM_DESCRIPTION",
  "PSI_SCHOOL_YEAR"
)

cols_broad <- c(
  "ENCRYPTED_TRUE_PEN",
  "PSI_CODE",
  "PSI_STUDENT_NUMBER",
  "PSI_SCHOOL_YEAR"
)

visa_map <- credential_non_dup |>
  select(ID, all_of(cols_specific)) |>
  # Attempt 1: Perfect Match (6 columns)
  left_join(
    credential_supvars_enrolment |>
      select(all_of(cols_specific), VISA_SPECIFIC = PSI_VISA_STATUS) |>
      distinct(),
    relationship = "many-to-many"
  ) |>
  # Attempt 2: Broad Match (4 columns)
  left_join(
    credential_supvars_enrolment |>
      select(all_of(cols_broad), VISA_BROAD = PSI_VISA_STATUS) |>
      distinct(),
    relationship = "many-to-many"
  ) |>
  # Apply Hierarchy: Perfect Match -> Broad Match
  mutate(PSI_VISA_STATUS = coalesce(VISA_SPECIFIC, VISA_BROAD)) |>
  select(-VISA_SPECIFIC, -VISA_BROAD) |>
  distinct()

visa_map <- visa_map |>
  group_by(ID) |>
  slice_sample(n = 1) # randomly choose a VISA STATUS in the event of multiples.

credential_supvars <- credential_supvars |>
  left_join(
    visa_map |> select(ID, PSI_VISA_STATUS),
    by = "ID"
  )

credential_non_dup <- credential_non_dup |>
  left_join(
    visa_map |> select(ID, PSI_VISA_STATUS),
    by = "ID"
  )


# ---- 14-15 research University + Outcomes Credential ----
research_universities <- c("SFU", "UBC", "UBCV", "UBCO", "UNBC", "UVIC", "RRU")
credential_non_dup <- credential_non_dup |>
  mutate(
    RESEARCH_UNIVERSITY = if_else(
      PSI_CODE %in% research_universities,
      1L,
      NA_integer_
    )
  )

credential_non_dup <- credential_non_dup |>
  left_join(
    outcome_credential |>
      select(PSI_CREDENTIAL_CATEGORY, OUTCOMES_CRED = Outcomes_Cred),
    by = "PSI_CREDENTIAL_CATEGORY"
  )

# ---- 13 Delay Date and Highest rank----
credential_non_dup <- credential_non_dup |>
  mutate(
    CONCATENATED_ID = if_else(
      !ENCRYPTED_TRUE_PEN %in% na_vals,
      ENCRYPTED_TRUE_PEN,
      paste0(PSI_STUDENT_NUMBER, PSI_CODE)
    )
  )

tbl_credential_highest_rank <- credential_non_dup |>
  filter(HIGHEST_CRED_BY_RANK == "Yes")

tbl_credential_delay_effect <- credential_non_dup |>
  select(
    LID = ID,
    CONCATENATED_ID,
    LATER_AWARD_DATE = CREDENTIAL_AWARD_DATE_D,
    PSI_AWARD_SCHOOL_YEAR,
    PSI_CREDENTIAL_CATEGORY
  ) |>
  inner_join(credential_rank, by = "PSI_CREDENTIAL_CATEGORY") |>
  inner_join(
    tbl_credential_highest_rank |>
      select(
        HID = ID,
        HIGHEST_AWARD_DATE = CREDENTIAL_AWARD_DATE_D,
        CONCATENATED_ID
      ),
    by = "CONCATENATED_ID",
    relationship = "many-to-many"
  ) |>
  filter(LATER_AWARD_DATE > HIGHEST_AWARD_DATE)

tbl_credential_delay_effect <- tbl_credential_delay_effect |>
  mutate(
    MONTHS_DIFF = (year(LATER_AWARD_DATE) - year(HIGHEST_AWARD_DATE)) *
      12 +
      (month(LATER_AWARD_DATE) - month(HIGHEST_AWARD_DATE)),
    # Apply credential temporal thresholds
    keep = case_when(
      PSI_CREDENTIAL_CATEGORY %in%
        c(
          "Apprenticeship",
          "Bachelors Degree",
          "First Professional Degree"
        ) ~ TRUE,
      PSI_CREDENTIAL_CATEGORY %in%
        c("Advanced Diploma", "Advanced Certificate") &
        MONTHS_DIFF <= 36 ~ TRUE,
      PSI_CREDENTIAL_CATEGORY %in%
        c(
          "Diploma",
          "Masters Degree",
          "Graduate Diploma",
          "Post-Degree Diploma"
        ) &
        MONTHS_DIFF <= 30 ~ TRUE,
      PSI_CREDENTIAL_CATEGORY %in%
        c(
          "Associate Degree",
          "Certificate",
          "Graduate Certificate",
          "Post-Degree Certificate"
        ) &
        MONTHS_DIFF <= 18 ~ TRUE,
      TRUE ~ FALSE
    )
  ) |>
  filter(keep)

tbl_credential_delay_effect <- tbl_credential_delay_effect |>
  # Isolate the latest award date per student
  slice_max(LATER_AWARD_DATE, n = 1, with_ties = TRUE, by = CONCATENATED_ID) |>
  slice_min(LID, n = 1, with_ties = FALSE, by = CONCATENATED_ID) |>
  select(
    HID,
    CREDENTIAL_AWARD_DATE_D_DELAYED = LATER_AWARD_DATE,
    PSI_AWARD_SCHOOL_YEAR_DELAYED = PSI_AWARD_SCHOOL_YEAR
  )


tbl_credential_highest_rank <- tbl_credential_highest_rank |>
  left_join(tbl_credential_delay_effect, by = join_by(ID == HID)) |>
  mutate(
    CREDENTIAL_AWARD_DATE_D_DELAYED = coalesce(
      CREDENTIAL_AWARD_DATE_D_DELAYED,
      CREDENTIAL_AWARD_DATE_D
    ),
    PSI_AWARD_SCHOOL_YEAR_DELAYED = coalesce(
      PSI_AWARD_SCHOOL_YEAR_DELAYED,
      PSI_AWARD_SCHOOL_YEAR
    )
  )

credential_non_dup <- credential_non_dup |>
  left_join(
    tbl_credential_highest_rank |>
      select(
        ID,
        CREDENTIAL_AWARD_DATE_D_DELAYED,
        PSI_AWARD_SCHOOL_YEAR_DELAYED
      ),
    by = "ID"
  ) |>
  mutate(
    CREDENTIAL_AWARD_DATE_D_DELAYED = coalesce(
      CREDENTIAL_AWARD_DATE_D_DELAYED,
      CREDENTIAL_AWARD_DATE_D
    ),
    PSI_AWARD_SCHOOL_YEAR_DELAYED = coalesce(
      PSI_AWARD_SCHOOL_YEAR_DELAYED,
      PSI_AWARD_SCHOOL_YEAR
    )
  )


dbWriteTable(
  con,
  Id(schema = my_schema, name = "credential_non_dup_r"),
  credential_non_dup,
  overwrite = TRUE
)

dbWriteTable(
  con,
  Id(schema = my_schema, name = "credential_supvars_r"),
  credential_supvars,
  overwrite = TRUE
)

dbWriteTable(
  con,
  Id(schema = my_schema, name = "credential_r"),
  credential,
  overwrite = TRUE
)

dbWriteTable(
  con,
  Id(schema = my_schema, name = "tbl_credential_highest_rank_r"),
  tbl_credential_highest_rank,
  overwrite = TRUE
)


# refine as needed
tables_to_keep <- c(
  "stp_enrolment",
  "stp_credential",
  "stp_enrolment_record_type",
  "stp_credential_record_type",
  "stp_enrolment_valid",
  "age_group_lookup",
  "credential_rank",
  "credential",
  "credential_non_dup",
  "credential_sup_vars",
  "tbl_credential_highest_rank",
  "tbl_credential_delay_effect",
  "outcome_credential",
  "con",
  "db_config",
  "my_schema",
  "db_schema"
)

rm(list = setdiff(ls(), tables_to_keep))

# ---- Break and do Program Matching ----

# ---- Clean Up ----
dbDisconnect(con)
