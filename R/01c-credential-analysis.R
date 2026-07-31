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
library(futile.logger)

## -------------------------- Logging Setup -------------------------------------------------------
## -----------------------------------------------------------------------------------------------
log_file <- "./R/execution_log.txt"
flog.appender(appender.file(log_file), name = "file_logger")
flog.threshold(INFO, name = "file_logger")

log_info <- function(msg) {
  flog.info(msg, name = "file_logger")
  print(paste(Sys.time(), "|", msg))
}

log_info("==== 01c-credential-analysis.R START ====")

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
log_info("Connected to SQL Server database")

## --------------------------------------Required Tables------------------------------------------
## -----------------------------------------------------------------------------------------------
stp_enrolment <- dbReadTable(
  con,
  SQL(glue::glue('"{my_schema}"."stp_enrolment_r"'))
)
log_info(glue::glue("Loaded stp_enrolment_r: {nrow(stp_enrolment)} rows, {ncol(stp_enrolment)} columns"))
stp_credential <- dbReadTable(
  con,
  SQL(glue::glue('"{my_schema}"."stp_credential_r"'))
)
log_info(glue::glue("Loaded stp_credential_r: {nrow(stp_credential)} rows, {ncol(stp_credential)} columns"))
stp_credential_record_type <- dbReadTable(
  con,
  SQL(glue::glue('"{my_schema}"."stp_credential_record_type_r"'))
)
log_info(glue::glue("Loaded stp_credential_record_type_r: {nrow(stp_credential_record_type)} rows"))
stp_enrolment_valid <- dbReadTable(
  con,
  SQL(glue::glue('"{my_schema}"."stp_enrolment_valid_r"'))
)
log_info(glue::glue("Loaded stp_enrolment_valid_r: {nrow(stp_enrolment_valid)} rows"))

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

## ---------------------------------------Credential View-----------------------------------------
# WHAT: Create the working `credential` table from stp_credential, keeping only records
#       with RecordStatus == 0 (valid credentials) and a non-blank CREDENTIAL_AWARD_DATE.
# WHY:  This is the foundational credential table for the entire supply model. It filters
#       out invalid records (missing student numbers, developmental, skills-based, etc.)
#       and credentials without an award date so that only genuine, completed credentials
#       enter the modelling pipeline.
#       Downstream, this table is used in two key ways:
#       1. In this script (01c): it is enriched with birthdate, gender, age, and CIP data,
#          then deduplicated to create `credential_non_dup` (the main supply modelling table).
#       2. In 01d-enrolment-analysis.R: it is read back from SQL to backfill gender data
#          into the enrolment analysis.
# HOW:  Inner join stp_credential with stp_credential_record_type on ID, filter for
#       RecordStatus == 0 and valid award dates, then select the columns needed for
#       downstream processing.
# reference: source("./sql/01-credential-analysis/"credential-sup-vars-from-enrolment.R")
# qry00
## -----------------------------------------------------------------------------------------------
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

log_info(glue::glue("Created credential view (RecordStatus == 0, non-blank award date): {nrow(credential)} rows"))

## ----------------------- Make Credential Sup Vars Enrolment Table ------------------------------
# WHAT: Build `credential_supvars_enrolment`, a crosswalk table linking each valid credential
#       record to its most recent enrolment record. The join is done in two passes:
#       1. By ENCRYPTED_TRUE_PEN (valid PENs only) -> cred_supvars_enrol_epen
#       2. By PSI_CODE + PSI_STUDENT_NUMBER (for records with invalid/missing PENs) -> cred_supvars_enrol_no_pen
#       The two passes are combined with rbind + distinct.
# WHY:  Enrolment records contain supply-relevant variables that credential records lack:
#       psi_birthdate_cleaned, PSI_VISA_STATUS, PSI_GENDER, LAST_SEEN_BIRTHDATE, etc.
#       This crosswalk allows those enrolment-side variables to be joined onto the
#       credential_supvars table for birthdate cleaning (section 04), gender backfill,
#       and VISA status mapping.
#       The many-to-many relationship is expected: a student may have multiple enrolment
#       records and multiple credentials across the matching period.
# reference: source("./sql/01-credential-analysis/"credential-sup-vars-from-enrolment.R")
# qry01 to qry12
## -----------------------------------------------------------------------------------------------
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

log_info(glue::glue("cred_supvars_enrol_epen (PEN match): {nrow(cred_supvars_enrol_epen)} rows"))


# Match via PSI_CODE/Student Number to recover records missed by PEN join.
# Misses may also occur due to temporal mismatches or students lacking "Valid" enrolment status.
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

log_info(glue::glue("cred_supvars_enrol_no_pen (Student Number/PSI match): {nrow(cred_supvars_enrol_no_pen)} rows"))

credential_supvars_enrolment <- rbind(
  cred_supvars_enrol_epen,
  cred_supvars_enrol_no_pen
) |>
  distinct()

log_info(glue::glue("Combined credential_supvars_enrolment: {nrow(credential_supvars_enrolment)} rows"))

# ----------------------------- Make Credential Sup Vars Table -----------------------------------
# WHAT: Create `credential_supvars`, a credential-level table that mirrors `credential`
#       but adds CREDENTIAL_AWARD_DATE_D (date-typed award date) and serves as the base
#       for enrichment with birthdate, gender, and VISA status in subsequent sections.
# WHY:  `credential_supvars` ("supply variables") is the enrichment staging table. It
#       starts as a copy of credential with a date-typed award date, then progressively
#       accumulates:
#       - psi_birthdate_cleaned / psi_birthdate_cleaned_D (section 04)
#       - psi_gender_cleaned (section 03, via gender-cleaning.r)
#       - LAST_SEEN_BIRTHDATE (section 04, fallback for missing birthdates)
#       - PSI_VISA_STATUS (VISA Status section, via visa_map)
#       Once enriched, these variables are joined back into the main `credential` table
#       (rebuilt in section 04) and carried into `credential_non_dup` for supply modelling.
# HOW:  Select credential columns, cast CREDENTIAL_AWARD_DATE to Date type.
#       Separately, credential_supvars_enrolment links enrolment records to credentials
#       (via PEN or PSI_CODE/Student Number) and brings in enrolment-side variables
#       (birthdates, VISA status, gender) that don't exist on the credential side.
# reference: source("./sql/01-credential-analysis/"credential-sup-vars-from-enrolment.R")
# qry0?
## -----------------------------------------------------------------------------------------------
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


## ----------------------------02 Developmental Records-------------------------------------------
# WHAT: Flag credentials whose category is not meaningful for supply modelling so they
#       can be excluded from the final credential table.
# WHY:  The following PSI_CREDENTIAL_CATEGORY values represent credentials that do not
#       correspond to a completed, recognized post-secondary qualification:
#       - "Developmental Credential": Preparatory or upgrading coursework, not a
#         standalone credential counted in supply.
#       - "Other": Unclassified credentials that don't fit any standard category.
#       - "None": No credential category was assigned by the institution.
#       - "Short Certificate": Credentials below the threshold for supply modelling
#         (e.g., brief non-credit courses).
#       These records are kept in stp_credential_record_type for audit purposes but
#       are filtered out later (line ~488) when the final `credential` table is rebuilt
#       by requiring is.na(DropCredCategory).
# HOW:  Left join a "Yes" flag onto stp_credential_record_type for any ID whose
#       credential category matches the exclusion list.
## -----------------------------------------------------------------------------------------------
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

log_info(glue::glue("02 Developmental Records: DropCredCategory flagged on {sum(!is.na(stp_credential_record_type$DropCredCategory))} records"))


## ---------------------------------------- 03 Miscellaneous -------------------------------------
# WHAT: Flag credentials awarded on or after September 1 of the current model year
#       so they can be excluded from the final credential table.
# WHY:  Credentials awarded in the partial/incomplete year (e.g., Sep 2023 onward for a
#       2023/2024 model run) represent an incomplete cohort. The full academic year has
#       not finished, so the count would under-represent the eventual total. These
#       records are kept for audit but filtered out later (line ~489) when the final
#       `credential` table is rebuilt by requiring is.na(DropPartialYear).
#       Note: The cutoff date ("2023-09-01") must be updated for each new model cycle.
# HOW:  Filter credential_supvars for records with CREDENTIAL_AWARD_DATE >= cutoff,
#       tag them with DropPartialYear = "Yes", and right_join back to
#       stp_credential_record_type so all records are preserved.
## -----------------------------------------------------------------------------------------------
stp_credential_record_type <-
  credential_supvars |>
  filter(CREDENTIAL_AWARD_DATE >= "2023-09-01") |>
  select(ID) |>
  mutate(DropPartialYear = "Yes") |>
  right_join(stp_credential_record_type, by = "ID")

log_info(glue::glue("03 Miscellaneous: DropPartialYear flagged on {sum(!is.na(stp_credential_record_type$DropPartialYear))} records"))

rm(
  latest_enrolment_no_epen,
  latest_enrolment_epen,
  cred_supvars_enrol_no_pen,
  cred_supvars_enrol_epen
)
gc()

## ---------------------------------------- 03 Gender Cleaning -----------------------------------
# !! Entire section is replaced with the gender_cleaning.r script
## -----------------------------------------------------------------------------------------------

log_info("03 Gender Cleaning: sourcing R/01c-gender-cleaning.r")
source("R/01c-gender-cleaning.r")
log_info("03 Gender Cleaning: complete")

## --------------------------------04 Birthdate cleaning (last seen birthdate)--------------------
# note: check if LAST_SEEN_BIRTHDATE can be included when supvars tables are created (at the top of script)
# note: a handful out but I think the R version is handling the coalesce correctly.
# we're only adding LAST_SEEN_BIRTHDATE for valid epens. Can we add for invalid EPENS?
## -----------------------------------------------------------------------------------------------

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

log_info(glue::glue("04 Birthdate cleaning complete. credential rebuilt: {nrow(credential)} rows after filtering RecordStatus==0, DropCredCategory, DropPartialYear"))
## -----------------------------------------------------------------------------------------------
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

log_info(glue::glue("05 Age and Credential: {nrow(credential)} rows. AGE_AT_GRAD range: {min(credential$AGE_AT_GRAD, na.rm=TRUE)}-{max(credential$AGE_AT_GRAD, na.rm=TRUE)}"))
## -----------------------------------------------------------------------------------------------

## -----------------------------------05b Make Non-Dup Table--------------------------------------
# WHAT: Deduplicate the `credential` table to create `credential_non_dup`, the primary
#       credential-level table used for supply modelling.
# WHY:  A student may receive the same credential (same institution, program, CIP, level,
#       category, and award date) multiple times due to data entry duplicates or re-issuance.
#       `credential_non_dup` keeps only one record per unique credential combination so that
#       each completed credential is counted exactly once in the supply model.
#
#       This table is the central output of the credential analysis pipeline and is consumed
#       by multiple downstream scripts:
#       - 01e-stp-distributions.R: reads credential_non_dup_r for FINAL_CIP_CLUSTER_CODE,
#         FINAL_CIP_CODE_4, RESEARCH_UNIVERSITY, and OUTCOMES_CRED to build aggregated
#         credential distribution tables.
#       - 02a-update-cred-non-dup.R: updates this table with final CIP codes from BGS/APPSO/GRAD
#         program matching.
#       - 02a-bgs-program-matching.R: uses credential_non_dup as the source for BGS and GRAD
#         credential ID tables.
#       - load-program-projections.R: reads this table for program projection loading.
#
#       After deduplication, the table is further enriched with:
#       - Gender imputation (stochastic proportional fill)
#       - Credential ranking (HIGHEST_CRED_BY_DATE, HIGHEST_CRED_BY_RANK)
#       - Age imputation
#       - VISA status
#       - Delay date (CREDENTIAL_AWARD_DATE_D_DELAYED)
#       - Research university flag and outcomes credential mapping
# HOW:  Group by all credential-identifying fields (PEN, PSI_CODE, program code, description,
#       CIP, level, category, award date) and keep the record with the highest ID per group.
#       Then forward-fill gender within each student group (by PEN/Student Number/PSI Code)
#       so the most recent gender value is applied to all that student's credentials.
## -----------------------------------------------------------------------------------------------

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

log_info(glue::glue("Non-dup table created: {nrow(credential_non_dup)} rows (deduplicated from {nrow(credential)} credential rows)"))
# This procedure performs proportional stochastic imputation to fill in missing gender data.
# It calculates the existing gender distribution for each credential category and then
# uses those ratios as weights to "flip a coin" for every empty record
# ensuring the final dataset maintains the same statistical balance as the known population.
## -----------------------------------------------------------------------------------------------

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

log_info(glue::glue("Gender imputation complete. Remaining NA genders: {sum(is.na(credential_non_dup$psi_gender_cleaned))}"))

rm(
  credential_supvars_birthdate_clean,
  gender_weights
)

## ---------------------------------08 Credential Ranking-----------------------------------------
# The R version produces similar results to SQL.  Some differences noted
# in how SQL and R handle tie-breaking leads to different row flags (HIGHEST_CRED_BY_DATE/RANK)
# for a handful of records, in almost all cases this is because two credentials of same rank are assigned on the same date
# This will hopefully have minimal impact on overall results.
## -----------------------------------------------------------------------------------------------

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

log_info(glue::glue("08 Credential Ranking complete. HIGHEST_CRED_BY_DATE: {sum(credential_non_dup$HIGHEST_CRED_BY_DATE == 'Yes', na.rm=TRUE)}, HIGHEST_CRED_BY_RANK: {sum(credential_non_dup$HIGHEST_CRED_BY_RANK == 'Yes', na.rm=TRUE)}"))
## -----------------------------------------------------------------------------------------------
age_weights <- credential_non_dup |>
  filter(
    !is.na(AGE_GROUP_AT_GRAD),
    HIGHEST_CRED_BY_DATE == "Yes"
  ) |>
  count(PSI_CREDENTIAL_CATEGORY, psi_gender_cleaned, AGE_AT_GRAD) |>
  complete(
    PSI_CREDENTIAL_CATEGORY,
    psi_gender_cleaned,
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

log_info(glue::glue("09 Age/Gender distributions: imputed {nrow(imputed_student_ages)} ages. Remaining NA AGE_AT_GRAD: {sum(is.na(credential_non_dup$AGE_AT_GRAD))}"))
## -----------------------------------------------------------------------------------------------
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

log_info(glue::glue("VISA Status mapping complete. VISA mapped on {sum(!is.na(credential_non_dup$PSI_VISA_STATUS))} / {nrow(credential_non_dup)} records"))


## -----------------------13 Delay Date and Highest rank------------------------------------------
# WHAT: Identify each student's highest-ranked credential and calculate a "delayed" award
#       date/year that accounts for subsequent credentials earned after the highest one.
# WHY:  In supply modelling, each student should be counted once, represented by their
#       highest credential. However, some students earn a lower-ranked credential first
#       and a higher-ranked one later. The "delay effect" captures the time gap between
#       the highest credential and any later credential within a temporal threshold,
#       so that the award year used for distribution counts reflects when the student
#       effectively completed their highest qualification path.
#
#       `tbl_credential_highest_rank` contains only the single highest-ranked credential
#       per student (HIGHEST_CRED_BY_RANK == "Yes"), enriched with:
#       - CREDENTIAL_AWARD_DATE_D_DELAYED: the later award date if a delay effect exists,
#         otherwise the original award date.
#       - PSI_AWARD_SCHOOL_YEAR_DELAYED: the corresponding school year.
#
#       This table is consumed by:
#       - 01e-stp-distributions.R: reads tbl_credential_highest_rank_r to build aggregated
#         credential distribution tables by gender, age group, CIP, and school year.
#       - load-program-projections.R: reads this table for program projection loading.
# HOW:  Filter credential_non_dup for HIGHEST_CRED_BY_RANK == "Yes", then self-join on
#       CONCATENATED_ID to find later credentials. Apply temporal thresholds per credential
#       category (e.g., Bachelors = unlimited, Diploma <= 30 months) to filter valid delays.
#       Coalesce delayed dates back onto the highest-rank table.
## -----------------------------------------------------------------------------------------------
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

log_info(glue::glue("13 Delay Date: tbl_credential_highest_rank: {nrow(tbl_credential_highest_rank)} rows"))

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

log_info(glue::glue("13 Delay Date: delay effect candidates before temporal threshold filter: {nrow(tbl_credential_delay_effect)}"))

tbl_credential_delay_effect <- tbl_credential_delay_effect |>
  mutate(
    MONTHS_DIFF = (year(LATER_AWARD_DATE) - year(HIGHEST_AWARD_DATE)) *
      12 +
      (month(LATER_AWARD_DATE) - month(HIGHEST_AWARD_DATE)),
    # Apply credential temporal thresholds
    #
    # WHAT: Filter delay-effect candidates so that only "realistic" delays are kept.
    #       A delay is realistic when the time gap between the highest credential and
    #       a later credential falls within a category-specific maximum number of months.
    #
    # WHY:  When a student earns a lower-ranked credential after their highest one, it
    #       may be a genuine continuation of the same educational path (e.g., earning a
    #       Certificate then a Diploma within 2 years) or an unrelated credential earned
    #       much later in life (e.g., a Certificate earned 15 years after a Bachelors).
    #       Only the former should shift the award year used for supply distribution
    #       counting, because it represents the effective completion of the student's
    #       highest qualification path. The thresholds are set based on typical program
    #       durations and are inherited from the original SQL model:
    #
    #       - Apprenticeship, Bachelors Degree, First Professional Degree: NO limit.
    #         These are high-value credentials where any subsequent lower credential is
    #         considered part of the same educational progression regardless of time gap.
    #       - Advanced Diploma, Advanced Certificate: <= 36 months (3 years).
    #         These programs typically take 1-2 years to complete; a 3-year window allows
    #         for part-time study or a gap year while still being a plausible continuation.
    #       - Diploma, Masters Degree, Graduate Diploma, Post-Degree Diploma: <= 30 months.
    #         Masters programs are typically 1-2 years; 30 months allows for thesis
    #         extensions or part-time completion.
    #       - Associate Degree, Certificate, Graduate Certificate, Post-Degree Certificate:
    #         <= 18 months (1.5 years). These are shorter programs; a tighter window ensures
    #         only closely-related subsequent credentials are counted as delays.
    #
    #       Records outside these thresholds are dropped (keep = FALSE), meaning the
    #       student's award year remains based on their highest credential's original date.
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

log_info(glue::glue("13 Delay Date: delay effect records after temporal threshold filter: {nrow(tbl_credential_delay_effect)}"))

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

## --------------------14-15 research University + Outcomes Credential----------------------------
# Notes: this currently updates credential_non_dup but I think we need these variables added to
# tbl_credential_highest_rank, as well.
## -----------------------------------------------------------------------------------------------
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

log_info(glue::glue("14-15 Research University + Outcomes Credential complete. RESEARCH_UNIVERSITY flagged: {sum(credential_non_dup$RESEARCH_UNIVERSITY == 1, na.rm=TRUE)} records"))

## ------------------------------------ Clean Up --------------------------------------------------
# Current workflow:
#  - Write key tables back to sql server.  These are tables needed for downstream work, or tables
# that might be needed for later reference outside of this analysis.
#  - Close DB connections
#  - Remove all objects at the end of each script.
## ------------------------------------------------------------------------------------------------

# refine as needed
tables_to_keep <- c(
  "stp_credential_record_type",
  "age_group_lookup",
  "credential",
  "credential_non_dup",
  "credential_supvars",
  "credential_supvars_enrolment",
  "tbl_credential_highest_rank",
  "tbl_credential_delay_effect"
)

write_table_to_db <- function(table_name, schema, con) {
  db_name <- paste0(table_name, "_r")
  dbWriteTable(
    con,
    SQL(glue::glue('"{schema}"."{db_name}"')),
    base::get(table_name, envir = .GlobalEnv),
    overwrite = TRUE
  )
  log_info(glue::glue("Wrote table '{schema}.{db_name}' ({nrow(base::get(table_name, envir = .GlobalEnv))} rows) to SQL Server"))
}

log_info(glue::glue("Writing {length(tables_to_keep)} tables to DB: {paste(tables_to_keep, collapse = ', ')}"))
walk(tables_to_keep, write_table_to_db, schema = my_schema, con = con)

dbDisconnect(con)
log_info("Disconnected from SQL Server")

log_info("==== 01c-credential-analysis.R COMPLETE ====")


# rm(list = ls())

# ---- Break and do Program Matching ----
