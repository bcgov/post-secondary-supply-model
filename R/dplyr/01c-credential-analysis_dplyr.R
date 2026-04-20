# Credential Analysis — dplyr Translation
# Original: R/01c-credential-analysis.R (~2595 lines, ~250 SQL ops)
#
# Pipeline context:
#   Processes STP_Credential data to produce cleaned, deduplicated credential records
#   with supplementary variables (gender, birthdate, visa status, age, ranking).
#   Outputs feed into program matching (02a) and graduate projections (04).
#
# Major sections:
#   1. Build CredentialSupVarsFromEnrolment — match enrolment records to credentials
#   2. Build CredentialSupVars — base credential table with empty SupVar columns
#   3. Developmental/partial year exclusions
#   4. Birthdate cleaning from enrolment
#   5. Gender cleaning (resolve multi-gender EPENs, unknowns, null-EPEN fallback)
#   6. Birthdate cleaning with LAST_SEEN_BIRTHDATE
#   7. Recreate Credential view with cleaned SupVars
#   8. Age at graduation calculation
#   9. Credential_Non_Dup creation (deduplication)
#  10. Gender imputation for Non_Dup (proportional assignment)
#  11. Credential ranking (highest by date/rank within EPEN or STUDENT_NUMBER/PSI_CODE)
#  12. Age imputation for missing ages
#  13. Visa status cleaning (multi-step enrolment matching)
#  14. Delay date calculation (later-awarded credentials within time windows)
#  15. Research university / outcome credential flags
#  16. Final distributions (8+ Credential_By_Year output tables)
#
# Input tables:
#   - STP_Credential, STP_Credential_Record_Type — raw credential data + record status
#   - STP_Enrolment, STP_Enrolment_Valid — enrolment records for SupVar matching
#   - AgeGroupLookup — age group index → label mapping
#   - CredentialRank — credential category → rank mapping
#   - OutcomeCredential — credential category → outcome cred label mapping
#
# Output tables (written to DB):
#   - CredentialSupVars — cleaned credential-level SupVars
#   - CredentialSupVarsFromEnrolment — enrolment records matched to credentials
#   - Credential_Non_Dup — deduplicated credentials
#   - tblCredential_HighestRank — highest-ranked credentials with delay dates
#   - Credential_By_Year_AgeGroup_* (8 variants) — final distribution tables

library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(dbplyr)

# ---- Configure LAN and file paths ----
db_config <- config::get("decimal")
lan <- config::get("lan")
my_schema <- config::get("myschema")

con <- dbConnect(odbc::odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")

# Helper: reference a table in the user's schema
sch_tbl <- function(name) {
  tbl(con, dbplyr::in_schema(my_schema, name))
}

# Helper: check if a value is blank/missing/unspecified
is_blank <- function(x) {
  is.na(x) | x %in% c("", " ", "(Unspecified)")
}


# ******************************************************************************
# Section 1: Build CredentialSupVarsFromEnrolment
# WHY: Credential records lack gender, birthdate, visa status, etc. We match them
# to enrolment records to fill in these supplementary variables. Matching is done
# first by ENCRYPTED_TRUE_PEN (EPEN), then by PSI_CODE + PSI_STUDENT_NUMBER for
# records without a valid EPEN.
# ******************************************************************************

# ---- Pull source tables ----
stp_credential <- sch_tbl("STP_Credential") %>% collect() |> rename_with(toupper)
stp_cred_rec_type <- sch_tbl("STP_Credential_Record_Type") %>% collect() |> rename_with(toupper)
stp_enrolment <- sch_tbl("STP_Enrolment") %>% collect() |> rename_with(toupper)
stp_enrolment_valid <- sch_tbl("STP_Enrolment_Valid") %>% collect() |> rename_with(toupper)

# ---- Create Credential view equivalent ----
# WHY: Filter to valid records (RecordStatus=0) with non-blank award dates.
credential <- stp_credential %>%
  inner_join(stp_cred_rec_type %>% select(ID, RECORDSTATUS),
             by = "ID") %>%
  filter(RECORDSTATUS == 0,
         CREDENTIAL_AWARD_DATE != "",
         CREDENTIAL_AWARD_DATE != " ",
         CREDENTIAL_AWARD_DATE != "(Unspecified)")

# ---- Part A: Match by EPEN ----
# Find max school year per EPEN, then get enrolment IDs at that max year.
enrol_max_year_by_epen <- stp_enrolment_valid %>%
  filter(!is_blank(ENCRYPTED_TRUE_PEN)) %>%
  group_by(ENCRYPTED_TRUE_PEN) %>%
  summarise(MaxSchoolYear = max(PSI_SCHOOL_YEAR), .groups = "drop")

# Get enrolment records at max school year per EPEN
enrol_by_epen <- stp_enrolment_valid %>%
  inner_join(enrol_max_year_by_epen,
             by = c("ENCRYPTED_TRUE_PEN", "PSI_SCHOOL_YEAR" = "MaxSchoolYear")) %>%
  select(ID, ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, MaxSchoolYear) %>%
  distinct()

# Get full enrolment data for matched IDs
enrol_step3_epen <- stp_enrolment_valid %>%
  semi_join(enrol_by_epen, by = "ID") %>%
  select(ID, PSI_STUDENT_NUMBER, ENCRYPTED_TRUE_PEN, PSI_SCHOOL_YEAR,
         PSI_STUDENT_POSTAL_CODE_CURRENT, PSI_ENROLMENT_SEQUENCE, PSI_CODE, PSI_MIN_START_DATE)

# Build CredentialSupVarsFromEnrolment Part A — join enrolment to Credential by EPEN
csve_part_a <- enrol_step3_epen %>%
  inner_join(
    credential %>% select(RECORDSTATUS, ENCRYPTED_TRUE_PEN),
    by = "ENCRYPTED_TRUE_PEN"
  ) %>%
  distinct() %>%
  transmute(
    ENROLMENTID = ID,
    ENCRYPTED_TRUE_PEN,
    PSI_MIN_START_DATE,
    CREDENTIALRECORDSTATUS = RECORDSTATUS,
    PSI_STUDENT_POSTAL_CODE_CURRENT,
    PSI_SCHOOL_YEAR,
    PSI_CODE,
    PSI_STUDENT_NUMBER,
    PSI_ENROLMENT_SEQUENCE
  )

# ---- Part B: Match by PSI_CODE + PSI_STUDENT_NUMBER (for null/blank EPENs) ----
# Get credential EPENs that were NOT matched in Part A
cred_epens_not_matched <- credential %>%
  anti_join(csve_part_a, by = "ENCRYPTED_TRUE_PEN") %>%
  select(PSI_CODE, PSI_STUDENT_NUMBER)

# Get credential records with null/blank EPENs
cred_null_epens <- credential %>%
  filter(ENCRYPTED_TRUE_PEN == "") %>%
  select(ID, PSI_CODE, PSI_STUDENT_NUMBER)

# Find max school year per PSI_CODE/STUDENT_NUMBER for null-EPEN records
enrol_max_year_by_stuid <- stp_enrolment_valid %>%
  filter(is_blank(ENCRYPTED_TRUE_PEN)) %>%
  group_by(PSI_CODE, PSI_STUDENT_NUMBER) %>%
  summarise(MaxSchoolYear = max(PSI_SCHOOL_YEAR), .groups = "drop")

# Get enrolment records at max year by PSI_CODE/STUDENT_NUMBER
enrol_by_stuid <- stp_enrolment_valid %>%
  inner_join(enrol_max_year_by_stuid,
             by = c("PSI_CODE", "PSI_STUDENT_NUMBER", "PSI_SCHOOL_YEAR" = "MaxSchoolYear")) %>%
  select(ID, ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, MaxSchoolYear) %>%
  distinct()

# Full enrolment data for null-EPEN matched IDs
enrol_step6_stuid <- stp_enrolment_valid %>%
  semi_join(enrol_by_stuid, by = "ID") %>%
  select(ID, PSI_STUDENT_NUMBER, ENCRYPTED_TRUE_PEN, PSI_SCHOOL_YEAR,
         PSI_STUDENT_POSTAL_CODE_CURRENT, PSI_ENROLMENT_SEQUENCE, PSI_CODE, PSI_MIN_START_DATE)

# Build CredentialSupVarsFromEnrolment Part B — join by PSI_CODE/STUDENT_NUMBER
csve_part_b <- enrol_step6_stuid %>%
  inner_join(
    credential %>% select(RECORDSTATUS, PSI_CODE, PSI_STUDENT_NUMBER),
    by = c("PSI_CODE", "PSI_STUDENT_NUMBER")
  ) %>%
  distinct() %>%
  transmute(
    ENROLMENTID = ID,
    ENCRYPTED_TRUE_PEN,
    PSI_MIN_START_DATE,
    CREDENTIALRECORDSTATUS = RECORDSTATUS,
    PSI_STUDENT_POSTAL_CODE_CURRENT,
    PSI_SCHOOL_YEAR,
    PSI_CODE,
    PSI_STUDENT_NUMBER,
    PSI_ENROLMENT_SEQUENCE
  )

# Combine both parts
CredentialSupVarsFromEnrolment <- bind_rows(csve_part_a, csve_part_b)

# Add supplementary columns from STP_Enrolment
enrol_supvars <- stp_enrolment %>%
  select(ID, PSI_BIRTHDATE_CLEANED, PSI_VISA_STATUS, PSI_BIRTHDATE,
         PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
         PSI_CIP_CODE, PSI_CONTINUING_EDUCATION_COURSE_ONLY, PSI_GENDER,
         LAST_SEEN_BIRTHDATE)

CredentialSupVarsFromEnrolment <- CredentialSupVarsFromEnrolment %>%
  left_join(enrol_supvars, by = c("ENROLMENTID" = "ID")) %>%
  mutate(
    PSI_BIRTHDATE_CLEANED = if_else(
      PSI_BIRTHDATE_CLEANED == as.Date("1900-01-01"),
      as.Date(NA), PSI_BIRTHDATE_CLEANED
    )
  )


# ******************************************************************************
# Section 2: Build CredentialSupVars
# WHY: Create the base credential supplementary variables table from the Credential
# view, with empty columns to be filled in by subsequent cleaning steps.
# ******************************************************************************

CredentialSupVars <- credential %>%
  transmute(
    ID,
    ENCRYPTED_TRUE_PEN,
    PSI_STUDENT_NUMBER,
    PSI_CODE,
    PSI_SCHOOL_YEAR,
    CREDENTIAL_AWARD_DATE,
    CREDENTIALRECORDSTATUS = RECORDSTATUS,
    PSI_PROGRAM_CODE,
    PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    PSI_CREDENTIAL_CIP,
    PSI_CREDENTIAL_LEVEL,
    PSI_CREDENTIAL_CATEGORY
  ) %>%
  # Add empty columns to be filled later
  mutate(
    CREDENTIAL_AWARD_DATE_D = as.Date(NA),
    PSI_AWARD_SCHOOL_YEAR = NA_character_,
    RECORD_TO_DELETE = NA_integer_,
    PSI_BIRTHDATE_CLEANED_D = as.Date(NA),
    PSI_BIRTHDATE_CLEANED = as.Date(NA),
    LAST_DATE_HIGHEST_CRED = NA_character_,
    HIGHEST_CRED_BY_DATE = NA_character_,
    HIGHEST_CRED_BY_RANK = NA_character_,
    HIGHEST_CRED_BY_SCHOOL_YEAR = NA_character_,
    OUTCOMES_CRED = NA_character_,
    RESEARCH_UNIVERSITY = NA_integer_,
    CREDENTIAL_AWARD_DATE_D_DELAYED = as.Date(NA),
    PSI_AWARD_SCHOOL_YEAR_DELAYED = NA_character_,
    AGE_AT_GRAD = NA_integer_,
    AGE_GROUP_AT_GRAD = NA_integer_,
    PSI_GENDER_CLEANED = NA_character_
  )


# ******************************************************************************
# Section 3: Developmental/partial year exclusions
# WHY: Flag credentials that should be excluded: developmental credentials, OTHER,
# NONE, SHORT CERTIFICATE categories, and credentials awarded in the partial year
# (>= model year Sept 1). These flags are set on STP_Credential_Record_Type.
# ******************************************************************************

# Developmental credential categories
drop_cred_cats <- c("DEVELOPMENTAL CREDENTIAL", "OTHER", "NONE", "SHORT CERTIFICATE")

cred_ids_to_drop <- credential %>%
  filter(PSI_CREDENTIAL_CATEGORY %in% drop_cred_cats) %>%
  select(ID)

# Flag in STP_Credential_Record_Type (will be used by the view filter)
stp_cred_rec_type <- stp_cred_rec_type %>%
  mutate(DROPCREDCATEGORY = if_else(ID %in% cred_ids_to_drop$ID, "Yes", NA_character_))

# Partial year exclusion — flag credentials with award date >= 2023-09-01
# WHY: Credentials from the current incomplete year should be excluded.
# !! Update this date for each model run !!
partial_year_cutoff <- as.Date("2023-09-01")

# First set award date on CredentialSupVars
CredentialSupVars <- CredentialSupVars %>%
  mutate(CREDENTIAL_AWARD_DATE_D = as.Date(CREDENTIAL_AWARD_DATE))

cred_ids_partial <- CredentialSupVars %>%
  filter(CREDENTIAL_AWARD_DATE_D >= partial_year_cutoff) %>%
  select(ID)

stp_cred_rec_type <- stp_cred_rec_type %>%
  mutate(DROPPARTIALYEAR = if_else(ID %in% cred_ids_partial$ID, "Yes", NA_character_))


# ******************************************************************************
# Section 4: Birthdate cleaning from enrolment
# WHY: Extract unique birthdate values per EPEN/PSI_CODE/STUDENT_NUMBER from
# enrolment data. Used to fill missing birthdates in CredentialSupVars.
# ******************************************************************************

cred_sup_vars_birthdate_clean <- CredentialSupVarsFromEnrolment %>%
  select(ENCRYPTED_TRUE_PEN, PSI_BIRTHDATE_CLEANED, PSI_BIRTHDATE_CLEANED_D,
         PSI_STUDENT_NUMBER, PSI_CODE) %>%
  distinct() %>%
  mutate(
    PSI_BIRTHDATE_CLEANED_D = if_else(
      !is.na(PSI_BIRTHDATE_CLEANED) & !is_blank(PSI_BIRTHDATE_CLEANED),
      as.Date(PSI_BIRTHDATE_CLEANED), PSI_BIRTHDATE_CLEANED_D
    )
  )


# ******************************************************************************
# Section 5: Gender cleaning
# WHY: Many records have missing, unknown, or inconsistent gender values across
# enrolment records. This extensive cleaning process resolves multi-gender EPENs
# by selecting the most recent gender, cleans unknowns, and falls back to
# PSI_CODE/PSI_STUDENT_NUMBER matching for null-EPEN records.
#
# The original uses ~35 temp tables. Here we use dplyr pipelines with intermediate
# R variables.
# ******************************************************************************

# ---- 5a: Build CredentialSupVars_Gender — unique EPEN/gender from enrolment ----
csvs_gender <- CredentialSupVarsFromEnrolment %>%
  select(ENCRYPTED_TRUE_PEN, PSI_GENDER) %>%
  distinct()

# ---- 5b: Find EPENs with multiple genders ----
multi_gender_epens <- csvs_gender %>%
  count(ENCRYPTED_TRUE_PEN) %>%
  filter(n > 1) %>%
  select(ENCRYPTED_TRUE_PEN)

# ---- 5c: Resolve multi-gender EPENs ----
# WHY: When an EPEN has multiple genders across enrolment records, we pick the gender
# from the most recent school year, breaking ties by max enrolment sequence.
# Original: Steps 1-5 across 3 temp tables.

if (nrow(multi_gender_epens) > 0) {
  # Max school year + enrolment sequence per EPEN/gender combo
  multi_gender_max <- CredentialSupVarsFromEnrolment %>%
    semi_join(multi_gender_epens, by = "ENCRYPTED_TRUE_PEN") %>%
    group_by(ENCRYPTED_TRUE_PEN, PSI_GENDER) %>%
    summarise(
      MAX_PSI_SCHOOL_YEAR = max(PSI_SCHOOL_YEAR),
      MAX_PSI_ENROLMENT_SEQUENCE = max(PSI_ENROLMENT_SEQUENCE),
      .groups = "drop"
    )

  # Overall max per EPEN
  multi_gender_overall_max <- multi_gender_max %>%
    group_by(ENCRYPTED_TRUE_PEN) %>%
    summarise(
      MAX_MAX_PSI_SCHOOL_YEAR = max(MAX_PSI_SCHOOL_YEAR),
      MAX_MAX_PSI_ENROLMENT_SEQUENCE = max(MAX_PSI_ENROLMENT_SEQUENCE),
      .groups = "drop"
    )

  # Resolve: join to find the gender at the max school year + max enrolment sequence
  multi_gender_resolved <- multi_gender_overall_max %>%
    filter(!is_blank(ENCRYPTED_TRUE_PEN)) %>%
    inner_join(
      multi_gender_max,
      by = c("ENCRYPTED_TRUE_PEN" = "ENCRYPTED_TRUE_PEN",
             "MAX_MAX_PSI_SCHOOL_YEAR" = "MAX_PSI_SCHOOL_YEAR",
             "MAX_MAX_PSI_ENROLMENT_SEQUENCE" = "MAX_PSI_ENROLMENT_SEQUENCE")
    ) %>%
    select(ENCRYPTED_TRUE_PEN, PSI_GENDER_TO_USE = PSI_GENDER) %>%
    distinct()
} else {
  multi_gender_resolved <- tibble(
    ENCRYPTED_TRUE_PEN = character(),
    PSI_GENDER_TO_USE = character()
  )
}

# ---- 5d: Apply resolved genders to the gender table ----
csvs_gender <- csvs_gender %>%
  left_join(multi_gender_resolved, by = "ENCRYPTED_TRUE_PEN") %>%
  mutate(
    PSI_GENDER_CLEANED_FLAG = if_else(!is.na(PSI_GENDER_TO_USE), "Yes", NA_character_),
    PSI_GENDER_CLEANED = coalesce(PSI_GENDER_TO_USE, PSI_GENDER)
  )

# ---- 5e: Clean unknowns ('U', 'Unknown') by looking at all enrolment genders ----
# WHY: Some EPENs have gender='U' or 'Unknown'. We check if other enrolment records
# for the same EPEN have a valid gender (Male/Female/Gender Diverse).
unknown_genders <- csvs_gender %>%
  filter(PSI_GENDER_CLEANED %in% c("U", "Unknown"))

if (nrow(unknown_genders) > 0) {
  # Find non-U/Unknown genders from enrolment for these EPENs
  unknowns_with_alternatives <- unknown_genders %>%
    select(ENCRYPTED_TRUE_PEN) %>%
    inner_join(
      CredentialSupVarsFromEnrolment %>%
        select(ENCRYPTED_TRUE_PEN, PSI_GENDER) %>%
        distinct(),
      by = "ENCRYPTED_TRUE_PEN"
    ) %>%
    filter(!PSI_GENDER %in% c("U", "Unknown")) %>%
    group_by(ENCRYPTED_TRUE_PEN) %>%
    summarise(GENDERTOUSE = first(PSI_GENDER), .groups = "drop")

  # Update csvs_gender with resolved unknowns
  csvs_gender <- csvs_gender %>%
    left_join(unknowns_with_alternatives, by = "ENCRYPTED_TRUE_PEN") %>%
    mutate(
      PSI_GENDER_CLEANED = if_else(
        PSI_GENDER_CLEANED %in% c("U", "Unknown") & !is.na(GENDERTOUSE),
        GENDERTOUSE, PSI_GENDER_CLEANED
      )
    ) %>%
    select(-GENDERTOUSE)
}

# ---- 5f: Apply gender to CredentialSupVars by EPEN ----
# WHY: Update CredentialSupVars PSI_GENDER_CLEANED from the resolved gender table.
CredentialSupVars <- CredentialSupVars %>%
  left_join(
    csvs_gender %>%
      filter(!is_blank(ENCRYPTED_TRUE_PEN)) %>%
      select(ENCRYPTED_TRUE_PEN, PSI_GENDER_CLEANED),
    by = "ENCRYPTED_TRUE_PEN",
    suffix = c("", "_from_gender")
  ) %>%
  mutate(PSI_GENDER_CLEANED = coalesce(PSI_GENDER_CLEANED_from_gender, PSI_GENDER_CLEANED)) %>%
  select(-PSI_GENDER_CLEANED_from_gender)

# ---- 5g: Handle null-EPEN records — match by PSI_CODE/PSI_STUDENT_NUMBER ----
# WHY: Records without a valid EPEN need gender from STP_Enrolment matched by
# PSI_CODE + PSI_STUDENT_NUMBER.
null_gender_records <- CredentialSupVars %>%
  filter(is.na(PSI_GENDER_CLEANED)) %>%
  select(ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE)

if (nrow(null_gender_records) > 0) {
  # Get genders from STP_Enrolment by PSI_CODE/STUDENT_NUMBER
  enrol_gender_by_stuid <- null_gender_records %>%
    inner_join(
      stp_enrolment %>%
        select(PSI_STUDENT_NUMBER, PSI_CODE, PSI_GENDER) %>%
        distinct(),
      by = c("PSI_STUDENT_NUMBER", "PSI_CODE")
    )

  # Find EPENs with multiple genders from this match
  multi_gender_stuid <- enrol_gender_by_stuid %>%
    count(ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE) %>%
    filter(n > 1)

  # Resolve multi-gender by most recent school year (same logic as 5c)
  if (nrow(multi_gender_stuid) > 0) {
    multi_stuid_max <- multi_gender_stuid %>%
      select(-n) %>%
      inner_join(
        CredentialSupVarsFromEnrolment %>%
          select(ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, PSI_GENDER,
                 PSI_SCHOOL_YEAR, PSI_ENROLMENT_SEQUENCE),
        by = c("ENCRYPTED_TRUE_PEN", "PSI_STUDENT_NUMBER", "PSI_CODE")
      ) %>%
      group_by(ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, PSI_GENDER) %>%
      summarise(
        MAX_PSI_SCHOOL_YEAR = max(PSI_SCHOOL_YEAR),
        MAX_PSI_ENROLMENT_SEQUENCE = max(PSI_ENROLMENT_SEQUENCE),
        .groups = "drop"
      )

    multi_stuid_overall_max <- multi_stuid_max %>%
      group_by(ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE) %>%
      summarise(
        MAX_MAX_YEAR = max(MAX_PSI_SCHOOL_YEAR),
        MAX_MAX_SEQ = max(MAX_PSI_ENROLMENT_SEQUENCE),
        .groups = "drop"
      )

    multi_stuid_resolved <- multi_stuid_overall_max %>%
      inner_join(
        multi_stuid_max,
        by = c("ENCRYPTED_TRUE_PEN", "PSI_STUDENT_NUMBER", "PSI_CODE",
               "MAX_MAX_YEAR" = "MAX_PSI_SCHOOL_YEAR",
               "MAX_MAX_SEQ" = "MAX_PSI_ENROLMENT_SEQUENCE")
      ) %>%
      select(ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, PSI_GENDER_TO_USE = PSI_GENDER) %>%
      distinct()
  } else {
    multi_stuid_resolved <- tibble(
      ENCRYPTED_TRUE_PEN = character(),
      PSI_STUDENT_NUMBER = character(),
      PSI_CODE = character(),
      PSI_GENDER_TO_USE = character()
    )
  }

  # Build final gender for null-EPEN records
  enrol_gender_final <- enrol_gender_by_stuid %>%
    left_join(multi_stuid_resolved,
              by = c("ENCRYPTED_TRUE_PEN", "PSI_STUDENT_NUMBER", "PSI_CODE")) %>%
    mutate(
      PSI_GENDER_CLEANED_FLAG = if_else(!is.na(PSI_GENDER_TO_USE), "Yes", NA_character_)
    ) %>%
    # Clean unknowns from the enrolment match
    mutate(
      PSI_GENDER_CLEANED = coalesce(PSI_GENDER_TO_USE, PSI_GENDER)
    ) %>%
    # If still unknown, keep as-is
    mutate(
      PSI_GENDER_CLEANED_FLAG = if_else(
        PSI_GENDER %in% c("U", "Unknown", "(Unspecified)") & is.na(PSI_GENDER_CLEANED_FLAG),
        "Yes", PSI_GENDER_CLEANED_FLAG
      ),
      PSI_GENDER_CLEANED = if_else(is.na(PSI_GENDER_CLEANED_FLAG), PSI_GENDER, PSI_GENDER_CLEANED)
    ) %>%
    select(PSI_STUDENT_NUMBER, PSI_CODE, PSI_GENDER_CLEANED, PSI_GENDER_CLEANED_FLAG) %>%
    distinct()

  # Apply to CredentialSupVars
  CredentialSupVars <- CredentialSupVars %>%
    left_join(
      enrol_gender_final %>% filter(PSI_GENDER_CLEANED_FLAG == "Yes"),
      by = c("PSI_STUDENT_NUMBER", "PSI_CODE"),
      suffix = c("", "_enrol")
    ) %>%
    mutate(
      PSI_GENDER_CLEANED = if_else(
        is.na(PSI_GENDER_CLEANED) & !is.na(PSI_GENDER_CLEANED_enrol),
        PSI_GENDER_CLEANED_enrol, PSI_GENDER_CLEANED
      )
    ) %>%
    select(-PSI_GENDER_CLEANED_enrol, -PSI_GENDER_CLEANED_FLAG_enrol)
}


# ******************************************************************************
# Section 6: Birthdate cleaning with LAST_SEEN_BIRTHDATE
# WHY: Fill missing birthdates from enrolment data, first by EPEN, then by
# PSI_CODE/STUDENT_NUMBER, and finally from LAST_SEEN_BIRTHDATE.
# ******************************************************************************

# Join birthdate by EPEN (non-blank EPENs)
CredentialSupVars <- CredentialSupVars %>%
  left_join(
    cred_sup_vars_birthdate_clean %>%
      filter(!is_blank(ENCRYPTED_TRUE_PEN)) %>%
      select(ENCRYPTED_TRUE_PEN, BD_CLEANED = PSI_BIRTHDATE_CLEANED,
             BD_CLEANED_D = PSI_BIRTHDATE_CLEANED_D) %>%
      distinct(),
    by = "ENCRYPTED_TRUE_PEN"
  ) %>%
  mutate(
    PSI_BIRTHDATE_CLEANED = coalesce(BD_CLEANED, PSI_BIRTHDATE_CLEANED),
    PSI_BIRTHDATE_CLEANED_D = coalesce(BD_CLEANED_D, PSI_BIRTHDATE_CLEANED_D)
  ) %>%
  select(-BD_CLEANED, -BD_CLEANED_D)

# Join birthdate by PSI_CODE/STUDENT_NUMBER (blank EPENs)
CredentialSupVars <- CredentialSupVars %>%
  left_join(
    cred_sup_vars_birthdate_clean %>%
      filter(is_blank(ENCRYPTED_TRUE_PEN)) %>%
      select(PSI_STUDENT_NUMBER, PSI_CODE, BD_CLEANED = PSI_BIRTHDATE_CLEANED,
             BD_CLEANED_D = PSI_BIRTHDATE_CLEANED_D) %>%
      distinct(),
    by = c("PSI_STUDENT_NUMBER", "PSI_CODE")
  ) %>%
  mutate(
    PSI_BIRTHDATE_CLEANED = if_else(
      is_blank(ENCRYPTED_TRUE_PEN) & !is_blank(PSI_CODE) & !is_blank(PSI_STUDENT_NUMBER),
      coalesce(BD_CLEANED, PSI_BIRTHDATE_CLEANED), PSI_BIRTHDATE_CLEANED
    ),
    PSI_BIRTHDATE_CLEANED_D = if_else(
      is_blank(ENCRYPTED_TRUE_PEN) & !is_blank(PSI_CODE) & !is_blank(PSI_STUDENT_NUMBER),
      coalesce(BD_CLEANED_D, PSI_BIRTHDATE_CLEANED_D), PSI_BIRTHDATE_CLEANED_D
    )
  ) %>%
  select(-BD_CLEANED, -BD_CLEANED_D)

# Get LAST_SEEN_BIRTHDATE from enrolment
last_seen_bd <- stp_enrolment %>%
  select(ID, LAST_SEEN_BIRTHDATE)

CredentialSupVarsFromEnrolment <- CredentialSupVarsFromEnrolment %>%
  left_join(last_seen_bd, by = c("ENROLMENTID" = "ID"))

# Apply LAST_SEEN_BIRTHDATE to CredentialSupVars
lsbd_by_epen <- CredentialSupVarsFromEnrolment %>%
  filter(!is.na(LAST_SEEN_BIRTHDATE)) %>%
  select(ENCRYPTED_TRUE_PEN, LAST_SEEN_BIRTHDATE) %>%
  distinct()

CredentialSupVars <- CredentialSupVars %>%
  left_join(lsbd_by_epen, by = "ENCRYPTED_TRUE_PEN") %>%
  mutate(
    PSI_BIRTHDATE_CLEANED = if_else(
      !is.na(LAST_SEEN_BIRTHDATE) & !is_blank(LAST_SEEN_BIRTHDATE) &
        (is.na(PSI_BIRTHDATE_CLEANED) | is_blank(PSI_BIRTHDATE_CLEANED)),
      LAST_SEEN_BIRTHDATE, PSI_BIRTHDATE_CLEANED
    )
  ) %>%
  select(-LAST_SEEN_BIRTHDATE)


# ******************************************************************************
# Section 7: Recreate Credential view with cleaned SupVars
# WHY: Rebuild the working credential dataset with cleaned SupVars (gender, birthdate)
# and with the exclusion filters applied (no developmental, no partial year).
# ******************************************************************************

credential <- stp_credential %>%
  inner_join(stp_cred_rec_type %>% select(ID, RECORDSTATUS, DROPCREDCATEGORY, DROPPARTIALYEAR),
             by = "ID") %>%
  filter(RECORDSTATUS == 0,
         is.na(DROPCREDCATEGORY),
         is.na(DROPPARTIALYEAR)) %>%
  inner_join(
    CredentialSupVars %>%
      select(ID, CREDENTIAL_AWARD_DATE_D, AGE_AT_GRAD, AGE_GROUP_AT_GRAD,
             PSI_AWARD_SCHOOL_YEAR, RECORD_TO_DELETE, LAST_DATE_HIGHEST_CRED,
             HIGHEST_CRED_BY_DATE, HIGHEST_CRED_BY_RANK, HIGHEST_CRED_BY_SCHOOL_YEAR,
             OUTCOMES_CRED, RESEARCH_UNIVERSITY, CREDENTIAL_AWARD_DATE_D_DELAYED,
             PSI_AWARD_SCHOOL_YEAR_DELAYED, PSI_BIRTHDATE_CLEANED,
             PSI_BIRTHDATE_CLEANED_D, PSI_GENDER_CLEANED),
    by = "ID"
  )


# ******************************************************************************
# Section 8: Age at graduation calculation
# WHY: Compute age at graduation from birthdate and award date. Also assign age
# group using the AgeGroupLookup table.
# ******************************************************************************

# Update birthdate_cleaned_D from birthdate_cleaned (date conversion)
credential <- credential %>%
  mutate(
    PSI_BIRTHDATE_CLEANED_D = if_else(
      !is.na(PSI_BIRTHDATE_CLEANED),
      as.Date(PSI_BIRTHDATE_CLEANED), PSI_BIRTHDATE_CLEANED_D
    )
  )

# Also update CredentialSupVars
CredentialSupVars <- CredentialSupVars %>%
  mutate(
    PSI_BIRTHDATE_CLEANED_D = if_else(
      !is.na(PSI_BIRTHDATE_CLEANED),
      as.Date(PSI_BIRTHDATE_CLEANED), PSI_BIRTHDATE_CLEANED_D
    )
  )

# Compute age at graduation using the "birthday not yet passed" correction
# WHY: SQL uses DATEDIFF + DATEADD to check if birthday has occurred in the award year.
# We use lubridate's %--% interval for correct age calculation.
age_group_lookup <- sch_tbl("AgeGroupLookup") %>% collect() |> rename_with(toupper)

credential <- credential %>%
  mutate(
    AGE_AT_GRAD = if_else(
      !is.na(PSI_BIRTHDATE_CLEANED_D) & !is_blank(PSI_BIRTHDATE_CLEANED),
      as.integer(lubridate::time_length(
        PSI_BIRTHDATE_CLEANED_D %--% CREDENTIAL_AWARD_DATE_D, "years"
      )),
      AGE_AT_GRAD
    )
  )

# Assign age group from lookup
# WHY: Map numeric age to an age group index based on lower/upper bounds.
credential <- credential %>%
  mutate(AGE_GROUP_AT_GRAD = NA_integer_)

for (i in seq_len(nrow(age_group_lookup))) {
  lb <- age_group_lookup$LOWERBOUND[i]
  ub <- age_group_lookup$UPPERBOUND[i]
  idx <- age_group_lookup$AGEINDEX[i]
  credential <- credential %>%
    mutate(
      AGE_GROUP_AT_GRAD = if_else(
        !is.na(AGE_AT_GRAD) & AGE_AT_GRAD >= lb & AGE_AT_GRAD <= ub,
        idx, AGE_GROUP_AT_GRAD
      )
    )
}

# Compute school year from award date
# WHY: School year spans Sep-Aug. If award month >= 9, it's the start of a new year pair.
credential <- credential %>%
  mutate(
    PSI_AWARD_SCHOOL_YEAR = if_else(
      is.na(PSI_AWARD_SCHOOL_YEAR),
      if_else(
        lubridate::month(CREDENTIAL_AWARD_DATE_D) >= 9,
        paste0(lubridate::year(CREDENTIAL_AWARD_DATE_D), "/",
               lubridate::year(CREDENTIAL_AWARD_DATE_D) + 1),
        paste0(lubridate::year(CREDENTIAL_AWARD_DATE_D) - 1, "/",
               lubridate::year(CREDENTIAL_AWARD_DATE_D))
      ),
      PSI_AWARD_SCHOOL_YEAR
    )
  )

# Sync back to CredentialSupVars
CredentialSupVars <- CredentialSupVars %>%
  left_join(
    credential %>% select(ID, CREDENTIAL_AWARD_DATE_D, AGE_AT_GRAD, AGE_GROUP_AT_GRAD, PSI_AWARD_SCHOOL_YEAR),
    by = "ID",
    suffix = c("", "_cred")
  ) %>%
  mutate(
    CREDENTIAL_AWARD_DATE_D = coalesce(CREDENTIAL_AWARD_DATE_D_cred, CREDENTIAL_AWARD_DATE_D),
    AGE_AT_GRAD = coalesce(AGE_AT_GRAD_cred, AGE_AT_GRAD),
    AGE_GROUP_AT_GRAD = coalesce(AGE_GROUP_AT_GRAD_cred, AGE_GROUP_AT_GRAD),
    PSI_AWARD_SCHOOL_YEAR = coalesce(PSI_AWARD_SCHOOL_YEAR_cred, PSI_AWARD_SCHOOL_YEAR)
  ) %>%
  select(-ends_with("_cred"))


# ******************************************************************************
# Section 9: Credential_Non_Dup creation (deduplication)
# WHY: Remove duplicate credential records — keep one row per unique combination
# of EPEN, PSI_CODE, program code, description, CIP, level, category, and award date.
# Also apply additional gender cleaning from STP_Enrolment.
# ******************************************************************************

# Try to fill missing gender from STP_Enrolment
enrol_gender <- stp_enrolment %>%
  filter(PSI_GENDER %in% c("Female", "Male", "Gender Diverse")) %>%
  select(ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, PSI_GENDER) %>%
  distinct()

credential <- credential %>%
  left_join(enrol_gender,
            by = c("ENCRYPTED_TRUE_PEN", "PSI_STUDENT_NUMBER", "PSI_CODE"),
            suffix = c("", "_enrol")) %>%
  mutate(
    PSI_GENDER_CLEANED = if_else(
      is_blank(PSI_GENDER_CLEANED) & !is.na(PSI_GENDER_enrol),
      PSI_GENDER_enrol, PSI_GENDER_CLEANED
    )
  ) %>%
  select(-PSI_GENDER_enrol)

# Create deduplication view — one row per unique credential combination, keeping max ID
credential_remove_dup <- credential %>%
  group_by(ENCRYPTED_TRUE_PEN, PSI_CODE, PSI_PROGRAM_CODE,
            PSI_CREDENTIAL_PROGRAM_DESCRIPTION, PSI_CREDENTIAL_CIP,
            PSI_CREDENTIAL_LEVEL, PSI_CREDENTIAL_CATEGORY, CREDENTIAL_AWARD_DATE_D) %>%
  summarise(ID = max(ID), .groups = "drop")

# Create Credential_Non_Dup by joining
Credential_Non_Dup <- credential %>%
  semi_join(credential_remove_dup, by = "ID") %>%
  select(ID, PSI_STUDENT_NUMBER, PSI_BIRTHDATE_CLEANED, PSI_GENDER_CLEANED,
         ENCRYPTED_TRUE_PEN, PSI_SCHOOL_YEAR, PSI_CODE, CREDENTIAL_AWARD_DATE,
         RECORDSTATUS, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
         PSI_CREDENTIAL_CIP, PSI_CREDENTIAL_LEVEL, PSI_CREDENTIAL_CATEGORY,
         CREDENTIAL_AWARD_DATE_D, AGE_AT_GRAD, AGE_GROUP_AT_GRAD,
         PSI_BIRTHDATE_CLEANED_D, PSI_AWARD_SCHOOL_YEAR, RECORD_TO_DELETE,
         LAST_DATE_HIGHEST_CRED, HIGHEST_CRED_BY_DATE, HIGHEST_CRED_BY_RANK,
         OUTCOMES_CRED, HIGHEST_CRED_BY_SCHOOL_YEAR, RESEARCH_UNIVERSITY)


# ******************************************************************************
# Section 10: Gender cleaning for Credential_Non_Dup
# WHY: Resolve multi-gender EPENs within Non_Dup (pick gender from most recent
# credential award date), then impute remaining missing genders proportionally.
# ******************************************************************************

# Find EPENs with multiple genders in Non_Dup
dup_gender_epens <- Credential_Non_Dup %>%
  select(ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, PSI_GENDER_CLEANED) %>%
  distinct() %>%
  count(ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE) %>%
  filter(n > 1)

if (nrow(dup_gender_epens) > 0) {
  # Pick gender from record with max award date
  dup_gender_max_date <- Credential_Non_Dup %>%
    semi_join(dup_gender_epens, by = "ENCRYPTED_TRUE_PEN") %>%
    group_by(ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE) %>%
    slice_max(CREDENTIAL_AWARD_DATE_D, n = 1, with_ties = FALSE) %>%
    ungroup() %>%
    select(ENCRYPTED_TRUE_PEN, PSI_GENDER_CLEANED) %>%
    distinct()

  # Apply resolved gender to all records with that EPEN
  Credential_Non_Dup <- Credential_Non_Dup %>%
    left_join(dup_gender_max_date, by = "ENCRYPTED_TRUE_PEN",
              suffix = c("", "_resolved")) %>%
    mutate(
      PSI_GENDER_CLEANED = if_else(
        PSI_GENDER_CLEANED != PSI_GENDER_CLEANED_resolved,
        PSI_GENDER_CLEANED_resolved, PSI_GENDER_CLEANED
      )
    ) %>%
    select(-PSI_GENDER_CLEANED_resolved)
}

# ---- Impute missing gender proportionally ----
# WHY: Records with blank/unknown gender are assigned Female/Male/Gender Diverse
# based on the observed gender proportions within each credential category.
# Original: Manual UPDATE TOP(N) queries after computing distribution.

no_gender <- Credential_Non_Dup %>%
  filter(is_blank(PSI_GENDER_CLEANED)) %>%
  select(ID, ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE,
         PSI_GENDER_CLEANED, PSI_CREDENTIAL_CATEGORY) %>%
  distinct()

if (nrow(no_gender) > 0) {
  # Compute gender distribution by credential category
  gender_dist <- Credential_Non_Dup %>%
    filter(!is_blank(PSI_GENDER_CLEANED)) %>%
    count(PSI_GENDER_CLEANED, PSI_CREDENTIAL_CATEGORY, name = "GENDERCOUNT")

  total_by_cat <- gender_dist %>%
    group_by(PSI_CREDENTIAL_CATEGORY) %>%
    summarise(TOTAL = sum(GENDERCOUNT), .groups = "drop")

  female_pct <- gender_dist %>%
    filter(PSI_GENDER_CLEANED == "Female") %>%
    left_join(total_by_cat, by = "PSI_CREDENTIAL_CATEGORY") %>%
    mutate(P = GENDERCOUNT / TOTAL) %>%
    select(PSI_CREDENTIAL_CATEGORY, P)

  male_pct <- gender_dist %>%
    filter(PSI_GENDER_CLEANED == "Male") %>%
    left_join(total_by_cat, by = "PSI_CREDENTIAL_CATEGORY") %>%
    mutate(P = GENDERCOUNT / TOTAL) %>%
    select(PSI_CREDENTIAL_CATEGORY, P)

  # Count nulls by category
  nulls_by_cat <- no_gender %>%
    count(PSI_CREDENTIAL_CATEGORY, name = "NULLCOUNT")

  # Compute number of females to assign per category
  top_nf <- female_pct %>%
    inner_join(nulls_by_cat, by = "PSI_CREDENTIAL_CATEGORY") %>%
    mutate(N = round(P * NULLCOUNT))

  top_nm <- male_pct %>%
    inner_join(nulls_by_cat, by = "PSI_CREDENTIAL_CATEGORY") %>%
    mutate(N = round(P * NULLCOUNT))

  # Assign genders: for each category, mark first N records as Female, etc.
  no_gender_unique <- no_gender %>%
    group_by(ENCRYPTED_TRUE_PEN, PSI_CREDENTIAL_CATEGORY) %>%
    slice(1) %>%
    ungroup()

  # Build assignment: for each category, assign Female to top_nf, Male to top_nm,
  # rest as Gender Diverse
  no_gender_assigned <- no_gender_unique %>%
    arrange(PSI_CREDENTIAL_CATEGORY, ENCRYPTED_TRUE_PEN) %>%
    group_by(PSI_CREDENTIAL_CATEGORY) %>%
    mutate(row_num = row_number()) %>%
    ungroup() %>%
    left_join(top_nf %>% select(PSI_CREDENTIAL_CATEGORY, NF = N),
              by = "PSI_CREDENTIAL_CATEGORY") %>%
    left_join(top_nm %>% select(PSI_CREDENTIAL_CATEGORY, NM = N),
              by = "PSI_CREDENTIAL_CATEGORY") %>%
    mutate(
      NF = coalesce(NF, 0),
      NM = coalesce(NM, 0),
      PSI_GENDER_CLEANED = case_when(
        row_num <= NF ~ "Female",
        row_num <= NF + NM ~ "Male",
        TRUE ~ "Gender Diverse"
      )
    ) %>%
    select(ENCRYPTED_TRUE_PEN, PSI_CREDENTIAL_CATEGORY, PSI_GENDER_CLEANED)

  # Apply to Credential_Non_Dup
  Credential_Non_Dup <- Credential_Non_Dup %>%
    left_join(no_gender_assigned,
              by = c("ENCRYPTED_TRUE_PEN", "PSI_CREDENTIAL_CATEGORY"),
              suffix = c("", "_imputed")) %>%
    mutate(
      PSI_GENDER_CLEANED = if_else(
        is_blank(PSI_GENDER_CLEANED), PSI_GENDER_CLEANED_imputed, PSI_GENDER_CLEANED
      )
    ) %>%
    select(-PSI_GENDER_CLEANED_imputed)
}


# ******************************************************************************
# Section 11: Credential ranking
# WHY: For EPENs with multiple credentials, identify the highest credential by
# date and by rank. Non-duplicated credentials (single per EPEN) get 'Yes' for both.
# Original: Multi-step process with views and temp tables, then R-based ranking.
# ******************************************************************************

credential_rank <- sch_tbl("CredentialRank") %>% collect() |> rename_with(toupper)

# EPENs with more than one credential (valid EPEN)
multi_cred_by_epen <- Credential_Non_Dup %>%
  filter(!is_blank(ENCRYPTED_TRUE_PEN)) %>%
  count(ENCRYPTED_TRUE_PEN) %>%
  filter(n > 1)

# PSI_CODE/STUDENT_NUMBER combos with more than one credential (null/blank EPEN)
multi_cred_by_stuid <- Credential_Non_Dup %>%
  filter(is_blank(ENCRYPTED_TRUE_PEN)) %>%
  count(ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER) %>%
  filter(n > 1)

# Build ranking dataset — all records for multi-credential EPENs or STUDENT_NUMBERs
ranking_records <- Credential_Non_Dup %>%
  inner_join(credential_rank %>% select(PSI_CREDENTIAL_CATEGORY, RANK),
             by = "PSI_CREDENTIAL_CATEGORY") %>%
  filter(
    (ENCRYPTED_TRUE_PEN %in% multi_cred_by_epen$ENCRYPTED_TRUE_PEN) |
      (is_blank(ENCRYPTED_TRUE_PEN) &
         paste0(ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER) %in%
         paste0(multi_cred_by_stuid$ENCRYPTED_TRUE_PEN, multi_cred_by_stuid$PSI_STUDENT_NUMBER))
  ) %>%
  select(ID, ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE,
         CREDENTIAL_AWARD_DATE_D, RANK)

# Compute highest by date and by rank using R (same as original)
res <- ranking_records %>%
  select(ID, ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE,
         CREDENTIAL_AWARD_DATE_D, RANK) %>%
  mutate(
    HIGHEST_CRED_BY_DATE = NA_character_,
    HIGHEST_CRED_BY_RANK = NA_character_
  )

# Highest by date: first record sorted by date desc, then rank
res <- res %>%
  group_by(ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER) %>%
  arrange(PSI_CODE, desc(CREDENTIAL_AWARD_DATE_D), RANK, .by_group = TRUE) %>%
  mutate(HIGHEST_CRED_BY_DATE = replace(HIGHEST_CRED_BY_DATE, 1, "Yes")) %>%
  ungroup()

# Highest by rank: first record sorted by rank, then date desc
res <- res %>%
  group_by(ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER) %>%
  arrange(PSI_CODE, RANK, desc(CREDENTIAL_AWARD_DATE_D), .by_group = TRUE) %>%
  mutate(HIGHEST_CRED_BY_RANK = replace(HIGHEST_CRED_BY_RANK, 1, "Yes")) %>%
  ungroup()

# Apply rankings to Credential_Non_Dup
ranking_result <- res %>%
  select(ID, HIGHEST_CRED_BY_DATE, HIGHEST_CRED_BY_RANK)

Credential_Non_Dup <- Credential_Non_Dup %>%
  left_join(ranking_result, by = "ID", suffix = c("", "_ranked")) %>%
  mutate(
    HIGHEST_CRED_BY_DATE = coalesce(HIGHEST_CRED_BY_DATE_ranked, HIGHEST_CRED_BY_DATE),
    HIGHEST_CRED_BY_RANK = coalesce(HIGHEST_CRED_BY_RANK_ranked, HIGHEST_CRED_BY_RANK)
  ) %>%
  select(-HIGHEST_CRED_BY_DATE_ranked, -HIGHEST_CRED_BY_RANK_ranked)

# Non-multi-credential records get 'Yes' for both
Credential_Non_Dup <- Credential_Non_Dup %>%
  mutate(
    HIGHEST_CRED_BY_DATE = if_else(
      !ID %in% ranking_result$ID & is.na(HIGHEST_CRED_BY_DATE),
      "Yes", HIGHEST_CRED_BY_DATE
    ),
    HIGHEST_CRED_BY_RANK = if_else(
      !ID %in% ranking_result$ID & is.na(HIGHEST_CRED_BY_RANK),
      "Yes", HIGHEST_CRED_BY_RANK
    )
  )


# ******************************************************************************
# Section 12: Age imputation
# WHY: Some credentials are missing age at graduation. We impute ages based on
# the observed age distribution within each gender/credential category.
# ******************************************************************************

cred_no_age <- Credential_Non_Dup %>%
  filter(is.na(AGE_AT_GRAD)) %>%
  select(ID, ENCRYPTED_TRUE_PEN, PSI_GENDER_CLEANED, PSI_AWARD_SCHOOL_YEAR,
         PSI_CREDENTIAL_CATEGORY, CREDENTIAL_AWARD_DATE_D)

cred_no_age_unique <- Credential_Non_Dup %>%
  filter(is.na(AGE_AT_GRAD), HIGHEST_CRED_BY_DATE == "Yes") %>%
  select(ID, ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE,
         PSI_GENDER_CLEANED, PSI_CREDENTIAL_CATEGORY, CREDENTIAL_AWARD_DATE_D,
         PSI_AWARD_SCHOOL_YEAR)

if (nrow(cred_no_age_unique) > 0) {
  # Count nulls per gender/category
  null_age_count <- cred_no_age_unique %>%
    count(PSI_GENDER_CLEANED, PSI_CREDENTIAL_CATEGORY, name = "NUMWITHNULLAGE")

  # Observed age distribution (from records with known age, highest by date only)
  age_dist <- Credential_Non_Dup %>%
    filter(!is.na(AGE_GROUP_AT_GRAD), HIGHEST_CRED_BY_DATE == "Yes") %>%
    count(PSI_GENDER_CLEANED, AGE_AT_GRAD, PSI_CREDENTIAL_CATEGORY, name = "NUMGRADS")

  # Compute proportions
  age_dist_props <- age_dist %>%
    group_by(PSI_GENDER_CLEANED, PSI_CREDENTIAL_CATEGORY) %>%
    mutate(P = NUMGRADS / sum(NUMGRADS)) %>%
    ungroup()

  # Join with null counts to get number to assign per age value
  age_assignments <- age_dist_props %>%
    inner_join(null_age_count, by = c("PSI_GENDER_CLEANED", "PSI_CREDENTIAL_CATEGORY")) %>%
    mutate(N = round(P * NUMWITHNULLAGE)) %>%
    arrange(PSI_GENDER_CLEANED, PSI_CREDENTIAL_CATEGORY, AGE_AT_GRAD)

  # Apply age imputation: for each gender/category, assign ages proportionally
  imputed_ages <- cred_no_age_unique %>%
    arrange(PSI_GENDER_CLEANED, PSI_CREDENTIAL_CATEGORY, ID) %>%
    group_by(PSI_GENDER_CLEANED, PSI_CREDENTIAL_CATEGORY) %>%
    mutate(row_num = row_number()) %>%
    ungroup()

  # Build cumulative age assignments
  age_assign_cumul <- age_assignments %>%
    arrange(PSI_GENDER_CLEANED, PSI_CREDENTIAL_CATEGORY, AGE_AT_GRAD) %>%
    group_by(PSI_GENDER_CLEANED, PSI_CREDENTIAL_CATEGORY) %>%
    mutate(cum_n = cumsum(N)) %>%
    ungroup() %>%
    select(PSI_GENDER_CLEANED, PSI_CREDENTIAL_CATEGORY, AGE_AT_GRAD, cum_n)

  # Assign age by matching row_num to cumulative thresholds
  imputed_ages <- imputed_ages %>%
    left_join(
      age_assign_cumul,
      by = c("PSI_GENDER_CLEANED", "PSI_CREDENTIAL_CATEGORY")
    ) %>%
    group_by(PSI_GENDER_CLEANED, PSI_CREDENTIAL_CATEGORY, ID) %>%
    filter(row_num <= first(cum_n)) %>%
    slice(1) %>%
    ungroup() %>%
    select(ID, AGE_AT_GRAD_imputed = AGE_AT_GRAD)

  # Apply to Credential_Non_Dup
  Credential_Non_Dup <- Credential_Non_Dup %>%
    left_join(imputed_ages, by = "ID") %>%
    mutate(
      AGE_AT_GRAD = if_else(is.na(AGE_AT_GRAD), AGE_AT_GRAD_imputed, AGE_AT_GRAD)
    ) %>%
    select(-AGE_AT_GRAD_imputed)

  # For remaining nulls, assign random age 19-54
  remaining_null <- Credential_Non_Dup %>%
    filter(is.na(AGE_AT_GRAD))

  if (nrow(remaining_null) > 0) {
    set.seed(42)
    Credential_Non_Dup <- Credential_Non_Dup %>%
      mutate(
        AGE_AT_GRAD = if_else(
          is.na(AGE_AT_GRAD),
          sample(19:54, n(), replace = TRUE),
          AGE_AT_GRAD
        )
      )
  }
}

# Reassign age groups after imputation
Credential_Non_Dup <- Credential_Non_Dup %>%
  mutate(AGE_GROUP_AT_GRAD = NA_integer_)

for (i in seq_len(nrow(age_group_lookup))) {
  lb <- age_group_lookup$LOWERBOUND[i]
  ub <- age_group_lookup$UPPERBOUND[i]
  idx <- age_group_lookup$AGEINDEX[i]
  Credential_Non_Dup <- Credential_Non_Dup %>%
    mutate(
      AGE_GROUP_AT_GRAD = if_else(
        !is.na(AGE_AT_GRAD) & AGE_AT_GRAD >= lb & AGE_AT_GRAD <= ub,
        idx, AGE_GROUP_AT_GRAD
      )
    )
}


# ******************************************************************************
# Section 13: Visa status cleaning
# WHY: Fill missing visa status in Credential_Non_Dup from CredentialSupVarsFromEnrolment
# using progressively looser matching criteria (all fields → partial fields → EPEN+code+year).
# ******************************************************************************

# Start with visa status from CredentialSupVars
visa_from_supvars <- CredentialSupVars %>%
  select(ID, PSI_VISA_STATUS)

# Get visa from enrolment with multiple join strategies
# WHY: Enrolment records may match on different field combinations. We try the most
# specific match first, then fall back to broader matches.

# Strategy 1: Match on all fields (EPEN, code, student_number, program_code, description, school_year)
visa_enrol_full <- CredentialSupVarsFromEnrolment %>%
  select(ENCRYPTED_TRUE_PEN, PSI_CODE, PSI_STUDENT_NUMBER,
         PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
         PSI_SCHOOL_YEAR, PSI_VISA_STATUS) %>%
  distinct()

# Build Non_Dup visa cleaning
cred_nd_visa <- Credential_Non_Dup %>%
  left_join(visa_from_supvars, by = "ID", suffix = c("", "_sv")) %>%
  mutate(PSI_VISA_STATUS = PSI_VISA_STATUS_sv) %>%
  select(-PSI_VISA_STATUS_sv)

# Fill from enrolment — first by full match
cred_nd_visa <- cred_nd_visa %>%
  left_join(
    visa_enrol_full %>%
      filter(!is.na(PSI_VISA_STATUS)) %>%
      rename(VISA_ENROL = PSI_VISA_STATUS),
    by = c("ENCRYPTED_TRUE_PEN", "PSI_CODE", "PSI_STUDENT_NUMBER",
           "PSI_PROGRAM_CODE", "PSI_CREDENTIAL_PROGRAM_DESCRIPTION",
           "PSI_SCHOOL_YEAR")
  ) %>%
  mutate(PSI_VISA_STATUS = coalesce(PSI_VISA_STATUS, VISA_ENROL)) %>%
  select(-VISA_ENROL)

# Strategy 2: Match on EPEN + code + student_number + school_year (for remaining nulls)
cred_nd_visa <- cred_nd_visa %>%
  left_join(
    CredentialSupVarsFromEnrolment %>%
      filter(!is.na(PSI_VISA_STATUS)) %>%
      select(ENCRYPTED_TRUE_PEN, PSI_CODE, PSI_STUDENT_NUMBER, PSI_SCHOOL_YEAR, PSI_VISA_STATUS) %>%
      distinct() %>%
      rename(VISA_ENROL2 = PSI_VISA_STATUS),
    by = c("ENCRYPTED_TRUE_PEN", "PSI_CODE", "PSI_STUDENT_NUMBER", "PSI_SCHOOL_YEAR")
  ) %>%
  mutate(PSI_VISA_STATUS = if_else(
    is.na(PSI_VISA_STATUS), VISA_ENROL2, PSI_VISA_STATUS
  )) %>%
  select(-VISA_ENROL2)

# Update CredentialSupVars from the cleaned Non_Dup visa
CredentialSupVars <- CredentialSupVars %>%
  left_join(
    cred_nd_visa %>% select(ID, PSI_VISA_STATUS_ND = PSI_VISA_STATUS),
    by = "ID"
  ) %>%
  mutate(
    PSI_VISA_STATUS = if_else(
      is.na(PSI_VISA_STATUS) | PSI_VISA_STATUS %in% c("", " ", "(Unspecified)"),
      PSI_VISA_STATUS_ND, PSI_VISA_STATUS
    )
  ) %>%
  select(-PSI_VISA_STATUS_ND)

# Update Credential_Non_Dup
Credential_Non_Dup <- Credential_Non_Dup %>%
  left_join(
    CredentialSupVars %>% select(ID, PSI_VISA_STATUS_SV = PSI_VISA_STATUS),
    by = "ID"
  ) %>%
  mutate(PSI_VISA_STATUS = PSI_VISA_STATUS_SV) %>%
  select(-PSI_VISA_STATUS_SV)


# ******************************************************************************
# Section 14: Delay date calculation
# WHY: Some credentials are superseded by later credentials within a time window.
# The "delay date" is the date of the later credential, used to shift the award
# date for projection purposes. Time windows vary by credential category.
# ******************************************************************************

# Build concatenated ID for grouping
# WHY: EPEN is the primary identifier; for null/blank EPENs, use PSI_CODE + STUDENT_NUMBER.
Credential_Non_Dup <- Credential_Non_Dup %>%
  mutate(
    CONCATENATED_ID = if_else(
      !is_blank(ENCRYPTED_TRUE_PEN), ENCRYPTED_TRUE_PEN,
      paste0(PSI_STUDENT_NUMBER, PSI_CODE)
    )
  )

# Build tblCredential_HighestRank — highest-ranked credentials with SupVars
tblCred_HighestRank <- Credential_Non_Dup %>%
  filter(HIGHEST_CRED_BY_RANK == "Yes") %>%
  inner_join(
    CredentialSupVars %>%
      select(ID, CREDENTIAL_AWARD_DATE_D_DELAYED, PSI_AWARD_SCHOOL_YEAR_DELAYED, PSI_VISA_STATUS),
    by = "ID"
  )

# Find later-awarded credentials for the same concatenated_id
# WHY: For each highest-rank credential, find other credentials with the same ID
# group that were awarded later. Apply time windows by credential category.
later_awarded <- Credential_Non_Dup %>%
  inner_join(
    credential_rank %>% select(PSI_CREDENTIAL_CATEGORY, RANK),
    by = "PSI_CREDENTIAL_CATEGORY"
  ) %>%
  inner_join(
    tblCred_HighestRank %>%
      select(HID = ID, CONCATENATED_ID, HIGHEST_AWARD_DATE = CREDENTIAL_AWARD_DATE_D),
    by = "CONCATENATED_ID"
  ) %>%
  filter(CREDENTIAL_AWARD_DATE_D > HIGHEST_AWARD_DATE) %>%
  rename(LID = ID, LATER_AWARD_DATE = CREDENTIAL_AWARD_DATE_D)

# Apply time windows per credential category
# WHY: Not all later credentials count as delays. Different categories have different
# windows (e.g., certificates: 18 months, diplomas: 30 months, degrees: unlimited).
later_awarded_filtered <- later_awarded %>%
  mutate(
    MONTHS_DIFF = as.numeric(difftime(LATER_AWARD_DATE, HIGHEST_AWARD_DATE, units = "days")) / 30.44
  ) %>%
  filter(
    PSI_CREDENTIAL_CATEGORY %in% c("APPRENTICESHIP", "BACHELORS DEGREE", "FIRST PROFESSIONAL DEGREE") |
      (PSI_CREDENTIAL_CATEGORY %in% c("ADVANCED DIPLOMA", "ADVANCED CERTIFICATE") & MONTHS_DIFF <= 36) |
      (PSI_CREDENTIAL_CATEGORY == "ASSOCIATE DEGREE" & MONTHS_DIFF <= 18) |
      (PSI_CREDENTIAL_CATEGORY == "CERTIFICATE" & MONTHS_DIFF <= 18) |
      (PSI_CREDENTIAL_CATEGORY == "DIPLOMA" & MONTHS_DIFF <= 30) |
      (PSI_CREDENTIAL_CATEGORY == "MASTERS DEGREE" & MONTHS_DIFF <= 30) |
      (PSI_CREDENTIAL_CATEGORY == "GRADUATE CERTIFICATE" & MONTHS_DIFF <= 18) |
      (PSI_CREDENTIAL_CATEGORY == "GRADUATE DIPLOMA" & MONTHS_DIFF <= 30) |
      (PSI_CREDENTIAL_CATEGORY == "POST-DEGREE CERTIFICATE" & MONTHS_DIFF <= 18) |
      (PSI_CREDENTIAL_CATEGORY == "POST-DEGREE DIPLOMA" & MONTHS_DIFF <= 30)
  )

# Find the max later award date per concatenated_id
delay_effect <- later_awarded_filtered %>%
  group_by(CONCATENATED_ID) %>%
  slice_max(LATER_AWARD_DATE, n = 1, with_ties = FALSE) %>%
  ungroup() %>%
  select(LID, HID, CONCATENATED_ID, LATER_AWARD_DATE, PSI_AWARD_SCHOOL_YEAR)

# Update delay dates in tblCred_HighestRank
tblCred_HighestRank <- tblCred_HighestRank %>%
  left_join(
    delay_effect %>% select(HID, DELAY_DATE = LATER_AWARD_DATE, DELAY_YEAR = PSI_AWARD_SCHOOL_YEAR),
    by = c("ID" = "HID")
  ) %>%
  mutate(
    CREDENTIAL_AWARD_DATE_D_DELAYED = coalesce(DELAY_DATE, CREDENTIAL_AWARD_DATE_D_DELAYED),
    PSI_AWARD_SCHOOL_YEAR_DELAYED = coalesce(DELAY_YEAR, PSI_AWARD_SCHOOL_YEAR_DELAYED)
  ) %>%
  select(-DELAY_DATE, -DELAY_YEAR)

# Default: if no delay date, use original award date
tblCred_HighestRank <- tblCred_HighestRank %>%
  mutate(
    CREDENTIAL_AWARD_DATE_D_DELAYED = if_else(
      is.na(CREDENTIAL_AWARD_DATE_D_DELAYED), CREDENTIAL_AWARD_DATE_D,
      CREDENTIAL_AWARD_DATE_D_DELAYED
    ),
    PSI_AWARD_SCHOOL_YEAR_DELAYED = if_else(
      is.na(PSI_AWARD_SCHOOL_YEAR_DELAYED), PSI_AWARD_SCHOOL_YEAR,
      PSI_AWARD_SCHOOL_YEAR_DELAYED
    )
  )

# Apply delay dates back to Credential_Non_Dup
Credential_Non_Dup <- Credential_Non_Dup %>%
  mutate(
    CREDENTIAL_AWARD_DATE_D_DELAYED = NA_Date_,
    PSI_AWARD_SCHOOL_YEAR_DELAYED = NA_character_
  ) %>%
  left_join(
    tblCred_HighestRank %>%
      filter(!is.na(CREDENTIAL_AWARD_DATE_D_DELAYED)) %>%
      select(ID, DELAY_D = CREDENTIAL_AWARD_DATE_D_DELAYED,
             DELAY_YR = PSI_AWARD_SCHOOL_YEAR_DELAYED),
    by = "ID"
  ) %>%
  mutate(
    CREDENTIAL_AWARD_DATE_D_DELAYED = coalesce(DELAY_D, CREDENTIAL_AWARD_DATE_D),
    PSI_AWARD_SCHOOL_YEAR_DELAYED = coalesce(DELAY_YR, PSI_AWARD_SCHOOL_YEAR)
  ) %>%
  select(-DELAY_D, -DELAY_YR)


# ******************************************************************************
# Section 15: Research university + Outcome credential flags
# WHY: Flag credentials from research universities (used for DACSO exclusion) and
# assign outcome credential labels for categorization.
# ******************************************************************************

research_unis <- c("SFU", "UBC", "UBCV", "UBCO", "UNBC", "UVIC", "RRU")

Credential_Non_Dup <- Credential_Non_Dup %>%
  mutate(RESEARCH_UNIVERSITY = if_else(PSI_CODE %in% research_unis, 1L, NA_integer_))

outcome_credential <- sch_tbl("OutcomeCredential") %>% collect() |> rename_with(toupper)

Credential_Non_Dup <- Credential_Non_Dup %>%
  left_join(
    outcome_credential %>% select(PSI_CREDENTIAL_CATEGORY, OUTCOMES_CRED_OUT = OUTCOMES_CRED),
    by = "PSI_CREDENTIAL_CATEGORY"
  ) %>%
  mutate(OUTCOMES_CRED = OUTCOMES_CRED_OUT) %>%
  select(-OUTCOMES_CRED_OUT)


# ******************************************************************************
# Section 16: Final distributions
# WHY: Build credential count tables by year/age group/credential category with
# various filters (domestic, exclude CIPs, exclude RU+DACSO). These feed into
# the graduate projection model (04) and program projections (06).
# ******************************************************************************

# Build base table from HighestRank joined with AgeGroupLookup and Credential_Non_Dup
# for FINAL_CIP_CLUSTER_CODE access
base_dist <- tblCred_HighestRank %>%
  inner_join(
    age_group_lookup %>% select(AGEINDEX, AGEGROUP),
    by = c("AGE_GROUP_AT_GRAD" = "AGEINDEX")
  )

base_dist_with_cip <- base_dist %>%
  left_join(
    Credential_Non_Dup %>%
      select(ID, FINAL_CIP_CLUSTER_CODE, FINAL_CIP_CODE_4, RESEARCH_UNIVERSITY,
             OUTCOMES_CRED, PSI_VISA_STATUS),
    by = "ID"
  )

# Helper function to build a Credential_By_Year table
# WHY: All 8 distribution tables follow the same pattern: filter + group_by + count.
# This helper reduces code duplication.
build_cred_by_year <- function(data, group_vars, filter_expr = TRUE) {
  data %>%
    filter({{ filter_expr }}) %>%
    group_by(across(all_of(group_vars))) %>%
    summarise(COUNT = n(), .groups = "drop")
}

# Common filters
no_apprenticeship <- expr(PSI_CREDENTIAL_CATEGORY != "APPRENTICESHIP")
is_domestic <- expr(PSI_VISA_STATUS == "DOMESTIC" | is.na(PSI_VISA_STATUS))
exclude_cip_09_10 <- expr(FINAL_CIP_CLUSTER_CODE != "09" & FINAL_CIP_CLUSTER_CODE != "10")
exclude_ru_dacso <- expr(
  (RESEARCH_UNIVERSITY == 1 & OUTCOMES_CRED != "DACSO") | is.na(RESEARCH_UNIVERSITY)
)

# 1. Credential_By_Year_AgeGroup
Credential_By_Year_AgeGroup <- base_dist %>%
  filter(PSI_CREDENTIAL_CATEGORY != "APPRENTICESHIP") %>%
  mutate(EXPR1 = paste0(PSI_CREDENTIAL_CATEGORY, AGEGROUP)) %>%
  count(AGEGROUP, PSI_CREDENTIAL_CATEGORY, EXPR1, PSI_AWARD_SCHOOL_YEAR_DELAYED, name = "COUNT") %>%
  arrange(AGEGROUP, PSI_CREDENTIAL_CATEGORY, PSI_AWARD_SCHOOL_YEAR_DELAYED)

# 2. Credential_By_Year_AgeGroup_Exclude_CIPs
Credential_By_Year_AgeGroup_Exclude_CIPs <- base_dist_with_cip %>%
  filter(PSI_CREDENTIAL_CATEGORY != "APPRENTICESHIP",
         FINAL_CIP_CLUSTER_CODE != "09", FINAL_CIP_CLUSTER_CODE != "10") %>%
  mutate(EXPR1 = paste0(PSI_CREDENTIAL_CATEGORY, AGEGROUP)) %>%
  count(AGEGROUP, PSI_CREDENTIAL_CATEGORY, EXPR1, PSI_AWARD_SCHOOL_YEAR_DELAYED, name = "COUNT") %>%
  arrange(AGEGROUP, PSI_CREDENTIAL_CATEGORY, PSI_AWARD_SCHOOL_YEAR_DELAYED)

# 3. Credential_By_Year_AgeGroup_Domestic
Credential_By_Year_AgeGroup_Domestic <- base_dist %>%
  filter(PSI_CREDENTIAL_CATEGORY != "APPRENTICESHIP",
         PSI_VISA_STATUS == "DOMESTIC" | is.na(PSI_VISA_STATUS)) %>%
  mutate(EXPR1 = paste0(PSI_CREDENTIAL_CATEGORY, AGEGROUP)) %>%
  count(AGEGROUP, PSI_CREDENTIAL_CATEGORY, EXPR1, PSI_AWARD_SCHOOL_YEAR_DELAYED, name = "COUNT") %>%
  arrange(AGEGROUP, PSI_CREDENTIAL_CATEGORY, PSI_AWARD_SCHOOL_YEAR_DELAYED)

# 4. Credential_By_Year_AgeGroup_Domestic_Exclude_CIPs
Credential_By_Year_AgeGroup_Domestic_Exclude_CIPs <- base_dist_with_cip %>%
  filter(PSI_CREDENTIAL_CATEGORY != "APPRENTICESHIP",
         (PSI_VISA_STATUS == "DOMESTIC" | is.na(PSI_VISA_STATUS)),
         FINAL_CIP_CLUSTER_CODE != "09", FINAL_CIP_CLUSTER_CODE != "10") %>%
  mutate(EXPR1 = paste0(PSI_CREDENTIAL_CATEGORY, AGEGROUP)) %>%
  count(AGEGROUP, PSI_CREDENTIAL_CATEGORY, EXPR1, PSI_AWARD_SCHOOL_YEAR_DELAYED, name = "COUNT") %>%
  arrange(AGEGROUP, PSI_CREDENTIAL_CATEGORY, PSI_AWARD_SCHOOL_YEAR_DELAYED)

# 5. Credential_By_Year_AgeGroup_Domestic_Exclude_RU_DACSO
Credential_By_Year_AgeGroup_Domestic_Exclude_RU_DACSO <- base_dist_with_cip %>%
  filter(PSI_CREDENTIAL_CATEGORY != "APPRENTICESHIP",
         (PSI_VISA_STATUS == "DOMESTIC" | is.na(PSI_VISA_STATUS)),
         (RESEARCH_UNIVERSITY == 1 & OUTCOMES_CRED != "DACSO") | is.na(RESEARCH_UNIVERSITY)) %>%
  mutate(EXPR1 = paste0(PSI_CREDENTIAL_CATEGORY, AGEGROUP)) %>%
  count(AGEGROUP, PSI_CREDENTIAL_CATEGORY, EXPR1, PSI_AWARD_SCHOOL_YEAR_DELAYED, name = "COUNT") %>%
  arrange(AGEGROUP, PSI_CREDENTIAL_CATEGORY, PSI_AWARD_SCHOOL_YEAR_DELAYED)

# 6. Credential_By_Year_CIP4_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs
Credential_By_Year_CIP4_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs <- base_dist_with_cip %>%
  filter(PSI_CREDENTIAL_CATEGORY != "APPRENTICESHIP",
         (PSI_VISA_STATUS == "DOMESTIC" | is.na(PSI_VISA_STATUS)),
         ((RESEARCH_UNIVERSITY == 1 & OUTCOMES_CRED != "DACSO") | is.na(RESEARCH_UNIVERSITY)),
         FINAL_CIP_CLUSTER_CODE != "09", FINAL_CIP_CLUSTER_CODE != "10") %>%
  mutate(EXPR1 = paste0(PSI_CREDENTIAL_CATEGORY, AGEGROUP)) %>%
  count(AGEGROUP, PSI_CREDENTIAL_CATEGORY, EXPR1, FINAL_CIP_CODE_4,
        PSI_AWARD_SCHOOL_YEAR_DELAYED, name = "COUNT") %>%
  arrange(AGEGROUP, PSI_CREDENTIAL_CATEGORY, FINAL_CIP_CODE_4, PSI_AWARD_SCHOOL_YEAR_DELAYED)

# 7. Credential_By_Year_CIP4_Gender_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs
Credential_By_Year_CIP4_Gender_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs <- base_dist_with_cip %>%
  filter(PSI_CREDENTIAL_CATEGORY != "APPRENTICESHIP",
         (PSI_VISA_STATUS == "DOMESTIC" | is.na(PSI_VISA_STATUS)),
         ((RESEARCH_UNIVERSITY == 1 & OUTCOMES_CRED != "DACSO") | is.na(RESEARCH_UNIVERSITY)),
         FINAL_CIP_CLUSTER_CODE != "09", FINAL_CIP_CLUSTER_CODE != "10") %>%
  mutate(EXPR1 = paste0(PSI_CREDENTIAL_CATEGORY, AGEGROUP, PSI_GENDER_CLEANED)) %>%
  count(PSI_GENDER_CLEANED, AGEGROUP, PSI_CREDENTIAL_CATEGORY, EXPR1, FINAL_CIP_CODE_4,
        PSI_AWARD_SCHOOL_YEAR_DELAYED, name = "COUNT") %>%
  arrange(AGEGROUP, PSI_CREDENTIAL_CATEGORY, PSI_AWARD_SCHOOL_YEAR_DELAYED, desc(PSI_GENDER_CLEANED))

# 8. Credential_By_Year_Gender_AgeGroup_Domestic_Exclude_CIPs
Credential_By_Year_Gender_AgeGroup_Domestic_Exclude_CIPs <- base_dist_with_cip %>%
  filter(PSI_CREDENTIAL_CATEGORY != "APPRENTICESHIP",
         (PSI_VISA_STATUS == "DOMESTIC" | is.na(PSI_VISA_STATUS)),
         FINAL_CIP_CLUSTER_CODE != "09", FINAL_CIP_CLUSTER_CODE != "10") %>%
  mutate(EXPR1 = paste0(PSI_CREDENTIAL_CATEGORY, AGEGROUP, PSI_GENDER_CLEANED)) %>%
  count(PSI_GENDER_CLEANED, AGEGROUP, PSI_CREDENTIAL_CATEGORY, EXPR1,
        PSI_AWARD_SCHOOL_YEAR_DELAYED, name = "COUNT") %>%
  arrange(AGEGROUP, PSI_CREDENTIAL_CATEGORY, PSI_AWARD_SCHOOL_YEAR_DELAYED, desc(PSI_GENDER_CLEANED))

# 9. Credential_By_Year_Gender_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs
Credential_By_Year_Gender_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs <- base_dist_with_cip %>%
  filter(PSI_CREDENTIAL_CATEGORY != "APPRENTICESHIP",
         (PSI_VISA_STATUS == "DOMESTIC" | is.na(PSI_VISA_STATUS)),
         ((RESEARCH_UNIVERSITY == 1 & OUTCOMES_CRED != "DACSO") | is.na(RESEARCH_UNIVERSITY)),
         FINAL_CIP_CLUSTER_CODE != "09", FINAL_CIP_CLUSTER_CODE != "10") %>%
  mutate(EXPR1 = paste0(PSI_CREDENTIAL_CATEGORY, AGEGROUP, PSI_GENDER_CLEANED)) %>%
  count(PSI_GENDER_CLEANED, AGEGROUP, PSI_CREDENTIAL_CATEGORY, EXPR1,
        PSI_AWARD_SCHOOL_YEAR_DELAYED, name = "COUNT") %>%
  arrange(AGEGROUP, PSI_CREDENTIAL_CATEGORY, PSI_AWARD_SCHOOL_YEAR_DELAYED, desc(PSI_GENDER_CLEANED))

# 10. Checking table
Checking_Excluding_RU_DACSO_Variables <- Credential_Non_Dup %>%
  filter(RESEARCH_UNIVERSITY == 1, OUTCOMES_CRED == "DACSO",
         PSI_AWARD_SCHOOL_YEAR_DELAYED == "2018/2019") %>%
  count(RESEARCH_UNIVERSITY, PSI_CODE, OUTCOMES_CRED, PSI_CREDENTIAL_CATEGORY,
        PSI_AWARD_SCHOOL_YEAR_DELAYED, name = "EXPR1") %>%
  arrange(OUTCOMES_CRED, RESEARCH_UNIVERSITY)


# ******************************************************************************
# Section 17: Write all output tables to database
# WHY: Persist results for downstream scripts. CredentialSupVars and
# Credential_Non_Dup are the primary outputs used by 02a-02b pipeline scripts.
# ******************************************************************************

write_schema_table <- function(name, data) {
  dbWriteTable(con, SQL(glue::glue('"{my_schema}"."{name}"')), data, overwrite = TRUE)
}

# Write main tables
write_schema_table("CredentialSupVars", CredentialSupVars)
write_schema_table("CredentialSupVarsFromEnrolment", CredentialSupVarsFromEnrolment)
write_schema_table("Credential_Non_Dup", Credential_Non_Dup)
write_schema_table("tblCredential_HighestRank", tblCred_HighestRank)

# Update STP_Credential_Record_Type with exclusion flags
# KEPT AS SQL: ALTER TABLE (DDL) and UPDATE on existing table
dbExecute(con, glue::glue(
  "IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('[{my_schema}].[STP_Credential_Record_Type]') AND name = 'DropCredCategory') ",
  "ALTER TABLE [{my_schema}].[STP_Credential_Record_Type] ADD DropCredCategory NVARCHAR(50) NULL"
))
dbExecute(con, glue::glue(
  "UPDATE [{my_schema}].[STP_Credential_Record_Type] SET DropCredCategory = NULL"
))
for (id in cred_ids_to_drop$ID) {
  dbExecute(con, glue::glue(
    "UPDATE [{my_schema}].[STP_Credential_Record_Type] SET DropCredCategory = 'Yes' WHERE ID = {id}"
  ))
}

dbExecute(con, glue::glue(
  "IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('[{my_schema}].[STP_Credential_Record_Type]') AND name = 'DropPartialYear') ",
  "ALTER TABLE [{my_schema}].[STP_Credential_Record_Type] ADD DropPartialYear NVARCHAR(50) NULL"
))
dbExecute(con, glue::glue(
  "UPDATE [{my_schema}].[STP_Credential_Record_Type] SET DropPartialYear = NULL"
))
for (id in cred_ids_partial$ID) {
  dbExecute(con, glue::glue(
    "UPDATE [{my_schema}].[STP_Credential_Record_Type] SET DropPartialYear = 'Yes' WHERE ID = {id}"
  ))
}

# Write distribution tables
write_schema_table("Credential_By_Year_AgeGroup", Credential_By_Year_AgeGroup)
write_schema_table("Credential_By_Year_AgeGroup_Exclude_CIPs", Credential_By_Year_AgeGroup_Exclude_CIPs)
write_schema_table("Credential_By_Year_AgeGroup_Domestic", Credential_By_Year_AgeGroup_Domestic)
write_schema_table("Credential_By_Year_AgeGroup_Domestic_Exclude_CIPs", Credential_By_Year_AgeGroup_Domestic_Exclude_CIPs)
write_schema_table("Credential_By_Year_AgeGroup_Domestic_Exclude_RU_DACSO", Credential_By_Year_AgeGroup_Domestic_Exclude_RU_DACSO)
write_schema_table("Credential_By_Year_CIP4_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs", Credential_By_Year_CIP4_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs)
write_schema_table("Credential_By_Year_CIP4_Gender_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs", Credential_By_Year_CIP4_Gender_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs)
write_schema_table("Credential_By_Year_Gender_AgeGroup_Domestic_Exclude_CIPs", Credential_By_Year_Gender_AgeGroup_Domestic_Exclude_CIPs)
write_schema_table("Credential_By_Year_Gender_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs", Credential_By_Year_Gender_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs)
write_schema_table("Checking_Excluding_RU_DACSO_Variables", Checking_Excluding_RU_DACSO_Variables)

# International flag update
# KEPT AS SQL: UPDATE with JOIN on existing table
dbExecute(con, glue::glue(
  "UPDATE [{my_schema}].[CredentialSupVars] ",
  "SET International_Include_Flag = [{my_schema}].[tbl_CredentialHighestRank_International].[International_Include_Flag] ",
  "FROM [{my_schema}].[CredentialSupVars] ",
  "INNER JOIN [{my_schema}].[tbl_CredentialHighestRank_International] ",
  "ON [{my_schema}].[CredentialSupVars].[ID] = [{my_schema}].[tbl_CredentialHighestRank_International].[ID];"
))


# ---- Clean up ----
dbDisconnect(con)
