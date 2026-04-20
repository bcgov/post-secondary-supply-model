# Enrolment Analysis — dplyr Translation
# Original: R/01d-enrolment-analysis.R
#
# Pipeline context:
#   Processes STP enrolment data to create the MinEnrolment working dataset used by
#   downstream graduate and program projection scripts. The main tasks are:
#     1. Build MinEnrolment from filtered STP enrolment records
#     2. Compute age at enrolment from birthdate/start date
#     3. Clean and impute gender (Credential update, duplicate resolution, proportional)
#     4. Impute missing ages using proportional sampling
#     5. Produce final enrolment distribution summaries
#
# Input tables:
#   - STP_Enrolment — preprocessed enrolment data (from 01a)
#   - STP_Enrolment_Record_Type — record classification (MinEnrolment, FirstEnrolment, RecordStatus)
#   - AgeGroupLookup — age group range definitions (AgeIndex, AgeGroup, LowerBound, UpperBound)
#   - Credential — credential view with cleaned gender (from 01b/01c)
#
# Output:
#   - MinEnrolment (table) — filtered enrolment records with cleaned age/gender
#   - qry09c_MinEnrolment — enrolment counts by gender/age group/year
#   - qry09c_MinEnrolment_Domestic — domestic-only enrolment counts
#   - qry09c_MinEnrolment_by_Credential_and_CIP_Code — counts by credential/CIP

library(tidyverse)
library(odbc)
library(DBI)
set.seed(123456)

# ---- Configure LAN Paths and DB Connection -----
lan <- config::get("lan")
db_config <- config::get("decimal")
my_schema <- config::get("myschema")
db_schema <- config::get("dbschema")

con <- dbConnect(odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")

# Helper: reference a table in the user's schema
sch_tbl <- function(name) {
  tbl(con, dbplyr::in_schema(my_schema, name))
}

# Helper: look up age group index from age value using the AgeGroupLookup table.
# WHY: SQL uses CROSS JOIN with range condition (LowerBound <= age <= UpperBound).
# In dplyr, we vectorize this by matching each age against the lookup ranges.
lookup_age_group <- function(age, age_lookup) {
  sapply(age, function(a) {
    if (is.na(a)) return(NA_real_)
    idx <- which(age_lookup$LOWERBOUND <= a & age_lookup$UPPERBOUND >= a)
    if (length(idx) == 0) return(NA_real_)
    age_lookup$AGEINDEX[idx[1]]
  })
}

# Helper: check if a gender value represents "unknown"
is_unknown_gender <- function(g) {
  is.na(g) | g %in% c("", "U", "Unknown", "(Unspecified)")
}

# Helper: compute age at a date from birthdate.
# WHY: Equivalent to SQL's datediff(year,bday,start) with birthday-has-occurred check.
# The SQL adds years to bday and checks if it exceeds start; if so, subtract 1.
compute_age_at_date <- function(birth_date, start_date) {
  birth_lt <- as.POSIXlt(birth_date)
  start_lt <- as.POSIXlt(start_date)
  age <- as.numeric(start_lt$year - birth_lt$year)
  not_had_birthday <- (start_lt$mon < birth_lt$mon) |
    (start_lt$mon == birth_lt$mon & start_lt$mday < birth_lt$mday)
  if_else(not_had_birthday, age - 1, age)
}


# ---- Pull required tables into R ----
stp_enrolment <- sch_tbl("STP_Enrolment") %>%
  collect() |> rename_with(toupper)

stp_enrolment_record_type <- sch_tbl("STP_Enrolment_Record_Type") %>%
  collect() |> rename_with(toupper)

age_lookup <- sch_tbl("AgeGroupLookup") %>%
  collect() |> rename_with(toupper)

credential <- sch_tbl("Credential") %>%
  collect() |> rename_with(toupper)


# ******************************************************************************
# Part 1: Build MinEnrolment (qry01a–01e, qry_CreateMinEnrolmentView)
#
# Joins STP_Enrolment with STP_Enrolment_Record_Type, adds support variables
# (date conversions, age, age group, first enrolment flag), then filters to
# records with RecordStatus=0 and MinEnrolment=1.
# WHY: MinEnrolment is the core working dataset for downstream projections.
# RecordStatus=0 = valid record, MinEnrolment=1 = earliest enrolment per student.
# In SQL this was a CREATE VIEW; in dplyr it's a filtered dataframe written to DB.
# ******************************************************************************

min_enrolment <- stp_enrolment %>%
  inner_join(
    stp_enrolment_record_type %>% select(ID, MINENROLMENT, FIRSTENROLMENT, RECORDSTATUS),
    by = "ID"
  ) %>%
  mutate(
    # qry01c: Convert birthdate string to Date
    PSI_BIRTHDATE_CLEANED_D = as.Date(PSI_BIRTHDATE_CLEANED),
    # qry01d1: Convert start date, excluding empty/unspecified
    PSI_MIN_START_DATE_D = if_else(
      is.na(PSI_MIN_START_DATE) | PSI_MIN_START_DATE == "" |
        PSI_MIN_START_DATE == "(Unspecified)",
      as.Date(NA), as.Date(PSI_MIN_START_DATE)
    ),
    # qry01d2: Null out birthdates converted from empty/invalid to 1900-01-01
    PSI_BIRTHDATE_CLEANED_D = if_else(
      !is.na(PSI_BIRTHDATE_CLEANED_D) &
        PSI_BIRTHDATE_CLEANED_D == as.Date("1900-01-01") &
        (is.na(PSI_BIRTHDATE_CLEANED) | PSI_BIRTHDATE_CLEANED == "" |
           PSI_BIRTHDATE_CLEANED == "(Unspecified)"),
      as.Date(NA), PSI_BIRTHDATE_CLEANED_D
    ),
    # Initialize age and age group columns
    AGE_AT_ENROL_DATE = NA_real_,
    AGE_GROUP_ENROL_DATE = NA_real_,
    # qry01e: Flag first enrolments
    IS_FIRST_ENROLMENT = if_else(FIRSTENROLMENT == 1, "Yes", NA_character_),
    IS_SKILLS_BASED = NA_real_
  ) %>%
  # Filter to valid minimum enrolment records
  filter(RECORDSTATUS == 0, MINENROLMENT == 1) %>%
  select(-MINENROLMENT, -FIRSTENROLMENT, -RECORDSTATUS)


# ******************************************************************************
# Part 2: Compute age at enrolment (qry02a–02b)
#
# Computes age from birthdate and start date, then maps to age group index.
# WHY: Age is a key dimension for graduate projections.
# ******************************************************************************

# qry02a: Compute age at enrolment
min_enrolment <- min_enrolment %>%
  mutate(
    AGE_AT_ENROL_DATE = if_else(
      !is.na(PSI_BIRTHDATE_CLEANED_D) & !is.na(PSI_MIN_START_DATE_D),
      compute_age_at_date(PSI_BIRTHDATE_CLEANED_D, PSI_MIN_START_DATE_D),
      AGE_AT_ENROL_DATE
    )
  )

# qry02b: Look up age group from AgeGroupLookup (SQL CROSS JOIN with range condition)
min_enrolment <- min_enrolment %>%
  mutate(AGE_GROUP_ENROL_DATE = lookup_age_group(AGE_AT_ENROL_DATE, age_lookup))


# ******************************************************************************
# Part 3: Gender update from Credential (qry04a1–04a2)
#
# Updates enrolment gender from the Credential view's cleaned gender, matching
# first by EPEN then by PSI_STUDENT_NUMBER + PSI_CODE.
# WHY: Credential preprocessing produces a more accurate cleaned gender value.
# ******************************************************************************

# Build distinct lookup of cleaned gender from Credential (deduplicated to avoid row multiplication)
cred_gender_epen <- credential %>%
  filter(!is.na(ENCRYPTED_TRUE_PEN) & ENCRYPTED_TRUE_PEN != "" &
           ENCRYPTED_TRUE_PEN != "(Unspecified)") %>%
  distinct(ENCRYPTED_TRUE_PEN, PSI_GENDER_CLEANED)

cred_gender_studnum <- credential %>%
  distinct(PSI_STUDENT_NUMBER, PSI_CODE, PSI_GENDER_CLEANED)

# qry04a1: Update via EPEN match
min_enrolment <- min_enrolment %>%
  left_join(cred_gender_epen, by = "ENCRYPTED_TRUE_PEN") %>%
  mutate(
    PSI_GENDER = if_else(
      !is.na(ENCRYPTED_TRUE_PEN) & ENCRYPTED_TRUE_PEN != "" &
        ENCRYPTED_TRUE_PEN != "(Unspecified)" &
        !is.na(PSI_GENDER_CLEANED) & PSI_GENDER != PSI_GENDER_CLEANED,
      PSI_GENDER_CLEANED, PSI_GENDER
    )
  ) %>%
  select(-PSI_GENDER_CLEANED)

# qry04a2: Update via studnum+psicode match (for records without valid EPEN)
min_enrolment <- min_enrolment %>%
  left_join(cred_gender_studnum, by = c("PSI_STUDENT_NUMBER", "PSI_CODE")) %>%
  mutate(
    PSI_GENDER = if_else(
      (is.na(ENCRYPTED_TRUE_PEN) | ENCRYPTED_TRUE_PEN == "" |
         ENCRYPTED_TRUE_PEN == "(Unspecified)") &
        !is.na(PSI_GENDER_CLEANED) & PSI_GENDER != PSI_GENDER_CLEANED,
      PSI_GENDER_CLEANED, PSI_GENDER
    )
  ) %>%
  select(-PSI_GENDER_CLEANED)


# ******************************************************************************
# Part 4: Gender duplicate resolution (qry04b–04e)
#
# Some students have multiple enrolment records with different genders.
# We assign ONE consistent gender per student using this strategy:
#   1. Identify students by EPEN (if valid) or PSI_STUDENT_NUMBER + PSI_CODE
#   2. Find students with > 1 distinct gender across their records
#   3. Prefer gender from first enrolment record
#   4. If first enrolment gender is unknown, use a non-unknown gender from any record
#   5. Update all records for that student with the resolved gender
# WHY: Inconsistent gender across enrolment records causes issues in projections.
# In SQL this was ~15 operations across 4 temp tables; in dplyr it's one pipeline.
# ******************************************************************************

# Build student identifier: EPEN if valid, otherwise studnum+psicode concatenation
min_enrolment <- min_enrolment %>%
  mutate(
    STUDENT_ID = case_when(
      !is.na(ENCRYPTED_TRUE_PEN) & ENCRYPTED_TRUE_PEN != "" &
        ENCRYPTED_TRUE_PEN != "(Unspecified)" ~ ENCRYPTED_TRUE_PEN,
      TRUE ~ paste0(PSI_STUDENT_NUMBER, PSI_CODE)
    )
  )

# Find students with conflicting genders across their records
dup_student_ids <- min_enrolment %>%
  distinct(STUDENT_ID, PSI_GENDER) %>%
  count(STUDENT_ID, name = "n_genders") %>%
  filter(n_genders > 1) %>%
  pull(STUDENT_ID)

if (length(dup_student_ids) > 0) {
  # Get gender from first enrolment for duplicate students
  first_enrol_gender <- min_enrolment %>%
    filter(IS_FIRST_ENROLMENT == "Yes", STUDENT_ID %in% dup_student_ids) %>%
    distinct(STUDENT_ID, PSI_GENDER) %>%
    rename(FIRST_GENDER = PSI_GENDER)

  # Get non-unknown gender from any record (fallback for unknown first enrolment gender)
  non_unknown_gender <- min_enrolment %>%
    filter(STUDENT_ID %in% dup_student_ids, !PSI_GENDER %in% c("Unknown")) %>%
    distinct(STUDENT_ID, PSI_GENDER) %>%
    rename(NON_UNKNOWN_GENDER = PSI_GENDER)

  # Resolve: prefer first enrolment gender; if unknown, use non-unknown
  resolved_gender <- first_enrol_gender %>%
    left_join(non_unknown_gender, by = "STUDENT_ID") %>%
    mutate(
      RESOLVED_GENDER = case_when(
        FIRST_GENDER %in% c("Male", "Female", "Gender Diverse") ~ FIRST_GENDER,
        !is.na(NON_UNKNOWN_GENDER) ~ NON_UNKNOWN_GENDER,
        TRUE ~ FIRST_GENDER
      )
    ) %>%
    distinct(STUDENT_ID, RESOLVED_GENDER)

  # Update MinEnrolment with resolved genders
  min_enrolment <- min_enrolment %>%
    left_join(resolved_gender, by = "STUDENT_ID") %>%
    mutate(PSI_GENDER = coalesce(RESOLVED_GENDER, PSI_GENDER)) %>%
    select(-RESOLVED_GENDER)

  rm(first_enrol_gender, non_unknown_gender, resolved_gender)
}

min_enrolment <- min_enrolment %>% select(-STUDENT_ID)


# ******************************************************************************
# Part 5: Gender imputation for unknown/missing genders (qry05–06a5)
#
# Assigns genders proportionally to records with unknown/blank/NULL gender.
# The proportions are computed from the known gender distribution of first
# enrolments, then applied in sequence:
#   1. First enrolments: proportional Female/Male/Gender Diverse
#   2. Non-first enrolments: use first enrolment gender via studnum+psicode match
#   3. Remaining: proportional Female/Male/Gender Diverse
#   4. EPEN duplicates: resolve remaining conflicts proportionally
# WHY: Downstream projections need a gender for every record.
# !! UPDATE: The hardcoded TOP(N) counts in the original are replaced with
# dynamic computation from the proportion distribution.
# ******************************************************************************

# qry05a2: Compute known gender distribution
prop_dist <- min_enrolment %>%
  filter(
    !PSI_GENDER %in% c("Unknown", "", "(Unspecified)"),
    !is.na(PSI_GENDER),
    IS_FIRST_ENROLMENT == "Yes"
  ) %>%
  count(PSI_GENDER, name = "NUMENROLLED")


# ---- 5a: Impute gender for first enrolments with unknown gender ----

# qry05a1b: Extract first enrolments with unknown gender
extract_no_gender_first <- min_enrolment %>%
  filter(is_unknown_gender(PSI_GENDER), IS_FIRST_ENROLMENT == "Yes") %>%
  distinct(ID, ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, PSI_GENDER)

# Compute proportional counts dynamically (replaces hardcoded TOP(N))
n_first <- nrow(extract_no_gender_first)
prop_first <- prop_dist %>%
  mutate(p = NUMENROLLED / sum(NUMENROLLED), top_n = round(p * n_first))

n_female <- prop_first %>% filter(PSI_GENDER == "Female") %>% pull(top_n)
n_male <- prop_first %>% filter(PSI_GENDER == "Male") %>% pull(top_n)

# Assign genders in sequence: Female → Male → Gender Diverse
unknown_idx <- which(is_unknown_gender(extract_no_gender_first$PSI_GENDER))
if (n_female > 0 && length(unknown_idx) >= n_female) {
  extract_no_gender_first$PSI_GENDER[unknown_idx[1:n_female]] <- "Female"
}
if (n_male > 0 && n_female + n_male <= length(unknown_idx)) {
  extract_no_gender_first$PSI_GENDER[unknown_idx[(n_female + 1):(n_female + n_male)]] <- "Male"
}
still_unk <- which(is_unknown_gender(extract_no_gender_first$PSI_GENDER))
if (length(still_unk) > 0) {
  extract_no_gender_first$PSI_GENDER[still_unk] <- "Gender Diverse"
}


# ---- 5b: Extract ALL records with unknown gender ----

# qry05a1: Extract all records with unknown gender
extract_no_gender <- min_enrolment %>%
  filter(is_unknown_gender(PSI_GENDER)) %>%
  distinct(ID, ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, PSI_GENDER)


# ---- 5c: Update non-first from first enrolment gender ----

# qry06a3_CorrectGender1: Match via PSI_STUDENT_NUMBER + PSI_CODE
extract_no_gender <- extract_no_gender %>%
  left_join(
    extract_no_gender_first %>% select(PSI_STUDENT_NUMBER, PSI_CODE, PSI_GENDER),
    by = c("PSI_STUDENT_NUMBER", "PSI_CODE"),
    suffix = c("", "_first")
  ) %>%
  mutate(
    PSI_GENDER = if_else(
      is_unknown_gender(PSI_GENDER) & !is.na(PSI_GENDER_FIRST),
      PSI_GENDER_FIRST, PSI_GENDER
    )
  ) %>%
  select(-PSI_GENDER_FIRST)


# ---- 5d: Assign remaining proportionally ----

still_unknown_idx <- which(is_unknown_gender(extract_no_gender$PSI_GENDER))
n_remaining <- length(still_unknown_idx)

if (n_remaining > 0) {
  prop_rem <- prop_dist %>%
    mutate(p = NUMENROLLED / sum(NUMENROLLED), top_n = round(p * n_remaining))

  n_f <- prop_rem %>% filter(PSI_GENDER == "Female") %>% pull(top_n)
  n_m <- prop_rem %>% filter(PSI_GENDER == "Male") %>% pull(top_n)

  if (n_f > 0 && n_f <= n_remaining) {
    extract_no_gender$PSI_GENDER[still_unknown_idx[1:n_f]] <- "Female"
  }
  if (n_m > 0 && n_f + n_m <= n_remaining) {
    extract_no_gender$PSI_GENDER[still_unknown_idx[(n_f + 1):(n_f + n_m)]] <- "Male"
  }
  still_unk2 <- which(is_unknown_gender(extract_no_gender$PSI_GENDER))
  if (length(still_unk2) > 0) {
    extract_no_gender$PSI_GENDER[still_unk2] <- "Gender Diverse"
  }
}


# ---- 5e: Handle EPEN duplicates (same EPEN, multiple assigned genders) ----

# qry06a4a–06a4c: Find EPENs with conflicting genders and resolve proportionally
dup_epens <- extract_no_gender %>%
  filter(!is.na(ENCRYPTED_TRUE_PEN) & ENCRYPTED_TRUE_PEN != "" &
           ENCRYPTED_TRUE_PEN != "(Unspecified)") %>%
  count(ENCRYPTED_TRUE_PEN, name = "n_genders") %>%
  filter(n_genders > 1)

if (nrow(dup_epens) > 0) {
  n_dup <- nrow(dup_epens)
  prop_dup <- prop_dist %>%
    mutate(p = NUMENROLLED / sum(NUMENROLLED), top_n = round(p * n_dup))

  n_f_dup <- prop_dup %>% filter(PSI_GENDER == "Female") %>% pull(top_n)
  n_gd_dup <- prop_dup %>% filter(PSI_GENDER == "Gender Diverse") %>% pull(top_n)

  dup_epens$PSI_GENDER_TO_USE <- NA_character_
  if (n_f_dup > 0) dup_epens$PSI_GENDER_TO_USE[1:n_f_dup] <- "Female"
  gd_range <- (n_f_dup + 1):(n_f_dup + n_gd_dup)
  if (n_gd_dup > 0 && n_f_dup + n_gd_dup <= n_dup) {
    dup_epens$PSI_GENDER_TO_USE[gd_range] <- "Gender Diverse"
  }
  dup_epens$PSI_GENDER_TO_USE[is.na(dup_epens$PSI_GENDER_TO_USE)] <- "Male"

  extract_no_gender <- extract_no_gender %>%
    left_join(
      dup_epens %>% select(ENCRYPTED_TRUE_PEN, PSI_GENDER_TO_USE),
      by = "ENCRYPTED_TRUE_PEN"
    ) %>%
    mutate(PSI_GENDER = coalesce(PSI_GENDER_TO_USE, PSI_GENDER)) %>%
    select(-PSI_GENDER_TO_USE)
}


# ---- 5f: Push imputed genders back to MinEnrolment ----

# qry06a5: Update MinEnrolment with all imputed genders
min_enrolment <- min_enrolment %>%
  left_join(
    extract_no_gender %>% select(ID, PSI_GENDER),
    by = "ID",
    suffix = c("", "_imputed")
  ) %>%
  mutate(PSI_GENDER = coalesce(PSI_GENDER_IMPUTED, PSI_GENDER)) %>%
  select(-PSI_GENDER_IMPUTED)

# qry06a4c: Check proportions after gender assignment
prop_check <- min_enrolment %>%
  filter(
    !PSI_GENDER %in% c("Unknown", "", "(Unspecified)"),
    !is.na(PSI_GENDER),
    IS_FIRST_ENROLMENT == "Yes"
  ) %>%
  count(PSI_GENDER, name = "NUMENROLLED")
prop_check

rm(extract_no_gender_first, extract_no_gender, dup_epens)


# ******************************************************************************
# Part 6: Age imputation for records with missing age (qry07a–07e)
#
# Imputes missing ages using proportional sampling from known age distributions,
# then calculates ages for students with multiple enrolments using year differences.
# WHY: Some records have no birthdate, so age cannot be computed directly.
# We sample ages from the known distribution, stratified by gender.
# ******************************************************************************

# qry07a: Extract records with missing age
extract_no_age <- min_enrolment %>%
  filter(is.na(AGE_AT_ENROL_DATE)) %>%
  distinct(ID, ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE,
           AGE_AT_ENROL_DATE, PSI_SCHOOL_YEAR, PSI_MIN_START_DATE, PSI_MIN_START_DATE_D)

# qry07b: Extract first enrolments with missing age
extract_no_age_first <- min_enrolment %>%
  filter(is.na(AGE_AT_ENROL_DATE), IS_FIRST_ENROLMENT == "Yes") %>%
  distinct(ID, ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_GENDER, PSI_CODE, AGE_AT_ENROL_DATE)

# qry07b2: Flag first enrolments in extract_no_age
extract_no_age <- extract_no_age %>%
  mutate(IS_FIRST_ENROLMENT = if_else(ID %in% extract_no_age_first$ID, "Yes", NA_character_))

# qry07c: Compute age distribution for sampling
age_dist <- min_enrolment %>%
  filter(!is.na(AGE_GROUP_ENROL_DATE), IS_FIRST_ENROLMENT == "Yes") %>%
  count(AGE_AT_ENROL_DATE, PSI_GENDER, name = "NUMENROLLED")

n_miss <- extract_no_age_first %>%
  filter(is.na(AGE_AT_ENROL_DATE)) %>%
  count(PSI_GENDER, name = "N_MISS")

age_dist <- age_dist %>%
  group_by(PSI_GENDER) %>%
  mutate(PROPENROLLED = round(NUMENROLLED / sum(NUMENROLLED), 5)) %>%
  ungroup() %>%
  inner_join(n_miss, by = "PSI_GENDER") %>%
  mutate(NUMDISTRIBUTION = round(PROPENROLLED * N_MISS)) %>%
  select(-N_MISS)

dbWriteTable(con, "AgeDistributionbyGender", age_dist, overwrite = TRUE)

# ---- Sample ages proportionally by gender ----
m_id <- extract_no_age_first %>%
  filter(PSI_GENDER == "Male", is.na(AGE_AT_ENROL_DATE)) %>% pull(ID)

f_id <- extract_no_age_first %>%
  filter(PSI_GENDER == "Female", is.na(AGE_AT_ENROL_DATE)) %>% pull(ID)

gd_id <- extract_no_age_first %>%
  filter(PSI_GENDER == "Gender Diverse", is.na(AGE_AT_ENROL_DATE)) %>% pull(ID)

m_dist <- age_dist %>% filter(NUMDISTRIBUTION > 0, PSI_GENDER == "Male")
f_dist <- age_dist %>% filter(NUMDISTRIBUTION > 0, PSI_GENDER == "Female")
gd_dist <- age_dist %>% filter(NUMDISTRIBUTION > 0, PSI_GENDER == "Gender Diverse")

m_sampled <- data.frame(
  ID = m_id,
  AGE_AT_ENROL_DATE = sample(m_dist$AGE_AT_ENROL_DATE, size = length(m_id),
                              replace = TRUE, prob = m_dist$PROPENROLLED)
)
f_sampled <- data.frame(
  ID = f_id,
  AGE_AT_ENROL_DATE = sample(f_dist$AGE_AT_ENROL_DATE, size = length(f_id),
                              replace = TRUE, prob = f_dist$PROPENROLLED)
)
# Gender Diverse groups may be too small for proportional sampling
gd_sampled <- tryCatch(
  data.frame(
    ID = gd_id,
    AGE_AT_ENROL_DATE = sample(gd_dist$AGE_AT_ENROL_DATE, size = length(gd_id),
                                replace = TRUE, prob = gd_dist$PROPENROLLED)
  ),
  error = function(e) data.frame(ID = gd_id, AGE_AT_ENROL_DATE = NA_real_)
)

# Apply sampled ages to first enrolments
extract_no_age_first <- extract_no_age_first %>%
  left_join(bind_rows(m_sampled, f_sampled), by = "ID", suffix = c("", ".new")) %>%
  mutate(AGE_AT_ENROL_DATE = if_else(is.na(AGE_AT_ENROL_DATE),
                                      AGE_AT_ENROL_DATE.NEW, AGE_AT_ENROL_DATE)) %>%
  select(-AGE_AT_ENROL_DATE.NEW)

# Fill remaining GD records from any available age
no_age_ids <- extract_no_age_first %>%
  filter(is.na(AGE_AT_ENROL_DATE)) %>% pull(ID)

if (length(no_age_ids) > 0) {
  gd_fill <- data.frame(
    ID = no_age_ids,
    AGE_AT_ENROL_DATE = sample(
      extract_no_age_first %>% filter(!is.na(AGE_AT_ENROL_DATE)) %>% pull(AGE_AT_ENROL_DATE),
      size = length(no_age_ids)
    )
  )
  extract_no_age_first <- extract_no_age_first %>%
    left_join(gd_fill, by = "ID", suffix = c("", ".new")) %>%
    mutate(AGE_AT_ENROL_DATE = if_else(is.na(AGE_AT_ENROL_DATE),
                                        AGE_AT_ENROL_DATE.NEW, AGE_AT_ENROL_DATE)) %>%
    select(-AGE_AT_ENROL_DATE.NEW)
}

dbWriteTable(con, "Extract_No_Age_First_Enrolment", extract_no_age_first, overwrite = TRUE)


# qry07d1: Update Extract_No_Age with first enrolment ages
extract_no_age <- extract_no_age %>%
  left_join(
    extract_no_age_first %>% select(ID, AGE_AT_ENROL_DATE),
    by = "ID", suffix = c("", ".first")
  ) %>%
  mutate(AGE_AT_ENROL_DATE = coalesce(AGE_AT_ENROL_DATE, AGE_AT_ENROL_DATE.FIRST)) %>%
  select(-AGE_AT_ENROL_DATE.FIRST)


# ---- qry02a–02b: Calculate ages for students with multiple enrolments ----
# WHY: If a student was 20 at their first enrolment in 2015/2016, they should be
# ~21 in 2016/2017, ~22 in 2017/2018, etc. This loop computes missing ages from
# the first enrolment's known age + year difference.
# KEPT AS R LOOP: The original already uses R for this calculation.

multiple_enrol <- extract_no_age %>%
  filter(!is.na(PSI_STUDENT_NUMBER), !is.na(PSI_CODE)) %>%
  count(PSI_STUDENT_NUMBER, PSI_CODE, name = "n") %>%
  filter(n > 1)

calc_ages <- extract_no_age %>%
  semi_join(multiple_enrol, by = c("PSI_STUDENT_NUMBER", "PSI_CODE")) %>%
  arrange(PSI_STUDENT_NUMBER, PSI_CODE, PSI_MIN_START_DATE_D, desc(IS_FIRST_ENROLMENT)) %>%
  select(ID, PSI_STUDENT_NUMBER, PSI_CODE, PSI_SCHOOL_YEAR,
         PSI_MIN_START_DATE_D, AGE_AT_ENROL_DATE, IS_FIRST_ENROLMENT) %>%
  as.data.frame()

for (i in seq_len(nrow(multiple_enrol))) {
  sn <- multiple_enrol$PSI_STUDENT_NUMBER[i]
  code <- multiple_enrol$PSI_CODE[i]
  rs <- calc_ages[calc_ages$PSI_STUDENT_NUMBER == sn & calc_ages$PSI_CODE == code,
                   c("PSI_STUDENT_NUMBER", "PSI_CODE", "PSI_MIN_START_DATE_D", "AGE_AT_ENROL_DATE")]

  if (nrow(rs) > 0 && !is.na(rs$AGE_AT_ENROL_DATE[1])) {
    date1 <- as.POSIXlt(rs$PSI_MIN_START_DATE_D[1])
    age1 <- rs$AGE_AT_ENROL_DATE[1]
    rs$AGE_AT_ENROL_DATE_NEW <- NA_real_
    rs$AGE_AT_ENROL_DATE_NEW[1] <- rs$AGE_AT_ENROL_DATE[1]
    for (j in 2:nrow(rs)) {
      date2 <- as.POSIXlt(rs$PSI_MIN_START_DATE_D[j])
      rs$AGE_AT_ENROL_DATE_NEW[j] <- age1 + (date2$year - date1$year)
    }
    calc_ages <- merge(calc_ages,
                        rs[, c("PSI_STUDENT_NUMBER", "PSI_CODE", "PSI_MIN_START_DATE_D",
                               "AGE_AT_ENROL_DATE", "AGE_AT_ENROL_DATE_NEW")],
                        by = c("PSI_STUDENT_NUMBER", "PSI_CODE", "PSI_MIN_START_DATE_D",
                               "AGE_AT_ENROL_DATE"),
                        all.x = TRUE)
    calc_ages$AGE_AT_ENROL_DATE <- ifelse(is.na(calc_ages$AGE_AT_ENROL_DATE),
                                           calc_ages$AGE_AT_ENROL_DATE_NEW,
                                           calc_ages$AGE_AT_ENROL_DATE)
    calc_ages$AGE_AT_ENROL_DATE_NEW <- NULL
  }
}

calc_ages_final <- calc_ages %>%
  as_tibble() %>%
  select(ID, AGE_AT_ENROL_DATE)

# Update Extract_No_Age with computed ages
extract_no_age <- extract_no_age %>%
  left_join(calc_ages_final, by = "ID", suffix = c("", ".computed")) %>%
  mutate(
    AGE_AT_ENROL_DATE = if_else(
      is.na(AGE_AT_ENROL_DATE) & !is.na(AGE_AT_ENROL_DATE.COMPUTED),
      AGE_AT_ENROL_DATE.COMPUTED, AGE_AT_ENROL_DATE
    )
  ) %>%
  select(-AGE_AT_ENROL_DATE.COMPUTED)


# ---- qry07d2–07d3: Manual age fixes (self-join on birthdate) ----
# WHY: For remaining records with no age, try to get birthdate from another record
# of the same student, then compute age from birthdate + start date.
# In SQL this was a view with self-join; in dplyr we do it directly.

no_age_remaining <- extract_no_age %>%
  filter(is.na(AGE_AT_ENROL_DATE)) %>%
  distinct(PSI_STUDENT_NUMBER, PSI_CODE)

# Get birthdates from matching students in MinEnrolment
available_birthdates <- min_enrolment %>%
  semi_join(no_age_remaining, by = c("PSI_STUDENT_NUMBER", "PSI_CODE")) %>%
  filter(!is.na(PSI_BIRTHDATE_CLEANED_D)) %>%
  distinct(PSI_STUDENT_NUMBER, PSI_CODE, PSI_BIRTHDATE_CLEANED_D)

# Compute age from found birthdates
no_age_with_bday <- extract_no_age %>%
  filter(is.na(AGE_AT_ENROL_DATE)) %>%
  left_join(available_birthdates, by = c("PSI_STUDENT_NUMBER", "PSI_CODE"),
            suffix = c("", ".other")) %>%
  mutate(
    PSI_BIRTHDATE_CLEANED_D = coalesce(PSI_BIRTHDATE_CLEANED_D, PSI_BIRTHDATE_CLEANED_D.OTHER),
    AGE_AT_ENROL_DATE = if_else(
      is.na(AGE_AT_ENROL_DATE) & !is.na(PSI_BIRTHDATE_CLEANED_D) & !is.na(PSI_MIN_START_DATE_D),
      compute_age_at_date(PSI_BIRTHDATE_CLEANED_D, PSI_MIN_START_DATE_D),
      AGE_AT_ENROL_DATE
    )
  ) %>%
  select(ID, AGE_AT_ENROL_DATE)

# Merge back
extract_no_age <- extract_no_age %>%
  left_join(no_age_with_bday, by = "ID", suffix = c("", ".fix")) %>%
  mutate(AGE_AT_ENROL_DATE = coalesce(AGE_AT_ENROL_DATE, AGE_AT_ENROL_DATE.FIX)) %>%
  select(-AGE_AT_ENROL_DATE.FIX)


# qry07e: Update MinEnrolment with all computed ages
min_enrolment <- min_enrolment %>%
  left_join(
    extract_no_age %>% select(ID, AGE_AT_ENROL_DATE),
    by = "ID", suffix = c("", ".fixed")
  ) %>%
  mutate(AGE_AT_ENROL_DATE = coalesce(AGE_AT_ENROL_DATE, AGE_AT_ENROL_DATE.FIXED)) %>%
  select(-AGE_AT_ENROL_DATE.FIXED)

# qry08: Final age group lookup
min_enrolment <- min_enrolment %>%
  mutate(AGE_GROUP_ENROL_DATE = lookup_age_group(AGE_AT_ENROL_DATE, age_lookup))

# Cleanup temp tables from database
# KEPT AS SQL: DROP TABLE (cleanup of intermediate tables)
dbExecute(con, glue::glue(
  "IF OBJECT_ID('{my_schema}.Extract_No_Age_First_Enrolment') IS NOT NULL ",
  "DROP TABLE [{my_schema}].Extract_No_Age_First_Enrolment;"
))
dbExecute(con, glue::glue(
  "IF OBJECT_ID('{my_schema}.AgeDistributionbyGender') IS NOT NULL ",
  "DROP TABLE [{my_schema}].AgeDistributionbyGender;"
))

rm(extract_no_age, extract_no_age_first, calc_ages, calc_ages_final,
   multiple_enrol, no_age_remaining, no_age_with_bday, available_birthdates,
   m_sampled, f_sampled, gd_sampled, m_id, f_id, gd_id,
   m_dist, f_dist, gd_dist, no_age_ids)


# ******************************************************************************
# Part 7: Write MinEnrolment to database
# WHY: Downstream scripts (04-graduate-projections, 06-program-projections, etc.)
# reference MinEnrolment as a database table. In the original, this was a SQL VIEW.
# ******************************************************************************
dbWriteTable(con,
             SQL(glue::glue('"{my_schema}"."MinEnrolment"')),
             min_enrolment,
             overwrite = TRUE)


# ******************************************************************************
# Part 8: Final distributions (qry09c)
# Produce summary tables for reporting and model validation.
# WHY: These distribution tables show enrolment counts broken down by various
# dimensions (gender, age, credential, CIP code).
# ******************************************************************************

# qry09c: Enrolment by credential and CIP code
qry09c_by_cred_cip <- min_enrolment %>%
  count(PSI_SCHOOL_YEAR, PSI_CREDENTIAL_CATEGORY, PSI_CIP_CODE, name = "EXPR1") %>%
  arrange(PSI_CREDENTIAL_CATEGORY, PSI_CIP_CODE)

dbWriteTable(con,
             SQL(glue::glue('"{my_schema}"."qry09c_MinEnrolment_by_Credential_and_CIP_Code"')),
             qry09c_by_cred_cip, overwrite = TRUE)

# qry09c: Domestic enrolment by gender/age group/year
qry09c_domestic <- min_enrolment %>%
  filter(PSI_VISA_STATUS == "DOMESTIC") %>%
  inner_join(age_lookup %>% select(AGEINDEX, AGEGROUP),
             by = c("AGE_GROUP_ENROL_DATE" = "AGEINDEX")) %>%
  count(PSI_GENDER, AGEGROUP, PSI_SCHOOL_YEAR, name = "EXPR1") %>%
  arrange(PSI_GENDER, AGEGROUP, PSI_SCHOOL_YEAR)

dbWriteTable(con,
             SQL(glue::glue('"{my_schema}"."qry09c_MinEnrolment_Domestic"')),
             qry09c_domestic, overwrite = TRUE)

# qry09c: Enrolment by gender/age group/year (all visa statuses, excluding current year)
# !! UPDATE: Change the excluded year to match the current model run
qry09c_all <- min_enrolment %>%
  inner_join(age_lookup %>% select(AGEINDEX, AGEGROUP),
             by = c("AGE_GROUP_ENROL_DATE" = "AGEINDEX")) %>%
  filter(PSI_SCHOOL_YEAR != "2023/2024") %>%
  mutate(GROUPS = paste0(PSI_GENDER, AGEGROUP)) %>%
  count(PSI_GENDER, GROUPS, PSI_SCHOOL_YEAR, name = "EXPR1") %>%
  arrange(PSI_GENDER, GROUPS, PSI_SCHOOL_YEAR)

dbWriteTable(con,
             SQL(glue::glue('"{my_schema}"."qry09c_MinEnrolment"')),
             qry09c_all, overwrite = TRUE)

# qry09c: Enrolment by PSI type (commented out in original — requires PSI_CODE_RECODE table)
# psi_code_recode <- sch_tbl("PSI_CODE_RECODE") %>% collect() |> rename_with(toupper)
# qry09c_psi_type <- min_enrolment %>%
#   inner_join(age_lookup %>% select(AGEINDEX, AGEGROUP),
#              by = c("AGE_GROUP_ENROL_DATE" = "AGEINDEX")) %>%
#   inner_join(psi_code_recode, by = "PSI_CODE") %>%
#   filter(!AGE_GROUP_ENROL_DATE %in% c(1, 9)) %>%
#   count(PSI_SCHOOL_YEAR, PSI_TYPE_RECODE, PSI_CODE_RECODE, name = "EXPR1") %>%
#   arrange(PSI_SCHOOL_YEAR)
# dbWriteTable(con,
#              SQL(glue::glue('"{my_schema}"."qry09c_MinEnrolment_PSI_TYPE"')),
#              qry09c_psi_type, overwrite = TRUE)


# ---- Final check: verify required tables exist ----
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Credential_Record_Type"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Enrolment_Record_Type"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Enrolment"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Enrolment_Valid"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Credential"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."MinEnrolment"')))

dbDisconnect(con)
