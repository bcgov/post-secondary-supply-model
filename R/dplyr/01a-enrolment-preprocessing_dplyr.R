# STP Enrolment Preprocessing — dplyr Translation
# Original: R/01a-enrolment-preprocessing.R
#
# Pipeline context:
#   First step of the PSSM pipeline. Cleans raw STP enrolment data by:
#     1. Reformatting dates from yy-mm-dd to yyyy-mm-dd
#     2. Classifying each record with a RecordStatus code (0=good, 1-8=excluded)
#     3. Identifying minimum and first enrolment records per student/year
#     4. Resolving conflicting birthdates across records for the same student
#
# Input tables:
#   - STP_Enrolment — raw enrolment data (from load-stp-enrol.R)
#
# Output tables:
#   - STP_Enrolment — updated with cleaned dates and psi_birthdate_cleaned
#   - STP_Enrolment_Record_Type — ID, RecordStatus, MinEnrolment, FirstEnrolment
#   - STP_Enrolment_Valid — subset of STP_Enrolment where RecordStatus=0

library(arrow)
library(tidyverse)
library(odbc)
library(DBI)
library(dbplyr)

# ---- Configure LAN Paths and DB Connection -----
lan <- config::get("lan")
db_config <- config::get("decimal")
my_schema <- config::get("myschema")

con <- dbConnect(odbc(),
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

# ---- Check required table ----
assertthat::assert_that(
  dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Enrolment"'))),
  msg = "STP_Enrolment table not found"
)


# ******************************************************************************
# Part 0: Pull STP_Enrolment into R
# ******************************************************************************
stp_enrolment <- sch_tbl("STP_Enrolment") %>%
  collect() |> rename_with(toupper)

# Check for null/blank EPENs
stp_enrolment %>%
  filter(is_blank(ENCRYPTED_TRUE_PEN)) %>%
  tally(name = "n_null_epens")

# Count distinct EPENs
stp_enrolment %>%
  distinct(ENCRYPTED_TRUE_PEN) %>%
  tally(name = "n_epens")


# ******************************************************************************
# Part 1: Add ID column as primary key
# ******************************************************************************
# WHY: The original SQL adds an IDENTITY column. In R, we add a row number.
# KEPT AS SQL: ALTER TABLE + ADD CONSTRAINT (DDL for primary key)
stp_enrolment <- stp_enrolment %>%
  mutate(ID = row_number())

# Also add to DB so downstream SQL scripts work
dbExecute(con, "ALTER TABLE STP_Enrolment ADD ID INT IDENTITY(1,1) NOT NULL")
dbExecute(con, "ALTER TABLE STP_Enrolment ADD CONSTRAINT STP_Enrolment_PK_ID PRIMARY KEY (ID)")


# ******************************************************************************
# Part 2: Reformat dates from yy-mm-dd to yyyy-mm-dd
# ******************************************************************************
# WHY: Some date fields are in 2-digit year format (e.g., "23-01-15"). Need to
# convert to 4-digit years: yy < 24 → "20"+yy, yy > 23 → "19"+yy, blank → "".

convert_yy_to_yyyy <- function(date_col) {
  if_else(
    is.na(date_col),
    NA_character_,
    case_when(
      str_sub(date_col, 1, 2) == "  " ~ "",
      as.numeric(str_sub(date_col, 1, 2)) < 24 ~ paste0("20", date_col),
      as.numeric(str_sub(date_col, 1, 2)) >= 24 ~ paste0("19", date_col),
      TRUE ~ date_col
    )
  )
}

stp_enrolment <- stp_enrolment %>%
  mutate(
    PSI_BIRTHDATE = convert_yy_to_yyyy(PSI_BIRTHDATE),
    LAST_SEEN_BIRTHDATE = convert_yy_to_yyyy(LAST_SEEN_BIRTHDATE),
    PSI_MIN_START_DATE = convert_yy_to_yyyy(PSI_MIN_START_DATE),
    PSI_PROGRAM_EFFECTIVE_DATE = convert_yy_to_yyyy(PSI_PROGRAM_EFFECTIVE_DATE)
  )


# ******************************************************************************
# Part 3: Create Record Type table and classify records
# ******************************************************************************
# WHY: Each enrolment record is assigned a RecordStatus indicating whether it
# should be included (0) or excluded (1-8) from the model. The classification
# is sequential and priority-based: once a record gets a status, it keeps it.
#
# Record Status codes:
#   0 = Good (keep)
#   1 = Missing Student Number
#   2 = Developmental
#   3 = No PSI Transition
#   4 = Credential Only (not assigned in this script)
#   5 = PSI Outside BC
#   6 = Skills Based
#   7 = Developmental CIP
#   8 = Recommendation for Certification (not assigned in this script)

# Initialize record type table with ID and NULL status
record_type <- tibble(ID = stp_enrolment$ID) %>%
  mutate(RecordStatus = NA_integer_, MinEnrolment = NA_integer_, FirstEnrolment = NA_integer_)


# ---- Status 1: No PEN or Student Number ----
# WHY: Records without any student identifier (PEN or PSI_CODE/STUDENT_NUMBER)
# cannot be matched across years and must be excluded.
has_valid_id <- stp_enrolment %>%
  filter(
    (!is_blank(PSI_STUDENT_NUMBER) & !is_blank(PSI_CODE))
    | !is_blank(ENCRYPTED_TRUE_PEN)
  ) %>%
  select(ID)

record_type <- record_type %>%
  mutate(RecordStatus = if_else(!ID %in% has_valid_id$ID & is.na(RecordStatus), 1L, RecordStatus))


# ---- Status 2: Developmental ----
# WHY: Developmental study level courses are preparatory and not part of the
# post-secondary credential pipeline.
is_developmental <- stp_enrolment %>%
  filter(PSI_STUDY_LEVEL == "DEVELOPMENTAL") %>%
  select(ID)

record_type <- record_type %>%
  mutate(RecordStatus = if_else(ID %in% is_developmental$ID & is.na(RecordStatus), 2L, RecordStatus))


# ---- Status 6: Skills Based (multi-step) ----
# WHY: Skills-based continuing education courses are excluded unless they lead
# to a recognized credential. This is a multi-step process because some skills
# courses are manually reviewed and kept (or excluded) based on expert judgment.

# Step 1: Identify skills-based courses (SKILLS CRS ONLY, not developmental,
# NONE/OTHER credential category)
skills_based <- stp_enrolment %>%
  filter(PSI_CONTINUING_EDUCATION_COURSE_ONLY == "SKILLS CRS ONLY",
         PSI_STUDY_LEVEL != "DEVELOPMENTAL",
         PSI_CREDENTIAL_CATEGORY %in% c("NONE", "OTHER")) %>%
  mutate(CIP2 = str_sub(PSI_CIP_CODE, 1, 2))

# Diagnostic: check list of programs considered skills-based
skills_based %>%
  count(PSI_CODE, PSI_CONTINUING_EDUCATION_COURSE_ONLY, CIP2, PSI_PROGRAM_CODE,
        PSI_CREDENTIAL_PROGRAM_DESCRIPTION, PSI_STUDY_LEVEL, PSI_CREDENTIAL_CATEGORY)

# Mark specific programs to keep (e.g., UFV/UCFV TEACH ED)
skills_based_keep <- skills_based %>%
  mutate(KEEP = if_else(
    (PSI_CODE == "UFV" & PSI_PROGRAM_CODE == "TEACH ED")
    | (PSI_CODE == "UCFV" & PSI_PROGRAM_CODE == "TEACH ED"),
    "Y", NA_character_
  ))

# Assign status 6 to skills-based records NOT marked for keeping
record_type <- record_type %>%
  left_join(
    skills_based_keep %>% select(ID, KEEP) %>% mutate(IS_SKILLS = TRUE),
    by = "ID"
  ) %>%
  mutate(RecordStatus = if_else(
    IS_SKILLS & is.na(RecordStatus) & is.na(KEEP), 6L, RecordStatus
  )) %>%
  select(-IS_SKILLS, -KEEP)


# Step 2: Continuing education with developmental CIP codes
# WHY: Additional skills-based exclusion for records with specific CIP prefixes
# (21, 32-37, 53, 89) that are developmental/continuing education.
cont_ed_cips <- stp_enrolment %>%
  filter(PSI_STUDY_LEVEL != "DEVELOPMENTAL",
         PSI_CONTINUING_EDUCATION_COURSE_ONLY != "SKILLS CRS ONLY",
         PSI_CREDENTIAL_CATEGORY %in% c("NONE", "OTHER"),
         str_sub(PSI_CIP_CODE, 1, 2) %in% c("21", "32", "33", "34", "35", "36", "37", "53", "89")) %>%
  select(ID)

record_type <- record_type %>%
  mutate(RecordStatus = if_else(ID %in% cont_ed_cips$ID & is.na(RecordStatus), 6L, RecordStatus))


# Step 3: Continuing Education / Studies / Audit programs by description
# WHY: Programs with names containing "Continuing Education", "Continuing Studies",
# "Audit", or starting with "CE " are typically non-credential courses.
cont_ed_more <- stp_enrolment %>%
  filter(
    str_detect(PSI_CREDENTIAL_PROGRAM_DESCRIPTION, "Continuing Education$")
    | str_detect(PSI_CREDENTIAL_PROGRAM_DESCRIPTION, "Continuing Studies$")
    | str_detect(PSI_CREDENTIAL_PROGRAM_DESCRIPTION, "Audit")
    | str_detect(PSI_CREDENTIAL_PROGRAM_DESCRIPTION, "^CE ")
  ) %>%
  select(ID)

record_type <- record_type %>%
  mutate(RecordStatus = if_else(ID %in% cont_ed_more$ID & is.na(RecordStatus), 6L, RecordStatus))


# Step 4: Keep skills-based programs with valid credentials
# WHY: Some skills-based courses lead to real credentials (not NONE/OTHER/SHORT CERT)
# and should be kept. They need special handling because the automatic rules would
# exclude them, but manual review confirms they're legitimate.
keep_skills <- stp_enrolment %>%
  filter(
    PSI_CONTINUING_EDUCATION_COURSE_ONLY == "SKILLS CRS ONLY",
    PSI_STUDY_LEVEL != "DEVELOPMENTAL",
    !PSI_CREDENTIAL_CATEGORY %in% c("NONE", "OTHER", "SHORT CERTIFICATE"),
    !str_detect(PSI_CREDENTIAL_PROGRAM_DESCRIPTION, "Continuing Studies$"),
    !str_detect(PSI_CREDENTIAL_PROGRAM_DESCRIPTION, "Audit"),
    !str_detect(PSI_CREDENTIAL_PROGRAM_DESCRIPTION, "Continuing Education$"),
    !str_detect(PSI_CREDENTIAL_PROGRAM_DESCRIPTION, "^CE ")
  ) %>%
  mutate(CIP2 = str_sub(PSI_CIP_CODE, 1, 2)) %>%
  mutate(EXCLUDE = if_else(
    (PSI_CODE == "SEL" & PSI_CREDENTIAL_PROGRAM_DESCRIPTION == "COMMUNITY, CORPORATE & INTERNATIONAL DEVELOPMENT")
    | (PSI_CODE == "NIC" & CIP2 %in% c("21", "32", "33", "34", "35", "36", "37", "53", "89")),
    "Y", NA_character_
  ))

# Set status 0 (keep) for skills-based records not excluded
keep_skills_ids <- keep_skills %>% filter(is.na(EXCLUDE)) %>% pull(ID)
record_type <- record_type %>%
  mutate(RecordStatus = if_else(ID %in% keep_skills_ids & is.na(RecordStatus), 0L, RecordStatus))

# Set status 6 (exclude) for skills-based records that are excluded
exclude_skills_ids <- keep_skills %>% filter(EXCLUDE == "Y") %>% pull(ID)
record_type <- record_type %>%
  mutate(RecordStatus = if_else(ID %in% exclude_skills_ids & is.na(RecordStatus), 6L, RecordStatus))


# Step 5: Selkirk specific exclusion
# WHY: Selkirk College has a specific program that needs exclusion based on
# manual review. This is an institution-specific data quality adjustment.
selkirk_exclude <- stp_enrolment %>%
  filter(PSI_CODE == "SEL",
         PSI_CREDENTIAL_PROGRAM_DESCRIPTION == "COMMUNITY, CORPORATE & INTERNATIONAL DEVELOPMENT") %>%
  pull(ID)

record_type <- record_type %>%
  mutate(RecordStatus = if_else(ID %in% selkirk_exclude & is.na(RecordStatus), 6L, RecordStatus))


# Step 6: Suspect skills-based courses
# WHY: The remaining unclassified records are checked against the skills-based
# course catalog. Programs that have been manually reviewed (Keep IS NULL) are
# classified as skills-based and excluded.
# NOTE: This step involves a manual review table (tmp_tbl_SkillsBasedCourses.KEEP).
# In the original SQL, the KEEP column is manually set. Here we replicate the logic.
dev_cip_codes <- c("21", "32", "33", "34", "35", "36", "37", "53", "89")

suspect_skills <- stp_enrolment %>%
  inner_join(record_type %>% filter(is.na(RecordStatus)) %>% select(ID), by = "ID") %>%
  filter(PSI_CONTINUING_EDUCATION_COURSE_ONLY == "NOT SKILLS CRS ONLY",
         str_sub(PSI_CIP_CODE, 1, 2) %in% dev_cip_codes) %>%
  select(ID)

record_type <- record_type %>%
  mutate(RecordStatus = if_else(ID %in% suspect_skills$ID & is.na(RecordStatus), 7L, RecordStatus))


# ---- Status 3: No PSI Transition ----
# WHY: Records where the student has no transition between PSIs are excluded
# because they represent non-progression.
no_transition <- stp_enrolment %>%
  filter(PSI_ENTRY_STATUS == "No Transition") %>%
  pull(ID)

record_type <- record_type %>%
  mutate(RecordStatus = if_else(ID %in% no_transition & is.na(RecordStatus), 3L, RecordStatus))


# ---- Status 5: PSI Outside BC ----
# WHY: Students attending institutions outside BC are excluded from the
# BC-focused supply model.
outside_bc <- stp_enrolment %>%
  filter(ATTENDING_PSI_OUTSIDE_BC == "Y") %>%
  pull(ID)

record_type <- record_type %>%
  mutate(RecordStatus = if_else(ID %in% outside_bc & is.na(RecordStatus), 5L, RecordStatus))


# ---- Status 0: Default - all remaining records are good ----
record_type <- record_type %>%
  mutate(RecordStatus = if_else(is.na(RecordStatus), 0L, RecordStatus))

# Diagnostic: check record type distribution
record_type %>%
  count(RecordStatus, name = "Count")


# ******************************************************************************
# Part 4: Create STP_Enrolment_Valid
# ******************************************************************************
# WHY: Only records with RecordStatus=0 are included in the model.
stp_enrolment_valid <- stp_enrolment %>%
  inner_join(record_type %>% filter(RecordStatus == 0) %>% select(ID), by = "ID") %>%
  select(ID, PSI_STUDENT_NUMBER, ENCRYPTED_TRUE_PEN, PSI_SCHOOL_YEAR,
         PSI_STUDENT_POSTAL_CODE_CURRENT, PSI_ENROLMENT_SEQUENCE, PSI_CODE,
         PSI_MIN_START_DATE)

# Check records associated with > 1 EPEN
cat("Records associated with > 1 EPEN:\n")
stp_enrolment_valid %>%
  distinct(PSI_CODE, PSI_STUDENT_NUMBER, ENCRYPTED_TRUE_PEN) %>%
  count(PSI_CODE, PSI_STUDENT_NUMBER, name = "n") %>%
  filter(n != 1)


# ******************************************************************************
# Part 5: Min Enrolment — find minimum enrolment sequence per student/year
# ******************************************************************************
# WHY: For each student and school year, the record with the lowest enrolment
# sequence is the "minimum enrolment". This identifies the first interaction
# with the institution in that year.
# Two passes: first by EPEN (for students with valid PENs), then by
# PSI_CODE/PSI_STUDENT_NUMBER (for students with blank EPENs).

# ---- By ENCRYPTED_TRUE_PEN ----
# WHY: EPEN is the preferred identifier. Find the min enrolment sequence
# per EPEN per year, then pick the record with the lowest ID as tiebreaker.
min_enrol_pen <- stp_enrolment_valid %>%
  filter(!is_blank(ENCRYPTED_TRUE_PEN)) %>%
  group_by(ENCRYPTED_TRUE_PEN, PSI_SCHOOL_YEAR) %>%
  slice_min(PSI_ENROLMENT_SEQUENCE, with_ties = TRUE) %>%
  slice_min(ID, with_ties = FALSE) %>%
  ungroup() %>%
  select(MinOfID = ID)

# ---- By PSI_CODE/PSI_STUDENT_NUMBER ----
# WHY: For students without valid EPENs, use the institution-specific
# student number + institution code as the identifier.
min_enrol_stuid <- stp_enrolment_valid %>%
  filter(is_blank(ENCRYPTED_TRUE_PEN)) %>%
  group_by(PSI_STUDENT_NUMBER, PSI_CODE, PSI_SCHOOL_YEAR) %>%
  slice_min(PSI_ENROLMENT_SEQUENCE, with_ties = TRUE) %>%
  slice_min(ID, with_ties = FALSE) %>%
  ungroup() %>%
  select(MinOfID = ID)

# Combine both sets of min enrolment IDs
all_min_enrol_ids <- c(min_enrol_pen$MinOfID, min_enrol_stuid$MinOfID)

record_type <- record_type %>%
  mutate(MinEnrolment = if_else(ID %in% all_min_enrol_ids, 1L, 0L))


# ******************************************************************************
# Part 6: First Enrolment — find earliest enrolment record per student
# ******************************************************************************
# WHY: The first enrolment record (earliest start date, lowest enrolment sequence)
# is used to determine the student's initial program and credential. This is
# different from min enrolment which is per year; first enrolment is overall.
# Two passes: by EPEN and by PSI_CODE/PSI_STUDENT_NUMBER.

# ---- By ENCRYPTED_TRUE_PEN ----
first_enrol_pen <- stp_enrolment_valid %>%
  filter(!is_blank(ENCRYPTED_TRUE_PEN)) %>%
  group_by(ENCRYPTED_TRUE_PEN) %>%
  slice_min(PSI_MIN_START_DATE, with_ties = TRUE) %>%
  slice_min(PSI_ENROLMENT_SEQUENCE, with_ties = TRUE) %>%
  slice_min(ID, with_ties = FALSE) %>%
  ungroup() %>%
  select(MinID = ID)

# ---- By PSI_CODE/PSI_STUDENT_NUMBER ----
first_enrol_stuid <- stp_enrolment_valid %>%
  filter(is_blank(ENCRYPTED_TRUE_PEN)) %>%
  group_by(PSI_STUDENT_NUMBER, PSI_CODE) %>%
  slice_min(PSI_MIN_START_DATE, with_ties = TRUE) %>%
  slice_min(PSI_ENROLMENT_SEQUENCE, with_ties = TRUE) %>%
  slice_min(ID, with_ties = FALSE) %>%
  ungroup() %>%
  select(MinID = ID)

# Combine and flag
all_first_enrol_ids <- c(first_enrol_pen$MinID, first_enrol_stuid$MinID)

record_type <- record_type %>%
  mutate(FirstEnrolment = if_else(ID %in% all_first_enrol_ids, 1L,
                                   if_else(is.na(FirstEnrolment), 0L, FirstEnrolment)))


# ******************************************************************************
# Part 7: Birthdate Cleaning
# ******************************************************************************
# WHY: The same student (identified by EPEN) may have different birthdates
# across records. We resolve this by:
#   1. Finding EPENs with multiple distinct birthdates
#   2. Choosing the most common birthdate (or the one matching LAST_SEEN_BIRTHDATE)
#   3. Filling null birthdates from non-null records for the same EPEN
#   4. Setting cleaned birthdate on all records for that EPEN

# Step 1: Count birthdate records per EPEN
birthdate_distinct <- stp_enrolment %>%
  filter(!is_blank(PSI_BIRTHDATE), !is_blank(ENCRYPTED_TRUE_PEN)) %>%
  count(ENCRYPTED_TRUE_PEN, PSI_BIRTHDATE, name = "NumBirthdateRecords")

# Step 2: Find EPENs with more than one distinct birthdate
multi_birthdate_epens <- birthdate_distinct %>%
  count(ENCRYPTED_TRUE_PEN, name = "N_Birthdates") %>%
  filter(N_Birthdates > 1) %>%
  pull(ENCRYPTED_TRUE_PEN)

# Step 3: For each EPEN with multiple birthdates, get min and max dates with counts
min_birthdates <- birthdate_distinct %>%
  group_by(ENCRYPTED_TRUE_PEN) %>%
  slice_min(PSI_BIRTHDATE, with_ties = FALSE) %>%
  ungroup() %>%
  select(ENCRYPTED_TRUE_PEN, MinPSIBirthdate = PSI_BIRTHDATE, NumMinBirthdateRecords = NumBirthdateRecords)

max_birthdates <- birthdate_distinct %>%
  group_by(ENCRYPTED_TRUE_PEN) %>%
  slice_max(PSI_BIRTHDATE, with_ties = FALSE) %>%
  ungroup() %>%
  select(ENCRYPTED_TRUE_PEN, MaxPSIBirthdate = PSI_BIRTHDATE, NumMaxBirthdateRecords = NumBirthdateRecords)

# Step 4: Join with LAST_SEEN_BIRTHDATE to determine which to use
# WHY: The logic prefers the birthdate that matches LAST_SEEN_BIRTHDATE.
# If that doesn't resolve it, uses the one with more records. Defaults to MIN.
last_seen_lookup <- stp_enrolment %>%
  filter(ENCRYPTED_TRUE_PEN %in% multi_birthdate_epens) %>%
  distinct(ENCRYPTED_TRUE_PEN, LAST_SEEN_BIRTHDATE)

birthdate_resolution <- tibble(ENCRYPTED_TRUE_PEN = multi_birthdate_epens) %>%
  inner_join(min_birthdates, by = "ENCRYPTED_TRUE_PEN") %>%
  inner_join(max_birthdates, by = "ENCRYPTED_TRUE_PEN") %>%
  inner_join(last_seen_lookup, by = "ENCRYPTED_TRUE_PEN") %>%
  mutate(
    UseMaxOrMin_FINAL = case_when(
      MaxPSIBirthdate == LAST_SEEN_BIRTHDATE ~ "MAX",
      NumMaxBirthdateRecords > NumMinBirthdateRecords ~ "MAX",
      NumMaxBirthdateRecords < NumMinBirthdateRecords ~ "MIN",
      TRUE ~ "MIN"
    ),
    psi_birthdate_cleaned = if_else(UseMaxOrMin_FINAL == "MAX", MaxPSIBirthdate, MinPSIBirthdate)
  ) %>%
  select(ENCRYPTED_TRUE_PEN, psi_birthdate_cleaned)

# Step 5: Fill null birthdates from non-null records for the same EPEN
# WHY: Some records for an EPEN have null birthdate while others have values.
# We find the non-null value and use it.
null_birthdate_epens <- stp_enrolment %>%
  filter(!is_blank(ENCRYPTED_TRUE_PEN), is_blank(PSI_BIRTHDATE)) %>%
  distinct(ENCRYPTED_TRUE_PEN)

nonnull_birthdates <- stp_enrolment %>%
  filter(!is_blank(ENCRYPTED_TRUE_PEN), !is_blank(PSI_BIRTHDATE)) %>%
  distinct(ENCRYPTED_TRUE_PEN, PSI_BIRTHDATE)

null_cleaned <- null_birthdate_epens %>%
  inner_join(nonnull_birthdates, by = "ENCRYPTED_TRUE_PEN") %>%
  left_join(birthdate_resolution %>% select(ENCRYPTED_TRUE_PEN, psi_birthdate_cleaned),
            by = "ENCRYPTED_TRUE_PEN") %>%
  group_by(ENCRYPTED_TRUE_PEN) %>%
  slice(1) %>%
  ungroup() %>%
  mutate(psi_birthdate_cleaned = if_else(is.na(psi_birthdate_cleaned), PSI_BIRTHDATE, psi_birthdate_cleaned)) %>%
  select(ENCRYPTED_TRUE_PEN, psi_birthdate_cleaned)

# Step 6: Apply cleaned birthdates to STP_Enrolment
# WHY: First apply the multi-birthdate resolution, then the null fill,
# then fall back to original PSI_BIRTHDATE if still null.
stp_enrolment <- stp_enrolment %>%
  left_join(birthdate_resolution, by = "ENCRYPTED_TRUE_PEN") %>%
  mutate(psi_birthdate_cleaned = coalesce(psi_birthdate_cleaned, PSI_BIRTHDATE))

# Apply null birthdate cleaning for records where psi_birthdate_cleaned is still blank
stp_enrolment <- stp_enrolment %>%
  left_join(null_cleaned %>% rename(psi_birthdate_null_cleaned = psi_birthdate_cleaned),
            by = "ENCRYPTED_TRUE_PEN") %>%
  mutate(
    psi_birthdate_cleaned = case_when(
      !is_blank(psi_birthdate_cleaned) & psi_birthdate_cleaned != "" ~ psi_birthdate_cleaned,
      !is.na(psi_birthdate_null_cleaned) & psi_birthdate_null_cleaned != "" ~ psi_birthdate_null_cleaned,
      TRUE ~ psi_birthdate_cleaned
    )
  ) %>%
  select(-psi_birthdate_null_cleaned)

# Final fallback: use PSI_BIRTHDATE where psi_birthdate_cleaned is still null/blank
stp_enrolment <- stp_enrolment %>%
  mutate(
    psi_birthdate_cleaned = case_when(
      (!is_blank(psi_birthdate_cleaned)) ~ psi_birthdate_cleaned,
      (!is_blank(PSI_BIRTHDATE)) ~ PSI_BIRTHDATE,
      TRUE ~ psi_birthdate_cleaned
    )
  )

# Sanity check on multi-birthdate records for STUID-based students
multi_stuid_birthdates <- stp_enrolment %>%
  filter(is_blank(ENCRYPTED_TRUE_PEN),
         !is_blank(PSI_BIRTHDATE),
         !is_blank(PSI_STUDENT_NUMBER),
         !is_blank(PSI_CODE)) %>%
  distinct(PSI_STUDENT_NUMBER, PSI_CODE, PSI_BIRTHDATE, psi_birthdate_cleaned) %>%
  count(PSI_STUDENT_NUMBER, PSI_CODE, name = "n") %>%
  filter(n > 1)


# ******************************************************************************
# Part 8: Write results to database
# ******************************************************************************
# WHY: The record type table, valid enrolment, and updated STP_Enrolment are
# written back to the database for downstream pipeline steps.

# Write STP_Enrolment_Record_Type
dbWriteTable(con, SQL(glue::glue('"{my_schema}"."STP_Enrolment_Record_Type"')),
             record_type, overwrite = TRUE)

# Write STP_Enrolment_Valid
dbWriteTable(con, SQL(glue::glue('"{my_schema}"."STP_Enrolment_Valid"')),
             stp_enrolment_valid, overwrite = TRUE)

# Update STP_Enrolment with cleaned birthdates and reformatted dates
# KEPT AS SQL: ALTER TABLE to add psi_birthdate_cleaned column
dbExecute(con, "ALTER TABLE STP_Enrolment ADD psi_birthdate_cleaned NVARCHAR(50) NULL")

# Update dates and birthdate in DB via batch update
# WHY: We update the DB table to match our R computations. Using a temp table
# join is more efficient than row-by-row updates.
# KEPT AS SQL: UPDATE...FROM for bulk column updates
stp_updates <- stp_enrolment %>%
  select(ID, PSI_BIRTHDATE, LAST_SEEN_BIRTHDATE, PSI_MIN_START_DATE,
         PSI_PROGRAM_EFFECTIVE_DATE, psi_birthdate_cleaned)

dbWriteTable(con, SQL(glue::glue('"{my_schema}"."tmp_stp_updates"')),
             stp_updates, overwrite = TRUE)

dbExecute(con, glue::glue("
  UPDATE STP_Enrolment
  SET PSI_BIRTHDATE = u.PSI_BIRTHDATE,
      LAST_SEEN_BIRTHDATE = u.LAST_SEEN_BIRTHDATE,
      PSI_MIN_START_DATE = u.PSI_MIN_START_DATE,
      PSI_PROGRAM_EFFECTIVE_DATE = u.PSI_PROGRAM_EFFECTIVE_DATE,
      psi_birthdate_cleaned = u.psi_birthdate_cleaned
  FROM tmp_stp_updates u
  WHERE STP_Enrolment.ID = u.ID;
"))

dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[tmp_stp_updates];"))


# ---- Verify tables exist ----
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Enrolment_Record_Type"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Enrolment"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Enrolment_Valid"')))

# ---- Disconnect ----
dbDisconnect(con)
