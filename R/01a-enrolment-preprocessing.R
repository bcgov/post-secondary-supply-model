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

# STP Enrolment Preprocessing: Workflow #1
# Description:
# Relies on: STP_Enrolment data table
# Creates tables: STP_Enrolment_Record_Type, STP_Enrolment_Valid, STP_Enrolment

library(tidyverse)
library(odbc)
library(DBI)

# ---- Configure LAN Paths and DB Connection -----
#lan <- config::get("lan")
#source("./sql/01-enrolment-preprocessing/01-enrolment-preprocessing-sql.R")
#source("./sql/01-enrolment-preprocessing/convert-date-scripts.R")
#source("./sql/01-enrolment-preprocessing/pssm-birthdate-cleaning.R")

## ---- extract data from SQL Server ----
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

if (!dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Enrolment_raw"')))) {
  stop(
    "STP_Enrolment table does not exist in the database. Please check the data load process."
  )
}

enrol_raw <- dbGetQuery(
  con,
  glue::glue("SELECT * FROM [{my_schema}].[STP_Enrolment_raw];")
)

enrol <- enrol_raw # save a copy while testing (the extract takes a long time) 8:42 AM

# ---- Initial Data Checks ----
# SQL -> R translation: qry00a to qry00d

enrol |>
  filter(
    ENCRYPTED_TRUE_PEN %in%
      c('', ' ', '(Unspecified)') |
      is.na(ENCRYPTED_TRUE_PEN)
  ) |>
  nrow()

enrol |> distinct(ENCRYPTED_TRUE_PEN) |> count()

enrol <- enrol |> mutate(ID = row_number()) # may not be required in R but keeping for consistency

# ---- Reformat yy-mm-dd to yyyy-mm-dd ----
# this section translates the SQL date conversion scripts to R
# source("./sql/01-enrolment-preprocessing/convert-date-scripts.R")

convert_date <- function(vec) {
  # handle edge cases as/if needed
  # Years 26-99 go to 19xx
  # Years 00-25 go to 20xx
  yy <- as.numeric(substr(vec, 1, 2))

  century_prefix <- case_when(
    is.na(yy) ~ NA_character_,
    yy < 24 ~ "20",
    TRUE ~ "19"
  )

  lubridate::ymd(paste0(century_prefix, vec))
}

date_cols <- c(
  "PSI_PROGRAM_EFFECTIVE_DATE",
  "PSI_MIN_START_DATE",
  "PSI_BIRTHDATE",
  "LAST_SEEN_BIRTHDATE"
)

enrol <- enrol |>
  mutate(
    across(
      .cols = date_cols,
      .fns = convert_date,
      .names = "{.col}"
    )
  )


# ---- Create Record Type Table ----
# this section handles the SQL -> R translation of the Record Type creation table
# queries from qry01 to qry08 series

# Record Status codes:
# 0 = Good
# 1 = Missing Student Number
# 2 = Developmental
# 3 = No PSI Transition
# 4 = Credential Only (No Enrolment Record)
# 5 = PSI_Outside_BC
# 6 = Skills Based
# 7 = Developmental CIP
# 8 = Recommendation for Certification

# ---- Define lookup table for ID/Record Status and populate with ID column and EPEN
#dbExecute(con, qry01_ExtractAllID_into_STP_Enrolment_Record_Type)

# ----- Find records with Record_Status = 1 and update look up table -----
# Keep records for NA or invalid PSI/CODE combos fields or NA/invalid PENs.
# Basically, no way to indentify the student.
# qry02 series

# Define the set of 'bad' string values
invalid_pen <- c('', ' ', '(Unspecified)')

rec_status_1 <- enrol |>
  anti_join(
    enrol |>
      filter(
        (!(PSI_STUDENT_NUMBER %in% invalid_pen) & # AND
          !(PSI_CODE %in% invalid_pen)) | # OR
          !(ENCRYPTED_TRUE_PEN %in% invalid_pen)
      ) |>
      select(ID),
    by = "ID"
  ) |>
  pull(ID)

# ----- Find records with Record_Status = 2 and update look up table -----
# qry03a and qry03b
rec_status_2 <- enrol |>
  filter(PSI_STUDY_LEVEL %in% c('DEVELOPMENTAL', 'Developmental')) |>
  pull(ID)


# ----- Find records with Record_Status = 6 and update look up table -----
# qry03c to qry03f series

rec_status_6 <- enrol %>%
  filter(
    PSI_CONTINUING_EDUCATION_COURSE_ONLY == 'Skills Crs Only',
    PSI_STUDY_LEVEL != 'Developmental',
    PSI_CREDENTIAL_CATEGORY %in% c('None', 'Other')
  ) |>
  filter(
    !((PSI_CODE %in% c('UFV', 'UCFV')) & (PSI_PROGRAM_CODE == 'TEACH ED'))
  ) |>
  filter(!ID %in% c(rec_status_1, rec_status_2)) |>
  pull(ID)

# qry03d series
cips <- c('21', '32', '33', '34', '35', '36', '37', '53', '89')
continuing_ed <- enrol |>
  # Temporarily add the two-digit CIP code for filtering
  mutate(CIP2 = str_sub(PSI_CIP_CODE, 1, 2)) |>
  filter(
    PSI_STUDY_LEVEL != 'Developmental',
    PSI_CONTINUING_EDUCATION_COURSE_ONLY != 'Skills Crs Only',
    PSI_CREDENTIAL_CATEGORY %in% c('None', 'Other'),
    CIP2 %in% cips
  ) |>
  filter(!ID %in% c(rec_status_1, rec_status_2, rec_status_6)) |>
  pull(ID)

rec_status_6 <- c(rec_status_6, continuing_ed)

continuing_ed_more <- enrol |>
  filter(
    grepl(
      "Continuing Education|Continuing Studies|Audit|^CE ",
      PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
      ignore.case = TRUE
    )
  ) |>
  filter(!ID %in% c(rec_status_2, rec_status_1, rec_status_6)) |>
  pull(ID)

rec_status_6 <- c(rec_status_6, continuing_ed_more)

# qry03e and qry03f
keep_skills_based <- enrol |>
  mutate(
    CIP2 = substr(PSI_CIP_CODE, 1, 2)
  ) |>
  filter(
    PSI_CONTINUING_EDUCATION_COURSE_ONLY == 'Skills Crs Only',
    PSI_STUDY_LEVEL != 'Developmental',
    !PSI_CREDENTIAL_CATEGORY %in% c('None', 'Other', 'Short Certificate'),
    !grepl(
      "Continuing Education|Continuing Studies|Audit|^CE ",
      PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
      ignore.case = TRUE
    )
  )

exclude_yes <- keep_skills_based |>
  filter(
    (PSI_CODE == 'SEL' &
      PSI_CREDENTIAL_PROGRAM_DESCRIPTION ==
        'Community, Corporate & International Development') |
      (PSI_CODE == 'NIC' & CIP2 %in% cips)
  ) |>
  pull(ID)

rec_status_0 <- keep_skills_based |> pull(ID) |> setdiff(exclude_yes)
rec_status_6 <- c(rec_status_6, exclude_yes)

# ----------------------------------------------------------------
# !!! TODO manual investigation done here in the past and requires a review
# leaving for now as has minimal impact on final distributions
# Not Translated to R.
# ----------------------------------------------------------------
#dbExecute(con, qry03g_create_table_SkillsBasedCourses)
#dbExecute(
#  con,
#  "ALTER TABLE tmp_tbl_SkillsBasedCourses ADD KEEP nvarchar(2) NULL;"
#)
#dbExecute(con, qry03g_b_Keep_More_Skills_Based)
#dbExecute(con, qry03g_c_Update_Keep_More_Skills_Based)
#dbExecute(con, qry03g_c2_Update_More_Selkirk)
#dbExecute(con, qry03g_d_EnrolCoursesSeen)
#dbExecute(con, qry03h_create_table_Suspect_Skills_Based)
#dbExecute(con, qry03i_Find_Suspect_Skills_Based)
#dbExecute(con, qry03i2_Drop_Suspect_Skills_Based) #see documentation, this is related to some manula work that wasn't done in 2023
#dbExecute(con, qry03j_Update_Suspect_Skills_Based)
# ----------------------------------------------------------------

# ---- Find records with Record_Status = 7 and update look up table ----
# qry 03k and qry03l series

rec_status_7 <- enrol |>
  mutate(
    CIP2 = substr(PSI_CIP_CODE, 1, 2)
  ) |>
  filter(
    PSI_CONTINUING_EDUCATION_COURSE_ONLY == 'Not Skills Crs Only',
    !ID %in% c(rec_status_0, rec_status_1, rec_status_2, rec_status_6),
    CIP2 %in% cips
  )

# ---- Find records with Record_Status = 3 and update look up table ----
# qry 04 series

rec_status_3 <- enrol |>
  filter(
    PSI_ENTRY_STATUS == 'No Transition',
    !ID %in% c(rec_status_0, rec_status_1, rec_status_2, rec_status_6)
  ) |>
  pull(ID)


# ---- Find records with Record_Status = 5 and update look up table ----
# qry 06 series
rec_status_5 <- enrol |>
  filter(ATTENDING_PSI_OUTSIDE_BC == 'Y') |>
  filter(
    !ID %in% c(rec_status_1, rec_status_2, rec_status_3, rec_status_6)
  ) |>
  pull(ID)

# ---- Set Remaining Records to Record_Status = 0 ----
# qry07 series
# TODO

# ----- Create table of Record Status = 0 only (Valid Enrolment) ----
# here, I'm pulling the one from decimal as a workaround for development.
# Because I'm not finished with the record status 6 yet

rec_status <- dbGetQuery(
  con,
  glue::glue("SELECT * FROM [{my_schema}].[STP_Enrolment_Record_Type]")
)

enrol <- dbGetQuery(
  con,
  glue::glue("SELECT * FROM [{my_schema}].[STP_Enrolment];")
)

enrol <- enrol |>
  left_join(
    rec_status |>
      select(ID, RecordStatus),
    by = "ID"
  )

# check count of records in STP_Enrolment_Valid associated with > 1 EPEN.
cat("Records associated with > 1 EPEN:")

enrol |>
  filter(RecordStatus == 0) |>
  distinct(PSI_CODE, PSI_STUDENT_NUMBER, ENCRYPTED_TRUE_PEN) |>
  group_by(PSI_CODE, PSI_STUDENT_NUMBER) |>
  summarise(n = n()) |>
  filter(n > 1)


# ---- Min Enrolment ----
# Find record with minimum enrollment sequence for each student per school year
# by ENCRYPTED_TRUE_PEN

enrol <- enrol |>
  mutate(is_first_start_date = FALSE, is_min_enrol_seq = FALSE)

q9 <- enrol |>
  select(
    ENCRYPTED_TRUE_PEN,
    PSI_MIN_START_DATE,
    PSI_SCHOOL_YEAR,
    PSI_ENROLMENT_SEQUENCE,
    RecordStatus,
    ID
  ) |>
  filter(RecordStatus == 0) |>
  filter(!ENCRYPTED_TRUE_PEN %in% invalid_pen) |>
  group_by(ENCRYPTED_TRUE_PEN, PSI_SCHOOL_YEAR) |>
  arrange(PSI_ENROLMENT_SEQUENCE, .by_group = TRUE) |>
  mutate(is_min_enrol_seq = row_number() == 1) |>
  ungroup()
q9 |> count(is_min_enrol_seq)

q10 <- enrol |>
  select(
    ENCRYPTED_TRUE_PEN,
    PSI_STUDENT_NUMBER,
    PSI_CODE,
    PSI_MIN_START_DATE,
    PSI_SCHOOL_YEAR,
    PSI_ENROLMENT_SEQUENCE,
    RecordStatus,
    ID,
  ) |>
  filter(RecordStatus == 0) |>
  filter(ENCRYPTED_TRUE_PEN %in% invalid_pen) |>
  group_by(PSI_STUDENT_NUMBER, PSI_CODE, PSI_SCHOOL_YEAR) |>
  arrange(PSI_ENROLMENT_SEQUENCE, .by_group = TRUE) |>
  mutate(is_min_enrol_seq_combo = row_number() == 1) |>
  ungroup()
q10 |> count(is_min_enrol_seq_combo)

q12 <- enrol |>
  select(
    ENCRYPTED_TRUE_PEN,
    PSI_MIN_START_DATE,
    PSI_SCHOOL_YEAR,
    PSI_ENROLMENT_SEQUENCE,
    RecordStatus,
    ID
  ) |>
  filter(RecordStatus == 0) |>
  filter(!ENCRYPTED_TRUE_PEN %in% invalid_pen) |>
  group_by(ENCRYPTED_TRUE_PEN) |>
  arrange(PSI_MIN_START_DATE, .by_group = TRUE) |>
  mutate(is_first_enrol = row_number() == 1)


q13 <- enrol |>
  select(
    ENCRYPTED_TRUE_PEN,
    PSI_STUDENT_NUMBER,
    PSI_CODE,
    PSI_MIN_START_DATE,
    PSI_SCHOOL_YEAR,
    PSI_ENROLMENT_SEQUENCE,
    RecordStatus,
    ID
  ) |>
  filter(RecordStatus == 0) |>
  filter(ENCRYPTED_TRUE_PEN %in% invalid_pen) |>
  group_by(PSI_STUDENT_NUMBER, PSI_CODE) |>
  arrange(PSI_MIN_START_DATE, .by_group = TRUE) |>
  mutate(is_first_enrol = row_number() == 1)


enrol |> count(is_first_enrol)
enrol |> count(is_min_enrol_seq_combo)


enrol_valid <- enrol |>
  select(ID, RecordStatus, is_first_enrol, is_min_enrol_seq)

#dbExecute(con, qry09a_MinEnrolmentPEN)
#dbExecute(con, qry09b_MinEnrolmentPEN)
#dbExecute(con, qry09c_MinEnrolmentPEN)

# by PSI_CODE/PSI_STUDENT_NUMBER combo for students records with null ENCRYPTED_TRUE_PEN's
#dbExecute(con, qry10a_MinEnrolmentSTUID)
#dbExecute(con, qry10b_MinEnrolmentSTUID)
#dbExecute(con, qry10c_MinEnrolmentSTUID)

# Flag each record in STP_Enrolment_Record_Type as min enrollment (TRUE = 1, FALSE  = 0)
#dbExecute(con, qry11a_Update_MinEnrolmentPEN)
#dbExecute(con, qry11b_Update_MinEnrolmentSTUID)
#dbExecute(con, qry11c_Update_MinEnrolment_NA)

# ---- First Enrollment Date ----
# Find earliest enrollment record for each student per school year
# by ENCRYPTED_TRUE_PEN
#dbExecute(con, qry12a_FirstEnrolmentPEN)
#dbExecute(con, qry12b_FirstEnrolmentPEN)
#dbExecute(con, qry12c_FirstEnrolmentPEN)

# by PSI_CODE/PSI_STUDENT_NUMBER combo for students records with null ENCRYPTED_TRUE_PEN's
dbExecute(con, qry13a_FirstEnrolmentSTUID)
dbExecute(con, qry13b_FirstEnrolmentSTUID)
dbExecute(con, qry13c_FirstEnrolmentSTUID)


# Flag each record in STP_Enrolment_Record_Type as first enrollment (TRUE = 1, FALSE  = 0)
dbExecute(con, qry14a_Update_FirstEnrolmentPEN)
dbExecute(con, qry14b_Update_FirstEnrolmentSTUID)
dbExecute(con, qry14c_Update_FirstEnrolmentNA)


# ---- Clean Birthdates ----
dbExecute(con, qry01_BirthdateCleaning)
dbExecute(con, qry02_BirthdateCleaning)
dbExecute(con, qry03_BirthdateCleaning)
dbExecute(con, qry04_BirthdateCleaning)
dbExecute(
  con,
  "ALTER table tmp_MaxPSIBirthdate ADD NumBirthdateRecords INT NULL"
)
dbExecute(
  con,
  "ALTER table tmp_MinPSIBirthdate ADD NumBirthdateRecords INT NULL"
)
dbExecute(con, qry05_BirthdateCleaning)
dbExecute(con, qry06_BirthdateCleaning)
dbExecute(
  con,
  "ALTER table tmp_MoreThanOne_Birthdate 
                ADD MinPSIBirthdate NVARCHAR(50) NULL,
                    NumMinBirthdateRecords INT NULL,
                    MaxPSIBirthdate NVARCHAR(50) NULL,
                    NumMaxBirthdateRecords INT NULL"
)
dbExecute(con, qry07a_BirthdateCleaning)
dbExecute(con, qry07b_BirthdateCleaning)
dbExecute(con, "DROP TABLE tmp_MinPSIBirthdate")
dbExecute(con, "DROP TABLE tmp_MaxPSIBirthdate")

dbExecute(
  con,
  "ALTER table tmp_MoreThanOne_Birthdate 
                ADD LastSeenBirthdate NVARCHAR(50) NULL;"
)
dbExecute(con, qry08_BirthdateCleaning)
dbExecute(
  con,
  "ALTER table tmp_MoreThanOne_Birthdate 
                ADD UseMaxOrMin_FINAL NVARCHAR(50) NULL;"
)
dbExecute(con, qry09_BirthdateCleaning)
dbExecute(
  con,
  "ALTER table tmp_MoreThanOne_Birthdate 
                ADD psi_birthdate_cleaned NVARCHAR(50) NULL;"
)
dbExecute(con, qry10_BirthdateCleaning)
dbExecute(con, qry11_BirthdateCleaning)

dbExecute(
  con,
  "ALTER TABLE STP_Enrolment ADD psi_birthdate_cleaned NVARCHAR(50) NULL"
)

#Update STP Enrolment with birthdates for those EPENS which have > 1 birthdate records
dbExecute(con, qry12_BirthdateCleaning)

# some records have a null PSI_BIRTHDATE, search for non-null PSI_BIRTHDATE for these EPENS
dbExecute(con, qry13_BirthdateCleaning)
dbExecute(con, qry14_BirthdateCleaning)
dbExecute(con, qry15_BirthdateCleaning)
dbExecute(
  con,
  "ALTER TABLE tmp_NullBirthdateCleaned ADD psi_birthdate_cleaned NVARCHAR(50) NULL"
)
dbExecute(con, qry16_BirthdateCleaning)
dbExecute(con, qry17_BirthdateCleaning)

# Update STP_Enrolment with birthdates found in non-null records
dbExecute(con, qry18_BirthdateCleaning)
dbExecute(con, qry19_BirthdateCleaning)

# sanity check on psi_birthdate_cleaned - finish this and save report
dbExecute(con, qry20_BirthdateCleaning)
dbGetQuery(con, qry21_BirthdateCleaning)

# ---- Clean Up and check tables to keep ----
dbExecute(con, "DROP TABLE tmp_BirthDate")
dbExecute(con, "DROP TABLE tmp_MoreThanOne_Birthdate")
dbExecute(con, "DROP TABLE tmp_NullBirthdate")
dbExecute(con, "DROP TABLE tmp_NonNullBirthdate")
dbExecute(con, "DROP TABLE tmp_NullBirthdateCleaned")

dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Enrolment_Record_Type"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Enrolment"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Enrolment_Valid"')))

dbDisconnect(con)
