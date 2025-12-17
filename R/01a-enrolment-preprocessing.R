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

## ---------------------------------- extract data from SQL Server ------------------------------

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

enrol <- enrol_raw # save a copy while testing
## -----------------------------------------------------------------------------------------------

## --------------------------------------Initial Data Checks--------------------------------------
# qry00a to qry00d

enrol |>
  filter(
    ENCRYPTED_TRUE_PEN %in%
      c('', ' ', '(Unspecified)') |
      is.na(ENCRYPTED_TRUE_PEN)
  ) |>
  nrow()

enrol |> distinct(ENCRYPTED_TRUE_PEN) |> count()

enrol <- enrol |> mutate(ID = row_number()) # may not be required in R but keeping for consistency
# -------------------------------------------------------------------------------------------------

## --------------------------------------Reformat yy-mm-dd to yyyy-mm-dd---------------------------
## source("./sql/01-enrolment-preprocessing/convert-date-scripts.R")

convert_date <- function(vec) {
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
## ------------------------------------------------------------------------------------------------

## --------------------------------------- Create Record Type Table -------------------------------
# surce("./sql/01-enrolment-preprocessing/01-enrolment-preprocessing-sql.R")
# qry01 to qry08 series

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

## Find records with Record_Status = 1
# qry02 series

# Define the set of 'bad' string values
invalid_pen <- c('', ' ', '(Unspecified)')
cips <- c('21', '32', '33', '34', '35', '36', '37', '53', '89')

rec_status_1 <-
  enrol |>
  filter(
    ((PSI_STUDENT_NUMBER %in% invalid_pen) | (PSI_CODE %in% invalid_pen)) &
      (ENCRYPTED_TRUE_PEN %in% invalid_pen)
  ) |>
  pull(ID)

## Find records with Record_Status = 2
# qry03a and qry03b
rec_status_2 <- enrol |>
  filter(PSI_STUDY_LEVEL %in% c('DEVELOPMENTAL', 'Developmental')) |>
  pull(ID)


## Find records with Record_Status = 6
# qry03c to qry03f series

rec_status_6 <- enrol |>
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

## ---------------------------------------------------------------
# !!! TODO manual investigation done here in the past and requires a review
# leaving for now as has minimal impact on final distributions
# Not Translated to R.
## ---------------------------------------------------------------
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
## ---------------------------------------------------------------

## Find records with Record_Status = 7
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

## Find records with Record_Status = 3
# qry 04 series

rec_status_3 <- enrol |>
  filter(
    PSI_ENTRY_STATUS == 'No Transition',
    !ID %in% c(rec_status_0, rec_status_1, rec_status_2, rec_status_6)
  ) |>
  pull(ID)


## Find records with Record_Status = 5
# qry 06 series
rec_status_5 <- enrol |>
  filter(ATTENDING_PSI_OUTSIDE_BC == 'Y') |>
  filter(
    !ID %in% c(rec_status_1, rec_status_2, rec_status_3, rec_status_6)
  ) |>
  pull(ID)

## Set Remaining Records to Record_Status = 0 ----
# qry07 series
# TODO

## ------------------------------------------------------------------------------------------------

## --------------------------------------- Create Valid Enrolment Table ---------------------------

## ---- Create table of Record Status = 0 only (Valid Enrolment) ----
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
## ------------------------------------------------------------------------------------------------

## ------------------------------------- Min Enrolment --------------------------------------------
## qry09 to qry14
# Find record with minimum enrollment sequence for each student per school year
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
  arrange(PSI_ENROLMENT_SEQUENCE, ID) |>
  mutate(is_min_enrol_seq = row_number() == 1) |>
  ungroup()

q9 |> filter(is_min_enrol_seq == 1) # same as qry09c, with a few extra cols

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
  arrange(PSI_ENROLMENT_SEQUENCE, ID) |>
  mutate(is_min_enrol_seq_combo = row_number() == 1) |>
  ungroup()

q10 |> filter(is_min_enrol_seq_combo == 1) # same as qry10c, with a few extra cols

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
  arrange(PSI_MIN_START_DATE, PSI_ENROLMENT_SEQUENCE, ID) |>
  mutate(is_first_enrol = row_number() == 1) |>
  ungroup()

q12 |> filter(is_first_enrol == 1) # same as qry12c, with a few extra cols.

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
  arrange(PSI_MIN_START_DATE, PSI_ENROLMENT_SEQUENCE, ID) |>
  mutate(is_first_enrol_combo = row_number() == 1) |>
  ungroup()

q13 |> filter(is_first_enrol_combo == TRUE) # same as qry13c, with a few extra cols.

## ------------------------------------------------------------------------------------------------

## ------------------------------------- Clean Birthdates -----------------------------------------
# source("./sql/01-enrolment-preprocessing/pssm-birthdate-cleaning.R")
# qry01 to qry08
birthdate_cleaning_summary <- enrol |>
  select(ENCRYPTED_TRUE_PEN, PSI_BIRTHDATE, LAST_SEEN_BIRTHDATE) |>
  filter(
    !PSI_BIRTHDATE %in% c('', ' ', '(Unspecified)'),
    !ENCRYPTED_TRUE_PEN %in% c('', ' ', '(Unspecified)')
  ) |>
  group_by(ENCRYPTED_TRUE_PEN) |>
  filter(n_distinct(PSI_BIRTHDATE) > 1) |>
  group_by(ENCRYPTED_TRUE_PEN, PSI_BIRTHDATE) |>
  summarize(
    NBirthdateRecords = n(),
    LastSeenBirthdate = first(LAST_SEEN_BIRTHDATE), #should only be one "last seen" per student
    .groups = "drop_last"
  ) |>
  summarize(
    MinPSIBirthdate = min(PSI_BIRTHDATE),
    MaxPSIBirthdate = max(PSI_BIRTHDATE),
    NumMinBirthdateRecords = NBirthdateRecords[
      PSI_BIRTHDATE == min(PSI_BIRTHDATE)
    ][1],
    NumMaxBirthdateRecords = NBirthdateRecords[
      PSI_BIRTHDATE == max(PSI_BIRTHDATE)
    ][1],
    LastSeenBirthdate = first(LastSeenBirthdate)
  ) |>
  ungroup()

#qry09 to qry11
birthdate_update <- birthdate_cleaning_summary |>
  mutate(
    PSI_Birthdate_cleaned = case_when(
      MaxPSIBirthdate == LastSeenBirthdate ~ MaxPSIBirthdate,
      NumMaxBirthdateRecords > NumMinBirthdateRecords ~ MaxPSIBirthdate,
      NumMaxBirthdateRecords < NumMinBirthdateRecords ~ MinPSIBirthdate,
      TRUE ~ MinPSIBirthdate
    )
  ) |>
  select(ENCRYPTED_TRUE_PEN, PSI_Birthdate_cleaned)

#qry012
enrol <- enrol |>
  left_join(
    birthdate_update,
    by = "ENCRYPTED_TRUE_PEN"
  ) |>
  mutate(
    psi_birthdate_cleaned = coalesce(PSI_Birthdate_cleaned, PSI_BIRTHDATE)
  ) |>
  select(-PSI_Birthdate_cleaned)

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


dbDisconnect(con)
