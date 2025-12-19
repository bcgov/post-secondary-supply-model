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
# source("./sql/01-enrolment-preprocessing/01-enrolment-preprocessing-sql.R")
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

# hard coded values
invalid_pen <- c('', ' ', '(Unspecified)')
cips <- c('21', '32', '33', '34', '35', '36', '37', '53', '89')
ce_pattern = "Continuing Education|Continuing Studies|Audit|^CE " # original SQl used patterns %Continuing Education and %Continuing Studies

enrol_rec_status <- enrol |>
  select(
    ID,
    ENCRYPTED_TRUE_PEN,
    ATTENDING_PSI_OUTSIDE_BC,
    PSI_CIP_CODE,
    PSI_CODE,
    PSI_CONTINUING_EDUCATION_COURSE_ONLY,
    PSI_CREDENTIAL_CATEGORY,
    PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    PSI_ENROLMENT_SEQUENCE,
    PSI_ENTRY_STATUS,
    PSI_MIN_START_DATE,
    PSI_PROGRAM_CODE,
    PSI_SCHOOL_YEAR,
    PSI_STUDENT_NUMBER,
    PSI_STUDY_LEVEL
  ) |>
  mutate(CIP2 = str_sub(PSI_CIP_CODE, 1, 2))


enrol_rec_status <- enrol_rec_status |>
  mutate(
    # Start the master classification
    rec_type = case_when(
      # 1. STATUS 1: ID Invalidity (inverted logic)
      ((PSI_STUDENT_NUMBER %in% invalid_pen | PSI_CODE %in% invalid_pen) &
        ENCRYPTED_TRUE_PEN %in% invalid_pen) ~ 1,

      # 2. STATUS 2: Developmental
      toupper(PSI_STUDY_LEVEL) == 'DEVELOPMENTAL' ~ 2,

      # 3. STATUS 6: Skills Based Courses (qry03c)
      (PSI_CONTINUING_EDUCATION_COURSE_ONLY == 'Skills Crs Only' &
        PSI_CREDENTIAL_CATEGORY %in% c('None', 'Other') &
        !((PSI_CODE %in% c('UFV', 'UCFV')) &
          (PSI_PROGRAM_CODE == 'TEACH ED'))) ~ 6,

      # 4. STATUS 6: Skills Based Courses - Continuing Ed
      str_detect(
        PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
        regex(ce_pattern, ignore_case = TRUE)
      ) ~ 6,

      # 5. STATUS 6: Skills Based Courses - More Continuing Ed (CIP based - qry03d)
      (PSI_CREDENTIAL_CATEGORY %in% c('None', 'Other') & CIP2 %in% cips) ~ 6,

      # 6. STATUS 6: Specific Skills-Base
      # note, this actually misses a number of courses previously marked as Developmental.
      # I dont think this matters since we end up only using Record Type = 0, anyways.
      (PSI_CONTINUING_EDUCATION_COURSE_ONLY == 'Skills Crs Only' &
        !PSI_CREDENTIAL_CATEGORY %in% c('None', 'Other', 'Short Certificate') &
        ((PSI_CODE == 'SEL' &
          PSI_CREDENTIAL_PROGRAM_DESCRIPTION ==
            'Community, Corporate & International Development') |
          (PSI_CODE == 'NIC' & CIP2 %in% cips))) ~ 6,

      # 7. STATUS 7: Developmental CIP
      PSI_CONTINUING_EDUCATION_COURSE_ONLY == 'Not Skills Crs Only' &
        CIP2 %in% cips ~ 7, # qry 03k and qry03l series

      # 8. STATUS 3: No PSI Transition
      PSI_ENTRY_STATUS == 'No Transition' ~ 3, # qry 04 series

      # 9. STATUS 5: PSI_Outside_BC
      ATTENDING_PSI_OUTSIDE_BC == 'Y' ~ 5, # qry 06 series

      # 7. DEFAULT: Fallback for all other records
      TRUE ~ 0
    )
  )

# !!! TODO manual investigation done here in the past and requires a review
# leaving for now as has minimal impact on final distributions
# Not Translated to R.
#
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

## Set Remaining Records to Record_Status = 0 ----
# qry07 series
# TODO

## ------------------------------------------------------------------------------------------------

## --------------------------------------- Create Valid Enrolment Table ---------------------------
## ---- Create table of Record Status = 0 only (Valid Enrolment) ----

valid_enrol <- enrol_rec_status |>
  select(
    ID,
    rec_type,
    ENCRYPTED_TRUE_PEN,
    PSI_STUDENT_NUMBER,
    PSI_CODE,
    PSI_MIN_START_DATE,
    PSI_SCHOOL_YEAR,
    PSI_ENROLMENT_SEQUENCE
  ) |>
  filter(rec_type == 0)


# here, I'm pulling the one from decimal as a workaround for development.
# Because I'm not finished with the record status 6 yet

sql <- glue::glue(
  "
SELECT R.ID
  , R.RecordStatus as rec_type
  , E. ENCRYPTED_TRUE_PEN
  , E. PSI_STUDENT_NUMBER
  , E. PSI_CODE
  , E. PSI_MIN_START_DATE
  , E. PSI_SCHOOL_YEAR
  , E. PSI_ENROLMENT_SEQUENCE
FROM [{my_schema}].[STP_Enrolment_Record_Type] R
INNER JOIN [{my_schema}].[STP_Enrolment] E
ON E.ID = R.ID
WHERE RecordStatus = 0;"
)

valid_enrol <- dbGetQuery(con, sql)


sql <- glue::glue("SELECT * FROM [{my_schema}].[STP_Enrolment_Record_Type]")
enrol_rec_status <- dbGetQuery(con, sql)
dbDisconnect(con)

## ------------------------------------------------------------------------------------------------

## ------------------------------------- Min Enrolment --------------------------------------------
# In a handful of cases, the SQL version improperly orders records with PSI_ENROLMENT_SEQUENCE == 10 and 11.
# R's arrange() handles them properly,
## qry09 to qry14

# Logic for valid PEN's
valid_pen_data <- valid_enrol |>
  filter(!ENCRYPTED_TRUE_PEN %in% invalid_pen) |>
  group_by(ENCRYPTED_TRUE_PEN) |>
  arrange(
    PSI_MIN_START_DATE,
    as.numeric(PSI_ENROLMENT_SEQUENCE),
    as.numeric(ID)
  ) |>
  mutate(is_first_enrol = row_number() == 1) |>
  # Standard grouping for the specific year-level flag
  group_by(ENCRYPTED_TRUE_PEN, PSI_SCHOOL_YEAR) |>
  mutate(is_min_enrol_seq = row_number() == 1) |>
  ungroup()

# Logic for Invalid PEN's (Student Number + PSI Code Combo)
invalid_pen_data <- valid_enrol |>
  filter(ENCRYPTED_TRUE_PEN %in% invalid_pen) |>
  group_by(PSI_STUDENT_NUMBER, PSI_CODE) |>
  arrange(
    PSI_MIN_START_DATE,
    as.numeric(PSI_ENROLMENT_SEQUENCE),
    as.numeric(ID)
  ) |>
  mutate(is_first_enrol_combo = row_number() == 1) |>
  group_by(PSI_STUDENT_NUMBER, PSI_CODE, PSI_SCHOOL_YEAR) |>
  mutate(is_min_enrol_seq_combo = row_number() == 1) |>
  ungroup()

# Combine
valid_enrol_final <- bind_rows(valid_pen_data, invalid_pen_data) |>
  mutate(across(starts_with("is_"), ~ replace_na(.x, FALSE))) |>
  mutate(
    is_min_enrol = if_else((is_min_enrol_seq | is_min_enrol_seq_combo), 1, 0),
    is_first_enrol = if_else((is_first_enrol | is_first_enrol_combo), 1, 0)
  ) |>
  select(ID, rec_type, is_min_enrol, is_first_enrol)

valid_enrol_final |> count(rec_type, is_min_enrol, is_first_enrol)
enrol_rec_status |> count(rec_type, is_min_enrol, is_first_enrol)

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
  #filter(n_distinct(PSI_BIRTHDATE) > 1) |>
  group_by(ENCRYPTED_TRUE_PEN, PSI_BIRTHDATE) |>
  summarize(
    NBirthdateRecords = n(),
    LastSeenBirthdate = first(LAST_SEEN_BIRTHDATE), #should only be one "last seen" per student
    .groups = "drop_last"
  ) |>
  summarize(
    DistinctBirthdates = n(), # Useful for auditing
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
    psi_birthdate_cleaned = case_when(
      # If they only have one date, use it
      MinPSIBirthdate == MaxPSIBirthdate ~ MinPSIBirthdate,

      # Tie-breaker 1: Match the 'Last Seen' date
      MaxPSIBirthdate == LastSeenBirthdate ~ MaxPSIBirthdate,
      #MinPSIBirthdate == LastSeenBirthdate ~ MinPSIBirthdate, # old logic didn't include this

      # Tie-breaker 2: Use the date that appears most frequently
      NumMaxBirthdateRecords > NumMinBirthdateRecords ~ MaxPSIBirthdate,
      NumMaxBirthdateRecords < NumMinBirthdateRecords ~ MinPSIBirthdate,

      # Default fallback
      TRUE ~ MinPSIBirthdate
    )
  ) |>
  select(ENCRYPTED_TRUE_PEN, psi_birthdate_cleaned)

enrol <- enrol |>
  left_join(
    birthdate_update,
    by = "ENCRYPTED_TRUE_PEN"
  ) |>
  mutate(PSI_BIRTHDATE_FINAL = coalesce(psi_birthdate_cleaned, PSI_BIRTHDATE)) # or PSI_BIRTHDATE = coalesce....
