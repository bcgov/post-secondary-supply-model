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

# if (!dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Enrolment_raw"')))) {
#   stop(
#     "STP_Enrolment table does not exist in the database. Please check the data load process."
#   )
# }
#
# stp_enrolment <- dbGetQuery(
#   con,
#   glue::glue("SELECT * FROM [{my_schema}].[STP_Enrolment];")
# )
#

# stp_enrolment <- stp_enrolment |> mutate(ID = row_number()) # may not be required in R but keeping for consistency

stp_enrolment <- dbGetQuery(
  con,
  glue::glue("SELECT * FROM [{my_schema}].[STP_Enrolment];")
) |>
  select(-psi_birthdate_cleaned)

## -----------------------------------------------------------------------------------------------

## --------------------------------------Initial Data Checks--------------------------------------
## reference: source("./sql/01-enrolment-preprocessing/convert-date-scripts.R")
##   qry00a to qry00d

stp_enrolment |>
  filter(
    ENCRYPTED_TRUE_PEN %in%
      c("", " ", "(Unspecified)") |
      is.na(ENCRYPTED_TRUE_PEN)
  ) |>
  nrow()

stp_enrolment |> distinct(ENCRYPTED_TRUE_PEN) |> count()


# -------------------------------------------------------------------------------------------------

## --------------------------------------Reformat yy-mm-dd to yyyy-mm-dd---------------------------
## reference: source("./sql/01-enrolment-preprocessing/convert-date-scripts.R")
## all queries in the file

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

stp_enrolment <- stp_enrolment |>
  mutate(
    across(
      .cols = date_cols,
      .fns = convert_date,
      .names = "{.col}"
    )
  )
## ------------------------------------------------------------------------------------------------

## --------------------------------------- Create Record Type Table -------------------------------
## reference: source("./sql/01-enrolment-preprocessing/01-enrolment-preprocessing.R")
##   qry01 to qry07 series

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
# original SQl used patterns %Continuing Education and %Continuing Studies
invalid_pen <- c("", " ", "(Unspecified)")
cips <- c("21", "32", "33", "34", "35", "36", "37", "53", "89")
ce_pattern <- "Continuing Education|Continuing Studies|Audit|^CE "

stp_enrolment_record_type <- stp_enrolment |>
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


stp_enrolment_record_type <- stp_enrolment_record_type |>
  mutate(
    RecordStatus = case_when(
      # Record Status 1: qry02a to qry02c
      (PSI_STUDENT_NUMBER %in% invalid_pen | PSI_CODE %in% invalid_pen) &
        ENCRYPTED_TRUE_PEN %in% invalid_pen ~ 1,

      # Record Status 2: qry03a and qry03b
      toupper(PSI_STUDY_LEVEL) == "DEVELOPMENTAL" ~ 2,

      # Record Status 6: qry03c to qry03j
      PSI_CONTINUING_EDUCATION_COURSE_ONLY == "Skills Crs Only" &
        PSI_CREDENTIAL_CATEGORY %in% c("None", "Other") &
        !(PSI_CODE %in% c("UFV", "UCFV") & PSI_PROGRAM_CODE == "TEACH ED") ~ 6,

      # More Record Status 6:
      str_detect(
        PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
        regex(ce_pattern, ignore_case = TRUE)
      ) ~ 6,

      # More Record Status 6:
      (PSI_CREDENTIAL_CATEGORY %in% c("None", "Other") & CIP2 %in% cips) ~ 6,

      # More Record Status 6:
      PSI_CONTINUING_EDUCATION_COURSE_ONLY == "Skills Crs Only" &
        !PSI_CREDENTIAL_CATEGORY %in% c("None", "Other", "Short Certificate") &
        ((PSI_CODE == "SEL" &
          PSI_CREDENTIAL_PROGRAM_DESCRIPTION ==
            "Community, Corporate & International Development") |
          (PSI_CODE == "NIC" & CIP2 %in% cips)) ~ 6,

      # Record Status 7: qry03k
      PSI_CONTINUING_EDUCATION_COURSE_ONLY == "Not Skills Crs Only" &
        CIP2 %in% cips ~ 7, # qry 03k and qry03l series

      # Record Status 3: qry04a to qry04b
      PSI_ENTRY_STATUS == "No Transition" ~ 3,

      # Record Status 5: qry06a to
      ATTENDING_PSI_OUTSIDE_BC == "Y" ~ 5,

      # DEFAULT: Fallback for all other records
      TRUE ~ 0
    )
  ) |>
  select(ID, RecordStatus)

# Notes: in the SQL queries from 2019 and earlier, some manual investigation was done to
# find more skills based courses and/or keep some that were excluded.  The manual
# investigation resulted in a table with a column "keep".  This was used to further
# refine the record status (affcting only record status 0 and 6).
# The affected queries are: qry03g, 03g_b, 03g_c, 03g_c2, 03_d, 03h, 03i, 03i2, 03j
# for now, pull the final enrolement record type table from decimal to keep
# coding.

sql <- glue::glue("SELECT * FROM [{my_schema}].[STP_Enrolment_Record_Type]")
stp_enrolment_record_type <- dbGetQuery(con, sql) |>
  select(ID, RecordStatus) # we may need to change the column names later

## ------------------------------------------------------------------------------------------------

## ------------------------------------------------------------------------------------------------

## --------------------------------------- Create Valid Enrolment Table ---------------------------
## ---- Create table of Record Status = 0 only (Valid Enrolment) ----

stp_enrolment_valid <- stp_enrolment |>
  select(
    ID,
    ENCRYPTED_TRUE_PEN,
    PSI_STUDENT_NUMBER,
    PSI_CODE,
    PSI_MIN_START_DATE,
    PSI_SCHOOL_YEAR,
    PSI_ENROLMENT_SEQUENCE
  ) |>
  inner_join(stp_enrolment_record_type, by = join_by(ID)) |>
  filter(RecordStatus == 0) |>
  select(-RecordStatus)

## ------------------------------------------------------------------------------------------------

## ------------------------------------- Min Enrolment --------------------------------------------
## reference: source("./sql/01-enrolment-preprocessing/01-enrolment-preprocessing.R")
## qry09 to qry14
# Notes: n a handful of cases, the SQL version improperly orders records with PSI_ENROLMENT_SEQUENCE == 10 and 11.
# R's arrange() handles them properly,

# Logic for valid PEN's
valid_pen_data <- stp_enrolment_valid |>
  filter(!ENCRYPTED_TRUE_PEN %in% invalid_pen) |>
  group_by(ENCRYPTED_TRUE_PEN) |>
  arrange(
    PSI_MIN_START_DATE,
    as.numeric(PSI_ENROLMENT_SEQUENCE),
    as.numeric(ID)
  ) |>
  mutate(is_first_enrol = row_number() == 1) |>
  group_by(ENCRYPTED_TRUE_PEN, PSI_SCHOOL_YEAR) |>
  mutate(is_min_enrol_seq = row_number() == 1) |>
  ungroup()

# Logic for Invalid PEN's (Student Number + PSI Code Combo)
invalid_pen_data <- stp_enrolment_valid |>
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

# Combine - this should be the same as the old stp_enrolment_record_type
stp_enrolment_valid_final <- bind_rows(valid_pen_data, invalid_pen_data) |>
  mutate(across(starts_with("is_"), ~ replace_na(.x, FALSE))) |>
  mutate(
    is_min_enrol = if_else((is_min_enrol_seq | is_min_enrol_seq_combo), 1, 0),
    is_first_enrol = if_else((is_first_enrol | is_first_enrol_combo), 1, 0)
  ) |>
  select(ID, is_min_enrol, is_first_enrol)

stp_enrolment_record_type <- stp_enrolment_record_type |>
  left_join(stp_enrolment_valid_final) |>
  mutate(across(starts_with("is_"), ~ replace_na(.x, 0)))

stp_enrolment_record_type |> count(RecordStatus, is_min_enrol, is_first_enrol)

## ------------------------------------------------------------------------------------------------

## ------------------------------------- Clean Birthdates -----------------------------------------
## reference: source("./sql/01-enrolment-preprocessing/pssm-birthdate-cleaning.R")
## qry01 to qry11

# qry01 to qry08
birthdate_cleaning_summary <- stp_enrolment |>
  select(ENCRYPTED_TRUE_PEN, PSI_BIRTHDATE, LAST_SEEN_BIRTHDATE) |>
  filter(
    !PSI_BIRTHDATE %in% c("", " ", "(Unspecified)"),
    !ENCRYPTED_TRUE_PEN %in% c("", " ", "(Unspecified)")
  ) |>
  group_by(ENCRYPTED_TRUE_PEN) |>
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

      # Tie-breaker 1: Match the "Last Seen" date
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

stp_enrolment <- stp_enrolment |>
  left_join(
    birthdate_update,
    by = "ENCRYPTED_TRUE_PEN"
  ) |>
  mutate(PSI_BIRTHDATE_FINAL = coalesce(psi_birthdate_cleaned, PSI_BIRTHDATE))
# I think this _FINAL version should be renamed to _CLEANED - confirm??

tables_to_keep <- c(
  "stp_enrolment",
  "stp_credential",
  "stp_enrolment_record_type",
  "stp_credential_record_type",
  "stp_enrolment_valid"
)

rm(list = setdiff(ls(), tables_to_keep))
