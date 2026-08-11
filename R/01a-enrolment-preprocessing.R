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

log_info("==== 01a-enrolment-preprocessing.R START ====")

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
# currently repurposing data from 2023 run.  When running next update change this code to pull
# from the original raw dataset and then untoggle the commented sections below to reformat dates,
# and add an ID field.
## -----------------------------------------------------------------------------------------------

stp_enrolment <- dbGetQuery(
  con,
  glue::glue(
    "SELECT
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
    PSI_GENDER,
    PSI_MIN_START_DATE,
    PSI_PROGRAM_CODE,
    PSI_PROGRAM_EFFECTIVE_DATE,
    PSI_SCHOOL_YEAR,
    PSI_STUDENT_NUMBER,
    PSI_STUDENT_POSTAL_CODE_CURRENT,
    PSI_STUDY_LEVEL,
    PSI_VISA_STATUS,
    PSI_BIRTHDATE,
    LAST_SEEN_BIRTHDATE
  FROM [{my_schema}].[STP_Enrolment];"
  )
)
log_info(glue::glue(
  "Loaded STP_Enrolment: {nrow(stp_enrolment)} rows, {ncol(stp_enrolment)} columns"
))


## --------------------------------------Initial Data Checks--------------------------------------
## reference: source("./sql/01-enrolment-preprocessing/01-enrolment-preprocessing-sql.R")
##   qry00a to qry00d
## -----------------------------------------------------------------------------------------------

invalid_pen_count <- stp_enrolment |>
  filter(
    ENCRYPTED_TRUE_PEN %in%
      c("", " ", "(Unspecified)") |
      is.na(ENCRYPTED_TRUE_PEN)
  ) |>
  nrow()
log_info(glue::glue(
  "Rows with invalid/missing ENCRYPTED_TRUE_PEN: {invalid_pen_count}"
))

distinct_pen_count <- stp_enrolment |> distinct(ENCRYPTED_TRUE_PEN) |> nrow()
log_info(glue::glue("Distinct ENCRYPTED_TRUE_PEN values: {distinct_pen_count}"))

# Untoggle when running new data and/or add a conditional to test for the presence of the ID field.
# stp_enrolment <- stp_enrolment |> mutate(ID = row_number())

## --------------------------------------Reformat yy-mm-dd to yyyy-mm-dd---------------------------
## reference: source("./sql/01-enrolment-preprocessing/convert-date-scripts.R")
## all queries in the file
## -------------------------------------------------------------------------------------------------

convert_date <- function(vec) {
  # Years 26-99 go to 19xx
  # Years 00-25 go to 20xx
  yy <- as.numeric(substr(vec, 1, 2))

  century_prefix <- case_when(
    is.na(yy) ~ NA_character_,
    yy < 26 ~ "20",
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

# Uncomment when running new data and/or add a conditional to test the date format.
# stp_enrolment <- stp_enrolment |>
#   mutate(
#     across(
#       .cols = date_cols,
#       .fns = convert_date,
#       .names = "{.col}"
#     )
#   )

## --------------------------------------- Create Record Type Table -------------------------------
## reference: source("./sql/01-enrolment-preprocessing/01-enrolment-preprocessing.R")
##   qry01 to qry07 series
## ------------------------------------------------------------------------------------------------

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
ce_pattern <- "Continuing Education$|Continuing Studies$|Audit|^Ce "

stp_enrolment_record_type_base <- stp_enrolment |>
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

log_info(glue::glue(
  "Created stp_enrolment_record_type_base: {nrow(stp_enrolment_record_type_base)} rows"
))


stp_enrolment_record_type <- stp_enrolment_record_type_base |>
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

      # DEFAULT: Fallback for all other records
      TRUE ~ NA
    )
  )

log_info("First RecordStatus pass complete. Counts by status:")
log_info(paste(
  capture.output(print(stp_enrolment_record_type |> count(RecordStatus))),
  collapse = "\n"
))

# Notes: in the SQL queries from 2019 and earlier, some manual investigation was done to
# find more skills based courses that were inadvertently excluded (s.b. RecordStatus 0) in the above work.
# The manual investigation result were recorded column "Keep".  We did not do this manual work so I'm
# leaving the scaffolding in case it gets done later.
# The affected queries are: qry03g, 03g_b, 03g_c, 03g_c2, 03_d, 03h, 03i, 03i2, 03j

tmp_tbl_skills_based_courses <- stp_enrolment_record_type_base |>
  inner_join(
    stp_enrolment_record_type |> filter(RecordStatus == 6) |> select(ID),
    by = "ID"
  ) |>
  distinct(
    PSI_CODE,
    PSI_PROGRAM_CODE,
    PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    CIP2,
    PSI_CREDENTIAL_CATEGORY,
    PSI_STUDY_LEVEL,
    PSI_CONTINUING_EDUCATION_COURSE_ONLY
  ) |>
  mutate(KEEP = as.character(NA)) # Initializing KEEP as NULL/NA

log_info(glue::glue(
  "Created tmp_tbl_skills_based_courses: {nrow(tmp_tbl_skills_based_courses)} distinct skill-based course combinations"
))

# this needs to be here
stp_enrolment_record_type <- stp_enrolment_record_type |>
  mutate(
    RecordStatus = case_when(
      !is.na(RecordStatus) ~ RecordStatus,

      PSI_CODE == 'SEL' &
        PSI_CREDENTIAL_PROGRAM_DESCRIPTION ==
          'Community, Corporate & International Development' ~ 6,

      TRUE ~ NA
    )
  )

# this also needs to be here to align with queries.  Could there
# be a less complicated version of this workflow?
ids_to_flag_6 <- stp_enrolment_record_type_base |>
  inner_join(
    stp_enrolment_record_type |> filter(is.na(RecordStatus)) |> select(ID),
    by = "ID"
  ) |>
  semi_join(
    tmp_tbl_skills_based_courses |> filter(is.na(KEEP)),
    by = c(
      "PSI_CODE",
      "PSI_PROGRAM_CODE",
      "PSI_CREDENTIAL_PROGRAM_DESCRIPTION",
      "CIP2",
      "PSI_CREDENTIAL_CATEGORY",
      "PSI_STUDY_LEVEL"
    )
  ) |>
  pull(ID)

log_info(glue::glue(
  "IDs flagged as skills-based (RecordStatus 6) from semi-join: {length(ids_to_flag_6)}"
))


stp_enrolment_record_type <- stp_enrolment_record_type |>
  mutate(
    RecordStatus = case_when(
      !is.na(RecordStatus) ~ RecordStatus,

      ID %in% ids_to_flag_6 ~ 6,

      PSI_CONTINUING_EDUCATION_COURSE_ONLY == "Not Skills Crs Only" &
        CIP2 %in% cips ~ 7, # qry 03k and qry03l series

      PSI_ENTRY_STATUS == "No Transition" ~ 3,

      ATTENDING_PSI_OUTSIDE_BC == "Y" ~ 5,

      TRUE ~ 0
    )
  )

stp_enrolment_record_type <- stp_enrolment_record_type |>
  select(ID, RecordStatus)

log_info("Final RecordStatus assignment complete. Counts by status:")
log_info(paste(
  capture.output(print(stp_enrolment_record_type |> count(RecordStatus))),
  collapse = "\n"
))


## --------------------------------------- Create Valid Enrolment Table ---------------------------
## Creates a table of Record Status = 0 only (Valid Enrolment)
## ------------------------------------------------------------------------------------------------

stp_enrolment_valid <- stp_enrolment |>
  select(
    ID,
    ENCRYPTED_TRUE_PEN,
    PSI_STUDENT_NUMBER,
    PSI_CODE,
    PSI_MIN_START_DATE,
    PSI_SCHOOL_YEAR,
    PSI_ENROLMENT_SEQUENCE,
    PSI_STUDENT_POSTAL_CODE_CURRENT
  ) |>
  inner_join(stp_enrolment_record_type, by = join_by(ID)) |>
  filter(RecordStatus == 0) |>
  select(-RecordStatus)

log_info(glue::glue(
  "Created stp_enrolment_valid (RecordStatus == 0): {nrow(stp_enrolment_valid)} rows"
))


## ------------------------------------- Min Enrolment --------------------------------------------
## reference: source("./sql/01-enrolment-preprocessing/01-enrolment-preprocessing.R")
## qry09 to qry14
# Notes: n a handful of cases, the SQL version improperly orders records with PSI_ENROLMENT_SEQUENCE == 10 and 11.
# R's arrange() handles them properly,
## ------------------------------------------------------------------------------------------------

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

log_info(glue::glue(
  "Valid PEN records: {nrow(valid_pen_data)}, Invalid PEN records: {nrow(invalid_pen_data)}"
))

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

log_info("Min enrolment summary (RecordStatus, is_min_enrol, is_first_enrol):")
log_info(paste(
  capture.output(print(
    stp_enrolment_record_type |>
      count(RecordStatus, is_min_enrol, is_first_enrol)
  )),
  collapse = "\n"
))


## ------------------------------------- Clean Birthdates -----------------------------------------
## reference: source("./sql/01-enrolment-preprocessing/pssm-birthdate-cleaning.R")
## qry01 to qry11
## Notes: Look out for different NA or non-valid types as this can alter the behaviour of
## min, max, first.
## ------------------------------------------------------------------------------------------------

# qry01 to qry08
birthdate_cleaning_summary <- stp_enrolment |>
  select(ENCRYPTED_TRUE_PEN, PSI_BIRTHDATE, LAST_SEEN_BIRTHDATE) |>
  filter(
    !PSI_BIRTHDATE %in% c("", " ", "(Unspecified)", NA_Date_, NA_character_),
    !ENCRYPTED_TRUE_PEN %in%
      c("", " ", "(Unspecified)", NA_Date_, NA_character_)
  ) |>
  group_by(ENCRYPTED_TRUE_PEN, PSI_BIRTHDATE) |>
  summarize(
    NBirthdateRecords = n(),
    LastSeenBirthdate = first(LAST_SEEN_BIRTHDATE, na_rm = TRUE), # should only be one "last seen" per student; I've had issues with this na_rm so check it.
    .groups = "drop_last"
  ) |>
  summarize(
    DistinctBirthdates = n(), # Useful for auditing
    MinPSIBirthdate = min(PSI_BIRTHDATE, na.rm = TRUE),
    MaxPSIBirthdate = max(PSI_BIRTHDATE, na.rm = TRUE),
    NumMinBirthdateRecords = NBirthdateRecords[
      PSI_BIRTHDATE == min(PSI_BIRTHDATE, na.rm = TRUE)
    ][1],
    NumMaxBirthdateRecords = NBirthdateRecords[
      PSI_BIRTHDATE == max(PSI_BIRTHDATE, na.rm = TRUE)
    ][1],
    LastSeenBirthdate = first(LastSeenBirthdate, na_rm = TRUE)
  ) |>
  ungroup()

log_info(glue::glue(
  "Birthdate cleaning summary: {nrow(birthdate_cleaning_summary)} students with birthdate records"
))

#qry09 to qry11
birthdate_update <- birthdate_cleaning_summary |>
  mutate(
    psi_birthdate_cleaned = case_when(
      # If they only have one date, use it
      MinPSIBirthdate == MaxPSIBirthdate ~ MinPSIBirthdate,

      # If the max birthdate matches the "Last Seen" date, use that one
      MaxPSIBirthdate == LastSeenBirthdate ~ MaxPSIBirthdate,
      #MinPSIBirthdate == LastSeenBirthdate ~ MinPSIBirthdate, # old logic didn't include this

      # Otherwise use the date that appears most frequently
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
  mutate(psi_birthdate_cleaned = coalesce(psi_birthdate_cleaned, PSI_BIRTHDATE))
# Keeping as lower case to match the SQL versions, jfn.
log_info(glue::glue(
  "Birthdate cleaning complete. Students with cleaned birthdate: {sum(!is.na(stp_enrolment$psi_birthdate_cleaned))} / {nrow(stp_enrolment)}"
))

## ------------------------------------ Clean Up --------------------------------------------------
# Current workflow:
#  - Write key tables back to sql server.  These are tables needed for downstream work, or tables
# that might be needed for later reference outside of this analysis.
#  - Close DB connections
#  - Remove all objects at the end of each script.
## ------------------------------------------------------------------------------------------------

tables_to_keep <- c(
  "stp_enrolment",
  "stp_enrolment_record_type",
  "stp_enrolment_valid"
)

write_table_to_db <- function(table_name, schema, con) {
  db_name <- paste0(table_name, "_r")
  dbWriteTable(
    con,
    SQL(glue::glue('"{schema}"."{db_name}"')),
    base::get(table_name, envir = .GlobalEnv),
    overwrite = TRUE
  )
  log_info(glue::glue(
    "Wrote table '{schema}.{db_name}' ({nrow(base::get(table_name, envir = .GlobalEnv))} rows) to SQL Server"
  ))
}

log_info(glue::glue(
  "Writing {length(tables_to_keep)} tables to DB: {paste(tables_to_keep, collapse = ', ')}"
))
walk(tables_to_keep, write_table_to_db, schema = my_schema, con = con)

dbDisconnect(con)
log_info("Disconnected from SQL Server")

log_info("==== 01a-enrolment-preprocessing.R COMPLETE ====")

# rm(list = ls())
