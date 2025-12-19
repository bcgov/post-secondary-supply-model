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

# Workflow #2
# Credential Preprocessing
# Description:
# Relies on STP_Credential, STP_Enrolment_Record_Type, STP_Enrolment_Valid, STP_Enrolment data tables
# Creates tables _____ which are used in subsequent workflows

library(tidyverse)
library(odbc)
library(DBI)

# ---- Configure LAN Paths and DB Connection -----
source("./sql/01-credential-preprocessing/01a-credential-preprocessing.R")

# set connection string to decimal
db_config <- config::get("decimal")
my_schema <- config::get("myschema")

con <- dbConnect(
  odbc(),
  Driver = db_config$driver,
  Server = db_config$server,
  Database = db_config$database,
  Trusted_Connection = "True"
)

# ---- Check Required Tables etc. ----
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Credential"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Enrolment_Record_Type"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Enrolment"')))

credential <- dbGetQuery(
  con,
  glue::glue("SELECT * FROM [{my_schema}].[STP_Credential];")
)

credential.orig <- credential # save a copy while testing

credential |>
  filter(
    ENCRYPTED_TRUE_PEN %in%
      c('', ' ', '(Unspecified)') |
      is.na(ENCRYPTED_TRUE_PEN)
  ) |>
  nrow()

credential |> distinct(ENCRYPTED_TRUE_PEN) |> count()

# Add primary key: this may not be necessary but leaving for now
credential <- credential |>
  mutate(ID = row_number()) |>
  relocate(ID)

# ---- Reformat yy-mm-dd to yyyy-mm-dd ----
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
  "CREDENTIAL_AWARD_DATE",
  "PSI_PROGRAM_EFFECTIVE_DATE"
)

credential <- credential |>
  mutate(
    across(
      .cols = date_cols,
      .fns = convert_date,
      .names = "{.col}"
    )
  )

# ---- Process by Record Type ----
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

# ---- Create lookup table for ID/Record Status and populate with ID column and EPEN ----
invalid_vals <- c('', ' ', '(Unspecified)')
dev_cips <- c('21', '32', '33', '34', '35', '36', '37', '53', '89') # this may be the same list as defined in enrolement processing

enrol_skills_lookup <- stp_enrolment_record_type |>
  filter(RecordStatus == 6) |>
  mutate(CIP2 = substr(PSI_CIP_CODE, 1, 2)) |>
  distinct(
    PSI_CODE,
    PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    CIP2,
    PSI_CREDENTIAL_CATEGORY,
    PSI_STUDY_LEVEL
  ) |>
  mutate(is_skills_match = TRUE)

# ---- Find records with Record_Status = 1  ----
credential <- credential |>
  mutate(CIP2 = substr(PSI_CREDENTIAL_CIP, 1, 2)) |>
  left_join(
    enrol_skills_lookup,
    by = c(
      "PSI_CODE" = "PSI_CODE",
      "PSI_CREDENTIAL_PROGRAM_DESCRIPTION" = "PSI_CREDENTIAL_PROGRAM_DESCRIPTION",
      "CIP2" = "CIP2",
      "PSI_CREDENTIAL_CATEGORY" = "PSI_CREDENTIAL_CATEGORY",
      "PSI_CREDENTIAL_LEVEL" = "PSI_STUDY_LEVEL"
    )
  ) |>
  mutate(
    RecordStatus = case_when(
      # --- Record Type 1 ---
      (ENCRYPTED_TRUE_PEN %in% invalid_vals) &
        (PSI_STUDENT_NUMBER %in% invalid_vals | PSI_CODE %in% invalid_vals) ~ 1,
      # --- Record Type 2 ---
      PSI_CREDENTIAL_LEVEL == 'Developmental' ~ 2,

      # --- Record Type 6---
      is_skills_match == TRUE ~ 6,

      # --- Status 7: Developmental CIPs (With "Keep" Exceptions) ---
      # there may be more exceptions to add - in previous years some manual checks were done.
      (CIP2 %in% dev_cips) &
        !((PSI_CODE == 'UVIC' &
          PSI_CREDENTIAL_PROGRAM_DESCRIPTION ==
            'PROF SPEC CERTIFICATE IN MIDDLE YEARS LANG AND LITERACY') |
          (PSI_CODE == 'NIC' &
            PSI_CREDENTIAL_PROGRAM_DESCRIPTION == 'Aquaculture Technician 1') |
          (PSI_CODE == 'NIC' &
            PSI_CREDENTIAL_PROGRAM_DESCRIPTION == 'Coastal Forest Resource') |
          (PSI_CODE == 'NIC' &
            PSI_CREDENTIAL_PROGRAM_DESCRIPTION ==
              'Underground Mining Essentials')) ~ 7,
      # --- Status 8: Recommendation for Certification
      PSI_CREDENTIAL_CATEGORY == 'Recommendation For Certification' ~ 8,

      # Default: leave other records as NA (or 0) for now
      TRUE ~ 0
    )
  ) |>
  select(-is_skills_match)

credential |> count(RecordStatus)

tables_to_keep = c(
  'stp_enrolment',
  'stp_credential',
  'stp_enrolment_record_type',
  'stp_credential_record_type',
  'stp_enrolment_valid'
)

rm(list = setdiff(ls(), tables_to_keep))

dbDisconnect(con)
