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
names(credential)
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
invalid_vals <- c('', ' ', '(Unspecified)', NA)

# ---- Find records with Record_Status = 1  ----
credential <- credential |>
  mutate(
    RecordStatus = case_when(
      # --- Record Type 1 ---
      (ENCRYPTED_TRUE_PEN %in% invalid_vals) &
        (PSI_STUDENT_NUMBER %in% invalid_vals | PSI_CODE %in% invalid_vals) ~ 1,
      # --- Record Type 2 ---
      PSI_CREDENTIAL_LEVEL == 'Developmental' ~ 2,

      # Default: leave other records as NA (or 0) for now
      TRUE ~ NA_real_
    )
  )

credential |> count(RecordStatus)
credential_rec_status_sql <- glue::glue(
  "SELECT RecordStatus, COUNT(*) FROM [{my_schema}].[STP_Credential_Record_Type] GROUP BY RecordStatus"
)
dbGetQuery(con, credential_rec_status_sql)


# ---- Find records with Record_Status = 6  ----
dbExecute(con, qry03c_create_table_EnrolmentSkillsBasedCourse)
dbExecute(con, qry03d_create_table_Suspect_Skills_Based)
dbExecute(con, qry03e_Find_Suspect_Skills_Based)
dbExecute(con, qry03f_Update_Suspect_Skills_Based)
dbExecute(
  con,
  glue::glue("DROP TABLE [{my_schema}].[tmp_tbl_Cred_Suspect_Skills_Based];")
)
dbExecute(
  con,
  glue::glue("DROP TABLE [{my_schema}].[Cred_Suspect_Skills_Based];")
)
dbExecute(
  con,
  glue::glue("DROP TABLE [{my_schema}].[tmp_tbl_EnrolmentSkillsBasedCourses];")
)

# ---- Find records with Record_Status = 7 and update look up table ----
dbExecute(con, qry03g_Drop_Developmental_Credential_CIPS)
dbExecute(
  con,
  "ALTER TABLE Drop_Developmental_PSI_CREDENTIAL_CIPS ADD Keep NVARCHAR(2)"
)

###  ---- ** Manual **  ----
# Check against the outcomes programs table to see if some are non-developmental CIP. If so, set keep = 'Y'.
data <- dbReadTable(
  con,
  "Drop_Developmental_PSI_CREDENTIAL_CIPS",
  col_types = cols(.default = col_character())
)
data.entry(data)
dbWriteTable(
  con,
  name = "Drop_Developmental_PSI_CREDENTIAL_CIPS",
  as.data.frame(data),
  overwrite = TRUE
)

dbExecute(con, qry03h_Update_Developmental_CIPs)
dbExecute(
  con,
  glue::glue(
    "DROP TABLE [{my_schema}].[Drop_Developmental_PSI_CREDENTIAL_CIPS];"
  )
)

# ---- Find records with Record_Status = 8 and update look up table ----
dbExecute(con, qry03i_Drop_RecommendationForCert)
dbExecute(con, qry03j_Update_RecommendationForCert)
dbExecute(
  con,
  glue::glue("DROP TABLE [{my_schema}].[Drop_Cred_RecommendForCert];")
)

dbExecute(con, qry04_Update_RecordStatus_Not_Dropped)
dbGetQuery(con, RecordTypeSummary)

# ---- Clean Up and check tables to keep ----
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Credential"')))
dbExistsTable(
  con,
  SQL(glue::glue('"{my_schema}"."STP_Credential_Record_Type"'))
)
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Enrolment_Record_Type"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Enrolment"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Enrolment_Valid"')))

dbDisconnect(con)
