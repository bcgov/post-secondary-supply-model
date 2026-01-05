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

# ---- Configure LAN Paths and DB Connection -----
source("./sql/01-credential-analysis/01b-credential-analysis.R")
source("./sql/01-credential-analysis/credential-sup-vars-from-enrolment.R")
source(
  "./sql/01-credential-analysis/credential-sup-vars-additional-gender-cleaning.R"
)
source("./sql/01-credential-analysis/credential-non-dup-psi_visa_status.R")
#source("./sql/01-credential-analysis/credential-ranking.R")

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

# ---- Get Required Tables (for testing only) ----
# remove after development and replace with a check that the table exists
# in the current R environment
stp_enrolment <- dbReadTable(
  con,
  SQL(glue::glue('"{my_schema}"."STP_Enrolment"'))
)
stp_credential <- dbReadTable(
  con,
  SQL(glue::glue('"{my_schema}"."STP_Credential"'))
)

stp_enrolment_record_type <- dbReadTable(
  con,
  SQL(glue::glue('"{my_schema}"."STP_Enrolment_Record_Type"'))
)
stp_credential_record_type <- dbReadTable(
  con,
  SQL(glue::glue('"{my_schema}"."STP_Credential_Record_Type"')) # EPEN shouldn't be in this table - why is it here??
)

stp_enrolment_valid <- dbReadTable(
  con,
  SQL(glue::glue('"{my_schema}"."STP_Enrolment_Valid"'))
)

stp_credential.orig <- stp_credential # save a copy while testing
stp_enrolment_valid.orig <- stp_enrolment_valid # save a copy while testing
stp_enrolment.orig <- stp_enrolment # save a copy while testing
stp_enrolment_record_type.orig <- stp_enrolment_record_type # save a copy while testing
stp_credential_record_type.orig <- stp_credential_record_type # save a copy while testing

# Define lookup tables
outcome_credential <- data.frame(
  Credential = c(
    "ADVANCED DIPLOMA",
    "APPRENTICESHIP",
    "ASSOCIATE DEGREE",
    "BACHELORS DEGREE",
    "CERTIFICATE",
    "DIPLOMA",
    "DOCTORATE",
    "FIRST PROFESSIONAL DEGREE",
    "GRADUATE CERTIFICATE",
    "GRADUATE DIPLOMA",
    "MASTERS DEGREE",
    "POST-DEGREE CERTIFICATE",
    "POST-DEGREE DIPLOMA",
    "ADVANCED CERTIFICATE",
    "ADVANCED DIPLOMA",
    "APPRENTICESHIP",
    "ASSOCIATE DEGREE",
    "BACHELORS DEGREE",
    "CERTIFICATE",
    "DIPLOMA",
    "DOCTORATE",
    "FIRST PROFESSIONAL DEGREE",
    "GRADUATE CERTIFICATE",
    "GRADUATE DIPLOMA",
    "MASTERS DEGREE",
    "POST-DEGREE CERTIFICATE",
    "POST-DEGREE DIPLOMA",
    "ADVANCED CERTIFICATE"
  ),
  Category = c(
    "DACSO",
    "APPSO",
    "DACSO",
    "BGS",
    "DACSO",
    "DACSO",
    "GRAD",
    "BGS",
    "GRAD",
    "GRAD",
    "GRAD",
    "DACSO",
    "DACSO",
    "DACSO",
    "DACSO",
    "APPSO",
    "DACSO",
    "BGS",
    "DACSO",
    "DACSO",
    "GRAD",
    "BGS",
    "GRAD",
    "GRAD",
    "GRAD",
    "DACSO",
    "DACSO",
    "DACSO"
  ),
  stringsAsFactors = FALSE
)

credential_rank <- data.frame(
  Credential = c(
    "ADVANCED CERTIFICATE",
    "ADVANCED DIPLOMA",
    "APPRENTICESHIP",
    "ASSOCIATE DEGREE",
    "BACHELORS DEGREE",
    "CERTIFICATE",
    "DIPLOMA",
    "DOCTORATE",
    "FIRST PROFESSIONAL DEGREE",
    "GRADUATE CERTIFICATE",
    "GRADUATE DIPLOMA",
    "MASTERS DEGREE",
    "POST-DEGREE CERTIFICATE",
    "POST-DEGREE DIPLOMA",
    "ADVANCED CERTIFICATE",
    "ADVANCED DIPLOMA",
    "APPRENTICESHIP",
    "ASSOCIATE DEGREE",
    "BACHELORS DEGREE",
    "CERTIFICATE",
    "DIPLOMA",
    "DOCTORATE",
    "FIRST PROFESSIONAL DEGREE",
    "GRADUATE CERTIFICATE",
    "GRADUATE DIPLOMA",
    "MASTERS DEGREE",
    "POST-DEGREE CERTIFICATE",
    "POST-DEGREE DIPLOMA"
  ),
  Rank = c(
    10,
    9,
    14,
    11,
    8,
    13,
    12,
    1,
    7,
    4,
    3,
    2,
    6,
    5,
    10,
    9,
    14,
    11,
    8,
    13,
    12,
    1,
    7,
    4,
    3,
    2,
    6,
    5
  ),
  stringsAsFactors = FALSE
)

age_group_lookup <- data.frame(
  AgeIndex = 1:9,
  AgeGroup = c(
    "15 to 16",
    "17 to 19",
    "20 to 24",
    "25 to 29",
    "30 to 34",
    "35 to 44",
    "45 to 54",
    "55 to 64",
    "65 to 89"
  ),
  LowerBound = c(15, 17, 20, 25, 30, 35, 45, 55, 65),
  UpperBound = c(16, 19, 24, 29, 34, 44, 54, 64, 89),
  stringsAsFactors = FALSE
)

# ---- Create a view with STP_Credential data with record_type == 0 and a non-blank award date ----
# qry00
credential <- stp_credential |>
  inner_join(
    stp_credential_record_type |> select(ID, RecordStatus),
    by = "ID"
  ) |>
  filter(
    RecordStatus == 0,
    !is.na(CREDENTIAL_AWARD_DATE),
    !CREDENTIAL_AWARD_DATE %in% c("", " ", "(Unspecified)")
  ) |>
  select(
    ID,
    ENCRYPTED_TRUE_PEN,
    PSI_SCHOOL_YEAR,
    PSI_STUDENT_NUMBER,
    PSI_CODE,
    CREDENTIAL_AWARD_DATE,
    PSI_PROGRAM_CODE,
    PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    PSI_CREDENTIAL_CIP,
    PSI_CREDENTIAL_LEVEL,
    PSI_CREDENTIAL_CATEGORY,
    RecordStatus
  )

# -------------------------------------------------------------------------------------------------
# ----------------------------- Make Credential Sup Vars Enrolment Table --------------------------------
## reference: source("./sql/01-credential-analysis/credential-sup-vars-additional-gender-cleaning.R")
# qry01 to qry05
latest_enrolment_epen <- stp_enrolment_valid |>
  filter(
    !is.na(ENCRYPTED_TRUE_PEN),
    !ENCRYPTED_TRUE_PEN %in% c("", " ", "(Unspecified)")
  ) |>
  group_by(ENCRYPTED_TRUE_PEN) |>
  filter(PSI_SCHOOL_YEAR == max(PSI_SCHOOL_YEAR, na.rm = TRUE)) |>
  ungroup() |>
  distinct(
    ID,
    ENCRYPTED_TRUE_PEN,
    PSI_STUDENT_NUMBER,
    PSI_CODE,
    PSI_SCHOOL_YEAR,
    PSI_STUDENT_POSTAL_CODE_CURRENT,
    PSI_ENROLMENT_SEQUENCE,
    PSI_MIN_START_DATE
  )

credential_supvars_enrolment_epen <- latest_enrolment_epen |>
  inner_join(
    credential,
    by = c("ENCRYPTED_TRUE_PEN"),
    relationship = "many-to-many"
  ) |>
  select(
    EnrolmentID = ID.x,
    ENCRYPTED_TRUE_PEN,
    PSI_MIN_START_DATE,
    CredentialRecordStatus = RecordStatus,
    PSI_STUDENT_POSTAL_CODE_CURRENT,
    PSI_SCHOOL_YEAR = PSI_SCHOOL_YEAR.x,
    PSI_CODE = PSI_CODE.x,
    PSI_STUDENT_NUMBER = PSI_STUDENT_NUMBER.x,
    PSI_ENROLMENT_SEQUENCE
  ) |>
  distinct()


# Match via PSI_CODE/Student Number to recover records missed by PEN join.
# Misses may also occur due to temporal mismatches or students lacking "Valid" enrolment status.
#qry06 to qry12
latest_enrolment_no_epen <- stp_enrolment_valid |>
  filter(
    is.na(ENCRYPTED_TRUE_PEN) |
      ENCRYPTED_TRUE_PEN %in% c("", " ", "(Unspecified)")
  ) |>
  group_by(PSI_CODE, PSI_STUDENT_NUMBER) |>
  filter(PSI_SCHOOL_YEAR == max(PSI_SCHOOL_YEAR, na.rm = TRUE)) |>
  ungroup() |>
  distinct(
    ID,
    ENCRYPTED_TRUE_PEN,
    PSI_STUDENT_NUMBER,
    PSI_CODE,
    PSI_SCHOOL_YEAR,
    PSI_STUDENT_POSTAL_CODE_CURRENT,
    PSI_ENROLMENT_SEQUENCE,
    PSI_MIN_START_DATE
  )

credential_supvars_enrolment_no_epen <- latest_enrolment_no_epen |>
  inner_join(
    credential,
    by = c("PSI_CODE", "PSI_STUDENT_NUMBER"),
    relationship = "many-to-many"
  ) |>
  select(
    EnrolmentID = ID.x,
    ENCRYPTED_TRUE_PEN = ENCRYPTED_TRUE_PEN.x,
    PSI_MIN_START_DATE,
    CredentialRecordStatus = RecordStatus,
    PSI_STUDENT_POSTAL_CODE_CURRENT,
    PSI_SCHOOL_YEAR = PSI_SCHOOL_YEAR.x,
    PSI_CODE = PSI_CODE,
    PSI_STUDENT_NUMBER = PSI_STUDENT_NUMBER,
    PSI_ENROLMENT_SEQUENCE
  ) |>
  distinct()

credential_supvars_enrolment <- rbind(
  credential_supvars_enrolment_epen,
  credential_supvars_enrolment_no_epen
) |>
  distinct()

# -------------------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------------------
# ---- Make Credential Sup Vars Table ----
# this gets added to later
credential_supvars <- credential |>
  select(
    ID,
    ENCRYPTED_TRUE_PEN,
    PSI_STUDENT_NUMBER,
    PSI_CODE,
    PSI_SCHOOL_YEAR,
    CREDENTIAL_AWARD_DATE,
    CredentialRecordStatus = RecordStatus, # Renaming 'RecordStatus' as in SQL
    PSI_PROGRAM_CODE,
    PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    PSI_CREDENTIAL_CIP,
    PSI_CREDENTIAL_LEVEL,
    PSI_CREDENTIAL_CATEGORY
  ) |>
  mutate(CREDENTIAL_AWARD_DATE_D = as.Date(CREDENTIAL_AWARD_DATE)) # create date format of award date

credential_supvars_enrolment <- credential_supvars_enrolment |>
  left_join(
    stp_enrolment |>
      select(
        ID,
        psi_birthdate_cleaned,
        PSI_VISA_STATUS,
        PSI_BIRTHDATE,
        PSI_PROGRAM_CODE,
        PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
        PSI_CIP_CODE,
        PSI_CONTINUING_EDUCATION_COURSE_ONLY,
        PSI_GENDER
      ),
    by = c("EnrolmentID" = "ID")
  ) |>
  mutate(
    psi_birthdate_cleaned = if_else(
      psi_birthdate_cleaned == "1900-01-01",
      NA,
      psi_birthdate_cleaned
    )
  )


# ---- 02 Developmental Records ----
# add a drop credential flag, presumably for later use
stp_credential_record_type <-
  stp_credential_record_type |>
  left_join(
    credential |>
      filter(
        PSI_CREDENTIAL_CATEGORY %in%
          c(
            'Developmental Credential',
            'Other',
            'None',
            'Short Certificate'
          )
      ) |>
      mutate(DropCredCategory = "Yes") |>
      select(ID, DropCredCategory, PSI_CREDENTIAL_CATEGORY),
    by = "ID"
  ) |>
  mutate(
    DropCredCategory = replace_na(DropCredCategory, "No")
  )

# ---- 03 Miscellaneous ----
stp_credential_record_type <-
  credential_supvars |>
  filter(CREDENTIAL_AWARD_DATE >= "2023-09-01") |>
  select(ID) |>
  mutate(DropPartialYear = "Yes") |>
  right_join(stp_credential_record_type, by = "ID") |>
  mutate(
    DropPartialYear = if_else(is.na(DropPartialYear), "No", DropPartialYear)
  )


# ---- 03 Gender Cleaning ----
# Performs a targeted data recovery for missing gender information.
# Essentially, identify students with missing gender values and
# attempt to "backfill" them using their most recent valid record from a lookup table.
# There are two passes:
#     1. looks for most recent gender record from credential_supvars_enrolment
#     2. looks for most recent gender record from stp_enrolment
# within each pass, the data is placed into buckets based on whether ENCRYPTED_TRUE_PEN is available or not
# note: the "not" bucket hasn't been implemented yet for pass 1, in order to keep in alignment with the SQL environment
# also, this strategy assumes that the most recent record is the most accurate, which may not always be the case.
# we could use a more sophisticated approach if needed, such as considering multiple records or averaging values.

na_vals <- c('U', 'Unknown', '(Unspecified)', '', ' ', NA_character_)
credential_supvars <- credential_supvars |>
  mutate(PSI_GENDER_CLEANED = NA_character_)

#useful for development
#credential_supvars_enrolment.bk <- credential_supvars_enrolment
#credential_supvars.bk <- credential_supvars
#credential_supvars <- credential_supvars.bk

# First pass: find genders from credential_supvars_enrolment
missing_gender <- credential_supvars |>
  filter(PSI_GENDER_CLEANED %in% na_vals) |> # Select initial subset of columns
  distinct(
    ENCRYPTED_TRUE_PEN,
    PSI_STUDENT_NUMBER,
    PSI_CODE,
    PSI_GENDER_CLEANED
  )

src_gender_lookup <- credential_supvars_enrolment |>
  filter(!PSI_GENDER %in% na_vals) |>
  distinct(
    ENCRYPTED_TRUE_PEN,
    PSI_STUDENT_NUMBER,
    PSI_CODE,
    PSI_GENDER,
    PSI_ENROLMENT_SEQUENCE,
    PSI_SCHOOL_YEAR
  )

# implementing only the EPEN bucket for now
epen_missing_gender <- missing_gender |>
  filter(!ENCRYPTED_TRUE_PEN %in% na_vals) |>
  inner_join(
    src_gender_lookup |>
      filter(!ENCRYPTED_TRUE_PEN %in% na_vals),
    by = c("ENCRYPTED_TRUE_PEN")
  ) |>
  group_by(ENCRYPTED_TRUE_PEN) |>
  arrange(desc(PSI_SCHOOL_YEAR), desc(PSI_ENROLMENT_SEQUENCE)) |>
  slice(1) |>
  ungroup() |>
  select(ENCRYPTED_TRUE_PEN, PSI_GENDER_CLEANED = PSI_GENDER) |>
  distinct()

# add recovered genders to credential_supvars
credential_supvars <- credential_supvars |>
  left_join(
    epen_missing_gender |>
      select(
        ENCRYPTED_TRUE_PEN,
        PSI_GENDER_CLEANED
      ),
    suffix = c("", "_y"),
    # Safety distinct to ensure no row duplication
    distinct(ENCRYPTED_TRUE_PEN, .keep_all = TRUE),
    by = "ENCRYPTED_TRUE_PEN"
  ) |>
  select(-PSI_GENDER_CLEANED_y)

# Second pass: find genders from stp_enrolment for those still missing
still_missing_gender <- credential_supvars |>
  filter(PSI_GENDER_CLEANED %in% na_vals) |> # Select initial subset of columns
  distinct(
    ENCRYPTED_TRUE_PEN,
    PSI_STUDENT_NUMBER,
    PSI_CODE,
    PSI_GENDER_CLEANED
  )

src_gender_lookup <- stp_enrolment |>
  filter(!PSI_GENDER %in% na_vals) |>
  distinct(
    ENCRYPTED_TRUE_PEN,
    PSI_STUDENT_NUMBER,
    PSI_CODE,
    PSI_GENDER,
    PSI_ENROLMENT_SEQUENCE,
    PSI_SCHOOL_YEAR
  )

# implementing both buckets this time
# 1. EPEN bucket
epen_still_missing_gender <- still_missing_gender |>
  filter(!ENCRYPTED_TRUE_PEN %in% na_vals) |>
  inner_join(
    src_gender_lookup |>
      filter(!ENCRYPTED_TRUE_PEN %in% na_vals),
    by = c("ENCRYPTED_TRUE_PEN")
  ) |>
  group_by(ENCRYPTED_TRUE_PEN) |>
  arrange(desc(PSI_SCHOOL_YEAR), desc(PSI_ENROLMENT_SEQUENCE)) |>
  slice(1) |>
  ungroup() |>
  select(ENCRYPTED_TRUE_PEN, GENDER_FROM_STP_ENROLMENT = PSI_GENDER) |>
  distinct()

# 2. No EPEN bucket
no_epen_still_missing_gender <- still_missing_gender |>
  filter(ENCRYPTED_TRUE_PEN %in% na_vals) |>
  inner_join(
    src_gender_lookup |>
      filter(ENCRYPTED_TRUE_PEN %in% na_vals),
    by = c("PSI_STUDENT_NUMBER", "PSI_CODE")
  ) |>
  group_by(PSI_STUDENT_NUMBER, PSI_CODE) |>
  arrange(desc(PSI_SCHOOL_YEAR), desc(PSI_ENROLMENT_SEQUENCE)) |>
  slice(1) |>
  ungroup() |>
  select(
    PSI_STUDENT_NUMBER,
    PSI_CODE,
    GENDER_FROM_STP_ENROLMENT = PSI_GENDER
  ) |>
  distinct()

# add recovered genders back to credential_supvars
credential_supvars <- credential_supvars |>
  left_join(
    epen_still_missing_gender,
    by = "ENCRYPTED_TRUE_PEN"
  ) |>
  left_join(
    no_epen_still_missing_gender,
    by = c("PSI_STUDENT_NUMBER", "PSI_CODE")
  ) |>
  mutate(
    PSI_GENDER_CLEANED = coalesce(
      PSI_GENDER_CLEANED,
      GENDER_FROM_STP_ENROLMENT.x,
      GENDER_FROM_STP_ENROLMENT.y
    )
  ) |>
  select(-GENDER_FROM_STP_ENROLMENT.x, -GENDER_FROM_STP_ENROLMENT.y)

credential_supvars.ref <- credential_supvars

# ---- 04 Birthdate cleaning (last seen birthdate) ----
# note: check if LAST_SEEN_BIRTHDATE can be included when supvars tables are created (at the top of script)
na_vals <- c("", " ", NA_character_, NA, '(Unspecified)')

credential_supvars_enrolment <- credential_supvars_enrolment |>
  left_join(
    stp_enrolment |> select(ID, LAST_SEEN_BIRTHDATE),
    by = c("EnrolmentID" = "ID")
  )

# check if LAST_SEEN_BIRTHDATE can be included when table is created
credential_supvars <- credential_supvars |>
  left_join(
    credential_supvars_enrolment |>
      distinct(ENCRYPTED_TRUE_PEN, LAST_SEEN_BIRTHDATE),
    by = "ENCRYPTED_TRUE_PEN"
  )

credential_supvars_birthdate_clean <- credential_supvars_enrolment |>
  select(
    ENCRYPTED_TRUE_PEN,
    psi_birthdate_cleaned,
    PSI_STUDENT_NUMBER,
    PSI_CODE
  ) |>
  distinct() |>
  mutate(
    # we should handle NA transformation when loading into R.
    psi_birthdate_cleaned = if_else(
      psi_birthdate_cleaned %in% na_vals,
      NA_character_,
      psi_birthdate_cleaned
    )
  ) |>
  mutate(
    psi_birthdate_cleaned_D = as.Date(psi_birthdate_cleaned) # we should be able to just cast this in the beginnning
  )

credential_supvars <- credential_supvars |>
  left_join(
    credential_supvars_birthdate_clean |>
      filter(!ENCRYPTED_TRUE_PEN %in% na_vals) |>
      distinct(
        ENCRYPTED_TRUE_PEN,
        bd_pen = psi_birthdate_cleaned,
        bd_pen_d = psi_birthdate_cleaned_D
      ),
    by = c("ENCRYPTED_TRUE_PEN")
  ) |>
  left_join(
    credential_supvars_birthdate_clean |>
      distinct(
        PSI_STUDENT_NUMBER,
        PSI_CODE,
        bd_stu = psi_birthdate_cleaned,
        bd_stu_d = psi_birthdate_cleaned_D
      ),
    by = c("PSI_STUDENT_NUMBER", "PSI_CODE")
  )

credential_supvars <- credential_supvars |>
  mutate(
    psi_birthdate_cleaned = coalesce(bd_pen, bd_stu),
    psi_birthdate_cleaned_D = coalesce(bd_pen_d, bd_stu_d),
  ) |>
  select(-bd_pen, -bd_pen_d, -bd_stu, -bd_stu_d) # a small handful of dates found in R version that were NA in SQL version


credential_supvars <- credential_supvars |>
  mutate(
    psi_birthdate_cleaned_D = coalesce(
      psi_birthdate_cleaned_D,
      as.Date(LAST_SEEN_BIRTHDATE)
    )
  )

credential <- stp_credential |>
  select(
    ID,
    ENCRYPTED_TRUE_PEN,
    PSI_STUDENT_NUMBER,
    PSI_CODE,
    PSI_FULL_NAME,
    PSI_SCHOOL_YEAR,
    PSI_PROGRAM_CODE,
    PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    PSI_CREDENTIAL_CATEGORY,
    PSI_CREDENTIAL_LEVEL,
    PSI_CREDENTIAL_CIP,
    CREDENTIAL_AWARD_DATE
  ) |>
  inner_join(
    credential_supvars |>
      select(
        ID,
        CREDENTIAL_AWARD_DATE_D,
        psi_birthdate_cleaned,
        psi_birthdate_cleaned_D,
        PSI_GENDER_CLEANED
      ),
    by = "ID"
  ) |>
  inner_join(
    stp_credential_record_type |>
      select(ID, RecordStatus, DropCredCategory, DropPartialYear),
    by = "ID"
  ) |>
  filter(
    RecordStatus == 0,
    is.na(DropCredCategory),
    is.na(DropPartialYear)
  )


# ---- 05 Age and Credential Update and Cleaning ----
credential <- credential |>
  mutate(
    AGE_AT_GRAD = as.integer(floor(
      interval(psi_birthdate_cleaned_D, CREDENTIAL_AWARD_DATE_D) / years(1)
    ))
  ) |>
  left_join(
    age_group_lookup,
    by = join_by(between(AGE_AT_GRAD, LowerBound, UpperBound))
  ) |>
  mutate(AGE_GROUP_AT_GRAD = AgeIndex) |>
  select(-any_of(names(age_group_lookup)))

# calculate credential school year based on award date
credential <- credential |>
  mutate(
    cred_month = lubridate::month(CREDENTIAL_AWARD_DATE_D),
    cred_year = lubridate::year(CREDENTIAL_AWARD_DATE_D)
  ) |>
  mutate(
    cred_year_start = if_else(cred_month < 9, cred_year - 1, cred_year),
    cred_year_end = if_else(cred_month < 9, cred_year, cred_year + 1)
  ) |>
  mutate(
    PSI_CREDENTIAL_SCHOOL_YEAR = paste0(
      cred_year_start,
      "/",
      substr(as.character(cred_year_end), 3, 4)
    )
  ) |>
  select(-cred_month, -cred_year, -cred_year_start, -cred_year_end)

valid_genders <- c('Female', 'Male', 'Gender Diverse')

# pull more genders from the stp_enrolment table to fill in gaps
credential <- credential |>
  left_join(
    stp_enrolment |>
      filter(PSI_GENDER %in% valid_genders) |>
      distinct(
        ENCRYPTED_TRUE_PEN,
        PSI_STUDENT_NUMBER,
        PSI_CODE,
        PSI_GENDER_FROM_ENROLMENT = PSI_GENDER
      ),
    by = c("ENCRYPTED_TRUE_PEN", "PSI_STUDENT_NUMBER", "PSI_CODE"),
    relationship = "many-to-many"
  ) |>
  mutate(
    PSI_GENDER_CLEANED = coalesce(PSI_GENDER_CLEANED, PSI_GENDER_FROM_ENROLMENT)
  ) |>
  select(-PSI_GENDER_FROM_ENROLMENT)


dbReadTable(
  con,
  SQL(glue::glue('"{my_schema}"."Credential"'))
) -> cr_sql
names(cr_sql) <- toupper(names(cr_sql))
names(cr_r) <- toupper(names(cr_r))
glimpse(cr_r)
glimpse(cr_sql)
cr_sql$CREDENTIAL_AWARD_DATE_D <- as.Date(cr_sql$CREDENTIAL_AWARD_DATE_D)
cr_sql$PSI_BIRTHDATE_CLEANED_D <- as.Date(cr_sql$PSI_BIRTHDATE_CLEANED_D)
anti_join(cr_r, cr_sql) |> nrow()

dbExecute(con, qry07a1c_tmp_Credential_Gender)
dbExecute(con, qry07a1d_tmp_Credential_GenderDups)
dbExecute(con, qry07a1e_tmp_Credential_GenderDups_FindMaxCredDate)

dbExecute(
  con,
  "ALTER TABLE tmp_Dup_Credential_EPEN_Gender_MaxCredDate ADD PSI_GENDER varchar(10)"
)
dbExecute(con, qry07a1f_tmp_Credential_GenderDups_PickGender)
dbExecute(con, qry07a1g_Update_Credential_Non_Dup_GenderDups)
dbExecute(con, qry07a1h_Update_Credential_GenderDups)
dbExecute(con, qry07a2a_ExtractNoGender)
dbExecute(con, qry07a2b_ExtractNoGenderUnique)
dbExecute(con, qry07a2c_Create_CRED_Extract_No_Gender_EPEN_with_MultiCred)
dbExecute(
  con,
  "ALTER TABLE CRED_Extract_No_Gender_Unique ADD MultiCredFlag varchar(2)"
)
dbExecute(con, qry07a2d_Update_MultiCredFlag)
dbExecute(con, "DROP TABLE CRED_Extract_No_Gender_EPEN_with_MultiCred")

## ---- Impute Missing Gender ----
d <- dbGetQuery(con, qry07b_GenderDistribution) %>%
  mutate(Expr1 = replace_na(Expr1, 0))

nulls <- d %>%
  filter(is.na(PSI_GENDER)) %>%
  select(-PSI_GENDER)

f_d <- d %>%
  filter(!is.na(PSI_GENDER)) %>%
  group_by(PSI_CREDENTIAL_CATEGORY) %>%
  mutate(p = Expr1 / sum(Expr1)) %>%
  filter(PSI_GENDER == 'Female') %>%
  select(-c(PSI_GENDER, Expr1))

m_d <- d %>%
  filter(!is.na(PSI_GENDER)) %>%
  group_by(PSI_CREDENTIAL_CATEGORY) %>%
  mutate(p = Expr1 / sum(Expr1)) %>%
  filter(PSI_GENDER == 'Male') %>%
  select(-c(PSI_GENDER, Expr1))

top_nf <- inner_join(f_d, nulls) %>%
  mutate(n = round(Expr1 * p)) %>%
  select(PSI_CREDENTIAL_CATEGORY, n)

top_nm <- inner_join(m_d, nulls) %>%
  mutate(n = round(Expr1 * p)) %>%
  select(PSI_CREDENTIAL_CATEGORY, n)

top_nf
top_nm


## ---- STOP !! manually add top_nf to queries below ----
# then change queries and do the same for top_mf
# Code later: https://github.com/r-dbi/DBI/issues/193
dbExecute(con, qry07c10_Assign_TopID_GenderF_GradCert)
dbExecute(con, qry07c11_Assign_TopID_GenderF_GradDipl)
dbExecute(con, qry07c12_Assign_TopID_GenderF_Masters)
dbExecute(con, qry07c13_Assign_TopID_GenderF_PostDegCert)
dbExecute(con, qry07c14_Assign_TopID_GenderF_PostDegDipl)
dbExecute(con, qry07c1_Assign_TopID_GenderF_AdvancedCert)
dbExecute(con, qry07c2_Assign_TopID_GenderF_AdvancedDip)
dbExecute(con, qry07c3_Assign_TopID_GenderF_Apprenticeship)
dbExecute(con, qry07c4_Assign_TopID_GenderF_AssocDegree)
dbExecute(con, qry07c5_Assign_TopID_GenderF_Bachelor)
dbExecute(con, qry07c6_Assign_TopID_GenderF_Certificate)
dbExecute(con, qry07c7_Assign_TopID_GenderF_Diploma)
dbExecute(con, qry07c8_Assign_TopID_GenderF_Doctorate)
dbExecute(con, qry07c9_Assign_TopID_GenderF_FirstProfDeg)
dbExecute(con, qry07c_Assign_TopID_GenderM)
dbExecute(con, qry07d_CorrectGender1)
dbExecute(con, qry07d_CorrectGender2)
dbExecute(con, "DROP TABLE CRED_Extract_No_Gender")
dbExecute(con, "DROP TABLE CRED_Extract_No_Gender_Unique")
dbExecute(con, "DROP VIEW Credential_Remove_Dup")
dbExecute(con, "DROP TABLE tmp_credential_epen_gender")
dbExecute(con, "DROP TABLE tmp_dup_credential_epen_gender")
dbExecute(con, "DROP TABLE tmp_dup_credential_epen_gender_maxcreddate")

# ---- 08 Credential Ranking ----
dbExecute(con, qry08_Create_Credential_Ranking_View_a)
dbExecute(con, qry08_Create_Credential_Ranking_View_b)
dbExecute(con, qry08_Create_Credential_Ranking_View_c)
dbExecute(con, qry08_Create_Credential_Ranking_View_d)
dbExecute(
  con,
  "ALTER TABLE tmp_Credential_Ranking_step3 ADD PSI_STUDENT_NUMBER varchar(50)"
)
dbExecute(
  con,
  "ALTER TABLE tmp_Credential_Ranking_step3 ADD PSI_CODE varchar(50)"
)
dbExecute(con, qry08_Create_Credential_Ranking_View_e)
dbExecute(con, qry08_Create_Credential_Ranking_View_f)
dbExecute(con, "DROP TABLE tmp_Credential_Ranking_step1")
dbExecute(con, "DROP TABLE tmp_Credential_Ranking_step2")
dbExecute(con, "DROP TABLE tmp_CredentialNonDup_STUD_NUM_PSI_CODE_MoreThanOne")
dbExecute(con, qry08_Create_Credential_Ranking_View_g)

res <- dbGetQuery(
  con,
  "SELECT DISTINCT id,
                        credential_ranking.encrypted_true_pen,
                        credential_ranking.psi_student_number,
                        credential_ranking.psi_code,
                        [encrypted_true_pen]+[psi_student_number] AS concatenated_id,
                        credential_ranking.credential_award_date_d,
                        credential_ranking.rank,
                        credential_ranking.highest_cred_by_date, 
                        credential_ranking.highest_cred_by_rank FROM credential_ranking"
)
names(res) <- tolower(names(res))

res <- res %>%
  mutate(highest_cred_by_rank = NA) %>%
  mutate(highest_cred_by_date = NA)

res <- res %>%
  group_by(encrypted_true_pen, psi_student_number) %>%
  arrange(
    encrypted_true_pen,
    psi_student_number,
    psi_code,
    desc(credential_award_date_d),
    rank,
    .by_group = TRUE
  ) %>%
  mutate(highest_cred_by_date = replace(highest_cred_by_date, 1, 'Yes')) %>%
  ungroup()

res <- res %>%
  group_by(encrypted_true_pen, psi_student_number) %>%
  arrange(
    encrypted_true_pen,
    psi_student_number,
    psi_code,
    rank,
    desc(credential_award_date_d),
    .by_group = TRUE
  ) %>%
  mutate(highest_cred_by_rank = replace(highest_cred_by_rank, 1, 'Yes')) %>%
  ungroup()

dbWriteTable(con, name = 'tmp_Credential_Ranking', res, overwrite = TRUE)

dbExecute(
  con,
  "ALTER TABLE tmp_credential_Ranking ALTER COLUMN id INT NOT NULL;"
)

dbExecute(con, qry08a1_Update_CredentialNonDup_with_highestDate_Rank)
dbExecute(con, qry08a_Run_after_Credential_Ranking)
dbExecute(con, qry08b_Rank_non_multi_cred)
dbExecute(con, "DROP TABLE tmp_Credential_Ranking")
dbExecute(con, "DROP TABLE tmp_Credential_Ranking_step3")
dbExecute(con, "DROP VIEW Credential_Ranking")

# ---- 09 Age Gender Distributions ----
dbExecute(con, qry09a_ExtractNoAge)
dbExecute(con, "ALTER TABLE CRED_Extract_No_Age ADD PRIMARY KEY (id);")
dbExecute(con, qry09b_ExtractNoAgeUnique)
dbExecute(con, "ALTER TABLE CRED_Extract_No_Age_Unique ADD PRIMARY KEY (id);")

sql <- "SELECT PSI_GENDER_CLEANED, PSI_CREDENTIAL_CATEGORY, COUNT(*) AS NumWithNullAge
FROM CRED_Extract_No_Age_Unique GROUP BY PSI_GENDER_CLEANED, PSI_CREDENTIAL_CATEGORY"
CRED_Extract_No_Age_Unique <- dbGetQuery(con, sql)
CREDAgeDistributionbyGender <- dbGetQuery(con, qry09d_ShowAgeGenderDistribution)

d <- CREDAgeDistributionbyGender %>%
  group_by(PSI_GENDER_CLEANED, PSI_CREDENTIAL_CATEGORY) %>%
  mutate(p = NumGrads / sum(NumGrads)) %>%
  left_join(
    CRED_Extract_No_Age_Unique,
    by = join_by(PSI_GENDER_CLEANED, PSI_CREDENTIAL_CATEGORY)
  ) %>%
  mutate(n = round(p * NumWithNullAge)) %>%
  arrange(PSI_GENDER_CLEANED, PSI_CREDENTIAL_CATEGORY, AGE_AT_GRAD) %>%
  filter(!is.na(NumWithNullAge))

# consider sampling instead to ensure randomness and give full coverage
print("imputing missing age_at_grad ....")
for (i in 1:nrow(d)) {
  sql <- "UPDATE TOP(?n) CRED_Extract_No_Age_Unique
          SET AGE_AT_GRAD = ?age 
          WHERE PSI_GENDER_CLEANED  = ?gender
            AND PSI_CREDENTIAL_CATEGORY = ?cred
            AND (AGE_AT_GRAD IS NULL OR AGE_AT_GRAD = ' ');"
  sql <- sqlInterpolate(
    con,
    sql,
    n = as.numeric(d[i, "n"]),
    age = as.numeric(d[i, "AGE_AT_GRAD"]),
    gender = as.character(d[i, "PSI_GENDER_CLEANED"]),
    cred = as.character(d[i, "PSI_CREDENTIAL_CATEGORY"])
  )
  dbExecute(con, sql)
}
print("....done")

# assign a random age between 19 and 54 to any remaining nulls.
dbExecute(
  con,
  "UPDATE CRED_Extract_No_Age_Unique
                SET AGE_AT_GRAD = (ABS(CHECKSUM(NewId())) % 35) + 19
                WHERE AGE_AT_GRAD IS NULL OR AGE_AT_GRAD = ' '"
)

# See documentation at this point for further processing of multiple credentials

dbExecute(con, qry10_Update_Extract_No_Age)
dbExecute(con, qry11a_UpdateAgeAtGrad)
dbExecute(con, qry11b_UpdateAGAtGrad)
dbExecute(con, "DROP TABLE CRED_Extract_No_Age")
dbExecute(con, "DROP TABLE CRED_Extract_No_Age_Unique")
#dbExecute(con, "DROP TABLE CREDAgeDistributionbyGender")

# ---- VISA Status ----
dbExecute(con, "ALTER TABLE CredentialSupVars ADD PSI_VISA_STATUS varchar(50)")
dbExecute(con, "ALTER TABLE Credential_Non_Dup ADD PSI_VISA_STATUS varchar(50)")
dbGetQuery(con, CredentialSupVars_VisaStatus_Cleaning_check)
dbExecute(con, CredentialSupVars_VisaStatus_Cleaning_1)
dbExecute(con, CredentialSupVars_VisaStatus_Cleaning_2)
dbExecute(con, CredentialSupVars_VisaStatus_Cleaning_3)
dbExecute(con, CredentialSupVars_VisaStatus_Cleaning_4)
dbExecute(con, CredentialSupVars_VisaStatus_Cleaning_5)
dbExecute(con, CredentialSupVars_VisaStatus_Cleaning_6)
dbGetQuery(con, CredentialSupVars_VisaStatus_Cleaning_check)
dbExecute(con, "DROP TABLE CredentialSupVars_VisaStatus_Cleaning_Step2")
dbExecute(con, "DROP TABLE Credential_Non_Dup_VisaStatus_Cleaning_Step1")
dbExecute(con, "DROP TABLE CredentialSupVars_VisaStatus_Cleaning_Step1")

# ---- Highest Rank ----
dbExecute(
  con,
  "ALTER TABLE Credential_Non_Dup ADD CONCATENATED_ID VARCHAR(255) NULL"
)
dbExecute(
  con,
  "UPDATE Credential_Non_Dup SET CONCATENATED_ID = ENCRYPTED_TRUE_PEN 
                 WHERE (ENCRYPTED_TRUE_PEN IS NOT NULL AND ENCRYPTED_TRUE_PEN NOT IN ('', ' ', '(Unspecified)'))"
)
dbExecute(
  con,
  "UPDATE Credential_Non_Dup SET CONCATENATED_ID = PSI_STUDENT_NUMBER + PSI_CODE 
                WHERE (ENCRYPTED_TRUE_PEN IS NULL) OR (ENCRYPTED_TRUE_PEN IN ('', ' ', '(Unspecified)'))"
)
dbExecute(con, qry12_Create_View_tblCredentialHighestRank)

dbExecute(con, qry18a_ExtrLaterAwarded)
dbExecute(con, qry18b_ExtrLaterAwarded)
dbExecute(con, qry18c_ExtrLaterAwarded)
dbExecute(con, qry18d_ExtrLaterAwarded)
dbExecute(con, "DROP TABLE tmp_qry18b_ExtrLaterAwarded_2")
dbExecute(con, "DROP TABLE tmp_qry18c_ExtrLaterAwarded_3")
dbExecute(con, "DROP TABLE tblcredential_laterawarded")

# ---- 13 Delay Date ----
dbExecute(con, qry19_UpdateDelayDate)
dbExecute(con, "DROP TABLE tblCredential_DelayEffect")

dbExecute(
  con,
  "ALTER TABLE Credential_Non_Dup 
                ADD CREDENTIAL_AWARD_DATE_D_DELAYED date, 
                PSI_AWARD_SCHOOL_YEAR_DELAYED varchar(50);"
)

dbExecute(con, qry13a_UpdateDelayedCredDate)
dbExecute(con, qry13b_UpdateDelayedCredDate)
dbExecute(con, qry13_UpdateDelayedCredDate)

# ---- 14-15 research University + Outcomes Credential ----
dbExecute(con, qry14_ResearchUniversity)
dbExecute(con, qry15_OutcomeCredential)

# update non-dup table here

# ---- Break and do Program Matching ----
# IMPORTANT!!! THIS SECTION CAN ONLY BE RUN AFTER THE PROGRAM MATCHING WORK HAS BEEN DONE
# TODO: This will later be moved to a different script.
# Tables that are needed at this point include:

# ---- Check Required Tables ----
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."credential_non_dup"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."AgeGroupLookup"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."tblCredential_HighestRank"')))

# ---- 20 Final Distributions ----
# NOTE: Exclude_CIPs queries end up with Invalid column name 'FINAL_CIP_CLUSTER_CODE'.
dbExecute(con, qry20a_1Credential_By_Year_AgeGroup)
dbExecute(con, qry20a_1Credential_By_Year_AgeGroup_Exclude_CIPs)
dbExecute(con, qry20a_2Credential_By_Year_AgeGroup_Domestic)
dbExecute(con, qry20a_2Credential_By_Year_AgeGroup_Domestic_Exclude_CIPs)
dbExecute(con, qry20a_3Credential_By_Year_AgeGroup_Domestic_Exclude_RU_DACSO)
dbExecute(
  con,
  qry20a_4Credential_By_Year_CIP4_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs
)
dbExecute(
  con,
  qry20a_4Credential_By_Year_CIP4_Gender_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs
) #
dbExecute(con, qry20a_4Credential_By_Year_Gender_AgeGroup_Domestic_Exclude_CIPs)
dbExecute(
  con,
  qry20a_4Credential_By_Year_Gender_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs
)

# these two need a table we don't have - ignore for now
# dbGetQuery(con, qry20a_4Credential_By_Year_PSI_TYPE_Domestic_Exclude_RU_DACSO_Exclude_CIPs)
# dbGetQuery(con, qry20a_4Credential_By_Year_PSI_TYPE_Domestic_Exclude_RU_DACSO_Exclude_CIPs_Not_Highest)

dbExecute(con, qry20a_99_Checking_Excluding_RU_DACSO_Variables)

# not used?
# dbGetQuery(con, qryCreateIDinSTPCredential)

dbExecute(con, qry_Update_Cdtl_Sup_Vars_InternationalFlag)

# ---- Clean Up ----
dbExecute(con, "DROP TABLE CredentialSupVarsFromEnrolment")
dbExecute(con, "DROP TABLE CredentialSupVars")
dbExecute(con, "DROP TABLE CredentialSupVars_BirthdateClean")
dbExecute(con, "DROP VIEW Credential")
dbExecute(con, "DROP VIEW Credential_Ranking")
dbDisconnect(con)

# ---- These tables used later ----
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."tblCredential_HighestRank"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."Credential_Non_Dup"')))
