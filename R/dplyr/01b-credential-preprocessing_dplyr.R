# Credential Preprocessing — dplyr Translation
# Original: R/01b-credential-preprocessing.R
#
# Pipeline context:
#   Workflow #2 in the PSSM pipeline. Takes raw credential data (STP_Credential,
#   loaded by load-stp-cred.R) and classifies each record with a RecordStatus code
#   that determines whether it should be included in downstream analysis.
#
#   RecordStatus codes:
#     0 = Good (included in model)
#     1 = Missing Student Number
#     2 = Developmental
#     3 = No PSI Transition
#     4 = Credential Only (No Enrolment Record)
#     5 = PSI_Outside_BC
#     6 = Skills Based
#     7 = Developmental CIP
#     8 = Recommendation for Certification
#
#   Steps 1–4 happen in earlier scripts (01a, load-stp-cred). This script handles
#   statuses 1, 2, 6, 7, 8, and 0 (default for records not flagged as any other status).
#   The classification is sequential — each step only assigns a status to records that
#   haven't been classified yet. Earlier assignments take priority.
#
# Input tables:
#   - STP_Credential — raw credential records (from load-stp-cred.R)
#   - STP_Enrolment_Record_Type — enrolment records with classification (from 01a)
#   - STP_Enrolment — raw enrolment records
#
# Output table:
#   - STP_Credential_Record_Type — ID + EPEN + RecordStatus + MinEnrolment + FirstEnrolment
#
# Side effects:
#   - Adds ID primary key to STP_Credential
#   - Reformats date columns in STP_Credential from yy-mm-dd to yyyy-mm-dd

library(arrow)
library(tidyverse)
library(dbplyr)
library(odbc)
library(DBI)

# Helper: negated %in% for readable exclusion filters
`%notin%` <- function(x, y) !(x %in% y)

# ---- Configure LAN Paths and DB Connection -----
lan <- config::get("lan")
db_config <- config::get("decimal")
my_schema <- config::get("myschema")

con <- dbConnect(
  odbc(),
  Driver = db_config$driver,
  Server = db_config$server,
  Database = db_config$database,
  Trusted_Connection = "True"
)

# Helper: reference a table in the user's schema
sch_tbl <- function(name) {
  tbl(con, dbplyr::in_schema(my_schema, name))
}

# ---- Check Required Tables etc. ----
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Credential"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Enrolment_Record_Type"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Enrolment"')))


# Diagnostic queries to verify data quality before processing. These count null/blank
# encrypted PENs and distinct PENs — useful for catching data loading issues.
# ---- Checks ----
sch_tbl("STP_Credential") %>%
  filter(
    ENCRYPTED_TRUE_PEN %in%
      c('', ' ', '(Unspecified)') |
      is.na(ENCRYPTED_TRUE_PEN)
  ) %>%
  summarise(n_null_epens = n()) %>%
  collect()

sch_tbl("STP_Credential") %>%
  summarise(n_epens = n_distinct(ENCRYPTED_TRUE_PEN)) %>%
  collect()

# STP_Credential needs an auto-incrementing ID for joins throughout the pipeline.
# ---- Add primary key ----
# KEPT AS SQL: DDL operations (ALTER TABLE)
dbExecute(
  con,
  "
  ALTER TABLE STP_Credential
  ADD ID INT IDENTITY(1,1) NOT NULL"
)

dbExecute(
  con,
  "
  ALTER TABLE STP_Credential
  ADD CONSTRAINT STP_Credential_PK_ID PRIMARY KEY (ID)"
)

# Some dates are in yy-mm-dd format (2-digit year). Convert to yyyy-mm-dd:
# years < 24 get '20' prefix (2000s), years > 23 get '19' prefix (1900s),
# blanks (leading spaces) become empty string.
#
# We compute converted dates in R then write them back via a temp table + SQL UPDATE,
# since the target table has an IDENTITY column that can't be simply overwritten.
# ---- Reformat yy-mm-dd to yyyy-mm-dd ----
# check date variable format here
dbGetQuery(
  con,
  "SELECT TOP 100 CREDENTIAL_AWARD_DATE, PSI_PROGRAM_EFFECTIVE_DATE FROM STP_Credential;"
)
dbGetQuery(con, "SELECT TOP 100 * FROM STP_Credential;")

# Pull all columns needed for both date conversion and the classification steps below.
# This avoids multiple round trips to the database.
stp_cred <- sch_tbl("STP_Credential") %>%
  select(
    ID,
    ENCRYPTED_TRUE_PEN,
    PSI_STUDENT_NUMBER,
    PSI_CODE,
    PSI_CREDENTIAL_CIP,
    PSI_CREDENTIAL_CATEGORY,
    PSI_CREDENTIAL_LEVEL,
    PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    PSI_SCHOOL_YEAR,
    CREDENTIAL_AWARD_DATE,
    PSI_PROGRAM_EFFECTIVE_DATE
  ) %>%
  collect() |>
  rename_with(toupper)

# Compute converted dates using dplyr case_when
tmp_convert_dates <- stp_cred %>%
  select(ID, CREDENTIAL_AWARD_DATE, PSI_PROGRAM_EFFECTIVE_DATE) %>%
  mutate(
    CREDENTIAL_AWARD_DATE_CONVERT = case_when(
      substr(CREDENTIAL_AWARD_DATE, 1, 2) == '  ' ~ '',
      as.integer(substr(CREDENTIAL_AWARD_DATE, 1, 2)) < 24 ~ paste0(
        '20',
        CREDENTIAL_AWARD_DATE
      ),
      as.integer(substr(CREDENTIAL_AWARD_DATE, 1, 2)) > 23 ~ paste0(
        '19',
        CREDENTIAL_AWARD_DATE
      ),
      TRUE ~ CREDENTIAL_AWARD_DATE
    ),
    PSI_PROGRAM_EFFECTIVE_DATE_CONVERT = case_when(
      substr(PSI_PROGRAM_EFFECTIVE_DATE, 1, 2) == '  ' ~ '',
      as.integer(substr(PSI_PROGRAM_EFFECTIVE_DATE, 1, 2)) < 24 ~ paste0(
        '20',
        PSI_PROGRAM_EFFECTIVE_DATE
      ),
      as.integer(substr(PSI_PROGRAM_EFFECTIVE_DATE, 1, 2)) > 23 ~ paste0(
        '19',
        PSI_PROGRAM_EFFECTIVE_DATE
      ),
      TRUE ~ PSI_PROGRAM_EFFECTIVE_DATE
    )
  ) %>%
  select(ID, CREDENTIAL_AWARD_DATE_CONVERT, PSI_PROGRAM_EFFECTIVE_DATE_CONVERT)

dbWriteTable(
  con,
  "tmp_ConvertDateFormatCredential",
  tmp_convert_dates,
  overwrite = TRUE
)

# KEPT AS SQL: UPDATE...FROM (multi-table update, no dplyr equivalent for updating
# one database table from another)
dbExecute(
  con,
  "
  UPDATE STP_Credential
  SET CREDENTIAL_AWARD_DATE = tmp.CREDENTIAL_AWARD_DATE_CONVERT
  FROM tmp_ConvertDateFormatCredential tmp
  WHERE STP_Credential.ID = tmp.ID;"
)

dbExecute(
  con,
  "
  UPDATE STP_Credential
  SET PSI_PROGRAM_EFFECTIVE_DATE = tmp.PSI_PROGRAM_EFFECTIVE_DATE_CONVERT
  FROM tmp_ConvertDateFormatCredential tmp
  WHERE STP_Credential.ID = tmp.ID;"
)

dbExecute(con, "DROP TABLE tmp_ConvertDateFormatCredential")

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

# Initialize the classification table with all credential IDs and their encrypted PENs.
# The remaining columns (RecordStatus, MinEnrolment, FirstEnrolment) will be filled
# by the classification steps below.
# ---- Create lookup table for ID/Record Status and populate with ID column and EPEN ----
cred_record_type <- stp_cred %>%
  select(ID, ENCRYPTED_TRUE_PEN) %>%
  mutate(
    RECORDSTATUS = NA_integer_,
    MINENROLMENT = NA_integer_,
    FIRSTENROLMENT = NA_integer_
  )


# Status 1 = Missing Student Number. Records that have neither a valid student number
# + PSI code combo nor a valid encrypted PEN. Without any identifier, these can't be
# matched to enrolment data and must be excluded from downstream analysis.
#
# WHY the complement approach: we identify records WITH valid IDs first, then use
# anti_join to find the rest. This avoids complex negated filter logic.
# ---- Find records with Record_Status = 1 ----

records_with_valid_id <- stp_cred %>%
  filter(
    (PSI_STUDENT_NUMBER %notin%
      c('', ' ', '(Unspecified)') &
      PSI_CODE %notin% c('', ' ', '(Unspecified)')) |
      (ENCRYPTED_TRUE_PEN %notin% c('', ' ', '(Unspecified)'))
  )

ids_status_1 <- stp_cred %>%
  anti_join(records_with_valid_id, by = "ID") %>%
  pull(ID)

cred_record_type <- cred_record_type %>%
  mutate(RECORDSTATUS = if_else(ID %in% ids_status_1, 1L, RECORDSTATUS))


# Status 2 = Developmental. Records with credential level "DEVELOPMENTAL" that
# haven't already been assigned a status. These represent non-credit developmental
# courses that don't lead to credentials counted in the model.
# ---- Find records with Record_Status = 2 ----

ids_status_2 <- stp_cred %>%
  filter(PSI_CREDENTIAL_LEVEL == "DEVELOPMENTAL") %>%
  pull(ID)

cred_record_type <- cred_record_type %>%
  mutate(
    RECORDSTATUS = if_else(
      is.na(RECORDSTATUS) & ID %in% ids_status_2,
      2L,
      RECORDSTATUS
    )
  )


# Status 6 = Skills Based. These are credentials whose program profile matches
# enrolment records already classified as skills-based in the enrolment preprocessing
# step (01a). The matching is on PSI code, program description, CIP2 prefix,
# credential category, and credential level (mapped to study level in enrolment data).
#
# Step 1: Build the skills-based course profile from enrolment data — this defines
# which program combinations are considered skills-based.
# Step 2: Find unclassified credentials that match any profile.
# ---- Find records with Record_Status = 6 ----

stp_enrolment_record_type <- sch_tbl("STP_Enrolment_Record_Type") %>%
  select(ID, RECORDSTATUS) %>%
  collect() |>
  rename_with(toupper)

stp_enrolment <- sch_tbl("STP_Enrolment") %>%
  select(
    ID,
    PSI_CODE,
    PSI_PROGRAM_CODE,
    PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    PSI_CIP_CODE,
    PSI_CREDENTIAL_CATEGORY,
    PSI_STUDY_LEVEL,
    PSI_CONTINUING_EDUCATION_COURSE_ONLY
  ) %>%
  collect() |>
  rename_with(toupper)

# Step 1: Aggregate enrolment records with RecordStatus = 6 by program attributes
enrolment_skills <- stp_enrolment_record_type %>%
  inner_join(stp_enrolment, by = "ID") %>%
  filter(RECORDSTATUS == 6) %>%
  mutate(CIP2 = substr(PSI_CIP_CODE, 1, 2)) %>%
  count(
    PSI_CODE,
    PSI_PROGRAM_CODE,
    PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    CIP2,
    PSI_CREDENTIAL_CATEGORY,
    PSI_STUDY_LEVEL,
    PSI_CONTINUING_EDUCATION_COURSE_ONLY,
    name = "COUNT"
  )

# Step 2: Match unclassified credentials against skills-based profiles.
# semi_join finds credentials whose program attributes match any row in enrolment_skills.
suspect_skills <- stp_cred %>%
  inner_join(
    cred_record_type %>% filter(is.na(RECORDSTATUS)) %>% select(ID),
    by = "ID"
  ) %>%
  mutate(CIP2 = substr(PSI_CREDENTIAL_CIP, 1, 2)) %>%
  semi_join(
    enrolment_skills,
    by = c(
      "PSI_CODE",
      "PSI_CREDENTIAL_PROGRAM_DESCRIPTION",
      "CIP2",
      "PSI_CREDENTIAL_CATEGORY",
      "PSI_CREDENTIAL_LEVEL" = "PSI_STUDY_LEVEL"
    )
  )

cred_record_type <- cred_record_type %>%
  mutate(
    RECORDSTATUS = if_else(
      is.na(RECORDSTATUS) & ID %in% suspect_skills$ID,
      6L,
      RECORDSTATUS
    )
  )


# Status 7 = Developmental CIP. Credentials with CIP prefixes in a set of known
# developmental/skills codes are flagged, EXCEPT those the analyst manually marks to
# keep. This requires human judgment because some CIP codes in these prefixes are
# legitimate non-developmental programs.
# ---- Find records with Record_Status = 7 and update look up table ----

dev_cip_prefixes <- c('21', '32', '33', '34', '35', '36', '37', '53', '89')

drop_dev_cips <- stp_cred %>%
  inner_join(
    cred_record_type %>% filter(is.na(RECORDSTATUS)) %>% select(ID),
    by = "ID"
  ) %>%
  mutate(CIP2 = substr(PSI_CREDENTIAL_CIP, 1, 2)) %>%
  filter(CIP2 %in% dev_cip_prefixes) %>%
  select(
    ID,
    ENCRYPTED_TRUE_PEN,
    PSI_CODE,
    PSI_STUDENT_NUMBER,
    PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    CIP2,
    PSI_CREDENTIAL_CATEGORY
  ) %>%
  mutate(KEEP = NA_character_)

###  ---- ** Manual **  ----
# The analyst reviews these suspect records against the outcomes programs table.
# Setting Keep = 'Y' for a record EXCLUDES it from status 7 (keeps it in the model).
data.entry(drop_dev_cips)

ids_status_7 <- drop_dev_cips %>%
  filter(is.na(KEEP)) %>%
  pull(ID)

cred_record_type <- cred_record_type %>%
  mutate(
    RECORDSTATUS = if_else(
      is.na(RECORDSTATUS) & ID %in% ids_status_7,
      7L,
      RECORDSTATUS
    )
  )


# Status 8 = Recommendation for Certification. These represent recommendations rather
# than actual credential awards, so they're excluded from the model.
# ---- Find records with Record_Status = 8 and update look up table ----

ids_status_8 <- stp_cred %>%
  filter(PSI_CREDENTIAL_CATEGORY == "RECOMMENDATION FOR CERTIFICATION") %>%
  inner_join(
    cred_record_type %>% filter(is.na(RECORDSTATUS)) %>% select(ID),
    by = "ID"
  ) %>%
  pull(ID)

cred_record_type <- cred_record_type %>%
  mutate(
    RECORDSTATUS = if_else(
      is.na(RECORDSTATUS) & ID %in% ids_status_8,
      8L,
      RECORDSTATUS
    )
  )


# All records that passed every exclusion check are "Good" — included in the model.
# ---- qry04_Update_RecordStatus_Not_Dropped ----
cred_record_type <- cred_record_type %>%
  mutate(RECORDSTATUS = if_else(is.na(RECORDSTATUS), 0L, RECORDSTATUS))

# ---- RecordTypeSummary ----
# Diagnostic summary showing how many records were assigned each status code.
cred_record_type %>%
  count(RECORDSTATUS, name = "EXPR1")

# Write the completed classification table to the database for downstream scripts
# (02a through 08) to reference.
dbWriteTable(
  con,
  SQL(glue::glue('"{my_schema}"."STP_Credential_Record_Type"')),
  cred_record_type,
  overwrite = TRUE
)

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
