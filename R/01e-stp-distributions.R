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

log_info("==== 01e-stp-distributions.R START ====")

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
## -----------------------------------------------------------------------------------------------

age_group_lookup <- dbReadTable(
  con,
  SQL(glue::glue('"{my_schema}"."age_group_lookup_r"'))
)
log_info(glue::glue("Loaded age_group_lookup_r: {nrow(age_group_lookup)} rows"))

# We need to bring in PSI_VISA_STATUS here as well.  (add in 01d)
min_enrolment <- dbGetQuery(
  con,
  glue::glue(
    'SELECT
      PSI_SCHOOL_YEAR,
      PSI_CREDENTIAL_CATEGORY,
      PSI_CIP_CODE,
      PSI_GENDER,
      AGE_GROUP_ENROL_DATE
    FROM "{my_schema}"."min_enrolment_r"'
  )
)
log_info(glue::glue("Loaded min_enrolment_r: {nrow(min_enrolment)} rows"))

credential_non_dup <- dbGetQuery(
  con,
  glue::glue(
    'SELECT
      ID,
      RESEARCH_UNIVERSITY,
      OUTCOMES_CRED,
      FINAL_CIP_CLUSTER_CODE, 
      FINAL_CIP_CODE_4
    FROM "{my_schema}"."credential_non_dup_r"'
  ) # the FINAL_CIP is created from 02a session, so 02a session must be done before this step
)
log_info(glue::glue(
  "Loaded credential_non_dup_r: {nrow(credential_non_dup)} rows"
))

tbl_credential_highest_rank <-
  dbGetQuery(
    con,
    glue::glue(
      'SELECT
      ID,
      psi_gender_cleaned,
      AGE_GROUP_AT_GRAD,
      PSI_CREDENTIAL_CATEGORY,
      PSI_AWARD_SCHOOL_YEAR_DELAYED,
      PSI_VISA_STATUS
    FROM "{my_schema}"."tbl_credential_highest_rank_r"'
    )
  )
log_info(glue::glue(
  "Loaded tbl_credential_highest_rank_r: {nrow(tbl_credential_highest_rank)} rows"
))


# bring research university and outcomes credential into tbl_credential_highest_rank
# this should have been done at end of 01c-credential-analysis.R
tbl_credential_highest_rank <- tbl_credential_highest_rank |>
  left_join(
    credential_non_dup |> select(ID, RESEARCH_UNIVERSITY, OUTCOMES_CRED),
    by = "ID"
  )
log_info(
  "Enriched tbl_credential_highest_rank with RESEARCH_UNIVERSITY and OUTCOMES_CRED from credential_non_dup"
)

## ---------------------------- Final Credential Distributions --------------------------------
# References: 01c-credential-analysis.R
#  qry20a_ series
#
# What the code does:
#  - Generate aggregated counts of the highest credential earned by various combinations of
# gender, school year, age group, credential category, cip code and visa status
#  - Several aggregations are created but only Credential_By_Year_Gender_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs
# was used last model run (PSSM 2023/2024)
#
# BA Notes:
# two others in the original SQL queries which require a table we don't have:
# dbGetQuery(con, qry20a_4Credential_By_Year_PSI_TYPE_Domestic_Exclude_RU_DACSO_Exclude_CIPs)
# dbGetQuery(con, qry20a_4Credential_By_Year_PSI_TYPE_Domestic_Exclude_RU_DACSO_Exclude_CIPs_Not_Highest)

## -----------------------------------------------------------------------------------------------

credential_by_year_age_group <- tbl_credential_highest_rank |>
  inner_join(age_group_lookup, by = c("AGE_GROUP_AT_GRAD" = "AgeIndex")) |> #filters out invalid ages
  filter(PSI_CREDENTIAL_CATEGORY != "Apprenticeship") |>
  group_by(AgeGroup, PSI_CREDENTIAL_CATEGORY, PSI_AWARD_SCHOOL_YEAR_DELAYED) |>
  summarise(Count = n(), .groups = "drop") |>
  arrange(AgeGroup, PSI_CREDENTIAL_CATEGORY, PSI_AWARD_SCHOOL_YEAR_DELAYED)

log_info(glue::glue(
  "Credential distribution: credential_by_year_age_group: {nrow(credential_by_year_age_group)} rows"
))

# Exclude CIP clusters 09 and 10
credential_by_year_age_group_exclude_cips <- tbl_credential_highest_rank |>
  inner_join(age_group_lookup, by = c("AGE_GROUP_AT_GRAD" = "AgeIndex")) |>
  inner_join(
    credential_non_dup |> select(ID, FINAL_CIP_CLUSTER_CODE),
    by = "ID"
  ) |>
  filter(
    PSI_CREDENTIAL_CATEGORY != "Apprenticeship",
    FINAL_CIP_CLUSTER_CODE != "09",
    FINAL_CIP_CLUSTER_CODE != "10"
  ) |>
  group_by(AgeGroup, PSI_CREDENTIAL_CATEGORY, PSI_AWARD_SCHOOL_YEAR_DELAYED) |>
  summarise(Count = n(), .groups = "drop") |>
  arrange(AgeGroup, PSI_CREDENTIAL_CATEGORY, PSI_AWARD_SCHOOL_YEAR_DELAYED)

log_info(glue::glue(
  "Credential distribution: credential_by_year_age_group_exclude_cips: {nrow(credential_by_year_age_group_exclude_cips)} rows"
))

# Domestic only
credential_by_year_age_group_domestic <- tbl_credential_highest_rank |>
  inner_join(age_group_lookup, by = c("AGE_GROUP_AT_GRAD" = "AgeIndex")) |>
  filter(
    PSI_CREDENTIAL_CATEGORY != "Apprenticeship",
    PSI_VISA_STATUS == "Domestic" | is.na(PSI_VISA_STATUS)
  ) |>
  group_by(AgeGroup, PSI_CREDENTIAL_CATEGORY, PSI_AWARD_SCHOOL_YEAR_DELAYED) |>
  summarise(Count = n(), .groups = "drop") |>
  arrange(AgeGroup, PSI_CREDENTIAL_CATEGORY, PSI_AWARD_SCHOOL_YEAR_DELAYED)

log_info(glue::glue(
  "Credential distribution: credential_by_year_age_group_domestic: {nrow(credential_by_year_age_group_domestic)} rows"
))

# Domestic only, exclude CIPs
credential_by_year_age_group_domestic_exclude_cips <- tbl_credential_highest_rank |>
  inner_join(age_group_lookup, by = c("AGE_GROUP_AT_GRAD" = "AgeIndex")) |>
  inner_join(
    credential_non_dup |> select(ID, FINAL_CIP_CLUSTER_CODE),
    by = "ID"
  ) |>
  filter(
    PSI_CREDENTIAL_CATEGORY != "Apprenticeship",
    (PSI_VISA_STATUS == "Domestic" | is.na(PSI_VISA_STATUS)),
    FINAL_CIP_CLUSTER_CODE != "09",
    FINAL_CIP_CLUSTER_CODE != "10"
  ) |>
  group_by(AgeGroup, PSI_CREDENTIAL_CATEGORY, PSI_AWARD_SCHOOL_YEAR_DELAYED) |>
  summarise(Count = n(), .groups = "drop") |>
  arrange(AgeGroup, PSI_CREDENTIAL_CATEGORY, PSI_AWARD_SCHOOL_YEAR_DELAYED)

log_info(glue::glue(
  "Credential distribution: credential_by_year_age_group_domestic_exclude_cips: {nrow(credential_by_year_age_group_domestic_exclude_cips)} rows"
))

# Domestic only, exclude research universities and DACSO
# Notes from 2019 docs suggest we exclude credentials which are
# included in DACSO but that are granted by research universities.
credential_by_year_age_group_domestic_exclude_ru_dacso <- tbl_credential_highest_rank |>
  inner_join(age_group_lookup, by = c("AGE_GROUP_AT_GRAD" = "AgeIndex")) |>
  filter(
    PSI_CREDENTIAL_CATEGORY != "Apprenticeship",
    PSI_VISA_STATUS == "Domestic" | is.na(PSI_VISA_STATUS),
    is.na(RESEARCH_UNIVERSITY) |
      (RESEARCH_UNIVERSITY == 1 & OUTCOMES_CRED != 'DACSO')
  ) |>
  group_by(AgeGroup, PSI_CREDENTIAL_CATEGORY, PSI_AWARD_SCHOOL_YEAR_DELAYED) |>
  summarise(Count = n(), .groups = "drop") |>
  arrange(AgeGroup, PSI_CREDENTIAL_CATEGORY, PSI_AWARD_SCHOOL_YEAR_DELAYED)

log_info(glue::glue(
  "Credential distribution: credential_by_year_age_group_domestic_exclude_ru_dacso: {nrow(credential_by_year_age_group_domestic_exclude_ru_dacso)} rows"
))

# CIP4, AgeGroup, Domestic, Exclude RU & DACSO, Exclude CIPs
credential_by_year_cip4_agegroup_domestic_exclude_ru_dacso_exclude_cips <- tbl_credential_highest_rank |>
  inner_join(age_group_lookup, by = c("AGE_GROUP_AT_GRAD" = "AgeIndex")) |>
  inner_join(
    credential_non_dup |> select(ID, FINAL_CIP_CLUSTER_CODE, FINAL_CIP_CODE_4),
    by = "ID"
  ) |>
  filter(
    PSI_CREDENTIAL_CATEGORY != "Apprenticeship",
    PSI_VISA_STATUS == "Domestic" | is.na(PSI_VISA_STATUS),
    is.na(RESEARCH_UNIVERSITY) |
      (RESEARCH_UNIVERSITY == 1 & OUTCOMES_CRED != 'DACSO'),
    FINAL_CIP_CLUSTER_CODE != "09",
    FINAL_CIP_CLUSTER_CODE != "10"
  ) |>
  group_by(
    FINAL_CIP_CODE_4,
    AgeGroup,
    PSI_CREDENTIAL_CATEGORY,
    PSI_AWARD_SCHOOL_YEAR_DELAYED
  ) |>
  summarise(Count = n(), .groups = "drop") |>
  arrange(
    FINAL_CIP_CODE_4,
    AgeGroup,
    PSI_CREDENTIAL_CATEGORY,
    PSI_AWARD_SCHOOL_YEAR_DELAYED
  )

log_info(glue::glue(
  "Credential distribution: credential_by_year_cip4_agegroup_domestic_exclude_ru_dacso_exclude_cips: {nrow(credential_by_year_cip4_agegroup_domestic_exclude_ru_dacso_exclude_cips)} rows"
))

# CIP4, Gender, AgeGroup, Domestic, Exclude RU & DACSO, Exclude CIPs
credential_by_year_cip4_gender_agegroup_domestic_exclude_ru_dacso_exclude_cips <- tbl_credential_highest_rank |>
  inner_join(age_group_lookup, by = c("AGE_GROUP_AT_GRAD" = "AgeIndex")) |>
  inner_join(
    credential_non_dup |>
      select(ID, FINAL_CIP_CLUSTER_CODE, FINAL_CIP_CODE_4),
    by = "ID"
  ) |>
  filter(
    PSI_CREDENTIAL_CATEGORY != "Apprenticeship",
    (PSI_VISA_STATUS == "Domestic" | is.na(PSI_VISA_STATUS)),
    is.na(RESEARCH_UNIVERSITY) |
      (RESEARCH_UNIVERSITY == 1 & OUTCOMES_CRED != 'DACSO'),
    FINAL_CIP_CLUSTER_CODE != "09",
    FINAL_CIP_CLUSTER_CODE != "10"
  ) |>
  group_by(
    FINAL_CIP_CODE_4,
    psi_gender_cleaned,
    AgeGroup,
    PSI_CREDENTIAL_CATEGORY,
    PSI_AWARD_SCHOOL_YEAR_DELAYED
  ) |>
  summarise(Count = n(), .groups = "drop") |>
  arrange(
    FINAL_CIP_CODE_4,
    psi_gender_cleaned,
    AgeGroup,
    PSI_CREDENTIAL_CATEGORY,
    PSI_AWARD_SCHOOL_YEAR_DELAYED
  )

log_info(glue::glue(
  "Credential distribution: credential_by_year_cip4_gender_agegroup_domestic_exclude_ru_dacso_exclude_cips: {nrow(credential_by_year_cip4_gender_agegroup_domestic_exclude_ru_dacso_exclude_cips)} rows"
))

# Gender, AgeGroup, Domestic, Exclude CIPs
credential_by_year_gender_agegroup_domestic_exclude_cips <- tbl_credential_highest_rank |>
  inner_join(age_group_lookup, by = c("AGE_GROUP_AT_GRAD" = "AgeIndex")) |>
  inner_join(
    credential_non_dup |>
      select(ID, FINAL_CIP_CLUSTER_CODE),
    by = "ID"
  ) |>
  filter(
    PSI_CREDENTIAL_CATEGORY != "Apprenticeship",
    PSI_VISA_STATUS == "Domestic" | is.na(PSI_VISA_STATUS),
    FINAL_CIP_CLUSTER_CODE != "09",
    FINAL_CIP_CLUSTER_CODE != "10"
  ) |>
  group_by(
    psi_gender_cleaned,
    AgeGroup,
    PSI_CREDENTIAL_CATEGORY,
    PSI_AWARD_SCHOOL_YEAR_DELAYED
  ) |>
  summarise(Count = n(), .groups = "drop") |>
  arrange(
    psi_gender_cleaned,
    AgeGroup,
    PSI_CREDENTIAL_CATEGORY,
    PSI_AWARD_SCHOOL_YEAR_DELAYED
  )

log_info(glue::glue(
  "Credential distribution: credential_by_year_gender_agegroup_domestic_exclude_cips: {nrow(credential_by_year_gender_agegroup_domestic_exclude_cips)} rows"
))

# Gender, AgeGroup, Domestic, Exclude RU & DACSO, Exclude CIPs
credential_by_year_gender_agegroup_domestic_exclude_ru_dacso_exclude_cips <- tbl_credential_highest_rank |>
  inner_join(age_group_lookup, by = c("AGE_GROUP_AT_GRAD" = "AgeIndex")) |>
  inner_join(
    credential_non_dup |>
      select(ID, FINAL_CIP_CLUSTER_CODE),
    by = "ID"
  ) |>
  filter(
    PSI_CREDENTIAL_CATEGORY != "Apprenticeship",
    (PSI_VISA_STATUS == "Domestic" | is.na(PSI_VISA_STATUS)),
    is.na(RESEARCH_UNIVERSITY) |
      (RESEARCH_UNIVERSITY == 1 & OUTCOMES_CRED != 'DACSO'),
    FINAL_CIP_CLUSTER_CODE != "09",
    FINAL_CIP_CLUSTER_CODE != "10"
  ) |>
  group_by(
    psi_gender_cleaned,
    AgeGroup,
    PSI_CREDENTIAL_CATEGORY,
    PSI_AWARD_SCHOOL_YEAR_DELAYED
  ) |>
  summarise(Count = n(), .groups = "drop") |>
  arrange(
    psi_gender_cleaned,
    AgeGroup,
    PSI_CREDENTIAL_CATEGORY,
    PSI_AWARD_SCHOOL_YEAR_DELAYED
  )

log_info(glue::glue(
  "Credential distribution: credential_by_year_gender_agegroup_domestic_exclude_ru_dacso_exclude_cips: {nrow(credential_by_year_gender_agegroup_domestic_exclude_ru_dacso_exclude_cips)} rows"
))

## ------------------------------Final Enrolment Distributions-----------------------------
# Reference: 01d-enrolment-analysis.R
# Replicates: qry09c_ series
#
# What the code does:
#  - Generate aggregated counts of the minimum enrolment records by various combinations of
#  gender, school year, age group, credential category and cip code.
#  - Several were created for looking at different scenarios but
#  only qry09c_MinEnrolment was used in enrolment forecasting for 2023/2024 (last model run)
## -----------------------------------------------------------------------------------------------

qry09c_minenrolment_by_credential_and_cip_code <- min_enrolment |>
  count(PSI_SCHOOL_YEAR, PSI_CREDENTIAL_CATEGORY, PSI_CIP_CODE, name = "Expr1")

log_info(glue::glue(
  "Enrolment distribution: qry09c_minenrolment_by_credential_and_cip_code: {nrow(qry09c_minenrolment_by_credential_and_cip_code)} rows"
))

# We need to bring in PSI_VISA_STATUS to min_enrolment for this  query to work.
# qry09c_MinEnrolment_Domestic <- min_enrolment |>
#   inner_join(age_group_lookup, by = c("AGE_GROUP_ENROL_DATE" = "AgeIndex")) |>
#   filter(PSI_VISA_STATUS == 'Domestic') |>
#   count(PSI_GENDER, PSI_SCHOOL_YEAR, AgeGroup, name = "Expr1")

qry09c_minenrolment <- min_enrolment |>
  inner_join(age_group_lookup, by = c("AGE_GROUP_ENROL_DATE" = "AgeIndex")) |>
  mutate("Groups" = paste0(PSI_GENDER, AgeGroup)) |>
  count(PSI_GENDER, PSI_SCHOOL_YEAR, Groups, name = "Expr1")

log_info(glue::glue(
  "Enrolment distribution: qry09c_minenrolment: {nrow(qry09c_minenrolment)} rows"
))


## ------------------------------------ Clean Up --------------------------------------------------
# Current workflow:
#  - Write key tables back to sql server.  These are tables needed for downstream work, or tables
# that might be needed for later reference outside of this analysis.
#  - Close DB connections
#  - Remove all objects at the end of each script.
## ------------------------------------------------------------------------------------------------
tables_to_keep <- c(
  'credential_by_year_age_group',
  'credential_by_year_age_group_domestic',
  'credential_by_year_age_group_domestic_exclude_cips',
  'credential_by_year_age_group_domestic_exclude_ru_dacso',
  'credential_by_year_age_group_exclude_cips',
  'credential_by_year_cip4_agegroup_domestic_exclude_ru_dacso_exclude_cips',
  'credential_by_year_cip4_gender_agegroup_domestic_exclude_ru_dacso_exclude_cips',
  'credential_by_year_gender_agegroup_domestic_exclude_cips',
  'credential_by_year_gender_agegroup_domestic_exclude_ru_dacso_exclude_cips',
  'qry09c_minenrolment_by_credential_and_cip_code',
  #'qry09c_minenrolment_domestic',
  'qry09c_minenrolment'
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

log_info("==== 01e-stp-distributions.R COMPLETE ====")


# rm(list = ls())
