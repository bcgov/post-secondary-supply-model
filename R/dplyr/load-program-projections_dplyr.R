# Load Program Projections Data — dplyr Translation
# Original: R/load-program-projections.R
#
# Pipeline context:
#   Data loader for script 06 (program-projections). Reads lookup CSVs from the LAN,
#   CIP reference tables from the database, rollover data from previous runs, and
#   builds the `tbl_Program_Projection_Input` table that 06 uses to distribute projected
#   graduates across CIP programs.
#
#   This script is sourced by the orchestrator (run_all_three_model_runs.r) and is gated
#   by the `regular_run`, `qi_run`, and `ptib_run` flags set by the prep scripts.
#
# Input:
#   - CSV lookup files from LAN (T_Cohort_Program_Distributions_Y2_to_Y12, etc.)
#   - INFOWARE_L_CIP tables from SQL Server (CIP taxonomy reference)
#   - Rollover CSVs (previous run's program distributions)
#   - Database tables: tblCredential_HighestRank, Credential_Non_Dup, AgeGroupLookup
#
# Output:
#   - Multiple lookup tables written to SQL Server
#   - tbl_Program_Projection_Input — the main input for 06-program-projections
#   - Cohort_Program_Distributions_Static/Projected — rollover data written to DB

library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(RJDBC)

# ---- Configure LAN and file paths ----
lan <- config::get("lan")
my_schema <- config::get("myschema")

# ---- Connection to decimal ----
db_config <- config::get("decimal")
decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")

# Helper: reference a table in the user's schema
sch_tbl <- function(name) {
  tbl(decimal_con, dbplyr::in_schema(my_schema, name))
}

if (regular_run == T | ptib_run == T) {

# ---- Lookups  ----
# Read CSV lookup files from LAN. These define the distribution parameters, age groups,
# credential groupings, and weights used by 06-program-projections to allocate graduates
# across programs and years.
T_Cohort_Program_Distributions_Y2_to_Y12 <-
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/06/T_Cohort_Program_Distributions_Y2_to_Y12.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")

T_APPR_Y2_to_Y10 <-
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/06/T_APPR_Y2_to_Y10.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")

tbl_Age_Groups_Near_Completers <-
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/06/tbl_Age_Groups_Near_Completers.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")

tbl_Age_Groups <-
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/07/tbl_Age_Groups.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")

T_PSSM_Projection_Cred_Grp <-
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/06/T_PSSM_Projection_Cred_Grp.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")

T_Weights_STP <-
readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/06/T_Weights_STP.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")

AgeGroupLookup <-
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/06/AgeGroupLookup.csv"),  col_types = cols(.default = col_guess()))

# ---- Read CIP reference tables from Student Outcomes database ----
# These INFOWARE tables define the official CIP taxonomy hierarchy used by
# 06-program-projections to map graduates to programs.
INFOWARE_L_CIP_4DIGITS_CIP2016 <- sch_tbl("INFOWARE_L_CIP_4DIGITS_CIP2016_raw") %>%
  collect() |> rename_with(toupper)
INFOWARE_L_CIP_6DIGITS_CIP2016 <- sch_tbl("INFOWARE_L_CIP_6DIGITS_CIP2016_raw") %>%
  collect() |> rename_with(toupper)


# ---- Write lookups to decimal ----
dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."AgeGroupLookup"')), AgeGroupLookup, overwrite = TRUE)
dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."tbl_Age_Groups_Near_Completers"')), tbl_Age_Groups_Near_Completers, overwrite = TRUE)
dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."tbl_Age_Groups"')), tbl_Age_Groups, overwrite = TRUE)
dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."T_Cohort_Program_Distributions_Y2_to_Y12"')),  T_Cohort_Program_Distributions_Y2_to_Y12, overwrite = TRUE)
dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."T_APPR_Y2_to_Y10"')),  T_APPR_Y2_to_Y10, overwrite = TRUE)
dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_4DIGITS_CIP2016"')), INFOWARE_L_CIP_4DIGITS_CIP2016, overwrite = TRUE)
dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_6DIGITS_CIP2016"')), INFOWARE_L_CIP_6DIGITS_CIP2016, overwrite = TRUE)
dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."T_PSSM_Projection_Cred_Grp"')), T_PSSM_Projection_Cred_Grp, overwrite = TRUE)
dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."T_Weights_STP"')),  T_Weights_STP, overwrite = TRUE)

# ---- Build tbl_Program_Projection_Input ----
# This table counts historical graduates by age group, credential category, CIP code,
# and school year. It feeds into 06-program-projections which uses the distribution of
# past graduates to project future program-level outputs.
#
# The filter logic selects records where:
#   - The graduate is domestic (or visa status is null) AND
#   - The institution is a research university (or that flag is null) AND
#   - The outcome is NOT DACSO (we handle DACSO separately) AND
#   - The CIP cluster is NOT 09 or 10 (excluded from projections)
# Then groups by age group, credential, year, and CIP to get counts,
# excluding apprenticeship credentials.

# Pull the three source tables needed for the join
credential_highest_rank <- sch_tbl("tblCredential_HighestRank") %>%
  collect() |> rename_with(toupper)

age_group_lookup <- sch_tbl("AgeGroupLookup") %>%
  collect() |> rename_with(toupper)

credential_non_dup <- sch_tbl("Credential_Non_Dup") %>%
  select(ID, FINAL_CIP_CLUSTER_CODE) %>%
  collect() |> rename_with(toupper)

# Select only the columns needed from each table before joining to avoid
# duplicate column names.
chr_cols <- credential_highest_rank %>%
  select(
    ID,
    AGE_GROUP_AT_GRAD,
    PSI_CREDENTIAL_CATEGORY,
    PSI_AWARD_SCHOOL_YEAR_DELAYED,
    PSI_VISA_STATUS,
    RESEARCH_UNIVERSITY,
    OUTCOMES_CRED
  )

agl_cols <- age_group_lookup %>%
  select(AGEGROUP, AGEINDEX)

# Join the three tables and apply the filter/group logic.
# WHY the filter is structured this way: SQL Server used 4 OR'd groups of AND
# conditions to handle the 2×2 matrix of (DOMESTIC|NULL) × (RESEARCH_UNIV|NULL).
# In dplyr, we simplify to two %in% checks that cover the same combinations.
tbl_Program_Projection_Input <- chr_cols %>%
  inner_join(agl_cols, by = c("AGE_GROUP_AT_GRAD" = "AGEINDEX")) %>%
  inner_join(credential_non_dup, by = c("ID" = "ID")) %>%
  filter(
    PSI_VISA_STATUS %in% c("DOMESTIC", NA),
    RESEARCH_UNIVERSITY %in% c(1, NA),
    OUTCOMES_CRED != "DACSO",
    FINAL_CIP_CLUSTER_CODE != "09",
    FINAL_CIP_CLUSTER_CODE != "10"
  ) %>%
  mutate(Expr1 = paste0(PSI_CREDENTIAL_CATEGORY, AGEGROUP)) %>%
  group_by(AGEGROUP, PSI_CREDENTIAL_CATEGORY, Expr1, FINAL_CIP_CODE_4, PSI_AWARD_SCHOOL_YEAR_DELAYED) %>%
  summarise(Count = n(), .groups = "drop") %>%
  filter(PSI_CREDENTIAL_CATEGORY != "APPRENTICESHIP") %>%
  select(AGEGROUP, PSI_CREDENTIAL_CATEGORY, Expr1, FINAL_CIP_CODE_4, PSI_AWARD_SCHOOL_YEAR_DELAYED, Count)

dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."tbl_Program_Projection_Input"')), tbl_Program_Projection_Input, overwrite = TRUE)

# ---- Rollover ----
# Load previous run's program distributions so the model can build on prior results
# rather than starting from scratch each time.
Cohort_Program_Distributions_Projected <-
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/rollover/06/Cohort_Program_Distributions_Projected.csv"), col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")

Cohort_Program_Distributions_Static <-
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/rollover/06/Cohort_Program_Distributions_Static.csv"),
                  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")

# Write rollover tables and adjust column types to NVARCHAR for string columns
# that may come in as different types from the CSV.
# KEPT AS SQL: DDL operations (ALTER TABLE)
dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."Cohort_Program_Distributions_Static"')),  Cohort_Program_Distributions_Static, overwrite = TRUE)
dbExecute(decimal_con, "DELETE FROM Cohort_Program_Distributions_Static")
dbExecute(decimal_con, "ALTER TABLE Cohort_Program_Distributions_Static ALTER COLUMN LCIP2_CRED NVARCHAR(50)")
dbExecute(decimal_con, "ALTER TABLE Cohort_Program_Distributions_Static ALTER COLUMN TTRAIN NVARCHAR(50)")
dbExecute(decimal_con, "ALTER TABLE Cohort_Program_Distributions_Static ALTER COLUMN GRAD_STATUS NVARCHAR(50)")

dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."Cohort_Program_Distributions_Projected"')),  Cohort_Program_Distributions_Projected, overwrite = TRUE)
dbExecute(decimal_con, "DELETE FROM Cohort_Program_Distributions_Projected")
dbExecute(decimal_con, "ALTER TABLE Cohort_Program_Distributions_Projected ALTER COLUMN LCIP2_CRED NVARCHAR(50)")
dbExecute(decimal_con, "ALTER TABLE Cohort_Program_Distributions_Projected ALTER COLUMN TTRAIN NVARCHAR(50)")
dbExecute(decimal_con, "ALTER TABLE Cohort_Program_Distributions_Projected ALTER COLUMN GRAD_STATUS NVARCHAR(50)")

# Validate that T_Cohorts_Recoded only contains the expected survey years.
# If this fails, something went wrong in the cohort building step (02b-1).
stopifnot(exprs = {
  dbGetQuery(decimal_con, "SELECT DISTINCT survey_year FROM T_Cohorts_Recoded")$survey_year == c(2019:2023)
})


}


if (qi_run == T) {
  # ---- Read CIP reference tables for QI run ----
  # QI runs need the CIP taxonomy but not the full set of lookups or rollover data.
  INFOWARE_L_CIP_4DIGITS_CIP2016 <- sch_tbl("INFOWARE_L_CIP_4DIGITS_CIP2016_raw") %>%
    collect() |> rename_with(toupper)
  INFOWARE_L_CIP_6DIGITS_CIP2016 <- sch_tbl("INFOWARE_L_CIP_6DIGITS_CIP2016_raw") %>%
    collect() |> rename_with(toupper)

  # ---- Write to decimal ----
  dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_4DIGITS_CIP2016"')), INFOWARE_L_CIP_4DIGITS_CIP2016, overwrite = TRUE)
  dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_6DIGITS_CIP2016"')), INFOWARE_L_CIP_6DIGITS_CIP2016, overwrite = TRUE)
}


# ---- Disconnect ----
dbDisconnect(decimal_con)
# rm(list=ls())
