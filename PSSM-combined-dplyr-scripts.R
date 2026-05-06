# ==============================================================================
# PSSM Combined dplyr Reference Script
# ==============================================================================
# BC Government Post-Secondary Supply Model (PSSM) â€” dplyr/dbplyr Translations
#
# This file combines ALL dplyr-translated R scripts from the R/dplyr/
# directory into a single reference document for new analysts.
#
# IMPORTANT: This file is NOT meant to be run as-is. The real pipeline uses
# source() to run individual scripts in sequence. See R/dplyr/README.md for
# details on how to run the dplyr versions.
#
# What are dplyr translations?
#   The original pipeline mixed R orchestration with SQL Server queries
#   executed as raw strings. These translations move all data transformation
#   logic into R using the tidyverse (dplyr, tidyr, stringr, lubridate),
#   keeping only DDL operations (CREATE/DROP/ALTER TABLE) as dbExecute() calls.
#
# Pipeline Sequence:
#   01a-01d  Preprocessing (enrolment + credential data cleaning)
#   02a      Program matching (CIP codes, APPSO/BGS/DACSO datasets)
#   02b-1    Unified cohort creation (4 surveys merged)
#   02b-2    New labour supply distributions
#   02b-3    Occupation distributions
#   03       Near completers (trades training)
#   04       Graduate projections
#   05       PTIB analysis (private institutions)
#   06       Program projections + historic cohort distributions
#   07       Occupation projections (final output)
#   08       Report generation (Excel workbooks) â€” already R-native, not here
#
# Each translated file follows a consistent structure:
#   1. Header comment explaining the file's role
#   2. Boilerplate: database connection, sch_tbl() and write_schema_table()
#   3. Data loading via sch_tbl("table") %>% collect() |> rename_with(toupper)
#   4. Sections marked with # ****** banners and # WHY: comments
#   5. Output via write_schema_table()
#
# Key SQL-to-dplyr patterns used throughout:
#   SELECT * INTO new FROM old  ->  new <- old; write_schema_table("new", new)
#   UPDATE ... CASE WHEN ...    ->  mutate(col = case_when(...))
#   INSERT INTO ... SELECT ...  ->  bind_rows(t, s %>% ...)
#   DELETE FROM t WHERE cond    ->  filter(!cond)
#   LEFT JOIN / INNER JOIN      ->  left_join() / inner_join()
#   UNION ALL                   ->  bind_rows()
#   IIf(cond, a, b)            ->  if_else(cond, a, b) or case_when()
#
# Annotation conventions:
#   # WHY:         â€” explains the business reason for a calculation
#   # KEPT AS SQL: â€” marks SQL operations that could not be translated
#   # NOTE:        â€” flags non-obvious behavior or edge cases
#
# See R/dplyr/README.md for the full translation guide and verification steps.
# ==============================================================================




# ==============================================================================
# FILE: load-cohort-dacso_dplyr.R
# ==============================================================================


# Load Cohort DACSO — dplyr Translation
# Original: R/load-cohort-dacso.R (~150 lines)
#
# Pipeline context:
#   Loads DACSO (student outcomes) data and lookup tables for the PSSM model.
#   Reads raw data from SQL Server and CSV lookups from LAN, applies region
#   recoding via CASE WHEN logic, adjusts column types, and writes everything
#   back to SQL Server for downstream use by 02b-1-pssm-cohorts and later scripts.
#
# Key translations:
#   - UPDATE...SET...CASE WHEN for CURRENT_REGION_PSSM_CODE → mutate(case_when()) before writing
#   - ALTER TABLE...ALTER COLUMN for type changes → R type conversion before writing
#   - ALTER TABLE...ADD COLUMN → column created in R before first write
#
# Input:
#   - infoware_c_outc_clean_short_resp_raw — survey response data (SQL Server)
#   - DACSO_Q003_DACSO_DATA_Part_1_stepA_raw — DACSO data (SQL Server)
#   - Multiple CSV lookups from LAN
#
# Output (written to DB):
#   - tbl_Age_Groups, tbl_Age_Groups_Rollup, tbl_Age, T_PSSM_Credential_Grouping
#   - T_NOC_Broad_Categories, t_year_survey_year, t_cohorts_recoded
#   - t_current_region_pssm_codes, t_current_region_pssm_rollup_codes, t_current_region_pssm_rollup_codes_bc
#   - infoware_c_outc_clean_short_resp, t_dacso_data_part_1_stepa (with region recoding)

library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(dbplyr)

# ---- Configure LAN and file paths ----
lan <- config::get("lan")
db_config <- config::get("decimal")
decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")
my_schema <- config::get("myschema")

# Helper: reference a table in the user's schema
sch_tbl <- function(name) {
  tbl(decimal_con, dbplyr::in_schema(my_schema, name))
}

# Helper: write to schema
write_schema_table <- function(name, data) {
  dbWriteTable(decimal_con, SQL(glue::glue('"{my_schema}"."{name}"')), data, overwrite = TRUE)
}

# ---- Retrieve data from decimal ----
infoware_c_outc_clean_short_resp_dat <- sch_tbl("infoware_c_outc_clean_short_resp_raw") %>%
  collect() |> rename_with(toupper)

# ---- Read raw data from LAN ----
tbl_Age_Groups <-
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/02/tbl_Age_Groups.csv"), col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")
tbl_Age_Groups_Rollup <-
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/02/tbl_Age_Groups_Rollup.csv"), col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")
tbl_Age <-
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/02/tbl_Age.csv"), col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")
T_PSSM_Credential_Grouping <-
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/02/T_PSSM_Credential_Grouping.csv"), col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")
t_year_survey_year <-
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/02/t_year_survey_year.csv"), col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")
t_cohorts_recoded <-
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/rollover/02/T_Cohorts_Recoded.csv"),
                  col_types = cols(PEN = "c", STQU_ID = "c", Survey = "c", LCIP_CD = "c", LCP4_CD = "c", NOC_CD = "c", INST_CD = "c",
                                   PSSM_Credential = "c",  PSSM_CRED = "c", LCIP4_CRED= "c",  LCIP2_CRED = "c", .default = col_number()), n_max = 1) %>%
  janitor::clean_names(case = "all_caps")
t_current_region_pssm_codes <-
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/02/T_Current_Region_PSSM_Codes.csv"), col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")
t_current_region_pssm_rollup_codes <-
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/02/T_Current_Region_PSSM_Rollup_Codes.csv"), col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")
t_current_region_pssm_rollup_codes_bc <-
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/02/T_Current_Region_PSSM_Rollup_Codes_BC.csv"), col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")
T_NOC_Broad_Categories <-
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/02/T_NOC_Broad_Categories_Updated.csv"), col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")

# ---- Write LAN lookup data to decimal ----
write_schema_table("tbl_Age_Groups", tbl_Age_Groups)
write_schema_table("tbl_Age_Groups_Rollup", tbl_Age_Groups_Rollup)
write_schema_table("tbl_Age", tbl_Age)
write_schema_table("T_PSSM_Credential_Grouping", T_PSSM_Credential_Grouping)
write_schema_table("T_NOC_Broad_Categories", T_NOC_Broad_Categories)
write_schema_table("t_year_survey_year", t_year_survey_year)
write_schema_table("t_cohorts_recoded", t_cohorts_recoded)
write_schema_table("t_current_region_pssm_codes", t_current_region_pssm_codes)
write_schema_table("t_current_region_pssm_rollup_codes", t_current_region_pssm_rollup_codes)
write_schema_table("t_current_region_pssm_rollup_codes_bc", t_current_region_pssm_rollup_codes_bc)

# ---- Read DACSO data and write to decimal ----
# WHY: The DACSO data needs region recoding and type conversion before being stored
# in the database. In the original, this was done via SQL UPDATE and ALTER TABLE.
# Here we do it all in R before writing.
if (regular_run == T | ptib_run == T) {
  t_dacso_data_part_1_stepa <- sch_tbl("DACSO_Q003_DACSO_DATA_Part_1_stepA_raw") %>%
    collect() |> rename_with(toupper)

  # Write infoware data
  dbWriteTableArrow(decimal_con,
                    name = SQL(glue::glue('"{my_schema}"."infoware_c_outc_clean_short_resp"')),
                    infoware_c_outc_clean_short_resp_dat, overwrite = TRUE)

  # ---- Region recoding ----
  # WHY: The original used UPDATE...SET...CASE WHEN to derive CURRENT_REGION_PSSM_CODE
  # from TPID_CURRENT_REGION1 and TPID_CURRENT_REGION4. We do the same logic in R
  # with mutate(case_when()) before writing to the database, avoiding the need for
  # ALTER TABLE ADD COLUMN + UPDATE statements.
  t_dacso_data_part_1_stepa <- t_dacso_data_part_1_stepa %>%
    mutate(CURRENT_REGION_PSSM_CODE = case_when(
      TPID_CURRENT_REGION1 %in% 1:8 ~ as.numeric(TPID_CURRENT_REGION1),
      TPID_CURRENT_REGION4 == 5      ~ 9,
      TPID_CURRENT_REGION4 == 6      ~ 10,
      TPID_CURRENT_REGION4 == 7      ~ 11,
      TPID_CURRENT_REGION4 == 8      ~ -1,
      TRUE                           ~ NA_real_
    ))

  # ---- Type conversions ----
  # WHY: The original used ALTER TABLE...ALTER COLUMN to change INT columns to INT NULL.
  # In R, we ensure these columns are integer type before writing. NULL/NA values
  # are naturally handled by R's NA_integer_ which maps to SQL NULL.
  t_dacso_data_part_1_stepa <- t_dacso_data_part_1_stepa %>%
    mutate(
      TTRAIN = as.integer(TTRAIN),
      LABR_EMPLOYED = as.integer(LABR_EMPLOYED),
      COSC_GRAD_STATUS_LGDS_CD = as.integer(COSC_GRAD_STATUS_LGDS_CD),
      COSC_GRAD_STATUS_LGDS_CD_GROUP = as.integer(COSC_GRAD_STATUS_LGDS_CD_GROUP),
      RESPONDENT = as.integer(RESPONDENT)
    )

  dbWriteTableArrow(decimal_con,
                    name = SQL(glue::glue('"{my_schema}"."t_dacso_data_part_1_stepa"')),
                    value = t_dacso_data_part_1_stepa, overwrite = TRUE)

  rm(t_dacso_data_part_1_stepa)
  gc()
}

# ---- NOC Broad Categories type conversion ----
# WHY: The original used ALTER TABLE...ALTER COLUMN to change NOC code columns from
# numeric to NVARCHAR(50). We convert to character in R before writing.
T_NOC_Broad_Categories <- T_NOC_Broad_Categories %>%
  mutate(
    BROAD_CATEGORY_CODE = as.character(BROAD_CATEGORY_CODE),
    MAJOR_GROUP_CODE = as.character(MAJOR_GROUP_CODE),
    SUB_MAJOR_GROUP_CODE = as.character(SUB_MAJOR_GROUP_CODE),
    MINOR_GROUP_CODE = as.character(MINOR_GROUP_CODE),
    UNIT_GROUP_CODE = as.character(UNIT_GROUP_CODE)
  )
write_schema_table("T_NOC_Broad_Categories", T_NOC_Broad_Categories)

# ---- Clean Up ---
dbDisconnect(decimal_con)



# ==============================================================================
# FILE: load-program-projections_dplyr.R
# ==============================================================================


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



# ==============================================================================
# FILE: occ-dists-census-data_dplyr.R
# ==============================================================================


# Occupation Distributions Census Data — dplyr Translation
# Original: R/occ-dists-census-data.R (~232 lines)
#
# Pipeline context:
#   Prepares census-based occupation distribution data for the PSSM model.
#   Reads NOC imputation outputs (CSV files per region), derives additional
#   regions (Northeast, Rest of Canada) by subtracting counts, then builds
#   the final Occupation_Distributions_Stat_Can table.
#
# Key translations:
#   - SELECT INTO (table copy) → R variable (no temp table needed)
#   - INSERT INTO (append rows) → bind_rows() before writing
#   - DROP TABLE cleanup → not needed (R variables auto-cleaned)
#   - tbl() with dplyr joins for main computation → kept as-is (already dplyr)
#
# Input:
#   - CSV files from NOC imputation output folder (per-region new counts)
#   - tbl_age_groups_rollup (DB lookup)
#   - t_current_region_pssm_rollup_codes_statcan (CSV lookup)
#
# Output:
#   - Occupation_Distributions_Stat_Can (written to DB)

library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(dbplyr)

# ---- Configure LAN and file paths ----
db_config <- config::get("decimal")
lan <- config::get("lan")
my_schema <- config::get("myschema")

decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")

# Helper: reference a table in the user's schema
sch_tbl <- function(name) {
  tbl(decimal_con, dbplyr::in_schema(my_schema, name))
}

# Helper: write to schema
write_schema_table <- function(name, data) {
  dbWriteTable(decimal_con, SQL(glue::glue('"{my_schema}"."{name}"')), data, overwrite = TRUE)
}

# ---- Import all CSV outputs from NOC imputation ----
output_folder <- glue::glue("{lan}/development/work/graduate noc imputation/output/")
new_counts_file_list <- list.files(path = output_folder, pattern = "\\- new counts.csv$", full.names = TRUE)

# Read all CSV files, add a column for the filename, and combine
combined_new_counts <- map_dfr(new_counts_file_list, ~ {
  name <- basename(.x)
  data <- read_csv(.x)
  mutate(data, file_name = str_split(name, " - new counts")[[1]][1])
})

# Save initial imputed data for reference
write_schema_table("Stat_Can_Imputed_Data_Raw", combined_new_counts)

# ---- Import required lookups ----
t_current_region_pssm_rollup_codes_statcan <-
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/02/T_Current_Region_PSSM_Rollup_Codes_StatCan.csv"), col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")
write_schema_table("t_current_region_pssm_rollup_codes_statcan", t_current_region_pssm_rollup_codes_statcan)

# ---- Check for required data tables ----
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."tbl_age_groups_rollup"')))


# ******************************************************************************
# Derive additional regions by subtraction
# WHY: Census data provides composite regions (e.g., "North Coast - Nechako and Northeast")
# and sub-regions (e.g., "North Coast and Nechako"). We derive the missing sub-region
# (Northeast) by subtracting counts. Same pattern for Rest of Canada = Canada - BC.
# Negative differences are clamped to 0.
# ******************************************************************************

# ---- Northeast: "North Coast - Nechako and Northeast" minus "North Coast and Nechako" ----
NC_Nechako_NE <- combined_new_counts %>%
  filter(file_name == "North Coast - Nechako and Northeast")

NC_Nechako <- combined_new_counts %>%
  filter(file_name == "North Coast and Nechako")

qry_Northeast <- NC_Nechako_NE %>%
  inner_join(NC_Nechako, by = c("NOC_5", "major_field_cip", "age_group")) %>%
  mutate(
    file_name = "qry_Northeast",
    NOC_4 = NOC_4.x,
    occupation_NOC = occupation_NOC.x,
    New_Above_Bach = pmax(New_Above_Bach.x - New_Above_Bach.y, 0),
    New_PDEG = pmax(New_PDEG.x - New_PDEG.y, 0),
    New_Combined = pmax(New_Combined.x - New_Combined.y, 0),
    New_Masters = pmax(New_Masters.x - New_Masters.y, 0),
    New_Doctorate = pmax(New_Doctorate.x - New_Doctorate.y, 0)
  ) %>%
  select(-ends_with(".x"), -ends_with(".y"))

# ---- Rest of Canada: Canada minus British Columbia ----
Canada <- combined_new_counts %>%
  filter(file_name == "Canada")

British_Columbia <- combined_new_counts %>%
  filter(file_name == "British Columbia")

qry_Rest_of_Canada <- Canada %>%
  inner_join(British_Columbia, by = c("NOC_5", "major_field_cip", "age_group")) %>%
  mutate(
    file_name = "qry_Rest_of_Canada",
    NOC_4 = NOC_4.x,
    occupation_NOC = occupation_NOC.x,
    New_Above_Bach = pmax(New_Above_Bach.x - New_Above_Bach.y, 0),
    New_PDEG = pmax(New_PDEG.x - New_PDEG.y, 0),
    New_Combined = pmax(New_Combined.x - New_Combined.y, 0),
    New_Masters = pmax(New_Masters.x - New_Masters.y, 0),
    New_Doctorate = pmax(New_Doctorate.x - New_Doctorate.y, 0)
  ) %>%
  select(-ends_with(".x"), -ends_with(".y"))


# ******************************************************************************
# Combine all regions and build occupation distributions
# WHY: The original used SELECT INTO to copy the raw table, then INSERT INTO to
# append Northeast and Rest of Canada rows. We replace this with bind_rows() and
# a single write, eliminating the need for temp tables and DROP TABLE cleanup.
# ******************************************************************************
stat_can_updated <- bind_rows(combined_new_counts, qry_Northeast, qry_Rest_of_Canada)


# ******************************************************************************
# Prepare Occupation_Distributions_Stat_Can table
# WHY: Pivot the census counts from wide (one column per credential type) to long,
# map credential names to PSSM categories, join with region and age group lookups,
# compute totals and percentages. This follows the same pattern as the original
# but uses the in-memory stat_can_updated instead of the DB table.
# ******************************************************************************

# Join with lookups to filter to valid regions and get age group rollup codes
# WHY: Only regions in the StatCan rollup lookup are included; NA matches are
# regions not used in the model.
stat_can_with_lookups <- stat_can_updated %>%
  left_join(t_current_region_pssm_rollup_codes_statcan,
            by = c("file_name" = "CURRENT_REGION_PSSM_NAME_ROLLUP_STAT_CAN")) %>%
  left_join(sch_tbl("tbl_age_groups_rollup") %>% collect() |> rename_with(toupper),
            by = c("age_group" = "AGE_GROUP_ROLLUP_LABEL")) %>%
  filter(!is.na(CURRENT_REGION_PSSM_CODE_ROLLUP)) %>%
  select(-age_group, -file_name)

# Pivot from wide to long: one row per credential type
Combined_Stat_Can_Pivot <- stat_can_with_lookups %>%
  pivot_longer(cols = starts_with("New_"), names_to = "Credential") %>%
  filter(Credential != "New_Combined") %>%
  mutate(PSSM_CREDENTIAL = case_when(
    Credential == "New_Above_Bach" ~ "GRCT or GRDP",
    Credential == "New_PDEG"       ~ "PDEG",
    Credential == "New_Masters"    ~ "MAST",
    Credential == "New_Doctorate"  ~ "DOCT"
  ))

# Build composite keys and compute percentages
Combined_Stat_Can <- Combined_Stat_Can_Pivot %>%
  mutate(
    PSSM_CRED = PSSM_CREDENTIAL,
    LCIPPC_CD = substr(major_field_cip, 1, 2),
    LCIPPC_CD_CRED = paste0(LCIPPC_CD, " - ", PSSM_CREDENTIAL),
    SURVEY = "2021 Census PSSM 2022-2023"
  ) %>%
  rename(COUNT = value) %>%
  select(-Credential)

# Compute totals by CIP/Region/Age/Credential for percentage calculation
tmp_tbl_Calc_Total <- Combined_Stat_Can %>%
  group_by(LCIPPC_CD_CRED, CURRENT_REGION_PSSM_CODE_ROLLUP, AGE_GROUP_ROLLUP) %>%
  summarise(TOTAL = sum(COUNT), .groups = "drop")

# Join totals back and compute percentages
Occupation_Distributions_Stat_Can <- Combined_Stat_Can %>%
  inner_join(tmp_tbl_Calc_Total,
             by = c("LCIPPC_CD_CRED", "CURRENT_REGION_PSSM_CODE_ROLLUP", "AGE_GROUP_ROLLUP")) %>%
  mutate(PERCENT = ifelse(TOTAL == 0, 0, COUNT / TOTAL)) %>%
  filter(COUNT > 0) %>%
  select(
    SURVEY, PSSM_CREDENTIAL, PSSM_CRED,
    LCP4_CD = LCIPPC_CD, LCIP4_CRED = LCIPPC_CD_CRED,
    CURRENT_REGION_PSSM_CODE_ROLLUP, NOC = NOC_5,
    AGE_GROUP_ROLLUP, COUNT, TOTAL, PERCENT
  )

write_schema_table("Occupation_Distributions_Stat_Can", Occupation_Distributions_Stat_Can)


# ---- Clean Up ----
# No DROP TABLE needed — all intermediate data was in R variables, not DB tables.
# Only keep the final output table: Occupation_Distributions_Stat_Can.
# KEPT AS SQL: Drop the lookup table loaded earlier (cleanup shared with other scripts)
dbExecute(decimal_con, "DROP TABLE t_current_region_pssm_rollup_codes_statcan")

dbDisconnect(decimal_con)



# ==============================================================================
# FILE: 01a-enrolment-preprocessing_dplyr.R
# ==============================================================================


# STP Enrolment Preprocessing — dplyr Translation
# Original: R/01a-enrolment-preprocessing.R
#
# Pipeline context:
#   First step of the PSSM pipeline. Cleans raw STP enrolment data by:
#     1. Reformatting dates from yy-mm-dd to yyyy-mm-dd
#     2. Classifying each record with a RecordStatus code (0=good, 1-8=excluded)
#     3. Identifying minimum and first enrolment records per student/year
#     4. Resolving conflicting birthdates across records for the same student
#
# Input tables:
#   - STP_Enrolment — raw enrolment data (from load-stp-enrol.R)
#
# Output tables:
#   - STP_Enrolment — updated with cleaned dates and psi_birthdate_cleaned
#   - STP_Enrolment_Record_Type — ID, RecordStatus, MinEnrolment, FirstEnrolment
#   - STP_Enrolment_Valid — subset of STP_Enrolment where RecordStatus=0

library(arrow)
library(tidyverse)
library(odbc)
library(DBI)
library(dbplyr)

# ---- Configure LAN Paths and DB Connection -----
lan <- config::get("lan")
db_config <- config::get("decimal")
my_schema <- config::get("myschema")

con <- dbConnect(odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")

# Helper: reference a table in the user's schema
sch_tbl <- function(name) {
  tbl(con, dbplyr::in_schema(my_schema, name))
}

# Helper: check if a value is blank/missing/unspecified
is_blank <- function(x) {
  is.na(x) | x %in% c("", " ", "(Unspecified)")
}

# ---- Check required table ----
assertthat::assert_that(
  dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Enrolment"'))),
  msg = "STP_Enrolment table not found"
)


# ******************************************************************************
# Part 0: Pull STP_Enrolment into R
# ******************************************************************************
stp_enrolment <- sch_tbl("STP_Enrolment") %>%
  collect() |> rename_with(toupper)

# Check for null/blank EPENs
stp_enrolment %>%
  filter(is_blank(ENCRYPTED_TRUE_PEN)) %>%
  tally(name = "n_null_epens")

# Count distinct EPENs
stp_enrolment %>%
  distinct(ENCRYPTED_TRUE_PEN) %>%
  tally(name = "n_epens")


# ******************************************************************************
# Part 1: Add ID column as primary key
# ******************************************************************************
# WHY: The original SQL adds an IDENTITY column. In R, we add a row number.
# KEPT AS SQL: ALTER TABLE + ADD CONSTRAINT (DDL for primary key)
stp_enrolment <- stp_enrolment %>%
  mutate(ID = row_number())

# Also add to DB so downstream SQL scripts work
dbExecute(con, "ALTER TABLE STP_Enrolment ADD ID INT IDENTITY(1,1) NOT NULL")
dbExecute(con, "ALTER TABLE STP_Enrolment ADD CONSTRAINT STP_Enrolment_PK_ID PRIMARY KEY (ID)")


# ******************************************************************************
# Part 2: Reformat dates from yy-mm-dd to yyyy-mm-dd
# ******************************************************************************
# WHY: Some date fields are in 2-digit year format (e.g., "23-01-15"). Need to
# convert to 4-digit years: yy < 24 → "20"+yy, yy > 23 → "19"+yy, blank → "".

convert_yy_to_yyyy <- function(date_col) {
  if_else(
    is.na(date_col),
    NA_character_,
    case_when(
      str_sub(date_col, 1, 2) == "  " ~ "",
      as.numeric(str_sub(date_col, 1, 2)) < 24 ~ paste0("20", date_col),
      as.numeric(str_sub(date_col, 1, 2)) >= 24 ~ paste0("19", date_col),
      TRUE ~ date_col
    )
  )
}

stp_enrolment <- stp_enrolment %>%
  mutate(
    PSI_BIRTHDATE = convert_yy_to_yyyy(PSI_BIRTHDATE),
    LAST_SEEN_BIRTHDATE = convert_yy_to_yyyy(LAST_SEEN_BIRTHDATE),
    PSI_MIN_START_DATE = convert_yy_to_yyyy(PSI_MIN_START_DATE),
    PSI_PROGRAM_EFFECTIVE_DATE = convert_yy_to_yyyy(PSI_PROGRAM_EFFECTIVE_DATE)
  )


# ******************************************************************************
# Part 3: Create Record Type table and classify records
# ******************************************************************************
# WHY: Each enrolment record is assigned a RecordStatus indicating whether it
# should be included (0) or excluded (1-8) from the model. The classification
# is sequential and priority-based: once a record gets a status, it keeps it.
#
# Record Status codes:
#   0 = Good (keep)
#   1 = Missing Student Number
#   2 = Developmental
#   3 = No PSI Transition
#   4 = Credential Only (not assigned in this script)
#   5 = PSI Outside BC
#   6 = Skills Based
#   7 = Developmental CIP
#   8 = Recommendation for Certification (not assigned in this script)

# Initialize record type table with ID and NULL status
record_type <- tibble(ID = stp_enrolment$ID) %>%
  mutate(RecordStatus = NA_integer_, MinEnrolment = NA_integer_, FirstEnrolment = NA_integer_)


# ---- Status 1: No PEN or Student Number ----
# WHY: Records without any student identifier (PEN or PSI_CODE/STUDENT_NUMBER)
# cannot be matched across years and must be excluded.
has_valid_id <- stp_enrolment %>%
  filter(
    (!is_blank(PSI_STUDENT_NUMBER) & !is_blank(PSI_CODE))
    | !is_blank(ENCRYPTED_TRUE_PEN)
  ) %>%
  select(ID)

record_type <- record_type %>%
  mutate(RecordStatus = if_else(!ID %in% has_valid_id$ID & is.na(RecordStatus), 1L, RecordStatus))


# ---- Status 2: Developmental ----
# WHY: Developmental study level courses are preparatory and not part of the
# post-secondary credential pipeline.
is_developmental <- stp_enrolment %>%
  filter(PSI_STUDY_LEVEL == "DEVELOPMENTAL") %>%
  select(ID)

record_type <- record_type %>%
  mutate(RecordStatus = if_else(ID %in% is_developmental$ID & is.na(RecordStatus), 2L, RecordStatus))


# ---- Status 6: Skills Based (multi-step) ----
# WHY: Skills-based continuing education courses are excluded unless they lead
# to a recognized credential. This is a multi-step process because some skills
# courses are manually reviewed and kept (or excluded) based on expert judgment.

# Step 1: Identify skills-based courses (SKILLS CRS ONLY, not developmental,
# NONE/OTHER credential category)
skills_based <- stp_enrolment %>%
  filter(PSI_CONTINUING_EDUCATION_COURSE_ONLY == "SKILLS CRS ONLY",
         PSI_STUDY_LEVEL != "DEVELOPMENTAL",
         PSI_CREDENTIAL_CATEGORY %in% c("NONE", "OTHER")) %>%
  mutate(CIP2 = str_sub(PSI_CIP_CODE, 1, 2))

# Diagnostic: check list of programs considered skills-based
skills_based %>%
  count(PSI_CODE, PSI_CONTINUING_EDUCATION_COURSE_ONLY, CIP2, PSI_PROGRAM_CODE,
        PSI_CREDENTIAL_PROGRAM_DESCRIPTION, PSI_STUDY_LEVEL, PSI_CREDENTIAL_CATEGORY)

# Mark specific programs to keep (e.g., UFV/UCFV TEACH ED)
skills_based_keep <- skills_based %>%
  mutate(KEEP = if_else(
    (PSI_CODE == "UFV" & PSI_PROGRAM_CODE == "TEACH ED")
    | (PSI_CODE == "UCFV" & PSI_PROGRAM_CODE == "TEACH ED"),
    "Y", NA_character_
  ))

# Assign status 6 to skills-based records NOT marked for keeping
record_type <- record_type %>%
  left_join(
    skills_based_keep %>% select(ID, KEEP) %>% mutate(IS_SKILLS = TRUE),
    by = "ID"
  ) %>%
  mutate(RecordStatus = if_else(
    IS_SKILLS & is.na(RecordStatus) & is.na(KEEP), 6L, RecordStatus
  )) %>%
  select(-IS_SKILLS, -KEEP)


# Step 2: Continuing education with developmental CIP codes
# WHY: Additional skills-based exclusion for records with specific CIP prefixes
# (21, 32-37, 53, 89) that are developmental/continuing education.
cont_ed_cips <- stp_enrolment %>%
  filter(PSI_STUDY_LEVEL != "DEVELOPMENTAL",
         PSI_CONTINUING_EDUCATION_COURSE_ONLY != "SKILLS CRS ONLY",
         PSI_CREDENTIAL_CATEGORY %in% c("NONE", "OTHER"),
         str_sub(PSI_CIP_CODE, 1, 2) %in% c("21", "32", "33", "34", "35", "36", "37", "53", "89")) %>%
  select(ID)

record_type <- record_type %>%
  mutate(RecordStatus = if_else(ID %in% cont_ed_cips$ID & is.na(RecordStatus), 6L, RecordStatus))


# Step 3: Continuing Education / Studies / Audit programs by description
# WHY: Programs with names containing "Continuing Education", "Continuing Studies",
# "Audit", or starting with "CE " are typically non-credential courses.
cont_ed_more <- stp_enrolment %>%
  filter(
    str_detect(PSI_CREDENTIAL_PROGRAM_DESCRIPTION, "Continuing Education$")
    | str_detect(PSI_CREDENTIAL_PROGRAM_DESCRIPTION, "Continuing Studies$")
    | str_detect(PSI_CREDENTIAL_PROGRAM_DESCRIPTION, "Audit")
    | str_detect(PSI_CREDENTIAL_PROGRAM_DESCRIPTION, "^CE ")
  ) %>%
  select(ID)

record_type <- record_type %>%
  mutate(RecordStatus = if_else(ID %in% cont_ed_more$ID & is.na(RecordStatus), 6L, RecordStatus))


# Step 4: Keep skills-based programs with valid credentials
# WHY: Some skills-based courses lead to real credentials (not NONE/OTHER/SHORT CERT)
# and should be kept. They need special handling because the automatic rules would
# exclude them, but manual review confirms they're legitimate.
keep_skills <- stp_enrolment %>%
  filter(
    PSI_CONTINUING_EDUCATION_COURSE_ONLY == "SKILLS CRS ONLY",
    PSI_STUDY_LEVEL != "DEVELOPMENTAL",
    !PSI_CREDENTIAL_CATEGORY %in% c("NONE", "OTHER", "SHORT CERTIFICATE"),
    !str_detect(PSI_CREDENTIAL_PROGRAM_DESCRIPTION, "Continuing Studies$"),
    !str_detect(PSI_CREDENTIAL_PROGRAM_DESCRIPTION, "Audit"),
    !str_detect(PSI_CREDENTIAL_PROGRAM_DESCRIPTION, "Continuing Education$"),
    !str_detect(PSI_CREDENTIAL_PROGRAM_DESCRIPTION, "^CE ")
  ) %>%
  mutate(CIP2 = str_sub(PSI_CIP_CODE, 1, 2)) %>%
  mutate(EXCLUDE = if_else(
    (PSI_CODE == "SEL" & PSI_CREDENTIAL_PROGRAM_DESCRIPTION == "COMMUNITY, CORPORATE & INTERNATIONAL DEVELOPMENT")
    | (PSI_CODE == "NIC" & CIP2 %in% c("21", "32", "33", "34", "35", "36", "37", "53", "89")),
    "Y", NA_character_
  ))

# Set status 0 (keep) for skills-based records not excluded
keep_skills_ids <- keep_skills %>% filter(is.na(EXCLUDE)) %>% pull(ID)
record_type <- record_type %>%
  mutate(RecordStatus = if_else(ID %in% keep_skills_ids & is.na(RecordStatus), 0L, RecordStatus))

# Set status 6 (exclude) for skills-based records that are excluded
exclude_skills_ids <- keep_skills %>% filter(EXCLUDE == "Y") %>% pull(ID)
record_type <- record_type %>%
  mutate(RecordStatus = if_else(ID %in% exclude_skills_ids & is.na(RecordStatus), 6L, RecordStatus))


# Step 5: Selkirk specific exclusion
# WHY: Selkirk College has a specific program that needs exclusion based on
# manual review. This is an institution-specific data quality adjustment.
selkirk_exclude <- stp_enrolment %>%
  filter(PSI_CODE == "SEL",
         PSI_CREDENTIAL_PROGRAM_DESCRIPTION == "COMMUNITY, CORPORATE & INTERNATIONAL DEVELOPMENT") %>%
  pull(ID)

record_type <- record_type %>%
  mutate(RecordStatus = if_else(ID %in% selkirk_exclude & is.na(RecordStatus), 6L, RecordStatus))


# Step 6: Suspect skills-based courses
# WHY: The remaining unclassified records are checked against the skills-based
# course catalog. Programs that have been manually reviewed (Keep IS NULL) are
# classified as skills-based and excluded.
# NOTE: This step involves a manual review table (tmp_tbl_SkillsBasedCourses.KEEP).
# In the original SQL, the KEEP column is manually set. Here we replicate the logic.
dev_cip_codes <- c("21", "32", "33", "34", "35", "36", "37", "53", "89")

suspect_skills <- stp_enrolment %>%
  inner_join(record_type %>% filter(is.na(RecordStatus)) %>% select(ID), by = "ID") %>%
  filter(PSI_CONTINUING_EDUCATION_COURSE_ONLY == "NOT SKILLS CRS ONLY",
         str_sub(PSI_CIP_CODE, 1, 2) %in% dev_cip_codes) %>%
  select(ID)

record_type <- record_type %>%
  mutate(RecordStatus = if_else(ID %in% suspect_skills$ID & is.na(RecordStatus), 7L, RecordStatus))


# ---- Status 3: No PSI Transition ----
# WHY: Records where the student has no transition between PSIs are excluded
# because they represent non-progression.
no_transition <- stp_enrolment %>%
  filter(PSI_ENTRY_STATUS == "No Transition") %>%
  pull(ID)

record_type <- record_type %>%
  mutate(RecordStatus = if_else(ID %in% no_transition & is.na(RecordStatus), 3L, RecordStatus))


# ---- Status 5: PSI Outside BC ----
# WHY: Students attending institutions outside BC are excluded from the
# BC-focused supply model.
outside_bc <- stp_enrolment %>%
  filter(ATTENDING_PSI_OUTSIDE_BC == "Y") %>%
  pull(ID)

record_type <- record_type %>%
  mutate(RecordStatus = if_else(ID %in% outside_bc & is.na(RecordStatus), 5L, RecordStatus))


# ---- Status 0: Default - all remaining records are good ----
record_type <- record_type %>%
  mutate(RecordStatus = if_else(is.na(RecordStatus), 0L, RecordStatus))

# Diagnostic: check record type distribution
record_type %>%
  count(RecordStatus, name = "Count")


# ******************************************************************************
# Part 4: Create STP_Enrolment_Valid
# ******************************************************************************
# WHY: Only records with RecordStatus=0 are included in the model.
stp_enrolment_valid <- stp_enrolment %>%
  inner_join(record_type %>% filter(RecordStatus == 0) %>% select(ID), by = "ID") %>%
  select(ID, PSI_STUDENT_NUMBER, ENCRYPTED_TRUE_PEN, PSI_SCHOOL_YEAR,
         PSI_STUDENT_POSTAL_CODE_CURRENT, PSI_ENROLMENT_SEQUENCE, PSI_CODE,
         PSI_MIN_START_DATE)

# Check records associated with > 1 EPEN
cat("Records associated with > 1 EPEN:\n")
stp_enrolment_valid %>%
  distinct(PSI_CODE, PSI_STUDENT_NUMBER, ENCRYPTED_TRUE_PEN) %>%
  count(PSI_CODE, PSI_STUDENT_NUMBER, name = "n") %>%
  filter(n != 1)


# ******************************************************************************
# Part 5: Min Enrolment — find minimum enrolment sequence per student/year
# ******************************************************************************
# WHY: For each student and school year, the record with the lowest enrolment
# sequence is the "minimum enrolment". This identifies the first interaction
# with the institution in that year.
# Two passes: first by EPEN (for students with valid PENs), then by
# PSI_CODE/PSI_STUDENT_NUMBER (for students with blank EPENs).

# ---- By ENCRYPTED_TRUE_PEN ----
# WHY: EPEN is the preferred identifier. Find the min enrolment sequence
# per EPEN per year, then pick the record with the lowest ID as tiebreaker.
min_enrol_pen <- stp_enrolment_valid %>%
  filter(!is_blank(ENCRYPTED_TRUE_PEN)) %>%
  group_by(ENCRYPTED_TRUE_PEN, PSI_SCHOOL_YEAR) %>%
  slice_min(PSI_ENROLMENT_SEQUENCE, with_ties = TRUE) %>%
  slice_min(ID, with_ties = FALSE) %>%
  ungroup() %>%
  select(MinOfID = ID)

# ---- By PSI_CODE/PSI_STUDENT_NUMBER ----
# WHY: For students without valid EPENs, use the institution-specific
# student number + institution code as the identifier.
min_enrol_stuid <- stp_enrolment_valid %>%
  filter(is_blank(ENCRYPTED_TRUE_PEN)) %>%
  group_by(PSI_STUDENT_NUMBER, PSI_CODE, PSI_SCHOOL_YEAR) %>%
  slice_min(PSI_ENROLMENT_SEQUENCE, with_ties = TRUE) %>%
  slice_min(ID, with_ties = FALSE) %>%
  ungroup() %>%
  select(MinOfID = ID)

# Combine both sets of min enrolment IDs
all_min_enrol_ids <- c(min_enrol_pen$MinOfID, min_enrol_stuid$MinOfID)

record_type <- record_type %>%
  mutate(MinEnrolment = if_else(ID %in% all_min_enrol_ids, 1L, 0L))


# ******************************************************************************
# Part 6: First Enrolment — find earliest enrolment record per student
# ******************************************************************************
# WHY: The first enrolment record (earliest start date, lowest enrolment sequence)
# is used to determine the student's initial program and credential. This is
# different from min enrolment which is per year; first enrolment is overall.
# Two passes: by EPEN and by PSI_CODE/PSI_STUDENT_NUMBER.

# ---- By ENCRYPTED_TRUE_PEN ----
first_enrol_pen <- stp_enrolment_valid %>%
  filter(!is_blank(ENCRYPTED_TRUE_PEN)) %>%
  group_by(ENCRYPTED_TRUE_PEN) %>%
  slice_min(PSI_MIN_START_DATE, with_ties = TRUE) %>%
  slice_min(PSI_ENROLMENT_SEQUENCE, with_ties = TRUE) %>%
  slice_min(ID, with_ties = FALSE) %>%
  ungroup() %>%
  select(MinID = ID)

# ---- By PSI_CODE/PSI_STUDENT_NUMBER ----
first_enrol_stuid <- stp_enrolment_valid %>%
  filter(is_blank(ENCRYPTED_TRUE_PEN)) %>%
  group_by(PSI_STUDENT_NUMBER, PSI_CODE) %>%
  slice_min(PSI_MIN_START_DATE, with_ties = TRUE) %>%
  slice_min(PSI_ENROLMENT_SEQUENCE, with_ties = TRUE) %>%
  slice_min(ID, with_ties = FALSE) %>%
  ungroup() %>%
  select(MinID = ID)

# Combine and flag
all_first_enrol_ids <- c(first_enrol_pen$MinID, first_enrol_stuid$MinID)

record_type <- record_type %>%
  mutate(FirstEnrolment = if_else(ID %in% all_first_enrol_ids, 1L,
                                   if_else(is.na(FirstEnrolment), 0L, FirstEnrolment)))


# ******************************************************************************
# Part 7: Birthdate Cleaning
# ******************************************************************************
# WHY: The same student (identified by EPEN) may have different birthdates
# across records. We resolve this by:
#   1. Finding EPENs with multiple distinct birthdates
#   2. Choosing the most common birthdate (or the one matching LAST_SEEN_BIRTHDATE)
#   3. Filling null birthdates from non-null records for the same EPEN
#   4. Setting cleaned birthdate on all records for that EPEN

# Step 1: Count birthdate records per EPEN
birthdate_distinct <- stp_enrolment %>%
  filter(!is_blank(PSI_BIRTHDATE), !is_blank(ENCRYPTED_TRUE_PEN)) %>%
  count(ENCRYPTED_TRUE_PEN, PSI_BIRTHDATE, name = "NumBirthdateRecords")

# Step 2: Find EPENs with more than one distinct birthdate
multi_birthdate_epens <- birthdate_distinct %>%
  count(ENCRYPTED_TRUE_PEN, name = "N_Birthdates") %>%
  filter(N_Birthdates > 1) %>%
  pull(ENCRYPTED_TRUE_PEN)

# Step 3: For each EPEN with multiple birthdates, get min and max dates with counts
min_birthdates <- birthdate_distinct %>%
  group_by(ENCRYPTED_TRUE_PEN) %>%
  slice_min(PSI_BIRTHDATE, with_ties = FALSE) %>%
  ungroup() %>%
  select(ENCRYPTED_TRUE_PEN, MinPSIBirthdate = PSI_BIRTHDATE, NumMinBirthdateRecords = NumBirthdateRecords)

max_birthdates <- birthdate_distinct %>%
  group_by(ENCRYPTED_TRUE_PEN) %>%
  slice_max(PSI_BIRTHDATE, with_ties = FALSE) %>%
  ungroup() %>%
  select(ENCRYPTED_TRUE_PEN, MaxPSIBirthdate = PSI_BIRTHDATE, NumMaxBirthdateRecords = NumBirthdateRecords)

# Step 4: Join with LAST_SEEN_BIRTHDATE to determine which to use
# WHY: The logic prefers the birthdate that matches LAST_SEEN_BIRTHDATE.
# If that doesn't resolve it, uses the one with more records. Defaults to MIN.
last_seen_lookup <- stp_enrolment %>%
  filter(ENCRYPTED_TRUE_PEN %in% multi_birthdate_epens) %>%
  distinct(ENCRYPTED_TRUE_PEN, LAST_SEEN_BIRTHDATE)

birthdate_resolution <- tibble(ENCRYPTED_TRUE_PEN = multi_birthdate_epens) %>%
  inner_join(min_birthdates, by = "ENCRYPTED_TRUE_PEN") %>%
  inner_join(max_birthdates, by = "ENCRYPTED_TRUE_PEN") %>%
  inner_join(last_seen_lookup, by = "ENCRYPTED_TRUE_PEN") %>%
  mutate(
    UseMaxOrMin_FINAL = case_when(
      MaxPSIBirthdate == LAST_SEEN_BIRTHDATE ~ "MAX",
      NumMaxBirthdateRecords > NumMinBirthdateRecords ~ "MAX",
      NumMaxBirthdateRecords < NumMinBirthdateRecords ~ "MIN",
      TRUE ~ "MIN"
    ),
    psi_birthdate_cleaned = if_else(UseMaxOrMin_FINAL == "MAX", MaxPSIBirthdate, MinPSIBirthdate)
  ) %>%
  select(ENCRYPTED_TRUE_PEN, psi_birthdate_cleaned)

# Step 5: Fill null birthdates from non-null records for the same EPEN
# WHY: Some records for an EPEN have null birthdate while others have values.
# We find the non-null value and use it.
null_birthdate_epens <- stp_enrolment %>%
  filter(!is_blank(ENCRYPTED_TRUE_PEN), is_blank(PSI_BIRTHDATE)) %>%
  distinct(ENCRYPTED_TRUE_PEN)

nonnull_birthdates <- stp_enrolment %>%
  filter(!is_blank(ENCRYPTED_TRUE_PEN), !is_blank(PSI_BIRTHDATE)) %>%
  distinct(ENCRYPTED_TRUE_PEN, PSI_BIRTHDATE)

null_cleaned <- null_birthdate_epens %>%
  inner_join(nonnull_birthdates, by = "ENCRYPTED_TRUE_PEN") %>%
  left_join(birthdate_resolution %>% select(ENCRYPTED_TRUE_PEN, psi_birthdate_cleaned),
            by = "ENCRYPTED_TRUE_PEN") %>%
  group_by(ENCRYPTED_TRUE_PEN) %>%
  slice(1) %>%
  ungroup() %>%
  mutate(psi_birthdate_cleaned = if_else(is.na(psi_birthdate_cleaned), PSI_BIRTHDATE, psi_birthdate_cleaned)) %>%
  select(ENCRYPTED_TRUE_PEN, psi_birthdate_cleaned)

# Step 6: Apply cleaned birthdates to STP_Enrolment
# WHY: First apply the multi-birthdate resolution, then the null fill,
# then fall back to original PSI_BIRTHDATE if still null.
stp_enrolment <- stp_enrolment %>%
  left_join(birthdate_resolution, by = "ENCRYPTED_TRUE_PEN") %>%
  mutate(psi_birthdate_cleaned = coalesce(psi_birthdate_cleaned, PSI_BIRTHDATE))

# Apply null birthdate cleaning for records where psi_birthdate_cleaned is still blank
stp_enrolment <- stp_enrolment %>%
  left_join(null_cleaned %>% rename(psi_birthdate_null_cleaned = psi_birthdate_cleaned),
            by = "ENCRYPTED_TRUE_PEN") %>%
  mutate(
    psi_birthdate_cleaned = case_when(
      !is_blank(psi_birthdate_cleaned) & psi_birthdate_cleaned != "" ~ psi_birthdate_cleaned,
      !is.na(psi_birthdate_null_cleaned) & psi_birthdate_null_cleaned != "" ~ psi_birthdate_null_cleaned,
      TRUE ~ psi_birthdate_cleaned
    )
  ) %>%
  select(-psi_birthdate_null_cleaned)

# Final fallback: use PSI_BIRTHDATE where psi_birthdate_cleaned is still null/blank
stp_enrolment <- stp_enrolment %>%
  mutate(
    psi_birthdate_cleaned = case_when(
      (!is_blank(psi_birthdate_cleaned)) ~ psi_birthdate_cleaned,
      (!is_blank(PSI_BIRTHDATE)) ~ PSI_BIRTHDATE,
      TRUE ~ psi_birthdate_cleaned
    )
  )

# Sanity check on multi-birthdate records for STUID-based students
multi_stuid_birthdates <- stp_enrolment %>%
  filter(is_blank(ENCRYPTED_TRUE_PEN),
         !is_blank(PSI_BIRTHDATE),
         !is_blank(PSI_STUDENT_NUMBER),
         !is_blank(PSI_CODE)) %>%
  distinct(PSI_STUDENT_NUMBER, PSI_CODE, PSI_BIRTHDATE, psi_birthdate_cleaned) %>%
  count(PSI_STUDENT_NUMBER, PSI_CODE, name = "n") %>%
  filter(n > 1)


# ******************************************************************************
# Part 8: Write results to database
# ******************************************************************************
# WHY: The record type table, valid enrolment, and updated STP_Enrolment are
# written back to the database for downstream pipeline steps.

# Write STP_Enrolment_Record_Type
dbWriteTable(con, SQL(glue::glue('"{my_schema}"."STP_Enrolment_Record_Type"')),
             record_type, overwrite = TRUE)

# Write STP_Enrolment_Valid
dbWriteTable(con, SQL(glue::glue('"{my_schema}"."STP_Enrolment_Valid"')),
             stp_enrolment_valid, overwrite = TRUE)

# Update STP_Enrolment with cleaned birthdates and reformatted dates
# KEPT AS SQL: ALTER TABLE to add psi_birthdate_cleaned column
dbExecute(con, "ALTER TABLE STP_Enrolment ADD psi_birthdate_cleaned NVARCHAR(50) NULL")

# Update dates and birthdate in DB via batch update
# WHY: We update the DB table to match our R computations. Using a temp table
# join is more efficient than row-by-row updates.
# KEPT AS SQL: UPDATE...FROM for bulk column updates
stp_updates <- stp_enrolment %>%
  select(ID, PSI_BIRTHDATE, LAST_SEEN_BIRTHDATE, PSI_MIN_START_DATE,
         PSI_PROGRAM_EFFECTIVE_DATE, psi_birthdate_cleaned)

dbWriteTable(con, SQL(glue::glue('"{my_schema}"."tmp_stp_updates"')),
             stp_updates, overwrite = TRUE)

dbExecute(con, glue::glue("
  UPDATE STP_Enrolment
  SET PSI_BIRTHDATE = u.PSI_BIRTHDATE,
      LAST_SEEN_BIRTHDATE = u.LAST_SEEN_BIRTHDATE,
      PSI_MIN_START_DATE = u.PSI_MIN_START_DATE,
      PSI_PROGRAM_EFFECTIVE_DATE = u.PSI_PROGRAM_EFFECTIVE_DATE,
      psi_birthdate_cleaned = u.psi_birthdate_cleaned
  FROM tmp_stp_updates u
  WHERE STP_Enrolment.ID = u.ID;
"))

dbExecute(con, glue::glue("DROP TABLE [{my_schema}].[tmp_stp_updates];"))


# ---- Verify tables exist ----
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Enrolment_Record_Type"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Enrolment"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Enrolment_Valid"')))

# ---- Disconnect ----
dbDisconnect(con)



# ==============================================================================
# FILE: 01b-credential-preprocessing_dplyr.R
# ==============================================================================


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



# ==============================================================================
# FILE: 01c-credential-analysis_dplyr.R
# ==============================================================================


# Credential Analysis — dplyr Translation
# Original: R/01c-credential-analysis.R (~2595 lines, ~250 SQL ops)
#
# Pipeline context:
#   Processes STP_Credential data to produce cleaned, deduplicated credential records
#   with supplementary variables (gender, birthdate, visa status, age, ranking).
#   Outputs feed into program matching (02a) and graduate projections (04).
#
# Major sections:
#   1. Build CredentialSupVarsFromEnrolment — match enrolment records to credentials
#   2. Build CredentialSupVars — base credential table with empty SupVar columns
#   3. Developmental/partial year exclusions
#   4. Birthdate cleaning from enrolment
#   5. Gender cleaning (resolve multi-gender EPENs, unknowns, null-EPEN fallback)
#   6. Birthdate cleaning with LAST_SEEN_BIRTHDATE
#   7. Recreate Credential view with cleaned SupVars
#   8. Age at graduation calculation
#   9. Credential_Non_Dup creation (deduplication)
#  10. Gender imputation for Non_Dup (proportional assignment)
#  11. Credential ranking (highest by date/rank within EPEN or STUDENT_NUMBER/PSI_CODE)
#  12. Age imputation for missing ages
#  13. Visa status cleaning (multi-step enrolment matching)
#  14. Delay date calculation (later-awarded credentials within time windows)
#  15. Research university / outcome credential flags
#  16. Final distributions (8+ Credential_By_Year output tables)
#
# Input tables:
#   - STP_Credential, STP_Credential_Record_Type — raw credential data + record status
#   - STP_Enrolment, STP_Enrolment_Valid — enrolment records for SupVar matching
#   - AgeGroupLookup — age group index → label mapping
#   - CredentialRank — credential category → rank mapping
#   - OutcomeCredential — credential category → outcome cred label mapping
#
# Output tables (written to DB):
#   - CredentialSupVars — cleaned credential-level SupVars
#   - CredentialSupVarsFromEnrolment — enrolment records matched to credentials
#   - Credential_Non_Dup — deduplicated credentials
#   - tblCredential_HighestRank — highest-ranked credentials with delay dates
#   - Credential_By_Year_AgeGroup_* (8 variants) — final distribution tables

library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(dbplyr)

# ---- Configure LAN and file paths ----
db_config <- config::get("decimal")
lan <- config::get("lan")
my_schema <- config::get("myschema")

con <- dbConnect(odbc::odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")

# Helper: reference a table in the user's schema
sch_tbl <- function(name) {
  tbl(con, dbplyr::in_schema(my_schema, name))
}

# Helper: check if a value is blank/missing/unspecified
is_blank <- function(x) {
  is.na(x) | x %in% c("", " ", "(Unspecified)")
}


# ******************************************************************************
# Section 1: Build CredentialSupVarsFromEnrolment
# WHY: Credential records lack gender, birthdate, visa status, etc. We match them
# to enrolment records to fill in these supplementary variables. Matching is done
# first by ENCRYPTED_TRUE_PEN (EPEN), then by PSI_CODE + PSI_STUDENT_NUMBER for
# records without a valid EPEN.
# ******************************************************************************

# ---- Pull source tables ----
stp_credential <- sch_tbl("STP_Credential") %>% collect() |> rename_with(toupper)
stp_cred_rec_type <- sch_tbl("STP_Credential_Record_Type") %>% collect() |> rename_with(toupper)
stp_enrolment <- sch_tbl("STP_Enrolment") %>% collect() |> rename_with(toupper)
stp_enrolment_valid <- sch_tbl("STP_Enrolment_Valid") %>% collect() |> rename_with(toupper)

# ---- Create Credential view equivalent ----
# WHY: Filter to valid records (RecordStatus=0) with non-blank award dates.
credential <- stp_credential %>%
  inner_join(stp_cred_rec_type %>% select(ID, RECORDSTATUS),
             by = "ID") %>%
  filter(RECORDSTATUS == 0,
         CREDENTIAL_AWARD_DATE != "",
         CREDENTIAL_AWARD_DATE != " ",
         CREDENTIAL_AWARD_DATE != "(Unspecified)")

# ---- Part A: Match by EPEN ----
# Find max school year per EPEN, then get enrolment IDs at that max year.
enrol_max_year_by_epen <- stp_enrolment_valid %>%
  filter(!is_blank(ENCRYPTED_TRUE_PEN)) %>%
  group_by(ENCRYPTED_TRUE_PEN) %>%
  summarise(MaxSchoolYear = max(PSI_SCHOOL_YEAR), .groups = "drop")

# Get enrolment records at max school year per EPEN
enrol_by_epen <- stp_enrolment_valid %>%
  inner_join(enrol_max_year_by_epen,
             by = c("ENCRYPTED_TRUE_PEN", "PSI_SCHOOL_YEAR" = "MaxSchoolYear")) %>%
  select(ID, ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, MaxSchoolYear) %>%
  distinct()

# Get full enrolment data for matched IDs
enrol_step3_epen <- stp_enrolment_valid %>%
  semi_join(enrol_by_epen, by = "ID") %>%
  select(ID, PSI_STUDENT_NUMBER, ENCRYPTED_TRUE_PEN, PSI_SCHOOL_YEAR,
         PSI_STUDENT_POSTAL_CODE_CURRENT, PSI_ENROLMENT_SEQUENCE, PSI_CODE, PSI_MIN_START_DATE)

# Build CredentialSupVarsFromEnrolment Part A — join enrolment to Credential by EPEN
csve_part_a <- enrol_step3_epen %>%
  inner_join(
    credential %>% select(RECORDSTATUS, ENCRYPTED_TRUE_PEN),
    by = "ENCRYPTED_TRUE_PEN"
  ) %>%
  distinct() %>%
  transmute(
    ENROLMENTID = ID,
    ENCRYPTED_TRUE_PEN,
    PSI_MIN_START_DATE,
    CREDENTIALRECORDSTATUS = RECORDSTATUS,
    PSI_STUDENT_POSTAL_CODE_CURRENT,
    PSI_SCHOOL_YEAR,
    PSI_CODE,
    PSI_STUDENT_NUMBER,
    PSI_ENROLMENT_SEQUENCE
  )

# ---- Part B: Match by PSI_CODE + PSI_STUDENT_NUMBER (for null/blank EPENs) ----
# Get credential EPENs that were NOT matched in Part A
cred_epens_not_matched <- credential %>%
  anti_join(csve_part_a, by = "ENCRYPTED_TRUE_PEN") %>%
  select(PSI_CODE, PSI_STUDENT_NUMBER)

# Get credential records with null/blank EPENs
cred_null_epens <- credential %>%
  filter(ENCRYPTED_TRUE_PEN == "") %>%
  select(ID, PSI_CODE, PSI_STUDENT_NUMBER)

# Find max school year per PSI_CODE/STUDENT_NUMBER for null-EPEN records
enrol_max_year_by_stuid <- stp_enrolment_valid %>%
  filter(is_blank(ENCRYPTED_TRUE_PEN)) %>%
  group_by(PSI_CODE, PSI_STUDENT_NUMBER) %>%
  summarise(MaxSchoolYear = max(PSI_SCHOOL_YEAR), .groups = "drop")

# Get enrolment records at max year by PSI_CODE/STUDENT_NUMBER
enrol_by_stuid <- stp_enrolment_valid %>%
  inner_join(enrol_max_year_by_stuid,
             by = c("PSI_CODE", "PSI_STUDENT_NUMBER", "PSI_SCHOOL_YEAR" = "MaxSchoolYear")) %>%
  select(ID, ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, MaxSchoolYear) %>%
  distinct()

# Full enrolment data for null-EPEN matched IDs
enrol_step6_stuid <- stp_enrolment_valid %>%
  semi_join(enrol_by_stuid, by = "ID") %>%
  select(ID, PSI_STUDENT_NUMBER, ENCRYPTED_TRUE_PEN, PSI_SCHOOL_YEAR,
         PSI_STUDENT_POSTAL_CODE_CURRENT, PSI_ENROLMENT_SEQUENCE, PSI_CODE, PSI_MIN_START_DATE)

# Build CredentialSupVarsFromEnrolment Part B — join by PSI_CODE/STUDENT_NUMBER
csve_part_b <- enrol_step6_stuid %>%
  inner_join(
    credential %>% select(RECORDSTATUS, PSI_CODE, PSI_STUDENT_NUMBER),
    by = c("PSI_CODE", "PSI_STUDENT_NUMBER")
  ) %>%
  distinct() %>%
  transmute(
    ENROLMENTID = ID,
    ENCRYPTED_TRUE_PEN,
    PSI_MIN_START_DATE,
    CREDENTIALRECORDSTATUS = RECORDSTATUS,
    PSI_STUDENT_POSTAL_CODE_CURRENT,
    PSI_SCHOOL_YEAR,
    PSI_CODE,
    PSI_STUDENT_NUMBER,
    PSI_ENROLMENT_SEQUENCE
  )

# Combine both parts
CredentialSupVarsFromEnrolment <- bind_rows(csve_part_a, csve_part_b)

# Add supplementary columns from STP_Enrolment
enrol_supvars <- stp_enrolment %>%
  select(ID, PSI_BIRTHDATE_CLEANED, PSI_VISA_STATUS, PSI_BIRTHDATE,
         PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
         PSI_CIP_CODE, PSI_CONTINUING_EDUCATION_COURSE_ONLY, PSI_GENDER,
         LAST_SEEN_BIRTHDATE)

CredentialSupVarsFromEnrolment <- CredentialSupVarsFromEnrolment %>%
  left_join(enrol_supvars, by = c("ENROLMENTID" = "ID")) %>%
  mutate(
    PSI_BIRTHDATE_CLEANED = if_else(
      PSI_BIRTHDATE_CLEANED == as.Date("1900-01-01"),
      as.Date(NA), PSI_BIRTHDATE_CLEANED
    )
  )


# ******************************************************************************
# Section 2: Build CredentialSupVars
# WHY: Create the base credential supplementary variables table from the Credential
# view, with empty columns to be filled in by subsequent cleaning steps.
# ******************************************************************************

CredentialSupVars <- credential %>%
  transmute(
    ID,
    ENCRYPTED_TRUE_PEN,
    PSI_STUDENT_NUMBER,
    PSI_CODE,
    PSI_SCHOOL_YEAR,
    CREDENTIAL_AWARD_DATE,
    CREDENTIALRECORDSTATUS = RECORDSTATUS,
    PSI_PROGRAM_CODE,
    PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    PSI_CREDENTIAL_CIP,
    PSI_CREDENTIAL_LEVEL,
    PSI_CREDENTIAL_CATEGORY
  ) %>%
  # Add empty columns to be filled later
  mutate(
    CREDENTIAL_AWARD_DATE_D = as.Date(NA),
    PSI_AWARD_SCHOOL_YEAR = NA_character_,
    RECORD_TO_DELETE = NA_integer_,
    PSI_BIRTHDATE_CLEANED_D = as.Date(NA),
    PSI_BIRTHDATE_CLEANED = as.Date(NA),
    LAST_DATE_HIGHEST_CRED = NA_character_,
    HIGHEST_CRED_BY_DATE = NA_character_,
    HIGHEST_CRED_BY_RANK = NA_character_,
    HIGHEST_CRED_BY_SCHOOL_YEAR = NA_character_,
    OUTCOMES_CRED = NA_character_,
    RESEARCH_UNIVERSITY = NA_integer_,
    CREDENTIAL_AWARD_DATE_D_DELAYED = as.Date(NA),
    PSI_AWARD_SCHOOL_YEAR_DELAYED = NA_character_,
    AGE_AT_GRAD = NA_integer_,
    AGE_GROUP_AT_GRAD = NA_integer_,
    PSI_GENDER_CLEANED = NA_character_
  )


# ******************************************************************************
# Section 3: Developmental/partial year exclusions
# WHY: Flag credentials that should be excluded: developmental credentials, OTHER,
# NONE, SHORT CERTIFICATE categories, and credentials awarded in the partial year
# (>= model year Sept 1). These flags are set on STP_Credential_Record_Type.
# ******************************************************************************

# Developmental credential categories
drop_cred_cats <- c("DEVELOPMENTAL CREDENTIAL", "OTHER", "NONE", "SHORT CERTIFICATE")

cred_ids_to_drop <- credential %>%
  filter(PSI_CREDENTIAL_CATEGORY %in% drop_cred_cats) %>%
  select(ID)

# Flag in STP_Credential_Record_Type (will be used by the view filter)
stp_cred_rec_type <- stp_cred_rec_type %>%
  mutate(DROPCREDCATEGORY = if_else(ID %in% cred_ids_to_drop$ID, "Yes", NA_character_))

# Partial year exclusion — flag credentials with award date >= 2023-09-01
# WHY: Credentials from the current incomplete year should be excluded.
# !! Update this date for each model run !!
partial_year_cutoff <- as.Date("2023-09-01")

# First set award date on CredentialSupVars
CredentialSupVars <- CredentialSupVars %>%
  mutate(CREDENTIAL_AWARD_DATE_D = as.Date(CREDENTIAL_AWARD_DATE))

cred_ids_partial <- CredentialSupVars %>%
  filter(CREDENTIAL_AWARD_DATE_D >= partial_year_cutoff) %>%
  select(ID)

stp_cred_rec_type <- stp_cred_rec_type %>%
  mutate(DROPPARTIALYEAR = if_else(ID %in% cred_ids_partial$ID, "Yes", NA_character_))


# ******************************************************************************
# Section 4: Birthdate cleaning from enrolment
# WHY: Extract unique birthdate values per EPEN/PSI_CODE/STUDENT_NUMBER from
# enrolment data. Used to fill missing birthdates in CredentialSupVars.
# ******************************************************************************

cred_sup_vars_birthdate_clean <- CredentialSupVarsFromEnrolment %>%
  select(ENCRYPTED_TRUE_PEN, PSI_BIRTHDATE_CLEANED, PSI_BIRTHDATE_CLEANED_D,
         PSI_STUDENT_NUMBER, PSI_CODE) %>%
  distinct() %>%
  mutate(
    PSI_BIRTHDATE_CLEANED_D = if_else(
      !is.na(PSI_BIRTHDATE_CLEANED) & !is_blank(PSI_BIRTHDATE_CLEANED),
      as.Date(PSI_BIRTHDATE_CLEANED), PSI_BIRTHDATE_CLEANED_D
    )
  )


# ******************************************************************************
# Section 5: Gender cleaning
# WHY: Many records have missing, unknown, or inconsistent gender values across
# enrolment records. This extensive cleaning process resolves multi-gender EPENs
# by selecting the most recent gender, cleans unknowns, and falls back to
# PSI_CODE/PSI_STUDENT_NUMBER matching for null-EPEN records.
#
# The original uses ~35 temp tables. Here we use dplyr pipelines with intermediate
# R variables.
# ******************************************************************************

# ---- 5a: Build CredentialSupVars_Gender — unique EPEN/gender from enrolment ----
csvs_gender <- CredentialSupVarsFromEnrolment %>%
  select(ENCRYPTED_TRUE_PEN, PSI_GENDER) %>%
  distinct()

# ---- 5b: Find EPENs with multiple genders ----
multi_gender_epens <- csvs_gender %>%
  count(ENCRYPTED_TRUE_PEN) %>%
  filter(n > 1) %>%
  select(ENCRYPTED_TRUE_PEN)

# ---- 5c: Resolve multi-gender EPENs ----
# WHY: When an EPEN has multiple genders across enrolment records, we pick the gender
# from the most recent school year, breaking ties by max enrolment sequence.
# Original: Steps 1-5 across 3 temp tables.

if (nrow(multi_gender_epens) > 0) {
  # Max school year + enrolment sequence per EPEN/gender combo
  multi_gender_max <- CredentialSupVarsFromEnrolment %>%
    semi_join(multi_gender_epens, by = "ENCRYPTED_TRUE_PEN") %>%
    group_by(ENCRYPTED_TRUE_PEN, PSI_GENDER) %>%
    summarise(
      MAX_PSI_SCHOOL_YEAR = max(PSI_SCHOOL_YEAR),
      MAX_PSI_ENROLMENT_SEQUENCE = max(PSI_ENROLMENT_SEQUENCE),
      .groups = "drop"
    )

  # Overall max per EPEN
  multi_gender_overall_max <- multi_gender_max %>%
    group_by(ENCRYPTED_TRUE_PEN) %>%
    summarise(
      MAX_MAX_PSI_SCHOOL_YEAR = max(MAX_PSI_SCHOOL_YEAR),
      MAX_MAX_PSI_ENROLMENT_SEQUENCE = max(MAX_PSI_ENROLMENT_SEQUENCE),
      .groups = "drop"
    )

  # Resolve: join to find the gender at the max school year + max enrolment sequence
  multi_gender_resolved <- multi_gender_overall_max %>%
    filter(!is_blank(ENCRYPTED_TRUE_PEN)) %>%
    inner_join(
      multi_gender_max,
      by = c("ENCRYPTED_TRUE_PEN" = "ENCRYPTED_TRUE_PEN",
             "MAX_MAX_PSI_SCHOOL_YEAR" = "MAX_PSI_SCHOOL_YEAR",
             "MAX_MAX_PSI_ENROLMENT_SEQUENCE" = "MAX_PSI_ENROLMENT_SEQUENCE")
    ) %>%
    select(ENCRYPTED_TRUE_PEN, PSI_GENDER_TO_USE = PSI_GENDER) %>%
    distinct()
} else {
  multi_gender_resolved <- tibble(
    ENCRYPTED_TRUE_PEN = character(),
    PSI_GENDER_TO_USE = character()
  )
}

# ---- 5d: Apply resolved genders to the gender table ----
csvs_gender <- csvs_gender %>%
  left_join(multi_gender_resolved, by = "ENCRYPTED_TRUE_PEN") %>%
  mutate(
    PSI_GENDER_CLEANED_FLAG = if_else(!is.na(PSI_GENDER_TO_USE), "Yes", NA_character_),
    PSI_GENDER_CLEANED = coalesce(PSI_GENDER_TO_USE, PSI_GENDER)
  )

# ---- 5e: Clean unknowns ('U', 'Unknown') by looking at all enrolment genders ----
# WHY: Some EPENs have gender='U' or 'Unknown'. We check if other enrolment records
# for the same EPEN have a valid gender (Male/Female/Gender Diverse).
unknown_genders <- csvs_gender %>%
  filter(PSI_GENDER_CLEANED %in% c("U", "Unknown"))

if (nrow(unknown_genders) > 0) {
  # Find non-U/Unknown genders from enrolment for these EPENs
  unknowns_with_alternatives <- unknown_genders %>%
    select(ENCRYPTED_TRUE_PEN) %>%
    inner_join(
      CredentialSupVarsFromEnrolment %>%
        select(ENCRYPTED_TRUE_PEN, PSI_GENDER) %>%
        distinct(),
      by = "ENCRYPTED_TRUE_PEN"
    ) %>%
    filter(!PSI_GENDER %in% c("U", "Unknown")) %>%
    group_by(ENCRYPTED_TRUE_PEN) %>%
    summarise(GENDERTOUSE = first(PSI_GENDER), .groups = "drop")

  # Update csvs_gender with resolved unknowns
  csvs_gender <- csvs_gender %>%
    left_join(unknowns_with_alternatives, by = "ENCRYPTED_TRUE_PEN") %>%
    mutate(
      PSI_GENDER_CLEANED = if_else(
        PSI_GENDER_CLEANED %in% c("U", "Unknown") & !is.na(GENDERTOUSE),
        GENDERTOUSE, PSI_GENDER_CLEANED
      )
    ) %>%
    select(-GENDERTOUSE)
}

# ---- 5f: Apply gender to CredentialSupVars by EPEN ----
# WHY: Update CredentialSupVars PSI_GENDER_CLEANED from the resolved gender table.
CredentialSupVars <- CredentialSupVars %>%
  left_join(
    csvs_gender %>%
      filter(!is_blank(ENCRYPTED_TRUE_PEN)) %>%
      select(ENCRYPTED_TRUE_PEN, PSI_GENDER_CLEANED),
    by = "ENCRYPTED_TRUE_PEN",
    suffix = c("", "_from_gender")
  ) %>%
  mutate(PSI_GENDER_CLEANED = coalesce(PSI_GENDER_CLEANED_from_gender, PSI_GENDER_CLEANED)) %>%
  select(-PSI_GENDER_CLEANED_from_gender)

# ---- 5g: Handle null-EPEN records — match by PSI_CODE/PSI_STUDENT_NUMBER ----
# WHY: Records without a valid EPEN need gender from STP_Enrolment matched by
# PSI_CODE + PSI_STUDENT_NUMBER.
null_gender_records <- CredentialSupVars %>%
  filter(is.na(PSI_GENDER_CLEANED)) %>%
  select(ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE)

if (nrow(null_gender_records) > 0) {
  # Get genders from STP_Enrolment by PSI_CODE/STUDENT_NUMBER
  enrol_gender_by_stuid <- null_gender_records %>%
    inner_join(
      stp_enrolment %>%
        select(PSI_STUDENT_NUMBER, PSI_CODE, PSI_GENDER) %>%
        distinct(),
      by = c("PSI_STUDENT_NUMBER", "PSI_CODE")
    )

  # Find EPENs with multiple genders from this match
  multi_gender_stuid <- enrol_gender_by_stuid %>%
    count(ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE) %>%
    filter(n > 1)

  # Resolve multi-gender by most recent school year (same logic as 5c)
  if (nrow(multi_gender_stuid) > 0) {
    multi_stuid_max <- multi_gender_stuid %>%
      select(-n) %>%
      inner_join(
        CredentialSupVarsFromEnrolment %>%
          select(ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, PSI_GENDER,
                 PSI_SCHOOL_YEAR, PSI_ENROLMENT_SEQUENCE),
        by = c("ENCRYPTED_TRUE_PEN", "PSI_STUDENT_NUMBER", "PSI_CODE")
      ) %>%
      group_by(ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, PSI_GENDER) %>%
      summarise(
        MAX_PSI_SCHOOL_YEAR = max(PSI_SCHOOL_YEAR),
        MAX_PSI_ENROLMENT_SEQUENCE = max(PSI_ENROLMENT_SEQUENCE),
        .groups = "drop"
      )

    multi_stuid_overall_max <- multi_stuid_max %>%
      group_by(ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE) %>%
      summarise(
        MAX_MAX_YEAR = max(MAX_PSI_SCHOOL_YEAR),
        MAX_MAX_SEQ = max(MAX_PSI_ENROLMENT_SEQUENCE),
        .groups = "drop"
      )

    multi_stuid_resolved <- multi_stuid_overall_max %>%
      inner_join(
        multi_stuid_max,
        by = c("ENCRYPTED_TRUE_PEN", "PSI_STUDENT_NUMBER", "PSI_CODE",
               "MAX_MAX_YEAR" = "MAX_PSI_SCHOOL_YEAR",
               "MAX_MAX_SEQ" = "MAX_PSI_ENROLMENT_SEQUENCE")
      ) %>%
      select(ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, PSI_GENDER_TO_USE = PSI_GENDER) %>%
      distinct()
  } else {
    multi_stuid_resolved <- tibble(
      ENCRYPTED_TRUE_PEN = character(),
      PSI_STUDENT_NUMBER = character(),
      PSI_CODE = character(),
      PSI_GENDER_TO_USE = character()
    )
  }

  # Build final gender for null-EPEN records
  enrol_gender_final <- enrol_gender_by_stuid %>%
    left_join(multi_stuid_resolved,
              by = c("ENCRYPTED_TRUE_PEN", "PSI_STUDENT_NUMBER", "PSI_CODE")) %>%
    mutate(
      PSI_GENDER_CLEANED_FLAG = if_else(!is.na(PSI_GENDER_TO_USE), "Yes", NA_character_)
    ) %>%
    # Clean unknowns from the enrolment match
    mutate(
      PSI_GENDER_CLEANED = coalesce(PSI_GENDER_TO_USE, PSI_GENDER)
    ) %>%
    # If still unknown, keep as-is
    mutate(
      PSI_GENDER_CLEANED_FLAG = if_else(
        PSI_GENDER %in% c("U", "Unknown", "(Unspecified)") & is.na(PSI_GENDER_CLEANED_FLAG),
        "Yes", PSI_GENDER_CLEANED_FLAG
      ),
      PSI_GENDER_CLEANED = if_else(is.na(PSI_GENDER_CLEANED_FLAG), PSI_GENDER, PSI_GENDER_CLEANED)
    ) %>%
    select(PSI_STUDENT_NUMBER, PSI_CODE, PSI_GENDER_CLEANED, PSI_GENDER_CLEANED_FLAG) %>%
    distinct()

  # Apply to CredentialSupVars
  CredentialSupVars <- CredentialSupVars %>%
    left_join(
      enrol_gender_final %>% filter(PSI_GENDER_CLEANED_FLAG == "Yes"),
      by = c("PSI_STUDENT_NUMBER", "PSI_CODE"),
      suffix = c("", "_enrol")
    ) %>%
    mutate(
      PSI_GENDER_CLEANED = if_else(
        is.na(PSI_GENDER_CLEANED) & !is.na(PSI_GENDER_CLEANED_enrol),
        PSI_GENDER_CLEANED_enrol, PSI_GENDER_CLEANED
      )
    ) %>%
    select(-PSI_GENDER_CLEANED_enrol, -PSI_GENDER_CLEANED_FLAG_enrol)
}


# ******************************************************************************
# Section 6: Birthdate cleaning with LAST_SEEN_BIRTHDATE
# WHY: Fill missing birthdates from enrolment data, first by EPEN, then by
# PSI_CODE/STUDENT_NUMBER, and finally from LAST_SEEN_BIRTHDATE.
# ******************************************************************************

# Join birthdate by EPEN (non-blank EPENs)
CredentialSupVars <- CredentialSupVars %>%
  left_join(
    cred_sup_vars_birthdate_clean %>%
      filter(!is_blank(ENCRYPTED_TRUE_PEN)) %>%
      select(ENCRYPTED_TRUE_PEN, BD_CLEANED = PSI_BIRTHDATE_CLEANED,
             BD_CLEANED_D = PSI_BIRTHDATE_CLEANED_D) %>%
      distinct(),
    by = "ENCRYPTED_TRUE_PEN"
  ) %>%
  mutate(
    PSI_BIRTHDATE_CLEANED = coalesce(BD_CLEANED, PSI_BIRTHDATE_CLEANED),
    PSI_BIRTHDATE_CLEANED_D = coalesce(BD_CLEANED_D, PSI_BIRTHDATE_CLEANED_D)
  ) %>%
  select(-BD_CLEANED, -BD_CLEANED_D)

# Join birthdate by PSI_CODE/STUDENT_NUMBER (blank EPENs)
CredentialSupVars <- CredentialSupVars %>%
  left_join(
    cred_sup_vars_birthdate_clean %>%
      filter(is_blank(ENCRYPTED_TRUE_PEN)) %>%
      select(PSI_STUDENT_NUMBER, PSI_CODE, BD_CLEANED = PSI_BIRTHDATE_CLEANED,
             BD_CLEANED_D = PSI_BIRTHDATE_CLEANED_D) %>%
      distinct(),
    by = c("PSI_STUDENT_NUMBER", "PSI_CODE")
  ) %>%
  mutate(
    PSI_BIRTHDATE_CLEANED = if_else(
      is_blank(ENCRYPTED_TRUE_PEN) & !is_blank(PSI_CODE) & !is_blank(PSI_STUDENT_NUMBER),
      coalesce(BD_CLEANED, PSI_BIRTHDATE_CLEANED), PSI_BIRTHDATE_CLEANED
    ),
    PSI_BIRTHDATE_CLEANED_D = if_else(
      is_blank(ENCRYPTED_TRUE_PEN) & !is_blank(PSI_CODE) & !is_blank(PSI_STUDENT_NUMBER),
      coalesce(BD_CLEANED_D, PSI_BIRTHDATE_CLEANED_D), PSI_BIRTHDATE_CLEANED_D
    )
  ) %>%
  select(-BD_CLEANED, -BD_CLEANED_D)

# Get LAST_SEEN_BIRTHDATE from enrolment
last_seen_bd <- stp_enrolment %>%
  select(ID, LAST_SEEN_BIRTHDATE)

CredentialSupVarsFromEnrolment <- CredentialSupVarsFromEnrolment %>%
  left_join(last_seen_bd, by = c("ENROLMENTID" = "ID"))

# Apply LAST_SEEN_BIRTHDATE to CredentialSupVars
lsbd_by_epen <- CredentialSupVarsFromEnrolment %>%
  filter(!is.na(LAST_SEEN_BIRTHDATE)) %>%
  select(ENCRYPTED_TRUE_PEN, LAST_SEEN_BIRTHDATE) %>%
  distinct()

CredentialSupVars <- CredentialSupVars %>%
  left_join(lsbd_by_epen, by = "ENCRYPTED_TRUE_PEN") %>%
  mutate(
    PSI_BIRTHDATE_CLEANED = if_else(
      !is.na(LAST_SEEN_BIRTHDATE) & !is_blank(LAST_SEEN_BIRTHDATE) &
        (is.na(PSI_BIRTHDATE_CLEANED) | is_blank(PSI_BIRTHDATE_CLEANED)),
      LAST_SEEN_BIRTHDATE, PSI_BIRTHDATE_CLEANED
    )
  ) %>%
  select(-LAST_SEEN_BIRTHDATE)


# ******************************************************************************
# Section 7: Recreate Credential view with cleaned SupVars
# WHY: Rebuild the working credential dataset with cleaned SupVars (gender, birthdate)
# and with the exclusion filters applied (no developmental, no partial year).
# ******************************************************************************

credential <- stp_credential %>%
  inner_join(stp_cred_rec_type %>% select(ID, RECORDSTATUS, DROPCREDCATEGORY, DROPPARTIALYEAR),
             by = "ID") %>%
  filter(RECORDSTATUS == 0,
         is.na(DROPCREDCATEGORY),
         is.na(DROPPARTIALYEAR)) %>%
  inner_join(
    CredentialSupVars %>%
      select(ID, CREDENTIAL_AWARD_DATE_D, AGE_AT_GRAD, AGE_GROUP_AT_GRAD,
             PSI_AWARD_SCHOOL_YEAR, RECORD_TO_DELETE, LAST_DATE_HIGHEST_CRED,
             HIGHEST_CRED_BY_DATE, HIGHEST_CRED_BY_RANK, HIGHEST_CRED_BY_SCHOOL_YEAR,
             OUTCOMES_CRED, RESEARCH_UNIVERSITY, CREDENTIAL_AWARD_DATE_D_DELAYED,
             PSI_AWARD_SCHOOL_YEAR_DELAYED, PSI_BIRTHDATE_CLEANED,
             PSI_BIRTHDATE_CLEANED_D, PSI_GENDER_CLEANED),
    by = "ID"
  )


# ******************************************************************************
# Section 8: Age at graduation calculation
# WHY: Compute age at graduation from birthdate and award date. Also assign age
# group using the AgeGroupLookup table.
# ******************************************************************************

# Update birthdate_cleaned_D from birthdate_cleaned (date conversion)
credential <- credential %>%
  mutate(
    PSI_BIRTHDATE_CLEANED_D = if_else(
      !is.na(PSI_BIRTHDATE_CLEANED),
      as.Date(PSI_BIRTHDATE_CLEANED), PSI_BIRTHDATE_CLEANED_D
    )
  )

# Also update CredentialSupVars
CredentialSupVars <- CredentialSupVars %>%
  mutate(
    PSI_BIRTHDATE_CLEANED_D = if_else(
      !is.na(PSI_BIRTHDATE_CLEANED),
      as.Date(PSI_BIRTHDATE_CLEANED), PSI_BIRTHDATE_CLEANED_D
    )
  )

# Compute age at graduation using the "birthday not yet passed" correction
# WHY: SQL uses DATEDIFF + DATEADD to check if birthday has occurred in the award year.
# We use lubridate's %--% interval for correct age calculation.
age_group_lookup <- sch_tbl("AgeGroupLookup") %>% collect() |> rename_with(toupper)

credential <- credential %>%
  mutate(
    AGE_AT_GRAD = if_else(
      !is.na(PSI_BIRTHDATE_CLEANED_D) & !is_blank(PSI_BIRTHDATE_CLEANED),
      as.integer(lubridate::time_length(
        PSI_BIRTHDATE_CLEANED_D %--% CREDENTIAL_AWARD_DATE_D, "years"
      )),
      AGE_AT_GRAD
    )
  )

# Assign age group from lookup
# WHY: Map numeric age to an age group index based on lower/upper bounds.
credential <- credential %>%
  mutate(AGE_GROUP_AT_GRAD = NA_integer_)

for (i in seq_len(nrow(age_group_lookup))) {
  lb <- age_group_lookup$LOWERBOUND[i]
  ub <- age_group_lookup$UPPERBOUND[i]
  idx <- age_group_lookup$AGEINDEX[i]
  credential <- credential %>%
    mutate(
      AGE_GROUP_AT_GRAD = if_else(
        !is.na(AGE_AT_GRAD) & AGE_AT_GRAD >= lb & AGE_AT_GRAD <= ub,
        idx, AGE_GROUP_AT_GRAD
      )
    )
}

# Compute school year from award date
# WHY: School year spans Sep-Aug. If award month >= 9, it's the start of a new year pair.
credential <- credential %>%
  mutate(
    PSI_AWARD_SCHOOL_YEAR = if_else(
      is.na(PSI_AWARD_SCHOOL_YEAR),
      if_else(
        lubridate::month(CREDENTIAL_AWARD_DATE_D) >= 9,
        paste0(lubridate::year(CREDENTIAL_AWARD_DATE_D), "/",
               lubridate::year(CREDENTIAL_AWARD_DATE_D) + 1),
        paste0(lubridate::year(CREDENTIAL_AWARD_DATE_D) - 1, "/",
               lubridate::year(CREDENTIAL_AWARD_DATE_D))
      ),
      PSI_AWARD_SCHOOL_YEAR
    )
  )

# Sync back to CredentialSupVars
CredentialSupVars <- CredentialSupVars %>%
  left_join(
    credential %>% select(ID, CREDENTIAL_AWARD_DATE_D, AGE_AT_GRAD, AGE_GROUP_AT_GRAD, PSI_AWARD_SCHOOL_YEAR),
    by = "ID",
    suffix = c("", "_cred")
  ) %>%
  mutate(
    CREDENTIAL_AWARD_DATE_D = coalesce(CREDENTIAL_AWARD_DATE_D_cred, CREDENTIAL_AWARD_DATE_D),
    AGE_AT_GRAD = coalesce(AGE_AT_GRAD_cred, AGE_AT_GRAD),
    AGE_GROUP_AT_GRAD = coalesce(AGE_GROUP_AT_GRAD_cred, AGE_GROUP_AT_GRAD),
    PSI_AWARD_SCHOOL_YEAR = coalesce(PSI_AWARD_SCHOOL_YEAR_cred, PSI_AWARD_SCHOOL_YEAR)
  ) %>%
  select(-ends_with("_cred"))


# ******************************************************************************
# Section 9: Credential_Non_Dup creation (deduplication)
# WHY: Remove duplicate credential records — keep one row per unique combination
# of EPEN, PSI_CODE, program code, description, CIP, level, category, and award date.
# Also apply additional gender cleaning from STP_Enrolment.
# ******************************************************************************

# Try to fill missing gender from STP_Enrolment
enrol_gender <- stp_enrolment %>%
  filter(PSI_GENDER %in% c("Female", "Male", "Gender Diverse")) %>%
  select(ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, PSI_GENDER) %>%
  distinct()

credential <- credential %>%
  left_join(enrol_gender,
            by = c("ENCRYPTED_TRUE_PEN", "PSI_STUDENT_NUMBER", "PSI_CODE"),
            suffix = c("", "_enrol")) %>%
  mutate(
    PSI_GENDER_CLEANED = if_else(
      is_blank(PSI_GENDER_CLEANED) & !is.na(PSI_GENDER_enrol),
      PSI_GENDER_enrol, PSI_GENDER_CLEANED
    )
  ) %>%
  select(-PSI_GENDER_enrol)

# Create deduplication view — one row per unique credential combination, keeping max ID
credential_remove_dup <- credential %>%
  group_by(ENCRYPTED_TRUE_PEN, PSI_CODE, PSI_PROGRAM_CODE,
            PSI_CREDENTIAL_PROGRAM_DESCRIPTION, PSI_CREDENTIAL_CIP,
            PSI_CREDENTIAL_LEVEL, PSI_CREDENTIAL_CATEGORY, CREDENTIAL_AWARD_DATE_D) %>%
  summarise(ID = max(ID), .groups = "drop")

# Create Credential_Non_Dup by joining
Credential_Non_Dup <- credential %>%
  semi_join(credential_remove_dup, by = "ID") %>%
  select(ID, PSI_STUDENT_NUMBER, PSI_BIRTHDATE_CLEANED, PSI_GENDER_CLEANED,
         ENCRYPTED_TRUE_PEN, PSI_SCHOOL_YEAR, PSI_CODE, CREDENTIAL_AWARD_DATE,
         RECORDSTATUS, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
         PSI_CREDENTIAL_CIP, PSI_CREDENTIAL_LEVEL, PSI_CREDENTIAL_CATEGORY,
         CREDENTIAL_AWARD_DATE_D, AGE_AT_GRAD, AGE_GROUP_AT_GRAD,
         PSI_BIRTHDATE_CLEANED_D, PSI_AWARD_SCHOOL_YEAR, RECORD_TO_DELETE,
         LAST_DATE_HIGHEST_CRED, HIGHEST_CRED_BY_DATE, HIGHEST_CRED_BY_RANK,
         OUTCOMES_CRED, HIGHEST_CRED_BY_SCHOOL_YEAR, RESEARCH_UNIVERSITY)


# ******************************************************************************
# Section 10: Gender cleaning for Credential_Non_Dup
# WHY: Resolve multi-gender EPENs within Non_Dup (pick gender from most recent
# credential award date), then impute remaining missing genders proportionally.
# ******************************************************************************

# Find EPENs with multiple genders in Non_Dup
dup_gender_epens <- Credential_Non_Dup %>%
  select(ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, PSI_GENDER_CLEANED) %>%
  distinct() %>%
  count(ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE) %>%
  filter(n > 1)

if (nrow(dup_gender_epens) > 0) {
  # Pick gender from record with max award date
  dup_gender_max_date <- Credential_Non_Dup %>%
    semi_join(dup_gender_epens, by = "ENCRYPTED_TRUE_PEN") %>%
    group_by(ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE) %>%
    slice_max(CREDENTIAL_AWARD_DATE_D, n = 1, with_ties = FALSE) %>%
    ungroup() %>%
    select(ENCRYPTED_TRUE_PEN, PSI_GENDER_CLEANED) %>%
    distinct()

  # Apply resolved gender to all records with that EPEN
  Credential_Non_Dup <- Credential_Non_Dup %>%
    left_join(dup_gender_max_date, by = "ENCRYPTED_TRUE_PEN",
              suffix = c("", "_resolved")) %>%
    mutate(
      PSI_GENDER_CLEANED = if_else(
        PSI_GENDER_CLEANED != PSI_GENDER_CLEANED_resolved,
        PSI_GENDER_CLEANED_resolved, PSI_GENDER_CLEANED
      )
    ) %>%
    select(-PSI_GENDER_CLEANED_resolved)
}

# ---- Impute missing gender proportionally ----
# WHY: Records with blank/unknown gender are assigned Female/Male/Gender Diverse
# based on the observed gender proportions within each credential category.
# Original: Manual UPDATE TOP(N) queries after computing distribution.

no_gender <- Credential_Non_Dup %>%
  filter(is_blank(PSI_GENDER_CLEANED)) %>%
  select(ID, ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE,
         PSI_GENDER_CLEANED, PSI_CREDENTIAL_CATEGORY) %>%
  distinct()

if (nrow(no_gender) > 0) {
  # Compute gender distribution by credential category
  gender_dist <- Credential_Non_Dup %>%
    filter(!is_blank(PSI_GENDER_CLEANED)) %>%
    count(PSI_GENDER_CLEANED, PSI_CREDENTIAL_CATEGORY, name = "GENDERCOUNT")

  total_by_cat <- gender_dist %>%
    group_by(PSI_CREDENTIAL_CATEGORY) %>%
    summarise(TOTAL = sum(GENDERCOUNT), .groups = "drop")

  female_pct <- gender_dist %>%
    filter(PSI_GENDER_CLEANED == "Female") %>%
    left_join(total_by_cat, by = "PSI_CREDENTIAL_CATEGORY") %>%
    mutate(P = GENDERCOUNT / TOTAL) %>%
    select(PSI_CREDENTIAL_CATEGORY, P)

  male_pct <- gender_dist %>%
    filter(PSI_GENDER_CLEANED == "Male") %>%
    left_join(total_by_cat, by = "PSI_CREDENTIAL_CATEGORY") %>%
    mutate(P = GENDERCOUNT / TOTAL) %>%
    select(PSI_CREDENTIAL_CATEGORY, P)

  # Count nulls by category
  nulls_by_cat <- no_gender %>%
    count(PSI_CREDENTIAL_CATEGORY, name = "NULLCOUNT")

  # Compute number of females to assign per category
  top_nf <- female_pct %>%
    inner_join(nulls_by_cat, by = "PSI_CREDENTIAL_CATEGORY") %>%
    mutate(N = round(P * NULLCOUNT))

  top_nm <- male_pct %>%
    inner_join(nulls_by_cat, by = "PSI_CREDENTIAL_CATEGORY") %>%
    mutate(N = round(P * NULLCOUNT))

  # Assign genders: for each category, mark first N records as Female, etc.
  no_gender_unique <- no_gender %>%
    group_by(ENCRYPTED_TRUE_PEN, PSI_CREDENTIAL_CATEGORY) %>%
    slice(1) %>%
    ungroup()

  # Build assignment: for each category, assign Female to top_nf, Male to top_nm,
  # rest as Gender Diverse
  no_gender_assigned <- no_gender_unique %>%
    arrange(PSI_CREDENTIAL_CATEGORY, ENCRYPTED_TRUE_PEN) %>%
    group_by(PSI_CREDENTIAL_CATEGORY) %>%
    mutate(row_num = row_number()) %>%
    ungroup() %>%
    left_join(top_nf %>% select(PSI_CREDENTIAL_CATEGORY, NF = N),
              by = "PSI_CREDENTIAL_CATEGORY") %>%
    left_join(top_nm %>% select(PSI_CREDENTIAL_CATEGORY, NM = N),
              by = "PSI_CREDENTIAL_CATEGORY") %>%
    mutate(
      NF = coalesce(NF, 0),
      NM = coalesce(NM, 0),
      PSI_GENDER_CLEANED = case_when(
        row_num <= NF ~ "Female",
        row_num <= NF + NM ~ "Male",
        TRUE ~ "Gender Diverse"
      )
    ) %>%
    select(ENCRYPTED_TRUE_PEN, PSI_CREDENTIAL_CATEGORY, PSI_GENDER_CLEANED)

  # Apply to Credential_Non_Dup
  Credential_Non_Dup <- Credential_Non_Dup %>%
    left_join(no_gender_assigned,
              by = c("ENCRYPTED_TRUE_PEN", "PSI_CREDENTIAL_CATEGORY"),
              suffix = c("", "_imputed")) %>%
    mutate(
      PSI_GENDER_CLEANED = if_else(
        is_blank(PSI_GENDER_CLEANED), PSI_GENDER_CLEANED_imputed, PSI_GENDER_CLEANED
      )
    ) %>%
    select(-PSI_GENDER_CLEANED_imputed)
}


# ******************************************************************************
# Section 11: Credential ranking
# WHY: For EPENs with multiple credentials, identify the highest credential by
# date and by rank. Non-duplicated credentials (single per EPEN) get 'Yes' for both.
# Original: Multi-step process with views and temp tables, then R-based ranking.
# ******************************************************************************

credential_rank <- sch_tbl("CredentialRank") %>% collect() |> rename_with(toupper)

# EPENs with more than one credential (valid EPEN)
multi_cred_by_epen <- Credential_Non_Dup %>%
  filter(!is_blank(ENCRYPTED_TRUE_PEN)) %>%
  count(ENCRYPTED_TRUE_PEN) %>%
  filter(n > 1)

# PSI_CODE/STUDENT_NUMBER combos with more than one credential (null/blank EPEN)
multi_cred_by_stuid <- Credential_Non_Dup %>%
  filter(is_blank(ENCRYPTED_TRUE_PEN)) %>%
  count(ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER) %>%
  filter(n > 1)

# Build ranking dataset — all records for multi-credential EPENs or STUDENT_NUMBERs
ranking_records <- Credential_Non_Dup %>%
  inner_join(credential_rank %>% select(PSI_CREDENTIAL_CATEGORY, RANK),
             by = "PSI_CREDENTIAL_CATEGORY") %>%
  filter(
    (ENCRYPTED_TRUE_PEN %in% multi_cred_by_epen$ENCRYPTED_TRUE_PEN) |
      (is_blank(ENCRYPTED_TRUE_PEN) &
         paste0(ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER) %in%
         paste0(multi_cred_by_stuid$ENCRYPTED_TRUE_PEN, multi_cred_by_stuid$PSI_STUDENT_NUMBER))
  ) %>%
  select(ID, ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE,
         CREDENTIAL_AWARD_DATE_D, RANK)

# Compute highest by date and by rank using R (same as original)
res <- ranking_records %>%
  select(ID, ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE,
         CREDENTIAL_AWARD_DATE_D, RANK) %>%
  mutate(
    HIGHEST_CRED_BY_DATE = NA_character_,
    HIGHEST_CRED_BY_RANK = NA_character_
  )

# Highest by date: first record sorted by date desc, then rank
res <- res %>%
  group_by(ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER) %>%
  arrange(PSI_CODE, desc(CREDENTIAL_AWARD_DATE_D), RANK, .by_group = TRUE) %>%
  mutate(HIGHEST_CRED_BY_DATE = replace(HIGHEST_CRED_BY_DATE, 1, "Yes")) %>%
  ungroup()

# Highest by rank: first record sorted by rank, then date desc
res <- res %>%
  group_by(ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER) %>%
  arrange(PSI_CODE, RANK, desc(CREDENTIAL_AWARD_DATE_D), .by_group = TRUE) %>%
  mutate(HIGHEST_CRED_BY_RANK = replace(HIGHEST_CRED_BY_RANK, 1, "Yes")) %>%
  ungroup()

# Apply rankings to Credential_Non_Dup
ranking_result <- res %>%
  select(ID, HIGHEST_CRED_BY_DATE, HIGHEST_CRED_BY_RANK)

Credential_Non_Dup <- Credential_Non_Dup %>%
  left_join(ranking_result, by = "ID", suffix = c("", "_ranked")) %>%
  mutate(
    HIGHEST_CRED_BY_DATE = coalesce(HIGHEST_CRED_BY_DATE_ranked, HIGHEST_CRED_BY_DATE),
    HIGHEST_CRED_BY_RANK = coalesce(HIGHEST_CRED_BY_RANK_ranked, HIGHEST_CRED_BY_RANK)
  ) %>%
  select(-HIGHEST_CRED_BY_DATE_ranked, -HIGHEST_CRED_BY_RANK_ranked)

# Non-multi-credential records get 'Yes' for both
Credential_Non_Dup <- Credential_Non_Dup %>%
  mutate(
    HIGHEST_CRED_BY_DATE = if_else(
      !ID %in% ranking_result$ID & is.na(HIGHEST_CRED_BY_DATE),
      "Yes", HIGHEST_CRED_BY_DATE
    ),
    HIGHEST_CRED_BY_RANK = if_else(
      !ID %in% ranking_result$ID & is.na(HIGHEST_CRED_BY_RANK),
      "Yes", HIGHEST_CRED_BY_RANK
    )
  )


# ******************************************************************************
# Section 12: Age imputation
# WHY: Some credentials are missing age at graduation. We impute ages based on
# the observed age distribution within each gender/credential category.
# ******************************************************************************

cred_no_age <- Credential_Non_Dup %>%
  filter(is.na(AGE_AT_GRAD)) %>%
  select(ID, ENCRYPTED_TRUE_PEN, PSI_GENDER_CLEANED, PSI_AWARD_SCHOOL_YEAR,
         PSI_CREDENTIAL_CATEGORY, CREDENTIAL_AWARD_DATE_D)

cred_no_age_unique <- Credential_Non_Dup %>%
  filter(is.na(AGE_AT_GRAD), HIGHEST_CRED_BY_DATE == "Yes") %>%
  select(ID, ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE,
         PSI_GENDER_CLEANED, PSI_CREDENTIAL_CATEGORY, CREDENTIAL_AWARD_DATE_D,
         PSI_AWARD_SCHOOL_YEAR)

if (nrow(cred_no_age_unique) > 0) {
  # Count nulls per gender/category
  null_age_count <- cred_no_age_unique %>%
    count(PSI_GENDER_CLEANED, PSI_CREDENTIAL_CATEGORY, name = "NUMWITHNULLAGE")

  # Observed age distribution (from records with known age, highest by date only)
  age_dist <- Credential_Non_Dup %>%
    filter(!is.na(AGE_GROUP_AT_GRAD), HIGHEST_CRED_BY_DATE == "Yes") %>%
    count(PSI_GENDER_CLEANED, AGE_AT_GRAD, PSI_CREDENTIAL_CATEGORY, name = "NUMGRADS")

  # Compute proportions
  age_dist_props <- age_dist %>%
    group_by(PSI_GENDER_CLEANED, PSI_CREDENTIAL_CATEGORY) %>%
    mutate(P = NUMGRADS / sum(NUMGRADS)) %>%
    ungroup()

  # Join with null counts to get number to assign per age value
  age_assignments <- age_dist_props %>%
    inner_join(null_age_count, by = c("PSI_GENDER_CLEANED", "PSI_CREDENTIAL_CATEGORY")) %>%
    mutate(N = round(P * NUMWITHNULLAGE)) %>%
    arrange(PSI_GENDER_CLEANED, PSI_CREDENTIAL_CATEGORY, AGE_AT_GRAD)

  # Apply age imputation: for each gender/category, assign ages proportionally
  imputed_ages <- cred_no_age_unique %>%
    arrange(PSI_GENDER_CLEANED, PSI_CREDENTIAL_CATEGORY, ID) %>%
    group_by(PSI_GENDER_CLEANED, PSI_CREDENTIAL_CATEGORY) %>%
    mutate(row_num = row_number()) %>%
    ungroup()

  # Build cumulative age assignments
  age_assign_cumul <- age_assignments %>%
    arrange(PSI_GENDER_CLEANED, PSI_CREDENTIAL_CATEGORY, AGE_AT_GRAD) %>%
    group_by(PSI_GENDER_CLEANED, PSI_CREDENTIAL_CATEGORY) %>%
    mutate(cum_n = cumsum(N)) %>%
    ungroup() %>%
    select(PSI_GENDER_CLEANED, PSI_CREDENTIAL_CATEGORY, AGE_AT_GRAD, cum_n)

  # Assign age by matching row_num to cumulative thresholds
  imputed_ages <- imputed_ages %>%
    left_join(
      age_assign_cumul,
      by = c("PSI_GENDER_CLEANED", "PSI_CREDENTIAL_CATEGORY")
    ) %>%
    group_by(PSI_GENDER_CLEANED, PSI_CREDENTIAL_CATEGORY, ID) %>%
    filter(row_num <= first(cum_n)) %>%
    slice(1) %>%
    ungroup() %>%
    select(ID, AGE_AT_GRAD_imputed = AGE_AT_GRAD)

  # Apply to Credential_Non_Dup
  Credential_Non_Dup <- Credential_Non_Dup %>%
    left_join(imputed_ages, by = "ID") %>%
    mutate(
      AGE_AT_GRAD = if_else(is.na(AGE_AT_GRAD), AGE_AT_GRAD_imputed, AGE_AT_GRAD)
    ) %>%
    select(-AGE_AT_GRAD_imputed)

  # For remaining nulls, assign random age 19-54
  remaining_null <- Credential_Non_Dup %>%
    filter(is.na(AGE_AT_GRAD))

  if (nrow(remaining_null) > 0) {
    set.seed(42)
    Credential_Non_Dup <- Credential_Non_Dup %>%
      mutate(
        AGE_AT_GRAD = if_else(
          is.na(AGE_AT_GRAD),
          sample(19:54, n(), replace = TRUE),
          AGE_AT_GRAD
        )
      )
  }
}

# Reassign age groups after imputation
Credential_Non_Dup <- Credential_Non_Dup %>%
  mutate(AGE_GROUP_AT_GRAD = NA_integer_)

for (i in seq_len(nrow(age_group_lookup))) {
  lb <- age_group_lookup$LOWERBOUND[i]
  ub <- age_group_lookup$UPPERBOUND[i]
  idx <- age_group_lookup$AGEINDEX[i]
  Credential_Non_Dup <- Credential_Non_Dup %>%
    mutate(
      AGE_GROUP_AT_GRAD = if_else(
        !is.na(AGE_AT_GRAD) & AGE_AT_GRAD >= lb & AGE_AT_GRAD <= ub,
        idx, AGE_GROUP_AT_GRAD
      )
    )
}


# ******************************************************************************
# Section 13: Visa status cleaning
# WHY: Fill missing visa status in Credential_Non_Dup from CredentialSupVarsFromEnrolment
# using progressively looser matching criteria (all fields → partial fields → EPEN+code+year).
# ******************************************************************************

# Start with visa status from CredentialSupVars
visa_from_supvars <- CredentialSupVars %>%
  select(ID, PSI_VISA_STATUS)

# Get visa from enrolment with multiple join strategies
# WHY: Enrolment records may match on different field combinations. We try the most
# specific match first, then fall back to broader matches.

# Strategy 1: Match on all fields (EPEN, code, student_number, program_code, description, school_year)
visa_enrol_full <- CredentialSupVarsFromEnrolment %>%
  select(ENCRYPTED_TRUE_PEN, PSI_CODE, PSI_STUDENT_NUMBER,
         PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
         PSI_SCHOOL_YEAR, PSI_VISA_STATUS) %>%
  distinct()

# Build Non_Dup visa cleaning
cred_nd_visa <- Credential_Non_Dup %>%
  left_join(visa_from_supvars, by = "ID", suffix = c("", "_sv")) %>%
  mutate(PSI_VISA_STATUS = PSI_VISA_STATUS_sv) %>%
  select(-PSI_VISA_STATUS_sv)

# Fill from enrolment — first by full match
cred_nd_visa <- cred_nd_visa %>%
  left_join(
    visa_enrol_full %>%
      filter(!is.na(PSI_VISA_STATUS)) %>%
      rename(VISA_ENROL = PSI_VISA_STATUS),
    by = c("ENCRYPTED_TRUE_PEN", "PSI_CODE", "PSI_STUDENT_NUMBER",
           "PSI_PROGRAM_CODE", "PSI_CREDENTIAL_PROGRAM_DESCRIPTION",
           "PSI_SCHOOL_YEAR")
  ) %>%
  mutate(PSI_VISA_STATUS = coalesce(PSI_VISA_STATUS, VISA_ENROL)) %>%
  select(-VISA_ENROL)

# Strategy 2: Match on EPEN + code + student_number + school_year (for remaining nulls)
cred_nd_visa <- cred_nd_visa %>%
  left_join(
    CredentialSupVarsFromEnrolment %>%
      filter(!is.na(PSI_VISA_STATUS)) %>%
      select(ENCRYPTED_TRUE_PEN, PSI_CODE, PSI_STUDENT_NUMBER, PSI_SCHOOL_YEAR, PSI_VISA_STATUS) %>%
      distinct() %>%
      rename(VISA_ENROL2 = PSI_VISA_STATUS),
    by = c("ENCRYPTED_TRUE_PEN", "PSI_CODE", "PSI_STUDENT_NUMBER", "PSI_SCHOOL_YEAR")
  ) %>%
  mutate(PSI_VISA_STATUS = if_else(
    is.na(PSI_VISA_STATUS), VISA_ENROL2, PSI_VISA_STATUS
  )) %>%
  select(-VISA_ENROL2)

# Update CredentialSupVars from the cleaned Non_Dup visa
CredentialSupVars <- CredentialSupVars %>%
  left_join(
    cred_nd_visa %>% select(ID, PSI_VISA_STATUS_ND = PSI_VISA_STATUS),
    by = "ID"
  ) %>%
  mutate(
    PSI_VISA_STATUS = if_else(
      is.na(PSI_VISA_STATUS) | PSI_VISA_STATUS %in% c("", " ", "(Unspecified)"),
      PSI_VISA_STATUS_ND, PSI_VISA_STATUS
    )
  ) %>%
  select(-PSI_VISA_STATUS_ND)

# Update Credential_Non_Dup
Credential_Non_Dup <- Credential_Non_Dup %>%
  left_join(
    CredentialSupVars %>% select(ID, PSI_VISA_STATUS_SV = PSI_VISA_STATUS),
    by = "ID"
  ) %>%
  mutate(PSI_VISA_STATUS = PSI_VISA_STATUS_SV) %>%
  select(-PSI_VISA_STATUS_SV)


# ******************************************************************************
# Section 14: Delay date calculation
# WHY: Some credentials are superseded by later credentials within a time window.
# The "delay date" is the date of the later credential, used to shift the award
# date for projection purposes. Time windows vary by credential category.
# ******************************************************************************

# Build concatenated ID for grouping
# WHY: EPEN is the primary identifier; for null/blank EPENs, use PSI_CODE + STUDENT_NUMBER.
Credential_Non_Dup <- Credential_Non_Dup %>%
  mutate(
    CONCATENATED_ID = if_else(
      !is_blank(ENCRYPTED_TRUE_PEN), ENCRYPTED_TRUE_PEN,
      paste0(PSI_STUDENT_NUMBER, PSI_CODE)
    )
  )

# Build tblCredential_HighestRank — highest-ranked credentials with SupVars
tblCred_HighestRank <- Credential_Non_Dup %>%
  filter(HIGHEST_CRED_BY_RANK == "Yes") %>%
  inner_join(
    CredentialSupVars %>%
      select(ID, CREDENTIAL_AWARD_DATE_D_DELAYED, PSI_AWARD_SCHOOL_YEAR_DELAYED, PSI_VISA_STATUS),
    by = "ID"
  )

# Find later-awarded credentials for the same concatenated_id
# WHY: For each highest-rank credential, find other credentials with the same ID
# group that were awarded later. Apply time windows by credential category.
later_awarded <- Credential_Non_Dup %>%
  inner_join(
    credential_rank %>% select(PSI_CREDENTIAL_CATEGORY, RANK),
    by = "PSI_CREDENTIAL_CATEGORY"
  ) %>%
  inner_join(
    tblCred_HighestRank %>%
      select(HID = ID, CONCATENATED_ID, HIGHEST_AWARD_DATE = CREDENTIAL_AWARD_DATE_D),
    by = "CONCATENATED_ID"
  ) %>%
  filter(CREDENTIAL_AWARD_DATE_D > HIGHEST_AWARD_DATE) %>%
  rename(LID = ID, LATER_AWARD_DATE = CREDENTIAL_AWARD_DATE_D)

# Apply time windows per credential category
# WHY: Not all later credentials count as delays. Different categories have different
# windows (e.g., certificates: 18 months, diplomas: 30 months, degrees: unlimited).
later_awarded_filtered <- later_awarded %>%
  mutate(
    MONTHS_DIFF = as.numeric(difftime(LATER_AWARD_DATE, HIGHEST_AWARD_DATE, units = "days")) / 30.44
  ) %>%
  filter(
    PSI_CREDENTIAL_CATEGORY %in% c("APPRENTICESHIP", "BACHELORS DEGREE", "FIRST PROFESSIONAL DEGREE") |
      (PSI_CREDENTIAL_CATEGORY %in% c("ADVANCED DIPLOMA", "ADVANCED CERTIFICATE") & MONTHS_DIFF <= 36) |
      (PSI_CREDENTIAL_CATEGORY == "ASSOCIATE DEGREE" & MONTHS_DIFF <= 18) |
      (PSI_CREDENTIAL_CATEGORY == "CERTIFICATE" & MONTHS_DIFF <= 18) |
      (PSI_CREDENTIAL_CATEGORY == "DIPLOMA" & MONTHS_DIFF <= 30) |
      (PSI_CREDENTIAL_CATEGORY == "MASTERS DEGREE" & MONTHS_DIFF <= 30) |
      (PSI_CREDENTIAL_CATEGORY == "GRADUATE CERTIFICATE" & MONTHS_DIFF <= 18) |
      (PSI_CREDENTIAL_CATEGORY == "GRADUATE DIPLOMA" & MONTHS_DIFF <= 30) |
      (PSI_CREDENTIAL_CATEGORY == "POST-DEGREE CERTIFICATE" & MONTHS_DIFF <= 18) |
      (PSI_CREDENTIAL_CATEGORY == "POST-DEGREE DIPLOMA" & MONTHS_DIFF <= 30)
  )

# Find the max later award date per concatenated_id
delay_effect <- later_awarded_filtered %>%
  group_by(CONCATENATED_ID) %>%
  slice_max(LATER_AWARD_DATE, n = 1, with_ties = FALSE) %>%
  ungroup() %>%
  select(LID, HID, CONCATENATED_ID, LATER_AWARD_DATE, PSI_AWARD_SCHOOL_YEAR)

# Update delay dates in tblCred_HighestRank
tblCred_HighestRank <- tblCred_HighestRank %>%
  left_join(
    delay_effect %>% select(HID, DELAY_DATE = LATER_AWARD_DATE, DELAY_YEAR = PSI_AWARD_SCHOOL_YEAR),
    by = c("ID" = "HID")
  ) %>%
  mutate(
    CREDENTIAL_AWARD_DATE_D_DELAYED = coalesce(DELAY_DATE, CREDENTIAL_AWARD_DATE_D_DELAYED),
    PSI_AWARD_SCHOOL_YEAR_DELAYED = coalesce(DELAY_YEAR, PSI_AWARD_SCHOOL_YEAR_DELAYED)
  ) %>%
  select(-DELAY_DATE, -DELAY_YEAR)

# Default: if no delay date, use original award date
tblCred_HighestRank <- tblCred_HighestRank %>%
  mutate(
    CREDENTIAL_AWARD_DATE_D_DELAYED = if_else(
      is.na(CREDENTIAL_AWARD_DATE_D_DELAYED), CREDENTIAL_AWARD_DATE_D,
      CREDENTIAL_AWARD_DATE_D_DELAYED
    ),
    PSI_AWARD_SCHOOL_YEAR_DELAYED = if_else(
      is.na(PSI_AWARD_SCHOOL_YEAR_DELAYED), PSI_AWARD_SCHOOL_YEAR,
      PSI_AWARD_SCHOOL_YEAR_DELAYED
    )
  )

# Apply delay dates back to Credential_Non_Dup
Credential_Non_Dup <- Credential_Non_Dup %>%
  mutate(
    CREDENTIAL_AWARD_DATE_D_DELAYED = NA_Date_,
    PSI_AWARD_SCHOOL_YEAR_DELAYED = NA_character_
  ) %>%
  left_join(
    tblCred_HighestRank %>%
      filter(!is.na(CREDENTIAL_AWARD_DATE_D_DELAYED)) %>%
      select(ID, DELAY_D = CREDENTIAL_AWARD_DATE_D_DELAYED,
             DELAY_YR = PSI_AWARD_SCHOOL_YEAR_DELAYED),
    by = "ID"
  ) %>%
  mutate(
    CREDENTIAL_AWARD_DATE_D_DELAYED = coalesce(DELAY_D, CREDENTIAL_AWARD_DATE_D),
    PSI_AWARD_SCHOOL_YEAR_DELAYED = coalesce(DELAY_YR, PSI_AWARD_SCHOOL_YEAR)
  ) %>%
  select(-DELAY_D, -DELAY_YR)


# ******************************************************************************
# Section 15: Research university + Outcome credential flags
# WHY: Flag credentials from research universities (used for DACSO exclusion) and
# assign outcome credential labels for categorization.
# ******************************************************************************

research_unis <- c("SFU", "UBC", "UBCV", "UBCO", "UNBC", "UVIC", "RRU")

Credential_Non_Dup <- Credential_Non_Dup %>%
  mutate(RESEARCH_UNIVERSITY = if_else(PSI_CODE %in% research_unis, 1L, NA_integer_))

outcome_credential <- sch_tbl("OutcomeCredential") %>% collect() |> rename_with(toupper)

Credential_Non_Dup <- Credential_Non_Dup %>%
  left_join(
    outcome_credential %>% select(PSI_CREDENTIAL_CATEGORY, OUTCOMES_CRED_OUT = OUTCOMES_CRED),
    by = "PSI_CREDENTIAL_CATEGORY"
  ) %>%
  mutate(OUTCOMES_CRED = OUTCOMES_CRED_OUT) %>%
  select(-OUTCOMES_CRED_OUT)


# ******************************************************************************
# Section 16: Final distributions
# WHY: Build credential count tables by year/age group/credential category with
# various filters (domestic, exclude CIPs, exclude RU+DACSO). These feed into
# the graduate projection model (04) and program projections (06).
# ******************************************************************************

# Build base table from HighestRank joined with AgeGroupLookup and Credential_Non_Dup
# for FINAL_CIP_CLUSTER_CODE access
base_dist <- tblCred_HighestRank %>%
  inner_join(
    age_group_lookup %>% select(AGEINDEX, AGEGROUP),
    by = c("AGE_GROUP_AT_GRAD" = "AGEINDEX")
  )

base_dist_with_cip <- base_dist %>%
  left_join(
    Credential_Non_Dup %>%
      select(ID, FINAL_CIP_CLUSTER_CODE, FINAL_CIP_CODE_4, RESEARCH_UNIVERSITY,
             OUTCOMES_CRED, PSI_VISA_STATUS),
    by = "ID"
  )

# Helper function to build a Credential_By_Year table
# WHY: All 8 distribution tables follow the same pattern: filter + group_by + count.
# This helper reduces code duplication.
build_cred_by_year <- function(data, group_vars, filter_expr = TRUE) {
  data %>%
    filter({{ filter_expr }}) %>%
    group_by(across(all_of(group_vars))) %>%
    summarise(COUNT = n(), .groups = "drop")
}

# Common filters
no_apprenticeship <- expr(PSI_CREDENTIAL_CATEGORY != "APPRENTICESHIP")
is_domestic <- expr(PSI_VISA_STATUS == "DOMESTIC" | is.na(PSI_VISA_STATUS))
exclude_cip_09_10 <- expr(FINAL_CIP_CLUSTER_CODE != "09" & FINAL_CIP_CLUSTER_CODE != "10")
exclude_ru_dacso <- expr(
  (RESEARCH_UNIVERSITY == 1 & OUTCOMES_CRED != "DACSO") | is.na(RESEARCH_UNIVERSITY)
)

# 1. Credential_By_Year_AgeGroup
Credential_By_Year_AgeGroup <- base_dist %>%
  filter(PSI_CREDENTIAL_CATEGORY != "APPRENTICESHIP") %>%
  mutate(EXPR1 = paste0(PSI_CREDENTIAL_CATEGORY, AGEGROUP)) %>%
  count(AGEGROUP, PSI_CREDENTIAL_CATEGORY, EXPR1, PSI_AWARD_SCHOOL_YEAR_DELAYED, name = "COUNT") %>%
  arrange(AGEGROUP, PSI_CREDENTIAL_CATEGORY, PSI_AWARD_SCHOOL_YEAR_DELAYED)

# 2. Credential_By_Year_AgeGroup_Exclude_CIPs
Credential_By_Year_AgeGroup_Exclude_CIPs <- base_dist_with_cip %>%
  filter(PSI_CREDENTIAL_CATEGORY != "APPRENTICESHIP",
         FINAL_CIP_CLUSTER_CODE != "09", FINAL_CIP_CLUSTER_CODE != "10") %>%
  mutate(EXPR1 = paste0(PSI_CREDENTIAL_CATEGORY, AGEGROUP)) %>%
  count(AGEGROUP, PSI_CREDENTIAL_CATEGORY, EXPR1, PSI_AWARD_SCHOOL_YEAR_DELAYED, name = "COUNT") %>%
  arrange(AGEGROUP, PSI_CREDENTIAL_CATEGORY, PSI_AWARD_SCHOOL_YEAR_DELAYED)

# 3. Credential_By_Year_AgeGroup_Domestic
Credential_By_Year_AgeGroup_Domestic <- base_dist %>%
  filter(PSI_CREDENTIAL_CATEGORY != "APPRENTICESHIP",
         PSI_VISA_STATUS == "DOMESTIC" | is.na(PSI_VISA_STATUS)) %>%
  mutate(EXPR1 = paste0(PSI_CREDENTIAL_CATEGORY, AGEGROUP)) %>%
  count(AGEGROUP, PSI_CREDENTIAL_CATEGORY, EXPR1, PSI_AWARD_SCHOOL_YEAR_DELAYED, name = "COUNT") %>%
  arrange(AGEGROUP, PSI_CREDENTIAL_CATEGORY, PSI_AWARD_SCHOOL_YEAR_DELAYED)

# 4. Credential_By_Year_AgeGroup_Domestic_Exclude_CIPs
Credential_By_Year_AgeGroup_Domestic_Exclude_CIPs <- base_dist_with_cip %>%
  filter(PSI_CREDENTIAL_CATEGORY != "APPRENTICESHIP",
         (PSI_VISA_STATUS == "DOMESTIC" | is.na(PSI_VISA_STATUS)),
         FINAL_CIP_CLUSTER_CODE != "09", FINAL_CIP_CLUSTER_CODE != "10") %>%
  mutate(EXPR1 = paste0(PSI_CREDENTIAL_CATEGORY, AGEGROUP)) %>%
  count(AGEGROUP, PSI_CREDENTIAL_CATEGORY, EXPR1, PSI_AWARD_SCHOOL_YEAR_DELAYED, name = "COUNT") %>%
  arrange(AGEGROUP, PSI_CREDENTIAL_CATEGORY, PSI_AWARD_SCHOOL_YEAR_DELAYED)

# 5. Credential_By_Year_AgeGroup_Domestic_Exclude_RU_DACSO
Credential_By_Year_AgeGroup_Domestic_Exclude_RU_DACSO <- base_dist_with_cip %>%
  filter(PSI_CREDENTIAL_CATEGORY != "APPRENTICESHIP",
         (PSI_VISA_STATUS == "DOMESTIC" | is.na(PSI_VISA_STATUS)),
         (RESEARCH_UNIVERSITY == 1 & OUTCOMES_CRED != "DACSO") | is.na(RESEARCH_UNIVERSITY)) %>%
  mutate(EXPR1 = paste0(PSI_CREDENTIAL_CATEGORY, AGEGROUP)) %>%
  count(AGEGROUP, PSI_CREDENTIAL_CATEGORY, EXPR1, PSI_AWARD_SCHOOL_YEAR_DELAYED, name = "COUNT") %>%
  arrange(AGEGROUP, PSI_CREDENTIAL_CATEGORY, PSI_AWARD_SCHOOL_YEAR_DELAYED)

# 6. Credential_By_Year_CIP4_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs
Credential_By_Year_CIP4_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs <- base_dist_with_cip %>%
  filter(PSI_CREDENTIAL_CATEGORY != "APPRENTICESHIP",
         (PSI_VISA_STATUS == "DOMESTIC" | is.na(PSI_VISA_STATUS)),
         ((RESEARCH_UNIVERSITY == 1 & OUTCOMES_CRED != "DACSO") | is.na(RESEARCH_UNIVERSITY)),
         FINAL_CIP_CLUSTER_CODE != "09", FINAL_CIP_CLUSTER_CODE != "10") %>%
  mutate(EXPR1 = paste0(PSI_CREDENTIAL_CATEGORY, AGEGROUP)) %>%
  count(AGEGROUP, PSI_CREDENTIAL_CATEGORY, EXPR1, FINAL_CIP_CODE_4,
        PSI_AWARD_SCHOOL_YEAR_DELAYED, name = "COUNT") %>%
  arrange(AGEGROUP, PSI_CREDENTIAL_CATEGORY, FINAL_CIP_CODE_4, PSI_AWARD_SCHOOL_YEAR_DELAYED)

# 7. Credential_By_Year_CIP4_Gender_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs
Credential_By_Year_CIP4_Gender_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs <- base_dist_with_cip %>%
  filter(PSI_CREDENTIAL_CATEGORY != "APPRENTICESHIP",
         (PSI_VISA_STATUS == "DOMESTIC" | is.na(PSI_VISA_STATUS)),
         ((RESEARCH_UNIVERSITY == 1 & OUTCOMES_CRED != "DACSO") | is.na(RESEARCH_UNIVERSITY)),
         FINAL_CIP_CLUSTER_CODE != "09", FINAL_CIP_CLUSTER_CODE != "10") %>%
  mutate(EXPR1 = paste0(PSI_CREDENTIAL_CATEGORY, AGEGROUP, PSI_GENDER_CLEANED)) %>%
  count(PSI_GENDER_CLEANED, AGEGROUP, PSI_CREDENTIAL_CATEGORY, EXPR1, FINAL_CIP_CODE_4,
        PSI_AWARD_SCHOOL_YEAR_DELAYED, name = "COUNT") %>%
  arrange(AGEGROUP, PSI_CREDENTIAL_CATEGORY, PSI_AWARD_SCHOOL_YEAR_DELAYED, desc(PSI_GENDER_CLEANED))

# 8. Credential_By_Year_Gender_AgeGroup_Domestic_Exclude_CIPs
Credential_By_Year_Gender_AgeGroup_Domestic_Exclude_CIPs <- base_dist_with_cip %>%
  filter(PSI_CREDENTIAL_CATEGORY != "APPRENTICESHIP",
         (PSI_VISA_STATUS == "DOMESTIC" | is.na(PSI_VISA_STATUS)),
         FINAL_CIP_CLUSTER_CODE != "09", FINAL_CIP_CLUSTER_CODE != "10") %>%
  mutate(EXPR1 = paste0(PSI_CREDENTIAL_CATEGORY, AGEGROUP, PSI_GENDER_CLEANED)) %>%
  count(PSI_GENDER_CLEANED, AGEGROUP, PSI_CREDENTIAL_CATEGORY, EXPR1,
        PSI_AWARD_SCHOOL_YEAR_DELAYED, name = "COUNT") %>%
  arrange(AGEGROUP, PSI_CREDENTIAL_CATEGORY, PSI_AWARD_SCHOOL_YEAR_DELAYED, desc(PSI_GENDER_CLEANED))

# 9. Credential_By_Year_Gender_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs
Credential_By_Year_Gender_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs <- base_dist_with_cip %>%
  filter(PSI_CREDENTIAL_CATEGORY != "APPRENTICESHIP",
         (PSI_VISA_STATUS == "DOMESTIC" | is.na(PSI_VISA_STATUS)),
         ((RESEARCH_UNIVERSITY == 1 & OUTCOMES_CRED != "DACSO") | is.na(RESEARCH_UNIVERSITY)),
         FINAL_CIP_CLUSTER_CODE != "09", FINAL_CIP_CLUSTER_CODE != "10") %>%
  mutate(EXPR1 = paste0(PSI_CREDENTIAL_CATEGORY, AGEGROUP, PSI_GENDER_CLEANED)) %>%
  count(PSI_GENDER_CLEANED, AGEGROUP, PSI_CREDENTIAL_CATEGORY, EXPR1,
        PSI_AWARD_SCHOOL_YEAR_DELAYED, name = "COUNT") %>%
  arrange(AGEGROUP, PSI_CREDENTIAL_CATEGORY, PSI_AWARD_SCHOOL_YEAR_DELAYED, desc(PSI_GENDER_CLEANED))

# 10. Checking table
Checking_Excluding_RU_DACSO_Variables <- Credential_Non_Dup %>%
  filter(RESEARCH_UNIVERSITY == 1, OUTCOMES_CRED == "DACSO",
         PSI_AWARD_SCHOOL_YEAR_DELAYED == "2018/2019") %>%
  count(RESEARCH_UNIVERSITY, PSI_CODE, OUTCOMES_CRED, PSI_CREDENTIAL_CATEGORY,
        PSI_AWARD_SCHOOL_YEAR_DELAYED, name = "EXPR1") %>%
  arrange(OUTCOMES_CRED, RESEARCH_UNIVERSITY)


# ******************************************************************************
# Section 17: Write all output tables to database
# WHY: Persist results for downstream scripts. CredentialSupVars and
# Credential_Non_Dup are the primary outputs used by 02a-02b pipeline scripts.
# ******************************************************************************

write_schema_table <- function(name, data) {
  dbWriteTable(con, SQL(glue::glue('"{my_schema}"."{name}"')), data, overwrite = TRUE)
}

# Write main tables
write_schema_table("CredentialSupVars", CredentialSupVars)
write_schema_table("CredentialSupVarsFromEnrolment", CredentialSupVarsFromEnrolment)
write_schema_table("Credential_Non_Dup", Credential_Non_Dup)
write_schema_table("tblCredential_HighestRank", tblCred_HighestRank)

# Update STP_Credential_Record_Type with exclusion flags
# KEPT AS SQL: ALTER TABLE (DDL) and UPDATE on existing table
dbExecute(con, glue::glue(
  "IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('[{my_schema}].[STP_Credential_Record_Type]') AND name = 'DropCredCategory') ",
  "ALTER TABLE [{my_schema}].[STP_Credential_Record_Type] ADD DropCredCategory NVARCHAR(50) NULL"
))
dbExecute(con, glue::glue(
  "UPDATE [{my_schema}].[STP_Credential_Record_Type] SET DropCredCategory = NULL"
))
for (id in cred_ids_to_drop$ID) {
  dbExecute(con, glue::glue(
    "UPDATE [{my_schema}].[STP_Credential_Record_Type] SET DropCredCategory = 'Yes' WHERE ID = {id}"
  ))
}

dbExecute(con, glue::glue(
  "IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('[{my_schema}].[STP_Credential_Record_Type]') AND name = 'DropPartialYear') ",
  "ALTER TABLE [{my_schema}].[STP_Credential_Record_Type] ADD DropPartialYear NVARCHAR(50) NULL"
))
dbExecute(con, glue::glue(
  "UPDATE [{my_schema}].[STP_Credential_Record_Type] SET DropPartialYear = NULL"
))
for (id in cred_ids_partial$ID) {
  dbExecute(con, glue::glue(
    "UPDATE [{my_schema}].[STP_Credential_Record_Type] SET DropPartialYear = 'Yes' WHERE ID = {id}"
  ))
}

# Write distribution tables
write_schema_table("Credential_By_Year_AgeGroup", Credential_By_Year_AgeGroup)
write_schema_table("Credential_By_Year_AgeGroup_Exclude_CIPs", Credential_By_Year_AgeGroup_Exclude_CIPs)
write_schema_table("Credential_By_Year_AgeGroup_Domestic", Credential_By_Year_AgeGroup_Domestic)
write_schema_table("Credential_By_Year_AgeGroup_Domestic_Exclude_CIPs", Credential_By_Year_AgeGroup_Domestic_Exclude_CIPs)
write_schema_table("Credential_By_Year_AgeGroup_Domestic_Exclude_RU_DACSO", Credential_By_Year_AgeGroup_Domestic_Exclude_RU_DACSO)
write_schema_table("Credential_By_Year_CIP4_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs", Credential_By_Year_CIP4_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs)
write_schema_table("Credential_By_Year_CIP4_Gender_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs", Credential_By_Year_CIP4_Gender_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs)
write_schema_table("Credential_By_Year_Gender_AgeGroup_Domestic_Exclude_CIPs", Credential_By_Year_Gender_AgeGroup_Domestic_Exclude_CIPs)
write_schema_table("Credential_By_Year_Gender_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs", Credential_By_Year_Gender_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs)
write_schema_table("Checking_Excluding_RU_DACSO_Variables", Checking_Excluding_RU_DACSO_Variables)

# International flag update
# KEPT AS SQL: UPDATE with JOIN on existing table
dbExecute(con, glue::glue(
  "UPDATE [{my_schema}].[CredentialSupVars] ",
  "SET International_Include_Flag = [{my_schema}].[tbl_CredentialHighestRank_International].[International_Include_Flag] ",
  "FROM [{my_schema}].[CredentialSupVars] ",
  "INNER JOIN [{my_schema}].[tbl_CredentialHighestRank_International] ",
  "ON [{my_schema}].[CredentialSupVars].[ID] = [{my_schema}].[tbl_CredentialHighestRank_International].[ID];"
))


# ---- Clean up ----
dbDisconnect(con)



# ==============================================================================
# FILE: 01c-gender-cleaning.r
# ==============================================================================


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

# credential_supvars is the only table in the global environment that gets altered by this script.
# toggle to create a convenience backup (development only)
# credential_supvars.bk <- credential_supvars

credential_supvars_gender <- credential_supvars_enrolment |>
  distinct(
    ENCRYPTED_TRUE_PEN,
    PSI_GENDER
  )

# ------------------------------------------------------------------------------------
# MULTIPLE GENDERS
#  - Find all students with multiple recorded genders (in credential_supvars_enrolment).
#  - Choose one (select 1st after arranging by PSI_SCHOOL_YEAR, PSI_ENROLMENT_SEQUENCE)
#  - Map the resolved gender values back to credential_supvars_gender.
#  - Prioritize the corrected value, retain the original entry for all other records.
# ------------------------------------------------------------------------------------
# qry03f Cleaning 1 through 5
supvars_enrol_more_than_1 <- credential_supvars_enrolment |>
  group_by(ENCRYPTED_TRUE_PEN) |>
  filter(n_distinct(PSI_GENDER) > 1) |>
  slice_max(
    order_by = tibble(PSI_SCHOOL_YEAR, PSI_ENROLMENT_SEQUENCE),
    n = 1,
    with_ties = FALSE
  ) |>
  filter(
    !is.na(ENCRYPTED_TRUE_PEN),
    !(ENCRYPTED_TRUE_PEN %in% c("", " ", "(Unspecified)"))
  ) |>
  select(
    ENCRYPTED_TRUE_PEN,
    PSI_GENDER_To_Use = PSI_GENDER
  ) |>
  ungroup() |>
  distinct()

# qry03f_6 thorough 11 (redundant, removed)

# (qry03f_12, 13, & 14)
credential_supvars_gender <- credential_supvars_gender |>
  left_join(
    supvars_enrol_more_than_1 |>
      select(ENCRYPTED_TRUE_PEN, PSI_GENDER_To_Use),
    by = "ENCRYPTED_TRUE_PEN"
  ) |>
  mutate(
    psi_gender_cleaned_flag = if_else(
      !is.na(PSI_GENDER_To_Use),
      "Yes",
      NA_character_
    ),
    psi_gender_cleaned = coalesce(PSI_GENDER_To_Use, PSI_GENDER)
  ) |>
  select(-PSI_GENDER_To_Use)

rm(supvars_enrol_more_than_1)


# ------------------------------------------------------------------------------------
# UNKNOWNS
#  - Find records with "Unknown" genders remaining in credential_supvars_gender.
#  - Search credential_supvars_gender for another record containing valid gender.
#  - Resolve multiples by choosing the "first" gender (based on alphabetical order).
#  - Map the resolved gender values back to credential_supvars_gender.
#  - Prioritize the corrected value, retain the original entry for all other records.
# ------------------------------------------------------------------------------------
# qry03f 15, 17, 18, 19
gender_recovery_lookup <- credential_supvars_gender |>
  filter(psi_gender_cleaned %in% c("U", "Unknown")) |>
  inner_join(
    credential_supvars_enrolment |>
      filter(!PSI_GENDER %in% c("U", "Unknown")) |>
      select(ENCRYPTED_TRUE_PEN, ResolvedGender = PSI_GENDER),
    by = "ENCRYPTED_TRUE_PEN"
  ) |>
  filter(
    !is.na(ENCRYPTED_TRUE_PEN),
    !(ENCRYPTED_TRUE_PEN %in% c("", " ", "(Unspecified)"))
  ) |>
  group_by(ENCRYPTED_TRUE_PEN) |>
  slice_max(order_by = ResolvedGender, n = 1, with_ties = FALSE) |>
  ungroup() |>
  select(ENCRYPTED_TRUE_PEN, psi_gender_cleaned_NEW = ResolvedGender)

# qry03f 20 and 21 (redundant, removed)

credential_supvars_gender <- credential_supvars_gender |>
  left_join(gender_recovery_lookup, by = "ENCRYPTED_TRUE_PEN") |>
  mutate(
    psi_gender_cleaned = if_else(
      psi_gender_cleaned == 'Unknown',
      psi_gender_cleaned_NEW,
      psi_gender_cleaned
    )
  ) |>
  select(-psi_gender_cleaned_NEW)

credential_supvars <- credential_supvars |>
  left_join(
    credential_supvars_gender |>
      filter(
        !is.na(ENCRYPTED_TRUE_PEN),
        !(ENCRYPTED_TRUE_PEN %in% c("", " ", "(Unspecified)"))
      ) |>
      select(ENCRYPTED_TRUE_PEN, psi_gender_cleaned),
    by = "ENCRYPTED_TRUE_PEN"
  )
credential_supvars <- credential_supvars |> distinct()

# ---------------------------------------------------------------------------------------------------------
# NULLS (section needs to be rewritten)
#   - Identify records where the cleaned gender is missing in credential_supvars
#   - Search stp enrolment data to find any recorded gender values based on PSI_STUDENT_NUMBER and PSI_CODE
#   - Multiple resolved genders
#   -   - Identify cases with multiple conflicting genders for the same student/ID
#   -   - Resolve conflicts by selecting the "top" gender in the most recent year from credential_supvars_enrolment
#   - Map the resolved gender values back to the primary dataset, update flags and such, but the code needs to be rewritten.
# ---------------------------------------------------------------------------------------------------------

# qry03f_24 & 25
# create a list of records still missing a gender
credential_supvars_missing <- credential_supvars |>
  filter(is.na(psi_gender_cleaned)) |>
  select(ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, psi_gender_cleaned)

# search stp_enrolment for records that can be used to backfill in the missing genders
credential_supvars_missing_recovered <- credential_supvars_missing |>
  inner_join(
    stp_enrolment |> select(PSI_STUDENT_NUMBER, PSI_CODE, PSI_GENDER),
    by = c("PSI_STUDENT_NUMBER", "PSI_CODE"),
    relationship = "many-to-many"
  ) |>
  distinct()

# qry03f_26 & 27
# isolate records associated with multiple genders per student
missing_recovered_multis <- credential_supvars_missing_recovered |>
  group_by(ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE) |>
  summarise(GenderCount = n_distinct(PSI_GENDER), .groups = "drop") |>
  filter(GenderCount > 1)

# select (resolve) one gender per student for the multi-gender records
missing_recovered_multis_distinct <- missing_recovered_multis |>
  select(ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE) |>
  inner_join(
    credential_supvars_enrolment |>
      select(
        ENCRYPTED_TRUE_PEN,
        PSI_GENDER,
        PSI_SCHOOL_YEAR,
        PSI_ENROLMENT_SEQUENCE
      ),
    by = "ENCRYPTED_TRUE_PEN"
  ) |>
  group_by(ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, PSI_GENDER) |>
  summarise(
    MAX_PSI_SCHOOL_YEAR = max(PSI_SCHOOL_YEAR, na.rm = TRUE),
    MAX_PSI_ENROLMENT_SEQUENCE = max(PSI_ENROLMENT_SEQUENCE, na.rm = TRUE),
    .groups = "drop"
  ) |>
  group_by(ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE) |>
  slice_max(
    order_by = tibble(MAX_PSI_SCHOOL_YEAR, MAX_PSI_ENROLMENT_SEQUENCE),
    n = 1,
    with_ties = FALSE
  ) |>
  ungroup() |>
  rename(ResolvedGender = PSI_GENDER)


# qry03f_28, 32, 33, & 34
# assign the resolved gender to the multi-gender records
credential_supvars_missing_recovered <- credential_supvars_missing_recovered |>
  left_join(
    missing_recovered_multis_distinct |>
      select(PSI_STUDENT_NUMBER, PSI_CODE, ResolvedGender),
    by = c("PSI_STUDENT_NUMBER", "PSI_CODE")
  ) |>
  mutate(
    PSI_GENDER_CLEANED_FLAG = if_else(
      PSI_GENDER %in% c("U", "Unknown", "(Unspecified)"),
      "Yes",
      NA_character_
    ),
    PSI_GENDER_CLEANED_FLAG = if_else(
      !is.na(ResolvedGender),
      "Yes",
      PSI_GENDER_CLEANED_FLAG
    ),
    psi_gender_cleaned = coalesce(ResolvedGender, psi_gender_cleaned),
    psi_gender_cleaned = if_else(
      is.na(PSI_GENDER_CLEANED_FLAG),
      PSI_GENDER,
      psi_gender_cleaned
    ),
    PSI_GENDER_CLEANED_FLAG = if_else(
      is.na(PSI_GENDER_CLEANED_FLAG),
      "Yes",
      PSI_GENDER_CLEANED_FLAG
    )
  ) |>
  select(-ResolvedGender)


# qry03f_35
# backfill missing genders in credential_supvars
credential_supvars <- credential_supvars |>
  left_join(
    credential_supvars_missing_recovered |>
      filter(PSI_GENDER_CLEANED_FLAG == "Yes") |>
      select(PSI_STUDENT_NUMBER, PSI_CODE, psi_gender_cleaned),
    by = c("PSI_STUDENT_NUMBER", "PSI_CODE")
  ) |>
  mutate(
    psi_gender_cleaned = if_else(
      is.na(psi_gender_cleaned.x),
      psi_gender_cleaned.y,
      psi_gender_cleaned.x
    )
  ) |>
  select(-psi_gender_cleaned.x, -psi_gender_cleaned.y)
credential_supvars <- credential_supvars |> distinct()

rm(
  missing_recovered_multis_distinct,
  credential_supvars_missing_recovered,
  credential_supvars_missing,
  missing_recovered_multis,
  credential_supvars_gender,
  gender_recovery_lookup
)
gc()



# ==============================================================================
# FILE: 01c-gender-cleaning_dplyr.r
# ==============================================================================


# ******************************************************************************
# Section 5: Gender cleaning
# WHY: Many records have missing, unknown, or inconsistent gender values across
# enrolment records. This extensive cleaning process resolves multi-gender EPENs
# by selecting the most recent gender, cleans unknowns, and falls back to
# PSI_CODE/PSI_STUDENT_NUMBER matching for null-EPEN records.
#
# The original uses ~35 temp tables. Here we use dplyr pipelines with intermediate
# R variables.
# ******************************************************************************

# ---- 5a: Build CredentialSupVars_Gender — unique EPEN/gender from enrolment ----
csvs_gender <- CredentialSupVarsFromEnrolment %>%
  select(ENCRYPTED_TRUE_PEN, PSI_GENDER) %>%
  distinct()

# ---- 5b: Find EPENs with multiple genders ----
multi_gender_epens <- csvs_gender %>%
  count(ENCRYPTED_TRUE_PEN) %>%
  filter(n > 1) %>%
  select(ENCRYPTED_TRUE_PEN)

# ---- 5c: Resolve multi-gender EPENs ----
# WHY: When an EPEN has multiple genders across enrolment records, we pick the gender
# from the most recent school year, breaking ties by max enrolment sequence.
# Original: Steps 1-5 across 3 temp tables.

if (nrow(multi_gender_epens) > 0) {
  # Max school year + enrolment sequence per EPEN/gender combo
  multi_gender_max <- CredentialSupVarsFromEnrolment %>%
    semi_join(multi_gender_epens, by = "ENCRYPTED_TRUE_PEN") %>%
    group_by(ENCRYPTED_TRUE_PEN, PSI_GENDER) %>%
    summarise(
      MAX_PSI_SCHOOL_YEAR = max(PSI_SCHOOL_YEAR),
      MAX_PSI_ENROLMENT_SEQUENCE = max(PSI_ENROLMENT_SEQUENCE),
      .groups = "drop"
    )

  # Overall max per EPEN
  multi_gender_overall_max <- multi_gender_max %>%
    group_by(ENCRYPTED_TRUE_PEN) %>%
    summarise(
      MAX_MAX_PSI_SCHOOL_YEAR = max(MAX_PSI_SCHOOL_YEAR),
      MAX_MAX_PSI_ENROLMENT_SEQUENCE = max(MAX_PSI_ENROLMENT_SEQUENCE),
      .groups = "drop"
    )

  # Resolve: join to find the gender at the max school year + max enrolment sequence
  multi_gender_resolved <- multi_gender_overall_max %>%
    filter(!is_blank(ENCRYPTED_TRUE_PEN)) %>%
    inner_join(
      multi_gender_max,
      by = c(
        "ENCRYPTED_TRUE_PEN" = "ENCRYPTED_TRUE_PEN",
        "MAX_MAX_PSI_SCHOOL_YEAR" = "MAX_PSI_SCHOOL_YEAR",
        "MAX_MAX_PSI_ENROLMENT_SEQUENCE" = "MAX_PSI_ENROLMENT_SEQUENCE"
      )
    ) %>%
    select(ENCRYPTED_TRUE_PEN, PSI_GENDER_TO_USE = PSI_GENDER) %>%
    distinct()
} else {
  multi_gender_resolved <- tibble(
    ENCRYPTED_TRUE_PEN = character(),
    PSI_GENDER_TO_USE = character()
  )
}

# ---- 5d: Apply resolved genders to the gender table ----
csvs_gender <- csvs_gender %>%
  left_join(multi_gender_resolved, by = "ENCRYPTED_TRUE_PEN") %>%
  mutate(
    PSI_GENDER_CLEANED_FLAG = if_else(
      !is.na(PSI_GENDER_TO_USE),
      "Yes",
      NA_character_
    ),
    PSI_GENDER_CLEANED = coalesce(PSI_GENDER_TO_USE, PSI_GENDER)
  )

# ---- 5e: Clean unknowns ('U', 'Unknown') by looking at all enrolment genders ----
# WHY: Some EPENs have gender='U' or 'Unknown'. We check if other enrolment records
# for the same EPEN have a valid gender (Male/Female/Gender Diverse).
unknown_genders <- csvs_gender %>%
  filter(PSI_GENDER_CLEANED %in% c("U", "Unknown"))

if (nrow(unknown_genders) > 0) {
  # Find non-U/Unknown genders from enrolment for these EPENs
  unknowns_with_alternatives <- unknown_genders %>%
    select(ENCRYPTED_TRUE_PEN) %>%
    inner_join(
      CredentialSupVarsFromEnrolment %>%
        select(ENCRYPTED_TRUE_PEN, PSI_GENDER) %>%
        distinct(),
      by = "ENCRYPTED_TRUE_PEN"
    ) %>%
    filter(!PSI_GENDER %in% c("U", "Unknown")) %>%
    group_by(ENCRYPTED_TRUE_PEN) %>%
    summarise(GENDERTOUSE = first(PSI_GENDER), .groups = "drop")

  # Update csvs_gender with resolved unknowns
  csvs_gender <- csvs_gender %>%
    left_join(unknowns_with_alternatives, by = "ENCRYPTED_TRUE_PEN") %>%
    mutate(
      PSI_GENDER_CLEANED = if_else(
        PSI_GENDER_CLEANED %in% c("U", "Unknown") & !is.na(GENDERTOUSE),
        GENDERTOUSE,
        PSI_GENDER_CLEANED
      )
    ) %>%
    select(-GENDERTOUSE)
}

# ---- 5f: Apply gender to CredentialSupVars by EPEN ----
# WHY: Update CredentialSupVars PSI_GENDER_CLEANED from the resolved gender table.
CredentialSupVars <- CredentialSupVars %>%
  left_join(
    csvs_gender %>%
      filter(!is_blank(ENCRYPTED_TRUE_PEN)) %>%
      select(ENCRYPTED_TRUE_PEN, PSI_GENDER_CLEANED),
    by = "ENCRYPTED_TRUE_PEN",
    suffix = c("", "_from_gender")
  ) %>%
  mutate(
    PSI_GENDER_CLEANED = coalesce(
      PSI_GENDER_CLEANED_from_gender,
      PSI_GENDER_CLEANED
    )
  ) %>%
  select(-PSI_GENDER_CLEANED_from_gender)

# ---- 5g: Handle null-EPEN records — match by PSI_CODE/PSI_STUDENT_NUMBER ----
# WHY: Records without a valid EPEN need gender from STP_Enrolment matched by
# PSI_CODE + PSI_STUDENT_NUMBER.
null_gender_records <- CredentialSupVars %>%
  filter(is.na(PSI_GENDER_CLEANED)) %>%
  select(ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE)

if (nrow(null_gender_records) > 0) {
  # Get genders from STP_Enrolment by PSI_CODE/STUDENT_NUMBER
  enrol_gender_by_stuid <- null_gender_records %>%
    inner_join(
      stp_enrolment %>%
        select(PSI_STUDENT_NUMBER, PSI_CODE, PSI_GENDER) %>%
        distinct(),
      by = c("PSI_STUDENT_NUMBER", "PSI_CODE")
    )

  # Find EPENs with multiple genders from this match
  multi_gender_stuid <- enrol_gender_by_stuid %>%
    count(ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE) %>%
    filter(n > 1)

  # Resolve multi-gender by most recent school year (same logic as 5c)
  if (nrow(multi_gender_stuid) > 0) {
    multi_stuid_max <- multi_gender_stuid %>%
      select(-n) %>%
      inner_join(
        CredentialSupVarsFromEnrolment %>%
          select(
            ENCRYPTED_TRUE_PEN,
            PSI_STUDENT_NUMBER,
            PSI_CODE,
            PSI_GENDER,
            PSI_SCHOOL_YEAR,
            PSI_ENROLMENT_SEQUENCE
          ),
        by = c("ENCRYPTED_TRUE_PEN", "PSI_STUDENT_NUMBER", "PSI_CODE")
      ) %>%
      group_by(ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, PSI_GENDER) %>%
      summarise(
        MAX_PSI_SCHOOL_YEAR = max(PSI_SCHOOL_YEAR),
        MAX_PSI_ENROLMENT_SEQUENCE = max(PSI_ENROLMENT_SEQUENCE),
        .groups = "drop"
      )

    multi_stuid_overall_max <- multi_stuid_max %>%
      group_by(ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE) %>%
      summarise(
        MAX_MAX_YEAR = max(MAX_PSI_SCHOOL_YEAR),
        MAX_MAX_SEQ = max(MAX_PSI_ENROLMENT_SEQUENCE),
        .groups = "drop"
      )

    multi_stuid_resolved <- multi_stuid_overall_max %>%
      inner_join(
        multi_stuid_max,
        by = c(
          "ENCRYPTED_TRUE_PEN",
          "PSI_STUDENT_NUMBER",
          "PSI_CODE",
          "MAX_MAX_YEAR" = "MAX_PSI_SCHOOL_YEAR",
          "MAX_MAX_SEQ" = "MAX_PSI_ENROLMENT_SEQUENCE"
        )
      ) %>%
      select(
        ENCRYPTED_TRUE_PEN,
        PSI_STUDENT_NUMBER,
        PSI_CODE,
        PSI_GENDER_TO_USE = PSI_GENDER
      ) %>%
      distinct()
  } else {
    multi_stuid_resolved <- tibble(
      ENCRYPTED_TRUE_PEN = character(),
      PSI_STUDENT_NUMBER = character(),
      PSI_CODE = character(),
      PSI_GENDER_TO_USE = character()
    )
  }

  # Build final gender for null-EPEN records
  enrol_gender_final <- enrol_gender_by_stuid %>%
    left_join(
      multi_stuid_resolved,
      by = c("ENCRYPTED_TRUE_PEN", "PSI_STUDENT_NUMBER", "PSI_CODE")
    ) %>%
    mutate(
      PSI_GENDER_CLEANED_FLAG = if_else(
        !is.na(PSI_GENDER_TO_USE),
        "Yes",
        NA_character_
      )
    ) %>%
    # Clean unknowns from the enrolment match
    mutate(
      PSI_GENDER_CLEANED = coalesce(PSI_GENDER_TO_USE, PSI_GENDER)
    ) %>%
    # If still unknown, keep as-is
    mutate(
      PSI_GENDER_CLEANED_FLAG = if_else(
        PSI_GENDER %in%
          c("U", "Unknown", "(Unspecified)") &
          is.na(PSI_GENDER_CLEANED_FLAG),
        "Yes",
        PSI_GENDER_CLEANED_FLAG
      ),
      PSI_GENDER_CLEANED = if_else(
        is.na(PSI_GENDER_CLEANED_FLAG),
        PSI_GENDER,
        PSI_GENDER_CLEANED
      )
    ) %>%
    select(
      PSI_STUDENT_NUMBER,
      PSI_CODE,
      PSI_GENDER_CLEANED,
      PSI_GENDER_CLEANED_FLAG
    ) %>%
    distinct()

  # Apply to CredentialSupVars
  CredentialSupVars <- CredentialSupVars %>%
    left_join(
      enrol_gender_final %>% filter(PSI_GENDER_CLEANED_FLAG == "Yes"),
      by = c("PSI_STUDENT_NUMBER", "PSI_CODE"),
      suffix = c("", "_enrol")
    ) %>%
    mutate(
      PSI_GENDER_CLEANED = if_else(
        is.na(PSI_GENDER_CLEANED) & !is.na(PSI_GENDER_CLEANED_enrol),
        PSI_GENDER_CLEANED_enrol,
        PSI_GENDER_CLEANED
      )
    ) %>%
    select(-PSI_GENDER_CLEANED_enrol, -PSI_GENDER_CLEANED_FLAG_enrol)
}



# ==============================================================================
# FILE: 01d-enrolment-analysis.R
# ==============================================================================


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
set.seed(123456)

# ---- Check required Tables etc. ----
# Where is the SQL version: Originally sourced from branch 'main' (line 41)
# What the code does: Ensures all source dataframes
#   and lookup tables are present in global environemnt before processing.
# BA Notes:
# - Required tables, and "tables to keep" at the end of the script, reflect
#  tables used/retained from this script only.  Additional changes will be needed
# if running as a continuous workflow.

required_tables <- c(
  "age_group_lookup",
  "outcome_credential",
  "credential",
  "stp_enrolment",
  "stp_enrolment_record_type"
)

missing <- required_tables[!sapply(required_tables, exists, where = .GlobalEnv)]

if (length(missing) > 0) {
  stop(paste(
    "The following required tables are missing from the environment:",
    paste(missing, collapse = ", ")
  ))
}

# define global variables
na_vals = c("", " ", "(Unspecified)", NA)

stp_cols <- c(
  "ID",
  "PSI_BIRTHDATE",
  "psi_birthdate_cleaned",
  "PSI_GENDER",
  "PSI_STUDENT_NUMBER",
  "ENCRYPTED_TRUE_PEN",
  "PSI_SCHOOL_YEAR",
  "PSI_CODE",
  "PSI_MIN_START_DATE",
  "PSI_CIP_CODE",
  "PSI_CREDENTIAL_CATEGORY"
)
cred_cols <- c("ENCRYPTED_TRUE_PEN", "PSI_CODE", "PSI_STUDENT_NUMBER", "psi_gender_cleaned")

# ---- Extract first-time enrolled records ----
# Where is the SQL version: Originally sourced from branch 'main' (line 46)
# Replicates: qry01a through qry01e (STP Enrolment Analysis), qry_CreateMinEnrolmentView and qry02a-qry04a2 (except 03 series)
# What the code does:
# - Constructs the core 'min_enrolment' dataframe by joining raw STP data with
#  record-type filters and previously initialized supplemental variables.
# - Calculates the primary 'AGE_AT_ENROL_DATE' intervals
# - maps age groups via an inequality join.
# BA Notes:
# - Non-essential columns have been dropped from STP_Enrolment and Credential tables to speed up processing



# define min_enrolment dataframe from stp_enrolment and stp_enrolment_record_type
# keeping only valid first enrolment records (RecordStatus == 0, MinEnrolment == 1)
min_enrolment <- stp_enrolment |>
  select(all_of(stp_cols)) |>
  inner_join(
    stp_enrolment_record_type |>
      filter(RecordStatus == 0, MinEnrolment == 1) |>
      select(ID, FirstEnrolment),
    by = "ID"
  ) |>
  rename_with(toupper)

# clean and format date variables, calculate age at enrolment, and flag first enrolments
min_enrolment <- min_enrolment |>
  mutate(
    PSI_BIRTHDATE_CLEANED_D = if_else(
      PSI_BIRTHDATE_CLEANED %in%
        na_vals |
        PSI_BIRTHDATE_CLEANED == as.Date("1900-01-01"),
      as.Date(NA),
      as.Date(PSI_BIRTHDATE_CLEANED)
    ),
    PSI_MIN_START_DATE_D = if_else(
      PSI_MIN_START_DATE %in% na_vals,
      as.Date(NA),
      as.Date(PSI_MIN_START_DATE)
    ),
    IS_FIRST_ENROLMENT = if_else(FIRSTENROLMENT == 1, "Yes", NA_character_),
    AGE_AT_ENROL_DATE = if_else(
      !is.na(PSI_BIRTHDATE_CLEANED_D) & !is.na(PSI_MIN_START_DATE_D),
      floor(interval(PSI_BIRTHDATE_CLEANED_D, PSI_MIN_START_DATE_D) / years(1)),
      NA_real_
    )
  ) |>
  select(-FIRSTENROLMENT)

# map age groups via an inequality join
min_enrolment <- min_enrolment |>
  left_join(
    age_group_lookup,
    by = join_by(between(AGE_AT_ENROL_DATE, LowerBound, UpperBound))
  ) |>
  mutate(AGE_GROUP_ENROL_DATE = AgeIndex) |>
  select(-AgeIndex, -AgeGroup, -LowerBound, -UpperBound)


# ---- Find gender for distinct non-null EPENs, or non-null PSI_CODE/PSI_NUMBER  ----
# Where is the SQL version: Originally sourced from branch 'main' (line 62)
# What the code does: identifies invalid genders  ("", " ", "(Unspecified)", NA) and 
#   uses genders from the credential view to impute (backfill) gender into min_enrolment.  Accomplished 
#   by performing two passes - 1) using valid epens, then 2) valid psi code/number combinations for records without valid epens.
# BA Notes:
# We have 3 quite different methods for imputing invalid genders in the next couple of sections
# We should consider reducing complexity and creating a more unified approach

# select the first valid gender for each student in credential data - pass #1 valid epens
credential_epen <- credential |>
  select(all_of(cred_cols)) |>
  filter(!ENCRYPTED_TRUE_PEN %in% na_vals, !psi_gender_cleaned %in% na_vals) |>
  select(ENCRYPTED_TRUE_PEN, gender_cred_epen = psi_gender_cleaned) |>
  slice_head(
    by = ENCRYPTED_TRUE_PEN,
    n = 1
  )

# select the first valid gender for each student in credential data - pass #2 invalid epens
credential_no_epen <- credential |>
  select(all_of(cred_cols)) |>
  filter(ENCRYPTED_TRUE_PEN %in% na_vals, !psi_gender_cleaned %in% na_vals) |>
  select(
    PSI_STUDENT_NUMBER,
    PSI_CODE,
    gender_cred_no_epen = psi_gender_cleaned
  ) |>
  slice_head(
    by = c(PSI_STUDENT_NUMBER, PSI_CODE),
    n = 1
  )

# back fill NA genders in min_enrolment
min_enrolment <- min_enrolment |>
  left_join(credential_epen, by = join_by(ENCRYPTED_TRUE_PEN)) |> # some duplicates being introduced here
  left_join(credential_no_epen, by = join_by(PSI_STUDENT_NUMBER, PSI_CODE)) |>
  mutate(
    gender_cred = coalesce(gender_cred_epen, gender_cred_no_epen)
  ) |>
  mutate(
    PSI_GENDER = case_when(
      is.na(PSI_GENDER) ~ gender_cred,
      is.na(gender_cred) ~ PSI_GENDER,
      TRUE ~ if_else(PSI_GENDER != gender_cred, gender_cred, PSI_GENDER)
    )
  ) |>
  select(-gender_cred_epen, -gender_cred_no_epen, -gender_cred)

# ---- Assign one gender/student and update MinEnrolment table ----
# Where is the SQL version: Originally sourced from branch 'main' (line 78)
# Replicates: qry04c through qry04e2
# What the code does:
# - Performs a historic imputation process based on a student’s earliest recorded data in stp_enrolment.
# - Accomplished in one pass by creating a concatenated ID (encrypted true pen where available, and psi code/number where not) 

# select first-time recorded gender from min-enrolment data
first_gender_lookup <- min_enrolment |>
  filter(IS_FIRST_ENROLMENT == "Yes") |>
  mutate(
    CONCATENATED_ID = if_else(
      !ENCRYPTED_TRUE_PEN %in% na_vals,
      ENCRYPTED_TRUE_PEN,
      paste0(PSI_STUDENT_NUMBER, PSI_CODE)
    )
  ) |>
  distinct(CONCATENATED_ID, FIRST_GENDER = PSI_GENDER)

# forward fill NA genders (join on a concatenated ID, instead of 2-passes, epen, no epen)
min_enrolment <- min_enrolment |>
  mutate(
    CONCATENATED_ID = if_else(
      !ENCRYPTED_TRUE_PEN %in% na_vals,
      ENCRYPTED_TRUE_PEN,
      paste0(PSI_STUDENT_NUMBER, PSI_CODE)
    )
  ) |>
  left_join(first_gender_lookup, by = "CONCATENATED_ID") |>
  mutate(
    PSI_GENDER = coalesce(FIRST_GENDER, PSI_GENDER)
  ) |>
  select(-FIRST_GENDER)

# ---- impute gender  ----
# Where is the SQL version: SQL version starts ~line 101 on branch main
# Replicates: qry05a1 through qry06a5 and some R code from lines 101 to 170 (main)
# What the code does:
# - Performs a proportional imputation for missing gender data.
# It calculates the distribution of the known population from the set of first enrolment records
#  and applies that same ratio to missing first records.
# Simulataneously performs a historical imputation, where the first seen record is carried forward. 

na_vals <- c("U", "Unknown", "(Unspecified)", "", NA)

# first-time "unknowns"
extract_no_gender_first <- min_enrolment |>
  filter(IS_FIRST_ENROLMENT == "Yes", PSI_GENDER %in% na_vals) |>
  select(ID, ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, PSI_GENDER)

total_unknowns <- nrow(extract_no_gender_first)

# first-time valid genders (Male, Female, Gender Diverse)
gender_weights <- min_enrolment |>
  filter(IS_FIRST_ENROLMENT == "Yes", !PSI_GENDER %in% na_vals) |>
  count(PSI_GENDER) |>
  mutate(PROPORTION = n / sum(n)) |>
  mutate(TARGET_N = round(PROPORTION * total_unknowns))

# resample unknownws
imputed_first_enrolments <- extract_no_gender_first |>
  mutate(
    PSI_GENDER_IMPUTED = sample(rep(
      gender_weights$PSI_GENDER,
      times = gender_weights$TARGET_N
    ))
  )

# carry forward first seen gender for records with valid encrypted true pen
extract_no_gender_epen <- min_enrolment |>
  filter(PSI_GENDER %in% na_vals, !ENCRYPTED_TRUE_PEN %in% na_vals) |>
  select(ID, ENCRYPTED_TRUE_PEN) |>
  inner_join(
    imputed_first_enrolments |>
      distinct(ENCRYPTED_TRUE_PEN, PSI_GENDER_IMPUTED),
    by = join_by(ENCRYPTED_TRUE_PEN)
  )

# carry forward first seen gender for records without valid encrypted true pen
extract_no_gender_no_epen <- min_enrolment |>
  filter(PSI_GENDER %in% na_vals, ENCRYPTED_TRUE_PEN %in% na_vals) |>
  filter(!PSI_CODE %in% na_vals, !PSI_STUDENT_NUMBER %in% na_vals) |>
  select(ID, PSI_CODE, PSI_STUDENT_NUMBER) |>
  inner_join(
    imputed_first_enrolments |>
      distinct(PSI_CODE, PSI_STUDENT_NUMBER, PSI_GENDER_IMPUTED),
    by = join_by(PSI_CODE, PSI_STUDENT_NUMBER)
  )

# backfill genders in min_enrolment data
min_enrolment <- min_enrolment |>
  left_join(
    extract_no_gender_epen |> select(ID, PSI_GENDER_IMPUTED),
    by = "ID"
  ) |>
  left_join(
    extract_no_gender_no_epen |> select(ID, PSI_GENDER_IMPUTED),
    by = "ID",
    suffix = c(".pen", ".nopen")
  ) |>
  mutate(
    PSI_GENDER = if_else(PSI_GENDER %in% na_vals, NA_character_, PSI_GENDER),
    PSI_GENDER = coalesce(
      PSI_GENDER,
      PSI_GENDER_IMPUTED.pen,
      PSI_GENDER_IMPUTED.nopen
    )
  ) |>
  select(-PSI_GENDER_IMPUTED.pen, -PSI_GENDER_IMPUTED.nopen)


# ---- Create Age and Gender Distrbutions ----
# and 
# ----- Assign age to records with missing age -----
# Where is the SQL version: Originally sourced from branch 'main' (line 164:281)
# Replicates: qry07a-qry08 and much R code
# What the code does:
# - This code extracts and isolates records where the student's age could not be calculated.
# - Performs a Stratified Proportional Imputation for missing ages.
# - Followed by a Temporal Projection to fill in subsequent records.
# - Updates extract_no_age with imputed ages
# BA Notes:
# - compare extract_no_age to Extract_No_Age and
# - compare extract_no_age_first_enrol to Extract_No_Age_First_Enrolment
# - distribution of assigned (imputed) ages are similar for this version vs last

# extract records with missing age at enrolment
extract_no_age <- min_enrolment |>
  filter(is.na(AGE_AT_ENROL_DATE)) |>
  distinct(
    ID,
    ENCRYPTED_TRUE_PEN,
    PSI_STUDENT_NUMBER,
    PSI_CODE,
    AGE_AT_ENROL_DATE,
    PSI_SCHOOL_YEAR,
    PSI_MIN_START_DATE,
    PSI_MIN_START_DATE_D,
    IS_FIRST_ENROLMENT,
    PSI_GENDER
  )

# extract records with missing age at enrolment for first enrolments only (for imputation)
extract_no_age_first_enrol <- min_enrolment |>
  filter(is.na(AGE_AT_ENROL_DATE), IS_FIRST_ENROLMENT == "Yes") |>
  distinct(
    ID,
    ENCRYPTED_TRUE_PEN,
    PSI_STUDENT_NUMBER,
    PSI_GENDER,
    PSI_CODE,
    AGE_AT_ENROL_DATE_first = AGE_AT_ENROL_DATE
  )

# function to impute age by gender
# assumes that wt contains a p for every age 
impute_age_by_gender <- function(df, gender_name, wt) {
  # isolate frequency distribution for specified gender
  dist <- wt |> filter(PSI_GENDER == gender_name)

  # if the distribution is empty/missing, use the global age distribution
  if (nrow(dist) == 0) {
    dist <- wt |>
      count(AGE_AT_ENROL_DATE, wt = count) |>
      mutate(p = n / sum(n))
  }

  # sample ages and assign to df
  df$AGE_AT_ENROL_DATE <- sample(
    dist$AGE_AT_ENROL_DATE,
    size = nrow(df),
    replace = TRUE,
    prob = dist$p
  )
  return(df)
}

# calculate natural age by gender distribution from known ages at first enrolment
age_weights <- min_enrolment |>
  filter(!is.na(AGE_AT_ENROL_DATE), IS_FIRST_ENROLMENT == "Yes") |>
  count(PSI_GENDER, AGE_AT_ENROL_DATE, name = "count") |>
  group_by(PSI_GENDER) |>
  mutate(p = count / sum(count)) |>
  ungroup()

# impute ages for records with missing age at first enrolment
# improvement: assumes age_weights includes a p for every age
# but if there are missing ages, this forces sampling with 0 p.
extract_no_age_first_enrol <- extract_no_age_first_enrol |>
  split(~PSI_GENDER) |>
  imap(~ impute_age_by_gender(.x, .y, age_weights)) |>
  list_rbind()

# assign ages to all first enrolment records that are missing ages
extract_no_age <- extract_no_age |>
  select(-AGE_AT_ENROL_DATE) |>
  left_join(
    extract_no_age_first_enrol |>
      distinct(ID, AGE_AT_ENROL_DATE)
  )

# calculate missing ages from first enrolments
calc_ages <- extract_no_age |>
  arrange(PSI_STUDENT_NUMBER, PSI_CODE, PSI_MIN_START_DATE_D) |>
  group_by(PSI_STUDENT_NUMBER, PSI_CODE) |>
  mutate(
    base_date = first(PSI_MIN_START_DATE_D),
    base_age = first(AGE_AT_ENROL_DATE),
    AGE_AT_ENROL_DATE = if_else(
      is.na(AGE_AT_ENROL_DATE) & !is.na(base_age),
      base_age +
        (as.POSIXlt(PSI_MIN_START_DATE_D)$year - as.POSIXlt(base_date)$year),
      AGE_AT_ENROL_DATE
    )
  ) |>
  ungroup() |>
  select(-base_date, -base_age)

calc_ages <- calc_ages %>% select(ID, AGE_AT_ENROL_DATE)

# 
extract_no_age <- extract_no_age |>
  left_join(
    calc_ages |> rename(AGE_AT_ENROL_DATE_to_update = AGE_AT_ENROL_DATE)
  ) |>
  mutate(
    AGE_AT_ENROL_DATE = coalesce(AGE_AT_ENROL_DATE, AGE_AT_ENROL_DATE_to_update)
  ) |>
  select(-AGE_AT_ENROL_DATE_to_update)

# ---- some manual edits ----
# BA Notes: Some manual updates were made here to remaining missing ages. 
# I haven't done the manual fixes as we're getting away from manual work

min_enrolment <- min_enrolment |>
  left_join(
    extract_no_age |>
      distinct(ID, AGE_AT_ENROL_DATE_to_update = AGE_AT_ENROL_DATE)
  ) |>
  mutate(
    AGE_AT_ENROL_DATE = coalesce(AGE_AT_ENROL_DATE, AGE_AT_ENROL_DATE_to_update)
  ) |>
  select(-AGE_AT_ENROL_DATE_to_update)

min_enrolment <- min_enrolment |>
  left_join(
    age_group_lookup |> select(AgeIndex, LowerBound, UpperBound),
    by = join_by(between(AGE_AT_ENROL_DATE, LowerBound, UpperBound))
  ) |>
  mutate(
    AGE_GROUP_ENROL_DATE = AgeIndex
  ) |>
  select(-AgeIndex, -LowerBound, -UpperBound)


# ---- Final Distributions ----
# !! This section moved to 01e-stp-distributions


# ---- Clean Up ----
tables_to_keep <- c(
  "age_group_lookup",
  "min_enrolment"
)

rm(list = setdiff(ls(), tables_to_keep))



# ==============================================================================
# FILE: 01d-enrolment-analysis_dplyr.R
# ==============================================================================


# Enrolment Analysis — dplyr Translation
# Original: R/01d-enrolment-analysis.R
#
# Pipeline context:
#   Processes STP enrolment data to create the MinEnrolment working dataset used by
#   downstream graduate and program projection scripts. The main tasks are:
#     1. Build MinEnrolment from filtered STP enrolment records
#     2. Compute age at enrolment from birthdate/start date
#     3. Clean and impute gender (Credential update, duplicate resolution, proportional)
#     4. Impute missing ages using proportional sampling
#     5. Produce final enrolment distribution summaries
#
# Input tables:
#   - STP_Enrolment — preprocessed enrolment data (from 01a)
#   - STP_Enrolment_Record_Type — record classification (MinEnrolment, FirstEnrolment, RecordStatus)
#   - AgeGroupLookup — age group range definitions (AgeIndex, AgeGroup, LowerBound, UpperBound)
#   - Credential — credential view with cleaned gender (from 01b/01c)
#
# Output:
#   - MinEnrolment (table) — filtered enrolment records with cleaned age/gender
#   - qry09c_MinEnrolment — enrolment counts by gender/age group/year
#   - qry09c_MinEnrolment_Domestic — domestic-only enrolment counts
#   - qry09c_MinEnrolment_by_Credential_and_CIP_Code — counts by credential/CIP

library(tidyverse)
library(odbc)
library(DBI)
set.seed(123456)

# ---- Configure LAN Paths and DB Connection -----
lan <- config::get("lan")
db_config <- config::get("decimal")
my_schema <- config::get("myschema")
db_schema <- config::get("dbschema")

con <- dbConnect(odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")

# Helper: reference a table in the user's schema
sch_tbl <- function(name) {
  tbl(con, dbplyr::in_schema(my_schema, name))
}

# Helper: look up age group index from age value using the AgeGroupLookup table.
# WHY: SQL uses CROSS JOIN with range condition (LowerBound <= age <= UpperBound).
# In dplyr, we vectorize this by matching each age against the lookup ranges.
lookup_age_group <- function(age, age_lookup) {
  sapply(age, function(a) {
    if (is.na(a)) return(NA_real_)
    idx <- which(age_lookup$LOWERBOUND <= a & age_lookup$UPPERBOUND >= a)
    if (length(idx) == 0) return(NA_real_)
    age_lookup$AGEINDEX[idx[1]]
  })
}

# Helper: check if a gender value represents "unknown"
is_unknown_gender <- function(g) {
  is.na(g) | g %in% c("", "U", "Unknown", "(Unspecified)")
}

# Helper: compute age at a date from birthdate.
# WHY: Equivalent to SQL's datediff(year,bday,start) with birthday-has-occurred check.
# The SQL adds years to bday and checks if it exceeds start; if so, subtract 1.
compute_age_at_date <- function(birth_date, start_date) {
  birth_lt <- as.POSIXlt(birth_date)
  start_lt <- as.POSIXlt(start_date)
  age <- as.numeric(start_lt$year - birth_lt$year)
  not_had_birthday <- (start_lt$mon < birth_lt$mon) |
    (start_lt$mon == birth_lt$mon & start_lt$mday < birth_lt$mday)
  if_else(not_had_birthday, age - 1, age)
}


# ---- Pull required tables into R ----
stp_enrolment <- sch_tbl("STP_Enrolment") %>%
  collect() |> rename_with(toupper)

stp_enrolment_record_type <- sch_tbl("STP_Enrolment_Record_Type") %>%
  collect() |> rename_with(toupper)

age_lookup <- sch_tbl("AgeGroupLookup") %>%
  collect() |> rename_with(toupper)

credential <- sch_tbl("Credential") %>%
  collect() |> rename_with(toupper)


# ******************************************************************************
# Part 1: Build MinEnrolment (qry01a–01e, qry_CreateMinEnrolmentView)
#
# Joins STP_Enrolment with STP_Enrolment_Record_Type, adds support variables
# (date conversions, age, age group, first enrolment flag), then filters to
# records with RecordStatus=0 and MinEnrolment=1.
# WHY: MinEnrolment is the core working dataset for downstream projections.
# RecordStatus=0 = valid record, MinEnrolment=1 = earliest enrolment per student.
# In SQL this was a CREATE VIEW; in dplyr it's a filtered dataframe written to DB.
# ******************************************************************************

min_enrolment <- stp_enrolment %>%
  inner_join(
    stp_enrolment_record_type %>% select(ID, MINENROLMENT, FIRSTENROLMENT, RECORDSTATUS),
    by = "ID"
  ) %>%
  mutate(
    # qry01c: Convert birthdate string to Date
    PSI_BIRTHDATE_CLEANED_D = as.Date(PSI_BIRTHDATE_CLEANED),
    # qry01d1: Convert start date, excluding empty/unspecified
    PSI_MIN_START_DATE_D = if_else(
      is.na(PSI_MIN_START_DATE) | PSI_MIN_START_DATE == "" |
        PSI_MIN_START_DATE == "(Unspecified)",
      as.Date(NA), as.Date(PSI_MIN_START_DATE)
    ),
    # qry01d2: Null out birthdates converted from empty/invalid to 1900-01-01
    PSI_BIRTHDATE_CLEANED_D = if_else(
      !is.na(PSI_BIRTHDATE_CLEANED_D) &
        PSI_BIRTHDATE_CLEANED_D == as.Date("1900-01-01") &
        (is.na(PSI_BIRTHDATE_CLEANED) | PSI_BIRTHDATE_CLEANED == "" |
           PSI_BIRTHDATE_CLEANED == "(Unspecified)"),
      as.Date(NA), PSI_BIRTHDATE_CLEANED_D
    ),
    # Initialize age and age group columns
    AGE_AT_ENROL_DATE = NA_real_,
    AGE_GROUP_ENROL_DATE = NA_real_,
    # qry01e: Flag first enrolments
    IS_FIRST_ENROLMENT = if_else(FIRSTENROLMENT == 1, "Yes", NA_character_),
    IS_SKILLS_BASED = NA_real_
  ) %>%
  # Filter to valid minimum enrolment records
  filter(RECORDSTATUS == 0, MINENROLMENT == 1) %>%
  select(-MINENROLMENT, -FIRSTENROLMENT, -RECORDSTATUS)


# ******************************************************************************
# Part 2: Compute age at enrolment (qry02a–02b)
#
# Computes age from birthdate and start date, then maps to age group index.
# WHY: Age is a key dimension for graduate projections.
# ******************************************************************************

# qry02a: Compute age at enrolment
min_enrolment <- min_enrolment %>%
  mutate(
    AGE_AT_ENROL_DATE = if_else(
      !is.na(PSI_BIRTHDATE_CLEANED_D) & !is.na(PSI_MIN_START_DATE_D),
      compute_age_at_date(PSI_BIRTHDATE_CLEANED_D, PSI_MIN_START_DATE_D),
      AGE_AT_ENROL_DATE
    )
  )

# qry02b: Look up age group from AgeGroupLookup (SQL CROSS JOIN with range condition)
min_enrolment <- min_enrolment %>%
  mutate(AGE_GROUP_ENROL_DATE = lookup_age_group(AGE_AT_ENROL_DATE, age_lookup))


# ******************************************************************************
# Part 3: Gender update from Credential (qry04a1–04a2)
#
# Updates enrolment gender from the Credential view's cleaned gender, matching
# first by EPEN then by PSI_STUDENT_NUMBER + PSI_CODE.
# WHY: Credential preprocessing produces a more accurate cleaned gender value.
# ******************************************************************************

# Build distinct lookup of cleaned gender from Credential (deduplicated to avoid row multiplication)
cred_gender_epen <- credential %>%
  filter(!is.na(ENCRYPTED_TRUE_PEN) & ENCRYPTED_TRUE_PEN != "" &
           ENCRYPTED_TRUE_PEN != "(Unspecified)") %>%
  distinct(ENCRYPTED_TRUE_PEN, PSI_GENDER_CLEANED)

cred_gender_studnum <- credential %>%
  distinct(PSI_STUDENT_NUMBER, PSI_CODE, PSI_GENDER_CLEANED)

# qry04a1: Update via EPEN match
min_enrolment <- min_enrolment %>%
  left_join(cred_gender_epen, by = "ENCRYPTED_TRUE_PEN") %>%
  mutate(
    PSI_GENDER = if_else(
      !is.na(ENCRYPTED_TRUE_PEN) & ENCRYPTED_TRUE_PEN != "" &
        ENCRYPTED_TRUE_PEN != "(Unspecified)" &
        !is.na(PSI_GENDER_CLEANED) & PSI_GENDER != PSI_GENDER_CLEANED,
      PSI_GENDER_CLEANED, PSI_GENDER
    )
  ) %>%
  select(-PSI_GENDER_CLEANED)

# qry04a2: Update via studnum+psicode match (for records without valid EPEN)
min_enrolment <- min_enrolment %>%
  left_join(cred_gender_studnum, by = c("PSI_STUDENT_NUMBER", "PSI_CODE")) %>%
  mutate(
    PSI_GENDER = if_else(
      (is.na(ENCRYPTED_TRUE_PEN) | ENCRYPTED_TRUE_PEN == "" |
         ENCRYPTED_TRUE_PEN == "(Unspecified)") &
        !is.na(PSI_GENDER_CLEANED) & PSI_GENDER != PSI_GENDER_CLEANED,
      PSI_GENDER_CLEANED, PSI_GENDER
    )
  ) %>%
  select(-PSI_GENDER_CLEANED)


# ******************************************************************************
# Part 4: Gender duplicate resolution (qry04b–04e)
#
# Some students have multiple enrolment records with different genders.
# We assign ONE consistent gender per student using this strategy:
#   1. Identify students by EPEN (if valid) or PSI_STUDENT_NUMBER + PSI_CODE
#   2. Find students with > 1 distinct gender across their records
#   3. Prefer gender from first enrolment record
#   4. If first enrolment gender is unknown, use a non-unknown gender from any record
#   5. Update all records for that student with the resolved gender
# WHY: Inconsistent gender across enrolment records causes issues in projections.
# In SQL this was ~15 operations across 4 temp tables; in dplyr it's one pipeline.
# ******************************************************************************

# Build student identifier: EPEN if valid, otherwise studnum+psicode concatenation
min_enrolment <- min_enrolment %>%
  mutate(
    STUDENT_ID = case_when(
      !is.na(ENCRYPTED_TRUE_PEN) & ENCRYPTED_TRUE_PEN != "" &
        ENCRYPTED_TRUE_PEN != "(Unspecified)" ~ ENCRYPTED_TRUE_PEN,
      TRUE ~ paste0(PSI_STUDENT_NUMBER, PSI_CODE)
    )
  )

# Find students with conflicting genders across their records
dup_student_ids <- min_enrolment %>%
  distinct(STUDENT_ID, PSI_GENDER) %>%
  count(STUDENT_ID, name = "n_genders") %>%
  filter(n_genders > 1) %>%
  pull(STUDENT_ID)

if (length(dup_student_ids) > 0) {
  # Get gender from first enrolment for duplicate students
  first_enrol_gender <- min_enrolment %>%
    filter(IS_FIRST_ENROLMENT == "Yes", STUDENT_ID %in% dup_student_ids) %>%
    distinct(STUDENT_ID, PSI_GENDER) %>%
    rename(FIRST_GENDER = PSI_GENDER)

  # Get non-unknown gender from any record (fallback for unknown first enrolment gender)
  non_unknown_gender <- min_enrolment %>%
    filter(STUDENT_ID %in% dup_student_ids, !PSI_GENDER %in% c("Unknown")) %>%
    distinct(STUDENT_ID, PSI_GENDER) %>%
    rename(NON_UNKNOWN_GENDER = PSI_GENDER)

  # Resolve: prefer first enrolment gender; if unknown, use non-unknown
  resolved_gender <- first_enrol_gender %>%
    left_join(non_unknown_gender, by = "STUDENT_ID") %>%
    mutate(
      RESOLVED_GENDER = case_when(
        FIRST_GENDER %in% c("Male", "Female", "Gender Diverse") ~ FIRST_GENDER,
        !is.na(NON_UNKNOWN_GENDER) ~ NON_UNKNOWN_GENDER,
        TRUE ~ FIRST_GENDER
      )
    ) %>%
    distinct(STUDENT_ID, RESOLVED_GENDER)

  # Update MinEnrolment with resolved genders
  min_enrolment <- min_enrolment %>%
    left_join(resolved_gender, by = "STUDENT_ID") %>%
    mutate(PSI_GENDER = coalesce(RESOLVED_GENDER, PSI_GENDER)) %>%
    select(-RESOLVED_GENDER)

  rm(first_enrol_gender, non_unknown_gender, resolved_gender)
}

min_enrolment <- min_enrolment %>% select(-STUDENT_ID)


# ******************************************************************************
# Part 5: Gender imputation for unknown/missing genders (qry05–06a5)
#
# Assigns genders proportionally to records with unknown/blank/NULL gender.
# The proportions are computed from the known gender distribution of first
# enrolments, then applied in sequence:
#   1. First enrolments: proportional Female/Male/Gender Diverse
#   2. Non-first enrolments: use first enrolment gender via studnum+psicode match
#   3. Remaining: proportional Female/Male/Gender Diverse
#   4. EPEN duplicates: resolve remaining conflicts proportionally
# WHY: Downstream projections need a gender for every record.
# !! UPDATE: The hardcoded TOP(N) counts in the original are replaced with
# dynamic computation from the proportion distribution.
# ******************************************************************************

# qry05a2: Compute known gender distribution
prop_dist <- min_enrolment %>%
  filter(
    !PSI_GENDER %in% c("Unknown", "", "(Unspecified)"),
    !is.na(PSI_GENDER),
    IS_FIRST_ENROLMENT == "Yes"
  ) %>%
  count(PSI_GENDER, name = "NUMENROLLED")


# ---- 5a: Impute gender for first enrolments with unknown gender ----

# qry05a1b: Extract first enrolments with unknown gender
extract_no_gender_first <- min_enrolment %>%
  filter(is_unknown_gender(PSI_GENDER), IS_FIRST_ENROLMENT == "Yes") %>%
  distinct(ID, ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, PSI_GENDER)

# Compute proportional counts dynamically (replaces hardcoded TOP(N))
n_first <- nrow(extract_no_gender_first)
prop_first <- prop_dist %>%
  mutate(p = NUMENROLLED / sum(NUMENROLLED), top_n = round(p * n_first))

n_female <- prop_first %>% filter(PSI_GENDER == "Female") %>% pull(top_n)
n_male <- prop_first %>% filter(PSI_GENDER == "Male") %>% pull(top_n)

# Assign genders in sequence: Female → Male → Gender Diverse
unknown_idx <- which(is_unknown_gender(extract_no_gender_first$PSI_GENDER))
if (n_female > 0 && length(unknown_idx) >= n_female) {
  extract_no_gender_first$PSI_GENDER[unknown_idx[1:n_female]] <- "Female"
}
if (n_male > 0 && n_female + n_male <= length(unknown_idx)) {
  extract_no_gender_first$PSI_GENDER[unknown_idx[(n_female + 1):(n_female + n_male)]] <- "Male"
}
still_unk <- which(is_unknown_gender(extract_no_gender_first$PSI_GENDER))
if (length(still_unk) > 0) {
  extract_no_gender_first$PSI_GENDER[still_unk] <- "Gender Diverse"
}


# ---- 5b: Extract ALL records with unknown gender ----

# qry05a1: Extract all records with unknown gender
extract_no_gender <- min_enrolment %>%
  filter(is_unknown_gender(PSI_GENDER)) %>%
  distinct(ID, ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE, PSI_GENDER)


# ---- 5c: Update non-first from first enrolment gender ----

# qry06a3_CorrectGender1: Match via PSI_STUDENT_NUMBER + PSI_CODE
extract_no_gender <- extract_no_gender %>%
  left_join(
    extract_no_gender_first %>% select(PSI_STUDENT_NUMBER, PSI_CODE, PSI_GENDER),
    by = c("PSI_STUDENT_NUMBER", "PSI_CODE"),
    suffix = c("", "_first")
  ) %>%
  mutate(
    PSI_GENDER = if_else(
      is_unknown_gender(PSI_GENDER) & !is.na(PSI_GENDER_FIRST),
      PSI_GENDER_FIRST, PSI_GENDER
    )
  ) %>%
  select(-PSI_GENDER_FIRST)


# ---- 5d: Assign remaining proportionally ----

still_unknown_idx <- which(is_unknown_gender(extract_no_gender$PSI_GENDER))
n_remaining <- length(still_unknown_idx)

if (n_remaining > 0) {
  prop_rem <- prop_dist %>%
    mutate(p = NUMENROLLED / sum(NUMENROLLED), top_n = round(p * n_remaining))

  n_f <- prop_rem %>% filter(PSI_GENDER == "Female") %>% pull(top_n)
  n_m <- prop_rem %>% filter(PSI_GENDER == "Male") %>% pull(top_n)

  if (n_f > 0 && n_f <= n_remaining) {
    extract_no_gender$PSI_GENDER[still_unknown_idx[1:n_f]] <- "Female"
  }
  if (n_m > 0 && n_f + n_m <= n_remaining) {
    extract_no_gender$PSI_GENDER[still_unknown_idx[(n_f + 1):(n_f + n_m)]] <- "Male"
  }
  still_unk2 <- which(is_unknown_gender(extract_no_gender$PSI_GENDER))
  if (length(still_unk2) > 0) {
    extract_no_gender$PSI_GENDER[still_unk2] <- "Gender Diverse"
  }
}


# ---- 5e: Handle EPEN duplicates (same EPEN, multiple assigned genders) ----

# qry06a4a–06a4c: Find EPENs with conflicting genders and resolve proportionally
dup_epens <- extract_no_gender %>%
  filter(!is.na(ENCRYPTED_TRUE_PEN) & ENCRYPTED_TRUE_PEN != "" &
           ENCRYPTED_TRUE_PEN != "(Unspecified)") %>%
  count(ENCRYPTED_TRUE_PEN, name = "n_genders") %>%
  filter(n_genders > 1)

if (nrow(dup_epens) > 0) {
  n_dup <- nrow(dup_epens)
  prop_dup <- prop_dist %>%
    mutate(p = NUMENROLLED / sum(NUMENROLLED), top_n = round(p * n_dup))

  n_f_dup <- prop_dup %>% filter(PSI_GENDER == "Female") %>% pull(top_n)
  n_gd_dup <- prop_dup %>% filter(PSI_GENDER == "Gender Diverse") %>% pull(top_n)

  dup_epens$PSI_GENDER_TO_USE <- NA_character_
  if (n_f_dup > 0) dup_epens$PSI_GENDER_TO_USE[1:n_f_dup] <- "Female"
  gd_range <- (n_f_dup + 1):(n_f_dup + n_gd_dup)
  if (n_gd_dup > 0 && n_f_dup + n_gd_dup <= n_dup) {
    dup_epens$PSI_GENDER_TO_USE[gd_range] <- "Gender Diverse"
  }
  dup_epens$PSI_GENDER_TO_USE[is.na(dup_epens$PSI_GENDER_TO_USE)] <- "Male"

  extract_no_gender <- extract_no_gender %>%
    left_join(
      dup_epens %>% select(ENCRYPTED_TRUE_PEN, PSI_GENDER_TO_USE),
      by = "ENCRYPTED_TRUE_PEN"
    ) %>%
    mutate(PSI_GENDER = coalesce(PSI_GENDER_TO_USE, PSI_GENDER)) %>%
    select(-PSI_GENDER_TO_USE)
}


# ---- 5f: Push imputed genders back to MinEnrolment ----

# qry06a5: Update MinEnrolment with all imputed genders
min_enrolment <- min_enrolment %>%
  left_join(
    extract_no_gender %>% select(ID, PSI_GENDER),
    by = "ID",
    suffix = c("", "_imputed")
  ) %>%
  mutate(PSI_GENDER = coalesce(PSI_GENDER_IMPUTED, PSI_GENDER)) %>%
  select(-PSI_GENDER_IMPUTED)

# qry06a4c: Check proportions after gender assignment
prop_check <- min_enrolment %>%
  filter(
    !PSI_GENDER %in% c("Unknown", "", "(Unspecified)"),
    !is.na(PSI_GENDER),
    IS_FIRST_ENROLMENT == "Yes"
  ) %>%
  count(PSI_GENDER, name = "NUMENROLLED")
prop_check

rm(extract_no_gender_first, extract_no_gender, dup_epens)


# ******************************************************************************
# Part 6: Age imputation for records with missing age (qry07a–07e)
#
# Imputes missing ages using proportional sampling from known age distributions,
# then calculates ages for students with multiple enrolments using year differences.
# WHY: Some records have no birthdate, so age cannot be computed directly.
# We sample ages from the known distribution, stratified by gender.
# ******************************************************************************

# qry07a: Extract records with missing age
extract_no_age <- min_enrolment %>%
  filter(is.na(AGE_AT_ENROL_DATE)) %>%
  distinct(ID, ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_CODE,
           AGE_AT_ENROL_DATE, PSI_SCHOOL_YEAR, PSI_MIN_START_DATE, PSI_MIN_START_DATE_D)

# qry07b: Extract first enrolments with missing age
extract_no_age_first <- min_enrolment %>%
  filter(is.na(AGE_AT_ENROL_DATE), IS_FIRST_ENROLMENT == "Yes") %>%
  distinct(ID, ENCRYPTED_TRUE_PEN, PSI_STUDENT_NUMBER, PSI_GENDER, PSI_CODE, AGE_AT_ENROL_DATE)

# qry07b2: Flag first enrolments in extract_no_age
extract_no_age <- extract_no_age %>%
  mutate(IS_FIRST_ENROLMENT = if_else(ID %in% extract_no_age_first$ID, "Yes", NA_character_))

# qry07c: Compute age distribution for sampling
age_dist <- min_enrolment %>%
  filter(!is.na(AGE_GROUP_ENROL_DATE), IS_FIRST_ENROLMENT == "Yes") %>%
  count(AGE_AT_ENROL_DATE, PSI_GENDER, name = "NUMENROLLED")

n_miss <- extract_no_age_first %>%
  filter(is.na(AGE_AT_ENROL_DATE)) %>%
  count(PSI_GENDER, name = "N_MISS")

age_dist <- age_dist %>%
  group_by(PSI_GENDER) %>%
  mutate(PROPENROLLED = round(NUMENROLLED / sum(NUMENROLLED), 5)) %>%
  ungroup() %>%
  inner_join(n_miss, by = "PSI_GENDER") %>%
  mutate(NUMDISTRIBUTION = round(PROPENROLLED * N_MISS)) %>%
  select(-N_MISS)

dbWriteTable(con, "AgeDistributionbyGender", age_dist, overwrite = TRUE)

# ---- Sample ages proportionally by gender ----
m_id <- extract_no_age_first %>%
  filter(PSI_GENDER == "Male", is.na(AGE_AT_ENROL_DATE)) %>% pull(ID)

f_id <- extract_no_age_first %>%
  filter(PSI_GENDER == "Female", is.na(AGE_AT_ENROL_DATE)) %>% pull(ID)

gd_id <- extract_no_age_first %>%
  filter(PSI_GENDER == "Gender Diverse", is.na(AGE_AT_ENROL_DATE)) %>% pull(ID)

m_dist <- age_dist %>% filter(NUMDISTRIBUTION > 0, PSI_GENDER == "Male")
f_dist <- age_dist %>% filter(NUMDISTRIBUTION > 0, PSI_GENDER == "Female")
gd_dist <- age_dist %>% filter(NUMDISTRIBUTION > 0, PSI_GENDER == "Gender Diverse")

m_sampled <- data.frame(
  ID = m_id,
  AGE_AT_ENROL_DATE = sample(m_dist$AGE_AT_ENROL_DATE, size = length(m_id),
                              replace = TRUE, prob = m_dist$PROPENROLLED)
)
f_sampled <- data.frame(
  ID = f_id,
  AGE_AT_ENROL_DATE = sample(f_dist$AGE_AT_ENROL_DATE, size = length(f_id),
                              replace = TRUE, prob = f_dist$PROPENROLLED)
)
# Gender Diverse groups may be too small for proportional sampling
gd_sampled <- tryCatch(
  data.frame(
    ID = gd_id,
    AGE_AT_ENROL_DATE = sample(gd_dist$AGE_AT_ENROL_DATE, size = length(gd_id),
                                replace = TRUE, prob = gd_dist$PROPENROLLED)
  ),
  error = function(e) data.frame(ID = gd_id, AGE_AT_ENROL_DATE = NA_real_)
)

# Apply sampled ages to first enrolments
extract_no_age_first <- extract_no_age_first %>%
  left_join(bind_rows(m_sampled, f_sampled), by = "ID", suffix = c("", ".new")) %>%
  mutate(AGE_AT_ENROL_DATE = if_else(is.na(AGE_AT_ENROL_DATE),
                                      AGE_AT_ENROL_DATE.NEW, AGE_AT_ENROL_DATE)) %>%
  select(-AGE_AT_ENROL_DATE.NEW)

# Fill remaining GD records from any available age
no_age_ids <- extract_no_age_first %>%
  filter(is.na(AGE_AT_ENROL_DATE)) %>% pull(ID)

if (length(no_age_ids) > 0) {
  gd_fill <- data.frame(
    ID = no_age_ids,
    AGE_AT_ENROL_DATE = sample(
      extract_no_age_first %>% filter(!is.na(AGE_AT_ENROL_DATE)) %>% pull(AGE_AT_ENROL_DATE),
      size = length(no_age_ids)
    )
  )
  extract_no_age_first <- extract_no_age_first %>%
    left_join(gd_fill, by = "ID", suffix = c("", ".new")) %>%
    mutate(AGE_AT_ENROL_DATE = if_else(is.na(AGE_AT_ENROL_DATE),
                                        AGE_AT_ENROL_DATE.NEW, AGE_AT_ENROL_DATE)) %>%
    select(-AGE_AT_ENROL_DATE.NEW)
}

dbWriteTable(con, "Extract_No_Age_First_Enrolment", extract_no_age_first, overwrite = TRUE)


# qry07d1: Update Extract_No_Age with first enrolment ages
extract_no_age <- extract_no_age %>%
  left_join(
    extract_no_age_first %>% select(ID, AGE_AT_ENROL_DATE),
    by = "ID", suffix = c("", ".first")
  ) %>%
  mutate(AGE_AT_ENROL_DATE = coalesce(AGE_AT_ENROL_DATE, AGE_AT_ENROL_DATE.FIRST)) %>%
  select(-AGE_AT_ENROL_DATE.FIRST)


# ---- qry02a–02b: Calculate ages for students with multiple enrolments ----
# WHY: If a student was 20 at their first enrolment in 2015/2016, they should be
# ~21 in 2016/2017, ~22 in 2017/2018, etc. This loop computes missing ages from
# the first enrolment's known age + year difference.
# KEPT AS R LOOP: The original already uses R for this calculation.

multiple_enrol <- extract_no_age %>%
  filter(!is.na(PSI_STUDENT_NUMBER), !is.na(PSI_CODE)) %>%
  count(PSI_STUDENT_NUMBER, PSI_CODE, name = "n") %>%
  filter(n > 1)

calc_ages <- extract_no_age %>%
  semi_join(multiple_enrol, by = c("PSI_STUDENT_NUMBER", "PSI_CODE")) %>%
  arrange(PSI_STUDENT_NUMBER, PSI_CODE, PSI_MIN_START_DATE_D, desc(IS_FIRST_ENROLMENT)) %>%
  select(ID, PSI_STUDENT_NUMBER, PSI_CODE, PSI_SCHOOL_YEAR,
         PSI_MIN_START_DATE_D, AGE_AT_ENROL_DATE, IS_FIRST_ENROLMENT) %>%
  as.data.frame()

for (i in seq_len(nrow(multiple_enrol))) {
  sn <- multiple_enrol$PSI_STUDENT_NUMBER[i]
  code <- multiple_enrol$PSI_CODE[i]
  rs <- calc_ages[calc_ages$PSI_STUDENT_NUMBER == sn & calc_ages$PSI_CODE == code,
                   c("PSI_STUDENT_NUMBER", "PSI_CODE", "PSI_MIN_START_DATE_D", "AGE_AT_ENROL_DATE")]

  if (nrow(rs) > 0 && !is.na(rs$AGE_AT_ENROL_DATE[1])) {
    date1 <- as.POSIXlt(rs$PSI_MIN_START_DATE_D[1])
    age1 <- rs$AGE_AT_ENROL_DATE[1]
    rs$AGE_AT_ENROL_DATE_NEW <- NA_real_
    rs$AGE_AT_ENROL_DATE_NEW[1] <- rs$AGE_AT_ENROL_DATE[1]
    for (j in 2:nrow(rs)) {
      date2 <- as.POSIXlt(rs$PSI_MIN_START_DATE_D[j])
      rs$AGE_AT_ENROL_DATE_NEW[j] <- age1 + (date2$year - date1$year)
    }
    calc_ages <- merge(calc_ages,
                        rs[, c("PSI_STUDENT_NUMBER", "PSI_CODE", "PSI_MIN_START_DATE_D",
                               "AGE_AT_ENROL_DATE", "AGE_AT_ENROL_DATE_NEW")],
                        by = c("PSI_STUDENT_NUMBER", "PSI_CODE", "PSI_MIN_START_DATE_D",
                               "AGE_AT_ENROL_DATE"),
                        all.x = TRUE)
    calc_ages$AGE_AT_ENROL_DATE <- ifelse(is.na(calc_ages$AGE_AT_ENROL_DATE),
                                           calc_ages$AGE_AT_ENROL_DATE_NEW,
                                           calc_ages$AGE_AT_ENROL_DATE)
    calc_ages$AGE_AT_ENROL_DATE_NEW <- NULL
  }
}

calc_ages_final <- calc_ages %>%
  as_tibble() %>%
  select(ID, AGE_AT_ENROL_DATE)

# Update Extract_No_Age with computed ages
extract_no_age <- extract_no_age %>%
  left_join(calc_ages_final, by = "ID", suffix = c("", ".computed")) %>%
  mutate(
    AGE_AT_ENROL_DATE = if_else(
      is.na(AGE_AT_ENROL_DATE) & !is.na(AGE_AT_ENROL_DATE.COMPUTED),
      AGE_AT_ENROL_DATE.COMPUTED, AGE_AT_ENROL_DATE
    )
  ) %>%
  select(-AGE_AT_ENROL_DATE.COMPUTED)


# ---- qry07d2–07d3: Manual age fixes (self-join on birthdate) ----
# WHY: For remaining records with no age, try to get birthdate from another record
# of the same student, then compute age from birthdate + start date.
# In SQL this was a view with self-join; in dplyr we do it directly.

no_age_remaining <- extract_no_age %>%
  filter(is.na(AGE_AT_ENROL_DATE)) %>%
  distinct(PSI_STUDENT_NUMBER, PSI_CODE)

# Get birthdates from matching students in MinEnrolment
available_birthdates <- min_enrolment %>%
  semi_join(no_age_remaining, by = c("PSI_STUDENT_NUMBER", "PSI_CODE")) %>%
  filter(!is.na(PSI_BIRTHDATE_CLEANED_D)) %>%
  distinct(PSI_STUDENT_NUMBER, PSI_CODE, PSI_BIRTHDATE_CLEANED_D)

# Compute age from found birthdates
no_age_with_bday <- extract_no_age %>%
  filter(is.na(AGE_AT_ENROL_DATE)) %>%
  left_join(available_birthdates, by = c("PSI_STUDENT_NUMBER", "PSI_CODE"),
            suffix = c("", ".other")) %>%
  mutate(
    PSI_BIRTHDATE_CLEANED_D = coalesce(PSI_BIRTHDATE_CLEANED_D, PSI_BIRTHDATE_CLEANED_D.OTHER),
    AGE_AT_ENROL_DATE = if_else(
      is.na(AGE_AT_ENROL_DATE) & !is.na(PSI_BIRTHDATE_CLEANED_D) & !is.na(PSI_MIN_START_DATE_D),
      compute_age_at_date(PSI_BIRTHDATE_CLEANED_D, PSI_MIN_START_DATE_D),
      AGE_AT_ENROL_DATE
    )
  ) %>%
  select(ID, AGE_AT_ENROL_DATE)

# Merge back
extract_no_age <- extract_no_age %>%
  left_join(no_age_with_bday, by = "ID", suffix = c("", ".fix")) %>%
  mutate(AGE_AT_ENROL_DATE = coalesce(AGE_AT_ENROL_DATE, AGE_AT_ENROL_DATE.FIX)) %>%
  select(-AGE_AT_ENROL_DATE.FIX)


# qry07e: Update MinEnrolment with all computed ages
min_enrolment <- min_enrolment %>%
  left_join(
    extract_no_age %>% select(ID, AGE_AT_ENROL_DATE),
    by = "ID", suffix = c("", ".fixed")
  ) %>%
  mutate(AGE_AT_ENROL_DATE = coalesce(AGE_AT_ENROL_DATE, AGE_AT_ENROL_DATE.FIXED)) %>%
  select(-AGE_AT_ENROL_DATE.FIXED)

# qry08: Final age group lookup
min_enrolment <- min_enrolment %>%
  mutate(AGE_GROUP_ENROL_DATE = lookup_age_group(AGE_AT_ENROL_DATE, age_lookup))

# Cleanup temp tables from database
# KEPT AS SQL: DROP TABLE (cleanup of intermediate tables)
dbExecute(con, glue::glue(
  "IF OBJECT_ID('{my_schema}.Extract_No_Age_First_Enrolment') IS NOT NULL ",
  "DROP TABLE [{my_schema}].Extract_No_Age_First_Enrolment;"
))
dbExecute(con, glue::glue(
  "IF OBJECT_ID('{my_schema}.AgeDistributionbyGender') IS NOT NULL ",
  "DROP TABLE [{my_schema}].AgeDistributionbyGender;"
))

rm(extract_no_age, extract_no_age_first, calc_ages, calc_ages_final,
   multiple_enrol, no_age_remaining, no_age_with_bday, available_birthdates,
   m_sampled, f_sampled, gd_sampled, m_id, f_id, gd_id,
   m_dist, f_dist, gd_dist, no_age_ids)


# ******************************************************************************
# Part 7: Write MinEnrolment to database
# WHY: Downstream scripts (04-graduate-projections, 06-program-projections, etc.)
# reference MinEnrolment as a database table. In the original, this was a SQL VIEW.
# ******************************************************************************
dbWriteTable(con,
             SQL(glue::glue('"{my_schema}"."MinEnrolment"')),
             min_enrolment,
             overwrite = TRUE)


# ******************************************************************************
# Part 8: Final distributions (qry09c)
# Produce summary tables for reporting and model validation.
# WHY: These distribution tables show enrolment counts broken down by various
# dimensions (gender, age, credential, CIP code).
# ******************************************************************************

# qry09c: Enrolment by credential and CIP code
qry09c_by_cred_cip <- min_enrolment %>%
  count(PSI_SCHOOL_YEAR, PSI_CREDENTIAL_CATEGORY, PSI_CIP_CODE, name = "EXPR1") %>%
  arrange(PSI_CREDENTIAL_CATEGORY, PSI_CIP_CODE)

dbWriteTable(con,
             SQL(glue::glue('"{my_schema}"."qry09c_MinEnrolment_by_Credential_and_CIP_Code"')),
             qry09c_by_cred_cip, overwrite = TRUE)

# qry09c: Domestic enrolment by gender/age group/year
qry09c_domestic <- min_enrolment %>%
  filter(PSI_VISA_STATUS == "DOMESTIC") %>%
  inner_join(age_lookup %>% select(AGEINDEX, AGEGROUP),
             by = c("AGE_GROUP_ENROL_DATE" = "AGEINDEX")) %>%
  count(PSI_GENDER, AGEGROUP, PSI_SCHOOL_YEAR, name = "EXPR1") %>%
  arrange(PSI_GENDER, AGEGROUP, PSI_SCHOOL_YEAR)

dbWriteTable(con,
             SQL(glue::glue('"{my_schema}"."qry09c_MinEnrolment_Domestic"')),
             qry09c_domestic, overwrite = TRUE)

# qry09c: Enrolment by gender/age group/year (all visa statuses, excluding current year)
# !! UPDATE: Change the excluded year to match the current model run
qry09c_all <- min_enrolment %>%
  inner_join(age_lookup %>% select(AGEINDEX, AGEGROUP),
             by = c("AGE_GROUP_ENROL_DATE" = "AGEINDEX")) %>%
  filter(PSI_SCHOOL_YEAR != "2023/2024") %>%
  mutate(GROUPS = paste0(PSI_GENDER, AGEGROUP)) %>%
  count(PSI_GENDER, GROUPS, PSI_SCHOOL_YEAR, name = "EXPR1") %>%
  arrange(PSI_GENDER, GROUPS, PSI_SCHOOL_YEAR)

dbWriteTable(con,
             SQL(glue::glue('"{my_schema}"."qry09c_MinEnrolment"')),
             qry09c_all, overwrite = TRUE)

# qry09c: Enrolment by PSI type (commented out in original — requires PSI_CODE_RECODE table)
# psi_code_recode <- sch_tbl("PSI_CODE_RECODE") %>% collect() |> rename_with(toupper)
# qry09c_psi_type <- min_enrolment %>%
#   inner_join(age_lookup %>% select(AGEINDEX, AGEGROUP),
#              by = c("AGE_GROUP_ENROL_DATE" = "AGEINDEX")) %>%
#   inner_join(psi_code_recode, by = "PSI_CODE") %>%
#   filter(!AGE_GROUP_ENROL_DATE %in% c(1, 9)) %>%
#   count(PSI_SCHOOL_YEAR, PSI_TYPE_RECODE, PSI_CODE_RECODE, name = "EXPR1") %>%
#   arrange(PSI_SCHOOL_YEAR)
# dbWriteTable(con,
#              SQL(glue::glue('"{my_schema}"."qry09c_MinEnrolment_PSI_TYPE"')),
#              qry09c_psi_type, overwrite = TRUE)


# ---- Final check: verify required tables exist ----
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Credential_Record_Type"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Enrolment_Record_Type"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Enrolment"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Enrolment_Valid"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STP_Credential"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."MinEnrolment"')))

dbDisconnect(con)



# ==============================================================================
# FILE: 02a-appso-programs_dplyr.R
# ==============================================================================


# Create APPSO CIP Records — dplyr Translation
# Original: R/02a-appso-programs.R
#
# Pipeline context:
#   APPSO (Apprentice outcomes) represent apprenticeship credentials reported by
#   institutions. Their CIP codes are institution-reported and may not conform to the
#   standard INFOWARE taxonomy. This script cleans those CIP codes and matches them
#   to the official taxonomy so they can be used in downstream program/occupation matching.
#
#   This is one of four program matching scripts that each produce a CIP-matched ID table:
#     02a-dacso-program-matching  → Credential_Non_Dup_Programs_DACSO_FinalCIPs
#     02a-bgs-program-matching    → Credential_Non_Dup_BGS_IDs
#     (GRAD matching)             → Credential_Non_Dup_GRAD_IDs
#     02a-appso-programs          → Credential_Non_Dup_APPSO_IDs  ← THIS SCRIPT
#   All four are consumed by 02a-update-cred-non-dup to populate final CIP codes.
#
# Input tables:
#   - credential_non_dup — main credential table (from 01b-credential-preprocessing)
#   - INFOWARE_L_CIP_6/4/2DIGITS_CIP2016 — official CIP taxonomy lookups
#
# Output table:
#   - Credential_Non_Dup_APPSO_IDs — consumed by 02a-update-cred-non-dup (Step 5)

library(arrow)
library(tidyverse)
library(dbplyr)
library(odbc)
library(DBI)

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

# ---- Check Required Tables ----
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."credential_non_dup"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_2DIGITS_CIP2016"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_4DIGITS_CIP2016"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_6DIGITS_CIP2016"')))

# ==============================================================================
# START QUERIES — Clean APPSO CIP Codes
# ==============================================================================

# Pull reference data from INFOWARE lookup tables.
# These define the official CIP hierarchy: 6-digit → 4-digit → 2-digit.
cip6 <- sch_tbl("INFOWARE_L_CIP_6DIGITS_CIP2016") %>%
  select(LCIP_CD_WITH_PERIOD, LCIP_LCP4_CD, LCIP_LCP2_CD) %>%
  collect() |>
  rename_with(toupper)

cip4 <- sch_tbl("INFOWARE_L_CIP_4DIGITS_CIP2016") %>%
  select(LCP4_CD, LCP4_CIP_4DIGITS_NAME) %>%
  collect() |>
  rename_with(toupper)

cip2 <- sch_tbl("INFOWARE_L_CIP_2DIGITS_CIP2016") %>%
  select(LCP2_CD, LCP2_DIGITS_NAME) %>%
  collect() |>
  rename_with(toupper)

# We need a distinct list of APPSO CIP codes to clean, rather than processing
# every individual credential record. Grouping by CIP + outcome type lets us clean
# each unique CIP code once and then join the results back to all matching records.
# ---- Step 1: Create cleaning table from APPSO records ----

cred_non_dup <- sch_tbl("credential_non_dup") %>% collect()
cred_non_dup <- cred_non_dup |>
  rename_with(toupper) # Ensure column names are uppercase for consistency with original SQL

appso_cleaning <- cred_non_dup %>%
  filter(OUTCOMES_CRED == "APPSO") %>%
  count(PSI_CREDENTIAL_CIP, OUTCOMES_CRED, name = "Expr1") %>%
  mutate(
    # Save original CIP before cleaning so we can join back later (Step 5)
    PSI_CREDENTIAL_CIP_orig = PSI_CREDENTIAL_CIP,
    # Initialize columns that will be filled by the matching steps below
    STP_CIP_CODE_4 = NA_character_,
    STP_CIP_CODE_4_NAME = NA_character_,
    STP_CIP_CODE_2 = NA_character_,
    STP_CIP_CODE_2_NAME = NA_character_
  )

# Some institutions report CIP codes without the standard 7-character format
# (e.g., missing leading zeros or trailing digits). These won't match the INFOWARE
# lookup tables unless we normalize them first.
# ---- Step 2: Fix CIP codes with wrong length ----

appso_cleaning <- appso_cleaning %>%
  mutate(
    PSI_CREDENTIAL_CIP = case_when(
      nchar(PSI_CREDENTIAL_CIP) == 6 &
        !grepl("\\.", substr(PSI_CREDENTIAL_CIP, 1, 2)) ~ paste0(
        PSI_CREDENTIAL_CIP,
        "0"
      ),
      TRUE ~ PSI_CREDENTIAL_CIP
    ),
    PSI_CREDENTIAL_CIP = case_when(
      nchar(PSI_CREDENTIAL_CIP) == 6 ~ paste0("0", PSI_CREDENTIAL_CIP),
      TRUE ~ PSI_CREDENTIAL_CIP
    )
  )

# Each credential needs both a 4-digit and 2-digit CIP code for downstream
# program and occupation matching (scripts 06 and 07). The INFOWARE tables define
# the official hierarchy. We try progressively shorter matches because some
# institution CIPs don't have an exact 6-digit match in the taxonomy.
# ---- Step 3: Match CIP codes to INFOWARE taxonomy ----

# Step 3a: Exact match on full 6-digit CIP → gives us both 4-digit and 2-digit
appso_cleaning <- appso_cleaning %>%
  left_join(cip6, by = c("PSI_CREDENTIAL_CIP" = "LCIP_CD_WITH_PERIOD")) %>%
  mutate(
    STP_CIP_CODE_4 = coalesce(LCIP_LCP4_CD, STP_CIP_CODE_4),
    STP_CIP_CODE_2 = coalesce(LCIP_LCP2_CD, STP_CIP_CODE_2)
  ) %>%
  select(-LCIP_LCP4_CD, -LCIP_LCP2_CD)

# Step 3b: Partial match on first 5 digits for CIPs that didn't match exactly.
# Some valid CIPs differ only in the last digit from a known code.
appso_cleaning <- appso_cleaning %>%
  mutate(PSI_CIP_5 = substr(PSI_CREDENTIAL_CIP, 1, 5)) %>%
  left_join(
    cip6 %>%
      mutate(PSI_CIP_5 = substr(LCIP_CD_WITH_PERIOD, 1, 5)) %>%
      filter(!duplicated(PSI_CIP_5)),
    by = "PSI_CIP_5"
  ) %>%
  mutate(
    STP_CIP_CODE_4 = coalesce(STP_CIP_CODE_4, LCIP_LCP4_CD),
    STP_CIP_CODE_2 = coalesce(STP_CIP_CODE_2, LCIP_LCP2_CD)
  ) %>%
  select(-PSI_CIP_5, -LCIP_CD_WITH_PERIOD, -LCIP_LCP4_CD, -LCIP_LCP2_CD)

# Step 3c: General programs (e.g., "Computer Science" XX.00) → default to XX.01.
# WHY: Some CIP families have a "general" code (XX.00) that doesn't exist in INFOWARE.
# We map these to the first specific sub-category (XX.01) as a reasonable default.
general_programs <- c(
  "11.00",
  "13.00",
  "14.00",
  "19.00",
  "23.00",
  "24.00",
  "26.00",
  "40.00",
  "42.00",
  "45.00",
  "50.00",
  "52.00",
  "55.00"
)

appso_cleaning <- appso_cleaning %>%
  mutate(
    STP_CIP_CODE_4 = case_when(
      substr(PSI_CREDENTIAL_CIP, 1, 5) %in%
        general_programs &
        is.na(STP_CIP_CODE_4) ~ paste0(substr(PSI_CREDENTIAL_CIP, 1, 2), "01"),
      TRUE ~ STP_CIP_CODE_4
    )
  )

# Step 3d: Fall back to first 2 digits for any still-unmatched 2-digit CIP codes.
appso_cleaning <- appso_cleaning %>%
  mutate(PSI_CIP_2 = substr(PSI_CREDENTIAL_CIP, 1, 2)) %>%
  left_join(
    cip6 %>%
      mutate(PSI_CIP_2 = substr(LCIP_CD_WITH_PERIOD, 1, 2)) %>%
      filter(!duplicated(PSI_CIP_2)),
    by = "PSI_CIP_2"
  ) %>%
  mutate(STP_CIP_CODE_2 = coalesce(STP_CIP_CODE_2, LCIP_LCP2_CD)) %>%
  select(-PSI_CIP_2, -LCIP_CD_WITH_PERIOD, -LCIP_LCP4_CD, -LCIP_LCP2_CD)

# The final output needs both CIP codes and their names for reporting and for
# analysts to verify the matches are sensible. These names come from the INFOWARE
# lookup tables at the 4-digit and 2-digit levels.
# ---- Step 4: Add human-readable CIP names ----

# 4-digit CIP names
appso_cleaning <- appso_cleaning %>%
  left_join(cip4, by = c("STP_CIP_CODE_4" = "LCP4_CD")) %>%
  mutate(
    STP_CIP_CODE_4_NAME = coalesce(LCP4_CIP_4DIGITS_NAME, STP_CIP_CODE_4_NAME)
  ) %>%
  select(-LCP4_CIP_4DIGITS_NAME)

# 2-digit CIP names
appso_cleaning <- appso_cleaning %>%
  left_join(cip2, by = c("STP_CIP_CODE_2" = "LCP2_CD")) %>%
  mutate(
    STP_CIP_CODE_2_NAME = coalesce(LCP2_DIGITS_NAME, STP_CIP_CODE_2_NAME)
  ) %>%
  select(-LCP2_DIGITS_NAME)

# Flag unmatched 4-digit CIPs so analysts can investigate
appso_cleaning <- appso_cleaning %>%
  mutate(
    STP_CIP_CODE_4_NAME = ifelse(
      is.na(STP_CIP_CODE_4_NAME),
      "Invalid 4-digit CIP",
      STP_CIP_CODE_4_NAME
    )
  )

# Write intermediate cleaning table (kept for debugging; dropped at end of script)
dbWriteTable(
  con,
  "Credential_Non_Dup_STP_APPSO_Cleaning",
  appso_cleaning,
  overwrite = TRUE
)

# This is the final output consumed by 02a-update-cred-non-dup (Step 5).
# It joins the cleaned CIP codes back to the original credential records, giving
# each APPSO credential a standardized 4-digit and 2-digit CIP code. Join on the
# original (pre-cleaning) CIP code and OUTCOMES_CRED to match the cleaned results
# back to the right credential records.
# ---- Step 5: Create APPSO IDs table ----

appso_ids <- cred_non_dup %>%
  filter(OUTCOMES_CRED == "APPSO") %>%
  select(
    ID,
    PSI_CODE,
    PSI_PROGRAM_CODE,
    PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    PSI_CREDENTIAL_CIP,
    PSI_AWARD_SCHOOL_YEAR,
    OUTCOMES_CRED
  ) %>%
  inner_join(
    appso_cleaning %>%
      select(
        PSI_CREDENTIAL_CIP_orig,
        OUTCOMES_CRED,
        STP_CIP_CODE_4,
        STP_CIP_CODE_4_NAME,
        STP_CIP_CODE_2,
        STP_CIP_CODE_2_NAME
      ),
    by = c("PSI_CREDENTIAL_CIP" = "PSI_CREDENTIAL_CIP_orig", "OUTCOMES_CRED")
  ) %>%
  transmute(
    ID,
    PSI_CODE,
    PSI_PROGRAM_CODE = ifelse(
      PSI_PROGRAM_CODE == "(Unspecified)",
      NA_character_,
      PSI_PROGRAM_CODE
    ),
    PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    PSI_CREDENTIAL_CIP,
    PSI_AWARD_SCHOOL_YEAR,
    OUTCOMES_CRED,
    FINAL_CIP_CODE_4 = STP_CIP_CODE_4,
    FINAL_CIP_CODE_4_NAME = STP_CIP_CODE_4_NAME,
    FINAL_CIP_CODE_2 = STP_CIP_CODE_2,
    FINAL_CIP_CODE_2_NAME = STP_CIP_CODE_2_NAME
  )

dbWriteTable(con, "Credential_Non_Dup_APPSO_IDs", appso_ids, overwrite = TRUE)

# ---- Clean up ----
dbExecute(con, "DROP TABLE Credential_Non_Dup_STP_APPSO_Cleaning")
dbDisconnect(con)



# ==============================================================================
# FILE: 02a-bgs-program-matching_dplyr.R
# ==============================================================================


# BGS Program Matching — dplyr Translation
# Original: R/02a-bgs-program-matching.R
#
# Pipeline context:
#   Aligns CIP codes between BGS (BC Graduate Survey) data from INFOWARE and
#   STP (Student Transitions Project) credential data. The matching is done at
#   the case level using encrypted PENs, then CIP codes are validated/corrected
#   using a multi-step matching process:
#     1. Build outcomes data from BGS survey distributions
#     2. Clean STP credential CIP codes against INFOWARE taxonomy
#     3. Build case-level crosswalk (XWALK) matching BGS to STP on PEN
#     4. Auto-match on institution, award year, and CIP codes
#     5. Manual matching for remaining uncertain cases
#     6. Update Credential_Non_Dup_BGS_IDs with final CIPs
#     7. Update T_BGS_Data_Final_for_OutcomesMatching with final CIPs
#
# Input tables:
#   - INFOWARE_BGS_DIST_19_23, INFOWARE_BGS_DIST_18_22 — BGS distributions (Oracle)
#   - INFOWARE_BGS_COHORT_INFO — BGS cohort info (Oracle)
#   - INFOWARE_L_CIP_*DIGITS_CIP2016 — CIP taxonomy (Oracle)
#   - Credential_Non_Dup — deduplicated credentials (from 02a-update-cred-non-dup)
#   - STP_Credential — credential records (for PSI_PEN)
#
# Output tables:
#   - T_BGS_Data_Final_for_OutcomesMatching — BGS data with matched CIPs
#   - Credential_Non_Dup_BGS_IDs — BGS credentials with final CIPs
#   - Credential_Non_Dup_GRAD_IDs — GRAD credentials with final CIPs
#   - BGS_Matching_STP_Credential_PEN — crosswalk table

options(java.parameters = " -Xmx102400m")

library(tidyverse)
library(RODBC)
library(odbc)
library(DBI)
library(glue)
library(RJDBC)

# Helper: reference a table in the user's schema
my_schema <- config::get("myschema")

sch_tbl <- function(name) {
  tbl(con, dbplyr::in_schema(my_schema, name))
}


# ******************************************************************************
# Read INFOWARE tables from Oracle
# ******************************************************************************
# WHY: BGS survey data and CIP taxonomy live in Oracle/INFOWARE. These are large
# tables (80K+ rows) that need to be transferred to SQL Server for processing.
# The chunked writes are necessary because the ODBC driver has row limits.

iw_config <- config::get("infoware")
jdbc_config <- config::get("jdbc")

jdbcDriver <- JDBC(jdbc_config$class, classPath = jdbc_config$path)

iw_con <- dbConnect(jdbcDriver,
                    iw_config$database,
                    iw_config$uid,
                    iw_config$pwd)

# !! UPDATE THE TABLES to include the desired year ranges !!
INFOWARE_BGS_DIST_19_23  <- dbReadTable(iw_con, "INFOWARE.BGS_DIST_19_23")
INFOWARE_BGS_DIST_18_22  <- dbReadTable(iw_con, "INFOWARE.BGS_DIST_18_22")
INFOWARE_BGS_COHORT_INFO <- dbReadTable(iw_con, "INFOWARE.BGS_COHORT_INFO")
INFOWARE_L_CIP_6DIGITS_CIP2016 <- dbReadTable(iw_con, "INFOWARE.L_CIP_6DIGITS_CIP2016")
INFOWARE_L_CIP_4DIGITS_CIP2016 <- dbReadTable(iw_con, "INFOWARE.L_CIP_4DIGITS_CIP2016")
INFOWARE_L_CIP_2DIGITS_CIP2016 <- dbReadTable(iw_con, "INFOWARE.L_CIP_2DIGITS_CIP2016")

dbDisconnect(iw_con)

# ---- Connect to Decimal ----
db_config <- config::get("decimal")
con <- dbConnect(odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")

# ---- Write INFOWARE tables to Decimal (chunked for large tables) ----
# !! UPDATE ROW NUMBERS to match actual data !!
dbWriteTable(con, "INFOWARE_BGS_DIST_19_23", INFOWARE_BGS_DIST_19_23[1:80000,])
dbWriteTable(con, "INFOWARE_BGS_DIST_19_23", INFOWARE_BGS_DIST_19_23[80001:121074,], append = TRUE)
dbWriteTable(con, "INFOWARE_BGS_DIST_18_22", INFOWARE_BGS_DIST_18_22[1:80000,])
dbWriteTable(con, "INFOWARE_BGS_DIST_18_22", INFOWARE_BGS_DIST_18_22[80001:118632,], append = TRUE)
dbWriteTable(con, "INFOWARE_BGS_COHORT_INFO", INFOWARE_BGS_COHORT_INFO[1:80000,])
dbWriteTable(con, "INFOWARE_BGS_COHORT_INFO", INFOWARE_BGS_COHORT_INFO[80001:160000,], append = TRUE)
dbWriteTable(con, "INFOWARE_BGS_COHORT_INFO", INFOWARE_BGS_COHORT_INFO[160001:240000,], append = TRUE)
dbWriteTable(con, "INFOWARE_BGS_COHORT_INFO", INFOWARE_BGS_COHORT_INFO[240001:290758,], append = TRUE)
dbWriteTable(con, "INFOWARE_L_CIP_6DIGITS_CIP2016", INFOWARE_L_CIP_6DIGITS_CIP2016)
dbWriteTable(con, "INFOWARE_L_CIP_4DIGITS_CIP2016", INFOWARE_L_CIP_4DIGITS_CIP2016)
dbWriteTable(con, "INFOWARE_L_CIP_2DIGITS_CIP2016", INFOWARE_L_CIP_2DIGITS_CIP2016)

# Remove large Oracle tables from R memory
rm(INFOWARE_BGS_DIST_19_23, INFOWARE_BGS_DIST_18_22, INFOWARE_BGS_COHORT_INFO,
   INFOWARE_L_CIP_6DIGITS_CIP2016, INFOWARE_L_CIP_4DIGITS_CIP2016,
   INFOWARE_L_CIP_2DIGITS_CIP2016)


# ******************************************************************************
# PART 1: BUILD OUTCOMES DATA
# ******************************************************************************
# WHY: Combine multiple years of BGS survey distribution data with cohort info
# to create the working outcomes matching table. The BGS data comes in year-range
# tables (e.g., 19_23, 18_22) that need to be unioned and joined with cohort
# information on STQU_ID (the student questionnaire ID).
#
# Original: Two SELECT INTO + one INSERT INTO + one ALTER TABLE + DEFAULT
# Translated: Two inner_join + bind_rows + mutate

bgs_dist_19_23 <- sch_tbl("INFOWARE_BGS_DIST_19_23") %>%
  collect() |> rename_with(toupper)

bgs_dist_18_22 <- sch_tbl("INFOWARE_BGS_DIST_18_22") %>%
  collect() |> rename_with(toupper)

bgs_cohort_info <- sch_tbl("INFOWARE_BGS_COHORT_INFO") %>%
  collect() |> rename_with(toupper)

# Step 1: Join 19_23 distribution with cohort info on STQU_ID
# The cohort info provides CIP codes, program descriptions, and CPC codes.
bgs_outcomes_19_23 <- bgs_dist_19_23 %>%
  inner_join(
    bgs_cohort_info %>%
      select(STQU_ID, PEN, STUDID, SRV_Y_N, SUBM_CD,
             CIP2DIG, CIP2DIG_NAME, CIP4DIG, CIP_4DIGIT_NO_PERIOD, CIP4DIG_NAME,
             CIP_6DIGIT_1, CIP_6DIGIT_NO_PERIOD, CIP6DIG_NAME,
             PROGRAM, DASHBOARD_PROGRAM, CPC),
    by = "STQU_ID"
  )

# Step 2: Join 18_22 distribution with cohort info, keeping only 2018 data
# WHY only 2018: Older data overlaps with the 19_23 range, so we only take the
# earliest year that doesn't overlap.
bgs_outcomes_18_22 <- bgs_dist_18_22 %>%
  filter(YEAR == 2018) %>%
  inner_join(
    bgs_cohort_info %>%
      select(STQU_ID, PEN, STUDID, SRV_Y_N, SUBM_CD,
             CIP2DIG, CIP2DIG_NAME, CIP4DIG, CIP_4DIGIT_NO_PERIOD, CIP4DIG_NAME,
             CIP_6DIGIT_1, CIP_6DIGIT_NO_PERIOD, CIP6DIG_NAME,
             PROGRAM, DASHBOARD_PROGRAM, CPC),
    by = "STQU_ID"
  )

# Combine both year ranges and add PSSM_CREDENTIAL = 'BACH' (all BGS are bachelors)
T_BGS_Data_Final_for_OutcomesMatching <- bind_rows(bgs_outcomes_19_23, bgs_outcomes_18_22) %>%
  mutate(PSSM_CREDENTIAL = "BACH")

dbWriteTable(con, "T_BGS_Data_Final_for_OutcomesMatching",
             T_BGS_Data_Final_for_OutcomesMatching, overwrite = TRUE)

rm(bgs_outcomes_19_23, bgs_outcomes_18_22, bgs_dist_19_23, bgs_dist_18_22, bgs_cohort_info)


# ******************************************************************************
# PART 2: CLEAN CREDENTIAL CIP
# ******************************************************************************
# WHY: STP credential data has CIP codes in various formats (XX.XXXX, X.XXXX,
# XX.XXX) that need to be standardized and matched against the official INFOWARE
# CIP taxonomy to produce clean 4-digit and 2-digit CIP codes. These cleaned
# codes are used for matching BGS survey data to STP credentials.
#
# Original: ~12 SQL operations (SELECT INTO, ALTER TABLE, UPDATE)
# Translated: Single dplyr pipeline with sequential CIP matching steps

# Pull source tables
credential_non_dup <- sch_tbl("Credential_Non_Dup") %>%
  select(ID, PSI_CODE, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
         PSI_CREDENTIAL_CIP, PSI_AWARD_SCHOOL_YEAR, OUTCOMES_CRED) %>%
  collect() |> rename_with(toupper)

cip6 <- sch_tbl("INFOWARE_L_CIP_6DIGITS_CIP2016") %>%
  collect() |> rename_with(toupper)

cip4_ref <- sch_tbl("INFOWARE_L_CIP_4DIGITS_CIP2016") %>%
  collect() |> rename_with(toupper)

cip2_ref <- sch_tbl("INFOWARE_L_CIP_2DIGITS_CIP2016") %>%
  collect() |> rename_with(toupper)

# Build distinct match tables from the 6-digit CIP taxonomy
# WHY: CIP matching is hierarchical — try full 6-digit first, then 4-digit
# prefix, then 2-digit prefix. Each step catches codes the previous missed.
cip6_full <- cip6 %>%
  select(LCIP_CD_WITH_PERIOD, LCIP_LCP4_CD, LCIP_LCP2_CD) %>%
  distinct()

cip6_prefix4 <- cip6 %>%
  mutate(CIP_PREFIX_5 = substr(LCIP_CD_WITH_PERIOD, 1, 5)) %>%
  distinct(CIP_PREFIX_5, LCIP_LCP4_CD, LCIP_LCP2_CD)

cip6_prefix2 <- cip6 %>%
  mutate(CIP_PREFIX_2 = substr(LCIP_CD_WITH_PERIOD, 1, 2)) %>%
  distinct(CIP_PREFIX_2, LCIP_LCP2_CD)

# General CIP codes (00 ending) that should be recoded to 01 ending
general_cip_prefixes <- c("11.00", "13.00", "14.00", "19.00", "23.00", "24.00",
                           "26.00", "40.00", "42.00", "45.00", "50.00", "52.00",
                           "55.00")

# Build CIP cleaning table: count credentials by CIP and outcome type
cip_cleaning <- credential_non_dup %>%
  filter(OUTCOMES_CRED %in% c("BGS", "GRAD")) %>%
  count(PSI_CREDENTIAL_CIP, OUTCOMES_CRED, name = "EXPR1") %>%
  # Preserve original CIP before any cleaning
  mutate(PSI_CREDENTIAL_CIP_ORIG = PSI_CREDENTIAL_CIP) %>%
  # Fix format: XX.XXX (6 chars, no period in first 2) → XX.XXXX0
  mutate(PSI_CREDENTIAL_CIP = if_else(
    nchar(PSI_CREDENTIAL_CIP) == 6 & !grepl("\\.", substr(PSI_CREDENTIAL_CIP, 1, 2)),
    paste0(PSI_CREDENTIAL_CIP, "0"),
    PSI_CREDENTIAL_CIP
  )) %>%
  # Fix format: X.XXXX (6 chars remaining) → 0X.XXXX
  mutate(PSI_CREDENTIAL_CIP = if_else(
    nchar(PSI_CREDENTIAL_CIP) == 6,
    paste0("0", PSI_CREDENTIAL_CIP),
    PSI_CREDENTIAL_CIP
  ))

# Match CIP codes in a sequential cascade: full match → prefix-4 → general → prefix-2
cip_cleaning <- cip_cleaning %>%
  # Step 1a: Full 6-digit match to INFOWARE
  left_join(cip6_full,
    by = c("PSI_CREDENTIAL_CIP" = "LCIP_CD_WITH_PERIOD"),
    suffix = c("", "_full")
  ) %>%
  rename(STP_CIP_CODE_4 = LCIP_LCP4_CD, STP_CIP_CODE_2 = LCIP_LCP2_CD) %>%
  # Step 1b: 4-digit prefix match for remaining NULLs
  mutate(CIP_PREFIX_5 = substr(PSI_CREDENTIAL_CIP, 1, 5)) %>%
  left_join(
    cip6_prefix4 %>% rename(STP_CIP_CODE_4_P4 = LCIP_LCP4_CD, STP_CIP_CODE_2_P4 = LCIP_LCP2_CD),
    by = "CIP_PREFIX_5"
  ) %>%
  mutate(
    STP_CIP_CODE_4 = coalesce(STP_CIP_CODE_4, STP_CIP_CODE_4_P4),
    STP_CIP_CODE_2 = coalesce(STP_CIP_CODE_2, STP_CIP_CODE_2_P4)
  ) %>%
  select(-STP_CIP_CODE_4_P4, -STP_CIP_CODE_2_P4, -CIP_PREFIX_5) %>%
  # Step 1c: Recode general programs (00 ending → 01 ending)
  mutate(
    STP_CIP_CODE_4 = if_else(
      is.na(STP_CIP_CODE_4) & substr(PSI_CREDENTIAL_CIP, 1, 5) %in% general_cip_prefixes,
      paste0(substr(PSI_CREDENTIAL_CIP, 1, 2), "01"),
      STP_CIP_CODE_4
    )
  ) %>%
  # Step 1d: 2-digit prefix match for remaining NULL CIP2
  mutate(CIP_PREFIX_2 = substr(PSI_CREDENTIAL_CIP, 1, 2)) %>%
  left_join(
    cip6_prefix2 %>% rename(STP_CIP_CODE_2_P2 = LCIP_LCP2_CD),
    by = "CIP_PREFIX_2"
  ) %>%
  mutate(STP_CIP_CODE_2 = coalesce(STP_CIP_CODE_2, STP_CIP_CODE_2_P2)) %>%
  select(-STP_CIP_CODE_2_P2, -CIP_PREFIX_2) %>%
  # Step 2: Add 4-digit CIP names from INFOWARE
  left_join(
    cip4_ref %>% select(LCP4_CD, LCP4_CIP_4DIGITS_NAME),
    by = c("STP_CIP_CODE_4" = "LCP4_CD")
  ) %>%
  rename(STP_CIP_CODE_4_NAME = LCP4_CIP_4DIGITS_NAME) %>%
  # Step 3: Add 2-digit CIP names from INFOWARE
  left_join(
    cip2_ref %>% select(LCP2_CD, LCP2_DIGITS_NAME),
    by = c("STP_CIP_CODE_2" = "LCP2_CD")
  ) %>%
  rename(STP_CIP_CODE_2_NAME = LCP2_DIGITS_NAME) %>%
  # Step 4: Mark remaining NULL 4D names as invalid
  mutate(STP_CIP_CODE_4_NAME = if_else(
    is.na(STP_CIP_CODE_4_NAME), "Invalid 4-digit CIP", STP_CIP_CODE_4_NAME
  ))

dbWriteTable(con, "Credential_Non_Dup_STP_CIP4_Cleaning", cip_cleaning, overwrite = TRUE)

# ---- Split into BGS and GRAD credential tables ----
# WHY: BGS credentials need further CIP cleaning via XWALK matching, while GRAD
# credentials use their STP CIPs directly as final (no outcomes matching needed).
# Both are subsets of Credential_Non_Dup joined with the cleaned CIP codes.

Credential_Non_Dup_BGS_IDs <- credential_non_dup %>%
  filter(OUTCOMES_CRED == "BGS") %>%
  inner_join(
    cip_cleaning %>% select(PSI_CREDENTIAL_CIP_ORIG, OUTCOMES_CRED,
                            STP_CIP_CODE_4, STP_CIP_CODE_4_NAME,
                            STP_CIP_CODE_2, STP_CIP_CODE_2_NAME),
    by = c("PSI_CREDENTIAL_CIP" = "PSI_CREDENTIAL_CIP_ORIG",
           "OUTCOMES_CRED" = "OUTCOMES_CRED")
  )

Credential_Non_Dup_GRAD_IDs <- credential_non_dup %>%
  filter(OUTCOMES_CRED == "GRAD") %>%
  inner_join(
    cip_cleaning %>% select(PSI_CREDENTIAL_CIP_ORIG, OUTCOMES_CRED,
                            STP_CIP_CODE_4, STP_CIP_CODE_4_NAME,
                            STP_CIP_CODE_2, STP_CIP_CODE_2_NAME),
    by = c("PSI_CREDENTIAL_CIP" = "PSI_CREDENTIAL_CIP_ORIG",
           "OUTCOMES_CRED" = "OUTCOMES_CRED")
  ) %>%
  rename(
    FINAL_CIP_CODE_4 = STP_CIP_CODE_4,
    FINAL_CIP_CODE_4_NAME = STP_CIP_CODE_4_NAME,
    FINAL_CIP_CODE_2 = STP_CIP_CODE_2,
    FINAL_CIP_CODE_2_NAME = STP_CIP_CODE_2_NAME
  )

# Replace (Unspecified) with NA in BGS_IDs
Credential_Non_Dup_BGS_IDs <- Credential_Non_Dup_BGS_IDs %>%
  mutate(PSI_PROGRAM_CODE = if_else(PSI_PROGRAM_CODE == "(Unspecified)", NA_character_, PSI_PROGRAM_CODE))

dbWriteTable(con, "Credential_Non_Dup_BGS_IDs", Credential_Non_Dup_BGS_IDs, overwrite = TRUE)
dbWriteTable(con, "Credential_Non_Dup_GRAD_IDs", Credential_Non_Dup_GRAD_IDs, overwrite = TRUE)

rm(credential_non_dup, cip_cleaning, cip6_full, cip6_prefix4, cip6_prefix2)


# ******************************************************************************
# PART 3: BUILD CASE-LEVEL XWALK
# ******************************************************************************
# WHY: The crosswalk (XWALK) table matches individual BGS survey respondents to
# their corresponding STP credential records using encrypted PENs. This enables
# CIP code validation — when BGS and STP agree on a CIP code, we have high
# confidence in the assignment. When they disagree, analyst review is needed.

### Part 3A: Initial XWALK ----

# Add PSI_PEN from STP_Credential if not already present
# WHY: Credential_Non_Dup may not have the PEN column; it comes from STP_Credential.
if (!"PSI_PEN" %in% colnames(Credential_Non_Dup_BGS_IDs)) {
  stp_credential_pen <- sch_tbl("STP_Credential") %>%
    select(ID, PSI_PEN) %>%
    collect() |> rename_with(toupper)

  Credential_Non_Dup_BGS_IDs <- Credential_Non_Dup_BGS_IDs %>%
    left_join(stp_credential_pen, by = "ID")

  dbWriteTable(con, "Credential_Non_Dup_BGS_IDs", Credential_Non_Dup_BGS_IDs, overwrite = TRUE)
  rm(stp_credential_pen)
}

# Join BGS data with STP credentials on PEN to create the XWALK
# WHY: Matching on encrypted PEN links BGS survey respondents to their STP
# credential records. The HAVING clause in the original SQL filters out blank/zero PENs.
# In dplyr, we filter before joining to avoid creating invalid matches.
BGS_Matching_STP_Credential_PEN <- T_BGS_Data_Final_for_OutcomesMatching %>%
  filter(!is.na(PEN) & PEN != "" & PEN != "0") %>%
  inner_join(
    Credential_Non_Dup_BGS_IDs %>%
      filter(OUTCOMES_CRED == "BGS") %>%
      select(ID, PSI_PEN, OUTCOMES_CRED, PSI_CODE, PSI_AWARD_SCHOOL_YEAR,
             STP_CIP_CODE_4, STP_CIP_CODE_4_NAME, STP_CIP_CODE_2, STP_CIP_CODE_2_NAME,
             PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION),
    by = c("PEN" = "PSI_PEN")
  ) %>%
  select(
    STQU_ID, ID, PEN, OUTCOMES_CRED,
    INSTITUTION_CODE, PSI_CODE, YEAR, PSI_AWARD_SCHOOL_YEAR,
    BGS_FINAL_CIP_CODE_4 = CIP_4DIGIT_NO_PERIOD,
    BGS_FINAL_CIP_CODE_4_NAME = CIP4DIG_NAME,
    STP_FINAL_CIP_CODE_4 = STP_CIP_CODE_4,
    STP_FINAL_CIP_CODE_4_NAME = STP_CIP_CODE_4_NAME,
    BGS_FINAL_CIP_CODE_2 = CIP2DIG,
    BGS_FINAL_CIP_CODE_2_NAME = CIP2DIG_NAME,
    STP_FINAL_CIP_CODE_2 = STP_CIP_CODE_2,
    STP_FINAL_CIP_CODE_2_NAME = STP_CIP_CODE_2_NAME,
    BGS_PROGRAM_CODE = CPC,
    BGS_PROGRAM_DESC = PROGRAM,
    STP_PROGRAM_CODE = PSI_PROGRAM_CODE,
    STP_PROGRAM_DESC = PSI_CREDENTIAL_PROGRAM_DESCRIPTION
  ) %>%
  distinct()

dbWriteTable(con, "BGS_Matching_STP_Credential_PEN", BGS_Matching_STP_Credential_PEN, overwrite = TRUE)


### Part 3B: Auto matching using flags ----

# WHY: We compute match flags to classify the quality of each BGS-STP match.
# The flags indicate whether institution, award year, 4-digit CIP, and 2-digit CIP
# all agree. "All 3" matches (inst + year + CIP) are auto-accepted.

# Institution code mapping: many BC post-secondary institutions changed names/codes
# over time. This mapping handles known equivalences.
inst_code_pairs <- tribble(
  ~PSI_CODE, ~INSTITUTION_CODE,
  "CAPU",    "CAP",
  "CAP",     "CAPU",
  "DOUG",    "DGL",
  "UCC",     "TRU",
  "ECIAD",   "ECU",
  "ECIAD",   "ECUAD",
  "ECU",     "ECUAD",
  "ECU",     "ECIAD",
  "KWAN",    "KPU",
  "KWAN",    "KWN",
  "KPU",     "KWN",
  "MALA",    "VIU",
  "MALA",    "MAL",
  "OUC",     "OKAN",
  "OUC",     "OKN",
  "OKAN",    "OKN",
  "OKAN",    "OUC",
  "OLA",     "TRUOL",
  "UCFV",    "UFV",
  "UCFV",    "FVAL",
  "UFV",     "FVAL",
  "UFV",     "UCFV",
  "MAL",     "VIU",
  "UBCO",    "UBC",
  "UBCV",    "UBC"
)

# Award year mapping: BGS surveys are 2 years after graduation, and school years
# span two calendar years. This maps each BGS survey year to the two possible
# STP award school years.
award_year_map <- tribble(
  ~YEAR, ~PSI_AWARD_SCHOOL_YEAR,
  2000L, "1997/1998", 2000L, "1998/1999",
  2002L, "1999/2000", 2002L, "2000/2001",
  2004L, "2001/2002", 2004L, "2002/2003",
  2006L, "2003/2004", 2006L, "2004/2005",
  2008L, "2005/2006", 2008L, "2006/2007",
  2009L, "2006/2007", 2009L, "2007/2008",
  2010L, "2007/2008", 2010L, "2008/2009",
  2011L, "2008/2009", 2011L, "2009/2010",
  2012L, "2009/2010", 2012L, "2010/2011",
  2013L, "2010/2011", 2013L, "2011/2012",
  2014L, "2011/2012", 2014L, "2012/2013",
  2015L, "2012/2013", 2015L, "2013/2014",
  2016L, "2013/2014", 2016L, "2014/2015",
  2017L, "2014/2015", 2017L, "2015/2016",
  2018L, "2015/2016", 2018L, "2016/2017",
  2019L, "2016/2017", 2019L, "2017/2018",
  2020L, "2017/2018", 2020L, "2018/2019",
  2021L, "2018/2019", 2021L, "2019/2020",
  2022L, "2019/2020", 2022L, "2020/2021",
  2023L, "2020/2021", 2023L, "2021/2022"
)

# Compute all match flags in a single pipeline
# WHY: The original had 7 separate UPDATE statements. In dplyr, we compute all
# flags at once using vectorized operations.
BGS_Matching_STP_Credential_PEN <- BGS_Matching_STP_Credential_PEN %>%
  mutate(
    # Match_Inst: same institution or known equivalent
    Match_Inst = if_else(
      PSI_CODE == INSTITUTION_CODE |
        paste(PSI_CODE, INSTITUTION_CODE) %in% paste(inst_code_pairs$PSI_CODE, inst_code_pairs$INSTITUTION_CODE),
      "Yes", NA_character_
    ),
    # Match_Award_School_Year: BGS year maps to STP award year (2-year lag)
    Match_Award_School_Year = if_else(
      paste(YEAR, PSI_AWARD_SCHOOL_YEAR) %in% paste(award_year_map$YEAR, award_year_map$PSI_AWARD_SCHOOL_YEAR),
      "Yes", NA_character_
    ),
    # Match_CIP_CODE_4: 4-digit CIP codes agree
    Match_CIP_CODE_4 = if_else(BGS_FINAL_CIP_CODE_4 == STP_FINAL_CIP_CODE_4, "Yes", NA_character_),
    # Match_CIP_CODE_2: 2-digit CIP codes agree
    Match_CIP_CODE_2 = if_else(BGS_FINAL_CIP_CODE_2 == STP_FINAL_CIP_CODE_2, "Yes", NA_character_),
    # Match_All_3_CIP4_Flag: inst + year + CIP4 all match
    Match_All_3_CIP4_Flag = if_else(
      Match_CIP_CODE_4 == "Yes" & Match_Award_School_Year == "Yes" & Match_Inst == "Yes",
      "Yes", NA_character_
    ),
    # Match_All_3_CIP2_Flag: inst + year + CIP2 all match
    Match_All_3_CIP2_Flag = if_else(
      Match_CIP_CODE_2 == "Yes" & Match_Award_School_Year == "Yes" & Match_Inst == "Yes",
      "Yes", NA_character_
    )
  )

# Initialize final columns (these get filled by the matching steps below)
BGS_Matching_STP_Credential_PEN <- BGS_Matching_STP_Credential_PEN %>%
  mutate(
    Final_Consider_A_Match = NA_character_,
    Final_Probable_Match = NA_character_,
    FINAL_CIP_CODE_4 = NA_character_,
    FINAL_CIP_CODE_4_NAME = NA_character_,
    FINAL_CIP_CODE_2 = NA_character_,
    FINAL_CIP_CODE_2_NAME = NA_character_,
    FINAL_CIP_CLUSTER_CODE = NA_character_,
    FINAL_CIP_CLUSTER_NAME = NA_character_,
    USE_BGS_CIP = NA_character_
  )

# ---- Set final CIPs for Match_All_3_CIP4_Flag matches ----
# WHY: When all three match criteria agree (inst, year, CIP4), BGS and STP CIPs
# are identical, so we use BGS CIP as final (same as STP in this case).
BGS_Matching_STP_Credential_PEN <- BGS_Matching_STP_Credential_PEN %>%
  mutate(
    Final_Consider_A_Match = if_else(Match_All_3_CIP4_Flag == "Yes", "Yes", Final_Consider_A_Match),
    FINAL_CIP_CODE_4 = if_else(Match_All_3_CIP4_Flag == "Yes", BGS_FINAL_CIP_CODE_4, FINAL_CIP_CODE_4),
    FINAL_CIP_CODE_2 = if_else(Match_All_3_CIP4_Flag == "Yes", BGS_FINAL_CIP_CODE_2, FINAL_CIP_CODE_2),
    USE_BGS_CIP = if_else(Match_All_3_CIP4_Flag == "Yes", "Yes", USE_BGS_CIP)
  )

# ---- CIP2 review and matching ----
# WHY: When CIP4 doesn't match but CIP2 does (with inst + year), we need to decide
# which source has the more appropriate CIP. The logic prefers the more specific
# CIP code and falls back to STP for consistency.

# Get aggregated CIP2-only matches (CIP2 match but no CIP4 match)
matched_2d_cips <- BGS_Matching_STP_Credential_PEN %>%
  filter(Match_All_3_CIP2_Flag == "Yes" & is.na(Match_All_3_CIP4_Flag)) %>%
  group_by(INSTITUTION_CODE, PSI_CODE, YEAR, PSI_AWARD_SCHOOL_YEAR,
           BGS_PROGRAM_CODE, STP_PROGRAM_CODE, BGS_PROGRAM_DESC, STP_PROGRAM_DESC,
           BGS_FINAL_CIP_CODE_4, BGS_FINAL_CIP_CODE_4_NAME,
           STP_FINAL_CIP_CODE_4, STP_FINAL_CIP_CODE_4_NAME,
           BGS_FINAL_CIP_CODE_2, BGS_FINAL_CIP_CODE_2_NAME,
           STP_FINAL_CIP_CODE_2, STP_FINAL_CIP_CODE_2_NAME,
           Match_All_3_CIP2_Flag) %>%
  summarise(Expr1 = n(), .groups = "drop")

# Get full crosswalk for reference
t1 <- BGS_Matching_STP_Credential_PEN %>%
  group_by(across(everything())) %>%
  summarise(Expr1 = n(), .groups = "drop")

# Step 1: Use more detailed CIP when one source has a "general program" code
general_cip4s <- c("1101", "1301", "1401", "1901", "2301", "2401", "2601",
                    "4001", "4201", "4501", "5001", "5201", "5501")

matched_2d_cips <- matched_2d_cips %>%
  mutate(CIP_TO_USE = case_when(
    BGS_FINAL_CIP_CODE_4 %in% general_cip4s ~ "STP",
    STP_FINAL_CIP_CODE_4 %in% general_cip4s ~ "BGS",
    TRUE ~ NA_character_
  ))

# Step 2A: Match STP program to STP programs where CIPs also match — use STP
matched_2d_cips <- matched_2d_cips %>%
  left_join(
    t1 %>% distinct(INSTITUTION_CODE, STP_PROGRAM_CODE, STP_PROGRAM_DESC,
                    CIP = BGS_FINAL_CIP_CODE_4, STP_FINAL_CIP_CODE_4),
    by = c("INSTITUTION_CODE", "STP_PROGRAM_CODE", "STP_PROGRAM_DESC", "STP_FINAL_CIP_CODE_4")
  ) %>%
  mutate(CIP_TO_USE = if_else(!is.na(CIP_TO_USE), CIP_TO_USE,
                               if_else(!is.na(CIP), "STP", NA_character_))) %>%
  select(-CIP)

# Step 2B: Match BGS program to BGS programs where CIPs also match — use BGS
matched_2d_cips <- matched_2d_cips %>%
  left_join(
    t1 %>% distinct(INSTITUTION_CODE, BGS_PROGRAM_CODE, BGS_PROGRAM_DESC,
                    BGS_FINAL_CIP_CODE_4, CIP = STP_FINAL_CIP_CODE_4),
    by = c("INSTITUTION_CODE", "BGS_PROGRAM_CODE", "BGS_PROGRAM_DESC", "BGS_FINAL_CIP_CODE_4")
  ) %>%
  mutate(CIP_TO_USE = if_else(!is.na(CIP_TO_USE), CIP_TO_USE,
                               if_else(!is.na(CIP), "BGS", NA_character_))) %>%
  select(-CIP)

# Step 3: Custom year-specific overrides
# BGS cip=2701 (Mathematics) vs STP cip=2703 (Applied Mathematics) → use STP
matched_2d_cips <- matched_2d_cips %>%
  mutate(CIP_TO_USE = if_else(
    !is.na(CIP_TO_USE), CIP_TO_USE,
    if_else(BGS_FINAL_CIP_CODE_4 == "2701" & STP_FINAL_CIP_CODE_4 == "2703", "STP", NA_character_)
  ))

# BGS cip=1405 (Bioengineering) vs STP cip=1407 (Chemical Engineering) → use STP
matched_2d_cips <- matched_2d_cips %>%
  mutate(CIP_TO_USE = if_else(
    !is.na(CIP_TO_USE), CIP_TO_USE,
    if_else(BGS_FINAL_CIP_CODE_4 == "1405" & STP_FINAL_CIP_CODE_4 == "1407", "STP", NA_character_)
  ))

# Remaining unmatched: likely double majors with different ordering — use STP
matched_2d_cips <- matched_2d_cips %>%
  mutate(CIP_TO_USE = if_else(is.na(CIP_TO_USE), "STP", CIP_TO_USE))

# Apply CIP2 review results to the XWALK
# WHY: The review determined which source to use for each CIP2 match. We update
# the FINAL_CIP columns and USE_BGS_CIP flag accordingly.
BGS_Matching_STP_Credential_PEN <- BGS_Matching_STP_Credential_PEN %>%
  left_join(
    matched_2d_cips %>%
      select(INSTITUTION_CODE, PSI_CODE, YEAR, PSI_AWARD_SCHOOL_YEAR,
             BGS_PROGRAM_CODE, STP_PROGRAM_CODE, BGS_PROGRAM_DESC, STP_PROGRAM_DESC,
             BGS_FINAL_CIP_CODE_4, STP_FINAL_CIP_CODE_4, Match_All_3_CIP2_Flag, CIP_TO_USE),
    by = c("INSTITUTION_CODE", "PSI_CODE", "YEAR", "PSI_AWARD_SCHOOL_YEAR",
           "BGS_PROGRAM_CODE", "STP_PROGRAM_CODE", "BGS_PROGRAM_DESC", "STP_PROGRAM_DESC",
           "BGS_FINAL_CIP_CODE_4", "STP_FINAL_CIP_CODE_4", "Match_All_3_CIP2_Flag")
  ) %>%
  mutate(
    FINAL_CIP_CODE_4 = case_when(
      !is.na(FINAL_CIP_CODE_4) ~ FINAL_CIP_CODE_4,
      CIP_TO_USE == "BGS" ~ BGS_FINAL_CIP_CODE_4,
      CIP_TO_USE == "STP" ~ STP_FINAL_CIP_CODE_4,
      TRUE ~ FINAL_CIP_CODE_4
    ),
    FINAL_CIP_CODE_2 = case_when(
      !is.na(FINAL_CIP_CODE_2) ~ FINAL_CIP_CODE_2,
      CIP_TO_USE == "BGS" ~ BGS_FINAL_CIP_CODE_2,
      CIP_TO_USE == "STP" ~ STP_FINAL_CIP_CODE_2,
      TRUE ~ FINAL_CIP_CODE_2
    ),
    USE_BGS_CIP = case_when(
      !is.na(USE_BGS_CIP) ~ USE_BGS_CIP,
      CIP_TO_USE == "BGS" ~ "Yes",
      CIP_TO_USE == "STP" ~ "No",
      TRUE ~ USE_BGS_CIP
    ),
    Final_Consider_A_Match = if_else(!is.na(USE_BGS_CIP) & is.na(Final_Consider_A_Match),
                                      "Yes", Final_Consider_A_Match)
  ) %>%
  select(-CIP_TO_USE)

rm(matched_2d_cips, t1)


### Part 3C: Manual matching ----

# WHY: Records that match on institution and award year but not on CIP4 need
# manual review. The analyst exports unmatched program combinations to CSV,
# reviews them (choosing BGS or STP CIP), and reads the results back.

# Get records matching on inst + year but not yet assigned a final CIP
BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_orig <- BGS_Matching_STP_Credential_PEN %>%
  filter(Match_Inst == "Yes" & Match_Award_School_Year == "Yes" & is.na(Final_Consider_A_Match)) %>%
  select(STQU_ID, ID, PEN, INSTITUTION_CODE, PSI_CODE, YEAR, PSI_AWARD_SCHOOL_YEAR,
         Match_Inst, Match_Award_School_Year, Match_All_3_CIP4_Flag, Match_All_3_CIP2_Flag,
         Final_Consider_A_Match, BGS_FINAL_CIP_CODE_4, BGS_FINAL_CIP_CODE_4_NAME,
         STP_FINAL_CIP_CODE_4, STP_FINAL_CIP_CODE_4_NAME,
         BGS_FINAL_CIP_CODE_2, BGS_FINAL_CIP_CODE_2_NAME,
         STP_FINAL_CIP_CODE_2, STP_FINAL_CIP_CODE_2_NAME,
         BGS_PROGRAM_CODE, BGS_PROGRAM_DESC, STP_PROGRAM_CODE, STP_PROGRAM_DESC,
         USE_BGS_CIP)

# Export for manual review
BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_orig %>%
  mutate(across(everything(), trimws)) %>%
  group_by(INSTITUTION_CODE, PSI_CODE, BGS_FINAL_CIP_CODE_4, BGS_FINAL_CIP_CODE_4_NAME,
           STP_FINAL_CIP_CODE_4, STP_FINAL_CIP_CODE_4_NAME, BGS_PROGRAM_CODE,
           BGS_PROGRAM_DESC, STP_PROGRAM_CODE, STP_PROGRAM_DESC, USE_BGS_CIP) %>%
  summarize(Count = n(), .groups = "drop") %>%
  write_csv("BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_ProgramCombos_orig.csv")

# ---- MANUAL STEP ----
# Analyst reviews the CSV, fills in USE_BGS_CIP column (Yes/No/x), and saves as
# BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_ProgramCombos.csv

BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_ProgramCombos <-
  read_csv("BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_ProgramCombos.csv")

# Join manual review back to row-level data
BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly <- BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_orig %>%
  mutate(across(everything(), trimws)) %>%
  select(-USE_BGS_CIP) %>%
  left_join(
    BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_ProgramCombos %>%
      select(-BGS_FINAL_CIP_CODE_4_NAME, -STP_FINAL_CIP_CODE_4_NAME),
    by = c("INSTITUTION_CODE", "PSI_CODE", "BGS_FINAL_CIP_CODE_4",
           "STP_FINAL_CIP_CODE_4", "BGS_PROGRAM_CODE", "BGS_PROGRAM_DESC",
           "STP_PROGRAM_CODE", "STP_PROGRAM_DESC")
  )

# Set final CIPs based on manual review
BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly <- BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly %>%
  mutate(
    FINAL_CIP_CODE_4 = case_when(
      USE_BGS_CIP == "No" ~ STP_FINAL_CIP_CODE_4,
      USE_BGS_CIP == "Yes" ~ BGS_FINAL_CIP_CODE_4
    ),
    FINAL_CIP_CODE_2 = case_when(
      USE_BGS_CIP == "No" ~ STP_FINAL_CIP_CODE_2,
      USE_BGS_CIP == "Yes" ~ BGS_FINAL_CIP_CODE_2
    )
  )

# Update XWALK with manually matched CIPs
# WHY: The manual review results need to be applied to the main XWALK table.
# We match on both ID (STP credential) and STQU_ID (BGS survey) for precision.
BGS_Matching_STP_Credential_PEN <- BGS_Matching_STP_Credential_PEN %>%
  left_join(
    BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly %>%
      select(ID, STQU_ID, FINAL_CIP_CODE_4_manual = FINAL_CIP_CODE_4,
             FINAL_CIP_CODE_2_manual = FINAL_CIP_CODE_2, USE_BGS_CIP_manual = USE_BGS_CIP),
    by = c("ID", "STQU_ID")
  ) %>%
  mutate(
    Final_Probable_Match = if_else(
      is.na(Final_Probable_Match) & is.na(FINAL_CIP_CODE_4) & is.na(FINAL_CIP_CODE_2) &
        !is.na(FINAL_CIP_CODE_4_manual),
      "Yes", Final_Probable_Match
    ),
    FINAL_CIP_CODE_4 = if_else(is.na(FINAL_CIP_CODE_4) & !is.na(FINAL_CIP_CODE_4_manual),
                                FINAL_CIP_CODE_4_manual, FINAL_CIP_CODE_4),
    FINAL_CIP_CODE_2 = if_else(is.na(FINAL_CIP_CODE_2) & !is.na(FINAL_CIP_CODE_2_manual),
                                FINAL_CIP_CODE_2_manual, FINAL_CIP_CODE_2),
    USE_BGS_CIP = if_else(is.na(USE_BGS_CIP) & !is.na(USE_BGS_CIP_manual),
                           USE_BGS_CIP_manual, USE_BGS_CIP)
  ) %>%
  select(-FINAL_CIP_CODE_4_manual, -FINAL_CIP_CODE_2_manual, -USE_BGS_CIP_manual)

# Fill remaining unmatched records with STP CIPs as final
# WHY: Records that couldn't be matched to BGS use STP CIPs as the default.
BGS_Matching_STP_Credential_PEN <- BGS_Matching_STP_Credential_PEN %>%
  mutate(
    FINAL_CIP_CODE_4 = if_else(
      is.na(FINAL_CIP_CODE_4) & is.na(FINAL_CIP_CODE_4_NAME) &
        is.na(FINAL_CIP_CODE_2) & is.na(FINAL_CIP_CODE_2_NAME),
      STP_FINAL_CIP_CODE_4, FINAL_CIP_CODE_4
    ),
    FINAL_CIP_CODE_4_NAME = if_else(
      is.na(FINAL_CIP_CODE_4_NAME) & is.na(FINAL_CIP_CODE_2_NAME),
      STP_FINAL_CIP_CODE_4_NAME, FINAL_CIP_CODE_4_NAME
    ),
    FINAL_CIP_CODE_2 = if_else(
      is.na(FINAL_CIP_CODE_2) & is.na(FINAL_CIP_CODE_2_NAME),
      STP_FINAL_CIP_CODE_2, FINAL_CIP_CODE_2
    ),
    FINAL_CIP_CODE_2_NAME = if_else(
      is.na(FINAL_CIP_CODE_2_NAME),
      STP_FINAL_CIP_CODE_2_NAME, FINAL_CIP_CODE_2_NAME
    ),
    USE_BGS_CIP = if_else(
      is.na(FINAL_CIP_CODE_4) & is.na(FINAL_CIP_CODE_4_NAME) &
        is.na(FINAL_CIP_CODE_2) & is.na(FINAL_CIP_CODE_2_NAME),
      "No", USE_BGS_CIP
    )
  )

rm(BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly,
   BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_orig,
   BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_ProgramCombos)


### Part 3D: Fill in Final Columns ----

# WHY: Fill CIP names and cluster codes from INFOWARE taxonomy. These were left
# NULL during the matching process and need to be populated for downstream use.
BGS_Matching_STP_Credential_PEN <- BGS_Matching_STP_Credential_PEN %>%
  # Fill CIP4 names
  left_join(
    cip4_ref %>% select(LCP4_CD, LCP4_CIP_4DIGITS_NAME),
    by = c("FINAL_CIP_CODE_4" = "LCP4_CD")
  ) %>%
  mutate(FINAL_CIP_CODE_4_NAME = coalesce(FINAL_CIP_CODE_4_NAME, LCP4_CIP_4DIGITS_NAME)) %>%
  select(-LCP4_CIP_4DIGITS_NAME) %>%
  # Fill CIP2 names and cluster codes
  left_join(
    cip2_ref %>% select(LCP2_CD, LCP2_DIGITS_NAME, LCP2_LCIPPC_CD, LCP2_LCIPPC_NAME),
    by = c("FINAL_CIP_CODE_2" = "LCP2_CD")
  ) %>%
  mutate(
    FINAL_CIP_CODE_2_NAME = coalesce(FINAL_CIP_CODE_2_NAME, LCP2_DIGITS_NAME),
    FINAL_CIP_CLUSTER_CODE = coalesce(FINAL_CIP_CLUSTER_CODE, LCP2_LCIPPC_CD),
    FINAL_CIP_CLUSTER_NAME = coalesce(FINAL_CIP_CLUSTER_NAME, LCP2_LCIPPC_NAME)
  ) %>%
  select(-LCP2_DIGITS_NAME, -LCP2_LCIPPC_CD, -LCP2_LCIPPC_NAME)

dbWriteTable(con, "BGS_Matching_STP_Credential_PEN", BGS_Matching_STP_Credential_PEN, overwrite = TRUE)


# ******************************************************************************
# PART 4: UPDATE CREDENTIAL_NON_DUP
# ******************************************************************************
# WHY: Transfer the matched CIPs from the XWALK back to the credential tables.
# This updates Credential_Non_Dup_BGS_IDs with the final CIP assignments from
# the matching process, then handles unmatched programs.

### Part 4A: Update with XWALK ----

# Join BGS_IDs with XWALK to get final CIPs
# WHY: Step 1 fills from XWALK where Final_Consider_A_Match is set (auto-matched).
# Step 2 fills remaining from XWALK where Final_Probable_Match is set (manually matched).
xwalk_step1 <- BGS_Matching_STP_Credential_PEN %>%
  filter(!is.na(Final_Consider_A_Match) & Final_Consider_A_Match != "")

xwalk_step2 <- BGS_Matching_STP_Credential_PEN %>%
  filter(!is.na(Final_Probable_Match) & Final_Probable_Match != "")

Credential_Non_Dup_BGS_IDs <- Credential_Non_Dup_BGS_IDs %>%
  # Add empty columns for final CIP data
  mutate(
    OUTCOMES_CIP_CODE_4 = NA_character_,
    OUTCOMES_CIP_CODE_4_NAME = NA_character_,
    Final_Consider_A_Match = NA_character_,
    Final_Probable_Match = NA_character_,
    USE_BGS_CIP = NA_character_,
    FINAL_CIP_CODE_4 = NA_character_,
    FINAL_CIP_CODE_4_NAME = NA_character_,
    FINAL_CIP_CODE_2 = NA_character_,
    FINAL_CIP_CODE_2_NAME = NA_character_,
    FINAL_CIP_CLUSTER_CODE = NA_character_,
    FINAL_CIP_CLUSTER_NAME = NA_character_
  ) %>%
  # Step 1: Fill from auto-matched XWALK records
  left_join(
    xwalk_step1 %>%
      select(ID, XW_FINAL_CIP_CODE_4 = FINAL_CIP_CODE_4,
             XW_FINAL_CIP_CODE_4_NAME = FINAL_CIP_CODE_4_NAME,
             XW_FINAL_CIP_CODE_2 = FINAL_CIP_CODE_2,
             XW_FINAL_CIP_CODE_2_NAME = FINAL_CIP_CODE_2_NAME,
             XW_FINAL_CIP_CLUSTER_CODE = FINAL_CIP_CLUSTER_CODE,
             XW_FINAL_CIP_CLUSTER_NAME = FINAL_CIP_CLUSTER_NAME,
             XW_USE_BGS_CIP = USE_BGS_CIP,
             XW_OUTCOMES_CIP_CODE_4 = BGS_FINAL_CIP_CODE_4,
             XW_OUTCOMES_CIP_CODE_4_NAME = BGS_FINAL_CIP_CODE_4_NAME,
             XW_Final_Consider_A_Match = Final_Consider_A_Match,
             XW_Final_Probable_Match = Final_Probable_Match),
    by = "ID"
  ) %>%
  mutate(
    FINAL_CIP_CODE_4 = coalesce(FINAL_CIP_CODE_4, XW_FINAL_CIP_CODE_4),
    FINAL_CIP_CODE_4_NAME = coalesce(FINAL_CIP_CODE_4_NAME, XW_FINAL_CIP_CODE_4_NAME),
    FINAL_CIP_CODE_2 = coalesce(FINAL_CIP_CODE_2, XW_FINAL_CIP_CODE_2),
    FINAL_CIP_CODE_2_NAME = coalesce(FINAL_CIP_CODE_2_NAME, XW_FINAL_CIP_CODE_2_NAME),
    FINAL_CIP_CLUSTER_CODE = coalesce(FINAL_CIP_CLUSTER_CODE, XW_FINAL_CIP_CLUSTER_CODE),
    FINAL_CIP_CLUSTER_NAME = coalesce(FINAL_CIP_CLUSTER_NAME, XW_FINAL_CIP_CLUSTER_NAME),
    USE_BGS_CIP = coalesce(USE_BGS_CIP, XW_USE_BGS_CIP),
    OUTCOMES_CIP_CODE_4 = coalesce(OUTCOMES_CIP_CODE_4, XW_OUTCOMES_CIP_CODE_4),
    OUTCOMES_CIP_CODE_4_NAME = coalesce(OUTCOMES_CIP_CODE_4_NAME, XW_OUTCOMES_CIP_CODE_4_NAME),
    Final_Consider_A_Match = coalesce(Final_Consider_A_Match, XW_Final_Consider_A_Match),
    Final_Probable_Match = coalesce(Final_Probable_Match, XW_Final_Probable_Match)
  ) %>%
  select(-starts_with("XW_")) %>%
  # Step 2: Fill remaining from manually matched XWALK records
  left_join(
    xwalk_step2 %>%
      select(ID, XW_FINAL_CIP_CODE_4 = FINAL_CIP_CODE_4,
             XW_FINAL_CIP_CODE_4_NAME = FINAL_CIP_CODE_4_NAME,
             XW_FINAL_CIP_CODE_2 = FINAL_CIP_CODE_2,
             XW_FINAL_CIP_CODE_2_NAME = FINAL_CIP_CODE_2_NAME,
             XW_FINAL_CIP_CLUSTER_CODE = FINAL_CIP_CLUSTER_CODE,
             XW_FINAL_CIP_CLUSTER_NAME = FINAL_CIP_CLUSTER_NAME,
             XW_USE_BGS_CIP = USE_BGS_CIP,
             XW_OUTCOMES_CIP_CODE_4 = BGS_FINAL_CIP_CODE_4,
             XW_OUTCOMES_CIP_CODE_4_NAME = BGS_FINAL_CIP_CODE_4_NAME,
             XW_Final_Probable_Match = Final_Probable_Match),
    by = "ID"
  ) %>%
  mutate(
    FINAL_CIP_CODE_4 = if_else(
      is.na(FINAL_CIP_CODE_4) & is.na(FINAL_CIP_CODE_4_NAME) & is.na(FINAL_CIP_CODE_2) &
        is.na(FINAL_CIP_CODE_2_NAME) & is.na(FINAL_CIP_CLUSTER_CODE) &
        is.na(FINAL_CIP_CLUSTER_NAME) & is.na(USE_BGS_CIP) & is.na(OUTCOMES_CIP_CODE_4) &
        is.na(OUTCOMES_CIP_CODE_4_NAME) & is.na(Final_Probable_Match),
      XW_FINAL_CIP_CODE_4, FINAL_CIP_CODE_4
    ),
    FINAL_CIP_CODE_4_NAME = if_else(
      is.na(FINAL_CIP_CODE_4_NAME) & !is.na(XW_FINAL_CIP_CODE_4_NAME),
      XW_FINAL_CIP_CODE_4_NAME, FINAL_CIP_CODE_4_NAME
    ),
    FINAL_CIP_CODE_2 = if_else(is.na(FINAL_CIP_CODE_2) & !is.na(XW_FINAL_CIP_CODE_2),
                                XW_FINAL_CIP_CODE_2, FINAL_CIP_CODE_2),
    FINAL_CIP_CODE_2_NAME = if_else(is.na(FINAL_CIP_CODE_2_NAME) & !is.na(XW_FINAL_CIP_CODE_2_NAME),
                                     XW_FINAL_CIP_CODE_2_NAME, FINAL_CIP_CODE_2_NAME),
    FINAL_CIP_CLUSTER_CODE = if_else(is.na(FINAL_CIP_CLUSTER_CODE) & !is.na(XW_FINAL_CIP_CLUSTER_CODE),
                                      XW_FINAL_CIP_CLUSTER_CODE, FINAL_CIP_CLUSTER_CODE),
    FINAL_CIP_CLUSTER_NAME = if_else(is.na(FINAL_CIP_CLUSTER_NAME) & !is.na(XW_FINAL_CIP_CLUSTER_NAME),
                                      XW_FINAL_CIP_CLUSTER_NAME, FINAL_CIP_CLUSTER_NAME),
    USE_BGS_CIP = if_else(is.na(USE_BGS_CIP) & !is.na(XW_USE_BGS_CIP),
                           XW_USE_BGS_CIP, USE_BGS_CIP),
    OUTCOMES_CIP_CODE_4 = if_else(is.na(OUTCOMES_CIP_CODE_4) & !is.na(XW_OUTCOMES_CIP_CODE_4),
                                   XW_OUTCOMES_CIP_CODE_4, OUTCOMES_CIP_CODE_4),
    OUTCOMES_CIP_CODE_4_NAME = if_else(is.na(OUTCOMES_CIP_CODE_4_NAME) & !is.na(XW_OUTCOMES_CIP_CODE_4_NAME),
                                        XW_OUTCOMES_CIP_CODE_4_NAME, OUTCOMES_CIP_CODE_4_NAME),
    Final_Probable_Match = if_else(is.na(Final_Probable_Match) & !is.na(XW_Final_Probable_Match),
                                   XW_Final_Probable_Match, Final_Probable_Match)
  ) %>%
  select(-starts_with("XW_"))

rm(xwalk_step1, xwalk_step2)


### Part 4B: Update Unmatched CIPs ----

# WHY: Records that still have no final CIP after XWALK matching use their STP
# CIP codes as the default. The "No because no match" flag indicates these
# weren't matched to BGS outcomes data.
Credential_Non_Dup_BGS_IDs <- Credential_Non_Dup_BGS_IDs %>%
  mutate(
    FINAL_CIP_CODE_4 = if_else(
      is.na(FINAL_CIP_CODE_4) & is.na(FINAL_CIP_CODE_2) &
        is.na(Final_Consider_A_Match) & is.na(Final_Probable_Match),
      STP_CIP_CODE_4, FINAL_CIP_CODE_4
    ),
    FINAL_CIP_CODE_4_NAME = if_else(
      is.na(FINAL_CIP_CODE_4) & is.na(FINAL_CIP_CODE_2) &
        is.na(Final_Consider_A_Match) & is.na(Final_Probable_Match),
      STP_CIP_CODE_4_NAME, FINAL_CIP_CODE_4_NAME
    ),
    FINAL_CIP_CODE_2 = if_else(
      is.na(FINAL_CIP_CODE_2) & is.na(Final_Consider_A_Match) & is.na(Final_Probable_Match),
      STP_CIP_CODE_2, FINAL_CIP_CODE_2
    ),
    FINAL_CIP_CODE_2_NAME = if_else(
      is.na(FINAL_CIP_CODE_2_NAME) & is.na(Final_Consider_A_Match) & is.na(Final_Probable_Match),
      STP_CIP_CODE_2_NAME, FINAL_CIP_CODE_2_NAME
    ),
    USE_BGS_CIP = if_else(
      is.na(FINAL_CIP_CODE_4) & is.na(FINAL_CIP_CODE_2) &
        is.na(Final_Consider_A_Match) & is.na(Final_Probable_Match),
      "No because no match", USE_BGS_CIP
    )
  ) %>%
  # Fill cluster codes from INFOWARE for any still NULL
  left_join(
    cip2_ref %>% select(LCP2_CD, LCP2_LCIPPC_CD, LCP2_LCIPPC_NAME),
    by = c("FINAL_CIP_CODE_2" = "LCP2_CD")
  ) %>%
  mutate(
    FINAL_CIP_CLUSTER_CODE = coalesce(FINAL_CIP_CLUSTER_CODE, LCP2_LCIPPC_CD),
    FINAL_CIP_CLUSTER_NAME = coalesce(FINAL_CIP_CLUSTER_NAME, LCP2_LCIPPC_NAME)
  ) %>%
  select(-LCP2_LCIPPC_CD, -LCP2_LCIPPC_NAME)

# ---- Identify unmatched programs that could use BGS CIPs ----
# WHY: Some programs weren't matched in the XWALK but have BGS CIP data from
# other records in the same program. We identify these and update them.

# Programs using BGS CIPs (matched)
Credential_Matched_CIPS_using_BGS <- Credential_Non_Dup_BGS_IDs %>%
  filter(USE_BGS_CIP == "Yes") %>%
  count(PSI_CODE, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
        OUTCOMES_CIP_CODE_4, OUTCOMES_CIP_CODE_4_NAME,
        STP_CIP_CODE_4, STP_CIP_CODE_4_NAME, STP_CIP_CODE_2, STP_CIP_CODE_2_NAME,
        FINAL_CIP_CODE_4, FINAL_CIP_CODE_4_NAME, FINAL_CIP_CODE_2, FINAL_CIP_CODE_2_NAME,
        FINAL_CIP_CLUSTER_CODE, FINAL_CIP_CLUSTER_NAME,
        Final_Consider_A_Match, Final_Probable_Match, USE_BGS_CIP,
        name = "EXPR1")

# Programs not matched
Credential_Unmatched_CIPS <- Credential_Non_Dup_BGS_IDs %>%
  filter(USE_BGS_CIP == "No because no match") %>%
  count(PSI_CODE, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
        OUTCOMES_CIP_CODE_4, OUTCOMES_CIP_CODE_4_NAME,
        STP_CIP_CODE_4, STP_CIP_CODE_4_NAME, STP_CIP_CODE_2, STP_CIP_CODE_2_NAME,
        FINAL_CIP_CODE_4, FINAL_CIP_CODE_4_NAME, FINAL_CIP_CODE_2, FINAL_CIP_CODE_2_NAME,
        FINAL_CIP_CLUSTER_CODE, FINAL_CIP_CLUSTER_NAME,
        Final_Consider_A_Match, Final_Probable_Match, USE_BGS_CIP,
        name = "EXPR1")

# Find unmatched programs that were matched via BGS for different records
Credential_Unmatched_CIPS_to_review <- Credential_Unmatched_CIPS %>%
  select(-OUTCOMES_CIP_CODE_4, -OUTCOMES_CIP_CODE_4_NAME) %>%
  left_join(
    Credential_Matched_CIPS_using_BGS %>%
      distinct(PSI_CODE, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
               STP_CIP_CODE_4, OUTCOMES_CIP_CODE_4, OUTCOMES_CIP_CODE_4_NAME),
    by = c("PSI_CODE", "PSI_PROGRAM_CODE", "PSI_CREDENTIAL_PROGRAM_DESCRIPTION", "STP_CIP_CODE_4")
  ) %>%
  mutate(
    Unmatched_But_in_BGS_Program = if_else(!is.na(OUTCOMES_CIP_CODE_4), "Yes", NA_character_),
    BGS_CIP_is_Different = if_else(OUTCOMES_CIP_CODE_4 != STP_CIP_CODE_4, "Yes", NA_character_)
  ) %>%
  group_by(PSI_CODE, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION, STP_CIP_CODE_4) %>%
  filter(Unmatched_But_in_BGS_Program == "Yes" & BGS_CIP_is_Different == "Yes") %>%
  ungroup() %>%
  select(-PSI_CREDENTIAL_PROGRAM_DESCRIPTION, everything(), PSI_CREDENTIAL_PROGRAM_DESCRIPTION) %>%
  arrange(FINAL_CIP_CODE_4)

# ---- Year-specific custom updates ----
# WHY: Based on analyst review, these specific programs should use BGS CIPs
# even though they weren't matched in the XWALK. Update for each model run.
# !! UPDATE THIS TABLE FOR EACH MODEL RUN !!
Credential_Unmatched_CIPS_to_update <- tibble::tribble(
  ~PSI_CREDENTIAL_PROGRAM_DESCRIPTION,                                                  ~FINAL_CIP_CODE_4, ~FINAL_CIP_CODE_2,
  "Bachelor Of Applied Science In Mechatronic Systems Engineering",                     "1442",            "14",
  "Bachelor Of Athletic And Exercise Therapy",                                          "5123",            "51",
  "Bachelor Of Fine Arts In Dance",                                                     "5003",            "50",
  "Bachelor Of Fine Arts In Film",                                                      "5006",            "50",
  "Bachelor Of Fine Arts In Music - Composition",                                       "5009",            "50",
  "Bachelor Of Fine Arts In Music - Electroacoustic",                                   "5009",            "50",
  "Bachelor Of Fine Arts In Theatre - Performance",                                     "5005",            "50",
  "Bachelor Of Fine Arts In Theatre - Production And Design",                           "5005",            "50",
  "Bachelor Of Science In Geographic Information Science",                              "4507",            "45",
  "Bachelor Of Social Work In Indigenous Child Welfare",                                "4407",            "44",
  "Bachelor Of Social Work In Indigenous Social Work",                                  "4407",            "44",
  "Bachelor Of Child & Youth Care In Child & Youth Care",                               "1907",            "19",
  "Bachelor Of Child & Youth Care In Child & Youth Care - Child Life Stream",           "1907",            "19",
  "Bachelor Of Child & Youth Care In Child & Youth Care - Early Years Stream",          "1907",            "19",
  "Bachelor Of Child & Youth Care In Child & Youth Care - Child Protection",            "1907",            "19",
  "Bachelor Of Child & Youth Care In Child & Youth Care - Indigenous Stream",           "1907",            "19"
)

# Apply custom updates to unmatched programs
Credential_Non_Dup_BGS_IDs <- Credential_Non_Dup_BGS_IDs %>%
  left_join(
    Credential_Unmatched_CIPS_to_update %>%
      rename(UPD_FINAL_CIP_CODE_4 = FINAL_CIP_CODE_4, UPD_FINAL_CIP_CODE_2 = FINAL_CIP_CODE_2),
    by = "PSI_CREDENTIAL_PROGRAM_DESCRIPTION"
  ) %>%
  mutate(
    FINAL_CIP_CODE_4 = if_else(
      !is.na(UPD_FINAL_CIP_CODE_4) & is.na(Final_Consider_A_Match) & is.na(Final_Probable_Match),
      UPD_FINAL_CIP_CODE_4, FINAL_CIP_CODE_4
    ),
    FINAL_CIP_CODE_4_NAME = if_else(
      !is.na(UPD_FINAL_CIP_CODE_4) & is.na(Final_Consider_A_Match) & is.na(Final_Probable_Match),
      NA_character_, FINAL_CIP_CODE_4_NAME
    ),
    FINAL_CIP_CODE_2 = if_else(
      !is.na(UPD_FINAL_CIP_CODE_2) & is.na(Final_Consider_A_Match) & is.na(Final_Probable_Match),
      UPD_FINAL_CIP_CODE_2, FINAL_CIP_CODE_2
    ),
    FINAL_CIP_CODE_2_NAME = if_else(
      !is.na(UPD_FINAL_CIP_CODE_2) & is.na(Final_Consider_A_Match) & is.na(Final_Probable_Match),
      NA_character_, FINAL_CIP_CODE_2_NAME
    ),
    FINAL_CIP_CLUSTER_CODE = if_else(
      !is.na(UPD_FINAL_CIP_CODE_2) & is.na(Final_Consider_A_Match) & is.na(Final_Probable_Match),
      NA_character_, FINAL_CIP_CLUSTER_CODE
    ),
    FINAL_CIP_CLUSTER_NAME = if_else(
      !is.na(UPD_FINAL_CIP_CODE_2) & is.na(Final_Consider_A_Match) & is.na(Final_Probable_Match),
      NA_character_, FINAL_CIP_CLUSTER_NAME
    )
  ) %>%
  select(-UPD_FINAL_CIP_CODE_4, -UPD_FINAL_CIP_CODE_2)

# Re-fill CIP names and cluster codes from INFOWARE after updates
Credential_Non_Dup_BGS_IDs <- Credential_Non_Dup_BGS_IDs %>%
  left_join(
    cip4_ref %>% select(LCP4_CD, LCP4_CIP_4DIGITS_NAME),
    by = c("FINAL_CIP_CODE_4" = "LCP4_CD")
  ) %>%
  mutate(FINAL_CIP_CODE_4_NAME = coalesce(FINAL_CIP_CODE_4_NAME, LCP4_CIP_4DIGITS_NAME)) %>%
  select(-LCP4_CIP_4DIGITS_NAME) %>%
  left_join(
    cip2_ref %>% select(LCP2_CD, LCP2_DIGITS_NAME, LCP2_LCIPPC_CD, LCP2_LCIPPC_NAME),
    by = c("FINAL_CIP_CODE_2" = "LCP2_CD")
  ) %>%
  mutate(
    FINAL_CIP_CODE_2_NAME = coalesce(FINAL_CIP_CODE_2_NAME, LCP2_DIGITS_NAME),
    FINAL_CIP_CLUSTER_CODE = coalesce(FINAL_CIP_CLUSTER_CODE, LCP2_LCIPPC_CD),
    FINAL_CIP_CLUSTER_NAME = coalesce(FINAL_CIP_CLUSTER_NAME, LCP2_LCIPPC_NAME)
  ) %>%
  select(-LCP2_DIGITS_NAME, -LCP2_LCIPPC_CD, -LCP2_LCIPPC_NAME)

dbWriteTable(con, "Credential_Non_Dup_BGS_IDs", Credential_Non_Dup_BGS_IDs, overwrite = TRUE)

rm(Credential_Matched_CIPS_using_BGS, Credential_Unmatched_CIPS,
   Credential_Unmatched_CIPS_to_review, Credential_Unmatched_CIPS_to_update)


# ******************************************************************************
# PART 5: UPDATE T_BGS_DATA_FINAL
# ******************************************************************************
# WHY: Transfer the matched CIPs from the XWALK to the BGS outcomes data table.
# This mirrors Part 4 but for the BGS survey data rather than STP credentials.

### Part 5A: Update with XWALK ----

xwalk_step1 <- BGS_Matching_STP_Credential_PEN %>%
  filter(!is.na(Final_Consider_A_Match) & Final_Consider_A_Match != "")

xwalk_step2 <- BGS_Matching_STP_Credential_PEN %>%
  filter(!is.na(Final_Probable_Match) & Final_Probable_Match != "")

T_BGS_Data_Final_for_OutcomesMatching <- T_BGS_Data_Final_for_OutcomesMatching %>%
  # Add columns for final CIP data from XWALK
  mutate(
    STP_CIP_CODE_4 = NA_character_,
    STP_CIP_CODE_4_NAME = NA_character_,
    Final_Consider_A_Match = NA_character_,
    Final_Probable_Match = NA_character_,
    USE_BGS_CIP = NA_character_,
    USE_STP_CIP = NA_character_,
    FINAL_CIP_CODE_4 = NA_character_,
    FINAL_CIP_CODE_4_NAME = NA_character_,
    FINAL_CIP_CODE_2 = NA_character_,
    FINAL_CIP_CODE_2_NAME = NA_character_,
    FINAL_CIP_CLUSTER_CODE = NA_character_,
    FINAL_CIP_CLUSTER_NAME = NA_character_
  ) %>%
  # Step 1: Fill from auto-matched XWALK (Final_Consider_A_Match)
  left_join(
    xwalk_step1 %>%
      select(STQU_ID,
             XW_FINAL_CIP_CODE_4 = FINAL_CIP_CODE_4,
             XW_FINAL_CIP_CODE_4_NAME = FINAL_CIP_CODE_4_NAME,
             XW_FINAL_CIP_CODE_2 = FINAL_CIP_CODE_2,
             XW_FINAL_CIP_CODE_2_NAME = FINAL_CIP_CODE_2_NAME,
             XW_FINAL_CIP_CLUSTER_CODE = FINAL_CIP_CLUSTER_CODE,
             XW_FINAL_CIP_CLUSTER_NAME = FINAL_CIP_CLUSTER_NAME,
             XW_USE_BGS_CIP = USE_BGS_CIP,
             XW_STP_CIP_CODE_4 = STP_FINAL_CIP_CODE_4,
             XW_STP_CIP_CODE_4_NAME = STP_FINAL_CIP_CODE_4_NAME,
             XW_Final_Consider_A_Match = Final_Consider_A_Match,
             XW_Final_Probable_Match = Final_Probable_Match),
    by = "STQU_ID"
  ) %>%
  mutate(
    FINAL_CIP_CODE_4 = coalesce(FINAL_CIP_CODE_4, XW_FINAL_CIP_CODE_4),
    FINAL_CIP_CODE_4_NAME = coalesce(FINAL_CIP_CODE_4_NAME, XW_FINAL_CIP_CODE_4_NAME),
    FINAL_CIP_CODE_2 = coalesce(FINAL_CIP_CODE_2, XW_FINAL_CIP_CODE_2),
    FINAL_CIP_CODE_2_NAME = coalesce(FINAL_CIP_CODE_2_NAME, XW_FINAL_CIP_CODE_2_NAME),
    FINAL_CIP_CLUSTER_CODE = coalesce(FINAL_CIP_CLUSTER_CODE, XW_FINAL_CIP_CLUSTER_CODE),
    FINAL_CIP_CLUSTER_NAME = coalesce(FINAL_CIP_CLUSTER_NAME, XW_FINAL_CIP_CLUSTER_NAME),
    USE_BGS_CIP = coalesce(USE_BGS_CIP, XW_USE_BGS_CIP),
    STP_CIP_CODE_4 = coalesce(STP_CIP_CODE_4, XW_STP_CIP_CODE_4),
    STP_CIP_CODE_4_NAME = coalesce(STP_CIP_CODE_4_NAME, XW_STP_CIP_CODE_4_NAME),
    Final_Consider_A_Match = coalesce(Final_Consider_A_Match, XW_Final_Consider_A_Match),
    Final_Probable_Match = coalesce(Final_Probable_Match, XW_Final_Probable_Match)
  ) %>%
  select(-starts_with("XW_")) %>%
  # Step 2: Fill from manually matched XWALK (Final_Probable_Match)
  left_join(
    xwalk_step2 %>%
      select(STQU_ID,
             XW_FINAL_CIP_CODE_4 = FINAL_CIP_CODE_4,
             XW_FINAL_CIP_CODE_4_NAME = FINAL_CIP_CODE_4_NAME,
             XW_FINAL_CIP_CODE_2 = FINAL_CIP_CODE_2,
             XW_FINAL_CIP_CODE_2_NAME = FINAL_CIP_CODE_2_NAME,
             XW_FINAL_CIP_CLUSTER_CODE = FINAL_CIP_CLUSTER_CODE,
             XW_FINAL_CIP_CLUSTER_NAME = FINAL_CIP_CLUSTER_NAME,
             XW_USE_BGS_CIP = USE_BGS_CIP,
             XW_STP_CIP_CODE_4 = STP_FINAL_CIP_CODE_4,
             XW_STP_CIP_CODE_4_NAME = STP_FINAL_CIP_CODE_4_NAME,
             XW_Final_Probable_Match = Final_Probable_Match),
    by = "STQU_ID"
  ) %>%
  mutate(
    FINAL_CIP_CODE_4 = if_else(
      is.na(FINAL_CIP_CODE_4) & is.na(FINAL_CIP_CODE_4_NAME) & is.na(FINAL_CIP_CODE_2) &
        is.na(FINAL_CIP_CODE_2_NAME) & is.na(FINAL_CIP_CLUSTER_CODE) &
        is.na(FINAL_CIP_CLUSTER_NAME) & is.na(USE_BGS_CIP) & is.na(STP_CIP_CODE_4) &
        is.na(STP_CIP_CODE_4_NAME) & is.na(Final_Probable_Match) &
        !is.na(XW_FINAL_CIP_CODE_4),
      XW_FINAL_CIP_CODE_4, FINAL_CIP_CODE_4
    ),
    FINAL_CIP_CODE_4_NAME = coalesce(FINAL_CIP_CODE_4_NAME, XW_FINAL_CIP_CODE_4_NAME),
    FINAL_CIP_CODE_2 = coalesce(FINAL_CIP_CODE_2, XW_FINAL_CIP_CODE_2),
    FINAL_CIP_CODE_2_NAME = coalesce(FINAL_CIP_CODE_2_NAME, XW_FINAL_CIP_CODE_2_NAME),
    FINAL_CIP_CLUSTER_CODE = coalesce(FINAL_CIP_CLUSTER_CODE, XW_FINAL_CIP_CLUSTER_CODE),
    FINAL_CIP_CLUSTER_NAME = coalesce(FINAL_CIP_CLUSTER_NAME, XW_FINAL_CIP_CLUSTER_NAME),
    USE_BGS_CIP = coalesce(USE_BGS_CIP, XW_USE_BGS_CIP),
    STP_CIP_CODE_4 = coalesce(STP_CIP_CODE_4, XW_STP_CIP_CODE_4),
    STP_CIP_CODE_4_NAME = coalesce(STP_CIP_CODE_4_NAME, XW_STP_CIP_CODE_4_NAME),
    Final_Probable_Match = coalesce(Final_Probable_Match, XW_Final_Probable_Match)
  ) %>%
  select(-starts_with("XW_")) %>%
  # Convert USE_BGS_CIP to USE_STP_CIP (inverse logic)
  mutate(USE_STP_CIP = case_when(
    USE_BGS_CIP == "Yes" ~ "No",
    USE_BGS_CIP == "No" ~ "Yes",
    TRUE ~ NA_character_
  )) %>%
  select(-USE_BGS_CIP)

rm(xwalk_step1, xwalk_step2)


### Part 5B: Update Unmatched CIPs ----

# WHY: BGS records without a match in the XWALK use their own BGS CIP codes.
# This is the inverse of Part 4B where unmatched STP records used STP CIPs.
T_BGS_Data_Final_for_OutcomesMatching <- T_BGS_Data_Final_for_OutcomesMatching %>%
  mutate(
    FINAL_CIP_CODE_4 = if_else(
      is.na(FINAL_CIP_CODE_4) & is.na(FINAL_CIP_CODE_2) &
        is.na(Final_Consider_A_Match) & is.na(Final_Probable_Match),
      CIP_4DIGIT_NO_PERIOD, FINAL_CIP_CODE_4
    ),
    FINAL_CIP_CODE_4_NAME = if_else(
      is.na(FINAL_CIP_CODE_4_NAME) & is.na(FINAL_CIP_CODE_2) &
        is.na(Final_Consider_A_Match) & is.na(Final_Probable_Match),
      CIP4DIG_NAME, FINAL_CIP_CODE_4_NAME
    ),
    FINAL_CIP_CODE_2 = if_else(
      is.na(FINAL_CIP_CODE_2) & is.na(Final_Consider_A_Match) & is.na(Final_Probable_Match),
      CIP2DIG, FINAL_CIP_CODE_2
    ),
    FINAL_CIP_CODE_2_NAME = if_else(
      is.na(FINAL_CIP_CODE_2_NAME) & is.na(Final_Consider_A_Match) & is.na(Final_Probable_Match),
      CIP2DIG_NAME, FINAL_CIP_CODE_2_NAME
    ),
    USE_STP_CIP = if_else(
      is.na(FINAL_CIP_CODE_4) & is.na(FINAL_CIP_CODE_2) &
        is.na(Final_Consider_A_Match) & is.na(Final_Probable_Match),
      "No because no match", USE_STP_CIP
    )
  ) %>%
  # Fill cluster codes from INFOWARE
  left_join(
    cip2_ref %>% select(LCP2_CD, LCP2_LCIPPC_CD, LCP2_LCIPPC_NAME),
    by = c("FINAL_CIP_CODE_2" = "LCP2_CD")
  ) %>%
  mutate(
    FINAL_CIP_CLUSTER_CODE = coalesce(FINAL_CIP_CLUSTER_CODE, LCP2_LCIPPC_CD),
    FINAL_CIP_CLUSTER_NAME = coalesce(FINAL_CIP_CLUSTER_NAME, LCP2_LCIPPC_NAME)
  ) %>%
  select(-LCP2_LCIPPC_CD, -LCP2_LCIPPC_NAME)

# ---- Identify unmatched BGS programs that could use STP CIPs ----
T_BGS_Data_Matched_CIPS_using_STP <- T_BGS_Data_Final_for_OutcomesMatching %>%
  filter(USE_STP_CIP == "Yes") %>%
  count(INSTITUTION_CODE, CPC, PROGRAM,
        STP_CIP_CODE_4, STP_CIP_CODE_4_NAME,
        CIP_4DIGIT_NO_PERIOD, CIP4DIG_NAME, CIP2DIG, CIP2DIG_NAME,
        FINAL_CIP_CODE_4, FINAL_CIP_CODE_4_NAME, FINAL_CIP_CODE_2, FINAL_CIP_CODE_2_NAME,
        FINAL_CIP_CLUSTER_CODE, FINAL_CIP_CLUSTER_NAME,
        Final_Consider_A_Match, Final_Probable_Match, USE_STP_CIP,
        name = "EXPR1")

T_BGS_Data_Unmatched_CIPS <- T_BGS_Data_Final_for_OutcomesMatching %>%
  filter(USE_STP_CIP == "No because no match") %>%
  count(INSTITUTION_CODE, CPC, PROGRAM,
        STP_CIP_CODE_4, STP_CIP_CODE_4_NAME,
        CIP_4DIGIT_NO_PERIOD, CIP4DIG_NAME, CIP2DIG, CIP2DIG_NAME,
        FINAL_CIP_CODE_4, FINAL_CIP_CODE_4_NAME, FINAL_CIP_CODE_2, FINAL_CIP_CODE_2_NAME,
        FINAL_CIP_CLUSTER_CODE, FINAL_CIP_CLUSTER_NAME,
        Final_Consider_A_Match, Final_Probable_Match, USE_STP_CIP,
        name = "EXPR1")

# Find unmatched programs that have STP matches for different records
T_BGS_Data_Unmatched_CIPS_to_review <- T_BGS_Data_Unmatched_CIPS %>%
  select(-STP_CIP_CODE_4, -STP_CIP_CODE_4_NAME) %>%
  left_join(
    T_BGS_Data_Matched_CIPS_using_STP %>%
      distinct(INSTITUTION_CODE, CPC, PROGRAM, CIP_4DIGIT_NO_PERIOD,
               STP_CIP_CODE_4, STP_CIP_CODE_4_NAME),
    by = c("INSTITUTION_CODE", "CPC", "PROGRAM", "CIP_4DIGIT_NO_PERIOD")
  ) %>%
  mutate(
    Unmatched_But_in_STP_Program = if_else(!is.na(STP_CIP_CODE_4), "Yes", NA_character_),
    STP_CIP_is_Different = if_else(STP_CIP_CODE_4 != CIP_4DIGIT_NO_PERIOD, "Yes", NA_character_)
  ) %>%
  group_by(INSTITUTION_CODE, CPC, PROGRAM, CIP_4DIGIT_NO_PERIOD) %>%
  filter(Unmatched_But_in_STP_Program == "Yes" & STP_CIP_is_Different == "Yes") %>%
  ungroup() %>%
  select(-PROGRAM, everything(), PROGRAM) %>%
  arrange(FINAL_CIP_CODE_4)

# ---- Year-specific custom updates for BGS data ----
# !! UPDATE THIS TABLE FOR EACH MODEL RUN !!
T_BGS_Data_Unmatched_CIPS_to_update <- tibble::tribble(
  ~PROGRAM,                                                                   ~FINAL_CIP_CODE_4, ~FINAL_CIP_CODE_2,
  "Bachelor of Applied Science - Mechatronic Systems Engineering Major",      "1442",            "14",
  "Bachelor of Applied Science In Chemical Engineering",                       "1407",            "14",
  "Bachelor of Applied Science In Chemical Engineering Minor In Commerce",     "1407",            "14",
  "Bachelor of Applied Science In Chemical Engineering Option in Biology",     "1407",            "14",
  "Bachelor of Environment - Resource and Environmental Management Major",     "0301",            "03",
  "Bachelor of Environment - Resource and Environmental Management Major, First Nations Studies Minor", "0301", "03",
  "Bachelor of Environment - Resource and Environmental Management Major, Geography Minor", "0301", "03",
  "Bachelor of Science - Biomedical Physiology Major",                         "2609",            "26",
  "Bachelor of Science in Applied Psychology",                                 "4228",            "42"
)

# Apply custom updates
T_BGS_Data_Final_for_OutcomesMatching <- T_BGS_Data_Final_for_OutcomesMatching %>%
  left_join(
    T_BGS_Data_Unmatched_CIPS_to_update %>%
      rename(UPD_FINAL_CIP_CODE_4 = FINAL_CIP_CODE_4, UPD_FINAL_CIP_CODE_2 = FINAL_CIP_CODE_2),
    by = "PROGRAM"
  ) %>%
  mutate(
    FINAL_CIP_CODE_4 = if_else(
      !is.na(UPD_FINAL_CIP_CODE_4) & is.na(Final_Consider_A_Match) & is.na(Final_Probable_Match),
      UPD_FINAL_CIP_CODE_4, FINAL_CIP_CODE_4
    ),
    FINAL_CIP_CODE_4_NAME = if_else(
      !is.na(UPD_FINAL_CIP_CODE_4) & is.na(Final_Consider_A_Match) & is.na(Final_Probable_Match),
      NA_character_, FINAL_CIP_CODE_4_NAME
    ),
    FINAL_CIP_CODE_2 = if_else(
      !is.na(UPD_FINAL_CIP_CODE_2) & is.na(Final_Consider_A_Match) & is.na(Final_Probable_Match),
      UPD_FINAL_CIP_CODE_2, FINAL_CIP_CODE_2
    ),
    FINAL_CIP_CODE_2_NAME = if_else(
      !is.na(UPD_FINAL_CIP_CODE_2) & is.na(Final_Consider_A_Match) & is.na(Final_Probable_Match),
      NA_character_, FINAL_CIP_CODE_2_NAME
    ),
    FINAL_CIP_CLUSTER_CODE = if_else(
      !is.na(UPD_FINAL_CIP_CODE_2) & is.na(Final_Consider_A_Match) & is.na(Final_Probable_Match),
      NA_character_, FINAL_CIP_CLUSTER_CODE
    ),
    FINAL_CIP_CLUSTER_NAME = if_else(
      !is.na(UPD_FINAL_CIP_CODE_2) & is.na(Final_Consider_A_Match) & is.na(Final_Probable_Match),
      NA_character_, FINAL_CIP_CLUSTER_NAME
    )
  ) %>%
  select(-UPD_FINAL_CIP_CODE_4, -UPD_FINAL_CIP_CODE_2)

# Re-fill CIP names and cluster codes from INFOWARE
T_BGS_Data_Final_for_OutcomesMatching <- T_BGS_Data_Final_for_OutcomesMatching %>%
  left_join(
    cip4_ref %>% select(LCP4_CD, LCP4_CIP_4DIGITS_NAME),
    by = c("FINAL_CIP_CODE_4" = "LCP4_CD")
  ) %>%
  mutate(FINAL_CIP_CODE_4_NAME = coalesce(FINAL_CIP_CODE_4_NAME, LCP4_CIP_4DIGITS_NAME)) %>%
  select(-LCP4_CIP_4DIGITS_NAME) %>%
  left_join(
    cip2_ref %>% select(LCP2_CD, LCP2_DIGITS_NAME, LCP2_LCIPPC_CD, LCP2_LCIPPC_NAME),
    by = c("FINAL_CIP_CODE_2" = "LCP2_CD")
  ) %>%
  mutate(
    FINAL_CIP_CODE_2_NAME = coalesce(FINAL_CIP_CODE_2_NAME, LCP2_DIGITS_NAME),
    FINAL_CIP_CLUSTER_CODE = coalesce(FINAL_CIP_CLUSTER_CODE, LCP2_LCIPPC_CD),
    FINAL_CIP_CLUSTER_NAME = coalesce(FINAL_CIP_CLUSTER_NAME, LCP2_LCIPPC_NAME)
  ) %>%
  select(-LCP2_DIGITS_NAME, -LCP2_LCIPPC_CD, -LCP2_LCIPPC_NAME)

dbWriteTable(con, "T_BGS_Data_Final_for_OutcomesMatching",
             T_BGS_Data_Final_for_OutcomesMatching, overwrite = TRUE)

rm(T_BGS_Data_Matched_CIPS_using_STP, T_BGS_Data_Unmatched_CIPS,
   T_BGS_Data_Unmatched_CIPS_to_review, T_BGS_Data_Unmatched_CIPS_to_update)


# ---- Clean up ----

dbDisconnect(con)



# ==============================================================================
# FILE: 02a-dacso-program-matching_dplyr.R
# ==============================================================================


# DACSO Program Matching — dplyr Translation
# Original: R/02a-dacso-program-matching.R
#
# Pipeline context:
#   Aligns CIP codes between DACSO (outcomes survey) and STP (Student Transitions
#   Project) credential data. Builds and updates a crosswalk (XWALK) table that maps
#   program codes to standardized 4-digit CIP codes.
#
#   Steps:
#     1. Update XWALK with new DACSO programs (historical linkages + new programs)
#     2. Create STP credential table, auto-match to XWALK
#     3. Institution-specific custom matching (BCIT, CAPU, VIU)
#     4. Compute final CIP codes from INFOWARE taxonomy
#
# Input tables:
#   - INFOWARE_PROGRAMS — master program listing (Oracle/JDBC)
#   - INFOWARE_L_CIP_*DIGITS_CIP2016 — CIP taxonomy (Oracle/JDBC)
#   - INFOWARE_PROGRAMS_HIST_PRGMID_XREF — historical program linkages (Oracle/JDBC)
#   - DACSO_STP_ProgramsCIP4_XWALK_ALL_2020 — previous cycle XWALK (Access)
#   - Credential_Non_Dup — deduplicated credentials (from 02a-update-cred-non-dup)
#
# Output tables:
#   - DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 — updated XWALK
#   - Credential_Non_Dup_Programs_DACSO_FinalCIPS — final CIP assignments

library(tidyverse)
library(RODBC)
library(config)
library(glue)
library(odbc)
library(RJDBC)

# Helper: reference a table in the user's schema
my_schema <- config::get("myschema")

sch_tbl <- function(name) {
  tbl(con, dbplyr::in_schema(my_schema, name))
}


# ******************************************************************************
# SETUP: Read external tables and write to Decimal
# ******************************************************************************
# WHY: INFOWARE tables come from Oracle/JDBC, previous XWALK comes from an Access
# database. These need to be written to SQL Server for the pipeline to access them.

iw_config <- config::get("infoware")
jdbc_config <- config::get("jdbc")

jdbcDriver <- JDBC(jdbc_config$class, classPath = jdbc_config$path)

iw_con <- dbConnect(jdbcDriver,
                    iw_config$database,
                    iw_config$uid,
                    iw_config$pwd)

INFOWARE_PROGRAMS <- dbReadTable(iw_con, "INFOWARE.PROGRAMS")
INFOWARE_L_CIP_6DIGITS_CIP2016 <- dbReadTable(iw_con, "INFOWARE.L_CIP_6DIGITS_CIP2016")
INFOWARE_L_CIP_4DIGITS_CIP2016 <- dbReadTable(iw_con, "INFOWARE.L_CIP_4DIGITS_CIP2016")
INFOWARE_L_CIP_2DIGITS_CIP2016 <- dbReadTable(iw_con, "INFOWARE.L_CIP_2DIGITS_CIP2016")
INFOWARE_PROGRAMS_HIST_PRGMID_XREF <- dbReadTable(iw_con, "INFOWARE.PROGRAMS_HIST_PRGMID_XREF")

dbDisconnect(iw_con)

# Read previous cycle XWALK from Access database
connection <- config::get("connection")$outcomes_dacso
acc_con <- odbcDriverConnect(connection)

DACSO_STP_ProgramsCIP4_XWALK_ALL_2020 <- sqlQuery(acc_con, "SELECT * FROM DACSO_STP_ProgramsCIP4_XWALK_ALL_2020;")

odbcClose(acc_con)

# ---- Connect to Decimal ----
db_config <- config::get("decimal")
con <- dbConnect(odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")

# Write all source tables to Decimal
dbWriteTable(con, SQL(glue::glue('"{my_schema}"."DACSO_STP_ProgramsCIP4_XWALK_ALL_2020"')), DACSO_STP_ProgramsCIP4_XWALK_ALL_2020)
dbWriteTable(con, SQL(glue::glue('"{my_schema}"."INFOWARE_PROGRAMS"')), INFOWARE_PROGRAMS)
dbWriteTable(con, SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_6DIGITS_CIP2016"')), INFOWARE_L_CIP_6DIGITS_CIP2016)
dbWriteTable(con, SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_4DIGITS_CIP2016"')), INFOWARE_L_CIP_4DIGITS_CIP2016)
dbWriteTable(con, SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_2DIGITS_CIP2016"')), INFOWARE_L_CIP_2DIGITS_CIP2016)
dbWriteTable(con, SQL(glue::glue('"{my_schema}"."INFOWARE_PROGRAMS_HIST_PRGMID_XREF"')), INFOWARE_PROGRAMS_HIST_PRGMID_XREF)

rm(DACSO_STP_ProgramsCIP4_XWALK_ALL_2020, INFOWARE_PROGRAMS, INFOWARE_L_CIP_6DIGITS_CIP2016,
   INFOWARE_L_CIP_4DIGITS_CIP2016, INFOWARE_L_CIP_2DIGITS_CIP2016, INFOWARE_PROGRAMS_HIST_PRGMID_XREF)


# ******************************************************************************
# PART 1: UPDATE XWALK WITH NEW DACSO DATA
# ******************************************************************************
# WHY: New DACSO programs from the 2021-2023 survey cycles need to be added to
# the XWALK. Programs with historical linkages inherit their CIP codes from the
# previous program version. Programs without linkages get their CIP directly.
#
# This section is already R-native dplyr in the original — kept with minimal changes.

## ---- Create programs_table from combining INFOWARE tables ----
programs_table <- tbl(con, "INFOWARE_PROGRAMS") %>%
  inner_join(tbl(con, "INFOWARE_L_CIP_6DIGITS_CIP2016"), by = c("LCIP_CD_CIP2016" = "LCIP_CD")) %>%
  inner_join(tbl(con, "INFOWARE_L_CIP_4DIGITS_CIP2016"), by = c("LCIP_LCP4_CD" = "LCP4_CD")) %>%
  select(PRGM_ID, PRGM_FIRST_SEEN_SUBM_CD, PRGM_INST_CD, PRGM_INST_PROGRAM_NAME,
         PRGM_INST_PROGRAM_NAME_CLEANED,
         PRGM_LCPC_CD, PRGM_TTRAIN_FLAG, LCIP_CD_CIP2016, LCIP_NAME_CIP2016,
         PRGM_CREDENTIAL, NOTES, HAS_HISTORICAL_PRGM_ID_LINK,
         CIP_CLUSTER_ARTS_APPLIED, DACSO_OLD_PRGM_ID_DO_NOT_USE, DUP_PROGRAM_USE_THIS_PRGM_ID,
         LCIP_LCP4_CD, LCP4_CIP_4DIGITS_NAME) %>%
  collect()

## ---- Make new XWALK from last years XWALK ----
DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 <- tbl(con, "DACSO_STP_ProgramsCIP4_XWALK_ALL_2020") %>%
  collect() %>%
  mutate(CIP_CODE_4 = str_pad(CIP_CODE_4, width = 4, side = "left", pad = "0"))

# ---- Add programs WITHOUT historical linkages ----
programs_table %>%
  filter(PRGM_FIRST_SEEN_SUBM_CD %in% c('C_Outc21', 'C_Outc22', 'C_Outc23')) %>%
  group_by(PRGM_FIRST_SEEN_SUBM_CD, HAS_HISTORICAL_PRGM_ID_LINK) %>% tally()

new_dacso_programs_21_23 <- programs_table %>%
  filter(PRGM_FIRST_SEEN_SUBM_CD %in% c('C_Outc21', 'C_Outc22', 'C_Outc23') &
         (is.na(HAS_HISTORICAL_PRGM_ID_LINK) | HAS_HISTORICAL_PRGM_ID_LINK == " "))

DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 <- DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 %>%
  bind_rows(
    programs_table %>%
      filter(PRGM_FIRST_SEEN_SUBM_CD %in% c('C_Outc21', 'C_Outc22', 'C_Outc23') &
             (is.na(HAS_HISTORICAL_PRGM_ID_LINK) | HAS_HISTORICAL_PRGM_ID_LINK == " ")) %>%
      mutate(New_DACSO_Program2021_23 = case_when(
        PRGM_FIRST_SEEN_SUBM_CD == "C_Outc21" ~ "Yes2021",
        PRGM_FIRST_SEEN_SUBM_CD == "C_Outc22" ~ "Yes2022",
        PRGM_FIRST_SEEN_SUBM_CD == "C_Outc23" ~ "Yes2023"
      )) %>%
      select(COCI_INST_CD = PRGM_INST_CD, PRGM_LCPC_CD, PRGM_INST_PROGRAM_NAME,
             CIP_CODE_4 = LCIP_LCP4_CD, LCP4_CIP_4DIGITS_NAME, PRGM_ID,
             PRGM_CREDENTIAL, New_DACSO_Program2021_23)
  )

# ---- 2021: Programs WITH historical linkages ----
Updated_DACSO_Programs_in_2021_with_links <- programs_table %>%
  filter(PRGM_FIRST_SEEN_SUBM_CD == 'C_Outc21' & HAS_HISTORICAL_PRGM_ID_LINK == 'Y') %>%
  inner_join(
    tbl(con, "INFOWARE_PROGRAMS_HIST_PRGMID_XREF") %>%
      filter(YEAR_LINK_CREATED == 'C_Outc21' & SURVEY_CODE == 'DACSO') %>%
      collect(),
    by = "PRGM_ID"
  ) %>%
  select(PRGM_ID, PRGM_FIRST_SEEN_SUBM_CD, PRGM_INST_CD, PRGM_LCPC_CD,
         PRGM_INST_PROGRAM_NAME, PRGM_TTRAIN_FLAG, PRGM_CREDENTIAL,
         PRGM_INST_PROGRAM_NAME_CLEANED, NOTES, HAS_HISTORICAL_PRGM_ID_LINK,
         DUP_PROGRAM_USE_THIS_PRGM_ID, CIP_CLUSTER_ARTS_APPLIED,
         DACSO_OLD_PRGM_ID_DO_NOT_USE, LCIP_CD_CIP2016, LCIP_NAME_CIP2016,
         LCIP_LCP4_CD, LCP4_CIP_4DIGITS_NAME, HISTORICAL_PRGM_ID,
         YEAR_LINK_CREATED, SURVEY_CODE)

Updated_DACSO_Programs_in_2021_with_links <- Updated_DACSO_Programs_in_2021_with_links %>%
  inner_join(
    programs_table %>%
      select(PRGM_ID, HISTORICAL_CPC_CD = PRGM_LCPC_CD,
             HISTORICAL_PROGRAM_NAME = PRGM_INST_PROGRAM_NAME,
             HISTORICAL_CIP4_CD = LCIP_LCP4_CD),
    by = c(HISTORICAL_PRGM_ID = "PRGM_ID")
  ) %>%
  mutate(Updated_CPC_Flag = if_else(PRGM_LCPC_CD != HISTORICAL_CPC_CD, 'Yes', NA_character_),
         Updated_CIP_Flag = if_else(LCIP_LCP4_CD != HISTORICAL_CIP4_CD, 'Yes', NA_character_))

DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 <- DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 %>%
  left_join(
    Updated_DACSO_Programs_in_2021_with_links %>%
      mutate(HISTORICAL_CPC_CD = as.character(HISTORICAL_CPC_CD)) %>%
      select(PRGM_INST_CD, HISTORICAL_CPC_CD, HISTORICAL_PROGRAM_NAME, HISTORICAL_CIP4_CD,
             PRGM_LCPC_CD, PRGM_INST_PROGRAM_NAME, LCIP_LCP4_CD, LCP4_CIP_4DIGITS_NAME,
             Updated_DACSO_CPC2021 = Updated_CPC_Flag, Updated_DACSO_CIP2021 = Updated_CIP_Flag),
    by = c(COCI_INST_CD = "PRGM_INST_CD", PRGM_LCPC_CD = "HISTORICAL_CPC_CD",
           PRGM_INST_PROGRAM_NAME = "HISTORICAL_PROGRAM_NAME", CIP_CODE_4 = "HISTORICAL_CIP4_CD")
  ) %>%
  mutate(PRGM_LCPC_CD = ifelse(!is.na(PRGM_LCPC_CD.y), PRGM_LCPC_CD.y, PRGM_LCPC_CD),
         PRGM_INST_PROGRAM_NAME = ifelse(!is.na(PRGM_INST_PROGRAM_NAME.y), PRGM_INST_PROGRAM_NAME.y, PRGM_INST_PROGRAM_NAME),
         CIP_CODE_4 = ifelse(!is.na(LCIP_LCP4_CD), LCIP_LCP4_CD, CIP_CODE_4)) %>%
  mutate(LCP4_CIP_4DIGITS_NAME = ifelse(!is.na(LCP4_CIP_4DIGITS_NAME.y), LCP4_CIP_4DIGITS_NAME.y, LCP4_CIP_4DIGITS_NAME.x),
         .after = "CIP_CODE_4") %>%
  select(-ends_with(".x"), -ends_with(".y"), -LCIP_LCP4_CD)

# ***** manual work needed — review 2021 remaining programs and apply overrides *****
# See original R/02a-dacso-program-matching.R lines 202-222 for manual override logic.
# The analyst reviews specific PRGM_IDs and applies case_when overrides.

# ---- 2022: Programs WITH historical linkages ----
# Same pattern as 2021 — see original lines 225-327 for full logic.
# ***** manual work needed — review 2022 remaining programs *****

# ---- 2023: Programs WITH historical linkages ----
# Same pattern — see original lines 330-415 for full logic.
# ***** manual work needed — review 2023 remaining programs *****

# ---- Add STP matching columns to XWALK ----
DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 <- DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 %>%
  mutate(
    New_STP_Program2021_23 = NA_character_,
    Updated_DACSO_CDTL2021_23 = NA_character_
  )

dbWriteTable(con, "DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23",
             DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23, overwrite = TRUE)


# ******************************************************************************
# PART 2: UPDATE XWALK WITH NEW STP CREDENTIAL DATA
# ******************************************************************************
# WHY: STP credential programs need to be matched to the XWALK to get their CIP
# codes. First, programs already in the XWALK are matched; then new programs are
# auto-matched on program code and description.
#
# Original: ~12 SQL operations (SELECT INTO, ALTER TABLE, UPDATE...FROM JOIN)
# Translated: Pull tables into R, perform sequential left_join + mutate operations.

credential_non_dup <- sch_tbl("Credential_Non_Dup") %>%
  select(PSI_CODE, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
         PSI_CREDENTIAL_CIP, PSI_CREDENTIAL_LEVEL, PSI_CREDENTIAL_CATEGORY, OUTCOMES_CRED) %>%
  collect() |> rename_with(toupper)

xwalk <- DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23

# Pull INFOWARE CIP reference tables
cip6 <- sch_tbl("INFOWARE_L_CIP_6DIGITS_CIP2016") %>% collect() |> rename_with(toupper)
cip4_ref <- sch_tbl("INFOWARE_L_CIP_4DIGITS_CIP2016") %>% collect() |> rename_with(toupper)
cip2_ref <- sch_tbl("INFOWARE_L_CIP_2DIGITS_CIP2016") %>% collect() |> rename_with(toupper)

# ---- Build STP_Credential_Non_Dup_Programs_DACSO ----
# WHY: Create a DACSO-only subset of Credential_Non_Dup, grouped by program attributes.
# Original: SELECT INTO with GROUP BY HAVING
stp_dacso <- credential_non_dup %>%
  filter(OUTCOMES_CRED == "DACSO") %>%
  count(PSI_CODE, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
        PSI_CREDENTIAL_CIP, PSI_CREDENTIAL_LEVEL, PSI_CREDENTIAL_CATEGORY,
        OUTCOMES_CRED, name = "EXPR1") %>%
  # Add empty columns that will be filled by matching steps
  mutate(
    OUTCOMES_CIP_CODE_4 = NA_character_,
    OUTCOMES_CIP_CODE_4_NAME = NA_character_,
    FINAL_CIP_CODE_4 = NA_character_,
    FINAL_CIP_CODE_4_NAME = NA_character_,
    FINAL_CIP_CODE_2 = NA_character_,
    FINAL_CIP_CODE_2_NAME = NA_character_,
    FINAL_CIP_CLUSTER_CODE = NA_character_,
    FINAL_CIP_CLUSTER_NAME = NA_character_,
    STP_CIP_CODE_4 = NA_character_,
    STP_CIP_CODE_4_NAME = NA_character_,
    Already_Matched = NA_character_,
    New_Auto_Match = NA_character_,
    New_Manual_Match = NA_character_,
    COCI_INST_CD = NA_character_
  )

# ---- Add COCI_INST_CD from XWALK ----
# WHY: PSI_CODE and COCI_INST_CD are different code systems for institutions.
# The XWALK maps between them.
psi_to_coci <- xwalk %>%
  filter(!is.na(PSI_CODE) & !is.na(COCI_INST_CD)) %>%
  distinct(PSI_CODE, COCI_INST_CD)

stp_dacso <- stp_dacso %>%
  left_join(psi_to_coci %>% rename(COCI_INST_CD_MAP = COCI_INST_CD), by = "PSI_CODE") %>%
  mutate(COCI_INST_CD = coalesce(COCI_INST_CD_MAP, COCI_INST_CD)) %>%
  select(-COCI_INST_CD_MAP)


# ---- Populate STP_CIP_CODE_4 from INFOWARE ----
# WHY: Match PSI_CREDENTIAL_CIP to the 6-digit CIP taxonomy to get 4-digit codes.
# Original: UPDATE with 3-table JOIN (STP INNER JOIN CIP6 ON CIP, then CIP4 ON LCP4_CD)
cip6_lookup <- cip6 %>%
  select(LCIP_CD_WITH_PERIOD, LCIP_LCP4_CD) %>%
  inner_join(cip4_ref %>% select(LCP4_CD, LCP4_CIP_4DIGITS_NAME),
             by = c("LCIP_LCP4_CD" = "LCP4_CD"))

stp_dacso <- stp_dacso %>%
  left_join(
    cip6_lookup %>% select(LCIP_CD_WITH_PERIOD, LCIP_LCP4_CD, LCP4_CIP_4DIGITS_NAME),
    by = c("PSI_CREDENTIAL_CIP" = "LCIP_CD_WITH_PERIOD")
  ) %>%
  mutate(
    STP_CIP_CODE_4 = coalesce(STP_CIP_CODE_4, LCIP_LCP4_CD),
    STP_CIP_CODE_4_NAME = coalesce(STP_CIP_CODE_4_NAME, LCP4_CIP_4DIGITS_NAME)
  ) %>%
  select(-LCIP_LCP4_CD, -LCP4_CIP_4DIGITS_NAME)

# ---- Already matched programs ----
# WHY: Programs already in the XWALK (matched from previous cycles) inherit
# their CIP codes. Try matching on PSI_CODE first, then COCI_INST_CD.

# Match on PSI_CODE + PSI_PROGRAM_CODE + PSI_CREDENTIAL_PROGRAM_DESCRIPTION
xwalk_exact <- xwalk %>%
  filter(!is.na(PSI_CODE) & !is.na(PSI_PROGRAM_CODE) & !is.na(PSI_CREDENTIAL_PROGRAM_DESC)) %>%
  select(PSI_CODE, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESC, CIP_CODE_4, LCP4_CIP_4DIGITS_NAME)

stp_dacso <- stp_dacso %>%
  left_join(
    xwalk_exact %>% rename(XW_CIP4 = CIP_CODE_4, XW_CIP4_NAME = LCP4_CIP_4DIGITS_NAME),
    by = c("PSI_CODE", "PSI_PROGRAM_CODE" = "PSI_PROGRAM_CODE",
           "PSI_CREDENTIAL_PROGRAM_DESCRIPTION" = "PSI_CREDENTIAL_PROGRAM_DESC")
  ) %>%
  mutate(
    Already_Matched = if_else(!is.na(XW_CIP4) & is.na(Already_Matched), "Yes", Already_Matched),
    OUTCOMES_CIP_CODE_4 = coalesce(OUTCOMES_CIP_CODE_4, XW_CIP4),
    OUTCOMES_CIP_CODE_4_NAME = coalesce(OUTCOMES_CIP_CODE_4_NAME, XW_CIP4_NAME)
  ) %>%
  select(-XW_CIP4, -XW_CIP4_NAME)

# Match on COCI_INST_CD + PSI_PROGRAM_CODE + PSI_CREDENTIAL_PROGRAM_DESCRIPTION
xwalk_coci <- xwalk %>%
  filter(!is.na(COCI_INST_CD) & !is.na(PSI_PROGRAM_CODE) & !is.na(PSI_CREDENTIAL_PROGRAM_DESC)) %>%
  select(COCI_INST_CD, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESC, CIP_CODE_4, LCP4_CIP_4DIGITS_NAME)

stp_dacso <- stp_dacso %>%
  left_join(
    xwalk_coci %>% rename(XW_CIP4 = CIP_CODE_4, XW_CIP4_NAME = LCP4_CIP_4DIGITS_NAME),
    by = c("COCI_INST_CD", "PSI_PROGRAM_CODE" = "PSI_PROGRAM_CODE",
           "PSI_CREDENTIAL_PROGRAM_DESCRIPTION" = "PSI_CREDENTIAL_PROGRAM_DESC")
  ) %>%
  mutate(
    Already_Matched = if_else(is.na(Already_Matched) & is.na(OUTCOMES_CIP_CODE_4) &
                                !is.na(XW_CIP4), "Yes", Already_Matched),
    OUTCOMES_CIP_CODE_4 = if_else(is.na(OUTCOMES_CIP_CODE_4) & !is.na(XW_CIP4), XW_CIP4, OUTCOMES_CIP_CODE_4),
    OUTCOMES_CIP_CODE_4_NAME = if_else(is.na(OUTCOMES_CIP_CODE_4_NAME) & !is.na(XW_CIP4_NAME), XW_CIP4_NAME, OUTCOMES_CIP_CODE_4_NAME)
  ) %>%
  select(-XW_CIP4, -XW_CIP4_NAME)

# ---- Newly matched programs ----
# WHY: Programs not in the XWALK can be matched on DACSO program names/codes.
# The XWALK has PRGM_INST_PROGRAM_NAME and PRGM_LCPC_CD fields from DACSO.

# Match on PSI_CODE + PSI_PROGRAM_CODE=PRGM_LCPC_CD + DESC=PRGM_INST_PROGRAM_NAME
xwalk_new_a <- xwalk %>%
  filter(!is.na(PSI_CODE) & !is.na(PRGM_LCPC_CD) & !is.na(PRGM_INST_PROGRAM_NAME)) %>%
  select(PSI_CODE, PRGM_LCPC_CD, PRGM_INST_PROGRAM_NAME, CIP_CODE_4, LCP4_CIP_4DIGITS_NAME)

stp_dacso <- stp_dacso %>%
  left_join(
    xwalk_new_a %>% rename(XW_CIP4 = CIP_CODE_4, XW_CIP4_NAME = LCP4_CIP_4DIGITS_NAME),
    by = c("PSI_CODE", "PSI_PROGRAM_CODE" = "PRGM_LCPC_CD",
           "PSI_CREDENTIAL_PROGRAM_DESCRIPTION" = "PRGM_INST_PROGRAM_NAME")
  ) %>%
  mutate(
    New_Auto_Match = if_else(is.na(Already_Matched) & !is.na(XW_CIP4), "Yes", New_Auto_Match),
    OUTCOMES_CIP_CODE_4 = if_else(is.na(Already_Matched) & !is.na(XW_CIP4), XW_CIP4, OUTCOMES_CIP_CODE_4),
    OUTCOMES_CIP_CODE_4_NAME = if_else(is.na(Already_Matched) & !is.na(XW_CIP4_NAME), XW_CIP4_NAME, OUTCOMES_CIP_CODE_4_NAME)
  ) %>%
  select(-XW_CIP4, -XW_CIP4_NAME)

# Match on COCI_INST_CD + PSI_PROGRAM_CODE=PRGM_LCPC_CD + DESC=PRGM_INST_PROGRAM_NAME
xwalk_new_a2 <- xwalk %>%
  filter(!is.na(COCI_INST_CD) & !is.na(PRGM_LCPC_CD) & !is.na(PRGM_INST_PROGRAM_NAME)) %>%
  select(COCI_INST_CD, PRGM_LCPC_CD, PRGM_INST_PROGRAM_NAME, CIP_CODE_4, LCP4_CIP_4DIGITS_NAME)

stp_dacso <- stp_dacso %>%
  left_join(
    xwalk_new_a2 %>% rename(XW_CIP4 = CIP_CODE_4, XW_CIP4_NAME = LCP4_CIP_4DIGITS_NAME),
    by = c("COCI_INST_CD", "PSI_PROGRAM_CODE" = "PRGM_LCPC_CD",
           "PSI_CREDENTIAL_PROGRAM_DESCRIPTION" = "PRGM_INST_PROGRAM_NAME")
  ) %>%
  mutate(
    New_Auto_Match = if_else(is.na(OUTCOMES_CIP_CODE_4) & is.na(OUTCOMES_CIP_CODE_4_NAME) &
                               is.na(New_Auto_Match) & is.na(Already_Matched) & !is.na(XW_CIP4),
                             "Yes", New_Auto_Match),
    OUTCOMES_CIP_CODE_4 = if_else(is.na(OUTCOMES_CIP_CODE_4) & !is.na(XW_CIP4), XW_CIP4, OUTCOMES_CIP_CODE_4),
    OUTCOMES_CIP_CODE_4_NAME = if_else(is.na(OUTCOMES_CIP_CODE_4_NAME) & !is.na(XW_CIP4_NAME), XW_CIP4_NAME, OUTCOMES_CIP_CODE_4_NAME)
  ) %>%
  select(-XW_CIP4, -XW_CIP4_NAME)

# ---- Update XWALK with newly matched STP programs ----
# WHY: When new STP programs are matched to DACSO entries in the XWALK, we update
# the XWALK with the STP program info so future runs can find them as "already matched".

# Get the newly matched STP programs
newly_matched <- stp_dacso %>%
  filter(New_Auto_Match == "Yes") %>%
  select(PSI_CODE, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
         STP_CIP_CODE_4, STP_CIP_CODE_4_NAME)

# Update XWALK for PSI_CODE matches
DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 <- DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 %>%
  left_join(
    newly_matched %>%
      rename(XW_STP_PGM_CODE = PSI_PROGRAM_CODE, XW_STP_PGM_DESC = PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
             XW_STP_CIP4 = STP_CIP_CODE_4, XW_STP_CIP4_NAME = STP_CIP_CODE_4_NAME),
    by = c("PSI_CODE", "PRGM_LCPC_CD" = "PSI_PROGRAM_CODE",
           "PRGM_INST_PROGRAM_NAME" = "PSI_CREDENTIAL_PROGRAM_DESCRIPTION")
  ) %>%
  mutate(
    PSI_PROGRAM_CODE = if_else(!is.na(XW_STP_PGM_CODE), XW_STP_PGM_CODE, PSI_PROGRAM_CODE),
    PSI_CREDENTIAL_PROGRAM_DESC = if_else(!is.na(XW_STP_PGM_DESC), XW_STP_PGM_DESC, PSI_CREDENTIAL_PROGRAM_DESC),
    STP_CIP4_CODE = if_else(!is.na(XW_STP_CIP4), XW_STP_CIP4, STP_CIP4_CODE),
    STP_CIP4_NAME = if_else(!is.na(XW_STP_CIP4_NAME), XW_STP_CIP4_NAME, STP_CIP4_NAME),
    New_STP_Program2021_23 = if_else(!is.na(XW_STP_PGM_CODE), "Yes", New_STP_Program2021_23),
    One_To_One_Match = if_else(!is.na(XW_STP_PGM_CODE), "Yes2021_23", One_To_One_Match)
  ) %>%
  select(-starts_with("XW_STP"))

# Update XWALK for COCI_INST_CD matches (only rows not yet updated)
DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 <- DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 %>%
  left_join(
    newly_matched %>%
      rename(XW_STP_PGM_CODE = PSI_PROGRAM_CODE, XW_STP_PGM_DESC = PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
             XW_STP_CIP4 = STP_CIP_CODE_4, XW_STP_CIP4_NAME = STP_CIP_CODE_4_NAME),
    by = c("COCI_INST_CD" = "PSI_CODE", "PRGM_LCPC_CD" = "PSI_PROGRAM_CODE",
           "PRGM_INST_PROGRAM_NAME" = "PSI_CREDENTIAL_PROGRAM_DESCRIPTION")
  ) %>%
  mutate(
    PSI_PROGRAM_CODE = if_else(is.na(New_STP_Program2021_23) & !is.na(XW_STP_PGM_CODE),
                                XW_STP_PGM_CODE, PSI_PROGRAM_CODE),
    PSI_CREDENTIAL_PROGRAM_DESC = if_else(is.na(New_STP_Program2021_23) & !is.na(XW_STP_PGM_DESC),
                                           XW_STP_PGM_DESC, PSI_CREDENTIAL_PROGRAM_DESC),
    STP_CIP4_CODE = if_else(is.na(New_STP_Program2021_23) & !is.na(XW_STP_CIP4),
                             XW_STP_CIP4, STP_CIP4_CODE),
    STP_CIP4_NAME = if_else(is.na(New_STP_Program2021_23) & !is.na(XW_STP_CIP4_NAME),
                             XW_STP_CIP4_NAME, STP_CIP4_NAME),
    New_STP_Program2021_23 = if_else(is.na(New_STP_Program2021_23) & !is.na(XW_STP_PGM_CODE),
                                      "Yes", New_STP_Program2021_23),
    One_To_One_Match = if_else(is.na(One_To_One_Match) & !is.na(XW_STP_PGM_CODE),
                                "Yes2021_23", One_To_One_Match)
  ) %>%
  select(-starts_with("XW_STP"))

rm(newly_matched)


# ******************************************************************************
# PART 3: INSTITUTION-SPECIFIC CUSTOM MATCHING
# ******************************************************************************
# WHY: Some institutions use different program code formats in STP vs DACSO.
# BCIT includes credential suffixes, CAPU uses different code lengths, and VIU
# wraps codes in credential-category prefixes. We extract the DACSO-compatible
# portion and match on that.

# Find unmatched STP programs
stp_unmatched <- stp_dacso %>%
  filter((is.na(OUTCOMES_CIP_CODE_4) | is.na(OUTCOMES_CIP_CODE_4_NAME)) &
         is.na(Already_Matched) & is.na(New_Auto_Match))

# ---- BCIT matching ----
# WHY: BCIT submits CPC codes with credential abbreviation suffixes (e.g., _TTDIPL)
# but DACSO codes don't have the suffix. Take first 4 characters of PSI_PROGRAM_CODE.
stp_dacso <- stp_dacso %>%
  mutate(BCIT_TEST_PROGRAM_CODE = if_else(PSI_CODE == "BCIT",
                                            substr(PSI_PROGRAM_CODE, 1, 4), NA_character_))

# Helper function: match STP to XWALK on test code
match_on_test_code <- function(stp_df, xwalk_df, test_col, match_flag,
                               join_cols_desc = TRUE) {
  join_by <- if (join_cols_desc) {
    by <- c("COCI_INST_CD", test_col = "PRGM_LCPC_CD",
            "PSI_CREDENTIAL_PROGRAM_DESCRIPTION" = "PRGM_INST_PROGRAM_NAME")
  } else {
    by <- c("COCI_INST_CD", test_col = "PRGM_LCPC_CD")
  }

  stp_df %>%
    left_join(
      xwalk_df %>%
        filter(!is.na(COCI_INST_CD) & !is.na(PRGM_LCPC_CD)) %>%
        select(COCI_INST_CD, PRGM_LCPC_CD, PRGM_INST_PROGRAM_NAME, CIP_CODE_4, LCP4_CIP_4DIGITS_NAME),
      by = by
    ) %>%
    mutate(
      New_Auto_Match = if_else(
        is.na(OUTCOMES_CIP_CODE_4) & is.na(OUTCOMES_CIP_CODE_4_NAME) & is.na(New_Auto_Match) &
          !is.na(CIP_CODE_4),
        match_flag, New_Auto_Match
      ),
      OUTCOMES_CIP_CODE_4 = if_else(
        is.na(OUTCOMES_CIP_CODE_4) & !is.na(CIP_CODE_4), CIP_CODE_4, OUTCOMES_CIP_CODE_4
      ),
      OUTCOMES_CIP_CODE_4_NAME = if_else(
        is.na(OUTCOMES_CIP_CODE_4_NAME) & !is.na(LCP4_CIP_4DIGITS_NAME),
        LCP4_CIP_4DIGITS_NAME, OUTCOMES_CIP_CODE_4_NAME
      )
    ) %>%
    select(-CIP_CODE_4, -LCP4_CIP_4DIGITS_NAME, -PRGM_INST_PROGRAM_NAME)
}

# BCIT: match with description
stp_dacso <- match_on_test_code(stp_dacso, xwalk, "BCIT_TEST_PROGRAM_CODE",
                                 "Yes2021_23BCIT", join_cols_desc = TRUE)
# BCIT: match without description (code only)
stp_dacso <- match_on_test_code(stp_dacso, xwalk, "BCIT_TEST_PROGRAM_CODE",
                                 "Yes2021_23BCIT", join_cols_desc = FALSE)

# ---- CAPU matching ----
# WHY: CAPU codes are 6 digits in STP but 3-4 digits in DACSO. Some also have
# dash suffixes. Try multiple code lengths.
stp_dacso <- stp_dacso %>%
  mutate(CAP_TEST_PROGRAM_CODE = if_else(
    COCI_INST_CD == "CAPU" & grepl("-", PSI_PROGRAM_CODE),
    substr(PSI_PROGRAM_CODE, 1, regexpr("-", PSI_PROGRAM_CODE, fixed = TRUE) - 1),
    NA_character_
  ))

# Match with dash-removed codes + description
stp_dacso <- match_on_test_code(stp_dacso, xwalk, "CAP_TEST_PROGRAM_CODE",
                                 "Yes2021_23CAPU", join_cols_desc = TRUE)
stp_dacso <- match_on_test_code(stp_dacso, xwalk, "CAP_TEST_PROGRAM_CODE",
                                 "Yes2021_23CAPU", join_cols_desc = FALSE)

# Try 4-digit prefix
stp_dacso <- stp_dacso %>%
  mutate(CAP_TEST_PROGRAM_CODE = if_else(COCI_INST_CD == "CAPU",
                                           substr(PSI_PROGRAM_CODE, 1, 4), CAP_TEST_PROGRAM_CODE))

stp_dacso <- match_on_test_code(stp_dacso, xwalk, "CAP_TEST_PROGRAM_CODE",
                                 "Yes2021_23CAPU", join_cols_desc = TRUE)
stp_dacso <- match_on_test_code(stp_dacso, xwalk, "CAP_TEST_PROGRAM_CODE",
                                 "Yes2021_23CAPU", join_cols_desc = FALSE)

# Try 3-digit prefix
stp_dacso <- stp_dacso %>%
  mutate(CAP_TEST_PROGRAM_CODE = if_else(COCI_INST_CD == "CAPU",
                                           substr(PSI_PROGRAM_CODE, 1, 3), CAP_TEST_PROGRAM_CODE))

stp_dacso <- match_on_test_code(stp_dacso, xwalk, "CAP_TEST_PROGRAM_CODE",
                                 "Yes2021_23CAPU", join_cols_desc = TRUE)
stp_dacso <- match_on_test_code(stp_dacso, xwalk, "CAP_TEST_PROGRAM_CODE",
                                 "Yes2021_23CAPU", join_cols_desc = FALSE)

# ---- VIU matching ----
# WHY: VIU codes in STP are like "CERT-WELDM_01" but DACSO just has "WELDM".
# Extract the substring between "-" and "_".
stp_dacso <- stp_dacso %>%
  mutate(VIU_TEST_PROGRAM_CODE = if_else(
    PSI_CODE == "VIU" & grepl("-", PSI_PROGRAM_CODE) & grepl("_", PSI_PROGRAM_CODE),
    substr(PSI_PROGRAM_CODE,
           regexpr("-", PSI_PROGRAM_CODE, fixed = TRUE) + 1,
           regexpr("_", PSI_PROGRAM_CODE, fixed = TRUE) - 1),
    NA_character_
  ))

stp_dacso <- match_on_test_code(stp_dacso, xwalk, "VIU_TEST_PROGRAM_CODE",
                                 "Yes2021_23VIU", join_cols_desc = TRUE)
stp_dacso <- match_on_test_code(stp_dacso, xwalk, "VIU_TEST_PROGRAM_CODE",
                                 "Yes2021_23VIU", join_cols_desc = FALSE)

# ---- Remaining catch-all matching ----
# WHY: Try matching remaining unmatched programs on COCI_INST_CD + program code,
# then on COCI_INST_CD + program description.

# Match on COCI_INST_CD + PSI_PROGRAM_CODE=PRGM_LCPC_CD
xwalk_remaining <- xwalk %>%
  filter(!is.na(COCI_INST_CD) & !is.na(PRGM_LCPC_CD)) %>%
  select(COCI_INST_CD, PRGM_LCPC_CD, CIP_CODE_4, LCP4_CIP_4DIGITS_NAME)

stp_dacso <- stp_dacso %>%
  left_join(
    xwalk_remaining %>% rename(XW_CIP4 = CIP_CODE_4, XW_CIP4_NAME = LCP4_CIP_4DIGITS_NAME),
    by = c("COCI_INST_CD", "PSI_PROGRAM_CODE" = "PRGM_LCPC_CD")
  ) %>%
  mutate(
    New_Auto_Match = if_else(
      is.na(OUTCOMES_CIP_CODE_4) & is.na(OUTCOMES_CIP_CODE_4_NAME) & is.na(New_Auto_Match) &
        !is.na(XW_CIP4), "Yes_2021_23test", New_Auto_Match
    ),
    OUTCOMES_CIP_CODE_4 = if_else(is.na(OUTCOMES_CIP_CODE_4) & !is.na(XW_CIP4), XW_CIP4, OUTCOMES_CIP_CODE_4),
    OUTCOMES_CIP_CODE_4_NAME = if_else(is.na(OUTCOMES_CIP_CODE_4_NAME) & !is.na(XW_CIP4_NAME), XW_CIP4_NAME, OUTCOMES_CIP_CODE_4_NAME)
  ) %>%
  select(-XW_CIP4, -XW_CIP4_NAME)

# Match on COCI_INST_CD + PSI_CREDENTIAL_PROGRAM_DESCRIPTION=PRGM_INST_PROGRAM_NAME
xwalk_remaining_desc <- xwalk %>%
  filter(!is.na(COCI_INST_CD) & !is.na(PRGM_INST_PROGRAM_NAME)) %>%
  select(COCI_INST_CD, PRGM_INST_PROGRAM_NAME, CIP_CODE_4, LCP4_CIP_4DIGITS_NAME)

stp_dacso <- stp_dacso %>%
  left_join(
    xwalk_remaining_desc %>% rename(XW_CIP4 = CIP_CODE_4, XW_CIP4_NAME = LCP4_CIP_4DIGITS_NAME),
    by = c("COCI_INST_CD", "PSI_CREDENTIAL_PROGRAM_DESCRIPTION" = "PRGM_INST_PROGRAM_NAME")
  ) %>%
  mutate(
    New_Auto_Match = if_else(
      is.na(OUTCOMES_CIP_CODE_4) & is.na(OUTCOMES_CIP_CODE_4_NAME) & is.na(New_Auto_Match) &
        !is.na(XW_CIP4), "Yes_2021_23test", New_Auto_Match
    ),
    OUTCOMES_CIP_CODE_4 = if_else(is.na(OUTCOMES_CIP_CODE_4) & !is.na(XW_CIP4), XW_CIP4, OUTCOMES_CIP_CODE_4),
    OUTCOMES_CIP_CODE_4_NAME = if_else(is.na(OUTCOMES_CIP_CODE_4_NAME) & !is.na(XW_CIP4_NAME), XW_CIP4_NAME, OUTCOMES_CIP_CODE_4_NAME)
  ) %>%
  select(-XW_CIP4, -XW_CIP4_NAME)

rm(xwalk_remaining, xwalk_remaining_desc)


# ******************************************************************************
# PART 4: FINAL UPDATE TO STP CIPS
# ******************************************************************************
# WHY: Compute final CIP codes. Matched programs use the outcomes CIP; unmatched
# programs use STP CIP from the INFOWARE taxonomy. Then fill 2-digit CIP and
# cluster codes from the 4-digit code.
#
# Original: ~8 SQL operations (UPDATE, SELECT INTO, DROP TABLE)
# Translated: Sequential mutate + left_join operations in memory.

# Step 1: Where outcomes CIP exists, use it as final
stp_dacso <- stp_dacso %>%
  mutate(
    FINAL_CIP_CODE_4 = if_else(!is.na(OUTCOMES_CIP_CODE_4) & !is.na(OUTCOMES_CIP_CODE_4_NAME),
                                OUTCOMES_CIP_CODE_4, FINAL_CIP_CODE_4),
    FINAL_CIP_CODE_4_NAME = if_else(!is.na(OUTCOMES_CIP_CODE_4) & !is.na(OUTCOMES_CIP_CODE_4_NAME),
                                     OUTCOMES_CIP_CODE_4_NAME, FINAL_CIP_CODE_4_NAME)
  )

# Step 2: Where no outcomes match, use INFOWARE to derive CIP from PSI_CREDENTIAL_CIP
# WHY: This fills FINAL_CIP for programs that weren't matched to DACSO outcomes.
# It uses the full CIP hierarchy (6-digit → 4-digit → 2-digit → names → cluster).
cip6_full <- cip6 %>%
  select(LCIP_CD_WITH_PERIOD, LCIP_LCP4_CD, LCIP_LCP2_CD, LCIP_LCIPPC_CD, LCIP_LCIPPC_NAME) %>%
  inner_join(cip4_ref %>% select(LCP4_CD, LCP4_CIP_4DIGITS_NAME), by = c("LCIP_LCP4_CD" = "LCP4_CD")) %>%
  inner_join(cip2_ref %>% select(LCP2_CD, LCP2_DIGITS_NAME), by = c("LCIP_LCP2_CD" = "LCP2_CD"))

stp_dacso <- stp_dacso %>%
  left_join(
    cip6_full %>% select(LCIP_CD_WITH_PERIOD, LCIP_LCP4_CD, LCP4_CIP_4DIGITS_NAME,
                          LCIP_LCP2_CD, LCP2_DIGITS_NAME),
    by = c("PSI_CREDENTIAL_CIP" = "LCIP_CD_WITH_PERIOD")
  ) %>%
  mutate(
    FINAL_CIP_CODE_4 = if_else(
      is.na(FINAL_CIP_CODE_4) & is.na(FINAL_CIP_CODE_4_NAME) & is.na(FINAL_CIP_CODE_2) &
        is.na(FINAL_CIP_CODE_2_NAME) & is.na(OUTCOMES_CIP_CODE_4),
      LCIP_LCP4_CD, FINAL_CIP_CODE_4
    ),
    FINAL_CIP_CODE_4_NAME = if_else(
      is.na(FINAL_CIP_CODE_4_NAME) & is.na(FINAL_CIP_CODE_2) & is.na(FINAL_CIP_CODE_2_NAME) &
        is.na(OUTCOMES_CIP_CODE_4),
      LCP4_CIP_4DIGITS_NAME, FINAL_CIP_CODE_4_NAME
    ),
    FINAL_CIP_CODE_2 = if_else(
      is.na(FINAL_CIP_CODE_2) & is.na(FINAL_CIP_CODE_2_NAME) & is.na(OUTCOMES_CIP_CODE_4),
      LCIP_LCP2_CD, FINAL_CIP_CODE_2
    ),
    FINAL_CIP_CODE_2_NAME = if_else(
      is.na(FINAL_CIP_CODE_2_NAME) & is.na(OUTCOMES_CIP_CODE_4),
      LCP2_DIGITS_NAME, FINAL_CIP_CODE_2_NAME
    )
  ) %>%
  select(-LCIP_LCP4_CD, -LCP4_CIP_4DIGITS_NAME, -LCIP_LCP2_CD, -LCP2_DIGITS_NAME)

# Step 3: Fill remaining NULL FINAL_CIP columns from the 4-digit code
# WHY: Some records got FINAL_CIP_CODE_4 from Step 1 (outcomes) but still need
# 4-digit name, 2-digit code, 2-digit name, and cluster codes.
cip4_lookup <- cip6 %>%
  select(LCIP_LCP4_CD, LCIP_LCP2_CD) %>%
  distinct() %>%
  inner_join(cip4_ref %>% select(LCP4_CD, LCP4_CIP_4DIGITS_NAME), by = c("LCIP_LCP4_CD" = "LCP4_CD")) %>%
  inner_join(cip2_ref %>% select(LCP2_CD, LCP2_DIGITS_NAME), by = c("LCIP_LCP2_CD" = "LCP2_CD"))

stp_dacso <- stp_dacso %>%
  left_join(
    cip4_lookup, by = c("FINAL_CIP_CODE_4" = "LCIP_LCP4_CD")
  ) %>%
  mutate(
    FINAL_CIP_CODE_4_NAME = coalesce(FINAL_CIP_CODE_4_NAME, LCP4_CIP_4DIGITS_NAME),
    FINAL_CIP_CODE_2 = coalesce(FINAL_CIP_CODE_2, LCIP_LCP2_CD),
    FINAL_CIP_CODE_2_NAME = coalesce(FINAL_CIP_CODE_2_NAME, LCP2_DIGITS_NAME)
  ) %>%
  select(-LCIP_LCP2_CD, -LCP4_CIP_4DIGITS_NAME, -LCP2_DIGITS_NAME)

# Step 4: Fill FINAL_CIP_CLUSTER from FINAL_CIP_CODE_4
# WHY: The 6-digit CIP taxonomy maps each 4-digit code to a cluster code.
cip6_cluster <- cip6 %>%
  select(LCIP_LCP4_CD, LCIP_LCIPPC_CD, LCIP_LCIPPC_NAME) %>%
  distinct()

stp_dacso <- stp_dacso %>%
  left_join(cip6_cluster, by = c("FINAL_CIP_CODE_4" = "LCIP_LCP4_CD")) %>%
  mutate(
    FINAL_CIP_CLUSTER_CODE = coalesce(FINAL_CIP_CLUSTER_CODE, LCIP_LCIPPC_CD),
    FINAL_CIP_CLUSTER_NAME = coalesce(FINAL_CIP_CLUSTER_NAME, LCIP_LCIPPC_NAME)
  ) %>%
  select(-LCIP_LCIPPC_CD, -LCIP_LCIPPC_NAME)

# Step 5: Fill FINAL_CIP_CODE_2_NAME from 2-digit lookup
stp_dacso <- stp_dacso %>%
  left_join(
    cip2_ref %>% select(LCP2_CD, LCP2_DIGITS_NAME),
    by = c("FINAL_CIP_CODE_2" = "LCP2_CD")
  ) %>%
  mutate(FINAL_CIP_CODE_2_NAME = coalesce(FINAL_CIP_CODE_2_NAME, LCP2_DIGITS_NAME)) %>%
  select(-LCP2_DIGITS_NAME)

# ---- Review CIP changes ----
# WHY: Diagnostic — shows programs where the final CIP differs from the original
# STP CIP. Useful for catching incorrect matches.
review_changed_cips <- stp_dacso %>%
  filter(FINAL_CIP_CODE_4 != STP_CIP_CODE_4)

# ---- Write final output tables ----
dbWriteTable(con, "STP_Credential_Non_Dup_Programs_DACSO", stp_dacso, overwrite = TRUE)
dbWriteTable(con, "Credential_Non_Dup_Programs_DACSO_FinalCIPS", stp_dacso, overwrite = TRUE)
dbWriteTable(con, "DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23",
             DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23, overwrite = TRUE)

dbDisconnect(con)



# ==============================================================================
# FILE: 02a-update-cred-non-dup_dplyr.R
# ==============================================================================


# Update Credential Non Dup — dplyr Translation
# Original: R/02a-update-cred-non-dup.R
#
# Pipeline context:
#   This script is the merge point for all four CIP code matching sources. After the
#   program matching scripts (02a-dacso, 02a-bgs, 02a-appso, and GRAD matching) have
#   each produced a table of matched CIP codes, this script merges them into the main
#   Credential_Non_Dup table. It then handles remaining unmatched records by falling
#   back to institution-reported (STP) CIP codes.
#
#   The priority order for CIP sources is:
#     1. DACSO (richest matching — joins on 7 columns)
#     2. BGS (matched by ID from 02a-bgs)
#     3. GRAD (matched by ID from GRAD matching)
#     4. APPSO (matched by ID from 02a-appso)
#     5. STP fallback (institution-reported, cleaned via INFOWARE lookup)
#
#   The output Credential_Non_Dup table with final CIP codes feeds into:
#     - 02b-1-pssm-cohorts (cohort creation)
#     - 03-near-completers-ttrain
#     - 05-ptib-analysis
#     - 06-program-projections
#
# Input tables:
#   - credential_non_dup — main credential table (from 01b)
#   - Credential_Non_Dup_Programs_DACSO_FinalCIPs — from 02a-dacso
#   - Credential_Non_Dup_BGS_IDs — from 02a-bgs
#   - Credential_Non_Dup_GRAD_IDs — from GRAD matching
#   - Credential_Non_Dup_APPSO_IDs — from 02a-appso
#   - INFOWARE_L_CIP_* — CIP taxonomy lookups (for fallback matching)

library(arrow)
library(tidyverse)
library(dbplyr)
library(odbc)
library(DBI)

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

# ---- Check Required Tables ----
# main table
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."credential_non_dup"')))

# tables with CIP updates (produced by the 4 program matching scripts)
dbExistsTable(
  con,
  SQL(glue::glue('"{my_schema}"."Credential_Non_Dup_Programs_DACSO_FinalCIPs"'))
)
dbExistsTable(
  con,
  SQL(glue::glue('"{my_schema}"."Credential_Non_Dup_BGS_IDs"'))
)
dbExistsTable(
  con,
  SQL(glue::glue('"{my_schema}"."Credential_Non_Dup_GRAD_IDs"'))
)
dbExistsTable(
  con,
  SQL(glue::glue('"{my_schema}"."Credential_Non_Dup_APPSO_IDs"'))
)

# reference tables
dbExistsTable(
  con,
  SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_2DIGITS_CIP2016"'))
)

# ==============================================================================
# START QUERIES
# ==============================================================================

# The credential table doesn't yet have columns for the final matched CIP codes.
# This step adds them. They will be populated by the four CIP sources (Steps 2–5)
# and the STP fallback (Steps 7–13).
# KEPT AS SQL: ALTER TABLE is DDL — no dplyr equivalent
# ---- Step 1: Add CIP columns to Credential_Non_Dup ----
dbExecute(
  con,
  "ALTER TABLE Credential_Non_Dup
ADD         OUTCOMES_CIP_CODE_4 varchar(4),
            OUTCOMES_CIP_CODE_4_NAME varchar(255),
            FINAL_CIP_CODE_4 varchar(4),
            FINAL_CIP_CODE_4_NAME varchar(255),
            FINAL_CIP_CODE_2 varchar(2),
            FINAL_CIP_CODE_2_NAME varchar(255),
            FINAL_CIP_CLUSTER_CODE varchar(10),
            FINAL_CIP_CLUSTER_NAME varchar(255),
            STP_CIP_CODE_4 varchar(4),
            STP_CIP_CODE_4_NAME varchar(255),
            STP_CIP_CODE_2 varchar(2),
            STP_CIP_CODE_2_NAME varchar(255);"
)

# DACSO provides the richest matching — it joins on 7 columns (institution,
# program code, description, CIP, credential level, category, and outcome type).
# It is applied first so its matches take priority over the simpler ID-based matches
# from BGS, GRAD, and APPSO in Steps 3–5. The resulting CIP columns will be
# populated in cred_non_dup for DACSO-matched records and remain NA for others.
# ---- Step 2: Update CIP codes from DACSO (primary source) ----

cred_non_dup <- sch_tbl("credential_non_dup") %>% collect()
cred_non_dup <- cred_non_dup |> rename_with(toupper)

dacso_cips <- sch_tbl("Credential_Non_Dup_Programs_DACSO_FinalCIPs") %>%
  collect()
dacso_cips <- dacso_cips |> rename_with(toupper)

# Drop the empty CIP columns (just added by ALTER TABLE) before joining so the
# CIP columns only come from dacso_cips — no .x/.y suffixes needed.
dacso_join <- dacso_cips %>%
  select(
    PSI_CODE,
    PSI_PROGRAM_CODE,
    PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    PSI_CREDENTIAL_CIP,
    PSI_CREDENTIAL_LEVEL,
    PSI_CREDENTIAL_CATEGORY,
    OUTCOMES_CRED,
    OUTCOMES_CIP_CODE_4,
    OUTCOMES_CIP_CODE_4_NAME,
    FINAL_CIP_CODE_4,
    FINAL_CIP_CODE_4_NAME,
    FINAL_CIP_CODE_2,
    FINAL_CIP_CODE_2_NAME,
    FINAL_CIP_CLUSTER_CODE,
    FINAL_CIP_CLUSTER_NAME,
    STP_CIP_CODE_4,
    STP_CIP_CODE_4_NAME
  )

cred_non_dup <- cred_non_dup %>%
  select(
    -any_of(c(
      "OUTCOMES_CIP_CODE_4",
      "OUTCOMES_CIP_CODE_4_NAME",
      "FINAL_CIP_CODE_4",
      "FINAL_CIP_CODE_4_NAME",
      "FINAL_CIP_CODE_2",
      "FINAL_CIP_CODE_2_NAME",
      "FINAL_CIP_CLUSTER_CODE",
      "FINAL_CIP_CLUSTER_NAME",
      "STP_CIP_CODE_4",
      "STP_CIP_CODE_4_NAME"
    ))
  ) %>%
  left_join(
    dacso_join,
    by = c(
      "PSI_CODE",
      "PSI_PROGRAM_CODE",
      "PSI_CREDENTIAL_PROGRAM_DESCRIPTION",
      "PSI_CREDENTIAL_CIP",
      "PSI_CREDENTIAL_LEVEL",
      "PSI_CREDENTIAL_CATEGORY",
      "OUTCOMES_CRED"
    )
  )

# BGS (BC Government Student outcomes) records weren't matched by DACSO.
# These are matched by a simple ID lookup from the BGS program matching script
# (02a-bgs-program-matching). rows_update only overwrites NA values where IDs match.
# ---- Step 3: Update CIP codes from BGS program matching ----

bgs_cips <- sch_tbl("Credential_Non_Dup_BGS_IDs") %>% collect()
bgs_cips <- bgs_cips |> rename_with(toupper)

bgs_updates <- bgs_cips %>%
  select(
    ID,
    FINAL_CIP_CODE_4,
    FINAL_CIP_CODE_4_NAME,
    FINAL_CIP_CODE_2,
    FINAL_CIP_CODE_2_NAME,
    FINAL_CIP_CLUSTER_CODE,
    FINAL_CIP_CLUSTER_NAME
  ) %>%
  # Ensure ID is unique to prevent rows_update() from erroring
  distinct(ID, .keep_all = TRUE)

cred_non_dup <- cred_non_dup %>%
  rows_update(bgs_updates, by = "ID", unmatched = "ignore")

# GRAD (graduate outcomes) records get their CIP codes from the GRAD matching.
# Like BGS, this is a simple ID-based lookup applied after DACSO.
# ---- Step 4: Update CIP codes from GRAD program matching ----

grad_cips <- sch_tbl("Credential_Non_Dup_GRAD_IDs") %>% collect()
grad_cips <- grad_cips |> rename_with(toupper)

grad_updates <- grad_cips %>%
  select(
    ID,
    FINAL_CIP_CODE_4,
    FINAL_CIP_CODE_4_NAME,
    FINAL_CIP_CODE_2,
    FINAL_CIP_CODE_2_NAME
  )

cred_non_dup <- cred_non_dup %>%
  rows_update(grad_updates, by = "ID", unmatched = "ignore")

# APPSO (Apprentice outcomes) records get their CIP codes from the APPSO
# cleaning script (02a-appso-programs). Like BGS/GRAD, simple ID-based lookup.
# ---- Step 5: Update CIP codes from APPSO program matching ----

appso_cips <- sch_tbl("Credential_Non_Dup_APPSO_IDs") %>% collect()
appso_cips <- appso_cips |> rename_with(toupper)

appso_updates <- appso_cips %>%
  select(
    ID,
    FINAL_CIP_CODE_4,
    FINAL_CIP_CODE_4_NAME,
    FINAL_CIP_CODE_2,
    FINAL_CIP_CODE_2_NAME
  )

cred_non_dup <- cred_non_dup %>%
  rows_update(appso_updates, by = "ID", unmatched = "ignore")

# GRAD and APPSO records need cluster codes (broader career groupings) that
# map from their 2-digit CIP codes. These clusters are used in downstream occupation
# matching (script 07) to group CIP programs into occupational categories.
# ---- Step 6: Populate cluster codes for GRAD and APPSO records ----

cip2_lookup <- sch_tbl("INFOWARE_L_CIP_2DIGITS_CIP2016") %>%
  select(LCP2_CD, LCP2_LCIPPC_CD, LCP2_LCIPPC_NAME) %>%
  collect()
cip2_lookup <- cip2_lookup |> rename_with(toupper)

cred_non_dup <- cred_non_dup %>%
  left_join(cip2_lookup, by = c("FINAL_CIP_CODE_2" = "LCP2_CD")) %>%
  mutate(
    FINAL_CIP_CLUSTER_CODE = case_when(
      OUTCOMES_CRED %in%
        c("GRAD", "APPSO") &
        !is.na(LCP2_LCIPPC_CD) ~ LCP2_LCIPPC_CD,
      TRUE ~ FINAL_CIP_CLUSTER_CODE
    ),
    FINAL_CIP_CLUSTER_NAME = case_when(
      OUTCOMES_CRED %in%
        c("GRAD", "APPSO") &
        !is.na(LCP2_LCIPPC_NAME) ~ LCP2_LCIPPC_NAME,
      TRUE ~ FINAL_CIP_CLUSTER_NAME
    )
  ) %>%
  select(-LCP2_LCIPPC_CD, -LCP2_LCIPPC_NAME)

# Write updated Credential_Non_Dup back to database
dbWriteTable(
  con,
  SQL(glue::glue('"{my_schema}"."Credential_Non_Dup"')),
  cred_non_dup,
  overwrite = TRUE
)

# ---- Check for leftover NULLs in final CIP 4 column ----
# WHY: This diagnostic check shows how many records still need CIP codes after the
# four primary sources. These NULLs will be filled by the STP fallback below.
tbl(con, "Credential_Non_Dup") %>%
  filter(is.na(FINAL_CIP_CODE_4)) %>%
  count(OUTCOMES_CRED, FINAL_CIP_CODE_4)

# ==============================================================================
# CLEAN UP NULLS — Match leftover NULLs using STP (institution-reported) CIP codes
# ==============================================================================

# Some credentials weren't matched by any of the four CIP sources above (DACSO,
# BGS, GRAD, APPSO). As a last resort, we use the institution's own reported CIP code
# (STP_CIP), clean it against the INFOWARE taxonomy (same cleaning logic as in
# 02a-appso-programs), and use the result. This ensures every record has a CIP code
# for downstream processing.

# Extract distinct CIP codes from unmatched records, clean them, then join back.
# ---- Step 7: Create cleaning table for NULL CIP records ----

null_cleaning <- cred_non_dup %>%
  filter(is.na(FINAL_CIP_CODE_4)) %>%
  count(PSI_CREDENTIAL_CIP, OUTCOMES_CRED, name = "Expr1")

dbWriteTable(
  con,
  "Credential_Non_Dup_STP_NULL_Cleaning",
  null_cleaning,
  overwrite = TRUE
)

# KEPT AS SQL: ALTER TABLE (DDL)
dbExecute(
  con,
  "ALTER TABLE Credential_Non_Dup_STP_NULL_Cleaning
ADD STP_CIP_CODE_4 varchar (255),
STP_CIP_CODE_4_NAME varchar (255),
STP_CIP_CODE_2 varchar (255),
STP_CIP_CODE_2_NAME varchar (255),
STP_CIP_CLUSTER_CODE varchar(10),
STP_CIP_CLUSTER_NAME varchar(255),
PSI_CREDENTIAL_CIP_orig varchar (255)"
)

# We'll modify PSI_CREDENTIAL_CIP during cleaning, so we preserve the original
# to use as a join key when matching the cleaned results back to credential records
# (Step 12).
# ---- Step 8: Save original CIP before cleaning ----

null_cleaning <- null_cleaning %>%
  mutate(PSI_CREDENTIAL_CIP_orig = PSI_CREDENTIAL_CIP)

# Same cleaning logic as in 02a-appso-programs Step 2 — institution CIP codes
# may be missing leading zeros or have the wrong number of digits, which prevents
# matching against the INFOWARE lookup tables.
# ---- Step 9: Fix CIP codes with wrong length ----

null_cleaning <- null_cleaning %>%
  mutate(
    PSI_CREDENTIAL_CIP = case_when(
      nchar(PSI_CREDENTIAL_CIP) == 6 &
        !grepl("\\.", substring(PSI_CREDENTIAL_CIP, 1, 2)) ~ paste0(
        PSI_CREDENTIAL_CIP,
        "0"
      ),
      TRUE ~ PSI_CREDENTIAL_CIP
    ),
    PSI_CREDENTIAL_CIP = case_when(
      nchar(PSI_CREDENTIAL_CIP) == 6 ~ paste0("0", PSI_CREDENTIAL_CIP),
      TRUE ~ PSI_CREDENTIAL_CIP
    )
  )

# Try to match each cleaned CIP code to the official INFOWARE taxonomy using
# progressively shorter matches (exact 6-digit → 5-digit → general → 2-digit),
# same strategy as 02a-appso-programs Step 3.
# ---- Step 10: Match CIP codes from INFOWARE lookup tables ----

cip6 <- sch_tbl("INFOWARE_L_CIP_6DIGITS_CIP2016") %>%
  select(LCIP_CD_WITH_PERIOD, LCIP_LCP4_CD, LCIP_LCP2_CD) %>%
  collect()
cip6 <- cip6 |> rename_with(toupper)

cip4 <- sch_tbl("INFOWARE_L_CIP_4DIGITS_CIP2016") %>%
  select(LCP4_CD, LCP4_CIP_4DIGITS_NAME) %>%
  collect()
cip4 <- cip4 |> rename_with(toupper)

cip2 <- sch_tbl("INFOWARE_L_CIP_2DIGITS_CIP2016") %>%
  select(LCP2_CD, LCP2_DIGITS_NAME, LCP2_LCIPPC_CD, LCP2_LCIPPC_NAME) %>%
  collect()
cip2 <- cip2 |> rename_with(toupper)

# Step 10a: Exact match on full 6-digit CIP
null_cleaning <- null_cleaning %>%
  left_join(cip6, by = c("PSI_CREDENTIAL_CIP" = "LCIP_CD_WITH_PERIOD")) %>%
  rename(STP_CIP_CODE_4 = LCIP_LCP4_CD, STP_CIP_CODE_2 = LCIP_LCP2_CD)

# Step 10b: Partial match on first 5 digits (for those still NULL)
# Some valid CIPs differ only in the last digit from a known code
cip6_partial <- cip6 %>%
  mutate(PSI_CIP_5 = substr(LCIP_CD_WITH_PERIOD, 1, 5))

null_cleaning <- null_cleaning %>%
  mutate(PSI_CIP_5 = substr(PSI_CREDENTIAL_CIP, 1, 5)) %>%
  left_join(
    cip6_partial %>% filter(!duplicated(PSI_CIP_5)),
    by = "PSI_CIP_5"
  ) %>%
  mutate(
    STP_CIP_CODE_4 = coalesce(STP_CIP_CODE_4, LCIP_LCP4_CD),
    STP_CIP_CODE_2 = coalesce(STP_CIP_CODE_2, LCIP_LCP2_CD)
  ) %>%
  select(-PSI_CIP_5, -LCIP_CD_WITH_PERIOD, -LCIP_LCP4_CD, -LCIP_LCP2_CD)

# Step 10c: General program CIPs (XX.00 → XX.01)
# WHY: Some CIP families have a "general" code (XX.00) that doesn't exist in INFOWARE.
# Map these to the first specific sub-category as a reasonable default.
general_programs <- c(
  "11.00",
  "13.00",
  "14.00",
  "19.00",
  "23.00",
  "24.00",
  "26.00",
  "40.00",
  "42.00",
  "45.00",
  "50.00",
  "52.00",
  "55.00"
)

null_cleaning <- null_cleaning %>%
  mutate(
    STP_CIP_CODE_4 = case_when(
      substr(PSI_CREDENTIAL_CIP, 1, 5) %in%
        general_programs &
        is.na(STP_CIP_CODE_4) ~ paste0(substr(PSI_CREDENTIAL_CIP, 1, 2), "01"),
      TRUE ~ STP_CIP_CODE_4
    )
  )

# Step 10d: Fall back to first 2 digits for any still-unmatched 2-digit CIP codes
null_cleaning <- null_cleaning %>%
  mutate(PSI_CIP_2 = substr(PSI_CREDENTIAL_CIP, 1, 2)) %>%
  left_join(
    cip6 %>%
      mutate(PSI_CIP_2 = substr(LCIP_CD_WITH_PERIOD, 1, 2)) %>%
      filter(!duplicated(PSI_CIP_2)),
    by = "PSI_CIP_2"
  ) %>%
  mutate(STP_CIP_CODE_2 = coalesce(STP_CIP_CODE_2, LCIP_LCP2_CD)) %>%
  select(-PSI_CIP_2, -LCIP_CD_WITH_PERIOD, -LCIP_LCP4_CD, -LCIP_LCP2_CD)

# Add human-readable names for the matched CIP codes, needed for reporting and
# for analysts to verify that the CIP matches are sensible.
# ---- Step 11: Add CIP names from lookup tables ----

null_cleaning <- null_cleaning %>%
  left_join(cip4, by = c("STP_CIP_CODE_4" = "LCP4_CD")) %>%
  rename(STP_CIP_CODE_4_NAME = LCP4_CIP_4DIGITS_NAME)

null_cleaning <- null_cleaning %>%
  left_join(cip2, by = c("STP_CIP_CODE_2" = "LCP2_CD")) %>%
  rename(
    STP_CIP_CODE_2_NAME = LCP2_DIGITS_NAME,
    STP_CIP_CLUSTER_CODE = LCP2_LCIPPC_CD,
    STP_CIP_CLUSTER_NAME = LCP2_LCIPPC_NAME
  )

# Flag unmatched 4-digit CIPs so analysts can investigate
null_cleaning <- null_cleaning %>%
  mutate(
    STP_CIP_CODE_4_NAME = ifelse(
      is.na(STP_CIP_CODE_4_NAME),
      "Invalid 4-digit CIP",
      STP_CIP_CODE_4_NAME
    )
  )

# Write cleaned NULL table back for the join step
dbWriteTable(
  con,
  "Credential_Non_Dup_STP_NULL_Cleaning",
  null_cleaning %>% select(-any_of(c("PSI_CIP_5"))),
  overwrite = TRUE
)

# Join the cleaned CIP results back to the original credential records that had
# NULL final CIP codes, creating a lookup table for the final update in Step 13.
# Match on the original (pre-cleaning) CIP code and OUTCOMES_CRED to ensure
# each cleaned CIP maps to the correct credential records.
# ---- Step 12: Create ID list of NULL records with matched STP CIPs ----

null_ids <- cred_non_dup %>%
  filter(is.na(FINAL_CIP_CODE_4)) %>%
  inner_join(
    null_cleaning %>%
      select(
        PSI_CREDENTIAL_CIP_orig,
        OUTCOMES_CRED,
        STP_CIP_CODE_4,
        STP_CIP_CODE_4_NAME,
        STP_CIP_CODE_2,
        STP_CIP_CODE_2_NAME,
        STP_CIP_CLUSTER_CODE,
        STP_CIP_CLUSTER_NAME
      ),
    by = c("PSI_CREDENTIAL_CIP" = "PSI_CREDENTIAL_CIP_orig", "OUTCOMES_CRED")
  ) %>%
  mutate(
    FINAL_CIP_CODE_4 = STP_CIP_CODE_4,
    FINAL_CIP_CODE_4_NAME = STP_CIP_CODE_4_NAME,
    FINAL_CIP_CODE_2 = STP_CIP_CODE_2,
    FINAL_CIP_CODE_2_NAME = STP_CIP_CODE_2_NAME,
    FINAL_CIP_CLUSTER_CODE = STP_CIP_CLUSTER_CODE,
    FINAL_CIP_CLUSTER_NAME = STP_CIP_CLUSTER_NAME,
    PSI_PROGRAM_CODE = ifelse(
      PSI_PROGRAM_CODE == "(Unspecified)",
      NA_character_,
      PSI_PROGRAM_CODE
    )
  ) %>%
  select(
    ID,
    PSI_CODE,
    PSI_PROGRAM_CODE,
    PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    PSI_CREDENTIAL_CIP,
    PSI_AWARD_SCHOOL_YEAR,
    OUTCOMES_CRED,
    FINAL_CIP_CODE_4,
    FINAL_CIP_CODE_4_NAME,
    FINAL_CIP_CODE_2,
    FINAL_CIP_CODE_2_NAME,
    FINAL_CIP_CLUSTER_CODE,
    FINAL_CIP_CLUSTER_NAME
  )

dbWriteTable(con, "Credential_Non_Dup_NULL_IDs", null_ids, overwrite = TRUE)

# This is the final merge — update the main credential table with the STP-derived
# CIP codes for records that weren't matched by any of the four primary sources.
# After this step, every record should have a FINAL_CIP_CODE_4 value.
# ---- Step 13: Apply the NULL CIP fallback to Credential_Non_Dup ----

null_updates <- null_ids %>%
  select(
    ID,
    FINAL_CIP_CODE_4,
    FINAL_CIP_CODE_4_NAME,
    FINAL_CIP_CODE_2,
    FINAL_CIP_CODE_2_NAME,
    FINAL_CIP_CLUSTER_CODE,
    FINAL_CIP_CLUSTER_NAME
  )

cred_non_dup <- cred_non_dup %>%
  rows_update(null_updates, by = "ID", unmatched = "ignore")

# Write final updated table
dbWriteTable(
  con,
  SQL(glue::glue('"{my_schema}"."Credential_Non_Dup"')),
  cred_non_dup,
  overwrite = TRUE
)

# ---- Check for remaining NULLs ----
tbl(con, "Credential_Non_Dup") %>%
  filter(is.na(FINAL_CIP_CODE_4)) %>%
  count(OUTCOMES_CRED, FINAL_CIP_CODE_4)

# BGS records with CIP code "99" represent "undeclared activity" — they need
# their cluster names set explicitly. Also fill any remaining NULL FINAL_CIPs with
# the institution-reported STP values as an absolute last resort.
# ---- Step 14: Final cleanup of edge cases ----

cred_non_dup <- cred_non_dup %>%
  mutate(
    FINAL_CIP_CODE_2_NAME = case_when(
      OUTCOMES_CRED == "BGS" & FINAL_CIP_CODE_2 == "99" ~ "Undeclared activity",
      TRUE ~ FINAL_CIP_CODE_2_NAME
    ),
    FINAL_CIP_CLUSTER_CODE = case_when(
      OUTCOMES_CRED == "BGS" & FINAL_CIP_CODE_2 == "99" ~ "99",
      TRUE ~ FINAL_CIP_CLUSTER_CODE
    ),
    FINAL_CIP_CLUSTER_NAME = case_when(
      OUTCOMES_CRED == "BGS" & FINAL_CIP_CODE_2 == "99" ~ "Undeclared activity",
      TRUE ~ FINAL_CIP_CLUSTER_NAME
    )
  )

# Fall back to STP CIP for any records where FINAL_CIP is still NULL or blank
cred_non_dup <- cred_non_dup %>%
  mutate(
    FINAL_CIP_CODE_4 = case_when(
      is.na(FINAL_CIP_CODE_4) | FINAL_CIP_CODE_4 == " " ~ STP_CIP_CODE_4,
      TRUE ~ FINAL_CIP_CODE_4
    ),
    FINAL_CIP_CODE_4_NAME = case_when(
      is.na(FINAL_CIP_CODE_4) | FINAL_CIP_CODE_4 == " " ~ STP_CIP_CODE_4_NAME,
      TRUE ~ FINAL_CIP_CODE_4_NAME
    ),
    FINAL_CIP_CODE_2 = case_when(
      is.na(FINAL_CIP_CODE_4) | FINAL_CIP_CODE_4 == " " ~ STP_CIP_CODE_2,
      TRUE ~ FINAL_CIP_CODE_2
    ),
    FINAL_CIP_CODE_2_NAME = case_when(
      is.na(FINAL_CIP_CODE_4) | FINAL_CIP_CODE_4 == " " ~ STP_CIP_CODE_2_NAME,
      TRUE ~ FINAL_CIP_CODE_2_NAME
    )
  )

# Set cluster to '99'/'Undeclared activity' for GRAD records with null clusters and CIP 99
cred_non_dup <- cred_non_dup %>%
  mutate(
    FINAL_CIP_CLUSTER_CODE = case_when(
      OUTCOMES_CRED == "GRAD" &
        is.na(FINAL_CIP_CLUSTER_CODE) &
        is.na(FINAL_CIP_CLUSTER_NAME) &
        FINAL_CIP_CODE_2 == "99" ~ "99",
      TRUE ~ FINAL_CIP_CLUSTER_CODE
    ),
    FINAL_CIP_CLUSTER_NAME = case_when(
      OUTCOMES_CRED == "GRAD" &
        is.na(FINAL_CIP_CLUSTER_CODE) &
        is.na(FINAL_CIP_CLUSTER_NAME) &
        FINAL_CIP_CODE_2 == "99" ~ "Undeclared activity",
      TRUE ~ FINAL_CIP_CLUSTER_NAME
    )
  )

# Write final table
dbWriteTable(
  con,
  SQL(glue::glue('"{my_schema}"."Credential_Non_Dup"')),
  cred_non_dup,
  overwrite = TRUE
)

# ---- Clean up ----
dbExecute(con, "DROP TABLE Credential_Non_Dup_STP_NULL_Cleaning")
dbDisconnect(con)



# ==============================================================================
# FILE: 02b-1-pssm-cohorts_dplyr.R
# ==============================================================================


# PSSM Cohorts — dplyr Translation
# Original: R/02b-1-pssm-cohorts.R
#
# Pipeline context:
#   Processes 4 student outcome surveys (TRD, APPSO, BGS, DACSO) into a unified
#   T_Cohorts_Recoded table used by all downstream model steps (02b-2 through 08).
#   Each survey requires different preprocessing:
#     TRD:   Apply weights, derive labour supply, add age groups
#     APPSO: Data is already preprocessed; just reshape for T_Cohorts_Recoded
#     BGS:   Recode institutions, update CIPs, apply weights, derive labour supply
#     DACSO: Join with credential grouping, update fields, filter, apply weights
#
#   All surveys share a common output schema (T_Cohorts_Recoded) with columns:
#     pen, stqu_id, survey, survey_year, inst_cd, lcp4_cd, ttrain, noc_cd,
#     age_at_survey, age_group, age_group_rollup, grad_status, respondent,
#     new_labour_supply, old_labour_supply, weight, pssm_credential, pssm_cred,
#     lcip4_cred, lcip2_cred, current_region_pssm_code
#
# Input tables:
#   - TRD: t_trd_data, t_weights, t_year_survey_year, tbl_age, tbl_age_groups
#   - APPSO: t_appso_data_final, t_year_survey_year, tbl_age, tbl_age_groups
#   - BGS: t_bgs_data_final, t_bgs_inst_recode, t_bgs_data_final_for_outcomesmatching,
#           t_weights, tbl_age, tbl_age_groups
#   - DACSO: t_dacso_data_part_1_stepa, t_pssm_credential_grouping,
#            infoware_c_outc_clean_short_resp, t_weights, t_year_survey_year,
#            tbl_age, tbl_age_groups
#
# Output:
#   - T_Cohorts_Recoded — unified cohort table (replaces old survey records)
#   - t_dacso_data_part_1 — intermediate DACSO table (kept for downstream use)

library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(RJDBC)
library(dbplyr)
library(glue)
library(assertthat)

# ---- Configure LAN and file paths ----
db_config <- config::get("decimal")
lan <- config::get("lan")
my_schema <- config::get("myschema")

# ---- Connection to decimal ----
decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")

# Helper: reference a table in the user's schema
sch_tbl <- function(name) {
  tbl(decimal_con, dbplyr::in_schema(my_schema, name))
}

# List of required tables with categories
required_tables <- list(
  TRD = c("TRD_Graduates", "T_TRD_DATA"),
  APP = c("T_APPSO_DATA_Final", "APPSO_Graduates"),
  BGS = c("T_BGS_Data_Final", "T_BGS_INST_Recode", "T_bgs_data_final_for_outcomesmatching", "T_Weights"),
  DACSO = c("t_dacso_data_part_1_stepa", "infoware_c_outc_clean_short_resp"),
  Lookups = c("t_current_region_pssm_codes", "t_current_region_pssm_rollup_codes",
              "t_current_region_pssm_rollup_codes_bc", "tbl_age", "tbl_age_groups",
              "t_pssm_credential_grouping", "t_year_survey_year")
)

# Check for required data tables in the database
for (category in names(required_tables)) {
  for (table_name in required_tables[[category]]) {
    full_table_name <- SQL(glue::glue('"{my_schema}"."{table_name}"'))
    assert_that(
      dbExistsTable(decimal_con, full_table_name),
      msg = paste("Error:", table_name, "does not exist in schema", my_schema)
    )
  }
}


# ---- Pull all shared lookup tables ----
# These lookups are used by multiple surveys, so we pull them once at the start.
# After collect(), all column names are uppercased for consistency with SQL Server.

tbl_age <- sch_tbl("tbl_age") %>%
  select(AGE, AGE_GROUP) %>%
  collect() |> rename_with(toupper)

tbl_age_groups <- sch_tbl("tbl_age_groups") %>%
  select(AGE_GROUP, AGE_GROUP_ROLLUP) %>%
  collect() |> rename_with(toupper)

year_survey <- sch_tbl("t_year_survey_year") %>%
  collect() |> rename_with(toupper)

t_weights <- sch_tbl("T_Weights") %>%
  collect() |> rename_with(toupper)

pssm_cred_grouping <- sch_tbl("t_pssm_credential_grouping") %>%
  collect() |> rename_with(toupper)

# Determine weight column based on run type.
# WHY: Regular/PTIB runs use the standard Weight, QI runs use Weight_QI.
# Each model run type sets different flags (only one of regular_run, qi_run, ptib_run is TRUE).
if (regular_run | ptib_run) {
  weight_var <- "WEIGHT"
} else if (qi_run) {
  weight_var <- "WEIGHT_QI"
}


# ******************************************************************************
# ---- TRD Queries ----
# Applies weight for model year and derives New Labour Supply
# ******************************************************************************

# Pull TRD data with only the columns needed for cohort building.
trd_data <- sch_tbl("t_trd_data") %>%
  select(PEN, KEY, SUBM_CD, INST, LCIP_CD, LCIP_LCP4_CD, TTRAIN, NOC_CD,
         TRD_AGE_AT_SURVEY, GRADSTAT_GROUP, RESPONDENT,
         TRD_LABR_EMPLOYED, TRD_LABR_IN_LABOUR_MARKET,
         PSSM_CREDENTIAL, CURRENT_REGION_PSSM_CODE) %>%
  collect() |> rename_with(toupper)

# Compute new_labour_supply from labour market response codes.
# WHY: The labour supply indicator determines whether a graduate is counted as
# contributing to the labour supply (1 = yes, 0 = no). Different surveys use
# different questions, so the logic varies by survey type.
# NOTE: In the original, this was an UPDATE...FROM SQL. Here we compute in R.
trd_data <- trd_data %>%
  mutate(NEW_LABOUR_SUPPLY = case_when(
    TRD_LABR_EMPLOYED == 1 ~ 1,
    TRD_LABR_IN_LABOUR_MARKET == 1 & TRD_LABR_EMPLOYED == 0 ~ 1,
    TRD_LABR_EMPLOYED == 0 ~ 0,
    RESPONDENT == "1" ~ 0,
    TRUE ~ 0
  ))

# Join with weights table (inner join — only matching rows get a weight value).
# The weight adjusts for survey non-response and ensures the sample is representative.
trd_weights <- t_weights %>%
  filter(MODEL == "2022-2023", SURVEY == "TRD") %>%
  select(SUBM_CD, all_of(weight_var)) %>%
  rename(WEIGHT = !!sym(weight_var))

trd_data <- trd_data %>%
  inner_join(trd_weights, by = "SUBM_CD")

# Build TRD cohort records for T_Cohorts_Recoded.
# WHY: This transforms raw TRD survey data into the unified cohort schema used by
# all downstream model steps. The CASE WHEN / string concatenation logic for composite
# keys (PSSM_CRED, LCIP4_CRED, LCIP2_CRED) is more readable in dplyr than in SQL.
# NOTE: ttrain = 2 is recoded to 1 for consistency with other surveys.
trd_cohort <- trd_data %>%
  inner_join(year_survey %>% filter(SURVEY == "TRD"), by = "SUBM_CD") %>%
  left_join(tbl_age, by = c("TRD_AGE_AT_SURVEY" = "AGE")) %>%
  left_join(tbl_age_groups, by = "AGE_GROUP") %>%
  mutate(
    TTRAIN_ADJ = if_else(TTRAIN == 2, 1, TTRAIN),
    TTRAIN_STR = if_else(TTRAIN == 2, "1", as.character(TTRAIN)),
    NOC_CD = if_else(NOC_CD == "XXXXX", "99999", NOC_CD)
  ) %>%
  transmute(
    PEN = PEN,
    STQU_ID = paste0("TRD - ", KEY),
    SURVEY = SURVEY.y,
    SURVEY_YEAR = SURVEY_YEAR,
    INST_CD = INST,
    LCIP_CD = LCIP_CD,
    LCP4_CD = LCIP_LCP4_CD,
    TTRAIN = TTRAIN_ADJ,
    NOC_CD = NOC_CD,
    AGE_AT_SURVEY = TRD_AGE_AT_SURVEY,
    AGE_GROUP = AGE_GROUP,
    AGE_GROUP_ROLLUP = AGE_GROUP_ROLLUP,
    GRAD_STATUS = GRADSTAT_GROUP,
    RESPONDENT = RESPONDENT,
    NEW_LABOUR_SUPPLY = NEW_LABOUR_SUPPLY,
    OLD_LABOUR_SUPPLY = NA_real_,
    WEIGHT = WEIGHT,
    PSSM_CREDENTIAL = PSSM_CREDENTIAL,
    PSSM_CRED = paste0(GRADSTAT_GROUP, " - ", PSSM_CREDENTIAL),
    LCIP4_CRED = paste0(GRADSTAT_GROUP, " - ", LCIP_LCP4_CD, " - ", TTRAIN_STR, " - ", PSSM_CREDENTIAL),
    LCIP2_CRED = paste0(GRADSTAT_GROUP, " - ", str_sub(LCIP_LCP4_CD, 1, 2), " - ", TTRAIN_STR, " - ", PSSM_CREDENTIAL),
    CURRENT_REGION_PSSM_CODE = CURRENT_REGION_PSSM_CODE
  )


# ******************************************************************************
# ---- APPSO Queries ----
# Refresh survey records in T_Cohorts_Recoded
# ******************************************************************************

# APPSO data is already preprocessed by earlier steps. It already has new_labour_supply,
# weight, pssm_credential, and lcip4_cred. We just need to reshape it into the
# T_Cohorts_Recoded schema and join with age lookups.
appso_data <- sch_tbl("t_appso_data_final") %>%
  select(PEN, KEY, SUBM_CD, INST, LCIP_CD, LCIP_LCP4_CD, NOC_CD,
         APP_AGE_AT_SURVEY, RESPONDENT,
         NEW_LABOUR_SUPPLY, WEIGHT,
         PSSM_CREDENTIAL, LCIP4_CRED, CURRENT_REGION_PSSM_CODE) %>%
  collect() |> rename_with(toupper)

# Build APPSO cohort records. Note the differences from TRD:
#   - All APPSO records have grad_status = '1' (they are all graduates)
#   - LCIP2_CRED is derived differently (no grad_status prefix)
#   - ttrain is not in APPSO data (not applicable to apprenticeship)
appso_cohort <- appso_data %>%
  inner_join(year_survey %>% filter(SURVEY == "appso"), by = "SUBM_CD") %>%
  left_join(tbl_age, by = c("APP_AGE_AT_SURVEY" = "AGE")) %>%
  left_join(tbl_age_groups, by = "AGE_GROUP") %>%
  mutate(
    NOC_CD = if_else(NOC_CD == "xxxxx", "99999", NOC_CD)
  ) %>%
  transmute(
    PEN = PEN,
    STQU_ID = paste0("APPSO - ", as.integer(KEY)),
    SURVEY = SURVEY.y,
    SURVEY_YEAR = SURVEY_YEAR,
    INST_CD = INST,
    LCIP_CD = LCIP_CD,
    LCP4_CD = LCIP_LCP4_CD,
    TTRAIN = NA_real_,
    NOC_CD = NOC_CD,
    AGE_AT_SURVEY = APP_AGE_AT_SURVEY,
    AGE_GROUP = AGE_GROUP,
    AGE_GROUP_ROLLUP = AGE_GROUP_ROLLUP,
    GRAD_STATUS = "1",
    RESPONDENT = RESPONDENT,
    NEW_LABOUR_SUPPLY = NEW_LABOUR_SUPPLY,
    OLD_LABOUR_SUPPLY = NA_real_,
    WEIGHT = WEIGHT,
    PSSM_CREDENTIAL = PSSM_CREDENTIAL,
    PSSM_CRED = PSSM_CREDENTIAL,
    LCIP4_CRED = LCIP4_CRED,
    LCIP2_CRED = paste0(str_sub(LCIP_LCP4_CD, 1, 2), " - ", PSSM_CREDENTIAL),
    CURRENT_REGION_PSSM_CODE = CURRENT_REGION_PSSM_CODE
  )


# ******************************************************************************
# ---- BGS Queries ----
# Recode institution codes, update CIPs, derive labour supply, apply weights
# ******************************************************************************

# Pull BGS data and related lookup tables.
bgs_data <- sch_tbl("t_bgs_data_final") %>%
  collect() |> rename_with(toupper)

bgs_inst_recode <- sch_tbl("T_BGS_INST_Recode") %>%
  collect() |> rename_with(toupper)

bgs_outcomes <- sch_tbl("T_bgs_data_final_for_outcomesmatching") %>%
  select(STQU_ID, FINAL_CIP_CODE_4, FINAL_CIP_CODE_2, FINAL_CIP_CLUSTER_CODE) %>%
  collect() |> rename_with(toupper)

# ---- BGS_Q001b: Recode institution codes ----
# WHY: Some BGS institutions have codes that changed over time. The recode table maps
# old codes to the current standard so that weight adjustments across years by program
# can be applied consistently.
bgs_data <- bgs_data %>%
  left_join(bgs_inst_recode %>% select(INST, INST_RECODE), by = "INST") %>%
  mutate(INST = coalesce(INST_RECODE, INST)) %>%
  select(-INST_RECODE)

# ---- BGS_Q001c: Update CIPs after program matching ----
# WHY: The outcomes matching step determines the correct CIP code for each record.
# We update the main BGS table with these matched CIP codes.
bgs_data <- bgs_data %>%
  left_join(bgs_outcomes, by = "STQU_ID") %>%
  mutate(
    CIP_CODE_4 = coalesce(FINAL_CIP_CODE_4, CIP_CODE_4),
    CIP_CODE_2 = coalesce(FINAL_CIP_CODE_2, CIP_CODE_2),
    LCIP_LCIPPC_CD = coalesce(FINAL_CIP_CLUSTER_CODE, LCIP_LCIPPC_CD)
  ) %>%
  select(-FINAL_CIP_CODE_4, -FINAL_CIP_CODE_2, -FINAL_CIP_CLUSTER_CODE)

# ---- BGS_Q002: Set lcip4_cred and pssm_credential ----
# WHY: BGS records are all bachelor's degrees, so the credential is always 'BACH'.
# The LCIP4_CRED composite key is built from CIP code + credential type.
bgs_data <- bgs_data %>%
  mutate(
    LCIP4_CRED = paste0(CIP_CODE_4, " - BACH"),
    PSSM_CREDENTIAL = "BACH"
  )

# Compute BGS labour supply from activity and labour force response codes.
# WHY: BGS uses different survey questions than TRD/DACSO to determine labour supply.
# The logic handles multiple response combinations for current activity and work status.
bgs_data <- bgs_data %>%
  mutate(BGS_NEW_LABOUR_SUPPLY = case_when(
    CURRENT_ACTIVITY == 1 ~ 1,
    CURRENT_ACTIVITY == 4 & FULL_TM_WRK == 1 ~ 1,
    CURRENT_ACTIVITY == 4 & FULL_TM_WRK == 0 ~ 2,
    CURRENT_ACTIVITY == 3 & IN_LBR_FRC == 1 ~ 1,
    is.na(CURRENT_ACTIVITY) & is.na(FULL_TM_WRK) & IN_LBR_FRC == 1 ~ 1,
    is.na(CURRENT_ACTIVITY) & IN_LBR_FRC == 1 ~ 1,
    SRV_Y_N == 0 ~ 0,
    TRUE ~ 0
  ))

# Join with weights (BGS joins on survey_year, not subm_cd like TRD/DACSO).
bgs_weights <- t_weights %>%
  filter(MODEL == "2022-2023", SURVEY == "BGS") %>%
  select(SURVEY_YEAR, all_of(weight_var)) %>%
  rename(WEIGHT = !!sym(weight_var))

bgs_data <- bgs_data %>%
  inner_join(bgs_weights, by = "SURVEY_YEAR")

# Build BGS cohort records.
# WHY: BGS records are all graduates (grad_status = '1'). The LCIP2_CRED uses a
# simpler format than TRD/DACSO since BGS doesn't have ttrain or grad_status grouping.
bgs_cohort <- bgs_data %>%
  mutate(
    NOC_CD = if_else(NOC == "XXXXX", "99999", NOC)
  ) %>%
  transmute(
    PEN = PEN,
    STQU_ID = paste0("BGS - ", as.integer(STQU_ID)),
    SURVEY = "BGS",
    SURVEY_YEAR = SURVEY_YEAR,
    INST_CD = INST,
    LCP4_CD = CIP_CODE_4,
    TTRAIN = NA_real_,
    NOC_CD = NOC_CD,
    AGE_AT_SURVEY = AGE,
    AGE_GROUP = AGE_GROUP,
    AGE_GROUP_ROLLUP = AGE_GROUP_ROLLUP,
    GRAD_STATUS = "1",
    RESPONDENT = SRV_Y_N,
    NEW_LABOUR_SUPPLY = BGS_NEW_LABOUR_SUPPLY,
    OLD_LABOUR_SUPPLY = OLD_LABOUR_SUPPLY,
    WEIGHT = WEIGHT,
    PSSM_CREDENTIAL = PSSM_CREDENTIAL,
    PSSM_CRED = PSSM_CREDENTIAL,
    LCIP4_CRED = LCIP4_CRED,
    LCIP2_CRED = paste0(str_sub(CIP_CODE_4, 2, 3), " - BACH"),
    CURRENT_REGION_PSSM_CODE = CURRENT_REGION_PSSM_CODE
  )


# ******************************************************************************
# ---- DACSO Queries ----
# Adds age, updates credential, creates new LCIP4_CRED variable
# ******************************************************************************

# Pull DACSO source data and infoware outcomes reference.
dacso_stepa <- sch_tbl("t_dacso_data_part_1_stepa") %>%
  collect() |> rename_with(toupper)

infoware_outc <- sch_tbl("infoware_c_outc_clean_short_resp") %>%
  select(STQU_ID, Q08, PFST_HAD_PREVIOUS_CDTL, PFST_FURSTDY_INCL_STILL_ATTD) %>%
  collect() |> rename_with(toupper)

# ---- DACSO_Q003: Build DACSO data part 1 ----
# Join stepa data with credential grouping to add PSSM credential categories,
# and with age tables to add age groups. Only include credentials that are in the
# model (dacso_include_in_model IS NOT NULL).
# WHY: DACSO covers many credential types, but only some are relevant for the PSSM
# model. The grouping table maps each credential to a PSSM category and flags which
# ones to include.
dacso_part1 <- dacso_stepa %>%
  inner_join(
    pssm_cred_grouping %>%
      filter(!is.na(DACSO_INCLUDE_IN_MODEL)) %>%
      select(PRGM_CREDENTIAL_AWARDED, PRGM_CREDENTIAL_AWARDED_NAME,
             PSSM_CREDENTIAL, PSSM_CREDENTIAL_NAME, DACSO_INCLUDE_IN_MODEL),
    by = c("PRGM_CREDENTIAL" = "PRGM_CREDENTIAL_AWARDED")
  ) %>%
  left_join(tbl_age, by = c("COCI_AGE_AT_SURVEY" = "AGE")) %>%
  left_join(tbl_age_groups, by = "AGE_GROUP") %>%
  mutate(
    LCIP4_CRED = paste0(
      as.character(COSC_GRAD_STATUS_LGDS_CD_GROUP), " - ", LCP4_CD, " - ",
      if_else(as.character(TTRAIN) == "2", "1", as.character(TTRAIN)), " - ",
      PSSM_CREDENTIAL
    )
  )

# ---- DACSO_Q003b: Update fields from infoware outcomes ----
# WHY: Some DACSO fields need correction based on the cleaned outcomes data from
# INFOWARE. The had_previous_credential field uses a conditional: if q08 is '1',
# use the pfst value; otherwise use q08 directly.
dacso_part1 <- dacso_part1 %>%
  left_join(infoware_outc, by = c("COCI_STQU_ID" = "STQU_ID")) %>%
  mutate(
    HAD_PREVIOUS_CREDENTIAL = if_else(
      Q08 == "1", PFST_HAD_PREVIOUS_CDTL, Q08
    ),
    PFST_IN_POST_SEC_BEFORE = coalesce(Q08, PFST_IN_POST_SEC_BEFORE),
    PFST_HAD_PREVIOUS_CDTL = coalesce(PFST_HAD_PREVIOUS_CDTL, PFST_HAD_PREVIOUS_CDTL),
    PFST_FURSTDY_INCL_STILL_ATTD = coalesce(PFST_FURSTDY_INCL_STILL_ATTD, PFST_FURSTDY_INCL_STILL_ATTD)
  ) %>%
  select(-Q08, -PFST_HAD_PREVIOUS_CDTL.y, -PFST_FURSTDY_INCL_STILL_ATTD.y) %>%
  rename(
    PFST_HAD_PREVIOUS_CDTL = PFST_HAD_PREVIOUS_CDTL.x,
    PFST_FURSTDY_INCL_STILL_ATTD = PFST_FURSTDY_INCL_STILL_ATTD.x
  )

# ---- DACSO_Q005: Compute labour supply and weights ----
# WHY: DACSO uses different survey questions than TRD/BGS for labour supply.
# The logic handles current activity, employment status, and labour market participation.
dacso_part1 <- dacso_part1 %>%
  mutate(NEW_LABOUR_SUPPLY = case_when(
    PFST_CURRENT_ACTIVITY == 3 ~ 1,
    PFST_CURRENT_ACTIVITY == 2 & LABR_EMPLOYED_FULL_PART_TIME == 1 ~ 1,
    PFST_CURRENT_ACTIVITY == 2 & LABR_EMPLOYED_FULL_PART_TIME == 0 ~ 2,
    PFST_CURRENT_ACTIVITY == 4 & LABR_IN_LABOUR_MARKET == 1 ~ 1,
    RESPONDENT == "1" ~ 0,
    TRUE ~ 0
  ))

# Join with weights (DACSO joins on subm_cd).
dacso_weights <- t_weights %>%
  filter(MODEL == "2022-2023", SURVEY == "DACSO") %>%
  select(SUBM_CD, all_of(weight_var)) %>%
  rename(WEIGHT = !!sym(weight_var))

dacso_part1 <- dacso_part1 %>%
  inner_join(dacso_weights, by = c("COCI_SUBM_CD" = "SUBM_CD"))

# Write t_dacso_data_part_1 to database — this intermediate table is kept for
# downstream use and verification.
dbWriteTable(decimal_con, SQL(glue::glue('"{my_schema}"."t_dacso_data_part_1"')),
             dacso_part1, overwrite = TRUE)

# Build DACSO cohort records.
# WHY: DACSO has the most complex cohort record structure due to grad_status grouping,
# ttrain recoding, and the multiple composite keys (PSSM_CRED, LCIP4_CRED, LCIP2_CRED).
# Each key combines different dimensions for different levels of aggregation.
dacso_cohort <- dacso_part1 %>%
  inner_join(year_survey %>% filter(SURVEY == "DACSO"), by = c("COCI_SUBM_CD" = "SUBM_CD")) %>%
  mutate(
    TTRAIN_ADJ = if_else(TTRAIN == 2, 1, TTRAIN),
    TTRAIN_STR = if_else(TTRAIN == 2, "1", as.character(TTRAIN)),
    NOC_CD = if_else(LABR_OCCUPATION_LNOC_CD == "XXXXX", "99999", LABR_OCCUPATION_LNOC_CD)
  ) %>%
  transmute(
    PEN = COCI_PEN,
    STQU_ID = paste0("DACSO - ", COCI_STQU_ID),
    SURVEY = SURVEY.y,
    SURVEY_YEAR = SURVEY_YEAR,
    INST_CD = COCI_INST_CD,
    LCP4_CD = LCP4_CD,
    TTRAIN = TTRAIN_ADJ,
    NOC_CD = NOC_CD,
    AGE_AT_SURVEY = COCI_AGE_AT_SURVEY,
    AGE_GROUP = AGE_GROUP,
    AGE_GROUP_ROLLUP = AGE_GROUP_ROLLUP,
    GRAD_STATUS = COSC_GRAD_STATUS_LGDS_CD_GROUP,
    RESPONDENT = RESPONDENT,
    NEW_LABOUR_SUPPLY = NEW_LABOUR_SUPPLY,
    OLD_LABOUR_SUPPLY = OLD_LABOUR_SUPPLY,
    WEIGHT = WEIGHT,
    PSSM_CREDENTIAL = PSSM_CREDENTIAL,
    PSSM_CRED = paste0(COSC_GRAD_STATUS_LGDS_CD_GROUP, " - ", PSSM_CREDENTIAL),
    LCIP4_CRED = paste0(COSC_GRAD_STATUS_LGDS_CD_GROUP, " - ", LCP4_CD, " - ", TTRAIN_STR, " - ", PSSM_CREDENTIAL),
    LCIP2_CRED = paste0(COSC_GRAD_STATUS_LGDS_CD_GROUP, " - ", str_sub(LCP4_CD, 1, 2), " - ", TTRAIN_STR, " - ", PSSM_CREDENTIAL),
    CURRENT_REGION_PSSM_CODE = CURRENT_REGION_PSSM_CODE
  )


# ******************************************************************************
# ---- T_Cohorts_Recoded ----
# Refresh all survey records in T_Cohorts_Recoded
# ******************************************************************************

# Pull existing T_Cohorts_Recoded and remove old records for all 4 surveys.
# WHY: Each model run refreshes the survey data from scratch. The DELETE + INSERT
# pattern ensures no stale records remain. In dplyr, we filter out old survey records
# and bind the new ones.
cohorts <- sch_tbl("T_Cohorts_Recoded") %>%
  collect() |> rename_with(toupper)

# Remove old survey records for all 4 surveys
cohorts <- cohorts %>%
  filter(!SURVEY %in% c("TRD", "APPSO", "BGS", "DACSO"))

# Add new records from all 4 surveys
cohorts <- bind_rows(cohorts, trd_cohort, appso_cohort, bgs_cohort, dacso_cohort)

# Write the updated T_Cohorts_Recoded back to the database
dbWriteTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_Cohorts_Recoded"')),
             cohorts, overwrite = TRUE)


# ---- Keep ----
dbExistsTable(decimal_con, "APPSO_Graduates")
dbExistsTable(decimal_con, "TRD_Graduates")
dbExistsTable(decimal_con, "t_dacso_data_part_1")
dbExistsTable(decimal_con, "T_Cohorts_Recoded")

# ---- Clean Up Lookups (if desired, not a needed step) ----
# dbExecute(decimal_con, "DROP TABLE T_BGS_INST_Recode;")
# dbExecute(decimal_con, "DROP TABLE T_PSSM_Credential_Grouping")
# dbExecute(decimal_con, "DROP TABLE t_year_survey_year")
# dbExecute(decimal_con, "DROP TABLE t_current_region_pssm_codes")
# dbExecute(decimal_con, "DROP TABLE t_current_region_pssm_rollup_codes")
# dbExecute(decimal_con, "DROP TABLE t_current_region_pssm_rollup_codes_bc")

dbDisconnect(decimal_con)
# rm(list=ls())



# ==============================================================================
# FILE: 02b-2-pssm-cohorts-new-labour-supply_dplyr.R
# ==============================================================================


# New Labour Supply Distributions — dplyr Translation
# Original: R/02b-2-pssm-cohorts-new-labour-supply.R
#
# Pipeline context:
#   Processes cohort data from student outcomes and creates new labour supply
#   distributions. Runs after 02b-1-pssm-cohorts (which builds T_Cohorts_Recoded).
#
#   At a high level:
#     1. Updates invalid NOC codes in T_Cohorts_Recoded
#     2. Recodes NLS for records with NLS-2 but no NLS-1 (anti-join pattern)
#     3. Creates weights for new labour supply (Weight_NLS)
#     4. Builds 4 labour supply distribution tables (with/without TTRAIN, 4D/2D)
#     5. Appends StatCan census data
#
# Input tables:
#   - T_Cohorts_Recoded — unified cohort table (from 02b-1)
#   - T_Current_Region_PSSM_Codes / T_Current_Region_PSSM_Rollup_Codes — region lookups
#   - T_NOC_Broad_Categories — NOC validation lookup
#   - Labour_Supply_Distribution_Stat_Can — census data
#
# Output tables (4 variants + 1 weight table):
#   - Labour_Supply_Distribution — 4D CIP, with TTRAIN
#   - Labour_Supply_Distribution_No_TT — 4D CIP, without TTRAIN
#   - Labour_Supply_Distribution_LCP2 — 2D CIP, with TTRAIN
#   - Labour_Supply_Distribution_LCP2_No_TT — 2D CIP, without TTRAIN
#   - tmp_tbl_Weights_NLS — weight lookup kept for downstream use

library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(dbplyr)

# ---- Configure LAN and file paths ----
db_config <- config::get("decimal")
lan <- config::get("lan")
my_schema <- config::get("myschema")

decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")

# Helper: reference a table in the user's schema
sch_tbl <- function(name) {
  tbl(decimal_con, dbplyr::in_schema(my_schema, name))
}

# Helper: produce grad status prefix for composite keys.
# WHY: LCIP4_CRED and LCIP2_CRED conditionally prepend grad status with " - ".
# When grad status is NULL the prefix is empty; otherwise "status - ".
grad_prefix <- function(status) {
  if_else(is.na(status), "", paste0(status, " - "))
}

# ---- Check required tables ----
required_tables <- c(
  "t_cohorts_recoded",
  "t_current_region_pssm_rollup_codes",
  "t_current_region_pssm_codes",
  "T_NOC_Broad_Categories",
  "Labour_Supply_Distribution_Stat_Can"
)

for (table_name in required_tables) {
  full_table_name <- SQL(glue::glue('"{my_schema}"."{table_name}"'))
  assertthat::assert_that(
    dbExistsTable(decimal_con, full_table_name),
    msg = paste("Error:", table_name, "does not exist in schema", my_schema)
  )
}


# ******************************************************************************
# Part 0: Pull source data into R
# ******************************************************************************
# WHY: The entire pipeline operates on T_Cohorts_Recoded plus lookup tables.
# Pulling them into R once avoids repeated database round-trips.

cohorts <- sch_tbl("T_Cohorts_Recoded") %>%
  collect() |> rename_with(toupper)

region_codes <- sch_tbl("T_Current_Region_PSSM_Codes") %>%
  collect() |> rename_with(toupper)

region_rollup <- sch_tbl("T_Current_Region_PSSM_Rollup_Codes") %>%
  collect() |> rename_with(toupper)

noc_broad <- sch_tbl("T_NOC_Broad_Categories") %>%
  collect() |> rename_with(toupper)


# ******************************************************************************
# Part 1: Handle invalid NOC codes
# ******************************************************************************
# WHY: Some NOC codes in the cohort data are invalid (e.g., '4122X') and need to
# be set to '99999' (unknown). Also diagnostic checks for other invalid NOCs.

# Check base weights
cohorts %>%
  count(SURVEY, SURVEY_YEAR, WEIGHT)

# Find invalid NOC codes: records with a NOC that doesn't exist in the
# official NOC broad categories lookup, filtered to recent survey years.
# WHY: Identifies data quality issues that may need manual correction.
invalid_nocs <- cohorts %>%
  filter(!is.na(AGE_GROUP_ROLLUP),
         CURRENT_REGION_PSSM_CODE != -1,
         SURVEY_YEAR %in% c("2019", "2020", "2021", "2022", "2023"),
         !is.na(NOC_CD),
         NOC_CD != "") %>%
  anti_join(noc_broad, by = c("NOC_CD" = "UNIT_GROUP_CODE")) %>%
  distinct(STQU_ID, SURVEY, SURVEY_YEAR, NOC_CD)

# Fix known invalid NOC code 4122X → 99999
cohorts <- cohorts %>%
  mutate(NOC_CD = if_else(NOC_CD == "4122X", "99999", NOC_CD))


# ******************************************************************************
# Part 2: Recode NLS for records with NLS-2 but no NLS-1
# ******************************************************************************
# WHY: If a cohort member has a new labour supply value of 2 (NLS-2) but there's
# no matching NLS-1 record with the same survey/region/age/institution/credential,
# the NLS is recoded to 3 (indicating NLS-2 without NLS-1).
# This uses an anti-join pattern: find NLS-2 records that have no NLS-1 match.

# Common filter for both NLS-1 and NLS-2 queries: weight > 0, valid age, grad or non-completer
nls_filter <- function(df) {
  df %>%
    filter(as.numeric(WEIGHT) > 0,
           !is.na(AGE_GROUP_ROLLUP),
           GRAD_STATUS %in% c("1", "3"))
}

# NLS-1 records: group by key dimensions, filter to NLS=1
# WHY: We join with region rollup to get the rollup code for matching.
cohorts_with_region <- cohorts %>%
  inner_join(region_codes %>% select(CURRENT_REGION_PSSM_CODE, CURRENT_REGION_PSSM_CODE_ROLLOUP),
             by = "CURRENT_REGION_PSSM_CODE") %>%
  inner_join(region_rollup %>% select(CURRENT_REGION_PSSM_CODE_ROLLOUP),
             by = "CURRENT_REGION_PSSM_CODE_ROLLOUP")

nls1_keys <- cohorts_with_region %>%
  nls_filter() %>%
  filter(as.numeric(NEW_LABOUR_SUPPLY) == 1) %>%
  distinct(SURVEY, CURRENT_REGION_PSSM_CODE_ROLLOUP, AGE_GROUP_ROLLUP,
           INST_CD, LCIP4_CRED)

# NLS-2 records: find those with no matching NLS-1 record
nls2_no_nls1 <- cohorts_with_region %>%
  nls_filter() %>%
  filter(as.numeric(NEW_LABOUR_SUPPLY) == 2) %>%
  anti_join(nls1_keys,
            by = c("SURVEY", "CURRENT_REGION_PSSM_CODE_ROLLOUP",
                   "AGE_GROUP_ROLLUP", "INST_CD", "LCIP4_CRED"))

# Recode NLS from 2 → 3 for those without NLS-1
# WHY: The anti-join identifies exactly which STQU_IDs need recoding.
nls2_ids <- nls2_no_nls1 %>% distinct(STQU_ID)

cohorts <- cohorts %>%
  mutate(NEW_LABOUR_SUPPLY = if_else(
    STQU_ID %in% nls2_ids$STQU_ID & as.numeric(NEW_LABOUR_SUPPLY) == 2,
    3,
    NEW_LABOUR_SUPPLY
  ))


# ******************************************************************************
# Part 3: Count respondents by region (diagnostic)
# ******************************************************************************
# WHY: Provides a diagnostic view of cohort respondent counts by survey,
# year, and region for verification purposes.

cohorts_by_region <- cohorts %>%
  inner_join(region_codes %>% select(CURRENT_REGION_PSSM_CODE, CURRENT_REGION_PSSM_NAME),
             by = "CURRENT_REGION_PSSM_CODE") %>%
  filter(!is.na(AGE_GROUP_ROLLUP), RESPONDENT == "1", as.numeric(WEIGHT) > 0) %>%
  count(SURVEY, SURVEY_YEAR, CURRENT_REGION_PSSM_CODE, CURRENT_REGION_PSSM_NAME,
        AGE_GROUP_ROLLUP, name = "N", sort = TRUE)


# ******************************************************************************
# Part 4: Create NLS weights (Z01 → Z08)
# ******************************************************************************
# WHY: The NLS weight adjusts for non-response bias. It's computed as:
#   Weight_Prob = Count / Respondents (probability of selection)
#   Weight = Weight_Prob * Weight_Year (combine with year weight)
#   Weight_Adj_Fac = Base / Weighted (adjustment to match cohort totals)
#   Weight_NLS = Weight * Weight_Adj_Fac (final NLS weight)
# In the original, this was 8 temp tables. Here it's a single pipeline.

# ---- Z01: Base NLS records ----
# Filter to NLS in {0,1,2,3}, weight > 0, valid age, grad status 1 or 3
# WHY: These are the cohort records eligible for NLS weighting.
base_nls <- cohorts %>%
  filter(as.numeric(NEW_LABOUR_SUPPLY) %in% c(0, 1, 2, 3),
         as.numeric(WEIGHT) > 0,
         !is.na(AGE_GROUP_ROLLUP),
         GRAD_STATUS %in% c("1", "3"))

# ---- Z02c: Weight_tmp ----
# Count records and respondents by survey/year/institution/age/grad_status/ttrain/lcip4_cred
# WHY: Respondents are those who responded AND have a valid region code (not -1).
weight_tmp <- base_nls %>%
  group_by(SURVEY, SURVEY_YEAR, INST_CD, AGE_GROUP_ROLLUP, GRAD_STATUS,
           TTRAIN, LCIP4_CRED, WEIGHT) %>%
  summarise(
    COUNT = n(),
    RESPONDENTS = sum(if_else(RESPONDENT == "1" & CURRENT_REGION_PSSM_CODE != -1, 1, 0)),
    .groups = "drop"
  ) %>%
  rename(WEIGHT_YEAR = WEIGHT)

# ---- Z02c → Z04: Compute Weight_Prob, Weight, Weighted, then aggregate ----
# WHY: Weight_Prob = Count/Respondents adjusts for non-response. When Respondents=0
# (no respondents for that cell), Weight_Prob is set to 1 (no adjustment).
weight_full <- weight_tmp %>%
  mutate(
    WEIGHT_PROB = if_else(RESPONDENTS == 0, 1, as.numeric(COUNT) / as.numeric(RESPONDENTS)),
    WEIGHT = WEIGHT_PROB * as.numeric(WEIGHT_YEAR),
    WEIGHTED = as.numeric(RESPONDENTS) * WEIGHT_PROB * as.numeric(WEIGHT_YEAR)
  )

# ---- Z03: Weight_Total ----
# Aggregate weighted counts to the grouping level used for adjustment.
weight_total <- weight_full %>%
  group_by(SURVEY, INST_CD, AGE_GROUP_ROLLUP, GRAD_STATUS, TTRAIN, LCIP4_CRED) %>%
  summarise(
    BASE = sum(COUNT),
    WEIGHTED_TOTAL = sum(WEIGHTED),
    .groups = "drop"
  )

# ---- Z04: Weight_Adj_Fac ----
# WHY: The adjustment factor scales the weights so that the weighted total
# matches the base (unweighted) count. This corrects for over/under-representation.
weight_adj_fac <- weight_total %>%
  mutate(WEIGHT_ADJ_FAC = if_else(WEIGHTED_TOTAL == 0, 0,
                                   as.numeric(BASE) / as.numeric(WEIGHTED_TOTAL)))

# ---- Z05: tmp_tbl_weights_nls ----
# Join weight_full with adjustment factor and compute final Weight_NLS.
# WHY: This is the final weight table that gets applied to T_Cohorts_Recoded.
tmp_tbl_weights_nls <- weight_full %>%
  inner_join(
    weight_adj_fac %>% select(SURVEY, INST_CD, AGE_GROUP_ROLLUP, GRAD_STATUS,
                               TTRAIN, LCIP4_CRED, WEIGHT_ADJ_FAC),
    by = c("SURVEY", "INST_CD", "AGE_GROUP_ROLLUP", "GRAD_STATUS",
           "TTRAIN", "LCIP4_CRED")
  ) %>%
  mutate(WEIGHT_NLS = WEIGHT * WEIGHT_ADJ_FAC)

# ---- Z06-Z07: Add Weight_NLS column to T_Cohorts_Recoded ----
# KEPT AS SQL: ALTER TABLE to add/modify the Weight_NLS column
dbExecute(decimal_con, "ALTER TABLE T_Cohorts_Recoded ALTER COLUMN Weight_NLS Float NULL;")

# ---- Z07: Null out Weight_NLS (in R memory) ----
cohorts <- cohorts %>%
  mutate(WEIGHT_NLS = NA_real_)

# ---- Z08: Update Weight_NLS from tmp_tbl_weights_nls ----
# WHY: Only update records with valid region (not -1) that are in the base_nls set.
# Match on survey/year/inst/age/grad_status/ttrain/lcip4_cred.
base_nls_ids <- base_nls %>%
  distinct(STQU_ID)

weight_nls_lookup <- tmp_tbl_weights_nls %>%
  select(SURVEY, SURVEY_YEAR, INST_CD, AGE_GROUP_ROLLUP, GRAD_STATUS,
         TTRAIN, LCIP4_CRED, WEIGHT_NLS)

# Build the update: for each record in cohorts that is in base_nls and has
# valid region, look up the Weight_NLS from the weight table.
cohorts_to_update <- cohorts %>%
  semi_join(base_nls_ids, by = "STQU_ID") %>%
  filter(CURRENT_REGION_PSSM_CODE != -1) %>%
  inner_join(weight_nls_lookup,
             by = c("SURVEY", "SURVEY_YEAR", "INST_CD", "AGE_GROUP_ROLLUP",
                    "GRAD_STATUS", "TTRAIN", "LCIP4_CRED")) %>%
  select(STQU_ID, WEIGHT_NLS)

# Apply the update: replace Weight_NLS for matched records
cohorts <- cohorts %>%
  left_join(cohorts_to_update %>% rename(WEIGHT_NLS_NEW = WEIGHT_NLS),
            by = "STQU_ID") %>%
  mutate(WEIGHT_NLS = if_else(!is.na(WEIGHT_NLS_NEW), WEIGHT_NLS_NEW, WEIGHT_NLS)) %>%
  select(-WEIGHT_NLS_NEW)

# ---- Z09: Check weights (diagnostic) ----
# WHY: Verify that Weight_NLS values are reasonable. Records with Weight_NLS=0 or NULL
# and valid region should be investigated (may indicate missing data).
check_no_weight <- cohorts %>%
  semi_join(base_nls_ids, by = "STQU_ID") %>%
  filter(as.numeric(NEW_LABOUR_SUPPLY) %in% c(0, 1, 2, 3),
         as.numeric(WEIGHT) > 0,
         (is.na(WEIGHT_NLS) | WEIGHT_NLS == 0),
         CURRENT_REGION_PSSM_CODE != -1,
         !is.na(AGE_GROUP_ROLLUP),
         GRAD_STATUS %in% c("1", "2", "3")) %>%
  count(SURVEY, INST_CD, AGE_GROUP_ROLLUP, TTRAIN, LCIP4_CRED, GRAD_STATUS, name = "BASE")


# ******************************************************************************
# Part 5: Weighted labour supply distributions (Q006a → Q007a)
# ******************************************************************************
# WHY: This is the core computation. We compute weighted counts of respondents
# by NLS status across various grouping dimensions, then compute percentages.
# The result feeds 4 output tables:
#   - 4D CIP with TTRAIN
#   - 4D CIP without TTRAIN
#   - 2D CIP with TTRAIN
#   - 2D CIP without TTRAIN

# ---- Q006a: Weight respondents by NLS ----
# Join cohorts with region rollup, filter to respondents with valid weight.
# WHY: Only respondents (RESPONDENT='1') with positive weight contribute to
# the distribution. Region rollup code 9999 is excluded (unknown region).
weight_new_ls <- cohorts %>%
  inner_join(region_codes %>% select(CURRENT_REGION_PSSM_CODE, CURRENT_REGION_PSSM_CODE_ROLLOUP),
             by = "CURRENT_REGION_PSSM_CODE") %>%
  inner_join(region_rollup %>% select(CURRENT_REGION_PSSM_CODE_ROLLOUP),
             by = "CURRENT_REGION_PSSM_CODE_ROLLOUP") %>%
  filter(RESPONDENT == "1",
         as.numeric(WEIGHT) > 0,
         CURRENT_REGION_PSSM_CODE_ROLLOUP != 9999,
         !is.na(AGE_GROUP_ROLLUP),
         GRAD_STATUS %in% c("1", "3"),
         as.numeric(NEW_LABOUR_SUPPLY) %in% c(0, 1, 2, 3)) %>%
  mutate(WEIGHT_NLS = as.numeric(WEIGHT_NLS)) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
           SURVEY_YEAR, INST_CD, AGE_GROUP_ROLLUP, GRAD_STATUS,
           LCP4_CD, TTRAIN, LCIP4_CRED, LCIP2_CRED,
           NEW_LABOUR_SUPPLY, WEIGHT_NLS) %>%
  summarise(COUNT = n(), .groups = "drop") %>%
  mutate(WEIGHTED = COUNT * WEIGHT_NLS)

# ---- Q006b variants: Aggregate weighted counts by NLS status ----
# WHY: We need separate aggregations for NLS=1,2,3 (new labour supply) and NLS=0
# (no new labour supply), across 4 grouping variants.

# Variant A: NLS=1,2,3 with TTRAIN (4D CIP)
q006b_nls123 <- weight_new_ls %>%
  filter(as.numeric(NEW_LABOUR_SUPPLY) %in% c(1, 2, 3)) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
           AGE_GROUP_ROLLUP, LCP4_CD, TTRAIN, LCIP4_CRED, LCIP2_CRED) %>%
  summarise(COUNT = sum(WEIGHTED), UNWEIGHTED_COUNT = sum(COUNT), .groups = "drop")

# Variant B: NLS=0 with TTRAIN (4D CIP)
q006b_nls0 <- weight_new_ls %>%
  filter(as.numeric(NEW_LABOUR_SUPPLY) == 0) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
           AGE_GROUP_ROLLUP, LCP4_CD, TTRAIN, LCIP4_CRED, LCIP2_CRED) %>%
  summarise(COUNT = sum(WEIGHTED), .groups = "drop")

# Variant C: NLS=0 without TTRAIN (4D CIP, no TTRAIN dimension)
# WHY: No-TT variants drop TTRAIN from grouping and reconstruct LCIP4_CRED/LCIP2_CRED
# without TTRAIN but still including grad_status prefix.
q006b_nls0_no_tt <- weight_new_ls %>%
  filter(as.numeric(NEW_LABOUR_SUPPLY) == 0) %>%
  mutate(
    LCIP4_CRED_NT = paste0(grad_prefix(GRAD_STATUS), LCP4_CD, " - ", PSSM_CREDENTIAL),
    LCIP2_CRED_NT = paste0(grad_prefix(GRAD_STATUS), str_sub(LCP4_CD, 1, 2), " - ", PSSM_CREDENTIAL)
  ) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
           AGE_GROUP_ROLLUP, LCP4_CD, LCIP4_CRED_NT, LCIP2_CRED_NT) %>%
  summarise(COUNT = sum(WEIGHTED), .groups = "drop") %>%
  rename(LCIP4_CRED = LCIP4_CRED_NT, LCIP2_CRED = LCIP2_CRED_NT)

# Variant D: NLS=1,2,3 without TTRAIN (4D CIP, no TTRAIN)
q006b_nls123_no_tt <- weight_new_ls %>%
  filter(as.numeric(NEW_LABOUR_SUPPLY) %in% c(1, 2, 3)) %>%
  mutate(
    LCIP4_CRED_NT = paste0(grad_prefix(GRAD_STATUS), LCP4_CD, " - ", PSSM_CREDENTIAL),
    LCIP2_CRED_NT = paste0(grad_prefix(GRAD_STATUS), str_sub(LCP4_CD, 1, 2), " - ", PSSM_CREDENTIAL)
  ) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
           AGE_GROUP_ROLLUP, LCP4_CD, LCIP4_CRED_NT, LCIP2_CRED_NT) %>%
  summarise(COUNT = sum(WEIGHTED), UNWEIGHTED_COUNT = sum(COUNT), .groups = "drop") %>%
  rename(LCIP4_CRED = LCIP4_CRED_NT, LCIP2_CRED = LCIP2_CRED_NT)

# Variant E: NLS=0 with TTRAIN (2D CIP)
q006b_nls0_2d <- weight_new_ls %>%
  filter(as.numeric(NEW_LABOUR_SUPPLY) == 0) %>%
  mutate(LCP2_CD = str_sub(LCP4_CD, 1, 2)) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
           AGE_GROUP_ROLLUP, LCP2_CD, TTRAIN, LCIP2_CRED) %>%
  summarise(COUNT = sum(WEIGHTED), .groups = "drop") %>%
  rename(LCP2_CRED = LCIP2_CRED)

# Variant F: NLS=0 without TTRAIN (2D CIP)
q006b_nls0_2d_no_tt <- weight_new_ls %>%
  filter(as.numeric(NEW_LABOUR_SUPPLY) == 0) %>%
  mutate(
    LCP2_CD = str_sub(LCP4_CD, 1, 2),
    LCP2_CRED_NT = paste0(
      if_else(str_sub(PSSM_CRED, 1, 1) %in% c("1", "3"),
              paste0(str_sub(PSSM_CRED, 1, 1), " - "), ""),
      str_sub(LCP4_CD, 1, 2), " - ", PSSM_CREDENTIAL
    )
  ) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
           AGE_GROUP_ROLLUP, LCP2_CD, LCP2_CRED_NT) %>%
  summarise(COUNT = sum(WEIGHTED), .groups = "drop") %>%
  rename(LCP2_CRED = LCP2_CRED_NT)

# Variant G: NLS=1,2,3 with TTRAIN (2D CIP)
q006b_nls123_2d <- weight_new_ls %>%
  filter(as.numeric(NEW_LABOUR_SUPPLY) %in% c(1, 2, 3)) %>%
  mutate(LCP2_CD = str_sub(LCP4_CD, 1, 2)) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
           AGE_GROUP_ROLLUP, LCP2_CD, TTRAIN, LCIP2_CRED) %>%
  summarise(COUNT = sum(WEIGHTED), .groups = "drop") %>%
  rename(LCP2_CRED = LCIP2_CRED)

# Variant H: NLS=1,2,3 without TTRAIN (2D CIP)
q006b_nls123_2d_no_tt <- weight_new_ls %>%
  filter(as.numeric(NEW_LABOUR_SUPPLY) %in% c(1, 2, 3)) %>%
  mutate(
    LCP2_CD = str_sub(LCP4_CD, 1, 2),
    LCP2_CRED_NT = paste0(
      if_else(str_sub(PSSM_CRED, 1, 1) %in% c("1", "3"),
              paste0(str_sub(PSSM_CRED, 1, 1), " - "), ""),
      str_sub(LCP4_CD, 1, 2), " - ", PSSM_CREDENTIAL
    )
  ) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
           AGE_GROUP_ROLLUP, LCP2_CD, LCP2_CRED_NT) %>%
  summarise(COUNT = sum(WEIGHTED), .groups = "drop") %>%
  rename(LCP2_CRED = LCP2_CRED_NT)

# ---- Q006b Totals: Denominators for percentage computation ----
# WHY: Each variant needs a matching total (denominator). Totals include ALL
# NLS values (0,1,2,3) grouped at the same level but without region.

# Total with TTRAIN (4D CIP)
total_4d <- weight_new_ls %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP_ROLLUP,
           LCP4_CD, TTRAIN, LCIP4_CRED, LCIP2_CRED) %>%
  summarise(TOTAL = sum(WEIGHTED), .groups = "drop")

# Total without TTRAIN (4D CIP)
total_4d_no_tt <- weight_new_ls %>%
  mutate(
    LCIP4_CRED_NT = paste0(grad_prefix(GRAD_STATUS), LCP4_CD, " - ", PSSM_CREDENTIAL),
    LCIP2_CRED_NT = paste0(grad_prefix(GRAD_STATUS), str_sub(LCP4_CD, 1, 2), " - ", PSSM_CREDENTIAL)
  ) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP_ROLLUP,
           LCP4_CD, LCIP4_CRED_NT, LCIP2_CRED_NT) %>%
  summarise(TOTAL = sum(WEIGHTED), .groups = "drop") %>%
  rename(LCIP4_CRED = LCIP4_CRED_NT, LCIP2_CRED = LCIP2_CRED_NT)

# Total with TTRAIN (2D CIP)
total_2d <- weight_new_ls %>%
  mutate(LCP2_CD = str_sub(LCP4_CD, 1, 2)) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP_ROLLUP,
           LCP2_CD, TTRAIN, LCIP2_CRED) %>%
  summarise(TOTAL = sum(WEIGHTED), .groups = "drop") %>%
  rename(LCP2_CRED = LCIP2_CRED)

# Total without TTRAIN (2D CIP)
total_2d_no_tt <- weight_new_ls %>%
  mutate(
    LCP2_CD = str_sub(LCP4_CD, 1, 2),
    LCP2_CRED_NT = paste0(
      if_else(str_sub(PSSM_CRED, 1, 1) %in% c("1", "3"),
              paste0(str_sub(PSSM_CRED, 1, 1), " - "), ""),
      str_sub(LCP4_CD, 1, 2), " - ", PSSM_CREDENTIAL
    )
  ) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP_ROLLUP,
           LCP2_CD, LCP2_CRED_NT) %>%
  summarise(TOTAL = sum(WEIGHTED), .groups = "drop") %>%
  rename(LCP2_CRED = LCP2_CRED_NT)


# ---- Q007a: Compute percentages (count / total) ----
# WHY: The percentage represents the share of new labour supply within each
# program/age group. For NLS=1,2,3 it's count/total (direct percentage).
# For NLS=0 it's 1 - count/total (complement: percentage NOT in NLS=0).

# 4D with TTRAIN: NLS=1,2,3 percentage
q007a_4d <- total_4d %>%
  left_join(q006b_nls123 %>% select(PSSM_CREDENTIAL, PSSM_CRED,
             CURRENT_REGION_PSSM_CODE_ROLLOUP, AGE_GROUP_ROLLUP,
             LCIP4_CRED, COUNT),
    by = c("PSSM_CREDENTIAL", "PSSM_CRED", "AGE_GROUP_ROLLUP", "LCIP4_CRED")
  ) %>%
  filter(!is.na(CURRENT_REGION_PSSM_CODE_ROLLOUP)) %>%
  mutate(PERC = if_else(is.na(COUNT), 0, COUNT) / TOTAL)

# 4D with TTRAIN: NLS=0 percentage (1 - count/total, only where result=0)
# WHY: This captures cases where NLS=0 accounts for 100% (no new labour supply).
q007a_4d_nls0 <- total_4d %>%
  left_join(q006b_nls0 %>% select(PSSM_CREDENTIAL, PSSM_CRED,
             CURRENT_REGION_PSSM_CODE_ROLLOUP, AGE_GROUP_ROLLUP,
             LCIP4_CRED, COUNT),
    by = c("PSSM_CREDENTIAL", "PSSM_CRED", "AGE_GROUP_ROLLUP", "LCIP4_CRED")
  ) %>%
  filter(!is.na(COUNT), COUNT > 0) %>%
  mutate(PERC = 1 - (if_else(is.na(COUNT), 0, COUNT) / TOTAL)) %>%
  filter(PERC == 0)

# 4D no TTRAIN: NLS=1,2,3 percentage
q007a_4d_no_tt <- total_4d_no_tt %>%
  left_join(q006b_nls123_no_tt %>% select(PSSM_CREDENTIAL, PSSM_CRED,
             CURRENT_REGION_PSSM_CODE_ROLLOUP, AGE_GROUP_ROLLUP,
             LCIP4_CRED, COUNT),
    by = c("PSSM_CREDENTIAL", "PSSM_CRED", "AGE_GROUP_ROLLUP", "LCIP4_CRED")
  ) %>%
  filter(!is.na(CURRENT_REGION_PSSM_CODE_ROLLOUP)) %>%
  mutate(PERC = if_else(is.na(COUNT), 0, COUNT) / TOTAL)

# 4D no TTRAIN: NLS=0 percentage
q007a_4d_nls0_no_tt <- total_4d_no_tt %>%
  left_join(q006b_nls0_no_tt %>% select(PSSM_CREDENTIAL, PSSM_CRED,
             CURRENT_REGION_PSSM_CODE_ROLLOUP, AGE_GROUP_ROLLUP,
             LCIP4_CRED, COUNT),
    by = c("PSSM_CREDENTIAL", "PSSM_CRED", "AGE_GROUP_ROLLUP", "LCIP4_CRED")
  ) %>%
  filter(!is.na(COUNT), COUNT > 0) %>%
  mutate(PERC = 1 - (if_else(is.na(COUNT), 0, COUNT) / TOTAL)) %>%
  filter(PERC == 0)

# 2D with TTRAIN: NLS=1,2,3 percentage
q007a_2d <- total_2d %>%
  left_join(q006b_nls123_2d %>% select(PSSM_CREDENTIAL, PSSM_CRED,
             CURRENT_REGION_PSSM_CODE_ROLLOUP, AGE_GROUP_ROLLUP,
             LCP2_CRED, COUNT),
    by = c("PSSM_CREDENTIAL", "PSSM_CRED", "AGE_GROUP_ROLLUP", "LCP2_CRED")
  ) %>%
  filter(!is.na(CURRENT_REGION_PSSM_CODE_ROLLOUP)) %>%
  mutate(PERC = if_else(is.na(COUNT), 0, COUNT) / TOTAL)

# 2D no TTRAIN: NLS=1,2,3 percentage
q007a_2d_no_tt <- total_2d_no_tt %>%
  left_join(q006b_nls123_2d_no_tt %>% select(PSSM_CREDENTIAL, PSSM_CRED,
             CURRENT_REGION_PSSM_CODE_ROLLOUP, AGE_GROUP_ROLLUP,
             LCP2_CRED, COUNT),
    by = c("PSSM_CREDENTIAL", "PSSM_CRED", "AGE_GROUP_ROLLUP", "LCP2_CRED")
  ) %>%
  filter(!is.na(CURRENT_REGION_PSSM_CODE_ROLLOUP)) %>%
  mutate(PERC = if_else(is.na(COUNT), 0, COUNT) / TOTAL)

# 2D with TTRAIN: NLS=0 percentage
q007a_2d_nls0 <- total_2d %>%
  left_join(q006b_nls0_2d %>% select(PSSM_CREDENTIAL, PSSM_CRED,
             CURRENT_REGION_PSSM_CODE_ROLLOUP, AGE_GROUP_ROLLUP,
             LCP2_CRED, COUNT),
    by = c("PSSM_CREDENTIAL", "PSSM_CRED", "AGE_GROUP_ROLLUP", "LCP2_CRED")
  ) %>%
  filter(!is.na(COUNT), COUNT > 0) %>%
  mutate(PERC = 1 - (if_else(is.na(COUNT), 0, COUNT) / TOTAL)) %>%
  filter(PERC == 0)

# 2D no TTRAIN: NLS=0 percentage
q007a_2d_nls0_no_tt <- total_2d_no_tt %>%
  left_join(q006b_nls0_2d_no_tt %>% select(PSSM_CREDENTIAL, PSSM_CRED,
             CURRENT_REGION_PSSM_CODE_ROLLOUP, AGE_GROUP_ROLLUP,
             LCP2_CRED, COUNT),
    by = c("PSSM_CREDENTIAL", "PSSM_CRED", "AGE_GROUP_ROLLUP", "LCP2_CRED")
  ) %>%
  filter(!is.na(COUNT), COUNT > 0) %>%
  mutate(PERC = 1 - (if_else(is.na(COUNT), 0, COUNT) / TOTAL)) %>%
  filter(PERC == 0)


# ******************************************************************************
# Part 6: Build and write final distribution tables
# ******************************************************************************
# WHY: Each of the 4 output tables gets rows from both NLS=1,2,3 (direct %)
# and NLS=0 (complement %). In the original, this was DELETE + INSERT INTO.
# Here we filter existing data, bind new rows, and overwrite.

# Standardize column names for output.
# The 4D tables have: SURVEY, PSSM_CREDENTIAL, PSSM_CRED,
#   CURRENT_REGION_PSSM_CODE_ROLLOUP, AGE_GROUP_ROLLUP, LCP4_CD,
#   TTRAIN (or not for No_TT), LCIP4_CRED, LCIP2_CRED, COUNT, TOTAL, PERC
# The 2D tables have similar but use LCP2_CD/LCP2_CRED instead of LCP4/LCIP4.

# ---- Labour_Supply_Distribution (4D, with TTRAIN) ----
ls_4d_nls123 <- q007a_4d %>%
  mutate(SURVEY = "Student Outcomes") %>%
  select(SURVEY, PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
         AGE_GROUP_ROLLUP, LCP4_CD, TTRAIN, LCIP4_CRED, LCIP2_CRED,
         COUNT, TOTAL, NEW_LABOUR_SUPPLY = PERC)

ls_4d_nls0 <- q007a_4d_nls0 %>%
  mutate(SURVEY = "Student Outcomes") %>%
  select(SURVEY, PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
         AGE_GROUP_ROLLUP, LCP4_CD, TTRAIN, LCIP4_CRED, LCIP2_CRED,
         COUNT, TOTAL, NEW_LABOUR_SUPPLY = PERC)

ls_new <- bind_rows(ls_4d_nls123, ls_4d_nls0)

# Read existing data, remove old "Student Outcomes" rows, append new
if (dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Labour_Supply_Distribution"')))) {
  ls_existing <- sch_tbl("Labour_Supply_Distribution") %>%
    collect() |> rename_with(toupper) %>%
    filter(SURVEY != "Student Outcomes")
  ls_final <- bind_rows(ls_existing, ls_new)
} else {
  ls_final <- ls_new
}

dbWriteTable(decimal_con, SQL(glue::glue('"{my_schema}"."Labour_Supply_Distribution"')),
             ls_final, overwrite = TRUE)


# ---- Labour_Supply_Distribution_No_TT (4D, no TTRAIN) ----
ls_4d_no_tt_nls123 <- q007a_4d_no_tt %>%
  mutate(SURVEY = "Student Outcomes") %>%
  select(SURVEY, PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
         AGE_GROUP_ROLLUP, LCP4_CD, LCIP4_CRED, LCIP2_CRED,
         COUNT, TOTAL, NEW_LABOUR_SUPPLY = PERC)

ls_4d_no_tt_nls0 <- q007a_4d_nls0_no_tt %>%
  mutate(SURVEY = "Student Outcomes") %>%
  select(SURVEY, PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
         AGE_GROUP_ROLLUP, LCP4_CD, LCIP4_CRED, LCIP2_CRED,
         COUNT, TOTAL, NEW_LABOUR_SUPPLY = PERC)

ls_no_tt_new <- bind_rows(ls_4d_no_tt_nls123, ls_4d_no_tt_nls0)

if (dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Labour_Supply_Distribution_No_TT"')))) {
  ls_no_tt_existing <- sch_tbl("Labour_Supply_Distribution_No_TT") %>%
    collect() |> rename_with(toupper) %>%
    filter(SURVEY != "Student Outcomes")
  ls_no_tt_final <- bind_rows(ls_no_tt_existing, ls_no_tt_new)
} else {
  ls_no_tt_final <- ls_no_tt_new
}

dbWriteTable(decimal_con, SQL(glue::glue('"{my_schema}"."Labour_Supply_Distribution_No_TT"')),
             ls_no_tt_final, overwrite = TRUE)


# ---- Labour_Supply_Distribution_LCP2 (2D, with TTRAIN) ----
ls_2d_nls123 <- q007a_2d %>%
  mutate(SURVEY = "Student Outcomes") %>%
  select(SURVEY, PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
         AGE_GROUP_ROLLUP, LCP2_CD, TTRAIN, LCP2_CRED,
         COUNT, TOTAL, NEW_LABOUR_SUPPLY = PERC)

ls_2d_nls0 <- q007a_2d_nls0 %>%
  mutate(SURVEY = "Student Outcomes") %>%
  select(SURVEY, PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
         AGE_GROUP_ROLLUP, LCP2_CD, TTRAIN, LCP2_CRED,
         COUNT, TOTAL, NEW_LABOUR_SUPPLY = PERC)

ls_2d_new <- bind_rows(ls_2d_nls123, ls_2d_nls0)

if (dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Labour_Supply_Distribution_LCP2"')))) {
  ls_2d_existing <- sch_tbl("Labour_Supply_Distribution_LCP2") %>%
    collect() |> rename_with(toupper) %>%
    filter(SURVEY != "Student Outcomes")
  ls_2d_final <- bind_rows(ls_2d_existing, ls_2d_new)
} else {
  ls_2d_final <- ls_2d_new
}

dbWriteTable(decimal_con, SQL(glue::glue('"{my_schema}"."Labour_Supply_Distribution_LCP2"')),
             ls_2d_final, overwrite = TRUE)


# ---- Labour_Supply_Distribution_LCP2_No_TT (2D, no TTRAIN) ----
ls_2d_no_tt_nls123 <- q007a_2d_no_tt %>%
  mutate(SURVEY = "Student Outcomes") %>%
  select(SURVEY, PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
         AGE_GROUP_ROLLUP, LCP2_CD, LCP2_CRED,
         COUNT, TOTAL, NEW_LABOUR_SUPPLY = PERC)

ls_2d_no_tt_nls0 <- q007a_2d_nls0_no_tt %>%
  mutate(SURVEY = "Student Outcomes") %>%
  select(SURVEY, PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
         AGE_GROUP_ROLLUP, LCP2_CD, LCP2_CRED,
         COUNT, TOTAL, NEW_LABOUR_SUPPLY = PERC)

ls_2d_no_tt_new <- bind_rows(ls_2d_no_tt_nls123, ls_2d_no_tt_nls0)

if (dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Labour_Supply_Distribution_LCP2_No_TT"')))) {
  ls_2d_no_tt_existing <- sch_tbl("Labour_Supply_Distribution_LCP2_No_TT") %>%
    collect() |> rename_with(toupper) %>%
    filter(SURVEY != "Student Outcomes")
  ls_2d_no_tt_final <- bind_rows(ls_2d_no_tt_existing, ls_2d_no_tt_new)
} else {
  ls_2d_no_tt_final <- ls_2d_no_tt_new
}

dbWriteTable(decimal_con, SQL(glue::glue('"{my_schema}"."Labour_Supply_Distribution_LCP2_No_TT"')),
             ls_2d_no_tt_final, overwrite = TRUE)


# ******************************************************************************
# Part 7: Append StatCan census data
# ******************************************************************************
# WHY: The Labour_Supply_Distribution table also includes census data from
# Statistics Canada. We remove any existing census rows and re-append.
# KEPT AS SQL: INSERT INTO...SELECT (cross-schema append with column mapping)

# Remove existing StatCan rows from Labour_Supply_Distribution
ls_final <- ls_final %>%
  filter(SURVEY != "2021 Census PSSM 2023-2024")

# Append StatCan data
stat_can <- sch_tbl("Labour_Supply_Distribution_Stat_Can") %>%
  collect() |> rename_with(toupper) %>%
  mutate(SURVEY = "2021 Census PSSM 2023-2024") %>%
  select(SURVEY, PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD, LCIP4_CRED,
         CURRENT_REGION_PSSM_CODE_ROLLOUP, AGE_GROUP_ROLLUP,
         COUNT, TOTAL, NEW_LABOUR_SUPPLY)

ls_final <- bind_rows(ls_final, stat_can)

dbWriteTable(decimal_con, SQL(glue::glue('"{my_schema}"."Labour_Supply_Distribution"')),
             ls_final, overwrite = TRUE)


# ******************************************************************************
# Part 8: Write Weight_NLS back to T_Cohorts_Recoded and tmp_tbl_weights_nls
# ******************************************************************************
# WHY: The updated cohorts table (with Weight_NLS) needs to be written back
# so downstream scripts can use it. The weight lookup table is also kept.

# Write tmp_tbl_weights_nls for downstream reference
dbWriteTable(decimal_con, SQL(glue::glue('"{my_schema}"."tmp_tbl_Weights_NLS"')),
             tmp_tbl_weights_nls, overwrite = TRUE)


# ******************************************************************************
# Part 9: Cleanup and verification
# ******************************************************************************

# Verify tables exist
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Labour_Supply_Distribution"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Labour_Supply_Distribution_No_TT"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Labour_Supply_Distribution_LCP2"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Labour_Supply_Distribution_LCP2_No_TT"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."tmp_tbl_Weights_NLS"')))

# Final weight check
cohorts %>%
  count(SURVEY, SURVEY_YEAR, WEIGHT)

# ---- Disconnect ----
dbDisconnect(decimal_con)



# ==============================================================================
# FILE: 02b-3-pssm-cohorts-occupation-distributions_dplyr.R
# ==============================================================================


# Occupation Distributions — dplyr Translation
# Original: R/02b-3-pssm-cohorts-occupation-distributions.R
#
# Pipeline context:
#   Processes cohort data to derive occupation distributions. Runs after
#   02b-2-pssm-cohorts-new-labour-supply (which computed Weight_NLS).
#
#   Creates occupation weights (Weight_OCC), then builds 6 distribution tables:
#     - Occupation_Distributions (4D, with TTRAIN, by region)
#     - Occupation_Distributions_No_TT (4D, no TTRAIN, by region)
#     - Occupation_Distributions_LCP2 (2D, with TTRAIN, by region)
#     - Occupation_Distributions_LCP2_No_TT (2D, no TTRAIN, by region)
#     - Occupation_Distributions_LCP2_BC (2D, with TTRAIN, BC-wide)
#     - Occupation_Distributions_LCP2_BC_No_TT (2D, no TTRAIN, BC-wide)
#   Plus PDEG Law modifications and StatCan census append.
#
# Input tables:
#   - T_Cohorts_Recoded — unified cohort table (from 02b-1)
#   - T_Current_Region_PSSM_Codes / T_Current_Region_PSSM_Rollup_Codes
#   - T_Current_Region_PSSM_Rollup_Codes_BC — BC-wide rollup
#   - tmp_tbl_Weights_NLS — NLS weights (from 02b-2)
#   - T_NOC_Broad_Categories — NOC validation lookup
#   - Occupation_Distributions_Stat_Can — census data
#
# Output tables:
#   - 6 Occupation_Distributions variants
#   - tmp_tbl_Weights_OCC — weight lookup for downstream use

library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(dbplyr)

# ---- Configure LAN and file paths ----
db_config <- config::get("decimal")
lan <- config::get("lan")
my_schema <- config::get("myschema")

decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")

# Helper: reference a table in the user's schema
sch_tbl <- function(name) {
  tbl(decimal_con, dbplyr::in_schema(my_schema, name))
}

grad_prefix <- function(status) {
  if_else(is.na(status), "", paste0(status, " - "))
}

# ---- Check required tables ----
required_tables <- c(
  "t_cohorts_recoded",
  "t_current_region_pssm_codes",
  "t_current_region_pssm_rollup_codes",
  "tmp_tbl_Weights_NLS",
  "T_NOC_Broad_Categories",
  "Occupation_Distributions_Stat_Can"
)

for (table_name in required_tables) {
  full_table_name <- SQL(glue::glue('"{my_schema}"."{table_name}"'))
  assertthat::assert_that(
    dbExistsTable(decimal_con, full_table_name),
    msg = paste("Error:", table_name, "does not exist in schema", my_schema)
  )
}


# ******************************************************************************
# Part 0: Pull source data into R
# ******************************************************************************
cohorts <- sch_tbl("T_Cohorts_Recoded") %>%
  collect() |> rename_with(toupper)

region_codes <- sch_tbl("T_Current_Region_PSSM_Codes") %>%
  collect() |> rename_with(toupper)

region_rollup <- sch_tbl("T_Current_Region_PSSM_Rollup_Codes") %>%
  collect() |> rename_with(toupper)

# BC-wide rollup (aggregates regions to province-level)
region_rollup_bc <- sch_tbl("T_Current_Region_PSSM_Rollup_Codes_BC") %>%
  collect() |> rename_with(toupper)

tmp_weights_nls <- sch_tbl("tmp_tbl_Weights_NLS") %>%
  collect() |> rename_with(toupper)

# Join cohorts with region rollup
cohorts_with_region <- cohorts %>%
  inner_join(region_codes %>% select(CURRENT_REGION_PSSM_CODE, CURRENT_REGION_PSSM_CODE_ROLLOUP),
             by = "CURRENT_REGION_PSSM_CODE") %>%
  inner_join(region_rollup %>% select(CURRENT_REGION_PSSM_CODE_ROLLOUP),
             by = "CURRENT_REGION_PSSM_CODE_ROLLOUP")


# ******************************************************************************
# Part 1: Weight_OCC creation (Z01 → Z08)
# ******************************************************************************
# WHY: The OCC weight adjusts for non-response in occupation reporting.
# It's computed similarly to Weight_NLS but focuses on respondents with valid
# NOC codes. The pipeline: base → respondents → weight → total → adj_fac → Weight_OCC.

# ---- Z01: Base_OCC records ----
# WHY: Records with NLS=1,2,3 and valid region, for building the OCC weight base.
base_occ <- cohorts %>%
  filter(as.numeric(WEIGHT) > 0,
         CURRENT_REGION_PSSM_CODE != -1,
         !is.na(AGE_GROUP_ROLLUP),
         as.numeric(NEW_LABOUR_SUPPLY) %in% c(1, 2, 3),
         GRAD_STATUS %in% c("1", "3")) %>%
  select(STQU_ID, SURVEY, INST_CD, AGE_GROUP_ROLLUP, TTRAIN, LCIP4_CRED,
         SURVEY_YEAR, GRAD_STATUS, NEW_LABOUR_SUPPLY, WEIGHT_NLS)

# ---- Z02a: Base with region rollup, grouped by year ----
# WHY: Counts records and computes weighted base by survey/year/region/age/cred.
z02a_base <- cohorts_with_region %>%
  filter(as.numeric(NEW_LABOUR_SUPPLY) %in% c(1, 2, 3),
         RESPONDENT == "1",
         as.numeric(WEIGHT) > 0,
         CURRENT_REGION_PSSM_CODE_ROLLOUP != 9999,
         !is.na(AGE_GROUP_ROLLUP),
         GRAD_STATUS %in% c("1", "3")) %>%
  mutate(WEIGHT_NLS = as.numeric(WEIGHT_NLS)) %>%
  group_by(SURVEY, CURRENT_REGION_PSSM_CODE_ROLLOUP, SURVEY_YEAR, INST_CD,
           AGE_GROUP_ROLLUP, GRAD_STATUS, TTRAIN, LCIP4_CRED, WEIGHT_NLS) %>%
  summarise(
    COUNT = n(),
    BASE = n() * WEIGHT_NLS,
    .groups = "drop"
  )

# ---- Z02b: Respondents with valid NOC (not 99999) ----
z02b_valid_noc <- cohorts_with_region %>%
  filter(!is.na(NOC_CD), NOC_CD != "99999",
         as.numeric(NEW_LABOUR_SUPPLY) %in% c(1, 3),
         as.numeric(WEIGHT) > 0,
         !is.na(AGE_GROUP_ROLLUP),
         GRAD_STATUS %in% c("1", "3")) %>%
  group_by(SURVEY, CURRENT_REGION_PSSM_CODE_ROLLOUP, SURVEY_YEAR, INST_CD,
           AGE_GROUP_ROLLUP, GRAD_STATUS, TTRAIN, LCIP4_CRED) %>%
  summarise(
    RESPONDENTS = sum(if_else(RESPONDENT == "1" & CURRENT_REGION_PSSM_CODE != -1, 1, 0)),
    .groups = "drop"
  )

# ---- Z02b: Respondents with NOC=99999 ----
z02b_noc_99999 <- cohorts_with_region %>%
  filter(NOC_CD == "99999",
         as.numeric(NEW_LABOUR_SUPPLY) %in% c(1, 3),
         as.numeric(WEIGHT) > 0,
         !is.na(AGE_GROUP_ROLLUP),
         GRAD_STATUS %in% c("1", "3")) %>%
  group_by(SURVEY, CURRENT_REGION_PSSM_CODE_ROLLOUP, SURVEY_YEAR, INST_CD,
           AGE_GROUP_ROLLUP, GRAD_STATUS, TTRAIN, LCIP4_CRED) %>%
  summarise(
    RESPONDENTS = sum(if_else(RESPONDENT == "1" & CURRENT_REGION_PSSM_CODE != -1, 1, 0)),
    .groups = "drop"
  )

# ---- Z02b: NOC 99999 where 100% of records have that NOC ----
# WHY: If ALL records in a cell have NOC=99999 (respondents/count = 1),
# those records should still get a weight (using the valid-NOC weight).
z02b_noc_99999_100 <- z02a_base %>%
  inner_join(z02b_noc_99999,
             by = c("SURVEY", "CURRENT_REGION_PSSM_CODE_ROLLOUP", "SURVEY_YEAR",
                    "INST_CD", "AGE_GROUP_ROLLUP", "GRAD_STATUS", "TTRAIN", "LCIP4_CRED")) %>%
  filter(RESPONDENTS / COUNT == 1)

# ---- Z02b: Union of respondents ----
z02b_union <- bind_rows(
  z02b_valid_noc,
  z02b_noc_99999_100 %>% select(SURVEY, CURRENT_REGION_PSSM_CODE_ROLLOUP, SURVEY_YEAR,
                                 INST_CD, AGE_GROUP_ROLLUP, GRAD_STATUS, TTRAIN, LCIP4_CRED, RESPONDENTS)
)

# ---- Z02c: Weight computation ----
# WHY: Weight_NLS_Base = Base/Respondents adjusts for non-response.
z02c_weight <- z02a_base %>%
  left_join(z02b_union,
            by = c("SURVEY", "CURRENT_REGION_PSSM_CODE_ROLLOUP", "SURVEY_YEAR",
                   "INST_CD", "AGE_GROUP_ROLLUP", "GRAD_STATUS", "TTRAIN", "LCIP4_CRED")) %>%
  mutate(
    RESPONDENTS = coalesce(RESPONDENTS, 0),
    WEIGHT_NLS_BASE = if_else(RESPONDENTS == 0, 1, as.numeric(COUNT) / as.numeric(RESPONDENTS)),
    WEIGHTED = as.numeric(RESPONDENTS) * if_else(RESPONDENTS == 0, 1, as.numeric(COUNT) / as.numeric(RESPONDENTS))
  )

# ---- Z03: Weight total ----
z03_total <- z02c_weight %>%
  group_by(SURVEY, CURRENT_REGION_PSSM_CODE_ROLLOUP, INST_CD, AGE_GROUP_ROLLUP,
           GRAD_STATUS, TTRAIN, LCIP4_CRED) %>%
  summarise(BASE = sum(COUNT), WEIGHTED = sum(WEIGHTED), .groups = "drop")

# ---- Z04: Weight adjustment factor ----
z04_adj_fac <- z03_total %>%
  mutate(WEIGHT_ADJ_FAC = if_else(WEIGHTED == 0, 0, as.numeric(BASE) / as.numeric(WEIGHTED)))

# ---- Z05: Final Weight_OCC ----
# WHY: Weight_OCC = Weight_NLS_Base * Weight_Adj_Fac. Joins with region codes
# to get the individual region code back from the rollup.
tmp_tbl_weights_occ <- z02c_weight %>%
  inner_join(z04_adj_fac %>% select(SURVEY, CURRENT_REGION_PSSM_CODE_ROLLOUP, INST_CD,
                                     AGE_GROUP_ROLLUP, GRAD_STATUS, TTRAIN, LCIP4_CRED, WEIGHT_ADJ_FAC),
             by = c("SURVEY", "CURRENT_REGION_PSSM_CODE_ROLLOUP", "INST_CD",
                    "AGE_GROUP_ROLLUP", "GRAD_STATUS", "TTRAIN", "LCIP4_CRED")) %>%
  mutate(WEIGHT_OCC = WEIGHT_NLS_BASE * WEIGHT_ADJ_FAC) %>%
  inner_join(
    region_rollup %>% select(CURRENT_REGION_PSSM_CODE_ROLLOUP) %>%
      inner_join(region_codes %>% select(CURRENT_REGION_PSSM_CODE, CURRENT_REGION_PSSM_CODE_ROLLOUP),
                 by = "CURRENT_REGION_PSSM_CODE_ROLLOUP"),
    by = "CURRENT_REGION_PSSM_CODE_ROLLOUP"
  )


# ******************************************************************************
# Part 2: Apply Weight_OCC to T_Cohorts_Recoded
# ******************************************************************************
# WHY: Update Weight_OCC on cohort records. Two passes: valid NOC and NOC=99999 100%.

# KEPT AS SQL: ALTER TABLE for Weight_OCC column
dbExecute(decimal_con, "ALTER TABLE T_Cohorts_Recoded ALTER COLUMN Weight_OCC FLOAT NULL")
dbExecute(decimal_con, "ALTER TABLE T_Cohorts_Recoded ALTER COLUMN Weight_Age FLOAT NULL")

# Null out Weight_OCC in R memory
cohorts <- cohorts %>%
  mutate(WEIGHT_OCC = NA_real_)

# Update for records with valid NOC (not 99999, not null) and valid region
weight_occ_lookup <- tmp_tbl_weights_occ %>%
  select(SURVEY, SURVEY_YEAR, INST_CD, AGE_GROUP_ROLLUP, GRAD_STATUS,
         TTRAIN, LCIP4_CRED, CURRENT_REGION_PSSM_CODE, WEIGHT_OCC)

base_occ_ids <- base_occ %>% distinct(STQU_ID)

cohorts_valid_noc <- cohorts %>%
  semi_join(base_occ_ids, by = "STQU_ID") %>%
  filter(CURRENT_REGION_PSSM_CODE != -1,
         !is.na(NOC_CD), NOC_CD != "99999") %>%
  inner_join(weight_occ_lookup,
             by = c("SURVEY", "SURVEY_YEAR", "INST_CD", "AGE_GROUP_ROLLUP",
                    "GRAD_STATUS", "TTRAIN", "LCIP4_CRED", "CURRENT_REGION_PSSM_CODE")) %>%
  select(STQU_ID, WEIGHT_OCC)

# Update for NOC=99999 records that are in the 100% NOC-99999 cells
noc_99999_ids <- z02b_noc_99999_100 %>%
  select(SURVEY, CURRENT_REGION_PSSM_CODE_ROLLOUP, SURVEY_YEAR, INST_CD,
         AGE_GROUP_ROLLUP, GRAD_STATUS, TTRAIN, LCIP4_CRED) %>%
  inner_join(tmp_tbl_weights_occ %>% select(SURVEY, CURRENT_REGION_PSSM_CODE_ROLLOUP,
                                             SURVEY_YEAR, INST_CD, AGE_GROUP_ROLLUP,
                                             GRAD_STATUS, TTRAIN, LCIP4_CRED,
                                             CURRENT_REGION_PSSM_CODE, WEIGHT_OCC),
             by = c("SURVEY", "CURRENT_REGION_PSSM_CODE_ROLLOUP", "SURVEY_YEAR",
                    "INST_CD", "AGE_GROUP_ROLLUP", "GRAD_STATUS", "TTRAIN", "LCIP4_CRED"))

cohorts_noc_99999 <- cohorts %>%
  semi_join(base_occ_ids, by = "STQU_ID") %>%
  filter(CURRENT_REGION_PSSM_CODE != -1, !is.na(NOC_CD)) %>%
  inner_join(noc_99999_ids %>% select(CURRENT_REGION_PSSM_CODE, SURVEY_YEAR,
                                       INST_CD, AGE_GROUP_ROLLUP, GRAD_STATUS,
                                       TTRAIN, LCIP4_CRED, WEIGHT_OCC),
             by = c("CURRENT_REGION_PSSM_CODE", "SURVEY_YEAR", "INST_CD",
                    "AGE_GROUP_ROLLUP", "GRAD_STATUS", "TTRAIN", "LCIP4_CRED")) %>%
  select(STQU_ID, WEIGHT_OCC)

# Apply both updates
all_weight_occ_updates <- bind_rows(cohorts_valid_noc, cohorts_noc_99999)

cohorts <- cohorts %>%
  left_join(all_weight_occ_updates %>% rename(WEIGHT_OCC_NEW = WEIGHT_OCC),
            by = "STQU_ID") %>%
  mutate(WEIGHT_OCC = if_else(!is.na(WEIGHT_OCC_NEW), WEIGHT_OCC_NEW, WEIGHT_OCC)) %>%
  select(-WEIGHT_OCC_NEW)

# Write Weight_OCC lookup table for downstream
dbWriteTable(decimal_con, SQL(glue::glue('"{my_schema}"."tmp_tbl_Weights_OCC"')),
             tmp_tbl_weights_occ, overwrite = TRUE)


# ******************************************************************************
# Part 3: Weighted occupation counts (Q009)
# ******************************************************************************
# WHY: Count respondents by occupation code, weighted by Weight_OCC. The NOC code
# XXXXX is recoded to 99999. Multiple variants are needed for the 6 output tables.

# Base weighted occupation counts
q009_weight_occs <- cohorts_with_region %>%
  filter(as.numeric(NEW_LABOUR_SUPPLY) %in% c(1, 3),
         as.numeric(WEIGHT) > 0) %>%
  mutate(
    NOC_CD = if_else(NOC_CD == "XXXXX", "99999", NOC_CD),
    WEIGHT_OCC = as.numeric(WEIGHT_OCC)
  ) %>%
  filter(CURRENT_REGION_PSSM_CODE_ROLLOUP != 9999,
         !is.na(AGE_GROUP_ROLLUP),
         GRAD_STATUS %in% c("1", "3"),
         !is.na(NOC_CD),
         !is.na(WEIGHT_OCC)) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
           SURVEY_YEAR, INST_CD, AGE_GROUP_ROLLUP, GRAD_STATUS, LCP4_CD,
           TTRAIN, LCIP4_CRED, LCIP2_CRED, NOC_CD, WEIGHT_OCC) %>%
  summarise(COUNT = n(), .groups = "drop") %>%
  mutate(WEIGHTED = COUNT * WEIGHT_OCC)


# ******************************************************************************
# Part 4: Build distribution variants (Q009b totals + Q010 percentages)
# ******************************************************************************
# WHY: Same pattern as Labour_Supply_Distribution: compute weighted counts and
# totals for different grouping dimensions, then compute percentages.

# Helper: compute percentage = count / total
compute_dist <- function(counts, totals, join_cols) {
  totals %>%
    left_join(counts %>% select(all_of(c(join_cols, "NOC_CD", "COUNT"))), by = join_cols) %>%
    mutate(PERC_DIST = if_else(is.na(COUNT), 0, COUNT) / TOTAL)
}

# ---- Variant 1: 4D with TTRAIN (by region) ----
q009b_counts_4d <- q009_weight_occs %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
           AGE_GROUP_ROLLUP, LCP4_CD, TTRAIN, LCIP4_CRED, LCIP2_CRED, NOC_CD) %>%
  summarise(COUNT = sum(WEIGHTED), .groups = "drop")

q009b_totals_4d <- q009_weight_occs %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
           AGE_GROUP_ROLLUP, LCP4_CD, TTRAIN, LCIP4_CRED, LCIP2_CRED) %>%
  summarise(TOTAL = sum(WEIGHTED), .groups = "drop")

q010_4d <- compute_dist(q009b_counts_4d, q009b_totals_4d,
                         c("PSSM_CREDENTIAL", "PSSM_CRED", "CURRENT_REGION_PSSM_CODE_ROLLOUP",
                           "AGE_GROUP_ROLLUP", "LCP4_CD", "TTRAIN", "LCIP4_CRED", "LCIP2_CRED"))

# ---- Variant 2: 4D no TTRAIN (by region) ----
q009b_counts_4d_no_tt <- q009_weight_occs %>%
  mutate(
    LCIP4_CRED_NT = paste0(grad_prefix(GRAD_STATUS), LCP4_CD, " - ", PSSM_CREDENTIAL),
    LCIP2_CRED_NT = paste0(grad_prefix(GRAD_STATUS), str_sub(LCP4_CD, 1, 2), " - ", PSSM_CREDENTIAL)
  ) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
           AGE_GROUP_ROLLUP, LCP4_CD, LCIP4_CRED_NT, LCIP2_CRED_NT, NOC_CD) %>%
  summarise(COUNT = sum(WEIGHTED), .groups = "drop") %>%
  rename(LCIP4_CRED = LCIP4_CRED_NT, LCIP2_CRED = LCIP2_CRED_NT)

q009b_totals_4d_no_tt <- q009_weight_occs %>%
  mutate(
    LCIP4_CRED_NT = paste0(grad_prefix(GRAD_STATUS), LCP4_CD, " - ", PSSM_CREDENTIAL),
    LCIP2_CRED_NT = paste0(grad_prefix(GRAD_STATUS), str_sub(LCP4_CD, 1, 2), " - ", PSSM_CREDENTIAL)
  ) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
           AGE_GROUP_ROLLUP, LCP4_CD, LCIP4_CRED_NT, LCIP2_CRED_NT) %>%
  summarise(TOTAL = sum(WEIGHTED), .groups = "drop") %>%
  rename(LCIP4_CRED = LCIP4_CRED_NT, LCIP2_CRED = LCIP2_CRED_NT)

q010_4d_no_tt <- compute_dist(q009b_counts_4d_no_tt, q009b_totals_4d_no_tt,
                               c("PSSM_CREDENTIAL", "PSSM_CRED", "CURRENT_REGION_PSSM_CODE_ROLLOUP",
                                 "AGE_GROUP_ROLLUP", "LCP4_CD", "LCIP4_CRED", "LCIP2_CRED"))

# ---- Variant 3: 2D with TTRAIN (by region) ----
q009b_counts_2d <- q009_weight_occs %>%
  mutate(LCP2_CD = str_sub(LCP4_CD, 1, 2)) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
           AGE_GROUP_ROLLUP, LCP2_CD, TTRAIN, LCIP2_CRED, NOC_CD) %>%
  summarise(COUNT = sum(WEIGHTED), .groups = "drop")

q009b_totals_2d <- q009_weight_occs %>%
  mutate(LCP2_CD = str_sub(LCP4_CD, 1, 2)) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
           AGE_GROUP_ROLLUP, LCP2_CD, TTRAIN, LCIP2_CRED) %>%
  summarise(TOTAL = sum(WEIGHTED), .groups = "drop")

q010_2d <- compute_dist(q009b_counts_2d, q009b_totals_2d,
                         c("PSSM_CREDENTIAL", "PSSM_CRED", "CURRENT_REGION_PSSM_CODE_ROLLOUP",
                           "AGE_GROUP_ROLLUP", "LCP2_CD", "TTRAIN", "LCIP2_CRED"))

# ---- Variant 4: 2D no TTRAIN (by region) ----
q009b_counts_2d_no_tt <- q009_weight_occs %>%
  mutate(
    LCP2_CD = str_sub(LCP4_CD, 1, 2),
    LCP2_CRED_NT = paste0(
      if_else(str_sub(PSSM_CRED, 1, 1) %in% c("1", "3"),
              paste0(str_sub(PSSM_CRED, 1, 1), " - "), ""),
      str_sub(LCP4_CD, 1, 2), " - ", PSSM_CREDENTIAL
    )
  ) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
           AGE_GROUP_ROLLUP, LCP2_CD, LCP2_CRED_NT, NOC_CD) %>%
  summarise(COUNT = sum(WEIGHTED), .groups = "drop") %>%
  rename(LCIP2_CRED = LCP2_CRED_NT)

q009b_totals_2d_no_tt <- q009_weight_occs %>%
  mutate(
    LCP2_CD = str_sub(LCP4_CD, 1, 2),
    LCP2_CRED_NT = paste0(
      if_else(str_sub(PSSM_CRED, 1, 1) %in% c("1", "3"),
              paste0(str_sub(PSSM_CRED, 1, 1), " - "), ""),
      str_sub(LCP4_CD, 1, 2), " - ", PSSM_CREDENTIAL
    )
  ) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
           AGE_GROUP_ROLLUP, LCP2_CD, LCP2_CRED_NT) %>%
  summarise(TOTAL = sum(WEIGHTED), .groups = "drop") %>%
  rename(LCIP2_CRED = LCP2_CRED_NT)

q010_2d_no_tt <- compute_dist(q009b_counts_2d_no_tt, q009b_totals_2d_no_tt,
                               c("PSSM_CREDENTIAL", "PSSM_CRED", "CURRENT_REGION_PSSM_CODE_ROLLOUP",
                                 "AGE_GROUP_ROLLUP", "LCP2_CD", "LCIP2_CRED"))

# ---- Variant 5: 2D with TTRAIN (BC-wide) ----
# WHY: BC-wide variants aggregate across all regions within BC.
q009b_counts_2d_bc <- q009_weight_occs %>%
  inner_join(region_rollup_bc %>% select(CURRENT_REGION_PSSM_CODE_ROLLOUP),
             by = "CURRENT_REGION_PSSM_CODE_ROLLOUP") %>%
  mutate(LCP2_CD = str_sub(LCP4_CD, 1, 2)) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, LCP2_CD, TTRAIN, LCIP2_CRED, NOC_CD) %>%
  summarise(COUNT = sum(WEIGHTED), .groups = "drop")

q009b_totals_2d_bc <- q009_weight_occs %>%
  inner_join(region_rollup_bc %>% select(CURRENT_REGION_PSSM_CODE_ROLLOUP),
             by = "CURRENT_REGION_PSSM_CODE_ROLLOUP") %>%
  mutate(LCP2_CD = str_sub(LCP4_CD, 1, 2)) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, LCP2_CD, TTRAIN, LCIP2_CRED) %>%
  summarise(TOTAL = sum(WEIGHTED), .groups = "drop")

q010_2d_bc <- compute_dist(q009b_counts_2d_bc, q009b_totals_2d_bc,
                            c("PSSM_CREDENTIAL", "PSSM_CRED", "LCP2_CD", "TTRAIN", "LCIP2_CRED"))

# ---- Variant 6: 2D no TTRAIN (BC-wide) ----
q009b_counts_2d_bc_no_tt <- q009_weight_occs %>%
  inner_join(region_rollup_bc %>% select(CURRENT_REGION_PSSM_CODE_ROLLOUP),
             by = "CURRENT_REGION_PSSM_CODE_ROLLOUP") %>%
  mutate(
    LCP2_CD = str_sub(LCP4_CD, 1, 2),
    LCP2_CRED_NT = paste0(
      if_else(str_sub(PSSM_CRED, 1, 1) %in% c("1", "3"),
              paste0(str_sub(PSSM_CRED, 1, 1), " - "), ""),
      str_sub(LCP4_CD, 1, 2), " - ", PSSM_CREDENTIAL
    )
  ) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, LCP2_CD, LCP2_CRED_NT, NOC_CD) %>%
  summarise(COUNT = sum(WEIGHTED), .groups = "drop") %>%
  rename(LCIP2_CRED = LCP2_CRED_NT)

q009b_totals_2d_bc_no_tt <- q009_weight_occs %>%
  inner_join(region_rollup_bc %>% select(CURRENT_REGION_PSSM_CODE_ROLLOUP),
             by = "CURRENT_REGION_PSSM_CODE_ROLLOUP") %>%
  mutate(
    LCP2_CD = str_sub(LCP4_CD, 1, 2),
    LCP2_CRED_NT = paste0(
      if_else(str_sub(PSSM_CRED, 1, 1) %in% c("1", "3"),
              paste0(str_sub(PSSM_CRED, 1, 1), " - "), ""),
      str_sub(LCP4_CD, 1, 2), " - ", PSSM_CREDENTIAL
    )
  ) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, LCP2_CD, LCP2_CRED_NT) %>%
  summarise(TOTAL = sum(WEIGHTED), .groups = "drop") %>%
  rename(LCIP2_CRED = LCP2_CRED_NT)

q010_2d_bc_no_tt <- compute_dist(q009b_counts_2d_bc_no_tt, q009b_totals_2d_bc_no_tt,
                                  c("PSSM_CREDENTIAL", "PSSM_CRED", "LCP2_CD", "LCIP2_CRED"))


# ******************************************************************************
# Part 5: Write occupation distribution tables
# ******************************************************************************
# WHY: Each of the 6 output tables gets "Student Outcomes" rows replaced.
# Pattern: filter existing to keep non-Student-Outcomes, bind new rows, overwrite.

write_occ_table <- function(table_name, new_data, survey_col = "SURVEY") {
  full_name <- SQL(glue::glue('"{my_schema}"."{table_name}"'))
  new_rows <- new_data %>% mutate(!!survey_col := "Student Outcomes")

  if (dbExistsTable(decimal_con, full_name)) {
    existing <- sch_tbl(table_name) %>%
      collect() |> rename_with(toupper) %>%
      filter(SURVEY != "Student Outcomes")
    final <- bind_rows(existing, new_rows)
  } else {
    final <- new_rows
  }
  dbWriteTable(decimal_con, full_name, final, overwrite = TRUE)
}

# 4D with TTRAIN
write_occ_table("Occupation_Distributions",
  q010_4d %>% select(PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
                     AGE_GROUP_ROLLUP, LCP4_CD, TTRAIN, LCIP4_CRED, LCIP2_CRED,
                     NOC = NOC_CD, COUNT, TOTAL, PERCENT = PERC_DIST)
)

# 4D no TTRAIN
write_occ_table("Occupation_Distributions_No_TT",
  q010_4d_no_tt %>% select(PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
                            AGE_GROUP_ROLLUP, LCP4_CD, LCIP4_CRED, LCIP2_CRED,
                            NOC = NOC_CD, COUNT, TOTAL, PERCENT = PERC_DIST)
)

# 2D with TTRAIN
write_occ_table("Occupation_Distributions_LCP2",
  q010_2d %>% select(PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
                     AGE_GROUP_ROLLUP, LCP2_CD, TTRAIN, LCIP2_CRED,
                     NOC = NOC_CD, COUNT, TOTAL, PERCENT = PERC_DIST)
)

# 2D no TTRAIN
write_occ_table("Occupation_Distributions_LCP2_No_TT",
  q010_2d_no_tt %>% select(PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
                            AGE_GROUP_ROLLUP, LCP2_CD, LCIP2_CRED,
                            NOC = NOC_CD, COUNT, TOTAL, PERCENT = PERC_DIST)
)

# 2D with TTRAIN BC-wide
write_occ_table("Occupation_Distributions_LCP2_BC",
  q010_2d_bc %>% select(PSSM_CREDENTIAL, PSSM_CRED, LCP2_CD, TTRAIN, LCIP2_CRED,
                        NOC = NOC_CD, COUNT, TOTAL, PERCENT = PERC_DIST)
)

# 2D no TTRAIN BC-wide
write_occ_table("Occupation_Distributions_LCP2_BC_No_TT",
  q010_2d_bc_no_tt %>% select(PSSM_CREDENTIAL, PSSM_CRED, LCP2_CD, LCIP2_CRED,
                               NOC = NOC_CD, COUNT, TOTAL, PERCENT = PERC_DIST)
)


# ******************************************************************************
# Part 6: PDEG Law modifications (Q010d → Q010e)
# ******************************************************************************
# WHY: PDEG (post-degree) credentials in CIP cluster 07 (Law) need special handling.
# The occupation distribution for PDEG Law is derived from BACH Law (CIP 22)
# graduates, since law graduates typically have a bachelor's degree followed by
# a law degree. This replaces the default PDEG Law distribution.

# ---- NLS: PDEG 07 from BACH 22 ----
# Remove existing PDEG 07 rows from Labour_Supply_Distribution
ls_dist <- sch_tbl("Labour_Supply_Distribution") %>%
  collect() |> rename_with(toupper)

ls_dist <- ls_dist %>%
  filter(!(SURVEY == "2021 Census PSSM 2023-2024" & PSSM_CREDENTIAL == "PDEG" & LCP4_CD == "07"))

# Compute PDEG 07 NLS from BACH 22 data
pdeg_07_count <- ls_dist %>%
  filter(SURVEY == "Student Outcomes", PSSM_CREDENTIAL == "BACH",
         str_sub(LCP4_CD, 1, 2) == "22") %>%
  group_by(SURVEY, TTRAIN, CURRENT_REGION_PSSM_CODE_ROLLOUP, AGE_GROUP_ROLLUP) %>%
  summarise(COUNT = sum(COUNT), .groups = "drop") %>%
  mutate(PSSM_CREDENTIAL = "PDEG", PSSM_CRED = "PDEG", LCP4_CD = "07",
         LCIP4_CRED = "07 - PDEG")

pdeg_07_total <- ls_dist %>%
  filter(SURVEY == "Student Outcomes", PSSM_CREDENTIAL == "BACH",
         str_sub(LCP4_CD, 1, 2) == "22") %>%
  group_by(SURVEY, TTRAIN, AGE_GROUP_ROLLUP, TOTAL) %>%
  summarise(.groups = "drop") %>%
  mutate(PSSM_CREDENTIAL = "PDEG", PSSM_CRED = "PDEG", LCP4_CD = "07",
         LCIP4_CRED = "07 - PDEG") %>%
  group_by(SURVEY, PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD, TTRAIN, LCIP4_CRED, AGE_GROUP_ROLLUP) %>%
  summarise(TOTAL = sum(TOTAL), .groups = "drop")

pdeg_07_nls <- pdeg_07_total %>%
  left_join(pdeg_07_count %>% select(SURVEY, TTRAIN, CURRENT_REGION_PSSM_CODE_ROLLOUP,
                                      AGE_GROUP_ROLLUP, COUNT),
            by = c("SURVEY", "TTRAIN", "AGE_GROUP_ROLLUP")) %>%
  filter(!is.na(CURRENT_REGION_PSSM_CODE_ROLLOUP)) %>%
  mutate(NEW_LABOUR_SUPPLY = if_else(is.na(COUNT), 0, COUNT) / TOTAL)

ls_dist <- bind_rows(ls_dist, pdeg_07_nls)
dbWriteTable(decimal_con, SQL(glue::glue('"{my_schema}"."Labour_Supply_Distribution"')),
             ls_dist, overwrite = TRUE)


# ---- Occupation: PDEG 07 from BACH 22 ----
occ_dist <- sch_tbl("Occupation_Distributions") %>%
  collect() |> rename_with(toupper)

# Remove old PDEG 07 rows
occ_dist <- occ_dist %>%
  filter(!(SURVEY == "2021 Census PSSM 2022-2023" & PSSM_CREDENTIAL == "PDEG" & LCP4_CD == "07"))

# Compute PDEG 07 occupation distribution from BACH 22
pdeg_07_occ_count <- occ_dist %>%
  filter(SURVEY == "Student Outcomes", PSSM_CREDENTIAL == "BACH",
         str_sub(LCP4_CD, 1, 2) == "22") %>%
  group_by(SURVEY, TTRAIN, CURRENT_REGION_PSSM_CODE_ROLLOUP, AGE_GROUP_ROLLUP, NOC) %>%
  summarise(COUNT = sum(COUNT), .groups = "drop") %>%
  mutate(PSSM_CREDENTIAL = "PDEG", PSSM_CRED = "PDEG", LCP4_CD = "07",
         LCIP4_CRED = "07 - PDEG")

pdeg_07_occ_total <- pdeg_07_occ_count %>%
  group_by(SURVEY, PSSM_CREDENTIAL, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLOUP,
           AGE_GROUP_ROLLUP, LCP4_CD, TTRAIN, LCIP4_CRED) %>%
  summarise(TOTAL = sum(COUNT), .groups = "drop")

pdeg_07_occ_dist <- pdeg_07_occ_total %>%
  left_join(pdeg_07_occ_count %>% select(SURVEY, CURRENT_REGION_PSSM_CODE_ROLLOUP,
                                          AGE_GROUP_ROLLUP, LCIP4_CRED, NOC, COUNT),
            by = c("SURVEY", "CURRENT_REGION_PSSM_CODE_ROLLOUP", "AGE_GROUP_ROLLUP", "LCIP4_CRED")) %>%
  mutate(PERCENT = COUNT / TOTAL)

occ_dist <- bind_rows(occ_dist, pdeg_07_occ_dist)
dbWriteTable(decimal_con, SQL(glue::glue('"{my_schema}"."Occupation_Distributions"')),
             occ_dist, overwrite = TRUE)


# ******************************************************************************
# Part 7: End date imputation + DACSO ENDDT fix
# ******************************************************************************
# WHY: DACSO records with null ENDDT get a default end date of Dec of (survey_year - 2).
cohorts <- cohorts %>%
  mutate(ENDDT = if_else(is.na(ENDDT) & SURVEY == "DACSO",
                          paste0(as.numeric(SURVEY_YEAR) - 2, "-12"), ENDDT))

# Suppression table for public release (only for regular_run)
if (exists("regular_run") && regular_run == TRUE) {
  tryCatch({
    age_groups_rollup <- sch_tbl("tbl_Age_Groups_Rollup") %>%
      collect() |> rename_with(toupper)

    suppression_noc <- cohorts %>%
      inner_join(age_groups_rollup %>% select(AGE_GROUP_ROLLUP, AGE_GROUP_ROLLUP_LABEL),
                 by = "AGE_GROUP_ROLLUP") %>%
      filter(as.numeric(WEIGHT) > 0, !is.na(AGE_GROUP_ROLLUP)) %>%
      count(AGE_GROUP_ROLLUP, AGE_GROUP_ROLLUP_LABEL, NOC_CD, name = "Expr1") %>%
      filter(Expr1 < 5) %>%
      arrange(Expr1)

    dbWriteTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_Suppression_Public_Release_NOC"')),
                 suppression_noc, overwrite = TRUE)
  }, error = function(e) {
    print(paste("Error:", e$message))
    stop()
  })
}


# ******************************************************************************
# Part 8: Append StatCan census data
# ******************************************************************************
# WHY: Occupation_Distributions also includes census data from Statistics Canada.
occ_dist_final <- occ_dist %>%
  filter(SURVEY != "2021 Census PSSM 2023-2024")

stat_can_occ <- sch_tbl("Occupation_Distributions_Stat_Can") %>%
  collect() |> rename_with(toupper) %>%
  mutate(SURVEY = "2021 Census PSSM 2023-2024")

occ_dist_final <- bind_rows(occ_dist_final, stat_can_occ)

dbWriteTable(decimal_con, SQL(glue::glue('"{my_schema}"."Occupation_Distributions"')),
             occ_dist_final, overwrite = TRUE)


# ******************************************************************************
# Part 9: Cleanup and verification
# ******************************************************************************/
# Drop lookup tables no longer needed
# KEPT AS SQL: DROP TABLE (cleanup of tables loaded by earlier scripts)
for (tbl_name in c("tmp_tbl_Weights_OCC", "tmp_tbl_Weights_NLS", "tbl_Age_Groups",
                    "tbl_Age_Groups_Rollup", "tbl_Age", "T_PSSM_Credential_Grouping",
                    "T_Weights", "t_year_survey_year", "t_current_region_pssm_codes",
                    "t_current_region_pssm_rollup_codes", "t_current_region_pssm_rollup_codes_bc")) {
  full_name <- SQL(glue::glue('"{my_schema}"."{tbl_name}"'))
  if (dbExistsTable(decimal_con, full_name)) {
    dbExecute(decimal_con, glue::glue("DROP TABLE {tbl_name}"))
  }
}

# Verify tables exist
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Occupation_Distributions"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Occupation_Distributions_No_TT"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Occupation_Distributions_LCP2"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Occupation_Distributions_LCP2_No_TT"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Occupation_Distributions_LCP2_BC"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Occupation_Distributions_LCP2_BC_No_TT"')))

# ---- Disconnect ----
dbDisconnect(decimal_con)



# ==============================================================================
# FILE: 03-near-completers-ttrain_dplyr.R
# ==============================================================================


# Near Completers TTRAIN — dplyr Translation
# Original: R/03-near-completers-ttrain.R
#
# Pipeline context:
#   Computes near-completer ratios by age group, credential, CIP4 code, gender, and
#   TTRAIN status. Near-completers (grad_status='3') who later earned an STP credential
#   are identified and subtracted. The ratios feed into program projections (step 06).
#
# Input tables:
#   - T_DACSO_Data_Part_1 — DACSO outcomes data (from 02b-1)
#   - Credential_Non_Dup — deduplicated credentials (from 02a)
#   - dbo.STP_Credential — credential records (for PSI_PEN)
#   - tmp_tbl_Age, tmp_tbl_Age_AppendNewYears — age/date data
#   - combine_creds, AgeGroupLookup, CredentialRank, stp_dacso_prgm_credential_lookup — lookups
#   - t_pssm_projection_cred_grp — credential groupings
#
# Output tables:
#   - T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN — TTRAIN ratios
#   - T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history — historical TTRAIN
#   - T_DACSO_Near_Completers_RatioAgeAtGradCIP4 — CIP4 ratios
#   - T_DACSO_Near_Completers_RatioByGender — gender ratios
#   - T_DACSO_Near_Completers_RatioByGender_year — historical gender ratios

library(tidyverse)
library(odbc)
library(DBI)
library(config)
library(glue)

my_schema <- config::get("myschema")

sch_tbl <- function(name) {
  tbl(con, dbplyr::in_schema(my_schema, name))
}


# ---- Connect to Decimal ----
db_config <- config::get("decimal")
con <- dbConnect(odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")


# ******************************************************************************
# PART C: DERIVE AGE AT GRAD
# ******************************************************************************
# WHY: DACSO records have birth date (BTHDT) and end date (ENDDT) in YYYYMM format.
# We need to compute age at graduation for grouping near-completers and graduates.
# Original: 11 SQL operations (ALTER TABLE, UPDATE, INSERT INTO, DROP TABLE)
# Translated: dplyr string/date operations in memory.

# Pull the age data
tmp_age_append <- sch_tbl("tmp_tbl_Age_AppendNewYears") %>% collect() |> rename_with(toupper)
tmp_age <- sch_tbl("tmp_tbl_Age") %>% collect() |> rename_with(toupper)

# Clean birth/end dates: YYYYMM → MM/1/YYYY string → Date type
tmp_age_append <- tmp_age_append %>%
  mutate(
    BTHDT_CLEANED = paste0(substr(BTHDT, 5, 6), "/1/", substr(BTHDT, 1, 4)),
    ENDDT_CLEANED = case_when(
      ENDDT == "000000" ~ "",
      TRUE ~ paste0(substr(ENDDT, 5, 6), "/1/", substr(ENDDT, 1, 4))
    )
  ) %>%
  mutate(
    BTHDT_DATE = as.Date(BTHDT_CLEANED, format = "%m/%d/%Y"),
    ENDDT_DATE = as.Date(ENDDT_CLEANED, format = "%m/%d/%Y")
  ) %>%
  mutate(ENDDT_DATE = if_else(is.na(ENDDT_CLEANED) | ENDDT_CLEANED == "", as.Date(NA), ENDDT_DATE))

# Append to persistent age table
tmp_age_append <- tmp_age_append %>%
  select(COCI_STQU_ID, COCI_SUBM_CD, BTHDT_DATE, ENDDT_DATE, COCI_AGE_AT_SURVEY) %>%
  rename(TPID_DATE_OF_BIRTH = BTHDT_DATE, COSC_GRAD_CREDENTIAL_DATE = ENDDT_DATE,
         COSC_ENRL_END_DATE = ENDDT_DATE)

# Bind with existing age data
tmp_age <- bind_rows(tmp_age, tmp_age_append)

# Compute Age_At_Grad with birthday adjustment
# WHY: Simple year diff overcounts by 1 if graduation date falls before birthday
# in that calendar year.
tmp_age <- tmp_age %>%
  mutate(
    grad_date = coalesce(COSC_GRAD_CREDENTIAL_DATE, COSC_ENRL_END_DATE),
    Age_At_Grad = if_else(
      is.na(TPID_DATE_OF_BIRTH) | is.na(grad_date), NA_real_,
      as.numeric(substr(as.character(grad_date), 1, 4)) -
        as.numeric(substr(as.character(TPID_DATE_OF_BIRTH), 1, 4)) -
        if_else(
          as.numeric(substr(as.character(grad_date), 6, 7)) <
            as.numeric(substr(as.character(TPID_DATE_OF_BIRTH), 6, 7)), 1, 0
        )
    )
  )

# Pull T_DACSO_Data_Part_1 and add Age_At_Grad
t_dacso_data_part_1 <- sch_tbl("T_DACSO_Data_Part_1") %>% collect() |> rename_with(toupper)

t_dacso_data_part_1 <- t_dacso_data_part_1 %>%
  left_join(
    tmp_age %>% select(COCI_STQU_ID, Age_At_Grad),
    by = "COCI_STQU_ID"
  )

# Update the persistent tmp_tbl_Age table
dbWriteTable(con, "tmp_tbl_Age", tmp_age, overwrite = TRUE)


# ******************************************************************************
# PART D: TEMP SELECTION TABLE
# ******************************************************************************
# WHY: A column-subset version of T_DACSO_Data_Part_1 used for intermediate computations.
# Original: SELECT INTO
# Translated: select() in memory.

t_dacso_data_part_1_tempselection <- t_dacso_data_part_1 %>%
  select(COCI_STQU_ID, COCI_SUBM_CD, Age_At_Grad,
         COSC_GRAD_STATUS_LGDS_CD_GROUP, PRGM_CREDENTIAL_AWARDED,
         PRGM_CREDENTIAL_AWARDED_NAME, PSSM_CREDENTIAL, PSSM_CREDENTIAL_NAME)


# ******************************************************************************
# PART E: ADD PEN TO CREDENTIAL_NON_DUP
# ******************************************************************************
# WHY: Need PSI_PEN for PEN-based matching between DACSO and STP data.
# Original: ALTER TABLE + UPDATE...INNER JOIN
# Translated: left_join in memory, write back.

credential_non_dup <- sch_tbl("Credential_Non_Dup") %>%
  select(ID, PSI_PEN, PSI_CREDENTIAL_CATEGORY, PSI_AWARD_SCHOOL_YEAR,
         FINAL_CIP_CODE_4, PSI_CODE, OUTCOMES_CRED) %>%
  collect() |> rename_with(toupper)

if (!"PSI_PEN" %in% colnames(credential_non_dup) || all(is.na(credential_non_dup$PSI_PEN))) {
  stp_cred_pen <- sch_tbl("STP_Credential") %>%
    select(ID, PSI_PEN) %>%
    collect() |> rename_with(toupper)

  credential_non_dup <- credential_non_dup %>%
    select(-any_of("PSI_PEN")) %>%
    left_join(stp_cred_pen, by = "ID")

  dbWriteTable(con, "Credential_Non_Dup", credential_non_dup, overwrite = TRUE)
  rm(stp_cred_pen)
}


# ******************************************************************************
# PART F: DACSO MATCHING TO STP CREDENTIAL
# ******************************************************************************
# WHY: Match DACSO survey respondents to their STP credential records on PEN,
# then compute match flags (credential type, CIP4, CIP2, award year, institution).
# Original: 15 SQL operations (SELECT INTO, ALTER TABLE, UPDATE)
# Translated: inner_join + mutate pipeline.

# Pull lookup table for credential name mapping
stp_dacso_prgm_cred_lookup <- sch_tbl("stp_dacso_prgm_credential_lookup") %>%
  collect() |> rename_with(toupper)

# Join DACSO to Credential_Non_Dup on PEN
dacso_matching_stp_credential_pen <- t_dacso_data_part_1 %>%
  filter(COCI_PEN != "" & !is.na(COCI_PEN)) %>%
  inner_join(
    credential_non_dup %>% filter(OUTCOMES_CRED == "DACSO") %>% select(-OUTCOMES_CRED),
    by = c("COCI_PEN" = "PSI_PEN")
  )

# Add credential name from lookup
dacso_matching_stp_credential_pen <- dacso_matching_stp_credential_pen %>%
  left_join(
    stp_dacso_prgm_cred_lookup %>%
      select(PRGRM_CREDENTIAL_AWARDED, PRGM_CREDENTIAL_AWARDED_NAME = PRGM_CREDENTIAL_AWARDED_NAME),
    by = c("PRGM_CREDENTIAL_AWARDED" = "PRGRM_CREDENTIAL_AWARDED")
  )

# ---- Compute match flags ----
# WHY: Each flag indicates whether a specific dimension matches between DACSO and STP.

# Award year mapping: each survey cycle maps to two valid award school years
award_year_map <- tribble(
  ~COCI_SUBM_CD, ~PSI_AWARD_SCHOOL_YEAR,
  "C_Outc07", "2004/2005", "C_Outc07", "2005/2006",
  "C_Outc08", "2005/2006", "C_Outc08", "2006/2007",
  "C_Outc09", "2006/2007", "C_Outc09", "2007/2008",
  "C_Outc10", "2007/2008", "C_Outc10", "2008/2009",
  "C_Outc11", "2008/2009", "C_Outc11", "2009/2010",
  "C_Outc12", "2009/2010", "C_Outc12", "2010/2011",
  "C_Outc13", "2010/2011", "C_Outc13", "2011/2012",
  "C_Outc14", "2011/2012", "C_Outc14", "2012/2013",
  "C_Outc15", "2012/2013", "C_Outc15", "2013/2014",
  "C_Outc16", "2013/2014", "C_Outc16", "2014/2015",
  "C_Outc17", "2014/2015", "C_Outc17", "2015/2016",
  "C_Outc18", "2015/2016", "C_Outc18", "2016/2017",
  "C_Outc19", "2016/2017", "C_Outc19", "2017/2018",
  "C_Outc20", "2017/2018", "C_Outc20", "2018/2019",
  "C_Outc21", "2018/2019", "C_Outc21", "2019/2020",
  "C_Outc22", "2019/2020", "C_Outc22", "2020/2021",
  "C_Outc23", "2020/2021", "C_Outc23", "2021/2022"
)

# Institution code aliases
inst_aliases <- tribble(
  ~PSI_CODE, ~COCI_INST_CD,
  "CAP",     "CAPU",  "CAPU",    "CAP",
  "KWAN",    "KPU",   "KPU",     "KWAN",
  "OLA",     "TRUOL", "TRUOL",   "OLA",
  "MALA",    "VIU",   "VIU",     "MALA",
  "OUC",     "OKAN",  "OKAN",    "OUC",
  "UCFV",    "UFV",   "UFV",     "UCFV",
  "UCC",     "TRU",   "TRU",     "UCC",
  "NWCC",    "CMTN",  "CMTN",    "NWCC"
)

dacso_matching_stp_credential_pen <- dacso_matching_stp_credential_pen %>%
  mutate(
    match_credential = if_else(
      !is.na(PRGm_CREDENTIAL_AWARDED_NAME) &
        PRGM_CREDENTIAL_AWARDED_NAME == PSI_CREDENTIAL_CATEGORY, "yes", NA_character_
    ),
    match_cip_code_4 = if_else(
      !is.na(LCP4_CD) & !is.na(FINAL_CIP_CODE_4) & LCP4_CD == FINAL_CIP_CODE_4,
      "yes", NA_character_
    ),
    match_CIP_CODE_2 = if_else(
      !is.na(LCP4_CD) & !is.na(FINAL_CIP_CODE_4) &
        substr(LCP4_CD, 1, 2) == substr(FINAL_CIP_CODE_4, 1, 2),
      "Yes", NA_character_
    ),
    match_award_school_year = if_else(
      paste(COCI_SUBM_CD, PSI_AWARD_SCHOOL_YEAR) %in%
        paste(award_year_map$COCI_SUBM_CD, award_year_map$PSI_AWARD_SCHOOL_YEAR),
      "yes", NA_character_
    ),
    match_inst = if_else(
      PSI_CODE == COCI_INST_CD |
        paste(PSI_CODE, COCI_INST_CD) %in% paste(inst_aliases$PSI_CODE, inst_aliases$COCI_INST_CD),
      "yes", NA_character_
    )
  ) %>%
  mutate(
    match_all_4_flag = if_else(
      match_credential == "yes" & match_cip_code_4 == "yes" &
        match_award_school_year == "yes" & match_inst == "yes",
      "yes", NA_character_
    ),
    final_consider_a_match = if_else(
      match_all_4_flag == "yes", "yes",
      if_else(
        match_credential == "yes" & match_CIP_CODE_2 == "Yes" &
          match_award_school_year == "yes" & match_inst == "yes" &
          is.na(match_cip_code_4),
        "yes", NA_character_
      )
    )
  )


# ******************************************************************************
# PART G: FLAG NEAR-COMPLETERS WITH EARLIER/LATER CREDENTIAL
# ******************************************************************************
# WHY: Near-completers (grad_status='3') who earned an STP credential before or
# after their DACSO survey should be subtracted from the near-completer count.
# This section identifies those credentials and resolves duplicates.
# Original: ~56 SQL operations
# Translated: dplyr pipeline with group_by + slice_max for deduplication.

# ---- Find near-completers with STP credential matches ----
nearcompleters_step1 <- t_dacso_data_part_1 %>%
  filter(
    COSC_GRAD_STATUS_LGDS_CD_GROUP == "3",
    !is.na(Age_At_Grad), Age_At_Grad >= 17, Age_At_Grad <= 64,
    COCI_SUBM_CD %in% paste0("C_Outc", c("07","08","09","10","11","12","13","14","15","16","17","18","19","20","21","22","23"))
  ) %>%
  inner_join(
    dacso_matching_stp_credential_pen %>%
      select(COCI_STQU_ID, ID, PSI_AWARD_SCHOOL_YEAR, everything()),
    by = "COCI_STQU_ID"
  ) %>%
  filter(!is.na(ID))

# ---- Determine before/after credential timing ----
# WHY: Each survey cycle has valid "before" award years. If the STP credential
# year falls within the "before" window, it was earned before the DACSO survey.
before_year_map <- tribble(
  ~COCI_SUBM_CD, ~PSI_AWARD_SCHOOL_YEAR,
  "C_Outc07", "2002/2003", "C_Outc07", "2003/2004", "C_Outc07", "2004/2005", "C_Outc07", "2005/2006",
  "C_Outc08", "2002/2003", "C_Outc08", "2003/2004", "C_Outc08", "2004/2005", "C_Outc08", "2005/2006", "C_Outc08", "2006/2007",
  "C_Outc09", "2002/2003", "C_Outc09", "2003/2004", "C_Outc09", "2004/2005", "C_Outc09", "2005/2006", "C_Outc09", "2006/2007", "C_Outc09", "2007/2008",
  "C_Outc10", "2002/2003", "C_Outc10", "2003/2004", "C_Outc10", "2004/2005", "C_Outc10", "2005/2006", "C_Outc10", "2006/2007", "C_Outc10", "2007/2008", "C_Outc10", "2008/2009",
  "C_Outc11", "2002/2003", "C_Outc11", "2003/2004", "C_Outc11", "2004/2005", "C_Outc11", "2005/2006", "C_Outc11", "2006/2007", "C_Outc11", "2007/2008", "C_Outc11", "2008/2009", "C_Outc11", "2009/2010",
  "C_Outc12", "2002/2003", "C_Outc12", "2003/2004", "C_Outc12", "2004/2005", "C_Outc12", "2005/2006", "C_Outc12", "2006/2007", "C_Outc12", "2007/2008", "C_Outc12", "2008/2009", "C_Outc12", "2009/2010", "C_Outc12", "2010/2011",
  "C_Outc13", "2002/2003", "C_Outc13", "2003/2004", "C_Outc13", "2004/2005", "C_Outc13", "2005/2006", "C_Outc13", "2006/2007", "C_Outc13", "2007/2008", "C_Outc13", "2008/2009", "C_Outc13", "2009/2010", "C_Outc13", "2010/2011", "C_Outc13", "2011/2012",
  "C_Outc14", "2002/2003", "C_Outc14", "2003/2004", "C_Outc14", "2004/2005", "C_Outc14", "2005/2006", "C_Outc14", "2006/2007", "C_Outc14", "2007/2008", "C_Outc14", "2008/2009", "C_Outc14", "2009/2010", "C_Outc14", "2010/2011", "C_Outc14", "2011/2012", "C_Outc14", "2012/2013",
  "C_Outc15", "2002/2003", "C_Outc15", "2003/2004", "C_Outc15", "2004/2005", "C_Outc15", "2005/2006", "C_Outc15", "2006/2007", "C_Outc15", "2007/2008", "C_Outc15", "2008/2009", "C_Outc15", "2009/2010", "C_Outc15", "2010/2011", "C_Outc15", "2011/2012", "C_Outc15", "2012/2013", "C_Outc15", "2013/2014",
  "C_Outc16", "2002/2003", "C_Outc16", "2003/2004", "C_Outc16", "2004/2005", "C_Outc16", "2005/2006", "C_Outc16", "2006/2007", "C_Outc16", "2007/2008", "C_Outc16", "2008/2009", "C_Outc16", "2009/2010", "C_Outc16", "2010/2011", "C_Outc16", "2011/2012", "C_Outc16", "2012/2013", "C_Outc16", "2013/2014", "C_Outc16", "2014/2015",
  "C_Outc17", "2002/2003", "C_Outc17", "2003/2004", "C_Outc17", "2004/2005", "C_Outc17", "2005/2006", "C_Outc17", "2006/2007", "C_Outc17", "2007/2008", "C_Outc17", "2008/2009", "C_Outc17", "2009/2010", "C_Outc17", "2010/2011", "C_Outc17", "2011/2012", "C_Outc17", "2012/2013", "C_Outc17", "2013/2014", "C_Outc17", "2014/2015", "C_Outc17", "2015/2016",
  "C_Outc18", "2002/2003", "C_Outc18", "2003/2004", "C_Outc18", "2004/2005", "C_Outc18", "2005/2006", "C_Outc18", "2006/2007", "C_Outc18", "2007/2008", "C_Outc18", "2008/2009", "C_Outc18", "2009/2010", "C_Outc18", "2010/2011", "C_Outc18", "2011/2012", "C_Outc18", "2012/2013", "C_Outc18", "2013/2014", "C_Outc18", "2014/2015", "C_Outc18", "2015/2016", "C_Outc18", "2016/2017",
  "C_Outc19", "2002/2003", "C_Outc19", "2003/2004", "C_Outc19", "2004/2005", "C_Outc19", "2005/2006", "C_Outc19", "2006/2007", "C_Outc19", "2007/2008", "C_Outc19", "2008/2009", "C_Outc19", "2009/2010", "C_Outc19", "2010/2011", "C_Outc19", "2011/2012", "C_Outc19", "2012/2013", "C_Outc19", "2013/2014", "C_Outc19", "2014/2015", "C_Outc19", "2015/2016", "C_Outc19", "2016/2017", "C_Outc19", "2017/2018",
  "C_Outc20", "2002/2003", "C_Outc20", "2003/2004", "C_Outc20", "2004/2005", "C_Outc20", "2005/2006", "C_Outc20", "2006/2007", "C_Outc20", "2007/2008", "C_Outc20", "2008/2009", "C_Outc20", "2009/2010", "C_Outc20", "2010/2011", "C_Outc20", "2011/2012", "C_Outc20", "2012/2013", "C_Outc20", "2013/2014", "C_Outc20", "2014/2015", "C_Outc20", "2015/2016", "C_Outc20", "2016/2017", "C_Outc20", "2017/2018", "C_Outc20", "2018/2019",
  "C_Outc21", "2002/2003", "C_Outc21", "2003/2004", "C_Outc21", "2004/2005", "C_Outc21", "2005/2006", "C_Outc21", "2006/2007", "C_Outc21", "2007/2008", "C_Outc21", "2008/2009", "C_Outc21", "2009/2010", "C_Outc21", "2010/2011", "C_Outc21", "2011/2012", "C_Outc21", "2012/2013", "C_Outc21", "2013/2014", "C_Outc21", "2014/2015", "C_Outc21", "2015/2016", "C_Outc21", "2016/2017", "C_Outc21", "2017/2018", "C_Outc21", "2018/2019", "C_Outc21", "2019/2020",
  "C_Outc22", "2002/2003", "C_Outc22", "2003/2004", "C_Outc22", "2004/2005", "C_Outc22", "2005/2006", "C_Outc22", "2006/2007", "C_Outc22", "2007/2008", "C_Outc22", "2008/2009", "C_Outc22", "2009/2010", "C_Outc22", "2010/2011", "C_Outc22", "2011/2012", "C_Outc22", "2012/2013", "C_Outc22", "2013/2014", "C_Outc22", "2014/2015", "C_Outc22", "2015/2016", "C_Outc22", "2016/2017", "C_Outc22", "2017/2018", "C_Outc22", "2018/2019", "C_Outc22", "2019/2020", "C_Outc22", "2020/2021",
  "C_Outc23", "2002/2003", "C_Outc23", "2003/2004", "C_Outc23", "2004/2005", "C_Outc23", "2005/2006", "C_Outc23", "2006/2007", "C_Outc23", "2007/2008", "C_Outc23", "2008/2009", "C_Outc23", "2009/2010", "C_Outc23", "2010/2011", "C_Outc23", "2011/2012", "C_Outc23", "2012/2013", "C_Outc23", "2013/2014", "C_Outc23", "2014/2015", "C_Outc23", "2015/2016", "C_Outc23", "2016/2017", "C_Outc23", "2017/2018", "C_Outc23", "2018/2019", "C_Outc23", "2019/2020", "C_Outc23", "2020/2021", "C_Outc23", "2021/2022"
)

nearcompleters_step1 <- nearcompleters_step1 %>%
  mutate(
    STP_Credential_Awarded_Before_DACSO = if_else(
      paste(COCI_SUBM_CD, PSI_AWARD_SCHOOL_YEAR) %in%
        paste(before_year_map$COCI_SUBM_CD, before_year_map$PSI_AWARD_SCHOOL_YEAR),
      "Yes", NA_character_
    ),
    STP_Credential_Awarded_After_DACSO = if_else(
      is.na(STP_Credential_Awarded_Before_DACSO), "Yes", NA_character_
    )
  )

# ---- Resolve duplicates: pick max award year, then max ID for ties ----
# WHY: Near-completers matching multiple STP credentials get deduplicated by picking
# the most recent award year, then breaking ties with the max ID.
nearcompleters_step1 <- nearcompleters_step1 %>%
  group_by(COCI_STQU_ID) %>%
  mutate(n_creds = n()) %>%
  ungroup() %>%
  mutate(Has_Multiple_STP_Credentials = if_else(n_creds > 1, "Yes", NA_character_)) %>%
  group_by(COCI_STQU_ID) %>%
  arrange(desc(PSI_AWARD_SCHOOL_YEAR), desc(ID), .by_group = TRUE) %>%
  mutate(row_num = row_number()) %>%
  ungroup() %>%
  mutate(
    Final_Record_To_Use = if_else(n_creds == 1 | row_num == 1, "Yes", NA_character_),
    Dup_STQUID_UseThisRecord = if_else(n_creds > 1 & row_num == 1, "Yes", NA_character_)
  )

# ---- Create T_DACSO_NearCompleters ----
T_DACSO_NearCompleters <- t_dacso_data_part_1 %>%
  filter(
    COSC_GRAD_STATUS_LGDS_CD_GROUP == "3",
    !is.na(Age_At_Grad), Age_At_Grad >= 17, Age_At_Grad <= 64
  ) %>%
  select(COCI_STQU_ID, COCI_SUBM_CD, Age_At_Grad, COSC_GRAD_STATUS_LGDS_CD_GROUP,
         PRGM_CREDENTIAL_AWARDED, PRGM_CREDENTIAL_AWARDED_NAME, PSSM_CREDENTIAL, PSSM_CREDENTIAL_NAME)

# Propagate before/after flags from finalized records
final_flags <- nearcompleters_step1 %>%
  filter(Final_Record_To_Use == "Yes") %>%
  select(COCI_STQU_ID, STP_Credential_Awarded_Before_DACSO,
         STP_Credential_Awarded_After_DACSO, Has_Multiple_STP_Credentials)

T_DACSO_NearCompleters <- T_DACSO_NearCompleters %>%
  left_join(final_flags, by = "COCI_STQU_ID") %>%
  mutate(
    STP_Credential_Awarded_Before_DACSO_Final = STP_Credential_Awarded_Before_DACSO,
    STP_Credential_Awarded_After_DACSO_Final = STP_Credential_Awarded_After_DACSO
  )

# ---- Update Has_STP_Credential and Grad_Status_Factoring_in_STP ----
# WHY: Near-completers who earned an STP credential are reclassified as completers.
has_stp <- T_DACSO_NearCompleters %>%
  filter(STP_Credential_Awarded_Before_DACSO == "Yes" |
         STP_Credential_Awarded_After_DACSO == "Yes") %>%
  distinct(COCI_STQU_ID) %>%
  mutate(Has_STP_Credential = "Yes")

t_dacso_data_part_1_tempselection <- t_dacso_data_part_1_tempselection %>%
  left_join(has_stp, by = "COCI_STQU_ID") %>%
  mutate(
    Has_STP_Credential = coalesce(Has_STP_Credential, NA_character_),
    Grad_Status_Factoring_in_STP = COSC_GRAD_STATUS_LGDS_CD_GROUP,
    Grad_Status_Factoring_in_STP = if_else(
      COSC_GRAD_STATUS_LGDS_CD_GROUP == "3" & Has_STP_Credential == "Yes",
      "1", Grad_Status_Factoring_in_STP
    )
  )

# Propagate to main table
t_dacso_data_part_1 <- t_dacso_data_part_1 %>%
  left_join(
    t_dacso_data_part_1_tempselection %>%
      select(COCI_STQU_ID, Has_STP_Credential, Grad_Status_Factoring_in_STP),
    by = "COCI_STQU_ID"
  )

# Update the matching table with Dup_STQUID flags
dup_flags <- nearcompleters_step1 %>%
  filter(!is.na(Dup_STQUID_UseThisRecord)) %>%
  select(COCI_STQU_ID, ID, Dup_STQUID_UseThisRecord)

dacso_matching_stp_credential_pen <- dacso_matching_stp_credential_pen %>%
  left_join(
    dup_flags %>% rename(DUP_FLAG = Dup_STQUID_UseThisRecord),
    by = c("COCI_STQU_ID", "ID")
  ) %>%
  mutate(Dup_STQUID_UseThisRecord = coalesce(DUP_FLAG, Dup_STQUID_UseThisRecord)) %>%
  select(-DUP_FLAG)

# Write updated main tables back to DB
dbWriteTable(con, "T_DACSO_Data_Part_1", t_dacso_data_part_1, overwrite = TRUE)
dbWriteTable(con, "T_DACSO_DATA_Part_1_TempSelection",
             t_dacso_data_part_1_tempselection, overwrite = TRUE)

rm(nearcompleters_step1, final_flags, has_stp, dup_flags, T_DACSO_NearCompleters)


# ******************************************************************************
# PART H: DIAGNOSTIC CHECKS (kept as comments for reference)
# ******************************************************************************
# Original: 5 PIVOT/GROUP BY diagnostic queries
# These are read-only diagnostics — translate to dplyr if needed.
# Example: t_dacso_data_part_1_tempselection %>%
#   filter(!is.na(COSC_GRAD_STATUS_LGDS_CD_GROUP), Age_At_Grad >= 17, Age_At_Grad <= 64) %>%
#   count(COSC_GRAD_STATUS_LGDS_CD_GROUP, COCI_SUBM_CD) %>%
#   pivot_wider(names_from = COCI_SUBM_CD, values_from = n, values_fill = 0)


# ******************************************************************************
# PART I: CIP4 RATIO COMPUTATIONS
# ******************************************************************************
# WHY: Compute near-completer/completer ratios by CIP4 code and age group.
# Four aggregations feed into a single ratio computation.
# Original: 17 SQL operations (SELECT INTO, ALTER TABLE, UPDATE, DROP TABLE)
# Translated: dplyr aggregation pipelines in memory.

# Pull lookup tables
combine_creds <- sch_tbl("combine_creds") %>% collect() |> rename_with(toupper)
age_group_lookup <- sch_tbl("AgeGroupLookup") %>% collect() |> rename_with(toupper)
credential_rank <- sch_tbl("CredentialRank") %>% collect() |> rename_with(toupper)

# Helper: clean lcip4_cred labels (strip "- 0" or "- 1" suffixes, fix "1 -" → "3 -")
clean_lcip4 <- function(df) {
  df %>%
    mutate(
      lcip4_cred = gsub("^1 - ", "3 - ", lcip4_cred),
      lcip4_cred = gsub(" - 0 $| - 1 $", "", lcip4_cred)
    )
}

# ---- Near-completers total by CIP4 ----
nc_cip4 <- t_dacso_data_part_1 %>%
  filter(
    COSC_GRAD_STATUS_LGDS_CD_GROUP == "3",
    COCI_SUBM_CD %in% c("C_Outc19", "C_Outc20"),
    !is.na(Age_At_Grad), Age_At_Grad >= 17, Age_At_Grad <= 64
  ) %>%
  inner_join(age_group_lookup, by = c("AGE_AT_GRAD" = "AGEINDEX")) %>%
  inner_join(credential_rank, by = c("PRGM_CREDENTIAL_AWARDED_NAME" = "PRGM_CREDENTIAL_AWARDED_NAME")) %>%
  group_by(AGEGROUP, PRGM_CREDENTIAL_AWARDED_NAME, LCIP4_CRED, LCP4_CD, LCP4_CIP_4DIGITS_NAME) %>%
  summarise(Count = n(), .groups = "drop")

nc_cip4_combined <- nc_cip4 %>%
  inner_join(
    combine_creds %>% filter(USE_IN_PSSM_2017_18 == "Yes"),
    by = c("PRGM_CREDENTIAL_AWARDED_NAME" = "PRGM_CREDENTIAL_AWARDED_NAME")
  ) %>%
  clean_lcip4() %>%
  group_by(AGEGROUP, LCIP4_CRED, LCP4_CD) %>%
  summarise(Count = sum(COMBINED_CRED_COUNT, na.rm = TRUE), .groups = "drop")

# ---- Near-completers with STP credential by CIP4 ----
nc_stp_cip4 <- t_dacso_data_part_1 %>%
  inner_join(age_group_lookup, by = c("AGE_AT_GRAD" = "AGEINDEX")) %>%
  inner_join(credential_rank, by = c("PRGM_CREDENTIAL_AWARDED_NAME" = "PRGM_CREDENTIAL_AWARDED_NAME")) %>%
  inner_join(
    t_dacso_data_part_1_tempselection %>%
      select(COCI_STQU_ID, Has_STP_Credential),
    by = "COCI_STQU_ID"
  ) %>%
  filter(
    COSC_GRAD_STATUS_LGDS_CD_GROUP == "3",
    COCI_SUBM_CD %in% c("C_Outc19", "C_Outc20"),
    !is.na(Age_At_Grad), Age_At_Grad >= 17, Age_At_Grad <= 64,
    Has_STP_Credential == "Yes"
  ) %>%
  group_by(AGEGROUP, PRGM_CREDENTIAL_AWARDED_NAME, LCIP4_CRED, LCP4_CD, LCP4_CIP_4DIGITS_NAME) %>%
  summarise(Count = n(), .groups = "drop")

nc_stp_cip4_combined <- nc_stp_cip4 %>%
  inner_join(
    combine_creds %>% filter(USE_IN_PSSM_2017_18 == "Yes"),
    by = c("PRGM_CREDENTIAL_AWARDED_NAME" = "PRGM_CREDENTIAL_AWARDED_NAME")
  ) %>%
  clean_lcip4() %>%
  group_by(AGEGROUP, LCIP4_CRED, LCP4_CD) %>%
  summarise(nc_with_earlier_or_later = sum(COMBINED_CRED_COUNT, na.rm = TRUE), .groups = "drop")

# ---- Completers factoring in STP by CIP4 ----
comp_stp_cip4 <- t_dacso_data_part_1 %>%
  inner_join(age_group_lookup, by = c("AGE_AT_GRAD" = "AGEINDEX")) %>%
  inner_join(credential_rank, by = c("PRGM_CREDENTIAL_AWARDED_NAME" = "PRGM_CREDENTIAL_AWARDED_NAME")) %>%
  filter(
    Grad_Status_Factoring_in_STP == "1",
    COCI_SUBM_CD %in% c("C_Outc19", "C_Outc20"),
    !is.na(Age_At_Grad), Age_At_Grad >= 17, Age_At_Grad <= 64
  ) %>%
  group_by(AGEGROUP, PRGM_CREDENTIAL_AWARDED_NAME, LCIP4_CRED, LCP4_CD, LCP4_CIP_4DIGITS_NAME) %>%
  summarise(Count = n(), .groups = "drop")

comp_stp_cip4_combined <- comp_stp_cip4 %>%
  mutate(lcip4_cred = if_else(grepl("^1 - ", LCIP4_CRED),
                               sub("^1 - ", "3 - ", LCIP4_CRED), LCIP4_CRED)) %>%
  inner_join(
    combine_creds %>% filter(USE_IN_PSSM_2017_18 == "Yes"),
    by = c("PRGM_CREDENTIAL_AWARDED_NAME" = "PRGM_CREDENTIAL_AWARDED_NAME")
  ) %>%
  clean_lcip4() %>%
  group_by(AGEGROUP, LCIP4_CRED, LCP4_CD) %>%
  summarise(completers = sum(COMBINED_CRED_COUNT, na.rm = TRUE), .groups = "drop")

# ---- Raw completers by CIP4 ----
comp_cip4 <- t_dacso_data_part_1 %>%
  inner_join(age_group_lookup, by = c("AGE_AT_GRAD" = "AGEINDEX")) %>%
  inner_join(credential_rank, by = c("PRGM_CREDENTIAL_AWARDED_NAME" = "PRGM_CREDENTIAL_AWARDED_NAME")) %>%
  filter(
    COSC_GRAD_STATUS_LGDS_CD_GROUP == "1",
    COCI_SUBM_CD %in% c("C_Outc19", "C_Outc20"),
    !is.na(Age_At_Grad), Age_At_Grad >= 17, Age_At_Grad <= 64
  ) %>%
  group_by(AGEGROUP, PRGM_CREDENTIAL_AWARDED_NAME, LCIP4_CRED, LCP4_CD, LCP4_CIP_4DIGITS_NAME) %>%
  summarise(Count = n(), .groups = "drop")

comp_cip4_combined <- comp_cip4 %>%
  mutate(lcip4_cred = if_else(grepl("^1 - ", LCIP4_CRED),
                               sub("^1 - ", "3 - ", LCIP4_CRED), LCIP4_CRED)) %>%
  inner_join(
    combine_creds %>% filter(USE_IN_PSSM_2017_18 == "Yes"),
    by = c("PRGM_CREDENTIAL_AWARDED_NAME" = "PRGM_CREDENTIAL_AWARDED_NAME")
  ) %>%
  clean_lcip4() %>%
  group_by(AGEGROUP, LCIP4_CRED, LCP4_CD) %>%
  summarise(c_not_factoring_stp = sum(COMBINED_CRED_COUNT, na.rm = TRUE), .groups = "drop")

# ---- Compute final CIP4 ratios ----
T_DACSO_Near_Completers_RatioAgeAtGradCIP4 <- nc_cip4_combined %>%
  left_join(nc_stp_cip4_combined, by = c("AGEGROUP", "LCIP4_CRED", "LCP4_CD")) %>%
  left_join(comp_stp_cip4_combined, by = c("AGEGROUP", "LCIP4_CRED", "LCP4_CD")) %>%
  left_join(comp_cip4_combined, by = c("AGEGROUP", "LCIP4_CRED", "LCP4_CD")) %>%
  replace_na(list(nc_with_earlier_or_later = 0, completers = 0, c_not_factoring_stp = 0)) %>%
  mutate(
    near_completers_stp_cred = Count - nc_with_earlier_or_later,
    ratio = if_else(completers == 0, NA_real_, near_completers_stp_cred / completers),
    ratio_not_factoring_stp = if_else(c_not_factoring_stp == 0, NA_real_,
                                       near_completers_stp_cred / c_not_factoring_stp),
    ratio = na_if(ratio, Inf),
    ratio_not_factoring_stp = na_if(ratio_not_factoring_stp, Inf)
  ) %>%
  rename(age_group = AGEGROUP, count = Count)

dbWriteTable(con, "T_DACSO_Near_Completers_RatioAgeAtGradCIP4",
             T_DACSO_Near_Completers_RatioAgeAtGradCIP4, overwrite = TRUE)


# ******************************************************************************
# PART J: GENDER RATIO COMPUTATIONS
# ******************************************************************************
# WHY: Near-completer ratios by gender (tpid_lgnd_cd), age group, and credential.
# Original: 3 SELECT INTO + dbReadTable + dplyr ratio computation
# Translated: All in-memory aggregation.

nc_gender <- t_dacso_data_part_1 %>%
  inner_join(age_group_lookup, by = c("AGE_AT_GRAD" = "AGEINDEX")) %>%
  inner_join(credential_rank, by = c("PRGM_CREDENTIAL_AWARDED_NAME" = "PRGM_CREDENTIAL_AWARDED_NAME")) %>%
  filter(
    COSC_GRAD_STATUS_LGDS_CD_GROUP == "3",
    COCI_SUBM_CD %in% c("C_Outc19", "C_Outc20"),
    !is.na(Age_At_Grad), Age_At_Grad >= 17, Age_At_Grad <= 64,
    TPID_LGND_CD != "0"
  ) %>%
  count(AGEGROUP, PRGM_CREDENTIAL_AWARDED_NAME, TPID_LGND_CD, name = "Count")

nc_stp_gender <- t_dacso_data_part_1 %>%
  inner_join(age_group_lookup, by = c("AGE_AT_GRAD" = "AGEINDEX")) %>%
  inner_join(credential_rank, by = c("PRGM_CREDENTIAL_AWARDED_NAME" = "PRGM_CREDENTIAL_AWARDED_NAME")) %>%
  inner_join(
    t_dacso_data_part_1_tempselection %>% select(COCI_STQU_ID, Has_STP_Credential),
    by = "COCI_STQU_ID"
  ) %>%
  filter(
    COSC_GRAD_STATUS_LGDS_CD_GROUP == "3",
    COCI_SUBM_CD %in% c("C_Outc19", "C_Outc20"),
    !is.na(Age_At_Grad), Age_At_Grad >= 17, Age_At_Grad <= 64,
    Has_STP_Credential == "Yes",
    TPID_LGND_CD != "0"
  ) %>%
  count(AGEGROUP, PRGM_CREDENTIAL_AWARDED_NAME, TPID_LGND_CD, name = "nc_with_early_or_late")

comp_gender <- t_dacso_data_part_1 %>%
  inner_join(age_group_lookup, by = c("AGE_AT_GRAD" = "AGEINDEX")) %>%
  inner_join(credential_rank, by = c("PRGM_CREDENTIAL_AWARDED_NAME" = "PRGM_CREDENTIAL_AWARDED_NAME")) %>%
  filter(
    COSC_GRAD_STATUS_LGDS_CD_GROUP == "1",
    COCI_SUBM_CD %in% c("C_Outc19", "C_Outc20"),
    !is.na(Age_At_Grad), Age_At_Grad >= 17, Age_At_Grad <= 64,
    TPID_LGND_CD != "0"
  ) %>%
  count(AGEGROUP, PRGM_CREDENTIAL_AWARDED_NAME, TPID_LGND_CD, name = "completers")

T_DACSO_Near_Completers_RatioByGender <- nc_gender %>%
  left_join(nc_stp_gender, by = c("AGEGROUP", "PRGM_CREDENTIAL_AWARDED_NAME", "TPID_LGND_CD")) %>%
  left_join(comp_gender, by = c("AGEGROUP", "PRGM_CREDENTIAL_AWARDED_NAME", "TPID_LGND_CD")) %>%
  replace_na(list(nc_with_early_or_late = 0, completers = 0)) %>%
  mutate(
    n_nc_stp = Count - nc_with_early_or_late,
    ratio = if_else(completers == 0, NA_real_, n_nc_stp / completers),
    ratio = na_if(ratio, Inf)
  ) %>%
  rename(gender = TPID_LGND_CD, age_group = AGEGROUP)

dbWriteTable(con, "T_DACSO_Near_Completers_RatioByGender",
             T_DACSO_Near_Completers_RatioByGender, overwrite = TRUE)


# ******************************************************************************
# PART K: GENDER RATIO BY YEAR (HISTORICAL)
# ******************************************************************************
# WHY: Same as Part J but broken out by survey year for historical analysis.
# Original: 3 SELECT INTO + dplyr ratio computation
# Translated: Same aggregation with COCI_SUBM_CD in group_by.

nc_gender_year <- t_dacso_data_part_1 %>%
  inner_join(age_group_lookup, by = c("AGE_AT_GRAD" = "AGEINDEX")) %>%
  inner_join(credential_rank, by = c("PRGM_CREDENTIAL_AWARDED_NAME" = "PRGM_CREDENTIAL_AWARDED_NAME")) %>%
  filter(
    COSC_GRAD_STATUS_LGDS_CD_GROUP == "3",
    !is.na(Age_At_Grad), Age_At_Grad >= 17, Age_At_Grad <= 64,
    TPID_LGND_CD != "0"
  ) %>%
  count(COCI_SUBM_CD, AGEGROUP, PRGM_CREDENTIAL_AWARDED_NAME, TPID_LGND_CD, name = "Count")

nc_stp_gender_year <- t_dacso_data_part_1 %>%
  inner_join(age_group_lookup, by = c("AGE_AT_GRAD" = "AGEINDEX")) %>%
  inner_join(credential_rank, by = c("PRGM_CREDENTIAL_AWARDED_NAME" = "PRGM_CREDENTIAL_AWARDED_NAME")) %>%
  inner_join(
    t_dacso_data_part_1_tempselection %>% select(COCI_STQU_ID, Has_STP_Credential),
    by = "COCI_STQU_ID"
  ) %>%
  filter(
    COSC_GRAD_STATUS_LGDS_CD_GROUP == "3",
    !is.na(Age_At_Grad), Age_At_Grad >= 17, Age_At_Grad <= 64,
    Has_STP_Credential == "Yes",
    TPID_LGND_CD != "0"
  ) %>%
  count(COCI_SUBM_CD, AGEGROUP, PRGM_CREDENTIAL_AWARDED_NAME, TPID_LGND_CD, name = "nc_with_early_or_late")

comp_gender_year <- t_dacso_data_part_1 %>%
  inner_join(age_group_lookup, by = c("AGE_AT_GRAD" = "AGEINDEX")) %>%
  inner_join(credential_rank, by = c("PRGM_CREDENTIAL_AWARDED_NAME" = "PRGM_CREDENTIAL_AWARDED_NAME")) %>%
  filter(
    COSC_GRAD_STATUS_LGDS_CD_GROUP == "1",
    !is.na(Age_At_Grad), Age_At_Grad >= 17, Age_At_Grad <= 64,
    TPID_LGND_CD != "0"
  ) %>%
  count(COCI_SUBM_CD, AGEGROUP, PRGM_CREDENTIAL_AWARDED_NAME, TPID_LGND_CD, name = "completers")

T_DACSO_Near_Completers_RatioByGender_year <- nc_gender_year %>%
  left_join(nc_stp_gender_year,
    by = c("COCI_SUBM_CD", "AGEGROUP", "PRGM_CREDENTIAL_AWARDED_NAME", "TPID_LGND_CD")) %>%
  left_join(comp_gender_year,
    by = c("COCI_SUBM_CD", "AGEGROUP", "PRGM_CREDENTIAL_AWARDED_NAME", "TPID_LGND_CD")) %>%
  replace_na(list(nc_with_early_or_late = 0, completers = 0)) %>%
  mutate(
    n_nc_stp = Count - nc_with_early_or_late,
    ratio = if_else(completers == 0, NA_real_, n_nc_stp / completers),
    ratio = na_if(ratio, Inf),
    year = as.numeric(paste0("20", substr(COCI_SUBM_CD, nchar(COCI_SUBM_CD) - 1, nchar(COCI_SUBM_CD)))) - 1
  ) %>%
  rename(gender = TPID_LGND_CD, age_group = AGEGROUP)

dbWriteTable(con, "T_DACSO_Near_Completers_RatioByGender_year",
             T_DACSO_Near_Completers_RatioByGender_year, overwrite = TRUE)


# ******************************************************************************
# PART L: TTRAIN TABLES (CURRENT)
# ******************************************************************************
# WHY: Near-completer ratios with TTRAIN (trades training) dimension for CIP4-based
# program projections. Uses C_Outc19/20 as representative years.
# Original: 3 SELECT INTO + 2 DROP TABLE
# Translated: In-memory aggregation.

t_pssm_proj_cred_grp <- sch_tbl("t_pssm_projection_cred_grp") %>% collect() |> rename_with(toupper)

nc_ttrain <- t_dacso_data_part_1 %>%
  inner_join(age_group_lookup, by = c("AGE_AT_GRAD" = "AGEINDEX")) %>%
  inner_join(credential_rank, by = c("PRGM_CREDENTIAL_AWARDED_NAME" = "PRGM_CREDENTIAL_AWARDED_NAME")) %>%
  filter(
    COSC_GRAD_STATUS_LGDS_CD_GROUP == "3",
    COCI_SUBM_CD %in% c("C_Outc19", "C_Outc20"),
    !is.na(Age_At_Grad), Age_At_Grad >= 17, Age_At_Grad <= 64
  ) %>%
  group_by(AGEGROUP, PRGM_CREDENTIAL_AWARDED_NAME, LCIP4_CRED, LCP4_CD,
            LCP4_CIP_4DIGITS_NAME, TTRAIN, COSC_GRAD_STATUS_LGDS_CD_GROUP) %>%
  summarise(Count = n(), .groups = "drop")

nc_stp_ttrain <- t_dacso_data_part_1 %>%
  inner_join(age_group_lookup, by = c("AGE_AT_GRAD" = "AGEINDEX")) %>%
  inner_join(credential_rank, by = c("PRGM_CREDENTIAL_AWARDED_NAME" = "PRGM_CREDENTIAL_AWARDED_NAME")) %>%
  inner_join(
    t_dacso_data_part_1_tempselection %>% select(COCI_STQU_ID, Has_STP_Credential),
    by = "COCI_STQU_ID"
  ) %>%
  filter(
    COSC_GRAD_STATUS_LGDS_CD_GROUP == "3",
    COCI_SUBM_CD %in% c("C_Outc19", "C_Outc20"),
    !is.na(Age_At_Grad), Age_At_Grad >= 17, Age_At_Grad <= 64,
    Has_STP_Credential == "Yes"
  ) %>%
  group_by(AGEGROUP, PRGM_CREDENTIAL_AWARDED_NAME, LCIP4_CRED, LCP4_CD,
            LCP4_CIP_4DIGITS_NAME, TTRAIN) %>%
  summarise(stp_count = n(), .groups = "drop")

T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN <- nc_ttrain %>%
  left_join(t_pssm_proj_cred_grp %>%
              select(PSSM_PROJECTION_CREDENTIAL, PSSM_CREDENTIAL),
            by = c("PRGM_CREDENTIAL_AWARDED_NAME" = "PSSM_PROJECTION_CREDENTIAL")) %>%
  left_join(nc_stp_ttrain,
    by = c("AGEGROUP", "PRGM_CREDENTIAL_AWARDED_NAME", "LCIP4_CRED", "LCP4_CD", "TTRAIN")
  ) %>%
  mutate(
    Near_completers_from_C_Outc19_20_with_earlier_or_later_STP = coalesce(stp_count, 0L),
    Near_completers_STP_Credentials = Count - Near_completers_from_C_Outc19_20_with_earlier_or_later_STP,
    PSSM_CRED = paste0(COSC_GRAD_STATUS_LGDS_CD_GROUP, " - ", PSSM_CREDENTIAL)
  ) %>%
  rename(age_group = AGEGROUP)

dbWriteTable(con, "T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN",
             T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN, overwrite = TRUE)


# ******************************************************************************
# PART M: TTRAIN TABLES (HISTORICAL)
# ******************************************************************************
# WHY: Same as Part L but broken out by survey year for historical analysis.
# Original: 3 SELECT INTO + 2 DROP TABLE
# Translated: Same aggregation with COCI_SUBM_CD in group_by.

nc_ttrain_hist <- t_dacso_data_part_1 %>%
  inner_join(age_group_lookup, by = c("AGE_AT_GRAD" = "AGEINDEX")) %>%
  inner_join(credential_rank, by = c("PRGM_CREDENTIAL_AWARDED_NAME" = "PRGM_CREDENTIAL_AWARDED_NAME")) %>%
  filter(
    COSC_GRAD_STATUS_LGDS_CD_GROUP == "3",
    !is.na(Age_At_Grad), Age_At_Grad >= 17, Age_At_Grad <= 64
  ) %>%
  group_by(COCI_SUBM_CD, AGEGROUP, PRGM_CREDENTIAL_AWARDED_NAME, LCIP4_CRED,
            LCP4_CD, LCP4_CIP_4DIGITS_NAME, TTRAIN, COSC_GRAD_STATUS_LGDS_CD_GROUP) %>%
  summarise(Count = n(), .groups = "drop")

nc_stp_ttrain_hist <- t_dacso_data_part_1 %>%
  inner_join(age_group_lookup, by = c("AGE_AT_GRAD" = "AGEINDEX")) %>%
  inner_join(credential_rank, by = c("PRGM_CREDENTIAL_AWARDED_NAME" = "PRGM_CREDENTIAL_AWARDED_NAME")) %>%
  inner_join(
    t_dacso_data_part_1_tempselection %>% select(COCI_STQU_ID, Has_STP_Credential),
    by = "COCI_STQU_ID"
  ) %>%
  filter(
    COSC_GRAD_STATUS_LGDS_CD_GROUP == "3",
    !is.na(Age_At_Grad), Age_At_Grad >= 17, Age_At_Grad <= 64,
    Has_STP_Credential == "Yes"
  ) %>%
  group_by(COCI_SUBM_CD, AGEGROUP, PRGM_CREDENTIAL_AWARDED_NAME, LCIP4_CRED,
            LCP4_CD, LCP4_CIP_4DIGITS_NAME, TTRAIN) %>%
  summarise(stp_count = n(), .groups = "drop")

T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history <- nc_ttrain_hist %>%
  left_join(t_pssm_proj_cred_grp %>%
              select(PSSM_PROJECTION_CREDENTIAL, PSSM_CREDENTIAL),
            by = c("PRGM_CREDENTIAL_AWARDED_NAME" = "PSSM_PROJECTION_CREDENTIAL")) %>%
  left_join(nc_stp_ttrain_hist,
    by = c("COCI_SUBM_CD", "AGEGROUP", "PRGM_CREDENTIAL_AWARDED_NAME",
           "LCIP4_CRED", "LCP4_CD", "TTRAIN")
  ) %>%
  mutate(
    Near_completers_from_C_Outc19_20_with_earlier_or_later_STP = coalesce(stp_count, 0L),
    Near_completers_STP_Credentials = Count - Near_completers_from_C_Outc19_20_with_earlier_or_later_STP,
    PSSM_CRED = paste0(COSC_GRAD_STATUS_LGDS_CD_GROUP, " - ", PSSM_CREDENTIAL)
  ) %>%
  rename(age_group = AGEGROUP)

dbWriteTable(con, "T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history",
             T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history, overwrite = TRUE)


# ******************************************************************************
# FINAL CLEANUP
# ******************************************************************************
# Drop lookup tables no longer needed
# KEPT AS SQL: DROP TABLE (cleanup)
dbExecute(con, "DROP TABLE IF EXISTS stp_dacso_prgm_credential_lookup")
dbExecute(con, "DROP TABLE IF EXISTS tbl_Age")
dbExecute(con, "DROP TABLE IF EXISTS AgeGroupLookup")
dbExecute(con, "DROP TABLE IF EXISTS combine_creds")
dbExecute(con, "DROP TABLE IF EXISTS t_pssm_projection_cred_grp")

# Write the matching table back (needed by downstream scripts)
dbWriteTable(con, "DACSO_Matching_STP_Credential_PEN",
             dacso_matching_stp_credential_pen, overwrite = TRUE)

dbDisconnect(con)



# ==============================================================================
# FILE: 04-graduate-projections_dplyr.R
# ==============================================================================


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

# This is a dplyr translation of R/04-graduate-projections.R.
# Database reads use dbplyr's tbl() with in_schema() for lazy evaluation and
# explicit collect() calls with rename_with(toupper) for column name consistency.
# All R-side data processing logic (lm, predict, pivot, joins, mutations, etc.)
# is unchanged from the original. dbWriteTable calls remain as-is.

# Pipeline context:
#   This is the core forecasting engine for the PSSM model. It projects future
#   graduate numbers by credential type, age group, and gender using:
#     1. Population projections (from BC Stats)
#     2. Historical enrolment rates (from STP enrolment data)
#     3. Historical graduation rates (from credential data)
#
#   The methodology:
#     - Fit linear models to historical enrolment rates (2002–2022)
#     - Forecast rates for 5 years, then hold constant for 7 more years (12 total)
#     - Apply forecasted rates to population projections → forecasted enrolments
#     - Apply 2-year average graduation rates → forecasted graduates
#     - Distribute graduates across credential categories using historical proportions
#     - Add near-completers (from DACSO ratios) and apprenticeship graduates (from APPSO)
#
#   Output feeds into:
#     - 06-program-projections (distributes graduates across CIP programs)
#     - 07-occupation-projections (maps CIP programs to occupations)
#     - 08-create-final-reports (Excel workbooks)

# Notes: Development\Graduate Model\Enrollment & Graduation Projections 2019-2020 PEOPLE 2020.xlsm (2019) and documentation reveal different #'s
# of output years.  Used 12 for PSSM2023.
# Script handles only Male and Female


library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(RJDBC)
library(dbplyr)

# Helper: reference a table in the user's schema via dbplyr
sch_tbl <- function(name) { tbl(decimal_con, dbplyr::in_schema(my_schema, name)) }

# ---- Configure LAN and file paths ----
db_config <- config::get("decimal")
lan <- config::get("lan")
my_schema <- config::get("myschema")

# Note: SQL repeated in sql/01.  Delete one copy after merging 2023 run.
# ---- Connection to decimal ----
db_config <- config::get("decimal")
decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")

# ---- Check for required data tables ----
assert_that(dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."population_projections"'))),
            msg = paste("Error:", table_name, "does not exist in schema", my_schema)
)

# ---- Read data from decimal ----
population_projections <- sch_tbl("population_projections") %>% collect() |> rename_with(toupper)
min_enrolments <- sch_tbl("qry09c_MinEnrolment") %>% collect() |> rename_with(toupper)
credentials <- sch_tbl("Credential_By_Year_Gender_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs") %>% collect() |> rename_with(toupper)

# ---- Tidy data for calculations  ----
population_projections <- population_projections %>%
  select(-c(REGION, LOCAL_HEALTH_AREA, TOTAL)) %>%
  pivot_longer(cols = starts_with("X"),  names_to = "AGE_GROUP", values_to = "POP") %>%
  filter(GENDER %in% c("F", "M")) %>%
  mutate(GENDER = case_when(GENDER == 'F' ~ 'Female', GENDER == 'M' ~ 'Male', TRUE ~ NA)) %>%
  mutate(AGE_GROUP = gsub("X", "", AGE_GROUP)) %>%
  mutate(AGE_GROUP = gsub("_TO_", " to ", AGE_GROUP)) %>%
  select(-TYPE)

min_enrolments <- min_enrolments %>%
  rename("AGE_GROUP" = "GROUPS", "N" = "EXPR1", "GENDER" = "PSI_GENDER", "YEAR" = "PSI_SCHOOL_YEAR") %>%
  mutate(AGE_GROUP = gsub("Female|Male|Gender Diverse Gender Diverse", "", AGE_GROUP)) %>%
  mutate(YEAR = as.numeric(stringr::str_sub(YEAR, 1, 4))) %>%
  arrange(GENDER, AGE_GROUP, YEAR)

credentials <- credentials %>%
  rename("AGE_GROUP" = "AGEGROUP", "N" = "COUNT",
         "GENDER" = "PSI_GENDER_CLEANED", "YEAR" = "PSI_AWARD_SCHOOL_YEAR_DELAYED") %>%
  mutate(YEAR = as.numeric(stringr::str_sub(YEAR, 1, 4))) %>%
  select(-EXPR1) %>%
  filter(YEAR >=2006, YEAR <=2022)

# ---- Forecasted Enrolments ----
# Calculate the enrolment rate (enrolment / population) for each age group and
# gender over time. This rate will be forecasted forward to predict future enrolments.
## Enrolment Rate ----
p_enrolments <- min_enrolments %>%
  inner_join(population_projections, by = join_by(GENDER, AGE_GROUP, YEAR)) %>% # removes Gender Diverse
  mutate(P = 100*N/POP)

p_enrolments %>%
  mutate(P=round(P,2)) %>%
  pivot_wider(id_cols = c(GENDER, AGE_GROUP), values_from = P, names_from = YEAR) %>%
  View()

# Fit linear models to historical enrolment rates and project forward 12 years.
# The first 5 years use the linear trend; the remaining 7 hold the final rate constant.
# workbook forecasting done for 12 years
## Forecasted Enrolment Rate ----
f_enrolments <- p_enrolments |>
  split(list(p_enrolments$AGE_GROUP, p_enrolments$GENDER), drop=TRUE, sep = "_") |>
  map(\(df) lm(P ~ YEAR, data = df)) |>
  map(predict.lm,
      newdata = data.frame(YEAR = c(2023:2027,rep(2027,7)),
                           row.names = as.character(2023:2034)))

# Apply the forecasted enrolment rates to the projected population to get
# the actual number of forecasted enrolled students by age/gender/year.
## Forecasted Enrolments ----
rn <- as.numeric(rownames(data.frame(f_enrolments)))
f_enrolments_t <- data.frame(f_enrolments) %>%
  mutate(YEAR = rn) %>%
  pivot_longer(cols = c(-YEAR), values_to = "RATE") %>%
  separate_wider_delim(cols = name, delim = "_", names = c("AGE_GROUP", "GENDER")) %>%
  mutate(AGE_GROUP = gsub("\\.", " ", AGE_GROUP)) %>%
  mutate(AGE_GROUP = gsub("X", "", AGE_GROUP))

f_enrolments_t  <- f_enrolments_t %>%
  inner_join(population_projections, by = join_by(YEAR, AGE_GROUP, GENDER)) %>%
  mutate(N_ENROL_FORECASTED = RATE*POP*.01)

f_enrolments_t %>%
  pivot_wider(id_cols = c(AGE_GROUP, GENDER), values_from = N_ENROL_FORECASTED, names_from = YEAR)

# HISTORICAL - Pop/Enrolments ----
# WHY: Combine historical and forecasted enrolments for comparison plots
# grab historical data and append
historical_forecasted_enrolments <- tibble(p_enrolments %>%
                                             select(YEAR, AGE_GROUP, GENDER, N=N) %>%
                                             mutate(TYPE='H. ENROLMENT') %>%
                                             bind_rows(f_enrolments_t %>%
                                                         select(YEAR, AGE_GROUP, GENDER, N=N_ENROL_FORECASTED) %>%
                                                         mutate(TYPE='F. ENROLMENT')))

pop_projections_for_compare <- population_projections %>%
  mutate(N = POP) %>%
  select(YEAR, AGE_GROUP, GENDER, N) %>%
  mutate(TYPE='POPULATION') %>%
  filter(YEAR<2035)

# ---- Forecasted Graduates ----
## Graduation Rates (annual, as a percentage of enrolment) ----
# WHY: Calculate the ratio of graduates to enrolled students for each age/gender/year.
# This is the basis for projecting future graduate numbers.
annual_grad_rate <- credentials %>%
  summarize(N_GRADS = sum(N, na.rm = TRUE), .by = c(GENDER, AGE_GROUP, YEAR)) %>%
  inner_join( # removes Gender Diverse
    min_enrolments %>%
      summarize(N_ENROL = sum(N, na.rm = TRUE), .by = c(GENDER, AGE_GROUP, YEAR)),
    by = join_by(GENDER, AGE_GROUP, YEAR)) %>%
  mutate(P_GRADS_ENROL = 100*N_GRADS/N_ENROL)

## Graduation Rate (2-yr average, as percentage of enrolment) ----
# WHY: Use a 2-year average (2021-2022) graduation rate to smooth out year-to-year
# fluctuations. This is applied to forecasted enrolments to get forecasted graduates.
avg_2_yr_grad_rate <- annual_grad_rate %>%
  filter(YEAR %in% 2021:2022) %>%
  summarise(GRAD_RATE = sum(N_GRADS)/sum(N_ENROL),
            .by  = c(GENDER, AGE_GROUP))

f_graduates_t <- f_enrolments_t %>%
  inner_join(avg_2_yr_grad_rate, by = join_by(AGE_GROUP, GENDER)) %>%
  mutate(N_GRAD_FORECASTED = N_ENROL_FORECASTED * GRAD_RATE)

## Forecasted Graduates by Credential ----
# WHY: The total forecasted graduates need to be split across credential categories
# (Bachelor's, Diploma, Certificate, etc.) using historical proportions.
f_graduates_t  <- f_graduates_t  %>%
  select(YEAR, AGE_GROUP, GENDER, N_GRAD_FORECASTED)


# HISTORICAL - grads ----
# WHY: Combine historical and forecasted graduates for comparison plots
historical_forecasted_grads <- annual_grad_rate %>%
  select(YEAR, AGE_GROUP, GENDER, N=N_GRADS) %>%
  mutate(TYPE = 'H. GRADS') %>%
  bind_rows(
    f_graduates_t %>%
      select(YEAR, AGE_GROUP, GENDER, N=N_GRAD_FORECASTED) %>%
      mutate(TYPE = 'F. GRADS')
  )

## 2-yr average distribution of graduates by credential ----
# WHY: Determine what proportion of graduates in each age/gender group earns each
# credential type. This proportion is applied to forecasted totals.
avg_2_yr_credentials <- credentials %>%
  filter(YEAR %in% 2021:2022, GENDER != 'Gender Diverse') %>%
  summarise(YR_2_N = sum(N), .by = c(GENDER, AGE_GROUP, PSI_CREDENTIAL_CATEGORY)) %>%
  group_by(GENDER, AGE_GROUP) %>%
  mutate(N=sum(YR_2_N),
         P = round(YR_2_N/N,3)) %>%
  ungroup() %>%
  complete(GENDER, AGE_GROUP, PSI_CREDENTIAL_CATEGORY, fill = list(YR_2_N=0,N=0,P=0)) %>%
  select(AGE_GROUP, GENDER, PSI_CREDENTIAL_CATEGORY, P)

f_graduates <- f_graduates_t  %>%
  full_join(avg_2_yr_credentials, relationship = "many-to-many") %>%
  mutate(N_GRAD_FORECASTED = N_GRAD_FORECASTED*P) %>%
  select(-P) %>%
  summarize(N=sum(N_GRAD_FORECASTED), .by  = c(PSI_CREDENTIAL_CATEGORY, YEAR, AGE_GROUP, GENDER))

## HISTORICAL - combined GRAD CRED ----
historical_forecasted_grad_creds <-
  credentials %>%
  select(YEAR, AGE_GROUP, GENDER, PSI_CREDENTIAL_CATEGORY, N) %>%
  mutate(TYPE = 'H. GRADS by Cred') %>%
  bind_rows(
    f_graduates %>%
      select(YEAR, AGE_GROUP, GENDER, PSI_CREDENTIAL_CATEGORY, N) %>%
      mutate(TYPE = 'F. GRADS by Cred')
  )

# PLOT INTERLUDE ----
# get things on same ish scale
library(ggplot2)
min_ns <- historical_forecasted_enrolments %>%
  bind_rows(pop_projections_for_compare) %>%
  bind_rows(historical_forecasted_grads) %>%
  bind_rows(historical_forecasted_grad_creds) %>%
  mutate(GROUP = case_when(
    TYPE=='POPULATION' ~ TYPE,
    TRUE ~ str_sub(TYPE, start=4)
    )
  ) %>%
  group_by(YEAR, GROUP) %>%
  mutate(n = sum(N)) %>%
  ungroup() %>%
  group_by(GROUP) %>%
  mutate(min_n = min(n)) %>%
  ungroup() %>%
  distinct(GROUP, TYPE, min_n)

historical_forecasted_enrolments %>%
  bind_rows(pop_projections_for_compare) %>%
  bind_rows(historical_forecasted_grads) %>%
  #bind_rows(historical_forecasted_grad_creds) %>%
  left_join(min_ns, by = 'TYPE') %>%
  group_by(YEAR, GROUP) %>%
  summarize(n = sum(N), min_n=min(min_n)) %>%  # View()
  mutate(
    n = n/min_n
  ) %>%
  ggplot(aes(x=YEAR, y=n, color=GROUP)) +
  geom_line(linewidth=1) +
  geom_vline(aes(xintercept = 2023))

# Near-completers are individuals who started but didn't complete a credential.
# They represent potential future labour supply. DACSO provides ratios that estimate
# how many near-completers in each credential/age/gender group are likely to complete.
# preprocess nc data before joining with graduates
# ---- Projected Near Completers (NC) ----
T_DACSO_Near_Completers_RatioByGender <- sch_tbl("T_DACSO_Near_Completers_RatioByGender") %>% collect() %>%
  rename_with(toupper) %>%
  janitor::clean_names("all_caps") %>%
  mutate(PSI_CREDENTIAL_CATEGORY = PRGM_CREDENTIAL_AWARDED_NAME) %>%
  select(PSI_CREDENTIAL_CATEGORY, AGE_GROUP, GENDER, RATIO) %>%
  mutate(GENDER = if_else(GENDER == 1, 'Male', 'Female'))

# recode age groups 35-44, 45-54, 55-64  => 35 to 64
# WHY: Near-completer data uses coarser age groups than the graduation data for
# older age brackets. We need to map the finer groups to the coarser ones for joining.
T_DACSO_Near_Completers_RatioByGender <- f_graduates %>%
  distinct(AGE_GROUP) %>%
  rename("AGE_GROUP_RECODE" = "AGE_GROUP") %>%
  mutate(AGE_GROUP = if_else(AGE_GROUP_RECODE %in% c("35 to 44", "45 to 54", "55 to 64"), "35 to 64", AGE_GROUP_RECODE)) %>%
  full_join(T_DACSO_Near_Completers_RatioByGender, relationship = "many-to-many") %>%
  select(-AGE_GROUP) %>%
  rename("AGE_GROUP" = "AGE_GROUP_RECODE")

# join nc to graduates
# WHY: Apply the near-completer ratio to the forecasted graduate totals to get
# the additional supply from near-completers who are projected to complete.
f_graduates_nc <- f_graduates %>%
  inner_join(T_DACSO_Near_Completers_RatioByGender) %>%
  mutate(N=N*RATIO) %>%
  select(-RATIO)

# HISTORICAL - near-completers ----
# note that near-completer data (right now) only goes back 5 years to 2019
# will want to make sure that comparisons only go that far back
# could in theory go farther, just easier right now to stay at 2019
# preprocess nc data before joining with graduates
T_DACSO_Near_Completers_RatioByGender_year <- sch_tbl("T_DACSO_Near_Completers_RatioByGender_year") %>% collect() %>%
  rename_with(toupper) %>%
  janitor::clean_names("all_caps") %>%
  mutate(PSI_CREDENTIAL_CATEGORY = PRGM_CREDENTIAL_AWARDED_NAME) %>%
  select(YEAR, PSI_CREDENTIAL_CATEGORY, AGE_GROUP, GENDER, N_NC_STP) %>%
  mutate(GENDER = if_else(GENDER == 1, 'Male', 'Female'))

# recode age groups 35-44, 45-54, 55-64  => 35 to 64
T_DACSO_Near_Completers_RatioByGender_year <- f_graduates %>%
  distinct(AGE_GROUP) %>%
  rename("AGE_GROUP_RECODE" = "AGE_GROUP") %>%
  mutate(AGE_GROUP = if_else(AGE_GROUP_RECODE %in% c("35 to 44", "45 to 54", "55 to 64"), "35 to 64", AGE_GROUP_RECODE)) %>%
  full_join(T_DACSO_Near_Completers_RatioByGender_year, relationship = "many-to-many") %>%
  select(-AGE_GROUP) %>%
  rename("AGE_GROUP" = "AGE_GROUP_RECODE")

f_graduates_nc_historical <- T_DACSO_Near_Completers_RatioByGender_year %>%
  select(PSI_CREDENTIAL_CATEGORY, YEAR, AGE_GROUP, GENDER, N=N_NC_STP) %>%
  filter(YEAR < 2023)

historical_forecasted_grad_ncs <-
  f_graduates_nc_historical %>%
  select(YEAR, AGE_GROUP, GENDER, PSI_CREDENTIAL_CATEGORY, N) %>%
  mutate(TYPE = 'H. NCs') %>%
  bind_rows(
    f_graduates_nc %>%
      select(YEAR, AGE_GROUP, GENDER, PSI_CREDENTIAL_CATEGORY, N) %>%
      mutate(TYPE = 'F. NCs')
  )

# PLOT ----
f_graduates_nc %>%
  bind_rows(f_graduates_nc_historical) %>%
  group_by(YEAR) %>%
  summarize(n = sum(N)) %>%
  ggplot(aes(x=YEAR,y=n)) +
  geom_line() + geom_point()

# WHY: Map credential categories to PSSM credential groups for the final output.
# These groups are used throughout the downstream pipeline (scripts 06, 07, 08).
# make pssm cred label
f_graduates <- f_graduates %>%
  filter(!AGE_GROUP %in% c("15 to 16")) %>%
  mutate(PSSM_CRED = case_when(
         toupper(PSI_CREDENTIAL_CATEGORY) == "ADVANCED CERTIFICATE" ~ "1 - ADCT OR ADIP",
         toupper(PSI_CREDENTIAL_CATEGORY) == "ASSOCIATE DEGREE" ~ "1 - ADGR OR UT",
         toupper(PSI_CREDENTIAL_CATEGORY) == "ADVANCED DIPLOMA" ~ "1 - ADCT OR ADIP",
         toupper(PSI_CREDENTIAL_CATEGORY) == "BACHELORS DEGREE" ~ "BACH",
         toupper(PSI_CREDENTIAL_CATEGORY) == "CERTIFICATE" ~ "1 - CERT",
         toupper(PSI_CREDENTIAL_CATEGORY) == "DIPLOMA" ~ "1 - DIPL",
         toupper(PSI_CREDENTIAL_CATEGORY) == "DOCTORATE" ~ "DOCT",
         toupper(PSI_CREDENTIAL_CATEGORY) == "GRADUATE CERTIFICATE" ~ "GRCT OR GRDP",
         toupper(PSI_CREDENTIAL_CATEGORY) == "GRADUATE DIPLOMA" ~ "GRCT OR GRDP",
         toupper(PSI_CREDENTIAL_CATEGORY) == "MASTERS DEGREE" ~ "MAST",
         toupper(PSI_CREDENTIAL_CATEGORY) == "NONE" ~ "INVALID",
         toupper(PSI_CREDENTIAL_CATEGORY) == "OTHER" ~ "",
         toupper(PSI_CREDENTIAL_CATEGORY) == "POST-DEGREE CERTIFICATE" ~ "1 - PDCT OR PDDP",
         toupper(PSI_CREDENTIAL_CATEGORY) == "POST-DEGREE DIPLOMA" ~ "1 - PDCT OR PDDP",
         toupper(PSI_CREDENTIAL_CATEGORY) == "FIRST PROFESSIONAL DEGREE" ~ "PDEG",
         toupper(PSI_CREDENTIAL_CATEGORY) == "SHORT CERTIFICATE" ~ "INVALID",
         toupper(PSI_CREDENTIAL_CATEGORY) == "UNIVERSITY TRANSFER" ~ "1 - ADGR OR UT",
         TRUE ~ NA))


# WHY: Near-completers get a different PSSM credential prefix ("3 - ") to distinguish
# them from regular graduates ("1 - ") in the final output tables.
# update pssm cred label for nc
f_graduates_nc <- f_graduates_nc %>%
  filter(!AGE_GROUP %in% c("15 to 16")) %>%
  mutate(PSSM_CRED = case_when(
    toupper(PSI_CREDENTIAL_CATEGORY) == "ASSOCIATE DEGREE" ~ "3 - ADGR OR UT",
    toupper(PSI_CREDENTIAL_CATEGORY) == "ADVANCED DIPLOMA" ~ "3 - ADCT OR ADIP",
    toupper(PSI_CREDENTIAL_CATEGORY) == "CERTIFICATE" ~ "3 - CERT",
    toupper(PSI_CREDENTIAL_CATEGORY) == "DIPLOMA" ~ "3 - DIPL",
    toupper(PSI_CREDENTIAL_CATEGORY) == "POST-DEGREE CERTIFICATE" ~ "3 - PDCT OR PDDP",
    toupper(PSI_CREDENTIAL_CATEGORY) == "POST-DEGREE DIPLOMA" ~ "3 - PDCT OR PDDP",
    toupper(PSI_CREDENTIAL_CATEGORY) == "UNIVERSITY TRANSFER" ~ "3 - ADGR OR UT",
    TRUE ~ "NA"))

# WHY: Combine regular graduates and near-completers, aggregate by PSSM credential
# group, and format years for the output table.
f_graduates_agg <- f_graduates %>% rbind(f_graduates_nc) %>%
  group_by(PSSM_CRED, YEAR, AGE_GROUP) %>%
  summarise(GRADUATES=sum(N)) %>%
  mutate(SURVEY = 'Credential_Projections_Transp') %>%
  mutate(YEAR = paste0(as.character(YEAR), "/", as.character(YEAR + 1))) %>%
  filter(!AGE_GROUP %in% c('65 to 89','15 to 16'))

# HISTORICAL - Update PSSM CRED LABEL for GRADS/NCs ----
hf_grad_creds <- historical_forecasted_grad_creds %>%
  filter(!AGE_GROUP %in% c("15 to 16")) %>%
  mutate(PSSM_CRED = case_when(
    toupper(PSI_CREDENTIAL_CATEGORY) == "ADVANCED CERTIFICATE" ~ "1 - ADCT OR ADIP",
    toupper(PSI_CREDENTIAL_CATEGORY) == "ASSOCIATE DEGREE" ~ "1 - ADGR OR UT",
    toupper(PSI_CREDENTIAL_CATEGORY) == "ADVANCED DIPLOMA" ~ "1 - ADCT OR ADIP",
    toupper(PSI_CREDENTIAL_CATEGORY) == "BACHELORS DEGREE" ~ "BACH",
    toupper(PSI_CREDENTIAL_CATEGORY) == "CERTIFICATE" ~ "1 - CERT",
    toupper(PSI_CREDENTIAL_CATEGORY) == "DIPLOMA" ~ "1 - DIPL",
    toupper(PSI_CREDENTIAL_CATEGORY) == "DOCTORATE" ~ "DOCT",
    toupper(PSI_CREDENTIAL_CATEGORY) == "GRADUATE CERTIFICATE" ~ "GRCT OR GRDP",
    toupper(PSI_CREDENTIAL_CATEGORY) == "GRADUATE DIPLOMA" ~ "GRCT OR GRDP",
    toupper(PSI_CREDENTIAL_CATEGORY) == "MASTERS DEGREE" ~ "MAST",
    toupper(PSI_CREDENTIAL_CATEGORY) == "NONE" ~ "INVALID",
    toupper(PSI_CREDENTIAL_CATEGORY) == "OTHER" ~ "",
    toupper(PSI_CREDENTIAL_CATEGORY) == "POST-DEGREE CERTIFICATE" ~ "1 - PDCT OR PDDP",
    toupper(PSI_CREDENTIAL_CATEGORY) == "POST-DEGREE DIPLOMA" ~ "1 - PDCT OR PDDP",
    toupper(PSI_CREDENTIAL_CATEGORY) == "FIRST PROFESSIONAL DEGREE" ~ "PDEG",
    toupper(PSI_CREDENTIAL_CATEGORY) == "SHORT CERTIFICATE" ~ "INVALID",
    toupper(PSI_CREDENTIAL_CATEGORY) == "UNIVERSITY TRANSFER" ~ "1 - ADGR OR UT",
    TRUE ~ NA))

hf_nc <- historical_forecasted_grad_ncs %>%
  filter(!AGE_GROUP %in% c("15 to 16")) %>%
  mutate(PSSM_CRED = case_when(
    toupper(PSI_CREDENTIAL_CATEGORY) == "ASSOCIATE DEGREE" ~ "3 - ADGR OR UT",
    toupper(PSI_CREDENTIAL_CATEGORY) == "ADVANCED DIPLOMA" ~ "3 - ADCT OR ADIP",
    toupper(PSI_CREDENTIAL_CATEGORY) == "CERTIFICATE" ~ "3 - CERT",
    toupper(PSI_CREDENTIAL_CATEGORY) == "DIPLOMA" ~ "3 - DIPL",
    toupper(PSI_CREDENTIAL_CATEGORY) == "POST-DEGREE CERTIFICATE" ~ "3 - PDCT OR PDDP",
    toupper(PSI_CREDENTIAL_CATEGORY) == "POST-DEGREE DIPLOMA" ~ "3 - PDCT OR PDDP",
    toupper(PSI_CREDENTIAL_CATEGORY) == "UNIVERSITY TRANSFER" ~ "3 - ADGR OR UT",
    TRUE ~ "NA"))

hf_grad_nc_creds_agg <- hf_grad_creds %>% rbind(hf_nc) %>%
  group_by(PSSM_CRED, YEAR, AGE_GROUP) %>%
  summarise(GRADUATES=sum(N)) %>%
  mutate(TYPE = if_else(YEAR<2023, 'H. Total G NC by Cred', 'F. Total G NC By Cred')) %>%
  mutate(SURVEY = 'Credential_Projections_Transp') %>%
  mutate(YEAR = paste0(as.character(YEAR), "/", as.character(YEAR + 1))) %>%
  filter(!AGE_GROUP %in% c('65 to 89','15 to 16'))

# PLOT comparison ----
# get things on same ish scale
library(ggplot2)
min_ns <- historical_forecasted_enrolments %>%
  bind_rows(pop_projections_for_compare) %>%
  bind_rows(historical_forecasted_grads) %>%
  bind_rows(historical_forecasted_grad_creds) %>%
  bind_rows(historical_forecasted_grad_ncs) %>%
  mutate(GROUP = case_when(
    TYPE=='POPULATION' ~ TYPE,
    TRUE ~ str_sub(TYPE, start=4)
  )
  ) %>%
  group_by(YEAR, GROUP) %>%
  mutate(n = sum(N)) %>%
  ungroup() %>%
  group_by(GROUP) %>%
  mutate(min_n = min(n)) %>%
  ungroup() %>%
  distinct(GROUP, TYPE, min_n)

historical_forecasted_enrolments %>%
  bind_rows(pop_projections_for_compare) %>%
  #bind_rows(historical_forecasted_grads) %>%
  bind_rows(historical_forecasted_grad_creds) %>%
  bind_rows(historical_forecasted_grad_ncs) %>%
  left_join(min_ns, by = 'TYPE') %>%
  group_by(YEAR, GROUP) %>%
  summarize(n = sum(N), min_n=min(min_n)) %>%  # View()
  mutate(
    n = n/min_n
  ) %>%
  ggplot(aes(x=YEAR, y=n, color=GROUP)) +
  geom_line(linewidth=1) +
  geom_vline(aes(xintercept = 2023))


# Apprenticeship (APPSO) graduates are handled separately from the main model
# because they don't follow the same enrolment-rate methodology. Instead, we use
# a simple 2-year average of historical APPSO graduates as the projection.
# ---- Graduate Projections for Apprenticeship ----
APPSO_Graduates <- sch_tbl("APPSO_Graduates") %>% collect() |> rename_with(toupper)

appso_2_yr_avg <- APPSO_Graduates %>%
  mutate(YEAR = str_replace(SUBM_CD, "C_Outc", "20")) %>%
  rename("N" = "EXPR1", "PSSM_CRED" = "PSSM_CREDENTIAL") %>%
  summarize(N = sum(N, na.rm = TRUE), .by = c(YEAR, PSSM_CRED, AGE_GROUP)) %>%
  filter(YEAR %in% c('2022','2023')) %>%
  summarize(GRADUATES = sum(N/2, na.rm = TRUE), .by = c(PSSM_CRED, AGE_GROUP)) %>%
  mutate(YEAR = "2023/2024") %>%
  mutate(SURVEY = 'APPSO') %>%
  mutate(PSI_CREDENTIAL_CATEGORY = "NA") %>%
  filter(!is.na(AGE_GROUP)) %>%
  filter(!AGE_GROUP %in% c('65 to 89','15 to 16'))

# WHY: Append APPSO graduates to the credential-based projections for a complete picture.
# All grad data, forecast
f_graduates_agg <- f_graduates_agg %>%
  rbind(appso_2_yr_avg) %>%
  mutate(PSSM_CREDENTIAL = gsub("(1 - )|(3 - )", "", PSSM_CRED))

# fix APPSO forward projection to go to the end
# done in 06 - not needed here
# f_graduates_agg <- f_graduates_agg %>%
#   ungroup() %>%
#   arrange(PSSM_CREDENTIAL, AGE_GROUP, YEAR) %>%
#   complete(PSSM_CREDENTIAL, AGE_GROUP, YEAR) %>%
#   group_by(PSSM_CREDENTIAL, AGE_GROUP) %>%
#   fill(GRADUATES)

# HISTORICAL - APPSO ----
appso_historical <- APPSO_Graduates %>%
  mutate(YEAR = as.numeric(str_replace(SUBM_CD, "C_Outc", "20"))) %>%
  filter(YEAR<2023) %>%
  rename("N" = "EXPR1", "PSSM_CRED" = "PSSM_CREDENTIAL") %>%
  summarize(GRADUATES = sum(N, na.rm = TRUE), .by = c(YEAR, PSSM_CRED, AGE_GROUP)) %>%
  mutate(YEAR = paste0(as.character(YEAR), "/", as.character(YEAR + 1))) %>%
  mutate(SURVEY = 'APPSO') %>%
  mutate(PSI_CREDENTIAL_CATEGORY = "NA") %>%
  filter(!is.na(AGE_GROUP)) %>%
  filter(!AGE_GROUP %in% c('65 to 89','15 to 16'))

historical_forecast_appso <-
  appso_historical %>%
  select(YEAR, PSSM_CRED, AGE_GROUP, GRADUATES, SURVEY, PSI_CREDENTIAL_CATEGORY) %>%
  mutate(TYPE = 'H. APPSO') %>%
  bind_rows(
    appso_2_yr_avg %>%
      select(YEAR, PSSM_CRED, AGE_GROUP, GRADUATES, SURVEY, PSI_CREDENTIAL_CATEGORY) %>%
      mutate(TYPE = 'F. APPSO')
  )

# HISTORICAL - All grad data ----
hf_grad_nc_appso_agg <- hf_grad_nc_creds_agg %>%
  rbind(historical_forecast_appso) %>%
  mutate(PSSM_CREDENTIAL = gsub("(1 - )|(3 - )", "", PSSM_CRED))

# fix APPSO forward projection to go to the end
# done in 06, not needed here
# hf_grad_nc_appso_agg <- hf_grad_nc_appso_agg %>%
#   ungroup() %>%
#   arrange(PSSM_CREDENTIAL, AGE_GROUP, YEAR) %>%
#   complete(PSSM_CREDENTIAL, AGE_GROUP, YEAR) %>%
#   group_by(PSSM_CREDENTIAL, AGE_GROUP) %>%
#   fill(GRADUATES)

# SAVE TO SQL DATABASE ----
dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."Graduate_Projections"')), f_graduates_agg, overwrite = TRUE)

# SAVE Historical ----
dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."Graduate_Projections_Include_Historical"')), hf_grad_nc_appso_agg, overwrite = TRUE)

# ---- Graduate Projections for Trades ----
# TODO: add in trades to Graduate Projections (above) and project same as APPSO.
TRD_Graduates <- sch_tbl("TRD_Graduates") %>% collect() |> rename_with(toupper)



# ==============================================================================
# FILE: 05-ptib-analysis_dplyr.R
# ==============================================================================


# PTIB Analysis — dplyr Translation
# Original: R/05-ptib-analysis.R
#
# Pipeline context:
#   Processes Private Training Institutions Branch (PTIB) data for inclusion in the
#   PSSM model. PTIB credentials are private institution graduates that need to be
#   integrated with the public institution data processed by the main pipeline.
#
#   This script runs after the main graduate projections (04) and before the program
#   projections (06). It produces two outputs:
#     1. PTIB graduate data appended to Graduate_Projections (for 04)
#     2. PTIB cohort distributions for program projections (for 06)
#
# Input tables:
#   - T_Private_Institutions_Credentials_Raw — raw PTIB data (from load-ptib.R)
#   - T_PSSM_Credential_Grouping — maps PTIB credential names to PSSM categories
#   - INFOWARE_L_CIP_6DIGITS_CIP2016 — official CIP taxonomy for validation
#   - T_PTIB_Y1_to_Y10 — year lookup for projection year expansion
#
# Output:
#   - qry_Private_Credentials_05i1_Grads_by_Year — PTIB grads by year (kept in DB)
#   - qry_Private_Credentials_06d1_Cohort_Dist — PTIB cohort distributions (kept in DB)
#   - Rows appended to Graduate_Projections

library(RODBC)
library(arrow)
library(tidyverse)
library(dbplyr)
library(odbc)
library(RJDBC) ## loads DBI

# ---- Configure LAN and file paths ----
lan <- config::get("lan")
my_schema <- config::get("myschema")

# ---- Connection to database ----
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

# ---- Required data tables ----
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_PSSM_Credential_Grouping"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_Private_Institutions_Credentials_Raw"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_PTIB_Y1_to_Y10"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_6DIGITS_CIP2016"')))

# Pull all source tables into R for processing.
pssm_cred_grouping <- sch_tbl("T_PSSM_Credential_Grouping") %>%
  collect() |> rename_with(toupper)

ptib_raw <- sch_tbl("T_Private_Institutions_Credentials_Raw") %>%
  collect() |> rename_with(toupper)

ptib_y1_to_y10 <- sch_tbl("T_PTIB_Y1_to_Y10") %>%
  collect() |> rename_with(toupper)

cip6 <- sch_tbl("INFOWARE_L_CIP_6DIGITS_CIP2016") %>%
  select(LCIP_CD, LCIP_NAME) %>%
  collect() |> rename_with(toupper)

# ******************************************************************************
# Part 1: Clean PTIB data
# * Map PTIB credential names to PSSM credential categories
# * Clean CIP codes (remove periods, validate against INFOWARE)
# * Flag excluded programs (ESL, not-for-credit, unclassified)
# * Recode age groups
# * Compute 2-year averages of graduates/enrolments
# ******************************************************************************

# Join PTIB raw data with PSSM credential grouping to get standardized credential names.
# WHY: PTIB reports credential names that don't match the PSSM taxonomy. The grouping
# table maps each institution-specific name to a PSSM category (e.g., "Certificate" → "CERT").
# Records with no match (PSSM_Credential IS NULL) or "None" credentials are excluded.
# ---- qry_Private_Credentials_00a_Append ----
ptib_creds <- ptib_raw %>%
  inner_join(pssm_cred_grouping,
    by = c("CREDENTIAL" = "PRGM_CREDENTIAL_AWARDED_NAME")) %>%
  filter(!is.na(PSSM_CREDENTIAL), CREDENTIAL != "None") %>%
  transmute(
    INTYEAR = YEAR,
    CREDENTIAL = PSSM_CREDENTIAL,
    LCIP_CD = CIP,
    AGE_GROUP = AGE_GROUP,
    IMMIGRATION_STATUS = IMMIGRATION_STATUS,
    GRADUATES = SUM_OF_GRADUATES,
    ENROLLED_NOT_GRADUATED = SUM_OF_ENROLMENTS,
    ENROLMENT = SUM_OF_TOTAL_ENROLMENTS
  )

# ---- Check CIP length ----
# All CIP codes should be 7 characters (e.g., "11.0701"). This diagnostic identifies
# any that aren't, which could indicate data quality issues.
ptib_creds %>%
  filter(nchar(LCIP_CD) != 7) %>%
  select(LCIP_CD, CIP_LENGTH = nchar(LCIP_CD))

# ---- Remove periods from CIPs ----
# Standardize CIP codes by removing the period separator (e.g., "11.0701" → "110701").
ptib_creds <- ptib_creds %>%
  mutate(LCIP_CD = str_replace_all(LCIP_CD, fixed("."), ""))

# Sanity check: after removing periods, all CIPs should be 6 characters
ptib_creds %>%
  filter(nchar(LCIP_CD) != 6) %>%
  select(LCIP_CD)

# ---- Check CIPs against INFOWARE ----
# Find PTIB CIP codes that don't exist in the official INFOWARE taxonomy.
# These may need manual correction or exclusion.
ptib_creds %>%
  anti_join(cip6, by = "LCIP_CD") %>%
  distinct(LCIP_CD)

# ---- Update Exclude column ----
# Flag records that should be excluded from the model:
#   - English as a second language programs
#   - Not-for-credit programs
#   - Unclassified CIP code 99999
# We join with INFOWARE to get the program name for matching, then apply exclusion rules.
ptib_creds <- ptib_creds %>%
  left_join(cip6, by = "LCIP_CD") %>%
  mutate(EXCLUDE = if_else(
    LCIP_NAME == "English as a second language" |
    grepl("not for credit", LCIP_NAME) |
    LCIP_CD == "99999",
    "1", NA_character_
  )) %>%
  select(-LCIP_NAME)

# ---- Update age groups ----
# Replace dashes with " to " in age group labels for consistency with the
# standard PSSM age group format (e.g., "25-29" → "25 to 29").
ptib_creds <- ptib_creds %>%
  mutate(AGE_GROUP = str_replace_all(AGE_GROUP, "-", " to "))

# ---- Copy to Clean table ----
# Create a clean copy before computing averages. In the original, this was a separate
# database table; here it's just an R variable for clarity.
ptib_clean <- ptib_creds

# check relevant years to update queries below
ptib_creds %>% count(INTYEAR)

## !! update DATA years in below queries

# ---- qry_Private_Credentials_00g_Avg ----
# Compute 2-year averages of enrolments/graduates by credential/CIP/age/immigration.
# WHY: PTIB data can be volatile year-to-year. Averaging smooths out fluctuations.
# Only non-excluded records are included. The average label (e.g., 'Avg 2021 & 2022')
# must be updated for each model run to match the actual data years.
# NOTE: The /2 divisor assumes exactly 2 years of data. Update if this changes.
ptib_avg <- ptib_clean %>%
  filter(is.na(EXCLUDE)) %>%
  group_by(CREDENTIAL, LCIP_CD, AGE_GROUP, IMMIGRATION_STATUS) %>%
  summarise(
    ENROLMENT = sum(ENROLMENT) / 2,
    ENROLLED_NOT_GRADUATED = sum(ENROLLED_NOT_GRADUATED) / 2,
    GRADUATES = sum(GRADUATES) / 2,
    .groups = "drop"
  ) %>%
  mutate(
    INTYEAR = "Avg 2021 & 2022",
    EXCLUDE = NA_character_
  )

# Replace all data with the averaged data (original did INSERT + DELETE)
ptib_creds <- ptib_avg

# ******************************************************************************
# Part 2: Domestic graduates
#
# Estimates the number of domestic graduates from PTIB data. The challenge is that
# some records have blank or unknown immigration status. We handle this by:
#   1. Counting known domestic graduates (01a)
#   2. Counting all known graduates (domestic + international) (01b)
#   3. Computing the domestic percentage (01c)
#   4. Applying that percentage to blank/unknown records (01d)
#   5. Unioning domestic + estimated domestic, then summing (01e-01f)
#   6. Expanding to all projection years and appending to Graduate_Projections
# ******************************************************************************

## STOP !!! Update MODEL year in queries below ----

# Only CERT and DIPL credentials are included in the model (not apprenticeship, etc.)
ptib_grads <- ptib_creds %>%
  filter(
    is.na(EXCLUDE),
    !is.na(GRADUATES),
    CREDENTIAL %in% c("CERT", "DIPL")
  )

# ---- qry01a: Count domestic grads ----
# Sum graduates where immigration status is explicitly "Domestic".
domestic <- ptib_grads %>%
  filter(IMMIGRATION_STATUS == "Domestic") %>%
  group_by(CREDENTIAL, LCIP_CD, AGE_GROUP) %>%
  summarise(DOMESTIC = sum(GRADUATES), .groups = "drop")

# ---- qry01b: Count domestic and international grads ----
# Sum all graduates with known immigration status (Domestic, International, or #N/A).
dom_intl <- ptib_grads %>%
  filter(IMMIGRATION_STATUS %in% c("Domestic", "International", "#N/A")) %>%
  group_by(CREDENTIAL, LCIP_CD, AGE_GROUP) %>%
  summarise(DOMESTIC_INTERNATIONAL = sum(GRADUATES), .groups = "drop")

# ---- qry01c: Compute percent domestic ----
# Join domestic and total counts, then compute the domestic fraction.
# WHY: We need this percentage to estimate how many of the blank/unknown-status
# graduates are likely domestic.
pct_domestic <- domestic %>%
  left_join(dom_intl, by = c("CREDENTIAL", "LCIP_CD", "AGE_GROUP")) %>%
  mutate(PERCENT_DOMESTIC = if_else(DOMESTIC == 0, 0, DOMESTIC / DOMESTIC_INTERNATIONAL))

# ---- qry01d: Estimate domestic grads for blank/unknown immigration status ----
# Apply the domestic percentage to graduates with blank or unknown status.
# This distributes unknown-status graduates proportionally between domestic and international.
blank_grads <- ptib_grads %>%
  filter(IMMIGRATION_STATUS %in% c("(blank)", "Unknown")) %>%
  select(CREDENTIAL, LCIP_CD, AGE_GROUP, GRADUATES) %>%
  left_join(
    pct_domestic %>% select(CREDENTIAL, LCIP_CD, AGE_GROUP, PERCENT_DOMESTIC),
    by = c("CREDENTIAL", "LCIP_CD", "AGE_GROUP")
  ) %>%
  mutate(DOMESTIC = GRADUATES * PERCENT_DOMESTIC) %>%
  select(CREDENTIAL, LCIP_CD, AGE_GROUP, DOMESTIC)

# ---- qry01e-01f: Union domestic + estimated, then sum ----
# Combine known domestic graduates with estimated domestic from blank/unknown records,
# then sum across both sources by credential/CIP/age group.
qry01f <- bind_rows(domestic, blank_grads) %>%
  group_by(CREDENTIAL, LCIP_CD, AGE_GROUP) %>%
  summarise(GRADS = sum(DOMESTIC, na.rm = TRUE), .groups = "drop") %>%
  mutate(YEAR = "2023/2024")

# ---- qry05i: Summarize grads by credential/age (drop CIP) ----
# Aggregate across CIP codes to get total domestic grads per credential/age combination.
qry05i <- qry01f %>%
  group_by(YEAR, CREDENTIAL, AGE_GROUP) %>%
  summarise(SUMOFGRADS = sum(GRADS), .groups = "drop")

# ---- qry05i1: Expand to all projection years ----
# Join with the year lookup table to create rows for each projection year (Y1 through Y10).
# The T_PTIB_Y1_to_Y10 table maps the base year to each projection year.
qry05i1 <- qry05i %>%
  inner_join(ptib_y1_to_y10, by = c("YEAR" = "Y1")) %>%
  mutate(
    SURVEY = "PTIB",
    PSSM_CRED = paste0("P - ", CREDENTIAL)
  ) %>%
  select(SURVEY, PSSM_CRED, AGE_GROUP, YEAR = Y1_TO_Y10, GRADUATES = SUMOFGRADS)

# ---- qry05i2: Delete excess age groups ----
# Remove age groups that aren't used in the model: blank, unknown, 65+, and 16 or less.
excess_age_groups <- c("(blank)", "Unknown", "65+", "16 or less")

qry05i1 <- qry05i1 %>%
  filter(!AGE_GROUP %in% excess_age_groups)

# Write the PTIB graduate data to database (kept for downstream reference)
dbWriteTable(decimal_con, SQL(glue::glue('"{my_schema}"."qry_Private_Credentials_05i1_Grads_by_Year"')),
             qry05i1, overwrite = TRUE)

# ---- Update Graduate_Projections ----
# Append PTIB graduate data to the main Graduate_Projections table. This is an append
# (not overwrite) because the table already contains public institution data from step 04.
# KEPT AS SQL: INSERT INTO...SELECT (appending to existing table)
dbExecute(decimal_con, "
  INSERT INTO Graduate_Projections (Survey, PSSM_CRED, Age_Group, [Year], Graduates)
  SELECT Survey, PSSM_CRED, Age_Group, [Year], Graduates
  FROM qry_Private_Credentials_05i1_Grads_by_Year;")


# ******************************************************************************
# Part 3: Cohort distributions
#
# Computes the distribution of PTIB graduates across CIP programs (4-digit and 2-digit)
# for each credential/age group. This distribution is used by 06-program-projections
# to allocate projected PTIB graduates across programs.
# ******************************************************************************

# ---- qry06b: Count grads by CIP ----
# Aggregate domestic graduates by credential and CIP code, constructing the composite
# keys (LCP4_CD, LCIP4_CRED, LCIP2_CRED) used throughout the pipeline for program matching.
qry06b <- qry01f %>%
  mutate(
    PSSM_CRED = paste0("P - ", CREDENTIAL),
    LCP4_CD = str_sub(LCIP_CD, 1, 4),
    LCIP4_CRED = paste0("P - ", str_sub(LCIP_CD, 1, 4), " - ", CREDENTIAL),
    LCIP2_CRED = paste0("P - ", str_sub(LCIP_CD, 1, 2), " - ", CREDENTIAL)
  ) %>%
  group_by(YEAR, CREDENTIAL, PSSM_CRED, LCP4_CD, LCIP4_CRED, LCIP2_CRED, AGE_GROUP) %>%
  summarise(COUNT = sum(GRADS), .groups = "drop")

# ---- qry06c: Sum totals by age group ----
# Compute the total graduates per credential/age group. This is the denominator for
# computing the program distribution percentages.
qry06c <- qry06b %>%
  group_by(YEAR, CREDENTIAL, PSSM_CRED, AGE_GROUP) %>%
  summarise(TOTAL = sum(COUNT), .groups = "drop")

# ---- qry06d1: Compute program distribution percentages ----
# Join counts with totals to compute each CIP program's share of graduates.
# This table feeds into 06-program-projections to distribute projected graduates
# across programs using the same proportions observed in the PTIB data.
qry06d1 <- qry06b %>%
  inner_join(qry06c, by = c("YEAR", "CREDENTIAL", "PSSM_CRED", "AGE_GROUP")) %>%
  mutate(
    SURVEY = "PTIB",
    PERCENT = if_else(TOTAL == 0, 0, COUNT / TOTAL)
  ) %>%
  select(SURVEY, CREDENTIAL, PSSM_CRED, LCP4_CD, LCIP4_CRED, LCIP2_CRED,
         AGE_GROUP, YEAR, COUNT, TOTAL, PERCENT) %>%
  filter(!AGE_GROUP %in% excess_age_groups)

dbWriteTable(decimal_con, SQL(glue::glue('"{my_schema}"."qry_Private_Credentials_06d1_Cohort_Dist"')),
             qry06d1, overwrite = TRUE)


# ---- Clean up ----

# Drop lookup tables that are no longer needed after PTIB processing.
# KEPT AS SQL: DROP TABLE (cleanup of tables loaded by earlier scripts)
dbExecute(decimal_con, "DROP TABLE T_PSSM_Credential_Grouping")
dbExecute(decimal_con, "DROP TABLE T_PTIB_Y1_to_Y10")

## ---- disconnect ----
dbDisconnect(decimal_con)
# rm(list=ls())



# ==============================================================================
# FILE: 06-historic-cohort-program-distribution_dplyr.R
# ==============================================================================


# Historic Cohort Program Distribution — dplyr Translation
# Original: R/06-historic-cohort-program-distribution.R
#
# Pipeline context:
#   Builds a historic cohort program distribution table by combining data from 5 sources:
#     1. PTIB — private institution credentials (from 05-ptib-analysis)
#     2. Near Completers — students close to graduation (from 03-near-completers-ttrain)
#     3. Main cohorts (STP + TTRAIN) — weighted credential/program distributions
#     4. Post-degree credentials (PDEG, MAST, DOCT)
#     5. Apprenticeships (APPRAPPR, APPRCERT)
#   Each source is processed independently then combined via bind_rows.
#   The combined table feeds into 06-program-projections for distributing projected
#   graduates across CIP programs.
#
# Input tables:
#   - qry_Private_Credentials_06d1_Cohort_Dist — PTIB distributions (from 05)
#   - T_DACSO_Near_Completers_*_history — near completer ratios (from 03)
#   - T_PSSM_Projection_Cred_Grp — credential groupings for projections
#   - tbl_Program_Projection_Input — STP credential counts (from load-program-projections)
#   - T_Weights_STP — year-based weights for STP data
#   - T_Cohorts_Recoded — unified cohort table (from 02b-1)
#   - tbl_Age_Groups / tbl_Age_Groups_Near_Completers — age group lookups
#   - qry_12_LCP4_LCIPPC_Recode_9999 — CIP cluster mapping for PDEG
#
# Output:
#   - Cohort_Program_Distributions_history — combined historic distributions

library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(dbplyr)

# ---- Configure LAN and file paths ----
db_config <- config::get("decimal")
lan <- config::get("lan")
my_schema <- config::get("myschema")
decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")

# Helper: reference a table in the user's schema
sch_tbl <- function(name) {
  tbl(decimal_con, dbplyr::in_schema(my_schema, name))
}

# Helper: produce grad status prefix for composite keys.
# WHY: Many composite keys (PSSM_CRED, LCIP4_CRED, LCIP2_CRED) conditionally include
# the grad status code with a " - " separator. If grad status is NULL, the prefix
# is empty; otherwise it's "status - ". SQL uses CONCAT with CASE WHEN to handle this.
grad_prefix <- function(status) {
  if_else(is.na(status), "", paste0(status, " - "))
}


# ******************************************************************************
# survey == 'PTIB' — Private institution credentials
# ******************************************************************************
# PTIB distributions are already computed by 05-ptib-analysis. Just reshape.
ptib <- sch_tbl("qry_Private_Credentials_06d1_Cohort_Dist") %>%
  collect() |> rename_with(toupper) %>%
  rename(PSSM_CREDENTIAL = CREDENTIAL) %>%
  mutate(YEAR = 2023) %>%
  select(-YEAR.y, YEAR = YEAR.x)

# Fix: handle the column rename properly after toupper
# The original table has columns: SURVEY, Credential, PSSM_CRED, LCP4_CD, etc.
# After toupper: CREDENTIAL → rename to PSSM_CREDENTIAL
# YEAR column from mutate needs careful handling since original may also have YEAR
ptib <- sch_tbl("qry_Private_Credentials_06d1_Cohort_Dist") %>%
  collect() |> rename_with(toupper) %>%
  rename(PSSM_CREDENTIAL = CREDENTIAL) %>%
  mutate(YEAR = 2023) %>%
  select(-any_of("YEAR")) %>%
  # Re-add YEAR since we just removed both
  mutate(YEAR = 2023)


# ******************************************************************************
# survey == 'Program_Projections_2023-2024_qry_13d' — Near Completers
# ******************************************************************************
# Copy near-completer ratios table from source schema (cross-schema copy).
# KEPT AS SQL: SELECT INTO across schemas
dbExecute(decimal_con, glue::glue(
  "SELECT * INTO [{my_schema}].[T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history] ",
  "FROM T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history;"
))

# Pull the near-completer history data and compute distributions.
# WHY: Near completers represent students who were close to graduating but didn't
# complete. Their distribution across CIP programs informs the model about potential
# future graduates. We sum counts by program/credential/age, compute totals and
# percentages, and map to the standard age group format.
near_comp_history <- sch_tbl("T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history") %>%
  collect() |> rename_with(toupper)

age_groups_nc <- sch_tbl("tbl_Age_Groups_Near_Completers") %>%
  collect() |> rename_with(toupper)

near_completers <- near_comp_history %>%
  mutate(
    LCIP2_CRED = paste0(
      grad_prefix(COSC_GRAD_STATUS_LGDS_CD_GROUP),
      str_sub(LCP4_CD, 1, 2), " - ",
      as.character(TTRAIN), " - ",
      PSSM_CREDENTIAL
    )
  ) %>%
  group_by(COCI_SUBM_CD, PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD,
           COSC_GRAD_STATUS_LGDS_CD_GROUP, TTRAIN, LCIP4_CRED, LCIP2_CRED,
           AGE_GROUP) %>%
  summarise(COUNT = sum(NEAR_COMPLETERS_STP_CREDENTIALS), .groups = "drop") %>%
  rename(YEAR = COCI_SUBM_CD, GRAD_STATUS = COSC_GRAD_STATUS_LGDS_CD_GROUP) %>%
  group_by(YEAR, PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP) %>%
  mutate(TOTAL = sum(COUNT)) %>%
  ungroup() %>%
  inner_join(age_groups_nc,
    by = c("AGE_GROUP" = "AGE_GROUP_LABEL_NEAR_COMPLETER_PROJECTION")) %>%
  mutate(
    SURVEY = "Program_Projections_2023-2024_qry_13d",
    AGE_GROUP = AGE_GROUP_LABEL_GRADUATE_PROJECTION,
    YEAR = as.numeric(paste0("20", str_sub(YEAR, start = -2))),
    PERCENT = ifelse(TOTAL == 0, 0, COUNT / TOTAL)
  ) %>%
  select(SURVEY, PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD, LCIP4_CRED, LCIP2_CRED,
         AGE_GROUP, YEAR, COUNT, TOTAL, PERCENT)


# ******************************************************************************
# survey = 'Program_Projections_2023-2024_Q012e' — Main cohorts (STP + TTRAIN)
# ADCT or ADIP, ADGR or UT, BACH, CERT, DIPL — excludes apprenticeships, PDEG, MAST, DOCT
# ******************************************************************************/

# Pull all source tables needed for both STP and PDEG sections.
proj_cred_grp <- sch_tbl("T_PSSM_Projection_Cred_Grp") %>%
  collect() |> rename_with(toupper)

prog_proj_input <- sch_tbl("tbl_Program_Projection_Input") %>%
  collect() |> rename_with(toupper)

weights_stp <- sch_tbl("T_Weights_STP") %>%
  collect() |> rename_with(toupper)

# ---- Part 1: STP data ----
# Weighted credential/program counts from STP data (non-apprenticeship, non-PDEG).
# WHY: This is the main body of graduates. Weights adjust for year-to-year variation.
main_cohorts_stp <- prog_proj_input %>%
  inner_join(
    weights_stp %>% filter(MODEL == "2023-2024"),
    by = c("PSI_AWARD_SCHOOL_YEAR_DELAYED" = "YEAR_CODE")
  ) %>%
  inner_join(
    proj_cred_grp %>% select(PSSM_PROJECTION_CREDENTIAL, PSSM_CREDENTIAL, COSC_GRAD_STATUS_LGDS_CD),
    by = c("PSI_CREDENTIAL_CATEGORY" = "PSSM_PROJECTION_CREDENTIAL")
  ) %>%
  filter(WEIGHT > 0) %>%
  filter(!PSSM_CREDENTIAL %in% c("APPRAPPR", "APPRCERT", "GRCT or GRDP", "PDEG", "MAST", "DOCT")) %>%
  mutate(
    YEAR = PSI_AWARD_SCHOOL_YEAR_DELAYED,
    GRAD_STATUS = COSC_GRAD_STATUS_LGDS_CD,
    LCP4_CD = FINAL_CIP_CODE_4,
    PSSM_CRED = paste0(grad_prefix(GRAD_STATUS), PSSM_CREDENTIAL),
    LCIP4_CRED = paste0(grad_prefix(GRAD_STATUS), LCP4_CD, " - ", PSSM_CREDENTIAL),
    LCIP2_CRED = paste0(grad_prefix(GRAD_STATUS), str_sub(LCP4_CD, 1, 2), " - ", PSSM_CREDENTIAL)
  ) %>%
  group_by(YEAR, PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD, GRAD_STATUS,
           LCIP4_CRED, LCIP2_CRED, AGE_GROUP, WEIGHT) %>%
  summarise(COUNTS = sum(COUNT), .groups = "drop") %>%
  mutate(WEIGHTED = COUNTS * WEIGHT) %>%
  group_by(YEAR, PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP) %>%
  mutate(TOTAL = sum(WEIGHTED)) %>%
  ungroup() %>%
  mutate(
    YEAR = as.numeric(str_sub(YEAR, 6)),
    PERCENT = ifelse(TOTAL == 0, 0, WEIGHTED / TOTAL)
  )

# ---- Part 2: TTRAIN split ----
# Weighted counts from T_Cohorts_Recoded, split by TTRAIN flag (trades training).
# WHY: Trades training (TTRAIN) is a separate dimension in the model. This data
# allows the STP distributions to be further split by TTRAIN status.
cohorts_recoded <- sch_tbl("T_Cohorts_Recoded") %>%
  collect() |> rename_with(toupper)

age_groups <- sch_tbl("tbl_Age_Groups") %>%
  collect() |> rename_with(toupper)

main_cohorts_TTRAIN <- cohorts_recoded %>%
  inner_join(
    age_groups %>% select(AGE_GROUP, AGE_GROUP_LABEL),
    by = "AGE_GROUP"
  ) %>%
  filter(GRAD_STATUS != "3") %>%
  mutate(
    YEAR = SURVEY_YEAR,
    PSSM_CRED = PSSM_CREDENTIAL,
    LCIP4_CRED = paste0(
      grad_prefix(GRAD_STATUS),
      LCP4_CD, " - ",
      if_else(is.na(TTRAIN), "", paste0(as.character(TTRAIN), " - ")),
      PSSM_CREDENTIAL
    ),
    LCIP2_CRED = paste0(
      grad_prefix(GRAD_STATUS),
      str_sub(LCP4_CD, 1, 2), " - ",
      if_else(is.na(TTRAIN), "", paste0(as.character(TTRAIN), " - ")),
      PSSM_CREDENTIAL
    )
  ) %>%
  group_by(YEAR, PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD, GRAD_STATUS,
           TTRAIN, LCIP4_CRED, LCIP2_CRED, AGE_GROUP_LABEL, WEIGHT) %>%
  summarise(COUNTS = n(), .groups = "drop") %>%
  rename(AGE_GROUP = AGE_GROUP_LABEL) %>%
  mutate(WEIGHTED = COUNTS * WEIGHT) %>%
  filter(!is.na(TTRAIN) & WEIGHT > 0) %>%
  group_by(YEAR, PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD, GRAD_STATUS, AGE_GROUP) %>%
  mutate(TOTAL = sum(WEIGHTED)) %>%
  ungroup() %>%
  mutate(PERCENT = ifelse(TOTAL == 0, 0, WEIGHTED / TOTAL))

# ---- Combine STP + TTRAIN ----
# WHY: The STP data provides base counts, and TTRAIN splits them into trades/non-trades.
# We multiply STP weighted counts by the TTRAIN percentage to get the final distribution.
main_cohorts <- main_cohorts_stp %>%
  left_join(
    main_cohorts_TTRAIN %>% select(-PSSM_CRED),
    by = c("YEAR", "PSSM_CREDENTIAL", "LCP4_CD", "GRAD_STATUS", "AGE_GROUP"),
    suffix = c("_STP", "_TTRAIN")
  ) %>%
  mutate(
    SURVEY = "Program_Projections_2023-2024_Q012e",
    LCIP4_CRED = ifelse(is.na(LCIP4_CRED_TTRAIN), LCIP4_CRED_STP, LCIP4_CRED_TTRAIN),
    LCIP2_CRED = ifelse(is.na(LCIP2_CRED_TTRAIN), LCIP2_CRED_STP, LCIP2_CRED_TTRAIN),
    COUNT = ifelse(is.na(PERCENT_TTRAIN), WEIGHTED_STP, WEIGHTED_STP * PERCENT_TTRAIN),
    TOTAL = TOTAL_STP,
    PERCENT = COUNT / TOTAL
  ) %>%
  select(SURVEY, PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD, GRAD_STATUS, TTRAIN,
         LCIP4_CRED, LCIP2_CRED, AGE_GROUP, YEAR, COUNT, TOTAL, PERCENT)


# ******************************************************************************
# survey = 'Program_Projections_2023-2024_Q013e' — Post-degree (PDEG, MAST, DOCT)
# ******************************************************************************
# Weighted credential counts for post-degree credentials using cluster-level CIP codes.
# WHY: Post-degree credentials (masters, doctorates, graduate certificates) use a
# different CIP aggregation level (LCIPPC instead of LCP4) because they map to
# broader program clusters.
lcppc_recode <- sch_tbl("qry_12_LCP4_LCIPPC_Recode_9999") %>%
  collect() |> rename_with(toupper)

pdeg <- prog_proj_input %>%
  inner_join(
    weights_stp %>% filter(MODEL == "2023-2024"),
    by = c("PSI_AWARD_SCHOOL_YEAR_DELAYED" = "YEAR_CODE")
  ) %>%
  inner_join(
    proj_cred_grp %>% select(PSSM_PROJECTION_CREDENTIAL, PSSM_CREDENTIAL, COSC_GRAD_STATUS_LGDS_CD),
    by = c("PSI_CREDENTIAL_CATEGORY" = "PSSM_PROJECTION_CREDENTIAL")
  ) %>%
  inner_join(
    lcppc_recode %>% select(LCIP_LCP4_CD, LCIP_LCIPPC_CD),
    by = c("FINAL_CIP_CODE_4" = "LCIP_LCP4_CD")
  ) %>%
  filter(WEIGHT > 0) %>%
  filter(PSSM_CREDENTIAL %in% c("GRCT or GRDP", "PDEG", "MAST", "DOCT")) %>%
  mutate(
    YEAR = PSI_AWARD_SCHOOL_YEAR_DELAYED,
    GRAD_STATUS = COSC_GRAD_STATUS_LGDS_CD,
    LCP4_CD = LCIP_LCIPPC_CD,
    PSSM_CRED = paste0(grad_prefix(GRAD_STATUS), PSSM_CREDENTIAL),
    LCIP4_CRED = paste0(grad_prefix(GRAD_STATUS), LCP4_CD, " - ", PSSM_CREDENTIAL)
  ) %>%
  group_by(YEAR, PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD, LCIP4_CRED, AGE_GROUP, WEIGHT) %>%
  summarise(COUNTS = sum(COUNT), .groups = "drop") %>%
  mutate(WEIGHTED = COUNTS * WEIGHT) %>%
  group_by(YEAR, PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP) %>%
  mutate(TOTAL = sum(WEIGHTED)) %>%
  ungroup() %>%
  mutate(
    SURVEY = "Program_Projections_2023-2024_Q013e",
    YEAR = as.numeric(str_sub(YEAR, 6)),
    PERCENT = ifelse(TOTAL == 0, 0, WEIGHTED / TOTAL)
  ) %>%
  select(SURVEY, PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD, LCIP4_CRED,
         AGE_GROUP, YEAR, COUNT = WEIGHTED, TOTAL, PERCENT)


# ******************************************************************************
# survey = 'Program_Projections_2023-2024_Q014e' — Apprenticeships
# ******************************************************************************
# Weighted counts for apprenticeship credentials from T_Cohorts_Recoded.
# WHY: Apprenticeships (APPRAPPR, APPRCERT) are tracked separately from the main
# STP cohorts because they have a distinct credential pathway.
appso <- cohorts_recoded %>%
  inner_join(
    age_groups %>% select(AGE_GROUP, AGE_GROUP_LABEL),
    by = "AGE_GROUP"
  ) %>%
  filter(PSSM_CREDENTIAL %in% c("APPRAPPR", "APPRCERT")) %>%
  filter(WEIGHT > 0) %>%
  mutate(
    YEAR = SURVEY_YEAR,
    PSSM_CRED = PSSM_CREDENTIAL
  ) %>%
  group_by(YEAR, PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD, TTRAIN,
           LCIP4_CRED, LCIP2_CRED, AGE_GROUP_LABEL, WEIGHT) %>%
  summarise(COUNT = n(), .groups = "drop") %>%
  rename(AGE_GROUP = AGE_GROUP_LABEL) %>%
  mutate(WEIGHTED = COUNT * WEIGHT) %>%
  group_by(YEAR, PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP) %>%
  mutate(TOTAL = sum(WEIGHTED)) %>%
  ungroup() %>%
  mutate(
    SURVEY = "Program_Projections_2023-2024_Q014e",
    PERCENT = ifelse(TOTAL == 0, 0, WEIGHTED / TOTAL)
  ) %>%
  select(SURVEY, PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD, LCIP4_CRED, LCIP2_CRED,
         AGE_GROUP, YEAR, COUNT, TOTAL, PERCENT)


# ******************************************************************************
# Combine all sources into historic distributions
# ******************************************************************************
Cohort_Program_Distributions_history <-
  bind_rows(ptib, near_completers, main_cohorts, pdeg, appso)

dbWriteTable(decimal_con,
             SQL(glue::glue('"{my_schema}"."Cohort_Program_Distributions_history"')),
             Cohort_Program_Distributions_history)

dbDisconnect(decimal_con)



# ==============================================================================
# FILE: 06-program-projections_dplyr.R
# ==============================================================================


# Program Projections — dplyr Translation
# Original: R/06-program-projections.R
#
# Pipeline context:
#   Creates static and projected program distributions from several sources:
#     1. PTIB — private institution distributions (from 05)
#     2. Near Completers — students close to graduation (from 03)
#     3. Main cohorts (STP + TTRAIN) — weighted credential/program distributions
#     4. Post-degree credentials (PDEG, MAST, DOCT) — using LCIPPC cluster codes
#     5. Apprenticeships (APPRAPPR, APPRCERT)
#   Each source produces Y1 (base year) distributions. Static and Projected tables
#   are built by combining Y1 data with Y2-Y12 expansions and Werner projections.
#
# Input tables:
#   - T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN — near completer data (from 03)
#   - tbl_Program_Projection_Input — STP credential counts (from load-program-projections)
#   - T_Cohorts_Recoded — unified cohort table (from 02b-1)
#   - T_PSSM_Projection_Cred_Grp — credential groupings
#   - T_Weights_STP — year-based weights
#   - tbl_Age_Groups / tbl_Age_Groups_Near_Completers — age group lookups
#   - INFOWARE_L_CIP_6DIGITS_CIP2016 — CIP taxonomy for LCIPPC mapping
#   - T_Cohort_Program_Distributions_Y2_to_Y12 — year expansion lookup
#   - T_APPR_Y2_to_Y10 — apprenticeship year expansion lookup
#
# Output:
#   - Cohort_Program_Distributions_Projected — projected distributions (Y1-Y12)
#   - Cohort_Program_Distributions_Static — static distributions (Y1-Y12)

library(tidyverse)
library(odbc)
library(config)
library(DBI)
library(glue)
library(assertthat)

# ---- Configure ----
db_config <- config::get("decimal")
lan <- config::get("lan")
my_schema <- config::get("myschema")

decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")

# Helper: reference a table in the user's schema
sch_tbl <- function(name) {
  tbl(decimal_con, dbplyr::in_schema(my_schema, name))
}

# Helper: produce grad status prefix for composite keys
grad_prefix <- function(status) {
  if_else(is.na(status), "", paste0(status, " - "))
}

# ---- Check required tables ----
required_tables <- c(
  "T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN",
  "tbl_Program_Projection_Input", "T_Cohorts_Recoded",
  "Cohort_Program_Distributions_Projected", "Cohort_Program_Distributions_Static",
  "INFOWARE_L_CIP_4DIGITS_CIP2016", "INFOWARE_L_CIP_6DIGITS_CIP2016",
  "T_PSSM_Projection_Cred_Grp", "T_Weights_STP",
  "tbl_Age_Groups_Near_Completers", "T_Cohort_Program_Distributions_Y2_to_Y12"
)
for (table_name in required_tables) {
  full_table_name <- SQL(glue::glue('"{my_schema}"."{table_name}"'))
  assert_that(
    dbExistsTable(decimal_con, full_table_name),
    msg = paste("Error:", table_name, "does not exist in schema", my_schema)
  )
}

# ---- Pull all source tables into R ----
near_comp_raw <- sch_tbl("T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN") %>%
  collect() |> rename_with(toupper)

prog_proj_input <- sch_tbl("tbl_Program_Projection_Input") %>%
  collect() |> rename_with(toupper)

cohorts_recoded <- sch_tbl("T_Cohorts_Recoded") %>%
  collect() |> rename_with(toupper)

proj_cred_grp <- sch_tbl("T_PSSM_Projection_Cred_Grp") %>%
  collect() |> rename_with(toupper)

weights_stp <- sch_tbl("T_Weights_STP") %>%
  collect() |> rename_with(toupper)

age_groups_nc <- sch_tbl("tbl_Age_Groups_Near_Completers") %>%
  collect() |> rename_with(toupper)

age_groups <- sch_tbl("tbl_Age_Groups") %>%
  collect() |> rename_with(toupper)

year_lookup_y2_y12 <- sch_tbl("T_Cohort_Program_Distributions_Y2_to_Y12") %>%
  collect() |> rename_with(toupper)

year_lookup_appr <- sch_tbl("T_APPR_Y2_to_Y10") %>%
  collect() |> rename_with(toupper)

cip6 <- sch_tbl("INFOWARE_L_CIP_6DIGITS_CIP2016") %>%
  collect() |> rename_with(toupper)

# LCIPPC recode: map 4-digit CIP codes to program cluster codes (PDEG/MAST/DOCT use this)
lcippc_recode <- cip6 %>%
  mutate(LCIP_LCIPPC_CD = if_else(LCIP_LCP4_CD == "9999", "99", LCIP_LCIPPC_CD)) %>%
  distinct(LCIP_LCP4_CD, LCIP_LCIPPC_CD)

# Accumulate Y1 data for each output table
projected_sections <- list()
static_sections <- list()


# ******************************************************************************
# Section 1: PTIB — Private institution distributions (conditional on ptib_run)
# WHY: PTIB distributions come pre-computed from 05-ptib-analysis. They go into
# both Projected and Static tables for Y1.
# ******************************************************************************
if (ptib_run == TRUE) {
  ptib_dist <- sch_tbl("qry_Private_Credentials_06d1_Cohort_Dist") %>%
    collect() |> rename_with(toupper) %>%
    rename(PSSM_CREDENTIAL = CREDENTIAL)

  projected_sections$ptib <- ptib_dist
  static_sections$ptib <- ptib_dist
  dbExecute(decimal_con, "DROP TABLE qry_Private_Credentials_06d1_Cohort_Dist")
}


# ******************************************************************************
# Section 2: Near Completers (qry_13a–13d)
# WHY: Near completers represent students close to graduating. Their distributions
# are aggregated from T_DACSO_Near_Completers data and go into both tables for Y1.
# In SQL this was 3 SELECT INTO + 2 INSERT INTO + 3 DROP TABLE = 8 ops.
# ******************************************************************************

# qry_13a: Aggregate near completers by program/credential/age/TTRAIN
near_comp_agg <- near_comp_raw %>%
  mutate(
    LCIP2_CRED = paste0(
      grad_prefix(COSC_GRAD_STATUS_LGDS_CD_GROUP),
      str_sub(LCP4_CD, 1, 2), " - ",
      as.character(TTRAIN), " - ", PSSM_CREDENTIAL
    )
  ) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD, COSC_GRAD_STATUS_LGDS_CD_GROUP,
           TTRAIN, LCIP4_CRED, LCIP2_CRED, AGE_GROUP) %>%
  summarise(COUNT = sum(NEAR_COMPLETERS_STP_CREDENTIALS), .groups = "drop")

# qry_13b–13c: Compute totals and distribution percentages
near_comp_dist <- near_comp_agg %>%
  inner_join(
    near_comp_agg %>%
      group_by(PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP) %>%
      summarise(TOTALS = sum(COUNT), .groups = "drop"),
    by = c("PSSM_CREDENTIAL", "PSSM_CRED", "AGE_GROUP")
  ) %>%
  mutate(PERCENT = if_else(TOTALS == 0, 0, as.numeric(COUNT) / TOTALS))

# qry_13d: Map age groups and format for output
near_comp_mapped <- near_comp_dist %>%
  inner_join(age_groups_nc,
             by = c("AGE_GROUP" = "AGE_GROUP_LABEL_NEAR_COMPLETER_PROJECTION")) %>%
  transmute(
    SURVEY = "Program_Projections_2023-2024_qry_13d",
    PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD,
    GRAD_STATUS = COSC_GRAD_STATUS_LGDS_CD_GROUP,
    TTRAIN = as.character(TTRAIN), LCIP4_CRED, LCIP2_CRED,
    AGE_GROUP = AGE_GROUP_LABEL_GRADUATE_PROJECTION,
    YEAR = "2023/2024", COUNT, TOTAL = TOTALS, PERCENT
  )

projected_sections$near_comp <- near_comp_mapped
static_sections$near_comp <- near_comp_mapped


# ******************************************************************************
# Section 3: Main Cohorts — STP credentials (Q012b–Q012e)
# WHY: The main body of graduates (non-apprenticeship, non-PDEG). STP data is
# weighted by year, then split by TTRAIN using T_Cohorts_Recoded distributions.
# Goes into Static only.
# In SQL this was 8 SELECT INTO + 1 INSERT INTO + 8 DROP TABLE = 17 ops.
# ******************************************************************************

# Q012b: Weighted cohort distribution from STP data
weight_cohort <- prog_proj_input %>%
  inner_join(
    weights_stp %>% filter(MODEL == "2023-2024"),
    by = c("PSI_AWARD_SCHOOL_YEAR_DELAYED" = "YEAR_CODE")
  ) %>%
  inner_join(
    proj_cred_grp %>% select(PSSM_PROJECTION_CREDENTIAL, PSSM_CREDENTIAL, COSC_GRAD_STATUS_LGDS_CD),
    by = c("PSI_CREDENTIAL_CATEGORY" = "PSSM_PROJECTION_CREDENTIAL")
  ) %>%
  filter(WEIGHT > 0) %>%
  filter(!PSSM_CREDENTIAL %in% c("APPRAPPR", "APPRCERT", "GRCT or GRDP", "PDEG", "MAST", "DOCT")) %>%
  mutate(
    PSSM_CRED = paste0(grad_prefix(COSC_GRAD_STATUS_LGDS_CD), PSSM_CREDENTIAL),
    LCP4_CD = FINAL_CIP_CODE_4,
    LCIP4_CRED = paste0(grad_prefix(COSC_GRAD_STATUS_LGDS_CD), FINAL_CIP_CODE_4, " - ", PSSM_CREDENTIAL),
    LCIP2_CRED = paste0(grad_prefix(COSC_GRAD_STATUS_LGDS_CD), str_sub(FINAL_CIP_CODE_4, 1, 2), " - ", PSSM_CREDENTIAL),
    WEIGHTED = COUNT * WEIGHT
  )

# Q012c: Aggregate weighted counts
weighted_dist <- weight_cohort %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD, COSC_GRAD_STATUS_LGDS_CD,
           LCIP4_CRED, LCIP2_CRED, AGEGROUP) %>%
  summarise(COUNT = sum(WEIGHTED), .groups = "drop")

# Q012c1–c2: TTRAIN split from T_Cohorts_Recoded
ttrain_agg <- cohorts_recoded %>%
  inner_join(age_groups %>% select(AGE_GROUP, AGE_GROUP_LABEL), by = "AGE_GROUP") %>%
  filter(GRAD_STATUS != "3", !is.na(TTRAIN), WEIGHT > 0) %>%
  mutate(PSSM_CRED = PSSM_CREDENTIAL) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD, GRAD_STATUS, TTRAIN,
           LCIP4_CRED, LCIP2_CRED, AGE_GROUP_LABEL, WEIGHT) %>%
  summarise(COUNTS = n(), .groups = "drop") %>%
  mutate(WEIGHTED = COUNTS * WEIGHT) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD, GRAD_STATUS, TTRAIN,
           LCIP4_CRED, LCIP2_CRED, AGE_GROUP_LABEL) %>%
  summarise(COUNT = sum(WEIGHTED), .groups = "drop")

# Q012c3–c4: Compute TTRAIN percentages
ttrain_pct <- ttrain_agg %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD, GRAD_STATUS, AGE_GROUP_LABEL) %>%
  mutate(TOTALS = sum(COUNT)) %>%
  ungroup() %>%
  mutate(PERCENT = if_else(TOTALS == 0, 0, COUNT / TOTALS)) %>%
  select(PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD, GRAD_STATUS, TTRAIN, AGE_GROUP_LABEL, PERCENT)

# Q012c5: Distribute STP counts by TTRAIN percentage
main_cohorts_final <- weighted_dist %>%
  left_join(
    ttrain_pct %>% rename(AGEGROUP = AGE_GROUP_LABEL),
    by = c("PSSM_CREDENTIAL", "PSSM_CRED", "LCP4_CD",
           "COSC_GRAD_STATUS_LGDS_CD" = "GRAD_STATUS", "AGEGROUP")
  ) %>%
  mutate(
    LCIP4_CRED = paste0(
      grad_prefix(COSC_GRAD_STATUS_LGDS_CD), LCP4_CD, " - ",
      if_else(is.na(TTRAIN), "", paste0(as.character(TTRAIN), " - ")), PSSM_CREDENTIAL
    ),
    LCIP2_CRED = paste0(
      grad_prefix(COSC_GRAD_STATUS_LGDS_CD), str_sub(LCP4_CD, 1, 2), " - ",
      if_else(is.na(TTRAIN), "", paste0(as.character(TTRAIN), " - ")), PSSM_CREDENTIAL
    ),
    COUNT_DISTRIBUTED = if_else(is.na(PERCENT), COUNT, COUNT * PERCENT)
  )

# Q012d–Q012e: Compute totals and format for Static output
main_cohort_totals <- weight_cohort %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, AGEGROUP) %>%
  summarise(TOTALS = sum(WEIGHTED), .groups = "drop")

static_sections$main_cohorts <- main_cohorts_final %>%
  inner_join(main_cohort_totals, by = c("PSSM_CRED", "AGEGROUP")) %>%
  transmute(
    SURVEY = "Program_Projections_2023-2024_Q012e",
    PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD,
    GRAD_STATUS = COSC_GRAD_STATUS_LGDS_CD, TTRAIN,
    LCIP4_CRED, LCIP2_CRED, AGE_GROUP = AGEGROUP,
    YEAR = "2023/2024", COUNT = COUNT_DISTRIBUTED, TOTAL = TOTALS,
    PERCENT = if_else(TOTALS == 0, 0, COUNT_DISTRIBUTED / TOTALS)
  )


# ******************************************************************************
# Section 4: PDEG / MAST / DOCT (Q013b–Q013e)
# WHY: Post-degree credentials use LCIPPC cluster codes instead of LCP4 because
# they map to broader program clusters. Goes into Static only.
# In SQL this was 4 SELECT INTO + 1 INSERT INTO + 3 DROP TABLE = 8 ops.
# ******************************************************************************

pdeg_weighted <- prog_proj_input %>%
  inner_join(
    weights_stp %>% filter(MODEL == "2023-2024"),
    by = c("PSI_AWARD_SCHOOL_YEAR_DELAYED" = "YEAR_CODE")
  ) %>%
  inner_join(
    proj_cred_grp %>% select(PSSM_PROJECTION_CREDENTIAL, PSSM_CREDENTIAL, COSC_GRAD_STATUS_LGDS_CD),
    by = c("PSI_CREDENTIAL_CATEGORY" = "PSSM_PROJECTION_CREDENTIAL")
  ) %>%
  inner_join(
    lcippc_recode, by = c("FINAL_CIP_CODE_4" = "LCIP_LCP4_CD")
  ) %>%
  filter(WEIGHT > 0, PSSM_CREDENTIAL %in% c("GRCT or GRDP", "PDEG", "MAST", "DOCT")) %>%
  mutate(
    PSSM_CRED = paste0(grad_prefix(COSC_GRAD_STATUS_LGDS_CD), PSSM_CREDENTIAL),
    LCIPPC_CRED = paste0(grad_prefix(COSC_GRAD_STATUS_LGDS_CD), LCIP_LCIPPC_CD, " - ", PSSM_CREDENTIAL),
    WEIGHTED = COUNT * WEIGHT
  )

# Aggregate and compute distribution
pdeg_dist <- pdeg_weighted %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, LCIP_LCIPPC_CD, LCIPPC_CRED, AGEGROUP) %>%
  summarise(COUNT = sum(WEIGHTED), .groups = "drop")

pdeg_totals <- pdeg_weighted %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, AGEGROUP) %>%
  summarise(TOTALS = sum(WEIGHTED), .groups = "drop")

static_sections$pdeg <- pdeg_dist %>%
  inner_join(pdeg_totals, by = c("PSSM_CREDENTIAL", "PSSM_CRED", "AGEGROUP")) %>%
  transmute(
    SURVEY = "Program_Projections_2023-2024_Q013e",
    PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD = LCIP_LCIPPC_CD,
    LCIP4_CRED = LCIPPC_CRED,
    GRAD_STATUS = NA_character_, TTRAIN = NA_character_,
    LCIP2_CRED = NA_character_,
    AGE_GROUP = AGEGROUP, YEAR = "2023/2024",
    COUNT, TOTAL = TOTALS,
    PERCENT = if_else(TOTALS == 0, 0, COUNT / TOTALS)
  )


# ******************************************************************************
# Section 5: Apprenticeships (Q014b–Q014e)
# WHY: Apprenticeship credentials (APPRAPPR, APPRCERT) are tracked separately.
# Goes into both Projected and Static for Y1.
# In SQL this was 3 SELECT INTO + 2 INSERT INTO + 3 DROP TABLE = 8 ops.
# ******************************************************************************

appr_weighted <- cohorts_recoded %>%
  inner_join(age_groups %>% select(AGE_GROUP, AGE_GROUP_LABEL), by = "AGE_GROUP") %>%
  filter(PSSM_CREDENTIAL %in% c("APPRAPPR", "APPRCERT"), WEIGHT > 0) %>%
  mutate(PSSM_CRED = PSSM_CREDENTIAL) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD, TTRAIN,
           LCIP4_CRED, LCIP2_CRED, AGE_GROUP_LABEL, WEIGHT) %>%
  summarise(COUNTS = n(), .groups = "drop") %>%
  mutate(WEIGHTED = COUNTS * WEIGHT)

appr_dist <- appr_weighted %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD, LCIP4_CRED, LCIP2_CRED, AGE_GROUP_LABEL) %>%
  summarise(COUNT = sum(WEIGHTED), .groups = "drop")

appr_totals <- appr_weighted %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP_LABEL) %>%
  summarise(TOTALS = sum(WEIGHTED), .groups = "drop")

appr_output <- appr_dist %>%
  inner_join(appr_totals, by = c("PSSM_CREDENTIAL", "PSSM_CRED", "AGE_GROUP_LABEL")) %>%
  transmute(
    SURVEY = "Program_Projections_2023-2024_Q014e",
    PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD,
    GRAD_STATUS = NA_character_, TTRAIN = NA_character_,
    LCIP4_CRED, LCIP2_CRED, AGE_GROUP = AGE_GROUP_LABEL,
    YEAR = "2023/2024", COUNT, TOTAL = TOTALS,
    PERCENT = if_else(TOTALS == 0, 0, COUNT / TOTALS)
  )

projected_sections$appr <- appr_output
static_sections$appr <- appr_output

# Q014f: Expand apprenticeship grads to Y2-Y10 in Graduate_Projections
appr_grad_expanded <- sch_tbl("Graduate_Projections") %>%
  collect() |> rename_with(toupper) %>%
  filter(SURVEY == "APPSO") %>%
  inner_join(year_lookup_appr, by = c("YEAR" = "Y1")) %>%
  select(SURVEY, PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP, YEAR = Y2_TO_Y10, GRADUATES)

dbWriteTable(decimal_con, "tmp_APPSO_Grads_Expanded", appr_grad_expanded, overwrite = TRUE)
dbExecute(decimal_con, "
  INSERT INTO Graduate_Projections (Survey, PSSM_Credential, PSSM_CRED, Age_Group, [Year], Graduates)
  SELECT Survey, PSSM_Credential, PSSM_CRED, Age_Group, [Year], Graduates
  FROM tmp_APPSO_Grads_Expanded;")
dbExecute(decimal_con, "DROP TABLE tmp_APPSO_Grads_Expanded")


# ******************************************************************************
# Section 6: Build Y1 data and expand to Y2-Y12
# WHY: Y1 distributions are the base year (2023/2024). They need to be expanded
# to Y2-Y12 for both Static and Projected tables using the year lookup.
# Static gets ALL surveys expanded; Projected gets APPR + near completers only.
# ******************************************************************************

# Combine all Y1 sections
static_y1 <- bind_rows(static_sections)
projected_y1 <- bind_rows(projected_sections)

# Q015e22: Expand ALL Static Y1 to Y2-Y12
static_expanded <- static_y1 %>%
  inner_join(year_lookup_y2_y12 %>% select(Y1, Y2_TO_Y10), by = c("YEAR" = "Y1")) %>%
  mutate(SURVEY = "Program_Projections_2023-2024_Q015e22", YEAR = Y2_TO_Y10) %>%
  select(-Y2_TO_Y10)

# Q015e21: Expand APPR + near completers for Projected
appr_nc_y1 <- projected_y1 %>%
  filter(PSSM_CRED %in% c("APPRAPPR", "APPRCERT") | grepl("^3 - ", PSSM_CRED))

projected_expanded <- appr_nc_y1 %>%
  inner_join(year_lookup_y2_y12 %>% select(Y1, Y2_TO_Y10), by = c("YEAR" = "Y1")) %>%
  mutate(SURVEY = "Program_Projections_2023-2024_Q015e21", YEAR = Y2_TO_Y10) %>%
  select(-Y2_TO_Y10)

# Final Static = Y1 + Y2-Y12
final_static <- bind_rows(static_y1, static_expanded)
dbWriteTable(decimal_con,
             SQL(glue::glue('"{my_schema}"."Cohort_Program_Distributions_Static"')),
             final_static, overwrite = TRUE)

# Write intermediate Projected (before Werner projections)
projected_before_werner <- bind_rows(projected_y1, projected_expanded)
dbWriteTable(decimal_con,
             SQL(glue::glue('"{my_schema}"."Cohort_Program_Distributions_Projected"')),
             projected_before_werner, overwrite = TRUE)


# ******************************************************************************
# Section 7: Werner Program — external projection model
# WHY: The Werner program is an external R script that produces projected graduate
# counts by CIP/credential/age for years Y1-Y12. It reads from a CSV input file
# and writes to a CSV output file.
# KEPT AS-IS: This section is already R-native (not SQL).
# ******************************************************************************
input_data <- prog_proj_input %>%
  select(-any_of("EXPR1")) %>%
  tidyr::complete(AGEGROUP, PSI_CREDENTIAL_CATEGORY, FINAL_CIP_CODE_4,
                  PSI_AWARD_SCHOOL_YEAR_DELAYED, fill = list(COUNT = 0)) %>%
  pivot_wider(names_from = "PSI_AWARD_SCHOOL_YEAR_DELAYED", values_from = "COUNT") %>%
  rename("CIP" = "FINAL_CIP_CODE_4", "AGE" = "AGEGROUP", "CRED" = "PSI_CREDENTIAL_CATEGORY") %>%
  select(CIP, CRED, AGE, 4:ncol(.)) %>%
  arrange(CIP, CRED, AGE)

write_csv(input_data, glue::glue("{lan}/development/csv/gh-source/tmp/06/input-data.csv"))

# Run Werner program
source(glue::glue("{lan}/development/R/program projections.R"))

output_data <- read_delim(glue::glue("{lan}/development/csv/gh-source/tmp/06/output.csv"),
                          delim = "\t", col_names = TRUE)
names(output_data) <- paste0(2023:(2023 + 11), "/", 2024:(2024 + 11))

T_Predict_CIP_CRED_AGE <- cbind(input_data, output_data)

T_Predict_CIP_CRED_AGE_Flipped <- T_Predict_CIP_CRED_AGE %>%
  pivot_longer(-c(CIP, CRED, AGE), names_to = "Year", values_to = "Count") %>%
  filter(Year %in% c("2023/2024", "2024/2025", "2025/2026", "2026/2027", "2027/2028",
                      "2028/2029", "2029/2030", "2030/2031", "2031/2032", "2032/2033",
                      "2033/2034", "2034/2035"))

# Diagnostic: check total projected counts by year
T_Predict_CIP_CRED_AGE_Flipped %>%
  group_by(Year) %>%
  summarise(SumOfCount = sum(Count))


# ******************************************************************************
# Section 8: Projected distributions — non-PDEG (qry_10a–10c)
# WHY: Applies the Werner projections to non-PDEG/MAST/DOCT credentials,
# computing program distribution percentages by CIP/credential/age/year.
# In SQL this was 2 SELECT INTO + 1 INSERT INTO + 2 DROP TABLE = 5 ops.
# ******************************************************************************

proj_non_pdeg <- T_Predict_CIP_CRED_AGE_Flipped %>%
  inner_join(
    proj_cred_grp %>% select(PSSM_PROJECTION_CREDENTIAL, PSSM_CREDENTIAL, COSC_GRAD_STATUS_LGDS_CD),
    by = c("CRED" = "PSSM_PROJECTION_CREDENTIAL")
  ) %>%
  filter(!PSSM_CREDENTIAL %in% c("APPRAPPR", "APPRCERT", "GRCT or GRDP", "PDEG", "MAST", "DOCT")) %>%
  mutate(
    PSSM_CRED = paste0(grad_prefix(COSC_GRAD_STATUS_LGDS_CD), PSSM_CREDENTIAL),
    LCIP4_CRED = paste0(grad_prefix(COSC_GRAD_STATUS_LGDS_CD), CIP, " - ", PSSM_CREDENTIAL),
    LCIP2_CRED = paste0(grad_prefix(COSC_GRAD_STATUS_LGDS_CD), str_sub(CIP, 1, 2), " - ", PSSM_CREDENTIAL)
  ) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, CIP, LCIP4_CRED, LCIP2_CRED, AGE, Year) %>%
  summarise(COUNT = sum(Count), .groups = "drop")

proj_non_pdeg_totals <- proj_non_pdeg %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, AGE, Year) %>%
  summarise(TOTALS = sum(COUNT), .groups = "drop")

proj_non_pdeg_output <- proj_non_pdeg %>%
  inner_join(proj_non_pdeg_totals, by = c("PSSM_CREDENTIAL", "PSSM_CRED", "AGE", "Year")) %>%
  transmute(
    SURVEY = "Program_Projections_2023-2024_qry10c",
    PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD = CIP,
    GRAD_STATUS = NA_character_, TTRAIN = NA_character_,
    LCIP4_CRED, LCIP2_CRED, AGE_GROUP = AGE,
    YEAR = Year, COUNT, TOTAL = TOTALS,
    PERCENT = if_else(TOTALS == 0, 0, COUNT / TOTALS)
  )


# ******************************************************************************
# Section 9: Projected distributions — PDEG / MAST / DOCT (qry_12a–12c)
# WHY: Same as Section 8 but for post-degree credentials using LCIPPC cluster codes.
# In SQL this was 2 SELECT INTO + 1 INSERT INTO + 2 DROP TABLE = 5 ops.
# ******************************************************************************

proj_pdeg <- T_Predict_CIP_CRED_AGE_Flipped %>%
  inner_join(
    proj_cred_grp %>% select(PSSM_PROJECTION_CREDENTIAL, PSSM_CREDENTIAL, COSC_GRAD_STATUS_LGDS_CD),
    by = c("CRED" = "PSSM_PROJECTION_CREDENTIAL")
  ) %>%
  inner_join(lcippc_recode, by = c("CIP" = "LCIP_LCP4_CD")) %>%
  filter(PSSM_CREDENTIAL %in% c("GRCT or GRDP", "PDEG", "MAST", "DOCT")) %>%
  mutate(
    PSSM_CRED = paste0(grad_prefix(COSC_GRAD_STATUS_LGDS_CD), PSSM_CREDENTIAL),
    LCIPPC_CRED = paste0(grad_prefix(COSC_GRAD_STATUS_LGDS_CD), LCIP_LCIPPC_CD, " - ", PSSM_CREDENTIAL)
  ) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, LCIP_LCIPPC_CD, LCIPPC_CRED, AGE, Year) %>%
  summarise(COUNT = sum(Count), .groups = "drop")

proj_pdeg_totals <- proj_pdeg %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, AGE, Year) %>%
  summarise(TOTALS = sum(COUNT), .groups = "drop")

proj_pdeg_output <- proj_pdeg %>%
  inner_join(proj_pdeg_totals, by = c("PSSM_CREDENTIAL", "PSSM_CRED", "AGE", "Year")) %>%
  transmute(
    SURVEY = "Program_Projections_2023-2024_qry12c",
    PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD = LCIP_LCIPPC_CD,
    GRAD_STATUS = NA_character_, TTRAIN = NA_character_,
    LCIP4_CRED = LCIPPC_CRED, LCIP2_CRED = NA_character_,
    AGE_GROUP = AGE, YEAR = Year, COUNT, TOTAL = TOTALS,
    PERCENT = if_else(TOTALS == 0, 0, COUNT / TOTALS)
  )


# ******************************************************************************
# Section 10: Build final Projected table
# Combine: Y1 (PTIB + near completers + apprenticeships) + Y2-Y12 expanded
#          + Werner non-PDEG projections + Werner PDEG projections
# ******************************************************************************

final_projected <- bind_rows(projected_before_werner, proj_non_pdeg_output, proj_pdeg_output)

dbWriteTable(decimal_con,
             SQL(glue::glue('"{my_schema}"."Cohort_Program_Distributions_Projected"')),
             final_projected, overwrite = TRUE)


# ******************************************************************************
# Section 11: Diagnostic — check for missing combinations (qry_12d)
# WHY: Validates that every combination in Static has a corresponding entry in
# Projected (for the same credential/CIP/age/year). Missing combinations may
# indicate data issues.
# ******************************************************************************
missing_combos <- final_static %>%
  filter(!AGE_GROUP %in% c("15 to 16", "65 to 89")) %>%
  select(PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD, LCIP4_CRED, AGE_GROUP, YEAR, COUNT) %>%
  anti_join(
    final_projected %>% select(PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD, AGE_GROUP, YEAR),
    by = c("PSSM_CREDENTIAL", "PSSM_CRED", "LCP4_CD", "AGE_GROUP", "YEAR")
  )
missing_combos


# ---- Clean Up: Drop lookup tables no longer needed ----
# KEPT AS SQL: DROP TABLE (cleanup of lookup tables loaded by earlier scripts)
dbExecute(decimal_con, "DROP TABLE AgeGroupLookup")
dbExecute(decimal_con, "DROP TABLE tbl_Age_Groups_Near_Completers")
dbExecute(decimal_con, "DROP TABLE tbl_Age_Groups")
dbExecute(decimal_con, "DROP TABLE T_Cohort_Program_Distributions_Y2_to_Y12")
dbExecute(decimal_con, "DROP TABLE T_APPR_Y2_to_Y10")
dbExecute(decimal_con, "DROP TABLE T_PSSM_Projection_Cred_Grp")
dbExecute(decimal_con, "DROP TABLE T_Weights_STP")

# Verify final tables exist
dbExistsTable(decimal_con, "Cohort_Program_Distributions_Projected")
dbExistsTable(decimal_con, "Cohort_Program_Distributions_Static")
dbExistsTable(decimal_con, "tbl_Program_Projection_Input")

dbDisconnect(decimal_con)



# ==============================================================================
# FILE: 07-occupation-projections_dplyr.R
# ==============================================================================


# Occupation Projections — dplyr Translation
# Original: R/07-occupation-projections.R (~2949 lines, ~198 SQL ops)
#
# Pipeline context:
#   Computes final occupation projections from graduate projections, program distributions,
#   labour supply distributions, and occupation distributions. Produces pivot tables
#   by NOC level (1D-5D) for reporting.
#
# Major sections:
#   Q_0: Setup (Cohort_Program_Distributions copy, PTIB append/delete, T_LCP2_LCP4)
#   Q_1: Grad projections by age/program (join grads × program dists, exclude flagged programs)
#   Q_2: Labour supply by LCIP4_CRED (cascade: direct → No_TT proxy → Private_Cred proxy → LCP2 proxy)
#   Q_3: Occupations by LCIP4_CRED (cascade: direct → No_TT proxy → Private_Cred proxy → LCP2 proxy)
#   Q_4: NOC pivot tables (1D-5D by PSSM_CRED and by Year)
#   Q_5: BC and Total rollups, UNION with regional data
#   Q_6: Model/QI/PTIB table copies, QI error rate, coverage indicator
#
# Input tables:
#   - Graduate_Projections — projected graduates by PSSM_CRED/Age_Group/Year
#   - Cohort_Program_Distributions (or _Static/_Projected) — program distribution percentages
#   - Labour_Supply_Distribution (4 variants: base, LCP2, No_TT, LCP2_No_TT)
#   - Occupation_Distributions (4 variants: base, LCP2, No_TT, LCP2_No_TT)
#   - T_Exclude_from_Projections_* — exclusion lists
#   - T_NOC_Broad_Categories — NOC hierarchy mapping
#   - T_LCP2_LCP4 — CIP 2-digit to 4-digit mapping
#
# Output tables (written to DB):
#   - Q_1_Grad_Projections_by_Age_by_Program, Q_1c_Grad_Projections_by_Program
#   - tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union (labour supply final)
#   - tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union (occupations final)
#   - Q_4_NOC_*_Totals_by_PSSM_CRED, Q_4_NOC_*_Totals_by_Year (1D-5D pivot tables)
#   - Q_4_NOC_Totals_by_Year, Q_4_NOC_Totals_by_Year_BC, Q_4_NOC_Totals_by_Year_Total
#   - Q_5_NOC_Totals_by_Year_and_BC, Q_5_NOC_Totals_by_Year_and_BC_and_Total
#   - tmp_tbl_Model, tmp_tbl_QI, tmp_tbl_Model_Inc_Private_Inst

library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(dbplyr)

# ---- Configure LAN and file paths ----
db_config <- config::get("decimal")
lan <- config::get("lan")
my_schema <- config::get("myschema")

decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")

# Helper: reference a table in the user's schema
sch_tbl <- function(name) {
  tbl(decimal_con, dbplyr::in_schema(my_schema, name))
}

# Helper: write to schema
write_schema_table <- function(name, data) {
  dbWriteTable(decimal_con, SQL(glue::glue('"{my_schema}"."{name}"')), data, overwrite = TRUE)
}

# Helper: compute a private-cred proxy (cross CERT↔DIPL for PTIB P - CERT / P - DIPL)
# WHY: PTIB credentials (P - CERT, P - DIPL) often lack their own distribution data.
# We use the other private credential as a proxy (CERT uses DIPL distributions and vice versa).
private_cred_proxy <- function(unknowns, dist_table, join_vars) {
  unknowns %>%
    inner_join(dist_table, by = join_vars) %>%
    filter(
      (PSSM_CRED == "P - CERT" & str_detect(paste0(PSSM_CRED.y, collapse=""), "P - DIPL")) |
        (PSSM_CRED == "P - DIPL" & str_detect(paste0(PSSM_CRED.y, collapse=""), "P - CERT"))
    )
}


# ******************************************************************************
# Q_0: Setup — Cohort_Program_Distributions, PTIB, T_LCP2_LCP4
# WHY: Copy Static program distributions if the working table doesn't exist.
# Append/delete PTIB data from distribution tables based on run flags.
# ******************************************************************************

# Copy static distributions if needed
# KEPT AS SQL: SELECT INTO across schemas
if (!dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Cohort_Program_Distributions"')))) {
  dbExecute(decimal_con, "SELECT * INTO Cohort_Program_Distributions FROM Cohort_Program_Distributions_Static;")
}

# T_LCP2_LCP4: CIP 2-digit to 4-digit mapping
if (!dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_LCP2_LCP4"')))) {
  lcp2_lcp4 <- sch_tbl("INFOWARE_L_CIP_6DIGITS_CIP2016") %>%
    select(LCIP_LCP2_CD, LCIP_LCP4_CD) %>%
    distinct() %>%
    collect() |> rename_with(toupper)
  write_schema_table("T_LCP2_LCP4", lcp2_lcp4)
} else {
  lcp2_lcp4 <- sch_tbl("T_LCP2_LCP4") %>% collect() |> rename_with(toupper)
}

# PTIB append/delete
# WHY: PTIB (private institution) data needs to be added to or removed from distribution
# tables depending on whether we're running the PTIB model.
# KEPT AS SQL: INSERT INTO and DELETE on existing DB tables (modifying row-level data)
if (ptib_run == TRUE) {
  # Append PTIB rows to 4 distribution tables
  for (tbl_name in c("Labour_Supply_Distribution_No_TT",
                      "Labour_Supply_Distribution_LCP2_No_TT",
                      "Occupation_Distributions_No_TT",
                      "Occupation_Distributions_LCP2_No_TT")) {
    lcp_col <- if (grepl("LCP2", tbl_name)) "LCP2_CD" else "LCP4_CD"
    lcip_col <- if (grepl("LCP2", tbl_name)) "LCP2_CRED" else "LCIP4_CRED"
    existing <- sch_tbl(tbl_name) %>% collect() |> rename_with(toupper)

    ptib_rows <- existing %>%
      filter(PSSM_CREDENTIAL %in% c("CERT", "DIPL", "ADGR or UT", "BACH", "MAST", "DOCT"),
             !str_detect(!!sym(lcip_col), "^3 - ")) %>%
      mutate(
        SURVEY = "PTIB",
        PSSM_CRED = paste0("P - ", PSSM_CREDENTIAL),
        !!lcip_col := paste0("P - ", !!sym(lcp_col), " - ", PSSM_CREDENTIAL)
      )

    dbWriteTable(decimal_con, SQL(glue::glue('"{my_schema}"."{tbl_name}"')),
                 bind_rows(existing, ptib_rows), overwrite = TRUE)
  }
} else {
  # Delete PTIB rows from 4 distribution tables
  for (tbl_name in c("Labour_Supply_Distribution_No_TT",
                      "Labour_Supply_Distribution_LCP2_No_TT",
                      "Occupation_Distributions_No_TT",
                      "Occupation_Distributions_LCP2_No_TT")) {
    dbExecute(decimal_con, glue::glue(
      "DELETE FROM [{my_schema}].[{tbl_name}] WHERE PSSM_CRED LIKE 'P - %';"
    ))
  }
}


# ******************************************************************************
# Q_1: Grad Projections by Age by Program
# WHY: Multiply projected graduates by program distribution percentages to get
# graduates per CIP program. Exclude flagged programs (no outcomes data available).
# ******************************************************************************

grad_projections <- sch_tbl("Graduate_Projections") %>% collect() |> rename_with(toupper)
cohort_prog_dist <- sch_tbl("Cohort_Program_Distributions") %>% collect() |> rename_with(toupper)
exclude_lcp4 <- sch_tbl("T_Exclude_from_Projections_LCP4_CD") %>% collect() |> rename_with(toupper)
exclude_pssm_cred <- sch_tbl("T_Exclude_from_Projections_PSSM_Credential") %>% collect() |> rename_with(toupper)
exclude_lcip4 <- sch_tbl("T_Exclude_from_Projections_LCIP4_CRED") %>% collect() |> rename_with(toupper)

# Exclude lookup sets
exclude_lcp4_set <- exclude_lcp4$LCIP_LCP4_CD
exclude_pssm_set <- exclude_pssm_cred$PSSM_CREDENTIAL
exclude_lcip4_set <- exclude_lcip4$LCIP4_CRED

# Q_1_Grad_Projections_by_Age_by_Program
Q1 <- grad_projections %>%
  inner_join(cohort_prog_dist,
             by = c("PSSM_CRED", "AGE_GROUP" = "AGE_GROUP", "YEAR")) %>%
  mutate(GRADS = GRADUATES * PERCENT) %>%
  filter(
    !LCP4_CD %in% exclude_lcp4_set,
    !PSSM_CREDENTIAL %in% exclude_pssm_set,
    !LCIP4_CRED %in% exclude_lcip4_set
  ) %>%
  select(PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP, YEAR, LCP4_CD, GRAD_STATUS,
         TTRAIN, LCIP4_CRED, GRADS)

write_schema_table("Q_1_Grad_Projections_by_Age_by_Program", Q1)

# Also build static version
cohort_prog_static <- sch_tbl("Cohort_Program_Distributions_Static") %>% collect() |> rename_with(toupper)

Q1_static <- grad_projections %>%
  inner_join(cohort_prog_static,
             by = c("PSSM_CRED", "AGE_GROUP" = "AGE_GROUP", "YEAR")) %>%
  mutate(GRADS = GRADUATES * PERCENT) %>%
  filter(
    !LCP4_CD %in% exclude_lcp4_set,
    !PSSM_CREDENTIAL %in% exclude_pssm_set,
    !LCIP4_CRED %in% exclude_lcip4_set
  )

# Q_1c: Roll up to age group rollup level
age_groups <- sch_tbl("tbl_Age_Groups") %>% collect() |> rename_with(toupper)
age_groups_rollup <- sch_tbl("tbl_Age_Groups_Rollup") %>% collect() |> rename_with(toupper)

Q1c <- Q1 %>%
  inner_join(age_groups %>% select(AGE_GROUP_LABEL, AGE_GROUP_ROLLUP),
             by = c("AGE_GROUP" = "AGE_GROUP_LABEL")) %>%
  inner_join(age_groups_rollup %>% select(AGE_GROUP_ROLLUP, AGE_GROUP_ROLLUP_LABEL),
             by = "AGE_GROUP_ROLLUP") %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP_ROLLUP, AGE_GROUP_ROLLUP_LABEL,
           YEAR, GRAD_STATUS, TTRAIN, LCP4_CD, LCIP4_CRED) %>%
  summarise(GRADS = sum(GRADS), .groups = "drop")

write_schema_table("Q_1c_Grad_Projections_by_Program", Q1c)


# ******************************************************************************
# Q_2: Labour Supply by LCIP4_CRED
# WHY: Multiply grads by labour supply distribution (NLS) percentages.
# Uses cascading fallback: direct match → No_TT proxy → Private_Cred proxy → LCP2 proxy.
# Each step resolves "unknowns" (programs without a direct match) using progressively
# broader approximations.
# ******************************************************************************

ls_dist <- sch_tbl("Labour_Supply_Distribution") %>% collect() |> rename_with(toupper)
ls_dist_no_tt <- sch_tbl("Labour_Supply_Distribution_No_TT") %>% collect() |> rename_with(toupper)
ls_dist_lcp2 <- sch_tbl("Labour_Supply_Distribution_LCP2") %>% collect() |> rename_with(toupper)
ls_dist_lcp2_no_tt <- sch_tbl("Labour_Supply_Distribution_LCP2_No_TT") %>% collect() |> rename_with(toupper)
exclude_lcp2_proxy <- sch_tbl("T_Exclude_from_Labour_Supply_Unknown_LCP2_Proxy") %>%
  collect() |> rename_with(toupper)

# ---- Step 1: Direct match with Labour_Supply_Distribution ----
Q2_direct <- Q1c %>%
  inner_join(
    ls_dist %>% select(LCIP4_CRED, AGE_GROUP_ROLLUP, NEW_LABOUR_SUPPLY, CURRENT_REGION_PSSM_CODE_ROLLUP),
    by = c("LCIP4_CRED", "AGE_GROUP_ROLLUP")
  ) %>%
  mutate(NLS = GRADS * NEW_LABOUR_SUPPLY)

# Unknowns after direct match
Q2_unknowns <- Q1c %>%
  anti_join(Q2_direct, by = c("LCIP4_CRED", "AGE_GROUP_ROLLUP")) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP_ROLLUP, AGE_GROUP_ROLLUP_LABEL,
           TTRAIN, LCP4_CD, LCIP4_CRED, YEAR) %>%
  summarise(GRADS = sum(GRADS), .groups = "drop")

# ---- Step 2: No_TT proxy (match on LCP4_CD, AGE_GROUP_ROLLUP, PSSM_CRED without TTRAIN) ----
Q2_no_tt_proxy <- Q2_unknowns %>%
  inner_join(
    Q1c %>% select(PSSM_CRED, AGE_GROUP_ROLLUP, LCIP4_CRED, YEAR),
    by = c("PSSM_CRED", "AGE_GROUP_ROLLUP", "LCIP4_CRED", "YEAR")
  ) %>%
  inner_join(
    ls_dist_no_tt %>% select(LCIP4_CRED, AGE_GROUP_ROLLUP, NEW_LABOUR_SUPPLY,
                              CURRENT_REGION_PSSM_CODE_ROLLUP),
    by = c("LCIP4_CRED", "AGE_GROUP_ROLLUP")
  ) %>%
  mutate(NLS = GRADS * NEW_LABOUR_SUPPLY)

Q2_after_no_tt <- bind_rows(Q2_direct, Q2_no_tt_proxy)

# Unknowns after No_TT
Q2_unknowns_2 <- Q1c %>%
  anti_join(Q2_after_no_tt, by = c("LCIP4_CRED", "AGE_GROUP_ROLLUP"))

# ---- Step 3: Private_Cred proxy (P-CERT uses P-DIPL distributions, and vice versa) ----
Q2_private_proxy <- Q2_unknowns_2 %>%
  inner_join(
    Q1c %>% select(PSSM_CRED, AGE_GROUP_ROLLUP, LCIP4_CRED, YEAR),
    by = c("PSSM_CRED", "AGE_GROUP_ROLLUP", "LCIP4_CRED", "YEAR")
  ) %>%
  inner_join(
    ls_dist_no_tt %>% select(PSSM_CRED, LCP4_CD, AGE_GROUP_ROLLUP,
                              NEW_LABOUR_SUPPLY, CURRENT_REGION_PSSM_CODE_ROLLUP),
    by = c("AGE_GROUP_ROLLUP", "LCP4_CD")
  ) %>%
  filter(
    (PSSM_CRED.x == "P - CERT" & PSSM_CRED.y == "P - DIPL") |
      (PSSM_CRED.x == "P - DIPL" & PSSM_CRED.y == "P - CERT")
  ) %>%
  mutate(NLS = GRADS * NEW_LABOUR_SUPPLY)

Q2_after_private <- bind_rows(Q2_after_no_tt, Q2_private_proxy)

# Unknowns after Private_Cred
Q2_unknowns_3 <- Q1c %>%
  anti_join(Q2_after_private, by = c("LCIP4_CRED", "AGE_GROUP_ROLLUP"))

# ---- Step 4: LCP2 proxy (match on 2-digit CIP code) ----
Q2_lcp2_proxy <- Q2_unknowns_3 %>%
  inner_join(lcp2_lcp4, by = c("LCP4_CD" = "LCIP_LCP4_CD")) %>%
  anti_join(exclude_lcp2_proxy, by = c("LCP4_CD" = "LCIP_LCP4_CD")) %>%
  inner_join(
    ls_dist_lcp2 %>% select(PSSM_CRED, LCP2_CD, AGE_GROUP_ROLLUP,
                              NEW_LABOUR_SUPPLY, CURRENT_REGION_PSSM_CODE_ROLLUP),
    by = c("PSSM_CRED", "LCIP_LCP2_CD" = "LCP2_CD", "AGE_GROUP_ROLLUP")
  ) %>%
  mutate(NLS = GRADS * NEW_LABOUR_SUPPLY)

Q2_after_lcp2 <- bind_rows(Q2_after_private, Q2_lcp2_proxy)

# Unknowns after LCP2
Q2_unknowns_4 <- Q1c %>%
  anti_join(Q2_after_lcp2, by = c("LCIP4_CRED", "AGE_GROUP_ROLLUP"))

# ---- Step 5: LCP2 No_TT proxy ----
Q2_lcp2_no_tt_proxy <- Q2_unknowns_4 %>%
  inner_join(lcp2_lcp4, by = c("LCP4_CD" = "LCIP_LCP4_CD")) %>%
  anti_join(exclude_lcp2_proxy, by = c("LCP4_CD" = "LCIP_LCP4_CD")) %>%
  inner_join(
    ls_dist_lcp2_no_tt %>% select(PSSM_CRED, LCP2_CD, AGE_GROUP_ROLLUP,
                                    NEW_LABOUR_SUPPLY, CURRENT_REGION_PSSM_CODE_ROLLUP),
    by = c("PSSM_CRED", "LCIP_LCP2_CD" = "LCP2_CD", "AGE_GROUP_ROLLUP")
  ) %>%
  mutate(NLS = GRADS * NEW_LABOUR_SUPPLY)

Q2_after_lcp2_no_tt <- bind_rows(Q2_after_lcp2, Q2_lcp2_no_tt_proxy)

# Unknowns after LCP2 No_TT
Q2_unknowns_5 <- Q1c %>%
  anti_join(Q2_after_lcp2_no_tt, by = c("LCIP4_CRED", "AGE_GROUP_ROLLUP"))

# ---- Step 6: LCP2 Private_Cred proxy ----
Q2_lcp2_private_proxy <- Q2_unknowns_5 %>%
  inner_join(lcp2_lcp4, by = c("LCP4_CD" = "LCIP_LCP4_CD")) %>%
  inner_join(
    ls_dist_lcp2_no_tt %>% select(PSSM_CRED, LCP2_CD, AGE_GROUP_ROLLUP,
                                    NEW_LABOUR_SUPPLY, CURRENT_REGION_PSSM_CODE_ROLLUP),
    by = c("PSSM_CRED", "LCIP_LCP2_CD" = "LCP2_CD", "AGE_GROUP_ROLLUP")
  ) %>%
  filter(
    (PSSM_CRED.x == "P - CERT" & PSSM_CRED.y == "P - DIPL") |
      (PSSM_CRED.x == "P - DIPL" & PSSM_CRED.y == "P - CERT")
  ) %>%
  mutate(NLS = GRADS * NEW_LABOUR_SUPPLY)

# Final labour supply union
labour_supply_final <- bind_rows(Q2_after_lcp2_no_tt, Q2_lcp2_private_proxy) %>%
    rename_with(toupper)

# Remaining unknowns (tracked but not resolved)
Q2_final_unknowns <- Q1c %>%
  anti_join(labour_supply_final, by = c("LCIP4_CRED", "AGE_GROUP_ROLLUP"))

write_schema_table("tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union", labour_supply_final)
write_schema_table("Q_2f_Labour_Supply", Q2_final_unknowns)


# ******************************************************************************
# Q_3: Occupations by LCIP4_CRED
# WHY: Multiply labour supply (NLS) by occupation distribution percentages to get
# projected new labour supply per occupation (NOC). Same cascading fallback pattern
# as Q_2 for resolving unknowns.
# ******************************************************************************

occ_dist <- sch_tbl("Occupation_Distributions") %>% collect() |> rename_with(toupper)
occ_dist_no_tt <- sch_tbl("Occupation_Distributions_No_TT") %>% collect() |> rename_with(toupper)
occ_dist_lcp2 <- sch_tbl("Occupation_Distributions_LCP2") %>% collect() |> rename_with(toupper)
occ_dist_lcp2_no_tt <- sch_tbl("Occupation_Distributions_LCP2_No_TT") %>% collect() |> rename_with(toupper)

# ---- Step 1: Direct match with Occupation_Distributions ----
Q3_direct <- labour_supply_final %>%
  inner_join(
    occ_dist %>% select(LCIP4_CRED, AGE_GROUP_ROLLUP, CURRENT_REGION_PSSM_CODE_ROLLUP, NOC, PERCENT),
    by = c("LCIP4_CRED", "AGE_GROUP_ROLLUP", "CURRENT_REGION_PSSM_CODE_ROLLUP")
  ) %>%
  mutate(OCCSN = NLS * PERCENT)

# Unknowns
Q3_unknowns <- labour_supply_final %>%
  anti_join(
    Q3_direct,
    by = c("LCIP4_CRED", "AGE_GROUP_ROLLUP", "CURRENT_REGION_PSSM_CODE_ROLLUP")
  )

# ---- Step 2: No_TT proxy ----
Q3_no_tt_proxy <- Q3_unknowns %>%
  inner_join(
    occ_dist_no_tt %>% select(PSSM_CRED, LCP4_CD, AGE_GROUP_ROLLUP,
                               CURRENT_REGION_PSSM_CODE_ROLLUP, NOC, PERCENT),
    by = c("PSSM_CRED", "LCP4_CD", "AGE_GROUP_ROLLUP", "CURRENT_REGION_PSSM_CODE_ROLLUP")
  ) %>%
  mutate(OCCSN = NLS * PERCENT)

Q3_after_no_tt <- bind_rows(Q3_direct, Q3_no_tt_proxy)

Q3_unknowns_2 <- labour_supply_final %>%
  anti_join(Q3_after_no_tt,
            by = c("LCIP4_CRED", "AGE_GROUP_ROLLUP", "CURRENT_REGION_PSSM_CODE_ROLLUP"))

# ---- Step 3: Private_Cred proxy ----
Q3_private_proxy <- Q3_unknowns_2 %>%
  inner_join(
    labour_supply_final %>% select(PSSM_CRED, LCP4_CD, AGE_GROUP_ROLLUP,
                                    CURRENT_REGION_PSSM_CODE_ROLLUP, YEAR),
    by = c("PSSM_CRED", "AGE_GROUP_ROLLUP", "CURRENT_REGION_PSSM_CODE_ROLLUP", "YEAR")
  ) %>%
  inner_join(
    occ_dist_no_tt %>% select(LCP4_CD, AGE_GROUP_ROLLUP, CURRENT_REGION_PSSM_CODE_ROLLUP, NOC, PERCENT),
    by = c("LCP4_CD", "AGE_GROUP_ROLLUP", "CURRENT_REGION_PSSM_CODE_ROLLUP")
  ) %>%
  filter(
    (PSSM_CRED.x == "P - CERT" & PSSM_CRED.y == "P - DIPL") |
      (PSSM_CRED.x == "P - DIPL" & PSSM_CRED.y == "P - CERT")
  ) %>%
  mutate(OCCSN = NLS * PERCENT)

Q3_after_private <- bind_rows(Q3_after_no_tt, Q3_private_proxy)

Q3_unknowns_3 <- labour_supply_final %>%
  anti_join(Q3_after_private,
            by = c("LCIP4_CRED", "AGE_GROUP_ROLLUP", "CURRENT_REGION_PSSM_CODE_ROLLUP", "YEAR"))

# ---- Step 4: LCP2 proxy ----
Q3_lcp2_proxy <- Q3_unknowns_3 %>%
  inner_join(lcp2_lcp4, by = c("LCP4_CD" = "LCIP_LCP4_CD")) %>%
  anti_join(exclude_lcp2_proxy, by = c("LCP4_CD" = "LCIP_LCP4_CD")) %>%
  inner_join(
    occ_dist_lcp2 %>% select(PSSM_CRED, LCP2_CD, AGE_GROUP_ROLLUP,
                              CURRENT_REGION_PSSM_CODE_ROLLUP, NOC, PERCENT),
    by = c("PSSM_CRED", "LCIP_LCP2_CD" = "LCP2_CD", "AGE_GROUP_ROLLUP",
           "CURRENT_REGION_PSSM_CODE_ROLLUP")
  ) %>%
  mutate(OCCSN = NLS * PERCENT)

Q3_after_lcp2 <- bind_rows(Q3_after_private, Q3_lcp2_proxy)

Q3_unknowns_4 <- labour_supply_final %>%
  anti_join(Q3_after_lcp2,
            by = c("LCIP4_CRED", "AGE_GROUP_ROLLUP", "CURRENT_REGION_PSSM_CODE_ROLLUP", "YEAR"))

# ---- Step 5: LCP2 No_TT proxy ----
Q3_lcp2_no_tt_proxy <- Q3_unknowns_4 %>%
  inner_join(lcp2_lcp4, by = c("LCP4_CD" = "LCIP_LCP4_CD")) %>%
  anti_join(exclude_lcp2_proxy, by = c("LCP4_CD" = "LCIP_LCP4_CD")) %>%
  inner_join(
    occ_dist_lcp2_no_tt %>% select(PSSM_CRED, LCP2_CD, AGE_GROUP_ROLLUP,
                                     CURRENT_REGION_PSSM_CODE_ROLLUP, NOC, PERCENT),
    by = c("PSSM_CRED", "LCIP_LCP2_CD" = "LCP2_CD", "AGE_GROUP_ROLLUP",
           "CURRENT_REGION_PSSM_CODE_ROLLUP")
  ) %>%
  mutate(OCCSN = NLS * PERCENT)

Q3_after_lcp2_no_tt <- bind_rows(Q3_after_lcp2, Q3_lcp2_no_tt_proxy)

Q3_unknowns_5 <- labour_supply_final %>%
  anti_join(Q3_after_lcp2_no_tt,
            by = c("LCIP4_CRED", "AGE_GROUP_ROLLUP", "CURRENT_REGION_PSSM_CODE_ROLLUP", "YEAR"))

# ---- Step 6: LCP2 Private_Cred proxy ----
Q3_lcp2_private_proxy <- Q3_unknowns_5 %>%
  inner_join(lcp2_lcp4, by = c("LCP4_CD" = "LCIP_LCP4_CD")) %>%
  inner_join(
    occ_dist_lcp2_no_tt %>% select(PSSM_CRED, LCP2_CD, AGE_GROUP_ROLLUP,
                                     CURRENT_REGION_PSSM_CODE_ROLLUP, NOC, PERCENT),
    by = c("PSSM_CRED", "LCIP_LCP2_CD" = "LCP2_CD", "AGE_GROUP_ROLLUP",
           "CURRENT_REGION_PSSM_CODE_ROLLUP")
  ) %>%
  filter(
    (PSSM_CRED.x == "P - CERT" & PSSM_CRED.y == "P - DIPL") |
      (PSSM_CRED.x == "P - DIPL" & PSSM_CRED.y == "P - CERT")
  ) %>%
  mutate(OCCSN = NLS * PERCENT)

Q3_after_lcp2_private <- bind_rows(Q3_after_lcp2_no_tt, Q3_lcp2_private_proxy)

# ---- Step 7: Remaining unknowns get NOC=99999, Percent=1 ----
Q3_unknowns_final <- labour_supply_final %>%
  anti_join(Q3_after_lcp2_private,
            by = c("LCIP4_CRED", "AGE_GROUP_ROLLUP", "CURRENT_REGION_PSSM_CODE_ROLLUP", "YEAR")) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP_ROLLUP, AGE_GROUP_ROLLUP_LABEL,
            YEAR, TTRAIN, LCP4_CD, LCIP4_CRED, CURRENT_REGION_PSSM_CODE_ROLLUP) %>%
  summarise(NLS = sum(NLS), .groups = "drop") %>%
  filter(NLS > 0) %>%
  mutate(NOC = 99999, PERCENT = 1, OCCSN = NLS)

# Final occupation union (filter positive OccsN)
occupations_final <- bind_rows(Q3_after_lcp2_private, Q3_unknowns_final) %>%
  filter(OCCSN > 0) %>%
  rename_with(toupper)

write_schema_table("tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union", occupations_final)


# ******************************************************************************
# Q_4: NOC Pivot Tables (1D-5D)
# WHY: Create pivot tables aggregating occupation projections by NOC hierarchy level
# (1-digit broad category through 5-digit unit group) and by PSSM_CRED or Year.
# These are the final output tables for reporting.
# NOTE: The original uses SQL PIVOT which we translate to tidyr::pivot_wider.
# ******************************************************************************

noc_broad <- sch_tbl("T_NOC_Broad_Categories") %>% collect() |> rename_with(toupper)

# Join NOC categories to get hierarchy codes
occ_with_noc <- occupations_final %>%
  inner_join(noc_broad, by = c("NOC" = "UNIT_GROUP_CODE"))

# Helper: build NOC pivot by PSSM_CRED
# WHY: Each NOC level (1D=1-digit, 2D=2-digit, etc.) produces a separate pivot table.
build_noc_pivot_by_cred <- function(data, noc_col, eng_col) {
  pssm_creds <- unique(data$PSSM_CRED)
  data %>%
    mutate(
      NOC_LEVEL = nchar(!!sym(noc_col)),
      NOC = !!sym(noc_col),
      ENGLISH_NAME = !!sym(eng_col)
    ) %>%
    select(PSSM_CRED, OCCSN, NOC_LEVEL, NOC, ENGLISH_NAME) %>%
    pivot_wider(names_from = PSSM_CRED, values_from = OCCSN,
                values_fn = sum, values_fill = 0) %>%
    arrange(NOC_LEVEL, NOC)
}

# Helper: build NOC pivot by Year
# WHY: Separate pivot tables show projections by year for each NOC level.
build_noc_pivot_by_year <- function(data, noc_col, eng_col) {
  data %>%
    mutate(
      NOC_LEVEL = nchar(!!sym(noc_col)),
      NOC = !!sym(noc_col),
      ENGLISH_NAME = !!sym(eng_col)
    ) %>%
    select(YEAR, OCCSN, AGE_GROUP_ROLLUP_LABEL, NOC_LEVEL, NOC, ENGLISH_NAME,
           CURRENT_REGION_PSSM_CODE_ROLLUP, CURRENT_REGION_PSSM_NAME_ROLLUP) %>%
    pivot_wider(names_from = YEAR, values_from = OCCSN,
                values_fn = sum, values_fill = NA) %>%
    arrange(NOC_LEVEL, NOC)
}

# 1D - Broad Category (1-digit NOC)
Q4_1D_by_cred <- build_noc_pivot_by_cred(occ_with_noc, "BROAD_CATEGORY_CODE", "BROAD_CATEGORY_ENGLISH_NAME")
Q4_1D_by_year <- build_noc_pivot_by_year(occ_with_noc, "BROAD_CATEGORY_CODE", "BROAD_CATEGORY_ENGLISH_NAME")

# 2D - Major Group (2-digit NOC)
Q4_2D_by_cred <- build_noc_pivot_by_cred(occ_with_noc, "MAJOR_GROUP_CODE", "MAJOR_GROUP_ENGLISH_NAME")
Q4_2D_by_year <- build_noc_pivot_by_year(occ_with_noc, "MAJOR_GROUP_CODE", "MAJOR_GROUP_ENGLISH_NAME")

# 3D - Sub-Major Group (3-digit NOC)
Q4_3D_by_cred <- build_noc_pivot_by_cred(occ_with_noc, "SUB_MAJOR_GROUP_CODE", "SUB_MAJOR_ENGLISH_NAME")
Q4_3D_by_year <- build_noc_pivot_by_year(occ_with_noc, "SUB_MAJOR_GROUP_CODE", "SUB_MAJOR_ENGLISH_NAME")

# 4D - Minor Group (4-digit NOC)
Q4_4D_by_cred <- build_noc_pivot_by_cred(occ_with_noc, "MINOR_GROUP_CODE", "MINOR_GROUP_ENGLISH_NAME")
Q4_4D_by_year <- build_noc_pivot_by_year(occ_with_noc, "MINOR_GROUP_CODE", "MINOR_GROUP_ENGLISH_NAME")

# 5D - Unit Group (5-digit NOC)
Q4_5D_by_cred <- build_noc_pivot_by_cred(occ_with_noc, "UNIT_GROUP_CODE", "ENGLISH_NAME")
Q4_5D_by_year <- build_noc_pivot_by_year(occ_with_noc, "UNIT_GROUP_CODE", "ENGLISH_NAME")

# Write all pivot tables
write_schema_table("Q_4_NOC_1D_Totals_by_PSSM_CRED", Q4_1D_by_cred)
write_schema_table("Q_4_NOC_1D_Totals_by_Year", Q4_1D_by_year)
write_schema_table("Q_4_NOC_2D_Totals_by_PSSM_CRED", Q4_2D_by_cred)
write_schema_table("Q_4_NOC_2D_Totals_by_Year", Q4_2D_by_year)
write_schema_table("Q_4_NOC_3D_Totals_by_PSSM_CRED", Q4_3D_by_cred)
write_schema_table("Q_4_NOC_3D_Totals_by_Year", Q4_3D_by_year)
write_schema_table("Q_4_NOC_4D_Totals_by_PSSM_CRED", Q4_4D_by_cred)
write_schema_table("Q_4_NOC_4D_Totals_by_Year", Q4_4D_by_year)
write_schema_table("Q_4_NOC_5D_Totals_by_PSSM_CRED", Q4_5D_by_cred)
write_schema_table("Q_4_NOC_5D_Totals_by_Year", Q4_5D_by_year)

# Union all NOC levels by Year
Q4_totals_by_year <- bind_rows(
  Q4_1D_by_year, Q4_2D_by_year, Q4_3D_by_year, Q4_4D_by_year, Q4_5D_by_year
)
write_schema_table("Q_4_NOC_Totals_by_Year", Q4_totals_by_year)

# ---- BC Rollup ----
# WHY: Aggregate regional NOC projections up to the BC level. The rollup_codes_bc
# table maps individual region codes to their BC-level equivalents, and rollup_codes
# provides the BC-level code and name. Year columns are summed across regions.
rollup_codes_bc <- sch_tbl("T_Current_Region_PSSM_Rollup_Codes_BC") %>%
  collect() |> rename_with(toupper)
rollup_codes <- sch_tbl("T_Current_Region_PSSM_Rollup_Codes") %>%
  collect() |> rename_with(toupper)

# Identify year columns (used for summarise across)
year_cols <- names(Q4_totals_by_year)[grepl("^\\d{4}/\\d{4}$", names(Q4_totals_by_year))]

Q4_totals_by_year_bc <- Q4_totals_by_year %>%
  inner_join(rollup_codes_bc %>% select(CURRENT_REGION_PSSM_CODE_ROLLUP, CURRENT_REGION_PSSM_CODE_ROLLUP_BC),
             by = "CURRENT_REGION_PSSM_CODE_ROLLUP") %>%
  inner_join(rollup_codes %>% select(CURRENT_REGION_PSSM_CODE_ROLLUP, CURRENT_REGION_PSSM_NAME_ROLLUP),
             by = c("CURRENT_REGION_PSSM_CODE_ROLLUP_BC" = "CURRENT_REGION_PSSM_CODE_ROLLUP")) %>%
  mutate(Expr1000 = paste0(AGE_GROUP_ROLLUP_LABEL, "-", NOC, "-", CURRENT_REGION_PSSM_CODE_ROLLUP_BC)) %>%
  rename(CURRENT_REGION_PSSM_CODE_ROLLUP_ORIG = CURRENT_REGION_PSSM_CODE_ROLLUP,
         CURRENT_REGION_PSSM_NAME_ROLLUP_ORIG = CURRENT_REGION_PSSM_NAME_ROLLUP,
         CURRENT_REGION_PSSM_CODE_ROLLUP = CURRENT_REGION_PSSM_CODE_ROLLUP_BC) %>%
  select(-CURRENT_REGION_PSSM_CODE_ROLLUP_ORIG, -CURRENT_REGION_PSSM_NAME_ROLLUP_ORIG) %>%
  group_by(Expr1000, AGE_GROUP_ROLLUP_LABEL, NOC_LEVEL, NOC, ENGLISH_NAME,
           CURRENT_REGION_PSSM_CODE_ROLLUP, CURRENT_REGION_PSSM_NAME_ROLLUP) %>%
  summarise(across(all_of(year_cols), ~sum(., na.rm = TRUE)), .groups = "drop")

write_schema_table("Q_4_NOC_Totals_by_Year_BC", Q4_totals_by_year_bc)

# ---- Total Rollup ----
# WHY: Same as BC rollup but uses current_region_pssm_code_rollup_total to aggregate
# all regions into a single provincial total.
Q4_totals_by_year_total <- Q4_totals_by_year %>%
  inner_join(rollup_codes_bc %>% select(CURRENT_REGION_PSSM_CODE_ROLLUP, CURRENT_REGION_PSSM_CODE_ROLLUP_TOTAL),
             by = "CURRENT_REGION_PSSM_CODE_ROLLUP") %>%
  inner_join(rollup_codes %>% select(CURRENT_REGION_PSSM_CODE_ROLLUP, CURRENT_REGION_PSSM_NAME_ROLLUP),
             by = c("CURRENT_REGION_PSSM_CODE_ROLLUP_TOTAL" = "CURRENT_REGION_PSSM_CODE_ROLLUP")) %>%
  mutate(Expr1000 = paste0(AGE_GROUP_ROLLUP_LABEL, "-", NOC, "-", CURRENT_REGION_PSSM_CODE_ROLLUP_TOTAL)) %>%
  rename(CURRENT_REGION_PSSM_CODE_ROLLUP_ORIG = CURRENT_REGION_PSSM_CODE_ROLLUP,
         CURRENT_REGION_PSSM_NAME_ROLLUP_ORIG = CURRENT_REGION_PSSM_NAME_ROLLUP,
         CURRENT_REGION_PSSM_CODE_ROLLUP = CURRENT_REGION_PSSM_CODE_ROLLUP_TOTAL) %>%
  select(-CURRENT_REGION_PSSM_CODE_ROLLUP_ORIG, -CURRENT_REGION_PSSM_NAME_ROLLUP_ORIG) %>%
  group_by(Expr1000, AGE_GROUP_ROLLUP_LABEL, NOC_LEVEL, NOC, ENGLISH_NAME,
           CURRENT_REGION_PSSM_CODE_ROLLUP, CURRENT_REGION_PSSM_NAME_ROLLUP) %>%
  summarise(across(all_of(year_cols), ~sum(., na.rm = TRUE)), .groups = "drop")

write_schema_table("Q_4_NOC_Totals_by_Year_Total", Q4_totals_by_year_total)


# ******************************************************************************
# Q_5: BC and Total rollups + UNION
# WHY: Combine regional data with BC-level and Total-level rollups. The final output
# table contains all three aggregation levels: individual regions, BC subtotal,
# and provincial total.
# ******************************************************************************

# Q_5_NOC_Totals_by_Year_and_BC = regional + BC rollup
Q5_bc <- bind_rows(Q4_totals_by_year, Q4_totals_by_year_bc)
write_schema_table("Q_5_NOC_Totals_by_Year_and_BC", Q5_bc)

# Q_5_NOC_Totals_by_Year_and_BC_and_Total = regional + BC + Total rollup
Q5_bc_total <- bind_rows(Q4_totals_by_year, Q4_totals_by_year_bc, Q4_totals_by_year_total)
write_schema_table("Q_5_NOC_Totals_by_Year_and_BC_and_Total", Q5_bc_total)


# ******************************************************************************
# Q_6: Model / QI / PTIB table copies
# WHY: Store final model output under different names for each model run type.
# The regular model, QI model, and PTIB model each get their own copy.
# ******************************************************************************

if (regular_run == TRUE) {
  write_schema_table("tmp_tbl_Model", Q5_bc_total)
}

if (qi_run == TRUE) {
  write_schema_table("tmp_tbl_QI", Q5_bc_total)
}

if (ptib_run == TRUE) {
  write_schema_table("tmp_tbl_Model_Inc_Private_Inst", Q5_bc_total)
}


# ---- Clean up ----
# Drop intermediate Q_2 temp tables (they're now R variables)
# Keep final output tables: Q_1, Q_1c, tmp_tbl_Q_2d, tmp_tbl_Q_3d, Q_4_*, Q_5_*

dbDisconnect(decimal_con)



# ==============================================================================
# FILE: prep-for-fresh-run_dplyr.R
# ==============================================================================


# Prep for Fresh Run — dplyr Translation
# Original: R/prep-for-fresh-run.R (~201 lines)
#
# Pipeline context:
#   Prepares the database environment for a full model re-run (regular_run).
#   Three phases: (1) Drop all non-raw tables, (2) Drop additional specific tables,
#   (3) Copy required base tables from dbo schema, (4) Execute pipeline scripts.
#
# Key translations:
#   - DROP TABLE (DDL) → kept as dbExecute (no dplyr equivalent for DDL)
#   - SELECT INTO (table copy across schemas) → dbReadTable + dbWriteTable
#   - INFORMATION_SCHEMA query → kept as dbGetQuery (metadata query)
#   - File sourcing → unchanged (same pipeline scripts)
#
# Flags set:
#   regular_run = TRUE, qi_run = FALSE, ptib_run = FALSE

library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(RJDBC)
source("./R/utils.R")

# ---- Configuration ----
db_config <- config::get("decimal")
my_schema <- config::get("myschema")
regular_run <- T
qi_run <- F
ptib_run <- F

decimal_con <- dbConnect(odbc::odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")

# ******************************************************************************
# Phase 1: Drop all non-raw tables in the user's schema
# WHY: Before a fresh run, all intermediate and output tables from previous runs
# must be removed. Raw data tables (ending in "_raw") are preserved since they
# contain the original source data loaded from external files.
# ******************************************************************************
tables_query <- paste0(
  "SELECT TABLE_NAME
     FROM INFORMATION_SCHEMA.TABLES
     WHERE TABLE_SCHEMA = '", my_schema, "' AND TABLE_TYPE = 'BASE TABLE'"
)
all_tables <- dbGetQuery(decimal_con, tables_query)$TABLE_NAME

# Keep raw tables, drop everything else
remove_tables <- all_tables[stringr::str_detect(all_tables, pattern = "_raw$", negate = T)]

# Drop all non-raw tables in a transaction
dbBegin(decimal_con)
tryCatch({
  for (table in remove_tables) {
    dbExecute(decimal_con, glue::glue('DROP TABLE "{my_schema}"."{table}"'))
  }
  dbCommit(decimal_con)
  print("All tables deleted successfully.")
}, error = function(e) {
  dbRollback(decimal_con)
  print(paste("Error:", e$message))
}, finally = {
  dbDisconnect(decimal_con)
})

decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")

# ******************************************************************************
# Phase 2: Drop additional specific tables
# WHY: Some tables may have been missed in Phase 1 (e.g., tables created after
# the initial drop, or tables that need to be removed between the regular and
# QI/PTIB runs). These use IF OBJECT_ID to avoid errors if tables don't exist.
# KEPT AS SQL: DROP TABLE IF EXISTS (DDL operation, no dplyr equivalent)
# ******************************************************************************
drop_if_exists <- c(
  "T_Suppression_Public_Release_NOC",
  "qry_Private_Credentials_05i1_Grads_by_Year",
  "tmp_tbl_Model",
  "tmp_tbl_Model_Inc_Private_Inst",
  "tmp_tbl_Model_Program_Projection",
  "tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union",
  "tmp_tbl_QI",
  "Labour_Supply_Distribution",
  "Labour_Supply_Distribution_No_TT",
  "Labour_Supply_Distribution_LCP2_No_TT",
  "Labour_Supply_Distribution_LCP2",
  "Occupation_Distributions",
  "Occupation_Distributions_no_TT",
  "Occupation_Distributions_LCP2",
  "Occupation_Distributions_LCP2_no_tt",
  "Occupation_Distributions_LCP2_bc_no_tt",
  "Occupation_Distributions_LCP2_bc"
)

for (table_name in drop_if_exists) {
  dbExecute(decimal_con, glue::glue(
    "IF OBJECT_ID('[{my_schema}].[{table_name}]', 'U') IS NOT NULL ",
    "DROP TABLE [{my_schema}].[{table_name}];"
  ))
}

# ******************************************************************************
# Phase 3: Copy required base tables from dbo to user schema
# WHY: Some tables are modified during the pipeline run (e.g., Credential_Non_Dup
# gets records added). For a fresh run, these need to be re-created from the
# clean dbo originals. The original used SELECT INTO; we use dbReadTable +
# dbWriteTable for the same effect.
# ******************************************************************************
copy_tables <- c(
  "T_bgs_data_final_for_outcomesmatching",
  "Labour_Supply_Distribution_Stat_Can",
  "Occupation_Distributions_Stat_Can",
  "Credential_Non_Dup"
)

dbBegin(decimal_con)
tryCatch({
  for (table_short in copy_tables) {
    # Drop if exists first (for idempotency)
    dbExecute(decimal_con, glue::glue(
      "IF OBJECT_ID('{my_schema}.{table_short}', 'U') IS NOT NULL ",
      "DROP TABLE [{my_schema}].[{table_short}];"
    ))
    # Read from dbo schema, write to user schema
    source_data <- dbReadTable(decimal_con, SQL(glue::glue('"dbo"."{table_short}"')))
    dbWriteTable(decimal_con,
                 SQL(glue::glue('"{my_schema}"."{table_short}"')),
                 source_data, overwrite = TRUE)
  }
  dbCommit(decimal_con)
  print("All tables copied successfully.")
}, error = function(e) {
  dbRollback(decimal_con)
  print(paste("Error:", e$message))
}, finally = {
  dbDisconnect(decimal_con)
})

decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")


# ******************************************************************************
# Phase 4: Execute pipeline scripts for regular run
# WHY: Sources each pipeline script in order, wrapped with time_execution()
# for timing and error handling. Only runs when regular_run flag is set.
# ******************************************************************************
regular_run_files <- c(
  "./R/load-cohort-appso.R",
  "./R/load-cohort-bgs.R",
  "./R/load-cohort-dacso.R",
  "./R/load-cohort-trd.R",
  "./R/02b-1-pssm-cohorts.R",
  "./R/02b-2-pssm-cohorts-new-labour-supply.R",
  "./R/02b-3-pssm-cohorts-occupation-distributions.R",
  "./R/load-near-completers-ttrain.R",
  "./R/03-near-completers-ttrain.R",
  "./R/load-graduate-projections.R",
  "./R/04-graduate-projections.R",
  "./R/load-program-projections.R",
  "./R/06-program-projections.R",
  "./R/load-occupation-projections.R",
  "./R/07-occupation-projections.R"
)

print(glue::glue("regular model run flag: {regular_run}"))
if (regular_run == T & qi_run != T & ptib_run != T) {
  for (file_path in regular_run_files) {
    print(glue::glue("regular model run flag: {regular_run}"))
    print(glue::glue("qi model run flag: {qi_run}"))
    print(glue::glue("ptib model furn flag: {ptib_run}"))
    time_execution(file_path)
  }
}


# ---- Disconnect ----
dbDisconnect(decimal_con)
gc()



# ==============================================================================
# FILE: prep-for-qi-run_dplyr.R
# ==============================================================================


# Prep for QI Run — dplyr Translation
# Original: R/prep-for-qi-run.R (~91 lines)
#
# Pipeline context:
#   After running the regular model, preps the database for a Quality Indicator
#   (QI) model run. Drops intermediate tables that will be re-created, then
#   sources a subset of pipeline scripts (excludes PTIB, near-completers,
#   graduate projections, and program projections which aren't needed for QI).
#
# Key translations:
#   - DROP TABLE IF EXISTS (DDL) → kept as dbExecute (no dplyr equivalent)
#   - File sourcing → unchanged (subset of pipeline scripts)
#
# Flags set:
#   regular_run = FALSE, qi_run = TRUE, ptib_run = FALSE

library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(RJDBC)
source("./R/utils.R")

# ---- Configuration ----
db_config <- config::get("decimal")
my_schema <- config::get("myschema")
regular_run <- F
qi_run <- T
ptib_run <- F

decimal_con <- dbConnect(odbc::odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")

# ******************************************************************************
# Drop intermediate tables for QI re-run
# WHY: QI run re-uses most tables from the regular run but needs to re-create
# some intermediate tables. Commented-out tables are intentionally kept because
# they're needed in the QI run but not in the PTIB run.
# KEPT AS SQL: DROP TABLE IF EXISTS (DDL operation, no dplyr equivalent)
# ******************************************************************************
drop_if_exists <- c(
  # "T_BGS_Data_Final",        # needed in QI run
  "T_Cohorts_Recoded",
  # "t_dacso_data_part_1_stepa", # needed in QI run
  "t_dacso_data_part_1",
  # "infoware_c_outc_clean_short_resp", # needed in QI run
  # "T_TRD_DATA",              # needed in QI run
  # "TRD_Graduates",           # needed in QI run
  "Credential_Non_Dup",
  "T_DACSO_Near_Completers_RatioAgeAtGradCIP4",
  "T_DACSO_Near_Completers_RatioByGender",
  "T_DACSO_Near_Completers_RatioByGender_year",
  "T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN",
  "T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history",
  "population_projections",
  "tbl_Program_Projection_Input",
  # "Cohort_Program_Distributions_Projected", # needed in 07
  "T_PSSM_Credential_Grouping_Appendix",
  "T_LCP2_LCP4",
  "Cohort_Program_Distributions"
)

for (table_name in drop_if_exists) {
  dbExecute(decimal_con, glue::glue(
    "IF OBJECT_ID('{my_schema}.{table_name}', 'U') IS NOT NULL ",
    "DROP TABLE [{my_schema}].[{table_name}];"
  ))
}


# ******************************************************************************
# Execute pipeline scripts for QI run
# WHY: QI run uses a subset of the full pipeline — excludes PTIB steps,
# near-completers, graduate projections, and program projections since the
# QI model is a lighter-weight validation run.
# ******************************************************************************
qi_run_files <- c(
  "./R/load-cohort-appso.R",
  "./R/load-cohort-bgs.R",
  "./R/load-cohort-dacso.R",
  # "./R/load-cohort-trd.R",
  "./R/02b-1-pssm-cohorts.R",
  "./R/02b-2-pssm-cohorts-new-labour-supply.R",
  "./R/02b-3-pssm-cohorts-occupation-distributions.R",
  # "./R/load-near-completers-ttrain.R",
  # "./R/03-near-completers-ttrain.R",
  # "./R/load-graduate-projections.R",
  # "./R/04-graduate-projections.R",
  # "./R/load-ptib.R",
  # "./R/05-ptib-analysis.R",
  "./R/load-program-projections.R",
  # "./R/06-program-projections.R",
  "./R/load-occupation-projections.R",
  "./R/07-occupation-projections.R"
)

print(glue::glue("qi model run flag: {qi_run}"))
if (regular_run != T & qi_run == T & ptib_run != T) {
  for (file_path in qi_run_files) {
    print(glue::glue("regular model run flag: {regular_run}"))
    print(glue::glue("qi model run flag: {qi_run}"))
    print(glue::glue("ptib model furn flag: {ptib_run}"))
    time_execution(file_path)
  }
}


# ---- Disconnect ----
dbDisconnect(decimal_con)
gc()



# ==============================================================================
# FILE: prep-for-ptib-run_dplyr.R
# ==============================================================================


# Prep for PTIB Run — dplyr Translation
# Original: R/prep-for-ptib-run.R (~164 lines)
#
# Pipeline context:
#   After running the regular and QI model runs, preps the database environment
#   for a PTIB (Private Training Institutions) model run. Drops intermediate tables
#   that will be re-created, verifies required tables exist, copies base tables
#   from dbo, then sources pipeline scripts.
#
# Key translations:
#   - DROP TABLE IF EXISTS (DDL) → kept as dbExecute
#   - SELECT INTO (cross-schema copy) → dbReadTable + dbWriteTable
#   - Required table assertions → kept as-is (using dbExistsTable)
#   - File sourcing → unchanged
#
# Flags set:
#   regular_run = FALSE, qi_run = FALSE, ptib_run = TRUE

library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(RJDBC)
source("./R/utils.R")

# ---- Configuration ----
db_config <- config::get("decimal")
my_schema <- config::get("myschema")
regular_run <- F
qi_run <- F
ptib_run <- T

decimal_con <- dbConnect(odbc::odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")

# ---- Verify required tables exist ----
library(assertthat)
required_tables <- c(
  "T_Suppression_Public_Release_NOC",
  "tmp_tbl_Model",
  "tmp_tbl_QI"
)

for (table_name in required_tables) {
  full_table_name <- SQL(glue::glue('"{my_schema}"."{table_name}"'))
  assert_that(
    dbExistsTable(decimal_con, full_table_name),
    msg = paste("Error:", table_name, "does not exist in schema", my_schema)
  )
}

# ******************************************************************************
# Drop intermediate tables for PTIB re-run
# WHY: PTIB run re-uses tables from the regular run but needs to re-create
# intermediate tables that were modified during the regular run. These tables
# will be rebuilt by the downstream pipeline scripts.
# KEPT AS SQL: DROP TABLE IF EXISTS (DDL operation, no dplyr equivalent)
# ******************************************************************************
drop_if_exists <- c(
  "T_BGS_Data_Final",
  "T_Cohorts_Recoded",
  "t_dacso_data_part_1_stepa",
  "t_dacso_data_part_1",
  "infoware_c_outc_clean_short_resp",
  "T_TRD_DATA",
  "TRD_Graduates",
  "Credential_Non_Dup",
  "T_DACSO_Near_Completers_RatioAgeAtGradCIP4",
  "T_DACSO_Near_Completers_RatioByGender",
  "T_DACSO_Near_Completers_RatioByGender_year",
  "T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN",
  "T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history",
  "population_projections",
  "tbl_Program_Projection_Input",
  "Cohort_Program_Distributions_Projected",
  "T_PSSM_Credential_Grouping_Appendix",
  "T_LCP2_LCP4",
  "Cohort_Program_Distributions"
)

for (table_name in drop_if_exists) {
  dbExecute(decimal_con, glue::glue(
    "IF OBJECT_ID('{my_schema}.{table_name}', 'U') IS NOT NULL ",
    "DROP TABLE [{my_schema}].[{table_name}];"
  ))
}

# ******************************************************************************
# Copy required base tables from dbo to user schema
# WHY: Credential_Non_Dup is modified during the pipeline run, so for a PTIB
# re-run it needs to be re-created from the clean dbo original.
# ******************************************************************************
copy_tables <- c("Credential_Non_Dup")

dbBegin(decimal_con)
tryCatch({
  for (table_short in copy_tables) {
    dbExecute(decimal_con, glue::glue(
      "IF OBJECT_ID('{my_schema}.{table_short}', 'U') IS NOT NULL ",
      "DROP TABLE [{my_schema}].[{table_short}];"
    ))
    source_data <- dbReadTable(decimal_con, SQL(glue::glue('"dbo"."{table_short}"')))
    dbWriteTable(decimal_con,
                 SQL(glue::glue('"{my_schema}"."{table_short}"')),
                 source_data, overwrite = TRUE)
  }
  dbCommit(decimal_con)
  print("All tables copied successfully.")
}, error = function(e) {
  dbRollback(decimal_con)
  print(paste("Error:", e$message))
}, finally = {
  dbDisconnect(decimal_con)
})

decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")


# ******************************************************************************
# Execute pipeline scripts for PTIB run
# WHY: Sources each pipeline script in order, including PTIB-specific steps
# (load-ptib.R, 05-ptib-analysis.R) that are excluded from the regular run.
# ******************************************************************************
ptib_run_files <- c(
  "./R/load-cohort-appso.R",
  "./R/load-cohort-bgs.R",
  "./R/load-cohort-dacso.R",
  "./R/load-cohort-trd.R",
  "./R/02b-1-pssm-cohorts.R",
  "./R/02b-2-pssm-cohorts-new-labour-supply.R",
  "./R/02b-3-pssm-cohorts-occupation-distributions.R",
  "./R/load-near-completers-ttrain.R",
  "./R/03-near-completers-ttrain.R",
  "./R/load-graduate-projections.R",
  "./R/04-graduate-projections.R",
  "./R/load-ptib.R",
  "./R/05-ptib-analysis.R",
  "./R/load-program-projections.R",
  "./R/06-program-projections.R",
  "./R/load-occupation-projections.R",
  "./R/07-occupation-projections.R"
)

print(glue::glue("ptib model run flag: {ptib_run}"))
if (regular_run != T & qi_run != T & ptib_run == T) {
  for (file_path in ptib_run_files) {
    print(glue::glue("regular model run flag: {regular_run}"))
    print(glue::glue("qi model run flag: {qi_run}"))
    print(glue::glue("ptib model furn flag: {ptib_run}"))
    time_execution(file_path)
  }
}


# ---- Disconnect ----
dbDisconnect(decimal_con)
gc()



# ==============================================================================
# FILE: zz-adhoc-outputs-lf_dplyr.R
# ==============================================================================


# Ad-hoc Outputs (Labour Force) — dplyr Translation
# Original: R/zz-adhoc-outputs-lf.R (~155 lines)
#
# Pipeline context:
#   Ad-hoc script that fixes Expr1 composite keys in model output tables and
#   produces an internal summary table with Quality Indicator and Coverage Indicator.
#   This is a diagnostic/output script, not part of the main pipeline.
#
# Key translations:
#   - SELECT * FROM (3 reads) → sch_tbl() %>% collect()
#   - Complex analytical SQL with LEFT JOIN, Ceiling, IIf → dplyr left_join + mutate
#   - Quality Indicator: IIf nested logic → case_when
#   - Coverage Indicator: IIf(IsNull...) → if_else / case_when
#
# Input:
#   - tmp_tbl_model — model output (DB)
#   - tmp_tbl_qi — QI model output (DB)
#   - tmp_tbl_Model_Inc_Private_Inst — PTIB model output (DB)
#
# Output:
#   - Fixed tmp_tbl_model, tmp_tbl_qi, tmp_tbl_Model_Inc_Private_Inst (written to DB)
#   - Internal CSV summary with Quality/Coverage indicators

library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(dbplyr)

# ---- Configure LAN and file paths ----
db_config <- config::get("decimal")
lan <- config::get("lan")
my_schema <- config::get("myschema")

decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")

# Helper: reference a table in the user's schema
sch_tbl <- function(name) {
  tbl(decimal_con, dbplyr::in_schema(my_schema, name))
}

# Helper: write to schema
write_schema_table <- function(name, data) {
  dbWriteTable(decimal_con, SQL(glue::glue('"{my_schema}"."{name}"')), data, overwrite = TRUE)
}


# ******************************************************************************
# Fix Expr1 composite keys in model output tables
# WHY: The Expr1 column is a composite key (Age_Group-NOC-Region). In a previous
# iteration, some 5-digit NOC records had incorrectly computed Expr1 values.
# This script regenerates Expr1 for all NOC_Level == 5 rows.
# ******************************************************************************

# ---- Fix tmp_tbl_model ----
tmp_model <- sch_tbl("tmp_tbl_model") %>% collect() |> rename_with(toupper)

tmp_model_fixed <- tmp_model %>%
  mutate(
    EXPR1 = case_when(
      NOC_LEVEL == 5 ~ paste0(AGE_GROUP_ROLLUP_LABEL, "-", NOC, "-", CURRENT_REGION_PSSM_CODE_ROLLUP),
      TRUE ~ EXPR1
    )
  )

# Diagnostic checks
tmp_model %>% count(EXPR1) %>% arrange(desc(n))
tmp_model %>% count(EXPR1) %>% count()
tmp_model %>% count()

tmp_model_fixed %>% count(EXPR1) %>% arrange(desc(n))
tmp_model_fixed %>% count(EXPR1) %>% count()

write_schema_table("tmp_tbl_model", tmp_model_fixed)


# ---- Fix tmp_tbl_qi ----
tmp_qi <- sch_tbl("tmp_tbl_qi") %>% collect() |> rename_with(toupper)

tmp_qi_fixed <- tmp_qi %>%
  mutate(
    EXPR1 = case_when(
      NOC_LEVEL == 5 ~ paste0(AGE_GROUP_ROLLUP_LABEL, "-", NOC, "-", CURRENT_REGION_PSSM_CODE_ROLLUP),
      TRUE ~ EXPR1
    )
  )

# Diagnostic checks
tmp_qi %>% count(EXPR1) %>% arrange(desc(n))
tmp_qi %>% count(EXPR1) %>% count()
tmp_qi %>% count()

tmp_qi_fixed %>% count(EXPR1) %>% arrange(desc(n))
tmp_qi_fixed %>% count(EXPR1) %>% count()

write_schema_table("tmp_tbl_qi", tmp_qi_fixed)


# ---- Fix tmp_tbl_Model_Inc_Private_Inst ----
tmp_tbl_Model_Inc_Private_Inst <- sch_tbl("tmp_tbl_Model_Inc_Private_Inst") %>%
  collect() |> rename_with(toupper)

tmp_tbl_Model_Inc_Private_Inst_fixed <- tmp_tbl_Model_Inc_Private_Inst %>%
  mutate(
    EXPR1 = case_when(
      NOC_LEVEL == 5 ~ paste0(AGE_GROUP_ROLLUP_LABEL, "-", NOC, "-", CURRENT_REGION_PSSM_CODE_ROLLUP),
      TRUE ~ EXPR1
    )
  )

# Diagnostic checks
tmp_tbl_Model_Inc_Private_Inst %>% count(EXPR1) %>% arrange(desc(n))
tmp_tbl_Model_Inc_Private_Inst %>% count(EXPR1) %>% count()
tmp_tbl_Model_Inc_Private_Inst %>% count()

tmp_tbl_Model_Inc_Private_Inst_fixed %>% count(EXPR1) %>% arrange(desc(n))
tmp_tbl_Model_Inc_Private_Inst_fixed %>% count(EXPR1) %>% count()

write_schema_table("tmp_tbl_Model_Inc_Private_Inst", tmp_tbl_Model_Inc_Private_Inst_fixed)


# ******************************************************************************
# Create internal summary table with Quality/Coverage indicators
# WHY: The original used a complex SQL query with LEFT JOINs, Ceiling() rounding,
# and nested IIf() logic for Quality Indicator and Coverage Indicator.
# We translate this to dplyr joins + mutate.
#
# Quality Indicator: measures how much the model projection differs from the QI baseline.
#   - If the difference is < 25%, report the actual ratio.
#   - If either value is < 10 or NULL, flag as '999999999' (unreliable).
#   - Otherwise, report the raw ratio.
#
# Coverage Indicator: ratio of model (without PTIB) to model (with PTIB).
#   - If PTIB value is 0 or NULL, report 0.
#   - Otherwise, model / PTIB model.
# ******************************************************************************

# Identify year columns (matching pattern like "2023/2024")
year_cols <- names(tmp_model_fixed)[grepl("^\\d{4}/\\d{4}$", names(tmp_model_fixed))]

# Join model with QI and PTIB model on Expr1
internal <- tmp_model_fixed %>%
  filter(NOC_LEVEL == 5) %>%
  # Left join with QI — suffix to distinguish QI year columns
  left_join(
    tmp_qi_fixed %>% select(EXPR1, all_of(paste0("`", year_cols, "`"))),
    by = "EXPR1",
    suffix = c("", ".QI")
  ) %>%
  # Left join with PTIB model — suffix to distinguish PTIB year columns
  left_join(
    tmp_tbl_Model_Inc_Private_Inst_fixed %>% select(EXPR1, all_of(paste0("`", year_cols, "`"))),
    by = "EXPR1",
    suffix = c("", ".PTIB")
  )

# Apply Ceiling to all year columns from the model
# WHY: The original used Ceiling() to round up all year projections.
internal <- internal %>%
  mutate(across(all_of(year_cols), ~ceiling(.)))

# Compute Quality Indicator and Coverage Indicator for the base year
# WHY: The original used nested IIf() logic. We translate to case_when() for clarity.
base_year <- "2023/2024"
qi_col <- paste0(base_year, ".QI")
ptib_col <- paste0(base_year, ".PTIB")

# Only compute if the QI and PTIB columns exist after the join
if (qi_col %in% names(internal) && ptib_col %in% names(internal)) {
  internal <- internal %>%
    mutate(
      # Quality Indicator: how much does model differ from QI?
      `Quality Indicator` = case_when(
        # Difference < 25%: report the actual ratio
        abs(!!sym(base_year) - !!sym(qi_col)) / !!sym(qi_col) < 0.25
          ~ abs(!!sym(base_year) - !!sym(qi_col)) / !!sym(qi_col),
        # Small values or NULL: flag as unreliable
        !!sym(base_year) < 10 | !!sym(qi_col) < 10 |
          is.na(!!sym(base_year)) | is.na(!!sym(qi_col))
          ~ 999999999,
        # Default: report the raw ratio
        TRUE
          ~ abs(!!sym(base_year) - !!sym(qi_col)) / !!sym(qi_col)
      ),
      # Coverage Indicator: model / model_with_PTIB
      `Coverage Indicator` = case_when(
        is.na(!!sym(ptib_col)) | !!sym(ptib_col) == 0 ~ 0,
        TRUE ~ !!sym(base_year) / !!sym(ptib_col)
      )
    )
}

# Select and order columns matching original output
internal <- internal %>%
  select(EXPR1, AGE_GROUP_ROLLUP_LABEL, NOC_LEVEL, NOC, ENGLISH_NAME,
         CURRENT_REGION_PSSM_CODE_ROLLUP, CURRENT_REGION_PSSM_NAME_ROLLUP,
         all_of(year_cols),
         `Quality Indicator`, `Coverage Indicator`) %>%
  arrange(AGE_GROUP_ROLLUP_LABEL, NOC)

# Write to CSV
internal %>% write_csv(glue::glue("{lan}/development/work/adhoc-outputs/internal-occs-20240926.csv"))


# ---- Clean Up ----
dbDisconnect(decimal_con)



# ==============================================================================
# FILE: zz-graduate-historical-forecasted_dplyr.R
# ==============================================================================


# Graduate Historical + Forecasted Table — dplyr Translation
# Original: R/zz-graduate-historical-forecasted.R
#
# Pipeline context:
#   Ad-hoc reporting script that combines historical graduate counts with projected
#   graduate counts in a single table for presentation and comparison purposes. This
#   lets analysts see both past actuals and future forecasts side-by-side.
#
#   This script sits downstream of the main pipeline — it reads from tables produced
#   by scripts 04 (Graduate_Projections) and 06 (Cohort_Program_Distributions).
#   It is NOT part of the main model run (not sourced by the orchestrator scripts).
#
# Input tables:
#   - Graduate_Projections — from 04-graduate-projections
#   - Cohort_Program_Distributions — from 06-program-projections
#   - Graduate_Projections_Include_Historical — from 04-graduate-projections
#   - T_Exclude_from_Projections_* — exclusion lists for specific CIP/credential types
#   - tbl_Age_Groups / tbl_Age_Groups_Rollup — age group lookup tables
#   - T_PSSM_Credential_Grouping_Appendix — credential grouping for presentation
#
# Output:
#   - qry99_Presentations_Graduates_Appendix — pivoted summary (console output)
#   - CSV file written to LAN for ad-hoc analysis

library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(dbplyr)
library(ggplot2)

# ---- Helper function for referencing schema-qualified tables ----
sch_tbl <- function(name) { tbl(con, dbplyr::in_schema(my_schema, name)) }

# ---- Configure LAN and file paths ----
db_config <- config::get("decimal")
lan <- config::get("lan")
my_schema <- config::get("myschema")

# ---- Connection to decimal ----
db_config <- config::get("decimal")
con <- dbConnect(odbc::odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")

# ---- Check for required data tables ----
# Derived tables (created by earlier pipeline scripts)
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."Cohort_Program_Distributions"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."Graduate_Projections"')))

# Lookups
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."T_Exclude_from_Projections_LCP4_CD"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."T_Exclude_from_Projections_LCIP4_CRED"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."T_Exclude_from_Projections_PSSM_Credential"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."tbl_Age_Groups"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."tbl_Age_Groups_Rollup"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."T_PSSM_Credential_Grouping_Appendix"')))


# We need to apply exclusion lists to the graduate projections before presenting
# them. Some CIP codes and credential types are excluded from the final reports.
# ---- Q_1 Series: Projected graduates by program ----

# Pull reference tables from DB
exclude_lcp4 <- sch_tbl("T_Exclude_from_Projections_LCP4_CD") %>% collect() |> rename_with(toupper)
exclude_pssm_cred <- sch_tbl("T_Exclude_from_Projections_PSSM_Credential") %>% collect() |> rename_with(toupper)
exclude_lcip4_cred <- sch_tbl("T_Exclude_from_Projections_LCIP4_CRED") %>% collect() |> rename_with(toupper)
grad_proj <- sch_tbl("Graduate_Projections") %>% collect() |> rename_with(toupper)
cohort_prog_dist <- sch_tbl("Cohort_Program_Distributions") %>% collect() |> rename_with(toupper)

# Join projections with program distributions, then exclude specific CIPs/credentials
# WHY: Program distributions split projected graduates across CIP programs. The exclusion
# lists remove CIP codes and credential types that shouldn't appear in reports.
Q_1_Grad_Projections_by_Age_by_Program <- grad_proj %>%
  inner_join(cohort_prog_dist,
    by = c("YEAR" = "YEAR", "AGE_GROUP" = "AGE_GROUP", "PSSM_CRED" = "PSSM_CRED")) %>%
  anti_join(exclude_lcp4,
    by = c("LCP4_CD" = "LCIP_LCP4_CD")) %>%
  anti_join(exclude_pssm_cred,
    by = c("PSSM_CREDENTIAL" = "PSSM_CREDENTIAL")) %>%
  anti_join(exclude_lcip4_cred,
    by = c("LCIP4_CRED" = "LCIP4_CRED")) %>%
  mutate(GRADS = GRADUATES * PERCENT) %>%
  select(PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP, YEAR,
         LCP4_CD, GRAD_STATUS, TTRAIN, LCIP4_CRED, GRADS)

dbWriteTable(con, SQL(glue::glue('"{my_schema}"."Q_1_Grad_Projections_by_Age_by_Program"')),
             Q_1_Grad_Projections_by_Age_by_Program, overwrite = TRUE)


# Roll up age groups for program-level projections
# WHY: Reports show graduates at a coarser age grouping than the detailed model uses.
# The tbl_Age_Groups and tbl_Age_Groups_Rollup tables define the mapping.
age_groups <- sch_tbl("tbl_Age_Groups") %>% collect() |> rename_with(toupper)
age_groups_rollup <- sch_tbl("tbl_Age_Groups_Rollup") %>% collect() |> rename_with(toupper)

Q_1c_Grad_Projections_by_Program <- Q_1_Grad_Projections_by_Age_by_Program %>%
  inner_join(age_groups, by = c("AGE_GROUP" = "AGE_GROUP_LABEL")) %>%
  inner_join(age_groups_rollup, by = c("AGE_GROUP_ROLLUP" = "AGE_GROUP_ROLLUP")) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP_ROLLUP, AGE_GROUP_ROLLUP_LABEL,
           YEAR, GRAD_STATUS, TTRAIN, LCP4_CD, LCIP4_CRED) %>%
  summarise(GRADS = sum(GRADS), .groups = "drop") %>%
  select(PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP_ROLLUP, AGE_GROUP_ROLLUP_LABEL,
         YEAR, GRAD_STATUS, TTRAIN, LCP4_CD, LCIP4_CRED, GRADS)

dbWriteTable(con, SQL(glue::glue('"{my_schema}"."Q_1c_Grad_Projections_by_Program"')),
             Q_1c_Grad_Projections_by_Program, overwrite = TRUE)


# The PIVOT query creates a human-readable cross-tabulation with years as columns
# and credential types as rows — the format needed for the appendix report.
# KEPT AS SQL: SQL Server PIVOT has no dplyr equivalent
# ---- Final Presentation Table ----
qry99_Presentations_Graduates_Appendix <-
  "SELECT Age_Group_Rollup_Label, PSSM_Credential_Name,
[2023/2024],
[2024/2025],
[2025/2026],
[2026/2027],
[2027/2028],
[2028/2029],
[2029/2030],
[2030/2031],
[2031/2032],
[2032/2033],
[2033/2034],
[2034/2035]
FROM (
SELECT Q_1c_Grad_Projections_by_Program.Age_Group_Rollup_Label,
Q_1c_Grad_Projections_by_Program.Year as yr,
T_PSSM_Credential_Grouping_Appendix.PSSM_Credential_Name,
Grads
FROM T_PSSM_Credential_Grouping_Appendix
INNER JOIN Q_1c_Grad_Projections_by_Program
	ON T_PSSM_Credential_Grouping_Appendix.PSSM_Credential = Q_1c_Grad_Projections_by_Program.PSSM_Credential
WHERE (((Q_1c_Grad_Projections_by_Program.PSSM_CRED) Not Like 'P - %'))
) AS SourceTable
PIVOT (
    Sum([Grads]) FOR Yr IN ([2023/2024],
[2024/2025],
[2025/2026],
[2026/2027],
[2027/2028],
[2028/2029],
[2029/2030],
[2030/2031],
[2031/2032],
[2032/2033],
[2033/2034],
[2034/2035])
) AS PivotTable;"
dbGetQuery(con, qry99_Presentations_Graduates_Appendix) %>%
  mutate(across(where(is.numeric), round))

# KEPT AS SQL: DROP TABLE (DDL)
dbExecute(con, "DROP TABLE Q_1_Grad_Projections_by_Age_by_Program")
dbExecute(con, "DROP TABLE Q_1c_Grad_Projections_by_Program")


# To compare historical actuals with projections, we pull data from the
# Graduate_Projections_Include_Historical table (which has both) and the
# Graduate_Projections table (which has projections only), then verify they match
# for the base year (2023/2024).
# ---- Historical + Projected Graduates for Comparison ----

# Pull historical + projected graduates with credential groupings for display
grads <- sch_tbl("Graduate_Projections_Include_Historical") %>%
  select(age_group, PSSM_CREDENTIAL, year, graduates) %>%
  collect() |> rename_with(toupper) %>%
  left_join(
    sch_tbl("T_PSSM_Credential_Grouping_Appendix") %>% collect() |> rename_with(toupper),
    by = c("PSSM_CREDENTIAL" = "PSSM_CREDENTIAL")
  ) %>%
  left_join(
    sch_tbl("tbl_Age_Groups") %>% collect() |> rename_with(toupper) %>%
      select(AGE_GROUP_LABEL, AGE_GROUP_ROLLUP),
    by = c("AGE_GROUP" = "AGE_GROUP_LABEL")
  ) %>%
  left_join(
    sch_tbl("tbl_Age_Groups_Rollup") %>% collect() |> rename_with(toupper),
    by = c("AGE_GROUP_ROLLUP" = "AGE_GROUP_ROLLUP")
  ) %>%
  select(AGE_GROUP, AGE_GROUP_ROLLUP_LABEL, PSSM_CREDENTIAL_NAME, YEAR, GRADUATES)

# Pull projected graduates only (for comparison with historical base year)
grads_proj <- sch_tbl("Graduate_Projections") %>%
  select(age_group, PSSM_CREDENTIAL, year, graduates) %>%
  collect() |> rename_with(toupper) %>%
  left_join(
    sch_tbl("T_PSSM_Credential_Grouping_Appendix") %>% collect() |> rename_with(toupper),
    by = c("PSSM_CREDENTIAL" = "PSSM_CREDENTIAL")
  ) %>%
  left_join(
    sch_tbl("tbl_Age_Groups") %>% collect() |> rename_with(toupper) %>%
      select(AGE_GROUP_LABEL, AGE_GROUP_ROLLUP),
    by = c("AGE_GROUP" = "AGE_GROUP_LABEL")
  ) %>%
  left_join(
    sch_tbl("tbl_Age_Groups_Rollup") %>% collect() |> rename_with(toupper),
    by = c("AGE_GROUP_ROLLUP" = "AGE_GROUP_ROLLUP")
  ) %>%
  select(AGE_GROUP, AGE_GROUP_ROLLUP_LABEL, PSSM_CREDENTIAL_NAME, YEAR, GRADUATES)

# Verify projections match historical data for the base year (2023/2024)
grads %>%
  filter(YEAR>='2023/2024', !grepl('Apprenticeship', PSSM_CREDENTIAL_NAME)) %>%
  all.equal(grads_proj %>% filter(!grepl('Apprenticeship', PSSM_CREDENTIAL_NAME)))

grads %>% filter(YEAR == '2023/2024')
grads_proj %>% filter(YEAR == '2023/2024')

# Fill in missing years for a complete time series
# WHY: Some credential/age combinations may have gaps in the historical data.
# fill() carries the last known value forward to create a continuous series.
grads_completed <-  grads %>%
  arrange(PSSM_CREDENTIAL_NAME, AGE_GROUP, YEAR) %>%
  complete(PSSM_CREDENTIAL_NAME, AGE_GROUP, YEAR) %>%
  group_by(PSSM_CREDENTIAL_NAME, AGE_GROUP) %>%
  fill(GRADUATES, AGE_GROUP_ROLLUP_LABEL)

grads_completed %>% View()

grads_completed %>% filter(PSSM_CREDENTIAL_NAME == 'Apprenticeship') %>%
  filter(AGE_GROUP_ROLLUP_LABEL == '17 to 29') %>%
  filter(YEAR>='2023/2024')

# Create a wide-format summary table for CSV export
# WHY: The final output is a CSV file with years as columns, credential types and
# age groups as rows — the format analysts need for charting and comparison.
grads_by_age_cred <- grads_completed %>%
  filter(YEAR>='2018/2019') %>%
  group_by(AGE_GROUP_ROLLUP_LABEL, PSSM_CREDENTIAL_NAME, YEAR) %>%
  summarise(n = round(sum(GRADUATES, drop.na=TRUE), 0)) %>%
  pivot_wider(id_cols = c('AGE_GROUP_ROLLUP_LABEL', 'PSSM_CREDENTIAL_NAME'), names_from = 'YEAR', values_from = 'n') %>%
  arrange(PSSM_CREDENTIAL_NAME, AGE_GROUP_ROLLUP_LABEL) %>%
  filter(!is.na(AGE_GROUP_ROLLUP_LABEL))


# Diagnostic plot: graduates over time by credential type
grads_completed %>%
  mutate(YEAR = as.numeric(str_sub(YEAR, 1,4))) %>%
  group_by(PSSM_CREDENTIAL_NAME, YEAR) %>%
  summarize(n = sum(GRADUATES)) %>% # View()
  ggplot(aes(x = YEAR, y=n, color=PSSM_CREDENTIAL_NAME)) +
  geom_line()+
  geom_vline(aes(xintercept = 2023))

# Write summary CSV to LAN for ad-hoc analysis
grads_by_age_cred %>% write_csv(
  glue::glue('{lan}\\development\\work\\adhoc-outputs\\graduate_projections_include_historical_no_ptib.csv')
)

