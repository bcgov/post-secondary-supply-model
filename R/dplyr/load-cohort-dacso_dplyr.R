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
