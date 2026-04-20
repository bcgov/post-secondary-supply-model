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
