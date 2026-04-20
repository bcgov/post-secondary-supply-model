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
