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
