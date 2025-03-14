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

# *********************************************************************************
# After running the regular and QI model and PTIB runs, prep for re-run if needed.
# Option 1: CAUTION: Drop EVERY single table in your SQL IDIR schema (2 ways)
# Option 2: Drop select tables required for a fresh run from 02b of regular
# *********************************************************************************
# rm(list = ls()) # cannot have this anymore since it is a part of 'run_all_three_model_runs.r'. Otherwise, it will remove all global variables such as file_path0
library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(RJDBC)
source("./R/utils.R")
# ---- Configuration ----
db_config <- config::get("decimal")
my_schema <- config::get("myschema")
# initiate flags
regular_run <-  T
qi_run <- F
ptib_run <- F

# ---- Connection to decimal ----
db_config <- config::get("decimal")
decimal_con <- dbConnect(odbc::odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")

# ---- 1. CAUTION: delete every table in your schema ----
# Option 1a
#Open a query in SQL Server Mgmt Studio and run/execute the following:

# Select 'dbExecute(decimal_con, glue::glue("Drop Table [{my_schema}].[' + Table_Name + '];"))' 
# From   Information_Schema.Tables 
# Where  Table_Schema = 'IDIR\IDIR_NAME'

#Then use the output to run in R (exclude any tables you don't want to drop)

## ALTERNATIVELY Option 1b
# Step 1: Retrieve all table names in the schema
tables_query <- paste0(
  "SELECT TABLE_NAME
     FROM INFORMATION_SCHEMA.TABLES
     WHERE TABLE_SCHEMA = '", my_schema, "' AND TABLE_TYPE = 'BASE TABLE'"
)
all_tables <- dbGetQuery(decimal_con, tables_query)$TABLE_NAME

# remove all other non-raw student outcome data.
remove_tables <- all_tables[stringr::str_detect(all_tables, pattern = "_raw$", negate = T) ]


# Step 2: Begin transaction and delete tables
# commented out to prevent accidental deletions
# REMINDER: ALL IDIR tables WILL be deleted; confirm my_schema used in this process

dbBegin(decimal_con)
tryCatch({
  for (table in remove_tables) {
    drop_statement <- glue::glue('DROP TABLE "{my_schema}"."{table}"')
    dbExecute(decimal_con, drop_statement)
  }
  dbCommit(decimal_con)  # Commit transaction if all deletions succeed
  print("All tables deleted successfully.")
}, error = function(e) {
  dbRollback(decimal_con)  # Rollback if there's an error
  print(paste("Error:", e$message))
}, finally = {
  dbDisconnect(decimal_con)
})

decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")
  
# ---- 2. Drop specific tables required for re-run ----
# assumes you also ran the drops in 07-occupation-projections.R
dbExecute(decimal_con, glue::glue("IF OBJECT_ID('[{my_schema}].[T_Suppression_Public_Release_NOC]', 'U') IS NOT NULL DROP TABLE [{my_schema}].[T_Suppression_Public_Release_NOC];"))
dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.qry_Private_Credentials_05i1_Grads_by_Year', 'U') IS NOT NULL DROP TABLE [{my_schema}].[qry_Private_Credentials_05i1_Grads_by_Year];"))
dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.tmp_tbl_Model', 'U') IS NOT NULL DROP TABLE [{my_schema}].[tmp_tbl_Model];"))
dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.tmp_tbl_Model_Inc_Private_Inst', 'U') IS NOT NULL DROP TABLE [{my_schema}].[tmp_tbl_Model_Inc_Private_Inst];"))
dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.tmp_tbl_Model_Program_Projection', 'U') IS NOT NULL DROP TABLE [{my_schema}].[tmp_tbl_Model_Program_Projection];"))
dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union', 'U') IS NOT NULL DROP TABLE [{my_schema}].[tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union];"))
dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.tmp_tbl_QI', 'U') IS NOT NULL DROP TABLE [{my_schema}].[tmp_tbl_QI];"))
dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.Labour_Supply_Distribution', 'U') IS NOT NULL DROP TABLE [{my_schema}].[Labour_Supply_Distribution];"))
dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.Labour_Supply_Distribution_No_TT', 'U') IS NOT NULL DROP TABLE [{my_schema}].[Labour_Supply_Distribution_No_TT];"))
dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.Labour_Supply_Distribution_LCP2_No_TT', 'U') IS NOT NULL DROP TABLE [{my_schema}].[Labour_Supply_Distribution_LCP2_No_TT];"))
dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.Labour_Supply_Distribution_LCP2', 'U') IS NOT NULL DROP TABLE [{my_schema}].[Labour_Supply_Distribution_LCP2];"))
dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.Occupation_Distributions', 'U') IS NOT NULL DROP TABLE [{my_schema}].[Occupation_Distributions];"))
dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.Occupation_Distributions_no_TT', 'U') IS NOT NULL DROP TABLE [{my_schema}].[Occupation_Distributions_no_TT];"))
dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.Occupation_Distributions_LCP2', 'U') IS NOT NULL DROP TABLE [{my_schema}].[Occupation_Distributions_LCP2];"))
dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.Occupation_Distributions_LCP2_no_tt', 'U') IS NOT NULL DROP TABLE [{my_schema}].[Occupation_Distributions_LCP2_no_tt];"))
dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.Occupation_Distributions_LCP2_bc_no_tt', 'U') IS NOT NULL DROP TABLE [{my_schema}].[Occupation_Distributions_LCP2_bc_no_tt];"))
dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.Occupation_Distributions_LCP2_bc', 'U') IS NOT NULL DROP TABLE [{my_schema}].[Occupation_Distributions_LCP2_bc];"))



# ---- 3. Copy tables required for re-run ----
# copy those tables. those tables (Credential_Non_Dup) are changed during the steps so it needs to copy again from scratch.
copy_tables = c(
  '[dbo]."T_bgs_data_final_for_outcomesmatching"',
  '[dbo]."Labour_Supply_Distribution_Stat_Can"',
  '[dbo]."Occupation_Distributions_Stat_Can"',
  '[dbo]."Credential_Non_Dup"'
)

dbBegin(decimal_con)
tryCatch({
  for (table in copy_tables) {
    # Extract the part after the dot
    table_short <- str_extract(table, '(?<=\\.)"[^"]+"') %>% str_remove_all("\"")
    # must have the SQL to make dbExistsTable work
    # if (!dbExistsTable(decimal_con, SQL(glue::glue("{my_schema}.{table_short}")))){
    # Some tables will be changed by the code so it is better to recreate them.
      copy_statement <- glue::glue('SELECT * 
               INTO [{my_schema}].{table_short}
               FROM {table};')  
      dbExecute(decimal_con, copy_statement)
    # }

  }
  dbCommit(decimal_con)  # Commit transaction if all deletions succeed
  print("All tables copied successfully.")
}, error = function(e) {
  dbRollback(decimal_con)  # Rollback if there's an error
  print(paste("Error:", e$message))
}, finally = {
  dbDisconnect(decimal_con)
})

decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")

# ---- 4. re-run step by step ----



# List of R file paths
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
  # "./R/load-ptib.R",
  # "./R/05-ptib-analysis.R",
  "./R/load-program-projections.R",
  "./R/06-program-projections.R",
  "./R/load-occupation-projections.R",
  "./R/07-occupation-projections.R"
)


# for regular run

print(glue::glue("regular model run flag: {regular_run}"))
if (regular_run == T & qi_run != T & ptib_run !=T ){
  
  
  # Loop through each file, calling time_execution for each
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
