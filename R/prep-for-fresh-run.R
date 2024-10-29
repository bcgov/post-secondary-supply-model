# *********************************************************************************
# After running the regular and QI model and PTIB runs, prep for re-run if needed.
# Option 1: CAUTION: Drop EVERY single table in your SQL IDIR schema (2 ways)
# Option 2: Drop select tables required for a fresh run from 02b of regular
# *********************************************************************************
library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(RJDBC)

# ---- Configuration ----
db_config <- config::get("decimal")
my_schema <- config::get("myschema")

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
tables <- dbGetQuery(decimal_con, tables_query)$TABLE_NAME

# Step 2: Begin transaction and delete tables
# commented out to prevent accidental deletions
# REMINDER: ALL IDIR tables WILL be deleted; confirm my_schema used in this process
my_schema

# dbBegin(decimal_con)
# tryCatch({
#   for (table in tables) {
#     drop_statement <- glue::glue('DROP TABLE "{my_schema}"."{table}"')
#     dbExecute(decimal_con, drop_statement)
#   }
#   dbCommit(decimal_con)  # Commit transaction if all deletions succeed
#   print("All tables deleted successfully.")
# }, error = function(e) {
#   dbRollback(decimal_con)  # Rollback if there's an error
#   print(paste("Error:", e$message))
# }, finally = {
#   dbDisconnect(decimal_con)
# })
  
# ---- 2. Drop specific tables required for re-run ----
# assumes you also ran the drops in 07-occupation-projections.R
dbExecute(decimal_con, glue::glue("DROP TABLE [{my_schema}].[T_Suppression_Public_Release_NOC];"))
dbExecute(decimal_con, glue::glue("DROP TABLE [{my_schema}].[qry_Private_Credentials_05i1_Grads_by_Year];"))
dbExecute(decimal_con, glue::glue("DROP TABLE [{my_schema}].[tmp_tbl_Model];"))
dbExecute(decimal_con, glue::glue("DROP TABLE [{my_schema}].[tmp_tbl_Model_Inc_Private_Inst];"))
dbExecute(decimal_con, glue::glue("DROP TABLE [{my_schema}].[tmp_tbl_Model_Program_Projection];"))
dbExecute(decimal_con, glue::glue("DROP TABLE [{my_schema}].[tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union];"))
dbExecute(decimal_con, glue::glue("DROP TABLE [{my_schema}].[tmp_tbl_QI];"))
dbExecute(decimal_con, glue::glue("DROP TABLE [{my_schema}].[Labour_Supply_Distribution];"))
dbExecute(decimal_con, glue::glue("DROP TABLE [{my_schema}].[Labour_Supply_Distribution_No_TT];"))
dbExecute(decimal_con, glue::glue("DROP TABLE [{my_schema}].[Labour_Supply_Distribution_LCP2_No_TT];"))
dbExecute(decimal_con, glue::glue("DROP TABLE [{my_schema}].[Labour_Supply_Distribution_LCP2];"))
dbExecute(decimal_con, glue::glue("DROP TABLE [{my_schema}].[Occupation_Distributions];"))
dbExecute(decimal_con, glue::glue("DROP TABLE [{my_schema}].[Occupation_Distributions_no_TT];"))
dbExecute(decimal_con, glue::glue("DROP TABLE [{my_schema}].[Occupation_Distributions_LCP2];"))
dbExecute(decimal_con, glue::glue("DROP TABLE [{my_schema}].[Occupation_Distributions_LCP2_no_tt];"))
dbExecute(decimal_con, glue::glue("DROP TABLE [{my_schema}].[Occupation_Distributions_LCP2_bc_no_tt];"))
dbExecute(decimal_con, glue::glue("DROP TABLE [{my_schema}].[Occupation_Distributions_LCP2_bc];"))

# ---- Disconnect ----
dbDisconnect(decimal_con)
rm(list = ls())

