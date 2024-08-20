# Update Credential Non Dup
# Description: 
# Relies on:
#   - credential_non_dup, 
#   - Credential_Non_Dup_Programs_DACSO_FinalCIPs
#   - Credential_Non_Dup_BGS_IDs
#   - Credential_Non_Dup_GRAD_IDs
# Creates updated credential non duplicate table with updated CIP records
# Uses work done during program matching

library(arrow)
library(tidyverse)
library(odbc)
library(DBI)

# ---- Configure LAN Paths and DB Connection -----
lan <- config::get("lan")
source("./sql/02a-program-matching/02a-update-cred-non-dup.R")

db_config <- config::get("decimal")
my_schema <- config::get("myschema")

con <- dbConnect(odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")

# ---- Check Required Tables ----
# main table
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."credential_non_dup"')))

# tables with CIP updates 
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."Credential_Non_Dup_Programs_DACSO_FinalCIPs"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."Credential_Non_Dup_BGS_IDs"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."Credential_Non_Dup_GRAD_IDs"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."Credential_Non_Dup_APPSO_IDs"')))

# reference tables
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_2DIGITS_CIP2016"')))

# START QUERIES ----
# ---- Create additional required columns ----
dbExecute(con, qry_Credential_Non_Dup_Add_Columns) 

# ---- Update non dup with new CIP codes from DACSO, BGS and GRAD records ----
dbExecute(con, qry_update_Credential_Non_Dup_DACSO_Final_CIPs) 
dbExecute(con, qry_update_Credential_Non_Dup_BGS_Final_CIPs) 
dbExecute(con, qry_update_Credential_Non_Dup_GRAD_Final_CIPs) 
dbExecute(con, qry_update_Credential_Non_Dup_APPSO_Final_CIPs) 

# ---- update cluster codes for GRAD and APPSO (was left out of previous code)
dbExecute(con, qry_update_Credential_Non_Dup_GRAD_APPSO_Cluster)

# ---- clean up queries, clean up 'undeclared activities' ----
dbExecute(con, SQLQuery4)
dbExecute(con, SQLQuery6)
dbExecute(con, SQLQuery7)

# ---- Clean up ----
dbDisconnect(con)