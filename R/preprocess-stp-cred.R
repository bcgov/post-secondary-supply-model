library(arrow)
library(tidyverse)
library(odbc)
library(DBI)
library(safepaths)

# ---- Configure LAN Paths and DB Connection -----
# set_network_path("<path_to_2023_project_folder>")
lan <- get_network_path()

# set connection string to decimal
db_config <- config::get("decimal")
con <- dbConnect(odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")

source("./sql/preprocess-stp-cred.R")

# ---- Run Queries -------------------------------
dbGetQuery(con, qry00a_check_null_epens)
dbGetQuery(con, qry00b_check_unique_epens)
dbGetQuery(con, qry00c_CreateIDinSTPCredential)
dbGetQuery(con, qry00d_SetPKeyinSTPCredential)
dbGetQuery(con, qry01_ExtractAllID_into_STP_Credential_Record_Type)



