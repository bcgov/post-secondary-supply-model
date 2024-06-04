# ---- Required SSMS TABLES
library(odbc)
library(tidyverse)
library(DBI)
library(config)

source("./sql/02a-dacso_program_matching.R")

#---- Connect to Decimal ----
db_config <- config::get("decimal")
con <- dbConnect(odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")
dbGetQuery(con, qry_Credential_Non_Dup_Add_Columns)
dbGetQuery(con, qry_DASCO_STP_Credential_Programs)

close(con)