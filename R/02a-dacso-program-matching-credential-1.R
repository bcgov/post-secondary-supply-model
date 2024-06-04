# ---- Required Tables (SSMS) ----
# Credential_Non_Dup

library(odbc)
library(tidyverse)
library(DBI)
library(config)
library(RODBC)

source("./sql/02a-dacso-program-matching.R")

#---- Connect to Decimal ----
db_config <- config::get("decimal")
con <- dbConnect(odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")

# ---- Make STP_Credential_Non_Dup_Programs_DACSO ----
dbGetQuery(con, qry_Credential_Non_Dup_Add_Columns)
dbGetQuery(con, qry_DASCO_STP_Credential_Programs)

# ---- Move STP_Credential_Non_Dup_Programs_DACSO to Outcomes Database
cred_non_dup <- dbGetQuery(con, "SELECT * FROM STP_Credential_Non_Dup_Programs_DACSO")

dbDisconnect(con)
connection <- config::get("connection")$outcomes_dacso
con <- odbcDriverConnect(connection)

sqlSave(con, dat = cred_non_dup, tablename = 'dbo_STP_Credential_Non_Dup_Programs_DACSO', append = FALSE, colnames = FALSE)

close(con)

