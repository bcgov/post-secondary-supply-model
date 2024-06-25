# ******************************************************************************
# Graduate NOC imputations on Stat Can data  
# ******************************************************************************

# ---- libraries and global variables
library(arrow)
library(tidyverse)
library(odbc)
library(DBI)
library(janitor)

# ----- Connection to decimal ----
db_config <- config::get("decimal")
con <- dbConnect(odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")

# ---- Required Tables ----
# Stat Can data: See raw data documentation
# STAT_CAN
dbExistsTable(con, "STAT_CAN")

# ---- Read from decimal ----
stat_can_data_raw <- dbReadTable(con, "STAT_CAN")

# ---- Disconnect ----
dbDisconnect(con)
