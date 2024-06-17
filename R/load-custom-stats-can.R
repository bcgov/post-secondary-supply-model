# ******************************************************************************
# Load custom Statistics Canada data from staging area in LAN project folder, to decimal.  
# note: the following data type conversion problems have been seen
#   - dates are uploaded format YY-MM-DD instead of YYYY-MM-DD
# ******************************************************************************

# ---- libraries and global variables
library(arrow)
library(tidyverse)
library(odbc)
library(DBI)
library(safepaths)
library(config)

# ---- Configure LAN Paths and DB Connection ----

# ----- Connection to decimal ----
db_config <- config::get("decimal")
con <- dbConnect(odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")

# ---- Define Schema ----

# ---- Write to decimal ----

# ---- Read from decimal ----

# ---- Light QA ----