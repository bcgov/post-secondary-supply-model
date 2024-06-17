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
library(janitor)

# ---- Configure LAN Paths and DB Connection ----
lan <- config::get("lan")
raw_data_file <- glue::glue("{lan}/data/statcan/stat-can-data-export.csv")

# ----- Connection to decimal ----
db_config <- config::get("decimal")
con <- dbConnect(odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")

# ---- Read raw data  ----
#raw_data <- read_csv(raw_data_file) # won't run due to funky apostrophe in header
raw_data <- read_csv(raw_data_file,locale=locale(encoding="latin1"))

# ---- Define Schema ----

# ---- Write to decimal ----

# ---- Read from decimal ----

# ---- Light QA ----