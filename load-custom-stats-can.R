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

# ---- Define Schema ----

# ---- Write to decimal ----

# ---- Read from decimal ----

# ---- Light QA ----