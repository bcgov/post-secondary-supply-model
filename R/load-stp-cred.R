# ******************************************************************************
# Load STP credential data from staging area in LAN project folder, to decimal.  
# Requested data to be in tab-separated text file
# STP credential data loads with a few data type conversion problems: 
#   - querying SQL data returns PSI_CREDENTIAL_PROGRAM_DESC as a quoted string
#   - dates are uploaded format YY-MM-DD instead of YYYY-MM-DD
# ******************************************************************************
library(arrow)
library(tidyverse)
library(odbc)
library(DBI)
library(config)

# ---- Configure LAN and file paths ----
lan <- config::get("lan")
raw_data <- glue::glue("{lan}/data/stp/STP_ISA_PSSM/STP_CREDENTIAL_2023.dsv")

## ----- Connection to decimal ----
db_config <- config::get("decimal")
con <- dbConnect(odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")

# ---- Define Schema ----
schema <- 
  schema(CREDENTIAL_AWARD_DATE = string(),
         PSI_CODE = string(),
         PSI_FULL_NAME = string(),
         PSI_PEN = string(),
         PSI_STUDENT_NUMBER = string(),
         PSI_SCHOOL_YEAR = string(),
         PSI_PROGRAM_CODE = string(),
         PSI_PROGRAM_EFFECTIVE_DATE = string(),
         PSI_CREDENTIAL_CATEGORY =string(),
         PSI_CREDENTIAL_LEVEL = string(),
         PSI_CREDENTIAL_CIP = string(),
         PSI_CREDENTIAL_PROGRAM_DESCRIPTION = string(),
         SNAPSHOT_DATE = string(),
         ENCRYPTED_TRUE_PEN = string(),
         STP_ALT_ID = string())

# ---- Write to decimal ----
tblnm <- tools::file_path_sans_ext(basename(raw_data))
cat(glue::glue("Processing {tblnm}: {Sys.time()} ..."))

data <- open_dataset(
  sources = raw_data, 
  format = "tsv",
  schema = schema,
  skip = 1
)

dbWriteTableArrow(con, 
                  name = "STP_Credential", 
                  nanoarrow::as_nanoarrow_array_stream(data), append = TRUE)

cat(glue::glue("...completed {Sys.time()}"))
cat("\n")






