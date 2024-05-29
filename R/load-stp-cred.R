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
library(safepaths)
library(config)

# ---- Configure LAN Paths and DB Connection ----
lan <- get_network_path()
stp_2023 <- glue::glue("{lan}/data/stp/")

## ----- Raw data file (remove if not needed) ---- 
i <- grepl("2019-2020", list.files(dirname(lan)))
stp_2019 <- glue::glue("{list.files(dirname(lan), full.names = TRUE)[i]}/Data/STP")
raw_data <- glue::glue("{stp_2019}/STP_Credential.txt")

## ----- Connection to decimal ----
db_config <- config::get("decimal")
con <- dbConnect(odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")


# ---- Define Schema ----
schema <- 
  schema(PTY_ID = string(),
         CREDENTIAL_AWARD_DATE= string(),
         PSI_CODE= string(),
         PSI_FULL_NAME= string(),
         PSI_PEN= string(),
         PSI_STUDENT_NUMBER= string(),
         PSI_SCHOOL_YEAR= string(),
         PSI_PROGRAM_CODE= string(),
         PSI_PROGRAM_EFFECTIVE_DATE= string(),
         PSI_CREDENTIAL_CATEGORY=string(),
         PSI_CREDENTIAL_LEVE= string(),
         PSI_CREDENTIAL_CIP= string(),
         PSI_CREDENTIAL_PROGRAM_DESC= string(),
         ENCRYPTED_TRUE_PEN= string(),
         STP_ALT_ID= string())

# ---- Write to decimal ----
data <- open_dataset(
  sources = raw_data, 
  col_types = schema,
  format = "tsv"
)

print(glue::glue("Processing STP_Credential"))
dbWriteTableArrow(con, name = "STP_Credential", 
                  nanoarrow::as_nanoarrow_array_stream(data)) # also, dbWriteTableArrow takes a schema()






