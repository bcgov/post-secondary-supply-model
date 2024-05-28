# ______________________________________________________________________________
# STP data written to decimal.  
# Current state - the script takes 100% of the full 2019 data, keeping in mind
# the 2023 data will be larger
# STP credential data loads with a few data type conversion problems: 
#   querying SQL data returns PSI_CREDENTIAL_PROGRAM_DESC as a quoted string
# dates are uploaded format YY-MM-DD instead of YYYY-MM-DD
# ______________________________________________________________________________

library(arrow)
library(tidyverse)
library(odbc)
library(DBI)
library(safepaths)
library(config)

# ---- Configure LAN Paths and DB Connection ----
# set_network_path("<path_to_2023_project_folder>") # Can this be set in config file?
lan <- get_network_path()
stp_2023 <- glue::glue("{lan}/data/stp/partitioned")
dir.create(glue::glue("{lan}/data/stp/partitioned"))

# some manual tweaking needed here but only needed for testing anyways.  
i <- grepl("2019-2020", list.files(dirname(lan)))
stp_2019 <- glue::glue("{list.files(dirname(lan), full.names = TRUE)[i]}/Data/STP")

raw_data <- glue::glue("{stp_2019}/STP_Credential.txt")

# set connection string to decimal
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

# ---- read tab-delimited data
data <- open_dataset(
  sources = raw_data, 
  col_types = schema,
  format = "tsv"
)

# ---- write to decimal
dbWriteTableArrow(con, name = "STP_Credential", nanoarrow::as_nanoarrow_array_stream(data)) # also, dbWriteTableArrow takes a schema()
print(glue::glue("Processing STP_Credential"))





