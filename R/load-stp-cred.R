# ------------------------------------------------------------------------------
# STP data written to decimal.  
# Curent state - the script takes 100% of the full 2019 data, keeping in mind,the 2023 data will be larger
# STP credential data loads with a few data type conversion problems: 
#   querying SQL data returns PSI_CREDENTIAL_PROGRAM_DESC as a quoted string
# dates are uploaded format YY-MM-DD instead of YYYY-MM-DD
# ------------------------------------------------------------------------------
library(arrow)
library(tidyverse)
library(odbc)
library(DBI)
library(safepaths)
library(config)

# ----------------- Configure LAN Paths and DB Connection ----------------------
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


# ----------------------------- Define Schema --------------------------------------
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

# read in tab-delimited data from text file
data <- open_dataset(
  sources = raw_data, 
  col_types = schema,
  format = "tsv"
)

# partition by school year and write to disk in .csv format
# note: arrow drops the grouping variable so defined one we won't use in analysis

data |>
  mutate(SCHOOL_YEAR = str_replace(PSI_SCHOOL_YEAR, "/", "-")) |>
  group_by(SCHOOL_YEAR) |>
  write_dataset(path = glue::glue("{stp_2023}/csv"), format = "csv", hive_style = TRUE)
# Note: option to write to csv - just need to play with it to figure out what will work best#time: 3:37PM

# ----------- Read partitioned data and write to PSSM 2023  --------------------
fls <- list.files(glue::glue("{stp_2023}/parquet"), full.names = TRUE, recursive = TRUE)
write_to_decimal(fls[1], con, schema = enrol_schema)
invisible(lapply(fls[2:18], 
                 write_to_decimal, 
                 con = con,  
                 schema = enrol_schema, 
                 append = TRUE))


# ---------------------------------- USER-DEFINED FUNCTIONS --------------------
# open partitioned file and write to decimal
# option to use dbWriteTable(con, name = Id(schema = enrol_schema, table = nm), value = df, append = append)

# some fiddling still needs to be done here
write_to_decimal <- function(fl, con, schema, append = FALSE, format = "parquet"){
  nm <- glue::glue("{basename(dirname(fl))}")
  df <- open_dataset(fl, format = format, schema = enrol_schema) %>% collect()
  dbWriteTableArrow(con, name = nm, nanoarrow::as_nanoarrow_array_stream(df)) # also, dbWriteTableArrow takes a schema()
  print(glue::glue("Processing {nm}"))
}





