# ------------------------------------------------------------------------------
# It partitions older STP data to parquet format, and then writes to decimal.  
# It could handle csv as well, I just ran into some issues with csv so converted 
# to parquet. Right now it works on ~20% of the full 2019 data, there is full 
# dataset in 2017 project folder that could be tested as well.  Keeping in mind, 
# the 2023 data will be larger again.
# ------------------------------------------------------------------------------

library(arrow)
library(tidyverse)
library(odbc)
library(DBI)
library(safepaths)

# ----------------- Configure LAN Paths and DB Connection ----------------------
# set_network_path("<path_to_2023_project_folder>") # I wonder if this can be set in config file
lan <- get_network_path()
stp_2023 <- glue::glue("{lan}/data/stp/partitioned")
dir.create(glue::glue("{lan}/data/stp/partitioned"))

# some manual tweaking needed here but only needed for testing anyways.  
i <- grepl("2019-2020", list.files(dirname(lan)))
stp_2019 <- glue::glue("{list.files(dirname(lan), full.names = TRUE)[i]}/Data/STP")

raw_data <- glue::glue("{stp_2019}/STP_EXTRACT_20201803_5.txt")

# set connection string to decimal
db_config <- config::get("decimal")
con <- dbConnect(odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")


# ----------------------------- Partition --------------------------------------
enrol_schema <- 
  schema(PSI_PEN = string(),
       PSI_BIRTHDATE = string(),
       PSI_GENDER = string(),
       PSI_STUDENT_NUMBER = string(),
       PSI_STUD_POSTAL_CD_FRST_CNTACT = string(),
       TRUE_PEN = string(),
       ENCRYPTED_TRUE_PEN = string(),
       PTY_ID = string(),
       STP_ALT_ID = string(),
       LAST_SEEN_BIRTHDATE = string(),
       LAST_SEEN_GENDER = string(),
       BC_K_12_STUDENT_EVER = string(),
       LAST_SEEN_ABOR_EVER_FLAG = string(),
       LAST_SEEN_SCHOOL_YEAR = string(),
       LAST_SEEN_DISTRICT_NUMBER = string(),
       LAST_SEEN_DISTRICT_LONG_NAME = string(),
       LAST_SEEN_STUDENT_GRADE = string(),
       LAST_SEEN_SP_NEED_CODE = string(),
       GRAD_YEAR_MONTH = string(),
       GRAD_DISTRICT_NUMBER = string(),
       GRAD_DISTRICT_LONG_NAME = string(),
       GRAD_COLLEGE_REGION = string(),
       GRADUATION_CREDENTIAL_NAME = string(),
       AGPA_PCT = string(),
       ATTENDING_PSI_OUTSIDE_BC = string(),
       LAST_SEEN_HOME_LANG = string(),
       LAST_SEEN_NON_RES_FLAG = string(),
       PSI_SCHOOL_YEAR = string(),
       PSI_REG_TERM = string(),
       PSI_STUD_POSTAL_CD_CURR = string(),
       PSI_ABORIGINAL_STATUS = string(),
       PSI_NEW_STUDENT_FLAG = string(),
       PSI_ENROLMENT_SEQUENCE = string(),
       PSI_CODE = string(),
       PSI_TYPE = string(),
       PSI_FULL_NAME = string(),
       PSI_BASIS_OF_ADMISSION = string(),
       PSI_MIN_START_DATE = string(),
       PSI_CREDENTIAL_PROGRAM_DESC = string(),
       PSI_PROGRAM_CODE = string(),
       PSI_PROGRAM_EFFECTIVE_DATE = string(),
       PSI_CIP_CODE = string(),
       EFFECTIVE_DATE = string(),
       PSI_FACULTY = string(),
       PSI_CE_CRS_ONLY = string(),
       PSI_CREDENTIAL_CATEGORY = string(),
       PSI_VISA_STATUS = string(),
       PSI_STUDY_LEVEL = string(),
       PSI_ENTRY_STATUS = string(),
       OVERALL_ABORIGINAL_STATUS = string())

# read in tab-delimited data from text file
enrol_csv <- open_dataset(
  sources = raw_data, 
  col_types = enrol_schema,
  format = "tsv"
)

# partition by school year and write to disk in .csv format
# note: arrow drops the grouping variable from the data, so I created one
enrol_csv |>
  mutate(SCHOOL_YEAR = str_replace(PSI_SCHOOL_YEAR, "/", "-")) |>
  group_by(SCHOOL_YEAR) |>
  write_dataset(path = glue::glue("{stp_2023}/parquet"), format = "parquet", hive_style = TRUE)
# Note: option to write to csv - just need to play with it to figure out what will work best

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




