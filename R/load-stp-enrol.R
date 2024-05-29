# ******************************************************************************
# Load STP enrolment data from staging area in LAN project folder, to decimal.  
# Requested data to be in tab-separated text files partitioned by school year.
# The hope if that smaller datasets are more easily processed by R and SMSS
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
raw_data <- glue::glue("{stp_2019}/STP_EXTRACT_20201803_5.txt")

## ----- Connection to decimal ----
db_config <- config::get("decimal")
con <- dbConnect(odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")

# ---- Define Schema ----
schema <- 
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

# ---- Write to decimal ----
fls <- list.files(stp_2023, full.names = TRUE, recursive = TRUE)

# first file creates a table in SSMS
write_to_decimal(fls[1], con, schema = schema)

# append the remainder to table in SSMS
invisible(lapply(fls[2:length(fls)], 
                 write_to_decimal, 
                 con = con,  
                 schema = schema, 
                 append = TRUE))


# ---- Functions ----
write_to_decimal <- function(flnm, con, schema, append = FALSE, format = "tsv"){
  
  tblnm <- glue::glue("{basename(dirname(flnm))}")
  print(glue::glue("Processing {tblnm}..."))
  
  # read tab-delimited raw-data file
  data <- open_dataset(sources = raw_data,
                     format = format, 
                     schema = schema)
  
  dbWriteTableArrow(con, 
                    name = tblnm, 
                    nanoarrow::as_nanoarrow_array_stream(data)) # dbWriteTableArrow takes optional schema()
  
}




