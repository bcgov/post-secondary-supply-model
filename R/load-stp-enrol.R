# ******************************************************************************
# Load STP enrolment data from staging area in LAN project folder, to decimal.  
# Requested data to be in tab-separated text files partitioned by school year.
# The hope if that smaller datasets are more easily processed by R and SMSS
# ******************************************************************************
library(arrow)
library(tidyverse)
library(odbc)
library(DBI)
library(config)

# ---- Configure LAN and file paths ----
lan <- config::get("lan")

fls <- list.files(glue::glue("{lan}/data/stp/STP_ISA_PSSM"), full.names = TRUE, recursive = FALSE)
fls <- fls[grepl("STP_ENROLMENT", fls)]

# ---- Connection to decimal ----
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
       PSI_STUDENT_POSTAL_CODE_FIRST_CONTACT = string(),
       TRUE_PEN = string(),
       ENCRYPTED_TRUE_PEN = string(),
       STP_ALT_ID = string(),
       LAST_SEEN_BIRTHDATE = string(),
       LAST_SEEN_GENDER = string(),
       BC_K_12_STUDENT_EVER = string(),
       LAST_SEEN_INDIGENOUS_EVER_BACKDATED = string(), # data codes also different
       LAST_SEEN_SCHOOL_YEAR = string(),
       LAST_SEEN_DISTRICT_NUMBER = string(),
       LAST_SEEN_DISTRICT_NAME = string(),
       LAST_SEEN_STUDENT_GRADE = string(),
       LAST_SEEN_SPECIAL_NEED_CODE = string(),
       GRAD_YEAR_MONTH = string(),
       AT_GRAD_DISTRICT_NUMBER = string(),
       AT_GRAD_DISTRICT_NAME = string(),
       AT_GRAD_CURRENT_COLLEGE_REGION_NAME = string(),
       AT_GRAD_CURRENT_COLLEGE_REGION_NUMBER = string(),
       CREDENTIAL_NAME = string(),
       AGPA_PERCENT = string(),
       ATTENDING_PSI_OUTSIDE_BC = string(),
       LAST_SEEN_HOME_LANGUAGE = string(),
       LAST_SEEN_RESIDENCY = string(),
       PSI_SCHOOL_YEAR = string(),
       PSI_REGISTRATION_TERM = string(),
       PSI_STUDENT_POSTAL_CODE_CURRENT = string(),
       PSI_INDIGENOUS_STATUS = string(),
       PSI_NEW_STUDENT_FLAG = string(),
       PSI_ENROLMENT_SEQUENCE = string(),
       PSI_CODE = string(),
       PSI_TYPE = string(),
       PSI_FULL_NAME = string(),
       PSI_BASIS_OF_ADMISSION = string(),
       PSI_MIN_START_DATE = string(),
       PSI_CREDENTIAL_PROGRAM_DESCRIPTION = string(),
       PSI_PROGRAM_CODE = string(),
       PSI_PROGRAM_EFFECTIVE_DATE = string(),
       PSI_CIP_CODE = string(),
       PSI_FACULTY = string(),
       PSI_CONTINUING_EDUCATION_COURSE_ONLY = string(),
       PSI_CREDENTIAL_CATEGORY = string(),
       PSI_VISA_STATUS = string(),
       PSI_STUDY_LEVEL = string(),
       PSI_ENTRY_STATUS = string(),
       OVERALL_INDIGENOUS_STATUS = string()
      )

# ---- Write to decimal ----
invisible(lapply(fls[1], 
                 write_to_decimal, 
                 con = con,  
                 schema = schema, 
                 append = TRUE))

# ---- Functions ----
write_to_decimal <- function(flnm, con, schema, append = FALSE, format = "tsv"){
  
  tblnm <- tools::file_path_sans_ext(basename(flnm))
  cat(glue::glue("Processing {tblnm}: {Sys.time()} ..."))
  cat()
  
  data <- open_dataset(sources = flnm,
                     format = format, 
                     schema = schema, 
                     skip = 1)
  
  DBI::dbWriteTableArrow(con, 
                    name = "STP_Enrolment", 
                    nanoarrow::as_nanoarrow_array_stream(data), 
                    append = append)
  
  cat(glue::glue("...completed {Sys.time()}"))
  cat("\n")
  
}


