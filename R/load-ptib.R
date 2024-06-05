# ******************************************************************************
# Load PTIB enrolment data from staging area in LAN project folder, to decimal.  
# Raw data is in excel.
# ******************************************************************************
library(arrow)
library(tidyverse)
library(odbc)
library(DBI)
library(safepaths)
library(config)
library(xlsx)

# ---- Configure LAN Paths and DB Connection ----
lan <- get_network_path()
ptib <- glue::glue("{lan}/data/ptib/")

## ----- Raw data file  ----
raw_data <- glue::glue("{ptib}/PTIB 2021 and 2022 Enrolment Data for BC Stats 2024.05.31.xlsx")

## ----- Read raw data  ----
data <- xlsx::read.xlsx(raw_data, sheetIndex = 1, startRow = 3) %>% 
  janitor::clean_names() %>% 
  rename(graduates = credential_1) ## fairly sure this is the case since this 
                                   ## column is equal to total_enrolments - enrolments_not_graduated

## ----- Connection to decimal ----
db_config <- config::get("decimal")
con <- dbConnect(odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")

# ---- Define Schema ----
schema <- 
  schema(calendar_year = float(),
         institution = string(),
         program = string(),
         cip = string(),
         age_group = string(),
         credential = string(),
         immigration_status = string(),
         graduates = float(),            
         enrolments_not_graduated = float(),
         total_enrolments = float())

# ---- Write to decimal ----
dbWriteTableArrow(con,
                  name = "PTIB_Enrolment",
                  nanoarrow::as_nanoarrow_array_stream(data))


## dbRemoveTable(con, "PTIB_Enrolment") ## remove table for testing
