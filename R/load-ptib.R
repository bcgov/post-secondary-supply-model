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
library(readxl)

# ---- Configure LAN Paths and DB Connection ----
lan <- get_network_path()
ptib <- glue::glue("{lan}/data/ptib/")

## ----- Raw data file  ----
raw_data_file <- glue::glue("{ptib}/PTIB 2021 and 2022 Enrolment Data for BC Stats.xlsx")

## ----- Read raw data  ----
raw_data <- read_xlsx(raw_data_file, sheet = 1, skip = 2) %>% 
  janitor::clean_names() %>% 
  rename(year = calendar_year,
         graduates = credential_1) ## fairly sure this is the case since this 
                                   ## column is equal to total_enrolments - enrolments_not_graduated

## ----- Aggregate data ----
data <- raw_data %>%
  group_by(year, credential, cip, age_group, immigration_status) %>%
  summarize(sum_of_graduates = sum(graduates, na.rm = TRUE),
            sum_of_enrolments = sum(enrolments_not_graduated, na.rm = TRUE),
            sum_of_total_enrolments = sum(total_enrolments, na.rm = TRUE),
            .groups = "drop")

## ----- Connection to decimal ----
db_config <- config::get("decimal")
con <- dbConnect(odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")

# ---- Define Schema ----
schema <-
  schema(year = float(),
         credential = string(),
         cip = string(),
         age_group = string(),
         immigration_status = string(),
         sum_of_graduates = float(),
         sum_of_enrolments = float(),
         sum_of_total_enrolments = float())

# ---- Write to decimal ----
dbWriteTableArrow(con,
                  name = "PTIB_Credentials",
                  nanoarrow::as_nanoarrow_array_stream(data))


## dbRemoveTable(con, "PTIB_Enrolment") ## remove table for testing
