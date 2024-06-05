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
raw_data_file <- glue::glue("{ptib}/PTIB 2021 and 2022 Enrolment Data for BC Stats 2024.05.31.xlsx")

## ----- Read raw data  ----
raw_data <- xlsx::read.xlsx(raw_data_file, sheetIndex = 1, startRow = 3) %>% 
  janitor::clean_names() %>% 
  rename(graduates = credential_1) ## fairly sure this is the case since this 
                                   ## column is equal to total_enrolments - enrolments_not_graduated

## ----- Aggregate data ----
data <- raw_data %>%
  group_by(calendar_year, credential, cip, age_group, immigration_status) %>%
  summarize(`Sum of Graduates` = sum(graduates, na.rm = TRUE),
            `Sum of Enrolments` = sum(enrolments_not_graduated, na.rm = TRUE),
            `Sum of Total Enrolments` = sum(total_enrolments, na.rm = TRUE),
            .groups = "drop") %>%
  select(Year = calendar_year,	`Credential (Program) (Program)` = credential,
         CIP = cip,	`Age Group` = age_group, `Immigration Status` = immigration_status,
         `Sum of Graduates`,	`Sum of Enrolments`, 	`Sum of Total Enrolments`)

## ----- Connection to decimal ----
db_config <- config::get("decimal")
con <- dbConnect(odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")

# ---- Define Schema ----
schema <-
  schema(Year = float(),
         `Credential (Program) (Program)` = string(),
         CIP = string(),
         `Age Group` = string(),
         `Immigration Status` = string(),
         `Sum of Graduates` = float(),
         `Sum of Erolments` = float(),
         `Sum of Total Enrolments` = float())

# ---- Write to decimal ----
dbWriteTableArrow(con,
                  name = "PTIB_Enrolment",
                  nanoarrow::as_nanoarrow_array_stream(data))


## dbRemoveTable(con, "PTIB_Enrolment") ## remove table for testing
