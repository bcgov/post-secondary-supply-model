# ******************************************************************************
# Load custom Statistics Canada data from staging area in LAN project folder, to decimal.  
# ******************************************************************************

# ---- libraries and global variables
library(arrow)
library(tidyverse)
library(odbc)
library(DBI)
library(janitor)

# ---- Configure LAN Paths ----
lan <- config::get("lan")
raw_data_file <- glue::glue("{lan}/data/statcan/stat-can-data-export.csv")

# ----- Connection to decimal ----
db_config <- config::get("decimal")
con <- dbConnect(odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")

# ---- Read raw data  ----
raw_data <- read_csv(raw_data_file,locale=locale(encoding="latin1"))

# ---- Clean data ----
data <- raw_data %>% 
  clean_names() %>% 
  rename(age_group = age,
         occupation_NOC = occupation,
         masters_degree_and_earned_doctorate = master_s_degree_and_earned_doctorate # funky apostrophe header name
  ) %>% 
  # fix the geography column en-dashes
  mutate(geography = str_replace(geography,"\u0096","-"))

# ---- Write to decimal ----
dbWriteTableArrow(con,
                  name = "STAT_CAN",
                  nanoarrow::as_nanoarrow_array_stream(data))

# ---- Read from decimal ----
dbReadTable(con, "STAT_CAN")

# ---- Disconnect ----
dbDisconnect(con)
