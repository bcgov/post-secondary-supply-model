# ******************************************************************************
# Load population projections and data required for Graduate Projection Step
# ******************************************************************************

library(tidyverse)
library(RODBC)
library(config)
library(DBI)

# ---- Configure LAN and file paths ----
lan <- config::get("lan")
my_schema <- config::get("myschema")

# ---- Connection to decimal ----
db_config <- config::get("decimal")
decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")


# ---- Read raw data  ----
raw_data_file_path <- glue::glue("{lan}/data/people2020/population_projections.csv", overwrite = TRUE)

raw_data_file <- readr::read_csv(raw_data_file_path, col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")

# ---- Write to decimal ----
dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."population_projections"')), raw_data_file)

# ---- Disconnect ----
dbDisconnect(decimal_con)
