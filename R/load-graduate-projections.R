# ******************************************************************************
# Load population projections and data required for Graduate Projection Step
# ******************************************************************************

library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(RJDBC)

# ---- Configure LAN and file paths ----
db_config <- config::get("pdbtrn")
jdbc_driver_config <- config::get("jdbc")
lan <- config::get("lan")

# ---- Connection to outcomes ----
jdbcDriver <- JDBC(driverClass = jdbc_driver_config$class,
                   classPath = jdbc_driver_config$path)

outcomes_con <- dbConnect(drv = jdbcDriver, 
                          url = db_config$url,
                          user = db_config$user,
                          password = db_config$password)

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
dbWriteTable(decimal_con, name = "population_projections", raw_data_file)

# ---- Disconnect ----
dbDisconnect(decimal_con)
