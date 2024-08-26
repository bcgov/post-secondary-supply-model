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
raw_data_file <- glue::glue("{lan}/data/people2020/extract-name.csv")

# ---- Read testing data ----
raw_data_file <- readr::read_csv(glue::glue("{lan}/development/csv/gh-source/testing/04/Population_Projections PEOPLE2020 downloaded 2021-03-02.csv"), 
                                 col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")

# ---- Write to decimal ----
dbWriteTableArrow(decimal_con,
                  name = "population_projections",
                  nanoarrow::as_nanoarrow_array_stream(raw_data_file))

# ---- Read from decimal ----
dbReadTable(decimal_con, "population_projections")

# ---- Disconnect ----
dbDisconnect(decimal_con)
