# ******************************************************************************
# Load datasets required to run program projections step
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
# Lookups
T_Cohort_Program_Distributions_Y2_to_Y12 <-  
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/T_Cohort_Program_Distributions_Y2_to_Y12.csv"), 
                  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")


# ---- Read testing data ----
Cohort_Program_Distributions_Projected <-
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/testing/Cohort_Program_Distributions_Projected.csv"),
                  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")

Cohort_Program_Distributions_Static <-
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/testing/Cohort_Program_Distributions_Static.csv"),
                  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")

# ---- Write to decimal ----
dbWriteTable(decimal_con,
             name = "T_Cohort_Program_Distributions_Y2_to_Y12",
             T_Cohort_Program_Distributions_Y2_to_Y12)

dbWriteTable(decimal_con,
             name = "Cohort_Program_Distributions_Projected",
             Cohort_Program_Distributions_Projected)

dbWriteTable(decimal_con,
             name = "Cohort_Program_Distributions_Static",
             Cohort_Program_Distributions_Static)

# ---- Disconnect ----
dbDisconnect(decimal_con)
