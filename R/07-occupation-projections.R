library(tidyverse)
library(RODBC)
library(config)
library(DBI)

# ---- Configure LAN and file paths ----
db_config <- config::get("decimal")
lan <- config::get("lan")
my_schema <- config::get("myschema")

source(glue::glue("{lan}/development/sql/gh-source/07-occupation-projections/07-occupation-projections.R"))

# ---- Connection to decimal ----
db_config <- config::get("decimal")
decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")

# ---- Check for required data tables ----
# Derived tables
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}".""')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}".""')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}".""')))

dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Cohort_Program_Distributions_Projected"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Cohort_Program_Distributions_Static"')))

# Lookups
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_4DIGITS_CIP2016"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_6DIGITS_CIP2016"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}".""')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}".""')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}".""')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}".""')))

# ---- Clean Up ----
# Test Data
dbExecute(decimal_con, "drop table ")

# Lookups
dbExecute(decimal_con, "drop table INFOWARE_L_CIP_4DIGITS_CIP2016")
dbExecute(decimal_con, "drop table ")
dbExecute(decimal_con, "drop table INFOWARE_L_CIP_6DIGITS_CIP2016")
dbExecute(decimal_con, "drop table ")
dbExecute(decimal_con, "drop table ")
dbExecute(decimal_con, "drop table ")

# Keep 
dbExists(decimal_con, "")
dbExists(decimal_con, "")






