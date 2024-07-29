# Notes: watch for Age_Grouping variable, documentation mentions having removed it from earlier queries and linked later.  not sure what this means.
# also, need to update T-Year_Survey_Year as is a dependency in DACSO_Q005_DACSO_DATA_Part_1b2_Cohort_Recoded.  The pattern to update is obvious from prior
# year's entries, but some rationale would be helpful.

library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(RJDBC)

# ---- Configure LAN and file paths ----
db_config <- config::get("decimal")
lan <- config::get("lan")
my_schema <- config::get("myschema")

# ---- Connection to database ----
db_config <- config::get("decimal")
decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")

# ---- Read raw data ----
source(glue::glue("{lan}/development/sql/gh-source/02b-pssm-cohorts/02b-pssm-cohorts-dacso.R"))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}".""')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}".""')))

# ---- Execute SQL ----
# Recodes CIP codes
dbExecute(decimal_con, )

# 
dbExecute(decimal_con, )

# 
dbExecute(decimal_con, )

# ---- Clean Up ----
dbDisconnect(decimal_con)
dbExecute(decimal_con, "DROP TABLE")
dbExecute(decimal_con, "DROP TABLE")
dbExecute(decimal_con, "DROP TABLE")
