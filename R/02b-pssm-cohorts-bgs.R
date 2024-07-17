library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(RJDBC)

# ---- Configure LAN and file paths ----
db_config <- config::get("decimal")
lan <- config::get("lan")
my_schema <- config::get("myschema")
source(glue::glue("{lan}/development/sql/gh-source/02b-pssm-cohorts/02b-pssm-cohort-bgs.R"))

# ---- Connection to decimal ----
db_config <- config::get("decimal")
decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")

# ---- Connection to access ----



# ---- Check for required data tables ----
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_BGS_Data_Final"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_BGS_INST_Recode"')))

#Notes: watch for Age_Grouping variable, Ian mentiones having removed it from earlier queries, to be linked later. 


# Note: update CIPS after program matching.  Some eyes needed to double check method.


# ---- Clean Up ----
dbDisconnect(decimal_con)



