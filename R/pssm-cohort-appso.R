# ---- Required Tables ----
# Primary Outcomes tables: See raw data documentation
# tbl_age
# tbl_age_groups


library(tidyverse)
library(RODBC)
library(config)
library(glue)
library(DBI)

lan <- config::get("lan_citrix")
source(glue("{lan}/development/sql/gh-source/02b-pssm-cohorts-appso.R"))

#---- Connect to Outcomes Database ----
connection <- config::get("connection")$outcomes_cohorts
con <- odbcDriverConnect(connection)

# appso data from primary db
sqlQuery(con, APPSO_DATA_01_Final)  

# aggregated counts from primary db
sqlQuery(con, APPSO_Graduates)

close(con)