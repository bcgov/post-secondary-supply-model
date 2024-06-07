# ---- Required Tables ----
# Primary Outcomes tables: See raw data documentation
# tbl_Age 
# tbl_Age_Groups


library(tidyverse)
library(RODBC)
library(config)
library(glue)
library(DBI)

lan <- config::get("lan_citrix")
source(glue("{lan}/development/sql/gh-source/02b-pssm-cohorts-trd.R"))

#---- Connect to Outcomes Database ----
connection <- config::get("connection")$outcomes_cohorts
con <- odbcDriverConnect(connection)

# trd data from primary tables
sqlQuery(con, `000_TRD_DATA_01`)  

# aggregated counts from primary tables
sqlQuery(con, `000_TRD_Graduates`)
                
close(con)