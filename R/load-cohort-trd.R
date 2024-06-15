# ---- Required Tables ----
# Primary Outcomes tables: See raw data documentation
# tbl_Age 
# tbl_Age_Groups

library(tidyverse)
library(RODBC)
library(config)
library(glue)
library(DBI)

lan <- config::get("lan")
lan_alder <- config::get("lan_alder")

source(glue::glue("{lan}/development/sql/gh-source/02b-pssm-cohorts-trd.R"))

#---- Connect to Outcomes Database ----
connection <- config::get("connection")$outcomes_cohorts
con <- odbcDriverConnect(connection)

# trd data from primary tables
sqlQuery(con, `Q000_TRD_DATA_01`)  
# age and age_group variables are joined from lookups not in SO db
# LCIP4_CRED variable is derived from other vars

# aggregated counts from primary tables
sqlQuery(con, `000_TRD_Graduates`)
                
close(con)