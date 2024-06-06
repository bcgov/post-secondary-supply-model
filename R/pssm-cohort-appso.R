# ---- Required Tables ----
# See outcomes surveys raw data documentation
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

                
close(con)