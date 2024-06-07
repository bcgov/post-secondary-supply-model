# ---- Required Tables ----
# Primary Outcomes tables: See raw data documentation
# t_dacso_data_part_1_stepa
# t_pssm_credential_grouping
# tbl_age
# tbl_age_groups 

library(tidyverse)
library(RODBC)
library(config)
library(glue)
library(DBI)

lan <- config::get("lan_citrix")
source(glue("{lan}/development/sql/gh-source/02b-pssm-cohorts-dacso.R"))

#---- Connect to Outcomes Database ----
connection <- config::get("connection")$outcomes_cohorts
con <- odbcDriverConnect(connection)

                
close(con)