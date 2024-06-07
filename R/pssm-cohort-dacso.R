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

# dacso data from primary tables
sqlQuery(con, DACSO_Q003_DACSO_DATA_Part_1_stepA)
sqlQuery(con, DACSO_Q003_DACSO_DATA_Part_1_stepB)
sqlQuery(con, DACSO_Q003b_DACSO_DATA_Part_1_Further_Ed)
                
close(con)