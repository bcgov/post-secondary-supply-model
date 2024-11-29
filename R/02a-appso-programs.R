# Copyright 2024 Province of British Columbia
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations under the License.

# Create APPSO CIP records 
# Description: 
# Relies on:
#   - credential_non_dup, 
#   - infoware CIP tables 
# Creates updated list of IDS with appropriate extra CIP columns for APPSO records
# Uses the same queries as the BGS/GRAD CIP matching 

library(arrow)
library(tidyverse)
library(odbc)
library(DBI)

# ---- Configure LAN Paths and DB Connection -----
lan <- config::get("lan")
source("./sql/02a-program-matching/02a-appso-programs.R")

db_config <- config::get("decimal")
my_schema <- config::get("myschema")

con <- dbConnect(odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")

# ---- Check Required Tables ----
# main table
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."credential_non_dup"')))

# reference tables
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_2DIGITS_CIP2016"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_4DIGITS_CIP2016"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_6DIGITS_CIP2016"')))

# START QUERIES ----
# ---- create APPSO CIP table ----

# create cleaning table 
dbExecute(con, qry_APPSO_STP_CIP_Cleaning)

# add extra cols 
dbExecute(con, qry_APPSO_STP_CIP_add_columns)
dbExecute(con, qry_APPSO_STP_CIP_update_original)

# clean CIPs to be correct format 
dbExecute(con, qry_APPSO_STP_CIP_clean_cip_1)
dbExecute(con, qry_APPSO_STP_CIP_clean_cip_2)

## Update CIP 4 and 2D codes from INFOWARE, matching PSI_CREDENTIAL_CIP to LCIP_CD_WITH_PERIOD
dbExecute(con, qry_Clean_APPSO_STP_CIP_Step1_a) # all 6 digits
dbExecute(con, qry_Clean_APPSO_STP_CIP_Step1_b) # first 4 digits
dbExecute(con, qry_Clean_APPSO_STP_CIP_Step1_c) # recode general program CIPs from 00 ending to 01 ending
dbExecute(con, qry_Clean_APPSO_STP_CIP_Step1_d) # match first 2 digits
dbExecute(con, qry_Clean_APPSO_STP_CIP_Step2) # add CIP 4D names
dbExecute(con, qry_Clean_APPSO_STP_CIP_Step3) # add CIP 2D names
dbExecute(con, qry_Clean_APPSO_STP_CIP_step4) # mark “Invalid 4-digit CIP” for remaining blank 4D names
dbExecute(con, qry_Update_Credential_with_STP_CIP_APPSO) # create ID list
dbExecute(con, qry_Update_Credential_with_STP_CIP_APPSO_nulls) # in 2023 only PSI_PROGRAM_CODE had (Unspecified) - replace with NULLs

# ---- Clean up ----
dbExecute(con, "DROP TABLE Credential_Non_Dup_STP_APPSO_Cleaning")
dbDisconnect(con)
