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

# Update Credential Non Dup
# Description: 
# Relies on:
#   - credential_non_dup, 
#   - Credential_Non_Dup_Programs_DACSO_FinalCIPs
#   - Credential_Non_Dup_BGS_IDs
#   - Credential_Non_Dup_GRAD_IDs
# Creates updated credential non duplicate table with updated CIP records
# Uses work done during program matching

library(arrow)
library(tidyverse)
library(odbc)
library(DBI)

# ---- Configure LAN Paths and DB Connection -----
lan <- config::get("lan")
source("./sql/02a-program-matching/02a-update-cred-non-dup.R")
source("./sql/02a-program-matching/02a-convert-leftover-nulls.R")

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

# tables with CIP updates 
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."Credential_Non_Dup_Programs_DACSO_FinalCIPs"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."Credential_Non_Dup_BGS_IDs"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."Credential_Non_Dup_GRAD_IDs"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."Credential_Non_Dup_APPSO_IDs"')))

# reference tables
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_2DIGITS_CIP2016"')))

# START QUERIES ----
# ---- Create additional required columns ----
dbExecute(con, qry_Credential_Non_Dup_Add_Columns) 

# ---- Update non dup with new CIP codes from DACSO, BGS and GRAD records ----
dbExecute(con, qry_update_Credential_Non_Dup_DACSO_Final_CIPs) 
dbExecute(con, qry_update_Credential_Non_Dup_BGS_Final_CIPs) 
dbExecute(con, qry_update_Credential_Non_Dup_GRAD_Final_CIPs) 
dbExecute(con, qry_update_Credential_Non_Dup_APPSO_Final_CIPs) 

# ---- update cluster codes for GRAD and APPSO (was left out of previous code)
dbExecute(con, qry_update_Credential_Non_Dup_GRAD_APPSO_Cluster)

# ---- check for any leftover NULLs in the final cip 4 column
## checks 
{
  tbl(con, "Credential_Non_Dup") %>% filter(is.na(FINAL_CIP_CODE_4)) %>% count(outcomes_cred, FINAL_CIP_CODE_4)
}

# CLEAN UP NULLS ----
# Note: these are stored in separate sql script
# It would be good to merge the APPSO, GRAD, NULL work all into one, as it's all a repeat of the same process
# create cleaning table 
dbExecute(con, qry_NULL_STP_CIP_Cleaning)

# add extra cols 
dbExecute(con, qry_NULL_STP_CIP_add_columns)
dbExecute(con, qry_NULL_STP_CIP_update_original)

# clean CIPs to be correct format 
dbExecute(con, qry_NULL_STP_CIP_clean_cip_1)
dbExecute(con, qry_NULL_STP_CIP_clean_cip_2)

## Update CIP 4 and 2D codes from INFOWARE, matching PSI_CREDENTIAL_CIP to LCIP_CD_WITH_PERIOD
dbExecute(con, qry_Clean_NULL_STP_CIP_Step1_a) # all 6 digits
dbExecute(con, qry_Clean_NULL_STP_CIP_Step1_b) # first 4 digits
dbExecute(con, qry_Clean_NULL_STP_CIP_Step1_c) # recode general program CIPs from 00 ending to 01 ending
dbExecute(con, qry_Clean_NULL_STP_CIP_Step1_d) # match first 2 digits
dbExecute(con, qry_Clean_NULL_STP_CIP_Step2) # add CIP 4D names
dbExecute(con, qry_Clean_NULL_STP_CIP_Step3) # add CIP 2D names
dbExecute(con, qry_Clean_NULL_STP_CIP_step4) # mark “Invalid 4-digit CIP” for remaining blank 4D names
dbExecute(con, qry_Update_Credential_with_STP_CIP_NULL) # create ID list
dbExecute(con, qry_Update_Credential_with_STP_CIP_NULL_nulls) # in 2023 only PSI_PROGRAM_CODE had (Unspecified) - replace with NULLs

# update the final NULL CIPs
dbExecute(con, qry_update_Credential_Non_Dup_NULL_Final_CIPs) 

## checks 
{
  tbl(con, "Credential_Non_Dup") %>% filter(is.na(FINAL_CIP_CODE_4)) %>% count(outcomes_cred, FINAL_CIP_CODE_4)
}

# ---- clean up queries, clean up 'undeclared activities' ----
dbExecute(con, SQLQuery4)
dbExecute(con, SQLQuery6)
dbExecute(con, SQLQuery7)

# ---- Clean up ----
dbExecute(con, "DROP TABLE Credential_Non_Dup_STP_NULL_Cleaning")
dbDisconnect(con)