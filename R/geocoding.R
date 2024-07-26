# ******************************************************************************
# ---- Run Geocoding Step prior to 2b-pssm-cohorts-xxx as part of the same workflow 
# ******************************************************************************
library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(RJDBC)

# ---- Configure LAN and file paths ----
db_config <- config::get("decimal")
lan <- config::get("lan")
my_schema <- config::get("myschema")
source(glue::glue("{lan}/development/sql/gh-source/02b-pssm-cohorts/geocoding.R"))

# ---- Connection to decimal ----
db_config <- config::get("decimal")
decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")

# ---- Check for required data tables ----
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."trd_current_region_data"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."appso_current_region_data"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."dacso_current_region_data"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."bgs_current_region_data"')))

# ---- TRD Geocoding Step ----
# Note: check years in queries
dbExecute(decimal_con, "ALTER TABLE trd_current_region_data ADD current_region_pssm_code FLOAT NULL")
dbExecute(decimal_con, qry_000_TRD_Update_Current_Region_PSSM_step1)
dbExecute(decimal_con, qry_000_TRD_Update_Current_Region_PSSM_step2)
dbGetQuery(decimal_con, qry_000_TRD_results) # includes years 2020 and 2021, but Respondents (2021 only) is NA. # some NULL PSSM codes in each year

# ---- APPSO Geocoding Step ----
# Note: check years in queries and records with NULL pssm codes. There was some investigation in prior years.
dbExecute(decimal_con, "ALTER TABLE appso_current_region_data_update ADD current_region_pssm_code FLOAT NULL")
dbExecute(decimal_con, "INSERT INTO appso_current_region_data SELECT * FROM appso_current_region_data_update")
dbExecute(decimal_con, "DROP TABLE appso_current_region_data_update")
dbExecute(decimal_con, qry_APPSO_Update_Current_Region_PSSM_step1)
dbExecute(decimal_con, qry_APPSO_Update_Current_Region_PSSM_step2)
dbGetQuery(decimal_con, qry_APPSO_results) # some NULL PSSM codes in 2016+

# ---- DACSO Geocoding Step ----
# Note: check years in queries
dbExecute(decimal_con, "ALTER TABLE dacso_current_region_data_update ADD current_region_pssm_code NVARCHAR(255) NULL")
dbExecute(decimal_con, "INSERT INTO dacso_current_region_data SELECT * FROM dacso_current_region_data_update")
dbExecute(decimal_con, "DROP TABLE dacso_current_region_data_update")
dbExecute(decimal_con, qry_DACSO_Update_Current_Region_PSSM_step1)
dbExecute(decimal_con, qry_DACSO_Update_Current_Region_PSSM_step2)
dbGetQuery(decimal_con, qry_DACSO_results)

# ---- BGS Geocoding Step ----
# Note: check years in queries
dbExecute(decimal_con, "ALTER TABLE bgs_current_region_data_update ADD current_region_pssm_code NVARCHAR(255) NULL")
dbExecute(decimal_con, "INSERT INTO bgs_current_region_data (stqu_id, survey_year, srv_y_n, inst, REGION_CD, CURRENT_REGION, CURRENT_REGION_NAME, POSTAL)
          SELECT STQU_ID, SURVEY_YEAR, SRV_Y_N, INST, REGION_CD, CURRENT_REGION, CURRENT_REGION_NAME, NEW_POSTAL FROM bgs_current_region_data_update")
dbExecute(decimal_con, "DROP TABLE bgs_current_region_data_update")
dbExecute(decimal_con, qry_BGS_update_Current_Region_PSSM_step0)
dbExecute(decimal_con, qry_BGS_update_Current_Region_PSSM_step1)
dbExecute(decimal_con, qry_BGS_update_Current_Region_PSSM_step2)
dbExecute(decimal_con, qry_BGS_update_Current_Region_PSSM_step3)
dbExecute(decimal_con, qry_BGS_update_Current_Region_PSSM_step4)
dbExecute(decimal_con, qry_BGS_update_Current_Region_PSSM_step4b)
dbExecute(decimal_con, qry_BGS_update_Current_Region_PSSM_step5)
dbGetQuery(decimal_con, qry_BGS_results) # srv_y_n is NA instead of 0 2011 and earlier

# ---- Clean Up ----
dbDisconnect(decimal_con)
