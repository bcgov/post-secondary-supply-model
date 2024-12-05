# Copyright 2024 Province of British Columbia
# 
# Licensed under the Apache License, Version 2.0 (the &quot;License&quot;);
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an &quot;AS IS&quot; BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations under the License.

##########################################################################################################################################
# This script loads student outcomes data for students who students who 
# TRD: were formerly enrolled in a trades program (i.e. an apprenticeship, trades foundation program or trades-related vocational program)
# APP: have completed the final year of their apprenticeship technical training within the first year of graduation.
# BGS: recently graduated with a Baccalaureate degree (Baccalaureate students are surveyed two years after graduation)
# DAC: recently graduated after completing programs at public colleges, institutes, and teaching-intensive universities (~18 months prior)

# The following data sets are read into SQL server from a csv housed in the project LAN drive. 
# Original data is sourced from the student outcomes survey database, and provisioned to the project by the student outcomes survey team.
#   Q000_TRD_DATA_01: unique survey responses for each person/survey year (a few duplicates)
#   Q000_TRD_Graduates: a count of graduates by credential type, age and survey year
#   T_APPSO_DATA_Final: unique survey responses for each person/survey year  (a few duplicates)
#   APPSO_Graduates: a count of graduates by credential type, age and survey year
#   BGS_Data_Update: unique survey responses for each person/survey year (for years since last model run)
#   t_dacso_data_part_1_stepa: unique survey responses for each person/survey year (for years since last model run)
#   infoware_c_outc_clean_short_resp
#   tmp_tbl_Age_AppendNewYears
#   INFOWARE_L_CIP_4DIGITS_CIP2016
#   INFOWARE_L_CIP_6DIGITS_CIP2016
#############################################################################################################################################

#---------------------------------------------------------------------------------------------------------------------------
# notes: 
#   minor differences in code description/name (for sml n) in INFOWARE_L_CIP_4DIGITS_CIP2016, INFOWARE_L_CIP_4DIGITS_CIP2016
#   differences in prgm_credential (for sml n) in T_DACSO_DATA_Part_1_stepA
#   TODO: handle BGS seperatly
#---------------------------------------------------------------------------------------------------------------------------

library(tidyverse)
library(config)
library(RODBC)
library(DBI)

# set up db connection and lan paths
db_config <- config::get("decimal")
decimal_con <- dbConnect(odbc::odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")

lan = config::get("lan")
so_lan_path <- glue::glue("{lan}/data/student-outcomes/csv/so-provision/")

# read csv's into objects in memory.
fls <- list.files(so_lan_path, pattern = ".csv", full.names = TRUE)
tmp_table_age_fls <- list.files(so_lan_path, pattern = "qry_make_tmp_table_Age_step1_20[0-9][0-9].csv", full.names = TRUE)

# I've read that using assign this way is for novice programmers ... it works though, so..?
fls %>%  
  set_names(tools::file_path_sans_ext(basename(fls))) %>% 
  map(read_csv, show_col_types = FALSE) %>%
  imap(~ assign(..2, ..1, envir = .GlobalEnv)) %>%
  invisible()

# reloads tmp_table_age_fls so a bit of a hack, but it's the cleanest way I can think of
tmp_table_age_fls %>%  
  set_names(tools::file_path_sans_ext(basename(tmp_table_age_fls))) %>% 
  map(read_csv, show_col_types = FALSE, col_types="dcdcd") %>%
  imap(~ assign(..2, ..1, envir = .GlobalEnv)) %>%
  invisible()

# combine tmp_table_Age_step1_20xx
tmp_table_Age <- ls(patt="tmp_table_Age_20") %>% 
  mget(envir = .GlobalEnv) %>%
  bind_rows()

# recode data (TODO: check these are needed after loading directly to decimal)
APPSO_Data_Final$PEN <- as.character(APPSO_Data_Final$PEN)
APPSO_Data_Final$APP_TIME_TO_FIND_EMPLOY_MJOB <- as.numeric(APPSO_Data_Final$APP_TIME_TO_FIND_EMPLOY_MJOB)

INFOWARE_C_OutC_Clean_Short_Resp$Q08 <-as.character(INFOWARE_C_OutC_Clean_Short_Resp$Q08)
INFOWARE_C_OutC_Clean_Short_Resp$FINAL_DISPOSITION <-as.character(INFOWARE_C_OutC_Clean_Short_Resp$FINAL_DISPOSITION)
INFOWARE_C_OutC_Clean_Short_Resp$RESPONDENT <-as.character(INFOWARE_C_OutC_Clean_Short_Resp$RESPONDENT)
INFOWARE_C_OutC_Clean_Short_Resp$CREDENTIAL_DERIVED <-as.character(INFOWARE_C_OutC_Clean_Short_Resp$CREDENTIAL_DERIVED)
INFOWARE_C_OutC_Clean_Short_Resp$TTRAIN<-as.character(INFOWARE_C_OutC_Clean_Short_Resp$TTRAIN)

DACSO_Q003_DACSO_DATA_Part_1_stepA$COCI_PEN = as.character(DACSO_Q003_DACSO_DATA_Part_1_stepA$COCI_PEN)
DACSO_Q003_DACSO_DATA_Part_1_stepA$TPID_LGND_CD = as.character(DACSO_Q003_DACSO_DATA_Part_1_stepA$TPID_LGND_CD)

BGS_Q001_BGS_Data_2019_2023$PEN = as.character(BGS_Q001_BGS_Data_2019_2023$PEN)

# write to decimal.  TODO: code this as a list or function.
dbWriteTable(decimal_con, overwrite = TRUE, name = SQL(glue::glue('"dbo"."T_APPSO_Data_Final_raw"')),  value = APPSO_Data_Final)
dbWriteTable(decimal_con, overwrite = TRUE, name = SQL(glue::glue('"dbo"."APPSO_Graduates_raw"')),  value = APPSO_Graduates)
dbWriteTable(decimal_con, overwrite = TRUE, name = SQL(glue::glue('"dbo"."TRD_Graduates_raw"')),  value = Q000_TRD_Graduates)
dbWriteTable(decimal_con, overwrite = TRUE, name = SQL(glue::glue('"dbo"."INFOWARE_L_CIP_6DIGITS_CIP2016_raw"')),  value = INFOWARE_L_CIP_6DIGITS_CIP2016)
dbWriteTable(decimal_con, overwrite = TRUE, name = SQL(glue::glue('"dbo"."INFOWARE_L_CIP_4DIGITS_CIP2016_raw"')),  value = INFOWARE_L_CIP_4DIGITS_CIP2016)
dbWriteTable(decimal_con, overwrite = TRUE, name = SQL(glue::glue('"dbo"."infoware_c_outc_clean_short_resp_raw"')),  value = infoware_c_outc_clean_short_resp)
dbWriteTable(decimal_con, overwrite = TRUE, name = SQL(glue::glue('"dbo"."tmp_table_Age_raw"')),  value = tmp_table_Age)
#dbWriteTable(decimal_con, overwrite = TRUE, name = SQL(glue::glue('"dbo"."BGS_Q001_BGS_Data_2019_2023"')),  value = BGS_Q001_BGS_Data_2019_2023)

