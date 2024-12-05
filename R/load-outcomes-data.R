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

library(tidyverse)
library(config)

lan = config::get("lan")

so_lan_path <- glue::glue("{lan}/data/student-outcomes/csv/so-provision/")
so_tables <- c("Q000_TRD_DATA_01", "Q000_TRD_Graduates", "T_APPSO_DATA_Final", "APPSO_Graduates", "BGS_Data_Update", "t_dacso_data_part_1_stepa", 
  "infoware_c_outc_clean_short_resp", "tmp_tbl_Age_AppendNewYears", "INFOWARE_L_CIP_4DIGITS_CIP2016", "INFOWARE_L_CIP_6DIGITS_CIP2016")

# read csv's into objects in memory. Run ls() after code chunk to confirm 
fls <- list.files(so_lan_path, pattern = ".csv", full.names = TRUE) 

fls %>%  
  set_names(tools::file_path_sans_ext(basename(fls))) %>% 
  map(read_csv, show_col_types = FALSE) %>%
  imap(~ assign(..2, ..1, envir = .GlobalEnv)) %>%
  invisible()

if(!all(so_tables %in% ls())) 
    warning("Not all student outcome tables were read into current environment.")

# recode data
APPSO_Data_Final$PEN <- as.character(APPSO_Data_Final$PEN)
APPSO_Data_Final$APP_TIME_TO_FIND_EMPLOY_MJOB <- as.numeric(APPSO_Data_Final$APP_TIME_TO_FIND_EMPLOY_MJOB)



# write to decimal
dbWriteTable(decimal_con, 
             name = SQL(glue::glue('"dbo"."T_APPSO_Data_Final"')), 
             value = APPSO_Data_Final,
             overwrite = TRUE)

# write to decimal
dbWriteTable(decimal_con, 
             name = SQL(glue::glue('"dbo"."APPSO_Graduates"')), 
             value = APPSO_Graduates,
             overwrite = TRUE)

#*************************************************************************************************
# QA scratch stuff only - I'll remove from here down when finished
#*************************************************************************************************
library(RODBC)
library(DBI)

db_config <- config::get("decimal")
decimal_con <- dbConnect(odbc::odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")

ssms_APPSO_Graduates <- dbGetQuery(decimal_con, "SELECT * FROM APPSO_Graduates")
ssms_APPSO_Graduates <- ssms_APPSO_Graduates %>% select(-7)
dim(ssms_APPSO_Graduates)
dim(APPSO_Graduates)
anti_join(ssms_APPSO_Graduates, APPSO_Graduates)




APPSO_Data_Final$PEN = as.character(APPSO_Data_Final$PEN)
APPSO_Data_Final$APP_TIME_TO_FIND_EMPLOY_MJOB = as.numeric(APPSO_Data_Final$APP_TIME_TO_FIND_EMPLOY_MJOB)
i=c(1:20,22)
anti_join(APPSO_Data_Final[,i], T_APPSO_DATA_Final[,i])
str(APPSO_Data_Final)