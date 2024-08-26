# This script loads student outcomes data for students who students who recently graduated with a 
# Baccalaureate degree (Baccalaureate students are surveyed two years after graduation)
#
# The following data set is read into SQL server from the student outcomes survey database:
#   BGS_Data_Update: unique survey responses for each person/survey year (for years since last model run)
#
# The following data sets are read into SQL server from the LAN:
#   T_BGS_Data: unique survey responses for each person/survey year (last model run) 
#   T_weights: carried forward from last models run and updated with new data.  Waiting for confirmation
#   T_BGS_INST_Re-code: look-up used to re-code several institution codes
#
# Notes: T_BGS_Data +  BGS_Data_Update contain the full set of survey responses used. 
# T_BGS_Data_Final_2017.csv used for 2019 model run, T_BGS_Data_Final.csv for 2023 model run (rollover)
# Some changes to variable names were done for consistency and will be needed when using 2023 BGS_Data_Final
# use query BGS_Q001_BGS_Data_2019_2023 for 2023 model run but note overlapping years (2019)

library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(RJDBC)

# ---- Configure LAN and file paths ----
db_config <- config::get("pdbtrn")
jdbc_driver_config <- config::get("jdbc")
lan <- config::get("lan")
source("./sql/02b-pssm-cohorts/bgs-data.sql")

# ---- Connection to outcomes ----
jdbcDriver <- JDBC(driverClass = jdbc_driver_config$class,
                   classPath = jdbc_driver_config$path)

outcomes_con <- dbConnect(drv = jdbcDriver, 
                          url = db_config$url,
                          user = db_config$user,
                          password = db_config$password)

# ---- Connection to decimal ----
db_config <- config::get("decimal")
decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")

# ---- Read LAN Data ----
# Lookups
T_weights  <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/02/t_weights.csv"), 
                  col_types = cols(Group = "d", Weight = "d", WeightQI = "d", .default = col_character())) %>%
  janitor::clean_names(case = "all_caps")

tmp_BGS_INST_REGION_CDS <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/02/tmp_BGS_INST_REGION_CDS.csv"), 
                  col_types = cols(.default = col_character())) %>%
  janitor::clean_names(case = "all_caps")

T_BGS_INST_Recode <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/02/T_BGS_INST_Recode.csv"), 
                  col_types = cols(.default = col_character())) %>%
  janitor::clean_names(case = "all_caps")

# ---- Read Outcomes Data ----
BGS_Data_Update <- dbGetQuery(outcomes_con, BGS_Q001_BGS_Data_2019_2023)
BGS_Data_Update <- BGS_Data_Update %>% 
  rename("FULL_TM_WRK" = FULL_TM, 
         "FULL_TM_SCHOOL" = D03_STUDYING_FT, 
         "IN_LBR_FRC" = LBR_FRC_LABOUR_MARKET,
         "EMPLOYED" = LBR_FRC_CURRENTLY_EMPLOYED,
         "UNEMPLOYED" = LBR_FRC_UNEMPLOYED,
         "TRAINING_RELATED" = E10_IN_TRAINING_RELATED_JOB, 
         "TOOK_FURTH_ED" = D01_R1) %>% # this can be added to original query
  mutate(AGE_17_34 = if_else(between(AGE, 17, 34), 1, 0)) %>%
  mutate(OLD_LABOUR_SUPPLY = NA) %>% # I don't think we use this?
  select(-c(D02_R1_CURRENTLY_STUDYING, SUBM_CD)) # nor these?

BGS_Data_Update <- BGS_Data_Update %>%
  mutate(CURRENT_REGION_PSSM_CODE =  case_when (
    REGION_CD %in% 1:8 ~ REGION_CD, 
    CURRENT_REGION %in%c(6,9,10) ~ 10,
    CURRENT_REGION == 7 ~ 11,
    CURRENT_REGION == 5 ~ 9,
    CURRENT_REGION == 8 ~ -1,
    TRUE ~ NA)) 

BGS_Data_Update <- BGS_Data_Update %>% 
  inner_join(tmp_BGS_INST_REGION_CDS, by = join_by(INST)) %>%
  mutate(CURRENT_REGION_PSSM_CODE = 
           if_else((CURRENT_REGION_PSSM_CODE == -1 | is.na(CURRENT_REGION_PSSM_CODE)) &
                    (SRV_Y_N == 0 | is.na(SRV_Y_N)), as.numeric(CURRENT_REGION_PSSM), CURRENT_REGION_PSSM_CODE))

# ---- Make T_BGS_Data_Final ----
T_BGS_Data_Final <- BGS_Data_Update %>% 
  select(-c(CUR_RES,REGION_CD,CURRENT_REGION))
#%>%
#  filter(SURVEY_YEAR %in% 2018:2019) %>%
#  rbind(T_BGS_Data)

# ---- Write to decimal----
dbWriteTable(decimal_con, name = "T_Weights", value = T_weights)
dbWriteTable(decimal_con, name = "T_BGS_Data_Final", value = T_BGS_Data_Final)
dbWriteTable(decimal_con, name = "T_BGS_INST_Recode", value = T_BGS_INST_Recode, overwrite = TRUE)

# ---- TESTING ONLY ----
T_bgs_data_final_for_outcomesmatching2020  <- # derived in program matching and will be available in ssql from program matching in future.
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/testing/02/t_bgs_data_final_for_outcomesmatching2020.csv"), 
                  col_types = cols(.default = col_character())) %>%
  janitor::clean_names(case = "all_caps")
dbWriteTable(decimal_con, name = "T_bgs_data_final_for_outcomesmatching2020", value = T_bgs_data_final_for_outcomesmatching2020)

T_BGS_Data <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/testing/02/T_BGS_Data_Final_2017.csv"), 
                  col_types = cols(PEN = "c", .default = col_guess())) %>%
  janitor::clean_names(case = "all_caps") %>%
  select(-c(ID)) %>%
  rename("NOC" = NOC_CD_2011)


# ---- Clean Up ----
dbDisconnect(outcomes_con)
dbDisconnect(decimal_con)




