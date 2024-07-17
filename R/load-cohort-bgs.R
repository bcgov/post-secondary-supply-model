library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(RJDBC)

# ---- Configure LAN and file paths ----
db_config <- config::get("pdbtrn")
jdbc_driver_config <- config::get("jdbc")
lan <- config::get("lan")
source(glue::glue("{lan}/data/student-outcomes/sql/bgs-data.sql"))

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

# ---- Read raw data from LAN ----
dbo_t_bgs_data_final_for_outcomesmatching2020  <- 
  readr::read_csv(glue::glue("{lan}/data/student-outcomes/csv/dbo_t_bgs_data_final_for_outcomesmatching2020.csv"), col_types = cols(.default = col_character())) %>%
  janitor::clean_names(case = "all_caps")
  
T_BGS_INST_Recode <- 
  readr::read_csv(glue::glue("{lan}/data/student-outcomes/csv/T_BGS_INST_Recode.csv"), 
      col_types = cols(.default = col_character())) %>%
  janitor::clean_names(case = "all_caps")

T_BGS_Data <- readr::read_csv(glue::glue("{lan}/data/student-outcomes/csv/T_BGS_Data_Final.csv"), col_types = cols(.default = col_character())) %>%
  janitor::clean_names(case = "all_caps") %>%
  select(-c(ID, SUBM_CD)) %>%
  rename("NOC" = NOC_CD_2016)
          
BGS_Data_Update <- dbGetQuery(outcomes_con, BGS_Q001_BGS_Data_2020_2023)  %>%
  janitor::clean_names(case = "all_caps") %>%
  rename("FULL_TM_WRK" = FULL_TM, 
         "FULL_TM_SCHOOL" = D03_STUDYING_FT, 
         "IN_LBR_FRC" = LBR_FRC_LABOUR_MARKET,
         "EMPLOYED" = LBR_FRC_CURRENTLY_EMPLOYED,
         "UNEMPLOYED" = LBR_FRC_UNEMPLOYED,
         "TRAINING_RELATED" = E10_IN_TRAINING_RELATED_JOB, 
         "TOOK_FURTH_ED" = D01_R1) %>%
  mutate(CURRENT_ACTIVITY = case_when(
    (EMPLOYED == 1 & D02_R1_CURRENTLY_STUDYING == 0) ~ 1, 
    (EMPLOYED == 0 & D02_R1_CURRENTLY_STUDYING == 1) ~ 2, 
    (EMPLOYED == 0 & D02_R1_CURRENTLY_STUDYING == 0) ~ 3,
    (EMPLOYED == 1 & D02_R1_CURRENTLY_STUDYING == 1) ~ 4,
    TRUE ~ 0
  )) %>%
  mutate(SURVEY_YEAR = stringr::str_replace(SUBM_CD, "^C_Outc", "20")) %>%
  mutate(AGE_17_34 = if_else(between(AGE, 17, 34), 1, 0)) %>%
  mutate(CURRENT_REGION_PSSM_CODE = NA, 
         OLD_LABOUR_SUPPLY = NA) %>%
  select(-c(D02_R1_CURRENTLY_STUDYING, SUBM_CD))

T_BGS_Data <- rbind(BGS_Data_Update, T_BGS_Data)

# ---- write to decimal
dbWriteTable(decimal_con, name = "T_BGS_INST_Recode", value = T_BGS_INST_Recode)
dbWriteTable(decimal_con, name = "T_BGS_Data_Final", value = T_BGS_Data)
dbWriteTable(decimal_con, name = "dbo_t_bgs_data_final_for_outcomesmatching2020", value = T_BGS_Data)

dbDisconnect(outcomes_con)
dbDisconnect(decimal_con)

