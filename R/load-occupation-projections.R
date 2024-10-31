# ******************************************************************************
# Load datasets required to run program projections step
# ******************************************************************************

library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(RJDBC)

# ---- Configure LAN and file paths ----
db_config <- config::get("pdbtrn")
jdbc_driver_config <- config::get("jdbc")
lan <- config::get("lan")
my_schema <- config::get("myschema")

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


# ---- Lookups  ----
# From the LAN
T_Exclude_from_Projections_LCP4_CD <-  
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/07/T_Exclude_from_Projections_LCP4_CD.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")
T_Exclude_from_Projections_LCIP4_CRED <-  
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/07/T_Exclude_from_Projections_LCIP4_CRED.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")
T_Exclude_from_Projections_PSSM_Credential <-  
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/07/T_Exclude_from_Projections_PSSM_Credential.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")
T_Exclude_from_Labour_Supply_Unknown_LCP2_Proxy <-  
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/07/T_Exclude_from_Labour_Supply_Unknown_LCP2_Proxy.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")
T_LCP2_LCP4 <-  
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/07/T_LCP2_LCP4.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")
tbl_Age_Groups <-  
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/07/tbl_Age_Groups.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")
tbl_Age_Groups_Rollup <-  
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/07/tbl_Age_Groups_Rollup.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")
T_Current_Region_PSSM_Rollup_Codes <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/07/T_Current_Region_PSSM_Rollup_Codes.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")
T_Current_Region_PSSM_Rollup_Codes_BC <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/07/T_Current_Region_PSSM_Rollup_Codes_BC.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")
T_PSSM_CRED_RECODE <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/07/T_PSSM_CRED_RECODE.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")
T_PSSM_Credential_Grouping_Appendix <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/07/T_PSSM_Credential_Grouping_Appendix.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")
# T_NOC_Skill_Type <- 
#   readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/07/T_NOC_Skill_Type.csv"),  col_types = cols(.default = col_guess())) %>%
#   janitor::clean_names(case = "all_caps")

# ---- Write to decimal ----
dbWriteTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_Exclude_from_Projections_LCP4_CD"')), T_Exclude_from_Projections_LCP4_CD)
dbWriteTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_Exclude_from_Projections_LCIP4_CRED"')),  T_Exclude_from_Projections_LCIP4_CRED)
dbWriteTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_Exclude_from_Projections_PSSM_Credential"')), T_Exclude_from_Projections_PSSM_Credential)
dbWriteTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_Exclude_from_Labour_Supply_Unknown_LCP2_Proxy"')), T_Exclude_from_Labour_Supply_Unknown_LCP2_Proxy)
dbWriteTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_Current_Region_PSSM_Rollup_Codes"')), T_Current_Region_PSSM_Rollup_Codes, overwrite = T)
if (regular_run == T){
dbWriteTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_PSSM_Credential_Grouping_Appendix"')), T_PSSM_Credential_Grouping_Appendix)
dbWriteTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_LCP2_LCP4"')),  T_LCP2_LCP4)
}
dbWriteTable(decimal_con, SQL(glue::glue('"{my_schema}"."tbl_Age_Groups"')),  tbl_Age_Groups)
dbWriteTable(decimal_con, SQL(glue::glue('"{my_schema}"."tbl_Age_Groups_Rollup"')),  tbl_Age_Groups_Rollup, overwrite = T)
# dbWriteTable(decimal_con, name = "tbl_NOC_Skill_Level_Aged_17_34",  tbl_NOC_Skill_Level_Aged_17_34)
#dbWriteTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_NOC_Skill_Type"')),  T_NOC_Skill_Type)
dbWriteTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_Current_Region_PSSM_Rollup_Codes_BC"')), T_Current_Region_PSSM_Rollup_Codes_BC, overwrite = T)
dbWriteTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_PSSM_CRED_RECODE"')), T_PSSM_CRED_RECODE)

# ---- Disconnect ----
dbDisconnect(decimal_con)
dbDisconnect(outcomes_con)
