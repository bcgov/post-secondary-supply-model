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
tbl_Age_Groups <-  
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/07/tbl_Age_Groups.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")
tbl_Age_Groups_Rollup <-  
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/07/tbl_Age_Groups_Rollup.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")
tbl_NOC_Skill_Level_Aged_17_34 <-
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/07/tbl_NOC_Skill_Level_Aged_17_34.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")
T_NOC_Skill_Type <-
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/07/T_NOC_Skill_Type.csv"),  col_types = cols(.default = col_guess())) %>%
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
T_Suppression_Public_Release_NOC <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/07/T_Suppression_Public_Release_NOC.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")

# From outcomes
INFOWARE_L_CIP_4DIGITS_CIP2016 <- dbGetQuery(outcomes_con, "SELECT * FROM L_CIP_4DIGITS_CIP2016")
INFOWARE_L_CIP_6DIGITS_CIP2016 <- dbGetQuery(outcomes_con, "SELECT * FROM L_CIP_6DIGITS_CIP2016")

# ---- Rollover data ----
Cohort_Program_Distributions_Projected <-
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/testing/07/Cohort_Program_Distributions_Projected.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")
Cohort_Program_Distributions_Static <-
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/testing/07/Cohort_Program_Distributions_Static.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")
Cohort_Program_Distributions <-
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/testing/07/Cohort_Program_Distributions.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")
Graduate_Projections <-
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/testing/07/Graduate_Projections.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")

# ---- Read testing data ----
Labour_Supply_Distribution_LCP2_No_TT <-
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/testing/07/Labour_Supply_Distribution_LCP2_No_TT.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")
Labour_Supply_Distribution_No_TT <-
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/testing/07/Labour_Supply_Distribution_No_TT.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")
Labour_Supply_Distribution <-
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/testing/07/Labour_Supply_Distribution.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")
Labour_Supply_Distribution_LCP2 <-
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/testing/07/Labour_Supply_Distribution_LCP2.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")

Occupation_Distributions_LCP2_No_TT <-
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/testing/07/Occupation_Distributions_LCP2_No_TT.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")
Occupation_Distributions_No_TT <-
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/testing/07/Occupation_Distributions_No_TT.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")
Occupation_Distributions <-
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/testing/07/Occupation_Distributions.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")
Occupation_Distributions_LCP2 <-
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/testing/07/Occupation_Distributions_LCP2.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")

# ---- Write to decimal ----
dbWriteTable(decimal_con, name = "T_Exclude_from_Projections_LCP4_CD", T_Exclude_from_Projections_LCP4_CD)
dbWriteTable(decimal_con, name = "T_Exclude_from_Projections_LCIP4_CRED",  T_Exclude_from_Projections_LCIP4_CRED)
dbWriteTable(decimal_con, name = "T_Exclude_from_Projections_PSSM_Credential", T_Exclude_from_Projections_PSSM_Credential)
dbWriteTable(decimal_con, name = "T_Exclude_from_Labour_Supply_Unknown_LCP2_Proxy", T_Exclude_from_Labour_Supply_Unknown_LCP2_Proxy)
dbWriteTable(decimal_con, name = "T_Exclude_from_Labour_Supply_Unknown_LCP2_Proxy", T_Current_Region_PSSM_Rollup_Codes)
dbWriteTable(decimal_con, name = "T_Exclude_from_Labour_Supply_Unknown_LCP2_Proxy", T_Exclude_from_Labour_Supply_Unknown_LCP2_Proxy)
dbWriteTable(decimal_con, name = "T_Suppression_Public_Release_NOC", T_Suppression_Public_Release_NOC)

dbWriteTable(decimal_con, name = "INFOWARE_L_CIP_4DIGITS_CIP2016", INFOWARE_L_CIP_4DIGITS_CIP2016)
dbWriteTable(decimal_con, name = "INFOWARE_L_CIP_6DIGITS_CIP2016", INFOWARE_L_CIP_6DIGITS_CIP2016)
dbWriteTable(decimal_con, name = "tbl_Age_Groups",  tbl_Age_Groups)
dbWriteTable(decimal_con, name = "tbl_Age_Groups_Rollup",  tbl_Age_Groups_Rollup)
dbWriteTable(decimal_con, name = "tbl_NOC_Skill_Level_Aged_17_34",  tbl_NOC_Skill_Level_Aged_17_34)
dbWriteTable(decimal_con, name = "T_NOC_Skill_Type",  T_NOC_Skill_Type)
dbWriteTable(decimal_con, name = "T_Current_Region_PSSM_Rollup_Codes", T_Current_Region_PSSM_Rollup_Codes)
dbWriteTable(decimal_con, name = "T_Current_Region_PSSM_Rollup_Codes_BC", T_Current_Region_PSSM_Rollup_Codes_BC)
dbWriteTable(decimal_con, name = "T_PSSM_CRED_RECODE", T_PSSM_CRED_RECODE)

dbWriteTable(decimal_con, name = "Labour_Supply_Distribution",  Labour_Supply_Distribution)
dbWriteTable(decimal_con, name = "Labour_Supply_Distribution_LCP2",  Labour_Supply_Distribution_LCP2)
dbWriteTable(decimal_con, name = "Labour_Supply_Distribution_No_TT",  Labour_Supply_Distribution_No_TT)
dbWriteTable(decimal_con, name = "Labour_Supply_Distribution_LCP2_No_TT",  Labour_Supply_Distribution_LCP2_No_TT)

dbWriteTable(decimal_con, name = "Occupation_Distributions",  Occupation_Distributions)
dbWriteTable(decimal_con, name = "Occupation_Distributions_LCP2",  Occupation_Distributions_LCP2)
dbWriteTable(decimal_con, name = "Occupation_Distributions_No_TT",  Occupation_Distributions_No_TT)
dbWriteTable(decimal_con, name = "Occupation_Distributions_LCP2_No_TT",  Occupation_Distributions_LCP2_No_TT)

dbWriteTable(decimal_con, name = "Cohort_Program_Distributions_Static",  Cohort_Program_Distributions_Static)
dbWriteTable(decimal_con, name = "Cohort_Program_Distributions_Projected",  Cohort_Program_Distributions_Projected)
dbWriteTable(decimal_con, name = "Cohort_Program_Distributions",  Cohort_Program_Distributions)
dbWriteTable(decimal_con, name = "Graduate_Projections",  Graduate_Projections)



# ---- Disconnect ----
dbDisconnect(decimal_con)
dbDisconnect(outcomes_con)
