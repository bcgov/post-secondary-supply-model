# ******************************************************************************
# Load datasets required to run program projections step
# Note: Rollover dataset originally contain these entries in SURVEY:
# Projected
#  - PTIB                                    
#  - Program_Projections_2019-2020_Q014e   
#  - Program_Projections_2019-2020_qry10c 
#  - Program_Projections_2019-2020_qry12c   
#  - Program_Projections_2019-2020_qry_13d 
# Static
#  - PTIB                                 
#  - Program_Projections_2019-2020_Q012e
#  - Program_Projections_2019-2020_Q013e
#  - Program_Projections_2019-2020_Q014e
#  - Program_Projections_2019-2020_qry_13d
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

source("./sql/06-program-projections/06-program-projections.R")

# ---- Lookups  ----
T_Cohort_Program_Distributions_Y2_to_Y12 <-  
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/06/T_Cohort_Program_Distributions_Y2_to_Y12.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")

T_APPR_Y2_to_Y10 <-  
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/06/T_APPR_Y2_to_Y10.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")

tbl_Age_Groups_Near_Completers <-  
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/06/tbl_Age_Groups_Near_Completers.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")

tbl_Age_Groups <-  
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/07/tbl_Age_Groups.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")

T_PSSM_Projection_Cred_Grp  <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/06/T_PSSM_Projection_Cred_Grp.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")

T_Weights_STP <- 
readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/06/T_Weights_STP.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")

AgeGroupLookup <- 
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/06/AgeGroupLookup.csv"),  col_types = cols(.default = col_guess()))

# ---- Read from Student Outcomes ----
INFOWARE_L_CIP_4DIGITS_CIP2016 <- dbGetQuery(outcomes_con, "SELECT * FROM L_CIP_4DIGITS_CIP2016")
INFOWARE_L_CIP_6DIGITS_CIP2016 <- dbGetQuery(outcomes_con, "SELECT * FROM L_CIP_6DIGITS_CIP2016")


# ---- Rollover data ----
# TODO make schema instead
Cohort_Program_Distributions_Projected <-
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/rollover/06/Cohort_Program_Distributions_Projected.csv"), n_max = 200, col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")

Cohort_Program_Distributions_Static <-
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/rollover/06/Cohort_Program_Distributions_Static.csv"),  n_max = 200,
                  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")

# ---- Write to decimal ----
dbWriteTable(decimal_con, name = "AgeGroupLookup", AgeGroupLookup)
dbWriteTable(decimal_con, name = "tbl_Age_Groups_Near_Completers", tbl_Age_Groups_Near_Completers)
dbWriteTable(decimal_con, name = "tbl_Age_Groups", tbl_Age_Groups)
dbWriteTable(decimal_con, name = "T_Cohort_Program_Distributions_Y2_to_Y12",  T_Cohort_Program_Distributions_Y2_to_Y12)
dbWriteTable(decimal_con, name = "T_APPR_Y2_to_Y10",  T_APPR_Y2_to_Y10)
dbWriteTable(decimal_con, name = "INFOWARE_L_CIP_4DIGITS_CIP2016", INFOWARE_L_CIP_4DIGITS_CIP2016)
dbWriteTable(decimal_con, name = "INFOWARE_L_CIP_6DIGITS_CIP2016", INFOWARE_L_CIP_6DIGITS_CIP2016)
dbWriteTable(decimal_con, name = "T_PSSM_Projection_Cred_Grp", T_PSSM_Projection_Cred_Grp)
dbWriteTable(decimal_con, name = "T_Weights_STP",  T_Weights_STP)

# ---- Build tbl_Program_Projection_Input ---- 
tbl_Program_Projection_Input <- dbGetQuery(decimal_con, qry_Build_Program_Projection_Input)
dbWriteTable(decimal_con, name = "tbl_Program_Projection_Input", tbl_Program_Projection_Input)

# ---- Rollover ----
dbWriteTable(decimal_con, name = "Cohort_Program_Distributions_Static",  Cohort_Program_Distributions_Static)
dbGetQuery(decimal_con, "delete from Cohort_Program_Distributions_Static")
dbWriteTable(decimal_con, name = "Cohort_Program_Distributions_Projected",  Cohort_Program_Distributions_Projected)
dbGetQuery(decimal_con, "delete from Cohort_Program_Distributions_Projected")

# check that only required survey years are in T_Cohorts_Recoded
stopifnot(exprs = {
  dbGetQuery(decimal_con, "select distinct survey_year from T_Cohorts_Recoded")$survey_year==c(2019:2023)
})

# ---- Disconnect ----
dbDisconnect(decimal_con)
dbDisconnect(outcomes_con)
rm(list=ls())
