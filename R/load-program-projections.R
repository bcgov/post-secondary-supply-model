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
lan <- config::get("lan")
my_schema <- config::get("myschema")

# ---- Connection to decimal ----
db_config <- config::get("decimal")
decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")

source("./sql/06-program-projections/06-program-projections.R")

if (regular_run == T){
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
INFOWARE_L_CIP_4DIGITS_CIP2016 <- dbReadTable(decimal_con, SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_4DIGITS_CIP2016_raw"')))
INFOWARE_L_CIP_6DIGITS_CIP2016 <- dbReadTable(decimal_con, SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_6DIGITS_CIP2016_raw"')))


# ---- Rollover data ----
# TODO make schema instead
Cohort_Program_Distributions_Projected <-
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/rollover/06/Cohort_Program_Distributions_Projected.csv"), col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")

Cohort_Program_Distributions_Static <-
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/rollover/06/Cohort_Program_Distributions_Static.csv"),
                  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")

# ---- Write to decimal ----
dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."AgeGroupLookup"')), AgeGroupLookup, overwrite = TRUE)
dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."tbl_Age_Groups_Near_Completers"')), tbl_Age_Groups_Near_Completers, overwrite = TRUE)
dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."tbl_Age_Groups"')), tbl_Age_Groups, overwrite = TRUE)
dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."T_Cohort_Program_Distributions_Y2_to_Y12"')),  T_Cohort_Program_Distributions_Y2_to_Y12, overwrite = TRUE)
dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."T_APPR_Y2_to_Y10"')),  T_APPR_Y2_to_Y10, overwrite = TRUE)
dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_4DIGITS_CIP2016"')), INFOWARE_L_CIP_4DIGITS_CIP2016, overwrite = TRUE)
dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_6DIGITS_CIP2016"')), INFOWARE_L_CIP_6DIGITS_CIP2016, overwrite = TRUE)
dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."T_PSSM_Projection_Cred_Grp"')), T_PSSM_Projection_Cred_Grp, overwrite = TRUE)
dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."T_Weights_STP"')),  T_Weights_STP, overwrite = TRUE)

# ---- Build tbl_Program_Projection_Input ---- 
tbl_Program_Projection_Input <- dbGetQuery(decimal_con, qry_Build_Program_Projection_Input)
dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."tbl_Program_Projection_Input"')), tbl_Program_Projection_Input, overwrite = TRUE)

# ---- Rollover ----
dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."Cohort_Program_Distributions_Static"')),  Cohort_Program_Distributions_Static, overwrite = TRUE)
dbGetQuery(decimal_con, "delete from Cohort_Program_Distributions_Static")
dbGetQuery(decimal_con, "ALTER TABLE Cohort_Program_Distributions_Static ALTER COLUMN LCIP2_CRED NVARCHAR(50)")
dbGetQuery(decimal_con, "ALTER TABLE Cohort_Program_Distributions_Static ALTER COLUMN TTRAIN NVARCHAR(50)")
dbGetQuery(decimal_con, "ALTER TABLE Cohort_Program_Distributions_Static ALTER COLUMN GRAD_STATUS NVARCHAR(50)")

dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."Cohort_Program_Distributions_Projected"')),  Cohort_Program_Distributions_Projected, overwrite = TRUE)
dbGetQuery(decimal_con, "delete from Cohort_Program_Distributions_Projected")
dbGetQuery(decimal_con, "ALTER TABLE Cohort_Program_Distributions_Projected ALTER COLUMN LCIP2_CRED NVARCHAR(50)")
dbGetQuery(decimal_con, "ALTER TABLE Cohort_Program_Distributions_Projected ALTER COLUMN TTRAIN NVARCHAR(50)")
dbGetQuery(decimal_con, "ALTER TABLE Cohort_Program_Distributions_Projected ALTER COLUMN GRAD_STATUS NVARCHAR(50)")

# check that only required survey years are in T_Cohorts_Recoded
stopifnot(exprs = {
  dbGetQuery(decimal_con, "select distinct survey_year from T_Cohorts_Recoded")$survey_year==c(2019:2023)
})
} else {
  # ---- Read from Student Outcomes ----
INFOWARE_L_CIP_4DIGITS_CIP2016 <- dbReadTable(decimal_con, SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_4DIGITS_CIP2016_raw"')))
INFOWARE_L_CIP_6DIGITS_CIP2016 <- dbReadTable(decimal_con, SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_6DIGITS_CIP2016_raw"')))
  # ---- Write to decimal ----
  dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_4DIGITS_CIP2016"')), INFOWARE_L_CIP_4DIGITS_CIP2016, overwrite = TRUE)
  dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_6DIGITS_CIP2016"')), INFOWARE_L_CIP_6DIGITS_CIP2016, overwrite = TRUE)
}


# ---- Disconnect ----
dbDisconnect(decimal_con)
# rm(list=ls())
