# ******************************************************************************
# After running the regular and QI model runs, prep for ptib run.  
# ******************************************************************************
# rm(list = ls())
library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(RJDBC)
source("./R/utils.R")
# ---- Configuration ----
db_config <- config::get("decimal")
my_schema <- config::get("myschema")
# ---- Connection to decimal ----
db_config <- config::get("decimal")
decimal_con <- dbConnect(odbc::odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")
regular_run <-  F
qi_run <- T
ptib_run <-  F

# ---- Clean environment ----
# the commented tables are needed in qi model run but not in ptib model run
# dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.T_BGS_Data_Final', 'U') IS NOT NULL DROP TABLE [{my_schema}].[T_BGS_Data_Final];"))
dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.T_Cohorts_Recoded', 'U') IS NOT NULL DROP TABLE [{my_schema}].[T_Cohorts_Recoded];"))
# dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.t_dacso_data_part_1_stepa', 'U') IS NOT NULL DROP TABLE [{my_schema}].[t_dacso_data_part_1_stepa];"))
dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.t_dacso_data_part_1', 'U') IS NOT NULL DROP TABLE [{my_schema}].[t_dacso_data_part_1];"))
# dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.infoware_c_outc_clean_short_resp', 'U') IS NOT NULL DROP TABLE [{my_schema}].[infoware_c_outc_clean_short_resp];"))
# dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.T_TRD_DATA', 'U') IS NOT NULL DROP TABLE [{my_schema}].[T_TRD_DATA];"))
# dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.TRD_Graduates', 'U') IS NOT NULL DROP TABLE [{my_schema}].[TRD_Graduates];"))
dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.Credential_Non_Dup', 'U') IS NOT NULL DROP TABLE [{my_schema}].[Credential_Non_Dup];"))
dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.T_DACSO_Near_Completers_RatioAgeAtGradCIP4', 'U') IS NOT NULL DROP TABLE [{my_schema}].[T_DACSO_Near_Completers_RatioAgeAtGradCIP4];"))
dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.T_DACSO_Near_Completers_RatioByGender', 'U') IS NOT NULL DROP TABLE [{my_schema}].[T_DACSO_Near_Completers_RatioByGender];"))
dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.T_DACSO_Near_Completers_RatioByGender_year', 'U') IS NOT NULL DROP TABLE [{my_schema}].[T_DACSO_Near_Completers_RatioByGender_year];"))
dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN', 'U') IS NOT NULL DROP TABLE [{my_schema}].[T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN];"))
dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history', 'U') IS NOT NULL DROP TABLE [{my_schema}].[T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history];"))
dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.population_projections', 'U') IS NOT NULL DROP TABLE [{my_schema}].[population_projections];"))
dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.tbl_Program_Projection_Input', 'U') IS NOT NULL DROP TABLE [{my_schema}].[tbl_Program_Projection_Input];"))
# dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.Cohort_Program_Distributions_Projected', 'U') IS NOT NULL DROP TABLE [{my_schema}].[Cohort_Program_Distributions_Projected];")) # needed in 07
dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.T_PSSM_Credential_Grouping_Appendix', 'U') IS NOT NULL DROP TABLE [{my_schema}].[T_PSSM_Credential_Grouping_Appendix];"))
dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.T_LCP2_LCP4', 'U') IS NOT NULL DROP TABLE [{my_schema}].[T_LCP2_LCP4];"))
dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.Cohort_Program_Distributions', 'U') IS NOT NULL DROP TABLE [{my_schema}].[Cohort_Program_Distributions];"))


# ---- 5. re-run step by step ----



# for QI run

qi_run_files <- c(
  "./R/load-cohort-appso.R",
  "./R/load-cohort-bgs.R",
  "./R/load-cohort-dacso.R",
  # "./R/load-cohort-trd.R", # 
  "./R/02b-1-pssm-cohorts.R",
  "./R/02b-2-pssm-cohorts-new-labour-supply.R",
  "./R/02b-3-pssm-cohorts-occupation-distributions.R",
  # "./R/load-near-completers-ttrain.R",
  # "./R/03-near-completers-ttrain.R",
  # "./R/load-graduate-projections.R",
  # "./R/04-graduate-projections.R",
  # "./R/load-ptib.R",
  # "./R/05-ptib-analysis.R",
  "./R/load-program-projections.R",
  # "./R/06-program-projections.R",
  "./R/load-occupation-projections.R",
  "./R/07-occupation-projections.R"
)


print(glue::glue("qi model run flag: {qi_run}"))
if (regular_run != T & qi_run == T & ptib_run !=T ){
  # Loop through each file, calling time_execution for each
  for (file_path in qi_run_files) {
    print(glue::glue("regular model run flag: {regular_run}"))
    print(glue::glue("qi model run flag: {qi_run}"))
    print(glue::glue("ptib model furn flag: {ptib_run}"))
    time_execution(file_path)
  }
}



# ---- Disconnect ----
dbDisconnect(decimal_con)
# rm(list = ls())
gc()
