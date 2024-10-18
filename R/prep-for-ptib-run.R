# ******************************************************************************
# After running the regular and QI model runs, prep for ptib run.  
# ******************************************************************************
library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(RJDBC)

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

# ---- Required tables ----
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_Suppression_Public_Release_NOC"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."tmp_tbl_Model"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."tmp_tbl_QI"')))

# ---- Clean environment ----
dbExecute(decimal_con, glue::glue("DROP TABLE [{my_schema}].[T_BGS_Data_Final];"))
dbExecute(decimal_con, glue::glue("DROP TABLE [{my_schema}].[T_Cohorts_Recoded];"))
dbExecute(decimal_con, glue::glue("DROP TABLE [{my_schema}].[t_dacso_data_part_1_stepa];"))
dbExecute(decimal_con, glue::glue("DROP TABLE [{my_schema}].[t_dacso_data_part_1];"))
dbExecute(decimal_con, glue::glue("DROP TABLE [{my_schema}].[infoware_c_outc_clean_short_resp];"))
dbExecute(decimal_con, glue::glue("DROP TABLE [{my_schema}].[T_TRD_DATA];"))
dbExecute(decimal_con, glue::glue("DROP TABLE [{my_schema}].[TRD_Graduates];"))
dbExecute(decimal_con, glue::glue("DROP TABLE [{my_schema}].[Credential_Non_Dup];"))
dbExecute(decimal_con, glue::glue("DROP TABLE [{my_schema}].[T_DACSO_Near_Completers_RatioAgeAtGradCIP4];"))
dbExecute(decimal_con, glue::glue("DROP TABLE [{my_schema}].[T_DACSO_Near_Completers_RatioByGender];"))
dbExecute(decimal_con, glue::glue("DROP TABLE [{my_schema}].[T_DACSO_Near_Completers_RatioByGender_year];"))
dbExecute(decimal_con, glue::glue("DROP TABLE [{my_schema}].[T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN];"))
dbExecute(decimal_con, glue::glue("DROP TABLE [{my_schema}].[T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history];"))
dbExecute(decimal_con, glue::glue("DROP TABLE [{my_schema}].[population_projections];"))
dbExecute(decimal_con, glue::glue("DROP TABLE [{my_schema}].[tbl_Program_Projection_Input];"))
dbExecute(decimal_con, glue::glue("DROP TABLE [{my_schema}].[Cohort_Program_Distributions_Projected];"))
dbExecute(decimal_con, glue::glue("DROP TABLE [{my_schema}].[T_PSSM_Credential_Grouping_Appendix];"))
dbExecute(decimal_con, glue::glue("DROP TABLE [{my_schema}].[T_LCP2_LCP4];"))
dbExecute(decimal_con, glue::glue("DROP TABLE [{my_schema}].[Cohort_Program_Distributions];"))

# ---- Disconnect ----
dbDisconnect(decimal_con)
rm(list = ls())
gc()
