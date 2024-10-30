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
# regular_run <- config::get("regular_run")
regular_run <- T
# ptib_flag <- config::get("ptib_flag")
ptib_flag <-  T
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


# ---- 4. Copy tables required for re-run ----
copy_tables = c(
  # '[dbo]."T_bgs_data_final_for_outcomesmatching"',
  # '[IDIR\\ALOWERY]."Labour_Supply_Distribution_Stat_Can"',
  # '[IDIR\\ALOWERY]."Occupation_Distributions_Stat_Can"',
  '[dbo]."Credential_Non_Dup"'
)

dbBegin(decimal_con)
tryCatch({
  for (table in copy_tables) {
    # Extract the part after the dot
    table_short <- str_extract(table, '(?<=\\.)"[^"]+"')
    copy_statement <- glue::glue('SELECT * 
               INTO [{my_schema}].{table_short}
               FROM {table};')  
    dbExecute(decimal_con, copy_statement)
  }
  dbCommit(decimal_con)  # Commit transaction if all deletions succeed
  print("All tables copied successfully.")
}, error = function(e) {
  dbRollback(decimal_con)  # Rollback if there's an error
  print(paste("Error:", e$message))
}, finally = {
  dbDisconnect(decimal_con)
})

decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")

# ---- 5. re-run step by step ----
if (ptib_flag == T) {
  
  source(glue::glue("./R/load-cohort-appso.R"))
  source(glue::glue("./R/load-cohort-bgs.R"))
  source(glue::glue("./R/load-cohort-dacso.R"))
  source(glue::glue("./R/load-cohort-trd.R"))
  source(glue::glue("./R/02b-1-pssm-cohorts.R"))
  source(glue::glue("./R/02b-2-pssm-cohorts-new-labour-supply.R"))
  source(glue::glue("./R/02b-3-pssm-cohorts-occupation-distributions.R"))
  source(glue::glue("./R/load-near-completers-ttrain.R"))
  source(glue::glue("./R/03-near-completers-ttrain.R"))
  source(glue::glue("./R/load-graduate-projections.R"))
  source(glue::glue("./R/04-graduate-projections.R"))
  source(glue::glue("./R/load-ptib.R"))
  source(glue::glue("./R/05-ptib-analysis.R"))
  source(glue::glue("./R/load-program-projections.R"))
  source(glue::glue("./R/06-program-projections.R"))
  source(glue::glue("./R/load-occupation-projections.R"))
  source(glue::glue("./R/07-occupation-projections.R"))
  
} 


source(glue::glue("./R/08-create-final-reports.R"))

# ---- Disconnect ----
dbDisconnect(decimal_con)
rm(list = ls())
gc()
