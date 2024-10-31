# ******************************************************************************
# After running the regular and QI model runs, prep for ptib run.  
# ******************************************************************************
# rm(list = ls())
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
regular_run <-  T
ptib_flag <-  T

# ----  Copy tables required for re-run ----
# copy those tables. those tables (Credential_Non_Dup) are changed during the steps so it needs to copy again from scratch.
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
    table_short <- str_extract(table, '(?<=\\.)"[^"]+"') %>% str_remove_all("\"")
    # must have the SQL to make dbExistsTable work
    # Some tables will be changed by the code so it is better to recreate them.
    # if (!dbExistsTable(decimal_con, SQL(glue::glue("{my_schema}.{table_short}")))){
    drop_statement <- glue::glue(
      "IF OBJECT_ID('{my_schema}.{table_short}', 'U') IS NOT NULL
    DROP TABLE [{my_schema}].[{table_short}];"
    )
    dbExecute(decimal_con, drop_statement)
    
    # -- Create the table by copying data into it
    copy_statement <- glue::glue('SELECT *
               INTO [{my_schema}].{table_short}
               FROM {table};')
    dbExecute(decimal_con, copy_statement)
    # }
    
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

# ---- Required tables ----
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_Suppression_Public_Release_NOC"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."tmp_tbl_Model"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."tmp_tbl_QI"')))

# ---- Clean environment ----
dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.T_BGS_Data_Final', 'U') IS NOT NULL DROP TABLE [{my_schema}].[T_BGS_Data_Final];"))
dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.T_Cohorts_Recoded', 'U') IS NOT NULL DROP TABLE [{my_schema}].[T_Cohorts_Recoded];"))
dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.t_dacso_data_part_1_stepa', 'U') IS NOT NULL DROP TABLE [{my_schema}].[t_dacso_data_part_1_stepa];"))
dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.t_dacso_data_part_1', 'U') IS NOT NULL DROP TABLE [{my_schema}].[t_dacso_data_part_1];"))
dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.infoware_c_outc_clean_short_resp', 'U') IS NOT NULL DROP TABLE [{my_schema}].[infoware_c_outc_clean_short_resp];"))
dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.T_TRD_DATA', 'U') IS NOT NULL DROP TABLE [{my_schema}].[T_TRD_DATA];"))
dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.TRD_Graduates', 'U') IS NOT NULL DROP TABLE [{my_schema}].[TRD_Graduates];"))
dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.Credential_Non_Dup', 'U') IS NOT NULL DROP TABLE [{my_schema}].[Credential_Non_Dup];"))
dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.T_DACSO_Near_Completers_RatioAgeAtGradCIP4', 'U') IS NOT NULL DROP TABLE [{my_schema}].[T_DACSO_Near_Completers_RatioAgeAtGradCIP4];"))
dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.T_DACSO_Near_Completers_RatioByGender', 'U') IS NOT NULL DROP TABLE [{my_schema}].[T_DACSO_Near_Completers_RatioByGender];"))
dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.T_DACSO_Near_Completers_RatioByGender_year', 'U') IS NOT NULL DROP TABLE [{my_schema}].[T_DACSO_Near_Completers_RatioByGender_year];"))
dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN', 'U') IS NOT NULL DROP TABLE [{my_schema}].[T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN];"))
dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history', 'U') IS NOT NULL DROP TABLE [{my_schema}].[T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history];"))
dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.population_projections', 'U') IS NOT NULL DROP TABLE [{my_schema}].[population_projections];"))
dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.tbl_Program_Projection_Input', 'U') IS NOT NULL DROP TABLE [{my_schema}].[tbl_Program_Projection_Input];"))
dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.Cohort_Program_Distributions_Projected', 'U') IS NOT NULL DROP TABLE [{my_schema}].[Cohort_Program_Distributions_Projected];"))
dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.T_PSSM_Credential_Grouping_Appendix', 'U') IS NOT NULL DROP TABLE [{my_schema}].[T_PSSM_Credential_Grouping_Appendix];"))
dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.T_LCP2_LCP4', 'U') IS NOT NULL DROP TABLE [{my_schema}].[T_LCP2_LCP4];"))
dbExecute(decimal_con, glue::glue("IF OBJECT_ID('{my_schema}.Cohort_Program_Distributions', 'U') IS NOT NULL DROP TABLE [{my_schema}].[Cohort_Program_Distributions];"))


# ---- 5. re-run step by step ----

# Define the time_execution function to track execution time and handle errors
time_execution <- function(file_path) {
  start_time <- Sys.time()
  print(paste("Starting:", file_path))
  
  # Execute the R script
  tryCatch({
    source(file_path)
    end_time <- Sys.time()
    elapsed <- end_time - start_time
    print(paste("Completed:", file_path, "in", round(elapsed, 2), "seconds"))
  }, error = function(e) {
    # Print error message if execution fails
    print(paste("Error in file:", file_path, " - ", e$message))
  })
}

# List of R file paths
ptib_run_files <- c(
  "./R/load-cohort-appso.R",
  "./R/load-cohort-bgs.R",
  "./R/load-cohort-dacso.R",
  "./R/load-cohort-trd.R", # not sure if this one
  "./R/02b-1-pssm-cohorts.R",
  "./R/02b-2-pssm-cohorts-new-labour-supply.R",
  "./R/02b-3-pssm-cohorts-occupation-distributions.R",
  "./R/load-near-completers-ttrain.R",
  "./R/03-near-completers-ttrain.R",
  "./R/load-graduate-projections.R",
  "./R/04-graduate-projections.R",
  "./R/load-ptib.R",
  "./R/05-ptib-analysis.R",
  "./R/load-program-projections.R",
  "./R/06-program-projections.R",
  "./R/load-occupation-projections.R",
  "./R/07-occupation-projections.R"
)


if (regular_run==T & ptib_flag == T) {
  
  # Loop through each file, calling time_execution for each
  for (file_path in ptib_run_files[1:5]) {
    time_execution(file_path)
  }
  
} 


source(glue::glue("./R/08-create-final-reports.R"))

# ---- Disconnect ----
dbDisconnect(decimal_con)
rm(list = ls())
gc()
