library(tidyverse)
library(RODBC)
library(config)
library(DBI)

# ---- Configure LAN and file paths ----
db_config <- config::get("decimal")
lan <- config::get("lan")
my_schema <- config::get("myschema")

source(glue::glue("{lan}/development/sql/gh-source/06-program-projections/06-program-projections.R"))

# ---- Connection to decimal ----
db_config <- config::get("decimal")
decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")

# ---- Check for required data tables ----
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN"'))) 
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Cohort_Program_Distributions_Projected"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}".""')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}".""')))

# ---- Near Completers ----
# computes distribution of near completers
dbExecute(decimal_con, qry_13a0_Delete_Near_Completers_Static) 
dbExecute(decimal_con, qry_13a1_Near_completers) 
dbExecute(decimal_con, qry_13b_Near_Completers_Total) 
dbExecute(decimal_con, qry_13c2_Near_Completers_Program_Dist_TTRAIN_not_used) 
dbExecute(decimal_con, qry_13d_Append_Near_Completers_Program_Dist_Projected_TTRAIN) 
dbExecute(decimal_con, qry_13d_Append_Near_Completers_Program_Dist_Static_TTRAIN) 

# ---- Static Program Distributions ----


# ---- Apprenticeship Graduates ----
