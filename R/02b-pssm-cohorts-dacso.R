library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(RJDBC)

# ---- Configure LAN and file paths ----
db_config <- config::get("decimal")
lan <- config::get("lan")
my_schema <- config::get("myschema")

# ---- Connection to database ----
db_config <- config::get("decimal")
decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")

# ---- Read raw data ----
source(glue::glue("{lan}/data/student-outcomes/sql/dacso-data.sql"))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."tbl_age"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."tbl_age_groups"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."t_pssm_credential_grouping"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."infoware_c_outc_clean_short_resp"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."infoware_c_outc_clean2"')))

# ---- Clean Up ----
dbDisconnect(decimal_con)
