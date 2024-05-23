library(arrow)
library(tidyverse)
library(odbc)
library(DBI)
library(safepaths)

# ----------------- Configure LAN Paths and DB Connection ----------------------
# set_network_path("<path_to_2023_project_folder>") # Can this be set in config file?
lan <- get_network_path()

# set connection string to decimal
db_config <- config::get("decimal")
con <- dbConnect(odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")

strSQL <- "SELECT COUNT (*) AS n_null_epens
  FROM STP_Credential_2019
  WHERE STP_Credential.ENCRYPTED_TRUE_PEN =''"

dbGetQuery(con, strSQL)

