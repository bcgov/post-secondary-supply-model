# ---- Required Tables ----
# Primary Outcomes tables: See raw data documentation
# tbl_age
# tbl_age_groups
library(tidyverse)
library(RODBC)
library(config)
library(glue)
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

# ---- Read raw data and disconnect ----
source(glue("{lan}/data/student-outcomes/sql/appso-data.sql"))

T_APPSO_DATA_Final <- dbGetQuery(outcomes_con, APPSO_DATA_01_Final)
APPSO_Graduates <- dbGetQuery(outcomes_con, APPSO_Graduates)

dbDisconnect(outcomes_con)

# ---- Connection to decimal ----
db_config <- config::get("decimal")
decimal_con <- dbConnect(odbc::odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")

dbWriteTable(decimal_con, name = "T_APPSO_DATA_Final", value = T_APPSO_DATA_Final)
dbWriteTable(decimal_con, name = "APPSO_Graduates", value = APPSO_Graduates)

dbDisconnect(decimal_con)

