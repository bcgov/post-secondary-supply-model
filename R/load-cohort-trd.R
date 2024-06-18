# ---- Required Tables ----
# Primary Outcomes tables: See raw data documentation
# tbl_Age 
# tbl_Age_Groups

# ******************************************************************************

# ******************************************************************************

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
source(glue("{lan}/data/student-outcomes/sql/Q000_TRD_DATA_01.sql"))
source(glue("{lan}/data/student-outcomes/sql/Q000_TRD_Graduates.sql"))

Q000_TRD_DATA_01 <- dbGetQuery(outcomes_con, Q000_TRD_DATA_01)
Q000_TRD_Graduates <- dbGetQuery(outcomes_con, Q000_TRD_Graduates)

dbDisconnect(outcomes_con)

# ---- Connection to decimal ----
db_config <- config::get("decimal")
decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")
dbWriteTable(decimal_con, name = "Q000_TRD_DATA_01", value = Q000_TRD_DATA_01)
dbWriteTable(decimal_con, name = "Q000_TRD_Graduates", value = Q000_TRD_Graduates)

dbDisconnect(decimal_con)
