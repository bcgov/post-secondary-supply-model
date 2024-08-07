
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
source(glue("{lan}/data/student-outcomes/sql/trd-data.sql"))

Q000_TRD_DATA_01 <- dbGetQuery(outcomes_con, Q000_TRD_DATA_01)
Q000_TRD_Graduates <- dbGetQuery(outcomes_con, Q000_TRD_Graduates)

# Convert some variables that should be numeric
Q000_TRD_DATA_01 <- Q000_TRD_DATA_01 %>% 
  mutate(GRADSTAT = as.numeric(GRADSTAT), 
         KEY = as.numeric(KEY),  
         TTRAIN = as.numeric(TTRAIN))

# Gradstat group : couldn't find in outcomes data so defining here.
Q000_TRD_DATA_01 <- Q000_TRD_DATA_01 %>% 
  mutate(LCIP4_CRED = paste0(GRADSTAT_GROUP, ' - ' , LCIP_LCP4_CD , ' - ' , TTRAIN , ' - ' , PSSM_CREDENTIAL))

dbDisconnect(outcomes_con)

# ---- Connection to decimal ----
db_config <- config::get("decimal")
decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")
dbWriteTable(decimal_con, name = "T_TRD_DATA", value = Q000_TRD_DATA_01)
dbWriteTable(decimal_con, name = "Q000_TRD_Graduates", value = Q000_TRD_Graduates)

dbDisconnect(decimal_con)
