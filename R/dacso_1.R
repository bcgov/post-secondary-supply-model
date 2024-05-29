library(tidyverse)
library(RODBC)

#---- Connect to Access Database
network_path <- ""
db_name <- ""

msaccess_con <- odbcDriverConnect(paste0("Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=", network_path, dbname))
sqlTables(msaccess_con,  tableType = c("TABLE", "VIEW", "SYNONYM"))