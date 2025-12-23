# Load Infoware Lookups
# Description: 
#   Connects to the Infoware (Oracle) database via JDBC.
#   Reads the static CIP code and Program lookup tables.
#   Writes them to the target SQL Server schema for use by other scripts.
#   This script should be run once at the beginning of the workflow or when lookups change.

library(tidyverse)
library(odbc)
library(DBI)
library(RJDBC)
library(config)

# ---- Configure Connections -----
# Load configuration
iw_config <- config::get("infoware")
jdbc_config <- config::get("jdbc")
db_config <- config::get("decimal")
my_schema <- config::get("myschema")

# Check if JDBC is configured
if (is.null(jdbc_config$class) || is.null(jdbc_config$path)) {
  stop("JDBC configuration (class/path) is missing in config.yml. Cannot load Infoware tables.")
}

# Connect to SQL Server (Target)
con <- dbConnect(odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")

# Connect to Infoware (Source)
tryCatch({
  jdbcDriver <- JDBC(jdbc_config$class, classPath = jdbc_config$path)
  iw_con <- dbConnect(jdbcDriver, iw_config$database, iw_config$uid, iw_config$pwd)
  
  # ---- Define Tables to Load ----
  tables_to_load <- c(
    "INFOWARE.L_CIP_6DIGITS_CIP2016",
    "INFOWARE.L_CIP_4DIGITS_CIP2016",
    "INFOWARE.L_CIP_2DIGITS_CIP2016",
    "INFOWARE.PROGRAMS",
    "INFOWARE.PROGRAMS_HIST_PRGMID_XREF",
    # BGS-specific tables often needed
    "INFOWARE.BGS_DIST_19_23",
    "INFOWARE.BGS_DIST_18_22",
    "INFOWARE.BGS_COHORT_INFO"
  )
  
  # ---- Load and Write Tables ----
  for (table_name in tables_to_load) {
    message(glue::glue("Reading {table_name} from Infoware..."))
    
    # Read from Oracle
    # Note: dbReadTable in RJDBC might need the schema specified differently depending on driver version
    # Trying direct name first.
    df <- dbReadTable(iw_con, table_name)
    
    # Strip schema from name for target table (e.g. "INFOWARE.L_CIP..." -> "INFOWARE_L_CIP...")
    # Convention in this project seems to vary:
    # 02a-appso uses "INFOWARE_L_CIP_..."
    # We will replace '.' with '_' for the target name to be safe and consistent with 02a-appso
    target_name <- gsub("\.", "_", table_name)
    
    message(glue::glue("Writing {target_name} to SQL Server (Schema: {my_schema})..."))
    
    # Remove existing table if it exists
    if (dbExistsTable(con, Id(schema = my_schema, table = target_name))) {
      dbRemoveTable(con, Id(schema = my_schema, table = target_name))
    }
    
    # Write to SQL Server
    dbWriteTable(con, Id(schema = my_schema, table = target_name), df)
    
    message(glue::glue("Successfully loaded {target_name}."))
  }
  
  message("All Infoware tables loaded successfully.")
  
}, error = function(e) {
  message("Error loading Infoware tables:")
  message(e)
}, finally = {
  # Cleanup connections
  if (exists("iw_con")) dbDisconnect(iw_con)
  if (exists("con")) dbDisconnect(con)
})
