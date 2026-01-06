# Load Infoware Lookups
# Description:
#   Loads Infoware tables from CSV files on the LAN.
#   Previously connected to Infoware (Oracle) database via JDBC.
#   Writes them to the target SQL Server schema for use by other scripts.
#   This script should be run once at the beginning of the workflow or when lookups change.

library(tidyverse)
library(odbc)
library(DBI)
library(config)

# ---- Configure Connections -----
# Load configuration
db_config <- config::get("decimal")
my_schema <- config::get("myschema")
lan_infoware <- config::get("lan_inforware") # Note: typo 'inforware' in config.yml

# Connect to SQL Server (Target)
con <- dbConnect(
  odbc(),
  Driver = db_config$driver,
  Server = db_config$server,
  Database = db_config$database,
  Trusted_Connection = "True"
)

# ---- Define Tables to Load ----
# Format: list(table_name = csv_filename)
tables_to_load <- list(
  "INFOWARE_L_CIP_6DIGITS_CIP2016" = "L_CIP_6DIGITS_CIP2016.csv",
  "INFOWARE_L_CIP_4DIGITS_CIP2016" = "L_CIP_4DIGITS_CIP2016.csv",
  "INFOWARE_L_CIP_2DIGITS_CIP2016" = "L_CIP_2DIGITS_CIP2016.csv",
  "INFOWARE_PROGRAMS" = "PROGRAMS.csv",
  "INFOWARE_PROGRAMS_HIST_PRGMID_XREF" = "PROGRAMS_HIST_PRGMID_XREF.csv",
  "INFOWARE_BGS_DIST_19_23" = "BGS_DIST_19_23.csv",
  "INFOWARE_BGS_DIST_18_22" = "BGS_DIST_18_22.csv",
  "INFOWARE_BGS_COHORT_INFO" = "BGS_COHORT_INFO.csv"
)

# ---- Load and Write Tables ----
tryCatch(
  {
    for (target_name in names(tables_to_load)) {
      csv_file <- file.path(lan_infoware, tables_to_load[[target_name]])

      message(glue::glue("Reading {csv_file}..."))

      if (!file.exists(csv_file)) {
        warning(glue::glue("File not found: {csv_file}. Skipping {target_name}."))
        next
      }

      # Read from CSV
      df <- read_csv(csv_file, show_col_types = FALSE)

      message(glue::glue(
        "Writing {target_name} to SQL Server (Schema: {my_schema})..."
      ))

      # Remove existing table if it exists
      if (dbExistsTable(con, Id(schema = my_schema, table = target_name))) {
        dbRemoveTable(con, Id(schema = my_schema, table = target_name))
      }

      # Write to SQL Server
      dbWriteTable(con, Id(schema = my_schema, table = target_name), df)

      message(glue::glue("Successfully loaded {target_name}."))
    }

    message("All available Infoware tables loaded from CSV successfully.")
  },
  error = function(e) {
    message("Error loading Infoware tables:")
    message(e)
  },
  finally = {
    # Cleanup connections
    if (exists("con")) dbDisconnect(con)
  }
)
