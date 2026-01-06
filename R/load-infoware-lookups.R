# Load Infoware Lookups
#
# PSSM Model Context:
# Infoware is the source system for CIP codes, program catalogs, and BGS outcomes data.
# This script loads reference data from CSV exports into SQL Server for use by other scripts.
#
# Required Configuration
#   config.yml with:
#     - decimal: SQL Server connection settings
#     - lan_inforware: Path to directory containing CSV files (note: typo in config key)
#
# Input Files (from LAN CSV directory)
#   L_CIP_6DIGITS_CIP2016.csv - 6-digit Classification of Instructional Programs codes
#   L_CIP_4DIGITS_CIP2016.csv - 4-digit CIP codes with names
#   L_CIP_2DIGITS_CIP2016.csv - 2-digit CIP codes with names and clusters
#   PROGRAMS.csv - Institution program catalog
#   PROGRAMS_HIST_PRGMID_XREF.csv - Historical program ID cross-reference
#   BGS_DIST_19_23.csv - BGS outcomes distribution 2019-2023
#   BGS_DIST_18_22.csv - BGS outcomes distribution 2018-2022
#   BGS_COHORT_INFO.csv - BGS cohort metadata
#
# Output Tables (SQL Server Schema)
#   INFOWARE_L_CIP_6DIGITS_CIP2016
#   INFOWARE_L_CIP_4DIGITS_CIP2016
#   INFOWARE_L_CIP_2DIGITS_CIP2016
#   INFOWARE_PROGRAMS
#   INFOWARE_PROGRAMS_HIST_PRGMID_XREF
#   INFOWARE_BGS_DIST_19_23
#   INFOWARE_BGS_DIST_18_22
#   INFOWARE_BGS_COHORT_INFO
#
# WHAT: Loads Infoware reference data from CSV files into SQL Server.
# WHY: The PSSM model requires standardized CIP codes and program metadata for matching.
#      This script centralizes reference data loading from the source CSV exports.
# HOW: 1) Connect to target SQL Server database
#      2) Define mapping of table names to CSV filenames
#      3) For each file: read CSV, drop existing table, write to SQL Server
#      4) Handle missing files gracefully with warnings
#
# TODO [LOW]: Fix typo in config key from 'inforware' to 'infoware'
# TODO [LOW]: Add checksum validation for CSV files before loading
# TODO [LOW]: Add parallel loading for multiple tables

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
        warning(glue::glue(
          "File not found: {csv_file}. Skipping {target_name}."
        ))
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
