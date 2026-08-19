# Copyright 2024 Province of British Columbia
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# ----------------------------------------------------------
# Script: load-infoware-lookups.R
#
# Copies INFOWARE lookup/cohort tables from the Oracle
# INFOWARE database into the analyst's schema on Decimal
# (SQL Server), prefixing each destination with "INFOWARE_".
# Tables that already exist are skipped, so this script is
# safe to re-run.
# ----------------------------------------------------------

# ---- Packages ----
pacman::p_load(
  tidyverse, # data wrangling: mutate/across/filter/str_detect/pull/...
  odbc,      # ODBC driver interface (Decimal + Oracle connections)
  DBI        # database interface: dbConnect/dbReadTable/dbWriteTable/...
)

## ----------------------------------------------------------
## Reasons for change, other notes
## ----------------------------------------------------------
## Packages removed because they were no longer used in this script:
##   RODBC, RJDBC - earlier ODBC/JDBC connection approaches, now replaced
##                  by the `odbc` driver used below.
##   glue         - only referenced in removed exploratory/verification code.
##   dbplyr       - only referenced in the removed `tbl(con, ...)` checks.

# ---- Configuration (see config.yml) ----
db_config <- config::get("decimal")  # Decimal (SQL Server) connection
my_schema <- config::get("myschema") # analyst's working schema on Decimal
share_schema <- config::get("shareschema") # shared schema on Decimal (not used here)
iw_config <- config::get("infoware") # Oracle INFOWARE connection

## ----------------------------------------------------------
## Reasons for change, other notes
## ----------------------------------------------------------
## `config::get("lan")` and `share_schema` were read here in earlier
## versions but are not needed for loading INFOWARE lookups, so they have
## been removed. Note: the previous `share_schema <-` line was an
## incomplete assignment that silently captured the Decimal DBI
## connection object; it has been deleted.

# ---- Connect to Decimal (SQL Server) ----
con <- dbConnect(
  odbc(),
  Driver             = db_config$driver,
  Server             = db_config$server,
  Database           = db_config$database,
  Trusted_Connection = "TRUE"
)

# ---- Connect to INFOWARE (Oracle) ----
# Requires the "Oracle in instantclient_19c" ODBC driver to be installed.
odbcListDrivers() # confirm available drivers / Oracle client is present
iw_con <- dbConnect(
  odbc::odbc(),
  Driver = "Oracle in instantclient_19c",
  DBQ    = iw_config$dbq,
  UID    = iw_config$uid,
  PWD    = iw_config$pwd
)

## ----------------------------------------------------------
## Reasons for change, other notes
## ----------------------------------------------------------
## Removed: a one-off discovery block that read ALL_TABLES from INFOWARE
## and listed the L_CIP_* table names. It was diagnostic only and its
## result was never used. To list available CIP lookups on demand, run:
##   dbReadTable(iw_con, "ALL_TABLES") |>
##     filter(str_detect(TABLE_NAME, "^L_CIP_")) |>
##     pull(TABLE_NAME)

# ----------------------------------------------------------
# Helper: copy one INFOWARE table into Decimal
# ----------------------------------------------------------
# - Destination is named "INFOWARE_<source>" (overridable via dest_name).
# - Skips the copy if the destination already exists (idempotent re-runs).
# - Coerces character columns to UTF-8 to fix encoding artefacts that
#   come back from Oracle (e.g. accented program/institution names).
copy_infoware_table <- function(tbl_name,
                                dest_name = paste0("INFOWARE_", tbl_name),
                                source_con = iw_con,
                                dest_con = con,
                                source_schema = "INFOWARE",
                                dest_schema = share_schema,
                                sanitize = TRUE) {
  dest_id <- DBI::Id(schema = dest_schema, table = dest_name)

  if (dbExistsTable(dest_con, dest_id)) {
    cat("Skipping", dest_name, "- already exists\n")
    return(invisible(FALSE))
  }

  cat("Copying", tbl_name, "->", dest_name, "...\n")
  data <- dbReadTable(source_con, DBI::Id(schema = source_schema, table = tbl_name))

  if (sanitize) {
    data <- data |>
      mutate(across(where(is.character),
                    ~ iconv(., from = "", to = "UTF-8", sub = "")))
  }

  dbWriteTable(
    dest_con, 
    dest_id, 
    data)
  cat("  Copied", nrow(data), "rows\n")
  invisible(TRUE)
}

## ----------------------------------------------------------
## Reasons for change, other notes
## ----------------------------------------------------------
## The copy logic used to be duplicated: a generic loop over `table_names`
## AND separate, hand-written blocks for each CIP/PROGRAMS table (some of
## which would error because the loop had already created the destination).
## Both paths are replaced by the single `copy_infoware_table` helper above.
##
## All copied tables now go through UTF-8 sanitizing for consistency.
## Previously only the CIP-2016 lookups and the PROGRAMS backup were
## sanitized; the CIP-2021 / PROGRAMS / XREF tables were not.

## "PROGRAMS_BKUP_NOV_2024_CIP2016_CIP2021" is written as
## "INFOWARE_PROGRAMS_2016" (the shorter name it was already given by the
## dedicated block); the redundant "INFOWARE_PROGRAMS_BKUP_..." copy is gone.

# ---- Tables to copy from INFOWARE -> Decimal ----

# CIP-2016 code lookups (2/4/6-digit)
copy_infoware_table("L_CIP_2DIGITS_CIP2016")
copy_infoware_table("L_CIP_4DIGITS_CIP2016")
copy_infoware_table("L_CIP_6DIGITS_CIP2016")

# CIP-2021 code lookups (2/4/6-digit)
copy_infoware_table("L_CIP_2DIGITS_CIP2021")
copy_infoware_table("L_CIP_4DIGITS_CIP2021")
copy_infoware_table("L_CIP_6DIGITS_CIP2021")

# Program tables (note the custom destination names)
copy_infoware_table("PROGRAMS", dest_name = "INFOWARE_PROGRAMS")
copy_infoware_table(
  "PROGRAMS_BKUP_NOV_2024_CIP2016_CIP2021",
  dest_name = "INFOWARE_PROGRAMS_2016"
)
copy_infoware_table(
  "PROGRAMS_HIST_PRGMID_XREF",
  dest_name = "INFOWARE_PROGRAMS_HIST_PRGMID_XREF"
)

# ---- Disconnect ----
dbDisconnect(iw_con)
dbDisconnect(con)

## ----------------------------------------------------------
## Reasons for change, other notes
## ----------------------------------------------------------
## The following commented-out blocks have been removed. They are kept
## here as notes for context/history:
##
## 1. Verification queries using dbplyr (`tbl(con, "INFOWARE_BGS_*") |>
##    tally()` / `distinct(STQU_ID)`) to check row counts and ID
##    uniqueness. Removed because ad hoc verification in the console is
##    simpler; keeping it dragged in a dbplyr dependency for nothing.
##
## 2. An `rm(INFOWARE_BGS_DIST_*, ...)` block deleting in-memory copies of
##    tables that this script never creates in memory anyway (data is
##    streamed straight from source to destination), so the `rm()` did
##    nothing.
##
## 3. An older "write to Decimal" path that split large tables (BGS_DIST,
##    BGS_COHORT_INFO) into ~80k-row chunks and appended them, as a
##    workaround for connection size limits. The current `dbWriteTable`
##    path no longer needs chunking.
##
## 4. An older JDBC-based INFOWARE connection (RJDBC + `jdbc_config`) and
##    an intermediate hardcoded ODBC variant (Driver
##    "Oracle in instantclient_19_30", DBQ "***"), both superseded
##    by the config-driven `odbc` connection above.
##
## 5. Glue/SQL string builders (`SQL(glue::glue('"{my_schema}"."..."'))`)
##    used by the old per-table write blocks; the `DBI::Id(schema, table)`
##    form used now handles schema/table quoting correctly without glue.
