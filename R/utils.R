# Copyright 2026 Province of British Columbia
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations under the License.

library(tidyverse)
library(RODBC)
library(DBI)
library(futile.logger)

# Keep utils side-effect free: do not read config or open DB connections on source.
# Create connections in the calling script and pass them into helper functions.


# time_execution: source an R script with timing, console + file logging, and
# fail-fast error handling. Wraps each pipeline module so every step is timed,
# logged, and aborts the run on the first failure. Intended use:
#
#   time_execution("R/01a-enrolment-preprocessing.R")
#
# The script is sourced with local = globalenv(), so objects it creates (data
# frames, the DB connection, the file_logger, etc.) persist in the global
# environment for the modules that follow. echo = TRUE echoes each line to the
# console, and keep.source = TRUE keeps source references so traceback() can
# point at the offending line on failure.
#
# Output goes to both the console (print()) and the "file_logger" appender
# (futile.logger::flog.*). On error the handler logs the message and traceback,
# then re-raises the original condition via stop(e) — callers see the real
# error, not a blank one.
#
# Args:
#   file_path: path to the R script to execute.
#
# Side effects: sources `file_path` into .GlobalEnv; writes START/COMPLETE (with
# elapsed seconds) or error + traceback to console and "file_logger".
# Returns: the result of source() (invisible NULL on success); on error the
# original condition is re-raised.
time_execution <- function(file_path) {
  # Log a start message with a timestamp
  futile.logger::flog.info(paste("Starting:", file_path), name = "file_logger")

  # Log a start message with a timestamp
  print(
    "#################################################################################################"
  )
  print(paste(Sys.time(), "Starting:", file_path))
  print(
    "#################################################################################################"
  )

  start_time <- Sys.time()

  tryCatch(
    {
      # Source the file with echo, and log each line to the log file
      source(
        file_path,
        echo = TRUE,
        keep.source = TRUE,
        # local = TRUE
        local = globalenv()
      ) # Make global variables accessible in source

      # Log the completion message with elapsed time
      end_time <- Sys.time()
      elapsed <- end_time - start_time
      print(
        "########################################################################"
      )
      print(paste(
        Sys.time(),
        "Completed:",
        file_path,
        "in",
        round(elapsed, 2),
        "seconds"
      ))
      print(
        "########################################################################"
      )
      flog.info(
        paste("Completed:", file_path, "in", round(elapsed, 2), "seconds"),
        name = "file_logger"
      )
    },
    error = function(e) {
      # Log the error message if execution fails
      error_message <- paste(
        Sys.time(),
        "Error in file:",
        file_path,
        " - ",
        e$message
      )
      print("###############################################")
      print(error_message)
      print("###############################################")
      # Log the error message if execution fails
      futile.logger::flog.error(
        paste("Error in file:", file_path, "-", e$message),
        name = "file_logger"
      )
      futile.logger::flog.error(paste(capture.output(traceback()), collapse = "\n"), name = "file_logger")

      # Re-raise the original condition so callers see the real error message,
      # class, and call site. A bare stop() would throw an empty error and
      # discard everything we just logged about.
      stop(e)
    }
  )
}

# read_table_from_db: counterpart to write_table_to_db.
#
# Reads a schema-qualified table written by the pipeline (suffixed "_r") back
# into the global environment, bound to `table_name`. Intended to be called
# over a vector of names with purrr::walk, e.g.:
#
#   walk(required_tables, read_table_from_db, schema = my_schema, con = con)
#
# Args:
#   table_name: base table name (without the "_r" suffix), used both to build
#               the DB name and as the global variable to assign.
#   schema:     SQL schema, typically config::get("myschema") — never hardcoded.
#   con:        an open DBI connection (Trusted_Connection = "True").
#
# Side effect: assigns the table into .GlobalEnv[[table_name]].
# Returns: `table_name` invisibly.
read_table_from_db <- function(table_name, schema, con) {
  db_name <- glue::glue("{table_name}_r")
  .GlobalEnv[[table_name]] <- DBI::dbReadTable(
    con,
    DBI::Id(schema = schema, table = as.character(db_name))
  )
  invisible(table_name)
}


# lower_col_names: standardise a table's column names to lower case.
#
# Counterpart to the rename_with(toupper) calls used elsewhere in the pipeline.
# Returns the data frame with all column names lower-cased; the data itself is
# unchanged.
#
# Args:
#   table_name: a name of a data frame / tibble.
#
# Returns: `df` with lower-case column names.
lower_col_names_global <- function(table_name) {
  .GlobalEnv[[table_name]] <- .GlobalEnv[[table_name]] |>
    rename_with(tolower)
  invisible(table_name)
}
