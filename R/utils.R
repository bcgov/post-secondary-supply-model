# Define the time_execution function to track execution time and handle errors
# source(file_path, echo = TRUE, keep.source = TRUE):
#
#   echo = TRUE: Prints each line of code as it is executed, helping to trace progress and identify errors.
# keep.source = TRUE: Retains source references to each line, which can improve the accuracy of the traceback.
# local = TRUE: Executes in a local environment, preventing side effects on the global environment (optional but useful for modularization).
# traceback(): After an error, traceback() provides a stack trace that shows the line numbers and function calls leading up to the error, making it easier to identify the specific line in the sourced file that caused the issue.
# Log File Connection: log_conn <- file(log_file, open = "a") opens a connection in append mode to add logs to execution_log.txt.
#
# Logging Each Step:
#
#   Start: Logs a start message with a timestamp before running the script.
# Completion: Logs the completion message with the elapsed time after successful execution.
# Error: Logs the error message and writes the traceback() to the log file for debugging.
# finally Block: Ensures the log file connection is closed after execution, even if an error occurs.
#
# Custom Log File: You can specify a custom log file by passing a different path to log_file.

# By default, source() runs the code in a new environment, so variables defined in the global environment (like log_file) are not accessible within that sourced script unless explicitly passed or the globalenv is specified.
# To make all global variables accessible within each source() call, set local = globalenv(). This allows the sourced file to inherit the global environment variables, including log_file and file_logger:

library(tidyverse)
library(RODBC)
library(DBI)
library(futile.logger)

# Keep utils side-effect free: do not read config or open DB connections on source.
# Create connections in the calling script and pass them into helper functions.
# NOTE: Keep utils.R side-effect free.
# Do not open DB connections or read config at source-time;
# callers should establish connections explicitly and pass them into helpers.

# ---- Connect to SQL Server and read StatCan Tables ----
# (moved to calling scripts)

time_execution <- function(file_path) {
  # Log a start message with a timestamp
  flog.info(paste("Starting:", file_path), name = "file_logger")

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
  .GlobalEnv[[table_name]] <- dbReadTable(
    con,
    SQL(glue::glue('"{schema}"."{db_name}"'))
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
