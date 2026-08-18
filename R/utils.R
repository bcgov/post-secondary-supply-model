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

# Keep utils free of runtime side effects like reading config files or opening DB connections on source.
# Create DB connections in the calling script and pass them into helper functions.

library(readr)
library(dplyr)
library(stringr)
library(purrr)
library(tibble)

# # ---- Connect to SQL Server and read StatCan Tables ----
# lan <- config::get("lan")
# my_schema <- config::get("myschema")
# db_config <- config::get("decimal")
# con <- dbConnect(
#   odbc::odbc(),
#   Driver = db_config$driver,
#   Server = db_config$server,
#   Database = db_config$database,
#   Trusted_Connection = "True"
# )

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
      futile.logger::flog.error(
        paste(capture.output(traceback()), collapse = "\n"),
        name = "file_logger"
      )

      # Re-raise the original condition so callers see the real error message,
      # class, and call site. A bare stop() would throw an empty error and
      # discard everything we just logged about.
      stop(e)
    }
  )
}

# write_table_to_db: write a global data frame to SQL Server as <name>_r.
#
# Counterpart to read_table_from_db.  Fetches the data frame named `table_name`
# from .GlobalEnv, strips invalid UTF-8 from character columns (which would
# otherwise make odbcDataType()/nchar() fail), and writes it to
# schema."table_name_r" with overwrite = TRUE.  Intended to be called over a
# vector of names with purrr::walk, e.g.:
#
#   walk(tables_to_keep, write_table_to_db, schema = write_schema, con = con)
#
# Args:
#   table_name: name of a data frame in .GlobalEnv; the SQL table is named
#               "<table_name>_r".
#   schema:     SQL schema, typically config::get("shareschema").
#   con:        an open DBI connection (Trusted_Connection = "True").
#
# Side effect: creates/overwrites the table in the database.
# Returns: invisible(NULL).
write_table_to_db <- function(table_name, schema, con) {
  db_name <- paste0(table_name, "_r")
  # Some source files contain invalid UTF-8 byte sequences (e.g. PROGRAM
  # names), which make odbcDataType()/nchar() fail when writing.  Strip
  # invalid bytes from all character columns before writing.
  data <- base::get(table_name, envir = .GlobalEnv) %>%
    mutate(across(
      where(is.character),
      ~ iconv(.x, from = "UTF-8", to = "UTF-8", sub = "")
    ))
  dbWriteTable(
    con,
    SQL(glue::glue('"{schema}"."{db_name}"')),
    data,
    overwrite = TRUE
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


# Detect the character encoding of a CSV file using readr::guess_encoding.
# Preference is given to common encodings (UTF-8, Windows-1252, ISO-8859-1,
# UTF-16) and the best guess must meet min_confidence; otherwise fallback is used.
detect_csv_encoding <- function(
  path,
  n_max = 10000,
  min_confidence = 0.50,
  fallback = "Windows-1252"
) {
  guesses <- readr::guess_encoding(
    file = path,
    n_max = n_max,
    threshold = 0.20
  )

  if (nrow(guesses) == 0) {
    return(fallback)
  }

  preferred <- c(
    "UTF-8",
    "windows-1252",
    "Windows-1252",
    "ISO-8859-1",
    "UTF-16LE",
    "UTF-16BE"
  )

  guesses_preferred <- guesses %>%
    filter(encoding %in% preferred) %>%
    arrange(desc(confidence))

  if (
    nrow(guesses_preferred) > 0 &&
      guesses_preferred$confidence[1] >= min_confidence
  ) {
    return(guesses_preferred$encoding[1])
  }

  fallback
}

# Build a readr col_types spec that forces ID-like columns (matched by name
# patterns such as *_ID, *NUMBER, *CODE, PEN) to character, so leading zeros
# and large ID values are preserved rather than parsed as integers/doubles.
make_id_col_types <- function(path, encoding) {
  header <- names(read_csv(
    path,
    locale = locale(encoding = encoding),
    n_max = 0,
    show_col_types = FALSE
  ))

  id_cols <- header[
    str_detect(
      toupper(header),
      "(^ID$|_ID$|ID$|^ID_ |KEY$|NUMBER$|NO$|CODE$ |CIP|NOC|PEN$|^PEN$|^PEN_|^STUDENT_NUMBER$|^STUDENT_NO$|^STUDENT_ID$|^STUDENT_CODE$)"
    )
  ]

  id_specs <- rep(list(col_character()), length(id_cols))
  names(id_specs) <- id_cols

  do.call(cols, id_specs)
}

# Remove bytes that are invalid UTF-8 by round-tripping through iconv with
# sub = "", which silently drops unconvertible sequences.
strip_invalid_utf8 <- function(x) {
  iconv(x, from = "UTF-8", to = "UTF-8", sub = "")
}

# End-to-end reader for CSV exports (typically from Oracle) that may not be UTF-8.
# Detects the encoding, forces ID-like columns to character, reads the file, then
# strips any residual invalid UTF-8 from character columns.
# The detected encoding and the list of ID columns treated as character are
# attached as data-frame attributes ("detected_encoding", "id_columns_as_character").
read_oracle_csv_auto <- function(
  path,
  fallback_encoding = "Windows-1252",
  min_confidence = 0.50
) {
  detected_encoding <- detect_csv_encoding(
    path = path,
    min_confidence = min_confidence,
    fallback = fallback_encoding
  )

  id_col_spec <- make_id_col_types(
    path = path,
    encoding = detected_encoding
  )

  df <- read_csv(
    path,
    locale = locale(encoding = detected_encoding),
    col_types = id_col_spec,
    show_col_types = FALSE
  ) %>%
    mutate(across(where(is.character), strip_invalid_utf8))

  attr(df, "detected_encoding") <- detected_encoding
  attr(df, "id_columns_as_character") <- names(id_col_spec$cols)

  df
}

# example usage:
# df <- read_oracle_csv_auto(appso_file )
