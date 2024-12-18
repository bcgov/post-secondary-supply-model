# Copyright 2024 Province of British Columbia
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

# *********************************************************************************
# 
# run three model runs
# clear objects from workspace in the "Environment" page before run this script
# variable or data only available within each "source" file scope
# *********************************************************************************


library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(RJDBC)
# Load the futile.logger package
library(futile.logger)
source("./R/utils.R") # for time_execution function

# make sure vpn is on, and the lan is available. Or switch to safepath approach. Otherwise, the data loading does not work without error. 

# Since futile.logger uses a global configuration, it’s best to set up the log file and level outside the time_execution function, especially if you plan to call this function in a loop. This way, the logging configuration applies across all function calls in the loop.
# Set up logging to a specified file and set the threshold
log_file <- "./R/execution_log.txt"
flog.appender(appender.file(log_file), name = "file_logger")
flog.threshold(INFO, name = "file_logger")

# make following code only run once for loading raw data into your schema

###########################################################################################
# only run once to load raw data student outcome to decimal database
###########################################################################################
# List of R file paths
# load_data_files <- c(
#   "./R/load-outcomes-data.R"
# 
# )
# 
# 
# 
# for (rawdata_file_path in load_data_files) {
#   print(paste(Sys.time(), "Starting:", rawdata_file_path))
#   
#   tryCatch({
#     time_execution(rawdata_file_path)
#   }, error = function(e) {
#     flog.error(paste("Error processing file:", rawdata_file_path, "-", e$message), name = "file_logger")
#     stop()
#   })
# }
###########################################################################################

# every time when we have new data or new parameters, rerun following code. 
# List of R file paths
three_model_run_files <- c(
  "./R/prep-for-fresh-run.R",
  "./R/prep-for-qi-run.R",
  "./R/prep-for-ptib-run.R"
)


# initiate flags
regular_run <-  T
qi_run <- F
ptib_run <- F






# Loop through each file, calling time_execution for each
start_time0 <- Sys.time()
for (file_path0 in three_model_run_files) {
  print(paste(Sys.time(), "Starting:", file_path0))

  tryCatch({
    time_execution(file_path0)
  }, error = function(e) {
    flog.error(paste("Error processing file:", file_path0, "-", e$message), name = "file_logger")
    stop()
  })
}
# end_time0 <- Sys.time()
# elapsed0 <- end_time0 - start_time0
print(paste(Sys.time(), glue::glue("Complete three model runs ......")))
flog.info(paste(Sys.time(), glue::glue("Complete three model runs ......")), name = "file_logger")
