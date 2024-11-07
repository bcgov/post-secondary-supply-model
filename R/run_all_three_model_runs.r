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



# List of R file paths
three_model_run_files <- c(
  "./R/prep-for-fresh-run.R",
  "./R/prep-for-qi-run-test.R",
  "./R/prep-for-ptib-run.R"
)


# initiate flags
regular_run <-  T
qi_run <- F
ptib_run <- F





# Since futile.logger uses a global configuration, it’s best to set up the log file and level outside the time_execution function, especially if you plan to call this function in a loop. This way, the logging configuration applies across all function calls in the loop.
# Set up logging to a specified file and set the threshold
log_file <- "./R/execution_log.txt"
flog.appender(appender.file(log_file), name = "file_logger")
flog.threshold(INFO, name = "file_logger")

# Loop through each file, calling time_execution for each
start_time0 <- Sys.time()
for (file_path0 in three_model_run_files) {
  print(paste(Sys.time(), "Starting:", file_path0))

  tryCatch({
    time_execution(file_path0)
  }, error = function(e) {
    flog.error(paste("Error processing file:", file_path0, "-", e$message), name = "file_logger")
  })
}
end_time0 <- Sys.time()
elapsed0 <- end_time0 - start_time0
print(paste(Sys.time(), glue::glue("Complete three model runs ......"), "in", round(elapsed0, 2), "seconds"))
flog.info(paste(Sys.time(), glue::glue("Complete three model runs ......"), "in", round(elapsed0, 2), "seconds"), name = "file_logger")
