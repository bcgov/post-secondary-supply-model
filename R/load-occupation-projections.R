# ******************************************************************************
# Load datasets required to run program projections step
# ******************************************************************************

library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(RJDBC)

# ---- Configure LAN and file paths ----
db_config <- config::get("pdbtrn")
jdbc_driver_config <- config::get("jdbc")
lan <- config::get("lan")

# ---- Connection to outcomes ----
jdbcDriver <- JDBC(driverClass = jdbc_driver_config$class,
                   classPath = jdbc_driver_config$path)

outcomes_con <- dbConnect(drv = jdbcDriver, 
                          url = db_config$url,
                          user = db_config$user,
                          password = db_config$password)

# ---- Connection to decimal ----
db_config <- config::get("decimal")
decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")


# ---- Lookups  ----
# From the LAN
T_Cohort_Program_Distributions_Y2_to_Y12 <-  
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")

# From outcomes
INFOWARE_L_CIP_4DIGITS_CIP2016 <- dbGetQuery(outcomes_con, "SELECT * FROM L_CIP_4DIGITS_CIP2016")
INFOWARE_L_CIP_6DIGITS_CIP2016 <- dbGetQuery(outcomes_con, "SELECT * FROM L_CIP_6DIGITS_CIP2016")

# ---- Rollover data ----
Cohort_Program_Distributions_Projected <-
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/testing/07/Cohort_Program_Distributions_Projected.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")

Cohort_Program_Distributions_Static <-
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/testing/07/Cohort_Program_Distributions_Static.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")

# ---- Read testing data ----
Labour_Supply_Distribution_LCP2_No_TT <-
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/testing/07/Labour_Supply_Distribution_LCP2_No_TT.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")

Labour_Supply_Distribution_No_TT <-
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/testing/07/Labour_Supply_Distribution_No_TT.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")

Occupation_Distributions_LCP2_No_TT <-
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/testing/07/Occupation_Distributions_LCP2_No_TT.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")

Occupation_Distributions_No_TT <-
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/testing/07/Occupation_Distributions_No_TT.csv"),  col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")



# ---- Write to decimal ----
dbWriteTable(decimal_con, name = "",  )
dbWriteTable(decimal_con, name = "",  )
dbWriteTable(decimal_con, name = "Labour_Supply_Distribution_No_TT",  Labour_Supply_Distribution_No_TT)
dbWriteTable(decimal_con, name = "Labour_Supply_Distribution_LCP2_No_TT",  Labour_Supply_Distribution_LCP2_No_TT)
dbWriteTable(decimal_con, name = "Occupation_Distributions_No_TT",  Occupation_Distributions_No_TT)
dbWriteTable(decimal_con, name = "Occupation_Distributions_LCP2_No_TT",  Occupation_Distributions_LCP2_No_TT)
dbWriteTable(decimal_con, name = "INFOWARE_L_CIP_4DIGITS_CIP2016", INFOWARE_L_CIP_4DIGITS_CIP2016)
dbWriteTable(decimal_con, name = "INFOWARE_L_CIP_6DIGITS_CIP2016", INFOWARE_L_CIP_6DIGITS_CIP2016)
dbWriteTable(decimal_con, name = "",  )
dbWriteTable(decimal_con, name = "",  )
dbWriteTable(decimal_con, name = "Cohort_Program_Distributions_Static",  Cohort_Program_Distributions_Static)
dbWriteTable(decimal_con, name = "Cohort_Program_Distributions_Projected",  Cohort_Program_Distributions_Projected)
dbWriteTable(decimal_con, name = "",  )
dbWriteTable(decimal_con, name = "",  )

# ---- Disconnect ----
dbDisconnect(decimal_con)
dbDisconnect(outcomes_con)
