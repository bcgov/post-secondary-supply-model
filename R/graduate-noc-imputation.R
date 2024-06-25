# ******************************************************************************
# Graduate NOC imputations on Stat Can data  
# ******************************************************************************

# ---- libraries and global variables
library(arrow)
library(tidyverse)
library(odbc)
library(DBI)
library(janitor)

# ----- Connection to decimal ----
db_config <- config::get("decimal")
con <- dbConnect(odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")

# ---- Required Tables ----
# Stat Can data: See raw data documentation
# STAT_CAN
dbExistsTable(con, "STAT_CAN")

# ---- Read from decimal ----
stat_can_data_raw <- dbReadTable(con, "STAT_CAN")

# ---- Disconnect ----
dbDisconnect(con)

# ---- Clean up data ----
# review geography variable in data
stat_can_data_raw %>% count(geography)

# create region variable based off geography
# note formatting/naming can change within stat can data; review below (order important)
stat_can_data <- stat_can_data_raw %>%
  mutate(region=case_when(str_detect(geography,"Canada") ~ "Canada",
                          str_detect(geography,"BC excluding") ~ "BC excluding Vancouver Island Coast and Lower Mainland",
                          str_detect(geography,"Vancouver Island and Coast") ~ "Vancouver Island and Coast",
                          str_detect(geography,"Lower Mainland") ~ "Lower Mainland - Southwest",
                          (str_detect(geography,"Thompson") & str_detect(geography,"Okanagan and Kootenay")) ~ "Thompson - Okanagan and Kootenay",
                          (str_detect(geography,"Thompson") & str_detect(geography,"Okanagan")) ~ "Thompson - Okanagan",
                          str_detect(geography,"Cariboo") ~ "Cariboo",
                          str_detect(geography,"North Coast, Nechako and Northeast") ~ "North Coast - Nechako and Northeast",
                          str_detect(geography,"North Coast, Nechako") ~ "North Coast and Nechako",
                          str_detect(geography,"British Columbia") ~ "British Columbia",
                          str_detect(geography,"Kootenay") ~ "Kootenay",
                          TRUE ~ "missing"))
# check
stat_can_data %>% filter(region=="missing") # expect 0 rows
stat_can_data %>% count(region,geography) # review regions

# filter out totals from age and study fields
stat_can_data <- stat_can_data %>%
  filter(age_group != "Total - population 17 to 64 years old" & major_field_cip != "Total - Major Field of study (BC Program Cluster aggregation of CIP 2016)")

