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

my_schema <- config::get("myschema")

# ---- Required Tables ----
# Stat Can data: See raw data documentation
# STAT_CAN
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."STAT_CAN"')))

# ---- Read from decimal ----
stat_can_data_raw <- dbReadTable(con, SQL(glue::glue('"{my_schema}"."STAT_CAN"')))

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

# review age groups and major fields total variable names
stat_can_data %>% count(age_group); stat_can_data %>% count(major_field_cip)

# filter out totals from age and study fields
stat_can_data <- stat_can_data %>%
  filter(age_group != "Total - population 17 to 64 years old" & major_field_cip != "Total - Major Field of study (BC Program Cluster aggregation of CIP 2016)")

# ---- Declare credential variables of interest ----
above_bach_var <- "university_certificate_or_diploma_above_bachelor_level"
pdeg_var <- "degree_in_medicine_dentistry_veterinary_medicine_or_optometry"
combined_masters_doctorate_var <- "masters_degree_and_earned_doctorate"
masters_var <- "masters_degree"
doctorate_var <- "earned_doctorate"
total_var <- "total_highest_certificate_diploma_or_degree"

# run graduate noc by region ----
lan <- config::get("lan")
regions <- stat_can_data %>% pull(region) %>% unique()

for(i in regions) {
  print(i)
  newcounts_fn=glue::glue("{lan}/data/statcan/output/",i," - new counts.csv")
  summary_fn=glue::glue("{lan}/data/statcan/output/",i," - summary.csv")
  data <- stat_can_data %>% filter(region==i)
  source(here::here("R","noc-imputation.R"))
}
