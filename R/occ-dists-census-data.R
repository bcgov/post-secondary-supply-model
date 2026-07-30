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

required_tables <- c(
  "new_noc_counts",
  "t_current_region_pssm_rollup_codes_statcan",
  "tbl_age_groups_rollup"
)

missing <- required_tables[!sapply(required_tables, exists, where = .GlobalEnv)]

if (length(missing) > 0) {
  stop(paste(
    "The following required tables are missing from the environment:",
    paste(missing, collapse = ", ")
  ))
}

library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(RJDBC)

# ---- Configure LAN and file paths ----
db_config <- config::get("decimal")
lan <- config::get("lan")
my_schema <- config::get("myschema")

# ---- Connection to decimal ----
decimal_con <- dbConnect(
  odbc::odbc(),
  Driver = db_config$driver,
  Server = db_config$server,
  Database = db_config$database,
  Trusted_Connection = "True"
)

# ---- Import all csv outputs ----
output_folder <- glue::glue(
  "{lan}/development/work/graduate noc imputation/output/"
)

# Get a list of all new counts CSV files in the output folder
new_counts_file_list <- list.files(
  path = output_folder,
  pattern = "\\- new counts.csv$",
  full.names = TRUE
)
new_counts_file_list

# Read all CSV files, add a column for the filename, and combine them into one data frame
combined_new_counts <- map_dfr(
  new_counts_file_list,
  ~ {
    name <- basename(.x)
    data <- read_csv(.x)
    data <- mutate(data, file_name = str_split(name, " - new counts")[[1]][1])

    return(data)
  }
)

# save initial imputed data
dbWriteTable(
  decimal_con,
  name = Id(schema = my_schema, table = "Stat_Can_Imputed_Data_Raw_r"),
  combined_new_counts
)

# ---- Import required lookups ----
t_current_region_pssm_rollup_codes_statcan <-
  readr::read_csv(
    glue::glue(
      "{lan}/development/csv/gh-source/lookups/02/T_Current_Region_PSSM_Rollup_Codes_StatCan.csv"
    ),
    col_types = cols(.default = col_guess())
  ) %>%
  janitor::clean_names(case = "all_caps")

dbWriteTable(
  decimal_con,
  name = Id(
    schema = my_schema,
    table = "t_current_region_pssm_rollup_codes_statcan_r"
  ),
  value = t_current_region_pssm_rollup_codes_statcan
)

# ---- Check for required data tables ----
# lookups
dbExistsTable(
  decimal_con,
  Id(schema = my_schema, table = "tbl_age_groups_rollup_r")
)

# ---- Create required Region counts ----
## Create Northeast ----
# Take "North Coast - Nechako and Northeast" and remove "North Coast and Nechako" to get Northeast
NC_Nechako_NE <- tbl(
  decimal_con,
  Id(schema = my_schema, table = "Stat_Can_Imputed_Data_Raw_r")
) %>%
  filter(file_name == "North Coast - Nechako and Northeast") %>%
  collect()

NC_Nechako <- tbl(
  decimal_con,
  Id(schema = my_schema, table = "Stat_Can_Imputed_Data_Raw_r")
) %>%
  filter(file_name == "North Coast and Nechako") %>%
  collect()

qry_Northeast <- NC_Nechako_NE %>%
  inner_join(NC_Nechako, by = c('NOC_5', 'major_field_cip', 'age_group')) %>%
  mutate(
    file_name = "qry_Northeast",
    NOC_4 = NOC_4.x,
    occupation_NOC = occupation_NOC.x
  ) %>%
  mutate(
    New_Above_Bach = ifelse(
      (New_Above_Bach.x - New_Above_Bach.y <= 0),
      0,
      (New_Above_Bach.x - New_Above_Bach.y)
    ),
    New_PDEG = ifelse(
      (New_PDEG.x - New_PDEG.y <= 0),
      0,
      (New_PDEG.x - New_PDEG.y)
    ),
    New_Combined = ifelse(
      (New_Combined.x - New_Combined.y <= 0),
      0,
      (New_Combined.x - New_Combined.y)
    ),
    New_Masters = ifelse(
      (New_Masters.x - New_Masters.y <= 0),
      0,
      (New_Masters.x - New_Masters.y)
    ),
    New_Doctorate = ifelse(
      (New_Doctorate.x - New_Doctorate.y <= 0),
      0,
      (New_Doctorate.x - New_Doctorate.y)
    )
  ) %>%
  select(-ends_with(".x"), -ends_with(".y"))

dbWriteTable(
  decimal_con,
  name = Id(schema = my_schema, table = "qry_Northeast"),
  value = qry_Northeast
)


## Create Rest of Canada counts ----
# Take "Canada" and remove "British Columbia" to get Rest of Canada
Canada <- tbl(
  decimal_con,
  Id(schema = my_schema, table = "Stat_Can_Imputed_Data_Raw_r")
) %>%
  filter(file_name == "Canada") %>%
  collect()

British_Columbia <- tbl(
  decimal_con,
  Id(schema = my_schema, table = "Stat_Can_Imputed_Data_Raw_r")
) %>%
  filter(file_name == "British Columbia") %>%
  collect()

qry_Rest_of_Canada <- Canada %>%
  inner_join(
    British_Columbia,
    by = c('NOC_5', 'major_field_cip', 'age_group')
  ) %>%
  mutate(
    file_name = "qry_Rest_of_Canada",
    NOC_4 = NOC_4.x,
    occupation_NOC = occupation_NOC.x
  ) %>%
  mutate(
    New_Above_Bach = ifelse(
      (New_Above_Bach.x - New_Above_Bach.y <= 0),
      0,
      (New_Above_Bach.x - New_Above_Bach.y)
    ),
    New_PDEG = ifelse(
      (New_PDEG.x - New_PDEG.y <= 0),
      0,
      (New_PDEG.x - New_PDEG.y)
    ),
    New_Combined = ifelse(
      (New_Combined.x - New_Combined.y <= 0),
      0,
      (New_Combined.x - New_Combined.y)
    ),
    New_Masters = ifelse(
      (New_Masters.x - New_Masters.y <= 0),
      0,
      (New_Masters.x - New_Masters.y)
    ),
    New_Doctorate = ifelse(
      (New_Doctorate.x - New_Doctorate.y <= 0),
      0,
      (New_Doctorate.x - New_Doctorate.y)
    )
  ) %>%
  select(-ends_with(".x"), -ends_with(".y"))

dbWriteTable(
  decimal_con,
  name = Id(schema = my_schema, table = "qry_Rest_of_Canada"),
  value = qry_Rest_of_Canada
)

# ---- Add the updated regions to an updated StatCan table ----
# make new table
dbExecute(
  decimal_con,
  SQL(glue::glue(
    "SELECT *
               INTO [{my_schema}].[Stat_Can_Imputed_Data_Updated_r]
               FROM [{my_schema}].[Stat_Can_Imputed_Data_Raw_r];"
  ))
)

# add Northeast
dbGetQuery(
  decimal_con,
  SQL(glue::glue(
    "INSERT INTO [{my_schema}].[Stat_Can_Imputed_Data_Updated_r]
  ( age_group, major_field_cip, NOC_5, file_name, NOC_4, occupation_NOC,
  New_Above_Bach, New_PDEG, New_Combined, New_Masters, New_Doctorate )
SELECT q.age_group,
q.major_field_cip,
q.NOC_5,
q.file_name,
q.NOC_4,
q.occupation_NOC,
q.New_Above_Bach,
q.New_PDEG,
q.New_Combined,
q.New_Masters,
q.New_Doctorate
FROM [{my_schema}].[qry_Northeast] AS q"
  ))
)

# Add rest of canada
dbGetQuery(
  decimal_con,
  SQL(glue::glue(
    "INSERT INTO [{my_schema}].[Stat_Can_Imputed_Data_Updated_r]
  ( age_group, major_field_cip, NOC_5, file_name, NOC_4, occupation_NOC,
  New_Above_Bach, New_PDEG, New_Combined, New_Masters, New_Doctorate )
SELECT q.age_group,
q.major_field_cip,
q.NOC_5,
q.file_name,
q.NOC_4,
q.occupation_NOC,
q.New_Above_Bach,
q.New_PDEG,
q.New_Combined,
q.New_Masters,
q.New_Doctorate
FROM [{my_schema}].[qry_Rest_of_Canada] AS q"
  ))
)

# ---- Prepare a Stat_Can version of Occupation_Distributions table ----
## Add in lookups ----
# filter out unused regions based on lookup table
Combined_Stat_Can_Original <- tbl(
  decimal_con,
  Id(schema = my_schema, table = "Stat_Can_Imputed_Data_Updated_r")
) %>%
  left_join(
    tbl(
      decimal_con,
      Id(
        schema = my_schema,
        table = "t_current_region_pssm_rollup_codes_statcan_r"
      )
    ),
    by = c("file_name" = "CURRENT_REGION_PSSM_NAME_ROLLUP_STAT_CAN")
  ) %>%
  left_join(
    tbl(decimal_con, Id(schema = my_schema, table = "tbl_age_groups_rollup_r")),
    by = c("age_group" = "AGE_GROUP_ROLLUP_LABEL")
  ) %>%
  filter(!is.na(CURRENT_REGION_PSSM_CODE_ROLLUP)) %>%
  select(-age_group, -file_name) %>%
  collect()

## Prepare columns ----
# create one column for all the counts & rename accordingly
Combined_Stat_Can_Pivot <- Combined_Stat_Can_Original %>%
  pivot_longer(cols = starts_with("New_"), names_to = "Credential") %>%
  filter(!Credential == "New_Combined") %>%
  mutate(
    PSSM_CREDENTIAL = case_when(
      Credential == "New_Above_Bach" ~ "GRCT or GRDP",
      Credential == "New_PDEG" ~ "PDEG",
      Credential == "New_Masters" ~ "MAST",
      Credential == "New_Doctorate" ~ "DOCT"
    )
  )

# update required variables
Combined_Stat_Can <- Combined_Stat_Can_Pivot %>%
  mutate(PSSM_CRED = PSSM_CREDENTIAL) %>%
  mutate(LCIPPC_CD = substr(major_field_cip, 1, 2)) %>%
  mutate(LCIPPC_CD_CRED = paste0(LCIPPC_CD, " - ", PSSM_CREDENTIAL)) %>%
  mutate(SURVEY = "2021 Census PSSM 2022-2023") %>%
  rename(COUNT = value) %>%
  select(-Credential)

## Run calculations ----
# Find totals by CIP/Region/Age/Credential
tmp_tbl_Calc_Total <- Combined_Stat_Can %>%
  select(
    LCIPPC_CD_CRED,
    CURRENT_REGION_PSSM_CODE_ROLLUP,
    AGE_GROUP_ROLLUP,
    COUNT
  ) %>%
  group_by(
    LCIPPC_CD_CRED,
    CURRENT_REGION_PSSM_CODE_ROLLUP,
    AGE_GROUP_ROLLUP
  ) %>%
  summarise(TOTAL = sum(COUNT))

# add totals to table
Combined_Stat_Can <- Combined_Stat_Can %>%
  inner_join(
    tmp_tbl_Calc_Total %>%
      select(
        TOTAL,
        LCIPPC_CD_CRED,
        CURRENT_REGION_PSSM_CODE_ROLLUP,
        AGE_GROUP_ROLLUP
      ),
    by = c(
      "AGE_GROUP_ROLLUP",
      "CURRENT_REGION_PSSM_CODE_ROLLUP",
      "LCIPPC_CD_CRED"
    )
  )

# calculate the percents
Combined_Stat_Can <- Combined_Stat_Can %>%
  mutate(PERCENT = ifelse(TOTAL == 0, 0, COUNT / TOTAL))


## Manipulate the table ----
# remove any with 0 counts; select desired columns
Occupation_Distributions_Stat_Can <- Combined_Stat_Can %>%
  filter(COUNT > 0) %>%
  select(
    SURVEY,
    PSSM_CREDENTIAL,
    PSSM_CRED,
    LCP4_CD = LCIPPC_CD,
    LCIP4_CRED = LCIPPC_CD_CRED,
    CURRENT_REGION_PSSM_CODE_ROLLUP,
    NOC = NOC_5,
    AGE_GROUP_ROLLUP,
    COUNT,
    TOTAL,
    PERCENT
  )

## Save table ----
dbWriteTable(
  decimal_con,
  name = Id(schema = my_schema, table = "Occupation_Distributions_Stat_Can_r"),
  Occupation_Distributions_Stat_Can
)

# ---- Clean Up ----
## Drop intermediate tables ----
dbExecute(
  decimal_con,
  SQL(glue::glue("DROP TABLE [{my_schema}].[qry_Northeast]"))
)
dbExecute(
  decimal_con,
  SQL(glue::glue("DROP TABLE [{my_schema}].[qry_Rest_of_Canada]"))
)
dbExecute(
  decimal_con,
  SQL(glue::glue("DROP TABLE [{my_schema}].[Stat_Can_Imputed_Data_Updated_r]"))
)
dbExecute(
  decimal_con,
  SQL(glue::glue("DROP TABLE [{my_schema}].[Stat_Can_Imputed_Data_Raw_r]"))
)
## Drop lookups ----
dbExecute(
  decimal_con,
  SQL(glue::glue(
    "DROP TABLE [{my_schema}].[t_current_region_pssm_rollup_codes_statcan_r]"
  ))
)
## Disconnect ----
dbDisconnect(decimal_con)
