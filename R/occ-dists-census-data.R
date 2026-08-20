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

# This script prepares census data for the Occupations_Distributions table.
#
# WHERE THIS SITS IN THE MODEL (companion: R/02b-3-pssm-cohorts-occupation-
# distributions.R; weighting context: docs/weights-explained-02b-2-and-02b-3.md):
#   OCCSN(NOC) = GRADUATES(cred,age) x P(CIP|cred,age)
#                x P(in labour supply|CIP)   <- 02b-2
#                x P(NOC|CIP,region)         <- 02b-3, benchmarked by THIS TABLE
# As on the labour-supply side, graduate credentials (GRCT or GRDP, PDEG,
# MAST, DOCT) have no student-outcomes survey coverage -- their occupation
# mix comes from the 2021 Census (distributed to NOCs by the imputation
# work below), and 02b-3 appends this table's rows to
# occupation_distributions (Survey label "2021 Census ..."; downstream
# consumers prefix-match that label).
#
# MANUAL PER-CYCLE PREREQUISITE -- this script is NOT in the
# prep-for-fresh-run.R chain. Run it BY HAND before 02b-3 when the output
# table below is absent on the model database (as it was in the 2025
# refresh); nothing else produces it.
#
# PREREQUISITE OF THIS SCRIPT: the graduate NOC-imputation work must be
# COMPLETE first -- its per-region "<region> - new counts.csv" outputs
# (LAN development/work/graduate noc imputation/output/) are the raw
# counts consumed below.
#
# Inputs: those imputation CSVs; LAN lookup
# T_Current_Region_PSSM_Rollup_Codes_StatCan.csv; tbl_age_groups_rollup_r
# already present in the personal schema (loader-chain output; stage from
# dbo when standalone).
#
# Output (personal schema):
#   - Occupation_Distributions_Stat_Can_r -- what 02b-3's guarded,
#     `_r`-preferring read expects. Intermediates (Stat_Can_Imputed_Data_
#     Raw_r / _Updated_r, qry_Northeast, qry_Rest_of_Canada) and the
#     statcan rollup lookup `_r` are dropped again in the cleanup. NOTE:
#     this script writes+drops the same lookup `_r` table as
#     labour-supply-dists-census-data.R -- run the two census scripts
#     SEQUENTIALLY, never concurrently. The hardcoded SURVEY label keeps
#     the export VINTAGE ("2021 Census PSSM 2022-2023"); 02b-3 relabels to
#     the current model year at read time. Do not "fix" the year here.

library(tidyverse)
library(RODBC)
library(config)
library(DBI)
## ----------------------------------------------------------
## Reasons for change, other notes
## 2025 refresh: library(RJDBC) dropped -- loaded but never used
## (the script connects via DBI/odbc), and it pulls in rJava/Oracle
## dependencies for nothing. Both census prep scripts ran clean
## with it removed.
## ----------------------------------------------------------

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
# Each "<region> - new counts.csv" file is one census geography's imputed
# counts (NOC_5 x CIP x age group, split by credential). file_name keeps
# the region so the combined table stays addressable by geography. The raw
# copy is written to the personal schema so the region derivations and the
# final lookup join can run database-side.
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
# Same census rollup lookup as the labour-supply script: region names ->
# PSSM region-rollup codes, staged to the personal schema for the DB-side
# join. (Sequencing note in the header -- the labour script drops this same
# `_r` table in its cleanup.)
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
# The age lookup is NOT created here -- it must already exist in the
# personal schema (loader-chain output; stage from dbo when standalone).
# The existence check only prints; the join later fails if false.
# lookups
dbExistsTable(
  decimal_con,
  Id(schema = my_schema, table = "tbl_age_groups_rollup_r")
)

# ---- Create required Region counts ----
# Same two nested-geography splits as the labour-supply script, but done by
# inner-joining the two regions' rows on (NOC_5, CIP, age) and subtracting
# the count columns (.x = the combined/wider region, .y = the sub-region),
# flooring differences at 0 (imputed counts can round past each other):
#   Northeast      = "North Coast - Nechako and Northeast" minus
#                    "North Coast and Nechako"
#   Rest of Canada = "Canada" minus "British Columbia"
# Each derived region keeps the combined region's NOC_4 / occupation label.
## Create Northeast ----
# Take "North Coast - Nechako and Northeast" and remove "North Coast and Nechako" to get Northeast
NC_Nechako_NE <- tbl(decimal_con, Id(schema = my_schema, table ="Stat_Can_Imputed_Data_Raw_r")) %>%
  filter(file_name == "North Coast - Nechako and Northeast") %>%
  collect()

NC_Nechako <- tbl(decimal_con, Id(schema = my_schema, table ="Stat_Can_Imputed_Data_Raw_r")) %>%
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
Canada <- tbl(decimal_con, Id(schema = my_schema, table ="Stat_Can_Imputed_Data_Raw_r")) %>%
  filter(file_name == "Canada") %>%
  collect()

British_Columbia <- tbl(decimal_con, Id(schema = my_schema, table ="Stat_Can_Imputed_Data_Raw_r")) %>%
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
# Copy the raw regions, then INSERT the two derived regions, producing one
# table covering all geographies.
# make new table
dbExecute(
  decimal_con,
  SQL(glue::glue("SELECT *
               INTO [{my_schema}].[Stat_Can_Imputed_Data_Updated_r]
               FROM [{my_schema}].[Stat_Can_Imputed_Data_Raw_r];"))
)

# add Northeast
dbGetQuery(
  decimal_con,
  SQL(glue::glue("INSERT INTO [{my_schema}].[Stat_Can_Imputed_Data_Updated_r]
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
FROM [{my_schema}].[qry_Northeast] AS q"))
)

# Add rest of canada
dbGetQuery(
  decimal_con,
  SQL(glue::glue("INSERT INTO [{my_schema}].[Stat_Can_Imputed_Data_Updated_r]
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
FROM [{my_schema}].[qry_Rest_of_Canada] AS q"))
)

# ---- Prepare a Stat_Can version of Occupation_Distributions table ----
## Add in lookups ----
# Map region names -> PSSM region-rollup codes and age labels -> age-rollup
# codes; rows matching neither (geographies/ages the PSSM does not model)
# drop out via the !is.na filter.
# filter out unused regions based on lookup table
Combined_Stat_Can_Original <- tbl(
  decimal_con,
  Id(schema = my_schema, table = "Stat_Can_Imputed_Data_Updated_r")
) %>%
  left_join(
    tbl(decimal_con, Id(schema = my_schema, table = "t_current_region_pssm_rollup_codes_statcan_r")),
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
# The imputation splits each cell's counts into per-credential columns
# (New_Above_Bach / New_PDEG / New_Masters / New_Doctorate / New_Combined).
# Pivot them long and relabel to the PSSM credential names; New_Combined
# (all above-bachelor credentials pooled) is dropped -- the model wants
# per-credential rows only.
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
# LCIPPC_CD is the 2-digit CIP prefix (name historical); LCIPPC_CD_CRED
# ("XX - CREDENTIAL") is the census-row key format 02b-3's append expects.
Combined_Stat_Can <- Combined_Stat_Can_Pivot %>%
  mutate(PSSM_CRED = PSSM_CREDENTIAL) %>%
  mutate(LCIPPC_CD = substr(major_field_cip, 1, 2)) %>%
  mutate(LCIPPC_CD_CRED = paste0(LCIPPC_CD, " - ", PSSM_CREDENTIAL)) %>%
  mutate(SURVEY = "2021 Census PSSM 2022-2023") %>%
  rename(COUNT = value) %>%
  select(-Credential)

## Run calculations ----
# Find totals by CIP/Region/Age/Credential
# NOTE the contrast with the labour-supply script: that benchmark divides
# by the CANADA-wide pool; this one divides WITHIN each (2-digit CIP x
# credential x region x age) cell -- PERCENT = COUNT / TOTAL is the census
# occupation mix P(NOC | CIP2, region, age) that 02b-3 needs, so the
# denominator must be regional.
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
# NOC at the 5-digit level (NOC_5; NOC_4 is carried in the raw data).
# Column set/order matches what 02b-3's append transmute expects.
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
  name = Id(schema = my_schema, table ="Occupation_Distributions_Stat_Can_r"),
  Occupation_Distributions_Stat_Can
)

# ---- Clean Up ----
## Drop intermediate tables ----
dbExecute(decimal_con, SQL(glue::glue("DROP TABLE [{my_schema}].[qry_Northeast]")))
dbExecute(decimal_con, SQL(glue::glue("DROP TABLE [{my_schema}].[qry_Rest_of_Canada]")))
dbExecute(decimal_con, SQL(glue::glue("DROP TABLE [{my_schema}].[Stat_Can_Imputed_Data_Updated_r]")))
dbExecute(decimal_con, SQL(glue::glue("DROP TABLE [{my_schema}].[Stat_Can_Imputed_Data_Raw_r]")))
## Drop lookups ----
dbExecute(
  decimal_con,
  SQL(glue::glue("DROP TABLE [{my_schema}].[t_current_region_pssm_rollup_codes_statcan_r]"))
)
## Disconnect ----
dbDisconnect(decimal_con)
