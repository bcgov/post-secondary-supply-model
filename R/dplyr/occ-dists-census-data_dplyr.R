# Occupation Distributions Census Data — dplyr Translation
# Original: R/occ-dists-census-data.R (~232 lines)
#
# Pipeline context:
#   Prepares census-based occupation distribution data for the PSSM model.
#   Reads NOC imputation outputs (CSV files per region), derives additional
#   regions (Northeast, Rest of Canada) by subtracting counts, then builds
#   the final Occupation_Distributions_Stat_Can table.
#
# Key translations:
#   - SELECT INTO (table copy) → R variable (no temp table needed)
#   - INSERT INTO (append rows) → bind_rows() before writing
#   - DROP TABLE cleanup → not needed (R variables auto-cleaned)
#   - tbl() with dplyr joins for main computation → kept as-is (already dplyr)
#
# Input:
#   - CSV files from NOC imputation output folder (per-region new counts)
#   - tbl_age_groups_rollup (DB lookup)
#   - t_current_region_pssm_rollup_codes_statcan (CSV lookup)
#
# Output:
#   - Occupation_Distributions_Stat_Can (written to DB)

library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(dbplyr)

# ---- Configure LAN and file paths ----
db_config <- config::get("decimal")
lan <- config::get("lan")
my_schema <- config::get("myschema")

decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")

# Helper: reference a table in the user's schema
sch_tbl <- function(name) {
  tbl(decimal_con, dbplyr::in_schema(my_schema, name))
}

# Helper: write to schema
write_schema_table <- function(name, data) {
  dbWriteTable(decimal_con, SQL(glue::glue('"{my_schema}"."{name}"')), data, overwrite = TRUE)
}

# ---- Import all CSV outputs from NOC imputation ----
output_folder <- glue::glue("{lan}/development/work/graduate noc imputation/output/")
new_counts_file_list <- list.files(path = output_folder, pattern = "\\- new counts.csv$", full.names = TRUE)

# Read all CSV files, add a column for the filename, and combine
combined_new_counts <- map_dfr(new_counts_file_list, ~ {
  name <- basename(.x)
  data <- read_csv(.x)
  mutate(data, file_name = str_split(name, " - new counts")[[1]][1])
})

# Save initial imputed data for reference
write_schema_table("Stat_Can_Imputed_Data_Raw", combined_new_counts)

# ---- Import required lookups ----
t_current_region_pssm_rollup_codes_statcan <-
  readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/02/T_Current_Region_PSSM_Rollup_Codes_StatCan.csv"), col_types = cols(.default = col_guess())) %>%
  janitor::clean_names(case = "all_caps")
write_schema_table("t_current_region_pssm_rollup_codes_statcan", t_current_region_pssm_rollup_codes_statcan)

# ---- Check for required data tables ----
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."tbl_age_groups_rollup"')))


# ******************************************************************************
# Derive additional regions by subtraction
# WHY: Census data provides composite regions (e.g., "North Coast - Nechako and Northeast")
# and sub-regions (e.g., "North Coast and Nechako"). We derive the missing sub-region
# (Northeast) by subtracting counts. Same pattern for Rest of Canada = Canada - BC.
# Negative differences are clamped to 0.
# ******************************************************************************

# ---- Northeast: "North Coast - Nechako and Northeast" minus "North Coast and Nechako" ----
NC_Nechako_NE <- combined_new_counts %>%
  filter(file_name == "North Coast - Nechako and Northeast")

NC_Nechako <- combined_new_counts %>%
  filter(file_name == "North Coast and Nechako")

qry_Northeast <- NC_Nechako_NE %>%
  inner_join(NC_Nechako, by = c("NOC_5", "major_field_cip", "age_group")) %>%
  mutate(
    file_name = "qry_Northeast",
    NOC_4 = NOC_4.x,
    occupation_NOC = occupation_NOC.x,
    New_Above_Bach = pmax(New_Above_Bach.x - New_Above_Bach.y, 0),
    New_PDEG = pmax(New_PDEG.x - New_PDEG.y, 0),
    New_Combined = pmax(New_Combined.x - New_Combined.y, 0),
    New_Masters = pmax(New_Masters.x - New_Masters.y, 0),
    New_Doctorate = pmax(New_Doctorate.x - New_Doctorate.y, 0)
  ) %>%
  select(-ends_with(".x"), -ends_with(".y"))

# ---- Rest of Canada: Canada minus British Columbia ----
Canada <- combined_new_counts %>%
  filter(file_name == "Canada")

British_Columbia <- combined_new_counts %>%
  filter(file_name == "British Columbia")

qry_Rest_of_Canada <- Canada %>%
  inner_join(British_Columbia, by = c("NOC_5", "major_field_cip", "age_group")) %>%
  mutate(
    file_name = "qry_Rest_of_Canada",
    NOC_4 = NOC_4.x,
    occupation_NOC = occupation_NOC.x,
    New_Above_Bach = pmax(New_Above_Bach.x - New_Above_Bach.y, 0),
    New_PDEG = pmax(New_PDEG.x - New_PDEG.y, 0),
    New_Combined = pmax(New_Combined.x - New_Combined.y, 0),
    New_Masters = pmax(New_Masters.x - New_Masters.y, 0),
    New_Doctorate = pmax(New_Doctorate.x - New_Doctorate.y, 0)
  ) %>%
  select(-ends_with(".x"), -ends_with(".y"))


# ******************************************************************************
# Combine all regions and build occupation distributions
# WHY: The original used SELECT INTO to copy the raw table, then INSERT INTO to
# append Northeast and Rest of Canada rows. We replace this with bind_rows() and
# a single write, eliminating the need for temp tables and DROP TABLE cleanup.
# ******************************************************************************
stat_can_updated <- bind_rows(combined_new_counts, qry_Northeast, qry_Rest_of_Canada)


# ******************************************************************************
# Prepare Occupation_Distributions_Stat_Can table
# WHY: Pivot the census counts from wide (one column per credential type) to long,
# map credential names to PSSM categories, join with region and age group lookups,
# compute totals and percentages. This follows the same pattern as the original
# but uses the in-memory stat_can_updated instead of the DB table.
# ******************************************************************************

# Join with lookups to filter to valid regions and get age group rollup codes
# WHY: Only regions in the StatCan rollup lookup are included; NA matches are
# regions not used in the model.
stat_can_with_lookups <- stat_can_updated %>%
  left_join(t_current_region_pssm_rollup_codes_statcan,
            by = c("file_name" = "CURRENT_REGION_PSSM_NAME_ROLLUP_STAT_CAN")) %>%
  left_join(sch_tbl("tbl_age_groups_rollup") %>% collect() |> rename_with(toupper),
            by = c("age_group" = "AGE_GROUP_ROLLUP_LABEL")) %>%
  filter(!is.na(CURRENT_REGION_PSSM_CODE_ROLLUP)) %>%
  select(-age_group, -file_name)

# Pivot from wide to long: one row per credential type
Combined_Stat_Can_Pivot <- stat_can_with_lookups %>%
  pivot_longer(cols = starts_with("New_"), names_to = "Credential") %>%
  filter(Credential != "New_Combined") %>%
  mutate(PSSM_CREDENTIAL = case_when(
    Credential == "New_Above_Bach" ~ "GRCT or GRDP",
    Credential == "New_PDEG"       ~ "PDEG",
    Credential == "New_Masters"    ~ "MAST",
    Credential == "New_Doctorate"  ~ "DOCT"
  ))

# Build composite keys and compute percentages
Combined_Stat_Can <- Combined_Stat_Can_Pivot %>%
  mutate(
    PSSM_CRED = PSSM_CREDENTIAL,
    LCIPPC_CD = substr(major_field_cip, 1, 2),
    LCIPPC_CD_CRED = paste0(LCIPPC_CD, " - ", PSSM_CREDENTIAL),
    SURVEY = "2021 Census PSSM 2022-2023"
  ) %>%
  rename(COUNT = value) %>%
  select(-Credential)

# Compute totals by CIP/Region/Age/Credential for percentage calculation
tmp_tbl_Calc_Total <- Combined_Stat_Can %>%
  group_by(LCIPPC_CD_CRED, CURRENT_REGION_PSSM_CODE_ROLLUP, AGE_GROUP_ROLLUP) %>%
  summarise(TOTAL = sum(COUNT), .groups = "drop")

# Join totals back and compute percentages
Occupation_Distributions_Stat_Can <- Combined_Stat_Can %>%
  inner_join(tmp_tbl_Calc_Total,
             by = c("LCIPPC_CD_CRED", "CURRENT_REGION_PSSM_CODE_ROLLUP", "AGE_GROUP_ROLLUP")) %>%
  mutate(PERCENT = ifelse(TOTAL == 0, 0, COUNT / TOTAL)) %>%
  filter(COUNT > 0) %>%
  select(
    SURVEY, PSSM_CREDENTIAL, PSSM_CRED,
    LCP4_CD = LCIPPC_CD, LCIP4_CRED = LCIPPC_CD_CRED,
    CURRENT_REGION_PSSM_CODE_ROLLUP, NOC = NOC_5,
    AGE_GROUP_ROLLUP, COUNT, TOTAL, PERCENT
  )

write_schema_table("Occupation_Distributions_Stat_Can", Occupation_Distributions_Stat_Can)


# ---- Clean Up ----
# No DROP TABLE needed — all intermediate data was in R variables, not DB tables.
# Only keep the final output table: Occupation_Distributions_Stat_Can.
# KEPT AS SQL: Drop the lookup table loaded earlier (cleanup shared with other scripts)
dbExecute(decimal_con, "DROP TABLE t_current_region_pssm_rollup_codes_statcan")

dbDisconnect(decimal_con)
