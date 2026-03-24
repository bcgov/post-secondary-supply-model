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


stat_can_imputed_data_raw <- new_noc_counts |> rename(file_name = region)

# ---- Create required Region counts ----
## Create Northeast ----
# Take "North Coast - Nechako and Northeast" and remove "North Coast and Nechako" to get Northeast
NC_Nechako_NE <-
  stat_can_imputed_data_raw |>
  filter(file_name == "North Coast - Nechako and Northeast")

NC_Nechako <- stat_can_imputed_data_raw |>
  filter(file_name == "North Coast and Nechako")

qry_Northeast <- NC_Nechako_NE %>%
  inner_join(
    NC_Nechako,
    by = c('noc_5', 'major_field_cip', 'age_group', 'noc_4', 'occupation_NOC')
  ) %>%
  mutate(
    file_name = "qry_Northeast"
  ) %>%
  mutate(
    New_above_bach = ifelse(
      (New_above_bach.x - New_above_bach.y <= 0),
      0,
      (New_above_bach.x - New_above_bach.y)
    ),
    New_PDEG = ifelse(
      (New_pdeg.x - New_pdeg.y <= 0),
      0,
      (New_pdeg.x - New_pdeg.y)
    ),
    New_Combined = ifelse(
      (New_combined.x - New_combined.y <= 0),
      0,
      (New_combined.x - New_combined.y)
    ),
    New_Masters = ifelse(
      (New_masters.x - New_masters.y <= 0),
      0,
      (New_masters.x - New_masters.y)
    ),
    New_Doctorate = ifelse(
      (New_doctorate.x - New_doctorate.y <= 0),
      0,
      (New_doctorate.x - New_doctorate.y)
    )
  ) %>%
  select(-ends_with(".x"), -ends_with(".y"))


## Create Rest of Canada counts ----
# Take "Canada" and remove "British Columbia" to get Rest of Canada
Canada <- stat_can_imputed_data_raw |>
  filter(file_name == "Canada")

British_Columbia <- stat_can_imputed_data_raw |>
  filter(file_name == "British Columbia")

qry_Rest_of_Canada <- Canada %>%
  inner_join(
    British_Columbia,
    by = c('noc_5', 'noc_4', 'occupation_NOC', 'major_field_cip', 'age_group')
  ) %>%
  mutate(
    file_name = "qry_Rest_of_Canada"
  ) %>%
  mutate(
    New_Above_Bach = ifelse(
      (New_above_bach.x - New_above_bach.y <= 0),
      0,
      (New_above_bach.x - New_above_bach.y)
    ),
    New_PDEG = ifelse(
      (New_pdeg.x - New_pdeg.y <= 0),
      0,
      (New_pdeg.x - New_pdeg.y)
    ),
    New_Combined = ifelse(
      (New_combined.x - New_combined.y <= 0),
      0,
      (New_combined.x - New_combined.y)
    ),
    New_Masters = ifelse(
      (New_masters.x - New_masters.y <= 0),
      0,
      (New_masters.x - New_masters.y)
    ),
    New_Doctorate = ifelse(
      (New_doctorate.x - New_doctorate.y <= 0),
      0,
      (New_doctorate.x - New_doctorate.y)
    )
  ) %>%
  select(-ends_with(".x"), -ends_with(".y"))

# ---- Add the updated regions to an updated StatCan table ----
names(qry_Rest_of_Canada) <- tolower(names(qry_Rest_of_Canada))
names(qry_Northeast) <- tolower(names(qry_Northeast))
names(stat_can_imputed_data_raw) <- tolower(names(stat_can_imputed_data_raw))

stat_can_imputed_data_imputed <- stat_can_imputed_data_raw |>
  rbind(qry_Rest_of_Canada) |>
  rbind(qry_Northeast)


# ---- Prepare a Stat_Can version of Occupation_Distributions table ----
# Note: original versions were filtered on age groups here
combined_stat_can_original <- stat_can_imputed_data_imputed |>
  left_join(
    t_current_region_pssm_rollup_codes_statcan,
    by = c("file_name" = "CURRENT_REGION_PSSM_NAME_ROLLUP_STAT_CAN")
  ) |>
  left_join(
    tbl_age_groups_rollup,
    by = c("age_group" = "AGE_GROUP_ROLLUP_LABEL")
  ) |>
  filter(!is.na(CURRENT_REGION_PSSM_CODE_ROLLUP)) |>
  select(-age_group, -file_name)

## Prepare columns ----
# create one column for all the counts & rename accordingly
combined_stat_can <- combined_stat_can_original %>%
  pivot_longer(cols = starts_with("new_"), names_to = "Credential") %>%
  filter(!Credential == "new_combined") %>%
  mutate(
    PSSM_CREDENTIAL = case_when(
      Credential == "new_above_bach" ~ "GRCT or GRDP",
      Credential == "new_pdeg" ~ "PDEG",
      Credential == "new_masters" ~ "MAST",
      Credential == "new_doctorate" ~ "DOCT",
      TRUE ~ NA_character_
    )
  ) |>
  mutate(PSSM_CRED = PSSM_CREDENTIAL) %>%
  mutate(LCIPPC_CD = substr(major_field_cip, 1, 2)) %>%
  mutate(LCIPPC_CD_CRED = paste0(LCIPPC_CD, " - ", PSSM_CREDENTIAL)) %>%
  mutate(SURVEY = "2021 Census PSSM 2022-2023") %>%
  rename(COUNT = value) %>%
  select(-Credential)

## Run calculations ----
# Find totals by CIP/Region/Age/Credential
tmp_tbl_calc_total <- combined_stat_can %>%
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
  summarise(TOTAL = sum(COUNT, na.rm = TRUE))

# add totals to table
combined_stat_can <- combined_stat_can %>%
  inner_join(
    tmp_tbl_calc_total %>%
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
combined_stat_can <- combined_stat_can %>%
  mutate(PERCENT = ifelse(TOTAL == 0, 0, COUNT / TOTAL))


## Manipulate the table ----
# remove any with 0 counts; select desired columns
occupation_distributions_stat_can <- combined_stat_can %>%
  filter(COUNT > 0) %>%
  select(
    SURVEY,
    PSSM_CREDENTIAL,
    PSSM_CRED,
    LCP4_CD = LCIPPC_CD,
    LCIP4_CRED = LCIPPC_CD_CRED,
    CURRENT_REGION_PSSM_CODE_ROLLUP,
    NOC = noc_5,
    AGE_GROUP_ROLLUP = AGE_GROUP_ROLLUP,
    COUNT,
    TOTAL,
    PERCENT
  ) |>
  mutate(across(where(is.numeric), ~ round(.x, 3)))
