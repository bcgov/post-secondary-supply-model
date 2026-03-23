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

# ---- Data Requirements and SQL Definitons ----
# these should now be in the R environment
required_tables <- c(
  "stat_can_data_raw"
)

missing <- required_tables[!sapply(required_tables, exists, where = .GlobalEnv)]

if (length(missing) > 0) {
  stop(paste(
    "The following required tables are missing from the environment:",
    paste(missing, collapse = ", ")
  ))
}

# ---- Clean up data ----
stat_can_data_raw %>% count(geography)
stat_can_data_raw %>% count(age_group)
stat_can_data_raw %>% count(major_field_cip)

data <- stat_can_data
working_data <- data |>
  select(
    geography,
    region,
    age_group,
    occupation_NOC,
    major_field_cip,
    ,
    "above_bach" = university_certificate_or_diploma_above_bachelor_level,
    "pdeg" = degree_in_medicine_dentistry_veterinary_medicine_or_optometry,
    "combined" = masters_degree_and_earned_doctorate,
    "masters" = masters_degree,
    "doctorate" = earned_doctorate,
    "total" = total_highest_certificate_diploma_or_degree
  ) |>
  mutate(
    NOC = str_extract(occupation_NOC, "^\\d+"),
    NOC_LVL = if_else(
      !is.na(NOC),
      as.character(str_length(NOC)),
      occupation_NOC
    )
  ) |>
  select(-occupation_NOC, -geography)

# Pull NOC 5 Distribution ----
noc_5 <- working_data |>
  filter(NOC_LVL %in% c("5")) |>
  rename(noc_5 = NOC) |>
  mutate(noc_4 = str_extract(noc_5, "^\\d{1,4}")) |>
  select(-NOC_LVL) |>
  distinct()

noc_5 <- noc_5 |>
  group_by(age_group, region, major_field_cip, noc_4) |>
  mutate(sum_total_noc5 = sum(total)) |>
  ungroup() |>
  pivot_longer(
    cols = c("above_bach", "pdeg", "combined", "masters", "doctorate"),
    names_to = "credential_name",
    values_to = "credential" # Or whatever name you prefer for the counts
  )

# Pull NOC 4 Distribution ----
noc_4 <- working_data |>
  filter(NOC_LVL %in% c("4")) |>
  rename(noc_4 = NOC) |>
  select(-NOC_LVL) |>
  distinct()

noc_4 <- noc_4 |>
  group_by(age_group, region, major_field_cip, noc_4) |>
  mutate(sum_total_noc4 = sum(total)) |>
  ungroup() |>
  pivot_longer(
    cols = c("above_bach", "pdeg", "combined", "masters", "doctorate"),
    names_to = "credential_name",
    values_to = "cred_total"
  ) |>
  rename(noc4_total = total)

# Combine NOC 4 and NOC 5 distributions and create new count summary
noc_4_noc_5 <- noc_4 |>
  left_join(
    noc_5,
    by = c("age_group", "region", "major_field_cip", "noc_4", "credential_name")
  )

# missing occupation_noc
new_noc_counts <- noc_4_noc_5 |>
  group_by(age_group, region, major_field_cip, noc_4, credential_name) |>
  mutate(sum_total = sum(total)) |>
  mutate(
    new_total_total = max(
      sum_total - sum(ifelse(credential == 0, 0, total)),
      0
    ),
    new_cred_total = max(cred_total - sum(credential), 0),
    new_credential = ifelse(
      credential == 0,
      ifelse(
        new_total_total == 0,
        0,
        ifelse(
          total > new_total_total,
          new_cred_total,
          new_cred_total * total / new_total_total
        )
      ),
      credential
    )
  ) |>
  select(
    age_group,
    region,
    major_field_cip,
    noc_4,
    noc_5,
    credential_name,
    new_credential
  )

# Make summary tables for comparison ----
all_occupations_summary <- working_data |>
  filter(NOC_LVL %in% c("4", "5", "All occupations")) %>%
  group_by(region, age_group, major_field_cip, NOC_LVL) %>%
  summarize(across(where(is.numeric), sum)) |>
  mutate(
    NOC_LVL = case_when(
      NOC_LVL == "4" ~ "NOC4",
      NOC_LVL == 5 ~ "NOC5",
      TRUE ~ "All_Occs"
    )
  ) |>
  pivot_wider(
    id_cols = c(region, age_group, major_field_cip),
    names_from = NOC_LVL,
    values_from = c(above_bach, pdeg, combined, masters, doctorate, total),
    names_glue = "{NOC_LVL}_{.value}"
  ) |>
  select(-contains("_TOTAL")) |>
  ungroup()

# New Summary table by 5D NOC ----
NOC_5_summary_2 <- new_noc_counts %>%
  group_by(region, age_group, major_field_cip, credential_name) %>%
  summarize(New_Noc4 = sum(new_credential)) |>
  ungroup() |>
  pivot_wider(
    names_from = credential_name,
    values_from = New_Noc4,
    names_glue = "New_Noc4_{.name}"
  ) |>
  ungroup()

compare_summaries <- all_occupations_summary %>%
  left_join(NOC_5_summary_2, by = c("age_group", "major_field_cip", "region")) # Combine summary tables ----

# got to here
# add total row
compare_summaries <- compare_summaries %>%
  bind_rows(
    compare_summaries %>%
      ungroup() %>%
      select(-age_group, -major_field_cip) %>%
      summarize_all(sum) %>%
      mutate(age_group = "Total", major_field_cip = "Total")
  )

# Save files ----
write_csv(new_noc_counts, newcounts_fn)
write_csv(compare_summaries, summary_fn)
