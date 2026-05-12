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

# This script processes cohort data from student outcomes and creates new labour supply distributions.
# Outcomes data has been standardized so all cohorts/surveys are combined in a single dataset.
#
# At a high level, the script:
#   Searches and updates invalid NOC codes in bgs and dacso tables
#   recodes the new labour supply for those with an NLS-2 record and no NLS-1
#   Weights each year up to the cohort (Prob_Weight) and apply year weights
#     (1,2,3,4,5) and adjust to the cohort.
#   Create weights for new labour supply (Weight_NLS)
#   Create weights for occupational distribution (Weight_OCC).
#
# Includes records with a labour force status for those aged 17 to 64,
# Includes those with an invalid NOC where 100% of CIP is invalid, as the cohort number.

# Notes:  create Weight_Age is used to calculate the age for the private institution credentials
# and needed if the data set doesn’t have age. Some invalid NOC codes (see documentation)
#         PDEG included at the end of occupation_distribution scripts.
#
# FIXME Labour_Supply_Distribution_LCP2/LCP_No_TT have LCP2_CRED not LCIP2_CRED
# FIXME Missing Non-Student Outcomes and PDEG at this point.  To be brought in after

library(tidyverse)
library(config)
library(glue)
library(assertthat)

# ---- Configure LAN and file paths ----
lan <- config::get("lan")

# library(RODBC)
# library(DBI)
# my_schema <- config::get("myschema")
# db_config <- config::get("decimal")
# decimal_con <- dbConnect(
#   odbc::odbc(),
#   Driver = db_config$driver,
#   Server = db_config$server,
#   Database = db_config$database,
#   Trusted_Connection = "True"
# )

# Load necessary libraries
# t_cohorts_recoded <- tibble(dbReadTable(
#   decimal_con,
#   SQL(glue::glue('"{my_schema}"."T_Cohorts_Recoded"'))
# ))
#
# labour_supply_distribution_stat_can <- tibble(dbReadTable(
#   decimal_con,
#   SQL(glue::glue('"{my_schema}"."Labour_Supply_Distribution_Stat_Can"'))
# ))

# t_current_region_pssm_codes <-
#   readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/02/T_Current_Region_PSSM_Codes.csv"), col_types = cols(.default = col_guess())) %>%
#   janitor::clean_names(case = "all_caps")
# t_current_region_pssm_rollup_codes <-
#   readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/02/T_Current_Region_PSSM_Rollup_Codes.csv"), col_types = cols(.default = col_guess())) %>%
#   janitor::clean_names(case = "all_caps")
# t_noc_broad_categories <-
#   readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/02/T_NOC_Broad_Categories_Updated.csv"), col_types = cols(.default = col_guess())) %>%
#   janitor::clean_names(case = "all_caps")

# List of required tables
required_tables <- c(
  "t_cohorts_recoded",
  "t_current_region_pssm_rollup_codes",
  "t_current_region_pssm_codes",
  "t_noc_broad_categories",
  "labour_supply_distribution_stat_can"
)

missing <- required_tables[!sapply(required_tables, exists, where = .GlobalEnv)]

# Assert that the table exists in the database
assert_that(
  length(missing) == 0,
  msg = paste(
    "Error:",
    "The following required tables are missing from the environment:",
    paste(missing, collapse = ", ")
  )
)

## set years
years <- c(2019, 2020, 2021, 2022, 2023)

# ------ set column names and table names ------
t_cohorts_recoded <- t_cohorts_recoded |>
  rename_with(toupper)
t_current_region_pssm_codes <- t_current_region_pssm_codes |>
  rename_with(toupper)
t_current_region_pssm_rollup_codes <- t_current_region_pssm_rollup_codes |>
  rename_with(toupper)
t_noc_broad_categories <- t_noc_broad_categories |>
  rename_with(toupper)
labour_supply_distribution_stat_can <- labour_supply_distribution_stat_can |>
  rename_with(toupper)

# -------------------------- initial data checks ---------------------

# ---- base weights
# dbGetQuery(decimal_con, DACSO_Q005_DACSO_DATA_Part_1b3_Check_Weights) # Check base weights
t_cohorts_recoded |>
  count(SURVEY, SURVEY_YEAR, WEIGHT) |>
  select(-n)

# ---- invalid noc codes
# dbExecute(decimal_con, DACSO_Q99A_STQUI_ID)
# dbGetQuery(decimal_con, DACSO_Q005_DACSO_DATA_Part_1b4_Check_NOC_Valid)

t_cohorts_recoded |>
  left_join(
    t_noc_broad_categories,
    by = c("NOC_CD" = "UNIT_GROUP_CODE"),
    keep = TRUE
  ) %>%
  mutate(
    SURVEY_STQU_ID = STQU_ID,
    STQU_ID_ONLY = sub("^.*?- ", "", STQU_ID)
  ) %>%
  filter(
    !is.na(AGE_GROUP_ROLLUP),
    CURRENT_REGION_PSSM_CODE != -1,
    SURVEY_YEAR %in% c('2019', '2020', '2021', '2022', '2023'),
    !is.na(NOC_CD),
    NOC_CD != "",
    is.na(UNIT_GROUP_CODE)
  ) |>
  distinct(STQU_ID_ONLY, STQU_ID, SURVEY, SURVEY_YEAR, NOC_CD, UNIT_GROUP_CODE)

# dbExecute(decimal_con, DACSO_Q005_DACSO_Data_Part_1b7_Update_After_Recoding)
# dbExecute(decimal_con, DACSO_Q005_DACSO_Data_Part_1b8_Update_After_Recoding)

# No invalid nocs in dacso survey data. If there were, we would need to account for them.
# in 2019 there were some invalid nocs: 403X were set to 4031, 4032, or 9999
# in 2021 there were some invalid nocs: 4122X were set to 99999

t_cohorts_recoded <- t_cohorts_recoded |>
  mutate(NOC_CD = if_else(NOC_CD == "4122X", "99999", NOC_CD))

# ---- recode new labour supply for those with an NLS-2 record and no NLS1

# dbExecute(decimal_con, DACSO_Q005_DACSO_DATA_Part_1c_NLS1)
# dbExecute(decimal_con, DACSO_Q005_DACSO_DATA_Part_1c_NLS2)
# dbExecute(decimal_con, DACSO_Q005_DACSO_DATA_Part_1c_NLS2_Recode)
dacso_q005_dacso_data_part_1c_nls1 <- t_cohorts_recoded %>%
  inner_join(t_current_region_pssm_codes, by = "CURRENT_REGION_PSSM_CODE") %>%
  inner_join(
    t_current_region_pssm_rollup_codes,
    by = "CURRENT_REGION_PSSM_CODE_ROLLUP"
  ) %>%
  filter(
    as.numeric(WEIGHT) > 0,
    !is.na(NOC_CD),
    !is.na(AGE_GROUP_ROLLUP),
    GRAD_STATUS %in% c('1', '3'),
    as.numeric(NEW_LABOUR_SUPPLY) == 1
  ) %>%
  group_by(
    SURVEY,
    CURRENT_REGION_PSSM_CODE_ROLLUP,
    AGE_GROUP_ROLLUP,
    INST_CD,
    LCIP4_CRED,
    GRAD_STATUS,
    NEW_LABOUR_SUPPLY
  ) %>%
  summarise(BASE = n(), .groups = "drop") %>%
  filter(BASE > 0)

dacso_q005_dacso_data_part_1c_nls2 <- t_cohorts_recoded %>%
  inner_join(t_current_region_pssm_codes, by = "CURRENT_REGION_PSSM_CODE") %>%
  inner_join(
    t_current_region_pssm_rollup_codes,
    by = "CURRENT_REGION_PSSM_CODE_ROLLUP"
  ) %>%
  filter(
    as.numeric(WEIGHT) > 0,
    !is.na(AGE_GROUP_ROLLUP),
    GRAD_STATUS %in% c('1', '3'),
    as.numeric(NEW_LABOUR_SUPPLY) == 2
  ) %>%
  distinct(
    SURVEY,
    CURRENT_REGION_PSSM_CODE_ROLLUP,
    AGE_GROUP_ROLLUP,
    INST_CD,
    LCIP4_CRED,
    GRAD_STATUS,
    NEW_LABOUR_SUPPLY,
    STQU_ID
  )

# DACSO_Q005_DACSO_DATA_Part_1c_NLS2_Recode
# Identify STQU_IDs where no match exists in NLS1 based on the specified columns
target_ids <- dacso_q005_dacso_data_part_1c_nls2 %>%
  left_join(
    dacso_q005_dacso_data_part_1c_nls1,
    by = c(
      "SURVEY",
      "CURRENT_REGION_PSSM_CODE_ROLLUP",
      "AGE_GROUP_ROLLUP",
      "INST_CD",
      "LCIP4_CRED"
    )
  ) %>%
  filter(is.na(BASE)) %>% # Checks for missing match in NLS1
  pull(STQU_ID)

t_cohorts_recoded <- t_cohorts_recoded %>%
  mutate(
    NEW_LABOUR_SUPPLY = if_else(STQU_ID %in% target_ids, 3, NEW_LABOUR_SUPPLY)
  )

rm(
  dacso_q005_dacso_data_part_1c_nls1,
  dacso_q005_dacso_data_part_1c_nls2,
  target_ids
)

# count the number of records in the cohort for the years included
# dbGetQuery(decimal_con, DACSO_Q005_Z_Cohort_Resp_by_Region)
t_cohorts_recoded %>%
  inner_join(t_current_region_pssm_codes, by = "CURRENT_REGION_PSSM_CODE") %>%
  filter(
    !is.na(AGE_GROUP_ROLLUP),
    RESPONDENT == '1',
    as.numeric(WEIGHT) > 0
  ) %>%
  group_by(
    SURVEY,
    SURVEY_YEAR,
    CURRENT_REGION_PSSM_CODE,
    CURRENT_REGION_PSSM_NAME,
    AGE_GROUP_ROLLUP
  ) %>%
  summarise(N = n(), .groups = "drop") %>%
  arrange(SURVEY, SURVEY_YEAR, CURRENT_REGION_PSSM_CODE)

# ------ create base weights for the full cohort
# dbExecute(decimal_con, DACSO_Q005_Z01_Base_NLS)
z01_base_nls <- t_cohorts_recoded %>%
  filter(
    NEW_LABOUR_SUPPLY %in% c(0, 1, 2, 3),
    as.numeric(WEIGHT) > 0,
    !is.na(AGE_GROUP_ROLLUP),
    GRAD_STATUS %in% c('1', '3')
  ) %>%
  group_by(
    SURVEY,
    INST_CD,
    AGE_GROUP_ROLLUP,
    TTRAIN,
    LCIP4_CRED,
    STQU_ID,
    GRAD_STATUS
  ) %>%
  summarise(BASE = n(), .groups = "drop") %>%
  select(SURVEY, INST_CD, AGE_GROUP_ROLLUP, TTRAIN, LCIP4_CRED, BASE, STQU_ID)

# not used but consider investigating (see documentation)
#dbExecute(decimal_con, DACSO_Q005_Z02a_Base)
#dbExecute(decimal_con, DACSO_Q005_Z02b_Respondents)
#dbExecute(decimal_con, DACSO_Q005_Z02b_Respondents_Region_9999)
#dbExecute(decimal_con, DACSO_Q005_Z02b_Respondents_Union)

# ------ create nls weights
#dbExecute(decimal_con, DACSO_Q005_Z02c_Weight_tmp)
z02c_weight_tmp <- t_cohorts_recoded %>%
  filter(
    NEW_LABOUR_SUPPLY %in% c(0, 1, 2, 3),
    as.numeric(WEIGHT) > 0,
    !is.na(AGE_GROUP_ROLLUP),
    GRAD_STATUS %in% c('1', '3')
  ) %>%
  group_by(
    SURVEY,
    SURVEY_YEAR,
    INST_CD,
    AGE_GROUP_ROLLUP,
    GRAD_STATUS,
    TTRAIN,
    LCIP4_CRED,
    WEIGHT
  ) %>%
  summarise(
    COUNT = n(),
    RESPONDENTS = sum(
      if_else(RESPONDENT == '1' & CURRENT_REGION_PSSM_CODE != -1, 1, 0),
      na.rm = TRUE
    ),
    .groups = "drop"
  ) %>%
  mutate(WEIGHT_YEAR = WEIGHT) %>%
  select(
    SURVEY,
    SURVEY_YEAR,
    INST_CD,
    AGE_GROUP_ROLLUP,
    GRAD_STATUS,
    TTRAIN,
    LCIP4_CRED,
    COUNT,
    RESPONDENTS,
    WEIGHT_YEAR
  )

#dbExecute(decimal_con, DACSO_Q005_Z02c_Weight)
z02c_weight <- z02c_weight_tmp %>%
  mutate(
    WEIGHT_PROB = if_else(
      RESPONDENTS == 0,
      1,
      as.numeric(COUNT) / as.numeric(RESPONDENTS)
    ),
    WEIGHT = WEIGHT_PROB * WEIGHT_YEAR,
    WEIGHTED = RESPONDENTS * WEIGHT_PROB * WEIGHT_YEAR
  )

#dbExecute(decimal_con, DACSO_Q005_Z03_Weight_Total)
z03_weight_total <- z02c_weight %>%
  group_by(
    SURVEY,
    INST_CD,
    AGE_GROUP_ROLLUP,
    GRAD_STATUS,
    TTRAIN,
    LCIP4_CRED
  ) %>%
  summarise(
    BASE = sum(COUNT, na.rm = TRUE),
    WEIGHTED = sum(WEIGHTED, na.rm = TRUE),
    .groups = "drop"
  )

# dbExecute(decimal_con, DACSO_Q005_Z04_Weight_Adj_Fac)
z04_weight_adj_fac <- z03_weight_total %>%
  mutate(WEIGHT_ADJ_FAC = if_else(WEIGHTED == 0, 0, BASE / WEIGHTED))

# dbExecute(decimal_con, DACSO_Q005_Z05_Weight_NLS)
tmp_tbl_weights_nls <- z02c_weight %>%
  inner_join(
    z04_weight_adj_fac |>
      select(
        SURVEY,
        INST_CD,
        AGE_GROUP_ROLLUP,
        GRAD_STATUS,
        LCIP4_CRED,
        WEIGHT_ADJ_FAC
      ),
    by = c("SURVEY", "INST_CD", "AGE_GROUP_ROLLUP", "GRAD_STATUS", "LCIP4_CRED")
  ) %>%
  mutate(WEIGHT_NLS = WEIGHT * WEIGHT_ADJ_FAC) %>%
  select(
    SURVEY,
    SURVEY_YEAR,
    INST_CD,
    AGE_GROUP_ROLLUP,
    GRAD_STATUS,
    TTRAIN,
    LCIP4_CRED,
    COUNT,
    RESPONDENTS,
    WEIGHT_PROB,
    WEIGHT_YEAR,
    WEIGHT,
    WEIGHTED,
    WEIGHT_ADJ_FAC,
    WEIGHT_NLS
  )

# null Weight_NLS field and update (if you’ve been messing with iterations)
# dbExecute(decimal_con, DACSO_Q005_Z08_Weight_NLS_Update)
# dbExecute(decimal_con, DACSO_Q005_Z07_Weight_NLS_Null)

t_cohorts_recoded <- t_cohorts_recoded |> mutate(WEIGHT_NLS = NA_real_)

t_cohorts_recoded <- t_cohorts_recoded %>%
  left_join(
    tmp_tbl_weights_nls %>%
      select(
        SURVEY,
        SURVEY_YEAR,
        INST_CD,
        AGE_GROUP_ROLLUP,
        GRAD_STATUS,
        LCIP4_CRED,
        NEW_VAL = WEIGHT_NLS
      ),
    by = c(
      "SURVEY",
      "SURVEY_YEAR",
      "INST_CD",
      "AGE_GROUP_ROLLUP",
      "GRAD_STATUS",
      "LCIP4_CRED"
    )
  ) %>%
  mutate(
    # Only update if current_region_pssm_code != -1 and we have a new value
    WEIGHT_NLS = if_else(
      CURRENT_REGION_PSSM_CODE != -1 & !is.na(NEW_VAL),
      NEW_VAL,
      WEIGHT_NLS
    )
  ) %>%
  select(-NEW_VAL)

# ------ check weights and clear interim tables
# dbGetQuery(decimal_con, DACSO_Q005_Z09_Check_Weights)
z09_check_weights <- t_cohorts_recoded %>%
  inner_join(z01_base_nls |> select(STQU_ID, BASE), by = "STQU_ID") %>%
  group_by(
    SURVEY_YEAR,
    INST_CD,
    AGE_GROUP_ROLLUP,
    GRAD_STATUS,
    TTRAIN,
    LCIP4_CRED,
    WEIGHT_NLS
  ) %>%
  summarise(
    RESPONDENTS = sum(
      if_else(RESPONDENT == '1' & CURRENT_REGION_PSSM_CODE != -1, 1, 0),
      na.rm = TRUE
    ),
    WEIGHTED = RESPONDENTS * as.numeric(WEIGHT_NLS),
    BASE = sum(BASE, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(SURVEY_YEAR, WEIGHT_NLS)

# dbGetQuery(decimal_con, DACSO_Q005_Z09_Check_Weights_No_Weight_CIP)
z09_check_weights_no_weight_cip <- t_cohorts_recoded %>%
  filter(
    NEW_LABOUR_SUPPLY %in% c(0, 1, 2, 3),
    as.numeric(WEIGHT) > 0,
    (WEIGHT_NLS == 0 | is.na(WEIGHT_NLS)),
    CURRENT_REGION_PSSM_CODE != -1,
    !is.na(AGE_GROUP_ROLLUP),
    GRAD_STATUS %in% c('1', '2', '3')
  ) %>%
  group_by(
    SURVEY,
    INST_CD,
    AGE_GROUP_ROLLUP,
    TTRAIN,
    LCIP4_CRED,
    GRAD_STATUS
  ) %>%
  summarise(BASE = n(), .groups = "drop") %>%
  select(SURVEY, INST_CD, AGE_GROUP_ROLLUP, TTRAIN, LCIP4_CRED, BASE)

rm(
  z01_base_nls,
  z02c_weight_tmp,
  z02c_weight,
  z03_weight_total,
  z04_weight_adj_fac,
  tmp_tbl_weights_nls
)


# apply nls weights to group totals
# dbExecute(decimal_con, DACSO_Q006a_Weight_New_Labour_Supply)
dacso_q006a_weight_new_labour_supply <- t_cohorts_recoded %>%
  inner_join(t_current_region_pssm_codes, by = "CURRENT_REGION_PSSM_CODE") %>%
  inner_join(
    t_current_region_pssm_rollup_codes,
    by = "CURRENT_REGION_PSSM_CODE_ROLLUP"
  ) %>%
  filter(
    RESPONDENT == '1',
    as.numeric(WEIGHT) > 0,
    CURRENT_REGION_PSSM_CODE_ROLLUP != 9999,
    !is.na(AGE_GROUP_ROLLUP),
    GRAD_STATUS %in% c('1', '3'),
    NEW_LABOUR_SUPPLY %in% c(0, 1, 2, 3)
  ) %>%
  group_by(
    PSSM_CREDENTIAL,
    PSSM_CRED,
    CURRENT_REGION_PSSM_CODE_ROLLUP,
    SURVEY_YEAR,
    INST_CD,
    AGE_GROUP_ROLLUP,
    GRAD_STATUS,
    LCP4_CD,
    TTRAIN,
    LCIP4_CRED,
    LCIP2_CRED,
    NEW_LABOUR_SUPPLY,
    WEIGHT_NLS
  ) %>%
  summarise(COUNT = n(), .groups = "drop") %>%
  mutate(WEIGHTED = COUNT * as.numeric(WEIGHT_NLS)) %>%
  select(
    PSSM_CREDENTIAL,
    PSSM_CRED,
    CURRENT_REGION_PSSM_CODE_ROLLUP,
    SURVEY_YEAR,
    INST_CD,
    AGE_GROUP_ROLLUP,
    GRAD_STATUS,
    LCP4_CD,
    TTRAIN,
    LCIP4_CRED,
    LCIP2_CRED,
    NEW_LABOUR_SUPPLY,
    COUNT,
    WEIGHT_NLS,
    WEIGHTED
  )

# calculate weighted new labor supply - various distributions
# dbExecute(decimal_con, DACSO_Q006b_Weighted_New_Labour_Supply)
# dbExecute(decimal_con, DACSO_Q006b_Weighted_New_Labour_Supply_0)
# dbExecute(decimal_con, DACSO_Q006b_Weighted_New_Labour_Supply_0_2D)
# dbExecute(decimal_con, DACSO_Q006b_Weighted_New_Labour_Supply_0_2D_No_TT)
# dbExecute(decimal_con, DACSO_Q006b_Weighted_New_Labour_Supply_0_No_TT)
# dbExecute(decimal_con, DACSO_Q006b_Weighted_New_Labour_Supply_2D)
# dbExecute(decimal_con, DACSO_Q006b_Weighted_New_Labour_Supply_2D_No_TT)
# dbExecute(decimal_con, DACSO_Q006b_Weighted_New_Labour_Supply_No_TT)
# dbExecute(decimal_con, DACSO_Q006b_Weighted_New_Labour_Supply_Total)
# dbExecute(decimal_con, DACSO_Q006b_Weighted_New_Labour_Supply_Total_2D)
# dbExecute(decimal_con, DACSO_Q006b_Weighted_New_Labour_Supply_Total_2D_No_TT)
# dbExecute(decimal_con, DACSO_Q006b_Weighted_New_Labour_Supply_Total_No_TT)
# DACSO_Q006b_Weighted_New_Labour_Supply
dacso_q006b_weighted_new_labour_supply <- dacso_q006a_weight_new_labour_supply %>%
  filter(NEW_LABOUR_SUPPLY %in% c(1, 2, 3), !is.na(AGE_GROUP_ROLLUP)) %>%
  group_by(
    PSSM_CREDENTIAL,
    PSSM_CRED,
    CURRENT_REGION_PSSM_CODE_ROLLUP,
    AGE_GROUP_ROLLUP,
    LCP4_CD,
    TTRAIN,
    LCIP4_CRED,
    LCIP2_CRED
  ) %>%
  summarise(
    NEW_COUNT = sum(WEIGHTED, na.rm = TRUE),
    UNWEIGHTED_COUNT = sum(COUNT, na.rm = TRUE),
    .groups = "drop"
  ) |>
  rename(COUNT = NEW_COUNT)

# DACSO_Q006b_Weighted_New_Labour_Supply_0
dacso_q006b_weighted_new_labour_supply_0 <- dacso_q006a_weight_new_labour_supply %>%
  filter(NEW_LABOUR_SUPPLY == 0) %>%
  group_by(
    PSSM_CREDENTIAL,
    PSSM_CRED,
    CURRENT_REGION_PSSM_CODE_ROLLUP,
    AGE_GROUP_ROLLUP,
    LCP4_CD,
    TTRAIN,
    LCIP4_CRED,
    LCIP2_CRED
  ) %>%
  summarise(COUNT = sum(WEIGHTED, na.rm = TRUE), .groups = "drop")

# DACSO_Q006b_Weighted_New_Labour_Supply_0_2D
dacso_q006b_weighted_new_labour_supply_0_2d <- dacso_q006a_weight_new_labour_supply %>%
  filter(NEW_LABOUR_SUPPLY == 0) %>%
  group_by(
    PSSM_CREDENTIAL,
    PSSM_CRED,
    CURRENT_REGION_PSSM_CODE_ROLLUP,
    AGE_GROUP_ROLLUP,
    LCP2_CD = substr(LCP4_CD, 1, 2),
    TTRAIN,
    LCP2_CRED = LCIP2_CRED
  ) %>%
  summarise(COUNT = sum(WEIGHTED, na.rm = TRUE), .groups = "drop")

# DACSO_Q006b_Weighted_New_Labour_Supply_0_2D_No_TT
dacso_q006b_weighted_new_labour_supply_0_2d_no_tt <- dacso_q006a_weight_new_labour_supply %>%
  filter(NEW_LABOUR_SUPPLY == 0) %>%
  mutate(
    LCP2_CD = substr(LCP4_CD, 1, 2),
    PREFIX = if_else(
      substr(PSSM_CRED, 1, 1) %in% c('1', '3'),
      paste0(substr(PSSM_CRED, 1, 1), " - "),
      ""
    ),
    LCP2_CRED = paste0(PREFIX, LCP2_CD, " - ", PSSM_CREDENTIAL)
  ) %>%
  group_by(
    PSSM_CREDENTIAL,
    PSSM_CRED,
    CURRENT_REGION_PSSM_CODE_ROLLUP,
    AGE_GROUP_ROLLUP,
    LCP2_CD,
    LCP2_CRED
  ) %>%
  summarise(COUNT = sum(WEIGHTED, na.rm = TRUE), .groups = "drop")

# DACSO_Q006b_Weighted_New_Labour_Supply_0_No_TT
dacso_q006b_weighted_new_labour_supply_0_no_tt <- dacso_q006a_weight_new_labour_supply %>%
  filter(NEW_LABOUR_SUPPLY == 0) %>%
  mutate(
    GS_PREFIX = if_else(
      is.na(GRAD_STATUS),
      NA_character_,
      paste0(as.character(GRAD_STATUS), " - ")
    ),
    LCIP4_CRED = paste0(GS_PREFIX, LCP4_CD, " - ", PSSM_CREDENTIAL),
    LCIP2_CRED = paste0(
      GS_PREFIX,
      substr(LCP4_CD, 1, 2),
      " - ",
      PSSM_CREDENTIAL
    )
  ) %>%
  group_by(
    PSSM_CREDENTIAL,
    PSSM_CRED,
    CURRENT_REGION_PSSM_CODE_ROLLUP,
    AGE_GROUP_ROLLUP,
    LCP4_CD,
    LCIP4_CRED,
    LCIP2_CRED
  ) %>%
  summarise(COUNT = sum(WEIGHTED, na.rm = TRUE), .groups = "drop")

# DACSO_Q006b_Weighted_New_Labour_Supply_2D
dacso_q006b_weighted_new_labour_supply_2d <- dacso_q006a_weight_new_labour_supply %>%
  filter(NEW_LABOUR_SUPPLY %in% c(1, 2, 3)) %>%
  group_by(
    PSSM_CREDENTIAL,
    PSSM_CRED,
    CURRENT_REGION_PSSM_CODE_ROLLUP,
    AGE_GROUP_ROLLUP,
    LCP2_CD = substr(LCP4_CD, 1, 2),
    TTRAIN,
    LCP2_CRED = LCIP2_CRED
  ) %>%
  summarise(COUNT = sum(WEIGHTED, na.rm = TRUE), .groups = "drop")

# DACSO_Q006b_Weighted_New_Labour_Supply_2D_No_TT
dacso_q006b_weighted_new_labour_supply_2d_no_tt <- dacso_q006a_weight_new_labour_supply %>%
  filter(NEW_LABOUR_SUPPLY %in% c(1, 2, 3)) %>%
  mutate(
    LCP2_CD = substr(LCP4_CD, 1, 2),
    PREFIX = if_else(
      substr(PSSM_CRED, 1, 1) %in% c('1', '3'),
      paste0(substr(PSSM_CRED, 1, 1), " - "),
      ""
    ),
    LCP2_CRED = paste0(PREFIX, LCP2_CD, " - ", PSSM_CREDENTIAL)
  ) %>%
  group_by(
    PSSM_CREDENTIAL,
    PSSM_CRED,
    CURRENT_REGION_PSSM_CODE_ROLLUP,
    AGE_GROUP_ROLLUP,
    LCP2_CD,
    LCP2_CRED
  ) %>%
  summarise(COUNT = sum(WEIGHTED, na.rm = TRUE), .groups = "drop")

# DACSO_Q006b_Weighted_New_Labour_Supply_No_TT
dacso_q006b_weighted_new_labour_supply_no_tt <- dacso_q006a_weight_new_labour_supply %>%
  filter(NEW_LABOUR_SUPPLY %in% c(1, 2, 3), !is.na(AGE_GROUP_ROLLUP)) %>%
  mutate(
    GS_PREFIX = if_else(
      is.na(GRAD_STATUS),
      NA_character_,
      paste0(as.character(GRAD_STATUS), " - ")
    ),
    LCIP4_CRED = paste0(GS_PREFIX, LCP4_CD, " - ", PSSM_CREDENTIAL),
    LCIP2_CRED = paste0(
      GS_PREFIX,
      substr(LCP4_CD, 1, 2),
      " - ",
      PSSM_CREDENTIAL
    )
  ) %>%
  group_by(
    PSSM_CREDENTIAL,
    PSSM_CRED,
    CURRENT_REGION_PSSM_CODE_ROLLUP,
    AGE_GROUP_ROLLUP,
    LCP4_CD,
    LCIP4_CRED,
    LCIP2_CRED
  ) %>%
  summarise(
    NEW_COUNT = sum(WEIGHTED, na.rm = TRUE),
    UNWEIGHTED_COUNT = sum(COUNT, na.rm = TRUE),
    .groups = "drop"
  ) |>
  rename(COUNT = NEW_COUNT)

# DACSO_Q006b_Weighted_New_Labour_Supply_Total
dacso_q006b_weighted_new_labour_supply_total <- dacso_q006a_weight_new_labour_supply %>%
  filter(NEW_LABOUR_SUPPLY %in% c(0, 1, 2, 3)) %>%
  group_by(
    PSSM_CREDENTIAL,
    PSSM_CRED,
    AGE_GROUP_ROLLUP,
    LCP4_CD,
    TTRAIN,
    LCIP4_CRED,
    LCIP2_CRED
  ) %>%
  summarise(TOTAL = sum(WEIGHTED, na.rm = TRUE), .groups = "drop")

# DACSO_Q006b_Weighted_New_Labour_Supply_Total_2D
dacso_q006b_weighted_new_labour_supply_total_2d <- dacso_q006a_weight_new_labour_supply %>%
  filter(NEW_LABOUR_SUPPLY %in% c(0, 1, 2, 3)) %>%
  group_by(
    PSSM_CREDENTIAL,
    PSSM_CRED,
    AGE_GROUP_ROLLUP,
    LCP2_CD = substr(LCP4_CD, 1, 2),
    TTRAIN,
    LCP2_CRED = LCIP2_CRED
  ) %>%
  summarise(TOTAL = sum(WEIGHTED, na.rm = TRUE), .groups = "drop")

# DACSO_Q006b_Weighted_New_Labour_Supply_Total_2D_No_TT
dacso_q006b_weighted_new_labour_supply_total_2d_no_tt <- dacso_q006a_weight_new_labour_supply %>%
  filter(NEW_LABOUR_SUPPLY %in% c(0, 1, 2, 3)) %>%
  mutate(
    LCP2_CD = substr(LCP4_CD, 1, 2),
    PREFIX = if_else(
      substr(PSSM_CRED, 1, 1) %in% c('1', '3'),
      paste0(substr(PSSM_CRED, 1, 1), " - "),
      ""
    ),
    LCP2_CRED = paste0(PREFIX, LCP2_CD, " - ", PSSM_CREDENTIAL)
  ) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP_ROLLUP, LCP2_CD, LCP2_CRED) %>%
  summarise(TOTAL = sum(WEIGHTED, na.rm = TRUE), .groups = "drop")

# DACSO_Q006b_Weighted_New_Labour_Supply_Total_No_TT
dacso_q006b_weighted_new_labour_supply_total_no_tt <- dacso_q006a_weight_new_labour_supply %>%
  filter(NEW_LABOUR_SUPPLY %in% c(0, 1, 2, 3)) %>%
  mutate(
    GS_PREFIX = if_else(
      is.na(GRAD_STATUS),
      NA_character_,
      paste0(as.character(GRAD_STATUS), " - ")
    ),
    LCIP4_CRED = paste0(GS_PREFIX, LCP4_CD, " - ", PSSM_CREDENTIAL),
    LCIP2_CRED = paste0(
      GS_PREFIX,
      substr(LCP4_CD, 1, 2),
      " - ",
      PSSM_CREDENTIAL
    )
  ) %>%
  group_by(
    PSSM_CREDENTIAL,
    PSSM_CRED,
    AGE_GROUP_ROLLUP,
    LCP4_CD,
    LCIP4_CRED,
    LCIP2_CRED
  ) %>%
  summarise(TOTAL = sum(WEIGHTED, na.rm = TRUE), .groups = "drop")


dbExecute(decimal_con, DACSO_Q007a_Weighted_New_Labour_Supply)
dbExecute(decimal_con, DACSO_Q007a_Weighted_New_Labour_Supply_0)
dbExecute(decimal_con, DACSO_Q007a_Weighted_New_Labour_Supply_0_2D)
dbExecute(decimal_con, DACSO_Q007a_Weighted_New_Labour_Supply_0_2D_No_TT)
dbExecute(decimal_con, DACSO_Q007a_Weighted_New_Labour_Supply_0_No_TT)
dbExecute(decimal_con, DACSO_Q007a_Weighted_New_Labour_Supply_2D)
dbExecute(decimal_con, DACSO_Q007a_Weighted_New_Labour_Supply_2D_No_TT)
dbExecute(decimal_con, DACSO_Q007a_Weighted_New_Labour_Supply_No_TT)

dbExecute(decimal_con, "DROP TABLE DACSO_Q006a_Weight_New_Labour_Supply")
dbExecute(decimal_con, "DROP TABLE DACSO_Q006b_Weighted_New_Labour_Supply")
dbExecute(decimal_con, "DROP TABLE DACSO_Q006b_Weighted_New_Labour_Supply_0")
dbExecute(decimal_con, "DROP TABLE DACSO_Q006b_Weighted_New_Labour_Supply_0_2D")
dbExecute(
  decimal_con,
  "DROP TABLE dacso_q006b_weighted_new_labour_supply_0_2d_no_tt"
)
dbExecute(
  decimal_con,
  "DROP TABLE dacso_q006b_weighted_new_labour_supply_0_no_tt"
)
dbExecute(decimal_con, "DROP TABLE DACSO_Q006b_Weighted_New_Labour_Supply_2D")
dbExecute(
  decimal_con,
  "DROP TABLE dacso_q006b_weighted_new_labour_supply_2d_no_tt"
)
dbExecute(
  decimal_con,
  "DROP TABLE DACSO_Q006b_Weighted_New_Labour_Supply_Total"
)
dbExecute(
  decimal_con,
  "DROP TABLE DACSO_Q006b_Weighted_New_Labour_Supply_Total_2D"
)
dbExecute(
  decimal_con,
  "DROP TABLE DACSO_Q006b_Weighted_New_Labour_Supply_Total_2D_No_TT"
)
dbExecute(
  decimal_con,
  "DROP TABLE dacso_q006b_weighted_new_labour_supply_total_no_tt"
)
dbExecute(
  decimal_con,
  "DROP TABLE dacso_q006b_weighted_new_labour_supply_no_tt"
)

# ---- Final Distributions ----
nls_def <- c(
  Survey = "nvarchar(50)",
  PSSM_Credential = "nvarchar(50)",
  PSSM_CRED = "nvarchar(50)",
  LCP4_CD = "nvarchar(50)",
  TTRAIN = "nvarchar(50)",
  LCIP4_CRED = "nvarchar(50)",
  LCIP2_CRED = "nvarchar(50)",
  Current_Region_PSSM_Code_Rollup = "integer",
  Age_Group_Rollup = "integer",
  Count = "float",
  Total = "float",
  New_Labour_Supply = "float"
)

if (
  !dbExistsTable(
    decimal_con,
    SQL(glue::glue('"{my_schema}"."Labour_Supply_Distribution"'))
  )
) {
  dbCreateTable(decimal_con, "Labour_Supply_Distribution", nls_def)
}
if (
  !dbExistsTable(
    decimal_con,
    SQL(glue::glue('"{my_schema}"."Labour_Supply_Distribution_No_TT"'))
  )
) {
  dbCreateTable(decimal_con, "Labour_Supply_Distribution_No_TT", nls_def)
}

dbExecute(decimal_con, DACSO_Q007b0_Delete_New_Labour_Supply)
dbExecute(decimal_con, DACSO_Q007b0_Delete_New_Labour_Supply_No_TT)
# dbExecute(decimal_con, DACSO_Q007b0_Delete_New_Labour_Supply_No_TT_QI)
# dbExecute(decimal_con, DACSO_Q007b0_Delete_New_Labour_Supply_QI)
dbExecute(decimal_con, DACSO_Q007b1_Append_New_Labour_Supply)
dbExecute(decimal_con, DACSO_Q007b1_Append_New_Labour_Supply_No_TT)
dbExecute(decimal_con, DACSO_Q007b2_Append_New_Labour_Supply_0)
dbExecute(decimal_con, DACSO_Q007b2_Append_New_Labour_Supply_0_No_TT)

nls_def <- c(
  Survey = "nvarchar(50)",
  PSSM_Credential = "nvarchar(50)",
  PSSM_CRED = "nvarchar(50)",
  LCP2_CD = "nvarchar(50)",
  TTRAIN = "nvarchar(50)",
  LCP2_CRED = "nvarchar(50)",
  Current_Region_PSSM_Code_Rollup = "integer",
  Age_Group_Rollup = "integer",
  Count = "float",
  Total = "float",
  New_Labour_Supply = "float"
)

if (
  !dbExistsTable(
    decimal_con,
    SQL(glue::glue('"{my_schema}"."Labour_Supply_Distribution_LCP2"'))
  )
) {
  dbCreateTable(
    decimal_con,
    SQL(glue::glue('"{my_schema}"."Labour_Supply_Distribution_LCP2"')),
    nls_def
  )
}
if (
  !dbExistsTable(
    decimal_con,
    SQL(glue::glue('"{my_schema}"."Labour_Supply_Distribution_LCP2_No_TT"'))
  )
) {
  dbCreateTable(
    decimal_con,
    SQL(glue::glue('"{my_schema}"."Labour_Supply_Distribution_LCP2_No_TT"')),
    nls_def
  )
}

dbExecute(decimal_con, DACSO_Q007c0_Delete_New_Labour_Supply_2D)
dbExecute(decimal_con, DACSO_Q007c0_Delete_New_Labour_Supply_2D_No_TT)
# dbExecute(decimal_con, DACSO_Q007c0_Delete_New_Labour_Supply_2D_No_TT_QI)
# dbExecute(decimal_con, DACSO_Q007c0_Delete_New_Labour_Supply_2D_QI)
dbExecute(decimal_con, DACSO_Q007c1_Append_New_Labour_Supply_2D)
dbExecute(decimal_con, DACSO_Q007c1_Append_New_Labour_Supply_2D_No_TT)
dbExecute(decimal_con, DACSO_Q007c2_Append_New_Labour_Supply_0_2D)
dbExecute(decimal_con, DACSO_Q007c2_Append_New_Labour_Supply_0_2D_No_TT)

dbExecute(decimal_con, "DROP TABLE DACSO_Q007a_Weighted_New_Labour_Supply")
dbExecute(decimal_con, "DROP TABLE DACSO_Q007a_Weighted_New_Labour_Supply_0")
dbExecute(decimal_con, "DROP TABLE DACSO_Q007a_Weighted_New_Labour_Supply_0_2D")
dbExecute(
  decimal_con,
  "DROP TABLE DACSO_Q007a_Weighted_New_Labour_Supply_0_2D_No_TT"
)
dbExecute(
  decimal_con,
  "DROP TABLE DACSO_Q007a_Weighted_New_Labour_Supply_0_No_TT"
)
dbExecute(decimal_con, "DROP TABLE DACSO_Q007a_Weighted_New_Labour_Supply_2D")
dbExecute(
  decimal_con,
  "DROP TABLE DACSO_Q007a_Weighted_New_Labour_Supply_2D_No_TT"
)
dbExecute(
  decimal_con,
  "DROP TABLE DACSO_Q007a_Weighted_New_Labour_Supply_No_TT"
)

dbExecute(
  decimal_con,
  "DELETE 
                        FROM Labour_Supply_Distribution
                        WHERE (((Labour_Supply_Distribution.Survey)='2021 Census PSSM 2023-2024'));"
)

# uncomment if running for the first time
# dbExecute(decimal_con, "ALTER TABLE Labour_Supply_Distribution_Stat_Can ADD TTRAIN NVARCHAR(50)")
# dbExecute(decimal_con, "ALTER TABLE Labour_Supply_Distribution_Stat_Can ADD LCIP2_CRED NVARCHAR(50)")
dbExecute(
  decimal_con,
  "INSERT INTO Labour_Supply_Distribution (
	   [SURVEY]
      ,[PSSM_CREDENTIAL]
      ,[PSSM_CRED]
      ,[LCP4_CD]
      ,[LCIP4_CRED]
      ,[CURRENT_REGION_PSSM_CODE_ROLLUP]
      ,[AGE_GROUP_ROLLUP]
      ,[COUNT]
      ,[TOTAL]
      ,[NEW_LABOUR_SUPPLY])
SELECT '2021 Census PSSM 2023-2024' as Survey
      ,[PSSM_CREDENTIAL]
      ,[PSSM_CRED]
      ,[LCP4_CD]
      ,[LCIP4_CRED]
      ,[CURRENT_REGION_PSSM_CODE_ROLLUP]
      ,[AGE_GROUP_ROLLUP]
      ,[COUNT]
      ,[TOTAL]
      ,[NEW_LABOUR_SUPPLY] FROM Labour_Supply_Distribution_Stat_Can"
)


# ---- Clean Up ----

# ---- Keep ----
dbExistsTable(decimal_con, "Labour_Supply_Distribution")
dbExistsTable(decimal_con, "Labour_Supply_Distribution_No_TT")
dbExistsTable(decimal_con, "Labour_Supply_Distribution_LCP2")
dbExistsTable(decimal_con, "Labour_Supply_Distribution_LCP2_No_TT")
dbExistsTable(decimal_con, "tmp_tbl_Weights_NLS")


# Make sure in the check weights script, they are all as expected for non QI (e.g., 1,2,3,4,5). If different, then either skipped the right _QI in 02b-1, or in load-appso.
DACSO_Q005_DACSO_DATA_Part_1b3_Check_Weights <- "
SELECT t_cohorts_recoded.survey,
       t_cohorts_recoded.survey_year,
       t_cohorts_recoded.weight
FROM   t_cohorts_recoded
GROUP  BY t_cohorts_recoded.survey,
          t_cohorts_recoded.survey_year,
          t_cohorts_recoded.weight;"
dbGetQuery(decimal_con, DACSO_Q005_DACSO_DATA_Part_1b3_Check_Weights)
