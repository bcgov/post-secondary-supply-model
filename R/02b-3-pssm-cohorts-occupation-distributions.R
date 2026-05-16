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

# This script processes cohort data from student outcomes and derives occupation distributions.
# Outcomes data has been standardized so all cohorts/surveys are combined in a single dataset before
# processing.
#
#   Create weights for occupational distribution (Weight_OCC).
#
# Includes records with a labour force status for those aged 17 to 64,
# Includes those with an invalid NOC where 100% of CIP is invalid, as the cohort number.

# Note:  create Weight_Age is used to calculate the age for the private institution credentials
# and needed if the data set doesn’t have age.
# Check PDCT or PDDP
# DACSO_Q008_Z01_Base_OCC documentation - records should be the same as equivalent NLS query

library(tidyverse)
library(glue)
library(assertthat)

# -------------------------- DEVELOPMENT ONLY -------------------------------------
# use this section for development and checking against the SQL versions
# remove whatever we don't use when done, depending on final design decisions

# library(RODBC)
# library(DBI)
# library(config)
#
#
# # ---- Configure LAN and file paths ----
# db_config <- config::get("decimal")
# lan <- config::get("lan")
# my_schema <- config::get("myschema")
#
# # ---- Connection to database ----
# db_config <- config::get("decimal")
# decimal_con <- dbConnect(
#   odbc::odbc(),
#   Driver = db_config$driver,
#   Server = db_config$server,
#   Database = db_config$database,
#   Trusted_Connection = "True"
# )

# occupation_distributions_stat_can <- dbReadTable(
#   decimal_con,
#   SQL(glue::glue('"Occupation_Distributions_Stat_Can"'))
# )
#
# # ---- Source Queries ----
# source(glue::glue(
#   "./sql/02b-pssm-cohorts/02b-pssm-cohorts-occupation-distributions.R"
# ))

# -------------------------- Required Tables -----------------------------------------

# List of required tables
required_tables <- c(
  "t_cohorts_recoded",
  "t_current_region_pssm_codes",
  "t_current_region_pssm_rollup_codes",
  "tmp_tbl_weights_nls",
  "t_noc_broad_categories",
  "occupation_distributions_stat_can"
)

# Check for required data tables in the database
missing <- required_tables[!sapply(required_tables, exists, where = .GlobalEnv)]

if (length(missing) > 0) {
  stop(paste(
    "The following required tables are missing from the environment:",
    paste(missing, collapse = ", ")
  ))
}

# --------------------------  Standardize Names -----------------------------
t_cohorts_recoded <- t_cohorts_recoded |>
  rename_with(toupper)
t_current_region_pssm_codes <- t_current_region_pssm_codes |>
  rename_with(toupper)
t_current_region_pssm_rollup_codes <- t_current_region_pssm_rollup_codes |>
  rename_with(toupper)
tmp_tbl_weights_nls <- tmp_tbl_weights_nls |>
  rename_with(toupper)
t_noc_broad_categories <- t_noc_broad_categories |>
  rename_with(toupper)
occupation_distributions_stat_can <- occupation_distributions_stat_can |>
  rename_with(toupper)


# ---- Execute SQL ----

# Identify the "Universe" of graduates, grouping by key demographics.
# These records are included in the ‘base’ for the occupational distribution
# Base represents the number of respondents and can be verified by summing
# DACSO_Q006b_Weighted_New_Labour_Supply.Unweighted_Count

# dbExecute(decimal_con, DACSO_Q008_Z01_Base_OCC)
dacso_q008_z01_base_occ <- t_cohorts_recoded %>%
  filter(WEIGHT > 0, CURRENT_REGION_PSSM_CODE != -1) %>%
  group_by(
    SURVEY,
    INST_CD,
    AGE_GROUP_ROLLUP,
    TTRAIN,
    LCIP4_CRED,
    STQU_ID,
    NEW_LABOUR_SUPPLY,
    GRAD_STATUS
  ) %>%
  summarise(BASE = n(), .groups = "drop") %>%
  filter(
    !is.na(AGE_GROUP_ROLLUP),
    NEW_LABOUR_SUPPLY %in% c(1, 2, 3),
    GRAD_STATUS %in% c("1", "3")
  ) |>
  select(-GRAD_STATUS)

# calculates the Weighted NLS via Base: Count(*)*[Weight_NLS];
# dbExecute(decimal_con, DACSO_Q008_Z02a_Base)
dacso_q008_z02a_base <- t_cohorts_recoded %>%
  inner_join(t_current_region_pssm_codes, by = "CURRENT_REGION_PSSM_CODE") %>%
  inner_join(
    t_current_region_pssm_rollup_codes,
    by = "CURRENT_REGION_PSSM_CODE_ROLLUP"
  ) %>%
  filter(
    NEW_LABOUR_SUPPLY %in% c(1, 2, 3),
    RESPONDENT == "1", # RESPONDENT == 1 for all NLS in (1,2,3)
    WEIGHT > 0,
    CURRENT_REGION_PSSM_CODE_ROLLUP != 9999, # same as CURRENT_REGION_PSSM_CODE != -1
    !is.na(AGE_GROUP_ROLLUP), # age group rollups = NA have all been filtered by this points.
    GRAD_STATUS %in% c("1", "3")
  ) %>%
  group_by(
    SURVEY,
    CURRENT_REGION_PSSM_CODE_ROLLUP,
    SURVEY_YEAR,
    INST_CD,
    AGE_GROUP_ROLLUP,
    GRAD_STATUS,
    TTRAIN,
    LCIP4_CRED
  ) %>%
  summarise(COUNT = n(), BASE = n() * unique(WEIGHT_NLS), .groups = "drop")

# create a table with only respondents with either 1) valid NOCs
# or 2) where every respondent in the same demographic group + CIP has an invalid NOC.
# DACSO_Q008_Z02b_Respondents
# DACSO_Q008_Z02b_Respondents_NOC_99999
# DACSO_Q008_Z02b_Respondents_NOC_99999_100_perc
# DACSO_Q008_Z02b_Respondents_Union

dacso_q008_z02b_respondents <- t_cohorts_recoded %>%
  inner_join(t_current_region_pssm_codes, by = "CURRENT_REGION_PSSM_CODE") %>%
  inner_join(
    t_current_region_pssm_rollup_codes,
    by = "CURRENT_REGION_PSSM_CODE_ROLLUP"
  ) %>%
  mutate(NOC_CD_99999_FLAG = ifelse(NOC_CD == "99999", 1, 0)) %>%
  filter(
    !is.na(NOC_CD),
    NEW_LABOUR_SUPPLY %in% c(1, 3),
    WEIGHT > 0
  ) %>%
  group_by(
    SURVEY,
    CURRENT_REGION_PSSM_CODE_ROLLUP,
    SURVEY_YEAR,
    INST_CD,
    AGE_GROUP_ROLLUP,
    GRAD_STATUS,
    TTRAIN,
    LCIP4_CRED,
    NOC_CD_99999_FLAG
  ) %>%
  summarise(
    RESPONDENTS = sum(ifelse(
      RESPONDENT == "1" & CURRENT_REGION_PSSM_CODE != -1,
      1,
      0
    )),
    .groups = "drop"
  ) %>%
  filter(!is.na(AGE_GROUP_ROLLUP), GRAD_STATUS %in% c("1", "3"))

dacso_q008_z02b_respondents_union <-
  dacso_q008_z02b_respondents |>
  left_join(
    dacso_q008_z02a_base |> select(-TTRAIN, -BASE),
    by = join_by(
      SURVEY,
      CURRENT_REGION_PSSM_CODE_ROLLUP,
      SURVEY_YEAR,
      INST_CD,
      AGE_GROUP_ROLLUP,
      GRAD_STATUS,
      LCIP4_CRED
    )
  ) |>
  filter(
    NOC_CD_99999_FLAG == 0 | (NOC_CD_99999_FLAG == 1 & RESPONDENTS / COUNT == 1)
  ) |>
  select(-WEIGHT_NLS, -COUNT)

# calculate the weighted and weighted nls
# dbExecute(decimal_con, DACSO_Q008_Z02c_Weight)
dacso_q008_z02c_weight <- dacso_q008_z02a_base %>%
  left_join(
    dacso_q008_z02b_respondents_union |> select(-NOC_CD_99999_FLAG),
    by = c(
      "SURVEY",
      "CURRENT_REGION_PSSM_CODE_ROLLUP",
      "SURVEY_YEAR",
      "INST_CD",
      "AGE_GROUP_ROLLUP",
      "GRAD_STATUS",
      "LCIP4_CRED",
      "TTRAIN"
    )
  ) %>%
  mutate(
    WEIGHT_NLS_BASE = ifelse(
      # set weight to 0 if there are no repsondents in the union table, for this group.
      coalesce(RESPONDENTS, 0) == 0,
      1,
      BASE / RESPONDENTS
    ),
    WEIGHTED = coalesce(RESPONDENTS, 0) * WEIGHT_NLS_BASE
  ) %>%
  group_by(
    SURVEY,
    CURRENT_REGION_PSSM_CODE_ROLLUP,
    SURVEY_YEAR,
    INST_CD,
    AGE_GROUP_ROLLUP,
    GRAD_STATUS,
    TTRAIN,
    LCIP4_CRED
  ) %>%
  summarise(
    WEIGHT_NLS = unique(WEIGHT_NLS_BASE),
    BASE = unique(BASE),
    RESPONDENTS = unique(RESPONDENTS),
    WEIGHT_NLS_BASE = unique(WEIGHT_NLS_BASE),
    WEIGHTED = unique(WEIGHTED),
    .groups = "drop"
  )

# sum the newly weighted totals and original population totals for later comparison
# NOTE: we are missing year here. CHeck that this is right.
# dbExecute(decimal_con, DACSO_Q008_Z03_Weight_Total)
dacso_q008_z03_weight_total <- dacso_q008_z02c_weight %>%
  group_by(
    SURVEY,
    CURRENT_REGION_PSSM_CODE_ROLLUP,
    INST_CD,
    AGE_GROUP_ROLLUP,
    GRAD_STATUS,
    TTRAIN,
    LCIP4_CRED
  ) %>%
  summarise(BASE = sum(BASE), WEIGHTED = sum(WEIGHTED), .groups = "drop")

# dbExecute(decimal_con, DACSO_Q008_Z04_Weight_Adj_Fac)
# compare newly weighted totals and original population totals. if there is a discrepancy, it creates a Weight_Adj_Fac
dacso_q008_z04_weight_adj_fac <- dacso_q008_z03_weight_total %>%
  mutate(WEIGHT_ADJ_FAC = ifelse(WEIGHTED == 0, 0, BASE / WEIGHTED))

# GOOD TO HERE - COMPARED WITH SQL OUTPUT

# multiplies the raw weight by the adjustment factor to create the final variable: Weight_OCC.
# dbExecute(decimal_con, DACSO_Q008_Z05_Weight_OCC)
tmp_tbl_weights_occ <- dacso_q008_z02c_weight %>%
  select(-WEIGHTED) %>%
  inner_join(
    dacso_q008_z04_weight_adj_fac %>% select(-BASE, -TTRAIN),
    by = c(
      "SURVEY",
      "CURRENT_REGION_PSSM_CODE_ROLLUP",
      "INST_CD",
      "AGE_GROUP_ROLLUP",
      "GRAD_STATUS",
      "LCIP4_CRED"
    )
  ) %>%
  inner_join(
    t_current_region_pssm_rollup_codes |>
      select(
        -OLD_CURRENT_REGION_PSSM_CODE_ROLLUP,
        -CURRENT_REGION_PSSM_NAME_ROLLUP
      ),
    by = "CURRENT_REGION_PSSM_CODE_ROLLUP"
  ) %>%
  inner_join(
    t_current_region_pssm_codes |>
      select(CURRENT_REGION_PSSM_CODE_ROLLUP, CURRENT_REGION_PSSM_CODE),
    by = "CURRENT_REGION_PSSM_CODE_ROLLUP"
  ) %>%
  mutate(WEIGHT_OCC = WEIGHT_NLS_BASE * WEIGHT_ADJ_FAC) |>
  select(-WEIGHT_NLS)


# ---------- Check Distributions for Missing Dsitributions, nls/occ ratios -------
# Data Quality check - find any demographic groups that missing from the occupational weights
# dbExecute(decimal_con, DACSO_Q008_Z05b_Finding_NLS2_Missing)
tmp_tbl_weights_nls %>%
  anti_join(
    tmp_tbl_weights_occ,
    by = c("SURVEY", "INST_CD", "AGE_GROUP_ROLLUP", "GRAD_STATUS", "LCIP4_CRED")
  )

tmp_tbl_weights_occ %>%
  anti_join(
    tmp_tbl_weights_nls,
    by = c("SURVEY", "INST_CD", "AGE_GROUP_ROLLUP", "GRAD_STATUS", "LCIP4_CRED")
  )

# dbExecute(decimal_con, DACSO_Q008_Z05b_NOC4D_NLS_XTab)
# show the volume of graduate survey respondents across different occupations,
# broken down by age group and specific labor supply classifications
# not implemented as it was too complicated and I don't know why we need it

# dbExecute(decimal_con, DACSO_Q008_Z05b_Weight_Comparison)
dacso_q008_z05b_weight_comparison <- tmp_tbl_weights_nls %>%
  select(-RESPONDENTS) %>%
  inner_join(
    tmp_tbl_weights_occ |>
      select(
        WEIGHT_OCC,
        RESPONDENTS,
        SURVEY_YEAR,
        INST_CD,
        GRAD_STATUS,
        LCIP4_CRED,
        AGE_GROUP_ROLLUP
      ),
    by = c(
      "SURVEY_YEAR",
      "INST_CD",
      "GRAD_STATUS",
      "LCIP4_CRED",
      "AGE_GROUP_ROLLUP"
    )
  ) %>%
  filter(
    WEIGHT_OCC != WEIGHT_NLS,
    !is.na(RESPONDENTS)
  ) %>%
  mutate(
    RATIO = WEIGHT_OCC / WEIGHT_NLS
  ) |>
  select(
    SURVEY,
    SURVEY_YEAR,
    INST_CD,
    AGE_GROUP_ROLLUP,
    GRAD_STATUS,
    LCIP4_CRED,
    WEIGHT_NLS,
    WEIGHT_OCC,
    RESPONDENTS,
    RATIO,
    TTRAIN
  )

# ----------  Add OCC Weights to Cohort Data and Calculate Occupation Distributions -------
# dbExecute(decimal_con, DACSO_Q008_Z06_Add_Weight_OCC_Field)
t_cohorts_recoded <- t_cohorts_recoded %>%
  mutate(WEIGHT_OCC = as.numeric(NA))

# ---- DACSO_Q008_Z08_Weight_OCC_Update ----
# ---- DACSO_Q008_Z08_Weight_OCC_Update_NOC_99999_100_perc ----
w_unc <- tmp_tbl_weights_occ %>%
  inner_join(
    dacso_q008_z02b_respondents_union |> filter(NOC_CD_99999_FLAG == 1),
    by = c(
      "SURVEY",
      "CURRENT_REGION_PSSM_CODE_ROLLUP",
      "SURVEY_YEAR",
      "INST_CD",
      "AGE_GROUP_ROLLUP",
      "GRAD_STATUS",
      "LCIP4_CRED"
    )
  ) %>%
  select(
    AGE_GROUP_ROLLUP,
    INST_CD,
    SURVEY_YEAR,
    GRAD_STATUS,
    LCIP4_CRED,
    CURRENT_REGION_PSSM_CODE,
    W_UNC = WEIGHT_OCC
  )

t_cohorts_recoded <- t_cohorts_recoded %>%
  left_join(
    tmp_tbl_weights_occ |>
      select(
        CURRENT_REGION_PSSM_CODE,
        LCIP4_CRED,
        GRAD_STATUS,
        SURVEY_YEAR,
        INST_CD,
        AGE_GROUP_ROLLUP,
        W_STD = WEIGHT_OCC
      ),
    by = c(
      "CURRENT_REGION_PSSM_CODE",
      "LCIP4_CRED",
      "GRAD_STATUS",
      "SURVEY_YEAR",
      "INST_CD",
      "AGE_GROUP_ROLLUP"
    )
  ) %>%
  left_join(
    w_unc,
    by = c(
      "AGE_GROUP_ROLLUP",
      "INST_CD",
      "SURVEY_YEAR",
      "GRAD_STATUS",
      "LCIP4_CRED",
      "CURRENT_REGION_PSSM_CODE"
    )
  ) %>%
  mutate(
    WEIGHT_OCC = case_when(
      CURRENT_REGION_PSSM_CODE == -1 | is.na(NOC_CD) ~ WEIGHT_OCC,
      !(STQU_ID %in% dacso_q008_z01_base_occ$STQU_ID) |
        !(CURRENT_REGION_PSSM_CODE %in%
          t_current_region_pssm_codes$CURRENT_REGION_PSSM_CODE) ~ WEIGHT_OCC,
      NOC_CD != "99999" ~ coalesce(W_STD, WEIGHT_OCC),
      TRUE ~ coalesce(W_UNC, WEIGHT_OCC)
    )
  ) %>%
  select(-W_STD, -W_UNC)

#dbGetQuery(decimal_con, DACSO_Q008_Z09_Check_Weights)
# TO DO ^^^^^

dbExecute(decimal_con, DACSO_Q009_Weight_Occs)
dbExecute(decimal_con, DACSO_Q009_Weighted_Occs_2D)
dbExecute(decimal_con, DACSO_Q009_Weighted_Occs_2D_BC)
dbExecute(decimal_con, DACSO_Q009_Weighted_Occs_2D_BC_No_TT)
dbExecute(decimal_con, DACSO_Q009_Weighted_Occs_2D_No_TT)
dbExecute(decimal_con, DACSO_Q009_Weighted_Occs_Total_2D)
dbExecute(decimal_con, DACSO_Q009_Weighted_Occs_Total_2D_BC)
dbExecute(decimal_con, DACSO_Q009_Weighted_Occs_Total_2D_BC_No_TT)
dbExecute(decimal_con, DACSO_Q009_Weighted_Occs_Total_2D_No_TT)
dbExecute(decimal_con, DACSO_Q009b_Weighted_Occs)
dbExecute(decimal_con, DACSO_Q009b_Weighted_Occs_No_TT)
dbExecute(decimal_con, DACSO_Q009b_Weighted_Occs_Total)
dbExecute(decimal_con, DACSO_Q009b_Weighted_Occs_Total_No_TT)

dbExecute(decimal_con, DACSO_Q010_Weighted_Occs_Dist)
dbExecute(decimal_con, DACSO_Q010_Weighted_Occs_Dist_2D)
dbExecute(decimal_con, DACSO_Q010_Weighted_Occs_Dist_2D_BC)
dbExecute(decimal_con, DACSO_Q010_Weighted_Occs_Dist_2D_BC_No_TT)
dbExecute(decimal_con, DACSO_Q010_Weighted_Occs_Dist_2D_No_TT)
dbExecute(decimal_con, DACSO_Q010_Weighted_Occs_Dist_No_TT)

dbExecute(decimal_con, "DROP TABLE DACSO_Q009_Weight_Occs")
dbExecute(decimal_con, "DROP TABLE DACSO_Q009_Weighted_Occs_2D")
dbExecute(decimal_con, "DROP TABLE DACSO_Q009_Weighted_Occs_2D_BC")
dbExecute(decimal_con, "DROP TABLE DACSO_Q009_Weighted_Occs_2D_BC_No_TT")
dbExecute(decimal_con, "DROP TABLE DACSO_Q009_Weighted_Occs_2D_No_TT")
dbExecute(decimal_con, "DROP TABLE DACSO_Q009_Weighted_Occs_Total_2D")
dbExecute(decimal_con, "DROP TABLE DACSO_Q009_Weighted_Occs_Total_2D_BC")
dbExecute(decimal_con, "DROP TABLE DACSO_Q009_Weighted_Occs_Total_2D_BC_No_TT")
dbExecute(decimal_con, "DROP TABLE DACSO_Q009_Weighted_Occs_Total_2D_No_TT")
dbExecute(decimal_con, "DROP TABLE DACSO_Q009b_Weighted_Occs")
dbExecute(decimal_con, "DROP TABLE dacso_q009b_weighted_occs_no_tt")
dbExecute(decimal_con, "DROP TABLE DACSO_Q009b_Weighted_Occs_Total")
dbExecute(decimal_con, "DROP TABLE dacso_q009b_weighted_occs_total_no_tt")

occs_def <- c(
  Survey = "nvarchar(50)",
  PSSM_Credential = "nvarchar(50)",
  PSSM_CRED = "nvarchar(50)",
  LCP4_CD = "nvarchar(50)",
  TTRAIN = "nvarchar(50)",
  LCIP4_CRED = "nvarchar(50)",
  LCIP2_CRED = "nvarchar(50)",
  NOC = "nvarchar(50)",
  Current_Region_PSSM_Code_Rollup = "integer",
  Age_Group_Rollup = "integer",
  Count = "float",
  Total = "float",
  Percent = "float"
)


if (
  !dbExistsTable(
    decimal_con,
    SQL(glue::glue('"{my_schema}"."Occupation_Distributions"'))
  )
) {
  dbCreateTable(
    decimal_con,
    SQL(glue::glue('"{my_schema}"."Occupation_Distributions"')),
    occs_def
  )
}
if (
  !dbExistsTable(
    decimal_con,
    SQL(glue::glue('"{my_schema}"."Occupation_Distributions_No_TT"'))
  )
) {
  dbCreateTable(
    decimal_con,
    SQL(glue::glue('"{my_schema}"."Occupation_Distributions_No_TT"')),
    occs_def
  )
}

dbExecute(decimal_con, DACSO_Q010a0_Delete_Occupational_Distribution)
dbExecute(decimal_con, DACSO_Q010a0_Delete_Occupational_Distribution_No_TT)
#dbExecute(decimal_con, DACSO_Q010a0_Delete_Occupational_Distribution_No_TT_QI)
#dbExecute(decimal_con, DACSO_Q010a0_Delete_Occupational_Distribution_QI)
dbExecute(decimal_con, DACSO_Q010a1_Append_Occupational_Distribution)
dbExecute(decimal_con, DACSO_Q010a1_Append_Occupational_Distribution_No_TT)

occs_def <- c(
  Survey = "nvarchar(50)",
  PSSM_Credential = "nvarchar(50)",
  PSSM_CRED = "nvarchar(50)",
  LCP2_CD = "nvarchar(50)",
  TTRAIN = "nvarchar(50)",
  LCIP2_CRED = "nvarchar(50)",
  NOC = "nvarchar(50)",
  Current_Region_PSSM_Code_Rollup = "integer",
  Age_Group_Rollup = "integer",
  Count = "float",
  Total = "float",
  Percent = "float"
)


if (
  !dbExistsTable(
    decimal_con,
    SQL(glue::glue('"{my_schema}"."Occupation_Distributions_LCP2"'))
  )
) {
  dbCreateTable(
    decimal_con,
    SQL(glue::glue('"{my_schema}"."Occupation_Distributions_LCP2"')),
    occs_def
  )
}
if (
  !dbExistsTable(
    decimal_con,
    SQL(glue::glue('"{my_schema}"."Occupation_Distributions_LCP2_No_TT"'))
  )
) {
  dbCreateTable(
    decimal_con,
    SQL(glue::glue('"{my_schema}"."Occupation_Distributions_LCP2_No_TT"')),
    occs_def
  )
}

dbExecute(decimal_con, DACSO_Q010b0_Delete_Occupational_Distribution_LCP2)
dbExecute(decimal_con, DACSO_Q010b0_Delete_Occupational_Distribution_LCP2_No_TT)
# dbExecute(decimal_con, DACSO_Q010b0_Delete_Occupational_Distribution_LCP2_No_TT_QI)
# dbExecute(decimal_con, DACSO_Q010b0_Delete_Occupational_Distribution_LCP2_QI)
dbExecute(decimal_con, DACSO_Q010b1_Append_Occupational_Distribution_LCP2)
dbExecute(decimal_con, DACSO_Q010b1_Append_Occupational_Distribution_LCP2_No_TT)

occs_def <- c(
  Survey = "nvarchar(50)",
  PSSM_Credential = "nvarchar(50)",
  PSSM_CRED = "nvarchar(50)",
  LCP2_CD = "nvarchar(50)",
  TTRAIN = "nvarchar(50)",
  LCIP2_CRED = "nvarchar(50)",
  NOC = "nvarchar(50)",
  Count = "float",
  Total = "float",
  Percent = "float"
)

if (
  !dbExistsTable(
    decimal_con,
    SQL(glue::glue('"{my_schema}"."Occupation_Distributions_LCP2_BC"'))
  )
) {
  dbCreateTable(
    decimal_con,
    SQL(glue::glue('"{my_schema}"."Occupation_Distributions_LCP2_BC"')),
    occs_def
  )
}

if (
  !dbExistsTable(
    decimal_con,
    SQL(glue::glue('"{my_schema}"."Occupation_Distributions_LCP2_BC_No_TT"'))
  )
) {
  dbCreateTable(decimal_con, "Occupation_Distributions_LCP2_BC_No_TT", occs_def)
}

dbExecute(decimal_con, DACSO_Q010c0_Delete_Occupational_Distribution_LCP2_BC)
dbExecute(
  decimal_con,
  DACSO_Q010c0_Delete_Occupational_Distribution_LCP2_BC_No_TT
)
# dbExecute(decimal_con, DACSO_Q010c0_Delete_Occupational_Distribution_LCP2_BC_No_TT_QI)
# dbExecute(decimal_con, DACSO_Q010c0_Delete_Occupational_Distribution_LCP2_BC_QI)
dbExecute(decimal_con, DACSO_Q010c1_Append_Occupational_Distribution_LCP2_BC)
dbExecute(
  decimal_con,
  DACSO_Q010c1_Append_Occupational_Distribution_LCP2_BC_No_TT
)

dbExecute(decimal_con, "DROP TABLE DACSO_Q010_Weighted_Occs_Dist_2D_BC_No_TT")
dbExecute(decimal_con, "DROP TABLE dacso_q010_weighted_occs_dist_2d_no_tt")
dbExecute(decimal_con, "DROP TABLE DACSO_Q010_Weighted_Occs_Dist_No_TT")
dbExecute(decimal_con, "DROP TABLE DACSO_Q010_Weighted_Occs_Dist")
dbExecute(decimal_con, "DROP TABLE DACSO_Q010_Weighted_Occs_Dist_2D")
dbExecute(decimal_con, "DROP TABLE DACSO_Q010_Weighted_Occs_Dist_2D_BC")

dbExecute(
  decimal_con,
  DACSO_Q010d1_Delete_PDEG_CIP_Cluster_07_Law_New_Labour_Supply
)
#dbExecute(decimal_con, DACSO_Q010d1_Delete_PDEG_CIP_Cluster_07_Law_New_Labour_Supply_QI)
dbExecute(decimal_con, DACSO_Q010d2_NLS_PDEG_07_Count)
dbExecute(decimal_con, DACSO_Q010d3_NLS_PDEG_07_Subtotal)
dbExecute(decimal_con, DACSO_Q010d4_NLS_PDEG_07_Total)
dbExecute(decimal_con, DACSO_Q010d5_NLS_PDEG_07_Weighted_New_Labour_Supply)
dbExecute(decimal_con, DACSO_Q010d6_Append_NLS_PDEG_07_New_Labour_Supply)

dbExecute(
  decimal_con,
  "DROP TABLE DACSO_Q010d5_NLS_PDEG_07_Weighted_New_Labour_Supply"
)
dbExecute(decimal_con, "DROP TABLE DACSO_Q010d2_NLS_PDEG_07_Count")
dbExecute(decimal_con, "DROP TABLE DACSO_Q010d3_NLS_PDEG_07_Subtotal")
dbExecute(decimal_con, "DROP TABLE DACSO_Q010d4_NLS_PDEG_07_Total")

dbExecute(
  decimal_con,
  DACSO_Q010e1_Delete_PDEG_CIP_Cluster_07_Law_Occupation_Dist
)
#dbExecute(decimal_con, DACSO_Q010e1_Delete_PDEG_CIP_Cluster_07_Law_Occupation_Dist_QI)
dbExecute(decimal_con, DACSO_Q010e2_Weighted_Occs_PDEG_07)
dbExecute(decimal_con, DACSO_Q010e3_Weighted_Occs_Total_PDEG_07)
dbExecute(decimal_con, DACSO_Q010e4_Weighted_Occs_Dist_PDEG_07)
dbExecute(decimal_con, DACSO_Q010e5_Append_Occupational_Distribution_PDEG_07)

dbExecute(decimal_con, "DROP TABLE DACSO_Q010e2_Weighted_Occs_PDEG_07")
dbExecute(decimal_con, "DROP TABLE DACSO_Q010e3_Weighted_Occs_Total_PDEG_07")
dbExecute(decimal_con, "DROP TABLE DACSO_Q010e4_Weighted_Occs_Dist_PDEG_07")

dbExecute(decimal_con, DACSO_Q99A_ENDDT_IMPUTED)

if (qi_run == TRUE | ptib_run == TRUE) {
  # do  nothing
}
if (regular_run == TRUE) {
  tryCatch(
    {
      dbExecute(decimal_con, DACSO_qry99_Suppression_Public_Release_NOC)
    },
    error = function(e) {
      print(paste("Error:", e$message))
      stop()
    }
  )
}

dbExecute(
  decimal_con,
  "DELETE 
                        FROM Occupation_Distributions
                        WHERE (((Occupation_Distributions.Survey)='2021 Census PSSM 2023-2024'));"
)

# uncomment if running for the first time
# dbExecute(decimal_con, "ALTER TABLE Occupation_Distributions_Stat_Can ADD TTRAIN NVARCHAR(50)")
# dbExecute(decimal_con, "ALTER TABLE Occupation_Distributions_Stat_Can ADD LCIP2_CRED NVARCHAR(50)")
dbExecute(
  decimal_con,
  "INSERT INTO Occupation_Distributions ([Survey]
      ,[PSSM_Credential]
      ,[PSSM_CRED]
      ,[LCP4_CD]
      ,[TTRAIN]
      ,[LCIP4_CRED]
      ,[LCIP2_CRED]
      ,[NOC]
      ,[Current_Region_PSSM_Code_Rollup]
      ,[Age_Group_Rollup]
      ,[Count]
      ,[Total]
      ,[Percent])
          SELECT '2021 Census PSSM 2023-2024' as Survey
      ,[PSSM_Credential]
      ,[PSSM_CRED]
      ,[LCP4_CD]
      ,[TTRAIN]
      ,[LCIP4_CRED]
      ,[LCIP2_CRED]
      ,[NOC]
      ,[Current_Region_PSSM_Code_Rollup]
      ,[Age_Group_Rollup]
      ,[Count]
      ,[Total]
      ,[Percent] FROM Occupation_Distributions_Stat_Can"
)

# ---- Clean Up ----
dbExecute(decimal_con, "DROP TABLE tmp_tbl_Weights_OCC")
dbExecute(decimal_con, "DROP TABLE tmp_tbl_Weights_NLS")
dbExecute(decimal_con, "DROP TABLE tbl_Age_Groups")
dbExecute(decimal_con, "DROP TABLE tbl_Age_Groups_Rollup")
dbExecute(decimal_con, "DROP TABLE tbl_Age")
dbExecute(decimal_con, "DROP TABLE T_PSSM_Credential_Grouping")
dbExecute(decimal_con, "DROP TABLE T_Weights")
dbExecute(decimal_con, "DROP TABLE t_year_survey_year")
dbExecute(decimal_con, "DROP TABLE t_current_region_pssm_codes")
dbExecute(decimal_con, "DROP TABLE t_current_region_pssm_rollup_codes")
dbExecute(decimal_con, "DROP TABLE t_current_region_pssm_rollup_codes_bc")

# ---- Keep ----
dbExistsTable(decimal_con, "Occupation_Distributions")
dbExistsTable(decimal_con, "Occupation_Distributions_No_TT")
dbExistsTable(decimal_con, "Occupation_Distributions_LCP2")
dbExistsTable(decimal_con, "Occupation_Distributions_LCP2_No_TT")
dbExistsTable(decimal_con, "Occupation_Distributions_LCP2_BC")
dbExistsTable(decimal_con, "Occupation_Distributions_LCP2_BC_No_TT")


dbDisconnect(decimal_con)
