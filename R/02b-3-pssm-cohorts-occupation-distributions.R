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

library(RODBC)
library(DBI)
library(config)
#
#
# # ---- Configure LAN and file paths ----
db_config <- config::get("decimal")
lan <- config::get("lan")
my_schema <- config::get("myschema")
#
# ----Connection to database ----
db_config <- config::get("decimal")
decimal_con <- dbConnect(
  odbc::odbc(),
  Driver = db_config$driver,
  Server = db_config$server,
  Database = db_config$database,
  Trusted_Connection = "True"
)

occupation_distributions_stat_can <- dbReadTable(
  decimal_con,
  SQL(glue::glue('"Occupation_Distributions_Stat_Can"'))
)

#dbDisconnect(decimal_con)

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

# calculates the Weighted NLS: Base: Count(*)*[Weight_NLS];
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
    LCIP4_CRED,
    WEIGHT_NLS # this is just to keep the variable
  ) %>%
  summarise(COUNT = n(), BASE = n() * unique(WEIGHT_NLS), .groups = "drop")

# original queries first created a table with only respondents with a NOC and NLS=1,3;
# then split the table into 2: NOC_CD=9999 (Unknown) and NOC_CD != 9999 (Known).
# For Unknowns, kept only those demographic group where every respondent in thad an 9999 NOC.
# The two tables were unioned back together.
# DACSO_Q008_Z02b_Respondents
# DACSO_Q008_Z02b_Respondents_NOC_99999
# DACSO_Q008_Z02b_Respondents_NOC_99999_100_perc
# DACSO_Q008_Z02b_Respondents_Union
# I've done this in two steps: 1) create a table of respondents,
# then 2) filter to those with a known NOC, or where all respondents in the demographic group are unknowns.
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
    dacso_q008_z02a_base |> select(-TTRAIN, -BASE, -WEIGHT_NLS),
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
  select(-COUNT)

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
      # set weight to 1 if strata was excluded from the union table.
      # if_else(is.na) would make more sense?
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
    WEIGHT_NLS = unique(WEIGHT_NLS), # original NLS weight from 02b derived in 02b-2.
    BASE = unique(BASE), # original NLS-weighted count
    RESPONDENTS = unique(RESPONDENTS), # original count of respondents, or NA if not in the union table
    WEIGHT_NLS_BASE = unique(WEIGHT_NLS_BASE), # derived in this query: BASE / RESPONDENTS, or 1 if not in the union table
    WEIGHTED = unique(WEIGHTED), # derived in this query: RESPONDENTS * WEIGHT_NLS_BASE, or 0 if not in the union table.
    .groups = "drop"
  )

# sum the newly weighted totals and original population totals for later comparison
# NOTE: we are missing year here. Check that this is right.
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
  summarise(
    BASE = sum(BASE), # remove survey year for and sum the original NLS_weighted counts across strata
    WEIGHTED = sum(WEIGHTED), # adjusted weighted count after accounting for only respondents with NOC or where all respondents in group are unknowns.
    .groups = "drop"
  )

# dbExecute(decimal_con, DACSO_Q008_Z04_Weight_Adj_Fac)
# calculate the adjustment factor required for the weighted counts to equal the base
dacso_q008_z04_weight_adj_fac <- dacso_q008_z03_weight_total %>%
  mutate(WEIGHT_ADJ_FAC = ifelse(WEIGHTED == 0, 0, BASE / WEIGHTED))


# applies the adjustment factor to raw weight (Weight_OCC: [Weight_NLS_Base]*[Weight_Adj_Fac])
# remember we are weighting up NLS=1,2,3 as the probability and year weights are already incorporated in the NLS weights
# creates a temporary table to hold the weights

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


# show the volume of graduate survey respondents across different occupations,
# broken down by age group and specific labor supply classifications
# not implemented as it was too complicated and I don't know why we need it.
# dbExecute(decimal_con, DACSO_Q008_Z05b_NOC4D_NLS_XTab)

# dbExecute(decimal_con, DACSO_Q008_Z05b_Weight_Comparison)
# look at the ratio between the nls and occ weights
# examine extreme values of 20+ for Weight_OCC
# from 2019 notes - these differences are as a result of dropping the records in the above queries (unknown region and NOC)
# but I am concerned that there are so many that are the same (i.e. dropping unknown region and NOC's affected far fewer groups
# than in 2019 Access database - we should investigate why.
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

# DACSO_Q008_Z08_Weight_OCC_Update
# DACSO_Q008_Z08_Weight_OCC_Update_NOC_99999_100_perc

# grab only those records where the entire CIP is 100% NOC 9999
w_100p_9999 <- tmp_tbl_weights_occ %>%
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
    W_100P_9999 = WEIGHT_OCC
  )

# the case_when at the bottom is pretty confusing but it is basically
# updating weight_occ following these rules:
# where unknown PSSM region or NOC is null or 9999: keep original weight_occ
# where STQU_ID is not in the original occ_base (NLS = 1,2,3 and GradStatus = 1,3 and a few other conditions) or region code is not valid: keep original weight_occ
# where NOC_CD != "99999", keep either W_STD or original weight_occ
# otherwise update weight_occ to W_100P_9999, which is the weight where the entire CIP is 100% NOC 9999
# the logic is strange though since
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
        W_TMP_TBL = WEIGHT_OCC
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
    w_100p_9999,
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
      is.na(NOC_CD) |
        !(STQU_ID %in% dacso_q008_z01_base_occ$STQU_ID) ~ WEIGHT_OCC, # this will be NA, as all WEIGHT_OCC are na
      NOC_CD != "99999" ~ W_TMP_TBL, # all WEIGHT_OCC are na - can just set to W_STD
      TRUE ~ W_100P_9999 # all WEIGHT_OCC are na - can just set to W_100P_9999
    )
  ) %>%
  select(-W_TMP_TBL, -W_100P_9999)

# didn't implement this as it is just a check.
#dbGetQuery(decimal_con, DACSO_Q008_Z09_Check_Weights)

# ----- Create Occupation Distributions -----
#dbExecute(decimal_con, DACSO_Q009_Weight_Occs)
dacso_q009_weight_occs <- t_cohorts_recoded |>
  inner_join(
    t_current_region_pssm_codes |>
      inner_join(
        t_current_region_pssm_rollup_codes |>
          select(CURRENT_REGION_PSSM_CODE_ROLLUP),
        by = c(
          "CURRENT_REGION_PSSM_CODE_ROLLUP" = "CURRENT_REGION_PSSM_CODE_ROLLUP"
        )
      ),
    by = c("CURRENT_REGION_PSSM_CODE" = "CURRENT_REGION_PSSM_CODE")
  ) |>
  filter(
    NEW_LABOUR_SUPPLY %in% c(1, 3),
    WEIGHT > 0
  ) |>
  mutate(
    NOC_CD = if_else(NOC_CD == "XXXXX", "99999", NOC_CD)
  ) |>
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
    NOC_CD,
    WEIGHT_OCC
  ) |>
  summarize(
    COUNT = n(),
    WEIGHT_OCC = first(WEIGHT_OCC),
    WEIGHTED = n() * first(WEIGHT_OCC),
    .groups = "drop"
  ) |>
  mutate(
    IN_BC = grepl("^59", CURRENT_REGION_PSSM_CODE_ROLLUP),
    LCP2_CD = str_sub(LCP4_CD, 1, 2)
  ) |>
  filter(
    CURRENT_REGION_PSSM_CODE_ROLLUP != 99999,
    !is.na(AGE_GROUP_ROLLUP),
    GRAD_STATUS %in% c("1", "3"),
    !is.na(NOC_CD),
    !is.na(WEIGHT_OCC)
  )

# I collapsed the following 3 queries into one for readability.
#  - dbExecute(decimal_con, DACSO_Q009_Weighted_Occs_2D)
#  - dbExecute(decimal_con, DACSO_Q009_Weighted_Occs_Total_2D)
#  - dbExecute(decimal_con, DACSO_Q010_Weighted_Occs_Dist_2D)
group_vars <- c(
  "PSSM_CREDENTIAL",
  "PSSM_CRED",
  "CURRENT_REGION_PSSM_CODE_ROLLUP",
  "AGE_GROUP_ROLLUP",
  "LCP2_CD",
  "TTRAIN",
  "LCIP2_CRED"
)

dacso_q010_weighted_occs_dist <-
  dacso_q009_weight_occs |>
  group_by(across(all_of(group_vars))) |>
  summarize(
    TOTAL = sum(WEIGHTED, na.rm = TRUE),
    .groups = "drop"
  ) |>
  left_join(
    dacso_q009_weight_occs |>
      group_by(across(c(all_of(group_vars), "NOC_CD"))) |>
      summarize(
        COUNT = sum(WEIGHTED, na.rm = TRUE),
        .groups = "drop"
      ),
    by = group_vars
  ) |>
  mutate(PERC_DIST = COUNT / TOTAL)

compare(
  "dacso_q009_weighted_occs_total_2d",
  dacso_q009_weighted_occs_total_2d
)

# I collapsed the following 3 queries into one for readability.
#  - dbExecute(decimal_con, DACSO_Q009_Weighted_Occs_2D_BC)
#  - dbExecute(decimal_con, DACSO_Q009_Weighted_Occs_Total_2D_BC)
#  - dbExecute(decimal_con, DACSO_Q010_Weighted_Occs_Dist_2D_BC)
group_vars <- c(
  "PSSM_CREDENTIAL",
  "PSSM_CRED",
  "LCP2_CD",
  "TTRAIN",
  "LCIP2_CRED"
)


dacso_q010_weighted_occs_dist_2d_bc <- dacso_q009_weight_occs |>
  filter(IN_BC == TRUE) |>
  group_by(across(all_of(group_vars))) |>
  summarize(
    TOTAL = sum(WEIGHTED, na.rm = TRUE),
    .groups = "drop"
  ) |>
  left_join(
    dacso_q009_weight_occs |>
      filter(IN_BC) |>
      group_by(across(c(all_of(group_vars), "NOC_CD"))) |>
      summarize(
        COUNT = sum(WEIGHTED, na.rm = TRUE),
        .groups = "drop"
      ),
    by = group_vars
  ) |>
  mutate(PERC_DIST = COUNT / TOTAL)

compare(
  "dacso_q010_weighted_occs_dist_2d_bc",
  dacso_q010_weighted_occs_dist_2d_bc
)

# I collapsed the following 3 queries into one for readability.
# - dbExecute(decimal_con, DACSO_Q009_Weighted_Occs_2D_BC_No_TT)
# - dbExecute(decimal_con, DACSO_Q009_Weighted_Occs_Total_2D_BC_No_TT)
# - dbExecute(decimal_con, DACSO_Q010_Weighted_Occs_Dist_2D_BC_No_TT)
group_vars <- c(
  "PSSM_CREDENTIAL",
  "PSSM_CRED",
  "LCP2_CD",
  "LCIP2_CRED"
)

dacso_q010_weighted_occs_dist_2d_bc_no_tt <- dacso_q009_weight_occs |>
  filter(IN_BC == TRUE) |>
  mutate(LCIP2_CRED = remove_ttrain(LCIP2_CRED)) |>
  group_by(across(all_of(group_vars))) |>
  summarize(
    TOTAL = sum(WEIGHTED, na.rm = TRUE),
    .groups = "drop"
  ) |>
  left_join(
    dacso_q009_weight_occs |>
      filter(IN_BC == TRUE) |>
      mutate(LCIP2_CRED = remove_ttrain(LCIP2_CRED)) |>
      group_by(across(c(all_of(group_vars), "NOC_CD"))) |>
      summarize(
        COUNT = sum(WEIGHTED, na.rm = TRUE),
        .groups = "drop"
      ),
    by = group_vars
  ) |>
  mutate(PERC_DIST = COUNT / TOTAL)

compare(
  "dacso_q010_weighted_occs_dist_2d_bc_no_tt",
  dacso_q010_weighted_occs_dist_2d_bc_no_tt
)


# I collapsed the following 3 queries into one for readability.
#  - dbExecute(decimal_con, DACSO_Q009_Weighted_Occs_2D_No_TT)
#  - dbExecute(decimal_con, DACSO_Q009_Weighted_Occs_Total_2D_No_TT)
#  - dbExecute(decimal_con, DACSO_Q010_Weighted_Occs_Dist_2D_No_TT)
group_vars <- c(
  "PSSM_CREDENTIAL",
  "PSSM_CRED",
  "CURRENT_REGION_PSSM_CODE_ROLLUP",
  "AGE_GROUP_ROLLUP",
  "LCP2_CD",
  "LCIP2_CRED"
)

dacso_q010_weighted_occs_dist_2d_no_tt <- dacso_q009_weight_occs |>
  mutate(LCIP2_CRED = remove_ttrain(LCIP2_CRED)) |>
  group_by(across(all_of(group_vars))) |>
  summarize(
    TOTAL = sum(WEIGHTED, na.rm = TRUE),
    .groups = "drop"
  ) |>
  left_join(
    dacso_q009_weight_occs |>
      mutate(LCIP2_CRED = remove_ttrain(LCIP2_CRED)) |>
      group_by(across(c(all_of(group_vars), "NOC_CD"))) |>
      summarize(
        COUNT = sum(WEIGHTED, na.rm = TRUE),
        .groups = "drop"
      ),
    by = group_vars
  ) |>
  mutate(PERC_DIST = COUNT / TOTAL)

compare(
  "dacso_q010_weighted_occs_dist_2d_no_tt",
  dacso_q010_weighted_occs_dist_2d_no_tt
)

# I collapsed the following 3 queries into one for readability.
# - dbExecute(decimal_con, DACSO_Q009b_Weighted_Occs)
# - dbExecute(decimal_con, DACSO_Q009b_Weighted_Occs_Total)
# - dbExecute(decimal_con, DACSO_Q010_Weighted_Occs_Dist)
group_vars <- c(
  "PSSM_CREDENTIAL",
  "PSSM_CRED",
  "CURRENT_REGION_PSSM_CODE_ROLLUP",
  "AGE_GROUP_ROLLUP",
  "LCP4_CD",
  "TTRAIN",
  "LCIP4_CRED",
  "LCIP2_CRED"
)
dacso_q010_weighted_occs_dist <- dacso_q009_weight_occs |>
  group_by(across(c(all_of(group_vars), "NOC_CD"))) |>
  summarize(
    COUNT = sum(WEIGHTED, na.rm = TRUE),
    .groups = "drop"
  ) |>
  left_join(
    dacso_q009_weight_occs |>
      group_by(across(all_of(group_vars))) |>
      summarize(
        TOTAL = sum(WEIGHTED, na.rm = TRUE),
        .groups = "drop"
      ),
    by = group_vars
  ) |>
  mutate(
    PERC_DIST = COUNT / TOTAL
  )

compare("dacso_q010_weighted_occs_dist", dacso_q010_weighted_occs_dist)


# I collapsed the following 3 queries into one for readability.
# - dbExecute(decimal_con, DACSO_Q009b_Weighted_Occs_No_TT)
# - dbExecute(decimal_con, DACSO_Q009b_Weighted_Occs_Total_No_TT)
# - dbExecute(decimal_con, DACSO_Q010_Weighted_Occs_Dist_No_TT)
group_vars <- c(
  "PSSM_CREDENTIAL",
  "PSSM_CRED",
  "CURRENT_REGION_PSSM_CODE_ROLLUP",
  "AGE_GROUP_ROLLUP",
  "LCP4_CD",
  "LCIP4_CRED",
  "LCIP2_CRED"
)

dacso_q009b_weighted_occs_no_tt <- dacso_q009_weight_occs |>
  mutate(
    LCIP4_CRED = make_lcip_cred(GRAD_STATUS, LCP4_CD, PSSM_CREDENTIAL),
    LCIP2_CRED = make_lcip_cred(GRAD_STATUS, LCP2_CD, PSSM_CREDENTIAL)
  ) |>
  group_by(across(c(all_of(group_vars), "NOC_CD"))) |>
  summarize(
    COUNT = sum(WEIGHTED, na.rm = TRUE),
    .groups = "drop"
  )

dacso_q009b_weighted_occs_total_no_tt <- dacso_q009_weight_occs |>
  mutate(
    LCIP4_CRED = make_lcip_cred(GRAD_STATUS, LCP4_CD, PSSM_CREDENTIAL),
    LCIP2_CRED = make_lcip_cred(GRAD_STATUS, LCP2_CD, PSSM_CREDENTIAL)
  ) |>
  group_by(across(c(all_of(group_vars)))) |>
  summarize(
    TOTAL = sum(WEIGHTED, na.rm = TRUE),
    .groups = "drop"
  )

# not working as expected...
dacso_q010_weighted_occs_dist_no_tt <- dacso_q009b_weighted_occs_total_no_tt |>
  left_join(
    dacso_q009b_weighted_occs_no_tt,
    by = c(group_vars)
  ) |>
  mutate(
    PERC_DIST = COUNT / TOTAL
  )

compare(
  "dacso_q010_weighted_occs_dist_no_tt",
  dacso_q010_weighted_occs_dist_no_tt
)


# ----- Create Final Occupation Distribution Tables -----
# Noting that the original SQL have seperate queries to clear occupation distributions
# for qi run.  here, we are assuming that qi run weights are handled in the 02b-1 script.

# dbExecute(decimal_con, DACSO_Q010a1_Append_Occupational_Distribution)
occupation_distributions <- dacso_q010_weighted_occs_dist |>
  transmute(
    Survey = "Student Outcomes",
    PSSM_Credential = PSSM_CREDENTIAL,
    PSSM_CRED = PSSM_CRED,
    LCP4_CD,
    TTRAIN,
    LCIP4_CRED,
    LCIP2_CRED,
    NOC = NOC_CD,
    Current_Region_PSSM_Code_Rollup = CURRENT_REGION_PSSM_CODE_ROLLUP,
    Age_Group_Rollup = AGE_GROUP_ROLLUP,
    Count = COUNT,
    Total = TOTAL,
    Percent = PERC_DIST
  )

# dbExecute(decimal_con, DACSO_Q010a1_Append_Occupational_Distribution_No_TT)
occupation_distributions_no_tt <- dacso_q010_weighted_occs_dist_no_tt |>
  transmute(
    Survey = "Student Outcomes",
    PSSM_Credential = PSSM_CREDENTIAL,
    PSSM_CRED = PSSM_CRED,
    LCP4_CD,
    LCIP4_CRED,
    LCIP2_CRED,
    NOC = NOC_CD,
    Current_Region_PSSM_Code_Rollup = CURRENT_REGION_PSSM_CODE_ROLLUP,
    Age_Group_Rollup = AGE_GROUP_ROLLUP,
    Count = COUNT,
    Total = TOTAL,
    Percent = PERC_DIST
  )

# dbExecute(decimal_con, DACSO_Q010b1_Append_Occupational_Distribution_LCP2)
occupation_distributions_lcp2 <- dacso_q010_weighted_occs_dist |>
  transmute(
    Survey = "Student Outcomes",
    PSSM_Credential = PSSM_CREDENTIAL,
    PSSM_CRED = PSSM_CRED,
    LCP2_CD,
    TTRAIN,
    LCIP2_CRED,
    NOC = NOC_CD,
    Current_Region_PSSM_Code_Rollup = CURRENT_REGION_PSSM_CODE_ROLLUP,
    Age_Group_Rollup = AGE_GROUP_ROLLUP,
    Count = COUNT,
    Total = TOTAL,
    Percent = PERC_DIST
  )

# dbExecute(decimal_con, DACSO_Q010b1_Append_Occupational_Distribution_LCP2_No_TT)
occupation_distributions_lcp2_no_tt <- dacso_q010_weighted_occs_dist_2D_no_tt |>
  transmute(
    Survey = "Student Outcomes",
    PSSM_Credential = PSSM_CREDENTIAL,
    PSSM_CRED = PSSM_CRED,
    LCP2_CD,
    LCIP2_CRED,
    NOC = NOC_CD,
    Current_Region_PSSM_Code_Rollup = CURRENT_REGION_PSSM_CODE_ROLLUP,
    Age_Group_Rollup = AGE_GROUP_ROLLUP,
    Count = COUNT,
    Total = TOTAL,
    Percent = PERC_DIST
  )

# dbExecute(decimal_con, DACSO_Q010c1_Append_Occupational_Distribution_LCP2_BC)
occupation_distributions_lcp2_bc <- dacso_q010_weighted_occs_dist_2d_bc |>
  transmute(
    Survey = "Student Outcomes",
    PSSM_Credential = PSSM_CREDENTIAL,
    PSSM_CRED = PSSM_CRED,
    LCP2_CD = LCP2_CD,
    TTRAIN = TTRAIN,
    LCIP2_CRED = LCIP2_CRED,
    NOC = NOC_CD,
    Count = COUNT,
    Total = TOTAL,
    Percent = PERC_DIST
  )


# ------  create distribution for pdeg/law
# as with the other distributions, double check that the logic for a qi run makes sense.
# our design should mean we don't need to account for a seperate qi weight/distributions because
# qi weight is abstracted out in the 02b1 script.  The weight in this code should represent either qi or non-qi run.
# we shouldn't need to run a seperate query like this:
#dbExecute(decimal_con, DACSO_Q010d1_Delete_PDEG_CIP_Cluster_07_Law_New_Labour_Supply_QI)

dbExecute(decimal_con, DACSO_Q010d2_NLS_PDEG_07_Count)
dbExecute(decimal_con, DACSO_Q010d3_NLS_PDEG_07_Subtotal)
dbExecute(decimal_con, DACSO_Q010d4_NLS_PDEG_07_Total)
dbExecute(decimal_con, DACSO_Q010d5_NLS_PDEG_07_Weighted_New_Labour_Supply)
dbExecute(decimal_con, DACSO_Q010d6_Append_NLS_PDEG_07_New_Labour_Supply)
dbExecute(
  decimal_con,
  DACSO_Q010e1_Delete_PDEG_CIP_Cluster_07_Law_Occupation_Dist
)
#dbExecute(decimal_con, DACSO_Q010e1_Delete_PDEG_CIP_Cluster_07_Law_Occupation_Dist_QI)
dbExecute(decimal_con, DACSO_Q010e2_Weighted_Occs_PDEG_07)
dbExecute(decimal_con, DACSO_Q010e3_Weighted_Occs_Total_PDEG_07)
dbExecute(decimal_con, DACSO_Q010e4_Weighted_Occs_Dist_PDEG_07)
dbExecute(decimal_con, DACSO_Q010e5_Append_Occupational_Distribution_PDEG_07)

# ------  extra stuff that I am not sure where it fits in yet but want to make sure I don't lose it.
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

# ---- include post-bach degrees from stats can data
# leaving here becuase it's where this was run in the original SQL.
# but we should check that it makes sense to include here, as opposed to above.
# don't think it should matter but noting for self anyways.
occupation_distributions <- occupation_distributions |>
  rbind(
    occupation_distributions_stat_can |>
      transmute(
        Survey = "2021 Census PSSM 2023-2024",
        PSSM_Credential,
        PSSM_CRED,
        LCP4_CD,
        TTRAIN,
        LCIP4_CRED,
        LCIP2_CRED,
        NOC,
        Current_Region_PSSM_Code_Rollup,
        Age_Group_Rollup,
        Count,
        Total,
        Percent
      )
  )

# ---- Clean Up ----

# ---- Keep ----
tables_to_keep <- c(
  "occupation_distributions",
  "occupation_distributions_no_tt",
  "occupation_distributions_lcp2",
  "occupation_distributions_lcp2_no_tt",
  "occupation_distributions_lcp2_bc",
  "occupation_distributions_lcp2_bc_no_tt"
)
rm(list = setdiff(ls(), tables_to_keep))

dbDisconnect(decimal_con)
