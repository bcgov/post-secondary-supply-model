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

library(tidyverse)
library(glue)
library(assertthat)
library(RODBC)
library(DBI)
library(config)

# ---- Helper Functions ----
remove_ttrain <- function(x) {
  purrr::map_chr(x, function(str) {
    parts <- strsplit(str, " - ", fixed = TRUE)[[1]]
    if (length(parts) == 4) {
      paste(c(parts[1:2], parts[4:length(parts)]), collapse = " - ")
    } else {
      str
    }
  })
}

make_lcip_cred <- function(grad_status, lcp_cd, pssm_credential) {
  if_else(
    is.na(grad_status),
    NA_character_,
    str_c(as.character(grad_status), " - ", lcp_cd, " - ", pssm_credential)
  )
}

# ---- Configure LAN and file paths ----
db_config <- config::get("decimal")
lan <- config::get("lan")
my_schema <- config::get("myschema")

# ----Connection to database ----
db_config <- config::get("decimal")
con <- dbConnect(
  odbc::odbc(),
  Driver = db_config$driver,
  Server = db_config$server,
  Database = db_config$database,
  Trusted_Connection = "True"
)

# -------------------------- Required Tables -----------------------------------------
# move this into load scripts?
occupation_distributions_stat_can <- dbReadTable(
  con,
  SQL(glue::glue('"Occupation_Distributions_Stat_Can_r"'))
) |>
  # not sure which one this should be but this matches what is in the SQL table
  mutate(
    SURVEY = if_else(
      SURVEY == "2021 Census PSSM 2022-2023",
      "2021 Census PSSM 2023-2024",
      SURVEY
    )
  )

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

# --------------------------  Calculate Weights --------------------------

# Identify the "Universe" of graduates, grouping by key demographics.
# BASE represents the number of valid respondents and can be verified by summing
# sum(DACSO_Q006b_Weighted_New_Labour_Supply.Unweighted_Count)
# filter removes unknown regions, but keeps the rest, including international students.
# filter includes only NLS 1,2,3 and grad status 1,3.
# filter keeps only respondents who were 17-34 at age of survey
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

# calculate the Weighted NLS: Base: Count(*)*[Weight_NLS];
# filter join removes NA regions and any regions outside of BC.
# filter includes only NLS 1,2,3 and grad status 1,3.
# filter keeps only respondents who were 17-34 at age of survey
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

# Create a table of respondents that we want to keep
# I collapsed the following 4 queries for readability.
# - DACSO_Q008_Z02b_Respondents
# - DACSO_Q008_Z02b_Respondents_NOC_99999
# - DACSO_Q008_Z02b_Respondents_NOC_99999_100_perc
# - DACSO_Q008_Z02b_Respondents_Union
# Reasoning: original queries create a table with only respondents with a NOC and NLS=1,3.
# Then split the table into: NOC_CD=99999 (Unknown) and !NOC_CD=99999 (Known).
# For the NOC_CD=99999 (Unknown) group, kept only the demographic groups where every respondent in the group had a 99999 NOC.
# The two tables were unioned back together.
# I've done this in two steps and get the same results.
# 1) create a table of respondents then
# 2) filter to those with !NOC_CD=99999 (Known), or where all respondents in the demographic group are unknowns.

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

# calculate the weighted and weighted nls for all of the ppl in z02a_base
# this is all respondents with NLS=1-3, GRAD_STATUS=1,3, aged 17-34 at time of survey, living in BC
# bring in the count of respondents for each group.  If there are no respondents for a group, the overall count
# of weighted (n_ppl) will be 0.
# otherwise, the weighted base count will be "weighted up" by some factor
# I beleive what this is doing is up-wieghitng the NLS=2 group into the NLS = 1,3 group.
# for those in the list of respondents we want to condsider
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

# ---- adjustment factor
# dbExecute(decimal_con, DACSO_Q008_Z04_Weight_Adj_Fac)
# WEIGHT_ADJ_FACis the adjustment factor required for the weighted counts to equal the base
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


# ---------- Data Quality Checks Check -------
# 1) find any demographic groups that missing from the occupational weights
# dbExecute(decimal_con, DACSO_Q008_Z05b_Finding_NLS2_Missing)
chk_missing_occ <- tmp_tbl_weights_nls %>%
  anti_join(
    tmp_tbl_weights_occ,
    by = c("SURVEY", "INST_CD", "AGE_GROUP_ROLLUP", "GRAD_STATUS", "LCIP4_CRED")
  )

# 2) find any demographic groups that are missing from the NLS weights
chk_missing_nls <- tmp_tbl_weights_occ %>%
  anti_join(
    tmp_tbl_weights_nls,
    by = c("SURVEY", "INST_CD", "AGE_GROUP_ROLLUP", "GRAD_STATUS", "LCIP4_CRED")
  )

# 3) show the volume of graduate survey respondents across different occupations, age group and labor supply
# dbExecute(decimal_con, DACSO_Q008_Z05b_NOC4D_NLS_XTab)
# BA notes: not implemented as it was too complicated and I don't know why we need it.

# 4) Examine the ratio between the nls and occ weights
# dbExecute(decimal_con, DACSO_Q008_Z05b_Weight_Comparison)
# examine extreme values of 20+ for Weight_OCC
# these differences should be the result of dropping records in the above queries (such as unknown region and NOC)
# BA Notes: I am concerned that there many with no change.  This says to me that dropping unknown region and NOC's affected far fewer groups
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

# -------------------  Add OCC Weights to Cohort Data ---------------------
# calculate and add WEIGHT_OCC to the main cohort table.
# Implements these queries:
#  - DACSO_Q008_Z06_Add_Weight_OCC_Field
#  - DACSO_Q008_Z08_Weight_OCC_Update
#  - DACSO_Q008_Z08_Weight_OCC_Update_NOC_99999_100_perc

t_cohorts_recoded <- t_cohorts_recoded %>%
  mutate(WEIGHT_OCC = as.numeric(NA)) # initialize the field to

# only those records where the entire CIP is 100% NOC 9999
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
# calculating weight_occ following these rules:
# where unknown PSSM region or NOC is null or 9999: keep original weight_occ
# where STQU_ID is not in the original occ_base (NLS = 1,2,3 and GradStatus = 1,3 and a few other conditions) or region code is not valid: keep original weight_occ
# where NOC_CD != "99999", keep either W_STD or original weight_occ if W_STD is NA
# otherwise update weight_occ to W_100P_9999, which is the weight where the entire CIP is 100% NOC 9999
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

# didn't implement this as it is another check.
# dbGetQuery(decimal_con, DACSO_Q008_Z09_Check_Weights)

# ----- Create Occupation Distributions -----
#dbExecute(decimal_con, DACSO_Q009_Weight_Occs)
# this is the main table that feeds into the occupation distributions tables.
# filter join to remove unknown regions
# filter includes only NLS 1,3 and grad status 1,3.
# add a flag to indicate whether a record is in BC or not.
# add a variable for LCP2 by truncating the LCP4 code.
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
    NOC_CD = if_else(NOC_CD == "XXXXX", "99999", NOC_CD) # not sure this is necessary as we should have recoded the NOC's already.
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


# ----- Create Final Occupation Distribution Tables -----
# The weight in this code should work for either a qi or non-qi run,
# so we shouldn't need to run a separate query like:
# dbExecute(decimal_con, DACSO_Q010d1_Delete_PDEG_CIP_Cluster_07_Law_New_Labour_Supply_QI)
# To do: Double-check that the logic for qi vs. regular runs makes sense.
# In theory, our design means we don't need to handle separate qi weights,
# distributions, or queries in this script, because the qi/regular weight
# choice is abstracted out in the 02b1 script.

# 1. occupation_distributions
# collapsed the following 4 queries into tidyverse block for readability.
# - dbExecute(decimal_con, DACSO_Q009b_Weighted_Occs)
# - dbExecute(decimal_con, DACSO_Q009b_Weighted_Occs_Total)
# - dbExecute(decimal_con, DACSO_Q010_Weighted_Occs_Dist)
# - dbExecute(decimal_con, DACSO_Q010a1_Append_Occupational_Distribution)
# Tested against my_schema.occupation_distributions: some differences noted due to rounding

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

occupation_distributions <- dacso_q009_weight_occs |>
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
  ) |>
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

# 2. occupation_distributions_lcp2
# collapse the following 4 queries into tidyverse block for readability.
#  - dbExecute(decimal_con, DACSO_Q009_Weighted_Occs_2D)
#  - dbExecute(decimal_con, DACSO_Q009_Weighted_Occs_Total_2D)
#  - dbExecute(decimal_con, DACSO_Q010_Weighted_Occs_Dist_2D)
#  - dbExecute(decimal_con, DACSO_Q010b1_Append_Occupational_Distribution_LCP2)
# Tested against my_schema.occupation_distributions_lcp2: some differences noted due to rounding

group_vars <- c(
  "PSSM_CREDENTIAL",
  "PSSM_CRED",
  "CURRENT_REGION_PSSM_CODE_ROLLUP",
  "AGE_GROUP_ROLLUP",
  "LCP2_CD",
  "TTRAIN",
  "LCIP2_CRED"
)

occupation_distributions_lcp2 <-
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
  mutate(PERC_DIST = COUNT / TOTAL) |>
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

# 3. occupation_distributions_lcp2_bc
# collapse the following 4 queries into tidyverse block for readability.
#  - dbExecute(decimal_con, DACSO_Q009_Weighted_Occs_2D_BC)
#  - dbExecute(decimal_con, DACSO_Q009_Weighted_Occs_Total_2D_BC)
#  - dbExecute(decimal_con, DACSO_Q010_Weighted_Occs_Dist_2D_BC)
# - dbExecute(decimal_con, DACSO_Q010c1_Append_Occupational_Distribution_LCP2_BC)
# Tested against my_schema.occupation_distributions_lcp2_bc: some differences noted due to rounding

group_vars <- c(
  "PSSM_CREDENTIAL",
  "PSSM_CRED",
  "LCP2_CD",
  "TTRAIN",
  "LCIP2_CRED"
)

occupation_distributions_lcp2_bc <- dacso_q009_weight_occs |>
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
  mutate(PERC_DIST = COUNT / TOTAL) |>
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

# 4. occupation_distributions_lcp2_bc_no_tt
# collapse the following 4 queries into tidyverse block for readability.
# - dbExecute(decimal_con, DACSO_Q009_Weighted_Occs_2D_BC_No_TT)
# - dbExecute(decimal_con, DACSO_Q009_Weighted_Occs_Total_2D_BC_No_TT)
# - dbExecute(decimal_con, DACSO_Q010_Weighted_Occs_Dist_2D_BC_No_TT)
# - dbExecute(decimal_con, DACSO_Q010c1_Append_Occupational_Distribution_LCP2_BC_No_TT)
# Tested against my_schema.occupation_distributions_lcp2_bc_no_tt: no differences noted

group_vars <- c(
  "PSSM_CREDENTIAL",
  "PSSM_CRED",
  "LCP2_CD",
  "LCIP2_CRED"
)


occupation_distributions_lcp2_bc_no_tt <- dacso_q009_weight_occs |>
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
  mutate(PERC_DIST = COUNT / TOTAL) |>
  transmute(
    Survey = "Student Outcomes",
    PSSM_Credential = PSSM_CREDENTIAL,
    PSSM_CRED = PSSM_CRED,
    LCP2_CD = LCP2_CD,
    LCIP2_CRED = LCIP2_CRED,
    TTRAIN = NA_character_, # to match the SQL tables
    NOC = NOC_CD,
    Count = COUNT,
    Total = TOTAL,
    Percent = PERC_DIST
  )


# 5. occupation_distributions_lcp2_no_tt
# I collapsed the following 3 queries into one for readability.
#  - dbExecute(decimal_con, DACSO_Q009_Weighted_Occs_2D_No_TT)
#  - dbExecute(decimal_con, DACSO_Q009_Weighted_Occs_Total_2D_No_TT)
#  - dbExecute(decimal_con, DACSO_Q010_Weighted_Occs_Dist_2D_No_TT)
#  - dbExecute(decimal_con, DACSO_Q010b1_Append_Occupational_Distribution_LCP2_No_TT)
# Tested against my_schema.occupation_distributions_lcp2_no_tt: no differences noted

group_vars <- c(
  "PSSM_CREDENTIAL",
  "PSSM_CRED",
  "CURRENT_REGION_PSSM_CODE_ROLLUP",
  "AGE_GROUP_ROLLUP",
  "LCP2_CD",
  "LCIP2_CRED"
)

occupation_distributions_lcp2_no_tt <- dacso_q009_weight_occs |>
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
  mutate(PERC_DIST = COUNT / TOTAL) |>
  transmute(
    Survey = "Student Outcomes",
    PSSM_Credential = PSSM_CREDENTIAL,
    PSSM_CRED = PSSM_CRED,
    LCP2_CD,
    LCIP2_CRED,
    NOC = NOC_CD,
    Current_Region_PSSM_Code_Rollup = CURRENT_REGION_PSSM_CODE_ROLLUP,
    Age_Group_Rollup = AGE_GROUP_ROLLUP,
    TTRAIN = NA_character_, # to match the SQL tables
    Count = COUNT,
    Total = TOTAL,
    Percent = PERC_DIST
  )

# 6. occupation_distributions_no_tt
# Collapsed the following 4 queries into one for readability.
# - dbExecute(decimal_con, DACSO_Q009b_Weighted_Occs_No_TT)
# - dbExecute(decimal_con, DACSO_Q009b_Weighted_Occs_Total_No_TT)
# - dbExecute(decimal_con, DACSO_Q010_Weighted_Occs_Dist_No_TT)
# - dbExecute(decimal_con, DACSO_Q010a1_Append_Occupational_Distribution_No_TT)
# BA Notes:
# I double-checked the original SQl carefully.
# It casts LCIP4_CRED to nvarchar(20), which actually drops the following groups:
#   - ADCT or ADIP
#   - PDCT or PDDP
#   - ADGR or UT
# I don't understand the purpose, but have replicated this logic filtering to LCIP4_CRED with 20 or fewer characters.
# Tested against my_schema.occupation_distributions_no_tt: no differences noted, but I am concerned
# with why we are dropping groups with LCIP4_CRED longer than 20 characters.

group_vars <- c(
  "PSSM_CREDENTIAL",
  "PSSM_CRED",
  "CURRENT_REGION_PSSM_CODE_ROLLUP",
  "AGE_GROUP_ROLLUP",
  "LCP4_CD",
  "LCIP4_CRED",
  "LCIP2_CRED"
)

occupation_distributions_no_tt <- dacso_q009_weight_occs |>
  mutate(
    LCIP4_CRED = make_lcip_cred(GRAD_STATUS, LCP4_CD, PSSM_CREDENTIAL),
    LCIP2_CRED = make_lcip_cred(GRAD_STATUS, LCP2_CD, PSSM_CREDENTIAL)
  ) |>
  group_by(across(c(all_of(group_vars)))) |>
  summarize(
    TOTAL = sum(WEIGHTED, na.rm = TRUE),
    .groups = "drop"
  ) |>
  left_join(
    dacso_q009_weight_occs |>
      mutate(
        LCIP4_CRED = make_lcip_cred(GRAD_STATUS, LCP4_CD, PSSM_CREDENTIAL),
        LCIP2_CRED = make_lcip_cred(GRAD_STATUS, LCP2_CD, PSSM_CREDENTIAL)
      ) |>
      group_by(across(c(all_of(group_vars), "NOC_CD"))) |>
      summarize(
        COUNT = sum(WEIGHTED, na.rm = TRUE),
        .groups = "drop"
      ) |>
      filter(str_length(LCIP4_CRED) <= 20), # I don't understand the purpose of this filter.  See notes above.
    by = c(group_vars)
  ) |>
  mutate(
    PERC_DIST = COUNT / TOTAL
  ) |>
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
    TTRAIN = NA_character_, # to match the SQL tables
    Count = COUNT,
    Total = TOTAL,
    Percent = PERC_DIST
  )

# ------  create distribution for pdeg/law distribution ------
# These queries calculate New Labour Supply Distribution for Law/PDEG
# TODO: Move to 02b-2
labour_supply_distribution <- labour_supply_distribution |>
  filter(
    !(str_starts(Survey, '2021 Census') & # upper case SURVEY
      PSSM_CREDENTIAL == "PDEG" &
      str_starts(LCP4_CD, "07"))
  ) #6459 records kept

labour_supply_distribution_pdeg <- labour_supply_distribution |>
  filter(
    PSSM_CREDENTIAL == "BACH",
    str_starts(LCP4_CD, "22"),
    str_starts(Survey, "Student Outcomes")
  )

group_vars <- c(
  "Survey",
  "TTRAIN",
  "AGE_GROUP_ROLLUP"
)

# ---- dacso_q010d2_nls_pdeg_07_count ----
dacso_q010d5 <- labour_supply_distribution_pdeg |>
  group_by(across(all_of(c(group_vars, "CURRENT_REGION_PSSM_CODE_ROLLUP")))) |>
  summarize(
    COUNT = sum(COUNT, na.rm = TRUE),
    .groups = "drop"
  ) |>
  left_join(
    labour_supply_distribution_pdeg |>
      distinct(across(all_of(c(group_vars, "TOTAL")))) |>
      group_by(across(all_of(group_vars))) |>
      summarize(
        TOTAL = sum(TOTAL, na.rm = TRUE),
        .groups = "drop"
      ),
    by = group_vars
  ) |>
  transmute(
    Survey = "Student Outcomes",
    PSSM_CREDENTIAL = "PDEG",
    PSSM_CRED = "PDEG",
    CURRENT_REGION_PSSM_CODE_ROLLUP,
    AGE_GROUP_ROLLUP,
    LCP4_CD = "07",
    TTRAIN,
    LCIP4_CRED = "07 - PDEG",
    LCIP2_CRED = NA_character_,
    COUNT,
    TOTAL,
    New_Labour_Supply = if_else(is.na(COUNT), 0, COUNT / TOTAL)
  )

labour_supply_distribution <- labour_supply_distribution |>
  rbind(dacso_q010d5)

# ---- Calculate Occupational Distribution for Law/PDEG
# Collapse the following 4 queries into one for readability.
# - dbExecute(decimal_con, DACSO_Q010e2_Weighted_Occs_PDEG_07)
# - dbExecute(decimal_con, DACSO_Q010e3_Weighted_Occs_Total_PDEG_07)
# - dbExecute(decimal_con, DACSO_Q010e4_Weighted_Occs_Dist_PDEG_07)
# - dbExecute(decimal_con, DACSO_Q010e5_Append_Occupational_Distribution_PDEG_07)
# Tested against my_schema.dacso_q010e2_weighted_occs_pdeg_07: no differences noted
# but I am concerned that we are double counting PDEG respondents:
# LCP2_CD == '22' are not dropped from BACH.

group_vars <- c(
  "Survey",
  "TTRAIN",
  "Current_Region_PSSM_Code_Rollup",
  "Age_Group_Rollup"
)

occupation_distributions_pdeg <- occupation_distributions |>
  filter(
    substr(LCP4_CD, 1, 2) == "22",
    PSSM_Credential == "BACH",
    Survey == "Student Outcomes"
  )

dacso_q010e4_weighted_occs_dist_pdeg_07 <- occupation_distributions_pdeg |>
  group_by(across(all_of(c(group_vars, "NOC")))) |>
  summarize(
    Count = sum(Count, na.rm = TRUE),
    .groups = "drop"
  ) |>
  left_join(
    occupation_distributions_pdeg |>
      group_by(across(all_of(group_vars))) |>
      summarize(
        Total = sum(Count, na.rm = TRUE),
        .groups = "drop"
      ),
    by = group_vars
  ) |>
  transmute(
    Survey = "Student Outcomes",
    PSSM_Credential = "PDEG",
    PSSM_CRED = "PDEG",
    LCP4_CD = "07",
    TTRAIN,
    LCIP4_CRED = "07 - PDEG",
    LCIP2_CRED = NA_character_,
    NOC,
    Current_Region_PSSM_Code_Rollup,
    Age_Group_Rollup,
    Count,
    Total,
    Percent = Count / Total
  )

occupation_distributions <- occupation_distributions |>
  rbind(dacso_q010e4_weighted_occs_dist_pdeg_07)


# Not at all sure why we need  DACSO_Q99A so leaving it for now.
# dbExecute(decimal_con, DACSO_Q99A_ENDDT_IMPUTED)

# ------ create t_suppression_public_release_noc
# t_suppression_public_release_noc seems to be a list of NOCS we want to suppress though not
# sure why it's here and not in the final report section 07/08
# Since t_age_group_rollup isn't in the R environment, I've used age group rollup filter
# and it does filter to 17-64 as the SQL version does, but the label is missing.
# dbExecute(decimal_con, DACSO_qry99_Suppression_Public_Release_NOC)

if (regular_run == TRUE) {
  t_suppression_public_release_noc <- t_cohorts_recoded |>
    filter(WEIGHT > 0) |> # does nothing
    filter(!is.na(AGE_GROUP_ROLLUP)) |>
    summarize(
      Expr1 = n(),
      .by = c(AGE_GROUP_ROLLUP, NOC_CD)
    ) |>
    filter(Expr1 < 5) |>
    mutate(
      AGE_GROUP_ROLLUP_LABEL = case_when(
        AGE_GROUP_ROLLUP == 1 ~ "17 to 29",
        AGE_GROUP_ROLLUP == 2 ~ "30 to 44",
        AGE_GROUP_ROLLUP == 3 ~ "45 to 64",
        TRUE ~ NA_character_
      )
    ) |>
    arrange(Expr1)
}

# ---- include post-bach degrees from stats can data
# this was included here in the original SQL but means the stats can data isn't included
# in the Public Release above?
# Definitely a TODO to check that it makes sense to include here
occupation_distributions <- occupation_distributions |>
  rename_with(toupper) # make upper case to match the stats can data

occupation_distributions <- occupation_distributions |>
  rbind(
    occupation_distributions_stat_can |>
      transmute(
        SURVEY = "2021 Census PSSM 2023-2024",
        PSSM_CREDENTIAL = PSSM_CREDENTIAL,
        PSSM_CRED,
        LCP4_CD,
        TTRAIN = NA_character_,
        LCIP4_CRED,
        LCIP2_CRED = NA_character_,
        NOC,
        CURRENT_REGION_PSSM_CODE_ROLLUP,
        AGE_GROUP_ROLLUP,
        COUNT,
        TOTAL,
        PERCENT
      )
  )

## ------------------------------------ Clean Up --------------------------------------------------
# Current workflow:
#  - Write key tables back to sql server.  These are tables needed for downstream work, or tables
# that might be needed for later reference outside of this analysis.
#  - Close DB connections
#  - Remove all other objects at the end of each script.
## ------------------------------------------------------------------------------------------------

tables_to_keep <- c(
  "labour_supply_distribution",
  "occupation_distributions",
  "occupation_distributions_no_tt",
  "occupation_distributions_lcp2",
  "occupation_distributions_lcp2_no_tt",
  "occupation_distributions_lcp2_bc",
  "occupation_distributions_lcp2_bc_no_tt"
)

write_table_to_db <- function(table_name, schema, con) {
  db_name <- paste0(table_name, "_r")
  dbWriteTable(
    con,
    SQL(glue::glue('"{schema}"."{db_name}"')),
    base::get(table_name, envir = .GlobalEnv),
    overwrite = TRUE
  )
}

walk(tables_to_keep, write_table_to_db, schema = my_schema, con = con)
