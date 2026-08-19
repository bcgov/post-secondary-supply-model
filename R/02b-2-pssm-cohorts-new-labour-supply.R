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
# Outcomes data has been standardized so all cohorts/surveys are combined in a single dataset
# (T_Cohorts_Recoded, built by 02b-1-pssm-cohorts.R).
#
# At a high level, the script:
#   Searches and updates invalid NOC codes in bgs and dacso tables
#   recodes the new labour supply for those with an NLS-2 record and no NLS-1
#   Weights each year up to the cohort (Prob_Weight) and apply year weights
#     (1,2,3,4,5) and adjust to the cohort.
#   Create weights for new labour supply (Weight_NLS)

# Includes records with a labour force status for those aged 17 to 64,
# Includes those with an invalid NOC where 100% of CIP is invalid, as the cohort number.

# WHERE THIS SITS IN THE MODEL (docs/project-summary-for-new-analyst.md section 2;
# weighting detail in docs/weights-explained-02b-2-and-02b-3.md section 3):
#   OCCSN(NOC) = GRADUATES(cred,age) x P(CIP|cred,age)      <- Module 06
#                x P(in labour supply|CIP)                   <- THIS SCRIPT (02b-2)
#                x P(NOC|CIP,region)                         <- 02b-3
# This script produces the participation term: the New_Labour_Supply column of
# the labour_supply_distribution tables is P(in labour supply | CIP, age,
# region), estimated from the survey cohorts and benchmarked at the margins by
# the 2021-Census StatCan distributions appended near the end.
# The WEIGHT_NLS column it adds to t_cohorts_recoded is the respondent weight
# 02b-3 re-uses when it computes P(NOC|CIP,region) over the labour-supply
# population (NLS 1-3) -- the two scripts share the same 4-stage weighting
# template but calibrate to different anchors (raw cohort count here,
# NLS-weighted labour-supply base there; see the weights doc).
#
# INPUT PROVENANCE (where each required table comes from):
#   - t_cohorts_recoded: in-session from 02b-1 when chained by
#     prep-for-fresh-run.R, or my_schema.t_cohorts_recoded_r standalone. The
#     master respondent-level table; carries the NLS codes, the
#     LCIP4_CRED / LCIP2_CRED stratum keys and the model-year WEIGHT.
#   - t_current_region_pssm_codes / t_current_region_pssm_rollup_codes /
#     t_noc_broad_categories: stable dbo lookups, loaded into the session by
#     load-cohort-dacso.R ahead of this script in the runner chain.
#   - labour_supply_distribution_stat_can: the 2021-Census benchmark, built by
#     R/labour-supply-dists-census-data.R from the StatCan export
#     (LAN data/statcan/stat-can-data-export-for-labour-supply-distributions.xlsx)
#     into my_schema -- the only required table no cohort loader materializes,
#     so this script reads it from the database directly.
#
# OUTPUTS (written to the PERSONAL schema, my_schema, as <name>_r):
#   - labour_supply_distribution / _no_tt / _lcp2 / _lcp2_no_tt: the four
#     distribution tables Module 07 joins. The _lcp2 variants carry the 2-digit
#     key column LCP2_CRED -- deliberately NOT LCIP2_CRED, matching the SQL
#     originals and the PSSM2023 baseline tables.
#   - t_cohorts_recoded: re-written with the WEIGHT_NLS column added (02b-1's
#     write lacks it; this overwrite is the correct sequencing).
#   - tmp_tbl_weights_nls: the per-stratum/year weight table 02b-3 requires
#     in-session.
# Census rows inside the outputs carry 2-digit LCP4_CD-based keys and coexist
# with the SO rows' 4-digit composite keys; consumers tell them apart by the
# Survey column ("Student Outcomes" vs "2021 Census ...") and always
# prefix-match the census label downstream, never full-label equality.
#
# Notes:  some invalid NOC codes are recoded below (see Initial Data Checks);
#         PDEG (law) is included at the end via the BACH legal-professions
#         proxy block -- the earlier "missing PDEG / Non-Student Outcomes"
#         FIXME is resolved by the StatCan append + that block.

# -------------------------- Libraries and Global Variables  ---------------------

library(tidyverse)
library(config)
library(glue)
library(assertthat)


# Model data window: the survey years feeding this run. The window is
# enforced upstream -- 02b-1's t_year_survey_year joins already restrict
# t_cohorts_recoded to these years -- so this vector documents the window
# for the run and scopes the invalid-NOC diagnostic below. Keep it in sync
# with the window when the model refreshes.
## ----------------------------------------------------------
## Reasons for change, other notes
## 2025 refresh: window slid 2019-2023 -> 2021-2025 (02b-1's
## output now spans the new years). The vector is now actually
## referenced -- the invalid-NOC diagnostic below filters on it,
## replacing the old quoted-string list that never matched the
## numeric SURVEY_YEAR column and silently returned nothing.
## ----------------------------------------------------------
years <- c(2021:2025) # years of data used this model run

## ----------------------------------------------------------
## Reasons for change, other notes
## 2025 refresh: this section is production code, not development
## scaffolding -- the connection feeds the StatCan read here and
## the write-backs at the end of the script. Banner renamed and
## the unused library(RODBC) dropped (the script connects via
## DBI/odbc; RODBC was never called).
## ----------------------------------------------------------
# ---- DB Connection and StatCan Read ----
library(DBI)

# ---- Connect to SQL Server and read StatCan Tables ----
# Production input, despite the section banner above: the connection serves
# the StatCan read here AND the write-backs at the end of the script.
lan <- config::get("lan")
my_schema <- config::get("myschema")
db_config <- config::get("decimal")
con <- dbConnect(
  odbc::odbc(),
  Driver = db_config$driver,
  Server = db_config$server,
  Database = db_config$database,
  Trusted_Connection = "True"
)


# 2021-Census benchmark distributions (see header INPUT PROVENANCE): built
# into my_schema by R/labour-supply-dists-census-data.R. The label remap
# below normalizes the census script's export-vintage label ("2022-2023",
# the cycle the export was mapped in) to the current model-year label at
# read time -- this remap is the per-refresh mechanism for relabeling.
# Downstream consumers only prefix-match "2021 Census", never the full label.
## ----------------------------------------------------------
## Reasons for change, other notes
## 2025 refresh: guarded per the 02b-1 pattern (commit 1449ce3)
## so standalone runs and double-sourcing don't re-read. The read
## prefers the _r table written by R/labour-supply-dists-census-
## data.R (this cycle's sourcing decision, ticket #150) and falls
## back to the no-suffix name prep-for-fresh-run copies from the
## second schema. Remap target updated to the current model year
## (2021 Census PSSM 2024-2025); the census script's own label
## stays as the export vintage, remapped here each refresh.
## ----------------------------------------------------------
if (!exists("labour_supply_distribution_stat_can")) {
  stat_can_name <- if (dbExistsTable(
    con,
    Id(schema = my_schema, table = "Labour_Supply_Distribution_Stat_Can_r")
  )) {
    "Labour_Supply_Distribution_Stat_Can_r"
  } else {
    "Labour_Supply_Distribution_Stat_Can"
  }
  labour_supply_distribution_stat_can <- tibble(dbReadTable(
    con,
    SQL(glue::glue('"{my_schema}"."{stat_can_name}"'))
  )) |>
    # not sure which one this should be but this matches what is in the SQL table
    mutate(
      SURVEY = if_else(
        SURVEY == "2021 Census PSSM 2022-2023",
        "2021 Census PSSM 2024-2025",
        SURVEY
      )
    )
}

# -------------------------- Required Tables -----------------------------------------

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

# --------------------------  Standardize Names -----------------------------
# Defensive case normalization: 02b-1's output is already upper-case, but the
# dbo lookups and (especially) tables read back from SQL Server can arrive
# with mixed-case column names depending on how they were written. Everything
# downstream keys on the upper-case contract.
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


# -------------------------- Initial Data Checks ---------------------
# Diagnostics only -- both blocks print to the console and are not assigned.
# They surface weight coverage and stray NOC codes before the weighting runs.

# ---- base weights
# one row per distinct (survey, survey year, base WEIGHT) combination --
# eyeballs that every survey/year stratum carries exactly one year weight.
# DACSO_Q005_DACSO_DATA_Part_1b3_Check_Weights
t_cohorts_recoded |>
  count(SURVEY, SURVEY_YEAR, WEIGHT) |>
  select(-n)

# ---- invalid noc codes
# Lists NOC_CD values present in the cohort data but absent from
# t_noc_broad_categories (UNIT_GROUP_CODE is NA after the left join).
# SURVEY_YEAR is numeric in 02b-1's output -- compare against the numeric
# `years` vector, not quoted strings, or the filter silently empties.
# in 2019 there were some invalid nocs: 403X were set to 4031, 4032, or 9999
# in 2021 there were some invalid nocs: 4122X were set to 99999
# (2025 refresh check: 4122X is the only pattern remaining, 465 rows across
#  BGS/DACSO 2021-2025 -- see the recode below)
# DACSO_Q99A_STQUI_ID
# DACSO_Q005_DACSO_DATA_Part_1b4_Check_NOC_Valid
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
    as.numeric(SURVEY_YEAR) %in% years, # numeric `years` vector -- quoted strings never match the numeric column
    !is.na(NOC_CD),
    NOC_CD != "",
    is.na(UNIT_GROUP_CODE)
  ) |>
  distinct(STQU_ID_ONLY, STQU_ID, SURVEY, SURVEY_YEAR, NOC_CD, UNIT_GROUP_CODE)

# These two queries weren't done last year as we are missing the tables.
# DACSO_Q005_DACSO_Data_Part_1b7_Update_After_Recoding
# DACSO_Q005_DACSO_Data_Part_1b8_Update_After_Recoding
# details: NOC codes from t_bgs_data_final and t_dacso_data underwent a recoding or
# imputing process.  Todo: check documentation for details and consider how/if
# to implement.

t_cohorts_recoded <- t_cohorts_recoded |>
  mutate(NOC_CD = if_else(NOC_CD == "4122X", "99999", NOC_CD))

# -------------------------- Recode New Labour Supply ----------------------
# NLS 2 -> 3 recode. NLS-2 = in the labour supply while still studying (DACSO
# and BGS only); NLS-1 = in the supply proper. When a stratum's NLS-2 records
# have NO NLS-1 records anywhere in the same (survey, region rollup, age,
# institution, LCIP4_CRED) cell, the studying-while-working signal is all the
# stratum has -- recode those respondents to 3 so they still count toward the
# labour-supply numerator here, while staying distinguishable from NLS-1
# downstream (02b-3 excludes NLS-2 from its NOC denominator but keeps 3).
# for records with an NLS-2 record and no NLS1, set to 3
# DACSO_Q005_DACSO_DATA_Part_1c_NLS1
# DACSO_Q005_DACSO_DATA_Part_1c_NLS2
# DACSO_Q005_DACSO_DATA_Part_1c_NLS2_Recode

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

# check the number of records in the cohort for the years included
# DACSO_Q005_Z_Cohort_Resp_by_Region
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

# -------------------------- Create Base Weights --------------------------
# The full-cohort base: respondent-level record counts per stratum (+STQU_ID),
# over ALL valid NLS values (0-3). This is the anchor the WEIGHT_NLS
# adjustment factor calibrates to -- the calibration identity is
#   sum(resp * WEIGHT_NLS) == BASE_TOTAL  per stratum
# (checked by the z09 blocks below; derivation in the weights doc section 3).
# this is for the full cohort
# DACSO_Q005_Z01_Base_NLS
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
#DACSO_Q005_Z02a_Base
#DACSO_Q005_Z02b_Respondents
#DACSO_Q005_Z02b_Respondents_Region_9999
#DACSO_Q005_Z02b_Respondents_Union

# -------------------------- Create NLS Weights --------------------------
# The 4-stage weighting template (docs/weights-explained-02b-2-and-02b-3.md
# section 3 -- same template as 02b-3, different anchor):
#   WEIGHT_NLS = (COUNT / RESPONDENTS)     <- inverse response rate: each
#                                            region-valid respondent stands
#                                            for the stratum's non-response
#                  * WEIGHT_YEAR           <- recency weight 1-5 from 02b-1
#                  * ADJ_FAC               <- BASE_TOTAL / WEIGHTED_TOTAL:
#                                            divides out the average year
#                                            weight so the stratum total
#                                            anchors to the RAW cohort count
# The SQL intermediates Z02c/Z03/Z04/Z05 are collapsed into the single
# tmp_tbl_weights_nls pipeline below (strata drop SURVEY_YEAR at the
# adjustment stage, pooling the 5-year window).
# DACSO_Q005_Z02c_Weight_tmp
# DACSO_Q005_Z02c_Weight
# DACSO_Q005_Z03_Weight_Total
# DACSO_Q005_Z04_Weight_Adj_Fac
# DACSO_Q005_Z05_Weight_NLS

# ---- count respondents and total records in each strata
# strata are (survey, survey_year, inst_cd, age_group_rollup, grad_status, ttrain, lcip4_cred)
tmp_tbl_weights_nls <- t_cohorts_recoded %>%
  filter(
    NEW_LABOUR_SUPPLY %in% 0:3,
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
      RESPONDENT == '1' & CURRENT_REGION_PSSM_CODE != -1,
      na.rm = TRUE
    ),
    .groups = "drop"
  )


# ---- adjusted year weights for each strata
# strata are (survey, survey_year, inst_cd, age_group_rollup, grad_status, ttrain, lcip4_cred)
tmp_tbl_weights_nls <- tmp_tbl_weights_nls |>
  mutate(
    WEIGHT_YEAR = WEIGHT,
    WEIGHT_PROB = if_else(RESPONDENTS == 0, 1, as.numeric(COUNT) / RESPONDENTS),
    WEIGHT = WEIGHT_PROB * WEIGHT_YEAR,
    WEIGHTED = RESPONDENTS * WEIGHT # This is respondents * (count/resp) * weight_year = count * weight_year
  )

# ---- calculate final NLS Weight for each strata
# strata are (survey, inst_cd, age_group_rollup, grad_status, ttrain, lcip4_cred)
tmp_tbl_weights_nls <- tmp_tbl_weights_nls |>
  group_by(
    SURVEY,
    INST_CD,
    AGE_GROUP_ROLLUP,
    GRAD_STATUS,
    TTRAIN,
    LCIP4_CRED
  ) %>%
  mutate(
    BASE_TOTAL = sum(COUNT, na.rm = TRUE),
    WEIGHTED_TOTAL = sum(WEIGHTED, na.rm = TRUE),
    WEIGHT_ADJ_FAC = if_else(
      WEIGHTED_TOTAL == 0,
      0,
      BASE_TOTAL / WEIGHTED_TOTAL
    ),
    WEIGHT_NLS = WEIGHT * WEIGHT_ADJ_FAC
  ) %>%
  ungroup()

# ---- null Weight_NLS field and update
# Initialize then join WEIGHT_NLS back onto the respondent table, keyed on
# everything but region (the weight is region-blind by design -- 02b-3
# introduces region fresh; weights doc section 8.1): rows with no valid
# region (-1) or no matching stratum keep NA.
# Mainly if working in SQL but maybe needed for QI run depending on how we handle the QI weights.
# Todo: check documentation and consider how/if to implement.
# DACSO_Q005_Z08_Weight_NLS_Update
# DACSO_Q005_Z07_Weight_NLS_Null
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

# ----- check weights
# z09 verification blocks: the first recomputes RESPONDENTS * WEIGHT_NLS per
# stratum -- summed over a stratum it must reproduce BASE (the calibration
# identity of the weighting above); the second lists strata whose weight
# ended up 0/NA so those cohorts are visible rather than silently dropped.
# DACSO_Q005_Z09_Check_Weights
# DACSO_Q005_Z09_Check_Weights_No_Weight_CIP
z09_check_weights3 <- t_cohorts_recoded %>%
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
    BASE = sum(BASE, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(WEIGHTED = RESPONDENTS * as.numeric(WEIGHT_NLS)) %>%
  arrange(SURVEY_YEAR, WEIGHT_NLS)

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

# ---- clear environment
## ----------------------------------------------------------
## Reasons for change, other notes
## 2025 refresh: rm() named four SQL-only intermediates
## (z02c_weight_tmp, z02c_weight, z03_weight_total,
## z04_weight_adj_fac) that never exist in R -- the pipeline
## collapsed them into tmp_tbl_weights_nls -- so the call errored
## every run at this point. Trimmed to the one real object.
## ----------------------------------------------------------
rm(z01_base_nls)

# -------------------------- Weighted New Labour Supply --------------------------
# apply nls weights to group totals and filter to observations of interest
# include only records with
#  - a valid NLS value (0-3)
#  - valid age group
#  - grad status of 1 or 3
#  - a weight greater than 0
#  - a current region pssm code that is not -1 or 9999
# then calulate the weighted count of records for each strata
# stratum are: pssm_credential, current_region_pssm_code_rollup, survey_year,
# inst_cd, age_group_rollup, grad_status, lcp4_cd, ttrain

# DACSO_Q006a_Weight_New_Labour_Supply
weight_new_labour_supply <- t_cohorts_recoded %>%
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

# -------------------------- Weighted Counts - NLS Distributions --------------------------
# calculate weighted counts of new labor supply - various distributions.  There are 12 of these
# Naming scheme for the 12 count tables (and the 8 proportion tables built on
# them below):
#   (base) ..... NLS 1-3 counts (the labour-supply numerator)
#   _0 ......... NLS 0 counts (the out-of-supply complement)
#   _total ..... NLS 0-3 counts (the denominator; drops the region dimension)
#   _2d ........ 2-digit CIP keys (LCP2_CD) instead of 4-digit
#   _no_tt ..... TTRAIN collapsed out of the stratum
# Only the pairs feeding the final four labour_supply_distribution* outputs
# matter downstream; the rest are the SQL lineage kept for traceability.
# DACSO_Q006b_Weighted_New_Labour_Supply
# DACSO_Q006b_Weighted_New_Labour_Supply_0
# DACSO_Q006b_Weighted_New_Labour_Supply_0_2D
# DACSO_Q006b_Weighted_New_Labour_Supply_0_2D_No_TT
# DACSO_Q006b_Weighted_New_Labour_Supply_0_No_TT
# DACSO_Q006b_Weighted_New_Labour_Supply_2D
# DACSO_Q006b_Weighted_New_Labour_Supply_2D_No_TT
# DACSO_Q006b_Weighted_New_Labour_Supply_No_TT
# DACSO_Q006b_Weighted_New_Labour_Supply_Total
# DACSO_Q006b_Weighted_New_Labour_Supply_Total_2D
# DACSO_Q006b_Weighted_New_Labour_Supply_Total_2D_No_TT
# DACSO_Q006b_Weighted_New_Labour_Supply_Total_No_TT

# calculate weighted and unweighted count of new labour supply by full strata
# stratum are: pssm_credential, current_region_pssm_code_rollup, age_group_rollup, grad_status, lcp4_cd, ttrain
# include only records with
#  - a valid NLS value (1-3)
dacso_q006b_weighted_new_labour_supply <- weight_new_labour_supply %>%
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


# calculate weighted count of new labour supply by strata
# stratum are: pssm_credential, current_region_pssm_code_rollup, age_group_rollup, grad_status, lcp4_cd, ttrain
# include only records with a valid NLS value of 0
dacso_q006b_weighted_new_labour_supply_0 <- weight_new_labour_supply %>%
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

# calculate weighted count of new labour supply by strata
# stratum are: pssm_credential, current_region_pssm_code_rollup, age_group_rollup, grad_status, lcp2_cd, ttrain
# include only records with a valid NLS value of 0 and group by lcp2_cd instead of lcp4_cd
dacso_q006b_weighted_new_labour_supply_0_2d <- weight_new_labour_supply %>%
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

# calculate weighted count of new labour supply by strata
# stratum are: pssm_credential, current_region_pssm_code_rollup, age_group_rollup, grad_status, lcp2_cd
# include only records with a valid NLS value of 0
dacso_q006b_weighted_new_labour_supply_0_2d_no_tt <- weight_new_labour_supply %>%
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

# calculate weighted count of new labour supply by strata
# stratum are: pssm_credential, current_region_pssm_code_rollup, age_group_rollup, grad_status, lcp4_cd
# include only records with a valid NLS value of 0
dacso_q006b_weighted_new_labour_supply_0_no_tt <- weight_new_labour_supply %>%
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

# calculate weighted count of new labour supply by strata
# stratum are: pssm_credential, current_region_pssm_code_rollup, age_group_rollup, ttrain, lcp2_cd
# include only records with a valid NLS value of 1, 2, or 3
dacso_q006b_weighted_new_labour_supply_2d <- weight_new_labour_supply %>%
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

# calculate weighted count of new labour supply by strata
# stratum are: pssm_credential, current_region_pssm_code_rollup, age_group_rollup, ttrain, lcp2_cd
# include only records with a valid NLS value of 1, 2, or 3
dacso_q006b_weighted_new_labour_supply_2d_no_tt <- weight_new_labour_supply %>%
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

# calculate weighted count of new labour supply by strata
# stratum are: pssm_credential, current_region_pssm_code_rollup, age_group_rollup, lcp4_cd
# include only records with a valid NLS value of 1, 2, or 3
dacso_q006b_weighted_new_labour_supply_no_tt <- weight_new_labour_supply %>%
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

# calculate weighted count of new labour supply by strata
# stratum are: pssm_credential, age_group_rollup, lcp4_cd, ttrain
# include only records with a valid NLS value of 0, 1, 2, or 3
dacso_q006b_weighted_new_labour_supply_total <- weight_new_labour_supply %>%
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

# calculate weighted count of new labour supply by strata
# stratum are: pssm_credential, age_group_rollup, ttrain, lcp2_cd
# include only records with a valid NLS value of 0, 1, 2, or 3
dacso_q006b_weighted_new_labour_supply_total_2d <- weight_new_labour_supply %>%
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

# calculate weighted count of new labour supply by strata
# stratum are: pssm_credential, age_group_rollup, lcp2_cd
# include only records with a valid NLS value of 0, 1, 2, or 3
dacso_q006b_weighted_new_labour_supply_total_2d_no_tt <- weight_new_labour_supply %>%
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

# calculate weighted count of new labour supply by strata
# stratum are: pssm_credential, age_group_rollup, lcp4_cd
# include only records with a valid NLS value of 0, 1, 2, or 3
dacso_q006b_weighted_new_labour_supply_total_no_tt <- weight_new_labour_supply %>%
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

# -------------------------- Weighted Proportions - NLS Distributions --------------------------
# calculate weighted proportions/percentages of new labor supply - 8 various distributions.  Each
# query/code chunk below is a combination of one of the 12 weighted counts tables above.
# Semantics (weights doc section 3): PERC = region's weighted NLS 1-3 count /
# all-region weighted NLS 0-3 total, i.e. P(in labour supply | CIP, age,
# region). The join keys are (AGE_GROUP_ROLLUP, LCIP4_CRED) -- the total
# table carries no region, so the join fans each total out across the
# regions present in the numerator table.
# The _0 complement tables append explicit New_Labour_Supply = 0 rows: PERC
# = 1 - COUNT/TOTAL with filter(COUNT > 0, PERC == 0) keeps exactly the
# strata that are ENTIRELY out of supply (NLS-0 count == total).

# DACSO_Q007a_Weighted_New_Labour_Supply
# DACSO_Q007a_Weighted_New_Labour_Supply_0
# DACSO_Q007a_Weighted_New_Labour_Supply_0_2D
# DACSO_Q007a_Weighted_New_Labour_Supply_0_2D_No_TT
# DACSO_Q007a_Weighted_New_Labour_Supply_0_No_TT
# DACSO_Q007a_Weighted_New_Labour_Supply_2D
# DACSO_Q007a_Weighted_New_Labour_Supply_2D_No_TT
# DACSO_Q007a_Weighted_New_Labour_Supply_No_TT

dacso_q007a_weighted_new_labour_supply <- dacso_q006b_weighted_new_labour_supply_total %>%
  left_join(
    dacso_q006b_weighted_new_labour_supply %>%
      select(
        AGE_GROUP_ROLLUP,
        LCIP4_CRED,
        CURRENT_REGION_PSSM_CODE_ROLLUP,
        COUNT
      ),
    by = c("AGE_GROUP_ROLLUP", "LCIP4_CRED")
  ) %>%
  replace_na(list(COUNT = 0)) %>%
  filter(!is.na(CURRENT_REGION_PSSM_CODE_ROLLUP)) %>%
  mutate(PERC = COUNT / TOTAL)

dacso_q007a_weighted_new_labour_supply_0 <- dacso_q006b_weighted_new_labour_supply_total %>%
  left_join(
    dacso_q006b_weighted_new_labour_supply_0 %>%
      select(
        AGE_GROUP_ROLLUP,
        LCIP4_CRED,
        CURRENT_REGION_PSSM_CODE_ROLLUP,
        COUNT
      ),
    by = c("AGE_GROUP_ROLLUP", "LCIP4_CRED")
  ) %>%
  replace_na(list(COUNT = 0)) %>%
  mutate(PERC = 1 - (COUNT / TOTAL)) %>%
  filter(COUNT > 0, PERC == 0)

dacso_q007a_weighted_new_labour_supply_0_2d <- dacso_q006b_weighted_new_labour_supply_total_2d %>%
  left_join(
    dacso_q006b_weighted_new_labour_supply_0_2d %>%
      select(
        AGE_GROUP_ROLLUP,
        LCP2_CRED,
        CURRENT_REGION_PSSM_CODE_ROLLUP,
        COUNT
      ),
    by = c("AGE_GROUP_ROLLUP", "LCP2_CRED")
  ) %>%
  replace_na(list(COUNT = 0)) %>%
  mutate(PERC = 1 - (COUNT / TOTAL)) %>%
  filter(COUNT > 0, PERC == 0)

dacso_q007a_weighted_new_labour_supply_0_2d_no_tt <- dacso_q006b_weighted_new_labour_supply_total_2d_no_tt %>%
  left_join(
    dacso_q006b_weighted_new_labour_supply_0_2d_no_tt %>%
      select(
        AGE_GROUP_ROLLUP,
        LCP2_CRED,
        CURRENT_REGION_PSSM_CODE_ROLLUP,
        COUNT
      ),
    by = c("AGE_GROUP_ROLLUP", "LCP2_CRED")
  ) %>%
  replace_na(list(COUNT = 0)) %>%
  mutate(PERC = 1 - (COUNT / TOTAL)) %>%
  filter(COUNT > 0, PERC == 0)

dacso_q007a_weighted_new_labour_supply_0_no_tt <- dacso_q006b_weighted_new_labour_supply_total_no_tt %>%
  left_join(
    dacso_q006b_weighted_new_labour_supply_0_no_tt %>%
      select(
        AGE_GROUP_ROLLUP,
        LCIP4_CRED,
        CURRENT_REGION_PSSM_CODE_ROLLUP,
        COUNT
      ),
    by = c("AGE_GROUP_ROLLUP", "LCIP4_CRED")
  ) %>%
  replace_na(list(COUNT = 0)) %>%
  mutate(PERC = 1 - (COUNT / TOTAL)) %>%
  filter(COUNT > 0, PERC == 0)

dacso_q007a_weighted_new_labour_supply_2d <- dacso_q006b_weighted_new_labour_supply_total_2d %>%
  left_join(
    dacso_q006b_weighted_new_labour_supply_2d %>%
      select(
        AGE_GROUP_ROLLUP,
        LCP2_CRED,
        CURRENT_REGION_PSSM_CODE_ROLLUP,
        COUNT
      ),
    by = c("AGE_GROUP_ROLLUP", "LCP2_CRED")
  ) %>%
  replace_na(list(COUNT = 0)) %>%
  filter(!is.na(CURRENT_REGION_PSSM_CODE_ROLLUP)) %>%
  mutate(PERC = COUNT / TOTAL)

dacso_q007a_weighted_new_labour_supply_2d_no_tt <- dacso_q006b_weighted_new_labour_supply_total_2d_no_tt %>%
  left_join(
    dacso_q006b_weighted_new_labour_supply_2d_no_tt %>%
      select(
        AGE_GROUP_ROLLUP,
        LCP2_CRED,
        CURRENT_REGION_PSSM_CODE_ROLLUP,
        COUNT
      ),
    by = c("AGE_GROUP_ROLLUP", "LCP2_CRED")
  ) %>%
  replace_na(list(COUNT = 0)) %>%
  filter(!is.na(CURRENT_REGION_PSSM_CODE_ROLLUP)) %>%
  mutate(PERC = COUNT / TOTAL)

dacso_q007a_weighted_new_labour_supply_no_tt <- dacso_q006b_weighted_new_labour_supply_total_no_tt %>%
  left_join(
    dacso_q006b_weighted_new_labour_supply_no_tt %>%
      select(
        AGE_GROUP_ROLLUP,
        LCIP4_CRED,
        CURRENT_REGION_PSSM_CODE_ROLLUP,
        COUNT
      ),
    by = c("AGE_GROUP_ROLLUP", "LCIP4_CRED")
  ) %>%
  replace_na(list(COUNT = 0)) %>%
  filter(!is.na(CURRENT_REGION_PSSM_CODE_ROLLUP)) %>%
  mutate(PERC = COUNT / TOTAL)

# ----- clear environment
rm(
  weight_new_labour_supply,
  dacso_q006b_weighted_new_labour_supply,
  dacso_q006b_weighted_new_labour_supply_0,
  dacso_q006b_weighted_new_labour_supply_0_2d,
  dacso_q006b_weighted_new_labour_supply_0_2d_no_tt,
  dacso_q006b_weighted_new_labour_supply_0_no_tt,
  dacso_q006b_weighted_new_labour_supply_2d,
  dacso_q006b_weighted_new_labour_supply_2d_no_tt,
  dacso_q006b_weighted_new_labour_supply_no_tt,
  dacso_q006b_weighted_new_labour_supply_total,
  dacso_q006b_weighted_new_labour_supply_total_2d,
  dacso_q006b_weighted_new_labour_supply_total_2d_no_tt,
  dacso_q006b_weighted_new_labour_supply_total_no_tt
)

# -------------------------- Final NLS Distributions --------------------------
# DACSO_Q007b0_Delete_New_Labour_Supply
# DACSO_Q007b0_Delete_New_Labour_Supply_No_TT
# DACSO_Q007b0_Delete_New_Labour_Supply_No_TT_QI
# DACSO_Q007b0_Delete_New_Labour_Supply_QI
# DACSO_Q007b1_Append_New_Labour_Supply
# DACSO_Q007b2_Append_New_Labour_Supply_0
# DACSO_Q007b1_Append_New_Labour_Supply_No_TT
# DACSO_Q007b2_Append_New_Labour_Supply_0_No_TT

labour_supply_distribution <- bind_rows(
  dacso_q007a_weighted_new_labour_supply %>%
    mutate(Survey = "Student Outcomes") %>%
    rename(New_Labour_Supply = PERC),
  dacso_q007a_weighted_new_labour_supply_0 %>%
    mutate(Survey = "Student Outcomes") %>%
    rename(New_Labour_Supply = PERC)
) %>%
  select(
    Survey,
    PSSM_CREDENTIAL,
    PSSM_CRED,
    CURRENT_REGION_PSSM_CODE_ROLLUP,
    AGE_GROUP_ROLLUP,
    LCP4_CD,
    TTRAIN,
    LCIP4_CRED,
    LCIP2_CRED,
    COUNT,
    TOTAL,
    New_Labour_Supply
  )

labour_supply_distribution_no_tt <- bind_rows(
  dacso_q007a_weighted_new_labour_supply_no_tt %>%
    mutate(Survey = "Student Outcomes") %>%
    rename(New_Labour_Supply = PERC),
  dacso_q007a_weighted_new_labour_supply_0_no_tt %>%
    mutate(Survey = "Student Outcomes") %>%
    rename(New_Labour_Supply = PERC)
) %>%
  select(
    Survey,
    PSSM_CREDENTIAL,
    PSSM_CRED,
    CURRENT_REGION_PSSM_CODE_ROLLUP,
    AGE_GROUP_ROLLUP,
    LCP4_CD,
    LCIP4_CRED,
    LCIP2_CRED,
    COUNT,
    TOTAL,
    New_Labour_Supply
  )

# these should make final distributions similar to above, but I don't believe we use them.
# DACSO_Q007c0_Delete_New_Labour_Supply_2D
# DACSO_Q007c0_Delete_New_Labour_Supply_2D_No_TT
# DACSO_Q007c0_Delete_New_Labour_Supply_2D_No_TT_QI
# DACSO_Q007c0_Delete_New_Labour_Supply_2D_QI
# DACSO_Q007c1_Append_New_Labour_Supply_2D
# DACSO_Q007c1_Append_New_Labour_Supply_2D_No_TT
# DACSO_Q007c2_Append_New_Labour_Supply_0_2D
# DACSO_Q007c2_Append_New_Labour_Supply_0_2D_No_TT

labour_supply_distribution_lcp2 <- dacso_q007a_weighted_new_labour_supply_2d %>%
  mutate(Survey = "Student Outcomes") %>%
  rename(New_Labour_Supply = PERC) %>%
  select(
    Survey,
    PSSM_CREDENTIAL,
    PSSM_CRED,
    CURRENT_REGION_PSSM_CODE_ROLLUP,
    AGE_GROUP_ROLLUP,
    LCP2_CD,
    TTRAIN,
    LCP2_CRED,
    COUNT,
    TOTAL,
    New_Labour_Supply
  )

labour_supply_distribution_lcp2 <- bind_rows(
  labour_supply_distribution_lcp2,
  dacso_q007a_weighted_new_labour_supply_0_2d %>%
    mutate(Survey = "Student Outcomes") %>%
    rename(New_Labour_Supply = PERC) %>%
    select(
      Survey,
      PSSM_CREDENTIAL,
      PSSM_CRED,
      CURRENT_REGION_PSSM_CODE_ROLLUP,
      AGE_GROUP_ROLLUP,
      LCP2_CD,
      TTRAIN,
      LCP2_CRED,
      COUNT,
      TOTAL,
      New_Labour_Supply
    )
)

labour_supply_distribution_lcp2_no_tt <- dacso_q007a_weighted_new_labour_supply_2d_no_tt %>%
  mutate(Survey = "Student Outcomes") %>%
  rename(New_Labour_Supply = PERC) %>%
  select(
    Survey,
    PSSM_CREDENTIAL,
    PSSM_CRED,
    CURRENT_REGION_PSSM_CODE_ROLLUP,
    AGE_GROUP_ROLLUP,
    LCP2_CD,
    LCP2_CRED,
    COUNT,
    TOTAL,
    New_Labour_Supply
  )

labour_supply_distribution_lcp2_no_tt <- bind_rows(
  labour_supply_distribution_lcp2_no_tt,
  dacso_q007a_weighted_new_labour_supply_0_2d_no_tt %>%
    mutate(Survey = "Student Outcomes") %>%
    rename(New_Labour_Supply = PERC) %>%
    select(
      Survey,
      PSSM_CREDENTIAL,
      PSSM_CRED,
      CURRENT_REGION_PSSM_CODE_ROLLUP,
      AGE_GROUP_ROLLUP,
      LCP2_CD,
      LCP2_CRED,
      COUNT,
      TOTAL,
      New_Labour_Supply
    )
)


# -------------------------   Include StatCan Data -------------------------
# Append the 2021-Census benchmark rows to the main distribution (only the
# 4-digit table gets them -- matches the SQL originals). bind_rows NA-pads
# the columns absent at source: the census table has no TTRAIN and no
# LCIP2_CRED, so census rows carry NA in both. Census rows also use 2-digit
# LCP4_CD values; consumers distinguish them via the Survey prefix (header).
# there'e no TTRAIN variable in the statcan data, this should coerce NA in that column, though.
labour_supply_distribution <- labour_supply_distribution %>%
  bind_rows(
    labour_supply_distribution_stat_can %>%
      transmute(
        Survey = SURVEY,
        PSSM_CREDENTIAL,
        PSSM_CRED,
        CURRENT_REGION_PSSM_CODE_ROLLUP,
        AGE_GROUP_ROLLUP,
        LCP4_CD,
        LCIP4_CRED,
        # LCIP2_CRED,
        COUNT,
        TOTAL,
        New_Labour_Supply = NEW_LABOUR_SUPPLY
      )
  )


# ------  create distribution for pdeg/law distribution ------
# These queries calculate New Labour Supply Distribution for Law/PDEG.
# The census PSSM_CREDENTIAL == PDEG / LCP4 "07" (law) rows are removed
# first, then a replacement distribution is derived from the Student
# Outcomes side: BGS respondents with a legal-professions baccalaureate
# (BACH, LCP4 starting "22" -- the pre-law population) supply the proxy
# signal, relabeled as PDEG / LCP4 "07". The juris-doctor completers
# themselves are never surveyed, so their participation term rides on the
# pre-law cohort's (same rationale as Module 07's proxy for GRAD).
labour_supply_distribution <- labour_supply_distribution |>
  filter(
    !(str_starts(Survey, '2021 Census') & # upper case SURVEY
      PSSM_CREDENTIAL == "PDEG" &
      str_starts(LCP4_CD, "07"))
  )

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

## ------------------------------------ Clean Up --------------------------------------------------
# Current workflow:
#  - Write key tables back to sql server.  These are tables needed for downstream work, or tables
# that might be needed for later reference outside of this analysis.
#  - Close DB connections
#  - Remove all other objects at the end of each script.
## ------------------------------------------------------------------------------------------------
# Write-back contract: each kept table lands in the PERSONAL schema
# (my_schema) as <name>_r, overwrite. Downstream consumers:
#   - 07-occupation-projections reads the four labour_supply_distribution*
#     tables back from my_schema (required-tables section there);
#   - 02b-3 consumes t_cohorts_recoded (with WEIGHT_NLS) and
#     tmp_tbl_weights_nls in-session when chained by the runner;
#   - 06 uses none of these.
# t_cohorts_recoded is intentionally re-written here even though 02b-1 also
# wrote it -- this write adds the WEIGHT_NLS column.

## ----------------------------------------------------------
## Reasons for change, other notes
## 2025 refresh: the list named six objects this script never
## creates (appso_graduates, t_dacso_data_part_1, trd_graduates
## and the three dbo lookups) -- standalone runs errored inside
## base::get, and chained runs rewrote stale loader copies to
## my_schema. Trimmed to what 02b-2 itself creates or modifies;
## 02b-3 gets the lookups from load-cohort-dacso in-session.
## ----------------------------------------------------------
tables_to_keep <- c(
  "labour_supply_distribution",
  "labour_supply_distribution_no_tt",
  "labour_supply_distribution_lcp2",
  "labour_supply_distribution_lcp2_no_tt",
  "t_cohorts_recoded",   # modified here: WEIGHT_NLS column added
  "tmp_tbl_weights_nls"  # created here
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
