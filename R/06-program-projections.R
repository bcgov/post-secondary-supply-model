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

# This script creates static and projected distributions from several sources.
#  - Apprenticeship and TTRAIN distributions are derived from program cohort summaries
#    built in workflow 2b (T_Cohorts_Recoded)
#  - Near Completers distributions by age and CIP were summarized in workflow 3, the
#    source data is those students in the DACSO program survey cohort, who (did or did not?)
#    receive an earlier or later credential.
#  - the remainder are derived from Credential Non Dup table and tblCredential_HighestRank
#
# At a high level, the script:
#   Adds near completers to projected and static distribution data sets (Y1)
#   Adds program cohorts to static distribution data sets (Y1)
#   Adds masters and doctorates to static distribution data sets (Y1)
#   Adds apprenticeships to static and projected data sets (Y1)
#   Creates static distributions for apprenticeships and near-completers (Y2-12)
#   Creates projected distributions for apprenticeships and near-completers (Y2-12), holding Y2-12 constant.
#   Creates projected distributions all other credentials (Y2-Y12)
#     - uses R program written by Werner and adapted by Ian
#
# Includes: generally age groups are 17-19, 20-24, 25-30, 30-34, 35-44, 45-54, 55-64
# Year 1: 2019/2020
# Year 2+: 2020/2021 - 2030/2031
# Notes: Years need to be updated each model run.  Check we are projecting 12 years.  Also which age groupings
# will we be using?
# FIXME: lookups T_APPR_Y2_to_Y10 and T_Cohort_Program_Distributions_Y2_to_Y12 ID fields aren't sequential
#        keep eyes open for impacts of this.
#        04-graduate-projections: remove space in final table name, add survey column and populate

library(tidyverse)

# List of required tables for Derived Tables, Rollovers, and Lookups
required_tables <- c(
  # actually used in load script
  "tbl_credential_highest_rank",
  "credential_non_dup",

  # Rollovers from last run
  "cohort_program_distributions_projected",
  "cohort_program_distributions_static",

  # Lookups
  "agegrouplookup",
  "infoware_l_cip_4digits_cip2016",
  "infoware_l_cip_6digits_cip2016",
  "t_appr_y2_to_y10",
  "t_cohort_program_distributions_y2_to_y12",
  "t_pssm_projection_cred_grp",
  "t_weights_stp",
  "tbl_age_groups",
  "tbl_age_groups_near_completers",

  # Derived tables
  "tbl_program_projection_input",
  "qry_private_credentials_06d1_cohort_dist",
  "dacso_near_completers_ratios_age_at_grad_cip4_ttrain",
  "t_cohorts_recoded"
)
names(qry_private_credentials_06d1_cohort_dist)[2] <- "PSSM_CREDENTIAL"

missing <- required_tables[!sapply(required_tables, exists, where = .GlobalEnv)]

if (length(missing) > 0) {
  stop(paste(
    "The following required tables are missing from the environment:",
    paste(missing, collapse = ", ")
  ))
}

na_vals = c("", " ", "(Unspecified)", NA)

# ---- survey == "PTIB" (Static and Projected) ----
if (ptib_run == TRUE) {
  cohort_program_distributions_projected <- rbind(
    cohort_program_distributions_projected,
    qry_private_credentials_06d1_cohort_dist
  )
  cohort_program_distributions_static <- rbind(
    cohort_program_distributions_static,
    qry_private_credentials_06d1_cohort_dist
  )
}

# ---- survey == 'Program_Projections_2023-2024_qry_13d' (Static and Projected) ----
# calculate frequencies of near completers by age group and CIP
near_completers_totals <- dacso_near_completers_ratios_age_at_grad_cip4_ttrain |>
  group_by(
    PSSM_CREDENTIAL,
    PSSM_CRED,
    LCP4_CD,
    COSC_GRAD_STATUS_LGDS_CD_GROUP,
    TTRAIN,
    LCIP4_CRED,
    AGE_GROUP
  ) |>
  summarise(
    COUNT = sum(NEAR_COMPLETERS_STP_CREDENTIALS, na.rm = TRUE),
    .groups = "drop"
  ) |>
  group_by(PSSM_CRED, AGE_GROUP) |>
  mutate(TOTAL = sum(COUNT, na.rm = TRUE)) |>
  ungroup() |>
  mutate(
    PERCENT = if_else(TOTAL == 0, 0, as.numeric(COUNT) / as.numeric(TOTAL))
  )

# mapping age groups for near completers to match those used in (cohort program distributions?)
near_completers_totals_ages <- near_completers_totals |>
  inner_join(
    tbl_age_groups_near_completers,
    by = join_by(AGE_GROUP == AGE_GROUP_LABEL_NEAR_COMPLETER_PROJECTION)
  ) |>
  mutate(
    SURVEY = "Program_Projections_2023-2024_qry_13d",
    GRAD_STATUS = as.character(COSC_GRAD_STATUS_LGDS_CD_GROUP),
    TTRAIN = as.character(TTRAIN),
    LCIP2_CRED = paste(
      COSC_GRAD_STATUS_LGDS_CD_GROUP,
      str_sub(LCP4_CD, 1, 2),
      TTRAIN,
      PSSM_CREDENTIAL,
      sep = " - "
    ),
    AGE_GROUP = AGE_GROUP_LABEL_GRADUATE_PROJECTION,
    YEAR = "2023/2024"
  )

# add to projected and static distributions
cohort_program_distributions_projected <- cohort_program_distributions_projected |>
  filter(!str_detect(PSSM_CRED, "^3 - ")) |>
  bind_rows(near_completers_totals_ages)

cohort_program_distributions_static <- cohort_program_distributions_static |>
  filter(!str_detect(PSSM_CRED, "^3 - ")) |>
  bind_rows(near_completers_totals_ages)


# survey == 'Program_Projections_2023-2024_Q012e' (Static) ----
# check NULL lcip2 codes - in the past many have been NULL for BACH
tbl_program_projection_input |>
  anti_join(
    infoware_l_cip_4digits_cip2016,
    by = join_by(FINAL_CIP_CODE_4 == LCP4_CD)
  ) |>
  distinct(FINAL_CIP_CODE_4, Count)

# distribution ratios for program cohorts
qry_dist_ratios <- t_cohorts_recoded |>
  filter(GRAD_STATUS != "3", !is.na(TTRAIN), WEIGHT > 0) |>
  inner_join(tbl_age_groups, by = join_by(AGE_GROUP == AGE_GROUP)) |>
  summarise(
    RATIO = sum(WEIGHT, na.rm = TRUE) /
      sum(t_cohorts_recoded$WEIGHT[t_cohorts_recoded$WEIGHT > 0], na.rm = TRUE),
    .by = c(PSSM_CREDENTIAL, LCP4_CD, GRAD_STATUS, AGE_GROUP_LABEL, TTRAIN)
  ) |>
  mutate(
    RATIO = RATIO / sum(RATIO, na.rm = TRUE),
    .by = c(PSSM_CREDENTIAL, LCP4_CD, GRAD_STATUS, AGE_GROUP_LABEL)
  )

# weighted age, credential, grad status distributions for projected distribution
age_credential_weighted <- t_pssm_projection_cred_grp |>
  filter(
    !PSSM_CREDENTIAL %in%
      c('APPRAPPR', 'APPRCERT', 'GRCT or GRDP', 'PDEG', 'MAST', 'DOCT')
  ) |>
  inner_join(
    tbl_program_projection_input,
    by = join_by(PSSM_PROJECTION_CREDENTIAL == PSI_CREDENTIAL_CATEGORY)
  ) |>
  inner_join(
    t_weights_stp |> filter(MODEL == "2023-2024", WEIGHT > 0),
    by = join_by(PSI_AWARD_SCHOOL_YEAR_DELAYED == YEAR_CODE)
  ) |>
  summarise(
    WEIGHTED_BASE_COUNT = sum(Count * WEIGHT, na.rm = TRUE),
    .by = c(
      PSSM_CREDENTIAL,
      COSC_GRAD_STATUS_LGDS_CD,
      FINAL_CIP_CODE_4,
      AgeGroup
    )
  )

age_credential_weighted_adjusted <- age_credential_weighted |>
  left_join(
    qry_dist_ratios,
    by = join_by(
      PSSM_CREDENTIAL,
      FINAL_CIP_CODE_4 == LCP4_CD,
      COSC_GRAD_STATUS_LGDS_CD == GRAD_STATUS,
      AgeGroup == AGE_GROUP_LABEL
    )
  ) |>
  mutate(
    COUNT = if_else(
      is.na(RATIO),
      WEIGHTED_BASE_COUNT,
      WEIGHTED_BASE_COUNT * RATIO
    )
  )

age_credential_weighted_adjusted2 <- age_credential_weighted_adjusted |>
  mutate(
    STATUS_PREFIX = if_else(
      is.na(COSC_GRAD_STATUS_LGDS_CD),
      "",
      paste0(COSC_GRAD_STATUS_LGDS_CD, " - ")
    ),
    PSSM_CRED = paste0(STATUS_PREFIX, PSSM_CREDENTIAL),
  ) |>
  mutate(
    TOTAL = sum(COUNT, na.rm = TRUE),
    .by = c(PSSM_CRED, AgeGroup)
  ) |>
  mutate(
    PERCENT = if_else(TOTAL == 0, 0, as.numeric(COUNT) / as.numeric(TOTAL))
  )

final_insert_data <- age_credential_weighted_adjusted2 |>
  mutate(
    SURVEY = "Program_Projections_2023-2024_Q012e",
    LCP4_CD = FINAL_CIP_CODE_4,
    GRAD_STATUS = as.character(COSC_GRAD_STATUS_LGDS_CD),
    TTRAIN = as.character(TTRAIN),
    LCIP4_CRED = paste0(
      STATUS_PREFIX,
      FINAL_CIP_CODE_4,
      " - ",
      if_else(is.na(TTRAIN), "", paste0(TTRAIN, " - ")),
      PSSM_CREDENTIAL
    ),
    LCIP2_CRED = paste0(
      STATUS_PREFIX,
      str_sub(FINAL_CIP_CODE_4, 1, 2),
      " - ",
      if_else(is.na(TTRAIN), "", paste0(TTRAIN, " - ")),
      PSSM_CREDENTIAL
    ),
    AGE_GROUP = AgeGroup,
    YEAR = "2023/2024"
  ) |>
  select(
    -c(
      COSC_GRAD_STATUS_LGDS_CD,
      FINAL_CIP_CODE_4,
      AgeGroup,
      RATIO,
      WEIGHTED_BASE_COUNT,
      STATUS_PREFIX
    )
  )

cohort_program_distributions_static <- cohort_program_distributions_static |>
  filter(!str_detect(SURVEY, "Q012e$")) |>
  bind_rows(final_insert_data)


# survey == 'Program_Projections_2023-2024_Q013e' (Static) ----
# Add masters and doctorates to static distribution datasets
# Note: lcip4_cd showing as 2D for masters and doct - cluster.
# (same in prior model runs)

qry_12_lcp4_lcippc_recode_9999 <- infoware_l_cip_6digits_cip2016 |>
  mutate(
    LCIP_LCIPPC_CD = if_else(LCIP_LCP4_CD == "9999", "99", LCIP_LCIPPC_CD)
  ) |>
  distinct(LCIP_LCP4_CD, LCIP_LCIPPC_CD)


# weighted age, credential, grad status distributions for projected distribution
graduate_age_credential <- t_pssm_projection_cred_grp |>
  filter(PSSM_CREDENTIAL %in% c('GRCT or GRDP', 'PDEG', 'MAST', 'DOCT')) |>
  inner_join(
    tbl_program_projection_input,
    by = join_by(PSSM_PROJECTION_CREDENTIAL == PSI_CREDENTIAL_CATEGORY)
  ) |>
  inner_join(
    t_weights_stp |> filter(MODEL == '2023-2024', WEIGHT > 0),
    by = join_by(PSI_AWARD_SCHOOL_YEAR_DELAYED == YEAR_CODE)
  ) |>
  inner_join(
    qry_12_lcp4_lcippc_recode_9999,
    by = join_by(FINAL_CIP_CODE_4 == LCIP_LCP4_CD)
  ) |>
  mutate(
    STATUS_PREFIX = if_else(
      is.na(COSC_GRAD_STATUS_LGDS_CD),
      "",
      paste0(COSC_GRAD_STATUS_LGDS_CD, " - ")
    ),
    PSSM_CRED_TMP = paste0(STATUS_PREFIX, PSSM_CREDENTIAL),
    LCIP_CRED_TMP = paste0(
      STATUS_PREFIX,
      LCIP_LCIPPC_CD,
      " - ",
      PSSM_CREDENTIAL
    )
  )

# weight the distributions and calculate totals and percentages
graduate_age_credential_weighted <- graduate_age_credential |>
  summarise(
    COUNT = sum(Count * WEIGHT, na.rm = TRUE), # Aggregated weighted volume
    .by = c(
      PSSM_CREDENTIAL,
      PSSM_CRED_TMP,
      LCIP_LCIPPC_CD,
      LCIP_CRED_TMP,
      AgeGroup
    )
  ) |>
  mutate(
    TOTAL = sum(COUNT, na.rm = TRUE), # Hierarchical total via window function
    PERCENT = if_else(TOTAL == 0, 0, as.numeric(COUNT) / as.numeric(TOTAL)),
    .by = c(PSSM_CRED_TMP, AgeGroup)
  )

# create final dataset for insertion, mapping to cohort program distribution
q013e_weighted_cohort_distribution2 <- graduate_age_credential_weighted |>
  transmute(
    SURVEY = "Program_Projections_2023-2024_Q013e",
    PSSM_CREDENTIAL,
    PSSM_CRED = PSSM_CRED_TMP,
    LCP4_CD = LCIP_LCIPPC_CD,
    LCIP4_CRED = LCIP_CRED_TMP,
    AGE_GROUP = AgeGroup,
    YEAR = "2023/2024",
    COUNT,
    TOTAL,
    PERCENT
  )

cohort_program_distributions_static <- cohort_program_distributions_static |>
  filter(!str_detect(SURVEY, "Q013e$")) |>
  bind_rows(q013e_weighted_cohort_distribution)

# survey == 'Program_Projections_2023-2024_Q014e' (Static and Projected) ----
# adds apprenticeships to static and projected datasets
q014e_weighted_cohort_distribution <- t_cohorts_recoded |>
  inner_join(tbl_age_groups, by = join_by(AGE_GROUP == AGE_GROUP)) |>
  filter(PSSM_CREDENTIAL %in% c('APPRAPPR', 'APPRCERT'), WEIGHT > 0) |>
  # Stage I & II: Granular weighted aggregation
  summarise(
    COUNT = sum(WEIGHT, na.rm = TRUE),
    .by = c(
      PSSM_CREDENTIAL,
      LCP4_CD,
      TTRAIN,
      LCIP4_CRED,
      LCIP2_CRED,
      AGE_GROUP_LABEL
    )
  ) |>
  mutate(
    TOTAL = sum(COUNT, na.rm = TRUE),
    PERCENT = if_else(TOTAL == 0, 0, as.numeric(COUNT) / as.numeric(TOTAL)),
    .by = c(PSSM_CREDENTIAL, AGE_GROUP_LABEL)
  ) |>
  transmute(
    SURVEY = "Program_Projections_2023-2024_Q014e",
    PSSM_CREDENTIAL,
    PSSM_CRED = PSSM_CREDENTIAL,
    LCP4_CD,
    LCIP4_CRED,
    LCIP2_CRED,
    AGE_GROUP = AGE_GROUP_LABEL,
    YEAR = "2023/2024",
    COUNT,
    TOTAL,
    PERCENT
  )

cohort_program_distributions_projected <- cohort_program_distributions_projected |>
  filter(!str_detect(SURVEY, "Q014e$")) |>
  bind_rows(q014e_weighted_cohort_distribution)

cohort_program_distributions_static <- cohort_program_distributions_static |>
  filter(!str_detect(SURVEY, "Q014e$")) |>
  bind_rows(q014e_weighted_cohort_distribution)

# --- move this to graduate_projections.R, I think ---
# expands static appr in graduate projections - holding counts constant
graduate_projections <- dbReadTable(decimal_con, "Graduate_Projections")

new_projections <- graduate_projections |>
  filter(SURVEY == "APPSO") |>
  inner_join(
    t_appr_y2_to_y10,
    by = join_by(YEAR == Y1)
  ) |>
  transmute(
    SURVEY,
    PSSM_CREDENTIAL,
    PSSM_CRED,
    AGE_GROUP,
    YEAR = Y2_TO_Y10,
    GRADUATES
  )

graduate_projections <- graduate_projections |>
  bind_rows(new_projections)

# survey == 'Program_Projections_2023-2024_Q015e21' (Static and Projected) ----
# expands apprenticeships and near-completers to include 2020+12YR where
#  survey == Program_Projections_2023-2024_qry_13d
#  survey == Program_Projections_2023-2024_Q014e

temporal_expansion_base <- cohort_program_distributions_static |>
  inner_join(
    t_cohort_program_distributions_y2_to_y12 |> select(-ID),
    by = join_by(YEAR == Y1)
  )

q015e21_projected_expansion <- temporal_expansion_base |>
  filter(
    PSSM_CRED %in% c('APPRAPPR', 'APPRCERT') | str_starts(PSSM_CRED, "3 - ")
  ) |>
  transmute(
    SURVEY = "Program_Projections_2023-2024_Q015e21",
    PSSM_CREDENTIAL,
    PSSM_CRED,
    LCP4_CD,
    GRAD_STATUS,
    TTRAIN,
    LCIP4_CRED,
    LCIP2_CRED,
    AGE_GROUP,
    YEAR = Y2_TO_Y10,
    COUNT,
    TOTAL,
    PERCENT
  )

q015e22_static_expansion <- temporal_expansion_base |>
  transmute(
    SURVEY = "Program_Projections_2023-2024_Q015e22",
    PSSM_CREDENTIAL,
    PSSM_CRED,
    LCP4_CD,
    GRAD_STATUS,
    TTRAIN,
    LCIP4_CRED,
    LCIP2_CRED,
    AGE_GROUP,
    YEAR = Y2_TO_Y10,
    COUNT,
    TOTAL,
    PERCENT
  )

# note to self - let's take a closer look at what is happening here and make
# sure it is explainable and makes sense.  
cohort_program_distributions_projected <- cohort_program_distributions_projected |>
  filter(!str_detect(SURVEY, "Q015e21$")) |>
  bind_rows(q015e21_projected_expansion)

cohort_program_distributions_static <- cohort_program_distributions_static |>
  filter(!str_detect(SURVEY, "Q015e22$")) |>
  bind_rows(q015e22_static_expansion)

# Werner program ----
input_data <- tbl_program_projection_input %>%
  select(-Expr1) %>%
  complete(
    AgeGroup,
    PSI_CREDENTIAL_CATEGORY,
    FINAL_CIP_CODE_4,
    PSI_AWARD_SCHOOL_YEAR_DELAYED,
    fill = list(Count = 0)
  ) %>%
  pivot_wider(
    names_from = "PSI_AWARD_SCHOOL_YEAR_DELAYED",
    values_from = "Count"
  ) %>%
  rename(
    "CIP" = "FINAL_CIP_CODE_4",
    "AGE" = "AgeGroup",
    "CRED" = "PSI_CREDENTIAL_CATEGORY"
  ) %>%
  select(CIP, CRED, AGE, 4:ncol(.)) %>%
  arrange(CIP, CRED, AGE)

write_csv(
  input_data,
  "./tmp/input-data.csv"
)

## run Werner program ----
source(glue::glue("./R/program projections.R"))

output_data <- read_delim(
  glue::glue("./tmp/output.csv"),
  delim = "\t",
  col_names = TRUE
)
names(output_data) <- paste0(2023:(2023 + 11), "/", 2024:(2024 + 11))

t_predict_cip_cred_age <- cbind(input_data, output_data)

t_predict_cip_cred_age_flipped <- t_predict_cip_cred_age %>%
  pivot_longer(-c(CIP, CRED, AGE), names_to = "Year", values_to = "Count") %>%
  filter(
    Year %in%
      c(
        '2023/2024',
        '2024/2025',
        '2025/2026',
        '2026/2027',
        '2027/2028',
        '2028/2029',
        '2029/2030',
        '2030/2031',
        '2031/2032',
        '2032/2033',
        '2033/2034',
        '2034/2035'
      )
  )

t_predict_cip_cred_age_flipped |>
  summarise(
    SUMOFCOUNT = sum(Count, na.rm = TRUE),
    .by = Year
  )


# survey == 'Program_Projections_2023-2024_qry10c' (Projected) ----
# adds projected counts to Cohort_Program_Distributions_Projected where PSSM_Credential NOT IN ('GRCT or GRDP','PDEG','MAST','DOCT')
# (ALSO NOT IN ('APPRAPPR','APPRCERT') as these were done earlier)
cohort_program_distributions_projected <- cohort_program_distributions_projected |>
  filter(
    PSSM_CRED %in%
      c('APPRAPPR', 'APPRCERT') |
      str_starts(PSSM_CRED, "3 -") |
      str_starts(PSSM_CRED, "P -")
  )

qry_10c_program_dist_distribution <- t_predict_cip_cred_age_flipped |>
  # Stage I: Referential Integration
  inner_join(
    t_pssm_projection_cred_grp,
    by = join_by(CRED == PSSM_PROJECTION_CREDENTIAL)
  ) |>
  # Restrictive filtering pursuant to categorical exclusion protocols
  filter(
    !PSSM_CREDENTIAL %in%
      c('APPRAPPR', 'APPRCERT', 'GRCT or GRDP', 'PDEG', 'MAST', 'DOCT')
  ) |>
  # Stage II: Derived Attribute Generation (Concatenation Logic)
  mutate(
    STATUS_PREFIX = if_else(
      is.na(COSC_GRAD_STATUS_LGDS_CD),
      "",
      paste0(COSC_GRAD_STATUS_LGDS_CD, " - ")
    ),
    PSSM_CRED_TMP = paste0(STATUS_PREFIX, PSSM_CREDENTIAL),
    LCIP4_CRED_TMP = paste0(STATUS_PREFIX, CIP, " - ", PSSM_CREDENTIAL),
    LCIP2_CRED_TMP = paste0(
      STATUS_PREFIX,
      str_sub(CIP, 1, 2),
      " - ",
      PSSM_CREDENTIAL
    )
  ) |>
  # Stage III: Granular Aggregation (Replacing qry_10a)
  summarise(
    COUNT_VAL = sum(Count, na.rm = TRUE),
    .by = c(
      PSSM_CREDENTIAL,
      PSSM_CRED_TMP,
      CIP,
      LCIP4_CRED_TMP,
      LCIP2_CRED_TMP,
      AGE,
      Year
    )
  ) |>
  # Stage IV: Window-Based Denominator and Percent Calculation (Replacing qry_10b & 10c)
  mutate(
    TOTAL_VAL = sum(COUNT_VAL, na.rm = TRUE),
    PERCENT_VAL = if_else(
      TOTAL_VAL == 0,
      0,
      as.numeric(COUNT_VAL) / as.numeric(TOTAL_VAL)
    ),
    .by = c(PSSM_CRED_TMP, AGE, Year)
  ) |>
  # Stage V: Terminal Schema Mapping
  transmute(
    SURVEY = "Program_Projections_2023-2024_qry10c",
    PSSM_CREDENTIAL,
    PSSM_CRED = PSSM_CRED_TMP,
    LCP4_CD = CIP,
    LCIP4_CRED = LCIP4_CRED_TMP,
    LCIP2_CRED = LCIP2_CRED_TMP,
    AGE_GROUP = AGE,
    YEAR = Year,
    COUNT = COUNT_VAL,
    TOTAL = TOTAL_VAL,
    PERCENT = PERCENT_VAL
  )

# Longitudinal Repository Reconciliation
cohort_program_distributions_projected <- cohort_program_distributions_projected |>
  filter(!str_detect(SURVEY, "qry10c$")) |>
  bind_rows(qry_10c_program_dist_distribution)


# survey == 'Program_Projections_2023-2024_qry12c' (Projected) ----
# adds projected counts to Cohort_Program_Distributions_Projected where PSSM_Credential IN ('GRCT or GRDP','PDEG','MAST','DOCT')

q12c_program_dist_distribution <- t_predict_cip_cred_age_flipped |>
  inner_join(
    t_pssm_projection_cred_grp,
    by = join_by(CRED == PSSM_PROJECTION_CREDENTIAL)
  ) |>
  inner_join(
    qry_12_lcp4_lcippc_recode_9999,
    by = join_by(CIP == LCIP_LCP4_CD)
  ) |>
  # Stage II: Categorical Restriction Protocol
  filter(
    PSSM_CREDENTIAL %in% c('GRCT or GRDP', 'PDEG', 'MAST', 'DOCT')
  ) |>
  # Stage III: Concatenation Logic and Derived Attribute Generation
  mutate(
    STATUS_PREFIX = if_else(
      is.na(COSC_GRAD_STATUS_LGDS_CD),
      "",
      paste0(COSC_GRAD_STATUS_LGDS_CD, " - ")
    ),
    PSSM_CRED_TMP = paste0(STATUS_PREFIX, PSSM_CREDENTIAL),
    LCIPPC_CD_TMP = LCIP_LCIPPC_CD,
    LCIPPC_CRED_TMP = paste0(
      STATUS_PREFIX,
      LCIP_LCIPPC_CD,
      " - ",
      PSSM_CREDENTIAL
    )
  ) |>
  # Stage IV: Aggregation and Hierarchical Denominator Derivation
  # Executes the primary summation (Q12a) and windowed total calculation (Q12b)
  summarise(
    COUNT_VAL = sum(Count, na.rm = TRUE),
    .by = c(
      PSSM_CREDENTIAL,
      PSSM_CRED_TMP,
      LCIPPC_CD_TMP,
      LCIPPC_CRED_TMP,
      AGE,
      Year
    )
  ) |>
  mutate(
    TOTAL_VAL = sum(COUNT_VAL, na.rm = TRUE),
    PERCENT_VAL = if_else(
      TOTAL_VAL == 0,
      0,
      as.numeric(COUNT_VAL) / as.numeric(TOTAL_VAL)
    ),
    .by = c(PSSM_CRED_TMP, AGE, Year)
  ) |>
  # Stage V: Alignment with Administrative Schema (Cohort_Program_Distributions_Projected)
  transmute(
    SURVEY = "Program_Projections_2023-2024_qry12c",
    PSSM_CREDENTIAL,
    PSSM_CRED = PSSM_CRED_TMP,
    LCP4_CD = LCIPPC_CD_TMP,
    LCIP4_CRED = LCIPPC_CRED_TMP,
    AGE_GROUP = AGE,
    YEAR = Year,
    COUNT = COUNT_VAL,
    TOTAL = TOTAL_VAL,
    PERCENT = PERCENT_VAL
  )

cohort_program_distributions_projected <- cohort_program_distributions_projected |>
  filter(!str_detect(SURVEY, "qry12c$")) |>
  bind_rows(q12c_program_dist_distribution)


# check for combinations produced in static that were missed in the projected
cohort_program_distributions_static |>
  filter(!AGE_GROUP %in% c('15 to 16', '65 to 89')) |>
  anti_join(
    cohort_program_distributions_projected,
    by = join_by(
      YEAR,
      AGE_GROUP,
      LCP4_CD,
      PSSM_CRED,
      PSSM_CREDENTIAL
    )
  ) |>
  select(
    PSSM_CREDENTIAL,
    PSSM_CRED,
    LCP4_CD,
    LCIP4_CRED,
    AGE_GROUP,
    YEAR,
    COUNT
  )


# ---- Clean Up ----
tables_to_keep <- c(
  "cohort_program_distributions_projected",
  "cohort_program_distributions_static",
  "graduate_projections",
  "tbl_program_projection_input"
)

rm(list = setdiff(ls(), tables_to_keep))
