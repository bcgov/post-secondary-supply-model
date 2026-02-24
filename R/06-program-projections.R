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
# Add near completers to projected and static distribution datasets
cohort_program_distributions_projected <- cohort_program_distributions_projected |>
  filter(!str_detect(PSSM_CRED, "^3 - "))
cohort_program_distributions_static <- cohort_program_distributions_static |>
  filter(!str_detect(PSSM_CRED, "^3 - "))

final_near_completers_mapped <- dacso_near_completers_ratios_age_at_grad_cip4_ttrain |>
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
  mutate(TOTALS = sum(COUNT, na.rm = TRUE)) |>
  ungroup() |>
  inner_join(
    tbl_age_groups_near_completers,
    by = join_by(AGE_GROUP == AGE_GROUP_LABEL_NEAR_COMPLETER_PROJECTION)
  ) |>
  transmute(
    SURVEY = "Program_Projections_2023-2024_qry_13d",
    PSSM_CREDENTIAL,
    PSSM_CRED,
    LCP4_CD,
    GRAD_STATUS = as.character(COSC_GRAD_STATUS_LGDS_CD_GROUP),
    TTRAIN = as.character(TTRAIN),
    LCIP4_CRED,
    LCIP2_CRED = paste(
      COSC_GRAD_STATUS_LGDS_CD_GROUP,
      str_sub(LCP4_CD, 1, 2),
      TTRAIN,
      PSSM_CREDENTIAL,
      sep = " - "
    ),
    AGE_GROUP = AGE_GROUP_LABEL_GRADUATE_PROJECTION,
    YEAR = "2023/2024",
    COUNT,
    TOTAL = TOTALS,
    PERCENT = if_else(TOTALS == 0, 0, as.numeric(COUNT) / as.numeric(TOTALS))
  )

cohort_program_distributions_projected <- bind_rows(
  cohort_program_distributions_projected,
  final_near_completers_mapped
)

cohort_program_distributions_static <- bind_rows(
  cohort_program_distributions_static,
  final_near_completers_mapped
)


# survey == 'Program_Projections_2023-2024_Q012e' (Static) ----
# Add program cohorts to static distribution datasets

# check NULL lcip2 codes - in the past many have been NULL for BACH
tbl_program_projection_input |>
  anti_join(
    infoware_l_cip_4digits_cip2016,
    by = join_by(FINAL_CIP_CODE_4 == LCP4_CD)
  ) |>
  distinct(FINAL_CIP_CODE_4, Count)

qry_dist_ratios <- t_cohorts_recoded |>
  inner_join(tbl_age_groups, by = join_by(AGE_GROUP == AGE_GROUP)) |>
  filter(GRAD_STATUS != "3", !is.na(TTRAIN), WEIGHT > 0) |>
  summarise(
    RATIO = sum(WEIGHT, na.rm = TRUE) /
      sum(t_cohorts_recoded$WEIGHT[t_cohorts_recoded$WEIGHT > 0], na.rm = TRUE),
    .by = c(PSSM_CREDENTIAL, LCP4_CD, GRAD_STATUS, AGE_GROUP_LABEL, TTRAIN)
  ) |>
  # Re-calculating ratio relative to the parent credential-CIP-age cohort
  mutate(
    RATIO = RATIO / sum(RATIO),
    .by = c(PSSM_CREDENTIAL, LCP4_CD, GRAD_STATUS, AGE_GROUP_LABEL)
  )

final_insert_data <- t_pssm_projection_cred_grp |>
  inner_join(
    tbl_program_projection_input,
    by = join_by(PSSM_PROJECTION_CREDENTIAL == PSI_CREDENTIAL_CATEGORY)
  ) |>
  inner_join(
    t_weights_stp,
    by = join_by(PSI_AWARD_SCHOOL_YEAR_DELAYED == YEAR_CODE)
  ) |>
  filter(
    MODEL == "2023-2024",
    !PSSM_CREDENTIAL %in%
      c('APPRAPPR', 'APPRCERT', 'GRCT or GRDP', 'PDEG', 'MAST', 'DOCT'),
    WEIGHT > 0
  ) |>
  # Initial aggregation of weighted projected counts
  summarise(
    WEIGHTED_BASE_COUNT = sum(Count * WEIGHT, na.rm = TRUE),
    .by = c(
      PSSM_CREDENTIAL,
      COSC_GRAD_STATUS_LGDS_CD,
      FINAL_CIP_CODE_4,
      AgeGroup
    )
  ) |>
  # Integration of historical distribution ratios for TTRAIN expansion
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
    STATUS_PREFIX = if_else(
      is.na(COSC_GRAD_STATUS_LGDS_CD),
      "",
      paste0(COSC_GRAD_STATUS_LGDS_CD, " - ")
    ),
    PSSM_CRED_VAL = paste0(STATUS_PREFIX, PSSM_CREDENTIAL),
    ADJUSTED_COUNT = if_else(
      is.na(RATIO),
      WEIGHTED_BASE_COUNT,
      WEIGHTED_BASE_COUNT * RATIO
    )
  ) |>
  mutate(
    # Window-based hierarchical total calculation (Denominator)
    TOTAL_FOR_PERCENT = sum(ADJUSTED_COUNT, na.rm = TRUE),
    .by = c(PSSM_CRED_VAL, AgeGroup)
  ) |>
  transmute(
    SURVEY = "Program_Projections_2023-2024_Q012e",
    PSSM_CREDENTIAL,
    PSSM_CRED = PSSM_CRED_VAL,
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
    YEAR = "2023/2024",
    COUNT = ADJUSTED_COUNT,
    TOTAL = TOTAL_FOR_PERCENT,
    PERCENT = if_else(TOTAL == 0, 0, as.numeric(COUNT) / as.numeric(TOTAL))
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

q013e_weighted_cohort_distribution <- t_pssm_projection_cred_grp |>
  filter(PSSM_CREDENTIAL %in% c('GRCT or GRDP', 'PDEG', 'MAST', 'DOCT')) |>
  inner_join(
    tbl_program_projection_input,
    by = join_by(PSSM_PROJECTION_CREDENTIAL == PSI_CREDENTIAL_CATEGORY)
  ) |>
  inner_join(
    t_weights_stp |> filter(MODEL == '2023-2024'),
    by = join_by(PSI_AWARD_SCHOOL_YEAR_DELAYED == YEAR_CODE)
  ) |>
  inner_join(
    qry_12_lcp4_lcippc_recode_9999,
    by = join_by(FINAL_CIP_CODE_4 == LCIP_LCP4_CD)
  ) |>
  filter(WEIGHT > 0) |>
  mutate(
    # Centralized referential key generation
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
  ) |>
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
  ) |>
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
  # Stage III: Hierarchical total derivation and percentage calculation
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
s_t <- dbReadTable(decimal_con, "Graduate_Projections")

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
    t_cohort_program_distributions_y2_to_y12,
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

dbExecute(decimal_con, qry_10a_Program_Dist_Count)
dbExecute(decimal_con, qry_10b_Program_Dist_Total)
dbExecute(decimal_con, qry_10c_Program_Dist_Distribution)
dbExecute(decimal_con, "DROP TABLE qry_10a_Program_Dist_Count")
dbExecute(decimal_con, "DROP TABLE qry_10b_Program_Dist_Total")

# survey == 'Program_Projections_2023-2024_qry12c' (Projected) ----
# adds projected counts to Cohort_Program_Distributions_Projected where PSSM_Credential IN ('GRCT or GRDP','PDEG','MAST','DOCT')
dbExecute(decimal_con, qry_12a_Program_Dist_Count)
dbExecute(decimal_con, qry_12b_Program_Dist_Total)
dbExecute(decimal_con, qry_12c_Program_Dist_Distribution)
dbExecute(decimal_con, "DROP TABLE qry_12a_Program_Dist_Count")
dbExecute(decimal_con, "DROP TABLE qry_12b_Program_Dist_Total")
dbExecute(decimal_con, "drop table qry_12_LCP4_LCIPPC_Recode_9999")
dbExecute(decimal_con, "drop table T_Predict_CIP_CRED_AGE_Flipped")

# check for combinations produced in static that were missed in the projected
dbGetQuery(decimal_con, qry_12d_Check_Missing)

# ---- Clean Up ----
# Lookups
dbExecute(decimal_con, "drop table AgeGroupLookup")
dbExecute(decimal_con, "drop table tbl_Age_Groups_Near_Completers")
dbExecute(decimal_con, "drop table tbl_Age_Groups")
dbExecute(decimal_con, "drop table T_Cohort_Program_Distributions_Y2_to_Y12")
dbExecute(decimal_con, "drop table T_APPR_Y2_to_Y10")
dbExecute(decimal_con, "drop table T_PSSM_Projection_Cred_Grp")
dbExecute(decimal_con, "drop table T_Weights_STP")

# Keep for next workflow
dbExistsTable(decimal_con, "Cohort_Program_Distributions_Projected")
dbExistsTable(decimal_con, "Cohort_Program_Distributions_Static")

# Keep in DB
dbExistsTable(decimal_con, "tbl_Program_Projection_Input")
