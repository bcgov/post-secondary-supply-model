# Copyright 2026 Province of British Columbia
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations under the License.

# IMPORTANT!!! THE NEXT SECTION CAN ONLY BE RUN AFTER THE PROGRAM MATCHING WORK HAS BEEN DONE

# ---- 20 Final Distributions ----
# NOTE: Exclude_CIPs queries end up with Invalid column name 'FINAL_CIP_CLUSTER_CODE'.
credential_by_year_age_group <- tbl_credential_highest_rank |>
  inner_join(age_group_lookup, by = c("AGE_GROUP_AT_GRAD" = "AgeIndex")) |> #filters out invalid ages
  filter(PSI_CREDENTIAL_CATEGORY != "Apprenticeship") |>
  group_by(AgeGroup, PSI_CREDENTIAL_CATEGORY, PSI_AWARD_SCHOOL_YEAR_DELAYED) |>
  summarise(Count = n(), .groups = "drop") |>
  arrange(AgeGroup, PSI_CREDENTIAL_CATEGORY, PSI_AWARD_SCHOOL_YEAR_DELAYED)

# Exclude CIP clusters 09 and 10
credential_by_year_age_group_exclude_cips <- tbl_credential_highest_rank |>
  inner_join(age_group_lookup, by = c("AGE_GROUP_AT_GRAD" = "AgeIndex")) |>
  inner_join(
    credential_non_dup |> select(id, FINAL_CIP_CLUSTER_CODE),
    by = "id"
  ) |>
  filter(
    PSI_CREDENTIAL_CATEGORY != "Apprenticeship",
    FINAL_CIP_CLUSTER_CODE != "09",
    FINAL_CIP_CLUSTER_CODE != "10"
  ) |>
  group_by(AgeGroup, PSI_CREDENTIAL_CATEGORY, PSI_AWARD_SCHOOL_YEAR_DELAYED) |>
  summarise(Count = n(), .groups = "drop") |>
  arrange(AgeGroup, PSI_CREDENTIAL_CATEGORY, PSI_AWARD_SCHOOL_YEAR_DELAYED)

# Domestic only
credential_by_year_age_group_domestic <- tbl_credential_highest_rank |>
  inner_join(age_group_lookup, by = c("AGE_GROUP_AT_GRAD" = "AgeIndex")) |>
  filter(
    PSI_CREDENTIAL_CATEGORY != "Apprenticeship",
    PSI_VISA_STATUS == "Domestic" | is.na(PSI_VISA_STATUS)
  ) |>
  group_by(AgeGroup, PSI_CREDENTIAL_CATEGORY, PSI_AWARD_SCHOOL_YEAR_DELAYED) |>
  summarise(Count = n(), .groups = "drop") |>
  arrange(AgeGroup, PSI_CREDENTIAL_CATEGORY, PSI_AWARD_SCHOOL_YEAR_DELAYED)

# Domestic only, exclude CIPs
credential_by_year_age_group_domestic_exclude_cips <- tbl_credential_highest_rank |>
  inner_join(age_group_lookup, by = c("AGE_GROUP_AT_GRAD" = "AgeIndex")) |>
  inner_join(
    credential_non_dup |> select(id, FINAL_CIP_CLUSTER_CODE),
    by = "id"
  ) |>
  filter(
    PSI_CREDENTIAL_CATEGORY != "Apprenticeship",
    (PSI_VISA_STATUS == "Domestic" | is.na(PSI_VISA_STATUS)),
    FINAL_CIP_CLUSTER_CODE != "09",
    FINAL_CIP_CLUSTER_CODE != "10"
  ) |>
  group_by(AgeGroup, PSI_CREDENTIAL_CATEGORY, PSI_AWARD_SCHOOL_YEAR_DELAYED) |>
  summarise(Count = n(), .groups = "drop") |>
  arrange(AgeGroup, PSI_CREDENTIAL_CATEGORY, PSI_AWARD_SCHOOL_YEAR_DELAYED)

# Domestic only, exclude research universities and DACSO
credential_by_year_age_group_domestic_exclude_ru_dacso <- tbl_credential_highest_rank |>
  inner_join(age_group_lookup, by = c("AGE_GROUP_AT_GRAD" = "AgeIndex")) |>
  filter(
    PSI_CREDENTIAL_CATEGORY != "Apprenticeship",
    PSI_VISA_STATUS == "Domestic" | is.na(PSI_VISA_STATUS),
    is.na(RESEARCH_UNIVERSITY) |
      (RESEARCH_UNIVERSITY == 1 & OUTCOMES_CRED != 'DACSO')
  ) |>
  group_by(AgeGroup, PSI_CREDENTIAL_CATEGORY, PSI_AWARD_SCHOOL_YEAR_DELAYED) |>
  summarise(Count = n(), .groups = "drop") |>
  arrange(AgeGroup, PSI_CREDENTIAL_CATEGORY, PSI_AWARD_SCHOOL_YEAR_DELAYED)

# CIP4, AgeGroup, Domestic, Exclude RU & DACSO, Exclude CIPs
credential_by_year_cip4_agegroup_domestic_exclude_ru_dacso_exclude_cips <- tbl_credential_highest_rank |>
  inner_join(age_group_lookup, by = c("AGE_GROUP_AT_GRAD" = "AgeIndex")) |>
  inner_join(
    credential_non_dup |> select(id, FINAL_CIP_CLUSTER_CODE, FINAL_CIP_CODE_4),
    by = "id"
  ) |>
  filter(
    PSI_CREDENTIAL_CATEGORY != "Apprenticeship",
    PSI_VISA_STATUS == "Domestic" | is.na(PSI_VISA_STATUS),
    is.na(RESEARCH_UNIVERSITY) |
      (RESEARCH_UNIVERSITY == 1 & OUTCOMES_CRED != 'DACSO'),
    FINAL_CIP_CLUSTER_CODE != "09",
    FINAL_CIP_CLUSTER_CODE != "10"
  ) |>
  group_by(
    FINAL_CIP_CODE_4,
    AgeGroup,
    PSI_CREDENTIAL_CATEGORY,
    PSI_AWARD_SCHOOL_YEAR_DELAYED
  ) |>
  summarise(Count = n(), .groups = "drop") |>
  arrange(
    FINAL_CIP_CODE_4,
    AgeGroup,
    PSI_CREDENTIAL_CATEGORY,
    PSI_AWARD_SCHOOL_YEAR_DELAYED
  )

# CIP4, Gender, AgeGroup, Domestic, Exclude RU & DACSO, Exclude CIPs
credential_by_year_cip4_gender_agegroup_domestic_exclude_ru_dacso_exclude_cips <- tbl_credential_highest_rank |>
  inner_join(age_group_lookup, by = c("AGE_GROUP_AT_GRAD" = "AgeIndex")) |>
  inner_join(
    credential_non_dup |> select(id, FINAL_CIP_CLUSTER_CODE, FINAL_CIP_CODE_4),
    by = "id"
  ) |>
  filter(
    PSI_CREDENTIAL_CATEGORY != "Apprenticeship",
    (PSI_VISA_STATUS == "Domestic" | is.na(PSI_VISA_STATUS)),
    is.na(RESEARCH_UNIVERSITY) |
      (RESEARCH_UNIVERSITY == 1 & OUTCOMES_CRED != 'DACSO'),
    FINAL_CIP_CLUSTER_CODE != "09",
    FINAL_CIP_CLUSTER_CODE != "10"
  ) |>
  group_by(
    FINAL_CIP_CODE_4,
    psi_gender_cleaned,
    AgeGroup,
    PSI_CREDENTIAL_CATEGORY,
    PSI_AWARD_SCHOOL_YEAR_DELAYED
  ) |>
  summarise(Count = n(), .groups = "drop") |>
  arrange(
    FINAL_CIP_CODE_4,
    psi_gender_cleaned,
    AgeGroup,
    PSI_CREDENTIAL_CATEGORY,
    PSI_AWARD_SCHOOL_YEAR_DELAYED
  )

# Gender, AgeGroup, Domestic, Exclude CIPs
credential_by_year_gender_agegroup_domestic_exclude_cips <- tbl_credential_highest_rank |>
  inner_join(age_group_lookup, by = c("AGE_GROUP_AT_GRAD" = "AgeIndex")) |>
  inner_join(
    credential_non_dup |> select(id, FINAL_CIP_CLUSTER_CODE),
    by = "id"
  ) |>
  filter(
    PSI_CREDENTIAL_CATEGORY != "Apprenticeship",
    PSI_VISA_STATUS == "Domestic" | is.na(PSI_VISA_STATUS),
    FINAL_CIP_CLUSTER_CODE != "09",
    FINAL_CIP_CLUSTER_CODE != "10"
  ) |>
  group_by(
    psi_gender_cleaned,
    AgeGroup,
    PSI_CREDENTIAL_CATEGORY,
    PSI_AWARD_SCHOOL_YEAR_DELAYED
  ) |>
  summarise(Count = n(), .groups = "drop") |>
  arrange(
    psi_gender_cleaned,
    AgeGroup,
    PSI_CREDENTIAL_CATEGORY,
    PSI_AWARD_SCHOOL_YEAR_DELAYED
  )

# Gender, AgeGroup, Domestic, Exclude RU & DACSO, Exclude CIPs
credential_by_year_gender_agegroup_domestic_exclude_ru_dacso_exclude_cips <- tbl_credential_highest_rank |>
  inner_join(age_group_lookup, by = c("AGE_GROUP_AT_GRAD" = "AgeIndex")) |>
  inner_join(
    credential_non_dup |> select(id, FINAL_CIP_CLUSTER_CODE),
    by = "id"
  ) |>
  filter(
    PSI_CREDENTIAL_CATEGORY != "Apprenticeship",
    (PSI_VISA_STATUS == "Domestic" | is.na(PSI_VISA_STATUS)),
    is.na(RESEARCH_UNIVERSITY) |
      (RESEARCH_UNIVERSITY == 1 & OUTCOMES_CRED != 'DACSO'),
    FINAL_CIP_CLUSTER_CODE != "09",
    FINAL_CIP_CLUSTER_CODE != "10"
  ) |>
  group_by(
    psi_gender_cleaned,
    AgeGroup,
    PSI_CREDENTIAL_CATEGORY,
    PSI_AWARD_SCHOOL_YEAR_DELAYED
  ) |>
  summarise(Count = n(), .groups = "drop") |>
  arrange(
    psi_gender_cleaned,
    AgeGroup,
    PSI_CREDENTIAL_CATEGORY,
    PSI_AWARD_SCHOOL_YEAR_DELAYED
  )

# ---- Final Distributions ----
# From 01d-enrolment-analysis.R
# SQL version starts at line 283 on branch main
# Replicates: qry09c_ series
# What the code does: Execute relational joins to generate standardized gender-age cohorts and academic datasets.
# Followed by frequency analysis of enrollment metrics stratified by residency status, credential category, and field of study.
# BA Notes: The original SQL filters on a single school year, but the filter is commented out.

qry09c_MinEnrolment_by_Credential_and_CIP_Code <- min_enrolment |>
  count(PSI_SCHOOL_YEAR, PSI_CREDENTIAL_CATEGORY, PSI_CIP_CODE, name = "Expr1")

qry09c_MinEnrolment_Domestic <- min_enrolment |>
  inner_join(age_group_lookup, by = c("AGE_GROUP_ENROL_DATE" = "AgeIndex")) |>
  filter(PSI_VISA_STATUS == 'Domestic') |>
  count(PSI_GENDER, PSI_SCHOOL_YEAR, AgeGroup, name = "Expr1")

qry09c_MinEnrolment <- min_enrolment |>
  inner_join(age_group_lookup, by = c("AGE_GROUP_ENROL_DATE" = "AgeIndex")) |>
  mutate("Groups" = paste0(PSI_GENDER, AgeGroup)) |>
  count(PSI_GENDER, PSI_SCHOOL_YEAR, Groups, name = "Expr1")

# refine this list as we refactor
tables_to_keep <- c(
  "stp_enrolment",
  "stp_credential",
  "stp_enrolment_record_type",
  "stp_credential_record_type",
  "stp_enrolment_valid",
  "age_group_lookup",
  "credential_rank",
  "credential",
  "credential_non_dup",
  "credential_sup_vars",
  "tbl_credential_highest_rank",
  "tbl_credential_delay_effect",
  "outcome_credential",
  "min_enrolment",
  "qry09c_MinEnrolment_by_Credential_and_CIP_Code",
  "qry09c_MinEnrolment_Domestic",
  "qry09c_MinEnrolment"
)

rm(list = setdiff(ls(), tables_to_keep))
