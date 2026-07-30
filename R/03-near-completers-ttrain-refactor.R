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

# Refactor of 03-near-completers-ttrain.R.
#
# WHAT THIS SCRIPT PRODUCES
#   Five "near-completer ratio" tables. A near-completer is a DACSO respondent
#   who left a program without finishing at the surveyed institution
#   (cosc_grad_status_lgds_cd_group == "3"). Some of them later earned a
#   credential elsewhere (recorded in the STP credential data). These tables
#   estimate, for each slice (age / gender / CIP / credential / year / trades),
#   the ratio of "true" near-completers to completers, which downstream script
#   04 uses to convert projected near-completers into projected graduates.
#
# THE CORE RATIO (built the same way in every block):
#   numerator   = near-completers  -  near-completers who already hold an STP
#                                      credential (they effectively completed)
#   denominator = completers
#   ratio       = numerator / denominator
#
# Removed (verified not to affect the final tables):
#   - match_credential / match_cip_code_4 / match_award_school_year / match_inst /
#     final_consider_a_match / match_summary_table  (computed, never used downstream)
#   - the stp_credential_awarded_before/after split  (collapses to "any match")
#   - the multiple-credential winner resolution       (flag is binary per student)
#   - t_dacso_data_part_1_tempselection                (two no-op inner joins)
#   - interactive pivot_wider "check" displays

library(tidyverse)
library(glue)
library(DBI)
library(odbc)
library(config)

source("R/utils.R")

# ---- Connection / schema ----
# my_schema is the analyst's own IDIR schema (e.g. "IDIR\JDUAN"); never hardcoded.
my_schema <- config::get("myschema")

# Only open a connection if one isn't already in the global env. When this script
# is sourced after earlier pipeline steps, `con` usually already exists, so we
# reuse it (and leave it open for the next script). con_created tracks whether WE
# opened it, so the Clean Up section only disconnects what it created.
con_created <- !exists("con", where = .GlobalEnv)
if (con_created) {
  db_config <- config::get("decimal")
  con <- dbConnect(
    odbc::odbc(),
    Driver = db_config$driver,
    Server = db_config$server,
    Database = db_config$database,
    Trusted_Connection = "True" # Windows Integrated Authentication
  )
}

# ---- Required Tables ----
# These must already be in the environment (loaded by load-near-completers-ttrain.R).
# This script does not read them from the DB itself.
required_tables <- c(
  "t_dacso_data_part_1", # DACSO respondent-level records
  "stp_credential", # STP credentials (id -> psi_pen)
  "credential_non_dup", # de-duplicated credential records
  "age_group_lookup", # age -> age_group bands
  "stp_dacso_prgm_credential_lookup",
  "combine_creds", # which credentials are used in PSSM
  "tbl_age",
  "t_pssm_projection_cred_grp", # credential -> PSSM projection credential
  "tmp_tbl_age", # birth/end dates for age-at-grad calc
  "tmp_tbl_age_append_new_years", # newest survey years to append to tmp_tbl_age
  "credential_rank"
)

# Fail fast with a clear message if any input is missing.
missing <- required_tables[
  !map_lgl(required_tables, exists, where = .GlobalEnv)
]
if (length(missing) > 0) {
  stop(glue(
    "The following required tables are missing from the environment: ",
    "{paste(missing, collapse = ', ')}"
  ))
}

# Standardise all input column names to lower case so every join below can rely
# on consistent casing (the source tables arrive with mixed casing).
walk(required_tables, lower_col_names_global)

# Values that should be treated as "missing" when checking PENs.
na_vals <- c("", " ", "(Unspecified)", NA)

# ---- Derive Age at Grad ----
# Goal: compute each respondent's age when they finished/left their program
# (age_at_grad), which is an output dimension and the 17-64 eligibility filter.

# Reshape the newest survey years to match tmp_tbl_age's columns, then parse the
# year-month date strings (ym) into real dates so we can do date arithmetic.
tmp_tbl_age_append_new_years <- tmp_tbl_age_append_new_years |>
  select(
    cosc_stqu_id = coci_stqu_id,
    cosc_subm_cd = coci_subm_cd,
    tpid_date_of_birth = bthdt,
    cosc_enrl_end_date = enddt,
    coci_age_at_survey = coci_age_at_survey
  ) |>
  mutate(
    tpid_date_of_birth = lubridate::ym(tpid_date_of_birth, quiet = TRUE),
    cosc_enrl_end_date = lubridate::ym(cosc_enrl_end_date, quiet = TRUE),
    cosc_grad_credential_date = NA_character_, # new years have no grad-cred date
    age_at_grad = NA_real_ # recomputed below for all rows
  )

# Append the new years to the historical age table, drop the stale age_at_grad,
# and de-duplicate so each student-submission appears once.
tmp_tbl_age <- tmp_tbl_age |>
  rbind(tmp_tbl_age_append_new_years) |>
  select(-age_at_grad) |>
  distinct()

# Compute age_at_grad. Reference date = grad-credential date if present, else the
# enrolment end date. year_diff is the raw year gap; we subtract 1 when the
# reference date falls before the student's birthday that year (birthday hasn't
# happened yet), giving a correct integer age.
tmp_tbl_age <- tmp_tbl_age |>
  mutate(
    ref_date = coalesce(cosc_grad_credential_date, cosc_enrl_end_date),
    year_diff = year(ref_date) - year(tpid_date_of_birth),
    birthday_ref_year = make_date(
      year(ref_date),
      month(tpid_date_of_birth),
      day(tpid_date_of_birth)
    ),
    age_at_grad = if_else(
      ref_date < birthday_ref_year,
      year_diff - 1,
      year_diff
    )
  ) |>
  select(-ref_date, -year_diff, -birthday_ref_year) # drop scratch columns

# Attach age_at_grad onto the main DACSO records. inner_join + distinct keeps only
# students we could age, with no duplicate rows.
t_dacso_data_part_1 <- t_dacso_data_part_1 |>
  inner_join(
    tmp_tbl_age |> select(cosc_stqu_id, age_at_grad),
    by = c("cosc_stqu_id" = "cosc_stqu_id")
  ) |>
  distinct()

# ---- Flag near-completers who hold an STP credential (PEN semi-join) ----
# Logic: a status-3 near-completer who later earned ANY tracked STP credential is
# treated as having effectively completed. We match on PEN (the person id shared
# between DACSO and the credential data).

# Build the lookup id -> psi_pen freshly (drop any pre-existing psi_pen first so a
# re-run doesn't create psi_pen.x / psi_pen.y).
if ("psi_pen" %in% names(credential_non_dup)) {
  credential_non_dup <- credential_non_dup |> select(-psi_pen)
}

credential_non_dup <- credential_non_dup |>
  left_join(stp_credential |> distinct(id, psi_pen), by = "id")

# The set of PENs that hold at least one valid STP credential.
stp_pens <- credential_non_dup |>
  filter(!psi_pen %in% na_vals) |>
  distinct(psi_pen) |>
  pull(psi_pen)

# Identify the near-completers to flag: status "3", aged 17-64 at grad, in the
# eligible survey windows (C_Outc07-23), whose PEN appears in stp_pens.
matched_near_completer_ids <- t_dacso_data_part_1 |>
  filter(
    cosc_grad_status_lgds_cd_group == "3",
    age_at_grad >= 17,
    age_at_grad <= 64,
    coci_subm_cd %in% paste0("C_Outc", sprintf("%02d", 7:23)),
    as.character(coci_pen) %in% stp_pens
  ) |>
  distinct(coci_stqu_id) |>
  pull(coci_stqu_id)

# Write two derived columns onto every DACSO row:
#   has_stp_credential          : "Yes" for the matched near-completers above, else NA
#   grad_status_factoring_in_stp: reclassifies a matched near-completer ("3") as a
#                                 completer ("1"); everyone else keeps their status.
t_dacso_data_part_1 <- t_dacso_data_part_1 |>
  mutate(
    has_stp_credential = if_else(
      coci_stqu_id %in% matched_near_completer_ids,
      "Yes",
      NA_character_
    ),
    grad_status_factoring_in_stp = if_else(
      !is.na(has_stp_credential) & cosc_grad_status_lgds_cd_group == "3",
      "1",
      as.character(cosc_grad_status_lgds_cd_group)
    )
  )

# ---- Age At Grad by CIP4 Ratios ----
# Build the reusable `base` for the two reference survey years (C_Outc19/20):
#   - assign each row to an age_group band,
#   - attach credential rank,
#   - keep only credentials flagged for use in PSSM (combine_creds),
#   - tidy the lcip4_cred label, and create lcip4_cred_cleaned where a "1 - "
#     (completer) prefix is rewritten to "3 - " so completer and near-completer
#     rows can share a credential key when joined.
base <- t_dacso_data_part_1 |>
  select(-age_group) |>
  filter(coci_subm_cd %in% c("C_Outc19", "C_Outc20")) |>
  inner_join(
    age_group_lookup,
    by = join_by(age_at_grad >= lower_bound, age_at_grad <= upper_bound)
  ) |>
  left_join(
    credential_rank,
    by = c("prgm_credential_awarded_name" = "psi_credential_category")
  ) |>
  inner_join(
    combine_creds |> filter(use_in_pssm_2017_18 == "Yes"),
    by = "prgm_credential_awarded_name"
  ) |>
  mutate(
    lcip4_cred = gsub("-\\s(0|1)\\s", "", lcip4_cred),
    lcip4_cred_cleaned = if_else(
      str_detect(lcip4_cred, "^1 - "),
      str_replace(lcip4_cred, "^1 - ", "3 - "),
      lcip4_cred
    )
  )

# Numerator part 1: all near-completers (status 3) by age x credential x CIP4.
nearcompleters_cip4_combinedcred <- base |>
  filter(cosc_grad_status_lgds_cd_group == "3") |>
  count(age_group, lcip4_cred, lcp4_cd, name = "count")

# Numerator part 2: near-completers who ALSO hold an STP credential (to subtract).
near_completers_cip4_with_stp_combined_cred <- base |>
  filter(has_stp_credential == "Yes") |>
  count(age_group, lcip4_cred, lcp4_cd, name = "nc_with_earlier_or_later")

# Denominator option A: completers AFTER factoring in STP (status reclassified).
completers_factoring_in_stp_cip4_combined_cred <- base |>
  filter(
    grad_status_factoring_in_stp == "1",
    age_at_grad >= 17,
    age_at_grad <= 64
  ) |>
  count(age_group, lcip4_cred_cleaned, lcp4_cd, name = "completers") |>
  rename(lcip4_cred = lcip4_cred_cleaned)

# Denominator option B: completers NOT factoring in STP (raw status 1).
completers_cip4_combined_cred <- base |>
  filter(
    cosc_grad_status_lgds_cd_group == "1",
    age_at_grad >= 17,
    age_at_grad <= 64
  ) |>
  count(age_group, lcip4_cred_cleaned, lcp4_cd, name = "c_not_factoring_stp") |>
  rename(lcip4_cred = lcip4_cred_cleaned)

# Assemble the ratio table. After joining all four counts:
#   near_completers_stp_cred = count - nc_with_earlier_or_later  (true near-completers)
#   ratio                    = true near-completers / completers
# Final two mutates turn dividing-by-zero artefacts (Inf, NaN) into NA.
t_dacso_nearcompleters_ratioageatgradcip4 <-
  nearcompleters_cip4_combinedcred |>
  left_join(
    near_completers_cip4_with_stp_combined_cred,
    by = join_by(age_group, lcip4_cred, lcp4_cd)
  ) |>
  left_join(
    completers_factoring_in_stp_cip4_combined_cred,
    by = join_by(age_group, lcip4_cred, lcp4_cd)
  ) |>
  left_join(
    completers_cip4_combined_cred,
    by = join_by(age_group, lcip4_cred, lcp4_cd)
  ) |>
  mutate(across(where(is.numeric), ~ replace_na(., 0))) |> # absent join = 0 count
  mutate(
    near_completers_stp_cred = count - nc_with_earlier_or_later,
    ratio = near_completers_stp_cred / completers,
    ratio_not_factoring_stp = near_completers_stp_cred / c_not_factoring_stp
  ) |>
  mutate(across(where(is.double), ~ na_if(., Inf))) |> # x / 0 -> NA
  mutate_all(function(x) ifelse(is.nan(x), NA, x)) # 0 / 0 -> NA

# ---- Gender by CIP4 Ratios ----
# Same recipe, but sliced by gender instead of CIP4. tpid_lgnd_cd != "0" drops the
# "unknown" gender code. Credential is the program credential name (not lcip4_cred).
base <- t_dacso_data_part_1 |>
  select(-age_group) |>
  filter(coci_subm_cd %in% c("C_Outc19", "C_Outc20"), tpid_lgnd_cd != "0") |>
  inner_join(
    age_group_lookup,
    by = join_by(age_at_grad >= lower_bound, age_at_grad <= upper_bound)
  ) |>
  left_join(
    credential_rank,
    by = c("prgm_credential_awarded_name" = "psi_credential_category")
  )

near_completes_total_by_gender <- base |>
  filter(cosc_grad_status_lgds_cd_group == "3") |>
  count(tpid_lgnd_cd, age_group, prgm_credential_awarded_name, name = "count")

near_completes_total_with_stp_by_gender <- base |>
  filter(has_stp_credential == "Yes") |>
  count(
    tpid_lgnd_cd,
    age_group,
    prgm_credential_awarded_name,
    name = "nc_with_early_or_late"
  )

completers_agg_by_gender <- base |>
  filter(
    cosc_grad_status_lgds_cd_group == "1",
    age_at_grad >= 17,
    age_at_grad <= 64
  ) |>
  count(
    tpid_lgnd_cd,
    age_group,
    prgm_credential_awarded_name,
    name = "completers"
  )

# n_nc_stp = true near-completers (status 3 minus those with an STP credential).
ratio.df <- near_completes_total_by_gender |>
  left_join(near_completes_total_with_stp_by_gender) |>
  left_join(completers_agg_by_gender) |>
  rename("gender" = "tpid_lgnd_cd") |>
  mutate(across(where(is.numeric), ~ replace_na(., 0))) |>
  mutate(
    n_nc_stp = count - nc_with_early_or_late,
    ratio = n_nc_stp / completers
  )

# Special pooling rule: Associate Degree and University Transfer are combined into
# a single "Associate Degree" group and given one shared ratio (ratio_adgt),
# because UT students frequently complete as Associate Degree holders.
ratio.df2 <- ratio.df |>
  filter(
    prgm_credential_awarded_name %in%
      c("Associate Degree", "University Transfer")
  ) |>
  mutate(prgm_credential_awarded_name = "Associate Degree") |>
  summarise(
    ratio_adgt = sum(n_nc_stp) / sum(completers),
    .by = c(gender, age_group, prgm_credential_awarded_name)
  )

# Overwrite the ratio for AD/UT rows with the pooled ratio_adgt; clean up Inf/NaN.
t_dacso_near_completers_ratio_by_gender <-
  ratio.df |>
  left_join(ratio.df2) |>
  mutate(
    ratio = if_else(
      prgm_credential_awarded_name %in%
        c("Associate Degree", "University Transfer"),
      ratio_adgt,
      ratio
    )
  ) |>
  mutate(across(where(is.double), ~ na_if(., Inf))) |>
  mutate_all(function(x) ifelse(is.nan(x), NA, x)) |>
  select(-ratio_adgt)

# ---- Age by Gender and Year Ratios ----
# Same as the gender table, but adds the survey submission code (coci_subm_cd) to
# every grouping so ratios are produced per survey YEAR. All years are kept here.
base <- t_dacso_data_part_1 |>
  select(-age_group) |>
  filter(tpid_lgnd_cd != "0") |>
  inner_join(
    age_group_lookup,
    by = join_by(age_at_grad >= lower_bound, age_at_grad <= upper_bound)
  ) |>
  left_join(
    credential_rank,
    by = c("prgm_credential_awarded_name" = "psi_credential_category")
  )

near_completes_total_by_gender_year <- base |>
  filter(cosc_grad_status_lgds_cd_group == "3") |>
  count(
    coci_subm_cd,
    tpid_lgnd_cd,
    age_group,
    prgm_credential_awarded_name,
    name = "Count"
  )

near_completes_total_with_stp_by_gender_year <- base |>
  filter(has_stp_credential == "Yes") |>
  count(
    coci_subm_cd,
    tpid_lgnd_cd,
    age_group,
    prgm_credential_awarded_name,
    name = "nc_with_early_or_late"
  )

completers_agg_by_gender_age_year <- base |>
  filter(
    cosc_grad_status_lgds_cd_group == "1",
    age_at_grad >= 17,
    age_at_grad <= 64
  ) |>
  count(
    coci_subm_cd,
    tpid_lgnd_cd,
    age_group,
    prgm_credential_awarded_name,
    name = "completers"
  )

ratio.df <- near_completes_total_by_gender_year |>
  left_join(near_completes_total_with_stp_by_gender_year) |>
  left_join(completers_agg_by_gender_age_year) |>
  rename("gender" = "tpid_lgnd_cd") |>
  mutate(across(where(is.numeric), ~ replace_na(., 0))) |>
  mutate(
    n_nc_stp = Count - nc_with_early_or_late,
    ratio = n_nc_stp / completers
  )

# Same AD/UT pooling as above, now also within each survey code.
ratio.df2 <- ratio.df |>
  filter(
    prgm_credential_awarded_name %in%
      c("Associate Degree", "University Transfer")
  ) |>
  mutate(prgm_credential_awarded_name = "Associate Degree") |>
  summarise(
    ratio_adgt = sum(n_nc_stp) / sum(completers),
    .by = c(gender, age_group, prgm_credential_awarded_name)
  )

# Final table also derives a calendar `year` from the survey code (e.g. C_Outc19
# -> 2019 -> survey year 2018, hence the - 1).
t_dacso_near_completers_ratio_by_gender_year <-
  ratio.df |>
  left_join(ratio.df2) |>
  mutate(
    ratio = if_else(
      prgm_credential_awarded_name %in%
        c("Associate Degree", "University Transfer"),
      ratio_adgt,
      ratio
    )
  ) |>
  mutate(across(where(is.double), ~ na_if(., Inf))) |>
  mutate_all(function(x) ifelse(is.nan(x), NA, x)) |>
  select(-ratio_adgt) |>
  mutate(year = as.numeric(paste0("20", str_sub(coci_subm_cd, 7, 8))) - 1)

# ---- TTRAIN tables (C_Outc19/20 only) ----
# TTRAIN distinguishes trades-training programs. These tables keep TTRAIN and the
# full CIP4 detail, and map the program credential to the PSSM projection
# credential grouping. Reference years C_Outc19/20 only.

# Near-completer counts by age x credential x CIP4 x TTRAIN.
near_completes_total_by_cip4_ttrain <- t_dacso_data_part_1 |>
  filter(
    cosc_grad_status_lgds_cd_group == "3",
    coci_subm_cd %in% c("C_Outc19", "C_Outc20")
  ) |>
  select(-age_group) |>
  inner_join(
    age_group_lookup,
    by = join_by(age_at_grad >= lower_bound, age_at_grad <= upper_bound)
  ) |>
  left_join(
    credential_rank,
    by = c("prgm_credential_awarded_name" = "psi_credential_category")
  ) |>
  count(
    age_group,
    prgm_credential_awarded_name,
    lcip4_cred,
    lcp4_cd,
    lcp4_cip_4digits_name,
    ttrain,
    cosc_grad_status_lgds_cd_group,
    name = "Count"
  )

# Of those near-completers, the ones who hold an STP credential (to subtract).
near_completes_total_with_stp_credential_bycip4_ttrain <- t_dacso_data_part_1 |>
  filter(
    coci_subm_cd %in% c("C_Outc19", "C_Outc20"),
    has_stp_credential == "Yes"
  ) |>
  select(-age_group) |>
  inner_join(
    age_group_lookup,
    by = join_by(age_at_grad >= lower_bound, age_at_grad <= upper_bound)
  ) |>
  left_join(
    credential_rank,
    by = c("prgm_credential_awarded_name" = "psi_credential_category")
  ) |>
  count(
    age_group,
    prgm_credential_awarded_name,
    has_stp_credential,
    lcip4_cred,
    lcp4_cd,
    lcp4_cip_4digits_name,
    ttrain,
    cosc_grad_status_lgds_cd_group,
    name = "Count"
  )

# Join to the PSSM credential grouping, then subtract the STP-holders to get the
# true near-completer count. The gsub() calls fix label casing/spelling so the
# credential names match the projection lookup ("Post-Degree" vs "Post-degree",
# " OR " vs " or "). pssm_cred prefixes the status code, e.g. "3 - <credential>".
t_dacso_near_completers_ratiosageatgradcip4_ttrain <-
  near_completes_total_by_cip4_ttrain |>
  inner_join(
    t_pssm_projection_cred_grp |>
      rename_with(tolower) |>
      mutate(
        pssm_projection_credential = gsub(
          "Post-Degree",
          "Post-degree",
          pssm_projection_credential
        ),
        pssm_credential = gsub(" OR ", " or ", pssm_credential)
      ),
    by = c("prgm_credential_awarded_name" = "pssm_projection_credential")
  ) |>
  left_join(
    near_completes_total_with_stp_credential_bycip4_ttrain |>
      select(
        ttrain,
        age_group,
        prgm_credential_awarded_name,
        lcip4_cred,
        nc_with_stp = Count
      ),
    by = c("ttrain", "age_group", "prgm_credential_awarded_name", "lcip4_cred")
  ) |>
  mutate(
    nc_with_stp = replace_na(nc_with_stp, 0),
    pssm_cred = paste0(cosc_grad_status_lgds_cd_group, " - ", pssm_credential),
    near_completers_stp_credentials = Count - nc_with_stp # true near-completers
  ) |>
  summarise(
    Count = sum(Count),
    near_completers_from_c_outc19_20_with_earlier_or_later_stp = sum(
      nc_with_stp
    ),
    .by = c(
      pssm_credential,
      pssm_cred,
      age_group,
      lcip4_cred,
      lcp4_cd,
      lcp4_cip_4digits_name,
      cosc_grad_status_lgds_cd_group,
      ttrain,
      near_completers_stp_credentials
    )
  )

# ---- HISTORICAL TTRAIN tables (all years) ----
# Identical to the TTRAIN block above, but keeps coci_subm_cd (survey year) in
# every grouping and uses ALL survey years rather than just C_Outc19/20.
near_completes_total_by_cip4_ttrain_history <- t_dacso_data_part_1 |>
  filter(cosc_grad_status_lgds_cd_group == "3") |>
  select(-age_group) |>
  inner_join(
    age_group_lookup,
    by = join_by(age_at_grad >= lower_bound, age_at_grad <= upper_bound)
  ) |>
  left_join(
    credential_rank,
    by = c("prgm_credential_awarded_name" = "psi_credential_category")
  ) |>
  count(
    age_group,
    coci_subm_cd,
    prgm_credential_awarded_name,
    lcip4_cred,
    lcp4_cd,
    lcp4_cip_4digits_name,
    ttrain,
    cosc_grad_status_lgds_cd_group,
    name = "Count"
  )

near_completes_total_with_stp_credential_bycip4_ttrain_history <- t_dacso_data_part_1 |>
  filter(has_stp_credential == "Yes") |>
  select(-age_group) |>
  inner_join(
    age_group_lookup,
    by = join_by(age_at_grad >= lower_bound, age_at_grad <= upper_bound)
  ) |>
  left_join(
    credential_rank,
    by = c("prgm_credential_awarded_name" = "psi_credential_category")
  ) |>
  count(
    age_group,
    coci_subm_cd,
    prgm_credential_awarded_name,
    has_stp_credential,
    lcip4_cred,
    lcp4_cd,
    lcp4_cip_4digits_name,
    ttrain,
    cosc_grad_status_lgds_cd_group,
    name = "Count"
  )

# Note the reverse gsub here ("Post-degree" -> "Post-Degree"): the historical
# lookup is NOT lower-cased on the credential side, so the label fix-up goes the
# other direction to make the join key match.
t_dacso_near_completers_ratiosageatgradcip4_ttrain_history <-
  near_completes_total_by_cip4_ttrain_history |>
  mutate(
    prgm_credential_awarded_name = gsub(
      "Post-degree",
      "Post-Degree",
      prgm_credential_awarded_name
    )
  ) |>
  inner_join(
    t_pssm_projection_cred_grp |> rename_with(tolower),
    by = c("prgm_credential_awarded_name" = "pssm_projection_credential")
  ) |>
  left_join(
    near_completes_total_with_stp_credential_bycip4_ttrain_history |>
      mutate(age_group = as.character(age_group)) |> # align type for the join key
      select(
        ttrain,
        coci_subm_cd,
        age_group,
        prgm_credential_awarded_name,
        lcip4_cred,
        nc_with_stp = Count
      ),
    by = c(
      "ttrain",
      "coci_subm_cd",
      "age_group",
      "prgm_credential_awarded_name",
      "lcip4_cred"
    )
  ) |>
  mutate(
    nc_with_stp = replace_na(nc_with_stp, 0),
    pssm_cred = paste0(cosc_grad_status_lgds_cd_group, " - ", pssm_credential),
    near_completers_stp_credentials = Count - nc_with_stp
  ) |>
  summarise(
    Count = sum(Count),
    near_completers_from_c_outc19_20_with_earlier_or_later_stp = sum(
      nc_with_stp
    ),
    .by = c(
      pssm_credential,
      pssm_cred,
      age_group,
      coci_subm_cd,
      lcip4_cred,
      lcp4_cd,
      lcp4_cip_4digits_name,
      cosc_grad_status_lgds_cd_group,
      ttrain,
      near_completers_stp_credentials
    )
  )

# ---- Compare refactored tables to current `*_r` tables in the database ----
# Validation: confirm this refactor reproduces the existing pipeline output by
# diffing each table against its `*_r` copy already in the database.
tables_to_keep <- c(
  "t_dacso_near_completers_ratio_by_gender_year",
  "t_dacso_near_completers_ratio_by_gender",
  "t_dacso_nearcompleters_ratioageatgradcip4",
  "t_dacso_near_completers_ratiosageatgradcip4_ttrain_history",
  "t_dacso_near_completers_ratiosageatgradcip4_ttrain"
)

if (!requireNamespace("waldo", quietly = TRUE)) {
  stop(
    "Package 'waldo' is required for the comparison step (install.packages('waldo'))."
  )
}

# Normalise both sides before comparing so cosmetic differences don't show up as
# diffs: lower-case names, force all numerics to double (int vs double), sort
# columns alphabetically, and sort rows by all columns (row order is irrelevant).
normalise_for_compare <- function(df) {
  df <- as_tibble(df)
  names(df) <- tolower(names(df))
  df |>
    mutate(across(where(is.numeric), as.double)) |>
    relocate(sort(names(df))) |>
    arrange(pick(everything()))
}

# Compare one in-memory refactored table to its `*_r` copy in the DB. Reads the DB
# table directly (NOT via read_table_from_db, which would overwrite the in-memory
# object we are trying to validate). Returns a one-row summary; `diff` is a
# list-column holding the full waldo diff for later inspection.
compare_to_db <- function(table_name, schema, con) {
  refactored <- normalise_for_compare(.GlobalEnv[[table_name]])

  db_name <- glue("{table_name}_r")
  current <- con |>
    dbReadTable(SQL(glue('"{schema}"."{db_name}"'))) |>
    normalise_for_compare()

  diff <- waldo::compare(
    current,
    refactored,
    x_arg = "database",
    y_arg = "refactored"
  )

  tibble(
    table = table_name,
    identical = length(diff) == 0, # waldo returns length-0 when there is no diff
    n_db = nrow(current),
    n_refactored = nrow(refactored),
    diff = list(diff)
  )
}

# Run the comparison across all five tables and stack the summaries into one tibble.
comparison_results <- tables_to_keep |>
  set_names() |>
  map(compare_to_db, schema = my_schema, con = con) |>
  list_rbind()

# ---- Clean Up ----
# Only disconnect if THIS script opened the connection; always release memory.
if (con_created) {
  dbDisconnect(con)
}
gc()

# Return the comparison summary as the script's result.
comparison_results
