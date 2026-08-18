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

# get the utils functions
source("R/utils.R")
## --------------------------------------Required Tables------------------------------------------
## -----------------------------------------------------------------------------------------------
library(purrr)
# these should now be in the R environment
required_tables <- c(
  "t_dacso_data_part_1",
  "stp_credential",
  "credential_non_dup",
  "age_group_lookup",
  "stp_dacso_prgm_credential_lookup",
  "combine_creds",
  "tbl_age",
  "t_pssm_projection_cred_grp",
  "tmp_tbl_age",
  "credential_rank"
)

missing <- required_tables[!sapply(required_tables, exists, where = .GlobalEnv)]

if (length(missing) > 0) {
  stop(paste(
    "The following required tables are missing from the environment:",
    paste(missing, collapse = ", ")
  ))
}

# Tables are expected to be preloaded by the paired load script; fail fast above if missing.
# (If you intend to auto-load missing tables from the DB, remove the stop() and wrap
# the read_table_from_db/lower_col_names_global calls in `if (length(missing) > 0)`.)

na_vals <- c("", " ", "(Unspecified)", NA)

# ---- Derive Age at Grad ----
# replicates lines 69:87 (main branch)
# Notes: re. the output at line 116 is a "Check" used to pick representitive years
# from which to calculate the completers to near-completers ratio.

# bring age at grad into t_dacso dataset
t_dacso_data_part_1 <- t_dacso_data_part_1 |>
  inner_join(
    tmp_tbl_age |>
      select(cosc_stqu_id, age_at_grad),
    by = c("coci_stqu_id" = "cosc_stqu_id")
  ) |>
  distinct() # just in case

# this table isn't really relevent to this section
t_dacso_data_part_1_tempselection <- t_dacso_data_part_1 |>
  distinct(
    coci_stqu_id,
    coci_subm_cd,
    coci_age_at_survey,
    age_at_grad,
    cosc_grad_status_lgds_cd_group,
    prgm_credential_awarded,
    prgm_credential_awarded_name,
    pssm_credential,
    pssm_credential_name
  )

# just a check of students by year and grad status
t_dacso_data_part_1_tempselection |>
  filter(
    !is.na(cosc_grad_status_lgds_cd_group),
    age_at_grad >= 17,
    age_at_grad <= 64
  ) |>
  summarize(
    student_count = n(),
    .by = c("cosc_grad_status_lgds_cd_group", "coci_subm_cd")
  ) |>
  pivot_wider(
    names_from = coci_subm_cd,
    values_from = student_count,
    values_fill = 0
  )

# ---- Add PEN to Non-Dup table ----
# replicates lines 90:101 (main branch)
# Notes:
#  There's a note from main branch script that says -  "Move to earlier workflow - 02 series.
#  This updates credential non-dup in current schema only".  We should
#  confirm that we only need the psi_pen in the current R environment.

if ("psi_pen" %in% names(credential_non_dup)) {
  # drop psi pen
  credential_non_dup <- credential_non_dup |>
    select(-psi_pen)
}

credential_non_dup <- credential_non_dup |>
  left_join(
    stp_credential |>
      rename_with(str_to_lower) |>
      select(id = id, psi_pen = psi_pen), #ID is unique, s.b. distinct
    by = "id"
  )

# ---- DACSO Matching STP Credential ----
# replicates lines 104:133 (main branch)
# testing: 1) compare dacso_matching_stp_credential_pen in R vs dacso_matching_stp_credential_pen in SQL
#          2) compare match_summary_table in R to output from qry06 at line 124 (main) vs
## Notes:
dacso_matching_stp_credential_pen <- t_dacso_data_part_1 |>
  filter(!coci_pen %in% na_vals) |>
  mutate(coci_pen = as.character(coci_pen)) |>
  inner_join(
    credential_non_dup,
    by = c("coci_pen" = "psi_pen"),
    relationship = "many-to-many"
  ) |>
  distinct(
    coci_stqu_id,
    coci_inst_cd,
    id,
    coci_pen,
    psi_code,
    prgm_credential_awarded,
    prgm_credential_awarded_name,
    pssm_credential,
    pssm_credential_name,
    psi_credential_category,
    outcomes_cred,
    lcp4_cd,
    final_cip_code_4,
    coci_subm_cd,
    psi_award_school_year,
    cosc_grad_status_lgds_cd_group
  )

# populate stp_prgm_credential_awarded_name
names(stp_dacso_prgm_credential_lookup) <- tolower(names(
  stp_dacso_prgm_credential_lookup
))

dacso_matching_stp_credential_pen <- dacso_matching_stp_credential_pen |>
  left_join(
    stp_dacso_prgm_credential_lookup |>
      select(
        prgrm_credential_awarded,
        stp_prgm_credential_awarded_name
      ),
    by = c("prgm_credential_awarded" = "prgrm_credential_awarded")
  )

dacso_matching_stp_credential_pen <- dacso_matching_stp_credential_pen |>
  mutate(
    match_credential = if_else(
      str_equal(
        prgm_credential_awarded_name,
        psi_credential_category,
        ignore_case = TRUE
      ),
      "yes",
      NA_character_
    ),
    match_cip_code_4 = if_else(
      lcp4_cd == final_cip_code_4,
      "yes",
      NA_character_
    ),
    match_cip_code_2 = if_else(
      str_sub(lcp4_cd, 1, 2) == str_sub(final_cip_code_4, 1, 2),
      "Yes", #align with SQL version which capitalizes this
      NA_character_
    ),
    suffix = as.numeric(str_extract(coci_subm_cd, "\\d+$")),
    base_year = 1997 + suffix,
    psi_start_year = as.numeric(str_sub(psi_award_school_year, 1, 4)),
    match_award_school_year = case_when(
      psi_start_year == base_year ~ "yes",
      psi_start_year == base_year + 1 ~ "yes",
      TRUE ~ NA_character_
    ),
    match_inst = if_else(
      psi_code == coci_inst_cd |
        (psi_code == "CAP" & coci_inst_cd == "CAPU") |
        (psi_code == "KWAN" & coci_inst_cd == "KPU") |
        (psi_code == "OLA" & coci_inst_cd == "TRU") |
        (psi_code == "MALA" & coci_inst_cd == "VIU") |
        (psi_code == "OUC" & coci_inst_cd == "OKAN") |
        (psi_code == "UCFV" & coci_inst_cd == "UFV") |
        (psi_code == "UCC" & coci_inst_cd == "TRU") |
        (psi_code == "NWCC" & coci_inst_cd == "CMTN"),
      "yes",
      NA_character_
    )
  ) |>
  select(-suffix, -base_year, -psi_start_year)

match_summary_table <- dacso_matching_stp_credential_pen |>
  group_by(
    match_credential,
    match_cip_code_4,
    match_award_school_year,
    match_inst
  ) |>
  summarize(
    Expr1 = n(),
    .groups = "drop"
  ) |>
  arrange(
    desc(match_credential),
    desc(match_cip_code_4),
    desc(match_award_school_year),
    desc(match_inst)
  )

# These are considered final matches to STP credential
dacso_matching_stp_credential_pen <- dacso_matching_stp_credential_pen |>
  mutate(
    match_all_4_flag = if_else(
      if_all(
        c(
          match_credential,
          match_cip_code_4,
          match_award_school_year,
          match_inst
        ),
        ~ .x == "yes"
      ),
      "yes",
      NA_character_
    ),
    final_consider_a_match = case_when(
      match_all_4_flag == "yes" ~ "yes",
      match_credential == "yes" &
        match_cip_code_2 == "Yes" &
        is.na(match_cip_code_4) &
        match_award_school_year == "yes" &
        match_inst == "yes" ~ "yes",

      TRUE ~ NA_character_
    )
  )

# ---- Flag near-completers with earlier or later credential----
# replicates lines 135:147  (main branch)
# testing: at end of section, compare t_dacso_nearcompleters in R vs t_dacso_nearcompleters in SQL

#identify "Near-Completers" and join their survey data with their credential records.
nearcompleters_in_stp_credential_step1 <- t_dacso_data_part_1 |>
  select(
    coci_stqu_id,
    coci_subm_cd,
    age_at_grad,
    prgm_credential_awarded,
    prgm_credential_awarded_name,
    pssm_credential,
    pssm_credential_name,
    lcp4_cd,
    cosc_grad_status_lgds_cd_group
  ) |>
  # 1. Filter for specific outcome codes, age range, and status '3'
  filter(
    coci_subm_cd %in% paste0("C_Outc", sprintf("%02d", 7:23)),
    age_at_grad >= 17,
    age_at_grad <= 64,
    cosc_grad_status_lgds_cd_group == "3"
  ) |>
  inner_join(
    dacso_matching_stp_credential_pen |>
      select(
        coci_stqu_id,
        id,
        coci_pen,
        coci_inst_cd,
        psi_code,
        prgm_credential_awarded,
        prgm_credential_awarded_name,
        stp_prgm_credential_awarded_name,
        pssm_credential,
        pssm_credential_name,
        psi_credential_category,
        outcomes_cred,
        final_cip_code_4,
        psi_award_school_year,
        match_award_school_year,
        match_inst,
        final_consider_a_match,
        match_all_4_flag,
        match_credential,
        match_cip_code_4,
        match_cip_code_2
      ),
    by = "coci_stqu_id",
    suffix = c("_dacso", "_stp") # Handles duplicate column names automatically
  ) |>
  rename(
    dacso_prgm_credential_awarded = prgm_credential_awarded_dacso,
    dacso_prgm_credential_awarded_name = prgm_credential_awarded_name_dacso,
    dacso_pssm_credential = pssm_credential_dacso,
    dacso_pssm_credential_name = pssm_credential_name_dacso
  )


# Logic: The max 'Before' award year is always (Survey Value + 1998)
# e.g., Outcome 07 + 1998 = 2005. Any award <= 2005 is 'Before'.
nearcompleters_in_stp_credential_step1 <- nearcompleters_in_stp_credential_step1 |>
  mutate(
    survey_val = as.numeric(str_extract(coci_subm_cd, "\\d+")),
    award_year_start = as.numeric(str_sub(psi_award_school_year, 1, 4)),
    stp_credential_awarded_before_dacso = if_else(
      award_year_start <= (survey_val + 1998),
      "Yes",
      NA_character_
    )
  ) |>
  select(-survey_val, -award_year_start)

nearcompleters_in_stp_credential_step1 <- nearcompleters_in_stp_credential_step1 |>
  mutate(
    stp_credential_awarded_after_dacso = if_else(
      is.na(stp_credential_awarded_before_dacso),
      "Yes",
      NA_character_
    )
  )


t_dacso_nearcompleters <- t_dacso_data_part_1 |>
  # 1. Filter for the core population
  filter(
    cosc_grad_status_lgds_cd_group == "3",
    age_at_grad >= 17,
    age_at_grad <= 64
  ) |>
  # 2. Join with the full credential list (this creates multiple rows temporarily)
  left_join(
    nearcompleters_in_stp_credential_step1 |>
      select(
        coci_stqu_id,
        stp_credential_awarded_before_dacso,
        stp_credential_awarded_after_dacso
      ),
    by = "coci_stqu_id"
  ) |>
  group_by(across(c(
    coci_stqu_id,
    coci_subm_cd,
    age_at_grad,
    cosc_grad_status_lgds_cd_group,
    prgm_credential_awarded,
    prgm_credential_awarded_name,
    pssm_credential,
    pssm_credential_name
  ))) |>
  summarize(
    stp_credential_awarded_before_dacso = if_else(
      any(stp_credential_awarded_before_dacso == "Yes", na.rm = TRUE),
      "Yes",
      NA_character_
    ),
    stp_credential_awarded_after_dacso = if_else(
      any(stp_credential_awarded_after_dacso == "Yes", na.rm = TRUE),
      "Yes",
      NA_character_
    ),
    .groups = "drop"
  )

# ---- Flag near-completers with multiple credentials----
# replicates lines 150:204 (main branch)
# testing: at end of section, compare t_dacso_data_part_1 in R vs t_dacso_data_part_1 in SQL.  This one table
# should carry all of the flags we create in this section.
# Notes: there is quite a bit we can do to condense this code. Leaving for now but noting that
# the logic is similar to prior workflows

# Update the main matching table with 'Multiple' and 'UseThis' flags
nearcompleters_in_stp_credential_step1 <- nearcompleters_in_stp_credential_step1 |>
  group_by(coci_stqu_id) |>
  mutate(
    has_multiple_stp_credentials = if_else(n() > 1, "Yes", NA_character_)
  ) |>
  ungroup()

ids <- nearcompleters_in_stp_credential_step1 |>
  filter(has_multiple_stp_credentials == "Yes") |>
  pull(coci_stqu_id)

# Update the final reporting table with the 'Multiple' flag
t_dacso_nearcompleters <- t_dacso_nearcompleters |>
  mutate(
    has_multiple_stp_credentials = if_else(
      coci_stqu_id %in% ids,
      "Yes",
      NA_character_
    )
  )

nearcompleters_in_stp_credential_step1 <- nearcompleters_in_stp_credential_step1 |>
  group_by(coci_stqu_id) |>
  mutate(
    Max_Award_School_Year = if_else(
      psi_award_school_year == max(psi_award_school_year, na.rm = TRUE),
      "Yes",
      NA_character_
    )
  ) |>
  ungroup()

nearcompleters_in_stp_credential_step1 <- nearcompleters_in_stp_credential_step1 |>
  mutate(
    dup_stquid_usethisrecord = if_else(
      has_multiple_stp_credentials == "Yes" &
        Max_Award_School_Year == "Yes",
      "Yes",
      NA_character_
    )
  )


# This replaces Steps 5, 6, PickMaxYear 2, and PickMaxYear 3
final_winners <- nearcompleters_in_stp_credential_step1 |>
  filter(dup_stquid_usethisrecord == "Yes") |>
  group_by(coci_stqu_id) |>
  filter(n() > 1) |>
  filter(id == max(id, na.rm = TRUE)) |>
  mutate(final_record_to_use = "Yes") |>
  ungroup() |>
  select(
    id,
    coci_stqu_id,
    final_record_to_use,
    stp_credential_awarded_before_dacso,
    stp_credential_awarded_after_dacso
  )

# 2. Update the main matching table (Replaces Step 10)
nearcompleters_in_stp_credential_step1 <- nearcompleters_in_stp_credential_step1 |>
  left_join(
    final_winners |> select(id, coci_stqu_id, final_record_to_use),
    by = c("id", "coci_stqu_id")
  )

# 3. Update the final reporting table (Replaces Step 13)
t_dacso_nearcompleters <- t_dacso_nearcompleters |>
  left_join(
    final_winners |>
      select(
        coci_stqu_id,
        stp_credential_awarded_before_dacso_update = stp_credential_awarded_before_dacso,
        stp_credential_awarded_after_dacso_update = stp_credential_awarded_after_dacso,
      ),
    by = "coci_stqu_id"
  ) |>
  mutate(
    stp_credential_awarded_before_dacso = coalesce(
      stp_credential_awarded_before_dacso,
      stp_credential_awarded_before_dacso_update
    ),
    stp_credential_awarded_after_dacso = coalesce(
      stp_credential_awarded_after_dacso,
      stp_credential_awarded_after_dacso_update
    )
  ) |>
  select(
    -stp_credential_awarded_before_dacso_update,
    -stp_credential_awarded_after_dacso_update
  )

# I have no idea what these comments about.  Leaving in case I have an epiphany later.
# off by a small handful here in "t_dacso_nearcompleters.stp_credential_awarded_before_dacso" before column?
#  (I there where there were multiple awards in a single year)?
#dbExecute(decimal_con, qry_NearCompleters_MultiCdtls_Cleaning_Step13)?

# Update the primary matching table with the finalized 'UseThisRecord' flag
dacso_matching_stp_credential_pen <- dacso_matching_stp_credential_pen |>
  left_join(
    final_winners |> select(id, coci_stqu_id, final_record_to_use),
    by = c("id", "coci_stqu_id")
  ) |>
  rename(dup_stquid_usethisrecord = final_record_to_use)

nearcompleters_in_stp_credential_step1 <- nearcompleters_in_stp_credential_step1 |>
  mutate(
    final_record_to_use = if_else(
      is.na(final_record_to_use) & is.na(has_multiple_stp_credentials),
      "Yes",
      final_record_to_use
    )
  )

t_dacso_nearcompleters <- t_dacso_nearcompleters |>
  left_join(
    nearcompleters_in_stp_credential_step1 |>
      filter(final_record_to_use == "yes") |>
      select(
        coci_stqu_id,
        stp_credential_awarded_before_dacso_final = stp_credential_awarded_before_dacso,
        stp_credential_awarded_after_dacso_final = stp_credential_awarded_after_dacso
      ),
    by = "coci_stqu_id"
  )

t_dacso_data_part_1_tempselection <- t_dacso_data_part_1_tempselection |>
  left_join(
    t_dacso_nearcompleters |>
      filter(
        stp_credential_awarded_before_dacso == "Yes" |
          stp_credential_awarded_after_dacso == "Yes"
      ) |>
      select(coci_stqu_id) |>
      mutate(has_stp_credential = "Yes"),
    by = "coci_stqu_id"
  ) |>
  mutate(grad_status_factoring_in_stp = cosc_grad_status_lgds_cd_group)

t_dacso_data_part_1_tempselection <- t_dacso_data_part_1_tempselection |>
  mutate(
    grad_status_factoring_in_stp = as.character(if_else(
      (grad_status_factoring_in_stp == 3 &
        !is.na(has_stp_credential) &
        has_stp_credential == "Yes"),
      1,
      grad_status_factoring_in_stp
    ))
  )

t_dacso_data_part_1 <- t_dacso_data_part_1 |>
  left_join(
    t_dacso_data_part_1_tempselection |>
      select(
        coci_stqu_id,
        has_stp_credential = has_stp_credential,
        grad_status_factoring_in_stp = grad_status_factoring_in_stp
      ),
    by = "coci_stqu_id"
  )

# ----- Check Near Completers Ratios -----
# replicates lines 207:211 (main branch)
# testing: Each SQL query generates a table output in the R console.
#   Compare against the accompanying R verion (which also produces a table at console)

t_dacso_data_part_1_tempselection |>
  filter(
    !is.na(cosc_grad_status_lgds_cd_group),
    age_at_grad >= 17,
    age_at_grad <= 64
  ) |>
  group_by(cosc_grad_status_lgds_cd_group, coci_subm_cd) |>
  summarise(count = n(), .groups = "drop") |>
  pivot_wider(
    names_from = coci_subm_cd,
    values_from = count,
    id_cols = cosc_grad_status_lgds_cd_group
  ) |>
  mutate(across(starts_with("C_Outc"), ~ replace_na(., 0)))

t_dacso_data_part_1_tempselection |>
  filter(
    !is.na(grad_status_factoring_in_stp),
    age_at_grad >= 17,
    age_at_grad <= 64
  ) |>
  count(grad_status_factoring_in_stp, coci_subm_cd) |>
  pivot_wider(
    names_from = coci_subm_cd,
    values_from = n,
    values_fill = 0
  ) |>
  arrange(grad_status_factoring_in_stp)

t_dacso_data_part_1_tempselection |>
  filter(
    !is.na(cosc_grad_status_lgds_cd_group),
    age_at_grad >= 17,
    age_at_grad <= 64
  ) |>
  group_by(
    pssm_credential,
    pssm_credential_name,
    cosc_grad_status_lgds_cd_group,
    coci_subm_cd
  ) |>
  summarise(count = n(), .groups = "drop") |>
  pivot_wider(
    names_from = coci_subm_cd,
    values_from = count,
    values_fill = 0
  ) |>
  arrange(pssm_credential, cosc_grad_status_lgds_cd_group)

t_dacso_data_part_1_tempselection |>
  filter(
    !is.na(grad_status_factoring_in_stp),
    age_at_grad >= 17,
    age_at_grad <= 64
  ) |>
  group_by(
    pssm_credential,
    pssm_credential_name,
    grad_status_factoring_in_stp,
    coci_subm_cd
  ) |>
  summarise(count = n(), .groups = "drop") |>
  pivot_wider(
    names_from = coci_subm_cd,
    values_from = count,
    values_fill = 0
  ) |>
  arrange(pssm_credential, grad_status_factoring_in_stp)

t_dacso_data_part_1_tempselection |>
  filter(
    age_at_grad >= 17,
    age_at_grad <= 64
  ) |>
  count(
    coci_subm_cd,
    cosc_grad_status_lgds_cd_group,
    grad_status_factoring_in_stp,
    name = "expr1"
  ) |>
  arrange(
    coci_subm_cd,
    cosc_grad_status_lgds_cd_group,
    grad_status_factoring_in_stp
  )


# ----------------------- Transferred From Excel Sheet -----------------------

# ----------------------- Age At Grad by CIP4 Ratios -----------------------
# replicates lines 214:279 (main branch)
# testing: 1) run the code from here to to end of line ~784
# Compare T_DACSO_Near_Completers_RatioAgeAtGradCIP4 (on decimal) to t_dacso_nearcompleters_ratioageatgradcip4 (R version)

# Notes:
# 1) age_group_lookup colnames are set to lower case here to align with the SQL queries from here to the end of script.
# 2) col H: we appear to be using different age groups from the original.  However,
# there is code to handle this in 04-graduate-projections.R so we get to decide which way is "right" later.
# 3) The section replicates queries linked to this Excel workbook: 2017-2018\Development\Graduate Model\Near Completers.
# Each section of code is labeled with a reference to the sheet name and column of data - these values will not be identical (we used different years)
# but should be similar.
# 4) AgeGroupLookup columns in SQL follow a different naming convention.  I've changed the colnames here - I'm thinking we can remove them
# when we figure this out.
names(age_group_lookup) <- tolower(names(age_group_lookup)) # move to load script
names(credential_rank) <- tolower(names(credential_rank))
names(t_dacso_data_part_1) <- tolower(names(t_dacso_data_part_1))

base <- t_dacso_data_part_1 |>
  select(-age_group) |>
  filter(
    coci_subm_cd %in% c("C_Outc19", "C_Outc20")
  ) |>
  inner_join(
    age_group_lookup,
    by = join_by(age_at_grad >= lower_bound, age_at_grad <= upper_bound)
  ) |>
  left_join(
    credential_rank,
    by = c("prgm_credential_awarded_name" = "psi_credential_category")
  ) |>
  inner_join(
    combine_creds |>
      filter(use_in_pssm_2017_18 == "Yes"),
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

#1 (col H in Excel sheet C_Outc12_13_14RatiosAgeGradCIP4)
nearcompleters_cip4_combinedcred <- base |>
  filter(cosc_grad_status_lgds_cd_group == "3") |>
  count(
    age_group,
    lcip4_cred,
    lcp4_cd,
    name = "count"
  )

#2 (col I in Excel sheet)
near_completers_cip4_with_stp_combined_cred <- base |>
  filter(has_stp_credential == "Yes") |>
  inner_join(
    t_dacso_data_part_1_tempselection |> distinct(coci_stqu_id),
    by = "coci_stqu_id"
  ) |>
  count(age_group, lcip4_cred, lcp4_cd, name = "nc_with_earlier_or_later")

#3 (col K in Excel sheet)
completers_factoring_in_stp_cip4_combined_cred <- base |>
  filter(
    grad_status_factoring_in_stp == "1",
    age_at_grad >= 17,
    age_at_grad <= 64
  ) |>
  count(age_group, lcip4_cred_cleaned, lcp4_cd, name = "completers") |>
  rename(lcip4_cred = lcip4_cred_cleaned)

#4 (col M in Excel sheet)
completers_cip4_combined_cred <- base |>
  filter(
    cosc_grad_status_lgds_cd_group == "1",
    age_at_grad >= 17,
    age_at_grad <= 64
  ) |>
  count(age_group, lcip4_cred_cleaned, lcp4_cd, name = "c_not_factoring_stp") |>
  rename(lcip4_cred = lcip4_cred_cleaned)

# Make final ratios ----
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
  mutate(across(where(is.numeric), ~ replace_na(., 0))) |>
  mutate(
    near_completers_stp_cred = count - nc_with_earlier_or_later,
    ratio = near_completers_stp_cred / completers,
    ratio_not_factoring_stp = near_completers_stp_cred / c_not_factoring_stp
  ) |>
  mutate(across(where(is.double), ~ na_if(., Inf))) |>
  mutate_all(function(x) ifelse(is.nan(x), NA, x))

# ----------------------- Gender by CIP4 Ratios -----------------------
# replicates lines 281:324 (main branch)
# testing: 1) run the code from here to to end of this section (line ~890)
# Compare T_DACSO_Near_Completers_RatioByGender (on decimal) to t_dacso_near_completers_ratio_by_gender. (R version)
# Notes:
# 1) See notes on age ratios, this code replicates queries linked to the same Excel workbook, different sheet.

# Queries are for Excel: C_Outc12_13_14RatiosByGender
base <- t_dacso_data_part_1 |>
  select(-age_group, -has_stp_credential) |>
  #rename(age_at_grad = Age_At_Grad) |>
  filter(
    coci_subm_cd %in% c("C_Outc19", "C_Outc20"),
    tpid_lgnd_cd != "0"
  ) |>
  inner_join(
    age_group_lookup,
    by = join_by(age_at_grad >= lower_bound, age_at_grad <= upper_bound)
  ) |>
  left_join(
    credential_rank,
    by = c("prgm_credential_awarded_name" = "psi_credential_category")
  )

# 1: paste to col E (C_Outc12_13_14RatiosByGender)
near_completes_total_by_gender <- base |>
  filter(
    cosc_grad_status_lgds_cd_group == "3"
  ) |>
  count(
    tpid_lgnd_cd,
    age_group,
    prgm_credential_awarded_name,
    name = "count"
  )


# ---------------------??????????????????????????
#2: paste to col F (C_Outc12_13_14RatiosByGender)
near_completes_total_with_stp_by_gender <- base |>
  inner_join(
    t_dacso_data_part_1_tempselection |>
      distinct(coci_stqu_id, has_stp_credential),
    by = "coci_stqu_id"
  ) |>
  filter(
    has_stp_credential == "Yes"
  ) |>
  count(
    tpid_lgnd_cd,
    age_group,
    prgm_credential_awarded_name,
    name = "nc_with_early_or_late"
  )

#3: looks like paste to H (C_Outc12_13_14RatiosByGender) (Need to check this)
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

ratio.df <- near_completes_total_by_gender |>
  left_join(near_completes_total_with_stp_by_gender) |>
  left_join(completers_agg_by_gender) |>
  rename("gender" = "tpid_lgnd_cd")

# we want the adjusted ratio from column L (C_Outc12_13_14RatiosByGender)
# (alternatively just the normal ratio for nc for this year)
ratio.df <- ratio.df |>
  mutate(across(where(is.numeric), ~ replace_na(., 0))) |>
  mutate(n_nc_stp = count - nc_with_early_or_late) |>
  mutate(ratio = n_nc_stp / completers)

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

#--------- Age by Gender and Year Ratios ----------
# replicates lines 326:376 (main branch)
# testing: 1) run the code from here to to end of this section (line ~980)
# Compare T_DACSO_Near_Completers_RatioByGender_year  (on decimal) to t_dacso_near_completers_ratio_by_gender_year. (R version)
# Notes:
# 1) See notes on age ratios, this code replicates queries linked to the same Excel workbook, different sheet.

# 4.1: paste to col E (C_Outc12_13_14RatiosByGender)
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
  filter(
    cosc_grad_status_lgds_cd_group == "3"
  ) |>
  count(
    coci_subm_cd,
    tpid_lgnd_cd,
    age_group,
    prgm_credential_awarded_name,
    name = "Count"
  )

# 4.2: paste to col F (C_Outc12_13_14RatiosByGender)
near_completes_total_with_stp_by_gender_year <- base |>
  # This inner join only brings in has_stp_credentials - it has no other purpose
  # If we leave has_stp_credentials in base, there is no need for this.
  # inner_join(
  #   t_dacso_data_part_1_tempselection |>
  #     select(coci_stqu_id, has_stp_credential),
  #   by = "coci_stqu_id"
  # ) |>
  filter(
    has_stp_credential == "Yes"
  ) |>
  count(
    coci_subm_cd,
    tpid_lgnd_cd,
    age_group,
    prgm_credential_awarded_name,
    name = "nc_with_early_or_late"
  )

# 4.3 get full ratio (C_Outc12_13_14RatiosByGender)
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
  rename("gender" = "tpid_lgnd_cd")

# we want the adjusted ratio from column L (C_Outc12_13_14RatiosByGender)
# (or just the normal ratio for nc for this year)
ratio.df <- ratio.df |>
  mutate(across(where(is.numeric), ~ replace_na(., 0))) |>
  mutate(n_nc_stp = Count - nc_with_early_or_late) |>
  mutate(ratio = n_nc_stp / completers)

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
  # subtract one here so that it's the first half of the school year
  mutate(
    year = as.numeric(paste0('20', str_sub(coci_subm_cd, 7, 8))) - 1
  )


# GOT TO HERE...leaving for now but the TTRAIN flag is available if we want to include it.

# ---- TTRAIN tables ----

# Mirrors: Near_completes_total_by_CIP4_TTRAIN (C_Outc19/20 only)
near_completes_total_by_cip4_ttrain <- t_dacso_data_part_1 |>
  filter(
    cosc_grad_status_lgds_cd_group == "3",
    coci_subm_cd %in% c("C_Outc19", "C_Outc20")
  ) |>
  select(-age_group) |>
  # age group already introduced from lookup table
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

# Mirrors: Near_completes_total_with_STP_Credential_ByCIP4_TTRAIN (C_Outc19/20 only)
near_completes_total_with_stp_credential_bycip4_ttrain <- t_dacso_data_part_1 |>
  filter(
    coci_subm_cd %in% c("C_Outc19", "C_Outc20"),
    has_stp_credential == "Yes"
  ) |>
  select(-age_group) |>
  # age group already introduced from lookup table
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

# Mirrors: T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN
t_dacso_near_completers_ratiosageatgradcip4_ttrain <- near_completes_total_by_cip4_ttrain |>
  inner_join(
    t_pssm_projection_cred_grp |>
      rename_with(tolower) |>
      mutate(
        pssm_projection_credential = gsub(
          "Post-Degree",
          "Post-degree",
          pssm_projection_credential
        ),
        pssm_credential = gsub(
          " OR ",
          " or ",
          pssm_credential
        )
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
      lcip4_cred,
      lcp4_cd,
      lcp4_cip_4digits_name,
      cosc_grad_status_lgds_cd_group,
      ttrain,
      near_completers_stp_credentials
    )
  )


# ---- HISTORICAL TTRAIN tables ----

# Mirrors: Near_completes_total_by_CIP4_TTRAIN (all years)
near_completes_total_by_cip4_ttrain_history <- t_dacso_data_part_1 |>
  filter(
    cosc_grad_status_lgds_cd_group == "3"
  ) |>
  select(-age_group) |>
  # age group already introduced from lookup table
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

# Mirrors: Near_completes_total_with_STP_Credential_ByCIP4_TTRAIN (all years)
near_completes_total_with_stp_credential_bycip4_ttrain_history <- t_dacso_data_part_1 |>
  filter(
    has_stp_credential == "Yes"
  ) |>
  select(-age_group) |>
  # age group already introduced from lookup table
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

# Mirrors: T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history
# this needs to be fixed
t_dacso_near_completers_ratiosageatgradcip4_ttrain_history <- near_completes_total_by_cip4_ttrain_history |>
  mutate(
    prgm_credential_awarded_name = gsub(
      "Post-degree",
      "Post-Degree",
      prgm_credential_awarded_name
    )
  ) |>
  inner_join(
    t_pssm_projection_cred_grp |>
      rename_with(tolower),
    by = c("prgm_credential_awarded_name" = "pssm_projection_credential")
  ) |>
  left_join(
    near_completes_total_with_stp_credential_bycip4_ttrain_history |>
      mutate(age_group = as.character(age_group)) |>
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

## ------------------------------------ Clean Up --------------------------------------------------
# Current workflow:
#  - Write key tables back to sql server.  These are tables needed for downstream work, or tables
# that might be needed for later reference outside of this analysis.
#  - Close DB connections
#  - Remove all objects at the end of each script.
## ------------------------------------------------------------------------------------------------

# ---- Clean Up ----
# TODO: clean up this section
tables_to_keep <- c(
  "t_dacso_near_completers_ratio_by_gender_year",
  "t_dacso_near_completers_ratio_by_gender",
  "t_dacso_nearcompleters_ratioageatgradcip4",
  "t_dacso_near_completers_ratiosageatgradcip4_ttrain_history",
  "t_dacso_near_completers_ratiosageatgradcip4_ttrain"
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

dbDisconnect(con)
