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

# ============================================================================
# WHAT THIS SCRIPT PRODUCES
#   The program-mix distributions  P(CIP | credential, age)  used by step 07.
#   Two tables are written to the analyst's IDIR schema:
#     cohort_program_distributions_static_r     - 2023/24 snapshot mix
#     cohort_program_distributions_projected_r  - year-varying mix (12-yr horizon)
#   plus graduate_projections_r, tbl_program_projection_input_r, and the
#   qry_12_lcp4_lcippc_recode_9999_r lookup.
#
# WHERE THIS SITS IN THE MODEL
#   OCCSN(NOC) = GRADUATES(cred, age)
#              x P(CIP | cred, age)        <- cohort_program_distributions  (06)
#              x P(in labour supply | CIP) <- labour_supply_distribution    (02b-2)
#              x P(NOC | CIP, region)      <- occupation_distributions      (02b-3)
#   This script builds the FIRST factor. PERCENT is always
#       PERCENT = COUNT(CIP, cred, age) / TOTAL(cred, age) = P(CIP | cred, age).
#
# HOW IT IS ASSEMBLED
#   The two distribution tables are built up survey-by-survey. Each block:
#     1. computes weighted COUNT / TOTAL / PERCENT for one graduate stream,
#     2. tags it with a distinguishing SURVEY label, then
#     3. replaces only its own rows via the idempotent pattern
#          filter(!str_detect(SURVEY, "<tag>$")) |> bind_rows(<new>).
#   STATIC blocks fix the mix at 2023/24; PROJECTED blocks key on YEAR so the
#   mix can drift across the horizon (the Werner program supplies the yearly
#   counts in qry10c / qry12c).
#
# WHAT IS TTRAIN AND WHY IT MATTERS
#   TTRAIN ("trades training") is a code carried on t_cohorts_recoded that is
#   populated ONLY for trades-training programs (it is NA for everything else).
#   Two programs can share the same 4-digit CIP and credential yet lead to very
#   different occupations when one is delivered as trades training and the other
#   is not. Step 07 maps programs to occupations (NOCs) using the LCIP4_CRED /
#   LCIP2_CRED keys, and those keys EMBED the TTRAIN value, e.g.
#       "1 - 4603 - <TTRAIN> - DIPL"   vs   "1 - 4603 - DIPL"
#   so preserving the trades split here is what lets the occupation step send
#   trades and non-trades graduates of the same CIP to different NOCs.
#
#   How it flows through this script:
#     - cohort_ratios learns, per credential x CIP4 x grad-status x age cell, the
#       SHARE of weight in each trades stream (shares within a cell sum to 1).
#     - that share fans a single weighted CIP4 count out into one row per stream
#       (count x share), so the program mix gains trades-level granularity.
#     - cells with no trades training (RATIO is NA) pass through unsplit, and the
#       overall credential x age totals are unchanged - only redistributed.
#
# RUN CONTEXT
#   Sourced via the prep-for-*-run.R scripts (never run directly), so `con`,
#   `my_schema`, and the run flags (regular_run / qi_run / ptib_run) already
#   exist in the global environment. The connection is intentionally left open
#   for the next step; this script does not dbDisconnect().
# ============================================================================

# ---- Configure LAN and file paths ----
lan <- config::get("lan")
my_schema <- config::get("myschema")
db_schema <- config::get("dbschema")

# ---- Connection to decimal ----
db_config <- config::get("decimal")
con <- dbConnect(
  odbc::odbc(),
  Driver = db_config$driver,
  Server = db_config$server,
  Database = db_config$database,
  Trusted_Connection = "True"
)

# ---- Required inputs (must already be loaded by load-program-projections.R) ----
required_tables <- c(
  # actually used in load script
  "tbl_credential_highest_rank",
  "credential_non_dup",

  # Rollovers from last run (empty-but-typed shells filled in below)
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

  "dacso_near_completers_ratios_age_at_grad_cip4_ttrain",
  "t_cohorts_recoded"
)

if (ptib_run == TRUE) {
  required_tables <- c(
    "qry_private_credentials_06d1_cohort_dist",
    required_tables
  )
}

# Fail fast if the paired load script did not put everything in the environment.
missing <- required_tables[!sapply(required_tables, exists, where = .GlobalEnv)]

if (length(missing) > 0) {
  stop(paste(
    "The following required tables are missing from the environment:",
    paste(missing, collapse = ", ")
  ))
}


# if table does not exist, read it from db
for (table_name in missing) {
  print(table_name)

  if (!exists(table_name)) {
    read_table_from_db(table_name, my_schema, con)
  }
}


# na_vals <- c("", " ", "(Unspecified)", NA)  # REMOVED: defined but never used here.

# ============================================================================
# PART 1 of 2 - STATIC PROGRAM MIX  (the 2023/2024 snapshot) ----
#   Builds cohort_program_distributions_static. Streams that are ALSO projected
#   (PTIB, near-completers, apprenticeships) seed the projected table here too.
# ============================================================================

# ============================================================================
# STREAM 1 - PTIB private colleges  (SURVEY 'PTIB'; static + projected) ----
# ============================================================================
# PTIB run only: append the private-college cohort distribution (built in 05) to
# both tables before the public streams are added.
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

# ============================================================================
# STREAM 2 - Near-completers  (SURVEY 'qry_13d'; static + projected) ----
# ============================================================================
# Near-completer stream. Row-level counts live in NEAR_COMPLETERS_STP_CREDENTIALS
# (from step 03). Aggregate to age x CIP x grad-status x ttrain x credential, then
# compute the program-mix PERCENT within PSSM_CRED x AGE_GROUP.

# aggregate counts of near completers by age, cip, grad status, ttrain, credential
# row-level counts are in variable 'NEAR_COMPLETERS_STP_CREDENTIALS'

# some remapping needed when using dbo version

# Heritage note: when sourcing the dbo (not _r) version of the near-completers
# table, the " OR "/" or " casing differs and must be remapped first. Disabled
# because the _r table used here already has the correct casing.
#dacso_near_completers_ratios_age_at_grad_cip4_ttrain<-
#dacso_near_completers_ratios_age_at_grad_cip4_ttrain |> mutate(across(
#    c(PSSM_CREDENTIAL,  PSSM_CRED, LCIP4_CRED),
#    ~ gsub(" OR "," or ",.x)
#  ))

near_completers_totals <- dacso_near_completers_ratios_age_at_grad_cip4_ttrain |>
  summarise(
    COUNT = sum(NEAR_COMPLETERS_STP_CREDENTIALS, na.rm = TRUE),
    .by = c(
      PSSM_CREDENTIAL,
      PSSM_CRED,
      LCP4_CD,
      COSC_GRAD_STATUS_LGDS_CD_GROUP,
      TTRAIN,
      LCIP4_CRED,
      AGE_GROUP
    )
  ) |>
  # Denominator = credential x age; PERCENT is the share of that credential's
  # near-completers falling into each CIP4.
  mutate(
    TOTAL = sum(COUNT, na.rm = TRUE),
    PERCENT = if_else(TOTAL == 0, 0, as.numeric(COUNT) / as.numeric(TOTAL)),
    .by = c(PSSM_CRED, AGE_GROUP)
  )
#          PERCENT = COUNT(CIP, cred, age) / TOTAL(cred, age) = P(CIP | cred, age)

# Map near-completer age bands -> graduate-projection age bands (one band can
# fan out to several, hence many-to-many).
near_completers_totals <- near_completers_totals |>
  inner_join(
    tbl_age_groups_near_completers,
    by = join_by(AGE_GROUP == AGE_GROUP_LABEL_NEAR_COMPLETER_PROJECTION),
    relationship = "many-to-many"
  )


# SURVEY here is not a survey instrument — it's a lineage tag that says which processing "stream" produced each row. The names are inherited from the original dbo SQL queries (Q_012e, Q_013e, …, qry_13d), kept so every row can be traced back to its source and so each stream can replace only its own rows idempotently (filter(!str_detect(SURVEY, "Q012e$")) |> bind_rows(...)).
# The five values you listed are exactly the tags in cohort_program_distributions_static_r (the static-mix output). The projected table carries a different set (Q015e21, qry10c, qry12c).

# Shape to the common distribution schema (SURVEY tag, GRAD_STATUS/TTRAIN as
# character, LCIP2_CRED key, recoded AGE_GROUP, fixed YEAR).
near_completers_totals <- near_completers_totals |>
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

# Replace this stream's rows (any "3 - " near-completer rows) in both tables.
cohort_program_distributions_projected <- cohort_program_distributions_projected |>
  filter(!str_detect(PSSM_CRED, "^3 - ")) |>
  bind_rows(near_completers_totals)

cohort_program_distributions_static <- cohort_program_distributions_static |>
  filter(!str_detect(PSSM_CRED, "^3 - ")) |>
  bind_rows(near_completers_totals)


# ============================================================================
# STREAM 3 - Main public cohorts  (SURVEY 'Q012e'; static) ----
# ============================================================================
# Main public cohorts (non-grad, non-apprenticeship credentials).

# QA (interactive): list CIP4 codes in the input that are missing from the CIP
# lookup. Not assigned/used downstream - run by hand if validating.
# tbl_program_projection_input |>
#   anti_join(
#     infoware_l_cip_4digits_cip2016,
#     by = join_by(FINAL_CIP_CODE_4 == LCP4_CD)
#   ) |>
#   distinct(FINAL_CIP_CODE_4, Count)

# TTRAIN sub-split ratios from the survey cohorts. NOTE: dividing by the global
# positive-weight total here cancels out in RATIO below, so it only rescales an
# intermediate; the final RATIO is TotalWeight share within
# credential x CIP x grad-status x age.
# cohort_ratios <- t_cohorts_recoded |>
#   filter(GRAD_STATUS != "3", !is.na(TTRAIN), WEIGHT > 0) |>
#   inner_join(tbl_age_groups, by = join_by(AGE_GROUP == AGE_GROUP)) |>
#   summarise(
#     TotalWeight = sum(WEIGHT, na.rm = TRUE) /
#       sum(t_cohorts_recoded$WEIGHT[t_cohorts_recoded$WEIGHT > 0], na.rm = TRUE),
#     .by = c(PSSM_CREDENTIAL, LCP4_CD, GRAD_STATUS, AGE_GROUP_LABEL, TTRAIN)
#   ) |>
#   mutate(
#     RATIO = TotalWeight / sum(TotalWeight, na.rm = TRUE),
#     .by = c(PSSM_CREDENTIAL, LCP4_CD, GRAD_STATUS, AGE_GROUP_LABEL)
#   ) |>
#   select(-TotalWeight)
# cohort_ratios is computed twice back-to-back (same expression), and the first result is immediately overwritten by the second in line 286.
# This adds unnecessary work and makes it harder to reason about which definition is intended.

# Base weighted cohort: credential grouping x program input x survey-year weights,
# restricted to the public (non-grad, non-apprenticeship) credentials.
credential_cohorts <- t_pssm_projection_cred_grp |>
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
  )

# Weighted graduate volume by credential x grad-status x CIP4 x age.
credential_cohorts_weighted <- credential_cohorts |>
  summarise(
    WEIGHTED_BASE_COUNT = sum(Count * WEIGHT, na.rm = TRUE),
    .by = c(
      PSSM_CREDENTIAL,
      COSC_GRAD_STATUS_LGDS_CD,
      FINAL_CIP_CODE_4,
      AgeGroup
    )
  )

# ---- TTRAIN sub-split ratios ----
# Goal: learn, from the survey cohorts, how each
#   (credential x CIP4 x grad-status x age)
# cell divides across trades-training streams.
#
#   filter(): keep only trades rows (!is.na(TTRAIN)); drop near-completers
#     (GRAD_STATUS "3", handled in the qry_13d block) and zero-weight rows.
#   TotalWeight: weighted size of each trades stream. The division by the global
#     positive-weight total is a CONSTANT, so it cancels in RATIO below - it does
#     not affect the result and could be dropped (kept for parity with old runs).
#   RATIO: each stream's SHARE of its cell. Because .by excludes TTRAIN, the
#     shares within a cell sum to 1 (e.g. a cell with two streams -> 0.7 + 0.3;
#     a cell with one stream -> a single 1.0 row).
cohort_ratios <- t_cohorts_recoded |>
  filter(!is.na(TTRAIN), GRAD_STATUS != "3", WEIGHT > 0) |>
  inner_join(tbl_age_groups, by = join_by(AGE_GROUP == AGE_GROUP)) |>
  summarise(
    TotalWeight = sum(WEIGHT, na.rm = TRUE) /
      sum(t_cohorts_recoded$WEIGHT[t_cohorts_recoded$WEIGHT > 0], na.rm = TRUE),
    .by = c(PSSM_CREDENTIAL, LCP4_CD, GRAD_STATUS, AGE_GROUP_LABEL, TTRAIN)
  ) |>
  mutate(
    RATIO = TotalWeight / sum(TotalWeight, na.rm = TRUE),
    .by = c(PSSM_CREDENTIAL, LCP4_CD, GRAD_STATUS, AGE_GROUP_LABEL)
  ) |>
  select(-TotalWeight)

# Apply the TTRAIN sub-split. credential_cohorts_weighted has ONE row per
# (credential x grad-status x CIP4 x age); cohort_ratios may have SEVERAL rows
# for that cell (one per trades stream). This left_join is therefore ONE-TO-MANY
# and FANS each base-count row out into one row per trades stream.
#
# COUNT then ALLOCATES the base count across those streams in proportion to RATIO
# (multiply by the share - NOT divide). Cells with no trades training have
# RATIO = NA and pass through as a single unsplit row carrying the full count.
# Net effect: the same credential x age total, redistributed at trades-level
# granularity so step 07 can map each stream to its own occupations.
credential_cohorts_weighted_adjusted <- credential_cohorts_weighted |>
  mutate(COSC_GRAD_STATUS_LGDS_CD = as.character(COSC_GRAD_STATUS_LGDS_CD)) |>
  left_join(
    cohort_ratios,
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
  ) |>
  rename(AGE_GROUP = "AgeGroup") |>
  select(-RATIO, -WEIGHTED_BASE_COUNT)

# Build PSSM_CRED ("<status> - <credential>") and the program-mix PERCENT within
# PSSM_CRED x AGE_GROUP.
credential_cohorts_weighted_adjusted <- credential_cohorts_weighted_adjusted |>
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
    .by = c(PSSM_CRED, AGE_GROUP)
  ) |>
  mutate(
    PERCENT = if_else(TOTAL == 0, 0, as.numeric(COUNT) / as.numeric(TOTAL))
  )
#          PERCENT = COUNT(CIP, cred, age) / TOTAL(cred, age) = P(CIP | cred, age)

# Shape to the common schema (SURVEY tag, LCP4_CD, LCIP4_CRED / LCIP2_CRED keys).
final_credential_cohorts <- credential_cohorts_weighted_adjusted |>
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
    YEAR = "2023/2024"
  ) |>
  select(
    -c(
      COSC_GRAD_STATUS_LGDS_CD,
      FINAL_CIP_CODE_4,
      STATUS_PREFIX
    )
  )

# Replace the Q012e rows in the STATIC table (this stream is static only).
cohort_program_distributions_static <- cohort_program_distributions_static |>
  filter(!str_detect(SURVEY, "Q012e$")) |>
  bind_rows(final_credential_cohorts)


# ============================================================================
# STREAM 4 - Graduate-level credentials  (SURVEY 'Q013e'; static) ----
# ============================================================================
# Graduate-level credentials (GRCT or GRDP, PDEG, MAST, DOCT). These use the
# LCIPPC roll-up instead of raw CIP4.
# Note: lcip4_cd shows as 2D for masters/doctorates (cluster) - same as prior runs.

# CIP4 -> LCIPPC lookup, with the "9999" unknown CIP mapped to PPC "99".
qry_12_lcp4_lcippc_recode_9999 <- infoware_l_cip_6digits_cip2016 |>
  mutate(
    LCIP_LCIPPC_CD = if_else(LCIP_LCP4_CD == "9999", "99", LCIP_LCIPPC_CD)
  ) |>
  distinct(LCIP_LCP4_CD, LCIP_LCIPPC_CD)

# Base weighted cohort for grad-level credentials, recoded to LCIPPC.
graduate_credential_cohorts <-
  tbl_program_projection_input |>
  inner_join(
    t_pssm_projection_cred_grp |>
      filter(PSSM_CREDENTIAL %in% c('GRCT or GRDP', 'PDEG', 'MAST', 'DOCT')),
    by = join_by(PSI_CREDENTIAL_CATEGORY == PSSM_PROJECTION_CREDENTIAL)
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

# Weighted COUNT and program-mix PERCENT within credential x age.
graduate_credential_cohorts <- graduate_credential_cohorts |>
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
#          PERCENT = COUNT(CIP, cred, age) / TOTAL(cred, age) = P(CIP | cred, age)

# Shape to the common schema.
final_graduate_credential_cohorts <- graduate_credential_cohorts |>
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

# Replace the Q013e rows in the STATIC table (static only).
cohort_program_distributions_static <- cohort_program_distributions_static |>
  filter(!str_detect(SURVEY, "Q013e$")) |>
  bind_rows(final_graduate_credential_cohorts)

# ============================================================================
# STREAM 5 - Apprenticeships  (SURVEY 'Q014e'; static + projected) ----
# ============================================================================
# Apprenticeship credentials (APPRAPPR, APPRCERT): straight weighted counts. No
# ratio split is needed here - TTRAIN, LCIP4_CRED and LCIP2_CRED are taken AS-IS
# from t_cohorts_recoded (the keys already embed TTRAIN), and the weighted counts
# are summed directly.
apprenticeship_credential <- t_cohorts_recoded |>
  inner_join(tbl_age_groups, by = join_by(AGE_GROUP == AGE_GROUP)) |>
  filter(PSSM_CREDENTIAL %in% c('APPRAPPR', 'APPRCERT'), WEIGHT > 0)

# Weighted COUNT and program-mix PERCENT within credential x age.
apprenticeship_credential <- apprenticeship_credential |>
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
  )

# Shape to the common schema.
final_apprenticeship_credential <- apprenticeship_credential |>
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

# Replace the Q014e rows in BOTH tables (apprenticeships are static and projected).
cohort_program_distributions_projected <- cohort_program_distributions_projected |>
  filter(!str_detect(SURVEY, "Q014e$")) |>
  bind_rows(final_apprenticeship_credential)

cohort_program_distributions_static <- cohort_program_distributions_static |>
  filter(!str_detect(SURVEY, "Q014e$")) |>
  bind_rows(final_apprenticeship_credential)

# ============================================================================
# PART 2 of 2 - PROJECTED PROGRAM MIX  (year-varying over the 12-year horizon) ----
#   Builds cohort_program_distributions_projected. The Werner engine supplies
#   per-year counts; the rollover tables fan the static mix across future years.
# ============================================================================

# ============================================================================
# STEP 6 - APPSO graduate roll-forward Y2..Y10  (writes graduate_projections) ----
# ============================================================================
# --- move this to graduate_projections.R, I think ---
# Expand the static APPSO graduate projections across Y2..Y10, holding the
# graduate count constant for each forward year (read back from step 04 output).
graduate_projections <- dbReadTable(con, "Graduate_Projections_r")

new_projections <- graduate_projections |>
  filter(SURVEY == "APPSO") |>
  inner_join(
    t_appr_y2_to_y10,
    by = join_by(YEAR == Y1),
    relationship = "many-to-many"
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

# ============================================================================
# STEP 7 - Roll static mix forward  ('Q015e21' -> projected; 'Q015e22' -> static) ----
# ============================================================================
# Roll the STATIC near-completer + apprenticeship mix forward across Y2..Y12,
# producing year-varying rows for the PROJECTED table (and a parallel static tag).

# Expand every static row across the rollover horizon: 10 years in the future 2024/25-2034/35.
static_projected <- cohort_program_distributions_static |>
  inner_join(
    t_cohort_program_distributions_y2_to_y12 |> select(-ID),
    by = join_by(YEAR == Y1),
    relationship = "many-to-many"
  )

# Q015e21: only near-completers ("3 - ") and apprenticeships -> goes to PROJECTED.
static_projected_app_nc <- static_projected |>
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

# Q015e22: everything (the rolled-forward static counterpart) -> stays in STATIC.
static_projected_no_app_nc <- static_projected |>
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

# Replace Q015e21 rows in PROJECTED.
cohort_program_distributions_projected <- cohort_program_distributions_projected |>
  filter(!str_detect(SURVEY, "Q015e21$")) |>
  bind_rows(static_projected_app_nc)

# Replace Q015e22 rows in STATIC.
cohort_program_distributions_static <- cohort_program_distributions_static |>
  filter(!str_detect(SURVEY, "Q015e22$")) |>
  bind_rows(static_projected_no_app_nc)

# ============================================================================
# STEP 8 - Werner forecast engine  (turns history into 12-year counts) ----
# ============================================================================
# External forecasting engine that turns the historical CIP x cred x age counts
# into a 12-year forecast. We hand it a wide CSV (years as columns), run it, and
# read the forecast back. This is what makes the PROJECTED mix year-varying.
input_data <- tbl_program_projection_input |>
  select(-Expr1) |>
  # Fill every CIP x cred x age x year combination with 0 so the wide matrix the
  # Werner program expects has no gaps.
  complete(
    AgeGroup,
    PSI_CREDENTIAL_CATEGORY,
    FINAL_CIP_CODE_4,
    PSI_AWARD_SCHOOL_YEAR_DELAYED,
    fill = list(Count = 0)
  ) |>
  pivot_wider(
    names_from = "PSI_AWARD_SCHOOL_YEAR_DELAYED",
    values_from = "Count"
  ) |>
  rename(
    "CIP" = "FINAL_CIP_CODE_4",
    "AGE" = "AgeGroup",
    "CRED" = "PSI_CREDENTIAL_CATEGORY"
  ) |>
  # select(everything()) |>  # REMOVED: no-op, returns all columns unchanged.
  arrange(CIP, CRED, AGE)

dir.create("./tmp", showWarnings = FALSE)

write_csv(
  input_data,
  "./tmp/input-data.csv"
)

## run Werner program ----
# Reads ./tmp/input-data.csv and writes the forecast to ./tmp/output.csv.
source(glue::glue("./R/program projections.R"))

output_data <- read_delim(
  glue::glue("./tmp/output.csv"),
  delim = "\t",
  col_names = TRUE
)

# The forecast columns are the 12 school years 2023/2024 .. 2034/2035.
names(output_data) <- paste0(2023:(2023 + 11), "/", 2024:(2024 + 11))

# Re-attach the CIP/CRED/AGE keys to the forecast counts, then go long.
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

# QA (interactive): forecast totals by year. Not assigned/used downstream.
# t_predict_cip_cred_age_flipped |>
#   summarise(
#     SUMOFCOUNT = sum(Count, na.rm = TRUE),
#     .by = Year
#   )

# ============================================================================
# STREAM 9 - Public projected mix  (SURVEY 'qry10c'; projected) ----
# ============================================================================
# Adds projected counts to the PROJECTED table for the public credentials
# (NOT grad-level GRCT/PDEG/MAST/DOCT, and NOT apprenticeships - done earlier).

# First, keep only the rows already finalised in PROJECTED (apprenticeships,
# near-completers "3 -", and any "P -" private rows); the qry10c rows are rebuilt.
cohort_program_distributions_projected <- cohort_program_distributions_projected |>
  filter(
    PSSM_CRED %in%
      c('APPRAPPR', 'APPRCERT') |
      str_starts(PSSM_CRED, "3 -") |
      str_starts(PSSM_CRED, "P -")
  )

# Join the Werner forecast to the credential grouping; keep public credentials.
credential_age_projections <- t_predict_cip_cred_age_flipped |>
  inner_join(
    t_pssm_projection_cred_grp,
    by = join_by(CRED == PSSM_PROJECTION_CREDENTIAL)
  ) |>
  filter(
    !PSSM_CREDENTIAL %in%
      c('APPRAPPR', 'APPRCERT', 'GRCT or GRDP', 'PDEG', 'MAST', 'DOCT')
  ) |>
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
  )

# Program-mix PERCENT computed PER YEAR (credential x age x Year) - this is what
# lets the projected mix drift over the horizon.
credential_age_projections <- credential_age_projections |>
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
  mutate(
    TOTAL_VAL = sum(COUNT_VAL, na.rm = TRUE),
    PERCENT_VAL = if_else(
      TOTAL_VAL == 0,
      0,
      as.numeric(COUNT_VAL) / as.numeric(TOTAL_VAL)
    ),
    .by = c(PSSM_CRED_TMP, AGE, Year)
  )

# Shape to the common schema.
credential_age_projections <- credential_age_projections |>
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

cohort_program_distributions_projected <- cohort_program_distributions_projected |>
  filter(!str_detect(SURVEY, "qry10c$")) |>
  bind_rows(credential_age_projections)


# ============================================================================
# STREAM 10 - Grad-level projected mix  (SURVEY 'qry12c'; projected) ----
# ============================================================================
# Same as qry10c but for the grad-level credentials (GRCT or GRDP, PDEG, MAST,
# DOCT), recoded to LCIPPC.
credential_age_projections_grads <- t_predict_cip_cred_age_flipped |>
  inner_join(
    t_pssm_projection_cred_grp,
    by = join_by(CRED == PSSM_PROJECTION_CREDENTIAL)
  ) |>
  inner_join(
    qry_12_lcp4_lcippc_recode_9999,
    by = join_by(CIP == LCIP_LCP4_CD)
  ) |>
  filter(
    PSSM_CREDENTIAL %in% c('GRCT or GRDP', 'PDEG', 'MAST', 'DOCT')
  ) |>
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
  )

# Program-mix PERCENT per year (credential x age x Year).
credential_age_projections_grads <- credential_age_projections_grads |>
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
  )

credential_age_projections_grads <- credential_age_projections_grads |>
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
  bind_rows(credential_age_projections_grads)

# QA (interactive): static combinations (excluding edge age bands) that have no
# match in projected. Not assigned/used downstream - run by hand if validating.
# cohort_program_distributions_static |>
#   filter(!AGE_GROUP %in% c('15 to 16', '65 to 89')) |>
#   anti_join(
#     cohort_program_distributions_projected,
#     by = join_by(
#       YEAR,
#       AGE_GROUP,
#       LCP4_CD,
#       PSSM_CRED,
#       PSSM_CREDENTIAL
#     )
#   ) |>
#   select(
#     PSSM_CREDENTIAL,
#     PSSM_CRED,
#     LCP4_CD,
#     LCIP4_CRED,
#     AGE_GROUP,
#     YEAR,
#     COUNT
#   )

## ------------------------------------ Clean Up --------------------------------------------------
# Write the key tables back to SQL Server as "<name>_r" in the analyst's schema.
# NOTE: the connection is intentionally NOT closed here - it is left open for the
# next pipeline step that the prep script sources.
## ------------------------------------------------------------------------------------------------

tables_to_keep <- c(
  "cohort_program_distributions_projected",
  "cohort_program_distributions_static",
  "graduate_projections",
  "tbl_program_projection_input",
  "qry_12_lcp4_lcippc_recode_9999"
)

# Write one named global object to "<schema>"."<table_name>_r", overwriting.
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
