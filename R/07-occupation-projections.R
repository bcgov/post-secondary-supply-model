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

# This script computes the final NLS distributions based on program projections, labour supply distributions
# and occupation distributions.

# Tables were created in the cohorts database process and populated here
# Then all of the labour supply and occ queries were moved around a bit to
# incorporate looking for unknown labour supply and unknown occs in more steps
# than was previously necessary for the LCIP2 and private institution proxies.

# QI: The model is rerun a second time and all of these queries are also re-run
# to create an indicator which measures the quality of predictability for each NOC projection.

# Age groups: 17 to 19, 20 to 24, 25 to 29, and 30 to 34, and 35 to 64
# Credentials: From Diploma, Associate Degree, and Certificate Outcomes Survey cohorts.
# Survey years: 2019/2020 to 2030/2031 for PSSM 2019

#  Note: Q_1_Grad_Projections_by_Age_by_Program links to the following tables to exclude programs
#  where Student Outcomes results not available or inappropriate
#   - T_Exclude_from_Projections_LCIP4_CRED
#	  - T_Exclude_from_Projections_LCP4_CD
#	  - T_Exclude_from_Projections_PSSM_Credential

# the final output calculated using this formula
# OCCSN(NOC) = GRADUATES(cred, age)
#            × P(CIP | cred, age)        ← cohort_program_distributions  (06)
#            × P(in labour supply | CIP) ← labour_supply_distribution    (02b-2)
#            × P(NOC | CIP, region)      ← occupation_distributions       (02b-3)
#
# Fixes To do: some of the CIP2 variable names are missing an "I" in the Labour_Supply_Distribution datasets.

# ============================================================================
# WHAT THIS SCRIPT DOES
#   Final pipeline step. Turns graduate forecasts into occupation (NOC) supply
#   projections by chaining three probability distributions together:
#
#     OCCSN(NOC) = GRADUATES(cred, age)                                  (04)
#                x P(CIP | cred, age)        <- cohort_program_distributions (06)
#                x P(in labour supply | CIP) <- labour_supply_distribution  (02b-2)
#                x P(NOC | CIP, region)      <- occupation_distributions     (02b-3)
#
#   GRADS  = GRADUATES x P(CIP|cred,age)   (Q_1 series)
#   NLS    = GRADS     x P(supply|CIP)     (Q_2 series)  "New Labour Supply"
#   OCCSN  = NLS       x P(NOC|CIP,region) (Q_3 series)  occupation supply
#   then rolled up the NOC hierarchy (Q_4) and across regions (Q_5).
#
# THE KEY PATTERN - A PROXY WATERFALL
#   Every grad cell is matched to the distribution tables on its LCIP4_CRED key
#   (which encodes grad-status, CIP4, TTRAIN and credential). Many cells have no
#   exact survey match, so the code falls back through looser proxies, in order:
#       1. exact LCIP4_CRED match
#       2. "No_TT" tables       - same key WITHOUT the TTRAIN segment
#       3. private CERT<->DIPL  - swap the two private credentials for each other
#       4. 2-digit CIP (LCP2)   - borrow the broader program group's rates
#       5. LCP2 "No_TT", LCP2 private swap ...
#   At each step:  inner_join = cells matched at this level,
#                  anti_join  = leftovers passed down to the next proxy,
#                  bind_rows  = accumulate matched cells into a running union.
#   Whatever is STILL unmatched after every proxy is assigned NOC 99999 (Q_3e).
#
# WHY TTRAIN MATTERS HERE
#   TTRAIN ("trades training") is embedded inside LCIP4_CRED. A trades program's
#   key only matches the trades distribution; the "No_TT" tables are the SAME
#   distributions with TTRAIN stripped out, which is why they are the FIRST
#   fallback for programs whose TTRAIN value has no survey respondents.
#
# THE .x / .y SUFFIXES
#   Several joins collide on PSSM_CRED / LCP4_CD, so dplyr appends .x (left) and
#   .y (right). The private-swap steps rely on this to compare the two sides
#   (e.g. keep rows where PSSM_CRED.x != PSSM_CRED.y), then rename .x back.
#
# RUN CONTEXT
#   Sourced via the prep-for-*-run.R scripts; `decimal_con`, `my_schema` and the
#   run flags already exist. The flag assignments below are standalone-dev
#   defaults and are OVERRIDDEN when the orchestrator sources this file.
# ============================================================================

# ------------------------  libraries and global variables ------------------------
library(tidyverse)
# library(RODBC)   # REMOVED: unused. This script uses DBI/odbc, not RODBC.
library(config)
library(DBI)
library(glue)
library(assertthat)

# model toggle: "static" fixes the program mix at 2023/24; "projected" lets it
# drift by year (see 06). This picks which 06 output feeds the chain below.
model <- "static"
# Dev defaults only - overridden by the prep script when run in the pipeline.
ptib_run <- TRUE
regular_run <- TRUE
qi_run <- FALSE

# Select the program-mix table that matches the toggle.
if (model == "static") {
  cohort_program_distributions <- cohort_program_distributions_static
} else {
  cohort_program_distributions <- cohort_program_distributions_projected
}

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

# source("./sql/07-occupation-projections/occupation-projections.R")

# ------------------------ Check for required data tables ------------------------
# Fail fast if the paired load script did not put every input in the environment.
# List of required tables for Derived Tables and Lookups
# Might get FALSE that Cohort_Program_Distributions exists during first run-through.
# Use the Static Cohort_Program_Distributions table.

required_tables <- c(
  # Derived tables
  "labour_supply_distribution",
  "labour_supply_distribution_lcp2",
  "labour_supply_distribution_no_tt",
  "labour_supply_distribution_lcp2_no_tt",
  "occupation_distributions",
  "occupation_distributions_no_tt",
  "occupation_distributions_lcp2",
  "occupation_distributions_lcp2_no_tt",
  "cohort_program_distributions_projected",
  "cohort_program_distributions_static",
  "cohort_program_distributions",
  "graduate_projections",

  # Lookups
  "infoware_l_cip_4digits_cip2016",
  "infoware_l_cip_6digits_cip2016",
  "t_exclude_from_projections_lcp4_cd",
  "t_exclude_from_projections_lcip4_cred",
  "t_exclude_from_projections_pssm_credential",
  "t_exclude_from_labour_supply_unknown_lcp2_proxy",
  "tbl_age_groups",
  "tbl_age_groups_rollup",
  "t_noc_broad_categories",
  "t_lcp2_lcp4",
  "t_current_region_pssm_rollup_codes",
  "t_current_region_pssm_rollup_codes_bc",
  "t_pssm_cred_recode",
  "t_pssm_credential_grouping_appendix"
)

# Check for required data tables in global environment
for (table_name in required_tables) {
  assert_that(
    exists(table_name),
    msg = paste(
      "Error:",
      table_name,
      "does not exist in the global environment."
    )
  )
}


# --------------------   implement checks --------------------------
# in the interest of time, we aren't doing this.
# dbGetQuery(decimal_con, Count_Cohort_Program_Distributions)
# dbGetQuery(decimal_con, Count_Labour_Supply_Distribution1)
# dbGetQuery(decimal_con, Count_Labour_Supply_Distribution2)
# dbGetQuery(decimal_con, Count_Occupation_Distributions1) # We want this to contain all of our PSSM Credentials
# dbGetQuery(decimal_con, Count_Occupation_Distributions2)
# dbGetQuery(decimal_con, Occupation_Unknown)

# 4-digit -> 2-digit CIP map. Used by every LCP2 proxy step below to borrow a
# broader program group's rates when a 4-digit CIP has no survey data.
t_lcp2_lcp4 <- infoware_l_cip_6digits_cip2016 |>
  distinct(LCIP_LCP2_CD, LCIP_LCP4_CD)

# ------------- add PTIB to labour supply and occupation distributions -------------
# Private institutions aren't surveyed/surveyed separatedly, so we COPY the public distributions and
# re-tag them as private proxies (SURVEY = "PTIB", PSSM_CRED prefixed "P - ").
# Only the PERCENT / NLS ratios are meaningful for PTIB - the COUNT/TOTAL columns
# are borrowed and should not be read as true private counts.
# (Existing PTIB rows are deleted in the load script.)
if (ptib_run == TRUE) {
  # ---- Create new ptib distributions
  ptib_labour_supply_no_tt <- labour_supply_distribution_no_tt |>
    filter(
      PSSM_CREDENTIAL %in%
        c("CERT", "DIPL", "ADGR or UT", "BACH", "MAST", "DOCT"),
      !str_starts(LCIP4_CRED, "3 - ")
    ) |>
    mutate(
      SURVEY = "PTIB",
      PSSM_CRED = paste0("P - ", PSSM_CREDENTIAL),
      LCIP4_CRED = paste0("P - ", LCP4_CD, " - ", PSSM_CREDENTIAL),
      LCIP2_CRED = NA_character_
    )

  ptib_labour_supply_lcp2_no_tt <- labour_supply_distribution_lcp2_no_tt |>
    filter(
      PSSM_CREDENTIAL %in%
        c("CERT", "DIPL", "ADGR or UT", "BACH", "MAST", "DOCT"),
      !str_starts(LCP2_CRED, "3 - ")
    ) |>
    mutate(
      SURVEY = "PTIB",
      PSSM_CRED = paste0("P - ", PSSM_CREDENTIAL),
      LCP2_CRED = paste0("P - ", LCP2_CD, " - ", PSSM_CREDENTIAL)
    )

  ptib_occupation_distributions_lcp2_no_tt <- occupation_distributions_lcp2_no_tt |>
    filter(
      PSSM_CREDENTIAL %in%
        c("CERT", "DIPL", "ADGR or UT", "BACH", "MAST", "DOCT"),
      !str_starts(LCIP2_CRED, "3 - ")
    ) |>
    mutate(
      SURVEY = "PTIB",
      PSSM_CRED = paste0("P - ", PSSM_CREDENTIAL),
      LCIP2_CRED = paste0("P - ", LCP2_CD, " - ", PSSM_CREDENTIAL)
    )

  ptib_occupation_distributions_no_tt <- occupation_distributions_no_tt |>
    filter(
      PSSM_CREDENTIAL %in%
        c("CERT", "DIPL", "ADGR or UT", "BACH", "MAST", "DOCT"),
      !str_starts(LCIP4_CRED, "3 - ")
    ) |>
    mutate(
      SURVEY = "PTIB",
      PSSM_CRED = paste0("P - ", PSSM_CREDENTIAL),
      LCIP4_CRED = paste0("P - ", LCP4_CD, " - ", PSSM_CREDENTIAL)
    )

  # ---- Append new ptib distributions to labour supply and occupation distributions
  labour_supply_distribution_no_tt <- bind_rows(
    labour_supply_distribution_no_tt,
    ptib_labour_supply_no_tt
  )

  labour_supply_distribution_lcp2_no_tt <- bind_rows(
    labour_supply_distribution_lcp2_no_tt,
    ptib_labour_supply_lcp2_no_tt
  )

  occupation_distributions_no_tt <- bind_rows(
    occupation_distributions_no_tt,
    ptib_occupation_distributions_no_tt
  )

  occupation_distributions_lcp2_no_tt <- bind_rows(
    occupation_distributions_lcp2_no_tt,
    ptib_occupation_distributions_lcp2_no_tt
  )
}

# ============================================================================
# Q_1 SERIES - apply the program mix:  GRADS = GRADUATES x P(CIP | cred, age) ----
# ============================================================================
# Spread each credential-age graduate forecast across its 4-digit programs, then
# drop programs Student Outcomes can't or shouldn't project (the three exclude
# lists). distinct() guards against a duplicated dbo copy of graduate_projections.
q_1_grad_projections_by_age_by_program <- graduate_projections |>
  distinct(PSSM_CRED, AGE_GROUP, YEAR, GRADUATES) |>
  inner_join(
    cohort_program_distributions,
    by = join_by(PSSM_CRED, AGE_GROUP, YEAR)
  ) |>
  anti_join(
    t_exclude_from_projections_lcp4_cd,
    by = c("LCP4_CD" = "LCIP_LCP4_CD")
  ) |>
  anti_join(
    t_exclude_from_projections_pssm_credential,
    by = "PSSM_CREDENTIAL"
  ) |>
  anti_join(
    t_exclude_from_projections_lcip4_cred,
    by = "LCIP4_CRED"
  ) |>
  mutate(
    GRADS = GRADUATES * PERCENT # GRADS = graduates falling into this CIP
  ) |>
  select(
    PSSM_CREDENTIAL,
    PSSM_CRED,
    AGE_GROUP,
    YEAR,
    LCP4_CD,
    GRAD_STATUS,
    TTRAIN,
    LCIP4_CRED,
    GRADS
  )

# REMOVED: _static variant is never consumed downstream (it only differs by
# blanking LCIP4_CRED). Kept commented for parity with the SQL query of the same
# name; re-enable if a caller ever needs it.
# q_1_grad_projections_by_age_by_program_static <- graduate_projections |>
#   distinct(PSSM_CRED, AGE_GROUP, YEAR, GRADUATES) |>
#   inner_join(
#     cohort_program_distributions,
#     by = join_by(PSSM_CRED, AGE_GROUP, YEAR)
#   ) |>
#   anti_join(
#     t_exclude_from_projections_pssm_credential,
#     by = "PSSM_CREDENTIAL"
#   ) |>
#   anti_join(
#     t_exclude_from_projections_lcp4_cd,
#     by = c("LCP4_CD" = "LCIP_LCP4_CD")
#   ) |>
#   anti_join(
#     t_exclude_from_projections_lcip4_cred,
#     by = "LCIP4_CRED"
#   ) |>
#   mutate(
#     GRADS = GRADUATES * PERCENT,
#     LCIP4_CRED = NA_character_
#   ) |>
#   select(PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP, YEAR, LCP4_CD, GRAD_STATUS, TTRAIN, LCIP4_CRED, GRADS)

# REMOVED: interactive QA only (Q_1b check) - not assigned, just prints a wide
# grads-by-year table. Run by hand if validating against SQL.
# q_1_grad_projections_by_age_by_program |>
#   group_by(PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP, YEAR) |>
#   summarise(GRADS = sum(GRADS), .groups = "drop") |>
#   pivot_wider(names_from = YEAR, values_from = GRADS, values_fill = 0) |>
#   arrange(PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP)

# Roll the 9 fine age bands up to the 5 projection bands (17-19 ... 35-64) that
# the labour-supply and occupation distributions are keyed on.
q_1c_grad_projections_by_program <- q_1_grad_projections_by_age_by_program |>
  inner_join(
    tbl_age_groups,
    by = c("AGE_GROUP" = "AGE_GROUP_LABEL")
  ) |>
  inner_join(
    tbl_age_groups_rollup,
    by = "AGE_GROUP_ROLLUP"
  ) |>
  summarise(
    GRADS = sum(GRADS),
    .by = c(
      PSSM_CREDENTIAL,
      PSSM_CRED,
      AGE_GROUP_ROLLUP,
      AGE_GROUP_ROLLUP_LABEL,
      YEAR,
      GRAD_STATUS,
      TTRAIN,
      LCP4_CD,
      LCIP4_CRED
    )
  )

# REMOVED: LCP2 grads table is never consumed - the LCP2 proxy steps join the
# 4-digit grads table to labour_supply_distribution_lcp2 directly. Kept for SQL
# parity.
# q_1c_grad_projections_by_program_lcp2 <- q_1_grad_projections_by_age_by_program |>
#   inner_join(tbl_age_groups, by = c("AGE_GROUP" = "AGE_GROUP_LABEL")) |>
#   inner_join(tbl_age_groups_rollup, by = "AGE_GROUP_ROLLUP") |>
#   mutate(
#     LCP2_CD = str_sub(LCP4_CD, 1, 2),
#     LCIP2_CRED = paste0(
#       case_when(
#         str_sub(PSSM_CRED, 1, 1) %in% c("1", "3", "P") ~ paste0(str_sub(PSSM_CRED, 1, 1), " - "),
#         TRUE ~ ""
#       ),
#       LCP2_CD, " - ", if_else(is.na(TTRAIN), "", paste0(TTRAIN, " - ")), PSSM_CREDENTIAL
#     )
#   ) |>
#   summarise(
#     GRADS = sum(GRADS),
#     .by = c(PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP_ROLLUP, AGE_GROUP_ROLLUP_LABEL,
#             YEAR, LCP2_CD, GRAD_STATUS, TTRAIN, LCIP2_CRED)
#   )

# ============================================================================
# Q_2 SERIES - labour-supply factor:  NLS = GRADS x P(in labour supply | CIP) ----
#   Proxy waterfall (see header). Each step matches more leftovers; the running
#   union is the *_union table carried into the next step.
# ============================================================================

# Step 1 - exact LCIP4_CRED match (the best case).
q_2_labour_supply_by_lcip4_cred <- q_1c_grad_projections_by_program |>
  inner_join(
    labour_supply_distribution |>
      select(
        LCIP4_CRED,
        NEW_LABOUR_SUPPLY,
        CURRENT_REGION_PSSM_CODE_ROLLUP,
        AGE_GROUP_ROLLUP
      ),
    by = join_by(LCIP4_CRED, AGE_GROUP_ROLLUP),
    relationship = "many-to-many"
  ) |>
  mutate(NLS = GRADS * NEW_LABOUR_SUPPLY) |>
  select(-GRADS, -GRAD_STATUS)

# Step 2 - No-TT proxy: cells with NO exact match (anti_join) borrow the
# TTRAIN-stripped labour supply rate. Covers programs with blank/zero TTRAIN and
# private institutions (absent from the survey table entirely).
q_2a2_labour_supply_unknown_no_tt_proxy <- q_1c_grad_projections_by_program |>
  anti_join(
    labour_supply_distribution,
    by = join_by(LCIP4_CRED, AGE_GROUP_ROLLUP)
  ) |>
  summarise(
    GRADS = sum(GRADS),
    .by = c(
      PSSM_CREDENTIAL,
      PSSM_CRED,
      AGE_GROUP_ROLLUP,
      AGE_GROUP_ROLLUP_LABEL,
      TTRAIN,
      LCP4_CD,
      LCIP4_CRED,
      YEAR
    )
  ) |>
  inner_join(
    labour_supply_distribution_no_tt |>
      select(
        LCIP4_CRED,
        AGE_GROUP_ROLLUP,
        CURRENT_REGION_PSSM_CODE_ROLLUP,
        NEW_LABOUR_SUPPLY
      ),
    by = join_by(LCIP4_CRED, AGE_GROUP_ROLLUP),
    relationship = "many-to-many"
  ) |>
  mutate(NLS = GRADS * NEW_LABOUR_SUPPLY) |>
  select(names(q_2_labour_supply_by_lcip4_cred))

# Accumulate step 1 + step 2.
tmp_tbl_q_2a4_labour_supply_by_lcip4_cred_no_tt_union_tmp <- bind_rows(
  q_2_labour_supply_by_lcip4_cred,
  q_2a2_labour_supply_unknown_no_tt_proxy
)

rm(q_2a2_labour_supply_unknown_no_tt_proxy)

# Step 3 - private CERT<->DIPL swap: still-unmatched private cells borrow the
# OTHER private credential's rate (PTIB Branch treats CERT/DIPL interchangeably).
# The PSSM_CRED.x != PSSM_CRED.y filter enforces the swap.
q_2b2_labour_supply_unknown_private_cred_proxy <- q_1c_grad_projections_by_program |>
  anti_join(
    tmp_tbl_q_2a4_labour_supply_by_lcip4_cred_no_tt_union_tmp,
    by = join_by(LCIP4_CRED, AGE_GROUP_ROLLUP)
  ) |>
  filter(PSSM_CRED %in% c("P - CERT", "P - DIPL")) |>
  summarise(
    GRADS = sum(GRADS),
    .by = c(
      PSSM_CREDENTIAL,
      PSSM_CRED,
      AGE_GROUP_ROLLUP,
      AGE_GROUP_ROLLUP_LABEL,
      TTRAIN,
      LCP4_CD,
      LCIP4_CRED,
      YEAR,
      GRAD_STATUS
    )
  ) |>
  inner_join(
    labour_supply_distribution_no_tt |>
      filter(PSSM_CRED %in% c("P - CERT", "P - DIPL")) |>
      select(
        AGE_GROUP_ROLLUP,
        LCP4_CD,
        PSSM_CRED,
        CURRENT_REGION_PSSM_CODE_ROLLUP,
        NEW_LABOUR_SUPPLY
      ),
    by = join_by(AGE_GROUP_ROLLUP, LCP4_CD)
  ) |>
  filter(
    (PSSM_CRED.x != PSSM_CRED.y)
  ) |>
  mutate(NLS = GRADS * NEW_LABOUR_SUPPLY, PSSM_CRED = PSSM_CRED.x) |>
  select(names(tmp_tbl_q_2a4_labour_supply_by_lcip4_cred_no_tt_union_tmp))

q_2b3_labour_supply_by_lcip4_cred_private_cred_proxy_union <- bind_rows(
  tmp_tbl_q_2a4_labour_supply_by_lcip4_cred_no_tt_union_tmp,
  q_2b2_labour_supply_unknown_private_cred_proxy
)

rm(
  # q_2b_labour_supply_unknown,
  q_2b2_labour_supply_unknown_private_cred_proxy
)

# Step 4 - 2-digit CIP proxy: remaining cells borrow the broader LCP2 group's
# rate via t_lcp2_lcp4. Excludes CIP-51 medical programs (occupation link too
# tight to generalise); private "P - " programs are allowed to use this proxy.
q_2c_labour_supply_unknown_lcp2_proxy <- q_1c_grad_projections_by_program |>
  anti_join(
    q_2b3_labour_supply_by_lcip4_cred_private_cred_proxy_union,
    by = join_by(LCIP4_CRED, AGE_GROUP_ROLLUP)
  ) |>
  summarise(
    GRADS = sum(GRADS),
    .by = c(
      PSSM_CREDENTIAL,
      PSSM_CRED,
      AGE_GROUP_ROLLUP,
      AGE_GROUP_ROLLUP_LABEL,
      TTRAIN,
      LCP4_CD,
      LCIP4_CRED,
      YEAR,
      GRAD_STATUS
    )
  ) |>
  filter(
    !LCP4_CD %in%
      t_exclude_from_labour_supply_unknown_lcp2_proxy$LCIP_LCP4_CD |
      str_starts(LCIP4_CRED, "P - ")
  ) |>
  inner_join(
    t_lcp2_lcp4,
    by = c("LCP4_CD" = "LCIP_LCP4_CD")
  ) |>
  inner_join(
    labour_supply_distribution_lcp2 |> select(-PSSM_CREDENTIAL, -TTRAIN),
    by = join_by(AGE_GROUP_ROLLUP, PSSM_CRED, LCIP_LCP2_CD == LCP2_CD),
    relationship = "many-to-many"
  ) |>
  mutate(NLS = GRADS * NEW_LABOUR_SUPPLY) |>
  select(names(q_2b3_labour_supply_by_lcip4_cred_private_cred_proxy_union))


q_2c2_labour_supply_unknown_lcp2_proxy_union <- bind_rows(
  q_2b3_labour_supply_by_lcip4_cred_private_cred_proxy_union,
  q_2c_labour_supply_unknown_lcp2_proxy
)

rm(q_2c_labour_supply_unknown_lcp2_proxy)

# Step 5 - LCP2 No-TT proxy for the private leftovers.
# ...existing code (bug-fix comment about OR filter) ...
q_2c3_labour_supply_unknown <- q_1c_grad_projections_by_program |>
  anti_join(
    q_2c2_labour_supply_unknown_lcp2_proxy_union,
    by = join_by(LCIP4_CRED, AGE_GROUP_ROLLUP)
  ) |>
  summarise(
    GRADS = sum(GRADS),
    .by = c(
      PSSM_CREDENTIAL,
      PSSM_CRED,
      AGE_GROUP_ROLLUP,
      AGE_GROUP_ROLLUP_LABEL,
      TTRAIN,
      LCP4_CD,
      LCIP4_CRED,
      YEAR
    )
  )


q_2c4_labour_supply_unknown_lcp2_proxy_no_tt <- labour_supply_distribution_lcp2_no_tt |>
  select(
    LCP2_CD,
    AGE_GROUP_ROLLUP,
    PSSM_CRED,
    NEW_LABOUR_SUPPLY,
    CURRENT_REGION_PSSM_CODE_ROLLUP
  ) |>
  inner_join(
    t_exclude_from_labour_supply_unknown_lcp2_proxy |>
      right_join(
        q_2c3_labour_supply_unknown |>
          mutate(LCIP_LCP4_CD = LCP4_CD),
        by = c("LCIP_LCP4_CD" = "LCIP_LCP4_CD"),
        keep = TRUE
      ) |>
      rename(LCIP_LCP4_CD = LCIP_LCP4_CD.x) |>
      inner_join(t_lcp2_lcp4, by = c("LCP4_CD" = "LCIP_LCP4_CD")) |>
      select(-LCIP_LCP4_CD.y, -LCIP_LCP2_CD.x) |>
      rename(LCP2_CD = LCIP_LCP2_CD.y),
    by = c(
      "LCP2_CD" = "LCP2_CD",
      "AGE_GROUP_ROLLUP" = "AGE_GROUP_ROLLUP",
      "PSSM_CRED" = "PSSM_CRED"
    ),
    relationship = "many-to-many"
  ) |>
  filter(is.na(LCIP_LCP4_CD) & str_detect(LCIP4_CRED, "P - ")) |>
  mutate(NLS = GRADS * NEW_LABOUR_SUPPLY) |>
  select(names(q_2c2_labour_supply_unknown_lcp2_proxy_union))


tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union_tmp <- bind_rows(
  q_2c2_labour_supply_unknown_lcp2_proxy_union,
  q_2c4_labour_supply_unknown_lcp2_proxy_no_tt
)

rm(
  q_2c4_labour_supply_unknown_lcp2_proxy_no_tt,
  q_2c2_labour_supply_unknown_lcp2_proxy_union,
  q_2c3_labour_supply_unknown
)

# Step 6 - final LCP2 private CERT<->DIPL swap; result is the COMPLETE labour
# supply union that the Q_3 occupation series consumes.
q_2d2_labour_supply_unknown <- q_1c_grad_projections_by_program |>
  anti_join(
    tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union_tmp,
    by = join_by(LCIP4_CRED, AGE_GROUP_ROLLUP)
  ) |>
  summarise(
    GRADS = sum(GRADS),
    .by = c(
      PSSM_CREDENTIAL,
      PSSM_CRED,
      AGE_GROUP_ROLLUP,
      AGE_GROUP_ROLLUP_LABEL,
      TTRAIN,
      LCP4_CD,
      LCIP4_CRED,
      YEAR
    )
  )

q_2d3_labour_supply_unknown_lcp2_private_cred_proxy <- q_2d2_labour_supply_unknown |>
  filter(PSSM_CRED %in% c("P - CERT", "P - DIPL")) |>
  inner_join(
    t_lcp2_lcp4,
    by = c("LCP4_CD" = "LCIP_LCP4_CD")
  ) |>
  inner_join(
    labour_supply_distribution_lcp2_no_tt |>
      filter(PSSM_CRED %in% c("P - CERT", "P - DIPL")),
    by = c("AGE_GROUP_ROLLUP" = "AGE_GROUP_ROLLUP", "LCIP_LCP2_CD" = "LCP2_CD")
  ) |>
  filter(PSSM_CRED.x != PSSM_CRED.y) |>
  transmute(
    PSSM_CREDENTIAL = PSSM_CREDENTIAL.x,
    PSSM_CRED = PSSM_CRED.x,
    AGE_GROUP_ROLLUP,
    AGE_GROUP_ROLLUP_LABEL,
    YEAR,
    TTRAIN = TTRAIN, # just TTRAIN?
    LCP4_CD,
    LCIP4_CRED,
    NEW_LABOUR_SUPPLY,
    CURRENT_REGION_PSSM_CODE_ROLLUP,
    NLS = GRADS * NEW_LABOUR_SUPPLY
  )


tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union <- bind_rows(
  tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union_tmp,
  q_2d3_labour_supply_unknown_lcp2_private_cred_proxy |>
    select(names(tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union_tmp))
)

rm(
  q_2d2_labour_supply_unknown,
  q_2d3_labour_supply_unknown_lcp2_private_cred_proxy
)

# REMOVED: q_2f_labour_supply was computed then immediately rm()'d (author marked
# "not used?"). It measured residual unmatched labour supply for QA only.
# q_2f_labour_supply <- q_1c_grad_projections_by_program |>
#   anti_join(tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union,
#             by = join_by(LCIP4_CRED, AGE_GROUP_ROLLUP)) |>
#   summarise(GRADS = sum(GRADS),
#             .by = c(PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP_ROLLUP,
#                     AGE_GROUP_ROLLUP_LABEL, LCP4_CD, LCIP4_CRED, YEAR))
# rm(q_2f_labour_supply)

# Free the Q_1 intermediates (all matched into the labour-supply union now).
removers <- ls()[grep("q_1", ls())]
rm(list = removers)

# ============================================================================
# Q_3 SERIES - occupation factor:  OCCSN = NLS x P(NOC | CIP, region) ----
#   SAME proxy waterfall as Q_2, but now adds CURRENT_REGION_PSSM_CODE_ROLLUP to
#   the match keys and multiplies NLS by the occupation share to get OCCSN.
# ============================================================================

# Step 1 - exact LCIP4_CRED x region match.
q_3_occupations_by_lcip4_cred <- tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union |>
  inner_join(
    occupation_distributions |>
      select(
        NOC,
        PERCENT,
        LCIP4_CRED,
        CURRENT_REGION_PSSM_CODE_ROLLUP,
        AGE_GROUP_ROLLUP
      ),
    by = join_by(
      LCIP4_CRED,
      CURRENT_REGION_PSSM_CODE_ROLLUP,
      AGE_GROUP_ROLLUP
    ),
    relationship = "many-to-many"
  ) |>
  mutate(OCCSN = NLS * PERCENT) |>
  select(
    PSSM_CREDENTIAL,
    PSSM_CRED,
    AGE_GROUP_ROLLUP,
    AGE_GROUP_ROLLUP_LABEL,
    YEAR,
    TTRAIN,
    LCP4_CD,
    LCIP4_CRED,
    CURRENT_REGION_PSSM_CODE_ROLLUP,
    NOC,
    PERCENT,
    OCCSN
  )

# Leftovers with no occupation match at this level.
q_3b_occupations_unknown <- tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union |>
  anti_join(
    occupation_distributions |>
      select(
        NOC,
        PERCENT,
        LCIP4_CRED,
        CURRENT_REGION_PSSM_CODE_ROLLUP,
        AGE_GROUP_ROLLUP
      ),
    by = join_by(
      LCIP4_CRED,
      CURRENT_REGION_PSSM_CODE_ROLLUP,
      AGE_GROUP_ROLLUP
    )
  )

# Step 2 - No-TT occupation proxy for those leftovers.
q_3b11_occupations_unknown_no_tt_proxy <- q_3b_occupations_unknown |>
  inner_join(
    occupation_distributions_no_tt |>
      select(
        NOC,
        PERCENT,
        LCP4_CD,
        PSSM_CRED,
        CURRENT_REGION_PSSM_CODE_ROLLUP,
        AGE_GROUP_ROLLUP
      ),
    by = join_by(
      CURRENT_REGION_PSSM_CODE_ROLLUP,
      LCP4_CD,
      AGE_GROUP_ROLLUP,
      PSSM_CRED
    ),
    relationship = "many-to-many"
  ) |>
  mutate(OCCSN = NLS * PERCENT) |>
  select(
    PSSM_CREDENTIAL,
    PSSM_CRED,
    AGE_GROUP_ROLLUP,
    AGE_GROUP_ROLLUP_LABEL,
    YEAR,
    TTRAIN,
    LCP4_CD,
    LCIP4_CRED,
    CURRENT_REGION_PSSM_CODE_ROLLUP,
    NOC,
    PERCENT,
    OCCSN
  )

q_3b12_occupations_by_lcip4_cred_no_tt_proxy_union <- bind_rows(
  q_3_occupations_by_lcip4_cred,
  q_3b11_occupations_unknown_no_tt_proxy |>
    select(names(q_3_occupations_by_lcip4_cred))
)

tmp_tbl_q3b12_occupations_by_lcip4_cred_no_tt_union_tmp <- q_3b12_occupations_by_lcip4_cred_no_tt_proxy_union

# Recompute leftovers against the running union (note keys include YEAR here).
q_3b14_occupations_unknown <- tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union |>
  anti_join(
    tmp_tbl_q3b12_occupations_by_lcip4_cred_no_tt_union_tmp,
    by = join_by(
      YEAR,
      CURRENT_REGION_PSSM_CODE_ROLLUP,
      LCIP4_CRED,
      AGE_GROUP_ROLLUP
    )
  )


# Step 3 - private CERT<->DIPL occupation swap.
q_3b2_occupations_unknown_private_cred_proxy <-
  q_3b14_occupations_unknown |>
  inner_join(
    tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union |>
      select(
        LCIP4_CRED,
        YEAR,
        AGE_GROUP_ROLLUP,
        PSSM_CRED,
        LCP4_CD,
        CURRENT_REGION_PSSM_CODE_ROLLUP
      ),
    by = join_by(LCIP4_CRED, YEAR, AGE_GROUP_ROLLUP, PSSM_CRED),
    relationship = "many-to-many"
  ) |>
  inner_join(
    occupation_distributions_no_tt |>
      select(
        NOC,
        PERCENT,
        LCP4_CD,
        AGE_GROUP_ROLLUP,
        CURRENT_REGION_PSSM_CODE_ROLLUP,
        PSSM_CRED
      ),
    by = join_by(
      LCP4_CD.y == LCP4_CD,
      AGE_GROUP_ROLLUP,
      CURRENT_REGION_PSSM_CODE_ROLLUP.y == CURRENT_REGION_PSSM_CODE_ROLLUP
    ),
    relationship = "many-to-many"
  ) |>
  filter(
    (PSSM_CRED.x == 'P - CERT' & PSSM_CRED.y == 'P - DIPL') |
      (PSSM_CRED.x == 'P - DIPL' & PSSM_CRED.y == 'P - CERT')
  ) |>
  mutate(OCCSN = NLS * PERCENT) |>
  rename(
    PSSM_CRED = PSSM_CRED.x,
    LCP4_CD = LCP4_CD.x,
    CURRENT_REGION_PSSM_CODE_ROLLUP = CURRENT_REGION_PSSM_CODE_ROLLUP.x
  ) |>
  select(names(tmp_tbl_q3b12_occupations_by_lcip4_cred_no_tt_union_tmp))

# dbExecute(decimal_con, Q_3b3_Occupations_by_LCIP4_CRED_Private_Cred_Proxy_Union)
q_3b3_occupations_by_lcip4_cred_private_cred_proxy_union <- bind_rows(
  tmp_tbl_q3b12_occupations_by_lcip4_cred_no_tt_union_tmp,
  q_3b2_occupations_unknown_private_cred_proxy |>
    select(names(tmp_tbl_q3b12_occupations_by_lcip4_cred_no_tt_union_tmp))
)

# dbExecute(decimal_con, Q_3b4_Occupations_Unknown)
q_3b4_occupations_unknown <- tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union |>
  anti_join(
    q_3b3_occupations_by_lcip4_cred_private_cred_proxy_union,
    by = join_by(
      LCIP4_CRED,
      AGE_GROUP_ROLLUP,
      CURRENT_REGION_PSSM_CODE_ROLLUP,
      YEAR
    )
  )

rm(
  q_3_occupations_by_lcip4_cred,
  q_3b_occupations_unknown,
  q_3b11_occupations_unknown_no_tt_proxy,
  q_3b12_occupations_by_lcip4_cred_no_tt_proxy_union,
  q_3b14_occupations_unknown,
  q_3b3_occupations_by_lcip4_cred_private_cred_proxy_union
)


# Step 4 - 2-digit CIP occupation proxy (mirrors Q_2 step 4).
# --- 03C Series
#dbExecute(decimal_con, Q_3c_Occupations_Unknown_LCP2_Proxy)
q_3c_occupations_unknown_lcp2_proxy <- q_3b4_occupations_unknown |>
  left_join(
    t_exclude_from_labour_supply_unknown_lcp2_proxy |>
      transmute(LCP4_CD = LCIP_LCP4_CD, LCIP_LCP4_CD),
    by = join_by(LCP4_CD)
  ) |>
  filter(
    is.na(LCIP_LCP4_CD) | str_starts(LCIP4_CRED, "P - ")
  ) |>
  inner_join(
    t_lcp2_lcp4,
    by = c("LCP4_CD" = "LCIP_LCP4_CD"),
    relationship = "many-to-many"
  ) |>
  inner_join(
    occupation_distributions_lcp2 |> select(-PSSM_CREDENTIAL, -TTRAIN),
    by = join_by(
      AGE_GROUP_ROLLUP,
      PSSM_CRED,
      LCIP_LCP2_CD == LCP2_CD,
      CURRENT_REGION_PSSM_CODE_ROLLUP
    ),
    relationship = "many-to-many"
  ) |>
  mutate(OCCSN = NLS * PERCENT) |>
  select(
    PSSM_CREDENTIAL,
    PSSM_CRED,
    AGE_GROUP_ROLLUP,
    AGE_GROUP_ROLLUP_LABEL,
    YEAR,
    TTRAIN,
    LCP4_CD,
    LCIP4_CRED,
    CURRENT_REGION_PSSM_CODE_ROLLUP,
    NOC,
    PERCENT,
    OCCSN
  )


# Steps 5-6 - LCP2 No-TT and LCP2 private swap occupation proxies.
# --- 03D Series
q_3d_occupations_by_lcip4_cred_lcp2_union <- bind_rows(
  tmp_tbl_q3b12_occupations_by_lcip4_cred_no_tt_union_tmp,
  q_3b2_occupations_unknown_private_cred_proxy,
  q_3c_occupations_unknown_lcp2_proxy
)

tmp_tbl_q_3d_occupations_by_lcip4_cred_lcp2_union_tmp <- q_3d_occupations_by_lcip4_cred_lcp2_union

q_3d2_occupations_unknown <- tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union |>
  anti_join(
    tmp_tbl_q_3d_occupations_by_lcip4_cred_lcp2_union_tmp,
    by = join_by(
      LCIP4_CRED,
      CURRENT_REGION_PSSM_CODE_ROLLUP,
      YEAR,
      AGE_GROUP_ROLLUP
    )
  )


q_3d21_occupations_unknown_lcp2_proxy_no_tt <-
  t_lcp2_lcp4 |>
  inner_join(
    occupation_distributions_lcp2_no_tt |>
      select(
        NOC,
        PERCENT,
        LCP2_CD,
        AGE_GROUP_ROLLUP,
        CURRENT_REGION_PSSM_CODE_ROLLUP,
        PSSM_CRED
      ),
    by = join_by(LCIP_LCP2_CD == LCP2_CD),
    relationship = "many-to-many"
  ) |>
  select(-LCIP_LCP2_CD, ) |>
  inner_join(
    q_3d2_occupations_unknown |>
      left_join(
        t_exclude_from_labour_supply_unknown_lcp2_proxy |>
          mutate(LCP4_CD = LCIP_LCP4_CD),
        by = join_by(LCP4_CD)
      ),
    by = join_by(
      LCIP_LCP4_CD == LCP4_CD,
      AGE_GROUP_ROLLUP,
      CURRENT_REGION_PSSM_CODE_ROLLUP,
      PSSM_CRED
    ),
    relationship = "many-to-many"
  ) |>
  rename(LCP4_CD = LCIP_LCP4_CD) |>
  rename(LCIP_LCP4_CD = LCIP_LCP4_CD.y) |>
  filter(is.na(LCIP_LCP4_CD) & str_detect(LCIP4_CRED, "P - ")) |>
  mutate(OCCSN = NLS * PERCENT)

q_3d22_occupations_by_lcip4_cred_lcp2_no_t_proxy_union <- bind_rows(
  tmp_tbl_q_3d_occupations_by_lcip4_cred_lcp2_union_tmp,
  q_3d21_occupations_unknown_lcp2_proxy_no_tt
)


q_3d24_occupations_unknown <- tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union |>
  left_join(
    q_3d22_occupations_by_lcip4_cred_lcp2_no_t_proxy_union |>
      select(
        LCIP4_CRED,
        CURRENT_REGION_PSSM_CODE_ROLLUP,
        YEAR,
        AGE_GROUP_ROLLUP
      ),
    by = join_by(
      CURRENT_REGION_PSSM_CODE_ROLLUP,
      LCIP4_CRED,
      YEAR,
      AGE_GROUP_ROLLUP
    ),
    keep = TRUE,
    relationship = "many-to-many"
  ) |>
  filter(
    is.na(AGE_GROUP_ROLLUP.y) &
      is.na(YEAR.y) &
      is.na(LCIP4_CRED.y) &
      is.na(CURRENT_REGION_PSSM_CODE_ROLLUP.y)
  ) |>
  select(-ends_with(".y")) |>
  rename_with(~ str_replace(., "\\.x$", ""))

q_3d3_occupations_unknown_lcp2_private_cred_proxy <- q_3d24_occupations_unknown |>
  filter(PSSM_CRED %in% c("P - CERT", "P - DIPL")) |>
  inner_join(
    t_lcp2_lcp4,
    by = join_by(LCP4_CD == LCIP_LCP4_CD)
  ) |>
  inner_join(
    occupation_distributions_lcp2_no_tt |>
      filter(PSSM_CRED %in% c("P - CERT", "P - DIPL")),
    by = join_by(
      CURRENT_REGION_PSSM_CODE_ROLLUP,
      AGE_GROUP_ROLLUP,
      LCIP_LCP2_CD == LCP2_CD
    )
  ) |>
  filter(PSSM_CRED.x != PSSM_CRED.y) |>
  mutate(OCCSN = NLS * PERCENT) |>
  select(
    PSSM_CREDENTIAL = PSSM_CREDENTIAL.x,
    PSSM_CRED = PSSM_CRED.x,
    AGE_GROUP_ROLLUP,
    AGE_GROUP_ROLLUP_LABEL,
    YEAR,
    TTRAIN = TTRAIN.x, # just TTRAIN?
    LCP4_CD,
    LCIP4_CRED,
    CURRENT_REGION_PSSM_CODE_ROLLUP,
    NOC,
    PERCENT,
    OCCSN
  )

q_3d4_occupations_by_lcip4_cred_lcp2_lcp2_private_union <- bind_rows(
  tmp_tbl_q_3d_occupations_by_lcip4_cred_lcp2_union_tmp,
  q_3d21_occupations_unknown_lcp2_proxy_no_tt,
  q_3d3_occupations_unknown_lcp2_private_cred_proxy
)

# --- 03E Series
# dbExecute(decimal_con, Q_3e_Occupations_Unknown)
# dbExecute(decimal_con, Q_3e2_Occupations_Unknown)
# dbExecute(decimal_con, Q_3e3_Occupations_by_LCIP4_CRED_LCP2_Union)

# Unknown bucket: anything STILL unmatched gets NOC 99999 at PERCENT = 1, so its
# full NLS flows through as "occupation unknown" rather than being dropped.

q_3e_occupations_unknown <- tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union |>
  anti_join(
    q_3d4_occupations_by_lcip4_cred_lcp2_lcp2_private_union,
    by = join_by(
      LCIP4_CRED,
      CURRENT_REGION_PSSM_CODE_ROLLUP,
      AGE_GROUP_ROLLUP,
      YEAR
    )
  ) |>
  group_by(
    PSSM_CREDENTIAL,
    PSSM_CRED,
    AGE_GROUP_ROLLUP,
    AGE_GROUP_ROLLUP_LABEL,
    YEAR,
    TTRAIN,
    LCP4_CD,
    LCIP4_CRED,
    CURRENT_REGION_PSSM_CODE_ROLLUP
  ) |>
  summarise(
    NLS = sum(NLS),
    .groups = "drop"
  )

q_3e2_occupations_unknown <- tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union |>
  anti_join(
    q_3d4_occupations_by_lcip4_cred_lcp2_lcp2_private_union,
    by = join_by(
      LCIP4_CRED,
      CURRENT_REGION_PSSM_CODE_ROLLUP,
      AGE_GROUP_ROLLUP,
      YEAR
    )
  ) |>
  group_by(
    PSSM_CREDENTIAL,
    PSSM_CRED,
    AGE_GROUP_ROLLUP,
    AGE_GROUP_ROLLUP_LABEL,
    YEAR,
    TTRAIN,
    LCP4_CD,
    LCIP4_CRED,
    CURRENT_REGION_PSSM_CODE_ROLLUP
  ) |>
  summarise(
    OCCSN = sum(NLS),
    .groups = "drop"
  ) |>
  filter(OCCSN > 0) |>
  mutate(
    NOC = 99999,
    PERCENT = 1
  ) |>
  transmute(
    PSSM_CREDENTIAL,
    PSSM_CRED,
    AGE_GROUP_ROLLUP,
    AGE_GROUP_ROLLUP_LABEL,
    YEAR,
    TTRAIN,
    LCP4_CD,
    LCIP4_CRED,
    CURRENT_REGION_PSSM_CODE_ROLLUP,
    NOC,
    PERCENT,
    OCCSN
  )

# Final occupation union = all matched cells + the 99999 unknown bucket.
q_3e3_occupations_by_lcip4_cred_lcp2_union <- bind_rows(
  q_3d4_occupations_by_lcip4_cred_lcp2_lcp2_private_union |>
    mutate(NOC = as.double(NOC)),
  q_3e2_occupations_unknown |> mutate(NOC = as.double(NOC))
)

# Free the Q_2 intermediates.
# remove q2 queries
removers <- ls()[grep("q_2", ls())]
rm(list = removers)


# Keep only positive OCCSN; this is the single table the NOC rollups build on.
# --- 03F Series
# dbExecute(decimal_con, Q_3f_Occupations)
tmp_tbl_q_3d_occupations_by_lcip4_cred_lcp2_union <- q_3e3_occupations_by_lcip4_cred_lcp2_union |>
  filter(OCCSN > 0 & !is.na(OCCSN)) |>
  select(
    PSSM_CREDENTIAL,
    PSSM_CRED,
    AGE_GROUP_ROLLUP,
    AGE_GROUP_ROLLUP_LABEL,
    YEAR,
    TTRAIN,
    LCP4_CD,
    LCIP4_CRED,
    CURRENT_REGION_PSSM_CODE_ROLLUP,
    NOC,
    PERCENT,
    OCCSN
  )

# Free all Q_3 intermediates EXCEPT the final union just built.
# remove q3 queries except for the final union table
removers <- ls()[grep("q_3|q3", ls())]
removers <- setdiff(
  removers,
  "tmp_tbl_q_3d_occupations_by_lcip4_cred_lcp2_union"
)
rm(list = removers)


# ============================================================================
# Q_4 SERIES - roll OCCSN up the NOC hierarchy (1- to 5-digit) ----
#   Attach the NOC broad->unit hierarchy and region names, then total OCCSN at
#   each NOC level and pivot years to columns. NOC_*_LEVEL = digit count.
# ============================================================================

# ---- Q_4_NOC_D Series ----
# Theses haven't been translated - do we use them?
# dbExecute(decimal_con, Q_4_NOC_1D_Totals_by_PSSM_CRED)
# dbExecute(decimal_con, Q_4_NOC_2D_Totals_by_PSSM_CRED)
# dbExecute(decimal_con, Q_4_NOC_2D_Totals_by_PSSM_CRED_Appendix)
# dbExecute(decimal_con, Q_4_NOC_3D_Totals_by_PSSM_CRED)
# dbExecute(decimal_con, Q_4_NOC_4D_Totals_by_PSSM_CRED)
# dbExecute(decimal_con, Q_4_NOC_5D_Totals_by_PSSM_CRED)

# the following section handles these queries together
# dbExecute(decimal_con, Q_4_NOC_1D_Totals_by_Year)
# dbExecute(decimal_con, Q_4_NOC_2D_Totals_by_Year)
# dbExecute(decimal_con, Q_4_NOC_3D_Totals_by_Year)
# dbExecute(decimal_con, Q_4_NOC_4D_Totals_by_Year)
# dbExecute(decimal_con, Q_4_NOC_5D_Totals_by_Year)
# FIXME dbExecute(decimal_con, Q_4_NOC_5D_Totals_by_Year_Input_for_Rounding)

noc_projections_base <- tmp_tbl_q_3d_occupations_by_lcip4_cred_lcp2_union |>
  mutate(
    NOC = str_pad(as.character(NOC), width = 5, side = "left", pad = "0")
  ) |>
  inner_join(
    t_current_region_pssm_rollup_codes |>
      select(-OLD_CURRENT_REGION_PSSM_CODE_ROLLUP),
    by = "CURRENT_REGION_PSSM_CODE_ROLLUP"
  ) |>
  inner_join(
    t_noc_broad_categories,
    by = c("NOC" = "UNIT_GROUP_CODE")
  ) |>
  transmute(
    NOC_1 = BROAD_CATEGORY_CODE,
    NOC_2 = MAJOR_GROUP_CODE,
    NOC_3 = SUB_MAJOR_GROUP_CODE,
    NOC_4 = MINOR_GROUP_CODE,
    NOC_5 = NOC,
    NOC_1_ENGLISH_NAME = BROAD_CATEGORY_ENGLISH_NAME,
    NOC_2_ENGLISH_NAME = MAJOR_GROUP_ENGLISH_NAME,
    NOC_3_ENGLISH_NAME = SUB_MAJOR_ENGLISH_NAME,
    NOC_4_ENGLISH_NAME = MINOR_GROUP_ENGLISH_NAME,
    NOC_5_ENGLISH_NAME = ENGLISH_NAME,
    NOC_1_LEVEL = str_length(NOC_1),
    NOC_2_LEVEL = str_length(NOC_2),
    NOC_3_LEVEL = str_length(NOC_3),
    NOC_4_LEVEL = str_length(NOC_4),
    NOC_5_LEVEL = str_length(NOC_5),
    AGE_GROUP_ROLLUP,
    AGE_GROUP_ROLLUP_LABEL,
    YEAR,
    CURRENT_REGION_PSSM_CODE_ROLLUP,
    CURRENT_REGION_PSSM_NAME_ROLLUP,
    OCCSN
  )

# Helper: total OCCSN at one NOC level and spread years across columns.
sum_noc_totals <- function(data, level) {
  noc_cols <- paste0(
    c("NOC_", "NOC_", "NOC_"),
    level,
    c("", "_LEVEL", "_ENGLISH_NAME")
  )

  data |>
    summarise(
      OCCSN = sum(OCCSN),
      .by = c(
        YEAR,
        AGE_GROUP_ROLLUP_LABEL,
        CURRENT_REGION_PSSM_CODE_ROLLUP,
        CURRENT_REGION_PSSM_NAME_ROLLUP,
        all_of(noc_cols)
      )
    ) |>
    pivot_wider(names_from = YEAR, values_from = OCCSN, values_fill = 0) |>
    rename(
      NOC = !!sym(noc_cols[1]),
      NOC_LEVEL = !!sym(noc_cols[2]),
      ENGLISH_NAME = !!sym(noc_cols[3])
    )
}

q_4_noc_1d_totals_by_year <- sum_noc_totals(noc_projections_base, 1)
q_4_noc_2d_totals_by_year <- sum_noc_totals(noc_projections_base, 2)
q_4_noc_3d_totals_by_year <- sum_noc_totals(noc_projections_base, 3)
q_4_noc_4d_totals_by_year <- sum_noc_totals(noc_projections_base, 4)
q_4_noc_5d_totals_by_year <- sum_noc_totals(noc_projections_base, 5)
# ---- Q_4_NOC_Totals Series ----
# FIXME dbGetQuery(decimal_con, Q_4_NOC_Totals_by_Year_and_PSSM_CRED)
# dbExecute(decimal_con, Q_4_NOC_Totals_by_Year)
# dbExecute(decimal_con, Q_4_NOC_Totals_by_Year_BC)
# dbExecute(decimal_con, Q_4_NOC_Totals_by_Year_Total)

# ---- Q_4_NOC_Totals: stack all levels, then add BC and grand totals ----
q_4_noc_totals_by_year <- rbind(
  q_4_noc_4d_totals_by_year,
  q_4_noc_3d_totals_by_year,
  q_4_noc_2d_totals_by_year,
  q_4_noc_1d_totals_by_year,
  q_4_noc_5d_totals_by_year
)

# BC aggregate: collapse the "59.." economic regions into one province row.
q_4_noc_totals_by_year_bc <- q_4_noc_totals_by_year %>%
  filter(str_starts(CURRENT_REGION_PSSM_CODE_ROLLUP, "59")) |>
  mutate(
    CURRENT_REGION_PSSM_CODE_ROLLUP = 5900,
    CURRENT_REGION_PSSM_NAME_ROLLUP = "British Columbia"
  ) |>
  group_by(
    AGE_GROUP_ROLLUP_LABEL,
    NOC_LEVEL,
    NOC,
    ENGLISH_NAME,
    CURRENT_REGION_PSSM_CODE_ROLLUP,
    CURRENT_REGION_PSSM_NAME_ROLLUP
  ) |>
  summarise(across(starts_with("20"), sum), .groups = "drop")

# Province-wide grand total across all regions.
q_4_noc_totals_by_year_total <- q_4_noc_totals_by_year %>%
  group_by(
    AGE_GROUP_ROLLUP_LABEL,
    NOC_LEVEL,
    NOC,
    ENGLISH_NAME
  ) |>
  summarise(across(starts_with("20"), sum), .groups = "drop") |>
  mutate(
    CURRENT_REGION_PSSM_CODE_ROLLUP = 0,
    CURRENT_REGION_PSSM_NAME_ROLLUP = "Total"
  )


# ============================================================================
# Q_5 SERIES - assemble region + BC (+ grand total) views ----
# ============================================================================

# ---- Q_5 Series ----
# dbExecute(decimal_con, Q_5_NOC_Totals_by_Year_and_BC)
# dbExecute(decimal_con, Q_5_NOC_Totals_by_Year_and_BC_and_Total)

q_5_noc_totals_by_year_and_bc <- bind_rows(
  q_4_noc_totals_by_year,
  q_4_noc_totals_by_year_bc
)

q_5_noc_totals_by_year_and_bc_and_total <- bind_rows(
  q_4_noc_totals_by_year,
  q_4_noc_totals_by_year_bc,
  q_4_noc_totals_by_year_total
)

# ============================================================================
# Q_6 SERIES - stash this run's result under a run-specific name ----
#   Each of the three model runs lands in its own object so 08 can combine them.
# ============================================================================
if (regular_run == TRUE) {
  tmp_tbl_model <- q_5_noc_totals_by_year_and_bc_and_total
  # write tmp_tbl_model to decimal
}

if (qi_run == TRUE) {
  tmp_tbl_qi <- q_5_noc_totals_by_year_and_bc_and_total
}

if (ptib_run == TRUE) {
  tmp_tbl_model_inc_private_inst <- q_5_noc_totals_by_year_and_bc_and_total
}

if (model == "program_projection") {
  tmp_tbl_model_program_projection <- q_5_noc_totals_by_year_and_bc_and_total
}


tables_to_keep <- c(
  "tmp_tbl_model",
  "tmp_tbl_qi",
  "tmp_tbl_model_inc_private_inst",
  "tmp_tbl_model_program_projection"
)

write_table_to_db <- function(table_name, schema, con) {
  db_name <- paste0(table_name, "_r")
  dbWriteTable(
    con,
    SQL(glue::glue('"{schema}"."{db_name}"')),
    .GlobalEnv[[table_name]],
    overwrite = TRUE
  )
}

walk(tables_to_keep, write_table_to_db, schema = my_schema, con = con)


if (regular_run == TRUE | qi_run == TRUE) {
  dbExecute(decimal_con, "DROP TABLE Q_5_NOC_Totals_by_Year_and_BC")
  dbExecute(decimal_con, "DROP TABLE Q_5_NOC_Totals_by_Year_and_BC_and_Total")
}

# see 08 script to replace below
# # ---- model with QI ----
# if (regular_run != T){
#   dbGetQuery(decimal_con, Q_7_QI) %>%
#     write_csv(glue::glue("{lan}/reports-final/drafts/error_rate_by_noc_static_incl_ptib.csv"))
#
#   dbGetQuery(decimal_con, Q_8_Labour_Supply_Total_by_Year) %>%
#     write_csv(glue::glue("{lan}/reports-final/drafts/labour_supply_by_year_static_incl_ptib.csv"))
#
#   # gives final model output with quality indicator and coverage indicator counts-not too useful for anything, better queries below
#   dbExecute(decimal_con, qry_10a_Model)
#
#   dbGetQuery(decimal_con, "SELECT * FROM qry_10a_Model") %>%
#     write_csv(glue::glue("{lan}/reports-final/drafts/full_model_static_incl_ptib.csv"))
# }

#
# # ---- public release ----
# dbExecute(decimal_con, qry_10a_Model_Public_Release) # gives rounded 5-digit NOC result
# dbExecute(decimal_con, qry_10a_Model_Public_Release_Suppressed) # shows the 5-digit NOCs that have been suppressed
# dbExecute(decimal_con, qry_10a_Model_Public_Release_Suppressed_Total) # sum of the 5-digit NOCs that have been suppressed and can be included in final public release
# dbExecute(decimal_con, qry_10a_Model_Public_Release_Union) # final output with suppressed counts for public release
#
# dbGetQuery(decimal_con, "SELECT * FROM qry_10a_Model_Public_Release_Union") %>%
#   write_csv(glue::glue("{lan}/reports-final/drafts/public_release_static_incl_ptib.csv"))
#
#
# # ---- internal release ----
#  dbExecute(decimal_con, qry_10a_Model_QI_PPCI) # gives rounded 5-digit NOC output with quality indicator and coverage indicator as calculated percentages-internal use only
#  dbExecute(decimal_con, qry_10a_Model_QI_PPCI_No_Supp) # for internal use release only-no suppression applied; LMIO needs it to work on the Labour Market Outlook
# # dbExecute(decimal_con, qry_10a_Model_QI_PPCI_Suppressed) # shows the 5-digit NOCs that have been suppressed
# # dbExecute(decimal_con, qry_10a_Model_QI_PPCI_Suppressed_Total) # sum of the 5-digit NOCs that have been suppressed
# dbGetQuery(decimal_con, "SELECT * FROM qry_10a_Model_QI_PPCI_No_Supp") %>%
#    write_csv(glue::glue("{lan}/reports-final/drafts/internal_only_static_no_ptib.csv"))
#
# dbExecute(decimal_con, qry_10b_Quality_Indicator)
# dbExecute(decimal_con, qry_10c_Coverage_Indicator)
# # dbExecute(decimal_con, qry_10d_tmp_No_Near_Completers)
#
#
# dbGetQuery(decimal_con, qry_LCIP4_CRED)
# #dbGetQuery(decimal_con, qry_LCIP4_CRED_Filtered_NOC)
# dbGetQuery(decimal_con, qry_LCIP4_CRED_NOC)
# # dbExecute(decimal_con, qry100_Grad_Skill_Level)
#
# # ---- public release ----
# dbGetQuery(decimal_con, qry99_Presentations_Graduates_Appendix) %>%
#   mutate(across(where(is.numeric), round)) %>%
#   write_csv(glue::glue("{lan}/reports-final/drafts/graduate_projections_noc_2021_static_incl_ptib.csv"))
#
#
# dbGetQuery(decimal_con, qry99_Presentations_Graduates_Appendix_by_Age_Group_Totals)
# # dbExecute(decimal_con, qry99_Presentations_Graduates_Appendix_Unrounded)
# # dbExecute(decimal_con, qry99_Presentations_Graduates_Including_those_not_projected)
# # dbExecute(decimal_con, qry99_Presentations_Labour_Force)
# # dbExecute(decimal_con, qry99_Presentations_Labour_Force_BC)
# # dbExecute(decimal_con, qry99_Presentations_Labour_Force_Overall)
# # dbExecute(decimal_con, qry99_Presentations_Occs)
# # dbExecute(decimal_con, qry99_Presentations_PPSCI_Graduates)
# # dbExecute(decimal_con, qry9999_NOC_4031_4032)

# ---- Clean Up ----
