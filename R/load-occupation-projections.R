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

# ******************************************************************************
# Load datasets required to run the OCCUPATION projections step (step 07).
# (Header previously said "program projections" - stale; this feeds 07, not 06.)
# ******************************************************************************

# ============================================================================
# WHAT THIS SCRIPT DOES
#   Data-loading partner for 07-occupation-projections.R. It pulls every input
#   that step 07 needs into the global environment, then writes only the lookup
#   CSVs back to the analyst's IDIR schema as "<name>_r". Inputs come from two
#   places:
#     1. SQL Server "_r" tables built by earlier steps (02b, 04, 06).
#     2. Lookup CSVs on the LAN (development/csv/gh-source/lookups/07/...).
#
# WHAT IT LOADS AND WHY 07 NEEDS IT
#   The two distribution families drive 07's probability chain:
#     OCCSN = GRADUATES x P(CIP|cred,age) x P(supply|CIP) x P(NOC|CIP,region)
#   - labour_supply_distribution*   -> P(in labour supply | CIP)   (Q_2 series)
#   - occupation_distributions*     -> P(NOC | CIP, region)        (Q_3 series)
#   Each family is loaded in FOUR variants because 07 falls back through looser
#   proxies when an exact program match is missing:
#     <base>          exact LCIP4_CRED match
#     <base>_No_TT    same key with TTRAIN stripped out  (trades fallback)
#     <base>_LCP2     2-digit CIP (broader program group)
#     <base>_LCP2_No_TT   2-digit CIP with TTRAIN stripped
#   Plus the program mix (cohort_program_distributions_*, from 06), the graduate
#   forecasts (graduate_projections, from 04), and CIP code lookups.
#
# THREE TRANSFORMS REPEATED ON ALMOST EVERY TABLE (explained in full below at the
# first labour-supply block, then referenced briefly afterwards):
#   1. janitor::clean_names(case = "all_caps")
#   2. str_replace(" OR ", " or ")  on the credential key columns
#   3. filter(!SURVEY == "PTIB")    (distribution tables only)
#
# RUN CONTEXT
#   The connection is intentionally LEFT OPEN for step 07 (the paired analysis
#   script), so this file does not dbDisconnect(). Only the lookup CSVs are
#   persisted; the SQL-read tables already exist as "_r" and stay in memory.
# ============================================================================

library(tidyverse)
# library(RODBC)   # REMOVED: unused. This script uses DBI/odbc, not RODBC.
library(config)
library(DBI)

# ---- Configure LAN and file paths ----
# Environment-specific values come from config.yml (never hardcoded). `lan` is
# the network path to the lookup CSVs; `my_schema` is this analyst's IDIR schema.
lan <- config::get("lan")
my_schema <- config::get("myschema")

# ---- Connection to decimal ----
# Windows Integrated Authentication (Trusted_Connection = "True") per convention.
db_config <- config::get("decimal")
con <- dbConnect(
  odbc::odbc(),
  Driver = db_config$driver,
  Server = db_config$server,
  Database = db_config$database,
  Trusted_Connection = "True"
)

# -------------------- READ DATA FROM SQL SERVER ------------------------
# read the following tables from {my_schema}, noting that we
# might get FALSE that Cohort_Program_Distributions exists during first run-through.
# Use the Static Cohort_Program_Distributions table?

# ---- Labour_Supply_Distribution (exact-match variant) ----
# This first block carries the FULL explanation of the three repeated transforms;
# every distribution table below applies the same three steps.
labour_supply_distribution <-
  dbReadTable(
    con,
    SQL(glue::glue('"{my_schema}"."Labour_Supply_Distribution_r"'))
  ) %>%
  # 1. Force column names to UPPER_SNAKE_CASE so they match the heritage keys
  #    that 07 joins on (e.g. LCIP4_CRED, AGE_GROUP_ROLLUP, PSSM_CRED).
  janitor::clean_names(case = "all_caps") |>
  # 2. Normalise credential casing. Some heritage rows store "<X> OR <Y>" with an
  #    upper-case "OR", but 07's join keys use lower-case "or". Without this fix
  #    the proxy joins in 07 would SILENTLY miss those credentials. any_of() means
  #    the rename is applied only to whichever of these key columns are present.
  mutate(across(
    any_of(c("PSSM_CRED", "PSSM_CREDENTIAL", "LCP2_CRED", "LCIP2_CRED")),
    ~ str_replace(., " OR ", " or ")
  )) |>
  # 3. Drop survey-sourced PRIVATE rows. 07 REBUILDS the PTIB (private) rows as
  #    proxies - it copies the public distributions and re-tags them "P - ". So
  #    the original PTIB rows must be removed here first to avoid double-counting.
  filter(!SURVEY == "PTIB")

# ---- Labour_Supply_Distribution_LCP2 (2-digit CIP fallback) ----
# Same three transforms (clean_names / " OR "->" or " / drop PTIB) as above.
labour_supply_distribution_lcp2 <-
  dbReadTable(
    con,
    SQL(glue::glue('"{my_schema}"."Labour_Supply_Distribution_LCP2_r"'))
  ) %>%
  janitor::clean_names(case = "all_caps") |>
  mutate(across(
    any_of(c("PSSM_CRED", "PSSM_CREDENTIAL", "LCP2_CRED", "LCIP2_CRED")),
    ~ str_replace(., " OR ", " or ")
  )) |>
  filter(!SURVEY == "PTIB")

# ---- Labour_Supply_Distribution_No_TT (TTRAIN-stripped fallback) ----
# First fallback in 07 for trades programs whose TTRAIN value has no survey data.
labour_supply_distribution_no_tt <-
  dbReadTable(
    con,
    SQL(glue::glue('"{my_schema}"."Labour_Supply_Distribution_No_TT_r"'))
  ) %>%
  janitor::clean_names(case = "all_caps") |>
  mutate(across(
    any_of(c("PSSM_CRED", "PSSM_CREDENTIAL", "LCP2_CRED", "LCIP2_CRED")),
    ~ str_replace(., " OR ", " or ")
  )) |>
  filter(!SURVEY == "PTIB")

# ---- Labour_Supply_Distribution_LCP2_No_TT (broadest fallback) ----
# 2-digit CIP with TTRAIN stripped - the loosest labour-supply proxy in 07.
labour_supply_distribution_lcp2_no_tt <-
  dbReadTable(
    con,
    SQL(glue::glue('"{my_schema}"."Labour_Supply_Distribution_LCP2_No_TT_r"'))
  ) %>%
  janitor::clean_names(case = "all_caps") |>
  mutate(across(
    any_of(c("PSSM_CRED", "PSSM_CREDENTIAL", "LCP2_CRED", "LCIP2_CRED")),
    ~ str_replace(., " OR ", " or ")
  )) |>
  filter(!SURVEY == "PTIB")

# ---- Occupation_Distributions (exact-match variant) ----
# The four occupation variants mirror the four labour-supply variants above and
# feed 07's Q_3 proxy waterfall. Same three transforms throughout.
occupation_distributions <-
  dbReadTable(
    con,
    SQL(glue::glue('"{my_schema}"."Occupation_Distributions_r"'))
  ) %>%
  janitor::clean_names(case = "all_caps") |>
  mutate(across(
    any_of(c("PSSM_CRED", "PSSM_CREDENTIAL", "LCP2_CRED", "LCIP2_CRED")),
    ~ str_replace(., " OR ", " or ")
  )) |>
  filter(!SURVEY == "PTIB")


# ---- Occupation_Distributions_No_TT (TTRAIN-stripped fallback) ----
occupation_distributions_no_tt <-
  dbReadTable(
    con,
    SQL(glue::glue('"{my_schema}"."Occupation_Distributions_No_TT_r"'))
  ) %>%
  janitor::clean_names(case = "all_caps") |>
  mutate(across(
    any_of(c("PSSM_CRED", "PSSM_CREDENTIAL", "LCP2_CRED", "LCIP2_CRED")),
    ~ str_replace(., " OR ", " or ")
  )) |>
  filter(!SURVEY == "PTIB")

# ---- Occupation_Distributions_LCP2 (2-digit CIP fallback) ----
occupation_distributions_lcp2 <-
  dbReadTable(
    con,
    SQL(glue::glue('"{my_schema}"."Occupation_Distributions_LCP2_r"'))
  ) %>%
  janitor::clean_names(case = "all_caps") |>
  mutate(across(
    any_of(c("PSSM_CRED", "PSSM_CREDENTIAL", "LCP2_CRED", "LCIP2_CRED")),
    ~ str_replace(., " OR ", " or ")
  )) |>
  filter(!SURVEY == "PTIB")


# ---- Occupation_Distributions_LCP2_No_TT (broadest fallback) ----
occupation_distributions_lcp2_no_tt <-
  dbReadTable(
    con,
    SQL(glue::glue('"{my_schema}"."Occupation_Distributions_LCP2_No_TT_r"'))
  ) %>%
  janitor::clean_names(case = "all_caps") |>
  mutate(across(
    any_of(c("PSSM_CRED", "PSSM_CREDENTIAL", "LCP2_CRED", "LCIP2_CRED")),
    ~ str_replace(., " OR ", " or ")
  )) |>
  filter(!SURVEY == "PTIB")

# ---- Cohort_Program_Distributions_Projected (P(CIP|cred,age), from 06) ----
# NOTE: the program-mix and graduate tables do NOT drop PTIB. 07 reads their PTIB
# rows directly (the private program mix from 06 is legitimate input), unlike the
# distribution tables above where 07 rebuilds PTIB as proxies.
cohort_program_distributions_projected <-
  dbReadTable(
    con,
    SQL(glue::glue('"{my_schema}"."Cohort_Program_Distributions_Projected_r"'))
  ) %>%
  janitor::clean_names(case = "all_caps") |>
  mutate(across(
    any_of(c("PSSM_CRED", "PSSM_CREDENTIAL", "LCP2_CRED", "LCIP2_CRED")),
    ~ str_replace(., " OR ", " or ")
  ))

# ---- Cohort_Program_Distributions_Static (2023/24 snapshot mix, from 06) ----
# 07's `model` toggle picks projected vs static; both are loaded so either works.
cohort_program_distributions_static <-
  dbReadTable(
    con,
    SQL(glue::glue('"{my_schema}"."Cohort_Program_Distributions_Static_r"'))
  ) %>%
  janitor::clean_names(case = "all_caps") |>
  mutate(across(
    any_of(c("PSSM_CRED", "PSSM_CREDENTIAL", "LCP2_CRED", "LCIP2_CRED")),
    ~ str_replace(., " OR ", " or ")
  ))

# ---- Graduate_Projections (GRADUATES by cred x age x year, from 04) ----
# The starting volume in 07's chain; PTIB rows retained (see note above).
graduate_projections <-
  dbReadTable(
    con,
    SQL(glue::glue('"{my_schema}"."Graduate_Projections_r"'))
  ) %>%
  janitor::clean_names(case = "all_caps") |>
  mutate(across(
    any_of(c("PSSM_CRED", "PSSM_CREDENTIAL", "LCP2_CRED", "LCIP2_CRED")),
    ~ str_replace(., " OR ", " or ")
  ))

# ---- CIP code lookups (Statistics Canada CIP 2016) ----
# 4-digit and 6-digit CIP tables. 07 derives its LCP2<->LCP4 map from the 6-digit
# version for the 2-digit proxy steps. No " OR "->" or " needed (no credential
# columns here).
# ---- Student Outcomes Lookups (loaded for ALL runs, incl. QI) ----
# Statistics Canada CIP 2016 code lookups (4- and 6-digit). latin1 encoding is
# required because the source files contain accented characters. These are kept
# in memory for downstream steps (not written to the DB below).
infoware_l_cip_4digits_cip2016 <- readr::read_csv(
  glue::glue(
    "{lan}/development/csv/infoware/INFOWARE_L_CIP_4DIGITS_CIP2016.csv"
  ),
  col_types = cols(
    .default = col_guess()
  ),
  locale = locale(
    encoding = "latin1"
  )
) %>%
  janitor::clean_names(case = "all_caps")

infoware_l_cip_6digits_cip2016 <- readr::read_csv(
  glue::glue(
    "{lan}/development/csv/infoware/INFOWARE_L_CIP_6DIGITS_CIP2016.csv"
  ),
  col_types = cols(.default = col_guess()),
  locale = locale(
    encoding = "latin1"
  )
) %>%
  janitor::clean_names(case = "all_caps")

# infoware_l_cip_4digits_cip2016 <-
#   dbReadTable(
#     con,
#     SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_4DIGITS_CIP2016"'))
#   ) %>%
#   janitor::clean_names(case = "all_caps")

# infoware_l_cip_6digits_cip2016 <-
#   dbReadTable(
#     con,
#     SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_6DIGITS_CIP2016"'))
#   ) %>%
#   janitor::clean_names(case = "all_caps")

# -------------------- LOAD LOOKUPS ------------------------
# Small reference CSVs from the LAN. These are the tables PERSISTED back to the DB
# at the end (the SQL-read tables above are not re-written). The three exclusion
# lists force every column to character so later anti_join() key comparisons in 07
# don't fail on type mismatches.

# Programs to exclude from projection by 4-digit CIP (Student Outcomes can't /
# shouldn't project these).
t_exclude_from_projections_lcp4_cd <-
  readr::read_csv(
    glue::glue(
      "{lan}/development/csv/gh-source/lookups/07/T_Exclude_from_Projections_LCP4_CD.csv"
    ),
    col_types = cols(.default = col_guess())
  ) %>%
  janitor::clean_names(case = "all_caps") %>%
  mutate(across(everything(), ~ as.character(.)))

# Exclusions by full CIP-credential key. Currently EMPTY, but loaded so 07's
# anti_join() has a table to reference (an empty anti_join is a safe no-op).
t_exclude_from_projections_lcip4_cred <-
  readr::read_csv(
    glue::glue(
      "{lan}/development/csv/gh-source/lookups/07/T_Exclude_from_Projections_LCIP4_CRED.csv"
    ),
    col_types = cols(.default = col_guess())
  ) %>%
  janitor::clean_names(case = "all_caps") %>%
  mutate(across(everything(), ~ as.character(.)))

# Exclusions by credential. Also EMPTY; loaded for the same anti_join no-op reason.
t_exclude_from_projections_pssm_credential <-
  readr::read_csv(
    glue::glue(
      "{lan}/development/csv/gh-source/lookups/07/T_Exclude_from_Projections_PSSM_Credential.csv"
    ),
    col_types = cols(.default = col_guess())
  ) %>%
  janitor::clean_names(case = "all_caps") %>%
  mutate(across(everything(), ~ as.character(.)))

# CIP-credential combinations barred from using the 2-digit labour-supply proxy
# (e.g. CIP-51 medical programs whose occupation link is too specific to borrow
# from the broader group). Consumed in 07's Q_2 LCP2 step.
t_exclude_from_labour_supply_unknown_lcp2_proxy <-
  readr::read_csv(
    glue::glue(
      "{lan}/development/csv/gh-source/lookups/07/T_Exclude_from_Labour_Supply_Unknown_LCP2_Proxy.csv"
    ),
    col_types = cols(.default = col_guess())
  ) %>%
  janitor::clean_names(case = "all_caps") %>%
  mutate(across(everything(), ~ as.character(.)))

# 4-digit -> 2-digit CIP map. (07 also derives this from the 6-digit CIP table;
# this CSV is the lookup-sourced copy that gets persisted as "_r".)
t_lcp2_lcp4 <-
  readr::read_csv(
    glue::glue("{lan}/development/csv/gh-source/lookups/07/T_LCP2_LCP4.csv"),
    col_types = cols(.default = col_guess())
  ) %>%
  janitor::clean_names(case = "all_caps") %>%
  mutate(across(everything(), ~ as.character(.)))

# Fine age band -> rollup band. Used in 07's Q_1c step to roll the 9 fine bands
# up to the 5 projection bands the distributions are keyed on.
tbl_age_groups <-
  readr::read_csv(
    glue::glue("{lan}/development/csv/gh-source/lookups/07/tbl_Age_Groups.csv"),
    col_types = cols(.default = col_guess())
  ) %>%
  janitor::clean_names(case = "all_caps")

# Rollup band code -> label (e.g. "17 to 29").
tbl_age_groups_rollup <-
  readr::read_csv(
    glue::glue(
      "{lan}/development/csv/gh-source/lookups/07/tbl_Age_Groups_Rollup.csv"
    ),
    col_types = cols(.default = col_guess())
  ) %>%
  janitor::clean_names(case = "all_caps")

# Region rollup codes (all regions) - attached to NOC totals in 07's Q_4 step.
t_current_region_pssm_rollup_codes <-
  readr::read_csv(
    glue::glue(
      "{lan}/development/csv/gh-source/lookups/07/T_Current_Region_PSSM_Rollup_Codes.csv"
    ),
    col_types = cols(.default = col_guess())
  ) %>%
  janitor::clean_names(case = "all_caps")

# Region rollup codes (BC only) - used when collapsing regions to the BC total.
t_current_region_pssm_rollup_codes_bc <-
  readr::read_csv(
    glue::glue(
      "{lan}/development/csv/gh-source/lookups/07/T_Current_Region_PSSM_Rollup_Codes_BC.csv"
    ),
    col_types = cols(.default = col_guess())
  ) %>%
  janitor::clean_names(case = "all_caps")

# Credential recode lookup (heritage credential code mappings).
t_pssm_cred_recode <-
  readr::read_csv(
    glue::glue(
      "{lan}/development/csv/gh-source/lookups/07/T_PSSM_CRED_RECODE.csv"
    ),
    col_types = cols(.default = col_guess())
  ) %>%
  janitor::clean_names(case = "all_caps")

# Credential code -> human-readable name (used by the report appendix in 08).
t_pssm_credential_grouping_appendix <-
  readr::read_csv(
    glue::glue(
      "{lan}/development/csv/gh-source/lookups/07/T_PSSM_Credential_Grouping_Appendix.csv"
    ),
    col_types = cols(.default = col_guess())
  ) %>%
  janitor::clean_names(case = "all_caps")

# DISABLED upstream: NOC skill-type lookup is no longer used by 07 (kept for
# reference / possible future use). Left commented intentionally.
# T_NOC_Skill_Type <-
#   readr::read_csv(glue::glue("{lan}/development/csv/gh-source/lookups/07/T_NOC_Skill_Type.csv"),  col_types = cols(.default = col_guess())) %>%
#   janitor::clean_names(case = "all_caps")

# NOC broad->unit hierarchy (1- to 5-digit). 07 joins this in Q_4 to roll OCCSN
# up the NOC hierarchy. NOTE: read from the step-02 folder, not step-07.
t_noc_broad_categories <-
  readr::read_csv(
    glue::glue(
      "{lan}/development/csv/gh-source/lookups/02/T_NOC_Broad_Categories_Updated.csv"
    ),
    col_types = cols(.default = col_guess())
  ) %>%
  janitor::clean_names(case = "all_caps")

## ------------------------------------ Clean Up --------------------------------------------------
# Workflow:
#  - Persist ONLY the lookup CSVs loaded above, each written to "<schema>"."<name>_r".
#    The SQL-read distribution/program/grad tables are NOT re-written (they already
#    exist as "_r") - they stay in memory for step 07.
#  - The connection is intentionally LEFT OPEN for step 07 (paired script); this
#    file does not dbDisconnect().
#  - NOTE: the comment lines below about "Close DB connections" and "Remove all
#    objects" are stale boilerplate - neither happens here by design.
## ------------------------------------------------------------------------------------------------

tables_to_keep <- c(
  # keep all tables that were read into this script via read_csv, but not the
  # tables read from SQL Server
  "t_exclude_from_projections_lcp4_cd",
  "t_exclude_from_projections_lcip4_cred",
  "t_exclude_from_projections_pssm_credential",
  "t_exclude_from_labour_supply_unknown_lcp2_proxy",
  "t_lcp2_lcp4",
  "tbl_age_groups",
  "tbl_age_groups_rollup",
  "t_current_region_pssm_rollup_codes",
  "t_current_region_pssm_rollup_codes_bc",
  "t_pssm_cred_recode",
  "t_pssm_credential_grouping_appendix",
  "t_noc_broad_categories"
)

# Write one named global object to "<schema>"."<table_name>_r", overwriting any
# existing copy. Called over tables_to_keep with walk().
write_table_to_db <- function(table_name, schema, con) {
  db_name <- paste0(table_name, "_r")
  dbWriteTable(
    con,
    SQL(glue::glue('"{schema}"."{db_name}"')),
    base::get(table_name, envir = .GlobalEnv), # fetch the object by name
    overwrite = TRUE
  )
}

walk(tables_to_keep, write_table_to_db, schema = my_schema, con = con)
gc()

# NOTE: the program-mix and graduate tables do NOT drop PTIB. 07 reads their PTIB
# rows directly (the private program mix from 06 is legitimate input), unlike the
# distribution tables above where 07 rebuilds PTIB as proxies.
