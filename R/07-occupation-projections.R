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
#

# Fixes To do: some of the CIP2 variable names are missing an "I" in the Labour_Supply_Distribution datasets.

# ------------------------  libraries and global variables ------------------------
library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(glue)
library(assertthat)

model <- "static" # toogle between static and projected models

# source("./sql/07-occupation-projections/occupation-projections.R")

# ------------------------ Check for required data tables ------------------------

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
  "t_noc_broad_categories"
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

# toggle static or projected.
if (model == "static") {
  cohort_program_distributions <- cohort_program_distributions_static
} else {
  cohort_program_distributions <- cohort_program_distributions_projected
}

# --------------------   implement checks --------------------------
# in the interest of time, we aren't doing this.
# dbGetQuery(decimal_con, Count_Cohort_Program_Distributions)
# dbGetQuery(decimal_con, Count_Labour_Supply_Distribution1)
# dbGetQuery(decimal_con, Count_Labour_Supply_Distribution2)
# dbGetQuery(decimal_con, Count_Occupation_Distributions1) # We want this to contain all of our PSSM Credentials
# dbGetQuery(decimal_con, Count_Occupation_Distributions2)
# dbGetQuery(decimal_con, Occupation_Unknown)

# creates mapping for LCIP4 to LCIP2
t_lcp2_lcp4 <- infoware_l_cip_6digits_cip2016 |>
  distinct(LCIP_LCP2_CD, LCIP_LCP4_CD)

# ---------------------------- add PTIB to labour supply and occupation distributions ------------------------
# Should this be moved to 2b-2 and 2b-3?
# use existing labour supply and occ_dists as a proxy for private training institutions.
# note to self - be mindful that count and total doesn't represent true counts/total for PTIB, but the
# NLS and PERCENT columns can, as the represent a ratio.

# --- delete existing PTIB Surveys
labour_supply_distribution_no_tt <- labour_supply_distribution_no_tt |>
  filter(!SURVEY == "PTIB")
labour_supply_distribution_lcp2_no_tt <- labour_supply_distribution_lcp2_no_tt |>
  filter(!SURVEY == "PTIB")
occupation_distributions_no_tt <- occupation_distributions_no_tt |>
  filter(!SURVEY == "PTIB")
occupation_distributions_no_tt <- occupation_distributions_no_tt |>
  filter(!SURVEY == "PTIB")

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
      LCIP4_CRED = paste0("P - ", LCP4_CD, " - ", PSSM_CREDENTIAL)
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
      LCIP2_CRED = paste0("P - ", LCP2_CD, " - ", PSSM_CREDENTIAL)
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

# ---- Q_1 Series ----
dbExecute(decimal_con, Q_1_Grad_Projections_by_Age_by_Program)
dbExecute(decimal_con, Q_1_Grad_Projections_by_Age_by_Program_Static)
dbGetQuery(decimal_con, Q_1b_Checking_Grads_by_Year_Excludes_CIPs)
dbExecute(decimal_con, Q_1c_Grad_Projections_by_Program)
dbExecute(decimal_con, Q_1c_Grad_Projections_by_Program_LCP2)

# ---- Q_2 Series ----
dbExecute(decimal_con, Q_2_Labour_Supply_by_LCIP4_CRED)
dbExecute(decimal_con, Q_2a_Labour_Supply_Unknown)
dbExecute(decimal_con, Q_2a2_Labour_Supply_Unknown_No_TT_Proxy)
dbExecute(decimal_con, Q_2a3_Labour_Supply_by_LCIP4_CRED_No_TT_Proxy_Union)
dbExecute(decimal_con, Q_2a4_Labour_Supply)
dbExecute(decimal_con, Q_2b_Labour_Supply_Unknown)
dbExecute(decimal_con, Q_2b2_Labour_Supply_Unknown_Private_Cred_Proxy)
dbExecute(
  decimal_con,
  Q_2b3_Labour_Supply_by_LCIP4_CRED_Private_Cred_Proxy_Union
)
dbExecute(decimal_con, Q_2b4_Labour_Supply_Unknown)
dbExecute(decimal_con, Q_2c_Labour_Supply_Unknown_LCP2_Proxy)
dbExecute(decimal_con, Q_2c2_Labour_Supply_Unknown_LCP2_Proxy_Union)
dbExecute(decimal_con, Q_2c3_Labour_Supply_Unknown)
dbExecute(decimal_con, Q_2c4_Labour_Supply_Unknown_LCP2_Proxy_No_TT)
dbExecute(decimal_con, Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union)
dbExecute(decimal_con, Q_2d2_Labour_Supply)
dbExecute(decimal_con, Q_2d2_Labour_Supply_Unknown)
dbExecute(decimal_con, Q_2d3_Labour_Supply_Unknown_LCP2_Private_Cred_Proxy)
dbExecute(
  decimal_con,
  Q_2d4_Labour_Supply_by_LCIP4_CRED_LCP2_LCP2_Private_Union
)
dbExecute(decimal_con, Q_2f_Labour_Supply)
dbExecute(decimal_con, Q_2f2_Labour_Supply_Unknown) # numbers are low

#dbExecute(decimal_con, "DROP TABLE Q_1c_Grad_Projections_by_Program")
dbExecute(decimal_con, "DROP TABLE Q_2_Labour_Supply_by_LCIP4_CRED")
dbExecute(decimal_con, "DROP TABLE Q_2a_Labour_Supply_Unknown")
dbExecute(decimal_con, "DROP TABLE Q_2a2_Labour_Supply_Unknown_No_TT_Proxy")
dbExecute(
  decimal_con,
  "DROP TABLE Q_2a3_Labour_Supply_by_LCIP4_CRED_No_TT_Proxy_Union"
)
dbExecute(decimal_con, "DROP TABLE Q_2b_Labour_Supply_Unknown")
dbExecute(
  decimal_con,
  "DROP TABLE Q_2b2_Labour_Supply_Unknown_Private_Cred_Proxy"
)
dbExecute(
  decimal_con,
  "DROP TABLE Q_2b3_Labour_Supply_by_LCIP4_CRED_Private_Cred_Proxy_Union"
)
dbExecute(decimal_con, "DROP TABLE Q_2b4_Labour_Supply_Unknown")
dbExecute(decimal_con, "DROP TABLE Q_2c_Labour_Supply_Unknown_LCP2_Proxy")
dbExecute(
  decimal_con,
  "DROP TABLE Q_2c2_Labour_Supply_Unknown_LCP2_Proxy_Union"
)
dbExecute(decimal_con, "DROP TABLE Q_2c3_Labour_Supply_Unknown")
dbExecute(
  decimal_con,
  "DROP TABLE Q_2c4_Labour_Supply_Unknown_LCP2_Proxy_No_TT"
)
dbExecute(decimal_con, "DROP TABLE Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union")
dbExecute(decimal_con, "DROP TABLE Q_2d2_Labour_Supply_Unknown")
dbExecute(
  decimal_con,
  "DROP TABLE Q_2d3_Labour_Supply_Unknown_LCP2_Private_Cred_Proxy"
)
dbExecute(
  decimal_con,
  "DROP TABLE Q_2d4_Labour_Supply_by_LCIP4_CRED_LCP2_LCP2_Private_Union"
)
dbExecute(decimal_con, "DROP TABLE Q_2f_Labour_Supply")
#dbExecute(decimal_con, "DROP TABLE tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union_tmp")
#dbExecute(decimal_con, "DROP TABLE tmp_tbl_Q_2a4_Labour_Supply_by_LCIP4_CRED_No_TT_Union_tmp")

# ---- Q_3 Series ----
dbExecute(decimal_con, Q_3_Occupations_by_LCIP4_CRED)
dbExecute(decimal_con, Q_3b_Occupations_Unknown) # numbers too high
dbExecute(decimal_con, Q_3b11_Ocupations_Unknown_No_TT_Proxy) # numbers too high
dbExecute(decimal_con, q_3b12_Occupations_by_LCIP4_CRED_No_TT_Proxy_Union)
dbExecute(decimal_con, Q_3b13_Occupations)
dbExecute(decimal_con, Q_3b14_Occupations_Unknown)
dbExecute(decimal_con, Q_3b2_Occupations_Unknown_Private_Cred_Proxy)
dbExecute(decimal_con, Q_3b3_Occupations_by_LCIP4_CRED_Private_Cred_Proxy_Union)
dbExecute(decimal_con, Q_3b4_Occupations_Unknown)

dbExecute(decimal_con, Q_3c_Occupations_Unknown_LCP2_Proxy)
dbExecute(decimal_con, Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union)
dbExecute(decimal_con, Q_3d2_Occupations)
dbExecute(decimal_con, Q_3d2_Occupations_Unknown)
dbExecute(decimal_con, Q_3d21_Occupations_Unknown_LCP2_Proxy_No_TT)
dbExecute(decimal_con, Q_3d22_Occupations_by_LCIP4_CRED_LCP2_No_T_Proxy_Union)
dbExecute(decimal_con, Q_3d24_Occupations_Unknown)
dbExecute(decimal_con, Q_3d3_Occupations_Unknown_LCP2_Private_Cred_Proxy)
dbExecute(decimal_con, Q_3d4_Occupations_by_LCIP4_CRED_LCP2_LCP2_Private_Union)
dbExecute(decimal_con, Q_3e_Occupations_Unknown)
dbExecute(decimal_con, Q_3e2_Occupations_Unknown)
dbExecute(decimal_con, Q_3e3_Occupations_by_LCIP4_CRED_LCP2_Union)
dbExecute(decimal_con, Q_3f_Occupations)

dbExecute(decimal_con, "DROP TABLE Q_3_Occupations_by_LCIP4_CRED")
dbExecute(decimal_con, "DROP TABLE Q_3b_Occupations_Unknown")
dbExecute(decimal_con, "DROP TABLE Q_3b11_Ocupations_Unknown_No_TT_Proxy")
dbExecute(
  decimal_con,
  "DROP TABLE q_3b12_Occupations_by_LCIP4_CRED_No_TT_Proxy_Union"
)
dbExecute(decimal_con, "DROP TABLE Q_3b14_Occupations_Unknown")
dbExecute(
  decimal_con,
  "DROP TABLE Q_3b2_Occupations_Unknown_Private_Cred_Proxy"
)
dbExecute(
  decimal_con,
  "DROP TABLE Q_3b3_Occupations_by_LCIP4_CRED_Private_Cred_Proxy_Union"
)
dbExecute(decimal_con, "DROP TABLE Q_3b4_Occupations_Unknown")
dbExecute(decimal_con, "DROP TABLE Q_3c_Occupations_Unknown_LCP2_Proxy")
dbExecute(decimal_con, "DROP TABLE Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union")
dbExecute(decimal_con, "DROP TABLE Q_3d2_Occupations_Unknown")
dbExecute(decimal_con, "DROP TABLE Q_3d21_Occupations_Unknown_LCP2_Proxy_No_TT")
dbExecute(
  decimal_con,
  "DROP TABLE Q_3d22_Occupations_by_LCIP4_CRED_LCP2_No_T_Proxy_Union"
)
dbExecute(decimal_con, "DROP TABLE Q_3d24_Occupations_Unknown")
dbExecute(
  decimal_con,
  "DROP TABLE Q_3d3_Occupations_Unknown_LCP2_Private_Cred_Proxy"
)
dbExecute(
  decimal_con,
  "DROP TABLE Q_3d4_Occupations_by_LCIP4_CRED_LCP2_LCP2_Private_Union"
)
dbExecute(decimal_con, "DROP TABLE Q_3e_Occupations_Unknown")
dbExecute(decimal_con, "DROP TABLE Q_3e2_Occupations_Unknown")
dbExecute(decimal_con, "DROP TABLE Q_3e3_Occupations_by_LCIP4_CRED_LCP2_Union")
#dbExecute(decimal_con, "DROP TABLE tmp_tbl_Q3b12_Occupations_by_LCIP4_CRED_No_TT_Union_tmp")
#dbExecute(decimal_con, "DROP TABLE tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union")

# ---- Q_4_NOC_D Series ----
dbExecute(decimal_con, Q_4_NOC_1D_Totals_by_PSSM_CRED)
dbExecute(decimal_con, Q_4_NOC_1D_Totals_by_Year)
dbExecute(decimal_con, Q_4_NOC_2D_Totals_by_PSSM_CRED)
dbExecute(decimal_con, Q_4_NOC_2D_Totals_by_PSSM_CRED_Appendix)
dbExecute(decimal_con, Q_4_NOC_2D_Totals_by_Year)
dbExecute(decimal_con, Q_4_NOC_3D_Totals_by_PSSM_CRED)
dbExecute(decimal_con, Q_4_NOC_3D_Totals_by_Year)
dbExecute(decimal_con, Q_4_NOC_4D_Totals_by_PSSM_CRED)
dbExecute(decimal_con, Q_4_NOC_4D_Totals_by_Year)
dbExecute(decimal_con, Q_4_NOC_5D_Totals_by_PSSM_CRED)
dbExecute(decimal_con, Q_4_NOC_5D_Totals_by_Year)
# FIXME dbExecute(decimal_con, Q_4_NOC_5D_Totals_by_Year_Input_for_Rounding)

# ---- Q_4_NOC_Totals Series ----
# FIXME dbGetQuery(decimal_con, Q_4_NOC_Totals_by_Year_and_PSSM_CRED)
dbExecute(decimal_con, Q_4_NOC_Totals_by_Year)
dbExecute(decimal_con, Q_4_NOC_Totals_by_Year_BC)
dbExecute(decimal_con, Q_4_NOC_Totals_by_Year_Total)

dbExecute(decimal_con, "DROP TABLE Q_4_NOC_1D_Totals_by_PSSM_CRED")
dbExecute(decimal_con, "DROP TABLE Q_4_NOC_1D_Totals_by_Year")
dbExecute(decimal_con, "DROP TABLE Q_4_NOC_2D_Totals_by_PSSM_CRED")
dbExecute(decimal_con, "DROP TABLE Q_4_NOC_2D_Totals_by_PSSM_CRED_Appendix")
dbExecute(decimal_con, "DROP TABLE Q_4_NOC_2D_Totals_by_Year")
dbExecute(decimal_con, "DROP TABLE Q_4_NOC_3D_Totals_by_PSSM_CRED")
dbExecute(decimal_con, "DROP TABLE Q_4_NOC_3D_Totals_by_Year")
dbExecute(decimal_con, "DROP TABLE Q_4_NOC_4D_Totals_by_PSSM_CRED")
dbExecute(decimal_con, "DROP TABLE Q_4_NOC_4D_Totals_by_Year")
dbExecute(decimal_con, "DROP TABLE Q_4_NOC_5D_Totals_by_PSSM_CRED")
dbExecute(decimal_con, "DROP TABLE Q_4_NOC_5D_Totals_by_Year")
#dbExecute(decimal_con, "DROP TABLE Q_4_NOC_4D_Totals_by_Year_Input_for_Rounding")

# ---- Q_5 Series ----
dbExecute(decimal_con, Q_5_NOC_Totals_by_Year_and_BC)
dbExecute(decimal_con, Q_5_NOC_Totals_by_Year_and_BC_and_Total)
dbExecute(decimal_con, "DROP TABLE Q_4_NOC_Totals_by_Year")
dbExecute(decimal_con, "DROP TABLE Q_4_NOC_Totals_by_Year_BC")
dbExecute(decimal_con, "DROP TABLE Q_4_NOC_Totals_by_Year_Total")

# ---- Q_6 Series ----
if (regular_run == T) {
  dbExecute(decimal_con, Q_6_tmp_tbl_Model)
}

if (qi_run == T) {
  dbExecute(decimal_con, Q_6_tmp_tbl_Model_QI) # QI toggle
}

if (ptib_run == T) {
  dbExecute(decimal_con, Q_6_tmp_tbl_Model_Inc_Private_Inst)
}
#dbExecute(decimal_con, Q_6_tmp_tbl_Model_Program_Projection)

if (regular_run == T | qi_run == T) {
  dbExecute(decimal_con, "DROP TABLE Q_5_NOC_Totals_by_Year_and_BC")
  dbExecute(decimal_con, "DROP TABLE Q_5_NOC_Totals_by_Year_and_BC_and_Total")

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
  dbExecute(
    decimal_con,
    "DROP TABLE tmp_tbl_Q3b12_Occupations_by_LCIP4_CRED_No_TT_Union_tmp"
  )
  dbExecute(
    decimal_con,
    "DROP TABLE tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union"
  )
  dbExecute(
    decimal_con,
    "DROP TABLE tmp_tbl_Q_2a4_Labour_Supply_by_LCIP4_CRED_No_TT_Union_tmp"
  )
  dbExecute(
    decimal_con,
    "DROP TABLE tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union_tmp"
  )
  dbExecute(
    decimal_con,
    "DROP TABLE tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union_tmp"
  )
  dbExecute(
    decimal_con,
    "DROP TABLE tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union"
  )

  dbExecute(decimal_con, "DROP TABLE Q_1_Grad_Projections_by_Age_by_Program")
  dbExecute(
    decimal_con,
    "DROP TABLE Q_1_Grad_Projections_by_Age_by_Program_Static"
  )
  dbExecute(decimal_con, "DROP TABLE Q_1c_Grad_Projections_by_Program_LCP2")
  dbExecute(decimal_con, "DROP TABLE Q_1c_Grad_Projections_by_Program")

  # Lookups
  dbExecute(decimal_con, "drop table INFOWARE_L_CIP_4DIGITS_CIP2016")
  dbExecute(decimal_con, "drop table INFOWARE_L_CIP_6DIGITS_CIP2016")
  #dbExecute(decimal_con, "DROP TABLE T_NOC_Skill_Type")
  #dbExecute(decimal_con, "DROP TABLE tbl_NOC_Skill_Level_Aged_17_34")
  dbExecute(decimal_con, "DROP TABLE T_Current_Region_PSSM_Rollup_Codes")
  dbExecute(decimal_con, "DROP TABLE T_Current_Region_PSSM_Rollup_Codes_BC")
  dbExecute(decimal_con, "DROP TABLE T_PSSM_CRED_RECODE")
  dbExecute(decimal_con, "DROP TABLE T_Exclude_from_Projections_LCP4_CD")
  dbExecute(decimal_con, "DROP TABLE T_Exclude_from_Projections_LCIP4_CRED")
  dbExecute(
    decimal_con,
    "DROP TABLE T_Exclude_from_Projections_PSSM_Credential"
  )
  dbExecute(decimal_con, "DROP TABLE tbl_Age_Groups")
  dbExecute(decimal_con, "DROP TABLE tbl_Age_Groups_Rollup")
  dbExecute(
    decimal_con,
    "DROP TABLE T_Exclude_from_Labour_Supply_Unknown_LCP2_Proxy"
  )
}
# Keep
# dbExists(decimal_con, "")
# dbExists(decimal_con, "")
