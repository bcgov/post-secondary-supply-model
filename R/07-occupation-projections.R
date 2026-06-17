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

# toggle static or projected.
if (model == "static") {
  cohort_program_distributions <- cohort_program_distributions_static
} else {
  cohort_program_distributions <- cohort_program_distributions_projected
}

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

# # compare all required tables to SQL versions
# for (table_name in required_tables) {
#   print(glue("Comparing {table_name} to SQL version..."))
#   res <- compare(table_name, base::get(table_name))
#   cat("\n\n\n")
# }



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
# NLS and PERCENT columns may since they represent a ratio.

# --- delete existing PTIB Surveys
# done in load script

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

# ---- Q_1 Series ----
# dbExecute(decimal_con, Q_1_Grad_Projections_by_Age_by_Program)
# run distinct here to remove duplicates in case you 
# grabbed the dbo version of graduate_projections (development only).
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
    GRADS = GRADUATES * PERCENT
  ) |> 
  select(PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP, YEAR, LCP4_CD, GRAD_STATUS, TTRAIN, LCIP4_CRED, GRADS)


# dbExecute(decimal_con, Q_1_Grad_Projections_by_Age_by_Program_Static)
# this will be identical to the query above, if the model toggle is set to
# static (odd choice but we can deal with this later).
q_1_grad_projections_by_age_by_program_static <- graduate_projections |>
  distinct(PSSM_CRED, AGE_GROUP, YEAR, GRADUATES) |>
  inner_join(
    cohort_program_distributions,
    by = join_by(PSSM_CRED, AGE_GROUP, YEAR)
  ) |>
  anti_join(
    t_exclude_from_projections_pssm_credential,
    by = "PSSM_CREDENTIAL"
  ) |>
  anti_join(
    t_exclude_from_projections_lcp4_cd,
    by = c("LCP4_CD" = "LCIP_LCP4_CD")
  ) |>
  anti_join(
    t_exclude_from_projections_lcip4_cred,
    by = "LCIP4_CRED"
  ) |>
  mutate(
    GRADS = GRADUATES * PERCENT,
    LCIP4_CRED = NA_character_
  ) |> 
  select(PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP, YEAR, LCP4_CD, GRAD_STATUS, TTRAIN, LCIP4_CRED, GRADS)

# dbGetQuery(decimal_con, Q_1b_Checking_Grads_by_Year_Excludes_CIPs)
# shows that this query is still being handled differently in R and SQL
# primarily because of the OR vs or issue in credential labels
q_1_grad_projections_by_age_by_program |>
  group_by(PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP, YEAR) |>
  summarise(GRADS = sum(GRADS), .groups = "drop") |>
  pivot_wider(names_from = YEAR, values_from = GRADS, values_fill = 0) |>
  arrange(PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP)

#dbExecute(decimal_con, Q_1c_Grad_Projections_by_Program)
q_1c_grad_projections_by_program <- q_1_grad_projections_by_age_by_program |>
  inner_join(tbl_age_groups, by = c("AGE_GROUP" = "AGE_GROUP_LABEL")) |>
  inner_join(tbl_age_groups_rollup, by = "AGE_GROUP_ROLLUP") |>
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

#dbExecute(decimal_con, Q_1c_Grad_Projections_by_Program_LCP2)
q_1c_grad_projections_by_program_lcp2 <- q_1_grad_projections_by_age_by_program |>
  inner_join(tbl_age_groups, by = c("AGE_GROUP" = "AGE_GROUP_LABEL")) |>
  inner_join(tbl_age_groups_rollup, by = "AGE_GROUP_ROLLUP") |>
  mutate(
    LCP2_CD = str_sub(LCP4_CD, 1, 2),
    LCIP2_CRED = paste0(
      case_when(
        str_sub(PSSM_CRED, 1, 1) %in% c("1", "3", "P") ~ paste0(
          str_sub(PSSM_CRED, 1, 1),
          " - "
        ),
        TRUE ~ ""
      ),
      LCP2_CD,
      " - ",
      if_else(is.na(TTRAIN), "", paste0(TTRAIN, " - ")),
      PSSM_CREDENTIAL
    )
  ) |>
  summarise(
    GRADS = sum(GRADS),
    .by = c(
      PSSM_CREDENTIAL,
      PSSM_CRED,
      AGE_GROUP_ROLLUP,
      AGE_GROUP_ROLLUP_LABEL,
      YEAR,
      LCP2_CD,
      GRAD_STATUS,
      TTRAIN,
      LCIP2_CRED
    )
  ) 

# ---- Q_2 Series ----
# dbExecute(decimal_con, Q_2_Labour_Supply_by_LCIP4_CRED)
# Find all records where there are respondents for 4-digit CIP and age group rollup; 
# Calc NLS using No Labour Supply Distribution.
q_2_labour_supply_by_lcip4_cred <- q_1c_grad_projections_by_program |>
  inner_join(
    labour_supply_distribution |>
      select(
        LCIP4_CRED,
        NEW_LABOUR_SUPPLY,
        CURRENT_REGION_PSSM_CODE_ROLLUP,
        AGE_GROUP_ROLLUP
      ),
    by = join_by(LCIP4_CRED, AGE_GROUP_ROLLUP)
  ) |>
  mutate(NLS = GRADS * NEW_LABOUR_SUPPLY) |>
  select(-GRADS, -GRAD_STATUS)

# Combine the following queries for readability
# - dbExecute(decimal_con, Q_2a_Labour_Supply_Unknown)
# - dbExecute(decimal_con, Q_2a2_Labour_Supply_Unknown_No_TT_Proxy)
# - dbExecute(decimal_con, Q_2a3_Labour_Supply_by_LCIP4_CRED_No_TT_Proxy_Union)
# - dbExecute(decimal_con, Q_2a4_Labour_Supply)
# 1 Finds all records where there are no respondents for 4-digit CIP and age group rollup; 
# Basically those where TTRAIN was blank in LCIP4_CRED and in the Labour_Supply_Distribution table bc it had TTRAIN=0 injected + 
# private training institutions, as these aren't in the Labour_Supply_Distribution table at all.
# 2. Calc NLS using No TT Labour Supply Distribution. This is the labour supply distribution without the TTRAIN variable 
# embedded in LCIP4_CRED to capture those programs that don’t have TTRAIN specified; 

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
    by = join_by(LCIP4_CRED, AGE_GROUP_ROLLUP)
  ) |>
  mutate(NLS = GRADS * NEW_LABOUR_SUPPLY) |>
  select(names(q_2_labour_supply_by_lcip4_cred))

# add to labour supply by LCIP4_CRED
tmp_tbl_q_2a4_labour_supply_by_lcip4_cred_no_tt_union_tmp <- bind_rows(
  q_2_labour_supply_by_lcip4_cred,
  q_2a2_labour_supply_unknown_no_tt_proxy
)

rm(q_2a2_labour_supply_unknown_no_tt_proxy)

# Combine the following queries for readability
# - dbExecute(decimal_con, Q_2b_Labour_Supply_Unknown)
# - dbExecute(decimal_con, Q_2b2_Labour_Supply_Unknown_Private_Cred_Proxy)
# - dbExecute(decimal_con, Q_2b3_Labour_Supply_by_LCIP4_CRED_Private_Cred_Proxy_Union)

# 1. finds all the records where labour supply still isn’t specified yet.
# 2. calcs NLS for private institutions for 4-digit CIPs were no survey respondents 
# Note 1. substituting CERT for DIPL and DIPL for CERT 4-digit CIP results b/c the 
# Private Training Institution Branch says that private institutions do not have a 
# hard and fast definition of credentials by length so CERT and DIPL used interchangeably; 
# Note 2. use Labour_Supply_Distribution_No_TT table b/c obviously private institutions don’t have trades training variable; 
# Note 3. because of the CERT/DIPL substitution,  actual total will be larger, reducing the unknown

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
  mutate(NLS = GRADS * NEW_LABOUR_SUPPLY, 
        PSSM_CRED = PSSM_CRED.x) |>
  select(names(tmp_tbl_q_2a4_labour_supply_by_lcip4_cred_no_tt_union_tmp))

q_2b3_labour_supply_by_lcip4_cred_private_cred_proxy_union <- bind_rows(
  tmp_tbl_q_2a4_labour_supply_by_lcip4_cred_no_tt_union_tmp,
  q_2b2_labour_supply_unknown_private_cred_proxy 
)

rm(q_2b_labour_supply_unknown, q_2b2_labour_supply_unknown_private_cred_proxy)


# Combine the following queries for readability
# - dbExecute(decimal_con, Q_2b4_Labour_Supply_Unknown)
# - dbExecute(decimal_con, Q_2c_Labour_Supply_Unknown_LCP2_Proxy)
# - dbExecute(decimal_con, Q_2c2_Labour_Supply_Unknown_LCP2_Proxy_Union)
# 1. finds all the records where labour supply still isn’t specified yet.
# 2. calcs NLS for all 4-digit CIPs with no survey respondents (matching all the filter criteria) 
# Note 1. connecting to the 2-digit CIP as a proxy via T_LCP2_LCP4 (2-digit to 4-digit CIP link); 
# (make sure this is up-to-date - shouldn’t change until CIP 2016 updated again). 
# Must have links between Q_2b4_Labour_Supply_Unknown and Q_2_Labour_Supply_by_LCIP2_CRED on PSSM_CRED, Year and Age_Group_Rollup_Label; 
# Note 2. excludes programs that we don’t want 2-digit CIP to serve as a proxy for 4-digit CIPs (51 medical programs due to close program occ linkage). 
# Note 3. allow the private institutions to use this proxy for all programs since the alternative is using an NHS tab which doesn’t have economic region in it. 
# Note 4. Updated filter to “P - “ since now have the PDEG degree.

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
    by = join_by(AGE_GROUP_ROLLUP, PSSM_CRED, LCIP_LCP2_CD == LCP2_CD)
  ) |>
  mutate(NLS = GRADS * NEW_LABOUR_SUPPLY) |>
  select(names(q_2b3_labour_supply_by_lcip4_cred_private_cred_proxy_union))


q_2c2_labour_supply_unknown_lcp2_proxy_union <- bind_rows(
  q_2b3_labour_supply_by_lcip4_cred_private_cred_proxy_union,
  q_2c_labour_supply_unknown_lcp2_proxy 
)

rm(q_2c_labour_supply_unknown_lcp2_proxy)

# Combine the following queries for readability
# - dbExecute(decimal_con, Q_2c3_Labour_Supply_Unknown)
# - dbExecute(decimal_con, Q_2c4_Labour_Supply_Unknown_LCP2_Proxy_No_TT)
# - dbExecute(decimal_con, Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union)
# - dbExecute(decimal_con, Q_2d2_Labour_Supply)
# 1. finds all the records where labour supply still isn’t specified yet.

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


# there was a bug in the original query. The filter is intended to capture the records where 
# is.na(LCIP_LCP4_CD) is true, but also include any records where the LCIP4_CRED starts with "P - " (i.e. private institutions).
# filter s.b. OR

q_2c4_labour_supply_unknown_lcp2_proxy_no_tt <- labour_supply_distribution_lcp2_no_tt |>
        select(LCP2_CD, AGE_GROUP_ROLLUP, PSSM_CRED, NEW_LABOUR_SUPPLY, CURRENT_REGION_PSSM_CODE_ROLLUP) |>
        inner_join(
          t_exclude_from_labour_supply_unknown_lcp2_proxy |> 
          right_join(
            q_2c3_labour_supply_unknown |> 
            mutate(LCIP_LCP4_CD = LCP4_CD), by = c("LCIP_LCP4_CD" = "LCIP_LCP4_CD"), keep = TRUE) |> 
            rename(LCIP_LCP4_CD = LCIP_LCP4_CD.x) |>
          inner_join(t_lcp2_lcp4, by = c("LCP4_CD" = "LCIP_LCP4_CD")) |>
            select(-LCIP_LCP4_CD.y, -LCIP_LCP2_CD.x) |>
            rename(LCP2_CD = LCIP_LCP2_CD.y), 
          by = c("LCP2_CD" = "LCP2_CD", "AGE_GROUP_ROLLUP" = "AGE_GROUP_ROLLUP", "PSSM_CRED" = "PSSM_CRED")) |>
  filter(is.na(LCIP_LCP4_CD) & str_detect(LCIP4_CRED, "P - ")) |>
  mutate(NLS = GRADS * NEW_LABOUR_SUPPLY) |>
  select(names(q_2c2_labour_supply_unknown_lcp2_proxy_union))
  



tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union_tmp  <- bind_rows(
  q_2c2_labour_supply_unknown_lcp2_proxy_union,
  q_2c4_labour_supply_unknown_lcp2_proxy_no_tt)

rm(q_2c4_labour_supply_unknown_lcp2_proxy_no_tt, q_2c2_labour_supply_unknown_lcp2_proxy_union, q_2c3_labour_supply_unknown)

# Combine the following queries for readability
# - dbExecute(decimal_con, Q_2d2_Labour_Supply_Unknown)
# - dbExecute(decimal_con, Q_2d3_Labour_Supply_Unknown_LCP2_Private_Cred_Proxy)
# - dbExecute(decimal_con, Q_2d4_Labour_Supply_by_LCIP4_CRED_LCP2_LCP2_Private_Union)
# - dbExecute(decimal_con, Q_2f_Labour_Supply)
# 1. finds all the records where labour supply still isn’t specified yet.

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
    TTRAIN = TTRAIN.x,
    LCP4_CD,
    LCIP4_CRED,
    CURRENT_REGION_PSSM_CODE_ROLLUP,
    NEW_LABOUR_SUPPLY,
    NLS = GRADS * NEW_LABOUR_SUPPLY
  )


tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union  <- bind_rows(
  tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union_tmp,
  q_2d3_labour_supply_unknown_lcp2_private_cred_proxy |>
    select(names(tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union_tmp))
)

rm(q_2d2_labour_supply_unknown, q_2d3_labour_supply_unknown_lcp2_private_cred_proxy)


# --- 2f series
# dbExecute(decimal_con, Q_2f2_Labour_Supply_Unknown) # numbers are low


# not used?
q_2f_labour_supply <- q_1c_grad_projections_by_program |>
  anti_join(
    tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union,
    by = join_by(LCIP4_CRED, AGE_GROUP_ROLLUP)
  ) |>
  summarise(
    GRADS = sum(GRADS),
    .by = c(
      PSSM_CREDENTIAL,
      PSSM_CRED,
      AGE_GROUP_ROLLUP,
      AGE_GROUP_ROLLUP_LABEL,
      LCP4_CD,
      LCIP4_CRED,
      YEAR
    )
  )

rm(q_2f_labour_supply)

# remove q1 queries
removers <- ls()[grep("q_1", ls())]
rm(list = removers)
  
# ---- Q_3 Series ----
# dbExecute(decimal_con, Q_3_Occupations_by_LCIP4_CRED)

# --- 03B Series
# dbExecute(decimal_con, Q_3b_Occupations_Unknown) # numbers too high
# dbExecute(decimal_con, Q_3b11_Ocupations_Unknown_No_TT_Proxy) # numbers too high
# dbExecute(decimal_con, q_3b12_Occupations_by_LCIP4_CRED_No_TT_Proxy_Union)
# dbExecute(decimal_con, Q_3b13_Occupations)
# dbExecute(decimal_con, Q_3b14_Occupations_Unknown)
# dbExecute(decimal_con, Q_3b2_Occupations_Unknown_Private_Cred_Proxy)
# dbExecute(decimal_con, Q_3b3_Occupations_by_LCIP4_CRED_Private_Cred_Proxy_Union)
# dbExecute(decimal_con, Q_3b4_Occupations_Unknown)

q_3_occupations_by_lcip4_cred <- tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union |>
  inner_join(
    occupation_distributions |> 
      select(NOC, PERCENT, LCIP4_CRED, CURRENT_REGION_PSSM_CODE_ROLLUP, AGE_GROUP_ROLLUP)
      , by = join_by(
      LCIP4_CRED,
      CURRENT_REGION_PSSM_CODE_ROLLUP,
      AGE_GROUP_ROLLUP
    )
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

q_3b_occupations_unknown <- tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union |>
  anti_join(
    occupation_distributions |> 
      select(NOC, PERCENT, LCIP4_CRED, CURRENT_REGION_PSSM_CODE_ROLLUP, AGE_GROUP_ROLLUP),
    by = join_by(
      LCIP4_CRED,
      CURRENT_REGION_PSSM_CODE_ROLLUP,
      AGE_GROUP_ROLLUP
    )
  )

q_3b11_occupations_unknown_no_tt_proxy <- q_3b_occupations_unknown |>
  inner_join(
    occupation_distributions_no_tt |> 
      select(NOC, PERCENT,LCP4_CD, PSSM_CRED, CURRENT_REGION_PSSM_CODE_ROLLUP, AGE_GROUP_ROLLUP),
    by = join_by(
      CURRENT_REGION_PSSM_CODE_ROLLUP,
      LCP4_CD,
      AGE_GROUP_ROLLUP,
      PSSM_CRED
    )
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


# dbExecute(decimal_con, Q_3b2_Occupations_Unknown_Private_Cred_Proxy)
q_3b2_occupations_unknown_private_cred_proxy <- 
  q_3b14_occupations_unknown |> 
  inner_join(
    tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union |> 
    select(LCIP4_CRED, YEAR, AGE_GROUP_ROLLUP, PSSM_CRED, LCP4_CD, CURRENT_REGION_PSSM_CODE_ROLLUP), by = join_by(LCIP4_CRED, YEAR, AGE_GROUP_ROLLUP, PSSM_CRED)
   ) |> 
  inner_join(occupation_distributions_no_tt |> 
    select(NOC, PERCENT, LCP4_CD, AGE_GROUP_ROLLUP, CURRENT_REGION_PSSM_CODE_ROLLUP, PSSM_CRED), 
    by = join_by(LCP4_CD.y == LCP4_CD, AGE_GROUP_ROLLUP, CURRENT_REGION_PSSM_CODE_ROLLUP.y == CURRENT_REGION_PSSM_CODE_ROLLUP)) |>
  filter((PSSM_CRED.x == 'P - CERT' & PSSM_CRED.y == 'P - DIPL') | (PSSM_CRED.x == 'P - DIPL' & PSSM_CRED.y == 'P - CERT')) |>
  mutate(OCCSN = NLS * PERCENT) |>
  rename(PSSM_CRED = PSSM_CRED.x, LCP4_CD = LCP4_CD.x, CURRENT_REGION_PSSM_CODE_ROLLUP = CURRENT_REGION_PSSM_CODE_ROLLUP.x)  |>
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

rm(q_3_occupations_by_lcip4_cred, q_3b_occupations_unknown, q_3b11_occupations_unknown_no_tt_proxy, 
  q_3b12_occupations_by_lcip4_cred_no_tt_proxy_union, q_3b14_occupations_unknown, q_3b3_occupations_by_lcip4_cred_private_cred_proxy_union)

# --- 03C Series
#dbExecute(decimal_con, Q_3c_Occupations_Unknown_LCP2_Proxy)
q_3c_occupations_unknown_lcp2_proxy <- q_3b4_occupations_unknown |>
  left_join(
    t_exclude_from_labour_supply_unknown_lcp2_proxy |> transmute(LCP4_CD = LCIP_LCP4_CD, LCIP_LCP4_CD),
    by = join_by(LCP4_CD)
  ) |>
  filter(
    is.na(LCIP_LCP4_CD) | str_starts(LCIP4_CRED, "P - ")
  ) |>
  inner_join(
    t_lcp2_lcp4 |> rename(LCP4_CD = LCIP_LCP4_CD),
    by = join_by(LCP4_CD)
  ) |>
  inner_join(
    occupation_distributions_lcp2 |> select(-PSSM_CREDENTIAL, -TTRAIN),
    by = join_by(
      LCIP_LCP2_CD == LCP2_CD,
      AGE_GROUP_ROLLUP,
      CURRENT_REGION_PSSM_CODE_ROLLUP,
      PSSM_CRED
    )) |>
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

# --- 03D Series
# dbExecute(decimal_con, Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union)
# dbExecute(decimal_con, Q_3d2_Occupations)
# dbExecute(decimal_con, Q_3d2_Occupations_Unknown)
# dbExecute(decimal_con, Q_3d21_Occupations_Unknown_LCP2_Proxy_No_TT)
# dbExecute(decimal_con, Q_3d22_Occupations_by_LCIP4_CRED_LCP2_No_T_Proxy_Union)
# dbExecute(decimal_con, Q_3d24_Occupations_Unknown)
# dbExecute(decimal_con, Q_3d3_Occupations_Unknown_LCP2_Private_Cred_Proxy)
# dbExecute(decimal_con, Q_3d4_Occupations_by_LCIP4_CRED_LCP2_LCP2_Private_Union)

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
    select(NOC, PERCENT, LCP2_CD, AGE_GROUP_ROLLUP, CURRENT_REGION_PSSM_CODE_ROLLUP, PSSM_CRED), 
    by = join_by(LCIP_LCP2_CD == LCP2_CD)
    ) |>
   select(-LCIP_LCP2_CD,) |>
   inner_join(
    q_3d2_occupations_unknown |>
    left_join(t_exclude_from_labour_supply_unknown_lcp2_proxy |> 
    mutate(LCP4_CD = LCIP_LCP4_CD), 
    by = join_by(LCP4_CD)
    ), 
  by = join_by(LCIP_LCP4_CD == LCP4_CD, AGE_GROUP_ROLLUP, CURRENT_REGION_PSSM_CODE_ROLLUP, PSSM_CRED)) |>
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
    ), keep = TRUE
  ) |>
  filter(
    is.na(AGE_GROUP_ROLLUP.y) &
    is.na(YEAR.y) &
    is.na(LCIP4_CRED.y) &
    is.na(CURRENT_REGION_PSSM_CODE_ROLLUP.y)
  ) |> select(-ends_with(".y")) |> rename_with(~str_replace(., "\\.x$", ""))

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
    TTRAIN = TTRAIN.x,
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


q_3e3_occupations_by_lcip4_cred_lcp2_union <- bind_rows(
  q_3d4_occupations_by_lcip4_cred_lcp2_lcp2_private_union |> mutate(NOC = as.double(NOC)),
  q_3e2_occupations_unknown |> mutate(NOC = as.double(NOC))
)


# remove q2 queries
removers <- ls()[grep("q_2", ls())]
rm(list = removers)

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

# remove q3 queries except for the final union table
removers <- ls()[grep("q_3|q3", ls())]
removers <- setdiff(removers, "tmp_tbl_q_3d_occupations_by_lcip4_cred_lcp2_union")
rm(list = removers)

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
  mutate(NOC = str_pad(as.character(NOC), width = 5, side = "left", pad = "0")) |>
  inner_join(
    t_current_region_pssm_rollup_codes |> select(-OLD_CURRENT_REGION_PSSM_CODE_ROLLUP), 
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

sum_noc_totals <- function(data, level) {
  noc_cols <- paste0(c("NOC_", "NOC_", "NOC_"), level, c("", "_LEVEL", "_ENGLISH_NAME"))
  
  data |>
    summarise(
      OCCSN = sum(OCCSN), 
      .by = c(YEAR, AGE_GROUP_ROLLUP_LABEL, CURRENT_REGION_PSSM_CODE_ROLLUP, 
              CURRENT_REGION_PSSM_NAME_ROLLUP, all_of(noc_cols))
    ) |> 
    pivot_wider(names_from = YEAR, values_from = OCCSN, values_fill = 0) |>
    rename(
      NOC = !!sym(noc_cols[1]),
      NOC_LEVEL = !!sym(noc_cols[2]),
      ENGLISH_NAME = !!sym(noc_cols[3])
    ) 
}

q_4_noc_1d_totals_by_year <- sum_noc_totals(noc_projections_base , 1)
q_4_noc_2d_totals_by_year <- sum_noc_totals(noc_projections_base , 2)
q_4_noc_3d_totals_by_year <- sum_noc_totals(noc_projections_base , 3)
q_4_noc_4d_totals_by_year <- sum_noc_totals(noc_projections_base , 4)
q_4_noc_5d_totals_by_year <- sum_noc_totals(noc_projections_base , 5)
# ---- Q_4_NOC_Totals Series ----
# FIXME dbGetQuery(decimal_con, Q_4_NOC_Totals_by_Year_and_PSSM_CRED)
# dbExecute(decimal_con, Q_4_NOC_Totals_by_Year)
# dbExecute(decimal_con, Q_4_NOC_Totals_by_Year_BC)
# dbExecute(decimal_con, Q_4_NOC_Totals_by_Year_Total)

q_4_noc_totals_by_year <- rbind(
  q_4_noc_4d_totals_by_year,
  q_4_noc_3d_totals_by_year,
  q_4_noc_2d_totals_by_year,
  q_4_noc_1d_totals_by_year,
  q_4_noc_5d_totals_by_year
) 


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

q_4_noc_totals_by_year_total <- q_4_noc_totals_by_year %>%
  group_by(
    AGE_GROUP_ROLLUP_LABEL,
    NOC_LEVEL,
    NOC,
    ENGLISH_NAME
  ) |>
  summarise(across(starts_with("20"), sum), .groups = "drop")|>
  mutate(
    CURRENT_REGION_PSSM_CODE_ROLLUP = 0,
    CURRENT_REGION_PSSM_NAME_ROLLUP = "Total") 

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

# ---- Q_6 Series ----
# this logic should be handled elsewhere
if (regular_run == T) {
  #dbExecute(decimal_con, Q_6_tmp_tbl_Model)
  tmp_tbl_model <- q_5_noc_totals_by_year_and_bc_and_total
}

if (qi_run == T) {
  #dbExecute(decimal_con, Q_6_tmp_tbl_Model_QI) # QI toggle
  tmp_tbl_qi <- q_5_noc_totals_by_year_and_bc_and_total
}

if (ptib_run == T) {
  #dbExecute(decimal_con, Q_6_tmp_tbl_Model_Inc_Private_Inst)
  tmp_tbl_model_inc_private_inst <- q_5_noc_totals_by_year_and_bc_and_total
}
# dbExecute(decimal_con, Q_6_tmp_tbl_Model_Program_Projection)
if(model == "program_projection"){
  tmp_tbl_model_program_projection <- q_5_noc_totals_by_year_and_bc_and_total
}

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


