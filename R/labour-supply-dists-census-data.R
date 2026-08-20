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

# This script prepares census data for the Labour_Supply_Distribution table.
#
# WHERE THIS SITS IN THE MODEL (companion: R/02b-2-pssm-cohorts-new-labour-
# supply.R; weighting context: docs/weights-explained-02b-2-and-02b-3.md):
#   OCCSN(NOC) = GRADUATES(cred,age) x P(CIP|cred,age)
#                x P(in labour supply|CIP)   <- 02b-2, benchmarked by THIS TABLE
#                x P(NOC|CIP,region)         <- 02b-3
# The four student-outcomes surveys cover trades/certificate/diploma/bachelor
# credentials, but graduate credentials (GRCT or GRDP, PDEG, MAST, DOCT) have
# no (or thin) survey coverage -- their labour-supply participation is
# benchmarked from the 2021 Census instead. 02b-2 appends this table's rows
# to labour_supply_distribution as the census benchmark (Survey label
# "2021 Census ..."; downstream consumers prefix-match that label, never
# full-label equality).
#
# MANUAL PER-CYCLE PREREQUISITE -- this script is NOT in the
# prep-for-fresh-run.R chain. When the output table below is absent on the
# model database (as it was in the 2025 refresh), run this script BY HAND
# before 02b-2; nothing else produces the table.
#
# Inputs (all must exist before running):
#   - the StatCan census export workbook (LAN data/statcan/...xlsx; sheets
#     "filtered_data" + "unfiltered_data"; the census content is stable
#     across model cycles -- the 2025-cycle export is byte-identical to the
#     previous cycle's, so re-running reproduces the same table)
#   - LAN lookup CSVs: T_Current_Region_PSSM_Rollup_Codes_StatCan.csv and
#     T_Region_StatCan_XWALK.csv (census geography spelling -> PSSM region
#     names)
#   - tbl_age_groups_rollup_r already present in the personal schema
#     (normally written by the loader chain; stage it from dbo if running
#     standalone)
#
# Output (personal schema):
#   - Labour_Supply_Distribution_Stat_Can_r -- what 02b-2's guarded,
#     `_r`-preferring read expects. Intermediates (Combined_..._Original_r
#     and the statcan rollup lookup `_r`) are dropped again in the cleanup.
#     The hardcoded SURVEY label below deliberately keeps the export VINTAGE
#     ("2021 Census PSSM 2022-2023") -- 02b-2 relabels to the current model
#     year at read time. Do not "fix" the year here.

library(tidyverse)
library(openxlsx)
library(RODBC)
library(config)
library(DBI)
## ----------------------------------------------------------
## Reasons for change, other notes
## 2025 refresh: library(RJDBC) dropped -- loaded but never used
## (the script connects via DBI/odbc), and it pulls in rJava/Oracle
## dependencies for nothing. Both census prep scripts ran clean
## with it removed.
## ----------------------------------------------------------

# ---- Configure LAN and file paths ----
db_config <- config::get("decimal")
lan <- config::get("lan")
my_schema <- config::get("myschema")

# ---- Connection to decimal ----
decimal_con <- dbConnect(
  odbc::odbc(),
  Driver = db_config$driver,
  Server = db_config$server,
  Database = db_config$database,
  Trusted_Connection = "True"
)

# ---- Import stat can data ----
# Both sheets carry decorative header rows, so data starts at row 5 and the
# 12 count columns are renamed below. They form a cross-tab of
# labour-force status x school attendance:
#   TOT_ / LF_ / LF_E_ / LF_U_  = total / in labour force / employed / unemployed
#   _tot_sa / _dnas / _as       = total school attendance / did not attend / attended
# The labour-supply numerator used later is LF_U_dnas + LF_E_tot_sa:
# unemployed-and-not-in-school + employed -- i.e. people in the labour
# force who are NOT still studying.
# GRCT/GRDP and PDEG are built from the FILTERED sheet; MAST and DOCT from
# the UNFILTERED one (a distinction inherited from the source workbook).
options(scipen = 999) # census counts in fixed notation, not scientific
stat_can_export <- glue::glue(
  "{lan}/data/statcan/stat-can-data-export-for-labour-supply-distributions.xlsx"
)

## Fix column headers ----
# TOT = total_labour_force_status, LF = "in_labour_force", LF_E = "employed", LF_U = "unemployed")
# tot_sa = "total_school_attendance", dnas = "did_not_attend_school", as = "attended_school")
VAR_status <- c("tot_sa", "dnas", "as")

cols_data <- c(
  "age_group",
  "HCDD",
  "geography",
  "major_field_cip",
  paste0("TOT_", VAR_status),
  paste0("LF_", VAR_status),
  paste0("LF_E_", VAR_status),
  paste0("LF_U_", VAR_status)
)

count_cols <- c(
  paste0("TOT_", VAR_status),
  paste0("LF_", VAR_status),
  paste0("LF_E_", VAR_status),
  paste0("LF_U_", VAR_status)
)


sc_export_unfilt_orig <- read.xlsx(
  stat_can_export,
  sheet = "unfiltered_data",
  startRow = 5
)
names(sc_export_unfilt_orig) <- cols_data
sc_export_filt_orig <- read.xlsx(
  stat_can_export,
  sheet = "filtered_data",
  startRow = 5
)
names(sc_export_filt_orig) <- cols_data

## Clean values ----
sc_export_filt_orig <- sc_export_filt_orig %>%
  mutate(
    age_group = str_trim(age_group),
    HCDD = str_trim(HCDD),
    major_field_cip = str_trim(major_field_cip)
  )

sc_export_unfilt_orig <- sc_export_unfilt_orig %>%
  mutate(
    age_group = str_trim(age_group),
    HCDD = str_trim(HCDD),
    major_field_cip = str_trim(major_field_cip)
  )

# ---- Import required lookups ----
# The xwalk translates StatCan's raw geography spellings (including a
# \x96-encoded en-dash) into the fixed region names the derivations below
# key on. The rollup lookup maps those names to PSSM region-rollup codes;
# it is written to the personal schema here because the final lookup join
# happens database-side (see "Prepare a Stat_Can version..." below). NOTE:
# occ-dists-census-data.R writes and drops the same `_r` lookup table --
# never run the two census scripts concurrently.
t_current_region_pssm_rollup_codes_statcan <-
  readr::read_csv(
    glue::glue(
      "{lan}/development/csv/gh-source/lookups/02/T_Current_Region_PSSM_Rollup_Codes_StatCan.csv"
    ),
    col_types = cols(.default = col_guess())
  ) %>%
  janitor::clean_names(case = "all_caps")

t_region_statcan_xwalk <-
  readr::read_csv(
    glue::glue(
      "{lan}/development/csv/gh-source/lookups/02/T_Region_StatCan_XWALK.csv"
    ),
    col_types = cols(.default = col_guess())
  ) %>%
  janitor::clean_names(case = "all_caps") %>%
  mutate(RAW_STAT_CAN_NAME = str_replace(RAW_STAT_CAN_NAME, "\x96", "–"))
# repeated in 'occ-dists-census-data.R'
dbWriteTable(
  decimal_con,
  name = Id(
    schema = my_schema,
    table = "t_current_region_pssm_rollup_codes_statcan_r"
  ),
  value = t_current_region_pssm_rollup_codes_statcan
)

# ---- Check for required data tables ----
# The age lookup is NOT created here -- it must already exist in the
# personal schema (loader chain output; stage from dbo when standalone).
# dbExistsTable below only PRINTS the check; the join later fails if false.
# lookups
dbExistsTable(
  decimal_con,
  Id(schema = my_schema, table = "tbl_age_groups_rollup_r")
)

# ---- Create required Region counts ----
# The census export nests some geographies; PSSM needs them split. Two
# derivations, each done by reshaping wide (pivot to one column per
# region), subtracting, and reshaping back -- repeated for the filtered
# and unfiltered sheets:
#   Northeast      = "North Coast - Nechako and Northeast" minus
#                    "North Coast and Nechako"
#   Rest of Canada = "Canada" minus "British Columbia"
## use xwalk to get "clean" names
sc_export_filt <- sc_export_filt_orig %>%
  left_join(
    t_region_statcan_xwalk,
    by = c("geography" = "RAW_STAT_CAN_NAME")
  ) %>%
  select(-geography, REGION = FIXED_NAME)

sc_export_unfilt <- sc_export_unfilt_orig %>%
  left_join(
    t_region_statcan_xwalk,
    by = c("geography" = "RAW_STAT_CAN_NAME")
  ) %>%
  select(-geography, REGION = FIXED_NAME)

## Create Northeast ----
# Take "North Coast - Nechako and Northeast" and remove "North Coast and Nechako" to get Northeast
# start with filtered data; repeated for unfiltered - add to respective data
Northeast_filt <- sc_export_filt %>%
  filter(
    REGION %in%
      c("North Coast - Nechako and Northeast", "North Coast and Nechako")
  ) %>%
  pivot_longer(
    -c("age_group", "HCDD", "major_field_cip", "REGION"),
    names_to = "variable",
    values_to = "value"
  ) %>%
  pivot_wider(names_from = "REGION", values_from = "value") %>%
  mutate(
    qry_Northeast = `North Coast - Nechako and Northeast` -
      `North Coast and Nechako`
  ) %>%
  select(-`North Coast - Nechako and Northeast`, -`North Coast and Nechako`) %>%
  pivot_wider(names_from = "variable", values_from = "qry_Northeast") %>%
  mutate(REGION = "qry_Northeast")

sc_export_filt <- sc_export_filt %>% bind_rows(Northeast_filt)

Northeast_unfilt <- sc_export_unfilt %>%
  filter(
    REGION %in%
      c("North Coast - Nechako and Northeast", "North Coast and Nechako")
  ) %>%
  pivot_longer(
    -c("age_group", "HCDD", "major_field_cip", "REGION"),
    names_to = "variable",
    values_to = "value"
  ) %>%
  pivot_wider(names_from = "REGION", values_from = "value") %>%
  mutate(
    qry_Northeast = `North Coast - Nechako and Northeast` -
      `North Coast and Nechako`
  ) %>%
  select(-`North Coast - Nechako and Northeast`, -`North Coast and Nechako`) %>%
  pivot_wider(names_from = "variable", values_from = "qry_Northeast") %>%
  mutate(REGION = "qry_Northeast")

sc_export_unfilt <- sc_export_unfilt %>% bind_rows(Northeast_unfilt)
## Create Rest of Canada counts ----
# Take "Canada" and remove "British Columbia" to get Rest of Canada
# start with filtered data; repeated for unfiltered - add to respective data
Rest_of_Canada_filt <- sc_export_filt %>%
  filter(REGION %in% c("British Columbia", "Canada")) %>%
  pivot_longer(
    -c("age_group", "HCDD", "major_field_cip", "REGION"),
    names_to = "variable",
    values_to = "value"
  ) %>%
  pivot_wider(names_from = "REGION", values_from = "value") %>%
  mutate(qry_Rest_of_Canada = Canada - `British Columbia`) %>%
  select(-Canada, -`British Columbia`) %>%
  pivot_wider(names_from = "variable", values_from = "qry_Rest_of_Canada") %>%
  mutate(REGION = "qry_Rest_of_Canada")

sc_export_filt <- sc_export_filt %>% bind_rows(Rest_of_Canada_filt)

Rest_of_Canada_unfilt <- sc_export_unfilt %>%
  filter(REGION %in% c("British Columbia", "Canada")) %>%
  pivot_longer(
    -c("age_group", "HCDD", "major_field_cip", "REGION"),
    names_to = "variable",
    values_to = "value"
  ) %>%
  pivot_wider(names_from = "REGION", values_from = "value") %>%
  mutate(qry_Rest_of_Canada = Canada - `British Columbia`) %>%
  select(-Canada, -`British Columbia`) %>%
  pivot_wider(names_from = "variable", values_from = "qry_Rest_of_Canada") %>%
  mutate(REGION = "qry_Rest_of_Canada")

sc_export_unfilt <- sc_export_unfilt %>% bind_rows(Rest_of_Canada_unfilt)

# ---- Update data for each designation separately ----
# Shared recipe per credential (GRCT/GRDP, PDEG, MAST; DOCT varies, see its
# section): for each age x CIP cell,
#   LABOUR_SUPPLY      = min(unemployed-not-in-school + employed, total
#                            not-in-school) -- the census count of people
#                            in the labour force and NOT still studying
#   NEW_LABOUR_SUPPLY  = LABOUR_SUPPLY / the CANADA-WIDE total for the cell
#                        -- participation is expressed against the national
#                        pool (the anchor 02b-2's benchmark rows use), not
#                        the regional one
#   TOTAL              = the Canada-wide total (0 when supply is 0)
# The Canada totals are joined on from the "Canada" rows of the same data.

## GRCT or GRDP (uses filtered data) ----
grct_grdp_data <- sc_export_filt %>%
  filter(HCDD == "University certificate or diploma above bachelor level")

# get Canada totals by age_group and cip
grct_grdp_canada <- grct_grdp_data %>%
  filter(REGION == "Canada") %>%
  select(age_group, major_field_cip, TOT_tot_sa_Canada = TOT_tot_sa)

# append Canada totals; run required calculations
grct_grdp_data <- grct_grdp_data %>%
  left_join(grct_grdp_canada, by = c("age_group", "major_field_cip")) %>%
  mutate(
    LABOUR_SUPPLY = ifelse(
      (LF_U_dnas + LF_E_tot_sa) > TOT_tot_sa,
      TOT_tot_sa,
      (LF_U_dnas + LF_E_tot_sa)
    )
  ) %>%
  mutate(
    NEW_LABOUR_SUPPLY = ifelse(
      TOT_tot_sa_Canada == 0,
      0,
      LABOUR_SUPPLY / TOT_tot_sa_Canada
    )
  ) %>%
  mutate(TOTAL = ifelse(LABOUR_SUPPLY == 0, 0, TOT_tot_sa_Canada)) %>%
  mutate(PSSM_CREDENTIAL = "GRCT or GRDP", PSSM_CRED = PSSM_CREDENTIAL)

## PDEG (uses filtered data) ----
pdeg_data <- sc_export_filt %>%
  filter(
    HCDD == "Degree in medicine, dentistry, veterinary medicine or optometry"
  )

# get Canada totals by age_group and cip
pdeg_canada <- pdeg_data %>%
  filter(REGION == "Canada") %>%
  select(age_group, major_field_cip, TOT_tot_sa_Canada = TOT_tot_sa)

# append Canada totals; run required calculations
pdeg_data <- pdeg_data %>%
  left_join(pdeg_canada, by = c("age_group", "major_field_cip")) %>%
  mutate(
    LABOUR_SUPPLY = ifelse(
      (LF_U_dnas + LF_E_tot_sa) > TOT_tot_sa,
      TOT_tot_sa,
      (LF_U_dnas + LF_E_tot_sa)
    )
  ) %>%
  mutate(
    NEW_LABOUR_SUPPLY = ifelse(
      TOT_tot_sa_Canada == 0,
      0,
      LABOUR_SUPPLY / TOT_tot_sa_Canada
    )
  ) %>%
  mutate(TOTAL = ifelse(LABOUR_SUPPLY == 0, 0, TOT_tot_sa_Canada)) %>%
  mutate(PSSM_CREDENTIAL = "PDEG", PSSM_CRED = PSSM_CREDENTIAL)

## MAST (uses unfiltered data) ----
mast_data <- sc_export_unfilt %>%
  filter(HCDD == "Master's degree")

# get Canada totals by age_group and cip
mast_canada <- mast_data %>%
  filter(REGION == "Canada") %>%
  select(age_group, major_field_cip, TOT_tot_sa_Canada = TOT_tot_sa)

# append Canada totals; run required calculations
mast_data <- mast_data %>%
  left_join(mast_canada, by = c("age_group", "major_field_cip")) %>%
  mutate(
    LABOUR_SUPPLY = ifelse(
      (LF_U_dnas + LF_E_tot_sa) > TOT_tot_sa,
      TOT_tot_sa,
      (LF_U_dnas + LF_E_tot_sa)
    )
  ) %>%
  mutate(
    NEW_LABOUR_SUPPLY = ifelse(
      TOT_tot_sa_Canada == 0,
      0,
      LABOUR_SUPPLY / TOT_tot_sa_Canada
    )
  ) %>%
  mutate(TOTAL = ifelse(LABOUR_SUPPLY == 0, 0, TOT_tot_sa_Canada)) %>%
  mutate(PSSM_CREDENTIAL = "MAST", PSSM_CRED = PSSM_CREDENTIAL)

## DOCT (uses unfiltered data) ----
# Doctorates deviate from the shared recipe. Census suppression/rounding on
# small cells can saturate BC's own ratio at exactly 1 on a tiny base
# (BC_LABOUR_SUPPLY <= 30 -- cells this small are unreliable), so:
#   - where that happens (or the supply is 0), BC falls back to the
#     ALL-CIP total ratio for that age group (TOT_BC_NEW_LS);
#   - BC's per-CIP labour supply is then redistributed across programs via
#     LS_Program_Dist_BC (the CIP's share of the age group's not-in-school
#     BC population);
#   - Rest of Canada takes the nested product of its own two ratios (its
#     labour-supply share x its share of the Canada pool);
#   - every other region gets TOT_tot_sa_reg * LS_Program_Dist_BC -- the
#     region's not-in-school count scaled by BC's program distribution
#     (BC's mix used as the proxy, inherited from the original design).
doct_data <- sc_export_unfilt %>%
  filter(HCDD == "Earned doctorate")

# get Canada totals by age_group and cip
doct_canada <- doct_data %>%
  filter(REGION == "Canada") %>%
  select(age_group, major_field_cip, TOT_tot_sa_Canada = TOT_tot_sa)

# get total cip for BC total values
doct_bc_tot <- doct_data %>%
  filter(grepl("Total", major_field_cip)) %>%
  filter(REGION == "British Columbia") %>%
  select(age_group, TOT_tot_sa_bc = TOT_tot_sa)

# get total cip for each region
doct_all_tots <- doct_data %>%
  filter(grepl("Total", major_field_cip)) %>%
  select(REGION, age_group, TOT_tot_sa_reg = TOT_tot_sa)

# get BC data; run BC calculations
doct_bc <- doct_data %>%
  filter(REGION == "British Columbia") %>%
  left_join(doct_bc_tot, by = "age_group") %>%
  mutate(
    BC_LABOUR_SUPPLY = ifelse(
      (LF_U_dnas + LF_E_tot_sa) > TOT_tot_sa,
      TOT_tot_sa,
      (LF_U_dnas + LF_E_tot_sa)
    )
  ) %>%
  mutate(
    BC_NEW_LABOUR_SUPPLY_TEMP = ifelse(
      TOT_tot_sa == 0,
      0,
      BC_LABOUR_SUPPLY / TOT_tot_sa
    )
  )

doct_bc_tot_temp <- doct_bc %>%
  filter(grepl("Total", major_field_cip)) %>%
  select(age_group, TOT_BC_NEW_LS = BC_NEW_LABOUR_SUPPLY_TEMP)

# replace labour supply depending on outcomes
doct_bc <- doct_bc %>%
  left_join(doct_bc_tot_temp, by = "age_group") %>%
  mutate(
    BC_NEW_LABOUR_SUPPLY = case_when(
      (BC_NEW_LABOUR_SUPPLY_TEMP == 1 & BC_LABOUR_SUPPLY <= 30) ~ TOT_BC_NEW_LS,
      BC_LABOUR_SUPPLY == 0 ~ TOT_BC_NEW_LS,
      TRUE ~ BC_NEW_LABOUR_SUPPLY_TEMP
    )
  ) %>%
  mutate(
    LS_Program_Dist_BC = (BC_NEW_LABOUR_SUPPLY * TOT_tot_sa) / TOT_tot_sa_bc
  ) %>%
  select(
    age_group,
    major_field_cip,
    BC_NEW_LABOUR_SUPPLY,
    BC_NEW_LABOUR_SUPPLY_TEMP,
    BC_LABOUR_SUPPLY,
    TOT_tot_sa_bc,
    LS_Program_Dist_BC
  )

# run rest of canada calculations - differ from other regions
doct_rest_canada <- doct_data %>%
  filter(REGION == "qry_Rest_of_Canada") %>%
  left_join(doct_canada, by = c("age_group", "major_field_cip")) %>%
  mutate(
    REST_CAN_LABOUR_SUPPLY = ifelse(
      (LF_U_dnas + LF_E_tot_sa) > TOT_tot_sa,
      TOT_tot_sa,
      (LF_U_dnas + LF_E_tot_sa)
    )
  ) %>%
  mutate(
    REST_CAN_NEW_LABOUR_SUPPLY_TEMP = ifelse(
      TOT_tot_sa == 0,
      0,
      REST_CAN_LABOUR_SUPPLY / TOT_tot_sa
    )
  ) %>%
  mutate(
    REST_CAN_NEW_LABOUR_SUPPLY_TEMP2 = REST_CAN_LABOUR_SUPPLY *
      REST_CAN_NEW_LABOUR_SUPPLY_TEMP
  ) %>%
  mutate(
    REST_CAN_NEW_LABOUR_SUPPLY = ifelse(
      TOT_tot_sa_Canada == 0,
      0,
      REST_CAN_NEW_LABOUR_SUPPLY_TEMP2 / TOT_tot_sa_Canada
    )
  ) %>%
  select(
    REGION,
    age_group,
    major_field_cip,
    REST_CAN_LABOUR_SUPPLY,
    REST_CAN_NEW_LABOUR_SUPPLY_TEMP2,
    REST_CAN_NEW_LABOUR_SUPPLY,
    REST_CAN_NEW_LABOUR_SUPPLY_TEMP
  )

# append required values; run required calculations
doct_data_final <- doct_data %>%
  left_join(doct_canada, by = c("age_group", "major_field_cip")) %>%
  left_join(doct_bc, by = c("age_group", "major_field_cip")) %>%
  left_join(
    doct_rest_canada,
    by = c("REGION", "age_group", "major_field_cip")
  ) %>%
  left_join(doct_all_tots, by = c("REGION", "age_group")) %>%
  mutate(
    LABOUR_SUPPLY = case_when(
      REGION == "qry_Rest_of_Canada" ~ REST_CAN_LABOUR_SUPPLY,
      REGION == "British Columbia" ~ BC_LABOUR_SUPPLY,
      TRUE ~ TOT_tot_sa_reg * LS_Program_Dist_BC
    )
  ) %>%
  mutate(
    NEW_LABOUR_SUPPLY = case_when(
      REGION == "qry_Rest_of_Canada" ~ REST_CAN_NEW_LABOUR_SUPPLY,
      REGION == "British Columbia" ~ BC_NEW_LABOUR_SUPPLY,
      TRUE ~ (ifelse(
        TOT_tot_sa_Canada == 0,
        0,
        LABOUR_SUPPLY / TOT_tot_sa_Canada
      ))
    )
  ) %>%
  mutate(TOTAL = ifelse(LABOUR_SUPPLY == 0, 0, TOT_tot_sa_Canada)) %>%
  mutate(PSSM_CREDENTIAL = "DOCT", PSSM_CRED = PSSM_CREDENTIAL) %>%
  select(-contains(c("BC_", "REST_CAN_", "_bc", "_reg")))

# ---- Prepare a Stat_Can version of Labour_Supply_Distribution table ----
## Combine all datasets ----
# One credential-labelled dataset (GRCT/GRDP + PDEG + MAST + DOCT), then a
# round-trip through the personal schema so the lookup joins below can run
# database-side (tbl() + left_join, translated to SQL by dbplyr).
Combined_Stat_Can_Original <- grct_grdp_data %>%
  rbind(pdeg_data) %>%
  rbind(mast_data) %>%
  rbind(doct_data_final)

## Temporarily save to decimal ----
dbWriteTable(
  decimal_con,
  name = Id(schema = my_schema, table = "Combined_Labour_Supply_Stat_Can_Original_r"),
  value = Combined_Stat_Can_Original
)

## Add in lookups ----
# Map region names -> PSSM region-rollup codes and age labels -> age-rollup
# codes; rows that match neither lookup (geographies/ages the PSSM does not
# model) are dropped by the two !is.na filters.
# filter out unused regions and ages based on lookup tables
Combined_Stat_Can <- tbl(
  decimal_con,
  Id(schema = my_schema, table = "Combined_Labour_Supply_Stat_Can_Original_r")
) %>%
  left_join(
    tbl(decimal_con, Id(schema = my_schema, table = "t_current_region_pssm_rollup_codes_statcan_r")),
    by = c("REGION" = "CURRENT_REGION_PSSM_NAME_ROLLUP_STAT_CAN")
  ) %>%
  left_join(
    tbl(decimal_con, Id(schema = my_schema, table ="tbl_age_groups_rollup_r")),
    by = c("age_group" = "AGE_GROUP_ROLLUP_LABEL")
  ) %>%
  filter(!is.na(CURRENT_REGION_PSSM_CODE_ROLLUP)) %>%
  filter(!is.na(AGE_GROUP_ROLLUP)) %>%
  collect()

## Prepare final columns ----
# Drop the workbook's "Total" CIP rows; the PSSM keys on the 2-digit CIP
# prefix. NOTE: the column is named LCP4_CD for contract compatibility with
# 02b-2's output table, but it holds the TWO-digit prefix (census rows use
# 2-digit keys; consumers distinguish census rows by the Survey label).
# LCIP4_CRED follows the census row format "XX - CREDENTIAL".
Combined_Stat_Can <- Combined_Stat_Can %>%
  filter(!grepl("Total", major_field_cip)) %>%
  mutate(LCP4_CD = substr(major_field_cip, 1, 2)) %>%
  mutate(LCIP4_CRED = paste0(LCP4_CD, " - ", PSSM_CREDENTIAL)) %>%
  mutate(SURVEY = "2021 Census PSSM 2022-2023") %>%
  arrange(PSSM_CRED, AGE_GROUP_ROLLUP, CURRENT_REGION_PSSM_CODE_ROLLUP, LCP4_CD)

# select desired columns
# The exact column set/order 02b-2's append expects (02b-2 NA-pads the
# columns absent here: TTRAIN, LCIP2_CRED).
Labour_Supply_Distribution_Stat_Can <- Combined_Stat_Can %>%
  select(
    SURVEY,
    PSSM_CREDENTIAL,
    PSSM_CRED,
    LCP4_CD,
    LCIP4_CRED,
    CURRENT_REGION_PSSM_CODE_ROLLUP,
    AGE_GROUP_ROLLUP,
    COUNT = LABOUR_SUPPLY,
    TOTAL,
    NEW_LABOUR_SUPPLY
  )

## Save final table ----
# overwrite=TRUE so re-running the script is idempotent (same workbook in,
# same table out). 02b-2 reads this table with its guarded `_r`-preferring
# read and applies the model-year relabel at that point.
dbWriteTable(
  decimal_con,
  name = Id(schema = my_schema, table = "Labour_Supply_Distribution_Stat_Can_r"),
  Labour_Supply_Distribution_Stat_Can,
  overwrite = TRUE
)

# ---- Clean Up ----
## Drop intermediate tables ----
dbExecute(decimal_con, SQL(glue::glue("DROP TABLE [{my_schema}].[Combined_Labour_Supply_Stat_Can_Original_r]")))

## Drop lookups ----
dbExecute(decimal_con, SQL(glue::glue("DROP TABLE [{my_schema}].[t_current_region_pssm_rollup_codes_statcan_r]")))

## Disconnect ----
dbDisconnect(decimal_con)
