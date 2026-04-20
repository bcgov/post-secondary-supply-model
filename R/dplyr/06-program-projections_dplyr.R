# Program Projections — dplyr Translation
# Original: R/06-program-projections.R
#
# Pipeline context:
#   Creates static and projected program distributions from several sources:
#     1. PTIB — private institution distributions (from 05)
#     2. Near Completers — students close to graduation (from 03)
#     3. Main cohorts (STP + TTRAIN) — weighted credential/program distributions
#     4. Post-degree credentials (PDEG, MAST, DOCT) — using LCIPPC cluster codes
#     5. Apprenticeships (APPRAPPR, APPRCERT)
#   Each source produces Y1 (base year) distributions. Static and Projected tables
#   are built by combining Y1 data with Y2-Y12 expansions and Werner projections.
#
# Input tables:
#   - T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN — near completer data (from 03)
#   - tbl_Program_Projection_Input — STP credential counts (from load-program-projections)
#   - T_Cohorts_Recoded — unified cohort table (from 02b-1)
#   - T_PSSM_Projection_Cred_Grp — credential groupings
#   - T_Weights_STP — year-based weights
#   - tbl_Age_Groups / tbl_Age_Groups_Near_Completers — age group lookups
#   - INFOWARE_L_CIP_6DIGITS_CIP2016 — CIP taxonomy for LCIPPC mapping
#   - T_Cohort_Program_Distributions_Y2_to_Y12 — year expansion lookup
#   - T_APPR_Y2_to_Y10 — apprenticeship year expansion lookup
#
# Output:
#   - Cohort_Program_Distributions_Projected — projected distributions (Y1-Y12)
#   - Cohort_Program_Distributions_Static — static distributions (Y1-Y12)

library(tidyverse)
library(odbc)
library(config)
library(DBI)
library(glue)
library(assertthat)

# ---- Configure ----
db_config <- config::get("decimal")
lan <- config::get("lan")
my_schema <- config::get("myschema")

decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")

# Helper: reference a table in the user's schema
sch_tbl <- function(name) {
  tbl(decimal_con, dbplyr::in_schema(my_schema, name))
}

# Helper: produce grad status prefix for composite keys
grad_prefix <- function(status) {
  if_else(is.na(status), "", paste0(status, " - "))
}

# ---- Check required tables ----
required_tables <- c(
  "T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN",
  "tbl_Program_Projection_Input", "T_Cohorts_Recoded",
  "Cohort_Program_Distributions_Projected", "Cohort_Program_Distributions_Static",
  "INFOWARE_L_CIP_4DIGITS_CIP2016", "INFOWARE_L_CIP_6DIGITS_CIP2016",
  "T_PSSM_Projection_Cred_Grp", "T_Weights_STP",
  "tbl_Age_Groups_Near_Completers", "T_Cohort_Program_Distributions_Y2_to_Y12"
)
for (table_name in required_tables) {
  full_table_name <- SQL(glue::glue('"{my_schema}"."{table_name}"'))
  assert_that(
    dbExistsTable(decimal_con, full_table_name),
    msg = paste("Error:", table_name, "does not exist in schema", my_schema)
  )
}

# ---- Pull all source tables into R ----
near_comp_raw <- sch_tbl("T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN") %>%
  collect() |> rename_with(toupper)

prog_proj_input <- sch_tbl("tbl_Program_Projection_Input") %>%
  collect() |> rename_with(toupper)

cohorts_recoded <- sch_tbl("T_Cohorts_Recoded") %>%
  collect() |> rename_with(toupper)

proj_cred_grp <- sch_tbl("T_PSSM_Projection_Cred_Grp") %>%
  collect() |> rename_with(toupper)

weights_stp <- sch_tbl("T_Weights_STP") %>%
  collect() |> rename_with(toupper)

age_groups_nc <- sch_tbl("tbl_Age_Groups_Near_Completers") %>%
  collect() |> rename_with(toupper)

age_groups <- sch_tbl("tbl_Age_Groups") %>%
  collect() |> rename_with(toupper)

year_lookup_y2_y12 <- sch_tbl("T_Cohort_Program_Distributions_Y2_to_Y12") %>%
  collect() |> rename_with(toupper)

year_lookup_appr <- sch_tbl("T_APPR_Y2_to_Y10") %>%
  collect() |> rename_with(toupper)

cip6 <- sch_tbl("INFOWARE_L_CIP_6DIGITS_CIP2016") %>%
  collect() |> rename_with(toupper)

# LCIPPC recode: map 4-digit CIP codes to program cluster codes (PDEG/MAST/DOCT use this)
lcippc_recode <- cip6 %>%
  mutate(LCIP_LCIPPC_CD = if_else(LCIP_LCP4_CD == "9999", "99", LCIP_LCIPPC_CD)) %>%
  distinct(LCIP_LCP4_CD, LCIP_LCIPPC_CD)

# Accumulate Y1 data for each output table
projected_sections <- list()
static_sections <- list()


# ******************************************************************************
# Section 1: PTIB — Private institution distributions (conditional on ptib_run)
# WHY: PTIB distributions come pre-computed from 05-ptib-analysis. They go into
# both Projected and Static tables for Y1.
# ******************************************************************************
if (ptib_run == TRUE) {
  ptib_dist <- sch_tbl("qry_Private_Credentials_06d1_Cohort_Dist") %>%
    collect() |> rename_with(toupper) %>%
    rename(PSSM_CREDENTIAL = CREDENTIAL)

  projected_sections$ptib <- ptib_dist
  static_sections$ptib <- ptib_dist
  dbExecute(decimal_con, "DROP TABLE qry_Private_Credentials_06d1_Cohort_Dist")
}


# ******************************************************************************
# Section 2: Near Completers (qry_13a–13d)
# WHY: Near completers represent students close to graduating. Their distributions
# are aggregated from T_DACSO_Near_Completers data and go into both tables for Y1.
# In SQL this was 3 SELECT INTO + 2 INSERT INTO + 3 DROP TABLE = 8 ops.
# ******************************************************************************

# qry_13a: Aggregate near completers by program/credential/age/TTRAIN
near_comp_agg <- near_comp_raw %>%
  mutate(
    LCIP2_CRED = paste0(
      grad_prefix(COSC_GRAD_STATUS_LGDS_CD_GROUP),
      str_sub(LCP4_CD, 1, 2), " - ",
      as.character(TTRAIN), " - ", PSSM_CREDENTIAL
    )
  ) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD, COSC_GRAD_STATUS_LGDS_CD_GROUP,
           TTRAIN, LCIP4_CRED, LCIP2_CRED, AGE_GROUP) %>%
  summarise(COUNT = sum(NEAR_COMPLETERS_STP_CREDENTIALS), .groups = "drop")

# qry_13b–13c: Compute totals and distribution percentages
near_comp_dist <- near_comp_agg %>%
  inner_join(
    near_comp_agg %>%
      group_by(PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP) %>%
      summarise(TOTALS = sum(COUNT), .groups = "drop"),
    by = c("PSSM_CREDENTIAL", "PSSM_CRED", "AGE_GROUP")
  ) %>%
  mutate(PERCENT = if_else(TOTALS == 0, 0, as.numeric(COUNT) / TOTALS))

# qry_13d: Map age groups and format for output
near_comp_mapped <- near_comp_dist %>%
  inner_join(age_groups_nc,
             by = c("AGE_GROUP" = "AGE_GROUP_LABEL_NEAR_COMPLETER_PROJECTION")) %>%
  transmute(
    SURVEY = "Program_Projections_2023-2024_qry_13d",
    PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD,
    GRAD_STATUS = COSC_GRAD_STATUS_LGDS_CD_GROUP,
    TTRAIN = as.character(TTRAIN), LCIP4_CRED, LCIP2_CRED,
    AGE_GROUP = AGE_GROUP_LABEL_GRADUATE_PROJECTION,
    YEAR = "2023/2024", COUNT, TOTAL = TOTALS, PERCENT
  )

projected_sections$near_comp <- near_comp_mapped
static_sections$near_comp <- near_comp_mapped


# ******************************************************************************
# Section 3: Main Cohorts — STP credentials (Q012b–Q012e)
# WHY: The main body of graduates (non-apprenticeship, non-PDEG). STP data is
# weighted by year, then split by TTRAIN using T_Cohorts_Recoded distributions.
# Goes into Static only.
# In SQL this was 8 SELECT INTO + 1 INSERT INTO + 8 DROP TABLE = 17 ops.
# ******************************************************************************

# Q012b: Weighted cohort distribution from STP data
weight_cohort <- prog_proj_input %>%
  inner_join(
    weights_stp %>% filter(MODEL == "2023-2024"),
    by = c("PSI_AWARD_SCHOOL_YEAR_DELAYED" = "YEAR_CODE")
  ) %>%
  inner_join(
    proj_cred_grp %>% select(PSSM_PROJECTION_CREDENTIAL, PSSM_CREDENTIAL, COSC_GRAD_STATUS_LGDS_CD),
    by = c("PSI_CREDENTIAL_CATEGORY" = "PSSM_PROJECTION_CREDENTIAL")
  ) %>%
  filter(WEIGHT > 0) %>%
  filter(!PSSM_CREDENTIAL %in% c("APPRAPPR", "APPRCERT", "GRCT or GRDP", "PDEG", "MAST", "DOCT")) %>%
  mutate(
    PSSM_CRED = paste0(grad_prefix(COSC_GRAD_STATUS_LGDS_CD), PSSM_CREDENTIAL),
    LCP4_CD = FINAL_CIP_CODE_4,
    LCIP4_CRED = paste0(grad_prefix(COSC_GRAD_STATUS_LGDS_CD), FINAL_CIP_CODE_4, " - ", PSSM_CREDENTIAL),
    LCIP2_CRED = paste0(grad_prefix(COSC_GRAD_STATUS_LGDS_CD), str_sub(FINAL_CIP_CODE_4, 1, 2), " - ", PSSM_CREDENTIAL),
    WEIGHTED = COUNT * WEIGHT
  )

# Q012c: Aggregate weighted counts
weighted_dist <- weight_cohort %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD, COSC_GRAD_STATUS_LGDS_CD,
           LCIP4_CRED, LCIP2_CRED, AGEGROUP) %>%
  summarise(COUNT = sum(WEIGHTED), .groups = "drop")

# Q012c1–c2: TTRAIN split from T_Cohorts_Recoded
ttrain_agg <- cohorts_recoded %>%
  inner_join(age_groups %>% select(AGE_GROUP, AGE_GROUP_LABEL), by = "AGE_GROUP") %>%
  filter(GRAD_STATUS != "3", !is.na(TTRAIN), WEIGHT > 0) %>%
  mutate(PSSM_CRED = PSSM_CREDENTIAL) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD, GRAD_STATUS, TTRAIN,
           LCIP4_CRED, LCIP2_CRED, AGE_GROUP_LABEL, WEIGHT) %>%
  summarise(COUNTS = n(), .groups = "drop") %>%
  mutate(WEIGHTED = COUNTS * WEIGHT) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD, GRAD_STATUS, TTRAIN,
           LCIP4_CRED, LCIP2_CRED, AGE_GROUP_LABEL) %>%
  summarise(COUNT = sum(WEIGHTED), .groups = "drop")

# Q012c3–c4: Compute TTRAIN percentages
ttrain_pct <- ttrain_agg %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD, GRAD_STATUS, AGE_GROUP_LABEL) %>%
  mutate(TOTALS = sum(COUNT)) %>%
  ungroup() %>%
  mutate(PERCENT = if_else(TOTALS == 0, 0, COUNT / TOTALS)) %>%
  select(PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD, GRAD_STATUS, TTRAIN, AGE_GROUP_LABEL, PERCENT)

# Q012c5: Distribute STP counts by TTRAIN percentage
main_cohorts_final <- weighted_dist %>%
  left_join(
    ttrain_pct %>% rename(AGEGROUP = AGE_GROUP_LABEL),
    by = c("PSSM_CREDENTIAL", "PSSM_CRED", "LCP4_CD",
           "COSC_GRAD_STATUS_LGDS_CD" = "GRAD_STATUS", "AGEGROUP")
  ) %>%
  mutate(
    LCIP4_CRED = paste0(
      grad_prefix(COSC_GRAD_STATUS_LGDS_CD), LCP4_CD, " - ",
      if_else(is.na(TTRAIN), "", paste0(as.character(TTRAIN), " - ")), PSSM_CREDENTIAL
    ),
    LCIP2_CRED = paste0(
      grad_prefix(COSC_GRAD_STATUS_LGDS_CD), str_sub(LCP4_CD, 1, 2), " - ",
      if_else(is.na(TTRAIN), "", paste0(as.character(TTRAIN), " - ")), PSSM_CREDENTIAL
    ),
    COUNT_DISTRIBUTED = if_else(is.na(PERCENT), COUNT, COUNT * PERCENT)
  )

# Q012d–Q012e: Compute totals and format for Static output
main_cohort_totals <- weight_cohort %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, AGEGROUP) %>%
  summarise(TOTALS = sum(WEIGHTED), .groups = "drop")

static_sections$main_cohorts <- main_cohorts_final %>%
  inner_join(main_cohort_totals, by = c("PSSM_CRED", "AGEGROUP")) %>%
  transmute(
    SURVEY = "Program_Projections_2023-2024_Q012e",
    PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD,
    GRAD_STATUS = COSC_GRAD_STATUS_LGDS_CD, TTRAIN,
    LCIP4_CRED, LCIP2_CRED, AGE_GROUP = AGEGROUP,
    YEAR = "2023/2024", COUNT = COUNT_DISTRIBUTED, TOTAL = TOTALS,
    PERCENT = if_else(TOTALS == 0, 0, COUNT_DISTRIBUTED / TOTALS)
  )


# ******************************************************************************
# Section 4: PDEG / MAST / DOCT (Q013b–Q013e)
# WHY: Post-degree credentials use LCIPPC cluster codes instead of LCP4 because
# they map to broader program clusters. Goes into Static only.
# In SQL this was 4 SELECT INTO + 1 INSERT INTO + 3 DROP TABLE = 8 ops.
# ******************************************************************************

pdeg_weighted <- prog_proj_input %>%
  inner_join(
    weights_stp %>% filter(MODEL == "2023-2024"),
    by = c("PSI_AWARD_SCHOOL_YEAR_DELAYED" = "YEAR_CODE")
  ) %>%
  inner_join(
    proj_cred_grp %>% select(PSSM_PROJECTION_CREDENTIAL, PSSM_CREDENTIAL, COSC_GRAD_STATUS_LGDS_CD),
    by = c("PSI_CREDENTIAL_CATEGORY" = "PSSM_PROJECTION_CREDENTIAL")
  ) %>%
  inner_join(
    lcippc_recode, by = c("FINAL_CIP_CODE_4" = "LCIP_LCP4_CD")
  ) %>%
  filter(WEIGHT > 0, PSSM_CREDENTIAL %in% c("GRCT or GRDP", "PDEG", "MAST", "DOCT")) %>%
  mutate(
    PSSM_CRED = paste0(grad_prefix(COSC_GRAD_STATUS_LGDS_CD), PSSM_CREDENTIAL),
    LCIPPC_CRED = paste0(grad_prefix(COSC_GRAD_STATUS_LGDS_CD), LCIP_LCIPPC_CD, " - ", PSSM_CREDENTIAL),
    WEIGHTED = COUNT * WEIGHT
  )

# Aggregate and compute distribution
pdeg_dist <- pdeg_weighted %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, LCIP_LCIPPC_CD, LCIPPC_CRED, AGEGROUP) %>%
  summarise(COUNT = sum(WEIGHTED), .groups = "drop")

pdeg_totals <- pdeg_weighted %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, AGEGROUP) %>%
  summarise(TOTALS = sum(WEIGHTED), .groups = "drop")

static_sections$pdeg <- pdeg_dist %>%
  inner_join(pdeg_totals, by = c("PSSM_CREDENTIAL", "PSSM_CRED", "AGEGROUP")) %>%
  transmute(
    SURVEY = "Program_Projections_2023-2024_Q013e",
    PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD = LCIP_LCIPPC_CD,
    LCIP4_CRED = LCIPPC_CRED,
    GRAD_STATUS = NA_character_, TTRAIN = NA_character_,
    LCIP2_CRED = NA_character_,
    AGE_GROUP = AGEGROUP, YEAR = "2023/2024",
    COUNT, TOTAL = TOTALS,
    PERCENT = if_else(TOTALS == 0, 0, COUNT / TOTALS)
  )


# ******************************************************************************
# Section 5: Apprenticeships (Q014b–Q014e)
# WHY: Apprenticeship credentials (APPRAPPR, APPRCERT) are tracked separately.
# Goes into both Projected and Static for Y1.
# In SQL this was 3 SELECT INTO + 2 INSERT INTO + 3 DROP TABLE = 8 ops.
# ******************************************************************************

appr_weighted <- cohorts_recoded %>%
  inner_join(age_groups %>% select(AGE_GROUP, AGE_GROUP_LABEL), by = "AGE_GROUP") %>%
  filter(PSSM_CREDENTIAL %in% c("APPRAPPR", "APPRCERT"), WEIGHT > 0) %>%
  mutate(PSSM_CRED = PSSM_CREDENTIAL) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD, TTRAIN,
           LCIP4_CRED, LCIP2_CRED, AGE_GROUP_LABEL, WEIGHT) %>%
  summarise(COUNTS = n(), .groups = "drop") %>%
  mutate(WEIGHTED = COUNTS * WEIGHT)

appr_dist <- appr_weighted %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD, LCIP4_CRED, LCIP2_CRED, AGE_GROUP_LABEL) %>%
  summarise(COUNT = sum(WEIGHTED), .groups = "drop")

appr_totals <- appr_weighted %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP_LABEL) %>%
  summarise(TOTALS = sum(WEIGHTED), .groups = "drop")

appr_output <- appr_dist %>%
  inner_join(appr_totals, by = c("PSSM_CREDENTIAL", "PSSM_CRED", "AGE_GROUP_LABEL")) %>%
  transmute(
    SURVEY = "Program_Projections_2023-2024_Q014e",
    PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD,
    GRAD_STATUS = NA_character_, TTRAIN = NA_character_,
    LCIP4_CRED, LCIP2_CRED, AGE_GROUP = AGE_GROUP_LABEL,
    YEAR = "2023/2024", COUNT, TOTAL = TOTALS,
    PERCENT = if_else(TOTALS == 0, 0, COUNT / TOTALS)
  )

projected_sections$appr <- appr_output
static_sections$appr <- appr_output

# Q014f: Expand apprenticeship grads to Y2-Y10 in Graduate_Projections
appr_grad_expanded <- sch_tbl("Graduate_Projections") %>%
  collect() |> rename_with(toupper) %>%
  filter(SURVEY == "APPSO") %>%
  inner_join(year_lookup_appr, by = c("YEAR" = "Y1")) %>%
  select(SURVEY, PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP, YEAR = Y2_TO_Y10, GRADUATES)

dbWriteTable(decimal_con, "tmp_APPSO_Grads_Expanded", appr_grad_expanded, overwrite = TRUE)
dbExecute(decimal_con, "
  INSERT INTO Graduate_Projections (Survey, PSSM_Credential, PSSM_CRED, Age_Group, [Year], Graduates)
  SELECT Survey, PSSM_Credential, PSSM_CRED, Age_Group, [Year], Graduates
  FROM tmp_APPSO_Grads_Expanded;")
dbExecute(decimal_con, "DROP TABLE tmp_APPSO_Grads_Expanded")


# ******************************************************************************
# Section 6: Build Y1 data and expand to Y2-Y12
# WHY: Y1 distributions are the base year (2023/2024). They need to be expanded
# to Y2-Y12 for both Static and Projected tables using the year lookup.
# Static gets ALL surveys expanded; Projected gets APPR + near completers only.
# ******************************************************************************

# Combine all Y1 sections
static_y1 <- bind_rows(static_sections)
projected_y1 <- bind_rows(projected_sections)

# Q015e22: Expand ALL Static Y1 to Y2-Y12
static_expanded <- static_y1 %>%
  inner_join(year_lookup_y2_y12 %>% select(Y1, Y2_TO_Y10), by = c("YEAR" = "Y1")) %>%
  mutate(SURVEY = "Program_Projections_2023-2024_Q015e22", YEAR = Y2_TO_Y10) %>%
  select(-Y2_TO_Y10)

# Q015e21: Expand APPR + near completers for Projected
appr_nc_y1 <- projected_y1 %>%
  filter(PSSM_CRED %in% c("APPRAPPR", "APPRCERT") | grepl("^3 - ", PSSM_CRED))

projected_expanded <- appr_nc_y1 %>%
  inner_join(year_lookup_y2_y12 %>% select(Y1, Y2_TO_Y10), by = c("YEAR" = "Y1")) %>%
  mutate(SURVEY = "Program_Projections_2023-2024_Q015e21", YEAR = Y2_TO_Y10) %>%
  select(-Y2_TO_Y10)

# Final Static = Y1 + Y2-Y12
final_static <- bind_rows(static_y1, static_expanded)
dbWriteTable(decimal_con,
             SQL(glue::glue('"{my_schema}"."Cohort_Program_Distributions_Static"')),
             final_static, overwrite = TRUE)

# Write intermediate Projected (before Werner projections)
projected_before_werner <- bind_rows(projected_y1, projected_expanded)
dbWriteTable(decimal_con,
             SQL(glue::glue('"{my_schema}"."Cohort_Program_Distributions_Projected"')),
             projected_before_werner, overwrite = TRUE)


# ******************************************************************************
# Section 7: Werner Program — external projection model
# WHY: The Werner program is an external R script that produces projected graduate
# counts by CIP/credential/age for years Y1-Y12. It reads from a CSV input file
# and writes to a CSV output file.
# KEPT AS-IS: This section is already R-native (not SQL).
# ******************************************************************************
input_data <- prog_proj_input %>%
  select(-any_of("EXPR1")) %>%
  tidyr::complete(AGEGROUP, PSI_CREDENTIAL_CATEGORY, FINAL_CIP_CODE_4,
                  PSI_AWARD_SCHOOL_YEAR_DELAYED, fill = list(COUNT = 0)) %>%
  pivot_wider(names_from = "PSI_AWARD_SCHOOL_YEAR_DELAYED", values_from = "COUNT") %>%
  rename("CIP" = "FINAL_CIP_CODE_4", "AGE" = "AGEGROUP", "CRED" = "PSI_CREDENTIAL_CATEGORY") %>%
  select(CIP, CRED, AGE, 4:ncol(.)) %>%
  arrange(CIP, CRED, AGE)

write_csv(input_data, glue::glue("{lan}/development/csv/gh-source/tmp/06/input-data.csv"))

# Run Werner program
source(glue::glue("{lan}/development/R/program projections.R"))

output_data <- read_delim(glue::glue("{lan}/development/csv/gh-source/tmp/06/output.csv"),
                          delim = "\t", col_names = TRUE)
names(output_data) <- paste0(2023:(2023 + 11), "/", 2024:(2024 + 11))

T_Predict_CIP_CRED_AGE <- cbind(input_data, output_data)

T_Predict_CIP_CRED_AGE_Flipped <- T_Predict_CIP_CRED_AGE %>%
  pivot_longer(-c(CIP, CRED, AGE), names_to = "Year", values_to = "Count") %>%
  filter(Year %in% c("2023/2024", "2024/2025", "2025/2026", "2026/2027", "2027/2028",
                      "2028/2029", "2029/2030", "2030/2031", "2031/2032", "2032/2033",
                      "2033/2034", "2034/2035"))

# Diagnostic: check total projected counts by year
T_Predict_CIP_CRED_AGE_Flipped %>%
  group_by(Year) %>%
  summarise(SumOfCount = sum(Count))


# ******************************************************************************
# Section 8: Projected distributions — non-PDEG (qry_10a–10c)
# WHY: Applies the Werner projections to non-PDEG/MAST/DOCT credentials,
# computing program distribution percentages by CIP/credential/age/year.
# In SQL this was 2 SELECT INTO + 1 INSERT INTO + 2 DROP TABLE = 5 ops.
# ******************************************************************************

proj_non_pdeg <- T_Predict_CIP_CRED_AGE_Flipped %>%
  inner_join(
    proj_cred_grp %>% select(PSSM_PROJECTION_CREDENTIAL, PSSM_CREDENTIAL, COSC_GRAD_STATUS_LGDS_CD),
    by = c("CRED" = "PSSM_PROJECTION_CREDENTIAL")
  ) %>%
  filter(!PSSM_CREDENTIAL %in% c("APPRAPPR", "APPRCERT", "GRCT or GRDP", "PDEG", "MAST", "DOCT")) %>%
  mutate(
    PSSM_CRED = paste0(grad_prefix(COSC_GRAD_STATUS_LGDS_CD), PSSM_CREDENTIAL),
    LCIP4_CRED = paste0(grad_prefix(COSC_GRAD_STATUS_LGDS_CD), CIP, " - ", PSSM_CREDENTIAL),
    LCIP2_CRED = paste0(grad_prefix(COSC_GRAD_STATUS_LGDS_CD), str_sub(CIP, 1, 2), " - ", PSSM_CREDENTIAL)
  ) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, CIP, LCIP4_CRED, LCIP2_CRED, AGE, Year) %>%
  summarise(COUNT = sum(Count), .groups = "drop")

proj_non_pdeg_totals <- proj_non_pdeg %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, AGE, Year) %>%
  summarise(TOTALS = sum(COUNT), .groups = "drop")

proj_non_pdeg_output <- proj_non_pdeg %>%
  inner_join(proj_non_pdeg_totals, by = c("PSSM_CREDENTIAL", "PSSM_CRED", "AGE", "Year")) %>%
  transmute(
    SURVEY = "Program_Projections_2023-2024_qry10c",
    PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD = CIP,
    GRAD_STATUS = NA_character_, TTRAIN = NA_character_,
    LCIP4_CRED, LCIP2_CRED, AGE_GROUP = AGE,
    YEAR = Year, COUNT, TOTAL = TOTALS,
    PERCENT = if_else(TOTALS == 0, 0, COUNT / TOTALS)
  )


# ******************************************************************************
# Section 9: Projected distributions — PDEG / MAST / DOCT (qry_12a–12c)
# WHY: Same as Section 8 but for post-degree credentials using LCIPPC cluster codes.
# In SQL this was 2 SELECT INTO + 1 INSERT INTO + 2 DROP TABLE = 5 ops.
# ******************************************************************************

proj_pdeg <- T_Predict_CIP_CRED_AGE_Flipped %>%
  inner_join(
    proj_cred_grp %>% select(PSSM_PROJECTION_CREDENTIAL, PSSM_CREDENTIAL, COSC_GRAD_STATUS_LGDS_CD),
    by = c("CRED" = "PSSM_PROJECTION_CREDENTIAL")
  ) %>%
  inner_join(lcippc_recode, by = c("CIP" = "LCIP_LCP4_CD")) %>%
  filter(PSSM_CREDENTIAL %in% c("GRCT or GRDP", "PDEG", "MAST", "DOCT")) %>%
  mutate(
    PSSM_CRED = paste0(grad_prefix(COSC_GRAD_STATUS_LGDS_CD), PSSM_CREDENTIAL),
    LCIPPC_CRED = paste0(grad_prefix(COSC_GRAD_STATUS_LGDS_CD), LCIP_LCIPPC_CD, " - ", PSSM_CREDENTIAL)
  ) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, LCIP_LCIPPC_CD, LCIPPC_CRED, AGE, Year) %>%
  summarise(COUNT = sum(Count), .groups = "drop")

proj_pdeg_totals <- proj_pdeg %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, AGE, Year) %>%
  summarise(TOTALS = sum(COUNT), .groups = "drop")

proj_pdeg_output <- proj_pdeg %>%
  inner_join(proj_pdeg_totals, by = c("PSSM_CREDENTIAL", "PSSM_CRED", "AGE", "Year")) %>%
  transmute(
    SURVEY = "Program_Projections_2023-2024_qry12c",
    PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD = LCIP_LCIPPC_CD,
    GRAD_STATUS = NA_character_, TTRAIN = NA_character_,
    LCIP4_CRED = LCIPPC_CRED, LCIP2_CRED = NA_character_,
    AGE_GROUP = AGE, YEAR = Year, COUNT, TOTAL = TOTALS,
    PERCENT = if_else(TOTALS == 0, 0, COUNT / TOTALS)
  )


# ******************************************************************************
# Section 10: Build final Projected table
# Combine: Y1 (PTIB + near completers + apprenticeships) + Y2-Y12 expanded
#          + Werner non-PDEG projections + Werner PDEG projections
# ******************************************************************************

final_projected <- bind_rows(projected_before_werner, proj_non_pdeg_output, proj_pdeg_output)

dbWriteTable(decimal_con,
             SQL(glue::glue('"{my_schema}"."Cohort_Program_Distributions_Projected"')),
             final_projected, overwrite = TRUE)


# ******************************************************************************
# Section 11: Diagnostic — check for missing combinations (qry_12d)
# WHY: Validates that every combination in Static has a corresponding entry in
# Projected (for the same credential/CIP/age/year). Missing combinations may
# indicate data issues.
# ******************************************************************************
missing_combos <- final_static %>%
  filter(!AGE_GROUP %in% c("15 to 16", "65 to 89")) %>%
  select(PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD, LCIP4_CRED, AGE_GROUP, YEAR, COUNT) %>%
  anti_join(
    final_projected %>% select(PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD, AGE_GROUP, YEAR),
    by = c("PSSM_CREDENTIAL", "PSSM_CRED", "LCP4_CD", "AGE_GROUP", "YEAR")
  )
missing_combos


# ---- Clean Up: Drop lookup tables no longer needed ----
# KEPT AS SQL: DROP TABLE (cleanup of lookup tables loaded by earlier scripts)
dbExecute(decimal_con, "DROP TABLE AgeGroupLookup")
dbExecute(decimal_con, "DROP TABLE tbl_Age_Groups_Near_Completers")
dbExecute(decimal_con, "DROP TABLE tbl_Age_Groups")
dbExecute(decimal_con, "DROP TABLE T_Cohort_Program_Distributions_Y2_to_Y12")
dbExecute(decimal_con, "DROP TABLE T_APPR_Y2_to_Y10")
dbExecute(decimal_con, "DROP TABLE T_PSSM_Projection_Cred_Grp")
dbExecute(decimal_con, "DROP TABLE T_Weights_STP")

# Verify final tables exist
dbExistsTable(decimal_con, "Cohort_Program_Distributions_Projected")
dbExistsTable(decimal_con, "Cohort_Program_Distributions_Static")
dbExistsTable(decimal_con, "tbl_Program_Projection_Input")

dbDisconnect(decimal_con)
