# Historic Cohort Program Distribution — dplyr Translation
# Original: R/06-historic-cohort-program-distribution.R
#
# Pipeline context:
#   Builds a historic cohort program distribution table by combining data from 5 sources:
#     1. PTIB — private institution credentials (from 05-ptib-analysis)
#     2. Near Completers — students close to graduation (from 03-near-completers-ttrain)
#     3. Main cohorts (STP + TTRAIN) — weighted credential/program distributions
#     4. Post-degree credentials (PDEG, MAST, DOCT)
#     5. Apprenticeships (APPRAPPR, APPRCERT)
#   Each source is processed independently then combined via bind_rows.
#   The combined table feeds into 06-program-projections for distributing projected
#   graduates across CIP programs.
#
# Input tables:
#   - qry_Private_Credentials_06d1_Cohort_Dist — PTIB distributions (from 05)
#   - T_DACSO_Near_Completers_*_history — near completer ratios (from 03)
#   - T_PSSM_Projection_Cred_Grp — credential groupings for projections
#   - tbl_Program_Projection_Input — STP credential counts (from load-program-projections)
#   - T_Weights_STP — year-based weights for STP data
#   - T_Cohorts_Recoded — unified cohort table (from 02b-1)
#   - tbl_Age_Groups / tbl_Age_Groups_Near_Completers — age group lookups
#   - qry_12_LCP4_LCIPPC_Recode_9999 — CIP cluster mapping for PDEG
#
# Output:
#   - Cohort_Program_Distributions_history — combined historic distributions

library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(dbplyr)

# ---- Configure LAN and file paths ----
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

# Helper: produce grad status prefix for composite keys.
# WHY: Many composite keys (PSSM_CRED, LCIP4_CRED, LCIP2_CRED) conditionally include
# the grad status code with a " - " separator. If grad status is NULL, the prefix
# is empty; otherwise it's "status - ". SQL uses CONCAT with CASE WHEN to handle this.
grad_prefix <- function(status) {
  if_else(is.na(status), "", paste0(status, " - "))
}


# ******************************************************************************
# survey == 'PTIB' — Private institution credentials
# ******************************************************************************
# PTIB distributions are already computed by 05-ptib-analysis. Just reshape.
ptib <- sch_tbl("qry_Private_Credentials_06d1_Cohort_Dist") %>%
  collect() |> rename_with(toupper) %>%
  rename(PSSM_CREDENTIAL = CREDENTIAL) %>%
  mutate(YEAR = 2023) %>%
  select(-YEAR.y, YEAR = YEAR.x)

# Fix: handle the column rename properly after toupper
# The original table has columns: SURVEY, Credential, PSSM_CRED, LCP4_CD, etc.
# After toupper: CREDENTIAL → rename to PSSM_CREDENTIAL
# YEAR column from mutate needs careful handling since original may also have YEAR
ptib <- sch_tbl("qry_Private_Credentials_06d1_Cohort_Dist") %>%
  collect() |> rename_with(toupper) %>%
  rename(PSSM_CREDENTIAL = CREDENTIAL) %>%
  mutate(YEAR = 2023) %>%
  select(-any_of("YEAR")) %>%
  # Re-add YEAR since we just removed both
  mutate(YEAR = 2023)


# ******************************************************************************
# survey == 'Program_Projections_2023-2024_qry_13d' — Near Completers
# ******************************************************************************
# Copy near-completer ratios table from source schema (cross-schema copy).
# KEPT AS SQL: SELECT INTO across schemas
dbExecute(decimal_con, glue::glue(
  "SELECT * INTO [{my_schema}].[T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history] ",
  "FROM T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history;"
))

# Pull the near-completer history data and compute distributions.
# WHY: Near completers represent students who were close to graduating but didn't
# complete. Their distribution across CIP programs informs the model about potential
# future graduates. We sum counts by program/credential/age, compute totals and
# percentages, and map to the standard age group format.
near_comp_history <- sch_tbl("T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history") %>%
  collect() |> rename_with(toupper)

age_groups_nc <- sch_tbl("tbl_Age_Groups_Near_Completers") %>%
  collect() |> rename_with(toupper)

near_completers <- near_comp_history %>%
  mutate(
    LCIP2_CRED = paste0(
      grad_prefix(COSC_GRAD_STATUS_LGDS_CD_GROUP),
      str_sub(LCP4_CD, 1, 2), " - ",
      as.character(TTRAIN), " - ",
      PSSM_CREDENTIAL
    )
  ) %>%
  group_by(COCI_SUBM_CD, PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD,
           COSC_GRAD_STATUS_LGDS_CD_GROUP, TTRAIN, LCIP4_CRED, LCIP2_CRED,
           AGE_GROUP) %>%
  summarise(COUNT = sum(NEAR_COMPLETERS_STP_CREDENTIALS), .groups = "drop") %>%
  rename(YEAR = COCI_SUBM_CD, GRAD_STATUS = COSC_GRAD_STATUS_LGDS_CD_GROUP) %>%
  group_by(YEAR, PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP) %>%
  mutate(TOTAL = sum(COUNT)) %>%
  ungroup() %>%
  inner_join(age_groups_nc,
    by = c("AGE_GROUP" = "AGE_GROUP_LABEL_NEAR_COMPLETER_PROJECTION")) %>%
  mutate(
    SURVEY = "Program_Projections_2023-2024_qry_13d",
    AGE_GROUP = AGE_GROUP_LABEL_GRADUATE_PROJECTION,
    YEAR = as.numeric(paste0("20", str_sub(YEAR, start = -2))),
    PERCENT = ifelse(TOTAL == 0, 0, COUNT / TOTAL)
  ) %>%
  select(SURVEY, PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD, LCIP4_CRED, LCIP2_CRED,
         AGE_GROUP, YEAR, COUNT, TOTAL, PERCENT)


# ******************************************************************************
# survey = 'Program_Projections_2023-2024_Q012e' — Main cohorts (STP + TTRAIN)
# ADCT or ADIP, ADGR or UT, BACH, CERT, DIPL — excludes apprenticeships, PDEG, MAST, DOCT
# ******************************************************************************/

# Pull all source tables needed for both STP and PDEG sections.
proj_cred_grp <- sch_tbl("T_PSSM_Projection_Cred_Grp") %>%
  collect() |> rename_with(toupper)

prog_proj_input <- sch_tbl("tbl_Program_Projection_Input") %>%
  collect() |> rename_with(toupper)

weights_stp <- sch_tbl("T_Weights_STP") %>%
  collect() |> rename_with(toupper)

# ---- Part 1: STP data ----
# Weighted credential/program counts from STP data (non-apprenticeship, non-PDEG).
# WHY: This is the main body of graduates. Weights adjust for year-to-year variation.
main_cohorts_stp <- prog_proj_input %>%
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
    YEAR = PSI_AWARD_SCHOOL_YEAR_DELAYED,
    GRAD_STATUS = COSC_GRAD_STATUS_LGDS_CD,
    LCP4_CD = FINAL_CIP_CODE_4,
    PSSM_CRED = paste0(grad_prefix(GRAD_STATUS), PSSM_CREDENTIAL),
    LCIP4_CRED = paste0(grad_prefix(GRAD_STATUS), LCP4_CD, " - ", PSSM_CREDENTIAL),
    LCIP2_CRED = paste0(grad_prefix(GRAD_STATUS), str_sub(LCP4_CD, 1, 2), " - ", PSSM_CREDENTIAL)
  ) %>%
  group_by(YEAR, PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD, GRAD_STATUS,
           LCIP4_CRED, LCIP2_CRED, AGE_GROUP, WEIGHT) %>%
  summarise(COUNTS = sum(COUNT), .groups = "drop") %>%
  mutate(WEIGHTED = COUNTS * WEIGHT) %>%
  group_by(YEAR, PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP) %>%
  mutate(TOTAL = sum(WEIGHTED)) %>%
  ungroup() %>%
  mutate(
    YEAR = as.numeric(str_sub(YEAR, 6)),
    PERCENT = ifelse(TOTAL == 0, 0, WEIGHTED / TOTAL)
  )

# ---- Part 2: TTRAIN split ----
# Weighted counts from T_Cohorts_Recoded, split by TTRAIN flag (trades training).
# WHY: Trades training (TTRAIN) is a separate dimension in the model. This data
# allows the STP distributions to be further split by TTRAIN status.
cohorts_recoded <- sch_tbl("T_Cohorts_Recoded") %>%
  collect() |> rename_with(toupper)

age_groups <- sch_tbl("tbl_Age_Groups") %>%
  collect() |> rename_with(toupper)

main_cohorts_TTRAIN <- cohorts_recoded %>%
  inner_join(
    age_groups %>% select(AGE_GROUP, AGE_GROUP_LABEL),
    by = "AGE_GROUP"
  ) %>%
  filter(GRAD_STATUS != "3") %>%
  mutate(
    YEAR = SURVEY_YEAR,
    PSSM_CRED = PSSM_CREDENTIAL,
    LCIP4_CRED = paste0(
      grad_prefix(GRAD_STATUS),
      LCP4_CD, " - ",
      if_else(is.na(TTRAIN), "", paste0(as.character(TTRAIN), " - ")),
      PSSM_CREDENTIAL
    ),
    LCIP2_CRED = paste0(
      grad_prefix(GRAD_STATUS),
      str_sub(LCP4_CD, 1, 2), " - ",
      if_else(is.na(TTRAIN), "", paste0(as.character(TTRAIN), " - ")),
      PSSM_CREDENTIAL
    )
  ) %>%
  group_by(YEAR, PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD, GRAD_STATUS,
           TTRAIN, LCIP4_CRED, LCIP2_CRED, AGE_GROUP_LABEL, WEIGHT) %>%
  summarise(COUNTS = n(), .groups = "drop") %>%
  rename(AGE_GROUP = AGE_GROUP_LABEL) %>%
  mutate(WEIGHTED = COUNTS * WEIGHT) %>%
  filter(!is.na(TTRAIN) & WEIGHT > 0) %>%
  group_by(YEAR, PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD, GRAD_STATUS, AGE_GROUP) %>%
  mutate(TOTAL = sum(WEIGHTED)) %>%
  ungroup() %>%
  mutate(PERCENT = ifelse(TOTAL == 0, 0, WEIGHTED / TOTAL))

# ---- Combine STP + TTRAIN ----
# WHY: The STP data provides base counts, and TTRAIN splits them into trades/non-trades.
# We multiply STP weighted counts by the TTRAIN percentage to get the final distribution.
main_cohorts <- main_cohorts_stp %>%
  left_join(
    main_cohorts_TTRAIN %>% select(-PSSM_CRED),
    by = c("YEAR", "PSSM_CREDENTIAL", "LCP4_CD", "GRAD_STATUS", "AGE_GROUP"),
    suffix = c("_STP", "_TTRAIN")
  ) %>%
  mutate(
    SURVEY = "Program_Projections_2023-2024_Q012e",
    LCIP4_CRED = ifelse(is.na(LCIP4_CRED_TTRAIN), LCIP4_CRED_STP, LCIP4_CRED_TTRAIN),
    LCIP2_CRED = ifelse(is.na(LCIP2_CRED_TTRAIN), LCIP2_CRED_STP, LCIP2_CRED_TTRAIN),
    COUNT = ifelse(is.na(PERCENT_TTRAIN), WEIGHTED_STP, WEIGHTED_STP * PERCENT_TTRAIN),
    TOTAL = TOTAL_STP,
    PERCENT = COUNT / TOTAL
  ) %>%
  select(SURVEY, PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD, GRAD_STATUS, TTRAIN,
         LCIP4_CRED, LCIP2_CRED, AGE_GROUP, YEAR, COUNT, TOTAL, PERCENT)


# ******************************************************************************
# survey = 'Program_Projections_2023-2024_Q013e' — Post-degree (PDEG, MAST, DOCT)
# ******************************************************************************
# Weighted credential counts for post-degree credentials using cluster-level CIP codes.
# WHY: Post-degree credentials (masters, doctorates, graduate certificates) use a
# different CIP aggregation level (LCIPPC instead of LCP4) because they map to
# broader program clusters.
lcppc_recode <- sch_tbl("qry_12_LCP4_LCIPPC_Recode_9999") %>%
  collect() |> rename_with(toupper)

pdeg <- prog_proj_input %>%
  inner_join(
    weights_stp %>% filter(MODEL == "2023-2024"),
    by = c("PSI_AWARD_SCHOOL_YEAR_DELAYED" = "YEAR_CODE")
  ) %>%
  inner_join(
    proj_cred_grp %>% select(PSSM_PROJECTION_CREDENTIAL, PSSM_CREDENTIAL, COSC_GRAD_STATUS_LGDS_CD),
    by = c("PSI_CREDENTIAL_CATEGORY" = "PSSM_PROJECTION_CREDENTIAL")
  ) %>%
  inner_join(
    lcppc_recode %>% select(LCIP_LCP4_CD, LCIP_LCIPPC_CD),
    by = c("FINAL_CIP_CODE_4" = "LCIP_LCP4_CD")
  ) %>%
  filter(WEIGHT > 0) %>%
  filter(PSSM_CREDENTIAL %in% c("GRCT or GRDP", "PDEG", "MAST", "DOCT")) %>%
  mutate(
    YEAR = PSI_AWARD_SCHOOL_YEAR_DELAYED,
    GRAD_STATUS = COSC_GRAD_STATUS_LGDS_CD,
    LCP4_CD = LCIP_LCIPPC_CD,
    PSSM_CRED = paste0(grad_prefix(GRAD_STATUS), PSSM_CREDENTIAL),
    LCIP4_CRED = paste0(grad_prefix(GRAD_STATUS), LCP4_CD, " - ", PSSM_CREDENTIAL)
  ) %>%
  group_by(YEAR, PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD, LCIP4_CRED, AGE_GROUP, WEIGHT) %>%
  summarise(COUNTS = sum(COUNT), .groups = "drop") %>%
  mutate(WEIGHTED = COUNTS * WEIGHT) %>%
  group_by(YEAR, PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP) %>%
  mutate(TOTAL = sum(WEIGHTED)) %>%
  ungroup() %>%
  mutate(
    SURVEY = "Program_Projections_2023-2024_Q013e",
    YEAR = as.numeric(str_sub(YEAR, 6)),
    PERCENT = ifelse(TOTAL == 0, 0, WEIGHTED / TOTAL)
  ) %>%
  select(SURVEY, PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD, LCIP4_CRED,
         AGE_GROUP, YEAR, COUNT = WEIGHTED, TOTAL, PERCENT)


# ******************************************************************************
# survey = 'Program_Projections_2023-2024_Q014e' — Apprenticeships
# ******************************************************************************
# Weighted counts for apprenticeship credentials from T_Cohorts_Recoded.
# WHY: Apprenticeships (APPRAPPR, APPRCERT) are tracked separately from the main
# STP cohorts because they have a distinct credential pathway.
appso <- cohorts_recoded %>%
  inner_join(
    age_groups %>% select(AGE_GROUP, AGE_GROUP_LABEL),
    by = "AGE_GROUP"
  ) %>%
  filter(PSSM_CREDENTIAL %in% c("APPRAPPR", "APPRCERT")) %>%
  filter(WEIGHT > 0) %>%
  mutate(
    YEAR = SURVEY_YEAR,
    PSSM_CRED = PSSM_CREDENTIAL
  ) %>%
  group_by(YEAR, PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD, TTRAIN,
           LCIP4_CRED, LCIP2_CRED, AGE_GROUP_LABEL, WEIGHT) %>%
  summarise(COUNT = n(), .groups = "drop") %>%
  rename(AGE_GROUP = AGE_GROUP_LABEL) %>%
  mutate(WEIGHTED = COUNT * WEIGHT) %>%
  group_by(YEAR, PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP) %>%
  mutate(TOTAL = sum(WEIGHTED)) %>%
  ungroup() %>%
  mutate(
    SURVEY = "Program_Projections_2023-2024_Q014e",
    PERCENT = ifelse(TOTAL == 0, 0, WEIGHTED / TOTAL)
  ) %>%
  select(SURVEY, PSSM_CREDENTIAL, PSSM_CRED, LCP4_CD, LCIP4_CRED, LCIP2_CRED,
         AGE_GROUP, YEAR, COUNT, TOTAL, PERCENT)


# ******************************************************************************
# Combine all sources into historic distributions
# ******************************************************************************
Cohort_Program_Distributions_history <-
  bind_rows(ptib, near_completers, main_cohorts, pdeg, appso)

dbWriteTable(decimal_con,
             SQL(glue::glue('"{my_schema}"."Cohort_Program_Distributions_history"')),
             Cohort_Program_Distributions_history)

dbDisconnect(decimal_con)
