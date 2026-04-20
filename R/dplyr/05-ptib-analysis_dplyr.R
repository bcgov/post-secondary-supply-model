# PTIB Analysis — dplyr Translation
# Original: R/05-ptib-analysis.R
#
# Pipeline context:
#   Processes Private Training Institutions Branch (PTIB) data for inclusion in the
#   PSSM model. PTIB credentials are private institution graduates that need to be
#   integrated with the public institution data processed by the main pipeline.
#
#   This script runs after the main graduate projections (04) and before the program
#   projections (06). It produces two outputs:
#     1. PTIB graduate data appended to Graduate_Projections (for 04)
#     2. PTIB cohort distributions for program projections (for 06)
#
# Input tables:
#   - T_Private_Institutions_Credentials_Raw — raw PTIB data (from load-ptib.R)
#   - T_PSSM_Credential_Grouping — maps PTIB credential names to PSSM categories
#   - INFOWARE_L_CIP_6DIGITS_CIP2016 — official CIP taxonomy for validation
#   - T_PTIB_Y1_to_Y10 — year lookup for projection year expansion
#
# Output:
#   - qry_Private_Credentials_05i1_Grads_by_Year — PTIB grads by year (kept in DB)
#   - qry_Private_Credentials_06d1_Cohort_Dist — PTIB cohort distributions (kept in DB)
#   - Rows appended to Graduate_Projections

library(RODBC)
library(arrow)
library(tidyverse)
library(dbplyr)
library(odbc)
library(RJDBC) ## loads DBI

# ---- Configure LAN and file paths ----
lan <- config::get("lan")
my_schema <- config::get("myschema")

# ---- Connection to database ----
db_config <- config::get("decimal")
decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")

# Helper: reference a table in the user's schema
sch_tbl <- function(name) {
  tbl(decimal_con, dbplyr::in_schema(my_schema, name))
}

# ---- Required data tables ----
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_PSSM_Credential_Grouping"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_Private_Institutions_Credentials_Raw"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_PTIB_Y1_to_Y10"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_6DIGITS_CIP2016"')))

# Pull all source tables into R for processing.
pssm_cred_grouping <- sch_tbl("T_PSSM_Credential_Grouping") %>%
  collect() |> rename_with(toupper)

ptib_raw <- sch_tbl("T_Private_Institutions_Credentials_Raw") %>%
  collect() |> rename_with(toupper)

ptib_y1_to_y10 <- sch_tbl("T_PTIB_Y1_to_Y10") %>%
  collect() |> rename_with(toupper)

cip6 <- sch_tbl("INFOWARE_L_CIP_6DIGITS_CIP2016") %>%
  select(LCIP_CD, LCIP_NAME) %>%
  collect() |> rename_with(toupper)

# ******************************************************************************
# Part 1: Clean PTIB data
# * Map PTIB credential names to PSSM credential categories
# * Clean CIP codes (remove periods, validate against INFOWARE)
# * Flag excluded programs (ESL, not-for-credit, unclassified)
# * Recode age groups
# * Compute 2-year averages of graduates/enrolments
# ******************************************************************************

# Join PTIB raw data with PSSM credential grouping to get standardized credential names.
# WHY: PTIB reports credential names that don't match the PSSM taxonomy. The grouping
# table maps each institution-specific name to a PSSM category (e.g., "Certificate" → "CERT").
# Records with no match (PSSM_Credential IS NULL) or "None" credentials are excluded.
# ---- qry_Private_Credentials_00a_Append ----
ptib_creds <- ptib_raw %>%
  inner_join(pssm_cred_grouping,
    by = c("CREDENTIAL" = "PRGM_CREDENTIAL_AWARDED_NAME")) %>%
  filter(!is.na(PSSM_CREDENTIAL), CREDENTIAL != "None") %>%
  transmute(
    INTYEAR = YEAR,
    CREDENTIAL = PSSM_CREDENTIAL,
    LCIP_CD = CIP,
    AGE_GROUP = AGE_GROUP,
    IMMIGRATION_STATUS = IMMIGRATION_STATUS,
    GRADUATES = SUM_OF_GRADUATES,
    ENROLLED_NOT_GRADUATED = SUM_OF_ENROLMENTS,
    ENROLMENT = SUM_OF_TOTAL_ENROLMENTS
  )

# ---- Check CIP length ----
# All CIP codes should be 7 characters (e.g., "11.0701"). This diagnostic identifies
# any that aren't, which could indicate data quality issues.
ptib_creds %>%
  filter(nchar(LCIP_CD) != 7) %>%
  select(LCIP_CD, CIP_LENGTH = nchar(LCIP_CD))

# ---- Remove periods from CIPs ----
# Standardize CIP codes by removing the period separator (e.g., "11.0701" → "110701").
ptib_creds <- ptib_creds %>%
  mutate(LCIP_CD = str_replace_all(LCIP_CD, fixed("."), ""))

# Sanity check: after removing periods, all CIPs should be 6 characters
ptib_creds %>%
  filter(nchar(LCIP_CD) != 6) %>%
  select(LCIP_CD)

# ---- Check CIPs against INFOWARE ----
# Find PTIB CIP codes that don't exist in the official INFOWARE taxonomy.
# These may need manual correction or exclusion.
ptib_creds %>%
  anti_join(cip6, by = "LCIP_CD") %>%
  distinct(LCIP_CD)

# ---- Update Exclude column ----
# Flag records that should be excluded from the model:
#   - English as a second language programs
#   - Not-for-credit programs
#   - Unclassified CIP code 99999
# We join with INFOWARE to get the program name for matching, then apply exclusion rules.
ptib_creds <- ptib_creds %>%
  left_join(cip6, by = "LCIP_CD") %>%
  mutate(EXCLUDE = if_else(
    LCIP_NAME == "English as a second language" |
    grepl("not for credit", LCIP_NAME) |
    LCIP_CD == "99999",
    "1", NA_character_
  )) %>%
  select(-LCIP_NAME)

# ---- Update age groups ----
# Replace dashes with " to " in age group labels for consistency with the
# standard PSSM age group format (e.g., "25-29" → "25 to 29").
ptib_creds <- ptib_creds %>%
  mutate(AGE_GROUP = str_replace_all(AGE_GROUP, "-", " to "))

# ---- Copy to Clean table ----
# Create a clean copy before computing averages. In the original, this was a separate
# database table; here it's just an R variable for clarity.
ptib_clean <- ptib_creds

# check relevant years to update queries below
ptib_creds %>% count(INTYEAR)

## !! update DATA years in below queries

# ---- qry_Private_Credentials_00g_Avg ----
# Compute 2-year averages of enrolments/graduates by credential/CIP/age/immigration.
# WHY: PTIB data can be volatile year-to-year. Averaging smooths out fluctuations.
# Only non-excluded records are included. The average label (e.g., 'Avg 2021 & 2022')
# must be updated for each model run to match the actual data years.
# NOTE: The /2 divisor assumes exactly 2 years of data. Update if this changes.
ptib_avg <- ptib_clean %>%
  filter(is.na(EXCLUDE)) %>%
  group_by(CREDENTIAL, LCIP_CD, AGE_GROUP, IMMIGRATION_STATUS) %>%
  summarise(
    ENROLMENT = sum(ENROLMENT) / 2,
    ENROLLED_NOT_GRADUATED = sum(ENROLLED_NOT_GRADUATED) / 2,
    GRADUATES = sum(GRADUATES) / 2,
    .groups = "drop"
  ) %>%
  mutate(
    INTYEAR = "Avg 2021 & 2022",
    EXCLUDE = NA_character_
  )

# Replace all data with the averaged data (original did INSERT + DELETE)
ptib_creds <- ptib_avg

# ******************************************************************************
# Part 2: Domestic graduates
#
# Estimates the number of domestic graduates from PTIB data. The challenge is that
# some records have blank or unknown immigration status. We handle this by:
#   1. Counting known domestic graduates (01a)
#   2. Counting all known graduates (domestic + international) (01b)
#   3. Computing the domestic percentage (01c)
#   4. Applying that percentage to blank/unknown records (01d)
#   5. Unioning domestic + estimated domestic, then summing (01e-01f)
#   6. Expanding to all projection years and appending to Graduate_Projections
# ******************************************************************************

## STOP !!! Update MODEL year in queries below ----

# Only CERT and DIPL credentials are included in the model (not apprenticeship, etc.)
ptib_grads <- ptib_creds %>%
  filter(
    is.na(EXCLUDE),
    !is.na(GRADUATES),
    CREDENTIAL %in% c("CERT", "DIPL")
  )

# ---- qry01a: Count domestic grads ----
# Sum graduates where immigration status is explicitly "Domestic".
domestic <- ptib_grads %>%
  filter(IMMIGRATION_STATUS == "Domestic") %>%
  group_by(CREDENTIAL, LCIP_CD, AGE_GROUP) %>%
  summarise(DOMESTIC = sum(GRADUATES), .groups = "drop")

# ---- qry01b: Count domestic and international grads ----
# Sum all graduates with known immigration status (Domestic, International, or #N/A).
dom_intl <- ptib_grads %>%
  filter(IMMIGRATION_STATUS %in% c("Domestic", "International", "#N/A")) %>%
  group_by(CREDENTIAL, LCIP_CD, AGE_GROUP) %>%
  summarise(DOMESTIC_INTERNATIONAL = sum(GRADUATES), .groups = "drop")

# ---- qry01c: Compute percent domestic ----
# Join domestic and total counts, then compute the domestic fraction.
# WHY: We need this percentage to estimate how many of the blank/unknown-status
# graduates are likely domestic.
pct_domestic <- domestic %>%
  left_join(dom_intl, by = c("CREDENTIAL", "LCIP_CD", "AGE_GROUP")) %>%
  mutate(PERCENT_DOMESTIC = if_else(DOMESTIC == 0, 0, DOMESTIC / DOMESTIC_INTERNATIONAL))

# ---- qry01d: Estimate domestic grads for blank/unknown immigration status ----
# Apply the domestic percentage to graduates with blank or unknown status.
# This distributes unknown-status graduates proportionally between domestic and international.
blank_grads <- ptib_grads %>%
  filter(IMMIGRATION_STATUS %in% c("(blank)", "Unknown")) %>%
  select(CREDENTIAL, LCIP_CD, AGE_GROUP, GRADUATES) %>%
  left_join(
    pct_domestic %>% select(CREDENTIAL, LCIP_CD, AGE_GROUP, PERCENT_DOMESTIC),
    by = c("CREDENTIAL", "LCIP_CD", "AGE_GROUP")
  ) %>%
  mutate(DOMESTIC = GRADUATES * PERCENT_DOMESTIC) %>%
  select(CREDENTIAL, LCIP_CD, AGE_GROUP, DOMESTIC)

# ---- qry01e-01f: Union domestic + estimated, then sum ----
# Combine known domestic graduates with estimated domestic from blank/unknown records,
# then sum across both sources by credential/CIP/age group.
qry01f <- bind_rows(domestic, blank_grads) %>%
  group_by(CREDENTIAL, LCIP_CD, AGE_GROUP) %>%
  summarise(GRADS = sum(DOMESTIC, na.rm = TRUE), .groups = "drop") %>%
  mutate(YEAR = "2023/2024")

# ---- qry05i: Summarize grads by credential/age (drop CIP) ----
# Aggregate across CIP codes to get total domestic grads per credential/age combination.
qry05i <- qry01f %>%
  group_by(YEAR, CREDENTIAL, AGE_GROUP) %>%
  summarise(SUMOFGRADS = sum(GRADS), .groups = "drop")

# ---- qry05i1: Expand to all projection years ----
# Join with the year lookup table to create rows for each projection year (Y1 through Y10).
# The T_PTIB_Y1_to_Y10 table maps the base year to each projection year.
qry05i1 <- qry05i %>%
  inner_join(ptib_y1_to_y10, by = c("YEAR" = "Y1")) %>%
  mutate(
    SURVEY = "PTIB",
    PSSM_CRED = paste0("P - ", CREDENTIAL)
  ) %>%
  select(SURVEY, PSSM_CRED, AGE_GROUP, YEAR = Y1_TO_Y10, GRADUATES = SUMOFGRADS)

# ---- qry05i2: Delete excess age groups ----
# Remove age groups that aren't used in the model: blank, unknown, 65+, and 16 or less.
excess_age_groups <- c("(blank)", "Unknown", "65+", "16 or less")

qry05i1 <- qry05i1 %>%
  filter(!AGE_GROUP %in% excess_age_groups)

# Write the PTIB graduate data to database (kept for downstream reference)
dbWriteTable(decimal_con, SQL(glue::glue('"{my_schema}"."qry_Private_Credentials_05i1_Grads_by_Year"')),
             qry05i1, overwrite = TRUE)

# ---- Update Graduate_Projections ----
# Append PTIB graduate data to the main Graduate_Projections table. This is an append
# (not overwrite) because the table already contains public institution data from step 04.
# KEPT AS SQL: INSERT INTO...SELECT (appending to existing table)
dbExecute(decimal_con, "
  INSERT INTO Graduate_Projections (Survey, PSSM_CRED, Age_Group, [Year], Graduates)
  SELECT Survey, PSSM_CRED, Age_Group, [Year], Graduates
  FROM qry_Private_Credentials_05i1_Grads_by_Year;")


# ******************************************************************************
# Part 3: Cohort distributions
#
# Computes the distribution of PTIB graduates across CIP programs (4-digit and 2-digit)
# for each credential/age group. This distribution is used by 06-program-projections
# to allocate projected PTIB graduates across programs.
# ******************************************************************************

# ---- qry06b: Count grads by CIP ----
# Aggregate domestic graduates by credential and CIP code, constructing the composite
# keys (LCP4_CD, LCIP4_CRED, LCIP2_CRED) used throughout the pipeline for program matching.
qry06b <- qry01f %>%
  mutate(
    PSSM_CRED = paste0("P - ", CREDENTIAL),
    LCP4_CD = str_sub(LCIP_CD, 1, 4),
    LCIP4_CRED = paste0("P - ", str_sub(LCIP_CD, 1, 4), " - ", CREDENTIAL),
    LCIP2_CRED = paste0("P - ", str_sub(LCIP_CD, 1, 2), " - ", CREDENTIAL)
  ) %>%
  group_by(YEAR, CREDENTIAL, PSSM_CRED, LCP4_CD, LCIP4_CRED, LCIP2_CRED, AGE_GROUP) %>%
  summarise(COUNT = sum(GRADS), .groups = "drop")

# ---- qry06c: Sum totals by age group ----
# Compute the total graduates per credential/age group. This is the denominator for
# computing the program distribution percentages.
qry06c <- qry06b %>%
  group_by(YEAR, CREDENTIAL, PSSM_CRED, AGE_GROUP) %>%
  summarise(TOTAL = sum(COUNT), .groups = "drop")

# ---- qry06d1: Compute program distribution percentages ----
# Join counts with totals to compute each CIP program's share of graduates.
# This table feeds into 06-program-projections to distribute projected graduates
# across programs using the same proportions observed in the PTIB data.
qry06d1 <- qry06b %>%
  inner_join(qry06c, by = c("YEAR", "CREDENTIAL", "PSSM_CRED", "AGE_GROUP")) %>%
  mutate(
    SURVEY = "PTIB",
    PERCENT = if_else(TOTAL == 0, 0, COUNT / TOTAL)
  ) %>%
  select(SURVEY, CREDENTIAL, PSSM_CRED, LCP4_CD, LCIP4_CRED, LCIP2_CRED,
         AGE_GROUP, YEAR, COUNT, TOTAL, PERCENT) %>%
  filter(!AGE_GROUP %in% excess_age_groups)

dbWriteTable(decimal_con, SQL(glue::glue('"{my_schema}"."qry_Private_Credentials_06d1_Cohort_Dist"')),
             qry06d1, overwrite = TRUE)


# ---- Clean up ----

# Drop lookup tables that are no longer needed after PTIB processing.
# KEPT AS SQL: DROP TABLE (cleanup of tables loaded by earlier scripts)
dbExecute(decimal_con, "DROP TABLE T_PSSM_Credential_Grouping")
dbExecute(decimal_con, "DROP TABLE T_PTIB_Y1_to_Y10")

## ---- disconnect ----
dbDisconnect(decimal_con)
# rm(list=ls())
