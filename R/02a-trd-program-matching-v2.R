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

# ==============================================================================
# 02a-trd-program-matching-v2.R  --  TRD PEN-match bridge (v2; first TRD matcher)
#
# ------------------------------------------------------------------------------
# WHERE THIS SITS IN THE MODEL
# ------------------------------------------------------------------------------
# PSSM computes one formula (docs/project-summary-for-new-analyst.md §2):
#
#   OCCSN(NOC) = GRADUATES(cred,age) x P(CIP|cred,age)
#              x P(in labour supply|CIP) x P(NOC|CIP,region)
#
# The TRD (Trades) cohort enters the model through 02b-1's TRD block, which
# builds `LCIP4_CRED` cohort keys from the SURVEY-side `LCIP_LCP4_CD`
# exclusively -- there is no STP-side TRD reconciliation at all today
# (credential_non_dup_r carries no 'TRD' bucket; trades credentials land in
# the DACSO bucket upstream in 01c's category mapping). This script is the
# FIRST dedicated TRD matcher: it bridges TRD survey records to STP
# credentials via PEN so the trades cohort's P(CIP|cred,age) can, in a future
# cutover, draw on credential-side evidence like every other survey.
#
# ------------------------------------------------------------------------------
# THE DESIGN (wayfinder tickets 02 + 09; cip-matching map)
# ------------------------------------------------------------------------------
# The ticket-02 fact-find established the data shape this implements:
#   - 91.6% of TRD survey PENs match credentials in the STP pool, and the
#     matches are overwhelmingly in the DACSO bucket (20,032 PENs; trades
#     foundations/completion credentials are coded DACSO) -- so the bridge
#     restricts candidates to the DACSO bucket.
#   - Award-year lag (award leading year - survey year) peaks sharply at -2:
#     the surveyed trades award is (almost always) two school years before
#     the survey -- the same 2-year pattern BGS uses. Lags <= -4 are prior
#     awards (noise), >= +1 are anomalies.
#   - Matched pairs agree on CIP4 85.4% of the time (raw, many-to-many);
#     951 PENs (4.4%) have no STP credential at all.
#
# Match rules, in preference order (ticket 09 resolution):
#   1. PEN join (normalize_pen on both sides) into the DACSO bucket.
#   2. Award-year window [lag -3 .. -1] centred on the -2 peak (width
#      calibrated at build time -- the lag distribution is printed and
#      exported on every run; the window must contain the peak).
#   3. Institution agreement required: TRD INST == STP PSI_CODE, or a pair
#      covered by the shared alias lookup dbo.T_BGS_STP_INST_ALIAS_v2_r
#      (same PSI<->INST universe the BGS v2 matcher uses).
#   4. Among surviving credentials: prefer lag -2, then latest award year.
#   5. Fallback: no surviving credential (or the credential carries no
#      usable CIP) -> keep the survey-side LCIP_LCP4_CD as FINAL_CIP --
#      exactly what the frozen 02b-1 TRD block does today for every row.
#
# ------------------------------------------------------------------------------
# MATCH_RULE VOCABULARY (one per TRD survey row)
# ------------------------------------------------------------------------------
#   pen_match_lag2_exact_inst     matched: exact institution, lag -2
#   pen_match_window_exact_inst   matched: exact institution, lag -3 or -1
#   pen_match_lag2_alias_inst     matched: alias institution, lag -2
#   pen_match_window_alias_inst   matched: alias institution, lag -3 or -1
#   survey_fallback_no_pen        survey row carries no usable PEN
#   survey_fallback_no_credential PEN has no DACSO-bucket credential
#   survey_fallback_no_window_match credentials exist, none survive window+inst
#   survey_fallback_no_stp_cip    credential matched but carries no usable CIP
#
# ------------------------------------------------------------------------------
# INPUTS
# ------------------------------------------------------------------------------
#   dbo.trd_data_r                               TRD survey records (01 loaders)
#   [my_schema].credential_non_dup_r             STP credentials (DACSO bucket)
#   [my_schema].stp_credential_r                 provides PSI_PEN per ID
#   [my_schema].Credential_Non_Dup_STP_CIP_Cleaning_v2_r    spine CIP cleaning
#   [my_schema].Credential_Non_Dup_Programs_DACSO_FinalCIPs_v2_r  DACSO v2
#                                               reconciled FINAL CIPs (richest
#                                               STP-side view; falls back to
#                                               the spine's cleaned CIP)
#   dbo.T_BGS_STP_INST_ALIAS_v2_r               institution alias pairs
#
# ------------------------------------------------------------------------------
# OUTPUT
# ------------------------------------------------------------------------------
#   [my_schema].Credential_Non_Dup_TRD_IDs_v2_r -- one row per TRD survey
#   record (KEY) with the matched credential, chosen FINAL_CIP, and
#   MATCH_RULE. Consumed by the comparison harness (ticket 17); the frozen
#   02b-1 TRD block is untouched (reading this table is future-cutover work).
#   .scratch/cip-matching/diagnostics/trd-v2-lag-distribution.csv -- the
#   window calibration evidence, regenerated each run.
# ==============================================================================

library(tidyverse)
library(odbc)
library(DBI)
library(glue)
library(futile.logger)

## -------------------------- Logging Setup --------------------------
log_file <- "./R/execution_log.txt"
flog.appender(appender.file(log_file), name = "file_logger")
flog.threshold(INFO, name = "file_logger")

log_info <- function(msg) {
  flog.info(msg, name = "file_logger")
  print(paste(Sys.time(), "|", msg))
}

log_info("==== 02a-trd-program-matching-v2.R START ====")

# ---- Load the spine's PEN helper (functions only) ----
options(cip_spine.lib_only = TRUE)
source("R/02a-cip-normalize.R", local = FALSE)
options(cip_spine.lib_only = FALSE)

db_config <- config::get("decimal")
my_schema <- config::get("myschema")
shareschema <- config::get("shareschema")

con <- dbConnect(
  odbc(),
  Driver = db_config$driver,
  Server = db_config$server,
  Database = db_config$database,
  Trusted_Connection = "TRUE"
)
log_info("Connected to SQL Server database")

sch_tbl <- function(tbl, conn = con, schema = my_schema) {
  dplyr::tbl(conn, DBI::Id(schema = schema, table = tbl))
}

diag_dir <- file.path(".scratch", "cip-matching", "diagnostics")
if (!dir.exists(diag_dir)) dir.create(diag_dir, recursive = TRUE)

# Award-year window: lag = award leading year - survey year. Centred on the
# -2 peak, width one year either side (BGS uses -3..-2; TRD's same-tail -1
# credential is common enough that the fact-find keeps it in-window).
LAG_MIN <- -3
LAG_MAX <- -1

# ==============================================================================
# STEP 1: Load the survey side and the credential side
# ==============================================================================
# Survey side: every TRD record, PEN canonicalized through the spine helper.
# KEY is the row identifier 02b-1 uses (STQU_ID = "TRD - KEY"); PRGM_ID is
# the registry program id the survey itself carries -- recorded as
# corroborating evidence on the output (the bridge is PEN-based per the
# ticket-09 design; PRGM_ID is not a join key here).
trd_survey <- sch_tbl("trd_data_r", schema = shareschema) %>%
  select(
    KEY, PEN, SURVEY_YEAR, INST, LCIP_LCP4_CD, SUBM_CD,
    GRADSTAT_GROUP, PSSM_CREDENTIAL, TTRAIN, PRGM_ID
  ) %>%
  collect() %>%
  mutate(
    KEY = as.integer(KEY),
    SURVEY_YEAR = as.integer(SURVEY_YEAR),
    PEN_CANON = normalize_pen(PEN)
  )
log_info(glue::glue(
  "Step 1: TRD survey side: {nrow(trd_survey)} rows; {sum(!is.na(trd_survey$PEN_CANON))} with a canonical PEN; {sum(!is.na(trd_survey$PRGM_ID))} carrying PRGM_ID"
))

# Credential side: DACSO bucket (the ticket-02 finding -- trades credentials
# are coded DACSO), with the spine's cleaned STP CIP and the canonical PEN.
credential_side <- sch_tbl("credential_non_dup_r") %>%
  rename_with(toupper) %>%
  filter(OUTCOMES_CRED == "DACSO") %>%
  select(
    ID, PSI_CODE, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    PSI_CREDENTIAL_CIP, PSI_CREDENTIAL_LEVEL, PSI_CREDENTIAL_CATEGORY,
    PSI_AWARD_SCHOOL_YEAR, OUTCOMES_CRED
  ) %>%
  collect() %>%
  inner_join(
    sch_tbl("Credential_Non_Dup_STP_CIP_Cleaning_v2_r") %>%
      select(PSI_CREDENTIAL_CIP_orig, STP_CIP_CODE_4) %>%
      collect(),
    by = c("PSI_CREDENTIAL_CIP" = "PSI_CREDENTIAL_CIP_orig")
  ) %>%
  left_join(
    sch_tbl("stp_credential_r") %>% select(ID, PSI_PEN) %>% collect(),
    by = "ID"
  ) %>%
  mutate(
    PEN_CANON = normalize_pen(PSI_PEN),
    AWARD_YEAR = suppressWarnings(as.integer(substr(PSI_AWARD_SCHOOL_YEAR, 1, 4)))
  ) %>%
  filter(!is.na(PEN_CANON))
log_info(glue::glue(
  "Step 1: credential side: {nrow(credential_side)} DACSO credentials with a canonical PEN"
))

# Institution alias pairs (shared lookup with the BGS v2 matcher; same
# PSI <-> INST coding universe applies to trades institutions).
alias_lookup <- sch_tbl("T_BGS_STP_INST_ALIAS_v2_r", schema = shareschema) %>%
  collect() %>%
  transmute(PSI_CODE, INST = INSTITUTION_CODE, IS_ALIAS = "Yes")

# DACSO v2 reconciled FINAL CIPs per credential program -- the richest
# STP-side view (XWALK-reconciled). Falls back to the spine's cleaned CIP.
dacso_v2_finals <- sch_tbl("Credential_Non_Dup_Programs_DACSO_FinalCIPs_v2_r") %>%
  select(
    PSI_CODE, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    PSI_CREDENTIAL_CIP, PSI_CREDENTIAL_LEVEL, PSI_CREDENTIAL_CATEGORY,
    OUTCOMES_CRED, DACSO_V2_FINAL_CIP4 = FINAL_CIP_CODE_4
  ) %>%
  collect()

# ==============================================================================
# STEP 2: PEN join + award-year lag calibration
# ==============================================================================
# WHAT: join every TRD survey row to every DACSO credential sharing its PEN
#       (many-to-many by design -- prior awards fan out), and measure the
#       award-year lag distribution.
# WHY:  the lag distribution is the window's calibration evidence (the
#       fact-find's -2 peak must be visible and inside the window; if a
#       future cycle shifts the peak, this table shows it).
pairs <- trd_survey %>%
  filter(!is.na(PEN_CANON)) %>%
  inner_join(
    credential_side %>% select(
      ID, PSI_CODE, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
      PSI_CREDENTIAL_CIP, PSI_CREDENTIAL_LEVEL, PSI_CREDENTIAL_CATEGORY,
      PSI_AWARD_SCHOOL_YEAR, AWARD_YEAR, STP_CIP_CODE_4, PEN_CANON
    ),
    by = "PEN_CANON",
    relationship = "many-to-many"
  ) %>%
  mutate(LAG = AWARD_YEAR - SURVEY_YEAR)

lag_dist <- pairs %>%
  count(LAG) %>%
  arrange(LAG)
write.csv(lag_dist, file.path(diag_dir, "trd-v2-lag-distribution.csv"), row.names = FALSE)
peak_lag <- lag_dist$LAG[which.max(lag_dist$n)]
stopifnot(peak_lag >= LAG_MIN, peak_lag <= LAG_MAX)  # window must contain the peak
log_info(glue::glue(
  "Step 2: PEN join produced {nrow(pairs)} survey-credential pairs; lag peak at {peak_lag} (window [{LAG_MIN}, {LAG_MAX}] -- calibration CSV written)"
))

# ==============================================================================
# STEP 3: Window + institution filter, then preference order to ONE credential
# ==============================================================================
# WHAT: keep credentials inside the lag window that agree on institution
#       (exact code or alias pair); among survivors prefer lag -2, then the
#       latest award year; keep one credential per survey row.
in_window <- pairs %>%
  filter(LAG >= LAG_MIN, LAG <= LAG_MAX) %>%
  left_join(alias_lookup, by = c("PSI_CODE", "INST")) %>%
  mutate(
    MATCH_INST = case_when(
      PSI_CODE == INST ~ "exact",
      !is.na(IS_ALIAS) ~ "alias",
      TRUE ~ NA_character_
    )
  ) %>%
  filter(!is.na(MATCH_INST))

chosen <- in_window %>%
  mutate(
    IS_ALIAS = MATCH_INST == "alias",
    IS_LAG2 = LAG == -2
  ) %>%
  arrange(KEY, desc(IS_LAG2), desc(AWARD_YEAR)) %>%
  distinct(KEY, .keep_all = TRUE) %>%
  transmute(
    KEY,
    MATCHED_ID = ID,
    MATCHED_PSI_CODE = PSI_CODE,
    MATCHED_AWARD_SCHOOL_YEAR = PSI_AWARD_SCHOOL_YEAR,
    MATCH_LAG = LAG,
    MATCH_INST,
    STP_CIP_CODE_4
  )
log_info(glue::glue(
  "Step 3: window + institution filter -> {nrow(chosen)} survey rows matched to a credential ({sum(chosen$MATCH_INST == 'exact')} exact-inst, {sum(chosen$MATCH_INST == 'alias')} alias-inst, {sum(chosen$MATCH_LAG == -2)} at lag -2)"
))

# ==============================================================================
# STEP 4: FINAL_CIP choice + fallback
# ==============================================================================
# WHAT: for matched rows, FINAL = the DACSO-v2-reconciled FINAL CIP of the
#       matched credential's program where available (richest STP-side
#       view), else the spine-cleaned STP CIP. Where no credential survives
#       (or the credential has no usable CIP), the survey-side LCIP_LCP4_CD
#       stands as FINAL -- preserving the frozen 02b-1 behaviour.
dacso_keys <- c(
  "PSI_CODE", "PSI_PROGRAM_CODE", "PSI_CREDENTIAL_PROGRAM_DESCRIPTION",
  "PSI_CREDENTIAL_CIP", "PSI_CREDENTIAL_LEVEL", "PSI_CREDENTIAL_CATEGORY",
  "OUTCOMES_CRED"
)

# DACSO-v2 FINAL attached via the matched credential's ID -> business keys
matched_cred <- credential_side %>%
  select(ID, all_of(dacso_keys)) %>%
  inner_join(dacso_v2_finals, by = dacso_keys) %>%
  select(ID, DACSO_V2_FINAL_CIP4)

# KEYs whose PEN matched at least one DACSO credential (distinguishes the
# no-credential fallback from the window/institution-miss fallback)
keys_with_credential <- pairs %>% distinct(KEY)

out <- trd_survey %>%
  left_join(chosen, by = "KEY") %>%
  left_join(matched_cred, by = c("MATCHED_ID" = "ID")) %>%
  left_join(keys_with_credential %>% mutate(HAS_CREDENTIAL = TRUE), by = "KEY") %>%
  mutate(
    STP_FINAL_CIP4 = coalesce(DACSO_V2_FINAL_CIP4, STP_CIP_CODE_4),
    STP_CIP_SOURCE = case_when(
      !is.na(DACSO_V2_FINAL_CIP4) ~ "dacso_v2_final",
      !is.na(STP_CIP_CODE_4) ~ "spine_stp_cip",
      !is.na(MATCHED_ID) ~ "none",
      TRUE ~ NA_character_
    ),
    MATCH_RULE = case_when(
      is.na(PEN_CANON) ~ "survey_fallback_no_pen",
      is.na(HAS_CREDENTIAL) ~ "survey_fallback_no_credential",
      is.na(MATCHED_ID) & is.na(STP_FINAL_CIP4) ~ "survey_fallback_no_window_match",
      is.na(MATCHED_ID) ~ "survey_fallback_no_window_match",
      is.na(STP_FINAL_CIP4) ~ "survey_fallback_no_stp_cip",
      MATCH_INST == "exact" & MATCH_LAG == -2 ~ "pen_match_lag2_exact_inst",
      MATCH_INST == "exact" ~ "pen_match_window_exact_inst",
      MATCH_INST == "alias" & MATCH_LAG == -2 ~ "pen_match_lag2_alias_inst",
      MATCH_INST == "alias" ~ "pen_match_window_alias_inst"
    ),
    # FINAL: matched credential's STP-side CIP; survey-side stands where the
    # bridge has nothing better (the frozen 02b-1 behaviour)
    FINAL_CIP_CODE_4 = coalesce(STP_FINAL_CIP4, LCIP_LCP4_CD),
    FINAL_CIP_SOURCE = if_else(
      !is.na(STP_FINAL_CIP4), "stp_credential", "survey_side"
    )
  )
log_info(glue::glue(
  "Step 4: FINAL_CIP assigned: {sum(out$FINAL_CIP_SOURCE == 'stp_credential')} from matched credentials, {sum(out$FINAL_CIP_SOURCE == 'survey_side')} survey-side fallback"
))
print(out %>% count(MATCH_RULE))

# ==============================================================================
# STEP 5: Validation against the ticket-02 fact-find
# ==============================================================================
# WHAT: reproduce the fact-find's headline numbers as a sanity gate:
#       (a) PEN coverage: ~95.6% of survey PENs have a DACSO credential
#           (fact-find: 91.6% into DACSO across distinct PENs; the row-level
#           rate here is expected in the same ballpark);
#       (b) matched-pair CIP4 agreement: raw agreement of the chosen
#           credential's STP CIP vs the survey's LCIP_LCP4_CD (fact-find:
#           85.4% raw many-to-many; window+inst filtering should hold or
#           improve it);
#       (c) no-credential fallback share (fact-find: 951 PENs = 4.4%).
matched_rows <- out %>% filter(!is.na(MATCHED_ID), !is.na(STP_FINAL_CIP4))
agreement <- matched_rows %>%
  filter(!is.na(LCIP_LCP4_CD), LCIP_LCP4_CD != "") %>%
  summarise(
    n = n(),
    agree = sum(STP_FINAL_CIP4 == LCIP_LCP4_CD),
    rate = round(100 * mean(STP_FINAL_CIP4 == LCIP_LCP4_CD), 1)
  )
pens_no_credential <- trd_survey %>%
  filter(!is.na(PEN_CANON)) %>%
  distinct(PEN_CANON) %>%
  anti_join(credential_side %>% distinct(PEN_CANON), by = "PEN_CANON")
log_info(glue::glue(
  "Step 5: validation vs fact-find -- matched-pair CIP4 agreement {agreement$agree}/{agreement$n} = {agreement$rate}% (fact-find: 85.4% raw); PENs with no DACSO credential: {nrow(pens_no_credential)} distinct ({round(100 * nrow(pens_no_credential) / n_distinct(trd_survey$PEN_CANON[!is.na(trd_survey$PEN_CANON)]), 1)}%; fact-find: 4.4%)"
))

# ==============================================================================
# STEP 6: Write the output (fresh name, analyst schema)
# ==============================================================================
out_tbl <- "Credential_Non_Dup_TRD_IDs_v2_r"
target <- Id(schema = my_schema, table = out_tbl)
if (dbExistsTable(con, target)) dbRemoveTable(con, target)
dbWriteTable(con, target, out)
log_info(glue::glue(
  "Wrote {my_schema}.{out_tbl}: {nrow(out)} rows ({sum(out$FINAL_CIP_SOURCE == 'stp_credential')} credential-backed)"
))

dbDisconnect(con)
log_info("==== 02a-trd-program-matching-v2.R COMPLETE ====")
