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
# 02a-cip-normalize.R  --  the shared STP-side CIP normalization spine (v2)
#
# ------------------------------------------------------------------------------
# WHERE THIS FITS IN THE MODEL
# ------------------------------------------------------------------------------
# PSSM computes one formula (docs/project-summary-for-new-analyst.md §2):
#
#   OCCSN(NOC) = GRADUATES(cred,age)          <- Module 04
#              x P(CIP | cred, age)           <- Module 06 (this is our factor)
#              x P(in labour supply | CIP)    <- Module 02b-2
#              x P(NOC | CIP, region)         <- Module 02b-3
#
# The second factor, P(CIP | cred, age), is only meaningful if every graduate
# carries a CIP code (Classification of Instructional Programs -- the field-of-
# study taxonomy) that is (a) valid in the current CIP2021 taxonomy and (b)
# consistent across the STP credential data and the Student Outcomes surveys.
# Downstream, `R/02b-1-pssm-cohorts.R` builds the cohort key
# `LCIP4_CRED = paste0(CIP_CODE_4, " - ", CRED)` (see 02b-1 line ~328) from the
# CIP4 this spine produces. A wrong or unresolved CIP4 here silently mis-buckets
# a graduate for the entire rest of the model.
#
# ------------------------------------------------------------------------------
# WHAT THIS FILE IS (and why it is a NEW file, not an edit of the originals)
# ------------------------------------------------------------------------------
# The STP CIP-cleaning cascade is currently duplicated, in slightly divergent
# forms, across four original scripts:
#   - R/02a-appso-programs.R            (steps 3-10, the cleanest reference)
#   - R/02a-bgs-program-matching.R
#   - R/02a-dacso-program-matching.R
#   - R/02a-update-cred-non-dup.R       (NULL-fallback block ~L444-546)
# This spine distils that logic into ONE pure, testable function so the CIP4
# namespace stops drifting between callers. Per the cip-matching wayfinder map
# (.scratch/cip-matching, tickets 03 + 13) the original scripts stay FROZEN and
# runnable (they recreate the baseline model run); this v2 spine coexists beside
# them and writes distinctly-named tables. Nothing here overwrites an original.
#
# ------------------------------------------------------------------------------
# THE CASCADE (and one deliberate improvement over the originals)
# ------------------------------------------------------------------------------
# normalize_stp_cip() resolves a raw STP CIP (e.g. "11.010") to a 4-digit code,
# a 2-digit code, their names, and a cluster, through ordered stages:
#   1. Format repair    -- add missing trailing / leading zero -> 7-char form.
#   2. Exact 6-digit    -- full "NN.NNNN" match in the INFOWARE 6-digit lookup;
#                          fills CIP4 + CIP2 + cluster. (rule: exact_6digit)
#   3. General program  -- codes ending ".00" in the 13 general-program prefixes
#                          map to the "01" variant (e.g. 24.00 -> 2401); fills
#                          CIP4 only.                     (rule: general_program)
#   4. 2-digit fallback -- first-5-char and first-2-char INFOWARE lookups fill
#                          the CIP2 (broad category) + cluster for rows the 4-
#                          digit stages could not resolve.
#   5. Names + flag     -- attach 4-/2-digit names; any CIP4 still unresolved is
#                          named "Invalid 4-digit CIP".  (rule: unresolved_4digit)
#
# DELIBERATE CHANGE vs the frozen originals: the originals let a first-5-char
# partial match PROMOTE to a 4-digit code. This spine does NOT -- only an exact
# 6-digit match or the explicit general-program rule may set CIP4. A 5-char
# prefix is trusted only for the broad 2-digit category. This is a correctness
# fix: a partial prefix is not evidence for a specific 4-digit field of study,
# and promoting it was a source of the CIP4 disagreement measured in ticket 01.
# The `CIP_MATCH_RULE` column records which stage resolved each row so the
# comparison harness (ticket 17) can quantify the effect.
#
# ------------------------------------------------------------------------------
# HOW TO USE IT
# ------------------------------------------------------------------------------
# As a library (pure functions, no DB):
#   options(cip_spine.lib_only = TRUE); source("R/02a-cip-normalize.R")
#   res <- normalize_stp_cip(df, lookups = list(cip6=, cip4=, cip2=))
#   pen <- normalize_pen(x)
# As a pipeline step (default): sourcing the file with cip_spine.lib_only unset
# runs the materialized wrapper, which reads distinct CIPs from
# credential_non_dup_r, applies normalize_stp_cip(), and writes
# Credential_Non_Dup_STP_CIP_Cleaning_v2_r to the analyst schema plus a
# gitignored disagreement diagnostic. Originals are never touched.
# ==============================================================================

library(dplyr)
library(tidyr)
library(stringr)

# The 13 "general program" CIP2 prefixes. A credential coded "NN.00" for one of
# these broad fields is not a valid 4-digit code in INFOWARE; the modelling
# convention (inherited verbatim from the frozen originals, e.g.
# R/02a-appso-programs.R general_program_prefixes) is to route it to the "01"
# general variant of that 2-digit family, e.g. 24.00 -> 2401. Kept as a single
# named constant so every caller shares one definition.
CIP_GENERAL_PROGRAM_PREFIXES <- c(
  "11.00", "13.00", "14.00", "19.00", "23.00", "24.00", "26.00",
  "40.00", "42.00", "45.00", "50.00", "52.00", "55.00"
)

# ------------------------------------------------------------------------------
# normalize_pen(x)  --  canonicalize a Personal Education Number (PEN)
# ------------------------------------------------------------------------------
# WHAT: turn any PEN representation into a clean integer string, or NA.
# WHY:  PEN is the join key linking a Student Outcomes survey respondent to
#       their STP credential (used by every per-survey matcher and by the
#       ticket-09 TRD PEN-match). The refreshed Oracle loads write PSI_PEN as a
#       FLOAT-derived string carrying a trailing ".0" (e.g. "116346735.0"), and
#       spreadsheet exports sometimes carry scientific notation ("1.17e+08") or
#       leading zeros. Survey-side PEN is a clean integer string, so a naive
#       string-equality join silently matches ZERO rows (ticket 12; probe 13l
#       measured raw join 0 vs normalized 156,149). Canonicalizing both sides
#       through this one helper removes that whole class of silent-break bug.
# HOW:  trim -> expand scientific notation and drop the ".0" via numeric round-
#       trip -> emit a plain integer string; anything non-numeric or <= 0
#       (blank, "0", junk) becomes NA so it can never join.
# Returns: character vector, same length as x, NA where no valid PEN exists.
normalize_pen <- function(x) {
  x_chr <- trimws(as.character(x))
  # Parse to a number first: this uniformly handles "123.0" (-> 123),
  # "1.17e+08" (-> 117000000) and "000475391" (-> 475391). Non-numeric -> NA.
  num <- suppressWarnings(as.numeric(x_chr))
  # format(..., scientific = FALSE) prints the full integer with no exponent
  # and no decimals (values are whole after rounding); trimws drops any padding.
  out <- ifelse(
    is.na(num) | num <= 0,
    NA_character_,
    trimws(format(round(num), scientific = FALSE, trim = TRUE))
  )
  out
}

# ------------------------------------------------------------------------------
# normalize_stp_cip(df, cip_col, lookups)  --  the STP CIP cleaning cascade
# ------------------------------------------------------------------------------
# WHAT: given a data.frame with a raw STP CIP column, return the SAME rows in the
#       SAME order plus the resolved CIP columns the model needs downstream:
#         STP_CIP_CODE_4, STP_CIP_CODE_4_NAME,
#         STP_CIP_CODE_2, STP_CIP_CODE_2_NAME,
#         STP_CIP_CLUSTER_CODE, STP_CIP_CLUSTER_NAME,
#         PSI_CREDENTIAL_CIP_orig  (the untouched input, for join-back),
#         CIP_MATCH_RULE           (decision trace: which stage resolved CIP4).
# WHY:  this is the single source of truth for STP-side CIP4 that feeds
#       FINAL_CIP_CODE_4 (via credential_non_dup_r) and ultimately the
#       LCIP4_CRED cohort key in 02b-1 -> P(CIP | cred, age) in Module 06.
# HOW:  a pure function -- it does not touch the database and does not mutate its
#       input. `lookups` is a named list of three INFOWARE CIP2021 frames so the
#       tests can inject synthetic fixtures; the pipeline wrapper below collects
#       the real dbo lookups into the same shape. Expected columns:
#         lookups$cip6: LCIP_CD_WITH_PERIOD, LCIP_LCP4_CD, LCIP_LCP2_CD,
#                       LCIP_LCIPPC_CD, LCIP_LCIPPC_NAME
#         lookups$cip4: LCP4_CD, LCP4_CIP_4DIGITS_NAME   (alias of LCP4_DIGITS_NAME)
#         lookups$cip2: LCP2_CD, LCP2_DIGITS_NAME
normalize_stp_cip <- function(df,
                              cip_col = "PSI_CREDENTIAL_CIP",
                              lookups) {
  stopifnot(is.data.frame(df), cip_col %in% names(df))
  cip6 <- lookups$cip6
  cip4 <- lookups$cip4
  cip2 <- lookups$cip2

  # Preserve the raw input verbatim. Stage 1 rewrites the working CIP in place,
  # but every join back to the caller's rows (and the originals' step-11 join)
  # keys on the ORIGINAL value, so we must keep it untouched.
  orig <- as.character(df[[cip_col]])
  cip <- orig

  # ---- Stage 1: repair malformed 6-character CIPs into the 7-char form -------
  # INFOWARE codes are 7 chars "NN.NNNN". Two common STP defects:
  #   1a) missing trailing digit: "11.010" (len 6, char 2 is a digit) -> "11.0100"
  #   1b) missing leading zero:   "1.0100" (len 6, char 2 is ".")     -> "01.0100"
  # Order matters: 1a first (only fires when char 2 is NOT "."), then 1b catches
  # anything still length 6.
  is6 <- !is.na(cip) & nchar(cip) == 6
  fix_trail <- is6 & substr(cip, 2, 2) != "."
  cip[fix_trail] <- paste0(cip[fix_trail], "0")
  is6 <- !is.na(cip) & nchar(cip) == 6
  cip[is6] <- paste0("0", cip[is6])

  # Working columns, all initialized NA; each stage fills only where still NA.
  code4 <- rep(NA_character_, length(cip))
  code2 <- rep(NA_character_, length(cip))
  rule <- rep(NA_character_, length(cip))

  # ---- Stage 2: exact 6-digit match (fills CIP4 + CIP2) ----------------------
  # The most reliable resolution: the full cleaned code exists in the 6-digit
  # lookup, so both the 4-digit and 2-digit codes are taken directly from it.
  m6 <- match(cip, cip6$LCIP_CD_WITH_PERIOD)
  hit6 <- !is.na(m6)
  code4[hit6] <- cip6$LCIP_LCP4_CD[m6[hit6]]
  code2[hit6] <- cip6$LCIP_LCP2_CD[m6[hit6]]
  rule[hit6] <- "exact_6digit"

  # ---- Stage 3: general-program rule (fills CIP4 only) -----------------------
  # For codes still lacking a CIP4, if the first 5 chars are a ".00" general
  # prefix, adopt the "01" variant (24.00 -> 2401). This is a rule, not a lookup,
  # so it applies even when no 4-digit lookup row backs the "01" variant.
  pre5 <- substr(cip, 1, 5)
  gp <- is.na(code4) & !is.na(cip) & pre5 %in% CIP_GENERAL_PROGRAM_PREFIXES
  code4[gp] <- paste0(substr(cip[gp], 1, 2), "01")
  rule[gp] <- "general_program"

  # Any row whose CIP4 is still NA is, by definition, unresolved at the 4-digit
  # level. We record that in the decision trace now (the 2-digit stages below
  # only touch CIP2 / cluster, never CIP4 -- the deliberate change described in
  # the header: a partial prefix must not invent a 4-digit field of study).
  # A blank / NA input CIP is likewise "unresolved"; every row gets a rule so
  # the harness can count coverage, including rows carrying no CIP at all.
  rule[is.na(rule) & (is.na(cip) | cip == "")] <- "unresolved_4digit"
  rule[is.na(rule)] <- "unresolved_4digit"

  # ---- Stage 4: 2-digit fallback (fills CIP2 + broad category only) ----------
  # For rows with no CIP2 yet, resolve the broad category from a first-5-char
  # then first-2-char match against the 6-digit lookup. This gives the model a
  # usable 2-digit rollup even where the specific 4-digit code is unknown.
  cip6_pre5 <- substr(cip6$LCIP_CD_WITH_PERIOD, 1, 5)
  need2 <- is.na(code2) & !is.na(cip)
  m5 <- match(pre5, cip6_pre5)
  fill5 <- need2 & !is.na(m5)
  code2[fill5] <- cip6$LCIP_LCP2_CD[m5[fill5]]

  cip6_pre2 <- substr(cip6$LCIP_CD_WITH_PERIOD, 1, 2)
  pre2 <- substr(cip, 1, 2)
  need2 <- is.na(code2) & !is.na(cip)
  m2 <- match(pre2, cip6_pre2)
  fill2 <- need2 & !is.na(m2)
  code2[fill2] <- cip6$LCIP_LCP2_CD[m2[fill2]]

  # ---- Stage 5a: cluster code + name -----------------------------------------
  # The cluster (LCIP_LCIPPC_*) is the coarse grouping 02b-1 uses when a CIP4 is
  # too sparse to model on its own. Derive it from the 6-digit table via the
  # resolved CIP4 (its representative row); this mirrors the ticket-01 baseline
  # method. distinct() collapses the many-6-to-one-4 relationship to one row.
  cluster_map <- cip6 %>%
    select(LCIP_LCP4_CD, LCIP_LCIPPC_CD, LCIP_LCIPPC_NAME) %>%
    distinct(LCIP_LCP4_CD, .keep_all = TRUE)
  mc <- match(code4, cluster_map$LCIP_LCP4_CD)
  cluster_code <- cluster_map$LCIP_LCIPPC_CD[mc]
  cluster_name <- cluster_map$LCIP_LCIPPC_NAME[mc]

  # ---- Stage 5b: attach 4-digit and 2-digit names ----------------------------
  # Names are for reporting and for eyeballing that a code makes sense. An
  # unresolved CIP4 has no name -> flag it "Invalid 4-digit CIP" (never leave a
  # bare NULL) so downstream quality checks can find it.
  m4n <- match(code4, cip4$LCP4_CD)
  code4_name <- cip4$LCP4_CIP_4DIGITS_NAME[m4n]
  code4_name[is.na(code4_name)] <- "Invalid 4-digit CIP"

  m2n <- match(code2, cip2$LCP2_CD)
  code2_name <- cip2$LCP2_DIGITS_NAME[m2n]

  # Return the caller's rows unchanged, with the resolved columns appended.
  out <- df
  out$STP_CIP_CODE_4 <- code4
  out$STP_CIP_CODE_4_NAME <- code4_name
  out$STP_CIP_CODE_2 <- code2
  out$STP_CIP_CODE_2_NAME <- code2_name
  out$STP_CIP_CLUSTER_CODE <- cluster_code
  out$STP_CIP_CLUSTER_NAME <- cluster_name
  out$PSI_CREDENTIAL_CIP_orig <- orig
  out$CIP_MATCH_RULE <- rule
  out
}

# ==============================================================================
# Pipeline wrapper (skipped when cip_spine.lib_only is TRUE, i.e. under test)
# ------------------------------------------------------------------------------
# Reads the real CIP2021 lookups from dbo, applies normalize_stp_cip() to the
# distinct STP CIPs in credential_non_dup_r, and materializes the unified
# cleaning table plus a survey-vs-STP disagreement diagnostic. This block never
# edits or overwrites an original 02a table -- it writes a fresh _v2_r name.
# ==============================================================================
if (!isTRUE(getOption("cip_spine.lib_only"))) {
  library(DBI)
  library(odbc)
  library(dbplyr)

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
  message(Sys.time(), " | 02a-cip-normalize: connected to SQL Server")

  # Collect the three CIP2021 lookups from dbo into plain frames. The 4-digit
  # name column was renamed LCP4_CIP_4DIGITS_NAME -> LCP4_DIGITS_NAME in CIP2021;
  # alias it back to the contract name the pure function expects (ticket 18).
  lk_cip6 <- tbl(con, in_schema(shareschema, "INFOWARE_L_CIP_6DIGITS_CIP2021")) %>%
    select(
      LCIP_CD_WITH_PERIOD, LCIP_LCP4_CD, LCIP_LCP2_CD,
      LCIP_LCIPPC_CD, LCIP_LCIPPC_NAME
    ) %>%
    collect()
  lk_cip4 <- tbl(con, in_schema(shareschema, "INFOWARE_L_CIP_4DIGITS_CIP2021")) %>%
    select(LCP4_CD, LCP4_CIP_4DIGITS_NAME = LCP4_DIGITS_NAME) %>%
    collect()
  lk_cip2 <- tbl(con, in_schema(shareschema, "INFOWARE_L_CIP_2DIGITS_CIP2021")) %>%
    select(LCP2_CD, LCP2_DIGITS_NAME) %>%
    collect()
  lookups <- list(cip6 = lk_cip6, cip4 = lk_cip4, cip2 = lk_cip2)
  message(Sys.time(), " | 02a-cip-normalize: loaded CIP2021 lookups from dbo")

  # One row per distinct raw STP CIP -- the cleaning is per-code, then joined
  # back to full credential rows by the caller, exactly like the originals.
  distinct_cips <- tbl(con, in_schema(my_schema, "credential_non_dup_r")) %>%
    distinct(PSI_CREDENTIAL_CIP) %>%
    collect()

  cleaned <- normalize_stp_cip(distinct_cips, lookups = lookups)
  message(
    Sys.time(), " | 02a-cip-normalize: normalized ",
    nrow(cleaned), " distinct CIPs; ",
    sum(cleaned$CIP_MATCH_RULE != "unresolved_4digit", na.rm = TRUE),
    " resolved at 4-digit"
  )

  # Materialize the unified cleaning table under a FRESH name so it coexists
  # with the two per-survey originals (no collision, originals stay frozen).
  out_tbl <- "Credential_Non_Dup_STP_CIP_Cleaning_v2_r"
  if (dbExistsTable(con, Id(schema = my_schema, table = out_tbl))) {
    dbRemoveTable(con, Id(schema = my_schema, table = out_tbl))
  }
  dbWriteTable(con, Id(schema = my_schema, table = out_tbl), cleaned)
  message(Sys.time(), " | 02a-cip-normalize: wrote ", my_schema, ".", out_tbl)

  # --------------------------------------------------------------------------
  # Side-car disagreement diagnostic (ticket 13, item 4)
  # --------------------------------------------------------------------------
  # WHAT: for BGS / DACSO / APPSO (the surveys with an STP-side bucket), compare
  #       each respondent's survey-side CIP4 against the SPINE-normalized STP
  #       CIP4, per PEN, and write the disagreeing pairs to a CSV for analyst
  #       review. TRD is excluded: it has no STP-side bucket (ticket 02) -- its
  #       PEN-match into DACSO is ticket 16's design, not the spine's.
  # WHY:  this is the "side-car disagreement diagnostic" from the spine
  #       interface design (ticket 03) -- it surfaces exactly where the shared
  #       STP normalization still contradicts the survey's own field-of-study
  #       coding, BEFORE any per-survey reconciliation logic runs. The v2
  #       matchers (tickets 14/15) and the comparison harness (ticket 17)
  #       consume these rows as their work-list.
  # HOW:  both sides are deduplicated to one CIP4 per PEN (modal value, ties
  #       broken by first-max -- same convention as the ticket-01/11 audits) so
  #       the many-credentials-per-PEN fan-out cannot inflate the pair count.
  #       PENs are canonicalized with normalize_pen() on both sides. The CSV is
  #       gitignored (.scratch/ holds local diagnostics only).
  modal_cip4 <- function(d) {
    d %>%
      dplyr::count(survey, pen, cip4) %>%
      dplyr::group_by(survey, pen) %>%
      dplyr::slice_max(order_by = n, n = 1, with_ties = FALSE) %>%
      dplyr::ungroup() %>%
      dplyr::select(survey, pen, cip4)
  }

  # STP side: PEN (canonicalized) + the spine's normalized CIP4, per survey
  # bucket. stp_credential_r provides PSI_PEN; the spine cleaning table
  # (written above) provides STP_CIP_CODE_4 keyed on the ORIGINAL raw CIP.
  stp_side <- dbGetQuery(con, sprintf(
    "SELECT c.OUTCOMES_CRED AS survey, s.PSI_PEN AS pen_raw, k.STP_CIP_CODE_4 AS cip4
     FROM [%s].[credential_non_dup_r] c
     JOIN [%s].[stp_credential_r] s ON c.ID = s.ID
     JOIN [%s].[Credential_Non_Dup_STP_CIP_Cleaning_v2_r] k
       ON k.PSI_CREDENTIAL_CIP_orig = c.PSI_CREDENTIAL_CIP
     WHERE k.STP_CIP_CODE_4 IS NOT NULL",
    my_schema, my_schema, my_schema
  ))
  stp_side <- stp_side %>%
    mutate(pen = normalize_pen(pen_raw)) %>%
    filter(!is.na(pen))

  # Survey side: PEN (canonicalized) + survey-reported CIP4 + institution, per
  # survey. Column names differ per survey table (loaded by the 01* scripts).
  survey_sql <- c(
    BGS   = "SELECT 'BGS' AS survey, PEN AS pen_raw, INST AS inst, CIP_CODE_4 AS cip4
             FROM dbo.t_bgs_data_final_r",
    DACSO = "SELECT 'DACSO' AS survey, COCI_PEN AS pen_raw, COCI_INST_CD AS inst, LCP4_CD AS cip4
             FROM dbo.t_dacso_data_part_1_stepa_r",
    APPSO = "SELECT 'APPSO' AS survey, PEN AS pen_raw, INST AS inst, LCIP_LCP4_CD AS cip4
             FROM dbo.t_appso_data_final_r"
  )
  svy_side <- do.call(rbind, lapply(names(survey_sql), function(s) {
    d <- dbGetQuery(con, survey_sql[[s]])
    d$pen_raw <- as.character(d$pen_raw)
    d
  }))
  svy_side <- svy_side %>%
    mutate(pen = normalize_pen(pen_raw)) %>%
    filter(!is.na(pen), !is.na(cip4), cip4 != "")

  # Pair per (survey, PEN) so each survey's respondents only meet that
  # survey's STP bucket, keep disagreements, write the CSV side-car. The
  # modal dedup above drops `inst`, so re-attach it from a distinct map
  # before writing (analysts read the CSV grouped by institution).
  diag_rows <- modal_cip4(svy_side) %>%
    inner_join(modal_cip4(stp_side), by = c("survey", "pen"),
               suffix = c("_svy", "_stp")) %>%
    filter(cip4_svy != cip4_stp) %>%
    left_join(
      svy_side %>% dplyr::distinct(survey, pen, inst),
      by = c("survey", "pen")
    ) %>%
    select(
      survey, pen, inst, cip4_survey = cip4_svy, cip4_stp = cip4_stp
    )
  diag_dir <- file.path(".scratch", "cip-matching", "diagnostics")
  if (!dir.exists(diag_dir)) dir.create(diag_dir, recursive = TRUE)
  diag_csv <- file.path(diag_dir, "spine-cip-disagreement.csv")
  write.csv(diag_rows, diag_csv, row.names = FALSE)
  message(
    Sys.time(), " | 02a-cip-normalize: spine-cip-disagreement.csv written: ",
    format(nrow(diag_rows), big.mark = ","), " disagreeing PENs"
  )

  dbDisconnect(con)
}
