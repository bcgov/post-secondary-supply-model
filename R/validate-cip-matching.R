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
# validate-cip-matching.R  --  original-vs-v2 comparison harness (wayfinder
# ticket 17; implements the ticket-04 validation metric)
#
# ------------------------------------------------------------------------------
# WHAT THIS IS
# ------------------------------------------------------------------------------
# The acceptance gate for the cip-matching v2 family (spine ticket 13 + BGS 14
# + DACSO 15 + TRD 16). It computes the validation metrics IDENTICALLY for
# the original path (the materialized 02a tables from ticket 11 — the
# post-02a baseline) and the v2 path (the fresh *_v2_r tables), and writes a
# side-by-side markdown report:
#
#   .scratch/cip-matching/validation-report.md   (gitignored, per the
#   SO-Oracle convention)
#
# READ-ONLY against the database: no table is created, altered, or dropped.
#
# ------------------------------------------------------------------------------
# THE METRICS (ticket-04 resolution)
# ------------------------------------------------------------------------------
#   (a) per-record  raw many-to-many pair disagreement (cheap count; inflated
#                    by prior-award pairs)
#   (b) per-PEN     modal-dedup CIP4 disagreement per PEN — PRIMARY: the
#                    acceptance bar rides here
#   (c) per-institution  institutional rates (alias/walk calibration signal)
#
# Acceptance bars:
#   APPSO / GRAD  row-for-row identical FINAL_CIP_CODE_4 vs the 02a original
#                 outputs — zero regression (v2 does not touch them; this
#                 guards against pipeline drift).
#   BGS / DACSO   per-PEN (b) CIP4 vs the post-02a baseline, floor X = 0pp
#                 reduction (bar locked at the T11-close floor: v2 must not
#                 regress; positive reduction expected post-curation).
#   TRD           reframed coverage metric (ticket 02/09): % survey rows
#                 credential-backed + matched-pair agreement; and a
#                 preservation guarantee — no survey row loses its CIP.
#
# Exit code: 0 = all bars pass (cutover PR acceptance); 1 = any fail (see
# the verdicts section — an expected fail documents its pending cause in the
# intentional-diffs register).
# ==============================================================================

library(tidyverse)
library(odbc)
library(DBI)
library(glue)

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
message(Sys.time(), " | validate-cip-matching: connected (read-only)")

report_path <- file.path(".scratch", "cip-matching", "validation-report.md")
diag_dir <- file.path(".scratch", "cip-matching", "diagnostics")

# Acceptance floor for BGS/DACSO per-PEN reduction, in percentage points
# (locked at the T11-close floor per the ticket-04 deferral).
BAR_REDUCTION_PP <- 0

verdicts <- list()

record_verdict <- function(survey, metric, v1, v2, bar, pass, note = "") {
  verdicts[[length(verdicts) + 1]] <<- tibble(
    SURVEY = survey, METRIC = metric,
    V1 = v1, V2 = v2, BAR = bar,
    VERDICT = if (pass) "PASS" else "FAIL",
    NOTE = note
  )
}

# ------------------------------------------------------------------------------
# Shared metric engine
# ------------------------------------------------------------------------------
# Modal CIP4 per PEN (most frequent; ties broken by first-max -- the
# convention every audit in this effort has used since ticket 01).
modal_cip4 <- function(d) {
  d %>%
    count(pen, cip4) %>%
    group_by(pen) %>%
    slice_max(order_by = n, n = 1, with_ties = FALSE) %>%
    ungroup() %>%
    select(pen, cip4)
}

# BigINT-cast helper for PEN columns (the .0-suffix artefact is neutralized
# the same way every probe since ticket 01 has done).
pen_cast <- function(col) glue::glue(
  "TRY_CAST(TRY_CAST({col} AS FLOAT) AS BIGINT)"
)

# ==============================================================================
# SECTION A — Per-survey metric engine: original vs v2
# ==============================================================================
# Original path for BGS/DACSO = credential_non_dup_r (the merged, materialized
# 02a output; per-ticket-11 baseline). v2 path = the per-survey *_v2_r tables:
# BGS reads Credential_Non_Dup_BGS_IDs_v2_r directly; DACSO reaches
# credential rows through the 7-column business-key join (the same join
# update-cred uses), so the v2 metric sees exactly what a future cutover
# would feed the model.

svy_bgs <- glue::glue(
  "SELECT {pen} AS pen, INST AS inst, SURVEY_YEAR AS cycle, CIP_CODE_4 AS cip4
   FROM dbo.t_bgs_data_final_r
   WHERE CIP_CODE_4 IS NOT NULL AND CIP_CODE_4 <> '' AND {pen} IS NOT NULL",
  pen = pen_cast("PEN")
)
svy_dacso <- glue::glue(
  "SELECT {pen} AS pen, COCI_INST_CD AS inst, COCI_SUBM_CD AS cycle, LCP4_CD AS cip4
   FROM dbo.t_dacso_data_part_1_stepa_r
   WHERE LCP4_CD IS NOT NULL AND LCP4_CD <> '' AND {pen} IS NOT NULL",
  pen = pen_cast("COCI_PEN")
)

stp_v1 <- function(bucket) glue::glue(
  "SELECT {pen} AS pen, c.FINAL_CIP_CODE_4 AS cip4
   FROM [{my_schema}].[credential_non_dup_r] c
   JOIN [{my_schema}].[stp_credential_r] s ON c.ID = s.ID
   WHERE c.OUTCOMES_CRED = '{bucket}' AND c.FINAL_CIP_CODE_4 IS NOT NULL
     AND {pen} IS NOT NULL",
  pen = pen_cast("s.PSI_PEN"), bucket = bucket
)
stp_bgs_v2 <- glue::glue(
  "SELECT {pen} AS pen, c.FINAL_CIP_CODE_4 AS cip4
   FROM [{my_schema}].[Credential_Non_Dup_BGS_IDs_v2_r] c
   JOIN [{my_schema}].[stp_credential_r] s ON c.ID = s.ID
   WHERE c.FINAL_CIP_CODE_4 IS NOT NULL AND {pen} IS NOT NULL",
  pen = pen_cast("s.PSI_PEN")
)
stp_dacso_v2 <- glue::glue(
  "SELECT {pen} AS pen, f.FINAL_CIP_CODE_4 AS cip4
   FROM [{my_schema}].[credential_non_dup_r] c
   JOIN [{my_schema}].[stp_credential_r] s ON c.ID = s.ID
   JOIN [{my_schema}].[Credential_Non_Dup_Programs_DACSO_FinalCIPs_v2_r] f
     ON f.PSI_CODE = c.PSI_CODE AND f.PSI_PROGRAM_CODE = c.PSI_PROGRAM_CODE
    AND f.PSI_CREDENTIAL_PROGRAM_DESCRIPTION = c.PSI_CREDENTIAL_PROGRAM_DESCRIPTION
    AND f.PSI_CREDENTIAL_CIP = c.PSI_CREDENTIAL_CIP
    AND f.PSI_CREDENTIAL_LEVEL = c.PSI_CREDENTIAL_LEVEL
    AND f.PSI_CREDENTIAL_CATEGORY = c.PSI_CREDENTIAL_CATEGORY
    AND f.OUTCOMES_CRED = c.OUTCOMES_CRED
   WHERE c.OUTCOMES_CRED = 'DACSO' AND f.FINAL_CIP_CODE_4 IS NOT NULL
     AND {pen} IS NOT NULL",
  pen = pen_cast("s.PSI_PEN")
)

survey_metric <- function(name, svy_sql, stp_v1_sql, stp_v2_sql, bucket) {
  svy <- dbGetQuery(con, svy_sql)
  # survey-side modal row per PEN carries the institution + cycle context
  # (deterministic first-row pick -- inst/cycle are stable per PEN in
  # practice; only used for groupings)
  svy_ic <- svy %>%
    group_by(pen) %>%
    slice_head(n = 1) %>%
    ungroup() %>%
    select(pen, inst, cycle)
  v1p <- inner_join(v1_v1pen(svy_sql, stp_v1_sql), svy_ic, by = "pen") %>%
    mutate(side = "v1")
  v2p <- inner_join(v1_v1pen(svy_sql, stp_v2_sql), svy_ic, by = "pen") %>%
    mutate(side = "v2")

  rate <- function(p) {
    tibble(
      pairs = nrow(p),
      disagree = sum(p$cip4_svy != p$cip4_stp),
      pct = round(100 * mean(p$cip4_svy != p$cip4_stp), 2)
    )
  }
  list(
    name = name, bucket = bucket,
    v1_pairs = v1p, v2_pairs = v2p,
    v1_rate = rate(v1p), v2_rate = rate(v2p),
    svy_rows = nrow(svy)
  )
}

# modal-dedup pair builder (metric b core)
v1_v1pen <- function(svy_sql, stp_sql) {
  svy <- dbGetQuery(con, svy_sql)
  stp <- dbGetQuery(con, stp_sql)
  inner_join(modal_cip4(svy), modal_cip4(stp), by = "pen", suffix = c("_svy", "_stp"))
}

message(Sys.time(), " | Section A: computing BGS metrics...")
bgs <- survey_metric("BGS", svy_bgs, stp_v1("BGS"), stp_bgs_v2, "BGS")
message(Sys.time(), " | Section A: computing DACSO metrics...")
dacso <- survey_metric("DACSO", svy_dacso, stp_v1("DACSO"), stp_dacso_v2, "DACSO")

for (m in list(bgs, dacso)) {
  delta <- round(m$v2_rate$pct - m$v1_rate$pct, 2)
  pass <- (m$v1_rate$pct - m$v2_rate$pct) >= BAR_REDUCTION_PP
  note <- if (m$name == "BGS" && !pass) {
    "Pending curation of the 299 exported borderline combinations into T_BGS_CUSTOM_CIP_CHOICES_v2_r (see docs/bgs-v2-program-matching-findings.md section 4)"
  } else ""
  record_verdict(
    m$name, "per-PEN CIP4", paste0(m$v1_rate$pct, "%"), paste0(m$v2_rate$pct, "%"),
    paste0("v2 <= v1 (floor -", BAR_REDUCTION_PP, "pp)"), pass, note
  )
}

# ==============================================================================
# SECTION B — APPSO / GRAD row-for-row zero-regression checks
# ==============================================================================
# v2 does not touch APPSO/GRAD (STP-direct pattern, map out-of-scope). The
# guard: the merged credential_non_dup_r rows must be row-for-row identical
# to the 02a original outputs they were copied from (update-cred applied them
# verbatim) -- catching any pipeline drift that would silently move an
# unchanged survey.
for (b in c("APPSO", "GRAD")) {
  ids_tbl <- if (b == "APPSO") "Credential_Non_Dup_APPSO_IDs_r" else "Credential_Non_Dup_GRAD_IDs_r"
  cmp <- dbGetQuery(con, glue::glue(
    "SELECT COUNT(*) AS n,
       SUM(CASE WHEN a.FINAL_CIP_CODE_4 = c.FINAL_CIP_CODE_4 THEN 1 ELSE 0 END) AS same
     FROM [{my_schema}].[{ids_tbl}] a
     JOIN [{my_schema}].[credential_non_dup_r] c ON a.ID = c.ID
     WHERE a.FINAL_CIP_CODE_4 IS NOT NULL"
  ))
  pass <- cmp$same == cmp$n
  record_verdict(
    b, "row-for-row FINAL_CIP_CODE_4", paste0(cmp$n, " rows"),
    paste0(cmp$same, " identical"), "100% identical", pass,
    if (!pass) "Pipeline drift: merged table differs from the 02a output" else ""
  )
}

# ==============================================================================
# SECTION C — TRD coverage metrics (reframed design)
# ==============================================================================
trd <- dbGetQuery(con, glue::glue(
  "SELECT MATCH_RULE, FINAL_CIP_CODE_4, LCIP_LCP4_CD, FINAL_CIP_SOURCE
   FROM [{my_schema}].[Credential_Non_Dup_TRD_IDs_v2_r]"
))
trd_total <- nrow(trd)
trd_backed <- sum(trd$MATCH_RULE %in% c(
  "pen_match_lag2_exact_inst", "pen_match_window_exact_inst",
  "pen_match_lag2_alias_inst", "pen_match_window_alias_inst"
))
trd_with_cip <- sum(
  !is.na(trd$FINAL_CIP_CODE_4) & trd$FINAL_CIP_CODE_4 != ""
)
trd_matched <- trd %>% filter(grepl("^pen_match", MATCH_RULE))
trd_agree <- trd_matched %>%
  filter(!is.na(LCIP_LCP4_CD), LCIP_LCP4_CD != "") %>%
  summarise(n = n(), agree = sum(FINAL_CIP_CODE_4 == LCIP_LCP4_CD)) %>%
  mutate(pct = round(100 * agree / n, 1))
# Preservation guarantee: every survey row that carried a CIP still has one
trd_lost <- trd %>%
  filter((!is.na(LCIP_LCP4_CD) & LCIP_LCP4_CD != "") &
           (is.na(FINAL_CIP_CODE_4) | FINAL_CIP_CODE_4 == "")) %>%
  nrow()
record_verdict(
  "TRD", "coverage (credential-backed)", "--",
  paste0(round(100 * trd_backed / trd_total, 1), "% of ", trd_total, " rows"),
  "no row loses its CIP", trd_lost == 0,
  paste0("matched-pair CIP4 agreement ", trd_agree$pct, "% (", trd_agree$agree,
         "/", trd_agree$n, ")")
)

# ==============================================================================
# SECTION D — BGS silent-break audit (ticket 12: the .0 PEN artefact)
# ------------------------------------------------------------------------------
# The refreshed Oracle loads write PSI_PEN with a trailing ".0" in the RAW
# stp_credential_r (1.3M rows -- expected, reported as context). The original
# normalizes at ACQUISITION (commit de8b299: the IDs table's PSI_PEN is cast
# on the way in), so the GATE is on the consumed table: the BGS IDs table
# must carry 0 artefact PENs and the original-path string-equality PEN join
# must be live (> 0 rows).
artefact_raw <- dbGetQuery(con, glue::glue(
  "SELECT COUNT(*) AS n
   FROM [{my_schema}].[stp_credential_r]
   WHERE PSI_PEN LIKE '%.0'"
))
artefact <- dbGetQuery(con, glue::glue(
  "SELECT COUNT(*) AS n
   FROM [{my_schema}].[Credential_Non_Dup_BGS_IDs_r]
   WHERE PSI_PEN LIKE '%.0'"
))
pen_join_n <- dbGetQuery(con, glue::glue(
  "SELECT COUNT(*) AS n
   FROM [{my_schema}].[T_BGS_Data_Final_for_OutcomesMatching_r] t
   INNER JOIN [{my_schema}].[Credential_Non_Dup_BGS_IDs_r] c
     ON t.PEN = c.PSI_PEN
   WHERE t.PEN IS NOT NULL AND t.PEN <> '' AND t.PEN <> '0'"
))
record_verdict(
  "BGS", "PEN silent-break audit",
  paste0(artefact$n, " .0 PENs in IDs table"),
  paste0(pen_join_n$n, " PEN-join rows"),
  "0 artefact rows in IDs table; join > 0",
  artefact$n == 0 && pen_join_n$n > 0,
  paste0("(raw stp_credential_r still carries ", format(artefact_raw$n, big.mark = ","),
         " artefact PENs by design -- normalized at acquisition; documented 2023 join expectation 133,952, refreshed cycles differ)")
)

# ==============================================================================
# SECTION E — Per-institution deltas, per-cycle drift, MATCH_RULE, caseloads
# ==============================================================================
inst_table <- function(m, top_n = 10) {
  v1i <- m$v1_pairs %>% group_by(inst) %>%
    summarise(v1_pairs = n(), v1_dis = sum(cip4_svy != cip4_stp),
              v1_pct = round(100 * mean(cip4_svy != cip4_stp), 2), .groups = "drop")
  v2i <- m$v2_pairs %>% group_by(inst) %>%
    summarise(v2_pairs = n(), v2_dis = sum(cip4_svy != cip4_stp),
              v2_pct = round(100 * mean(cip4_svy != cip4_stp), 2), .groups = "drop")
  full_join(v1i, v2i, by = "inst") %>%
    mutate(delta_pp = round(coalesce(v2_pct, 0) - coalesce(v1_pct, 0), 2)) %>%
    arrange(desc(v2_dis - coalesce(v1_dis, 0))) %>%
    slice_head(n = top_n)
}
bgs_inst <- inst_table(bgs)
dacso_inst <- inst_table(dacso)

cycle_table <- function(m) {
  v1c <- m$v1_pairs %>% group_by(cycle) %>%
    summarise(v1_pct = round(100 * mean(cip4_svy != cip4_stp), 2), .groups = "drop")
  v2c <- m$v2_pairs %>% group_by(cycle) %>%
    summarise(v2_pct = round(100 * mean(cip4_svy != cip4_stp), 2), .groups = "drop")
  full_join(v1c, v2c, by = "cycle") %>% arrange(cycle) %>%
    mutate(delta_pp = round(v2_pct - v1_pct, 2))
}
bgs_cycle <- cycle_table(bgs)
dacso_cycle <- cycle_table(dacso)

mr_dist <- function(tbl, col = "MATCH_RULE") {
  dbGetQuery(con, glue::glue(
    "SELECT {col} AS [rule], COUNT(*) AS n FROM [{my_schema}].[{tbl}] GROUP BY {col}"
  )) %>%
    arrange(desc(n))
}
mr_bgs <- mr_dist("Credential_Non_Dup_BGS_IDs_v2_r")
mr_dacso <- mr_dist("Credential_Non_Dup_Programs_DACSO_FinalCIPs_v2_r")
mr_trd <- mr_dist("Credential_Non_Dup_TRD_IDs_v2_r")

# Diagnostics written by the v2 runs (existence-checked; regenerated by each
# v2 run, gitignored)
csv_line_count <- function(f) if (file.exists(f)) {
  read.csv(f, check.names = FALSE) %>% nrow()
} else NA_integer_
n_alias_proposals <- csv_line_count(file.path(diag_dir, "bgs-v2-alias-proposals.csv"))
n_bgs_residual <- csv_line_count(file.path(diag_dir, "bgs-v2-residual-manual-review.csv"))
n_dacso_walk <- csv_line_count(file.path(diag_dir, "dacso-v2-walk-chains.csv"))

# ==============================================================================
# SECTION F — Intentional-diffs register (ticket-04 Q6 design: entries name
# the pending cause; contents filled at build time)
# ==============================================================================
register <- tribble(
  ~DIFF, ~CAUSE, ~STATUS,
  "BGS: 2,159 credential IDs flip FINAL_CIP4 vs v1",
  "v1 applied the 2023-era analyst-curated LAN CSV to the borderline caseload; v2 defaults those rows to STP pending fresh curation of 299 program combinations into dbo.T_BGS_CUSTOM_CIP_CHOICES_v2_r",
  "PENDING CURATION (BGS v2 findings section 4/5)",
  "DACSO: 71 FinalCIPs rows differ",
  "auto-walk resolves chains the original's per-cycle blocks missed (per-PEN -1.86pp net improvement)",
  "INTENTIONAL (accepted improvement)",
  "DACSO: PRGM_ID 3119 CIP 1907 vs registry 1315",
  "2021 analyst NOTES-review judgment; original value applied via dbo.T_DACSO_PRGM_EXCEPTIONS_v2_r pending reconciliation",
  "PENDING RECONCILIATION (DACSO v2 findings section 4/6)",
  "BGS: 37 credentials without STP CIP4",
  "spine deliberately does not promote 5-char partial matches to a 4-digit code (stricter than the original's cascade)",
  "INTENTIONAL (spine design, ticket 13)",
  "TRD: no v1 comparison possible",
  "first dedicated TRD matcher; baseline is the frozen 02b-1 survey-side-only behaviour, preserved on the fallback path",
  "NEW COVERAGE (TRD v2 findings)"
)

# ==============================================================================
# REPORT
# ==============================================================================
verdict_df <- bind_rows(verdicts)
all_pass <- all(verdict_df$VERDICT == "PASS")

fmt_headline <- function(m) {
  bind_rows(
    m$v1_rate %>% mutate(side = "v1 (original)"),
    m$v2_rate %>% mutate(side = "v2")
  ) %>% select(side, pairs, disagree, pct)
}

md_table <- function(df) {
  if (nrow(df) == 0) return("_none_")
  header <- paste0("| ", paste(names(df), collapse = " | "), " |")
  sep <- paste0("|", paste(rep("---", ncol(df)), collapse = "|"), "|")
  rows <- apply(df, 1, function(r) paste0("| ", paste(r, collapse = " | "), " |"))
  paste(c(header, sep, rows), collapse = "\n")
}

report <- c(
  "# CIP-matching validation report — original vs v2",
  "",
  glue::glue("Generated: {format(Sys.time(), '%Y-%m-%d %H:%M')}  "),
  glue::glue("Baseline: materialized 02a tables (ticket 11, post-02a).  "),
  glue::glue("v2: spine (13) + BGS (14) + DACSO (15) + TRD (16) first runs.  "),
  glue::glue("Acceptance floor: BGS/DACSO per-PEN reduction >= {BAR_REDUCTION_PP}pp; APPSO/GRAD row-for-row; TRD preservation."),
  "",
  "## Verdicts (exit gate)",
  "",
  md_table(verdict_df %>% select(SURVEY, METRIC, V1, V2, BAR, VERDICT)),
  "",
  if (all_pass) "_All bars passed._" else "_One or more bars failed — see the intentional-diffs register for pending causes._",
  "",
  "## 1. Headline per-PEN CIP4 disagreement (metric b, primary)",
  "",
  "### BGS",
  md_table(fmt_headline(bgs)),
  "",
  "### DACSO",
  md_table(fmt_headline(dacso)),
  "",
  "## 2. Per-institution top-10 (metric c — alias / walk calibration)",
  "",
  "### BGS (VIU the historical anchor)",
  md_table(bgs_inst),
  "",
  "### DACSO (CAM the historical anchor)",
  md_table(dacso_inst),
  "",
  "## 3. Per-cycle drift",
  "",
  "### BGS (by survey year)",
  md_table(bgs_cycle),
  "",
  "### DACSO (by SUBM_CD)",
  md_table(dacso_cycle),
  "",
  "## 4. v2 MATCH_RULE distributions",
  "",
  "### BGS credential IDs",
  md_table(mr_bgs),
  "",
  "### DACSO FinalCIPs (credential programs)",
  md_table(mr_dacso),
  "",
  "### TRD survey rows",
  md_table(mr_trd),
  "",
  "## 5. BGS silent-break audit (PEN artefact, ticket 12)",
  "",
  glue::glue("- `.0`-suffix PENs in the consumed Credential_Non_Dup_BGS_IDs_r: **{artefact$n}** (gate: 0)"),
  glue::glue("- `.0`-suffix PENs in raw stp_credential_r: **{format(artefact_raw$n, big.mark=',')}** (context only -- expected; normalized at acquisition per commit de8b299)"),
  glue::glue("- Original-path BGS PEN-join rows: **{format(pen_join_n$n, big.mark=',')}** (gate: > 0; documented 2023 figure 133,952)"),
  "",
  "## 6. Diagnostics inventory (regenerated by each v2 run)",
  "",
  glue::glue("- BGS alias proposals: {n_alias_proposals} rows (bgs-v2-alias-proposals.csv)"),
  glue::glue("- BGS residual manual-review caseload: {n_bgs_residual} rows (bgs-v2-residual-manual-review.csv)"),
  glue::glue("- DACSO walked chains: {n_dacso_walk} rows (dacso-v2-walk-chains.csv)"),
  glue::glue("- TRD lag distribution: trd-v2-lag-distribution.csv"),
  "",
  "## 7. Intentional-diffs register",
  "",
  md_table(register),
  ""
)

writeLines(report, report_path)
message(Sys.time(), " | validate-cip-matching: report written to ", report_path)

print(verdict_df %>% select(SURVEY, METRIC, V1, V2, VERDICT))

dbDisconnect(con)
message(Sys.time(), " | validate-cip-matching: ", if (all_pass) "ALL PASS" else "FAILURES PRESENT")
if (!all_pass) quit(status = 1)
quit(status = 0)
