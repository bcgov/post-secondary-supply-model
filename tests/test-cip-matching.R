# Copyright 2024 Province of British Columbia
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# ******************************************************************************
# Tests for the cip-matching v2 spine (wayfinder tickets 12/13/07 resolutions).
#
# The repo deliberately has no test framework (closed SO-Oracle map, T08
# decision); this is a standalone assertion runner instead. Seams under test
# (pre-agreed in the build tickets):
#   1. normalize_stp_cip()  -- the STP CIP cleaning cascade (pure function)
#   2. normalize_pen()      -- PEN canonicalization (pure vector function)
#   3. resolve_hist_links() -- DACSO historical-link walk (pure function)
#
# All fixtures are synthetic with known-good expected values, so the tests
# never touch the database and cannot be tautological.
#
# USAGE: Rscript tests/test-cip-matching.R   (exit 0 = pass, 1 = fail)
# ******************************************************************************

failures <- character(0)
n_checks <- 0L

expect_eq <- function(actual, expected, label) {
  n_checks <<- n_checks + 1L
  ok <- isTRUE(all.equal(actual, expected, check.attributes = FALSE))
  if (!ok) {
    failures <<- c(failures, sprintf(
      "%s\n  expected: %s\n  actual:   %s",
      label,
      paste(deparse(expected), collapse = " "),
      paste(deparse(actual), collapse = " ")
    ))
    cat("FAIL ", label, "\n", sep = "")
  } else {
    cat("ok   ", label, "\n", sep = "")
  }
}

# ---- Load the spine (functions only; build section is guarded) ----
spine_path <- file.path("R", "02a-cip-normalize.R")
if (!file.exists(spine_path)) {
  stop("Spine not found at ", spine_path, " -- write the implementation first.")
}
options(cip_spine.lib_only = TRUE)
source(spine_path, local = FALSE)
options(cip_spine.lib_only = FALSE)

# ---- Fixtures: synthetic INFOWARE-shaped lookups (we control the truth) ----
cip6_fix <- data.frame(
  LCIP_CD_WITH_PERIOD = c("01.0100", "11.0100", "24.0100", "99.0101"),
  LCIP_LCP4_CD        = c("0101",    "1101",    "2401",    "9901"),
  LCIP_LCP2_CD        = c("01",      "11",      "24",      "99"),
  LCIP_LCIPPC_CD      = c("01",      "11",      "24",      "99"),
  LCIP_LCIPPC_NAME    = c("Cluster A", "Cluster B", "Cluster C", "Cluster D"),
  stringsAsFactors = FALSE
)
cip4_fix <- data.frame(
  LCP4_CD             = c("0101", "1101", "2401", "9901"),
  LCP4_CIP_4DIGITS_NAME = c("Agricultural business",
                            "Computer systems",
                            "Trades general",
                            "Other four"),
  stringsAsFactors = FALSE
)
cip2_fix <- data.frame(
  LCP2_CD        = c("01", "11", "24", "99"),
  LCP2_DIGITS_NAME = c("Agriculture", "Computer", "Trades", "Other"),
  LCP2_LCIPPC_CD   = c("01", "11", "24", "99"),
  LCP2_LCIPPC_NAME = c("Cluster A", "Cluster B", "Cluster C", "Cluster D"),
  stringsAsFactors = FALSE
)
lookups_fix <- list(cip6 = cip6_fix, cip4 = cip4_fix, cip2 = cip2_fix)

# ******************************************************************************
# Seam 1: normalize_stp_cip()
# ******************************************************************************
df <- data.frame(
  PSI_CREDENTIAL_CIP = c(
    "01.0100",   # exact match
    "11.010",    # missing trailing zero -> fixed -> exact
    "1.0100",    # missing leading zero  -> fixed -> exact
    "24.00",     # general program -> 2401
    "55.00",     # general program with no lookup backing -> 5501
    "99.0123",   # unresolvable 4-digit; 2-digit fallback via prefix 99
    "77.7777",   # unresolvable everywhere
    NA_character_
  ),
  stringsAsFactors = FALSE
)
res <- normalize_stp_cip(df, lookups = lookups_fix)

expect_eq(res$STP_CIP_CODE_4,
          c("0101", "1101", "0101", "2401", "5501", NA, NA, NA),
          "cascade: 4-digit codes resolve through the stages")
expect_eq(res$STP_CIP_CODE_2,
          c("01", "11", "01", "24", NA, "99", NA, NA),
          "cascade: 2-digit codes; prefix fallback fills 99 case")
expect_eq(res$STP_CIP_CODE_4_NAME[c(1, 2, 6)],
          c("Agricultural business", "Computer systems", "Invalid 4-digit CIP"),
          "cascade: 4-digit names; unresolvable flagged Invalid")
expect_eq(res$STP_CIP_CODE_2_NAME[1], "Agriculture", "cascade: 2-digit name join")
expect_eq(res$PSI_CREDENTIAL_CIP_orig,
          df$PSI_CREDENTIAL_CIP,
          "cascade: original CIP preserved for joins")
expect_eq(res$CIP_MATCH_RULE,
          c("exact_6digit", "exact_6digit", "exact_6digit",
            "general_program", "general_program",
            "unresolved_4digit", "unresolved_4digit", "unresolved_4digit"),
          "cascade: decision trace records the resolving stage")
expect_eq(res$STP_CIP_CLUSTER_CODE[1], "01",
          "cascade: cluster code via 6-digit representative row")
expect_eq(res$STP_CIP_CLUSTER_NAME[1], "Cluster A",
          "cascade: cluster name via 6-digit representative row")

# Input frame must not be modified (pure function).
expect_eq(df$PSI_CREDENTIAL_CIP[1], "01.0100", "cascade: input left unmodified")

# ******************************************************************************
# Seam 2: normalize_pen()
# ******************************************************************************
expect_eq(
  normalize_pen(c("123456.0", "  119795458  ", "000475391", "0", "", NA_character_)),
  c("123456", "119795458", "475391", NA_character_, NA_character_, NA_character_),
  "pen: .0 suffix stripped, whitespace trimmed, leading zeros dropped, junk->NA"
)
expect_eq(
  normalize_pen("1.17e+08"),
  "117000000",
  "pen: scientific notation expanded to integer string"
)

# ******************************************************************************
# Seam 3: resolve_hist_links() -- DACSO historical-link walk
# (lives in R/02a-dacso-program-matching-v2.R, ticket 15; gated so the runner
# stays green while only the ticket-13 spine exists)
# ******************************************************************************
dacso_v2_path <- file.path("R", "02a-dacso-program-matching-v2.R")
if (!file.exists(dacso_v2_path)) {
  cat("skip seam 3: resolve_hist_links -- R/02a-dacso-program-matching-v2.R not built yet (ticket 15)\n")
} else {
  options(cip_spine.lib_only = TRUE)
  source(dacso_v2_path, local = FALSE)
  options(cip_spine.lib_only = FALSE)

# Chains (from the frozen original's manual notes, R/02a-dacso lines 674-680):
#   10355 -> 9855 -> 115   (115 already has a resolved CIP; terminal = 115)
#   10359 -> 9856 -> 9006  (9006 resolved; terminal = 9006)
#   10383 -> 9857 -> 4960  (no node resolved -> no terminal; max depth hit)
# Cycle: 1 -> 2 -> 1 -> ... must stop safely (no terminal).
xref_fix <- data.frame(
  PRGM_ID             = c(10355, 9855, 10359, 9856, 10383, 9857, 1, 2),
  HISTORICAL_PRGM_ID  = c( 9855,  115,  9856, 9006,  9857, 4960, 2, 1),
  YEAR_LINK_CREATED   = c("C_Outc21", "C_Outc22", "C_Outc21", "C_Outc22",
                          "C_Outc21", "C_Outc22", "C_Outc21", "C_Outc21"),
  SURVEY_CODE         = rep("DACSO", 8),
  stringsAsFactors = FALSE
)
programs_fix <- data.frame(
  PRGM_ID = c(115, 9006, 10355),
  PRGM_INST_CD = c("CAM", "BCIT", "CAM"),
  PRGM_LCPC_CD = c("CENG.DIP", "CNET.CERT", "NEW.UNRESOLVED"),
  LCIP_LCP4_CD = c("1502", "1110", NA_character_),
  stringsAsFactors = FALSE
)
walked <- resolve_hist_links(xref_fix, programs_fix, max_depth = 5L)

w <- function(pid) walked$walk[walked$PRGM_ID == pid]
expect_eq(w(10355)$TERMINAL_PRGM_ID, 115,
          "walk: 10355 -> 9855 -> 115 terminates on resolved node")
expect_eq(w(10355)$CHAIN, "10355>9855>115",
          "walk: chain recorded as visited sequence")
expect_eq(w(10355)$STP_CIP_CODE_4, "1502",
          "walk: terminal node's CIP adopted")
expect_eq(w(10359)$TERMINAL_PRGM_ID, 9006,
          "walk: second chain terminates on resolved node")
expect_eq(w(10359)$STP_CIP_CODE_4, "1110", "walk: terminal CIP adopted (9006)")
expect_eq(is.na(w(10383)$TERMINAL_PRGM_ID), TRUE,
          "walk: unresolved terminal yields NA (max-depth safety net)")
expect_eq(is.na(w(1)$TERMINAL_PRGM_ID), TRUE,
          "walk: cycles terminate safely via max depth")
expect_eq(is.na(w(9006)$TERMINAL_PRGM_ID), TRUE,
          "walk: already-resolved programs are not re-walked")

# Ambiguity: two historical links from one program; latest YEAR_LINK_CREATED wins.
xref_amb <- data.frame(
  PRGM_ID            = c(50, 50),
  HISTORICAL_PRGM_ID = c(60, 61),
  YEAR_LINK_CREATED  = c("C_Outc21", "C_Outc23"),  # 61 is the later link
  SURVEY_CODE        = c("DACSO", "DACSO"),
  stringsAsFactors = FALSE
)
prog_amb <- data.frame(
  PRGM_ID = c(60, 61), PRGM_INST_CD = c("CAM", "CAM"),
  PRGM_LCPC_CD = c("A.DIP", "B.DIP"),
  LCIP_LCP4_CD = c("6001", "6101"),
  stringsAsFactors = FALSE
)
amb <- resolve_hist_links(xref_amb, prog_amb, max_depth = 5L)
expect_eq(amb$walk$TERMINAL_PRGM_ID[amb$walk$PRGM_ID == 50], 61,
          "walk: latest YEAR_LINK_CREATED wins on ambiguity")
} # end seam 3 gate

# ---- Summary ----
cat("\n", sum(n_checks - length(failures)), "/", n_checks,
    " checks passed\n", sep = "")
if (length(failures) > 0) {
  cat("\nFailures:\n")
  cat(paste0(seq_along(failures), ". ", failures, collapse = "\n\n"), "\n")
  quit(status = 1)
}
quit(status = 0)
