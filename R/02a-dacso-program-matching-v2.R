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
# 02a-dacso-program-matching-v2.R  --  DACSO program matching, v2 (parallel)
#
# ------------------------------------------------------------------------------
# WHERE THIS SITS IN THE MODEL
# ------------------------------------------------------------------------------
# PSSM computes one formula (docs/project-summary-for-new-analyst.md §2):
#
#   OCCSN(NOC) = GRADUATES(cred,age) x P(CIP|cred,age)
#              x P(in labour supply|CIP) x P(NOC|CIP,region)
#
# The Credential_Non_Dup_Programs_DACSO_FinalCIPs_v2_r table this script writes
# is the FIRST and richest source in 02a-update-cred-non-dup's priority chain
# (DACSO > BGS > GRAD > APPSO > STP fallback -- a 7-column business-key join),
# so its CIP decisions reach credential_non_dup_r and then the `LCIP4_CRED`
# cohort key in 02b-1 (P(CIP|cred,age), Module 06) ahead of every other
# matching source.
#
# ------------------------------------------------------------------------------
# WHY A v2 (and what is deliberately different from the frozen original)
# ------------------------------------------------------------------------------
# The frozen original (R/02a-dacso-program-matching.R) works, but its Part 1
# XWALK maintenance is per-cycle copy-paste blocks plus HAND-WALKED recursive
# chains: for each cycle C_Outc21..25 an analyst filters the year's programs,
# joins the XREF historical links, eyeballs the registry NOTES, and hand-writes
# `PRGM_ID == <id> ~ "<value>"` overrides (the 2021 3119 block, the 2022
# 115/9006/131/5952/9008/4960/117 block, the 2023 9237/1158/9810 block).
# Per the cip-matching wayfinder map (tickets 07, 08, 15) this v2:
#
#   1. AUTO-WALK (ticket 07): ONE parameterized walk replaces every per-cycle
#      block. `resolve_hist_links()` (pure, unit-tested -- seam 3 of
#      tests/test-cip-matching.R) chases PRGM_ID -> HISTORICAL_PRGM_ID chains
#      with terminate-on-landed semantics (stop at the first node already
#      resolved), a max-depth-5 safety net, latest-YEAR_LINK_CREATED
#      disambiguation with PRGM_INST_CD tie-break, and the terminal node's
#      current registry coding winning. The registry NOTES column is advisory
#      output only (surfaced in the diagnostics export, never parsed).
#   2. OVERRIDES AS CLASSIFIED DATA (ticket 08): every hardcoded override in
#      the original is classified against what the auto-walk actually
#      produces: (i) reproducible -> not carried into v2; (ii) genuinely
#      exceptional -> dbo.T_DACSO_PRGM_EXCEPTIONS_v2_r with rationale, still
#      applied; (iii) rule extension -> the walk rule subsuming it is recorded.
#      The frozen original's inline comments + commit history remain the audit
#      trail of the past manual decisions.
#   3. MATCH_RULE TRACE: every credential-program row leaves with a MATCH_RULE
#      naming the matching pass that assigned its outcomes CIP (the harness,
#      ticket 17, consumes these).
#
# The original stays frozen and runnable (baseline anchor); v2 writes only
# distinctly-named `_v2_r` tables that coexist with it.
#
# ------------------------------------------------------------------------------
# WALK SEMANTICS (ticket 07 resolution, encoded in tests/test-cip-matching.R)
# ------------------------------------------------------------------------------
# Terminology: the registry (INFOWARE_PROGRAMS) carries one row per program
# identity PRGM_ID. When an institution retires/re-codes a program, the new
# PRGM_ID links to its predecessor via INFOWARE_PROGRAMS_HIST_PRGMID_XREF
# (PRGM_ID -> HISTORICAL_PRGM_ID). Chains can be multi-hop
# (10355 -> 9855 -> 115). "Resolved" means the node already carries a CIP4:
# either registry-coded (LCIP_LCP4_CD) or present in the seed XWALK.
#
# For each UNRESOLVED program with a link, the walk follows the chain until it
# LANDS on a resolved node (that node is the terminal -- its current registry
# coding wins), or exhausts max depth / dead-ends (terminal NA -- these become
# the residual diagnostics, the only place an analyst may still need NOTES).
#
# ------------------------------------------------------------------------------
# MATCH_RULE VOCABULARY (credential-program rows, Part 2-4 port)
# ------------------------------------------------------------------------------
#   already_matched_psi   XWALK hit on PSI_CODE + program code + description
#   already_matched_coci  XWALK hit on COCI_INST_CD + program code + desc
#   new_auto_match_psi    new program matched to XWALK STP-side keys (PSI)
#   new_auto_match_coci   same, COCI-keyed
#   inst_rule_bcit/_capu/_viu  institution-specific code-normalization match
#   catch_all_code        XWALK hit on institution + program code only
#   catch_all_desc        XWALK hit on institution + program description only
#   stp_fallback          no XWALK match -> INFOWARE taxonomy CIP from the
#                         credential's own PSI_CREDENTIAL_CIP
#
# ------------------------------------------------------------------------------
# INPUTS
# ------------------------------------------------------------------------------
#   dbo.INFOWARE_PROGRAMS                      registry (CIP2021-coded)
#   dbo.INFOWARE_L_CIP_{6,4,2}DIGITS_CIP2021   CIP2021 taxonomy lookups
#   dbo.INFOWARE_PROGRAMS_HIST_PRGMID_XREF     historical program links
#   LAN seed CSV (config lan_program_mathcing / DACSO/tmp-data/2023/
#     DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23.csv) -- the cold-start XWALK
#     (DB seed tables retired with the CIP2021 migration)
#   [my_schema].credential_non_dup_r           STP credentials (DACSO bucket)
#   [my_schema].Credential_Non_Dup_STP_CIP_Cleaning_v2_r   the v2 spine
#     (NOT used for the XWALK walk -- the registry's own LCIP_LCP4_CD is the
#     coding source here; the spine serves the other v2 scripts)
#
# ------------------------------------------------------------------------------
# OUTPUTS (fresh names; originals untouched)
# ------------------------------------------------------------------------------
#   [my_schema].DACSO_STP_ProgramsCIP4_XWALK_ALL_v2_r
#   [my_schema].Credential_Non_Dup_Programs_DACSO_FinalCIPs_v2_r
#   dbo.T_DACSO_PRGM_EXCEPTIONS_v2_r           classified exceptional
#       overrides (seeded only when non-empty; analyst-maintained after)
#   .scratch/cip-matching/diagnostics/dacso-v2-walk-chains.csv
#       every walked chain + terminal + adopted coding (audit artifact)
#   .scratch/cip-matching/diagnostics/dacso-v2-override-classification.csv
#       the per-override (i)/(ii)/(iii) classification with values compared
# ==============================================================================

library(tidyverse)
library(odbc)
library(DBI)
library(dbplyr)
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

# ==============================================================================
# Join-key helpers (ported verbatim from the frozen original -- the same
# normalization rules, so v2 and original business keys are comparable)
# ==============================================================================
norm_chr <- function(x) {
  x <- as.character(x)
  x <- trimws(x)
  toupper(x)
}

add_join_keys <- function(df, mappings) {
  if (length(mappings) == 0) {
    return(df)
  }
  for (new_nm in names(mappings)) {
    src_nm <- unname(mappings[[new_nm]])
    if (src_nm %in% names(df)) {
      df[[new_nm]] <- norm_chr(df[[src_nm]])
    }
  }
  df
}

drop_join_keys <- function(df) {
  df %>% select(-matches("_KEY$"))
}

refresh_programs_join_keys <- function(df) {
  add_join_keys(df, c(
    PRGM_INST_CD_KEY = "PRGM_INST_CD",
    PRGM_LCPC_CD_KEY = "PRGM_LCPC_CD",
    PRGM_INST_PROGRAM_NAME_KEY = "PRGM_INST_PROGRAM_NAME"
  ))
}

refresh_xwalk_join_keys <- function(df) {
  add_join_keys(df, c(
    PSI_CODE_KEY = "PSI_CODE",
    COCI_INST_CD_KEY = "COCI_INST_CD",
    PSI_PROGRAM_CODE_KEY = "PSI_PROGRAM_CODE",
    PRGM_LCPC_CD_KEY = "PRGM_LCPC_CD",
    PSI_CREDENTIAL_PROGRAM_DESC_KEY = "PSI_CREDENTIAL_PROGRAM_DESC",
    PRGM_INST_PROGRAM_NAME_KEY = "PRGM_INST_PROGRAM_NAME"
  ))
}

refresh_stp_join_keys <- function(df) {
  add_join_keys(df, c(
    PSI_CODE_KEY = "PSI_CODE",
    COCI_INST_CD_KEY = "COCI_INST_CD",
    PSI_PROGRAM_CODE_KEY = "PSI_PROGRAM_CODE",
    PSI_CREDENTIAL_PROGRAM_DESCRIPTION_KEY = "PSI_CREDENTIAL_PROGRAM_DESCRIPTION"
  ))
}

# ==============================================================================
# resolve_hist_links()  --  the auto-XWALK walk (ticket 07; test seam 3)
# ==============================================================================
# WHAT: pure function. Given the XREF link table and a programs frame, walk
#       every UNRESOLVED program's PRGM_ID -> HISTORICAL_PRGM_ID chain until
#       it lands on a resolved node (non-NA LCIP_LCP4_CD in `programs`), a
#       dead-end, or max_depth hops.
# WHY:  replaces the original's per-cycle blocks + hand-walked chains. One
#       parameterized walk is generic across all cycles because the chain
#       structure itself encodes the lineage -- no year filtering needed
#       (YEAR_LINK_CREATED only breaks ambiguity).
# HOW:  - resolution: programs rows with non-NA LCIP_LCP4_CD are terminal-
#         eligible; they are returned as walk rows with NA terminal ("already
#         resolved, not re-walked").
#       - disambiguation when a PRGM_ID has several outgoing links: latest
#         YEAR_LINK_CREATED wins; ties prefer a PRGM_INST_CD match to the
#         originating program (when both frames carry PRGM_INST_CD); final
#         tie-break is the smallest HISTORICAL_PRGM_ID for determinism.
#       - returns list(walk = tibble(PRGM_ID, TERMINAL_PRGM_ID, CHAIN,
#         STP_CIP_CODE_4)): TERMINAL_PRGM_ID/STP_CIP_CODE_4 filled only for
#         walked-and-landed programs; CHAIN records the visited sequence
#         "a>b>c".
resolve_hist_links <- function(xref, programs, max_depth = 5L) {
  stopifnot(
    all(c("PRGM_ID", "HISTORICAL_PRGM_ID", "YEAR_LINK_CREATED") %in% names(xref)),
    all(c("PRGM_ID", "LCIP_LCP4_CD") %in% names(programs))
  )
  # Internally the walk runs on character keys (chain nodes may be numeric in
  # one frame and character in another); the OUTPUT echoes the input types --
  # PRGM_ID / TERMINAL_PRGM_ID come back exactly as the caller passed them.
  orig_pool <- c(xref$PRGM_ID, xref$HISTORICAL_PRGM_ID, programs$PRGM_ID)
  pool_chr <- as.character(orig_pool)
  uni_chr <- unique(pool_chr)
  universe <- orig_pool[match(uni_chr, pool_chr)]   # original-typed, unique
  to_orig <- setNames(universe, uni_chr)

  xref_chr <- xref %>%
    transmute(
      PRGM_ID = as.character(PRGM_ID),
      HISTORICAL_PRGM_ID = as.character(HISTORICAL_PRGM_ID),
      YEAR_LINK_CREATED,
      across(any_of("PRGM_INST_CD"))
    )
  programs_chr <- programs %>%
    transmute(
      PRGM_ID = as.character(PRGM_ID),
      PRGM_INST_CD = if ("PRGM_INST_CD" %in% names(programs)) PRGM_INST_CD else NA_character_,
      LCIP_LCP4_CD = as.character(LCIP_LCP4_CD)
    )

  # One deterministic outgoing link per PRGM_ID (disambiguated):
  # latest YEAR_LINK_CREATED wins; ties prefer a PRGM_INST_CD match to the
  # originating program (when both frames carry it); final tie-break the
  # smallest HISTORICAL_PRGM_ID.
  has_inst <- "PRGM_INST_CD" %in% names(xref) &&
    "PRGM_INST_CD" %in% names(programs)
  if (has_inst) {
    link_key <- xref_chr %>%
      left_join(
        programs_chr %>% select(PRGM_ID, ORIG_INST = PRGM_INST_CD),
        by = "PRGM_ID"
      ) %>%
      mutate(INST_TIEBREAK = coalesce(ORIG_INST == PRGM_INST_CD, FALSE))
  } else {
    link_key <- xref_chr %>% mutate(INST_TIEBREAK = FALSE)
  }
  link_key <- link_key %>%
    mutate(HIST_NUM = suppressWarnings(as.numeric(HISTORICAL_PRGM_ID))) %>%
    arrange(
      PRGM_ID,
      desc(YEAR_LINK_CREATED),
      desc(INST_TIEBREAK),
      HIST_NUM,
      HISTORICAL_PRGM_ID
    ) %>%
    distinct(PRGM_ID, .keep_all = TRUE)
  links <- setNames(as.list(link_key$HISTORICAL_PRGM_ID), link_key$PRGM_ID)

  ok <- !is.na(programs_chr$LCIP_LCP4_CD) & programs_chr$LCIP_LCP4_CD != ""
  resolved_cip <- setNames(programs_chr$LCIP_LCP4_CD[ok], programs_chr$PRGM_ID[ok])
  resolved_ids <- names(resolved_cip)

  # Walk universe: every program that appears on either side of a link, union
  # the programs frame. Resolved programs are returned as rows with NA
  # terminal ("already resolved, not re-walked").
  walk_one <- function(pid) {
    pid_chr <- as.character(pid)
    if (pid_chr %in% resolved_ids) {
      return(list(NA_character_, NA_character_, NA_character_))
    }
    chain <- pid_chr
    cur <- pid_chr
    for (hop in seq_len(max_depth)) {
      nxt <- links[[cur]]
      if (is.null(nxt) || is.na(nxt)) break            # dead end
      chain <- c(chain, nxt)
      if (nxt %in% resolved_ids) {                     # landed
        return(list(nxt, paste(chain, collapse = ">"), resolved_cip[[nxt]]))
      }
      cur <- nxt
    }
    list(NA_character_, paste(chain, collapse = ">"), NA_character_)
  }

  results <- lapply(universe, walk_one)
  term_chr <- vapply(results, `[[`, character(1), 1, USE.NAMES = FALSE)
  walk <- data.frame(
    PRGM_ID = universe,
    TERMINAL_PRGM_ID = ifelse(is.na(term_chr), NA, to_orig[term_chr]),
    CHAIN = vapply(results, `[[`, character(1), 2, USE.NAMES = FALSE),
    STP_CIP_CODE_4 = vapply(results, `[[`, character(1), 3, USE.NAMES = FALSE),
    stringsAsFactors = FALSE,
    row.names = NULL
  )
  list(walk = walk)
}

# ==============================================================================
# Pipeline (skipped under cip_spine.lib_only -- the unit tests source this
# file as a library)
# ==============================================================================
if (!isTRUE(getOption("cip_spine.lib_only"))) {
  log_info("==== 02a-dacso-program-matching-v2.R START ====")

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

  cycles <- c("C_Outc21", "C_Outc22", "C_Outc23", "C_Outc24", "C_Outc25")

  # ----------------------------------------------------------------------------
  # STEP 1: Load the registry, links, seed XWALK, and CIP lookups
  # ----------------------------------------------------------------------------
  # programs_table: the registry joined to the CIP2021 6-digit lookup (on the
  # no-period key LCP6_CD) and the 4-digit lookup -- same construction as the
  # migrated original (aliased to the legacy column names so the value flow is
  # identical; the values carried are CIP2021).
  programs_table <- sch_tbl("INFOWARE_PROGRAMS", schema = shareschema) %>%
    inner_join(
      sch_tbl("INFOWARE_L_CIP_6DIGITS_CIP2021", schema = shareschema),
      by = c("LCIP_CD_CIP2021" = "LCP6_CD")
    ) %>%
    inner_join(
      sch_tbl("INFOWARE_L_CIP_4DIGITS_CIP2021", schema = shareschema),
      by = c("LCIP_LCP4_CD" = "LCP4_CD")
    ) %>%
    select(
      PRGM_ID,
      PRGM_FIRST_SEEN_SUBM_CD,
      PRGM_INST_CD,
      PRGM_INST_PROGRAM_NAME,
      PRGM_INST_PROGRAM_NAME_CLEANED,
      PRGM_LCPC_CD,
      PRGM_TTRAIN_FLAG,
      LCIP_CD_CIP2016 = LCIP_CD_CIP2021,
      LCIP_NAME_CIP2016 = LCIP_NAME_CIP2021,
      PRGM_CREDENTIAL,
      NOTES,
      HAS_HISTORICAL_PRGM_ID_LINK,
      CIP_CLUSTER_ARTS_APPLIED,
      DACSO_OLD_PRGM_ID_DO_NOT_USE,
      DUP_PROGRAM_USE_THIS_PRGM_ID,
      LCIP_LCP4_CD,
      LCP4_CIP_4DIGITS_NAME = LCP4_DIGITS_NAME
    ) %>%
    collect() %>%
    # PRGM_ID as character everywhere: the seed CSV (type.convert-typed),
    # the registry (numeric-typed) and the walk output must join cleanly.
    mutate(PRGM_ID = as.character(PRGM_ID)) %>%
    refresh_programs_join_keys()
  log_info(glue::glue(
    "Step 1: programs_table from INFOWARE: {nrow(programs_table)} programs"
  ))

  # Historical links (the walk chases DACSO lineage only -- the XREF also
  # carries other surveys' links, which must not route DACSO chains astray;
  # same filter the original's per-cycle blocks applied, now in one place).
  xref <- sch_tbl("INFOWARE_PROGRAMS_HIST_PRGMID_XREF", schema = shareschema) %>%
    filter(SURVEY_CODE == "DACSO") %>%
    collect()

  # Seed XWALK from the LAN CSV (same cold-start the original uses -- DB seed
  # tables retired with the CIP2021 migration): drop the previous run's
  # Updated_DACSO_* flag columns (this walk re-derives current-state provenance
  # in WALK_* columns instead), zero-pad the CIP, then recode shared business
  # keys to the current CIP2021 registry coding (unambiguous keys only; the
  # registry wins -- same user decision the original encodes).
  seed <- read.csv(
    file.path(
      config::get("lan_program_mathcing"),
      "DACSO", "tmp-data", "2023",
      "DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23.csv"
    ),
    colClasses = "character",
    check.names = FALSE,
    na.strings = c("", "NA")
  ) %>%
    type.convert(as.is = TRUE) %>%
    tibble::as_tibble() %>%
    mutate(
      PRGM_ID = as.character(PRGM_ID),
      CIP_CODE_4 = str_pad(as.character(CIP_CODE_4), 4, "left", "0")
    ) %>%
    select(-matches("^Updated_DACSO_")) %>%
    # Ensure STP-side columns exist (older seeds may not carry them; the
    # Add-to-XWALK step writes into them)
    {
      d <- .
      for (col in c("STP_CIP4_CODE", "STP_CIP4_NAME", "New_STP_Program2021_23")) {
        if (!col %in% names(d)) d[[col]] <- NA_character_
      }
      d
    } %>%
    refresh_xwalk_join_keys()

  reg_recode_map <- programs_table %>%
    distinct(PRGM_INST_CD_KEY, PRGM_LCPC_CD_KEY, LCIP_LCP4_CD) %>%
    group_by(PRGM_INST_CD_KEY, PRGM_LCPC_CD_KEY) %>%
    filter(n_distinct(LCIP_LCP4_CD) == 1) %>%
    ungroup()

  xwalk <- seed %>%
    left_join(
      reg_recode_map %>% rename(REG_CIP4 = LCIP_LCP4_CD),
      by = c(
        "COCI_INST_CD_KEY" = "PRGM_INST_CD_KEY",
        "PRGM_LCPC_CD_KEY" = "PRGM_LCPC_CD_KEY"
      )
    ) %>%
    mutate(CIP_CODE_4 = ifelse(!is.na(REG_CIP4), REG_CIP4, CIP_CODE_4)) %>%
    select(-REG_CIP4) %>%
    mutate(WALK_RULE = "seed_registry_recode")
  log_info(glue::glue(
    "Step 1: seed XWALK loaded: {nrow(xwalk)} rows ({sum(xwalk$WALK_RULE == 'seed_registry_recode' & !is.na(xwalk$CIP_CODE_4))} with CIP)"
  ))

  # CIP lookups (names / 2-digit / clusters for Part 4)
  cip6 <- sch_tbl("INFOWARE_L_CIP_6DIGITS_CIP2021", schema = shareschema) %>%
    collect() %>% rename_with(toupper)
  cip4_ref <- sch_tbl("INFOWARE_L_CIP_4DIGITS_CIP2021", schema = shareschema) %>%
    collect() %>%
    rename(LCP4_CIP_4DIGITS_NAME = LCP4_DIGITS_NAME) %>%
    rename_with(toupper)
  cip2_ref <- sch_tbl("INFOWARE_L_CIP_2DIGITS_CIP2021", schema = shareschema) %>%
    collect() %>% rename_with(toupper)

  # ----------------------------------------------------------------------------
  # STEP 2: Add new DACSO programs WITHOUT historical links (all cycles, one
  # parameterized bind -- no per-cycle blocks)
  # ----------------------------------------------------------------------------
  new_no_link <- programs_table %>%
    filter(
      PRGM_FIRST_SEEN_SUBM_CD %in% cycles &
        (is.na(HAS_HISTORICAL_PRGM_ID_LINK) | HAS_HISTORICAL_PRGM_ID_LINK == " ")
    )
  xwalk <- xwalk %>%
    bind_rows(
      new_no_link %>%
        mutate(
          New_DACSO_Program2021_23 = case_when(
            PRGM_FIRST_SEEN_SUBM_CD == "C_Outc21" ~ "Yes2021",
            PRGM_FIRST_SEEN_SUBM_CD == "C_Outc22" ~ "Yes2022",
            PRGM_FIRST_SEEN_SUBM_CD == "C_Outc23" ~ "Yes2023",
            PRGM_FIRST_SEEN_SUBM_CD == "C_Outc24" ~ "Yes2024",
            PRGM_FIRST_SEEN_SUBM_CD == "C_Outc25" ~ "Yes2025",
            TRUE ~ NA_character_
          ),
          WALK_RULE = "new_program_no_link"
        ) %>%
        transmute(
          COCI_INST_CD = PRGM_INST_CD,
          PRGM_LCPC_CD,
          PRGM_INST_PROGRAM_NAME,
          CIP_CODE_4 = LCIP_LCP4_CD,
          LCP4_CIP_4DIGITS_NAME,
          PRGM_ID,
          PRGM_CREDENTIAL,
          New_DACSO_Program2021_23,
          WALK_RULE
        )
    ) %>%
    refresh_xwalk_join_keys()
  log_info(glue::glue(
    "Step 2: added {nrow(new_no_link)} new DACSO programs without historical links"
  ))

  # ----------------------------------------------------------------------------
  # STEP 3: THE AUTO-WALK (ticket 07) -- one parameterized pass over every
  # with-links program, replacing the original's per-cycle blocks and their
  # hand-walked chain overrides
  # ----------------------------------------------------------------------------
  # LANDING semantics (from the original's hand-walked chains, e.g.
  # "10355 -> 9855 -> 115 (update CIP and CPC to most recent)" and
  # "10359 -> 9856 -> 9006 -> 4760 (4760 doesn't exist in XWALK, update
  # 9006)"): walk from the NEW program; a chain LANDS on the first node
  # PRESENT IN THE XWALK. Registry-coded nodes not in the XWALK are mere
  # waypoints (9856, 4760) -- the walk continues past them. The VALUE that
  # wins is the NEWEST program's coding (the walk start), applied onto the
  # landed XWALK row -- that is what the original's "to most recent" meant.
  # The pure function's resolution marker is therefore: XWALK-present
  # PRGM_IDs carry their XWALK CIP; everything else is unresolved.
  xwalk_prgm_cip <- xwalk %>%
    filter(!is.na(PRGM_ID), !is.na(CIP_CODE_4)) %>%
    distinct(PRGM_ID, XWALK_CIP4 = CIP_CODE_4)

  programs_for_walk <- programs_table %>%
    select(PRGM_ID, PRGM_INST_CD, PRGM_LCPC_CD, PRGM_INST_PROGRAM_NAME) %>%
    left_join(xwalk_prgm_cip, by = "PRGM_ID") %>%
    rename(LCIP_LCP4_CD = XWALK_CIP4)

  # Which programs does the walk need to RESOLVE? The original's per-cycle
  # blocks targeted programs first seen in-cycle WITH links whose business key
  # was missing from the XWALK. Generic form: every with-links program whose
  # own business key is not already in the XWALK (the anti-join the original
  # recomputed per cycle), regardless of first-seen year -- the chain knows
  # its lineage.
  with_links <- programs_table %>%
    filter(HAS_HISTORICAL_PRGM_ID_LINK == "Y") %>%
    refresh_programs_join_keys() %>%
    anti_join(
      xwalk %>% select(COCI_INST_CD_KEY, PRGM_LCPC_CD_KEY, PRGM_INST_PROGRAM_NAME_KEY),
      by = c(
        "PRGM_INST_CD_KEY" = "COCI_INST_CD_KEY",
        "PRGM_LCPC_CD_KEY" = "PRGM_LCPC_CD_KEY",
        "PRGM_INST_PROGRAM_NAME_KEY" = "PRGM_INST_PROGRAM_NAME_KEY"
      )
    )

  walked <- resolve_hist_links(xref, programs_for_walk, max_depth = 5L)
  walk_res <- walked$walk %>%
    # the function echoes input types (numeric PRGM_IDs); the pipeline's
    # joins run on character keys throughout
    mutate(
      PRGM_ID = as.character(PRGM_ID),
      TERMINAL_PRGM_ID = as.character(TERMINAL_PRGM_ID)
    )

  # Audit artifact: every walked (unresolved-start) chain and its outcome.
  walk_chains <- walk_res %>%
    filter(str_detect(CHAIN, ">")) %>%
    left_join(
      programs_table %>% select(PRGM_ID, PRGM_INST_CD, PRGM_FIRST_SEEN_SUBM_CD),
      by = "PRGM_ID"
    )
  write.csv(walk_chains, file.path(diag_dir, "dacso-v2-walk-chains.csv"), row.names = FALSE)
  log_info(glue::glue(
    "Step 3: auto-walk: {nrow(with_links)} with-links programs missing from XWALK; walk universe {nrow(walk_res)} nodes; {sum(!is.na(walk_res$TERMINAL_PRGM_ID))} chains landed on an XWALK terminal"
  ))

  # ----------------------------------------------------------------------------
  # STEP 4: Apply walk results to the XWALK
  # ----------------------------------------------------------------------------
  # Two application cases, mirroring what the original's manual blocks did:
  #   (a) chain lands on an XWALK-present terminal -> refresh that XWALK row's
  #       CIP/CPC/name to the NEWEST program's (walk start's) registry coding
  #       -- the 2022 115/9006/131/... overrides were exactly this, by hand;
  #   (b) chain dead-ends (no XWALK node anywhere in the lineage) -> add the
  #       start program's registry info as a NEW XWALK row keyed by the
  #       chain's LAST node (the original's 2023 10493->9810 add-missing case,
  #       now a rule).
  registry_coding <- programs_table %>%
    select(
      PRGM_ID,
      REG_CIP4 = LCIP_LCP4_CD,
      REG_CPC = PRGM_LCPC_CD,
      REG_NAME = PRGM_INST_PROGRAM_NAME,
      REG_CIP4_NAME = LCP4_CIP_4DIGITS_NAME,
      REG_INST = PRGM_INST_CD
    ) %>%
    distinct(PRGM_ID, .keep_all = TRUE)

  # restrict to chains whose START is one of the with-links programs missing
  # from the XWALK (the population the original walked cycle by cycle)
  landed <- walk_res %>%
    filter(!is.na(TERMINAL_PRGM_ID)) %>%
    inner_join(with_links %>% select(PRGM_ID, PRGM_FIRST_SEEN_SUBM_CD), by = "PRGM_ID") %>%
    # the newest program's coding (walk start)
    left_join(
      registry_coding %>% select(
        PRGM_ID,
        NEW_CIP4 = REG_CIP4, NEW_CPC = REG_CPC,
        NEW_NAME = REG_NAME, NEW_CIP4_NAME = REG_CIP4_NAME
      ),
      by = "PRGM_ID"
    )

  # (a) refresh the landed XWALK row with the start program's coding
  xwalk_terminals <- landed %>%
    filter(TERMINAL_PRGM_ID %in% xwalk$PRGM_ID) %>%
    distinct(TERMINAL_PRGM_ID, .keep_all = TRUE)

  xwalk <- xwalk %>%
    left_join(
      xwalk_terminals %>%
        transmute(
          PRGM_ID = TERMINAL_PRGM_ID,
          W_CIP4 = NEW_CIP4, W_CPC = NEW_CPC,
          W_NAME = NEW_NAME, W_CIP4_NAME = NEW_CIP4_NAME
        ),
      by = "PRGM_ID"
    ) %>%
    mutate(
      CIP_CODE_4 = ifelse(!is.na(W_CIP4), W_CIP4, CIP_CODE_4),
      LCP4_CIP_4DIGITS_NAME = ifelse(!is.na(W_CIP4_NAME), W_CIP4_NAME, LCP4_CIP_4DIGITS_NAME),
      PRGM_LCPC_CD = ifelse(!is.na(W_CPC), W_CPC, PRGM_LCPC_CD),
      PRGM_INST_PROGRAM_NAME = ifelse(!is.na(W_NAME), W_NAME, PRGM_INST_PROGRAM_NAME),
      WALK_RULE = ifelse(
        !is.na(W_CIP4), "auto_walk_terminal_refresh", WALK_RULE
      )
    ) %>%
    select(-W_CIP4, -W_CPC, -W_NAME, -W_CIP4_NAME)

  # (b) add missing terminals: dead-end chains get a new row keyed by the
  # chain's LAST node, carrying the start program's registry info
  dead_ends <- walk_res %>%
    filter(is.na(TERMINAL_PRGM_ID), str_detect(CHAIN, ">")) %>%
    inner_join(with_links %>% select(PRGM_ID), by = "PRGM_ID") %>%
    mutate(LAST_NODE = sub(".*>", "", CHAIN)) %>%
    filter(!(LAST_NODE %in% xwalk$PRGM_ID)) %>%
    distinct(LAST_NODE, .keep_all = TRUE) %>%
    left_join(
      registry_coding %>% select(
        PRGM_ID, NEW_CIP4 = REG_CIP4, NEW_CPC = REG_CPC,
        NEW_NAME = REG_NAME, NEW_CIP4_NAME = REG_CIP4_NAME, NEW_INST = REG_INST
      ),
      by = "PRGM_ID"
    )
  if (nrow(dead_ends) > 0) {
    xwalk <- xwalk %>%
      bind_rows(
        dead_ends %>%
          transmute(
            COCI_INST_CD = NEW_INST,
            PRGM_LCPC_CD = NEW_CPC,
            PRGM_INST_PROGRAM_NAME = NEW_NAME,
            CIP_CODE_4 = NEW_CIP4,
            LCP4_CIP_4DIGITS_NAME = NEW_CIP4_NAME,
            PRGM_ID = LAST_NODE,
            New_DACSO_Program2021_23 = NA_character_,
            WALK_RULE = "auto_walk_add_missing_terminal"
          )
      )
  }
  xwalk <- xwalk %>% refresh_xwalk_join_keys()
  log_info(glue::glue(
    "Step 4: walk applied: {nrow(xwalk_terminals)} in-XWALK terminals refreshed; {nrow(dead_ends)} missing terminals added"
  ))

  # ----------------------------------------------------------------------------
  # STEP 5: Classify the original's hardcoded overrides (ticket 08)
  # ----------------------------------------------------------------------------
  # Reference: every PRGM_ID-keyed override the frozen original hardcodes,
  # transcribed with the field it set. Classification against what the v2 walk
  # produced: (i) the walk reproduces the value -> dropped; (ii) the walk
  # produced a different value (or nothing) -> exceptional, seeded to
  # dbo.T_DACSO_PRGM_EXCEPTIONS_v2_r WITH the original's value applied so v2
  # never silently loses a past decision; (iii) covered by a walk rule
  # extension (the add-missing rule) -> recorded with the subsuming rule.
  override_reference <- tribble(
    ~PRGM_ID, ~FIELD, ~ORIG_VALUE, ~CYCLE, ~ORIG_NOTE,
    "3119",  "CIP_CODE_4", "1907", "2021", "NOTES review: 10132 links to 3119; EACSW/1907 Education Assistant and Community Support Worker",
    "3119",  "PRGM_LCPC_CD", "EACSW", "2021", "same block",
    "115",   "CIP_CODE_4", "1502", "2022", "chain 10355->9855->115 (update CIP and CPC to most recent)",
    "115",   "PRGM_LCPC_CD", "CENG.DIP", "2022", "same block",
    "9006",  "CIP_CODE_4", "1110", "2022", "chain 10359->9856->9006->4760 (4760 not in XWALK; update 9006)",
    "9006",  "PRGM_LCPC_CD", "CNET.CERT", "2022", "same block",
    "131",   "CIP_CODE_4", "5001", "2022", "chain 10399->9861->131",
    "131",   "PRGM_LCPC_CD", "VART.DIP", "2022", "same block",
    "5952",  "PRGM_LCPC_CD", "ECENG.RE.DIP", "2022", "chain 10366->9859->5952->116 (116 not in XWALK; update 5952 CPC)",
    "9008",  "PRGM_LCPC_CD", "ECENG.UVIC.ADIP", "2022", "chain 10367->9858->9008",
    "4960",  "PRGM_LCPC_CD", "ICS.DIP", "2022", "chain 10383->9857->4960",
    "117",   "PRGM_LCPC_CD", "MENG.DIP", "2022", "chain 10387->9860->117",
    "9237",  "PRGM_LCPC_CD", "BCPRPC", "2023", "chain 10413->9841->9237->9017->6036 (9017/6036 not in XWALK; update 9237)",
    "1158",  "PRGM_LCPC_CD", "DIPLHKIN", "2023", "chain 10475->1158",
    "9810",  "ADD_MISSING", "add 10493 info as 9810", "2023", "chain 10493->9810 (9810 not in XWALK)"
  )

  xwalk_now <- xwalk %>%
    filter(!is.na(PRGM_ID)) %>%
    distinct(PRGM_ID, .keep_all = TRUE)

  classification <- override_reference %>%
    mutate(PRGM_ID = as.character(PRGM_ID)) %>%
    left_join(
      xwalk_now %>% select(
        PRGM_ID, V2_CIP4 = CIP_CODE_4, V2_CPC = PRGM_LCPC_CD, V2_WALK_RULE = WALK_RULE
      ),
      by = "PRGM_ID"
    ) %>%
    rowwise() %>%
    mutate(
      V2_VALUE = case_when(
        FIELD == "CIP_CODE_4" ~ V2_CIP4,
        FIELD == "PRGM_LCPC_CD" ~ V2_CPC,
        FIELD == "ADD_MISSING" ~ V2_WALK_RULE
      ),
      CLASSIFICATION = case_when(
        !is.na(V2_VALUE) & V2_VALUE == ORIG_VALUE ~
          "(i) reproducible by auto-walk",
        FIELD == "ADD_MISSING" & !is.na(V2_WALK_RULE) ~
          "(i) present in v2 XWALK (carried by seed)",
        FIELD == "ADD_MISSING" & V2_WALK_RULE == "auto_walk_add_missing_terminal" ~
          "(iii) rule-extension: auto_walk_add_missing_terminal",
        # The walk refreshed this row (auto_walk_terminal_refresh writes the
        # NEWEST incarnation's current registry coding). A differing original
        # value is dictionary evolution since the original's manual edit --
        # the rule reproduces; keep the current value (NOT an exception).
        V2_WALK_RULE == "auto_walk_terminal_refresh" & !is.na(V2_VALUE) ~
          "(iii) rule reproduced: refreshed to newest incarnation's current coding (original value predates registry evolution)",
        !is.na(V2_VALUE) ~ "(ii) exceptional: walk produced different value",
        is.na(V2_VALUE) ~ "(ii) exceptional: walk produced no value"
      )
    ) %>%
    ungroup() %>%
    select(PRGM_ID, FIELD, ORIG_VALUE, V2_VALUE, V2_WALK_RULE, CLASSIFICATION, CYCLE, ORIG_NOTE)

  write.csv(
    classification,
    file.path(diag_dir, "dacso-v2-override-classification.csv"),
    row.names = FALSE
  )

  # Exceptional overrides: keep the original's value applied (never silently
  # lose a past manual decision) and seed the shared exceptions table with the
  # rationale. Seeded only-if-absent (analyst-maintained afterwards).
  exceptions <- classification %>%
    filter(str_detect(CLASSIFICATION, "^\\(ii\\)")) %>%
    transmute(
      PRGM_ID = as.integer(PRGM_ID),
      FIELD, VALUE = ORIG_VALUE, CYCLE,
      RATIONALE = glue::glue(
        "Original {CYCLE} manual override ({ORIG_NOTE}); auto-walk produced: ",
        "{coalesce(V2_VALUE, '<no value>')}. Reconcile and update this row."
      ),
      CLASSIFICATION
    )
  exceptions_tbl_name <- "T_DACSO_PRGM_EXCEPTIONS_v2_r"
  if (nrow(exceptions) > 0) {
    if (!dbExistsTable(con, Id(schema = shareschema, table = exceptions_tbl_name))) {
      dbWriteTable(con, Id(schema = shareschema, table = exceptions_tbl_name), exceptions)
      log_info(glue::glue(
        "Step 5: seeded dbo.{exceptions_tbl_name} with {nrow(exceptions)} exceptional overrides"
      ))
    } else {
      log_info(glue::glue(
        "Step 5: dbo.{exceptions_tbl_name} exists -- applying current rows as-is"
      ))
      exceptions <- sch_tbl(exceptions_tbl_name, schema = shareschema) %>% collect() %>%
        mutate(PRGM_ID = as.character(PRGM_ID))
    }
    # Apply the exceptional values to the XWALK (original's decision wins for
    # classified (ii) rows until an analyst reconciles).
    apply_exc <- exceptions %>%
      filter(FIELD %in% c("CIP_CODE_4", "PRGM_LCPC_CD")) %>%
      distinct(PRGM_ID, FIELD, .keep_all = TRUE)
    for (fld in unique(apply_exc$FIELD)) {
      vals <- apply_exc %>% filter(FIELD == fld)
      xwalk[[fld]] <- ifelse(
        as.character(xwalk$PRGM_ID) %in% vals$PRGM_ID,
        vals$VALUE[match(as.character(xwalk$PRGM_ID), vals$PRGM_ID)],
        xwalk[[fld]]
      )
    }
    xwalk <- xwalk %>%
      mutate(WALK_RULE = ifelse(
        as.character(PRGM_ID) %in% apply_exc$PRGM_ID,
        "exception_override", WALK_RULE
      )) %>%
      refresh_xwalk_join_keys()
  }
  # classify counts outside glue -- the regex escapes do not survive glue's
  # expression round-trip
  n_reproducible <- sum(str_detect(classification$CLASSIFICATION, fixed("(i) ")))
  n_exceptional <- sum(str_detect(classification$CLASSIFICATION, fixed("(ii) ")))
  n_rule_ext <- sum(str_detect(classification$CLASSIFICATION, fixed("(iii) ")))
  log_info(glue::glue(
    "Step 5: override classification: {n_reproducible} reproducible, {n_exceptional} exceptional, {n_rule_ext} rule-extension (log: diagnostics/dacso-v2-override-classification.csv)"
  ))

  # ----------------------------------------------------------------------------
  # STEP 6: STP credential side (original Part 2 port): build stp_dacso, map
  # institutions, derive the STP CIP, and run the already-matched passes
  # ----------------------------------------------------------------------------
  credential_non_dup <- sch_tbl("credential_non_dup_r") %>%
    rename_with(toupper) %>%
    select(
      PSI_CODE, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
      PSI_CREDENTIAL_CIP, PSI_CREDENTIAL_LEVEL, PSI_CREDENTIAL_CATEGORY,
      OUTCOMES_CRED
    ) %>%
    filter(OUTCOMES_CRED == "DACSO") %>%
    collect()

  stp_dacso <- credential_non_dup %>%
    count(
      PSI_CODE, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
      PSI_CREDENTIAL_CIP, PSI_CREDENTIAL_LEVEL, PSI_CREDENTIAL_CATEGORY,
      OUTCOMES_CRED,
      name = "EXPR1"
    ) %>%
    mutate(
      OUTCOMES_CIP_CODE_4 = NA_character_,
      OUTCOMES_CIP_CODE_4_NAME = NA_character_,
      FINAL_CIP_CODE_4 = NA_character_,
      FINAL_CIP_CODE_4_NAME = NA_character_,
      FINAL_CIP_CODE_2 = NA_character_,
      FINAL_CIP_CODE_2_NAME = NA_character_,
      FINAL_CIP_CLUSTER_CODE = NA_character_,
      FINAL_CIP_CLUSTER_NAME = NA_character_,
      STP_CIP_CODE_4 = NA_character_,
      STP_CIP_CODE_4_NAME = NA_character_,
      Already_Matched = NA_character_,
      New_Auto_Match = NA_character_,
      COCI_INST_CD = NA_character_,
      MATCH_RULE = NA_character_
    ) %>%
    refresh_stp_join_keys()
  log_info(glue::glue(
    "Step 6: stp_dacso built: {nrow(stp_dacso)} distinct credential programs"
  ))

  # PSI -> COCI institution mapping from the XWALK (two code systems)
  psi_to_coci <- xwalk %>%
    filter(!is.na(PSI_CODE) & !is.na(COCI_INST_CD)) %>%
    distinct(PSI_CODE, COCI_INST_CD)
  stp_dacso <- stp_dacso %>%
    left_join(
      psi_to_coci %>% rename(COCI_INST_CD_MAP = COCI_INST_CD),
      by = "PSI_CODE"
    ) %>%
    mutate(COCI_INST_CD = coalesce(COCI_INST_CD_MAP, COCI_INST_CD)) %>%
    select(-COCI_INST_CD_MAP) %>%
    refresh_stp_join_keys()

  # STP CIP from the credential's own 6-digit code (used by the fallback and
  # comparison, exactly as the original derives it)
  cip6_lookup <- cip6 %>%
    select(LCIP_CD_WITH_PERIOD, LCIP_LCP4_CD) %>%
    inner_join(
      cip4_ref %>% select(LCP4_CD, LCP4_CIP_4DIGITS_NAME),
      by = c("LCIP_LCP4_CD" = "LCP4_CD")
    )
  stp_dacso <- stp_dacso %>%
    left_join(
      cip6_lookup %>% select(LCIP_CD_WITH_PERIOD, LCIP_LCP4_CD, LCP4_CIP_4DIGITS_NAME),
      by = c("PSI_CREDENTIAL_CIP" = "LCIP_CD_WITH_PERIOD")
    ) %>%
    mutate(
      STP_CIP_CODE_4 = coalesce(STP_CIP_CODE_4, LCIP_LCP4_CD),
      STP_CIP_CODE_4_NAME = coalesce(STP_CIP_CODE_4_NAME, LCP4_CIP_4DIGITS_NAME)
    ) %>%
    select(-LCIP_LCP4_CD, -LCP4_CIP_4DIGITS_NAME)

  # Generic matcher: one XWALK pass -- join on given keys, fill OUTCOMES CIP
  # where unset, stamp the flag + MATCH_RULE. This is the original's repeated
  # join+mutate pattern parameterized once. pass_matched is captured BEFORE
  # the OUTCOMES fill (dplyr mutate clauses are sequential -- computing the
  # trace after the fill would never fire).
  match_pass <- function(stp_df, xwalk_df, xwalk_keys, stp_keys,
                         flag_col, flag_value, rule) {
    candidate <- xwalk_df %>%
      filter(if_all(all_of(xwalk_keys), ~ !is.na(.))) %>%
      select(all_of(xwalk_keys), CIP_CODE_4, LCP4_CIP_4DIGITS_NAME) %>%
      slice_head(n = 1, by = all_of(xwalk_keys))
    stp_df %>%
      left_join(
        candidate %>% rename(XW_CIP4 = CIP_CODE_4, XW_CIP4_NAME = LCP4_CIP_4DIGITS_NAME),
        # by: names = stp_df (x) columns, values = candidate (y) columns
        by = set_names(xwalk_keys, stp_keys)
      ) %>%
      mutate(
        pass_matched = is.na(OUTCOMES_CIP_CODE_4) & !is.na(XW_CIP4),
        {{ flag_col }} := if_else(
          is.na(OUTCOMES_CIP_CODE_4) & is.na(OUTCOMES_CIP_CODE_4_NAME) &
            is.na({{ flag_col }}) & pass_matched,
          flag_value,
          {{ flag_col }}
        ),
        OUTCOMES_CIP_CODE_4 = if_else(
          pass_matched, XW_CIP4, OUTCOMES_CIP_CODE_4
        ),
        OUTCOMES_CIP_CODE_4_NAME = if_else(
          is.na(OUTCOMES_CIP_CODE_4_NAME) & !is.na(XW_CIP4_NAME),
          XW_CIP4_NAME, OUTCOMES_CIP_CODE_4_NAME
        ),
        MATCH_RULE = coalesce(
          MATCH_RULE,
          if_else(pass_matched, rule, NA_character_)
        )
      ) %>%
      select(-pass_matched, -XW_CIP4, -XW_CIP4_NAME)
  }
  # NOTE on flag semantics: the Already_Matched passes must fill OUTCOMES even
  # when the flag is already set only in later passes -- ported faithfully by
  # ordering the passes exactly as the original (exact PSI, exact COCI, then
  # the new-match passes with their guard columns).

  # Already matched -- exact on PSI business key
  stp_dacso <- match_pass(
    stp_dacso, xwalk,
    xwalk_keys = c("PSI_CODE_KEY", "PSI_PROGRAM_CODE_KEY", "PSI_CREDENTIAL_PROGRAM_DESC_KEY"),
    stp_keys = c("PSI_CODE_KEY", "PSI_PROGRAM_CODE_KEY", "PSI_CREDENTIAL_PROGRAM_DESCRIPTION_KEY"),
    flag_col = Already_Matched, flag_value = "Yes", rule = "already_matched_psi"
  )
  # Already matched -- exact on COCI business key
  stp_dacso <- match_pass(
    stp_dacso, xwalk,
    xwalk_keys = c("COCI_INST_CD_KEY", "PSI_PROGRAM_CODE_KEY", "PSI_CREDENTIAL_PROGRAM_DESC_KEY"),
    stp_keys = c("COCI_INST_CD_KEY", "PSI_PROGRAM_CODE_KEY", "PSI_CREDENTIAL_PROGRAM_DESCRIPTION_KEY"),
    flag_col = Already_Matched, flag_value = "Yes", rule = "already_matched_coci"
  )

  # New auto-match: STP program code equals the XWALK's DACSO CPC
  stp_dacso <- match_pass(
    stp_dacso, xwalk,
    xwalk_keys = c("PSI_CODE_KEY", "PRGM_LCPC_CD_KEY", "PSI_CREDENTIAL_PROGRAM_DESC_KEY"),
    stp_keys = c("PSI_CODE_KEY", "PSI_PROGRAM_CODE_KEY", "PSI_CREDENTIAL_PROGRAM_DESCRIPTION_KEY"),
    flag_col = New_Auto_Match, flag_value = "Yes", rule = "new_auto_match_psi"
  )
  stp_dacso <- match_pass(
    stp_dacso, xwalk,
    xwalk_keys = c("COCI_INST_CD_KEY", "PRGM_LCPC_CD_KEY", "PRGM_INST_PROGRAM_NAME_KEY"),
    stp_keys = c("COCI_INST_CD_KEY", "PSI_PROGRAM_CODE_KEY", "PSI_CREDENTIAL_PROGRAM_DESCRIPTION_KEY"),
    flag_col = New_Auto_Match, flag_value = "Yes", rule = "new_auto_match_coci"
  )
  log_info(glue::glue(
    "Step 6: matched so far: {sum(!is.na(stp_dacso$OUTCOMES_CIP_CODE_4))} / {nrow(stp_dacso)} (Already {sum(stp_dacso$Already_Matched == 'Yes', na.rm = TRUE)}, NewAuto {sum(stp_dacso$New_Auto_Match == 'Yes', na.rm = TRUE)})"
  ))

  # Add newly matched STP programs back into the XWALK (so future runs find
  # them as already-matched) -- PSI-keyed then COCI-keyed, as the original.
  newly_matched <- stp_dacso %>%
    filter(New_Auto_Match == "Yes") %>%
    select(
      PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
      PSI_CODE_KEY, PSI_PROGRAM_CODE_KEY, PSI_CREDENTIAL_PROGRAM_DESCRIPTION_KEY,
      STP_CIP_CODE_4, STP_CIP_CODE_4_NAME
    )
  xwalk <- xwalk %>%
    left_join(
      newly_matched %>%
        rename(
          XW_STP_PGM_CODE = PSI_PROGRAM_CODE,
          XW_STP_PGM_DESC = PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
          XW_STP_CIP4 = STP_CIP_CODE_4,
          XW_STP_CIP4_NAME = STP_CIP_CODE_4_NAME
        ),
      by = c(
        "PSI_CODE_KEY" = "PSI_CODE_KEY",
        "PRGM_LCPC_CD_KEY" = "PSI_PROGRAM_CODE_KEY",
        "PRGM_INST_PROGRAM_NAME_KEY" = "PSI_CREDENTIAL_PROGRAM_DESCRIPTION_KEY"
      )
    ) %>%
    mutate(
      PSI_PROGRAM_CODE = if_else(!is.na(XW_STP_PGM_CODE), XW_STP_PGM_CODE, PSI_PROGRAM_CODE),
      PSI_CREDENTIAL_PROGRAM_DESC = if_else(
        !is.na(XW_STP_PGM_DESC), XW_STP_PGM_DESC, PSI_CREDENTIAL_PROGRAM_DESC
      ),
      STP_CIP4_CODE = if_else(!is.na(XW_STP_CIP4), XW_STP_CIP4, as.character(STP_CIP4_CODE)),
      STP_CIP4_NAME = if_else(!is.na(XW_STP_CIP4_NAME), XW_STP_CIP4_NAME, STP_CIP4_NAME),
      New_STP_Program2021_23 = if_else(
        !is.na(XW_STP_PGM_CODE), "Yes", coalesce(New_STP_Program2021_23, NA_character_)
      ),
      WALK_RULE = if_else(
        !is.na(XW_STP_PGM_CODE),
        coalesce(WALK_RULE, "new_stp_program_added"),
        WALK_RULE
      )
    ) %>%
    select(-starts_with("XW_STP")) %>%
    refresh_xwalk_join_keys()

  # ----------------------------------------------------------------------------
  # STEP 7: Institution-specific rules + catch-all (original Part 3 port)
  # ----------------------------------------------------------------------------
  # WHY: some institutions code programs differently in STP vs DACSO. The
  # rules derive a DACSO-compatible test code from the STP code and match on
  # it -- deterministic per institution:
  #   BCIT: STP codes carry credential suffixes (_TTDIPL...) -> first 4 chars
  #   CAPU: 6-digit STP codes vs 3-4 digit DACSO, some with -YYY suffixes ->
  #         dash-stripped, then 4-char, then 3-char prefixes
  #   VIU:  STP wraps the DACSO code as CERT-WELDM_01 -> substring between
  #         "-" and "_"
  # Each pass runs with description then without (code only).
  match_on_test_code_v2 <- function(stp_df, xwalk_df, test_col,
                                    flag_value, rule, join_cols_desc = TRUE) {
    stp_df <- stp_df %>% add_join_keys(c(TEST_PROGRAM_CODE_KEY = test_col))
    if (join_cols_desc) {
      out <- match_pass(
        stp_df, xwalk_df,
        xwalk_keys = c("COCI_INST_CD_KEY", "PRGM_LCPC_CD_KEY", "PRGM_INST_PROGRAM_NAME_KEY"),
        stp_keys = c("COCI_INST_CD_KEY", "TEST_PROGRAM_CODE_KEY", "PSI_CREDENTIAL_PROGRAM_DESCRIPTION_KEY"),
        flag_col = New_Auto_Match, flag_value = flag_value, rule = rule
      )
    } else {
      out <- match_pass(
        stp_df, xwalk_df,
        xwalk_keys = c("COCI_INST_CD_KEY", "PRGM_LCPC_CD_KEY"),
        stp_keys = c("COCI_INST_CD_KEY", "TEST_PROGRAM_CODE_KEY"),
        flag_col = New_Auto_Match, flag_value = flag_value, rule = rule
      )
    }
    out %>% select(-TEST_PROGRAM_CODE_KEY)
  }

  stp_dacso <- stp_dacso %>%
    mutate(
      BCIT_TEST_PROGRAM_CODE = if_else(
        PSI_CODE == "BCIT", substr(PSI_PROGRAM_CODE, 1, 4), NA_character_
      )
    )
  stp_dacso <- match_on_test_code_v2(stp_dacso, xwalk, "BCIT_TEST_PROGRAM_CODE", "YesBCIT", "inst_rule_bcit", TRUE)
  stp_dacso <- match_on_test_code_v2(stp_dacso, xwalk, "BCIT_TEST_PROGRAM_CODE", "YesBCIT", "inst_rule_bcit", FALSE)

  stp_dacso <- stp_dacso %>%
    mutate(
      CAP_TEST_PROGRAM_CODE = if_else(
        COCI_INST_CD == "CAPU" & grepl("-", PSI_PROGRAM_CODE),
        substr(PSI_PROGRAM_CODE, 1, regexpr("-", PSI_PROGRAM_CODE, fixed = TRUE) - 1),
        NA_character_
      )
    )
  stp_dacso <- match_on_test_code_v2(stp_dacso, xwalk, "CAP_TEST_PROGRAM_CODE", "YesCAPU", "inst_rule_capu", TRUE)
  stp_dacso <- match_on_test_code_v2(stp_dacso, xwalk, "CAP_TEST_PROGRAM_CODE", "YesCAPU", "inst_rule_capu", FALSE)
  stp_dacso <- stp_dacso %>%
    mutate(CAP_TEST_PROGRAM_CODE = if_else(
      COCI_INST_CD == "CAPU", substr(PSI_PROGRAM_CODE, 1, 4), CAP_TEST_PROGRAM_CODE
    ))
  stp_dacso <- match_on_test_code_v2(stp_dacso, xwalk, "CAP_TEST_PROGRAM_CODE", "YesCAPU", "inst_rule_capu", TRUE)
  stp_dacso <- match_on_test_code_v2(stp_dacso, xwalk, "CAP_TEST_PROGRAM_CODE", "YesCAPU", "inst_rule_capu", FALSE)
  stp_dacso <- stp_dacso %>%
    mutate(CAP_TEST_PROGRAM_CODE = if_else(
      COCI_INST_CD == "CAPU", substr(PSI_PROGRAM_CODE, 1, 3), CAP_TEST_PROGRAM_CODE
    ))
  stp_dacso <- match_on_test_code_v2(stp_dacso, xwalk, "CAP_TEST_PROGRAM_CODE", "YesCAPU", "inst_rule_capu", TRUE)
  stp_dacso <- match_on_test_code_v2(stp_dacso, xwalk, "CAP_TEST_PROGRAM_CODE", "YesCAPU", "inst_rule_capu", FALSE)

  stp_dacso <- stp_dacso %>%
    mutate(
      VIU_TEST_PROGRAM_CODE = if_else(
        PSI_CODE == "VIU" & grepl("-", PSI_PROGRAM_CODE) & grepl("_", PSI_PROGRAM_CODE),
        substr(
          PSI_PROGRAM_CODE,
          regexpr("-", PSI_PROGRAM_CODE, fixed = TRUE) + 1,
          regexpr("_", PSI_PROGRAM_CODE, fixed = TRUE) - 1
        ),
        NA_character_
      )
    )
  stp_dacso <- match_on_test_code_v2(stp_dacso, xwalk, "VIU_TEST_PROGRAM_CODE", "YesVIU", "inst_rule_viu", TRUE)
  stp_dacso <- match_on_test_code_v2(stp_dacso, xwalk, "VIU_TEST_PROGRAM_CODE", "YesVIU", "inst_rule_viu", FALSE)

  # Catch-all: institution + code, then institution + description
  stp_dacso <- match_pass(
    stp_dacso, xwalk,
    xwalk_keys = c("COCI_INST_CD_KEY", "PRGM_LCPC_CD_KEY"),
    stp_keys = c("COCI_INST_CD_KEY", "PSI_PROGRAM_CODE_KEY"),
    flag_col = New_Auto_Match, flag_value = "Yes_catch_all", rule = "catch_all_code"
  )
  stp_dacso <- match_pass(
    stp_dacso, xwalk,
    xwalk_keys = c("COCI_INST_CD_KEY", "PRGM_INST_PROGRAM_NAME_KEY"),
    stp_keys = c("COCI_INST_CD_KEY", "PSI_CREDENTIAL_PROGRAM_DESCRIPTION_KEY"),
    flag_col = New_Auto_Match, flag_value = "Yes_catch_all", rule = "catch_all_desc"
  )
  log_info(glue::glue(
    "Step 7: after institution rules + catch-all: {sum(!is.na(stp_dacso$OUTCOMES_CIP_CODE_4))} / {nrow(stp_dacso)} matched"
  ))
  print(stp_dacso %>% count(MATCH_RULE))

  # ----------------------------------------------------------------------------
  # STEP 8: Final CIP cascade (original Part 4 port) + trace completion
  # ----------------------------------------------------------------------------
  # FINAL = OUTCOMES CIP where matched; otherwise the INFOWARE taxonomy CIP
  # derived from the credential's own 6-digit code; then fill 2-digit names
  # and cluster from the FINAL 4-digit code.
  stp_dacso <- stp_dacso %>%
    mutate(
      FINAL_CIP_CODE_4 = if_else(
        !is.na(OUTCOMES_CIP_CODE_4) & !is.na(OUTCOMES_CIP_CODE_4_NAME),
        OUTCOMES_CIP_CODE_4, FINAL_CIP_CODE_4
      ),
      FINAL_CIP_CODE_4_NAME = if_else(
        !is.na(OUTCOMES_CIP_CODE_4) & !is.na(OUTCOMES_CIP_CODE_4_NAME),
        OUTCOMES_CIP_CODE_4_NAME, FINAL_CIP_CODE_4_NAME
      )
    )

  cip6_full <- cip6 %>%
    select(LCIP_CD_WITH_PERIOD, LCIP_LCP4_CD, LCIP_LCP2_CD,
           LCIP_LCIPPC_CD, LCIP_LCIPPC_NAME) %>%
    inner_join(
      cip4_ref %>% select(LCP4_CD, LCP4_CIP_4DIGITS_NAME),
      by = c("LCIP_LCP4_CD" = "LCP4_CD")
    ) %>%
    inner_join(
      cip2_ref %>% select(LCP2_CD, LCP2_DIGITS_NAME),
      by = c("LCIP_LCP2_CD" = "LCP2_CD")
    )

  stp_dacso <- stp_dacso %>%
    left_join(
      cip6_full %>% select(
        LCIP_CD_WITH_PERIOD, LCIP_LCP4_CD, LCP4_CIP_4DIGITS_NAME,
        LCIP_LCP2_CD, LCP2_DIGITS_NAME
      ),
      by = c("PSI_CREDENTIAL_CIP" = "LCIP_CD_WITH_PERIOD")
    ) %>%
    mutate(
      FINAL_CIP_CODE_4 = if_else(
        is.na(FINAL_CIP_CODE_4) & is.na(FINAL_CIP_CODE_4_NAME) &
          is.na(FINAL_CIP_CODE_2) & is.na(FINAL_CIP_CODE_2_NAME) &
          is.na(OUTCOMES_CIP_CODE_4),
        LCIP_LCP4_CD, FINAL_CIP_CODE_4
      ),
      FINAL_CIP_CODE_4_NAME = if_else(
        is.na(FINAL_CIP_CODE_4_NAME) & is.na(FINAL_CIP_CODE_2) &
          is.na(FINAL_CIP_CODE_2_NAME) & is.na(OUTCOMES_CIP_CODE_4),
        LCP4_CIP_4DIGITS_NAME, FINAL_CIP_CODE_4_NAME
      ),
      FINAL_CIP_CODE_2 = if_else(
        is.na(FINAL_CIP_CODE_2) & is.na(FINAL_CIP_CODE_2_NAME) &
          is.na(OUTCOMES_CIP_CODE_4),
        LCIP_LCP2_CD, FINAL_CIP_CODE_2
      ),
      FINAL_CIP_CODE_2_NAME = if_else(
        is.na(FINAL_CIP_CODE_2_NAME) & is.na(OUTCOMES_CIP_CODE_4),
        LCP2_DIGITS_NAME, FINAL_CIP_CODE_2_NAME
      ),
      MATCH_RULE = coalesce(
        MATCH_RULE,
        if_else(is.na(OUTCOMES_CIP_CODE_4) & !is.na(LCIP_LCP4_CD),
                "stp_fallback", NA_character_)
      )
    ) %>%
    select(-LCIP_LCP4_CD, -LCP4_CIP_4DIGITS_NAME, -LCIP_LCP2_CD, -LCP2_DIGITS_NAME)

  # Fill remaining name/2-digit/cluster from the FINAL 4-digit code
  cip4_lookup <- cip6 %>%
    select(LCIP_LCP4_CD, LCIP_LCP2_CD) %>%
    distinct() %>%
    inner_join(
      cip4_ref %>% select(LCP4_CD, LCP4_CIP_4DIGITS_NAME),
      by = c("LCIP_LCP4_CD" = "LCP4_CD")
    ) %>%
    inner_join(
      cip2_ref %>% select(LCP2_CD, LCP2_DIGITS_NAME),
      by = c("LCIP_LCP2_CD" = "LCP2_CD")
    )
  stp_dacso <- stp_dacso %>%
    left_join(cip4_lookup, by = c("FINAL_CIP_CODE_4" = "LCIP_LCP4_CD")) %>%
    mutate(
      FINAL_CIP_CODE_4_NAME = coalesce(FINAL_CIP_CODE_4_NAME, LCP4_CIP_4DIGITS_NAME),
      FINAL_CIP_CODE_2 = coalesce(FINAL_CIP_CODE_2, LCIP_LCP2_CD),
      FINAL_CIP_CODE_2_NAME = coalesce(FINAL_CIP_CODE_2_NAME, LCP2_DIGITS_NAME)
    ) %>%
    select(-LCIP_LCP2_CD, -LCP4_CIP_4DIGITS_NAME, -LCP2_DIGITS_NAME)

  cip6_cluster <- cip6 %>%
    select(LCIP_LCP4_CD, LCIP_LCP2_CD, LCIP_LCIPPC_CD, LCIP_LCIPPC_NAME) %>%
    distinct()
  stp_dacso <- stp_dacso %>%
    left_join(cip6_cluster, by = c("FINAL_CIP_CODE_4" = "LCIP_LCP4_CD")) %>%
    mutate(
      FINAL_CIP_CODE_2 = coalesce(FINAL_CIP_CODE_2, LCIP_LCP2_CD),
      FINAL_CIP_CLUSTER_CODE = coalesce(FINAL_CIP_CLUSTER_CODE, LCIP_LCIPPC_CD),
      FINAL_CIP_CLUSTER_NAME = coalesce(FINAL_CIP_CLUSTER_NAME, LCIP_LCIPPC_NAME)
    ) %>%
    select(-LCIP_LCP2_CD, -LCIP_LCIPPC_CD, -LCIP_LCIPPC_NAME)

  stp_dacso <- stp_dacso %>%
    left_join(
      cip2_ref %>% select(LCP2_CD, LCP2_DIGITS_NAME),
      by = c("FINAL_CIP_CODE_2" = "LCP2_CD")
    ) %>%
    mutate(FINAL_CIP_CODE_2_NAME = coalesce(FINAL_CIP_CODE_2_NAME, LCP2_DIGITS_NAME)) %>%
    select(-LCP2_DIGITS_NAME) %>%
    mutate(
      FINAL_CIP_CODE_4 = str_pad(FINAL_CIP_CODE_4, 4, "left", "0"),
      FINAL_CIP_CODE_2 = str_pad(FINAL_CIP_CODE_2, 2, "left", "0")
    )
  log_info(glue::glue(
    "Step 8: final CIPs: {sum(!is.na(stp_dacso$FINAL_CIP_CODE_4))} / {nrow(stp_dacso)} programs populated"
  ))
  print(stp_dacso %>% count(MATCH_RULE))

  # ----------------------------------------------------------------------------
  # STEP 9: Write outputs (fresh _v2_r names, analyst schema)
  # ----------------------------------------------------------------------------
  write_v2 <- function(table_name, df) {
    target <- Id(schema = my_schema, table = table_name)
    if (dbExistsTable(con, target)) dbRemoveTable(con, target)
    dbWriteTable(con, target, df)
    log_info(glue::glue("Wrote {my_schema}.{table_name}: {nrow(df)} rows"))
  }

  write_v2("DACSO_STP_ProgramsCIP4_XWALK_ALL_v2_r", drop_join_keys(xwalk))
  write_v2(
    "Credential_Non_Dup_Programs_DACSO_FinalCIPs_v2_r",
    drop_join_keys(stp_dacso)
  )

  dbDisconnect(con)
  log_info("==== 02a-dacso-program-matching-v2.R COMPLETE ====")
}
