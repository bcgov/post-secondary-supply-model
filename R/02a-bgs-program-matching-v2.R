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
# 02a-bgs-program-matching-v2.R  --  BGS program matching, v2 (parallel family)
#
# ------------------------------------------------------------------------------
# WHERE THIS SITS IN THE MODEL
# ------------------------------------------------------------------------------
# PSSM computes one formula (docs/project-summary-for-new-analyst.md §2):
#
#   OCCSN(NOC) = GRADUATES(cred,age) x P(CIP|cred,age)
#              x P(in labour supply|CIP) x P(NOC|CIP,region)
#
# This script decides, for every Baccalaureate Graduate Survey (BGS) respondent
# who PEN-matches an STP credential, whether the FINAL 4-digit CIP comes from
# the survey or the credential record. Those FINAL_CIP_CODE_4 values flow two
# ways into P(CIP|cred,age):
#   credential side -> Credential_Non_Dup_BGS_IDs_v2_r -> 02a-update-cred-non-dup
#                      priority step 2 (DACSO > BGS > GRAD > APPSO > STP)
#                      -> credential_non_dup_r -> 02b-1 `LCIP4_CRED` cohort key;
#   survey side     -> T_BGS_Data_Final_for_OutcomesMatching_v2_r -> 02b-1's
#                      BGS block (coalesces FINAL over the survey's own CIP).
#
# ------------------------------------------------------------------------------
# WHY A v2 (and what is deliberately different from the frozen original)
# ------------------------------------------------------------------------------
# The frozen original (R/02a-bgs-program-matching.R) works but encodes three
# kinds of analyst knowledge as CODE: institution aliases in a MATCH_INST
# case_when, custom CIP choices inline, and a manual-review CSV round-trip.
# Per the cip-matching wayfinder map (tickets 05, 06, 14) this v2:
#
#   1. ALIAS AUTOMATION (ticket 05): aliases live in a shared dbo lookup
#      `T_BGS_STP_INST_ALIAS_v2_r` (seeded verbatim from the original's
#      case_when). The script JOINS the table instead of hardcoding, and a
#      PEN-co-occurrence inference block PROPOSES new alias pairs as a report
#      for analyst confirmation -- proposals are never auto-applied.
#   2. DETERMINISTIC RULES (ticket 06): the original's Part 3B+ decision tree
#      (general-program preference, 4-digit cross-validation, default-STP for
#      double majors) is ported as deterministic case_whens, and every
#      assignment emits a MATCH_RULE decision-trace column for audit.
#   3. CUSTOM CHOICES AS DATA (ticket 06): the original's inline custom CIP
#      rules and the 4B.5 override tribble move to a shared dbo lookup
#      `T_BGS_CUSTOM_CIP_CHOICES_v2_r` that analysts extend instead of code.
#   4. SPINE CONSUMPTION (ticket 13): STP CIP cleaning comes from the shared
#      spine table Credential_Non_Dup_STP_CIP_Cleaning_v2_r (one canonical
#      cascade -- see R/02a-cip-normalize.R), and PENs are canonicalized with
#      the spine's normalize_pen() on both sides of the join. Note the spine
#      is deliberately stricter than the original's per-survey cleaning: a
#      first-5-char partial match no longer promotes to a 4-digit code, so a
#      small set of credentials carry no STP CIP4 here that WOULD have had one
#      under the original. Those rows are visible via MATCH_RULE.
#
# The original stays frozen and runnable (baseline anchor); v2 writes only
# distinctly-named `_v2_r` tables that coexist with it. The comparison harness
# (ticket 17) quantifies original-vs-v2 differences from the MATCH_RULE trace.
#
# ------------------------------------------------------------------------------
# MATCH_RULE VOCABULARY (the decision trace -- the harness consumes these)
# ------------------------------------------------------------------------------
#   exact_4digit_inst_year        inst+year+4-digit CIP all agree (use BGS==STP)
#   general_program_bgs_generic   BGS used a "general" CIP -> prefer STP specific
#   general_program_stp_generic   STP used a "general" CIP -> prefer BGS specific
#   xval_stp_4digit_evidence      STP CIP validated by a 4-digit match elsewhere
#   xval_bgs_4digit_evidence      BGS CIP validated by a 4-digit match elsewhere
#   custom_cip_choice             override from T_BGS_CUSTOM_CIP_CHOICES_v2_r
#   default_stp_double_majors     residual 2-digit matches default to STP
#   no_cip_match_default_stp      inst+year matched but CIPs disagree -> STP
#                                 (the original's manual-review caseload)
#   no_inst_year_match            PEN matched but inst/year flags failed; the
#                                 credential side falls back to its own STP CIP
#   program_override_lookup       credential-row override (4B.5-equivalent rows)
#   stp_fallback_no_match         credential never PEN-matched a survey row
#
# ------------------------------------------------------------------------------
# INPUTS (materialized by the 01* loaders, ticket 11's 02a run, ticket 13's
# spine run -- all in the analyst personal schema unless noted)
# ------------------------------------------------------------------------------
#   credential_non_dup_r                        STP credentials, all buckets
#   stp_credential_r                            provides PSI_PEN per ID
#   Credential_Non_Dup_STP_CIP_Cleaning_v2_r    spine's unified CIP cleaning
#   T_BGS_Data_Final_for_OutcomesMatching_r     survey side (original Part 1)
#   dbo.INFOWARE_L_CIP_{2,4}DIGITS_CIP2021      CIP2021 lookups (names/clusters)
#   dbo.T_BGS_STP_INST_ALIAS_v2_r               alias lookup (SEEDED HERE)
#   dbo.T_BGS_CUSTOM_CIP_CHOICES_v2_r           custom choices (SEEDED HERE)
#
# ------------------------------------------------------------------------------
# OUTPUTS
# ------------------------------------------------------------------------------
#   [my_schema].BGS_Matching_STP_Credential_PEN_v2_r
#       full PEN-join decision table incl. MATCH_RULE (the audit artifact)
#   [my_schema].Credential_Non_Dup_BGS_IDs_v2_r
#       one row per BGS credential ID with FINAL_CIP_* + MATCH_RULE
#   [my_schema].T_BGS_Data_Final_for_OutcomesMatching_v2_r
#       survey rows + FINAL_CIP_* (what 02b-1's BGS block would read)
#   .scratch/cip-matching/diagnostics/bgs-v2-alias-proposals.csv
#       PEN-co-occurrence alias proposals for analyst confirmation
#   .scratch/cip-matching/diagnostics/bgs-v2-residual-manual-review.csv
#       the borderline caseload the original sent to manual review (exported
#       only when non-zero -- v2 assigns it default STP meanwhile)
#
# Processing is deliberately in-memory (collect() early, base dplyr after):
# at ~525K credential / ~134K match rows the data fits comfortably, and
# local dplyr joins match NA-to-NA by default -- exactly the semantics the
# original had to restore on SQL Server with na_matches = "na".
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

log_info("==== 02a-bgs-program-matching-v2.R START ====")

# ---- Configure DB connection ----
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

# ---- Load the spine's helpers (functions only; the spine's own pipeline
# block is skipped under cip_spine.lib_only, and its materialized cleaning
# table is read directly below) ----
options(cip_spine.lib_only = TRUE)
source("R/02a-cip-normalize.R", local = FALSE)
options(cip_spine.lib_only = FALSE)

diag_dir <- file.path(".scratch", "cip-matching", "diagnostics")
if (!dir.exists(diag_dir)) dir.create(diag_dir, recursive = TRUE)

# ==============================================================================
# STEP 1: Seed the institution-alias lookup (ticket 05)
# ==============================================================================
# WHAT: create dbo.T_BGS_STP_INST_ALIAS_v2_r from a verbatim transcription of
#       the original's MATCH_INST case_when (R/02a-bgs-program-matching.R
#       ~L942-968), but ONLY when the table does not already exist.
# WHY:  the alias list is shared team knowledge -- analysts add rows here
#       instead of editing a case_when in code. Never clobber: if the table
#       exists we assume an analyst has curated it and treat it as canonical.
# HOW:  every (PSI_CODE, INSTITUTION_CODE) pair the original's family rows
#       accepted, including the same-code pairs those families enumerated.
#       Same-code matching does not need a row (PSI_CODE == INSTITUTION_CODE
#       is matched in code), but seeding them keeps the table a faithful
#       record of the original case_when's coverage.
alias_seed <- tribble(
  ~PSI_CODE, ~INSTITUTION_CODE, ~ALIAS_FAMILY,
  # CAPU / CAP (Capilano University)
  "CAPU",    "CAP",             "CAPU",
  "CAP",     "CAPU",            "CAPU",
  "CAPU",    "CAPU",            "CAPU",
  "CAP",     "CAP",             "CAPU",
  # DOUG / DGL (Douglas College)
  "DOUG",    "DGL",             "DOUG",
  # UCC -> TRU (University College of the Cariboo renamed Thompson Rivers)
  "UCC",     "TRU",             "UCC",
  # ECIAD -> ECU (Emily Carr: several historical codings)
  "ECIAD",   "ECU",             "ECIAD",
  "ECIAD",   "ECUAD",           "ECIAD",
  "ECIAD",   "ECIAD",           "ECIAD",
  "ECU",     "ECU",             "ECIAD",
  "ECU",     "ECUAD",           "ECIAD",
  "ECU",     "ECIAD",           "ECIAD",
  # KWAN -> KPU (Kwantlen renamed)
  "KWAN",    "KPU",             "KWAN",
  "KWAN",    "KWN",             "KWAN",
  "KPU",     "KPU",             "KWAN",
  "KPU",     "KWN",             "KWAN",
  # MALA -> VIU (Malaspina renamed Vancouver Island University)
  "MALA",    "VIU",             "MALA",
  "MALA",    "MAL",             "MALA",
  "MAL",     "VIU",             "MALA",
  "MAL",     "MAL",             "MALA",
  # OUC -> OKAN (Okanagan University College -> UBC Okanagan / Okanagan College)
  "OUC",     "OKAN",            "OUC",
  "OUC",     "OKN",             "OUC",
  "OUC",     "OUC",             "OUC",
  "OKAN",    "OKAN",            "OUC",
  "OKAN",    "OKN",             "OUC",
  "OKAN",    "OUC",             "OUC",
  # OLA -> TRUOL (Open Learning Agency -> TRU Open Learning)
  "OLA",     "TRUOL",           "OLA",
  # UCFV -> UFV (University College of the Fraser Valley renamed)
  "UCFV",    "UFV",             "UCFV",
  "UCFV",    "FVAL",            "UCFV",
  "UCFV",    "UCFV",            "UCFV",
  "UFV",     "UFV",             "UCFV",
  "UFV",     "FVAL",            "UCFV",
  "UFV",     "UCFV",            "UCFV",
  # UBCO/UBCV -> UBC (Okanagan + Vancouver campuses report under one STP code)
  "UBCO",    "UBC",             "UBCO",
  "UBCV",    "UBC",             "UBCO"
)

alias_tbl_name <- "T_BGS_STP_INST_ALIAS_v2_r"
if (!dbExistsTable(con, Id(schema = shareschema, table = alias_tbl_name))) {
  dbWriteTable(con, Id(schema = shareschema, table = alias_tbl_name), alias_seed)
  log_info(glue::glue(
    "Step 1: seeded dbo.{alias_tbl_name} with {nrow(alias_seed)} alias pairs (verbatim from the original MATCH_INST case_when)"
  ))
} else {
  log_info(glue::glue(
    "Step 1: dbo.{alias_tbl_name} already exists -- using it as-is (analyst-curated)"
  ))
}
alias_lookup <- sch_tbl(alias_tbl_name, schema = shareschema) %>% collect()

# ==============================================================================
# STEP 2: Seed the custom-CIP-choices lookup (ticket 06)
# ==============================================================================
# WHAT: create dbo.T_BGS_CUSTOM_CIP_CHOICES_v2_r from the original's two
#       hardcoded custom rules (Part 3B+ Decision Step 3) and the 4B.5 manual
#       override tribble -- again only when the table does not exist.
# WHY:  these are analyst judgements, not derivable logic; as a lookup,
#       analysts extend them per cycle without code changes.
# HOW:  one row per choice; CHOICE_TYPE distinguishes the two application
#       sites: "cip_pair_rule" fires on (BGS CIP4, STP CIP4) at the 2-digit
#       decision-point level; "program_override" fires on the credential
#       program description for unmatched credential rows (4B.5 semantics).
custom_choices_seed <- tribble(
  ~CHOICE_TYPE,           ~BGS_CIP4, ~STP_CIP4, ~PSI_CREDENTIAL_PROGRAM_DESCRIPTION,                                        ~FINAL_CIP_CODE_4, ~FINAL_CIP_CODE_2, ~CIP_TO_USE, ~NOTE,
  # Part 3B+ custom rules: BGS/STP code systematically disagree -> follow STP
  "cip_pair_rule",        "2701",    "2703",    NA,                                                                        NA,                NA,               "STP",       "Math vs Applied Math: BGS 2701, STP 2703; programs labelled operations research/mash",
  "cip_pair_rule",        "1405",    "1407",    NA,                                                                        NA,                NA,               "STP",       "Bioengineering vs Chemical Engineering: BGS 1405, STP 1407; programs all say Chemical Engineering",
  # 4B.5 override tribble: unmatched credential programs -> use the BGS-side CIP
  "program_override",     NA,        NA,        "Bachelor Of Applied Science In Mechatronic Systems Engineering",          "1442",            "14",             NA,          "4B.5 override (2023): no case-level match; use BGS CIP",
  "program_override",     NA,        NA,        "Bachelor Of Athletic And Exercise Therapy",                               "5123",            "51",             NA,          "4B.5 override (2023)",
  "program_override",     NA,        NA,        "Bachelor Of Fine Arts In Dance",                                          "5003",            "50",             NA,          "4B.5 override (2023)",
  "program_override",     NA,        NA,        "Bachelor Of Fine Arts In Film",                                           "5006",            "50",             NA,          "4B.5 override (2023)",
  "program_override",     NA,        NA,        "Bachelor Of Fine Arts In Music - Composition",                            "5009",            "50",             NA,          "4B.5 override (2023)",
  "program_override",     NA,        NA,        "Bachelor Of Fine Arts In Music - Electroacoustic",                        "5009",            "50",             NA,          "4B.5 override (2023)",
  "program_override",     NA,        NA,        "Bachelor Of Fine Arts In Theatre - Performance",                          "5005",            "50",             NA,          "4B.5 override (2023)",
  "program_override",     NA,        NA,        "Bachelor Of Fine Arts In Theatre - Production And Design",                "5005",            "50",             NA,          "4B.5 override (2023)",
  "program_override",     NA,        NA,        "Bachelor Of Science In Geographic Information Science",                   "4507",            "45",             NA,          "4B.5 override (2023)",
  "program_override",     NA,        NA,        "Bachelor Of Social Work In Indigenous Child Welfare",                      "4407",            "44",             NA,          "4B.5 override (2023)",
  "program_override",     NA,        NA,        "Bachelor Of Social Work In Indigenous Social Work",                        "4407",            "44",             NA,          "4B.5 override (2023)",
  "program_override",     NA,        NA,        "Bachelor Of Child & Youth Care In Child & Youth Care",                    "1907",            "19",             NA,          "4B.5 override (2023)",
  "program_override",     NA,        NA,        "Bachelor Of Child & Youth Care In Child & Youth Care - Child Life Stream", "1907",            "19",             NA,          "4B.5 override (2023)",
  "program_override",     NA,        NA,        "Bachelor Of Child & Youth Care In Child & Youth Care - Early Years Stream", "1907",           "19",             NA,          "4B.5 override (2023)",
  "program_override",     NA,        NA,        "Bachelor Of Child & Youth Care In Child & Youth Care - Child Protection",  "1907",            "19",             NA,          "4B.5 override (2023)",
  "program_override",     NA,        NA,        "Bachelor Of Child & Youth Care In Child & Youth Care - Indigenous Stream", "1907",            "19",             NA,          "4B.5 override (2023)"
)

choices_tbl_name <- "T_BGS_CUSTOM_CIP_CHOICES_v2_r"
if (!dbExistsTable(con, Id(schema = shareschema, table = choices_tbl_name))) {
  dbWriteTable(
    con, Id(schema = shareschema, table = choices_tbl_name), custom_choices_seed
  )
  log_info(glue::glue(
    "Step 2: seeded dbo.{choices_tbl_name} with {nrow(custom_choices_seed)} choices (2 cip_pair_rule + {sum(custom_choices_seed$CHOICE_TYPE == 'program_override')} program_override)"
  ))
} else {
  log_info(glue::glue(
    "Step 2: dbo.{choices_tbl_name} already exists -- using it as-is (analyst-curated)"
  ))
}
custom_choices <- sch_tbl(choices_tbl_name, schema = shareschema) %>% collect()
custom_pair_rules <- custom_choices %>%
  filter(CHOICE_TYPE == "cip_pair_rule", !is.na(BGS_CIP4), !is.na(STP_CIP4))
custom_program_overrides <- custom_choices %>%
  filter(CHOICE_TYPE == "program_override", !is.na(PSI_CREDENTIAL_PROGRAM_DESCRIPTION))

# Guard: one row per program description in the override set (the original
# stopped on duplicates -- same contract here).
dup_overrides <- custom_program_overrides %>%
  count(PSI_CREDENTIAL_PROGRAM_DESCRIPTION) %>% filter(n > 1)
if (nrow(dup_overrides) > 0) {
  stop("T_BGS_CUSTOM_CIP_CHOICES_v2_r program_override rows must be unique by PSI_CREDENTIAL_PROGRAM_DESCRIPTION")
}

# ==============================================================================
# STEP 3: Build the credential (STP) side
# ==============================================================================
# WHAT: BGS-bucket credentials from credential_non_dup_r, joined to the SPINE's
#       unified cleaning table for STP CIP4/CIP2, plus PSI_PEN from
#       stp_credential_r canonicalized through the spine's normalize_pen().
# WHY:  this is the STP half of the PEN join. Using the spine (instead of the
#       original's per-survey cleaning table) is the v2 contract: one shared
#       cascade, stricter 4-digit promotion. PEN canonicalization kills the
#       ".0"-suffix silent zero-join class of bug (ticket 12).
credential_side <- sch_tbl("credential_non_dup_r") %>%
  rename_with(toupper) %>%
  filter(OUTCOMES_CRED == "BGS") %>%
  select(
    ID, PSI_CODE, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    PSI_CREDENTIAL_CIP, PSI_AWARD_SCHOOL_YEAR, OUTCOMES_CRED
  ) %>%
  collect() %>%
  # "(Unspecified)" is an import artefact, not a program code
  mutate(PSI_PROGRAM_CODE = na_if(PSI_PROGRAM_CODE, "(Unspecified)")) %>%
  inner_join(
    sch_tbl("Credential_Non_Dup_STP_CIP_Cleaning_v2_r") %>%
      select(
        PSI_CREDENTIAL_CIP_orig,
        STP_CIP_CODE_4, STP_CIP_CODE_4_NAME,
        STP_CIP_CODE_2, STP_CIP_CODE_2_NAME
      ) %>%
      collect(),
    by = c("PSI_CREDENTIAL_CIP" = "PSI_CREDENTIAL_CIP_orig")
  ) %>%
  left_join(
    sch_tbl("stp_credential_r") %>% select(ID, PSI_PEN) %>% collect(),
    by = "ID"
  ) %>%
  mutate(PEN = normalize_pen(PSI_PEN))
log_info(glue::glue(
  "Step 3: credential side built: {nrow(credential_side)} BGS credentials; {sum(!is.na(credential_side$PEN))} with a canonical PEN; {sum(is.na(credential_side$STP_CIP_CODE_4))} without a spine-resolved STP CIP4"
))

# ==============================================================================
# STEP 4: Build the survey (BGS) side
# ==============================================================================
# WHAT: the materialized original Part-1 output (identical logic -- pure data
#       assembly, nothing v2 changes), with PEN canonicalized to match.
# WHY:  v2 consumes the ticket-11 materialized tables rather than re-deriving
#       unchanged logic; canonical PENs make the join robust to format drift.
# NOTE: the materialized table is the original's FINAL state -- the ticket-11
#       run appended the original's Part-5A decision columns
#       (STP_CIP_CODE_4*, FINAL_*, USE_STP_CIP, match flags) to it. v2 drops
#       those before joining so the v2 decisions are derived fresh and no
#       column-name collisions reach the PEN join.
survey_side <- sch_tbl("T_BGS_Data_Final_for_OutcomesMatching_r") %>%
  select(
    -STP_CIP_CODE_4, -STP_CIP_CODE_4_NAME,
    -FINAL_CONSIDER_A_MATCH, -FINAL_PROBABLE_MATCH, -USE_STP_CIP,
    -FINAL_CIP_CODE_4, -FINAL_CIP_CODE_4_NAME,
    -FINAL_CIP_CODE_2, -FINAL_CIP_CODE_2_NAME,
    -FINAL_CIP_CLUSTER_CODE, -FINAL_CIP_CLUSTER_NAME
  ) %>%
  collect() %>%
  mutate(PEN_CANON = normalize_pen(PEN)) %>%
  filter(!is.na(PEN_CANON))
log_info(glue::glue(
  "Step 4: survey side loaded: {nrow(survey_side)} rows ({sum(!is.na(survey_side$PEN_CANON))} with canonical PEN)"
))

# ==============================================================================
# STEP 5: PEN join (original Part 3A)
# ==============================================================================
# WHAT: inner-join survey rows to BGS credentials on the canonical PEN, then
#       rename to the original's matching-table vocabulary.
# WHY:  (STQU_ID, ID) is unique in this join (validated in the original) --
#       the case-level unit of the decision engine. Many-to-many at the PEN
#       level is expected (multiple credentials and/or survey years per PEN).
# HOW:  column names mirror the original so the harness can diff v1/v2 rows
#       column-for-column.
bgs_matching_v2 <- survey_side %>%
  inner_join(
    credential_side %>% filter(!is.na(PEN)),
    by = c("PEN_CANON" = "PEN"),
    relationship = "many-to-many"
  ) %>%
  transmute(
    STQU_ID, ID,
    PEN = PEN_CANON,
    OUTCOMES_CRED,
    INSTITUTION_CODE,            # BGS institution code
    PSI_CODE,                    # STP institution code
    YEAR,                        # BGS survey year
    PSI_AWARD_SCHOOL_YEAR,       # STP award year
    BGS_FINAL_CIP_CODE_4 = CIP_4DIGIT_NO_PERIOD,
    BGS_FINAL_CIP_CODE_4_NAME = CIP4DIG_NAME,
    STP_FINAL_CIP_CODE_4 = STP_CIP_CODE_4,
    STP_FINAL_CIP_CODE_4_NAME = STP_CIP_CODE_4_NAME,
    BGS_FINAL_CIP_CODE_2 = CIP2DIG,
    BGS_FINAL_CIP_CODE_2_NAME = CIP2DIG_NAME,
    STP_FINAL_CIP_CODE_2 = STP_CIP_CODE_2,
    STP_FINAL_CIP_CODE_2_NAME = STP_CIP_CODE_2_NAME,
    BGS_PROGRAM_CODE = CPC,
    BGS_PROGRAM_DESC = PROGRAM,
    STP_PROGRAM_CODE = PSI_PROGRAM_CODE,
    STP_PROGRAM_DESC = PSI_CREDENTIAL_PROGRAM_DESCRIPTION
  ) %>%
  distinct()
log_info(glue::glue(
  "Step 5: PEN join produced {nrow(bgs_matching_v2)} case rows ({nrow(distinct(bgs_matching_v2, STQU_ID, ID))} unique STQU_ID x ID)"
))

# ==============================================================================
# STEP 6: Match flags (original Part 3B)
# ==============================================================================
# WHAT: three atomic flags (institution, award year, CIP agreement) and two
#       compound confidence flags -- the original's Part 3B, with ONE change:
#       MATCH_INST is computed by joining the dbo alias table instead of a
#       hardcoded case_when (ticket 05).
#
# MATCH_AWARD_SCHOOL_YEAR keeps the original's explicit survey-year ->
# award-year table VERBATIM (out-of-scope note on the cip-matching map: the
# annual manual edit convention is retained). BGS surveys a cohort ~2 years
# after award, so survey year Y pairs with award years (Y-3)/(Y-2) and
# (Y-2)/(Y-1).

bgs_matching_v2 <- bgs_matching_v2 %>%
  left_join(
    alias_lookup %>% transmute(PSI_CODE, INSTITUTION_CODE, IS_ALIAS = "Yes"),
    by = c("PSI_CODE", "INSTITUTION_CODE")
  ) %>%
  mutate(
    # Institution match: same code, or a pair the alias table covers
    MATCH_INST = case_when(
      PSI_CODE == INSTITUTION_CODE ~ "Yes",
      !is.na(IS_ALIAS) ~ "Yes",
      TRUE ~ NA_character_
    )
  ) %>%
  select(-IS_ALIAS) %>%
  mutate(
    # Award-year lag window (verbatim from the original's annual table)
    MATCH_AWARD_SCHOOL_YEAR = case_when(
      (YEAR == 2000 & PSI_AWARD_SCHOOL_YEAR %in% c("1997/1998", "1998/1999")) |
        (YEAR == 2002 & PSI_AWARD_SCHOOL_YEAR %in% c("1999/2000", "2000/2001")) |
        (YEAR == 2004 & PSI_AWARD_SCHOOL_YEAR %in% c("2001/2002", "2002/2003")) |
        (YEAR == 2006 & PSI_AWARD_SCHOOL_YEAR %in% c("2003/2004", "2004/2005")) |
        (YEAR == 2008 & PSI_AWARD_SCHOOL_YEAR %in% c("2005/2006", "2006/2007")) |
        (YEAR == 2009 & PSI_AWARD_SCHOOL_YEAR %in% c("2006/2007", "2007/2008")) |
        (YEAR == 2010 & PSI_AWARD_SCHOOL_YEAR %in% c("2007/2008", "2008/2009")) |
        (YEAR == 2011 & PSI_AWARD_SCHOOL_YEAR %in% c("2008/2009", "2009/2010")) |
        (YEAR == 2012 & PSI_AWARD_SCHOOL_YEAR %in% c("2009/2010", "2010/2011")) |
        (YEAR == 2013 & PSI_AWARD_SCHOOL_YEAR %in% c("2010/2011", "2011/2012")) |
        (YEAR == 2014 & PSI_AWARD_SCHOOL_YEAR %in% c("2011/2012", "2012/2013")) |
        (YEAR == 2015 & PSI_AWARD_SCHOOL_YEAR %in% c("2012/2013", "2013/2014")) |
        (YEAR == 2016 & PSI_AWARD_SCHOOL_YEAR %in% c("2013/2014", "2014/2015")) |
        (YEAR == 2017 & PSI_AWARD_SCHOOL_YEAR %in% c("2014/2015", "2015/2016")) |
        (YEAR == 2018 & PSI_AWARD_SCHOOL_YEAR %in% c("2015/2016", "2016/2017")) |
        (YEAR == 2019 & PSI_AWARD_SCHOOL_YEAR %in% c("2016/2017", "2017/2018")) |
        (YEAR == 2020 & PSI_AWARD_SCHOOL_YEAR %in% c("2017/2018", "2018/2019")) |
        (YEAR == 2021 & PSI_AWARD_SCHOOL_YEAR %in% c("2018/2019", "2019/2020")) |
        (YEAR == 2022 & PSI_AWARD_SCHOOL_YEAR %in% c("2019/2020", "2020/2021")) |
        (YEAR == 2023 & PSI_AWARD_SCHOOL_YEAR %in% c("2020/2021", "2021/2022")) |
        (YEAR == 2024 & PSI_AWARD_SCHOOL_YEAR %in% c("2021/2022", "2022/2023")) |
        (YEAR == 2025 & PSI_AWARD_SCHOOL_YEAR %in% c("2022/2023", "2023/2024")) ~
        "Yes",
      TRUE ~ NA_character_
    ),
    # CIP agreement flags
    MATCH_CIP_CODE_4 = if_else(
      !is.na(BGS_FINAL_CIP_CODE_4) & !is.na(STP_FINAL_CIP_CODE_4) &
        BGS_FINAL_CIP_CODE_4 == STP_FINAL_CIP_CODE_4,
      "Yes", NA_character_
    ),
    MATCH_CIP_CODE_2 = if_else(
      !is.na(BGS_FINAL_CIP_CODE_2) & !is.na(STP_FINAL_CIP_CODE_2) &
        BGS_FINAL_CIP_CODE_2 == STP_FINAL_CIP_CODE_2,
      "Yes", NA_character_
    ),
    # Compound confidence flags
    MATCH_ALL_3_CIP4_FLAG = if_else(
      MATCH_CIP_CODE_4 == "Yes" & MATCH_AWARD_SCHOOL_YEAR == "Yes" &
        MATCH_INST == "Yes",
      "Yes", NA_character_
    ),
    MATCH_ALL_3_CIP2_FLAG = if_else(
      MATCH_CIP_CODE_2 == "Yes" & MATCH_AWARD_SCHOOL_YEAR == "Yes" &
        MATCH_INST == "Yes",
      "Yes", NA_character_
    )
  )
log_info(glue::glue(
  "Step 6: flags computed -- MATCH_INST Yes {sum(!is.na(bgs_matching_v2$MATCH_INST) & bgs_matching_v2$MATCH_INST == 'Yes')}, MATCH_AWARD_SCHOOL_YEAR Yes {sum(!is.na(bgs_matching_v2$MATCH_AWARD_SCHOOL_YEAR) & bgs_matching_v2$MATCH_AWARD_SCHOOL_YEAR == 'Yes')}, ALL3_CIP4 {sum(!is.na(bgs_matching_v2$MATCH_ALL_3_CIP4_FLAG))}, ALL3_CIP2 {sum(!is.na(bgs_matching_v2$MATCH_ALL_3_CIP2_FLAG))}"
))

# ==============================================================================
# STEP 7: High-confidence assignment (original Part 3B tail)
# ==============================================================================
# WHAT: rows where inst+year+4-digit CIP all agree get their FINAL CIP
#       immediately (BGS == STP there, so the choice is vacuous).
bgs_matching_v2 <- bgs_matching_v2 %>%
  mutate(
    FINAL_CIP_CODE_4 = if_else(
      MATCH_ALL_3_CIP4_FLAG == "Yes", BGS_FINAL_CIP_CODE_4, NA_character_
    ),
    FINAL_CIP_CODE_2 = if_else(
      MATCH_ALL_3_CIP4_FLAG == "Yes", BGS_FINAL_CIP_CODE_2, NA_character_
    ),
    # Name/cluster placeholders -- filled from the CIP2021 lookups in Step 13
    # once the FINAL codes are settled (in-memory equivalent of the original's
    # sql("CAST(NULL AS VARCHAR(255))") placeholder columns).
    FINAL_CIP_CODE_4_NAME = NA_character_,
    FINAL_CIP_CODE_2_NAME = NA_character_,
    FINAL_CIP_CLUSTER_CODE = NA_character_,
    FINAL_CIP_CLUSTER_NAME = NA_character_,
    USE_BGS_CIP = if_else(MATCH_ALL_3_CIP4_FLAG == "Yes", "Yes", NA_character_),
    FINAL_CONSIDER_A_MATCH = if_else(
      MATCH_ALL_3_CIP4_FLAG == "Yes", "Yes", NA_character_
    ),
    FINAL_PROBABLE_MATCH = NA_character_,
    MATCH_RULE = if_else(
      MATCH_ALL_3_CIP4_FLAG == "Yes", "exact_4digit_inst_year", NA_character_
    )
  )

# ==============================================================================
# STEP 8: Decision points for 2-digit-only matches (original t1/t2)
# ==============================================================================
# WHAT: collapse the CIP2-flagged / CIP4-unflagged rows to unique program
#       combinations -- the units the decision tree in Step 9 decides.
# WHY:  ~134K case rows reduce to ~1.6K decision points; deciding at the
#       program level keeps every record of the same program consistent.
t1 <- bgs_matching_v2 %>%
  filter(is.na(MATCH_ALL_3_CIP4_FLAG), MATCH_ALL_3_CIP2_FLAG == "Yes") %>%
  count(
    INSTITUTION_CODE, PSI_CODE, YEAR, PSI_AWARD_SCHOOL_YEAR,
    BGS_PROGRAM_CODE, STP_PROGRAM_CODE, BGS_PROGRAM_DESC, STP_PROGRAM_DESC,
    BGS_FINAL_CIP_CODE_4, BGS_FINAL_CIP_CODE_4_NAME,
    STP_FINAL_CIP_CODE_4, STP_FINAL_CIP_CODE_4_NAME,
    BGS_FINAL_CIP_CODE_2, BGS_FINAL_CIP_CODE_2_NAME,
    STP_FINAL_CIP_CODE_2, STP_FINAL_CIP_CODE_2_NAME,
    MATCH_INST, MATCH_AWARD_SCHOOL_YEAR, MATCH_CIP_CODE_4,
    MATCH_ALL_3_CIP4_FLAG, MATCH_CIP_CODE_2, MATCH_ALL_3_CIP2_FLAG,
    name = "Expr1"
  )

t2 <- bgs_matching_v2 %>%
  filter(is.na(MATCH_ALL_3_CIP4_FLAG), MATCH_ALL_3_CIP2_FLAG == "Yes") %>%
  count(
    INSTITUTION_CODE, PSI_CODE, YEAR, PSI_AWARD_SCHOOL_YEAR,
    BGS_PROGRAM_CODE, STP_PROGRAM_CODE, BGS_PROGRAM_DESC, STP_PROGRAM_DESC,
    BGS_FINAL_CIP_CODE_4, BGS_FINAL_CIP_CODE_4_NAME,
    STP_FINAL_CIP_CODE_4, STP_FINAL_CIP_CODE_4_NAME,
    MATCH_ALL_3_CIP2_FLAG,
    name = "Expr1"
  )
log_info(glue::glue(
  "Step 8: decision points -- t1 {nrow(t1)}, t2 {nrow(t2)} program combinations"
))

# ==============================================================================
# STEP 9: The deterministic decision tree (original Part 3B+, with trace)
# ==============================================================================
# WHAT: assign CIP_TO_USE to every t2 decision point through ordered rules,
#       recording WHICH rule decided each point in DECISION_RULE. The rule
#       ORDER matters and mirrors the original exactly.
# WHY:  each rule encodes a distinct trust heuristic:
#         (1) general-program: a "general" CIP (e.g. 1101 General
#             Agriculture) is less informative -- prefer the other side's
#             specific code;
#         (2A/2B) cross-validation: if the same STP (resp. BGS) program
#             coding appears in a 4-digit-agreeing match elsewhere at the
#             institution, that side's coding proved reliable -- follow it;
#         (3) custom choices: known systematic BGS/STP codebook disagreements
#             (from the shared lookup, not code);
#         (4) default STP: the residual is mostly double majors where BGS and
#             STP recorded the two programs in different orders; STP is the
#             modelling convention.

# -- Rule 1: general-program preference ---------------------------------------
general_cip4 <- c(
  "1101", "1301", "1401", "1901", "2301", "2401", "2601",
  "4001", "4201", "4501", "5001", "5201", "5501"
)
matched_2d <- t2 %>%
  mutate(
    CIP_TO_USE = case_when(
      BGS_FINAL_CIP_CODE_4 %in% general_cip4 ~ "STP",
      STP_FINAL_CIP_CODE_4 %in% general_cip4 ~ "BGS"
    ),
    # Trace mirrors CIP_TO_USE's precedence: BGS-general fires first
    DECISION_RULE = case_when(
      BGS_FINAL_CIP_CODE_4 %in% general_cip4 ~ "general_program_bgs_generic",
      STP_FINAL_CIP_CODE_4 %in% general_cip4 ~ "general_program_stp_generic"
    )
  )

# -- Rule 2A: cross-validate against 4-digit evidence, STP perspective --------
# Evidence set: institution + STP program + the STP CIP under question, with
# the BGS CIP that appeared on the (2-digit-only) rows carrying them. Ported
# verbatim from the original (including its column semantics).
evidence_stp <- t1 %>%
  distinct(
    INSTITUTION_CODE, STP_PROGRAM_CODE, STP_PROGRAM_DESC,
    CIP = BGS_FINAL_CIP_CODE_4, STP_FINAL_CIP_CODE_4
  )
matched_2d <- matched_2d %>%
  left_join(evidence_stp, by = c(
    "INSTITUTION_CODE", "STP_PROGRAM_CODE", "STP_PROGRAM_DESC",
    "STP_FINAL_CIP_CODE_4"
  )) %>%
  mutate(
    CIP_TO_USE = coalesce(CIP_TO_USE, if_else(!is.na(CIP), "STP", NA_character_)),
    DECISION_RULE = coalesce(
      DECISION_RULE,
      if_else(!is.na(CIP), "xval_stp_4digit_evidence", NA_character_)
    )
  ) %>%
  select(-CIP) %>%
  distinct()

# -- Rule 2B: cross-validate, BGS perspective (many-to-many by design) -------
evidence_bgs <- t1 %>%
  distinct(
    INSTITUTION_CODE, BGS_PROGRAM_CODE, BGS_PROGRAM_DESC,
    BGS_FINAL_CIP_CODE_4, CIP = STP_FINAL_CIP_CODE_4
  )
matched_2d <- matched_2d %>%
  left_join(evidence_bgs, by = c(
    "INSTITUTION_CODE", "BGS_PROGRAM_CODE", "BGS_PROGRAM_DESC",
    "BGS_FINAL_CIP_CODE_4"
  )) %>%
  mutate(
    CIP_TO_USE = coalesce(CIP_TO_USE, if_else(!is.na(CIP), "BGS", NA_character_)),
    DECISION_RULE = coalesce(
      DECISION_RULE,
      if_else(!is.na(CIP), "xval_bgs_4digit_evidence", NA_character_)
    )
  ) %>%
  select(-CIP) %>%
  distinct()

# -- Rule 3: custom choices from the shared lookup ----------------------------
# Applied by joining the cip_pair_rule rows on (BGS CIP4, STP CIP4).
matched_2d <- matched_2d %>%
  left_join(
    custom_pair_rules %>%
      transmute(
        BGS_FINAL_CIP_CODE_4 = BGS_CIP4,
        STP_FINAL_CIP_CODE_4 = STP_CIP4,
        lookup_CIP_TO_USE = CIP_TO_USE
      ),
    by = c("BGS_FINAL_CIP_CODE_4", "STP_FINAL_CIP_CODE_4")
  ) %>%
  mutate(
    CIP_TO_USE = coalesce(CIP_TO_USE, lookup_CIP_TO_USE),
    DECISION_RULE = coalesce(
      DECISION_RULE,
      if_else(!is.na(lookup_CIP_TO_USE), "custom_cip_choice", NA_character_)
    )
  ) %>%
  select(-lookup_CIP_TO_USE)

# -- Rule 4: default STP (residual double majors) -----------------------------
matched_2d <- matched_2d %>%
  mutate(
    CIP_TO_USE = coalesce(CIP_TO_USE, "STP"),
    DECISION_RULE = coalesce(DECISION_RULE, "default_stp_double_majors")
  )
log_info(glue::glue(
  "Step 9: decision tree assigned CIP_TO_USE to all {nrow(matched_2d)} decision points"
))
print(matched_2d %>% count(DECISION_RULE, CIP_TO_USE))

# ==============================================================================
# STEP 10: Apply decisions to the case table (original join-back)
# ==============================================================================
# WHAT: join the decision points back to the full case table on the original's
#       join keys and populate FINAL_CIP_*/USE_BGS_CIP for the 2-digit rows.
# HOW:  local dplyr joins match NA-to-NA, reproducing the original's explicit
#       na_matches = "na" SQL behaviour.
join_keys <- c(
  "INSTITUTION_CODE", "PSI_CODE", "YEAR", "PSI_AWARD_SCHOOL_YEAR",
  "BGS_PROGRAM_CODE", "STP_PROGRAM_CODE", "BGS_PROGRAM_DESC", "STP_PROGRAM_DESC",
  "BGS_FINAL_CIP_CODE_4", "STP_FINAL_CIP_CODE_4", "MATCH_ALL_3_CIP2_FLAG"
)
bgs_matching_v2 <- bgs_matching_v2 %>%
  left_join(
    matched_2d %>% select(all_of(join_keys), CIP_TO_USE, DECISION_RULE),
    by = join_keys
  ) %>%
  mutate(
    FINAL_CIP_CODE_4 = coalesce(
      FINAL_CIP_CODE_4,
      if_else(CIP_TO_USE == "BGS", BGS_FINAL_CIP_CODE_4, STP_FINAL_CIP_CODE_4)
    ),
    FINAL_CIP_CODE_2 = coalesce(
      FINAL_CIP_CODE_2,
      if_else(CIP_TO_USE == "BGS", BGS_FINAL_CIP_CODE_2, STP_FINAL_CIP_CODE_2)
    ),
    USE_BGS_CIP = coalesce(USE_BGS_CIP, if_else(CIP_TO_USE == "BGS", "Yes", "No")),
    FINAL_CONSIDER_A_MATCH = coalesce(FINAL_CONSIDER_A_MATCH, "Yes"),
    MATCH_RULE = coalesce(MATCH_RULE, DECISION_RULE)
  ) %>%
  select(-CIP_TO_USE, -DECISION_RULE)

# ==============================================================================
# STEP 11: Residual caseload (original Part 3C replacement)
# ==============================================================================
# WHAT: rows that matched on institution + award year but on NEITHER CIP flag.
#       The original exported these for manual review (borderline CSV); v2
#       assigns the modelling-convention default (STP) with the trace
#       no_cip_match_default_stp, and exports the caseload for review so the
#       analyst can still curate via the custom-choices lookup next cycle.
residual <- bgs_matching_v2 %>%
  filter(
    is.na(FINAL_CIP_CODE_4),
    MATCH_INST == "Yes", MATCH_AWARD_SCHOOL_YEAR == "Yes",
    is.na(MATCH_ALL_3_CIP4_FLAG), is.na(MATCH_ALL_3_CIP2_FLAG)
  )
log_info(glue::glue(
  "Step 11: residual borderline caseload (inst+year matched, no CIP agreement): {nrow(residual)} rows / {nrow(distinct(residual, INSTITUTION_CODE, BGS_PROGRAM_CODE, STP_PROGRAM_CODE))} program combinations -- defaulting to STP"
))
bgs_matching_v2 <- bgs_matching_v2 %>%
  mutate(
    FINAL_CIP_CODE_4 = coalesce(
      FINAL_CIP_CODE_4,
      if_else(
        MATCH_INST == "Yes" & MATCH_AWARD_SCHOOL_YEAR == "Yes" &
          is.na(MATCH_ALL_3_CIP4_FLAG) & is.na(MATCH_ALL_3_CIP2_FLAG),
        STP_FINAL_CIP_CODE_4, NA_character_
      )
    ),
    FINAL_CIP_CODE_2 = coalesce(
      FINAL_CIP_CODE_2,
      if_else(
        MATCH_INST == "Yes" & MATCH_AWARD_SCHOOL_YEAR == "Yes" &
          is.na(MATCH_ALL_3_CIP4_FLAG) & is.na(MATCH_ALL_3_CIP2_FLAG),
        STP_FINAL_CIP_CODE_2, NA_character_
      )
    ),
    USE_BGS_CIP = coalesce(USE_BGS_CIP, "No"),
    FINAL_PROBABLE_MATCH = coalesce(
      FINAL_PROBABLE_MATCH,
      if_else(
        MATCH_INST == "Yes" & MATCH_AWARD_SCHOOL_YEAR == "Yes" &
          is.na(MATCH_ALL_3_CIP4_FLAG) & is.na(MATCH_ALL_3_CIP2_FLAG),
        "Yes", NA_character_
      )
    ),
    MATCH_RULE = coalesce(
      MATCH_RULE,
      if_else(
        MATCH_INST == "Yes" & MATCH_AWARD_SCHOOL_YEAR == "Yes" &
          is.na(MATCH_ALL_3_CIP4_FLAG) & is.na(MATCH_ALL_3_CIP2_FLAG),
        "no_cip_match_default_stp", NA_character_
      )
    )
  )
if (nrow(residual) > 0) {
  write.csv(
    residual %>%
      select(
        INSTITUTION_CODE, PSI_CODE, YEAR, PSI_AWARD_SCHOOL_YEAR,
        BGS_PROGRAM_CODE, BGS_PROGRAM_DESC, STP_PROGRAM_CODE, STP_PROGRAM_DESC,
        BGS_FINAL_CIP_CODE_4, BGS_FINAL_CIP_CODE_4_NAME,
        STP_FINAL_CIP_CODE_4, STP_FINAL_CIP_CODE_4_NAME
      ),
    file.path(diag_dir, "bgs-v2-residual-manual-review.csv"),
    row.names = FALSE
  )
  log_info("Step 11: exported residual caseload to .scratch/cip-matching/diagnostics/bgs-v2-residual-manual-review.csv")
}

# Rows that never matched inst+year keep FINAL = NA and the trace records why
# (the credential-side fallback in Step 14 covers them there).
bgs_matching_v2 <- bgs_matching_v2 %>%
  mutate(
    MATCH_RULE = coalesce(
      MATCH_RULE,
      if_else(
        is.na(FINAL_CIP_CODE_4), "no_inst_year_match", NA_character_
      )
    )
  )

# ==============================================================================
# STEP 12: Alias-inference report (ticket 05's proposal engine)
# ==============================================================================
# WHAT: among PEN-matched case rows where MATCH_INST is NA, count how often
#       each (PSI_CODE, INSTITUTION_CODE) pair co-occurs. High-frequency pairs
#       are almost certainly unmapped aliases (a PEN matching both systems at
#       two differently-coded institutions).
# WHY:  this replaces the original's manual eyeball of unmatched pairs as the
#       discovery mechanism. Proposals are REPORTED ONLY -- an analyst reviews
#       and adds confirmed pairs to dbo.T_BGS_STP_INST_ALIAS_v2_r.
unmatched_pairs <- bgs_matching_v2 %>%
  filter(is.na(MATCH_INST)) %>%
  count(PSI_CODE, INSTITUTION_CODE, name = "pair_n") %>%
  arrange(desc(pair_n))
alias_proposals <- unmatched_pairs %>% filter(pair_n >= 10)
if (nrow(alias_proposals) > 0) {
  write.csv(
    alias_proposals,
    file.path(diag_dir, "bgs-v2-alias-proposals.csv"),
    row.names = FALSE
  )
}
log_info(glue::glue(
  "Step 12: alias inference -- {nrow(unmatched_pairs)} unmatched pairs, {nrow(alias_proposals)} with pair_n >= 10 proposed (report: diagnostics/bgs-v2-alias-proposals.csv)"
))

# ==============================================================================
# STEP 13: Final names + clusters (original Part 3D)
# ==============================================================================
# WHAT: attach CIP2021 names and cluster code/name to every resolved FINAL
#       CIP from the shared INFOWARE lookups.
cip_4_lu <- sch_tbl("INFOWARE_L_CIP_4DIGITS_CIP2021", schema = shareschema) %>%
  transmute(LCP4_CD, LCP4_CIP_4DIGITS_NAME = LCP4_DIGITS_NAME) %>%
  collect()
cip_2_lu <- sch_tbl("INFOWARE_L_CIP_2DIGITS_CIP2021", schema = shareschema) %>%
  transmute(LCP2_CD, LCP2_DIGITS_NAME, LCP2_LCIPPC_CD, LCP2_LCIPPC_NAME) %>%
  collect()

bgs_matching_v2 <- bgs_matching_v2 %>%
  left_join(cip_4_lu, by = c("FINAL_CIP_CODE_4" = "LCP4_CD")) %>%
  left_join(cip_2_lu, by = c("FINAL_CIP_CODE_2" = "LCP2_CD")) %>%
  mutate(
    FINAL_CIP_CODE_4_NAME = coalesce(LCP4_CIP_4DIGITS_NAME, FINAL_CIP_CODE_4_NAME),
    FINAL_CIP_CODE_2_NAME = coalesce(LCP2_DIGITS_NAME, FINAL_CIP_CODE_2_NAME),
    FINAL_CIP_CLUSTER_CODE = LCP2_LCIPPC_CD,
    FINAL_CIP_CLUSTER_NAME = LCP2_LCIPPC_NAME
  ) %>%
  select(-LCP4_CIP_4DIGITS_NAME, -LCP2_DIGITS_NAME,
         -LCP2_LCIPPC_CD, -LCP2_LCIPPC_NAME)

# ==============================================================================
# STEP 14: Credential-side output (original Parts 4A + 4B, simplified)
# ==============================================================================
# WHAT: build one row per BGS credential ID with FINAL_CIP_*:
#         (a) PEN-matched rows inherit the case decision (Step 7/10/11) --
#             CONSIDER_A_MATCH decisions first, PROBABLE_MATCH where still
#             empty, one decision per ID (conflicts resolved by first);
#         (b) unmatched credential programs may still take a program_override
#             row from the custom-choices lookup (4B.5 semantics);
#         (c) anything still empty falls back to the credential's own
#             spine-cleaned STP CIP (original Step 3 fallback).
# WHY:  this is the table 02a-update-cred-non-dup reads as priority source 2;
#       every row leaves with a FINAL CIP and a MATCH_RULE explaining it.

# (a) case decisions, deduplicated to one row per ID
decisions_by_id <- bgs_matching_v2 %>%
  filter(!is.na(FINAL_CIP_CODE_4)) %>%
  arrange(ID, desc(FINAL_CONSIDER_A_MATCH == "Yes")) %>%
  distinct(
    ID, .keep_all = TRUE
  ) %>%
  select(
    ID,
    dec_FINAL_CIP_CODE_4 = FINAL_CIP_CODE_4,
    dec_FINAL_CIP_CODE_4_NAME = FINAL_CIP_CODE_4_NAME,
    dec_FINAL_CIP_CODE_2 = FINAL_CIP_CODE_2,
    dec_FINAL_CIP_CODE_2_NAME = FINAL_CIP_CODE_2_NAME,
    dec_FINAL_CIP_CLUSTER_CODE = FINAL_CIP_CLUSTER_CODE,
    dec_FINAL_CIP_CLUSTER_NAME = FINAL_CIP_CLUSTER_NAME,
    dec_USE_BGS_CIP = USE_BGS_CIP,
    dec_OUTCOMES_CIP_CODE_4 = BGS_FINAL_CIP_CODE_4,
    dec_OUTCOMES_CIP_CODE_4_NAME = BGS_FINAL_CIP_CODE_4_NAME,
    dec_FINAL_CONSIDER_A_MATCH = FINAL_CONSIDER_A_MATCH,
    dec_FINAL_PROBABLE_MATCH = FINAL_PROBABLE_MATCH,
    dec_MATCH_RULE = MATCH_RULE
  )

credential_out <- credential_side %>%
  left_join(decisions_by_id, by = "ID") %>%
  mutate(
    FINAL_CIP_CODE_4 = dec_FINAL_CIP_CODE_4,
    FINAL_CIP_CODE_4_NAME = dec_FINAL_CIP_CODE_4_NAME,
    FINAL_CIP_CODE_2 = dec_FINAL_CIP_CODE_2,
    FINAL_CIP_CODE_2_NAME = dec_FINAL_CIP_CODE_2_NAME,
    FINAL_CIP_CLUSTER_CODE = dec_FINAL_CIP_CLUSTER_CODE,
    FINAL_CIP_CLUSTER_NAME = dec_FINAL_CIP_CLUSTER_NAME,
    USE_BGS_CIP = dec_USE_BGS_CIP,
    OUTCOMES_CIP_CODE_4 = dec_OUTCOMES_CIP_CODE_4,
    OUTCOMES_CIP_CODE_4_NAME = dec_OUTCOMES_CIP_CODE_4_NAME,
    FINAL_CONSIDER_A_MATCH = dec_FINAL_CONSIDER_A_MATCH,
    FINAL_PROBABLE_MATCH = dec_FINAL_PROBABLE_MATCH,
    MATCH_RULE = dec_MATCH_RULE
  ) %>%
  select(-starts_with("dec_"))

# (b) program overrides for still-unmatched credential programs (4B.5)
credential_out <- credential_out %>%
  left_join(
    custom_program_overrides %>%
      transmute(
        PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
        ov_FINAL_CIP_CODE_4 = FINAL_CIP_CODE_4,
        ov_FINAL_CIP_CODE_2 = FINAL_CIP_CODE_2
      ),
    by = "PSI_CREDENTIAL_PROGRAM_DESCRIPTION"
  ) %>%
  mutate(
    override_applies = is.na(FINAL_CIP_CODE_4) & !is.na(ov_FINAL_CIP_CODE_4),
    FINAL_CIP_CODE_4 = if_else(
      override_applies, as.character(ov_FINAL_CIP_CODE_4), FINAL_CIP_CODE_4
    ),
    FINAL_CIP_CODE_2 = if_else(
      override_applies, as.character(ov_FINAL_CIP_CODE_2), FINAL_CIP_CODE_2
    ),
    FINAL_CIP_CODE_4_NAME = if_else(override_applies, NA_character_, FINAL_CIP_CODE_4_NAME),
    FINAL_CIP_CODE_2_NAME = if_else(override_applies, NA_character_, FINAL_CIP_CODE_2_NAME),
    FINAL_CIP_CLUSTER_CODE = if_else(override_applies, NA_character_, FINAL_CIP_CLUSTER_CODE),
    FINAL_CIP_CLUSTER_NAME = if_else(override_applies, NA_character_, FINAL_CIP_CLUSTER_NAME),
    MATCH_RULE = if_else(override_applies, "program_override_lookup", MATCH_RULE)
  ) %>%
  select(-ov_FINAL_CIP_CODE_4, -ov_FINAL_CIP_CODE_2, -override_applies)

# (c) STP fallback for everything still empty
credential_out <- credential_out %>%
  mutate(
    fallback = is.na(FINAL_CIP_CODE_4),
    FINAL_CIP_CODE_4 = if_else(fallback, STP_CIP_CODE_4, FINAL_CIP_CODE_4),
    FINAL_CIP_CODE_4_NAME = if_else(fallback, STP_CIP_CODE_4_NAME, FINAL_CIP_CODE_4_NAME),
    FINAL_CIP_CODE_2 = if_else(fallback, STP_CIP_CODE_2, FINAL_CIP_CODE_2),
    FINAL_CIP_CODE_2_NAME = if_else(fallback, STP_CIP_CODE_2_NAME, FINAL_CIP_CODE_2_NAME),
    USE_BGS_CIP = if_else(fallback, "No because no match", USE_BGS_CIP),
    MATCH_RULE = if_else(fallback, "stp_fallback_no_match", MATCH_RULE)
  ) %>%
  select(-fallback)

# Refill names/clusters for override + fallback rows from the lookups
credential_out <- credential_out %>%
  left_join(cip_4_lu, by = c("FINAL_CIP_CODE_4" = "LCP4_CD")) %>%
  left_join(cip_2_lu, by = c("FINAL_CIP_CODE_2" = "LCP2_CD")) %>%
  mutate(
    FINAL_CIP_CODE_4_NAME = coalesce(FINAL_CIP_CODE_4_NAME, LCP4_CIP_4DIGITS_NAME),
    FINAL_CIP_CODE_2_NAME = coalesce(FINAL_CIP_CODE_2_NAME, LCP2_DIGITS_NAME),
    FINAL_CIP_CLUSTER_CODE = coalesce(FINAL_CIP_CLUSTER_CODE, LCP2_LCIPPC_CD),
    FINAL_CIP_CLUSTER_NAME = coalesce(FINAL_CIP_CLUSTER_NAME, LCP2_LCIPPC_NAME)
  ) %>%
  select(-LCP4_CIP_4DIGITS_NAME, -LCP2_DIGITS_NAME,
         -LCP2_LCIPPC_CD, -LCP2_LCIPPC_NAME) %>%
  # Zero-pad CIP codes to 4/2 digits (codes like "50" mean "0050")
  mutate(
    FINAL_CIP_CODE_4 = str_pad(FINAL_CIP_CODE_4, 4, "left", "0"),
    FINAL_CIP_CODE_2 = str_pad(FINAL_CIP_CODE_2, 2, "left", "0")
  )
log_info(glue::glue(
  "Step 14: credential output -- {nrow(credential_out)} rows, {sum(!is.na(credential_out$FINAL_CIP_CODE_4))} with FINAL_CIP_CODE_4"
))
print(credential_out %>% count(MATCH_RULE))

# ==============================================================================
# STEP 15: Survey-side output (original Part 5A)
# ==============================================================================
# WHAT: one FINAL decision per STQU_ID (earliest survey YEAR on duplicates,
#       matching the original's slice_min) joined back onto the full survey
#       table. Rows with no decision keep NA FINALs -- 02b-1 coalesces over
#       the survey's own CIP there.
survey_decisions <- bgs_matching_v2 %>%
  filter(!is.na(FINAL_CIP_CODE_4)) %>%
  arrange(STQU_ID, YEAR) %>%
  distinct(STQU_ID, .keep_all = TRUE) %>%
  select(
    STQU_ID,
    src_FINAL_CIP_CODE_4 = FINAL_CIP_CODE_4,
    src_FINAL_CIP_CODE_4_NAME = FINAL_CIP_CODE_4_NAME,
    src_FINAL_CIP_CODE_2 = FINAL_CIP_CODE_2,
    src_FINAL_CIP_CODE_2_NAME = FINAL_CIP_CODE_2_NAME,
    src_FINAL_CIP_CLUSTER_CODE = FINAL_CIP_CLUSTER_CODE,
    src_FINAL_CIP_CLUSTER_NAME = FINAL_CIP_CLUSTER_NAME,
    src_USE_BGS_CIP = USE_BGS_CIP,
    src_STP_CIP_CODE_4 = STP_FINAL_CIP_CODE_4,
    src_STP_CIP_CODE_4_NAME = STP_FINAL_CIP_CODE_4_NAME,
    src_FINAL_CONSIDER_A_MATCH = FINAL_CONSIDER_A_MATCH,
    src_FINAL_PROBABLE_MATCH = FINAL_PROBABLE_MATCH,
    src_MATCH_RULE = MATCH_RULE
  )

survey_out <- survey_side %>%
  select(-PEN_CANON) %>%
  left_join(survey_decisions, by = "STQU_ID") %>%
  mutate(
    FINAL_CIP_CODE_4 = src_FINAL_CIP_CODE_4,
    FINAL_CIP_CODE_4_NAME = src_FINAL_CIP_CODE_4_NAME,
    FINAL_CIP_CODE_2 = src_FINAL_CIP_CODE_2,
    FINAL_CIP_CODE_2_NAME = src_FINAL_CIP_CODE_2_NAME,
    FINAL_CIP_CLUSTER_CODE = src_FINAL_CIP_CLUSTER_CODE,
    FINAL_CIP_CLUSTER_NAME = src_FINAL_CIP_CLUSTER_NAME,
    USE_BGS_CIP = src_USE_BGS_CIP,
    STP_CIP_CODE_4 = src_STP_CIP_CODE_4,
    STP_CIP_CODE_4_NAME = src_STP_CIP_CODE_4_NAME,
    USE_STP_CIP = if_else(!is.na(src_USE_BGS_CIP) & src_USE_BGS_CIP == "No", "Yes", NA_character_),
    FINAL_CONSIDER_A_MATCH = src_FINAL_CONSIDER_A_MATCH,
    FINAL_PROBABLE_MATCH = src_FINAL_PROBABLE_MATCH,
    MATCH_RULE = src_MATCH_RULE
  ) %>%
  select(-starts_with("src_"))
log_info(glue::glue(
  "Step 15: survey output -- {nrow(survey_out)} rows, {sum(!is.na(survey_out$FINAL_CIP_CODE_4))} with FINAL_CIP_CODE_4"
))

# ==============================================================================
# STEP 16: Write the three output tables (fresh _v2_r names, analyst schema)
# ==============================================================================
write_v2 <- function(table_name, df) {
  target <- Id(schema = my_schema, table = table_name)
  if (dbExistsTable(con, target)) dbRemoveTable(con, target)
  dbWriteTable(con, target, df)
  log_info(glue::glue("Wrote {my_schema}.{table_name}: {nrow(df)} rows"))
}

write_v2("BGS_Matching_STP_Credential_PEN_v2_r", bgs_matching_v2)
write_v2("Credential_Non_Dup_BGS_IDs_v2_r", credential_out)
write_v2("T_BGS_Data_Final_for_OutcomesMatching_v2_r", survey_out)

dbDisconnect(con)
log_info("==== 02a-bgs-program-matching-v2.R COMPLETE ====")
