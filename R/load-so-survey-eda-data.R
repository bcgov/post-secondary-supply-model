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

# =============================================================================
# SO survey EDA data pulls (2027 model run) - read-only snapshot of the
# student-outcome (SO) survey tables into .rds caches.
#
# PURPOSE
#   The EDA report (docs/eda-student-outcomes-2027.qmd) reads ONLY from the
#   .rds caches produced here (never directly from SQL). Running this script
#   once snapshots the benchmark + new pools so the report is reproducible
#   without live DB access.
#
#   Benchmark pool = 2019-2023 cycles, read from PSSM2023 (schema IDIR\MYNAME
#   for the analyst copies, dbo for originals).
#   New pool       = 2024-2025 cycles, read from PSSM2025 dbo.
#
#   IMPORTANT: the PSSM2025 response tables hold ALL cycles 2021-2025, not
#   just 2024-25. The new-pool cycle filter (SURVEY_YEAR / SUBM_CD /
#   COCI_SUBM_CD -> 2024-2025) is applied here, when the combined datasets
#   are built (see COMBINED ANALYSIS DATASETS below). Without it the new-pool
#   BGS response rate reads 32.6% (contaminated) instead of 25.9%.
#
#   Read-only: this script writes nothing to the databases.
#
# OUTPUT
#   23 raw .rds files in .scratch/student-outcome-survey-eda/cache/ (~85 MB),
#   4 combined analysis datasets (so_{bgs,trd,app,dac}_combined.rds), plus a
#   manifest.csv (db, table, file, rows, cols, pulled_at).
#
#   The EDA report (docs/eda-student-outcomes-2027.qmd) reads ONLY the
#   *_combined.rds files for the four survey response tables - all cycle
#   filtering, column alignment and pool labelling happens here.
#
# HOW THE CACHES ARE USED (report sections A-L of docs/eda-student-outcomes-2027.qmd)
#   A  dataset overview            response tables + DIST respondent counts
#   B  response rate by cycle      response tables (respondent flag / frame)
#   C  representativeness          COHORT_INFO (population) vs DIST (respondents)
#   D  missingness                 response tables + DIST (earnings)
#   E  employment status           response tables (labour-market columns)
#   F  earnings                    DIST (TOTL_SAL, winsorized P1/P99)
#   G  occupation (NOC broad)      response tables (NOC cols) x t_noc_broad_categories_r
#   H  further education           BGS: TOOK_FURTH_ED; DACSO: outc table (PFST_*)
#   I  satisfaction                DIST only (B03_R1 / E28 / C01)
#   J  program coverage            INFOWARE_PROGRAMS universe vs response tables
#   K  institution-level outcomes  response tables + DIST (earnings per institution)
#   L  program-level employment    response tables (INST x CIP, floor >= 30)
#
# RUN FROM REPO ROOT:  Rscript R/load-so-survey-eda-data.R
# =============================================================================

suppressMessages({
  library(DBI)
  library(odbc)
  library(config)
})

cfg <- config::get("decimal") # connection settings; NEVER print config.yml (holds Oracle creds)
ofile <- sub(
  "^--file=",
  "",
  commandArgs(trailingOnly = FALSE)[grepl(
    "^--file=",
    commandArgs(trailingOnly = FALSE)
  )][1]
)
CACHE <- file.path(
  dirname(normalizePath(ofile)),
  "..",
  ".scratch",
  "student-outcome-survey-eda",
  "cache"
)
dir.create(CACHE, showWarnings = FALSE, recursive = TRUE)
manifest <- list()

# analyst schema in PSSM2023 holding the benchmark-pool copies of the
# survey tables (originals for the new pool live in PSSM2025 dbo)
myschema <- config::get("myschema")

conn <- function(db) {
  DBI::dbConnect(
    odbc::odbc(),
    Driver = cfg$driver,
    Server = cfg$server,
    Database = db, # "PSSM2025" (new pool) or "PSSM2023" (benchmark)
    Trusted_Connection = "True"
  )
}

pull_table <- function(db, schema, table, file, prune = NULL) {
  # full-table read: SELECT * -> keep the pull a faithful snapshot
  sql <- paste0("SELECT * FROM [", schema, "].[", table, "]")
  con <- conn(db)
  on.exit(DBI::dbDisconnect(con), add = TRUE) # always release the connection
  d <- DBI::dbGetQuery(con, sql)
  if (!is.null(prune)) {
    # drop PII/pruned columns before caching (see cohort_prune below)
    keep <- setdiff(names(d), prune)
    d <- d[keep]
  }
  path <- file.path(CACHE, paste0(file, ".rds"))
  saveRDS(d, path)
  # record the pull in the manifest for reproducibility/traceability
  manifest[[length(manifest) + 1]] <<- data.frame(
    db = db,
    table = table,
    file = file,
    rows = nrow(d),
    cols = ncol(d),
    pulled_at = format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  )
  cat(sprintf(
    "pulled %s.%s -> %s (%d x %d)\n",
    db,
    table,
    file,
    nrow(d),
    ncol(d)
  ))
  invisible(d)
}

# =============================================================================
# TABLE BY TABLE REFERENCE
# =============================================================================
# --- New pool (PSSM2025, dbo) ------------------------------------------------
# t_bgs_data_final_r -> so_bgs_new.rds (123,452 x 30)
#   BGS response table; frame INCLUDES non-respondents (SRV_Y_N = 1 respondent).
#   Key cols: SRV_Y_N (respondent flag), SURVEY_YEAR (2021-2025; filter 2024:2025
#   for new pool), STQU_ID (unique key), INST, CIP_CODE_4 (4-digit CIP),
#   EMPLOYED / UNEMPLOYED / IN_LBR_FRC (employment status), NOC (NOC-2021 5-digit,
#   "XXXXX" = unknown), TOOK_FURTH_ED (further ed, -1/NA = missing), AGE, WEIGHT.
#   Used in: A, B, D, E, G, I, J, K, L.
#
# trd_data_r -> so_trd_new.rds (22,066 x 25)
#   TRD response table; frame includes non-respondents (RESPONDENT = 1 respondent).
#   Key cols: SUBM_CD (C_Outc21..25; filter C_Outc24/25 for new pool), KEY,
#   RESPONDENT, INST, TRD_AGE_AT_SURVEY, TRD_LABR_IN_LABOUR_MARKET /
#   TRD_LABR_EMPLOYED / TRD_LABR_UNEMPLOYED, NOC_CD (NOC-2021), LCP6_CD,
#   LCIP_LCP4_CD. Used in: A, B, C, D, E, G, J, K, L.
#
# t_appso_data_final_r -> so_appso_new.rds (23,166 x 25)
#   APPSO response table; RESPONDENT = 1 respondent.
#   Key cols: SUBM_CD (filter C_Outc24/25), KEY, RESPONDENT, INST,
#   APP_AGE_AT_SURVEY, APP_LABR_IN_LABOUR_MARKET / APP_LABR_EMPLOYED /
#   APP_LABR_UNEMPLOYED, NOC_CD (mostly uncollected ~8% coverage - occupation
#   section caveat), LCP6_CD, LCIP_LCP4_CD. Used in: A, B, C, D, E, G, J, K, L.
#
# t_dacso_data_part_1_stepa_r -> so_dacso_new.rds (147,020 x 36)
#   DACSO response table; RESPONDENT = 1 or NULL (NULL = NON-respondent - never 0).
#   Key cols: COCI_STQU_ID (unique), COCI_SUBM_CD (C_Outc21..25; filter
#   C_Outc24/25), COCI_INST_CD, RESPONDENT, COCI_AGE_AT_SURVEY,
#   LABR_IN_LABOUR_MARKET / LABR_EMPLOYED / LABR_UNEMPLOYED,
#   LABR_OCCUPATION_LNOC_CD (NOC-2021, "XXXXX" unknown), LCP6_CD, LCP4_CD,
#   PFST_FURSTDY_INCL_STILL_ATTD (further-ed, also in outc table - H uses outc).
#   Used in: A, B, C, D, E, G, J, K, L.
#
# INFOWARE_BGS_DIST_20_24 / INFOWARE_BGS_DIST_21_25 -> so_dist_20_24.rds /
#   so_dist_21_25.rds (201 cols each)
#   BGS "distribution" tables: enriched respondent file. Note institution col is
#   INSTITUTION_CODE (NOT "INST" as in the response tables). Only source of:
#   earnings (TOTL_SAL annual gross salary; also TOTL_SAL_FULL_TM, INCOME),
#   satisfaction (B03_R1 education satisfaction, E28 program relevance,
#   C01 instruction quality - all 1..4 with -1/NA missing), respondent sex
#   (GENDER_NAME) and age bands (AGE_RNG_J), occupation NOC_2021 (NOC-2021;
#   only 2023-2025 comparable - DIST_18_22 is NOC-2016). SURVEY_MODE 1=telephone
#   2=online (used for the response-rate trend investigation); SURVMON all NA
#   (no field-date data). Key cols: STQU_ID, RESPONDENT, YEAR (cycle),
#   INSTITUTION_CODE, TOTL_SAL, B03_R1, E28, C01, GENDER_NAME, AGE_RNG_J,
#   NOC_2021, SURVEY_MODE. Used in: C, D, E (benchmark employment source via
#   DIST_19_23 respondents), F, G, I, K.
#
# INFOWARE_C_OutC_Clean_Short_Resp_raw -> so_dacso_outc.rds (147,020 x 16)
#   DACSO "outcome" table with real further-education answers (stepa PFST_*
#   columns are unpopulated). Key: STQU_ID + SUBM_CD; MULTIPLE ROWS PER KEY -
#   report dedupes (prefer the row with non-NA PFST_FURSTDY_INCL_STILL_ATTD).
#   Key cols: STQU_ID, SUBM_CD, RESPONDENT (1/NULL), PFST_FURSTDY_INCL_STILL_ATTD
#   (1 = continuing, 0 = not), PFST_HAD_PREVIOUS_CDTL. Used in: H.
#
# t_noc_broad_categories_r -> so_noc_broad.rds (517 x 12)
#   NOC-2021 lookup: 5-digit UNIT_GROUP_CODE -> BROAD_CATEGORY_ENGLISH_NAME
#   (+ TEER / major / sub-major / minor groups). Used in: G (maps BGS NOC,
#   TRD NOC_CD, DACSO LABR_OCCUPATION_LNOC_CD to broad categories).
#
# INFOWARE_PROGRAMS -> so_programs.rds (6,736 x 25)
#   PSSM program universe. PRGM_ID (PK), PRGM_INST_CD (institution code),
#   PRGM_CREDENTIAL (e.g. ADGR/UT), LCIP_CD_CIP2021 (6-digit CIP),
#   IN_CURRENT_DATA_EXTRACT ("Y" = current program, n = 2,014). Used in: J
#   (coverage denominator: survey INST x CIP cells vs current universe,
#   BGS via INST x CIP4).
#
# INFOWARE_L_CIP_6DIGITS_CIP2021 -> so_cip6_lookup.rds (2,119 x 9)
#   CIP2021 6-digit classification lookup: LCP6_CD -> LCP6_DIGITS_NAME
#   (English CIP name). Full classification (covers all 6-digit survey codes
#   except a handful that sit outside the CIP2021 taxonomy - those display
#   blank in section L). Also carries CIP4/CIP2/LCIPPC rollups. Used in: L
#   (program-level tables show the CIP name next to the code).
#
# INFOWARE_L_CIP_4DIGITS_CIP2021 -> so_cip4_lookup.rds
#   CIP2021 4-digit classification lookup: LCP4_CD -> LCP4_DIGITS_NAME.
#   Used in: L (BGS program-level table - BGS response data only carries
#   CIP4, so its row label is the 4-digit CIP name).
#
# t_year_survey_year_r -> so_year_survey_new.rds (82 x 6)
#   Cycle -> calendar-year mapping: SUBM_CD, SURVEY_YEAR, AWARD_SCHOOL_YEAR,
#   PROJECTION_YEAR. Loaded for reference only (survey year ~= school year + 2;
#   projection-year context); not referenced by report logic.
#
# t_bgs_inst_recode_r -> so_bgs_inst_recode.rds (13 x 2)
#   BGS institution merger recodes: INST -> INST_RECODE. Loaded for reference;
#   report K table uses raw INST codes (kept for pipeline parity checks).
#
# INFOWARE_BGS_COHORT_INFO -> so_cohort_info_new.rds (343,315 x 32, PII-pruned)
#   BGS eligible cohort/contact table = representativeness POPULATION for
#   section C. Pruned of PII (names, addresses, phones, emails, PEN/STUDID,
#   indicators). Key cols: CHRT_ELIG (1 = eligible frame), SUBM_CD (cycles -
#   filter C_Outc24/25), GENDER (1/2/3/0), BRTHYEAR (age = SURVEY_DATE year -
#   BRTHYEAR), SURVEY_DATE, INST, INST_NAME, CIP4DIG. Used in: C.
#
# --- Benchmark pool (PSSM2023, IDIR\ANALYST) ----------------------------------
#   Same tables as above, analyst schema copies, cycles 2019-2023:
#   t_bgs_data_final_r  -> so_bgs_bench.rds  (121,074 x 31; extra FULL_TM_SCHOOL;
#                          LCIP_CD = old name of LCP6_CD - report aligns renames)
#   trd_data_r          -> so_trd_bench.rds
#   t_appso_data_final_r-> so_appso_bench.rds
#   t_dacso_data_part_1_stepa_r -> so_dacso_bench.rds
#   INFOWARE_BGS_DIST_18_22 / _19_23 -> so_dist_18_22.rds / so_dist_19_23.rds
#                          (DIST_18_22 extends the earnings series to 2018 but
#                          carries NOC-2016 -> NOT comparable in G)
#   INFOWARE_BGS_COHORT_INFO -> so_cohort_info_bench.rds (same PII prune)
#   INFOWARE_C_OutC_Clean_Short_Resp_raw -> so_dacso_outc_bench.rds
#   t_year_survey_year_r -> so_year_survey_bench.rds
#
# Benchmark respondent flag semantics identical to new (BGS SRV_Y_N == 1;
# TRD/APPSO/DACSO RESPONDENT == 1; DACSO NULL = non-respondent).
# =============================================================================

# ---------- NEW data (PSSM2025, dbo) ----------
# BGS response table (frame incl. non-respondents; SRV_Y_N == 1 = respondent).
# Report filters SURVEY_YEAR %in% 2024:2025 for the new pool (table holds 2021-2025).
pull_table("PSSM2025", "dbo", "t_bgs_data_final_r", "so_bgs_new")
# TRD response table (RESPONDENT == 1; SUBM_CD filter C_Outc24/25)
pull_table("PSSM2025", "dbo", "trd_data_r", "so_trd_new")
# APPSO response table (RESPONDENT == 1; SUBM_CD filter C_Outc24/25)
pull_table("PSSM2025", "dbo", "t_appso_data_final_r", "so_appso_new")
# DACSO response table (RESPONDENT = 1 or NULL; COCI_SUBM_CD filter C_Outc24/25)
pull_table("PSSM2025", "dbo", "t_dacso_data_part_1_stepa_r", "so_dacso_new")
# BGS distribution table: earnings/satisfaction/sex/age (INSTITUTION_CODE, not INST).
# 20_24 extends 18_22 series; 21_25 is the current new-pool series (NOC-2021).
pull_table("PSSM2025", "dbo", "INFOWARE_BGS_DIST_20_24", "so_dist_20_24")
pull_table("PSSM2025", "dbo", "INFOWARE_BGS_DIST_21_25", "so_dist_21_25")
# DACSO further-education outcome table (stepa PFST_* cols unpopulated; outc has
# real answers, multiple rows per key - report dedupes)
pull_table(
  "PSSM2025",
  "dbo",
  "INFOWARE_C_OutC_Clean_Short_Resp_raw",
  "so_dacso_outc"
)
# NOC-2021 lookup: 5-digit unit group -> broad category (section G mapping)
pull_table("PSSM2025", "dbo", "t_noc_broad_categories_r", "so_noc_broad")
# PSSM program universe (section J coverage denominator; IN_CURRENT_DATA_EXTRACT)
pull_table("PSSM2025", "dbo", "INFOWARE_PROGRAMS", "so_programs")
# CIP2021 6-digit classification: LCP6_CD -> LCP6_DIGITS_NAME English name
# (section L program-level tables display the CIP name beside the code)
pull_table("PSSM2025", "dbo", "INFOWARE_L_CIP_6DIGITS_CIP2021", "so_cip6_lookup")
# CIP2021 4-digit classification: LCP4_CD -> LCP4_DIGITS_NAME (BGS section L)
pull_table("PSSM2025", "dbo", "INFOWARE_L_CIP_4DIGITS_CIP2021", "so_cip4_lookup")
# cycle -> calendar-year mapping (reference only; not used by report logic)
pull_table("PSSM2025", "dbo", "t_year_survey_year_r", "so_year_survey_new")
# BGS institution merger recodes (reference; report K uses raw INST)
pull_table("PSSM2025", "dbo", "t_bgs_inst_recode_r", "so_bgs_inst_recode")

# COHORT_INFO is PII-heavy: prune contact/address/tel/email/indicator columns.
# Prune list computed from the live column inventory (robust to schema drift).
cohort_cols <- DBI::dbGetQuery(
  conn("PSSM2025"),
  "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'INFOWARE_BGS_COHORT_INFO'"
)$COLUMN_NAME
cohort_prune <- c(
  "STUDID",
  "PEN",
  "CLST_CD",
  "CLSTR",
  "CLST2_CD",
  "CLSTR2",
  "CPC",
  "COOP",
  "FULL_NAME",
  "ADM_BSIS",
  "INEL_REASON",
  "NO_PHONE",
  "SUSPECT_HOME_TEL",
  "SUSPECT_MAIL_TEL",
  "SUSPECT_WORK_TEL",
  "SURVEY_MODE_ORIG",
  "ONLINE_CD",
  "DBL_MAJOR_MNR",
  "DASHBOARD_PROGRAM_DIST",
  "INDIGENOUS_IDENTITY_INDICATOR",
  "FIRST_NATIONS_INDICATOR",
  "METIS_INDICATOR",
  "INUIT_INDICATOR",
  "CENSUS_DIVISIONS",
  "NEW_POSTAL",
  "EMAIL",
  "PHONE",
  "OPHONE1",
  "OPHONE2",
  "OPHONE3",
  "OPHONE4",
  "ADDRESS",
  "POSTAL",
  "FIRST_NAME",
  "PDP",
  "PLATFORM",
  "MOBILE_INDICATOR",
  "INSTITUTIONAL_EMAIL",
  "EMAIL_COUNT",
  grep(
    "TEL|PHONE|EMAIL|MAIL_|ADDR|CITY|PROV|COUNTRY|EFFDT|CONT_|CONT2|FAX|ALUMNI",
    cohort_cols,
    value = TRUE
  )
)
pull_table(
  "PSSM2025",
  "dbo",
  "INFOWARE_BGS_COHORT_INFO",
  "so_cohort_info_new",
  prune = cohort_prune
)

# ---------- BENCHMARK data (PSSM2023, IDIR\JDUAN + dbo) ----------
# Same tables as new pool, analyst schema copies, cycles 2019-2023 (no
# cycle filter here - benchmark pool = the whole 5-cycle window).
# BGS bench has extra FULL_TM_SCHOOL col and old LCIP_CD name (report aligns renames).
pull_table("PSSM2023", myschema, "t_bgs_data_final_r", "so_bgs_bench")
# TRD/APPSO bench response tables (same RESPONDENT semantics as new)
pull_table("PSSM2023", myschema, "trd_data_r", "so_trd_bench")
pull_table("PSSM2023", myschema, "t_appso_data_final_r", "so_appso_bench")
# DACSO bench response table (COCI_SUBM_CD cycles 2019-2023)
pull_table(
  "PSSM2023",
  myschema,
  "t_dacso_data_part_1_stepa_r",
  "so_dacso_bench"
)
# 18_22 extends the earnings series to 2018 but carries NOC-2016 ->
# NOT comparable in section G (occupation); 19_23 is the main benchmark.
pull_table(
  "PSSM2023",
  myschema,
  "INFOWARE_BGS_DIST_18_22",
  "so_dist_18_22"
)
pull_table(
  "PSSM2023",
  myschema,
  "INFOWARE_BGS_DIST_19_23",
  "so_dist_19_23"
)
# same PII prune as new pool, applied to the benchmark snapshot
pull_table(
  "PSSM2023",
  myschema,
  "INFOWARE_BGS_COHORT_INFO",
  "so_cohort_info_bench",
  prune = cohort_prune
)
pull_table(
  "PSSM2023",
  myschema,
  "t_year_survey_year_r",
  "so_year_survey_bench"
)

# =============================================================================
# COMBINED ANALYSIS DATASETS (the report's load interface)
# =============================================================================
# Everything the EDA report used to do at load time now happens here so the
# report only reads the *_combined.rds files below:
#   * new-pool cycle filter (SURVEY_YEAR / SUBM_CD / COCI_SUBM_CD -> 2024-2025)
#   * column alignment across pools (LCIP_CD -> LCP6_CD; cols only one pool
#     has are padded with NA)
#   * pool label, respondent flag (resp), cycle (survey year / C_Outc parse)
#   * rbind of benchmark + new into a single dataset per survey
POOL_B <- "Benchmark 2019-2023"
POOL_N <- "New 2024-2025"

# align column names/levels across pools before the rbind
align <- function(x, ref) {
  nm <- names(x)
  nm[nm == "LCIP_CD"] <- "LCP6_CD" # benchmark BGS used the old column name
  names(x) <- nm
  x[setdiff(names(ref), names(x))] <- NA # pad cols only one side has
  x
}

# CIP2021 6-digit classification: LCP6_CD -> LCP6_DIGITS_NAME (English name).
# Joined into the TRD/APPSO/DACSO combined datasets below for section L
# (BGS response table has no CIP6 - its program-level section uses CIP4).
cip6 <- readRDS(file.path(CACHE, "so_cip6_lookup.rds"))
# CIP2021 4-digit classification: LCP4_CD -> LCP4_DIGITS_NAME, for BGS (CIP4).
cip4 <- readRDS(file.path(CACHE, "so_cip4_lookup.rds"))

# respondent flag semantics per survey (BGS SRV_Y_N == 1; TRD/APPSO/DACSO
# RESPONDENT == 1; DACSO RESPONDENT is NULL for non-respondents, never 0)
resp <- function(d) !is.na(d) & d == 1
# cycle integer from a submission code (C_Outc25 -> 25)
cycle_of <- function(subm) as.integer(sub("C_Outc", "", subm))

# align + label + rbind the two pools, then persist
combine <- function(b, n) {
  b <- align(b, n)
  n <- align(n, b)
  b$pool <- POOL_B
  n$pool <- POOL_N
  rbind(b, n)
}

# save a derived dataset + record it in the manifest
save_combined <- function(file, d) {
  path <- file.path(CACHE, paste0(file, ".rds"))
  saveRDS(d, path)
  manifest[[length(manifest) + 1]] <<- data.frame(
    db = "derived",
    table = file,
    file = file,
    rows = nrow(d),
    cols = ncol(d),
    pulled_at = format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  )
  cat(sprintf("combined %s (%d x %d)\n", file, nrow(d), ncol(d)))
  invisible(d)
}

# BGS: new pool = SURVEY_YEAR 2024-2025 (the PSSM2025 table holds 2021-2025;
# without this filter the new-pool response rate reads 32.6% not 25.9%)
bgs_b <- readRDS(file.path(CACHE, "so_bgs_bench.rds"))
bgs_n <- readRDS(file.path(CACHE, "so_bgs_new.rds"))
bgs_n <- bgs_n[bgs_n$SURVEY_YEAR %in% 2024:2025, ]
bgs <- combine(bgs_b, bgs_n)
bgs$resp <- resp(bgs$SRV_Y_N) # respondent flag
bgs$cycle <- bgs$SURVEY_YEAR # cycle = survey year (BGS only)
# CIP4 English name for the BGS program-level rows (BGS has no CIP6)
bgs$LCP4_DIGITS_NAME <- cip4$LCP4_DIGITS_NAME[match(bgs$CIP_CODE_4, cip4$LCP4_CD)]
save_combined("so_bgs_combined", bgs)

# TRD: new pool = SUBM_CD C_Outc24/25 (table holds C_Outc21..25)
trd_b <- readRDS(file.path(CACHE, "so_trd_bench.rds"))
trd_n <- readRDS(file.path(CACHE, "so_trd_new.rds"))
trd_n <- trd_n[trd_n$SUBM_CD %in% c("C_Outc24", "C_Outc25"), ]
trd <- combine(trd_b, trd_n)
trd$resp <- resp(trd$RESPONDENT)
trd$cycle <- cycle_of(trd$SUBM_CD)
trd$LCP6_DIGITS_NAME <- cip6$LCP6_DIGITS_NAME[match(trd$LCP6_CD, cip6$LCP6_CD)]
save_combined("so_trd_combined", trd)

# APPSO: same cycle filter as TRD
app_b <- readRDS(file.path(CACHE, "so_appso_bench.rds"))
app_n <- readRDS(file.path(CACHE, "so_appso_new.rds"))
app_n <- app_n[app_n$SUBM_CD %in% c("C_Outc24", "C_Outc25"), ]
app <- combine(app_b, app_n)
app$resp <- resp(app$RESPONDENT)
app$cycle <- cycle_of(app$SUBM_CD)
app$LCP6_DIGITS_NAME <- cip6$LCP6_DIGITS_NAME[match(app$LCP6_CD, cip6$LCP6_CD)]
save_combined("so_app_combined", app)

# DACSO: cycle filter on COCI_SUBM_CD. CIP English names joined for section L
# (a handful of survey codes sit outside the CIP2021 classification -> NA).
dac_b <- readRDS(file.path(CACHE, "so_dacso_bench.rds"))
dac_n <- readRDS(file.path(CACHE, "so_dacso_new.rds"))
dac_n <- dac_n[dac_n$COCI_SUBM_CD %in% c("C_Outc24", "C_Outc25"), ]
dac <- combine(dac_b, dac_n)
dac$resp <- resp(dac$RESPONDENT)
dac$cycle <- cycle_of(dac$COCI_SUBM_CD)
dac$LCP6_DIGITS_NAME <- cip6$LCP6_DIGITS_NAME[match(dac$LCP6_CD, cip6$LCP6_CD)]
save_combined("so_dac_combined", dac)

# =============================================================================
# PULL-STAGE VERIFICATIONS (diagnostic only - safe to delete)
# =============================================================================

# ---------- Per-cycle verification counts ----------
# frame size + respondent count per cycle, per survey; catches accidental
# frame/respondent-flag changes between pools before the report reads caches
cat("\n--- PER-CYCLE COUNTS ---\n")
cycle_counts <- function(db, schema, table, cycle_col, flag_col) {
  con <- conn(db)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  sql <- paste0(
    "SELECT [",
    cycle_col,
    "] AS cycle, COUNT(*) AS n, SUM(CASE WHEN [",
    flag_col,
    "] = 1 THEN 1 ELSE 0 END) AS respondents FROM [",
    schema,
    "].[",
    table,
    "] GROUP BY [",
    cycle_col,
    "] ORDER BY [",
    cycle_col,
    "]"
  )
  d <- DBI::dbGetQuery(con, sql)
  cat(sprintf("%s.%s by %s:\n", db, table, cycle_col))
  print(d, row.names = FALSE)
  d
}
cycle_counts(
  "PSSM2023",
  myschema,
  "t_bgs_data_final_r",
  "SURVEY_YEAR",
  "SRV_Y_N"
)
cycle_counts("PSSM2025", "dbo", "t_bgs_data_final_r", "SURVEY_YEAR", "SRV_Y_N")
cycle_counts("PSSM2023", myschema, "trd_data_r", "SUBM_CD", "RESPONDENT")
cycle_counts("PSSM2025", "dbo", "trd_data_r", "SUBM_CD", "RESPONDENT")
cycle_counts(
  "PSSM2023",
  myschema,
  "t_appso_data_final_r",
  "SUBM_CD",
  "RESPONDENT"
)
cycle_counts("PSSM2025", "dbo", "t_appso_data_final_r", "SUBM_CD", "RESPONDENT")
cycle_counts(
  "PSSM2023",
  myschema,
  "t_dacso_data_part_1_stepa_r",
  "COCI_SUBM_CD",
  "RESPONDENT"
)
cycle_counts(
  "PSSM2025",
  "dbo",
  "t_dacso_data_part_1_stepa_r",
  "COCI_SUBM_CD",
  "RESPONDENT"
)

cat("\n--- DIST YEAR x RESPONDENT COUNTS ---\n")
# respondent/non-respondent split per cycle in each DIST table (C and F source)
for (t in c(
  "INFOWARE_BGS_DIST_18_22",
  "INFOWARE_BGS_DIST_19_23",
  "INFOWARE_BGS_DIST_20_24",
  "INFOWARE_BGS_DIST_21_25"
)) {
  db <- if (t %in% c("INFOWARE_BGS_DIST_18_22", "INFOWARE_BGS_DIST_19_23")) {
    "PSSM2023"
  } else {
    "PSSM2025"
  }
  schema <- if (db == "PSSM2023") myschema else "dbo"
  con <- conn(db)
  d <- DBI::dbGetQuery(
    con,
    paste0(
      "SELECT YEAR, RESPONDENT, COUNT(*) AS n FROM [",
      schema,
      "].[",
      t,
      "] GROUP BY YEAR, RESPONDENT ORDER BY YEAR, RESPONDENT"
    )
  )
  DBI::dbDisconnect(con)
  cat(sprintf("%s.%s\n", db, t))
  print(d, row.names = FALSE)
}

cat("\n--- VERIFICATION 1: TRD NOC_CD patterns (benchmark vs new) ---\n")
# sanity check that NOC_CD format is comparable across pools (nchar distribution)
con <- conn("PSSM2023")
b <- DBI::dbGetQuery(
  con,
  paste0(
    "SELECT TOP 5000 NOC_CD FROM [",
    myschema,
    "].[trd_data_r] WHERE NOC_CD IS NOT NULL AND NOC_CD <> ''"
  )
)
DBI::dbDisconnect(con)
con <- conn("PSSM2025")
n <- DBI::dbGetQuery(
  con,
  "SELECT TOP 5000 NOC_CD FROM dbo.[trd_data_r] WHERE NOC_CD IS NOT NULL AND NOC_CD <> ''"
)
DBI::dbDisconnect(con)
cat(
  "benchmark nchar dist:",
  table(nchar(b$NOC_CD)),
  "samples:",
  paste(head(unique(b$NOC_CD), 8), collapse = ", "),
  "\n"
)
cat(
  "new nchar dist:",
  table(nchar(n$NOC_CD)),
  "samples:",
  paste(head(unique(n$NOC_CD), 8), collapse = ", "),
  "\n"
)

cat("\n--- VERIFICATION 2: INFOWARE_PROGRAMS columns + current filter ---\n")
# find which column flags "current program" (IN_CURRENT_DATA_EXTRACT expected);
# sample rows + value distribution of any candidate column
con <- conn("PSSM2025")
p <- DBI::dbGetQuery(con, "SELECT TOP 3 * FROM dbo.INFOWARE_PROGRAMS")
DBI::dbDisconnect(con)
print(names(p))
cand <- intersect(
  c(
    "STATUS",
    "CURRENT",
    "CURRENT_IND",
    "END_DATE",
    "END_DT",
    "RECENT",
    "DATE_ADDED",
    "ACTIVE"
  ),
  names(p)
)
if (length(cand)) {
  for (c in cand) {
    con <- conn("PSSM2025")
    d <- DBI::dbGetQuery(
      con,
      paste0(
        "SELECT [",
        c,
        "], COUNT(*) AS n FROM dbo.INFOWARE_PROGRAMS GROUP BY [",
        c,
        "]"
      )
    )
    DBI::dbDisconnect(con)
    cat("candidate current-col:", c, "\n")
    print(d, row.names = FALSE)
  }
}

cat("\n--- VERIFICATION 3: institution name maps ---\n")
# confirm INST code sets are consistent across tables (join keys in report K/J)
con <- conn("PSSM2025")
d <- DBI::dbGetQuery(
  con,
  "SELECT DISTINCT INST, INST_NAME, INST_SHORT_NAME FROM dbo.INFOWARE_BGS_COHORT_INFO ORDER BY INST"
)
DBI::dbDisconnect(con)
cat("BGS INST -> names (", nrow(d), "distinct):\n")
print(d, row.names = FALSE)
# TRD INST codes (report K institution-level outcomes)
con <- conn("PSSM2025")
d <- DBI::dbGetQuery(
  con,
  "SELECT DISTINCT INST, COUNT(*) AS n FROM dbo.trd_data_r GROUP BY INST ORDER BY INST"
)
DBI::dbDisconnect(con)
cat("TRD INST codes (", nrow(d), "):\n")
print(d, row.names = FALSE)
con <- conn("PSSM2025")
d <- DBI::dbGetQuery(
  con,
  "SELECT DISTINCT INST, COUNT(*) AS n FROM dbo.t_appso_data_final_r GROUP BY INST ORDER BY INST"
)
DBI::dbDisconnect(con)
cat("APPSO INST codes (", nrow(d), "):\n")
print(d, row.names = FALSE)
con <- conn("PSSM2025")
d <- DBI::dbGetQuery(
  con,
  "SELECT DISTINCT COCI_INST_CD, COUNT(*) AS n FROM dbo.t_dacso_data_part_1_stepa_r GROUP BY COCI_INST_CD ORDER BY COCI_INST_CD"
)
DBI::dbDisconnect(con)
cat("DACSO INST codes (", nrow(d), "):\n")
print(d, row.names = FALSE)
con <- conn("PSSM2025")
d <- DBI::dbGetQuery(
  con,
  "SELECT DISTINCT PRGM_INST_CD, COUNT(*) AS n FROM dbo.INFOWARE_PROGRAMS GROUP BY PRGM_INST_CD ORDER BY PRGM_INST_CD"
)
DBI::dbDisconnect(con)
cat("INFOWARE_PROGRAMS INST codes (", nrow(d), "):\n")
print(d, row.names = FALSE)

cat("\n--- VERIFICATION 4: DACSO outc-table join integrity ---\n")
# outc table must join back to the stepa response table on (STQU_ID, SUBM_CD);
# check for orphan keys and row-multiplicity
con <- conn("PSSM2025")
d <- DBI::dbGetQuery(
  con,
  "SELECT COUNT(*) AS n, COUNT(DISTINCT CAST(STQU_ID AS varchar(50)) + '|' + SUBM_CD) AS keys FROM dbo.INFOWARE_C_OutC_Clean_Short_Resp_raw"
)
DBI::dbDisconnect(con)
print(d, row.names = FALSE)
con <- conn("PSSM2025")
d <- DBI::dbGetQuery(
  con,
  paste0(
    "SELECT COUNT(DISTINCT CAST(o.STQU_ID AS varchar(50)) + '|' + o.SUBM_CD) AS matched_keys FROM dbo.INFOWARE_C_OutC_Clean_Short_Resp_raw o ",
    "INNER JOIN dbo.t_dacso_data_part_1_stepa_r s ON CAST(s.COCI_STQU_ID AS varchar(50)) = CAST(o.STQU_ID AS varchar(50)) AND s.COCI_SUBM_CD = o.SUBM_CD"
  )
)
DBI::dbDisconnect(con)
print(d, row.names = FALSE)

cat("\n--- VERIFICATION 5: DIST column parity (benchmark vs new) ---\n")
# every DIST snapshot must carry the earnings/satisfaction/demographic columns
# the report reads (catches schema drift between the four DIST tables)
cols_all <- lapply(
  c("so_dist_18_22", "so_dist_19_23", "so_dist_20_24", "so_dist_21_25"),
  function(f) names(readRDS(file.path(CACHE, paste0(f, ".rds"))))
)
need <- c(
  "TOTL_SAL",
  "TOTL_SAL_FULL_TM",
  "INCOME",
  "SAL_RNG_J",
  "LABR_MJOB_HOURLY_GROSS_SALARY",
  "B03_R1",
  "E28",
  "C01",
  "GENDER_NAME",
  "AGE_RNG_J"
)
for (i in seq_along(cols_all)) {
  f <- c("so_dist_18_22", "so_dist_19_23", "so_dist_20_24", "so_dist_21_25")[i]
  missing <- setdiff(need, cols_all[[i]])
  cat(sprintf(
    "%s: %d cols; missing needed: %s\n",
    f,
    length(cols_all[[i]]),
    if (length(missing)) paste(missing, collapse = ", ") else "none"
  ))
}

# ---------- Manifest ----------
# one row per pulled table: provenance + row/col counts + pull timestamp
man <- do.call(rbind, manifest)
man$rows <- as.integer(man$rows)
man$cols <- as.integer(man$cols)
write.csv(man, file.path(CACHE, "manifest.csv"), row.names = FALSE)
cat("\nManifest written:", nrow(man), "tables\n")
