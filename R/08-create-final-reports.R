# ============================================================================
# 08 - CREATE FINAL REPORTS  (the last step in the pipeline)
#
# WHAT THIS SCRIPT DOES
#   Reads the outputs of the THREE model runs and writes two formatted Excel
#   workbooks (each with a graduate sheet + an occupation sheet): one internal,
#   one public. This script IS run directly (step 4 of
#   run_all_three_model_runs.r), so unlike 02b-07 it opens and closes its own
#   DB connection.
#
# THE THREE MODEL RUNS IT CONSUMES  (each written to <my_schema> by step 07)
#   tmp_tbl_Model                  - Regular run (public institutions only)
#   tmp_tbl_QI                     - Quality-Index run (alternate assumptions)
#   tmp_tbl_Model_Inc_Private_Inst - PTIB run (public + private institutions)
#   All three share the same row key (Expr1 = "AgeGroup-NOC-Region"), so they
#   line up cell-for-cell and can be compared by joining on Expr1.
#
# THE TWO QUALITY COLUMNS  (computed on the FIRST projection year only)
#   Quality Indicator (QI): |Regular - QI| / QI. How far the estimate moves
#       under the QI run's alternate assumptions - smaller = more stable.
#   Coverage Indicator (CI): Regular / PTIB = public supply / (public+private).
#       The share of total new supply delivered by PUBLIC institutions.
#
# TWO OUTPUTS
#   Internal: all regions, 5-digit NOC, includes the QI and CI columns.
#   Public:   BC only (region 5900), QI below 0.25, low-count/suppressed NOCs
#             collapsed into "Other" (99998); disclosure-rounded to nearest 5.
# ============================================================================

# FINAL REPORT TABLES
#
# This script creates the final excel spreadsheet for the internal and public releases
#
# It expects that you have run through the model 3 times, and produced:
#   tmp_table_model,
#   tmp_table_QI,
#   tmp_tbl_Model_Inc_Private_Inst
#
# It also expects that you have the table to produce graduate projections:
#   Graduate_Projections
#   cohort_program_distributions
#
# To get the correct age groups for grad projections, you must also have
# age group look up tables and the nice name of credentials
#
# It expects that you have a list of exclusionary tables to exclude programs
#  where Student Outcomes results not available or inappropriate:
#   - T_Exclude_from_Projections_LCIP4_CRED
#	  - T_Exclude_from_Projections_LCP4_CD
#	  - T_Exclude_from_Projections_PSSM_Credential
#   - T_Suppression_Public_Release_NOC
#
# It also expects that you have template excel sheets set up and ready
# Note that the actual wording of the User Guide page of these sheets will
# Likely need updating from year to year. The user guide is NOT updated in this code.

library(tidyverse)
# library(RODBC)   # REMOVED: unused. This script uses DBI/odbc, not RODBC.
library(config)
library(DBI)
library(openxlsx) # Excel read/write/styling - the workbook engine for step 5
library(lubridate) # today() for timestamping the output filenames

# Date strings: today_string ("YYYYMMDD") goes in the file NAME; publication_date
# ("Month DD, YYYY") is printed on the User Guide sheet inside each workbook.
today_string <- format(today(), '%Y%m%d')
publication_date <- format(today(), '%B %d, %Y')

# Draft flag. When TRUE, a red "DRAFT" banner is added to each sheet and the
# filenames are prefixed "draft_". Toggle to FALSE for the final release. This
# only changes cosmetics - the numbers are identical either way.
is_draft <- TRUE

# ---- Configure LAN and file paths ----
db_config <- config::get("decimal") # DB connection settings (never hardcoded)
lan <- config::get("lan") # network path for templates + output workbooks
my_schema <- config::get("myschema") # this analyst's IDIR schema, e.g. IDIR\JDUAN

# ---- Connection to decimal ----
# db_config <- config::get("decimal")   # REMOVED: duplicate of the line above.
decimal_con <- dbConnect(
  odbc::odbc(),
  Driver = db_config$driver,
  Server = db_config$server,
  Database = db_config$database,
  Trusted_Connection = "True" # Windows Integrated Authentication
)

# ---- Check for/ read in required data tables ----
# Read everything 08 needs from <my_schema>. tibble() wraps each read so later
# dplyr verbs behave predictably. The three tmp_tbl_* are the three model runs.

# Derived tables (the three model runs + the grad/program inputs for the grad sheet)
tmp_tbl_model <- tibble(dbReadTable(
  decimal_con,
  SQL(glue::glue('"{my_schema}"."tmp_tbl_Model"'))
))
tmp_tbl_qi <- tibble(dbReadTable(
  decimal_con,
  SQL(glue::glue('"{my_schema}"."tmp_tbl_QI"'))
))
tmp_tbl_ptib <- tibble(dbReadTable(
  decimal_con,
  SQL(glue::glue('"{my_schema}"."tmp_tbl_Model_Inc_Private_Inst"'))
))
tmp_grad_projections <- tibble(dbReadTable(
  decimal_con,
  SQL(glue::glue('"{my_schema}"."Graduate_Projections"'))
))
# tmp_cohort_dist <- tibble(dbReadTable(
#   decimal_con,
#   SQL(glue::glue('"{my_schema}"."Cohort_Program_Distributions"')) # there are two program distribution table:  which one? statics or projected, from 'load-occupaation-projections.r'. The size of this table is  50000, which match the static one.
# ))

tmp_cohort_dist <- tibble(dbReadTable(
  decimal_con,
  SQL(glue::glue('"{my_schema}"."Cohort_Program_Distributions_static_r"')) # there are two program distribution table:  which one? statics or projected, from 'load-occupaation-projections.r'
))

# Exclusion tables: programs/credentials Student Outcomes can't or shouldn't
# project (applied to the grad sheet), plus suppressed NOCs (applied to public).
exclude_lcip_cred <- tibble(dbReadTable(
  decimal_con,
  SQL(glue::glue('"{my_schema}"."T_Exclude_from_Projections_LCIP4_CRED_r"'))
))
exclude_lcp_cd <- tibble(dbReadTable(
  decimal_con,
  SQL(glue::glue('"{my_schema}"."T_Exclude_from_Projections_LCP4_CD_r"'))
))
exclude_cred <- tibble(dbReadTable(
  decimal_con,
  SQL(glue::glue(
    '"{my_schema}"."T_Exclude_from_Projections_PSSM_Credential_r"'
  ))
))
exclude_nocs <- tibble(dbReadTable(
  decimal_con,
  SQL(glue::glue('"{my_schema}"."T_Suppression_Public_Release_NOC_r"'))
)) #from 02b-3

# Look-up tables: fine age band -> rollup band, and credential code -> nice name.
age_groups <- tibble(dbReadTable(
  decimal_con,
  SQL(glue::glue('"{my_schema}"."tbl_Age_Groups_r"'))
))
age_groups_rollup <- tibble(dbReadTable(
  decimal_con,
  SQL(glue::glue('"{my_schema}"."tbl_Age_Groups_Rollup_r"'))
))
credentials <- tibble(dbReadTable(
  decimal_con,
  SQL(glue::glue('"{my_schema}"."T_PSSM_Credential_Grouping_Appendix_r"'))
))

# 1. Create Final Graduate Projections ----
#
# Graduate forecasts arrive keyed by CREDENTIAL (PSSM_CRED), but the exclusion
# lists operate at PROGRAM level (LCP4_CD / LCIP4_CRED). So we temporarily spread
# each credential's graduates back across its programs using the cohort program
# mix PERCENT  (GRADS = GRADUATES x P(CIP | cred, age), the same factor as 06),
# drop the excluded programs, then re-aggregate up to credential for the report.
# PTIB (private) is filtered out throughout - the published grad table is
# public-only. toupper() on the join keys guards against case mismatches between
# the two source tables.
filtered_grads <- tmp_grad_projections %>%
  filter(SURVEY != 'PTIB') %>%
  select(YEAR, AGE_GROUP, PSSM_CRED, GRADUATES) %>%
  mutate(PSSM_CRED = toupper(PSSM_CRED)) %>% # include to make sure joins work correctly
  inner_join(
    tmp_cohort_dist %>%
      filter(SURVEY != 'PTIB') %>%
      mutate(PSSM_CRED = toupper(PSSM_CRED)) %>%
      select(
        PSSM_CREDENTIAL,
        YEAR,
        AGE_GROUP,
        PSSM_CRED,
        LCP4_CD,
        GRAD_STATUS,
        TTRAIN,
        LCIP4_CRED,
        PERCENT
      ),
    by = c('YEAR', 'AGE_GROUP', 'PSSM_CRED')
  ) %>%
  # Drop excluded credentials, 4-digit CIPs, and CIP-credential combinations.
  filter(
    !PSSM_CREDENTIAL %in% (exclude_cred %>% pull(PSSM_CREDENTIAL)),
    !LCP4_CD %in% (exclude_lcp_cd %>% pull(LCIP_LCP4_CD)),
    !LCIP4_CRED %in% (exclude_lcip_cred %>% pull(LCIP4_CRED))
  ) %>%
  mutate(
    GRADS = GRADUATES * PERCENT # graduates falling into this CIP program
  )

# Roll the fine age bands up to the report's rollup bands, total by
# age-rollup x year x credential, then attach the human-readable credential name.
grads_agg <- filtered_grads %>%
  mutate(PSSM_CREDENTIAL = toupper(PSSM_CREDENTIAL)) %>% # include so things aggregate correctly
  inner_join(
    age_groups,
    by = c('AGE_GROUP' = 'AGE_GROUP_LABEL')
  ) %>%
  inner_join(
    age_groups_rollup,
    by = 'AGE_GROUP_ROLLUP'
  ) %>%
  group_by(
    AGE_GROUP_ROLLUP_LABEL,
    YEAR,
    PSSM_CREDENTIAL
  ) %>%
  summarize(GRADS = sum(GRADS)) %>%
  inner_join(
    credentials %>% mutate(PSSM_CREDENTIAL = toupper(PSSM_CREDENTIAL)),
    by = 'PSSM_CREDENTIAL'
  ) %>%
  select(
    AGE_GROUP_ROLLUP_LABEL,
    YEAR,
    PSSM_CREDENTIAL_NAME,
    GRADS
  ) %>%
  ungroup()

# Disclosure control: round every published count to the nearest 5.
grads_rounded <- grads_agg %>%
  mutate(GRADS = as.integer(5 * round(GRADS / 5, 0)))

# Per-age-group "Total" rows. NOTE: totals are summed from the UNROUNDED grads_agg
# and then rounded, so a Total may not exactly equal the sum of its rounded
# credential rows. This is the intended disclosure practice (round once, at the
# end), not a bug.
grad_totals <- grads_agg %>%
  group_by(AGE_GROUP_ROLLUP_LABEL, YEAR) %>%
  summarize(GRADS = sum(GRADS), PSSM_CREDENTIAL_NAME = 'Total') %>%
  mutate(GRADS = as.integer(5 * round(GRADS / 5, 0)))

# Final grad sheet: stack the Total rows on, pivot years to columns, sort so
# "Total" sits last within each age group, and apply friendly column headers.
grads <- grads_rounded %>%
  bind_rows(grad_totals) %>%
  pivot_wider(names_from = YEAR, values_from = GRADS) %>%
  mutate(is_total = PSSM_CREDENTIAL_NAME == 'Total') %>%
  arrange(AGE_GROUP_ROLLUP_LABEL, is_total, PSSM_CREDENTIAL_NAME) %>%
  rename(
    `Age Group` = AGE_GROUP_ROLLUP_LABEL,
    `Credential Type` = PSSM_CREDENTIAL_NAME
  ) %>%
  select(-is_total)

# grads   # REMOVED: interactive inspection only (bare print).

# 2. Create Final Occupation Projections ----
#
# Join the three model runs on Expr1 to attach QI (from the QI run) and CI (from
# the PTIB run), keep only the 5-digit NOC rows, then derive the two quality
# columns from the FIRST projection year.

# Year columns come back from dbReadTable prefixed with "X" (e.g. "X2023.2024"
# because column names can't start with a digit). Pick the earliest as the base
# year used for the QI/CI calculations - written generically so it isn't
# hardcoded to 2023/24.
years <- tmp_tbl_model %>%
  select(starts_with('X'), starts_with("2")) %>%
  names()
# try to do this generically without mention of first year
first_year_col <- years[order(years)][1]

tmp_occ <- tmp_tbl_model %>%
  mutate(first_year = .[first_year_col] %>% pull()) %>% # Regular base-year value
  left_join(
    tmp_tbl_qi %>%
      mutate(QI = .[first_year_col] %>% pull()) %>% # QI-run base-year value
      select(Expr1, QI),
    by = "Expr1"
  ) %>%
  left_join(
    tmp_tbl_ptib %>%
      mutate(CI = .[first_year_col] %>% pull()) %>% # PTIB-run base-year value
      select(Expr1, CI),
    by = "Expr1"
  ) %>%
  filter(NOC_Level == 5) %>% # report at the most detailed 5-digit NOC
  arrange(
    Age_Group_Rollup_Label,
    NOC,
    Current_Region_PSSM_Code_Rollup
  ) %>%
  # Coverage Indicator = public / (public+private). If the PTIB figure is missing
  # (no private-inclusive value), record coverage as 0.
  mutate(
    `Public Post-Secondary Coverage Indicator` = ifelse(
      is.na(CI),
      0,
      first_year / CI
    )
  ) %>%
  # Quality Indicator = relative gap between the Regular and QI runs.
  mutate(QI_calc = (abs(first_year - QI) / QI)) %>%
  mutate(
    `Quality Indicator` = case_when(
      QI_calc < 0.25 ~ QI_calc, # stable estimate: report the value
      # too few graduates (or missing) to be reliable: suppress to NA
      (first_year < 10 | QI < 10 | is.na(first_year) | is.na(QI)) ~ NA_integer_,
      TRUE ~ QI_calc # unstable but reportable (>= 0.25)
    )
  ) %>%
  # round outputs
  mutate(across(starts_with('X'), ~ round(.)))

# Internal release: friendly headers, convert "X2023.2024" -> "2023/24", and put
# the columns in presentation order (keys, year columns, then QI/CI).
internal_release_data <- tmp_occ %>%
  rename(
    `Age Group` = Age_Group_Rollup_Label,
    `NOC Level` = NOC_Level,
    `NOC 2021` = `NOC`,
    `Occupation Description` = ENGLISH_NAME,
    `Region ID` = Current_Region_PSSM_Code_Rollup,
    `Region Name` = Current_Region_PSSM_Name_Rollup
  ) %>%
  rename_with(~ gsub('X(\\d{4}).\\d{2}(\\d{2})', '\\1/\\2', .)) %>%
  select(
    `Age Group`,
    `NOC Level`,
    `NOC 2021`,
    `Occupation Description`,
    `Region ID`,
    `Region Name`,
    matches('^\\d'), # the year columns, now named like 2023/24
    `Quality Indicator`,
    `Public Post-Secondary Coverage Indicator`
  )

# internal_release_data   # REMOVED: interactive inspection only (bare print).

# 3. Get Public Release Version of Occupations ----
# Public release is a modified version of internal that only includes:
# QI values below a threshold (0.25)
# Filtered to BC region only
# Excludes low count NOCs
QI_threshold <- 0.25

# Build the suppression key list in the SAME "AgeGroup-NOC-5900" shape as Expr1
# so suppressed NOCs can be removed by a simple membership test below.
exclude_nocs_list <- exclude_nocs %>%
  mutate(
    exclude = paste0(
      Age_Group_Rollup_Label,
      '-',
      NOC_CD,
      '-5900'
    )
  ) %>%
  pull(exclude)

# Keep only rows fit for public release: BC total (5900), stable quality, a known
# NOC, and not on the suppression list.
occ_filtered <- tmp_occ %>%
  filter(
    Current_Region_PSSM_Code_Rollup == 5900,
    QI_calc < QI_threshold,
    NOC != '99999', # 99999 = occupation unknown (assigned in 07)
    !Expr1 %in% exclude_nocs_list
  )

# Everything dropped by those filters is summed into a single "Other" (99998) row
# per age group, so the published column totals still reconcile to the full BC
# supply (suppressed detail is hidden, not discarded).
occ_unknown_total <-
  tmp_occ %>%
  filter(
    Current_Region_PSSM_Code_Rollup == 5900,
    !Expr1 %in% (occ_filtered %>% pull(Expr1))
  ) %>%
  group_by(
    Age_Group_Rollup_Label
  ) %>%
  summarize(
    NOC = '99998',
    ENGLISH_NAME = 'Other',
    across(starts_with('X'), ~ sum(.)) # already rounded
  )

# Final public sheet: published NOCs + the "Other" row, friendly headers, years
# renamed to "2023/24" form, sorted by age group then NOC.
public_release_data <- occ_filtered %>%
  select(Age_Group_Rollup_Label, NOC, ENGLISH_NAME, starts_with('X')) %>%
  bind_rows(
    occ_unknown_total %>%
      select(Age_Group_Rollup_Label, NOC, ENGLISH_NAME, starts_with('X'))
  ) %>%
  arrange(
    Age_Group_Rollup_Label,
    NOC
  ) %>%
  rename(
    `Age Group` = Age_Group_Rollup_Label,
    `NOC 2021` = `NOC`,
    `Occupation Description` = ENGLISH_NAME
  ) %>%
  rename_with(~ gsub('X(\\d{4}).\\d{2}(\\d{2})', '\\1/\\2', .)) %>%
  select(
    `Age Group`,
    `NOC 2021`,
    `Occupation Description`,
    matches('^\\d')
  )

# public_release_data   # REMOVED: interactive inspection only (bare print).

# 4. Excel Workbook Settings ----

# openxlsx cell styles reused by create_final_excel() below:
csDraft <- createStyle(
  # big red "DRAFT" banner
  fontSize = 20,
  fontColour = "#FF0000",
  textDecoration = "bold",
  wrapText = TRUE
)
csRegularBold <- createStyle(
  # header row (bold, centred, bordered)
  valign = "center",
  halign = 'center',
  wrapText = TRUE,
  textDecoration = "bold",
  border = "TopBottomLeftRight",
  borderColour = "#C0C0C0",
  borderStyle = "thin"
)
# csCount <- createStyle(halign = "right")   # REMOVED: defined but never used.
csPerc <- createStyle(
  # QI/CI percentage cells
  halign = "right",
  numFmt = "0.0%",
  border = "TopBottomLeftRight",
  borderColour = "#C0C0C0",
  borderStyle = "thin"
) ## Percent cells
csPub <- createStyle(textDecoration = "italic") # "Prepared by BC Stats" line
csBorder <- createStyle(
  # thin grid border for data cells
  border = "TopBottomLeftRight",
  borderColour = "#C0C0C0",
  borderStyle = "thin"
)

# create_final_excel(): builds ONE workbook from a template and saves it.
# Writes two sheets - "Graduate Projections" and "Occupation Projections" - with
# headers, borders, frozen top row, and landscape print setup. The is_internal
# flag adds percentage styling for the QI/CI columns (public sheets lack them).
# inputs:
#   - template: path to template user guide excel file
#   - final_excel: path to final save file for excel
#   - grad_data: copy of the graduation projections
#   - occ_data: copy of the occupation projections
#   - is_draft: whether to include 'draft' at the top of each sheet (and in sheet name)
#   - is_internal: whether this is the full internal dataset or not (has extra columns to format)
# outputs:
#   - none. Saves a copy of the data to the LAN at the specified location
create_final_excel <- function(
  template,
  final_excel,
  grad_data,
  occ_data,
  is_draft = TRUE,
  is_internal = TRUE
) {
  # load template
  outwb <- loadWorkbook(template)

  # if draft, add 'DRAFT' to first page
  if (is_draft) {
    writeData(
      outwb,
      "User Guide",
      x = 'DRAFT\nBC Post-Secondary Supply Model—2023/24 to 2034/35',
      startRow = 1,
      startCol = 1
    )
    addStyle(
      outwb,
      "User Guide",
      style = csDraft,
      rows = 1,
      cols = 1,
      gridExpand = TRUE
    )
    setRowHeights(outwb, "User Guide", rows = 1, heights = 60)
  }

  # update publication line
  writeData(
    outwb,
    "User Guide",
    x = paste0('Prepared by BC Stats, ', publication_date),
    startRow = 2,
    startCol = 1
  )
  addStyle(outwb, "User Guide", style = csPub, rows = 2, cols = 1)

  ## add new sheet for grads
  sheet <- addWorksheet(outwb, sheetName = "Graduate Projections")
  n_rows <- nrow(grad_data)
  n_cols <- length(grad_data)

  # add data to sheet
  startRow <- 1
  if (is_draft) {
    writeData(outwb, sheet, x = 'DRAFT', startRow = 1, startCol = 1)
    addStyle(outwb, sheet, style = csDraft, rows = 1, cols = 1)
    startRow <- 2
  }

  writeData(
    outwb,
    sheet,
    grad_data,
    colNames = TRUE,
    rowNames = FALSE,
    startRow = startRow,
    startCol = 1,
    withFilter = FALSE,
    keepNA = FALSE
  )

  # Freeze top row
  freezePane(outwb, sheet, firstActiveRow = startRow + 1)

  # add borders
  addStyle(
    outwb,
    sheet,
    csBorder,
    rows = startRow:(n_rows + startRow),
    cols = 1:n_cols,
    gridExpand = TRUE
  )

  # style headers
  addStyle(
    outwb,
    sheet,
    style = csRegularBold,
    rows = startRow,
    cols = 1:n_cols
  )

  # set col widths
  cred_col <- which(names(grad_data) == "Credential Type")
  setColWidths(outwb, sheet, cols = cred_col, widths = "auto")

  ## add new sheet for occupations
  sheet <- addWorksheet(outwb, sheetName = "Occupation Projections")
  n_rows <- nrow(occ_data)
  n_cols <- length(occ_data)

  # add data to sheet
  startRow <- 1
  if (is_draft) {
    writeData(outwb, sheet, x = 'DRAFT', startRow = 1, startCol = 1)
    addStyle(outwb, sheet, style = csDraft, rows = 1, cols = 1)
    startRow <- 2
  }

  writeData(
    outwb,
    sheet,
    occ_data,
    colNames = TRUE,
    rowNames = FALSE,
    startRow = startRow,
    startCol = 1,
    withFilter = TRUE,
    keepNA = FALSE
  )

  # Freeze top row
  freezePane(outwb, sheet, firstActiveRow = startRow + 1)

  # add borders
  addStyle(
    outwb,
    sheet,
    csBorder,
    rows = startRow:(n_rows + startRow),
    cols = 1:n_cols,
    gridExpand = TRUE
  )

  # style headers
  addStyle(
    outwb,
    sheet,
    style = csRegularBold,
    rows = startRow,
    cols = 1:n_cols
  )
  setRowHeights(outwb, sheet, rows = startRow, heights = 60)

  # set col widths
  occ_col <- which(names(occ_data) == "Occupation Description")
  setColWidths(outwb, sheet, cols = occ_col, widths = 40)

  # extra internal release columns
  if (is_internal) {
    rg_col <- which(names(occ_data) == "Region Name")
    setColWidths(outwb, sheet, cols = rg_col, widths = "auto")
    # style the percentages
    qi_col <- which(names(occ_data) == "Quality Indicator")
    ci_col <- which(
      names(occ_data) == "Public Post-Secondary Coverage Indicator"
    )
    addStyle(
      outwb,
      sheet,
      style = csPerc,
      rows = (startRow + 1):(n_rows + startRow),
      cols = qi_col
    )
    addStyle(
      outwb,
      sheet,
      style = csPerc,
      rows = (startRow + 1):(n_rows + startRow),
      cols = ci_col
    )
  }

  # delete excess rows? not sure why happening
  #deleteData(outwb, sheet, cols=1:n_cols, rows=)

  # prepare print settings
  sheet_names <- sheets(outwb)
  walk(
    sheet_names,
    ~ setHeaderFooter(
      outwb,
      .x,
      footer = c(
        "BC Post-Secondary Supply Model 2023/24 to 2034/35",
        NA,
        "Page &[Page] of &[Pages]"
      )
    )
  )

  walk(
    sheet_names,
    ~ pageSetup(
      outwb,
      .x,
      orientation = "landscape",
      fitToWidth = TRUE,
      printTitleRows = startRow
    )
  )

  # save output
  saveWorkbook(outwb, final_excel, overwrite = TRUE)
}

# 5. Final Excel - Internal/Public Use ----

##
# WARNING!!
# (MAY REQUIRE MANUAL UPDATES TO THE NOTES STILL!)
##

# Filename prefix: drafts are written as "draft_..." so they are never mistaken
# for a final release.
if (is_draft) {
  start_file <- "draft_"
} else {
  start_file <- ""
}

# Internal workbook: full detail incl. QI/CI (is_internal = TRUE).
template <- glue::glue('{lan}\\development\\work\\internal_use_template.xlsx')
final_excel <- glue::glue(
  '{lan}\\development\\work\\adhoc-outputs\\{start_file}internal_use_PSSM_2023-24_to_2034-35_{today_string}.xlsx'
)

create_final_excel(
  template = template,
  final_excel = final_excel,
  grad_data = grads,
  occ_data = internal_release_data,
  is_draft = is_draft,
  is_internal = TRUE
)

# Public workbook: BC-only, suppressed/low-count NOCs collapsed (is_internal = FALSE).
template <- glue::glue('{lan}\\development\\work\\public_use_template.xlsx')
final_excel <- glue::glue(
  '{lan}\\development\\work\\adhoc-outputs\\{start_file}public_use_PSSM_2023-24_to_2034-35_{today_string}.xlsx'
)

create_final_excel(
  template = template,
  final_excel = final_excel,
  grad_data = grads,
  occ_data = public_release_data,
  is_draft = is_draft,
  is_internal = FALSE
)

# ---- Disconnect ----
# This script owns its connection (run directly), so close it and release memory.
dbDisconnect(decimal_con)
gc()
