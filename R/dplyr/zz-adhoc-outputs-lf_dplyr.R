# Ad-hoc Outputs (Labour Force) — dplyr Translation
# Original: R/zz-adhoc-outputs-lf.R (~155 lines)
#
# Pipeline context:
#   Ad-hoc script that fixes Expr1 composite keys in model output tables and
#   produces an internal summary table with Quality Indicator and Coverage Indicator.
#   This is a diagnostic/output script, not part of the main pipeline.
#
# Key translations:
#   - SELECT * FROM (3 reads) → sch_tbl() %>% collect()
#   - Complex analytical SQL with LEFT JOIN, Ceiling, IIf → dplyr left_join + mutate
#   - Quality Indicator: IIf nested logic → case_when
#   - Coverage Indicator: IIf(IsNull...) → if_else / case_when
#
# Input:
#   - tmp_tbl_model — model output (DB)
#   - tmp_tbl_qi — QI model output (DB)
#   - tmp_tbl_Model_Inc_Private_Inst — PTIB model output (DB)
#
# Output:
#   - Fixed tmp_tbl_model, tmp_tbl_qi, tmp_tbl_Model_Inc_Private_Inst (written to DB)
#   - Internal CSV summary with Quality/Coverage indicators

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

# Helper: write to schema
write_schema_table <- function(name, data) {
  dbWriteTable(decimal_con, SQL(glue::glue('"{my_schema}"."{name}"')), data, overwrite = TRUE)
}


# ******************************************************************************
# Fix Expr1 composite keys in model output tables
# WHY: The Expr1 column is a composite key (Age_Group-NOC-Region). In a previous
# iteration, some 5-digit NOC records had incorrectly computed Expr1 values.
# This script regenerates Expr1 for all NOC_Level == 5 rows.
# ******************************************************************************

# ---- Fix tmp_tbl_model ----
tmp_model <- sch_tbl("tmp_tbl_model") %>% collect() |> rename_with(toupper)

tmp_model_fixed <- tmp_model %>%
  mutate(
    EXPR1 = case_when(
      NOC_LEVEL == 5 ~ paste0(AGE_GROUP_ROLLUP_LABEL, "-", NOC, "-", CURRENT_REGION_PSSM_CODE_ROLLUP),
      TRUE ~ EXPR1
    )
  )

# Diagnostic checks
tmp_model %>% count(EXPR1) %>% arrange(desc(n))
tmp_model %>% count(EXPR1) %>% count()
tmp_model %>% count()

tmp_model_fixed %>% count(EXPR1) %>% arrange(desc(n))
tmp_model_fixed %>% count(EXPR1) %>% count()

write_schema_table("tmp_tbl_model", tmp_model_fixed)


# ---- Fix tmp_tbl_qi ----
tmp_qi <- sch_tbl("tmp_tbl_qi") %>% collect() |> rename_with(toupper)

tmp_qi_fixed <- tmp_qi %>%
  mutate(
    EXPR1 = case_when(
      NOC_LEVEL == 5 ~ paste0(AGE_GROUP_ROLLUP_LABEL, "-", NOC, "-", CURRENT_REGION_PSSM_CODE_ROLLUP),
      TRUE ~ EXPR1
    )
  )

# Diagnostic checks
tmp_qi %>% count(EXPR1) %>% arrange(desc(n))
tmp_qi %>% count(EXPR1) %>% count()
tmp_qi %>% count()

tmp_qi_fixed %>% count(EXPR1) %>% arrange(desc(n))
tmp_qi_fixed %>% count(EXPR1) %>% count()

write_schema_table("tmp_tbl_qi", tmp_qi_fixed)


# ---- Fix tmp_tbl_Model_Inc_Private_Inst ----
tmp_tbl_Model_Inc_Private_Inst <- sch_tbl("tmp_tbl_Model_Inc_Private_Inst") %>%
  collect() |> rename_with(toupper)

tmp_tbl_Model_Inc_Private_Inst_fixed <- tmp_tbl_Model_Inc_Private_Inst %>%
  mutate(
    EXPR1 = case_when(
      NOC_LEVEL == 5 ~ paste0(AGE_GROUP_ROLLUP_LABEL, "-", NOC, "-", CURRENT_REGION_PSSM_CODE_ROLLUP),
      TRUE ~ EXPR1
    )
  )

# Diagnostic checks
tmp_tbl_Model_Inc_Private_Inst %>% count(EXPR1) %>% arrange(desc(n))
tmp_tbl_Model_Inc_Private_Inst %>% count(EXPR1) %>% count()
tmp_tbl_Model_Inc_Private_Inst %>% count()

tmp_tbl_Model_Inc_Private_Inst_fixed %>% count(EXPR1) %>% arrange(desc(n))
tmp_tbl_Model_Inc_Private_Inst_fixed %>% count(EXPR1) %>% count()

write_schema_table("tmp_tbl_Model_Inc_Private_Inst", tmp_tbl_Model_Inc_Private_Inst_fixed)


# ******************************************************************************
# Create internal summary table with Quality/Coverage indicators
# WHY: The original used a complex SQL query with LEFT JOINs, Ceiling() rounding,
# and nested IIf() logic for Quality Indicator and Coverage Indicator.
# We translate this to dplyr joins + mutate.
#
# Quality Indicator: measures how much the model projection differs from the QI baseline.
#   - If the difference is < 25%, report the actual ratio.
#   - If either value is < 10 or NULL, flag as '999999999' (unreliable).
#   - Otherwise, report the raw ratio.
#
# Coverage Indicator: ratio of model (without PTIB) to model (with PTIB).
#   - If PTIB value is 0 or NULL, report 0.
#   - Otherwise, model / PTIB model.
# ******************************************************************************

# Identify year columns (matching pattern like "2023/2024")
year_cols <- names(tmp_model_fixed)[grepl("^\\d{4}/\\d{4}$", names(tmp_model_fixed))]

# Join model with QI and PTIB model on Expr1
internal <- tmp_model_fixed %>%
  filter(NOC_LEVEL == 5) %>%
  # Left join with QI — suffix to distinguish QI year columns
  left_join(
    tmp_qi_fixed %>% select(EXPR1, all_of(paste0("`", year_cols, "`"))),
    by = "EXPR1",
    suffix = c("", ".QI")
  ) %>%
  # Left join with PTIB model — suffix to distinguish PTIB year columns
  left_join(
    tmp_tbl_Model_Inc_Private_Inst_fixed %>% select(EXPR1, all_of(paste0("`", year_cols, "`"))),
    by = "EXPR1",
    suffix = c("", ".PTIB")
  )

# Apply Ceiling to all year columns from the model
# WHY: The original used Ceiling() to round up all year projections.
internal <- internal %>%
  mutate(across(all_of(year_cols), ~ceiling(.)))

# Compute Quality Indicator and Coverage Indicator for the base year
# WHY: The original used nested IIf() logic. We translate to case_when() for clarity.
base_year <- "2023/2024"
qi_col <- paste0(base_year, ".QI")
ptib_col <- paste0(base_year, ".PTIB")

# Only compute if the QI and PTIB columns exist after the join
if (qi_col %in% names(internal) && ptib_col %in% names(internal)) {
  internal <- internal %>%
    mutate(
      # Quality Indicator: how much does model differ from QI?
      `Quality Indicator` = case_when(
        # Difference < 25%: report the actual ratio
        abs(!!sym(base_year) - !!sym(qi_col)) / !!sym(qi_col) < 0.25
          ~ abs(!!sym(base_year) - !!sym(qi_col)) / !!sym(qi_col),
        # Small values or NULL: flag as unreliable
        !!sym(base_year) < 10 | !!sym(qi_col) < 10 |
          is.na(!!sym(base_year)) | is.na(!!sym(qi_col))
          ~ 999999999,
        # Default: report the raw ratio
        TRUE
          ~ abs(!!sym(base_year) - !!sym(qi_col)) / !!sym(qi_col)
      ),
      # Coverage Indicator: model / model_with_PTIB
      `Coverage Indicator` = case_when(
        is.na(!!sym(ptib_col)) | !!sym(ptib_col) == 0 ~ 0,
        TRUE ~ !!sym(base_year) / !!sym(ptib_col)
      )
    )
}

# Select and order columns matching original output
internal <- internal %>%
  select(EXPR1, AGE_GROUP_ROLLUP_LABEL, NOC_LEVEL, NOC, ENGLISH_NAME,
         CURRENT_REGION_PSSM_CODE_ROLLUP, CURRENT_REGION_PSSM_NAME_ROLLUP,
         all_of(year_cols),
         `Quality Indicator`, `Coverage Indicator`) %>%
  arrange(AGE_GROUP_ROLLUP_LABEL, NOC)

# Write to CSV
internal %>% write_csv(glue::glue("{lan}/development/work/adhoc-outputs/internal-occs-20240926.csv"))


# ---- Clean Up ----
dbDisconnect(decimal_con)
