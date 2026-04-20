# Graduate Historical + Forecasted Table — dplyr Translation
# Original: R/zz-graduate-historical-forecasted.R
#
# Pipeline context:
#   Ad-hoc reporting script that combines historical graduate counts with projected
#   graduate counts in a single table for presentation and comparison purposes. This
#   lets analysts see both past actuals and future forecasts side-by-side.
#
#   This script sits downstream of the main pipeline — it reads from tables produced
#   by scripts 04 (Graduate_Projections) and 06 (Cohort_Program_Distributions).
#   It is NOT part of the main model run (not sourced by the orchestrator scripts).
#
# Input tables:
#   - Graduate_Projections — from 04-graduate-projections
#   - Cohort_Program_Distributions — from 06-program-projections
#   - Graduate_Projections_Include_Historical — from 04-graduate-projections
#   - T_Exclude_from_Projections_* — exclusion lists for specific CIP/credential types
#   - tbl_Age_Groups / tbl_Age_Groups_Rollup — age group lookup tables
#   - T_PSSM_Credential_Grouping_Appendix — credential grouping for presentation
#
# Output:
#   - qry99_Presentations_Graduates_Appendix — pivoted summary (console output)
#   - CSV file written to LAN for ad-hoc analysis

library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(dbplyr)
library(ggplot2)

# ---- Helper function for referencing schema-qualified tables ----
sch_tbl <- function(name) { tbl(con, dbplyr::in_schema(my_schema, name)) }

# ---- Configure LAN and file paths ----
db_config <- config::get("decimal")
lan <- config::get("lan")
my_schema <- config::get("myschema")

# ---- Connection to decimal ----
db_config <- config::get("decimal")
con <- dbConnect(odbc::odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")

# ---- Check for required data tables ----
# Derived tables (created by earlier pipeline scripts)
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."Cohort_Program_Distributions"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."Graduate_Projections"')))

# Lookups
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."T_Exclude_from_Projections_LCP4_CD"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."T_Exclude_from_Projections_LCIP4_CRED"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."T_Exclude_from_Projections_PSSM_Credential"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."tbl_Age_Groups"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."tbl_Age_Groups_Rollup"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."T_PSSM_Credential_Grouping_Appendix"')))


# We need to apply exclusion lists to the graduate projections before presenting
# them. Some CIP codes and credential types are excluded from the final reports.
# ---- Q_1 Series: Projected graduates by program ----

# Pull reference tables from DB
exclude_lcp4 <- sch_tbl("T_Exclude_from_Projections_LCP4_CD") %>% collect() |> rename_with(toupper)
exclude_pssm_cred <- sch_tbl("T_Exclude_from_Projections_PSSM_Credential") %>% collect() |> rename_with(toupper)
exclude_lcip4_cred <- sch_tbl("T_Exclude_from_Projections_LCIP4_CRED") %>% collect() |> rename_with(toupper)
grad_proj <- sch_tbl("Graduate_Projections") %>% collect() |> rename_with(toupper)
cohort_prog_dist <- sch_tbl("Cohort_Program_Distributions") %>% collect() |> rename_with(toupper)

# Join projections with program distributions, then exclude specific CIPs/credentials
# WHY: Program distributions split projected graduates across CIP programs. The exclusion
# lists remove CIP codes and credential types that shouldn't appear in reports.
Q_1_Grad_Projections_by_Age_by_Program <- grad_proj %>%
  inner_join(cohort_prog_dist,
    by = c("YEAR" = "YEAR", "AGE_GROUP" = "AGE_GROUP", "PSSM_CRED" = "PSSM_CRED")) %>%
  anti_join(exclude_lcp4,
    by = c("LCP4_CD" = "LCIP_LCP4_CD")) %>%
  anti_join(exclude_pssm_cred,
    by = c("PSSM_CREDENTIAL" = "PSSM_CREDENTIAL")) %>%
  anti_join(exclude_lcip4_cred,
    by = c("LCIP4_CRED" = "LCIP4_CRED")) %>%
  mutate(GRADS = GRADUATES * PERCENT) %>%
  select(PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP, YEAR,
         LCP4_CD, GRAD_STATUS, TTRAIN, LCIP4_CRED, GRADS)

dbWriteTable(con, SQL(glue::glue('"{my_schema}"."Q_1_Grad_Projections_by_Age_by_Program"')),
             Q_1_Grad_Projections_by_Age_by_Program, overwrite = TRUE)


# Roll up age groups for program-level projections
# WHY: Reports show graduates at a coarser age grouping than the detailed model uses.
# The tbl_Age_Groups and tbl_Age_Groups_Rollup tables define the mapping.
age_groups <- sch_tbl("tbl_Age_Groups") %>% collect() |> rename_with(toupper)
age_groups_rollup <- sch_tbl("tbl_Age_Groups_Rollup") %>% collect() |> rename_with(toupper)

Q_1c_Grad_Projections_by_Program <- Q_1_Grad_Projections_by_Age_by_Program %>%
  inner_join(age_groups, by = c("AGE_GROUP" = "AGE_GROUP_LABEL")) %>%
  inner_join(age_groups_rollup, by = c("AGE_GROUP_ROLLUP" = "AGE_GROUP_ROLLUP")) %>%
  group_by(PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP_ROLLUP, AGE_GROUP_ROLLUP_LABEL,
           YEAR, GRAD_STATUS, TTRAIN, LCP4_CD, LCIP4_CRED) %>%
  summarise(GRADS = sum(GRADS), .groups = "drop") %>%
  select(PSSM_CREDENTIAL, PSSM_CRED, AGE_GROUP_ROLLUP, AGE_GROUP_ROLLUP_LABEL,
         YEAR, GRAD_STATUS, TTRAIN, LCP4_CD, LCIP4_CRED, GRADS)

dbWriteTable(con, SQL(glue::glue('"{my_schema}"."Q_1c_Grad_Projections_by_Program"')),
             Q_1c_Grad_Projections_by_Program, overwrite = TRUE)


# The PIVOT query creates a human-readable cross-tabulation with years as columns
# and credential types as rows — the format needed for the appendix report.
# KEPT AS SQL: SQL Server PIVOT has no dplyr equivalent
# ---- Final Presentation Table ----
qry99_Presentations_Graduates_Appendix <-
  "SELECT Age_Group_Rollup_Label, PSSM_Credential_Name,
[2023/2024],
[2024/2025],
[2025/2026],
[2026/2027],
[2027/2028],
[2028/2029],
[2029/2030],
[2030/2031],
[2031/2032],
[2032/2033],
[2033/2034],
[2034/2035]
FROM (
SELECT Q_1c_Grad_Projections_by_Program.Age_Group_Rollup_Label,
Q_1c_Grad_Projections_by_Program.Year as yr,
T_PSSM_Credential_Grouping_Appendix.PSSM_Credential_Name,
Grads
FROM T_PSSM_Credential_Grouping_Appendix
INNER JOIN Q_1c_Grad_Projections_by_Program
	ON T_PSSM_Credential_Grouping_Appendix.PSSM_Credential = Q_1c_Grad_Projections_by_Program.PSSM_Credential
WHERE (((Q_1c_Grad_Projections_by_Program.PSSM_CRED) Not Like 'P - %'))
) AS SourceTable
PIVOT (
    Sum([Grads]) FOR Yr IN ([2023/2024],
[2024/2025],
[2025/2026],
[2026/2027],
[2027/2028],
[2028/2029],
[2029/2030],
[2030/2031],
[2031/2032],
[2032/2033],
[2033/2034],
[2034/2035])
) AS PivotTable;"
dbGetQuery(con, qry99_Presentations_Graduates_Appendix) %>%
  mutate(across(where(is.numeric), round))

# KEPT AS SQL: DROP TABLE (DDL)
dbExecute(con, "DROP TABLE Q_1_Grad_Projections_by_Age_by_Program")
dbExecute(con, "DROP TABLE Q_1c_Grad_Projections_by_Program")


# To compare historical actuals with projections, we pull data from the
# Graduate_Projections_Include_Historical table (which has both) and the
# Graduate_Projections table (which has projections only), then verify they match
# for the base year (2023/2024).
# ---- Historical + Projected Graduates for Comparison ----

# Pull historical + projected graduates with credential groupings for display
grads <- sch_tbl("Graduate_Projections_Include_Historical") %>%
  select(age_group, PSSM_CREDENTIAL, year, graduates) %>%
  collect() |> rename_with(toupper) %>%
  left_join(
    sch_tbl("T_PSSM_Credential_Grouping_Appendix") %>% collect() |> rename_with(toupper),
    by = c("PSSM_CREDENTIAL" = "PSSM_CREDENTIAL")
  ) %>%
  left_join(
    sch_tbl("tbl_Age_Groups") %>% collect() |> rename_with(toupper) %>%
      select(AGE_GROUP_LABEL, AGE_GROUP_ROLLUP),
    by = c("AGE_GROUP" = "AGE_GROUP_LABEL")
  ) %>%
  left_join(
    sch_tbl("tbl_Age_Groups_Rollup") %>% collect() |> rename_with(toupper),
    by = c("AGE_GROUP_ROLLUP" = "AGE_GROUP_ROLLUP")
  ) %>%
  select(AGE_GROUP, AGE_GROUP_ROLLUP_LABEL, PSSM_CREDENTIAL_NAME, YEAR, GRADUATES)

# Pull projected graduates only (for comparison with historical base year)
grads_proj <- sch_tbl("Graduate_Projections") %>%
  select(age_group, PSSM_CREDENTIAL, year, graduates) %>%
  collect() |> rename_with(toupper) %>%
  left_join(
    sch_tbl("T_PSSM_Credential_Grouping_Appendix") %>% collect() |> rename_with(toupper),
    by = c("PSSM_CREDENTIAL" = "PSSM_CREDENTIAL")
  ) %>%
  left_join(
    sch_tbl("tbl_Age_Groups") %>% collect() |> rename_with(toupper) %>%
      select(AGE_GROUP_LABEL, AGE_GROUP_ROLLUP),
    by = c("AGE_GROUP" = "AGE_GROUP_LABEL")
  ) %>%
  left_join(
    sch_tbl("tbl_Age_Groups_Rollup") %>% collect() |> rename_with(toupper),
    by = c("AGE_GROUP_ROLLUP" = "AGE_GROUP_ROLLUP")
  ) %>%
  select(AGE_GROUP, AGE_GROUP_ROLLUP_LABEL, PSSM_CREDENTIAL_NAME, YEAR, GRADUATES)

# Verify projections match historical data for the base year (2023/2024)
grads %>%
  filter(YEAR>='2023/2024', !grepl('Apprenticeship', PSSM_CREDENTIAL_NAME)) %>%
  all.equal(grads_proj %>% filter(!grepl('Apprenticeship', PSSM_CREDENTIAL_NAME)))

grads %>% filter(YEAR == '2023/2024')
grads_proj %>% filter(YEAR == '2023/2024')

# Fill in missing years for a complete time series
# WHY: Some credential/age combinations may have gaps in the historical data.
# fill() carries the last known value forward to create a continuous series.
grads_completed <-  grads %>%
  arrange(PSSM_CREDENTIAL_NAME, AGE_GROUP, YEAR) %>%
  complete(PSSM_CREDENTIAL_NAME, AGE_GROUP, YEAR) %>%
  group_by(PSSM_CREDENTIAL_NAME, AGE_GROUP) %>%
  fill(GRADUATES, AGE_GROUP_ROLLUP_LABEL)

grads_completed %>% View()

grads_completed %>% filter(PSSM_CREDENTIAL_NAME == 'Apprenticeship') %>%
  filter(AGE_GROUP_ROLLUP_LABEL == '17 to 29') %>%
  filter(YEAR>='2023/2024')

# Create a wide-format summary table for CSV export
# WHY: The final output is a CSV file with years as columns, credential types and
# age groups as rows — the format analysts need for charting and comparison.
grads_by_age_cred <- grads_completed %>%
  filter(YEAR>='2018/2019') %>%
  group_by(AGE_GROUP_ROLLUP_LABEL, PSSM_CREDENTIAL_NAME, YEAR) %>%
  summarise(n = round(sum(GRADUATES, drop.na=TRUE), 0)) %>%
  pivot_wider(id_cols = c('AGE_GROUP_ROLLUP_LABEL', 'PSSM_CREDENTIAL_NAME'), names_from = 'YEAR', values_from = 'n') %>%
  arrange(PSSM_CREDENTIAL_NAME, AGE_GROUP_ROLLUP_LABEL) %>%
  filter(!is.na(AGE_GROUP_ROLLUP_LABEL))


# Diagnostic plot: graduates over time by credential type
grads_completed %>%
  mutate(YEAR = as.numeric(str_sub(YEAR, 1,4))) %>%
  group_by(PSSM_CREDENTIAL_NAME, YEAR) %>%
  summarize(n = sum(GRADUATES)) %>% # View()
  ggplot(aes(x = YEAR, y=n, color=PSSM_CREDENTIAL_NAME)) +
  geom_line()+
  geom_vline(aes(xintercept = 2023))

# Write summary CSV to LAN for ad-hoc analysis
grads_by_age_cred %>% write_csv(
  glue::glue('{lan}\\development\\work\\adhoc-outputs\\graduate_projections_include_historical_no_ptib.csv')
)
