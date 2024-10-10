# FINAL REPORT TABLES 
#
# This script creates the final excel spreadsheet for the internal release
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
# Finally, it expects that you have a list of exclusionary tables to exclude programs 
#  where Student Outcomes results not available or inappropriate:
#   - T_Exclude_from_Projections_LCIP4_CRED
#	  - T_Exclude_from_Projections_LCP4_CD
#	  - T_Exclude_from_Projections_PSSM_Credential


library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(openxlsx)
library(lubridate)

# ---- Configure LAN and file paths ----
db_config <- config::get("decimal")
lan <- config::get("lan")
my_schema <- config::get("myschema")
my_schema <- 'IDIR\\ALOWERY'

# ---- Connection to decimal ----
db_config <- config::get("decimal")
decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")

# ---- Check for/ read in required data tables ----
# Derived tables
tmp_tbl_model <- tibble(dbReadTable(decimal_con, SQL(glue::glue('"{my_schema}"."tmp_tbl_Model"'))))
tmp_tbl_qi <- tibble(dbReadTable(decimal_con, SQL(glue::glue('"{my_schema}"."tmp_tbl_QI"'))))
tmp_tbl_ptib <- tibble(dbReadTable(decimal_con, SQL(glue::glue('"{my_schema}"."tmp_tbl_Model_Inc_Private_Inst"'))))
tmp_grad_projections <- tibble(dbReadTable(decimal_con, SQL(glue::glue('"{my_schema}"."Graduate_Projections"'))))
tmp_cohort_dist <- tibble(dbReadTable(decimal_con, SQL(glue::glue('"{my_schema}"."Cohort_Program_Distributions"'))))

# Exclusion Tables
exclude_lcip_cred <- tibble(dbReadTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_Exclude_from_Projections_LCIP4_CRED"'))))
exclude_lcp_cd <- tibble(dbReadTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_Exclude_from_Projections_LCP4_CD"'))))
exclude_cred <- tibble(dbReadTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_Exclude_from_Projections_PSSM_Credential"'))))

# Look up tables
age_groups <- tibble(dbReadTable(decimal_con, SQL(glue::glue('"{my_schema}"."tbl_Age_Groups"'))))
age_groups_rollup <- tibble(dbReadTable(decimal_con, SQL(glue::glue('"{my_schema}"."tbl_Age_Groups_Rollup"'))))
credentials <- tibble(dbReadTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_PSSM_Credential_Grouping_Appendix"'))))

# 1. Create Final Graduate Projections ----

# back calculate grad projections from cohort distribution, excluding specific cips
# note that for grad projections, we do not want to include PTIB 
filtered_grads <- tmp_grad_projections %>% 
  filter(SURVEY != 'PTIB') %>% 
  select(YEAR, AGE_GROUP, PSSM_CRED, GRADUATES) %>% 
  mutate(PSSM_CRED = toupper(PSSM_CRED)) %>% # include to make sure joins work correctly 
  inner_join(
    tmp_cohort_dist %>% 
      filter(SURVEY != 'PTIB') %>% 
      mutate(PSSM_CRED = toupper(PSSM_CRED)) %>% 
      select(PSSM_CREDENTIAL, YEAR, AGE_GROUP, PSSM_CRED, LCP4_CD, GRAD_STATUS, TTRAIN, LCIP4_CRED, PERCENT),
    by=c('YEAR', 'AGE_GROUP', 'PSSM_CRED')
  ) %>% 
  filter(
    !PSSM_CREDENTIAL %in% (exclude_cred %>% pull(PSSM_CREDENTIAL)),
    !LCP4_CD %in% (exclude_lcp_cd %>% pull(LCIP_LCP4_CD)),
    !LCIP4_CRED %in% (exclude_lcip_cred %>% pull(LCIP4_CRED))
  ) %>% 
  mutate(
    GRADS = GRADUATES*PERCENT
  )

# calculate totals by year and age group 
grads_agg <- filtered_grads %>% 
  mutate(PSSM_CREDENTIAL = toupper(PSSM_CREDENTIAL)) %>% # include so things aggregate correctly 
  inner_join(
    age_groups ,
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
  inner_join(credentials %>% mutate(PSSM_CREDENTIAL = toupper(PSSM_CREDENTIAL)), by='PSSM_CREDENTIAL') %>% 
  select(
    AGE_GROUP_ROLLUP_LABEL,
    YEAR,
    PSSM_CREDENTIAL_NAME,
    GRADS
  ) %>%
  ungroup()

# round each value to nearest 5
grads_rounded <- grads_agg %>% 
  mutate(GRADS = as.integer(5*round(GRADS/5, 0)))

# get totals per age_group and join back in 
grad_totals <- grads_rounded %>% 
  group_by(AGE_GROUP_ROLLUP_LABEL, YEAR) %>% 
  summarize(GRADS = sum(GRADS), PSSM_CREDENTIAL_NAME = 'Total')
grad_totals <- grads_agg %>% 
  group_by(AGE_GROUP_ROLLUP_LABEL, YEAR) %>% 
  summarize(GRADS = sum(GRADS), PSSM_CREDENTIAL_NAME = 'Total')  %>% 
  mutate(GRADS = as.integer(5*round(GRADS/5, 0)))
# pivot years to columns, sort, and make nice names
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

# 2. Create Final Occupation Projections ---- 

# join all 3 models together and create 
# QI (quality indicator) and 
# CI (coverage indicator) columns
internal_release_data <- tmp_tbl_model %>% 
  left_join(
    tmp_tbl_qi %>% select(Expr1,QI=X2023.2024),
    by="Expr1"
    ) %>% 
  left_join(
    tmp_tbl_ptib %>% select(Expr1,CI=X2023.2024),
    by="Expr1"
    ) %>% 
  filter(NOC_Level==5) %>% 
  arrange(
    Age_Group_Rollup_Label,
    NOC,
    Current_Region_PSSM_Code_Rollup
    ) %>% 
  mutate(`Public Post-Secondary Coverage Indicator`=ifelse(is.na(CI),0,X2023.2024/CI)) %>% 
  mutate(QI_calc = (abs(X2023.2024-QI)/QI)) %>% 
  mutate(`Quality Indicator`= case_when(QI_calc < 0.25 ~ QI_calc,
                                        (X2023.2024 < 10 | QI < 10 | is.na(X2023.2024) | is.na(QI)) ~ NA_integer_,
                                        TRUE ~ QI_calc)) %>% 
  # round values to ceiling 
  mutate(across(starts_with('X'), ~ceiling(.))) %>% 
  # get nice names for things 
  rename(
    `Age Group` = Age_Group_Rollup_Label,
    `NOC Level` = NOC_Level,
    `NOC 2021`= `NOC`,
    `Occupation Description` = ENGLISH_NAME,
    `Region ID` = Current_Region_PSSM_Code_Rollup,
    `Region Name` = Current_Region_PSSM_Name_Rollup
  ) %>% 
  rename_with(~gsub('X(\\d{4}).\\d{2}(\\d{2})', '\\1/\\2', .)) %>% 
  select(
    `Age Group`,
    `NOC Level`, 
    `NOC 2021`, 
    `Occupation Description`,
    `Region ID`,
    `Region Name`,
    matches('^\\d'),
    `Quality Indicator`,
    `Public Post-Secondary Coverage Indicator`
  )

internal_release_data

# 3. Join into final excel file ---- 

is_draft <- TRUE
today_string <- format(today(), '%Y%m%d')

# get readme template 
###################################################
# WARNING!! 
# (MAY REQUIRE MANUAL UPDATES TO THE NOTES STILL!)
#################################################
template <- glue::glue('{lan}\\development\\work\\internal_use_template.xlsx')

if (is_draft){
  start_file <- "draft_"
} else {
  start_file <- ""
}
final_excel <- glue::glue('{lan}\\development\\work\\adhoc-outputs\\{start_file}internal_use_PSSM_2023-24_to_2034-35_{today_string}.xlsx')

# load template 
outwb <- loadWorkbook(template)

# set a couple of styles
csDraft <- createStyle(fontSize = 20, fontColour = "#FF0000", textDecoration="bold")
csRegularBold <- createStyle(valign="center", halign='center', wrapText=TRUE, textDecoration = "bold")
csCount <- createStyle(halign = "right")  
csPerc <- createStyle(halign = "right", numFmt = "0.0%")  ## Percent cells 

# if draft, add 'DRAFT' to first page 
if (is_draft){
  writeData(outwb, "User Guide", x='DRAFT', startRow=1, startCol=1)
  addStyle(outwb, "User Guide", style = csDraft, rows = 1, cols = 1, gridExpand = TRUE)
} 

# add new sheet for grads ----
sheet <- addWorksheet(outwb, sheetName="Graduate Projections") 
n_rows <- nrow(grads)
n_cols <- length(grads)

# add data to sheet
startRow <- 1
if (is_draft){
  writeData(outwb, sheet, x='DRAFT', startRow=1, startCol=1)
  addStyle(outwb, sheet, style = csDraft, rows = 1, cols = 1, gridExpand = TRUE)
  startRow <- 2
} 

writeData(outwb, sheet, grads, colNames = TRUE, rowNames = FALSE, startRow=startRow, startCol=1, withFilter = FALSE,
          keepNA = FALSE)

# Freeze top row
freezePane(outwb,sheet, firstActiveRow=startRow+1)

# style headers
addStyle(outwb, sheet, style=csRegularBold, rows=startRow, cols=1:n_cols)


# add new sheet for occupations ----
sheet <- addWorksheet(outwb, sheetName="Occupation Projections") 
n_rows <- nrow(internal_release_data)
n_cols <- length(internal_release_data)

# add data to sheet 
startRow <- 1
if (is_draft){
  writeData(outwb, sheet, x='DRAFT', startRow=1, startCol=1)
  addStyle(outwb, sheet, style = csDraft, rows = 1, cols = 1, gridExpand = TRUE)
  startRow <- 2
} 

writeData(outwb, sheet, internal_release_data, colNames = TRUE, rowNames = FALSE, startRow=startRow, startCol=1, withFilter = TRUE,
          keepNA = FALSE)

# Freeze top row
freezePane(outwb,sheet, firstActiveRow=startRow+1)

# style headers
addStyle(outwb, sheet, style=csRegularBold, rows=startRow, cols=1:n_cols)

# set col widths
rg_col <- which(names(internal_release_data) == "Region Name")
setColWidths(outwb,sheet,cols = rg_col, widths = "auto")
occ_col <- which(names(internal_release_data) == "Occupation Description")
setColWidths(outwb,sheet,cols = occ_col, widths = 40)

# style the percentages 
qi_col <- which(names(internal_release_data) == "Quality Indicator")
ci_col <- which(names(internal_release_data) =="Public Post-Secondary Coverage Indicator")
addStyle(outwb, sheet, style = csPerc, rows=(startRow+1):(n_rows+2), cols=qi_col)
addStyle(outwb, sheet, style = csPerc, rows=(startRow+1):(n_rows+2), cols=ci_col)

# save output 
saveWorkbook(outwb, final_excel, overwrite=TRUE)

