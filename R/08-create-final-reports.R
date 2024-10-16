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
library(RODBC)
library(config)
library(DBI)
library(openxlsx)
library(lubridate)

# date for timestamping outputs 
today_string <- format(today(), '%Y%m%d')

# draft flag
# toggle this flag to switch between draft/final results 
# note this only updates superficial elements of the final excel tables 
is_draft <- TRUE

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
exclude_nocs <- tibble(dbReadTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_Suppression_Public_Release_NOC"'))))
  
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

grads

# 2. Create Final Occupation Projections ---- 

# join all 3 models together and create 
# QI (quality indicator) and 
# CI (coverage indicator) columns
years <- tmp_tbl_model %>% select(starts_with('X')) %>% names()
# try to do this generically without mention of first year 
first_year_col <- years[order(years)][1]

tmp_occ <- tmp_tbl_model %>% 
  mutate(first_year = .[first_year_col] %>% pull()) %>% 
  left_join(
    tmp_tbl_qi %>% 
      mutate(QI=.[first_year_col] %>% pull()) %>% 
      select(Expr1,QI),
    by="Expr1"
    ) %>% 
  left_join(
    tmp_tbl_ptib %>% 
      mutate(CI=.[first_year_col] %>% pull()) %>% 
      select(Expr1,CI),
    by="Expr1"
    ) %>% 
  filter(NOC_Level==5) %>% 
  arrange(
    Age_Group_Rollup_Label,
    NOC,
    Current_Region_PSSM_Code_Rollup
    ) %>% 
  mutate(`Public Post-Secondary Coverage Indicator`=ifelse(is.na(CI),0,first_year/CI)) %>% 
  mutate(QI_calc = (abs(first_year-QI)/QI)) %>% 
  mutate(`Quality Indicator`= case_when(QI_calc < 0.25 ~ QI_calc,
                                        (first_year < 10 | QI < 10 | is.na(first_year) | is.na(QI)) ~ NA_integer_,
                                        TRUE ~ QI_calc)) %>% 
  # round outputs 
  mutate(across(starts_with('X'), ~ceiling(.)))
  
# get nice names for things 
internal_release_data <- tmp_occ %>%
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

# 3. Get Public Release Version of Occupations ----
# Public release is a modified version of internal that only includes:
# QI values below a threshold (0.25)
# Filtered to BC region only 
# Excludes low count NOCs 
QI_threshold <- 0.25

exclude_nocs_list <- exclude_nocs %>% mutate(
  exclude = paste0(
    Age_Group_Rollup_Label,'-',
    NOC_CD,
    '-5900'
  )
) %>% pull(exclude)

occ_filtered <- tmp_occ %>% 
  filter(
    Current_Region_PSSM_Code_Rollup == 5900,
    QI_calc < QI_threshold,
    NOC != '99999',
    !Expr1 %in% exclude_nocs_list
  ) 

# grab everything that was excluded by these filters and include in a new category
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
    across(starts_with('X'), ~sum(.)) # already rounded 
  )

# get final outputs with nice names for public
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
    `NOC 2021`= `NOC`,
    `Occupation Description` = ENGLISH_NAME
  ) %>% 
  rename_with(~gsub('X(\\d{4}).\\d{2}(\\d{2})', '\\1/\\2', .)) %>% 
  select(
    `Age Group`,
    `NOC 2021`, 
    `Occupation Description`,
    matches('^\\d')
  )

public_release_data 

# 4. Excel Workbook Settings ---- 

# set a couple of styles
csDraft <- createStyle(fontSize = 20, fontColour = "#FF0000", textDecoration="bold")
csRegularBold <- createStyle(valign="center", halign='center', wrapText=TRUE, textDecoration = "bold")
csCount <- createStyle(halign = "right")  
csPerc <- createStyle(halign = "right", numFmt = "0.0%")  ## Percent cells 

# create a function that will make all changes for both notebooks
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
  is_draft=TRUE,
  is_internal=TRUE
){
  # load template 
  outwb <- loadWorkbook(template)
  
  # if draft, add 'DRAFT' to first page 
  if (is_draft){
    writeData(outwb, "User Guide", x='DRAFT', startRow=1, startCol=1)
    addStyle(outwb, "User Guide", style = csDraft, rows = 1, cols = 1)
  } 
  
  ## add new sheet for grads 
  sheet <- addWorksheet(outwb, sheetName="Graduate Projections") 
  n_rows <- nrow(grad_data)
  n_cols <- length(grad_data)
  
  # add data to sheet
  startRow <- 1
  if (is_draft){
    writeData(outwb, sheet, x='DRAFT', startRow=1, startCol=1)
    addStyle(outwb, sheet, style = csDraft, rows = 1, cols = 1)
    startRow <- 2
  } 
  
  writeData(outwb, sheet, grad_data, colNames = TRUE, rowNames = FALSE, startRow=startRow, startCol=1, withFilter = FALSE,
            keepNA = FALSE)
  
  # Freeze top row
  freezePane(outwb,sheet, firstActiveRow=startRow+1)
  
  # style headers
  addStyle(outwb, sheet, style=csRegularBold, rows=startRow, cols=1:n_cols)
  
  # set col widths
  cred_col <- which(names(grad_data) == "Credential Type")
  setColWidths(outwb,sheet,cols = cred_col, widths = "auto")
  
  ## add new sheet for occupations 
  sheet <- addWorksheet(outwb, sheetName="Occupation Projections") 
  n_rows <- nrow(occ_data)
  n_cols <- length(occ_data)
  
  # add data to sheet 
  startRow <- 1
  if (is_draft){
    writeData(outwb, sheet, x='DRAFT', startRow=1, startCol=1)
    addStyle(outwb, sheet, style = csDraft, rows = 1, cols = 1)
    startRow <- 2
  } 
  
  writeData(
    outwb, sheet, occ_data, 
    colNames = TRUE, rowNames = FALSE, 
    startRow=startRow, startCol=1, 
    withFilter = TRUE,
    keepNA = FALSE
    )
  
  # Freeze top row
  freezePane(outwb, sheet, firstActiveRow=startRow+1)
  
  # style headers
  addStyle(outwb, sheet, style=csRegularBold, rows=startRow, cols=1:n_cols)
  setRowHeights(outwb, sheet, rows=startRow, heights=60)
  
  # set col widths
  occ_col <- which(names(occ_data) == "Occupation Description")
  setColWidths(outwb,sheet,cols = occ_col, widths = 40)
  
  # extra internal release columns
  if (is_internal){
    rg_col <- which(names(occ_data) == "Region Name")
    setColWidths(outwb,sheet,cols = rg_col, widths = "auto")
    # style the percentages 
    qi_col <- which(names(occ_data) == "Quality Indicator")
    ci_col <- which(names(occ_data) =="Public Post-Secondary Coverage Indicator")
    addStyle(outwb, sheet, style = csPerc, rows=(startRow+1):(n_rows+startRow), cols=qi_col)
    addStyle(outwb, sheet, style = csPerc, rows=(startRow+1):(n_rows+startRow), cols=ci_col)
  }
  
  # delete excess rows? not sure why happening
  #deleteData(outwb, sheet, cols=1:n_cols, rows=)
  
  # save output 
  saveWorkbook(outwb, final_excel, overwrite=TRUE)
}

# 5. Final Excel - Internal/Public Use ---- 

##
# WARNING!! 
# (MAY REQUIRE MANUAL UPDATES TO THE NOTES STILL!)
##

if (is_draft){
  start_file <- "draft_"
} else {
  start_file <- ""
}

# Internal 
template <- glue::glue('{lan}\\development\\work\\internal_use_template.xlsx')
final_excel <- glue::glue('{lan}\\development\\work\\adhoc-outputs\\{start_file}internal_use_PSSM_2023-24_to_2034-35_{today_string}.xlsx')

create_final_excel(
  template = template, 
  final_excel = final_excel,
  grad_data = grads,
  occ_data = internal_release_data,
  is_draft = is_draft,
  is_internal=TRUE
)

# Public 
template <- glue::glue('{lan}\\development\\work\\public_use_template.xlsx')
final_excel <- glue::glue('{lan}\\development\\work\\adhoc-outputs\\{start_file}public_use_PSSM_2023-24_to_2034-35_{today_string}.xlsx')

create_final_excel(
  template = template, 
  final_excel = final_excel,
  grad_data = grads,
  occ_data = public_release_data,
  is_draft = is_draft,
  is_internal=FALSE
)

