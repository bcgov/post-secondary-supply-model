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

# pivot years to columns, sort, and make nice names
grads <- grads_rounded %>% 
  bind_rows(grad_totals) %>% 
  pivot_wider(names_from = YEAR, values_from = GRADS) %>% 
  mutate(is_total = PSSM_CREDENTIAL_NAME == 'Total') %>% 
  arrange(AGE_GROUP_ROLLUP_LABEL, is_total, PSSM_CREDENTIAL_NAME) %>% 
  rename(
    `Age Group` = AGE_GROUP_ROLLUP_LABEL,
    `Credential Type` = PSSM_CREDENTIAL_NAME
    )

grads


