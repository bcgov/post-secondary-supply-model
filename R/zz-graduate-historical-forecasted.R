# This script pulls parts of the 07-occupation-projections script out
# And attempts to re-produce the final graduates table, but with historical records

library(tidyverse)
library(RODBC)
library(config)
library(DBI)

# ---- Configure LAN and file paths ----
db_config <- config::get("decimal")
lan <- config::get("lan")
my_schema <- config::get("myschema")

source("./sql/zz-historical/historical-projections.R")

# ---- Connection to decimal ----
db_config <- config::get("decimal")
decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")

# ---- Check for required data tables ----
# Derived tables
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Cohort_Program_Distributions"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Graduate_Projections"')))

# Lookups
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_Exclude_from_Projections_LCP4_CD"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_Exclude_from_Projections_LCIP4_CRED"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_Exclude_from_Projections_PSSM_Credential"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."tbl_Age_Groups"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."tbl_Age_Groups_Rollup"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_PSSM_Credential_Grouping_Appendix"')))


# ---- Q_1 Series ---- 
dbExecute(decimal_con, Q_1_Grad_Projections_by_Age_by_Program) 
dbExecute(decimal_con, Q_1c_Grad_Projections_by_Program) 

# ---- Final Table ----
dbGetQuery(decimal_con, qry99_Presentations_Graduates_Appendix) %>% 
  mutate(across(where(is.numeric), round))

# drop tables
dbExecute(decimal_con, "DROP TABLE Q_1_Grad_Projections_by_Age_by_Program")
dbExecute(decimal_con, "DROP TABLE Q_1c_Grad_Projections_by_Program")


# historical numbers - bringing into R instead of multiple queries 
grads <- tibble(dbGetQuery(
  decimal_con,
  glue::glue('
  SELECT 
    age_group,
    pssm_credential_name,
    year,
    graduates
  FROM "{my_schema}".[Graduate_Projections_Include_Historical] t1
  LEFT JOIN "{my_schema}".[T_PSSM_Credential_Grouping_Appendix] t2
    ON t1.PSSM_CREDENTIAL = t2.PSSM_CREDENTIAL
             '
  )
)
)
grads %>% distinct(age_group)

grads %>% 
  mutate(age_group_rollup = case_when(
    age_group %in% c('17 to 19', '20 to 24', '25 to 29') ~ '17 to 29',
    age_group %in% c('30 to 34', '35 to 44') ~ '30 to 44',
    TRUE ~ '45 to 64'
  )) %>% 
  # fill forward the APPSO data
  group_by(age_group_rollup, pssm_credential_name) %>% 
  fill(graduates) %>% 
  ungroup() %>% 
  filter(year>='2023/2024') %>% 
  group_by(age_group_rollup, pssm_credential_name, year) %>% 
  summarise(n = round(sum(graduates),0)) %>% 
  pivot_wider(id_cols = c('age_group_rollup', 'pssm_credential_name'), names_from = 'year', values_from = 'n') %>% 
  arrange(pssm_credential_name, age_group_rollup) %>% 
  ungroup() %>% 
  summarize(sum(`2023/2024`))
