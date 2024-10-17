# This script pulls parts of the 07-occupation-projections script out
# And attempts to re-produce the final graduates table, but with historical records

library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(ggplot2)

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
my_schema <- 'IDIR\\BASHCROF'
my_schema <- 'IDIR\\LFREDRIC'
my_schema <- 'IDIR\\ALOWERY'
grads <- tibble(dbGetQuery(
  decimal_con,
  glue::glue('
   SELECT 
	t1.age_group,
    t4.age_group_rollup_label,
    t2.pssm_credential_name,
    t1.year,
    t1.graduates
  FROM "{my_schema}".[Graduate_Projections_Include_Historical] t1
  LEFT JOIN "{my_schema}".[T_PSSM_Credential_Grouping_Appendix] t2
	ON t1.PSSM_CREDENTIAL = t2.PSSM_CREDENTIAL
  LEFT JOIN "{my_schema}".[tbl_Age_Groups] t3
	ON t1.age_group = t3.age_group_label
  LEFT JOIN "{my_schema}".[tbl_Age_Groups_Rollup] t4
	ON t3.age_group_rollup = t4.age_group_rollup
             '
  )
)
)

grads_proj <- tibble(dbGetQuery(
  decimal_con,
  glue::glue('
   SELECT 
	t1.age_group,
    t4.age_group_rollup_label,
    t2.pssm_credential_name,
    t1.year,
    t1.graduates
  FROM "{my_schema}".[Graduate_Projections] t1
  LEFT JOIN "{my_schema}".[T_PSSM_Credential_Grouping_Appendix] t2
	ON t1.PSSM_CREDENTIAL = t2.PSSM_CREDENTIAL
  LEFT JOIN "{my_schema}".[tbl_Age_Groups] t3
	ON t1.age_group = t3.age_group_label
  LEFT JOIN "{my_schema}".[tbl_Age_Groups_Rollup] t4
	ON t3.age_group_rollup = t4.age_group_rollup
             '
  )
)
)

grads %>% 
  filter(year>='2023/2024', !grepl('Apprenticeship', pssm_credential_name)) %>% 
  all.equal(grads_proj %>% filter(!grepl('Apprenticeship', pssm_credential_name)))

grads %>% filter(year == '2023/2024')
grads_proj %>% filter(year == '2023/2024')

# fill in missing years
grads_completed <-  grads %>% 
  arrange(pssm_credential_name, age_group, year) %>%
  complete(pssm_credential_name, age_group, year) %>%
  group_by(pssm_credential_name, age_group) %>%
  fill(graduates, age_group_rollup_label)

grads_completed %>% View()

grads_completed %>% filter(pssm_credential_name == 'Apprenticeship') %>% 
  filter(age_group_rollup_label == '17 to 29') %>% 
  filter(year>='2023/2024')

grads_by_age_cred <- grads_completed %>% 
  filter(year>='2018/2019') %>% 
  group_by(age_group_rollup_label, pssm_credential_name, year) %>% 
  summarise(n = round(sum(graduates, drop.na=TRUE), 0)) %>% 
  pivot_wider(id_cols = c('age_group_rollup_label', 'pssm_credential_name'), names_from = 'year', values_from = 'n') %>% 
  arrange(pssm_credential_name, age_group_rollup_label) %>% 
  filter(!is.na(age_group_rollup_label))
  


grads_completed %>% 
  mutate(year = as.numeric(str_sub(year, 1,4))) %>% 
  group_by(pssm_credential_name, year) %>% 
  summarize(n = sum(graduates)) %>% # View() 
  ggplot(aes(x = year, y=n, color=pssm_credential_name)) +
  geom_line()+
  geom_vline(aes(xintercept = 2023))


grads_by_age_cred %>% write_csv(
  glue::glue('{lan}\\development\\work\\adhoc-outputs\\graduate_projections_include_historical_no_ptib.csv')
)
