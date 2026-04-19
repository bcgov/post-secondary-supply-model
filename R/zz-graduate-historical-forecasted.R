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

# [SELECT INTO] Create Q_1_Grad_Projections_by_Age_by_Program
# ---- Q_1_Grad_Projections_by_Age_by_Program ----
Q_1_Grad_Projections_by_Age_by_Program <-
  "SELECT Cohort_Program_Distributions.PSSM_Credential AS PSSM_Credential,
        Graduate_Projections.PSSM_CRED,
        Graduate_Projections.Age_Group,
        Graduate_Projections.Year,
        Cohort_Program_Distributions.LCP4_CD,
        Cohort_Program_Distributions.GRAD_STATUS,
        Cohort_Program_Distributions.TTRAIN,
        Cohort_Program_Distributions.LCIP4_CRED,
        [Graduate_Projections].[Graduates]*[Cohort_Program_Distributions].[Percent] AS Grads
INTO Q_1_Grad_Projections_by_Age_by_Program
FROM    ((T_Exclude_from_Projections_LCP4_CD
RIGHT JOIN (Graduate_Projections
  INNER JOIN Cohort_Program_Distributions
    ON  (Graduate_Projections.Year = Cohort_Program_Distributions.Year)
    AND (Graduate_Projections.Age_Group = Cohort_Program_Distributions.Age_Group)
    AND (Graduate_Projections.PSSM_CRED = Cohort_Program_Distributions.PSSM_CRED))
  ON    T_Exclude_from_Projections_LCP4_CD.LCIP_LCP4_CD = Cohort_Program_Distributions.LCP4_CD)
LEFT JOIN T_Exclude_from_Projections_PSSM_Credential
  ON    Cohort_Program_Distributions.PSSM_Credential = T_Exclude_from_Projections_PSSM_Credential.PSSM_Credential)
LEFT JOIN T_Exclude_from_Projections_LCIP4_CRED
  ON    Cohort_Program_Distributions.LCIP4_CRED = T_Exclude_from_Projections_LCIP4_CRED.LCIP4_CRED
WHERE   (((T_Exclude_from_Projections_LCP4_CD.LCIP_LCP4_CD) Is Null)
  AND   ((T_Exclude_from_Projections_PSSM_Credential.PSSM_Credential) Is Null)
  AND   ((T_Exclude_from_Projections_LCIP4_CRED.LCIP4_CRED) Is Null));"
dbExecute(decimal_con, Q_1_Grad_Projections_by_Age_by_Program) 

# [SELECT INTO] Create Q_1c_Grad_Projections_by_Program

# ---- Q_1c_Grad_Projections_by_Program ----
Q_1c_Grad_Projections_by_Program <-
  "SELECT Q_1_Grad_Projections_by_Age_by_Program.PSSM_Credential,
        Q_1_Grad_Projections_by_Age_by_Program.PSSM_CRED,
        tbl_Age_Groups_Rollup.Age_Group_Rollup,
        tbl_Age_Groups_Rollup.Age_Group_Rollup_Label,
        Q_1_Grad_Projections_by_Age_by_Program.Year,
        Q_1_Grad_Projections_by_Age_by_Program.GRAD_STATUS,
        Q_1_Grad_Projections_by_Age_by_Program.TTRAIN,
        Q_1_Grad_Projections_by_Age_by_Program.LCP4_CD,
        Q_1_Grad_Projections_by_Age_by_Program.LCIP4_CRED,
        Sum(Q_1_Grad_Projections_by_Age_by_Program.Grads) AS Grads
INTO Q_1c_Grad_Projections_by_Program
FROM    (Q_1_Grad_Projections_by_Age_by_Program
INNER JOIN tbl_Age_Groups
  ON    Q_1_Grad_Projections_by_Age_by_Program.Age_Group = tbl_Age_Groups.Age_Group_Label)
INNER JOIN tbl_Age_Groups_Rollup
  ON    tbl_Age_Groups.Age_Group_Rollup = tbl_Age_Groups_Rollup.Age_Group_Rollup
GROUP BY Q_1_Grad_Projections_by_Age_by_Program.PSSM_Credential,
        Q_1_Grad_Projections_by_Age_by_Program.PSSM_CRED,
        tbl_Age_Groups_Rollup.Age_Group_Rollup,
        tbl_Age_Groups_Rollup.Age_Group_Rollup_Label,
        Q_1_Grad_Projections_by_Age_by_Program.Year,
        Q_1_Grad_Projections_by_Age_by_Program.GRAD_STATUS,
        Q_1_Grad_Projections_by_Age_by_Program.TTRAIN,
        Q_1_Grad_Projections_by_Age_by_Program.LCP4_CD,
        Q_1_Grad_Projections_by_Age_by_Program.LCIP4_CRED;"
dbExecute(decimal_con, Q_1c_Grad_Projections_by_Program) 

# ---- Final Table ----

# [SQL]


# ---- qry99_Presentations_Graduates_Appendix ----
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
