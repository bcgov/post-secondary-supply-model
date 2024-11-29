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

# This script contains code to calculate graduation projection forecasts for the post-secondary supply model.
# At a high-level, the methodology is as follows:
#   1. Use estimates of population and population projections to calculate enrollment rates by age and gender
#   2. Enrolment rates for 2002/03 to 2018/19 are forecasted for 5 years and then held constant for 5 years
#   3. Forecasted enrolment rates are applied to the project population by age and gender to derive forecasted enrollment
#   4.  A 2 year average graduation rate (calculated as a percentage of enrollment by age group/gender) is multiplied by 
#   forecasted enrollments to derive forecasted graduates.
#   5.  forecasted graduates by credential/age/year is extrapolated from 2-yr average distribution of graduates by credential

# Notes: Development\Graduate Model\Enrollment & Graduation Projections 2019-2020 PEOPLE 2020.xlsm (2019) and documentation reveal different #'s
# of output years.  Used 12 for PSSM2023.
# Script handles only Male and Female 


library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(RJDBC)

# ---- Configure LAN and file paths ----
db_config <- config::get("decimal")
lan <- config::get("lan")
my_schema <- config::get("myschema")

# Note: SQL repeated in sql/01.  Delete one copy after merging 2023 run.
source("./sql/04-graduate-projections/04-graduate-projections-sql.R")

# ---- Connection to decimal ----
db_config <- config::get("decimal")
decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")

# ---- Check for required data tables ----
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."population_projections"')))

# ---- Read data from decimal  ----
population_projections <- dbReadTable(decimal_con, "population_projections")
min_enrolments <- dbGetQuery(decimal_con, "SELECT * FROM qry09c_MinEnrolment")
credentials <- dbGetQuery(decimal_con, "SELECT * FROM Credential_By_Year_Gender_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs")

# ---- Tidy data for calculations  ----
population_projections <- population_projections %>%
  select(-c(REGION, LOCAL_HEALTH_AREA, TOTAL)) %>%
  pivot_longer(cols = starts_with("X"),  names_to = "AGE_GROUP", values_to = "POP") %>% 
  filter(GENDER %in% c("F", "M")) %>%
  mutate(GENDER = case_when(GENDER == 'F' ~ 'Female', GENDER == 'M' ~ 'Male', TRUE ~ NA)) %>%
  mutate(AGE_GROUP = gsub("X", "", AGE_GROUP)) %>% 
  mutate(AGE_GROUP = gsub("_TO_", " to ", AGE_GROUP)) %>%
  select(-TYPE)

min_enrolments <- min_enrolments %>% 
  rename("AGE_GROUP" = "Groups", "N" = "Expr1", "GENDER" = "PSI_GENDER", "YEAR" = "PSI_SCHOOL_YEAR") %>% 
  mutate(AGE_GROUP = gsub("Female|Male|Gender Diverse Gender Diverse", "", AGE_GROUP)) %>%
  mutate(YEAR = as.numeric(stringr::str_sub(YEAR, 1, 4))) %>%
  arrange(GENDER, AGE_GROUP, YEAR)

credentials <- credentials %>%   
  rename("AGE_GROUP" = "AgeGroup", "N" = "Count", 
         "GENDER" = "psi_gender_cleaned", "YEAR" = "PSI_AWARD_SCHOOL_YEAR_DELAYED") %>%
  mutate(YEAR = as.numeric(stringr::str_sub(YEAR, 1, 4))) %>%
  select(-Expr1) %>%
  filter(YEAR >=2006, YEAR <=2022)

# ---- Forecasted Enrolments ----
## Enrolment Rate ----
p_enrolments <- min_enrolments %>% 
  inner_join(population_projections, by = join_by(GENDER, AGE_GROUP, YEAR)) %>% # removes Gender Diverse
  mutate(P = 100*N/POP)

p_enrolments %>%
  mutate(P=round(P,2)) %>%
  pivot_wider(id_cols = c(GENDER, AGE_GROUP), values_from = P, names_from = YEAR) %>% 
  View()

## Forecasted Enrolment Rate ----
# workbook forecasting done for 12 years
f_enrolments <- p_enrolments |> 
  split(list(p_enrolments$AGE_GROUP, p_enrolments$GENDER), drop=TRUE, sep = "_") |>
  map(\(df) lm(P ~ YEAR, data = df)) |> 
  map(predict.lm, 
      newdata = data.frame(YEAR = c(2023:2027,rep(2027,7)), 
                           row.names = as.character(2023:2034)))

## Forecasted Enrolments ----
rn <- as.numeric(rownames(data.frame(f_enrolments)))
f_enrolments_t <- data.frame(f_enrolments) %>% 
  mutate(YEAR = rn) %>%
  pivot_longer(cols = c(-YEAR), values_to = "RATE") %>%
  separate_wider_delim(cols = name, delim = "_", names = c("AGE_GROUP", "GENDER")) %>%
  mutate(AGE_GROUP = gsub("\\.", " ", AGE_GROUP)) %>%
  mutate(AGE_GROUP = gsub("X", "", AGE_GROUP))

f_enrolments_t  <- f_enrolments_t %>% 
  inner_join(population_projections, by = join_by(YEAR, AGE_GROUP, GENDER)) %>%
  mutate(N_ENROL_FORECASTED = RATE*POP*.01)

f_enrolments_t %>% 
  pivot_wider(id_cols = c(AGE_GROUP, GENDER), values_from = N_ENROL_FORECASTED, names_from = YEAR) 

# ---- Forecasted Graduates ----
## Graduation Rates (annual, as a percentage of enrolment) ----
annual_grad_rate <- credentials %>% 
  summarize(N_GRADS = sum(N, na.rm = TRUE), .by = c(GENDER, AGE_GROUP, YEAR)) %>%
  inner_join( # removes Gender Diverse
    min_enrolments %>%
      summarize(N_ENROL = sum(N, na.rm = TRUE), .by = c(GENDER, AGE_GROUP, YEAR)), 
    by = join_by(GENDER, AGE_GROUP, YEAR)) %>%
  mutate(P_GRADS_ENROL = 100*N_GRADS/N_ENROL) 

## Graduation Rate (2-yr average, as percentage of enrolment) ----
avg_2_yr_grad_rate <- annual_grad_rate %>% 
  filter(YEAR %in% 2021:2022) %>% 
  summarise(GRAD_RATE = sum(N_GRADS)/sum(N_ENROL), 
            .by  = c(GENDER, AGE_GROUP))

f_graduates_t <- f_enrolments_t %>% 
  inner_join(avg_2_yr_grad_rate, by = join_by(AGE_GROUP, GENDER)) %>%
  mutate(N_GRAD_FORECASTED = N_ENROL_FORECASTED * GRAD_RATE)


## Forecasted Graduates by Credential ----
f_graduates_t  <- f_graduates_t  %>% 
  select(YEAR, AGE_GROUP, GENDER, N_GRAD_FORECASTED)

## 2-yr average distribution of graduates by credential ----
avg_2_yr_credentials <- credentials %>% 
  filter(YEAR %in% 2021:2022, GENDER != 'Gender Diverse') %>%
  summarise(YR_2_N = sum(N), .by = c(GENDER, AGE_GROUP, PSI_CREDENTIAL_CATEGORY)) %>%
  group_by(GENDER, AGE_GROUP) %>%
  mutate(N=sum(YR_2_N), 
         P = round(YR_2_N/N,3)) %>% 
  ungroup() %>%
  complete(GENDER, AGE_GROUP, PSI_CREDENTIAL_CATEGORY, fill = list(YR_2_N=0,N=0,P=0)) %>% 
  select(AGE_GROUP, GENDER, PSI_CREDENTIAL_CATEGORY, P) 

f_graduates <- f_graduates_t  %>% 
  full_join(avg_2_yr_credentials, relationship = "many-to-many") %>%
  mutate(N_GRAD_FORECASTED = N_GRAD_FORECASTED*P) %>%
  select(-P) %>% 
  summarize(N=sum(N_GRAD_FORECASTED), .by  = c(PSI_CREDENTIAL_CATEGORY, YEAR, AGE_GROUP, GENDER))

# ---- Projected Near Completers (NC) ----
# preprocess nc data before joining with graduates
T_DACSO_Near_Completers_RatioByGender <- dbReadTable(decimal_con, "T_DACSO_Near_Completers_RatioByGender") %>%
  janitor::clean_names("all_caps") %>%
  mutate(PSI_CREDENTIAL_CATEGORY = PRGM_CREDENTIAL_AWARDED_NAME) %>%
  select(PSI_CREDENTIAL_CATEGORY, AGE_GROUP, GENDER, RATIO) %>%
  mutate(GENDER = if_else(GENDER == 1, 'Male', 'Female'))

# recode age groups 35-44, 45-54, 55-64  => 35 to 64 
T_DACSO_Near_Completers_RatioByGender <- f_graduates %>% 
  distinct(AGE_GROUP) %>%
  rename("AGE_GROUP_RECODE" = "AGE_GROUP") %>%
  mutate(AGE_GROUP = if_else(AGE_GROUP_RECODE %in% c("35 to 44", "45 to 54", "55 to 64"), "35 to 64", AGE_GROUP_RECODE)) %>%
  full_join(T_DACSO_Near_Completers_RatioByGender, relationship = "many-to-many") %>%
  select(-AGE_GROUP) %>%
  rename("AGE_GROUP" = "AGE_GROUP_RECODE")

# join nc to graduates
f_graduates_nc <- f_graduates %>% 
  inner_join(T_DACSO_Near_Completers_RatioByGender) %>%
  mutate(N=N*RATIO) %>%
  select(-RATIO)

# make pssm cred label
f_graduates <- f_graduates %>% 
  filter(!AGE_GROUP %in% c("15 to 16")) %>%
  mutate(PSSM_CRED = case_when(
         toupper(PSI_CREDENTIAL_CATEGORY) == "ADVANCED CERTIFICATE" ~ "1 - ADCT OR ADIP",
         toupper(PSI_CREDENTIAL_CATEGORY) == "ASSOCIATE DEGREE" ~ "1 - ADGR OR UT",
         toupper(PSI_CREDENTIAL_CATEGORY) == "ADVANCED DIPLOMA" ~ "1 - ADCT OR ADIP",
         toupper(PSI_CREDENTIAL_CATEGORY) == "BACHELORS DEGREE" ~ "BACH",
         toupper(PSI_CREDENTIAL_CATEGORY) == "CERTIFICATE" ~ "1 - CERT",
         toupper(PSI_CREDENTIAL_CATEGORY) == "DIPLOMA" ~ "1 - DIPL",
         toupper(PSI_CREDENTIAL_CATEGORY) == "DOCTORATE" ~ "DOCT",
         toupper(PSI_CREDENTIAL_CATEGORY) == "GRADUATE CERTIFICATE" ~ "GRCT OR GRDP",
         toupper(PSI_CREDENTIAL_CATEGORY) == "GRADUATE DIPLOMA" ~ "GRCT OR GRDP",
         toupper(PSI_CREDENTIAL_CATEGORY) == "MASTERS DEGREE" ~ "MAST",
         toupper(PSI_CREDENTIAL_CATEGORY) == "NONE" ~ "INVALID",
         toupper(PSI_CREDENTIAL_CATEGORY) == "OTHER" ~ "",
         toupper(PSI_CREDENTIAL_CATEGORY) == "POST-DEGREE CERTIFICATE" ~ "1 - PDCT OR PDDP",
         toupper(PSI_CREDENTIAL_CATEGORY) == "POST-DEGREE DIPLOMA" ~ "1 - PDCT OR PDDP",
         toupper(PSI_CREDENTIAL_CATEGORY) == "FIRST PROFESSIONAL DEGREE" ~ "PDEG",
         toupper(PSI_CREDENTIAL_CATEGORY) == "SHORT CERTIFICATE" ~ "INVALID",
         toupper(PSI_CREDENTIAL_CATEGORY) == "UNIVERSITY TRANSFER" ~ "1 - ADGR OR UT", 
         TRUE ~ NA))


# update pssm cred label for nc
f_graduates_nc <- f_graduates_nc %>%
  filter(!AGE_GROUP %in% c("15 to 16")) %>%
  mutate(PSSM_CRED = case_when(
    toupper(PSI_CREDENTIAL_CATEGORY) == "ASSOCIATE DEGREE" ~ "3 - ADGR OR UT",
    toupper(PSI_CREDENTIAL_CATEGORY) == "ADVANCED DIPLOMA" ~ "3 - ADCT OR ADIP",
    toupper(PSI_CREDENTIAL_CATEGORY) == "CERTIFICATE" ~ "3 - CERT",
    toupper(PSI_CREDENTIAL_CATEGORY) == "DIPLOMA" ~ "3 - DIPL",
    toupper(PSI_CREDENTIAL_CATEGORY) == "POST-DEGREE CERTIFICATE" ~ "3 - PDCT OR PDDP",
    toupper(PSI_CREDENTIAL_CATEGORY) == "POST-DEGREE DIPLOMA" ~ "3 - PDCT OR PDDP",
    toupper(PSI_CREDENTIAL_CATEGORY) == "UNIVERSITY TRANSFER" ~ "3 - ADGR OR UT", 
    TRUE ~ "NA"))

f_graduates_agg <- f_graduates %>% rbind(f_graduates_nc) %>%
  group_by(PSSM_CRED, YEAR, AGE_GROUP) %>%
  summarise(GRADUATES=sum(N)) %>%
  mutate(SURVEY = 'Credential_Projections_Transp') %>%
  mutate(YEAR = paste0(as.character(YEAR), "/", as.character(YEAR + 1))) %>%
  filter(!AGE_GROUP %in% c('65 to 89','15 to 16'))

# ---- Graduate Projections for Apprenticeship ----
APPSO_Graduates <- dbGetQuery(decimal_con, "SELECT * FROM APPSO_Graduates")

appso_2_yr_avg <- APPSO_Graduates %>% 
  mutate(YEAR = str_replace(SUBM_CD, "C_Outc", "20")) %>%
  rename("N" = "EXPR1", "PSSM_CRED" = "PSSM_CREDENTIAL") %>%
  summarize(N = sum(N, na.rm = TRUE), .by = c(YEAR, PSSM_CRED, AGE_GROUP)) %>%
  filter(YEAR %in% c('2022','2023')) %>%
  summarize(GRADUATES = sum(N/2, na.rm = TRUE), .by = c(PSSM_CRED, AGE_GROUP)) %>%
  mutate(YEAR = "2023/2024") %>%
  mutate(SURVEY = 'APPSO') %>%
  mutate(PSI_CREDENTIAL_CATEGORY = "NA") %>%
  filter(!is.na(AGE_GROUP)) %>%
  filter(!AGE_GROUP %in% c('65 to 89','15 to 16'))

f_graduates_agg <- f_graduates_agg %>% 
  rbind(appso_2_yr_avg) %>%
  mutate(PSSM_CREDENTIAL = gsub("(1 - )|(3 - )", "", PSSM_CRED)) 

dbWriteTable(decimal_con, name = "Graduate_Projections", f_graduates_agg, overwrite = TRUE)

  
  