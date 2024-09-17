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

# HISTORICAL - Pop/Enrolments ----
# grab historical data and append 
historical_forecasted_enrolments <- tibble(p_enrolments %>% 
                                             select(YEAR, AGE_GROUP, GENDER, N=N) %>% 
                                             mutate(TYPE='H. ENROLMENT') %>% 
                                             bind_rows(f_enrolments_t %>% 
                                                         select(YEAR, AGE_GROUP, GENDER, N=N_ENROL_FORECASTED) %>% 
                                                         mutate(TYPE='F. ENROLMENT')))

pop_projections_for_compare <- population_projections %>% 
  mutate(N = POP) %>% 
  select(YEAR, AGE_GROUP, GENDER, N) %>% 
  mutate(TYPE='POPULATION') %>% 
  filter(YEAR<2035)

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


# HISTORICAL - grads ----
historical_forecasted_grads <- annual_grad_rate %>% 
  select(YEAR, AGE_GROUP, GENDER, N=N_GRADS) %>% 
  mutate(TYPE = 'H. GRADS') %>% 
  bind_rows(
    f_graduates_t %>% 
      select(YEAR, AGE_GROUP, GENDER, N=N_GRAD_FORECASTED) %>% 
      mutate(TYPE = 'F. GRADS')
  )

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

## HISTORICAL - combined GRAD CRED ----
historical_forecasted_grad_creds <- 
  credentials %>% 
  select(YEAR, AGE_GROUP, GENDER, PSI_CREDENTIAL_CATEGORY, N) %>% 
  mutate(TYPE = 'H. GRADS by Cred') %>% 
  bind_rows(
    f_graduates %>% 
      select(YEAR, AGE_GROUP, GENDER, PSI_CREDENTIAL_CATEGORY, N) %>% 
      mutate(TYPE = 'F. GRADS by Cred')
  )

# get things on same ish scale
library(ggplot2)
min_ns <- historical_forecasted_enrolments %>% 
  bind_rows(pop_projections_for_compare) %>% 
  bind_rows(historical_forecasted_grads) %>% 
  bind_rows(historical_forecasted_grad_creds) %>%
  mutate(GROUP = case_when(
    TYPE=='POPULATION' ~ TYPE,
    TRUE ~ str_sub(TYPE, start=4)
    )
  ) %>% 
  group_by(YEAR, GROUP) %>% 
  mutate(n = sum(N)) %>%
  ungroup() %>% 
  group_by(GROUP) %>% 
  mutate(min_n = min(n)) %>% 
  ungroup() %>% 
  distinct(GROUP, TYPE, min_n)

historical_forecasted_enrolments %>% 
  bind_rows(pop_projections_for_compare) %>% 
  bind_rows(historical_forecasted_grads) %>% 
  #bind_rows(historical_forecasted_grad_creds) %>% 
  left_join(min_ns, by = 'TYPE') %>% 
  group_by(YEAR, GROUP) %>% 
  summarize(n = sum(N), min_n=min(min_n)) %>%  # View()
  mutate(
    n = n/min_n
  ) %>% 
  ggplot(aes(x=YEAR, y=n, color=GROUP)) +
  geom_line(linewidth=1) +
  geom_vline(aes(xintercept = 2023))

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

# HISTORICAL - near-completers ----
# note that near-completer data (right now) only goes back 5 years to 2019
# will want to make sure that comparisons only go that far back
# could in theory go farther, just easier right now to stay at 2019
# preprocess nc data before joining with graduates
T_DACSO_Near_Completers_RatioByGender_year <- dbReadTable(decimal_con, "T_DACSO_Near_Completers_RatioByGender_year") %>%
  janitor::clean_names("all_caps") %>%
  mutate(PSI_CREDENTIAL_CATEGORY = PRGM_CREDENTIAL_AWARDED_NAME) %>%
  select(YEAR, PSI_CREDENTIAL_CATEGORY, AGE_GROUP, GENDER, N_NC_STP) %>%
  mutate(GENDER = if_else(GENDER == 1, 'Male', 'Female'))

# recode age groups 35-44, 45-54, 55-64  => 35 to 64 
T_DACSO_Near_Completers_RatioByGender_year <- f_graduates %>% 
  distinct(AGE_GROUP) %>%
  rename("AGE_GROUP_RECODE" = "AGE_GROUP") %>%
  mutate(AGE_GROUP = if_else(AGE_GROUP_RECODE %in% c("35 to 44", "45 to 54", "55 to 64"), "35 to 64", AGE_GROUP_RECODE)) %>%
  full_join(T_DACSO_Near_Completers_RatioByGender_year, relationship = "many-to-many") %>%
  select(-AGE_GROUP) %>%
  rename("AGE_GROUP" = "AGE_GROUP_RECODE")

f_graduates_nc_historical <- T_DACSO_Near_Completers_RatioByGender_year %>% 
  select(PSI_CREDENTIAL_CATEGORY, YEAR, AGE_GROUP, GENDER, N=N_NC_STP) %>% 
  filter(YEAR < 2023)

historical_forecasted_grad_ncs <- 
  f_graduates_nc_historical %>% 
  select(YEAR, AGE_GROUP, GENDER, PSI_CREDENTIAL_CATEGORY, N) %>% 
  mutate(TYPE = 'H. NCs') %>% 
  bind_rows(
    f_graduates_nc %>% 
      select(YEAR, AGE_GROUP, GENDER, PSI_CREDENTIAL_CATEGORY, N) %>% 
      mutate(TYPE = 'F. NCs')
  )

f_graduates_nc %>% 
  bind_rows(f_graduates_nc_historical) %>% 
  group_by(YEAR) %>% 
  summarize(n = sum(N)) %>% 
  ggplot(aes(x=YEAR,y=n)) +
  geom_line() + geom_point()

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

# HISTORICAL - Update PSSM CRED LABEL for GRADS/NCs
hf_grad_creds <- historical_forecasted_grad_creds %>%
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

hf_nc <- historical_forecasted_grad_ncs %>%
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

hf_grad_nc_creds_agg <- hf_grad_creds %>% rbind(hf_nc) %>%
  group_by(PSSM_CRED, YEAR, AGE_GROUP) %>%
  summarise(GRADUATES=sum(N)) %>%
  mutate(TYPE = if_else(YEAR<2023, 'H. Total G NC by Cred', 'F. Total G NC By Cred')) %>% 
  mutate(SURVEY = 'Credential_Projections_Transp') %>%
  mutate(YEAR = paste0(as.character(YEAR), "/", as.character(YEAR + 1))) %>%
  filter(!AGE_GROUP %in% c('65 to 89','15 to 16'))

# get things on same ish scale
library(ggplot2)
min_ns <- historical_forecasted_enrolments %>% 
  bind_rows(pop_projections_for_compare) %>% 
  bind_rows(historical_forecasted_grads) %>% 
  bind_rows(historical_forecasted_grad_creds) %>%
  bind_rows(historical_forecasted_grad_ncs) %>% 
  mutate(GROUP = case_when(
    TYPE=='POPULATION' ~ TYPE,
    TRUE ~ str_sub(TYPE, start=4)
  )
  ) %>% 
  group_by(YEAR, GROUP) %>% 
  mutate(n = sum(N)) %>%
  ungroup() %>% 
  group_by(GROUP) %>% 
  mutate(min_n = min(n)) %>% 
  ungroup() %>% 
  distinct(GROUP, TYPE, min_n)

historical_forecasted_enrolments %>% 
  bind_rows(pop_projections_for_compare) %>% 
  #bind_rows(historical_forecasted_grads) %>% 
  bind_rows(historical_forecasted_grad_creds) %>% 
  bind_rows(historical_forecasted_grad_ncs) %>% 
  left_join(min_ns, by = 'TYPE') %>% 
  group_by(YEAR, GROUP) %>% 
  summarize(n = sum(N), min_n=min(min_n)) %>%  # View()
  mutate(
    n = n/min_n
  ) %>% 
  ggplot(aes(x=YEAR, y=n, color=GROUP)) +
  geom_line(linewidth=1) +
  geom_vline(aes(xintercept = 2023))


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

dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."Graduate_Projections"')), f_graduates_agg, overwrite = TRUE)

# ---- Graduate Projections for Trades ----
# TODO: add in trades to Graduate Projections (above) and project same as APPSO. 
TRD_Graduates <- dbGetQuery(decimal_con, "SELECT * FROM TRD_Graduates")

  
  