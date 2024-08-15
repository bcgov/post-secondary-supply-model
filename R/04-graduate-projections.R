library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(RJDBC)

# ---- Configure LAN and file paths ----
db_config <- config::get("decimal")
lan <- config::get("lan")
my_schema <- config::get("myschema")

source(glue::glue("{lan}/development/sql/gh-source/01d-enrolment-analysis/01d-enrolment-analysis.R"))
source(glue::glue("{lan}/development/sql/moved-to-gh/01b-credential-analysis/01b-credential-analysis.R"))

# ---- Connection to decimal ----
db_config <- config::get("decimal")
decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")

# ---- Check for required data tables ----
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."population_projections"')))

# Used to create queries below.  Alternative is to save the query result as a table.
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."MinEnrolment"'))) 
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."AgeGroupLookup"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."tblCredential_HighestRank"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Credential_Non_Dup"')))

# ---- Read data from decimal  ----
population_projections <- dbReadTable(decimal_con, "population_projections")
min_enrolments <- dbGetQuery(decimal_con, qry09c_MinEnrolment)
credentials <- dbGetQuery(decimal_con, qry20a_4Credential_By_Year_Gender_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs)

# ---- Tidy data for calculations  ----
population_projections <- population_projections %>%
  select(-c(REGION, LOCAL_HEALTH_AREA, TOTAL)) %>%
  pivot_longer(cols = starts_with("X"),  names_to = "AGE_GROUP", values_to = "POP") %>% 
  filter(GENDER %in% c("F", "M")) %>%
  mutate(AGE_GROUP = gsub("X", "", AGE_GROUP)) %>% 
  mutate(AGE_GROUP = gsub("_", " to ", AGE_GROUP))

min_enrolments <- min_enrolments %>% 
  rename("AGE_GROUP" = "Groups", "N" = "Expr1", "GENDER" = "PSI_GENDER", "YEAR" = "PSI_SCHOOL_YEAR") %>% 
  mutate(AGE_GROUP = gsub("F|M", "", AGE_GROUP)) %>%
  mutate(YEAR = as.numeric(stringr::str_sub(YEAR, 1, 4)))

credentials <- credentials %>%   
  rename("AGE_GROUP" = "AgeGroup", "N" = "Count", 
         "GENDER" = "psi_gender_cleaned", "YEAR" = "PSI_AWARD_SCHOOL_YEAR_DELAYED") %>%
  mutate(YEAR = as.numeric(stringr::str_sub(YEAR, 1, 4))) %>%
  select(-Expr1) %>%
  filter(YEAR >=2006, YEAR <=2018)

# ------------------------ Forecasted Enrolments ----
## Enrolment Rate ----
p_enrolments <- min_enrolments %>% 
  left_join(population_projections, by = join_by(GENDER, AGE_GROUP, YEAR)) %>%
  mutate(P = 100*N/POP)

## Exploratory ----
yrs <- 2007:2009
avg_p_enrolments <- 
  min_enrolments %>% 
    filter(YEAR %in% 2007:2009) %>% 
    summarize(SUM_N = sum(N, na.rm = FALSE), .by = c(GENDER, AGE_GROUP)) %>%
  inner_join(
      population_projections %>% 
      filter(YEAR %in% yrs) %>% 
      summarize(SUM_POP = sum(POP, na.rm = FALSE), .by = c(GENDER, AGE_GROUP)), 
      by = join_by(GENDER, AGE_GROUP)) %>%
  mutate(P_AVG = 100*SUM_N/SUM_POP)

## Forecasted Enrolment Rate ----
f_enrolments <- p_enrolments |> 
  split(list(p_enrolments$AGE_GROUP, p_enrolments$GENDER), drop=TRUE, sep = "_") |>
  map(\(df) lm(P ~ YEAR, data = df)) |> 
  map(predict.lm, 
      newdata = data.frame(YEAR = c(2019:2023,rep(2023,5)), 
                           row.names = as.character(2019:2028)))

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

# ---- Forecasted Graduates ----

## Graduation Rates (annual, as a percentage of enrolment) ----
annual_grad_rate <- credentials %>% 
  summarize(N_GRADS = sum(N, na.rm = TRUE), .by = c(GENDER, AGE_GROUP, YEAR)) %>%
  inner_join(
    min_enrolments %>%
      summarize(N_ENROL = sum(N, na.rm = TRUE), .by = c(GENDER, AGE_GROUP, YEAR)), 
    by = join_by(GENDER, AGE_GROUP, YEAR)) %>%
  mutate(P_GRADS_ENROL = 100*N_GRADS/N_ENROL)

## Graduation Rate (2-yr average, as percentage of enrolment) ----
avg_2_yr_grad_rate <- annual_grad_rate %>% 
  filter(YEAR %in% 2017:2018) %>% 
  summarise(GRAD_RATE = sum(N_GRADS)/sum(N_ENROL), 
            .by  = c(GENDER, AGE_GROUP))


## 2-yr average distribution of graduates by credential ----
avg_2_yr_credentials <- credentials %>% 
  filter(YEAR %in% 2017:2018) %>%
  summarise(YR_2_N = sum(N), .by = c(GENDER, AGE_GROUP, PSI_CREDENTIAL_CATEGORY)) %>%
  group_by(GENDER, AGE_GROUP) %>%
  mutate(N=sum(YR_2_N), 
         P = round(YR_2_N/N,3)) 

## Forecasted Graduates ----


f_graduates_t <- f_enrolments_t %>% 
  inner_join(avg_2_yr_grad_rate, by = join_by(AGE_GROUP, GENDER)) %>%
  mutate(N_GRAD_FORECASTED = N_ENROL_FORECASTED * GRAD_RATE)

## Forecasted Graduates by Credential ----
f_graduates_t  <- f_graduates_t  %>% 
  select(YEAR, AGE_GROUP, GENDER, N_GRAD_FORECASTED)

avg_2_yr_credentials <- avg_2_yr_credentials  %>% ungroup() %>%
  complete(GENDER, AGE_GROUP, PSI_CREDENTIAL_CATEGORY, fill = list(YR_2_N=0,N=0,P=0)) %>% 
  select(AGE_GROUP, GENDER, PSI_CREDENTIAL_CATEGORY, P) 

f_graduates <- f_graduates_t  %>% 
  full_join(avg_2_yr_credentials, relationship = "many-to-many") %>%
  mutate(N_GRAD_FORECASTED = N_GRAD_FORECASTED*P) %>%
  select(-P)

f_graduates %>% 
  summarize(N=sum(N_GRAD_FORECASTED), .by  = c(PSI_CREDENTIAL_CATEGORY, YEAR, AGE_GROUP)) %>% View()


# ---- Projected Near Completers ----
# apply near completer ratios to the projected graduates to get projected near completers


# ---- Graduate Projections for Apprenticeship ----

  