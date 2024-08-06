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
source(glue::glue("{lan}/development/sql/gh-source/01b-credential-analysis/01b-credential-analysis.R"))

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
  select(-Expr1)

# ---- Create distributions for forecasting ----
p_enrolments <- min_enrolments %>% 
  left_join(population_projections, by = join_by(GENDER, AGE_GROUP, YEAR)) %>%
  mutate(P = 100*N/POP)

# which years to average on?...TBD
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

# compute distribution of credential within each year, for each gender and age group
p_credentials <- credentials %>% 
  group_by(GENDER, AGE_GROUP, YEAR) %>% 
  mutate(100*N/sum(N))

# compute 2-yr average distribution of credential within each year, for each gender and age group
yrs <- 2017:2018
avg_2_yr_credentials <- credentials %>% 
  group_by(GENDER, AGE_GROUP, YEAR) %>% 
  mutate(Ttl = sum(N)) %>% 
  filter(YEAR %in% yrs) %>%
  ungroup() %>%
  group_by(GENDER, AGE_GROUP, PSI_CREDENTIAL_CATEGORY) %>%
  summarise(AVG_N = sum(N)/sum(Ttl))

# graduates as a percentage of enrolment for each age group

p_grads_enrol <- credentials %>% 
  summarize(N_GRADS = sum(N, na.rm = TRUE), .by = c(GENDER, AGE_GROUP, YEAR)) %>%
inner_join(
  min_enrolments %>% 
    summarize(N_ENROL = sum(N, na.rm = TRUE), .by = c(GENDER, AGE_GROUP, YEAR)), 
  by = join_by(GENDER, AGE_GROUP, YEAR)) %>%
mutate(P_GRADS_ENROL = 100*N_GRADS/N_ENROL)

# are we using the last 2 years each time?
avg_2_yr_grads_enrol <- p_grads_enrol %>% 
  filter(YEAR %in% 2017:2018) %>% 
  summarise(GRAD_RATE = 100*sum(N_GRADS)/sum(N_ENROL), 
            .by  = c(GENDER, AGE_GROUP))

# TO DO: here down

# ---- Projected Graduates ----

# create projected enrolments but for all the variable combinations
df <- p_enrolments %>% 
  filter(GENDER == 'F', AGE_GROUP == "25 to 29")
fit <- lm(P~YEAR, df)
b0 <- coef(fit)[1]
b1 <- coef(fit)[2]
prediction <- c(b0 + b1*(2019:2023), (b0 + b1*(rep(2023,5))))
prediction

# apply 2 year average graduation rate to projected enrollments

# ---- Projected Near Completers ----
# apply near completer ratios to the projected graduates to get projected near completers


# ---- Graduate Projections for Apprenticeship ----

  