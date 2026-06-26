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

# Refactor of 04-graduate-projections.R (heavier comments; dead lines commented out).
#
# WHAT THIS SCRIPT PRODUCES
#   Two tables written to the analyst's IDIR schema:
#     Graduate_Projections                     - forecast grads + near-completers + APPSO
#     Graduate_Projections_Include_Historical  - same, with the historical series appended
#   These feed the program (06) and occupation (07) projection scripts.
#
# THE METHOD (a population -> enrolment -> graduation cascade, by age x gender)
#   1. ENROLMENT RATE   P     = 100 * enrolments / population        (historical)
#   2. FORECAST RATE          = lm(P ~ YEAR) per age x gender; predict 2023-2027,
#                               then HOLD the 2027 rate flat through 2034
#   3. FORECAST ENROL   N     = RATE * POP * 0.01
#   4. GRAD RATE              = 2-yr-avg (2021/22) grads / enrolments
#   5. FORECAST GRADS   N     = N_ENROL * GRAD_RATE
#   6. SPLIT BY CRED          = apply the 2-yr-avg credential distribution
#   then layer on near-completers (ratios from 03) and APPSO apprenticeship grads.

library(tidyverse)
# library(RODBC)      # REMOVED: unused (script uses DBI / odbc:: namespaced calls)
library(config)
library(DBI)
# library(assertthat) # REMOVED: unused (no assert_that() calls in this script)

# ---- Configure schema / connection ----
# my_schema is the analyst's own IDIR schema (config.yml); never hardcode a schema.
# lan / db_schema were assigned but never used downstream -> removed.
# lan       <- config::get("lan")
my_schema <- config::get("myschema")
# db_schema <- config::get("dbschema")

# Reuse an existing connection if one is already open (orchestrated runs source
# scripts in sequence); otherwise open one. con_created tells Clean Up whether
# THIS script opened the connection, so we only disconnect what we created.
con_created <- !exists("con", where = .GlobalEnv)
if (con_created) {
  db_config <- config::get("decimal")
  con <- dbConnect(
    odbc::odbc(),
    Driver = db_config$driver,
    Server = db_config$server,
    Database = db_config$database,
    Trusted_Connection = "True" # Windows Integrated Authentication
  )
}

# ---- Required Tables ----
# Loaded into the environment by load-graduate-projections.R before this runs.
required_tables <- c(
  "population_projections", # BC Stats population projections (wide: one col per age band)
  "min_enrolments",         # historical enrolment counts by age x gender x year
  "credentials"             # historical graduate counts by credential x age x gender x year
)

missing <- required_tables[!sapply(required_tables, exists, where = .GlobalEnv)]
if (length(missing) > 0) {
  stop(paste(
    "The following required tables are missing from the environment:",
    paste(missing, collapse = ", ")
  ))
}

# na_vals was defined but never used in this script -> removed.
# na_vals <- c("", " ", "(Unspecified)", NA)

# ---- Tidy data for calculations ----
# population_projections arrives WIDE: one column per age band, named like
# "X20_TO_24". Reshape to long, keep only F/M (drops "Gender Diverse"), relabel
# gender to words, and strip the "X" / "_TO_" so AGE_GROUP matches the other
# tables (e.g. "20 to 24").
population_projections <- population_projections %>%
  select(-c(REGION, LOCAL_HEALTH_AREA, TOTAL)) %>%
  pivot_longer(
    cols = starts_with("X"),
    names_to = "AGE_GROUP",
    values_to = "POP"
  ) %>%
  filter(GENDER %in% c("F", "M")) %>%
  mutate(
    GENDER = case_when(
      GENDER == 'F' ~ 'Female',
      GENDER == 'M' ~ 'Male',
      TRUE ~ NA
    )
  ) %>%
  mutate(AGE_GROUP = gsub("X", "", AGE_GROUP)) %>%
  mutate(AGE_GROUP = gsub("_TO_", " to ", AGE_GROUP)) %>%
  select(-TYPE)

# min_enrolments: standardise column names and strip gender words out of the
# age-group label; take the first 4 chars of the school year ("2018/19" -> 2018).
min_enrolments <- min_enrolments %>%
  rename(
    "AGE_GROUP" = "Groups",
    "N" = "Expr1",
    "GENDER" = "PSI_GENDER",
    "YEAR" = "PSI_SCHOOL_YEAR"
  ) %>%
  mutate(
    AGE_GROUP = gsub("Female|Male|Gender Diverse Gender Diverse", "", AGE_GROUP)
  ) %>%
  mutate(YEAR = as.numeric(stringr::str_sub(YEAR, 1, 4))) %>%
  arrange(GENDER, AGE_GROUP, YEAR)

# credentials: standardise names, parse YEAR, restrict to the historical window
# used to estimate rates/distributions (2006-2022).
credentials <- credentials %>%
  rename(
    "AGE_GROUP" = "AgeGroup",
    "N" = "Count",
    "GENDER" = "psi_gender_cleaned",
    "YEAR" = "PSI_AWARD_SCHOOL_YEAR_DELAYED"
  ) %>%
  mutate(YEAR = as.numeric(stringr::str_sub(YEAR, 1, 4))) %>%
  select(-Expr1) %>%
  filter(YEAR >= 2006, YEAR <= 2022)

# ---- Forecasted Enrolments ----
## Enrolment Rate ----
# P = enrolments per 100 population. inner_join drops "Gender Diverse" (absent
# from population_projections, which only has F/M).
p_enrolments <- min_enrolments %>%
  inner_join(population_projections, by = join_by(GENDER, AGE_GROUP, YEAR)) %>%
  mutate(P = 100 * N / POP)

## Forecasted Enrolment Rate ----
# Fit a separate linear trend P ~ YEAR for EACH age x gender group, then predict
# 12 years: genuinely forecast 2023-2027 from the trend, then HOLD the 2027 value
# flat for 2028-2034 (rep(2027, 7)). Row names 2023-2034 label the forecast years.
f_enrolments <- p_enrolments |>
  split(
    list(p_enrolments$AGE_GROUP, p_enrolments$GENDER),
    drop = TRUE,
    sep = "_"
  ) |>
  map(\(df) lm(P ~ YEAR, data = df)) |>
  map(
    predict.lm,
    newdata = data.frame(
      YEAR = c(2023:2027, rep(2027, 7)),
      row.names = as.character(2023:2034)
    )
  )

## Forecasted Enrolments ----
# data.frame(f_enrolments) has one column per group, rows named by year; pull the
# years out (rn), pivot back to long, and recover AGE_GROUP / GENDER from the
# "AgeGroup_Gender" column name that split() created.
rn <- as.numeric(rownames(data.frame(f_enrolments)))
f_enrolments_t <- data.frame(f_enrolments) %>%
  mutate(YEAR = rn) %>%
  pivot_longer(cols = c(-YEAR), values_to = "RATE") %>%
  separate_wider_delim(
    cols = name,
    delim = "_",
    names = c("AGE_GROUP", "GENDER")
  ) %>%
  mutate(AGE_GROUP = gsub("\\.", " ", AGE_GROUP)) %>%
  mutate(AGE_GROUP = gsub("X", "", AGE_GROUP))

# Forecast enrolment counts = forecast rate * projected population / 100.
f_enrolments_t <- f_enrolments_t %>%
  inner_join(population_projections, by = join_by(YEAR, AGE_GROUP, GENDER)) %>%
  mutate(N_ENROL_FORECASTED = RATE * POP * .01)

# ---- Forecasted Graduates ----
## Graduation Rates (annual, as a percentage of enrolment) ----
# Total grads and total enrolments per age x gender x year (inner_join again
# drops Gender Diverse). P_GRADS_ENROL line left commented in the original.
annual_grad_count <- credentials %>%
  summarize(
    N_GRADS = sum(N, na.rm = TRUE),
    .by = c(GENDER, AGE_GROUP, YEAR)
  ) %>%
  inner_join(
    # removes Gender Diverse
    min_enrolments %>%
      summarize(
        N_ENROL = sum(N, na.rm = TRUE),
        .by = c(GENDER, AGE_GROUP, YEAR)
      ),
    by = join_by(GENDER, AGE_GROUP, YEAR)
  )
# mutate(P_GRADS_ENROL = 100 * N_GRADS / N_ENROL)

## Graduation Rate (2-yr average, as percentage of enrolment) ----
# Pool 2021+2022 grads and enrolments to get one stable rate per age x gender.
avg_2_yr_grad_rate <- annual_grad_count %>%
  filter(YEAR %in% 2021:2022) %>%
  summarise(GRAD_RATE = sum(N_GRADS) / sum(N_ENROL), .by = c(GENDER, AGE_GROUP))

# Forecast grads = forecast enrolments * 2-yr-avg grad rate.
f_graduates_t <- f_enrolments_t %>%
  inner_join(avg_2_yr_grad_rate, by = join_by(AGE_GROUP, GENDER)) %>%
  mutate(N_GRAD_FORECASTED = N_ENROL_FORECASTED * GRAD_RATE)

## Forecasted Graduates by Credential ----
f_graduates_t <- f_graduates_t %>%
  select(YEAR, AGE_GROUP, GENDER, N_GRAD_FORECASTED)

## 2-yr average distribution of graduates by credential ----
# Share of each age x gender's grads falling into each credential category
# (2021/22 pooled). complete() fills 0 for credential categories not observed in
# a given age x gender, so every combination has a proportion P.
avg_2_yr_credentials <- credentials %>%
  filter(YEAR %in% 2021:2022, GENDER != 'Gender Diverse') %>%
  summarise(
    YR_2_N = sum(N),
    .by = c(GENDER, AGE_GROUP, PSI_CREDENTIAL_CATEGORY)
  ) %>%
  group_by(GENDER, AGE_GROUP) %>%
  mutate(N = sum(YR_2_N), P = round(YR_2_N / N, 3)) %>%
  ungroup() %>%
  complete(
    GENDER,
    AGE_GROUP,
    PSI_CREDENTIAL_CATEGORY,
    fill = list(YR_2_N = 0, N = 0, P = 0)
  ) %>%
  select(AGE_GROUP, GENDER, PSI_CREDENTIAL_CATEGORY, P)

# Spread each forecast grad count across credential categories by their share P.
f_graduates <- f_graduates_t %>%
  full_join(avg_2_yr_credentials, relationship = "many-to-many") %>%
  mutate(N_GRAD_FORECASTED = N_GRAD_FORECASTED * P) %>%
  select(-P) %>%
  summarize(
    N = sum(N_GRAD_FORECASTED),
    .by = c(PSI_CREDENTIAL_CATEGORY, YEAR, AGE_GROUP, GENDER)
  )

# ---- Projected Near Completers (NC) ----
# Read the NC ratio table from 03. RATIO = near-completers per completer, by
# credential x age x gender. GENDER is stored coded (1 = Male) -> relabel.
T_DACSO_Near_Completers_RatioByGender <- dbReadTable(
  con,
  "T_DACSO_Near_Completers_RatioByGender"
) %>%
  janitor::clean_names("all_caps") %>%
  mutate(PSI_CREDENTIAL_CATEGORY = PRGM_CREDENTIAL_AWARDED_NAME) %>%
  select(PSI_CREDENTIAL_CATEGORY, AGE_GROUP, GENDER, RATIO) %>%
  mutate(GENDER = if_else(GENDER == 1, 'Male', 'Female'))

# NC ratios only exist for a broad "35 to 64" band. Expand them to the model's
# finer bands by mapping each of 35-44 / 45-54 / 55-64 to "35 to 64", joining the
# ratio, then keeping the fine band as AGE_GROUP.
T_DACSO_Near_Completers_RatioByGender <- f_graduates %>%
  distinct(AGE_GROUP) %>%
  rename("AGE_GROUP_RECODE" = "AGE_GROUP") %>%
  mutate(
    AGE_GROUP = if_else(
      AGE_GROUP_RECODE %in% c("35 to 44", "45 to 54", "55 to 64"),
      "35 to 64",
      AGE_GROUP_RECODE
    )
  ) %>%
  full_join(
    T_DACSO_Near_Completers_RatioByGender,
    relationship = "many-to-many"
  ) %>%
  select(-AGE_GROUP) %>%
  rename("AGE_GROUP" = "AGE_GROUP_RECODE")

# Projected near-completers = forecast grads * NC ratio. NOTE: this uses
# f_graduates BEFORE its PSSM_CRED label is added below; order matters.
f_graduates_nc <- f_graduates %>%
  inner_join(T_DACSO_Near_Completers_RatioByGender) %>%
  mutate(N = N * RATIO) %>%
  select(-RATIO)

# Map the wordy credential category to the model's PSSM_CRED code. Completers get
# a "1 - " prefix (degree-level creds BACH/DOCT/MAST/PDEG/GRCT have none).
# NOTE: this exact case_when is duplicated in the historical section (hf_grad_creds);
# a future cleanup could factor it into a helper, but it is left inline here so
# the refactor stays a faithful annotation pass.
f_graduates <- f_graduates %>%
  filter(!AGE_GROUP %in% c("15 to 16")) %>%
  mutate(
    PSSM_CRED = case_when(
      toupper(PSI_CREDENTIAL_CATEGORY) ==
        "ADVANCED CERTIFICATE" ~ "1 - ADCT OR ADIP",
      toupper(PSI_CREDENTIAL_CATEGORY) == "ASSOCIATE DEGREE" ~ "1 - ADGR OR UT",
      toupper(PSI_CREDENTIAL_CATEGORY) ==
        "ADVANCED DIPLOMA" ~ "1 - ADCT OR ADIP",
      toupper(PSI_CREDENTIAL_CATEGORY) == "BACHELORS DEGREE" ~ "BACH",
      toupper(PSI_CREDENTIAL_CATEGORY) == "CERTIFICATE" ~ "1 - CERT",
      toupper(PSI_CREDENTIAL_CATEGORY) == "DIPLOMA" ~ "1 - DIPL",
      toupper(PSI_CREDENTIAL_CATEGORY) == "DOCTORATE" ~ "DOCT",
      toupper(PSI_CREDENTIAL_CATEGORY) ==
        "GRADUATE CERTIFICATE" ~ "GRCT OR GRDP",
      toupper(PSI_CREDENTIAL_CATEGORY) == "GRADUATE DIPLOMA" ~ "GRCT OR GRDP",
      toupper(PSI_CREDENTIAL_CATEGORY) == "MASTERS DEGREE" ~ "MAST",
      toupper(PSI_CREDENTIAL_CATEGORY) == "NONE" ~ "INVALID",
      toupper(PSI_CREDENTIAL_CATEGORY) == "OTHER" ~ "",
      toupper(PSI_CREDENTIAL_CATEGORY) ==
        "POST-DEGREE CERTIFICATE" ~ "1 - PDCT OR PDDP",
      toupper(PSI_CREDENTIAL_CATEGORY) ==
        "POST-DEGREE DIPLOMA" ~ "1 - PDCT OR PDDP",
      toupper(PSI_CREDENTIAL_CATEGORY) == "FIRST PROFESSIONAL DEGREE" ~ "PDEG",
      toupper(PSI_CREDENTIAL_CATEGORY) == "SHORT CERTIFICATE" ~ "INVALID",
      toupper(PSI_CREDENTIAL_CATEGORY) ==
        "UNIVERSITY TRANSFER" ~ "1 - ADGR OR UT",
      TRUE ~ NA
    )
  )

# Same idea for near-completers, but with a "3 - " prefix (status 3). Only the
# credentials that can have near-completers are mapped; the rest -> "NA".
# NOTE: duplicated below as hf_nc (see comment above).
f_graduates_nc <- f_graduates_nc %>%
  filter(!AGE_GROUP %in% c("15 to 16")) %>%
  mutate(
    PSSM_CRED = case_when(
      toupper(PSI_CREDENTIAL_CATEGORY) == "ASSOCIATE DEGREE" ~ "3 - ADGR OR UT",
      toupper(PSI_CREDENTIAL_CATEGORY) ==
        "ADVANCED DIPLOMA" ~ "3 - ADCT OR ADIP",
      toupper(PSI_CREDENTIAL_CATEGORY) == "CERTIFICATE" ~ "3 - CERT",
      toupper(PSI_CREDENTIAL_CATEGORY) == "DIPLOMA" ~ "3 - DIPL",
      toupper(PSI_CREDENTIAL_CATEGORY) ==
        "POST-DEGREE CERTIFICATE" ~ "3 - PDCT OR PDDP",
      toupper(PSI_CREDENTIAL_CATEGORY) ==
        "POST-DEGREE DIPLOMA" ~ "3 - PDCT OR PDDP",
      toupper(PSI_CREDENTIAL_CATEGORY) ==
        "UNIVERSITY TRANSFER" ~ "3 - ADGR OR UT",
      TRUE ~ "NA"
    )
  )

# Stack completers + near-completers, total by PSSM_CRED x year x age, tag the
# survey, and format YEAR as a school year ("2023" -> "2023/2024").
f_graduates_agg <- f_graduates %>%
  rbind(f_graduates_nc) %>%
  group_by(PSSM_CRED, YEAR, AGE_GROUP) %>%
  summarise(GRADUATES = sum(N)) %>%
  mutate(SURVEY = 'Credential_Projections_Transp') %>%
  mutate(YEAR = paste0(as.character(YEAR), "/", as.character(YEAR + 1))) %>%
  filter(!AGE_GROUP %in% c('65 to 89', '15 to 16'))

# ---- Graduate Projections for Apprenticeship (APPSO) ----
# 2-yr average (2022/23) of apprenticeship grads, tagged SURVEY = 'APPSO'.
APPSO_Graduates <- dbGetQuery(con, "SELECT * FROM APPSO_Graduates")

appso_2_yr_avg <- APPSO_Graduates %>%
  mutate(YEAR = str_replace(SUBM_CD, "C_Outc", "20")) %>%
  rename("N" = "EXPR1", "PSSM_CRED" = "PSSM_CREDENTIAL") %>%
  summarize(N = sum(N, na.rm = TRUE), .by = c(YEAR, PSSM_CRED, AGE_GROUP)) %>%
  filter(YEAR %in% c('2022', '2023')) %>%
  summarize(
    GRADUATES = sum(N / 2, na.rm = TRUE),
    .by = c(PSSM_CRED, AGE_GROUP)
  ) %>%
  mutate(YEAR = "2023/2024") %>%
  mutate(SURVEY = 'APPSO') %>%
  mutate(PSI_CREDENTIAL_CATEGORY = "NA") %>%
  filter(!is.na(AGE_GROUP)) %>%
  filter(!AGE_GROUP %in% c('65 to 89', '15 to 16'))

# All forecast grad data: append APPSO and derive the un-prefixed PSSM_CREDENTIAL.
f_graduates_agg <- f_graduates_agg %>%
  rbind(appso_2_yr_avg) %>%
  mutate(PSSM_CREDENTIAL = gsub("(1 - )|(3 - )", "", PSSM_CRED))

# APPSO forward-fill to the horizon end is handled in 06, not here (kept as a note).
# f_graduates_agg <- f_graduates_agg %>%
#   ungroup() %>%
#   arrange(PSSM_CREDENTIAL, AGE_GROUP, YEAR) %>%
#   complete(PSSM_CREDENTIAL, AGE_GROUP, YEAR) %>%
#   group_by(PSSM_CREDENTIAL, AGE_GROUP) %>%
#   fill(GRADUATES)

# ---- Graduate Projections for Trades (TRD) ----
# TODO: add trades to Graduate Projections and project the same way as APPSO.
# TRD_Graduates is read but never used yet -> commented out to avoid an unused
# DB query. Uncomment once trades are integrated.
# TRD_Graduates <- dbGetQuery(con, "SELECT * FROM TRD_Graduates")

# ----------  Historical Outputs ----------
# "New work introduced last year" (author's note); used to back-test forecasts by
# rebuilding the same series for historical years and appending it.

# DEAD DIAGNOSTICS: the three objects below are computed but never used again and
# never written to the DB. Commented out to simplify. Uncomment if you need the
# population/enrolment/grad back-test comparisons interactively.
#
# historical_forecasted_enrolments <- tibble(
#   p_enrolments %>%
#     select(YEAR, AGE_GROUP, GENDER, N = N) %>%
#     mutate(TYPE = 'H. ENROLMENT') %>%
#     bind_rows(
#       f_enrolments_t %>%
#         select(YEAR, AGE_GROUP, GENDER, N = N_ENROL_FORECASTED) %>%
#         mutate(TYPE = 'F. ENROLMENT')
#     )
# )
#
# pop_projections_for_compare <- population_projections %>%
#   mutate(N = POP) %>%
#   select(YEAR, AGE_GROUP, GENDER, N) %>%
#   mutate(TYPE = 'POPULATION') %>%
#   filter(YEAR < 2035)
#
# historical_forecasted_grads <- annual_grad_count %>%
#   select(YEAR, AGE_GROUP, GENDER, N = N_GRADS) %>%
#   mutate(TYPE = 'H. GRADS') %>%
#   bind_rows(
#     f_graduates_t %>%
#       select(YEAR, AGE_GROUP, GENDER, N = N_GRAD_FORECASTED) %>%
#       mutate(TYPE = 'F. GRADS')
#   )

## HISTORICAL - combined GRAD CRED ----
# Historical grads-by-credential + forecast grads-by-credential. USED downstream.
historical_forecasted_grad_creds <-
  credentials %>%
  select(YEAR, AGE_GROUP, GENDER, PSI_CREDENTIAL_CATEGORY, N) %>%
  mutate(TYPE = 'H. GRADS by Cred') %>%
  bind_rows(
    f_graduates %>%
      select(YEAR, AGE_GROUP, GENDER, PSI_CREDENTIAL_CATEGORY, N) %>%
      mutate(TYPE = 'F. GRADS by Cred')
  )

# HISTORICAL - near-completers ----
# NC ratios with a per-year count (N_NC_STP). NC data only goes back to 2019, so
# historical comparisons should not extend earlier. GENDER coded (1 = Male).
T_DACSO_Near_Completers_RatioByGender_year <- dbReadTable(
  con,
  "T_DACSO_Near_Completers_RatioByGender_year"
) %>%
  janitor::clean_names("all_caps") %>%
  mutate(PSI_CREDENTIAL_CATEGORY = PRGM_CREDENTIAL_AWARDED_NAME) %>%
  select(YEAR, PSI_CREDENTIAL_CATEGORY, AGE_GROUP, GENDER, N_NC_STP) %>%
  mutate(GENDER = if_else(GENDER == 1, 'Male', 'Female'))

# Expand broad "35 to 64" NC band to fine bands (same recode as above).
T_DACSO_Near_Completers_RatioByGender_year <- f_gra// filepath: c:\Users\JDUAN\Downloads\project\r\bcgov\post-secondary-supply-model\R\04-graduate-projections-refactor.R
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

# Refactor of 04-graduate-projections.R (heavier comments; dead lines commented out).
#
# WHAT THIS SCRIPT PRODUCES
#   Two tables written to the analyst's IDIR schema:
#     Graduate_Projections                     - forecast grads + near-completers + APPSO
#     Graduate_Projections_Include_Historical  - same, with the historical series appended
#   These feed the program (06) and occupation (07) projection scripts.
#
# THE METHOD (a population -> enrolment -> graduation cascade, by age x gender)
#   1. ENROLMENT RATE   P     = 100 * enrolments / population        (historical)
#   2. FORECAST RATE          = lm(P ~ YEAR) per age x gender; predict 2023-2027,
#                               then HOLD the 2027 rate flat through 2034
#   3. FORECAST ENROL   N     = RATE * POP * 0.01
#   4. GRAD RATE              = 2-yr-avg (2021/22) grads / enrolments
#   5. FORECAST GRADS   N     = N_ENROL * GRAD_RATE
#   6. SPLIT BY CRED          = apply the 2-yr-avg credential distribution
#   then layer on near-completers (ratios from 03) and APPSO apprenticeship grads.

library(tidyverse)
# library(RODBC)      # REMOVED: unused (script uses DBI / odbc:: namespaced calls)
library(config)
library(DBI)
# library(assertthat) # REMOVED: unused (no assert_that() calls in this script)

# ---- Configure schema / connection ----
# my_schema is the analyst's own IDIR schema (config.yml); never hardcode a schema.
# lan / db_schema were assigned but never used downstream -> removed.
# lan       <- config::get("lan")
my_schema <- config::get("myschema")
# db_schema <- config::get("dbschema")

# Reuse an existing connection if one is already open (orchestrated runs source
# scripts in sequence); otherwise open one. con_created tells Clean Up whether
# THIS script opened the connection, so we only disconnect what we created.
con_created <- !exists("con", where = .GlobalEnv)
if (con_created) {
  db_config <- config::get("decimal")
  con <- dbConnect(
    odbc::odbc(),
    Driver = db_config$driver,
    Server = db_config$server,
    Database = db_config$database,
    Trusted_Connection = "True" # Windows Integrated Authentication
  )
}

# ---- Required Tables ----
# Loaded into the environment by load-graduate-projections.R before this runs.
required_tables <- c(
  "population_projections", # BC Stats population projections (wide: one col per age band)
  "min_enrolments",         # historical enrolment counts by age x gender x year
  "credentials"             # historical graduate counts by credential x age x gender x year
)

missing <- required_tables[!sapply(required_tables, exists, where = .GlobalEnv)]
if (length(missing) > 0) {
  stop(paste(
    "The following required tables are missing from the environment:",
    paste(missing, collapse = ", ")
  ))
}

# na_vals was defined but never used in this script -> removed.
# na_vals <- c("", " ", "(Unspecified)", NA)

# ---- Tidy data for calculations ----
# population_projections arrives WIDE: one column per age band, named like
# "X20_TO_24". Reshape to long, keep only F/M (drops "Gender Diverse"), relabel
# gender to words, and strip the "X" / "_TO_" so AGE_GROUP matches the other
# tables (e.g. "20 to 24").
population_projections <- population_projections %>%
  select(-c(REGION, LOCAL_HEALTH_AREA, TOTAL)) %>%
  pivot_longer(
    cols = starts_with("X"),
    names_to = "AGE_GROUP",
    values_to = "POP"
  ) %>%
  filter(GENDER %in% c("F", "M")) %>%
  mutate(
    GENDER = case_when(
      GENDER == 'F' ~ 'Female',
      GENDER == 'M' ~ 'Male',
      TRUE ~ NA
    )
  ) %>%
  mutate(AGE_GROUP = gsub("X", "", AGE_GROUP)) %>%
  mutate(AGE_GROUP = gsub("_TO_", " to ", AGE_GROUP)) %>%
  select(-TYPE)

# min_enrolments: standardise column names and strip gender words out of the
# age-group label; take the first 4 chars of the school year ("2018/19" -> 2018).
min_enrolments <- min_enrolments %>%
  rename(
    "AGE_GROUP" = "Groups",
    "N" = "Expr1",
    "GENDER" = "PSI_GENDER",
    "YEAR" = "PSI_SCHOOL_YEAR"
  ) %>%
  mutate(
    AGE_GROUP = gsub("Female|Male|Gender Diverse Gender Diverse", "", AGE_GROUP)
  ) %>%
  mutate(YEAR = as.numeric(stringr::str_sub(YEAR, 1, 4))) %>%
  arrange(GENDER, AGE_GROUP, YEAR)

# credentials: standardise names, parse YEAR, restrict to the historical window
# used to estimate rates/distributions (2006-2022).
credentials <- credentials %>%
  rename(
    "AGE_GROUP" = "AgeGroup",
    "N" = "Count",
    "GENDER" = "psi_gender_cleaned",
    "YEAR" = "PSI_AWARD_SCHOOL_YEAR_DELAYED"
  ) %>%
  mutate(YEAR = as.numeric(stringr::str_sub(YEAR, 1, 4))) %>%
  select(-Expr1) %>%
  filter(YEAR >= 2006, YEAR <= 2022)

# ---- Forecasted Enrolments ----
## Enrolment Rate ----
# P = enrolments per 100 population. inner_join drops "Gender Diverse" (absent
# from population_projections, which only has F/M).
p_enrolments <- min_enrolments %>%
  inner_join(population_projections, by = join_by(GENDER, AGE_GROUP, YEAR)) %>%
  mutate(P = 100 * N / POP)

## Forecasted Enrolment Rate ----
# Fit a separate linear trend P ~ YEAR for EACH age x gender group, then predict
# 12 years: genuinely forecast 2023-2027 from the trend, then HOLD the 2027 value
# flat for 2028-2034 (rep(2027, 7)). Row names 2023-2034 label the forecast years.
f_enrolments <- p_enrolments |>
  split(
    list(p_enrolments$AGE_GROUP, p_enrolments$GENDER),
    drop = TRUE,
    sep = "_"
  ) |>
  map(\(df) lm(P ~ YEAR, data = df)) |>
  map(
    predict.lm,
    newdata = data.frame(
      YEAR = c(2023:2027, rep(2027, 7)),
      row.names = as.character(2023:2034)
    )
  )

## Forecasted Enrolments ----
# data.frame(f_enrolments) has one column per group, rows named by year; pull the
# years out (rn), pivot back to long, and recover AGE_GROUP / GENDER from the
# "AgeGroup_Gender" column name that split() created.
rn <- as.numeric(rownames(data.frame(f_enrolments)))
f_enrolments_t <- data.frame(f_enrolments) %>%
  mutate(YEAR = rn) %>%
  pivot_longer(cols = c(-YEAR), values_to = "RATE") %>%
  separate_wider_delim(
    cols = name,
    delim = "_",
    names = c("AGE_GROUP", "GENDER")
  ) %>%
  mutate(AGE_GROUP = gsub("\\.", " ", AGE_GROUP)) %>%
  mutate(AGE_GROUP = gsub("X", "", AGE_GROUP))

# Forecast enrolment counts = forecast rate * projected population / 100.
f_enrolments_t <- f_enrolments_t %>%
  inner_join(population_projections, by = join_by(YEAR, AGE_GROUP, GENDER)) %>%
  mutate(N_ENROL_FORECASTED = RATE * POP * .01)

# ---- Forecasted Graduates ----
## Graduation Rates (annual, as a percentage of enrolment) ----
# Total grads and total enrolments per age x gender x year (inner_join again
# drops Gender Diverse). P_GRADS_ENROL line left commented in the original.
annual_grad_count <- credentials %>%
  summarize(
    N_GRADS = sum(N, na.rm = TRUE),
    .by = c(GENDER, AGE_GROUP, YEAR)
  ) %>%
  inner_join(
    # removes Gender Diverse
    min_enrolments %>%
      summarize(
        N_ENROL = sum(N, na.rm = TRUE),
        .by = c(GENDER, AGE_GROUP, YEAR)
      ),
    by = join_by(GENDER, AGE_GROUP, YEAR)
  )
# mutate(P_GRADS_ENROL = 100 * N_GRADS / N_ENROL)

## Graduation Rate (2-yr average, as percentage of enrolment) ----
# Pool 2021+2022 grads and enrolments to get one stable rate per age x gender.
avg_2_yr_grad_rate <- annual_grad_count %>%
  filter(YEAR %in% 2021:2022) %>%
  summarise(GRAD_RATE = sum(N_GRADS) / sum(N_ENROL), .by = c(GENDER, AGE_GROUP))

# Forecast grads = forecast enrolments * 2-yr-avg grad rate.
f_graduates_t <- f_enrolments_t %>%
  inner_join(avg_2_yr_grad_rate, by = join_by(AGE_GROUP, GENDER)) %>%
  mutate(N_GRAD_FORECASTED = N_ENROL_FORECASTED * GRAD_RATE)

## Forecasted Graduates by Credential ----
f_graduates_t <- f_graduates_t %>%
  select(YEAR, AGE_GROUP, GENDER, N_GRAD_FORECASTED)

## 2-yr average distribution of graduates by credential ----
# Share of each age x gender's grads falling into each credential category
# (2021/22 pooled). complete() fills 0 for credential categories not observed in
# a given age x gender, so every combination has a proportion P.
avg_2_yr_credentials <- credentials %>%
  filter(YEAR %in% 2021:2022, GENDER != 'Gender Diverse') %>%
  summarise(
    YR_2_N = sum(N),
    .by = c(GENDER, AGE_GROUP, PSI_CREDENTIAL_CATEGORY)
  ) %>%
  group_by(GENDER, AGE_GROUP) %>%
  mutate(N = sum(YR_2_N), P = round(YR_2_N / N, 3)) %>%
  ungroup() %>%
  complete(
    GENDER,
    AGE_GROUP,
    PSI_CREDENTIAL_CATEGORY,
    fill = list(YR_2_N = 0, N = 0, P = 0)
  ) %>%
  select(AGE_GROUP, GENDER, PSI_CREDENTIAL_CATEGORY, P)

# Spread each forecast grad count across credential categories by their share P.
f_graduates <- f_graduates_t %>%
  full_join(avg_2_yr_credentials, relationship = "many-to-many") %>%
  mutate(N_GRAD_FORECASTED = N_GRAD_FORECASTED * P) %>%
  select(-P) %>%
  summarize(
    N = sum(N_GRAD_FORECASTED),
    .by = c(PSI_CREDENTIAL_CATEGORY, YEAR, AGE_GROUP, GENDER)
  )

# ---- Projected Near Completers (NC) ----
# Read the NC ratio table from 03. RATIO = near-completers per completer, by
# credential x age x gender. GENDER is stored coded (1 = Male) -> relabel.
T_DACSO_Near_Completers_RatioByGender <- dbReadTable(
  con,
  "T_DACSO_Near_Completers_RatioByGender"
) %>%
  janitor::clean_names("all_caps") %>%
  mutate(PSI_CREDENTIAL_CATEGORY = PRGM_CREDENTIAL_AWARDED_NAME) %>%
  select(PSI_CREDENTIAL_CATEGORY, AGE_GROUP, GENDER, RATIO) %>%
  mutate(GENDER = if_else(GENDER == 1, 'Male', 'Female'))

# NC ratios only exist for a broad "35 to 64" band. Expand them to the model's
# finer bands by mapping each of 35-44 / 45-54 / 55-64 to "35 to 64", joining the
# ratio, then keeping the fine band as AGE_GROUP.
T_DACSO_Near_Completers_RatioByGender <- f_graduates %>%
  distinct(AGE_GROUP) %>%
  rename("AGE_GROUP_RECODE" = "AGE_GROUP") %>%
  mutate(
    AGE_GROUP = if_else(
      AGE_GROUP_RECODE %in% c("35 to 44", "45 to 54", "55 to 64"),
      "35 to 64",
      AGE_GROUP_RECODE
    )
  ) %>%
  full_join(
    T_DACSO_Near_Completers_RatioByGender,
    relationship = "many-to-many"
  ) %>%
  select(-AGE_GROUP) %>%
  rename("AGE_GROUP" = "AGE_GROUP_RECODE")

# Projected near-completers = forecast grads * NC ratio. NOTE: this uses
# f_graduates BEFORE its PSSM_CRED label is added below; order matters.
f_graduates_nc <- f_graduates %>%
  inner_join(T_DACSO_Near_Completers_RatioByGender) %>%
  mutate(N = N * RATIO) %>%
  select(-RATIO)

# Map the wordy credential category to the model's PSSM_CRED code. Completers get
# a "1 - " prefix (degree-level creds BACH/DOCT/MAST/PDEG/GRCT have none).
# NOTE: this exact case_when is duplicated in the historical section (hf_grad_creds);
# a future cleanup could factor it into a helper, but it is left inline here so
# the refactor stays a faithful annotation pass.
f_graduates <- f_graduates %>%
  filter(!AGE_GROUP %in% c("15 to 16")) %>%
  mutate(
    PSSM_CRED = case_when(
      toupper(PSI_CREDENTIAL_CATEGORY) ==
        "ADVANCED CERTIFICATE" ~ "1 - ADCT OR ADIP",
      toupper(PSI_CREDENTIAL_CATEGORY) == "ASSOCIATE DEGREE" ~ "1 - ADGR OR UT",
      toupper(PSI_CREDENTIAL_CATEGORY) ==
        "ADVANCED DIPLOMA" ~ "1 - ADCT OR ADIP",
      toupper(PSI_CREDENTIAL_CATEGORY) == "BACHELORS DEGREE" ~ "BACH",
      toupper(PSI_CREDENTIAL_CATEGORY) == "CERTIFICATE" ~ "1 - CERT",
      toupper(PSI_CREDENTIAL_CATEGORY) == "DIPLOMA" ~ "1 - DIPL",
      toupper(PSI_CREDENTIAL_CATEGORY) == "DOCTORATE" ~ "DOCT",
      toupper(PSI_CREDENTIAL_CATEGORY) ==
        "GRADUATE CERTIFICATE" ~ "GRCT OR GRDP",
      toupper(PSI_CREDENTIAL_CATEGORY) == "GRADUATE DIPLOMA" ~ "GRCT OR GRDP",
      toupper(PSI_CREDENTIAL_CATEGORY) == "MASTERS DEGREE" ~ "MAST",
      toupper(PSI_CREDENTIAL_CATEGORY) == "NONE" ~ "INVALID",
      toupper(PSI_CREDENTIAL_CATEGORY) == "OTHER" ~ "",
      toupper(PSI_CREDENTIAL_CATEGORY) ==
        "POST-DEGREE CERTIFICATE" ~ "1 - PDCT OR PDDP",
      toupper(PSI_CREDENTIAL_CATEGORY) ==
        "POST-DEGREE DIPLOMA" ~ "1 - PDCT OR PDDP",
      toupper(PSI_CREDENTIAL_CATEGORY) == "FIRST PROFESSIONAL DEGREE" ~ "PDEG",
      toupper(PSI_CREDENTIAL_CATEGORY) == "SHORT CERTIFICATE" ~ "INVALID",
      toupper(PSI_CREDENTIAL_CATEGORY) ==
        "UNIVERSITY TRANSFER" ~ "1 - ADGR OR UT",
      TRUE ~ NA
    )
  )

# Same idea for near-completers, but with a "3 - " prefix (status 3). Only the
# credentials that can have near-completers are mapped; the rest -> "NA".
# NOTE: duplicated below as hf_nc (see comment above).
f_graduates_nc <- f_graduates_nc %>%
  filter(!AGE_GROUP %in% c("15 to 16")) %>%
  mutate(
    PSSM_CRED = case_when(
      toupper(PSI_CREDENTIAL_CATEGORY) == "ASSOCIATE DEGREE" ~ "3 - ADGR OR UT",
      toupper(PSI_CREDENTIAL_CATEGORY) ==
        "ADVANCED DIPLOMA" ~ "3 - ADCT OR ADIP",
      toupper(PSI_CREDENTIAL_CATEGORY) == "CERTIFICATE" ~ "3 - CERT",
      toupper(PSI_CREDENTIAL_CATEGORY) == "DIPLOMA" ~ "3 - DIPL",
      toupper(PSI_CREDENTIAL_CATEGORY) ==
        "POST-DEGREE CERTIFICATE" ~ "3 - PDCT OR PDDP",
      toupper(PSI_CREDENTIAL_CATEGORY) ==
        "POST-DEGREE DIPLOMA" ~ "3 - PDCT OR PDDP",
      toupper(PSI_CREDENTIAL_CATEGORY) ==
        "UNIVERSITY TRANSFER" ~ "3 - ADGR OR UT",
      TRUE ~ "NA"
    )
  )

# Stack completers + near-completers, total by PSSM_CRED x year x age, tag the
# survey, and format YEAR as a school year ("2023" -> "2023/2024").
f_graduates_agg <- f_graduates %>%
  rbind(f_graduates_nc) %>%
  group_by(PSSM_CRED, YEAR, AGE_GROUP) %>%
  summarise(GRADUATES = sum(N)) %>%
  mutate(SURVEY = 'Credential_Projections_Transp') %>%
  mutate(YEAR = paste0(as.character(YEAR), "/", as.character(YEAR + 1))) %>%
  filter(!AGE_GROUP %in% c('65 to 89', '15 to 16'))

# ---- Graduate Projections for Apprenticeship (APPSO) ----
# 2-yr average (2022/23) of apprenticeship grads, tagged SURVEY = 'APPSO'.
APPSO_Graduates <- dbGetQuery(con, "SELECT * FROM APPSO_Graduates")

appso_2_yr_avg <- APPSO_Graduates %>%
  mutate(YEAR = str_replace(SUBM_CD, "C_Outc", "20")) %>%
  rename("N" = "EXPR1", "PSSM_CRED" = "PSSM_CREDENTIAL") %>%
  summarize(N = sum(N, na.rm = TRUE), .by = c(YEAR, PSSM_CRED, AGE_GROUP)) %>%
  filter(YEAR %in% c('2022', '2023')) %>%
  summarize(
    GRADUATES = sum(N / 2, na.rm = TRUE),
    .by = c(PSSM_CRED, AGE_GROUP)
  ) %>%
  mutate(YEAR = "2023/2024") %>%
  mutate(SURVEY = 'APPSO') %>%
  mutate(PSI_CREDENTIAL_CATEGORY = "NA") %>%
  filter(!is.na(AGE_GROUP)) %>%
  filter(!AGE_GROUP %in% c('65 to 89', '15 to 16'))

# All forecast grad data: append APPSO and derive the un-prefixed PSSM_CREDENTIAL.
f_graduates_agg <- f_graduates_agg %>%
  rbind(appso_2_yr_avg) %>%
  mutate(PSSM_CREDENTIAL = gsub("(1 - )|(3 - )", "", PSSM_CRED))

# APPSO forward-fill to the horizon end is handled in 06, not here (kept as a note).
# f_graduates_agg <- f_graduates_agg %>%
#   ungroup() %>%
#   arrange(PSSM_CREDENTIAL, AGE_GROUP, YEAR) %>%
#   complete(PSSM_CREDENTIAL, AGE_GROUP, YEAR) %>%
#   group_by(PSSM_CREDENTIAL, AGE_GROUP) %>%
#   fill(GRADUATES)

# ---- Graduate Projections for Trades (TRD) ----
# TODO: add trades to Graduate Projections and project the same way as APPSO.
# TRD_Graduates is read but never used yet -> commented out to avoid an unused
# DB query. Uncomment once trades are integrated.
# TRD_Graduates <- dbGetQuery(con, "SELECT * FROM TRD_Graduates")

# ----------  Historical Outputs ----------
# "New work introduced last year" (author's note); used to back-test forecasts by
# rebuilding the same series for historical years and appending it.

# DEAD DIAGNOSTICS: the three objects below are computed but never used again and
# never written to the DB. Commented out to simplify. Uncomment if you need the
# population/enrolment/grad back-test comparisons interactively.
#
# historical_forecasted_enrolments <- tibble(
#   p_enrolments %>%
#     select(YEAR, AGE_GROUP, GENDER, N = N) %>%
#     mutate(TYPE = 'H. ENROLMENT') %>%
#     bind_rows(
#       f_enrolments_t %>%
#         select(YEAR, AGE_GROUP, GENDER, N = N_ENROL_FORECASTED) %>%
#         mutate(TYPE = 'F. ENROLMENT')
#     )
# )
#
# pop_projections_for_compare <- population_projections %>%
#   mutate(N = POP) %>%
#   select(YEAR, AGE_GROUP, GENDER, N) %>%
#   mutate(TYPE = 'POPULATION') %>%
#   filter(YEAR < 2035)
#
# historical_forecasted_grads <- annual_grad_count %>%
#   select(YEAR, AGE_GROUP, GENDER, N = N_GRADS) %>%
#   mutate(TYPE = 'H. GRADS') %>%
#   bind_rows(
#     f_graduates_t %>%
#       select(YEAR, AGE_GROUP, GENDER, N = N_GRAD_FORECASTED) %>%
#       mutate(TYPE = 'F. GRADS')
#   )

## HISTORICAL - combined GRAD CRED ----
# Historical grads-by-credential + forecast grads-by-credential. USED downstream.
historical_forecasted_grad_creds <-
  credentials %>%
  select(YEAR, AGE_GROUP, GENDER, PSI_CREDENTIAL_CATEGORY, N) %>%
  mutate(TYPE = 'H. GRADS by Cred') %>%
  bind_rows(
    f_graduates %>%
      select(YEAR, AGE_GROUP, GENDER, PSI_CREDENTIAL_CATEGORY, N) %>%
      mutate(TYPE = 'F. GRADS by Cred')
  )

# HISTORICAL - near-completers ----
# NC ratios with a per-year count (N_NC_STP). NC data only goes back to 2019, so
# historical comparisons should not extend earlier. GENDER coded (1 = Male).
T_DACSO_Near_Completers_RatioByGender_year <- dbReadTable(
  con,
  "T_DACSO_Near_Completers_RatioByGender_year"
) %>%
  janitor::clean_names("all_caps") %>%
  mutate(PSI_CREDENTIAL_CATEGORY = PRGM_CREDENTIAL_AWARDED_NAME) %>%
  select(YEAR, PSI_CREDENTIAL_CATEGORY, AGE_GROUP, GENDER, N_NC_STP) %>%
  mutate(GENDER = if_else(GENDER == 1, 'Male', 'Female'))



# Expand broad "35 to 64" NC band to fine bands (same recode as the forecast one).
T_DACSO_Near_Completers_RatioByGender_year <- f_graduates %>%
  distinct(AGE_GROUP) %>%
  rename("AGE_GROUP_RECODE" = "AGE_GROUP") %>%
  mutate(
    AGE_GROUP = if_else(
      AGE_GROUP_RECODE %in% c("35 to 44", "45 to 54", "55 to 64"),
      "35 to 64",
      AGE_GROUP_RECODE
    )
  ) %>%
  full_join(
    T_DACSO_Near_Completers_RatioByGender_year,
    relationship = "many-to-many"
  ) %>%
  select(-AGE_GROUP) %>%
  rename("AGE_GROUP" = "AGE_GROUP_RECODE")

## HISTORICAL - completers by credential, labelled ----
# Apply the SAME "1 - " completer mapping used for the forecast (f_graduates).
# historical_forecasted_grad_creds already holds BOTH historical and forecast
# grads-by-credential, so labelling + aggregating here yields the completer half
# of the include-historical output in one pass.
# NOTE: case_when duplicated from f_graduates above (faithful annotation pass).
hf_grad_creds <- historical_forecasted_grad_creds %>%
  filter(!AGE_GROUP %in% c("15 to 16")) %>%
  mutate(
    PSSM_CRED = case_when(
      toupper(PSI_CREDENTIAL_CATEGORY) ==
        "ADVANCED CERTIFICATE" ~ "1 - ADCT OR ADIP",
      toupper(PSI_CREDENTIAL_CATEGORY) == "ASSOCIATE DEGREE" ~ "1 - ADGR OR UT",
      toupper(PSI_CREDENTIAL_CATEGORY) ==
        "ADVANCED DIPLOMA" ~ "1 - ADCT OR ADIP",
      toupper(PSI_CREDENTIAL_CATEGORY) == "BACHELORS DEGREE" ~ "BACH",
      toupper(PSI_CREDENTIAL_CATEGORY) == "CERTIFICATE" ~ "1 - CERT",
      toupper(PSI_CREDENTIAL_CATEGORY) == "DIPLOMA" ~ "1 - DIPL",
      toupper(PSI_CREDENTIAL_CATEGORY) == "DOCTORATE" ~ "DOCT",
      toupper(PSI_CREDENTIAL_CATEGORY) ==
        "GRADUATE CERTIFICATE" ~ "GRCT OR GRDP",
      toupper(PSI_CREDENTIAL_CATEGORY) == "GRADUATE DIPLOMA" ~ "GRCT OR GRDP",
      toupper(PSI_CREDENTIAL_CATEGORY) == "MASTERS DEGREE" ~ "MAST",
      toupper(PSI_CREDENTIAL_CATEGORY) == "NONE" ~ "INVALID",
      toupper(PSI_CREDENTIAL_CATEGORY) == "OTHER" ~ "",
      toupper(PSI_CREDENTIAL_CATEGORY) ==
        "POST-DEGREE CERTIFICATE" ~ "1 - PDCT OR PDDP",
      toupper(PSI_CREDENTIAL_CATEGORY) ==
        "POST-DEGREE DIPLOMA" ~ "1 - PDCT OR PDDP",
      toupper(PSI_CREDENTIAL_CATEGORY) == "FIRST PROFESSIONAL DEGREE" ~ "PDEG",
      toupper(PSI_CREDENTIAL_CATEGORY) == "SHORT CERTIFICATE" ~ "INVALID",
      toupper(PSI_CREDENTIAL_CATEGORY) ==
        "UNIVERSITY TRANSFER" ~ "1 - ADGR OR UT",
      TRUE ~ NA
    )
  )

## HISTORICAL - near-completers by credential, labelled ----
# Historical NC counts come straight from N_NC_STP (per-year actuals, back to
# 2019 only); forecast NC come from f_graduates_nc. Stack them, then apply the
# SAME "3 - " near-completer mapping used above.
# NOTE: case_when duplicated from f_graduates_nc above.
hf_nc <- T_DACSO_Near_Completers_RatioByGender_year %>%
  rename(N = N_NC_STP) %>%
  select(YEAR, AGE_GROUP, GENDER, PSI_CREDENTIAL_CATEGORY, N) %>%
  mutate(TYPE = 'H. NC by Cred') %>%
  bind_rows(
    f_graduates_nc %>%
      select(YEAR, AGE_GROUP, GENDER, PSI_CREDENTIAL_CATEGORY, N) %>%
      mutate(TYPE = 'F. NC by Cred')
  ) %>%
  filter(!AGE_GROUP %in% c("15 to 16")) %>%
  mutate(
    PSSM_CRED = case_when(
      toupper(PSI_CREDENTIAL_CATEGORY) == "ASSOCIATE DEGREE" ~ "3 - ADGR OR UT",
      toupper(PSI_CREDENTIAL_CATEGORY) ==
        "ADVANCED DIPLOMA" ~ "3 - ADCT OR ADIP",
      toupper(PSI_CREDENTIAL_CATEGORY) == "CERTIFICATE" ~ "3 - CERT",
      toupper(PSI_CREDENTIAL_CATEGORY) == "DIPLOMA" ~ "3 - DIPL",
      toupper(PSI_CREDENTIAL_CATEGORY) ==
        "POST-DEGREE CERTIFICATE" ~ "3 - PDCT OR PDDP",
      toupper(PSI_CREDENTIAL_CATEGORY) ==
        "POST-DEGREE DIPLOMA" ~ "3 - PDCT OR PDDP",
      toupper(PSI_CREDENTIAL_CATEGORY) ==
        "UNIVERSITY TRANSFER" ~ "3 - ADGR OR UT",
      TRUE ~ "NA"
    )
  )

## HISTORICAL - combined series, aggregated to the output grain ----
# Stack completers + near-completers, total to PSSM_CRED x YEAR x AGE_GROUP, and
# format YEAR as a school year to match the forecast aggregate (f_graduates_agg).
historical_series <- hf_grad_creds %>%
  bind_rows(hf_nc) %>%
  group_by(PSSM_CRED, YEAR, AGE_GROUP) %>%
  summarise(GRADUATES = sum(N), .groups = "drop") %>%
  mutate(SURVEY = 'Credential_Projections_Transp') %>%
  mutate(YEAR = paste0(as.character(YEAR), "/", as.character(YEAR + 1))) %>%
  filter(!AGE_GROUP %in% c('65 to 89', '15 to 16')) %>%
  mutate(PSSM_CREDENTIAL = gsub("(1 - )|(3 - )", "", PSSM_CRED))

# ---- Final output tables ----
# Graduate_Projections: the forward-looking forecast (grads + NCs + APPSO) built
# above in f_graduates_agg. This is the table 06/07 consume.
Graduate_Projections <- f_graduates_agg %>%
  ungroup() %>%
  select(SURVEY, PSSM_CRED, PSSM_CREDENTIAL, AGE_GROUP, YEAR, GRADUATES)

# Graduate_Projections_Include_Historical: the same forecast with the historical
# series appended, for back-testing.
Graduate_Projections_Include_Historical <- Graduate_Projections %>%
  bind_rows(
    historical_series %>%
      select(SURVEY, PSSM_CRED, PSSM_CREDENTIAL, AGE_GROUP, YEAR, GRADUATES)
  )

# ---- Write outputs to the analyst's IDIR schema ----
# Helper writes <table>_r into my_schema (glue interpolation; .GlobalEnv index
# avoids the masked-base::get() pitfall). Matches the other pipeline scripts.
write_table_to_db <- function(table_name, schema, con) {
  db_name <- glue::glue("{table_name}_r")
  dbWriteTable(
    con,
    SQL(glue::glue('"{schema}"."{db_name}"')),
    .GlobalEnv[[table_name]],
    overwrite = TRUE
  )
  invisible(table_name)
}

tables_to_keep <- c(
  "Graduate_Projections",
  "Graduate_Projections_Include_Historical"
)

walk(tables_to_keep, write_table_to_db, schema = my_schema, con = con)

# ---- Clean Up ----
# Only disconnect if THIS script opened the connection; always release memory.
if (con_created) {
  dbDisconnect(con)
}
gc()