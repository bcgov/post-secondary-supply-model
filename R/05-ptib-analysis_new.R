# ******************************************************************************
# Private Training Institutions Branch (PTIB)
#
# Required Tables
#   PTIB_Credentials
#   T_PSSM_Credential_Grouping
#   INFOWARE_L_CIP_6DIGITS_CIP2016
#   Graduate_Projections
#   Cohort_Program_Distributions_Projected
#   Cohort_Program_Distributions_Static
#   T_PTIB_Y1_to_Y10
#
# Part 1: Clean PTIB data
# * Update age groups, CIPs
# * Add and update exclude column
#
# Part 2: Domestic graduates
#
# Part 3: Cohort distributions
#
# ******************************************************************************

library(RODBC)
library(arrow)
library(tidyverse)
library(odbc)
library(DBI)

# Setup ----
## ---- Configure LAN Paths and DB Connection -----
lan <- config::get("lan")
source("sql/05-ptib-analysis/05-private-training-institutions.R")


## ---- Connect to Decimal ----
config <- config::get("decimal")
db_schema <- config::get("dbschema")
my_schema <- config::get("myschema")

con <- dbConnect(
  odbc(),
  Driver = config$driver,
  Server = config$server,
  Database = config$database,
  Trusted_Connection = "True"
)

# ---- Data Requirements and SQL Definitons ----
# PR Notes for this section:

# lookups from LAN
pssm_cred_grps <- read_csv(
  (glue::glue(
    "{lan}\\development\\csv\\gh-source\\lookups\\05\\T_PSSM_Credential_Grouping.csv"
  ))
)
names(pssm_cred_grps) <- toupper(names(pssm_cred_grps))

T_PTIB_Y1_to_Y10 <- read_csv(
  (glue::glue(
    "{lan}\\development\\csv\\gh-source\\lookups\\05\\T_PTIB_Y1_to_Y10.csv"
  ))
)

infoware <- read_csv(
  (glue::glue(
    "{lan}\\development\\csv\\gh-source\\lookups\\05\\INFOWARE_L_CIP_6DIGITS_CIP2016.csv"
  ))
)

# PTIB data
ptib_initial <- read_csv(
  glue::glue(
    "{lan}\\development\\csv\\gh-source\\testing\\05\\T_Private_Institutions_Credentials_Imported_2021-03.csv"
  ),
  col_types = "dccccccddd"
)

names(ptib_initial) <- c(
  "year",
  "credential",
  "cip",
  "age_group",
  "immigration_status",
  "sum_of_graduates",
  "sum_of_enrolments",
  "sum_of_total_enrolments"
)
raw_ptib_data <- ptib_initial

dbWriteTable(
  con,
  SQL(glue::glue(
    '"{my_schema}"."T_Private_Institutions_Credentials_Raw"'
  )),
  ptib_initial,
  overwrite = TRUE
)

# other tables should be in the R environment from earlier analysis
grad_proj <- dbReadTable(
  con,
  SQL(glue::glue('"{db_schema}"."Graduate_Projections"'))
)

cpd_proj <- dbReadTable(
  con,
  SQL(glue::glue('"{db_schema}"."Cohort_Program_Distributions_Projected"'))
)

cpd_static <- dbReadTable(
  con,
  SQL(glue::glue('"{db_schema}"."Cohort_Program_Distributions_Static"'))
)

# ---- Check Required Tables etc. ----

# required tables in decimal or R environment (location TBD)
required_tables <- c(
  'T_PTIB_Y1_to_Y10',
  'cpd_proj"',
  'cpd_static',
  'INFOWARE_L_CIP_6DIGITS_CIP2016',
  'grad_proj',
  'ptib_initial',
  'pssm_cred_grps'
)

missing <- required_tables[!sapply(required_tables, exists, where = .GlobalEnv)]

if (length(missing) > 0) {
  stop(paste(
    "The following required tables are missing from the environment:",
    paste(missing, collapse = ", ")
  ))
}

na_vals = c("", " ", "(Unspecified)", NA)

# Part 1 ----
# NOTE: Each step will be the SQL query first, followed up by R script version equivalents
### note to self, write final datasets to decimal if R script versions are eventually used instead

## ---- Add PSSM_Credential to PTIB data ----
T_Private_Institutions_Credentials <- pssm_cred_grps %>%
  select("PRGM_CREDENTIAL_AWARDED_NAME", "PSSM_CREDENTIAL") %>%
  filter(!is.na(PSSM_CREDENTIAL)) %>%
  inner_join(
    raw_ptib_data %>% filter(credential != "None"),
    by = c("PRGM_CREDENTIAL_AWARDED_NAME" = "credential")
  ) %>%
  select(
    intYear = year,
    Credential = PSSM_CREDENTIAL,
    LCIP_CD = cip,
    Age_Group = age_group,
    Immigration_Status = immigration_status,
    Graduates = sum_of_graduates,
    Enrolled_Not_Graduated = sum_of_enrolments,
    Enrolment = sum_of_total_enrolments
  ) |>
  mutate(
    Graduates = as.numeric(Graduates),
    Enrolled_Not_Graduated = as.numeric(Enrolled_Not_Graduated),
    Enrolment = as.numeric(Enrolment)
  )


## ---- Check CIP length ----
# want zero rows
T_Private_Institutions_Credentials %>%
  mutate(Expr1 = str_count(LCIP_CD)) %>%
  filter(Expr1 < 7) |>
  select(LCIP_CD, Expr1)

## ---- Remove periods from CIPs ----
T_Private_Institutions_Credentials <- T_Private_Institutions_Credentials %>%
  mutate(
    LCIP_CD = sapply(LCIP_CD, function(x) {
      parts <- str_split(x, "\\.", simplify = TRUE)
      prefix <- str_pad(parts[1], width = 2, side = "left", pad = "0")
      suffix <- str_pad(parts[2], width = 4, side = "right", pad = "0")
      return(paste0(prefix, ".", suffix))
    })
  ) |>
  mutate(LCIP_CD = str_replace_all(LCIP_CD, "\\.", ""))

T_Private_Institutions_Credentials %>%
  mutate(Expr1 = str_count(LCIP_CD)) %>%
  filter(Expr1 < 6) |>
  select(LCIP_CD, Expr1)


## ---- Check CIPs against infoware 6digit CIPs ----
### I think the SQL versions don't account for some CIPS with leading or trailing 0's
## R version handles this

# want zero rows
T_Private_Institutions_Credentials %>%
  select(LCIP_CD) %>%
  left_join(
    infoware %>% mutate(exists = "yes") %>% select(LCIP_CD, exists),
    by = c("LCIP_CD" = "LCIP_CD")
  ) %>%
  filter(is.na(exists))


## ---- Update Exclude column ----
# Excluded not for credit and ESL programs and unclassified 99.9999 manually with “Exclude=1”
T_Private_Institutions_Credentials <- T_Private_Institutions_Credentials %>%
  left_join(
    infoware %>% select(LCIP_CD, LCIP_NAME),
    by = "LCIP_CD"
  ) %>%
  mutate(
    Exclude = case_when(
      LCIP_NAME == "English as a second language" ~ "1",
      str_detect(LCIP_NAME, "(?i)not for credit") ~ "1", # Case-insensitive LIKE
      LCIP_CD == "99999" ~ "1",
      TRUE ~ NA_character_
    )
  ) %>%
  select(-LCIP_NAME)


## ---- Update age groups ----
T_Private_Institutions_Credentials <- T_Private_Institutions_Credentials %>%
  mutate(Age_Group = str_replace_all(Age_Group, "-", " to "))

## ---- Fix immigration status ----
# make decision on how to recode (blank, Unknown or NA) - leaving for this run
T_Private_Institutions_Credentials %>%
  count(Immigration_Status)

## ---- Copy to Clean table ----
T_Private_Institutions_Credentials_Clean <- T_Private_Institutions_Credentials

## ---- Age averages ----
## not sure if this will be necessary if only one year in data???
dbGetQuery(
  con,
  "ALTER TABLE T_Private_Institutions_Credentials
                ALTER COLUMN intYear VARCHAR(255);"
)

# check relevant years to update queries below
tbl(decimal_con, "T_Private_Institutions_Credentials") %>%
  collect() %>%
  count(intYear)

## !! update DATA years in below queries
dbExecute(decimal_con, qry_Private_Credentials_00g_Avg)

avg_summary <- T_Private_Institutions_Credentials_Clean %>%
  filter(is.na(Exclude)) %>%
  group_by(Credential, LCIP_CD, Age_Group, Immigration_Status, Exclude) %>%
  summarise(
    Enrolment = sum(Enrolment, na.rm = TRUE) / 2,
    Enrolled_Not_Graduated = sum(Enrolled_Not_Graduated, na.rm = TRUE) / 2,
    Graduates = sum(Graduates, na.rm = TRUE) / 2,
    .groups = "drop"
  ) %>%
  mutate(intYear = "Avg 2021 & 2022") %>%
  select(
    intYear,
    Credential,
    LCIP_CD,
    Age_Group,
    Immigration_Status,
    Enrolment,
    Enrolled_Not_Graduated,
    Graduates,
    Exclude
  )

T_Private_Institutions_Credentials <- bind_rows(
  T_Private_Institutions_Credentials |> mutate(intYear = as.character(intYear)),
  avg_summary
)

# Part 2 ----
## STOP !!! Update model year in queries ----

## ---- Count domestic grads ----
qry_Private_Credentials_01a_Domestic <- T_Private_Institutions_Credentials %>%
  filter(is.na(Exclude) & !is.na(Graduates)) %>%
  filter(Credential == "CERT" | Credential == "DIPL") %>%
  mutate(Year = "2023/2024") %>%
  select(
    Year,
    Credential,
    LCIP_CD,
    Age_Group,
    Graduates,
    Immigration_Status
  ) %>%
  mutate(
    Grad_Val = case_when(Immigration_Status == "Domestic" ~ Graduates, TRUE ~ 0)
  ) %>%
  select(-Graduates, -Immigration_Status) %>%
  group_by(Year, Credential, LCIP_CD, Age_Group) %>%
  summarize(Domestic = sum(Grad_Val))


## ---- Count domestic and international grads ----
qry_Private_Credentials_01b_Domestic_International <- T_Private_Institutions_Credentials %>%
  filter(is.na(Exclude) & !is.na(Graduates)) %>%
  filter(
    Immigration_Status == "Domestic" |
      Immigration_Status == "International" |
      Immigration_Status == "#N/A"
  ) %>% # what about (blank) if it existed???
  filter(Credential == "CERT" | Credential == "DIPL") %>%
  mutate(Year = "2023/2024") %>%
  select(Year, Credential, LCIP_CD, Age_Group, Graduates) %>%
  group_by(Year, Credential, LCIP_CD, Age_Group) %>%
  summarize(Domestic_International = sum(Graduates))

## ---- Compute percent of domestic and international grads that are domestic ----
qry_Private_Credentials_01c_Percent_Domestic <- qry_Private_Credentials_01a_Domestic %>%
  inner_join(
    qry_Private_Credentials_01b_Domestic_International,
    by = c("Age_Group", "LCIP_CD", "Credential", "Year")
  ) %>%
  mutate(
    Percent_Domestic = ifelse(
      Domestic == 0,
      0,
      Domestic / Domestic_International
    )
  )

## ---- Compute unknown or blank immigration status ----
## computes Blank/Unknown immigration status records to include as domestic grads;
qry_Private_Credentials_01d_Grads_Blank <- T_Private_Institutions_Credentials %>%
  filter(
    Immigration_Status == "(blank)" |
      Immigration_Status == "Unknown" & is.na(Exclude)
  ) %>%
  inner_join(
    qry_Private_Credentials_01c_Percent_Domestic,
    by = c("Age_Group", "LCIP_CD", "Credential")
  ) %>%
  mutate(Graduates_Blank = Graduates * Percent_Domestic) %>%
  select(Year, Credential, LCIP_CD, Age_Group, Graduates_Blank)


## ---- Join domestic and blank ----
qry_Private_Credentials_01e_Grads_Union <- qry_Private_Credentials_01a_Domestic %>%
  rbind(
    qry_Private_Credentials_01d_Grads_Blank %>%
      rename(Domestic = Graduates_Blank)
  )

## ---- Sum of union query ----
dbGetQuery(con, qry_Private_Credentials_01f_Grads)

qry_Private_Credentials_01f_Grads <- qry_Private_Credentials_01e_Grads_Union %>%
  group_by(Year, Credential, LCIP_CD, Age_Group) %>%
  summarize(Grads = sum(Domestic))

qry_Private_Credentials_01f_Grads <- as.data.frame(
  qry_Private_Credentials_01f_Grads
)

## ---- Summarize the Grads by Credential/Age ----
dbGetQuery(con, qry_Private_Credentials_05i_Grads)

qry_Private_Credentials_05i_Grads <- qry_Private_Credentials_01f_Grads %>%
  group_by(Year, Credential, Age_Group) %>%
  summarize(SumOfGrads = sum(Grads))

qry_Private_Credentials_05i_Grads <- as.data.frame(
  qry_Private_Credentials_05i_Grads
)

# note to self, the above is different between R and SQL for one value, but at the 12th digit
#[4] 709.1449275362319  - 709.1449275362320

## ---- Delete PTIB rows from Graduate_Projections ----
# dbExecute(con, qry_Private_Credentials_05i0_Grads_by_Year_Delete)
#
# Graduate_Projections <- dbReadTable(con, SQL(glue::glue('"{my_schema}"."Graduate_Projections"')))
#
# Graduate_Projections <- Graduate_Projections %>%
#   filter(Survey!="PTIB")

## ---- Update Graduate_Projections ----
## adds grads for all years to Graduate_Projections
dbGetQuery(con, qry_Private_Credentials_05i1_Grads_by_Year)

T_PTIB_Y1_to_Y10 <- dbReadTable(
  con,
  SQL(glue::glue('"{my_schema}"."T_PTIB_Y1_to_Y10"'))
)

Graduate_Projections_PTIB <- qry_Private_Credentials_05i_Grads %>%
  inner_join(
    T_PTIB_Y1_to_Y10,
    by = c("Year" = "Y1"),
    relationship = "many-to-many"
  ) %>%
  mutate(Survey = "PTIB") %>%
  #mutate(PSSM_Credential = NA) %>%
  mutate(PSSM_CRED = paste0("P - ", Credential)) %>%
  select(-Credential) %>%
  select(
    Survey,
    PSSM_CRED,
    Age_Group,
    Year = Y1_TO_Y10,
    Graduates = SumOfGrads
  ) %>%
  arrange(PSSM_CRED, Age_Group)

# Graduate_Projections <- Graduate_Projections %>%
#   rbind(Graduate_Projections_PTIB)

## ---- Delete excess age groups ----
## ADDED 2024 Replacement for manually deleting excess age groups
## Looks like, blanks, unknowns, 16 or less and 65+ were not in final table
dbGetQuery(con, qry_Private_Credentials_05i2_Delete_AgeGrps)

Graduate_Projections_PTIB <- Graduate_Projections_PTIB %>%
  filter((Survey == "PTIB" & Age_Group != "Unknown") | Survey != "PTIB") %>%
  filter((Survey == "PTIB" & Age_Group != "(blank)") | Survey != "PTIB") %>%
  filter((Survey == "PTIB" & Age_Group != "16 or less") | Survey != "PTIB") %>%
  filter((Survey == "PTIB" & Age_Group != "65+") | Survey != "PTIB")


# Part 3 ----

### note to self: need to work through R equivalents here still

## ---- TBD ----
dbExecute(con, qry_Private_Credentials_06b_Cohort_Dist)

## ---- TBD ----
dbExecute(con, qry_Private_Credentials_06c_Cohort_Dist_Total)

## ---- Delete PTIB rows from Cohort_Program_Distributions_Projected ----
dbExecute(con, qry_Private_Credentials_06d0_Cohort_Dist_Delete_Projected)

## ---- Delete PTIB rows from Cohort_Program_Distributions_Static ----
dbExecute(con, qry_Private_Credentials_06d0_Cohort_Dist_Delete_Static)

## ---- Update Cohort_Program_Distributions_Projected ----
dbExecute(con, qry_Private_Credentials_06d1_Cohort_Dist_Projected)

## ---- Update Cohort_Program_Distributions_Static ----
dbExecute(con, qry_Private_Credentials_06d1_Cohort_Dist_Static)

## ---- Placeholder for manually deleting excess age groups ----
## ---- Delete excess age groups from projected ----
## ADDED 2024 Replacement for manually deleting excess age groups
## Looks like, blanks, unknowns, 16 or less and 65+ were not in final table
# dbGetQuery(con, qry_Private_Credentials_06d2_Projected_Delete_AgeGrps)
#
# Cohort_Program_Distributions_Projected <- Cohort_Program_Distributions_Projected %>%
#   filter((Survey == "PTIB" & Age_Group != "Unknown") | Survey != "PTIB") %>%
#   filter((Survey == "PTIB" & Age_Group != "(blank)") | Survey != "PTIB") %>%
#   filter((Survey == "PTIB" & Age_Group != "16 or less") | Survey != "PTIB") %>%
#   filter((Survey == "PTIB" & Age_Group != "65+") | Survey != "PTIB")

## ---- Delete excess age groups from static ----
## ADDED 2024 Replacement for manually deleting excess age groups
## Looks like, blanks, unknowns, 16 or less and 65+ were not in final table
# dbGetQuery(con, qry_Private_Credentials_06d2_Static_Delete_AgeGrps)
#
# Cohort_Program_Distributions_Static <- Cohort_Program_Distributions_Static %>%
#   filter((Survey == "PTIB" & Age_Group != "Unknown") | Survey != "PTIB") %>%
#   filter((Survey == "PTIB" & Age_Group != "(blank)") | Survey != "PTIB") %>%
#   filter((Survey == "PTIB" & Age_Group != "16 or less") | Survey != "PTIB") %>%
#   filter((Survey == "PTIB" & Age_Group != "65+") | Survey != "PTIB")

# Clean up ----
## ---- Drop helper qry datasets ----
dbExecute(con, "DROP TABLE qry_Private_Credentials_01a_Domestic")
dbExecute(con, "DROP TABLE qry_Private_Credentials_01b_Domestic_International")
dbExecute(con, "DROP TABLE qry_Private_Credentials_01c_Percent_Domestic")
dbExecute(con, "DROP TABLE qry_Private_Credentials_01d_Grads_Blank")
dbExecute(con, "DROP TABLE qry_Private_Credentials_01e_Grads_Union")
dbExecute(con, "DROP TABLE qry_Private_Credentials_01f_Grads")
dbExecute(con, "DROP TABLE qry_Private_Credentials_05i_Grads")
dbExecute(con, "DROP TABLE qry_Private_Credentials_06b_Cohort_Dist")
dbExecute(con, "DROP TABLE qry_Private_Credentials_06c_Cohort_Dist_Total")

## ---- disconnect ----
dbDisconnect(con)
