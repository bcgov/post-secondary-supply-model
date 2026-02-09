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
dbWriteTable(
  con,
  SQL(glue::glue(
    '"{my_schema}"."T_PSSM_Credential_Grouping"'
  )),
  pssm_cred_grps
)

T_PTIB_Y1_to_Y10 <- read_csv(
  (glue::glue(
    "{lan}\\development\\csv\\gh-source\\lookups\\05\\T_PTIB_Y1_to_Y10.csv"
  ))
)

dbWriteTable(
  con,
  SQL(glue::glue('"{my_schema}"."T_PTIB_Y1_to_Y10"')),
  T_PTIB_Y1_to_Y10
)

INFOWARE_L_CIP_6DIGITS_CIP2016 <- read_csv(
  (glue::glue(
    "{lan}\\development\\csv\\gh-source\\lookups\\05\\INFOWARE_L_CIP_6DIGITS_CIP2016.csv"
  ))
)

dbWriteTable(
  con,
  SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_6DIGITS_CIP2016"')),
  INFOWARE_L_CIP_6DIGITS_CIP2016
)

# PTIB data
ptib_initial <- read_csv(
  (glue::glue(
    "{lan}\\development\\csv\\gh-source\\testing\\05\\T_Private_Institutions_Credentials_Imported_2021-03.csv"
  ))
)

dbWriteTable(
  con,
  SQL(glue::glue(
    '"{my_schema}"."T_Private_Institutions_Credentials_Imported_2021-03"'
  )),
  ptib_initial
)

# other tables should be in the R environment from earlier analysis
grad_proj <- dbReadTable(
  con,
  SQL(glue::glue('"{db_schema}"."Graduate_Projections"'))
)
dbWriteTable(
  con,
  SQL(glue::glue('"{my_schema}"."Graduate_Projections"')),
  grad_proj
)
dbWriteTable(
  con,
  SQL(glue::glue('"{my_schema}"."Graduate_Projections_Ref"')),
  grad_proj
)

cpd_proj <- dbReadTable(
  con,
  SQL(glue::glue('"{db_schema}"."Cohort_Program_Distributions_Projected"'))
)

dbWriteTable(
  con,
  SQL(glue::glue('"{my_schema}"."Cohort_Program_Distributions_Projected_Ref"')),
  cpd_proj
)

dbWriteTable(
  con,
  SQL(glue::glue('"{my_schema}"."Cohort_Program_Distributions_Projected"')),
  cpd_proj
)

cpd_static <- dbReadTable(
  con,
  SQL(glue::glue('"{db_schema}"."Cohort_Program_Distributions_Static"'))
)

dbWriteTable(
  con,
  SQL(glue::glue('"{my_schema}"."Cohort_Program_Distributions_Static"')),
  cpd_static
)

dbWriteTable(
  con,
  SQL(glue::glue('"{my_schema}"."Cohort_Program_Distributions_Static_Ref"')),
  cpd_static
)

## remove tables and use decimal versions for remainder of code
rm(
  T_Private_Institutions_Credentials_Imported_2021_03,
  T_PSSM_Credential_Grouping,
  T_PTIB_Y1_to_Y10
)
rm(
  Graduate_Projections,
  Cohort_Program_Distributions_Static,
  Cohort_Program_Distributions_Projected
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
dbGetQuery(con, qry_Private_Credentials_00a_Append)

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
  )

## ---- Check CIP length ----
### note to self, there was a comment column added here, but nothing to it, removed it (no apparent issues atm)
### note - nothing happens, prints to console
dbGetQuery(con, qry_Private_Credentials_00b_Check_CIP_Length)

# want zero rows
T_Private_Institutions_Credentials %>%
  mutate(Expr1 = str_count(LCIP_CD)) %>%
  filter(Expr1 < 7)

## ---- Remove periods from CIPs ----
dbGetQuery(con, qry_Private_Credentials_00c_Clean_CIP_Period)

T_Private_Institutions_Credentials <- T_Private_Institutions_Credentials %>%
  mutate(LCIP_CD = str_replace_all(LCIP_CD, "\\.", ""))

## ---- Check CIPs against infoware 6digit CIPs ----
### note - nothing happens, prints to console
dbGetQuery(con, qry_Private_Credentials_00d_Check_CIPs)

# import infoware table
infoware <- tbl(con, "INFOWARE_L_CIP_6DIGITS_CIP2016") %>% collect()
### note to self, R version not the same as sql yet - needs work

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
# Added SQL code to update these exclusions automatically
dbGetQuery(
  con,
  "ALTER TABLE T_Private_Institutions_Credentials 
                ADD  
                Exclude VARCHAR(255),
                LCIP_NAME VARCHAR(255)"
)

dbGetQuery(
  con,
  "UPDATE T_Private_Institutions_Credentials 
           SET
            T_Private_Institutions_Credentials.LCIP_NAME = INFOWARE_L_CIP_6DIGITS_CIP2016.LCIP_NAME
           FROM T_Private_Institutions_Credentials INNER JOIN INFOWARE_L_CIP_6DIGITS_CIP2016
           ON T_Private_Institutions_Credentials.LCIP_CD = INFOWARE_L_CIP_6DIGITS_CIP2016.LCIP_CD"
)

dbExecute(
  con,
  "UPDATE T_Private_Institutions_Credentials 
           SET T_Private_Institutions_Credentials.Exclude = 1
           WHERE (((T_Private_Institutions_Credentials.LCIP_NAME) ='English as a second language') OR
          ((T_Private_Institutions_Credentials.LCIP_NAME) LIKE '%not for credit%') OR
          ((T_Private_Institutions_Credentials.LCIP_CD)='999999') );"
)

dbGetQuery(
  con,
  "ALTER TABLE T_Private_Institutions_Credentials
                DROP COLUMN LCIP_NAME;"
)

# R code version
# get not for credit CIPs from infoware
find_nfc_cips <- infoware %>%
  filter(grepl("not for credit", LCIP_NAME)) %>%
  select(LCIP_CD) %>%
  distinct()
# get ESL programs from infoware
find_esl_cips <- infoware %>%
  filter(LCIP_NAME == "English as a second language") %>%
  select(LCIP_CD) %>%
  distinct()

# add Exclude flag
T_Private_Institutions_Credentials <- T_Private_Institutions_Credentials %>%
  mutate(
    Exclude = case_when(
      LCIP_CD %in% find_nfc_cips$LCIP_CD ~ '1', ## exclude not for credit
      LCIP_CD %in% find_esl_cips$LCIP_CD ~ '1', ## exclude esl
      LCIP_CD == "999999" ~ '1'
    )
  ) ## exclude undeclared/unclassified

rm(find_nfc_cips, find_esl_cips)

## ---- Update age groups ----
dbGetQuery(con, qry_Private_Credentials_00f_Recode_Age_Group)

T_Private_Institutions_Credentials <- T_Private_Institutions_Credentials %>%
  mutate(Age_Group = str_replace_all(Age_Group, "-", " to "))

## ---- Fix immigration status ----
# Immigration_Status “#N/A” recoded to “(blank)”
# none for 2019-20; instead there was “Unknown”
# note to self, could have a query to recode in case #N/A appears again
T_Private_Institutions_Credentials %>%
  count(Immigration_Status)

## ---- Copy to Clean table ----
# Copied as T_Private_Institutions_Credentials_Clean
# there may be duplicate CIPs after the cleaning but that is ok as next step sums and divides by 2 (the number of years) for the average
dbExecute(
  con,
  "SELECT *
                INTO T_Private_Institutions_Credentials_Clean
                FROM T_Private_Institutions_Credentials;"
)

T_Private_Institutions_Credentials_Clean <- T_Private_Institutions_Credentials

## ---- Age averages ----
## not sure if this will be necessary if only one year in data???
dbGetQuery(
  con,
  "ALTER TABLE T_Private_Institutions_Credentials
                ALTER COLUMN intYear VARCHAR(255);"
)

dbGetQuery(con, qry_Private_Credentials_00g_Avg)

# I updated the above query as it didn't run as-is, but 2017, 2018, and Avg were all in the resulting table
# I think table should actually ONLY result in table with just Avg; remove the 2017, 2018 individual rows
dbGetQuery(
  con,
  "DELETE FROM T_Private_Institutions_Credentials
                WHERE intYear <> 'Avg 2017 & 2018'"
)

## TBD if this is correct R version
T_Private_Institutions_Credentials <- T_Private_Institutions_Credentials_Clean %>%
  filter(is.na(Exclude)) %>%
  mutate(intYear = "Avg 2021 & 2022") %>%
  group_by(
    intYear,
    Credential,
    LCIP_CD,
    Age_Group,
    Immigration_Status,
    Exclude
  ) %>%
  mutate(
    Enrolment = sum(Enrolment) / 2,
    Enrolled_Not_Graduated = sum(Enrolled_Not_Graduated) / 2,
    Graduates = sum(Graduates) / 2
  ) %>%
  distinct() %>%
  ungroup()

T_Private_Institutions_Credentials <- as.data.frame(
  T_Private_Institutions_Credentials
)

# Part 2 ----
## STOP !!! Update model year in queries ----

## ---- Count domestic grads ----
dbGetQuery(con, qry_Private_Credentials_01a_Domestic)

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

qry_Private_Credentials_01a_Domestic <- as.data.frame(
  qry_Private_Credentials_01a_Domestic
)

## ---- Count domestic and international grads ----
### note to self: this qry still has #N/A which was changed to (blank)
### none this time, but could affect these queries ?
dbGetQuery(con, qry_Private_Credentials_01b_Domestic_International)

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

qry_Private_Credentials_01b_Domestic_International <- as.data.frame(
  qry_Private_Credentials_01b_Domestic_International
)

## ---- Compute percent of domestic and international grads that are domestic ----
dbGetQuery(con, qry_Private_Credentials_01c_Percent_Domestic)

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

qry_Private_Credentials_01c_Percent_Domestic <- as.data.frame(
  qry_Private_Credentials_01c_Percent_Domestic
)

## ---- Compute unknown or blank immigration status ----
## computes Blank/Unknown immigration status records to include as domestic grads;
## 2019-09-06 updated criteria to “(blank) Or Unknown”
dbGetQuery(con, qry_Private_Credentials_01d_Grads_Blank)

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

qry_Private_Credentials_01d_Grads_Blank <- as.data.frame(
  qry_Private_Credentials_01d_Grads_Blank
)

## ---- Join domestic and blank ----
dbGetQuery(con, qry_Private_Credentials_01e_Grads_Union)

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
