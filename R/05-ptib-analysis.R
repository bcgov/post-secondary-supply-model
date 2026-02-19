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

library(tidyverse)

# ---- Data Requirements and SQL Definitons ----
# this may not be used at all in the script
INFOWARE_L_CIP_6DIGITS_CIP2016 <- infoware

# ---- Check Required Tables etc. ----
# required tables in decimal or R environment (location TBD)
required_tables <- c(
  'T_PTIB_Y1_to_Y10',
  'cpd_proj',
  'cpd_static',
  'INFOWARE_L_CIP_6DIGITS_CIP2016',
  'grad_proj',
  'ptib_data',
  'pssm_cred_grps'
)

missing <- required_tables[!sapply(required_tables, exists, where = .GlobalEnv)]

if (length(missing) > 0) {
  stop(paste(
    "The following required tables are missing from the environment:",
    paste(missing, collapse = ", ")
  ))
}

na_vals <- c("", " ", "(Unspecified)", NA) #this should be updated?

# Part 1 ----

## ---- Add PSSM_Credential to PTIB data ----
# Note: this join filters is intended as a filter join.
# we should explicitly filter on the credential types we want to keep (CERT AND DIPL)
t_private_institutions_credentials <- pssm_cred_grps |>
  select("PRGM_CREDENTIAL_AWARDED_NAME", "PSSM_CREDENTIAL") |>
  filter(!is.na(PSSM_CREDENTIAL)) |>
  inner_join(
    ptib_data |> filter(credential != "None"),
    by = c("PRGM_CREDENTIAL_AWARDED_NAME" = "credential")
  ) |>
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
t_private_institutions_credentials |>
  mutate(Expr1 = str_count(LCIP_CD)) |>
  filter(Expr1 < 7) |>
  select(LCIP_CD, Expr1)

## ---- Remove periods from CIPs ----
t_private_institutions_credentials <- t_private_institutions_credentials |>
  mutate(
    # this part replicated the logic from load_ptib.R
    LCIP_CD = sapply(LCIP_CD, function(x) {
      parts <- str_split(x, "\\.", simplify = TRUE)
      prefix <- str_pad(parts[1], width = 2, side = "left", pad = "0")
      suffix <- str_pad(parts[2], width = 4, side = "right", pad = "0")
      return(paste0(prefix, ".", suffix))
    })
  ) |>
  mutate(LCIP_CD = str_replace_all(LCIP_CD, "\\.", ""))

t_private_institutions_credentials |>
  mutate(Expr1 = str_count(LCIP_CD)) |>
  filter(Expr1 < 6) |>
  select(LCIP_CD, Expr1)


## ---- Check CIPs against infoware 6digit CIPs ----
## R version handles the scenario where some CIPS have leading or trailing 0's
t_private_institutions_credentials |>
  select(LCIP_CD) |>
  left_join(
    infoware |> mutate(exists = "yes") |> select(LCIP_CD, exists),
    by = c("LCIP_CD" = "LCIP_CD")
  ) |>
  filter(is.na(exists))

## ---- Update Exclude column ----
# Exclude not for credit and ESL programs and unclassified 99.9999 manually with “Exclude=1”
t_private_institutions_credentials <- t_private_institutions_credentials |>
  left_join(
    infoware |> distinct(LCIP_CD, LCIP_NAME),
    by = "LCIP_CD"
  ) |>
  mutate(
    Exclude = case_when(
      LCIP_NAME == "English as a second language" ~ "1",
      str_detect(LCIP_NAME, "(?i)not for credit") ~ "1", # Case-insensitive LIKE
      LCIP_CD == "99999" ~ "1",
      TRUE ~ NA_character_
    )
  ) |>
  select(-LCIP_NAME)

## ---- Update age groups ----
t_private_institutions_credentials <- t_private_institutions_credentials |>
  mutate(Age_Group = str_replace_all(Age_Group, "-", " to "))

## ---- Fix immigration status ----
# make decision on how to recode (blank, Unknown or NA) - leaving for this run
t_private_institutions_credentials |>
  count(Immigration_Status)

## ---- Age averages ----
# check relevant years to update queries below
t_private_institutions_credentials |>
  count(intYear)

avg_summary <- t_private_institutions_credentials |>
  filter(is.na(Exclude)) |>
  group_by(Credential, LCIP_CD, Age_Group, Immigration_Status, Exclude) |>
  summarise(
    Enrolment = sum(Enrolment, na.rm = TRUE) / 2,
    Enrolled_Not_Graduated = sum(Enrolled_Not_Graduated, na.rm = TRUE) / 2,
    Graduates = sum(Graduates, na.rm = TRUE) / 2,
    .groups = "drop"
  ) |>
  mutate(intYear = "Avg 2021 & 2022") |>
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

# this is good for comparison, but we use just the average for the rest of the script.
#t_private_institutions_credentials <- bind_rows(
#  t_private_institutions_credentials |> mutate(intYear = as.character(intYear)),
#  avg_summary
#)
t_private_institutions_credentials_clean <- t_private_institutions_credentials
t_private_institutions_credentials <- avg_summary

# Part 2 ----
## ---- Count domestic grads ----
qry_Private_Credentials_01a_Domestic <- t_private_institutions_credentials |>
  filter(
    is.na(Exclude),
    !is.na(Graduates),
    Credential %in% c("CERT", "DIPL")
  ) |> # I don't think this does anything
  group_by(Credential, LCIP_CD, Age_Group) |>
  summarize(
    Domestic = sum(Graduates[Immigration_Status == "Domestic"], na.rm = TRUE), # here, we are summing over 2 years, and the average.  So wondering why.
    .groups = "drop"
  ) |>
  mutate(Year = "2023/2024") |>
  select(Year, Credential, LCIP_CD, Age_Group, Domestic)


## ---- Count domestic and international grads ----
qry_Private_Credentials_01b_Domestic_International <- t_private_institutions_credentials |>
  filter(is.na(Exclude), !is.na(Graduates)) |>
  filter(
    Immigration_Status == "Domestic" |
      Immigration_Status == "International" |
      Immigration_Status == "#N/A"
  ) |> # what about (blank) if it existed???
  filter(Credential == "CERT" | Credential == "DIPL") |>
  mutate(Year = "2023/2024") |>
  select(Year, Credential, LCIP_CD, Age_Group, Graduates) |>
  group_by(Year, Credential, LCIP_CD, Age_Group) |>
  summarize(Domestic_International = sum(Graduates))

## ---- Compute percent of domestic and international grads that are domestic ----
qry_Private_Credentials_01c_Percent_Domestic <- qry_Private_Credentials_01a_Domestic |>
  inner_join(
    qry_Private_Credentials_01b_Domestic_International,
    by = c("Age_Group", "LCIP_CD", "Credential", "Year")
  ) |>
  mutate(
    Percent_Domestic = ifelse(
      Domestic == 0,
      0,
      Domestic / Domestic_International
    )
  )

## ---- Compute unknown or blank immigration status ----
## computes Blank/Unknown immigration status records to include as domestic grads;
qry_Private_Credentials_01d_Grads_Blank <- t_private_institutions_credentials |>
  filter(
    (Immigration_Status == "(blank)" |
      Immigration_Status == "Unknown"),
    is.na(Exclude)
  ) |>
  inner_join(
    qry_Private_Credentials_01c_Percent_Domestic,
    by = c("Age_Group", "LCIP_CD", "Credential")
  ) |>
  mutate(Graduates_Blank = Graduates * Percent_Domestic) |>
  select(Year, Credential, LCIP_CD, Age_Group, Graduates_Blank)


## ---- Join domestic and blank ----
qry_Private_Credentials_01e_Grads_Union <- qry_Private_Credentials_01a_Domestic |>
  rbind(
    qry_Private_Credentials_01d_Grads_Blank |>
      rename(Domestic = Graduates_Blank)
  )

## ---- Sum of union query ----
qry_Private_Credentials_01f_Grads <- qry_Private_Credentials_01e_Grads_Union |>
  group_by(Year, Credential, LCIP_CD, Age_Group) |>
  summarize(Grads = sum(Domestic, na.rm = TRUE), .groups = "drop")

dbWriteTable(
  decimal_con,
  "qry_Private_Credentials_01f_Grads",
  qry_Private_Credentials_01f_Grads,
  overwrite = TRUE
)

## ---- Summarize the Grads by Credential/Age ----
qry_Private_Credentials_05i_Grads <- qry_Private_Credentials_01f_Grads |>
  group_by(Year, Credential, Age_Group) |>
  summarize(SumOfGrads = sum(Grads, na.rm = TRUE), .groups = "drop")

qry_Private_Credentials_05i1_Grads_by_Year <- qry_Private_Credentials_05i_Grads |>
  inner_join(
    T_PTIB_Y1_to_Y10,
    by = c("Year" = "Y1"),
    relationship = "many-to-many"
  ) |>
  mutate(
    Survey = "PTIB",
    PSSM_CRED = paste0("P - ", Credential)
  ) |>
  select(
    Survey,
    PSSM_CRED,
    Age_Group,
    Year = Y1_TO_Y10,
    Graduates = SumOfGrads
  )

## ---- Delete excess age groups ----
qry_Private_Credentials_05i1_Grads_by_Year <- qry_Private_Credentials_05i1_Grads_by_Year |>
  filter((Survey == "PTIB" & Age_Group != "Unknown") | Survey != "PTIB") |>
  filter((Survey == "PTIB" & Age_Group != "(blank)") | Survey != "PTIB") |>
  filter((Survey == "PTIB" & Age_Group != "16 or less") | Survey != "PTIB") |>
  filter((Survey == "PTIB" & Age_Group != "65+") | Survey != "PTIB")


## ---- Use to add PTIB rows to Graduate_Projections ----
Graduate_Projections_PTIB <- qry_Private_Credentials_05i1_Grads_by_Year

# Graduate_Projections <- Graduate_Projections |>
#   filter(Survey!="PTIB")

# Graduate_Projections <- Graduate_Projections |>
#   rbind(Graduate_Projections_PTIB)

# Part 3 ----
# add PTIB survey data to Cohort_Program_Distributions_Projected and Cohort_Program_Distributions_Static
qry_Private_Credentials_06b_Cohort_Dist <- qry_Private_Credentials_01f_Grads |>
  mutate(
    PSSM_CRED = paste0("P - ", Credential),
    LCP4_CD = substr(LCIP_CD, 1, 4),
    LCIP4_CRED = paste0("P - ", substr(LCIP_CD, 1, 4), " - ", Credential),
    LCIP2_CRED = paste0("P - ", substr(LCIP_CD, 1, 2), " - ", Credential)
  ) |>
  group_by(
    Year,
    Credential,
    PSSM_CRED,
    LCP4_CD,
    LCIP4_CRED,
    LCIP2_CRED,
    Age_Group
  ) |>
  summarize(
    Count = sum(Grads, na.rm = TRUE),
    .groups = "drop"
  )

qry_Private_Credentials_06c_Cohort_Dist_Total <- qry_Private_Credentials_06b_Cohort_Dist |>
  group_by(Year, Credential, PSSM_CRED, Age_Group) |>
  summarize(
    Total = sum(Count, na.rm = TRUE),
    .groups = "drop"
  )

qry_Private_Credentials_06d1_Cohort_Dist <- qry_Private_Credentials_06b_Cohort_Dist |>
  inner_join(
    qry_Private_Credentials_06c_Cohort_Dist_Total,
    by = c("PSSM_CRED", "Age_Group", "Year", "Credential")
  ) |>
  mutate(
    Survey = "PTIB",
    Percent = ifelse(Total == 0, 0, Count / Total),
    GRAD_STATUS = NA_character_,
    TTRAIN = NA_character_
  ) |>
  select(
    Survey,
    PSSM_CREDENTIAL = Credential,
    PSSM_CRED,
    LCP4_CD,
    LCIP4_CRED,
    LCIP2_CRED,
    Age_Group,
    Year,
    Count,
    Total,
    Percent,
    GRAD_STATUS,
    TTRAIN
  ) |>
  filter(Age_Group != "16 or less", Age_Group != "65+") |>
  janitor::clean_names(case = "all_caps")

# update static and projected cohort distributions
# this may not make sense in ourworkflow
cpd_proj <- cpd_proj |>
  filter(Survey != "PTIB")

cpd_static <- cpd_static |>
  filter(Survey != "PTIB")

cpd_static <- cpd_static |>
  rbind(qry_Private_Credentials_06d1_Cohort_Dist)

cpd_proj <- cpd_proj |>
  rbind(qry_Private_Credentials_06d1_Cohort_Dist)

cpd_proj <- cpd_proj |>
  filter((SURVEY == "PTIB" & AGE_GROUP != "Unknown") | SURVEY != "PTIB") |>
  filter((SURVEY == "PTIB" & AGE_GROUP != "(blank)") | SURVEY != "PTIB") |>
  filter((SURVEY == "PTIB" & AGE_GROUP != "16 or less") | SURVEY != "PTIB") |>
  filter((SURVEY == "PTIB" & AGE_GROUP != "65+") | SURVEY != "PTIB")

cpd_static <- cpd_static |>
  filter((SURVEY == "PTIB" & AGE_GROUP != "Unknown") | SURVEY != "PTIB") |>
  filter((SURVEY == "PTIB" & AGE_GROUP != "(blank)") | SURVEY != "PTIB") |>
  filter((SURVEY == "PTIB" & AGE_GROUP != "16 or less") | SURVEY != "PTIB") |>
  filter((SURVEY == "PTIB" & AGE_GROUP != "65+") | SURVEY != "PTIB")

# Clean up ----
