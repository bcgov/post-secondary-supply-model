# Copyright 2026 Province of British Columbia
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

# ---- Check Required Tables etc. ----
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
t_private_institutions_credentials <- ptib_data |>
  filter(credential != "None") |>
  inner_join(
    pssm_cred_grps |>
      select("PRGM_CREDENTIAL_AWARDED_NAME", "PSSM_CREDENTIAL") |>
      filter(!is.na(PSSM_CREDENTIAL)),
    by = c("credential" = "PRGM_CREDENTIAL_AWARDED_NAME")
  ) |>
  transmute(
    intYear = year,
    Credential = PSSM_CREDENTIAL,
    LCIP_CD = cip,
    Age_Group = age_group,
    Immigration_Status = immigration_status,
    Graduates = as.numeric(sum_of_graduates),
    Enrolled_Not_Graduated = as.numeric(sum_of_enrolments),
    Enrolment = as.numeric(sum_of_total_enrolments)
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

## ---- Check CIP length ----
t_private_institutions_credentials |>
  mutate(Expr1 = str_count(LCIP_CD)) |>
  filter(Expr1 < 6) |>
  select(LCIP_CD, Expr1)


## ---- Check CIPs against infoware 6digit CIPs ----
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

t_private_institutions_credentials <- t_private_institutions_credentials |>
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


# Part 2 ----
private_credentials_summary <- t_private_institutions_credentials |>
  filter(
    is.na(Exclude),
    !is.na(Graduates),
    Credential %in% c("CERT", "DIPL")
  ) |>
  summarise(
    Domestic = sum(Graduates[Immigration_Status == "Domestic"], na.rm = TRUE),
    Known_Total = sum(
      Graduates[Immigration_Status %in% c("Domestic", "International", "#N/A")],
      na.rm = TRUE
    ),
    Blank_Unknown = sum(
      Graduates[Immigration_Status %in% c("(blank)", "Unknown")],
      na.rm = TRUE
    ),
    .by = c(Credential, LCIP_CD, Age_Group)
  ) |>
  mutate(
    Year = "2023/2024",
    Percent_Domestic = if_else(Known_Total == 0, 0, Domestic / Known_Total),
    Grads = Domestic + (Blank_Unknown * Percent_Domestic)
  ) |>
  mutate(
    SumOfGrads = sum(Grads, na.rm = TRUE),
    .by = c(Year, Credential, Age_Group)
  )

## ---- Summarize the Grads by Credential/Age/CIP ----
qry_Private_Credentials_01f_Grads <- private_credentials_summary |>
  distinct(Year, Credential, LCIP_CD, Age_Group, Grads)

## ---- Summarize the Grads by Credential/Age ----
qry_Private_Credentials_05i_Grads <- private_credentials_summary |>
  distinct(Year, Credential, Age_Group, SumOfGrads)

qry_Private_Credentials_05i1_Grads_by_Year <- qry_Private_Credentials_05i_Grads |>
  inner_join(
    T_PTIB_Y1_to_Y10,
    by = c("Year" = "Y1"),
    relationship = "many-to-many"
  ) |>
  transmute(
    Survey = "PTIB",
    PSSM_CRED = paste0("P - ", Credential),
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
# Is this done in 06?
# Graduate_Projections <- Graduate_Projections |>
#   filter(Survey!="PTIB") |>
#   rbind(qry_Private_Credentials_05i1_Grads_by_Year)

# Part 3 ----
# add PTIB survey data to Cohort_Program_Distributions_Projected and Cohort_Program_Distributions_Static
qry_Private_Credentials_06d1_Cohort_Dist <- qry_Private_Credentials_01f_Grads |>
  mutate(
    LCP4_CD = substr(LCIP_CD, 1, 4)
  ) |>
  summarise(
    Count = sum(Grads, na.rm = TRUE),
    .by = c(Year, Credential, Age_Group, LCP4_CD)
  ) |>
  mutate(
    Total = sum(Count, na.rm = TRUE),
    .by = c(Year, Credential, Age_Group)
  )

qry_Private_Credentials_06d1_Cohort_Dist <- qry_Private_Credentials_06d1_Cohort_Dist |>
  transmute(
    SURVEY = "PTIB",
    PSSM_CREDENTIAL = Credential,
    PSSM_CRED = paste0("P - ", Credential),
    LCP4_CD = LCP4_CD,
    LCIP4_CRED = paste0("P - ", LCP4_CD, " - ", Credential),
    LCIP2_CRED = paste0("P - ", substr(LCP4_CD, 1, 2), " - ", Credential),
    AGE_GROUP = Age_Group,
    YEAR = Year,
    COUNT = Count,
    TOTAL = Total,
    PERCENT = if_else(Total == 0, 0, Count / Total),
    GRAD_STATUS = NA_character_,
    TTRAIN = NA_character_
  ) |>
  filter(
    !(SURVEY == "PTIB" &
      AGE_GROUP %in% c("Unknown", "(blank)", "16 or less", "65+"))
  )

## ---- Use to add PTIB rows to Program Projections ----
# Is this done in 07?
# Cohort_Program_Distributions_Static <- Cohort_Program_Distributions_Static  |>
#   filter(Survey!="PTIB") |>
#   rbind(qry_Private_Credentials_06d1_Cohort_Dist)

# Cohort_Program_Distributions_Projected <- Cohort_Program_Distributions_Projected  |>
#   filter(Survey!="PTIB") |>
#   rbind(qry_Private_Credentials_06d1_Cohort_Dist)

# Clean up ----
tables_to_keep <- c(
  "cpd_static",
  "cpd_proj",
  "qry_Private_Credentials_06d1_Cohort_Dist",
  "qry_Private_Credentials_05i1_Grads_by_Year",
  "Graduate_Projections_PTIB"
)
