# Create APPSO CIP records
# Description:
# Relies on:
#   - credential_non_dup,
#   - infoware CIP tables
# Creates updated list of IDS with appropriate extra CIP columns for APPSO records
# Uses the same queries as the BGS/GRAD CIP matching

library(arrow)
library(tidyverse)
library(odbc)
library(DBI)
library(dbplyr)

# ---- Configure LAN Paths and DB Connection -----
lan <- config::get("lan")

db_config <- config::get("decimal")
my_schema <- config::get("myschema")

con <- dbConnect(odbc(),
    Driver = db_config$driver,
    Server = db_config$server,
    Database = db_config$database,
    Trusted_Connection = "True"
)

# ---- Table References ----
credential_non_dup_tbl <- tbl(con, in_schema(my_schema, "credential_non_dup"))
cip_6_tbl <- tbl(con, in_schema(my_schema, "INFOWARE_L_CIP_6DIGITS_CIP2016"))
cip_4_tbl <- tbl(con, in_schema(my_schema, "INFOWARE_L_CIP_4DIGITS_CIP2016"))
cip_2_tbl <- tbl(con, in_schema(my_schema, "INFOWARE_L_CIP_2DIGITS_CIP2016"))

# ---- START PROCESSING ----

# 1. Create cleaning table (collect STP APPSO data)
# qry_APPSO_STP_CIP_Cleaning
appso_cleaning <- credential_non_dup_tbl %>%
    filter(OUTCOMES_CRED == "APPSO") %>%
    group_by(PSI_CREDENTIAL_CIP, OUTCOMES_CRED) %>%
    summarize(Expr1 = n(), .groups = "drop") %>%
    # qry_APPSO_STP_CIP_update_original
    mutate(PSI_CREDENTIAL_CIP_orig = PSI_CREDENTIAL_CIP) %>%
    # qry_APPSO_STP_CIP_clean_cip_1 & 2
    mutate(
        PSI_CREDENTIAL_CIP = case_when(
            nchar(PSI_CREDENTIAL_CIP) == 6 & !str_detect(substr(PSI_CREDENTIAL_CIP, 1, 2), "\\.") ~ paste0(PSI_CREDENTIAL_CIP, "0"),
            nchar(PSI_CREDENTIAL_CIP) == 6 ~ paste0("0", PSI_CREDENTIAL_CIP),
            TRUE ~ PSI_CREDENTIAL_CIP
        )
    ) %>%
    # Create columns needed for joins
    mutate(
        PSI_CREDENTIAL_CIP_5 = substr(PSI_CREDENTIAL_CIP, 1, 5),
        PSI_CREDENTIAL_CIP_2 = substr(PSI_CREDENTIAL_CIP, 1, 2)
    )

# 2. Add 4 and 2D CIP codes from INFOWARE
# qry_Clean_APPSO_STP_CIP_Step1_a (Exact match)
appso_cleaning <- appso_cleaning %>%
    left_join(
        cip_6_tbl %>% select(LCIP_CD_WITH_PERIOD, LCIP_LCP4_CD, LCIP_LCP2_CD),
        by = c("PSI_CREDENTIAL_CIP" = "LCIP_CD_WITH_PERIOD")
    ) %>%
    rename(STP_CIP_CODE_4 = LCIP_LCP4_CD, STP_CIP_CODE_2 = LCIP_LCP2_CD)

# qry_Clean_APPSO_STP_CIP_Step1_b (Match on first 5 chars)
appso_cleaning <- appso_cleaning %>%
    left_join(
        cip_6_tbl %>%
            mutate(CIP_5 = substr(LCIP_CD_WITH_PERIOD, 1, 5)) %>%
            select(CIP_5, LCIP_LCP4_CD_alt = LCIP_LCP4_CD, LCIP_LCP2_CD_alt = LCIP_LCP2_CD) %>%
            distinct(),
        by = c("PSI_CREDENTIAL_CIP_5" = "CIP_5")
    ) %>%
    mutate(
        STP_CIP_CODE_4 = coalesce(STP_CIP_CODE_4, LCIP_LCP4_CD_alt),
        STP_CIP_CODE_2 = coalesce(STP_CIP_CODE_2, LCIP_LCP2_CD_alt)
    ) %>%
    select(-LCIP_LCP4_CD_alt, -LCIP_LCP2_CD_alt)

# qry_Clean_APPSO_STP_CIP_Step1_c (General programs recode)
general_cips <- c("11.00", "13.00", "14.00", "19.00", "23.00", "24.00", "26.00", "40.00", "42.00", "45.00", "50.00", "52.00", "55.00")
appso_cleaning <- appso_cleaning %>%
    mutate(
        STP_CIP_CODE_4 = case_when(
            is.na(STP_CIP_CODE_4) & PSI_CREDENTIAL_CIP_5 %in% general_cips ~ paste0(substr(PSI_CREDENTIAL_CIP, 1, 2), "01"),
            TRUE ~ STP_CIP_CODE_4
        )
    )

# qry_Clean_APPSO_STP_CIP_Step1_d (Match on first 2 digits for 2D code)
appso_cleaning <- appso_cleaning %>%
    left_join(
        cip_6_tbl %>%
            mutate(CIP_2 = substr(LCIP_CD_WITH_PERIOD, 1, 2)) %>%
            select(CIP_2, LCIP_LCP2_CD_alt2 = LCIP_LCP2_CD) %>%
            distinct(),
        by = c("PSI_CREDENTIAL_CIP_2" = "CIP_2")
    ) %>%
    mutate(
        STP_CIP_CODE_2 = coalesce(STP_CIP_CODE_2, LCIP_LCP2_CD_alt2)
    ) %>%
    select(-LCIP_LCP2_CD_alt2)

# 3. Add names
# qry_Clean_APPSO_STP_CIP_Step2 (4D names)
appso_cleaning <- appso_cleaning %>%
    left_join(
        cip_4_tbl %>% select(LCP4_CD, LCP4_CIP_4DIGITS_NAME),
        by = c("STP_CIP_CODE_4" = "LCP4_CD")
    ) %>%
    rename(STP_CIP_CODE_4_NAME = LCP4_CIP_4DIGITS_NAME)

# qry_Clean_APPSO_STP_CIP_Step3 (2D names)
appso_cleaning <- appso_cleaning %>%
    left_join(
        cip_2_tbl %>% select(LCP2_CD, LCP2_DIGITS_NAME),
        by = c("STP_CIP_CODE_2" = "LCP2_CD")
    ) %>%
    rename(STP_CIP_CODE_2_NAME = LCP2_DIGITS_NAME)

# qry_Clean_APPSO_STP_CIP_step4 (Invalid names)
appso_cleaning <- appso_cleaning %>%
    mutate(
        STP_CIP_CODE_4_NAME = if_else(is.na(STP_CIP_CODE_4_NAME), "Invalid 4-digit CIP", STP_CIP_CODE_4_NAME)
    )

# 4. Final Join and Create ID List
# qry_Update_Credential_with_STP_CIP_APPSO
final_appso_ids <- credential_non_dup_tbl %>%
    filter(OUTCOMES_CRED == "APPSO") %>%
    inner_join(
        appso_cleaning,
        by = c("PSI_CREDENTIAL_CIP" = "PSI_CREDENTIAL_CIP_orig", "OUTCOMES_CRED" = "OUTCOMES_CRED")
    ) %>%
    select(
        ID, PSI_CODE, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
        PSI_CREDENTIAL_CIP, PSI_AWARD_SCHOOL_YEAR, OUTCOMES_CRED,
        FINAL_CIP_CODE_4 = STP_CIP_CODE_4,
        FINAL_CIP_CODE_4_NAME = STP_CIP_CODE_4_NAME,
        FINAL_CIP_CODE_2 = STP_CIP_CODE_2,
        FINAL_CIP_CODE_2_NAME = STP_CIP_CODE_2_NAME
    ) %>%
    # qry_Update_Credential_with_STP_CIP_APPSO_nulls
    mutate(
        PSI_PROGRAM_CODE = if_else(PSI_PROGRAM_CODE == "(Unspecified)", NA_character_, PSI_PROGRAM_CODE)
    )

# 5. Save to Database
# Check if table exists and remove it if it does
if (dbExistsTable(con, Id(schema = my_schema, table = "Credential_Non_Dup_APPSO_IDs"))) {
    dbRemoveTable(con, Id(schema = my_schema, table = "Credential_Non_Dup_APPSO_IDs"))
}

# Write the table
final_appso_ids %>%
    compute(name = "Credential_Non_Dup_APPSO_IDs", temporary = FALSE, schema = my_schema)

# ---- Clean up ----
dbDisconnect(con)
