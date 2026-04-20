# DACSO Program Matching — dplyr Translation
# Original: R/02a-dacso-program-matching.R
#
# Pipeline context:
#   Aligns CIP codes between DACSO (outcomes survey) and STP (Student Transitions
#   Project) credential data. Builds and updates a crosswalk (XWALK) table that maps
#   program codes to standardized 4-digit CIP codes.
#
#   Steps:
#     1. Update XWALK with new DACSO programs (historical linkages + new programs)
#     2. Create STP credential table, auto-match to XWALK
#     3. Institution-specific custom matching (BCIT, CAPU, VIU)
#     4. Compute final CIP codes from INFOWARE taxonomy
#
# Input tables:
#   - INFOWARE_PROGRAMS — master program listing (Oracle/JDBC)
#   - INFOWARE_L_CIP_*DIGITS_CIP2016 — CIP taxonomy (Oracle/JDBC)
#   - INFOWARE_PROGRAMS_HIST_PRGMID_XREF — historical program linkages (Oracle/JDBC)
#   - DACSO_STP_ProgramsCIP4_XWALK_ALL_2020 — previous cycle XWALK (Access)
#   - Credential_Non_Dup — deduplicated credentials (from 02a-update-cred-non-dup)
#
# Output tables:
#   - DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 — updated XWALK
#   - Credential_Non_Dup_Programs_DACSO_FinalCIPS — final CIP assignments

library(tidyverse)
library(RODBC)
library(config)
library(glue)
library(odbc)
library(RJDBC)

# Helper: reference a table in the user's schema
my_schema <- config::get("myschema")

sch_tbl <- function(name) {
  tbl(con, dbplyr::in_schema(my_schema, name))
}


# ******************************************************************************
# SETUP: Read external tables and write to Decimal
# ******************************************************************************
# WHY: INFOWARE tables come from Oracle/JDBC, previous XWALK comes from an Access
# database. These need to be written to SQL Server for the pipeline to access them.

iw_config <- config::get("infoware")
jdbc_config <- config::get("jdbc")

jdbcDriver <- JDBC(jdbc_config$class, classPath = jdbc_config$path)

iw_con <- dbConnect(jdbcDriver,
                    iw_config$database,
                    iw_config$uid,
                    iw_config$pwd)

INFOWARE_PROGRAMS <- dbReadTable(iw_con, "INFOWARE.PROGRAMS")
INFOWARE_L_CIP_6DIGITS_CIP2016 <- dbReadTable(iw_con, "INFOWARE.L_CIP_6DIGITS_CIP2016")
INFOWARE_L_CIP_4DIGITS_CIP2016 <- dbReadTable(iw_con, "INFOWARE.L_CIP_4DIGITS_CIP2016")
INFOWARE_L_CIP_2DIGITS_CIP2016 <- dbReadTable(iw_con, "INFOWARE.L_CIP_2DIGITS_CIP2016")
INFOWARE_PROGRAMS_HIST_PRGMID_XREF <- dbReadTable(iw_con, "INFOWARE.PROGRAMS_HIST_PRGMID_XREF")

dbDisconnect(iw_con)

# Read previous cycle XWALK from Access database
connection <- config::get("connection")$outcomes_dacso
acc_con <- odbcDriverConnect(connection)

DACSO_STP_ProgramsCIP4_XWALK_ALL_2020 <- sqlQuery(acc_con, "SELECT * FROM DACSO_STP_ProgramsCIP4_XWALK_ALL_2020;")

odbcClose(acc_con)

# ---- Connect to Decimal ----
db_config <- config::get("decimal")
con <- dbConnect(odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")

# Write all source tables to Decimal
dbWriteTable(con, SQL(glue::glue('"{my_schema}"."DACSO_STP_ProgramsCIP4_XWALK_ALL_2020"')), DACSO_STP_ProgramsCIP4_XWALK_ALL_2020)
dbWriteTable(con, SQL(glue::glue('"{my_schema}"."INFOWARE_PROGRAMS"')), INFOWARE_PROGRAMS)
dbWriteTable(con, SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_6DIGITS_CIP2016"')), INFOWARE_L_CIP_6DIGITS_CIP2016)
dbWriteTable(con, SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_4DIGITS_CIP2016"')), INFOWARE_L_CIP_4DIGITS_CIP2016)
dbWriteTable(con, SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_2DIGITS_CIP2016"')), INFOWARE_L_CIP_2DIGITS_CIP2016)
dbWriteTable(con, SQL(glue::glue('"{my_schema}"."INFOWARE_PROGRAMS_HIST_PRGMID_XREF"')), INFOWARE_PROGRAMS_HIST_PRGMID_XREF)

rm(DACSO_STP_ProgramsCIP4_XWALK_ALL_2020, INFOWARE_PROGRAMS, INFOWARE_L_CIP_6DIGITS_CIP2016,
   INFOWARE_L_CIP_4DIGITS_CIP2016, INFOWARE_L_CIP_2DIGITS_CIP2016, INFOWARE_PROGRAMS_HIST_PRGMID_XREF)


# ******************************************************************************
# PART 1: UPDATE XWALK WITH NEW DACSO DATA
# ******************************************************************************
# WHY: New DACSO programs from the 2021-2023 survey cycles need to be added to
# the XWALK. Programs with historical linkages inherit their CIP codes from the
# previous program version. Programs without linkages get their CIP directly.
#
# This section is already R-native dplyr in the original — kept with minimal changes.

## ---- Create programs_table from combining INFOWARE tables ----
programs_table <- tbl(con, "INFOWARE_PROGRAMS") %>%
  inner_join(tbl(con, "INFOWARE_L_CIP_6DIGITS_CIP2016"), by = c("LCIP_CD_CIP2016" = "LCIP_CD")) %>%
  inner_join(tbl(con, "INFOWARE_L_CIP_4DIGITS_CIP2016"), by = c("LCIP_LCP4_CD" = "LCP4_CD")) %>%
  select(PRGM_ID, PRGM_FIRST_SEEN_SUBM_CD, PRGM_INST_CD, PRGM_INST_PROGRAM_NAME,
         PRGM_INST_PROGRAM_NAME_CLEANED,
         PRGM_LCPC_CD, PRGM_TTRAIN_FLAG, LCIP_CD_CIP2016, LCIP_NAME_CIP2016,
         PRGM_CREDENTIAL, NOTES, HAS_HISTORICAL_PRGM_ID_LINK,
         CIP_CLUSTER_ARTS_APPLIED, DACSO_OLD_PRGM_ID_DO_NOT_USE, DUP_PROGRAM_USE_THIS_PRGM_ID,
         LCIP_LCP4_CD, LCP4_CIP_4DIGITS_NAME) %>%
  collect()

## ---- Make new XWALK from last years XWALK ----
DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 <- tbl(con, "DACSO_STP_ProgramsCIP4_XWALK_ALL_2020") %>%
  collect() %>%
  mutate(CIP_CODE_4 = str_pad(CIP_CODE_4, width = 4, side = "left", pad = "0"))

# ---- Add programs WITHOUT historical linkages ----
programs_table %>%
  filter(PRGM_FIRST_SEEN_SUBM_CD %in% c('C_Outc21', 'C_Outc22', 'C_Outc23')) %>%
  group_by(PRGM_FIRST_SEEN_SUBM_CD, HAS_HISTORICAL_PRGM_ID_LINK) %>% tally()

new_dacso_programs_21_23 <- programs_table %>%
  filter(PRGM_FIRST_SEEN_SUBM_CD %in% c('C_Outc21', 'C_Outc22', 'C_Outc23') &
         (is.na(HAS_HISTORICAL_PRGM_ID_LINK) | HAS_HISTORICAL_PRGM_ID_LINK == " "))

DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 <- DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 %>%
  bind_rows(
    programs_table %>%
      filter(PRGM_FIRST_SEEN_SUBM_CD %in% c('C_Outc21', 'C_Outc22', 'C_Outc23') &
             (is.na(HAS_HISTORICAL_PRGM_ID_LINK) | HAS_HISTORICAL_PRGM_ID_LINK == " ")) %>%
      mutate(New_DACSO_Program2021_23 = case_when(
        PRGM_FIRST_SEEN_SUBM_CD == "C_Outc21" ~ "Yes2021",
        PRGM_FIRST_SEEN_SUBM_CD == "C_Outc22" ~ "Yes2022",
        PRGM_FIRST_SEEN_SUBM_CD == "C_Outc23" ~ "Yes2023"
      )) %>%
      select(COCI_INST_CD = PRGM_INST_CD, PRGM_LCPC_CD, PRGM_INST_PROGRAM_NAME,
             CIP_CODE_4 = LCIP_LCP4_CD, LCP4_CIP_4DIGITS_NAME, PRGM_ID,
             PRGM_CREDENTIAL, New_DACSO_Program2021_23)
  )

# ---- 2021: Programs WITH historical linkages ----
Updated_DACSO_Programs_in_2021_with_links <- programs_table %>%
  filter(PRGM_FIRST_SEEN_SUBM_CD == 'C_Outc21' & HAS_HISTORICAL_PRGM_ID_LINK == 'Y') %>%
  inner_join(
    tbl(con, "INFOWARE_PROGRAMS_HIST_PRGMID_XREF") %>%
      filter(YEAR_LINK_CREATED == 'C_Outc21' & SURVEY_CODE == 'DACSO') %>%
      collect(),
    by = "PRGM_ID"
  ) %>%
  select(PRGM_ID, PRGM_FIRST_SEEN_SUBM_CD, PRGM_INST_CD, PRGM_LCPC_CD,
         PRGM_INST_PROGRAM_NAME, PRGM_TTRAIN_FLAG, PRGM_CREDENTIAL,
         PRGM_INST_PROGRAM_NAME_CLEANED, NOTES, HAS_HISTORICAL_PRGM_ID_LINK,
         DUP_PROGRAM_USE_THIS_PRGM_ID, CIP_CLUSTER_ARTS_APPLIED,
         DACSO_OLD_PRGM_ID_DO_NOT_USE, LCIP_CD_CIP2016, LCIP_NAME_CIP2016,
         LCIP_LCP4_CD, LCP4_CIP_4DIGITS_NAME, HISTORICAL_PRGM_ID,
         YEAR_LINK_CREATED, SURVEY_CODE)

Updated_DACSO_Programs_in_2021_with_links <- Updated_DACSO_Programs_in_2021_with_links %>%
  inner_join(
    programs_table %>%
      select(PRGM_ID, HISTORICAL_CPC_CD = PRGM_LCPC_CD,
             HISTORICAL_PROGRAM_NAME = PRGM_INST_PROGRAM_NAME,
             HISTORICAL_CIP4_CD = LCIP_LCP4_CD),
    by = c(HISTORICAL_PRGM_ID = "PRGM_ID")
  ) %>%
  mutate(Updated_CPC_Flag = if_else(PRGM_LCPC_CD != HISTORICAL_CPC_CD, 'Yes', NA_character_),
         Updated_CIP_Flag = if_else(LCIP_LCP4_CD != HISTORICAL_CIP4_CD, 'Yes', NA_character_))

DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 <- DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 %>%
  left_join(
    Updated_DACSO_Programs_in_2021_with_links %>%
      mutate(HISTORICAL_CPC_CD = as.character(HISTORICAL_CPC_CD)) %>%
      select(PRGM_INST_CD, HISTORICAL_CPC_CD, HISTORICAL_PROGRAM_NAME, HISTORICAL_CIP4_CD,
             PRGM_LCPC_CD, PRGM_INST_PROGRAM_NAME, LCIP_LCP4_CD, LCP4_CIP_4DIGITS_NAME,
             Updated_DACSO_CPC2021 = Updated_CPC_Flag, Updated_DACSO_CIP2021 = Updated_CIP_Flag),
    by = c(COCI_INST_CD = "PRGM_INST_CD", PRGM_LCPC_CD = "HISTORICAL_CPC_CD",
           PRGM_INST_PROGRAM_NAME = "HISTORICAL_PROGRAM_NAME", CIP_CODE_4 = "HISTORICAL_CIP4_CD")
  ) %>%
  mutate(PRGM_LCPC_CD = ifelse(!is.na(PRGM_LCPC_CD.y), PRGM_LCPC_CD.y, PRGM_LCPC_CD),
         PRGM_INST_PROGRAM_NAME = ifelse(!is.na(PRGM_INST_PROGRAM_NAME.y), PRGM_INST_PROGRAM_NAME.y, PRGM_INST_PROGRAM_NAME),
         CIP_CODE_4 = ifelse(!is.na(LCIP_LCP4_CD), LCIP_LCP4_CD, CIP_CODE_4)) %>%
  mutate(LCP4_CIP_4DIGITS_NAME = ifelse(!is.na(LCP4_CIP_4DIGITS_NAME.y), LCP4_CIP_4DIGITS_NAME.y, LCP4_CIP_4DIGITS_NAME.x),
         .after = "CIP_CODE_4") %>%
  select(-ends_with(".x"), -ends_with(".y"), -LCIP_LCP4_CD)

# ***** manual work needed — review 2021 remaining programs and apply overrides *****
# See original R/02a-dacso-program-matching.R lines 202-222 for manual override logic.
# The analyst reviews specific PRGM_IDs and applies case_when overrides.

# ---- 2022: Programs WITH historical linkages ----
# Same pattern as 2021 — see original lines 225-327 for full logic.
# ***** manual work needed — review 2022 remaining programs *****

# ---- 2023: Programs WITH historical linkages ----
# Same pattern — see original lines 330-415 for full logic.
# ***** manual work needed — review 2023 remaining programs *****

# ---- Add STP matching columns to XWALK ----
DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 <- DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 %>%
  mutate(
    New_STP_Program2021_23 = NA_character_,
    Updated_DACSO_CDTL2021_23 = NA_character_
  )

dbWriteTable(con, "DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23",
             DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23, overwrite = TRUE)


# ******************************************************************************
# PART 2: UPDATE XWALK WITH NEW STP CREDENTIAL DATA
# ******************************************************************************
# WHY: STP credential programs need to be matched to the XWALK to get their CIP
# codes. First, programs already in the XWALK are matched; then new programs are
# auto-matched on program code and description.
#
# Original: ~12 SQL operations (SELECT INTO, ALTER TABLE, UPDATE...FROM JOIN)
# Translated: Pull tables into R, perform sequential left_join + mutate operations.

credential_non_dup <- sch_tbl("Credential_Non_Dup") %>%
  select(PSI_CODE, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
         PSI_CREDENTIAL_CIP, PSI_CREDENTIAL_LEVEL, PSI_CREDENTIAL_CATEGORY, OUTCOMES_CRED) %>%
  collect() |> rename_with(toupper)

xwalk <- DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23

# Pull INFOWARE CIP reference tables
cip6 <- sch_tbl("INFOWARE_L_CIP_6DIGITS_CIP2016") %>% collect() |> rename_with(toupper)
cip4_ref <- sch_tbl("INFOWARE_L_CIP_4DIGITS_CIP2016") %>% collect() |> rename_with(toupper)
cip2_ref <- sch_tbl("INFOWARE_L_CIP_2DIGITS_CIP2016") %>% collect() |> rename_with(toupper)

# ---- Build STP_Credential_Non_Dup_Programs_DACSO ----
# WHY: Create a DACSO-only subset of Credential_Non_Dup, grouped by program attributes.
# Original: SELECT INTO with GROUP BY HAVING
stp_dacso <- credential_non_dup %>%
  filter(OUTCOMES_CRED == "DACSO") %>%
  count(PSI_CODE, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
        PSI_CREDENTIAL_CIP, PSI_CREDENTIAL_LEVEL, PSI_CREDENTIAL_CATEGORY,
        OUTCOMES_CRED, name = "EXPR1") %>%
  # Add empty columns that will be filled by matching steps
  mutate(
    OUTCOMES_CIP_CODE_4 = NA_character_,
    OUTCOMES_CIP_CODE_4_NAME = NA_character_,
    FINAL_CIP_CODE_4 = NA_character_,
    FINAL_CIP_CODE_4_NAME = NA_character_,
    FINAL_CIP_CODE_2 = NA_character_,
    FINAL_CIP_CODE_2_NAME = NA_character_,
    FINAL_CIP_CLUSTER_CODE = NA_character_,
    FINAL_CIP_CLUSTER_NAME = NA_character_,
    STP_CIP_CODE_4 = NA_character_,
    STP_CIP_CODE_4_NAME = NA_character_,
    Already_Matched = NA_character_,
    New_Auto_Match = NA_character_,
    New_Manual_Match = NA_character_,
    COCI_INST_CD = NA_character_
  )

# ---- Add COCI_INST_CD from XWALK ----
# WHY: PSI_CODE and COCI_INST_CD are different code systems for institutions.
# The XWALK maps between them.
psi_to_coci <- xwalk %>%
  filter(!is.na(PSI_CODE) & !is.na(COCI_INST_CD)) %>%
  distinct(PSI_CODE, COCI_INST_CD)

stp_dacso <- stp_dacso %>%
  left_join(psi_to_coci %>% rename(COCI_INST_CD_MAP = COCI_INST_CD), by = "PSI_CODE") %>%
  mutate(COCI_INST_CD = coalesce(COCI_INST_CD_MAP, COCI_INST_CD)) %>%
  select(-COCI_INST_CD_MAP)


# ---- Populate STP_CIP_CODE_4 from INFOWARE ----
# WHY: Match PSI_CREDENTIAL_CIP to the 6-digit CIP taxonomy to get 4-digit codes.
# Original: UPDATE with 3-table JOIN (STP INNER JOIN CIP6 ON CIP, then CIP4 ON LCP4_CD)
cip6_lookup <- cip6 %>%
  select(LCIP_CD_WITH_PERIOD, LCIP_LCP4_CD) %>%
  inner_join(cip4_ref %>% select(LCP4_CD, LCP4_CIP_4DIGITS_NAME),
             by = c("LCIP_LCP4_CD" = "LCP4_CD"))

stp_dacso <- stp_dacso %>%
  left_join(
    cip6_lookup %>% select(LCIP_CD_WITH_PERIOD, LCIP_LCP4_CD, LCP4_CIP_4DIGITS_NAME),
    by = c("PSI_CREDENTIAL_CIP" = "LCIP_CD_WITH_PERIOD")
  ) %>%
  mutate(
    STP_CIP_CODE_4 = coalesce(STP_CIP_CODE_4, LCIP_LCP4_CD),
    STP_CIP_CODE_4_NAME = coalesce(STP_CIP_CODE_4_NAME, LCP4_CIP_4DIGITS_NAME)
  ) %>%
  select(-LCIP_LCP4_CD, -LCP4_CIP_4DIGITS_NAME)

# ---- Already matched programs ----
# WHY: Programs already in the XWALK (matched from previous cycles) inherit
# their CIP codes. Try matching on PSI_CODE first, then COCI_INST_CD.

# Match on PSI_CODE + PSI_PROGRAM_CODE + PSI_CREDENTIAL_PROGRAM_DESCRIPTION
xwalk_exact <- xwalk %>%
  filter(!is.na(PSI_CODE) & !is.na(PSI_PROGRAM_CODE) & !is.na(PSI_CREDENTIAL_PROGRAM_DESC)) %>%
  select(PSI_CODE, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESC, CIP_CODE_4, LCP4_CIP_4DIGITS_NAME)

stp_dacso <- stp_dacso %>%
  left_join(
    xwalk_exact %>% rename(XW_CIP4 = CIP_CODE_4, XW_CIP4_NAME = LCP4_CIP_4DIGITS_NAME),
    by = c("PSI_CODE", "PSI_PROGRAM_CODE" = "PSI_PROGRAM_CODE",
           "PSI_CREDENTIAL_PROGRAM_DESCRIPTION" = "PSI_CREDENTIAL_PROGRAM_DESC")
  ) %>%
  mutate(
    Already_Matched = if_else(!is.na(XW_CIP4) & is.na(Already_Matched), "Yes", Already_Matched),
    OUTCOMES_CIP_CODE_4 = coalesce(OUTCOMES_CIP_CODE_4, XW_CIP4),
    OUTCOMES_CIP_CODE_4_NAME = coalesce(OUTCOMES_CIP_CODE_4_NAME, XW_CIP4_NAME)
  ) %>%
  select(-XW_CIP4, -XW_CIP4_NAME)

# Match on COCI_INST_CD + PSI_PROGRAM_CODE + PSI_CREDENTIAL_PROGRAM_DESCRIPTION
xwalk_coci <- xwalk %>%
  filter(!is.na(COCI_INST_CD) & !is.na(PSI_PROGRAM_CODE) & !is.na(PSI_CREDENTIAL_PROGRAM_DESC)) %>%
  select(COCI_INST_CD, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESC, CIP_CODE_4, LCP4_CIP_4DIGITS_NAME)

stp_dacso <- stp_dacso %>%
  left_join(
    xwalk_coci %>% rename(XW_CIP4 = CIP_CODE_4, XW_CIP4_NAME = LCP4_CIP_4DIGITS_NAME),
    by = c("COCI_INST_CD", "PSI_PROGRAM_CODE" = "PSI_PROGRAM_CODE",
           "PSI_CREDENTIAL_PROGRAM_DESCRIPTION" = "PSI_CREDENTIAL_PROGRAM_DESC")
  ) %>%
  mutate(
    Already_Matched = if_else(is.na(Already_Matched) & is.na(OUTCOMES_CIP_CODE_4) &
                                !is.na(XW_CIP4), "Yes", Already_Matched),
    OUTCOMES_CIP_CODE_4 = if_else(is.na(OUTCOMES_CIP_CODE_4) & !is.na(XW_CIP4), XW_CIP4, OUTCOMES_CIP_CODE_4),
    OUTCOMES_CIP_CODE_4_NAME = if_else(is.na(OUTCOMES_CIP_CODE_4_NAME) & !is.na(XW_CIP4_NAME), XW_CIP4_NAME, OUTCOMES_CIP_CODE_4_NAME)
  ) %>%
  select(-XW_CIP4, -XW_CIP4_NAME)

# ---- Newly matched programs ----
# WHY: Programs not in the XWALK can be matched on DACSO program names/codes.
# The XWALK has PRGM_INST_PROGRAM_NAME and PRGM_LCPC_CD fields from DACSO.

# Match on PSI_CODE + PSI_PROGRAM_CODE=PRGM_LCPC_CD + DESC=PRGM_INST_PROGRAM_NAME
xwalk_new_a <- xwalk %>%
  filter(!is.na(PSI_CODE) & !is.na(PRGM_LCPC_CD) & !is.na(PRGM_INST_PROGRAM_NAME)) %>%
  select(PSI_CODE, PRGM_LCPC_CD, PRGM_INST_PROGRAM_NAME, CIP_CODE_4, LCP4_CIP_4DIGITS_NAME)

stp_dacso <- stp_dacso %>%
  left_join(
    xwalk_new_a %>% rename(XW_CIP4 = CIP_CODE_4, XW_CIP4_NAME = LCP4_CIP_4DIGITS_NAME),
    by = c("PSI_CODE", "PSI_PROGRAM_CODE" = "PRGM_LCPC_CD",
           "PSI_CREDENTIAL_PROGRAM_DESCRIPTION" = "PRGM_INST_PROGRAM_NAME")
  ) %>%
  mutate(
    New_Auto_Match = if_else(is.na(Already_Matched) & !is.na(XW_CIP4), "Yes", New_Auto_Match),
    OUTCOMES_CIP_CODE_4 = if_else(is.na(Already_Matched) & !is.na(XW_CIP4), XW_CIP4, OUTCOMES_CIP_CODE_4),
    OUTCOMES_CIP_CODE_4_NAME = if_else(is.na(Already_Matched) & !is.na(XW_CIP4_NAME), XW_CIP4_NAME, OUTCOMES_CIP_CODE_4_NAME)
  ) %>%
  select(-XW_CIP4, -XW_CIP4_NAME)

# Match on COCI_INST_CD + PSI_PROGRAM_CODE=PRGM_LCPC_CD + DESC=PRGM_INST_PROGRAM_NAME
xwalk_new_a2 <- xwalk %>%
  filter(!is.na(COCI_INST_CD) & !is.na(PRGM_LCPC_CD) & !is.na(PRGM_INST_PROGRAM_NAME)) %>%
  select(COCI_INST_CD, PRGM_LCPC_CD, PRGM_INST_PROGRAM_NAME, CIP_CODE_4, LCP4_CIP_4DIGITS_NAME)

stp_dacso <- stp_dacso %>%
  left_join(
    xwalk_new_a2 %>% rename(XW_CIP4 = CIP_CODE_4, XW_CIP4_NAME = LCP4_CIP_4DIGITS_NAME),
    by = c("COCI_INST_CD", "PSI_PROGRAM_CODE" = "PRGM_LCPC_CD",
           "PSI_CREDENTIAL_PROGRAM_DESCRIPTION" = "PRGM_INST_PROGRAM_NAME")
  ) %>%
  mutate(
    New_Auto_Match = if_else(is.na(OUTCOMES_CIP_CODE_4) & is.na(OUTCOMES_CIP_CODE_4_NAME) &
                               is.na(New_Auto_Match) & is.na(Already_Matched) & !is.na(XW_CIP4),
                             "Yes", New_Auto_Match),
    OUTCOMES_CIP_CODE_4 = if_else(is.na(OUTCOMES_CIP_CODE_4) & !is.na(XW_CIP4), XW_CIP4, OUTCOMES_CIP_CODE_4),
    OUTCOMES_CIP_CODE_4_NAME = if_else(is.na(OUTCOMES_CIP_CODE_4_NAME) & !is.na(XW_CIP4_NAME), XW_CIP4_NAME, OUTCOMES_CIP_CODE_4_NAME)
  ) %>%
  select(-XW_CIP4, -XW_CIP4_NAME)

# ---- Update XWALK with newly matched STP programs ----
# WHY: When new STP programs are matched to DACSO entries in the XWALK, we update
# the XWALK with the STP program info so future runs can find them as "already matched".

# Get the newly matched STP programs
newly_matched <- stp_dacso %>%
  filter(New_Auto_Match == "Yes") %>%
  select(PSI_CODE, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
         STP_CIP_CODE_4, STP_CIP_CODE_4_NAME)

# Update XWALK for PSI_CODE matches
DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 <- DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 %>%
  left_join(
    newly_matched %>%
      rename(XW_STP_PGM_CODE = PSI_PROGRAM_CODE, XW_STP_PGM_DESC = PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
             XW_STP_CIP4 = STP_CIP_CODE_4, XW_STP_CIP4_NAME = STP_CIP_CODE_4_NAME),
    by = c("PSI_CODE", "PRGM_LCPC_CD" = "PSI_PROGRAM_CODE",
           "PRGM_INST_PROGRAM_NAME" = "PSI_CREDENTIAL_PROGRAM_DESCRIPTION")
  ) %>%
  mutate(
    PSI_PROGRAM_CODE = if_else(!is.na(XW_STP_PGM_CODE), XW_STP_PGM_CODE, PSI_PROGRAM_CODE),
    PSI_CREDENTIAL_PROGRAM_DESC = if_else(!is.na(XW_STP_PGM_DESC), XW_STP_PGM_DESC, PSI_CREDENTIAL_PROGRAM_DESC),
    STP_CIP4_CODE = if_else(!is.na(XW_STP_CIP4), XW_STP_CIP4, STP_CIP4_CODE),
    STP_CIP4_NAME = if_else(!is.na(XW_STP_CIP4_NAME), XW_STP_CIP4_NAME, STP_CIP4_NAME),
    New_STP_Program2021_23 = if_else(!is.na(XW_STP_PGM_CODE), "Yes", New_STP_Program2021_23),
    One_To_One_Match = if_else(!is.na(XW_STP_PGM_CODE), "Yes2021_23", One_To_One_Match)
  ) %>%
  select(-starts_with("XW_STP"))

# Update XWALK for COCI_INST_CD matches (only rows not yet updated)
DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 <- DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 %>%
  left_join(
    newly_matched %>%
      rename(XW_STP_PGM_CODE = PSI_PROGRAM_CODE, XW_STP_PGM_DESC = PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
             XW_STP_CIP4 = STP_CIP_CODE_4, XW_STP_CIP4_NAME = STP_CIP_CODE_4_NAME),
    by = c("COCI_INST_CD" = "PSI_CODE", "PRGM_LCPC_CD" = "PSI_PROGRAM_CODE",
           "PRGM_INST_PROGRAM_NAME" = "PSI_CREDENTIAL_PROGRAM_DESCRIPTION")
  ) %>%
  mutate(
    PSI_PROGRAM_CODE = if_else(is.na(New_STP_Program2021_23) & !is.na(XW_STP_PGM_CODE),
                                XW_STP_PGM_CODE, PSI_PROGRAM_CODE),
    PSI_CREDENTIAL_PROGRAM_DESC = if_else(is.na(New_STP_Program2021_23) & !is.na(XW_STP_PGM_DESC),
                                           XW_STP_PGM_DESC, PSI_CREDENTIAL_PROGRAM_DESC),
    STP_CIP4_CODE = if_else(is.na(New_STP_Program2021_23) & !is.na(XW_STP_CIP4),
                             XW_STP_CIP4, STP_CIP4_CODE),
    STP_CIP4_NAME = if_else(is.na(New_STP_Program2021_23) & !is.na(XW_STP_CIP4_NAME),
                             XW_STP_CIP4_NAME, STP_CIP4_NAME),
    New_STP_Program2021_23 = if_else(is.na(New_STP_Program2021_23) & !is.na(XW_STP_PGM_CODE),
                                      "Yes", New_STP_Program2021_23),
    One_To_One_Match = if_else(is.na(One_To_One_Match) & !is.na(XW_STP_PGM_CODE),
                                "Yes2021_23", One_To_One_Match)
  ) %>%
  select(-starts_with("XW_STP"))

rm(newly_matched)


# ******************************************************************************
# PART 3: INSTITUTION-SPECIFIC CUSTOM MATCHING
# ******************************************************************************
# WHY: Some institutions use different program code formats in STP vs DACSO.
# BCIT includes credential suffixes, CAPU uses different code lengths, and VIU
# wraps codes in credential-category prefixes. We extract the DACSO-compatible
# portion and match on that.

# Find unmatched STP programs
stp_unmatched <- stp_dacso %>%
  filter((is.na(OUTCOMES_CIP_CODE_4) | is.na(OUTCOMES_CIP_CODE_4_NAME)) &
         is.na(Already_Matched) & is.na(New_Auto_Match))

# ---- BCIT matching ----
# WHY: BCIT submits CPC codes with credential abbreviation suffixes (e.g., _TTDIPL)
# but DACSO codes don't have the suffix. Take first 4 characters of PSI_PROGRAM_CODE.
stp_dacso <- stp_dacso %>%
  mutate(BCIT_TEST_PROGRAM_CODE = if_else(PSI_CODE == "BCIT",
                                            substr(PSI_PROGRAM_CODE, 1, 4), NA_character_))

# Helper function: match STP to XWALK on test code
match_on_test_code <- function(stp_df, xwalk_df, test_col, match_flag,
                               join_cols_desc = TRUE) {
  join_by <- if (join_cols_desc) {
    by <- c("COCI_INST_CD", test_col = "PRGM_LCPC_CD",
            "PSI_CREDENTIAL_PROGRAM_DESCRIPTION" = "PRGM_INST_PROGRAM_NAME")
  } else {
    by <- c("COCI_INST_CD", test_col = "PRGM_LCPC_CD")
  }

  stp_df %>%
    left_join(
      xwalk_df %>%
        filter(!is.na(COCI_INST_CD) & !is.na(PRGM_LCPC_CD)) %>%
        select(COCI_INST_CD, PRGM_LCPC_CD, PRGM_INST_PROGRAM_NAME, CIP_CODE_4, LCP4_CIP_4DIGITS_NAME),
      by = by
    ) %>%
    mutate(
      New_Auto_Match = if_else(
        is.na(OUTCOMES_CIP_CODE_4) & is.na(OUTCOMES_CIP_CODE_4_NAME) & is.na(New_Auto_Match) &
          !is.na(CIP_CODE_4),
        match_flag, New_Auto_Match
      ),
      OUTCOMES_CIP_CODE_4 = if_else(
        is.na(OUTCOMES_CIP_CODE_4) & !is.na(CIP_CODE_4), CIP_CODE_4, OUTCOMES_CIP_CODE_4
      ),
      OUTCOMES_CIP_CODE_4_NAME = if_else(
        is.na(OUTCOMES_CIP_CODE_4_NAME) & !is.na(LCP4_CIP_4DIGITS_NAME),
        LCP4_CIP_4DIGITS_NAME, OUTCOMES_CIP_CODE_4_NAME
      )
    ) %>%
    select(-CIP_CODE_4, -LCP4_CIP_4DIGITS_NAME, -PRGM_INST_PROGRAM_NAME)
}

# BCIT: match with description
stp_dacso <- match_on_test_code(stp_dacso, xwalk, "BCIT_TEST_PROGRAM_CODE",
                                 "Yes2021_23BCIT", join_cols_desc = TRUE)
# BCIT: match without description (code only)
stp_dacso <- match_on_test_code(stp_dacso, xwalk, "BCIT_TEST_PROGRAM_CODE",
                                 "Yes2021_23BCIT", join_cols_desc = FALSE)

# ---- CAPU matching ----
# WHY: CAPU codes are 6 digits in STP but 3-4 digits in DACSO. Some also have
# dash suffixes. Try multiple code lengths.
stp_dacso <- stp_dacso %>%
  mutate(CAP_TEST_PROGRAM_CODE = if_else(
    COCI_INST_CD == "CAPU" & grepl("-", PSI_PROGRAM_CODE),
    substr(PSI_PROGRAM_CODE, 1, regexpr("-", PSI_PROGRAM_CODE, fixed = TRUE) - 1),
    NA_character_
  ))

# Match with dash-removed codes + description
stp_dacso <- match_on_test_code(stp_dacso, xwalk, "CAP_TEST_PROGRAM_CODE",
                                 "Yes2021_23CAPU", join_cols_desc = TRUE)
stp_dacso <- match_on_test_code(stp_dacso, xwalk, "CAP_TEST_PROGRAM_CODE",
                                 "Yes2021_23CAPU", join_cols_desc = FALSE)

# Try 4-digit prefix
stp_dacso <- stp_dacso %>%
  mutate(CAP_TEST_PROGRAM_CODE = if_else(COCI_INST_CD == "CAPU",
                                           substr(PSI_PROGRAM_CODE, 1, 4), CAP_TEST_PROGRAM_CODE))

stp_dacso <- match_on_test_code(stp_dacso, xwalk, "CAP_TEST_PROGRAM_CODE",
                                 "Yes2021_23CAPU", join_cols_desc = TRUE)
stp_dacso <- match_on_test_code(stp_dacso, xwalk, "CAP_TEST_PROGRAM_CODE",
                                 "Yes2021_23CAPU", join_cols_desc = FALSE)

# Try 3-digit prefix
stp_dacso <- stp_dacso %>%
  mutate(CAP_TEST_PROGRAM_CODE = if_else(COCI_INST_CD == "CAPU",
                                           substr(PSI_PROGRAM_CODE, 1, 3), CAP_TEST_PROGRAM_CODE))

stp_dacso <- match_on_test_code(stp_dacso, xwalk, "CAP_TEST_PROGRAM_CODE",
                                 "Yes2021_23CAPU", join_cols_desc = TRUE)
stp_dacso <- match_on_test_code(stp_dacso, xwalk, "CAP_TEST_PROGRAM_CODE",
                                 "Yes2021_23CAPU", join_cols_desc = FALSE)

# ---- VIU matching ----
# WHY: VIU codes in STP are like "CERT-WELDM_01" but DACSO just has "WELDM".
# Extract the substring between "-" and "_".
stp_dacso <- stp_dacso %>%
  mutate(VIU_TEST_PROGRAM_CODE = if_else(
    PSI_CODE == "VIU" & grepl("-", PSI_PROGRAM_CODE) & grepl("_", PSI_PROGRAM_CODE),
    substr(PSI_PROGRAM_CODE,
           regexpr("-", PSI_PROGRAM_CODE, fixed = TRUE) + 1,
           regexpr("_", PSI_PROGRAM_CODE, fixed = TRUE) - 1),
    NA_character_
  ))

stp_dacso <- match_on_test_code(stp_dacso, xwalk, "VIU_TEST_PROGRAM_CODE",
                                 "Yes2021_23VIU", join_cols_desc = TRUE)
stp_dacso <- match_on_test_code(stp_dacso, xwalk, "VIU_TEST_PROGRAM_CODE",
                                 "Yes2021_23VIU", join_cols_desc = FALSE)

# ---- Remaining catch-all matching ----
# WHY: Try matching remaining unmatched programs on COCI_INST_CD + program code,
# then on COCI_INST_CD + program description.

# Match on COCI_INST_CD + PSI_PROGRAM_CODE=PRGM_LCPC_CD
xwalk_remaining <- xwalk %>%
  filter(!is.na(COCI_INST_CD) & !is.na(PRGM_LCPC_CD)) %>%
  select(COCI_INST_CD, PRGM_LCPC_CD, CIP_CODE_4, LCP4_CIP_4DIGITS_NAME)

stp_dacso <- stp_dacso %>%
  left_join(
    xwalk_remaining %>% rename(XW_CIP4 = CIP_CODE_4, XW_CIP4_NAME = LCP4_CIP_4DIGITS_NAME),
    by = c("COCI_INST_CD", "PSI_PROGRAM_CODE" = "PRGM_LCPC_CD")
  ) %>%
  mutate(
    New_Auto_Match = if_else(
      is.na(OUTCOMES_CIP_CODE_4) & is.na(OUTCOMES_CIP_CODE_4_NAME) & is.na(New_Auto_Match) &
        !is.na(XW_CIP4), "Yes_2021_23test", New_Auto_Match
    ),
    OUTCOMES_CIP_CODE_4 = if_else(is.na(OUTCOMES_CIP_CODE_4) & !is.na(XW_CIP4), XW_CIP4, OUTCOMES_CIP_CODE_4),
    OUTCOMES_CIP_CODE_4_NAME = if_else(is.na(OUTCOMES_CIP_CODE_4_NAME) & !is.na(XW_CIP4_NAME), XW_CIP4_NAME, OUTCOMES_CIP_CODE_4_NAME)
  ) %>%
  select(-XW_CIP4, -XW_CIP4_NAME)

# Match on COCI_INST_CD + PSI_CREDENTIAL_PROGRAM_DESCRIPTION=PRGM_INST_PROGRAM_NAME
xwalk_remaining_desc <- xwalk %>%
  filter(!is.na(COCI_INST_CD) & !is.na(PRGM_INST_PROGRAM_NAME)) %>%
  select(COCI_INST_CD, PRGM_INST_PROGRAM_NAME, CIP_CODE_4, LCP4_CIP_4DIGITS_NAME)

stp_dacso <- stp_dacso %>%
  left_join(
    xwalk_remaining_desc %>% rename(XW_CIP4 = CIP_CODE_4, XW_CIP4_NAME = LCP4_CIP_4DIGITS_NAME),
    by = c("COCI_INST_CD", "PSI_CREDENTIAL_PROGRAM_DESCRIPTION" = "PRGM_INST_PROGRAM_NAME")
  ) %>%
  mutate(
    New_Auto_Match = if_else(
      is.na(OUTCOMES_CIP_CODE_4) & is.na(OUTCOMES_CIP_CODE_4_NAME) & is.na(New_Auto_Match) &
        !is.na(XW_CIP4), "Yes_2021_23test", New_Auto_Match
    ),
    OUTCOMES_CIP_CODE_4 = if_else(is.na(OUTCOMES_CIP_CODE_4) & !is.na(XW_CIP4), XW_CIP4, OUTCOMES_CIP_CODE_4),
    OUTCOMES_CIP_CODE_4_NAME = if_else(is.na(OUTCOMES_CIP_CODE_4_NAME) & !is.na(XW_CIP4_NAME), XW_CIP4_NAME, OUTCOMES_CIP_CODE_4_NAME)
  ) %>%
  select(-XW_CIP4, -XW_CIP4_NAME)

rm(xwalk_remaining, xwalk_remaining_desc)


# ******************************************************************************
# PART 4: FINAL UPDATE TO STP CIPS
# ******************************************************************************
# WHY: Compute final CIP codes. Matched programs use the outcomes CIP; unmatched
# programs use STP CIP from the INFOWARE taxonomy. Then fill 2-digit CIP and
# cluster codes from the 4-digit code.
#
# Original: ~8 SQL operations (UPDATE, SELECT INTO, DROP TABLE)
# Translated: Sequential mutate + left_join operations in memory.

# Step 1: Where outcomes CIP exists, use it as final
stp_dacso <- stp_dacso %>%
  mutate(
    FINAL_CIP_CODE_4 = if_else(!is.na(OUTCOMES_CIP_CODE_4) & !is.na(OUTCOMES_CIP_CODE_4_NAME),
                                OUTCOMES_CIP_CODE_4, FINAL_CIP_CODE_4),
    FINAL_CIP_CODE_4_NAME = if_else(!is.na(OUTCOMES_CIP_CODE_4) & !is.na(OUTCOMES_CIP_CODE_4_NAME),
                                     OUTCOMES_CIP_CODE_4_NAME, FINAL_CIP_CODE_4_NAME)
  )

# Step 2: Where no outcomes match, use INFOWARE to derive CIP from PSI_CREDENTIAL_CIP
# WHY: This fills FINAL_CIP for programs that weren't matched to DACSO outcomes.
# It uses the full CIP hierarchy (6-digit → 4-digit → 2-digit → names → cluster).
cip6_full <- cip6 %>%
  select(LCIP_CD_WITH_PERIOD, LCIP_LCP4_CD, LCIP_LCP2_CD, LCIP_LCIPPC_CD, LCIP_LCIPPC_NAME) %>%
  inner_join(cip4_ref %>% select(LCP4_CD, LCP4_CIP_4DIGITS_NAME), by = c("LCIP_LCP4_CD" = "LCP4_CD")) %>%
  inner_join(cip2_ref %>% select(LCP2_CD, LCP2_DIGITS_NAME), by = c("LCIP_LCP2_CD" = "LCP2_CD"))

stp_dacso <- stp_dacso %>%
  left_join(
    cip6_full %>% select(LCIP_CD_WITH_PERIOD, LCIP_LCP4_CD, LCP4_CIP_4DIGITS_NAME,
                          LCIP_LCP2_CD, LCP2_DIGITS_NAME),
    by = c("PSI_CREDENTIAL_CIP" = "LCIP_CD_WITH_PERIOD")
  ) %>%
  mutate(
    FINAL_CIP_CODE_4 = if_else(
      is.na(FINAL_CIP_CODE_4) & is.na(FINAL_CIP_CODE_4_NAME) & is.na(FINAL_CIP_CODE_2) &
        is.na(FINAL_CIP_CODE_2_NAME) & is.na(OUTCOMES_CIP_CODE_4),
      LCIP_LCP4_CD, FINAL_CIP_CODE_4
    ),
    FINAL_CIP_CODE_4_NAME = if_else(
      is.na(FINAL_CIP_CODE_4_NAME) & is.na(FINAL_CIP_CODE_2) & is.na(FINAL_CIP_CODE_2_NAME) &
        is.na(OUTCOMES_CIP_CODE_4),
      LCP4_CIP_4DIGITS_NAME, FINAL_CIP_CODE_4_NAME
    ),
    FINAL_CIP_CODE_2 = if_else(
      is.na(FINAL_CIP_CODE_2) & is.na(FINAL_CIP_CODE_2_NAME) & is.na(OUTCOMES_CIP_CODE_4),
      LCIP_LCP2_CD, FINAL_CIP_CODE_2
    ),
    FINAL_CIP_CODE_2_NAME = if_else(
      is.na(FINAL_CIP_CODE_2_NAME) & is.na(OUTCOMES_CIP_CODE_4),
      LCP2_DIGITS_NAME, FINAL_CIP_CODE_2_NAME
    )
  ) %>%
  select(-LCIP_LCP4_CD, -LCP4_CIP_4DIGITS_NAME, -LCIP_LCP2_CD, -LCP2_DIGITS_NAME)

# Step 3: Fill remaining NULL FINAL_CIP columns from the 4-digit code
# WHY: Some records got FINAL_CIP_CODE_4 from Step 1 (outcomes) but still need
# 4-digit name, 2-digit code, 2-digit name, and cluster codes.
cip4_lookup <- cip6 %>%
  select(LCIP_LCP4_CD, LCIP_LCP2_CD) %>%
  distinct() %>%
  inner_join(cip4_ref %>% select(LCP4_CD, LCP4_CIP_4DIGITS_NAME), by = c("LCIP_LCP4_CD" = "LCP4_CD")) %>%
  inner_join(cip2_ref %>% select(LCP2_CD, LCP2_DIGITS_NAME), by = c("LCIP_LCP2_CD" = "LCP2_CD"))

stp_dacso <- stp_dacso %>%
  left_join(
    cip4_lookup, by = c("FINAL_CIP_CODE_4" = "LCIP_LCP4_CD")
  ) %>%
  mutate(
    FINAL_CIP_CODE_4_NAME = coalesce(FINAL_CIP_CODE_4_NAME, LCP4_CIP_4DIGITS_NAME),
    FINAL_CIP_CODE_2 = coalesce(FINAL_CIP_CODE_2, LCIP_LCP2_CD),
    FINAL_CIP_CODE_2_NAME = coalesce(FINAL_CIP_CODE_2_NAME, LCP2_DIGITS_NAME)
  ) %>%
  select(-LCIP_LCP2_CD, -LCP4_CIP_4DIGITS_NAME, -LCP2_DIGITS_NAME)

# Step 4: Fill FINAL_CIP_CLUSTER from FINAL_CIP_CODE_4
# WHY: The 6-digit CIP taxonomy maps each 4-digit code to a cluster code.
cip6_cluster <- cip6 %>%
  select(LCIP_LCP4_CD, LCIP_LCIPPC_CD, LCIP_LCIPPC_NAME) %>%
  distinct()

stp_dacso <- stp_dacso %>%
  left_join(cip6_cluster, by = c("FINAL_CIP_CODE_4" = "LCIP_LCP4_CD")) %>%
  mutate(
    FINAL_CIP_CLUSTER_CODE = coalesce(FINAL_CIP_CLUSTER_CODE, LCIP_LCIPPC_CD),
    FINAL_CIP_CLUSTER_NAME = coalesce(FINAL_CIP_CLUSTER_NAME, LCIP_LCIPPC_NAME)
  ) %>%
  select(-LCIP_LCIPPC_CD, -LCIP_LCIPPC_NAME)

# Step 5: Fill FINAL_CIP_CODE_2_NAME from 2-digit lookup
stp_dacso <- stp_dacso %>%
  left_join(
    cip2_ref %>% select(LCP2_CD, LCP2_DIGITS_NAME),
    by = c("FINAL_CIP_CODE_2" = "LCP2_CD")
  ) %>%
  mutate(FINAL_CIP_CODE_2_NAME = coalesce(FINAL_CIP_CODE_2_NAME, LCP2_DIGITS_NAME)) %>%
  select(-LCP2_DIGITS_NAME)

# ---- Review CIP changes ----
# WHY: Diagnostic — shows programs where the final CIP differs from the original
# STP CIP. Useful for catching incorrect matches.
review_changed_cips <- stp_dacso %>%
  filter(FINAL_CIP_CODE_4 != STP_CIP_CODE_4)

# ---- Write final output tables ----
dbWriteTable(con, "STP_Credential_Non_Dup_Programs_DACSO", stp_dacso, overwrite = TRUE)
dbWriteTable(con, "Credential_Non_Dup_Programs_DACSO_FinalCIPS", stp_dacso, overwrite = TRUE)
dbWriteTable(con, "DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23",
             DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23, overwrite = TRUE)

dbDisconnect(con)
