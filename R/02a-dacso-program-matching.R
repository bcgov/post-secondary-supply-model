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

# ******************************************************************************
# Aligns CIP codes between DACSO and STP data
#
# Required Tables
#   DACSO_STP_ProgramsCIP4_XWALK_ALL_20XX (previous PSSM XWALK)
#   INFOWARE_PROGRAMS
#   INFOWARE_L_CIP_6DIGITS_CIP2016
#   INFOWARE_L_CIP_4DIGITS_CIP2016
#   INFOWARE_L_CIP_2DIGITS_CIP2016
#   INFOWARE_PROGRAMS_HIST_PRGMID_XREF
#   Credential_Non_Dup
#
# Resulting Tables
#   Credential_Non_Dup_Programs_DACSO_FinalCIPS
#   DACSO_STP_ProgramsCIP4_XWALK_ALL_20XX (current PSSM XWALK)
#
# STEPS:
# Setup: import and save required tables
#
# Part 1: Add DACSO programs to XWALK
# Part 2: Add STP programs to XWALK
# Part 3: Manual/custom STP to XWALK matching
# Part 4: Final update to STP CIPs
#
# ******************************************************************************

library(tidyverse)
library(RODBC)
library(config)
library(glue)
library(odbc)
library(RJDBC) ## loads DBI

# Setup ----

lan <- config::get("lan")
sch_tbl <- function(name) {
  tbl(con, dbplyr::in_schema(my_schema, name))
}

# -----------------------------------------------------------------------------
# Helper functions for SQL-like joins on character business keys
# -----------------------------------------------------------------------------
# SQL Server usually compares character join fields case-insensitively.
# R joins are case-sensitive. To make the translation safer and easier to audit,
# we create explicit *_KEY columns for the business keys that are most likely to
# differ by case or extra spaces.

norm_chr <- function(x) {
  x <- as.character(x)
  x <- trimws(x)
  toupper(x)
}

# Add one or more normalized join-key columns to a data frame.
# mappings is a named character vector:
#   new_key_name = existing_column_name
add_join_keys <- function(df, mappings) {
  if (length(mappings) == 0) {
    return(df)
  }

  for (new_nm in names(mappings)) {
    src_nm <- unname(mappings[[new_nm]])
    if (src_nm %in% names(df)) {
      df[[new_nm]] <- norm_chr(df[[src_nm]])
    }
  }

  df
}

# Remove helper key columns before writing final outputs.
drop_join_keys <- function(df) {
  df %>% select(-matches("_KEY$"))
}

# Standard key sets used repeatedly in this script.
refresh_programs_join_keys <- function(df) {
  add_join_keys(
    df,
    c(
      PRGM_INST_CD_KEY = "PRGM_INST_CD",
      PRGM_LCPC_CD_KEY = "PRGM_LCPC_CD",
      PRGM_INST_PROGRAM_NAME_KEY = "PRGM_INST_PROGRAM_NAME"
    )
  )
}

refresh_xwalk_join_keys <- function(df) {
  add_join_keys(
    df,
    c(
      PSI_CODE_KEY = "PSI_CODE",
      COCI_INST_CD_KEY = "COCI_INST_CD",
      PSI_PROGRAM_CODE_KEY = "PSI_PROGRAM_CODE",
      PRGM_LCPC_CD_KEY = "PRGM_LCPC_CD",
      PSI_CREDENTIAL_PROGRAM_DESC_KEY = "PSI_CREDENTIAL_PROGRAM_DESC",
      PRGM_INST_PROGRAM_NAME_KEY = "PRGM_INST_PROGRAM_NAME"
    )
  )
}

refresh_stp_join_keys <- function(df) {
  add_join_keys(
    df,
    c(
      PSI_CODE_KEY = "PSI_CODE",
      COCI_INST_CD_KEY = "COCI_INST_CD",
      PSI_PROGRAM_CODE_KEY = "PSI_PROGRAM_CODE",
      PSI_CREDENTIAL_PROGRAM_DESCRIPTION_KEY = "PSI_CREDENTIAL_PROGRAM_DESCRIPTION"
    )
  )
}


## ---- Read in INFOWARE tables ----
# iw_config <- config::get("infoware")
# jdbc_config <- config::get("jdbc")

# jdbcDriver <- JDBC(jdbc_config$class, classPath = jdbc_config$path)

# iw_con <- dbConnect(
#   jdbcDriver,
#   iw_config$database,
#   iw_config$uid,
#   iw_config$pwd
# )
# new way to load data from infoware
# iw_config <- config::get("infoware")

# # odbcListDrivers()

# iw_con <- dbConnect(
#   odbc::odbc(),
#   Driver = "Oracle in instantclient_19_30",
#   DBQ = "DEV01.world",
#   UID = iw_config$uid,
#   PWD = iw_config$pwd
# )
# all the comment-outed data steps are only running for once.
# now infoware has INFOWARE.L_CIP_6DIGITS_CIP2021 table. We may need to update soon.

# INFOWARE_PROGRAMS <- dbReadTable(iw_con, DBI::SQL("INFOWARE.PROGRAMS"))
# INFOWARE_PROGRAMS_2016 <- dbReadTable(iw_con, DBI::SQL("INFOWARE.PROGRAMS_BKUP_NOV_2024_CIP2016_CIP2021"))
# INFOWARE_L_CIP_6DIGITS_CIP2016 <- dbReadTable(
#   iw_con,
#   DBI::SQL("INFOWARE.L_CIP_6DIGITS_CIP2016")
# )
# INFOWARE_L_CIP_4DIGITS_CIP2016 <- dbReadTable(
#   iw_con,
#   DBI::SQL("INFOWARE.L_CIP_4DIGITS_CIP2016")
# )
# INFOWARE_L_CIP_2DIGITS_CIP2016 <- dbReadTable(
#   iw_con,
#   DBI::SQL("INFOWARE.L_CIP_2DIGITS_CIP2016")
# )
# INFOWARE_PROGRAMS_HIST_PRGMID_XREF <- dbReadTable(
#   iw_con,
#   DBI::SQL("INFOWARE.PROGRAMS_HIST_PRGMID_XREF")
# )

# dbDisconnect(iw_con)

# ## ---- Read in last years XWALK ----
# ## connect to outcomes (access) database
# # not working now
# connection <- config::get("connection")$outcomes_dacso
# acc_con <- odbcDriverConnect(connection)

# DACSO_STP_ProgramsCIP4_XWALK_ALL_2020 <- sqlQuery(
#   acc_con,
#   "SELECT * FROM DACSO_STP_ProgramsCIP4_XWALK_ALL_2020;"
# )

# odbcClose(acc_con)

## ---- Connect to Decimal ----
config <- config::get("decimal")
con <- dbConnect(
  odbc(),
  Driver = config$driver,
  Server = config$server,
  Database = config$database,
  Trusted_Connection = "True"
)
my_schema <- config::get("myschema")

# ## ---- Write initial tables to Decimal ----
# ## Save static versions of the INFOWARE tables and last cycle XWALK to Decimal
# # this one is missing
# dbWriteTable(
#   con,
#   SQL(glue::glue('"{my_schema}"."DACSO_STP_ProgramsCIP4_XWALK_ALL_2020"')),
#   DACSO_STP_ProgramsCIP4_XWALK_ALL_2020
# )
# dbWriteTable(
#   con,
#   SQL(glue::glue('"{my_schema}"."INFOWARE_PROGRAMS"')),
#   INFOWARE_PROGRAMS
# )
# dbWriteTable(
#   con,
#   SQL(glue::glue('"{my_schema}"."INFOWARE_PROGRAMS_2016"')),
#   INFOWARE_PROGRAMS_2016
# )
# dbWriteTable(
#   con,
#   SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_6DIGITS_CIP2016"')),
#   INFOWARE_L_CIP_6DIGITS_CIP2016
# )
# dbWriteTable(
#   con,
#   SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_4DIGITS_CIP2016"')),
#   INFOWARE_L_CIP_4DIGITS_CIP2016
# )
# dbWriteTable(
#   con,
#   SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_2DIGITS_CIP2016"')),
#   INFOWARE_L_CIP_2DIGITS_CIP2016
# )
# dbWriteTable(
#   con,
#   SQL(glue::glue('"{my_schema}"."INFOWARE_PROGRAMS_HIST_PRGMID_XREF"')),
#   INFOWARE_PROGRAMS_HIST_PRGMID_XREF
# )

# ## remove tables and use decimal versions for remainder of code
# rm(
#   DACSO_STP_ProgramsCIP4_XWALK_ALL_2020,
#   INFOWARE_PROGRAMS,
#   INFOWARE_L_CIP_6DIGITS_CIP2016,
#   INFOWARE_L_CIP_4DIGITS_CIP2016,
#   INFOWARE_L_CIP_2DIGITS_CIP2016,
#   INFOWARE_PROGRAMS_HIST_PRGMID_XREF
# )

# Part 1 ----

## ---- Create programs_table from combining INFOWARE tables ----
## define programs_table from which to grab new programs (with and without historical linkages)
# INFOWARE_PROGRAMS does not have LCIP_CD_CIP2016 anymore, instead, it has LCIP_CD_CIP2021
# so we use the backup version of the PROGRAMS table which has the LCIP_CD_CIP2016 column
programs_table <- tbl(con, "INFOWARE_PROGRAMS_2016") %>%
  inner_join(
    tbl(con, "INFOWARE_L_CIP_6DIGITS_CIP2016"),
    by = c("LCIP_CD_CIP2016" = "LCIP_CD")
  ) %>%
  inner_join(
    tbl(con, "INFOWARE_L_CIP_4DIGITS_CIP2016"),
    by = c("LCIP_LCP4_CD" = "LCP4_CD")
  ) %>%
  select(
    PRGM_ID,
    PRGM_FIRST_SEEN_SUBM_CD,
    PRGM_INST_CD,
    PRGM_INST_PROGRAM_NAME,
    PRGM_INST_PROGRAM_NAME_CLEANED,
    PRGM_LCPC_CD,
    PRGM_TTRAIN_FLAG,
    LCIP_CD_CIP2016,
    LCIP_NAME_CIP2016,
    PRGM_CREDENTIAL,
    NOTES,
    HAS_HISTORICAL_PRGM_ID_LINK,
    CIP_CLUSTER_ARTS_APPLIED,
    DACSO_OLD_PRGM_ID_DO_NOT_USE,
    DUP_PROGRAM_USE_THIS_PRGM_ID,
    LCIP_LCP4_CD,
    LCP4_CIP_4DIGITS_NAME
  ) %>%
  collect() %>%
  refresh_programs_join_keys()

## ---- Make new XWALK from last years XWALK ----
DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 <- tbl(
  con,
  "DACSO_STP_ProgramsCIP4_XWALK_ALL_2020"
) %>%
  collect() %>%
  mutate(
    CIP_CODE_4 = str_pad(CIP_CODE_4, width = 4, side = "left", pad = "0")
  ) %>%
  refresh_xwalk_join_keys()

## ---- Add to XWALK: New DACSO prgms WITHOUT historical linkages ----
## review the HAS_HISTORICAL_PRGM_ID_LINK values
programs_table %>%
  filter(PRGM_FIRST_SEEN_SUBM_CD %in% c('C_Outc21', 'C_Outc22', 'C_Outc23')) %>%
  group_by(PRGM_FIRST_SEEN_SUBM_CD, HAS_HISTORICAL_PRGM_ID_LINK) %>%
  tally()

new_dacso_programs_21_23 <- programs_table %>%
  filter(
    PRGM_FIRST_SEEN_SUBM_CD %in%
      c('C_Outc21', 'C_Outc22', 'C_Outc23') &
      (is.na(HAS_HISTORICAL_PRGM_ID_LINK) | HAS_HISTORICAL_PRGM_ID_LINK == " ")
  )
new_dacso_programs_21_23 %>% count(PRGM_FIRST_SEEN_SUBM_CD)

DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 <- DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 %>%
  bind_rows(
    programs_table %>%
      filter(
        PRGM_FIRST_SEEN_SUBM_CD %in%
          c('C_Outc21', 'C_Outc22', 'C_Outc23') &
          (is.na(HAS_HISTORICAL_PRGM_ID_LINK) |
            HAS_HISTORICAL_PRGM_ID_LINK == " ")
      ) %>%
      mutate(
        New_DACSO_Program2021_23 = case_when(
          PRGM_FIRST_SEEN_SUBM_CD == "C_Outc21" ~ "Yes2021",
          PRGM_FIRST_SEEN_SUBM_CD == "C_Outc22" ~ "Yes2022",
          PRGM_FIRST_SEEN_SUBM_CD == "C_Outc23" ~ "Yes2023"
        )
      ) %>%
      select(
        COCI_INST_CD = PRGM_INST_CD, #why change name?
        PRGM_LCPC_CD,
        PRGM_INST_PROGRAM_NAME,
        CIP_CODE_4 = LCIP_LCP4_CD,
        LCP4_CIP_4DIGITS_NAME,
        PRGM_ID,
        PRGM_CREDENTIAL,
        New_DACSO_Program2021_23
      )
  )

# The following steps are repeated for each year since last PSSM:
##  Find DACSO prgms WITH historical linkages
##  Get historical linkages for DACSO prgms
##  Update to XWALK: Updated DACSO programs WITH historical linkages
##  Find Remaining updated DACSO missing from XWALK for match to STP program

## ---- 2021 Find DACSO prgms WITH historical linkages ----
Updated_DACSO_Programs_in_2021_with_links <- programs_table %>%
  filter(
    PRGM_FIRST_SEEN_SUBM_CD == 'C_Outc21' & HAS_HISTORICAL_PRGM_ID_LINK == 'Y'
  ) %>%
  inner_join(
    tbl(con, "INFOWARE_PROGRAMS_HIST_PRGMID_XREF") %>%
      filter(YEAR_LINK_CREATED == 'C_Outc21' & SURVEY_CODE == 'DACSO') %>%
      collect(),
    by = "PRGM_ID"
  ) %>%
  select(
    PRGM_ID,
    PRGM_FIRST_SEEN_SUBM_CD,
    PRGM_INST_CD,
    PRGM_LCPC_CD,
    PRGM_INST_PROGRAM_NAME,
    PRGM_TTRAIN_FLAG,
    PRGM_CREDENTIAL,
    PRGM_INST_PROGRAM_NAME_CLEANED,
    NOTES,
    HAS_HISTORICAL_PRGM_ID_LINK,
    DUP_PROGRAM_USE_THIS_PRGM_ID,
    CIP_CLUSTER_ARTS_APPLIED,
    DACSO_OLD_PRGM_ID_DO_NOT_USE,
    LCIP_CD_CIP2016,
    LCIP_NAME_CIP2016,
    LCIP_LCP4_CD,
    LCP4_CIP_4DIGITS_NAME,
    HISTORICAL_PRGM_ID,
    YEAR_LINK_CREATED,
    SURVEY_CODE
  )

## ---- 2021 Get historical linkages for DACSO prgms ----
## use the historical linkage added from INFOWARE_PROGRAMS_HIST_PRGMID_XREF
## to link back to the programs table to fill in the historical program details
Updated_DACSO_Programs_in_2021_with_links <- Updated_DACSO_Programs_in_2021_with_links %>%
  inner_join(
    programs_table %>%
      select(
        PRGM_ID,
        HISTORICAL_CPC_CD = PRGM_LCPC_CD,
        HISTORICAL_PROGRAM_NAME = PRGM_INST_PROGRAM_NAME,
        HISTORICAL_CIP4_CD = LCIP_LCP4_CD
      ),
    by = c(HISTORICAL_PRGM_ID = "PRGM_ID")
  ) %>%
  mutate(
    Updated_CPC_Flag = case_when(
      PRGM_LCPC_CD != HISTORICAL_CPC_CD ~ 'Yes',
      TRUE ~ NA
    ),
    Updated_CIP_Flag = case_when(
      LCIP_LCP4_CD != HISTORICAL_CIP4_CD ~ 'Yes',
      TRUE ~ NA
    )
  )

## ---- 2021 Update to XWALK: Updated DACSO programs WITH historical linkages ----
# Use generated key columns for the business-key join below.
# This keeps the original columns unchanged and makes the SQL-like matching rule explicit.
DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 <- DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 %>%
  left_join(
    Updated_DACSO_Programs_in_2021_with_links %>%
      mutate(HISTORICAL_CPC_CD = as.character(HISTORICAL_CPC_CD)) %>%
      add_join_keys(
        c(
          COCI_INST_CD_KEY = "PRGM_INST_CD",
          PRGM_LCPC_CD_KEY = "HISTORICAL_CPC_CD",
          PRGM_INST_PROGRAM_NAME_KEY = "HISTORICAL_PROGRAM_NAME"
        )
      ) %>%
      select(
        COCI_INST_CD_KEY,
        PRGM_LCPC_CD_KEY,
        PRGM_INST_PROGRAM_NAME_KEY,
        HISTORICAL_CIP4_CD,
        PRGM_LCPC_CD_NEW = PRGM_LCPC_CD,
        PRGM_INST_PROGRAM_NAME_NEW = PRGM_INST_PROGRAM_NAME,
        LCIP_LCP4_CD,
        LCP4_CIP_4DIGITS_NAME_NEW = LCP4_CIP_4DIGITS_NAME,
        Updated_DACSO_CPC2021 = Updated_CPC_Flag,
        Updated_DACSO_CIP2021 = Updated_CIP_Flag
      ),
    by = c(
      "COCI_INST_CD_KEY",
      "PRGM_LCPC_CD_KEY",
      "PRGM_INST_PROGRAM_NAME_KEY",
      "CIP_CODE_4" = "HISTORICAL_CIP4_CD"
    )
  ) %>%
  mutate(
    PRGM_LCPC_CD = ifelse(
      !is.na(PRGM_LCPC_CD_NEW),
      PRGM_LCPC_CD_NEW,
      PRGM_LCPC_CD
    ),
    PRGM_INST_PROGRAM_NAME = ifelse(
      !is.na(PRGM_INST_PROGRAM_NAME_NEW),
      PRGM_INST_PROGRAM_NAME_NEW,
      PRGM_INST_PROGRAM_NAME
    ),
    CIP_CODE_4 = ifelse(!is.na(LCIP_LCP4_CD), LCIP_LCP4_CD, CIP_CODE_4),
    LCP4_CIP_4DIGITS_NAME = ifelse(
      !is.na(LCP4_CIP_4DIGITS_NAME_NEW),
      LCP4_CIP_4DIGITS_NAME_NEW,
      LCP4_CIP_4DIGITS_NAME
    )
  ) %>%
  select(
    -PRGM_LCPC_CD_NEW,
    -PRGM_INST_PROGRAM_NAME_NEW,
    -LCIP_LCP4_CD,
    -LCP4_CIP_4DIGITS_NAME_NEW
  ) %>%
  refresh_xwalk_join_keys()

## ---- 2021 Find Remaining updated DACSO missing from XWALK for match to STP program ----
# Use normalized helper keys for the anti-join so case-only differences do not create false misses.
Remaining_DACSO_Updates_CPCS_2021 <- Updated_DACSO_Programs_in_2021_with_links %>%
  add_join_keys(
    c(
      PRGM_LCPC_CD_KEY = "PRGM_LCPC_CD",
      COCI_INST_CD_KEY = "PRGM_INST_CD",
      PRGM_INST_PROGRAM_NAME_KEY = "PRGM_INST_PROGRAM_NAME"
    )
  ) %>%
  anti_join(
    DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 %>%
      select(PRGM_LCPC_CD_KEY, COCI_INST_CD_KEY, PRGM_INST_PROGRAM_NAME_KEY),
    by = c("PRGM_LCPC_CD_KEY", "COCI_INST_CD_KEY", "PRGM_INST_PROGRAM_NAME_KEY")
  )

# ***** manual work needed *****
# review infoware notes
programs_table %>%
  filter(PRGM_ID %in% Remaining_DACSO_Updates_CPCS_2021$PRGM_ID) %>%
  pull(PRGM_ID, NOTES)

# PRGM_ID 10132 links to 3119
programs_table %>% filter(PRGM_ID == "3119") %>% pull(NOTES)

# update based on review
DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 <- DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 %>%
  mutate(
    CIP_CODE_4 = case_when(PRGM_ID == 3119 ~ "1907", TRUE ~ CIP_CODE_4), # ? how can we know why 1907
    LCP4_CIP_4DIGITS_NAME = case_when(
      PRGM_ID ==
        3119 ~ "Human development, family studies and related services",
      TRUE ~ LCP4_CIP_4DIGITS_NAME
    ),
    Updated_DACSO_CIP2021 = case_when(
      PRGM_ID == 3119 ~ "Yes",
      TRUE ~ Updated_DACSO_CIP2021
    ),
    PRGM_LCPC_CD = case_when(PRGM_ID == 3119 ~ "EACSW", TRUE ~ PRGM_LCPC_CD),
    PRGM_INST_PROGRAM_NAME = case_when(
      PRGM_ID == 3119 ~ "Education Assistant and Community Support Worker",
      TRUE ~ PRGM_INST_PROGRAM_NAME
    ),
    Updated_DACSO_CPC2021 = case_when(
      PRGM_ID == 3119 ~ "Yes",
      TRUE ~ Updated_DACSO_CPC2021
    )
  ) %>%
  refresh_xwalk_join_keys()

## ---- 2022 Find DACSO prgms WITH historical linkages ----
Updated_DACSO_Programs_in_2022_with_links <- programs_table %>%
  filter(
    PRGM_FIRST_SEEN_SUBM_CD == 'C_Outc22' & HAS_HISTORICAL_PRGM_ID_LINK == 'Y'
  ) %>%
  inner_join(
    tbl(con, "INFOWARE_PROGRAMS_HIST_PRGMID_XREF") %>%
      filter(YEAR_LINK_CREATED == 'C_Outc22' & SURVEY_CODE == 'DACSO') %>%
      collect(),
    by = "PRGM_ID"
  ) %>%
  select(
    PRGM_ID,
    PRGM_FIRST_SEEN_SUBM_CD,
    PRGM_INST_CD,
    PRGM_LCPC_CD,
    PRGM_INST_PROGRAM_NAME,
    PRGM_TTRAIN_FLAG,
    PRGM_CREDENTIAL,
    PRGM_INST_PROGRAM_NAME_CLEANED,
    NOTES,
    HAS_HISTORICAL_PRGM_ID_LINK,
    DUP_PROGRAM_USE_THIS_PRGM_ID,
    CIP_CLUSTER_ARTS_APPLIED,
    DACSO_OLD_PRGM_ID_DO_NOT_USE,
    LCIP_CD_CIP2016,
    LCIP_NAME_CIP2016,
    LCIP_LCP4_CD,
    LCP4_CIP_4DIGITS_NAME,
    HISTORICAL_PRGM_ID,
    YEAR_LINK_CREATED,
    SURVEY_CODE
  )


## ---- 2022 Get historical linkages for DACSO prgms ----
## use the historical linkage added from INFOWARE_PROGRAMS_HIST_PRGMID_XREF
## to link back to the programs table to fill in the historical program details
Updated_DACSO_Programs_in_2022_with_links <- Updated_DACSO_Programs_in_2022_with_links %>%
  inner_join(
    programs_table %>%
      select(
        PRGM_ID,
        HISTORICAL_CPC_CD = PRGM_LCPC_CD,
        HISTORICAL_PROGRAM_NAME = PRGM_INST_PROGRAM_NAME,
        HISTORICAL_CIP4_CD = LCIP_LCP4_CD
      ),
    by = c(HISTORICAL_PRGM_ID = "PRGM_ID")
  ) %>%
  mutate(
    Updated_CPC_Flag = case_when(
      PRGM_LCPC_CD != HISTORICAL_CPC_CD ~ 'Yes',
      TRUE ~ NA
    ),
    Updated_CIP_Flag = case_when(
      LCIP_LCP4_CD != HISTORICAL_CIP4_CD ~ 'Yes',
      TRUE ~ NA
    )
  )

## ---- 2022 Update to XWALK: Updated DACSO programs WITH historical linkages ----
# Use generated key columns for the business-key join below.
# This keeps the original columns unchanged and makes the SQL-like matching rule explicit.
DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 <- DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 %>%
  left_join(
    Updated_DACSO_Programs_in_2022_with_links %>%
      mutate(HISTORICAL_CPC_CD = as.character(HISTORICAL_CPC_CD)) %>%
      add_join_keys(
        c(
          COCI_INST_CD_KEY = "PRGM_INST_CD",
          PRGM_LCPC_CD_KEY = "HISTORICAL_CPC_CD",
          PRGM_INST_PROGRAM_NAME_KEY = "HISTORICAL_PROGRAM_NAME"
        )
      ) %>%
      select(
        COCI_INST_CD_KEY,
        PRGM_LCPC_CD_KEY,
        PRGM_INST_PROGRAM_NAME_KEY,
        HISTORICAL_CIP4_CD,
        PRGM_LCPC_CD_NEW = PRGM_LCPC_CD,
        PRGM_INST_PROGRAM_NAME_NEW = PRGM_INST_PROGRAM_NAME,
        LCIP_LCP4_CD,
        LCP4_CIP_4DIGITS_NAME_NEW = LCP4_CIP_4DIGITS_NAME,
        Updated_DACSO_CPC2022 = Updated_CPC_Flag,
        Updated_DACSO_CIP2022 = Updated_CIP_Flag
      ),
    by = c(
      "COCI_INST_CD_KEY",
      "PRGM_LCPC_CD_KEY",
      "PRGM_INST_PROGRAM_NAME_KEY",
      "CIP_CODE_4" = "HISTORICAL_CIP4_CD"
    )
  ) %>%
  mutate(
    PRGM_LCPC_CD = ifelse(
      !is.na(PRGM_LCPC_CD_NEW),
      PRGM_LCPC_CD_NEW,
      PRGM_LCPC_CD
    ),
    PRGM_INST_PROGRAM_NAME = ifelse(
      !is.na(PRGM_INST_PROGRAM_NAME_NEW),
      PRGM_INST_PROGRAM_NAME_NEW,
      PRGM_INST_PROGRAM_NAME
    ),
    CIP_CODE_4 = ifelse(!is.na(LCIP_LCP4_CD), LCIP_LCP4_CD, CIP_CODE_4),
    LCP4_CIP_4DIGITS_NAME = ifelse(
      !is.na(LCP4_CIP_4DIGITS_NAME_NEW),
      LCP4_CIP_4DIGITS_NAME_NEW,
      LCP4_CIP_4DIGITS_NAME
    )
  ) %>%
  select(
    -PRGM_LCPC_CD_NEW,
    -PRGM_INST_PROGRAM_NAME_NEW,
    -LCIP_LCP4_CD,
    -LCP4_CIP_4DIGITS_NAME_NEW
  ) %>%
  refresh_xwalk_join_keys()

## ---- 2022 Find Remaining updated DACSO missing from XWALK for match to STP program ----
# Use normalized helper keys for the anti-join so case-only differences do not create false misses.
Remaining_DACSO_Updates_CPCS_2022 <- Updated_DACSO_Programs_in_2022_with_links %>%
  add_join_keys(
    c(
      PRGM_LCPC_CD_KEY = "PRGM_LCPC_CD",
      COCI_INST_CD_KEY = "PRGM_INST_CD",
      PRGM_INST_PROGRAM_NAME_KEY = "PRGM_INST_PROGRAM_NAME"
    )
  ) %>%
  anti_join(
    DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 %>%
      select(PRGM_LCPC_CD_KEY, COCI_INST_CD_KEY, PRGM_INST_PROGRAM_NAME_KEY),
    by = c("PRGM_LCPC_CD_KEY", "COCI_INST_CD_KEY", "PRGM_INST_PROGRAM_NAME_KEY")
  )

# ***** manual work needed *****
# review infoware notes
programs_table %>%
  filter(PRGM_ID %in% Remaining_DACSO_Updates_CPCS_2022$PRGM_ID) %>%
  pull(PRGM_ID, NOTES)
# 7 PRGM_IDs remaining CPCs had historical links for their historical links
##  i.e., they could be linked to more codes back
# review notes for each historical, to find the last historical link:
# 10355 -> 9855 -> 115 (update CIP and CPC to most recent)
# 10359 -> 9856 -> 9006 -> 4760 (4760 doesn't exist in XWALK, update 9006 - CPC & CIP)
# 10366 -> 9859 -> 5952 -> 116 (116 doesn't exist in XWALK, update 5952 - CPC only)
# 10367 -> 9858 -> 9008 (update CPC only)
# 10383 -> 9857 -> 4960 (update CPC only)
# 10387 -> 9860 -> 117 (update CPC only)
# 10399 -> 9861 -> 131 (update CIP and CPC to most recent)

# apply necessary updates
DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 <- DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 %>%
  mutate(
    CIP_CODE_4 = case_when(
      PRGM_ID == 115 ~ "1502",
      PRGM_ID == 9006 ~ "1110",
      PRGM_ID == 131 ~ "5001",
      TRUE ~ CIP_CODE_4
    ),
    LCP4_CIP_4DIGITS_NAME = case_when(
      PRGM_ID == 115 ~ "Civil engineering technology/technician",
      PRGM_ID ==
        9006 ~ "Computer/information technology administration and management",
      PRGM_ID == 131 ~ "Visual, digital and performing arts, general",
      TRUE ~ LCP4_CIP_4DIGITS_NAME
    ),
    Updated_DACSO_CIP2022 = case_when(
      PRGM_ID == 115 ~ "Yes",
      PRGM_ID == 9006 ~ "Yes",
      PRGM_ID == 131 ~ "Yes",
      TRUE ~ Updated_DACSO_CIP2022
    ),
    PRGM_LCPC_CD = case_when(
      PRGM_ID == 115 ~ "CENG.DIP",
      PRGM_ID == 9006 ~ "CNET.CERT",
      PRGM_ID == 5952 ~ "ECENG.RE.DIP",
      PRGM_ID == 9008 ~ "ECENG.UVIC.ADIP",
      PRGM_ID == 4960 ~ "ICS.DIP",
      PRGM_ID == 117 ~ "MENG.DIP",
      PRGM_ID == 131 ~ "VART.DIP",
      TRUE ~ PRGM_LCPC_CD
    ),
    PRGM_INST_PROGRAM_NAME = case_when(
      PRGM_ID == 115 ~ "Civil Engineering Technology (Diploma)",
      PRGM_ID ==
        9006 ~ "Computer Network Electronics Support Tech (Certificate)",
      PRGM_ID ==
        5952 ~ "Electronics & Computer Eng - Renewable Energy (Diploma)",
      PRGM_ID ==
        9008 ~ "Electrical & Computer Eng - Bridge to UVic (Adv Diploma)",
      PRGM_ID == 4960 ~ "Information & Computer Systems Technology (Diploma)",
      PRGM_ID == 117 ~ "Mechanical Engineering Technology (Diploma)",
      PRGM_ID == 131 ~ "Visual Arts (Diploma)",
      TRUE ~ PRGM_INST_PROGRAM_NAME
    ),
    Updated_DACSO_CPC2022 = case_when(
      PRGM_ID == 115 ~ "Yes",
      PRGM_ID == 9006 ~ "Yes",
      PRGM_ID == 5952 ~ "Yes",
      PRGM_ID == 9008 ~ "Yes",
      PRGM_ID == 4960 ~ "Yes",
      PRGM_ID == 117 ~ "Yes",
      PRGM_ID == 131 ~ "Yes",
      TRUE ~ Updated_DACSO_CPC2022
    )
  ) %>%
  refresh_xwalk_join_keys()

## ---- 2023 Find DACSO prgms WITH historical linkages ----
Updated_DACSO_Programs_in_2023_with_links <- programs_table %>%
  filter(
    PRGM_FIRST_SEEN_SUBM_CD == 'C_Outc23' & HAS_HISTORICAL_PRGM_ID_LINK == 'Y'
  ) %>%
  inner_join(
    tbl(con, "INFOWARE_PROGRAMS_HIST_PRGMID_XREF") %>%
      filter(YEAR_LINK_CREATED == 'C_Outc23' & SURVEY_CODE == 'DACSO') %>%
      collect(),
    by = "PRGM_ID"
  ) %>%
  select(
    PRGM_ID,
    PRGM_FIRST_SEEN_SUBM_CD,
    PRGM_INST_CD,
    PRGM_LCPC_CD,
    PRGM_INST_PROGRAM_NAME,
    PRGM_TTRAIN_FLAG,
    PRGM_CREDENTIAL,
    PRGM_INST_PROGRAM_NAME_CLEANED,
    NOTES,
    HAS_HISTORICAL_PRGM_ID_LINK,
    DUP_PROGRAM_USE_THIS_PRGM_ID,
    CIP_CLUSTER_ARTS_APPLIED,
    DACSO_OLD_PRGM_ID_DO_NOT_USE,
    LCIP_CD_CIP2016,
    LCIP_NAME_CIP2016,
    LCIP_LCP4_CD,
    LCP4_CIP_4DIGITS_NAME,
    HISTORICAL_PRGM_ID,
    YEAR_LINK_CREATED,
    SURVEY_CODE
  )


## ---- 2023 Get historical linkages for DACSO prgms ----
## use the historical linkage added from INFOWARE_PROGRAMS_HIST_PRGMID_XREF
## to link back to the programs table to fill in the historical program details
Updated_DACSO_Programs_in_2023_with_links <- Updated_DACSO_Programs_in_2023_with_links %>%
  inner_join(
    programs_table %>%
      select(
        PRGM_ID,
        HISTORICAL_CPC_CD = PRGM_LCPC_CD,
        HISTORICAL_PROGRAM_NAME = PRGM_INST_PROGRAM_NAME,
        HISTORICAL_CIP4_CD = LCIP_LCP4_CD
      ),
    by = c(HISTORICAL_PRGM_ID = "PRGM_ID")
  ) %>%
  mutate(
    Updated_CPC_Flag = case_when(
      PRGM_LCPC_CD != HISTORICAL_CPC_CD ~ 'Yes',
      TRUE ~ NA
    ),
    Updated_CIP_Flag = case_when(
      LCIP_LCP4_CD != HISTORICAL_CIP4_CD ~ 'Yes',
      TRUE ~ NA
    )
  )

## ---- 2023 Update to XWALK: Updated DACSO programs WITH historical linkages ----
# Use generated key columns for the business-key join below.
# This keeps the original columns unchanged and makes the SQL-like matching rule explicit.
DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 <- DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 %>%
  left_join(
    Updated_DACSO_Programs_in_2023_with_links %>%
      mutate(HISTORICAL_CPC_CD = as.character(HISTORICAL_CPC_CD)) %>%
      add_join_keys(
        c(
          COCI_INST_CD_KEY = "PRGM_INST_CD",
          PRGM_LCPC_CD_KEY = "HISTORICAL_CPC_CD",
          PRGM_INST_PROGRAM_NAME_KEY = "HISTORICAL_PROGRAM_NAME"
        )
      ) %>%
      select(
        COCI_INST_CD_KEY,
        PRGM_LCPC_CD_KEY,
        PRGM_INST_PROGRAM_NAME_KEY,
        HISTORICAL_CIP4_CD,
        PRGM_LCPC_CD_NEW = PRGM_LCPC_CD,
        PRGM_INST_PROGRAM_NAME_NEW = PRGM_INST_PROGRAM_NAME,
        LCIP_LCP4_CD,
        LCP4_CIP_4DIGITS_NAME_NEW = LCP4_CIP_4DIGITS_NAME,
        Updated_DACSO_CPC2023 = Updated_CPC_Flag,
        Updated_DACSO_CIP2023 = Updated_CIP_Flag
      ),
    by = c(
      "COCI_INST_CD_KEY",
      "PRGM_LCPC_CD_KEY",
      "PRGM_INST_PROGRAM_NAME_KEY",
      "CIP_CODE_4" = "HISTORICAL_CIP4_CD"
    )
  ) %>%
  mutate(
    PRGM_LCPC_CD = ifelse(
      !is.na(PRGM_LCPC_CD_NEW),
      PRGM_LCPC_CD_NEW,
      PRGM_LCPC_CD
    ),
    PRGM_INST_PROGRAM_NAME = ifelse(
      !is.na(PRGM_INST_PROGRAM_NAME_NEW),
      PRGM_INST_PROGRAM_NAME_NEW,
      PRGM_INST_PROGRAM_NAME
    ),
    CIP_CODE_4 = ifelse(!is.na(LCIP_LCP4_CD), LCIP_LCP4_CD, CIP_CODE_4),
    LCP4_CIP_4DIGITS_NAME = ifelse(
      !is.na(LCP4_CIP_4DIGITS_NAME_NEW),
      LCP4_CIP_4DIGITS_NAME_NEW,
      LCP4_CIP_4DIGITS_NAME
    )
  ) %>%
  select(
    -PRGM_LCPC_CD_NEW,
    -PRGM_INST_PROGRAM_NAME_NEW,
    -LCIP_LCP4_CD,
    -LCP4_CIP_4DIGITS_NAME_NEW
  ) %>%
  refresh_xwalk_join_keys()

## ---- 2023 Find Remaining updated DACSO missing from XWALK for match to STP program ----
# Use normalized helper keys for the anti-join so case-only differences do not create false misses.
Remaining_DACSO_Updates_CPCS_2023 <- Updated_DACSO_Programs_in_2023_with_links %>%
  add_join_keys(
    c(
      PRGM_LCPC_CD_KEY = "PRGM_LCPC_CD",
      COCI_INST_CD_KEY = "PRGM_INST_CD",
      PRGM_INST_PROGRAM_NAME_KEY = "PRGM_INST_PROGRAM_NAME"
    )
  ) %>%
  anti_join(
    DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 %>%
      select(PRGM_LCPC_CD_KEY, COCI_INST_CD_KEY, PRGM_INST_PROGRAM_NAME_KEY),
    by = c("PRGM_LCPC_CD_KEY", "COCI_INST_CD_KEY", "PRGM_INST_PROGRAM_NAME_KEY")
  )

# ***** manual work needed *****
# review infoware notes
programs_table %>%
  filter(PRGM_ID %in% Remaining_DACSO_Updates_CPCS_2023$PRGM_ID) %>%
  pull(PRGM_ID, NOTES)
# 3 PRGM_IDs remaining CPCs
# 1 linked to multiple historic codes: 10413 -> 9841 -> 9237 -> 9017 -> 6036 (9017 & 6036 doesn't exist in XWALK, update 9237 - CPC only)
# 1 was updated by 2022 update (10234 updated the CPC): 10475 -> 1158 (update CPC only)
# 1 has historical match not in XWALK: 10493 -> 9810 (9810 does not exist in XWALK - add 10493 info as 9810 PRGM_ID)

# add missing PRGM_ID
DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 <- DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 %>%
  bind_rows(
    Remaining_DACSO_Updates_CPCS_2023 %>%
      filter(PRGM_ID == "10493") %>%
      mutate(
        PSI_CODE = PRGM_INST_CD,
        COCI_INST_CD = PRGM_INST_CD,
        PSI_PROGRAM_CODE = PRGM_LCPC_CD,
        PSI_CREDENTIAL_PROGRAM_DESC = PRGM_INST_PROGRAM_NAME,
        PRGM_ID = 9810,
        New_DACSO_Program2021to23 = "Yes2023"
      ) %>%
      select(
        PSI_CODE,
        COCI_INST_CD,
        PSI_PROGRAM_CODE,
        PSI_CREDENTIAL_PROGRAM_DESC,
        PRGM_LCPC_CD,
        PRGM_INST_PROGRAM_NAME,
        PRGM_CREDENTIAL,
        CIP_CODE_4 = LCIP_LCP4_CD, #why change name?
        LCP4_CIP_4DIGITS_NAME,
        PRGM_ID,
        New_DACSO_Program2021to23
      )
  )

# update necessary values
DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 <- DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 %>%
  mutate(
    PRGM_LCPC_CD = case_when(
      PRGM_ID == 9237 ~ "BCPRPC",
      PRGM_ID == 1158 ~ "DIPLHKIN",
      TRUE ~ PRGM_LCPC_CD
    ),
    PRGM_INST_PROGRAM_NAME = case_when(
      PRGM_ID ==
        9237 ~ "BC Police Recruit Training: Qualified Municipal Constable",
      PRGM_ID == 1158 ~ "Human Kinetics",
      TRUE ~ PRGM_INST_PROGRAM_NAME
    ),
    Updated_DACSO_CPC2023 = case_when(
      PRGM_ID == 9237 ~ "Yes",
      PRGM_ID == 1158 ~ "Yes",
      TRUE ~ Updated_DACSO_CPC2023
    )
  )

## ---- Update to XWALK: DACSO programs with updated CIPS ----
## find the updated CIPs
updated_cips <- DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 %>%
  left_join(
    programs_table %>%
      select(PRGM_ID, CIP_CODE_4 = LCIP_LCP4_CD, LCP4_CIP_4DIGITS_NAME, NOTES),
    by = "PRGM_ID"
  ) %>%
  filter(
    CIP_CODE_4.x != CIP_CODE_4.y |
      LCP4_CIP_4DIGITS_NAME.x != LCP4_CIP_4DIGITS_NAME.y
  )

# filter out the known updates
updated_cips <- updated_cips %>%
  filter(
    is.na(Updated_DACSO_CIP2021) &
      is.na(Updated_DACSO_CIP2022) &
      is.na(Updated_DACSO_CIP2023)
  )

# review NOTES column where word "CIP" exists
updated_cips %>%
  filter(grepl("CIP", NOTES)) %>%
  select(CIP_CODE_4.x, CIP_CODE_4.y, NOTES, PRGM_ID) %>%
  print(n = 100)

# review NOTES column where word "CIP" doesn't exist
updated_cips %>%
  filter(!grepl("CIP", NOTES)) %>%
  select(CIP_CODE_4.x, CIP_CODE_4.y, NOTES, PRGM_ID) %>%
  print(n = 100)

updated_cips %>%
  filter(!grepl("CIP", NOTES)) %>%
  select(LCP4_CIP_4DIGITS_NAME.x, LCP4_CIP_4DIGITS_NAME.y, NOTES, PRGM_ID) %>%
  print(n = 100)

# Decision: PRGM_ID 9018 shouldn't be changed; the rest seem like acceptable changes, why?
# filter out from updated cips file
updated_cips <- updated_cips %>%
  filter(PRGM_ID != 9018)

# update CIPS in XWALK
DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 <- DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 %>%
  left_join(
    updated_cips %>%
      distinct(
        PRGM_ID,
        CIP_CODE_4 = CIP_CODE_4.y,
        LCP4_CIP_4DIGITS_NAME = LCP4_CIP_4DIGITS_NAME.y
      ),
    by = "PRGM_ID"
  ) %>%
  mutate(
    CIP_CODE_4 = ifelse(!is.na(CIP_CODE_4.y), CIP_CODE_4.y, CIP_CODE_4.x),
    LCP4_CIP_4DIGITS_NAME = ifelse(
      !is.na(LCP4_CIP_4DIGITS_NAME.y),
      LCP4_CIP_4DIGITS_NAME.y,
      LCP4_CIP_4DIGITS_NAME.x
    ),
    # adding all of these to only the most recent year updated cip column
    Updated_DACSO_CIP2023 = ifelse(
      !is.na(CIP_CODE_4.y),
      "Yes",
      Updated_DACSO_CIP2023
    )
  ) %>%
  select(-ends_with(".x"), -ends_with(".y"))


# ---- Add STP matching columns to XWALK ----
DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 <- DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 %>%
  mutate(
    New_STP_Program2021_23 = NA_character_,
    Updated_DACSO_CDTL2021_23 = NA_character_
  )

## ---- Write XWALK to Decimal ----
# append a suffix "_r" to the table name in decmile database to indicate which is created in R.
dbWriteTable(
  con,
  "DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23_r",
  DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23,
  overwrite = TRUE
)


# ******************************************************************************
# PART 2: UPDATE XWALK WITH NEW STP CREDENTIAL DATA
# ******************************************************************************
# WHY: STP credential programs need to be matched to the XWALK to get their CIP
# codes. First, programs already in the XWALK are matched; then new programs are
# auto-matched on program code and description.
#
# Original: ~12 SQL operations (SELECT INTO, ALTER TABLE, UPDATE...FROM JOIN)
# Translated: Pull tables into R, perform sequential left_join + mutate operations.

# check for required table
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."credential_non_dup"')))

credential_non_dup <- sch_tbl("Credential_Non_Dup") |>
  rename_with(toupper) %>%
  select(
    PSI_CODE,
    PSI_PROGRAM_CODE,
    PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    PSI_CREDENTIAL_CIP,
    PSI_CREDENTIAL_LEVEL,
    PSI_CREDENTIAL_CATEGORY,
    OUTCOMES_CRED
  ) %>%
  collect()

DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 <- DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 %>%
  refresh_xwalk_join_keys()
xwalk <- DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23

# Pull INFOWARE CIP reference tables
cip6 <- sch_tbl("INFOWARE_L_CIP_6DIGITS_CIP2016") %>%
  collect() |>
  rename_with(toupper)
cip4_ref <- sch_tbl("INFOWARE_L_CIP_4DIGITS_CIP2016") %>%
  collect() |>
  rename_with(toupper)
cip2_ref <- sch_tbl("INFOWARE_L_CIP_2DIGITS_CIP2016") %>%
  collect() |>
  rename_with(toupper)

## ---- Make STP_Credential_Non_Dup_Programs_DACSO ----
# Create a DACSO version of Credential_non_dup table with subset of columns
# WHY: Create a DACSO-only subset of Credential_Non_Dup, grouped by program attributes.
# Original: SELECT INTO with GROUP BY HAVING
stp_dacso <- credential_non_dup %>%
  filter(OUTCOMES_CRED == "DACSO") %>%
  count(
    PSI_CODE,
    PSI_PROGRAM_CODE,
    PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    PSI_CREDENTIAL_CIP,
    PSI_CREDENTIAL_LEVEL,
    PSI_CREDENTIAL_CATEGORY,
    OUTCOMES_CRED,
    name = "EXPR1"
  ) %>%
  ## ---- Restructure and populate imported STP data ----
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


# Create PSI_CODE to COCI_INST_CD lookup table
# ---- Add COCI_INST_CD from XWALK ----
# WHY: PSI_CODE and COCI_INST_CD are different code systems for institutions.
# The XWALK maps between them.
psi_to_coci <- xwalk %>%
  filter(!is.na(PSI_CODE) & !is.na(COCI_INST_CD)) %>%
  distinct(PSI_CODE, COCI_INST_CD)


# Add COCI_INST_CD to STP table
stp_dacso <- stp_dacso %>%
  left_join(
    psi_to_coci %>% rename(COCI_INST_CD_MAP = COCI_INST_CD),
    by = "PSI_CODE"
  ) %>%
  mutate(COCI_INST_CD = coalesce(COCI_INST_CD_MAP, COCI_INST_CD)) %>%
  select(-COCI_INST_CD_MAP) %>%
  refresh_stp_join_keys()


## ---- Populate STP_CIP_CODE_4, STP_CIP_CODE_4_NAME ----
# WHY: Match PSI_CREDENTIAL_CIP to the 6-digit CIP taxonomy to get 4-digit codes.
# Original: UPDATE with 3-table JOIN (STP INNER JOIN CIP6 ON CIP, then CIP4 ON LCP4_CD)
cip6_lookup <- cip6 %>%
  select(LCIP_CD_WITH_PERIOD, LCIP_LCP4_CD) %>%
  inner_join(
    cip4_ref %>% select(LCP4_CD, LCP4_CIP_4DIGITS_NAME),
    by = c("LCIP_LCP4_CD" = "LCP4_CD")
  )
stp_dacso <- stp_dacso %>%
  left_join(
    cip6_lookup %>%
      select(LCIP_CD_WITH_PERIOD, LCIP_LCP4_CD, LCP4_CIP_4DIGITS_NAME),
    by = c("PSI_CREDENTIAL_CIP" = "LCIP_CD_WITH_PERIOD")
  ) %>%
  mutate(
    STP_CIP_CODE_4 = coalesce(STP_CIP_CODE_4, LCIP_LCP4_CD), # STP_CIP_CODE_4 are created in previous step, so all are NA
    STP_CIP_CODE_4_NAME = coalesce(STP_CIP_CODE_4_NAME, LCP4_CIP_4DIGITS_NAME)
  ) %>%
  select(-LCIP_LCP4_CD, -LCP4_CIP_4DIGITS_NAME)

## ---- Already matched programs (might have new CIPs) ----
# reference: to check after each query to see counts of matched
# stp_dacso %>% count(Already_Matched)

# WHY: Programs already in the XWALK (matched from previous cycles) inherit
# their CIP codes. Try matching on PSI_CODE first, then COCI_INST_CD.

# Queries set Already_Matched column to Yes if match
# join STP data to XWALK on PSI_CODE, PSI_PROGRAM_CREDENTIAL_DESCRIPTION, PSI_PROGRAM_CODE
# Match on PSI_CODE + PSI_PROGRAM_CODE + PSI_CREDENTIAL_PROGRAM_DESCRIPTION
xwalk_exact <- xwalk %>%
  filter(
    !is.na(PSI_CODE) &
      !is.na(PSI_PROGRAM_CODE) &
      !is.na(PSI_CREDENTIAL_PROGRAM_DESC)
  ) %>%
  select(
    PSI_CODE_KEY,
    PSI_PROGRAM_CODE_KEY,
    PSI_CREDENTIAL_PROGRAM_DESC_KEY,
    CIP_CODE_4,
    LCP4_CIP_4DIGITS_NAME
  ) |> slice_head(n=1, by = c(PSI_CODE_KEY, PSI_PROGRAM_CODE_KEY, PSI_CREDENTIAL_PROGRAM_DESC_KEY))

stp_dacso <- stp_dacso %>%
  left_join(
    xwalk_exact %>%
      rename(XW_CIP4 = CIP_CODE_4, XW_CIP4_NAME = LCP4_CIP_4DIGITS_NAME),
    by = c(
      "PSI_CODE_KEY" = "PSI_CODE_KEY",
      "PSI_PROGRAM_CODE_KEY" = "PSI_PROGRAM_CODE_KEY",
      "PSI_CREDENTIAL_PROGRAM_DESCRIPTION_KEY" = "PSI_CREDENTIAL_PROGRAM_DESC_KEY"
    ),
    relationship = "many-to-many" # no reason is provided
  ) %>%
  mutate(
    Already_Matched = if_else(
      !is.na(XW_CIP4) & is.na(Already_Matched),
      "Yes",
      Already_Matched
    ),
    OUTCOMES_CIP_CODE_4 = coalesce(OUTCOMES_CIP_CODE_4, XW_CIP4), # OUTCOMES_CIP_CODE_4 are created in previous step, so all are NA
    OUTCOMES_CIP_CODE_4_NAME = coalesce(OUTCOMES_CIP_CODE_4_NAME, XW_CIP4_NAME)
  ) %>%
  select(-XW_CIP4, -XW_CIP4_NAME)


# ℹ Row 14 of `x` matches multiple rows in `y`.
# ℹ Row 2465 of `y` matches multiple rows in `x`.
stp_dacso <- stp_dacso %>% distinct() # thia is not in the original code
# 5462

# join STP data to XWALK on COCI_INST_CD, PSI_CREDENTIAL_PROGRAM_DESCRIPTION, PSI_PROGRAM_CODE
# Match on COCI_INST_CD + PSI_PROGRAM_CODE + PSI_CREDENTIAL_PROGRAM_DESCRIPTION
xwalk_coci <- xwalk %>%
  filter(
    !is.na(COCI_INST_CD) &
      !is.na(PSI_PROGRAM_CODE) &
      !is.na(PSI_CREDENTIAL_PROGRAM_DESC)
  ) %>%
  select(
    COCI_INST_CD_KEY,
    PSI_PROGRAM_CODE_KEY,
    PSI_CREDENTIAL_PROGRAM_DESC_KEY,
    CIP_CODE_4,
    LCP4_CIP_4DIGITS_NAME
  )

stp_dacso <- stp_dacso %>%
  left_join(
    xwalk_coci %>%
      rename(XW_CIP4 = CIP_CODE_4, XW_CIP4_NAME = LCP4_CIP_4DIGITS_NAME),
    by = c(
      "COCI_INST_CD_KEY" = "COCI_INST_CD_KEY",
      "PSI_PROGRAM_CODE_KEY" = "PSI_PROGRAM_CODE_KEY",
      "PSI_CREDENTIAL_PROGRAM_DESCRIPTION_KEY" = "PSI_CREDENTIAL_PROGRAM_DESC_KEY"
    ),
    relationship = "many-to-many" # no reason is provided
  ) %>%
  mutate(
    Already_Matched = if_else(
      is.na(Already_Matched) &
        is.na(OUTCOMES_CIP_CODE_4) &
        is.na(OUTCOMES_CIP_CODE_4_NAME) &
        !is.na(XW_CIP4),
      "Yes",
      Already_Matched
    ),
    OUTCOMES_CIP_CODE_4 = if_else(
      is.na(Already_Matched) &
        is.na(OUTCOMES_CIP_CODE_4) &
        is.na(OUTCOMES_CIP_CODE_4_NAME) &
        !is.na(XW_CIP4),
      XW_CIP4,
      OUTCOMES_CIP_CODE_4
    ),
    OUTCOMES_CIP_CODE_4_NAME = if_else(
      is.na(Already_Matched) &
        is.na(OUTCOMES_CIP_CODE_4) &
        is.na(OUTCOMES_CIP_CODE_4_NAME) &
        !is.na(XW_CIP4_NAME),
      XW_CIP4_NAME,
      OUTCOMES_CIP_CODE_4_NAME
    )
  ) %>%
  select(-XW_CIP4, -XW_CIP4_NAME)

# ℹ Row 14 of `x` matches multiple rows in `y`.
# ℹ Row 1251 of `y` matches multiple rows in `x`
stp_dacso <- stp_dacso %>% distinct() # this is not in the original code
# 5462 rows

## ---- Newly matched programs ----
# reference: to check after each query to see counts of matched
# stp_dacso %>% count(New_Auto_Match)

# Queries set New_Auto_Match column to Yes if match
# join STP data to XWALK on PSI_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION = PRGM_INST_PROGRAM_NAME, PSI_PROGRAM_CODE = PRGM_LCPC_CD
# WHY: Programs not in the XWALK can be matched on DACSO program names/codes.
# The XWALK has PRGM_INST_PROGRAM_NAME and PRGM_LCPC_CD fields from DACSO.

# Match on PSI_CODE + PSI_PROGRAM_CODE=PRGM_LCPC_CD + DESC=PRGM_INST_PROGRAM_NAME
xwalk_new_a <- xwalk %>%
  filter(
    !is.na(PSI_CODE) & !is.na(PRGM_LCPC_CD) & !is.na(PRGM_INST_PROGRAM_NAME)
  ) %>%
  select(
    PSI_CODE_KEY,
    PRGM_LCPC_CD_KEY,
    PSI_CREDENTIAL_PROGRAM_DESC_KEY,
    CIP_CODE_4,
    LCP4_CIP_4DIGITS_NAME
  )

stp_dacso <- stp_dacso %>%
  left_join(
    xwalk_new_a %>%
      rename(XW_CIP4 = CIP_CODE_4, XW_CIP4_NAME = LCP4_CIP_4DIGITS_NAME),
    by = c(
      "PSI_CODE_KEY",
      "PSI_PROGRAM_CODE_KEY" = "PRGM_LCPC_CD_KEY",
      "PSI_CREDENTIAL_PROGRAM_DESCRIPTION_KEY" = "PSI_CREDENTIAL_PROGRAM_DESC_KEY"
    ),
    relationship = "many-to-many" # no reason is provided
  ) %>%
  mutate(
    New_Auto_Match = if_else(
      is.na(Already_Matched) & !is.na(XW_CIP4),
      "Yes",
      New_Auto_Match
    ),
    OUTCOMES_CIP_CODE_4 = if_else(
      is.na(Already_Matched) & !is.na(XW_CIP4),
      XW_CIP4,
      OUTCOMES_CIP_CODE_4
    ),
    OUTCOMES_CIP_CODE_4_NAME = if_else(
      is.na(Already_Matched) & !is.na(XW_CIP4_NAME),
      XW_CIP4_NAME,
      OUTCOMES_CIP_CODE_4_NAME
    )
  ) %>%
  select(-XW_CIP4, -XW_CIP4_NAME)

# ℹ Row 783 of `x` matches multiple rows in `y`.
# ℹ Row 1608 of `y` matches multiple rows in `x`.

stp_dacso <- stp_dacso %>% distinct() # this is not in the original code
# 5462 rows

# join STP data to XWALK on COCI_INST_CD, PSI_CREDENTIAL_PROGRAM_DESCRIPTION = PRGM_INST_PROGRAM_NAME, PSI_PROGRAM_CODE = PRGM_LCPC_CD
# Match on COCI_INST_CD + PSI_PROGRAM_CODE=PRGM_LCPC_CD + DESC=PRGM_INST_PROGRAM_NAME
xwalk_new_a2 <- xwalk %>%
  filter(
    !is.na(COCI_INST_CD) & !is.na(PRGM_LCPC_CD) & !is.na(PRGM_INST_PROGRAM_NAME)
  ) %>%
  select(
    COCI_INST_CD_KEY,
    PRGM_LCPC_CD_KEY,
    PRGM_INST_PROGRAM_NAME_KEY,
    CIP_CODE_4,
    LCP4_CIP_4DIGITS_NAME
  )

stp_dacso <- stp_dacso %>%
  left_join(
    xwalk_new_a2 %>%
      rename(XW_CIP4 = CIP_CODE_4, XW_CIP4_NAME = LCP4_CIP_4DIGITS_NAME),
    by = c(
      "COCI_INST_CD_KEY",
      "PSI_PROGRAM_CODE_KEY" = "PRGM_LCPC_CD_KEY",
      "PSI_CREDENTIAL_PROGRAM_DESCRIPTION_KEY" = "PRGM_INST_PROGRAM_NAME_KEY"
    ),
    relationship = "many-to-many" # no reason is provided
  ) %>%
  mutate(
    New_Auto_Match = if_else(
      is.na(OUTCOMES_CIP_CODE_4) &
        is.na(OUTCOMES_CIP_CODE_4_NAME) &
        is.na(New_Auto_Match) &
        is.na(Already_Matched) &
        !is.na(XW_CIP4),
      "Yes",
      New_Auto_Match
    ),
    OUTCOMES_CIP_CODE_4 = if_else(
      is.na(OUTCOMES_CIP_CODE_4) & !is.na(XW_CIP4),
      XW_CIP4,
      OUTCOMES_CIP_CODE_4
    ),
    OUTCOMES_CIP_CODE_4_NAME = if_else(
      is.na(OUTCOMES_CIP_CODE_4_NAME) & !is.na(XW_CIP4_NAME),
      XW_CIP4_NAME,
      OUTCOMES_CIP_CODE_4_NAME
    )
  ) %>%
  select(-XW_CIP4, -XW_CIP4_NAME)

# ℹ Row 783 of `x` matches multiple rows in `y`.
# ℹ Row 1716 of `y` matches multiple rows in `x`.

stp_dacso <- stp_dacso %>% distinct() # this is not in the original code
# 5462 rows

## ---- Add to XWALK: newly matched STP programs ----
## qry_STP_Credential_DACSO_Programs_NewMatches_b ----

# join on PSI_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION = PRGM_INST_PROGRAM_NAME, PSI_PROGRAM_CODE = PRGM_LCPC_CD
# where New_Auto_Match = Yes, copy PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION, STP_CIP4_CODE into STP_CIP_CODE_4, CTP_SIP4_NAME into STP_CIP_CODE_4_NAME
# - Set New_STP_Program20XX = Yes and One_To_One_Match = Yes20XX
# WHY: When new STP programs are matched to DACSO entries in the XWALK, we update
# the XWALK with the STP program info so future runs can find them as "already matched".

# Get the newly matched STP programs
# Carry both the original columns and normalized keys so the join is easy to read.
newly_matched <- stp_dacso %>%
  filter(New_Auto_Match == "Yes") %>%
  select(
    PSI_PROGRAM_CODE,
    PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    PSI_CODE_KEY,
    PSI_PROGRAM_CODE_KEY,
    PSI_CREDENTIAL_PROGRAM_DESCRIPTION_KEY,
    STP_CIP_CODE_4,
    STP_CIP_CODE_4_NAME
  )
# 227

# Update XWALK for PSI_CODE matches
DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 <- DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 %>%
  left_join(
    newly_matched %>%
      rename(
        XW_STP_PGM_CODE = PSI_PROGRAM_CODE,
        XW_STP_PGM_DESC = PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
        XW_STP_CIP4 = STP_CIP_CODE_4,
        XW_STP_CIP4_NAME = STP_CIP_CODE_4_NAME
      ),
    by = c(
      "PSI_CODE_KEY" = "PSI_CODE_KEY",
      "PRGM_LCPC_CD_KEY" = "PSI_PROGRAM_CODE_KEY",
      "PRGM_INST_PROGRAM_NAME_KEY" = "PSI_CREDENTIAL_PROGRAM_DESCRIPTION_KEY"
    ),
    relationship = "many-to-many" # no reason is provided yet
  ) %>%
  mutate(
    PSI_PROGRAM_CODE = if_else(
      !is.na(XW_STP_PGM_CODE),
      XW_STP_PGM_CODE,
      PSI_PROGRAM_CODE
    ),
    PSI_CREDENTIAL_PROGRAM_DESC = if_else(
      !is.na(XW_STP_PGM_DESC),
      XW_STP_PGM_DESC,
      PSI_CREDENTIAL_PROGRAM_DESC
    ),
    STP_CIP4_CODE = if_else(
      !is.na(XW_STP_CIP4),
      XW_STP_CIP4,
      as.character(STP_CIP4_CODE)
    ),
    STP_CIP4_NAME = if_else(
      !is.na(XW_STP_CIP4_NAME),
      XW_STP_CIP4_NAME,
      STP_CIP4_NAME
    ),
    New_STP_Program2021_23 = if_else(
      !is.na(XW_STP_PGM_CODE),
      "Yes",
      New_STP_Program2021_23
    ),
    One_To_One_Match = if_else(
      !is.na(XW_STP_PGM_CODE),
      "Yes2021_23",
      One_To_One_Match
    )
  ) %>%
  select(-starts_with("XW_STP"))

#

DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 <- DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 %>%
  distinct()
# 4487

## qry_STP_Credential_DACSO_Programs_NewMatches_b_step2 ----
# same as above but join on COCI_INST_CD, PSI_CREDENTIAL_PROGRAM_DESCRIPTION = PRGM_INST_PROGRAM_NAME, PSI_PROGRAM_CODE = PRGM_LCPC_CD
# Update XWALK for COCI_INST_CD matches (only rows not yet updated)

newly_matched_2 <- stp_dacso %>%
  filter(New_Auto_Match == "Yes") %>%
  select(
    PSI_PROGRAM_CODE,
    PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
    COCI_INST_CD_KEY,
    PSI_PROGRAM_CODE_KEY,
    PSI_CREDENTIAL_PROGRAM_DESCRIPTION_KEY,
    STP_CIP_CODE_4,
    STP_CIP_CODE_4_NAME
  )

DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 <- DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 %>%
  left_join(
    newly_matched_2 %>%
      rename(
        XW_STP_PGM_CODE = PSI_PROGRAM_CODE,
        XW_STP_PGM_DESC = PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
        XW_STP_CIP4 = STP_CIP_CODE_4,
        XW_STP_CIP4_NAME = STP_CIP_CODE_4_NAME
      ),
    by = c(
      "COCI_INST_CD_KEY" = "COCI_INST_CD_KEY",
      "PRGM_LCPC_CD_KEY" = "PSI_PROGRAM_CODE_KEY",
      "PRGM_INST_PROGRAM_NAME_KEY" = "PSI_CREDENTIAL_PROGRAM_DESCRIPTION_KEY"
    ),
    relationship = "many-to-many" # no reason is provided yet
  ) %>%
  mutate(
    PSI_PROGRAM_CODE = if_else(
      is.na(New_STP_Program2021_23) & !is.na(XW_STP_PGM_CODE),
      XW_STP_PGM_CODE,
      PSI_PROGRAM_CODE
    ),
    PSI_CREDENTIAL_PROGRAM_DESC = if_else(
      is.na(New_STP_Program2021_23) & !is.na(XW_STP_PGM_DESC),
      XW_STP_PGM_DESC,
      PSI_CREDENTIAL_PROGRAM_DESC
    ),
    STP_CIP4_CODE = if_else(
      is.na(New_STP_Program2021_23) & !is.na(XW_STP_CIP4),
      XW_STP_CIP4,
      STP_CIP4_CODE
    ),
    STP_CIP4_NAME = if_else(
      is.na(New_STP_Program2021_23) & !is.na(XW_STP_CIP4_NAME),
      XW_STP_CIP4_NAME,
      STP_CIP4_NAME
    ),
    New_STP_Program2021_23 = if_else(
      is.na(New_STP_Program2021_23) & !is.na(XW_STP_PGM_CODE),
      "Yes",
      New_STP_Program2021_23
    ),
    One_To_One_Match = if_else(
      is.na(One_To_One_Match) & !is.na(XW_STP_PGM_CODE),
      "Yes2021_23",
      One_To_One_Match
    )
  ) %>%
  select(-starts_with("XW_STP"))

DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 <- DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 %>%
  distinct()
# 4487

# simplify the name
DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 <- DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 %>%
  refresh_xwalk_join_keys()

xwalk <- DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23
rm(newly_matched)
rm(newly_matched_2)
# ******************************************************************************
# PART 3: INSTITUTION-SPECIFIC CUSTOM MATCHING
# ******************************************************************************
# WHY: Some institutions use different program code formats in STP vs DACSO.
# BCIT includes credential suffixes, CAPU uses different code lengths, and VIU
# wraps codes in credential-category prefixes. We extract the DACSO-compatible
# portion and match on that.

## Find STP programs that are unmatched ----
stp_unmatched <- stp_dacso %>%
  filter(
    (is.na(OUTCOMES_CIP_CODE_4) | is.na(OUTCOMES_CIP_CODE_4_NAME)) &
      is.na(Already_Matched) &
      is.na(New_Auto_Match)
  )

# !!! review the unmatched STP programs and the XWALK to determine if these institution matches are still relevant

# reference: to check after each query to see counts of matched
# stp_dacso %>% count(New_Auto_Match)

# Helper function: match STP to XWALK on test code
#' Match STP programs to the DACSO XWALK using a test program code
#'
#' Perform an institution-aware join between STP program rows and the DACSO XWALK,
#' optionally including program descriptions in the join. When a match is found
#' the function sets New_Auto_Match to the supplied flag and populates the
#' OUTCOMES_CIP_CODE_4 and OUTCOMES_CIP_CODE_4_NAME fields (but only when they
#' are not already set).
#'
#' @param stp_df A data.frame / tibble of STP credential program rows. Expected
#'   to contain COCI_INST_CD and the test_col named column, and the OUTCOMES_*
#'   and New_Auto_Match columns that will be updated.
#' @param xwalk_df A data.frame / tibble of the DACSO XWALK. Expected to contain
#'   COCI_INST_CD, PRGM_LCPC_CD, PRGM_INST_PROGRAM_NAME, CIP_CODE_4 and
#'   LCP4_CIP_4DIGITS_NAME.
#' @param test_col Character. The name of the column in stp_df to use as the
#'   program code for matching (passed as a string).
#' @param match_flag Character. The value to set for New_Auto_Match when a match
#'   is made (for example, "YesXXBCIT").
#' @param join_cols_desc Logical. If TRUE include program description in the
#'   join (i.e. PSI_CREDENTIAL_PROGRAM_DESCRIPTION = PRGM_INST_PROGRAM_NAME);
#'   if FALSE join only on institution + code.
#' @return A tibble: a copy of stp_df with New_Auto_Match, OUTCOMES_CIP_CODE_4 and
#'   OUTCOMES_CIP_CODE_4_NAME updated for newly matched rows. Temporary CIP and
#'   name columns from the join are removed before returning.
#' @details The function filters xwalk_df to rows with non-missing COCI_INST_CD
#'   and PRGM_LCPC_CD, performs a left join according to the requested keys and
#'   then updates the STP outcome fields only when they are currently unset.
#' @examples
#' # match_on_test_code(stp_df, xwalk_df, \"BCIT_TEST_PROGRAM_CODE\", \"YesXXBCIT\")
#' @export
match_on_test_code <- function(
  stp_df,
  xwalk_df,
  test_col,
  match_flag,
  join_cols_desc = TRUE
) {
  # Generate a normalized key for the institution-specific test code, then use
  # a plain join on the generated keys. This keeps the matching rule visible in
  # the script and avoids hidden side effects from a custom join wrapper.
  stp_df <- stp_df %>%
    add_join_keys(c(TEST_PROGRAM_CODE_KEY = test_col))

  if (join_cols_desc) {
    stp_df <- stp_df %>%
      left_join(
        xwalk_df %>%
          filter(!is.na(COCI_INST_CD_KEY) & !is.na(PRGM_LCPC_CD_KEY)) %>%
          select(
            COCI_INST_CD_KEY,
            PRGM_LCPC_CD_KEY,
            PRGM_INST_PROGRAM_NAME_KEY,
            CIP_CODE_4,
            LCP4_CIP_4DIGITS_NAME
          ),
        by = c(
          "COCI_INST_CD_KEY",
          "TEST_PROGRAM_CODE_KEY" = "PRGM_LCPC_CD_KEY",
          "PSI_CREDENTIAL_PROGRAM_DESCRIPTION_KEY" = "PRGM_INST_PROGRAM_NAME_KEY"
        )
      )
  } else {
    stp_df <- stp_df %>%
      left_join(
        xwalk_df %>%
          filter(!is.na(COCI_INST_CD_KEY) & !is.na(PRGM_LCPC_CD_KEY)) %>%
          select(
            COCI_INST_CD_KEY,
            PRGM_LCPC_CD_KEY,
            CIP_CODE_4,
            LCP4_CIP_4DIGITS_NAME
          ),
        by = c(
          "COCI_INST_CD_KEY",
          "TEST_PROGRAM_CODE_KEY" = "PRGM_LCPC_CD_KEY"
        )
      )
  }

  stp_df %>%
    mutate(
      New_Auto_Match = if_else(
        is.na(OUTCOMES_CIP_CODE_4) &
          is.na(OUTCOMES_CIP_CODE_4_NAME) &
          is.na(New_Auto_Match) &
          !is.na(CIP_CODE_4),
        match_flag,
        New_Auto_Match
      ),
      OUTCOMES_CIP_CODE_4 = if_else(
        is.na(OUTCOMES_CIP_CODE_4) & !is.na(CIP_CODE_4),
        CIP_CODE_4,
        OUTCOMES_CIP_CODE_4
      ),
      OUTCOMES_CIP_CODE_4_NAME = if_else(
        is.na(OUTCOMES_CIP_CODE_4_NAME) & !is.na(LCP4_CIP_4DIGITS_NAME),
        LCP4_CIP_4DIGITS_NAME,
        OUTCOMES_CIP_CODE_4_NAME
      )
    ) %>%
    select(-CIP_CODE_4, -LCP4_CIP_4DIGITS_NAME, -TEST_PROGRAM_CODE_KEY)
}

## ---- Update BCIT ----
## BCIT submits CPC codes to STP that include the credential abbreviation suffix (e.g. _TTDIPL, _CERTTS...)
## but in DACSO their codes do not have the suffix
xwalk %>%
  filter(PSI_CODE == "BCIT") %>%
  pull(PRGM_LCPC_CD)
xwalk %>%
  filter(PSI_CODE == "BCIT") %>%
  pull(PSI_PROGRAM_CODE)

stp_unmatched %>% filter(PSI_CODE == "BCIT") %>% pull(PSI_PROGRAM_CODE)


# add a new program code column, and use that to match

# take the first 4 digits of the PSI_PROGRAM_CODE
# WHY: BCIT submits CPC codes with credential abbreviation suffixes (e.g., _TTDIPL)
# but DACSO codes don't have the suffix. Take first 4 characters of PSI_PROGRAM_CODE.
stp_dacso <- stp_dacso %>%
  mutate(
    BCIT_TEST_PROGRAM_CODE = if_else(
      PSI_CODE == "BCIT",
      substr(PSI_PROGRAM_CODE, 1, 4),
      NA_character_
    )
  )

# queries will set New_Auto_Match = 'YesXXBCIT'
# join STP data to XWALK on COCI_INST_CD, PSI_CREDENTIAL_PROGRAM_DESCRIPTION = PRGM_INST_PROGRAM_NAME, BCIT_TEST_PROGRAM_CODE = PRGM_LCPC_CD
# dbGetQuery(con, qry_Update_BCIT_Programs)
# BCIT: match with description
stp_dacso <- match_on_test_code(
  stp_dacso,
  xwalk,
  test_col = "BCIT_TEST_PROGRAM_CODE",
  match_flag = "Yes2021_23BCIT",
  join_cols_desc = TRUE
)
# ℹ Row 7 of `x` matches multiple rows in `y`.
# ℹ Row 2739 of `y` matches multiple rows in `x`.

stp_dacso <- stp_dacso %>% distinct() # this is not in the original code
# 5462

# join STP data to XWALK on COCI_INST_CD, BCIT_TEST_PROGRAM_CODE = PRGM_LCPC_CD without PSI_CREDENTIAL_PROGRAM_DESCRIPTION
# dbGetQuery(con, qry_Update_BCIT_Programs_b)
# BCIT: match without description (code only)
stp_dacso <- match_on_test_code(
  stp_dacso,
  xwalk,
  "BCIT_TEST_PROGRAM_CODE",
  "Yes2021_23BCIT",
  join_cols_desc = FALSE
)
# ℹ Row 7 of `x` matches multiple rows in `y`.
# ℹ Row 2739 of `y` matches multiple rows in `x`
# less constrain in join, so more duplications
stp_dacso <- stp_dacso %>% distinct() # this is not in the original code
# 5462

## ---- Update CAPU ----
# CAPU submits CPC codes to STP that are 6 digits long, but in DACSO they are generally 3 or 4 digits long
# run through twice - once for 4 digits, once for 3 digits
# some also seem to have -YYY after CPC codes in STP but not in DACSO
xwalk %>%
  filter(PSI_CODE == "CAP") %>%
  pull(PRGM_LCPC_CD)
xwalk %>%
  filter(PSI_CODE == "CAP") %>%
  pull(PSI_PROGRAM_CODE)
stp_unmatched %>% filter(PSI_CODE == "CAPU") %>% pull(PSI_PROGRAM_CODE)


# remove dashes
# WHY: CAPU codes are 6 digits in STP but 3-4 digits in DACSO. Some also have
# dash suffixes. Try multiple code lengths.
stp_dacso <- stp_dacso %>%
  mutate(
    CAP_TEST_PROGRAM_CODE = if_else(
      COCI_INST_CD == "CAPU" & grepl("-", PSI_PROGRAM_CODE),
      substr(
        PSI_PROGRAM_CODE,
        1,
        regexpr("-", PSI_PROGRAM_CODE, fixed = TRUE) - 1
      ),
      NA_character_
    )
  )

# Match with dash-removed codes + description
stp_dacso <- match_on_test_code(
  stp_dacso,
  xwalk,
  "CAP_TEST_PROGRAM_CODE",
  "Yes2021_23CAPU",
  join_cols_desc = TRUE
)

# ℹ Row 2269 of `x` matches multiple rows in `y`.
# ℹ Row 4175 of `y` matches multiple rows in `x`.
stp_dacso <- stp_dacso %>% distinct() # this is not in the original code
# 5462

stp_dacso <- match_on_test_code(
  stp_dacso,
  xwalk,
  "CAP_TEST_PROGRAM_CODE",
  "Yes2021_23CAPU",
  join_cols_desc = FALSE
)
# ℹ Row 1647 of `x` matches multiple rows in `y`.
# ℹ Row 4175 of `y` matches multiple rows in `x`.
stp_dacso <- stp_dacso %>% distinct() # this is not in the original code
# 5462

# 4 digits, another way of matching
# Try 4-digit prefix
stp_dacso <- stp_dacso %>%
  mutate(
    CAP_TEST_PROGRAM_CODE = if_else(
      COCI_INST_CD == "CAPU",
      substr(PSI_PROGRAM_CODE, 1, 4),
      CAP_TEST_PROGRAM_CODE
    )
  )

stp_dacso <- match_on_test_code(
  stp_dacso,
  xwalk,
  "CAP_TEST_PROGRAM_CODE",
  "Yes2021_23CAPU",
  join_cols_desc = TRUE
)

# ℹ Row 1202 of `x` matches multiple rows in `y`.
# ℹ Row 1684 of `y` matches multiple rows in `x`.
stp_dacso <- stp_dacso %>% distinct() # this is not in the original code
# 5462
stp_dacso <- match_on_test_code(
  stp_dacso,
  xwalk,
  "CAP_TEST_PROGRAM_CODE",
  "Yes2021_23CAPU",
  join_cols_desc = FALSE
)
# ℹ Row 1200 of `x` matches multiple rows in `y`.
# ℹ Row 1684 of `y` matches multiple rows in `x`.
stp_dacso <- stp_dacso %>% distinct() # this is not in the original code
# 5462

# 3 digits, another way of matching

# Try 3-digit prefix
stp_dacso <- stp_dacso %>%
  mutate(
    CAP_TEST_PROGRAM_CODE = if_else(
      COCI_INST_CD == "CAPU",
      substr(PSI_PROGRAM_CODE, 1, 3),
      CAP_TEST_PROGRAM_CODE
    )
  )

stp_dacso <- match_on_test_code(
  stp_dacso,
  xwalk,
  "CAP_TEST_PROGRAM_CODE",
  "Yes2021_23CAPU",
  join_cols_desc = TRUE
)
# ℹ Row 1348 of `x` matches multiple rows in `y`.
# ℹ Row 2219 of `y` matches multiple rows in `x`.
stp_dacso <- stp_dacso %>% distinct() # this is not in the original code
# 5462
stp_dacso <- match_on_test_code(
  stp_dacso,
  xwalk,
  "CAP_TEST_PROGRAM_CODE",
  "Yes2021_23CAPU",
  join_cols_desc = FALSE
)
# ℹ Row 1151 of `x` matches multiple rows in `y`.
# ℹ Row 2219 of `y` matches multiple rows in `x`.
stp_dacso <- stp_dacso %>% distinct() # this is not in the original code
# 5462

## ---- Update VIU ----
# STP versions seem longer (e.g., CERT-WELDM_01) versus DACSO (e.g.,WELDM)
# STP has the credential category (e.g., CERT) dash DACSO CPC followed by _01 or similar
# WHY: VIU codes in STP are like "CERT-WELDM_01" but DACSO just has "WELDM".
# Extract the substring between "-" and "_".
xwalk %>%
  filter(PSI_CODE == "VIU") %>%
  pull(PRGM_LCPC_CD)
stp_unmatched %>% filter(PSI_CODE == "VIU") %>% pull(PSI_PROGRAM_CODE)

stp_dacso <- stp_dacso %>%
  mutate(
    VIU_TEST_PROGRAM_CODE = if_else(
      PSI_CODE == "VIU" &
        grepl("-", PSI_PROGRAM_CODE) &
        grepl("_", PSI_PROGRAM_CODE),
      substr(
        PSI_PROGRAM_CODE,
        regexpr("-", PSI_PROGRAM_CODE, fixed = TRUE) + 1,
        regexpr("_", PSI_PROGRAM_CODE, fixed = TRUE) - 1
      ),
      NA_character_
    )
  )

stp_dacso <- match_on_test_code(
  stp_dacso,
  xwalk,
  "VIU_TEST_PROGRAM_CODE",
  "Yes2021_23VIU",
  join_cols_desc = TRUE
)
stp_dacso <- match_on_test_code(
  stp_dacso,
  xwalk,
  "VIU_TEST_PROGRAM_CODE",
  "Yes2021_23VIU",
  join_cols_desc = FALSE
)
# ℹ Row 5389 of `x` matches multiple rows in `y`.
# ℹ Row 936 of `y` matches multiple rows in `x`.
stp_dacso <- stp_dacso %>% distinct() # this is not in the original code
# 5462

## Update remaining matching ----

# ---- Remaining catch-all matching ----
# WHY: Try matching remaining unmatched programs on COCI_INST_CD + program code,
# then on COCI_INST_CD + program description.

# Match on COCI_INST_CD + PSI_PROGRAM_CODE=PRGM_LCPC_CD
xwalk_remaining <- xwalk %>%
  filter(!is.na(COCI_INST_CD) & !is.na(PRGM_LCPC_CD)) %>%
  select(COCI_INST_CD_KEY, PRGM_LCPC_CD_KEY, CIP_CODE_4, LCP4_CIP_4DIGITS_NAME)

stp_dacso <- stp_dacso %>%
  left_join(
    xwalk_remaining %>%
      rename(
        XW_CIP4 = CIP_CODE_4,
        XW_CIP4_NAME = LCP4_CIP_4DIGITS_NAME
      ),
    by = c("COCI_INST_CD_KEY", "PSI_PROGRAM_CODE_KEY" = "PRGM_LCPC_CD_KEY"),
    relationship = "many-to-many" # no reason is provided yet
  ) %>%
  mutate(
    New_Auto_Match = if_else(
      is.na(OUTCOMES_CIP_CODE_4) &
        is.na(OUTCOMES_CIP_CODE_4_NAME) &
        is.na(New_Auto_Match) &
        !is.na(XW_CIP4),
      "Yes_2021_23test",
      New_Auto_Match
    ),
    OUTCOMES_CIP_CODE_4 = if_else(
      is.na(OUTCOMES_CIP_CODE_4) & !is.na(XW_CIP4),
      XW_CIP4,
      OUTCOMES_CIP_CODE_4
    ),
    OUTCOMES_CIP_CODE_4_NAME = if_else(
      is.na(OUTCOMES_CIP_CODE_4_NAME) & !is.na(XW_CIP4_NAME),
      XW_CIP4_NAME,
      OUTCOMES_CIP_CODE_4_NAME
    )
  ) %>%
  select(-XW_CIP4, -XW_CIP4_NAME)


# ℹ Row 841 of `x` matches multiple rows in `y`.
# ℹ Row 1651 of `y` matches multiple rows in `x`.
stp_dacso <- stp_dacso %>% distinct() # this is not in the original code
# 5462

# Match on COCI_INST_CD + PSI_CREDENTIAL_PROGRAM_DESCRIPTION=PRGM_INST_PROGRAM_NAME
xwalk_remaining_desc <- xwalk %>%
  filter(!is.na(COCI_INST_CD) & !is.na(PRGM_INST_PROGRAM_NAME)) %>%
  select(
    COCI_INST_CD_KEY,
    PRGM_INST_PROGRAM_NAME_KEY,
    CIP_CODE_4,
    LCP4_CIP_4DIGITS_NAME
  )

stp_dacso <- stp_dacso %>%
  left_join(
    xwalk_remaining_desc %>%
      rename(XW_CIP4 = CIP_CODE_4, XW_CIP4_NAME = LCP4_CIP_4DIGITS_NAME),
    by = c(
      "COCI_INST_CD_KEY",
      "PSI_CREDENTIAL_PROGRAM_DESCRIPTION_KEY" = "PRGM_INST_PROGRAM_NAME_KEY"
    ),
    # keep = TRUE,
    relationship = "many-to-many" # no reason is provided yet
  ) %>%
  mutate(
    New_Auto_Match = if_else(
      is.na(OUTCOMES_CIP_CODE_4) &
        is.na(OUTCOMES_CIP_CODE_4_NAME) &
        is.na(New_Auto_Match) &
        !is.na(XW_CIP4),
      "Yes_2021_23test",
      New_Auto_Match
    ),
    OUTCOMES_CIP_CODE_4 = if_else(
      is.na(OUTCOMES_CIP_CODE_4) & !is.na(XW_CIP4),
      XW_CIP4,
      OUTCOMES_CIP_CODE_4
    ),
    OUTCOMES_CIP_CODE_4_NAME = if_else(
      is.na(OUTCOMES_CIP_CODE_4_NAME) & !is.na(XW_CIP4_NAME),
      XW_CIP4_NAME,
      OUTCOMES_CIP_CODE_4_NAME
    )
  ) %>%
  select(-XW_CIP4, -XW_CIP4_NAME)

stp_dacso <- stp_dacso %>% distinct() # this is not in the original code
# 5462
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

## ---- Update STP_Credential_Non_Dup_Programs_DACSO with final CIPS ----
# Use the outcomes cip4 data if there was a match for the final cip4
# Step 1: Where outcomes CIP exists, use it as final
stp_dacso <- stp_dacso %>%
  mutate(
    FINAL_CIP_CODE_4 = if_else(
      !is.na(OUTCOMES_CIP_CODE_4) & !is.na(OUTCOMES_CIP_CODE_4_NAME),
      OUTCOMES_CIP_CODE_4,
      FINAL_CIP_CODE_4
    ),
    FINAL_CIP_CODE_4_NAME = if_else(
      !is.na(OUTCOMES_CIP_CODE_4) & !is.na(OUTCOMES_CIP_CODE_4_NAME),
      OUTCOMES_CIP_CODE_4_NAME,
      FINAL_CIP_CODE_4_NAME
    )
  )

# Step 2: Where no outcomes match, use INFOWARE to derive CIP from PSI_CREDENTIAL_CIP
# WHY: This fills FINAL_CIP for programs that weren't matched to DACSO outcomes.
# It uses the full CIP hierarchy (6-digit → 4-digit → 2-digit → names → cluster).
cip6_full <- cip6 %>%
  select(
    LCIP_CD_WITH_PERIOD,
    LCIP_LCP4_CD,
    LCIP_LCP2_CD,
    LCIP_LCIPPC_CD,
    LCIP_LCIPPC_NAME
  ) %>%
  inner_join(
    cip4_ref %>% select(LCP4_CD, LCP4_CIP_4DIGITS_NAME),
    by = c("LCIP_LCP4_CD" = "LCP4_CD")
  ) %>%
  inner_join(
    cip2_ref %>% select(LCP2_CD, LCP2_DIGITS_NAME),
    by = c("LCIP_LCP2_CD" = "LCP2_CD")
  )

# Use the STP CIP4 outcomes for the rest where there is no match
stp_dacso <- stp_dacso %>%
  left_join(
    cip6_full %>%
      select(
        LCIP_CD_WITH_PERIOD,
        LCIP_LCP4_CD,
        LCP4_CIP_4DIGITS_NAME,
        LCIP_LCP2_CD,
        LCP2_DIGITS_NAME
      ),
    by = c("PSI_CREDENTIAL_CIP" = "LCIP_CD_WITH_PERIOD")
  ) %>%
  mutate(
    FINAL_CIP_CODE_4 = if_else(
      is.na(FINAL_CIP_CODE_4) &
        is.na(FINAL_CIP_CODE_4_NAME) &
        is.na(FINAL_CIP_CODE_2) &
        is.na(FINAL_CIP_CODE_2_NAME) &
        is.na(OUTCOMES_CIP_CODE_4),
      LCIP_LCP4_CD,
      FINAL_CIP_CODE_4
    ),
    FINAL_CIP_CODE_4_NAME = if_else(
      is.na(FINAL_CIP_CODE_4_NAME) &
        is.na(FINAL_CIP_CODE_2) &
        is.na(FINAL_CIP_CODE_2_NAME) &
        is.na(OUTCOMES_CIP_CODE_4),
      LCP4_CIP_4DIGITS_NAME,
      FINAL_CIP_CODE_4_NAME
    ),
    FINAL_CIP_CODE_2 = if_else(
      is.na(FINAL_CIP_CODE_2) &
        is.na(FINAL_CIP_CODE_2_NAME) &
        is.na(OUTCOMES_CIP_CODE_4),
      LCIP_LCP2_CD,
      FINAL_CIP_CODE_2
    ),
    FINAL_CIP_CODE_2_NAME = if_else(
      is.na(FINAL_CIP_CODE_2_NAME) & is.na(OUTCOMES_CIP_CODE_4),
      LCP2_DIGITS_NAME,
      FINAL_CIP_CODE_2_NAME
    )
  ) %>%
  select(
    -LCIP_LCP4_CD,
    -LCP4_CIP_4DIGITS_NAME,
    -LCIP_LCP2_CD,
    -LCP2_DIGITS_NAME
  )


# Step 3: Fill remaining NULL FINAL_CIP columns from the 4-digit code
# WHY: Some records got FINAL_CIP_CODE_4 from Step 1 (outcomes) but still need
# 4-digit name, 2-digit code, 2-digit name, and cluster codes.
cip4_lookup <- cip6 %>%
  select(LCIP_LCP4_CD, LCIP_LCP2_CD) %>%
  distinct() %>%
  inner_join(
    cip4_ref %>% select(LCP4_CD, LCP4_CIP_4DIGITS_NAME),
    by = c("LCIP_LCP4_CD" = "LCP4_CD")
  ) %>%
  inner_join(
    cip2_ref %>% select(LCP2_CD, LCP2_DIGITS_NAME),
    by = c("LCIP_LCP2_CD" = "LCP2_CD")
  )

stp_dacso <- stp_dacso %>%
  left_join(
    cip4_lookup,
    by = c("FINAL_CIP_CODE_4" = "LCIP_LCP4_CD")
  ) %>%
  mutate(
    FINAL_CIP_CODE_4_NAME = coalesce(
      FINAL_CIP_CODE_4_NAME,
      LCP4_CIP_4DIGITS_NAME
    ),
    FINAL_CIP_CODE_2 = coalesce(FINAL_CIP_CODE_2, LCIP_LCP2_CD),
    FINAL_CIP_CODE_2_NAME = coalesce(FINAL_CIP_CODE_2_NAME, LCP2_DIGITS_NAME)
  ) %>%
  select(-LCIP_LCP2_CD, -LCP4_CIP_4DIGITS_NAME, -LCP2_DIGITS_NAME)


# To update the final cip 2 and  final cip cluster based on the final cip4

# Step 4: Fill FINAL_CIP_CLUSTER from FINAL_CIP_CODE_4
# WHY: The 6-digit CIP taxonomy maps each 4-digit code to a cluster code.
cip6_cluster <- cip6 %>%
  select(LCIP_LCP4_CD, LCIP_LCP2_CD, LCIP_LCIPPC_CD, LCIP_LCIPPC_NAME) %>%
  distinct()

stp_dacso <- stp_dacso %>%
  left_join(cip6_cluster, by = c("FINAL_CIP_CODE_4" = "LCIP_LCP4_CD")) %>%
  mutate(
    FINAL_CIP_CODE_2 = coalesce(FINAL_CIP_CODE_2, LCIP_LCP2_CD),
    FINAL_CIP_CLUSTER_CODE = coalesce(FINAL_CIP_CLUSTER_CODE, LCIP_LCIPPC_CD),
    FINAL_CIP_CLUSTER_NAME = coalesce(FINAL_CIP_CLUSTER_NAME, LCIP_LCIPPC_NAME)
  ) %>%
  select(-LCIP_LCP2_CD, -LCIP_LCIPPC_CD, -LCIP_LCIPPC_NAME)


# To update the final cip 2 name based on the final cip2
# Step 5: Fill FINAL_CIP_CODE_2_NAME from 2-digit lookup
stp_dacso <- stp_dacso %>%
  left_join(
    cip2_ref %>% select(LCP2_CD, LCP2_DIGITS_NAME),
    by = c("FINAL_CIP_CODE_2" = "LCP2_CD")
  ) %>%
  mutate(
    FINAL_CIP_CODE_2_NAME = coalesce(FINAL_CIP_CODE_2_NAME, LCP2_DIGITS_NAME)
  ) %>%
  select(-LCP2_DIGITS_NAME)


# still
# 5462

# check the # of changed CIPS
# ---- Review CIP changes ----
# WHY: Diagnostic — shows programs where the final CIP differs from the original
# STP CIP. Useful for catching incorrect matches.
review_changed_cips <- stp_dacso %>%
  filter(FINAL_CIP_CODE_4 != STP_CIP_CODE_4)
# 193
# ---- Write final output tables ----
# Drop helper *_KEY columns before writing final outputs.
stp_dacso_out <- drop_join_keys(stp_dacso)
DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23_out <- drop_join_keys(
  DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23
)

dbWriteTable(
  con,
  "STP_Credential_Non_Dup_Programs_DACSO_r",
  stp_dacso_out,
  overwrite = TRUE
)
dbWriteTable(
  con,
  "Credential_Non_Dup_Programs_DACSO_FinalCIPS_r",
  stp_dacso_out,
  overwrite = TRUE
)
dbWriteTable(
  con,
  "DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23_r",
  DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23_out,
  overwrite = TRUE
)

dbDisconnect(con)
