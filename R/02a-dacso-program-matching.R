
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

## ---- Read in INFOWARE tables ----
iw_config <- config::get("infoware")
jdbc_config <- config::get("jdbc")

jdbcDriver <- JDBC(jdbc_config$class,
                   classPath=jdbc_config$path)

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

## ---- Read in last years XWALK ----
## connect to outcomes (access) database
connection <- config::get("connection")$outcomes_dacso
acc_con <- odbcDriverConnect(connection)

DACSO_STP_ProgramsCIP4_XWALK_ALL_2020 <- sqlQuery(acc_con, "SELECT * FROM DACSO_STP_ProgramsCIP4_XWALK_ALL_2020;")

odbcClose(acc_con)

## ---- Connect to Decimal ----
config <- config::get("decimal")
con <- dbConnect(odbc(),
                     Driver = config$driver,
                     Server = config$server,
                     Database = config$database,
                     Trusted_Connection = "True")
my_schema <- config::get("myschema")

## ---- Write initial tables to Decimal ----
## Save static versions of the INFOWARE tables and last cycle XWALK to Decimal
dbWriteTable(con, SQL(glue::glue('"{my_schema}"."DACSO_STP_ProgramsCIP4_XWALK_ALL_2020"')), DACSO_STP_ProgramsCIP4_XWALK_ALL_2020)
dbWriteTable(con, SQL(glue::glue('"{my_schema}"."INFOWARE_PROGRAMS"')), INFOWARE_PROGRAMS)
dbWriteTable(con, SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_6DIGITS_CIP2016"')), INFOWARE_L_CIP_6DIGITS_CIP2016)
dbWriteTable(con, SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_4DIGITS_CIP2016"')), INFOWARE_L_CIP_4DIGITS_CIP2016)
dbWriteTable(con, SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_2DIGITS_CIP2016"')), INFOWARE_L_CIP_2DIGITS_CIP2016)
dbWriteTable(con, SQL(glue::glue('"{my_schema}"."INFOWARE_PROGRAMS_HIST_PRGMID_XREF"')), INFOWARE_PROGRAMS_HIST_PRGMID_XREF)

## remove tables and use decimal versions for remainder of code
rm(DACSO_STP_ProgramsCIP4_XWALK_ALL_2020, INFOWARE_PROGRAMS, INFOWARE_L_CIP_6DIGITS_CIP2016, 
   INFOWARE_L_CIP_4DIGITS_CIP2016, INFOWARE_L_CIP_2DIGITS_CIP2016, INFOWARE_PROGRAMS_HIST_PRGMID_XREF )

# Part 1 ----

## ---- Create programs_table from combining INFOWARE tables ----
## define programs_table from which to grab new programs (with and without historical linkages)
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

## ---- Add to XWALK: New DACSO prgms WITHOUT historical linkages ----
## review the HAS_HISTORICAL_PRGM_ID_LINK values
programs_table %>%
  filter(PRGM_FIRST_SEEN_SUBM_CD %in% c('C_Outc21','C_Outc22','C_Outc23')) %>% 
  group_by(PRGM_FIRST_SEEN_SUBM_CD,HAS_HISTORICAL_PRGM_ID_LINK) %>% tally()

new_dacso_programs_21_23 <- programs_table %>%
  filter(PRGM_FIRST_SEEN_SUBM_CD %in% c('C_Outc21','C_Outc22','C_Outc23') & (is.na(HAS_HISTORICAL_PRGM_ID_LINK) | HAS_HISTORICAL_PRGM_ID_LINK==" "))
new_dacso_programs_21_23 %>% count(PRGM_FIRST_SEEN_SUBM_CD)

DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 <- DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 %>%
  bind_rows(
    programs_table %>%
      filter(PRGM_FIRST_SEEN_SUBM_CD %in% c('C_Outc21','C_Outc22','C_Outc23') & (is.na(HAS_HISTORICAL_PRGM_ID_LINK) | HAS_HISTORICAL_PRGM_ID_LINK==" ")) %>%
      mutate(New_DACSO_Program2021_23 = case_when(PRGM_FIRST_SEEN_SUBM_CD == "C_Outc21" ~ "Yes2021",
                                                   PRGM_FIRST_SEEN_SUBM_CD == "C_Outc22" ~ "Yes2022",
                                                   PRGM_FIRST_SEEN_SUBM_CD == "C_Outc23"~ "Yes2023")) %>%
      select(COCI_INST_CD = PRGM_INST_CD,
             PRGM_LCPC_CD,
             PRGM_INST_PROGRAM_NAME,
             CIP_CODE_4 = LCIP_LCP4_CD,
             LCP4_CIP_4DIGITS_NAME,
             PRGM_ID,
             PRGM_CREDENTIAL,
             New_DACSO_Program2021_23))

# The following steps are repeated for each year since last PSSM:
##  Find DACSO prgms WITH historical linkages
##  Get historical linkages for DACSO prgms 
##  Update to XWALK: Updated DACSO programs WITH historical linkages
##  Find Remaining updated DACSO missing from XWALK for match to STP program

## ---- 2021 Find DACSO prgms WITH historical linkages ----
Updated_DACSO_Programs_in_2021_with_links <- programs_table %>%
  filter(PRGM_FIRST_SEEN_SUBM_CD =='C_Outc21' & HAS_HISTORICAL_PRGM_ID_LINK =='Y') %>%
  inner_join(tbl(con, "INFOWARE_PROGRAMS_HIST_PRGMID_XREF") %>%
               filter(YEAR_LINK_CREATED =='C_Outc21' & SURVEY_CODE=='DACSO') %>%
               collect(), 
             by = "PRGM_ID") %>%
  select(PRGM_ID, PRGM_FIRST_SEEN_SUBM_CD, PRGM_INST_CD, PRGM_LCPC_CD, PRGM_INST_PROGRAM_NAME, 
         PRGM_TTRAIN_FLAG, PRGM_CREDENTIAL, PRGM_INST_PROGRAM_NAME_CLEANED, NOTES, 
         HAS_HISTORICAL_PRGM_ID_LINK, DUP_PROGRAM_USE_THIS_PRGM_ID, CIP_CLUSTER_ARTS_APPLIED, 
         DACSO_OLD_PRGM_ID_DO_NOT_USE, LCIP_CD_CIP2016, LCIP_NAME_CIP2016, LCIP_LCP4_CD, 
         LCP4_CIP_4DIGITS_NAME, HISTORICAL_PRGM_ID, YEAR_LINK_CREATED, SURVEY_CODE)

## ---- 2021 Get historical linkages for DACSO prgms ----
## use the historical linkage added from INFOWARE_PROGRAMS_HIST_PRGMID_XREF 
## to link back to the programs table to fill in the historical program details
Updated_DACSO_Programs_in_2021_with_links <- Updated_DACSO_Programs_in_2021_with_links %>%
  inner_join(
    programs_table %>%
      select(PRGM_ID, HISTORICAL_CPC_CD = PRGM_LCPC_CD, 
             HISTORICAL_PROGRAM_NAME = PRGM_INST_PROGRAM_NAME, 
             HISTORICAL_CIP4_CD = LCIP_LCP4_CD),
    by = c(HISTORICAL_PRGM_ID = "PRGM_ID")
  ) %>%
   mutate(Updated_CPC_Flag = case_when(PRGM_LCPC_CD != HISTORICAL_CPC_CD ~ 'Yes', 
                                       TRUE ~ NA),
         Updated_CIP_Flag = case_when(LCIP_LCP4_CD != HISTORICAL_CIP4_CD ~ 'Yes', 
                                      TRUE ~ NA))

## ---- 2021 Update to XWALK: Updated DACSO programs WITH historical linkages ----
DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 <- DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 %>%
  left_join(
    Updated_DACSO_Programs_in_2021_with_links %>%
      mutate(HISTORICAL_CPC_CD = as.character(HISTORICAL_CPC_CD)) %>%
      select(PRGM_INST_CD, HISTORICAL_CPC_CD, HISTORICAL_PROGRAM_NAME, HISTORICAL_CIP4_CD,
             PRGM_LCPC_CD, PRGM_INST_PROGRAM_NAME, LCIP_LCP4_CD, LCP4_CIP_4DIGITS_NAME,
             Updated_DACSO_CPC2021 = Updated_CPC_Flag, Updated_DACSO_CIP2021 = Updated_CIP_Flag),
    by = c(COCI_INST_CD = "PRGM_INST_CD", PRGM_LCPC_CD = "HISTORICAL_CPC_CD", 
           PRGM_INST_PROGRAM_NAME = "HISTORICAL_PROGRAM_NAME", CIP_CODE_4 = "HISTORICAL_CIP4_CD")) %>%
  mutate(PRGM_LCPC_CD = ifelse(!is.na(PRGM_LCPC_CD.y), PRGM_LCPC_CD.y, PRGM_LCPC_CD),
         PRGM_INST_PROGRAM_NAME = ifelse(!is.na(PRGM_INST_PROGRAM_NAME.y), PRGM_INST_PROGRAM_NAME.y, PRGM_INST_PROGRAM_NAME),
         CIP_CODE_4 = ifelse(!is.na(LCIP_LCP4_CD), LCIP_LCP4_CD, CIP_CODE_4)) %>%
  mutate(LCP4_CIP_4DIGITS_NAME = ifelse(!is.na(LCP4_CIP_4DIGITS_NAME.y), LCP4_CIP_4DIGITS_NAME.y, LCP4_CIP_4DIGITS_NAME.x), .after = "CIP_CODE_4") %>%
  select(-ends_with(".x"), -ends_with(".y"), -LCIP_LCP4_CD)

## ---- 2021 Find Remaining updated DACSO missing from XWALK for match to STP program ----
Remaining_DACSO_Updates_CPCS_2021 <- Updated_DACSO_Programs_in_2021_with_links %>%
  anti_join(DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23,
            by = c(PRGM_LCPC_CD = "PRGM_LCPC_CD", PRGM_INST_CD = "COCI_INST_CD",
                   PRGM_INST_PROGRAM_NAME = "PRGM_INST_PROGRAM_NAME"))

# ***** manual work needed *****
# review infoware notes
programs_table %>% filter(PRGM_ID %in% Remaining_DACSO_Updates_CPCS_2021$PRGM_ID) %>% pull(PRGM_ID,NOTES)

# PRGM_ID 10132 links to 3119
programs_table %>% filter(PRGM_ID == "3119") %>% pull(NOTES)

# update based on review
DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 <- DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 %>%
  mutate(CIP_CODE_4 = case_when(PRGM_ID == 3119 ~ "1907", 
                                TRUE ~ CIP_CODE_4),
         LCP4_CIP_4DIGITS_NAME = case_when(PRGM_ID == 3119 ~ "Human development, family studies and related services", 
                                           TRUE ~ LCP4_CIP_4DIGITS_NAME),
         Updated_DACSO_CIP2021 = case_when(PRGM_ID == 3119 ~ "Yes", 
                                           TRUE ~ Updated_DACSO_CIP2021),
         PRGM_LCPC_CD = case_when(PRGM_ID == 3119 ~ "EACSW", 
                                TRUE ~ PRGM_LCPC_CD),
         PRGM_INST_PROGRAM_NAME = case_when(PRGM_ID == 3119 ~ "Education Assistant and Community Support Worker", 
                                           TRUE ~ PRGM_INST_PROGRAM_NAME),
         Updated_DACSO_CPC2021 = case_when(PRGM_ID == 3119 ~ "Yes", 
                                           TRUE ~ Updated_DACSO_CPC2021))

## ---- 2022 Find DACSO prgms WITH historical linkages ----
Updated_DACSO_Programs_in_2022_with_links <- programs_table %>%
  filter(PRGM_FIRST_SEEN_SUBM_CD =='C_Outc22' & HAS_HISTORICAL_PRGM_ID_LINK =='Y') %>%
  inner_join(tbl(con, "INFOWARE_PROGRAMS_HIST_PRGMID_XREF") %>%
               filter(YEAR_LINK_CREATED =='C_Outc22' & SURVEY_CODE=='DACSO') %>%
               collect(), 
             by = "PRGM_ID") %>%
  select(PRGM_ID, PRGM_FIRST_SEEN_SUBM_CD, PRGM_INST_CD, PRGM_LCPC_CD, PRGM_INST_PROGRAM_NAME, 
         PRGM_TTRAIN_FLAG, PRGM_CREDENTIAL, PRGM_INST_PROGRAM_NAME_CLEANED, NOTES, 
         HAS_HISTORICAL_PRGM_ID_LINK, DUP_PROGRAM_USE_THIS_PRGM_ID, CIP_CLUSTER_ARTS_APPLIED, 
         DACSO_OLD_PRGM_ID_DO_NOT_USE, LCIP_CD_CIP2016, LCIP_NAME_CIP2016, LCIP_LCP4_CD, 
         LCP4_CIP_4DIGITS_NAME, HISTORICAL_PRGM_ID, YEAR_LINK_CREATED, SURVEY_CODE)


## ---- 2022 Get historical linkages for DACSO prgms ----
## use the historical linkage added from INFOWARE_PROGRAMS_HIST_PRGMID_XREF 
## to link back to the programs table to fill in the historical program details
Updated_DACSO_Programs_in_2022_with_links <- Updated_DACSO_Programs_in_2022_with_links %>%
  inner_join(
    programs_table %>%
      select(PRGM_ID, HISTORICAL_CPC_CD = PRGM_LCPC_CD, 
             HISTORICAL_PROGRAM_NAME = PRGM_INST_PROGRAM_NAME, 
             HISTORICAL_CIP4_CD = LCIP_LCP4_CD),
    by = c(HISTORICAL_PRGM_ID = "PRGM_ID")
  ) %>%
  mutate(Updated_CPC_Flag = case_when(PRGM_LCPC_CD != HISTORICAL_CPC_CD ~ 'Yes', 
                                      TRUE ~ NA),
         Updated_CIP_Flag = case_when(LCIP_LCP4_CD != HISTORICAL_CIP4_CD ~ 'Yes', 
                                      TRUE ~ NA))

## ---- 2022 Update to XWALK: Updated DACSO programs WITH historical linkages ----
DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 <- DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 %>%
  left_join(
    Updated_DACSO_Programs_in_2022_with_links %>%
      mutate(HISTORICAL_CPC_CD = as.character(HISTORICAL_CPC_CD)) %>%
      select(PRGM_INST_CD, HISTORICAL_CPC_CD, HISTORICAL_PROGRAM_NAME, HISTORICAL_CIP4_CD,
             PRGM_LCPC_CD, PRGM_INST_PROGRAM_NAME, LCIP_LCP4_CD, LCP4_CIP_4DIGITS_NAME,
             Updated_DACSO_CPC2022 = Updated_CPC_Flag, Updated_DACSO_CIP2022 = Updated_CIP_Flag),
    by = c(COCI_INST_CD = "PRGM_INST_CD", PRGM_LCPC_CD = "HISTORICAL_CPC_CD", 
           PRGM_INST_PROGRAM_NAME = "HISTORICAL_PROGRAM_NAME", CIP_CODE_4 = "HISTORICAL_CIP4_CD")) %>%
  mutate(PRGM_LCPC_CD = ifelse(!is.na(PRGM_LCPC_CD.y), PRGM_LCPC_CD.y, PRGM_LCPC_CD),
         PRGM_INST_PROGRAM_NAME = ifelse(!is.na(PRGM_INST_PROGRAM_NAME.y), PRGM_INST_PROGRAM_NAME.y, PRGM_INST_PROGRAM_NAME),
         CIP_CODE_4 = ifelse(!is.na(LCIP_LCP4_CD), LCIP_LCP4_CD, CIP_CODE_4)) %>%
  mutate(LCP4_CIP_4DIGITS_NAME = ifelse(!is.na(LCP4_CIP_4DIGITS_NAME.y), LCP4_CIP_4DIGITS_NAME.y, LCP4_CIP_4DIGITS_NAME.x), .after = "CIP_CODE_4") %>%
  select(-ends_with(".x"), -ends_with(".y"), -LCIP_LCP4_CD)

## ---- 2022 Find Remaining updated DACSO missing from XWALK for match to STP program ----
Remaining_DACSO_Updates_CPCS_2022 <- Updated_DACSO_Programs_in_2022_with_links %>%
  anti_join(DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23,
            by = c(PRGM_LCPC_CD = "PRGM_LCPC_CD", PRGM_INST_CD = "COCI_INST_CD",
                   PRGM_INST_PROGRAM_NAME = "PRGM_INST_PROGRAM_NAME"))

# ***** manual work needed *****
# review infoware notes
programs_table %>% filter(PRGM_ID %in% Remaining_DACSO_Updates_CPCS_2022$PRGM_ID) %>% pull(PRGM_ID,NOTES)
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
  mutate(CIP_CODE_4 = case_when(PRGM_ID == 115 ~ "1502",
                                PRGM_ID == 9006 ~ "1110",
                                PRGM_ID == 131 ~ "5001",
                                TRUE ~ CIP_CODE_4),
         LCP4_CIP_4DIGITS_NAME = case_when(PRGM_ID == 115 ~ "Civil engineering technology/technician", 
                                           PRGM_ID == 9006 ~ "Computer/information technology administration and management",
                                           PRGM_ID == 131 ~ "Visual, digital and performing arts, general", 
                                           TRUE ~ LCP4_CIP_4DIGITS_NAME),
         Updated_DACSO_CIP2022 = case_when(PRGM_ID == 115 ~ "Yes", 
                                           PRGM_ID == 9006 ~ "Yes",
                                           PRGM_ID == 131 ~ "Yes",
                                           TRUE ~ Updated_DACSO_CIP2022),
         PRGM_LCPC_CD = case_when(PRGM_ID == 115 ~ "CENG.DIP",
                                  PRGM_ID == 9006 ~ "CNET.CERT",
                                  PRGM_ID == 5952 ~ "ECENG.RE.DIP",
                                  PRGM_ID == 9008 ~ "ECENG.UVIC.ADIP",
                                  PRGM_ID == 4960 ~ "ICS.DIP",
                                  PRGM_ID == 117 ~ "MENG.DIP",
                                  PRGM_ID == 131 ~ "VART.DIP",
                                  TRUE ~ PRGM_LCPC_CD),
         PRGM_INST_PROGRAM_NAME = case_when(PRGM_ID == 115 ~ "Civil Engineering Technology (Diploma)",
                                            PRGM_ID == 9006 ~ "Computer Network Electronics Support Tech (Certificate)",
                                            PRGM_ID == 5952 ~ "Electronics & Computer Eng - Renewable Energy (Diploma)",
                                            PRGM_ID == 9008 ~ "Electrical & Computer Eng - Bridge to UVic (Adv Diploma)",
                                            PRGM_ID == 4960 ~ "Information & Computer Systems Technology (Diploma)",
                                            PRGM_ID == 117 ~ "Mechanical Engineering Technology (Diploma)",
                                            PRGM_ID == 131 ~ "Visual Arts (Diploma)",
                                            TRUE ~ PRGM_INST_PROGRAM_NAME),
         Updated_DACSO_CPC2022 = case_when(PRGM_ID == 115 ~ "Yes",
                                           PRGM_ID == 9006 ~ "Yes",
                                           PRGM_ID == 5952 ~ "Yes",
                                           PRGM_ID == 9008 ~ "Yes",
                                           PRGM_ID == 4960 ~ "Yes",
                                           PRGM_ID == 117 ~ "Yes",
                                           PRGM_ID == 131 ~ "Yes",
                                           TRUE ~ Updated_DACSO_CPC2022))

## ---- 2023 Find DACSO prgms WITH historical linkages ----
Updated_DACSO_Programs_in_2023_with_links <- programs_table %>%
  filter(PRGM_FIRST_SEEN_SUBM_CD =='C_Outc23' & HAS_HISTORICAL_PRGM_ID_LINK =='Y') %>%
  inner_join(tbl(con, "INFOWARE_PROGRAMS_HIST_PRGMID_XREF") %>%
               filter(YEAR_LINK_CREATED =='C_Outc23' & SURVEY_CODE=='DACSO') %>%
               collect(), 
             by = "PRGM_ID") %>%
  select(PRGM_ID, PRGM_FIRST_SEEN_SUBM_CD, PRGM_INST_CD, PRGM_LCPC_CD, PRGM_INST_PROGRAM_NAME, 
         PRGM_TTRAIN_FLAG, PRGM_CREDENTIAL, PRGM_INST_PROGRAM_NAME_CLEANED, NOTES, 
         HAS_HISTORICAL_PRGM_ID_LINK, DUP_PROGRAM_USE_THIS_PRGM_ID, CIP_CLUSTER_ARTS_APPLIED, 
         DACSO_OLD_PRGM_ID_DO_NOT_USE, LCIP_CD_CIP2016, LCIP_NAME_CIP2016, LCIP_LCP4_CD, 
         LCP4_CIP_4DIGITS_NAME, HISTORICAL_PRGM_ID, YEAR_LINK_CREATED, SURVEY_CODE)


## ---- 2023 Get historical linkages for DACSO prgms ----
## use the historical linkage added from INFOWARE_PROGRAMS_HIST_PRGMID_XREF 
## to link back to the programs table to fill in the historical program details
Updated_DACSO_Programs_in_2023_with_links <- Updated_DACSO_Programs_in_2023_with_links %>%
  inner_join(
    programs_table %>%
      select(PRGM_ID, HISTORICAL_CPC_CD = PRGM_LCPC_CD, 
             HISTORICAL_PROGRAM_NAME = PRGM_INST_PROGRAM_NAME, 
             HISTORICAL_CIP4_CD = LCIP_LCP4_CD),
    by = c(HISTORICAL_PRGM_ID = "PRGM_ID")
  ) %>%
  mutate(Updated_CPC_Flag = case_when(PRGM_LCPC_CD != HISTORICAL_CPC_CD ~ 'Yes', 
                                      TRUE ~ NA),
         Updated_CIP_Flag = case_when(LCIP_LCP4_CD != HISTORICAL_CIP4_CD ~ 'Yes', 
                                      TRUE ~ NA))

## ---- 2023 Update to XWALK: Updated DACSO programs WITH historical linkages ----
DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 <- DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 %>%
  left_join(
    Updated_DACSO_Programs_in_2023_with_links %>%
      mutate(HISTORICAL_CPC_CD = as.character(HISTORICAL_CPC_CD)) %>%
      select(PRGM_INST_CD, HISTORICAL_CPC_CD, HISTORICAL_PROGRAM_NAME, HISTORICAL_CIP4_CD,
             PRGM_LCPC_CD, PRGM_INST_PROGRAM_NAME, LCIP_LCP4_CD, LCP4_CIP_4DIGITS_NAME,
             Updated_DACSO_CPC2023 = Updated_CPC_Flag, Updated_DACSO_CIP2023 = Updated_CIP_Flag),
    by = c(COCI_INST_CD = "PRGM_INST_CD", PRGM_LCPC_CD = "HISTORICAL_CPC_CD", 
           PRGM_INST_PROGRAM_NAME = "HISTORICAL_PROGRAM_NAME", CIP_CODE_4 = "HISTORICAL_CIP4_CD")) %>%
  mutate(PRGM_LCPC_CD = ifelse(!is.na(PRGM_LCPC_CD.y), PRGM_LCPC_CD.y, PRGM_LCPC_CD),
         PRGM_INST_PROGRAM_NAME = ifelse(!is.na(PRGM_INST_PROGRAM_NAME.y), PRGM_INST_PROGRAM_NAME.y, PRGM_INST_PROGRAM_NAME),
         CIP_CODE_4 = ifelse(!is.na(LCIP_LCP4_CD), LCIP_LCP4_CD, CIP_CODE_4)) %>%
  mutate(LCP4_CIP_4DIGITS_NAME = ifelse(!is.na(LCP4_CIP_4DIGITS_NAME.y), LCP4_CIP_4DIGITS_NAME.y, LCP4_CIP_4DIGITS_NAME.x), .after = "CIP_CODE_4") %>%
  select(-ends_with(".x"), -ends_with(".y"), -LCIP_LCP4_CD)

## ---- 2023 Find Remaining updated DACSO missing from XWALK for match to STP program ----
Remaining_DACSO_Updates_CPCS_2023 <- Updated_DACSO_Programs_in_2023_with_links %>%
  anti_join(DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23,
            by = c(PRGM_LCPC_CD = "PRGM_LCPC_CD", PRGM_INST_CD = "COCI_INST_CD",
                   PRGM_INST_PROGRAM_NAME = "PRGM_INST_PROGRAM_NAME"))

# ***** manual work needed *****
# review infoware notes
programs_table %>% filter(PRGM_ID %in% Remaining_DACSO_Updates_CPCS_2023$PRGM_ID) %>% pull(PRGM_ID,NOTES)
# 3 PRGM_IDs remaining CPCs 
# 1 linked to multiple historic codes: 10413 -> 9841 -> 9237 -> 9017 -> 6036 (9017 & 6036 doesn't exist in XWALK, update 9237 - CPC only)
# 1 was updated by 2022 update (10234 updated the CPC): 10475 -> 1158 (update CPC only)
# 1 has historical match not in XWALK: 10493 -> 9810 (9810 does not exist in XWALK - add 10493 info as 9810 PRGM_ID)

# add missing PRGM_ID
DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 <- DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 %>%
  bind_rows(
    Remaining_DACSO_Updates_CPCS_2023 %>%
      filter(PRGM_ID=="10493") %>% 
      mutate(PSI_CODE = PRGM_INST_CD,
             COCI_INST_CD = PRGM_INST_CD,
             PSI_PROGRAM_CODE = PRGM_LCPC_CD,
             PSI_CREDENTIAL_PROGRAM_DESC = PRGM_INST_PROGRAM_NAME,
             PRGM_ID = 9810,
             New_DACSO_Program2021to23 = "Yes2023") %>%
      select(PSI_CODE, COCI_INST_CD, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESC, PRGM_LCPC_CD,
             PRGM_INST_PROGRAM_NAME, PRGM_CREDENTIAL,CIP_CODE_4 = LCIP_LCP4_CD, LCP4_CIP_4DIGITS_NAME,
             PRGM_ID, New_DACSO_Program2021to23)
  )

# update necessary values
DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 <- DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 %>%
  mutate(PRGM_LCPC_CD = case_when(PRGM_ID == 9237 ~ "BCPRPC",
                                  PRGM_ID == 1158 ~ "DIPLHKIN",
                                  TRUE ~ PRGM_LCPC_CD),
         PRGM_INST_PROGRAM_NAME = case_when(PRGM_ID == 9237 ~ "BC Police Recruit Training: Qualified Municipal Constable",
                                            PRGM_ID == 1158 ~ "Human Kinetics",
                                            TRUE ~ PRGM_INST_PROGRAM_NAME),
         Updated_DACSO_CPC2023 = case_when(PRGM_ID == 9237 ~ "Yes",
                                           PRGM_ID == 1158 ~ "Yes",
                                           TRUE ~ Updated_DACSO_CPC2023))

## ---- Update to XWALK: DACSO programs with updated CIPS ----
## find the updated CIPs
updated_cips <- DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 %>%
  left_join(programs_table %>%
              select(PRGM_ID, CIP_CODE_4 = LCIP_LCP4_CD, LCP4_CIP_4DIGITS_NAME, NOTES),
            by = "PRGM_ID") %>%
  filter(CIP_CODE_4.x != CIP_CODE_4.y | LCP4_CIP_4DIGITS_NAME.x != LCP4_CIP_4DIGITS_NAME.y)

# filter out the known updates
updated_cips <- updated_cips %>% 
  filter(is.na(Updated_DACSO_CIP2021) & is.na(Updated_DACSO_CIP2022) & is.na(Updated_DACSO_CIP2023))

# review NOTES column where word "CIP" exists
updated_cips %>% 
  filter(grepl("CIP",NOTES)) %>%
  select(CIP_CODE_4.x,CIP_CODE_4.y,NOTES,PRGM_ID) %>% print(n=100)

# review NOTES column where word "CIP" doesn't exist
updated_cips %>% 
  filter(!grepl("CIP",NOTES)) %>% 
  select(CIP_CODE_4.x,CIP_CODE_4.y,NOTES,PRGM_ID) %>% print(n=100)

updated_cips %>% 
  filter(!grepl("CIP",NOTES)) %>% 
  select(LCP4_CIP_4DIGITS_NAME.x,LCP4_CIP_4DIGITS_NAME.y,NOTES,PRGM_ID) %>% print(n=100)

# Decision: PRGM_ID 9018 shouldn't be changed; the rest seem like acceptable changes
# filter out from updated cips file
updated_cips <- updated_cips %>% 
  filter(PRGM_ID!=9018)

# update CIPS in XWALK
DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 <- DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 %>%
  left_join(updated_cips %>%
              distinct(PRGM_ID, CIP_CODE_4 = CIP_CODE_4.y, LCP4_CIP_4DIGITS_NAME = LCP4_CIP_4DIGITS_NAME.y),
            by = "PRGM_ID") %>%
  mutate(CIP_CODE_4 = ifelse(!is.na(CIP_CODE_4.y), CIP_CODE_4.y, CIP_CODE_4.x),
         LCP4_CIP_4DIGITS_NAME = ifelse(!is.na(LCP4_CIP_4DIGITS_NAME.y), LCP4_CIP_4DIGITS_NAME.y, LCP4_CIP_4DIGITS_NAME.x),
         # adding all of these to only the most recent year updated cip column
         Updated_DACSO_CIP2023 = ifelse(!is.na(CIP_CODE_4.y), "Yes", Updated_DACSO_CIP2023)) %>%
  select(-ends_with(".x"), -ends_with(".y"))

## ---- Write XWALK to Decimal ----
dbWriteTable(con, "DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23", DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23)

## Add two variables needed for STP program matching
dbGetQuery(con, "ALTER TABLE DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 
                  ADD  
                  New_STP_Program2021_23 VARCHAR(255), 
                  Updated_DACSO_CDTL2021_23 VARCHAR(255)")

# Part 2 ----
# import sql queries
## ** IMPORTANT - update queries with table years **
source("./sql/02a-program-matching/02a-dacso-program-matching.R")

# check for required table
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."credential_non_dup"')))

## ---- Make STP_Credential_Non_Dup_Programs_DACSO ----
# Create a DACSO version of Credential_non_dup table with subset of columns
dbGetQuery(con, qry_DASCO_STP_Credential_Programs)

## ---- Restructure and populate imported STP data ----
dbGetQuery(con, qry_DASCO_STP_Credential_Programs_Add_Columns)

# Create PSI_CODE to COCI_INST_CD lookup table
dbGetQuery(con, "SELECT DISTINCT PSI_CODE, COCI_INST_CD
           INTO tbl_STP_PSI_CODE_INST_CD_Lookup
           FROM DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23
           WHERE PSI_CODE IS NOT NULL and COCI_INST_CD IS NOT NULL")

# Add COCI_INST_CD to STP table
dbGetQuery(con, "UPDATE STP_Credential_Non_Dup_Programs_DACSO 
SET STP_Credential_Non_Dup_Programs_DACSO.COCI_INST_CD = tbl_STP_PSI_CODE_INST_CD_Lookup.COCI_INST_CD
FROM STP_Credential_Non_Dup_Programs_DACSO INNER JOIN tbl_STP_PSI_CODE_INST_CD_Lookup 
ON STP_Credential_Non_Dup_Programs_DACSO.PSI_CODE = tbl_STP_PSI_CODE_INST_CD_Lookup.PSI_CODE")

## ---- Populate STP_CIP_CODE_4, STP_CIP_CODE_4_NAME ----
dbGetQuery(con, qry_Update_STP_CIP_CODE4)

## ---- Already matched programs (might have new CIPs) ----
# reference: to check after each query to see counts of matched
# tbl(con,"STP_Credential_Non_Dup_Programs_DACSO") %>% collect() %>% count(Already_Matched)

# Queries set Already_Matched column to Yes if match
# join STP data to XWALK on PSI_CODE, PSI_PROGRAM_CREDENTIAL_DESCRIPTION, PSI_PROGRAM_CODE
dbGetQuery(con, qry_STP_Credential_DACSO_Programs_AlreadyMatched)

# join STP data to XWALK on COCI_INST_CD, PSI_CREDENTIAL_PROGRAM_DESCRIPTION, PSI_PROGRAM_CODE  
dbGetQuery(con, qry_STP_Credential_DACSO_Programs_AlreadyMatched_b)

## ---- Newly matched programs ----
# reference: to check after each query to see counts of matched
# tbl(con,"STP_Credential_Non_Dup_Programs_DACSO") %>% collect() %>% count(New_Auto_Match)

# Queries set New_Auto_Match column to Yes if match
# join STP data to XWALK on PSI_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION = PRGM_INST_PROGRAM_NAME, PSI_PROGRAM_CODE = PRGM_LCPC_CD
dbGetQuery(con, qry_STP_Credential_DACSO_Programs_NewMatches_a)

# join STP data to XWALK on COCI_INST_CD, PSI_CREDENTIAL_PROGRAM_DESCRIPTION = PRGM_INST_PROGRAM_NAME, PSI_PROGRAM_CODE = PRGM_LCPC_CD
dbGetQuery(con, qry_STP_Credential_DACSO_Programs_NewMatches_a_step2)

## ---- Add to XWALK: newly matched STP programs ----
# join on PSI_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION = PRGM_INST_PROGRAM_NAME, PSI_PROGRAM_CODE = PRGM_LCPC_CD
# where New_Auto_Match = Yes, copy PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION, STP_CIP4_CODE into STP_CIP_CODE_4, CTP_SIP4_NAME into STP_CIP_CODE_4_NAME
# - Set New_STP_Program20XX = Yes and One_To_One_Match = Yes20XX
dbGetQuery(con, qry_STP_Credential_DACSO_Programs_NewMatches_b)

# same as above but join on COCI_INST_CD, PSI_CREDENTIAL_PROGRAM_DESCRIPTION = PRGM_INST_PROGRAM_NAME, PSI_PROGRAM_CODE = PRGM_LCPC_CD
dbGetQuery(con, qry_STP_Credential_DACSO_Programs_NewMatches_b_step2)

# Part 3 ----

## Find STP programs that are unmatched ----
stp_unmatched <- tbl(con, "STP_Credential_Non_Dup_Programs_DACSO") %>%
  filter((is.na(OUTCOMES_CIP_CODE_4) | is.na(OUTCOMES_CIP_CODE_4_NAME)) & is.na(Already_Matched) & is.na(New_Auto_Match)) %>%
  collect()

# !!! review the unmatched STP programs and the XWALK to determine if these institution matches are still relevant

# reference: to check after each query to see counts of matched
# tbl(con,"STP_Credential_Non_Dup_Programs_DACSO") %>% collect() %>% count(New_Auto_Match)

## ---- Update BCIT ----
## BCIT submits CPC codes to STP that include the credential abbreviation suffix (e.g. _TTDIPL, _CERTTS...) 
## but in DACSO their codes do not have the suffix
tbl(con, "DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23") %>% filter(PSI_CODE=="BCIT") %>% pull(PRGM_LCPC_CD)
stp_unmatched %>% filter(PSI_CODE=="BCIT") %>% pull(PSI_PROGRAM_CODE)

# add a new program code column, and use that to match
dbGetQuery(con, "ALTER TABLE STP_Credential_Non_Dup_Programs_DACSO 
               ADD  
                  BCIT_TEST_PROGRAM_CODE VARCHAR(255)")

# take the first 4 digits of the PSI_PROGRAM_CODE
dbGetQuery(con,"UPDATE STP_Credential_Non_Dup_Programs_DACSO 
SET STP_Credential_Non_Dup_Programs_DACSO.BCIT_TEST_PROGRAM_CODE = Left([PSI_PROGRAM_CODE],4)
WHERE (([PSI_CODE]='BCIT'))")

# queries will set New_Auto_Match = 'YesXXBCIT'
# join STP data to XWALK on COCI_INST_CD, PSI_CREDENTIAL_PROGRAM_DESCRIPTION = PRGM_INST_PROGRAM_NAME, BCIT_TEST_PROGRAM_CODE = PRGM_LCPC_CD
dbGetQuery(con, qry_Update_BCIT_Programs)

# join STP data to XWALK on COCI_INST_CD, BCIT_TEST_PROGRAM_CODE = PRGM_LCPC_CD
dbGetQuery(con, qry_Update_BCIT_Programs_b)

## ---- Update CAPU ----
# CAPU submits CPC codes to STP that are 6 digits long, but in DACSO they are generally 3 or 4 digits long
# run through twice - once for 4 digits, once for 3 digits
# some also seem to have -YYY after CPC codes in STP but not in DACSO
tbl(con, "DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23") %>% filter(PSI_CODE=="CAP") %>% pull(PRGM_LCPC_CD)
stp_unmatched %>% filter(PSI_CODE=="CAPU") %>% pull(PSI_PROGRAM_CODE)

dbGetQuery(con, "ALTER TABLE STP_Credential_Non_Dup_Programs_DACSO 
               ADD  
                  CAP_TEST_PROGRAM_CODE VARCHAR(255)")
# remove dashes
dbGetQuery(con,"UPDATE STP_Credential_Non_Dup_Programs_DACSO 
SET STP_Credential_Non_Dup_Programs_DACSO.CAP_TEST_PROGRAM_CODE = LEFT([PSI_PROGRAM_CODE],CHARINDEX('-',[PSI_PROGRAM_CODE])-1)
WHERE (([COCI_INST_CD]='CAPU') AND ([PSI_PROGRAM_CODE] like '%-%'))")

dbGetQuery(con, qry_Update_CAPU_Programs_a)
dbGetQuery(con, qry_Update_CAPU_Programs_b) 

# 4 digits
dbGetQuery(con,"UPDATE STP_Credential_Non_Dup_Programs_DACSO 
SET STP_Credential_Non_Dup_Programs_DACSO.CAP_TEST_PROGRAM_CODE = Left([PSI_PROGRAM_CODE],4)
WHERE (([COCI_INST_CD]='CAPU'))")

dbGetQuery(con, qry_Update_CAPU_Programs_a)
dbGetQuery(con, qry_Update_CAPU_Programs_b)

# 3 digits
dbGetQuery(con,"UPDATE STP_Credential_Non_Dup_Programs_DACSO 
SET STP_Credential_Non_Dup_Programs_DACSO.CAP_TEST_PROGRAM_CODE = Left([PSI_PROGRAM_CODE],3)
WHERE (([COCI_INST_CD]='CAPU'))")

dbGetQuery(con, qry_Update_CAPU_Programs_a)
dbGetQuery(con, qry_Update_CAPU_Programs_b)


## ---- Update VIU ----
# STP versions seem longer (e.g., CERT-WELDM_01) versus DACSO (e.g.,WELDM)
# STP has the credential category (e.g., CERT) dash DACSO CPC followed by _01 or similar
tbl(con, "DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23") %>% filter(PSI_CODE=="VIU") %>% pull(PRGM_LCPC_CD)
stp_unmatched %>% filter(PSI_CODE=="VIU") %>% pull(PSI_PROGRAM_CODE)

dbGetQuery(con, "ALTER TABLE STP_Credential_Non_Dup_Programs_DACSO 
               ADD  
                  VIU_TEST_PROGRAM_CODE VARCHAR(255)")

# populate with the value between the - and _
dbGetQuery(con,"UPDATE STP_Credential_Non_Dup_Programs_DACSO 
SET STP_Credential_Non_Dup_Programs_DACSO.VIU_TEST_PROGRAM_CODE = SUBSTRING([PSI_PROGRAM_CODE],  charindex('-',[PSI_PROGRAM_CODE],1) + 1, charindex('_',[PSI_PROGRAM_CODE],1) - charindex('-',[PSI_PROGRAM_CODE],1) - 1 )
WHERE (([PSI_CODE]='VIU') AND ([PSI_PROGRAM_CODE] like '%-%'))")

dbGetQuery(con, qry_Update_VIU_Programs_a)
dbGetQuery(con, qry_Update_VIU_Programs_b)

## Update remaining matching ----
dbGetQuery(con, qry_Update_Remaining_Programs_Matching_DACSO_Seen)
dbGetQuery(con, qry_Update_Remaining_Programs_Matching_DACSO_Seen_b)

# Part 4 ----

## ---- Update STP_Credential_Non_Dup_Programs_DACSO with final CIPS ----
# Use the outcomes cip4 data if there was a match for the final cip4
dbGetQuery(con, qry_Update_STP_Cred_Non_Dup_Programs_DACSO_FinalCIP4_a)

# Use the STP CIP4 outcomes for the rest where there is no match
dbGetQuery(con, qry_Update_STP_Cred_Non_Dup_Programs_DACSO_FinalCIP4_b)
dbGetQuery(con, qry_Update_STP_Cred_Non_Dup_Programs_DACSO_FinalCIP4_c) 

# To update the final cip 2 and  final cip cluster based on the final cip4
dbGetQuery(con, qry_Update_STP_Cred_Non_Dup_Programs_DACSO_FinalCIP2_Cluster_a)

# To update the final cip 2 name based on the final cip2
dbGetQuery(con, qry_Update_STP_Cred_Non_Dup_Programs_DACSO_FinalCIP2_Cluster_b) 

# check the # of changed CIPS
dbGetQuery(con, qry_Check_CIP_Changes_STP_Cred_Non_Dup_DACSO)
nrow(tbl(con,"STP_Credential_Non_Dup_Programs_DACSO_CIPS_CHANGED_2021_23") %>% collect())
review_changed_cips <- tbl(con,"STP_Credential_Non_Dup_Programs_DACSO_CIPS_CHANGED_2021_23") %>% collect()

## Save to new table in DECIMAL: Credential_Non_Dup_Programs_DACSO_FinalCIPS ----
dbExecute(con,"SELECT * 
               INTO Credential_Non_Dup_Programs_DACSO_FinalCIPS
               FROM STP_Credential_Non_Dup_Programs_DACSO;")

# Clean up ----
dbExecute(con, "DROP TABLE tbl_STP_PSI_CODE_INST_CD_Lookup")
dbExecute(con, "DROP TABLE STP_Credential_Non_Dup_Programs_DACSO_CIPS_CHANGED_2021_23")
dbDisconnect(con)


