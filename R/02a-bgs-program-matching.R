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
# Aligns CIP codes between BGS survey and STP data
# Required Tables 
#   INFOWARE_BGS_DIST_19_23
#   INFOWARE_BGS_DIST_18_22
#   INFOWARE_BGS_COHORT_INFO
#   INFOWARE_L_CIP_6DIGITS_CIP2016
#   INFOWARE_L_CIP_4DIGITS_CIP2016
#   INFOWARE_L_CIP_2DIGITS_CIP2016   
#   Credential_non_Dup 
#   STP_Credential  - for PSI_PEN
# Resulting Tables
#   T_BGS_DATA_FINAL_for_OutcomesMatching
#   Credential_Non_Dup_BGS_IDs
#   Credential_Non_Dup_GRAD_IDs 
# Intermediate Tables - not including local transient tables
#   Credential_Non_Dup_STP_CIP4_Cleaning
#   BGS_Matching_STP_Credential_PEN
#   BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly
# ******************************************************************************

options(java.parameters = " -Xmx102400m") ## For reading oracle tables: increase amount of memory java is allowed to use

library(tidyverse)
library(RODBC)
library(odbc)
library(DBI)
library(glue)
library(RJDBC)

# import sql queries
source("./sql/02a-program-matching/02a-bgs-program-matching.R")

# ---- Read in INFOWARE tables ----

iw_config <- config::get("infoware")
jdbc_config <- config::get("jdbc")

jdbcDriver <- JDBC(jdbc_config$class,
                   classPath=jdbc_config$path)

iw_con <- dbConnect(jdbcDriver, 
                    iw_config$database,
                    iw_config$uid,
                    iw_config$pwd)

## ** NOTE **
## Ideally match on all data but prioritizing the most recent 6 years - see documentation
## Update which BGS_DIST tables to include. 

## Run the following to get a list of all tables available
# alltables_Infoware <- dbReadTable(iw_con,"ALL_TABLES")

INFOWARE_BGS_DIST_19_23  <- dbReadTable(iw_con, "INFOWARE.BGS_DIST_19_23")
INFOWARE_BGS_DIST_18_22  <- dbReadTable(iw_con, "INFOWARE.BGS_DIST_18_22")
# INFOWARE_BGS_DIST_17_21  <- dbReadTable(iw_con, "INFOWARE.BGS_DIST_17_21")
# INFOWARE_BGS_DIST_16_20  <- dbReadTable(iw_con, "INFOWARE.BGS_DIST_16_20")
# INFOWARE_BGS_DIST_15_19  <- dbReadTable(iw_con, "INFOWARE.BGS_DIST_15_19")
# INFOWARE_BGS_DIST_14_18  <- dbReadTable(iw_con, "INFOWARE.BGS_DIST_14_18")
# INFOWARE_BGS_DIST_13_17  <- dbReadTable(iw_con, "INFOWARE.BGS_DIST_13_17")
# INFOWARE_BGS_DIST_12_16  <- dbReadTable(iw_con, "INFOWARE.BGS_DIST_12_16")
INFOWARE_BGS_COHORT_INFO <- dbReadTable(iw_con, "INFOWARE.BGS_COHORT_INFO")
INFOWARE_L_CIP_6DIGITS_CIP2016 <- dbReadTable(iw_con, "INFOWARE.L_CIP_6DIGITS_CIP2016")
INFOWARE_L_CIP_4DIGITS_CIP2016 <- dbReadTable(iw_con, "INFOWARE.L_CIP_4DIGITS_CIP2016")
INFOWARE_L_CIP_2DIGITS_CIP2016 <- dbReadTable(iw_con, "INFOWARE.L_CIP_2DIGITS_CIP2016")

dbDisconnect(iw_con)

# ---- Connect to Decimal ----
config <- config::get("decimal")
con <- dbConnect(odbc(),
                 Driver = config$driver,
                 Server = config$server,
                 Database = config$database,
                 Trusted_Connection = "True")

# ---- Write initial tables to Decimal ----
## Save static versions of the INFOWARE tables and last cycle XWALK to Decimal
# !! UPDATE THE TABLES AND ROW NUMBERS !! - connection won't write the full datasets to decimal due to size
dbWriteTable(con, "INFOWARE_BGS_DIST_19_23", INFOWARE_BGS_DIST_19_23[1:80000,])
dbWriteTable(con, "INFOWARE_BGS_DIST_19_23", INFOWARE_BGS_DIST_19_23[80001:121074,], append = TRUE)
dbWriteTable(con, "INFOWARE_BGS_DIST_18_22", INFOWARE_BGS_DIST_18_22[1:80000,])
dbWriteTable(con, "INFOWARE_BGS_DIST_18_22", INFOWARE_BGS_DIST_18_22[80001:118632,], append = TRUE)
dbWriteTable(con, "INFOWARE_BGS_COHORT_INFO", INFOWARE_BGS_COHORT_INFO[1:80000,])
dbWriteTable(con, "INFOWARE_BGS_COHORT_INFO", INFOWARE_BGS_COHORT_INFO[80001:160000,], append = TRUE)
dbWriteTable(con, "INFOWARE_BGS_COHORT_INFO", INFOWARE_BGS_COHORT_INFO[160001:240000,], append = TRUE)
dbWriteTable(con, "INFOWARE_BGS_COHORT_INFO", INFOWARE_BGS_COHORT_INFO[240001:290758,], append = TRUE)
dbWriteTable(con, "INFOWARE_L_CIP_6DIGITS_CIP2016", INFOWARE_L_CIP_6DIGITS_CIP2016)
dbWriteTable(con, "INFOWARE_L_CIP_4DIGITS_CIP2016", INFOWARE_L_CIP_4DIGITS_CIP2016)
dbWriteTable(con, "INFOWARE_L_CIP_2DIGITS_CIP2016", INFOWARE_L_CIP_2DIGITS_CIP2016)

## check tables loaded correctly
{
  nrow <- tbl(con, "INFOWARE_BGS_DIST_19_23") %>% tally()
  nrow ## how many rows?
  tbl(con, "INFOWARE_BGS_DIST_19_23") %>% distinct(STQU_ID) %>% tally() ## are all IDs unique?
  
  nrow <- tbl(con, "INFOWARE_BGS_DIST_18_22") %>% tally()
  nrow ## how many rows?
  tbl(con, "INFOWARE_BGS_DIST_18_22") %>% distinct(STQU_ID) %>% tally() ## are all IDs unique?
  
  nrow <- tbl(con, "INFOWARE_BGS_COHORT_INFO") %>% tally()
  nrow ## how many rows?
  tbl(con, "INFOWARE_BGS_COHORT_INFO") %>% distinct(STQU_ID) %>% tally() ## are all IDs unique?
  
  rm(nrow)
}

## remove tables and use decimal versions for remainder of code
rm(INFOWARE_BGS_DIST_19_23, INFOWARE_BGS_DIST_18_22, INFOWARE_BGS_COHORT_INFO, INFOWARE_L_CIP_6DIGITS_CIP2016,
   INFOWARE_L_CIP_4DIGITS_CIP2016, INFOWARE_L_CIP_2DIGITS_CIP2016 )

## Part 1: Build Outcomes Data ----
## Created tables: T_BGS_Data_Final_for_OutcomesMatching

# BGS data: Build T_DATA_FINAL_for_OutcomesMatching table with past 6 years
## ** IMPORTANT - update queries with table years **
dbGetQuery(con, qry_Make_T_BGS_Data_for_OutcomesMatching_step1)
dbGetQuery(con, qry_Make_T_BGS_Data_for_OutcomesMatching_step2)
dbGetQuery(con, qry_Add_PSSM_CREDENTIAL)

## check counts by year
{
  tbl(con, "T_BGS_Data_Final_for_OutcomesMatching") %>% group_by(Year) %>% tally()
}

## check for any NAs in the 2,4,6 CIPS- try to fix NAs if they exist
{
  chk_bgs_cip <- dbGetQuery(con, qry_Check_BGS_CIP_Data)
  chk_bgs_cip[is.na(chk_bgs_cip)]
  rm(chk_bgs_cip)
}

## Part 2: Create Credential (STP) 4D and 2D CIP Codes ----
## Created tables: Credential_Non_Dup_STP_CIP4_Cleaning
##                 Credential_Non_Dup_BGS_IDs
##                 Credential_Non_Dup_GRAD_IDs

# Create Credential_Non_Dup_STP_CIP4_Cleaning to clean PSI_CREDENTIAL_CIP
## New: include GRAD records
dbGetQuery(con, qry_BGS_STP_CIP_Cleaning) 

## check correct version of Credential_Non_Dup used
{
  t1 <- tbl(con, dbplyr::in_schema("dbo", "credential_non_dup")) %>% 
    select(PSI_CREDENTIAL_CIP = psi_credential_cip, OUTCOMES_CRED = outcomes_cred) %>%
    filter(OUTCOMES_CRED %in% c("BGS", "GRAD")) %>% 
    group_by(PSI_CREDENTIAL_CIP, OUTCOMES_CRED) %>% 
    summarize(Expr1 = n(), .groups = "drop") %>% 
    collect() %>%
    arrange(PSI_CREDENTIAL_CIP, OUTCOMES_CRED)
  
  t2 <- tbl(con, "Credential_Non_Dup_STP_CIP4_Cleaning") %>% collect() %>% arrange(PSI_CREDENTIAL_CIP, OUTCOMES_CRED)
  
  all.equal(t1, t2)
  
  rm(t1, t2)
}

## Add STP_CIP columns
dbGetQuery(con, "ALTER TABLE Credential_Non_Dup_STP_CIP4_Cleaning
                 ADD STP_CIP_CODE_4 varchar (255),
                     STP_CIP_CODE_4_NAME varchar (255),
                     STP_CIP_CODE_2 varchar (255),
                     STP_CIP_CODE_2_NAME varchar (255)")

## add a second PSI_CREDENTIAL_CIP column to retain original PSI_CREDENTIAL_CIP
dbGetQuery(con, "ALTER TABLE Credential_Non_Dup_STP_CIP4_Cleaning
                 ADD PSI_CREDENTIAL_CIP_orig varchar (255)")

dbGetQuery(con, "UPDATE Credential_Non_Dup_STP_CIP4_Cleaning
                 SET PSI_CREDENTIAL_CIP_orig = PSI_CREDENTIAL_CIP")

## check PSI_CREDENTIAL_CIP to make sure they are formatted correctly
## i.e., in the format XX.XXXX
{
  chk <- tbl(con, "Credential_Non_Dup_STP_CIP4_Cleaning") %>%
    distinct(PSI_CREDENTIAL_CIP_orig) %>%
    collect()
  
  chk %>% filter(nchar(PSI_CREDENTIAL_CIP_orig) != 7)
  
  rm(chk)
}

# clean PSI_CREDENTIAL_CIP that don't have 7 characters (i.e., not in the format XX.XXXX)
{
  ## update PSI_CREDENTIAL_CIP to add trailing zero when formatted as XX.XXX
  dbGetQuery(con, "UPDATE Credential_Non_Dup_STP_CIP4_Cleaning
                 SET    PSI_CREDENTIAL_CIP = CONCAT(PSI_CREDENTIAL_CIP, '0')
                 WHERE  LEN(PSI_CREDENTIAL_CIP) = 6 AND
                        substring(PSI_CREDENTIAL_CIP,1,2) NOT LIKE '%.'") ## first 2 character do not contain .
  
  ## update PSI_CREDENTIAL_CIP to add leading zero when formatted as X.XXXX
  dbGetQuery(con, "UPDATE Credential_Non_Dup_STP_CIP4_Cleaning
                 SET    PSI_CREDENTIAL_CIP = CONCAT('0', PSI_CREDENTIAL_CIP)
                 WHERE LEN(PSI_CREDENTIAL_CIP) = 6")
}

## check PSI_CREDENTIAL_CIP to make sure they are formatted correctly
## i.e., in the format XX.XXXX
{
  chk <- tbl(con, "Credential_Non_Dup_STP_CIP4_Cleaning") %>%
    distinct(PSI_CREDENTIAL_CIP, PSI_CREDENTIAL_CIP_orig) %>%
    filter(nchar(PSI_CREDENTIAL_CIP_orig) != 7) %>%
    collect()
  
  rm(chk)
}

## update code below based on check above, to custom correct PSI_CREDENTIAL_CIP_CODES
## repeat until all formatting issues resolved
## this code was not needed for 2023 data
# {
#   dbGetQuery(con, "UPDATE Credential_Non_Dup_STP_CIP4_Cleaning
#                  SET    PSI_CREDENTIAL_CIP = CONCAT('0', PSI_CREDENTIAL_CIP, '00')
#                  WHERE PSI_CREDENTIAL_CIP = '9.09'")
#   
#   dbGetQuery(con, "UPDATE Credential_Non_Dup_STP_CIP4_Cleaning
#                  SET    PSI_CREDENTIAL_CIP = CONCAT(PSI_CREDENTIAL_CIP, '.0000')
#                  WHERE PSI_CREDENTIAL_CIP = '51'")
# }

## Update CIP 4 and 2D codes from INFOWARE, matching PSI_CREDENTIAL_CIP to LCIP_CD_WITH_PERIOD
## match all 6 digits
dbGetQuery(con, qry_Clean_BGS_STP_CIP_Step1_a) 

## check for any non-matches
{
  chk1 <- tbl(con, "Credential_Non_Dup_STP_CIP4_Cleaning") %>% 
    filter(is.na(STP_CIP_CODE_4)) %>%
    collect()
}

## match first 4 digits
dbGetQuery(con, qry_Clean_BGS_STP_CIP_Step1_b)

## check update and any non-matches
{
  chk2 <- tbl(con, "Credential_Non_Dup_STP_CIP4_Cleaning") %>% 
    filter(PSI_CREDENTIAL_CIP %in% chk1$PSI_CREDENTIAL_CIP) %>%
    collect()
  
  chk2 <- chk2 %>% filter(is.na(STP_CIP_CODE_4))
}

## recode general program CIPs from 00 ending to 01 ending
## Check which CIPs have general programs here: https://www.statcan.gc.ca/en/subjects/standard/cip/2021/index
dbGetQuery(con, qry_Clean_BGS_STP_CIP_Step1_c)

## check update and any non-matches
{
  chk3 <- tbl(con, "Credential_Non_Dup_STP_CIP4_Cleaning") %>% 
    filter(PSI_CREDENTIAL_CIP %in% chk2$PSI_CREDENTIAL_CIP) %>%
    collect()
  
  chk3 <- chk3 %>% filter(is.na(STP_CIP_CODE_4))
}

## match first 2 digits
dbGetQuery(con, qry_Clean_BGS_STP_CIP_Step1_d)

# Check for blank CIP4s and CIP2s
{
chk <- tbl(con, "Credential_Non_Dup_STP_CIP4_Cleaning") %>%
  filter(is.na(STP_CIP_CODE_4) | is.na(STP_CIP_CODE_2)) %>%
  collect() ## populate CIP4 and CIP2 that are blank (add to custom code above)

rm(chk, chk1, chk2, chk3) 
}

## Add the CIP 4 and 2D names
dbGetQuery(con, qry_Clean_BGS_STP_CIP_Step2) # add CIP 4D names
dbGetQuery(con, qry_Clean_BGS_STP_CIP_Step3) # add CIP 2D names
dbGetQuery(con, qry_Clean_BGS_STP_CIP_step4) # mark “Invalid 4-digit CIP” for remaining blank 4D names

## review table
{
chk <- tbl(con, "Credential_Non_Dup_STP_CIP4_Cleaning") %>% collect()
chk1 <- chk %>% filter(STP_CIP_CODE_4 != str_sub(PSI_CREDENTIAL_CIP_orig, end = 5) %>% str_remove_all("\\."))

rm(chk, chk1)
}

## Split updated STP table into BGS and Grad credentials
dbGetQuery(con, qry_Update_Credential_with_STP_CIP_BGS)
dbGetQuery(con, qry_Update_Credential_with_STP_CIP_GRAD) ## set CIPs as final for GRAD - no further cleaning

## check no blanks for STP_CIP_CODE_4
{
  ## total rows in BGS data  
  tbl(con, "Credential_Non_Dup_BGS_IDs") %>% tally()
  
  ## row with empty CIP4
  chk <- tbl(con, "Credential_Non_Dup_BGS_IDs") %>%
    filter(is.na(STP_CIP_CODE_4)) %>%
    distinct(PSI_CREDENTIAL_CIP, STP_CIP_CODE_4, STP_CIP_CODE_4_NAME) %>%
    collect()
  
  ## total rows in GRAD data  
  tbl(con, "Credential_Non_Dup_GRAD_IDs") %>% tally()

  chk <- tbl(con, "Credential_Non_Dup_GRAD_IDs") %>%
    filter(is.na(FINAL_CIP_CODE_4)) %>%
    distinct(PSI_CREDENTIAL_CIP, FINAL_CIP_CODE_4, FINAL_CIP_CODE_4_NAME) %>%
    collect()

  rm(chk)
}

## check Credential_Non_Dup_BGS_IDs for (Unspecified) - when Credential_Non_Dup loaded NULLs changed to (Unspecified) 
{
  chk <- tbl(con, "Credential_Non_Dup_BGS_IDs") %>% 
    filter(PSI_CODE ==  "(Unspecified)" | PSI_PROGRAM_CODE == "(Unspecified)" |
    PSI_CREDENTIAL_PROGRAM_DESCRIPTION == "(Unspecified)" | PSI_CREDENTIAL_CIP == "(Unspecified)" |
    PSI_AWARD_SCHOOL_YEAR == "(Unspecified)" | OUTCOMES_CRED == "(Unspecified)") %>%
    collect() 
  
  ## which columns have "(Unspecified)"
  map(chk, ~sum(str_detect(.x, "(Unspecified)"))) 
  
  ## 2023: only PSI_PROGRAM_CODE had (Unspecified) - replace with NULLs
  dbGetQuery(con, "
             Update Credential_Non_Dup_BGS_IDs
             SET PSI_PROGRAM_CODE = NULL
             WHERE PSI_PROGRAM_CODE = '(Unspecified)'")
  
  tbl(con, "Credential_Non_Dup_BGS_IDs") %>% 
    filter(PSI_PROGRAM_CODE == "(Unspecified)") %>%
    tally() 
  
  tbl(con, "Credential_Non_Dup_BGS_IDs") %>% 
    filter(is.na(PSI_PROGRAM_CODE)) %>%
    tally() 
  
  rm(chk)
}

## Part 3: Build Case-level XWALK ----
## Created tables: BGS_Matching_STP_Credential_PEN
##                 BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly

### Part 3A: Initial XWALK ----

# Add PEN - if missing PSI_PEN from Crednential_Non_Dup_BGS_IDs, add from STP_Credential
if(! "PSI_PEN" %in% colnames(tbl(con, "Credential_Non_Dup_BGS_IDs"))) {
  dbGetQuery(con, qry_Add_PSI_PEN)
  dbGetQuery(con, qry_Update_PSI_PEN)
}

## Create BGS_Matching_STP_Credential_PEN by joining STP (Credential_Non_Dup) to BGS data (T_BGS_Data_Final_for_OutcomesMatching)
dbGetQuery(con, qry01_Match_BGS_STP_Credential_on_PEN) 

## check number of rows
{
  tbl(con, "T_BGS_Data_Final_for_OutcomesMatching") %>% 
    select(STQU_ID, PEN) %>% filter(!is.na(PEN) & PEN != "" & PEN != "0") %>% 
    inner_join(tbl(con, "Credential_Non_Dup_BGS_IDs") %>% select(ID, PSI_PEN),
               by = c("PEN" = "PSI_PEN")) %>% tally()
  
  tbl(con, "BGS_Matching_STP_Credential_PEN") %>% tally()
}

## Add flag columns to track matching
dbGetQuery(con, qry01b_Match_BGS_STP_Credential_Add_Cols) 

## Add Final CIP columns to be populated
dbGetQuery(con, qry_add_empty_final_CIP)

### Part 3B: Auto matching using flags ----

### Create flags for matches on institution, award year, CIP4 and CIP2

dbGetQuery(con, qry02_Match_BGS_STP_Credential_Match_Inst) ## add Match_Inst flag

## Check that no new institution codes need to be added to query:
{
# find all institution codes that have a match, filter for any new codes
table <- tbl(con, "BGS_Matching_STP_Credential_PEN") %>%
    select(PSI_CODE, INSTITUTION_CODE, Match_Inst) 

codes <- table %>%
  filter(!is.na(Match_Inst)) %>%
  distinct(PSI_CODE, INSTITUTION_CODE) %>%
  collect()

table %>%
  filter(is.na(Match_Inst) & !PSI_CODE %in% codes$PSI_CODE) %>%
  count(PSI_CODE, INSTITUTION_CODE) %>%
  arrange(PSI_CODE) %>%
  collect()

rm(table, codes)
}

dbGetQuery(con, qry03_Match_BGS_STP_Credential_Match_AwardYear) ## add Match_Award_School_Year flag ** update years in query**
dbGetQuery(con, qry04_Match_BGS_STP_Credential_Match_CIPCODE4) ## add Match_CIP_CODE_4 flag
dbGetQuery(con, qry05_Match_BGS_STP_Credential_Match_CIPCODE2) ## add Match_CIP_CODE_2 flag
dbGetQuery(con, qry06_Match_BGS_STP_Credential_MatchAll3_CIP4Flag) ## add Match_All_3_CIP4_Flag
dbGetQuery(con, qry07_Match_BGS_STP_Credential_MatchAll3_CIP2Flag) ## add Match_All_3_CIP2_Flag

## some checks
{
  ## flag counts
  tbl(con, "BGS_Matching_STP_Credential_PEN") %>% group_by(Match_Inst) %>% tally()
  tbl(con, "BGS_Matching_STP_Credential_PEN") %>% group_by(Match_Award_School_Year) %>% tally()
  tbl(con, "BGS_Matching_STP_Credential_PEN") %>% group_by(Match_All_3_CIP4_Flag) %>% tally()
  tbl(con, "BGS_Matching_STP_Credential_PEN") %>% group_by(Match_All_3_CIP2_Flag) %>% tally()
  
  ## which institutions have the most/least matches on CIP4
  table <- tbl(con, "BGS_Matching_STP_Credential_PEN") %>%
    filter(Match_Inst == "Yes" & Match_Award_School_Year == "Yes") %>%
    group_by(INSTITUTION_CODE)
  
  chk <- table %>% filter(!is.na(Match_All_3_CIP4_Flag)) %>% tally %>%
    full_join(
      table %>% filter(is.na(Match_All_3_CIP4_Flag)) %>% tally,
      by = "INSTITUTION_CODE", 
      suffix = c("_matched", "_unmatched")) %>%
    full_join(
      table %>% tally, 
      by = "INSTITUTION_CODE") %>% 
    mutate(perc_unmatched = n_unmatched *100/n) %>%
    collect() %>%
    arrange(desc(perc_unmatched)) 
 
  rm(table, chk)
}

### Update Final columns based on flags

## Match_All3_CIP4 flag
## if CIPs match nothing to check - use BGS as final (same as STP)
dbGetQuery(con, qry_Update_Final_Match_if_MatchAll3_CIP4Flag)

## Check Matches with Match_All3_CIP2 flag and update final columns
## review the CIP2 matches to make sure the programs look like actual matches
## review the tables to determine which CIP to use (BGS or STP) for each program match
## write to csv if necessary - code not included
{
  t1 <- dbGetQuery(con, qry_Check_BGS_Match_All3_CIP2)
  t2 <- tbl(con, "BGS_Matching_STP_Credential_PEN") %>%
    filter(Match_All_3_CIP2_Flag == "Yes" & is.na(Match_All_3_CIP4_Flag)) %>%
    group_by(INSTITUTION_CODE, PSI_CODE, YEAR, PSI_AWARD_SCHOOL_YEAR, 
             BGS_PROGRAM_CODE, STP_PROGRAM_CODE, BGS_PROGRAM_DESC, STP_PROGRAM_DESC, 
             BGS_FINAL_CIP_CODE_4, BGS_FINAL_CIP_CODE_4_NAME, STP_FINAL_CIP_CODE_4, STP_FINAL_CIP_CODE_4_NAME, Match_All_3_CIP2_Flag) %>%
    summarize(Expr1 = n(), .groups = "drop") %>%
    collect()
  
  ## 2023 Code - based on review
  
  ## Step 1: take more detailed CIP when "general program" used in one source
  ## STP still generally has a more detailed program description - mostly end up going with STP CIP
  matched_2d_cips <- t2 %>%
    mutate(CIP_TO_USE = case_when(BGS_FINAL_CIP_CODE_4 %in% 
                                    c("1101","1301","1401","1901","2301","2401","2601","4001","4201","4501","5001","5201","5501") ~ "STP",
                                  STP_FINAL_CIP_CODE_4 %in% 
                                    c("1101","1301","1401","1901","2301","2401","2601","4001","4201","4501","5001","5201","5501") ~ "BGS"))
  
  count(matched_2d_cips, CIP_TO_USE)
  chk_cips_review1 <- t2 %>% inner_join(matched_2d_cips %>%
                                          select(INSTITUTION_CODE, PSI_CODE, YEAR,  PSI_AWARD_SCHOOL_YEAR, 
                                                 BGS_PROGRAM_CODE, BGS_PROGRAM_DESC, STP_PROGRAM_CODE, STP_PROGRAM_DESC, 
                                                 BGS_FINAL_CIP_CODE_4_NAME, STP_FINAL_CIP_CODE_4_NAME, CIP_TO_USE) %>% 
                                          filter(!is.na(CIP_TO_USE)))
  chk_cips_review2 <- matched_2d_cips %>% filter(is.na(CIP_TO_USE))
  
  
  ## Step 2A:
  ## USE CIP4 matches:
  ## Match STP program to STP programs, where STP CIPs also match - use STP CIP
  matched_2d_cips <- matched_2d_cips %>% 
    left_join(t1 %>% distinct(INSTITUTION_CODE, STP_PROGRAM_CODE, STP_PROGRAM_DESC, CIP = BGS_FINAL_CIP_CODE_4, STP_FINAL_CIP_CODE_4),
              by = c("INSTITUTION_CODE", "STP_PROGRAM_CODE", "STP_PROGRAM_DESC", "STP_FINAL_CIP_CODE_4")) %>%
    mutate(CIP_TO_USE = case_when(!is.na(CIP_TO_USE) ~ CIP_TO_USE,
                                  !is.na(CIP) ~ "STP")) %>%
    select(-CIP) 
  
  count(matched_2d_cips, CIP_TO_USE)
  chk_cips_review1 <- t2 %>% inner_join(matched_2d_cips %>%
                                          select(INSTITUTION_CODE, PSI_CODE, YEAR,  PSI_AWARD_SCHOOL_YEAR, 
                                                 BGS_PROGRAM_CODE, BGS_PROGRAM_DESC, STP_PROGRAM_CODE, STP_PROGRAM_DESC, 
                                                 BGS_FINAL_CIP_CODE_4_NAME, STP_FINAL_CIP_CODE_4_NAME, CIP_TO_USE) %>% 
                                          filter(!is.na(CIP_TO_USE)))
  chk_cips_review2 <- matched_2d_cips %>% filter(is.na(CIP_TO_USE))
  
  ## Step 2B:
  ## Match BGS program to BGS programs in t1 where BGS CIP also match - use BGS CIP
  matched_2d_cips <- matched_2d_cips %>%
    ## Match to BGS Program and CIP - use BGS CIP
    left_join(t1 %>% distinct(INSTITUTION_CODE, BGS_PROGRAM_CODE, BGS_PROGRAM_DESC, BGS_FINAL_CIP_CODE_4, CIP = STP_FINAL_CIP_CODE_4),
              by = c("INSTITUTION_CODE", "BGS_PROGRAM_CODE", "BGS_PROGRAM_DESC", "BGS_FINAL_CIP_CODE_4")) %>%
    mutate(CIP_TO_USE = case_when(!is.na(CIP_TO_USE) ~ CIP_TO_USE,
                                  !is.na(CIP) ~ "BGS")) %>%
    select(-CIP)
  
  count(matched_2d_cips, CIP_TO_USE)
  chk_cips_review1 <- t2 %>% inner_join(matched_2d_cips %>%
                                          select(INSTITUTION_CODE, PSI_CODE, YEAR,  PSI_AWARD_SCHOOL_YEAR, 
                                                 BGS_PROGRAM_CODE, BGS_PROGRAM_DESC, STP_PROGRAM_CODE, STP_PROGRAM_DESC, 
                                                 BGS_FINAL_CIP_CODE_4_NAME, STP_FINAL_CIP_CODE_4_NAME, CIP_TO_USE) %>% 
                                          filter(!is.na(CIP_TO_USE)))
  chk_cips_review2 <- matched_2d_cips %>% filter(is.na(CIP_TO_USE))
  
  ## Step 3: custom changes - may not be applicable every year
  ## BGS cip = 2701 (Mathematics) and STP cip = 2703 (Applied Mathematics) - go with STP
  ## These programs are "operations research" or "mash" - Program with "mash" in CIP4 matches uses 2703
  matched_2d_cips <- matched_2d_cips %>%
    mutate(CIP_TO_USE = case_when(!is.na(CIP_TO_USE) ~ CIP_TO_USE,
                                  BGS_FINAL_CIP_CODE_4 == "2701" & STP_FINAL_CIP_CODE_4 == "2703" ~ "STP"))
  
  count(matched_2d_cips, CIP_TO_USE)
  chk_cips_review1 <- t2 %>% inner_join(matched_2d_cips %>%
                                          select(INSTITUTION_CODE, PSI_CODE, YEAR,  PSI_AWARD_SCHOOL_YEAR, 
                                                 BGS_PROGRAM_CODE, BGS_PROGRAM_DESC, STP_PROGRAM_CODE, STP_PROGRAM_DESC, 
                                                 BGS_FINAL_CIP_CODE_4_NAME, STP_FINAL_CIP_CODE_4_NAME, CIP_TO_USE) %>% 
                                          filter(!is.na(CIP_TO_USE)))
  chk_cips_review2 <- matched_2d_cips %>% filter(is.na(CIP_TO_USE))
  
  ## BGS cip = 1405 (Bioengineering) and STP cip = 1407 (Chemical Engineering) - go with STP
  ## These programs all say "Chemical engineering
  matched_2d_cips <- matched_2d_cips %>%
    mutate(CIP_TO_USE = case_when(!is.na(CIP_TO_USE) ~ CIP_TO_USE,
                                  BGS_FINAL_CIP_CODE_4 == "1405" & STP_FINAL_CIP_CODE_4 == "1407" ~ "STP"))
  
  count(matched_2d_cips, CIP_TO_USE)
  chk_cips_review1 <- t2 %>% inner_join(matched_2d_cips %>%
                                          select(INSTITUTION_CODE, PSI_CODE, YEAR,  PSI_AWARD_SCHOOL_YEAR, 
                                                 BGS_PROGRAM_CODE, BGS_PROGRAM_DESC, STP_PROGRAM_CODE, STP_PROGRAM_DESC, 
                                                 BGS_FINAL_CIP_CODE_4_NAME, STP_FINAL_CIP_CODE_4_NAME, CIP_TO_USE) %>% 
                                          filter(!is.na(CIP_TO_USE)))
  chk_cips_review2 <- matched_2d_cips %>% filter(is.na(CIP_TO_USE))
  
  ## The remainder seem to be double majors where the order of the programs differs between BGS and STP
  ## go with STP for these to remain consistent with historical methods
  matched_2d_cips <- matched_2d_cips %>%
    mutate(CIP_TO_USE = case_when(!is.na(CIP_TO_USE) ~ CIP_TO_USE,
                                  TRUE ~ "STP"))
  
  count(matched_2d_cips, CIP_TO_USE)
}

## Update BGS_Matching_STP_Credential_PEN final cips based on matching work
{
 ## create a temporary table  
 temp_tbl <-  tbl(con, "BGS_Matching_STP_Credential_PEN") %>%
   collect() %>%
    left_join(matched_2d_cips %>%
                select(INSTITUTION_CODE, PSI_CODE, YEAR, PSI_AWARD_SCHOOL_YEAR, 
                       BGS_PROGRAM_CODE, STP_PROGRAM_CODE, BGS_PROGRAM_DESC, STP_PROGRAM_DESC, 
                       BGS_FINAL_CIP_CODE_4, STP_FINAL_CIP_CODE_4, Match_All_3_CIP2_Flag, CIP_TO_USE), 
              by = c("INSTITUTION_CODE", "PSI_CODE", "YEAR", "PSI_AWARD_SCHOOL_YEAR", "BGS_PROGRAM_CODE",
                     "STP_PROGRAM_CODE", "BGS_PROGRAM_DESC", "STP_PROGRAM_DESC", "BGS_FINAL_CIP_CODE_4", 
                     "STP_FINAL_CIP_CODE_4", "Match_All_3_CIP2_Flag")) %>%
    mutate(FINAL_CIP_CODE_4 = case_when(!is.na(FINAL_CIP_CODE_4) ~ FINAL_CIP_CODE_4,
                                        CIP_TO_USE == "BGS" ~ BGS_FINAL_CIP_CODE_4,
                                        CIP_TO_USE == "STP" ~ STP_FINAL_CIP_CODE_4),
           FINAL_CIP_CODE_2 = case_when(!is.na(FINAL_CIP_CODE_2) ~ FINAL_CIP_CODE_2,
                                        CIP_TO_USE == "BGS" ~ BGS_FINAL_CIP_CODE_2,
                                        CIP_TO_USE == "STP" ~ STP_FINAL_CIP_CODE_2),
           USE_BGS_CIP  = case_when(!is.na(USE_BGS_CIP) ~ USE_BGS_CIP,
                                    CIP_TO_USE == "BGS" ~ "Yes",
                                    CIP_TO_USE == "STP" ~ "No")) 
 
  ## write temp_tbl to SQL 
 dbWriteTable(con, "temp_tbl", temp_tbl)
 
 ## update BGS_Matching_STP_Credential_PEN
 dbGetQuery(con, "
            UPDATE BGS_Matching_STP_Credential_PEN
            SET    BGS_Matching_STP_Credential_PEN.FINAL_CIP_CODE_4 = temp_tbl.FINAL_CIP_CODE_4,
                   BGS_Matching_STP_Credential_PEN.FINAL_CIP_CODE_2 = temp_tbl.FINAL_CIP_CODE_2,
                   BGS_Matching_STP_Credential_PEN.USE_BGS_CIP = temp_tbl.USE_BGS_CIP
            FROM   BGS_Matching_STP_Credential_PEN INNER JOIN temp_tbl 
            ON     BGS_Matching_STP_Credential_PEN.INSTITUTION_CODE = temp_tbl.INSTITUTION_CODE AND
                   BGS_Matching_STP_Credential_PEN.PSI_CODE = temp_tbl.PSI_CODE AND
                   BGS_Matching_STP_Credential_PEN.YEAR = temp_tbl.YEAR AND
                   BGS_Matching_STP_Credential_PEN.PSI_AWARD_SCHOOL_YEAR = temp_tbl.PSI_AWARD_SCHOOL_YEAR AND
                   BGS_Matching_STP_Credential_PEN.BGS_PROGRAM_DESC = temp_tbl.BGS_PROGRAM_DESC AND
                   BGS_Matching_STP_Credential_PEN.STP_PROGRAM_DESC = temp_tbl.STP_PROGRAM_DESC AND
                   BGS_Matching_STP_Credential_PEN.BGS_FINAL_CIP_CODE_4 = temp_tbl.BGS_FINAL_CIP_CODE_4 AND
                   BGS_Matching_STP_Credential_PEN.STP_FINAL_CIP_CODE_4 = temp_tbl.STP_FINAL_CIP_CODE_4
            WHERE  BGS_Matching_STP_Credential_PEN.FINAL_CIP_CODE_4 is NULL ")
 
 dbGetQuery(con, "
            UPDATE BGS_Matching_STP_Credential_PEN
            SET BGS_Matching_STP_Credential_PEN.Final_Consider_A_Match = 'Yes'
            WHERE USE_BGS_CIP is not NULL")
 
 ## compare counts
 temp_tbl %>% count(Match_All_3_CIP4_Flag, Match_All_3_CIP2_Flag , USE_BGS_CIP)
 tbl(con, "BGS_Matching_STP_Credential_PEN") %>% count(Match_All_3_CIP4_Flag, Match_All_3_CIP2_Flag ,USE_BGS_CIP)
 tbl(con, "BGS_Matching_STP_Credential_PEN") %>% count(Final_Consider_A_Match)
 
 ## remove temp/chk tables
 rm(temp_tbl, matched_2d_cips, chk_cips_review1, chk_cips_review2, t1, t2)
 dbRemoveTable(con, "temp_tbl")
 
}

### Part 3C: Manual matching ----

## create a table of records that match on inst and award year (different CIP4)
## including the program information to check what is the right CIP to use
BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_orig <- dbGetQuery(con, qry_BGS_Matching_STP_Credential_PEN_Inst_AwardYearOnly) ## creates BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly

## aggregate the data by program and CIP and save to CSV for manual matching (review save location)
## save the worked on table without the _orig to avoid overwriting work
BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_orig %>%
  mutate(across(everything(), trimws)) %>%
  group_by(INSTITUTION_CODE, PSI_CODE, BGS_FINAL_CIP_CODE_4, BGS_FINAL_CIP_CODE_4_NAME,
           STP_FINAL_CIP_CODE_4, STP_FINAL_CIP_CODE_4_NAME, BGS_PROGRAM_CODE,
           BGS_PROGRAM_DESC, STP_PROGRAM_CODE, STP_PROGRAM_DESC, USE_BGS_CIP) %>%
  summarize(Count = n(), .groups = "drop") %>%
  write_csv("BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_ProgramCombos_orig.csv")

## save _orig file into an excel workbook removing _orig from the file name
## once satisfied with the table, copy it to a new workbook and save as a csv file
## read back in as BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_ProgramCombos (review file location)
BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_ProgramCombos <- read_csv("BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_ProgramCombos.csv")

## join manual work back to row-level data
## have to alter the tables since reading it in from CSV removes excess whitespace and changes empty strings to NA
BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly <- BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_orig %>%
  mutate(across(everything(), trimws)) %>%
  select(-USE_BGS_CIP) %>%  ## all NA at this stage
  left_join(BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_ProgramCombos %>%
              select(-BGS_FINAL_CIP_CODE_4_NAME, -STP_FINAL_CIP_CODE_4_NAME),
              by = c("INSTITUTION_CODE", "PSI_CODE", "BGS_FINAL_CIP_CODE_4", 
                   "STP_FINAL_CIP_CODE_4", 
                   "BGS_PROGRAM_CODE", "BGS_PROGRAM_DESC", 
                   "STP_PROGRAM_CODE", "STP_PROGRAM_DESC"))
 

## check USE_BGS_CIP column - do not want any NAs
{
  BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly %>% count(USE_BGS_CIP)
  
  ## if NAs check that both tables have the same STP_PROGRAM_CODEs
  chk1 <- BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_ProgramCombos %>% count(STP_PROGRAM_CODE)
  chk2 <- BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_orig %>% count(STP_PROGRAM_CODE)
  
  rm(chk1, chk2)
}

## update FINAL CIP columns
BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly <- BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly %>%
  mutate(FINAL_CIP_CODE_4 =      case_when(USE_BGS_CIP == "No" ~ STP_FINAL_CIP_CODE_4,
                                           USE_BGS_CIP == "Yes" ~ BGS_FINAL_CIP_CODE_4),
         FINAL_CIP_CODE_2 =      case_when(USE_BGS_CIP == "No" ~ STP_FINAL_CIP_CODE_2,
                                           USE_BGS_CIP == "Yes" ~ BGS_FINAL_CIP_CODE_2)) 

## check that there is no blank final CIPs where there is a match. Expect USE_BGS_CIP = x
{
  BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly %>% filter(is.na(FINAL_CIP_CODE_4)) %>% count(USE_BGS_CIP)  
  BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly %>% filter(is.na(FINAL_CIP_CODE_2)) %>% count(USE_BGS_CIP)  
}

## Write table to SQL
dbWriteTable(con, "BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly", BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly)

## Update BGS_Matching_STP_Credential_PEN with final CIPs chosen manually
{
  ## may want to save a back up copy of BGS_Matching_STP_Credential_PEN before updating it
  ## in case you want to make changes to the manual matching
  dbGetQuery(con, "select * into BGS_Matching_STP_Credential_PEN_bu from BGS_Matching_STP_Credential_PEN")
}
dbGetQuery(con, qry_update_CIP_for_MatchingYearInstOnly_step1)

## check remaining non-matches: compare program descriptions to ensure they really are non-matches
{
  ## get IDs of matches
  ids_exact <- tbl(con, "BGS_Matching_STP_Credential_PEN") %>% filter(Final_Consider_A_Match == "Yes") %>% distinct(ID) %>% collect()
  ids_probable <- tbl(con, "BGS_Matching_STP_Credential_PEN") %>% filter(Final_Probable_Match == "Yes") %>% distinct(ID) %>% collect()
  
  ## filter out non-matches for students that have an existing matched program
  ## review non-matches to see if any should be matched - if so, redo Part 3C to this point
   chk <- tbl(con, "BGS_Matching_STP_Credential_PEN") %>% 
              filter(is.na(FINAL_CIP_CODE_4)) %>%                           ## filter on empty FINAL CIP
              filter(!is.na(Match_Inst) & !is.na(Match_Award_School_Year)) %>% ## remove records that don't match on institution or year
              collect() %>%
              anti_join(ids_exact, by = "ID") %>%                          ## remove records that already have a match (from flags)
              anti_join(ids_probable, by = "ID") %>%                       ## remove records that already have a match (from manual)
              group_by(INSTITUTION_CODE, BGS_FINAL_CIP_CODE_4, BGS_FINAL_CIP_CODE_4_NAME,
                       STP_FINAL_CIP_CODE_4, STP_FINAL_CIP_CODE_4_NAME, BGS_PROGRAM_CODE,
                       BGS_PROGRAM_DESC, STP_PROGRAM_CODE, STP_PROGRAM_DESC, USE_BGS_CIP) %>%
              summarize(Count = n(), .groups = "drop")
   
   rm(chk, ids_exact, ids_probable)
}

## Update the rest of the records to use the STP CIPs as final (even if they are different from BGS)
dbGetQuery(con, qry_update_CIP_for_MatchingYearInstOnly_step2)

## check
{
tbl(con, "BGS_Matching_STP_Credential_PEN") %>% 
    count(USE_BGS_CIP, 
          FINAL_CIP_CODE_4 == STP_FINAL_CIP_CODE_4,
          FINAL_CIP_CODE_4 == BGS_FINAL_CIP_CODE_4)  
}

## remove local tables
rm(BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly,
   BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_orig,
   BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_ProgramCombos)

### Part 3D: Fill in Final Columns ----

## Add in FINAL_CIP_CODE_4_NAME
dbGetQuery(con, qry_fill_final_CIP4_NAME)
dbGetQuery(con, qry_fill_final_CIP2_NAME_and_CLUSTER)

## check for blanks
{
  tbl(con, "BGS_Matching_STP_Credential_PEN") %>% filter(is.na(FINAL_CIP_CLUSTER_CODE)) %>% tally()
}


## Part 4: Update Credential_Non_Dup  ----
## Created tables: Credential_Unmatched_CIPS_to_update
## Updated tables: Credential_Non_Dup_BGS_IDs

### Part 4A: Update with XWALK ----

{
  ## may want to make a backup copy of Credential_Non_Dup_BGS_IDs 
  ## in case you want to make changes to the manual matching
  dbGetQuery(con, "select * into Credential_Non_Dup_BGS_IDs_bu from Credential_Non_Dup_BGS_IDs")
}

## Fill in final CIPS with BGS_Matching_STP_Credential_PEN
dbGetQuery(con, qry_BGS_IDs_Credential_add_columns)
dbGetQuery(con, qry_update_Credential_Non_Dup_BGS_IDS_CIP_matches_step1) ## fill in final CIP, etc. from BGS_Matching_STP_Credential_PEN where Final_Consider_A_Match is not empty
dbGetQuery(con, qry_update_Credential_Non_Dup_BGS_IDS_CIP_matches_step2) ## fill in still empty CIP, etc. from BGS_Matching_STP_Credential_PEN where Final_Probable_Match is not empty

### Part 4B: Update Unmatched CIPs ----

## Fill in remaining final CIPS with STP CIP from Credential_Non_Dup_BGS_IDS
dbGetQuery(con, qry_update_remaining_BGS_CIPs_in_Cred_Non_Dup_BGS_IDS_step1) ## use STP CIP as final for remaining
dbGetQuery(con, qry_update_remaining_BGS_CIPs_in_Cred_Non_Dup_BGS_IDS_step2) ## fill in CIP_CLUSTER info for remaining

## Create a list of programs that matched to outcomes data and use BGS CIPs instead of STP
Credential_Matched_CIPS_using_BGS <- dbGetQuery(con, qry_List_STP_Credential_Non_Dup_Using_BGS_CIPS) ## find programs using BGS
## Create a list of programs that did not match to outcomes data
Credential_Unmatched_CIPS <- dbGetQuery(con, qry_List_STP_Credential_Non_Dup_Umatched) ## find unmatched programs

## Combine the lists to find any unmatched programs that were matched to outcomes for different records
## Filter where the BGS and STP CIPs differ
Credential_Unmatched_CIPS_to_review <- Credential_Unmatched_CIPS %>%
  select(-OUTCOMES_CIP_CODE_4, -OUTCOMES_CIP_CODE_4_NAME) %>%
  left_join(Credential_Matched_CIPS_using_BGS %>% 
              distinct(PSI_CODE, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION, STP_CIP_CODE_4,
                     OUTCOMES_CIP_CODE_4, OUTCOMES_CIP_CODE_4_NAME),
            by = c("PSI_CODE", "PSI_PROGRAM_CODE", "PSI_CREDENTIAL_PROGRAM_DESCRIPTION", "STP_CIP_CODE_4")) %>%
  mutate(Unmatched_But_in_BGS_Program = case_when(!is.na(OUTCOMES_CIP_CODE_4) ~ 'Yes'),
         BGS_CIP_is_Different = case_when(OUTCOMES_CIP_CODE_4 != STP_CIP_CODE_4 ~ 'Yes')) %>%
  group_by(PSI_CODE, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION, STP_CIP_CODE_4) %>%
  filter(Unmatched_But_in_BGS_Program == "Yes" & BGS_CIP_is_Different == "Yes") %>%
  select(-PSI_CREDENTIAL_PROGRAM_DESCRIPTION, everything(), PSI_CREDENTIAL_PROGRAM_DESCRIPTION) %>%
  arrange(FINAL_CIP_CODE_4)

## review the outcomes credentials matched to the unmatched programs
## filter out programs with more than one match
## if any should be updated - update the custom query below
chk <- Credential_Unmatched_CIPS_to_review %>% 
  group_by(PSI_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION,STP_CIP_CODE_4, STP_CIP_CODE_4_NAME) %>%
  summarize(OUTCOMES_CIP_NAME = paste(OUTCOMES_CIP_CODE_4_NAME, collapse = "\n "),
            OUTCOMES_CIP_CODE = paste(OUTCOMES_CIP_CODE_4, collapse = "\n "),
            count = n()) %>%
  filter(count == 1)

## make a table with PROGRAM_DESCRIPTIONs decided to update
Credential_Unmatched_CIPS_to_update <-  tibble::tribble(
  ~PSI_CREDENTIAL_PROGRAM_DESCRIPTION,                               ~FINAL_CIP_CODE_4, ~FINAL_CIP_CODE_2,
  "Bachelor Of Applied Science In Mechatronic Systems Engineering",               1442,                14,   
  "Bachelor Of Athletic And Exercise Therapy",                                    5123,                51, 
  "Bachelor Of Fine Arts In Dance",                                               5003,                50,
  "Bachelor Of Fine Arts In Film",                                                5006,                50,
  "Bachelor Of Fine Arts In Music - Composition",                                 5009,                50,   
  "Bachelor Of Fine Arts In Music - Electroacoustic",                             5009,                50,   
  "Bachelor Of Fine Arts In Theatre - Performance",                               5005,                50,   
  "Bachelor Of Fine Arts In Theatre - Production And Design",                     5005,                50,
  "Bachelor Of Science In Geographic Information Science",                        4507,                45,
  "Bachelor Of Social Work In Indigenous Child Welfare",                          4407,                44,   
  "Bachelor Of Social Work In Indigenous Social Work",                            4407,                44,   
  "Bachelor Of Child & Youth Care In Child & Youth Care",                         1907,                19,   
  "Bachelor Of Child & Youth Care In Child & Youth Care - Child Life Stream",     1907,                19,   
  "Bachelor Of Child & Youth Care In Child & Youth Care - Early Years Stream",    1907,                19,   
  "Bachelor Of Child & Youth Care In Child & Youth Care - Child Protection",      1907,                19,   
  "Bachelor Of Child & Youth Care In Child & Youth Care - Indigenous Stream",     1907,                19)

## write to SQL
dbWriteTable(con, "Credential_Unmatched_CIPS_to_update", Credential_Unmatched_CIPS_to_update)

## update Credential_Non_Dup_BGS_IDs so unmatched programs use linked BGS CIP instead
dbGetQuery(con,  qry_update_Credential_Non_DUP_BGS_IDs_unmatched)

## checks 
{
  tbl(con, "Credential_Non_Dup_BGS_IDs") %>% filter(is.na(FINAL_CIP_CODE_4_NAME)) %>% count(Final_Consider_A_Match, Final_Probable_Match)
  tbl(con, "Credential_Non_Dup_BGS_IDs") %>% filter(is.na(FINAL_CIP_CODE_4_NAME)) %>% count(FINAL_CIP_CODE_4)
}

dbGetQuery(con, qry_fill_final_CIP4_NAME_Credential)
dbGetQuery(con, qry_fill_final_CIP2_NAME_and_CLUSTER_Credential)

## check for blanks
{
  tbl(con, "Credential_Non_Dup_BGS_IDs") %>% filter(is.na(FINAL_CIP_CODE_4_NAME)) 
  tbl(con, "Credential_Non_Dup_BGS_IDs") %>% filter(is.na(FINAL_CIP_CLUSTER_CODE)) 
}


## remove local tables
rm(Credential_Matched_CIPS_using_BGS, Credential_Unmatched_CIPS, Credential_Unmatched_CIPS_to_review, chk, Credential_Unmatched_CIPS_to_update)

## Part 5: Update T_BGS_DATA_FINAL ----
## Created tables: T_BGS_Data_Final_CIPS_to_update
## Updated tables: T_BGS_Data_Final_CIP_for_OutcomesMatching

### Part 5A: Update with XWALK ----

{
  ## may want to make a backup copy of T_BGS_Data_Final_for_OutcomesMatching
  ## in case you want to make changes to the manual matching
  dbGetQuery(con, "select * into T_BGS_Data_Final_for_OutcomesMatching_bu from T_BGS_Data_Final_for_OutcomesMatching")
}

## Fill in final CIPS with BGS_Matching_STP_Credential_PEN
dbGetQuery(con, qry_T_BGS_Data_add_columns)
dbGetQuery(con, qry_update_T_BGS_Data_CIP_matches_step1) ## fill in final CIP, etc. from BGS_Matching_STP_Credential_PEN where Final_Consider_A_Match is not empty
dbGetQuery(con, qry_update_T_BGS_Data_CIP_matches_step2) ## fill in still empty CIP, etc. from BGS_Matching_STP_Credential_PEN where Final_Probable_Match is not empty
dbGetQuery(con, qry_update_T_BGS_Data_CIP_matches_step3) ## switch USE_BGS_CIP to USE_STP_CIP
dbGetQuery(con, qry_update_T_BGS_Data_CIP_matches_step4) ## drop USE_BGS_CIP 

### Part 5B: Update unmatched CIPs ----

## Fill in remaining final CIPS with BGS CIP from T_BGS_Data_Final_for_OutcomesMatching
dbGetQuery(con, qry_update_remaining_BGS_CIPs_in_T_BGS_Data_step1) ## use BGS CIPs as final for remaining
dbGetQuery(con, qry_update_remaining_BGS_CIPs_in_T_BGS_Data_step2) ## fill in CIP_CLUSTER info for remaining

## Create a list of programs that matched to STP data and use STP CIPs instead of BGS
T_BGS_Data_Matched_CIPS_using_STP <- dbGetQuery(con, qry_List_T_BGS_Data_Using_STP_CIPS) 
## Create a list of programs that did not match to STP data
T_BGS_Data_Unmatched_CIPS <- dbGetQuery(con, qry_List_T_BGS_Data_Umatched)

## Combine the lists to find any unmatched programs that were matched to STP for different records
## Filter where the BGS and STP CIPs differ
T_BGS_Data_Unmatched_CIPS_to_review <- T_BGS_Data_Unmatched_CIPS %>%
  select(-STP_CIP_CODE_4, -STP_CIP_CODE_4_NAME) %>%
  left_join(T_BGS_Data_Matched_CIPS_using_STP %>% 
              distinct(INSTITUTION_CODE, CPC, PROGRAM, CIP_4DIGIT_NO_PERIOD,
                       STP_CIP_CODE_4, STP_CIP_CODE_4_NAME),
            by = c("INSTITUTION_CODE", "CPC", "PROGRAM", "CIP_4DIGIT_NO_PERIOD")) %>%
  mutate(Unmatched_But_in_STP_Program = case_when(!is.na(STP_CIP_CODE_4) ~ 'Yes'),
         STP_CIP_is_Different = case_when(STP_CIP_CODE_4 != CIP_4DIGIT_NO_PERIOD ~ 'Yes')) %>%
  group_by(INSTITUTION_CODE, CPC, PROGRAM, CIP_4DIGIT_NO_PERIOD) %>%
  filter(Unmatched_But_in_STP_Program == "Yes" & STP_CIP_is_Different == "Yes") %>%
  select(-PROGRAM, everything(), PROGRAM) %>%
  arrange(FINAL_CIP_CODE_4)

## review the outcomes credentials matched to the unmatched programs
## filter out programs with more than one match
## if any should be updated - update the custom query below
chk <- T_BGS_Data_Unmatched_CIPS_to_review %>% 
  group_by(INSTITUTION_CODE, CPC, PROGRAM, CIP_4DIGIT_NO_PERIOD, CIP4DIG_NAME) %>%
  summarize(STP_CIP_NAME = paste(STP_CIP_CODE_4_NAME, collapse = "\n "),
            STP_CIP_CODE = paste(STP_CIP_CODE_4, collapse = "\n "),
            count = n()) %>%
  filter(count == 1)

## make a table with PROGRAM_DESCRIPTIONs decided to update
T_BGS_Data_Unmatched_CIPS_to_update <-  tibble::tribble(
                                                                                            ~PROGRAM, ~FINAL_CIP_CODE_4, ~FINAL_CIP_CODE_2,
                               "Bachelor of Applied Science - Mechatronic Systems Engineering Major",            "1442",              "14",
                                               "Bachelor of Applied Science In Chemical Engineering",            "1407",              "14",
                             "Bachelor of Applied Science In Chemical Engineering Minor In Commerce",            "1407",              "14",
                             "Bachelor of Applied Science In Chemical Engineering Option in Biology",            "1407",              "14",
                             "Bachelor of Environment - Resource and Environmental Management Major",            "0301",              "03",
"Bachelor of Environment - Resource and Environmental Management Major, First Nations Studies Minor",            "0301",              "03",
            "Bachelor of Environment - Resource and Environmental Management Major, Geography Minor",            "0301",              "03",
                                                 "Bachelor of Science - Biomedical Physiology Major",            "2609",              "26",
                                                         "Bachelor of Science in Applied Psychology",            "4228",              "42")


## write to SQL
dbWriteTable(con, "T_BGS_Data_Unmatched_CIPS_to_update", T_BGS_Data_Unmatched_CIPS_to_update)

## update T_BGS_Data_Final_for_OutcomesMatching so unmatched programs use linked STP CIP instead
dbGetQuery(con, qry_update_T_BGS_Data_unmatched)

## checks 
{
  tbl(con, "T_BGS_Data_Final_for_OutcomesMatching") %>% filter(is.na(FINAL_CIP_CODE_4_NAME)) %>% count(Final_Consider_A_Match, Final_Probable_Match)
  tbl(con, "T_BGS_Data_Final_for_OutcomesMatching") %>% filter(is.na(FINAL_CIP_CODE_4_NAME)) %>% count(FINAL_CIP_CODE_4)
}

dbGetQuery(con, qry_fill_final_CIP4_NAME_T_BGS_Data)
dbGetQuery(con, qry_fill_final_CIP2_NAME_and_CLUSTER_T_BGS_Data)

## check for blanks
{
  tbl(con, "T_BGS_Data_Final_for_OutcomesMatching") %>% filter(is.na(FINAL_CIP_CODE_4_NAME)) 
  tbl(con, "T_BGS_Data_Final_for_OutcomesMatching") %>% filter(is.na(FINAL_CIP_CLUSTER_CODE)) 
}

## remove local tables
rm(T_BGS_Data_Matched_CIPS_using_STP, T_BGS_Data_Unmatched_CIPS, T_BGS_Data_Unmatched_CIPS_to_review, chk, T_BGS_Data_Unmatched_CIPS_to_update)


## End ----
## remove backup tables
dbRemoveTable(con, "BGS_Matching_STP_Credential_PEN_bu")
dbRemoveTable(con, "Credential_Non_Dup_BGS_IDs_bu")
dbRemoveTable(con, "T_BGS_Data_Final_for_OutcomesMatching_bu")
dbDisconnect(con)
