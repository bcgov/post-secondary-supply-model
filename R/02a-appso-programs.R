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

# ---- Configure LAN Paths and DB Connection -----
lan <- config::get("lan")
db_config <- config::get("decimal")
my_schema <- config::get("myschema")

con <- dbConnect(odbc(),
                 Driver = db_config$driver,
                 Server = db_config$server,
                 Database = db_config$database,
                 Trusted_Connection = "True")

# ---- Check Required Tables ----
# main table
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."credential_non_dup"')))

# reference tables
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_2DIGITS_CIP2016"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_4DIGITS_CIP2016"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_6DIGITS_CIP2016"')))

# START QUERIES ----
# ---- create APPSO CIP table ----

# create cleaning table 

# [SELECT INTO] Create Credential_Non_Dup_STP_APPSO_Cleaning from Credential_Non_Dup
# APPSO IDS ----
##
## qry_APPSO_STP_CIP_Cleaning ----
## collect STP APPSO data
## New (from documentation): create table Credential_Non_Dup_STP_APPSO_Cleaning for cleaning STP CIP codes
qry_APPSO_STP_CIP_Cleaning <- "
SELECT PSI_CREDENTIAL_CIP,
       OUTCOMES_CRED,
       COUNT(*) AS Expr1
INTO   Credential_Non_Dup_STP_APPSO_Cleaning
FROM   Credential_Non_Dup
GROUP BY
       PSI_CREDENTIAL_CIP,
       OUTCOMES_CRED
HAVING OUTCOMES_CRED = 'APPSO'"
dbExecute(con, qry_APPSO_STP_CIP_Cleaning)

# add extra cols 

# [ALTER TABLE] Credential_Non_Dup_STP_APPSO_Cleaning

# add columns
qry_APPSO_STP_CIP_add_columns <- "
ALTER TABLE Credential_Non_Dup_STP_APPSO_Cleaning
ADD STP_CIP_CODE_4 varchar (255),
STP_CIP_CODE_4_NAME varchar (255),
STP_CIP_CODE_2 varchar (255),
STP_CIP_CODE_2_NAME varchar (255),
PSI_CREDENTIAL_CIP_orig varchar (255)
"
dbExecute(con, qry_APPSO_STP_CIP_add_columns)

# [UPDATE] Credential_Non_Dup_STP_APPSO_Cleaning

qry_APPSO_STP_CIP_update_original <- "
UPDATE Credential_Non_Dup_STP_APPSO_Cleaning
SET PSI_CREDENTIAL_CIP_orig = PSI_CREDENTIAL_CIP"
dbExecute(con, qry_APPSO_STP_CIP_update_original)

# clean CIPs to be correct format 

# [UPDATE] Credential_Non_Dup_STP_APPSO_Cleaning

# clean CIPs that are wrong length
qry_APPSO_STP_CIP_clean_cip_1 <- "
UPDATE Credential_Non_Dup_STP_APPSO_Cleaning
SET    PSI_CREDENTIAL_CIP = CONCAT(PSI_CREDENTIAL_CIP, '0')
WHERE  LEN(PSI_CREDENTIAL_CIP) = 6 AND
substring(PSI_CREDENTIAL_CIP,1,2) NOT LIKE '%.'"
dbExecute(con, qry_APPSO_STP_CIP_clean_cip_1)

# [UPDATE] Credential_Non_Dup_STP_APPSO_Cleaning

qry_APPSO_STP_CIP_clean_cip_2 <- "
UPDATE Credential_Non_Dup_STP_APPSO_Cleaning
SET    PSI_CREDENTIAL_CIP = CONCAT('0', PSI_CREDENTIAL_CIP)
WHERE LEN(PSI_CREDENTIAL_CIP) = 6"
dbExecute(con, qry_APPSO_STP_CIP_clean_cip_2)

## Update CIP 4 and 2D codes from INFOWARE, matching PSI_CREDENTIAL_CIP to LCIP_CD_WITH_PERIOD

# [UPDATE] Credential_Non_Dup_STP_APPSO_Cleaning

## qry_Clean_APPSO_STP_CIP_Step1_a ----
## Add 4 and 2D CIP codes from INFOWARE matching on PSI_CREDENTIAL_CIP
qry_Clean_APPSO_STP_CIP_Step1_a <- "
UPDATE Credential_Non_Dup_STP_APPSO_Cleaning
SET    Credential_Non_Dup_STP_APPSO_Cleaning.STP_CIP_CODE_4 = [INFOWARE_L_CIP_6DIGITS_CIP2016].[LCIP_LCP4_CD],
       Credential_Non_Dup_STP_APPSO_Cleaning.STP_CIP_CODE_2 = [INFOWARE_L_CIP_6DIGITS_CIP2016].[LCIP_LCP2_CD]
FROM   Credential_Non_Dup_STP_APPSO_Cleaning INNER JOIN INFOWARE_L_CIP_6DIGITS_CIP2016
ON     Credential_Non_Dup_STP_APPSO_Cleaning.PSI_CREDENTIAL_CIP = INFOWARE_L_CIP_6DIGITS_CIP2016.LCIP_CD_WITH_PERIOD"
dbExecute(con, qry_Clean_APPSO_STP_CIP_Step1_a) # all 6 digits

# [UPDATE] Credential_Non_Dup_STP_APPSO_Cleaning

## qry_Clean_APPSO_STP_CIP_Step1_b ----
## New: Add 4 and 2D CIP codes from INFOWARE matching on first 4 digits of PSI_CREDENTIAL_CIP
qry_Clean_APPSO_STP_CIP_Step1_b <- "
UPDATE Credential_Non_Dup_STP_APPSO_Cleaning
SET    Credential_Non_Dup_STP_APPSO_Cleaning.STP_CIP_CODE_4 = [INFOWARE_L_CIP_6DIGITS_CIP2016].[LCIP_LCP4_CD],
       Credential_Non_Dup_STP_APPSO_Cleaning.STP_CIP_CODE_2 = [INFOWARE_L_CIP_6DIGITS_CIP2016].[LCIP_LCP2_CD]
FROM   Credential_Non_Dup_STP_APPSO_Cleaning
INNER JOIN INFOWARE_L_CIP_6DIGITS_CIP2016
ON     substring(Credential_Non_Dup_STP_APPSO_Cleaning.PSI_CREDENTIAL_CIP,1,5) = substring(INFOWARE_L_CIP_6DIGITS_CIP2016.LCIP_CD_WITH_PERIOD,1,5)
WHERE  STP_CIP_CODE_4 is NULL"
dbExecute(con, qry_Clean_APPSO_STP_CIP_Step1_b) # first 4 digits

# [UPDATE] Credential_Non_Dup_STP_APPSO_Cleaning

## qry_Clean_APPSO_STP_CIP_Step2_c ----
## New: Add 4D CIP codes for general programs (if 00 change to 01)
## Check which CIPs have general programs here: https://www.statcan.gc.ca/en/subjects/standard/cip/2021/index
qry_Clean_APPSO_STP_CIP_Step1_c <- "
UPDATE Credential_Non_Dup_STP_APPSO_Cleaning
SET STP_CIP_CODE_4 = CONCAT(substring(Credential_Non_Dup_STP_APPSO_Cleaning.PSI_CREDENTIAL_CIP,1,2), '01')
WHERE (substring(Credential_Non_Dup_STP_APPSO_Cleaning.PSI_CREDENTIAL_CIP,1,5) = 11.00 OR
      substring(Credential_Non_Dup_STP_APPSO_Cleaning.PSI_CREDENTIAL_CIP,1,5) = 13.00 OR
      substring(Credential_Non_Dup_STP_APPSO_Cleaning.PSI_CREDENTIAL_CIP,1,5) = 14.00 OR
      substring(Credential_Non_Dup_STP_APPSO_Cleaning.PSI_CREDENTIAL_CIP,1,5) = 19.00 OR
      substring(Credential_Non_Dup_STP_APPSO_Cleaning.PSI_CREDENTIAL_CIP,1,5) = 23.00 OR
      substring(Credential_Non_Dup_STP_APPSO_Cleaning.PSI_CREDENTIAL_CIP,1,5) = 24.00 OR
      substring(Credential_Non_Dup_STP_APPSO_Cleaning.PSI_CREDENTIAL_CIP,1,5) = 26.00 OR
      substring(Credential_Non_Dup_STP_APPSO_Cleaning.PSI_CREDENTIAL_CIP,1,5) = 40.00 OR
      substring(Credential_Non_Dup_STP_APPSO_Cleaning.PSI_CREDENTIAL_CIP,1,5) = 42.00 OR
      substring(Credential_Non_Dup_STP_APPSO_Cleaning.PSI_CREDENTIAL_CIP,1,5) = 45.00 OR
      substring(Credential_Non_Dup_STP_APPSO_Cleaning.PSI_CREDENTIAL_CIP,1,5) = 50.00 OR
      substring(Credential_Non_Dup_STP_APPSO_Cleaning.PSI_CREDENTIAL_CIP,1,5) = 52.00 OR
      substring(Credential_Non_Dup_STP_APPSO_Cleaning.PSI_CREDENTIAL_CIP,1,5) = 55.00) AND
      STP_CIP_CODE_4 is NULL"
dbExecute(con, qry_Clean_APPSO_STP_CIP_Step1_c) # recode general program CIPs from 00 ending to 01 ending

# [UPDATE] Credential_Non_Dup_STP_APPSO_Cleaning

## qry_Clean_APPSO_STP_CIP_Step1_d ----
## New: Add 2D CIP codes from INFOWARE matching on first 2 digits of PSI_CREDENTIAL_CIP
qry_Clean_APPSO_STP_CIP_Step1_d <- "
UPDATE Credential_Non_Dup_STP_APPSO_Cleaning
SET    Credential_Non_Dup_STP_APPSO_Cleaning.STP_CIP_CODE_2 = [INFOWARE_L_CIP_6DIGITS_CIP2016].[LCIP_LCP2_CD]
FROM   Credential_Non_Dup_STP_APPSO_Cleaning INNER JOIN INFOWARE_L_CIP_6DIGITS_CIP2016
ON     substring(Credential_Non_Dup_STP_APPSO_Cleaning.PSI_CREDENTIAL_CIP,1,2) = substring(INFOWARE_L_CIP_6DIGITS_CIP2016.LCIP_CD_WITH_PERIOD,1,2)
WHERE  STP_CIP_CODE_2 is NULL"
dbExecute(con, qry_Clean_APPSO_STP_CIP_Step1_d) # match first 2 digits

# [UPDATE] Credential_Non_Dup_STP_APPSO_Cleaning

## qry_Clean_APPSO_STP_CIP_Step2 ----
# Add 4D names
qry_Clean_APPSO_STP_CIP_Step2 <- "
UPDATE Credential_Non_Dup_STP_APPSO_Cleaning
SET    Credential_Non_Dup_STP_APPSO_Cleaning.STP_CIP_CODE_4_NAME = [INFOWARE_L_CIP_4DIGITS_CIP2016].[LCP4_CIP_4DIGITS_NAME]
FROM   Credential_Non_Dup_STP_APPSO_Cleaning INNER JOIN INFOWARE_L_CIP_4DIGITS_CIP2016
ON     Credential_Non_Dup_STP_APPSO_Cleaning.STP_CIP_CODE_4 = INFOWARE_L_CIP_4DIGITS_CIP2016.LCP4_CD"
dbExecute(con, qry_Clean_APPSO_STP_CIP_Step2) # add CIP 4D names

# [UPDATE] Credential_Non_Dup_STP_APPSO_Cleaning

## qry_Clean_APPSO_STP_CIP_Step3 ----
# Add 2D names
qry_Clean_APPSO_STP_CIP_Step3 <- "
UPDATE Credential_Non_Dup_STP_APPSO_Cleaning
SET    Credential_Non_Dup_STP_APPSO_Cleaning.STP_CIP_CODE_2_NAME = [INFOWARE_L_CIP_2DIGITS_CIP2016].[LCP2_DIGITS_NAME]
FROM   Credential_Non_Dup_STP_APPSO_Cleaning
INNER JOIN INFOWARE_L_CIP_2DIGITS_CIP2016
ON     Credential_Non_Dup_STP_APPSO_Cleaning.STP_CIP_CODE_2 = INFOWARE_L_CIP_2DIGITS_CIP2016.LCP2_CD"
dbExecute(con, qry_Clean_APPSO_STP_CIP_Step3) # add CIP 2D names

# [UPDATE] Credential_Non_Dup_STP_APPSO_Cleaning

## qry_Clean_APPSO_STP_CIP_step4 ----
## New: Set blank 4D names to Invalid 4-digit CIP
qry_Clean_APPSO_STP_CIP_step4 <- "
UPDATE Credential_Non_Dup_STP_APPSO_Cleaning
SET    Credential_Non_Dup_STP_APPSO_Cleaning.STP_CIP_CODE_4_NAME = 'Invalid 4-digit CIP'
WHERE Credential_Non_Dup_STP_APPSO_Cleaning.STP_CIP_CODE_4_NAME is NULL"
dbExecute(con, qry_Clean_APPSO_STP_CIP_step4) # mark “Invalid 4-digit CIP” for remaining blank 4D names

# [SELECT INTO] Create Credential_Non_Dup_APPSO_IDs from Credential_Non_Dup

## qry_Update_Credential_with_STP_CIP_APPSO ----
## Update STP columns in Credential_Non_Dup and filter on APPSO credentials
qry_Update_Credential_with_STP_CIP_APPSO <-"
Select Credential_Non_Dup.ID,
       Credential_Non_Dup.PSI_CODE,
       Credential_Non_Dup.PSI_PROGRAM_CODE,
       Credential_Non_Dup.PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
       Credential_Non_Dup.PSI_CREDENTIAL_CIP,
       Credential_Non_Dup.PSI_AWARD_SCHOOL_YEAR,
       Credential_Non_Dup.OUTCOMES_CRED,
       FINAL_CIP_CODE_4 = Credential_Non_Dup_STP_APPSO_Cleaning.STP_CIP_CODE_4,
       FINAL_CIP_CODE_4_NAME = Credential_Non_Dup_STP_APPSO_Cleaning.STP_CIP_CODE_4_NAME,
       FINAL_CIP_CODE_2 = Credential_Non_Dup_STP_APPSO_Cleaning.STP_CIP_CODE_2,
       FINAL_CIP_CODE_2_NAME = Credential_Non_Dup_STP_APPSO_Cleaning.STP_CIP_CODE_2_NAME
INTO   Credential_Non_Dup_APPSO_IDs
FROM   Credential_Non_Dup INNER JOIN Credential_Non_Dup_STP_APPSO_Cleaning
ON     Credential_Non_Dup.PSI_CREDENTIAL_CIP = Credential_Non_Dup_STP_APPSO_Cleaning.PSI_CREDENTIAL_CIP_orig AND
       Credential_Non_Dup.OUTCOMES_CRED = Credential_Non_Dup_STP_APPSO_Cleaning.OUTCOMES_CRED
WHERE  Credential_Non_Dup.OUTCOMES_CRED = 'APPSO'"
dbExecute(con, qry_Update_Credential_with_STP_CIP_APPSO) # create ID list

# [UPDATE] Credential_Non_Dup_APPSO_IDs

## replace unspecified with NULL
qry_Update_Credential_with_STP_CIP_APPSO_nulls <- "
Update Credential_Non_Dup_APPSO_IDs
SET PSI_PROGRAM_CODE = NULL
WHERE PSI_PROGRAM_CODE = '(Unspecified)'
"
dbExecute(con, qry_Update_Credential_with_STP_CIP_APPSO_nulls) # in 2023 only PSI_PROGRAM_CODE had (Unspecified) - replace with NULLs

# ---- Clean up ----
dbExecute(con, "DROP TABLE Credential_Non_Dup_STP_APPSO_Cleaning")
dbDisconnect(con)
