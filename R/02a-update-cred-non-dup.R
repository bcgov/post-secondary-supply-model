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

# Update Credential Non Dup
# Description: 
# Relies on:
#   - credential_non_dup, 
#   - Credential_Non_Dup_Programs_DACSO_FinalCIPs
#   - Credential_Non_Dup_BGS_IDs
#   - Credential_Non_Dup_GRAD_IDs
# Creates updated credential non duplicate table with updated CIP records
# Uses work done during program matching

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

# tables with CIP updates 
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."Credential_Non_Dup_Programs_DACSO_FinalCIPs"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."Credential_Non_Dup_BGS_IDs"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."Credential_Non_Dup_GRAD_IDs"')))
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."Credential_Non_Dup_APPSO_IDs"')))

# reference tables
dbExistsTable(con, SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_2DIGITS_CIP2016"')))

# START QUERIES ----
# ---- Create additional required columns ----

# [ALTER TABLE] Credential_Non_Dup
## update columns of credential non dup
qry_Credential_Non_Dup_Add_Columns <- "
ALTER TABLE Credential_Non_Dup
ADD         OUTCOMES_CIP_CODE_4 varchar(4),
            OUTCOMES_CIP_CODE_4_NAME varchar(255),
            FINAL_CIP_CODE_4 varchar(4),
            FINAL_CIP_CODE_4_NAME varchar(255),
            FINAL_CIP_CODE_2 varchar(2),
            FINAL_CIP_CODE_2_NAME varchar(255),
            FINAL_CIP_CLUSTER_CODE varchar(10),
            FINAL_CIP_CLUSTER_NAME varchar(255),
            STP_CIP_CODE_4 varchar(4),
            STP_CIP_CODE_4_NAME varchar(255),
            STP_CIP_CODE_2 varchar(2),
            STP_CIP_CODE_2_NAME varchar(255);
"
dbExecute(con, qry_Credential_Non_Dup_Add_Columns) 

# ---- Update non dup with new CIP codes from DACSO, BGS and GRAD records ----

# [UPDATE] Credential_Non_Dup

## qry_update_Credential_Non_Dup_DACSO_Final_CIPs ----
qry_update_Credential_Non_Dup_DACSO_Final_CIPs <- "
UPDATE     Credential_Non_Dup
SET        OUTCOMES_CIP_CODE_4 = Credential_Non_Dup_Programs_DACSO_FinalCIPs.OUTCOMES_CIP_CODE_4,
           OUTCOMES_CIP_CODE_4_NAME = Credential_Non_Dup_Programs_DACSO_FinalCIPs.OUTCOMES_CIP_CODE_4_NAME,
           FINAL_CIP_CODE_4 = Credential_Non_Dup_Programs_DACSO_FinalCIPs.FINAL_CIP_CODE_4,
           FINAL_CIP_CODE_4_NAME = Credential_Non_Dup_Programs_DACSO_FinalCIPs.FINAL_CIP_CODE_4_NAME,
           FINAL_CIP_CODE_2 = Credential_Non_Dup_Programs_DACSO_FinalCIPs.FINAL_CIP_CODE_2,
           FINAL_CIP_CODE_2_NAME = Credential_Non_Dup_Programs_DACSO_FinalCIPs.FINAL_CIP_CODE_2_NAME,
           FINAL_CIP_CLUSTER_CODE = Credential_Non_Dup_Programs_DACSO_FinalCIPs.FINAL_CIP_CLUSTER_CODE,
           FINAL_CIP_CLUSTER_NAME = Credential_Non_Dup_Programs_DACSO_FinalCIPs.FINAL_CIP_CLUSTER_NAME,
           STP_CIP_CODE_4 = Credential_Non_Dup_Programs_DACSO_FinalCIPs.STP_CIP_CODE_4,
           STP_CIP_CODE_4_NAME = Credential_Non_Dup_Programs_DACSO_FinalCIPs.STP_CIP_CODE_4_NAME
FROM       Credential_Non_Dup_Programs_DACSO_FinalCIPs
INNER JOIN Credential_Non_Dup
ON         Credential_Non_Dup_Programs_DACSO_FinalCIPs.PSI_CODE = Credential_Non_Dup.PSI_CODE
AND        Credential_Non_Dup_Programs_DACSO_FinalCIPs.PSI_PROGRAM_CODE = Credential_Non_Dup.PSI_PROGRAM_CODE
AND        Credential_Non_Dup_Programs_DACSO_FinalCIPs.PSI_CREDENTIAL_PROGRAM_DESCRIPTION = Credential_Non_Dup.PSI_CREDENTIAL_PROGRAM_DESCRIPTION
AND        Credential_Non_Dup_Programs_DACSO_FinalCIPs.PSI_CREDENTIAL_CIP = Credential_Non_Dup.PSI_CREDENTIAL_CIP
AND        Credential_Non_Dup_Programs_DACSO_FinalCIPs.PSI_CREDENTIAL_LEVEL = Credential_Non_Dup.PSI_CREDENTIAL_LEVEL
AND        Credential_Non_Dup_Programs_DACSO_FinalCIPs.PSI_CREDENTIAL_CATEGORY = Credential_Non_Dup.PSI_CREDENTIAL_CATEGORY
AND        Credential_Non_Dup_Programs_DACSO_FinalCIPs.OUTCOMES_CRED = Credential_Non_Dup.OUTCOMES_CRED
"
dbExecute(con, qry_update_Credential_Non_Dup_DACSO_Final_CIPs) 

# [UPDATE] Credential_Non_Dup

## qry_update_Credential_Non_Dup_BGS_Final_CIPs ----
qry_update_Credential_Non_Dup_BGS_Final_CIPs <- "
UPDATE Credential_Non_Dup
	SET		 FINAL_CIP_CODE_4 = Credential_Non_Dup_BGS_IDs.FINAL_CIP_CODE_4,
	       FINAL_CIP_CODE_4_NAME = Credential_Non_Dup_BGS_IDs.FINAL_CIP_CODE_4_NAME,
	       FINAL_CIP_CODE_2 = Credential_Non_Dup_BGS_IDs.FINAL_CIP_CODE_2,
				 FINAL_CIP_CODE_2_NAME = Credential_Non_Dup_BGS_IDs.FINAL_CIP_CODE_2_NAME,
	       FINAL_CIP_CLUSTER_CODE = Credential_Non_Dup_BGS_IDs.FINAL_CIP_CLUSTER_CODE,
	       FINAL_CIP_CLUSTER_NAME = Credential_Non_Dup_BGS_IDs.FINAL_CIP_CLUSTER_NAME
FROM   Credential_Non_Dup INNER JOIN Credential_Non_Dup_BGS_IDs
ON     Credential_Non_Dup.id = Credential_Non_Dup_BGS_IDs.id
WHERE  Credential_Non_Dup.OUTCOMES_CRED = 'BGS'"
dbExecute(con, qry_update_Credential_Non_Dup_BGS_Final_CIPs) 

# [UPDATE] Credential_Non_Dup

## qry_update_Credential_Non_Dup_GRAD_Final_CIPs ----
qry_update_Credential_Non_Dup_GRAD_Final_CIPs <- "
UPDATE Credential_Non_Dup
SET    FINAL_CIP_CODE_4 = Credential_Non_Dup_GRAD_IDs.FINAL_CIP_CODE_4,
       FINAL_CIP_CODE_4_NAME = Credential_Non_Dup_GRAD_IDs.FINAL_CIP_CODE_4_NAME,
       FINAL_CIP_CODE_2 = Credential_Non_Dup_GRAD_IDs.FINAL_CIP_CODE_2,
       FINAL_CIP_CODE_2_NAME = Credential_Non_Dup_GRAD_IDs.FINAL_CIP_CODE_2_NAME
FROM   Credential_Non_Dup_GRAD_IDs INNER JOIN Credential_Non_Dup
ON     Credential_Non_Dup_GRAD_IDs.id = Credential_Non_Dup.id"
dbExecute(con, qry_update_Credential_Non_Dup_GRAD_Final_CIPs) 

# [UPDATE] Credential_Non_Dup

## qry_update_Credential_Non_Dup_APPSO_Final_CIPs ----
qry_update_Credential_Non_Dup_APPSO_Final_CIPs <- "
UPDATE Credential_Non_Dup
SET    FINAL_CIP_CODE_4 = Credential_Non_Dup_APPSO_IDs.FINAL_CIP_CODE_4,
       FINAL_CIP_CODE_4_NAME = Credential_Non_Dup_APPSO_IDs.FINAL_CIP_CODE_4_NAME,
       FINAL_CIP_CODE_2 = Credential_Non_Dup_APPSO_IDs.FINAL_CIP_CODE_2,
       FINAL_CIP_CODE_2_NAME = Credential_Non_Dup_APPSO_IDs.FINAL_CIP_CODE_2_NAME
FROM   Credential_Non_Dup_APPSO_IDs INNER JOIN Credential_Non_Dup
ON     Credential_Non_Dup_APPSO_IDs.id = Credential_Non_Dup.id"
dbExecute(con, qry_update_Credential_Non_Dup_APPSO_Final_CIPs) 

# ---- update cluster codes for GRAD and APPSO (was left out of previous code)

# [UPDATE] Credential_Non_Dup


## qry_update final cluster codes for GRAD and APPSO data
qry_update_Credential_Non_Dup_GRAD_APPSO_Cluster <- "
UPDATE Credential_Non_Dup
SET    FINAL_CIP_CLUSTER_CODE = LCP2_LCIPPC_CD,
       FINAL_CIP_CLUSTER_NAME = LCP2_LCIPPC_NAME
FROM   Credential_Non_Dup INNER JOIN INFOWARE_L_CIP_2DIGITS_CIP2016
ON     FINAL_CIP_CODE_2 = LCP2_CD
WHERE OUTCOMES_CRED = 'GRAD' OR OUTCOMES_CRED = 'APPSO'
"
dbExecute(con, qry_update_Credential_Non_Dup_GRAD_APPSO_Cluster)

# ---- check for any leftover NULLs in the final cip 4 column
## checks 
{
  tbl(con, "Credential_Non_Dup") %>% filter(is.na(FINAL_CIP_CODE_4)) %>% count(outcomes_cred, FINAL_CIP_CODE_4)
}

# CLEAN UP NULLS ----
# Note: these are stored in separate sql script
# It would be good to merge the APPSO, GRAD, NULL work all into one, as it's all a repeat of the same process
# create cleaning table 

# [SELECT INTO] Create Credential_Non_Dup_STP_NULL_Cleaning from Credential_Non_Dup
# NULL IDS ----
##
## qry_NULL_STP_CIP_Cleaning ----
## collect STP NULL data
## this will grab any NULLs that are leftover in the final_4_cip_code column and try to match them on their STP codes
## New (from documentation): create table Credential_Non_Dup_STP_NULL_Cleaning for cleaning STP CIP codes
qry_NULL_STP_CIP_Cleaning <- "
SELECT PSI_CREDENTIAL_CIP,
       OUTCOMES_CRED,
       COUNT(*) AS Expr1
INTO   Credential_Non_Dup_STP_NULL_Cleaning
FROM   Credential_Non_Dup
WHERE final_cip_code_4 IS NULL
GROUP BY
       PSI_CREDENTIAL_CIP,
       OUTCOMES_CRED
"
dbExecute(con, qry_NULL_STP_CIP_Cleaning)

# add extra cols 

# [ALTER TABLE] Credential_Non_Dup_STP_NULL_Cleaning

# add columns
qry_NULL_STP_CIP_add_columns <- "
ALTER TABLE Credential_Non_Dup_STP_NULL_Cleaning
ADD STP_CIP_CODE_4 varchar (255),
STP_CIP_CODE_4_NAME varchar (255),
STP_CIP_CODE_2 varchar (255),
STP_CIP_CODE_2_NAME varchar (255),
STP_CIP_CLUSTER_CODE varchar(10),
STP_CIP_CLUSTER_NAME varchar(255),
PSI_CREDENTIAL_CIP_orig varchar (255)
"
dbExecute(con, qry_NULL_STP_CIP_add_columns)

# [UPDATE] Credential_Non_Dup_STP_NULL_Cleaning

qry_NULL_STP_CIP_update_original <- "
UPDATE Credential_Non_Dup_STP_NULL_Cleaning
SET PSI_CREDENTIAL_CIP_orig = PSI_CREDENTIAL_CIP"
dbExecute(con, qry_NULL_STP_CIP_update_original)

# clean CIPs to be correct format 

# [UPDATE] Credential_Non_Dup_STP_NULL_Cleaning

# clean CIPs that are wrong length
qry_NULL_STP_CIP_clean_cip_1 <- "
UPDATE Credential_Non_Dup_STP_NULL_Cleaning
SET    PSI_CREDENTIAL_CIP = CONCAT(PSI_CREDENTIAL_CIP, '0')
WHERE  LEN(PSI_CREDENTIAL_CIP) = 6 AND
substring(PSI_CREDENTIAL_CIP,1,2) NOT LIKE '%.'"
dbExecute(con, qry_NULL_STP_CIP_clean_cip_1)

# [UPDATE] Credential_Non_Dup_STP_NULL_Cleaning

qry_NULL_STP_CIP_clean_cip_2 <- "
UPDATE Credential_Non_Dup_STP_NULL_Cleaning
SET    PSI_CREDENTIAL_CIP = CONCAT('0', PSI_CREDENTIAL_CIP)
WHERE LEN(PSI_CREDENTIAL_CIP) = 6"
dbExecute(con, qry_NULL_STP_CIP_clean_cip_2)

## Update CIP 4 and 2D codes from INFOWARE, matching PSI_CREDENTIAL_CIP to LCIP_CD_WITH_PERIOD

# [UPDATE] Credential_Non_Dup_STP_NULL_Cleaning

## qry_Clean_NULL_STP_CIP_Step1_a ----
## Add 4 and 2D CIP codes from INFOWARE matching on PSI_CREDENTIAL_CIP
qry_Clean_NULL_STP_CIP_Step1_a <- "
UPDATE Credential_Non_Dup_STP_NULL_Cleaning
SET    Credential_Non_Dup_STP_NULL_Cleaning.STP_CIP_CODE_4 = [INFOWARE_L_CIP_6DIGITS_CIP2016].[LCIP_LCP4_CD],
       Credential_Non_Dup_STP_NULL_Cleaning.STP_CIP_CODE_2 = [INFOWARE_L_CIP_6DIGITS_CIP2016].[LCIP_LCP2_CD]
FROM   Credential_Non_Dup_STP_NULL_Cleaning INNER JOIN INFOWARE_L_CIP_6DIGITS_CIP2016
ON     Credential_Non_Dup_STP_NULL_Cleaning.PSI_CREDENTIAL_CIP = INFOWARE_L_CIP_6DIGITS_CIP2016.LCIP_CD_WITH_PERIOD"
dbExecute(con, qry_Clean_NULL_STP_CIP_Step1_a) # all 6 digits

# [UPDATE] Credential_Non_Dup_STP_NULL_Cleaning

## qry_Clean_NULL_STP_CIP_Step1_b ----
## New: Add 4 and 2D CIP codes from INFOWARE matching on first 4 digits of PSI_CREDENTIAL_CIP
qry_Clean_NULL_STP_CIP_Step1_b <- "
UPDATE Credential_Non_Dup_STP_NULL_Cleaning
SET    Credential_Non_Dup_STP_NULL_Cleaning.STP_CIP_CODE_4 = [INFOWARE_L_CIP_6DIGITS_CIP2016].[LCIP_LCP4_CD],
       Credential_Non_Dup_STP_NULL_Cleaning.STP_CIP_CODE_2 = [INFOWARE_L_CIP_6DIGITS_CIP2016].[LCIP_LCP2_CD]
FROM   Credential_Non_Dup_STP_NULL_Cleaning
INNER JOIN INFOWARE_L_CIP_6DIGITS_CIP2016
ON     substring(Credential_Non_Dup_STP_NULL_Cleaning.PSI_CREDENTIAL_CIP,1,5) = substring(INFOWARE_L_CIP_6DIGITS_CIP2016.LCIP_CD_WITH_PERIOD,1,5)
WHERE  STP_CIP_CODE_4 is NULL"
dbExecute(con, qry_Clean_NULL_STP_CIP_Step1_b) # first 4 digits

# [UPDATE] Credential_Non_Dup_STP_NULL_Cleaning

## qry_Clean_NULL_STP_CIP_Step2_c ----
## New: Add 4D CIP codes for general programs (if 00 change to 01)
## Check which CIPs have general programs here: https://www.statcan.gc.ca/en/subjects/standard/cip/2021/index
qry_Clean_NULL_STP_CIP_Step1_c <- "
UPDATE Credential_Non_Dup_STP_NULL_Cleaning
SET STP_CIP_CODE_4 = CONCAT(substring(Credential_Non_Dup_STP_NULL_Cleaning.PSI_CREDENTIAL_CIP,1,2), '01')
WHERE (substring(Credential_Non_Dup_STP_NULL_Cleaning.PSI_CREDENTIAL_CIP,1,5) = 11.00 OR
      substring(Credential_Non_Dup_STP_NULL_Cleaning.PSI_CREDENTIAL_CIP,1,5) = 13.00 OR
      substring(Credential_Non_Dup_STP_NULL_Cleaning.PSI_CREDENTIAL_CIP,1,5) = 14.00 OR
      substring(Credential_Non_Dup_STP_NULL_Cleaning.PSI_CREDENTIAL_CIP,1,5) = 19.00 OR
      substring(Credential_Non_Dup_STP_NULL_Cleaning.PSI_CREDENTIAL_CIP,1,5) = 23.00 OR
      substring(Credential_Non_Dup_STP_NULL_Cleaning.PSI_CREDENTIAL_CIP,1,5) = 24.00 OR
      substring(Credential_Non_Dup_STP_NULL_Cleaning.PSI_CREDENTIAL_CIP,1,5) = 26.00 OR
      substring(Credential_Non_Dup_STP_NULL_Cleaning.PSI_CREDENTIAL_CIP,1,5) = 40.00 OR
      substring(Credential_Non_Dup_STP_NULL_Cleaning.PSI_CREDENTIAL_CIP,1,5) = 42.00 OR
      substring(Credential_Non_Dup_STP_NULL_Cleaning.PSI_CREDENTIAL_CIP,1,5) = 45.00 OR
      substring(Credential_Non_Dup_STP_NULL_Cleaning.PSI_CREDENTIAL_CIP,1,5) = 50.00 OR
      substring(Credential_Non_Dup_STP_NULL_Cleaning.PSI_CREDENTIAL_CIP,1,5) = 52.00 OR
      substring(Credential_Non_Dup_STP_NULL_Cleaning.PSI_CREDENTIAL_CIP,1,5) = 55.00) AND
      STP_CIP_CODE_4 is NULL"
dbExecute(con, qry_Clean_NULL_STP_CIP_Step1_c) # recode general program CIPs from 00 ending to 01 ending

# [UPDATE] Credential_Non_Dup_STP_NULL_Cleaning

## qry_Clean_NULL_STP_CIP_Step1_d ----
## New: Add 2D CIP codes from INFOWARE matching on first 2 digits of PSI_CREDENTIAL_CIP
qry_Clean_NULL_STP_CIP_Step1_d <- "
UPDATE Credential_Non_Dup_STP_NULL_Cleaning
SET    Credential_Non_Dup_STP_NULL_Cleaning.STP_CIP_CODE_2 = [INFOWARE_L_CIP_6DIGITS_CIP2016].[LCIP_LCP2_CD]
FROM   Credential_Non_Dup_STP_NULL_Cleaning INNER JOIN INFOWARE_L_CIP_6DIGITS_CIP2016
ON     substring(Credential_Non_Dup_STP_NULL_Cleaning.PSI_CREDENTIAL_CIP,1,2) = substring(INFOWARE_L_CIP_6DIGITS_CIP2016.LCIP_CD_WITH_PERIOD,1,2)
WHERE  STP_CIP_CODE_2 is NULL"
dbExecute(con, qry_Clean_NULL_STP_CIP_Step1_d) # match first 2 digits

# [UPDATE] Credential_Non_Dup_STP_NULL_Cleaning

## qry_Clean_NULL_STP_CIP_Step2 ----
# Add 4D names
qry_Clean_NULL_STP_CIP_Step2 <- "
UPDATE Credential_Non_Dup_STP_NULL_Cleaning
SET    Credential_Non_Dup_STP_NULL_Cleaning.STP_CIP_CODE_4_NAME = [INFOWARE_L_CIP_4DIGITS_CIP2016].[LCP4_CIP_4DIGITS_NAME]
FROM   Credential_Non_Dup_STP_NULL_Cleaning INNER JOIN INFOWARE_L_CIP_4DIGITS_CIP2016
ON     Credential_Non_Dup_STP_NULL_Cleaning.STP_CIP_CODE_4 = INFOWARE_L_CIP_4DIGITS_CIP2016.LCP4_CD"
dbExecute(con, qry_Clean_NULL_STP_CIP_Step2) # add CIP 4D names

# [UPDATE] Credential_Non_Dup_STP_NULL_Cleaning

## qry_Clean_NULL_STP_CIP_Step3 ----
# Add 2D names
qry_Clean_NULL_STP_CIP_Step3 <- "
UPDATE Credential_Non_Dup_STP_NULL_Cleaning
SET    Credential_Non_Dup_STP_NULL_Cleaning.STP_CIP_CODE_2_NAME = [INFOWARE_L_CIP_2DIGITS_CIP2016].[LCP2_DIGITS_NAME],
       Credential_Non_Dup_STP_NULL_Cleaning.STP_CIP_CLUSTER_CODE = LCP2_LCIPPC_CD,
       Credential_Non_Dup_STP_NULL_Cleaning.STP_CIP_CLUSTER_NAME = LCP2_LCIPPC_NAME
FROM   Credential_Non_Dup_STP_NULL_Cleaning
INNER JOIN INFOWARE_L_CIP_2DIGITS_CIP2016
ON     Credential_Non_Dup_STP_NULL_Cleaning.STP_CIP_CODE_2 = INFOWARE_L_CIP_2DIGITS_CIP2016.LCP2_CD"
dbExecute(con, qry_Clean_NULL_STP_CIP_Step3) # add CIP 2D names

# [UPDATE] Credential_Non_Dup_STP_NULL_Cleaning

## qry_Clean_NULL_STP_CIP_step4 ----
## New: Set blank 4D names to Invalid 4-digit CIP
qry_Clean_NULL_STP_CIP_step4 <- "
UPDATE Credential_Non_Dup_STP_NULL_Cleaning
SET    Credential_Non_Dup_STP_NULL_Cleaning.STP_CIP_CODE_4_NAME = 'Invalid 4-digit CIP'
WHERE Credential_Non_Dup_STP_NULL_Cleaning.STP_CIP_CODE_4_NAME is NULL"
dbExecute(con, qry_Clean_NULL_STP_CIP_step4) # mark “Invalid 4-digit CIP” for remaining blank 4D names

# [SELECT INTO] Create Credential_Non_Dup_NULL_IDs from Credential_Non_Dup

## qry_Update_Credential_with_STP_CIP_NULL ----
## Update STP columns in Credential_Non_Dup and filter on NULL credentials
qry_Update_Credential_with_STP_CIP_NULL <-"
Select Credential_Non_Dup.ID,
       Credential_Non_Dup.PSI_CODE,
       Credential_Non_Dup.PSI_PROGRAM_CODE,
       Credential_Non_Dup.PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
       Credential_Non_Dup.PSI_CREDENTIAL_CIP,
       Credential_Non_Dup.PSI_AWARD_SCHOOL_YEAR,
       Credential_Non_Dup.OUTCOMES_CRED,
       FINAL_CIP_CODE_4 = Credential_Non_Dup_STP_NULL_Cleaning.STP_CIP_CODE_4,
       FINAL_CIP_CODE_4_NAME = Credential_Non_Dup_STP_NULL_Cleaning.STP_CIP_CODE_4_NAME,
       FINAL_CIP_CODE_2 = Credential_Non_Dup_STP_NULL_Cleaning.STP_CIP_CODE_2,
       FINAL_CIP_CODE_2_NAME = Credential_Non_Dup_STP_NULL_Cleaning.STP_CIP_CODE_2_NAME,
       FINAL_CIP_CLUSTER_CODE = Credential_Non_Dup_STP_NULL_Cleaning.STP_CIP_CLUSTER_CODE,
       FINAL_CIP_CLUSTER_NAME = Credential_Non_Dup_STP_NULL_Cleaning.STP_CIP_CLUSTER_NAME
INTO   Credential_Non_Dup_NULL_IDs
FROM   Credential_Non_Dup INNER JOIN Credential_Non_Dup_STP_NULL_Cleaning
ON     Credential_Non_Dup.PSI_CREDENTIAL_CIP = Credential_Non_Dup_STP_NULL_Cleaning.PSI_CREDENTIAL_CIP_orig AND
       Credential_Non_Dup.OUTCOMES_CRED = Credential_Non_Dup_STP_NULL_Cleaning.OUTCOMES_CRED
WHERE  Credential_Non_Dup.final_cip_code_4 is NULL"
dbExecute(con, qry_Update_Credential_with_STP_CIP_NULL) # create ID list

# [UPDATE] Credential_Non_Dup_NULL_IDs

## replace unspecified with NULL
qry_Update_Credential_with_STP_CIP_NULL_nulls <- "
Update Credential_Non_Dup_NULL_IDs
SET PSI_PROGRAM_CODE = NULL
WHERE PSI_PROGRAM_CODE = '(Unspecified)'
"
dbExecute(con, qry_Update_Credential_with_STP_CIP_NULL_nulls) # in 2023 only PSI_PROGRAM_CODE had (Unspecified) - replace with NULLs

# update the final NULL CIPs

# [UPDATE] Credential_Non_Dup

## qry_update_Credential_Non_Dup_NULL_Final_CIPs ----
qry_update_Credential_Non_Dup_NULL_Final_CIPs <- "
UPDATE Credential_Non_Dup
SET    FINAL_CIP_CODE_4 = Credential_Non_Dup_NULL_IDs.FINAL_CIP_CODE_4,
       FINAL_CIP_CODE_4_NAME = Credential_Non_Dup_NULL_IDs.FINAL_CIP_CODE_4_NAME,
       FINAL_CIP_CODE_2 = Credential_Non_Dup_NULL_IDs.FINAL_CIP_CODE_2,
       FINAL_CIP_CODE_2_NAME = Credential_Non_Dup_NULL_IDs.FINAL_CIP_CODE_2_NAME
       FINAL_CIP_CLUSTER_CODE = Credential_Non_Dup_NULL_IDs.FINAL_CIP_CLUSTER_CODE,
       FINAL_CIP_CLUSTER_NAME = Credential_Non_Dup_NULL_IDs.FINAL_CIP_CLUSTER_NAME
FROM   Credential_Non_Dup_NULL_IDs INNER JOIN Credential_Non_Dup
ON     Credential_Non_Dup_NULL_IDs.id = Credential_Non_Dup.id"
dbExecute(con, qry_update_Credential_Non_Dup_NULL_Final_CIPs) 

## checks 
{
  tbl(con, "Credential_Non_Dup") %>% filter(is.na(FINAL_CIP_CODE_4)) %>% count(outcomes_cred, FINAL_CIP_CODE_4)
}

# ---- clean up queries, clean up 'undeclared activities' ----

# [UPDATE] Credential_Non_Dup


# ---- SQLQuery4 ----
## update some 99 codes for BGS
SQLQuery4 <- "
UPDATE    Credential_Non_Dup
SET       FINAL_CIP_CODE_2_NAME = 'Undeclared activity',
          FINAL_CIP_CLUSTER_CODE = '99',
          FINAL_CIP_CLUSTER_NAME = 'Undeclared activity'
WHERE     (OUTCOMES_CRED = 'BGS') AND (FINAL_CIP_CODE_2 = '99');"
dbExecute(con, SQLQuery4)

# [UPDATE] Credential_Non_Dup

# ---- SQLQuery6 ----
## update final to be STP if final was missing
SQLQuery6 <- "
UPDATE    Credential_Non_Dup
SET       FINAL_CIP_CODE_4 = STP_CIP_CODE_4,
          FINAL_CIP_CODE_4_NAME = STP_CIP_CODE_4_NAME,
          FINAL_CIP_CODE_2 = STP_CIP_CODE_2,
          FINAL_CIP_CODE_2_NAME = STP_CIP_CODE_2_NAME
WHERE     (FINAL_CIP_CODE_4 IS NULL) OR
                      (FINAL_CIP_CODE_4 = ' ');"
dbExecute(con, SQLQuery6)

# [UPDATE] Credential_Non_Dup

# ---- SQLQuery7 ----
## update some 99 codes to align
SQLQuery7 <- "
UPDATE    Credential_Non_Dup
SET       FINAL_CIP_CLUSTER_CODE = '99',
          FINAL_CIP_CLUSTER_NAME = 'Undeclared activity'
WHERE     (OUTCOMES_CRED = 'GRAD')
AND (FINAL_CIP_CLUSTER_CODE IS NULL)
AND (FINAL_CIP_CLUSTER_NAME IS NULL)
AND (FINAL_CIP_CODE_2 = '99');"
dbExecute(con, SQLQuery7)

# ---- Clean up ----
dbExecute(con, "DROP TABLE Credential_Non_Dup_STP_NULL_Cleaning")
dbDisconnect(con)