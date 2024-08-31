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

qry_NULL_STP_CIP_update_original <- "
UPDATE Credential_Non_Dup_STP_NULL_Cleaning
SET PSI_CREDENTIAL_CIP_orig = PSI_CREDENTIAL_CIP"

# clean CIPs that are wrong length
qry_NULL_STP_CIP_clean_cip_1 <- "
UPDATE Credential_Non_Dup_STP_NULL_Cleaning
SET    PSI_CREDENTIAL_CIP = CONCAT(PSI_CREDENTIAL_CIP, '0')
WHERE  LEN(PSI_CREDENTIAL_CIP) = 6 AND
substring(PSI_CREDENTIAL_CIP,1,2) NOT LIKE '%.'"

qry_NULL_STP_CIP_clean_cip_2 <- "
UPDATE Credential_Non_Dup_STP_NULL_Cleaning
SET    PSI_CREDENTIAL_CIP = CONCAT('0', PSI_CREDENTIAL_CIP)
WHERE LEN(PSI_CREDENTIAL_CIP) = 6"

## qry_Clean_NULL_STP_CIP_Step1_a ----
## Add 4 and 2D CIP codes from INFOWARE matching on PSI_CREDENTIAL_CIP
qry_Clean_NULL_STP_CIP_Step1_a <- "
UPDATE Credential_Non_Dup_STP_NULL_Cleaning
SET    Credential_Non_Dup_STP_NULL_Cleaning.STP_CIP_CODE_4 = [INFOWARE_L_CIP_6DIGITS_CIP2016].[LCIP_LCP4_CD],
       Credential_Non_Dup_STP_NULL_Cleaning.STP_CIP_CODE_2 = [INFOWARE_L_CIP_6DIGITS_CIP2016].[LCIP_LCP2_CD]
FROM   Credential_Non_Dup_STP_NULL_Cleaning INNER JOIN INFOWARE_L_CIP_6DIGITS_CIP2016 
ON     Credential_Non_Dup_STP_NULL_Cleaning.PSI_CREDENTIAL_CIP = INFOWARE_L_CIP_6DIGITS_CIP2016.LCIP_CD_WITH_PERIOD"

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

## qry_Clean_NULL_STP_CIP_Step1_d ----
## New: Add 2D CIP codes from INFOWARE matching on first 2 digits of PSI_CREDENTIAL_CIP
qry_Clean_NULL_STP_CIP_Step1_d <- "
UPDATE Credential_Non_Dup_STP_NULL_Cleaning
SET    Credential_Non_Dup_STP_NULL_Cleaning.STP_CIP_CODE_2 = [INFOWARE_L_CIP_6DIGITS_CIP2016].[LCIP_LCP2_CD]
FROM   Credential_Non_Dup_STP_NULL_Cleaning INNER JOIN INFOWARE_L_CIP_6DIGITS_CIP2016 
ON     substring(Credential_Non_Dup_STP_NULL_Cleaning.PSI_CREDENTIAL_CIP,1,2) = substring(INFOWARE_L_CIP_6DIGITS_CIP2016.LCIP_CD_WITH_PERIOD,1,2)
WHERE  STP_CIP_CODE_2 is NULL"

## qry_Clean_NULL_STP_CIP_Step2 ----
# Add 4D names
qry_Clean_NULL_STP_CIP_Step2 <- "
UPDATE Credential_Non_Dup_STP_NULL_Cleaning 
SET    Credential_Non_Dup_STP_NULL_Cleaning.STP_CIP_CODE_4_NAME = [INFOWARE_L_CIP_4DIGITS_CIP2016].[LCP4_CIP_4DIGITS_NAME]
FROM   Credential_Non_Dup_STP_NULL_Cleaning INNER JOIN INFOWARE_L_CIP_4DIGITS_CIP2016 
ON     Credential_Non_Dup_STP_NULL_Cleaning.STP_CIP_CODE_4 = INFOWARE_L_CIP_4DIGITS_CIP2016.LCP4_CD"

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

## qry_Clean_NULL_STP_CIP_step4 ----
## New: Set blank 4D names to Invalid 4-digit CIP
qry_Clean_NULL_STP_CIP_step4 <- "
UPDATE Credential_Non_Dup_STP_NULL_Cleaning
SET    Credential_Non_Dup_STP_NULL_Cleaning.STP_CIP_CODE_4_NAME = 'Invalid 4-digit CIP'
WHERE Credential_Non_Dup_STP_NULL_Cleaning.STP_CIP_CODE_4_NAME is NULL"

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

## replace unspecified with NULL
qry_Update_Credential_with_STP_CIP_NULL_nulls <- "
Update Credential_Non_Dup_NULL_IDs
SET PSI_PROGRAM_CODE = NULL
WHERE PSI_PROGRAM_CODE = '(Unspecified)'
"

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



