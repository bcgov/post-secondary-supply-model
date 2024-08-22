## use ALT + O to collapse all sections and ALT+SHIFT+O to expand all sections

# PART 1: BUILD OUTCOMES DATA ----

## qry_Make_T_BGS_Data_for_OutcomesMatching_step1 ----
## Create 2020 outcomes matching table from student outcomes BGS data
## Need 6 years of data, so use last two survey data tables (INFOWARE_BGS_DIST_XX_XX)
## Join the INFOWARE_BGS_COHORT_INFO to get additional info for individuals in those cohorts
qry_Make_T_BGS_Data_for_OutcomesMatching_step1 <- "
SELECT INFOWARE_BGS_COHORT_INFO.PEN, 
        INFOWARE_BGS_COHORT_INFO.STUDID, 
        INFOWARE_BGS_DIST_19_23.STQU_ID, 
        INFOWARE_BGS_COHORT_INFO.SRV_Y_N,
        INFOWARE_BGS_DIST_19_23.RESPONDENT,
        INFOWARE_BGS_DIST_19_23.Year,
        INFOWARE_BGS_COHORT_INFO.SUBM_CD,
        INFOWARE_BGS_DIST_19_23.INSTITUTION_CODE,
        INFOWARE_BGS_DIST_19_23.INSTITUTION, 
        INFOWARE_BGS_COHORT_INFO.CIP2DIG,
        INFOWARE_BGS_COHORT_INFO.CIP2DIG_NAME,
        INFOWARE_BGS_COHORT_INFO.CIP4DIG, 
        INFOWARE_BGS_COHORT_INFO.CIP_4DIGIT_NO_PERIOD, 
        INFOWARE_BGS_COHORT_INFO.CIP4DIG_NAME, 
        INFOWARE_BGS_COHORT_INFO.CIP_6DIGIT_1,
        INFOWARE_BGS_COHORT_INFO.CIP_6DIGIT_NO_PERIOD, 
        INFOWARE_BGS_COHORT_INFO.CIP6DIG_NAME,
        INFOWARE_BGS_COHORT_INFO.PROGRAM,
        INFOWARE_BGS_COHORT_INFO.DASHBOARD_PROGRAM, 
        INFOWARE_BGS_COHORT_INFO.CPC 
INTO    T_BGS_Data_Final_for_OutcomesMatching
FROM    INFOWARE_BGS_DIST_19_23 INNER JOIN INFOWARE_BGS_COHORT_INFO 
ON      INFOWARE_BGS_DIST_19_23.STQU_ID = INFOWARE_BGS_COHORT_INFO.STQU_ID"

## qry_Make_T_BGS_Data_for_OutcomesMatching_step2 ----
qry_Make_T_BGS_Data_for_OutcomesMatching_step2 <- "
INSERT INTO T_BGS_Data_Final_for_OutcomesMatching ( 
            PEN, STUDID, STQU_ID, SRV_Y_N, RESPONDENT, [Year], SUBM_CD, INSTITUTION_CODE,
            INSTITUTION, CIP2DIG, CIP2DIG_NAME, CIP4DIG, CIP_4DIGIT_NO_PERIOD, CIP4DIG_NAME, 
            CIP_6DIGIT_1, CIP_6DIGIT_NO_PERIOD, CIP6DIG_NAME, PROGRAM, DASHBOARD_PROGRAM, CPC
            )
SELECT INFOWARE_BGS_COHORT_INFO.PEN, 
       INFOWARE_BGS_COHORT_INFO.STUDID, 
       INFOWARE_BGS_DIST_18_22.STQU_ID,
       INFOWARE_BGS_COHORT_INFO.SRV_Y_N,
       INFOWARE_BGS_DIST_18_22.RESPONDENT,
       INFOWARE_BGS_DIST_18_22.Year,
       INFOWARE_BGS_COHORT_INFO.SUBM_CD,
       INFOWARE_BGS_DIST_18_22.INSTITUTION_CODE, 
       INFOWARE_BGS_DIST_18_22.INSTITUTION,
       INFOWARE_BGS_COHORT_INFO.CIP2DIG,
       INFOWARE_BGS_COHORT_INFO.CIP2DIG_NAME, 
       INFOWARE_BGS_COHORT_INFO.CIP4DIG,
       INFOWARE_BGS_COHORT_INFO.CIP_4DIGIT_NO_PERIOD, 
       INFOWARE_BGS_COHORT_INFO.CIP4DIG_NAME,
       INFOWARE_BGS_COHORT_INFO.CIP_6DIGIT_1,
       INFOWARE_BGS_COHORT_INFO.CIP_6DIGIT_NO_PERIOD,
       INFOWARE_BGS_COHORT_INFO.CIP6DIG_NAME,
       INFOWARE_BGS_COHORT_INFO.PROGRAM, 
       INFOWARE_BGS_COHORT_INFO.DASHBOARD_PROGRAM, 
       INFOWARE_BGS_COHORT_INFO.CPC
FROM   INFOWARE_BGS_DIST_18_22 INNER JOIN INFOWARE_BGS_COHORT_INFO 
ON     INFOWARE_BGS_DIST_18_22.STQU_ID = INFOWARE_BGS_COHORT_INFO.STQU_ID
WHERE  INFOWARE_BGS_DIST_18_22.Year = 2018"

## qry_Add_PSSM_CREDENTIAL ----
# New: Add column PSSM_CREDENTIAL and set to BACH
qry_Add_PSSM_CREDENTIAL <- "
ALTER TABLE T_BGS_Data_Final_for_OutcomesMatching
ADD PSSM_CREDENTIAL VARCHAR(255) NOT NULL DEFAULT 'BACH'"
# qry_Update_PSSM_CREDENTIAL <- "
# UPDATE T_BGS_Data_Final_for_OutcomesMatching 
# SET    T_BGS_Data_Final_for_OutcomesMatching.PSSM_CREDENTIAL = 'BACH'"

## qry_Check_BGS_CIP_Data ----
## Updated this to current table year (OutcomesMatching2020 from PSSM2019)
qry_Check_BGS_CIP_Data <- "
SELECT T_BGS_Data_Final_for_OutcomesMatching.CIP2DIG,
       T_BGS_Data_Final_for_OutcomesMatching.CIP2DIG_NAME, 
       T_BGS_Data_Final_for_OutcomesMatching.CIP4DIG,
       T_BGS_Data_Final_for_OutcomesMatching.CIP4DIG_NAME,
       T_BGS_Data_Final_for_OutcomesMatching.CIP_6DIGIT_NO_PERIOD,
       T_BGS_Data_Final_for_OutcomesMatching.CIP6DIG_NAME, 
       Count(*) AS N
FROM   T_BGS_Data_Final_for_OutcomesMatching
GROUP BY 
       T_BGS_Data_Final_for_OutcomesMatching.CIP2DIG,
       T_BGS_Data_Final_for_OutcomesMatching.CIP2DIG_NAME, 
       T_BGS_Data_Final_for_OutcomesMatching.CIP4DIG, 
       T_BGS_Data_Final_for_OutcomesMatching.CIP4DIG_NAME, 
       T_BGS_Data_Final_for_OutcomesMatching.CIP_6DIGIT_NO_PERIOD,
       T_BGS_Data_Final_for_OutcomesMatching.CIP6DIG_NAME"

# PART 2: CLEAN CREDENTIAL CIP ----
## Update CIP codes in STP data
## Working with Credential_Non_Dup_STP_CIP4_Cleaning

## qry_BGS_STP_CIP_Cleaning ----
## collect STP BGS data
## New (from documentation): create table Credential_Non_Dup_STP_CIP4_Cleaning for cleaning STP CIP codes
qry_BGS_STP_CIP_Cleaning <- "
SELECT PSI_CREDENTIAL_CIP, 
       OUTCOMES_CRED,
       COUNT(*) AS Expr1
INTO   Credential_Non_Dup_STP_CIP4_Cleaning
FROM   Credential_Non_Dup
GROUP BY 
       PSI_CREDENTIAL_CIP, 
       OUTCOMES_CRED
HAVING OUTCOMES_CRED = 'BGS' OR
       OUTCOMES_CRED = 'GRAD'"

## qry_Clean_BGS_STP_CIP_Step1_a ----
## Add 4 and 2D CIP codes from INFOWARE matching on PSI_CREDENTIAL_CIP
qry_Clean_BGS_STP_CIP_Step1_a <- "
UPDATE Credential_Non_Dup_STP_CIP4_Cleaning
SET    Credential_Non_Dup_STP_CIP4_Cleaning.STP_CIP_CODE_4 = [INFOWARE_L_CIP_6DIGITS_CIP2016].[LCIP_LCP4_CD],
       Credential_Non_Dup_STP_CIP4_Cleaning.STP_CIP_CODE_2 = [INFOWARE_L_CIP_6DIGITS_CIP2016].[LCIP_LCP2_CD]
FROM   Credential_Non_Dup_STP_CIP4_Cleaning INNER JOIN INFOWARE_L_CIP_6DIGITS_CIP2016 
ON     Credential_Non_Dup_STP_CIP4_Cleaning.PSI_CREDENTIAL_CIP = INFOWARE_L_CIP_6DIGITS_CIP2016.LCIP_CD_WITH_PERIOD"

## qry_Clean_BGS_STP_CIP_Step1_b ----
## New: Add 4 and 2D CIP codes from INFOWARE matching on first 4 digits of PSI_CREDENTIAL_CIP
qry_Clean_BGS_STP_CIP_Step1_b <- "
UPDATE Credential_Non_Dup_STP_CIP4_Cleaning
SET    Credential_Non_Dup_STP_CIP4_Cleaning.STP_CIP_CODE_4 = [INFOWARE_L_CIP_6DIGITS_CIP2016].[LCIP_LCP4_CD],
       Credential_Non_Dup_STP_CIP4_Cleaning.STP_CIP_CODE_2 = [INFOWARE_L_CIP_6DIGITS_CIP2016].[LCIP_LCP2_CD]
FROM   Credential_Non_Dup_STP_CIP4_Cleaning INNER JOIN INFOWARE_L_CIP_6DIGITS_CIP2016 
ON     substring(Credential_Non_Dup_STP_CIP4_Cleaning.PSI_CREDENTIAL_CIP,1,5) = substring(INFOWARE_L_CIP_6DIGITS_CIP2016.LCIP_CD_WITH_PERIOD,1,5)
WHERE  STP_CIP_CODE_4 is NULL"

## qry_Clean_BGS_STP_CIP_Step2_c ----
## New: Add 4D CIP codes for general programs (if 00 change to 01)
## Check which CIPs have general programs here: https://www.statcan.gc.ca/en/subjects/standard/cip/2021/index
qry_Clean_BGS_STP_CIP_Step1_c <- "
UPDATE Credential_Non_Dup_STP_CIP4_Cleaning
SET STP_CIP_CODE_4 = CONCAT(substring(Credential_Non_Dup_STP_CIP4_Cleaning.PSI_CREDENTIAL_CIP,1,2), '01')
WHERE (substring(Credential_Non_Dup_STP_CIP4_Cleaning.PSI_CREDENTIAL_CIP,1,5) = 11.00 OR
      substring(Credential_Non_Dup_STP_CIP4_Cleaning.PSI_CREDENTIAL_CIP,1,5) = 13.00 OR
      substring(Credential_Non_Dup_STP_CIP4_Cleaning.PSI_CREDENTIAL_CIP,1,5) = 14.00 OR
      substring(Credential_Non_Dup_STP_CIP4_Cleaning.PSI_CREDENTIAL_CIP,1,5) = 19.00 OR
      substring(Credential_Non_Dup_STP_CIP4_Cleaning.PSI_CREDENTIAL_CIP,1,5) = 23.00 OR
      substring(Credential_Non_Dup_STP_CIP4_Cleaning.PSI_CREDENTIAL_CIP,1,5) = 24.00 OR
      substring(Credential_Non_Dup_STP_CIP4_Cleaning.PSI_CREDENTIAL_CIP,1,5) = 26.00 OR
      substring(Credential_Non_Dup_STP_CIP4_Cleaning.PSI_CREDENTIAL_CIP,1,5) = 40.00 OR
      substring(Credential_Non_Dup_STP_CIP4_Cleaning.PSI_CREDENTIAL_CIP,1,5) = 42.00 OR
      substring(Credential_Non_Dup_STP_CIP4_Cleaning.PSI_CREDENTIAL_CIP,1,5) = 45.00 OR
      substring(Credential_Non_Dup_STP_CIP4_Cleaning.PSI_CREDENTIAL_CIP,1,5) = 50.00 OR
      substring(Credential_Non_Dup_STP_CIP4_Cleaning.PSI_CREDENTIAL_CIP,1,5) = 52.00 OR
      substring(Credential_Non_Dup_STP_CIP4_Cleaning.PSI_CREDENTIAL_CIP,1,5) = 55.00) AND
      STP_CIP_CODE_4 is NULL"

## qry_Clean_BGS_STP_CIP_Step1_d ----
## New: Add 2D CIP codes from INFOWARE matching on first 2 digits of PSI_CREDENTIAL_CIP
qry_Clean_BGS_STP_CIP_Step1_d <- "
UPDATE Credential_Non_Dup_STP_CIP4_Cleaning
SET    Credential_Non_Dup_STP_CIP4_Cleaning.STP_CIP_CODE_2 = [INFOWARE_L_CIP_6DIGITS_CIP2016].[LCIP_LCP2_CD]
FROM   Credential_Non_Dup_STP_CIP4_Cleaning INNER JOIN INFOWARE_L_CIP_6DIGITS_CIP2016 
ON     substring(Credential_Non_Dup_STP_CIP4_Cleaning.PSI_CREDENTIAL_CIP,1,2) = substring(INFOWARE_L_CIP_6DIGITS_CIP2016.LCIP_CD_WITH_PERIOD,1,2)
WHERE  STP_CIP_CODE_2 is NULL"

## qry_Clean_BGS_STP_CIP_Step2 ----
# Add 4D names
qry_Clean_BGS_STP_CIP_Step2 <- "
UPDATE Credential_Non_Dup_STP_CIP4_Cleaning 
SET    Credential_Non_Dup_STP_CIP4_Cleaning.STP_CIP_CODE_4_NAME = [INFOWARE_L_CIP_4DIGITS_CIP2016].[LCP4_CIP_4DIGITS_NAME]
FROM   Credential_Non_Dup_STP_CIP4_Cleaning INNER JOIN INFOWARE_L_CIP_4DIGITS_CIP2016 
ON     Credential_Non_Dup_STP_CIP4_Cleaning.STP_CIP_CODE_4 = INFOWARE_L_CIP_4DIGITS_CIP2016.LCP4_CD"

## qry_Clean_BGS_STP_CIP_Step3 ----
# Add 2D names
qry_Clean_BGS_STP_CIP_Step3 <- "
UPDATE Credential_Non_Dup_STP_CIP4_Cleaning
SET    Credential_Non_Dup_STP_CIP4_Cleaning.STP_CIP_CODE_2_NAME = [INFOWARE_L_CIP_2DIGITS_CIP2016].[LCP2_DIGITS_NAME]
FROM   Credential_Non_Dup_STP_CIP4_Cleaning INNER JOIN INFOWARE_L_CIP_2DIGITS_CIP2016 
ON     Credential_Non_Dup_STP_CIP4_Cleaning.STP_CIP_CODE_2 = INFOWARE_L_CIP_2DIGITS_CIP2016.LCP2_CD"

## qry_Clean_BGS_STP_CIP_step4 ----
## New: Set blank 4D names to Invalid 4-digit CIP
qry_Clean_BGS_STP_CIP_step4 <- "
UPDATE Credential_Non_Dup_STP_CIP4_Cleaning
SET    Credential_Non_Dup_STP_CIP4_Cleaning.STP_CIP_CODE_4_NAME = 'Invalid 4-digit CIP'
WHERE Credential_Non_Dup_STP_CIP4_Cleaning.STP_CIP_CODE_4_NAME is NULL"

## qry_Update_Credential_with_STP_CIP_BGS ----
## Update STP columns in Credential_Non_Dup and filter on BGS credentials
qry_Update_Credential_with_STP_CIP_BGS <-"
Select Credential_Non_Dup.ID,
       Credential_Non_Dup.PSI_CODE,
       Credential_Non_Dup.PSI_PROGRAM_CODE,
       Credential_Non_Dup.PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
       Credential_Non_Dup.PSI_CREDENTIAL_CIP,
       Credential_Non_Dup.PSI_AWARD_SCHOOL_YEAR,
       Credential_Non_Dup.OUTCOMES_CRED,
       Credential_Non_Dup_STP_CIP4_Cleaning.STP_CIP_CODE_4, 
       Credential_Non_Dup_STP_CIP4_Cleaning.STP_CIP_CODE_4_NAME,
       Credential_Non_Dup_STP_CIP4_Cleaning.STP_CIP_CODE_2, 
       Credential_Non_Dup_STP_CIP4_Cleaning.STP_CIP_CODE_2_NAME
INTO   Credential_Non_Dup_BGS_IDs
FROM   Credential_Non_Dup INNER JOIN Credential_Non_Dup_STP_CIP4_Cleaning 
ON     Credential_Non_Dup.PSI_CREDENTIAL_CIP = Credential_Non_Dup_STP_CIP4_Cleaning.PSI_CREDENTIAL_CIP_orig AND 
       Credential_Non_Dup.OUTCOMES_CRED = Credential_Non_Dup_STP_CIP4_Cleaning.OUTCOMES_CRED
WHERE  Credential_Non_Dup.OUTCOMES_CRED = 'BGS'"

## qry_Update_Credential_with_STP_CIP_Grad ----
## Update STP columns in Credential_Non_Dup and filter on Grad credentials
qry_Update_Credential_with_STP_CIP_GRAD <-"
Select Credential_Non_Dup.ID,
       Credential_Non_Dup.PSI_CODE,
       Credential_Non_Dup.PSI_PROGRAM_CODE,
       Credential_Non_Dup.PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
       Credential_Non_Dup.PSI_CREDENTIAL_CIP,
       Credential_Non_Dup.PSI_AWARD_SCHOOL_YEAR,
       Credential_Non_Dup.OUTCOMES_CRED,
       FINAL_CIP_CODE_4 = Credential_Non_Dup_STP_CIP4_Cleaning.STP_CIP_CODE_4, 
       FINAL_CIP_CODE_4_NAME = Credential_Non_Dup_STP_CIP4_Cleaning.STP_CIP_CODE_4_NAME,
       FINAL_CIP_CODE_2 = Credential_Non_Dup_STP_CIP4_Cleaning.STP_CIP_CODE_2, 
       FINAL_CIP_CODE_2_NAME = Credential_Non_Dup_STP_CIP4_Cleaning.STP_CIP_CODE_2_NAME
INTO   Credential_Non_Dup_GRAD_IDs
FROM   Credential_Non_Dup INNER JOIN Credential_Non_Dup_STP_CIP4_Cleaning 
ON     Credential_Non_Dup.PSI_CREDENTIAL_CIP = Credential_Non_Dup_STP_CIP4_Cleaning.PSI_CREDENTIAL_CIP_orig AND 
       Credential_Non_Dup.OUTCOMES_CRED = Credential_Non_Dup_STP_CIP4_Cleaning.OUTCOMES_CRED
WHERE  Credential_Non_Dup.OUTCOMES_CRED = 'GRAD'"

# PART 3: BUILD CASE-LEVEL XWALK ----
## Create BGS_Matching_STP_Credential_PEN

## PART 3A ----
## qry_Add_PSI_PEN ----
## New (from documentation): Add PSI_PEN column from STP_Credential to Credential_Non_Dup
##  run only if PSI column missing from Credential_Non_Dup
qry_Add_PSI_PEN <- "ALTER TABLE Credential_Non_Dup_BGS_IDs ADD PSI_PEN varchar(255)"

qry_Update_PSI_PEN <- "
UPDATE Credential_Non_Dup_BGS_IDs 
SET    PSI_PEN = STP_Credential.PSI_PEN
FROM   Credential_Non_Dup_BGS_IDs JOIN STP_Credential 
ON     STP_Credential.ID = Credential_Non_Dup_BGS_IDs.id"

## qry01_Match_BGS_STP_Credential_on_PEN ----
## combine BGS And STP data into BGS_Matching_STP_Credential_PEN
qry01_Match_BGS_STP_Credential_on_PEN <- "
SELECT T_BGS_Data_Final_for_OutcomesMatching.stqu_id,
       Credential_Non_Dup_BGS_IDs.ID, 
       T_BGS_Data_Final_for_OutcomesMatching.PEN,
       Credential_Non_Dup_BGS_IDs.OUTCOMES_CRED,
       T_BGS_Data_Final_for_OutcomesMatching.INSTITUTION_CODE,
       Credential_Non_Dup_BGS_IDs.PSI_CODE,
       T_BGS_Data_Final_for_OutcomesMatching.YEAR, 
       Credential_Non_Dup_BGS_IDs.PSI_AWARD_SCHOOL_YEAR,       
       T_BGS_Data_Final_for_OutcomesMatching.CIP_4DIGIT_NO_PERIOD as BGS_FINAL_CIP_CODE_4,
       T_BGS_Data_Final_for_OutcomesMatching.CIP4DIG_NAME AS BGS_FINAL_CIP_CODE_4_NAME,        
       Credential_Non_Dup_BGS_IDs.STP_CIP_CODE_4 AS STP_FINAL_CIP_CODE_4,
       Credential_Non_Dup_BGS_IDs.STP_CIP_CODE_4_NAME AS STP_FINAL_CIP_CODE_4_NAME,       
       T_BGS_Data_Final_for_OutcomesMatching.CIP2DIG AS BGS_FINAL_CIP_CODE_2, 
       T_BGS_Data_Final_for_OutcomesMatching.CIP2DIG_NAME AS BGS_FINAL_CIP_CODE_2_NAME,        
       Credential_Non_Dup_BGS_IDs.STP_CIP_CODE_2 AS STP_FINAL_CIP_CODE_2,
       Credential_Non_Dup_BGS_IDs.STP_CIP_CODE_2_NAME AS STP_FINAL_CIP_CODE_2_NAME,
       T_BGS_Data_Final_for_OutcomesMatching.CPC AS BGS_PROGRAM_CODE,
       T_BGS_Data_Final_for_OutcomesMatching.PROGRAM AS BGS_PROGRAM_DESC,
       Credential_Non_Dup_BGS_IDs.PSI_PROGRAM_CODE AS STP_PROGRAM_CODE,
       Credential_Non_Dup_BGS_IDs.PSI_CREDENTIAL_PROGRAM_DESCRIPTION AS STP_PROGRAM_DESC
 INTO  BGS_Matching_STP_Credential_PEN
 FROM  T_BGS_Data_Final_for_OutcomesMatching INNER JOIN Credential_Non_Dup_BGS_IDs 
 ON    T_BGS_Data_Final_for_OutcomesMatching.PEN = Credential_Non_Dup_BGS_IDs.PSI_PEN
 GROUP BY 
       T_BGS_Data_Final_for_OutcomesMatching.stqu_id,
       Credential_Non_Dup_BGS_IDs.ID, 
       T_BGS_Data_Final_for_OutcomesMatching.PEN,
       Credential_Non_Dup_BGS_IDs.OUTCOMES_CRED,
       T_BGS_Data_Final_for_OutcomesMatching.INSTITUTION_CODE,
       Credential_Non_Dup_BGS_IDs.PSI_CODE,
       T_BGS_Data_Final_for_OutcomesMatching.YEAR, 
       Credential_Non_Dup_BGS_IDs.PSI_AWARD_SCHOOL_YEAR,       
       T_BGS_Data_Final_for_OutcomesMatching.CIP_4DIGIT_NO_PERIOD,
       T_BGS_Data_Final_for_OutcomesMatching.CIP4DIG_NAME,        
       Credential_Non_Dup_BGS_IDs.STP_CIP_CODE_4,
       Credential_Non_Dup_BGS_IDs.STP_CIP_CODE_4_NAME,       
       T_BGS_Data_Final_for_OutcomesMatching.CIP2DIG, 
       T_BGS_Data_Final_for_OutcomesMatching.CIP2DIG_NAME,        
       Credential_Non_Dup_BGS_IDs.STP_CIP_CODE_2,
       Credential_Non_Dup_BGS_IDs.STP_CIP_CODE_2_NAME,
       T_BGS_Data_Final_for_OutcomesMatching.CPC,
       T_BGS_Data_Final_for_OutcomesMatching.PROGRAM,
       Credential_Non_Dup_BGS_IDs.PSI_PROGRAM_CODE,
       Credential_Non_Dup_BGS_IDs.PSI_CREDENTIAL_PROGRAM_DESCRIPTION
HAVING T_BGS_Data_Final_for_OutcomesMatching.PEN <> '' AND
       T_BGS_Data_Final_for_OutcomesMatching.PEN IS NOT NULL AND
       T_BGS_Data_Final_for_OutcomesMatching.PEN <> '0' AND 
       Credential_Non_Dup_BGS_IDs.OUTCOMES_CRED='BGS'"

## qry01b_Match_BGS_STP_Credential_Add_Cols ----
## add blank columns
qry01b_Match_BGS_STP_Credential_Add_Cols <- "
Alter Table BGS_Matching_STP_Credential_PEN 
ADD
	Match_Inst [varchar](50)NULL,
	Match_Award_School_Year [varchar](50)NULL,
	Match_CIP_CODE_4 [varchar](50)NULL,
	Match_All_3_CIP4_Flag [varchar](50)NULL,
	Match_CIP_CODE_2[varchar](50)NULL,
	Match_All_3_CIP2_Flag [varchar](50)NULL,
	Final_Consider_A_Match [varchar](50) NULL,
	Final_Probable_Match [varchar](50) NULL"



## qry_add_empty_final_CIP ----
qry_add_empty_final_CIP <- "
ALTER TABLE BGS_Matching_STP_Credential_PEN 
ADD         FINAL_CIP_CODE_4 VARCHAR(255),
            FINAL_CIP_CODE_4_NAME VARCHAR(255),
            FINAL_CIP_CODE_2 VARCHAR(255),
            FINAL_CIP_CODE_2_NAME VARCHAR(255),
            FINAL_CIP_CLUSTER_CODE VARCHAR(255),
            FINAL_CIP_CLUSTER_NAME VARCHAR(255),
            USE_BGS_CIP VARCHAR(255)"

## PART 3B ----
## qry02_Match_BGS_STP_Credential_Match_Inst ----
## set match institution flag
qry02_Match_BGS_STP_Credential_Match_Inst <- "
UPDATE BGS_Matching_STP_Credential_PEN
SET    Match_Inst = 'Yes'
WHERE (PSI_CODE = INSTITUTION_CODE) OR
      (PSI_CODE = 'CAPU')  AND (INSTITUTION_CODE = 'CAP')   OR
      (PSI_CODE = 'CAP')   AND (INSTITUTION_CODE = 'CAPU')  OR
      (PSI_CODE = 'DOUG')  AND (INSTITUTION_CODE = 'DGL')   OR
      (PSI_CODE = 'UCC')   AND (INSTITUTION_CODE = 'TRU')   OR
      (PSI_CODE = 'ECIAD') AND (INSTITUTION_CODE = 'ECU')   OR
      (PSI_CODE = 'ECIAD') AND (INSTITUTION_CODE = 'ECUAD') OR
      (PSI_CODE = 'ECU')   AND (INSTITUTION_CODE = 'ECUAD') OR
      (PSI_CODE = 'ECU')   AND (INSTITUTION_CODE = 'ECIAD') OR
      (PSI_CODE = 'KWAN')  AND (INSTITUTION_CODE = 'KPU')   OR
  		(PSI_CODE = 'KWAN')  AND (INSTITUTION_CODE = 'KWN')   OR
  		(PSI_CODE = 'KPU')   AND (INSTITUTION_CODE = 'KWN')   OR
      (PSI_CODE = 'MALA')  AND (INSTITUTION_CODE = 'VIU')   OR
      (PSI_CODE = 'MALA')  AND (INSTITUTION_CODE = 'MAL')   OR
      (PSI_CODE = 'OUC')   AND (INSTITUTION_CODE = 'OKAN')  OR
      (PSI_CODE = 'OUC')   AND (INSTITUTION_CODE = 'OKN')   OR
      (PSI_CODE = 'OKAN')  AND (INSTITUTION_CODE = 'OKN')   OR
      (PSI_CODE = 'OKAN')  AND (INSTITUTION_CODE = 'OUC')   OR
      (PSI_CODE = 'OLA')   AND (INSTITUTION_CODE = 'TRUOL') OR
      (PSI_CODE = 'UCFV')  AND (INSTITUTION_CODE = 'UFV')   OR
      (PSI_CODE = 'UCFV')  AND (INSTITUTION_CODE = 'FVAL')  OR
      (PSI_CODE = 'UFV')   AND (INSTITUTION_CODE = 'FVAL')  OR
      (PSI_CODE = 'UFV')   AND (INSTITUTION_CODE = 'UCFV')  OR
      (PSI_CODE = 'MAL')   AND (INSTITUTION_CODE = 'VIU')   OR
      (PSI_CODE = 'UBCO')  AND (INSTITUTION_CODE = 'UBC')   OR
      (PSI_CODE = 'UBCV')  AND (INSTITUTION_CODE = 'UBC')"

## qry03_Match_BGS_STP_Credential_Match_AwardYear ----
## set flag for matching award year (note the 2 year lag from STP to BGS as survey is 2 years out)
qry03_Match_BGS_STP_Credential_Match_AwardYear <- "
UPDATE BGS_Matching_STP_Credential_PEN
SET    Match_Award_School_Year = 'Yes'
WHERE  (YEAR = '2000') AND (PSI_AWARD_SCHOOL_YEAR = '1997/1998') OR
       (YEAR = '2000') AND (PSI_AWARD_SCHOOL_YEAR = '1998/1999') OR
       (YEAR = '2002') AND (PSI_AWARD_SCHOOL_YEAR = '1999/2000') OR
       (YEAR = '2002') AND (PSI_AWARD_SCHOOL_YEAR = '2000/2001') OR
       (YEAR = '2004') AND (PSI_AWARD_SCHOOL_YEAR = '2001/2002') OR
       (YEAR = '2004') AND (PSI_AWARD_SCHOOL_YEAR = '2002/2003') OR
       (YEAR = '2006') AND (PSI_AWARD_SCHOOL_YEAR = '2003/2004') OR
       (YEAR = '2006') AND (PSI_AWARD_SCHOOL_YEAR = '2004/2005') OR
       (YEAR = '2008') AND (PSI_AWARD_SCHOOL_YEAR = '2005/2006') OR
       (YEAR = '2008') AND (PSI_AWARD_SCHOOL_YEAR = '2006/2007') OR
       (YEAR = '2009') AND (PSI_AWARD_SCHOOL_YEAR = '2006/2007') OR
       (YEAR = '2009') AND (PSI_AWARD_SCHOOL_YEAR = '2007/2008') OR
       (YEAR = '2010') AND (PSI_AWARD_SCHOOL_YEAR = '2007/2008') OR
       (YEAR = '2010') AND (PSI_AWARD_SCHOOL_YEAR = '2008/2009') OR
       (YEAR = '2011') AND (PSI_AWARD_SCHOOL_YEAR = '2008/2009') OR
       (YEAR = '2011') AND (PSI_AWARD_SCHOOL_YEAR = '2009/2010') OR
       (YEAR = '2012') AND (PSI_AWARD_SCHOOL_YEAR = '2009/2010') OR
       (YEAR = '2012') AND (PSI_AWARD_SCHOOL_YEAR = '2010/2011') OR
       (YEAR = '2013') AND (PSI_AWARD_SCHOOL_YEAR = '2010/2011') OR
       (YEAR = '2013') AND (PSI_AWARD_SCHOOL_YEAR = '2011/2012') OR
       (YEAR = '2014') AND (PSI_AWARD_SCHOOL_YEAR = '2011/2012') OR
       (YEAR = '2014') AND (PSI_AWARD_SCHOOL_YEAR = '2012/2013') OR
       (YEAR = '2015') AND (PSI_AWARD_SCHOOL_YEAR = '2012/2013') OR
       (YEAR = '2015') AND (PSI_AWARD_SCHOOL_YEAR = '2013/2014') OR
       (YEAR = '2016') AND (PSI_AWARD_SCHOOL_YEAR = '2013/2014') OR
       (YEAR = '2016') AND (PSI_AWARD_SCHOOL_YEAR = '2014/2015') OR
       (YEAR = '2017') AND (PSI_AWARD_SCHOOL_YEAR = '2014/2015') OR
       (YEAR = '2017') AND (PSI_AWARD_SCHOOL_YEAR = '2015/2016') OR
			 (YEAR = '2018') AND (PSI_AWARD_SCHOOL_YEAR = '2015/2016') OR
       (YEAR = '2018') AND (PSI_AWARD_SCHOOL_YEAR = '2016/2017') OR
			 (YEAR = '2019') AND (PSI_AWARD_SCHOOL_YEAR = '2016/2017') OR
       (YEAR = '2019') AND (PSI_AWARD_SCHOOL_YEAR = '2017/2018') OR
       (YEAR = '2020') AND (PSI_AWARD_SCHOOL_YEAR = '2017/2018') OR
       (YEAR = '2020') AND (PSI_AWARD_SCHOOL_YEAR = '2018/2019') OR
       (YEAR = '2021') AND (PSI_AWARD_SCHOOL_YEAR = '2018/2019') OR
       (YEAR = '2021') AND (PSI_AWARD_SCHOOL_YEAR = '2019/2020') OR
       (YEAR = '2022') AND (PSI_AWARD_SCHOOL_YEAR = '2019/2020') OR
       (YEAR = '2022') AND (PSI_AWARD_SCHOOL_YEAR = '2020/2021') OR
       (YEAR = '2023') AND (PSI_AWARD_SCHOOL_YEAR = '2020/2021') OR
       (YEAR = '2023') AND (PSI_AWARD_SCHOOL_YEAR = '2021/2022')"

## qry04_Match_BGS_STP_Credential_Match_CIPCODE4 ----
## set flag for matching 4D cip code
qry04_Match_BGS_STP_Credential_Match_CIPCODE4 <- "
UPDATE BGS_Matching_STP_Credential_PEN
SET    Match_CIP_CODE_4 = 'Yes'
WHERE  BGS_Matching_STP_Credential_PEN.BGS_FINAL_CIP_CODE_4 = STP_FINAL_CIP_CODE_4"

## qry05_Match_BGS_STP_Credential_Match_CIPCODE2 ----
## set flag for matching 2d cip code
qry05_Match_BGS_STP_Credential_Match_CIPCODE2 <- "
UPDATE BGS_Matching_STP_Credential_PEN
SET    Match_CIP_CODE_2 = 'Yes'
WHERE  BGS_Matching_STP_Credential_PEN.BGS_FINAL_CIP_CODE_2 = STP_FINAL_CIP_CODE_2"

## qry06_Match_BGS_STP_Credential_MatchAll3_CIP4Flag ----
## set flag for matching on 4D cip code, award year and institution
qry06_Match_BGS_STP_Credential_MatchAll3_CIP4Flag <- "
UPDATE BGS_Matching_STP_Credential_PEN 
SET    BGS_Matching_STP_Credential_PEN.Match_All_3_CIP4_Flag = 'Yes'
WHERE  BGS_Matching_STP_Credential_PEN.Match_CIP_CODE_4 = 'Yes' AND
       BGS_Matching_STP_Credential_PEN.Match_Award_School_Year = 'Yes' AND 
       BGS_Matching_STP_Credential_PEN.Match_Inst = 'Yes'"

## qry07_Match_BGS_STP_Credential_MatchAll3_CIP2Flag ----
## set flag for matching on 2D cip code, award year and institution
qry07_Match_BGS_STP_Credential_MatchAll3_CIP2Flag <- "
UPDATE BGS_Matching_STP_Credential_PEN 
SET    BGS_Matching_STP_Credential_PEN.Match_All_3_CIP2_Flag = 'Yes'
WHERE  BGS_Matching_STP_Credential_PEN.Match_CIP_CODE_2 = 'Yes' AND
       BGS_Matching_STP_Credential_PEN.Match_Award_School_Year = 'Yes' AND 
       BGS_Matching_STP_Credential_PEN.Match_Inst = 'Yes'"

## qry_Update_Final_Match_if_MatchAll3_CIP4Flag ----
qry_Update_Final_Match_if_MatchAll3_CIP4Flag <- "
UPDATE BGS_Matching_STP_Credential_PEN
SET    BGS_Matching_STP_Credential_PEN.Final_Consider_A_Match = 'Yes', 
       BGS_Matching_STP_Credential_PEN.FINAL_CIP_CODE_4 = [BGS_Matching_STP_Credential_PEN].[BGS_FINAL_CIP_CODE_4],
       BGS_Matching_STP_Credential_PEN.FINAL_CIP_CODE_2 = [BGS_Matching_STP_Credential_PEN].[BGS_FINAL_CIP_CODE_2], 
       BGS_Matching_STP_Credential_PEN.USE_BGS_CIP = 'Yes'
WHERE  BGS_Matching_STP_Credential_PEN.Match_All_3_CIP4_Flag='Yes'"

## qry_Check_BGS_Match_All3_CIP2 ----
qry_Check_BGS_Match_All3_CIP2 <- "
SELECT BGS_Matching_STP_Credential_PEN.INSTITUTION_CODE,
       BGS_Matching_STP_Credential_PEN.PSI_CODE, 
       BGS_Matching_STP_Credential_PEN.YEAR, 
       BGS_Matching_STP_Credential_PEN.PSI_AWARD_SCHOOL_YEAR, 
       BGS_Matching_STP_Credential_PEN.BGS_PROGRAM_CODE,
       BGS_Matching_STP_Credential_PEN.STP_PROGRAM_CODE, 
       BGS_Matching_STP_Credential_PEN.BGS_PROGRAM_DESC,
       BGS_Matching_STP_Credential_PEN.STP_PROGRAM_DESC, 
       BGS_Matching_STP_Credential_PEN.BGS_FINAL_CIP_CODE_4, 
       BGS_Matching_STP_Credential_PEN.BGS_FINAL_CIP_CODE_4_NAME, 
       BGS_Matching_STP_Credential_PEN.STP_FINAL_CIP_CODE_4, 
       BGS_Matching_STP_Credential_PEN.STP_FINAL_CIP_CODE_4_NAME, 
       BGS_Matching_STP_Credential_PEN.BGS_FINAL_CIP_CODE_2, 
       BGS_Matching_STP_Credential_PEN.BGS_FINAL_CIP_CODE_2_NAME, 
       BGS_Matching_STP_Credential_PEN.STP_FINAL_CIP_CODE_2, 
       BGS_Matching_STP_Credential_PEN.STP_FINAL_CIP_CODE_2_NAME, 
       BGS_Matching_STP_Credential_PEN.Match_Inst, 
       BGS_Matching_STP_Credential_PEN.Match_Award_School_Year, 
       BGS_Matching_STP_Credential_PEN.Match_CIP_CODE_4, 
       BGS_Matching_STP_Credential_PEN.Match_All_3_CIP4_Flag, 
       BGS_Matching_STP_Credential_PEN.Match_CIP_CODE_2, 
       BGS_Matching_STP_Credential_PEN.Match_All_3_CIP2_Flag, 
       BGS_Matching_STP_Credential_PEN.Final_Consider_A_Match,
       BGS_Matching_STP_Credential_PEN.Final_Probable_Match,
       Count(*) AS Expr1
FROM   BGS_Matching_STP_Credential_PEN
GROUP BY 
       BGS_Matching_STP_Credential_PEN.INSTITUTION_CODE, 
       BGS_Matching_STP_Credential_PEN.PSI_CODE,  
       BGS_Matching_STP_Credential_PEN.YEAR, 
       BGS_Matching_STP_Credential_PEN.PSI_AWARD_SCHOOL_YEAR, 
       BGS_Matching_STP_Credential_PEN.BGS_PROGRAM_CODE, 
       BGS_Matching_STP_Credential_PEN.STP_PROGRAM_CODE,
       BGS_Matching_STP_Credential_PEN.BGS_PROGRAM_DESC, 
       BGS_Matching_STP_Credential_PEN.STP_PROGRAM_DESC, 
       BGS_Matching_STP_Credential_PEN.BGS_FINAL_CIP_CODE_4,
       BGS_Matching_STP_Credential_PEN.BGS_FINAL_CIP_CODE_4_NAME, 
       BGS_Matching_STP_Credential_PEN.STP_FINAL_CIP_CODE_4,
       BGS_Matching_STP_Credential_PEN.STP_FINAL_CIP_CODE_4_NAME,
       BGS_Matching_STP_Credential_PEN.BGS_FINAL_CIP_CODE_2,
       BGS_Matching_STP_Credential_PEN.BGS_FINAL_CIP_CODE_2_NAME,
       BGS_Matching_STP_Credential_PEN.STP_FINAL_CIP_CODE_2, 
       BGS_Matching_STP_Credential_PEN.STP_FINAL_CIP_CODE_2_NAME, 
       BGS_Matching_STP_Credential_PEN.Match_Inst, 
       BGS_Matching_STP_Credential_PEN.Match_Award_School_Year,
       BGS_Matching_STP_Credential_PEN.Match_CIP_CODE_4, 
       BGS_Matching_STP_Credential_PEN.Match_All_3_CIP4_Flag, 
       BGS_Matching_STP_Credential_PEN.Match_CIP_CODE_2,
       BGS_Matching_STP_Credential_PEN.Match_All_3_CIP2_Flag,
       BGS_Matching_STP_Credential_PEN.Final_Consider_A_Match, 
       BGS_Matching_STP_Credential_PEN.Final_Probable_Match
HAVING BGS_Matching_STP_Credential_PEN.Match_All_3_CIP4_Flag Is Null AND
       BGS_Matching_STP_Credential_PEN.Match_All_3_CIP2_Flag='Yes'"

## PART 3C ----
## qry_BGS_Matching_STP_Credential_PEN_Inst_AwardYearOnly ----
## new to 2024 don't write table into an SQL table at this time
## will write to SQL after doing manual program matching
qry_BGS_Matching_STP_Credential_PEN_Inst_AwardYearOnly <- "
SELECT STQU_ID, ID, PEN,
       BGS_Matching_STP_Credential_PEN.INSTITUTION_CODE,
       BGS_Matching_STP_Credential_PEN.PSI_CODE,
       BGS_Matching_STP_Credential_PEN.YEAR,
       BGS_Matching_STP_Credential_PEN.PSI_AWARD_SCHOOL_YEAR,
       BGS_Matching_STP_Credential_PEN.Match_Inst,
       BGS_Matching_STP_Credential_PEN.Match_Award_School_Year,
       BGS_Matching_STP_Credential_PEN.Match_All_3_CIP4_Flag,
       BGS_Matching_STP_Credential_PEN.Match_All_3_CIP2_Flag,
       BGS_Matching_STP_Credential_PEN.Final_Consider_A_Match,
       BGS_Matching_STP_Credential_PEN.BGS_FINAL_CIP_CODE_4,
       BGS_Matching_STP_Credential_PEN.BGS_FINAL_CIP_CODE_4_NAME,
       BGS_Matching_STP_Credential_PEN.STP_FINAL_CIP_CODE_4,
       BGS_Matching_STP_Credential_PEN.STP_FINAL_CIP_CODE_4_NAME,
       BGS_Matching_STP_Credential_PEN.BGS_FINAL_CIP_CODE_2,
       BGS_Matching_STP_Credential_PEN.BGS_FINAL_CIP_CODE_2_NAME,
       BGS_Matching_STP_Credential_PEN.STP_FINAL_CIP_CODE_2,
       BGS_Matching_STP_Credential_PEN.STP_FINAL_CIP_CODE_2_NAME,
       BGS_Matching_STP_Credential_PEN.BGS_PROGRAM_CODE,
       BGS_Matching_STP_Credential_PEN.BGS_PROGRAM_DESC,
       BGS_Matching_STP_Credential_PEN.STP_PROGRAM_CODE,
       BGS_Matching_STP_Credential_PEN.STP_PROGRAM_DESC,
       BGS_Matching_STP_Credential_PEN.USE_BGS_CIP
FROM   BGS_Matching_STP_Credential_PEN
WHERE  BGS_Matching_STP_Credential_PEN.Match_Inst='Yes' AND
       BGS_Matching_STP_Credential_PEN.Match_Award_School_Year='Yes' AND
       BGS_Matching_STP_Credential_PEN.Final_Consider_A_Match Is Null"

## qry_Summary_BGS_Matching_STP_Inst_AwardYear_Checking ----
qry_Summary_BGS_Matching_STP_Inst_AwardYear_Checking <- "
SELECT BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly.INSTITUTION_CODE,
       BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly.BGS_FINAL_CIP_CODE_4,
       BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly.BGS_FINAL_CIP_CODE_4_NAME,
       BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly.STP_FINAL_CIP_CODE_4,
       BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly.STP_FINAL_CIP_CODE_4_NAME,
       BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly.BGS_PROGRAM_CODE,
       BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly.BGS_PROGRAM_DESC,
       BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly.STP_PROGRAM_CODE,
       BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly.STP_PROGRAM_DESC,
       BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly.USE_BGS_CIP,
       Count(*) AS Count
FROM   BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly
GROUP BY
       BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly.INSTITUTION_CODE,
       BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly.BGS_FINAL_CIP_CODE_4,
       BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly.BGS_FINAL_CIP_CODE_4_NAME,
       BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly.STP_FINAL_CIP_CODE_4,
       BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly.STP_FINAL_CIP_CODE_4_NAME,
       BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly.BGS_PROGRAM_CODE,
       BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly.BGS_PROGRAM_DESC,
       BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly.STP_PROGRAM_CODE,
       BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly.STP_PROGRAM_DESC,
       BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly.USE_BGS_CIP"

## qry_update_CIP_for_MatchingYearInstOnly_step1 ----
qry_update_CIP_for_MatchingYearInstOnly_step1 <- "
UPDATE BGS_Matching_STP_Credential_PEN
SET    BGS_Matching_STP_Credential_PEN.Final_Probable_Match = 'Yes', 
       BGS_Matching_STP_Credential_PEN.FINAL_CIP_CODE_4 = [BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly].Final_CIP_CODE_4,
       BGS_Matching_STP_Credential_PEN.FINAL_CIP_CODE_2 = [BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly].Final_CIP_CODE_2, 
       BGS_Matching_STP_Credential_PEN.USE_BGS_CIP = [BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly].[USE_BGS_CIP]
FROM   BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly INNER JOIN BGS_Matching_STP_Credential_PEN 
ON     BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly.ID = BGS_Matching_STP_Credential_PEN.ID AND
       BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly.stqu_id = BGS_Matching_STP_Credential_PEN.stqu_id
WHERE  BGS_Matching_STP_Credential_PEN.Final_Probable_Match Is Null AND 
       BGS_Matching_STP_Credential_PEN.FINAL_CIP_CODE_4 Is Null AND 
       BGS_Matching_STP_Credential_PEN.FINAL_CIP_CODE_2 Is Null AND
       BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly.FINAL_CIP_CODE_4 Is Not Null"

## qry_update_CIP_for_MatchingYearInstOnly_step2 ----
qry_update_CIP_for_MatchingYearInstOnly_step2 <- "
UPDATE BGS_Matching_STP_Credential_PEN 
SET    BGS_Matching_STP_Credential_PEN.FINAL_CIP_CODE_4 = [BGS_Matching_STP_Credential_PEN].[STP_FINAL_CIP_CODE_4],
       BGS_Matching_STP_Credential_PEN.FINAL_CIP_CODE_4_NAME = [BGS_Matching_STP_Credential_PEN].[STP_FINAL_CIP_CODE_4_NAME],
       BGS_Matching_STP_Credential_PEN.FINAL_CIP_CODE_2 = [BGS_Matching_STP_Credential_PEN].[STP_FINAL_CIP_CODE_2],
       BGS_Matching_STP_Credential_PEN.FINAL_CIP_CODE_2_NAME = [BGS_Matching_STP_Credential_PEN].[STP_FINAL_CIP_CODE_2_NAME],
       BGS_Matching_STP_Credential_PEN.USE_BGS_CIP = 'No'
WHERE  BGS_Matching_STP_Credential_PEN.FINAL_CIP_CODE_4 Is Null AND
       BGS_Matching_STP_Credential_PEN.FINAL_CIP_CODE_4_NAME Is Null AND
       BGS_Matching_STP_Credential_PEN.FINAL_CIP_CODE_2 Is Null AND
       BGS_Matching_STP_Credential_PEN.FINAL_CIP_CODE_2_NAME Is Null"

## PART 3D ----
## qry_fill_final_CIP4_NAME ----
## New: fill in CIP4 NAME by linking to infoware table
qry_fill_final_CIP4_NAME <- "
UPDATE BGS_Matching_STP_Credential_PEN
SET    FINAL_CIP_CODE_4_NAME = LCP4_CIP_4DIGITS_NAME
FROM   BGS_Matching_STP_Credential_PEN INNER JOIN INFOWARE_L_CIP_4DIGITS_CIP2016
ON     FINAL_CIP_CODE_4 = LCP4_CD"

## qry_fill_final_CIP2_NAME_and_CLUSTER ----
## New: fill in CIP2 NAME and cluster code/name from infoware table
qry_fill_final_CIP2_NAME_and_CLUSTER <- "
UPDATE BGS_Matching_STP_Credential_PEN
SET    FINAL_CIP_CODE_2_NAME = LCP2_DIGITS_NAME,
       FINAL_CIP_CLUSTER_CODE = LCP2_LCIPPC_CD,
       FINAL_CIP_CLUSTER_NAME = LCP2_LCIPPC_NAME
FROM   BGS_Matching_STP_Credential_PEN INNER JOIN INFOWARE_L_CIP_2DIGITS_CIP2016
ON     FINAL_CIP_CODE_2 = LCP2_CD"

# PART 4: UPDATE CREDENTIAL_NON_DUP ----

## PART 4A ----
## qry_BGS_IDs_Credential_add_columns ----
## New: need to add columns to populate
qry_BGS_IDs_Credential_add_columns <- "
ALTER TABLE Credential_Non_DUP_BGS_IDs
ADD OUTCOMES_CIP_CODE_4 varchar (255),
    OUTCOMES_CIP_CODE_4_NAME varchar (255),
    Final_Consider_A_Match varchar (255),
    Final_Probable_Match varchar (255),
    USE_BGS_CIP varchar (255),
    FINAL_CIP_CODE_4 varchar (255),
    FINAL_CIP_CODE_4_NAME varchar (255),
    FINAL_CIP_CODE_2 varchar (255),
    FINAL_CIP_CODE_2_NAME varchar (255),
    FINAL_CIP_CLUSTER_CODE varchar (255),
    FINAL_CIP_CLUSTER_NAME varchar (255)"

## qry_update_Credential_Non_Dup_BGS_IDS_CIP_matches_step1 ----
qry_update_Credential_Non_Dup_BGS_IDS_CIP_matches_step1 <- "
UPDATE Credential_Non_DUP_BGS_IDs
SET    Credential_Non_DUP_BGS_IDs.FINAL_CIP_CODE_4 = [BGS_Matching_STP_Credential_PEN].[Final_CIP_CODE_4], 
       Credential_Non_DUP_BGS_IDs.FINAL_CIP_CODE_4_NAME = [BGS_Matching_STP_Credential_PEN].[FINAL_CIP_CODE_4_NAME],
       Credential_Non_DUP_BGS_IDs.FINAL_CIP_CODE_2 = [BGS_Matching_STP_Credential_PEN].[Final_CIP_CODE_2], 
       Credential_Non_DUP_BGS_IDs.FINAL_CIP_CODE_2_NAME = [BGS_Matching_STP_Credential_PEN].[FINAL_CIP_CODE_2_NAME],
       Credential_Non_DUP_BGS_IDs.FINAL_CIP_CLUSTER_CODE = [BGS_Matching_STP_Credential_PEN].[FINAL_CIP_CLUSTER_CODE], 
       Credential_Non_DUP_BGS_IDs.FINAL_CIP_CLUSTER_NAME = [BGS_Matching_STP_Credential_PEN].[FINAL_CIP_CLUSTER_NAME], 
       Credential_Non_DUP_BGS_IDs.USE_BGS_CIP = [BGS_Matching_STP_Credential_PEN].[USE_BGS_CIP], 
       Credential_Non_DUP_BGS_IDs.OUTCOMES_CIP_CODE_4 = [BGS_Matching_STP_Credential_PEN].[BGS_FINAL_CIP_CODE_4],
       Credential_Non_DUP_BGS_IDs.OUTCOMES_CIP_CODE_4_NAME = [BGS_Matching_STP_Credential_PEN].[BGS_FINAL_CIP_CODE_4_NAME],
       Credential_Non_DUP_BGS_IDs.Final_Consider_A_Match = [BGS_Matching_STP_Credential_PEN].[Final_Consider_A_Match],
       Credential_Non_DUP_BGS_IDs.Final_Probable_Match = [BGS_Matching_STP_Credential_PEN].[Final_Probable_Match]
FROM   Credential_Non_DUP_BGS_IDs INNER JOIN BGS_Matching_STP_Credential_PEN 
ON     Credential_Non_DUP_BGS_IDs.id = BGS_Matching_STP_Credential_PEN.ID 
WHERE  BGS_Matching_STP_Credential_PEN.Final_Consider_A_Match Is Not Null And 
       BGS_Matching_STP_Credential_PEN.Final_Consider_A_Match <>''"

## qry_update_Credential_Non_Dup_BGS_IDS_CIP_matches_step2 ----
qry_update_Credential_Non_Dup_BGS_IDS_CIP_matches_step2 <- "
UPDATE Credential_Non_DUP_BGS_IDs 
SET    Credential_Non_DUP_BGS_IDs.FINAL_CIP_CODE_4 = [BGS_Matching_STP_Credential_PEN].[Final_CIP_CODE_4], 
       Credential_Non_DUP_BGS_IDs.FINAL_CIP_CODE_4_NAME = [BGS_Matching_STP_Credential_PEN].[FINAL_CIP_CODE_4_NAME], 
       Credential_Non_DUP_BGS_IDs.FINAL_CIP_CODE_2 = [BGS_Matching_STP_Credential_PEN].[Final_CIP_CODE_2], 
       Credential_Non_DUP_BGS_IDs.FINAL_CIP_CODE_2_NAME = [BGS_Matching_STP_Credential_PEN].[FINAL_CIP_CODE_2_NAME], 
       Credential_Non_DUP_BGS_IDs.FINAL_CIP_CLUSTER_CODE = [BGS_Matching_STP_Credential_PEN].[FINAL_CIP_CLUSTER_CODE],
       Credential_Non_DUP_BGS_IDs.FINAL_CIP_CLUSTER_NAME = [BGS_Matching_STP_Credential_PEN].[FINAL_CIP_CLUSTER_NAME], 
       Credential_Non_DUP_BGS_IDs.USE_BGS_CIP = [BGS_Matching_STP_Credential_PEN].[USE_BGS_CIP], 
       Credential_Non_DUP_BGS_IDs.OUTCOMES_CIP_CODE_4 = [BGS_Matching_STP_Credential_PEN].[BGS_FINAL_CIP_CODE_4], 
       Credential_Non_DUP_BGS_IDs.OUTCOMES_CIP_CODE_4_NAME = [BGS_Matching_STP_Credential_PEN].[BGS_FINAL_CIP_CODE_4_NAME], 
       Credential_Non_DUP_BGS_IDs.Final_Probable_Match = [BGS_Matching_STP_Credential_PEN].[Final_Probable_Match]
FROM   Credential_Non_DUP_BGS_IDs INNER JOIN BGS_Matching_STP_Credential_PEN
ON     Credential_Non_DUP_BGS_IDs.id = BGS_Matching_STP_Credential_PEN.ID 
WHERE (Credential_Non_DUP_BGS_IDs.FINAL_CIP_CODE_4 Is Null AND 
       Credential_Non_DUP_BGS_IDs.FINAL_CIP_CODE_4_NAME Is Null AND
       Credential_Non_DUP_BGS_IDs.FINAL_CIP_CODE_2 Is Null AND 
       Credential_Non_DUP_BGS_IDs.FINAL_CIP_CODE_2_NAME Is Null AND 
       Credential_Non_DUP_BGS_IDs.FINAL_CIP_CLUSTER_CODE Is Null AND 
       Credential_Non_DUP_BGS_IDs.FINAL_CIP_CLUSTER_NAME Is Null AND 
       Credential_Non_DUP_BGS_IDs.USE_BGS_CIP Is Null AND 
       Credential_Non_DUP_BGS_IDs.OUTCOMES_CIP_CODE_4 Is Null AND
       Credential_Non_DUP_BGS_IDs.OUTCOMES_CIP_CODE_4_NAME Is Null AND 
       Credential_Non_DUP_BGS_IDs.Final_Probable_Match Is Null AND
        (BGS_Matching_STP_Credential_PEN.Final_Probable_Match Is Not Null And
         BGS_Matching_STP_Credential_PEN.Final_Probable_Match<>'')
       )"

## qry_update_remaining_BGS_CIPs_in_Cred_Non_Dup_BGS_IDS_step1 ----
qry_update_remaining_BGS_CIPs_in_Cred_Non_Dup_BGS_IDS_step1 <- "
UPDATE Credential_Non_DUP_BGS_IDs 
SET    Credential_Non_DUP_BGS_IDs.FINAL_CIP_CODE_4 = [Credential_Non_DUP_BGS_IDs].[STP_CIP_CODE_4], 
       Credential_Non_DUP_BGS_IDs.FINAL_CIP_CODE_4_NAME = [Credential_Non_Dup_BGS_IDs].[STP_CIP_CODE_4_NAME],
       Credential_Non_DUP_BGS_IDs.FINAL_CIP_CODE_2 = [Credential_Non_DUP_BGS_IDs].[STP_CIP_CODE_2], 
       Credential_Non_DUP_BGS_IDs.FINAL_CIP_CODE_2_NAME = [Credential_Non_Dup_BGS_IDs].[STP_CIP_CODE_2_NAME], 
       Credential_Non_DUP_BGS_IDs.USE_BGS_CIP = 'No because no match'
WHERE  Credential_Non_DUP_BGS_IDs.FINAL_CIP_CODE_4 Is Null AND
       Credential_Non_DUP_BGS_IDs.FINAL_CIP_CODE_2 Is Null AND 
       Credential_Non_DUP_BGS_IDs.Final_Consider_A_Match Is Null AND
       Credential_Non_DUP_BGS_IDs.Final_Probable_Match Is Null"

## qry_update_remaining_BGS_CIPs_in_Cred_Non_Dup_BGS_IDS_step2 ----
qry_update_remaining_BGS_CIPs_in_Cred_Non_Dup_BGS_IDS_step2 <- "
UPDATE Credential_Non_Dup_BGS_IDs 
SET    Credential_Non_Dup_BGS_IDs.FINAL_CIP_CLUSTER_CODE = [INFOWARE_L_CIP_2DIGITS_CIP2016].[LCP2_LCIPPC_CD], 
       Credential_Non_Dup_BGS_IDs.FINAL_CIP_CLUSTER_NAME = [INFOWARE_L_CIP_2DIGITS_CIP2016].[LCP2_LCIPPC_NAME]
FROM   Credential_Non_Dup_BGS_IDs INNER JOIN INFOWARE_L_CIP_2DIGITS_CIP2016 
ON     Credential_Non_Dup_BGS_IDs.FINAL_CIP_CODE_2 = INFOWARE_L_CIP_2DIGITS_CIP2016.LCP2_CD 
WHERE  Credential_Non_Dup_BGS_IDs.FINAL_CIP_CLUSTER_CODE Is Null AND
       Credential_Non_Dup_BGS_IDs.FINAL_CIP_CLUSTER_NAME Is Null"

## PART 4B ----
## qry_List_STP_Credential_Non_Dup_Using_BGS_CIPS ----
qry_List_STP_Credential_Non_Dup_Using_BGS_CIPS <- "
SELECT Credential_Non_Dup_BGS_IDs.PSI_CODE, 
       Credential_Non_Dup_BGS_IDs.PSI_PROGRAM_CODE,
       Credential_Non_Dup_BGS_IDs.PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
       Credential_Non_Dup_BGS_IDs.OUTCOMES_CIP_CODE_4, 
       Credential_Non_Dup_BGS_IDs.OUTCOMES_CIP_CODE_4_NAME, 
       Credential_Non_Dup_BGS_IDs.STP_CIP_CODE_4, 
       Credential_Non_Dup_BGS_IDs.STP_CIP_CODE_4_NAME,
       Credential_Non_Dup_BGS_IDs.STP_CIP_CODE_2,
       Credential_Non_Dup_BGS_IDs.STP_CIP_CODE_2_NAME,
       Credential_Non_Dup_BGS_IDs.FINAL_CIP_CODE_4,
       Credential_Non_Dup_BGS_IDs.FINAL_CIP_CODE_4_NAME,
       Credential_Non_Dup_BGS_IDs.FINAL_CIP_CODE_2, 
       Credential_Non_Dup_BGS_IDs.FINAL_CIP_CODE_2_NAME,
       Credential_Non_Dup_BGS_IDs.FINAL_CIP_CLUSTER_CODE,
       Credential_Non_Dup_BGS_IDs.FINAL_CIP_CLUSTER_NAME, 
       Credential_Non_Dup_BGS_IDs.Final_Consider_A_Match, 
       Credential_Non_Dup_BGS_IDs.Final_Probable_Match, 
       Credential_Non_Dup_BGS_IDs.USE_BGS_CIP,
       Count(*) AS Expr1 
FROM   Credential_Non_Dup_BGS_IDs
GROUP BY 
       Credential_Non_Dup_BGS_IDs.PSI_CODE, 
       Credential_Non_Dup_BGS_IDs.PSI_PROGRAM_CODE, 
       Credential_Non_Dup_BGS_IDs.PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
       Credential_Non_Dup_BGS_IDs.OUTCOMES_CIP_CODE_4, 
       Credential_Non_Dup_BGS_IDs.OUTCOMES_CIP_CODE_4_NAME,
       Credential_Non_Dup_BGS_IDs.STP_CIP_CODE_4,
       Credential_Non_Dup_BGS_IDs.STP_CIP_CODE_4_NAME, 
       Credential_Non_Dup_BGS_IDs.STP_CIP_CODE_2, 
       Credential_Non_Dup_BGS_IDs.STP_CIP_CODE_2_NAME, 
       Credential_Non_Dup_BGS_IDs.FINAL_CIP_CODE_4, 
       Credential_Non_Dup_BGS_IDs.FINAL_CIP_CODE_4_NAME,
       Credential_Non_Dup_BGS_IDs.FINAL_CIP_CODE_2,
       Credential_Non_Dup_BGS_IDs.FINAL_CIP_CODE_2_NAME,
       Credential_Non_Dup_BGS_IDs.FINAL_CIP_CLUSTER_CODE,
       Credential_Non_Dup_BGS_IDs.FINAL_CIP_CLUSTER_NAME,
       Credential_Non_Dup_BGS_IDs.Final_Consider_A_Match, 
       Credential_Non_Dup_BGS_IDs.Final_Probable_Match,
       Credential_Non_Dup_BGS_IDs.USE_BGS_CIP
HAVING Credential_Non_Dup_BGS_IDs.USE_BGS_CIP='Yes'"

## qry_List_STP_Credential_Non_Dup_Unmatched ----
## New: a query to build this table was missing
qry_List_STP_Credential_Non_Dup_Umatched <- "
SELECT Credential_Non_Dup_BGS_IDs.PSI_CODE, 
       Credential_Non_Dup_BGS_IDs.PSI_PROGRAM_CODE,
       Credential_Non_Dup_BGS_IDs.PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
       Credential_Non_Dup_BGS_IDs.OUTCOMES_CIP_CODE_4, 
       Credential_Non_Dup_BGS_IDs.OUTCOMES_CIP_CODE_4_NAME, 
       Credential_Non_Dup_BGS_IDs.STP_CIP_CODE_4, 
       Credential_Non_Dup_BGS_IDs.STP_CIP_CODE_4_NAME,
       Credential_Non_Dup_BGS_IDs.STP_CIP_CODE_2,
       Credential_Non_Dup_BGS_IDs.STP_CIP_CODE_2_NAME,
       Credential_Non_Dup_BGS_IDs.FINAL_CIP_CODE_4,
       Credential_Non_Dup_BGS_IDs.FINAL_CIP_CODE_4_NAME,
       Credential_Non_Dup_BGS_IDs.FINAL_CIP_CODE_2, 
       Credential_Non_Dup_BGS_IDs.FINAL_CIP_CODE_2_NAME,
       Credential_Non_Dup_BGS_IDs.FINAL_CIP_CLUSTER_CODE,
       Credential_Non_Dup_BGS_IDs.FINAL_CIP_CLUSTER_NAME, 
       Credential_Non_Dup_BGS_IDs.Final_Consider_A_Match, 
       Credential_Non_Dup_BGS_IDs.Final_Probable_Match, 
       Credential_Non_Dup_BGS_IDs.USE_BGS_CIP,
       Count(*) AS Expr1 
FROM   Credential_Non_Dup_BGS_IDs
GROUP BY 
       Credential_Non_Dup_BGS_IDs.PSI_CODE, 
       Credential_Non_Dup_BGS_IDs.PSI_PROGRAM_CODE, 
       Credential_Non_Dup_BGS_IDs.PSI_CREDENTIAL_PROGRAM_DESCRIPTION,
       Credential_Non_Dup_BGS_IDs.OUTCOMES_CIP_CODE_4, 
       Credential_Non_Dup_BGS_IDs.OUTCOMES_CIP_CODE_4_NAME,
       Credential_Non_Dup_BGS_IDs.STP_CIP_CODE_4,
       Credential_Non_Dup_BGS_IDs.STP_CIP_CODE_4_NAME, 
       Credential_Non_Dup_BGS_IDs.STP_CIP_CODE_2, 
       Credential_Non_Dup_BGS_IDs.STP_CIP_CODE_2_NAME, 
       Credential_Non_Dup_BGS_IDs.FINAL_CIP_CODE_4, 
       Credential_Non_Dup_BGS_IDs.FINAL_CIP_CODE_4_NAME,
       Credential_Non_Dup_BGS_IDs.FINAL_CIP_CODE_2,
       Credential_Non_Dup_BGS_IDs.FINAL_CIP_CODE_2_NAME,
       Credential_Non_Dup_BGS_IDs.FINAL_CIP_CLUSTER_CODE,
       Credential_Non_Dup_BGS_IDs.FINAL_CIP_CLUSTER_NAME,
       Credential_Non_Dup_BGS_IDs.Final_Consider_A_Match, 
       Credential_Non_Dup_BGS_IDs.Final_Probable_Match,
       Credential_Non_Dup_BGS_IDs.USE_BGS_CIP
HAVING Credential_Non_Dup_BGS_IDs.USE_BGS_CIP='No because no match'"

## qry_update_Credential_Non_DUP_BGS_IDs_unmatched ----
## New: update Credential_Non_Dup_BGS_IDs so unmatched programs use linked BGS CIP instead
qry_update_Credential_Non_DUP_BGS_IDs_unmatched <-  "
UPDATE Credential_Non_Dup_BGS_IDs
SET    Credential_Non_Dup_BGS_IDs.FINAL_CIP_CODE_4 = Credential_Unmatched_CIPS_to_update.FINAL_CIP_CODE_4, 
       Credential_Non_Dup_BGS_IDs.FINAL_CIP_CODE_4_NAME = NULL,
       Credential_Non_Dup_BGS_IDs.FINAL_CIP_CODE_2 =Credential_Unmatched_CIPS_to_update.FINAL_CIP_CODE_2, 
       Credential_Non_Dup_BGS_IDs.FINAL_CIP_CODE_2_NAME = NULL, 
       Credential_Non_Dup_BGS_IDs.FINAL_CIP_CLUSTER_CODE = NULL, 
       Credential_Non_Dup_BGS_IDs.FINAL_CIP_CLUSTER_NAME = NULL
FROM   Credential_Non_Dup_BGS_IDs INNER JOIN Credential_Unmatched_CIPS_to_update
ON     Credential_Non_Dup_BGS_IDs.PSI_CREDENTIAL_PROGRAM_DESCRIPTION = Credential_Unmatched_CIPS_to_update.PSI_CREDENTIAL_PROGRAM_DESCRIPTION
WHERE  Final_Consider_A_Match is NULL AND Final_Probable_Match is NULL"

## qry_fill_final_CIP4_NAME_Credential ----
## New: fill in CIP4 NAME of updated CIPs by linking to infoware table
qry_fill_final_CIP4_NAME_Credential <- "
UPDATE Credential_Non_Dup_BGS_IDs
SET    FINAL_CIP_CODE_4_NAME = LCP4_CIP_4DIGITS_NAME
FROM   Credential_Non_Dup_BGS_IDs INNER JOIN INFOWARE_L_CIP_4DIGITS_CIP2016
ON     FINAL_CIP_CODE_4 = LCP4_CD
WHERE  FINAL_CIP_CODE_4_NAME is NULL"

## qry_fill_final_CIP2_NAME_and_CLUSTER_Credential ----
## New: fill in CIP2 NAME and cluster code/name of updated CIPs from infoware table
qry_fill_final_CIP2_NAME_and_CLUSTER_Credential <- "
UPDATE Credential_Non_Dup_BGS_IDs
SET    FINAL_CIP_CODE_2_NAME = LCP2_DIGITS_NAME,
       FINAL_CIP_CLUSTER_CODE = LCP2_LCIPPC_CD,
       FINAL_CIP_CLUSTER_NAME = LCP2_LCIPPC_NAME
FROM   Credential_Non_Dup_BGS_IDs INNER JOIN INFOWARE_L_CIP_2DIGITS_CIP2016
ON     FINAL_CIP_CODE_2 = LCP2_CD
WHERE  FINAL_CIP_CODE_2_NAME is NULL"

# PART 5: UPDATE CIPS IN T_BGS_DATA_FINAL ----

## PART 5A ----
## qry_T_BGS_Data_add_columns ----
## New: mimicking Credential_Non_Dup update code
qry_T_BGS_Data_add_columns <- "
ALTER TABLE T_BGS_Data_Final_for_OutcomesMatching
ADD STP_CIP_CODE_4 varchar (255),
    STP_CIP_CODE_4_NAME varchar (255),
    Final_Consider_A_Match varchar (255),
    Final_Probable_Match varchar (255),
    USE_BGS_CIP varchar (255),
    USE_STP_CIP varchar (255),
    FINAL_CIP_CODE_4 varchar (255),
    FINAL_CIP_CODE_4_NAME varchar (255),
    FINAL_CIP_CODE_2 varchar (255),
    FINAL_CIP_CODE_2_NAME varchar (255),
    FINAL_CIP_CLUSTER_CODE varchar (255),
    FINAL_CIP_CLUSTER_NAME varchar (255)"

## qry_update_T_BGS_Data_CIP_matches_step1 ----
## New: mimicking Credential_Non_Dup update code
qry_update_T_BGS_Data_CIP_matches_step1 <- "
UPDATE T_BGS_Data_Final_for_OutcomesMatching
SET    T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CODE_4 = [BGS_Matching_STP_Credential_PEN].[Final_CIP_CODE_4], 
       T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CODE_4_NAME = [BGS_Matching_STP_Credential_PEN].[FINAL_CIP_CODE_4_NAME],
       T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CODE_2 = [BGS_Matching_STP_Credential_PEN].[Final_CIP_CODE_2], 
       T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CODE_2_NAME = [BGS_Matching_STP_Credential_PEN].[FINAL_CIP_CODE_2_NAME],
       T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CLUSTER_CODE = [BGS_Matching_STP_Credential_PEN].[FINAL_CIP_CLUSTER_CODE], 
       T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CLUSTER_NAME = [BGS_Matching_STP_Credential_PEN].[FINAL_CIP_CLUSTER_NAME], 
       T_BGS_Data_Final_for_OutcomesMatching.USE_BGS_CIP = [BGS_Matching_STP_Credential_PEN].[USE_BGS_CIP], 
       T_BGS_Data_Final_for_OutcomesMatching.STP_CIP_CODE_4 = [BGS_Matching_STP_Credential_PEN].[STP_FINAL_CIP_CODE_4],
       T_BGS_Data_Final_for_OutcomesMatching.STP_CIP_CODE_4_NAME = [BGS_Matching_STP_Credential_PEN].[STP_FINAL_CIP_CODE_4_NAME],
       T_BGS_Data_Final_for_OutcomesMatching.Final_Consider_A_Match = [BGS_Matching_STP_Credential_PEN].[Final_Consider_A_Match],
       T_BGS_Data_Final_for_OutcomesMatching.Final_Probable_Match = [BGS_Matching_STP_Credential_PEN].[Final_Probable_Match]
FROM   T_BGS_Data_Final_for_OutcomesMatching INNER JOIN BGS_Matching_STP_Credential_PEN 
ON     T_BGS_Data_Final_for_OutcomesMatching.STQU_ID = BGS_Matching_STP_Credential_PEN.stqu_id 
WHERE  BGS_Matching_STP_Credential_PEN.Final_Consider_A_Match Is Not Null And 
       BGS_Matching_STP_Credential_PEN.Final_Consider_A_Match <>''"

## qry_update_T_BGS_Data_CIP_matches_step2 ----
## New: mimicking Credential_Non_Dup update code
qry_update_T_BGS_Data_CIP_matches_step2 <- "
UPDATE T_BGS_Data_Final_for_OutcomesMatching
SET    T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CODE_4 = [BGS_Matching_STP_Credential_PEN].[Final_CIP_CODE_4], 
       T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CODE_4_NAME = [BGS_Matching_STP_Credential_PEN].[FINAL_CIP_CODE_4_NAME], 
       T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CODE_2 = [BGS_Matching_STP_Credential_PEN].[Final_CIP_CODE_2], 
       T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CODE_2_NAME = [BGS_Matching_STP_Credential_PEN].[FINAL_CIP_CODE_2_NAME], 
       T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CLUSTER_CODE = [BGS_Matching_STP_Credential_PEN].[FINAL_CIP_CLUSTER_CODE],
       T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CLUSTER_NAME = [BGS_Matching_STP_Credential_PEN].[FINAL_CIP_CLUSTER_NAME], 
       T_BGS_Data_Final_for_OutcomesMatching.USE_BGS_CIP = [BGS_Matching_STP_Credential_PEN].[USE_BGS_CIP], 
       T_BGS_Data_Final_for_OutcomesMatching.STP_CIP_CODE_4 = [BGS_Matching_STP_Credential_PEN].[STP_FINAL_CIP_CODE_4], 
       T_BGS_Data_Final_for_OutcomesMatching.STP_CIP_CODE_4_NAME = [BGS_Matching_STP_Credential_PEN].[STP_FINAL_CIP_CODE_4_NAME], 
       T_BGS_Data_Final_for_OutcomesMatching.Final_Probable_Match = [BGS_Matching_STP_Credential_PEN].[Final_Probable_Match]
FROM   T_BGS_Data_Final_for_OutcomesMatching INNER JOIN BGS_Matching_STP_Credential_PEN
ON     T_BGS_Data_Final_for_OutcomesMatching.STQU_ID = BGS_Matching_STP_Credential_PEN.stqu_id 
WHERE (T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CODE_4 Is Null AND 
       T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CODE_4_NAME Is Null AND
       T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CODE_2 Is Null AND 
       T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CODE_2_NAME Is Null AND 
       T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CLUSTER_CODE Is Null AND 
       T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CLUSTER_NAME Is Null AND 
       T_BGS_Data_Final_for_OutcomesMatching.USE_BGS_CIP Is Null AND 
       T_BGS_Data_Final_for_OutcomesMatching.STP_CIP_CODE_4 Is Null AND
       T_BGS_Data_Final_for_OutcomesMatching.STP_CIP_CODE_4_NAME Is Null AND 
       T_BGS_Data_Final_for_OutcomesMatching.Final_Probable_Match Is Null AND
        (BGS_Matching_STP_Credential_PEN.Final_Probable_Match Is Not Null And
         BGS_Matching_STP_Credential_PEN.Final_Probable_Match<>'')
       )"

## qry_update_T_BGS_Data_CIP_matches_step3 ----
## New: update USE_STP_CIP with USE_BGS_CIP
qry_update_T_BGS_Data_CIP_matches_step3 <- "
UPDATE T_BGS_Data_Final_for_OutcomesMatching
SET    USE_STP_CIP = (CASE
                      WHEN (USE_BGS_CIP = 'Yes') THEN 'No'
                      WHEN (USE_BGS_CIP = 'No')  THEN 'Yes'
                      END)
FROM T_BGS_Data_Final_for_OutcomesMatching"

## qry_update_T_BGS_Data_CIP_matches_step4 ----
## New: drop USE_BGS_CIP
qry_update_T_BGS_Data_CIP_matches_step4 <- "
ALTER TABLE T_BGS_Data_Final_for_OutcomesMatching
DROP COLUMN USE_BGS_CIP"

## qry_update_remaining_BGS_CIPs_in_T_BGS_Data_step1 ----
## New: mimicking Credential_Non_Dup update code
qry_update_remaining_BGS_CIPs_in_T_BGS_Data_step1 <- "
UPDATE T_BGS_Data_Final_for_OutcomesMatching 
SET    T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CODE_4 = [T_BGS_Data_Final_for_OutcomesMatching].[CIP_4DIGIT_NO_PERIOD], 
       T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CODE_4_NAME = [T_BGS_Data_Final_for_OutcomesMatching].[CIP4DIG_NAME],
       T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CODE_2 = [T_BGS_Data_Final_for_OutcomesMatching].[CIP2DIG], 
       T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CODE_2_NAME = [T_BGS_Data_Final_for_OutcomesMatching].[CIP2DIG_NAME], 
       T_BGS_Data_Final_for_OutcomesMatching.USE_STP_CIP = 'No because no match'
WHERE  T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CODE_4 Is Null AND
       T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CODE_2 Is Null AND 
       T_BGS_Data_Final_for_OutcomesMatching.Final_Consider_A_Match Is Null AND
       T_BGS_Data_Final_for_OutcomesMatching.Final_Probable_Match Is Null"

## qry_update_remaining_BGS_CIPs_in_T_BGS_Data_step2 ----
## New: mimicking Credential_Non_Dup update code
qry_update_remaining_BGS_CIPs_in_T_BGS_Data_step2 <- "
UPDATE T_BGS_Data_Final_for_OutcomesMatching 
SET    T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CLUSTER_CODE = [INFOWARE_L_CIP_2DIGITS_CIP2016].[LCP2_LCIPPC_CD], 
       T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CLUSTER_NAME = [INFOWARE_L_CIP_2DIGITS_CIP2016].[LCP2_LCIPPC_NAME]
FROM   T_BGS_Data_Final_for_OutcomesMatching INNER JOIN INFOWARE_L_CIP_2DIGITS_CIP2016 
ON     T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CODE_2 = INFOWARE_L_CIP_2DIGITS_CIP2016.LCP2_CD 
WHERE  T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CLUSTER_CODE Is Null AND
       T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CLUSTER_NAME Is Null"

## PART 5B ----
## qry_List_T_BGS_Data_Using_STP_CIPS ----
## New: a query to build this table was missing
qry_List_T_BGS_Data_Using_STP_CIPS <- "
SELECT T_BGS_Data_Final_for_OutcomesMatching.INSTITUTION_CODE, 
       T_BGS_Data_Final_for_OutcomesMatching.CPC,
       T_BGS_Data_Final_for_OutcomesMatching.PROGRAM,
       T_BGS_Data_Final_for_OutcomesMatching.STP_CIP_CODE_4, 
       T_BGS_Data_Final_for_OutcomesMatching.STP_CIP_CODE_4_NAME, 
       T_BGS_Data_Final_for_OutcomesMatching.CIP_4DIGIT_NO_PERIOD, 
       T_BGS_Data_Final_for_OutcomesMatching.CIP4DIG_NAME,
       T_BGS_Data_Final_for_OutcomesMatching.CIP2DIG,
       T_BGS_Data_Final_for_OutcomesMatching.CIP2DIG_NAME,
       T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CODE_4,
       T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CODE_4_NAME,
       T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CODE_2, 
       T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CODE_2_NAME,
       T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CLUSTER_CODE,
       T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CLUSTER_NAME, 
       T_BGS_Data_Final_for_OutcomesMatching.Final_Consider_A_Match, 
       T_BGS_Data_Final_for_OutcomesMatching.Final_Probable_Match, 
       T_BGS_Data_Final_for_OutcomesMatching.USE_STP_CIP,
       Count(*) AS Expr1 
FROM   T_BGS_Data_Final_for_OutcomesMatching
GROUP BY 
       T_BGS_Data_Final_for_OutcomesMatching.INSTITUTION_CODE, 
       T_BGS_Data_Final_for_OutcomesMatching.CPC,
       T_BGS_Data_Final_for_OutcomesMatching.PROGRAM,
       T_BGS_Data_Final_for_OutcomesMatching.STP_CIP_CODE_4, 
       T_BGS_Data_Final_for_OutcomesMatching.STP_CIP_CODE_4_NAME, 
       T_BGS_Data_Final_for_OutcomesMatching.CIP_4DIGIT_NO_PERIOD, 
       T_BGS_Data_Final_for_OutcomesMatching.CIP4DIG_NAME,
       T_BGS_Data_Final_for_OutcomesMatching.CIP2DIG,
       T_BGS_Data_Final_for_OutcomesMatching.CIP2DIG_NAME,
       T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CODE_4,
       T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CODE_4_NAME,
       T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CODE_2, 
       T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CODE_2_NAME,
       T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CLUSTER_CODE,
       T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CLUSTER_NAME, 
       T_BGS_Data_Final_for_OutcomesMatching.Final_Consider_A_Match, 
       T_BGS_Data_Final_for_OutcomesMatching.Final_Probable_Match, 
       T_BGS_Data_Final_for_OutcomesMatching.USE_STP_CIP
HAVING T_BGS_Data_Final_for_OutcomesMatching.USE_STP_CIP='Yes'"

## qry_List_T_BGS_Data_Umatched ----
## New: a query to build this table was missing
qry_List_T_BGS_Data_Umatched <- "
SELECT T_BGS_Data_Final_for_OutcomesMatching.INSTITUTION_CODE, 
       T_BGS_Data_Final_for_OutcomesMatching.CPC,
       T_BGS_Data_Final_for_OutcomesMatching.PROGRAM,
       T_BGS_Data_Final_for_OutcomesMatching.STP_CIP_CODE_4, 
       T_BGS_Data_Final_for_OutcomesMatching.STP_CIP_CODE_4_NAME, 
       T_BGS_Data_Final_for_OutcomesMatching.CIP_4DIGIT_NO_PERIOD, 
       T_BGS_Data_Final_for_OutcomesMatching.CIP4DIG_NAME,
       T_BGS_Data_Final_for_OutcomesMatching.CIP2DIG,
       T_BGS_Data_Final_for_OutcomesMatching.CIP2DIG_NAME,
       T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CODE_4,
       T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CODE_4_NAME,
       T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CODE_2, 
       T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CODE_2_NAME,
       T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CLUSTER_CODE,
       T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CLUSTER_NAME, 
       T_BGS_Data_Final_for_OutcomesMatching.Final_Consider_A_Match, 
       T_BGS_Data_Final_for_OutcomesMatching.Final_Probable_Match, 
       T_BGS_Data_Final_for_OutcomesMatching.USE_STP_CIP,
       Count(*) AS Expr1 
FROM   T_BGS_Data_Final_for_OutcomesMatching
GROUP BY 
       T_BGS_Data_Final_for_OutcomesMatching.INSTITUTION_CODE, 
       T_BGS_Data_Final_for_OutcomesMatching.CPC,
       T_BGS_Data_Final_for_OutcomesMatching.PROGRAM,
       T_BGS_Data_Final_for_OutcomesMatching.STP_CIP_CODE_4, 
       T_BGS_Data_Final_for_OutcomesMatching.STP_CIP_CODE_4_NAME, 
       T_BGS_Data_Final_for_OutcomesMatching.CIP_4DIGIT_NO_PERIOD, 
       T_BGS_Data_Final_for_OutcomesMatching.CIP4DIG_NAME,
       T_BGS_Data_Final_for_OutcomesMatching.CIP2DIG,
       T_BGS_Data_Final_for_OutcomesMatching.CIP2DIG_NAME,
       T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CODE_4,
       T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CODE_4_NAME,
       T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CODE_2, 
       T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CODE_2_NAME,
       T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CLUSTER_CODE,
       T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CLUSTER_NAME, 
       T_BGS_Data_Final_for_OutcomesMatching.Final_Consider_A_Match, 
       T_BGS_Data_Final_for_OutcomesMatching.Final_Probable_Match, 
       T_BGS_Data_Final_for_OutcomesMatching.USE_STP_CIP
HAVING T_BGS_Data_Final_for_OutcomesMatching.USE_STP_CIP='No because no match'"

## qry_update_T_BGS_Data_unmatched ----
## New: update T_BGS_Data_Final_for_OutcomesMatching so unmatched programs use linked STP CIP instead
qry_update_T_BGS_Data_unmatched <-  "
UPDATE T_BGS_Data_Final_for_OutcomesMatching
SET    T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CODE_4 = T_BGS_Data_Unmatched_CIPS_to_update.FINAL_CIP_CODE_4, 
       T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CODE_4_NAME = NULL,
       T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CODE_2 = T_BGS_Data_Unmatched_CIPS_to_update.FINAL_CIP_CODE_2, 
       T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CODE_2_NAME = NULL, 
       T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CLUSTER_CODE = NULL, 
       T_BGS_Data_Final_for_OutcomesMatching.FINAL_CIP_CLUSTER_NAME = NULL
FROM   T_BGS_Data_Final_for_OutcomesMatching INNER JOIN T_BGS_Data_Unmatched_CIPS_to_update
ON     T_BGS_Data_Final_for_OutcomesMatching.PROGRAM = T_BGS_Data_Unmatched_CIPS_to_update.PROGRAM
WHERE  Final_Consider_A_Match is NULL AND Final_Probable_Match is NULL"

## qry_fill_final_CIP4_NAME_T_BGS_Data ----
## New: fill in CIP4 NAME of updated CIPs by linking to infoware table
qry_fill_final_CIP4_NAME_T_BGS_Data <- "
UPDATE T_BGS_Data_Final_for_OutcomesMatching
SET    FINAL_CIP_CODE_4_NAME = LCP4_CIP_4DIGITS_NAME
FROM   T_BGS_Data_Final_for_OutcomesMatching INNER JOIN INFOWARE_L_CIP_4DIGITS_CIP2016
ON     FINAL_CIP_CODE_4 = LCP4_CD
WHERE  FINAL_CIP_CODE_4_NAME is NULL"

## qry_fill_final_CIP2_NAME_and_CLUSTER_T_BGS_Data ----
## New: fill in CIP2 NAME and cluster code/name of updated CIPs from infoware table
qry_fill_final_CIP2_NAME_and_CLUSTER_T_BGS_Data <- "
UPDATE T_BGS_Data_Final_for_OutcomesMatching
SET    FINAL_CIP_CODE_2_NAME = LCP2_DIGITS_NAME,
       FINAL_CIP_CLUSTER_CODE = LCP2_LCIPPC_CD,
       FINAL_CIP_CLUSTER_NAME = LCP2_LCIPPC_NAME
FROM   T_BGS_Data_Final_for_OutcomesMatching INNER JOIN INFOWARE_L_CIP_2DIGITS_CIP2016
ON     FINAL_CIP_CODE_2 = LCP2_CD
WHERE  FINAL_CIP_CODE_2_NAME is NULL"

# end ----



