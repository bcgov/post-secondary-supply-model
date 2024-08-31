# ******************************************************************************
# ---- Part 1: Update XWALK with new DACSO data ----
# All SQL queries replaced with R code
# ******************************************************************************

# ******************************************************************************
# ---- Part 2: Update XWALK with new STP Credential data ----

## qry_DASCO_STP_Credential_Programs ----
# select PSI_CODE, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESC, PSI_CREDENTIAL_CIP, PSI_CREDENTIAL_LEVEL, PSI_CREDENTIAL_CATEGORY, OUTCOMES_CRED
# filter where OUTCOMES_CRED = DACSO
# summarize (count) to get distinct rows by these variables
qry_DASCO_STP_Credential_Programs <- 
"SELECT PSI_CODE, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION, PSI_CREDENTIAL_CIP,
       PSI_CREDENTIAL_LEVEL, PSI_CREDENTIAL_CATEGORY, OUTCOMES_CRED, COUNT(*) AS Expr1
INTO   STP_Credential_Non_Dup_Programs_DACSO
FROM   Credential_Non_Dup
GROUP BY PSI_CODE, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION, PSI_CREDENTIAL_CIP,
         PSI_CREDENTIAL_LEVEL, PSI_CREDENTIAL_CATEGORY, OUTCOMES_CRED
HAVING OUTCOMES_CRED = 'DACSO'"

## qry_DASCO_STP_Credential_Programs_Add_Columns ----
qry_DASCO_STP_Credential_Programs_Add_Columns <- 
"ALTER TABLE STP_Credential_Non_Dup_Programs_DACSO
ADD
 OUTCOMES_CIP_CODE_4 varchar(4),
 OUTCOMES_CIP_CODE_4_NAME varchar(255),
 FINAL_CIP_CODE_4 varchar(4),
 FINAL_CIP_CODE_4_NAME varchar(255),
 FINAL_CIP_CODE_2 varchar(2),
 FINAL_CIP_CODE_2_NAME varchar(255),
 FINAL_CIP_CLUSTER_CODE varchar(10),
 FINAL_CIP_CLUSTER_NAME varchar(255),
 STP_CIP_CODE_4 varchar(4),
 STP_CIP_CODE_4_NAME varchar(255),
 Already_Matched VARCHAR(255), 
 New_Auto_Match VARCHAR(255), 
 New_Manual_Match VARCHAR(255),
 COCI_INST_CD VARCHAR(255)"

## qry_Update_STP_CIP_CODE4 ----
# take the STP information from the infoware tables and add to the STP_Credential_Non_Dup_Programs_DACSO
qry_Update_STP_CIP_CODE4 <- 
  "UPDATE STP_Credential_Non_Dup_Programs_DACSO
  SET STP_Credential_Non_Dup_Programs_DACSO.STP_CIP_CODE_4 = [INFOWARE_L_CIP_6DIGITS_CIP2016].[LCIP_LCP4_CD], 
STP_Credential_Non_Dup_Programs_DACSO.STP_CIP_CODE_4_NAME = [INFOWARE_L_CIP_4DIGITS_CIP2016].[LCP4_CIP_4DIGITS_NAME]
FROM INFOWARE_L_CIP_4DIGITS_CIP2016 INNER JOIN (STP_Credential_Non_Dup_Programs_DACSO 
INNER JOIN INFOWARE_L_CIP_6DIGITS_CIP2016 
ON STP_Credential_Non_Dup_Programs_DACSO.PSI_CREDENTIAL_CIP = INFOWARE_L_CIP_6DIGITS_CIP2016.LCIP_CD_WITH_PERIOD) 
ON INFOWARE_L_CIP_4DIGITS_CIP2016.LCP4_CD = INFOWARE_L_CIP_6DIGITS_CIP2016.LCIP_LCP4_CD"

## qry_STP_Credential_DACSO_Programs_AlreadyMatched ----
## initial program matching:
## if STP program is already in the XWALK (joining on PSI_CREDENTIAL, PSI_PROGRAM_CODE, PSI_CODE), 
## copy CIP_CODE_4, LCP4_CIP_4DIGITS_NAME into OUTCOMES_CIP_CODE_4 and OUTCOMES_CIP_CODE_4_NAME
## set Already_Matched to Yes
qry_STP_Credential_DACSO_Programs_AlreadyMatched <- 
  "UPDATE STP_Credential_Non_Dup_Programs_DACSO
SET 
  STP_Credential_Non_Dup_Programs_DACSO.Already_Matched = 'Yes', 
  STP_Credential_Non_Dup_Programs_DACSO.OUTCOMES_CIP_CODE_4 = [DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23].[CIP_CODE_4],
  STP_Credential_Non_Dup_Programs_DACSO.OUTCOMES_CIP_CODE_4_NAME = [DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23].[LCP4_CIP_4DIGITS_NAME]
FROM STP_Credential_Non_Dup_Programs_DACSO INNER JOIN DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 
ON (
  STP_Credential_Non_Dup_Programs_DACSO.PSI_CREDENTIAL_PROGRAM_DESCRIPTION = DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23.PSI_CREDENTIAL_PROGRAM_DESC) AND
(STP_Credential_Non_Dup_Programs_DACSO.PSI_PROGRAM_CODE = DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23.PSI_PROGRAM_CODE) AND
(STP_Credential_Non_Dup_Programs_DACSO.PSI_CODE = DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23.PSI_CODE
)"

## qry_STP_Credential_DACSO_Programs_AlreadyMatched_b ----
## secondary program matching:
## if STP program is already in the XWALK (joining on COCI_INST_CD, PSI_CREDENTIAL_PROGRAM_DESC, PSI_PROGRAM_CODE), 
## where Already_Matched, OUTCOMES_CIP_CODE_4 and OUTCOMES_CIP_CODE_4_NAME is Null
## copy CIP_CODE_4, LCP4_CIP_4DIGITS_NAME into OUTCOMES_CIP_CODE_4 and OUTCOMES_CIP_CODE_4_NAME
## set Already_Matched to Yes
qry_STP_Credential_DACSO_Programs_AlreadyMatched_b <- 
  "UPDATE STP_Credential_Non_Dup_Programs_DACSO 
SET 
  STP_Credential_Non_Dup_Programs_DACSO.Already_Matched = 'Yes', 
  STP_Credential_Non_Dup_Programs_DACSO.OUTCOMES_CIP_CODE_4 = [DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23].[CIP_CODE_4], 
  STP_Credential_Non_Dup_Programs_DACSO.OUTCOMES_CIP_CODE_4_NAME = [DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23].[LCP4_CIP_4DIGITS_NAME]
FROM STP_Credential_Non_Dup_Programs_DACSO INNER JOIN DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 
ON (
  STP_Credential_Non_Dup_Programs_DACSO.COCI_INST_CD = DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23.COCI_INST_CD) AND
  (STP_Credential_Non_Dup_Programs_DACSO.PSI_CREDENTIAL_PROGRAM_DESCRIPTION = DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23.PSI_CREDENTIAL_PROGRAM_DESC) AND
  (STP_Credential_Non_Dup_Programs_DACSO.PSI_PROGRAM_CODE = DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23.PSI_PROGRAM_CODE)
WHERE 
  (((STP_Credential_Non_Dup_Programs_DACSO.Already_Matched) Is Null) AND
  ((STP_Credential_Non_Dup_Programs_DACSO.OUTCOMES_CIP_CODE_4) Is Null) AND 
  ((STP_Credential_Non_Dup_Programs_DACSO.OUTCOMES_CIP_CODE_4_NAME) Is Null))"

## qry_STP_Credential_DACSO_Programs_NewMatches_a ----
## new matches where STP program info is the same as DACSO info:
## join to XWALK on PSI_CODE, PSI_CREDENTIAL_PROGRAM_DESC = PRGM_INST_PROGRAM_NAME, PSI_PROGRAM_CODE = PRGM_LCPC_CD
## where Already_Matched is null
## copy CIP_CODE_4, LCP4_CIP_4DIGITS_NAME into OUTCOMES_CIP_CODE_4 and OUTCOMES_CIP_CODE_4_NAME
## set New_Auto_Match to Yes
qry_STP_Credential_DACSO_Programs_NewMatches_a <- 
  "UPDATE STP_Credential_Non_Dup_Programs_DACSO 
SET 
  STP_Credential_Non_Dup_Programs_DACSO.OUTCOMES_CIP_CODE_4 = [DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23].[CIP_CODE_4], 
  STP_Credential_Non_Dup_Programs_DACSO.OUTCOMES_CIP_CODE_4_NAME = [DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23].[LCP4_CIP_4DIGITS_NAME],
  STP_Credential_Non_Dup_Programs_DACSO.New_Auto_Match = 'Yes'
FROM STP_Credential_Non_Dup_Programs_DACSO INNER JOIN DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 
ON (
  STP_Credential_Non_Dup_Programs_DACSO.PSI_CREDENTIAL_PROGRAM_DESCRIPTION = DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23.PRGM_INST_PROGRAM_NAME) AND
  (STP_Credential_Non_Dup_Programs_DACSO.PSI_PROGRAM_CODE = DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23.PRGM_LCPC_CD) AND
  (STP_Credential_Non_Dup_Programs_DACSO.PSI_CODE = DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23.PSI_CODE)
WHERE (((STP_Credential_Non_Dup_Programs_DACSO.Already_Matched) Is Null))"

## qry_STP_Credential_DACSO_Programs_NewMatches_a_step2 ----
## secondary new matches where STP program info is the same as DACSO info:
## join to XWALK on COCI_INST_CD, PSI_CREDENTIAL_PROGRAM_DESC = PRGM_INST_PROGRAM_NAME, PSI_PROGRAM_CODE = PRGM_LCPC_CD
## where New_Auto_Match, Already_Matched, OUTCOMES_CIP_CODE_4 and OUTCOMES_CIP_CODE_4_NAME is Null
## copy CIP_CODE_4, LCP4_CIP_4DIGITS_NAME into OUTCOMES_CIP_CODE_4 and OUTCOMES_CIP_CODE_4_NAME
## set New_Auto_Match to Yes
qry_STP_Credential_DACSO_Programs_NewMatches_a_step2 <- 
  "UPDATE STP_Credential_Non_Dup_Programs_DACSO
SET 
  STP_Credential_Non_Dup_Programs_DACSO.OUTCOMES_CIP_CODE_4 = [DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23].[CIP_CODE_4], 
  STP_Credential_Non_Dup_Programs_DACSO.OUTCOMES_CIP_CODE_4_NAME = [DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23].[LCP4_CIP_4DIGITS_NAME], 
  STP_Credential_Non_Dup_Programs_DACSO.New_Auto_Match = 'Yes'
FROM STP_Credential_Non_Dup_Programs_DACSO INNER JOIN DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 
ON (
  STP_Credential_Non_Dup_Programs_DACSO.COCI_INST_CD = DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23.COCI_INST_CD) AND
  (STP_Credential_Non_Dup_Programs_DACSO.PSI_CREDENTIAL_PROGRAM_DESCRIPTION = DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23.PRGM_INST_PROGRAM_NAME) AND
  (STP_Credential_Non_Dup_Programs_DACSO.PSI_PROGRAM_CODE = DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23.PRGM_LCPC_CD)
WHERE 
  (((STP_Credential_Non_Dup_Programs_DACSO.OUTCOMES_CIP_CODE_4) Is Null) AND
  ((STP_Credential_Non_Dup_Programs_DACSO.OUTCOMES_CIP_CODE_4_NAME) Is Null) AND
  ((STP_Credential_Non_Dup_Programs_DACSO.New_Auto_Match) Is Null) AND 
  ((STP_Credential_Non_Dup_Programs_DACSO.Already_Matched) Is Null))"

## qry_STP_Credential_DACSO_Programs_NewMatches_b ----
## join on PSI_CODE, PSI_CREDENTIAL_PROGRAM_DESC = PRGM_INST_PROGRAM_NAME, PSI_PROGRAM_CODE = PRGM_LCPC_CD
## where New_Auto_Match = Yes
## copy PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESC, STP_CIP4_CODE into STP_CIP_CODE_4, CTP_SIP4_NAME into STP_CIP_CODE_4_NAME
## set New_STP_Program20XX = Yes and One_To_One_Match = Yes20XX
qry_STP_Credential_DACSO_Programs_NewMatches_b <- 
  "UPDATE DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23
SET 
  DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23.PSI_PROGRAM_CODE = [STP_Credential_Non_Dup_Programs_DACSO].[PSI_PROGRAM_CODE],
  DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23.PSI_CREDENTIAL_PROGRAM_DESC = [STP_Credential_Non_Dup_Programs_DACSO].[PSI_CREDENTIAL_PROGRAM_DESCRIPTION], 
  DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23.STP_CIP4_CODE = [STP_Credential_Non_Dup_Programs_DACSO].[STP_CIP_CODE_4], 
  DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23.STP_CIP4_NAME = [STP_Credential_Non_Dup_Programs_DACSO].[STP_CIP_CODE_4_NAME], 
  DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23.New_STP_Program2021_23 = 'Yes', 
  DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23.One_To_One_Match = 'Yes2021_23'
FROM STP_Credential_Non_Dup_Programs_DACSO INNER JOIN DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23
ON (
  STP_Credential_Non_Dup_Programs_DACSO.PSI_CREDENTIAL_PROGRAM_DESCRIPTION = DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23.PRGM_INST_PROGRAM_NAME) AND 
  (STP_Credential_Non_Dup_Programs_DACSO.PSI_PROGRAM_CODE = DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23.PRGM_LCPC_CD) AND 
  (STP_Credential_Non_Dup_Programs_DACSO.PSI_CODE = DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23.PSI_CODE)
WHERE (((STP_Credential_Non_Dup_Programs_DACSO.New_Auto_Match)='Yes'))"

## qry_STP_Credential_DACSO_Programs_NewMatches_b_step2 ----
## secondary update XWALK with new STP matches:
## join on COCI_INST_CD, PSI_CREDENTIAL_PROGRAM_DESC = PRGM_INST_PROGRAM_NAME, PSI_PROGRAM_CODE = PRGM_LCPC_CD
## where New_Auto_Match = Yes and New_STP_Program20XX and One_To_One_Match are null
## copy PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESC, STP_CIP4_CODE into STP_CIP_CODE_4, CTP_SIP4_NAME into STP_CIP_CODE_4_NAME
## set New_STP_Program20XX = Yes and One_To_One_Match = Yes20XX
qry_STP_Credential_DACSO_Programs_NewMatches_b_step2 <- 
  "UPDATE DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23
SET 
  DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23.PSI_PROGRAM_CODE = [STP_Credential_Non_Dup_Programs_DACSO].[PSI_PROGRAM_CODE], 
  DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23.PSI_CREDENTIAL_PROGRAM_DESC = [STP_Credential_Non_Dup_Programs_DACSO].[PSI_CREDENTIAL_PROGRAM_DESCRIPTION], 
  DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23.STP_CIP4_CODE = [STP_Credential_Non_Dup_Programs_DACSO].[STP_CIP_CODE_4], 
  DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23.STP_CIP4_NAME = [STP_Credential_Non_Dup_Programs_DACSO].[STP_CIP_CODE_4_NAME], 
  DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23.New_STP_Program2021_23 = 'Yes', 
  DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23.One_To_One_Match = 'Yes2021_23'
FROM STP_Credential_Non_Dup_Programs_DACSO INNER JOIN DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 
ON (
  STP_Credential_Non_Dup_Programs_DACSO.COCI_INST_CD = DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23.COCI_INST_CD) AND
  (STP_Credential_Non_Dup_Programs_DACSO.PSI_CREDENTIAL_PROGRAM_DESCRIPTION = DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23.PRGM_INST_PROGRAM_NAME) AND
  (STP_Credential_Non_Dup_Programs_DACSO.PSI_PROGRAM_CODE = DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23.PRGM_LCPC_CD)
WHERE 
  (((DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23.New_STP_Program2021_23) Is Null) AND
  ((DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23.One_To_One_Match) Is Null) AND
  ((STP_Credential_Non_Dup_Programs_DACSO.New_Auto_Match)='Yes'))"

# ******************************************************************************




# ******************************************************************************
# ---- Part 3: Manual/custom STP to XWALK matching ----

## qry_Update_BCIT_Programs ----
# join STP data to XWALK on COCI_INST_CD, PSI_CREDENTIAL_PROGRAM_DESCRIPTION = PRGM_INST_PROGRAM_NAME, BCIT_TEST_PROGRAM_CODE = PRGM_LCPC_CD
# - set New_Auto_Match = 'YesXXBCIT'
qry_Update_BCIT_Programs <- 
"UPDATE STP_Credential_Non_Dup_Programs_DACSO
SET STP_Credential_Non_Dup_Programs_DACSO.OUTCOMES_CIP_CODE_4 = [DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23].[CIP_CODE_4], 
STP_Credential_Non_Dup_Programs_DACSO.OUTCOMES_CIP_CODE_4_NAME = [DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23].[LCP4_CIP_4DIGITS_NAME], 
STP_Credential_Non_Dup_Programs_DACSO.New_Auto_Match = 'Yes2021_23BCIT'
FROM STP_Credential_Non_Dup_Programs_DACSO INNER JOIN DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 
ON (STP_Credential_Non_Dup_Programs_DACSO.COCI_INST_CD = DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23.COCI_INST_CD) AND 
(STP_Credential_Non_Dup_Programs_DACSO.BCIT_TEST_PROGRAM_CODE = DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23.PRGM_LCPC_CD) AND 
(STP_Credential_Non_Dup_Programs_DACSO.PSI_CREDENTIAL_PROGRAM_DESCRIPTION = DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23.PRGM_INST_PROGRAM_NAME) 
WHERE (((STP_Credential_Non_Dup_Programs_DACSO.OUTCOMES_CIP_CODE_4) Is Null) AND
((STP_Credential_Non_Dup_Programs_DACSO.OUTCOMES_CIP_CODE_4_NAME) Is Null) AND
((STP_Credential_Non_Dup_Programs_DACSO.New_Auto_Match) Is Null))"

## qry_Update_BCIT_Programs_b ----
# join STP data to XWALK on COCI_INST_CD, BCIT_TEST_PROGRAM_CODE = PRGM_LCPC_CD
# - set New_Auto_Match = 'YesXXBCIT'
qry_Update_BCIT_Programs_b <- 
"UPDATE STP_Credential_Non_Dup_Programs_DACSO
SET STP_Credential_Non_Dup_Programs_DACSO.OUTCOMES_CIP_CODE_4 = [DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23].[CIP_CODE_4], 
STP_Credential_Non_Dup_Programs_DACSO.OUTCOMES_CIP_CODE_4_NAME = [DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23].[LCP4_CIP_4DIGITS_NAME], 
STP_Credential_Non_Dup_Programs_DACSO.New_Auto_Match = 'Yes2021_23BCIT'
FROM STP_Credential_Non_Dup_Programs_DACSO INNER JOIN DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23
ON (STP_Credential_Non_Dup_Programs_DACSO.COCI_INST_CD = DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23.COCI_INST_CD) AND 
(STP_Credential_Non_Dup_Programs_DACSO.BCIT_TEST_PROGRAM_CODE = DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23.PRGM_LCPC_CD) 
WHERE (((STP_Credential_Non_Dup_Programs_DACSO.OUTCOMES_CIP_CODE_4) Is Null) AND 
((STP_Credential_Non_Dup_Programs_DACSO.OUTCOMES_CIP_CODE_4_NAME) Is Null) AND 
((STP_Credential_Non_Dup_Programs_DACSO.New_Auto_Match) Is Null))"

## qry_Update_CAPU_Programs_a ----
qry_Update_CAPU_Programs_a <- 
"UPDATE STP_Credential_Non_Dup_Programs_DACSO 
SET STP_Credential_Non_Dup_Programs_DACSO.OUTCOMES_CIP_CODE_4 = [DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23].[CIP_CODE_4], 
STP_Credential_Non_Dup_Programs_DACSO.OUTCOMES_CIP_CODE_4_NAME = [DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23].[LCP4_CIP_4DIGITS_NAME], 
STP_Credential_Non_Dup_Programs_DACSO.New_Auto_Match = 'Yes2021_23CAPU'
FROM STP_Credential_Non_Dup_Programs_DACSO INNER JOIN DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 
ON (STP_Credential_Non_Dup_Programs_DACSO.PSI_CREDENTIAL_PROGRAM_DESCRIPTION = DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23.PRGM_INST_PROGRAM_NAME) AND 
(STP_Credential_Non_Dup_Programs_DACSO.COCI_INST_CD = DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23.COCI_INST_CD) AND 
(STP_Credential_Non_Dup_Programs_DACSO.CAP_TEST_PROGRAM_CODE = DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23.PRGM_LCPC_CD) 
WHERE (((STP_Credential_Non_Dup_Programs_DACSO.OUTCOMES_CIP_CODE_4) Is Null) AND 
((STP_Credential_Non_Dup_Programs_DACSO.OUTCOMES_CIP_CODE_4_NAME) Is Null) AND 
((STP_Credential_Non_Dup_Programs_DACSO.New_Auto_Match) Is Null))"

## qry_Update_CAPU_Programs_b ----
qry_Update_CAPU_Programs_b <- 
  "UPDATE STP_Credential_Non_Dup_Programs_DACSO
SET STP_Credential_Non_Dup_Programs_DACSO.OUTCOMES_CIP_CODE_4 = [DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23].[CIP_CODE_4], 
STP_Credential_Non_Dup_Programs_DACSO.OUTCOMES_CIP_CODE_4_NAME = [DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23].[LCP4_CIP_4DIGITS_NAME], 
STP_Credential_Non_Dup_Programs_DACSO.New_Auto_Match = 'Yes2021_23CAPU'
FROM STP_Credential_Non_Dup_Programs_DACSO INNER JOIN DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23
ON (STP_Credential_Non_Dup_Programs_DACSO.COCI_INST_CD = DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23.COCI_INST_CD) AND 
(STP_Credential_Non_Dup_Programs_DACSO.CAP_TEST_PROGRAM_CODE = DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23.PRGM_LCPC_CD) 
WHERE (((STP_Credential_Non_Dup_Programs_DACSO.OUTCOMES_CIP_CODE_4) Is Null) AND 
((STP_Credential_Non_Dup_Programs_DACSO.OUTCOMES_CIP_CODE_4_NAME) Is Null) AND 
((STP_Credential_Non_Dup_Programs_DACSO.New_Auto_Match) Is Null))"

## qry_Update_VIU_Programs_a ----
qry_Update_VIU_Programs_a <- 
  "UPDATE STP_Credential_Non_Dup_Programs_DACSO 
SET STP_Credential_Non_Dup_Programs_DACSO.OUTCOMES_CIP_CODE_4 = [DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23].[CIP_CODE_4], 
STP_Credential_Non_Dup_Programs_DACSO.OUTCOMES_CIP_CODE_4_NAME = [DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23].[LCP4_CIP_4DIGITS_NAME], 
STP_Credential_Non_Dup_Programs_DACSO.New_Auto_Match = 'Yes2021_23VIU'
FROM STP_Credential_Non_Dup_Programs_DACSO INNER JOIN DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 
ON (STP_Credential_Non_Dup_Programs_DACSO.PSI_CREDENTIAL_PROGRAM_DESCRIPTION = DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23.PRGM_INST_PROGRAM_NAME) AND 
(STP_Credential_Non_Dup_Programs_DACSO.COCI_INST_CD = DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23.COCI_INST_CD) AND 
(STP_Credential_Non_Dup_Programs_DACSO.VIU_TEST_PROGRAM_CODE = DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23.PRGM_LCPC_CD) 
WHERE (((STP_Credential_Non_Dup_Programs_DACSO.OUTCOMES_CIP_CODE_4) Is Null) AND 
((STP_Credential_Non_Dup_Programs_DACSO.OUTCOMES_CIP_CODE_4_NAME) Is Null) AND 
((STP_Credential_Non_Dup_Programs_DACSO.New_Auto_Match) Is Null))"

## qry_Update_VIU_Programs_b ----
qry_Update_VIU_Programs_b <- 
  "UPDATE STP_Credential_Non_Dup_Programs_DACSO
SET STP_Credential_Non_Dup_Programs_DACSO.OUTCOMES_CIP_CODE_4 = [DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23].[CIP_CODE_4], 
STP_Credential_Non_Dup_Programs_DACSO.OUTCOMES_CIP_CODE_4_NAME = [DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23].[LCP4_CIP_4DIGITS_NAME], 
STP_Credential_Non_Dup_Programs_DACSO.New_Auto_Match = 'Yes2021_23VIU'
FROM STP_Credential_Non_Dup_Programs_DACSO INNER JOIN DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23
ON (STP_Credential_Non_Dup_Programs_DACSO.COCI_INST_CD = DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23.COCI_INST_CD) AND 
(STP_Credential_Non_Dup_Programs_DACSO.VIU_TEST_PROGRAM_CODE = DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23.PRGM_LCPC_CD) 
WHERE (((STP_Credential_Non_Dup_Programs_DACSO.OUTCOMES_CIP_CODE_4) Is Null) AND 
((STP_Credential_Non_Dup_Programs_DACSO.OUTCOMES_CIP_CODE_4_NAME) Is Null) AND 
((STP_Credential_Non_Dup_Programs_DACSO.New_Auto_Match) Is Null))"

## qry_Update_Remaining_Programs_Matching_DACSO_Seen ----
qry_Update_Remaining_Programs_Matching_DACSO_Seen <- 
  "UPDATE STP_Credential_Non_Dup_Programs_DACSO 
    SET STP_Credential_Non_Dup_Programs_DACSO.OUTCOMES_CIP_CODE_4 = [DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23].[CIP_CODE_4], 
  STP_Credential_Non_Dup_Programs_DACSO.OUTCOMES_CIP_CODE_4_NAME = [DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23].[LCP4_CIP_4DIGITS_NAME], 
  STP_Credential_Non_Dup_Programs_DACSO.New_Auto_Match = 'Yes_2021_23test'
  FROM STP_Credential_Non_Dup_Programs_DACSO INNER JOIN DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 
  ON (STP_Credential_Non_Dup_Programs_DACSO.COCI_INST_CD = DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23.COCI_INST_CD) 
  AND (STP_Credential_Non_Dup_Programs_DACSO.PSI_PROGRAM_CODE = DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23.PRGM_LCPC_CD) 
WHERE (((STP_Credential_Non_Dup_Programs_DACSO.OUTCOMES_CIP_CODE_4) Is Null) 
AND ((STP_Credential_Non_Dup_Programs_DACSO.OUTCOMES_CIP_CODE_4_NAME) Is Null) 
AND ((STP_Credential_Non_Dup_Programs_DACSO.New_Auto_Match) Is Null))"

## qry_Update_Remaining_Programs_Matching_DACSO_Seen_b ----
qry_Update_Remaining_Programs_Matching_DACSO_Seen_b <- 
  "UPDATE STP_Credential_Non_Dup_Programs_DACSO 
    SET STP_Credential_Non_Dup_Programs_DACSO.OUTCOMES_CIP_CODE_4 = [DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23].[CIP_CODE_4], 
  STP_Credential_Non_Dup_Programs_DACSO.OUTCOMES_CIP_CODE_4_NAME = [DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23].[LCP4_CIP_4DIGITS_NAME], 
  STP_Credential_Non_Dup_Programs_DACSO.New_Auto_Match = 'Yes_2021_23test'
  FROM STP_Credential_Non_Dup_Programs_DACSO INNER JOIN DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 
  ON (STP_Credential_Non_Dup_Programs_DACSO.COCI_INST_CD = DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23.COCI_INST_CD) 
  AND (STP_Credential_Non_Dup_Programs_DACSO.PSI_CREDENTIAL_PROGRAM_DESCRIPTION = DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23.PRGM_INST_PROGRAM_NAME) 
WHERE (((STP_Credential_Non_Dup_Programs_DACSO.OUTCOMES_CIP_CODE_4) Is Null) 
AND ((STP_Credential_Non_Dup_Programs_DACSO.OUTCOMES_CIP_CODE_4_NAME) Is Null) 
AND ((STP_Credential_Non_Dup_Programs_DACSO.New_Auto_Match) Is Null))"

# ******************************************************************************




# ******************************************************************************
# ---- Part 4: Final update to STP CIPs ----
## qry_Update_STP_Cred_Non_Dup_Programs_DACSO_FinalCIP4_a ----
# Use the outcomes cip4 data if there was a match for the final cip4
qry_Update_STP_Cred_Non_Dup_Programs_DACSO_FinalCIP4_a <- 
  "UPDATE STP_Credential_Non_Dup_Programs_DACSO 
SET STP_Credential_Non_Dup_Programs_DACSO.FINAL_CIP_CODE_4 = [STP_Credential_Non_Dup_Programs_DACSO].[OUTCOMES_CIP_CODE_4], 
STP_Credential_Non_Dup_Programs_DACSO.FINAL_CIP_CODE_4_NAME = [STP_Credential_Non_Dup_Programs_DACSO].[OUTCOMES_CIP_CODE_4_NAME]
WHERE (((STP_Credential_Non_Dup_Programs_DACSO.OUTCOMES_CIP_CODE_4) Is Not Null) AND 
((STP_Credential_Non_Dup_Programs_DACSO.OUTCOMES_CIP_CODE_4_NAME) Is Not Null))"


## qry_Update_STP_Cred_Non_Dup_Programs_DACSO_FinalCIP4_b ----
# Use the STP CIP4 outcomes for the rest where there is no match
# populate the final cip code 2
qry_Update_STP_Cred_Non_Dup_Programs_DACSO_FinalCIP4_b <- 
  "UPDATE STP_Credential_Non_Dup_Programs_DACSO 
SET STP_Credential_Non_Dup_Programs_DACSO.FINAL_CIP_CODE_4 = [INFOWARE_L_CIP_6DIGITS_CIP2016].[LCIP_LCP4_CD], 
STP_Credential_Non_Dup_Programs_DACSO.FINAL_CIP_CODE_4_NAME = [INFOWARE_L_CIP_4DIGITS_CIP2016].[LCP4_CIP_4DIGITS_NAME], 
STP_Credential_Non_Dup_Programs_DACSO.FINAL_CIP_CODE_2 = [INFOWARE_L_CIP_6DIGITS_CIP2016].[LCIP_LCP2_CD], 
STP_Credential_Non_Dup_Programs_DACSO.FINAL_CIP_CODE_2_NAME = [INFOWARE_L_CIP_2DIGITS_CIP2016].[LCP2_DIGITS_NAME]
  FROM (STP_Credential_Non_Dup_Programs_DACSO INNER JOIN (INFOWARE_L_CIP_6DIGITS_CIP2016 INNER JOIN INFOWARE_L_CIP_2DIGITS_CIP2016 
  ON INFOWARE_L_CIP_6DIGITS_CIP2016.LCIP_LCP2_CD = INFOWARE_L_CIP_2DIGITS_CIP2016.LCP2_CD) 
  ON STP_Credential_Non_Dup_Programs_DACSO.PSI_CREDENTIAL_CIP = INFOWARE_L_CIP_6DIGITS_CIP2016.LCIP_CD_WITH_PERIOD) 
  INNER JOIN INFOWARE_L_CIP_4DIGITS_CIP2016 
  ON INFOWARE_L_CIP_6DIGITS_CIP2016.LCIP_LCP4_CD = INFOWARE_L_CIP_4DIGITS_CIP2016.LCP4_CD 
WHERE (((STP_Credential_Non_Dup_Programs_DACSO.FINAL_CIP_CODE_4) Is Null) 
AND ((STP_Credential_Non_Dup_Programs_DACSO.FINAL_CIP_CODE_4_NAME) Is Null) 
AND ((STP_Credential_Non_Dup_Programs_DACSO.FINAL_CIP_CODE_2) Is Null) 
AND ((STP_Credential_Non_Dup_Programs_DACSO.FINAL_CIP_CODE_2_NAME) Is Null) 
AND ((STP_Credential_Non_Dup_Programs_DACSO.OUTCOMES_CIP_CODE_4) Is Null) 
AND ((STP_Credential_Non_Dup_Programs_DACSO.OUTCOMES_CIP_CODE_4_NAME) Is Null))"

## qry_Update_STP_Cred_Non_Dup_Programs_DACSO_FinalCIP4_c ----
qry_Update_STP_Cred_Non_Dup_Programs_DACSO_FinalCIP4_c <- 
  "UPDATE STP_Credential_Non_Dup_Programs_DACSO 
  SET STP_Credential_Non_Dup_Programs_DACSO.FINAL_CIP_CODE_4_NAME = [INFOWARE_L_CIP_4DIGITS_CIP2016].[LCP4_CIP_4DIGITS_NAME], 
  STP_Credential_Non_Dup_Programs_DACSO.FINAL_CIP_CODE_2 = [INFOWARE_L_CIP_6DIGITS_CIP2016].[LCIP_LCP2_CD], 
  STP_Credential_Non_Dup_Programs_DACSO.FINAL_CIP_CODE_2_NAME = [INFOWARE_L_CIP_2DIGITS_CIP2016].[LCP2_DIGITS_NAME]
FROM STP_Credential_Non_Dup_Programs_DACSO INNER JOIN ((INFOWARE_L_CIP_6DIGITS_CIP2016 INNER JOIN INFOWARE_L_CIP_2DIGITS_CIP2016 
ON INFOWARE_L_CIP_6DIGITS_CIP2016.LCIP_LCP2_CD = INFOWARE_L_CIP_2DIGITS_CIP2016.LCP2_CD) INNER JOIN INFOWARE_L_CIP_4DIGITS_CIP2016 
ON INFOWARE_L_CIP_6DIGITS_CIP2016.LCIP_LCP4_CD = INFOWARE_L_CIP_4DIGITS_CIP2016.LCP4_CD) 
ON STP_Credential_Non_Dup_Programs_DACSO.FINAL_CIP_CODE_4 = INFOWARE_L_CIP_6DIGITS_CIP2016.LCIP_LCP4_CD 
WHERE (((STP_Credential_Non_Dup_Programs_DACSO.FINAL_CIP_CODE_4_NAME) Is Null) 
AND ((STP_Credential_Non_Dup_Programs_DACSO.FINAL_CIP_CODE_2) Is Null) 
AND ((STP_Credential_Non_Dup_Programs_DACSO.FINAL_CIP_CODE_2_NAME) Is Null) 
AND ((STP_Credential_Non_Dup_Programs_DACSO.OUTCOMES_CIP_CODE_4) Is Null) 
AND ((STP_Credential_Non_Dup_Programs_DACSO.OUTCOMES_CIP_CODE_4_NAME) Is Null))"

## qry_Update_STP_Cred_Non_Dup_Programs_DACSO_FinalCIP2_Cluster_a ----
qry_Update_STP_Cred_Non_Dup_Programs_DACSO_FinalCIP2_Cluster_a <- 
  "UPDATE STP_Credential_Non_Dup_Programs_DACSO
  SET STP_Credential_Non_Dup_Programs_DACSO.FINAL_CIP_CODE_2 = [INFOWARE_L_CIP_6DIGITS_CIP2016].[LCIP_LCP2_CD], 
STP_Credential_Non_Dup_Programs_DACSO.FINAL_CIP_CLUSTER_CODE = [INFOWARE_L_CIP_6DIGITS_CIP2016].[LCIP_LCIPPC_CD], 
STP_Credential_Non_Dup_Programs_DACSO.FINAL_CIP_CLUSTER_NAME = [INFOWARE_L_CIP_6DIGITS_CIP2016].[LCIP_LCIPPC_NAME]
FROM STP_Credential_Non_Dup_Programs_DACSO INNER JOIN INFOWARE_L_CIP_6DIGITS_CIP2016 
ON STP_Credential_Non_Dup_Programs_DACSO.FINAL_CIP_CODE_4 = INFOWARE_L_CIP_6DIGITS_CIP2016.LCIP_LCP4_CD"

## qry_Update_STP_Cred_Non_Dup_Programs_DACSO_FinalCIP2_Cluster_b ----
qry_Update_STP_Cred_Non_Dup_Programs_DACSO_FinalCIP2_Cluster_b <- 
  "UPDATE STP_Credential_Non_Dup_Programs_DACSO
  SET STP_Credential_Non_Dup_Programs_DACSO.FINAL_CIP_CODE_2_NAME = [INFOWARE_L_CIP_2DIGITS_CIP2016].[LCP2_DIGITS_NAME]
FROM STP_Credential_Non_Dup_Programs_DACSO INNER JOIN INFOWARE_L_CIP_2DIGITS_CIP2016 
  ON STP_Credential_Non_Dup_Programs_DACSO.FINAL_CIP_CODE_2 = INFOWARE_L_CIP_2DIGITS_CIP2016.LCP2_CD 
WHERE (((STP_Credential_Non_Dup_Programs_DACSO.FINAL_CIP_CODE_2_NAME) Is Null))"


## qry_Check_CIP_Changes_STP_Cred_Non_Dup_DACSO ----
## Check how many CIP codes in STP data are actually changed
qry_Check_CIP_Changes_STP_Cred_Non_Dup_DACSO <- 
  "SELECT PSI_CODE, PSI_PROGRAM_CODE, PSI_CREDENTIAL_PROGRAM_DESCRIPTION, PSI_CREDENTIAL_CIP, 
STP_CIP_CODE_4, STP_CIP_CODE_4_NAME, OUTCOMES_CIP_CODE_4, OUTCOMES_CIP_CODE_4_NAME, FINAL_CIP_CODE_4, FINAL_CIP_CODE_4_NAME, 
Already_Matched, New_Auto_Match, New_Manual_Match, COCI_INST_CD 
INTO STP_Credential_Non_Dup_Programs_DACSO_CIPS_CHANGED_2021_23
FROM STP_Credential_Non_Dup_Programs_DACSO
WHERE (((STP_Credential_Non_Dup_Programs_DACSO.FINAL_CIP_CODE_4)<>STP_Credential_Non_Dup_Programs_DACSO.STP_CIP_CODE_4))"
# ******************************************************************************
