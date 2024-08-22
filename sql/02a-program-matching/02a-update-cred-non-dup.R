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

## qry_update_Credential_Non_Dup_GRAD_Final_CIPs ----
qry_update_Credential_Non_Dup_GRAD_Final_CIPs <- "
UPDATE Credential_Non_Dup
SET    FINAL_CIP_CODE_4 = Credential_Non_Dup_GRAD_IDs.FINAL_CIP_CODE_4, 
       FINAL_CIP_CODE_4_NAME = Credential_Non_Dup_GRAD_IDs.FINAL_CIP_CODE_4_NAME, 
       FINAL_CIP_CODE_2 = Credential_Non_Dup_GRAD_IDs.FINAL_CIP_CODE_2, 
       FINAL_CIP_CODE_2_NAME = Credential_Non_Dup_GRAD_IDs.FINAL_CIP_CODE_2_NAME
FROM   Credential_Non_Dup_GRAD_IDs INNER JOIN Credential_Non_Dup 
ON     Credential_Non_Dup_GRAD_IDs.id = Credential_Non_Dup.id"

## qry_update_Credential_Non_Dup_APPSO_Final_CIPs ----
qry_update_Credential_Non_Dup_APPSO_Final_CIPs <- "
UPDATE Credential_Non_Dup
SET    FINAL_CIP_CODE_4 = Credential_Non_Dup_APPSO_IDs.FINAL_CIP_CODE_4, 
       FINAL_CIP_CODE_4_NAME = Credential_Non_Dup_APPSO_IDs.FINAL_CIP_CODE_4_NAME, 
       FINAL_CIP_CODE_2 = Credential_Non_Dup_APPSO_IDs.FINAL_CIP_CODE_2, 
       FINAL_CIP_CODE_2_NAME = Credential_Non_Dup_APPSO_IDs.FINAL_CIP_CODE_2_NAME
FROM   Credential_Non_Dup_APPSO_IDs INNER JOIN Credential_Non_Dup 
ON     Credential_Non_Dup_APPSO_IDs.id = Credential_Non_Dup.id"


## qry_update final cluster codes for GRAD and APPSO data
qry_update_Credential_Non_Dup_GRAD_APPSO_Cluster <- "
UPDATE Credential_Non_Dup
SET    FINAL_CIP_CLUSTER_CODE = LCP2_LCIPPC_CD,
       FINAL_CIP_CLUSTER_NAME = LCP2_LCIPPC_NAME
FROM   Credential_Non_Dup INNER JOIN INFOWARE_L_CIP_2DIGITS_CIP2016
ON     FINAL_CIP_CODE_2 = LCP2_CD
WHERE OUTCOMES_CRED = 'GRAD' OR OUTCOMES_CRED = 'APPSO'
"

## final clean up queries for non dup CIPs
# ---- SQLQuery1 ---- 
## not used 
SQLQuery1 <- "
SELECT   Match_Inst, 
         Match_School_Year, 
         Match_CIP_CODE_4, 
         Match_Credential, 
         Match_All_4_Flag, 
         Match_CIP_CODE_2, 
         Match_All_4_UseThisRecord, 
         Final_Consider_A_Match, 
         Final_Probable_Match, 
         COUNT(*) AS Expr1
FROM     DACSO_Matching_STP_Enrolment_PEN
GROUP BY Match_Inst, 
         Match_School_Year, 
         Match_CIP_CODE_4, 
         Match_Credential, 
         Match_All_4_Flag, 
         Match_CIP_CODE_2, 
         Match_All_4_UseThisRecord, 
         Final_Consider_A_Match, Final_Probable_Match
ORDER BY Match_Inst DESC, 
         Match_School_Year DESC, 
         Match_CIP_CODE_4 DESC, 
         Match_Credential DESC;"

# ---- SQLQuery2 ---- 
## not used 
SQLQuery2 <- "
UPDATE    T_BGS_Data_Final
SET       LCP2_DIGITS_NAME = INFOWARE.L_CIP_2DIGITS_CIP2016.LCP2_DIGITS_NAME
FROM      T_BGS_Data_Final 
INNER JOIN INFOWARE.L_CIP_2DIGITS_CIP2016
ON T_BGS_Data_Final.CIP_CODE_2 = INFOWARE.L_CIP_2DIGITS_CIP2016.LCP2_CD
WHERE     (T_BGS_Data_Final.LCP2_DIGITS_NAME IS NULL);"


# ---- SQLQuery3 ---- 
## not used, should be done already 
SQLQuery3 <- "
UPDATE    Credential_Non_Dup
SET       FINAL_CIP_CODE_2_NAME = INFOWARE_L_CIP_2DIGITS_CIP2016.LCP2_DIGITS_NAME, 
          FINAL_CIP_CLUSTER_CODE = INFOWARE_L_CIP_2DIGITS_CIP2016.LCP2_LCIPPC_CD, 
          FINAL_CIP_CLUSTER_NAME = INFOWARE_L_CIP_2DIGITS_CIP2016.LCP2_LCIPPC_NAME
FROM      Credential_Non_Dup 
INNER JOIN INFOWARE_L_CIP_2DIGITS_CIP2016
ON Credential_Non_Dup.FINAL_CIP_CODE_2 = INFOWARE_L_CIP_2DIGITS_CIP2016.LCP2_CD
WHERE     (Credential_Non_Dup.OUTCOMES_CRED = 'BGS');"


# ---- SQLQuery4 ---- 
## update some 99 codes for BGS
SQLQuery4 <- "
UPDATE    Credential_Non_Dup
SET       FINAL_CIP_CODE_2_NAME = 'Undeclared activity', 
          FINAL_CIP_CLUSTER_CODE = '99', 
          FINAL_CIP_CLUSTER_NAME = 'Undeclared activity'
WHERE     (OUTCOMES_CRED = 'BGS') AND (FINAL_CIP_CODE_2 = '99');"


# ---- SQLQuery5 ---- 
## not used (not credential_non_dup)
SQLQuery5 <- "
UPDATE    T_BGS_Data_Final
SET       LCIP_LCIPPC_CD = INFOWARE.L_CIP_2DIGITS_CIP2016.LCP2_LCIPPC_CD, 
          LCIP_LCIPPC_NAME = INFOWARE.L_CIP_2DIGITS_CIP2016.LCP2_LCIPPC_NAME
FROM      T_BGS_Data_Final 
INNER JOIN INFOWARE.L_CIP_2DIGITS_CIP2016 
ON T_BGS_Data_Final.CIP_CODE_2 = INFOWARE.L_CIP_2DIGITS_CIP2016.LCP2_CD;"

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




