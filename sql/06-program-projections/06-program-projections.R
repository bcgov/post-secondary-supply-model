# ---- qry_Build_Program_Projection_Input ----
qry_Build_Program_Projection_Input <- "
--CREATE VIEW tbl_Program_Projection_Input AS 
SELECT  A.AgeGroup, 
        tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY, 
        tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY + A.AgeGroup AS Expr1,
        Credential_Non_Dup.FINAL_CIP_CODE_4, 
        tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED, 
COUNT(*) AS Count 
FROM    tblCredential_HighestRank 
INNER JOIN AgeGroupLookup A
  ON    tblCredential_HighestRank.AGE_GROUP_AT_GRAD = A.AgeIndex 
INNER JOIN Credential_Non_Dup 
  ON    tblCredential_HighestRank.id = Credential_Non_Dup.id 
WHERE   (tblCredential_HighestRank.PSI_VISA_STATUS = 'DOMESTIC') 
  AND   (tblCredential_HighestRank.RESEARCH_UNIVERSITY = 1) 
  AND   (tblCredential_HighestRank.OUTCOMES_CRED <> 'DACSO') 
  AND   (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '09') 
  AND   (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '10') 
  OR    (tblCredential_HighestRank.PSI_VISA_STATUS IS NULL) 
  AND   (tblCredential_HighestRank.RESEARCH_UNIVERSITY = 1) 
  AND   (tblCredential_HighestRank.OUTCOMES_CRED <> 'DACSO') 
  AND   (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '09') 
  AND   (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '10') 
  OR    (tblCredential_HighestRank.PSI_VISA_STATUS = 'DOMESTIC') 
  AND   (tblCredential_HighestRank.RESEARCH_UNIVERSITY IS NULL) 
  AND   (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '09') 
  AND   (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '10') 
  OR    (tblCredential_HighestRank.PSI_VISA_STATUS IS NULL) 
  AND   (tblCredential_HighestRank.RESEARCH_UNIVERSITY IS NULL) 
  AND   (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '09') 
  AND   (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '10') 
GROUP BY A.AgeGroup, 
        tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY, 
        tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED,  
        Credential_Non_Dup.FINAL_CIP_CODE_4 
HAVING  (tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY <> 'APPRENTICESHIP');"

# ---- Q012a_Check_Total_for_Invalid_CIPs ---- 
Q012a_Check_Total_for_Invalid_CIPs <- 
"SELECT tbl_Program_Projection_Input.FINAL_CIP_CODE_4, 
        tbl_Program_Projection_Input.Count
FROM    tbl_Program_Projection_Input INNER JOIN INFOWARE_L_CIP_4DIGITS_CIP2016 
ON      tbl_Program_Projection_Input.FINAL_CIP_CODE_4 = INFOWARE_L_CIP_4DIGITS_CIP2016.LCP4_CD
WHERE   (((INFOWARE_L_CIP_4DIGITS_CIP2016.LCP4_CD) Is Null))
GROUP BY tbl_Program_Projection_Input.FINAL_CIP_CODE_4, tbl_Program_Projection_Input.Count;"

# ---- Q012b_Weight_Cohort_Dist ---- 
Q012b_Weight_Cohort_Dist <- 
"SELECT T_PSSM_Projection_Cred_Grp.PSSM_Credential, 
        CONCAT(CASE WHEN COSC_GRAD_STATUS_LGDS_CD IS Null THEN NULL ELSE cast(COSC_GRAD_STATUS_LGDS_CD as nvarchar(50)) + ' - ' END, [PSSM_Credential]) AS PSSM_CRED, 
        tbl_Program_Projection_Input.FINAL_CIP_CODE_4 AS LCP4_CD, T_PSSM_Projection_Cred_Grp.COSC_GRAD_STATUS_LGDS_CD, 
        CONCAT(CASE WHEN COSC_GRAD_STATUS_LGDS_CD IS Null THEN NULL ELSE cast(COSC_GRAD_STATUS_LGDS_CD as nvarchar(50)) + ' - ' END, [FINAL_CIP_CODE_4], ' - ', [T_PSSM_Projection_Cred_Grp].[PSSM_Credential]) AS LCIP4_CRED, 
        CONCAT(CASE WHEN COSC_GRAD_STATUS_LGDS_CD IS Null THEN NULL ELSE cast(COSC_GRAD_STATUS_LGDS_CD as nvarchar(50)) + ' - ' END, Left([FINAL_CIP_CODE_4],2), ' - ' , [T_PSSM_Projection_Cred_Grp].[PSSM_Credential]) AS LCIP2_CRED, 
        tbl_Program_Projection_Input.AgeGroup, 
        Sum(tbl_Program_Projection_Input.Count) AS Counts, 
        T_Weights_STP.Weight, 
        Sum([Count])*([Weight]) AS Weighted
INTO Q012b_Weight_Cohort_Dist
FROM    T_PSSM_Projection_Cred_Grp 
INNER JOIN (tbl_Program_Projection_Input 
  INNER JOIN T_Weights_STP 
    ON tbl_Program_Projection_Input.PSI_AWARD_SCHOOL_YEAR_DELAYED = T_Weights_STP.Year_Code) 
  ON T_PSSM_Projection_Cred_Grp.PSSM_Projection_Credential = tbl_Program_Projection_Input.PSI_CREDENTIAL_CATEGORY
WHERE   (((T_Weights_STP.Model)='2023-2024') AND ((T_PSSM_Projection_Cred_Grp.PSSM_Credential) Not In ('APPRAPPR','APPRCERT','GRCT or GRDP','PDEG','MAST','DOCT')))
GROUP BY T_PSSM_Projection_Cred_Grp.PSSM_Credential, 
        CONCAT(CASE WHEN COSC_GRAD_STATUS_LGDS_CD IS Null THEN NULL ELSE cast(COSC_GRAD_STATUS_LGDS_CD as nvarchar(50)) + ' - ' END, [PSSM_Credential]), 
        tbl_Program_Projection_Input.FINAL_CIP_CODE_4, 
        T_PSSM_Projection_Cred_Grp.COSC_GRAD_STATUS_LGDS_CD, 
        CONCAT(CASE WHEN COSC_GRAD_STATUS_LGDS_CD IS Null THEN NULL ELSE cast(COSC_GRAD_STATUS_LGDS_CD as nvarchar(50)) + ' - ' END, [FINAL_CIP_CODE_4], ' - ', [T_PSSM_Projection_Cred_Grp].[PSSM_Credential]), 
        CONCAT(CASE WHEN COSC_GRAD_STATUS_LGDS_CD IS Null THEN NULL ELSE cast(COSC_GRAD_STATUS_LGDS_CD as nvarchar(50)) + ' - ' END, Left([FINAL_CIP_CODE_4],2), ' - ', [T_PSSM_Projection_Cred_Grp].[PSSM_Credential]), 
        tbl_Program_Projection_Input.AgeGroup, 
        T_Weights_STP.Weight
HAVING (((T_Weights_STP.Weight)>0));"

# ---- Q012c_Weighted_Cohort_Dist ---- 
Q012c_Weighted_Cohort_Dist <- 
"SELECT Q012b_Weight_Cohort_Dist.PSSM_Credential, 
      Q012b_Weight_Cohort_Dist.PSSM_CRED, 
      Q012b_Weight_Cohort_Dist.LCP4_CD, 
      Q012b_Weight_Cohort_Dist.COSC_GRAD_STATUS_LGDS_CD, 
      Q012b_Weight_Cohort_Dist.LCIP4_CRED, 
      Q012b_Weight_Cohort_Dist.LCIP2_CRED, 
      Q012b_Weight_Cohort_Dist.AgeGroup, 
      Sum(Q012b_Weight_Cohort_Dist.Weighted) AS [Count]
INTO Q012c_Weighted_Cohort_Dist
FROM Q012b_Weight_Cohort_Dist
GROUP BY Q012b_Weight_Cohort_Dist.PSSM_Credential, 
      Q012b_Weight_Cohort_Dist.PSSM_CRED, 
      Q012b_Weight_Cohort_Dist.LCP4_CD, 
      Q012b_Weight_Cohort_Dist.COSC_GRAD_STATUS_LGDS_CD, 
      Q012b_Weight_Cohort_Dist.LCIP4_CRED, 
      Q012b_Weight_Cohort_Dist.LCIP2_CRED, 
      Q012b_Weight_Cohort_Dist.AgeGroup;"

# ---- Q012c1_Weighted_Cohort_Dist_TTRAIN ---- 
Q012c1_Weighted_Cohort_Dist_TTRAIN <- 
"SELECT T_Cohorts_Recoded.PSSM_Credential, 
        T_Cohorts_Recoded.PSSM_Credential AS PSSM_CRED, 
        T_Cohorts_Recoded.LCP4_CD, 
        T_Cohorts_Recoded.GRAD_STATUS, 
        T_Cohorts_Recoded.TTRAIN, T_Cohorts_Recoded.LCIP4_CRED, 
        T_Cohorts_Recoded.LCIP2_CRED, tbl_Age_Groups.Age_Group_Label AS Age_Group, 
        Count(*) AS Counts, 
        T_Cohorts_Recoded.Weight, 
        Count(*)*([Weight]) AS Weighted
INTO Q012c1_Weighted_Cohort_Dist_TTRAIN
FROM T_Cohorts_Recoded 
INNER JOIN tbl_Age_Groups 
  ON T_Cohorts_Recoded.Age_Group = tbl_Age_Groups.Age_Group
WHERE (((T_Cohorts_Recoded.GRAD_STATUS)<>'3'))
GROUP BY T_Cohorts_Recoded.PSSM_Credential, T_Cohorts_Recoded.LCP4_CD, 
        T_Cohorts_Recoded.GRAD_STATUS, T_Cohorts_Recoded.TTRAIN, 
        T_Cohorts_Recoded.LCIP4_CRED, T_Cohorts_Recoded.LCIP2_CRED, 
        tbl_Age_Groups.Age_Group_Label, T_Cohorts_Recoded.Weight, 
        T_Cohorts_Recoded.PSSM_Credential
HAVING (((T_Cohorts_Recoded.TTRAIN) Is Not Null) 
AND ((T_Cohorts_Recoded.Weight)>0));"

# ---- Q012c2_Weighted_Cohort_Dist ---- 
Q012c2_Weighted_Cohort_Dist <- 
"SELECT Q012c1_Weighted_Cohort_Dist_TTRAIN.PSSM_Credential, 
         Q012c1_Weighted_Cohort_Dist_TTRAIN.PSSM_CRED, 
         Q012c1_Weighted_Cohort_Dist_TTRAIN.LCP4_CD, 
         Q012c1_Weighted_Cohort_Dist_TTRAIN.GRAD_STATUS, 
         Q012c1_Weighted_Cohort_Dist_TTRAIN.TTRAIN, 
         Q012c1_Weighted_Cohort_Dist_TTRAIN.LCIP4_CRED, 
         Q012c1_Weighted_Cohort_Dist_TTRAIN.LCIP2_CRED, 
         Q012c1_Weighted_Cohort_Dist_TTRAIN.Age_Group, 
         Sum(Q012c1_Weighted_Cohort_Dist_TTRAIN.Weighted) AS [Count]
INTO Q012c2_Weighted_Cohort_Dist
FROM Q012c1_Weighted_Cohort_Dist_TTRAIN
GROUP BY Q012c1_Weighted_Cohort_Dist_TTRAIN.PSSM_Credential, 
         Q012c1_Weighted_Cohort_Dist_TTRAIN.PSSM_CRED, 
         Q012c1_Weighted_Cohort_Dist_TTRAIN.LCP4_CD, 
         Q012c1_Weighted_Cohort_Dist_TTRAIN.GRAD_STATUS, 
         Q012c1_Weighted_Cohort_Dist_TTRAIN.TTRAIN, 
         Q012c1_Weighted_Cohort_Dist_TTRAIN.LCIP4_CRED, 
         Q012c1_Weighted_Cohort_Dist_TTRAIN.LCIP2_CRED, 
         Q012c1_Weighted_Cohort_Dist_TTRAIN.Age_Group;"

# ---- Q012c3_Weighted_Cohort_Dist_Total ---- 
Q012c3_Weighted_Cohort_Dist_Total <- 
"SELECT Q012c2_Weighted_Cohort_Dist.PSSM_Credential, 
        Q012c2_Weighted_Cohort_Dist.PSSM_CRED, 
        Q012c2_Weighted_Cohort_Dist.LCP4_CD, 
        Q012c2_Weighted_Cohort_Dist.GRAD_STATUS, 
        Q012c2_Weighted_Cohort_Dist.Age_Group, 
        Sum(Q012c2_Weighted_Cohort_Dist.Count) AS Totals
INTO    Q012c3_Weighted_Cohort_Dist_Total
FROM    Q012c2_Weighted_Cohort_Dist
GROUP BY Q012c2_Weighted_Cohort_Dist.PSSM_Credential, 
        Q012c2_Weighted_Cohort_Dist.PSSM_CRED, 
        Q012c2_Weighted_Cohort_Dist.LCP4_CD, 
        Q012c2_Weighted_Cohort_Dist.GRAD_STATUS, 
        Q012c2_Weighted_Cohort_Dist.Age_Group;"

# ---- Q012c4_Weighted_Cohort_Distribution_Projected ---- 
Q012c4_Weighted_Cohort_Distribution_Projected <- 
"SELECT 'Program_Projections_2023-2024_Q015e' AS Survey, 
        Q012c2_Weighted_Cohort_Dist.PSSM_Credential, 
        Q012c2_Weighted_Cohort_Dist.PSSM_CRED, 
        Q012c2_Weighted_Cohort_Dist.LCP4_CD, 
        Q012c2_Weighted_Cohort_Dist.GRAD_STATUS, 
        Q012c2_Weighted_Cohort_Dist.TTRAIN, 
        Q012c2_Weighted_Cohort_Dist.Age_Group, 
        '2023/2024' AS Projection_Year, 
        Q012c2_Weighted_Cohort_Dist.Count, 
        Q012c3_Weighted_Cohort_Dist_Total.Totals, 
        IIf((Totals=0), 0 , CAST(Count AS float)/CAST(Totals as FLOAT)) AS [%]
INTO    Q012c4_Weighted_Cohort_Distribution_Projected
FROM    Q012c2_Weighted_Cohort_Dist 
INNER JOIN Q012c3_Weighted_Cohort_Dist_Total 
  ON    (Q012c2_Weighted_Cohort_Dist.Age_Group = Q012c3_Weighted_Cohort_Dist_Total.Age_Group)
  AND   (Q012c2_Weighted_Cohort_Dist.PSSM_CRED = Q012c3_Weighted_Cohort_Dist_Total.PSSM_CRED) 
  AND   (Q012c2_Weighted_Cohort_Dist.GRAD_STATUS = Q012c3_Weighted_Cohort_Dist_Total.GRAD_STATUS) 
  AND   (Q012c2_Weighted_Cohort_Dist.LCP4_CD = Q012c3_Weighted_Cohort_Dist_Total.LCP4_CD);"

# ---- Q012c5_Weighted_Cohort_Dist_TTRAIN ----
Q012c5_Weighted_Cohort_Dist_TTRAIN <- 
"SELECT Q012c_Weighted_Cohort_Dist.PSSM_Credential, 
        Q012c_Weighted_Cohort_Dist.PSSM_CRED, 
        Q012c_Weighted_Cohort_Dist.LCP4_CD, 
        Q012c_Weighted_Cohort_Dist.COSC_GRAD_STATUS_LGDS_CD, 
        Q012c4_Weighted_Cohort_Distribution_Projected.TTRAIN, 
        CONCAT(
          (CASE WHEN [COSC_GRAD_STATUS_LGDS_CD] IS NULL THEN Null ELSE CAST([COSC_GRAD_STATUS_LGDS_CD] AS NVARCHAR(50)) + ' - ' END),
			    [Q012c_Weighted_Cohort_Dist].[LCP4_CD], ' - ',
			    (CASE WHEN [TTRAIN] IS NULL THEN Null ELSE CAST([TTRAIN] AS NVARCHAR(50)) + ' - ' END),
			    [Q012c_Weighted_Cohort_Dist].[PSSM_Credential]
			   ) AS LCIP4_CRED, 
        CONCAT(
          (CASE WHEN [COSC_GRAD_STATUS_LGDS_CD] IS NULL THEN Null ELSE CAST([COSC_GRAD_STATUS_LGDS_CD] AS NVARCHAR(50)) + ' - ' END), 
			    Left([Q012c_Weighted_Cohort_Dist].[LCP4_CD],2) , ' - ',  
			    (CASE WHEN [TTRAIN] IS NULL THEN Null ELSE CAST([TTRAIN] AS NVARCHAR(50)) + ' - ' END), 
			    [Q012c_Weighted_Cohort_Dist].[PSSM_Credential]
			  ) AS LCIP2_CRED, 
        Q012c_Weighted_Cohort_Dist.AgeGroup, Q012c_Weighted_Cohort_Dist.Count, Q012c4_Weighted_Cohort_Distribution_Projected.[%], 
        CASE WHEN [Q012c4_Weighted_Cohort_Distribution_Projected].[%] IS NULL THEN [Q012c_Weighted_Cohort_Dist].[Count] ELSE [Q012c_Weighted_Cohort_Dist].[Count]*[Q012c4_Weighted_Cohort_Distribution_Projected].[%] END AS Count_Distributed
INTO    Q012c5_Weighted_Cohort_Dist_TTRAIN
FROM    Q012c_Weighted_Cohort_Dist 
LEFT JOIN Q012c4_Weighted_Cohort_Distribution_Projected 
  ON    (Q012c_Weighted_Cohort_Dist.AgeGroup = Q012c4_Weighted_Cohort_Distribution_Projected.Age_Group) 
  AND   (Q012c_Weighted_Cohort_Dist.COSC_GRAD_STATUS_LGDS_CD = Q012c4_Weighted_Cohort_Distribution_Projected.GRAD_STATUS) 
  AND   (Q012c_Weighted_Cohort_Dist.LCP4_CD = Q012c4_Weighted_Cohort_Distribution_Projected.LCP4_CD) 
  AND   (Q012c_Weighted_Cohort_Dist.PSSM_Credential = Q012c4_Weighted_Cohort_Distribution_Projected.PSSM_Credential);"

# ---- Q012d_Weighted_Cohort_Dist_Total ---- 
Q012d_Weighted_Cohort_Dist_Total <- 
"SELECT Q012b_Weight_Cohort_Dist.PSSM_Credential, 
        Q012b_Weight_Cohort_Dist.PSSM_CRED, 
        Q012b_Weight_Cohort_Dist.AgeGroup, 
        Sum(Q012b_Weight_Cohort_Dist.Weighted) AS Totals
INTO    Q012d_Weighted_Cohort_Dist_Total
FROM    Q012b_Weight_Cohort_Dist
GROUP BY Q012b_Weight_Cohort_Dist.PSSM_Credential, 
        Q012b_Weight_Cohort_Dist.PSSM_CRED, 
        Q012b_Weight_Cohort_Dist.AgeGroup;"

# ---- Q012e_Delete_Weighted_Cohort_Distribution ---- 
Q012e_Delete_Weighted_Cohort_Distribution <- 
"DELETE 
FROM Cohort_Program_Distributions_Static
WHERE (((Cohort_Program_Distributions_Static.Survey) Like '%Q012e'));"

# ---- Q012e_Weighted_Cohort_Distribution ---- 
Q012e_Weighted_Cohort_Distribution <- 
"INSERT INTO Cohort_Program_Distributions_Static 
( Survey, PSSM_Credential, PSSM_CRED, LCP4_CD, GRAD_STATUS, TTRAIN, LCIP4_CRED, LCIP2_CRED, Age_Group, [Year], [Count], Total, [Percent] )
SELECT 'Program_Projections_2023-2024_Q012e' AS Survey, 
Q012c5_Weighted_Cohort_Dist_TTRAIN.PSSM_Credential,
        Q012c5_Weighted_Cohort_Dist_TTRAIN.PSSM_CRED, 
        Q012c5_Weighted_Cohort_Dist_TTRAIN.LCP4_CD, 
        Q012c5_Weighted_Cohort_Dist_TTRAIN.COSC_GRAD_STATUS_LGDS_CD, 
        Q012c5_Weighted_Cohort_Dist_TTRAIN.TTRAIN, 
        Q012c5_Weighted_Cohort_Dist_TTRAIN.LCIP4_CRED, 
        Q012c5_Weighted_Cohort_Dist_TTRAIN.LCIP2_CRED, 
        Q012c5_Weighted_Cohort_Dist_TTRAIN.AgeGroup, 
        '2023/2024' AS Projection_Year, 
        Q012c5_Weighted_Cohort_Dist_TTRAIN.Count_Distributed, 
        Q012d_Weighted_Cohort_Dist_Total.Totals, 
        CASE WHEN Totals = 0 THEN 0 ELSE CAST(Count_Distributed AS FLOAT)/CAST(Totals AS FLOAT) END AS [Percent]
FROM    Q012c5_Weighted_Cohort_Dist_TTRAIN 
INNER JOIN Q012d_Weighted_Cohort_Dist_Total 
  ON    (Q012c5_Weighted_Cohort_Dist_TTRAIN.AgeGroup = Q012d_Weighted_Cohort_Dist_Total.AgeGroup) 
  AND   (Q012c5_Weighted_Cohort_Dist_TTRAIN.PSSM_CRED = Q012d_Weighted_Cohort_Dist_Total.PSSM_CRED);"

# ---- Q013a_Check_PDEG_CLP_07_Only_CIP_22 ---- 
Q013a_Check_PDEG_CLP_07_Only_CIP_22 <- 
"SELECT T_PSSM_Projection_Cred_Grp.PSSM_Credential, 
        CONCAT
        (CASE WHEN [COSC_GRAD_STATUS_LGDS_CD] IS NULL THEN Null ELSE CAST([COSC_GRAD_STATUS_LGDS_CD] AS NVARCHAR(50)) + ' - ' END, [PSSM_Credential]) AS PSSM_CRED, 
        tbl_Program_Projection_Input.FINAL_CIP_CODE_4, qry_12_LCP4_LCIPPC_Recode_9999.LCIP_LCIPPC_CD AS LCIPPC_CD, 
        CONCAT(CASE WHEN [COSC_GRAD_STATUS_LGDS_CD] IS NULL THEN Null ELSE CAST([COSC_GRAD_STATUS_LGDS_CD] AS NVARCHAR(50)) + ' - ' END, [LCIP_LCIPPC_CD], ' - ',
           [T_PSSM_Projection_Cred_Grp].[PSSM_Credential]) AS LCIPPC_CRED, 
        tbl_Program_Projection_Input.AgeGroup, 
        Sum(tbl_Program_Projection_Input.Count) AS Counts, 
        T_Weights_STP.Weight, 
        Sum([Count])*([Weight]) AS Weighted
FROM (T_PSSM_Projection_Cred_Grp 
INNER JOIN (tbl_Program_Projection_Input 
    INNER JOIN T_Weights_STP 
      ON tbl_Program_Projection_Input.PSI_AWARD_SCHOOL_YEAR_DELAYED = T_Weights_STP.Year_Code) 
  ON T_PSSM_Projection_Cred_Grp.PSSM_Projection_Credential = tbl_Program_Projection_Input.PSI_CREDENTIAL_CATEGORY) 
INNER JOIN qry_12_LCP4_LCIPPC_Recode_9999 
  ON tbl_Program_Projection_Input.FINAL_CIP_CODE_4 = qry_12_LCP4_LCIPPC_Recode_9999.LCIP_LCP4_CD
WHERE (((T_Weights_STP.Model)='2023-2024') 
AND ((T_PSSM_Projection_Cred_Grp.PSSM_Credential) In ('PDEG')))
GROUP BY T_PSSM_Projection_Cred_Grp.PSSM_Credential, 
		    CONCAT(CASE WHEN [COSC_GRAD_STATUS_LGDS_CD] IS NULL THEN Null ELSE CAST([COSC_GRAD_STATUS_LGDS_CD] AS NVARCHAR(50)) + ' - ' END, [PSSM_Credential]),
		    tbl_Program_Projection_Input.FINAL_CIP_CODE_4, 
		    qry_12_LCP4_LCIPPC_Recode_9999.LCIP_LCIPPC_CD, 
		    CONCAT(CASE WHEN [COSC_GRAD_STATUS_LGDS_CD] IS NULL THEN Null ELSE CAST([COSC_GRAD_STATUS_LGDS_CD] AS NVARCHAR(50)) + ' - ' END, [LCIP_LCIPPC_CD],
		        ' - ', [T_PSSM_Projection_Cred_Grp].[PSSM_Credential]), 
		tbl_Program_Projection_Input.AgeGroup, T_Weights_STP.Weight
HAVING (((T_Weights_STP.Weight)>0));"

# ---- Q013b_Weight_Cohort_Dist_MAST_DOCT_Others ---- 
Q013b_Weight_Cohort_Dist_MAST_DOCT_Others <- 
"SELECT T_PSSM_Projection_Cred_Grp.PSSM_Credential, 
		    CONCAT(CASE WHEN [COSC_GRAD_STATUS_LGDS_CD] IS NULL THEN Null ELSE CAST([COSC_GRAD_STATUS_LGDS_CD] AS NVARCHAR(50)) + ' - ' END, [PSSM_Credential]) AS PSSM_CRED, 
        qry_12_LCP4_LCIPPC_Recode_9999.LCIP_LCIPPC_CD AS LCIPPC_CD, 
        CONCAT(CASE WHEN [COSC_GRAD_STATUS_LGDS_CD] IS NULL THEN Null ELSE CAST([COSC_GRAD_STATUS_LGDS_CD] AS NVARCHAR(50)) + ' - ' END, [LCIP_LCIPPC_CD], ' - ',
             [T_PSSM_Projection_Cred_Grp].[PSSM_Credential]) AS LCIPPC_CRED, 
        tbl_Program_Projection_Input.AgeGroup, 
        Sum(tbl_Program_Projection_Input.Count) AS Counts, 
        T_Weights_STP.Weight, 
        Sum([Count])*([Weight]) AS Weighted
INTO Q013b_Weight_Cohort_Dist_MAST_DOCT_Others
FROM    (T_PSSM_Projection_Cred_Grp 
INNER JOIN (tbl_Program_Projection_Input   
    INNER JOIN T_Weights_STP 
      ON tbl_Program_Projection_Input.PSI_AWARD_SCHOOL_YEAR_DELAYED = T_Weights_STP.Year_Code) 
  ON T_PSSM_Projection_Cred_Grp.PSSM_Projection_Credential = tbl_Program_Projection_Input.PSI_CREDENTIAL_CATEGORY) 
INNER JOIN qry_12_LCP4_LCIPPC_Recode_9999 
  ON tbl_Program_Projection_Input.FINAL_CIP_CODE_4 = qry_12_LCP4_LCIPPC_Recode_9999.LCIP_LCP4_CD
WHERE (((T_Weights_STP.Model)='2023-2024') 
AND   ((T_PSSM_Projection_Cred_Grp.PSSM_Credential) In ('GRCT or GRDP','PDEG','MAST','DOCT')))
GROUP BY T_PSSM_Projection_Cred_Grp.PSSM_Credential, 
	    CONCAT(CASE WHEN [COSC_GRAD_STATUS_LGDS_CD] IS NULL THEN Null ELSE CAST([COSC_GRAD_STATUS_LGDS_CD] AS NVARCHAR(50)) + ' - ' END, [PSSM_Credential]), 
      qry_12_LCP4_LCIPPC_Recode_9999.LCIP_LCIPPC_CD, 
      CONCAT(CASE WHEN [COSC_GRAD_STATUS_LGDS_CD] IS NULL THEN Null ELSE CAST([COSC_GRAD_STATUS_LGDS_CD] AS NVARCHAR(50)) + ' - ' END, [LCIP_LCIPPC_CD], ' - ',
      [T_PSSM_Projection_Cred_Grp].[PSSM_Credential]), 
      tbl_Program_Projection_Input.AgeGroup, T_Weights_STP.Weight
HAVING (((T_Weights_STP.Weight)>0));"

# ---- Q013c_Weighted_Cohort_Dist ---- 
Q013c_Weighted_Cohort_Dist <- 
"SELECT Q013b_Weight_Cohort_Dist_MAST_DOCT_Others.PSSM_Credential, 
        Q013b_Weight_Cohort_Dist_MAST_DOCT_Others.PSSM_CRED,
        Q013b_Weight_Cohort_Dist_MAST_DOCT_Others.LCIPPC_CD, 
        Q013b_Weight_Cohort_Dist_MAST_DOCT_Others.LCIPPC_CRED, 
        Q013b_Weight_Cohort_Dist_MAST_DOCT_Others.AgeGroup, 
        Sum(Q013b_Weight_Cohort_Dist_MAST_DOCT_Others.Weighted) AS [Count]
INTO    Q013c_Weighted_Cohort_Dist
FROM    Q013b_Weight_Cohort_Dist_MAST_DOCT_Others
GROUP BY Q013b_Weight_Cohort_Dist_MAST_DOCT_Others.PSSM_Credential, 
        Q013b_Weight_Cohort_Dist_MAST_DOCT_Others.PSSM_CRED, 
        Q013b_Weight_Cohort_Dist_MAST_DOCT_Others.LCIPPC_CD, 
        Q013b_Weight_Cohort_Dist_MAST_DOCT_Others.LCIPPC_CRED, 
        Q013b_Weight_Cohort_Dist_MAST_DOCT_Others.AgeGroup;"

# ---- Q013d_Weighted_Cohort_Dist_Total ---- 
Q013d_Weighted_Cohort_Dist_Total <- 
"SELECT Q013b_Weight_Cohort_Dist_MAST_DOCT_Others.PSSM_Credential, 
        Q013b_Weight_Cohort_Dist_MAST_DOCT_Others.PSSM_CRED, 
        Q013b_Weight_Cohort_Dist_MAST_DOCT_Others.AgeGroup, 
        Sum(Q013b_Weight_Cohort_Dist_MAST_DOCT_Others.Weighted) AS Totals
INTO    Q013d_Weighted_Cohort_Dist_Total
FROM    Q013b_Weight_Cohort_Dist_MAST_DOCT_Others
GROUP BY Q013b_Weight_Cohort_Dist_MAST_DOCT_Others.PSSM_Credential, 
        Q013b_Weight_Cohort_Dist_MAST_DOCT_Others.PSSM_CRED, 
        Q013b_Weight_Cohort_Dist_MAST_DOCT_Others.AgeGroup;"

# ---- Q013e_Weighted_Cohort_Distribution ---- 
Q013e_Weighted_Cohort_Distribution <- "
INSERT INTO Cohort_Program_Distributions_Static (Survey, PSSM_Credential, PSSM_CRED, LCP4_CD, LCIP4_CRED, Age_Group, [Year], [Count], [Total], [Percent] )
SELECT 'Program_Projections_2023-2024_Q013e' AS Survey, 
        Q013c_Weighted_Cohort_Dist.PSSM_Credential, 
        Q013c_Weighted_Cohort_Dist.PSSM_CRED, 
        Q013c_Weighted_Cohort_Dist.LCIPPC_CD, 
        Q013c_Weighted_Cohort_Dist.LCIPPC_CRED, 
        Q013c_Weighted_Cohort_Dist.AgeGroup, 
        '2023/2024' AS Year, 
        Q013c_Weighted_Cohort_Dist.Count, 
        Q013d_Weighted_Cohort_Dist_Total.Totals, 
        CASE WHEN Totals = 0 THEN 0 ELSE CAST([Count] AS FLOAT)/CAST([Totals] AS FLOAT) END AS [Percent]
FROM    Q013c_Weighted_Cohort_Dist 
INNER JOIN Q013d_Weighted_Cohort_Dist_Total 
  ON    (Q013c_Weighted_Cohort_Dist.AgeGroup = Q013d_Weighted_Cohort_Dist_Total.AgeGroup) 
  AND   (Q013c_Weighted_Cohort_Dist.PSSM_CRED = Q013d_Weighted_Cohort_Dist_Total.PSSM_CRED);"

# ---- Q014b_Weighted_Cohort_Dist_APPR ---- 
Q014b_Weighted_Cohort_Dist_APPR <- 
"SELECT T_Cohorts_Recoded.PSSM_Credential, 
        T_Cohorts_Recoded.PSSM_Credential AS PSSM_CRED, 
        T_Cohorts_Recoded.LCP4_CD, 
        T_Cohorts_Recoded.TTRAIN, 
        T_Cohorts_Recoded.LCIP4_CRED, 
        T_Cohorts_Recoded.LCIP2_CRED, 
        tbl_Age_Groups.Age_Group_Label AS Age_Group, 
        Count(*) AS Counts, 
        T_Cohorts_Recoded.Weight, 
        Count(*)*([Weight]) AS Weighted
INTO    Q014b_Weighted_Cohort_Dist_APPR
FROM    T_Cohorts_Recoded INNER JOIN tbl_Age_Groups 
ON      T_Cohorts_Recoded.Age_Group = tbl_Age_Groups.Age_Group
WHERE   (((T_Cohorts_Recoded.PSSM_Credential) In ('APPRAPPR','APPRCERT')))
GROUP BY T_Cohorts_Recoded.PSSM_Credential, 
        T_Cohorts_Recoded.LCP4_CD, 
        T_Cohorts_Recoded.TTRAIN, 
        T_Cohorts_Recoded.LCIP4_CRED, 
        T_Cohorts_Recoded.LCIP2_CRED, tbl_Age_Groups.Age_Group_Label, 
        T_Cohorts_Recoded.Weight, 
        T_Cohorts_Recoded.PSSM_Credential
HAVING (((T_Cohorts_Recoded.Weight)>0));"

# ---- Q014c_Weighted_Cohort_Dist ----
Q014c_Weighted_Cohort_Dist <- 
"SELECT Q014b_Weighted_Cohort_Dist_APPR.PSSM_Credential, 
        Q014b_Weighted_Cohort_Dist_APPR.PSSM_CRED, 
        Q014b_Weighted_Cohort_Dist_APPR.LCP4_CD, 
        Q014b_Weighted_Cohort_Dist_APPR.LCIP4_CRED, 
        Q014b_Weighted_Cohort_Dist_APPR.LCIP2_CRED, 
        Q014b_Weighted_Cohort_Dist_APPR.Age_Group, 
        Sum(Q014b_Weighted_Cohort_Dist_APPR.Weighted) AS [Count]
INTO    Q014c_Weighted_Cohort_Dist
FROM    Q014b_Weighted_Cohort_Dist_APPR
GROUP BY Q014b_Weighted_Cohort_Dist_APPR.PSSM_Credential, 
        Q014b_Weighted_Cohort_Dist_APPR.PSSM_CRED, 
        Q014b_Weighted_Cohort_Dist_APPR.LCP4_CD, 
        Q014b_Weighted_Cohort_Dist_APPR.LCIP4_CRED, 
        Q014b_Weighted_Cohort_Dist_APPR.LCIP2_CRED, 
        Q014b_Weighted_Cohort_Dist_APPR.Age_Group;"

# ---- Q014d_Weighted_Cohort_Dist_Total ---- 
Q014d_Weighted_Cohort_Dist_Total <- 
"SELECT Q014b_Weighted_Cohort_Dist_APPR.PSSM_Credential, 
        Q014b_Weighted_Cohort_Dist_APPR.PSSM_CRED, 
        Q014b_Weighted_Cohort_Dist_APPR.Age_Group, 
        Sum(Q014b_Weighted_Cohort_Dist_APPR.Weighted) AS Totals
INTO    Q014d_Weighted_Cohort_Dist_Total
FROM    Q014b_Weighted_Cohort_Dist_APPR
GROUP BY Q014b_Weighted_Cohort_Dist_APPR.PSSM_Credential, 
        Q014b_Weighted_Cohort_Dist_APPR.PSSM_CRED, 
        Q014b_Weighted_Cohort_Dist_APPR.Age_Group;"

# Q014e_Weighted_Cohort_Distribution_Projected ----
Q014e_Weighted_Cohort_Distribution_Projected <- 
"INSERT INTO Cohort_Program_Distributions_Projected 
( Survey, PSSM_Credential, PSSM_CRED, LCP4_CD, LCIP4_CRED, LCIP2_CRED, Age_Group, [Year], [Count], Total, [Percent] )
SELECT 'Program_Projections_2023-2024_Q014e' AS Survey, 
        Q014c_Weighted_Cohort_Dist.PSSM_Credential, 
        Q014c_Weighted_Cohort_Dist.PSSM_CRED, 
        Q014c_Weighted_Cohort_Dist.LCP4_CD, 
        Q014c_Weighted_Cohort_Dist.LCIP4_CRED, 
        Q014c_Weighted_Cohort_Dist.LCIP2_CRED, 
        Q014c_Weighted_Cohort_Dist.Age_Group, 
        '2023/2024' AS Projection_Year, 
        Q014c_Weighted_Cohort_Dist.Count, Q014d_Weighted_Cohort_Dist_Total.Totals, 
        CASE WHEN Totals = 0 THEN 0 ELSE Count/Totals END AS [Percent]
FROM    Q014c_Weighted_Cohort_Dist 
INNER JOIN Q014d_Weighted_Cohort_Dist_Total 
  ON   (Q014c_Weighted_Cohort_Dist.Age_Group = Q014d_Weighted_Cohort_Dist_Total.Age_Group) 
  AND  (Q014c_Weighted_Cohort_Dist.PSSM_CRED = Q014d_Weighted_Cohort_Dist_Total.PSSM_CRED);"

# ---- Q014e_Weighted_Cohort_Distribution_Static ----
Q014e_Weighted_Cohort_Distribution_Static <- 
"INSERT INTO Cohort_Program_Distributions_Static 
( Survey, PSSM_Credential, PSSM_CRED, LCP4_CD, LCIP4_CRED, LCIP2_CRED, Age_Group, [Year], [Count], Total, [Percent] )
SELECT 'Program_Projections_2023-2024_Q014e' AS Survey, 
        Q014c_Weighted_Cohort_Dist.PSSM_Credential, 
        Q014c_Weighted_Cohort_Dist.PSSM_CRED, 
        Q014c_Weighted_Cohort_Dist.LCP4_CD, 
        Q014c_Weighted_Cohort_Dist.LCIP4_CRED, 
        Q014c_Weighted_Cohort_Dist.LCIP2_CRED, 
        Q014c_Weighted_Cohort_Dist.Age_Group, '2023/2024' AS Projection_Year, 
        Q014c_Weighted_Cohort_Dist.Count, Q014d_Weighted_Cohort_Dist_Total.Totals, 
        CASE WHEN Totals = 0 THEN 0 ELSE Count/Totals END AS [Percent]
FROM    Q014c_Weighted_Cohort_Dist
INNER JOIN Q014d_Weighted_Cohort_Dist_Total 
  ON    (Q014c_Weighted_Cohort_Dist.Age_Group = Q014d_Weighted_Cohort_Dist_Total.Age_Group) 
  AND   (Q014c_Weighted_Cohort_Dist.PSSM_CRED = Q014d_Weighted_Cohort_Dist_Total.PSSM_CRED);"

# ---- Q014f_APPSO_Grads_Y2_to_Y10 ---- 
Q014f_APPSO_Grads_Y2_to_Y10 <- 
"INSERT INTO Graduate_Projections ( Survey, PSSM_Credential, PSSM_CRED, Age_Group, [Year], Graduates )
SELECT  Graduate_Projections.Survey, 
        Graduate_Projections.PSSM_Credential, 
        Graduate_Projections.PSSM_CRED, 
        Graduate_Projections.Age_Group, 
        T_APPR_Y2_to_Y10.Y2_to_Y10, 
        Graduate_Projections.Graduates
FROM    Graduate_Projections INNER JOIN T_APPR_Y2_to_Y10 
ON      Graduate_Projections.Year = T_APPR_Y2_to_Y10.Y1
WHERE   (((Graduate_Projections.Survey)='APPSO'));"

# ---- Q015e21_Append_Selected_Static_Distribution_Y2_to_Y12_Projected ---- 
Q015e21_Append_Selected_Static_Distribution_Y2_to_Y12_Projected <- 
"INSERT INTO Cohort_Program_Distributions_Projected 
( Survey, PSSM_Credential, PSSM_CRED, LCP4_CD, GRAD_STATUS, TTRAIN, LCIP4_CRED, LCIP2_CRED, Age_Group, [Year], [Count], [Total], [Percent] )
SELECT 'Program_Projections_2023-2024_Q015e21' AS Survey, 
        Cohort_Program_Distributions_Static.PSSM_Credential, 
        Cohort_Program_Distributions_Static.PSSM_CRED, 
        Cohort_Program_Distributions_Static.LCP4_CD, 
        Cohort_Program_Distributions_Static.GRAD_STATUS, 
        Cohort_Program_Distributions_Static.TTRAIN, 
        Cohort_Program_Distributions_Static.LCIP4_CRED, 
        Cohort_Program_Distributions_Static.LCIP2_CRED, 
        Cohort_Program_Distributions_Static.Age_Group, 
        T_Cohort_Program_Distributions_Y2_to_Y12.Y2_to_Y10 as Year, 
        Cohort_Program_Distributions_Static.Count, 
        Cohort_Program_Distributions_Static.Total, 
        Cohort_Program_Distributions_Static.[Percent]
FROM    Cohort_Program_Distributions_Static 
INNER JOIN T_Cohort_Program_Distributions_Y2_to_Y12 
ON      Cohort_Program_Distributions_Static.Year = T_Cohort_Program_Distributions_Y2_to_Y12.Y1
WHERE   (((Cohort_Program_Distributions_Static.PSSM_CRED) In ('APPRAPPR','APPRCERT') 
    Or (Cohort_Program_Distributions_Static.PSSM_CRED) Like '3 - %'));"

# ---- Q015e22_Append_Distribution_Y2_to_Y12_Static ---- 
Q015e22_Append_Distribution_Y2_to_Y12_Static <- 
"INSERT INTO Cohort_Program_Distributions_Static 
( Survey, PSSM_Credential, PSSM_CRED, LCP4_CD, GRAD_STATUS, TTRAIN, LCIP4_CRED, LCIP2_CRED, Age_Group, [Year], [Count], Total, [Percent] )
        SELECT 'Program_Projections_2023-2024_Q015e22' AS Survey, 
        Cohort_Program_Distributions_Static.PSSM_Credential, 
        Cohort_Program_Distributions_Static.PSSM_CRED, 
        Cohort_Program_Distributions_Static.LCP4_CD, 
        Cohort_Program_Distributions_Static.GRAD_STATUS, 
        Cohort_Program_Distributions_Static.TTRAIN, 
        Cohort_Program_Distributions_Static.LCIP4_CRED, 
        Cohort_Program_Distributions_Static.LCIP2_CRED, 
        Cohort_Program_Distributions_Static.Age_Group, 
        T_Cohort_Program_Distributions_Y2_to_Y12.Y2_to_Y10, 
        Cohort_Program_Distributions_Static.Count, 
        Cohort_Program_Distributions_Static.Total, 
        Cohort_Program_Distributions_Static.[Percent]
FROM Cohort_Program_Distributions_Static 
INNER JOIN T_Cohort_Program_Distributions_Y2_to_Y12 
ON Cohort_Program_Distributions_Static.Year = T_Cohort_Program_Distributions_Y2_to_Y12.Y1;"

# ---- qry_05_Flip_T_Predict_CIP_CRED_AGE_1 ---- 
qry_05_Flip_T_Predict_CIP_CRED_AGE_1 <-
"SELECT T_Predict_CIP_CRED_AGE.CIP,
T_Predict_CIP_CRED_AGE.CRED, 
T_Predict_CIP_CRED_AGE.AGE, 
T_Predict_CIP_CRED_AGE.[2023/2024] AS [Count], 
'2023/2024' AS [Year] 
INTO T_Predict_CIP_CRED_AGE_Flipped
FROM T_Predict_CIP_CRED_AGE;"

# ---- qry_05_Flip_T_Predict_CIP_CRED_AGE_2 ---- 
qry_05_Flip_T_Predict_CIP_CRED_AGE_2 <-
"INSERT INTO T_Predict_CIP_CRED_AGE_Flipped ( CIP, CRED, AGE, [Year], [Count] )
SELECT T_Predict_CIP_CRED_AGE.CIP, 
T_Predict_CIP_CRED_AGE.CRED, 
T_Predict_CIP_CRED_AGE.AGE, 
'2020/2021' AS [Year], 
T_Predict_CIP_CRED_AGE.[2020/2021]
FROM T_Predict_CIP_CRED_AGE;"

# ---- qry_05_Flip_T_Predict_CIP_CRED_AGE_2_Check ---- 
qry_05_Flip_T_Predict_CIP_CRED_AGE_2_Check <-
"SELECT T_Predict_CIP_CRED_AGE_Flipped.Year, 
Sum(T_Predict_CIP_CRED_AGE_Flipped.Count) AS SumOfCount
FROM T_Predict_CIP_CRED_AGE_Flipped
GROUP BY T_Predict_CIP_CRED_AGE_Flipped.Year;"

# ---- qry_09_Delete_Selected_Static_Cohort_Dist_from_Projected ----  
qry_09_Delete_Selected_Static_Cohort_Dist_from_Projected <-"
DELETE 
FROM    Cohort_Program_Distributions_Projected
WHERE   (((Cohort_Program_Distributions_Projected.PSSM_CRED) Not In ('APPRAPPR','APPRCERT') 
  AND   (Cohort_Program_Distributions_Projected.PSSM_CRED) Not Like '3 -%' 
  AND   (Cohort_Program_Distributions_Projected.PSSM_CRED) Not Like 'P -%'));"

# ---- qry_10a_Program_Dist_Count ----
qry_10a_Program_Dist_Count <- 
"SELECT T_PSSM_Projection_Cred_Grp.PSSM_Credential, 
        CONCAT(CASE WHEN [COSC_GRAD_STATUS_LGDS_CD] IS NULL THEN Null ELSE CAST([COSC_GRAD_STATUS_LGDS_CD] AS NVARCHAR(50)) + ' - ' END, [T_PSSM_Projection_Cred_Grp].[PSSM_Credential]) AS PSSM_CRED, 
        T_Predict_CIP_CRED_AGE_Flipped.CIP, 
        CONCAT(CASE WHEN [COSC_GRAD_STATUS_LGDS_CD] IS NULL THEN Null ELSE CAST([COSC_GRAD_STATUS_LGDS_CD] AS NVARCHAR(50)) + ' - ' END, [CIP], ' - ' 
			, [T_PSSM_Projection_Cred_Grp].[PSSM_Credential]) AS LCIP4_CRED, 
        CONCAT(CASE WHEN [COSC_GRAD_STATUS_LGDS_CD] IS NULL THEN Null ELSE CAST([COSC_GRAD_STATUS_LGDS_CD] AS NVARCHAR(50)) + ' - ' END, Left([CIP],2), ' - ' 
			, [T_PSSM_Projection_Cred_Grp].[PSSM_Credential]) AS LCIP2_CRED, 
        T_Predict_CIP_CRED_AGE_Flipped.AGE, 
        T_Predict_CIP_CRED_AGE_Flipped.Year, 
       Sum(T_Predict_CIP_CRED_AGE_Flipped.Count) AS [Count]
INTO qry_10a_Program_Dist_Count 
FROM    T_Predict_CIP_CRED_AGE_Flipped 
INNER JOIN T_PSSM_Projection_Cred_Grp 
  ON T_Predict_CIP_CRED_AGE_Flipped.CRED = T_PSSM_Projection_Cred_Grp.PSSM_Projection_Credential
WHERE   (((T_PSSM_Projection_Cred_Grp.PSSM_Credential) Not In ('APPRAPPR','APPRCERT','GRCT or GRDP','PDEG','MAST','DOCT')))
GROUP BY T_PSSM_Projection_Cred_Grp.PSSM_Credential, 
         CONCAT(CASE WHEN [COSC_GRAD_STATUS_LGDS_CD] IS NULL THEN Null ELSE CAST([COSC_GRAD_STATUS_LGDS_CD] AS NVARCHAR(50)) + ' - ' END, [T_PSSM_Projection_Cred_Grp].[PSSM_Credential]), 
		    T_Predict_CIP_CRED_AGE_Flipped.CIP, 
         CONCAT(CASE WHEN [COSC_GRAD_STATUS_LGDS_CD] IS NULL THEN Null ELSE CAST([COSC_GRAD_STATUS_LGDS_CD] AS NVARCHAR(50)) + ' - ' END, [CIP], ' - ' 
			, [T_PSSM_Projection_Cred_Grp].[PSSM_Credential]) , 
        CONCAT(CASE WHEN [COSC_GRAD_STATUS_LGDS_CD] IS NULL THEN Null ELSE CAST([COSC_GRAD_STATUS_LGDS_CD] AS NVARCHAR(50)) + ' - ' END, Left([CIP],2), ' - ' 
			, [T_PSSM_Projection_Cred_Grp].[PSSM_Credential]), 
        T_Predict_CIP_CRED_AGE_Flipped.AGE, 
        T_Predict_CIP_CRED_AGE_Flipped.Year;"

# ---- qry_10b_Program_Dist_Total ----
qry_10b_Program_Dist_Total <- 
"SELECT qry_10a_Program_Dist_Count.PSSM_Credential, 
        qry_10a_Program_Dist_Count.PSSM_CRED, 
        qry_10a_Program_Dist_Count.AGE, 
        qry_10a_Program_Dist_Count.Year, 
        Sum(qry_10a_Program_Dist_Count.Count) AS Totals
INTO    qry_10b_Program_Dist_Total
FROM    qry_10a_Program_Dist_Count
GROUP BY qry_10a_Program_Dist_Count.PSSM_Credential, 
        qry_10a_Program_Dist_Count.PSSM_CRED, 
        qry_10a_Program_Dist_Count.AGE, 
        qry_10a_Program_Dist_Count.Year;"

# ---- qry_10c_Program_Dist_Distribution ---- 
qry_10c_Program_Dist_Distribution <- 
"INSERT INTO Cohort_Program_Distributions_Projected 
( Survey, PSSM_Credential, PSSM_CRED, LCP4_CD, LCIP4_CRED, LCIP2_CRED, Age_Group, [Year], [Count], Total, [Percent] )
SELECT 'Program_Projections_2023-2024_qry10c' AS Survey, 
        qry_10a_Program_Dist_Count.PSSM_Credential, 
        qry_10a_Program_Dist_Count.PSSM_CRED, 
        qry_10a_Program_Dist_Count.CIP AS LCP4_CD, 
        qry_10a_Program_Dist_Count.LCIP4_CRED, 
        qry_10a_Program_Dist_Count.LCIP2_CRED, 
        qry_10a_Program_Dist_Count.AGE AS Age_Group, 
        qry_10a_Program_Dist_Count.Year, 
        qry_10a_Program_Dist_Count.Count, 
        qry_10b_Program_Dist_Total.Totals, 
        CASE WHEN Totals = 0 THEN 0 ELSE CAST([Count] AS FLOAT)/CAST([Totals] AS FLOAT) END AS [Percent]
FROM    qry_10a_Program_Dist_Count 
INNER JOIN qry_10b_Program_Dist_Total 
  ON    (qry_10a_Program_Dist_Count.Year = qry_10b_Program_Dist_Total.Year) 
  AND   (qry_10a_Program_Dist_Count.AGE = qry_10b_Program_Dist_Total.AGE) 
  AND   (qry_10a_Program_Dist_Count.PSSM_CRED = qry_10b_Program_Dist_Total.PSSM_CRED);"

# ---- qry_12_LCP4_LCIPPC_Recode_9999 ----
qry_12_LCP4_LCIPPC_Recode_9999 <- 
"SELECT INFOWARE_L_CIP_6DIGITS_CIP2016.LCIP_LCP4_CD, 
        IIf([LCIP_LCP4_CD]='9999','99',[INFOWARE_L_CIP_6DIGITS_CIP2016].[LCIP_LCIPPC_CD]) AS LCIP_LCIPPC_CD
INTO    qry_12_LCP4_LCIPPC_Recode_9999
FROM    INFOWARE_L_CIP_6DIGITS_CIP2016
GROUP BY INFOWARE_L_CIP_6DIGITS_CIP2016.LCIP_LCP4_CD, 
        IIf([LCIP_LCP4_CD]='9999','99',[INFOWARE_L_CIP_6DIGITS_CIP2016].[LCIP_LCIPPC_CD]);"

# ---- qry_12a_Program_Dist_Count ----
qry_12a_Program_Dist_Count <- "
SELECT T_PSSM_Projection_Cred_Grp.PSSM_Credential, 
        CONCAT(CASE WHEN [COSC_GRAD_STATUS_LGDS_CD] IS NULL THEN Null ELSE CAST([COSC_GRAD_STATUS_LGDS_CD] AS NVARCHAR(50)) + ' - ' END, [T_PSSM_Projection_Cred_Grp].[PSSM_Credential]) AS PSSM_CRED, 
        qry_12_LCP4_LCIPPC_Recode_9999.LCIP_LCIPPC_CD AS LCIPPC_CD, 
        CONCAT(CASE WHEN [COSC_GRAD_STATUS_LGDS_CD] IS NULL THEN Null ELSE CAST([COSC_GRAD_STATUS_LGDS_CD] AS NVARCHAR(50)) + ' - ' END, [LCIP_LCIPPC_CD], ' - ', [T_PSSM_Projection_Cred_Grp].[PSSM_Credential]) AS LCIPPC_CRED, 
        T_Predict_CIP_CRED_AGE_Flipped.AGE AS Age_Group, 
        T_Predict_CIP_CRED_AGE_Flipped.Year, 
        Sum(T_Predict_CIP_CRED_AGE_Flipped.Count) AS [Count]
INTO    qry_12a_Program_Dist_Count
FROM    (T_Predict_CIP_CRED_AGE_Flipped INNER JOIN T_PSSM_Projection_Cred_Grp ON T_Predict_CIP_CRED_AGE_Flipped.CRED = T_PSSM_Projection_Cred_Grp.PSSM_Projection_Credential) 
INNER JOIN qry_12_LCP4_LCIPPC_Recode_9999
ON      T_Predict_CIP_CRED_AGE_Flipped.CIP = qry_12_LCP4_LCIPPC_Recode_9999.LCIP_LCP4_CD
WHERE   (((T_PSSM_Projection_Cred_Grp.PSSM_Credential) In ('GRCT or GRDP','PDEG','MAST','DOCT')))
GROUP BY T_PSSM_Projection_Cred_Grp.PSSM_Credential, 
        CONCAT(CASE WHEN [COSC_GRAD_STATUS_LGDS_CD] IS NULL THEN Null ELSE CAST([COSC_GRAD_STATUS_LGDS_CD] AS NVARCHAR(50)) + ' - ' END, [T_PSSM_Projection_Cred_Grp].[PSSM_Credential]), 
        qry_12_LCP4_LCIPPC_Recode_9999.LCIP_LCIPPC_CD, 
		CONCAT(CASE WHEN [COSC_GRAD_STATUS_LGDS_CD] IS NULL THEN Null ELSE CAST([COSC_GRAD_STATUS_LGDS_CD] AS NVARCHAR(50)) + ' - ' END, [LCIP_LCIPPC_CD], ' - ', [T_PSSM_Projection_Cred_Grp].[PSSM_Credential]), 
        T_Predict_CIP_CRED_AGE_Flipped.AGE, 
		T_Predict_CIP_CRED_AGE_Flipped.Year;"

# ---- qry_12b_Program_Dist_Total ----
qry_12b_Program_Dist_Total <-
"SELECT qry_12a_Program_Dist_Count.PSSM_Credential, 
        qry_12a_Program_Dist_Count.PSSM_CRED, 
        qry_12a_Program_Dist_Count.Age_Group, 
        qry_12a_Program_Dist_Count.Year, 
        Sum(qry_12a_Program_Dist_Count.Count) AS Totals
INTO qry_12b_Program_Dist_Total
FROM qry_12a_Program_Dist_Count
GROUP BY qry_12a_Program_Dist_Count.PSSM_Credential, 
         qry_12a_Program_Dist_Count.PSSM_CRED, 
         qry_12a_Program_Dist_Count.Age_Group, 
         qry_12a_Program_Dist_Count.Year;"

# ---- qry_12c_Program_Dist_Distribution ---- 
qry_12c_Program_Dist_Distribution <-
"INSERT INTO Cohort_Program_Distributions_Projected 
( Survey, PSSM_Credential, PSSM_CRED, LCP4_CD, LCIP4_CRED, Age_Group, [Year], [Count], Total, [Percent] )
SELECT 'Program_Projections_2023-2024_qry12c' AS Survey, 
        qry_12a_Program_Dist_Count.PSSM_Credential, 
        qry_12a_Program_Dist_Count.PSSM_CRED, 
        qry_12a_Program_Dist_Count.LCIPPC_CD, 
        qry_12a_Program_Dist_Count.LCIPPC_CRED, 
        qry_12a_Program_Dist_Count.Age_Group, 
        qry_12a_Program_Dist_Count.Year, 
        qry_12a_Program_Dist_Count.Count, 
        qry_12b_Program_Dist_Total.Totals, 
        IIf(([Totals]=0),0,[Count]/[Totals]) AS [%]
FROM    qry_12a_Program_Dist_Count 
INNER JOIN qry_12b_Program_Dist_Total 
  ON    (qry_12a_Program_Dist_Count.PSSM_CRED = qry_12b_Program_Dist_Total.PSSM_CRED) 
  AND   (qry_12a_Program_Dist_Count.Age_Group = qry_12b_Program_Dist_Total.Age_Group) 
  AND   (qry_12a_Program_Dist_Count.Year = qry_12b_Program_Dist_Total.Year);"

# ---- qry_12d_Check_Missing ---- 
qry_12d_Check_Missing <- 
"SELECT Cohort_Program_Distributions_Static.PSSM_Credential, 
        Cohort_Program_Distributions_Static.PSSM_CRED, 
        Cohort_Program_Distributions_Static.LCP4_CD, 
        Cohort_Program_Distributions_Static.LCIP4_CRED, 
        Cohort_Program_Distributions_Static.Age_Group, 
        Cohort_Program_Distributions_Static.Year, 
        Cohort_Program_Distributions_Projected.PSSM_Credential, 
        Cohort_Program_Distributions_Projected.PSSM_CRED, 
        Cohort_Program_Distributions_Projected.LCP4_CD, 
        Cohort_Program_Distributions_Projected.Age_Group, 
        Cohort_Program_Distributions_Projected.Year, 
        Cohort_Program_Distributions_Static.Count
FROM    Cohort_Program_Distributions_Static 
LEFT JOIN Cohort_Program_Distributions_Projected 
  ON    (Cohort_Program_Distributions_Static.Year = Cohort_Program_Distributions_Projected.Year) 
  AND   (Cohort_Program_Distributions_Static.Age_Group = Cohort_Program_Distributions_Projected.Age_Group) 
  AND   (Cohort_Program_Distributions_Static.LCP4_CD = Cohort_Program_Distributions_Projected.LCP4_CD) 
  AND   (Cohort_Program_Distributions_Static.PSSM_CRED = Cohort_Program_Distributions_Projected.PSSM_CRED) 
  AND   (Cohort_Program_Distributions_Static.PSSM_Credential = Cohort_Program_Distributions_Projected.PSSM_Credential)
WHERE (((Cohort_Program_Distributions_Static.Age_Group) 
      Not In ('15 to 16','65 to 89')) 
      AND ((Cohort_Program_Distributions_Projected.PSSM_Credential) Is Null) 
      AND ((Cohort_Program_Distributions_Projected.PSSM_CRED) Is Null) 
      AND ((Cohort_Program_Distributions_Projected.LCP4_CD) Is Null) 
      AND ((Cohort_Program_Distributions_Projected.Age_Group) Is Null) 
      AND ((Cohort_Program_Distributions_Projected.Year) Is Null));"

# ---- qry_13a0_Delete_Near_Completers_Projected ---- 
qry_13a0_Delete_Near_Completers_Projected <- 
"DELETE 
FROM Cohort_Program_Distributions_Projected
WHERE (((Cohort_Program_Distributions_Projected.PSSM_CRED) Like '3 - %'));"

# ---- qry_13a0_Delete_Near_Completers_Static ---- 
qry_13a0_Delete_Near_Completers_Static <- 
"DELETE 
FROM Cohort_Program_Distributions_Static
WHERE (((Cohort_Program_Distributions_Static.PSSM_CRED) Like '3 - %'));"

# ---- qry_13a_Near_completers ---- 
qry_13a_Near_completers <- 
"SELECT T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN.PSSM_Credential, 
        T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN.PSSM_CRED, 
        T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN.LCP4_CD, 
        T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN.COSC_GRAD_STATUS_LGDS_CD_Group, 
        T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN.TTRAIN AS COSC_TTRAIN, 
        T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN.LCIP4_CRED, 
        CAST([COSC_GRAD_STATUS_LGDS_CD_Group] as NVARCHAR(50)) + ' - ' + Left([LCP4_CD],2) + ' - ' + CAST([TTRAIN] as NVARCHAR(50)) + ' - ' + [PSSM_Credential] AS LCIP2_CRED, 
        T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN.Age_Group as AgeGroup, 
        Sum(T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN.[Near_completers_STP_Credentials]) AS [Count]
INTO    qry_13a_Near_completers
FROM    T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN
GROUP BY T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN.PSSM_Credential, 
        T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN.PSSM_CRED, 
        T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN.LCP4_CD, 
        T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN.COSC_GRAD_STATUS_LGDS_CD_Group, 
        T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN.TTRAIN, 
        T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN.LCIP4_CRED, 
        CAST([COSC_GRAD_STATUS_LGDS_CD_Group] as NVARCHAR(50)) + ' - ' + Left([LCP4_CD],2) + ' - ' + CAST([TTRAIN] as NVARCHAR(50)) + ' - ' + [PSSM_Credential], 
        T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN.Age_Group;"

# ---- qry_13b_Near_Completers_Total ---- 
qry_13b_Near_Completers_Total <- 
"SELECT qry_13a_Near_completers.PSSM_Credential, 
        qry_13a_Near_completers.PSSM_CRED, 
        qry_13a_Near_completers.AgeGroup, 
        Sum(qry_13a_Near_completers.Count) AS Totals
INTO    qry_13b_Near_Completers_Total
FROM    qry_13a_Near_completers
GROUP BY qry_13a_Near_completers.PSSM_Credential, 
        qry_13a_Near_completers.PSSM_CRED, 
        qry_13a_Near_completers.AgeGroup;"

# ---- qry_13c_Near_Completers_Program_Dist ---- 
qry_13c_Near_Completers_Program_Dist <- 
"SELECT qry_13a_Near_completers.PSSM_Credential, 
        qry_13a_Near_completers.PSSM_CRED, 
        qry_13a_Near_completers.LCP4_CD, 
        qry_13a_Near_completers.COSC_GRAD_STATUS_LGDS_CD_Group, 
        qry_13a_Near_completers.COSC_TTRAIN, 
        qry_13a_Near_completers.LCIP4_CRED, 
        qry_13a_Near_completers.LCIP2_CRED, 
        qry_13a_Near_completers.AgeGroup, 
        qry_13a_Near_completers.Count, 
        qry_13b_Near_Completers_Total.Totals, 
        IIf([Totals]=0,0, cast(Count AS float)/cast(Totals as float)) AS [%]
INTO    qry_13c_Near_Completers_Program_Dist
FROM    qry_13b_Near_Completers_Total 
INNER JOIN qry_13a_Near_completers 
  ON    (qry_13b_Near_Completers_Total.AgeGroup = qry_13a_Near_completers.AgeGroup) 
  AND   (qry_13b_Near_Completers_Total.PSSM_CRED = qry_13a_Near_completers.PSSM_CRED);"


# ---- qry_13d_Append_Near_Completers_Program_Dist_Projected_TTRAIN ---- 
qry_13d_Append_Near_Completers_Program_Dist_Projected_TTRAIN <- 
"INSERT INTO  Cohort_Program_Distributions_Projected 
        (Survey, PSSM_Credential, PSSM_CRED, LCP4_CD, GRAD_STATUS, TTRAIN, LCIP4_CRED, LCIP2_CRED, Age_Group, [Year], [Count], Total, [Percent] )
SELECT 'Program_Projections_2023-2024_qry_13d' AS Survey, 
        qry_13c_Near_Completers_Program_Dist.PSSM_Credential, 
        qry_13c_Near_Completers_Program_Dist.PSSM_CRED, 
        qry_13c_Near_Completers_Program_Dist.LCP4_CD, 
        qry_13c_Near_Completers_Program_Dist.COSC_GRAD_STATUS_LGDS_CD_Group, 
        qry_13c_Near_Completers_Program_Dist.COSC_TTRAIN, 
        qry_13c_Near_Completers_Program_Dist.LCIP4_CRED, 
        qry_13c_Near_Completers_Program_Dist.LCIP2_CRED, 
        tbl_Age_Groups_Near_Completers.Age_Group_Label_Graduate_Projection AS AgeGroup, '2023/2024' AS Projection_Year, 
        qry_13c_Near_Completers_Program_Dist.Count, qry_13c_Near_Completers_Program_Dist.Totals, 
        qry_13c_Near_Completers_Program_Dist.[%]
FROM    qry_13c_Near_Completers_Program_Dist 
INNER JOIN tbl_Age_Groups_Near_Completers 
  ON    qry_13c_Near_Completers_Program_Dist.AgeGroup = tbl_Age_Groups_Near_Completers.Age_Group_Label_Near_Completer_Projection;"


# ---- qry_13d_Append_Near_Completers_Program_Dist_Static_TTRAIN ---- 
qry_13d_Append_Near_Completers_Program_Dist_Static_TTRAIN <- 
"INSERT INTO Cohort_Program_Distributions_Static 
        ( Survey, PSSM_Credential, PSSM_CRED, LCP4_CD, GRAD_STATUS, TTRAIN, LCIP4_CRED, LCIP2_CRED, Age_Group, [Year], [Count], Total, [Percent] )
SELECT 'Program_Projections_2023-2024_qry_13d' AS Survey, 
        qry_13c_Near_Completers_Program_Dist.PSSM_Credential, 
        qry_13c_Near_Completers_Program_Dist.PSSM_CRED, 
        qry_13c_Near_Completers_Program_Dist.LCP4_CD, 
        qry_13c_Near_Completers_Program_Dist.COSC_GRAD_STATUS_LGDS_CD_Group, 
        qry_13c_Near_Completers_Program_Dist.COSC_TTRAIN, 
        qry_13c_Near_Completers_Program_Dist.LCIP4_CRED, 
        qry_13c_Near_Completers_Program_Dist.LCIP2_CRED, 
        tbl_Age_Groups_Near_Completers.Age_Group_Label_Graduate_Projection AS AgeGroup, '2023/2024' AS Projection_Year, 
        qry_13c_Near_Completers_Program_Dist.Count, 
        qry_13c_Near_Completers_Program_Dist.Totals, 
        qry_13c_Near_Completers_Program_Dist.[%]
FROM    qry_13c_Near_Completers_Program_Dist 
INNER JOIN tbl_Age_Groups_Near_Completers 
  ON    qry_13c_Near_Completers_Program_Dist.AgeGroup = tbl_Age_Groups_Near_Completers.Age_Group_Label_Near_Completer_Projection;"

# ---- NOT USED ----

# ---- qry_13c2_Near_Completers_Program_Dist_TTRAIN_not_used ---- 
qry_13c2_Near_Completers_Program_Dist_TTRAIN_not_used <- 
  "SELECT qry_13c_Near_Completers_Program_Dist.PSSM_Credential, 
        qry_13c_Near_Completers_Program_Dist.PSSM_CRED, 
        qry_13c_Near_Completers_Program_Dist.LCP4_CD,'3' AS GRADSTAT, '0' AS TTRAIN, '3 - ' + [LCP4_CD] + ' - 0 - ' + [PSSM_Credential] AS LCIP4_CRED, 
        qry_13c_Near_Completers_Program_Dist.AgeGroup, qry_13c_Near_Completers_Program_Dist.Count, qry_13c_Near_Completers_Program_Dist.Totals, 
        qry_13c_Near_Completers_Program_Dist.[%]
FROM    qry_13c_Near_Completers_Program_Dist
UNION ALL 
SELECT  qry_13c_Near_Completers_Program_Dist.PSSM_Credential, 
        qry_13c_Near_Completers_Program_Dist.PSSM_CRED, 
        qry_13c_Near_Completers_Program_Dist.LCP4_CD,'3' AS GRADSTAT, '1' AS TTRAIN, '3 - ' + [LCP4_CD] + ' - 1 - ' + [PSSM_Credential] AS LCIP4_CRED, 
        qry_13c_Near_Completers_Program_Dist.AgeGroup, qry_13c_Near_Completers_Program_Dist.Count, qry_13c_Near_Completers_Program_Dist.Totals, 
        qry_13c_Near_Completers_Program_Dist.[%]
FROM    qry_13c_Near_Completers_Program_Dist
UNION ALL 
SELECT  qry_13c_Near_Completers_Program_Dist.PSSM_Credential, 
        qry_13c_Near_Completers_Program_Dist.PSSM_CRED, 
        qry_13c_Near_Completers_Program_Dist.LCP4_CD, '3' AS GRADSTAT, '2' AS TTRAIN, '3 - ' + [LCP4_CD] + ' - 2 - ' + [PSSM_Credential] AS LCIP4_CRED, 
        qry_13c_Near_Completers_Program_Dist.AgeGroup, qry_13c_Near_Completers_Program_Dist.Count, qry_13c_Near_Completers_Program_Dist.Totals, 
        qry_13c_Near_Completers_Program_Dist.[%]
FROM    qry_13c_Near_Completers_Program_Dist;"