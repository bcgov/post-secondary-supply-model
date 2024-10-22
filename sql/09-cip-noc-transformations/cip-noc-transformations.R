# ******************************************************************************
# CIP-NOC Preparation Work ----

## qry_Make_T_Cohorts_Recoded_for_CIP_NOC ----
qry_Make_T_Cohorts_Recoded_for_CIP_NOC <-
"SELECT T_Cohorts_Recoded.PEN, 
T_Cohorts_Recoded.STQU_ID, 
T_Cohorts_Recoded.Survey, 
T_Cohorts_Recoded.Survey_Year, 
T_Cohorts_Recoded.INST_CD, 
T_Cohorts_Recoded.LCIP_CD, 
T_Cohorts_Recoded.LCP4_CD, 
T_Cohorts_Recoded.NOC_CD, 
T_Cohorts_Recoded.AGE_AT_SURVEY, 
T_Cohorts_Recoded.Age_Group, 
T_Cohorts_Recoded.Age_Group_Rollup, 
T_Cohorts_Recoded.AGE_AT_GRAD, 
T_Cohorts_Recoded.Age_Grad_Group, 
T_Cohorts_Recoded.Age_Grad_Group_Rollup, 
T_Cohorts_Recoded.GRAD_STATUS, 
T_Cohorts_Recoded.Respondent, 
T_Cohorts_Recoded.New_Labour_Supply, 
T_Cohorts_Recoded.Old_Labour_Supply, 
T_Cohorts_Recoded.Weight, 
T_Cohorts_Recoded.PSSM_Credential, 
T_Cohorts_Recoded.PSSM_CRED, 
T_Cohorts_Recoded.LCIP4_CRED, 
T_Cohorts_Recoded.CURRENT_REGION_PSSM_CODE, 
T_Cohorts_Recoded.Weight_NLS, 
T_Cohorts_Recoded.Weight_OCC, 
T_Cohorts_Recoded.Weight_Age, 
T_Cohorts_Recoded.New_Labour_Supply_CIP_NOC, 
T_Cohorts_Recoded.Weight_NLS_CIP_NOC, 
T_Cohorts_Recoded.Weight_OCC_CIP_NOC, 
T_Cohorts_Recoded.ENDDT 
INTO T_Cohorts_Recoded_CIP_NOC
FROM T_Cohorts_Recoded
WHERE (((T_Cohorts_Recoded.Survey_Year)='2019' 
        Or (T_Cohorts_Recoded.Survey_Year)='2020' 
        Or (T_Cohorts_Recoded.Survey_Year)='2021'
        Or (T_Cohorts_Recoded.Survey_Year)='2022' 
        Or (T_Cohorts_Recoded.Survey_Year)='2023'))"

## CIP_NOC_Update_NewLabourSupply_CIP_NOC ----
CIP_NOC_Update_NewLabourSupply_CIP_NOC <-
"UPDATE T_Cohorts_Recoded_CIP_NOC
SET T_Cohorts_Recoded_CIP_NOC.New_Labour_Supply_CIP_NOC = T_Cohorts_Recoded_CIP_NOC.New_Labour_Supply"

## DACSO_Q005_DACSO_DATA_Part_1c_NLS1 ----
DACSO_Q005_DACSO_DATA_Part_1c_NLS1_CIP_NOC <-
"SELECT T_Cohorts_Recoded_CIP_NOC.Survey, 
T_Current_Region_PSSM_Rollup_Codes.Current_Region_PSSM_Code_Rollup, 
T_Cohorts_Recoded_CIP_NOC.Age_Group_Rollup, 
T_Cohorts_Recoded_CIP_NOC.INST_CD, 
T_Cohorts_Recoded_CIP_NOC.LCIP4_CRED, 
Count(*) AS Base, 
T_Cohorts_Recoded_CIP_NOC.GRAD_STATUS, 
T_Cohorts_Recoded_CIP_NOC.New_Labour_Supply_CIP_NOC
INTO DACSO_Q005_DACSO_DATA_Part_1c_NLS1_CIP_NOC
FROM T_Cohorts_Recoded_CIP_NOC 
INNER JOIN (T_Current_Region_PSSM_Codes 
INNER JOIN T_Current_Region_PSSM_Rollup_Codes 
ON T_Current_Region_PSSM_Codes.Current_Region_PSSM_Code_Rollup = T_Current_Region_PSSM_Rollup_Codes.Current_Region_PSSM_Code_Rollup) 
ON T_Cohorts_Recoded_CIP_NOC.[CURRENT_REGION_PSSM_CODE] = T_Current_Region_PSSM_Codes.Current_Region_PSSM_Code
WHERE (((T_Cohorts_Recoded_CIP_NOC.AGE_AT_SURVEY)>=17 And (T_Cohorts_Recoded_CIP_NOC.AGE_AT_SURVEY)<=34) 
AND ((T_Cohorts_Recoded_CIP_NOC.Weight)>0) AND ((T_Cohorts_Recoded_CIP_NOC.NOC_CD) Is Not Null))
GROUP BY T_Cohorts_Recoded_CIP_NOC.Survey, 
T_Current_Region_PSSM_Rollup_Codes.Current_Region_PSSM_Code_Rollup, 
T_Cohorts_Recoded_CIP_NOC.Age_Group_Rollup, 
T_Cohorts_Recoded_CIP_NOC.INST_CD, 
T_Cohorts_Recoded_CIP_NOC.LCIP4_CRED, 
T_Cohorts_Recoded_CIP_NOC.GRAD_STATUS, 
T_Cohorts_Recoded_CIP_NOC.New_Labour_Supply_CIP_NOC
HAVING (((Count(*))>0) 
AND ((T_Cohorts_Recoded_CIP_NOC.GRAD_STATUS)='1' 
Or (T_Cohorts_Recoded_CIP_NOC.GRAD_STATUS)='3') 
AND ((T_Cohorts_Recoded_CIP_NOC.New_Labour_Supply_CIP_NOC)=1))"

## DACSO_Q005_DACSO_DATA_Part_1c_NLS2 ----
DACSO_Q005_DACSO_DATA_Part_1c_NLS2_CIP_NOC <-
"SELECT T_Cohorts_Recoded_CIP_NOC.Survey, 
T_Current_Region_PSSM_Rollup_Codes.Current_Region_PSSM_Code_Rollup, 
T_Cohorts_Recoded_CIP_NOC.Age_Group_Rollup, T_Cohorts_Recoded_CIP_NOC.INST_CD, 
T_Cohorts_Recoded_CIP_NOC.LCIP4_CRED, T_Cohorts_Recoded_CIP_NOC.GRAD_STATUS, 
T_Cohorts_Recoded_CIP_NOC.New_Labour_Supply_CIP_NOC, 
T_Cohorts_Recoded_CIP_NOC.STQU_ID
INTO DACSO_Q005_DACSO_DATA_Part_1c_NLS2_CIP_NOC
FROM T_Cohorts_Recoded_CIP_NOC 
INNER JOIN (T_Current_Region_PSSM_Codes 
INNER JOIN T_Current_Region_PSSM_Rollup_Codes 
ON T_Current_Region_PSSM_Codes.Current_Region_PSSM_Code_Rollup = T_Current_Region_PSSM_Rollup_Codes.Current_Region_PSSM_Code_Rollup) 
ON T_Cohorts_Recoded_CIP_NOC.[CURRENT_REGION_PSSM_CODE] = T_Current_Region_PSSM_Codes.Current_Region_PSSM_Code
WHERE (((T_Cohorts_Recoded_CIP_NOC.AGE_AT_SURVEY)>=17 
And (T_Cohorts_Recoded_CIP_NOC.AGE_AT_SURVEY)<=34) 
AND ((T_Cohorts_Recoded_CIP_NOC.Weight)>0))
GROUP BY T_Cohorts_Recoded_CIP_NOC.Survey, 
T_Current_Region_PSSM_Rollup_Codes.Current_Region_PSSM_Code_Rollup, 
T_Cohorts_Recoded_CIP_NOC.Age_Group_Rollup, T_Cohorts_Recoded_CIP_NOC.INST_CD, 
T_Cohorts_Recoded_CIP_NOC.LCIP4_CRED, T_Cohorts_Recoded_CIP_NOC.GRAD_STATUS, 
T_Cohorts_Recoded_CIP_NOC.New_Labour_Supply_CIP_NOC, 
T_Cohorts_Recoded_CIP_NOC.STQU_ID
HAVING (((T_Cohorts_Recoded_CIP_NOC.GRAD_STATUS)='1'
Or (T_Cohorts_Recoded_CIP_NOC.GRAD_STATUS)='3') 
AND ((T_Cohorts_Recoded_CIP_NOC.New_Labour_Supply_CIP_NOC)=2))"

