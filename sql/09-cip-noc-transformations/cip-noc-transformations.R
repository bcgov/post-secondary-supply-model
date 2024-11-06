# ******************************************************************************
# CIP-NOC Preparation Work Labour Supply ----

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

## DACSO_Q005_DACSO_DATA_Part_1c_NLS2_Recode_CIP_NOC ----
# updated from a distinctrow query
DACSO_Q005_DACSO_DATA_Part_1c_NLS2_Recode_CIP_NOC <- "
UPDATE T_Cohorts_Recoded_CIP_NOC 
SET T_Cohorts_Recoded_CIP_NOC.New_Labour_Supply_CIP_NOC = 3 
WHERE T_Cohorts_Recoded_CIP_NOC.STQU_ID IN (
SELECT T_Cohorts_Recoded_CIP_NOC.STQU_ID 
FROM      dacso_q005_dacso_data_part_1c_nls2_CIP_NOC
INNER JOIN T_Cohorts_Recoded_CIP_NOC
ON        dacso_q005_dacso_data_part_1c_nls2_CIP_NOC.stqu_id = T_Cohorts_Recoded_CIP_NOC.stqu_id
LEFT JOIN dacso_q005_dacso_data_part_1c_nls1_CIP_NOC
ON        dacso_q005_dacso_data_part_1c_nls2_CIP_NOC.lcip4_cred = dacso_q005_dacso_data_part_1c_nls1_CIP_NOC.lcip4_cred
  AND     dacso_q005_dacso_data_part_1c_nls2_CIP_NOC.inst_cd = dacso_q005_dacso_data_part_1c_nls1_CIP_NOC.inst_cd
  AND     dacso_q005_dacso_data_part_1c_nls2_CIP_NOC.age_group_rollup = dacso_q005_dacso_data_part_1c_nls1_CIP_NOC.age_group_rollup
  AND     dacso_q005_dacso_data_part_1c_nls2_CIP_NOC.current_region_pssm_code_rollup = dacso_q005_dacso_data_part_1c_nls1_CIP_NOC.current_region_pssm_code_rollup
  AND     dacso_q005_dacso_data_part_1c_nls2_CIP_NOC.survey = dacso_q005_dacso_data_part_1c_nls1_CIP_NOC.survey
WHERE     dacso_q005_dacso_data_part_1c_nls1_CIP_NOC.survey IS NULL
  AND     dacso_q005_dacso_data_part_1c_nls1_CIP_NOC.current_region_pssm_code_rollup IS NULL
  AND     dacso_q005_dacso_data_part_1c_nls1_CIP_NOC.age_group_rollup IS NULL
  AND     dacso_q005_dacso_data_part_1c_nls1_CIP_NOC.inst_cd IS NULL
  AND     dacso_q005_dacso_data_part_1c_nls1_CIP_NOC.lcip4_cred IS NULL);"


## DACSO_Q005_Z01_Base_NLS_CIP_NOC ----
DACSO_Q005_Z01_Base_NLS_CIP_NOC <-
  "SELECT T_Cohorts_Recoded_CIP_NOC.Survey, 
T_Cohorts_Recoded_CIP_NOC.INST_CD, 
T_Cohorts_Recoded_CIP_NOC.Age_Group_Rollup,
T_Cohorts_Recoded_CIP_NOC.LCIP4_CRED, 
Count(*) AS Base, 
T_Cohorts_Recoded_CIP_NOC.STQU_ID
INTO DACSO_Q005_Z01_Base_NLS_CIP_NOC
FROM T_Cohorts_Recoded_CIP_NOC
WHERE (((T_Cohorts_Recoded_CIP_NOC.New_Labour_Supply_CIP_NOC)=0 
Or (T_Cohorts_Recoded_CIP_NOC.New_Labour_Supply_CIP_NOC)=1 
Or (T_Cohorts_Recoded_CIP_NOC.New_Labour_Supply_CIP_NOC)=2 
Or (T_Cohorts_Recoded_CIP_NOC.New_Labour_Supply_CIP_NOC)=3) 
AND ((T_Cohorts_Recoded_CIP_NOC.AGE_AT_SURVEY)>=17 
And (T_Cohorts_Recoded_CIP_NOC.AGE_AT_SURVEY)<=34) 
AND ((T_Cohorts_Recoded_CIP_NOC.Weight)>0))
GROUP BY T_Cohorts_Recoded_CIP_NOC.Survey, 
T_Cohorts_Recoded_CIP_NOC.INST_CD, 
T_Cohorts_Recoded_CIP_NOC.Age_Group_Rollup, 
T_Cohorts_Recoded_CIP_NOC.LCIP4_CRED, 
T_Cohorts_Recoded_CIP_NOC.STQU_ID, 
T_Cohorts_Recoded_CIP_NOC.GRAD_STATUS
HAVING (((T_Cohorts_Recoded_CIP_NOC.Age_Group_Rollup) Is Not Null) 
AND ((T_Cohorts_Recoded_CIP_NOC.GRAD_STATUS)='1'))"

## DACSO_Q005_Z02_Weight_CIP_NOC ----
# two queries to complete existing one
DACSO_Q005_Z02_Weight_CIP_NOC_tmp <-
"SELECT T_Cohorts_Recoded_CIP_NOC.survey,
T_Cohorts_Recoded_CIP_NOC.survey_year,
T_Cohorts_Recoded_CIP_NOC.inst_cd,
T_Cohorts_Recoded_CIP_NOC.age_group_rollup,
T_Cohorts_Recoded_CIP_NOC.grad_status,
T_Cohorts_Recoded_CIP_NOC.lcip4_cred,
Count(*) AS Count,
Sum(CASE WHEN respondent = '1' THEN 1 ELSE 0 END) AS Respondents,
T_Cohorts_Recoded_CIP_NOC.weight AS weight_year
INTO DACSO_Q005_Z02_Weight_CIP_NOC_tmp
FROM   T_Cohorts_Recoded_CIP_NOC
WHERE (((T_Cohorts_Recoded_CIP_NOC.New_Labour_Supply_CIP_NOC)=0 
Or (T_Cohorts_Recoded_CIP_NOC.New_Labour_Supply_CIP_NOC)=1 
Or (T_Cohorts_Recoded_CIP_NOC.New_Labour_Supply_CIP_NOC)=2 
Or (T_Cohorts_Recoded_CIP_NOC.New_Labour_Supply_CIP_NOC)=3) 
AND ((T_Cohorts_Recoded_CIP_NOC.Weight)>0) 
AND ((T_Cohorts_Recoded_CIP_NOC.AGE_AT_SURVEY)>=17 
AND (T_Cohorts_Recoded_CIP_NOC.AGE_AT_SURVEY)<=34))
GROUP  BY t_cohorts_recoded_CIP_NOC.survey,
t_cohorts_recoded_CIP_NOC.survey_year,
t_cohorts_recoded_CIP_NOC.inst_cd,
t_cohorts_recoded_CIP_NOC.age_group_rollup,
t_cohorts_recoded_CIP_NOC.grad_status,
t_cohorts_recoded_CIP_NOC.lcip4_cred,
t_cohorts_recoded_CIP_NOC.weight
HAVING (((T_Cohorts_Recoded_CIP_NOC.Age_Group_Rollup) Is Not Null) 
AND ((T_Cohorts_Recoded_CIP_NOC.GRAD_STATUS)='1') AND ((T_Cohorts_Recoded_CIP_NOC.Weight)>0));"
 
DACSO_Q005_Z02_Weight_CIP_NOC <- "
SELECT *,
    Weight_Prob * Weight_Year AS Weight,
    Respondents * Weight_Prob * Weight_Year AS Weighted
INTO DACSO_Q005_Z02_Weight_CIP_NOC
FROM
  (SELECT *, CASE WHEN (Respondents = 0) THEN 1 ELSE cast(Count as float)/cast(Respondents as float) END AS Weight_Prob
  FROM  DACSO_Q005_Z02_Weight_CIP_NOC_tmp) T;"

## DACSO_Q005_Z03_Weight_Total_CIP_NOC ----
DACSO_Q005_Z03_Weight_Total_CIP_NOC <- 
"SELECT DACSO_Q005_Z02_Weight_CIP_NOC.Survey, 
DACSO_Q005_Z02_Weight_CIP_NOC.INST_CD, 
DACSO_Q005_Z02_Weight_CIP_NOC.Age_Group_Rollup, 
DACSO_Q005_Z02_Weight_CIP_NOC.GRAD_STATUS, 
DACSO_Q005_Z02_Weight_CIP_NOC.LCIP4_CRED, 
Sum(DACSO_Q005_Z02_Weight_CIP_NOC.Count) AS Base, 
Sum(DACSO_Q005_Z02_Weight_CIP_NOC.Weighted) AS Weighted
INTO DACSO_Q005_Z03_Weight_Total_CIP_NOC
FROM DACSO_Q005_Z02_Weight_CIP_NOC
GROUP BY DACSO_Q005_Z02_Weight_CIP_NOC.Survey, 
DACSO_Q005_Z02_Weight_CIP_NOC.INST_CD, 
DACSO_Q005_Z02_Weight_CIP_NOC.Age_Group_Rollup, 
DACSO_Q005_Z02_Weight_CIP_NOC.GRAD_STATUS, 
DACSO_Q005_Z02_Weight_CIP_NOC.LCIP4_CRED;"

## DACSO_Q005_Z04_Weight_Adj_Fac_CIP_NOC ----
DACSO_Q005_Z04_Weight_Adj_Fac_CIP_NOC <- 
"SELECT DACSO_Q005_Z03_Weight_Total_CIP_NOC.Survey, 
DACSO_Q005_Z03_Weight_Total_CIP_NOC.INST_CD, 
DACSO_Q005_Z03_Weight_Total_CIP_NOC.Age_Group_Rollup, 
DACSO_Q005_Z03_Weight_Total_CIP_NOC.GRAD_STATUS, 
DACSO_Q005_Z03_Weight_Total_CIP_NOC.LCIP4_CRED, 
DACSO_Q005_Z03_Weight_Total_CIP_NOC.Base, 
DACSO_Q005_Z03_Weight_Total_CIP_NOC.Weighted, 
CASE WHEN weighted = 0 THEN 0 ELSE (Base / Weighted) END AS Weight_Adj_Fac
INTO DACSO_Q005_Z04_Weight_Adj_Fac_CIP_NOC
FROM DACSO_Q005_Z03_Weight_Total_CIP_NOC;"

## DACSO_Q005_Z05_Weight_NLS_CIP_NOC ----
DACSO_Q005_Z05_Weight_NLS_CIP_NOC <- 
"SELECT DACSO_Q005_Z02_Weight_CIP_NOC.Survey, DACSO_Q005_Z02_Weight_CIP_NOC.Survey_Year, 
DACSO_Q005_Z02_Weight_CIP_NOC.INST_CD, DACSO_Q005_Z02_Weight_CIP_NOC.Age_Group_Rollup, 
DACSO_Q005_Z02_Weight_CIP_NOC.GRAD_STATUS, DACSO_Q005_Z02_Weight_CIP_NOC.LCIP4_CRED, 
DACSO_Q005_Z02_Weight_CIP_NOC.Count, DACSO_Q005_Z02_Weight_CIP_NOC.Respondents, 
DACSO_Q005_Z02_Weight_CIP_NOC.Weight_Prob, DACSO_Q005_Z02_Weight_CIP_NOC.Weight_Year, 
DACSO_Q005_Z02_Weight_CIP_NOC.Weight, DACSO_Q005_Z02_Weight_CIP_NOC.Weighted, 
DACSO_Q005_Z04_Weight_Adj_Fac_CIP_NOC.Weight_Adj_Fac, 
[Weight]*[Weight_Adj_Fac] AS Weight_NLS_CIP_NOC 
INTO tmp_tbl_Weights_NLS_CIP_NOC
FROM DACSO_Q005_Z02_Weight_CIP_NOC 
INNER JOIN DACSO_Q005_Z04_Weight_Adj_Fac_CIP_NOC 
ON (DACSO_Q005_Z02_Weight_CIP_NOC.Age_Group_Rollup = DACSO_Q005_Z04_Weight_Adj_Fac_CIP_NOC.Age_Group_Rollup) 
AND (DACSO_Q005_Z02_Weight_CIP_NOC.LCIP4_CRED = DACSO_Q005_Z04_Weight_Adj_Fac_CIP_NOC.LCIP4_CRED) 
AND (DACSO_Q005_Z02_Weight_CIP_NOC.GRAD_STATUS = DACSO_Q005_Z04_Weight_Adj_Fac_CIP_NOC.[GRAD_STATUS]) 
AND (DACSO_Q005_Z02_Weight_CIP_NOC.Survey = DACSO_Q005_Z04_Weight_Adj_Fac_CIP_NOC.Survey) 
AND (DACSO_Q005_Z02_Weight_CIP_NOC.[INST_CD] = DACSO_Q005_Z04_Weight_Adj_Fac_CIP_NOC.[INST_CD]);"

## DACSO_Q005_Z07_Weight_NLS_Null_CIP_NOC ----
DACSO_Q005_Z07_Weight_NLS_Null_CIP_NOC <- 
  "UPDATE T_Cohorts_Recoded_CIP_NOC SET T_Cohorts_Recoded_CIP_NOC.Weight_NLS_CIP_NOC = Null;"

## DACSO_Q005_Z08_Weight_NLS_Update_CIP_NOC ----
# updated from a distinctrow query
DACSO_Q005_Z08_Weight_NLS_Update_CIP_NOC <- 
"UPDATE t_cohorts_recoded_CIP_NOC
SET    t_cohorts_recoded_CIP_NOC.weight_nls_CIP_NOC = tmp_tbl_Weights_NLS_CIP_NOC.weight_nls_CIP_NOC
FROM   tmp_tbl_Weights_NLS_CIP_NOC
INNER JOIN (t_cohorts_recoded_CIP_NOC
  INNER JOIN dacso_q005_z01_base_nls_CIP_NOC
  ON t_cohorts_recoded_CIP_NOC.stqu_id = dacso_q005_z01_base_nls_CIP_NOC.stqu_id)
ON   tmp_tbl_Weights_NLS_CIP_NOC.lcip4_cred = t_cohorts_recoded_CIP_NOC.lcip4_cred
AND tmp_tbl_Weights_NLS_CIP_NOC.grad_status =  t_cohorts_recoded_CIP_NOC.grad_status
AND tmp_tbl_Weights_NLS_CIP_NOC.age_group_rollup =  t_cohorts_recoded_CIP_NOC.age_group_rollup
AND tmp_tbl_Weights_NLS_CIP_NOC.inst_cd =  t_cohorts_recoded_CIP_NOC.inst_cd
AND tmp_tbl_Weights_NLS_CIP_NOC.survey_year = t_cohorts_recoded_CIP_NOC.survey_year
AND tmp_tbl_Weights_NLS_CIP_NOC.survey = t_cohorts_recoded_CIP_NOC.survey;"

## DACSO_Q005_Z09_Check_Weights_CIP_NOC ----
DACSO_Q005_Z09_Check_Weights_CIP_NOC <- 
  "SELECT T_Cohorts_Recoded_CIP_NOC.Survey_Year, 
T_Cohorts_Recoded_CIP_NOC.INST_CD, 
T_Cohorts_Recoded_CIP_NOC.Age_Group_Rollup, 
T_Cohorts_Recoded_CIP_NOC.GRAD_STATUS, 
T_Cohorts_Recoded_CIP_NOC.LCIP4_CRED, 
Sum(IIf([Respondent]='1',1,0)) AS Respondents, 
T_Cohorts_Recoded_CIP_NOC.Weight_NLS_CIP_NOC, 
Sum(IIf([Respondent]='1',1,0))*[Weight_NLS_CIP_NOC] AS Weighted, 
Sum(DACSO_Q005_Z01_Base_NLS_CIP_NOC.Base) AS Base
FROM T_Cohorts_Recoded_CIP_NOC 
INNER JOIN DACSO_Q005_Z01_Base_NLS_CIP_NOC 
ON T_Cohorts_Recoded_CIP_NOC.[STQU_ID] = DACSO_Q005_Z01_Base_NLS_CIP_NOC.[STQU_ID]
GROUP BY T_Cohorts_Recoded_CIP_NOC.Survey_Year, T_Cohorts_Recoded_CIP_NOC.INST_CD, 
T_Cohorts_Recoded_CIP_NOC.Age_Group_Rollup, T_Cohorts_Recoded_CIP_NOC.GRAD_STATUS, 
T_Cohorts_Recoded_CIP_NOC.LCIP4_CRED, T_Cohorts_Recoded_CIP_NOC.Weight_NLS_CIP_NOC
ORDER BY T_Cohorts_Recoded_CIP_NOC.Survey_Year, 
T_Cohorts_Recoded_CIP_NOC.Weight_NLS_CIP_NOC;"

## DACSO_Q005_Z09_Check_Weights_No_Weight_CIP_NOC ----
DACSO_Q005_Z09_Check_Weights_No_Weight_CIP_NOC <- 
"SELECT T_Cohorts_Recoded_CIP_NOC.Survey, 
T_Cohorts_Recoded_CIP_NOC.INST_CD, 
T_Cohorts_Recoded_CIP_NOC.Age_Group_Rollup, 
T_Cohorts_Recoded_CIP_NOC.LCIP4_CRED, 
Count(*) AS Base
FROM T_Cohorts_Recoded_CIP_NOC
WHERE (((T_Cohorts_Recoded_CIP_NOC.New_Labour_Supply_CIP_NOC)=0 
Or (T_Cohorts_Recoded_CIP_NOC.New_Labour_Supply_CIP_NOC)=1 
Or (T_Cohorts_Recoded_CIP_NOC.New_Labour_Supply_CIP_NOC)=2 
Or (T_Cohorts_Recoded_CIP_NOC.New_Labour_Supply_CIP_NOC)=3) 
AND ((T_Cohorts_Recoded_CIP_NOC.AGE_AT_SURVEY)>=17 
And (T_Cohorts_Recoded_CIP_NOC.AGE_AT_SURVEY)<=34) 
AND ((T_Cohorts_Recoded_CIP_NOC.Weight)>0) 
AND ((T_Cohorts_Recoded_CIP_NOC.Weight_NLS_CIP_NOC)=0 
Or (T_Cohorts_Recoded_CIP_NOC.Weight_NLS_CIP_NOC) Is Null))
GROUP BY T_Cohorts_Recoded_CIP_NOC.Survey, 
T_Cohorts_Recoded_CIP_NOC.INST_CD, 
T_Cohorts_Recoded_CIP_NOC.Age_Group_Rollup, 
T_Cohorts_Recoded_CIP_NOC.LCIP4_CRED, 
T_Cohorts_Recoded_CIP_NOC.GRAD_STATUS
HAVING (((T_Cohorts_Recoded_CIP_NOC.Age_Group_Rollup) Is Not Null) 
AND ((T_Cohorts_Recoded_CIP_NOC.GRAD_STATUS)='1'));"

## DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC ----
DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC <- 
"SELECT T_Cohorts_Recoded_CIP_NOC.Survey, 
T_Current_Region_PSSM_Rollup_Codes.Current_Region_PSSM_Code_Rollup, 
T_Cohorts_Recoded_CIP_NOC.Survey_Year, T_Cohorts_Recoded_CIP_NOC.INST_CD, 
T_Cohorts_Recoded_CIP_NOC.Age_Group_Rollup, T_Cohorts_Recoded_CIP_NOC.GRAD_STATUS, 
T_Cohorts_Recoded_CIP_NOC.LCIP4_CRED, T_Cohorts_Recoded_CIP_NOC.New_Labour_Supply_CIP_NOC, 
Count(*) AS [Count], T_Cohorts_Recoded_CIP_NOC.Weight_NLS_CIP_NOC, 
Count(*)*[Weight_NLS_CIP_NOC] AS Weighted, 
T_Cohorts_Recoded_CIP_NOC.PSSM_Credential, 
T_Cohorts_Recoded_CIP_NOC.LCP4_CD
INTO DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC
FROM T_Cohorts_Recoded_CIP_NOC
INNER JOIN (T_Current_Region_PSSM_Codes 
INNER JOIN T_Current_Region_PSSM_Rollup_Codes 
ON T_Current_Region_PSSM_Codes.Current_Region_PSSM_Code_Rollup = T_Current_Region_PSSM_Rollup_Codes.Current_Region_PSSM_Code_Rollup) 
ON T_Cohorts_Recoded_CIP_NOC.[CURRENT_REGION_PSSM_CODE] = T_Current_Region_PSSM_Codes.Current_Region_PSSM_Code
WHERE (((T_Cohorts_Recoded_CIP_NOC.Respondent)='1') 
AND ((T_Cohorts_Recoded_CIP_NOC.AGE_AT_SURVEY)>=17 
And (T_Cohorts_Recoded_CIP_NOC.AGE_AT_SURVEY)<=34) 
AND ((T_Cohorts_Recoded_CIP_NOC.Weight)>0))
GROUP BY T_Cohorts_Recoded_CIP_NOC.Survey, 
T_Current_Region_PSSM_Rollup_Codes.Current_Region_PSSM_Code_Rollup, 
T_Cohorts_Recoded_CIP_NOC.Survey_Year, T_Cohorts_Recoded_CIP_NOC.INST_CD, 
T_Cohorts_Recoded_CIP_NOC.Age_Group_Rollup, T_Cohorts_Recoded_CIP_NOC.GRAD_STATUS, 
T_Cohorts_Recoded_CIP_NOC.LCIP4_CRED, T_Cohorts_Recoded_CIP_NOC.New_Labour_Supply_CIP_NOC, 
T_Cohorts_Recoded_CIP_NOC.Weight_NLS_CIP_NOC, T_Cohorts_Recoded_CIP_NOC.PSSM_Credential, 
T_Cohorts_Recoded_CIP_NOC.LCP4_CD
HAVING (((T_Cohorts_Recoded_CIP_NOC.Age_Group_Rollup) Is Not Null) 
AND ((T_Cohorts_Recoded_CIP_NOC.GRAD_STATUS)='1') 
AND ((T_Cohorts_Recoded_CIP_NOC.New_Labour_Supply_CIP_NOC)=0 
Or (T_Cohorts_Recoded_CIP_NOC.New_Labour_Supply_CIP_NOC)=1 
Or (T_Cohorts_Recoded_CIP_NOC.New_Labour_Supply_CIP_NOC)=2 
Or (T_Cohorts_Recoded_CIP_NOC.New_Labour_Supply_CIP_NOC)=3));"

## DACSO_Q006b_Weighted_New_Labour_Supply_CIP_NOC ----
DACSO_Q006b_Weighted_New_Labour_Supply_CIP_NOC <- 
"SELECT DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC.Survey, 
DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC.Current_Region_PSSM_Code_Rollup, 
DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC.Age_Group_Rollup, 
DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC.LCIP4_CRED, 
Sum(DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC.Weighted) AS [Count], 
DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC.PSSM_Credential, 
DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC.LCP4_CD
INTO DACSO_Q006b_Weighted_New_Labour_Supply_CIP_NOC
FROM DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC
WHERE (((DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC.New_Labour_Supply_CIP_NOC)=1 
Or (DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC.New_Labour_Supply_CIP_NOC)=2 
Or (DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC.New_Labour_Supply_CIP_NOC)=3))
GROUP BY DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC.Survey, 
DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC.Current_Region_PSSM_Code_Rollup, 
DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC.Age_Group_Rollup, 
DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC.LCIP4_CRED, 
DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC.PSSM_Credential, 
DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC.LCP4_CD
HAVING (((DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC.Age_Group_Rollup) Is Not Null));"

## DACSO_Q006b_Weighted_New_Labour_Supply_0_CIP_NOC ----
DACSO_Q006b_Weighted_New_Labour_Supply_0_CIP_NOC <- 
"SELECT DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC.Survey, 
DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC.Current_Region_PSSM_Code_Rollup, 
DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC.Age_Group_Rollup, 
DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC.LCIP4_CRED, 
Sum(DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC.Weighted) AS [Count], 
DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC.PSSM_Credential, 
DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC.LCP4_CD
INTO DACSO_Q006b_Weighted_New_Labour_Supply_0_CIP_NOC
FROM DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC
WHERE (((DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC.New_Labour_Supply_CIP_NOC)=0))
GROUP BY DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC.Survey, 
DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC.Current_Region_PSSM_Code_Rollup, 
DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC.Age_Group_Rollup, 
DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC.LCIP4_CRED, 
DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC.PSSM_Credential, 
DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC.LCP4_CD;"

## DACSO_Q006b_Weighted_New_Labour_Supply_Total_CIP_NOC ----
DACSO_Q006b_Weighted_New_Labour_Supply_Total_CIP_NOC <- 
"SELECT DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC.Survey, 
DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC.PSSM_Credential, 
DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC.LCP4_CD, 
DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC.LCIP4_CRED, 
DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC.Age_Group_Rollup, 
Sum(DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC.Weighted) AS Total
INTO DACSO_Q006b_Weighted_New_Labour_Supply_Total_CIP_NOC
FROM DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC
WHERE (((DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC.New_Labour_Supply_CIP_NOC)=0 
Or (DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC.New_Labour_Supply_CIP_NOC)=1 
Or (DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC.New_Labour_Supply_CIP_NOC)=2 
Or (DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC.New_Labour_Supply_CIP_NOC)=3))
GROUP BY DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC.Survey, 
DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC.PSSM_Credential, 
DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC.LCP4_CD, 
DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC.LCIP4_CRED, 
DACSO_Q006a_Weight_New_Labour_Supply_CIP_NOC.Age_Group_Rollup;"

## DACSO_Q007a_Weighted_New_Labour_Supply_CIP_NOC ----
DACSO_Q007a_Weighted_New_Labour_Supply_CIP_NOC <- 
"SELECT DACSO_Q006b_Weighted_New_Labour_Supply_Total_CIP_NOC.Survey, 
DACSO_Q006b_Weighted_New_Labour_Supply_CIP_NOC.Current_Region_PSSM_Code_Rollup, 
DACSO_Q006b_Weighted_New_Labour_Supply_CIP_NOC.Age_Group_Rollup, 
DACSO_Q006b_Weighted_New_Labour_Supply_Total_CIP_NOC.LCIP4_CRED, 
DACSO_Q006b_Weighted_New_Labour_Supply_CIP_NOC.Count, 
DACSO_Q006b_Weighted_New_Labour_Supply_Total_CIP_NOC.Total, 
DACSO_Q006b_Weighted_New_Labour_Supply_CIP_NOC.PSSM_Credential, 
DACSO_Q006b_Weighted_New_Labour_Supply_CIP_NOC.LCP4_CD,
ISNULL(Count,0)/Total AS perc
INTO DACSO_Q007a_Weighted_New_Labour_Supply_CIP_NOC
FROM DACSO_Q006b_Weighted_New_Labour_Supply_Total_CIP_NOC 
LEFT JOIN DACSO_Q006b_Weighted_New_Labour_Supply_CIP_NOC 
ON (DACSO_Q006b_Weighted_New_Labour_Supply_Total_CIP_NOC.Age_Group_Rollup = DACSO_Q006b_Weighted_New_Labour_Supply_CIP_NOC.Age_Group_Rollup) 
AND (DACSO_Q006b_Weighted_New_Labour_Supply_Total_CIP_NOC.LCIP4_CRED = DACSO_Q006b_Weighted_New_Labour_Supply_CIP_NOC.LCIP4_CRED) 
AND (DACSO_Q006b_Weighted_New_Labour_Supply_Total_CIP_NOC.Survey = DACSO_Q006b_Weighted_New_Labour_Supply_CIP_NOC.Survey)
WHERE (((DACSO_Q006b_Weighted_New_Labour_Supply_CIP_NOC.Current_Region_PSSM_Code_Rollup) Is Not Null));"

## DACSO_Q007a_Weighted_New_Labour_Supply_0_CIP_NOC ----
DACSO_Q007a_Weighted_New_Labour_Supply_0_CIP_NOC <- 
"SELECT DACSO_Q006b_Weighted_New_Labour_Supply_Total_CIP_NOC.Survey, 
DACSO_Q006b_Weighted_New_Labour_Supply_0_CIP_NOC.PSSM_Credential, 
DACSO_Q006b_Weighted_New_Labour_Supply_0_CIP_NOC.Current_Region_PSSM_Code_Rollup, 
DACSO_Q006b_Weighted_New_Labour_Supply_0_CIP_NOC.Age_Group_Rollup, 
DACSO_Q006b_Weighted_New_Labour_Supply_0_CIP_NOC.LCP4_CD, 
DACSO_Q006b_Weighted_New_Labour_Supply_Total_CIP_NOC.LCIP4_CRED, 
DACSO_Q006b_Weighted_New_Labour_Supply_0_CIP_NOC.Count, 
DACSO_Q006b_Weighted_New_Labour_Supply_Total_CIP_NOC.Total, 
1-(ISNULL(Count,0)/Total) AS perc
INTO DACSO_Q007a_Weighted_New_Labour_Supply_0_CIP_NOC
FROM DACSO_Q006b_Weighted_New_Labour_Supply_Total_CIP_NOC 
LEFT JOIN DACSO_Q006b_Weighted_New_Labour_Supply_0_CIP_NOC 
ON (DACSO_Q006b_Weighted_New_Labour_Supply_Total_CIP_NOC.Age_Group_Rollup = DACSO_Q006b_Weighted_New_Labour_Supply_0_CIP_NOC.Age_Group_Rollup) 
AND (DACSO_Q006b_Weighted_New_Labour_Supply_Total_CIP_NOC.LCIP4_CRED = DACSO_Q006b_Weighted_New_Labour_Supply_0_CIP_NOC.LCIP4_CRED) 
AND (DACSO_Q006b_Weighted_New_Labour_Supply_Total_CIP_NOC.Survey = DACSO_Q006b_Weighted_New_Labour_Supply_0_CIP_NOC.Survey)
WHERE (((DACSO_Q006b_Weighted_New_Labour_Supply_0_CIP_NOC.Count)>0) 
AND ((1-(ISNULL(Count,0)/Total))=0));"

## DACSO_Q007b1_Append_New_Labour_Supply_CIP_NOC ----
DACSO_Q007b1_Append_New_Labour_Supply_CIP_NOC <- 
"INSERT INTO Labour_Supply_Distribution_CIP_NOC 
( Survey, Current_Region_PSSM_Code_Rollup, LCIP4_CRED, PSSM_Credential, Age_Group_Rollup, LCP4_CD, 
  [Count], Total, New_Labour_Supply_CIP_NOC )
SELECT DACSO_Q007a_Weighted_New_Labour_Supply_CIP_NOC.Survey, 
DACSO_Q007a_Weighted_New_Labour_Supply_CIP_NOC.Current_Region_PSSM_Code_Rollup, 
DACSO_Q007a_Weighted_New_Labour_Supply_CIP_NOC.LCIP4_CRED, 
DACSO_Q007a_Weighted_New_Labour_Supply_CIP_NOC.PSSM_Credential, 
DACSO_Q007a_Weighted_New_Labour_Supply_CIP_NOC.Age_Group_Rollup, 
DACSO_Q007a_Weighted_New_Labour_Supply_CIP_NOC.LCP4_CD, 
DACSO_Q007a_Weighted_New_Labour_Supply_CIP_NOC.Count, 
DACSO_Q007a_Weighted_New_Labour_Supply_CIP_NOC.Total, 
DACSO_Q007a_Weighted_New_Labour_Supply_CIP_NOC.perc
FROM DACSO_Q007a_Weighted_New_Labour_Supply_CIP_NOC;"

## DACSO_Q007b2_Append_New_Labour_Supply_0_CIP_NOC ----
DACSO_Q007b2_Append_New_Labour_Supply_0_CIP_NOC <- 
"INSERT INTO Labour_Supply_Distribution_CIP_NOC ( Survey, PSSM_Credential, Current_Region_PSSM_Code_Rollup, Age_Group_Rollup, LCP4_CD, LCIP4_CRED, [Count], Total, New_Labour_Supply_CIP_NOC )
SELECT DACSO_Q007a_Weighted_New_Labour_Supply_0_CIP_NOC.Survey,
DACSO_Q007a_Weighted_New_Labour_Supply_0_CIP_NOC.PSSM_Credential, 
DACSO_Q007a_Weighted_New_Labour_Supply_0_CIP_NOC.Current_Region_PSSM_Code_Rollup, 
DACSO_Q007a_Weighted_New_Labour_Supply_0_CIP_NOC.Age_Group_Rollup, 
DACSO_Q007a_Weighted_New_Labour_Supply_0_CIP_NOC.LCP4_CD, 
DACSO_Q007a_Weighted_New_Labour_Supply_0_CIP_NOC.LCIP4_CRED, 
DACSO_Q007a_Weighted_New_Labour_Supply_0_CIP_NOC.Count, 
DACSO_Q007a_Weighted_New_Labour_Supply_0_CIP_NOC.Total, 
DACSO_Q007a_Weighted_New_Labour_Supply_0_CIP_NOC.perc
FROM DACSO_Q007a_Weighted_New_Labour_Supply_0_CIP_NOC;"

# ******************************************************************************
# CIP-NOC Preparation Work Occupation Distribution ----

## DACSO_Q008_Z01_Base_OCC_CIP_NOC ----
DACSO_Q008_Z01_Base_OCC_CIP_NOC <- 
"SELECT T_Cohorts_Recoded_CIP_NOC.Survey, 
T_Cohorts_Recoded_CIP_NOC.INST_CD, 
T_Cohorts_Recoded_CIP_NOC.Age_Group_Rollup, 
T_Cohorts_Recoded_CIP_NOC.LCIP4_CRED, 
Count(*) AS Base, 
T_Cohorts_Recoded_CIP_NOC.STQU_ID, 
T_Cohorts_Recoded_CIP_NOC.New_Labour_Supply_CIP_NOC, 
T_Cohorts_Recoded_CIP_NOC.AGE_AT_SURVEY
INTO DACSO_Q008_Z01_Base_OCC_CIP_NOC
FROM T_Cohorts_Recoded_CIP_NOC
WHERE (((T_Cohorts_Recoded_CIP_NOC.Weight)>0))
GROUP BY T_Cohorts_Recoded_CIP_NOC.Survey, 
T_Cohorts_Recoded_CIP_NOC.INST_CD, 
T_Cohorts_Recoded_CIP_NOC.Age_Group_Rollup, 
T_Cohorts_Recoded_CIP_NOC.LCIP4_CRED, 
T_Cohorts_Recoded_CIP_NOC.STQU_ID, 
T_Cohorts_Recoded_CIP_NOC.New_Labour_Supply_CIP_NOC, 
T_Cohorts_Recoded_CIP_NOC.AGE_AT_SURVEY, 
T_Cohorts_Recoded_CIP_NOC.GRAD_STATUS
HAVING (((T_Cohorts_Recoded_CIP_NOC.New_Labour_Supply_CIP_NOC)=1 
Or (T_Cohorts_Recoded_CIP_NOC.New_Labour_Supply_CIP_NOC)=2 
Or (T_Cohorts_Recoded_CIP_NOC.New_Labour_Supply_CIP_NOC)=3) 
AND ((T_Cohorts_Recoded_CIP_NOC.AGE_AT_SURVEY)>=17 
And (T_Cohorts_Recoded_CIP_NOC.AGE_AT_SURVEY)<=34) 
AND ((T_Cohorts_Recoded_CIP_NOC.GRAD_STATUS)='1'));"

## DACSO_Q008_Z02a_Base_CIP_NOC ----
DACSO_Q008_Z02a_Base_CIP_NOC <- 
"SELECT T_Cohorts_Recoded_CIP_NOC.Survey, 
T_Current_Region_PSSM_Rollup_Codes.Current_Region_PSSM_Code_Rollup, 
T_Cohorts_Recoded_CIP_NOC.Survey_Year, 
T_Cohorts_Recoded_CIP_NOC.Age_Group_Rollup, 
T_Cohorts_Recoded_CIP_NOC.INST_CD,
T_Cohorts_Recoded_CIP_NOC.GRAD_STATUS, 
T_Cohorts_Recoded_CIP_NOC.LCIP4_CRED, 
Count(*) AS [Count], 
T_Cohorts_Recoded_CIP_NOC.Weight_NLS_CIP_NOC, 
Count(*)*[Weight_NLS_CIP_NOC] AS Base
INTO DACSO_Q008_Z02a_Base_CIP_NOC
FROM T_Cohorts_Recoded_CIP_NOC 
INNER JOIN (T_Current_Region_PSSM_Codes INNER JOIN T_Current_Region_PSSM_Rollup_Codes 
ON T_Current_Region_PSSM_Codes.Current_Region_PSSM_Code_Rollup = T_Current_Region_PSSM_Rollup_Codes.Current_Region_PSSM_Code_Rollup) 
ON T_Cohorts_Recoded_CIP_NOC.[CURRENT_REGION_PSSM_CODE] = T_Current_Region_PSSM_Codes.Current_Region_PSSM_Code
WHERE (((T_Cohorts_Recoded_CIP_NOC.New_Labour_Supply_CIP_NOC)=1 
Or (T_Cohorts_Recoded_CIP_NOC.New_Labour_Supply_CIP_NOC)=2 
Or (T_Cohorts_Recoded_CIP_NOC.New_Labour_Supply_CIP_NOC)=3) 
AND ((T_Cohorts_Recoded_CIP_NOC.Respondent)='1') 
AND ((T_Cohorts_Recoded_CIP_NOC.AGE_AT_SURVEY)>=17 
And (T_Cohorts_Recoded_CIP_NOC.AGE_AT_SURVEY)<=34) 
AND ((T_Cohorts_Recoded_CIP_NOC.Weight)>0))
GROUP BY T_Cohorts_Recoded_CIP_NOC.Survey, 
T_Current_Region_PSSM_Rollup_Codes.Current_Region_PSSM_Code_Rollup, 
T_Cohorts_Recoded_CIP_NOC.Survey_Year, 
T_Cohorts_Recoded_CIP_NOC.Age_Group_Rollup, 
T_Cohorts_Recoded_CIP_NOC.INST_CD, 
T_Cohorts_Recoded_CIP_NOC.GRAD_STATUS, 
T_Cohorts_Recoded_CIP_NOC.LCIP4_CRED, 
T_Cohorts_Recoded_CIP_NOC.Weight_NLS_CIP_NOC
HAVING (((T_Cohorts_Recoded_CIP_NOC.GRAD_STATUS)='1'));"

## DACSO_Q008_Z02b_Respondents_CIP_NOC ----
DACSO_Q008_Z02b_Respondents_CIP_NOC <- 
"SELECT T_Cohorts_Recoded_CIP_NOC.Survey, 
T_Current_Region_PSSM_Rollup_Codes.Current_Region_PSSM_Code_Rollup, 
T_Cohorts_Recoded_CIP_NOC.Survey_Year, 
T_Cohorts_Recoded_CIP_NOC.INST_CD, 
T_Cohorts_Recoded_CIP_NOC.Age_Group_Rollup, 
T_Cohorts_Recoded_CIP_NOC.GRAD_STATUS, 
T_Cohorts_Recoded_CIP_NOC.LCIP4_CRED, 
Sum(IIf([Respondent]='1',1,0)) AS Respondents
INTO DACSO_Q008_Z02b_Respondents_CIP_NOC
FROM T_Cohorts_Recoded_CIP_NOC 
INNER JOIN (T_Current_Region_PSSM_Codes 
INNER JOIN T_Current_Region_PSSM_Rollup_Codes 
ON T_Current_Region_PSSM_Codes.Current_Region_PSSM_Code_Rollup = T_Current_Region_PSSM_Rollup_Codes.Current_Region_PSSM_Code_Rollup) 
ON T_Cohorts_Recoded_CIP_NOC.[CURRENT_REGION_PSSM_CODE] = T_Current_Region_PSSM_Codes.Current_Region_PSSM_Code
WHERE (((T_Cohorts_Recoded_CIP_NOC.NOC_CD) Is Not Null) 
AND ((T_Cohorts_Recoded_CIP_NOC.New_Labour_Supply_CIP_NOC)=1 
Or (T_Cohorts_Recoded_CIP_NOC.New_Labour_Supply_CIP_NOC)=3) 
AND ((T_Cohorts_Recoded_CIP_NOC.Weight)>0) 
AND ((T_Cohorts_Recoded_CIP_NOC.AGE_AT_SURVEY)>=17 
And (T_Cohorts_Recoded_CIP_NOC.AGE_AT_SURVEY)<=34))
GROUP BY T_Cohorts_Recoded_CIP_NOC.Survey, 
T_Current_Region_PSSM_Rollup_Codes.Current_Region_PSSM_Code_Rollup, 
T_Cohorts_Recoded_CIP_NOC.Survey_Year, 
T_Cohorts_Recoded_CIP_NOC.INST_CD, 
T_Cohorts_Recoded_CIP_NOC.Age_Group_Rollup, 
T_Cohorts_Recoded_CIP_NOC.GRAD_STATUS, 
T_Cohorts_Recoded_CIP_NOC.LCIP4_CRED
HAVING (((T_Cohorts_Recoded_CIP_NOC.GRAD_STATUS)='1'));"

## DACSO_Q008_Z02c_Weight_CIP_NOC ----
DACSO_Q008_Z02c_Weight_CIP_NOC <- 
"SELECT DACSO_Q008_Z02a_Base_CIP_NOC.Survey, 
DACSO_Q008_Z02a_Base_CIP_NOC.Current_Region_PSSM_Code_Rollup, 
DACSO_Q008_Z02a_Base_CIP_NOC.Survey_Year, 
DACSO_Q008_Z02a_Base_CIP_NOC.INST_CD, 
DACSO_Q008_Z02a_Base_CIP_NOC.Age_Group_Rollup, 
DACSO_Q008_Z02a_Base_CIP_NOC.GRAD_STATUS, 
DACSO_Q008_Z02a_Base_CIP_NOC.LCIP4_CRED, 
DACSO_Q008_Z02a_Base_CIP_NOC.Weight_NLS_CIP_NOC, 
DACSO_Q008_Z02a_Base_CIP_NOC.Base, 
DACSO_Q008_Z02b_Respondents_CIP_NOC.Respondents, 
IIf(ISNULL([DACSO_Q008_Z02b_Respondents_CIP_NOC].[Respondents],0)=0,1,[DACSO_Q008_Z02a_Base_CIP_NOC].[Base]/[DACSO_Q008_Z02b_Respondents_CIP_NOC].[Respondents]) AS Weight_NLS_CIP_NOC_Base, 
ISNULL([DACSO_Q008_Z02b_Respondents_CIP_NOC].[Respondents],0)*IIf(ISNULL([DACSO_Q008_Z02b_Respondents_CIP_NOC].[Respondents],0)=0,1,[DACSO_Q008_Z02a_Base_CIP_NOC].[Base]/[DACSO_Q008_Z02b_Respondents_CIP_NOC].[Respondents]) AS Weighted
INTO DACSO_Q008_Z02c_Weight_CIP_NOC
FROM DACSO_Q008_Z02a_Base_CIP_NOC 
LEFT JOIN DACSO_Q008_Z02b_Respondents_CIP_NOC 
ON (DACSO_Q008_Z02a_Base_CIP_NOC.Age_Group_Rollup = DACSO_Q008_Z02b_Respondents_CIP_NOC.Age_Group_Rollup) 
AND (DACSO_Q008_Z02a_Base_CIP_NOC.LCIP4_CRED = DACSO_Q008_Z02b_Respondents_CIP_NOC.LCIP4_CRED) 
AND (DACSO_Q008_Z02a_Base_CIP_NOC.GRAD_STATUS = DACSO_Q008_Z02b_Respondents_CIP_NOC.GRAD_STATUS) 
AND (DACSO_Q008_Z02a_Base_CIP_NOC.INST_CD = DACSO_Q008_Z02b_Respondents_CIP_NOC.[INST_CD]) 
AND (DACSO_Q008_Z02a_Base_CIP_NOC.Survey_Year = DACSO_Q008_Z02b_Respondents_CIP_NOC.Survey_Year) 
AND (DACSO_Q008_Z02a_Base_CIP_NOC.Current_Region_PSSM_Code_Rollup = DACSO_Q008_Z02b_Respondents_CIP_NOC.Current_Region_PSSM_Code_Rollup)
GROUP BY DACSO_Q008_Z02a_Base_CIP_NOC.Survey, 
DACSO_Q008_Z02a_Base_CIP_NOC.Current_Region_PSSM_Code_Rollup, 
DACSO_Q008_Z02a_Base_CIP_NOC.Survey_Year, 
DACSO_Q008_Z02a_Base_CIP_NOC.INST_CD, 
DACSO_Q008_Z02a_Base_CIP_NOC.Age_Group_Rollup, 
DACSO_Q008_Z02a_Base_CIP_NOC.GRAD_STATUS, 
DACSO_Q008_Z02a_Base_CIP_NOC.LCIP4_CRED, 
DACSO_Q008_Z02a_Base_CIP_NOC.Weight_NLS_CIP_NOC, 
DACSO_Q008_Z02a_Base_CIP_NOC.Base, 
DACSO_Q008_Z02b_Respondents_CIP_NOC.Respondents;"

## DACSO_Q008_Z03_Weight_Total_CIP_NOC ----
DACSO_Q008_Z03_Weight_Total_CIP_NOC <- 
"SELECT DACSO_Q008_Z02c_Weight_CIP_NOC.Survey, 
DACSO_Q008_Z02c_Weight_CIP_NOC.Current_Region_PSSM_Code_Rollup, 
DACSO_Q008_Z02c_Weight_CIP_NOC.Age_Group_Rollup, 
DACSO_Q008_Z02c_Weight_CIP_NOC.INST_CD, 
DACSO_Q008_Z02c_Weight_CIP_NOC.GRAD_STATUS, 
DACSO_Q008_Z02c_Weight_CIP_NOC.LCIP4_CRED, 
Sum(DACSO_Q008_Z02c_Weight_CIP_NOC.Base) AS Base, 
Sum(DACSO_Q008_Z02c_Weight_CIP_NOC.Weighted) AS Weighted
INTO DACSO_Q008_Z03_Weight_Total_CIP_NOC
FROM DACSO_Q008_Z02c_Weight_CIP_NOC
GROUP BY DACSO_Q008_Z02c_Weight_CIP_NOC.Survey, 
DACSO_Q008_Z02c_Weight_CIP_NOC.Current_Region_PSSM_Code_Rollup, 
DACSO_Q008_Z02c_Weight_CIP_NOC.Age_Group_Rollup, 
DACSO_Q008_Z02c_Weight_CIP_NOC.INST_CD, 
DACSO_Q008_Z02c_Weight_CIP_NOC.GRAD_STATUS, 
DACSO_Q008_Z02c_Weight_CIP_NOC.LCIP4_CRED;"

## DACSO_Q008_Z04_Weight_Adj_Fac_CIP_NOC ----
DACSO_Q008_Z04_Weight_Adj_Fac_CIP_NOC <- 
"SELECT DACSO_Q008_Z03_Weight_Total_CIP_NOC.Survey, 
DACSO_Q008_Z03_Weight_Total_CIP_NOC.Current_Region_PSSM_Code_Rollup, 
DACSO_Q008_Z03_Weight_Total_CIP_NOC.Age_Group_Rollup, 
DACSO_Q008_Z03_Weight_Total_CIP_NOC.INST_CD, 
DACSO_Q008_Z03_Weight_Total_CIP_NOC.GRAD_STATUS, 
DACSO_Q008_Z03_Weight_Total_CIP_NOC.LCIP4_CRED, 
DACSO_Q008_Z03_Weight_Total_CIP_NOC.Base, 
DACSO_Q008_Z03_Weight_Total_CIP_NOC.Weighted, 
IIf([Weighted]=0,0,[Base]/[Weighted]) AS Weight_Adj_Fac
INTO DACSO_Q008_Z04_Weight_Adj_Fac_CIP_NOC
FROM DACSO_Q008_Z03_Weight_Total_CIP_NOC;"

## DACSO_Q008_Z05_Weight_OCC_CIP_NOC ----
DACSO_Q008_Z05_Weight_OCC_CIP_NOC <- 
"SELECT DACSO_Q008_Z02c_Weight_CIP_NOC.Survey, 
DACSO_Q008_Z02c_Weight_CIP_NOC.Current_Region_PSSM_Code_Rollup, 
T_Current_Region_PSSM_Codes.Current_Region_PSSM_Code, 
DACSO_Q008_Z02c_Weight_CIP_NOC.Survey_Year, 
DACSO_Q008_Z02c_Weight_CIP_NOC.INST_CD,
DACSO_Q008_Z02c_Weight_CIP_NOC.Age_Group_Rollup, 
DACSO_Q008_Z02c_Weight_CIP_NOC.GRAD_STATUS, 
DACSO_Q008_Z02c_Weight_CIP_NOC.LCIP4_CRED, 
DACSO_Q008_Z02c_Weight_CIP_NOC.Base, 
DACSO_Q008_Z02c_Weight_CIP_NOC.Respondents, 
DACSO_Q008_Z02c_Weight_CIP_NOC.Weight_NLS_CIP_NOC_Base, 
DACSO_Q008_Z04_Weight_Adj_Fac_CIP_NOC.Weighted, 
DACSO_Q008_Z04_Weight_Adj_Fac_CIP_NOC.Weight_Adj_Fac, 
[Weight_NLS_CIP_NOC_Base]*[Weight_Adj_Fac] AS Weight_OCC_CIP_NOC 
INTO tmp_tbl_Weights_OCC_CIP_NOC
FROM ((DACSO_Q008_Z02c_Weight_CIP_NOC 
INNER JOIN DACSO_Q008_Z04_Weight_Adj_Fac_CIP_NOC
ON (DACSO_Q008_Z02c_Weight_CIP_NOC.Current_Region_PSSM_Code_Rollup = DACSO_Q008_Z04_Weight_Adj_Fac_CIP_NOC.Current_Region_PSSM_Code_Rollup) 
AND (DACSO_Q008_Z02c_Weight_CIP_NOC.LCIP4_CRED = DACSO_Q008_Z04_Weight_Adj_Fac_CIP_NOC.LCIP4_CRED) 
AND (DACSO_Q008_Z02c_Weight_CIP_NOC.[GRAD_STATUS] = DACSO_Q008_Z04_Weight_Adj_Fac_CIP_NOC.[GRAD_STATUS]) 
AND (DACSO_Q008_Z02c_Weight_CIP_NOC.Survey = DACSO_Q008_Z04_Weight_Adj_Fac_CIP_NOC.Survey) 
AND (DACSO_Q008_Z02c_Weight_CIP_NOC.[INST_CD] = DACSO_Q008_Z04_Weight_Adj_Fac_CIP_NOC.[INST_CD]) 
AND (DACSO_Q008_Z02c_Weight_CIP_NOC.Age_Group_Rollup = DACSO_Q008_Z04_Weight_Adj_Fac_CIP_NOC.Age_Group_Rollup)) 
INNER JOIN T_Current_Region_PSSM_Rollup_Codes 
ON DACSO_Q008_Z02c_Weight_CIP_NOC.Current_Region_PSSM_Code_Rollup = T_Current_Region_PSSM_Rollup_Codes.Current_Region_PSSM_Code_Rollup) 
INNER JOIN T_Current_Region_PSSM_Codes 
ON T_Current_Region_PSSM_Rollup_Codes.Current_Region_PSSM_Code_Rollup = T_Current_Region_PSSM_Codes.Current_Region_PSSM_Code_Rollup;"

## DACSO_Q008_Z07_Weight_OCC_Null_CIP_NOC ----
DACSO_Q008_Z07_Weight_OCC_Null_CIP_NOC <- 
  "UPDATE T_Cohorts_Recoded_CIP_NOC SET T_Cohorts_Recoded_CIP_NOC.Weight_OCC_CIP_NOC = Null;"

## DACSO_Q008_Z08_Weight_OCC_Update_CIP_NOC ----
# updated from a distinctrow query
DACSO_Q008_Z08_Weight_OCC_Update_CIP_NOC <- 
  "UPDATE t_cohorts_recoded_CIP_NOC
SET    t_cohorts_recoded_CIP_NOC.weight_occ_CIP_NOC = tmp_tbl_Weights_OCC_CIP_NOC.weight_occ_CIP_NOC
FROM   T_Current_Region_PSSM_Codes
INNER JOIN ((tmp_tbl_Weights_OCC_CIP_NOC
INNER JOIN t_cohorts_recoded_CIP_NOC
ON   tmp_tbl_Weights_OCC_CIP_NOC.lcip4_cred = t_cohorts_recoded_CIP_NOC.lcip4_cred
AND tmp_tbl_Weights_OCC_CIP_NOC.grad_status =  t_cohorts_recoded_CIP_NOC.grad_status
AND tmp_tbl_Weights_OCC_CIP_NOC.age_group_rollup =  t_cohorts_recoded_CIP_NOC.age_group_rollup
AND tmp_tbl_Weights_OCC_CIP_NOC.inst_cd =  t_cohorts_recoded_CIP_NOC.inst_cd
AND tmp_tbl_Weights_OCC_CIP_NOC.survey_year = t_cohorts_recoded_CIP_NOC.survey_year
AND tmp_tbl_Weights_OCC_CIP_NOC.Current_Region_PSSM_Code = t_cohorts_recoded_CIP_NOC.Current_Region_PSSM_Code)
INNER JOIN DACSO_Q008_Z01_Base_OCC_CIP_NOC
ON t_cohorts_recoded_CIP_NOC.stqu_id = DACSO_Q008_Z01_Base_OCC_CIP_NOC.stqu_id)
ON T_Current_Region_PSSM_Codes.Current_Region_PSSM_Code = T_Cohorts_Recoded_CIP_NOC.[CURRENT_REGION_PSSM_CODE];"

## DACSO_Q008_Z09_Check_Weights_CIP_NOC ----
DACSO_Q008_Z09_Check_Weights_CIP_NOC <- 
"SELECT T_Cohorts_Recoded_CIP_NOC.Survey_Year, 
T_Cohorts_Recoded_CIP_NOC.Survey, 
T_Cohorts_Recoded_CIP_NOC.INST_CD, 
T_Cohorts_Recoded_CIP_NOC.Age_Group_Rollup, 
T_Cohorts_Recoded_CIP_NOC.LCIP4_CRED, 
Sum(IIf([Respondent]='1',1,0)) AS Respondents, 
T_Cohorts_Recoded_CIP_NOC.Weight_OCC_CIP_NOC, 
Sum(IIf([Respondent]='1',1,0))*[Weight_OCC_CIP_NOC] AS Weighted, 
Sum(DACSO_Q008_Z01_Base_OCC_CIP_NOC.Base) AS Base, T_Cohorts_Recoded_CIP_NOC.Respondent
FROM T_Cohorts_Recoded_CIP_NOC 
INNER JOIN DACSO_Q008_Z01_Base_OCC_CIP_NOC 
ON T_Cohorts_Recoded_CIP_NOC.[STQU_ID] = DACSO_Q008_Z01_Base_OCC_CIP_NOC.[STQU_ID]
WHERE (((DACSO_Q008_Z01_Base_OCC_CIP_NOC.New_Labour_Supply_CIP_NOC)=1 
Or (DACSO_Q008_Z01_Base_OCC_CIP_NOC.New_Labour_Supply_CIP_NOC)=3) 
AND ((T_Cohorts_Recoded_CIP_NOC.NOC_CD) Is Not Null))
GROUP BY T_Cohorts_Recoded_CIP_NOC.Survey_Year, T_Cohorts_Recoded_CIP_NOC.Survey, 
T_Cohorts_Recoded_CIP_NOC.INST_CD, T_Cohorts_Recoded_CIP_NOC.Age_Group_Rollup, 
T_Cohorts_Recoded_CIP_NOC.LCIP4_CRED, T_Cohorts_Recoded_CIP_NOC.Weight_OCC_CIP_NOC, 
T_Cohorts_Recoded_CIP_NOC.Respondent
HAVING (((T_Cohorts_Recoded_CIP_NOC.Respondent)='1'))
ORDER BY T_Cohorts_Recoded_CIP_NOC.Survey_Year, T_Cohorts_Recoded_CIP_NOC.Weight_OCC_CIP_NOC;"

## DACSO_Q009_Weight_Occs_CIP_NOC ----
DACSO_Q009_Weight_Occs_CIP_NOC <- 
"SELECT T_Cohorts_Recoded_CIP_NOC.Survey, 
T_Cohorts_Recoded_CIP_NOC.PSSM_Credential, 
T_Current_Region_PSSM_Rollup_Codes.Current_Region_PSSM_Code_Rollup, 
T_Cohorts_Recoded_CIP_NOC.Survey_Year, 
T_Cohorts_Recoded_CIP_NOC.INST_CD, 
T_Cohorts_Recoded_CIP_NOC.Age_Group_Rollup, 
T_Cohorts_Recoded_CIP_NOC.GRAD_STATUS, 
T_Cohorts_Recoded_CIP_NOC.LCP4_CD, 
T_Cohorts_Recoded_CIP_NOC.LCIP4_CRED, 
IIf([T_Cohorts_Recoded_CIP_NOC].[NOC_CD]='XXXXX','99999',[T_Cohorts_Recoded_CIP_NOC].[NOC_CD]) AS NOC_CD, 
Count(*) AS [Count], 
T_Cohorts_Recoded_CIP_NOC.Weight_OCC_CIP_NOC, 
Count(*)*[Weight_OCC_CIP_NOC] AS Weighted
INTO DACSO_Q009_Weight_Occs_CIP_NOC
FROM T_Cohorts_Recoded_CIP_NOC 
INNER JOIN (T_Current_Region_PSSM_Codes 
INNER JOIN T_Current_Region_PSSM_Rollup_Codes 
ON T_Current_Region_PSSM_Codes.Current_Region_PSSM_Code_Rollup = T_Current_Region_PSSM_Rollup_Codes.Current_Region_PSSM_Code_Rollup) 
ON T_Cohorts_Recoded_CIP_NOC.[CURRENT_REGION_PSSM_CODE] = T_Current_Region_PSSM_Codes.Current_Region_PSSM_Code
WHERE (((T_Cohorts_Recoded_CIP_NOC.New_Labour_Supply_CIP_NOC)=1 
Or (T_Cohorts_Recoded_CIP_NOC.New_Labour_Supply_CIP_NOC)=3) 
AND ((T_Cohorts_Recoded_CIP_NOC.Weight)>0) 
AND ((T_Cohorts_Recoded_CIP_NOC.AGE_AT_SURVEY)>=17 
And (T_Cohorts_Recoded_CIP_NOC.AGE_AT_SURVEY)<=34))
GROUP BY T_Cohorts_Recoded_CIP_NOC.Survey, 
T_Cohorts_Recoded_CIP_NOC.PSSM_Credential, 
T_Current_Region_PSSM_Rollup_Codes.Current_Region_PSSM_Code_Rollup, 
T_Cohorts_Recoded_CIP_NOC.Survey_Year, 
T_Cohorts_Recoded_CIP_NOC.INST_CD, 
T_Cohorts_Recoded_CIP_NOC.Age_Group_Rollup, 
T_Cohorts_Recoded_CIP_NOC.GRAD_STATUS, 
T_Cohorts_Recoded_CIP_NOC.LCP4_CD, 
T_Cohorts_Recoded_CIP_NOC.LCIP4_CRED, 
IIf([T_Cohorts_Recoded_CIP_NOC].[NOC_CD]='XXXXX','99999',[T_Cohorts_Recoded_CIP_NOC].[NOC_CD]), 
T_Cohorts_Recoded_CIP_NOC.Weight_OCC_CIP_NOC
HAVING (((T_Cohorts_Recoded_CIP_NOC.GRAD_STATUS)='1') 
AND ((IIf([T_Cohorts_Recoded_CIP_NOC].[NOC_CD]='XXXXX','99999',[T_Cohorts_Recoded_CIP_NOC].[NOC_CD])) Is Not Null))
ORDER BY T_Cohorts_Recoded_CIP_NOC.GRAD_STATUS, 
T_Cohorts_Recoded_CIP_NOC.LCIP4_CRED, 
IIf([T_Cohorts_Recoded_CIP_NOC].[NOC_CD]='XXXXX','99999',[T_Cohorts_Recoded_CIP_NOC].[NOC_CD]);"

## DACSO_Q009b_Weighted_Occs_CIP_NOC ----
DACSO_Q009b_Weighted_Occs_CIP_NOC <- 
"SELECT DACSO_Q009_Weight_Occs_CIP_NOC.Survey, 
DACSO_Q009_Weight_Occs_CIP_NOC.PSSM_Credential, 
DACSO_Q009_Weight_Occs_CIP_NOC.Current_Region_PSSM_Code_Rollup, 
DACSO_Q009_Weight_Occs_CIP_NOC.Age_Group_Rollup, 
DACSO_Q009_Weight_Occs_CIP_NOC.LCP4_CD, 
DACSO_Q009_Weight_Occs_CIP_NOC.LCIP4_CRED, 
DACSO_Q009_Weight_Occs_CIP_NOC.NOC_CD, 
Sum(DACSO_Q009_Weight_Occs_CIP_NOC.Weighted) AS [Count], 
Sum(DACSO_Q009_Weight_Occs_CIP_NOC.Count) AS Unweighted_Count
INTO DACSO_Q009b_Weighted_Occs_CIP_NOC
FROM DACSO_Q009_Weight_Occs_CIP_NOC
GROUP BY DACSO_Q009_Weight_Occs_CIP_NOC.Survey, 
DACSO_Q009_Weight_Occs_CIP_NOC.PSSM_Credential, 
DACSO_Q009_Weight_Occs_CIP_NOC.Current_Region_PSSM_Code_Rollup, 
DACSO_Q009_Weight_Occs_CIP_NOC.Age_Group_Rollup, 
DACSO_Q009_Weight_Occs_CIP_NOC.LCP4_CD, 
DACSO_Q009_Weight_Occs_CIP_NOC.LCIP4_CRED, 
DACSO_Q009_Weight_Occs_CIP_NOC.NOC_CD
ORDER BY DACSO_Q009_Weight_Occs_CIP_NOC.LCIP4_CRED, 
DACSO_Q009_Weight_Occs_CIP_NOC.NOC_CD;"

## DACSO_Q009b_Weighted_Occs_Total_CIP_NOC ----
DACSO_Q009b_Weighted_Occs_Total_CIP_NOC <- 
"SELECT DACSO_Q009_Weight_Occs_CIP_NOC.Survey, 
DACSO_Q009_Weight_Occs_CIP_NOC.PSSM_Credential, 
DACSO_Q009_Weight_Occs_CIP_NOC.Current_Region_PSSM_Code_Rollup, 
DACSO_Q009_Weight_Occs_CIP_NOC.Age_Group_Rollup, 
DACSO_Q009_Weight_Occs_CIP_NOC.LCP4_CD, 
DACSO_Q009_Weight_Occs_CIP_NOC.LCIP4_CRED, 
Sum(DACSO_Q009_Weight_Occs_CIP_NOC.Weighted) AS Total, 
Sum(DACSO_Q009_Weight_Occs_CIP_NOC.Count) AS Unweighted_Total
INTO DACSO_Q009b_Weighted_Occs_Total_CIP_NOC
FROM DACSO_Q009_Weight_Occs_CIP_NOC
GROUP BY DACSO_Q009_Weight_Occs_CIP_NOC.Survey, 
DACSO_Q009_Weight_Occs_CIP_NOC.PSSM_Credential, 
DACSO_Q009_Weight_Occs_CIP_NOC.Current_Region_PSSM_Code_Rollup, 
DACSO_Q009_Weight_Occs_CIP_NOC.Age_Group_Rollup, 
DACSO_Q009_Weight_Occs_CIP_NOC.LCP4_CD, 
DACSO_Q009_Weight_Occs_CIP_NOC.LCIP4_CRED
ORDER BY DACSO_Q009_Weight_Occs_CIP_NOC.LCIP4_CRED;"

## DACSO_Q010_Weighted_Occs_Dist_CIP_NOC ----
DACSO_Q010_Weighted_Occs_Dist_CIP_NOC <- 
"SELECT DACSO_Q009b_Weighted_Occs_Total_CIP_NOC.Survey, 
DACSO_Q009b_Weighted_Occs_Total_CIP_NOC.PSSM_Credential, 
DACSO_Q009b_Weighted_Occs_Total_CIP_NOC.Current_Region_PSSM_Code_Rollup, 
DACSO_Q009b_Weighted_Occs_Total_CIP_NOC.Age_Group_Rollup, 
DACSO_Q009b_Weighted_Occs_Total_CIP_NOC.LCP4_CD, 
DACSO_Q009b_Weighted_Occs_Total_CIP_NOC.LCIP4_CRED, 
DACSO_Q009b_Weighted_Occs_CIP_NOC.NOC_CD, 
DACSO_Q009b_Weighted_Occs_CIP_NOC.Unweighted_Count, 
DACSO_Q009b_Weighted_Occs_Total_CIP_NOC.Unweighted_Total, 
CAST(DACSO_Q009b_Weighted_Occs_CIP_NOC.Unweighted_Count AS FLOAT) / 
             CAST(DACSO_Q009b_Weighted_Occs_Total_CIP_NOC.Unweighted_Total AS FLOAT) AS Unweighted_Percent, 
DACSO_Q009b_Weighted_Occs_CIP_NOC.Count, 
DACSO_Q009b_Weighted_Occs_Total_CIP_NOC.Total, 
Count/Total AS perc_dist
INTO DACSO_Q010_Weighted_Occs_Dist_CIP_NOC
FROM DACSO_Q009b_Weighted_Occs_Total_CIP_NOC 
LEFT JOIN DACSO_Q009b_Weighted_Occs_CIP_NOC 
ON (DACSO_Q009b_Weighted_Occs_Total_CIP_NOC.Age_Group_Rollup = DACSO_Q009b_Weighted_Occs_CIP_NOC.Age_Group_Rollup) 
AND (DACSO_Q009b_Weighted_Occs_Total_CIP_NOC.Current_Region_PSSM_Code_Rollup = DACSO_Q009b_Weighted_Occs_CIP_NOC.Current_Region_PSSM_Code_Rollup) 
AND (DACSO_Q009b_Weighted_Occs_Total_CIP_NOC.LCIP4_CRED = DACSO_Q009b_Weighted_Occs_CIP_NOC.LCIP4_CRED) 
AND (DACSO_Q009b_Weighted_Occs_Total_CIP_NOC.Survey = DACSO_Q009b_Weighted_Occs_CIP_NOC.Survey);"

## DACSO_Q010b_Append_Occupational_Distribution_CIP_NOC ----
DACSO_Q010b_Append_Occupational_Distribution_CIP_NOC <- 
"INSERT INTO Occupation_Distributions_CIP_NOC 
  ( Survey, PSSM_Credential, Current_Region_PSSM_Code_Rollup, Age_Group_Rollup, LCP4_CD, LCIP4_CRED, NOC, Unweighted_Count, 
  Unweighted_Total, Unweighted_Percent, [Count], Total, [Percent] )
SELECT DACSO_Q010_Weighted_Occs_Dist_CIP_NOC.Survey, 
DACSO_Q010_Weighted_Occs_Dist_CIP_NOC.PSSM_Credential, 
DACSO_Q010_Weighted_Occs_Dist_CIP_NOC.Current_Region_PSSM_Code_Rollup, 
DACSO_Q010_Weighted_Occs_Dist_CIP_NOC.Age_Group_Rollup, 
DACSO_Q010_Weighted_Occs_Dist_CIP_NOC.LCP4_CD, 
DACSO_Q010_Weighted_Occs_Dist_CIP_NOC.LCIP4_CRED, 
DACSO_Q010_Weighted_Occs_Dist_CIP_NOC.NOC_CD, 
DACSO_Q010_Weighted_Occs_Dist_CIP_NOC.Unweighted_Count, 
DACSO_Q010_Weighted_Occs_Dist_CIP_NOC.Unweighted_Total, 
DACSO_Q010_Weighted_Occs_Dist_CIP_NOC.Unweighted_Percent, 
DACSO_Q010_Weighted_Occs_Dist_CIP_NOC.Count, 
DACSO_Q010_Weighted_Occs_Dist_CIP_NOC.Total, 
DACSO_Q010_Weighted_Occs_Dist_CIP_NOC.perc_dist
FROM DACSO_Q010_Weighted_Occs_Dist_CIP_NOC;"

# ******************************************************************************
# Additional CIP-NOC work ----

## qry_update_Labour_Supply_Distribution_LCIP4_CRED_Cleaned_CIP_NOC ----
qry_update_Labour_Supply_Distribution_LCIP4_CRED_Cleaned_CIP_NOC <- 
"UPDATE Labour_Supply_Distribution_CIP_NOC 
SET LCIP4_CRED_Cleaned = 
CASE WHEN Survey = 'DACSO' AND LCIP4_CRED Like '1 - %' 
THEN SUBSTRING(LCIP4_CRED,5,LEN(LCIP4_CRED))
ELSE LCIP4_CRED
END;"

## qry_update_Occupation_Distributions_Exclude_Flag_CIP_NOC ----
qry_update_Occupation_Distributions_Exclude_Flag_CIP_NOC <- 
"UPDATE Occupation_Distributions_CIP_NOC 
SET Occupation_Distributions_CIP_NOC.Exclude_Flag = '1'
WHERE (((Occupation_Distributions_CIP_NOC.Current_Region_PSSM_Code_Rollup)=9910 
Or (Occupation_Distributions_CIP_NOC.Current_Region_PSSM_Code_Rollup)=9911 
Or (Occupation_Distributions_CIP_NOC.Current_Region_PSSM_Code_Rollup)=9999 
Or (Occupation_Distributions_CIP_NOC.Current_Region_PSSM_Code_Rollup)=9912));"

## qry_update_Occupation_Distributions_LCIP4_Cred_Cleaned_CIP_NOC ----
qry_update_Occupation_Distributions_LCIP4_Cred_Cleaned_CIP_NOC <- 
"UPDATE Occupation_Distributions_CIP_NOC 
SET LCIP4_CRED_Cleaned = 
CASE WHEN Survey = 'DACSO' AND LCIP4_CRED Like '1 - %' 
THEN SUBSTRING(LCIP4_CRED,5,LEN(LCIP4_CRED))
ELSE LCIP4_CRED
END;"

# Complete Occupation Distributions table ----
## qry_SurveyCIP4CredAgeRegionCombos_in_OccDistributions_CIP_NOC
qry_SurveyCIP4CredAgeRegionCombos_in_OccDistributions_CIP_NOC <- 
"SELECT Occupation_Distributions_CIP_NOC.Survey, 
Occupation_Distributions_CIP_NOC.LCP4_CD, 
Occupation_Distributions_CIP_NOC.PSSM_Credential, 
Occupation_Distributions_CIP_NOC.Current_Region_PSSM_Code_Rollup, 
Occupation_Distributions_CIP_NOC.Age_Group_Rollup
INTO qry_SurveyCIP4CredAgeRegionCombos_in_OccDistributions_CIP_NOC
FROM Occupation_Distributions_CIP_NOC
GROUP BY Occupation_Distributions_CIP_NOC.Survey, 
Occupation_Distributions_CIP_NOC.LCP4_CD, 
Occupation_Distributions_CIP_NOC.PSSM_Credential, 
Occupation_Distributions_CIP_NOC.Current_Region_PSSM_Code_Rollup, 
Occupation_Distributions_CIP_NOC.Age_Group_Rollup;"

## qry_LabourSupplyDistribution_Missing_from_OccDist_CIP_NOC ----
qry_LabourSupplyDistribution_Missing_from_OccDist_CIP_NOC <- 
"SELECT Labour_Supply_Distribution_CIP_NOC.Survey, 
Labour_Supply_Distribution_CIP_NOC.LCIP4_CRED, 
Labour_Supply_Distribution_CIP_NOC.Current_Region_PSSM_Code_Rollup, 
Labour_Supply_Distribution_CIP_NOC.Age_Group_Rollup, 
Labour_Supply_Distribution_CIP_NOC.Count, 
Labour_Supply_Distribution_CIP_NOC.Total, 
Labour_Supply_Distribution_CIP_NOC.New_Labour_Supply_CIP_NOC, 
Labour_Supply_Distribution_CIP_NOC.LCIP4_CRED_Cleaned, 
Labour_Supply_Distribution_CIP_NOC.LCP4_CD, 
Labour_Supply_Distribution_CIP_NOC.PSSM_Credential 
INTO tmp_Append_LabourSupplyDistribution_Missing_from_OccDist_CIP_NOC
FROM Labour_Supply_Distribution_CIP_NOC 
LEFT JOIN qry_SurveyCIP4CredAgeRegionCombos_in_OccDistributions_CIP_NOC 
ON (Labour_Supply_Distribution_CIP_NOC.Survey = qry_SurveyCIP4CredAgeRegionCombos_in_OccDistributions_CIP_NOC.Survey) 
AND (Labour_Supply_Distribution_CIP_NOC.LCP4_CD = qry_SurveyCIP4CredAgeRegionCombos_in_OccDistributions_CIP_NOC.LCP4_CD) 
AND (Labour_Supply_Distribution_CIP_NOC.PSSM_Credential = qry_SurveyCIP4CredAgeRegionCombos_in_OccDistributions_CIP_NOC.PSSM_Credential) 
AND (Labour_Supply_Distribution_CIP_NOC.Current_Region_PSSM_Code_Rollup = qry_SurveyCIP4CredAgeRegionCombos_in_OccDistributions_CIP_NOC.Current_Region_PSSM_Code_Rollup) 
AND (Labour_Supply_Distribution_CIP_NOC.Age_Group_Rollup = qry_SurveyCIP4CredAgeRegionCombos_in_OccDistributions_CIP_NOC.Age_Group_Rollup)
WHERE (((qry_SurveyCIP4CredAgeRegionCombos_in_OccDistributions_CIP_NOC.Survey) Is Null) 
AND ((qry_SurveyCIP4CredAgeRegionCombos_in_OccDistributions_CIP_NOC.LCP4_CD) Is Null) 
AND ((qry_SurveyCIP4CredAgeRegionCombos_in_OccDistributions_CIP_NOC.PSSM_Credential) Is Null) 
AND ((qry_SurveyCIP4CredAgeRegionCombos_in_OccDistributions_CIP_NOC.Current_Region_PSSM_Code_Rollup) Is Null) 
AND ((qry_SurveyCIP4CredAgeRegionCombos_in_OccDistributions_CIP_NOC.Age_Group_Rollup) Is Null));"

## qry_append_LabourSupplyDistributioN_Missing_toOccDist_CIP_NOC ----
qry_append_LabourSupplyDistribution_Missing_toOccDist_CIP_NOC <- 
"INSERT INTO Occupation_Distributions_CIP_NOC 
( Survey, LCIP4_CRED, Current_Region_PSSM_Code_Rollup, 
Age_Group_Rollup, [Count], Total, LCIP4_CRED_Cleaned, LCP4_CD, PSSM_Credential, NOC, 
[Percent], Appended_from_LabourSupply )
SELECT tmp_Append_LabourSupplyDistribution_Missing_from_OccDist_CIP_NOC.Survey, 
tmp_Append_LabourSupplyDistribution_Missing_from_OccDist_CIP_NOC.LCIP4_CRED, 
tmp_Append_LabourSupplyDistribution_Missing_from_OccDist_CIP_NOC.Current_Region_PSSM_Code_Rollup, 
tmp_Append_LabourSupplyDistribution_Missing_from_OccDist_CIP_NOC.Age_Group_Rollup, 
0 AS Expr1, 
0 AS Expr2, 
tmp_Append_LabourSupplyDistribution_Missing_from_OccDist_CIP_NOC.LCIP4_CRED_Cleaned, 
tmp_Append_LabourSupplyDistribution_Missing_from_OccDist_CIP_NOC.LCP4_CD, 
tmp_Append_LabourSupplyDistribution_Missing_from_OccDist_CIP_NOC.PSSM_Credential, 
99999 AS Expr3, 
0 AS Expr4, 
'Yes' AS Expr5
FROM tmp_Append_LabourSupplyDistribution_Missing_from_OccDist_CIP_NOC;"

# ******************************************************************************
# Calculations ----

# note commas/dashes have all been converted to _ from existing query names
## CIP_CIP_Totals ----
CIP_CIP_Totals <- 
"SELECT Occupation_Distributions_CIP_NOC.LCP4_CD, 
Sum(Occupation_Distributions_CIP_NOC.Unweighted_Count) AS Unweighted_Total, 
Sum(Occupation_Distributions_CIP_NOC.Count) AS Total, 
Occupation_Distributions_CIP_NOC.Exclude_Flag
INTO CIP_CIP_Totals
FROM Occupation_Distributions_CIP_NOC
GROUP BY Occupation_Distributions_CIP_NOC.LCP4_CD, 
Occupation_Distributions_CIP_NOC.Exclude_Flag
HAVING (((Occupation_Distributions_CIP_NOC.Exclude_Flag) Is Null));"

## CIP_CIP_NOC_Counts ----
CIP_CIP_NOC_Counts <- 
"SELECT Occupation_Distributions_CIP_NOC.LCP4_CD, 
Occupation_Distributions_CIP_NOC.NOC, 
Sum(Occupation_Distributions_CIP_NOC.Unweighted_Count) AS Unweighted_Count, 
Sum(Occupation_Distributions_CIP_NOC.Count) AS [Count], 
Occupation_Distributions_CIP_NOC.Exclude_Flag
INTO CIP_CIP_NOC_Counts
FROM Occupation_Distributions_CIP_NOC
GROUP BY Occupation_Distributions_CIP_NOC.LCP4_CD, 
Occupation_Distributions_CIP_NOC.NOC, 
Occupation_Distributions_CIP_NOC.Exclude_Flag
HAVING (((Occupation_Distributions_CIP_NOC.Exclude_Flag) Is Null));"

## CIP_CIP_NOC_Occ_Dist ----
CIP_CIP_NOC_Occ_Dist <- 
"SELECT CIP_CIP_NOC_Counts.LCP4_CD, 
CIP_CIP_NOC_Counts.NOC, 
CIP_CIP_NOC_Counts.Unweighted_Count, 
CIP_CIP_Totals.Unweighted_Total, 
[Unweighted_Count]/[Unweighted_Total] AS Unweighted_Percent, 
CIP_CIP_NOC_Counts.Count, CIP_CIP_Totals.Total, 
[Count]/[Total] AS [Percent] 
INTO Occupation_Distributions_CIP4D
FROM CIP_CIP_Totals 
INNER JOIN CIP_CIP_NOC_Counts 
ON CIP_CIP_Totals.LCP4_CD = CIP_CIP_NOC_Counts.LCP4_CD;"

## CIP_CIP_CRED_Totals ----
CIP_CIP_CRED_Totals <- 
"SELECT Occupation_Distributions_CIP_NOC.LCIP4_CRED_Cleaned, 
Occupation_Distributions_CIP_NOC.LCP4_CD, 
Occupation_Distributions_CIP_NOC.PSSM_Credential, 
Sum(Occupation_Distributions_CIP_NOC.Unweighted_Count) AS Unweighted_Total, 
Sum(Occupation_Distributions_CIP_NOC.Count) AS Total, 
Occupation_Distributions_CIP_NOC.Exclude_Flag
INTO CIP_CIP_CRED_Totals
FROM Occupation_Distributions_CIP_NOC
GROUP BY Occupation_Distributions_CIP_NOC.LCIP4_CRED_Cleaned, 
Occupation_Distributions_CIP_NOC.LCP4_CD, 
Occupation_Distributions_CIP_NOC.PSSM_Credential, 
Occupation_Distributions_CIP_NOC.Exclude_Flag
HAVING (((Occupation_Distributions_CIP_NOC.Exclude_Flag) Is Null));"

## CIP_CIP_CRED_NOC_Counts ----
CIP_CIP_CRED_NOC_Counts <- 
"SELECT Occupation_Distributions_CIP_NOC.LCIP4_CRED_Cleaned, 
Occupation_Distributions_CIP_NOC.LCP4_CD, 
Occupation_Distributions_CIP_NOC.PSSM_Credential, 
Occupation_Distributions_CIP_NOC.NOC, 
Sum(Occupation_Distributions_CIP_NOC.Unweighted_Count) AS Unweighted_Count, 
Sum(Occupation_Distributions_CIP_NOC.Count) AS [Count], 
Occupation_Distributions_CIP_NOC.Exclude_Flag
INTO CIP_CIP_CRED_NOC_Counts
FROM Occupation_Distributions_CIP_NOC
GROUP BY Occupation_Distributions_CIP_NOC.LCIP4_CRED_Cleaned, 
Occupation_Distributions_CIP_NOC.LCP4_CD, 
Occupation_Distributions_CIP_NOC.PSSM_Credential, 
Occupation_Distributions_CIP_NOC.NOC, 
Occupation_Distributions_CIP_NOC.Exclude_Flag
HAVING (((Occupation_Distributions_CIP_NOC.Exclude_Flag) Is Null));"

## CIP_CIP_CRED_NOC_Occ_Dist ----
CIP_CIP_CRED_NOC_Occ_Dist <- 
"SELECT CIP_CIP_CRED_NOC_Counts.LCIP4_CRED_Cleaned, 
CIP_CIP_CRED_NOC_Counts.LCP4_CD, 
CIP_CIP_CRED_NOC_Counts.PSSM_Credential, 
CIP_CIP_CRED_NOC_Counts.NOC, 
CIP_CIP_CRED_NOC_Counts.Unweighted_Count, 
CIP_CIP_Totals.Unweighted_Total, 
[Unweighted_Count]/[Unweighted_Total] AS Unweighted_Percent, 
CIP_CIP_CRED_NOC_Counts.Count, 
CIP_CIP_Totals.Total, 
[Count]/[Total] AS [Percent] 
INTO Occupation_Distributions_CIP4D_Credential
FROM CIP_CIP_Totals 
INNER JOIN CIP_CIP_CRED_NOC_Counts 
ON CIP_CIP_Totals.LCP4_CD = CIP_CIP_CRED_NOC_Counts.LCP4_CD
ORDER BY CIP_CIP_CRED_NOC_Counts.LCP4_CD;"

## CIP_NOC_Totals ----
CIP_NOC_Totals <- 
"SELECT Occupation_Distributions_CIP_NOC.NOC, 
Sum(Occupation_Distributions_CIP_NOC.Unweighted_Count) AS Unweighted_Total, 
Sum(Occupation_Distributions_CIP_NOC.Count) AS Total, 
Occupation_Distributions_CIP_NOC.Exclude_Flag
INTO CIP_NOC_Totals
FROM Occupation_Distributions_CIP_NOC
GROUP BY Occupation_Distributions_CIP_NOC.NOC, 
Occupation_Distributions_CIP_NOC.Exclude_Flag
HAVING (((Occupation_Distributions_CIP_NOC.Exclude_Flag) Is Null));"

## CIP_NOC_CIP_Occ_Dist ----
CIP_NOC_CIP_Occ_Dist <- 
"SELECT CIP_CIP_NOC_Counts.NOC, 
CIP_CIP_NOC_Counts.LCP4_CD, 
CIP_CIP_NOC_Counts.Unweighted_Count, 
CIP_NOC_Totals.Unweighted_Total, 
[Unweighted_Count]/[Unweighted_Total] AS Unweighted_Percent, 
CIP_CIP_NOC_Counts.Count, 
CIP_NOC_Totals.Total, 
[Count]/[Total] AS [Percent] 
INTO CIP4D_Distributions_NOC
FROM CIP_NOC_Totals 
INNER JOIN CIP_CIP_NOC_Counts 
ON CIP_NOC_Totals.NOC = CIP_CIP_NOC_Counts.NOC
ORDER BY CIP_CIP_NOC_Counts.NOC;"

## CIP_NOC_CRED_Totals ----
CIP_NOC_CRED_Totals <- 
"SELECT Occupation_Distributions_CIP_NOC.NOC, 
Occupation_Distributions_CIP_NOC.PSSM_Credential, 
Sum(Occupation_Distributions_CIP_NOC.Unweighted_Count) AS Unweighted_Total, 
Sum(Occupation_Distributions_CIP_NOC.Count) AS Total
INTO CIP_NOC_CRED_Totals
FROM Occupation_Distributions_CIP_NOC
GROUP BY Occupation_Distributions_CIP_NOC.NOC, 
Occupation_Distributions_CIP_NOC.PSSM_Credential;"

## CIP_NOC_CRED_CIP_Occ_Dist ----
CIP_NOC_CRED_CIP_Occ_Dist <- 
"SELECT CIP_CIP_CRED_NOC_Counts.NOC, 
CIP_CIP_CRED_NOC_Counts.PSSM_Credential, 
CIP_CIP_CRED_NOC_Counts.LCIP4_CRED_Cleaned, 
CIP_CIP_CRED_NOC_Counts.LCP4_CD, 
CIP_CIP_CRED_NOC_Counts.Unweighted_Count, 
CIP_NOC_Totals.Unweighted_Total, 
[Unweighted_Count]/[Unweighted_Total] AS Unweighted_Percent, 
CIP_CIP_CRED_NOC_Counts.Count, 
CIP_NOC_Totals.Total, 
[Count]/[Total] AS [Percent] 
INTO CIP4D_Distributions_NOC_Credential
FROM CIP_NOC_Totals 
INNER JOIN CIP_CIP_CRED_NOC_Counts
ON CIP_NOC_Totals.NOC = CIP_CIP_CRED_NOC_Counts.NOC
ORDER BY CIP_CIP_CRED_NOC_Counts.NOC;"
