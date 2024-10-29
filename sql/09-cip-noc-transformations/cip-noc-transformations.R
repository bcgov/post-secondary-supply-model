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
