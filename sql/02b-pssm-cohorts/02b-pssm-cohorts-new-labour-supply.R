
# ---- Q005 - Q007 New labour supply ----
DACSO_Q99A_STQUI_ID <- "
SELECT STQU_ID AS Survey_STQU_ID, 
       Survey, 
       Right(STQU_ID,LEN(STQU_ID)-CHARINDEX('-',STQU_ID)-1) AS STQU_ID_Only
INTO   DACSO_Q99A_STQUI_ID
FROM   T_Cohorts_Recoded;"

DACSO_Q005_DACSO_DATA_Part_1b4_Check_NOC_Valid <- "
SELECT    dacso_q99a_stqui_id.stqu_id_only,
          t_cohorts_recoded.stqu_id,
          t_cohorts_recoded.survey,
          t_cohorts_recoded.survey_year,
          t_cohorts_recoded.noc_cd,
          tbl_noc_skill_level_aged_17_34.unit_group_code
FROM      (t_cohorts_recoded
  LEFT JOIN tbl_noc_skill_level_aged_17_34
    ON t_cohorts_recoded.noc_cd =  tbl_noc_skill_level_aged_17_34.unit_group_code)
  INNER JOIN dacso_q99a_stqui_id
   ON t_cohorts_recoded.stqu_id = dacso_q99a_stqui_id.survey_stqu_id
WHERE     t_cohorts_recoded.age_group_rollup IS NOT NULL
  AND     t_cohorts_recoded.current_region_pssm_code <>- 1
GROUP  BY dacso_q99a_stqui_id.stqu_id_only,
          t_cohorts_recoded.stqu_id,
          t_cohorts_recoded.survey,
          t_cohorts_recoded.survey_year,
          t_cohorts_recoded.noc_cd,
          tbl_noc_skill_level_aged_17_34.unit_group_code
HAVING    t_cohorts_recoded.survey_year IN ('2019','2020','2021','2022','2023') 
  AND     t_cohorts_recoded.noc_cd IS NOT NULL
  AND     t_cohorts_recoded.noc_cd <> ''
  AND     tbl_noc_skill_level_aged_17_34.unit_group_code IS NULL;"


DACSO_Q005_DACSO_Data_Part_1b7_Update_After_Recoding <- "
UPDATE t_dacso_data_part_1
INNER JOIN t_dacso_noc_recoding
ON t_dacso_data_part_1.coci_stqu_id = t_dacso_noc_recoding.stqu_id_only
SET    t_dacso_data_part_1.labr_occupation_lnoc_cd = t_dacso_noc_recoding.noc_cd;"

DACSO_Q005_DACSO_Data_Part_1b8_Update_After_Recoding <- "
UPDATE t_bgs_data_final
INNER JOIN bgs_noc_recoded_imputed
ON t_bgs_data_final.stqu_id = bgs_noc_recoded_imputed.stqu_id
SET    t_bgs_data_final.noc_cd_2016 = bgs_noc_recoded_imputed.noc;"


DACSO_Q005_DACSO_DATA_Part_1c_NLS1 <- "
SELECT  t_cohorts_recoded.survey,
        t_current_region_pssm_rollup_codes.current_region_pssm_code_rollup,
        t_cohorts_recoded.age_group_rollup,
        t_cohorts_recoded.inst_cd,
        t_cohorts_recoded.lcip4_cred,
        t_cohorts_recoded.grad_status,
        t_cohorts_recoded.new_labour_supply,
        Count(*) AS Base
INTO    DACSO_Q005_DACSO_DATA_Part_1c_NLS1
FROM    t_cohorts_recoded
        INNER JOIN (t_current_region_pssm_codes
                INNER JOIN t_current_region_pssm_rollup_codes
                ON t_current_region_pssm_codes.current_region_pssm_code_rollup = t_current_region_pssm_rollup_codes.current_region_pssm_code_rollup)
        ON t_cohorts_recoded.current_region_pssm_code = t_current_region_pssm_codes.current_region_pssm_code
WHERE   CAST(t_cohorts_recoded.weight AS FLOAT) > 0
  AND   t_cohorts_recoded.noc_cd IS NOT NULL
GROUP BY t_cohorts_recoded.survey,
        t_current_region_pssm_rollup_codes.current_region_pssm_code_rollup,
        t_cohorts_recoded.age_group_rollup,
        t_cohorts_recoded.inst_cd,
        t_cohorts_recoded.lcip4_cred,
        t_cohorts_recoded.grad_status,
        t_cohorts_recoded.new_labour_supply
HAVING  t_cohorts_recoded.age_group_rollup IS NOT NULL
  AND   Count(*) > 0
  AND  (t_cohorts_recoded.grad_status = '1' OR t_cohorts_recoded.grad_status = '3' )
  AND  CAST(t_cohorts_recoded.new_labour_supply AS FLOAT) = 1;"

DACSO_Q005_DACSO_DATA_Part_1c_NLS2 <- "
SELECT t_cohorts_recoded.survey,
       t_current_region_pssm_rollup_codes.current_region_pssm_code_rollup,
       t_cohorts_recoded.age_group_rollup,
       t_cohorts_recoded.inst_cd,
       t_cohorts_recoded.lcip4_cred,
       t_cohorts_recoded.grad_status,
       t_cohorts_recoded.new_labour_supply,
       t_cohorts_recoded.stqu_id
INTO   DACSO_Q005_DACSO_DATA_Part_1c_NLS2
FROM   t_cohorts_recoded
       INNER JOIN (t_current_region_pssm_codes
            INNER JOIN t_current_region_pssm_rollup_codes
            ON t_current_region_pssm_codes.current_region_pssm_code_rollup = t_current_region_pssm_rollup_codes.current_region_pssm_code_rollup)
       ON t_cohorts_recoded.current_region_pssm_code = t_current_region_pssm_codes.current_region_pssm_code
WHERE  CAST(t_cohorts_recoded.weight AS FLOAT) > 0
GROUP BY t_cohorts_recoded.survey,
       t_current_region_pssm_rollup_codes.current_region_pssm_code_rollup,
       t_cohorts_recoded.age_group_rollup,
       t_cohorts_recoded.inst_cd,
       t_cohorts_recoded.lcip4_cred,
       t_cohorts_recoded.grad_status,
       t_cohorts_recoded.new_labour_supply,
       t_cohorts_recoded.stqu_id
HAVING t_cohorts_recoded.age_group_rollup IS NOT NULL
  AND  (t_cohorts_recoded.grad_status = '1' OR t_cohorts_recoded.grad_status = '3')
  AND  CAST(t_cohorts_recoded.new_labour_supply AS FLOAT) = 2;"

DACSO_Q005_DACSO_DATA_Part_1c_NLS2_Recode <- "
UPDATE t_cohorts_recoded 
SET t_cohorts_recoded.new_labour_supply = 3 
WHERE t_cohorts_recoded.STQU_ID IN (
SELECT t_cohorts_recoded.STQU_ID 
FROM      dacso_q005_dacso_data_part_1c_nls2
INNER JOIN t_cohorts_recoded
ON        dacso_q005_dacso_data_part_1c_nls2.stqu_id = t_cohorts_recoded.stqu_id
LEFT JOIN dacso_q005_dacso_data_part_1c_nls1
ON        dacso_q005_dacso_data_part_1c_nls2.lcip4_cred = dacso_q005_dacso_data_part_1c_nls1.lcip4_cred
  AND     dacso_q005_dacso_data_part_1c_nls2.inst_cd = dacso_q005_dacso_data_part_1c_nls1.inst_cd
  AND     dacso_q005_dacso_data_part_1c_nls2.age_group_rollup = dacso_q005_dacso_data_part_1c_nls1.age_group_rollup
  AND     dacso_q005_dacso_data_part_1c_nls2.current_region_pssm_code_rollup = dacso_q005_dacso_data_part_1c_nls1.current_region_pssm_code_rollup
  AND     dacso_q005_dacso_data_part_1c_nls2.survey = dacso_q005_dacso_data_part_1c_nls1.survey
WHERE     dacso_q005_dacso_data_part_1c_nls1.survey IS NULL
  AND     dacso_q005_dacso_data_part_1c_nls1.current_region_pssm_code_rollup IS NULL
  AND     dacso_q005_dacso_data_part_1c_nls1.age_group_rollup IS NULL
  AND     dacso_q005_dacso_data_part_1c_nls1.inst_cd IS NULL
  AND     dacso_q005_dacso_data_part_1c_nls1.lcip4_cred IS NULL);"

DACSO_Q005_Z_Cohort_Resp_by_Region <- "
SELECT      T_Cohorts_Recoded.Survey, 
            T_Cohorts_Recoded.Survey_Year,
            T_Current_Region_PSSM_Codes.Current_Region_PSSM_Code, 
            T_Current_Region_PSSM_Codes.Current_Region_PSSM_Name, 
            T_Cohorts_Recoded.Age_Group_Rollup,
            Count(*) as N
FROM        T_Cohorts_Recoded 
INNER JOIN  T_Current_Region_PSSM_Codes 
ON          T_Cohorts_Recoded.CURRENT_REGION_PSSM_CODE = T_Current_Region_PSSM_Codes.Current_Region_PSSM_Code
WHERE       T_Cohorts_Recoded.Age_Group_Rollup Is Not Null
AND         T_Cohorts_Recoded.Respondent='1'
AND         T_Cohorts_Recoded.Weight > 0
GROUP BY    T_Cohorts_Recoded.Survey, 
            T_Cohorts_Recoded.Survey_Year,
            T_Current_Region_PSSM_Codes.Current_Region_PSSM_Code, 
            T_Current_Region_PSSM_Codes.Current_Region_PSSM_Name, 
            T_Cohorts_Recoded.Age_Group_Rollup
ORDER BY    T_Cohorts_Recoded.Survey, T_Cohorts_Recoded.Survey_Year,
            T_Current_Region_PSSM_Codes.Current_Region_PSSM_Code;"

DACSO_Q005_Z01_Base_NLS <- 
"SELECT t_cohorts_recoded.survey,
       t_cohorts_recoded.inst_cd,
       t_cohorts_recoded.age_group_rollup,
       t_cohorts_recoded.ttrain,
       t_cohorts_recoded.lcip4_cred,
       Count(*) AS Base,
       t_cohorts_recoded.stqu_id
INTO DACSO_Q005_Z01_Base_NLS
FROM   t_cohorts_recoded
WHERE  ( ( ( t_cohorts_recoded.new_labour_supply ) = 0
            OR ( t_cohorts_recoded.new_labour_supply ) = 1
            OR ( t_cohorts_recoded.new_labour_supply ) = 2
            OR ( t_cohorts_recoded.new_labour_supply ) = 3 )
         AND ( ( t_cohorts_recoded.weight ) > 0 ) )
GROUP  BY t_cohorts_recoded.survey,
          t_cohorts_recoded.inst_cd,
          t_cohorts_recoded.age_group_rollup,
          t_cohorts_recoded.ttrain,
          t_cohorts_recoded.lcip4_cred,
          t_cohorts_recoded.stqu_id,
          t_cohorts_recoded.grad_status
HAVING ( ( ( t_cohorts_recoded.age_group_rollup ) IS NOT NULL )
         AND ( ( t_cohorts_recoded.grad_status ) = '1'
                OR ( t_cohorts_recoded.grad_status ) = '3' ) );"

DACSO_Q005_Z02a_Base <- 
"SELECT T_Cohorts_Recoded.Survey, T_Cohorts_Recoded.INST_CD, T_Cohorts_Recoded.Age_Group_Rollup, T_Cohorts_Recoded.GRAD_STATUS, T_Cohorts_Recoded.TTRAIN, T_Cohorts_Recoded.LCIP4_CRED, Count(*) AS Count, Count(*) AS Base
FROM T_Cohorts_Recoded
WHERE (((T_Cohorts_Recoded.New_Labour_Supply)=0 Or (T_Cohorts_Recoded.New_Labour_Supply)=1 Or (T_Cohorts_Recoded.New_Labour_Supply)=2 Or (T_Cohorts_Recoded.New_Labour_Supply)=3) AND ((T_Cohorts_Recoded.Weight)>0))
GROUP BY T_Cohorts_Recoded.Survey, T_Cohorts_Recoded.INST_CD, T_Cohorts_Recoded.Age_Group_Rollup, T_Cohorts_Recoded.GRAD_STATUS, T_Cohorts_Recoded.TTRAIN, T_Cohorts_Recoded.LCIP4_CRED
HAVING (((T_Cohorts_Recoded.Age_Group_Rollup) Is Not Null) AND ((T_Cohorts_Recoded.GRAD_STATUS)='1' Or (T_Cohorts_Recoded.GRAD_STATUS)='3'));"

DACSO_Q005_Z02b_Respondents <- 
"SELECT T_Cohorts_Recoded.Survey, T_Cohorts_Recoded.INST_CD, T_Cohorts_Recoded.Age_Group_Rollup, T_Cohorts_Recoded.GRAD_STATUS, T_Cohorts_Recoded.TTRAIN, T_Cohorts_Recoded.LCIP4_CRED, Sum(IIf(Respondent='1' And Current_Region_PSSM_Code<>-1,1,0)) AS Respondents
FROM T_Cohorts_Recoded
WHERE (((T_Cohorts_Recoded.New_Labour_Supply)=0 Or (T_Cohorts_Recoded.New_Labour_Supply)=1 Or (T_Cohorts_Recoded.New_Labour_Supply)=2 Or (T_Cohorts_Recoded.New_Labour_Supply)=3) AND ((T_Cohorts_Recoded.Weight)>0))
GROUP BY T_Cohorts_Recoded.Survey, T_Cohorts_Recoded.INST_CD, T_Cohorts_Recoded.Age_Group_Rollup, T_Cohorts_Recoded.GRAD_STATUS, T_Cohorts_Recoded.TTRAIN, T_Cohorts_Recoded.LCIP4_CRED
HAVING (((T_Cohorts_Recoded.Age_Group_Rollup) Is Not Null) AND ((T_Cohorts_Recoded.GRAD_STATUS)='1' Or (T_Cohorts_Recoded.GRAD_STATUS)='3'));"

DACSO_Q005_Z02b_Respondents_Region_9999 <- 
"SELECT T_Cohorts_Recoded.Survey, T_Cohorts_Recoded.INST_CD, T_Cohorts_Recoded.Age_Group_Rollup, T_Cohorts_Recoded.GRAD_STATUS, T_Cohorts_Recoded.TTRAIN, T_Cohorts_Recoded.LCIP4_CRED, Sum(IIf(Respondent='1',1,0)) AS Respondents
FROM T_Cohorts_Recoded
WHERE (((T_Cohorts_Recoded.CURRENT_REGION_PSSM_CODE)=-1) AND ((T_Cohorts_Recoded.New_Labour_Supply)=0 Or (T_Cohorts_Recoded.New_Labour_Supply)=1 Or (T_Cohorts_Recoded.New_Labour_Supply)=2 Or (T_Cohorts_Recoded.New_Labour_Supply)=3) AND ((T_Cohorts_Recoded.Weight)>0))
GROUP BY T_Cohorts_Recoded.Survey, T_Cohorts_Recoded.INST_CD, T_Cohorts_Recoded.Age_Group_Rollup, T_Cohorts_Recoded.GRAD_STATUS, T_Cohorts_Recoded.TTRAIN, T_Cohorts_Recoded.LCIP4_CRED
HAVING (((T_Cohorts_Recoded.Age_Group_Rollup) Is Not Null) AND ((T_Cohorts_Recoded.GRAD_STATUS)='1' Or (T_Cohorts_Recoded.GRAD_STATUS)='3'));"

#DACSO_Q005_Z02b_Respondents_Region_9999=100% <- 
#"SELECT DACSO_Q005_Z02a_Base.Survey, DACSO_Q005_Z02a_Base.INST_CD, DACSO_Q005_Z02a_Base.Age_Group_Rollup, DACSO_Q005_Z02a_Base.GRAD_STATUS, DACSO_Q005_Z02a_Base.TTRAIN, DACSO_Q005_Z02a_Base.LCIP4_CRED, DACSO_Q005_Z02a_Base.Count, DACSO_Q005_Z02a_Base.Base, DACSO_Q005_Z02b_Respondents_Region_9999.Respondents, Respondents/Count AS Expr1
#FROM DACSO_Q005_Z02a_Base INNER JOIN DACSO_Q005_Z02b_Respondents_Region_9999 ON (DACSO_Q005_Z02a_Base.Survey = DACSO_Q005_Z02b_Respondents_Region_9999.Survey) AND (DACSO_Q005_Z02a_Base.INST_CD = DACSO_Q005_Z02b_Respondents_Region_9999.INST_CD) AND (DACSO_Q005_Z02a_Base.Age_Group_Rollup = DACSO_Q005_Z02b_Respondents_Region_9999.Age_Group_Rollup) AND (DACSO_Q005_Z02a_Base.GRAD_STATUS = DACSO_Q005_Z02b_Respondents_Region_9999.GRAD_STATUS) AND (DACSO_Q005_Z02a_Base.LCIP4_CRED = DACSO_Q005_Z02b_Respondents_Region_9999.LCIP4_CRED)
#WHERE (((Respondents/Count)=1));"

DACSO_Q005_Z02b_Respondents_Union <- 
"SELECT DACSO_Q005_Z02b_Respondents.Survey, DACSO_Q005_Z02b_Respondents.INST_CD, DACSO_Q005_Z02b_Respondents.Age_Group_Rollup, DACSO_Q005_Z02b_Respondents.GRAD_STATUS, DACSO_Q005_Z02b_Respondents.TTRAIN, DACSO_Q005_Z02b_Respondents.LCIP4_CRED, DACSO_Q005_Z02b_Respondents.Respondents
FROM DACSO_Q005_Z02b_Respondents
UNION ALL SELECT DACSO_Q005_Z02b_Respondents_Region_9999=100%.Survey, DACSO_Q005_Z02b_Respondents_Region_9999=100%.INST_CD, DACSO_Q005_Z02b_Respondents_Region_9999=100%.Age_Group_Rollup, DACSO_Q005_Z02b_Respondents_Region_9999=100%.GRAD_STATUS, DACSO_Q005_Z02b_Respondents_Region_9999=100%.TTRAIN, DACSO_Q005_Z02b_Respondents_Region_9999=100%.LCIP4_CRED, DACSO_Q005_Z02b_Respondents_Region_9999=100%.Respondents
FROM DACSO_Q005_Z02b_Respondents_Region_9999=100%;"

DACSO_Q005_Z02c_Weight_tmp <- 
"SELECT t_cohorts_recoded.survey,
       t_cohorts_recoded.survey_year,
       t_cohorts_recoded.inst_cd,
       t_cohorts_recoded.age_group_rollup,
       t_cohorts_recoded.grad_status,
       t_cohorts_recoded.ttrain,
       t_cohorts_recoded.lcip4_cred,
       Count(*) AS Count,
       Sum(CASE WHEN respondent = '1' AND current_region_pssm_code <>- 1 THEN 1 ELSE 0 END) AS Respondents,
       t_cohorts_recoded.weight AS weight_year
INTO DACSO_Q005_Z02c_Weight_tmp
FROM   t_cohorts_recoded
WHERE  ( ( ( t_cohorts_recoded.new_labour_supply ) = 0
            OR ( t_cohorts_recoded.new_labour_supply ) = 1
            OR ( t_cohorts_recoded.new_labour_supply ) = 2
            OR ( t_cohorts_recoded.new_labour_supply ) = 3 )
         AND ( ( t_cohorts_recoded.weight ) > 0 ) )
GROUP  BY t_cohorts_recoded.survey,
          t_cohorts_recoded.survey_year,
          t_cohorts_recoded.inst_cd,
          t_cohorts_recoded.age_group_rollup,
          t_cohorts_recoded.grad_status,
          t_cohorts_recoded.ttrain,
          t_cohorts_recoded.lcip4_cred,
          t_cohorts_recoded.weight
HAVING ( ( ( t_cohorts_recoded.age_group_rollup ) IS NOT NULL )
         AND ( ( t_cohorts_recoded.grad_status ) = '1'
                OR ( t_cohorts_recoded.grad_status ) = '3' ) );"

DACSO_Q005_Z02c_Weight <- "
SELECT *, 
    Weight_Prob * Weight_Year AS Weight, 
    Respondents * Weight_Prob * Weight_Year AS Weighted
INTO DACSO_Q005_Z02c_Weight
FROM
  (SELECT *, CASE WHEN (Respondents = 0) THEN 1 ELSE cast(Count as float)/cast(Respondents as float) END AS Weight_Prob
  FROM  DACSO_Q005_Z02c_Weight_tmp) T;"

DACSO_Q005_Z03_Weight_Total <- 
"SELECT dacso_q005_z02c_weight.survey,
       dacso_q005_z02c_weight.inst_cd,
       dacso_q005_z02c_weight.age_group_rollup,
       dacso_q005_z02c_weight.grad_status,
       dacso_q005_z02c_weight.ttrain,
       dacso_q005_z02c_weight.lcip4_cred,
       Sum(dacso_q005_z02c_weight.count)    AS Base,
       Sum(dacso_q005_z02c_weight.weighted) AS Weighted
INTO DACSO_Q005_Z03_Weight_Total
FROM   dacso_q005_z02c_weight
GROUP  BY dacso_q005_z02c_weight.survey,
          dacso_q005_z02c_weight.inst_cd,
          dacso_q005_z02c_weight.age_group_rollup,
          dacso_q005_z02c_weight.grad_status,
          dacso_q005_z02c_weight.ttrain,
          dacso_q005_z02c_weight.lcip4_cred;"

DACSO_Q005_Z04_Weight_Adj_Fac <- 
"SELECT dacso_q005_z03_weight_total.survey,
       dacso_q005_z03_weight_total.inst_cd,
       dacso_q005_z03_weight_total.age_group_rollup,
       dacso_q005_z03_weight_total.grad_status,
       dacso_q005_z03_weight_total.ttrain,
       dacso_q005_z03_weight_total.lcip4_cred,
       dacso_q005_z03_weight_total.base,
       dacso_q005_z03_weight_total.weighted,
       CASE WHEN weighted = 0 THEN 0 ELSE (base / weighted) END AS Weight_Adj_Fac
INTO DACSO_Q005_Z04_Weight_Adj_Fac
FROM   dacso_q005_z03_weight_total;"

DACSO_Q005_Z05_Weight_NLS <- "
SELECT dacso_q005_z02c_weight.survey,
       dacso_q005_z02c_weight.survey_year,
       dacso_q005_z02c_weight.inst_cd,
       dacso_q005_z02c_weight.age_group_rollup,
       dacso_q005_z02c_weight.grad_status,
       dacso_q005_z02c_weight.ttrain,
       dacso_q005_z02c_weight.lcip4_cred,
       dacso_q005_z02c_weight.count,
       dacso_q005_z02c_weight.respondents,
       dacso_q005_z02c_weight.weight_prob,
       dacso_q005_z02c_weight.weight_year,
       dacso_q005_z02c_weight.weight,
       dacso_q005_z02c_weight.weighted,
       dacso_q005_z04_weight_adj_fac.weight_adj_fac,
       weight * weight_adj_fac AS Weight_NLS
INTO   tmp_tbl_weights_nls
FROM   dacso_q005_z02c_weight
       INNER JOIN dacso_q005_z04_weight_adj_fac
               ON ( dacso_q005_z02c_weight.lcip4_cred = dacso_q005_z04_weight_adj_fac.lcip4_cred )
                  AND ( dacso_q005_z02c_weight.grad_status = dacso_q005_z04_weight_adj_fac.grad_status )
                  AND ( dacso_q005_z02c_weight.survey = dacso_q005_z04_weight_adj_fac.survey )
                  AND ( dacso_q005_z02c_weight.inst_cd = dacso_q005_z04_weight_adj_fac.inst_cd )
                  AND ( dacso_q005_z02c_weight.age_group_rollup = dacso_q005_z04_weight_adj_fac.age_group_rollup );"

DACSO_Q005_Z06_Add_Weight_NLS_Field <- 
"ALTER TABLE T_Cohorts_Recoded ADD Weight_NLS Float NULL;"

DACSO_Q005_Z07_Weight_NLS_Null <- 
"UPDATE T_Cohorts_Recoded SET T_Cohorts_Recoded.Weight_NLS = Null;"

DACSO_Q005_Z08_Weight_NLS_Update <- "
UPDATE t_cohorts_recoded
SET    t_cohorts_recoded.weight_nls = tmp_tbl_Weights_NLS.weight_nls
FROM   tmp_tbl_Weights_NLS
INNER JOIN (t_cohorts_recoded
  INNER JOIN dacso_q005_z01_base_nls
  ON t_cohorts_recoded.stqu_id = dacso_q005_z01_base_nls.stqu_id)
ON   tmp_tbl_Weights_NLS.lcip4_cred = t_cohorts_recoded.lcip4_cred
AND tmp_tbl_Weights_NLS.grad_status =  t_cohorts_recoded.grad_status
AND tmp_tbl_Weights_NLS.age_group_rollup =  t_cohorts_recoded.age_group_rollup
AND tmp_tbl_Weights_NLS.inst_cd =  t_cohorts_recoded.inst_cd
AND tmp_tbl_Weights_NLS.survey_year = t_cohorts_recoded.survey_year
AND tmp_tbl_Weights_NLS.survey = t_cohorts_recoded.survey
WHERE  t_cohorts_recoded.current_region_pssm_code <>- 1;"

DACSO_Q005_Z09_Check_Weights <- "
SELECT t_cohorts_recoded.survey_year,
       t_cohorts_recoded.inst_cd,
       t_cohorts_recoded.age_group_rollup,
       t_cohorts_recoded.grad_status,
       t_cohorts_recoded.ttrain,
       t_cohorts_recoded.lcip4_cred,
       Sum(CASE WHEN respondent = '1' AND current_region_pssm_code <>- 1 THEN 1 ELSE 0 END) AS Respondents,
       t_cohorts_recoded.weight_nls,
       Sum(CASE WHEN respondent = '1' AND current_region_pssm_code <>- 1 THEN 1 ELSE 0 END) * cast(weight_nls as float) AS Weighted,
       Sum(dacso_q005_z01_base_nls.base) AS Base
FROM   t_cohorts_recoded
INNER JOIN dacso_q005_z01_base_nls
   ON t_cohorts_recoded.stqu_id = dacso_q005_z01_base_nls.stqu_id
GROUP  BY t_cohorts_recoded.survey_year,
          t_cohorts_recoded.inst_cd,
          t_cohorts_recoded.age_group_rollup,
          t_cohorts_recoded.grad_status,
          t_cohorts_recoded.ttrain,
          t_cohorts_recoded.lcip4_cred,
          t_cohorts_recoded.weight_nls
ORDER  BY t_cohorts_recoded.survey_year,
          t_cohorts_recoded.weight_nls;"

DACSO_Q005_Z09_Check_Weights_No_Weight_CIP <- 
"SELECT T_Cohorts_Recoded.Survey, T_Cohorts_Recoded.INST_CD, T_Cohorts_Recoded.Age_Group_Rollup, 
T_Cohorts_Recoded.TTRAIN, T_Cohorts_Recoded.LCIP4_CRED, Count(*) AS Base
FROM T_Cohorts_Recoded
WHERE (((T_Cohorts_Recoded.New_Labour_Supply)=0 Or (T_Cohorts_Recoded.New_Labour_Supply)=1 Or (T_Cohorts_Recoded.New_Labour_Supply)=2 Or (T_Cohorts_Recoded.New_Labour_Supply)=3) AND ((T_Cohorts_Recoded.Weight)>0) AND ((T_Cohorts_Recoded.Weight_NLS)=0 Or (T_Cohorts_Recoded.Weight_NLS) Is Null) AND ((T_Cohorts_Recoded.CURRENT_REGION_PSSM_CODE)<>-1))
GROUP BY T_Cohorts_Recoded.Survey, T_Cohorts_Recoded.INST_CD, T_Cohorts_Recoded.Age_Group_Rollup, T_Cohorts_Recoded.TTRAIN, T_Cohorts_Recoded.LCIP4_CRED, T_Cohorts_Recoded.GRAD_STATUS
HAVING (((T_Cohorts_Recoded.Age_Group_Rollup) Is Not Null) AND ((T_Cohorts_Recoded.GRAD_STATUS)='1' Or (T_Cohorts_Recoded.GRAD_STATUS)='2' Or (T_Cohorts_Recoded.GRAD_STATUS)='3'));"

DACSO_Q006a_Weight_New_Labour_Supply <- 
"SELECT t_cohorts_recoded.pssm_credential,
       t_cohorts_recoded.pssm_cred,
       t_current_region_pssm_rollup_codes.current_region_pssm_code_rollup,
       t_cohorts_recoded.survey_year,
       t_cohorts_recoded.inst_cd,
       t_cohorts_recoded.age_group_rollup,
       t_cohorts_recoded.grad_status,
       t_cohorts_recoded.lcp4_cd,
       t_cohorts_recoded.ttrain,
       t_cohorts_recoded.lcip4_cred,
       t_cohorts_recoded.lcip2_cred,
       t_cohorts_recoded.new_labour_supply,
       Count(*) AS Count,
       t_cohorts_recoded.weight_nls,
       Count(*) * weight_nls AS Weighted
INTO DACSO_Q006a_Weight_New_Labour_Supply
FROM   t_cohorts_recoded
INNER JOIN (t_current_region_pssm_codes
     INNER JOIN t_current_region_pssm_rollup_codes
      ON t_current_region_pssm_codes.current_region_pssm_code_rollup = t_current_region_pssm_rollup_codes.current_region_pssm_code_rollup)
ON t_cohorts_recoded.current_region_pssm_code = t_current_region_pssm_codes.current_region_pssm_code
WHERE  ( ( ( t_cohorts_recoded.respondent ) = '1' )
         AND ( ( t_cohorts_recoded.weight ) > 0 ) )
GROUP  BY t_cohorts_recoded.pssm_credential,
          t_cohorts_recoded.pssm_cred,
          t_current_region_pssm_rollup_codes.current_region_pssm_code_rollup,
          t_cohorts_recoded.survey_year,
          t_cohorts_recoded.inst_cd,
          t_cohorts_recoded.age_group_rollup,
          t_cohorts_recoded.grad_status,
          t_cohorts_recoded.lcp4_cd,
          t_cohorts_recoded.ttrain,
          t_cohorts_recoded.lcip4_cred,
          t_cohorts_recoded.lcip2_cred,
          t_cohorts_recoded.new_labour_supply,
          t_cohorts_recoded.weight_nls
HAVING  t_current_region_pssm_rollup_codes.current_region_pssm_code_rollup  <> 9999
AND     t_cohorts_recoded.age_group_rollup IS NOT NULL
AND   ( t_cohorts_recoded.grad_status = '1' 
  OR    t_cohorts_recoded.grad_status = '3' )
AND   ( t_cohorts_recoded.new_labour_supply = 0 
  OR    t_cohorts_recoded.new_labour_supply = 1
  OR    t_cohorts_recoded.new_labour_supply = 2 
  OR    t_cohorts_recoded.new_labour_supply = 3 ); "

DACSO_Q006b_Weighted_New_Labour_Supply <- 
"SELECT dacso_q006a_weight_new_labour_supply.pssm_credential,
       dacso_q006a_weight_new_labour_supply.pssm_cred,
       dacso_q006a_weight_new_labour_supply.current_region_pssm_code_rollup,
       dacso_q006a_weight_new_labour_supply.age_group_rollup,
       dacso_q006a_weight_new_labour_supply.lcp4_cd,
       dacso_q006a_weight_new_labour_supply.ttrain,
       dacso_q006a_weight_new_labour_supply.lcip4_cred,
       dacso_q006a_weight_new_labour_supply.lcip2_cred,
       Sum(dacso_q006a_weight_new_labour_supply.weighted) AS Count,
       Sum(dacso_q006a_weight_new_labour_supply.count)    AS Unweighted_Count
INTO   dacso_q006b_weighted_new_labour_supply
FROM   dacso_q006a_weight_new_labour_supply
WHERE  (( ( dacso_q006a_weight_new_labour_supply.new_labour_supply ) = 1
           OR ( dacso_q006a_weight_new_labour_supply.new_labour_supply ) = 2
           OR ( dacso_q006a_weight_new_labour_supply.new_labour_supply ) = 3 ))
GROUP  BY dacso_q006a_weight_new_labour_supply.pssm_credential,
          dacso_q006a_weight_new_labour_supply.pssm_cred,
          dacso_q006a_weight_new_labour_supply.current_region_pssm_code_rollup,
          dacso_q006a_weight_new_labour_supply.age_group_rollup,
          dacso_q006a_weight_new_labour_supply.lcp4_cd,
          dacso_q006a_weight_new_labour_supply.ttrain,
          dacso_q006a_weight_new_labour_supply.lcip4_cred,
          dacso_q006a_weight_new_labour_supply.lcip2_cred
HAVING (( ( dacso_q006a_weight_new_labour_supply.age_group_rollup ) IS NOT NULL
        ));"

DACSO_Q006b_Weighted_New_Labour_Supply_0 <- 
"SELECT DACSO_Q006a_Weight_New_Labour_Supply.PSSM_Credential, DACSO_Q006a_Weight_New_Labour_Supply.PSSM_CRED, DACSO_Q006a_Weight_New_Labour_Supply.Current_Region_PSSM_Code_Rollup, DACSO_Q006a_Weight_New_Labour_Supply.Age_Group_Rollup, DACSO_Q006a_Weight_New_Labour_Supply.LCP4_CD, DACSO_Q006a_Weight_New_Labour_Supply.TTRAIN, DACSO_Q006a_Weight_New_Labour_Supply.LCIP4_CRED, DACSO_Q006a_Weight_New_Labour_Supply.LCIP2_CRED, Sum(DACSO_Q006a_Weight_New_Labour_Supply.Weighted) AS Count
INTO DACSO_Q006b_Weighted_New_Labour_Supply_0
FROM DACSO_Q006a_Weight_New_Labour_Supply
WHERE (((DACSO_Q006a_Weight_New_Labour_Supply.New_Labour_Supply)=0))
GROUP BY DACSO_Q006a_Weight_New_Labour_Supply.PSSM_Credential, DACSO_Q006a_Weight_New_Labour_Supply.PSSM_CRED, DACSO_Q006a_Weight_New_Labour_Supply.Current_Region_PSSM_Code_Rollup, DACSO_Q006a_Weight_New_Labour_Supply.Age_Group_Rollup, DACSO_Q006a_Weight_New_Labour_Supply.LCP4_CD, DACSO_Q006a_Weight_New_Labour_Supply.TTRAIN, DACSO_Q006a_Weight_New_Labour_Supply.LCIP4_CRED, DACSO_Q006a_Weight_New_Labour_Supply.LCIP2_CRED;"

DACSO_Q006b_Weighted_New_Labour_Supply_0_2D <- 
"SELECT DACSO_Q006a_Weight_New_Labour_Supply.PSSM_Credential, DACSO_Q006a_Weight_New_Labour_Supply.PSSM_CRED, DACSO_Q006a_Weight_New_Labour_Supply.Current_Region_PSSM_Code_Rollup, DACSO_Q006a_Weight_New_Labour_Supply.Age_Group_Rollup, Left(LCP4_CD,2) AS LCP2_CD, DACSO_Q006a_Weight_New_Labour_Supply.TTRAIN, DACSO_Q006a_Weight_New_Labour_Supply.LCIP2_CRED AS LCP2_CRED, Sum(DACSO_Q006a_Weight_New_Labour_Supply.Weighted) AS Count
INTO DACSO_Q006b_Weighted_New_Labour_Supply_0_2D
FROM DACSO_Q006a_Weight_New_Labour_Supply
WHERE (((DACSO_Q006a_Weight_New_Labour_Supply.New_Labour_Supply)=0))
GROUP BY DACSO_Q006a_Weight_New_Labour_Supply.PSSM_Credential, DACSO_Q006a_Weight_New_Labour_Supply.PSSM_CRED, DACSO_Q006a_Weight_New_Labour_Supply.Current_Region_PSSM_Code_Rollup, DACSO_Q006a_Weight_New_Labour_Supply.Age_Group_Rollup, Left(LCP4_CD,2), DACSO_Q006a_Weight_New_Labour_Supply.TTRAIN, DACSO_Q006a_Weight_New_Labour_Supply.LCIP2_CRED;"

DACSO_Q006b_Weighted_New_Labour_Supply_0_2D_No_TT <- "
SELECT  dacso_q006a_weight_new_labour_supply.pssm_credential,
        dacso_q006a_weight_new_labour_supply.pssm_cred,
        dacso_q006a_weight_new_labour_supply.current_region_pssm_code_rollup,
        dacso_q006a_weight_new_labour_supply.age_group_rollup,
        LEFT(lcp4_cd, 2) AS LCP2_CD,
        (CASE WHEN LEFT(pssm_cred, 1) = '1' OR LEFT(pssm_cred, 1) = '3' THEN LEFT(pssm_cred, 1) + ' - ' ELSE '' END)
        + LEFT(lcp4_cd, 2) + ' - ' + pssm_credential AS LCP2_CRED,
        Sum(dacso_q006a_weight_new_labour_supply.weighted) AS Count
INTO    dacso_q006b_weighted_new_labour_supply_0_2d_no_tt
FROM    dacso_q006a_weight_new_labour_supply
WHERE   dacso_q006a_weight_new_labour_supply.new_labour_supply = 0
GROUP BY dacso_q006a_weight_new_labour_supply.pssm_credential,
        dacso_q006a_weight_new_labour_supply.pssm_cred,
        dacso_q006a_weight_new_labour_supply.current_region_pssm_code_rollup,
        dacso_q006a_weight_new_labour_supply.age_group_rollup,
        LEFT(lcp4_cd, 2),
       (CASE WHEN LEFT(pssm_cred, 1) = '1' OR LEFT(pssm_cred, 1) = '3' THEN LEFT(pssm_cred, 1) + ' - ' ELSE '' END) + LEFT(lcp4_cd, 2) + ' - ' + pssm_credential;"

DACSO_Q006b_Weighted_New_Labour_Supply_0_No_TT <- "
SELECT  dacso_q006a_weight_new_labour_supply.pssm_credential,
        dacso_q006a_weight_new_labour_supply.pssm_cred,
        dacso_q006a_weight_new_labour_supply.current_region_pssm_code_rollup,
        dacso_q006a_weight_new_labour_supply.age_group_rollup,
        dacso_q006a_weight_new_labour_supply.lcp4_cd,
        (CASE WHEN grad_status IS NULL THEN NULL ELSE cast(grad_status as nvarchar(10)) + ' - ' END) + lcp4_cd + ' - ' + pssm_credential AS LCIP4_CRED,
        (CASE WHEN grad_status IS NULL THEN NULL ELSE cast(grad_status as nvarchar(10)) + ' - ' END) + LEFT(lcp4_cd, 2) + ' - ' + pssm_credential AS LCIP2_CRED,
        Sum(dacso_q006a_weight_new_labour_supply.weighted) AS Count
INTO   dacso_q006b_weighted_new_labour_supply_0_no_tt
FROM   dacso_q006a_weight_new_labour_supply
WHERE  dacso_q006a_weight_new_labour_supply.new_labour_supply = 0
GROUP  BY dacso_q006a_weight_new_labour_supply.pssm_credential,
  dacso_q006a_weight_new_labour_supply.pssm_cred,
  dacso_q006a_weight_new_labour_supply.current_region_pssm_code_rollup,
  dacso_q006a_weight_new_labour_supply.age_group_rollup,
  dacso_q006a_weight_new_labour_supply.lcp4_cd,
  (CASE WHEN grad_status IS NULL THEN NULL ELSE cast(grad_status as nvarchar(10)) + ' - ' END) + lcp4_cd + ' - ' + pssm_credential,
  (CASE WHEN grad_status IS NULL THEN NULL ELSE cast(grad_status as nvarchar(10)) + ' - ' END) + LEFT(lcp4_cd, 2) + ' - ' + pssm_credential;"

DACSO_Q006b_Weighted_New_Labour_Supply_2D <- 
"SELECT DACSO_Q006a_Weight_New_Labour_Supply.PSSM_Credential, DACSO_Q006a_Weight_New_Labour_Supply.PSSM_CRED, DACSO_Q006a_Weight_New_Labour_Supply.Current_Region_PSSM_Code_Rollup, DACSO_Q006a_Weight_New_Labour_Supply.Age_Group_Rollup, Left(LCP4_CD,2) AS LCP2_CD, DACSO_Q006a_Weight_New_Labour_Supply.TTRAIN, DACSO_Q006a_Weight_New_Labour_Supply.LCIP2_CRED AS LCP2_CRED, Sum(DACSO_Q006a_Weight_New_Labour_Supply.Weighted) AS Count
INTO DACSO_Q006b_Weighted_New_Labour_Supply_2D
FROM DACSO_Q006a_Weight_New_Labour_Supply
WHERE (((DACSO_Q006a_Weight_New_Labour_Supply.New_Labour_Supply)=1 Or (DACSO_Q006a_Weight_New_Labour_Supply.New_Labour_Supply)=2 Or (DACSO_Q006a_Weight_New_Labour_Supply.New_Labour_Supply)=3))
GROUP BY DACSO_Q006a_Weight_New_Labour_Supply.PSSM_Credential, DACSO_Q006a_Weight_New_Labour_Supply.PSSM_CRED, DACSO_Q006a_Weight_New_Labour_Supply.Current_Region_PSSM_Code_Rollup, DACSO_Q006a_Weight_New_Labour_Supply.Age_Group_Rollup, Left(LCP4_CD,2), DACSO_Q006a_Weight_New_Labour_Supply.TTRAIN, DACSO_Q006a_Weight_New_Labour_Supply.LCIP2_CRED;"

DACSO_Q006b_Weighted_New_Labour_Supply_2D_No_TT <- "
SELECT    dacso_q006a_weight_new_labour_supply.pssm_credential,
          dacso_q006a_weight_new_labour_supply.pssm_cred,
          dacso_q006a_weight_new_labour_supply.current_region_pssm_code_rollup,
          dacso_q006a_weight_new_labour_supply.age_group_rollup,
          LEFT(lcp4_cd, 2) AS LCP2_CD,
          (CASE WHEN LEFT(pssm_cred, 1) = '1'  OR LEFT(pssm_cred, 1) = '3' THEN LEFT(pssm_cred, 1) + ' - ' ELSE '' END) +
          LEFT(lcp4_cd, 2) + ' - ' + pssm_credential AS LCP2_CRED,
          Sum(dacso_q006a_weight_new_labour_supply.weighted) AS Count
INTO      dacso_q006b_weighted_new_labour_supply_2d_no_tt
FROM      dacso_q006a_weight_new_labour_supply
WHERE     (dacso_q006a_weight_new_labour_supply.new_labour_supply = 1)
    OR    (dacso_q006a_weight_new_labour_supply.new_labour_supply = 2)
    OR    (dacso_q006a_weight_new_labour_supply.new_labour_supply = 3)
GROUP  BY dacso_q006a_weight_new_labour_supply.pssm_credential,
          dacso_q006a_weight_new_labour_supply.pssm_cred,
          dacso_q006a_weight_new_labour_supply.current_region_pssm_code_rollup,
          dacso_q006a_weight_new_labour_supply.age_group_rollup,
          LEFT(lcp4_cd, 2),
          (CASE WHEN LEFT(pssm_cred, 1) = '1'  OR LEFT(pssm_cred, 1) = '3' THEN LEFT(pssm_cred, 1) + ' - ' ELSE '' END) +
          LEFT(lcp4_cd, 2) + ' - ' + pssm_credential; "

DACSO_Q006b_Weighted_New_Labour_Supply_No_TT <- 
"SELECT dacso_q006a_weight_new_labour_supply.pssm_credential,
       dacso_q006a_weight_new_labour_supply.pssm_cred,
       dacso_q006a_weight_new_labour_supply.current_region_pssm_code_rollup,
       dacso_q006a_weight_new_labour_supply.age_group_rollup,
       dacso_q006a_weight_new_labour_supply.lcp4_cd,
       (CASE WHEN grad_status IS NULL THEN NULL ELSE cast(grad_status as nvarchar(10)) + ' - ' END) 
       + lcp4_cd + ' - ' + pssm_credential                AS LCIP4_CRED,
     (CASE WHEN grad_status IS NULL THEN NULL ELSE cast(grad_status as nvarchar(10)) + ' - ' END) 
       + LEFT(lcp4_cd, 2) + ' - ' + pssm_credential       AS LCIP2_CRED,
       Sum(dacso_q006a_weight_new_labour_supply.weighted) AS Count,
       Sum(dacso_q006a_weight_new_labour_supply.count)    AS Unweighted_Count
INTO   dacso_q006b_weighted_new_labour_supply_no_tt
FROM   dacso_q006a_weight_new_labour_supply
WHERE  (( ( dacso_q006a_weight_new_labour_supply.new_labour_supply ) = 1
           OR ( dacso_q006a_weight_new_labour_supply.new_labour_supply ) = 2
           OR ( dacso_q006a_weight_new_labour_supply.new_labour_supply ) = 3 ))
GROUP  BY dacso_q006a_weight_new_labour_supply.pssm_credential,
          dacso_q006a_weight_new_labour_supply.pssm_cred,
          dacso_q006a_weight_new_labour_supply.current_region_pssm_code_rollup,
          dacso_q006a_weight_new_labour_supply.age_group_rollup,
          dacso_q006a_weight_new_labour_supply.lcp4_cd,
         (CASE WHEN grad_status IS NULL THEN NULL ELSE cast(grad_status as nvarchar(10)) + ' - ' END) 
          + lcp4_cd + ' - ' + pssm_credential,
          (CASE WHEN grad_status IS NULL THEN NULL ELSE cast(grad_status as nvarchar(10)) + ' - ' END) 
          + LEFT(lcp4_cd, 2) + ' - ' + pssm_credential
HAVING (( ( dacso_q006a_weight_new_labour_supply.age_group_rollup ) IS NOT NULL
        )); "

DACSO_Q006b_Weighted_New_Labour_Supply_Total <- 
"SELECT dacso_q006a_weight_new_labour_supply.pssm_credential,
       dacso_q006a_weight_new_labour_supply.pssm_cred,
       dacso_q006a_weight_new_labour_supply.age_group_rollup,
       dacso_q006a_weight_new_labour_supply.lcp4_cd,
       dacso_q006a_weight_new_labour_supply.ttrain,
       dacso_q006a_weight_new_labour_supply.lcip4_cred,
       dacso_q006a_weight_new_labour_supply.lcip2_cred,
       Sum(dacso_q006a_weight_new_labour_supply.weighted) AS Total
INTO   dacso_q006b_weighted_new_labour_supply_total
FROM   dacso_q006a_weight_new_labour_supply
WHERE  (( ( dacso_q006a_weight_new_labour_supply.new_labour_supply ) = 0
           OR ( dacso_q006a_weight_new_labour_supply.new_labour_supply ) = 1
           OR ( dacso_q006a_weight_new_labour_supply.new_labour_supply ) = 2
           OR ( dacso_q006a_weight_new_labour_supply.new_labour_supply ) = 3 ))
GROUP  BY dacso_q006a_weight_new_labour_supply.pssm_credential,
          dacso_q006a_weight_new_labour_supply.pssm_cred,
          dacso_q006a_weight_new_labour_supply.age_group_rollup,
          dacso_q006a_weight_new_labour_supply.lcp4_cd,
          dacso_q006a_weight_new_labour_supply.ttrain,
          dacso_q006a_weight_new_labour_supply.lcip4_cred,
          dacso_q006a_weight_new_labour_supply.lcip2_cred;"

DACSO_Q006b_Weighted_New_Labour_Supply_Total_2D <- 
"SELECT DACSO_Q006a_Weight_New_Labour_Supply.PSSM_Credential, DACSO_Q006a_Weight_New_Labour_Supply.PSSM_CRED, 
DACSO_Q006a_Weight_New_Labour_Supply.Age_Group_Rollup, Left(LCP4_CD,2) AS LCP2_CD, 
DACSO_Q006a_Weight_New_Labour_Supply.TTRAIN, DACSO_Q006a_Weight_New_Labour_Supply.LCIP2_CRED AS LCP2_CRED, 
Sum(DACSO_Q006a_Weight_New_Labour_Supply.Weighted) AS Total
INTO DACSO_Q006b_Weighted_New_Labour_Supply_Total_2D
FROM DACSO_Q006a_Weight_New_Labour_Supply
WHERE (((DACSO_Q006a_Weight_New_Labour_Supply.New_Labour_Supply)=0 Or (DACSO_Q006a_Weight_New_Labour_Supply.New_Labour_Supply)=1 Or (DACSO_Q006a_Weight_New_Labour_Supply.New_Labour_Supply)=2 Or (DACSO_Q006a_Weight_New_Labour_Supply.New_Labour_Supply)=3))
GROUP BY DACSO_Q006a_Weight_New_Labour_Supply.PSSM_Credential, DACSO_Q006a_Weight_New_Labour_Supply.PSSM_CRED, DACSO_Q006a_Weight_New_Labour_Supply.Age_Group_Rollup, Left(LCP4_CD,2), DACSO_Q006a_Weight_New_Labour_Supply.TTRAIN, DACSO_Q006a_Weight_New_Labour_Supply.LCIP2_CRED;"

DACSO_Q006b_Weighted_New_Labour_Supply_Total_2D_No_TT <- 
"SELECT DACSO_Q006a_Weight_New_Labour_Supply.PSSM_Credential, DACSO_Q006a_Weight_New_Labour_Supply.PSSM_CRED, 
DACSO_Q006a_Weight_New_Labour_Supply.Age_Group_Rollup, Left(LCP4_CD,2) AS LCP2_CD, IIf((Left(PSSM_CRED,1)='1' Or Left(PSSM_CRED,1)='3'),Left(PSSM_CRED,1) + ' - ','') + Left(LCP4_CD,2) + ' - ' + PSSM_Credential AS LCP2_CRED, Sum(DACSO_Q006a_Weight_New_Labour_Supply.Weighted) AS Total
INTO DACSO_Q006b_Weighted_New_Labour_Supply_Total_2D_No_TT
FROM DACSO_Q006a_Weight_New_Labour_Supply
WHERE (((DACSO_Q006a_Weight_New_Labour_Supply.New_Labour_Supply)=0 Or (DACSO_Q006a_Weight_New_Labour_Supply.New_Labour_Supply)=1 Or (DACSO_Q006a_Weight_New_Labour_Supply.New_Labour_Supply)=2 Or (DACSO_Q006a_Weight_New_Labour_Supply.New_Labour_Supply)=3))
GROUP BY DACSO_Q006a_Weight_New_Labour_Supply.PSSM_Credential, DACSO_Q006a_Weight_New_Labour_Supply.PSSM_CRED, DACSO_Q006a_Weight_New_Labour_Supply.Age_Group_Rollup, Left(LCP4_CD,2), IIf((Left(PSSM_CRED,1)='1' Or Left(PSSM_CRED,1)='3'),Left(PSSM_CRED,1) + ' - ','') + Left(LCP4_CD,2) + ' - ' + PSSM_Credential;"

DACSO_Q006b_Weighted_New_Labour_Supply_Total_No_TT <- 
"SELECT dacso_q006a_weight_new_labour_supply.pssm_credential,
       dacso_q006a_weight_new_labour_supply.pssm_cred,
       dacso_q006a_weight_new_labour_supply.age_group_rollup,
       dacso_q006a_weight_new_labour_supply.lcp4_cd,
       (CASE WHEN grad_status IS NULL THEN NULL ELSE cast(grad_status as nvarchar(10)) + ' - ' END) + lcp4_cd + ' - ' + pssm_credential  AS LCIP4_CRED,
       (CASE WHEN grad_status IS NULL THEN NULL ELSE cast(grad_status as nvarchar(10)) + ' - ' END) + LEFT(lcp4_cd, 2) + ' - ' + pssm_credential  AS LCIP2_CRED,
       Sum(dacso_q006a_weight_new_labour_supply.weighted) AS Total
INTO   dacso_q006b_weighted_new_labour_supply_total_no_tt
FROM   DACSO_Q006a_Weight_New_Labour_Supply
WHERE  dacso_q006a_weight_new_labour_supply.new_labour_supply = 0
           OR dacso_q006a_weight_new_labour_supply.new_labour_supply = 1
           OR dacso_q006a_weight_new_labour_supply.new_labour_supply = 2
           OR dacso_q006a_weight_new_labour_supply.new_labour_supply = 3
GROUP  BY dacso_q006a_weight_new_labour_supply.pssm_credential,
          dacso_q006a_weight_new_labour_supply.pssm_cred,
          dacso_q006a_weight_new_labour_supply.age_group_rollup,
          dacso_q006a_weight_new_labour_supply.lcp4_cd,
          (CASE WHEN grad_status IS NULL THEN NULL ELSE cast(grad_status as nvarchar(10)) + ' - ' END) + lcp4_cd + ' - ' + pssm_credential,
          (CASE WHEN grad_status IS NULL THEN NULL ELSE cast(grad_status as nvarchar(10)) + ' - ' END) + LEFT(lcp4_cd, 2) + ' - ' + pssm_credential;"

DACSO_Q007a_Weighted_New_Labour_Supply <- 
"SELECT DACSO_Q006b_Weighted_New_Labour_Supply.PSSM_Credential, DACSO_Q006b_Weighted_New_Labour_Supply.PSSM_CRED, 
DACSO_Q006b_Weighted_New_Labour_Supply.Current_Region_PSSM_Code_Rollup, DACSO_Q006b_Weighted_New_Labour_Supply.Age_Group_Rollup, 
DACSO_Q006b_Weighted_New_Labour_Supply.LCP4_CD, DACSO_Q006b_Weighted_New_Labour_Supply_Total.TTRAIN, 
DACSO_Q006b_Weighted_New_Labour_Supply_Total.LCIP4_CRED, DACSO_Q006b_Weighted_New_Labour_Supply_Total.LCIP2_CRED, 
DACSO_Q006b_Weighted_New_Labour_Supply.Count, DACSO_Q006b_Weighted_New_Labour_Supply_Total.Total, ISNULL(Count,0)/Total AS perc
INTO DACSO_Q007a_Weighted_New_Labour_Supply
FROM DACSO_Q006b_Weighted_New_Labour_Supply_Total 
LEFT JOIN DACSO_Q006b_Weighted_New_Labour_Supply ON (DACSO_Q006b_Weighted_New_Labour_Supply_Total.Age_Group_Rollup = DACSO_Q006b_Weighted_New_Labour_Supply.Age_Group_Rollup) AND (DACSO_Q006b_Weighted_New_Labour_Supply_Total.LCIP4_CRED = DACSO_Q006b_Weighted_New_Labour_Supply.LCIP4_CRED)
WHERE (((DACSO_Q006b_Weighted_New_Labour_Supply.Current_Region_PSSM_Code_Rollup) Is Not Null));"

DACSO_Q007a_Weighted_New_Labour_Supply_0 <- 
"SELECT DACSO_Q006b_Weighted_New_Labour_Supply_0.PSSM_Credential, DACSO_Q006b_Weighted_New_Labour_Supply_0.PSSM_CRED, 
DACSO_Q006b_Weighted_New_Labour_Supply_0.Current_Region_PSSM_Code_Rollup, DACSO_Q006b_Weighted_New_Labour_Supply_0.Age_Group_Rollup, 
DACSO_Q006b_Weighted_New_Labour_Supply_0.LCP4_CD, DACSO_Q006b_Weighted_New_Labour_Supply_Total.TTRAIN, DACSO_Q006b_Weighted_New_Labour_Supply_Total.LCIP4_CRED, 
DACSO_Q006b_Weighted_New_Labour_Supply_Total.LCIP2_CRED, DACSO_Q006b_Weighted_New_Labour_Supply_0.Count, DACSO_Q006b_Weighted_New_Labour_Supply_Total.Total, 
1-(ISNULL(Count,0)/Total) AS perc
INTO DACSO_Q007a_Weighted_New_Labour_Supply_0
FROM DACSO_Q006b_Weighted_New_Labour_Supply_Total LEFT JOIN DACSO_Q006b_Weighted_New_Labour_Supply_0 ON (DACSO_Q006b_Weighted_New_Labour_Supply_Total.Age_Group_Rollup = DACSO_Q006b_Weighted_New_Labour_Supply_0.Age_Group_Rollup) AND (DACSO_Q006b_Weighted_New_Labour_Supply_Total.LCIP4_CRED = DACSO_Q006b_Weighted_New_Labour_Supply_0.LCIP4_CRED)
WHERE (((DACSO_Q006b_Weighted_New_Labour_Supply_0.Count)>0) AND ((1-(ISNULL(Count,0)/Total))=0));"

DACSO_Q007a_Weighted_New_Labour_Supply_0_2D <- 
"SELECT DACSO_Q006b_Weighted_New_Labour_Supply_0_2D.PSSM_Credential, DACSO_Q006b_Weighted_New_Labour_Supply_0_2D.PSSM_CRED, 
DACSO_Q006b_Weighted_New_Labour_Supply_0_2D.Current_Region_PSSM_Code_Rollup, DACSO_Q006b_Weighted_New_Labour_Supply_0_2D.Age_Group_Rollup, 
DACSO_Q006b_Weighted_New_Labour_Supply_Total_2D.LCP2_CD, DACSO_Q006b_Weighted_New_Labour_Supply_Total_2D.TTRAIN, 
DACSO_Q006b_Weighted_New_Labour_Supply_Total_2D.LCP2_CRED, DACSO_Q006b_Weighted_New_Labour_Supply_0_2D.Count, 
DACSO_Q006b_Weighted_New_Labour_Supply_Total_2D.Total, 1-(ISNULL(Count,0)/Total) AS perc
INTO DACSO_Q007a_Weighted_New_Labour_Supply_0_2D
FROM DACSO_Q006b_Weighted_New_Labour_Supply_Total_2D LEFT JOIN DACSO_Q006b_Weighted_New_Labour_Supply_0_2D ON (DACSO_Q006b_Weighted_New_Labour_Supply_Total_2D.Age_Group_Rollup = DACSO_Q006b_Weighted_New_Labour_Supply_0_2D.Age_Group_Rollup) AND (DACSO_Q006b_Weighted_New_Labour_Supply_Total_2D.LCP2_CRED = DACSO_Q006b_Weighted_New_Labour_Supply_0_2D.LCP2_CRED)
WHERE (((DACSO_Q006b_Weighted_New_Labour_Supply_0_2D.Count)>0) AND ((1-(ISNULL(Count,0)/Total))=0));"

DACSO_Q007a_Weighted_New_Labour_Supply_0_2D_No_TT <- 
"SELECT DACSO_Q006b_Weighted_New_Labour_Supply_0_2D_No_TT.PSSM_Credential, DACSO_Q006b_Weighted_New_Labour_Supply_0_2D_No_TT.PSSM_CRED, 
DACSO_Q006b_Weighted_New_Labour_Supply_0_2D_No_TT.Current_Region_PSSM_Code_Rollup, DACSO_Q006b_Weighted_New_Labour_Supply_0_2D_No_TT.Age_Group_Rollup, 
DACSO_Q006b_Weighted_New_Labour_Supply_Total_2D_No_TT.LCP2_CD, DACSO_Q006b_Weighted_New_Labour_Supply_Total_2D_No_TT.LCP2_CRED, 
DACSO_Q006b_Weighted_New_Labour_Supply_0_2D_No_TT.Count, DACSO_Q006b_Weighted_New_Labour_Supply_Total_2D_No_TT.Total, 1-(ISNULL(Count,0)/Total) AS perc
INTO DACSO_Q007a_Weighted_New_Labour_Supply_0_2D_No_TT
FROM DACSO_Q006b_Weighted_New_Labour_Supply_Total_2D_No_TT LEFT JOIN DACSO_Q006b_Weighted_New_Labour_Supply_0_2D_No_TT ON (DACSO_Q006b_Weighted_New_Labour_Supply_Total_2D_No_TT.LCP2_CRED = DACSO_Q006b_Weighted_New_Labour_Supply_0_2D_No_TT.LCP2_CRED) AND (DACSO_Q006b_Weighted_New_Labour_Supply_Total_2D_No_TT.Age_Group_Rollup = DACSO_Q006b_Weighted_New_Labour_Supply_0_2D_No_TT.Age_Group_Rollup)
WHERE (((DACSO_Q006b_Weighted_New_Labour_Supply_0_2D_No_TT.Count)>0) AND ((1-(ISNULL(Count,0)/Total))=0));"

DACSO_Q007a_Weighted_New_Labour_Supply_0_No_TT <- 
"SELECT DACSO_Q006b_Weighted_New_Labour_Supply_0_No_TT.PSSM_Credential, DACSO_Q006b_Weighted_New_Labour_Supply_0_No_TT.PSSM_CRED, 
DACSO_Q006b_Weighted_New_Labour_Supply_0_No_TT.Current_Region_PSSM_Code_Rollup, DACSO_Q006b_Weighted_New_Labour_Supply_0_No_TT.Age_Group_Rollup, 
DACSO_Q006b_Weighted_New_Labour_Supply_0_No_TT.LCP4_CD, DACSO_Q006b_Weighted_New_Labour_Supply_Total_No_TT.LCIP4_CRED, 
DACSO_Q006b_Weighted_New_Labour_Supply_Total_No_TT.LCIP2_CRED, DACSO_Q006b_Weighted_New_Labour_Supply_0_No_TT.Count, 
DACSO_Q006b_Weighted_New_Labour_Supply_Total_No_TT.Total, 1-(ISNULL(Count,0)/Total) AS perc
INTO DACSO_Q007a_Weighted_New_Labour_Supply_0_No_TT
FROM DACSO_Q006b_Weighted_New_Labour_Supply_Total_No_TT LEFT JOIN DACSO_Q006b_Weighted_New_Labour_Supply_0_No_TT ON (DACSO_Q006b_Weighted_New_Labour_Supply_Total_No_TT.LCIP4_CRED = DACSO_Q006b_Weighted_New_Labour_Supply_0_No_TT.LCIP4_CRED) AND (DACSO_Q006b_Weighted_New_Labour_Supply_Total_No_TT.Age_Group_Rollup = DACSO_Q006b_Weighted_New_Labour_Supply_0_No_TT.Age_Group_Rollup)
WHERE (((DACSO_Q006b_Weighted_New_Labour_Supply_0_No_TT.Count)>0) AND ((1-(ISNULL(Count,0)/Total))=0));"

DACSO_Q007a_Weighted_New_Labour_Supply_2D <- 
"SELECT DACSO_Q006b_Weighted_New_Labour_Supply_2D.PSSM_Credential, DACSO_Q006b_Weighted_New_Labour_Supply_2D.PSSM_CRED, DACSO_Q006b_Weighted_New_Labour_Supply_2D.Current_Region_PSSM_Code_Rollup, DACSO_Q006b_Weighted_New_Labour_Supply_2D.Age_Group_Rollup, DACSO_Q006b_Weighted_New_Labour_Supply_2D.LCP2_CD, DACSO_Q006b_Weighted_New_Labour_Supply_Total_2D.TTRAIN, DACSO_Q006b_Weighted_New_Labour_Supply_2D.LCP2_CRED, DACSO_Q006b_Weighted_New_Labour_Supply_2D.Count, DACSO_Q006b_Weighted_New_Labour_Supply_Total_2D.Total, ISNULL(Count,0)/Total AS perc
INTO DACSO_Q007a_Weighted_New_Labour_Supply_2D
FROM DACSO_Q006b_Weighted_New_Labour_Supply_Total_2D LEFT JOIN DACSO_Q006b_Weighted_New_Labour_Supply_2D ON (DACSO_Q006b_Weighted_New_Labour_Supply_Total_2D.Age_Group_Rollup = DACSO_Q006b_Weighted_New_Labour_Supply_2D.Age_Group_Rollup) AND (DACSO_Q006b_Weighted_New_Labour_Supply_Total_2D.LCP2_CRED = DACSO_Q006b_Weighted_New_Labour_Supply_2D.LCP2_CRED)
WHERE (((DACSO_Q006b_Weighted_New_Labour_Supply_2D.Current_Region_PSSM_Code_Rollup) Is Not Null));"

DACSO_Q007a_Weighted_New_Labour_Supply_2D_No_TT <- 
"SELECT DACSO_Q006b_Weighted_New_Labour_Supply_2D_No_TT.PSSM_Credential, DACSO_Q006b_Weighted_New_Labour_Supply_2D_No_TT.PSSM_CRED, 
DACSO_Q006b_Weighted_New_Labour_Supply_2D_No_TT.Current_Region_PSSM_Code_Rollup, 
DACSO_Q006b_Weighted_New_Labour_Supply_2D_No_TT.Age_Group_Rollup,
DACSO_Q006b_Weighted_New_Labour_Supply_2D_No_TT.LCP2_CD, DACSO_Q006b_Weighted_New_Labour_Supply_2D_No_TT.LCP2_CRED, 
DACSO_Q006b_Weighted_New_Labour_Supply_2D_No_TT.Count, DACSO_Q006b_Weighted_New_Labour_Supply_Total_2D_No_TT.Total, ISNULL(Count,0)/Total AS perc
INTO DACSO_Q007a_Weighted_New_Labour_Supply_2D_No_TT
FROM DACSO_Q006b_Weighted_New_Labour_Supply_Total_2D_No_TT 
LEFT JOIN DACSO_Q006b_Weighted_New_Labour_Supply_2D_No_TT ON (DACSO_Q006b_Weighted_New_Labour_Supply_Total_2D_No_TT.LCP2_CRED = DACSO_Q006b_Weighted_New_Labour_Supply_2D_No_TT.LCP2_CRED) AND (DACSO_Q006b_Weighted_New_Labour_Supply_Total_2D_No_TT.Age_Group_Rollup = DACSO_Q006b_Weighted_New_Labour_Supply_2D_No_TT.Age_Group_Rollup)
WHERE (((DACSO_Q006b_Weighted_New_Labour_Supply_2D_No_TT.Current_Region_PSSM_Code_Rollup) Is Not Null));"

DACSO_Q007a_Weighted_New_Labour_Supply_No_TT <- 
"SELECT DACSO_Q006b_Weighted_New_Labour_Supply_No_TT.PSSM_Credential, 
DACSO_Q006b_Weighted_New_Labour_Supply_No_TT.PSSM_CRED, DACSO_Q006b_Weighted_New_Labour_Supply_No_TT.Current_Region_PSSM_Code_Rollup, 
DACSO_Q006b_Weighted_New_Labour_Supply_No_TT.Age_Group_Rollup, 
DACSO_Q006b_Weighted_New_Labour_Supply_No_TT.LCP4_CD, DACSO_Q006b_Weighted_New_Labour_Supply_Total_No_TT.LCIP4_CRED, 
DACSO_Q006b_Weighted_New_Labour_Supply_Total_No_TT.LCIP2_CRED, DACSO_Q006b_Weighted_New_Labour_Supply_No_TT.Count, 
DACSO_Q006b_Weighted_New_Labour_Supply_Total_No_TT.Total, ISNULL(Count,0)/Total AS perc
INTO DACSO_Q007a_Weighted_New_Labour_Supply_No_TT
FROM DACSO_Q006b_Weighted_New_Labour_Supply_Total_No_TT LEFT JOIN DACSO_Q006b_Weighted_New_Labour_Supply_No_TT ON (DACSO_Q006b_Weighted_New_Labour_Supply_Total_No_TT.LCIP4_CRED = DACSO_Q006b_Weighted_New_Labour_Supply_No_TT.LCIP4_CRED) AND (DACSO_Q006b_Weighted_New_Labour_Supply_Total_No_TT.Age_Group_Rollup = DACSO_Q006b_Weighted_New_Labour_Supply_No_TT.Age_Group_Rollup)
WHERE (((DACSO_Q006b_Weighted_New_Labour_Supply_No_TT.Current_Region_PSSM_Code_Rollup) Is Not Null));"



DACSO_Q007b0_Delete_New_Labour_Supply <- "
DELETE
FROM Labour_Supply_Distribution
WHERE (((Labour_Supply_Distribution.Survey)='Student Outcomes'));"

DACSO_Q007b0_Delete_New_Labour_Supply_No_TT <- "
DELETE 
FROM Labour_Supply_Distribution_No_TT
WHERE (((Labour_Supply_Distribution_No_TT.Survey)='Student Outcomes'));"

DACSO_Q007b0_Delete_New_Labour_Supply_No_TT_QI <- "
DELETE 
FROM Labour_Supply_Distribution_No_TT_QI
WHERE (((Labour_Supply_Distribution_No_TT_QI.Survey)='Student Outcomes' Or (Labour_Supply_Distribution_No_TT_QI.Survey)='PTIB'));"

DACSO_Q007b0_Delete_New_Labour_Supply_QI <- "
DELETE 
FROM Labour_Supply_Distribution_QI
WHERE (((Labour_Supply_Distribution_QI.Survey)='Student Outcomes' Or (Labour_Supply_Distribution_QI.Survey)='PTIB'));"

DACSO_Q007b1_Append_New_Labour_Supply <- "
INSERT INTO Labour_Supply_Distribution ( Survey, PSSM_Credential, PSSM_CRED, Current_Region_PSSM_Code_Rollup, Age_Group_Rollup, LCP4_CD, TTRAIN, LCIP4_CRED, LCIP2_CRED, Count, Total, New_Labour_Supply )
SELECT 'Student Outcomes' AS Survey, 
DACSO_Q007a_Weighted_New_Labour_Supply.PSSM_Credential, 
DACSO_Q007a_Weighted_New_Labour_Supply.PSSM_CRED, 
DACSO_Q007a_Weighted_New_Labour_Supply.Current_Region_PSSM_Code_Rollup, 
DACSO_Q007a_Weighted_New_Labour_Supply.Age_Group_Rollup, 
DACSO_Q007a_Weighted_New_Labour_Supply.LCP4_CD, 
DACSO_Q007a_Weighted_New_Labour_Supply.TTRAIN, 
DACSO_Q007a_Weighted_New_Labour_Supply.LCIP4_CRED, 
DACSO_Q007a_Weighted_New_Labour_Supply.LCIP2_CRED, 
DACSO_Q007a_Weighted_New_Labour_Supply.Count, 
DACSO_Q007a_Weighted_New_Labour_Supply.Total, 
DACSO_Q007a_Weighted_New_Labour_Supply.perc AS New_Labour_Supply
FROM DACSO_Q007a_Weighted_New_Labour_Supply;"

DACSO_Q007b1_Append_New_Labour_Supply_No_TT <- "
INSERT INTO Labour_Supply_Distribution_No_TT (survey, pssm_credential, pssm_cred, current_region_pssm_code_rollup, age_group_rollup, lcp4_cd, lcip4_cred, lcip2_cred, count, total, new_labour_supply)
SELECT 'Student Outcomes' AS Survey,
dacso_q007a_weighted_new_labour_supply_no_tt.pssm_credential,
dacso_q007a_weighted_new_labour_supply_no_tt.pssm_cred,
dacso_q007a_weighted_new_labour_supply_no_tt.current_region_pssm_code_rollup,
dacso_q007a_weighted_new_labour_supply_no_tt.age_group_rollup,
dacso_q007a_weighted_new_labour_supply_no_tt.lcp4_cd,
dacso_q007a_weighted_new_labour_supply_no_tt.lcip4_cred,
dacso_q007a_weighted_new_labour_supply_no_tt.lcip2_cred,
dacso_q007a_weighted_new_labour_supply_no_tt.count,
dacso_q007a_weighted_new_labour_supply_no_tt.total,
dacso_q007a_weighted_new_labour_supply_no_tt.perc AS New_Labour_Supply
FROM   dacso_q007a_weighted_new_labour_supply_no_tt"

DACSO_Q007b2_Append_New_Labour_Supply_0 <- "
INSERT INTO Labour_Supply_Distribution (survey,pssm_credential,pssm_cred,current_region_pssm_code_rollup,age_group_rollup,lcp4_cd,ttrain,lcip4_cred,lcip2_cred,count,total,new_labour_supply)
SELECT 'Student Outcomes' AS Survey,
       dacso_q007a_weighted_new_labour_supply_0.pssm_credential,
       dacso_q007a_weighted_new_labour_supply_0.pssm_cred,
       dacso_q007a_weighted_new_labour_supply_0.current_region_pssm_code_rollup,
       dacso_q007a_weighted_new_labour_supply_0.age_group_rollup,
       dacso_q007a_weighted_new_labour_supply_0.lcp4_cd,
       dacso_q007a_weighted_new_labour_supply_0.ttrain,
       dacso_q007a_weighted_new_labour_supply_0.lcip4_cred,
       dacso_q007a_weighted_new_labour_supply_0.lcip2_cred,
       dacso_q007a_weighted_new_labour_supply_0.count,
       dacso_q007a_weighted_new_labour_supply_0.total,
       dacso_q007a_weighted_new_labour_supply_0.perc AS New_Labour_Supply
FROM   dacso_q007a_weighted_new_labour_supply_0; "

DACSO_Q007b2_Append_New_Labour_Supply_0_No_TT <- "
INSERT INTO Labour_Supply_Distribution_No_TT(survey,pssm_credential,pssm_cred,current_region_pssm_code_rollup,age_group_rollup,lcp4_cd,lcip4_cred,lcip2_cred,count,total,new_labour_supply)
SELECT 'Student Outcomes' AS Survey,
       dacso_q007a_weighted_new_labour_supply_0_no_tt.pssm_credential,
       dacso_q007a_weighted_new_labour_supply_0_no_tt.pssm_cred,
dacso_q007a_weighted_new_labour_supply_0_no_tt.current_region_pssm_code_rollup,
dacso_q007a_weighted_new_labour_supply_0_no_tt.age_group_rollup,
dacso_q007a_weighted_new_labour_supply_0_no_tt.lcp4_cd,
dacso_q007a_weighted_new_labour_supply_0_no_tt.lcip4_cred,
dacso_q007a_weighted_new_labour_supply_0_no_tt.lcip2_cred,
dacso_q007a_weighted_new_labour_supply_0_no_tt.count,
dacso_q007a_weighted_new_labour_supply_0_no_tt.total,
dacso_q007a_weighted_new_labour_supply_0_no_tt.perc
FROM   dacso_q007a_weighted_new_labour_supply_0_no_tt;"

DACSO_Q007c0_Delete_New_Labour_Supply_2D <- 
"DELETE 
FROM Labour_Supply_Distribution_LCP2
WHERE (((Labour_Supply_Distribution_LCP2.Survey)='Student Outcomes'));"

DACSO_Q007c0_Delete_New_Labour_Supply_2D_No_TT <- 
"DELETE 
FROM Labour_Supply_Distribution_LCP2_No_TT
WHERE (((Labour_Supply_Distribution_LCP2_No_TT.Survey)='Student Outcomes'));"

DACSO_Q007c0_Delete_New_Labour_Supply_2D_No_TT_QI <- 
"DELETE 
FROM Labour_Supply_Distribution_LCP2_No_TT_QI
WHERE (((Labour_Supply_Distribution_LCP2_No_TT_QI.Survey)='Student Outcomes' Or (Labour_Supply_Distribution_LCP2_No_TT_QI.Survey)='PTIB'));"

DACSO_Q007c0_Delete_New_Labour_Supply_2D_QI <- 
"DELETE 
FROM Labour_Supply_Distribution_LCP2_QI
WHERE (((Labour_Supply_Distribution_LCP2_QI.Survey)='Student Outcomes' Or (Labour_Supply_Distribution_LCP2_QI.Survey)='PTIB'));"

DACSO_Q007c1_Append_New_Labour_Supply_2D <- "
INSERT INTO Labour_Supply_Distribution_LCP2 ( Survey, PSSM_Credential, PSSM_CRED, Current_Region_PSSM_Code_Rollup, Age_Group_Rollup, LCP2_CD, TTRAIN, LCP2_CRED, Count, Total, New_Labour_Supply )
SELECT 'Student Outcomes' AS Survey, DACSO_Q007a_Weighted_New_Labour_Supply_2D.PSSM_Credential, DACSO_Q007a_Weighted_New_Labour_Supply_2D.PSSM_CRED, 
DACSO_Q007a_Weighted_New_Labour_Supply_2D.Current_Region_PSSM_Code_Rollup, DACSO_Q007a_Weighted_New_Labour_Supply_2D.Age_Group_Rollup, 
DACSO_Q007a_Weighted_New_Labour_Supply_2D.LCP2_CD, DACSO_Q007a_Weighted_New_Labour_Supply_2D.TTRAIN, DACSO_Q007a_Weighted_New_Labour_Supply_2D.LCP2_CRED, 
DACSO_Q007a_Weighted_New_Labour_Supply_2D.Count, DACSO_Q007a_Weighted_New_Labour_Supply_2D.Total, DACSO_Q007a_Weighted_New_Labour_Supply_2D.perc AS New_Labour_Supply
FROM DACSO_Q007a_Weighted_New_Labour_Supply_2D;"

DACSO_Q007c1_Append_New_Labour_Supply_2D_No_TT <- "
INSERT INTO Labour_Supply_Distribution_LCP2_No_TT ( Survey, PSSM_Credential, PSSM_CRED, Current_Region_PSSM_Code_Rollup, Age_Group_Rollup, LCP2_CD, LCP2_CRED, Count, Total, New_Labour_Supply )
SELECT 'Student Outcomes' AS Survey, DACSO_Q007a_Weighted_New_Labour_Supply_2D_No_TT.PSSM_Credential, DACSO_Q007a_Weighted_New_Labour_Supply_2D_No_TT.PSSM_CRED, 
DACSO_Q007a_Weighted_New_Labour_Supply_2D_No_TT.Current_Region_PSSM_Code_Rollup, DACSO_Q007a_Weighted_New_Labour_Supply_2D_No_TT.Age_Group_Rollup, 
DACSO_Q007a_Weighted_New_Labour_Supply_2D_No_TT.LCP2_CD, DACSO_Q007a_Weighted_New_Labour_Supply_2D_No_TT.LCP2_CRED, 
DACSO_Q007a_Weighted_New_Labour_Supply_2D_No_TT.Count, DACSO_Q007a_Weighted_New_Labour_Supply_2D_No_TT.Total, 
DACSO_Q007a_Weighted_New_Labour_Supply_2D_No_TT.perc AS New_Labour_Supply
FROM DACSO_Q007a_Weighted_New_Labour_Supply_2D_No_TT;"

DACSO_Q007c2_Append_New_Labour_Supply_0_2D <- "
INSERT INTO Labour_Supply_Distribution_LCP2 ( Survey, PSSM_Credential, PSSM_CRED, Current_Region_PSSM_Code_Rollup, Age_Group_Rollup, LCP2_CD, TTRAIN, LCP2_CRED, Count, Total, New_Labour_Supply )
SELECT 'Student Outcomes' AS Survey, DACSO_Q007a_Weighted_New_Labour_Supply_0_2D.PSSM_Credential, DACSO_Q007a_Weighted_New_Labour_Supply_0_2D.PSSM_CRED, 
DACSO_Q007a_Weighted_New_Labour_Supply_0_2D.Current_Region_PSSM_Code_Rollup, DACSO_Q007a_Weighted_New_Labour_Supply_0_2D.Age_Group_Rollup, 
DACSO_Q007a_Weighted_New_Labour_Supply_0_2D.LCP2_CD, DACSO_Q007a_Weighted_New_Labour_Supply_0_2D.TTRAIN, DACSO_Q007a_Weighted_New_Labour_Supply_0_2D.LCP2_CRED, 
DACSO_Q007a_Weighted_New_Labour_Supply_0_2D.Count, DACSO_Q007a_Weighted_New_Labour_Supply_0_2D.Total, DACSO_Q007a_Weighted_New_Labour_Supply_0_2D.perc AS New_Labour_Supply
FROM DACSO_Q007a_Weighted_New_Labour_Supply_0_2D;"

DACSO_Q007c2_Append_New_Labour_Supply_0_2D_No_TT <- "
INSERT INTO Labour_Supply_Distribution_LCP2_No_TT ( Survey, PSSM_Credential, PSSM_CRED, Current_Region_PSSM_Code_Rollup, Age_Group_Rollup, LCP2_CD, LCP2_CRED, Count, Total, New_Labour_Supply )
SELECT 'Student Outcomes' AS Survey, DACSO_Q007a_Weighted_New_Labour_Supply_0_2D_No_TT.PSSM_Credential, 
DACSO_Q007a_Weighted_New_Labour_Supply_0_2D_No_TT.PSSM_CRED, DACSO_Q007a_Weighted_New_Labour_Supply_0_2D_No_TT.Current_Region_PSSM_Code_Rollup, 
DACSO_Q007a_Weighted_New_Labour_Supply_0_2D_No_TT.Age_Group_Rollup, DACSO_Q007a_Weighted_New_Labour_Supply_0_2D_No_TT.LCP2_CD, 
DACSO_Q007a_Weighted_New_Labour_Supply_0_2D_No_TT.LCP2_CRED, DACSO_Q007a_Weighted_New_Labour_Supply_0_2D_No_TT.Count,
DACSO_Q007a_Weighted_New_Labour_Supply_0_2D_No_TT.Total, DACSO_Q007a_Weighted_New_Labour_Supply_0_2D_No_TT.perc AS New_Labour_Supply
FROM DACSO_Q007a_Weighted_New_Labour_Supply_0_2D_No_TT;"
