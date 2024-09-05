Q000_TRD_Graduates_Projection_Input <- 
"SELECT T_TRD_Data.PSSM_Credential, T_TRD_Data.PSSM_Credential AS PSSM_CRED, tbl_Age_Groups.Age_Group_Label, T_Year_Survey_Year.Award_School_Year, Count(*) AS [Count]
FROM (T_TRD_Data INNER JOIN T_Year_Survey_Year ON T_TRD_Data.SUBM_CD = T_Year_Survey_Year.SUBM_CD) INNER JOIN (tbl_Age INNER JOIN tbl_Age_Groups ON tbl_Age.Age_Group = tbl_Age_Groups.Age_Group) ON T_TRD_Data.TRD_AGE_AT_SURVEY = tbl_Age.Age
WHERE (((T_Year_Survey_Year.Survey)='TRD'))
GROUP BY T_TRD_Data.PSSM_Credential, tbl_Age_Groups.Age_Group_Label, T_Year_Survey_Year.Award_School_Year, T_TRD_Data.PSSM_Credential
ORDER BY tbl_Age_Groups.Age_Group_Label, T_Year_Survey_Year.Award_School_Year;"

Q000_TRD_Program_Projection_Input <- 
"SELECT tbl_Age_Groups.Age_Group_Label, T_TRD_Data.PSSM_Credential, [PSSM_Credential] + [Age_Group_Label] AS Expr1, T_TRD_Data.LCIP_LCP4_CD, Left([T_TRD_DATA].[LCIP_LCP4_CD],2) AS LCIP_CP2_CD, T_Year_Survey_Year.Award_School_Year, Count(*) AS [Count]
FROM (T_TRD_Data INNER JOIN (tbl_Age INNER JOIN tbl_Age_Groups ON tbl_Age.Age_Group = tbl_Age_Groups.Age_Group) ON T_TRD_Data.TRD_AGE_AT_SURVEY = tbl_Age.Age) INNER JOIN T_Year_Survey_Year ON T_TRD_Data.SUBM_CD = T_Year_Survey_Year.SUBM_CD
WHERE (((T_Year_Survey_Year.Survey)='TRD'))
GROUP BY tbl_Age_Groups.Age_Group_Label, T_TRD_Data.PSSM_Credential, [PSSM_Credential] + [Age_Group_Label], T_TRD_Data.LCIP_LCP4_CD, Left([T_TRD_DATA].[LCIP_LCP4_CD],2), T_Year_Survey_Year.Award_School_Year
ORDER BY tbl_Age_Groups.Age_Group_Label, T_Year_Survey_Year.Award_School_Year;"

Q000_TRD_Q003b_Add_CURRENT_REGION_PSSM <- 
"ALTER TABLE T_TRD_Data
ADD CURRENT_REGION_PSSM_CODE INT;"

Q000_TRD_Q003b_Add_CURRENT_REGION_PSSM2 <- "
UPDATE t_trd_data
SET    t_trd_data.current_region_pssm_code = trd_current_region_data.[current_region_pssm_code]
FROM t_trd_data
INNER JOIN trd_current_region_data
ON t_trd_data.[KEY] = trd_current_region_data.[KEY];"

Q000_TRD_Q003c_Derived_And_Weights <- "
UPDATE t_trd_data
SET    t_trd_data.new_labour_supply =
       CASE 
          WHEN TRD_LABR_EMPLOYED = 1 THEN 1 
          WHEN TRD_LABR_IN_LABOUR_MARKET = 1 And TRD_LABR_EMPLOYED = 0 THEN 1
          WHEN TRD_LABR_EMPLOYED = 0 THEN 0
          WHEN RESPONDENT = '1' THEN 0
          ELSE 0 
      END, 
      T_TRD_Data.Weight = T_Weights.Weight,
      T_TRD_Data.age_group = tbl_age.age_group,
      T_TRD_Data.age_group_rollup = tbl_age_groups.age_group_rollup
FROM ((t_trd_data
INNER JOIN t_weights
  ON t_trd_data.subm_cd = t_weights.subm_cd)
LEFT JOIN tbl_age
  ON t_trd_data.trd_age_at_survey = tbl_age.age)
LEFT JOIN tbl_age_groups
  ON tbl_age.age_group = tbl_age_groups.age_group
WHERE  t_weights.model = '2022-2023'
  AND t_weights.survey = 'TRD';"

Q000_TRD_Q003c_Derived_And_Weights_QI <- "
UPDATE t_trd_data
SET    t_trd_data.new_labour_supply =
       CASE 
          WHEN TRD_LABR_EMPLOYED = 1 THEN 1 
          WHEN TRD_LABR_IN_LABOUR_MARKET = 1 And TRD_LABR_EMPLOYED = 0 THEN 1
          WHEN TRD_LABR_EMPLOYED = 0 THEN 0
          WHEN RESPONDENT = '1' THEN 0
          ELSE 0 
      END, 
      T_TRD_Data.Weight = T_Weights.Weight_QI,
      T_TRD_Data.age_group = tbl_age.age_group,
      T_TRD_Data.age_group_rollup = tbl_age_groups.age_group_rollup
FROM ((t_trd_data
INNER JOIN t_weights
  ON t_trd_data.subm_cd = t_weights.subm_cd)
LEFT JOIN tbl_age
  ON t_trd_data.trd_age_at_survey = tbl_age.age)
LEFT JOIN tbl_age_groups
  ON tbl_age.age_group = tbl_age_groups.age_group
WHERE  t_weights.model = '2022-2023'
  AND t_weights.survey = 'TRD';"

Q000_TRD_Q005_1b1_Delete_Cohort <- "
DELETE  T_Cohorts_Recoded
FROM T_Cohorts_Recoded
WHERE T_Cohorts_Recoded.Survey='TRD';"

Q000_TRD_Q005_DACSO_DATA_Part_1b2_Cohort_Recoded <- "
INSERT INTO t_cohorts_recoded
            (pen,
             stqu_id,
             survey,
             survey_year,
             inst_cd,
             lcip_cd,
             lcp4_cd,
             ttrain,
             noc_cd,
             age_at_survey,
             age_group,
             age_group_rollup,
             grad_status,
             respondent,
             new_labour_supply,
             weight,
             pssm_credential,
             pssm_cred,
             lcip4_cred,
             lcip2_cred,
             current_region_pssm_code)
SELECT t_trd_data.pen,
       'TRD - ' + cast([key] as nvarchar(255)) AS stqu_id,
       t_year_survey_year.survey,
       t_year_survey_year.survey_year,
       t_trd_data.inst,
       t_trd_data.lcip_cd,
       t_trd_data.lcip_lcp4_cd,
       CASE WHEN ttrain = 2 THEN 1 ELSE ttrain END AS ttrain,
       CASE WHEN t_trd_data.noc_cd = 'XXXX' THEN '9999' ELSE t_trd_data.noc_cd END AS NOC_CD,
       t_trd_data.trd_age_at_survey,
       tbl_age_groups.age_group,
       tbl_age_groups.age_group_rollup,
       t_trd_data.gradstat_group,
       t_trd_data.respondent,
       t_trd_data.new_labour_supply,
       t_trd_data.weight,
       t_trd_data.pssm_credential,
       gradstat_group + ' - ' + pssm_credential  AS PSSM_CRED,
       gradstat_group + ' - ' + lcip_lcp4_cd + ' - ' +
       CASE WHEN ttrain = 2 THEN '1' ELSE cast(ttrain as nvarchar(10)) END + ' - ' + pssm_credential AS lcip4_cred,
       gradstat_group + ' - ' + LEFT(lcip_lcp4_cd, 2) + ' - ' +
       CASE WHEN ttrain = 2 THEN '1' ELSE cast(ttrain as nvarchar(10)) END + ' - ' + pssm_credential AS lcip2_cred,
       t_trd_data.current_region_pssm_code
FROM   t_trd_data
INNER JOIN t_year_survey_year
  ON t_trd_data.subm_cd = t_year_survey_year.subm_cd
  LEFT JOIN (tbl_Age  INNER JOIN tbl_Age_Groups  ON tbl_Age.Age_Group = tbl_Age_Groups.Age_Group)
	        ON tbl_Age.Age =  t_trd_data.TRD_AGE_AT_SURVEY
WHERE  t_year_survey_year.survey = 'TRD';"

Q000_TRD_Q99A_ENDDT <- 
"UPDATE infoware_trades_cohort_info 
INNER JOIN (t_cohorts_recoded 
INNER JOIN 000_trd_q99a_stqui_id 
ON t_cohorts_recoded.stqu_id = [000_TRD_Q99A_STQUI_ID].stqu_id) 
ON infoware_trades_cohort_info.KEY = [000_TRD_Q99A_STQUI_ID].KEY 
SET t_cohorts_recoded.enddt = LEFT([INFOWARE_TRADES_COHORT_INFO].[ENDDT],4) + '-' + RIGHT([INFOWARE_TRADES_COHORT_INFO].[ENDDT],2)
WHERE (((t_cohorts_recoded.survey)='TRD'));"

Q000_TRD_Q99A_ENDDT_IMPUTED <- 
"UPDATE T_Cohorts_Recoded SET T_Cohorts_Recoded.ENDDT = ([Survey_year]-2) + '-12'
WHERE (((T_Cohorts_Recoded.ENDDT) Is Null) AND ((T_Cohorts_Recoded.Survey)='TRD'));"

Q000_TRD_Q99A_STQUI_ID <- 
"SELECT INFOWARE_TRADES_COHORT_INFO.KEY, 'TRD - ' + [KEY] AS STQU_ID
FROM INFOWARE_TRADES_COHORT_INFO;"