APPSO_Graduates_Projection_Input <- 
"SELECT t_appso_data_final.pssm_credential,
       t_appso_data_final.pssm_credential AS PSSM_CRED,
       tbl_age_groups.age_group_label,
       t_year_survey_year.award_school_year,
       Count(*)                           AS Count
FROM   ((t_appso_data_final
         INNER JOIN tbl_age
                 ON t_appso_data_final.app_age_at_survey = tbl_age.age)
        INNER JOIN tbl_age_groups
                ON tbl_age.age_group = tbl_age_groups.age_group)
       INNER JOIN t_year_survey_year
               ON t_appso_data_final.subm_cd = t_year_survey_year.subm_cd
WHERE  (( ( t_year_survey_year.survey ) = 'APPSO' ))
GROUP  BY t_appso_data_final.pssm_credential,
          tbl_age_groups.age_group_label,
          t_year_survey_year.award_school_year,
          t_appso_data_final.pssm_credential
ORDER  BY tbl_age_groups.age_group_label,
          t_year_survey_year.award_school_year;"

APPSO_Program_Projection_Input <- 
"SELECT tbl_age_groups.age_group_label,
       t_appso_data_final.pssm_credential,
       pssm_credential & age_group_label        AS Expr1,
       t_appso_data_final.lcip_lcp4_cd,
       LEFT(t_appso_data_final.lcip_lcp4_cd, 2) AS LCIP_CP2_CD,
       t_year_survey_year.award_school_year,
       Count(*)                                     AS Count
FROM   ((t_appso_data_final
         INNER JOIN tbl_age
                 ON t_appso_data_final.app_age_at_survey = tbl_age.age)
        INNER JOIN tbl_age_groups
                ON tbl_age.age_group = tbl_age_groups.age_group)
       INNER JOIN t_year_survey_year
               ON t_appso_data_final.subm_cd = t_year_survey_year.subm_cd
WHERE  (( ( t_year_survey_year.survey ) = 'APPSO' ))
GROUP  BY tbl_age_groups.age_group_label,
          t_appso_data_final.pssm_credential,
          pssm_credential & age_group_label,
          t_appso_data_final.lcip_lcp4_cd,
          LEFT(t_appso_data_final.lcip_lcp4_cd, 2),
          t_year_survey_year.award_school_year
ORDER  BY tbl_age_groups.age_group_label,
          t_year_survey_year.award_school_year;"

APPSO_Q003b_Add_CURRENT_REGION_PSSM <- 
"ALTER TABLE T_APPSO_Data_Final
ADD CURRENT_REGION_PSSM_CODE INT NULL;"

APPSO_Q003b_Add_CURRENT_REGION_PSSM2 <- "
UPDATE t_appso_data_final
SET    t_appso_data_final.current_region_pssm_code = appso_current_region_data.current_region_pssm_code
FROM   t_appso_data_final
INNER JOIN appso_current_region_data
ON     t_appso_data_final.[KEY] = appso_current_region_data.[KEY];"

APPSO_Q003c_Derived_And_Weights <- "
UPDATE t_appso_data_final
SET   new_labour_supply = 
      CASE 
          WHEN APP_LABR_EMPLOYED = 1 THEN 1 
          WHEN APP_LABR_IN_LABOUR_MARKET = 1 And APP_LABR_EMPLOYED = 0 THEN 1
          WHEN APP_LABR_EMPLOYED = 0 THEN 0
          WHEN RESPONDENT = '1' THEN 0
          ELSE 0 
      END,
      weight = t_weights.weight,
      t_appso_data_final.age_group = tbl_age.age_group,
      t_appso_data_final.age_group_rollup = tbl_age_groups.age_group_rollup
FROM ((t_appso_data_final
INNER JOIN t_weights
  ON t_appso_data_final.subm_cd = t_weights.subm_cd)
LEFT JOIN tbl_age
  ON t_appso_data_final.app_age_at_survey = tbl_age.age)
LEFT JOIN tbl_age_groups
  ON tbl_age.age_group = tbl_age_groups.age_group
WHERE t_weights.model = '2022-2023'
  AND t_weights.survey = 'APPSO';"

APPSO_Q005_1b1_Delete_Cohort <- 
"DELETE T_Cohorts_Recoded
FROM T_Cohorts_Recoded
WHERE T_Cohorts_Recoded.Survey = 'APPSO';"

APPSO_Q005_DACSO_DATA_Part_1b2_Cohort_Recoded <- 
"INSERT INTO t_cohorts_recoded
            (pen,
             stqu_id,
             survey,
             survey_year,
             inst_cd,
             lcip_cd,
             lcp4_cd,
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
SELECT t_appso_data_final.pen,
       'APPSO - ' + cast(cast([key] AS INTEGER) AS NVARCHAR(255)) AS stqu_id,
       t_year_survey_year.survey,
       t_year_survey_year.survey_year,
       t_appso_data_final.inst,
       t_appso_data_final.lcip_cd,
       t_appso_data_final.lcip_lcp4_cd as lcp4_cd,
       CASE WHEN t_appso_data_final.noc_cd = 'xxxx' THEN '9999' ELSE t_appso_data_final.noc_cd END AS NOC_CD,
       t_appso_data_final.app_age_at_survey,
       tbl_Age_Groups.age_group,
       tbl_Age_Groups.age_group_rollup,
       '1' AS grad_status,
       t_appso_data_final.respondent,
       t_appso_data_final.new_labour_supply,
       t_appso_data_final.weight,
       t_appso_data_final.pssm_credential,
       t_appso_data_final.pssm_credential,
       t_appso_data_final.lcip4_cred,
       LEFT(lcip_lcp4_cd, 2) + ' - ' + pssm_credential AS LCIP2_CRED,
       t_appso_data_final.current_region_pssm_code
FROM   t_appso_data_final
       INNER JOIN t_year_survey_year
               ON t_appso_data_final.subm_cd = t_year_survey_year.subm_cd
       LEFT JOIN (tbl_Age  INNER JOIN tbl_Age_Groups  ON tbl_Age.Age_Group = tbl_Age_Groups.Age_Group)
	        ON tbl_Age.Age =  t_appso_data_final.APP_AGE_AT_SURVEY
WHERE  t_year_survey_year.survey = 'appso';"




APPSO_Q99A_ENDDT <- 
"UPDATE (INFOWARE_APPRENTICE_COHORT_INFO INNER JOIN APPSO_Q99A_STQUI_ID ON INFOWARE_APPRENTICE_COHORT_INFO.KEY = APPSO_Q99A_STQUI_ID.KEY) 
INNER JOIN T_Cohorts_Recoded 
ON APPSO_Q99A_STQUI_ID.STQU_ID = T_Cohorts_Recoded.STQU_ID 
SET T_Cohorts_Recoded.ENDDT = Left(INFOWARE_APPRENTICE_COHORT_INFO.ENDDT,4) + '-' & Right(INFOWARE_APPRENTICE_COHORT_INFO.ENDDT,2)
WHERE (((T_Cohorts_Recoded.Survey)='APPSO'));"

APPSO_Q99A_ENDDT_IMPUTED <- 
"UPDATE T_Cohorts_Recoded SET T_Cohorts_Recoded.ENDDT = (Survey_year-1) & '-12'
WHERE (((T_Cohorts_Recoded.ENDDT) Is Null) AND ((T_Cohorts_Recoded.Survey)='APPSO'));"

APPSO_Q99A_STQUI_ID <- 
"SELECT INFOWARE_APPRENTICE_COHORT_INFO.KEY, 'APPSO - ' + KEY AS STQU_ID
FROM INFOWARE_APPRENTICE_COHORT_INFO;"