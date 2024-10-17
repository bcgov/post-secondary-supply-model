# ---- DACSO_Q008_Z01_Base_OCC ----
DACSO_Q008_Z01_Base_OCC <- "
SELECT t_cohorts_recoded.survey,
       t_cohorts_recoded.inst_cd,
       t_cohorts_recoded.age_group_rollup,
       t_cohorts_recoded.ttrain,
       t_cohorts_recoded.lcip4_cred,
       Count(*) AS Base,
       t_cohorts_recoded.stqu_id,
       t_cohorts_recoded.new_labour_supply
INTO DACSO_Q008_Z01_Base_OCC
FROM   t_cohorts_recoded
WHERE  ( ( ( t_cohorts_recoded.weight ) > 0 )
         AND ( ( t_cohorts_recoded.current_region_pssm_code ) <>- 1 ) )
GROUP  BY t_cohorts_recoded.survey,
          t_cohorts_recoded.inst_cd,
          t_cohorts_recoded.age_group_rollup,
          t_cohorts_recoded.ttrain,
          t_cohorts_recoded.lcip4_cred,
          t_cohorts_recoded.stqu_id,
          t_cohorts_recoded.new_labour_supply,
          t_cohorts_recoded.grad_status
HAVING ( ( ( t_cohorts_recoded.age_group_rollup ) IS NOT NULL )
         AND ( ( t_cohorts_recoded.new_labour_supply ) = 1
                OR ( t_cohorts_recoded.new_labour_supply ) = 2
                OR ( t_cohorts_recoded.new_labour_supply ) = 3 )
         AND ( ( t_cohorts_recoded.grad_status ) = '1'
                OR ( t_cohorts_recoded.grad_status ) = '3' ) ); "



# ---- DACSO_Q008_Z02a_Base ----
DACSO_Q008_Z02a_Base <- 
"
SELECT t_cohorts_recoded.survey,
t_current_region_pssm_rollup_codes.current_region_pssm_code_rollup,
t_cohorts_recoded.survey_year,
t_cohorts_recoded.inst_cd,
t_cohorts_recoded.age_group_rollup,
t_cohorts_recoded.grad_status,
t_cohorts_recoded.ttrain,
t_cohorts_recoded.lcip4_cred,
Count(*)              AS Count,
t_cohorts_recoded.weight_nls,
Count(*) * weight_nls AS Base
INTO DACSO_Q008_Z02a_Base
FROM   t_cohorts_recoded
INNER JOIN (t_current_region_pssm_codes
            INNER JOIN t_current_region_pssm_rollup_codes
            ON
            t_current_region_pssm_codes.current_region_pssm_code_rollup =
              t_current_region_pssm_rollup_codes.current_region_pssm_code_rollup)
ON t_cohorts_recoded.current_region_pssm_code =
  t_current_region_pssm_codes.current_region_pssm_code
WHERE  ( ( ( t_cohorts_recoded.new_labour_supply ) = 1
           OR ( t_cohorts_recoded.new_labour_supply ) = 2
           OR ( t_cohorts_recoded.new_labour_supply ) = 3 )
         AND ( ( t_cohorts_recoded.respondent ) = '1' )
         AND ( ( t_cohorts_recoded.weight ) > 0 ) )
GROUP  BY t_cohorts_recoded.survey,
t_current_region_pssm_rollup_codes.current_region_pssm_code_rollup,
t_cohorts_recoded.survey_year,
t_cohorts_recoded.inst_cd,
t_cohorts_recoded.age_group_rollup,
t_cohorts_recoded.grad_status,
t_cohorts_recoded.ttrain,
t_cohorts_recoded.lcip4_cred,
t_cohorts_recoded.weight_nls
HAVING
(
  ( ( t_current_region_pssm_rollup_codes.current_region_pssm_code_rollup ) <> 9999 )
  AND ( ( t_cohorts_recoded.age_group_rollup ) IS NOT NULL )
  AND ( ( t_cohorts_recoded.grad_status ) = '1'
        OR ( t_cohorts_recoded.grad_status ) = '3' ) );"

  DACSO_Q008_Z02b_Respondents <- 
  "
  SELECT t_cohorts_recoded.survey,
  t_current_region_pssm_rollup_codes.current_region_pssm_code_rollup,
  t_cohorts_recoded.survey_year,
  t_cohorts_recoded.inst_cd,
  t_cohorts_recoded.age_group_rollup,
  t_cohorts_recoded.grad_status,
  t_cohorts_recoded.ttrain,
  t_cohorts_recoded.lcip4_cred,
  Sum(CASE
      WHEN respondent = '1'
      AND t_cohorts_recoded.current_region_pssm_code <>- 1 THEN 1
      ELSE 0
      END) AS Respondents
  INTO DACSO_Q008_Z02b_Respondents 
  FROM   t_cohorts_recoded
  INNER JOIN (t_current_region_pssm_codes
              INNER JOIN t_current_region_pssm_rollup_codes
              ON
              t_current_region_pssm_codes.current_region_pssm_code_rollup =
                t_current_region_pssm_rollup_codes.current_region_pssm_code_rollup)
  ON t_cohorts_recoded.current_region_pssm_code =
    t_current_region_pssm_codes.current_region_pssm_code
  WHERE  ( ( ( t_cohorts_recoded.noc_cd ) IS NOT NULL
             AND ( t_cohorts_recoded.noc_cd ) <> '99999' )
           AND ( ( t_cohorts_recoded.new_labour_supply ) = 1
                 OR ( t_cohorts_recoded.new_labour_supply ) = 3 )
           AND ( ( t_cohorts_recoded.weight ) > 0 ) )
  GROUP  BY t_cohorts_recoded.survey,
  t_current_region_pssm_rollup_codes.current_region_pssm_code_rollup,
  t_cohorts_recoded.survey_year,
  t_cohorts_recoded.inst_cd,
  t_cohorts_recoded.age_group_rollup,
  t_cohorts_recoded.grad_status,
  t_cohorts_recoded.ttrain,
  t_cohorts_recoded.lcip4_cred
  HAVING ( ( ( t_cohorts_recoded.age_group_rollup ) IS NOT NULL )
           AND ( ( t_cohorts_recoded.grad_status ) = '1'
                 OR ( t_cohorts_recoded.grad_status ) = '3' ) );"
  


# ---- DACSO_Q008_Z02b_Respondents_NOC_99999 ----
DACSO_Q008_Z02b_Respondents_NOC_99999 <- 
"
SELECT t_cohorts_recoded.survey,
t_current_region_pssm_rollup_codes.current_region_pssm_code_rollup,
t_cohorts_recoded.survey_year,
t_cohorts_recoded.inst_cd,
t_cohorts_recoded.age_group_rollup,
t_cohorts_recoded.grad_status,
t_cohorts_recoded.ttrain,
t_cohorts_recoded.lcip4_cred,
Sum(CASE
    WHEN respondent = '1'
    AND t_cohorts_recoded.current_region_pssm_code <>- 1 THEN 1
    ELSE 0
    END) AS Respondents
INTO DACSO_Q008_Z02b_Respondents_NOC_99999
FROM   t_cohorts_recoded
INNER JOIN (t_current_region_pssm_codes
            INNER JOIN t_current_region_pssm_rollup_codes
            ON
            t_current_region_pssm_codes.current_region_pssm_code_rollup =
              t_current_region_pssm_rollup_codes.current_region_pssm_code_rollup)
ON t_cohorts_recoded.current_region_pssm_code =
  t_current_region_pssm_codes.current_region_pssm_code
WHERE  ( ( ( t_cohorts_recoded.noc_cd ) = '99999' )
         AND ( ( t_cohorts_recoded.new_labour_supply ) = 1
               OR ( t_cohorts_recoded.new_labour_supply ) = 3 )
         AND ( ( t_cohorts_recoded.weight ) > 0 ) )
GROUP  BY t_cohorts_recoded.survey,
t_current_region_pssm_rollup_codes.current_region_pssm_code_rollup,
t_cohorts_recoded.survey_year,
t_cohorts_recoded.inst_cd,
t_cohorts_recoded.age_group_rollup,
t_cohorts_recoded.grad_status,
t_cohorts_recoded.ttrain,
t_cohorts_recoded.lcip4_cred
HAVING ( ( ( t_cohorts_recoded.age_group_rollup ) IS NOT NULL )
         AND ( ( t_cohorts_recoded.grad_status ) = '1'
               OR ( t_cohorts_recoded.grad_status ) = '3' ) );"



# ---- DACSO_Q008_Z02b_Respondents_NOC_99999_100_perc ----
DACSO_Q008_Z02b_Respondents_NOC_99999_100_perc <- 
"
SELECT dacso_q008_z02a_base.survey,
       dacso_q008_z02a_base.current_region_pssm_code_rollup,
       dacso_q008_z02a_base.survey_year,
       dacso_q008_z02a_base.inst_cd,
       dacso_q008_z02a_base.age_group_rollup,
       dacso_q008_z02a_base.grad_status,
       dacso_q008_z02a_base.ttrain,
       dacso_q008_z02a_base.lcip4_cred,
       dacso_q008_z02a_base.count,
       dacso_q008_z02a_base.base,
       dacso_q008_z02b_respondents_noc_99999.respondents,
       respondents / count AS Expr1
INTO DACSO_Q008_Z02b_Respondents_NOC_99999_100_perc
FROM   dacso_q008_z02a_base
       INNER JOIN dacso_q008_z02b_respondents_noc_99999
               ON ( dacso_q008_z02a_base.lcip4_cred =
                               dacso_q008_z02b_respondents_noc_99999.lcip4_cred )
                  AND ( dacso_q008_z02a_base.grad_status =
                            dacso_q008_z02b_respondents_noc_99999.grad_status )
                  AND ( dacso_q008_z02a_base.age_group_rollup =
dacso_q008_z02b_respondents_noc_99999.age_group_rollup )
AND ( dacso_q008_z02a_base.inst_cd =
dacso_q008_z02b_respondents_noc_99999.inst_cd )
AND ( dacso_q008_z02a_base.survey_year =
dacso_q008_z02b_respondents_noc_99999.survey_year )
AND ( dacso_q008_z02a_base.current_region_pssm_code_rollup =
dacso_q008_z02b_respondents_noc_99999.current_region_pssm_code_rollup )
AND ( dacso_q008_z02a_base.survey =
dacso_q008_z02b_respondents_noc_99999.survey )
WHERE  (( ( respondents / count ) = 1 ));"



# ---- DACSO_Q008_Z02b_Respondents_Union ----
DACSO_Q008_Z02b_Respondents_Union <- 
"
SELECT DACSO_Q008_Z02b_Respondents.Survey, 
DACSO_Q008_Z02b_Respondents.Current_Region_PSSM_Code_Rollup, 
DACSO_Q008_Z02b_Respondents.Survey_Year, 
DACSO_Q008_Z02b_Respondents.INST_CD, 
DACSO_Q008_Z02b_Respondents.Age_Group_Rollup, 
DACSO_Q008_Z02b_Respondents.GRAD_STATUS, 
DACSO_Q008_Z02b_Respondents.TTRAIN, 
DACSO_Q008_Z02b_Respondents.LCIP4_CRED, 
DACSO_Q008_Z02b_Respondents.Respondents
INTO DACSO_Q008_Z02b_Respondents_Union
FROM DACSO_Q008_Z02b_Respondents
UNION ALL 
SELECT DACSO_Q008_Z02b_Respondents_NOC_99999_100_perc.Survey, 
DACSO_Q008_Z02b_Respondents_NOC_99999_100_perc.Current_Region_PSSM_Code_Rollup, 
DACSO_Q008_Z02b_Respondents_NOC_99999_100_perc.Survey_Year, 
DACSO_Q008_Z02b_Respondents_NOC_99999_100_perc.INST_CD, 
DACSO_Q008_Z02b_Respondents_NOC_99999_100_perc.Age_Group_Rollup, 
DACSO_Q008_Z02b_Respondents_NOC_99999_100_perc.GRAD_STATUS,  
DACSO_Q008_Z02b_Respondents_NOC_99999_100_perc.TTRAIN, 
DACSO_Q008_Z02b_Respondents_NOC_99999_100_perc.LCIP4_CRED,
DACSO_Q008_Z02b_Respondents_NOC_99999_100_perc.Respondents
FROM DACSO_Q008_Z02b_Respondents_NOC_99999_100_perc;"



# ---- DACSO_Q008_Z02c_Weight ----
DACSO_Q008_Z02c_Weight <- 
"SELECT DACSO_Q008_Z02a_Base.Survey, 
        DACSO_Q008_Z02a_Base.Current_Region_PSSM_Code_Rollup, 
        DACSO_Q008_Z02a_Base.Survey_Year, DACSO_Q008_Z02a_Base.INST_CD, 
        DACSO_Q008_Z02a_Base.Age_Group_Rollup, DACSO_Q008_Z02a_Base.GRAD_STATUS, 
        DACSO_Q008_Z02a_Base.TTRAIN, DACSO_Q008_Z02a_Base.LCIP4_CRED, 
        DACSO_Q008_Z02a_Base.Weight_NLS, DACSO_Q008_Z02a_Base.Base, 
        DACSO_Q008_Z02b_Respondents_Union.Respondents, 
        CASE WHEN ISNULL(DACSO_Q008_Z02b_Respondents_Union.Respondents,0)=0 THEN 1 ELSE (DACSO_Q008_Z02a_Base.Base/DACSO_Q008_Z02b_Respondents_Union.Respondents) END AS Weight_NLS_Base, 
        ISNULL(DACSO_Q008_Z02b_Respondents_Union.Respondents,0)*(CASE WHEN ISNULL(DACSO_Q008_Z02b_Respondents_Union.Respondents,0) = 0 THEN 1 ELSE DACSO_Q008_Z02a_Base.Base/DACSO_Q008_Z02b_Respondents_Union.Respondents END) AS Weighted
INTO DACSO_Q008_Z02c_Weight
FROM DACSO_Q008_Z02a_Base 
LEFT JOIN DACSO_Q008_Z02b_Respondents_Union 
  ON (DACSO_Q008_Z02a_Base.LCIP4_CRED = DACSO_Q008_Z02b_Respondents_Union.LCIP4_CRED) 
  AND (DACSO_Q008_Z02a_Base.GRAD_STATUS = DACSO_Q008_Z02b_Respondents_Union.GRAD_STATUS) 
  AND (DACSO_Q008_Z02a_Base.Age_Group_Rollup = DACSO_Q008_Z02b_Respondents_Union.Age_Group_Rollup) 
  AND (DACSO_Q008_Z02a_Base.INST_CD = DACSO_Q008_Z02b_Respondents_Union.INST_CD) 
  AND (DACSO_Q008_Z02a_Base.Survey_Year = DACSO_Q008_Z02b_Respondents_Union.Survey_Year) 
  AND (DACSO_Q008_Z02a_Base.Current_Region_PSSM_Code_Rollup = DACSO_Q008_Z02b_Respondents_Union.Current_Region_PSSM_Code_Rollup)
GROUP BY DACSO_Q008_Z02a_Base.Survey, 
        DACSO_Q008_Z02a_Base.Current_Region_PSSM_Code_Rollup, 
        DACSO_Q008_Z02a_Base.Survey_Year, DACSO_Q008_Z02a_Base.INST_CD, 
        DACSO_Q008_Z02a_Base.Age_Group_Rollup, DACSO_Q008_Z02a_Base.GRAD_STATUS, 
        DACSO_Q008_Z02a_Base.TTRAIN, DACSO_Q008_Z02a_Base.LCIP4_CRED, 
        DACSO_Q008_Z02a_Base.Weight_NLS, DACSO_Q008_Z02a_Base.Base, 
        DACSO_Q008_Z02b_Respondents_Union.Respondents;"



# ---- DACSO_Q008_Z03_Weight_Total ----
DACSO_Q008_Z03_Weight_Total <- 
"SELECT dacso_q008_z02c_weight.survey,
       dacso_q008_z02c_weight.current_region_pssm_code_rollup,
       dacso_q008_z02c_weight.inst_cd,
       dacso_q008_z02c_weight.age_group_rollup,
       dacso_q008_z02c_weight.grad_status,
       dacso_q008_z02c_weight.ttrain,
       dacso_q008_z02c_weight.lcip4_cred,
       Sum(dacso_q008_z02c_weight.base)     AS Base,
       Sum(dacso_q008_z02c_weight.weighted) AS Weighted
INTO DACSO_Q008_Z03_Weight_Total
FROM   dacso_q008_z02c_weight
GROUP  BY dacso_q008_z02c_weight.survey,
          dacso_q008_z02c_weight.current_region_pssm_code_rollup,
          dacso_q008_z02c_weight.inst_cd,
          dacso_q008_z02c_weight.age_group_rollup,
          dacso_q008_z02c_weight.grad_status,
          dacso_q008_z02c_weight.ttrain,
          dacso_q008_z02c_weight.lcip4_cred;"



# ---- DACSO_Q008_Z04_Weight_Adj_Fac ----
DACSO_Q008_Z04_Weight_Adj_Fac <- 
"SELECT dacso_q008_z03_weight_total.survey,
       dacso_q008_z03_weight_total.current_region_pssm_code_rollup,
       dacso_q008_z03_weight_total.inst_cd,
       dacso_q008_z03_weight_total.age_group_rollup,
       dacso_q008_z03_weight_total.grad_status,
       dacso_q008_z03_weight_total.ttrain,
       dacso_q008_z03_weight_total.lcip4_cred,
       dacso_q008_z03_weight_total.base,
       dacso_q008_z03_weight_total.weighted,
       CASE
         WHEN weighted = 0 THEN 0
         ELSE base / weighted
       END AS Weight_Adj_Fac
INTO DACSO_Q008_Z04_Weight_Adj_Fac
FROM   dacso_q008_z03_weight_total;"



# ---- DACSO_Q008_Z05_Weight_OCC ----
DACSO_Q008_Z05_Weight_OCC <- 
"SELECT DACSO_Q008_Z02c_Weight.Survey, 
      DACSO_Q008_Z02c_Weight.Current_Region_PSSM_Code_Rollup,
      T_Current_Region_PSSM_Codes.Current_Region_PSSM_Code, DACSO_Q008_Z02c_Weight.Survey_Year, 
      DACSO_Q008_Z02c_Weight.INST_CD, DACSO_Q008_Z02c_Weight.Age_Group_Rollup, 
      DACSO_Q008_Z02c_Weight.GRAD_STATUS, DACSO_Q008_Z02c_Weight.TTRAIN, 
      DACSO_Q008_Z02c_Weight.LCIP4_CRED, DACSO_Q008_Z02c_Weight.Base, 
      DACSO_Q008_Z02c_Weight.Respondents, DACSO_Q008_Z02c_Weight.Weight_NLS_Base, 
      DACSO_Q008_Z04_Weight_Adj_Fac.Weighted, DACSO_Q008_Z04_Weight_Adj_Fac.Weight_Adj_Fac, 
      Weight_NLS_Base*Weight_Adj_Fac AS Weight_OCC 
INTO tmp_tbl_Weights_OCC
FROM ((DACSO_Q008_Z02c_Weight 
INNER JOIN DACSO_Q008_Z04_Weight_Adj_Fac 
  ON (DACSO_Q008_Z02c_Weight.Age_Group_Rollup = DACSO_Q008_Z04_Weight_Adj_Fac.Age_Group_Rollup) 
  AND (DACSO_Q008_Z02c_Weight.INST_CD = DACSO_Q008_Z04_Weight_Adj_Fac.INST_CD) 
  AND (DACSO_Q008_Z02c_Weight.Survey = DACSO_Q008_Z04_Weight_Adj_Fac.Survey) 
  AND (DACSO_Q008_Z02c_Weight.GRAD_STATUS = DACSO_Q008_Z04_Weight_Adj_Fac.GRAD_STATUS) 
  AND (DACSO_Q008_Z02c_Weight.LCIP4_CRED = DACSO_Q008_Z04_Weight_Adj_Fac.LCIP4_CRED) 
  AND (DACSO_Q008_Z02c_Weight.Current_Region_PSSM_Code_Rollup = DACSO_Q008_Z04_Weight_Adj_Fac.Current_Region_PSSM_Code_Rollup)) 
  INNER JOIN T_Current_Region_PSSM_Rollup_Codes 
    ON DACSO_Q008_Z02c_Weight.Current_Region_PSSM_Code_Rollup = T_Current_Region_PSSM_Rollup_Codes.Current_Region_PSSM_Code_Rollup) 
INNER JOIN T_Current_Region_PSSM_Codes 
  ON T_Current_Region_PSSM_Rollup_Codes.Current_Region_PSSM_Code_Rollup = T_Current_Region_PSSM_Codes.Current_Region_PSSM_Code_Rollup;"



# ---- DACSO_Q008_Z05b_Finding_NLS2_Missing ----
DACSO_Q008_Z05b_Finding_NLS2_Missing <- 
"SELECT 
tmp_tbl_Weights_OCC.Survey, 
tmp_tbl_Weights_OCC.INST_CD, 
tmp_tbl_Weights_OCC.Age_Group_Rollup, 
tmp_tbl_Weights_OCC.GRAD_STATUS, 
tmp_tbl_Weights_NLS.TTRAIN, 
tmp_tbl_Weights_OCC.LCIP4_CRED
INTO DACSO_Q008_Z05b_Finding_NLS2_Missing
FROM tmp_tbl_Weights_NLS 
LEFT JOIN tmp_tbl_Weights_OCC 
  ON (tmp_tbl_Weights_NLS.LCIP4_CRED = tmp_tbl_Weights_OCC.LCIP4_CRED) 
  AND (tmp_tbl_Weights_NLS.GRAD_STATUS = tmp_tbl_Weights_OCC.GRAD_STATUS) 
  AND (tmp_tbl_Weights_NLS.Age_Group_Rollup = tmp_tbl_Weights_OCC.Age_Group_Rollup) 
  AND (tmp_tbl_Weights_NLS.INST_CD = tmp_tbl_Weights_OCC.INST_CD) 
  AND (tmp_tbl_Weights_NLS.Survey = tmp_tbl_Weights_OCC.Survey)
WHERE (((tmp_tbl_Weights_OCC.Survey) Is Null) 
  AND ((tmp_tbl_Weights_OCC.INST_CD) Is Null) 
  AND ((tmp_tbl_Weights_OCC.Age_Group_Rollup) Is Null) 
  AND ((tmp_tbl_Weights_OCC.GRAD_STATUS) Is Null) 
  AND ((tmp_tbl_Weights_OCC.LCIP4_CRED) Is Null));"

# ---- DACSO_Q008_Z05b_NOC4D_NLS_XTab ----
DACSO_Q008_Z05b_NOC4D_NLS_XTab <- 
"
SELECT age_group_rollup, noc_cd, english_name, [1],[3]
INTO DACSO_Q008_Z05b_NOC4D_NLS_XTab
FROM(
SELECT t_cohorts_recoded.age_group_rollup,
       t_cohorts_recoded.noc_cd,
       T_NOC_Broad_Categories.english_name, 
	   t_cohorts_recoded.new_labour_supply, 
	   (CASE WHEN respondent='1' THEN 1 ELSE 0 END)*weight_nls as weight_nls
FROM   t_cohorts_recoded
LEFT JOIN T_NOC_Broad_Categories
	ON t_cohorts_recoded.noc_cd = T_NOC_Broad_Categories.unit_group_code
WHERE t_cohorts_recoded.age_group_rollup IS NOT NULL 
 AND t_cohorts_recoded.noc_cd IS NOT NULL 
 AND ( t_cohorts_recoded.new_labour_supply = 1 OR t_cohorts_recoded.new_labour_supply = 3 )
 AND t_cohorts_recoded.weight > 0
 AND ( t_cohorts_recoded.grad_status = '1' OR t_cohorts_recoded.grad_status = '3' )
 AND t_cohorts_recoded.lcp4_cd IS NOT NULL) P
PIVOT (
    SUM(weight_nls) 
	FOR new_labour_supply IN ([1], [3])
) AS PivotTable
order by age_group_rollup, noc_cd;"



# ---- DACSO_Q008_Z05b_Weight_Comparison ----
DACSO_Q008_Z05b_Weight_Comparison <- 
"
SELECT tmp_tbl_weights_nls.survey,
       tmp_tbl_weights_nls.survey_year,
       tmp_tbl_weights_nls.inst_cd,
       tmp_tbl_weights_nls.age_group_rollup,
       tmp_tbl_weights_nls.grad_status,
       tmp_tbl_weights_nls.ttrain,
       tmp_tbl_weights_nls.lcip4_cred,
       tmp_tbl_weights_nls.weight_nls,
       tmp_tbl_weights_occ.weight_occ,
       tmp_tbl_weights_occ.respondents,
       weight_occ / weight_nls AS Ratio
INTO DACSO_Q008_Z05b_Weight_Comparison
FROM   tmp_tbl_weights_nls
       INNER JOIN tmp_tbl_weights_occ
               ON ( tmp_tbl_weights_nls.survey_year =
                    tmp_tbl_weights_occ.survey_year )
                  AND ( tmp_tbl_weights_nls.inst_cd =
                        tmp_tbl_weights_occ.inst_cd )
                  AND ( tmp_tbl_weights_nls.grad_status =
                        tmp_tbl_weights_occ.grad_status )
                  AND ( tmp_tbl_weights_nls.lcip4_cred =
                      tmp_tbl_weights_occ.lcip4_cred )
                  AND ( tmp_tbl_weights_nls.age_group_rollup =
                        tmp_tbl_weights_occ.age_group_rollup )
WHERE  ( ( ( tmp_tbl_weights_occ.weight_occ ) <> weight_nls )
         AND ( ( tmp_tbl_weights_occ.respondents ) IS NOT NULL ) ); "


# ---- DACSO_Q008_Z06_Add_Weight_OCC_Field ----
DACSO_Q008_Z06_Add_Weight_OCC_Field <- 
"ALTER TABLE T_Cohorts_Recoded ADD Weight_OCC FLOAT NULL;"



# ---- DACSO_Q008_Z07_Weight_OCC_Null ----
DACSO_Q008_Z07_Weight_OCC_Null <- 
"UPDATE T_Cohorts_Recoded SET T_Cohorts_Recoded.Weight_OCC = Null;"



# ---- DACSO_Q008_Z08_Weight_OCC_Update ----
DACSO_Q008_Z08_Weight_OCC_Update <- 
"UPDATE T_Cohorts_Recoded 
SET T_Cohorts_Recoded.Weight_OCC = tmp_tbl_Weights_OCC.Weight_OCC
FROM T_Current_Region_PSSM_Codes 
INNER JOIN ((tmp_tbl_Weights_OCC INNER JOIN T_Cohorts_Recoded 
  ON (tmp_tbl_Weights_OCC.Current_Region_PSSM_Code = T_Cohorts_Recoded.CURRENT_REGION_PSSM_CODE) 
  AND (tmp_tbl_Weights_OCC.LCIP4_CRED = T_Cohorts_Recoded.LCIP4_CRED) 
  AND (tmp_tbl_Weights_OCC.GRAD_STATUS = T_Cohorts_Recoded.GRAD_STATUS) 
  AND (tmp_tbl_Weights_OCC.Survey_Year = T_Cohorts_Recoded.Survey_Year) 
  AND (tmp_tbl_Weights_OCC.INST_CD = T_Cohorts_Recoded.INST_CD) 
  AND (tmp_tbl_Weights_OCC.Age_Group_Rollup = T_Cohorts_Recoded.Age_Group_Rollup)) 
INNER JOIN DACSO_Q008_Z01_Base_OCC 
  ON T_Cohorts_Recoded.STQU_ID = DACSO_Q008_Z01_Base_OCC.STQU_ID) 
  ON T_Current_Region_PSSM_Codes.Current_Region_PSSM_Code = T_Cohorts_Recoded.CURRENT_REGION_PSSM_CODE 
SET T_Cohorts_Recoded.Weight_OCC = tmp_tbl_Weights_OCC.Weight_OCC
WHERE (((T_Cohorts_Recoded.CURRENT_REGION_PSSM_CODE)<>-1) 
  AND ((T_Cohorts_Recoded.NOC_CD) Is Not Null 
  And (T_Cohorts_Recoded.NOC_CD) <> '99999''));"


# ---- DACSO_Q008_Z08_Weight_OCC_Update ----
DACSO_Q008_Z08_Weight_OCC_Update <-
"UPDATE T_Cohorts_Recoded
SET T_Cohorts_Recoded.Weight_OCC = tmp_tbl_Weights_OCC.Weight_OCC 
from T_Cohorts_Recoded
inner join tmp_tbl_Weights_OCC
  ON  tmp_tbl_Weights_OCC.Current_Region_PSSM_Code = T_Cohorts_Recoded.CURRENT_REGION_PSSM_CODE 
  AND tmp_tbl_Weights_OCC.LCIP4_CRED = T_Cohorts_Recoded.LCIP4_CRED
  AND tmp_tbl_Weights_OCC.GRAD_STATUS = T_Cohorts_Recoded.GRAD_STATUS 
  AND tmp_tbl_Weights_OCC.Survey_Year = T_Cohorts_Recoded.Survey_Year
  AND tmp_tbl_Weights_OCC.INST_CD = T_Cohorts_Recoded.INST_CD
  AND tmp_tbl_Weights_OCC.Age_Group_Rollup = T_Cohorts_Recoded.Age_Group_Rollup
inner join T_Current_Region_PSSM_Codes
on T_Current_Region_PSSM_Codes.Current_Region_PSSM_Code = T_Cohorts_Recoded.CURRENT_REGION_PSSM_CODE
inner join DACSO_Q008_Z01_Base_OCC 
on DACSO_Q008_Z01_Base_OCC.STQU_ID = T_Cohorts_Recoded.STQU_ID
WHERE T_Cohorts_Recoded.CURRENT_REGION_PSSM_CODE <> -1
AND T_Cohorts_Recoded.NOC_CD Is Not Null 
And T_Cohorts_Recoded.NOC_CD <> '99999';"

# ---- DACSO_Q008_Z08_Weight_OCC_Update_NOC_99999_100_perc ----
DACSO_Q008_Z08_Weight_OCC_Update_NOC_99999_100_perc <- 
"UPDATE t_cohorts_recoded
SET        t_cohorts_recoded.weight_occ = tmp_tbl_weights_occ.weight_occ
from t_cohorts_recoded
INNER JOIN t_current_region_pssm_codes
 ON t_current_region_pssm_codes.current_region_pssm_code = t_cohorts_recoded.current_region_pssm_code
INNER JOIN tmp_tbl_weights_occ
ON         tmp_tbl_weights_occ.age_group_rollup = t_cohorts_recoded.age_group_rollup
AND        tmp_tbl_weights_occ.inst_cd = t_cohorts_recoded.inst_cd
AND        tmp_tbl_weights_occ.survey_year = t_cohorts_recoded.survey_year
AND        tmp_tbl_weights_occ.grad_status = t_cohorts_recoded.grad_status
AND        tmp_tbl_weights_occ.lcip4_cred = t_cohorts_recoded.lcip4_cred
AND        tmp_tbl_weights_occ.current_region_pssm_code = t_cohorts_recoded.current_region_pssm_code
INNER JOIN dacso_q008_z01_base_occ
ON         t_cohorts_recoded.stqu_id = dacso_q008_z01_base_occ.stqu_id
INNER JOIN dacso_q008_z02b_respondents_noc_99999_100_perc
ON         tmp_tbl_weights_occ.survey = dacso_q008_z02b_respondents_noc_99999_100_perc.survey
AND        tmp_tbl_weights_occ.current_region_pssm_code_rollup = dacso_q008_z02b_respondents_noc_99999_100_perc.current_region_pssm_code_rollup
AND        tmp_tbl_weights_occ.survey_year = dacso_q008_z02b_respondents_noc_99999_100_perc.survey_year
AND        tmp_tbl_weights_occ.inst_cd = dacso_q008_z02b_respondents_noc_99999_100_perc.inst_cd
AND        dacso_q008_z02b_respondents_noc_99999_100_perc.age_group_rollup = tmp_tbl_weights_occ.age_group_rollup
AND        tmp_tbl_weights_occ.grad_status = dacso_q008_z02b_respondents_noc_99999_100_perc.grad_status
AND        tmp_tbl_weights_occ.lcip4_cred = dacso_q008_z02b_respondents_noc_99999_100_perc.lcip4_cred
WHERE      t_cohorts_recoded.current_region_pssm_code<>-1
AND        t_cohorts_recoded.noc_cd IS NOT NULL;"



# ---- DACSO_Q008_Z09_Check_Weights ----
DACSO_Q008_Z09_Check_Weights <- 
"SELECT t_cohorts_recoded.survey_year,
       t_cohorts_recoded.inst_cd,
       t_cohorts_recoded.age_group_rollup,
       t_cohorts_recoded.ttrain,
       t_cohorts_recoded.lcip4_cred,
       Sum(Iif(respondent = '1'
               AND t_cohorts_recoded.current_region_pssm_code <>- 1, 1, 0))  AS
       Respondents,
       t_cohorts_recoded.weight_occ,
       Sum(Iif(respondent = '1'
               AND
       t_cohorts_recoded.current_region_pssm_code <>- 1, 1, 0)) * weight_occ AS Weighted,
       Sum(dacso_q008_z01_base_occ.base) AS Base,
       t_cohorts_recoded.respondent
FROM   t_cohorts_recoded
       INNER JOIN dacso_q008_z01_base_occ
               ON t_cohorts_recoded.stqu_id = dacso_q008_z01_base_occ.stqu_id
WHERE  (( ( dacso_q008_z01_base_occ.new_labour_supply ) = 1
           OR ( dacso_q008_z01_base_occ.new_labour_supply ) = 3 ))
GROUP  BY t_cohorts_recoded.survey_year,
          t_cohorts_recoded.inst_cd,
          t_cohorts_recoded.age_group_rollup,
          t_cohorts_recoded.ttrain,
          t_cohorts_recoded.lcip4_cred,
          t_cohorts_recoded.weight_occ,
          t_cohorts_recoded.respondent
HAVING ( ( ( t_cohorts_recoded.weight_occ ) IS NOT NULL )
         AND ( ( t_cohorts_recoded.respondent ) = '1' ) )
ORDER  BY t_cohorts_recoded.survey_year,
          t_cohorts_recoded.weight_occ;"




# ---- DACSO_Q009_Weight_Occs ----
DACSO_Q009_Weight_Occs <- "
SELECT t_cohorts_recoded.pssm_credential,
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
(CASE WHEN t_cohorts_recoded.noc_cd = 'XXXXX' THEN '99999' ELSE t_cohorts_recoded.noc_cd END) AS NOC_CD,
Count(*) AS Count,
t_cohorts_recoded.weight_occ,
Count(*) * weight_occ AS Weighted
INTO DACSO_Q009_Weight_Occs
FROM   t_cohorts_recoded
INNER JOIN (t_current_region_pssm_codes
        INNER JOIN t_current_region_pssm_rollup_codes
        ON t_current_region_pssm_codes.current_region_pssm_code_rollup = t_current_region_pssm_rollup_codes.current_region_pssm_code_rollup)
ON t_cohorts_recoded.current_region_pssm_code = t_current_region_pssm_codes.current_region_pssm_code
WHERE  ( ( ( t_cohorts_recoded.new_labour_supply ) = 1
           OR ( t_cohorts_recoded.new_labour_supply ) = 3 )
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
Iif(t_cohorts_recoded.noc_cd = 'XXXXX', '99999',
    t_cohorts_recoded.noc_cd),
t_cohorts_recoded.weight_occ
HAVING t_current_region_pssm_rollup_codes.current_region_pssm_code_rollup  <> 99999 
  AND t_cohorts_recoded.age_group_rollup IS NOT NULL
  AND ( t_cohorts_recoded.grad_status  = '1' OR t_cohorts_recoded.grad_status = '3' )
  AND Iif(t_cohorts_recoded.noc_cd = 'XXXXX', '99999', t_cohorts_recoded.noc_cd) IS NOT NULL
  AND t_cohorts_recoded.weight_occ IS NOT NULL
ORDER  BY t_cohorts_recoded.grad_status,
	t_cohorts_recoded.lcip4_cred,
	CASE WHEN t_cohorts_recoded.noc_cd = 'XXXXX' THEN '99999' ELSE t_cohorts_recoded.noc_cd END;"
  


# ---- DACSO_Q009_Weighted_Occs_2D ----
DACSO_Q009_Weighted_Occs_2D <- 
"SELECT dacso_q009_weight_occs.pssm_credential,
       dacso_q009_weight_occs.pssm_cred,
       dacso_q009_weight_occs.current_region_pssm_code_rollup,
       dacso_q009_weight_occs.age_group_rollup,
       LEFT(lcp4_cd, 2)                     AS LCP2_CD,
       dacso_q009_weight_occs.ttrain,
       dacso_q009_weight_occs.lcip2_cred,
       dacso_q009_weight_occs.noc_cd,
       Sum(dacso_q009_weight_occs.weighted) AS Count
INTO DACSO_Q009_Weighted_Occs_2D
FROM   dacso_q009_weight_occs
GROUP  BY dacso_q009_weight_occs.pssm_credential,
          dacso_q009_weight_occs.pssm_cred,
          dacso_q009_weight_occs.current_region_pssm_code_rollup,
          dacso_q009_weight_occs.age_group_rollup,
          LEFT(lcp4_cd, 2),
          dacso_q009_weight_occs.ttrain,
          dacso_q009_weight_occs.lcip2_cred,
          dacso_q009_weight_occs.noc_cd; "



# ---- DACSO_Q009_Weighted_Occs_2D_BC ----
DACSO_Q009_Weighted_Occs_2D_BC <- 
"SELECT dacso_q009_weight_occs.pssm_credential,
       dacso_q009_weight_occs.pssm_cred,
       LEFT(lcp4_cd, 2)                     AS LCP2_CD,
       dacso_q009_weight_occs.ttrain,
       dacso_q009_weight_occs.lcip2_cred,
       dacso_q009_weight_occs.noc_cd,
       Sum(dacso_q009_weight_occs.weighted) AS Count
INTO DACSO_Q009_Weighted_Occs_2D_BC
FROM   dacso_q009_weight_occs
       INNER JOIN t_current_region_pssm_rollup_codes_bc
               ON dacso_q009_weight_occs.current_region_pssm_code_rollup =
t_current_region_pssm_rollup_codes_bc.current_region_pssm_code_rollup
WHERE  (( (
t_current_region_pssm_rollup_codes_bc.current_region_pssm_code_rollup_bc )
     IS
      NOT NULL ))
GROUP  BY dacso_q009_weight_occs.pssm_credential,
          dacso_q009_weight_occs.pssm_cred,
          LEFT(lcp4_cd, 2),
          dacso_q009_weight_occs.ttrain,
          dacso_q009_weight_occs.lcip2_cred,
          dacso_q009_weight_occs.noc_cd; "



# ---- DACSO_Q009_Weighted_Occs_2D_BC_No_TT ----
DACSO_Q009_Weighted_Occs_2D_BC_No_TT <- 
"SELECT DACSO_Q009_Weight_Occs.PSSM_Credential, DACSO_Q009_Weight_Occs.PSSM_CRED, Left(LCP4_CD,2) AS LCP2_CD, 
IIf((Left(PSSM_CRED,1)='1' Or Left(PSSM_CRED,1)='3'),Left(PSSM_CRED,1) + ' - ','') + Left(LCP4_CD,2) + ' - ' + PSSM_Credential AS LCIP2_CRED, 
DACSO_Q009_Weight_Occs.NOC_CD, Sum(DACSO_Q009_Weight_Occs.Weighted) AS Count
INTO DACSO_Q009_Weighted_Occs_2D_BC_No_TT
FROM DACSO_Q009_Weight_Occs 
INNER JOIN T_Current_Region_PSSM_Rollup_Codes_BC 
ON DACSO_Q009_Weight_Occs.Current_Region_PSSM_Code_Rollup = T_Current_Region_PSSM_Rollup_Codes_BC.Current_Region_PSSM_Code_Rollup
WHERE (((T_Current_Region_PSSM_Rollup_Codes_BC.Current_Region_PSSM_Code_Rollup_BC) Is Not Null))
GROUP BY DACSO_Q009_Weight_Occs.PSSM_Credential, 
DACSO_Q009_Weight_Occs.PSSM_CRED, Left(LCP4_CD,2), IIf((Left(PSSM_CRED,1)='1' Or Left(PSSM_CRED,1)='3'),Left(PSSM_CRED,1) + ' - ','') + Left(LCP4_CD,2) + ' - ' + PSSM_Credential, DACSO_Q009_Weight_Occs.NOC_CD;"



# ---- DACSO_Q009_Weighted_Occs_2D_No_TT ----
DACSO_Q009_Weighted_Occs_2D_No_TT <- 
"SELECT DACSO_Q009_Weight_Occs.PSSM_Credential, 
DACSO_Q009_Weight_Occs.PSSM_CRED, 
DACSO_Q009_Weight_Occs.Current_Region_PSSM_Code_Rollup, 
DACSO_Q009_Weight_Occs.Age_Group_Rollup, Left(LCP4_CD,2) AS LCP2_CD, IIf((Left(PSSM_CRED,1)='1' Or Left(PSSM_CRED,1)='3'),Left(PSSM_CRED,1) + ' - ','') + Left(LCP4_CD,2) + ' - ' + PSSM_Credential AS LCIP2_CRED, 
DACSO_Q009_Weight_Occs.NOC_CD, Sum(DACSO_Q009_Weight_Occs.Weighted) AS Count
INTO DACSO_Q009_Weighted_Occs_2D_No_TT
FROM DACSO_Q009_Weight_Occs
GROUP BY DACSO_Q009_Weight_Occs.PSSM_Credential, DACSO_Q009_Weight_Occs.PSSM_CRED, 
DACSO_Q009_Weight_Occs.Current_Region_PSSM_Code_Rollup, DACSO_Q009_Weight_Occs.Age_Group_Rollup, Left(LCP4_CD,2), IIf((Left(PSSM_CRED,1)='1' Or Left(PSSM_CRED,1)='3'),Left(PSSM_CRED,1) + ' - ','') + Left(LCP4_CD,2) + ' - ' + PSSM_Credential, DACSO_Q009_Weight_Occs.NOC_CD;"



# ---- DACSO_Q009_Weighted_Occs_Total_2D ----
DACSO_Q009_Weighted_Occs_Total_2D <- 
"SELECT DACSO_Q009_Weight_Occs.PSSM_Credential, DACSO_Q009_Weight_Occs.PSSM_CRED, 
DACSO_Q009_Weight_Occs.Current_Region_PSSM_Code_Rollup, DACSO_Q009_Weight_Occs.Age_Group_Rollup, 
Left(LCP4_CD,2) AS LCP2_CD, DACSO_Q009_Weight_Occs.TTRAIN, DACSO_Q009_Weight_Occs.LCIP2_CRED, 
Sum(DACSO_Q009_Weight_Occs.Weighted) AS Total
INTO DACSO_Q009_Weighted_Occs_Total_2D 
FROM DACSO_Q009_Weight_Occs
GROUP BY DACSO_Q009_Weight_Occs.PSSM_Credential, DACSO_Q009_Weight_Occs.PSSM_CRED, 
DACSO_Q009_Weight_Occs.Current_Region_PSSM_Code_Rollup, DACSO_Q009_Weight_Occs.Age_Group_Rollup, 
Left(LCP4_CD,2), DACSO_Q009_Weight_Occs.TTRAIN, DACSO_Q009_Weight_Occs.LCIP2_CRED;"



# ---- DACSO_Q009_Weighted_Occs_Total_2D_BC ----
DACSO_Q009_Weighted_Occs_Total_2D_BC <- 
"SELECT DACSO_Q009_Weight_Occs.PSSM_Credential, 
DACSO_Q009_Weight_Occs.PSSM_CRED, 
Left(LCP4_CD,2) AS LCP2_CD, 
DACSO_Q009_Weight_Occs.TTRAIN, DACSO_Q009_Weight_Occs.LCIP2_CRED, Sum(DACSO_Q009_Weight_Occs.Weighted) AS Total
INTO DACSO_Q009_Weighted_Occs_Total_2D_BC
FROM DACSO_Q009_Weight_Occs INNER JOIN T_Current_Region_PSSM_Rollup_Codes_BC 
  ON DACSO_Q009_Weight_Occs.Current_Region_PSSM_Code_Rollup = T_Current_Region_PSSM_Rollup_Codes_BC.Current_Region_PSSM_Code_Rollup
WHERE (((T_Current_Region_PSSM_Rollup_Codes_BC.Current_Region_PSSM_Code_Rollup_BC) Is Not Null))
GROUP BY DACSO_Q009_Weight_Occs.PSSM_Credential, DACSO_Q009_Weight_Occs.PSSM_CRED, 
Left(LCP4_CD,2), DACSO_Q009_Weight_Occs.TTRAIN, DACSO_Q009_Weight_Occs.LCIP2_CRED;"



# ---- DACSO_Q009_Weighted_Occs_Total_2D_BC_No_TT ----
DACSO_Q009_Weighted_Occs_Total_2D_BC_No_TT <- 
"SELECT DACSO_Q009_Weight_Occs.PSSM_Credential, DACSO_Q009_Weight_Occs.PSSM_CRED, Left(LCP4_CD,2) AS LCP2_CD, IIf((Left(PSSM_CRED,1)='1' Or Left(PSSM_CRED,1)='3'),Left(PSSM_CRED,1) + ' - ','') + Left(LCP4_CD,2) + ' - ' + PSSM_Credential AS LCIP2_CRED, Sum(DACSO_Q009_Weight_Occs.Weighted) AS Total
INTO DACSO_Q009_Weighted_Occs_Total_2D_BC_No_TT
FROM DACSO_Q009_Weight_Occs INNER JOIN T_Current_Region_PSSM_Rollup_Codes_BC ON DACSO_Q009_Weight_Occs.Current_Region_PSSM_Code_Rollup = T_Current_Region_PSSM_Rollup_Codes_BC.Current_Region_PSSM_Code_Rollup
WHERE (((T_Current_Region_PSSM_Rollup_Codes_BC.Current_Region_PSSM_Code_Rollup_BC) Is Not Null))
GROUP BY DACSO_Q009_Weight_Occs.PSSM_Credential, 
DACSO_Q009_Weight_Occs.PSSM_CRED, Left(LCP4_CD,2), 
IIf((Left(PSSM_CRED,1)='1' Or Left(PSSM_CRED,1)='3'),
Left(PSSM_CRED,1) + ' - ','') + Left(LCP4_CD,2) + ' - ' + PSSM_Credential;"



# ---- DACSO_Q009_Weighted_Occs_Total_2D_No_TT ----
DACSO_Q009_Weighted_Occs_Total_2D_No_TT <- 
"SELECT DACSO_Q009_Weight_Occs.PSSM_Credential, DACSO_Q009_Weight_Occs.PSSM_CRED, 
DACSO_Q009_Weight_Occs.Current_Region_PSSM_Code_Rollup, DACSO_Q009_Weight_Occs.Age_Group_Rollup, 
Left(LCP4_CD,2) AS LCP2_CD, IIf((Left(PSSM_CRED,1)='1' Or Left(PSSM_CRED,1)='3'),
Left(PSSM_CRED,1) + ' - ','') + Left(LCP4_CD,2) + ' - ' + PSSM_Credential AS LCIP2_CRED, 
Sum(DACSO_Q009_Weight_Occs.Weighted) AS Total
INTO DACSO_Q009_Weighted_Occs_Total_2D_No_TT
FROM DACSO_Q009_Weight_Occs
GROUP BY DACSO_Q009_Weight_Occs.PSSM_Credential, 
DACSO_Q009_Weight_Occs.PSSM_CRED, 
DACSO_Q009_Weight_Occs.Current_Region_PSSM_Code_Rollup, DACSO_Q009_Weight_Occs.Age_Group_Rollup, 
Left(LCP4_CD,2), IIf((Left(PSSM_CRED,1)='1' Or Left(PSSM_CRED,1)='3'),Left(PSSM_CRED,1) + ' - ','') + Left(LCP4_CD,2) + ' - ' + PSSM_Credential;"



# ---- DACSO_Q009b_Weighted_Occs ----
DACSO_Q009b_Weighted_Occs <- 
"SELECT DACSO_Q009_Weight_Occs.PSSM_Credential, DACSO_Q009_Weight_Occs.PSSM_CRED, 
DACSO_Q009_Weight_Occs.Current_Region_PSSM_Code_Rollup, DACSO_Q009_Weight_Occs.Age_Group_Rollup, 
DACSO_Q009_Weight_Occs.LCP4_CD, DACSO_Q009_Weight_Occs.TTRAIN, DACSO_Q009_Weight_Occs.LCIP4_CRED, 
DACSO_Q009_Weight_Occs.LCIP2_CRED, DACSO_Q009_Weight_Occs.NOC_CD, Sum(DACSO_Q009_Weight_Occs.Weighted) AS Count
INTO DACSO_Q009b_Weighted_Occs
FROM DACSO_Q009_Weight_Occs
GROUP BY DACSO_Q009_Weight_Occs.PSSM_Credential, DACSO_Q009_Weight_Occs.PSSM_CRED, 
DACSO_Q009_Weight_Occs.Current_Region_PSSM_Code_Rollup, DACSO_Q009_Weight_Occs.Age_Group_Rollup, 
DACSO_Q009_Weight_Occs.LCP4_CD, DACSO_Q009_Weight_Occs.TTRAIN, DACSO_Q009_Weight_Occs.LCIP4_CRED, 
DACSO_Q009_Weight_Occs.LCIP2_CRED, DACSO_Q009_Weight_Occs.NOC_CD
ORDER BY DACSO_Q009_Weight_Occs.LCIP4_CRED, DACSO_Q009_Weight_Occs.NOC_CD;"



# ---- DACSO_Q009b_Weighted_Occs_No_TT ----
DACSO_Q009b_Weighted_Occs_No_TT <- 
"
SELECT dacso_q009_weight_occs.pssm_credential,
dacso_q009_weight_occs.pssm_cred,
dacso_q009_weight_occs.current_region_pssm_code_rollup,
dacso_q009_weight_occs.age_group_rollup,
dacso_q009_weight_occs.lcp4_cd,
(CASE WHEN grad_status IS NULL THEN NULL ELSE cast(grad_status as varchar(10)) + ' - ' END) + lcp4_cd + ' - ' + pssm_credential          AS LCIP4_CRED,
(CASE WHEN grad_status IS NULL THEN NULL ELSE cast(grad_status as varchar(10)) + ' - ' END) + LEFT(lcp4_cd, 2) + ' - ' + pssm_credential AS LCIP2_CRED,
dacso_q009_weight_occs.noc_cd,
Sum(dacso_q009_weight_occs.weighted)         AS Count
INTO   dacso_q009b_weighted_occs_no_tt
FROM   dacso_q009_weight_occs
GROUP  BY dacso_q009_weight_occs.pssm_credential,
dacso_q009_weight_occs.pssm_cred,
dacso_q009_weight_occs.current_region_pssm_code_rollup,
dacso_q009_weight_occs.age_group_rollup,
dacso_q009_weight_occs.lcp4_cd,
(CASE WHEN grad_status IS NULL THEN NULL ELSE cast(grad_status as varchar(10)) + ' - ' END) + lcp4_cd + ' - ' + pssm_credential,
(CASE WHEN grad_status IS NULL THEN NULL ELSE cast(grad_status as varchar(10)) + ' - ' END) + LEFT(lcp4_cd, 2) + ' - ' + pssm_credential,
dacso_q009_weight_occs.noc_cd
ORDER  BY dacso_q009_weight_occs.noc_cd;"



# ---- DACSO_Q009b_Weighted_Occs_Total ----
DACSO_Q009b_Weighted_Occs_Total <- 
"SELECT DACSO_Q009_Weight_Occs.PSSM_Credential, DACSO_Q009_Weight_Occs.PSSM_CRED, DACSO_Q009_Weight_Occs.Current_Region_PSSM_Code_Rollup, DACSO_Q009_Weight_Occs.Age_Group_Rollup, DACSO_Q009_Weight_Occs.LCP4_CD, DACSO_Q009_Weight_Occs.TTRAIN, DACSO_Q009_Weight_Occs.LCIP4_CRED, DACSO_Q009_Weight_Occs.LCIP2_CRED, Sum(DACSO_Q009_Weight_Occs.Weighted) AS Total
INTO DACSO_Q009b_Weighted_Occs_Total
FROM DACSO_Q009_Weight_Occs
GROUP BY DACSO_Q009_Weight_Occs.PSSM_Credential, DACSO_Q009_Weight_Occs.PSSM_CRED, DACSO_Q009_Weight_Occs.Current_Region_PSSM_Code_Rollup, DACSO_Q009_Weight_Occs.Age_Group_Rollup, DACSO_Q009_Weight_Occs.LCP4_CD, DACSO_Q009_Weight_Occs.TTRAIN, DACSO_Q009_Weight_Occs.LCIP4_CRED, DACSO_Q009_Weight_Occs.LCIP2_CRED
ORDER BY DACSO_Q009_Weight_Occs.LCIP4_CRED;"



# ---- DACSO_Q009b_Weighted_Occs_Total_No_TT ----
DACSO_Q009b_Weighted_Occs_Total_No_TT <- 
"SELECT   dacso_q009_weight_occs.pssm_credential,
dacso_q009_weight_occs.pssm_cred,
dacso_q009_weight_occs.current_region_pssm_code_rollup,
dacso_q009_weight_occs.age_group_rollup,
dacso_q009_weight_occs.lcp4_cd,
(CASE WHEN grad_status IS NULL THEN NULL ELSE cast(grad_status as varchar) + ' - ' END ) + lcp4_cd + ' - ' + pssm_credential as lcip4_cred,
(CASE WHEN grad_status IS NULL THEN NULL ELSE cast(grad_status as varchar) + ' - ' END )  + LEFT(lcp4_cd,2) + ' - ' + pssm_credential AS lcip2_cred,
sum(dacso_q009_weight_occs.weighted)                        AS total
INTO     dacso_q009b_weighted_occs_total_no_tt
FROM     dacso_q009_weight_occs
GROUP BY dacso_q009_weight_occs.pssm_credential,
dacso_q009_weight_occs.pssm_cred,
dacso_q009_weight_occs.current_region_pssm_code_rollup,
dacso_q009_weight_occs.age_group_rollup,
dacso_q009_weight_occs.lcp4_cd,
(CASE WHEN grad_status IS NULL THEN NULL ELSE cast(grad_status as varchar) + ' - ' END ) + lcp4_cd + ' - ' + pssm_credential,
(CASE WHEN grad_status IS NULL THEN NULL ELSE cast(grad_status as varchar) + ' - ' END ) + LEFT(lcp4_cd,2) + ' - ' + pssm_credential;"



# ---- DACSO_Q010_Weighted_Occs_Dist ----
DACSO_Q010_Weighted_Occs_Dist <- 
"
SELECT dacso_q009b_weighted_occs_total.pssm_credential,
       dacso_q009b_weighted_occs_total.pssm_cred,
       dacso_q009b_weighted_occs_total.current_region_pssm_code_rollup,
       dacso_q009b_weighted_occs_total.age_group_rollup,
       dacso_q009b_weighted_occs_total.lcp4_cd,
       dacso_q009b_weighted_occs_total.ttrain,
       dacso_q009b_weighted_occs_total.lcip4_cred,
       dacso_q009b_weighted_occs_total.lcip2_cred,
       dacso_q009b_weighted_occs.noc_cd,
       dacso_q009b_weighted_occs.count,
       dacso_q009b_weighted_occs_total.total,
       count / total AS perc_Dist
INTO DACSO_Q010_Weighted_Occs_Dist
FROM   dacso_q009b_weighted_occs_total
       LEFT JOIN dacso_q009b_weighted_occs
              ON ( dacso_q009b_weighted_occs_total.age_group_rollup =
                             dacso_q009b_weighted_occs.age_group_rollup )
                 AND (
       dacso_q009b_weighted_occs_total.current_region_pssm_code_rollup
       =
           dacso_q009b_weighted_occs.current_region_pssm_code_rollup )
                 AND ( dacso_q009b_weighted_occs_total.lcip4_cred =
                           dacso_q009b_weighted_occs.lcip4_cred ); "



# ---- DACSO_Q010_Weighted_Occs_Dist_2D ----
DACSO_Q010_Weighted_Occs_Dist_2D <- 
"SELECT dacso_q009_weighted_occs_total_2d.pssm_credential,
       dacso_q009_weighted_occs_total_2d.pssm_cred,
       dacso_q009_weighted_occs_total_2d.current_region_pssm_code_rollup,
       dacso_q009_weighted_occs_total_2d.age_group_rollup,
       dacso_q009_weighted_occs_total_2d.lcp2_cd,
       dacso_q009_weighted_occs_total_2d.ttrain,
       dacso_q009_weighted_occs_total_2d.lcip2_cred,
       dacso_q009_weighted_occs_2d.noc_cd,
       dacso_q009_weighted_occs_2d.count,
       dacso_q009_weighted_occs_total_2d.total,
       count / total AS perc_Dist
INTO DACSO_Q010_Weighted_Occs_Dist_2D
FROM   dacso_q009_weighted_occs_total_2d
       LEFT JOIN dacso_q009_weighted_occs_2d
              ON ( dacso_q009_weighted_occs_total_2d.lcip2_cred =
                             dacso_q009_weighted_occs_2d.lcip2_cred )
                 AND (
       dacso_q009_weighted_occs_total_2d.current_region_pssm_code_rollup =
           dacso_q009_weighted_occs_2d.current_region_pssm_code_rollup )
                 AND ( dacso_q009_weighted_occs_total_2d.age_group_rollup =
                           dacso_q009_weighted_occs_2d.age_group_rollup ); "



# ---- DACSO_Q010_Weighted_Occs_Dist_2D_BC ----
DACSO_Q010_Weighted_Occs_Dist_2D_BC <- 
"SELECT dacso_q009_weighted_occs_total_2d_bc.pssm_credential,
       dacso_q009_weighted_occs_total_2d_bc.pssm_cred,
       dacso_q009_weighted_occs_total_2d_bc.lcp2_cd,
       dacso_q009_weighted_occs_total_2d_bc.ttrain,
       dacso_q009_weighted_occs_total_2d_bc.lcip2_cred,
       dacso_q009_weighted_occs_2d_bc.noc_cd,
       dacso_q009_weighted_occs_2d_bc.count,
       dacso_q009_weighted_occs_total_2d_bc.total,
       count / total AS perc_Dist
INTO DACSO_Q010_Weighted_Occs_Dist_2D_BC 
FROM   dacso_q009_weighted_occs_total_2d_bc
       LEFT JOIN dacso_q009_weighted_occs_2d_bc
              ON dacso_q009_weighted_occs_total_2d_bc.lcip2_cred =
                 dacso_q009_weighted_occs_2d_bc.lcip2_cred; "




# ---- DACSO_Q010_Weighted_Occs_Dist_2D_BC_No_TT ----
DACSO_Q010_Weighted_Occs_Dist_2D_BC_No_TT <- 
"SELECT dacso_q009_weighted_occs_total_2d_bc_no_tt.pssm_credential,
       dacso_q009_weighted_occs_total_2d_bc_no_tt.pssm_cred,
       dacso_q009_weighted_occs_total_2d_bc_no_tt.lcp2_cd,
       dacso_q009_weighted_occs_total_2d_bc_no_tt.lcip2_cred,
       dacso_q009_weighted_occs_2d_bc_no_tt.noc_cd,
       dacso_q009_weighted_occs_2d_bc_no_tt.count,
       dacso_q009_weighted_occs_total_2d_bc_no_tt.total,
       count / total AS perc_Dist
INTO DACSO_Q010_Weighted_Occs_Dist_2D_BC_No_TT
FROM   dacso_q009_weighted_occs_total_2d_bc_no_tt
       LEFT JOIN dacso_q009_weighted_occs_2d_bc_no_tt
              ON dacso_q009_weighted_occs_total_2d_bc_no_tt.lcip2_cred =
                 dacso_q009_weighted_occs_2d_bc_no_tt.lcip2_cred; "
  


# ---- DACSO_Q010_Weighted_Occs_Dist_2D_No_TT ----
DACSO_Q010_Weighted_Occs_Dist_2D_No_TT <- 
"SELECT dacso_q009_weighted_occs_total_2d_no_tt.pssm_credential,
       dacso_q009_weighted_occs_total_2d_no_tt.pssm_cred,
       dacso_q009_weighted_occs_total_2d_no_tt.current_region_pssm_code_rollup,
       dacso_q009_weighted_occs_total_2d_no_tt.age_group_rollup,
       dacso_q009_weighted_occs_total_2d_no_tt.lcp2_cd,
       dacso_q009_weighted_occs_total_2d_no_tt.lcip2_cred,
       dacso_q009_weighted_occs_2d_no_tt.noc_cd,
       dacso_q009_weighted_occs_2d_no_tt.count,
       dacso_q009_weighted_occs_total_2d_no_tt.total,
       count / total AS perc_Dist
INTO   dacso_q010_weighted_occs_dist_2d_no_tt
FROM   dacso_q009_weighted_occs_total_2d_no_tt
       LEFT JOIN dacso_q009_weighted_occs_2d_no_tt
              ON ( dacso_q009_weighted_occs_total_2d_no_tt.age_group_rollup =
dacso_q009_weighted_occs_2d_no_tt.age_group_rollup )
AND (
dacso_q009_weighted_occs_total_2d_no_tt.current_region_pssm_code_rollup =
dacso_q009_weighted_occs_2d_no_tt.current_region_pssm_code_rollup )
AND ( dacso_q009_weighted_occs_total_2d_no_tt.lcip2_cred =
dacso_q009_weighted_occs_2d_no_tt.lcip2_cred ); "



# ---- DACSO_Q010_Weighted_Occs_Dist_No_TT ----
DACSO_Q010_Weighted_Occs_Dist_No_TT <- 
"SELECT dacso_q009b_weighted_occs_total_no_tt.pssm_credential,
       dacso_q009b_weighted_occs_total_no_tt.pssm_cred,
       dacso_q009b_weighted_occs_total_no_tt.current_region_pssm_code_rollup,
       dacso_q009b_weighted_occs_total_no_tt.age_group_rollup,
       dacso_q009b_weighted_occs_total_no_tt.lcp4_cd,
       dacso_q009b_weighted_occs_total_no_tt.lcip4_cred,
       dacso_q009b_weighted_occs_total_no_tt.lcip2_cred,
       dacso_q009b_weighted_occs_no_tt.noc_cd,
       dacso_q009b_weighted_occs_no_tt.count,
       dacso_q009b_weighted_occs_total_no_tt.total,
       count / total AS perc_Dist
INTO DACSO_Q010_Weighted_Occs_Dist_No_TT
FROM   dacso_q009b_weighted_occs_total_no_tt
LEFT JOIN dacso_q009b_weighted_occs_no_tt
  ON  (dacso_q009b_weighted_occs_total_no_tt.lcip4_cred = cast(dacso_q009b_weighted_occs_no_tt.lcip4_cred as nvarchar(20)))
  AND (dacso_q009b_weighted_occs_total_no_tt.current_region_pssm_code_rollup = dacso_q009b_weighted_occs_no_tt.current_region_pssm_code_rollup )
  AND (dacso_q009b_weighted_occs_total_no_tt.age_group_rollup = dacso_q009b_weighted_occs_no_tt.age_group_rollup ); "
  


# ---- DACSO_Q010a0_Delete_Occupational_Distribution ----
DACSO_Q010a0_Delete_Occupational_Distribution <- 
"DELETE 
FROM Occupation_Distributions
WHERE (((Occupation_Distributions.Survey)='Student Outcomes'));"



# ---- DACSO_Q010a0_Delete_Occupational_Distribution_No_TT ----
DACSO_Q010a0_Delete_Occupational_Distribution_No_TT <- 
"DELETE 
FROM Occupation_Distributions_No_TT
WHERE (((Occupation_Distributions_No_TT.Survey)='Student Outcomes'));"



# ---- DACSO_Q010a0_Delete_Occupational_Distribution_No_TT_QI ----
DACSO_Q010a0_Delete_Occupational_Distribution_No_TT_QI <- 
"DELETE
FROM Occupation_Distributions_No_TT_QI
WHERE (((Occupation_Distributions_No_TT_QI.Survey)='Student Outcomes' Or (Occupation_Distributions_No_TT_QI.Survey)='PTIB'));"



# ---- DACSO_Q010a0_Delete_Occupational_Distribution_QI ----
DACSO_Q010a0_Delete_Occupational_Distribution_QI <- 
"DELETE 
FROM Occupation_Distributions_QI
WHERE (((Occupation_Distributions_QI.Survey)='Student Outcomes' Or (Occupation_Distributions_QI.Survey)='PTIB'));"




# ---- DACSO_Q010a1_Append_Occupational_Distribution ----
DACSO_Q010a1_Append_Occupational_Distribution <- 
"INSERT INTO Occupation_Distributions (Survey, PSSM_Credential, PSSM_CRED, 
  Current_Region_PSSM_Code_Rollup, Age_Group_Rollup, LCP4_CD, TTRAIN, LCIP4_CRED, LCIP2_CRED, NOC, [Count], Total, [Percent])
SELECT 'Student Outcomes' AS Survey, 
DACSO_Q010_Weighted_Occs_Dist.PSSM_Credential, 
DACSO_Q010_Weighted_Occs_Dist.PSSM_CRED, 
DACSO_Q010_Weighted_Occs_Dist.Current_Region_PSSM_Code_Rollup, 
DACSO_Q010_Weighted_Occs_Dist.Age_Group_Rollup, 
DACSO_Q010_Weighted_Occs_Dist.LCP4_CD, 
DACSO_Q010_Weighted_Occs_Dist.TTRAIN, 
DACSO_Q010_Weighted_Occs_Dist.LCIP4_CRED, 
DACSO_Q010_Weighted_Occs_Dist.LCIP2_CRED, 
DACSO_Q010_Weighted_Occs_Dist.NOC_CD, 
DACSO_Q010_Weighted_Occs_Dist.Count, 
DACSO_Q010_Weighted_Occs_Dist.Total, 
DACSO_Q010_Weighted_Occs_Dist.perc_Dist
FROM DACSO_Q010_Weighted_Occs_Dist;"



# ---- DACSO_Q010a1_Append_Occupational_Distribution_No_TT ----
DACSO_Q010a1_Append_Occupational_Distribution_No_TT <- 
"INSERT INTO Occupation_Distributions_No_TT ( Survey, PSSM_Credential, PSSM_CRED, Current_Region_PSSM_Code_Rollup, 
Age_Group_Rollup, LCP4_CD, LCIP4_CRED, LCIP2_CRED, NOC, [Count], Total, [Percent])
SELECT 'Student Outcomes' AS Survey, 
DACSO_Q010_Weighted_Occs_Dist_No_TT.PSSM_Credential, 
DACSO_Q010_Weighted_Occs_Dist_No_TT.PSSM_CRED, 
DACSO_Q010_Weighted_Occs_Dist_No_TT.Current_Region_PSSM_Code_Rollup, 
DACSO_Q010_Weighted_Occs_Dist_No_TT.Age_Group_Rollup, 
DACSO_Q010_Weighted_Occs_Dist_No_TT.LCP4_CD, 
DACSO_Q010_Weighted_Occs_Dist_No_TT.LCIP4_CRED, 
DACSO_Q010_Weighted_Occs_Dist_No_TT.LCIP2_CRED, 
DACSO_Q010_Weighted_Occs_Dist_No_TT.NOC_CD, 
DACSO_Q010_Weighted_Occs_Dist_No_TT.Count, 
DACSO_Q010_Weighted_Occs_Dist_No_TT.Total, 
DACSO_Q010_Weighted_Occs_Dist_No_TT.perc_Dist
FROM DACSO_Q010_Weighted_Occs_Dist_No_TT;"



# ---- DACSO_Q010b0_Delete_Occupational_Distribution_LCP2 ----
DACSO_Q010b0_Delete_Occupational_Distribution_LCP2 <- 
"DELETE 
FROM Occupation_Distributions_LCP2
WHERE (((Occupation_Distributions_LCP2.Survey)='Student Outcomes'));"



# ---- DACSO_Q010b0_Delete_Occupational_Distribution_LCP2_No_TT ----
DACSO_Q010b0_Delete_Occupational_Distribution_LCP2_No_TT <- 
"DELETE 
FROM Occupation_Distributions_LCP2_No_TT
WHERE (((Occupation_Distributions_LCP2_No_TT.Survey)='Student Outcomes'));"



# ---- DACSO_Q010b0_Delete_Occupational_Distribution_LCP2_No_TT_QI ----
DACSO_Q010b0_Delete_Occupational_Distribution_LCP2_No_TT_QI <- 
"DELETE 
FROM Occupation_Distributions_LCP2_No_TT_QI
WHERE (((Occupation_Distributions_LCP2_No_TT_QI.Survey)='Student Outcomes' Or (Occupation_Distributions_LCP2_No_TT_QI.Survey)='PTIB'));"



# ---- DACSO_Q010b0_Delete_Occupational_Distribution_LCP2_QI ----
DACSO_Q010b0_Delete_Occupational_Distribution_LCP2_QI <- 
"DELETE 
FROM Occupation_Distributions_LCP2_QI
WHERE (((Occupation_Distributions_LCP2_QI.Survey)='Student Outcomes' Or (Occupation_Distributions_LCP2_QI.Survey)='PTIB'));"



# ---- DACSO_Q010b1_Append_Occupational_Distribution_LCP2 ----
DACSO_Q010b1_Append_Occupational_Distribution_LCP2 <- 
"INSERT INTO Occupation_Distributions_LCP2 ( Survey, PSSM_Credential, PSSM_CRED, Current_Region_PSSM_Code_Rollup, Age_Group_Rollup, LCP2_CD, TTRAIN, LCIP2_CRED, NOC, [Count], Total, [Percent] )
SELECT 'Student Outcomes' AS Survey, DACSO_Q010_Weighted_Occs_Dist_2D.PSSM_Credential, DACSO_Q010_Weighted_Occs_Dist_2D.PSSM_CRED, DACSO_Q010_Weighted_Occs_Dist_2D.Current_Region_PSSM_Code_Rollup, DACSO_Q010_Weighted_Occs_Dist_2D.Age_Group_Rollup, DACSO_Q010_Weighted_Occs_Dist_2D.LCP2_CD, DACSO_Q010_Weighted_Occs_Dist_2D.TTRAIN, DACSO_Q010_Weighted_Occs_Dist_2D.LCIP2_CRED, DACSO_Q010_Weighted_Occs_Dist_2D.NOC_CD, DACSO_Q010_Weighted_Occs_Dist_2D.Count, DACSO_Q010_Weighted_Occs_Dist_2D.Total, DACSO_Q010_Weighted_Occs_Dist_2D.perc_Dist
FROM DACSO_Q010_Weighted_Occs_Dist_2D;"



# ---- DACSO_Q010b1_Append_Occupational_Distribution_LCP2_No_TT ----
DACSO_Q010b1_Append_Occupational_Distribution_LCP2_No_TT <- 
"INSERT INTO Occupation_Distributions_LCP2_No_TT ( Survey, PSSM_Credential, PSSM_CRED, Current_Region_PSSM_Code_Rollup, Age_Group_Rollup, LCP2_CD, LCIP2_CRED, NOC, [Count], Total, [Percent] )
SELECT 'Student Outcomes' AS Survey, DACSO_Q010_Weighted_Occs_Dist_2D_No_TT.PSSM_Credential, DACSO_Q010_Weighted_Occs_Dist_2D_No_TT.PSSM_CRED, DACSO_Q010_Weighted_Occs_Dist_2D_No_TT.Current_Region_PSSM_Code_Rollup, DACSO_Q010_Weighted_Occs_Dist_2D_No_TT.Age_Group_Rollup, DACSO_Q010_Weighted_Occs_Dist_2D_No_TT.LCP2_CD, DACSO_Q010_Weighted_Occs_Dist_2D_No_TT.LCIP2_CRED, DACSO_Q010_Weighted_Occs_Dist_2D_No_TT.NOC_CD, DACSO_Q010_Weighted_Occs_Dist_2D_No_TT.Count, DACSO_Q010_Weighted_Occs_Dist_2D_No_TT.Total, DACSO_Q010_Weighted_Occs_Dist_2D_No_TT.perc_Dist
FROM DACSO_Q010_Weighted_Occs_Dist_2D_No_TT;"



# ---- DACSO_Q010c0_Delete_Occupational_Distribution_LCP2_BC ----
DACSO_Q010c0_Delete_Occupational_Distribution_LCP2_BC <- 
"DELETE 
FROM Occupation_Distributions_LCP2_BC
WHERE (((Occupation_Distributions_LCP2_BC.Survey)='Student Outcomes'));"



# ---- DACSO_Q010c0_Delete_Occupational_Distribution_LCP2_BC_No_TT ----
DACSO_Q010c0_Delete_Occupational_Distribution_LCP2_BC_No_TT <- 
"DELETE 
FROM Occupation_Distributions_LCP2_BC_No_TT
WHERE (((Occupation_Distributions_LCP2_BC_No_TT.Survey)='Student Outcomes'));"



# ---- DACSO_Q010c0_Delete_Occupational_Distribution_LCP2_BC_No_TT_QI ----
DACSO_Q010c0_Delete_Occupational_Distribution_LCP2_BC_No_TT_QI <- 
"DELETE 
FROM Occupation_Distributions_LCP2_BC_No_TT_QI
WHERE (((Occupation_Distributions_LCP2_BC_No_TT_QI.Survey)='Student Outcomes' Or (Occupation_Distributions_LCP2_BC_No_TT_QI.Survey)='PTIB'));"



# ---- DACSO_Q010c0_Delete_Occupational_Distribution_LCP2_BC_QI ----
DACSO_Q010c0_Delete_Occupational_Distribution_LCP2_BC_QI <- 
"DELETE 
FROM Occupation_Distributions_LCP2_BC_QI
WHERE (((Occupation_Distributions_LCP2_BC_QI.Survey)='Student Outcomes' Or (Occupation_Distributions_LCP2_BC_QI.Survey)='PTIB'));"



# ---- DACSO_Q010c1_Append_Occupational_Distribution_LCP2_BC ----
DACSO_Q010c1_Append_Occupational_Distribution_LCP2_BC <- 
"INSERT INTO Occupation_Distributions_LCP2_BC ( Survey, PSSM_Credential, PSSM_CRED, LCP2_CD, TTRAIN, LCIP2_CRED, NOC, [Count], Total, [Percent] )
SELECT 'Student Outcomes' AS Survey, DACSO_Q010_Weighted_Occs_Dist_2D_BC.PSSM_Credential, DACSO_Q010_Weighted_Occs_Dist_2D_BC.PSSM_CRED, DACSO_Q010_Weighted_Occs_Dist_2D_BC.LCP2_CD, DACSO_Q010_Weighted_Occs_Dist_2D_BC.TTRAIN, DACSO_Q010_Weighted_Occs_Dist_2D_BC.LCIP2_CRED, DACSO_Q010_Weighted_Occs_Dist_2D_BC.NOC_CD, DACSO_Q010_Weighted_Occs_Dist_2D_BC.Count, DACSO_Q010_Weighted_Occs_Dist_2D_BC.Total, DACSO_Q010_Weighted_Occs_Dist_2D_BC.perc_Dist
FROM DACSO_Q010_Weighted_Occs_Dist_2D_BC;"



# ---- DACSO_Q010c1_Append_Occupational_Distribution_LCP2_BC_No_TT ----
DACSO_Q010c1_Append_Occupational_Distribution_LCP2_BC_No_TT <- 
"INSERT INTO Occupation_Distributions_LCP2_BC_No_TT ( Survey, PSSM_Credential, PSSM_CRED, LCP2_CD, LCIP2_CRED, NOC, [Count], Total, [Percent] )
SELECT 'Student Outcomes' AS Survey, DACSO_Q010_Weighted_Occs_Dist_2D_BC_No_TT.PSSM_Credential, DACSO_Q010_Weighted_Occs_Dist_2D_BC_No_TT.PSSM_CRED, DACSO_Q010_Weighted_Occs_Dist_2D_BC_No_TT.LCP2_CD, DACSO_Q010_Weighted_Occs_Dist_2D_BC_No_TT.LCIP2_CRED, DACSO_Q010_Weighted_Occs_Dist_2D_BC_No_TT.NOC_CD, DACSO_Q010_Weighted_Occs_Dist_2D_BC_No_TT.Count, DACSO_Q010_Weighted_Occs_Dist_2D_BC_No_TT.Total, DACSO_Q010_Weighted_Occs_Dist_2D_BC_No_TT.perc_Dist
FROM DACSO_Q010_Weighted_Occs_Dist_2D_BC_No_TT;"

# ---- Q010d1 - DACSO_Q010e PDEG Law Modifications ----


# ---- DACSO_Q010d1_Delete_PDEG_CIP_Cluster_07_Law_New_Labour_Supply ----
DACSO_Q010d1_Delete_PDEG_CIP_Cluster_07_Law_New_Labour_Supply <- 
"DELETE 
FROM Labour_Supply_Distribution
WHERE (((Labour_Supply_Distribution.Survey)='2021 Census PSSM 2023-2024') 
AND   ((Labour_Supply_Distribution.PSSM_Credential)='PDEG') 
AND   ((Labour_Supply_Distribution.LCP4_CD)='07'));"



# ---- DACSO_Q010d1_Delete_PDEG_CIP_Cluster_07_Law_New_Labour_Supply_QI ----
DACSO_Q010d1_Delete_PDEG_CIP_Cluster_07_Law_New_Labour_Supply_QI <- 
"DELETE 
FROM Labour_Supply_Distribution_QI
WHERE (((Labour_Supply_Distribution_QI.Survey)='2021 Census PSSM 2023-2024') 
AND ((Labour_Supply_Distribution_QI.PSSM_Credential)='PDEG') 
AND ((Labour_Supply_Distribution_QI.LCP4_CD)='07'));"



# ---- DACSO_Q010d2_NLS_PDEG_07_Count ----
DACSO_Q010d2_NLS_PDEG_07_Count <- 
"SELECT Labour_Supply_Distribution.Survey, 'PDEG' AS PSSM_Credential, 'PDEG' AS PSSM_CRED, '07' AS LCP4_CD, Labour_Supply_Distribution.TTRAIN, '07 - PDEG' AS LCIP4_CRED, 
Labour_Supply_Distribution.Current_Region_PSSM_Code_Rollup, Labour_Supply_Distribution.Age_Group_Rollup, Sum(Labour_Supply_Distribution.Count) AS Count
INTO DACSO_Q010d2_NLS_PDEG_07_Count
FROM Labour_Supply_Distribution
WHERE (((Left(LCP4_CD,2))=22) AND ((Labour_Supply_Distribution.PSSM_Credential)='BACH'))
GROUP BY Labour_Supply_Distribution.Survey, Labour_Supply_Distribution.TTRAIN, Labour_Supply_Distribution.Current_Region_PSSM_Code_Rollup, Labour_Supply_Distribution.Age_Group_Rollup
HAVING (((Labour_Supply_Distribution.Survey)='Student Outcomes'));"



# ---- DACSO_Q010d3_NLS_PDEG_07_Subtotal ----
DACSO_Q010d3_NLS_PDEG_07_Subtotal <- 
"SELECT Labour_Supply_Distribution.Survey, 'PDEG' AS PSSM_Credential, 'PDEG' AS PSSM_CRED, '07' AS LCP4_CD, Labour_Supply_Distribution.TTRAIN, '07 - PDEG' AS LCIP4_CRED, 
Labour_Supply_Distribution.Age_Group_Rollup, Labour_Supply_Distribution.Total AS Subtotal
INTO DACSO_Q010d3_NLS_PDEG_07_Subtotal
FROM Labour_Supply_Distribution
WHERE (((Left(LCP4_CD,2))=22) AND ((Labour_Supply_Distribution.PSSM_Credential)='BACH'))
GROUP BY Labour_Supply_Distribution.Survey, Labour_Supply_Distribution.TTRAIN, Labour_Supply_Distribution.Age_Group_Rollup, Labour_Supply_Distribution.Total
HAVING (((Labour_Supply_Distribution.Survey)='Student Outcomes'));"



# ---- DACSO_Q010d4_NLS_PDEG_07_Total ----
DACSO_Q010d4_NLS_PDEG_07_Total <- 
"SELECT DACSO_Q010d3_NLS_PDEG_07_Subtotal.Survey, DACSO_Q010d3_NLS_PDEG_07_Subtotal.PSSM_Credential, DACSO_Q010d3_NLS_PDEG_07_Subtotal.PSSM_CRED, 
DACSO_Q010d3_NLS_PDEG_07_Subtotal.LCP4_CD, DACSO_Q010d3_NLS_PDEG_07_Subtotal.TTRAIN, DACSO_Q010d3_NLS_PDEG_07_Subtotal.LCIP4_CRED, 
DACSO_Q010d3_NLS_PDEG_07_Subtotal.Age_Group_Rollup, Sum(DACSO_Q010d3_NLS_PDEG_07_Subtotal.Subtotal) AS Total
INTO DACSO_Q010d4_NLS_PDEG_07_Total
FROM DACSO_Q010d3_NLS_PDEG_07_Subtotal
GROUP BY DACSO_Q010d3_NLS_PDEG_07_Subtotal.Survey, DACSO_Q010d3_NLS_PDEG_07_Subtotal.PSSM_Credential, 
DACSO_Q010d3_NLS_PDEG_07_Subtotal.PSSM_CRED, 
DACSO_Q010d3_NLS_PDEG_07_Subtotal.LCP4_CD, DACSO_Q010d3_NLS_PDEG_07_Subtotal.TTRAIN, 
DACSO_Q010d3_NLS_PDEG_07_Subtotal.LCIP4_CRED, DACSO_Q010d3_NLS_PDEG_07_Subtotal.Age_Group_Rollup;"



# ---- DACSO_Q010d5_NLS_PDEG_07_Weighted_New_Labour_Supply ----
DACSO_Q010d5_NLS_PDEG_07_Weighted_New_Labour_Supply <- 
"SELECT DACSO_Q010d4_NLS_PDEG_07_Total.Survey, DACSO_Q010d2_NLS_PDEG_07_Count.PSSM_Credential, DACSO_Q010d2_NLS_PDEG_07_Count.PSSM_CRED, 
DACSO_Q010d2_NLS_PDEG_07_Count.Current_Region_PSSM_Code_Rollup, DACSO_Q010d2_NLS_PDEG_07_Count.Age_Group_Rollup, DACSO_Q010d2_NLS_PDEG_07_Count.LCP4_CD, 
DACSO_Q010d4_NLS_PDEG_07_Total.TTRAIN, DACSO_Q010d4_NLS_PDEG_07_Total.LCIP4_CRED, DACSO_Q010d2_NLS_PDEG_07_Count.Count, DACSO_Q010d4_NLS_PDEG_07_Total.Total, 
ISNULL(Count,0)/Total AS perc
INTO DACSO_Q010d5_NLS_PDEG_07_Weighted_New_Labour_Supply
FROM DACSO_Q010d4_NLS_PDEG_07_Total 
LEFT JOIN DACSO_Q010d2_NLS_PDEG_07_Count ON (DACSO_Q010d4_NLS_PDEG_07_Total.LCIP4_CRED = DACSO_Q010d2_NLS_PDEG_07_Count.LCIP4_CRED) 
AND (DACSO_Q010d4_NLS_PDEG_07_Total.Age_Group_Rollup = DACSO_Q010d2_NLS_PDEG_07_Count.Age_Group_Rollup) 
AND (DACSO_Q010d4_NLS_PDEG_07_Total.Survey = DACSO_Q010d2_NLS_PDEG_07_Count.Survey)
WHERE (((DACSO_Q010d2_NLS_PDEG_07_Count.Current_Region_PSSM_Code_Rollup) Is Not Null));"



# ---- DACSO_Q010d6_Append_NLS_PDEG_07_New_Labour_Supply ----
DACSO_Q010d6_Append_NLS_PDEG_07_New_Labour_Supply <- 
"INSERT INTO Labour_Supply_Distribution ( Survey, PSSM_Credential, PSSM_CRED, Current_Region_PSSM_Code_Rollup, Age_Group_Rollup, LCP4_CD, TTRAIN, LCIP4_CRED, Count, Total, New_Labour_Supply )
SELECT DACSO_Q010d5_NLS_PDEG_07_Weighted_New_Labour_Supply.Survey, DACSO_Q010d5_NLS_PDEG_07_Weighted_New_Labour_Supply.PSSM_Credential, DACSO_Q010d5_NLS_PDEG_07_Weighted_New_Labour_Supply.PSSM_CRED, DACSO_Q010d5_NLS_PDEG_07_Weighted_New_Labour_Supply.Current_Region_PSSM_Code_Rollup, DACSO_Q010d5_NLS_PDEG_07_Weighted_New_Labour_Supply.Age_Group_Rollup, DACSO_Q010d5_NLS_PDEG_07_Weighted_New_Labour_Supply.LCP4_CD, DACSO_Q010d5_NLS_PDEG_07_Weighted_New_Labour_Supply.TTRAIN, DACSO_Q010d5_NLS_PDEG_07_Weighted_New_Labour_Supply.LCIP4_CRED, DACSO_Q010d5_NLS_PDEG_07_Weighted_New_Labour_Supply.Count, DACSO_Q010d5_NLS_PDEG_07_Weighted_New_Labour_Supply.Total, DACSO_Q010d5_NLS_PDEG_07_Weighted_New_Labour_Supply.perc
FROM DACSO_Q010d5_NLS_PDEG_07_Weighted_New_Labour_Supply;"



# ---- DACSO_Q010e1_Delete_PDEG_CIP_Cluster_07_Law_Occupation_Dist ----
DACSO_Q010e1_Delete_PDEG_CIP_Cluster_07_Law_Occupation_Dist <- 
"DELETE  
FROM Occupation_Distributions
WHERE (((Occupation_Distributions.Survey)='2021 Census PSSM 2022-2023') 
AND ((Occupation_Distributions.PSSM_Credential)='PDEG') AND ((Occupation_Distributions.LCP4_CD)='07'));"



# ---- DACSO_Q010e1_Delete_PDEG_CIP_Cluster_07_Law_Occupation_Dist_QI ----
DACSO_Q010e1_Delete_PDEG_CIP_Cluster_07_Law_Occupation_Dist_QI <- 
"DELETE  
FROM Occupation_Distributions_QI
WHERE (((Occupation_Distributions_QI.Survey)='2021 Census PSSM 2022-2023') 
AND ((Occupation_Distributions_QI.PSSM_Credential)='PDEG') AND ((Occupation_Distributions_QI.LCP4_CD)='07'));"



# ---- DACSO_Q010e2_Weighted_Occs_PDEG_07 ----
DACSO_Q010e2_Weighted_Occs_PDEG_07 <- 
"SELECT Occupation_Distributions.Survey, 'PDEG' AS PSSM_Credential, 'PDEG' AS PSSM_CRED, '07' AS LCP4_CD, Occupation_Distributions.TTRAIN, '07 - PDEG' AS LCIP4_CRED, Occupation_Distributions.Current_Region_PSSM_Code_Rollup, Occupation_Distributions.Age_Group_Rollup, Occupation_Distributions.NOC, Sum(Occupation_Distributions.Count) AS Count
INTO DACSO_Q010e2_Weighted_Occs_PDEG_07
FROM Occupation_Distributions
WHERE (((Left(LCP4_CD,2))=22) AND ((Occupation_Distributions.PSSM_Credential)='BACH'))
GROUP BY Occupation_Distributions.Survey, Occupation_Distributions.TTRAIN, Occupation_Distributions.Current_Region_PSSM_Code_Rollup, 
Occupation_Distributions.Age_Group_Rollup, Occupation_Distributions.NOC
HAVING (((Occupation_Distributions.Survey)='Student Outcomes'));"



# ---- DACSO_Q010e3_Weighted_Occs_Total_PDEG_07 ----
DACSO_Q010e3_Weighted_Occs_Total_PDEG_07 <- 
"SELECT DACSO_Q010e2_Weighted_Occs_PDEG_07.Survey, DACSO_Q010e2_Weighted_Occs_PDEG_07.PSSM_Credential, 
DACSO_Q010e2_Weighted_Occs_PDEG_07.PSSM_CRED, 
DACSO_Q010e2_Weighted_Occs_PDEG_07.Current_Region_PSSM_Code_Rollup, 
DACSO_Q010e2_Weighted_Occs_PDEG_07.Age_Group_Rollup, DACSO_Q010e2_Weighted_Occs_PDEG_07.LCP4_CD, 
DACSO_Q010e2_Weighted_Occs_PDEG_07.TTRAIN, DACSO_Q010e2_Weighted_Occs_PDEG_07.LCIP4_CRED, 
Sum(DACSO_Q010e2_Weighted_Occs_PDEG_07.Count) AS Total
INTO DACSO_Q010e3_Weighted_Occs_Total_PDEG_07
FROM DACSO_Q010e2_Weighted_Occs_PDEG_07
GROUP BY DACSO_Q010e2_Weighted_Occs_PDEG_07.Survey, DACSO_Q010e2_Weighted_Occs_PDEG_07.PSSM_Credential, 
DACSO_Q010e2_Weighted_Occs_PDEG_07.PSSM_CRED, DACSO_Q010e2_Weighted_Occs_PDEG_07.Current_Region_PSSM_Code_Rollup, 
DACSO_Q010e2_Weighted_Occs_PDEG_07.Age_Group_Rollup, DACSO_Q010e2_Weighted_Occs_PDEG_07.LCP4_CD, 
DACSO_Q010e2_Weighted_Occs_PDEG_07.TTRAIN, DACSO_Q010e2_Weighted_Occs_PDEG_07.LCIP4_CRED
ORDER BY DACSO_Q010e2_Weighted_Occs_PDEG_07.LCIP4_CRED;"



# ---- DACSO_Q010e4_Weighted_Occs_Dist_PDEG_07 ----
DACSO_Q010e4_Weighted_Occs_Dist_PDEG_07 <- 
"SELECT DACSO_Q010e3_Weighted_Occs_Total_PDEG_07.Survey, DACSO_Q010e3_Weighted_Occs_Total_PDEG_07.PSSM_Credential, 
DACSO_Q010e3_Weighted_Occs_Total_PDEG_07.PSSM_CRED, DACSO_Q010e3_Weighted_Occs_Total_PDEG_07.Current_Region_PSSM_Code_Rollup, 
DACSO_Q010e3_Weighted_Occs_Total_PDEG_07.Age_Group_Rollup, DACSO_Q010e3_Weighted_Occs_Total_PDEG_07.LCP4_CD, 
DACSO_Q010e3_Weighted_Occs_Total_PDEG_07.TTRAIN, DACSO_Q010e3_Weighted_Occs_Total_PDEG_07.LCIP4_CRED, DACSO_Q010e2_Weighted_Occs_PDEG_07.NOC, 
DACSO_Q010e2_Weighted_Occs_PDEG_07.Count, DACSO_Q010e3_Weighted_Occs_Total_PDEG_07.Total, Count/Total AS perc_Dist
INTO DACSO_Q010e4_Weighted_Occs_Dist_PDEG_07
FROM DACSO_Q010e3_Weighted_Occs_Total_PDEG_07 LEFT JOIN DACSO_Q010e2_Weighted_Occs_PDEG_07 ON (DACSO_Q010e3_Weighted_Occs_Total_PDEG_07.Survey = DACSO_Q010e2_Weighted_Occs_PDEG_07.Survey) AND (DACSO_Q010e3_Weighted_Occs_Total_PDEG_07.Current_Region_PSSM_Code_Rollup = DACSO_Q010e2_Weighted_Occs_PDEG_07.Current_Region_PSSM_Code_Rollup) AND (DACSO_Q010e3_Weighted_Occs_Total_PDEG_07.Age_Group_Rollup = DACSO_Q010e2_Weighted_Occs_PDEG_07.Age_Group_Rollup) AND (DACSO_Q010e3_Weighted_Occs_Total_PDEG_07.LCIP4_CRED = DACSO_Q010e2_Weighted_Occs_PDEG_07.LCIP4_CRED);"



# ---- DACSO_Q010e5_Append_Occupational_Distribution_PDEG_07 ----
DACSO_Q010e5_Append_Occupational_Distribution_PDEG_07 <- 
"INSERT INTO Occupation_Distributions ( Survey, PSSM_Credential, PSSM_CRED, Current_Region_PSSM_Code_Rollup, 
Age_Group_Rollup, LCP4_CD, TTRAIN, LCIP4_CRED, NOC, Count, Total, [Percent])
SELECT DACSO_Q010e4_Weighted_Occs_Dist_PDEG_07.Survey, DACSO_Q010e4_Weighted_Occs_Dist_PDEG_07.PSSM_Credential, 
DACSO_Q010e4_Weighted_Occs_Dist_PDEG_07.PSSM_CRED, DACSO_Q010e4_Weighted_Occs_Dist_PDEG_07.Current_Region_PSSM_Code_Rollup, 
DACSO_Q010e4_Weighted_Occs_Dist_PDEG_07.Age_Group_Rollup, DACSO_Q010e4_Weighted_Occs_Dist_PDEG_07.LCP4_CD, 
DACSO_Q010e4_Weighted_Occs_Dist_PDEG_07.TTRAIN, DACSO_Q010e4_Weighted_Occs_Dist_PDEG_07.LCIP4_CRED, 
DACSO_Q010e4_Weighted_Occs_Dist_PDEG_07.NOC, DACSO_Q010e4_Weighted_Occs_Dist_PDEG_07.Count, 
DACSO_Q010e4_Weighted_Occs_Dist_PDEG_07.Total, DACSO_Q010e4_Weighted_Occs_Dist_PDEG_07.perc_Dist
FROM DACSO_Q010e4_Weighted_Occs_Dist_PDEG_07;"



# ---- DACSO_Q99A_ENDDT_IMPUTED ----
DACSO_Q99A_ENDDT_IMPUTED <- 
"UPDATE T_Cohorts_Recoded SET T_Cohorts_Recoded.ENDDT = (Survey_year-2) + '-12'
WHERE (((T_Cohorts_Recoded.ENDDT) Is Null) AND ((T_Cohorts_Recoded.Survey)='DACSO'));"

#DACSO_Q99A_ENDDT - NOT USED <- 
#"UPDATE (INFOWARE_SURV_COHORT_COLLECTION_INFO INNER JOIN DACSO_Q99A_STQUI_ID ON INFOWARE_SURV_COHORT_COLLECTION_INFO.COCI_STQU_ID = DACSO_Q99A_STQUI_ID.COCI_STQU_ID) INNER JOIN T_Cohorts_Recoded ON DACSO_Q99A_STQUI_ID.STQU_ID_Only=T_Cohorts_Recoded.STQU_ID SET T_Cohorts_Recoded.ENDDT = INFOWARE_SURV_COHORT_COLLECTION_INFO.COSC_ENRL_END_DATE
#WHERE (((T_Cohorts_Recoded.Survey)='DACSO'));"

#DACSO_Q99A_STQUI_ID - NOT USED <- 
#"SELECT T_Cohorts_Recoded.STQU_ID AS Survey_STQU_ID, T_Cohorts_Recoded.Survey, CDbl(Right(STQU_ID,Len(STQU_ID)-InStr(1,STQU_ID,"-")-1)) AS STQU_ID_Only
#FROM T_Cohorts_Recoded;"



# ---- DACSO_qry99_Suppression_Public_Release_NOC ----
DACSO_qry99_Suppression_Public_Release_NOC <- 
"SELECT T_Cohorts_Recoded.Age_Group_Rollup, 
tbl_Age_Groups_Rollup.Age_Group_Rollup_Label, 
T_Cohorts_Recoded.NOC_CD, 
Count(*) AS Expr1 INTO T_Suppression_Public_Release_NOC
FROM T_Cohorts_Recoded 
INNER JOIN tbl_Age_Groups_Rollup ON T_Cohorts_Recoded.Age_Group_Rollup = tbl_Age_Groups_Rollup.Age_Group_Rollup
WHERE (((T_Cohorts_Recoded.Weight)>0))
GROUP BY T_Cohorts_Recoded.Age_Group_Rollup, tbl_Age_Groups_Rollup.Age_Group_Rollup_Label, T_Cohorts_Recoded.NOC_CD
HAVING (((T_Cohorts_Recoded.Age_Group_Rollup) Is Not Null) AND ((Count(*))<5))
ORDER BY Count(*);"


