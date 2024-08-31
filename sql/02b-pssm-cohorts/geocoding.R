qry_000_TRD_Current_Region_Data <- 
"SELECT trades_cohort_info.KEY,
       trades_cohort_info.subm_cd,
       trades_cohort_info.respondent,
       trades_cohort_info.new_postal_code,
       trades_cohort_info.current_region1,
       trades_cohort_info.current_region2,
       trades_cohort_info.current_region3,
       trades_cohort_info.current_region4
FROM   trades_cohort_info
WHERE subm_cd IN ('C_Outc14', 'C_Outc15', 'C_Outc16', 'C_Outc17', 'C_Outc18', 'C_Outc19', 'C_Outc20', 'C_Outc21')"


qry_000_TRD_Update_Current_Region_PSSM_step1 <- "
UPDATE trd_current_region_data
SET    trd_current_region_data.current_region_pssm_code = trd_current_region_data.current_region1
WHERE   trd_current_region_data.current_region_pssm_code IS NULL 
AND trd_current_region_data.current_region1 >= 1
AND trd_current_region_data.current_region1 <= 8;"

qry_000_TRD_Update_Current_Region_PSSM_step2 <- "
UPDATE  trd_current_region_data
SET     trd_current_region_data.current_region_pssm_code = 
        CASE WHEN current_region4 = 5 THEN 9
             WHEN current_region4 = 6 THEN 10
             WHEN current_region4 = 7 THEN 11
             WHEN current_region4 = 8 THEN -1
             ELSE NULL
        END
WHERE   trd_current_region_data.current_region_pssm_code IS NULL;"

qry_000_TRD_results <- "
SELECT current_region_pssm_code, [C_Outc14], [C_Outc15], [C_Outc16], [C_Outc17], [C_Outc18], [C_Outc19]
FROM (
	SELECT current_region_pssm_code, -- implicit group by column, result will have one row for each distinct value
	       [key], --column being aggregated
		     SUBM_CD --column that contains the values that will become column headers
	FROM   trd_current_region_data
	WHERE  respondent = '1') P
PIVOT (Count([key]) FOR SUBM_CD IN ([C_Outc14],[C_Outc15],[C_Outc16],[C_Outc17],[C_Outc18],[C_Outc19])) AS PVT
ORDER BY cast(current_region_pssm_code AS INT)"

qry_APPSO_Current_Region_Data <- 
"SELECT apprentice_cohort_info.KEY,
       apprentice_cohort_info.subm_cd,
       apprentice_cohort_info.respondent,
       apprentice_cohort_info.lrst_cd,
       apprentice_cohort_info.postal_code_new,
       apprentice_cohort_info.current_region1,
       apprentice_cohort_info.current_region2,
       apprentice_cohort_info.current_region3,
       apprentice_cohort_info.current_region4
FROM   apprentice_cohort_info
WHERE  apprentice_cohort_info.subm_cd IN ('C_Outc18', 'C_Outc19')"

qry_APPSO_Update_Current_Region_PSSM_step1 <- "
UPDATE appso_current_region_data
SET    appso_current_region_data.current_region_pssm_code = appso_current_region_data.current_region1
WHERE  appso_current_region_data.current_region_pssm_code IS NULL
  AND  appso_current_region_data.current_region1 >= 1
  AND  appso_current_region_data.current_region1 <= 8
  AND appso_current_region_data.subm_cd IN ('C_Outc18', 'C_Outc19')"

qry_APPSO_Update_Current_Region_PSSM_step2 <- "
UPDATE appso_current_region_data
SET    appso_current_region_data.current_region_pssm_code = 
       CASE WHEN current_region4 = 5 THEN 9
            WHEN current_region4 = 6 THEN 10
            WHEN current_region4 = 7 THEN 11
            WHEN current_region4 = 8 THEN -1
            ELSE NULL
       END 
WHERE  appso_current_region_data.current_region_pssm_code IS NULL
AND    appso_current_region_data.subm_cd IN ('C_Outc18', 'C_Outc19')"

qry_APPSO100_Unknown_2016 <- 
"INSERT INTO Unknown_Geocoding_Investigation ( stqu_id, survey_year, INST, CURRENT_REGION3_CODE, Survey, ADDRESS_Cohort_Raw, OPHONE1_Cohort_Raw, OPHONE2_Cohort_Raw, PHONE_Cohort_Raw, POSTAL_Cohort_Raw, NEW_POST, [Research Results], [New Phone1], [Phone Status1], [Phone category1], [New Phone2], [Phone Status2], [Phone category2], [New Phone3], [Phone Status3], [Phone category3], [New Phone4], [Phone Status4], [Phone category4], [New Phone5], [Phone Status5], [Phone category5], [New Phone6], [Phone Status6], [Phone category6], CURRENT_REGION_PSSM_CODE )
SELECT APPSO_Current_Region_Data.KEY, APPRENTICE_COHORT_INFO.SUBM_CD, APPRENTICE_COHORT_INFO.INST, APPRENTICE_COHORT_INFO.CURRENT_REGION3, 'APPSO' AS Expr1, APPRENTICE_COHORT_INFO.ADDRESS, APPRENTICE_COHORT_INFO.OPHONE1, APPRENTICE_COHORT_INFO.OPHONE2, APPRENTICE_COHORT_INFO.PHONE, APPRENTICE_COHORT_INFO.POSTAL, IIf(IsNull([APPRENTICE_COHORT_INFO].[POSTAL_CODE_NEW]),[Q59],[APPRENTICE_COHORT_INFO].[POSTAL_CODE_NEW]) AS Expr2, APPSO_sourcing_2016.[Research Results], APPSO_sourcing_2016.[New Phone1], APPSO_sourcing_2016.[Phone Status1], APPSO_sourcing_2016.[Phone category1], APPSO_sourcing_2016.[New Phone2], APPSO_sourcing_2016.[Phone Status2], APPSO_sourcing_2016.[Phone category2], APPSO_sourcing_2016.[New Phone3], APPSO_sourcing_2016.[Phone Status3], APPSO_sourcing_2016.[Phone category3], APPSO_sourcing_2016.[New Phone4], APPSO_sourcing_2016.[Phone Status4], APPSO_sourcing_2016.[Phone category4], APPSO_sourcing_2016.[New Phone5], APPSO_sourcing_2016.[Phone Status5], APPSO_sourcing_2016.[Phone category5], APPSO_sourcing_2016.[New Phone6], APPSO_sourcing_2016.[Phone Status6], APPSO_sourcing_2016.[Phone category6], APPSO_Current_Region_Data.CURRENT_REGION_PSSM_CODE
FROM APPSO_Current_Region_Data INNER JOIN ((APPRENTICE_COHORT_INFO LEFT JOIN APPSO_sourcing_2016 ON APPRENTICE_COHORT_INFO.KEY = APPSO_sourcing_2016.KEY) LEFT JOIN APPSO_long_response_2016 ON APPRENTICE_COHORT_INFO.KEY = APPSO_long_response_2016.KEY) ON APPSO_Current_Region_Data.KEY = APPRENTICE_COHORT_INFO.KEY
WHERE (((APPRENTICE_COHORT_INFO.SUBM_CD)='C_Outc16') AND ((APPSO_Current_Region_Data.CURRENT_REGION_PSSM_CODE)=-1 Or (APPSO_Current_Region_Data.CURRENT_REGION_PSSM_CODE)=9) AND ((APPRENTICE_COHORT_INFO.RESPONDENT)='1'));"

qry_APPSO100_Unknown_2017 <- 
"INSERT INTO Unknown_Geocoding_Investigation ( stqu_id, survey_year, INST, CURRENT_REGION3_CODE, Survey, ADDRESS_Cohort_Raw, OPHONE1_Cohort_Raw, OPHONE2_Cohort_Raw, PHONE_Cohort_Raw, POSTAL_Cohort_Raw, NEW_POST, [Research Results], [New Phone1], [Phone Status1], [Phone category1], CURRENT_REGION_PSSM_CODE )
SELECT APPSO_Current_Region_Data.KEY, APPRENTICE_COHORT_INFO.SUBM_CD, APPRENTICE_COHORT_INFO.INST, APPRENTICE_COHORT_INFO.CURRENT_REGION3, 'APPSO' AS Expr1, APPRENTICE_COHORT_INFO.ADDRESS, APPRENTICE_COHORT_INFO.OPHONE1, APPRENTICE_COHORT_INFO.OPHONE2, APPRENTICE_COHORT_INFO.PHONE, APPRENTICE_COHORT_INFO.POSTAL, IIf(IsNull([APPRENTICE_COHORT_INFO].[POSTAL_CODE_NEW]),[Q59],[APPRENTICE_COHORT_INFO].[POSTAL_CODE_NEW]) AS Expr2, APPSO_sourcing_2017.[Reseach Found], APPSO_sourcing_2017.[New Phone], APPSO_sourcing_2017.[Phone Status], APPSO_sourcing_2017.[Phone Category], APPSO_Current_Region_Data.CURRENT_REGION_PSSM_CODE
FROM ((APPSO_Current_Region_Data INNER JOIN APPRENTICE_COHORT_INFO ON APPSO_Current_Region_Data.KEY = APPRENTICE_COHORT_INFO.KEY) LEFT JOIN APPSO_sourcing_2017 ON APPRENTICE_COHORT_INFO.KEY = APPSO_sourcing_2017.key) LEFT JOIN APPSO_long_responses_2017 ON APPRENTICE_COHORT_INFO.KEY = APPSO_long_responses_2017.KEY
WHERE (((APPRENTICE_COHORT_INFO.SUBM_CD)='C_Outc17') AND ((APPSO_Current_Region_Data.CURRENT_REGION_PSSM_CODE)=-1 Or (APPSO_Current_Region_Data.CURRENT_REGION_PSSM_CODE)=9) AND ((APPRENTICE_COHORT_INFO.RESPONDENT)='1'));"

qry_APPSO_results <- "
SELECT current_region_pssm_code, [C_Outc05],[C_Outc06],[C_Outc07],[C_Outc08],[C_Outc09],[C_Outc10],[C_Outc11],[C_Outc12],[C_Outc13],[C_Outc14], [C_Outc15], [C_Outc16], [C_Outc17], [C_Outc18], [C_Outc19]
FROM (
	SELECT current_region_pssm_code, -- implicit group by column, result will have one row for each distinct value
	       [key], --column being aggregated
		     SUBM_CD --column that contains the values that will become column headers
	FROM   appso_current_region_data
	WHERE  respondent = '1') P
PIVOT (Count([key]) FOR SUBM_CD IN ([C_Outc05],[C_Outc06],[C_Outc07],[C_Outc08],[C_Outc09],[C_Outc10],[C_Outc11],[C_Outc12],[C_Outc13],[C_Outc14],[C_Outc15],[C_Outc16],[C_Outc17],[C_Outc18],[C_Outc19])) AS PVT
ORDER BY cast(current_region_pssm_code AS INT)"

qry_BGS_00_Append <- "
select D.stqu_id,
       D.year AS survey_year, 
       C.inst, 
       C.srv_y_n,
       D.region_cd,
       D.current_region, 
       D.current_region_name 
from  bgs_dist_15_19 D
inner join bgs_cohort_info C
    on C.stqu_id = D.stqu_id
where D.year IN (2018, 2019)"

qry_BGS_00_NEW_POSTAL <- "
SELECT BGS_S18_USO_V.STQU_ID, BGS_S18_USO_V.NEW_POST, BGS_S18_USO_V.NEW_POSTAL, 2018 AS SURVEY_YEAR
FROM BGS_S18_USO_V
UNION ALL 
SELECT BGS_Y19_USO_V.STQU_ID, BGS_Y19_USO_V.NEW_POST, BGS_Y19_USO_V.NEW_POSTAL, 2019 AS SURVEY_YEAR
FROM BGS_Y19_USO_V"

qry_BGS_00_Region_Codes <- 
"SELECT BGS_DIST_13_17.REGION_CD, BGS_DIST_13_17.CURRENT_REGION, BGS_DIST_13_17.CURRENT_REGION_NAME, BGS_DIST_13_17.CUR_RES, BGS_DIST_13_17.CUR_RES_NAME
FROM BGS_DIST_13_17
GROUP BY BGS_DIST_13_17.REGION_CD, BGS_DIST_13_17.CURRENT_REGION, BGS_DIST_13_17.CURRENT_REGION_NAME, BGS_DIST_13_17.CUR_RES, BGS_DIST_13_17.CUR_RES_NAME"

qry_BGS_results <- 
"TRANSFORM Count(*) AS Expr1
SELECT T_Current_Region_PSSM_Codes.Current_Region_PSSM_Code, T_Current_Region_PSSM_Codes.Current_Region_PSSM_Name
FROM T_Current_Region_PSSM_Codes INNER JOIN BGS_Current_Region_Data ON T_Current_Region_PSSM_Codes.Current_Region_PSSM_Code = BGS_Current_Region_Data.[CURRENT_REGION_PSSM_CODE]
WHERE (((BGS_Current_Region_Data.srv_y_n)=1))
GROUP BY T_Current_Region_PSSM_Codes.Current_Region_PSSM_Code, T_Current_Region_PSSM_Codes.Current_Region_PSSM_Name
ORDER BY T_Current_Region_PSSM_Codes.Current_Region_PSSM_Code, BGS_Current_Region_Data.survey_year
PIVOT BGS_Current_Region_Data.survey_year;"

qry_BGS_results <- "
SELECT current_region_pssm_code, [2014], [2015], [2016], [2017], [2018], [2019]
FROM (
  SELECT current_region_pssm_code,
  [stqu_id],
  survey_year
  FROM   bgs_current_region_data
  WHERE  srv_y_n = '1') P
PIVOT (Count([stqu_id]) FOR survey_year IN ([2014], [2015], [2016], [2017], [2018], [2019])) AS PVT
ORDER BY cast(current_region_pssm_code AS INT)"

qry_BGS_update_Current_Region_PSSM_step0 <- 
"UPDATE BGS_Current_Region_Data 
SET BGS_Current_Region_Data.CURRENT_REGION_PSSM_CODE = Null
WHERE (((BGS_Current_Region_Data.survey_year)='2018' Or (BGS_Current_Region_Data.survey_year)='2019'));"

qry_BGS_update_Current_Region_PSSM_step1 <- 
"UPDATE BGS_Current_Region_Data 
SET BGS_Current_Region_Data.CURRENT_REGION_PSSM_CODE = [BGS_Current_Region_Data].REGION_CD
WHERE (((BGS_Current_Region_Data.REGION_CD)>=1 
And (BGS_Current_Region_Data.REGION_CD)<=8) 
AND ((BGS_Current_Region_Data.survey_year)='2018' Or (BGS_Current_Region_Data.survey_year)='2019'));"

qry_BGS_update_Current_Region_PSSM_step2 <- 
"UPDATE BGS_Current_Region_Data 
SET BGS_Current_Region_Data.CURRENT_REGION_PSSM_CODE = 10
WHERE (((BGS_Current_Region_Data.CURRENT_REGION)=6 Or (BGS_Current_Region_Data.CURRENT_REGION)=9 Or (BGS_Current_Region_Data.CURRENT_REGION)=10) 
AND ((BGS_Current_Region_Data.survey_year)='2018' Or (BGS_Current_Region_Data.survey_year)='2019'));"

qry_BGS_update_Current_Region_PSSM_step3 <- 
"UPDATE BGS_Current_Region_Data 
SET BGS_Current_Region_Data.CURRENT_REGION_PSSM_CODE = 11
WHERE (((BGS_Current_Region_Data.CURRENT_REGION_PSSM_CODE) Is Null) AND ((BGS_Current_Region_Data.CURRENT_REGION)=7) 
AND ((BGS_Current_Region_Data.survey_year)='2018' Or (BGS_Current_Region_Data.survey_year)='2019'));"

qry_BGS_update_Current_Region_PSSM_step4 <- 
"UPDATE BGS_Current_Region_Data 
SET BGS_Current_Region_Data.CURRENT_REGION_PSSM_CODE = 9
WHERE (((BGS_Current_Region_Data.CURRENT_REGION_PSSM_CODE) Is Null) 
AND ((BGS_Current_Region_Data.CURRENT_REGION)=5) 
AND ((BGS_Current_Region_Data.survey_year)='2018' Or (BGS_Current_Region_Data.survey_year)='2019'));"

qry_BGS_update_Current_Region_PSSM_step4b <- 
"UPDATE BGS_Current_Region_Data 
SET BGS_Current_Region_Data.CURRENT_REGION_PSSM_CODE = -1
WHERE (((BGS_Current_Region_Data.CURRENT_REGION_PSSM_CODE) Is Null) 
AND ((BGS_Current_Region_Data.CURRENT_REGION)=8) 
AND ((BGS_Current_Region_Data.survey_year)='2018' Or (BGS_Current_Region_Data.survey_year)='2019'));"

qry_BGS_update_Current_Region_PSSM_step5 <- "
UPDATE    BGS_Current_Region_Data 
SET       BGS_Current_Region_Data.CURRENT_REGION_PSSM_CODE = [tmp_BGS_INST_REGION_CDS].Current_Region_PSSM
FROM      BGS_Current_Region_Data 
INNER JOIN tmp_BGS_INST_REGION_CDS 
  ON      BGS_Current_Region_Data.inst = tmp_BGS_INST_REGION_CDS.inst 
WHERE     (BGS_Current_Region_Data.CURRENT_REGION_PSSM_CODE=-1 Or BGS_Current_Region_Data.CURRENT_REGION_PSSM_CODE Is Null) 
AND       (BGS_Current_Region_Data.srv_y_n=0 Or BGS_Current_Region_Data.srv_y_n Is Null) 
AND       (BGS_Current_Region_Data.survey_year='2018' Or BGS_Current_Region_Data.survey_year='2019');"

qry_BGS99_Update_from_Geocoding <- "
UPDATE BGS_Current_Region_Data 
INNER JOIN Unknown_Geocoding_Investigation 
ON BGS_Current_Region_Data.POSTAL = Unknown_Geocoding_Investigation.POSTAL 
SET Unknown_Geocoding_Investigation.CURRENT_REGION_PSSM_CODE = [BGS_Current_Region_Data].[CURRENT_REGION_PSSM_CODE]
WHERE (((Unknown_Geocoding_Investigation.CURRENT_REGION_PSSM_CODE)=-1) AND ((BGS_Current_Region_Data.CURRENT_REGION_PSSM_CODE)<>-1));"

# won't always be accurate-not used
qry_BGS99b_FSA_Recoding <- 
"UPDATE Unknown_Geocoding_Investigation INNER JOIN T_BGS99_FSA ON Unknown_Geocoding_Investigation.FSA = T_BGS99_FSA.Expr1 SET Unknown_Geocoding_Investigation.CURRENT_REGION_PSSM_CODE = [T_BGS99_FSA].[CURRENT_REGION_PSSM_CODE], Unknown_Geocoding_Investigation.FSA_Used = '1'
WHERE (((Unknown_Geocoding_Investigation.CURRENT_REGION_PSSM_CODE)=-1));"

qry_DACSO_Current_Region_Data <- "
SELECT surv_cohort_collection_info.coci_stqu_id,
       surv_cohort_collection_info.coci_subm_cd,
       surv_cohort_collection_info.coci_lrst_cd,
       surv_cohort_collection_info.tpid_address_postal_new,
       surv_cohort_collection_info.tpid_current_region1,
       surv_cohort_collection_info.tpid_current_region2,
       surv_cohort_collection_info.tpid_current_region3,
       surv_cohort_collection_info.tpid_current_region4
FROM   surv_cohort_collection_info
INNER JOIN c_outc_clean2
    ON surv_cohort_collection_info.coci_stqu_id = c_outc_clean2.stqu_id
WHERE  surv_cohort_collection_info.coci_subm_cd IN ('C_Outc18', 'C_Outc19')"

qry_DACSO_Update_Current_Region_PSSM_step1 <- "
UPDATE  dacso_current_region_data
SET     dacso_current_region_data.current_region_pssm_code = tpid_current_region1
WHERE   dacso_current_region_data.current_region_pssm_code IS NULL 
    AND dacso_current_region_data.tpid_current_region1 >= 1
    AND dacso_current_region_data.tpid_current_region1 <= 8
    AND dacso_current_region_data.coci_subm_cd IN ('C_Outc18', 'C_Outc19');"
    
qry_DACSO_Update_Current_Region_PSSM_step2 <- "
UPDATE  dacso_current_region_data
SET     dacso_current_region_data.current_region_pssm_code = 
        CASE 
          WHEN tpid_current_region4 = 5 THEN 9
          WHEN tpid_current_region4 = 6 THEN 10
          WHEN tpid_current_region4 = 7 THEN 11
          WHEN tpid_current_region4 = 8 THEN -1
          ELSE NULL
       END 
WHERE  dacso_current_region_data.current_region_pssm_code IS NULL
AND    dacso_current_region_data.coci_subm_cd IN ('C_Outc18', 'C_Outc19'); "

qry_DACSO_results <- "
SELECT current_region_pssm_code,  [C_Outc02],[C_Outc03],[C_Outc04],[C_Outc05],[C_Outc06],[C_Outc07],[C_Outc08],[C_Outc09],[C_Outc10],[C_Outc11],[C_Outc12],[C_Outc13],[C_Outc14], [C_Outc15], [C_Outc16], [C_Outc17], [C_Outc18], [C_Outc19]
FROM (
	SELECT  current_region_pssm_code, -- implicit group by column, result will have one row for each distinct value
	        COCI_STQU_ID, --column being aggregated
		      COCI_SUBM_CD--column that contains the values that will become column headers
	FROM    dacso_current_region_data
	WHERE   COCI_LRST_CD = '000') P
PIVOT (Count(COCI_STQU_ID) 
FOR COCI_SUBM_CD IN ( [C_Outc02],[C_Outc03],[C_Outc04],[C_Outc05],[C_Outc06],[C_Outc07],[C_Outc08],[C_Outc09],[C_Outc10],[C_Outc11],[C_Outc12],[C_Outc13],[C_Outc14], [C_Outc15], [C_Outc16], [C_Outc17], [C_Outc18], [C_Outc19])) AS PVT
ORDER BY cast(current_region_pssm_code AS INT)"

qry98_BGS_PC <- 
"INSERT INTO T_FSA_Lookup ( FSA, CURRENT_REGION_PSSM_CODE, POSTAL )
SELECT Left([POSTAL],3) AS FSA, BGS_Current_Region_Data.CURRENT_REGION_PSSM_CODE, BGS_Current_Region_Data.POSTAL
FROM BGS_Current_Region_Data
WHERE (((BGS_Current_Region_Data.survey_year)='2014' Or (BGS_Current_Region_Data.survey_year)='2015' Or (BGS_Current_Region_Data.survey_year)='2016' Or (BGS_Current_Region_Data.survey_year)='2017') AND ((BGS_Current_Region_Data.srv_y_n)=1))
GROUP BY Left([POSTAL],3), BGS_Current_Region_Data.CURRENT_REGION_PSSM_CODE, BGS_Current_Region_Data.POSTAL
HAVING (((BGS_Current_Region_Data.POSTAL) Like 'V#?*' And (BGS_Current_Region_Data.POSTAL) Not Like 'V0R*'));"

qry98_DACSO_FSA <- 
"INSERT INTO T_FSA_Lookup ( FSA, CURRENT_REGION_PSSM_CODE, POSTAL )
SELECT Left([TPID_ADDRESS_POSTAL_NEW],3) AS Expr1, DACSO_Current_Region_Data.CURRENT_REGION_PSSM_CODE, DACSO_Current_Region_Data.TPID_ADDRESS_POSTAL_NEW
FROM DACSO_Current_Region_Data
WHERE (((DACSO_Current_Region_Data.COCI_SUBM_CD) In ('C_Outc14','C_Outc15','C_Outc16','C_Outc17')) AND ((DACSO_Current_Region_Data.TPID_ADDRESS_POSTAL_NEW) Like 'V#?*' And (DACSO_Current_Region_Data.TPID_ADDRESS_POSTAL_NEW) Not Like 'V0R*') AND ((DACSO_Current_Region_Data.COCI_LRST_CD)='000'))
GROUP BY Left([TPID_ADDRESS_POSTAL_NEW],3), DACSO_Current_Region_Data.CURRENT_REGION_PSSM_CODE, DACSO_Current_Region_Data.TPID_ADDRESS_POSTAL_NEW;"

