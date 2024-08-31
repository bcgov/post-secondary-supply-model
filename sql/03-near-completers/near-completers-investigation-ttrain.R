
#---- qry_make_tmp_table_Age_step2 ----
qry_make_tmp_table_Age_step2 <- "
UPDATE tmp_tbl_Age_AppendNewYears 
SET tmp_tbl_Age_AppendNewYears.BTHDT_CLEANED = Right(tmp_tbl_Age_AppendNewYears.BTHDT,2)+'/1/'+Left(tmp_tbl_Age_AppendNewYears.BTHDT,4), 
tmp_tbl_Age_AppendNewYears.ENDDT_CLEANED = Right(tmp_tbl_Age_AppendNewYears.ENDDT,2)+'/1/'+Left(tmp_tbl_Age_AppendNewYears.ENDDT,4);"

#---- qry_make_tmp_table_Age_step3 ----
qry_make_tmp_table_Age_step3 <- "
UPDATE tmp_tbl_Age_AppendNewYears 
SET tmp_tbl_Age_AppendNewYears.BTHDT_DATE = TRY_CONVERT(DATE, tmp_tbl_Age_AppendNewYears.BTHDT_CLEANED), 
tmp_tbl_Age_AppendNewYears.ENDDT_DATE = TRY_CONVERT(DATE, tmp_tbl_Age_AppendNewYears.ENDDT_CLEANED);"

#---- qry_make_tmp_table_Age_step4 ----
qry_make_tmp_table_Age_step4 <- "
INSERT INTO tmp_tbl_Age ( COSC_STQU_ID, COSC_SUBM_CD, TPID_DATE_OF_BIRTH, COSC_ENRL_END_DATE, COCI_AGE_AT_SURVEY )
SELECT tmp_tbl_Age_AppendNewYears.COCI_STQU_ID, 
tmp_tbl_Age_AppendNewYears.COCI_SUBM_CD, 
tmp_tbl_Age_AppendNewYears.BTHDT_DATE, 
tmp_tbl_Age_AppendNewYears.ENDDT_DATE,
tmp_tbl_Age_AppendNewYears.COCI_AGE_AT_SURVEY
FROM tmp_tbl_Age_AppendNewYears;"

#---- qry99_Update_Age_At_Grad ----
qry99_Update_Age_At_Grad <- "
UPDATE tmp_tbl_age
SET Age_At_Grad = Datediff(yyyy, tpid_date_of_birth, Isnull(cosc_grad_credential_date, cosc_enrl_end_date)) + 
CASE WHEN (Isnull(cosc_grad_credential_date, cosc_enrl_end_date) < 
    DateFromParts(Year(Isnull(cosc_grad_credential_date, cosc_enrl_end_date)), Month([tpid_date_of_birth]), Day(tpid_date_of_birth))) THEN -1 ELSE 0 END;"

#---- qry99a_Update_Age_At_Grad ----
qry99a_Update_Age_At_Grad <- "
UPDATE  T_DACSO_DATA_Part_1
SET     Age_At_Grad = tmp_tbl_Age.Age_At_Grad
FROM    tmp_tbl_Age 
INNER JOIN T_DACSO_DATA_Part_1 
ON tmp_tbl_Age.COSC_STQU_ID = T_DACSO_DATA_Part_1.COCI_STQU_ID"


#---- qry_make_T_DACSO_DATA_Part_1_TempSelection ----
qry_make_T_DACSO_DATA_Part_1_TempSelection <- "
SELECT T_DACSO_DATA_Part_1.COCI_STQU_ID, 
T_DACSO_DATA_Part_1.COCI_SUBM_CD, 
T_DACSO_DATA_Part_1.COCI_AGE_AT_SURVEY, 
T_DACSO_DATA_Part_1.Age_At_Grad, 
T_DACSO_DATA_Part_1.COSC_GRAD_STATUS_LGDS_CD_Group, 
T_DACSO_DATA_Part_1.PRGM_Credential_Awarded, 
T_DACSO_DATA_Part_1.PRGM_Credential_Awarded_Name, 
T_DACSO_DATA_Part_1.PSSM_Credential, 
T_DACSO_DATA_Part_1.PSSM_Credential_Name 
INTO T_DACSO_DATA_Part_1_TempSelection
FROM T_DACSO_DATA_Part_1;"

#---- qry99_Investigate_Near_Completes_vs_Graduates_by_Year ----
qry99_Investigate_Near_Completes_vs_Graduates_by_Year <- 
  "select COSC_GRAD_STATUS_LGDS_CD_Group, [C_Outc17],[C_Outc18],[C_Outc19],[C_Outc20],[C_Outc21],
[C_Outc22],[C_Outc23]
FROM
(
SELECT COSC_GRAD_STATUS_LGDS_CD_Group, COCI_STQU_ID , COCI_SUBM_CD 
FROM T_DACSO_DATA_Part_1_TempSelection
WHERE (((T_DACSO_DATA_Part_1_TempSelection.COSC_GRAD_STATUS_LGDS_CD_Group) Is Not Null) 
AND ((T_DACSO_DATA_Part_1_TempSelection.Age_At_Grad)>=17 And (T_DACSO_DATA_Part_1_TempSelection.Age_At_Grad)<=64))
) As P
PIVOT (
  Count(COCI_STQU_ID) 
  FOR COCI_SUBM_CD 
    IN([C_Outc17],[C_Outc18],[C_Outc19],[C_Outc20],[C_Outc21],[C_Outc22],[C_Outc23])
  ) AS PT;"

#---- qry_Find_NearCompleters_in_STP_Credential_Step1 ----
qry_Find_NearCompleters_in_STP_Credential_Step1 <- 
  "SELECT t_dacso_data_part_1.coci_stqu_id,
       t_dacso_data_part_1.coci_subm_cd,
       t_dacso_data_part_1.age_at_grad,
       t_dacso_data_part_1.prgm_credential_awarded AS DACSO_PRGM_Credential_Awarded,
       t_dacso_data_part_1.prgm_credential_awarded_name AS DACSO_PRGM_Credential_Awarded_Name,
       t_dacso_data_part_1.pssm_credential AS DACSO_PSSM_Credential,
       t_dacso_data_part_1.pssm_credential_name AS DACSO_PSSM_Credential_Name,
       --dacso_matching_stp_credential_pen.coci_stqu_id,
       dacso_matching_stp_credential_pen.id,
       dacso_matching_stp_credential_pen.coci_pen,
       dacso_matching_stp_credential_pen.psi_code,
       dacso_matching_stp_credential_pen.coci_inst_cd,
       dacso_matching_stp_credential_pen.prgm_credential_awarded,
       dacso_matching_stp_credential_pen.prgm_credential_awarded_name,
       dacso_matching_stp_credential_pen.stp_prgm_credential_awarded_name,
       dacso_matching_stp_credential_pen.pssm_credential,
       dacso_matching_stp_credential_pen.pssm_credential_name,
       dacso_matching_stp_credential_pen.psi_credential_category,
       dacso_matching_stp_credential_pen.outcomes_cred,
       t_dacso_data_part_1.lcp4_cd,
       dacso_matching_stp_credential_pen.final_cip_code_4,
       dacso_matching_stp_credential_pen.cosc_grad_status_lgds_cd_group,
       --dacso_matching_stp_credential_pen.coci_subm_cd,
       dacso_matching_stp_credential_pen.psi_award_school_year,
       dacso_matching_stp_credential_pen.match_credential,
       dacso_matching_stp_credential_pen.match_cip_code_4,
       dacso_matching_stp_credential_pen.match_award_school_year,
       dacso_matching_stp_credential_pen.match_inst,
       dacso_matching_stp_credential_pen.match_all_4_flag,
       dacso_matching_stp_credential_pen.match_cip_code_2,
       -- dacso_matching_stp_credential_pen.dup_stquid_usethisrecord,
       -- dacso_matching_stp_credential_pen.match to use dup cpc,
       -- dacso_matching_stp_credential_pen.match_all_4_usethisrecord,
       dacso_matching_stp_credential_pen.final_consider_a_match
       -- dacso_matching_stp_credential_pen.final_probable_match
INTO   nearcompleters_in_stp_credential_step1
FROM   t_dacso_data_part_1
LEFT JOIN dacso_matching_stp_credential_pen
  ON t_dacso_data_part_1.coci_stqu_id = dacso_matching_stp_credential_pen.coci_stqu_id
WHERE  ( ( ( t_dacso_data_part_1.coci_subm_cd ) = 'C_Outc07'
    OR ( t_dacso_data_part_1.coci_subm_cd ) = 'C_Outc08'
    OR ( t_dacso_data_part_1.coci_subm_cd ) = 'C_Outc09'
    OR ( t_dacso_data_part_1.coci_subm_cd ) = 'C_Outc10'
    OR ( t_dacso_data_part_1.coci_subm_cd ) = 'C_Outc11'
    OR ( t_dacso_data_part_1.coci_subm_cd ) = 'C_Outc12'
    OR ( t_dacso_data_part_1.coci_subm_cd ) = 'C_Outc13'
    OR ( t_dacso_data_part_1.coci_subm_cd ) = 'C_Outc14'
    OR ( t_dacso_data_part_1.coci_subm_cd ) = 'C_Outc15'
    OR ( t_dacso_data_part_1.coci_subm_cd ) = 'C_Outc16'
    OR ( t_dacso_data_part_1.coci_subm_cd ) = 'C_Outc17'
    OR ( t_dacso_data_part_1.coci_subm_cd ) = 'C_Outc18'
    OR ( t_dacso_data_part_1.coci_subm_cd ) = 'C_Outc19'
    OR ( t_dacso_data_part_1.coci_subm_cd ) = 'C_Outc20'
    OR ( t_dacso_data_part_1.coci_subm_cd ) = 'C_Outc21'
    OR ( t_dacso_data_part_1.coci_subm_cd ) = 'C_Outc22'
    OR ( t_dacso_data_part_1.coci_subm_cd ) = 'C_Outc23')
  AND ( ( t_dacso_data_part_1.age_at_grad ) >= 17 AND ( t_dacso_data_part_1.age_at_grad ) <= 64 )
  AND ( ( t_dacso_data_part_1.cosc_grad_status_lgds_cd_group ) = '3' )
  AND ( ( dacso_matching_stp_credential_pen.coci_stqu_id ) IS NOT NULL ));"

# ---- qry_Update_STP_Credential_Awarded_Before_DACSO ----
qry_Update_STP_Credential_Awarded_Before_DACSO <- 
  "UPDATE NearCompleters_in_STP_Credential_Step1 
  SET NearCompleters_in_STP_Credential_Step1.STP_Credential_Awarded_Before_DACSO = 'Yes'
WHERE (((NearCompleters_in_STP_Credential_Step1.coci_subm_cd)='C_Outc07') 
  AND ((NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2002/2003' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2003/2004' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2004/2005' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2005/2006')) 
OR (((NearCompleters_in_STP_Credential_Step1.coci_subm_cd)='C_Outc08') 
  AND ((NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2002/2003' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2003/2004' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2004/2005' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2005/2006' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2006/2007')) 
OR (((NearCompleters_in_STP_Credential_Step1.coci_subm_cd)='C_Outc09') 
  AND ((NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2002/2003' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2003/2004' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2004/2005' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2005/2006' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2006/2007' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2007/2008')) 
OR (((NearCompleters_in_STP_Credential_Step1.coci_subm_cd)='C_Outc10') 
  AND ((NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2002/2003' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2003/2004' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2004/2005' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2005/2006' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2006/2007' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2007/2008' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2008/2009')) 
OR (((NearCompleters_in_STP_Credential_Step1.coci_subm_cd)='C_Outc11') 
  AND ((NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2002/2003' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2003/2004' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2004/2005' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2005/2006' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2006/2007' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2007/2008' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2008/2009' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2009/2010')) 
OR (((NearCompleters_in_STP_Credential_Step1.coci_subm_cd)='C_Outc12') 
  AND ((NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2002/2003' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2003/2004' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2004/2005' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2005/2006' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2006/2007' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2007/2008' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2008/2009' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2009/2010' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2010/2011')) 
OR (((NearCompleters_in_STP_Credential_Step1.coci_subm_cd)='C_Outc13') 
  AND ((NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2002/2003' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2003/2004' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2004/2005' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2005/2006' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2006/2007'
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2007/2008' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2008/2009' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2009/2010' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2010/2011' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2011/2012')) 
OR (((NearCompleters_in_STP_Credential_Step1.coci_subm_cd)='C_Outc14') 
  AND ((NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2002/2003' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2003/2004' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2004/2005' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2005/2006' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2006/2007' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2007/2008' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2008/2009' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2009/2010' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2010/2011' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2011/2012' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2012/2013')) 
OR (((NearCompleters_in_STP_Credential_Step1.coci_subm_cd)='C_Outc15') 
  AND ((NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2002/2003' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2003/2004' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2004/2005' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2005/2006' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2006/2007' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2007/2008' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2008/2009' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2009/2010' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2010/2011' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2011/2012' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2012/2013' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2013/2014')) 
OR (((NearCompleters_in_STP_Credential_Step1.coci_subm_cd)='C_Outc16') 
  AND ((NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2002/2003' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2003/2004' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2004/2005' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2005/2006' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2006/2007' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2007/2008' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2008/2009' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2009/2010' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2010/2011' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2011/2012' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2012/2013' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2013/2014' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2014/2015')) 
OR (NearCompleters_in_STP_Credential_Step1.coci_subm_cd ='C_Outc17'
  AND ((NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2002/2003' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2003/2004' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2004/2005' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2005/2006' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2006/2007' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2007/2008' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2008/2009' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2009/2010' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2010/2011' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2011/2012' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2012/2013' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2013/2014' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2014/2015' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2015/2016'))
OR (NearCompleters_in_STP_Credential_Step1.coci_subm_cd ='C_Outc18'
  AND ((NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2002/2003' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2003/2004' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2004/2005' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2005/2006' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2006/2007' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2007/2008' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2008/2009' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2009/2010' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2010/2011' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2011/2012' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2012/2013' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2013/2014' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2014/2015' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2015/2016'
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2016/2017'))
OR (NearCompleters_in_STP_Credential_Step1.coci_subm_cd ='C_Outc19'
  AND ((NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2002/2003' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2003/2004' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2004/2005' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2005/2006' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2006/2007' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2007/2008' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2008/2009' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2009/2010' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2010/2011' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2011/2012' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2012/2013' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2013/2014' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2014/2015' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2015/2016'
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2016/2017'
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2017/2018'))
OR (NearCompleters_in_STP_Credential_Step1.coci_subm_cd ='C_Outc20'
  AND ((NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2002/2003' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2003/2004' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2004/2005' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2005/2006' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2006/2007' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2007/2008' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2008/2009' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2009/2010' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2010/2011' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2011/2012' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2012/2013' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2013/2014' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2014/2015' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2015/2016'
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2016/2017'
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2017/2018'
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2018/2019'))
OR (NearCompleters_in_STP_Credential_Step1.coci_subm_cd ='C_Outc21'
  AND ((NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2002/2003' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2003/2004' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2004/2005' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2005/2006' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2006/2007' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2007/2008' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2008/2009' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2009/2010' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2010/2011' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2011/2012' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2012/2013' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2013/2014' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2014/2015' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2015/2016'
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2016/2017'
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2017/2018'
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2018/2019'
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2019/2020'))
OR (NearCompleters_in_STP_Credential_Step1.coci_subm_cd ='C_Outc22'
  AND ((NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2002/2003' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2003/2004' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2004/2005' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2005/2006' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2006/2007' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2007/2008' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2008/2009' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2009/2010' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2010/2011' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2011/2012' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2012/2013' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2013/2014' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2014/2015' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2015/2016'
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2016/2017'
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2017/2018'
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2018/2019'
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2019/2020'
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2020/2021'))
OR (NearCompleters_in_STP_Credential_Step1.coci_subm_cd ='C_Outc23'
  AND ((NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2002/2003' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2003/2004' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2004/2005' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2005/2006' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2006/2007' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2007/2008' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2008/2009' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2009/2010' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2010/2011' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2011/2012' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2012/2013' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2013/2014' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2014/2015' 
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2015/2016'
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2016/2017'
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2017/2018'
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2018/2019'
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2019/2020'
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2020/2021'
    Or (NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR)='2021/2022'));"

# ---- qry_Update_STP_Credential_Awarded_After_DACSO ----
qry_Update_STP_Credential_Awarded_After_DACSO <- "
UPDATE NearCompleters_in_STP_Credential_Step1 
  SET NearCompleters_in_STP_Credential_Step1.STP_Credential_Awarded_After_DACSO = 'Yes'
WHERE (((NearCompleters_in_STP_Credential_Step1.STP_Credential_Awarded_Before_DACSO) Is Null));"

# ---- qry_make_table_NearCompleters ----
qry_make_table_NearCompleters <- "
SELECT T_DACSO_DATA_Part_1.coci_STQU_ID, 
T_DACSO_DATA_Part_1.coci_subm_cd, 
T_DACSO_DATA_Part_1.Age_At_Grad, 
T_DACSO_DATA_Part_1.COSC_GRAD_STATUS_LGDS_CD_Group, 
T_DACSO_DATA_Part_1.PRGM_Credential_Awarded, 
T_DACSO_DATA_Part_1.PRGM_Credential_Awarded_Name, 
T_DACSO_DATA_Part_1.PSSM_Credential, 
T_DACSO_DATA_Part_1.PSSM_Credential_Name 
INTO T_DACSO_NearCompleters
FROM T_DACSO_DATA_Part_1
WHERE (((T_DACSO_DATA_Part_1.Age_At_Grad)>=17 
And (T_DACSO_DATA_Part_1.Age_At_Grad)<=64) 
AND ((T_DACSO_DATA_Part_1.COSC_GRAD_STATUS_LGDS_CD_Group)='3'));"

# ---- qry_update_T_DACSO_Near_Completers_step1 ----
qry_update_T_DACSO_Near_Completers_step1 <- "
UPDATE T_DACSO_NearCompleters 
SET T_DACSO_NearCompleters.STP_Credential_Awarded_Before_DACSO = [NearCompleters_in_STP_Credential_Step1].[STP_Credential_Awarded_Before_DACSO]
FROM NearCompleters_in_STP_Credential_Step1
INNER JOIN T_DACSO_NearCompleters 
ON NearCompleters_in_STP_Credential_Step1.coci_STQU_ID = T_DACSO_NearCompleters.coci_STQU_ID;"

# ---- qry_update_T_DACSO_Near_Completers_step2 ----
qry_update_T_DACSO_Near_Completers_step2 <- "
UPDATE t_dacso_nearcompleters
SET   t_dacso_nearcompleters.stp_credential_awarded_after_dacso =
nearcompleters_in_stp_credential_step1.stp_credential_awarded_after_dacso
FROM nearcompleters_in_stp_credential_step1
INNER JOIN t_dacso_nearcompleters
  ON nearcompleters_in_stp_credential_step1.coci_stqu_id = t_dacso_nearcompleters.coci_stqu_id;"

# ---- qry_NearCompleters_With_More_Than_One_Cdtl ----
qry_NearCompleters_With_More_Than_One_Cdtl <- "
SELECT NearCompleters_in_STP_Credential_Step1.COCI_STQU_ID, Count(*) AS Expr1 
INTO tmp_DACSO_NearCompleters_with_Multiple_Cdtls
FROM NearCompleters_in_STP_Credential_Step1
GROUP BY NearCompleters_in_STP_Credential_Step1.COCI_STQU_ID
HAVING (((Count(*))>1));"

# ---- qry_Update_T_NearCompleters_HasMultipleCdtls ----
qry_Update_T_NearCompleters_HasMultipleCdtls <- "
UPDATE T_DACSO_NearCompleters
SET T_DACSO_NearCompleters.Has_Multiple_STP_Credentials = 'Yes'
FROM tmp_DACSO_NearCompleters_with_Multiple_Cdtls 
INNER JOIN T_DACSO_NearCompleters 
ON tmp_DACSO_NearCompleters_with_Multiple_Cdtls.COCI_STQU_ID = T_DACSO_NearCompleters.COCI_STQU_ID;"

# ---- qry_Clean_NearCompleters_MultiCdtls_Step1 ----
qry_Clean_NearCompleters_MultiCdtls_Step1 <- "
UPDATE NearCompleters_in_STP_Credential_Step1 
SET NearCompleters_in_STP_Credential_Step1.Has_Multiple_STP_Credentials = 'Yes'
FROM tmp_DACSO_NearCompleters_with_Multiple_Cdtls 
INNER JOIN NearCompleters_in_STP_Credential_Step1
ON NearCompleters_in_STP_Credential_Step1.COCI_STQU_ID = tmp_DACSO_NearCompleters_with_Multiple_Cdtls.COCI_STQU_ID;"

# ---- qry_NearCompleters_MultiCdtls_Cleaning_Step2 ----
qry_NearCompleters_MultiCdtls_Cleaning_Step2 <- "
SELECT NearCompleters_in_STP_Credential_Step1.COCI_STQU_ID, 
NearCompleters_in_STP_Credential_Step1.ID, 
NearCompleters_in_STP_Credential_Step1.Has_Multiple_STP_Credentials, 
NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR 
INTO tmp_NearCompletersWithMultiCredentials_Cleaning
FROM NearCompleters_in_STP_Credential_Step1
WHERE (((NearCompleters_in_STP_Credential_Step1.Has_Multiple_STP_Credentials)='Yes'))
ORDER BY NearCompleters_in_STP_Credential_Step1.COCI_STQU_ID, 
NearCompleters_in_STP_Credential_Step1.ID,
NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR DESC;"

# ---- qry_PickMaxYear_step ----
qry_PickMaxYear_step1 <- "
SELECT tmp_NearCompletersWithMultiCredentials_Cleaning.coci_STQU_ID, 
Max(tmp_NearCompletersWithMultiCredentials_Cleaning.PSI_AWARD_SCHOOL_YEAR) AS MaxOfPSI_AWARD_SCHOOL_YEAR 
INTO tmp_MaxAwardYear
FROM tmp_NearCompletersWithMultiCredentials_Cleaning
GROUP BY tmp_NearCompletersWithMultiCredentials_Cleaning.coci_STQU_ID;"

# ---- qry_NearCompleters_MultiCdtls_Cleaning_Step3 ----
qry_NearCompleters_MultiCdtls_Cleaning_Step3 <- "
UPDATE tmp_NearCompletersWithMultiCredentials_Cleaning
SET tmp_NearCompletersWithMultiCredentials_Cleaning.Max_Award_School_Year = 'Yes'
FROM tmp_MaxAwardYear 
INNER JOIN tmp_NearCompletersWithMultiCredentials_Cleaning 
ON (tmp_MaxAwardYear.MaxOfPSI_AWARD_SCHOOL_YEAR = tmp_NearCompletersWithMultiCredentials_Cleaning.PSI_AWARD_SCHOOL_YEAR) 
AND (tmp_MaxAwardYear.coci_STQU_ID = tmp_NearCompletersWithMultiCredentials_Cleaning.coci_STQU_ID);"

# ---- qry_NearCompleters_MultiCdtls_Cleaning_Step4 ----
qry_NearCompleters_MultiCdtls_Cleaning_Step4 <- "
UPDATE NearCompleters_in_STP_Credential_Step1
SET NearCompleters_in_STP_Credential_Step1.Dup_STQUID_UseThisRecord = 'Yes'
FROM tmp_NearCompletersWithMultiCredentials_Cleaning
INNER JOIN NearCompleters_in_STP_Credential_Step1 
ON (tmp_NearCompletersWithMultiCredentials_Cleaning.ID = NearCompleters_in_STP_Credential_Step1.ID) 
AND (tmp_NearCompletersWithMultiCredentials_Cleaning.coci_STQU_ID = NearCompleters_in_STP_Credential_Step1.coci_STQU_ID) 
WHERE (((tmp_NearCompletersWithMultiCredentials_Cleaning.Max_Award_School_Year)='Yes') 
AND ((NearCompleters_in_STP_Credential_Step1.Has_Multiple_STP_Credentials)='Yes'));"

# ---- qry_NearCompleters_MultiCdtls_Cleaning_Step5 ----
qry_NearCompleters_MultiCdtls_Cleaning_Step5 <- "
SELECT NearCompleters_in_STP_Credential_Step1.coci_STQU_ID, 
NearCompleters_in_STP_Credential_Step1.Has_Multiple_STP_Credentials, 
NearCompleters_in_STP_Credential_Step1.Dup_STQUID_UseThisRecord, Count(*) AS Expr1 
INTO tmp_NearCompletersWithMultiCredentials_MaxYear
FROM NearCompleters_in_STP_Credential_Step1
GROUP BY NearCompleters_in_STP_Credential_Step1.coci_STQU_ID, 
NearCompleters_in_STP_Credential_Step1.Has_Multiple_STP_Credentials, 
NearCompleters_in_STP_Credential_Step1.Dup_STQUID_UseThisRecord
HAVING (((NearCompleters_in_STP_Credential_Step1.Has_Multiple_STP_Credentials)='Yes') 
AND ((NearCompleters_in_STP_Credential_Step1.Dup_STQUID_UseThisRecord)='Yes') 
AND ((Count(*))>1));"

# ---- qry_NearCompleters_MultiCdtls_Cleaning_Step6 ----
qry_NearCompleters_MultiCdtls_Cleaning_Step6 <- "
SELECT 
NearCompleters_in_STP_Credential_Step1.coci_STQU_ID, 
NearCompleters_in_STP_Credential_Step1.coci_SUBM_CD, 
NearCompleters_in_STP_Credential_Step1.Age_At_Grad, 
NearCompleters_in_STP_Credential_Step1.COSC_GRAD_STATUS_LGDS_CD_Group, 
NearCompleters_in_STP_Credential_Step1.DACSO_PRGM_Credential_Awarded, 
NearCompleters_in_STP_Credential_Step1.DACSO_PRGM_Credential_Awarded_Name, 
NearCompleters_in_STP_Credential_Step1.DACSO_PSSM_Credential, 
NearCompleters_in_STP_Credential_Step1.DACSO_PSSM_Credential_Name, 
--NearCompleters_in_STP_Credential_Step1.DACSO_Matching_STP_Credential_PEN_coci_STQU_ID, 
NearCompleters_in_STP_Credential_Step1.ID, 
NearCompleters_in_STP_Credential_Step1.COCI_PEN, 
NearCompleters_in_STP_Credential_Step1.PSI_CODE, 
NearCompleters_in_STP_Credential_Step1.COCI_INST_CD, 
NearCompleters_in_STP_Credential_Step1.PRGM_Credential_Awarded, 
NearCompleters_in_STP_Credential_Step1.PRGM_Credential_Awarded_Name, 
--NearCompleters_in_STP_Credential_Step1.STP_PRGM_Credential_Awarded_Name, 
NearCompleters_in_STP_Credential_Step1.PSSM_Credential, 
NearCompleters_in_STP_Credential_Step1.PSSM_Credential_Name, 
NearCompleters_in_STP_Credential_Step1.PSI_CREDENTIAL_CATEGORY, 
NearCompleters_in_STP_Credential_Step1.OUTCOMES_CRED, 
NearCompleters_in_STP_Credential_Step1.LCP4_CD, 
NearCompleters_in_STP_Credential_Step1.FINAL_CIP_CODE_4, 
--NearCompleters_in_STP_Credential_Step1.DACSO_Matching_STP_Credential_PEN_coci_SUBM_CD, 
NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR, 
NearCompleters_in_STP_Credential_Step1.Match_Credential, 
NearCompleters_in_STP_Credential_Step1.Match_CIP_CODE_4, 
NearCompleters_in_STP_Credential_Step1.Match_Award_School_Year, 
NearCompleters_in_STP_Credential_Step1.Match_Inst, 
NearCompleters_in_STP_Credential_Step1.Match_All_4_Flag, 
NearCompleters_in_STP_Credential_Step1.Match_CIP_CODE_2, 
NearCompleters_in_STP_Credential_Step1.Dup_STQUID_UseThisRecord, 
--NearCompleters_in_STP_Credential_Step1.[Match to Use DUP CPC], 
--NearCompleters_in_STP_Credential_Step1.Match_All_4_UseThisRecord, 
NearCompleters_in_STP_Credential_Step1.Final_Consider_A_Match, 
--NearCompleters_in_STP_Credential_Step1.Final_Probable_Match, 
NearCompleters_in_STP_Credential_Step1.STP_Credential_Awarded_Before_DACSO, 
NearCompleters_in_STP_Credential_Step1.STP_Credential_Awarded_After_DACSO, 
NearCompleters_in_STP_Credential_Step1.Has_Multiple_STP_Credentials 
INTO tmp_NearCompletersWithMultiCredentials_MaxYearCleaning
FROM tmp_NearCompletersWithMultiCredentials_MaxYear 
INNER JOIN NearCompleters_in_STP_Credential_Step1 
ON tmp_NearCompletersWithMultiCredentials_MaxYear.coci_STQU_ID = NearCompleters_in_STP_Credential_Step1.coci_STQU_ID;"

# ---- qry_PickMaxYear_Step2 ----
qry_PickMaxYear_Step2 <- "
SELECT tmp_NearCompletersWithMultiCredentials_MaxYearCleaning.coci_STQU_ID, 
Max(tmp_NearCompletersWithMultiCredentials_MaxYearCleaning.ID) AS MaxOfID, 
tmp_NearCompletersWithMultiCredentials_MaxYearCleaning.Dup_STQUID_UseThisRecord 
INTO tmp_MaxAwardYearCleaning_MaxID
FROM tmp_NearCompletersWithMultiCredentials_MaxYearCleaning
GROUP BY tmp_NearCompletersWithMultiCredentials_MaxYearCleaning.coci_STQU_ID, 
tmp_NearCompletersWithMultiCredentials_MaxYearCleaning.Dup_STQUID_UseThisRecord
HAVING (((tmp_NearCompletersWithMultiCredentials_MaxYearCleaning.Dup_STQUID_UseThisRecord)='Yes'));"

# ---- qry_PickMaxYear_Step3 ----
qry_PickMaxYear_Step3 <- "
UPDATE tmp_NearCompletersWithMultiCredentials_MaxYearCleaning 
SET tmp_NearCompletersWithMultiCredentials_MaxYearCleaning.Final_Record_To_Use = 'Yes'
FROM tmp_NearCompletersWithMultiCredentials_MaxYearCleaning
INNER JOIN tmp_MaxAwardYearCleaning_MaxID 
ON (tmp_NearCompletersWithMultiCredentials_MaxYearCleaning.Dup_STQUID_UseThisRecord = tmp_MaxAwardYearCleaning_MaxID.Dup_STQUID_UseThisRecord) 
AND (tmp_NearCompletersWithMultiCredentials_MaxYearCleaning.ID = tmp_MaxAwardYearCleaning_MaxID.MaxOfID) 
AND (tmp_NearCompletersWithMultiCredentials_MaxYearCleaning.coci_STQU_ID = tmp_MaxAwardYearCleaning_MaxID.coci_STQU_ID);"

# ---- qry_NearCompleters_MultiCdtls_Cleaning_Step7 ----
qry_NearCompleters_MultiCdtls_Cleaning_Step7 <- "
UPDATE tmp_NearCompletersWithMultiCredentials_MaxYearCleaning
SET tmp_NearCompletersWithMultiCredentials_MaxYearCleaning.Final_Record_To_Use = 'Yes'
FROM tmp_MaxAwardYearCleaning 
INNER JOIN tmp_NearCompletersWithMultiCredentials_MaxYearCleaning 
ON (tmp_MaxAwardYearCleaning.MaxOfPSI_AWARD_SCHOOL_YEAR = tmp_NearCompletersWithMultiCredentials_MaxYearCleaning.PSI_AWARD_SCHOOL_YEAR) 
AND (tmp_MaxAwardYearCleaning.coci_STQU_ID = tmp_NearCompletersWithMultiCredentials_MaxYearCleaning.coci_STQU_ID);"

# ---- qry_NearCompleters_MultiCdtls_Cleaning_Step8 ----
qry_NearCompleters_MultiCdtls_Cleaning_Step8 <- 
  "SELECT tmp_NearCompletersWithMultiCredentials_MaxYearCleaning.coci_STQU_ID, 
Max(tmp_NearCompletersWithMultiCredentials_MaxYearCleaning.ID) AS MaxOfID, 
tmp_NearCompletersWithMultiCredentials_MaxYearCleaning.Dup_STQUID_UseThisRecord 
INTO tmp_MaxAwardYearCleaning_MaxID
FROM tmp_NearCompletersWithMultiCredentials_MaxYearCleaning
GROUP BY tmp_NearCompletersWithMultiCredentials_MaxYearCleaning.coci_STQU_ID, 
tmp_NearCompletersWithMultiCredentials_MaxYearCleaning.Dup_STQUID_UseThisRecord
HAVING (((tmp_NearCompletersWithMultiCredentials_MaxYearCleaning.Dup_STQUID_UseThisRecord)='Yes'));"

# ---- qry_NearCompleters_MultiCdtls_Cleaning_Step10 ----
qry_NearCompleters_MultiCdtls_Cleaning_Step10 <- "
UPDATE NearCompleters_in_STP_Credential_Step1 
SET NearCompleters_in_STP_Credential_Step1.Final_Record_to_Use = 'Yes'
FROM NearCompleters_in_STP_Credential_Step1 
INNER JOIN tmp_NearCompletersWithMultiCredentials_MaxYearCleaning 
ON (NearCompleters_in_STP_Credential_Step1.ID = tmp_NearCompletersWithMultiCredentials_MaxYearCleaning.ID) 
AND (NearCompleters_in_STP_Credential_Step1.coci_STQU_ID = tmp_NearCompletersWithMultiCredentials_MaxYearCleaning.coci_STQU_ID) 
WHERE (((tmp_NearCompletersWithMultiCredentials_MaxYearCleaning.Final_Record_To_Use)='Yes'));"

# ---- qry_NearCompleters_MultiCdtls_Cleaning_Step11 ----
qry_NearCompleters_MultiCdtls_Cleaning_Step11 <- 
  "UPDATE tmp_MaxAwardYear
  SET NearCompleters_in_STP_Credential_Step1.Final_Record_to_Use = 'Yes'
FROM tmp_MaxAwardYear
INNER JOIN NearCompleters_in_STP_Credential_Step1 
ON (tmp_MaxAwardYear.MaxOfPSI_AWARD_SCHOOL_YEAR = NearCompleters_in_STP_Credential_Step1.PSI_AWARD_SCHOOL_YEAR) 
AND (tmp_MaxAwardYear.coci_STQU_ID = NearCompleters_in_STP_Credential_Step1.coci_STQU_ID) 
WHERE (((NearCompleters_in_STP_Credential_Step1.Final_Record_to_Use) Is Null) 
AND ((tmp_MaxAwardYear.Ignore) Is Null));"

# ---- qry_NearCompleters_MultiCdtls_Cleaning_Step13 ----
qry_NearCompleters_MultiCdtls_Cleaning_Step13 <- "
UPDATE T_DACSO_NearCompleters 
SET T_DACSO_NearCompleters.STP_Credential_Awarded_Before_DACSO = [NearCompleters_in_STP_Credential_Step1].[STP_Credential_Awarded_Before_DACSO], 
    T_DACSO_NearCompleters.STP_Credential_Awarded_After_DACSO = [NearCompleters_in_STP_Credential_Step1].[STP_Credential_Awarded_After_DACSO]
FROM T_DACSO_NearCompleters
INNER JOIN NearCompleters_in_STP_Credential_Step1 
ON T_DACSO_NearCompleters.coci_STQU_ID = NearCompleters_in_STP_Credential_Step1.coci_STQU_ID 
WHERE (((NearCompleters_in_STP_Credential_Step1.Final_Record_to_Use)='Yes'));"

# ---- qry_PickMaxYear_step4 ----
qry_PickMaxYear_step4 <- "
UPDATE tmp_NearCompletersWithMultiCredentials_Cleaning 
SET tmp_NearCompletersWithMultiCredentials_Cleaning.DupUseThisRecordMaxYear = 'Yes'
FROM tmp_NearCompletersWithMultiCredentials_Cleaning 
INNER JOIN tmp_MaxAwardYearCleaning_MaxID 
ON (tmp_NearCompletersWithMultiCredentials_Cleaning.ID = tmp_MaxAwardYearCleaning_MaxID.MaxOfID) 
AND (tmp_NearCompletersWithMultiCredentials_Cleaning.coci_STQU_ID = tmp_MaxAwardYearCleaning_MaxID.coci_STQU_ID);"


# ---- qry_PickMaxYear_step5 ----
qry_PickMaxYear_step5 <- "
UPDATE tmp_NearCompletersWithMultiCredentials_Cleaning  
SET tmp_NearCompletersWithMultiCredentials_Cleaning.Max_Award_School_Year = Null
FROM tmp_NearCompletersWithMultiCredentials_Cleaning 
INNER JOIN tmp_MaxAwardYearCleaning_MaxID 
ON tmp_NearCompletersWithMultiCredentials_Cleaning.coci_STQU_ID = tmp_MaxAwardYearCleaning_MaxID.coci_STQU_ID
WHERE (((tmp_NearCompletersWithMultiCredentials_Cleaning.Max_Award_School_Year)='Yes') 
AND ((tmp_NearCompletersWithMultiCredentials_Cleaning.DupUseThisRecordMaxYear) Is Null));"


# ---- qry_Update_DupStqu_ID_UseThisRecord2 ----
qry_Update_DupStqu_ID_UseThisRecord2 <- "
UPDATE DACSO_Matching_STP_Credential_PEN 
SET DACSO_Matching_STP_Credential_PEN.Dup_STQUID_UseThisRecord = 'Yes'
FROM DACSO_Matching_STP_Credential_PEN 
INNER JOIN tmp_NearCompletersWithMultiCredentials_MaxYearCleaning 
ON (tmp_NearCompletersWithMultiCredentials_MaxYearCleaning.ID = DACSO_Matching_STP_Credential_PEN.ID) 
AND (DACSO_Matching_STP_Credential_PEN.COCI_STQU_ID = tmp_NearCompletersWithMultiCredentials_MaxYearCleaning.COCI_STQU_ID)
WHERE (((tmp_NearCompletersWithMultiCredentials_MaxYearCleaning.Final_Record_To_Use)='Yes'));"

# ---- qry_Update_Final_Record_To_Use_NearCompletersDups ----
qry_Update_Final_Record_To_Use_NearCompletersDups <- "
UPDATE NearCompleters_in_STP_Credential_Step1 
SET NearCompleters_in_STP_Credential_Step1.Final_Record_to_Use = tmp_NearCompletersWithMultiCredentials_MaxYearCleaning.Final_Record_To_Use
FROM NearCompleters_in_STP_Credential_Step1 
INNER JOIN tmp_NearCompletersWithMultiCredentials_MaxYearCleaning 
ON (NearCompleters_in_STP_Credential_Step1.ID = tmp_NearCompletersWithMultiCredentials_MaxYearCleaning.ID) 
AND (NearCompleters_in_STP_Credential_Step1.coci_STQU_ID = tmp_NearCompletersWithMultiCredentials_MaxYearCleaning.coci_STQU_ID)"

# ---- qry_NearCompleters_MultiCdtls_Cleaning_Step12 ----
qry_NearCompleters_MultiCdtls_Cleaning_Step12 <- 
"UPDATE NearCompleters_in_STP_Credential_Step1 
SET NearCompleters_in_STP_Credential_Step1.Final_Record_to_Use = 'Yes'
WHERE (((NearCompleters_in_STP_Credential_Step1.Final_Record_to_Use) Is Null) 
AND ((NearCompleters_in_STP_Credential_Step1.Has_Multiple_STP_Credentials) Is Null));"

# ---- qry_Update_Final_STP_Cred_Before_or_After_Step1 ----
qry_Update_Final_STP_Cred_Before_or_After_Step1 <- "
UPDATE T_DACSO_NearCompleters 
SET T_DACSO_NearCompleters.STP_Credential_Awarded_Before_DACSO_FINAL = NearCompleters_in_STP_Credential_Step1.STP_Credential_Awarded_Before_DACSO, 
    T_DACSO_NearCompleters.STP_Credential_Awarded_After_DACSO_FINAL  = NearCompleters_in_STP_Credential_Step1.STP_Credential_Awarded_After_DACSO
FRoM T_DACSO_NearCompleters 
INNER JOIN NearCompleters_in_STP_Credential_Step1 
ON T_DACSO_NearCompleters.coci_STQU_ID = NearCompleters_in_STP_Credential_Step1.coci_STQU_ID 
WHERE (((NearCompleters_in_STP_Credential_Step1.Final_Record_to_Use)='Yes'));"

# ---- qry_update_Has_STP_Credential ----
qry_update_Has_STP_Credential <- "
UPDATE T_DACSO_DATA_Part_1_TempSelection 
SET T_DACSO_DATA_Part_1_TempSelection.Has_STP_Credential = 'Yes'
FROM T_DACSO_DATA_Part_1_TempSelection 
INNER JOIN T_DACSO_NearCompleters 
ON T_DACSO_DATA_Part_1_TempSelection.coci_STQU_ID = T_DACSO_NearCompleters.coci_STQU_ID 
WHERE (((T_DACSO_NearCompleters.STP_Credential_Awarded_Before_DACSO)='Yes')) 
OR (((T_DACSO_NearCompleters.STP_Credential_Awarded_After_DACSO)='Yes'));"

# ---- qry_update_Grad_Status_Factoring_in_STP_step1 ----
qry_update_Grad_Status_Factoring_in_STP_step1 <- "
UPDATE T_DACSO_DATA_Part_1_TempSelection 
SET T_DACSO_DATA_Part_1_TempSelection.Grad_Status_Factoring_in_STP = T_DACSO_DATA_Part_1_TempSelection.COSC_GRAD_STATUS_LGDS_CD_Group;"

# ----  qry_update_Grad_Status_Factoring_in_STP_step2 ----
qry_update_Grad_Status_Factoring_in_STP_step2 <- "
UPDATE T_DACSO_DATA_Part_1_TempSelection 
SET T_DACSO_DATA_Part_1_TempSelection.Grad_Status_Factoring_in_STP = '1'
WHERE (((T_DACSO_DATA_Part_1_TempSelection.Grad_Status_Factoring_in_STP)='3') 
AND ((T_DACSO_DATA_Part_1_TempSelection.Has_STP_Credential)='Yes'));"

# ---- qry99_Investigate_Near_Completes_vs_Graduates_by_Year  ----
qry99_Investigate_Near_Completes_vs_Graduates_by_Year <- 
  "select COSC_GRAD_STATUS_LGDS_CD_Group, [C_Outc17],[C_Outc18],[C_Outc19],[C_Outc20],[C_Outc21],[C_Outc22],[C_Outc23]
FROM
(
SELECT COSC_GRAD_STATUS_LGDS_CD_Group, COCI_STQU_ID , COCI_SUBM_CD 
FROM T_DACSO_DATA_Part_1_TempSelection
WHERE (((T_DACSO_DATA_Part_1_TempSelection.COSC_GRAD_STATUS_LGDS_CD_Group) Is Not Null) 
AND ((T_DACSO_DATA_Part_1_TempSelection.Age_At_Grad)>=17 And (T_DACSO_DATA_Part_1_TempSelection.Age_At_Grad)<=64))
) As P
PIVOT (
  Count(COCI_STQU_ID) 
  FOR COCI_SUBM_CD 
    IN([C_Outc17],[C_Outc18],[C_Outc19],[C_Outc20],[C_Outc21],[C_Outc22],[C_Outc23])
  ) AS PT;"


# ---- qry99_GradStatus_Factoring_in_STP_Credential_by_Year ----
qry99_GradStatus_Factoring_in_STP_Credential_by_Year <- "
select Grad_Status_Factoring_in_STP, [C_Outc17],[C_Outc18],[C_Outc19],[C_Outc20],[C_Outc21],[C_Outc22],[C_Outc23]
FROM
(
SELECT T_DACSO_DATA_Part_1_TempSelection.Grad_Status_Factoring_in_STP, COCI_SUBM_CD
FROM T_DACSO_DATA_Part_1_TempSelection
WHERE (((T_DACSO_DATA_Part_1_TempSelection.Grad_Status_Factoring_in_STP) Is Not Null) 
AND ((T_DACSO_DATA_Part_1_TempSelection.Age_At_Grad)>=17 
And (T_DACSO_DATA_Part_1_TempSelection.Age_At_Grad)<=64))
) As P
PIVOT (
  Count(COCI_SUBM_CD) 
  FOR COCI_SUBM_CD 
    IN([C_Outc17],[C_Outc18],[C_Outc19],[C_Outc20],[C_Outc21],[C_Outc22],[C_Outc23])
) AS PT;"

# ---- qry99_GradStatus_byCred_by_Year_Age_At_Grad ----
qry99_GradStatus_byCred_by_Year_Age_At_Grad <- "
select PSSM_Credential,PSSM_Credential_Name,COSC_GRAD_STATUS_LGDS_CD_Group, [C_Outc17],[C_Outc18],[C_Outc19],[C_Outc20],[C_Outc21],[C_Outc22],[C_Outc23]
FROM
(
SELECT T_DACSO_DATA_Part_1_TempSelection.PSSM_Credential, 
T_DACSO_DATA_Part_1_TempSelection.PSSM_Credential_Name, 
T_DACSO_DATA_Part_1_TempSelection.COSC_GRAD_STATUS_LGDS_CD_Group,
COCI_SUBM_CD 
FROM (tbl_Age INNER JOIN (T_DACSO_DATA_Part_1_TempSelection 
INNER JOIN tmp_tbl_Age ON T_DACSO_DATA_Part_1_TempSelection.coci_STQU_ID = tmp_tbl_Age.COSC_STQU_ID) 
  ON tbl_Age.Age = tmp_tbl_Age.Age_At_Grad) 
--INNER JOIN agegrouplookup 
--  ON tbl_Age.Age_Group = agegrouplookup.Age_Group
WHERE (((T_DACSO_DATA_Part_1_TempSelection.COSC_GRAD_STATUS_LGDS_CD_Group) Is Not Null)) 
AND ((tmp_tbl_Age.Age_At_Grad)>=17 And (tmp_tbl_Age.Age_At_Grad)<=64)
) As P
PIVOT (
  Count(COCI_SUBM_CD) 
  FOR COCI_SUBM_CD 
    IN([C_Outc17],[C_Outc18],[C_Outc19],[C_Outc20],[C_Outc21],[C_Outc22],[C_Outc23])
) AS PT;"

# ---- qry99_GradStatus_Factoring_in_STP_byCred_by_Year_Age_At_Grad ----
qry99_GradStatus_Factoring_in_STP_byCred_by_Year_Age_At_Grad <- 
"select PSSM_Credential,PSSM_Credential_Name,Grad_Status_Factoring_in_STP, [C_Outc17],[C_Outc18],[C_Outc19],[C_Outc20],[C_Outc21],[C_Outc22],[C_Outc23]
FROM(
SELECT T_DACSO_DATA_Part_1_TempSelection.PSSM_Credential, 
T_DACSO_DATA_Part_1_TempSelection.PSSM_Credential_Name, 
T_DACSO_DATA_Part_1_TempSelection.Grad_Status_Factoring_in_STP,
COCI_SUBM_CD 
FROM (tbl_Age INNER JOIN (T_DACSO_DATA_Part_1_TempSelection 
INNER JOIN tmp_tbl_Age ON T_DACSO_DATA_Part_1_TempSelection.coci_STQU_ID = tmp_tbl_Age.COSC_STQU_ID) 
  ON tbl_Age.Age = tmp_tbl_Age.Age_At_Grad) 
--INNER JOIN agegrouplookup 
--  ON tbl_Age.Age_Group = cast(agegrouplookup.Age_Group as float)
WHERE (((T_DACSO_DATA_Part_1_TempSelection.Grad_Status_Factoring_in_STP) Is Not Null)) 
AND ((tmp_tbl_Age.Age_At_Grad)>=17 
And (tmp_tbl_Age.Age_At_Grad)<=64)
) As P
PIVOT (
  Count(COCI_SUBM_CD) 
  FOR COCI_SUBM_CD 
    IN([C_Outc17],[C_Outc18],[C_Outc19],[C_Outc20],[C_Outc21],[C_Outc22],[C_Outc23])
) AS PT;"

# ---- qry_details_of_STP_Credential_Matching  ----
qry_details_of_STP_Credential_Matching <- 
  "SELECT T_DACSO_DATA_Part_1_TempSelection.coci_SUBM_CD, 
T_DACSO_DATA_Part_1_TempSelection.COSC_GRAD_STATUS_LGDS_CD_Group, 
T_DACSO_DATA_Part_1_TempSelection.Grad_Status_Factoring_in_STP, Count(*) AS Expr1
FROM T_DACSO_DATA_Part_1_TempSelection
WHERE (((T_DACSO_DATA_Part_1_TempSelection.Age_At_Grad)>=17 
And (T_DACSO_DATA_Part_1_TempSelection.Age_At_Grad)<=64))
GROUP BY T_DACSO_DATA_Part_1_TempSelection.coci_SUBM_CD, 
T_DACSO_DATA_Part_1_TempSelection.COSC_GRAD_STATUS_LGDS_CD_Group, 
T_DACSO_DATA_Part_1_TempSelection.Grad_Status_Factoring_in_STP;"

# ---- qry99_Near_completes_total_by_CIP4  ----
qry99_Near_completes_total_by_CIP4 <-"
SELECT     AgeGroupLookup.Age_Group, T_DACSO_DATA_Part_1.PRGM_Credential_Awarded_Name, T_DACSO_DATA_Part_1.LCIP4_CRED, 
                         T_DACSO_DATA_Part_1.LCP4_CD, T_DACSO_DATA_Part_1.LCP4_CIP_4DIGITS_NAME, COUNT(*) AS Count
INTO NearCompleters_CIP4
FROM         T_DACSO_DATA_Part_1 
INNER JOIN AgeGroupLookup 
ON T_DACSO_DATA_Part_1.Age_At_Grad >= AgeGroupLookup.Lower_Bound 
AND T_DACSO_DATA_Part_1.Age_At_Grad <= AgeGroupLookup.Upper_Bound 
LEFT OUTER JOIN CredentialRank ON T_DACSO_DATA_Part_1.PRGM_Credential_Awarded_Name = CredentialRank.PSI_CREDENTIAL_CATEGORY
WHERE (T_DACSO_DATA_Part_1.COSC_GRAD_STATUS_LGDS_CD_Group = '3') 
AND (T_DACSO_DATA_Part_1.COCI_SUBM_CD IN ('C_Outc19', 'C_Outc20'))
GROUP BY AgeGroupLookup.Age_Group, T_DACSO_DATA_Part_1.PRGM_Credential_Awarded_Name, T_DACSO_DATA_Part_1.LCIP4_CRED, 
                         T_DACSO_DATA_Part_1.LCP4_CD, T_DACSO_DATA_Part_1.LCP4_CIP_4DIGITS_NAME
ORDER BY AgeGroupLookup.Age_Group, T_DACSO_DATA_Part_1.PRGM_Credential_Awarded_Name"

# ---- qry_Make_NearCompleters_CIP4_CombinedCred ----
qry_Make_NearCompleters_CIP4_CombinedCred <- "
SELECT nearcompleters_cip4.age_group,
      combine_creds.combined_cred_name,
      nearcompleters_cip4.lcip4_cred,
      nearcompleters_cip4.lcp4_cd,
      nearcompleters_cip4.lcp4_cip_4digits_name,
      Sum(nearcompleters_cip4.count) AS CombinedCredCount
INTO   nearcompleters_cip4_combinedcred
FROM   nearcompleters_cip4
INNER JOIN combine_creds
ON nearcompleters_cip4.prgm_credential_awarded_name =
  combine_creds.prgm_credential_awarded_name
WHERE  (( ( combine_creds.use_in_pssm_2017_18 ) = 'Yes' ))
GROUP  BY nearcompleters_cip4.age_group,
    combine_creds.combined_cred_name,
    nearcompleters_cip4.lcip4_cred,
    nearcompleters_cip4.lcp4_cd,
    nearcompleters_cip4.lcp4_cip_4digits_name;"

# ---- qry99_Near_completes_total_with_STP_Credential_ByCIP4 ----
qry99_Near_completes_total_with_STP_Credential_ByCIP4 <-
"SELECT     AgeGroupLookup.age_group, T_DACSO_DATA_Part_1.PRGM_Credential_Awarded_Name, COUNT(*) AS Count, 
                      T_DACSO_DATA_Part_1_TempSelection.Has_STP_Credential, lcip4_cred,
      lcp4_cd,
     lcp4_cip_4digits_name
INTO NearCompleters_CIP4_with_STP_Credential
FROM         T_DACSO_DATA_Part_1 INNER JOIN
                      AgeGroupLookup ON T_DACSO_DATA_Part_1.Age_At_Grad >= AgeGroupLookup.lower_bound AND 
                      T_DACSO_DATA_Part_1.Age_At_Grad <= AgeGroupLookup.upper_bound INNER JOIN
                      T_DACSO_DATA_Part_1_TempSelection ON T_DACSO_DATA_Part_1.COCI_STQU_ID = T_DACSO_DATA_Part_1_TempSelection.COCI_STQU_ID LEFT OUTER JOIN
                      CredentialRank ON T_DACSO_DATA_Part_1.PRGM_Credential_Awarded_Name = CredentialRank.PSI_CREDENTIAL_CATEGORY
WHERE     (T_DACSO_DATA_Part_1.COCI_SUBM_CD IN ('C_Outc19', 'C_Outc20'))
GROUP BY AgeGroupLookup.age_group, T_DACSO_DATA_Part_1.PRGM_Credential_Awarded_Name, T_DACSO_DATA_Part_1_TempSelection.Has_STP_Credential,  lcip4_cred,
      lcp4_cd,
     lcp4_cip_4digits_name
HAVING      (T_DACSO_DATA_Part_1_TempSelection.Has_STP_Credential = 'Yes')
ORDER BY AgeGroupLookup.age_group, T_DACSO_DATA_Part_1.PRGM_Credential_Awarded_Name"

# ---- qry_Make_NearCompleters_CIP4_With_STP_CombinedCred ----
qry_Make_NearCompleters_CIP4_With_STP_CombinedCred <- "
SELECT nearcompleters_cip4_with_stp_credential.age_group,
combine_creds.combined_cred_name,
nearcompleters_cip4_with_stp_credential.lcip4_cred,
nearcompleters_cip4_with_stp_credential.lcp4_cd,
nearcompleters_cip4_with_stp_credential.lcp4_cip_4digits_name,
Sum(nearcompleters_cip4_with_stp_credential.count) AS CombinedCredCount,
nearcompleters_cip4_with_stp_credential.has_stp_credential
INTO NearCompleters_CIP4_With_STP_CombinedCred
FROM   nearcompleters_cip4_with_stp_credential
INNER JOIN combine_creds
ON
nearcompleters_cip4_with_stp_credential.prgm_credential_awarded_name
= combine_creds.prgm_credential_awarded_name
WHERE  (( ( combine_creds.use_in_pssm_2017_18) = 'Yes' ))
GROUP  BY nearcompleters_cip4_with_stp_credential.age_group,
combine_creds.combined_cred_name,
nearcompleters_cip4_with_stp_credential.lcip4_cred,
nearcompleters_cip4_with_stp_credential.lcp4_cd,
nearcompleters_cip4_with_stp_credential.lcp4_cip_4digits_name,
nearcompleters_cip4_with_stp_credential.has_stp_credential;"

# ---- qry99_Completers_agg_factoring_in_STP_Credential_by_CIP4 ----
qry99_Completers_agg_factoring_in_STP_Credential_by_CIP4 <-
"SELECT agegrouplookup.age_group,
       t_dacso_data_part_1.prgm_credential_awarded_name,
       Count(*) AS Expr1,
       t_dacso_data_part_1.lcip4_cred,
       t_dacso_data_part_1.lcp4_cd,
       t_dacso_data_part_1.lcp4_cip_4digits_name
INTO   completersfactoringinstp_cip4
FROM   t_dacso_data_part_1
       INNER JOIN agegrouplookup
               ON t_dacso_data_part_1.age_at_grad >=
                  agegrouplookup.lower_bound
                  AND t_dacso_data_part_1.age_at_grad <=
                      agegrouplookup.upper_bound
       LEFT OUTER JOIN credentialrank AS CredentialRank_1
                    ON t_dacso_data_part_1.prgm_credential_awarded_name =
                       CredentialRank_1.psi_credential_category
WHERE  ( t_dacso_data_part_1.grad_status_factoring_in_stp = '1' )
       AND ( t_dacso_data_part_1.coci_subm_cd IN ('C_Outc19', 'C_Outc20'))
       AND ( t_dacso_data_part_1.age_at_grad >= 17 )
       AND ( t_dacso_data_part_1.age_at_grad <= 64 )
GROUP  BY agegrouplookup.age_group,
          t_dacso_data_part_1.prgm_credential_awarded_name,
          agegrouplookup.age_group,
          t_dacso_data_part_1.lcip4_cred,
          t_dacso_data_part_1.lcp4_cd,
          t_dacso_data_part_1.lcp4_cip_4digits_name
ORDER  BY agegrouplookup.age_group,
          t_dacso_data_part_1.prgm_credential_awarded_name"

# ---- qry_Make_CompletersFactoringInSTP_CIP4_CombinedCred  ----
qry_Make_CompletersFactoringInSTP_CIP4_CombinedCred <- "
SELECT completersfactoringinstp_cip4.age_group,
        combine_creds.combined_cred_name,
        completersfactoringinstp_cip4.lcip4_cred_cleaned,
        completersfactoringinstp_cip4.lcp4_cd,
        completersfactoringinstp_cip4.lcp4_cip_4digits_name,
        Sum(completersfactoringinstp_cip4.expr1) AS CombinedCredCount
INTO CompletersFactoringInSTP_CIP4_CombinedCred
FROM   completersfactoringinstp_cip4
INNER JOIN combine_creds
ON completersfactoringinstp_cip4.prgm_credential_awarded_name = combine_creds.prgm_credential_awarded_name
WHERE  (( ( combine_creds.use_in_pssm_2017_18) = 'Yes' ))
GROUP  BY completersfactoringinstp_cip4.age_group,
        combine_creds.combined_cred_name,
        completersfactoringinstp_cip4.lcip4_cred_cleaned,
        completersfactoringinstp_cip4.lcp4_cd,
        completersfactoringinstp_cip4.lcp4_cip_4digits_name;"

# ---- qry99_Completers_agg_by_gender ----
qry99_Completers_agg_by_gender <-
"SELECT     T_DACSO_DATA_Part_1.tpid_lgnd_cd, agegrouplookup.age_group, 
T_DACSO_DATA_Part_1.prgm_credential_awarded_name, COUNT(*)
AS Count
INTO Completers_agg_by_gender
FROM         T_DACSO_DATA_Part_1 
INNER JOIN agegrouplookup
ON T_DACSO_DATA_Part_1.Age_At_Grad >= agegrouplookup.lower_bound 
AND T_DACSO_DATA_Part_1.Age_At_Grad <= agegrouplookup.upper_bound 
LEFT OUTER JOIN CredentialRank
ON T_DACSO_DATA_Part_1.PRGM_Credential_Awarded_Name = CredentialRank.PSI_CREDENTIAL_CATEGORY
WHERE     (T_DACSO_DATA_Part_1.COCI_SUBM_CD IN ('C_Outc19', 'C_Outc20')) 
AND (T_DACSO_DATA_Part_1.Age_At_Grad >= 17) 
AND (T_DACSO_DATA_Part_1.Age_At_Grad <= 64) 
AND (T_DACSO_DATA_Part_1.COSC_GRAD_STATUS_LGDS_CD_Group = '1')
GROUP BY agegrouplookup.age_group, 
T_DACSO_DATA_Part_1.PRGM_Credential_Awarded_Name, 
agegrouplookup.age_group, 
T_DACSO_DATA_Part_1.TPID_LGND_CD
HAVING      (T_DACSO_DATA_Part_1.TPID_LGND_CD <> '0')
ORDER BY T_DACSO_DATA_Part_1.TPID_LGND_CD Desc, 
agegrouplookup.age_group, 
T_DACSO_DATA_Part_1.PRGM_Credential_Awarded_Name"

# ---- qry99_Completers_agg_byCIP4---- 
qry99_Completers_agg_byCIP4 <- "
SELECT AgeGroupLookup.age_group,
       t_dacso_data_part_1.prgm_credential_awarded_name,
       Count(*) AS Expr1,
       t_dacso_data_part_1.lcp4_cd,
       t_dacso_data_part_1.lcp4_cip_4digits_name,
       t_dacso_data_part_1.lcip4_cred
INTO   completerscip4
FROM   t_dacso_data_part_1
       INNER JOIN AgeGroupLookup
               ON t_dacso_data_part_1.age_at_grad >= AgeGroupLookup.lower_bound
              AND t_dacso_data_part_1.age_at_grad <= AgeGroupLookup.upper_bound
       LEFT OUTER JOIN credentialrank
                    ON t_dacso_data_part_1.prgm_credential_awarded_name = CredentialRank.psi_credential_category
WHERE  ( t_dacso_data_part_1.coci_subm_cd IN (
         'C_Outc12','C_Outc13','C_Outc14') )
       AND ( t_dacso_data_part_1.age_at_grad >= 17 )
       AND ( t_dacso_data_part_1.age_at_grad <= 64 )
       AND ( t_dacso_data_part_1.cosc_grad_status_lgds_cd_group = '1' )
GROUP  BY AgeGroupLookup.age_group,
          t_dacso_data_part_1.prgm_credential_awarded_name,
          t_dacso_data_part_1.lcp4_cd,
          t_dacso_data_part_1.lcp4_cip_4digits_name,
          t_dacso_data_part_1.lcip4_cred
ORDER  BY AgeGroupLookup.age_group,
          t_dacso_data_part_1.prgm_credential_awarded_name;"


# ---- qry_Make_Completers_CIP4_CombinedCred ----
qry_Make_Completers_CIP4_CombinedCred <- 
  "SELECT completerscip4.age_group,
        combine_creds.combined_cred_name,
        completerscip4.lcip4_cred_cleaned,
        completerscip4.lcp4_cd,
        completerscip4.lcp4_cip_4digits_name,
        Sum(completerscip4.expr1) AS CombinedCredCount
INTO Completers_CIP4_CombinedCred
FROM completerscip4
INNER JOIN combine_creds
ON completerscip4.prgm_credential_awarded_name = combine_creds.prgm_credential_awarded_name
WHERE  (( ( combine_creds.[use_in_pssm_2017_18] ) = 'Yes' ))
GROUP  BY completerscip4.age_group,
        combine_creds.combined_cred_name,
        completerscip4.lcip4_cred_cleaned,
        completerscip4.lcp4_cd,
        completerscip4.lcp4_cip_4digits_name;"

# ---- qry_NearCompletersByCIP4CombinedCred ----
qry_NearCompletersByCIP4CombinedCred <- 
  "SELECT NearCompleters_CIP4_CombinedCred.age_group, 
NearCompleters_CIP4_CombinedCred.Combined_Cred_Name, 
NearCompleters_CIP4_CombinedCred.LCIP4_CRED, 
NearCompleters_CIP4_CombinedCred.LCP4_CD, 
NearCompleters_CIP4_CombinedCred.LCP4_CIP_4DIGITS_NAME, 
NearCompleters_CIP4_CombinedCred.CombinedCredCount AS [Count], 
NearCompleters_CIP4_CombinedCred_with_STP_Credential.CombinedCredCount AS NearCompWithSTPCred
FROM NearCompleters_CIP4_CombinedCred 
LEFT JOIN NearCompleters_CIP4_CombinedCred_with_STP_Credential 
ON (NearCompleters_CIP4_CombinedCred.LCIP4_CRED = NearCompleters_CIP4_CombinedCred_with_STP_Credential.LCIP4_CRED) AND (NearCompleters_CIP4_CombinedCred.LCP4_CIP_4DIGITS_NAME = NearCompleters_CIP4_CombinedCred_with_STP_Credential.LCP4_CIP_4DIGITS_NAME) AND (NearCompleters_CIP4_CombinedCred.LCP4_CD = NearCompleters_CIP4_CombinedCred_with_STP_Credential.LCP4_CD) AND (NearCompleters_CIP4_CombinedCred.age_group = NearCompleters_CIP4_CombinedCred_with_STP_Credential.age_group)
GROUP BY NearCompleters_CIP4_CombinedCred.age_group, NearCompleters_CIP4_CombinedCred.Combined_Cred_Name, NearCompleters_CIP4_CombinedCred.LCIP4_CRED, 
NearCompleters_CIP4_CombinedCred.LCP4_CD, NearCompleters_CIP4_CombinedCred.LCP4_CIP_4DIGITS_NAME, NearCompleters_CIP4_CombinedCred.CombinedCredCount, NearCompleters_CIP4_CombinedCred_with_STP_Credential.CombinedCredCount;"


# ---- qry99_Near_completes_total_by_Gender ----
qry99_Near_completes_total_byGender <-"
SELECT t_dacso_data_part_1.tpid_lgnd_cd,
       agegrouplookup.age_group,
       t_dacso_data_part_1.prgm_credential_awarded_name,
       Count(*) AS Count
INTO Near_completes_total_byGender
FROM   t_dacso_data_part_1
       INNER JOIN agegrouplookup
               ON t_dacso_data_part_1.age_at_grad >=
                  agegrouplookup.lower_bound
                  AND t_dacso_data_part_1.age_at_grad <=
                      agegrouplookup.upper_bound
       LEFT OUTER JOIN credentialrank
                    ON t_dacso_data_part_1.prgm_credential_awarded_name =
                       credentialrank.psi_credential_category
WHERE  ( t_dacso_data_part_1.cosc_grad_status_lgds_cd_group = '3' )
       AND ( t_dacso_data_part_1.coci_subm_cd IN ('C_Outc19', 'C_Outc20'))
GROUP  BY agegrouplookup.age_group,
          t_dacso_data_part_1.prgm_credential_awarded_name,
          t_dacso_data_part_1.tpid_lgnd_cd
HAVING ( t_dacso_data_part_1.tpid_lgnd_cd <> '0' )
ORDER  BY t_dacso_data_part_1.tpid_lgnd_cd DESC,
          agegrouplookup.age_group,
          t_dacso_data_part_1.prgm_credential_awarded_name;"

# ---- qry99_Near_completes_total_with_STP_Credential_by_Gender ----
qry99_Near_completes_total_with_STP_Credential_by_Gender <-"
SELECT t_dacso_data_part_1.tpid_lgnd_cd,
       agegrouplookup.age_group,
       t_dacso_data_part_1.prgm_credential_awarded_name,
       Count(*) AS Count,
       t_dacso_data_part_1_tempselection.has_stp_credential
INTO Near_completes_total_with_STP_Credential_by_Gender
FROM   t_dacso_data_part_1
       INNER JOIN agegrouplookup
               ON t_dacso_data_part_1.age_at_grad >=
                  agegrouplookup.lower_bound
                  AND t_dacso_data_part_1.age_at_grad <=
                      agegrouplookup.upper_bound
       INNER JOIN t_dacso_data_part_1_tempselection
               ON t_dacso_data_part_1.coci_stqu_id =
                  t_dacso_data_part_1_tempselection.coci_stqu_id
       LEFT OUTER JOIN credentialrank
                    ON t_dacso_data_part_1.prgm_credential_awarded_name =
                       credentialrank.psi_credential_category
WHERE  ( t_dacso_data_part_1.coci_subm_cd IN ('C_Outc19', 'C_Outc20'))
GROUP  BY agegrouplookup.age_group,
          t_dacso_data_part_1.prgm_credential_awarded_name,
          t_dacso_data_part_1_tempselection.has_stp_credential,
          t_dacso_data_part_1.tpid_lgnd_cd
HAVING ( t_dacso_data_part_1_tempselection.has_stp_credential = 'Yes' )
       AND ( t_dacso_data_part_1.tpid_lgnd_cd <> '0' )
ORDER  BY t_dacso_data_part_1.tpid_lgnd_cd DESC,
          agegrouplookup.age_group,
          t_dacso_data_part_1.prgm_credential_awarded_name;"

# ---- qry99_Near_completes_factoring_in_STP_total ----
qry99_Near_completes_factoring_in_STP_total <-"
SELECT agegrouplookup.age_group,
       t_dacso_data_part_1.prgm_credential_awarded_name,
       Count(*) AS Count
FROM   t_dacso_data_part_1
       INNER JOIN agegrouplookup
               ON t_dacso_data_part_1.age_at_grad >=
                  agegrouplookup.lower_bound
                  AND t_dacso_data_part_1.age_at_grad <=
                      agegrouplookup.upper_bound
       LEFT OUTER JOIN credentialrank
                    ON t_dacso_data_part_1.prgm_credential_awarded_name =
                       credentialrank.psi_credential_category
WHERE  ( t_dacso_data_part_1.grad_status_factoring_in_stp = '3' )
       AND ( t_dacso_data_part_1.coci_subm_cd IN ('C_Outc19', 'C_Outc20'))
GROUP  BY agegrouplookup.age_group,
          t_dacso_data_part_1.prgm_credential_awarded_name
ORDER  BY agegrouplookup.age_group,
          t_dacso_data_part_1.prgm_credential_awarded_name;"

# ---- qry99_Completers_agg_factoring_in_STP_Credential ----
qry99_Completers_agg_factoring_in_STP_Credential <-"
SELECT agegrouplookup_1.age_group,
       t_dacso_data_part_1.prgm_credential_awarded_name,
       Count(*) AS Expr1
FROM   t_dacso_data_part_1
       INNER JOIN agegrouplookup AS agegrouplookup_1
               ON t_dacso_data_part_1.age_at_grad >=
                  agegrouplookup_1.lower_bound
                  AND t_dacso_data_part_1.age_at_grad <=
                      agegrouplookup_1.upper_bound
       LEFT OUTER JOIN credentialrank AS CredentialRank_1
                    ON t_dacso_data_part_1.prgm_credential_awarded_name =
                       CredentialRank_1.psi_credential_category
WHERE  ( t_dacso_data_part_1.grad_status_factoring_in_stp = '1' )
       AND ( t_dacso_data_part_1.coci_subm_cd IN ('C_Outc19', 'C_Outc20')
           )
       AND ( t_dacso_data_part_1.age_at_grad >= 17 )
       AND ( t_dacso_data_part_1.age_at_grad <= 64 )
GROUP  BY agegrouplookup_1.age_group,
          t_dacso_data_part_1.prgm_credential_awarded_name,
          agegrouplookup_1.age_group
ORDER  BY agegrouplookup_1.age_group,
          t_dacso_data_part_1.prgm_credential_awarded_name;"



# ---- qry99_Near_completes_total_by_CIP4_TTRAIN ----
qry99_Near_completes_total_by_CIP4_TTRAIN <- "
SELECT agegrouplookup.age_group,
t_dacso_data_part_1.prgm_credential_awarded_name,
Count(*) AS Count,
t_dacso_data_part_1.lcip4_cred,
t_dacso_data_part_1.lcp4_cd,
t_dacso_data_part_1.lcp4_cip_4digits_name,
t_dacso_data_part_1.ttrain,
t_dacso_data_part_1.cosc_grad_status_lgds_cd_group
INTO Near_completes_total_by_CIP4_TTRAIN
FROM   t_dacso_data_part_1
INNER JOIN agegrouplookup
ON t_dacso_data_part_1.age_at_grad >= agegrouplookup.lower_bound
AND t_dacso_data_part_1.age_at_grad <= agegrouplookup.upper_bound
LEFT OUTER JOIN credentialrank
ON t_dacso_data_part_1.prgm_credential_awarded_name = credentialrank.psi_credential_category
WHERE  (t_dacso_data_part_1.cosc_grad_status_lgds_cd_group = '3')
AND (t_dacso_data_part_1.coci_subm_cd IN ('C_Outc19', 'C_Outc20'))
GROUP  BY agegrouplookup.age_group,
t_dacso_data_part_1.prgm_credential_awarded_name,
t_dacso_data_part_1.lcip4_cred,
t_dacso_data_part_1.lcp4_cd,
t_dacso_data_part_1.lcp4_cip_4digits_name,
t_dacso_data_part_1.ttrain,
t_dacso_data_part_1.cosc_grad_status_lgds_cd_group
ORDER  BY agegrouplookup.age_group,
t_dacso_data_part_1.prgm_credential_awarded_name"

# ---- qry99_Near_completes_total_with_STP_Credential_ByCIP4_TTRAIN ----
qry99_Near_completes_total_with_STP_Credential_ByCIP4_TTRAIN <- "
SELECT agegrouplookup.age_group,
       t_dacso_data_part_1.prgm_credential_awarded_name,
       Count(*) AS Count,
       t_dacso_data_part_1_tempselection.has_stp_credential,
       t_dacso_data_part_1.lcip4_cred,
       t_dacso_data_part_1.lcp4_cd,
       t_dacso_data_part_1.lcp4_cip_4digits_name,
       t_dacso_data_part_1.ttrain,
       t_dacso_data_part_1.cosc_grad_status_lgds_cd_group
INTO Near_completes_total_with_STP_Credential_ByCIP4_TTRAIN
FROM   t_dacso_data_part_1
       INNER JOIN agegrouplookup
               ON t_dacso_data_part_1.age_at_grad >=
                  agegrouplookup.lower_bound
                  AND t_dacso_data_part_1.age_at_grad <=
                      agegrouplookup.upper_bound
       INNER JOIN t_dacso_data_part_1_tempselection
               ON t_dacso_data_part_1.coci_stqu_id =
                  t_dacso_data_part_1_tempselection.coci_stqu_id
       LEFT OUTER JOIN credentialrank
                    ON t_dacso_data_part_1.prgm_credential_awarded_name =
                       credentialrank.psi_credential_category
WHERE  ( t_dacso_data_part_1.coci_subm_cd IN ('C_Outc19', 'C_Outc20') )
GROUP  BY agegrouplookup.age_group,
          t_dacso_data_part_1.prgm_credential_awarded_name,
          t_dacso_data_part_1_tempselection.has_stp_credential,
          t_dacso_data_part_1.lcip4_cred,
          t_dacso_data_part_1.lcp4_cd,
          t_dacso_data_part_1.lcp4_cip_4digits_name,
          t_dacso_data_part_1.ttrain,
          t_dacso_data_part_1.cosc_grad_status_lgds_cd_group
HAVING ( t_dacso_data_part_1_tempselection.has_stp_credential = 'Yes' )
ORDER  BY agegrouplookup.age_group,
          t_dacso_data_part_1.prgm_credential_awarded_name"

# ---- qry99_Near_completes_program_dist_count ----
qry99_Near_completes_program_dist_count <- 
"SELECT t_pssm_projection_cred_grp.pssm_credential,
       cast(near_completes_total_by_cip4_ttrain.cosc_grad_status_lgds_cd_group as nvarchar(50)) + ' - ' +
       t_pssm_projection_cred_grp.pssm_credential AS PSSM_CRED,
       near_completes_total_by_cip4_ttrain.age_group,
       near_completes_total_by_cip4_ttrain.lcip4_cred,
       near_completes_total_by_cip4_ttrain.lcp4_cd,
       near_completes_total_by_cip4_ttrain.lcp4_cip_4digits_name,
       near_completes_total_by_cip4_ttrain.cosc_grad_status_lgds_cd_group,
       near_completes_total_by_cip4_ttrain.ttrain,
       Sum(near_completes_total_by_cip4_ttrain.count) AS Count,
       Sum(Isnull(near_completes_total_with_stp_credential_bycip4_ttrain.count, 0)) AS
          Near_completers_from_C_Outc19_20_with_earlier_or_later_STP,
       near_completes_total_by_cip4_ttrain.count - 
          Isnull(near_completes_total_with_stp_credential_bycip4_ttrain.count, 0) AS
          Near_completers_STP_Credentials
INTO T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN
FROM   near_completes_total_by_cip4_ttrain
INNER JOIN t_pssm_projection_cred_grp
  ON   near_completes_total_by_cip4_ttrain.prgm_credential_awarded_name = t_pssm_projection_cred_grp.pssm_projection_credential
LEFT OUTER JOIN  near_completes_total_with_stp_credential_bycip4_ttrain
  ON   near_completes_total_by_cip4_ttrain.ttrain = near_completes_total_with_stp_credential_bycip4_ttrain.ttrain
  AND  near_completes_total_by_cip4_ttrain.age_group = near_completes_total_with_stp_credential_bycip4_ttrain.age_group
  AND  near_completes_total_by_cip4_ttrain.prgm_credential_awarded_name = near_completes_total_with_stp_credential_bycip4_ttrain.prgm_credential_awarded_name
  AND  near_completes_total_by_cip4_ttrain.lcip4_cred = near_completes_total_with_stp_credential_bycip4_ttrain.lcip4_cred
GROUP  BY near_completes_total_by_cip4_ttrain.age_group,
      near_completes_total_by_cip4_ttrain.lcip4_cred,
      near_completes_total_by_cip4_ttrain.lcp4_cd,
      near_completes_total_by_cip4_ttrain.lcp4_cip_4digits_name,
      near_completes_total_by_cip4_ttrain.ttrain,
      t_pssm_projection_cred_grp.pssm_credential,
      '3 - ' + t_pssm_projection_cred_grp.pssm_credential,
      near_completes_total_by_cip4_ttrain.count - 
      Isnull(near_completes_total_with_stp_credential_bycip4_ttrain.count, 0),
        near_completes_total_by_cip4_ttrain.cosc_grad_status_lgds_cd_group,
      cast(near_completes_total_by_cip4_ttrain.cosc_grad_status_lgds_cd_group as nvarchar(50)) + ' - ' + t_pssm_projection_cred_grp.pssm_credential;"






# ----  NOT USED ----
# ---- qry_Number_of_NearCompleters_With_STP_Credential_by_Year ----
qry_Number_of_NearCompleters_With_STP_Credential_by_Year <- 
"SELECT T_DACSO_NearCompleters.coci_SUBM_CD, Count(*) AS Expr1
FROM T_DACSO_NearCompleters
WHERE (((T_DACSO_NearCompleters.STP_Credential_Awarded_Before_DACSO)='Yes')) OR (((T_DACSO_NearCompleters.STP_Credential_Awarded_After_DACSO)='Yes'))
GROUP BY T_DACSO_NearCompleters.coci_SUBM_CD;"



# ---- qry_make_tmp_table_GradStat_step1 ----
qry_make_tmp_table_GradStat_step1 <- "
INSERT INTO tmp_tbl_Age_AppendNewYears ( COCI_STQU_ID, COCI_SUBM_CD, BTHDT, ENDDT, COCI_AGE_AT_SURVEY )
SELECT INFOWARE_SURV_COHORT_COLLECTION_INFO.COCI_STQU_ID, INFOWARE_SURV_COHORT_COLLECTION_INFO.COCI_SUBM_CD, INFOWARE_CO_COHORT_SAMPLE_2016.BTHDT, INFOWARE_CO_COHORT_SAMPLE_2016.ENDDT, INFOWARE_SURV_COHORT_COLLECTION_INFO.COCI_AGE_AT_SURVEY
FROM INFOWARE_CO_COHORT_SAMPLE_2016 INNER JOIN INFOWARE_SURV_COHORT_COLLECTION_INFO ON INFOWARE_CO_COHORT_SAMPLE_2016.STQU_ID = INFOWARE_SURV_COHORT_COLLECTION_INFO.COCI_STQU_ID
WHERE (((INFOWARE_SURV_COHORT_COLLECTION_INFO.COCI_SUBM_CD)='C_Outc16'));"

# ---- qry99_GradStatus_byCred_by_Year_Age_At_Survey ----
qry99_GradStatus_byCred_by_Year_Age_At_Survey <- 
"TRANSFORM Count(*) AS Expr1
SELECT T_DACSO_DATA_Part_1_TempSelection.PSSM_Credential, T_DACSO_DATA_Part_1_TempSelection.PSSM_Credential_Name, T_DACSO_DATA_Part_1_TempSelection.Grad_Status_Factoring_in_STP
FROM (tbl_Age INNER JOIN agegrouplookup ON tbl_Age.Age_Group = agegrouplookup.Age_Group) INNER JOIN (T_DACSO_DATA_Part_1_TempSelection INNER JOIN tmp_tbl_Age ON T_DACSO_DATA_Part_1_TempSelection.coci_STQU_ID = tmp_tbl_Age.COSC_STQU_ID) ON tbl_Age.Age = tmp_tbl_Age.COCI_AGE_AT_SURVEY
WHERE (((T_DACSO_DATA_Part_1_TempSelection.Grad_Status_Factoring_in_STP) Is Not Null) AND ((tmp_tbl_Age.COCI_AGE_AT_SURVEY)>=17 And (tmp_tbl_Age.COCI_AGE_AT_SURVEY)<=64))
GROUP BY T_DACSO_DATA_Part_1_TempSelection.PSSM_Credential, T_DACSO_DATA_Part_1_TempSelection.PSSM_Credential_Name, T_DACSO_DATA_Part_1_TempSelection.Grad_Status_Factoring_in_STP
ORDER BY T_DACSO_DATA_Part_1_TempSelection.PSSM_Credential, T_DACSO_DATA_Part_1_TempSelection.Grad_Status_Factoring_in_STP
PIVOT T_DACSO_DATA_Part_1_TempSelection.coci_SUBM_CD;"


# ---- qry99_Graduates_for_QI ----
qry99_Graduates_for_QI <- 
"SELECT T_DACSO_DATA_Part_1.COSC_GRAD_STATUS_LGDS_CD, T_DACSO_DATA_Part_1.PSSM_Credential, T_DACSO_DATA_Part_1.coci_SUBM_CD, Count(*) AS Expr1
FROM T_DACSO_DATA_Part_1
WHERE (((T_DACSO_DATA_Part_1.COCI_AGE_AT_SURVEY)>=17 And (T_DACSO_DATA_Part_1.COCI_AGE_AT_SURVEY)<=64))
GROUP BY T_DACSO_DATA_Part_1.COSC_GRAD_STATUS_LGDS_CD, T_DACSO_DATA_Part_1.PSSM_Credential, T_DACSO_DATA_Part_1.coci_SUBM_CD
HAVING (((T_DACSO_DATA_Part_1.COSC_GRAD_STATUS_LGDS_CD) Is Not Null) AND ((T_DACSO_DATA_Part_1.coci_SUBM_CD)<>'C_Outc12' And (T_DACSO_DATA_Part_1.coci_SUBM_CD)<>'C_Outc06'))
ORDER BY T_DACSO_DATA_Part_1.COSC_GRAD_STATUS_LGDS_CD, T_DACSO_DATA_Part_1.PSSM_Credential, T_DACSO_DATA_Part_1.coci_SUBM_CD;"


# ---- qry99_Near_Completes_vs_Graduates_by_Year ----
qry99_Near_Completes_vs_Graduates_by_Year <- 
"TRANSFORM Count(*) AS Expr1
SELECT agegrouplookup.Age_Group_Label, T_DACSO_DATA_Part_1.PRGM_Credential_Awarded, T_DACSO_DATA_Part_1.PRGM_Credential_Awarded_Name, T_DACSO_DATA_Part_1.coci_SUBM_CD
FROM (T_DACSO_DATA_Part_1 INNER JOIN tbl_Age ON T_DACSO_DATA_Part_1.COCI_AGE_AT_SURVEY = tbl_Age.Age) INNER JOIN agegrouplookup ON tbl_Age.Age_Group = agegrouplookup.Age_Group
WHERE (((T_DACSO_DATA_Part_1.coci_SUBM_CD)='C_Outc09' Or (T_DACSO_DATA_Part_1.coci_SUBM_CD)='C_Outc08' Or (T_DACSO_DATA_Part_1.coci_SUBM_CD)='C_Outc10') AND ((T_DACSO_DATA_Part_1.COSC_GRAD_STATUS_LGDS_CD_Group) Is Not Null) AND ((T_DACSO_DATA_Part_1.COCI_AGE_AT_SURVEY)>=17 And (T_DACSO_DATA_Part_1.COCI_AGE_AT_SURVEY)<=64))
GROUP BY agegrouplookup.Age_Group_Label, T_DACSO_DATA_Part_1.PRGM_Credential_Awarded, T_DACSO_DATA_Part_1.PRGM_Credential_Awarded_Name, T_DACSO_DATA_Part_1.coci_SUBM_CD
PIVOT T_DACSO_DATA_Part_1.COSC_GRAD_STATUS_LGDS_CD_Group;"

# ---- qry99_Near_Completes_vs_Graduates_by_Year_by_Age ----
qry99_Near_Completes_vs_Graduates_by_Year_by_Age <- 
"TRANSFORM Count(*) AS Expr1
SELECT agegrouplookup.Age_Group_Label, T_DACSO_DATA_Part_1.PSSM_Credential, T_DACSO_DATA_Part_1.PSSM_Credential_Name, T_DACSO_DATA_Part_1.COSC_GRAD_STATUS_LGDS_CD
FROM T_DACSO_DATA_Part_1 INNER JOIN (agegrouplookup INNER JOIN tbl_Age ON agegrouplookup.Age_Group = tbl_Age.Age_Group) ON T_DACSO_DATA_Part_1.COCI_AGE_AT_SURVEY = tbl_Age.Age
WHERE (((T_DACSO_DATA_Part_1.COSC_GRAD_STATUS_LGDS_CD) Is Not Null) AND ((T_DACSO_DATA_Part_1.COCI_AGE_AT_SURVEY)>=17 And (T_DACSO_DATA_Part_1.COCI_AGE_AT_SURVEY)<=64))
GROUP BY agegrouplookup.Age_Group_Label, T_DACSO_DATA_Part_1.PSSM_Credential, agegrouplookup.Age_Group, T_DACSO_DATA_Part_1.PSSM_Credential_Name, T_DACSO_DATA_Part_1.COSC_GRAD_STATUS_LGDS_CD
ORDER BY agegrouplookup.Age_Group
PIVOT T_DACSO_DATA_Part_1.coci_SUBM_CD;"

# ---- qry9999_DACSO_Q006_Occ_by_completion ----
qry9999_DACSO_Q006_Occ_by_completion <- 
"SELECT 'DACSO' AS Survey, T_DACSO_DATA_Part_1.coci_OCCUPATION_LNOC_CD, tbl_NOC_Skill_Level_Aged_17_34.MINOR_GROUP_CODE, T_Year_Survey_Year.Year_Code, Count(T_DACSO_DATA_Part_1.coci_STQU_ID) AS [Count]
FROM tbl_NOC_Skill_Level_Aged_17_34 INNER JOIN (T_DACSO_DATA_Part_1 INNER JOIN T_Year_Survey_Year ON T_DACSO_DATA_Part_1.coci_SUBM_CD=T_Year_Survey_Year.SUBM_CD) ON tbl_NOC_Skill_Level_Aged_17_34.UNIT_GROUP_CODE=T_DACSO_DATA_Part_1.coci_OCCUPATION_LNOC_CD
WHERE (((T_DACSO_DATA_Part_1.coci_OCCUPATION_LNOC_CD) Is Not Null And (T_DACSO_DATA_Part_1.coci_OCCUPATION_LNOC_CD)<>'XXXX') AND ((T_DACSO_DATA_Part_1.COSC_GRAD_STATUS_LGDS_CD)='1' Or (T_DACSO_DATA_Part_1.COSC_GRAD_STATUS_LGDS_CD)='2') AND ((T_DACSO_DATA_Part_1.Weight)<>0 And (T_DACSO_DATA_Part_1.Weight) Is Not Null))
GROUP BY 'DACSO', T_DACSO_DATA_Part_1.coci_OCCUPATION_LNOC_CD, tbl_NOC_Skill_Level_Aged_17_34.MINOR_GROUP_CODE, T_Year_Survey_Year.Year_Code;"


