# ---- qry01_Match_DACSO_to_STP_Credential_Non_DUP_on_PEN ----
qry01_Match_DACSO_to_STP_Credential_Non_DUP_on_PEN <- "
SELECT 
    T_DACSO_DATA_Part_1.COCI_STQU_ID, 
    T_DACSO_DATA_Part_1.COCI_INST_CD,
    Credential_Non_Dup.ID, 
    T_DACSO_DATA_Part_1.COCI_PEN,  
    Credential_Non_Dup.PSI_CODE, 
    T_DACSO_DATA_Part_1.PRGM_Credential_Awarded, 
    T_DACSO_DATA_Part_1.PRGM_Credential_Awarded_Name, 
    T_DACSO_DATA_Part_1.PSSM_Credential, 
    T_DACSO_DATA_Part_1.PSSM_Credential_Name, 
    Credential_Non_Dup.PSI_CREDENTIAL_CATEGORY, 
    Credential_Non_Dup.OUTCOMES_CRED, 
    T_DACSO_DATA_Part_1.LCP4_CD, 
    Credential_Non_Dup.FINAL_CIP_CODE_4, 
    T_DACSO_DATA_Part_1.COCI_SUBM_CD, 
    Credential_Non_Dup.PSI_AWARD_SCHOOL_YEAR, 
    T_DACSO_DATA_Part_1.COSC_GRAD_STATUS_LGDS_CD_Group 
INTO DACSO_Matching_STP_Credential_PEN
FROM T_DACSO_DATA_Part_1 
INNER JOIN Credential_Non_Dup 
    ON T_DACSO_DATA_Part_1.COCI_PEN = Credential_Non_Dup.PSI_PEN
GROUP BY 
    T_DACSO_DATA_Part_1.COCI_STQU_ID, 
    T_DACSO_DATA_Part_1.COCI_INST_CD,
    Credential_Non_Dup.ID, 
    T_DACSO_DATA_Part_1.COCI_PEN,  
    Credential_Non_Dup.PSI_CODE, 
    T_DACSO_DATA_Part_1.PRGM_Credential_Awarded, 
    T_DACSO_DATA_Part_1.PRGM_Credential_Awarded_Name, 
    T_DACSO_DATA_Part_1.PSSM_Credential, 
    T_DACSO_DATA_Part_1.PSSM_Credential_Name, 
    Credential_Non_Dup.PSI_CREDENTIAL_CATEGORY, 
    Credential_Non_Dup.OUTCOMES_CRED, 
    T_DACSO_DATA_Part_1.LCP4_CD, 
    Credential_Non_Dup.FINAL_CIP_CODE_4, 
    T_DACSO_DATA_Part_1.COCI_SUBM_CD, 
    Credential_Non_Dup.PSI_AWARD_SCHOOL_YEAR, 
    T_DACSO_DATA_Part_1.COSC_GRAD_STATUS_LGDS_CD_Group 
HAVING (((T_DACSO_DATA_Part_1.COCI_PEN)<> ' '));"

# ---- qry_Update_STP_PRGM_Credential_Awarded_Name ----
qry_Update_STP_PRGM_Credential_Awarded_Name <- "
UPDATE dacso_matching_stp_credential_pen
SET dacso_matching_stp_credential_pen.stp_prgm_credential_awarded_name = stp_dacso_prgm_credential_lookup.stp_prgm_credential_awarded_name
FROM dacso_matching_stp_credential_pen
INNER JOIN stp_dacso_prgm_credential_lookup
  ON dacso_matching_stp_credential_pen.prgm_credential_awarded = stp_dacso_prgm_credential_lookup.prgrm_credential_awarded;"

# ---- qry02_Match_DACSO_STP_Credential_PSI_CRED_Category ----
qry02_Match_DACSO_STP_Credential_PSI_CRED_Category <- "
UPDATE dacso_matching_stp_credential_pen
SET    dacso_matching_stp_credential_pen.match_credential = 'yes'
WHERE  dacso_matching_stp_credential_pen.prgm_credential_awarded_name = dacso_matching_stp_credential_pen.psi_credential_category;"

# ---- qry03_Match_DACSO_STP_Credential_CIPCODE4 ----
qry03_Match_DACSO_STP_Credential_CIPCODE4 <- "
UPDATE dacso_matching_stp_credential_pen
SET    dacso_matching_stp_credential_pen.match_cip_code_4 = 'yes'
WHERE  dacso_matching_stp_credential_pen.lcp4_cd = dacso_matching_stp_credential_pen.final_cip_code_4;"

# ---- qry03b_Match_DACSO_STP_Credential_CIPCODE2 ----
qry03b_Match_DACSO_STP_Credential_CIPCODE2 <- "
UPDATE DACSO_Matching_STP_Credential_PEN 
SET DACSO_Matching_STP_Credential_PEN.Match_CIP_CODE_2 = 'Yes'
WHERE Left(LCP4_CD,2)=Left(DACSO_Matching_STP_Credential_PEN.FINAL_CIP_CODE_4,2);"

# ---- qry04_Match_DACSO_STP_Credential_AwardYear ----
qry04_Match_DACSO_STP_Credential_AwardYear <- "
UPDATE dacso_matching_stp_credential_pen
SET    dacso_matching_stp_credential_pen.match_award_school_year = 'yes'
WHERE  ( ( ( dacso_matching_stp_credential_pen.coci_subm_cd ) = 'C_Outc06' ) AND ( ( dacso_matching_stp_credential_pen.psi_award_school_year ) = '2003/2004' ) )
OR ( ( ( dacso_matching_stp_credential_pen.coci_subm_cd ) = 'C_Outc06' ) AND ( ( dacso_matching_stp_credential_pen.psi_award_school_year ) = '2004/2005' ) )
OR ( ( ( dacso_matching_stp_credential_pen.coci_subm_cd ) = 'C_Outc07' ) AND ( ( dacso_matching_stp_credential_pen.psi_award_school_year ) = '2004/2005' ) )
OR ( ( ( dacso_matching_stp_credential_pen.coci_subm_cd ) = 'C_Outc07' ) AND ( ( dacso_matching_stp_credential_pen.psi_award_school_year ) = '2005/2006' ) )
OR ( ( ( dacso_matching_stp_credential_pen.coci_subm_cd ) = 'C_Outc08' ) AND ( ( dacso_matching_stp_credential_pen.psi_award_school_year ) = '2005/2006' ) )
OR ( ( ( dacso_matching_stp_credential_pen.coci_subm_cd ) = 'C_Outc08' ) AND ( ( dacso_matching_stp_credential_pen.psi_award_school_year ) = '2006/2007' ) )
OR ( ( ( dacso_matching_stp_credential_pen.coci_subm_cd ) = 'C_Outc09' ) AND ( ( dacso_matching_stp_credential_pen.psi_award_school_year ) = '2006/2007' ) ) 
OR ( ( ( dacso_matching_stp_credential_pen.coci_subm_cd ) = 'C_Outc09' ) AND ( ( dacso_matching_stp_credential_pen.psi_award_school_year ) = '2007/2008' ) )
OR ( ( ( dacso_matching_stp_credential_pen.coci_subm_cd ) = 'C_Outc10' ) AND ( ( dacso_matching_stp_credential_pen.psi_award_school_year ) = '2007/2008' ) )
OR ( ( ( dacso_matching_stp_credential_pen.coci_subm_cd ) = 'C_Outc10' ) AND ( ( dacso_matching_stp_credential_pen.psi_award_school_year ) = '2008/2009' ) )
OR ( ( ( dacso_matching_stp_credential_pen.coci_subm_cd ) = 'C_Outc11' ) AND ( ( dacso_matching_stp_credential_pen.psi_award_school_year ) = '2008/2009' ) )
OR ( ( ( dacso_matching_stp_credential_pen.coci_subm_cd ) = 'C_Outc11' ) AND ( ( dacso_matching_stp_credential_pen.psi_award_school_year ) = '2009/2010' ) )
OR ( ( ( dacso_matching_stp_credential_pen.coci_subm_cd ) = 'C_Outc12' ) AND ( ( dacso_matching_stp_credential_pen.psi_award_school_year ) = '2009/2010' ) )
OR ( ( ( dacso_matching_stp_credential_pen.coci_subm_cd ) = 'C_Outc12' ) AND ( ( dacso_matching_stp_credential_pen.psi_award_school_year ) = '2010/2011' ) )
OR ( ( ( dacso_matching_stp_credential_pen.coci_subm_cd ) = 'C_Outc13' ) AND ( ( dacso_matching_stp_credential_pen.psi_award_school_year ) = '2010/2011' ) )
OR ( ( ( dacso_matching_stp_credential_pen.coci_subm_cd ) = 'C_Outc13' ) AND ( ( dacso_matching_stp_credential_pen.psi_award_school_year ) = '2011/2012' ) )
OR ( ( ( dacso_matching_stp_credential_pen.coci_subm_cd ) = 'C_Outc14' ) AND ( ( dacso_matching_stp_credential_pen.psi_award_school_year ) = '2011/2012' ) )
OR ( ( ( dacso_matching_stp_credential_pen.coci_subm_cd ) = 'C_Outc14' ) AND ( ( dacso_matching_stp_credential_pen.psi_award_school_year ) = '2012/2013' ) )
OR ( ( ( dacso_matching_stp_credential_pen.coci_subm_cd ) = 'C_Outc15' ) AND ( ( dacso_matching_stp_credential_pen.psi_award_school_year ) = '2012/2013' ) )
OR ( ( ( dacso_matching_stp_credential_pen.coci_subm_cd ) = 'C_Outc15' ) AND ( ( dacso_matching_stp_credential_pen.psi_award_school_year ) = '2013/2014' ) )
OR ( ( ( dacso_matching_stp_credential_pen.coci_subm_cd ) = 'C_Outc16' ) AND ( ( dacso_matching_stp_credential_pen.psi_award_school_year ) = '2013/2014' ) )
OR ( ( ( dacso_matching_stp_credential_pen.coci_subm_cd ) = 'C_Outc16' ) AND ( ( dacso_matching_stp_credential_pen.psi_award_school_year ) = '2014/2015' ) )
OR ( ( ( dacso_matching_stp_credential_pen.coci_subm_cd ) = 'C_Outc17' ) AND ( ( dacso_matching_stp_credential_pen.psi_award_school_year ) = '2014/2015' ) )
OR ( ( ( dacso_matching_stp_credential_pen.coci_subm_cd ) = 'C_Outc17' ) AND ( ( dacso_matching_stp_credential_pen.psi_award_school_year ) = '2015/2016' ) )
OR ( ( ( dacso_matching_stp_credential_pen.coci_subm_cd ) = 'C_Outc18' ) AND ( ( dacso_matching_stp_credential_pen.psi_award_school_year ) = '2015/2016' ) )
OR ( ( ( dacso_matching_stp_credential_pen.coci_subm_cd ) = 'C_Outc18' ) AND ( ( dacso_matching_stp_credential_pen.psi_award_school_year ) = '2016/2017' ) )
OR ( ( ( dacso_matching_stp_credential_pen.coci_subm_cd ) = 'C_Outc19' ) AND ( ( dacso_matching_stp_credential_pen.psi_award_school_year ) = '2016/2017' ) )
OR ( ( ( dacso_matching_stp_credential_pen.coci_subm_cd ) = 'C_Outc19' ) AND ( ( dacso_matching_stp_credential_pen.psi_award_school_year ) = '2017/2018' ) )
OR ( ( ( dacso_matching_stp_credential_pen.coci_subm_cd ) = 'C_Outc20' ) AND ( ( dacso_matching_stp_credential_pen.psi_award_school_year ) = '2017/2018' ) )
OR ( ( ( dacso_matching_stp_credential_pen.coci_subm_cd ) = 'C_Outc20' ) AND ( ( dacso_matching_stp_credential_pen.psi_award_school_year ) = '2018/2019' ) )
OR ( ( ( dacso_matching_stp_credential_pen.coci_subm_cd ) = 'C_Outc21' ) AND ( ( dacso_matching_stp_credential_pen.psi_award_school_year ) = '2018/2019' ) )
OR ( ( ( dacso_matching_stp_credential_pen.coci_subm_cd ) = 'C_Outc21' ) AND ( ( dacso_matching_stp_credential_pen.psi_award_school_year ) = '2019/2020' ) )
OR ( ( ( dacso_matching_stp_credential_pen.coci_subm_cd ) = 'C_Outc22' ) AND ( ( dacso_matching_stp_credential_pen.psi_award_school_year ) = '2019/2020' ) )
OR ( ( ( dacso_matching_stp_credential_pen.coci_subm_cd ) = 'C_Outc22' ) AND ( ( dacso_matching_stp_credential_pen.psi_award_school_year ) = '2020/2021' ) )
OR ( ( ( dacso_matching_stp_credential_pen.coci_subm_cd ) = 'C_Outc23' ) AND ( ( dacso_matching_stp_credential_pen.psi_award_school_year ) = '2020/2021' ) )
OR ( ( ( dacso_matching_stp_credential_pen.coci_subm_cd ) = 'C_Outc23' ) AND ( ( dacso_matching_stp_credential_pen.psi_award_school_year ) = '2021/2022' ) );"


# ---- qry05_Match_DACSO_STP_Credential_Inst ----
qry05_Match_DACSO_STP_Credential_Inst <- "
UPDATE dacso_matching_stp_credential_pen
SET    dacso_matching_stp_credential_pen.match_inst = 'yes'
WHERE PSI_CODE = COCI_INST_CD
Or (((PSI_CODE)='CAP') And ((COCI_INST_CD)='CAPU')) 
Or (((PSI_CODE)='KWAN') And ((COCI_INST_CD)='KPU')) 
Or (((PSI_CODE)='OLA') And ((COCI_INST_CD)='TRU')) 
Or (((PSI_CODE)='MALA') And ((COCI_INST_CD)='VIU')) 
Or (((PSI_CODE)='OUC') And ((COCI_INST_CD)='OKAN')) 
Or (((PSI_CODE)='UCFV') And ((COCI_INST_CD)='UFV')) 
Or (((PSI_CODE)='UCC') And ((COCI_INST_CD)='TRU')) 
Or (((PSI_CODE)='NWCC') And ((COCI_INST_CD)='CMTN'));"

# ---- qry06_Match_DACSO_STP_Credential_Summary ----
qry06_Match_DACSO_STP_Credential_Summary <- "
SELECT dacso_matching_stp_credential_pen.match_credential,
       dacso_matching_stp_credential_pen.match_cip_code_4,
       dacso_matching_stp_credential_pen.match_award_school_year,
       dacso_matching_stp_credential_pen.match_inst,
       Count(*) AS Expr1
FROM   dacso_matching_stp_credential_pen
GROUP  BY dacso_matching_stp_credential_pen.match_credential,
          dacso_matching_stp_credential_pen.match_cip_code_4,
          dacso_matching_stp_credential_pen.match_award_school_year,
          dacso_matching_stp_credential_pen.match_inst
ORDER  BY dacso_matching_stp_credential_pen.match_credential DESC,
          dacso_matching_stp_credential_pen.match_cip_code_4 DESC,
          dacso_matching_stp_credential_pen.match_award_school_year DESC,
          dacso_matching_stp_credential_pen.match_inst DESC;"

# ---- qry07_DACSO_STP_Credential_MatchAll4_Flag ----
qry07_DACSO_STP_Credential_MatchAll4_Flag <- "
UPDATE dacso_matching_stp_credential_pen
SET    dacso_matching_stp_credential_pen.final_consider_a_match = 'yes',
       dacso_matching_stp_credential_pen.match_all_4_flag = 'yes'
WHERE  ((( dacso_matching_stp_credential_pen.match_credential ) = 'yes') 
    AND (( dacso_matching_stp_credential_pen.match_cip_code_4 ) = 'yes')
    AND (( dacso_matching_stp_credential_pen.match_award_school_year ) = 'yes')
    AND (( dacso_matching_stp_credential_pen.match_inst ) = 'yes' ));"

# ---- qry08_DACSO_STP_Credential_Final_Match_Flag ----
qry08_DACSO_STP_Credential_Final_Match_Flag <- "
UPDATE dacso_matching_stp_credential_pen
SET    dacso_matching_stp_credential_pen.final_consider_a_match = 'yes'
WHERE  (( ( dacso_matching_stp_credential_pen.match_credential ) = 'yes')
    AND (( dacso_matching_stp_credential_pen.match_cip_code_2 ) = 'yes')
    AND (( dacso_matching_stp_credential_pen.match_cip_code_4 ) IS NULL)
    AND (( dacso_matching_stp_credential_pen.match_award_school_year ) = 'yes')
    AND (( dacso_matching_stp_credential_pen.match_inst ) = 'yes' ));"
