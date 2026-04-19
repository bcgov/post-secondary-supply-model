# Copyright 2024 Province of British Columbia
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations under the License.

# This script computes the ratio of near completers to graduates by age group and credential
# Near completers who later received a credential according to the STP Credential 
# file or had an earlier credential are subtracted from the total of all near completers.
# 
# Age groups: 17 to 19, 20 to 24, 25 to 29, and 35 to 64
# Credentials: From Diploma, Associate Degree, and Certificate Outcomes Survey cohorts. 
# Survey years: 2018, 2019, 2020, 2021, 2022, 2023 for PSSM 2023
# STP Credential years searched: 2002/03 - 2022/23 
#
# Annual ratios are computed for all available years and an average taken of two or three representative years
# (chosen by investigation).  PSSM model 2023 used an average ratio of 2018-2019.
# Notes: Using age at grad (not age at survey) for age groupings.  


library(tidyverse)
library(RODBC)
library(config)
library(DBI)
library(RJDBC)
library(assertthat)

# ---- Configure LAN and file paths ----
db_config <- config::get("decimal")
lan <- config::get("lan")
my_schema <- config::get("myschema")

# ---- Connection to database ----
db_config <- config::get("decimal")
decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")

# ---- Data Requirements and SQL Definitons ----

# ---- qry99_Investigate_Near_Completes_vs_Graduates_by_Year  ----
qry99_Investigate_Near_Completes_vs_Graduates_by_Year <- 
  "select COSC_GRAD_STATUS_LGDS_CD_Group, [C_Outc17],[C_Outc18],[C_Outc19],[C_Outc20],[C_Outc21],[C_Outc22],[C_Outc23]
FROM
(
SELECT COSC_GRAD_STATUS_LGDS_CD_Group, COCI_STQU_ID , COCI_SUBM_CD 
FROM T_DACSO_DATA_Part_1_TempSelection
WHERE (((T_DACSO_DATA_Part_1_TempSelection.COSC_GRAD_STATUS_LGDS_CD_Group) Is Not Null) 
AND ((T_DACSO_DATA_Part_1_TempSelection.Age_At_Grad)>=17 
And (T_DACSO_DATA_Part_1_TempSelection.Age_At_Grad)<=64))
) As P
PIVOT (
  Count(COCI_STQU_ID) 
  FOR COCI_SUBM_CD 
    IN([C_Outc17],[C_Outc18],[C_Outc19],[C_Outc20],[C_Outc21],[C_Outc22],[C_Outc23])
  ) AS PT;"


# tables made in earlier part of workflow
assert_that(dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."t_dacso_data_part_1"'))))
assert_that(dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Credential_Non_Dup"'))))

# rollover tables - this can be removed later
assert_that(dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."tmp_tbl_Age"'))))

# new data
assert_that(dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."tmp_tbl_Age_AppendNewYears"'))))

# lookups
assert_that(dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."tbl_Age"'))))
assert_that(dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."t_pssm_projection_cred_grp"'))))
assert_that(dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."combine_creds"'))))
assert_that(dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."stp_dacso_prgm_credential_lookup"'))))
assert_that(dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."AgeGroupLookup"'))))

# ---- Derive Age at Grad ----
dbExecute(decimal_con, "ALTER TABLE tmp_tbl_Age_AppendNewYears ADD BTHDT_CLEANED NVARCHAR(20) NULL")
dbExecute(decimal_con, "ALTER TABLE tmp_tbl_Age_AppendNewYears ADD ENDDT_CLEANED NVARCHAR(20) NULL")
dbExecute(decimal_con, "ALTER TABLE tmp_tbl_Age_AppendNewYears ADD BTHDT_DATE NVARCHAR(20) NULL")
dbExecute(decimal_con, "ALTER TABLE tmp_tbl_Age_AppendNewYears ADD ENDDT_DATE NVARCHAR(20) NULL")

# [UPDATE] tmp_tbl_Age_AppendNewYears

#---- qry_make_tmp_table_Age_step2 ----
qry_make_tmp_table_Age_step2 <- "
UPDATE tmp_tbl_Age_AppendNewYears 
SET tmp_tbl_Age_AppendNewYears.BTHDT_CLEANED = Right(tmp_tbl_Age_AppendNewYears.BTHDT,2)+'/1/'+Left(tmp_tbl_Age_AppendNewYears.BTHDT,4), 
tmp_tbl_Age_AppendNewYears.ENDDT_CLEANED = Right(tmp_tbl_Age_AppendNewYears.ENDDT,2)+'/1/'+Left(tmp_tbl_Age_AppendNewYears.ENDDT,4);"
dbExecute(decimal_con, qry_make_tmp_table_Age_step2)
dbExecute(decimal_con, "UPDATE tmp_tbl_Age_AppendNewYears SET ENDDT_CLEANED = '' WHERE ENDDT_CLEANED = '00/1/0000'")

# [UPDATE] tmp_tbl_Age_AppendNewYears

#---- qry_make_tmp_table_Age_step3 ----
qry_make_tmp_table_Age_step3 <- "
UPDATE tmp_tbl_Age_AppendNewYears 
SET tmp_tbl_Age_AppendNewYears.BTHDT_DATE = TRY_CONVERT(DATE, tmp_tbl_Age_AppendNewYears.BTHDT_CLEANED), 
tmp_tbl_Age_AppendNewYears.ENDDT_DATE = TRY_CONVERT(DATE, tmp_tbl_Age_AppendNewYears.ENDDT_CLEANED);"
dbExecute(decimal_con, qry_make_tmp_table_Age_step3)
dbExecute(decimal_con, "UPDATE tmp_tbl_Age_AppendNewYears SET ENDDT_DATE = NULL WHERE ENDDT_DATE = '1900-01-01'")

# [INSERT INTO] tmp_tbl_Age

#---- qry_make_tmp_table_Age_step4 ----
qry_make_tmp_table_Age_step4 <- "
INSERT INTO tmp_tbl_Age ( COSC_STQU_ID, COSC_SUBM_CD, TPID_DATE_OF_BIRTH, COSC_ENRL_END_DATE, COCI_AGE_AT_SURVEY )
SELECT tmp_tbl_Age_AppendNewYears.COCI_STQU_ID, 
tmp_tbl_Age_AppendNewYears.COCI_SUBM_CD, 
tmp_tbl_Age_AppendNewYears.BTHDT_DATE, 
tmp_tbl_Age_AppendNewYears.ENDDT_DATE,
tmp_tbl_Age_AppendNewYears.COCI_AGE_AT_SURVEY
FROM tmp_tbl_Age_AppendNewYears;"
dbExecute(decimal_con, qry_make_tmp_table_Age_step4)
dbExecute(decimal_con, "DROP TABLE tmp_tbl_Age_AppendNewYears") # drop the new table

dbExecute(decimal_con, "ALTER TABLE T_DACSO_Data_Part_1 ADD Age_At_Grad FLOAT NULL")
# dbExecute(decimal_con, "ALTER TABLE tmp_tbl_age ADD Age_At_Grad FLOAT NULL") # in 'load-near-completers-ttrain.R', we read the CSV for it and it has the age at grad. 

# [UPDATE] tmp_tbl_age

#---- qry99_Update_Age_At_Grad ----
qry99_Update_Age_At_Grad <- "
UPDATE tmp_tbl_age
SET Age_At_Grad = Datediff(yyyy, tpid_date_of_birth, Isnull(cosc_grad_credential_date, cosc_enrl_end_date)) + 
CASE WHEN (Isnull(cosc_grad_credential_date, cosc_enrl_end_date) < 
    DateFromParts(Year(Isnull(cosc_grad_credential_date, cosc_enrl_end_date)), Month([tpid_date_of_birth]), Day(tpid_date_of_birth))) THEN -1 ELSE 0 END;"
dbExecute(decimal_con, qry99_Update_Age_At_Grad)

# [UPDATE] T_DACSO_DATA_Part_1

#---- qry99a_Update_Age_At_Grad ----
qry99a_Update_Age_At_Grad <- "
UPDATE  T_DACSO_DATA_Part_1
SET     Age_At_Grad = tmp_tbl_Age.Age_At_Grad
FROM    tmp_tbl_Age 
INNER JOIN T_DACSO_DATA_Part_1 
ON tmp_tbl_Age.COSC_STQU_ID = T_DACSO_DATA_Part_1.COCI_STQU_ID"
dbExecute(decimal_con, qry99a_Update_Age_At_Grad)

# use a temporary subset of columns from T_DACSO_DATA_Part_1 for selection

# [SELECT INTO] Create T_DACSO_DATA_Part_1_TempSelection from T_DACSO_DATA_Part_1


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
dbExecute(decimal_con, qry_make_T_DACSO_DATA_Part_1_TempSelection)

# [SQL]

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
dbGetQuery(decimal_con, qry99_Investigate_Near_Completes_vs_Graduates_by_Year)

# ---- Add PEN to Non-Dup table ----
# Note: Move to earlier workflow - 02 series.  This updates credential non-dup in current schema only
sql <- glue::glue("ALTER TABLE pssm2023.[{my_schema}].credential_non_dup
ADD PSI_PEN NVARCHAR(255) NULL;")
dbExecute(decimal_con, sql)

sql <- glue::glue("UPDATE N
SET N.PSI_PEN = C.PSI_PEN
FROM pssm2023.[{my_schema}].credential_non_dup AS N
INNER JOIN dbo.STP_Credential AS C
ON N.ID = C.ID
")
dbExecute(decimal_con, sql)

# ---- DACSO Matching STP Credential ----

# [SELECT INTO] Create DACSO_Matching_STP_Credential_PEN from T_DACSO_DATA_Part_1
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
dbExecute(decimal_con, qry01_Match_DACSO_to_STP_Credential_Non_DUP_on_PEN)
dbExecute(decimal_con, "ALTER TABLE dacso_matching_stp_credential_pen ADD stp_prgm_credential_awarded_name nvarchar(50) NULL")
dbExecute(decimal_con, "ALTER TABLE dacso_matching_stp_credential_pen ADD match_credential nvarchar(10) NULL")
dbExecute(decimal_con, "ALTER TABLE dacso_matching_stp_credential_pen ADD match_cip_code_4 nvarchar(10) NULL")
dbExecute(decimal_con, "ALTER TABLE dacso_matching_stp_credential_pen ADD match_CIP_CODE_2 nvarchar(10) NULL")
dbExecute(decimal_con, "ALTER TABLE dacso_matching_stp_credential_pen ADD match_award_school_year nvarchar(10) NULL")
dbExecute(decimal_con, "ALTER TABLE dacso_matching_stp_credential_pen ADD match_inst nvarchar(10) NULL")

# [UPDATE] dacso_matching_stp_credential_pen

# ---- qry_Update_STP_PRGM_Credential_Awarded_Name ----
qry_Update_STP_PRGM_Credential_Awarded_Name <- "
UPDATE dacso_matching_stp_credential_pen
SET dacso_matching_stp_credential_pen.stp_prgm_credential_awarded_name = stp_dacso_prgm_credential_lookup.stp_prgm_credential_awarded_name
FROM dacso_matching_stp_credential_pen
INNER JOIN stp_dacso_prgm_credential_lookup
  ON dacso_matching_stp_credential_pen.prgm_credential_awarded = stp_dacso_prgm_credential_lookup.prgrm_credential_awarded;"
dbExecute(decimal_con, qry_Update_STP_PRGM_Credential_Awarded_Name)

# How many PEN matched records also match STP on credential category

# [UPDATE] dacso_matching_stp_credential_pen

# ---- qry02_Match_DACSO_STP_Credential_PSI_CRED_Category ----
qry02_Match_DACSO_STP_Credential_PSI_CRED_Category <- "
UPDATE dacso_matching_stp_credential_pen
SET    dacso_matching_stp_credential_pen.match_credential = 'yes'
WHERE  dacso_matching_stp_credential_pen.prgm_credential_awarded_name = dacso_matching_stp_credential_pen.psi_credential_category;"
dbExecute(decimal_con, qry02_Match_DACSO_STP_Credential_PSI_CRED_Category)
# How many PEN matched records also match STP on CIP4

# [UPDATE] dacso_matching_stp_credential_pen

# ---- qry03_Match_DACSO_STP_Credential_CIPCODE4 ----
qry03_Match_DACSO_STP_Credential_CIPCODE4 <- "
UPDATE dacso_matching_stp_credential_pen
SET    dacso_matching_stp_credential_pen.match_cip_code_4 = 'yes'
WHERE  dacso_matching_stp_credential_pen.lcp4_cd = dacso_matching_stp_credential_pen.final_cip_code_4;"
dbExecute(decimal_con, qry03_Match_DACSO_STP_Credential_CIPCODE4)
# How many PEN matched records also match STP on CIP2

# [UPDATE] DACSO_Matching_STP_Credential_PEN

# ---- qry03b_Match_DACSO_STP_Credential_CIPCODE2 ----
qry03b_Match_DACSO_STP_Credential_CIPCODE2 <- "
UPDATE DACSO_Matching_STP_Credential_PEN 
SET DACSO_Matching_STP_Credential_PEN.Match_CIP_CODE_2 = 'Yes'
WHERE Left(LCP4_CD,2)=Left(DACSO_Matching_STP_Credential_PEN.FINAL_CIP_CODE_4,2);"
dbExecute(decimal_con, qry03b_Match_DACSO_STP_Credential_CIPCODE2)
# How many PEN matched records also match STP on Award Year. 

# [UPDATE] dacso_matching_stp_credential_pen

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
dbExecute(decimal_con, qry04_Match_DACSO_STP_Credential_AwardYear) # Manual: Add the new year combinations to query design first 
# How many PEN matched records also match STP on Inst code

# [UPDATE] dacso_matching_stp_credential_pen


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
dbExecute(decimal_con, qry05_Match_DACSO_STP_Credential_Inst)
# Print summary of the matching results.

# [SQL]

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
dbGetQuery(decimal_con, qry06_Match_DACSO_STP_Credential_Summary)

#  These are considered final matches to STP credential.
dbExecute(decimal_con, "ALTER TABLE dacso_matching_stp_credential_pen ADD final_consider_a_match nvarchar(10) NULL")
dbExecute(decimal_con, "ALTER TABLE dacso_matching_stp_credential_pen ADD match_all_4_flag nvarchar(10) NULL")

# [UPDATE] dacso_matching_stp_credential_pen

# ---- qry07_DACSO_STP_Credential_MatchAll4_Flag ----
qry07_DACSO_STP_Credential_MatchAll4_Flag <- "
UPDATE dacso_matching_stp_credential_pen
SET    dacso_matching_stp_credential_pen.final_consider_a_match = 'yes',
       dacso_matching_stp_credential_pen.match_all_4_flag = 'yes'
WHERE  ((( dacso_matching_stp_credential_pen.match_credential ) = 'yes') 
    AND (( dacso_matching_stp_credential_pen.match_cip_code_4 ) = 'yes')
    AND (( dacso_matching_stp_credential_pen.match_award_school_year ) = 'yes')
    AND (( dacso_matching_stp_credential_pen.match_inst ) = 'yes' ));"
dbExecute(decimal_con, qry07_DACSO_STP_Credential_MatchAll4_Flag)

#  Flag records that match on inst, award year, credential, and CIP 2 (but not CIP 4) as final matches too. 

# [UPDATE] dacso_matching_stp_credential_pen

# ---- qry08_DACSO_STP_Credential_Final_Match_Flag ----
qry08_DACSO_STP_Credential_Final_Match_Flag <- "
UPDATE dacso_matching_stp_credential_pen
SET    dacso_matching_stp_credential_pen.final_consider_a_match = 'yes'
WHERE  (( ( dacso_matching_stp_credential_pen.match_credential ) = 'yes')
    AND (( dacso_matching_stp_credential_pen.match_cip_code_2 ) = 'yes')
    AND (( dacso_matching_stp_credential_pen.match_cip_code_4 ) IS NULL)
    AND (( dacso_matching_stp_credential_pen.match_award_school_year ) = 'yes')
    AND (( dacso_matching_stp_credential_pen.match_inst ) = 'yes' ));"
dbExecute(decimal_con, qry08_DACSO_STP_Credential_Final_Match_Flag)

# ---- Flag near-completers with earlier or later credential----

# [SELECT INTO] Create nearcompleters_in_stp_credential_step1 from t_dacso_data_part_1

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
dbExecute(decimal_con, qry_Find_NearCompleters_in_STP_Credential_Step1)
dbExecute(decimal_con, "ALTER TABLE nearcompleters_in_stp_credential_step1 ADD STP_Credential_Awarded_Before_DACSO NVARCHAR(10) NULL")
dbExecute(decimal_con, "ALTER TABLE nearcompleters_in_stp_credential_step1 ADD STP_Credential_Awarded_After_DACSO NVARCHAR(10) NULL")
dbExecute(decimal_con, "ALTER TABLE nearcompleters_in_stp_credential_step1 ADD Has_Multiple_STP_Credentials NVARCHAR(10) NULL")

# [UPDATE] NearCompleters_in_STP_Credential_Step1

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
dbExecute(decimal_con, qry_Update_STP_Credential_Awarded_Before_DACSO)

# [UPDATE] NearCompleters_in_STP_Credential_Step1

# ---- qry_Update_STP_Credential_Awarded_After_DACSO ----
qry_Update_STP_Credential_Awarded_After_DACSO <- "
UPDATE NearCompleters_in_STP_Credential_Step1 
  SET NearCompleters_in_STP_Credential_Step1.STP_Credential_Awarded_After_DACSO = 'Yes'
WHERE (((NearCompleters_in_STP_Credential_Step1.STP_Credential_Awarded_Before_DACSO) Is Null));"
dbExecute(decimal_con, qry_Update_STP_Credential_Awarded_After_DACSO)


# [SELECT INTO] Create T_DACSO_NearCompleters from T_DACSO_DATA_Part_1

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
dbExecute(decimal_con, qry_make_table_NearCompleters)
dbExecute(decimal_con, "ALTER TABLE T_DACSO_NearCompleters ADD STP_Credential_Awarded_Before_DACSO NVARCHAR(10) NULL")
dbExecute(decimal_con, "ALTER TABLE T_DACSO_NearCompleters ADD STP_Credential_Awarded_After_DACSO NVARCHAR(10) NULL")
dbExecute(decimal_con, "ALTER TABLE T_DACSO_NearCompleters ADD Has_Multiple_STP_Credentials NVARCHAR(10) NULL")

# [UPDATE] T_DACSO_NearCompleters

# ---- qry_update_T_DACSO_Near_Completers_step1 ----
qry_update_T_DACSO_Near_Completers_step1 <- "
UPDATE T_DACSO_NearCompleters 
SET T_DACSO_NearCompleters.STP_Credential_Awarded_Before_DACSO = [NearCompleters_in_STP_Credential_Step1].[STP_Credential_Awarded_Before_DACSO]
FROM NearCompleters_in_STP_Credential_Step1
INNER JOIN T_DACSO_NearCompleters 
ON NearCompleters_in_STP_Credential_Step1.coci_STQU_ID = T_DACSO_NearCompleters.coci_STQU_ID;"
dbExecute(decimal_con, qry_update_T_DACSO_Near_Completers_step1)

# [UPDATE] t_dacso_nearcompleters

# ---- qry_update_T_DACSO_Near_Completers_step2 ----
qry_update_T_DACSO_Near_Completers_step2 <- "
UPDATE t_dacso_nearcompleters
SET   t_dacso_nearcompleters.stp_credential_awarded_after_dacso =
nearcompleters_in_stp_credential_step1.stp_credential_awarded_after_dacso
FROM nearcompleters_in_stp_credential_step1
INNER JOIN t_dacso_nearcompleters
  ON nearcompleters_in_stp_credential_step1.coci_stqu_id = t_dacso_nearcompleters.coci_stqu_id;"
dbExecute(decimal_con, qry_update_T_DACSO_Near_Completers_step2)

# ---- Flag near-completers with multiple credentials----

# [SELECT INTO] Create tmp_DACSO_NearCompleters_with_Multiple_Cdtls from NearCompleters_in_STP_Credential_Step1

# ---- qry_NearCompleters_With_More_Than_One_Cdtl ----
qry_NearCompleters_With_More_Than_One_Cdtl <- "
SELECT NearCompleters_in_STP_Credential_Step1.COCI_STQU_ID, Count(*) AS Expr1 
INTO tmp_DACSO_NearCompleters_with_Multiple_Cdtls
FROM NearCompleters_in_STP_Credential_Step1
GROUP BY NearCompleters_in_STP_Credential_Step1.COCI_STQU_ID
HAVING (((Count(*))>1));"
dbExecute(decimal_con, qry_NearCompleters_With_More_Than_One_Cdtl)

# [UPDATE] T_DACSO_NearCompleters

# ---- qry_Update_T_NearCompleters_HasMultipleCdtls ----
qry_Update_T_NearCompleters_HasMultipleCdtls <- "
UPDATE T_DACSO_NearCompleters
SET T_DACSO_NearCompleters.Has_Multiple_STP_Credentials = 'Yes'
FROM tmp_DACSO_NearCompleters_with_Multiple_Cdtls 
INNER JOIN T_DACSO_NearCompleters 
ON tmp_DACSO_NearCompleters_with_Multiple_Cdtls.COCI_STQU_ID = T_DACSO_NearCompleters.COCI_STQU_ID;"
dbExecute(decimal_con, qry_Update_T_NearCompleters_HasMultipleCdtls)


# [UPDATE] NearCompleters_in_STP_Credential_Step1

# ---- qry_Clean_NearCompleters_MultiCdtls_Step1 ----
qry_Clean_NearCompleters_MultiCdtls_Step1 <- "
UPDATE NearCompleters_in_STP_Credential_Step1 
SET NearCompleters_in_STP_Credential_Step1.Has_Multiple_STP_Credentials = 'Yes'
FROM tmp_DACSO_NearCompleters_with_Multiple_Cdtls 
INNER JOIN NearCompleters_in_STP_Credential_Step1
ON NearCompleters_in_STP_Credential_Step1.COCI_STQU_ID = tmp_DACSO_NearCompleters_with_Multiple_Cdtls.COCI_STQU_ID;"
dbExecute(decimal_con, qry_Clean_NearCompleters_MultiCdtls_Step1)

# [SELECT INTO] Create tmp_NearCompletersWithMultiCredentials_Cleaning from NearCompleters_in_STP_Credential_Step1

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
dbExecute(decimal_con, qry_NearCompleters_MultiCdtls_Cleaning_Step2)

# Find record with max psi award year

# [SELECT INTO] Create tmp_MaxAwardYear from tmp_NearCompletersWithMultiCredentials_Cleaning

# ---- qry_PickMaxYear_step ----
qry_PickMaxYear_step1 <- "
SELECT tmp_NearCompletersWithMultiCredentials_Cleaning.coci_STQU_ID, 
Max(tmp_NearCompletersWithMultiCredentials_Cleaning.PSI_AWARD_SCHOOL_YEAR) AS MaxOfPSI_AWARD_SCHOOL_YEAR 
INTO tmp_MaxAwardYear
FROM tmp_NearCompletersWithMultiCredentials_Cleaning
GROUP BY tmp_NearCompletersWithMultiCredentials_Cleaning.coci_STQU_ID;"
dbExecute(decimal_con, qry_PickMaxYear_step1)
dbExecute(decimal_con, "ALTER TABLE tmp_NearCompletersWithMultiCredentials_Cleaning ADD Max_Award_School_Year NVARCHAR(10) NULL")

# [UPDATE] tmp_NearCompletersWithMultiCredentials_Cleaning

# ---- qry_NearCompleters_MultiCdtls_Cleaning_Step3 ----
qry_NearCompleters_MultiCdtls_Cleaning_Step3 <- "
UPDATE tmp_NearCompletersWithMultiCredentials_Cleaning
SET tmp_NearCompletersWithMultiCredentials_Cleaning.Max_Award_School_Year = 'Yes'
FROM tmp_MaxAwardYear 
INNER JOIN tmp_NearCompletersWithMultiCredentials_Cleaning 
ON (tmp_MaxAwardYear.MaxOfPSI_AWARD_SCHOOL_YEAR = tmp_NearCompletersWithMultiCredentials_Cleaning.PSI_AWARD_SCHOOL_YEAR) 
AND (tmp_MaxAwardYear.coci_STQU_ID = tmp_NearCompletersWithMultiCredentials_Cleaning.coci_STQU_ID);"
dbExecute(decimal_con, qry_NearCompleters_MultiCdtls_Cleaning_Step3)

dbExecute(decimal_con, "ALTER TABLE NearCompleters_in_STP_Credential_Step1 ADD Dup_STQUID_UseThisRecord NVARCHAR(10) NULL")

# [UPDATE] NearCompleters_in_STP_Credential_Step1

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
dbExecute(decimal_con, qry_NearCompleters_MultiCdtls_Cleaning_Step4)

# [SELECT INTO] Create tmp_NearCompletersWithMultiCredentials_MaxYear from NearCompleters_in_STP_Credential_Step1

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
dbExecute(decimal_con, qry_NearCompleters_MultiCdtls_Cleaning_Step5)

# [SELECT INTO] Create tmp_NearCompletersWithMultiCredentials_MaxYearCleaning from tmp_NearCompletersWithMultiCredentials_MaxYear

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
dbExecute(decimal_con, qry_NearCompleters_MultiCdtls_Cleaning_Step6)

# [SELECT INTO] Create tmp_MaxAwardYearCleaning_MaxID from tmp_NearCompletersWithMultiCredentials_MaxYearCleaning

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
dbExecute(decimal_con, qry_PickMaxYear_Step2)
dbExecute(decimal_con, "ALTER TABLE tmp_NearCompletersWithMultiCredentials_MaxYearCleaning ADD Final_Record_To_Use NVARCHAR(10) NULL")

# [UPDATE] tmp_NearCompletersWithMultiCredentials_MaxYearCleaning

# ---- qry_PickMaxYear_Step3 ----
qry_PickMaxYear_Step3 <- "
UPDATE tmp_NearCompletersWithMultiCredentials_MaxYearCleaning 
SET tmp_NearCompletersWithMultiCredentials_MaxYearCleaning.Final_Record_To_Use = 'Yes'
FROM tmp_NearCompletersWithMultiCredentials_MaxYearCleaning
INNER JOIN tmp_MaxAwardYearCleaning_MaxID 
ON (tmp_NearCompletersWithMultiCredentials_MaxYearCleaning.Dup_STQUID_UseThisRecord = tmp_MaxAwardYearCleaning_MaxID.Dup_STQUID_UseThisRecord) 
AND (tmp_NearCompletersWithMultiCredentials_MaxYearCleaning.ID = tmp_MaxAwardYearCleaning_MaxID.MaxOfID) 
AND (tmp_NearCompletersWithMultiCredentials_MaxYearCleaning.coci_STQU_ID = tmp_MaxAwardYearCleaning_MaxID.coci_STQU_ID);"
dbExecute(decimal_con, qry_PickMaxYear_Step3)
dbExecute(decimal_con, "ALTER TABLE NearCompleters_in_STP_Credential_Step1 ADD Final_Record_To_Use NVARCHAR(10) NULL")

# [UPDATE] NearCompleters_in_STP_Credential_Step1

# ---- qry_NearCompleters_MultiCdtls_Cleaning_Step10 ----
qry_NearCompleters_MultiCdtls_Cleaning_Step10 <- "
UPDATE NearCompleters_in_STP_Credential_Step1 
SET NearCompleters_in_STP_Credential_Step1.Final_Record_to_Use = 'Yes'
FROM NearCompleters_in_STP_Credential_Step1 
INNER JOIN tmp_NearCompletersWithMultiCredentials_MaxYearCleaning 
ON (NearCompleters_in_STP_Credential_Step1.ID = tmp_NearCompletersWithMultiCredentials_MaxYearCleaning.ID) 
AND (NearCompleters_in_STP_Credential_Step1.coci_STQU_ID = tmp_NearCompletersWithMultiCredentials_MaxYearCleaning.coci_STQU_ID) 
WHERE (((tmp_NearCompletersWithMultiCredentials_MaxYearCleaning.Final_Record_To_Use)='Yes'));"
dbExecute(decimal_con, qry_NearCompleters_MultiCdtls_Cleaning_Step10)

# [UPDATE] T_DACSO_NearCompleters

# ---- qry_NearCompleters_MultiCdtls_Cleaning_Step13 ----
qry_NearCompleters_MultiCdtls_Cleaning_Step13 <- "
UPDATE T_DACSO_NearCompleters 
SET T_DACSO_NearCompleters.STP_Credential_Awarded_Before_DACSO = [NearCompleters_in_STP_Credential_Step1].[STP_Credential_Awarded_Before_DACSO], 
    T_DACSO_NearCompleters.STP_Credential_Awarded_After_DACSO = [NearCompleters_in_STP_Credential_Step1].[STP_Credential_Awarded_After_DACSO]
FROM T_DACSO_NearCompleters
INNER JOIN NearCompleters_in_STP_Credential_Step1 
ON T_DACSO_NearCompleters.coci_STQU_ID = NearCompleters_in_STP_Credential_Step1.coci_STQU_ID 
WHERE (((NearCompleters_in_STP_Credential_Step1.Final_Record_to_Use)='Yes'));"
dbExecute(decimal_con, qry_NearCompleters_MultiCdtls_Cleaning_Step13)

dbExecute(decimal_con, "ALTER TABLE DACSO_Matching_STP_Credential_PEN ADD Dup_STQUID_UseThisRecord NVARCHAR(10) NULL")

# [UPDATE] DACSO_Matching_STP_Credential_PEN


# ---- qry_Update_DupStqu_ID_UseThisRecord2 ----
qry_Update_DupStqu_ID_UseThisRecord2 <- "
UPDATE DACSO_Matching_STP_Credential_PEN 
SET DACSO_Matching_STP_Credential_PEN.Dup_STQUID_UseThisRecord = 'Yes'
FROM DACSO_Matching_STP_Credential_PEN 
INNER JOIN tmp_NearCompletersWithMultiCredentials_MaxYearCleaning 
ON (tmp_NearCompletersWithMultiCredentials_MaxYearCleaning.ID = DACSO_Matching_STP_Credential_PEN.ID) 
AND (DACSO_Matching_STP_Credential_PEN.COCI_STQU_ID = tmp_NearCompletersWithMultiCredentials_MaxYearCleaning.COCI_STQU_ID)
WHERE (((tmp_NearCompletersWithMultiCredentials_MaxYearCleaning.Final_Record_To_Use)='Yes'));"
dbExecute(decimal_con, qry_Update_DupStqu_ID_UseThisRecord2)
#dbExecute(decimal_con, "ALTER TABLE NearCompleters_in_STP_Credential_Step1 ADD Final_Record_To_Use NVARCHAR(10) NULL")

# [UPDATE] NearCompleters_in_STP_Credential_Step1

# ---- qry_Update_Final_Record_To_Use_NearCompletersDups ----
qry_Update_Final_Record_To_Use_NearCompletersDups <- "
UPDATE NearCompleters_in_STP_Credential_Step1 
SET NearCompleters_in_STP_Credential_Step1.Final_Record_to_Use = tmp_NearCompletersWithMultiCredentials_MaxYearCleaning.Final_Record_To_Use
FROM NearCompleters_in_STP_Credential_Step1 
INNER JOIN tmp_NearCompletersWithMultiCredentials_MaxYearCleaning 
ON (NearCompleters_in_STP_Credential_Step1.ID = tmp_NearCompletersWithMultiCredentials_MaxYearCleaning.ID) 
AND (NearCompleters_in_STP_Credential_Step1.coci_STQU_ID = tmp_NearCompletersWithMultiCredentials_MaxYearCleaning.coci_STQU_ID)"
dbExecute(decimal_con, qry_Update_Final_Record_To_Use_NearCompletersDups)

# [UPDATE] NearCompleters_in_STP_Credential_Step1

# ---- qry_NearCompleters_MultiCdtls_Cleaning_Step12 ----
qry_NearCompleters_MultiCdtls_Cleaning_Step12 <- 
"UPDATE NearCompleters_in_STP_Credential_Step1 
SET NearCompleters_in_STP_Credential_Step1.Final_Record_to_Use = 'Yes'
WHERE (((NearCompleters_in_STP_Credential_Step1.Final_Record_to_Use) Is Null) 
AND ((NearCompleters_in_STP_Credential_Step1.Has_Multiple_STP_Credentials) Is Null));"
dbExecute(decimal_con, qry_NearCompleters_MultiCdtls_Cleaning_Step12)
dbExecute(decimal_con, "ALTER TABLE T_DACSO_NearCompleters ADD STP_Credential_Awarded_Before_DACSO_Final NVARCHAR(10) NULL")
dbExecute(decimal_con, "ALTER TABLE T_DACSO_NearCompleters ADD STP_Credential_Awarded_After_DACSO_Final NVARCHAR(10) NULL")

# [UPDATE] T_DACSO_NearCompleters

# ---- qry_Update_Final_STP_Cred_Before_or_After_Step1 ----
qry_Update_Final_STP_Cred_Before_or_After_Step1 <- "
UPDATE T_DACSO_NearCompleters 
SET T_DACSO_NearCompleters.STP_Credential_Awarded_Before_DACSO_FINAL = NearCompleters_in_STP_Credential_Step1.STP_Credential_Awarded_Before_DACSO, 
    T_DACSO_NearCompleters.STP_Credential_Awarded_After_DACSO_FINAL  = NearCompleters_in_STP_Credential_Step1.STP_Credential_Awarded_After_DACSO
FRoM T_DACSO_NearCompleters 
INNER JOIN NearCompleters_in_STP_Credential_Step1 
ON T_DACSO_NearCompleters.coci_STQU_ID = NearCompleters_in_STP_Credential_Step1.coci_STQU_ID 
WHERE (((NearCompleters_in_STP_Credential_Step1.Final_Record_to_Use)='Yes'));"
dbExecute(decimal_con, qry_Update_Final_STP_Cred_Before_or_After_Step1)

dbExecute(decimal_con, "ALTER TABLE T_DACSO_DATA_Part_1_TempSelection ADD Has_STP_Credential NVARCHAR(10) NULL")
dbExecute(decimal_con, "ALTER TABLE T_DACSO_Data_Part_1 ADD Has_STP_Credential NVARCHAR(10)")

# [UPDATE] T_DACSO_DATA_Part_1_TempSelection

# ---- qry_update_Has_STP_Credential ----
qry_update_Has_STP_Credential <- "
UPDATE T_DACSO_DATA_Part_1_TempSelection 
SET T_DACSO_DATA_Part_1_TempSelection.Has_STP_Credential = 'Yes'
FROM T_DACSO_DATA_Part_1_TempSelection 
INNER JOIN T_DACSO_NearCompleters 
ON T_DACSO_DATA_Part_1_TempSelection.coci_STQU_ID = T_DACSO_NearCompleters.coci_STQU_ID 
WHERE (((T_DACSO_NearCompleters.STP_Credential_Awarded_Before_DACSO)='Yes')) 
OR (((T_DACSO_NearCompleters.STP_Credential_Awarded_After_DACSO)='Yes'));"
dbExecute(decimal_con, qry_update_Has_STP_Credential)

dbExecute(decimal_con, "ALTER TABLE T_DACSO_Data_Part_1 ADD Grad_Status_Factoring_in_STP nvarchar(2) NULL")
dbExecute(decimal_con,"ALTER TABLE T_DACSO_DATA_Part_1_TempSelection ADD Grad_Status_Factoring_in_STP NVARCHAR(10) NULL")

# [UPDATE] T_DACSO_DATA_Part_1_TempSelection

# ---- qry_update_Grad_Status_Factoring_in_STP_step1 ----
qry_update_Grad_Status_Factoring_in_STP_step1 <- "
UPDATE T_DACSO_DATA_Part_1_TempSelection 
SET T_DACSO_DATA_Part_1_TempSelection.Grad_Status_Factoring_in_STP = T_DACSO_DATA_Part_1_TempSelection.COSC_GRAD_STATUS_LGDS_CD_Group;"
dbExecute(decimal_con,  qry_update_Grad_Status_Factoring_in_STP_step1)

# [UPDATE] T_DACSO_DATA_Part_1_TempSelection

# ----  qry_update_Grad_Status_Factoring_in_STP_step2 ----
qry_update_Grad_Status_Factoring_in_STP_step2 <- "
UPDATE T_DACSO_DATA_Part_1_TempSelection 
SET T_DACSO_DATA_Part_1_TempSelection.Grad_Status_Factoring_in_STP = '1'
WHERE (((T_DACSO_DATA_Part_1_TempSelection.Grad_Status_Factoring_in_STP)='3') 
AND ((T_DACSO_DATA_Part_1_TempSelection.Has_STP_Credential)='Yes'));"
dbExecute(decimal_con,  qry_update_Grad_Status_Factoring_in_STP_step2) 

dbExecute(decimal_con, "UPDATE T_DACSO_DATA_Part_1 
                        SET Has_STP_Credential = T_DACSO_DATA_Part_1_TempSelection.Has_STP_Credential,
                            Grad_Status_Factoring_In_STP = T_DACSO_DATA_Part_1_TempSelection.Grad_Status_Factoring_In_STP
                        FROM T_DACSO_DATA_Part_1 INNER JOIN T_DACSO_DATA_Part_1_TempSelection 
                        ON T_DACSO_DATA_Part_1.COCI_STQU_ID = T_DACSO_DATA_Part_1_TempSelection.COCI_STQU_ID")

dbExecute(decimal_con, "DROP TABLE tmp_NearCompletersWithMultiCredentials_Cleaning")
dbExecute(decimal_con, "DROP TABLE tmp_NearCompletersWithMultiCredentials_MaxYear")
dbExecute(decimal_con, "DROP TABLE tmp_NearCompletersWithMultiCredentials_MaxYearCleaning")
dbExecute(decimal_con, "DROP TABLE T_DACSO_NearCompleters")
dbExecute(decimal_con, "DROP TABLE tmp_MaxAwardYear")
dbExecute(decimal_con, "DROP TABLE tmp_DACSO_NearCompleters_with_Multiple_Cdtls")
dbExecute(decimal_con, "DROP TABLE tmp_MaxAwardYearCleaning_MaxID")
dbExecute(decimal_con, "DROP TABLE DACSO_Matching_STP_Credential_PEN")
dbExecute(decimal_con, "DROP TABLE nearcompleters_in_stp_credential_step1")

# ----- Check Near Completers Ratios -----
dbGetQuery(decimal_con, qry99_Investigate_Near_Completes_vs_Graduates_by_Year)

# [SQL]


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
dbGetQuery(decimal_con, qry99_GradStatus_Factoring_in_STP_Credential_by_Year)

# [SQL]

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
dbGetQuery(decimal_con, qry99_GradStatus_byCred_by_Year_Age_At_Grad) 

# [SQL]

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
dbGetQuery(decimal_con, qry99_GradStatus_Factoring_in_STP_byCred_by_Year_Age_At_Grad)

# [SQL]

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
dbGetQuery(decimal_con, qry_details_of_STP_Credential_Matching) 

# Queries are for Excel: C_Outc12_13_14RatiosAgeGradCIP4
#1 (col H in Excel sheet)

# [SELECT INTO] Create NearCompleters_CIP4 from T_DACSO_DATA_Part_1

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
dbExecute(decimal_con, qry99_Near_completes_total_by_CIP4)

# [SELECT INTO] Create nearcompleters_cip4_combinedcred from nearcompleters_cip4

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
dbExecute(decimal_con, qry_Make_NearCompleters_CIP4_CombinedCred) 
NearCompleters_CIP4_CombinedCred <- dbReadTable(decimal_con, "NearCompleters_CIP4_CombinedCred")
NearCompleters_CIP4_CombinedCred$lcip4_cred <- gsub("-\\s(0|1)\\s","", NearCompleters_CIP4_CombinedCred$lcip4_cred)
NearCompleters_CIP4_CombinedCred <- NearCompleters_CIP4_CombinedCred %>% 
  summarise(count = sum(CombinedCredCount, na.rm = TRUE), .by = c(age_group, lcip4_cred, lcp4_cd))

#2 (col I in Excel sheet)

# [SELECT INTO] Create NearCompleters_CIP4_with_STP_Credential from T_DACSO_DATA_Part_1

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
dbExecute(decimal_con, qry99_Near_completes_total_with_STP_Credential_ByCIP4)

# [SELECT INTO] Create NearCompleters_CIP4_With_STP_CombinedCred from nearcompleters_cip4_with_stp_credential

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
dbExecute(decimal_con, qry_Make_NearCompleters_CIP4_With_STP_CombinedCred)
NearCompleters_CIP4_With_STP_CombinedCred <- dbReadTable(decimal_con, "NearCompleters_CIP4_With_STP_CombinedCred")
NearCompleters_CIP4_With_STP_CombinedCred$lcip4_cred <- gsub("-\\s(0|1)\\s","", NearCompleters_CIP4_With_STP_CombinedCred$lcip4_cred)
NearCompleters_CIP4_With_STP_CombinedCred <- NearCompleters_CIP4_With_STP_CombinedCred %>% 
  summarise(nc_with_earlier_or_later = sum(CombinedCredCount, na.rm = TRUE), .by = c(age_group, lcip4_cred, lcp4_cd))

  
#3 (col K in Excel sheet)

# [SELECT INTO] Create completersfactoringinstp_cip4 from t_dacso_data_part_1

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
dbExecute(decimal_con, qry99_Completers_agg_factoring_in_STP_Credential_by_CIP4)
dbExecute(decimal_con, "alter table completersfactoringinstp_cip4 add lcip4_cred_cleaned nvarchar(50) NULL;")
dbExecute(decimal_con, "update completersfactoringinstp_cip4 
                        set lcip4_cred_cleaned = 
                        	CASE WHEN PATINDEX('%1 - %', lcip4_cred) = 1 THEN STUFF(lcip4_cred, 1, 3,'3 -')  
                        	ELSE lcip4_cred
                        	END
                        from completersfactoringinstp_cip4")


# [SELECT INTO] Create CompletersFactoringInSTP_CIP4_CombinedCred from completersfactoringinstp_cip4

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
dbExecute(decimal_con, qry_Make_CompletersFactoringInSTP_CIP4_CombinedCred)
CompletersFactoringInSTP_CIP4_CombinedCred <- dbReadTable(decimal_con, "CompletersFactoringInSTP_CIP4_CombinedCred")
CompletersFactoringInSTP_CIP4_CombinedCred$lcip4_cred <- gsub("-\\s(0|1)\\s","", CompletersFactoringInSTP_CIP4_CombinedCred$lcip4_cred_cleaned)
CompletersFactoringInSTP_CIP4_CombinedCred <- CompletersFactoringInSTP_CIP4_CombinedCred %>% 
  summarise(completers = sum(CombinedCredCount, na.rm = TRUE), .by = c(age_group, lcip4_cred, lcp4_cd))


#4 (col M in Excel sheet)

# [SELECT INTO] Create completerscip4 from t_dacso_data_part_1

# END HISTORICAL ----

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
WHERE  ( t_dacso_data_part_1.coci_subm_cd IN ('C_Outc19', 'C_Outc20'))
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
dbExecute(decimal_con, qry99_Completers_agg_byCIP4)
dbExecute(decimal_con, "alter table completerscip4 add lcip4_cred_cleaned nvarchar(50) NULL;")
dbExecute(decimal_con, "update completerscip4 
                        set lcip4_cred_cleaned = 
                        	CASE WHEN PATINDEX('%1 - %', lcip4_cred) = 1 THEN STUFF(lcip4_cred, 1, 3,'3 -') 
                        	ELSE lcip4_cred
                        	END
                        from completerscip4")


# [SELECT INTO] Create Completers_CIP4_CombinedCred from completerscip4


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
dbExecute(decimal_con, qry_Make_Completers_CIP4_CombinedCred) 
Completers_CIP4_CombinedCred <- dbReadTable(decimal_con, "Completers_CIP4_CombinedCred")
Completers_CIP4_CombinedCred$lcip4_cred <- gsub("-\\s(0|1)\\s","", Completers_CIP4_CombinedCred$lcip4_cred_cleaned)
Completers_CIP4_CombinedCred  <- Completers_CIP4_CombinedCred  %>% 
  summarise(c_not_factoring_stp = sum(CombinedCredCount, na.rm = TRUE), .by = c(age_group, lcip4_cred, lcp4_cd))

T_DACSO_Near_Completers_RatioAgeAtGradCIP4 <- NearCompleters_CIP4_CombinedCred %>%
  left_join(NearCompleters_CIP4_With_STP_CombinedCred, by = join_by(age_group, lcip4_cred, lcp4_cd)) %>%
  left_join(CompletersFactoringInSTP_CIP4_CombinedCred, by = join_by(age_group, lcip4_cred, lcp4_cd)) %>%
  left_join(Completers_CIP4_CombinedCred, by = join_by(age_group, lcip4_cred, lcp4_cd)) %>%
  mutate(across(where(is.numeric), ~replace_na(.,0))) %>%
  mutate(near_completers_stp_cred = count-nc_with_earlier_or_later, 
         ratio = near_completers_stp_cred/completers, 
         ratio_not_factoring_stp = near_completers_stp_cred/c_not_factoring_stp) %>%
  mutate(across(where(is.double), ~na_if(., Inf)))%>%
  mutate_all(function(x) ifelse(is.nan(x), NA, x))

dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."T_DACSO_Near_Completers_RatioAgeAtGradCIP4"')), T_DACSO_Near_Completers_RatioAgeAtGradCIP4)
dbExecute(decimal_con, "DROP TABLE NearCompleters_CIP4")
dbExecute(decimal_con, "DROP TABLE NearCompleters_CIP4_with_STP_Credential")
dbExecute(decimal_con, "DROP TABLE completersfactoringinstp_cip4")
dbExecute(decimal_con, "DROP TABLE completerscip4")

# Queries are for Excel: C_Outc12_13_14RatiosByGender
#1: paste to col E

# [SELECT INTO] Create Near_completes_total_byGender from t_dacso_data_part_1


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
dbExecute(decimal_con, qry99_Near_completes_total_byGender)
Near_completes_total_byGender <-  dbReadTable(decimal_con, "Near_completes_total_byGender")
dbExecute(decimal_con, "DROP TABLE Near_completes_total_byGender")

#2: paste to col F

# [SELECT INTO] Create Near_completes_total_with_STP_Credential_by_Gender from t_dacso_data_part_1

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
dbExecute(decimal_con, qry99_Near_completes_total_with_STP_Credential_by_Gender)
Near_completes_total_with_STP_Credential_by_Gender <- dbReadTable(decimal_con, "Near_completes_total_with_STP_Credential_by_Gender") %>% 
  rename("nc_with_early_or_late" = "Count")  %>% 
  select(-has_stp_credential)
dbExecute(decimal_con, "DROP TABLE Near_completes_total_with_STP_Credential_by_Gender")

#3: looks like paste to H (check)

# [SELECT INTO] Create Completers_agg_by_gender from T_DACSO_DATA_Part_1

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
dbExecute(decimal_con, qry99_Completers_agg_by_gender) 
Completers_agg_by_gender <- dbReadTable(decimal_con, "Completers_agg_by_gender") %>%
  rename("completers" = "Count")
dbExecute(decimal_con, "DROP TABLE Completers_agg_by_gender")

ratio.df = Near_completes_total_byGender %>% 
  left_join(Near_completes_total_with_STP_Credential_by_Gender)  %>%
  left_join (Completers_agg_by_gender) %>%
  rename("gender" = "tpid_lgnd_cd")

# we want the adjusted ratio from column L (or just the normal ratio for nc for this year)
ratio.df  <- ratio.df %>%
  mutate(across(where(is.numeric), ~replace_na(.,0))) %>%
  mutate(n_nc_stp = Count - nc_with_early_or_late) %>%
  mutate(ratio = n_nc_stp/completers)

ratio.df2 <- ratio.df %>%
    filter(prgm_credential_awarded_name %in% c("Associate Degree", "University Transfer")) %>%
    mutate(prgm_credential_awarded_name = "Associate Degree") %>%
    summarise(ratio_adgt= sum(n_nc_stp)/sum(completers), .by = c(gender, age_group, prgm_credential_awarded_name))

T_DACSO_Near_Completers_RatioByGender <- 
  ratio.df %>% 
  left_join(ratio.df2) %>%
  mutate(ratio = if_else(prgm_credential_awarded_name %in% c("Associate Degree", "University Transfer"), ratio_adgt, ratio)) %>%
  mutate(across(where(is.double), ~na_if(., Inf))) %>%
  mutate_all(function(x) ifelse(is.nan(x), NA, x)) %>%
  select(-ratio_adgt)

dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."T_DACSO_Near_Completers_RatioByGender"')), T_DACSO_Near_Completers_RatioByGender)

# 4. Same as above (3.) but by year - to get historical 

# 4.1: paste to col E

# [SELECT INTO] Create Near_completes_total_byGender_year from t_dacso_data_part_1

# ---- qry99_Near_completes_total_byGender ----
qry99_Near_completes_total_byGender_year <-"
SELECT t_dacso_data_part_1.coci_subm_cd, 
  t_dacso_data_part_1.tpid_lgnd_cd,
       agegrouplookup.age_group,
       t_dacso_data_part_1.prgm_credential_awarded_name,
       Count(*) AS Count
INTO Near_completes_total_byGender_year
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
GROUP  BY agegrouplookup.age_group, t_dacso_data_part_1.coci_subm_cd,
          t_dacso_data_part_1.prgm_credential_awarded_name,
          t_dacso_data_part_1.tpid_lgnd_cd
HAVING ( t_dacso_data_part_1.tpid_lgnd_cd <> '0' )
ORDER  BY t_dacso_data_part_1.tpid_lgnd_cd DESC,
          agegrouplookup.age_group,
          t_dacso_data_part_1.prgm_credential_awarded_name;"
dbExecute(decimal_con, qry99_Near_completes_total_byGender_year)
Near_completes_total_byGender_year <-  dbReadTable(decimal_con, "Near_completes_total_byGender_year")
dbExecute(decimal_con, "DROP TABLE Near_completes_total_byGender_year")

# 4.2: paste to col F

# [SELECT INTO] Create Near_completes_total_with_STP_Credential_by_Gender_year from t_dacso_data_part_1

# ---- qry99_Near_completes_total_with_STP_Credential_by_Gender ----
qry99_Near_completes_total_with_STP_Credential_by_Gender_year <-"
SELECT t_dacso_data_part_1.tpid_lgnd_cd,
t_dacso_data_part_1.coci_subm_cd,
       agegrouplookup.age_group,
       t_dacso_data_part_1.prgm_credential_awarded_name,
       Count(*) AS Count,
       t_dacso_data_part_1_tempselection.has_stp_credential
INTO Near_completes_total_with_STP_Credential_by_Gender_year
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
GROUP  BY agegrouplookup.age_group,
t_dacso_data_part_1.coci_subm_cd,
          t_dacso_data_part_1.prgm_credential_awarded_name,
          t_dacso_data_part_1_tempselection.has_stp_credential,
          t_dacso_data_part_1.tpid_lgnd_cd
HAVING ( t_dacso_data_part_1_tempselection.has_stp_credential = 'Yes' )
       AND ( t_dacso_data_part_1.tpid_lgnd_cd <> '0' )
ORDER  BY t_dacso_data_part_1.tpid_lgnd_cd DESC,
          agegrouplookup.age_group,
          t_dacso_data_part_1.prgm_credential_awarded_name;"
dbExecute(decimal_con, qry99_Near_completes_total_with_STP_Credential_by_Gender_year)
Near_completes_total_with_STP_Credential_by_Gender_year <- dbReadTable(decimal_con, "Near_completes_total_with_STP_Credential_by_Gender_year") %>% 
  rename("nc_with_early_or_late" = "Count")  %>% 
  select(-has_stp_credential)
dbExecute(decimal_con, "DROP TABLE Near_completes_total_with_STP_Credential_by_Gender_year")

# 4.3 get full ratio 

# [SELECT INTO] Create Completers_agg_by_gender_age_year from T_DACSO_DATA_Part_1

# HISTORICAL ----
# ---- qry99_Completers_agg_by_gender_age_year ----
qry99_Completers_agg_by_gender_age_year <-
  "SELECT     T_DACSO_DATA_Part_1.coci_subm_cd, T_DACSO_DATA_Part_1.tpid_lgnd_cd, agegrouplookup.age_group, 
T_DACSO_DATA_Part_1.prgm_credential_awarded_name, COUNT(*)
AS Count
INTO Completers_agg_by_gender_age_year
FROM         T_DACSO_DATA_Part_1 
INNER JOIN agegrouplookup
ON T_DACSO_DATA_Part_1.Age_At_Grad >= agegrouplookup.lower_bound 
AND T_DACSO_DATA_Part_1.Age_At_Grad <= agegrouplookup.upper_bound 
LEFT OUTER JOIN CredentialRank
ON T_DACSO_DATA_Part_1.PRGM_Credential_Awarded_Name = CredentialRank.PSI_CREDENTIAL_CATEGORY
WHERE     
    (T_DACSO_DATA_Part_1.Age_At_Grad >= 17) 
AND (T_DACSO_DATA_Part_1.Age_At_Grad <= 64) 
AND (T_DACSO_DATA_Part_1.COSC_GRAD_STATUS_LGDS_CD_Group = '1')
GROUP BY 
T_DACSO_DATA_Part_1.COCI_SUBM_CD,
agegrouplookup.age_group, 
T_DACSO_DATA_Part_1.PRGM_Credential_Awarded_Name, 
agegrouplookup.age_group, 
T_DACSO_DATA_Part_1.TPID_LGND_CD
HAVING      (T_DACSO_DATA_Part_1.TPID_LGND_CD <> '0')
ORDER BY T_DACSO_DATA_Part_1.TPID_LGND_CD Desc, 
agegrouplookup.age_group, 
T_DACSO_DATA_Part_1.PRGM_Credential_Awarded_Name"
dbExecute(decimal_con, qry99_Completers_agg_by_gender_age_year) 
Completers_agg_by_gender_age_year <- dbReadTable(decimal_con, "Completers_agg_by_gender_age_year") %>%
  rename("completers" = "Count")
dbExecute(decimal_con, "DROP TABLE Completers_agg_by_gender_age_year")

ratio.df = Near_completes_total_byGender_year %>% 
  left_join(Near_completes_total_with_STP_Credential_by_Gender_year)  %>%
  left_join(Completers_agg_by_gender_age_year) %>%
  rename("gender" = "tpid_lgnd_cd")

# we want the adjusted ratio from column L (or just the normal ratio for nc for this year)
ratio.df  <- ratio.df %>%
  mutate(across(where(is.numeric), ~replace_na(.,0))) %>%
  mutate(n_nc_stp = Count - nc_with_early_or_late) %>%
  mutate(ratio = n_nc_stp/completers)

ratio.df2 <- ratio.df %>%
  filter(prgm_credential_awarded_name %in% c("Associate Degree", "University Transfer")) %>%
  mutate(prgm_credential_awarded_name = "Associate Degree") %>%
  summarise(ratio_adgt= sum(n_nc_stp)/sum(completers), .by = c(gender, age_group, prgm_credential_awarded_name))

# my question here - is this the right year to switch to?
# in lookup table, DACSO data should be sent back by one 
T_DACSO_Near_Completers_RatioByGender_year <- 
  ratio.df %>% 
  left_join(ratio.df2) %>%
  mutate(ratio = if_else(prgm_credential_awarded_name %in% c("Associate Degree", "University Transfer"), ratio_adgt, ratio)) %>%
  mutate(across(where(is.double), ~na_if(., Inf))) %>%
  mutate_all(function(x) ifelse(is.nan(x), NA, x)) %>%
  select(-ratio_adgt) %>% 
  # subtract one here so that it's the first half of the school year
  mutate(
    year = as.numeric(paste0('20', str_sub(coci_subm_cd, 7,8)))-1
  )

dbWriteTable(decimal_con, name = SQL(glue::glue('"{my_schema}"."T_DACSO_Near_Completers_RatioByGender_year"')), T_DACSO_Near_Completers_RatioByGender_year)

# random query

# [SQL]

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
#dbGetQuery(decimal_con, qry99_Near_completes_factoring_in_STP_total)

# ---- TTRAIN tables ----
# This part is not completed  - see documentation
# Note: the first query filters on cosc_grad_status_lgds_cd_group = '3'

# [SELECT INTO] Create Near_completes_total_by_CIP4_TTRAIN from t_dacso_data_part_1


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
dbExecute(decimal_con, qry99_Near_completes_total_by_CIP4_TTRAIN)

# [SELECT INTO] Create Near_completes_total_with_STP_Credential_ByCIP4_TTRAIN from t_dacso_data_part_1

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
dbExecute(decimal_con, qry99_Near_completes_total_with_STP_Credential_ByCIP4_TTRAIN)

# [SELECT INTO] Create T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN from near_completes_total_by_cip4_ttrain

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
dbExecute(decimal_con, qry99_Near_completes_program_dist_count) 

dbExecute(decimal_con, "DROP TABLE Near_completes_total_by_CIP4_TTRAIN")
dbExecute(decimal_con, "DROP TABLE Near_completes_total_with_STP_Credential_ByCIP4_TTRAIN")

# ---- HISTORICAL TTRAIN queries ----
# note: this uses the same intermediate table names as the above, so make sure the 2 drops are performed

# [SELECT INTO] Create Near_completes_total_by_CIP4_TTRAIN from t_dacso_data_part_1


# ---- HISTORICAL ttrain tables ----
# ---- qry99_Near_completes_total_by_CIP4_TTRAIN_history ----
qry99_Near_completes_total_by_CIP4_TTRAIN_history <- "
SELECT agegrouplookup.age_group, t_dacso_data_part_1.coci_subm_cd,
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
GROUP  BY agegrouplookup.age_group,
t_dacso_data_part_1.coci_subm_cd,
t_dacso_data_part_1.prgm_credential_awarded_name,
t_dacso_data_part_1.lcip4_cred,
t_dacso_data_part_1.lcp4_cd,
t_dacso_data_part_1.lcp4_cip_4digits_name,
t_dacso_data_part_1.ttrain,
t_dacso_data_part_1.cosc_grad_status_lgds_cd_group
ORDER  BY agegrouplookup.age_group,
t_dacso_data_part_1.prgm_credential_awarded_name"
dbExecute(decimal_con, qry99_Near_completes_total_by_CIP4_TTRAIN_history)

# [SELECT INTO] Create Near_completes_total_with_STP_Credential_ByCIP4_TTRAIN from t_dacso_data_part_1

# ---- qry99_Near_completes_total_with_STP_Credential_ByCIP4_TTRAIN ----

qry99_Near_completes_total_with_STP_Credential_ByCIP4_TTRAIN_history <- "
SELECT agegrouplookup.age_group, t_dacso_data_part_1.coci_subm_cd,
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
GROUP  BY agegrouplookup.age_group, t_dacso_data_part_1.coci_subm_cd,
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
dbExecute(decimal_con, qry99_Near_completes_total_with_STP_Credential_ByCIP4_TTRAIN_history)

# [SELECT INTO] Create T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history from Near_completes_total_by_CIP4_TTRAIN

# ---- qry99_Near_completes_program_dist_count ----
qry99_Near_completes_program_dist_count_history <- 
  "SELECT t_pssm_projection_cred_grp.pssm_credential,
       cast(near_completes_total_by_cip4_ttrain.cosc_grad_status_lgds_cd_group as nvarchar(50)) + ' - ' +
       t_pssm_projection_cred_grp.pssm_credential AS PSSM_CRED,
       near_completes_total_by_cip4_ttrain.age_group,
       near_completes_total_by_cip4_ttrain.coci_subm_cd,
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
INTO T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN_history
FROM  Near_completes_total_by_CIP4_TTRAIN
INNER JOIN t_pssm_projection_cred_grp
  ON   near_completes_total_by_cip4_ttrain.prgm_credential_awarded_name = t_pssm_projection_cred_grp.pssm_projection_credential
LEFT OUTER JOIN  near_completes_total_with_stp_credential_bycip4_ttrain
  ON   near_completes_total_by_cip4_ttrain.ttrain = near_completes_total_with_stp_credential_bycip4_ttrain.ttrain
  AND  near_completes_total_by_cip4_ttrain.age_group = near_completes_total_with_stp_credential_bycip4_ttrain.age_group
  AND  near_completes_total_by_cip4_ttrain.prgm_credential_awarded_name = near_completes_total_with_stp_credential_bycip4_ttrain.prgm_credential_awarded_name
  AND  near_completes_total_by_cip4_ttrain.lcip4_cred = near_completes_total_with_stp_credential_bycip4_ttrain.lcip4_cred
  AND  near_completes_total_by_cip4_ttrain.coci_subm_cd = near_completes_total_with_stp_credential_bycip4_ttrain.coci_subm_cd
GROUP  BY near_completes_total_by_cip4_ttrain.age_group,
      near_completes_total_by_cip4_ttrain.coci_subm_cd,
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
dbExecute(decimal_con, qry99_Near_completes_program_dist_count_history) 

dbExecute(decimal_con, "DROP TABLE Near_completes_total_by_CIP4_TTRAIN")
dbExecute(decimal_con, "DROP TABLE Near_completes_total_with_STP_Credential_ByCIP4_TTRAIN")


# ---- Clean Up ----
# TODO: clean up this section
dbExecute(decimal_con, "DROP TABLE stp_dacso_prgm_credential_lookup")
dbExecute(decimal_con, "DROP TABLE tmp_tbl_Age")
dbExecute(decimal_con, "DROP TABLE tbl_Age")
dbExecute(decimal_con, "DROP TABLE AgeGroupLookup")
dbExecute(decimal_con, "DROP TABLE T_DACSO_DATA_Part_1_TempSelection")
dbExecute(decimal_con, "DROP TABLE combine_creds")
dbExecute(decimal_con, "DROP TABLE t_pssm_projection_cred_grp")
dbExecute(decimal_con, "drop table nearcompleters_cip4_combinedcred")
dbExecute(decimal_con, "drop table NearCompleters_CIP4_With_STP_CombinedCred")
dbExecute(decimal_con, "drop table CompletersFactoringInSTP_CIP4_CombinedCred")
dbExecute(decimal_con, "drop table Completers_CIP4_CombinedCred")

# ---- Keep for program projections ----
dbExistsTable(decimal_con, "T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN")
dbExistsTable(decimal_con, "T_DACSO_Near_Completers_RatioAgeAtGradCIP4")
dbExistsTable(decimal_con, "T_DACSO_Near_Completers_RatioByGender")


