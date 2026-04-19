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

# This script creates static and projected distributions from several sources.
#  - Apprenticeship and TTRAIN distributions are derived from program cohort summaries 
#    built in workflow 2b (T_Cohorts_Recoded)
#  - Near Completers distributions by age and CIP were summarized in workflow 3, the 
#    source data is those students in the DACSO program survey cohort, who (did or did not?)
#    receive an earlier or later credential.
#  - the remainder are derived from Credential Non Dup table and tblCredential_HighestRank 
#
# At a high level, the script:
#   Adds near completers to projected and static distribution data sets (Y1)
#   Adds program cohorts to static distribution data sets (Y1)
#   Adds masters and doctorates to static distribution data sets (Y1)
#   Adds apprenticeships to static and projected data sets (Y1)
#   Creates static distributions for apprenticeships and near-completers (Y2-12) 
#   Creates projected distributions for apprenticeships and near-completers (Y2-12), holding Y2-12 constant.  
#   Creates projected distributions all other credentials (Y2-Y12) 
#     - uses R program written by Werner and adapted by Ian
#   
# Includes: generally age groups are 17-19, 20-24, 25-30, 30-34, 35-44, 45-54, 55-64
# Year 1: 2019/2020 
# Year 2+: 2020/2021 - 2030/2031
# Notes: Years need to be updated each model run.  Check we are projecting 12 years.  Also which age groupings 
# will we be using?
# FIXME: lookups T_APPR_Y2_to_Y10 and T_Cohort_Program_Distributions_Y2_to_Y12 ID fields aren't sequential
#        keep eyes open for impacts of this.
#        04-graduate-projections: remove space in final table name, add survey column and populate

library(tidyverse)
library(RODBC)
library(config)
library(DBI)

# ---- Configure LAN and file paths ----
db_config <- config::get("decimal")
lan <- config::get("lan")
my_schema <- config::get("myschema")

# ---- Connection to decimal ----
db_config <- config::get("decimal")
decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")

# ---- Check for required data tables ----
# Load necessary libraries
library(DBI)
library(glue)
library(assertthat)

# List of required tables for Derived Tables, Rollovers, and Lookups
required_tables <- c(
  # Derived tables
  "T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN",
  "tbl_Program_Projection_Input",
  "T_Cohorts_Recoded",
  
  # Rollovers from last run
  "Cohort_Program_Distributions_Projected",
  "Cohort_Program_Distributions_Static",
  
  # Lookups
  "INFOWARE_L_CIP_4DIGITS_CIP2016",
  "INFOWARE_L_CIP_6DIGITS_CIP2016",
  "T_PSSM_Projection_Cred_Grp",
  "T_Weights_STP",
  "tbl_Age_Groups_Near_Completers",
  
  # Note table
  "T_Cohort_Program_Distributions_Y2_to_Y12"
)

# Check for required data tables in the database
for (table_name in required_tables) {
  # Build SQL statement
  full_table_name <- SQL(glue::glue('"{my_schema}"."{table_name}"'))
  
  # Assert that the table exists in the database
  assert_that(
    dbExistsTable(decimal_con, full_table_name),
    msg = paste("Error:", table_name, "does not exist in schema", my_schema)
  )
}


# ---- survey == "PTIB" (Static and Projected) ----
if (ptib_run == TRUE) {
  dbExecute(
    decimal_con,
    "INSERT INTO Cohort_Program_Distributions_Projected
            (Survey, PSSM_Credential, PSSM_CRED, LCP4_CD, LCIP4_CRED, LCIP2_CRED, Age_Group, [Year], [Count], Total, [Percent] )
            SELECT Survey, Credential, PSSM_CRED, LCP4_CD, LCIP4_CRED, LCIP2_CRED, Age_Group, [Year], [Count], Total, [Percent]
            FROM qry_Private_Credentials_06d1_Cohort_Dist;"
  )
  dbExecute(
    decimal_con,
    "INSERT INTO Cohort_Program_Distributions_Static
            ( Survey, PSSM_Credential, PSSM_CRED, LCP4_CD, LCIP4_CRED, LCIP2_CRED, Age_Group, [Year], [Count], Total, [Percent] )
            SELECT Survey, Credential, PSSM_CRED, LCP4_CD, LCIP4_CRED, LCIP2_CRED, Age_Group, [Year], [Count], Total, [Percent]
            FROM qry_Private_Credentials_06d1_Cohort_Dist;"
  )
  dbExecute(decimal_con,
            "DROP TABLE qry_Private_Credentials_06d1_Cohort_Dist")
}

# ---- survey == 'Program_Projections_2023-2024_qry_13d' (Static and Projected) ----
# Add near completers to projected and static distribution datasets

# [DELETE] Cohort_Program_Distributions_Projected

# ---- qry_13a0_Delete_Near_Completers_Projected ----
qry_13a0_Delete_Near_Completers_Projected <-
"DELETE
	FROM Cohort_Program_Distributions_Projected
	WHERE (((Cohort_Program_Distributions_Projected.PSSM_CRED) Like '3 - %'));"
dbExecute(decimal_con, qry_13a0_Delete_Near_Completers_Projected)

# [DELETE] Cohort_Program_Distributions_Static

# ---- qry_13a0_Delete_Near_Completers_Static ----
qry_13a0_Delete_Near_Completers_Static <-
"DELETE
	FROM Cohort_Program_Distributions_Static
	WHERE (((Cohort_Program_Distributions_Static.PSSM_CRED) Like '3 - %'));"
dbExecute(decimal_con, qry_13a0_Delete_Near_Completers_Static)

# [SELECT INTO] Create qry_13a_Near_completers from T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN

# ---- qry_13a_Near_completers ----
qry_13a_Near_completers <-
"SELECT T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN.PSSM_Credential,
        T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN.PSSM_CRED,
        T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN.LCP4_CD,
        T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN.COSC_GRAD_STATUS_LGDS_CD_Group,
        T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN.TTRAIN AS COSC_TTRAIN,
        T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN.LCIP4_CRED,
        CAST([COSC_GRAD_STATUS_LGDS_CD_Group] as NVARCHAR(50)) + ' - ' + Left([LCP4_CD],2) + ' - ' + CAST([TTRAIN] as NVARCHAR(50)) + ' - ' + [PSSM_Credential] AS LCIP2_CRED,
        T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN.Age_Group as AgeGroup,
        Sum(T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN.[Near_completers_STP_Credentials]) AS [Count]
	INTO    qry_13a_Near_completers
	FROM    T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN
	GROUP BY T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN.PSSM_Credential,
        T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN.PSSM_CRED,
        T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN.LCP4_CD,
        T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN.COSC_GRAD_STATUS_LGDS_CD_Group,
        T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN.TTRAIN,
        T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN.LCIP4_CRED,
        CAST([COSC_GRAD_STATUS_LGDS_CD_Group] as NVARCHAR(50)) + ' - ' + Left([LCP4_CD],2) + ' - ' + CAST([TTRAIN] as NVARCHAR(50)) + ' - ' + [PSSM_Credential],
        T_DACSO_Near_Completers_RatiosAgeAtGradCIP4_TTRAIN.Age_Group;"
dbExecute(decimal_con, qry_13a_Near_completers)

# [SELECT INTO] Create qry_13b_Near_Completers_Total from qry_13a_Near_completers

# ---- qry_13b_Near_Completers_Total ----
qry_13b_Near_Completers_Total <-
"SELECT qry_13a_Near_completers.PSSM_Credential,
        qry_13a_Near_completers.PSSM_CRED,
        qry_13a_Near_completers.AgeGroup,
        Sum(qry_13a_Near_completers.Count) AS Totals
	INTO    qry_13b_Near_Completers_Total
	FROM    qry_13a_Near_completers
	GROUP BY qry_13a_Near_completers.PSSM_Credential,
        qry_13a_Near_completers.PSSM_CRED,
        qry_13a_Near_completers.AgeGroup;"
dbExecute(decimal_con, qry_13b_Near_Completers_Total)

# [SELECT INTO] Create qry_13c_Near_Completers_Program_Dist from qry_13b_Near_Completers_Total

# ---- qry_13c_Near_Completers_Program_Dist ----
qry_13c_Near_Completers_Program_Dist <-
"SELECT qry_13a_Near_completers.PSSM_Credential,
        qry_13a_Near_completers.PSSM_CRED,
        qry_13a_Near_completers.LCP4_CD,
        qry_13a_Near_completers.COSC_GRAD_STATUS_LGDS_CD_Group,
        qry_13a_Near_completers.COSC_TTRAIN,
        qry_13a_Near_completers.LCIP4_CRED,
        qry_13a_Near_completers.LCIP2_CRED,
        qry_13a_Near_completers.AgeGroup,
        qry_13a_Near_completers.Count,
        qry_13b_Near_Completers_Total.Totals,
        IIf([Totals]=0,0, cast(Count AS float)/cast(Totals as float)) AS [%]
	INTO    qry_13c_Near_Completers_Program_Dist
	FROM    qry_13b_Near_Completers_Total
	INNER JOIN qry_13a_Near_completers
	  ON    (qry_13b_Near_Completers_Total.AgeGroup = qry_13a_Near_completers.AgeGroup)
	  AND   (qry_13b_Near_Completers_Total.PSSM_CRED = qry_13a_Near_completers.PSSM_CRED);"
dbExecute(decimal_con, qry_13c_Near_Completers_Program_Dist)

# [INSERT INTO] Cohort_Program_Distributions_Projected


# ---- qry_13d_Append_Near_Completers_Program_Dist_Projected_TTRAIN ----
qry_13d_Append_Near_Completers_Program_Dist_Projected_TTRAIN <-
"INSERT INTO  Cohort_Program_Distributions_Projected
	        (Survey, PSSM_Credential, PSSM_CRED, LCP4_CD, GRAD_STATUS, TTRAIN, LCIP4_CRED, LCIP2_CRED, Age_Group, [Year], [Count], Total, [Percent] )
	SELECT 'Program_Projections_2023-2024_qry_13d' AS Survey,
	        qry_13c_Near_Completers_Program_Dist.PSSM_Credential,
	        qry_13c_Near_Completers_Program_Dist.PSSM_CRED,
	        qry_13c_Near_Completers_Program_Dist.LCP4_CD,
	        qry_13c_Near_Completers_Program_Dist.COSC_GRAD_STATUS_LGDS_CD_Group,
	        qry_13c_Near_Completers_Program_Dist.COSC_TTRAIN,
	        qry_13c_Near_Completers_Program_Dist.LCIP4_CRED,
	        qry_13c_Near_Completers_Program_Dist.LCIP2_CRED,
	        tbl_Age_Groups_Near_Completers.Age_Group_Label_Graduate_Projection AS AgeGroup, '2023/2024' AS Projection_Year,
	        qry_13c_Near_Completers_Program_Dist.Count, qry_13c_Near_Completers_Program_Dist.Totals,
	        qry_13c_Near_Completers_Program_Dist.[%]
	FROM    qry_13c_Near_Completers_Program_Dist
	INNER JOIN tbl_Age_Groups_Near_Completers
	  ON    qry_13c_Near_Completers_Program_Dist.AgeGroup = tbl_Age_Groups_Near_Completers.Age_Group_Label_Near_Completer_Projection;"
dbExecute(decimal_con, qry_13d_Append_Near_Completers_Program_Dist_Projected_TTRAIN)

# [INSERT INTO] Cohort_Program_Distributions_Static


# ---- qry_13d_Append_Near_Completers_Program_Dist_Static_TTRAIN ----
qry_13d_Append_Near_Completers_Program_Dist_Static_TTRAIN <-
"INSERT INTO Cohort_Program_Distributions_Static
	        ( Survey, PSSM_Credential, PSSM_CRED, LCP4_CD, GRAD_STATUS, TTRAIN, LCIP4_CRED, LCIP2_CRED, Age_Group, [Year], [Count], Total, [Percent] )
	SELECT 'Program_Projections_2023-2024_qry_13d' AS Survey,
	        qry_13c_Near_Completers_Program_Dist.PSSM_Credential,
	        qry_13c_Near_Completers_Program_Dist.PSSM_CRED,
	        qry_13c_Near_Completers_Program_Dist.LCP4_CD,
	        qry_13c_Near_Completers_Program_Dist.COSC_GRAD_STATUS_LGDS_CD_Group,
	        qry_13c_Near_Completers_Program_Dist.COSC_TTRAIN,
	        qry_13c_Near_Completers_Program_Dist.LCIP4_CRED,
	        qry_13c_Near_Completers_Program_Dist.LCIP2_CRED,
	        tbl_Age_Groups_Near_Completers.Age_Group_Label_Graduate_Projection AS AgeGroup, '2023/2024' AS Projection_Year,
	        qry_13c_Near_Completers_Program_Dist.Count,
	        qry_13c_Near_Completers_Program_Dist.Totals,
	        qry_13c_Near_Completers_Program_Dist.[%]
	FROM    qry_13c_Near_Completers_Program_Dist
	INNER JOIN tbl_Age_Groups_Near_Completers
	  ON    qry_13c_Near_Completers_Program_Dist.AgeGroup = tbl_Age_Groups_Near_Completers.Age_Group_Label_Near_Completer_Projection;"
dbExecute(decimal_con, qry_13d_Append_Near_Completers_Program_Dist_Static_TTRAIN)
dbExecute(decimal_con, "drop table qry_13a_Near_completers")
dbExecute(decimal_con, "drop table qry_13b_Near_Completers_Total")
dbExecute(decimal_con, "drop table qry_13c_Near_Completers_Program_Dist")

# survey == 'Program_Projections_2023-2024_Q012e' (Static) ----
# Add program cohorts to static distribution datasets
# Note: many lcip2 creds are NULL for BACH

# [SQL]

# ---- Q012a_Check_Total_for_Invalid_CIPs ----
Q012a_Check_Total_for_Invalid_CIPs <-
"SELECT tbl_Program_Projection_Input.FINAL_CIP_CODE_4,
        tbl_Program_Projection_Input.Count
	FROM    tbl_Program_Projection_Input INNER JOIN INFOWARE_L_CIP_4DIGITS_CIP2016
	ON      tbl_Program_Projection_Input.FINAL_CIP_CODE_4 = INFOWARE_L_CIP_4DIGITS_CIP2016.LCP4_CD
	WHERE   (((INFOWARE_L_CIP_4DIGITS_CIP2016.LCP4_CD) Is Null))
	GROUP BY tbl_Program_Projection_Input.FINAL_CIP_CODE_4, tbl_Program_Projection_Input.Count;"
dbGetQuery(decimal_con, Q012a_Check_Total_for_Invalid_CIPs)

# [SELECT INTO] Create Q012b_Weight_Cohort_Dist from T_PSSM_Projection_Cred_Grp

# ---- Q012b_Weight_Cohort_Dist ----
Q012b_Weight_Cohort_Dist <-
"SELECT T_PSSM_Projection_Cred_Grp.PSSM_Credential,
        CONCAT(CASE WHEN COSC_GRAD_STATUS_LGDS_CD IS Null THEN NULL ELSE cast(COSC_GRAD_STATUS_LGDS_CD as nvarchar(50)) + ' - ' END, [PSSM_Credential]) AS PSSM_CRED,
        tbl_Program_Projection_Input.FINAL_CIP_CODE_4 AS LCP4_CD, T_PSSM_Projection_Cred_Grp.COSC_GRAD_STATUS_LGDS_CD,
        CONCAT(CASE WHEN COSC_GRAD_STATUS_LGDS_CD IS Null THEN NULL ELSE cast(COSC_GRAD_STATUS_LGDS_CD as nvarchar(50)) + ' - ' END, [FINAL_CIP_CODE_4], ' - ', [T_PSSM_Projection_Cred_Grp].[PSSM_Credential]) AS LCIP4_CRED,
        CONCAT(CASE WHEN COSC_GRAD_STATUS_LGDS_CD IS Null THEN NULL ELSE cast(COSC_GRAD_STATUS_LGDS_CD as nvarchar(50)) + ' - ' END, Left([FINAL_CIP_CODE_4],2), ' - ' , [T_PSSM_Projection_Cred_Grp].[PSSM_Credential]) AS LCIP2_CRED,
        tbl_Program_Projection_Input.AgeGroup,
        Sum(tbl_Program_Projection_Input.Count) AS Counts,
        T_Weights_STP.Weight,
        Sum([Count])*([Weight]) AS Weighted
	INTO Q012b_Weight_Cohort_Dist
	FROM    T_PSSM_Projection_Cred_Grp
	INNER JOIN (tbl_Program_Projection_Input
	  INNER JOIN T_Weights_STP
	    ON tbl_Program_Projection_Input.PSI_AWARD_SCHOOL_YEAR_DELAYED = T_Weights_STP.Year_Code)
	  ON T_PSSM_Projection_Cred_Grp.PSSM_Projection_Credential = tbl_Program_Projection_Input.PSI_CREDENTIAL_CATEGORY
	WHERE   (((T_Weights_STP.Model)='2023-2024') AND ((T_PSSM_Projection_Cred_Grp.PSSM_Credential) Not In ('APPRAPPR','APPRCERT','GRCT or GRDP','PDEG','MAST','DOCT')))
	GROUP BY T_PSSM_Projection_Cred_Grp.PSSM_Credential,
	        CONCAT(CASE WHEN COSC_GRAD_STATUS_LGDS_CD IS Null THEN NULL ELSE cast(COSC_GRAD_STATUS_LGDS_CD as nvarchar(50)) + ' - ' END, [PSSM_Credential]),
	        tbl_Program_Projection_Input.FINAL_CIP_CODE_4,
	        T_PSSM_Projection_Cred_Grp.COSC_GRAD_STATUS_LGDS_CD,
	        CONCAT(CASE WHEN COSC_GRAD_STATUS_LGDS_CD IS Null THEN NULL ELSE cast(COSC_GRAD_STATUS_LGDS_CD as nvarchar(50)) + ' - ' END, [FINAL_CIP_CODE_4], ' - ', [T_PSSM_Projection_Cred_Grp].[PSSM_Credential]),
	        CONCAT(CASE WHEN COSC_GRAD_STATUS_LGDS_CD IS Null THEN NULL ELSE cast(COSC_GRAD_STATUS_LGDS_CD as nvarchar(50)) + ' - ' END, Left([FINAL_CIP_CODE_4],2), ' - ', [T_PSSM_Projection_Cred_Grp].[PSSM_Credential]),
	        tbl_Program_Projection_Input.AgeGroup,
	        T_Weights_STP.Weight
	HAVING (((T_Weights_STP.Weight)>0));"
dbExecute(decimal_con, Q012b_Weight_Cohort_Dist)

# [SELECT INTO] Create Q012c_Weighted_Cohort_Dist from Q012b_Weight_Cohort_Dist

# ---- Q012c_Weighted_Cohort_Dist ----
Q012c_Weighted_Cohort_Dist <-
"SELECT Q012b_Weight_Cohort_Dist.PSSM_Credential,
	      Q012b_Weight_Cohort_Dist.PSSM_CRED,
	      Q012b_Weight_Cohort_Dist.LCP4_CD,
	      Q012b_Weight_Cohort_Dist.COSC_GRAD_STATUS_LGDS_CD,
	      Q012b_Weight_Cohort_Dist.LCIP4_CRED,
	      Q012b_Weight_Cohort_Dist.LCIP2_CRED,
	      Q012b_Weight_Cohort_Dist.AgeGroup,
	      Sum(Q012b_Weight_Cohort_Dist.Weighted) AS [Count]
	INTO Q012c_Weighted_Cohort_Dist
	FROM Q012b_Weight_Cohort_Dist
	GROUP BY Q012b_Weight_Cohort_Dist.PSSM_Credential,
	      Q012b_Weight_Cohort_Dist.PSSM_CRED,
	      Q012b_Weight_Cohort_Dist.LCP4_CD,
	      Q012b_Weight_Cohort_Dist.COSC_GRAD_STATUS_LGDS_CD,
	      Q012b_Weight_Cohort_Dist.LCIP4_CRED,
	      Q012b_Weight_Cohort_Dist.LCIP2_CRED,
	      Q012b_Weight_Cohort_Dist.AgeGroup;"
dbExecute(decimal_con, Q012c_Weighted_Cohort_Dist)

# [SELECT INTO] Create Q012c1_Weighted_Cohort_Dist_TTRAIN from T_Cohorts_Recoded

# ---- Q012c1_Weighted_Cohort_Dist_TTRAIN ----
Q012c1_Weighted_Cohort_Dist_TTRAIN <-
"SELECT T_Cohorts_Recoded.PSSM_Credential,
        T_Cohorts_Recoded.PSSM_Credential AS PSSM_CRED,
        T_Cohorts_Recoded.LCP4_CD,
        T_Cohorts_Recoded.GRAD_STATUS,
        T_Cohorts_Recoded.TTRAIN, T_Cohorts_Recoded.LCIP4_CRED,
        T_Cohorts_Recoded.LCIP2_CRED, tbl_Age_Groups.Age_Group_Label AS Age_Group,
        Count(*) AS Counts,
        T_Cohorts_Recoded.Weight,
        Count(*)*([Weight]) AS Weighted
	INTO Q012c1_Weighted_Cohort_Dist_TTRAIN
	FROM T_Cohorts_Recoded
	INNER JOIN tbl_Age_Groups
	  ON T_Cohorts_Recoded.Age_Group = tbl_Age_Groups.Age_Group
	WHERE (((T_Cohorts_Recoded.GRAD_STATUS)<>'3'))
	GROUP BY T_Cohorts_Recoded.PSSM_Credential, T_Cohorts_Recoded.LCP4_CD,
	        T_Cohorts_Recoded.GRAD_STATUS, T_Cohorts_Recoded.TTRAIN,
	        T_Cohorts_Recoded.LCIP4_CRED, T_Cohorts_Recoded.LCIP2_CRED,
	        tbl_Age_Groups.Age_Group_Label, T_Cohorts_Recoded.Weight,
	        T_Cohorts_Recoded.PSSM_Credential
	HAVING (((T_Cohorts_Recoded.TTRAIN) Is Not Null)
	AND ((T_Cohorts_Recoded.Weight)>0));"
dbExecute(decimal_con, Q012c1_Weighted_Cohort_Dist_TTRAIN)

# [SELECT INTO] Create Q012c2_Weighted_Cohort_Dist from Q012c1_Weighted_Cohort_Dist_TTRAIN

# ---- Q012c2_Weighted_Cohort_Dist ----
Q012c2_Weighted_Cohort_Dist <-
"SELECT Q012c1_Weighted_Cohort_Dist_TTRAIN.PSSM_Credential,
	         Q012c1_Weighted_Cohort_Dist_TTRAIN.PSSM_CRED,
	         Q012c1_Weighted_Cohort_Dist_TTRAIN.LCP4_CD,
	         Q012c1_Weighted_Cohort_Dist_TTRAIN.GRAD_STATUS,
	         Q012c1_Weighted_Cohort_Dist_TTRAIN.TTRAIN,
	         Q012c1_Weighted_Cohort_Dist_TTRAIN.LCIP4_CRED,
	         Q012c1_Weighted_Cohort_Dist_TTRAIN.LCIP2_CRED,
	         Q012c1_Weighted_Cohort_Dist_TTRAIN.Age_Group,
	         Sum(Q012c1_Weighted_Cohort_Dist_TTRAIN.Weighted) AS [Count]
	INTO Q012c2_Weighted_Cohort_Dist
	FROM Q012c1_Weighted_Cohort_Dist_TTRAIN
	GROUP BY Q012c1_Weighted_Cohort_Dist_TTRAIN.PSSM_Credential,
	         Q012c1_Weighted_Cohort_Dist_TTRAIN.PSSM_CRED,
	         Q012c1_Weighted_Cohort_Dist_TTRAIN.LCP4_CD,
	         Q012c1_Weighted_Cohort_Dist_TTRAIN.GRAD_STATUS,
	         Q012c1_Weighted_Cohort_Dist_TTRAIN.TTRAIN,
	         Q012c1_Weighted_Cohort_Dist_TTRAIN.LCIP4_CRED,
	         Q012c1_Weighted_Cohort_Dist_TTRAIN.LCIP2_CRED,
	         Q012c1_Weighted_Cohort_Dist_TTRAIN.Age_Group;"
dbExecute(decimal_con, Q012c2_Weighted_Cohort_Dist)

# [SELECT INTO] Create Q012c3_Weighted_Cohort_Dist_Total from Q012c2_Weighted_Cohort_Dist

# ---- Q012c3_Weighted_Cohort_Dist_Total ----
Q012c3_Weighted_Cohort_Dist_Total <-
"SELECT Q012c2_Weighted_Cohort_Dist.PSSM_Credential,
        Q012c2_Weighted_Cohort_Dist.PSSM_CRED,
        Q012c2_Weighted_Cohort_Dist.LCP4_CD,
        Q012c2_Weighted_Cohort_Dist.GRAD_STATUS,
        Q012c2_Weighted_Cohort_Dist.Age_Group,
        Sum(Q012c2_Weighted_Cohort_Dist.Count) AS Totals
	INTO    Q012c3_Weighted_Cohort_Dist_Total
	FROM    Q012c2_Weighted_Cohort_Dist
	GROUP BY Q012c2_Weighted_Cohort_Dist.PSSM_Credential,
        Q012c2_Weighted_Cohort_Dist.PSSM_CRED,
        Q012c2_Weighted_Cohort_Dist.LCP4_CD,
        Q012c2_Weighted_Cohort_Dist.GRAD_STATUS,
        Q012c2_Weighted_Cohort_Dist.Age_Group;"
dbExecute(decimal_con, Q012c3_Weighted_Cohort_Dist_Total)

# [SELECT INTO] Create Q012c4_Weighted_Cohort_Distribution_Projected from Q012c2_Weighted_Cohort_Dist

# ---- Q012c4_Weighted_Cohort_Distribution_Projected ----
Q012c4_Weighted_Cohort_Distribution_Projected <-
"SELECT 'Program_Projections_2023-2024_Q015e' AS Survey,
        Q012c2_Weighted_Cohort_Dist.PSSM_Credential,
        Q012c2_Weighted_Cohort_Dist.PSSM_CRED,
        Q012c2_Weighted_Cohort_Dist.LCP4_CD,
        Q012c2_Weighted_Cohort_Dist.GRAD_STATUS,
        Q012c2_Weighted_Cohort_Dist.TTRAIN,
        Q012c2_Weighted_Cohort_Dist.Age_Group,
        '2023/2024' AS Projection_Year,
        Q012c2_Weighted_Cohort_Dist.Count,
        Q012c3_Weighted_Cohort_Dist_Total.Totals,
        IIf((Totals=0), 0 , CAST(Count AS float)/CAST(Totals as FLOAT)) AS [%]
	INTO    Q012c4_Weighted_Cohort_Distribution_Projected
	FROM    Q012c2_Weighted_Cohort_Dist
	INNER JOIN Q012c3_Weighted_Cohort_Dist_Total
	  ON    (Q012c2_Weighted_Cohort_Dist.Age_Group = Q012c3_Weighted_Cohort_Dist_Total.Age_Group)
	  AND   (Q012c2_Weighted_Cohort_Dist.PSSM_CRED = Q012c3_Weighted_Cohort_Dist_Total.PSSM_CRED)
	  AND   (Q012c2_Weighted_Cohort_Dist.GRAD_STATUS = Q012c3_Weighted_Cohort_Dist_Total.GRAD_STATUS)
	  AND   (Q012c2_Weighted_Cohort_Dist.LCP4_CD = Q012c3_Weighted_Cohort_Dist_Total.LCP4_CD);"
dbExecute(decimal_con, Q012c4_Weighted_Cohort_Distribution_Projected) # why create this?

# [SELECT INTO] Create Q012c5_Weighted_Cohort_Dist_TTRAIN from Q012c_Weighted_Cohort_Dist

# ---- Q012c5_Weighted_Cohort_Dist_TTRAIN ----
Q012c5_Weighted_Cohort_Dist_TTRAIN <-
"SELECT Q012c_Weighted_Cohort_Dist.PSSM_Credential,
        Q012c_Weighted_Cohort_Dist.PSSM_CRED,
        Q012c_Weighted_Cohort_Dist.LCP4_CD,
        Q012c_Weighted_Cohort_Dist.COSC_GRAD_STATUS_LGDS_CD,
        Q012c4_Weighted_Cohort_Distribution_Projected.TTRAIN,
        CONCAT(
          (CASE WHEN [COSC_GRAD_STATUS_LGDS_CD] IS NULL THEN Null ELSE CAST([COSC_GRAD_STATUS_LGDS_CD] AS NVARCHAR(50)) + ' - ' END),
				    [Q012c_Weighted_Cohort_Dist].[LCP4_CD], ' - ',
				    (CASE WHEN [TTRAIN] IS NULL THEN Null ELSE CAST([TTRAIN] AS NVARCHAR(50)) + ' - ' END),
				    [Q012c_Weighted_Cohort_Dist].[PSSM_Credential]
				   ) AS LCIP4_CRED,
        CONCAT(
          (CASE WHEN [COSC_GRAD_STATUS_LGDS_CD] IS NULL THEN Null ELSE CAST([COSC_GRAD_STATUS_LGDS_CD] AS NVARCHAR(50)) + ' - ' END),
				    Left([Q012c_Weighted_Cohort_Dist].[LCP4_CD],2) , ' - ',
				    (CASE WHEN [TTRAIN] IS NULL THEN Null ELSE CAST([TTRAIN] AS NVARCHAR(50)) + ' - ' END),
				    [Q012c_Weighted_Cohort_Dist].[PSSM_Credential]
				  ) AS LCIP2_CRED,
        Q012c_Weighted_Cohort_Dist.AgeGroup, Q012c_Weighted_Cohort_Dist.Count, Q012c4_Weighted_Cohort_Distribution_Projected.[%],
        CASE WHEN [Q012c4_Weighted_Cohort_Distribution_Projected].[%] IS NULL THEN [Q012c_Weighted_Cohort_Dist].[Count] ELSE [Q012c_Weighted_Cohort_Dist].[Count]*[Q012c4_Weighted_Cohort_Distribution_Projected].[%] END AS Count_Distributed
	INTO    Q012c5_Weighted_Cohort_Dist_TTRAIN
	FROM    Q012c_Weighted_Cohort_Dist
	LEFT JOIN Q012c4_Weighted_Cohort_Distribution_Projected
	  ON    (Q012c_Weighted_Cohort_Dist.AgeGroup = Q012c4_Weighted_Cohort_Distribution_Projected.Age_Group)
	  AND   (Q012c_Weighted_Cohort_Dist.COSC_GRAD_STATUS_LGDS_CD = Q012c4_Weighted_Cohort_Distribution_Projected.GRAD_STATUS)
	  AND   (Q012c_Weighted_Cohort_Dist.LCP4_CD = Q012c4_Weighted_Cohort_Distribution_Projected.LCP4_CD)
	  AND   (Q012c_Weighted_Cohort_Dist.PSSM_Credential = Q012c4_Weighted_Cohort_Distribution_Projected.PSSM_Credential);"
dbExecute(decimal_con, Q012c5_Weighted_Cohort_Dist_TTRAIN)

# [SELECT INTO] Create Q012d_Weighted_Cohort_Dist_Total from Q012b_Weight_Cohort_Dist

# ---- Q012d_Weighted_Cohort_Dist_Total ----
Q012d_Weighted_Cohort_Dist_Total <-
"SELECT Q012b_Weight_Cohort_Dist.PSSM_Credential,
        Q012b_Weight_Cohort_Dist.PSSM_CRED,
        Q012b_Weight_Cohort_Dist.AgeGroup,
        Sum(Q012b_Weight_Cohort_Dist.Weighted) AS Totals
	INTO    Q012d_Weighted_Cohort_Dist_Total
	FROM    Q012b_Weight_Cohort_Dist
	GROUP BY Q012b_Weight_Cohort_Dist.PSSM_Credential,
        Q012b_Weight_Cohort_Dist.PSSM_CRED,
        Q012b_Weight_Cohort_Dist.AgeGroup;"
dbExecute(decimal_con, Q012d_Weighted_Cohort_Dist_Total)

# [DELETE] Cohort_Program_Distributions_Static

# ---- Q012e_Delete_Weighted_Cohort_Distribution ----
Q012e_Delete_Weighted_Cohort_Distribution <-
"DELETE
	FROM Cohort_Program_Distributions_Static
	WHERE (((Cohort_Program_Distributions_Static.Survey) Like '%Q012e'));"
dbExecute(decimal_con, Q012e_Delete_Weighted_Cohort_Distribution)

# [INSERT INTO] Cohort_Program_Distributions_Static

# ---- Q012e_Weighted_Cohort_Distribution ----
Q012e_Weighted_Cohort_Distribution <-
"INSERT INTO Cohort_Program_Distributions_Static
( Survey, PSSM_Credential, PSSM_CRED, LCP4_CD, GRAD_STATUS, TTRAIN, LCIP4_CRED, LCIP2_CRED, Age_Group, [Year], [Count], Total, [Percent] )
	SELECT 'Program_Projections_2023-2024_Q012e' AS Survey,
	Q012c5_Weighted_Cohort_Dist_TTRAIN.PSSM_Credential,
	        Q012c5_Weighted_Cohort_Dist_TTRAIN.PSSM_CRED,
	        Q012c5_Weighted_Cohort_Dist_TTRAIN.LCP4_CD,
	        Q012c5_Weighted_Cohort_Dist_TTRAIN.COSC_GRAD_STATUS_LGDS_CD,
	        Q012c5_Weighted_Cohort_Dist_TTRAIN.TTRAIN,
	        Q012c5_Weighted_Cohort_Dist_TTRAIN.LCIP4_CRED,
	        Q012c5_Weighted_Cohort_Dist_TTRAIN.LCIP2_CRED,
	        Q012c5_Weighted_Cohort_Dist_TTRAIN.AgeGroup,
	        '2023/2024' AS Projection_Year,
	        Q012c5_Weighted_Cohort_Dist_TTRAIN.Count_Distributed,
	        Q012d_Weighted_Cohort_Dist_Total.Totals,
	        CASE WHEN Totals = 0 THEN 0 ELSE CAST(Count_Distributed AS FLOAT)/CAST(Totals AS FLOAT) END AS [Percent]
	FROM    Q012c5_Weighted_Cohort_Dist_TTRAIN
	INNER JOIN Q012d_Weighted_Cohort_Dist_Total
	  ON    (Q012c5_Weighted_Cohort_Dist_TTRAIN.AgeGroup = Q012d_Weighted_Cohort_Dist_Total.AgeGroup)
	  AND   (Q012c5_Weighted_Cohort_Dist_TTRAIN.PSSM_CRED = Q012d_Weighted_Cohort_Dist_Total.PSSM_CRED);"
dbExecute(decimal_con, Q012e_Weighted_Cohort_Distribution)
dbExecute(decimal_con, "drop table Q012b_Weight_Cohort_Dist") 
dbExecute(decimal_con, "drop table Q012c_Weighted_Cohort_Dist") 
dbExecute(decimal_con, "drop table Q012c1_Weighted_Cohort_Dist_TTRAIN") 
dbExecute(decimal_con, "drop table Q012c2_Weighted_Cohort_Dist") 
dbExecute(decimal_con, "drop table Q012c3_Weighted_Cohort_Dist_Total") 
dbExecute(decimal_con, "drop table Q012c4_Weighted_Cohort_Distribution_Projected") 
dbExecute(decimal_con, "drop table Q012c5_Weighted_Cohort_Dist_TTRAIN") 
dbExecute(decimal_con, "drop table Q012d_Weighted_Cohort_Dist_Total") 

# survey == 'Program_Projections_2023-2024_Q013e' (Static) ----
# Add masters and doctorates to static distribution datasets
# Note: lcip4_cd showing as 2D for masters and doct - cluster.  
# (same in prior model runs)

# [SELECT INTO] Create qry_12_LCP4_LCIPPC_Recode_9999 from INFOWARE_L_CIP_6DIGITS_CIP2016

# ---- qry_12_LCP4_LCIPPC_Recode_9999 ----
qry_12_LCP4_LCIPPC_Recode_9999 <-
"SELECT INFOWARE_L_CIP_6DIGITS_CIP2016.LCIP_LCP4_CD,
        IIf([LCIP_LCP4_CD]='9999','99',[INFOWARE_L_CIP_6DIGITS_CIP2016].[LCIP_LCIPPC_CD]) AS LCIP_LCIPPC_CD
	INTO    qry_12_LCP4_LCIPPC_Recode_9999
	FROM    INFOWARE_L_CIP_6DIGITS_CIP2016
	GROUP BY INFOWARE_L_CIP_6DIGITS_CIP2016.LCIP_LCP4_CD,
	        IIf([LCIP_LCP4_CD]='9999','99',[INFOWARE_L_CIP_6DIGITS_CIP2016].[LCIP_LCIPPC_CD]);"
dbExecute(decimal_con, qry_12_LCP4_LCIPPC_Recode_9999)

# [SQL]

# ---- Q013a_Check_PDEG_CLP_07_Only_CIP_22 ----
Q013a_Check_PDEG_CLP_07_Only_CIP_22 <-
"SELECT T_PSSM_Projection_Cred_Grp.PSSM_Credential,
        CONCAT
        (CASE WHEN [COSC_GRAD_STATUS_LGDS_CD] IS NULL THEN Null ELSE CAST([COSC_GRAD_STATUS_LGDS_CD] AS NVARCHAR(50)) + ' - ' END, [PSSM_Credential]) AS PSSM_CRED,
        tbl_Program_Projection_Input.FINAL_CIP_CODE_4, qry_12_LCP4_LCIPPC_Recode_9999.LCIP_LCIPPC_CD AS LCIPPC_CD,
        CONCAT(CASE WHEN [COSC_GRAD_STATUS_LGDS_CD] IS NULL THEN Null ELSE CAST([COSC_GRAD_STATUS_LGDS_CD] AS NVARCHAR(50)) + ' - ' END, [LCIP_LCIPPC_CD], ' - ',
           [T_PSSM_Projection_Cred_Grp].[PSSM_Credential]) AS LCIPPC_CRED,
        tbl_Program_Projection_Input.AgeGroup,
        Sum(tbl_Program_Projection_Input.Count) AS Counts,
        T_Weights_STP.Weight,
        Sum([Count])*([Weight]) AS Weighted
	FROM (T_PSSM_Projection_Cred_Grp
	INNER JOIN (tbl_Program_Projection_Input
	    INNER JOIN T_Weights_STP
	      ON tbl_Program_Projection_Input.PSI_AWARD_SCHOOL_YEAR_DELAYED = T_Weights_STP.Year_Code)
	  ON T_PSSM_Projection_Cred_Grp.PSSM_Projection_Credential = tbl_Program_Projection_Input.PSI_CREDENTIAL_CATEGORY)
	INNER JOIN qry_12_LCP4_LCIPPC_Recode_9999
	  ON tbl_Program_Projection_Input.FINAL_CIP_CODE_4 = qry_12_LCP4_LCIPPC_Recode_9999.LCIP_LCP4_CD
	WHERE (((T_Weights_STP.Model)='2023-2024')
	AND ((T_PSSM_Projection_Cred_Grp.PSSM_Credential) In ('PDEG')))
	GROUP BY T_PSSM_Projection_Cred_Grp.PSSM_Credential,
			    CONCAT(CASE WHEN [COSC_GRAD_STATUS_LGDS_CD] IS NULL THEN Null ELSE CAST([COSC_GRAD_STATUS_LGDS_CD] AS NVARCHAR(50)) + ' - ' END, [PSSM_Credential]),
			    tbl_Program_Projection_Input.FINAL_CIP_CODE_4,
			    qry_12_LCP4_LCIPPC_Recode_9999.LCIP_LCIPPC_CD,
			    CONCAT(CASE WHEN [COSC_GRAD_STATUS_LGDS_CD] IS NULL THEN Null ELSE CAST([COSC_GRAD_STATUS_LGDS_CD] AS NVARCHAR(50)) + ' - ' END, [LCIP_LCIPPC_CD],
			        ' - ', [T_PSSM_Projection_Cred_Grp].[PSSM_Credential]),
			tbl_Program_Projection_Input.AgeGroup, T_Weights_STP.Weight
	HAVING (((T_Weights_STP.Weight)>0));"
dbGetQuery(decimal_con, Q013a_Check_PDEG_CLP_07_Only_CIP_22)

# [SELECT INTO] Create Q013b_Weight_Cohort_Dist_MAST_DOCT_Others

# ---- Q013b_Weight_Cohort_Dist_MAST_DOCT_Others ----
Q013b_Weight_Cohort_Dist_MAST_DOCT_Others <-
"SELECT T_PSSM_Projection_Cred_Grp.PSSM_Credential,
			    CONCAT(CASE WHEN [COSC_GRAD_STATUS_LGDS_CD] IS NULL THEN Null ELSE CAST([COSC_GRAD_STATUS_LGDS_CD] AS NVARCHAR(50)) + ' - ' END, [PSSM_Credential]) AS PSSM_CRED,
        qry_12_LCP4_LCIPPC_Recode_9999.LCIP_LCIPPC_CD AS LCIPPC_CD,
        CONCAT(CASE WHEN [COSC_GRAD_STATUS_LGDS_CD] IS NULL THEN Null ELSE CAST([COSC_GRAD_STATUS_LGDS_CD] AS NVARCHAR(50)) + ' - ' END, [LCIP_LCIPPC_CD], ' - ',
             [T_PSSM_Projection_Cred_Grp].[PSSM_Credential]) AS LCIPPC_CRED,
        tbl_Program_Projection_Input.AgeGroup,
        Sum(tbl_Program_Projection_Input.Count) AS Counts,
        T_Weights_STP.Weight,
        Sum([Count])*([Weight]) AS Weighted
	INTO Q013b_Weight_Cohort_Dist_MAST_DOCT_Others
	FROM    (T_PSSM_Projection_Cred_Grp
	INNER JOIN (tbl_Program_Projection_Input
	    INNER JOIN T_Weights_STP
	      ON tbl_Program_Projection_Input.PSI_AWARD_SCHOOL_YEAR_DELAYED = T_Weights_STP.Year_Code)
	  ON T_PSSM_Projection_Cred_Grp.PSSM_Projection_Credential = tbl_Program_Projection_Input.PSI_CREDENTIAL_CATEGORY)
	INNER JOIN qry_12_LCP4_LCIPPC_Recode_9999
	  ON tbl_Program_Projection_Input.FINAL_CIP_CODE_4 = qry_12_LCP4_LCIPPC_Recode_9999.LCIP_LCP4_CD
	WHERE (((T_Weights_STP.Model)='2023-2024')
	AND   ((T_PSSM_Projection_Cred_Grp.PSSM_Credential) In ('GRCT or GRDP','PDEG','MAST','DOCT')))
	GROUP BY T_PSSM_Projection_Cred_Grp.PSSM_Credential,
		    CONCAT(CASE WHEN [COSC_GRAD_STATUS_LGDS_CD] IS NULL THEN Null ELSE CAST([COSC_GRAD_STATUS_LGDS_CD] AS NVARCHAR(50)) + ' - ' END, [PSSM_Credential]),
	      qry_12_LCP4_LCIPPC_Recode_9999.LCIP_LCIPPC_CD,
	      CONCAT(CASE WHEN [COSC_GRAD_STATUS_LGDS_CD] IS NULL THEN Null ELSE CAST([COSC_GRAD_STATUS_LGDS_CD] AS NVARCHAR(50)) + ' - ' END, [LCIP_LCIPPC_CD], ' - ',
	      [T_PSSM_Projection_Cred_Grp].[PSSM_Credential]),
	      tbl_Program_Projection_Input.AgeGroup, T_Weights_STP.Weight
	HAVING (((T_Weights_STP.Weight)>0));"
dbExecute(decimal_con, Q013b_Weight_Cohort_Dist_MAST_DOCT_Others)

# [SELECT INTO] Create Q013c_Weighted_Cohort_Dist from Q013b_Weight_Cohort_Dist_MAST_DOCT_Others

# ---- Q013c_Weighted_Cohort_Dist ----
Q013c_Weighted_Cohort_Dist <-
"SELECT Q013b_Weight_Cohort_Dist_MAST_DOCT_Others.PSSM_Credential,
        Q013b_Weight_Cohort_Dist_MAST_DOCT_Others.PSSM_CRED,
        Q013b_Weight_Cohort_Dist_MAST_DOCT_Others.LCIPPC_CD,
        Q013b_Weight_Cohort_Dist_MAST_DOCT_Others.LCIPPC_CRED,
        Q013b_Weight_Cohort_Dist_MAST_DOCT_Others.AgeGroup,
        Sum(Q013b_Weight_Cohort_Dist_MAST_DOCT_Others.Weighted) AS [Count]
	INTO    Q013c_Weighted_Cohort_Dist
	FROM    Q013b_Weight_Cohort_Dist_MAST_DOCT_Others
	GROUP BY Q013b_Weight_Cohort_Dist_MAST_DOCT_Others.PSSM_Credential,
        Q013b_Weight_Cohort_Dist_MAST_DOCT_Others.PSSM_CRED,
        Q013b_Weight_Cohort_Dist_MAST_DOCT_Others.LCIPPC_CD,
        Q013b_Weight_Cohort_Dist_MAST_DOCT_Others.LCIPPC_CRED,
        Q013b_Weight_Cohort_Dist_MAST_DOCT_Others.AgeGroup;"
dbExecute(decimal_con, Q013c_Weighted_Cohort_Dist)

# [SELECT INTO] Create Q013d_Weighted_Cohort_Dist_Total from Q013b_Weight_Cohort_Dist_MAST_DOCT_Others

# ---- Q013d_Weighted_Cohort_Dist_Total ----
Q013d_Weighted_Cohort_Dist_Total <-
"SELECT Q013b_Weight_Cohort_Dist_MAST_DOCT_Others.PSSM_Credential,
        Q013b_Weight_Cohort_Dist_MAST_DOCT_Others.PSSM_CRED,
        Q013b_Weight_Cohort_Dist_MAST_DOCT_Others.AgeGroup,
        Sum(Q013b_Weight_Cohort_Dist_MAST_DOCT_Others.Weighted) AS Totals
	INTO    Q013d_Weighted_Cohort_Dist_Total
	FROM    Q013b_Weight_Cohort_Dist_MAST_DOCT_Others
	GROUP BY Q013b_Weight_Cohort_Dist_MAST_DOCT_Others.PSSM_Credential,
        Q013b_Weight_Cohort_Dist_MAST_DOCT_Others.PSSM_CRED,
        Q013b_Weight_Cohort_Dist_MAST_DOCT_Others.AgeGroup;"
dbExecute(decimal_con, Q013d_Weighted_Cohort_Dist_Total)
dbExecute(decimal_con, "DELETE FROM Cohort_Program_Distributions_Static 
          WHERE Survey LIKE 'Program_Projections_2023-2024_Q013e'") # Added

# [INSERT INTO] Cohort_Program_Distributions_Static

# ---- Q013e_Weighted_Cohort_Distribution ----
Q013e_Weighted_Cohort_Distribution <- "
INSERT INTO Cohort_Program_Distributions_Static (Survey, PSSM_Credential, PSSM_CRED, LCP4_CD, LCIP4_CRED, Age_Group, [Year], [Count], [Total], [Percent] )
	SELECT 'Program_Projections_2023-2024_Q013e' AS Survey,
	        Q013c_Weighted_Cohort_Dist.PSSM_Credential,
	        Q013c_Weighted_Cohort_Dist.PSSM_CRED,
	        Q013c_Weighted_Cohort_Dist.LCIPPC_CD,
	        Q013c_Weighted_Cohort_Dist.LCIPPC_CRED,
	        Q013c_Weighted_Cohort_Dist.AgeGroup,
	        '2023/2024' AS Year,
	        Q013c_Weighted_Cohort_Dist.Count,
	        Q013d_Weighted_Cohort_Dist_Total.Totals,
	        CASE WHEN Totals = 0 THEN 0 ELSE CAST([Count] AS FLOAT)/CAST([Totals] AS FLOAT) END AS [Percent]
	FROM    Q013c_Weighted_Cohort_Dist
	INNER JOIN Q013d_Weighted_Cohort_Dist_Total
	  ON    (Q013c_Weighted_Cohort_Dist.AgeGroup = Q013d_Weighted_Cohort_Dist_Total.AgeGroup)
	  AND   (Q013c_Weighted_Cohort_Dist.PSSM_CRED = Q013d_Weighted_Cohort_Dist_Total.PSSM_CRED);"
dbExecute(decimal_con, Q013e_Weighted_Cohort_Distribution)
dbExecute(decimal_con, "drop table Q013b_Weight_Cohort_Dist_MAST_DOCT_Others")
dbExecute(decimal_con, "drop table Q013c_Weighted_Cohort_Dist")
dbExecute(decimal_con, "drop table Q013d_Weighted_Cohort_Dist_Total")

# survey == 'Program_Projections_2023-2024_Q014e' (Static and Projected) ----
# adds apprenticeships to static and projected datasets

# [SELECT INTO] Create Q014b_Weighted_Cohort_Dist_APPR from T_Cohorts_Recoded

# ---- Q014b_Weighted_Cohort_Dist_APPR ----
Q014b_Weighted_Cohort_Dist_APPR <-
"SELECT T_Cohorts_Recoded.PSSM_Credential,
        T_Cohorts_Recoded.PSSM_Credential AS PSSM_CRED,
        T_Cohorts_Recoded.LCP4_CD,
        T_Cohorts_Recoded.TTRAIN,
        T_Cohorts_Recoded.LCIP4_CRED,
        T_Cohorts_Recoded.LCIP2_CRED,
        tbl_Age_Groups.Age_Group_Label AS Age_Group,
        Count(*) AS Counts,
        T_Cohorts_Recoded.Weight,
        Count(*)*([Weight]) AS Weighted
	INTO    Q014b_Weighted_Cohort_Dist_APPR
	FROM    T_Cohorts_Recoded INNER JOIN tbl_Age_Groups
	ON      T_Cohorts_Recoded.Age_Group = tbl_Age_Groups.Age_Group
	WHERE   (((T_Cohorts_Recoded.PSSM_Credential) In ('APPRAPPR','APPRCERT')))
	GROUP BY T_Cohorts_Recoded.PSSM_Credential,
        T_Cohorts_Recoded.LCP4_CD,
        T_Cohorts_Recoded.TTRAIN,
        T_Cohorts_Recoded.LCIP4_CRED,
        T_Cohorts_Recoded.LCIP2_CRED, tbl_Age_Groups.Age_Group_Label,
        T_Cohorts_Recoded.Weight,
        T_Cohorts_Recoded.PSSM_Credential
	HAVING (((T_Cohorts_Recoded.Weight)>0));"
dbExecute(decimal_con, Q014b_Weighted_Cohort_Dist_APPR)

# [SELECT INTO] Create Q014c_Weighted_Cohort_Dist from Q014b_Weighted_Cohort_Dist_APPR

# ---- Q014c_Weighted_Cohort_Dist ----
Q014c_Weighted_Cohort_Dist <-
"SELECT Q014b_Weighted_Cohort_Dist_APPR.PSSM_Credential,
        Q014b_Weighted_Cohort_Dist_APPR.PSSM_CRED,
        Q014b_Weighted_Cohort_Dist_APPR.LCP4_CD,
        Q014b_Weighted_Cohort_Dist_APPR.LCIP4_CRED,
        Q014b_Weighted_Cohort_Dist_APPR.LCIP2_CRED,
        Q014b_Weighted_Cohort_Dist_APPR.Age_Group,
        Sum(Q014b_Weighted_Cohort_Dist_APPR.Weighted) AS [Count]
	INTO    Q014c_Weighted_Cohort_Dist
	FROM    Q014b_Weighted_Cohort_Dist_APPR
	GROUP BY Q014b_Weighted_Cohort_Dist_APPR.PSSM_Credential,
        Q014b_Weighted_Cohort_Dist_APPR.PSSM_CRED,
        Q014b_Weighted_Cohort_Dist_APPR.LCP4_CD,
        Q014b_Weighted_Cohort_Dist_APPR.LCIP4_CRED,
        Q014b_Weighted_Cohort_Dist_APPR.LCIP2_CRED,
        Q014b_Weighted_Cohort_Dist_APPR.Age_Group;"
dbExecute(decimal_con, Q014c_Weighted_Cohort_Dist)

# [SELECT INTO] Create Q014d_Weighted_Cohort_Dist_Total from Q014b_Weighted_Cohort_Dist_APPR

# ---- Q014d_Weighted_Cohort_Dist_Total ----
Q014d_Weighted_Cohort_Dist_Total <-
"SELECT Q014b_Weighted_Cohort_Dist_APPR.PSSM_Credential,
        Q014b_Weighted_Cohort_Dist_APPR.PSSM_CRED,
        Q014b_Weighted_Cohort_Dist_APPR.Age_Group,
        Sum(Q014b_Weighted_Cohort_Dist_APPR.Weighted) AS Totals
	INTO    Q014d_Weighted_Cohort_Dist_Total
	FROM    Q014b_Weighted_Cohort_Dist_APPR
	GROUP BY Q014b_Weighted_Cohort_Dist_APPR.PSSM_Credential,
        Q014b_Weighted_Cohort_Dist_APPR.PSSM_CRED,
        Q014b_Weighted_Cohort_Dist_APPR.Age_Group;"
dbExecute(decimal_con, Q014d_Weighted_Cohort_Dist_Total)
dbExecute(decimal_con, "DELETE FROM Cohort_Program_Distributions_Projected 
          WHERE Survey LIKE 'Program_Projections_2023-2024_Q014e'") # Added
dbExecute(decimal_con, "DELETE FROM Cohort_Program_Distributions_Static 
          WHERE Survey LIKE 'Program_Projections_2023-2024_Q014e'") # Added

# [INSERT INTO] Cohort_Program_Distributions_Projected

# Q014e_Weighted_Cohort_Distribution_Projected ----
Q014e_Weighted_Cohort_Distribution_Projected <-
"INSERT INTO Cohort_Program_Distributions_Projected
( Survey, PSSM_Credential, PSSM_CRED, LCP4_CD, LCIP4_CRED, LCIP2_CRED, Age_Group, [Year], [Count], Total, [Percent] )
	SELECT 'Program_Projections_2023-2024_Q014e' AS Survey,
	        Q014c_Weighted_Cohort_Dist.PSSM_Credential,
	        Q014c_Weighted_Cohort_Dist.PSSM_CRED,
	        Q014c_Weighted_Cohort_Dist.LCP4_CD,
	        Q014c_Weighted_Cohort_Dist.LCIP4_CRED,
	        Q014c_Weighted_Cohort_Dist.LCIP2_CRED,
	        Q014c_Weighted_Cohort_Dist.Age_Group,
	        '2023/2024' AS Projection_Year,
	        Q014c_Weighted_Cohort_Dist.Count, Q014d_Weighted_Cohort_Dist_Total.Totals,
	        CASE WHEN Totals = 0 THEN 0 ELSE Count/Totals END AS [Percent]
	FROM    Q014c_Weighted_Cohort_Dist
	INNER JOIN Q014d_Weighted_Cohort_Dist_Total
	  ON   (Q014c_Weighted_Cohort_Dist.Age_Group = Q014d_Weighted_Cohort_Dist_Total.Age_Group)
	  AND  (Q014c_Weighted_Cohort_Dist.PSSM_CRED = Q014d_Weighted_Cohort_Dist_Total.PSSM_CRED);"
dbExecute(decimal_con, Q014e_Weighted_Cohort_Distribution_Projected)

# [INSERT INTO] Cohort_Program_Distributions_Static

# ---- Q014e_Weighted_Cohort_Distribution_Static ----
Q014e_Weighted_Cohort_Distribution_Static <-
"INSERT INTO Cohort_Program_Distributions_Static
( Survey, PSSM_Credential, PSSM_CRED, LCP4_CD, LCIP4_CRED, LCIP2_CRED, Age_Group, [Year], [Count], Total, [Percent] )
	SELECT 'Program_Projections_2023-2024_Q014e' AS Survey,
	        Q014c_Weighted_Cohort_Dist.PSSM_Credential,
	        Q014c_Weighted_Cohort_Dist.PSSM_CRED,
	        Q014c_Weighted_Cohort_Dist.LCP4_CD,
	        Q014c_Weighted_Cohort_Dist.LCIP4_CRED,
	        Q014c_Weighted_Cohort_Dist.LCIP2_CRED,
	        Q014c_Weighted_Cohort_Dist.Age_Group, '2023/2024' AS Projection_Year,
	        Q014c_Weighted_Cohort_Dist.Count, Q014d_Weighted_Cohort_Dist_Total.Totals,
	        CASE WHEN Totals = 0 THEN 0 ELSE Count/Totals END AS [Percent]
	FROM    Q014c_Weighted_Cohort_Dist
	INNER JOIN Q014d_Weighted_Cohort_Dist_Total
	  ON    (Q014c_Weighted_Cohort_Dist.Age_Group = Q014d_Weighted_Cohort_Dist_Total.Age_Group)
	  AND   (Q014c_Weighted_Cohort_Dist.PSSM_CRED = Q014d_Weighted_Cohort_Dist_Total.PSSM_CRED);"
dbExecute(decimal_con, Q014e_Weighted_Cohort_Distribution_Static )
dbExecute(decimal_con, "drop table Q014b_Weighted_Cohort_Dist_APPR")
dbExecute(decimal_con, "drop table Q014c_Weighted_Cohort_Dist")
dbExecute(decimal_con, "drop table Q014d_Weighted_Cohort_Dist_Total")

# expands static appr in graduate projections - holding counts constant

# [INSERT INTO] Graduate_Projections

# ---- Q014f_APPSO_Grads_Y2_to_Y10 ----
Q014f_APPSO_Grads_Y2_to_Y10 <-
"INSERT INTO Graduate_Projections ( Survey, PSSM_Credential, PSSM_CRED, Age_Group, [Year], Graduates )
	SELECT  Graduate_Projections.Survey,
	        Graduate_Projections.PSSM_Credential,
	        Graduate_Projections.PSSM_CRED,
	        Graduate_Projections.Age_Group,
	        T_APPR_Y2_to_Y10.Y2_to_Y10,
	        Graduate_Projections.Graduates
	FROM    Graduate_Projections INNER JOIN T_APPR_Y2_to_Y10
	ON      Graduate_Projections.Year = T_APPR_Y2_to_Y10.Y1
	WHERE   (((Graduate_Projections.Survey)='APPSO'));"
dbExecute(decimal_con, Q014f_APPSO_Grads_Y2_to_Y10)

# survey == 'Program_Projections_2023-2024_Q015e21' (Static and Projected) ----
# expands apprenticeships and near-completers to include 2020+12YR where
#  survey == Program_Projections_2023-2024_qry_13d
#  survey == Program_Projections_2023-2024_Q014e
dbExecute(decimal_con, "DELETE FROM Cohort_Program_Distributions_Projected 
          WHERE Survey LIKE 'Program_Projections_2023-2024_Q015e21'") # Run if you've been messing with iterations
dbExecute(decimal_con, "DELETE FROM Cohort_Program_Distributions_Static 
          WHERE Survey LIKE 'Program_Projections_2023-2024_Q015e22'") # Run if you've been messing with iterations

# [INSERT INTO] Cohort_Program_Distributions_Projected

# ---- Q015e21_Append_Selected_Static_Distribution_Y2_to_Y12_Projected ----
Q015e21_Append_Selected_Static_Distribution_Y2_to_Y12_Projected <-
"INSERT INTO Cohort_Program_Distributions_Projected
( Survey, PSSM_Credential, PSSM_CRED, LCP4_CD, GRAD_STATUS, TTRAIN, LCIP4_CRED, LCIP2_CRED, Age_Group, [Year], [Count], [Total], [Percent] )
	SELECT 'Program_Projections_2023-2024_Q015e21' AS Survey,
	        Cohort_Program_Distributions_Static.PSSM_Credential,
	        Cohort_Program_Distributions_Static.PSSM_CRED,
	        Cohort_Program_Distributions_Static.LCP4_CD,
	        Cohort_Program_Distributions_Static.GRAD_STATUS,
	        Cohort_Program_Distributions_Static.TTRAIN,
	        Cohort_Program_Distributions_Static.LCIP4_CRED,
	        Cohort_Program_Distributions_Static.LCIP2_CRED,
	        Cohort_Program_Distributions_Static.Age_Group,
	        T_Cohort_Program_Distributions_Y2_to_Y12.Y2_to_Y10 as Year,
	        Cohort_Program_Distributions_Static.Count,
	        Cohort_Program_Distributions_Static.Total,
	        Cohort_Program_Distributions_Static.[Percent]
	FROM    Cohort_Program_Distributions_Static
	INNER JOIN T_Cohort_Program_Distributions_Y2_to_Y12
	ON      Cohort_Program_Distributions_Static.Year = T_Cohort_Program_Distributions_Y2_to_Y12.Y1
	WHERE   (((Cohort_Program_Distributions_Static.PSSM_CRED) In ('APPRAPPR','APPRCERT')
	    Or (Cohort_Program_Distributions_Static.PSSM_CRED) Like '3 - %'));"
dbExecute(decimal_con, Q015e21_Append_Selected_Static_Distribution_Y2_to_Y12_Projected)

# [INSERT INTO] Cohort_Program_Distributions_Static

# ---- Q015e22_Append_Distribution_Y2_to_Y12_Static ----
Q015e22_Append_Distribution_Y2_to_Y12_Static <-
"INSERT INTO Cohort_Program_Distributions_Static
( Survey, PSSM_Credential, PSSM_CRED, LCP4_CD, GRAD_STATUS, TTRAIN, LCIP4_CRED, LCIP2_CRED, Age_Group, [Year], [Count], Total, [Percent] )
	        SELECT 'Program_Projections_2023-2024_Q015e22' AS Survey,
	        Cohort_Program_Distributions_Static.PSSM_Credential,
	        Cohort_Program_Distributions_Static.PSSM_CRED,
	        Cohort_Program_Distributions_Static.LCP4_CD,
	        Cohort_Program_Distributions_Static.GRAD_STATUS,
	        Cohort_Program_Distributions_Static.TTRAIN,
	        Cohort_Program_Distributions_Static.LCIP4_CRED,
	        Cohort_Program_Distributions_Static.LCIP2_CRED,
	        Cohort_Program_Distributions_Static.Age_Group,
	        T_Cohort_Program_Distributions_Y2_to_Y12.Y2_to_Y10,
	        Cohort_Program_Distributions_Static.Count,
	        Cohort_Program_Distributions_Static.Total,
	        Cohort_Program_Distributions_Static.[Percent]
	FROM Cohort_Program_Distributions_Static
	INNER JOIN T_Cohort_Program_Distributions_Y2_to_Y12
	ON Cohort_Program_Distributions_Static.Year = T_Cohort_Program_Distributions_Y2_to_Y12.Y1;"
dbExecute(decimal_con, Q015e22_Append_Distribution_Y2_to_Y12_Static)

# Werner program ----
# Program takes input_data and returns output_data (write to/read from LAN below)
input_data <- dbGetQuery(decimal_con, "SELECT * FROM tbl_Program_Projection_Input") %>% 
  select(-Expr1) %>%
  complete(AgeGroup, PSI_CREDENTIAL_CATEGORY, FINAL_CIP_CODE_4, PSI_AWARD_SCHOOL_YEAR_DELAYED, fill = list(Count = 0)) %>% 
  pivot_wider(names_from = "PSI_AWARD_SCHOOL_YEAR_DELAYED", values_from = "Count") %>%
  rename("CIP" = "FINAL_CIP_CODE_4", 
         "AGE" = "AgeGroup", 
         "CRED" = "PSI_CREDENTIAL_CATEGORY") %>%
  select(CIP, CRED, AGE, 4:ncol(.)) %>%
  arrange(CIP, CRED, AGE)

write_csv(input_data, glue::glue("{lan}/development/csv/gh-source/tmp/06/input-data.csv"))

## run Werner program ----
source(glue::glue("{lan}/development/R/program projections.R")) 

output_data <- read_delim(glue::glue("{lan}/development/csv/gh-source/tmp/06/output.csv"), delim = "\t", col_names = TRUE)
names(output_data)<- paste0(2023:(2023+11), "/", 2024:(2024+11))

T_Predict_CIP_CRED_AGE <- cbind(input_data, output_data)

# pivot T_Predict_CIP_CRED_AGE from wide to long
T_Predict_CIP_CRED_AGE_Flipped <- T_Predict_CIP_CRED_AGE %>% 
  pivot_longer(-c(CIP, CRED, AGE), names_to = "Year", values_to = "Count") %>%
  filter(Year %in% c('2023/2024','2024/2025','2025/2026', '2026/2027','2027/2028',
                     '2028/2029','2029/2030','2030/2031','2031/2032', '2032/2033', 
                     '2033/2034', '2034/2035'))

dbWriteTable(decimal_con, "T_Predict_CIP_CRED_AGE_Flipped", T_Predict_CIP_CRED_AGE_Flipped)

# [SQL]

# ---- qry_05_Flip_T_Predict_CIP_CRED_AGE_2_Check ----
qry_05_Flip_T_Predict_CIP_CRED_AGE_2_Check <-
"SELECT T_Predict_CIP_CRED_AGE_Flipped.Year,
	Sum(T_Predict_CIP_CRED_AGE_Flipped.Count) AS SumOfCount
	FROM T_Predict_CIP_CRED_AGE_Flipped
	GROUP BY T_Predict_CIP_CRED_AGE_Flipped.Year;"
dbGetQuery(decimal_con, qry_05_Flip_T_Predict_CIP_CRED_AGE_2_Check)


# [DELETE] Cohort_Program_Distributions_Projected

# ---- qry_09_Delete_Selected_Static_Cohort_Dist_from_Projected ----
qry_09_Delete_Selected_Static_Cohort_Dist_from_Projected <-"
	DELETE
	FROM    Cohort_Program_Distributions_Projected
	WHERE   (((Cohort_Program_Distributions_Projected.PSSM_CRED) Not In ('APPRAPPR','APPRCERT')
	  AND   (Cohort_Program_Distributions_Projected.PSSM_CRED) Not Like '3 -%'
	  AND   (Cohort_Program_Distributions_Projected.PSSM_CRED) Not Like 'P -%'));"
dbExecute(decimal_con, qry_09_Delete_Selected_Static_Cohort_Dist_from_Projected)

# survey == 'Program_Projections_2023-2024_qry10c' (Projected) ----
# adds projected counts to Cohort_Program_Distributions_Projected where PSSM_Credential NOT IN ('GRCT or GRDP','PDEG','MAST','DOCT') 
# (ALSO NOT IN ('APPRAPPR','APPRCERT') as these were done earlier)

# [SELECT INTO] Create qry_10a_Program_Dist_Count from T_Predict_CIP_CRED_AGE_Flipped

# ---- qry_10a_Program_Dist_Count ----
qry_10a_Program_Dist_Count <-
"SELECT T_PSSM_Projection_Cred_Grp.PSSM_Credential,
        CONCAT(CASE WHEN [COSC_GRAD_STATUS_LGDS_CD] IS NULL THEN Null ELSE CAST([COSC_GRAD_STATUS_LGDS_CD] AS NVARCHAR(50)) + ' - ' END, [T_PSSM_Projection_Cred_Grp].[PSSM_Credential]) AS PSSM_CRED,
        T_Predict_CIP_CRED_AGE_Flipped.CIP,
        CONCAT(CASE WHEN [COSC_GRAD_STATUS_LGDS_CD] IS NULL THEN Null ELSE CAST([COSC_GRAD_STATUS_LGDS_CD] AS NVARCHAR(50)) + ' - ' END, [CIP], ' - '
				, [T_PSSM_Projection_Cred_Grp].[PSSM_Credential]) AS LCIP4_CRED,
        CONCAT(CASE WHEN [COSC_GRAD_STATUS_LGDS_CD] IS NULL THEN Null ELSE CAST([COSC_GRAD_STATUS_LGDS_CD] AS NVARCHAR(50)) + ' - ' END, Left([CIP],2), ' - '
				, [T_PSSM_Projection_Cred_Grp].[PSSM_Credential]) AS LCIP2_CRED,
        T_Predict_CIP_CRED_AGE_Flipped.AGE,
        T_Predict_CIP_CRED_AGE_Flipped.Year,
	       Sum(T_Predict_CIP_CRED_AGE_Flipped.Count) AS [Count]
	INTO qry_10a_Program_Dist_Count
	FROM    T_Predict_CIP_CRED_AGE_Flipped
	INNER JOIN T_PSSM_Projection_Cred_Grp
	  ON T_Predict_CIP_CRED_AGE_Flipped.CRED = T_PSSM_Projection_Cred_Grp.PSSM_Projection_Credential
	WHERE   (((T_PSSM_Projection_Cred_Grp.PSSM_Credential) Not In ('APPRAPPR','APPRCERT','GRCT or GRDP','PDEG','MAST','DOCT')))
	GROUP BY T_PSSM_Projection_Cred_Grp.PSSM_Credential,
	         CONCAT(CASE WHEN [COSC_GRAD_STATUS_LGDS_CD] IS NULL THEN Null ELSE CAST([COSC_GRAD_STATUS_LGDS_CD] AS NVARCHAR(50)) + ' - ' END, [T_PSSM_Projection_Cred_Grp].[PSSM_Credential]),
			    T_Predict_CIP_CRED_AGE_Flipped.CIP,
	         CONCAT(CASE WHEN [COSC_GRAD_STATUS_LGDS_CD] IS NULL THEN Null ELSE CAST([COSC_GRAD_STATUS_LGDS_CD] AS NVARCHAR(50)) + ' - ' END, [CIP], ' - '
				, [T_PSSM_Projection_Cred_Grp].[PSSM_Credential]) ,
	        CONCAT(CASE WHEN [COSC_GRAD_STATUS_LGDS_CD] IS NULL THEN Null ELSE CAST([COSC_GRAD_STATUS_LGDS_CD] AS NVARCHAR(50)) + ' - ' END, Left([CIP],2), ' - '
				, [T_PSSM_Projection_Cred_Grp].[PSSM_Credential]),
	        T_Predict_CIP_CRED_AGE_Flipped.AGE,
	        T_Predict_CIP_CRED_AGE_Flipped.Year;"
dbExecute(decimal_con, qry_10a_Program_Dist_Count)

# [SELECT INTO] Create qry_10b_Program_Dist_Total from qry_10a_Program_Dist_Count

# ---- qry_10b_Program_Dist_Total ----
qry_10b_Program_Dist_Total <-
"SELECT qry_10a_Program_Dist_Count.PSSM_Credential,
        qry_10a_Program_Dist_Count.PSSM_CRED,
        qry_10a_Program_Dist_Count.AGE,
        qry_10a_Program_Dist_Count.Year,
        Sum(qry_10a_Program_Dist_Count.Count) AS Totals
	INTO    qry_10b_Program_Dist_Total
	FROM    qry_10a_Program_Dist_Count
	GROUP BY qry_10a_Program_Dist_Count.PSSM_Credential,
	        qry_10a_Program_Dist_Count.PSSM_CRED,
	        qry_10a_Program_Dist_Count.AGE,
	        qry_10a_Program_Dist_Count.Year;"
dbExecute(decimal_con, qry_10b_Program_Dist_Total)

# [INSERT INTO] Cohort_Program_Distributions_Projected

# ---- qry_10c_Program_Dist_Distribution ----
qry_10c_Program_Dist_Distribution <-
"INSERT INTO Cohort_Program_Distributions_Projected
( Survey, PSSM_Credential, PSSM_CRED, LCP4_CD, LCIP4_CRED, LCIP2_CRED, Age_Group, [Year], [Count], Total, [Percent] )
	SELECT 'Program_Projections_2023-2024_qry10c' AS Survey,
	        qry_10a_Program_Dist_Count.PSSM_Credential,
	        qry_10a_Program_Dist_Count.PSSM_CRED,
	        qry_10a_Program_Dist_Count.CIP AS LCP4_CD,
	        qry_10a_Program_Dist_Count.LCIP4_CRED,
	        qry_10a_Program_Dist_Count.LCIP2_CRED,
	        qry_10a_Program_Dist_Count.AGE AS Age_Group,
	        qry_10a_Program_Dist_Count.Year,
	        qry_10a_Program_Dist_Count.Count,
	        qry_10b_Program_Dist_Total.Totals,
	        CASE WHEN Totals = 0 THEN 0 ELSE CAST([Count] AS FLOAT)/CAST([Totals] AS FLOAT) END AS [Percent]
	FROM    qry_10a_Program_Dist_Count
	INNER JOIN qry_10b_Program_Dist_Total
	  ON    (qry_10a_Program_Dist_Count.Year = qry_10b_Program_Dist_Total.Year)
	  AND   (qry_10a_Program_Dist_Count.AGE = qry_10b_Program_Dist_Total.AGE)
	  AND   (qry_10a_Program_Dist_Count.PSSM_CRED = qry_10b_Program_Dist_Total.PSSM_CRED);"
dbExecute(decimal_con, qry_10c_Program_Dist_Distribution)
dbExecute(decimal_con, "DROP TABLE qry_10a_Program_Dist_Count")
dbExecute(decimal_con, "DROP TABLE qry_10b_Program_Dist_Total")

# survey == 'Program_Projections_2023-2024_qry12c' (Projected) ----
# adds projected counts to Cohort_Program_Distributions_Projected where PSSM_Credential IN ('GRCT or GRDP','PDEG','MAST','DOCT')

# [SELECT INTO] Create qry_12a_Program_Dist_Count

# ---- qry_12a_Program_Dist_Count ----
qry_12a_Program_Dist_Count <- "
	SELECT T_PSSM_Projection_Cred_Grp.PSSM_Credential,
	        CONCAT(CASE WHEN [COSC_GRAD_STATUS_LGDS_CD] IS NULL THEN Null ELSE CAST([COSC_GRAD_STATUS_LGDS_CD] AS NVARCHAR(50)) + ' - ' END, [T_PSSM_Projection_Cred_Grp].[PSSM_Credential]) AS PSSM_CRED,
	        qry_12_LCP4_LCIPPC_Recode_9999.LCIP_LCIPPC_CD AS LCIPPC_CD,
	        CONCAT(CASE WHEN [COSC_GRAD_STATUS_LGDS_CD] IS NULL THEN Null ELSE CAST([COSC_GRAD_STATUS_LGDS_CD] AS NVARCHAR(50)) + ' - ' END, [LCIP_LCIPPC_CD], ' - ', [T_PSSM_Projection_Cred_Grp].[PSSM_Credential]) AS LCIPPC_CRED,
	        T_Predict_CIP_CRED_AGE_Flipped.AGE AS Age_Group,
	        T_Predict_CIP_CRED_AGE_Flipped.Year,
	        Sum(T_Predict_CIP_CRED_AGE_Flipped.Count) AS [Count]
	INTO    qry_12a_Program_Dist_Count
	FROM    (T_Predict_CIP_CRED_AGE_Flipped INNER JOIN T_PSSM_Projection_Cred_Grp ON T_Predict_CIP_CRED_AGE_Flipped.CRED = T_PSSM_Projection_Cred_Grp.PSSM_Projection_Credential)
	INNER JOIN qry_12_LCP4_LCIPPC_Recode_9999
	ON      T_Predict_CIP_CRED_AGE_Flipped.CIP = qry_12_LCP4_LCIPPC_Recode_9999.LCIP_LCP4_CD
	WHERE   (((T_PSSM_Projection_Cred_Grp.PSSM_Credential) In ('GRCT or GRDP','PDEG','MAST','DOCT')))
	GROUP BY T_PSSM_Projection_Cred_Grp.PSSM_Credential,
	        CONCAT(CASE WHEN [COSC_GRAD_STATUS_LGDS_CD] IS NULL THEN Null ELSE CAST([COSC_GRAD_STATUS_LGDS_CD] AS NVARCHAR(50)) + ' - ' END, [T_PSSM_Projection_Cred_Grp].[PSSM_Credential]),
	        qry_12_LCP4_LCIPPC_Recode_9999.LCIP_LCIPPC_CD,
			CONCAT(CASE WHEN [COSC_GRAD_STATUS_LGDS_CD] IS NULL THEN Null ELSE CAST([COSC_GRAD_STATUS_LGDS_CD] AS NVARCHAR(50)) + ' - ' END, [LCIP_LCIPPC_CD], ' - ', [T_PSSM_Projection_Cred_Grp].[PSSM_Credential]),
	        T_Predict_CIP_CRED_AGE_Flipped.AGE,
			T_Predict_CIP_CRED_AGE_Flipped.Year;"
dbExecute(decimal_con, qry_12a_Program_Dist_Count)

# [SELECT INTO] Create qry_12b_Program_Dist_Total from qry_12a_Program_Dist_Count

# ---- qry_12b_Program_Dist_Total ----
qry_12b_Program_Dist_Total <-
"SELECT qry_12a_Program_Dist_Count.PSSM_Credential,
        qry_12a_Program_Dist_Count.PSSM_CRED,
        qry_12a_Program_Dist_Count.Age_Group,
        qry_12a_Program_Dist_Count.Year,
        Sum(qry_12a_Program_Dist_Count.Count) AS Totals
	INTO qry_12b_Program_Dist_Total
	FROM qry_12a_Program_Dist_Count
	GROUP BY qry_12a_Program_Dist_Count.PSSM_Credential,
	         qry_12a_Program_Dist_Count.PSSM_CRED,
	         qry_12a_Program_Dist_Count.Age_Group,
	         qry_12a_Program_Dist_Count.Year;"
dbExecute(decimal_con, qry_12b_Program_Dist_Total)

# [INSERT INTO] Cohort_Program_Distributions_Projected

# ---- qry_12c_Program_Dist_Distribution ----
qry_12c_Program_Dist_Distribution <-
"INSERT INTO Cohort_Program_Distributions_Projected
( Survey, PSSM_Credential, PSSM_CRED, LCP4_CD, LCIP4_CRED, Age_Group, [Year], [Count], Total, [Percent] )
	SELECT 'Program_Projections_2023-2024_qry12c' AS Survey,
	        qry_12a_Program_Dist_Count.PSSM_Credential,
	        qry_12a_Program_Dist_Count.PSSM_CRED,
	        qry_12a_Program_Dist_Count.LCIPPC_CD,
	        qry_12a_Program_Dist_Count.LCIPPC_CRED,
	        qry_12a_Program_Dist_Count.Age_Group,
	        qry_12a_Program_Dist_Count.Year,
	        qry_12a_Program_Dist_Count.Count,
	        qry_12b_Program_Dist_Total.Totals,
	        IIf(([Totals]=0),0,[Count]/[Totals]) AS [%]
	FROM    qry_12a_Program_Dist_Count
	INNER JOIN qry_12b_Program_Dist_Total
	  ON    (qry_12a_Program_Dist_Count.PSSM_CRED = qry_12b_Program_Dist_Total.PSSM_CRED)
	  AND   (qry_12a_Program_Dist_Count.Age_Group = qry_12b_Program_Dist_Total.Age_Group)
	  AND   (qry_12a_Program_Dist_Count.Year = qry_12b_Program_Dist_Total.Year);"
dbExecute(decimal_con, qry_12c_Program_Dist_Distribution)
dbExecute(decimal_con, "DROP TABLE qry_12a_Program_Dist_Count")
dbExecute(decimal_con, "DROP TABLE qry_12b_Program_Dist_Total")
dbExecute(decimal_con, "drop table qry_12_LCP4_LCIPPC_Recode_9999")
dbExecute(decimal_con, "drop table T_Predict_CIP_CRED_AGE_Flipped")

# check for combinations produced in static that were missed in the projected

# [SQL]

# ---- qry_12d_Check_Missing ----
qry_12d_Check_Missing <-
"SELECT Cohort_Program_Distributions_Static.PSSM_Credential,
        Cohort_Program_Distributions_Static.PSSM_CRED,
        Cohort_Program_Distributions_Static.LCP4_CD,
        Cohort_Program_Distributions_Static.LCIP4_CRED,
        Cohort_Program_Distributions_Static.Age_Group,
        Cohort_Program_Distributions_Static.Year,
        Cohort_Program_Distributions_Projected.PSSM_Credential,
        Cohort_Program_Distributions_Projected.PSSM_CRED,
        Cohort_Program_Distributions_Projected.LCP4_CD,
        Cohort_Program_Distributions_Projected.Age_Group,
        Cohort_Program_Distributions_Projected.Year,
        Cohort_Program_Distributions_Static.Count
	FROM    Cohort_Program_Distributions_Static
	LEFT JOIN Cohort_Program_Distributions_Projected
	  ON    (Cohort_Program_Distributions_Static.Year = Cohort_Program_Distributions_Projected.Year)
	  AND   (Cohort_Program_Distributions_Static.Age_Group = Cohort_Program_Distributions_Projected.Age_Group)
	  AND   (Cohort_Program_Distributions_Static.LCP4_CD = Cohort_Program_Distributions_Projected.LCP4_CD)
	  AND   (Cohort_Program_Distributions_Static.PSSM_CRED = Cohort_Program_Distributions_Projected.PSSM_CRED)
	  AND   (Cohort_Program_Distributions_Static.PSSM_Credential = Cohort_Program_Distributions_Projected.PSSM_Credential)
	WHERE (((Cohort_Program_Distributions_Static.Age_Group)
	      Not In ('15 to 16','65 to 89'))
	      AND ((Cohort_Program_Distributions_Projected.PSSM_Credential) Is Null)
	      AND ((Cohort_Program_Distributions_Projected.PSSM_CRED) Is Null)
	      AND ((Cohort_Program_Distributions_Projected.LCP4_CD) Is Null)
	      AND ((Cohort_Program_Distributions_Projected.Age_Group) Is Null)
	      AND ((Cohort_Program_Distributions_Projected.Year) Is Null));"
dbGetQuery(decimal_con, qry_12d_Check_Missing)

# ---- Clean Up ----
# Lookups
dbExecute(decimal_con, "drop table AgeGroupLookup")
dbExecute(decimal_con, "drop table tbl_Age_Groups_Near_Completers")
dbExecute(decimal_con, "drop table tbl_Age_Groups")
dbExecute(decimal_con, "drop table T_Cohort_Program_Distributions_Y2_to_Y12")
dbExecute(decimal_con, "drop table T_APPR_Y2_to_Y10")
dbExecute(decimal_con, "drop table T_PSSM_Projection_Cred_Grp")
dbExecute(decimal_con, "drop table T_Weights_STP")

# Keep for next workflow
dbExistsTable(decimal_con, "Cohort_Program_Distributions_Projected")
dbExistsTable(decimal_con, "Cohort_Program_Distributions_Static")

# Keep in DB
dbExistsTable(decimal_con, "tbl_Program_Projection_Input")


