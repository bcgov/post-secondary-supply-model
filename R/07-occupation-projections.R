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

# This script computes the final NLS distributions based on program projections, labour supply distributions
# and occupation distributions.  

# Tables were created in the cohorts database process and populated here
# Then all of the labour supply and occ queries were moved around a bit to 
# incorporate looking for unknown labour supply and unknown occs in more steps 
# than was previously necessary for the LCIP2 and private institution proxies.

# QI: The model is rerun a second time and all of these queries are also re-run
# to create an indicator which measures the quality of predictability for each NOC projection.

# Age groups: 17 to 19, 20 to 24, 25 to 29, and 30 to 34, and 35 to 64
# Credentials: From Diploma, Associate Degree, and Certificate Outcomes Survey cohorts. 
# Survey years: 2019/2020 to 2030/2031 for PSSM 2019

#  Note: Q_1_Grad_Projections_by_Age_by_Program links to the following tables to exclude programs 
#  where Student Outcomes results not available or inappropriate 
#   - T_Exclude_from_Projections_LCIP4_CRED
#	  - T_Exclude_from_Projections_LCP4_CD
#	  - T_Exclude_from_Projections_PSSM_Credential
#
# When switching between models, copy Static/Projected into Cohort_Program_Distributions
# Fixes To do: some of the CIP2 variable names are missing an "I" in the Labour_Supply_Distribution datasets.  


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

# List of required tables for Derived Tables and Lookups
required_tables <- c(
  # Derived tables
  "Labour_Supply_Distribution",
  "Labour_Supply_Distribution_LCP2",
  "Labour_Supply_Distribution_No_TT",
  "Labour_Supply_Distribution_LCP2_No_TT",
  "Occupation_Distributions", 
  
  "Occupation_Distributions_No_TT",
  "Occupation_Distributions_LCP2",
  "Occupation_Distributions_LCP2_No_TT",
  "Cohort_Program_Distributions_Projected",
  "Cohort_Program_Distributions_Static",
  # "Cohort_Program_Distributions", #Might get FALSE that Cohort_Program_Distributions exists during first run-through.   Use the Static Cohort_Program_Distributions table.
  "Graduate_Projections",
  
  # Lookups
  "INFOWARE_L_CIP_4DIGITS_CIP2016",
  "INFOWARE_L_CIP_6DIGITS_CIP2016",
  "T_Exclude_from_Projections_LCP4_CD",
  "T_Exclude_from_Projections_LCIP4_CRED",
  "T_Exclude_from_Projections_PSSM_Credential",
  "T_Exclude_from_Labour_Supply_Unknown_LCP2_Proxy",
  "tbl_Age_Groups",
  "tbl_Age_Groups_Rollup",
  "T_NOC_Broad_Categories"
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
#dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_NOC_Skill_Type"')))

# ---- SQL Commands ----
# toggle static or projected. 

#dbExecute(decimal_con, "SELECT * INTO Cohort_Program_Distributions 
#                        FROM Cohort_Program_Distributions_Projected;")
if (dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."Cohort_Program_Distributions"'))) == FALSE){
  dbExecute(decimal_con, "SELECT * INTO Cohort_Program_Distributions 
                         FROM Cohort_Program_Distributions_Static;")
}


# Checks

# [SQL]


# ---- Count_Cohort_Program_Distributions ----
Count_Cohort_Program_Distributions <- 
"SELECT Cohort_Program_Distributions.Survey,
Count(*) AS Expr1
FROM Cohort_Program_Distributions
GROUP BY Cohort_Program_Distributions.Survey;"
dbGetQuery(decimal_con, Count_Cohort_Program_Distributions) 

# [SQL]

# ---- Count_Labour_Supply_Distribution ----
Count_Labour_Supply_Distribution1 <- 
"SELECT Labour_Supply_Distribution.Survey, LCIP4_CRED,
Count(*) AS Expr1
FROM Labour_Supply_Distribution
GROUP BY Labour_Supply_Distribution.Survey, LCIP4_CRED;"
dbGetQuery(decimal_con, Count_Labour_Supply_Distribution1) 

# [SQL]

# ---- Count_Labour_Supply_Distribution ----
Count_Labour_Supply_Distribution2 <- 
"SELECT Labour_Supply_Distribution.Survey, PSSM_CREDENTIAL,
Count(*) AS Expr1
FROM Labour_Supply_Distribution
GROUP BY Labour_Supply_Distribution.Survey, PSSM_CREDENTIAL;"
dbGetQuery(decimal_con, Count_Labour_Supply_Distribution2) 

# [SQL]

# ---- Count_Occupation_Distributions ----
Count_Occupation_Distributions1 <- 
  "SELECT Occupation_Distributions.Survey, 
Occupation_Distributions.PSSM_CREDENTIAL,
Count(*) AS Expr1
FROM Occupation_Distributions
GROUP BY Occupation_Distributions.Survey, 
Occupation_Distributions.PSSM_CREDENTIAL;"
dbGetQuery(decimal_con, Count_Occupation_Distributions1) # We want this to contain all of our PSSM Credentials

# [SQL]

# ---- Count_Occupation_Distributions ----
Count_Occupation_Distributions2 <- 
"SELECT Occupation_Distributions.Survey, 
Occupation_Distributions.LCIP4_CRED, 
Count(*) AS Expr1
FROM Occupation_Distributions
GROUP BY Occupation_Distributions.Survey, 
Occupation_Distributions.LCIP4_CRED"
dbGetQuery(decimal_con, Count_Occupation_Distributions2) 

# [SQL]


# ---- Occupation_Unknown ----
Occupation_Unknown <- 
"SELECT Cohort_Program_Distributions.LCIP4_CRED, 
Labour_Supply_Distribution.LCIP4_CRED, 
Cohort_Program_Distributions.Age_Group, 
Occupation_Distributions.LCIP4_CRED, 
Occupation_Distributions.Age_Group_Rollup, 
Labour_Supply_Distribution.New_Labour_Supply, 
Cohort_Program_Distributions.Year, 
Cohort_Program_Distributions.Count, 
Labour_Supply_Distribution.Count, 
Occupation_Distributions.Count, 
Cohort_Program_Distributions.[percent]
FROM (Cohort_Program_Distributions 
INNER JOIN (Labour_Supply_Distribution 
	LEFT JOIN Occupation_Distributions 
	ON (Labour_Supply_Distribution.Age_Group_Rollup = Occupation_Distributions.Age_Group_Rollup) 
	AND (Labour_Supply_Distribution.LCIP4_CRED = Occupation_Distributions.LCIP4_CRED)
) 
ON Cohort_Program_Distributions.LCIP4_CRED = Labour_Supply_Distribution.LCIP4_CRED) 
INNER JOIN tbl_Age_Groups 
	ON (Labour_Supply_Distribution.Age_Group_Rollup = tbl_Age_Groups.Age_Group_Rollup) 
	AND (Cohort_Program_Distributions.Age_Group = tbl_Age_Groups.Age_Group_Label)
WHERE (((Occupation_Distributions.LCIP4_CRED) Is Null) 
AND ((Occupation_Distributions.Age_Group_Rollup) Is Null) 
AND ((Cohort_Program_Distributions.Year)='2023/2024'));"
dbGetQuery(decimal_con, Occupation_Unknown) 

# creates mapping for LCIP4 to LCIP2 
if (dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_LCP2_LCP4"'))) == FALSE){

# [SELECT INTO] Create T_LCP2_LCP4 from INFOWARE_L_CIP_6DIGITS_CIP2016


# ---- Q_0_LCP2_LCP4 ----
Q_0_LCP2_LCP4 <- 
"SELECT INFOWARE_L_CIP_6DIGITS_CIP2016.LCIP_LCP2_CD, 
INFOWARE_L_CIP_6DIGITS_CIP2016.LCIP_LCP4_CD INTO T_LCP2_LCP4
FROM INFOWARE_L_CIP_6DIGITS_CIP2016
GROUP BY INFOWARE_L_CIP_6DIGITS_CIP2016.LCIP_LCP2_CD, 
INFOWARE_L_CIP_6DIGITS_CIP2016.LCIP_LCP4_CD;"
dbExecute(decimal_con, Q_0_LCP2_LCP4) # The table “T_LCP2_LCP4” has already been create in previous script: load-occupation-projections.R 
}# the code is: Q_0_LCP2_LCP4 <- 
# "SELECT INFOWARE_L_CIP_6DIGITS_CIP2016.LCIP_LCP2_CD, 
# INFOWARE_L_CIP_6DIGITS_CIP2016.LCIP_LCP4_CD INTO T_LCP2_LCP4
# FROM INFOWARE_L_CIP_6DIGITS_CIP2016
# GROUP BY INFOWARE_L_CIP_6DIGITS_CIP2016.LCIP_LCP2_CD, 
# INFOWARE_L_CIP_6DIGITS_CIP2016.LCIP_LCP4_CD;"

# essentially duplicates records (as a placeholder to insert graduate records for ptib later?)  
if (ptib_run == TRUE) {

# [INSERT INTO] Labour_Supply_Distribution_No_TT

# ---- Q_0b_Append_Private_Institution_Labour_Supply_Distribution ----
Q_0b_Append_Private_Institution_Labour_Supply_Distribution <- 
"INSERT INTO Labour_Supply_Distribution_No_TT 
( Survey, PSSM_Credential, PSSM_CRED, LCP4_CD, LCIP4_CRED, Current_Region_PSSM_Code_Rollup, Age_Group_Rollup, [Count], Total, New_Labour_Supply )
SELECT 'PTIB' AS Survey, 
Labour_Supply_Distribution_No_TT.PSSM_Credential, 
'P - ' + [Labour_Supply_Distribution_No_TT].[PSSM_Credential] AS PSSM_CRED, 
Labour_Supply_Distribution_No_TT.LCP4_CD, 'P - ' + [Labour_Supply_Distribution_No_TT].[LCP4_CD] + ' - ' + [Labour_Supply_Distribution_No_TT].[PSSM_Credential] AS LCIP4_CRED, 
Labour_Supply_Distribution_No_TT.Current_Region_PSSM_Code_Rollup, 
Labour_Supply_Distribution_No_TT.Age_Group_Rollup, Labour_Supply_Distribution_No_TT.Count, Labour_Supply_Distribution_No_TT.Total, 
Labour_Supply_Distribution_No_TT.New_Labour_Supply
FROM Labour_Supply_Distribution_No_TT
WHERE (((Labour_Supply_Distribution_No_TT.PSSM_Credential) In ('CERT','DIPL','ADGR or UT','BACH','MAST','DOCT')) 
AND ((Labour_Supply_Distribution_No_TT.LCIP4_CRED) Not Like '3 - %'));"
dbExecute(decimal_con, Q_0b_Append_Private_Institution_Labour_Supply_Distribution) 

# [INSERT INTO] Labour_Supply_Distribution_LCP2_No_TT


# ---- Q_0b_Append_Private_Institution_Labour_Supply_Distribution_2D ----
Q_0b_Append_Private_Institution_Labour_Supply_Distribution_2D <- 
"INSERT INTO Labour_Supply_Distribution_LCP2_No_TT 
( Survey, PSSM_Credential, PSSM_CRED, LCP2_CD, LCP2_CRED, 
Current_Region_PSSM_Code_Rollup, Age_Group_Rollup, [Count], Total, New_Labour_Supply )
SELECT 'PTIB' AS Survey, Labour_Supply_Distribution_LCP2_No_TT.PSSM_Credential, 
'P - ' + [Labour_Supply_Distribution_LCP2_No_TT].[PSSM_Credential] AS PSSM_CRED, 
Labour_Supply_Distribution_LCP2_No_TT.LCP2_CD, 'P - ' + [Labour_Supply_Distribution_LCP2_No_TT].[LCP2_CD] + ' - ' + [Labour_Supply_Distribution_LCP2_No_TT].[PSSM_Credential] AS LCIP2_CRED, 
Labour_Supply_Distribution_LCP2_No_TT.Current_Region_PSSM_Code_Rollup, 
Labour_Supply_Distribution_LCP2_No_TT.Age_Group_Rollup, 
Labour_Supply_Distribution_LCP2_No_TT.Count, 
Labour_Supply_Distribution_LCP2_No_TT.Total, 
Labour_Supply_Distribution_LCP2_No_TT.New_Labour_Supply
FROM Labour_Supply_Distribution_LCP2_No_TT
WHERE (((Labour_Supply_Distribution_LCP2_No_TT.PSSM_Credential) In ('CERT','DIPL','ADGR or UT','BACH','MAST','DOCT')) 
AND ((Labour_Supply_Distribution_LCP2_No_TT.LCP2_CRED) Not Like '3 - %'));"
dbExecute(decimal_con, Q_0b_Append_Private_Institution_Labour_Supply_Distribution_2D)

# [INSERT INTO] Occupation_Distributions_No_TT


# ---- Q_0c_Append_Private_Institution_Occupation_Distribution ----
Q_0c_Append_Private_Institution_Occupation_Distribution <- 
"INSERT INTO Occupation_Distributions_No_TT 
( Survey, PSSM_Credential, PSSM_CRED, LCP4_CD, LCIP4_CRED, LCIP2_CRED, Current_Region_PSSM_Code_Rollup, Age_Group_Rollup, NOC, [Count], Total, [Percent] )
SELECT 'PTIB' AS Survey, 
Occupation_Distributions_No_TT.PSSM_Credential, 
'P - ' + [Occupation_Distributions_No_TT].[PSSM_Credential] AS PSSM_CRED, 
Occupation_Distributions_No_TT.LCP4_CD, 'P - ' + [Occupation_Distributions_No_TT].[LCP4_CD] + ' - ' + [Occupation_Distributions_No_TT].[PSSM_Credential] AS LCIP4_CRED, 
Occupation_Distributions_No_TT.LCIP2_CRED, 
Occupation_Distributions_No_TT.Current_Region_PSSM_Code_Rollup, 
Occupation_Distributions_No_TT.Age_Group_Rollup, 
Occupation_Distributions_No_TT.NOC, 
Occupation_Distributions_No_TT.[Count], 
Occupation_Distributions_No_TT.[Total], 
Occupation_Distributions_No_TT.[Percent]
FROM Occupation_Distributions_No_TT
WHERE (((Occupation_Distributions_No_TT.PSSM_Credential) In ('CERT','DIPL','ADGR or UT','BACH','MAST','DOCT')) 
AND ((Occupation_Distributions_No_TT.LCIP4_CRED) Not Like '3 - %'));"
dbExecute(decimal_con, Q_0c_Append_Private_Institution_Occupation_Distribution) 

# [INSERT INTO] Occupation_Distributions_LCP2_No_TT


# ---- Q_0c_Append_Private_Institution_Occupation_Distribution_2D ----
Q_0c_Append_Private_Institution_Occupation_Distribution_2D <- 
"INSERT INTO Occupation_Distributions_LCP2_No_TT 
( Survey, PSSM_Credential, PSSM_CRED, LCP2_CD, LCIP2_CRED, Current_Region_PSSM_Code_Rollup, Age_Group_Rollup, NOC, [Count], Total, [Percent] )
SELECT 'PTIB' AS Survey, 
Occupation_Distributions_LCP2_No_TT.PSSM_Credential, 
'P - ' + [Occupation_Distributions_LCP2_No_TT].[PSSM_Credential] AS PSSM_CRED, 
Occupation_Distributions_LCP2_No_TT.LCP2_CD, 'P - ' + [Occupation_Distributions_LCP2_No_TT].[LCP2_CD] + ' - ' + [Occupation_Distributions_LCP2_No_TT].[PSSM_Credential] AS LCIP2_CRED, 
Occupation_Distributions_LCP2_No_TT.Current_Region_PSSM_Code_Rollup, 
Occupation_Distributions_LCP2_No_TT.Age_Group_Rollup, 
Occupation_Distributions_LCP2_No_TT.NOC, 
Occupation_Distributions_LCP2_No_TT.[Count], 
Occupation_Distributions_LCP2_No_TT.[Total], 
Occupation_Distributions_LCP2_No_TT.[Percent]
FROM Occupation_Distributions_LCP2_No_TT
WHERE (((Occupation_Distributions_LCP2_No_TT.PSSM_Credential) In ('CERT','DIPL','ADGR or UT','BACH','MAST','DOCT')) 
AND ((Occupation_Distributions_LCP2_No_TT.LCIP2_CRED) Not Like '3 - %'));"
dbExecute(decimal_con, Q_0c_Append_Private_Institution_Occupation_Distribution_2D) 
}

# use these to delete PTIB for running a model without private institutions
if (ptib_run == FALSE) {

# [DELETE] Labour_Supply_Distribution_No_TT


# ---- Q_0a_Delete_Private_Inst_Labour_Supply_Distribution ----
Q_0a_Delete_Private_Inst_Labour_Supply_Distribution <- 
"DELETE 
FROM Labour_Supply_Distribution_No_TT
WHERE (((Labour_Supply_Distribution_No_TT.PSSM_CRED) Like 'P - %'));"
dbExecute(decimal_con, Q_0a_Delete_Private_Inst_Labour_Supply_Distribution) 

# [DELETE] Labour_Supply_Distribution_LCP2_No_TT


# ---- Q_0a_Delete_Private_Inst_Labour_Supply_Distribution_LCP2 ----
Q_0a_Delete_Private_Inst_Labour_Supply_Distribution_LCP2 <- 
"DELETE 
FROM Labour_Supply_Distribution_LCP2_No_TT
WHERE (((Labour_Supply_Distribution_LCP2_No_TT.PSSM_CRED) Like 'P - %'));"
dbExecute(decimal_con, Q_0a_Delete_Private_Inst_Labour_Supply_Distribution_LCP2) 

# [DELETE] Occupation_Distributions_No_TT


# ---- Q_0a_Delete_Private_Inst_Occupation_Distribution ----
Q_0a_Delete_Private_Inst_Occupation_Distribution <- 
"DELETE 
FROM Occupation_Distributions_No_TT
WHERE (((Occupation_Distributions_No_TT.PSSM_CRED) Like 'P - %'));"
dbExecute(decimal_con, Q_0a_Delete_Private_Inst_Occupation_Distribution) 

# [DELETE] Occupation_Distributions_LCP2_No_TT

# ---- Q_0a_Delete_Private_Inst_Occupation_Distribution_LCP2 ----
Q_0a_Delete_Private_Inst_Occupation_Distribution_LCP2 <- 
"DELETE 
FROM Occupation_Distributions_LCP2_No_TT
WHERE (((Occupation_Distributions_LCP2_No_TT.PSSM_CRED) Like 'P - %'));"
dbExecute(decimal_con, Q_0a_Delete_Private_Inst_Occupation_Distribution_LCP2)
}

# ---- Q_1 Series ---- 

# [SELECT INTO] Create Q_1_Grad_Projections_by_Age_by_Program


# ---- Q_1_Grad_Projections_by_Age_by_Program ----
Q_1_Grad_Projections_by_Age_by_Program <- 
"SELECT Cohort_Program_Distributions.PSSM_Credential AS PSSM_Credential, 
        Graduate_Projections.PSSM_CRED, 
        Graduate_Projections.Age_Group, 
        Graduate_Projections.Year, 
        Cohort_Program_Distributions.LCP4_CD, 
        Cohort_Program_Distributions.GRAD_STATUS, 
        Cohort_Program_Distributions.TTRAIN, 
        Cohort_Program_Distributions.LCIP4_CRED, 
        [Graduate_Projections].[Graduates]*[Cohort_Program_Distributions].[Percent] AS Grads
INTO Q_1_Grad_Projections_by_Age_by_Program
FROM    ((T_Exclude_from_Projections_LCP4_CD 
RIGHT JOIN (Graduate_Projections 
  INNER JOIN Cohort_Program_Distributions 
    ON  (Graduate_Projections.Year = Cohort_Program_Distributions.Year) 
    AND (Graduate_Projections.Age_Group = Cohort_Program_Distributions.Age_Group) 
    AND (Graduate_Projections.PSSM_CRED = Cohort_Program_Distributions.PSSM_CRED)) 
  ON    T_Exclude_from_Projections_LCP4_CD.LCIP_LCP4_CD = Cohort_Program_Distributions.LCP4_CD) 
LEFT JOIN T_Exclude_from_Projections_PSSM_Credential 
  ON    Cohort_Program_Distributions.PSSM_Credential = T_Exclude_from_Projections_PSSM_Credential.PSSM_Credential) 
LEFT JOIN T_Exclude_from_Projections_LCIP4_CRED 
  ON    Cohort_Program_Distributions.LCIP4_CRED = T_Exclude_from_Projections_LCIP4_CRED.LCIP4_CRED
WHERE   (((T_Exclude_from_Projections_LCP4_CD.LCIP_LCP4_CD) Is Null) 
  AND   ((T_Exclude_from_Projections_PSSM_Credential.PSSM_Credential) Is Null) 
  AND   ((T_Exclude_from_Projections_LCIP4_CRED.LCIP4_CRED) Is Null));"
dbExecute(decimal_con, Q_1_Grad_Projections_by_Age_by_Program) 

# [SELECT INTO] Create Q_1_Grad_Projections_by_Age_by_Program_Static


# ---- Q_1_Grad_Projections_by_Age_by_Program_Static ----
Q_1_Grad_Projections_by_Age_by_Program_Static <- "
SELECT Cohort_Program_Distributions_Static.PSSM_Credential, 
       Graduate_Projections.PSSM_CRED, 
       Graduate_Projections.Age_Group, 
       Graduate_Projections.Year, 
       Cohort_Program_Distributions_Static.LCP4_CD, 
       Cohort_Program_Distributions_Static.GRAD_STATUS, 
       Cohort_Program_Distributions_Static.TTRAIN, 
       --Cohort_Program_Distributions_Static.LCIP4_CRED, 
       [Graduate_Projections].[Graduates]*[Cohort_Program_Distributions_Static].[Percent] AS Grads, 
       T_Exclude_from_Projections_LCIP4_CRED.LCIP4_CRED
INTO Q_1_Grad_Projections_by_Age_by_Program_Static
FROM  (((Graduate_Projections 
INNER JOIN Cohort_Program_Distributions_Static 
  ON  (Cohort_Program_Distributions_Static.Year = Graduate_Projections.Year) 
  AND (Graduate_Projections.Age_Group = Cohort_Program_Distributions_Static.Age_Group) 
  AND (Graduate_Projections.PSSM_CRED = Cohort_Program_Distributions_Static.PSSM_CRED)) 
LEFT JOIN T_Exclude_from_Projections_PSSM_Credential 
  ON  Cohort_Program_Distributions_Static.PSSM_Credential = T_Exclude_from_Projections_PSSM_Credential.PSSM_Credential) 
LEFT JOIN T_Exclude_from_Projections_LCP4_CD 
  ON  Cohort_Program_Distributions_Static.LCP4_CD = T_Exclude_from_Projections_LCP4_CD.LCIP_LCP4_CD) 
LEFT JOIN T_Exclude_from_Projections_LCIP4_CRED 
  ON  Cohort_Program_Distributions_Static.LCIP4_CRED = T_Exclude_from_Projections_LCIP4_CRED.LCIP4_CRED
WHERE (((T_Exclude_from_Projections_LCIP4_CRED.LCIP4_CRED) Is Null) 
  AND ((T_Exclude_from_Projections_PSSM_Credential.PSSM_Credential) Is Null) 
  AND ((T_Exclude_from_Projections_LCP4_CD.LCIP_LCP4_CD) Is Null));"
dbExecute(decimal_con, Q_1_Grad_Projections_by_Age_by_Program_Static) 

# [SQL]


# ---- Q_1b_Checking_Grads_by_Year_Excludes_CIPs ----
Q_1b_Checking_Grads_by_Year_Excludes_CIPs <- "
SELECT PSSM_Credential, PSSM_CRED, Age_Group, [2022/2023], [2023/2024], [2024/2025], 
[2025/2026], [2026/2027], [2027/2028], [2028/2029], [2029/2030], [2030/2031], [2031/2032], 
[2032/2033], [2033/2034],[2034/2035]
FROM (
    SELECT PSSM_Credential, PSSM_CRED, Age_Group, Year, Grads
	FROM Q_1_Grad_Projections_by_Age_by_Program
) AS SourceTable
PIVOT (
    Sum(Grads) 
	FOR Year IN
    ([2022/2023], [2023/2024], [2024/2025], [2025/2026], [2026/2027], [2027/2028], 
    [2028/2029], [2029/2030], [2030/2031], [2031/2032], [2032/2033], [2033/2034],[2034/2035])
) AS PivotTable
order by PSSM_Credential, PSSM_CRED, Age_Group"
dbGetQuery(decimal_con, Q_1b_Checking_Grads_by_Year_Excludes_CIPs)

# [SELECT INTO] Create Q_1c_Grad_Projections_by_Program


# ---- Q_1c_Grad_Projections_by_Program ----
Q_1c_Grad_Projections_by_Program <- 
"SELECT Q_1_Grad_Projections_by_Age_by_Program.PSSM_Credential, 
        Q_1_Grad_Projections_by_Age_by_Program.PSSM_CRED, 
        tbl_Age_Groups_Rollup.Age_Group_Rollup, 
        tbl_Age_Groups_Rollup.Age_Group_Rollup_Label, 
        Q_1_Grad_Projections_by_Age_by_Program.Year, 
        Q_1_Grad_Projections_by_Age_by_Program.GRAD_STATUS, 
        Q_1_Grad_Projections_by_Age_by_Program.TTRAIN, 
        Q_1_Grad_Projections_by_Age_by_Program.LCP4_CD, 
        Q_1_Grad_Projections_by_Age_by_Program.LCIP4_CRED, 
        Sum(Q_1_Grad_Projections_by_Age_by_Program.Grads) AS Grads
INTO Q_1c_Grad_Projections_by_Program
FROM    (Q_1_Grad_Projections_by_Age_by_Program 
INNER JOIN tbl_Age_Groups 
  ON    Q_1_Grad_Projections_by_Age_by_Program.Age_Group = tbl_Age_Groups.Age_Group_Label) 
INNER JOIN tbl_Age_Groups_Rollup 
  ON    tbl_Age_Groups.Age_Group_Rollup = tbl_Age_Groups_Rollup.Age_Group_Rollup
GROUP BY Q_1_Grad_Projections_by_Age_by_Program.PSSM_Credential, 
        Q_1_Grad_Projections_by_Age_by_Program.PSSM_CRED, 
        tbl_Age_Groups_Rollup.Age_Group_Rollup, 
        tbl_Age_Groups_Rollup.Age_Group_Rollup_Label, 
        Q_1_Grad_Projections_by_Age_by_Program.Year, 
        Q_1_Grad_Projections_by_Age_by_Program.GRAD_STATUS, 
        Q_1_Grad_Projections_by_Age_by_Program.TTRAIN, 
        Q_1_Grad_Projections_by_Age_by_Program.LCP4_CD, 
        Q_1_Grad_Projections_by_Age_by_Program.LCIP4_CRED;"
dbExecute(decimal_con, Q_1c_Grad_Projections_by_Program) 

# [SELECT INTO] Create Q_1c_Grad_Projections_by_Program_LCP2


# ---- Q_1c_Grad_Projections_by_Program_LCP2 ----
Q_1c_Grad_Projections_by_Program_LCP2 <- 
" SELECT Q_1_Grad_Projections_by_Age_by_Program.PSSM_Credential, 
Q_1_Grad_Projections_by_Age_by_Program.PSSM_CRED, 
tbl_Age_Groups_Rollup.Age_Group_Rollup, 
tbl_Age_Groups_Rollup.Age_Group_Rollup_Label, 
Q_1_Grad_Projections_by_Age_by_Program.Year, 
Left(LCP4_CD,2) AS LCP2_CD, 
Q_1_Grad_Projections_by_Age_by_Program.GRAD_STATUS, 
Q_1_Grad_Projections_by_Age_by_Program.TTRAIN, 
CONCAT(
  (CASE WHEN (Left(PSSM_CRED,1)='1' Or Left(PSSM_CRED,1)='3' Or Left(PSSM_CRED,1)='P') THEN Left(PSSM_CRED,1) + ' - ' ELSE '' END)
  , Left(LCP4_CD,2) 
  , ' - ' 
  , CASE WHEN TTRAIN IS NULL THEN Null ELSE CAST(TTRAIN AS NVARCHAR(50)) + ' - ' END 
  , PSSM_Credential) AS LCIP2_CRED, 
Sum(Q_1_Grad_Projections_by_Age_by_Program.Grads) AS Grads
INTO Q_1c_Grad_Projections_by_Program_LCP2
FROM    (Q_1_Grad_Projections_by_Age_by_Program 
         INNER JOIN tbl_Age_Groups 
         ON    Q_1_Grad_Projections_by_Age_by_Program.Age_Group = tbl_Age_Groups.Age_Group_Label) 
INNER JOIN tbl_Age_Groups_Rollup 
ON    tbl_Age_Groups.Age_Group_Rollup = tbl_Age_Groups_Rollup.Age_Group_Rollup
GROUP BY Q_1_Grad_Projections_by_Age_by_Program.PSSM_Credential, 
Q_1_Grad_Projections_by_Age_by_Program.PSSM_CRED, 
tbl_Age_Groups_Rollup.Age_Group_Rollup, 
tbl_Age_Groups_Rollup.Age_Group_Rollup_Label, 
Q_1_Grad_Projections_by_Age_by_Program.Year, 
Left([LCP4_CD],2), 
Q_1_Grad_Projections_by_Age_by_Program.GRAD_STATUS, 
Q_1_Grad_Projections_by_Age_by_Program.TTRAIN,
CONCAT(
  (CASE WHEN (Left(PSSM_CRED,1)='1' Or Left(PSSM_CRED,1)='3' Or Left(PSSM_CRED,1)='P') THEN Left(PSSM_CRED,1) + ' - ' ELSE '' END)
  , Left(LCP4_CD,2) 
  , ' - ' 
  , CASE WHEN TTRAIN IS NULL THEN Null ELSE CAST(TTRAIN AS NVARCHAR(50)) + ' - ' END 
  , PSSM_Credential);"
dbExecute(decimal_con, Q_1c_Grad_Projections_by_Program_LCP2)

# ---- Q_2 Series ---- 

# [SELECT INTO] Create Q_2_Labour_Supply_by_LCIP4_CRED from Q_1c_Grad_Projections_by_Program


# ---- Q_2_Labour_Supply_by_LCIP4_CRED ----
Q_2_Labour_Supply_by_LCIP4_CRED <- "
SELECT Q_1c_Grad_Projections_by_Program.PSSM_Credential, 
Q_1c_Grad_Projections_by_Program.PSSM_CRED, 
Q_1c_Grad_Projections_by_Program.Age_Group_Rollup, 
Q_1c_Grad_Projections_by_Program.Age_Group_Rollup_Label, 
Q_1c_Grad_Projections_by_Program.Year, 
Q_1c_Grad_Projections_by_Program.TTRAIN, 
Q_1c_Grad_Projections_by_Program.LCP4_CD AS LCP4_CD, 
Q_1c_Grad_Projections_by_Program.LCIP4_CRED, 
Labour_Supply_Distribution.Current_Region_PSSM_Code_Rollup, 
Labour_Supply_Distribution.New_Labour_Supply, [Grads]*[New_Labour_Supply] AS NLS
INTO Q_2_Labour_Supply_by_LCIP4_CRED 
FROM Q_1c_Grad_Projections_by_Program 
INNER JOIN Labour_Supply_Distribution 
ON (Q_1c_Grad_Projections_by_Program.LCIP4_CRED = Labour_Supply_Distribution.LCIP4_CRED) 
AND (Q_1c_Grad_Projections_by_Program.Age_Group_Rollup = Labour_Supply_Distribution.Age_Group_Rollup);"
dbExecute(decimal_con, Q_2_Labour_Supply_by_LCIP4_CRED)

# [SELECT INTO] Create Q_2a_Labour_Supply_Unknown from q_1c_grad_projections_by_program


# ---- Q_2a_Labour_Supply_Unknown ----
Q_2a_Labour_Supply_Unknown <- 
"SELECT q_1c_grad_projections_by_program.pssm_credential,
       q_1c_grad_projections_by_program.pssm_cred,
       q_1c_grad_projections_by_program.age_group_rollup AS Age_Group_Rollup,
       q_1c_grad_projections_by_program.age_group_rollup_label,
       q_1c_grad_projections_by_program.ttrain,
       q_1c_grad_projections_by_program.lcp4_cd,
       q_1c_grad_projections_by_program.lcip4_cred,
       q_1c_grad_projections_by_program.year,
       Sum(q_1c_grad_projections_by_program.grads)       AS Grads
INTO Q_2a_Labour_Supply_Unknown
FROM   q_1c_grad_projections_by_program
       LEFT JOIN labour_supply_distribution
              ON ( q_1c_grad_projections_by_program.age_group_rollup =
                             labour_supply_distribution.age_group_rollup )
                 AND ( q_1c_grad_projections_by_program.lcip4_cred =
                           labour_supply_distribution.lcip4_cred )
WHERE  ( ( ( labour_supply_distribution.lcip4_cred ) IS NULL )
         AND ( ( labour_supply_distribution.age_group_rollup ) IS NULL ) )
GROUP  BY q_1c_grad_projections_by_program.pssm_credential,
          q_1c_grad_projections_by_program.pssm_cred,
          q_1c_grad_projections_by_program.age_group_rollup,
          q_1c_grad_projections_by_program.age_group_rollup_label,
          q_1c_grad_projections_by_program.ttrain,
          q_1c_grad_projections_by_program.lcp4_cd,
          q_1c_grad_projections_by_program.lcip4_cred,
          q_1c_grad_projections_by_program.year;"
dbExecute(decimal_con, Q_2a_Labour_Supply_Unknown) 

# [SELECT INTO] Create Q_2a2_Labour_Supply_Unknown_No_TT_Proxy from labour_supply_distribution_no_tt

# ---- Q_2a2_Labour_Supply_Unknown_No_TT_Proxy ----
Q_2a2_Labour_Supply_Unknown_No_TT_Proxy <- 
"SELECT q_1c_grad_projections_by_program.pssm_credential,
       q_1c_grad_projections_by_program.pssm_cred,
       q_1c_grad_projections_by_program.age_group_rollup,
       q_1c_grad_projections_by_program.age_group_rollup_label,
       q_1c_grad_projections_by_program.year,
       q_2a_labour_supply_unknown.ttrain,
       q_1c_grad_projections_by_program.lcp4_cd                         AS
       LCP4_CD,
       q_1c_grad_projections_by_program.lcip4_cred,
       labour_supply_distribution_no_tt.current_region_pssm_code_rollup,
       labour_supply_distribution_no_tt.new_labour_supply,
       [q_1c_grad_projections_by_program].[grads] * [new_labour_supply] AS NLS
INTO Q_2a2_Labour_Supply_Unknown_No_TT_Proxy
FROM   labour_supply_distribution_no_tt
       INNER JOIN (q_2a_labour_supply_unknown
                   INNER JOIN q_1c_grad_projections_by_program
                           ON ( q_2a_labour_supply_unknown.year =
                                q_1c_grad_projections_by_program.year )
                              AND ( q_2a_labour_supply_unknown.lcip4_cred =
q_1c_grad_projections_by_program.lcip4_cred )
AND ( q_2a_labour_supply_unknown.age_group_rollup =
q_1c_grad_projections_by_program.age_group_rollup )
AND ( q_2a_labour_supply_unknown.pssm_cred =
q_1c_grad_projections_by_program.pssm_cred ))
ON ( labour_supply_distribution_no_tt.age_group_rollup =
q_1c_grad_projections_by_program.age_group_rollup )
AND ( labour_supply_distribution_no_tt.lcip4_cred =
q_1c_grad_projections_by_program.lcip4_cred );"
dbExecute(decimal_con, Q_2a2_Labour_Supply_Unknown_No_TT_Proxy) 

# [SELECT INTO] Create Q_2a3_Labour_Supply_by_LCIP4_CRED_No_TT_Proxy_Union from Q_2_Labour_Supply_by_LCIP4_CRED


# ---- Q_2a3_Labour_Supply_by_LCIP4_CRED_No_TT_Proxy_Union ----
Q_2a3_Labour_Supply_by_LCIP4_CRED_No_TT_Proxy_Union <- 
"SELECT Q_2_Labour_Supply_by_LCIP4_CRED.*
INTO Q_2a3_Labour_Supply_by_LCIP4_CRED_No_TT_Proxy_Union
FROM Q_2_Labour_Supply_by_LCIP4_CRED
UNION ALL SELECT Q_2a2_Labour_Supply_Unknown_No_TT_Proxy.*
FROM Q_2a2_Labour_Supply_Unknown_No_TT_Proxy;"
dbExecute(decimal_con, Q_2a3_Labour_Supply_by_LCIP4_CRED_No_TT_Proxy_Union) 

# [SELECT INTO] Create tmp_tbl_Q_2a4_Labour_Supply_by_LCIP4_CRED_No_TT_Union_tmp from Q_2a3_Labour_Supply_by_LCIP4_CRED_No_TT_Proxy_Union


# ---- Q_2a4_Labour_Supply ----
Q_2a4_Labour_Supply <- 
"SELECT Q_2a3_Labour_Supply_by_LCIP4_CRED_No_TT_Proxy_Union.* 
INTO tmp_tbl_Q_2a4_Labour_Supply_by_LCIP4_CRED_No_TT_Union_tmp
FROM Q_2a3_Labour_Supply_by_LCIP4_CRED_No_TT_Proxy_Union;"
dbExecute(decimal_con, Q_2a4_Labour_Supply) 

# [SELECT INTO] Create Q_2b_Labour_Supply_Unknown from tmp_tbl_q_2a4_labour_supply_by_lcip4_cred_no_tt_union_tmp


# ---- Q_2b_Labour_Supply_Unknown ----
Q_2b_Labour_Supply_Unknown <- 
"SELECT q_1c_grad_projections_by_program.pssm_credential,
       q_1c_grad_projections_by_program.pssm_cred,
       q_1c_grad_projections_by_program.age_group_rollup AS Age_Group_Rollup,
       q_1c_grad_projections_by_program.age_group_rollup_label,
       q_1c_grad_projections_by_program.ttrain,
       q_1c_grad_projections_by_program.lcp4_cd,
       q_1c_grad_projections_by_program.lcip4_cred,
       q_1c_grad_projections_by_program.year,
       Sum(q_1c_grad_projections_by_program.grads)       AS Grads
INTO Q_2b_Labour_Supply_Unknown
FROM   tmp_tbl_q_2a4_labour_supply_by_lcip4_cred_no_tt_union_tmp
       RIGHT JOIN q_1c_grad_projections_by_program
               ON
       (
tmp_tbl_q_2a4_labour_supply_by_lcip4_cred_no_tt_union_tmp.age_group_rollup =
           q_1c_grad_projections_by_program.age_group_rollup )
           AND (
tmp_tbl_q_2a4_labour_supply_by_lcip4_cred_no_tt_union_tmp.lcip4_cred =
    q_1c_grad_projections_by_program.lcip4_cred )
WHERE  ( ( (
tmp_tbl_q_2a4_labour_supply_by_lcip4_cred_no_tt_union_tmp.lcip4_cred ) IS
       NULL )
         AND ( (
tmp_tbl_q_2a4_labour_supply_by_lcip4_cred_no_tt_union_tmp.age_group_rollup )
  IS NULL ) )
GROUP  BY q_1c_grad_projections_by_program.pssm_credential,
          q_1c_grad_projections_by_program.pssm_cred,
          q_1c_grad_projections_by_program.age_group_rollup,
          q_1c_grad_projections_by_program.age_group_rollup_label,
          q_1c_grad_projections_by_program.ttrain,
          q_1c_grad_projections_by_program.lcp4_cd,
          q_1c_grad_projections_by_program.lcip4_cred,
          q_1c_grad_projections_by_program.year;"
dbExecute(decimal_con, Q_2b_Labour_Supply_Unknown) 

# [SELECT INTO] Create Q_2b2_Labour_Supply_Unknown_Private_Cred_Proxy

# ---- Q_2b2_Labour_Supply_Unknown_Private_Cred_Proxy ----
Q_2b2_Labour_Supply_Unknown_Private_Cred_Proxy <- 
"SELECT q_1c_grad_projections_by_program.pssm_credential,
       q_1c_grad_projections_by_program.pssm_cred,
       q_1c_grad_projections_by_program.age_group_rollup,
       q_1c_grad_projections_by_program.age_group_rollup_label,
       q_1c_grad_projections_by_program.year,
       q_1c_grad_projections_by_program.ttrain,
       q_1c_grad_projections_by_program.lcp4_cd                         AS
       LCP4_CD,
       q_1c_grad_projections_by_program.lcip4_cred,
       labour_supply_distribution_no_tt.current_region_pssm_code_rollup,
       labour_supply_distribution_no_tt.new_labour_supply,
       [q_1c_grad_projections_by_program].[grads] * [new_labour_supply] AS NLS
INTO Q_2b2_Labour_Supply_Unknown_Private_Cred_Proxy 
FROM   (q_2b_labour_supply_unknown
        INNER JOIN q_1c_grad_projections_by_program
                ON ( q_2b_labour_supply_unknown.pssm_cred =
                                q_1c_grad_projections_by_program.pssm_cred )
                   AND ( q_2b_labour_supply_unknown.age_group_rollup =
                             q_1c_grad_projections_by_program.age_group_rollup )
                   AND ( q_2b_labour_supply_unknown.lcip4_cred =
                             q_1c_grad_projections_by_program.lcip4_cred )
                   AND ( q_2b_labour_supply_unknown.year =
                         q_1c_grad_projections_by_program.year ))
       INNER JOIN labour_supply_distribution_no_tt
               ON ( q_1c_grad_projections_by_program.age_group_rollup =
labour_supply_distribution_no_tt.age_group_rollup )
AND ( q_1c_grad_projections_by_program.lcp4_cd =
labour_supply_distribution_no_tt.lcp4_cd )
WHERE  ( ( ( q_1c_grad_projections_by_program.pssm_cred ) LIKE 'P - CERT' )
         AND ( ( labour_supply_distribution_no_tt.pssm_cred ) LIKE 'P - DIPL' )
       )
        OR ( ( ( q_1c_grad_projections_by_program.pssm_cred ) LIKE 'P - DIPL' )
             AND ( ( labour_supply_distribution_no_tt.pssm_cred ) LIKE
                   'P - CERT' ) );"
dbExecute(decimal_con, Q_2b2_Labour_Supply_Unknown_Private_Cred_Proxy) 

# [SELECT INTO] Create Q_2b3_Labour_Supply_by_LCIP4_CRED_Private_Cred_Proxy_Union from tmp_tbl_Q_2a4_Labour_Supply_by_LCIP4_CRED_No_TT_Union_tmp


# ---- Q_2b3_Labour_Supply_by_LCIP4_CRED_Private_Cred_Proxy_Union ----
Q_2b3_Labour_Supply_by_LCIP4_CRED_Private_Cred_Proxy_Union <- 
"SELECT tmp_tbl_Q_2a4_Labour_Supply_by_LCIP4_CRED_No_TT_Union_tmp.*
INTO Q_2b3_Labour_Supply_by_LCIP4_CRED_Private_Cred_Proxy_Union
FROM tmp_tbl_Q_2a4_Labour_Supply_by_LCIP4_CRED_No_TT_Union_tmp
UNION ALL SELECT Q_2b2_Labour_Supply_Unknown_Private_Cred_Proxy.*
FROM Q_2b2_Labour_Supply_Unknown_Private_Cred_Proxy;"
dbExecute(decimal_con, Q_2b3_Labour_Supply_by_LCIP4_CRED_Private_Cred_Proxy_Union) 

# [SELECT INTO] Create Q_2b4_Labour_Supply_Unknown from q_1c_grad_projections_by_program


# ---- Q_2b4_Labour_Supply_Unknown ----
Q_2b4_Labour_Supply_Unknown <- 
"SELECT q_1c_grad_projections_by_program.pssm_credential,
       q_1c_grad_projections_by_program.pssm_cred,
       q_1c_grad_projections_by_program.age_group_rollup AS Age_Group_Rollup,
       q_1c_grad_projections_by_program.age_group_rollup_label,
       q_1c_grad_projections_by_program.ttrain,
       q_1c_grad_projections_by_program.lcp4_cd,
       q_1c_grad_projections_by_program.lcip4_cred       AS LCIP4_CRED,
       q_1c_grad_projections_by_program.year,
       Sum(q_1c_grad_projections_by_program.grads)       AS Grads
INTO Q_2b4_Labour_Supply_Unknown
FROM   q_1c_grad_projections_by_program
       LEFT JOIN q_2b3_labour_supply_by_lcip4_cred_private_cred_proxy_union
              ON ( q_1c_grad_projections_by_program.age_group_rollup =
q_2b3_labour_supply_by_lcip4_cred_private_cred_proxy_union.age_group_rollup )
AND ( q_1c_grad_projections_by_program.lcip4_cred =
q_2b3_labour_supply_by_lcip4_cred_private_cred_proxy_union.lcip4_cred )
WHERE  ( ( (
q_2b3_labour_supply_by_lcip4_cred_private_cred_proxy_union.lcip4_cred ) IS
       NULL )
         AND
         (
         (
q_2b3_labour_supply_by_lcip4_cred_private_cred_proxy_union.age_group_rollup )
  IS NULL ) )
GROUP  BY q_1c_grad_projections_by_program.pssm_credential,
          q_1c_grad_projections_by_program.pssm_cred,
          q_1c_grad_projections_by_program.age_group_rollup,
          q_1c_grad_projections_by_program.age_group_rollup_label,
          q_1c_grad_projections_by_program.ttrain,
          q_1c_grad_projections_by_program.lcp4_cd,
          q_1c_grad_projections_by_program.lcip4_cred,
          q_1c_grad_projections_by_program.year;"
dbExecute(decimal_con, Q_2b4_Labour_Supply_Unknown) 

# [SELECT INTO] Create Q_2c_Labour_Supply_Unknown_LCP2_Proxy from Labour_Supply_Distribution_LCP2


# ---- Q_2c_Labour_Supply_Unknown_LCP2_Proxy ----
Q_2c_Labour_Supply_Unknown_LCP2_Proxy <- 
"SELECT Q_2b4_Labour_Supply_Unknown.PSSM_Credential, 
        Q_2b4_Labour_Supply_Unknown.PSSM_CRED, 
        Q_2b4_Labour_Supply_Unknown.Age_Group_Rollup, 
        Q_2b4_Labour_Supply_Unknown.Age_Group_Rollup_Label, 
        Q_2b4_Labour_Supply_Unknown.Year, 
        Q_2b4_Labour_Supply_Unknown.TTRAIN, Q_2b4_Labour_Supply_Unknown.LCP4_CD, 
        Q_2b4_Labour_Supply_Unknown.LCIP4_CRED, 
        Labour_Supply_Distribution_LCP2.Current_Region_PSSM_Code_Rollup, 
        Labour_Supply_Distribution_LCP2.New_Labour_Supply, 
        [Grads]*[New_Labour_Supply] AS NLS
INTO Q_2c_Labour_Supply_Unknown_LCP2_Proxy
FROM Labour_Supply_Distribution_LCP2 
INNER JOIN ((Q_2b4_Labour_Supply_Unknown 
    LEFT JOIN T_Exclude_from_Labour_Supply_Unknown_LCP2_Proxy 
    ON Q_2b4_Labour_Supply_Unknown.LCP4_CD = T_Exclude_from_Labour_Supply_Unknown_LCP2_Proxy.LCIP_LCP4_CD) 
  INNER JOIN T_LCP2_LCP4 ON Q_2b4_Labour_Supply_Unknown.LCP4_CD = T_LCP2_LCP4.LCIP_LCP4_CD) 
  ON (Labour_Supply_Distribution_LCP2.Age_Group_Rollup = Q_2b4_Labour_Supply_Unknown.Age_Group_Rollup) 
  AND (Labour_Supply_Distribution_LCP2.LCP2_CD = T_LCP2_LCP4.LCIP_LCP2_CD) 
  AND (Labour_Supply_Distribution_LCP2.PSSM_CRED = Q_2b4_Labour_Supply_Unknown.PSSM_CRED)
WHERE (((T_Exclude_from_Labour_Supply_Unknown_LCP2_Proxy.LCIP_LCP4_CD) Is Null)) 
OR (((Left([LCIP4_CRED],1))='P - '));"
dbExecute(decimal_con, Q_2c_Labour_Supply_Unknown_LCP2_Proxy) 

# [SELECT INTO] Create Q_2c2_Labour_Supply_Unknown_LCP2_Proxy_Union from Q_2b3_Labour_Supply_by_LCIP4_CRED_Private_Cred_Proxy_Union


# ---- Q_2c2_Labour_Supply_Unknown_LCP2_Proxy_Union ----
Q_2c2_Labour_Supply_Unknown_LCP2_Proxy_Union <- 
"SELECT Q_2b3_Labour_Supply_by_LCIP4_CRED_Private_Cred_Proxy_Union.*
INTO Q_2c2_Labour_Supply_Unknown_LCP2_Proxy_Union
FROM Q_2b3_Labour_Supply_by_LCIP4_CRED_Private_Cred_Proxy_Union
UNION ALL SELECT Q_2c_Labour_Supply_Unknown_LCP2_Proxy.*
FROM Q_2c_Labour_Supply_Unknown_LCP2_Proxy;"
dbExecute(decimal_con, Q_2c2_Labour_Supply_Unknown_LCP2_Proxy_Union) 

# [SELECT INTO] Create Q_2c3_Labour_Supply_Unknown from q_1c_grad_projections_by_program


# ---- Q_2c3_Labour_Supply_Unknown ----
Q_2c3_Labour_Supply_Unknown <- 
"SELECT q_1c_grad_projections_by_program.pssm_credential,
       q_1c_grad_projections_by_program.pssm_cred,
       q_1c_grad_projections_by_program.age_group_rollup AS Age_Group_Rollup,
       q_1c_grad_projections_by_program.age_group_rollup_label,
       q_1c_grad_projections_by_program.ttrain,
       q_1c_grad_projections_by_program.lcp4_cd,
       q_1c_grad_projections_by_program.lcip4_cred,
       q_1c_grad_projections_by_program.year,
       Sum(q_1c_grad_projections_by_program.grads)       AS Grads
INTO Q_2c3_Labour_Supply_Unknown
FROM   q_1c_grad_projections_by_program
       LEFT JOIN q_2c2_labour_supply_unknown_lcp2_proxy_union
              ON ( q_1c_grad_projections_by_program.age_group_rollup =
       q_2c2_labour_supply_unknown_lcp2_proxy_union.age_group_rollup )
       AND ( q_1c_grad_projections_by_program.lcip4_cred =
       q_2c2_labour_supply_unknown_lcp2_proxy_union.lcip4_cred )
WHERE  ( ( ( q_2c2_labour_supply_unknown_lcp2_proxy_union.lcip4_cred ) IS NULL )
         AND ( ( q_2c2_labour_supply_unknown_lcp2_proxy_union.age_group_rollup )
               IS
               NULL ) )
GROUP  BY q_1c_grad_projections_by_program.pssm_credential,
          q_1c_grad_projections_by_program.pssm_cred,
          q_1c_grad_projections_by_program.age_group_rollup,
          q_1c_grad_projections_by_program.age_group_rollup_label,
          q_1c_grad_projections_by_program.ttrain,
          q_1c_grad_projections_by_program.lcp4_cd,
          q_1c_grad_projections_by_program.lcip4_cred,
          q_1c_grad_projections_by_program.year;"
dbExecute(decimal_con, Q_2c3_Labour_Supply_Unknown) 

# [SELECT INTO] Create Q_2c4_Labour_Supply_Unknown_LCP2_Proxy_No_TT from labour_supply_distribution_lcp2_no_tt


# ---- Q_2c4_Labour_Supply_Unknown_LCP2_Proxy_No_TT ----
Q_2c4_Labour_Supply_Unknown_LCP2_Proxy_No_TT <- 
"SELECT q_2c3_labour_supply_unknown.pssm_credential,
       q_2c3_labour_supply_unknown.pssm_cred,
       q_2c3_labour_supply_unknown.age_group_rollup,
       q_2c3_labour_supply_unknown.age_group_rollup_label,
       q_2c3_labour_supply_unknown.year,
       q_2c3_labour_supply_unknown.ttrain,
       q_2c3_labour_supply_unknown.lcp4_cd,
       q_2c3_labour_supply_unknown.lcip4_cred,
       labour_supply_distribution_lcp2_no_tt.current_region_pssm_code_rollup,
       labour_supply_distribution_lcp2_no_tt.new_labour_supply,
       [grads] * [new_labour_supply] AS NLS
INTO   Q_2c4_Labour_Supply_Unknown_LCP2_Proxy_No_TT
FROM   labour_supply_distribution_lcp2_no_tt
       INNER JOIN ((t_exclude_from_labour_supply_unknown_lcp2_proxy
                    RIGHT JOIN q_2c3_labour_supply_unknown
                            ON
       t_exclude_from_labour_supply_unknown_lcp2_proxy.lcip_lcp4_cd =
       q_2c3_labour_supply_unknown.lcp4_cd)
                   INNER JOIN t_lcp2_lcp4
                           ON q_2c3_labour_supply_unknown.lcp4_cd =
                              t_lcp2_lcp4.lcip_lcp4_cd)
               ON ( t_lcp2_lcp4.lcip_lcp2_cd =
       labour_supply_distribution_lcp2_no_tt.lcp2_cd )
                  AND ( labour_supply_distribution_lcp2_no_tt.age_group_rollup =
                            q_2c3_labour_supply_unknown.age_group_rollup )
                  AND ( labour_supply_distribution_lcp2_no_tt.pssm_cred =
                            q_2c3_labour_supply_unknown.pssm_cred )
WHERE  (( ( t_exclude_from_labour_supply_unknown_lcp2_proxy.lcip_lcp4_cd ) IS
          NULL ))
        OR (( ( LEFT([lcip4_cred], 1) ) = 'P - ' ));"
dbExecute(decimal_con, Q_2c4_Labour_Supply_Unknown_LCP2_Proxy_No_TT) 

# [SELECT INTO] Create Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union from Q_2c2_Labour_Supply_Unknown_LCP2_Proxy_Union


# ---- Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union ----
Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union <- 
"SELECT Q_2c2_Labour_Supply_Unknown_LCP2_Proxy_Union.*
INTO Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union
FROM Q_2c2_Labour_Supply_Unknown_LCP2_Proxy_Union
UNION ALL Select Q_2c4_Labour_Supply_Unknown_LCP2_Proxy_No_TT.*
FROM Q_2c4_Labour_Supply_Unknown_LCP2_Proxy_No_TT;"
dbExecute(decimal_con, Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union) 

# [SELECT INTO] Create tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union_tmp from Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union


# ---- Q_2d2_Labour_Supply ----
Q_2d2_Labour_Supply <- 
"SELECT Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.* 
INTO tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union_tmp
FROM Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union;"
dbExecute(decimal_con, Q_2d2_Labour_Supply) 

# [SELECT INTO] Create Q_2d2_Labour_Supply_Unknown from q_1c_grad_projections_by_program


# ---- Q_2d2_Labour_Supply_Unknown ----
Q_2d2_Labour_Supply_Unknown <- 
"SELECT q_1c_grad_projections_by_program.pssm_credential,
       q_1c_grad_projections_by_program.pssm_cred,
       q_1c_grad_projections_by_program.age_group_rollup AS Age_Group_Rollup,
       q_1c_grad_projections_by_program.age_group_rollup_label,
       q_1c_grad_projections_by_program.ttrain,
       q_1c_grad_projections_by_program.lcp4_cd,
       q_1c_grad_projections_by_program.lcip4_cred       AS LCIP4_CRED,
       q_1c_grad_projections_by_program.year,
       Sum(q_1c_grad_projections_by_program.grads)       AS Grads
INTO   Q_2d2_Labour_Supply_Unknown 
FROM   q_1c_grad_projections_by_program
       LEFT JOIN tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union_tmp
              ON ( q_1c_grad_projections_by_program.age_group_rollup =
tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union_tmp.age_group_rollup )
AND ( q_1c_grad_projections_by_program.lcip4_cred =
          tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union_tmp.lcip4_cred )
WHERE  ( (
( tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union_tmp.lcip4_cred ) IS
NULL
         )
         AND
(
(
       tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union_tmp.age_group_rollup )
               IS
                   NULL ) )
GROUP  BY q_1c_grad_projections_by_program.pssm_credential,
          q_1c_grad_projections_by_program.pssm_cred,
          q_1c_grad_projections_by_program.age_group_rollup,
          q_1c_grad_projections_by_program.age_group_rollup_label,
          q_1c_grad_projections_by_program.ttrain,
          q_1c_grad_projections_by_program.lcp4_cd,
          q_1c_grad_projections_by_program.lcip4_cred,
          q_1c_grad_projections_by_program.year;"
dbExecute(decimal_con, Q_2d2_Labour_Supply_Unknown) 

# [SELECT INTO] Create Q_2d3_Labour_Supply_Unknown_LCP2_Private_Cred_Proxy from labour_supply_distribution_lcp2_no_tt

# ---- Q_2d3_Labour_Supply_Unknown_LCP2_Private_Cred_Proxy ----
Q_2d3_Labour_Supply_Unknown_LCP2_Private_Cred_Proxy <- 
"SELECT q_2d2_labour_supply_unknown.pssm_credential,
       q_2d2_labour_supply_unknown.pssm_cred,
       q_2d2_labour_supply_unknown.age_group_rollup,
       q_2d2_labour_supply_unknown.age_group_rollup_label,
       q_2d2_labour_supply_unknown.year,
       q_2d2_labour_supply_unknown.ttrain,
       q_2d2_labour_supply_unknown.lcp4_cd AS LCP4_CD,
       q_2d2_labour_supply_unknown.lcip4_cred,
       labour_supply_distribution_lcp2_no_tt.current_region_pssm_code_rollup,
       labour_supply_distribution_lcp2_no_tt.new_labour_supply,
       [grads] * [new_labour_supply]       AS NLS
INTO Q_2d3_Labour_Supply_Unknown_LCP2_Private_Cred_Proxy
FROM   labour_supply_distribution_lcp2_no_tt
       INNER JOIN (t_lcp2_lcp4
                   INNER JOIN q_2d2_labour_supply_unknown
                           ON t_lcp2_lcp4.lcip_lcp4_cd =
                              q_2d2_labour_supply_unknown.lcp4_cd)
               ON ( labour_supply_distribution_lcp2_no_tt.age_group_rollup =
                               q_2d2_labour_supply_unknown.age_group_rollup )
                  AND ( labour_supply_distribution_lcp2_no_tt.lcp2_cd =
                        t_lcp2_lcp4.lcip_lcp2_cd )
WHERE  ( ( ( q_2d2_labour_supply_unknown.pssm_cred ) LIKE 'P - CERT' )
         AND ( ( labour_supply_distribution_lcp2_no_tt.pssm_cred ) LIKE
               'P - DIPL' ) )
        OR ( ( ( q_2d2_labour_supply_unknown.pssm_cred ) LIKE 'P - DIPL' )
             AND ( ( labour_supply_distribution_lcp2_no_tt.pssm_cred ) LIKE
                   'P - CERT'
                 ) );"
dbExecute(decimal_con, Q_2d3_Labour_Supply_Unknown_LCP2_Private_Cred_Proxy) 

# [SELECT INTO] Create Q_2d4_Labour_Supply_by_LCIP4_CRED_LCP2_LCP2_Private_Union from tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union_tmp


# ---- Q_2d4_Labour_Supply_by_LCIP4_CRED_LCP2_LCP2_Private_Union ----
Q_2d4_Labour_Supply_by_LCIP4_CRED_LCP2_LCP2_Private_Union <- 
"SELECT tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union_tmp.*
INTO Q_2d4_Labour_Supply_by_LCIP4_CRED_LCP2_LCP2_Private_Union
FROM tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union_tmp
UNION ALL SELECT Q_2d3_Labour_Supply_Unknown_LCP2_Private_Cred_Proxy.*
FROM Q_2d3_Labour_Supply_Unknown_LCP2_Private_Cred_Proxy;"
dbExecute(decimal_con, Q_2d4_Labour_Supply_by_LCIP4_CRED_LCP2_LCP2_Private_Union) 

# [SELECT INTO] Create tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union from Q_2d4_Labour_Supply_by_LCIP4_CRED_LCP2_LCP2_Private_Union


# ---- Q_2f_Labour_Supply ----
Q_2f_Labour_Supply <- 
"SELECT Q_2d4_Labour_Supply_by_LCIP4_CRED_LCP2_LCP2_Private_Union.* 
INTO tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union
FROM Q_2d4_Labour_Supply_by_LCIP4_CRED_LCP2_LCP2_Private_Union;"
dbExecute(decimal_con, Q_2f_Labour_Supply) 

# [SELECT INTO] Create Q_2f_Labour_Supply from q_1c_grad_projections_by_program


# ---- Q_2f2_Labour_Supply_Unknown ----
Q_2f2_Labour_Supply_Unknown <- 
"SELECT q_1c_grad_projections_by_program.pssm_credential,
       q_1c_grad_projections_by_program.pssm_cred,
       q_1c_grad_projections_by_program.age_group_rollup,
       q_1c_grad_projections_by_program.age_group_rollup_label,
       q_1c_grad_projections_by_program.lcp4_cd,
       q_1c_grad_projections_by_program.lcip4_cred,
       q_1c_grad_projections_by_program.year,
       Sum(q_1c_grad_projections_by_program.grads) AS Grads
INTO   Q_2f_Labour_Supply
FROM   q_1c_grad_projections_by_program
       LEFT JOIN tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union
              ON ( q_1c_grad_projections_by_program.age_group_rollup =
tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.age_group_rollup )
AND ( q_1c_grad_projections_by_program.lcip4_cred =
tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.lcip4_cred )
WHERE  ( ( ( tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.lcip4_cred ) IS
           NULL )
         AND ( (
       tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.age_group_rollup )
               IS
               NULL ) )
GROUP  BY q_1c_grad_projections_by_program.pssm_credential,
          q_1c_grad_projections_by_program.pssm_cred,
          q_1c_grad_projections_by_program.age_group_rollup,
          q_1c_grad_projections_by_program.age_group_rollup_label,
          q_1c_grad_projections_by_program.lcp4_cd,
          q_1c_grad_projections_by_program.lcip4_cred,
          q_1c_grad_projections_by_program.year;"
dbExecute(decimal_con, Q_2f2_Labour_Supply_Unknown) # numbers are low

#dbExecute(decimal_con, "DROP TABLE Q_1c_Grad_Projections_by_Program")
dbExecute(decimal_con, "DROP TABLE Q_2_Labour_Supply_by_LCIP4_CRED")
dbExecute(decimal_con, "DROP TABLE Q_2a_Labour_Supply_Unknown")
dbExecute(decimal_con, "DROP TABLE Q_2a2_Labour_Supply_Unknown_No_TT_Proxy")
dbExecute(decimal_con, "DROP TABLE Q_2a3_Labour_Supply_by_LCIP4_CRED_No_TT_Proxy_Union")
dbExecute(decimal_con, "DROP TABLE Q_2b_Labour_Supply_Unknown")
dbExecute(decimal_con, "DROP TABLE Q_2b2_Labour_Supply_Unknown_Private_Cred_Proxy")
dbExecute(decimal_con, "DROP TABLE Q_2b3_Labour_Supply_by_LCIP4_CRED_Private_Cred_Proxy_Union")
dbExecute(decimal_con, "DROP TABLE Q_2b4_Labour_Supply_Unknown")
dbExecute(decimal_con, "DROP TABLE Q_2c_Labour_Supply_Unknown_LCP2_Proxy")
dbExecute(decimal_con, "DROP TABLE Q_2c2_Labour_Supply_Unknown_LCP2_Proxy_Union")
dbExecute(decimal_con, "DROP TABLE Q_2c3_Labour_Supply_Unknown")
dbExecute(decimal_con, "DROP TABLE Q_2c4_Labour_Supply_Unknown_LCP2_Proxy_No_TT")
dbExecute(decimal_con, "DROP TABLE Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union")
dbExecute(decimal_con, "DROP TABLE Q_2d2_Labour_Supply_Unknown")
dbExecute(decimal_con, "DROP TABLE Q_2d3_Labour_Supply_Unknown_LCP2_Private_Cred_Proxy")
dbExecute(decimal_con, "DROP TABLE Q_2d4_Labour_Supply_by_LCIP4_CRED_LCP2_LCP2_Private_Union")
dbExecute(decimal_con, "DROP TABLE Q_2f_Labour_Supply")
#dbExecute(decimal_con, "DROP TABLE tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union_tmp")
#dbExecute(decimal_con, "DROP TABLE tmp_tbl_Q_2a4_Labour_Supply_by_LCIP4_CRED_No_TT_Union_tmp")

# ---- Q_3 Series ---- 

# [SELECT INTO] Create Q_3_Occupations_by_LCIP4_CRED from tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union


# ---- Q_3_Occupations_by_LCIP4_CRED ----
Q_3_Occupations_by_LCIP4_CRED <- 
"SELECT tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.PSSM_Credential, 
        tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.PSSM_CRED, 
        tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.Age_Group_Rollup, 
        tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.Age_Group_Rollup_Label, 
        tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.Year, 
        tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.TTRAIN, 
        tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.LCP4_CD, 
        tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.LCIP4_CRED, 
        tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.Current_Region_PSSM_Code_Rollup, 
        Occupation_Distributions.NOC, Occupation_Distributions.[Percent], 
        [tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union].[NLS]*[Occupation_Distributions].[Percent] AS OccsN
INTO Q_3_Occupations_by_LCIP4_CRED
FROM tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union 
INNER JOIN Occupation_Distributions 
ON (tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.LCIP4_CRED = Occupation_Distributions.LCIP4_CRED) 
AND (tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.Current_Region_PSSM_Code_Rollup = Occupation_Distributions.Current_Region_PSSM_Code_Rollup) 
AND (tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.Age_Group_Rollup = Occupation_Distributions.Age_Group_Rollup);"
dbExecute(decimal_con, Q_3_Occupations_by_LCIP4_CRED) 

# [SELECT INTO] Create Q_3b_Occupations_Unknown from tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union


# ---- Q_3b_Occupations_Unknown ----
Q_3b_Occupations_Unknown <- 
"SELECT tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.pssm_credential,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.pssm_cred,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.age_group_rollup,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.age_group_rollup_label,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.ttrain,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.lcp4_cd,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.lcip4_cred,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.year,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.current_region_pssm_code_rollup,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.new_labour_supply,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.nls
INTO Q_3b_Occupations_Unknown
FROM   tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union
LEFT JOIN occupation_distributions
  ON( tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.age_group_rollup = occupation_distributions.age_group_rollup )
  AND(tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.current_region_pssm_code_rollup = occupation_distributions.current_region_pssm_code_rollup )
  AND ( tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.lcip4_cred = occupation_distributions.lcip4_cred )
WHERE  ( ( ( occupation_distributions.lcip4_cred ) IS NULL )
  AND ( ( occupation_distributions.current_region_pssm_code_rollup ) IS NULL )
  AND ( ( occupation_distributions.age_group_rollup ) IS NULL ) );"
dbExecute(decimal_con, Q_3b_Occupations_Unknown) # numbers too high

# [SELECT INTO] Create Q_3b11_Ocupations_Unknown_No_TT_Proxy from q_3b_occupations_unknown


# ---- Q_3b11_Ocupations_Unknown_No_TT_Proxy ----
Q_3b11_Ocupations_Unknown_No_TT_Proxy <- 
"SELECT q_3b_occupations_unknown.pssm_credential,
        q_3b_occupations_unknown.pssm_cred,
        q_3b_occupations_unknown.age_group_rollup,
        q_3b_occupations_unknown.age_group_rollup_label,
        q_3b_occupations_unknown.year,
        q_3b_occupations_unknown.ttrain,
        q_3b_occupations_unknown.lcp4_cd,
        q_3b_occupations_unknown.lcip4_cred,
        q_3b_occupations_unknown.current_region_pssm_code_rollup,
        occupation_distributions_no_tt.noc,
        occupation_distributions_no_tt.[percent],
        [nls] * [percent] AS OccsN
INTO Q_3b11_Ocupations_Unknown_No_TT_Proxy
FROM   q_3b_occupations_unknown
  INNER JOIN occupation_distributions_no_tt
    ON ( q_3b_occupations_unknown.current_region_pssm_code_rollup =  occupation_distributions_no_tt.current_region_pssm_code_rollup )
    AND ( q_3b_occupations_unknown.lcp4_cd = occupation_distributions_no_tt.lcp4_cd )
    AND ( q_3b_occupations_unknown.age_group_rollup = occupation_distributions_no_tt.age_group_rollup )
    AND ( q_3b_occupations_unknown.pssm_cred = occupation_distributions_no_tt.pssm_cred );"
dbExecute(decimal_con, Q_3b11_Ocupations_Unknown_No_TT_Proxy) # numbers too high

# [SELECT INTO] Create q_3b12_Occupations_by_LCIP4_CRED_No_TT_Proxy_Union from Q_3_Occupations_by_LCIP4_CRED


# ---- q_3b12_Occupations_by_LCIP4_CRED_No_TT_Proxy_Union ----
q_3b12_Occupations_by_LCIP4_CRED_No_TT_Proxy_Union <- 
"SELECT Q_3_Occupations_by_LCIP4_CRED.*
INTO q_3b12_Occupations_by_LCIP4_CRED_No_TT_Proxy_Union
FROM Q_3_Occupations_by_LCIP4_CRED
UNION ALL SELECT Q_3b11_Ocupations_Unknown_No_TT_Proxy.*
FROM Q_3b11_Ocupations_Unknown_No_TT_Proxy;"
dbExecute(decimal_con, q_3b12_Occupations_by_LCIP4_CRED_No_TT_Proxy_Union) 

# [SELECT INTO] Create tmp_tbl_Q3b12_Occupations_by_LCIP4_CRED_No_TT_Union_tmp from q_3b12_Occupations_by_LCIP4_CRED_No_TT_Proxy_Union


# ---- Q_3b13_Occupations ----
Q_3b13_Occupations <- 
"SELECT q_3b12_Occupations_by_LCIP4_CRED_No_TT_Proxy_Union.* 
INTO tmp_tbl_Q3b12_Occupations_by_LCIP4_CRED_No_TT_Union_tmp
FROM q_3b12_Occupations_by_LCIP4_CRED_No_TT_Proxy_Union;"
dbExecute(decimal_con, Q_3b13_Occupations) 

# [SELECT INTO] Create Q_3b14_Occupations_Unknown from tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union


# ---- Q_3b14_Occupations_Unknown ----
Q_3b14_Occupations_Unknown <- 
"SELECT tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.pssm_credential,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.pssm_cred,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.age_group_rollup,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.age_group_rollup_label,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.year,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.ttrain,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.lcp4_cd,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.lcip4_cred,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.current_region_pssm_code_rollup,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.new_labour_supply,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.nls
INTO Q_3b14_Occupations_Unknown
FROM   tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union
LEFT JOIN tmp_tbl_q3b12_occupations_by_lcip4_cred_no_tt_union_tmp
    ON  (tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.year = tmp_tbl_q3b12_occupations_by_lcip4_cred_no_tt_union_tmp.year )
    AND (tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.current_region_pssm_code_rollup = tmp_tbl_q3b12_occupations_by_lcip4_cred_no_tt_union_tmp.current_region_pssm_code_rollup )
    AND (tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.lcip4_cred = tmp_tbl_q3b12_occupations_by_lcip4_cred_no_tt_union_tmp.lcip4_cred )
    AND (tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.age_group_rollup = tmp_tbl_q3b12_occupations_by_lcip4_cred_no_tt_union_tmp.age_group_rollup )
WHERE  (((tmp_tbl_q3b12_occupations_by_lcip4_cred_no_tt_union_tmp.year ) IS NULL ) 
    AND ((tmp_tbl_q3b12_occupations_by_lcip4_cred_no_tt_union_tmp.age_group_rollup ) IS NULL )
    AND ((tmp_tbl_q3b12_occupations_by_lcip4_cred_no_tt_union_tmp.lcip4_cred ) IS NULL)
    AND ((tmp_tbl_q3b12_occupations_by_lcip4_cred_no_tt_union_tmp.current_region_pssm_code_rollup ) IS NULL ) );"
dbExecute(decimal_con, Q_3b14_Occupations_Unknown) 

# [SELECT INTO] Create Q_3b2_Occupations_Unknown_Private_Cred_Proxy


# ---- Q_3b2_Occupations_Unknown_Private_Cred_Proxy ----
Q_3b2_Occupations_Unknown_Private_Cred_Proxy <- 
"SELECT q_3b14_occupations_unknown.pssm_credential,
       q_3b14_occupations_unknown.pssm_cred,
       q_3b14_occupations_unknown.age_group_rollup,
       q_3b14_occupations_unknown.age_group_rollup_label,
       q_3b14_occupations_unknown.year,
       q_3b14_occupations_unknown.ttrain,
       q_3b14_occupations_unknown.lcp4_cd,
       q_3b14_occupations_unknown.lcip4_cred,
       q_3b14_occupations_unknown.current_region_pssm_code_rollup,
       occupation_distributions_no_tt.noc,
       occupation_distributions_no_tt.[percent],
       q_3b14_occupations_unknown.nls*occupation_distributions_no_tt.[percent] AS OccsN
INTO Q_3b2_Occupations_Unknown_Private_Cred_Proxy
FROM   (q_3b14_occupations_unknown
INNER JOIN tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union
  ON (q_3b14_occupations_unknown.lcip4_cred = tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.lcip4_cred )
  AND  (q_3b14_occupations_unknown.year =  tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.year )
  AND  (q_3b14_occupations_unknown.age_group_rollup =  tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.age_group_rollup )
  AND  (q_3b14_occupations_unknown.pssm_cred = tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.pssm_cred ))
INNER JOIN occupation_distributions_no_tt
  ON (tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.lcp4_cd = occupation_distributions_no_tt.lcp4_cd )
AND (tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.age_group_rollup = occupation_distributions_no_tt.age_group_rollup )
AND (tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.current_region_pssm_code_rollup = occupation_distributions_no_tt.current_region_pssm_code_rollup )
WHERE  ( ( ( q_3b14_occupations_unknown.pssm_cred ) LIKE 'P - CERT')
  AND ( ( occupation_distributions_no_tt.pssm_cred ) LIKE 'P - DIPL'))
  OR ( ( ( q_3b14_occupations_unknown.pssm_cred ) LIKE 'P - DIPL')
  AND ( ( occupation_distributions_no_tt.pssm_cred ) LIKE 'P - CERT'));"
dbExecute(decimal_con, Q_3b2_Occupations_Unknown_Private_Cred_Proxy) 

# [SELECT INTO] Create Q_3b3_Occupations_by_LCIP4_CRED_Private_Cred_Proxy_Union from tmp_tbl_Q3b12_Occupations_by_LCIP4_CRED_No_TT_Union_tmp


# ---- Q_3b3_Occupations_by_LCIP4_CRED_Private_Cred_Proxy_Union ----
Q_3b3_Occupations_by_LCIP4_CRED_Private_Cred_Proxy_Union <- 
"SELECT tmp_tbl_Q3b12_Occupations_by_LCIP4_CRED_No_TT_Union_tmp.*
INTO Q_3b3_Occupations_by_LCIP4_CRED_Private_Cred_Proxy_Union
FROM tmp_tbl_Q3b12_Occupations_by_LCIP4_CRED_No_TT_Union_tmp
UNION ALL SELECT Q_3b2_Occupations_Unknown_Private_Cred_Proxy.*
FROM Q_3b2_Occupations_Unknown_Private_Cred_Proxy;"
dbExecute(decimal_con, Q_3b3_Occupations_by_LCIP4_CRED_Private_Cred_Proxy_Union) 

# [SELECT INTO] Create Q_3b4_Occupations_Unknown from tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union


# ---- Q_3b4_Occupations_Unknown ----
Q_3b4_Occupations_Unknown <- 
"SELECT tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.pssm_credential,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.pssm_cred,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.age_group_rollup,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.age_group_rollup_label,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.year,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.ttrain,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.lcp4_cd,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.lcip4_cred,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.current_region_pssm_code_rollup,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.new_labour_supply,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.nls
INTO    Q_3b4_Occupations_Unknown
FROM    tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union
LEFT JOIN q_3b3_occupations_by_lcip4_cred_private_cred_proxy_union
ON ( tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.lcip4_cred = q_3b3_occupations_by_lcip4_cred_private_cred_proxy_union.lcip4_cred )
AND (tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.age_group_rollup = q_3b3_occupations_by_lcip4_cred_private_cred_proxy_union.age_group_rollup )
AND (tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.current_region_pssm_code_rollup = q_3b3_occupations_by_lcip4_cred_private_cred_proxy_union.current_region_pssm_code_rollup )
AND ( tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.year = q_3b3_occupations_by_lcip4_cred_private_cred_proxy_union.year )
WHERE  (((q_3b3_occupations_by_lcip4_cred_private_cred_proxy_union.age_group_rollup ) IS NULL)
 AND ((q_3b3_occupations_by_lcip4_cred_private_cred_proxy_union.year ) IS  NULL)
 AND ((q_3b3_occupations_by_lcip4_cred_private_cred_proxy_union.lcip4_cred )  IS NULL)
 AND ((q_3b3_occupations_by_lcip4_cred_private_cred_proxy_union.current_region_pssm_code_rollup ) IS NULL));"
dbExecute(decimal_con, Q_3b4_Occupations_Unknown) 


# [SELECT INTO] Create Q_3c_Occupations_Unknown_LCP2_Proxy

# ---- Q_3c_Occupations_Unknown_LCP2_Proxy ----
Q_3c_Occupations_Unknown_LCP2_Proxy <- 
"SELECT q_3b4_occupations_unknown.pssm_credential,
        q_3b4_occupations_unknown.pssm_cred,
        q_3b4_occupations_unknown.age_group_rollup,
        q_3b4_occupations_unknown.age_group_rollup_label,
        q_3b4_occupations_unknown.year,
        q_3b4_occupations_unknown.ttrain,
        q_3b4_occupations_unknown.lcp4_cd,
        q_3b4_occupations_unknown.lcip4_cred,
        q_3b4_occupations_unknown.current_region_pssm_code_rollup,
        occupation_distributions_lcp2.noc,
        occupation_distributions_lcp2.[percent],
        q_3b4_occupations_unknown.nls*occupation_distributions_lcp2.[percent] AS OccsN
INTO Q_3c_Occupations_Unknown_LCP2_Proxy
FROM   (q_3b4_occupations_unknown
INNER JOIN (t_lcp2_lcp4
INNER JOIN occupation_distributions_lcp2 ON t_lcp2_lcp4.lcip_lcp2_cd = occupation_distributions_lcp2.lcp2_cd)
ON (q_3b4_occupations_unknown.age_group_rollup = occupation_distributions_lcp2.age_group_rollup )
AND (q_3b4_occupations_unknown.current_region_pssm_code_rollup = occupation_distributions_lcp2.current_region_pssm_code_rollup )
AND ( q_3b4_occupations_unknown.pssm_cred =  occupation_distributions_lcp2.pssm_cred )
AND ( q_3b4_occupations_unknown.lcp4_cd = t_lcp2_lcp4.lcip_lcp4_cd ))
LEFT JOIN t_exclude_from_labour_supply_unknown_lcp2_proxy
 ON q_3b4_occupations_unknown.lcp4_cd = t_exclude_from_labour_supply_unknown_lcp2_proxy.lcip_lcp4_cd
WHERE  (( ( t_exclude_from_labour_supply_unknown_lcp2_proxy.lcip_lcp4_cd ) IS  NULL ))
 OR (( ( LEFT([lcip4_cred], 1) ) = 'P - ' ));"
dbExecute(decimal_con, Q_3c_Occupations_Unknown_LCP2_Proxy) 

# [SELECT INTO] Create Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union from tmp_tbl_Q3b12_Occupations_by_LCIP4_CRED_No_TT_Union_tmp


# ---- Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union ----
Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union <- 
"SELECT tmp_tbl_Q3b12_Occupations_by_LCIP4_CRED_No_TT_Union_tmp.*
INTO Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union
FROM tmp_tbl_Q3b12_Occupations_by_LCIP4_CRED_No_TT_Union_tmp
UNION ALL SELECT Q_3b2_Occupations_Unknown_Private_Cred_Proxy.*
FROM Q_3b2_Occupations_Unknown_Private_Cred_Proxy
UNION ALL SELECT Q_3c_Occupations_Unknown_LCP2_Proxy.*
FROM Q_3c_Occupations_Unknown_LCP2_Proxy;"
dbExecute(decimal_con, Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union) 

# [SELECT INTO] Create tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union_tmp from Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union


# ---- Q_3d2_Occupations ----
Q_3d2_Occupations <- 
"SELECT Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.* 
INTO tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union_tmp
FROM Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union;"
dbExecute(decimal_con, Q_3d2_Occupations) 

# [SELECT INTO] Create Q_3d2_Occupations_Unknown from tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union


# ---- Q_3d2_Occupations_Unknown ----
Q_3d2_Occupations_Unknown <- 
"SELECT tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.pssm_credential,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.pssm_cred,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.age_group_rollup,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.age_group_rollup_label,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.year,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.ttrain,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.lcp4_cd,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.lcip4_cred,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.current_region_pssm_code_rollup,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.new_labour_supply,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.nls
INTO Q_3d2_Occupations_Unknown
FROM   tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union
LEFT JOIN tmp_tbl_q_3d_occupations_by_lcip4_cred_lcp2_union_tmp
ON (tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.lcip4_cred =tmp_tbl_q_3d_occupations_by_lcip4_cred_lcp2_union_tmp.lcip4_cred )
AND (tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.current_region_pssm_code_rollup =tmp_tbl_q_3d_occupations_by_lcip4_cred_lcp2_union_tmp.current_region_pssm_code_rollup )
AND ( tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.year =tmp_tbl_q_3d_occupations_by_lcip4_cred_lcp2_union_tmp.year )
AND ( tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.age_group_rollup =tmp_tbl_q_3d_occupations_by_lcip4_cred_lcp2_union_tmp.age_group_rollup )
WHERE  ( ( (tmp_tbl_q_3d_occupations_by_lcip4_cred_lcp2_union_tmp.age_group_rollup ) IS NULL )
AND ( ( tmp_tbl_q_3d_occupations_by_lcip4_cred_lcp2_union_tmp.year ) IS NULL )
AND ( ( tmp_tbl_q_3d_occupations_by_lcip4_cred_lcp2_union_tmp.lcip4_cred ) IS NULL)
AND ((tmp_tbl_q_3d_occupations_by_lcip4_cred_lcp2_union_tmp.current_region_pssm_code_rollup ) IS NULL ) );"
dbExecute(decimal_con, Q_3d2_Occupations_Unknown) 

# [SELECT INTO] Create Q_3d21_Occupations_Unknown_LCP2_Proxy_No_TT


# ---- Q_3d21_Occupations_Unknown_LCP2_Proxy_No_TT ----
Q_3d21_Occupations_Unknown_LCP2_Proxy_No_TT <- 
"SELECT q_3d2_occupations_unknown.pssm_credential,
       q_3d2_occupations_unknown.pssm_cred,
       q_3d2_occupations_unknown.age_group_rollup,
       q_3d2_occupations_unknown.age_group_rollup_label,
       q_3d2_occupations_unknown.year,
       q_3d2_occupations_unknown.ttrain,
       q_3d2_occupations_unknown.lcp4_cd,
       q_3d2_occupations_unknown.lcip4_cred,
       q_3d2_occupations_unknown.current_region_pssm_code_rollup,
       occupation_distributions_lcp2_no_tt.noc,
       occupation_distributions_lcp2_no_tt.[percent],
       nls*[percent] AS OccsN
INTO Q_3d21_Occupations_Unknown_LCP2_Proxy_No_TT
FROM   (t_lcp2_lcp4
INNER JOIN occupation_distributions_lcp2_no_tt
ON t_lcp2_lcp4.lcip_lcp2_cd =occupation_distributions_lcp2_no_tt.lcp2_cd)
INNER JOIN (q_3d2_occupations_unknown
LEFT JOIN t_exclude_from_labour_supply_unknown_lcp2_proxy
ON q_3d2_occupations_unknown.lcp4_cd =t_exclude_from_labour_supply_unknown_lcp2_proxy.lcip_lcp4_cd)
ON ( t_lcp2_lcp4.lcip_lcp4_cd =q_3d2_occupations_unknown.lcp4_cd )
AND ( occupation_distributions_lcp2_no_tt.pssm_cred = q_3d2_occupations_unknown.pssm_cred )
AND (occupation_distributions_lcp2_no_tt.current_region_pssm_code_rollup = q_3d2_occupations_unknown.current_region_pssm_code_rollup )
AND ( occupation_distributions_lcp2_no_tt.age_group_rollup = q_3d2_occupations_unknown.age_group_rollup )
WHERE  (( ( t_exclude_from_labour_supply_unknown_lcp2_proxy.lcip_lcp4_cd ) IS NULL ))
 OR (( ( LEFT([lcip4_cred], 1) ) = 'P - ' ));"
dbExecute(decimal_con, Q_3d21_Occupations_Unknown_LCP2_Proxy_No_TT) 

# [SELECT INTO] Create Q_3d22_Occupations_by_LCIP4_CRED_LCP2_No_T_Proxy_Union from tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union_tmp


# ---- Q_3d22_Occupations_by_LCIP4_CRED_LCP2_No_T_Proxy_Union ----
Q_3d22_Occupations_by_LCIP4_CRED_LCP2_No_T_Proxy_Union <- 
"SELECT tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union_tmp.*
INTO Q_3d22_Occupations_by_LCIP4_CRED_LCP2_No_T_Proxy_Union
FROM tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union_tmp
UNION ALL SELECT Q_3d21_Occupations_Unknown_LCP2_Proxy_No_TT.*
FROM Q_3d21_Occupations_Unknown_LCP2_Proxy_No_TT;"
dbExecute(decimal_con, Q_3d22_Occupations_by_LCIP4_CRED_LCP2_No_T_Proxy_Union) 

# [SELECT INTO] Create Q_3d24_Occupations_Unknown from tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union


# ---- Q_3d24_Occupations_Unknown ----
Q_3d24_Occupations_Unknown <- 
"SELECT tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.pssm_credential,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.pssm_cred,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.age_group_rollup,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.age_group_rollup_label,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.year,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.ttrain,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.lcp4_cd,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.lcip4_cred,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.current_region_pssm_code_rollup,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.new_labour_supply,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.nls
INTO Q_3d24_Occupations_Unknown
FROM    tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union
LEFT JOIN q_3d22_occupations_by_lcip4_cred_lcp2_no_t_proxy_union
       ON (tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.current_region_pssm_code_rollup =q_3d22_occupations_by_lcip4_cred_lcp2_no_t_proxy_union.current_region_pssm_code_rollup )
AND ( tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.lcip4_cred = q_3d22_occupations_by_lcip4_cred_lcp2_no_t_proxy_union.lcip4_cred )
AND ( tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.year = q_3d22_occupations_by_lcip4_cred_lcp2_no_t_proxy_union.year )
AND ( tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.age_group_rollup =q_3d22_occupations_by_lcip4_cred_lcp2_no_t_proxy_union.age_group_rollup )
WHERE  ( ( (q_3d22_occupations_by_lcip4_cred_lcp2_no_t_proxy_union.age_group_rollup )IS NULL )
AND ( ( q_3d22_occupations_by_lcip4_cred_lcp2_no_t_proxy_union.year )IS NULL)
AND ( ( q_3d22_occupations_by_lcip4_cred_lcp2_no_t_proxy_union.lcip4_cred ) IS NULL)
AND ((q_3d22_occupations_by_lcip4_cred_lcp2_no_t_proxy_union.current_region_pssm_code_rollup ) IS NULL ) );"
dbExecute(decimal_con, Q_3d24_Occupations_Unknown) 

# [SELECT INTO] Create Q_3d3_Occupations_Unknown_LCP2_Private_Cred_Proxy

# ---- Q_3d3_Occupations_Unknown_LCP2_Private_Cred_Proxy ----
Q_3d3_Occupations_Unknown_LCP2_Private_Cred_Proxy <- 
"SELECT q_3d24_occupations_unknown.pssm_credential,
       q_3d24_occupations_unknown.pssm_cred,
       q_3d24_occupations_unknown.age_group_rollup,
       q_3d24_occupations_unknown.age_group_rollup_label,
       q_3d24_occupations_unknown.year,
       q_3d24_occupations_unknown.ttrain,
       q_3d24_occupations_unknown.lcp4_cd,
       q_3d24_occupations_unknown.lcip4_cred,
       q_3d24_occupations_unknown.current_region_pssm_code_rollup,
       occupation_distributions_lcp2_no_tt.noc,
       occupation_distributions_lcp2_no_tt.[percent],
       nls*[percent] AS OccsN
INTO Q_3d3_Occupations_Unknown_LCP2_Private_Cred_Proxy
FROM   (q_3d24_occupations_unknown
        INNER JOIN t_lcp2_lcp4
                ON q_3d24_occupations_unknown.lcp4_cd =
       t_lcp2_lcp4.lcip_lcp4_cd)
       INNER JOIN occupation_distributions_lcp2_no_tt
               ON ( q_3d24_occupations_unknown.current_region_pssm_code_rollup =occupation_distributions_lcp2_no_tt.current_region_pssm_code_rollup )
AND ( q_3d24_occupations_unknown.age_group_rollup =occupation_distributions_lcp2_no_tt.age_group_rollup )
AND ( t_lcp2_lcp4.lcip_lcp2_cd =occupation_distributions_lcp2_no_tt.lcp2_cd )
WHERE  ( ( ( q_3d24_occupations_unknown.pssm_cred ) LIKE 'P - CERT' )
AND ( ( occupation_distributions_lcp2_no_tt.pssm_cred ) LIKE 'P - DIPL') )
OR ( ( ( q_3d24_occupations_unknown.pssm_cred ) LIKE 'P - DIPL' )
AND ( ( occupation_distributions_lcp2_no_tt.pssm_cred ) LIKE'P - CERT' ) );"
dbExecute(decimal_con, Q_3d3_Occupations_Unknown_LCP2_Private_Cred_Proxy) 

# [SELECT INTO] Create Q_3d4_Occupations_by_LCIP4_CRED_LCP2_LCP2_Private_Union from tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union_tmp

# ---- Q_3d4_Occupations_by_LCIP4_CRED_LCP2_LCP2_Private_Union ----
Q_3d4_Occupations_by_LCIP4_CRED_LCP2_LCP2_Private_Union <- 
"SELECT tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union_tmp.*
INTO Q_3d4_Occupations_by_LCIP4_CRED_LCP2_LCP2_Private_Union
FROM tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union_tmp
UNION ALL SELECT Q_3d21_Occupations_Unknown_LCP2_Proxy_No_TT.*
FROM Q_3d21_Occupations_Unknown_LCP2_Proxy_No_TT
UNION ALL SELECT Q_3d3_Occupations_Unknown_LCP2_Private_Cred_Proxy.*
FROM Q_3d3_Occupations_Unknown_LCP2_Private_Cred_Proxy;"
dbExecute(decimal_con, Q_3d4_Occupations_by_LCIP4_CRED_LCP2_LCP2_Private_Union) 

# [SELECT INTO] Create Q_3e_Occupations_Unknown from tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union


# ---- Q_3e_Occupations_Unknown ----
Q_3e_Occupations_Unknown <- 
"SELECT tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.pssm_credential,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.pssm_cred,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.age_group_rollup,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.age_group_rollup_label,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.year,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.ttrain,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.lcp4_cd,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.lcip4_cred,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.current_region_pssm_code_rollup,
        Sum(tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.nls) AS NLS
INTO Q_3e_Occupations_Unknown 
FROM    tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union
LEFT JOIN q_3d4_occupations_by_lcip4_cred_lcp2_lcp2_private_union
  ON ( tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.lcip4_cred = q_3d4_occupations_by_lcip4_cred_lcp2_lcp2_private_union.lcip4_cred )
  AND (tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.current_region_pssm_code_rollup =q_3d4_occupations_by_lcip4_cred_lcp2_lcp2_private_union.current_region_pssm_code_rollup )
  AND ( tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.age_group_rollup =q_3d4_occupations_by_lcip4_cred_lcp2_lcp2_private_union.age_group_rollup )
  AND ( tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.year =q_3d4_occupations_by_lcip4_cred_lcp2_lcp2_private_union.year )
WHERE  ( ( (q_3d4_occupations_by_lcip4_cred_lcp2_lcp2_private_union.age_group_rollup ) IS NULL )
  AND ( ( q_3d4_occupations_by_lcip4_cred_lcp2_lcp2_private_union.year ) IS NULL )
  AND ( (q_3d4_occupations_by_lcip4_cred_lcp2_lcp2_private_union.lcip4_cred ) IS NULL)
 AND ((q_3d4_occupations_by_lcip4_cred_lcp2_lcp2_private_union.current_region_pssm_code_rollup ) IS NULL ) )
GROUP BY tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.pssm_credential,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.pssm_cred,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.age_group_rollup,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.age_group_rollup_label,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.year,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.ttrain,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.lcp4_cd,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.lcip4_cred,
        tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.current_region_pssm_code_rollup;"
dbExecute(decimal_con, Q_3e_Occupations_Unknown) 

# [SELECT INTO] Create Q_3e2_Occupations_Unknown from tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union

# ---- Q_3e2_Occupations_Unknown ----
Q_3e2_Occupations_Unknown <- 
"SELECT tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.pssm_credential,
       tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.pssm_cred,
       tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.age_group_rollup,
       tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.age_group_rollup_label,
       tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.year,
       tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.ttrain,
       tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.lcp4_cd,
       tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.lcip4_cred,
       tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.current_region_pssm_code_rollup,
       99999  AS NOC,
       1 AS [Percent],
       Sum(tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.nls) AS OccsN
INTO Q_3e2_Occupations_Unknown 
FROM   tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union
LEFT JOIN q_3d4_occupations_by_lcip4_cred_lcp2_lcp2_private_union
   ON ( tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.lcip4_cred =q_3d4_occupations_by_lcip4_cred_lcp2_lcp2_private_union.lcip4_cred )
  AND (tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.current_region_pssm_code_rollup =q_3d4_occupations_by_lcip4_cred_lcp2_lcp2_private_union.current_region_pssm_code_rollup )
  AND ( tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.age_group_rollup =q_3d4_occupations_by_lcip4_cred_lcp2_lcp2_private_union.age_group_rollup )
  AND ( tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.year =q_3d4_occupations_by_lcip4_cred_lcp2_lcp2_private_union.year )
WHERE  ( ( (q_3d4_occupations_by_lcip4_cred_lcp2_lcp2_private_union.age_group_rollup )IS NULL )
  AND ( ( q_3d4_occupations_by_lcip4_cred_lcp2_lcp2_private_union.year )IS NULL)
  AND ( (q_3d4_occupations_by_lcip4_cred_lcp2_lcp2_private_union.lcip4_cred ) IS NULL)
  AND ((q_3d4_occupations_by_lcip4_cred_lcp2_lcp2_private_union.current_region_pssm_code_rollup ) IS NULL ) )
GROUP  BY tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.pssm_credential,
          tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.pssm_cred,
          tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.age_group_rollup,
          tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.age_group_rollup_label,
          tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.year,
          tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.ttrain,
          tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.lcp4_cd,
          tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.lcip4_cred,
          tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.current_region_pssm_code_rollup
HAVING  (( ( Sum(tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.nls) ) > 0 ));"
dbExecute(decimal_con, Q_3e2_Occupations_Unknown) 

# [SELECT INTO] Create Q_3e3_Occupations_by_LCIP4_CRED_LCP2_Union from Q_3d4_Occupations_by_LCIP4_CRED_LCP2_LCP2_Private_Union


# ---- Q_3e3_Occupations_by_LCIP4_CRED_LCP2_Union ----
Q_3e3_Occupations_by_LCIP4_CRED_LCP2_Union <- 
"SELECT Q_3d4_Occupations_by_LCIP4_CRED_LCP2_LCP2_Private_Union.*
INTO Q_3e3_Occupations_by_LCIP4_CRED_LCP2_Union
FROM Q_3d4_Occupations_by_LCIP4_CRED_LCP2_LCP2_Private_Union
UNION ALL SELECT Q_3e2_Occupations_Unknown.*
FROM Q_3e2_Occupations_Unknown;"
dbExecute(decimal_con, Q_3e3_Occupations_by_LCIP4_CRED_LCP2_Union)

# [SELECT INTO] Create tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union from Q_3e3_Occupations_by_LCIP4_CRED_LCP2_Union


# ---- Q_3f_Occupations ----
Q_3f_Occupations <- 
"SELECT Q_3e3_Occupations_by_LCIP4_CRED_LCP2_Union.PSSM_Credential, 
        Q_3e3_Occupations_by_LCIP4_CRED_LCP2_Union.PSSM_CRED, 
        Q_3e3_Occupations_by_LCIP4_CRED_LCP2_Union.Age_Group_Rollup, 
        Q_3e3_Occupations_by_LCIP4_CRED_LCP2_Union.Age_Group_Rollup_Label, 
        Q_3e3_Occupations_by_LCIP4_CRED_LCP2_Union.Year, 
        Q_3e3_Occupations_by_LCIP4_CRED_LCP2_Union.TTRAIN, 
        Q_3e3_Occupations_by_LCIP4_CRED_LCP2_Union.LCP4_CD, 
        Q_3e3_Occupations_by_LCIP4_CRED_LCP2_Union.LCIP4_CRED, 
        Q_3e3_Occupations_by_LCIP4_CRED_LCP2_Union.Current_Region_PSSM_Code_Rollup, 
        Q_3e3_Occupations_by_LCIP4_CRED_LCP2_Union.NOC, 
        Q_3e3_Occupations_by_LCIP4_CRED_LCP2_Union.[percent], 
        Q_3e3_Occupations_by_LCIP4_CRED_LCP2_Union.OccsN 
INTO    tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union
FROM    Q_3e3_Occupations_by_LCIP4_CRED_LCP2_Union
WHERE (((Q_3e3_Occupations_by_LCIP4_CRED_LCP2_Union.OccsN)>0));"
dbExecute(decimal_con, Q_3f_Occupations) 

dbExecute(decimal_con, "DROP TABLE Q_3_Occupations_by_LCIP4_CRED")
dbExecute(decimal_con, "DROP TABLE Q_3b_Occupations_Unknown")
dbExecute(decimal_con, "DROP TABLE Q_3b11_Ocupations_Unknown_No_TT_Proxy")
dbExecute(decimal_con, "DROP TABLE q_3b12_Occupations_by_LCIP4_CRED_No_TT_Proxy_Union")
dbExecute(decimal_con, "DROP TABLE Q_3b14_Occupations_Unknown")
dbExecute(decimal_con, "DROP TABLE Q_3b2_Occupations_Unknown_Private_Cred_Proxy")
dbExecute(decimal_con, "DROP TABLE Q_3b3_Occupations_by_LCIP4_CRED_Private_Cred_Proxy_Union")
dbExecute(decimal_con, "DROP TABLE Q_3b4_Occupations_Unknown")
dbExecute(decimal_con, "DROP TABLE Q_3c_Occupations_Unknown_LCP2_Proxy")
dbExecute(decimal_con, "DROP TABLE Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union")
dbExecute(decimal_con, "DROP TABLE Q_3d2_Occupations_Unknown")
dbExecute(decimal_con, "DROP TABLE Q_3d21_Occupations_Unknown_LCP2_Proxy_No_TT")
dbExecute(decimal_con, "DROP TABLE Q_3d22_Occupations_by_LCIP4_CRED_LCP2_No_T_Proxy_Union")
dbExecute(decimal_con, "DROP TABLE Q_3d24_Occupations_Unknown")
dbExecute(decimal_con, "DROP TABLE Q_3d3_Occupations_Unknown_LCP2_Private_Cred_Proxy")
dbExecute(decimal_con, "DROP TABLE Q_3d4_Occupations_by_LCIP4_CRED_LCP2_LCP2_Private_Union")
dbExecute(decimal_con, "DROP TABLE Q_3e_Occupations_Unknown")
dbExecute(decimal_con, "DROP TABLE Q_3e2_Occupations_Unknown")
dbExecute(decimal_con, "DROP TABLE Q_3e3_Occupations_by_LCIP4_CRED_LCP2_Union")
#dbExecute(decimal_con, "DROP TABLE tmp_tbl_Q3b12_Occupations_by_LCIP4_CRED_No_TT_Union_tmp")
#dbExecute(decimal_con, "DROP TABLE tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union")

# ---- Q_4_NOC_D Series ---- 

# [SELECT INTO] Create Q_4_NOC_1D_Totals_by_PSSM_CRED from tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union


# ---- Q_4_NOC_1D_Totals_by_PSSM_CRED ----
Q_4_NOC_1D_Totals_by_PSSM_CRED <- "
SELECT NOC_Level, NOC, ENGLISH_NAME,
	 [1 - ADCT or ADIP],[1 - ADGR or UT],[1 - CERT],[1 - DIPL],[1 - PDCT or PDDP],
	 [3 - ADCT or ADIP],[3 - ADGR or UT],[3 - CERT],[3 - DIPL],[3 - PDCT or PDDP],
	 [APPRAPPR],[APPRCERT],[BACH],[DOCT],[GRCT or GRDP],[MAST],[P - CERT],[P - DIPL],[PDEG]
	 
INTO Q_4_NOC_1D_Totals_by_PSSM_CRED

FROM (
	SELECT 
	  tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.OccsN, 
		Len(T_NOC_Broad_Categories.BROAD_CATEGORY_CODE) AS NOC_Level, 
		 T_NOC_Broad_Categories.BROAD_CATEGORY_CODE AS NOC, 
		 T_NOC_Broad_Categories.BROAD_CATEGORY_ENGLISH_NAME AS ENGLISH_NAME, 
		PSSM_CRED
	FROM tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union 
	INNER JOIN T_NOC_Broad_Categories 
	  ON tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.NOC =  T_NOC_Broad_Categories.UNIT_GROUP_CODE
) AS SourceTable
PIVOT (
    Sum(OccsN)
	FOR PSSM_CRED IN
    ([1 - ADCT or ADIP],[1 - ADGR or UT],[1 - CERT],[1 - DIPL],[1 - PDCT or PDDP],
	 [3 - ADCT or ADIP],[3 - ADGR or UT],[3 - CERT],[3 - DIPL],[3 - PDCT or PDDP],
	 [APPRAPPR],[APPRCERT],[BACH],[DOCT],[GRCT or GRDP],[MAST],[P - CERT],[P - DIPL],[PDEG])
) AS PivotTable
ORDER BY NOC;"
dbExecute(decimal_con, Q_4_NOC_1D_Totals_by_PSSM_CRED) 

# [SELECT INTO] Create Q_4_NOC_1D_Totals_by_Year from T_Current_Region_PSSM_Rollup_Codes


# ---- Q_4_NOC_1D_Totals_by_Year ----
Q_4_NOC_1D_Totals_by_Year <- "
SELECT Expr1, Age_Group_Rollup_Label, NOC_Level, NOC,  ENGLISH_NAME,  
Current_Region_PSSM_Code_Rollup,  Current_Region_PSSM_Name_Rollup,
	 [2023/2024], [2024/2025], [2025/2026], [2026/2027], [2027/2028], [2028/2029], 
	 [2029/2030], [2030/2031], [2031/2032], [2032/2033], [2033/2034],[2034/2035]
	 
INTO Q_4_NOC_1D_Totals_by_Year

FROM (
		SELECT 
		  Year, 
			OccsN,
			CONCAT([Age_Group_Rollup_Label], '-' , BROAD_CATEGORY_CODE , '-'
				, CAST([T_Current_Region_PSSM_Rollup_Codes].[Current_Region_PSSM_Code_Rollup] AS NVARCHAR(50))) AS Expr1, 
			tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.Age_Group_Rollup_Label, 
			Len(T_NOC_Broad_Categories.BROAD_CATEGORY_CODE) AS NOC_Level, 
			CAST(T_NOC_Broad_Categories.BROAD_CATEGORY_CODE AS NVARCHAR(50)) AS NOC, 
			 T_NOC_Broad_Categories.BROAD_CATEGORY_ENGLISH_NAME AS ENGLISH_NAME, 
			T_Current_Region_PSSM_Rollup_Codes.Current_Region_PSSM_Code_Rollup, 
			T_Current_Region_PSSM_Rollup_Codes.Current_Region_PSSM_Name_Rollup
		FROM T_Current_Region_PSSM_Rollup_Codes 
		INNER JOIN tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union 
			ON T_Current_Region_PSSM_Rollup_Codes.Current_Region_PSSM_Code_Rollup = tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.Current_Region_PSSM_Code_Rollup
		INNER JOIN T_NOC_Broad_Categories 
		ON tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.NOC =  T_NOC_Broad_Categories.UNIT_GROUP_CODE
) AS SourceTable

PIVOT (
    Sum(OccsN)
	FOR Year IN
    ([2023/2024], [2024/2025], [2025/2026], [2026/2027], [2027/2028], [2028/2029], [2029/2030], 
    [2030/2031], [2031/2032], [2032/2033], [2033/2034],[2034/2035])
) AS PivotTable

ORDER BY  Expr1, Age_Group_Rollup_Label, NOC_Level,  NOC,  ENGLISH_NAME,  Current_Region_PSSM_Code_Rollup;"
dbExecute(decimal_con, Q_4_NOC_1D_Totals_by_Year)                                                                                                                                    

# [SELECT INTO] Create Q_4_NOC_2D_Totals_by_PSSM_CRED from tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union

# ---- Q_4_NOC_2D_Totals_by_PSSM_CRED ----
Q_4_NOC_2D_Totals_by_PSSM_CRED <- "
SELECT NOC_Level, NOC, ENGLISH_NAME,
	[1 - ADCT or ADIP],[1 - ADGR or UT],[1 - CERT],[1 - DIPL],[1 - PDCT or PDDP],[3 - ADCT or ADIP],
	[3 - ADGR or UT],[3 - CERT],[3 - DIPL],[3 - PDCT or PDDP],[APPRAPPR],[APPRCERT],[BACH],
	[DOCT],[GRCT or GRDP],[MAST],[P - CERT],[P - DIPL],[PDEG]
	
INTO Q_4_NOC_2D_Totals_by_PSSM_CRED

FROM (
SELECT	PSSM_CRED,
		tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.OccsN,
		Len(MAJOR_GROUP_CODE) AS NOC_Level, 
		T_NOC_Broad_Categories.MAJOR_GROUP_CODE AS NOC, 
		T_NOC_Broad_Categories.MAJOR_GROUP_ENGLISH_NAME AS ENGLISH_NAME
	FROM tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union 
	INNER JOIN T_NOC_Broad_Categories 
	ON tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.NOC = T_NOC_Broad_Categories.UNIT_GROUP_CODE
) AS SourceTable

PIVOT (
    Sum(OccsN)
	FOR PSSM_CRED IN
    ([1 - ADCT or ADIP],[1 - ADGR or UT],[1 - CERT],[1 - DIPL],[1 - PDCT or PDDP],[3 - ADCT or ADIP],
	[3 - ADGR or UT],[3 - CERT],[3 - DIPL],[3 - PDCT or PDDP],[APPRAPPR],[APPRCERT],[BACH],
	[DOCT],[GRCT or GRDP],[MAST],[P - CERT],[P - DIPL],[PDEG])
) AS PivotTable

ORDER BY NOC;"
dbExecute(decimal_con, Q_4_NOC_2D_Totals_by_PSSM_CRED) 

# [SELECT INTO] Create Q_4_NOC_2D_Totals_by_PSSM_CRED_Appendix from T_PSSM_CRED_RECODE


# ---- Q_4_NOC_2D_Totals_by_PSSM_CRED_Appendix ----
Q_4_NOC_2D_Totals_by_PSSM_CRED_Appendix <- "
SELECT NOC_Level, NOC, Expr1, 
[1 - ADCT or ADIP],[1 - ADGR or UT],[1 - CERT],[1 - DIPL],[1 - PDCT or PDDP],
[3 - ADCT or ADIP],[3 - ADGR or UT],[3 - CERT],[3 - DIPL],[3 - GRCT or GRDP],
[3 - PDCT or PDDP],[APPRAPPR],[APPRCERT],[BACH],[DOCT],[GRCT or GRDP],[MAST],[PDEG]

INTO Q_4_NOC_2D_Totals_by_PSSM_CRED_Appendix

FROM (
	SELECT T_PSSM_CRED_RECODE.PSSM_CRED, 
	  tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.OccsN,
	  Len(MAJOR_GROUP_CODE) AS NOC_Level, 
	  T_NOC_Broad_Categories.MAJOR_GROUP_CODE AS NOC, 
	  CONCAT(MAJOR_GROUP_CODE , ' ' , MAJOR_GROUP_ENGLISH_NAME) AS Expr1
	FROM T_PSSM_CRED_RECODE 
	INNER JOIN (tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union 
	INNER JOIN  T_NOC_Broad_Categories 
	  ON tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.NOC =  T_NOC_Broad_Categories.UNIT_GROUP_CODE) 
	  ON T_PSSM_CRED_RECODE.PSSM_CRED = tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.PSSM_CRED
	WHERE T_PSSM_CRED_RECODE.PSSM_CRED_Group In ('APPRAPPR','APPRCERT','CERT','DIPL','ADGR Or UT','ADCT or ADIP','BACH','PDCT or PDDP','MAST','DOCT')
) AS SourceTable

PIVOT (
    Sum(OccsN)
	FOR PSSM_CRED IN
    ([1 - ADCT or ADIP],[1 - ADGR or UT],[1 - CERT],[1 - DIPL],[1 - PDCT or PDDP],
	[3 - ADCT or ADIP],[3 - ADGR or UT],[3 - CERT],[3 - DIPL],[3 - GRCT or GRDP],
	[3 - PDCT or PDDP],[APPRAPPR],[APPRCERT],[BACH],[DOCT],[GRCT or GRDP],[MAST],[PDEG])
) AS PivotTable

ORDER BY  NOC_LEVEL, NOC;"
dbExecute(decimal_con, Q_4_NOC_2D_Totals_by_PSSM_CRED_Appendix)                                    

# [SELECT INTO] Create Q_4_NOC_2D_Totals_by_Year


# ---- Q_4_NOC_2D_Totals_by_Year ----
Q_4_NOC_2D_Totals_by_Year <- 
"SELECT 	Expr1, Age_Group_Rollup_Label, NOC_Level, NOC, ENGLISH_NAME, Current_Region_PSSM_Code_Rollup, Current_Region_PSSM_Name_Rollup,
	 [2023/2024], [2024/2025], [2025/2026], [2026/2027], [2027/2028], [2028/2029], 
	 [2029/2030], [2030/2031], [2031/2032], [2032/2033], [2033/2034],[2034/2035]
	 
INTO Q_4_NOC_2D_Totals_by_Year

FROM (
	SELECT Year, tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.OccsN, 
	CONCAT([Age_Group_Rollup_Label] , '-' , [T_NOC_Broad_Categories].[MAJOR_GROUP_CODE] , '-' 
		 , CAST([T_Current_Region_PSSM_Rollup_Codes].[Current_Region_PSSM_Code_Rollup] AS NVARCHAR(50))) AS Expr1, 
	tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.Age_Group_Rollup_Label, 
	Len(MAJOR_GROUP_CODE) AS NOC_Level, 
	 T_NOC_Broad_Categories.MAJOR_GROUP_CODE AS NOC, 
	 T_NOC_Broad_Categories.MAJOR_GROUP_ENGLISH_NAME AS ENGLISH_NAME, 
	T_Current_Region_PSSM_Rollup_Codes.Current_Region_PSSM_Code_Rollup, 
	T_Current_Region_PSSM_Rollup_Codes.Current_Region_PSSM_Name_Rollup
	FROM (T_Current_Region_PSSM_Rollup_Codes 
		INNER JOIN tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union 
			ON T_Current_Region_PSSM_Rollup_Codes.Current_Region_PSSM_Code_Rollup = tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.Current_Region_PSSM_Code_Rollup) 
		INNER JOIN  T_NOC_Broad_Categories 
		ON tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.NOC =  T_NOC_Broad_Categories.UNIT_GROUP_CODE
) AS SourceTable

PIVOT (
    Sum(OccsN)
	FOR Year IN
    ([2023/2024], [2024/2025], [2025/2026], [2026/2027], [2027/2028], [2028/2029], 
    [2029/2030], [2030/2031], [2031/2032], [2032/2033], [2033/2034],[2034/2035])
) AS PivotTable

ORDER BY Age_Group_Rollup_Label,  ENGLISH_NAME,  Current_Region_PSSM_Name_Rollup;"
dbExecute(decimal_con, Q_4_NOC_2D_Totals_by_Year) 

# [SELECT INTO] Create Q_4_NOC_3D_Totals_by_PSSM_CRED from tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union


# ---- Q_4_NOC_3D_Totals_by_PSSM_CRED ----
Q_4_NOC_3D_Totals_by_PSSM_CRED <- "
SELECT 	NOC_Level, NOC, ENGLISH_NAME,
		[1 - ADCT or ADIP],[1 - ADGR or UT],[1 - CERT],[1 - DIPL],[1 - PDCT or PDDP],[3 - ADCT or ADIP],
		[3 - ADGR or UT],[3 - CERT],[3 - DIPL],[3 - PDCT or PDDP],[APPRAPPR],[APPRCERT],[BACH],[DOCT],
		[GRCT or GRDP],[MAST],[P - CERT],[P - DIPL],[PDEG]
		
INTO Q_4_NOC_3D_Totals_by_PSSM_CRED

FROM (
	SELECT PSSM_CRED, 
	tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.OccsN,
		Len([SUB_MAJOR_GROUP_CODE]) AS NOC_Level,  
		T_NOC_Broad_Categories.SUB_MAJOR_GROUP_CODE AS NOC, 
		T_NOC_Broad_Categories.SUB_MAJOR_ENGLISH_NAME AS ENGLISH_NAME
	FROM tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union 
	INNER JOIN T_NOC_Broad_Categories 
		ON tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.NOC = T_NOC_Broad_Categories.UNIT_GROUP_CODE
) AS SourceTable

PIVOT (
    Sum(OccsN)
	FOR PSSM_CRED IN
		([1 - ADCT or ADIP],[1 - ADGR or UT],[1 - CERT],[1 - DIPL],[1 - PDCT or PDDP],[3 - ADCT or ADIP],
		[3 - ADGR or UT],[3 - CERT],[3 - DIPL],[3 - PDCT or PDDP],[APPRAPPR],[APPRCERT],[BACH],[DOCT],
		[GRCT or GRDP],[MAST],[P - CERT],[P - DIPL],[PDEG])
) AS PivotTable

ORDER BY NOC_Level,NOC;"
dbExecute(decimal_con, Q_4_NOC_3D_Totals_by_PSSM_CRED) 

# [SELECT INTO] Create Q_4_NOC_3D_Totals_by_Year


# ---- Q_4_NOC_3D_Totals_by_Year ----
Q_4_NOC_3D_Totals_by_Year <- "
SELECT 	Expr1,  Age_Group_Rollup_Label, NOC_Level, NOC, ENGLISH_NAME, Current_Region_PSSM_Code_Rollup, Current_Region_PSSM_Name_Rollup,
		[2023/2024], [2024/2025], [2025/2026], [2026/2027], [2027/2028], [2028/2029], 
		[2029/2030], [2030/2031], [2031/2032], [2032/2033], [2033/2034],[2034/2035]
		
INTO Q_4_NOC_3D_Totals_by_Year

FROM (
	SELECT 
	  Year,  
	  tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.OccsN,
		[Age_Group_Rollup_Label] + '-' 
			+ [T_NOC_Broad_Categories].[SUB_MAJOR_GROUP_CODE] + '-' 
			+ CAST([T_Current_Region_PSSM_Rollup_Codes].[Current_Region_PSSM_Code_Rollup] AS NVARCHAR(50)) AS Expr1, 
		tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.Age_Group_Rollup_Label, 
		Len(SUB_MAJOR_GROUP_CODE) AS NOC_Level, 
		T_NOC_Broad_Categories.SUB_MAJOR_GROUP_CODE AS NOC,
		T_NOC_Broad_Categories.SUB_MAJOR_ENGLISH_NAME AS ENGLISH_NAME, 
		T_Current_Region_PSSM_Rollup_Codes.Current_Region_PSSM_Code_Rollup,
		T_Current_Region_PSSM_Rollup_Codes.Current_Region_PSSM_Name_Rollup
	FROM (T_Current_Region_PSSM_Rollup_Codes 
	INNER JOIN tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union 
	  ON T_Current_Region_PSSM_Rollup_Codes.Current_Region_PSSM_Code_Rollup = tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.Current_Region_PSSM_Code_Rollup) 
	INNER JOIN T_NOC_Broad_Categories 
	  ON tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.NOC = T_NOC_Broad_Categories.UNIT_GROUP_CODE
) AS SourceTable

PIVOT (
    Sum(OccsN)
	FOR Year IN
    ([2023/2024], [2024/2025], [2025/2026], [2026/2027], [2027/2028], [2028/2029], 
    [2029/2030], [2030/2031], [2031/2032], [2032/2033], [2033/2034],[2034/2035])
) AS PivotTable

ORDER BY Age_Group_Rollup_Label, NOC_Level, ENGLISH_NAME, Current_Region_PSSM_Code_Rollup;"
dbExecute(decimal_con, Q_4_NOC_3D_Totals_by_Year) 

# [SELECT INTO] Create Q_4_NOC_4D_Totals_by_PSSM_CRED from tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union

# ---- Q_4_NOC_4D_Totals_by_PSSM_CRED ----
Q_4_NOC_4D_Totals_by_PSSM_CRED <- "
SELECT 	NOC_Level, NOC, ENGLISH_NAME,
		[1 - ADCT or ADIP],[1 - ADGR or UT],[1 - CERT],[1 - DIPL],[1 - PDCT or PDDP],[3 - ADCT or ADIP],
		[3 - ADGR or UT],[3 - CERT],[3 - DIPL],[3 - PDCT or PDDP],[APPRAPPR],[APPRCERT],[BACH],[DOCT],
		[GRCT or GRDP],[MAST],[P - CERT],[P - DIPL],[PDEG]
		
INTO Q_4_NOC_4D_Totals_by_PSSM_CRED

FROM (
	SELECT PSSM_CRED, 
	tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.OccsN,
		Len([MINOR_GROUP_CODE]) AS NOC_Level,  
		T_NOC_Broad_Categories.MINOR_GROUP_CODE AS NOC, 
		T_NOC_Broad_Categories.MINOR_GROUP_ENGLISH_NAME AS ENGLISH_NAME
	FROM tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union 
	INNER JOIN T_NOC_Broad_Categories 
		ON tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.NOC = T_NOC_Broad_Categories.UNIT_GROUP_CODE
) AS SourceTable

PIVOT (
    Sum(OccsN)
	FOR PSSM_CRED IN
		([1 - ADCT or ADIP],[1 - ADGR or UT],[1 - CERT],[1 - DIPL],[1 - PDCT or PDDP],[3 - ADCT or ADIP],
		[3 - ADGR or UT],[3 - CERT],[3 - DIPL],[3 - PDCT or PDDP],[APPRAPPR],[APPRCERT],[BACH],[DOCT],
		[GRCT or GRDP],[MAST],[P - CERT],[P - DIPL],[PDEG])
) AS PivotTable

ORDER BY NOC_Level,NOC;"
dbExecute(decimal_con, Q_4_NOC_4D_Totals_by_PSSM_CRED) 

# [SELECT INTO] Create Q_4_NOC_4D_Totals_by_Year

# ---- Q_4_NOC_4D_Totals_by_Year ----
Q_4_NOC_4D_Totals_by_Year <- "
SELECT 	Expr1,  Age_Group_Rollup_Label, NOC_Level, NOC, ENGLISH_NAME, Current_Region_PSSM_Code_Rollup, Current_Region_PSSM_Name_Rollup,
		[2023/2024], [2024/2025], [2025/2026], [2026/2027], [2027/2028], [2028/2029], 
		[2029/2030], [2030/2031], [2031/2032], [2032/2033], [2033/2034],[2034/2035]
		
INTO Q_4_NOC_4D_Totals_by_Year

FROM (
	SELECT 
	  Year,  
	  tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.OccsN,
		[Age_Group_Rollup_Label] + '-' 
			+ [T_NOC_Broad_Categories].[MINOR_GROUP_CODE] + '-' 
			+ CAST([T_Current_Region_PSSM_Rollup_Codes].[Current_Region_PSSM_Code_Rollup] AS NVARCHAR(50)) AS Expr1, 
		tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.Age_Group_Rollup_Label, 
		Len([MINOR_GROUP_CODE]) AS NOC_LEVEL, 
		T_NOC_Broad_Categories.MINOR_GROUP_CODE AS NOC,
		T_NOC_Broad_Categories.MINOR_GROUP_ENGLISH_NAME AS ENGLISH_NAME, 
		T_Current_Region_PSSM_Rollup_Codes.Current_Region_PSSM_Code_Rollup,
		T_Current_Region_PSSM_Rollup_Codes.Current_Region_PSSM_Name_Rollup
	FROM (T_Current_Region_PSSM_Rollup_Codes 
	INNER JOIN tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union 
	  ON T_Current_Region_PSSM_Rollup_Codes.Current_Region_PSSM_Code_Rollup = tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.Current_Region_PSSM_Code_Rollup) 
	INNER JOIN T_NOC_Broad_Categories 
	  ON tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.NOC = T_NOC_Broad_Categories.UNIT_GROUP_CODE
) AS SourceTable

PIVOT (
    Sum(OccsN)
	FOR Year IN
    ([2023/2024], [2024/2025], [2025/2026], [2026/2027], [2027/2028], [2028/2029], 
    [2029/2030], [2030/2031], [2031/2032], [2032/2033], [2033/2034],[2034/2035])
) AS PivotTable

ORDER BY Age_Group_Rollup_Label, NOC_Level, ENGLISH_NAME, Current_Region_PSSM_Code_Rollup;"
dbExecute(decimal_con, Q_4_NOC_4D_Totals_by_Year) 

# [SELECT INTO] Create Q_4_NOC_5D_Totals_by_PSSM_CRED from tmp_tbl_q_3d_occupations_by_lcip4_cred_lcp2_union


# ---- Q_4_NOC_5D_Totals_by_PSSM_CRED ----
Q_4_NOC_5D_Totals_by_PSSM_CRED <- 
"SELECT NOC_Level, NOC, ENGLISH_NAME,
		[1 - ADCT or ADIP],[1 - ADGR or UT],[1 - CERT],[1 - DIPL],[1 - PDCT or PDDP],[3 - ADCT or ADIP],
		[3 - ADGR or UT],[3 - CERT],[3 - DIPL],[3 - PDCT or PDDP],[APPRAPPR],[APPRCERT],[BACH],[DOCT],
		[GRCT or GRDP],[MAST],[P - CERT],[P - DIPL],[PDEG]
		
INTO Q_4_NOC_5D_Totals_by_PSSM_CRED

FROM (
	SELECT pssm_cred, 
	  tmp_tbl_q_3d_occupations_by_lcip4_cred_lcp2_union.occsn,
		Len(unit_group_code) AS NOC_LEVEL,
		T_NOC_Broad_Categories.unit_group_code AS NOC,	
		T_NOC_Broad_Categories.english_name AS ENGLISH_NAME
	FROM tmp_tbl_q_3d_occupations_by_lcip4_cred_lcp2_union
	INNER JOIN T_NOC_Broad_Categories 
		ON tmp_tbl_q_3d_occupations_by_lcip4_cred_lcp2_union.noc = T_NOC_Broad_Categories.unit_group_code
) AS SourceTable

PIVOT (
    Sum(OccsN)
	FOR PSSM_CRED IN
		([1 - ADCT or ADIP],[1 - ADGR or UT],[1 - CERT],[1 - DIPL],[1 - PDCT or PDDP],[3 - ADCT or ADIP],
		[3 - ADGR or UT],[3 - CERT],[3 - DIPL],[3 - PDCT or PDDP],[APPRAPPR],[APPRCERT],[BACH],[DOCT],
		[GRCT or GRDP],[MAST],[P - CERT],[P - DIPL],[PDEG])
) AS PivotTable

ORDER BY NOC_Level, NOC;"
dbExecute(decimal_con, Q_4_NOC_5D_Totals_by_PSSM_CRED) 

# [SELECT INTO] Create Q_4_NOC_5D_Totals_by_Year


# ---- Q_4_NOC_5D_Totals_by_Year ----
Q_4_NOC_5D_Totals_by_Year <- "
SELECT Expr1,  Age_Group_Rollup_Label,  NOC_Level, NOC,  ENGLISH_NAME, Current_Region_PSSM_Code_Rollup, Current_Region_PSSM_Name_Rollup,
		[2023/2024], [2024/2025], [2025/2026], [2026/2027], [2027/2028], [2028/2029], [2029/2030], 
		[2030/2031], [2031/2032], [2032/2033], [2033/2034],[2034/2035]
	    
INTO Q_4_NOC_5D_Totals_by_Year

FROM (
	SELECT year,	
		tmp_tbl_q_3d_occupations_by_lcip4_cred_lcp2_union.occsn,
		[age_group_rollup_label] + '-'
			+ RIGHT(REPLICATE('0', 5) + CAST([noc] AS NVARCHAR(5)), 5) + '-' 
			+ CAST([t_current_region_pssm_rollup_codes].[current_region_pssm_code_rollup] AS NVARCHAR(50)) AS Expr1,
		tmp_tbl_q_3d_occupations_by_lcip4_cred_lcp2_union.age_group_rollup_label,
		Len(unit_group_code) AS NOC_Level,
		T_NOC_Broad_Categories.unit_group_code AS NOC, 
		T_NOC_Broad_Categories.english_name AS ENGLISH_NAME,
		t_current_region_pssm_rollup_codes.current_region_pssm_code_rollup,
		t_current_region_pssm_rollup_codes.current_region_pssm_name_rollup
	FROM (t_current_region_pssm_rollup_codes 
	INNER JOIN tmp_tbl_q_3d_occupations_by_lcip4_cred_lcp2_union
	ON t_current_region_pssm_rollup_codes.current_region_pssm_code_rollup = tmp_tbl_q_3d_occupations_by_lcip4_cred_lcp2_union.current_region_pssm_code_rollup) 
	INNER JOIN T_NOC_Broad_Categories
	ON tmp_tbl_q_3d_occupations_by_lcip4_cred_lcp2_union.noc = T_NOC_Broad_Categories.unit_group_code
) AS SourceTable

PIVOT (
    Sum(OccsN)
	FOR Year IN
    ([2023/2024], [2024/2025], [2025/2026], [2026/2027], [2027/2028], [2028/2029], 
    [2029/2030], [2030/2031], [2031/2032], [2032/2033], [2033/2034],[2034/2035])
) AS PivotTable

ORDER BY age_group_rollup_label, NOC, current_region_pssm_code_rollup;"
dbExecute(decimal_con, Q_4_NOC_5D_Totals_by_Year) 

# [SELECT INTO] Create Q_4_NOC_5D_Totals_by_Year_Input_for_Rounding


# ---- Q_4_NOC_5D_Totals_by_Year_Input_for_Rounding ----
Q_4_NOC_5D_Totals_by_Year_Input_for_Rounding <- 
"SELECT 
  [age_group_rollup_label] + '-' 
    + RIGHT(REPLICATE('0', 5) + CAST([noc] AS NVARCHAR(5)), 5) + '-' 
    + CAST([t_current_region_pssm_rollup_codes].[current_region_pssm_code_rollup] AS NVARCHAR(50)) AS Expr1,
  tmp_tbl_q_3d_occupations_by_lcip4_cred_lcp2_union.age_group_rollup_label,
  Len(unit_group_code) AS NOC_Level,
  T_NOC_Broad_Categories.unit_group_code AS NOC,
  T_NOC_Broad_Categories.english_name AS ENGLISH NAME,
  t_current_region_pssm_rollup_codes.current_region_pssm_code_rollup,
  t_current_region_pssm_rollup_codes.current_region_pssm_name_rollup,
  tmp_tbl_q_3d_occupations_by_lcip4_cred_lcp2_union.year,
  Round(Sum([occsn]), 0) AS CountN
INTO Q_4_NOC_5D_Totals_by_Year_Input_for_Rounding 
FROM   (t_current_region_pssm_rollup_codes
  INNER JOIN tmp_tbl_q_3d_occupations_by_lcip4_cred_lcp2_union
  ON t_current_region_pssm_rollup_codes.current_region_pssm_code_rollup = tmp_tbl_q_3d_occupations_by_lcip4_cred_lcp2_union.current_region_pssm_code_rollup)
INNER JOIN T_NOC_Broad_Categories
   ON tmp_tbl_q_3d_occupations_by_lcip4_cred_lcp2_union.noc = T_NOC_Broad_Categories.unit_group_code
GROUP  BY [age_group_rollup_label] + '-' + '-' + RIGHT(REPLICATE('0', 5) + CAST([noc] AS NVARCHAR(5)), 5) + '-' +
  CAST([t_current_region_pssm_rollup_codes].[current_region_pssm_code_rollup] AS NVARCHAR(50)) ,
  tmp_tbl_q_3d_occupations_by_lcip4_cred_lcp2_union.age_group_rollup_label,
  Len(unit_group_code),
  T_NOC_Broad_Categories.unit_group_code,
  T_NOC_Broad_Categories.english_name,
  t_current_region_pssm_rollup_codes.current_region_pssm_code_rollup,
  t_current_region_pssm_rollup_codes.current_region_pssm_name_rollup,
  tmp_tbl_q_3d_occupations_by_lcip4_cred_lcp2_union.year
ORDER BY tmp_tbl_q_3d_occupations_by_lcip4_cred_lcp2_union.age_group_rollup_label,
T_NOC_Broad_Categories.unit_group_code,
t_current_region_pssm_rollup_codes.current_region_pssm_code_rollup;"
# FIXME dbExecute(decimal_con, Q_4_NOC_5D_Totals_by_Year_Input_for_Rounding)                                                                                                                                                                                                                                                         

# ---- Q_4_NOC_Totals Series ---- 

# [SQL]


# ---- Q_4_NOC_Totals_by_Year_and_PSSM_CRED ----
Q_4_NOC_Totals_by_Year_and_PSSM_CRED <- 
"SELECT tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.PSSM_CRED, 
tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.Year, 
tbl_NOC_Skill_Level_Aged_17_34.PSSM_Skill_Level, 
tbl_NOC_Skill_Level_Aged_17_34.SKILL_LEVEL_Initial, 
tbl_NOC_Skill_Level_Aged_17_34.SKILL_LEVEL_CATEGORY_CODE, 
tbl_NOC_Skill_Level_Aged_17_34.UNIT_GROUP_CODE AS NOC, 
tbl_NOC_Skill_Level_Aged_17_34.ENGLISH_NAME, 
Sum(tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.OccsN) AS CountN, 
RoundToLarger(Sum([OccsN]),0) AS CountNRnd
FROM tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union 
INNER JOIN tbl_NOC_Skill_Level_Aged_17_34 
ON tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.NOC = tbl_NOC_Skill_Level_Aged_17_34.UNIT_GROUP_CODE
GROUP BY tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.PSSM_CRED, 
tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.Year, tbl_NOC_Skill_Level_Aged_17_34.PSSM_Skill_Level, 
tbl_NOC_Skill_Level_Aged_17_34.SKILL_LEVEL_Initial, tbl_NOC_Skill_Level_Aged_17_34.SKILL_LEVEL_CATEGORY_CODE, 
tbl_NOC_Skill_Level_Aged_17_34.UNIT_GROUP_CODE, tbl_NOC_Skill_Level_Aged_17_34.ENGLISH_NAME
HAVING (((tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.Year) In ('2011/2012','2012/2013','2013/2014','2014/2015','2015/2016')));"
# FIXME dbGetQuery(decimal_con, Q_4_NOC_Totals_by_Year_and_PSSM_CRED) 

# [SELECT INTO] Create Q_4_NOC_Totals_by_Year from Q_4_NOC_4D_Totals_by_Year


# ---- Q_4_NOC_Totals_by_Year ----
Q_4_NOC_Totals_by_Year <- 
"SELECT *
INTO Q_4_NOC_Totals_by_Year 
FROM Q_4_NOC_4D_Totals_by_Year
UNION ALL SELECT *
FROM Q_4_NOC_3D_Totals_by_Year
UNION ALL SELECT *
FROM Q_4_NOC_2D_Totals_by_Year
UNION ALL SELECT *
FROM Q_4_NOC_1D_Totals_by_Year
UNION ALL SELECT *
FROM Q_4_NOC_5D_Totals_by_Year
ORDER BY 6, 4, 2, 8;"
dbExecute(decimal_con, Q_4_NOC_Totals_by_Year) 

# [SELECT INTO] Create Q_4_NOC_Totals_by_Year_BC


# ---- Q_4_NOC_Totals_by_Year_BC ----
Q_4_NOC_Totals_by_Year_BC <- 
"SELECT [q_4_noc_totals_by_year].[age_group_rollup_label] + '-'
       + [q_4_noc_totals_by_year].[noc] + '-'
       + CAST([t_current_region_pssm_rollup_codes].[current_region_pssm_code_rollup] AS NVARCHAR(50))  AS  Expr1000,
       q_4_noc_totals_by_year.age_group_rollup_label,
       q_4_noc_totals_by_year.noc_level,
       q_4_noc_totals_by_year.noc,
       q_4_noc_totals_by_year.english_name,
       t_current_region_pssm_rollup_codes.current_region_pssm_code_rollup,
       t_current_region_pssm_rollup_codes.current_region_pssm_name_rollup,
       Sum(q_4_noc_totals_by_year.[2023/2024]) AS [2023/2024],
       Sum(q_4_noc_totals_by_year.[2024/2025]) AS [2024/2025],
       Sum(q_4_noc_totals_by_year.[2025/2026]) AS [2025/2026],
       Sum(q_4_noc_totals_by_year.[2026/2027]) AS [2026/2027],
       Sum(q_4_noc_totals_by_year.[2027/2028]) AS [2027/2028],
       Sum(q_4_noc_totals_by_year.[2028/2029]) AS [2028/2029],
       Sum(q_4_noc_totals_by_year.[2029/2030]) AS [2029/2030],
       Sum(q_4_noc_totals_by_year.[2030/2031]) AS [2030/2031],
       Sum(q_4_noc_totals_by_year.[2031/2032]) AS [2031/2032],
       Sum(q_4_noc_totals_by_year.[2032/2033]) AS [2032/2033],
       Sum(q_4_noc_totals_by_year.[2033/2034]) AS [2033/2034],
       Sum(q_4_noc_totals_by_year.[2034/2035]) AS [2034/2035]
INTO Q_4_NOC_Totals_by_Year_BC
FROM   (q_4_noc_totals_by_year
  INNER JOIN t_current_region_pssm_rollup_codes_bc
    ON q_4_noc_totals_by_year.current_region_pssm_code_rollup = t_current_region_pssm_rollup_codes_bc.current_region_pssm_code_rollup)
  INNER JOIN t_current_region_pssm_rollup_codes
    ON t_current_region_pssm_rollup_codes_bc.current_region_pssm_code_rollup_bc = t_current_region_pssm_rollup_codes.current_region_pssm_code_rollup
GROUP  BY [q_4_noc_totals_by_year].[age_group_rollup_label] 
          + '-' + [q_4_noc_totals_by_year].[noc] + '-'
          + CAST([t_current_region_pssm_rollup_codes].[current_region_pssm_code_rollup] AS NVARCHAR(50)),
          q_4_noc_totals_by_year.age_group_rollup_label,
          q_4_noc_totals_by_year.noc_level,
          q_4_noc_totals_by_year.noc,
          q_4_noc_totals_by_year.english_name,
          t_current_region_pssm_rollup_codes.current_region_pssm_code_rollup,
          t_current_region_pssm_rollup_codes.current_region_pssm_name_rollup,
          t_current_region_pssm_rollup_codes.current_region_pssm_name_rollup;"
dbExecute(decimal_con, Q_4_NOC_Totals_by_Year_BC) 

# [SELECT INTO] Create Q_4_NOC_Totals_by_Year_Total

# ---- Q_4_NOC_Totals_by_Year_Total ----
Q_4_NOC_Totals_by_Year_Total <- "
SELECT [q_4_noc_totals_by_year].[age_group_rollup_label]
    + '-' + [q_4_noc_totals_by_year].[noc] + '-' +
    CAST([t_current_region_pssm_rollup_codes].[current_region_pssm_code_rollup] AS NVARCHAR(50)) AS Expr1000,
    q_4_noc_totals_by_year.age_group_rollup_label,
    q_4_noc_totals_by_year.noc_level,
    q_4_noc_totals_by_year.noc,
    q_4_noc_totals_by_year.english_name, 
    t_current_region_pssm_rollup_codes.current_region_pssm_code_rollup,
    t_current_region_pssm_rollup_codes.current_region_pssm_name_rollup,
    Sum(q_4_noc_totals_by_year.[2023/2024]) AS [2023/2024], 
    Sum(q_4_noc_totals_by_year.[2024/2025]) AS [2024/2025], 
    Sum(q_4_noc_totals_by_year.[2025/2026]) AS [2025/2026], 
    Sum(q_4_noc_totals_by_year.[2026/2027]) AS [2026/2027], 
    Sum(q_4_noc_totals_by_year.[2027/2028]) AS [2027/2028], 
    Sum(q_4_noc_totals_by_year.[2028/2029]) AS [2028/2029], 
    Sum(q_4_noc_totals_by_year.[2029/2030]) AS [2029/2030], 
    Sum(q_4_noc_totals_by_year.[2030/2031]) AS [2030/2031],
    Sum(q_4_noc_totals_by_year.[2031/2032]) AS [2031/2032],
    Sum(q_4_noc_totals_by_year.[2032/2033]) AS [2032/2033],
    Sum(q_4_noc_totals_by_year.[2033/2034]) AS [2033/2034],
    Sum(q_4_noc_totals_by_year.[2034/2035]) AS [2034/2035]
INTO Q_4_NOC_Totals_by_Year_Total
FROM (q_4_noc_totals_by_year
INNER JOIN t_current_region_pssm_rollup_codes_bc
ON q_4_noc_totals_by_year.current_region_pssm_code_rollup = t_current_region_pssm_rollup_codes_bc.current_region_pssm_code_rollup)
INNER JOIN t_current_region_pssm_rollup_codes 
ON t_current_region_pssm_rollup_codes_bc.current_region_pssm_code_rollup_total= t_current_region_pssm_rollup_codes.current_region_pssm_code_rollup
GROUP BY [q_4_noc_totals_by_year].[age_group_rollup_label]
    + '-' + [q_4_noc_totals_by_year].[noc] + '-' +
    CAST([t_current_region_pssm_rollup_codes].[current_region_pssm_code_rollup] AS NVARCHAR(50)),
    q_4_noc_totals_by_year.age_group_rollup_label,
    q_4_noc_totals_by_year.noc_level,
    q_4_noc_totals_by_year.noc, q_4_noc_totals_by_year.english_name,
    t_current_region_pssm_rollup_codes.current_region_pssm_code_rollup,
    t_current_region_pssm_rollup_codes.current_region_pssm_name_rollup,
    t_current_region_pssm_rollup_codes.current_region_pssm_name_rollup;"
dbExecute(decimal_con, Q_4_NOC_Totals_by_Year_Total)

dbExecute(decimal_con, "DROP TABLE Q_4_NOC_1D_Totals_by_PSSM_CRED")
dbExecute(decimal_con, "DROP TABLE Q_4_NOC_1D_Totals_by_Year")
dbExecute(decimal_con, "DROP TABLE Q_4_NOC_2D_Totals_by_PSSM_CRED")
dbExecute(decimal_con, "DROP TABLE Q_4_NOC_2D_Totals_by_PSSM_CRED_Appendix")
dbExecute(decimal_con, "DROP TABLE Q_4_NOC_2D_Totals_by_Year")
dbExecute(decimal_con, "DROP TABLE Q_4_NOC_3D_Totals_by_PSSM_CRED")
dbExecute(decimal_con, "DROP TABLE Q_4_NOC_3D_Totals_by_Year")
dbExecute(decimal_con, "DROP TABLE Q_4_NOC_4D_Totals_by_PSSM_CRED")
dbExecute(decimal_con, "DROP TABLE Q_4_NOC_4D_Totals_by_Year")
dbExecute(decimal_con, "DROP TABLE Q_4_NOC_5D_Totals_by_PSSM_CRED")
dbExecute(decimal_con, "DROP TABLE Q_4_NOC_5D_Totals_by_Year")
#dbExecute(decimal_con, "DROP TABLE Q_4_NOC_4D_Totals_by_Year_Input_for_Rounding")

# ---- Q_5 Series ---- 

# [SELECT INTO] Create Q_5_NOC_Totals_by_Year_and_BC from Q_4_NOC_Totals_by_Year


# ---- Q_5_NOC_Totals_by_Year_and_BC ----
Q_5_NOC_Totals_by_Year_and_BC <- 
"SELECT Q_4_NOC_Totals_by_Year.*
INTO Q_5_NOC_Totals_by_Year_and_BC
FROM Q_4_NOC_Totals_by_Year
UNION ALL SELECT Q_4_NOC_Totals_by_Year_BC.*
FROM Q_4_NOC_Totals_by_Year_BC
ORDER BY 6, 4, 2, 8;"
dbExecute(decimal_con, Q_5_NOC_Totals_by_Year_and_BC) 

# [SELECT INTO] Create Q_5_NOC_Totals_by_Year_and_BC_and_Total from Q_4_NOC_Totals_by_Year


# ---- Q_5_NOC_Totals_by_Year_and_BC_and_Total ----
Q_5_NOC_Totals_by_Year_and_BC_and_Total <- 
"SELECT Q_4_NOC_Totals_by_Year.*
INTO Q_5_NOC_Totals_by_Year_and_BC_and_Total
FROM Q_4_NOC_Totals_by_Year

UNION ALL SELECT Q_4_NOC_Totals_by_Year_BC.*
FROM Q_4_NOC_Totals_by_Year_BC

UNION ALL SELECT Q_4_NOC_Totals_by_Year_Total.*
FROM Q_4_NOC_Totals_by_Year_Total
ORDER BY 6, 4, 2, 8;"
dbExecute(decimal_con, Q_5_NOC_Totals_by_Year_and_BC_and_Total)
dbExecute(decimal_con, "DROP TABLE Q_4_NOC_Totals_by_Year")
dbExecute(decimal_con, "DROP TABLE Q_4_NOC_Totals_by_Year_BC")
dbExecute(decimal_con, "DROP TABLE Q_4_NOC_Totals_by_Year_Total")

# ---- Q_6 Series ---- 
if (regular_run == T){

# [SELECT INTO] Create tmp_tbl_Model from Q_5_NOC_Totals_by_Year_and_BC_and_Total

# ---- Q_6_tmp_tbl_Model ----
Q_6_tmp_tbl_Model <- 
"SELECT Q_5_NOC_Totals_by_Year_and_BC_and_Total.* 
INTO tmp_tbl_Model
FROM Q_5_NOC_Totals_by_Year_and_BC_and_Total;"
dbExecute(decimal_con, Q_6_tmp_tbl_Model)
  } 

if (qi_run == T) {

# [SELECT INTO] Create tmp_tbl_QI from Q_5_NOC_Totals_by_Year_and_BC_and_Total

Q_6_tmp_tbl_Model_QI <- 
"SELECT Q_5_NOC_Totals_by_Year_and_BC_and_Total.* 
 INTO tmp_tbl_QI
 FROM Q_5_NOC_Totals_by_Year_and_BC_and_Total;"
  dbExecute(decimal_con, Q_6_tmp_tbl_Model_QI) # QI toggle
}

if (ptib_run == T){

# [SELECT INTO] Create tmp_tbl_Model_Inc_Private_Inst from Q_5_NOC_Totals_by_Year_and_BC_and_Total

# ---- Q_6_tmp_tbl_Model_Inc_Private_Inst ----
Q_6_tmp_tbl_Model_Inc_Private_Inst <- 
"SELECT Q_5_NOC_Totals_by_Year_and_BC_and_Total.* 
INTO tmp_tbl_Model_Inc_Private_Inst
FROM Q_5_NOC_Totals_by_Year_and_BC_and_Total;"
dbExecute(decimal_con, Q_6_tmp_tbl_Model_Inc_Private_Inst) 
}

# [SELECT INTO] Create tmp_tbl_Model_Program_Projection from Q_5_NOC_Totals_by_Year_and_BC_and_Total

# ---- Q_6_tmp_tbl_Model_Program_Projection ----
Q_6_tmp_tbl_Model_Program_Projection <- 
"SELECT Q_5_NOC_Totals_by_Year_and_BC_and_Total.* 
INTO tmp_tbl_Model_Program_Projection
FROM Q_5_NOC_Totals_by_Year_and_BC_and_Total;"
#dbExecute(decimal_con, Q_6_tmp_tbl_Model_Program_Projection) 

if (regular_run == T | qi_run == T){
dbExecute(decimal_con, "DROP TABLE Q_5_NOC_Totals_by_Year_and_BC")
dbExecute(decimal_con, "DROP TABLE Q_5_NOC_Totals_by_Year_and_BC_and_Total")

# see 08 script to replace below
# # ---- model with QI ----
# if (regular_run != T){

# [SQL]

Q_7_QI <- 
"SELECT tmp_tbl_Model.Expr1, tmp_tbl_Model.NOC_Level, tmp_tbl_Model.NOC, tmp_tbl_Model.ENGLISH_NAME, tmp_tbl_Model.Current_Region_PSSM_Code_Rollup, 
tmp_tbl_Model.Current_Region_PSSM_Name_Rollup, 
tmp_tbl_Model.[2023/2024] AS Model_Y1, 
tmp_tbl_QI.[2023/2024] AS QI_Y1,
CASE WHEN [tmp_tbl_QI].[Expr1] = Null THEN '' ELSE Abs(tmp_tbl_Model.[2023/2024]-tmp_tbl_QI.[2023/2024])/tmp_tbl_QI.[2023/2024] END AS ErrorRate
FROM tmp_tbl_Model 
LEFT JOIN tmp_tbl_QI 
	ON tmp_tbl_Model.Expr1 = tmp_tbl_QI.Expr1"
#   dbGetQuery(decimal_con, Q_7_QI) %>%
#     write_csv(glue::glue("{lan}/reports-final/drafts/error_rate_by_noc_static_incl_ptib.csv"))
# 

# [SQL]


# ---- Q_8_Labour_Supply_Total_by_Year ----
Q_8_Labour_Supply_Total_by_Year <- 
"SELECT tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.PSSM_CRED AS Expr1, 
Sum(tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.NLS) AS SumOfNLS
FROM tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union
WHERE (((tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.Year) In 
  ('2023/2024','2024/2025','2026/2027','2027/2028','2029/2030','2030/2031','2031/2032','2032/2033','2033/2034','2034/2035')))
GROUP BY tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.PSSM_CRED;"
#   dbGetQuery(decimal_con, Q_8_Labour_Supply_Total_by_Year) %>%
#     write_csv(glue::glue("{lan}/reports-final/drafts/labour_supply_by_year_static_incl_ptib.csv"))
# 
#   # gives final model output with quality indicator and coverage indicator counts-not too useful for anything, better queries below

# [SELECT INTO] Create qry_10a_Model


# ---- qry_10a_Model ----
qry_10a_Model <- 
"SELECT tmp_tbl_Model.Expr1, 
tmp_tbl_Model.Age_Group_Rollup_Label, 
tmp_tbl_Model.NOC, 
tmp_tbl_Model.ENGLISH_NAME, 
tmp_tbl_Model.Current_Region_PSSM_Code_Rollup, 
tmp_tbl_Model.Current_Region_PSSM_Name_Rollup, 
tmp_tbl_Model.[2023/2024], 
tmp_tbl_Model.[2024/2025], 
tmp_tbl_Model.[2025/2026], 
tmp_tbl_Model.[2026/2027], 
tmp_tbl_Model.[2027/2028], 
tmp_tbl_Model.[2028/2029], 
tmp_tbl_Model.[2029/2030], 
tmp_tbl_Model.[2030/2031],
tmp_tbl_Model.[2031/2032],
tmp_tbl_Model.[2032/2033],
tmp_tbl_Model.[2033/2034],
tmp_tbl_Model.[2034/2035],
tmp_tbl_QI.[2023/2024] AS QI, 
tmp_tbl_Model_Inc_Private_Inst.[2023/2024] AS CI
INTO qry_10a_Model
FROM (tmp_tbl_Model 
LEFT JOIN tmp_tbl_QI 
ON tmp_tbl_Model.Expr1 = tmp_tbl_QI.Expr1) 
LEFT JOIN tmp_tbl_Model_Inc_Private_Inst ON tmp_tbl_Model.Expr1 = tmp_tbl_Model_Inc_Private_Inst.Expr1
ORDER BY 2, 7, 5, 3, 9;"
#   dbExecute(decimal_con, qry_10a_Model)
# 
#   dbGetQuery(decimal_con, "SELECT * FROM qry_10a_Model") %>%
#     write_csv(glue::glue("{lan}/reports-final/drafts/full_model_static_incl_ptib.csv"))
# }

# 
# # ---- public release ----

# [SELECT INTO] Create qry_10a_Model_Public_Release


# ---- qry_10a_Model_Public_Release ----
qry_10a_Model_Public_Release <- 
"SELECT tmp_tbl_Model.Expr1, tmp_tbl_Model.Age_Group_Rollup_Label, 
tmp_tbl_Model.NOC, tmp_tbl_Model.ENGLISH_NAME, 
tmp_tbl_Model.Current_Region_PSSM_Code_Rollup, tmp_tbl_Model.Current_Region_PSSM_Name_Rollup, 
CEILING([tmp_tbl_Model].[2023/2024]) AS [2023/2024], 
CEILING([tmp_tbl_Model].[2024/2025]) AS [2024/2025], 
CEILING([tmp_tbl_Model].[2025/2026]) AS [2025/2026], 
CEILING([tmp_tbl_Model].[2026/2027]) AS [2026/2027], 
CEILING([tmp_tbl_Model].[2027/2028]) AS [2027/2028], 
CEILING([tmp_tbl_Model].[2028/2029]) AS [2028/2029], 
CEILING([tmp_tbl_Model].[2029/2030]) AS [2029/2030], 
CEILING([tmp_tbl_Model].[2030/2031]) AS [2030/2031],
CEILING([tmp_tbl_Model].[2031/2032]) AS [2031/2032],
CEILING([tmp_tbl_Model].[2032/2033]) AS [2032/2033],
CEILING([tmp_tbl_Model].[2033/2034]) AS [2033/2034],
CEILING([tmp_tbl_Model].[2034/2035]) AS [2034/2035]
INTO qry_10a_Model_Public_Release
FROM ((tmp_tbl_Model 
LEFT JOIN tmp_tbl_QI 
  ON tmp_tbl_Model.Expr1 = tmp_tbl_QI.Expr1) 
  LEFT JOIN tmp_tbl_Model_Inc_Private_Inst 
    ON tmp_tbl_Model.Expr1 = tmp_tbl_Model_Inc_Private_Inst.Expr1) 
LEFT JOIN T_Suppression_Public_Release_NOC 
  ON (tmp_tbl_Model.Age_Group_Rollup_Label = T_Suppression_Public_Release_NOC.Age_Group_Rollup_Label) 
AND (tmp_tbl_Model.NOC = T_Suppression_Public_Release_NOC.NOC_CD)
WHERE (((tmp_tbl_Model.NOC_Level) = 5) And ((tmp_tbl_Model.NOC) <> '99999') 
And ((tmp_tbl_Model.Current_Region_PSSM_Code_Rollup)=5900) 
And (((Abs(tmp_tbl_Model.[2023/2024]-tmp_tbl_QI.[2023/2024])/tmp_tbl_QI.[2023/2024]))<0.25) 
And ((T_Suppression_Public_Release_NOC.Age_Group_Rollup_Label) Is Null) 
And ((T_Suppression_Public_Release_NOC.NOC_CD) Is Null))
ORDER BY tmp_tbl_Model.Age_Group_Rollup_Label, tmp_tbl_Model.NOC;"
# dbExecute(decimal_con, qry_10a_Model_Public_Release) # gives rounded 5-digit NOC result

# [SELECT INTO] Create qry_10a_Model_Public_Release_Suppressed from tmp_tbl_Model


# ---- qry_10a_Model_Public_Release_Suppressed ----
qry_10a_Model_Public_Release_Suppressed <- 
"SELECT tmp_tbl_Model.Age_Group_Rollup_Label, tmp_tbl_Model.NOC, tmp_tbl_Model.ENGLISH_NAME, 
tmp_tbl_Model.Current_Region_PSSM_Code_Rollup, tmp_tbl_Model.Current_Region_PSSM_Name_Rollup, 
CEILING(Sum([tmp_tbl_Model].[2023/2024])) AS [2023/2024], 
CEILING(Sum([tmp_tbl_Model].[2024/2025])) AS [2024/2025], 
CEILING(Sum([tmp_tbl_Model].[2025/2026])) AS [2025/2026], 
CEILING(Sum([tmp_tbl_Model].[2026/2027])) AS [2026/2027], 
CEILING(Sum([tmp_tbl_Model].[2027/2028])) AS [2027/2028], 
CEILING(Sum([tmp_tbl_Model].[2028/2029])) AS [2028/2029], 
CEILING(Sum([tmp_tbl_Model].[2029/2030])) AS [2029/2030], 
CEILING(Sum([tmp_tbl_Model].[2030/2031])) AS [2030/2031],
CEILING(Sum([tmp_tbl_Model].[2031/2032])) AS [2031/2032],
CEILING(Sum([tmp_tbl_Model].[2032/2033])) AS [2032/2033],
CEILING(Sum([tmp_tbl_Model].[2033/2034])) AS [2033/2034],
CEILING(Sum([tmp_tbl_Model].[2034/2035])) AS [2034/2035]
INTO qry_10a_Model_Public_Release_Suppressed
FROM tmp_tbl_Model LEFT JOIN qry_10a_Model_Public_Release 
ON (tmp_tbl_Model.Age_Group_Rollup_Label = qry_10a_Model_Public_Release.Age_Group_Rollup_Label) 
AND (tmp_tbl_Model.NOC = qry_10a_Model_Public_Release.NOC)
WHERE (((qry_10a_Model_Public_Release.Age_Group_Rollup_Label) Is Null) 
AND ((qry_10a_Model_Public_Release.NOC) Is Null) AND ((tmp_tbl_Model.NOC_Level)=5))
GROUP BY tmp_tbl_Model.Age_Group_Rollup_Label, 
tmp_tbl_Model.NOC, 
tmp_tbl_Model.ENGLISH_NAME, 
tmp_tbl_Model.Current_Region_PSSM_Code_Rollup, 
tmp_tbl_Model.Current_Region_PSSM_Name_Rollup
HAVING (((tmp_tbl_Model.Current_Region_PSSM_Code_Rollup)=5900))
ORDER BY tmp_tbl_Model.Age_Group_Rollup_Label, tmp_tbl_Model.NOC;"
# dbExecute(decimal_con, qry_10a_Model_Public_Release_Suppressed) # shows the 5-digit NOCs that have been suppressed

# [SELECT INTO] Create qry_10a_Model_Public_Release_Suppressed_Total from tmp_tbl_Model


# ---- qry_10a_Model_Public_Release_Suppressed_Total ----
qry_10a_Model_Public_Release_Suppressed_Total <- 
"SELECT '' AS Expr1, tmp_tbl_Model.Age_Group_Rollup_Label, '99998' AS [NOC], 'Other' AS [ENGLISH_NAME], 
tmp_tbl_Model.Current_Region_PSSM_Code_Rollup, tmp_tbl_Model.Current_Region_PSSM_Name_Rollup, 
Sum(CEILING(CASE WHEN [tmp_tbl_Model].[2023/2024] IS NULL THEN 0 ELSE [tmp_tbl_Model].[2023/2024] END)) AS [2023/2024], 
Sum(CEILING(CASE WHEN [tmp_tbl_Model].[2024/2025] IS NULL THEN 0 ELSE [tmp_tbl_Model].[2024/2025] END)) AS [2024/2025], 
Sum(CEILING(CASE WHEN [tmp_tbl_Model].[2025/2026] IS NULL THEN 0 ELSE [tmp_tbl_Model].[2025/2026] END)) AS [2025/2026], 
Sum(CEILING(CASE WHEN [tmp_tbl_Model].[2026/2027] IS NULL THEN 0 ELSE [tmp_tbl_Model].[2026/2027] END)) AS [2026/2027], 
Sum(CEILING(CASE WHEN [tmp_tbl_Model].[2027/2028] IS NULL THEN 0 ELSE [tmp_tbl_Model].[2027/2028] END)) AS [2027/2028], 
Sum(CEILING(CASE WHEN [tmp_tbl_Model].[2028/2029] IS NULL THEN 0 ELSE [tmp_tbl_Model].[2028/2029] END)) AS [2028/2029], 
Sum(CEILING(CASE WHEN [tmp_tbl_Model].[2029/2030] IS NULL THEN 0 ELSE [tmp_tbl_Model].[2029/2030] END)) AS [2029/2030], 
Sum(CEILING(CASE WHEN [tmp_tbl_Model].[2030/2031] IS NULL THEN 0 ELSE [tmp_tbl_Model].[2030/2031] END)) AS [2030/2031],
Sum(CEILING(CASE WHEN [tmp_tbl_Model].[2031/2032] IS NULL THEN 0 ELSE [tmp_tbl_Model].[2031/2032] END)) AS [2031/2032],
Sum(CEILING(CASE WHEN [tmp_tbl_Model].[2032/2033] IS NULL THEN 0 ELSE [tmp_tbl_Model].[2032/2033] END)) AS [2032/2033],
Sum(CEILING(CASE WHEN [tmp_tbl_Model].[2033/2034] IS NULL THEN 0 ELSE [tmp_tbl_Model].[2033/2034] END)) AS [2033/2034],
Sum(CEILING(CASE WHEN [tmp_tbl_Model].[2034/2035] IS NULL THEN 0 ELSE [tmp_tbl_Model].[2034/2035] END)) AS [2034/2035]
INTO qry_10a_Model_Public_Release_Suppressed_Total
FROM tmp_tbl_Model LEFT JOIN qry_10a_Model_Public_Release ON tmp_tbl_Model.Expr1 = qry_10a_Model_Public_Release.Expr1
WHERE (((tmp_tbl_Model.NOC_Level)=5) AND ((qry_10a_Model_Public_Release.Expr1) Is Null))
GROUP BY tmp_tbl_Model.Age_Group_Rollup_Label,
tmp_tbl_Model.Current_Region_PSSM_Code_Rollup, 
tmp_tbl_Model.Current_Region_PSSM_Name_Rollup
HAVING (((tmp_tbl_Model.Current_Region_PSSM_Code_Rollup) = 5900));"
# dbExecute(decimal_con, qry_10a_Model_Public_Release_Suppressed_Total) # sum of the 5-digit NOCs that have been suppressed and can be included in final public release

# [SELECT INTO] Create qry_10a_Model_Public_Release_Union from qry_10a_Model_Public_Release


# ---- qry_10a_Model_Public_Release_Union ----
qry_10a_Model_Public_Release_Union <- 
"SELECT qry_10a_Model_Public_Release.*
INTO qry_10a_Model_Public_Release_Union
FROM qry_10a_Model_Public_Release
UNION ALL SELECT qry_10a_Model_Public_Release_Suppressed_Total.*
FROM qry_10a_Model_Public_Release_Suppressed_Total
ORDER BY 2, 6, 4, 5, 8;"
# dbExecute(decimal_con, qry_10a_Model_Public_Release_Union) # final output with suppressed counts for public release
# 
# dbGetQuery(decimal_con, "SELECT * FROM qry_10a_Model_Public_Release_Union") %>% 
#   write_csv(glue::glue("{lan}/reports-final/drafts/public_release_static_incl_ptib.csv"))
# 
# 
# # ---- internal release ----

# [SQL]


# ---- qry_10a_Model_QI_PPCI ----
qry_10a_Model_QI_PPCI <- 
"SELECT tmp_tbl_Model.Expr1, 
tmp_tbl_Model.Age_Group_Rollup_Label, 
--no longer present in NOC data
--tmp_tbl_Model.SKILL_LEVEL_CATEGORY_CODE, 
tmp_tbl_Model.NOC_Level, 
-- no longer present in NOC data
--tmp_tbl_Model.NOC_SKILL_TYPE, 
tmp_tbl_Model.NOC, 
tmp_tbl_Model.ENGLISH_NAME, 
tmp_tbl_Model.Current_Region_PSSM_Code_Rollup, 
tmp_tbl_Model.Current_Region_PSSM_Name_Rollup, 
Ceiling([tmp_tbl_Model].[2023/2024]) AS [2023/2024], 
Ceiling([tmp_tbl_Model].[2024/2025]) AS [2024/2025], 
Ceiling([tmp_tbl_Model].[2025/2026]) AS [2025/2026], 
Ceiling([tmp_tbl_Model].[2026/2027]) AS [2026/2027], 
Ceiling([tmp_tbl_Model].[2027/2028]) AS [2027/2028], 
Ceiling([tmp_tbl_Model].[2028/2029]) AS [2028/2029], 
Ceiling([tmp_tbl_Model].[2029/2030]) AS [2029/2030], 
Ceiling([tmp_tbl_Model].[2030/2031]) AS [2030/2031], 
Ceiling([tmp_tbl_Model].[2031/2032]) AS [2031/2032], 
Ceiling([tmp_tbl_Model].[2032/2033]) AS [2032/2033], 
Ceiling([tmp_tbl_Model].[2033/2034]) AS [2033/2034], 
Ceiling([tmp_tbl_Model].[2034/2035]) AS [2034/2035], 
(Abs([tmp_tbl_Model].[2023/2024]-[tmp_tbl_QI].[2023/2024])/[tmp_tbl_QI].[2023/2024]) AS [Quality Indicator], 
IIf(IsNull([tmp_tbl_Model_Inc_Private_Inst].[2023/2024]),0,[tmp_tbl_Model].[2023/2024]/[tmp_tbl_Model_Inc_Private_Inst].[2023/2024]) AS [Coverage Indicator]
FROM ((tmp_tbl_Model 
LEFT JOIN tmp_tbl_QI ON tmp_tbl_Model.Expr1 = tmp_tbl_QI.Expr1) 
LEFT JOIN tmp_tbl_Model_Inc_Private_Inst ON tmp_tbl_Model.Expr1 = tmp_tbl_Model_Inc_Private_Inst.Expr1) 
LEFT JOIN T_Suppression_Public_Release_NOC ON (tmp_tbl_Model.Age_Group_Rollup_Label = T_Suppression_Public_Release_NOC.Age_Group_Rollup_Label) 
AND (tmp_tbl_Model.NOC = T_Suppression_Public_Release_NOC.NOC_CD)
WHERE (((tmp_tbl_Model.NOC_Level)=5) 
AND ((tmp_tbl_Model.NOC)<>'99999') 
AND ((T_Suppression_Public_Release_NOC.Age_Group_Rollup_Label) Is Null) 
AND ((T_Suppression_Public_Release_NOC.NOC_CD) Is Null))
ORDER BY tmp_tbl_Model.Age_Group_Rollup_Label, tmp_tbl_Model.NOC, 2, 5, 4, 8;"
#  dbExecute(decimal_con, qry_10a_Model_QI_PPCI) # gives rounded 5-digit NOC output with quality indicator and coverage indicator as calculated percentages-internal use only

# [SQL]


# ---- qry_10a_Model_QI_PPCI_No_Supp ----
qry_10a_Model_QI_PPCI_No_Supp <- 
"SELECT tmp_tbl_Model.Expr1, 
tmp_tbl_Model.Age_Group_Rollup_Label, 
--tmp_tbl_Model.SKILL_LEVEL_CATEGORY_CODE, 
tmp_tbl_Model.NOC_Level, 
--tmp_tbl_Model.NOC_SKILL_TYPE, 
tmp_tbl_Model.NOC, 
tmp_tbl_Model.ENGLISH_NAME, 
tmp_tbl_Model.Current_Region_PSSM_Code_Rollup, 
tmp_tbl_Model.Current_Region_PSSM_Name_Rollup, 
Ceiling([tmp_tbl_Model].[2023/2024]) AS [2023/2024], 
Ceiling([tmp_tbl_Model].[2024/2025]) AS [2024/2025], 
Ceiling([tmp_tbl_Model].[2025/2026]) AS [2025/2026],
Ceiling([tmp_tbl_Model].[2026/2027]) AS [2026/2027], 
Ceiling([tmp_tbl_Model].[2027/2028]) AS [2027/2028], 
Ceiling([tmp_tbl_Model].[2028/2029]) AS [2028/2029],
Ceiling([tmp_tbl_Model].[2029/2030]) AS [2029/2030],
Ceiling([tmp_tbl_Model].[2030/2031]) AS [2030/2031], 
Ceiling([tmp_tbl_Model].[2031/2032]) AS [2031/2032], 
Ceiling([tmp_tbl_Model].[2032/2033]) AS [2032/2033], 
Ceiling([tmp_tbl_Model].[2033/2034]) AS [2033/2034], 
Ceiling([tmp_tbl_Model].[2034/2035]) AS [2034/2035], 
IIf((Abs([tmp_tbl_Model].[2023/2024]-[tmp_tbl_QI].[2023/2024])/[tmp_tbl_QI].[2023/2024])<0.25,(Abs([tmp_tbl_Model].[2023/2024]-[tmp_tbl_QI].[2023/2024])/[tmp_tbl_QI].[2023/2024]),
IIf([tmp_tbl_Model].[2023/2024]<10 
Or [tmp_tbl_QI].[2023/2024]<10 
Or [tmp_tbl_Model].[2023/2024]=Null 
Or [tmp_tbl_QI].[2023/2024]=Null,'999999999',
(Abs([tmp_tbl_Model].[2023/2024]-[tmp_tbl_QI].[2023/2024])/[tmp_tbl_QI].[2023/2024]))) AS [Quality Indicator], 
IIf(
IsNull([tmp_tbl_Model_Inc_Private_Inst].[2023/2024],0)=0,0,
[tmp_tbl_Model].[2023/2024]/[tmp_tbl_Model_Inc_Private_Inst].[2023/2024]) AS [Coverage Indicator]
FROM (tmp_tbl_Model LEFT JOIN tmp_tbl_QI ON tmp_tbl_Model.Expr1 = tmp_tbl_QI.Expr1) 
LEFT JOIN tmp_tbl_Model_Inc_Private_Inst ON tmp_tbl_Model.Expr1 = tmp_tbl_Model_Inc_Private_Inst.Expr1
WHERE (((tmp_tbl_Model.NOC_Level)=5))
ORDER BY 2,4,6;"
#  dbExecute(decimal_con, qry_10a_Model_QI_PPCI_No_Supp) # for internal use release only-no suppression applied; LMIO needs it to work on the Labour Market Outlook 

# [SQL]


# ---- qry_10a_Model_QI_PPCI_Suppressed ----
qry_10a_Model_QI_PPCI_Suppressed <- 
"SELECT tmp_tbl_Model.Age_Group_Rollup_Label, 
--tmp_tbl_Model.SKILL_LEVEL_CATEGORY_CODE, 
--tmp_tbl_Model.NOC_SKILL_TYPE, 
tmp_tbl_Model.NOC, 
tmp_tbl_Model.ENGLISH_NAME, 
tmp_tbl_Model.Current_Region_PSSM_Code_Rollup, 
tmp_tbl_Model.Current_Region_PSSM_Name_Rollup, 
Ceiling(Sum([tmp_tbl_Model].[2023/2024])) AS [2023/2024], 
Ceiling(Sum([tmp_tbl_Model].[2024/2025])) AS [2024/2025], 
Ceiling(Sum([tmp_tbl_Model].[2025/2026])) AS [2025/2026], 
Ceiling(Sum([tmp_tbl_Model].[2026/2027])) AS [2026/2027], 
Ceiling(Sum([tmp_tbl_Model].[2027/2028])) AS [2027/2028], 
Ceiling(Sum([tmp_tbl_Model].[2028/2029])) AS [2028/2029], 
Ceiling(Sum([tmp_tbl_Model].[2029/2030])) AS [2029/2030], 
Ceiling(Sum([tmp_tbl_Model].[2030/2031])) AS [2030/2031], 
Ceiling(Sum([tmp_tbl_Model].[2031/2032])) AS [2031/2032], 
Ceiling(Sum([tmp_tbl_Model].[2032/2033])) AS [2032/2033], 
Ceiling(Sum([tmp_tbl_Model].[2033/2034])) AS [2033/2034], 
Ceiling(Sum([tmp_tbl_Model].[2034/2035])) AS [2034/2035], 
qry_10a_Model_QI_PPCI.Expr1
FROM tmp_tbl_Model LEFT JOIN qry_10a_Model_QI_PPCI ON tmp_tbl_Model.Expr1 = qry_10a_Model_QI_PPCI.Expr1
WHERE (((tmp_tbl_Model.NOC_Level)=5))
GROUP BY tmp_tbl_Model.Age_Group_Rollup_Label, tmp_tbl_Model.SKILL_LEVEL_CATEGORY_CODE, 
tmp_tbl_Model.NOC_SKILL_TYPE, tmp_tbl_Model.NOC, 
tmp_tbl_Model.ENGLISH_NAME, 
tmp_tbl_Model.Current_Region_PSSM_Code_Rollup, 
tmp_tbl_Model.Current_Region_PSSM_Name_Rollup, 
qry_10a_Model_QI_PPCI.Expr1
HAVING (((qry_10a_Model_QI_PPCI.Expr1) Is Null))
ORDER BY tmp_tbl_Model.Age_Group_Rollup_Label, tmp_tbl_Model.NOC;"
# # dbExecute(decimal_con, qry_10a_Model_QI_PPCI_Suppressed) # shows the 5-digit NOCs that have been suppressed

# [SQL]


# ---- qry_10a_Model_QI_PPCI_Suppressed_Total ----
qry_10a_Model_QI_PPCI_Suppressed_Total <- 
"SELECT tmp_tbl_Model.Age_Group_Rollup_Label, 
--'N/A' AS [NOC Skill Level], 
tmp_tbl_Model.NOC_Level, 
--'N/A' AS [NOC Skill Type], 
'99998' AS [NOC 2021], 
'Other' AS [Occupation Description], 
tmp_tbl_Model.Current_Region_PSSM_Code_Rollup, 
tmp_tbl_Model.Current_Region_PSSM_Name_Rollup, 
Sum(Ceiling([tmp_tbl_Model].[2023/2024])) AS [2023/2024], 
Sum(Ceiling([tmp_tbl_Model].[2024/2025])) AS [2024/2025], 
Sum(Ceiling([tmp_tbl_Model].[2025/2026])) AS [2025/2026], 
Sum(Ceiling([tmp_tbl_Model].[2026/2027])) AS [2026/2027], 
Sum(Ceiling([tmp_tbl_Model].[2027/2028])) AS [2027/2028], 
Sum(Ceiling([tmp_tbl_Model].[2028/2029])) AS [2028/2029], 
Sum(Ceiling([tmp_tbl_Model].[2029/2030])) AS [2029/2030], 
Sum(Ceiling([tmp_tbl_Model].[2030/2031])) AS [2030/2031], 
Sum(Ceiling([tmp_tbl_Model].[2031/2032])) AS [2031/2032], 
Sum(Ceiling([tmp_tbl_Model].[2032/2033])) AS [2032/2033], 
Sum(Ceiling([tmp_tbl_Model].[2033/2034])) AS [2033/2034], 
Sum(Ceiling([tmp_tbl_Model].[2034/2035])) AS [2034/2035]
FROM tmp_tbl_Model LEFT JOIN qry_10a_Model_QI_PPCI ON tmp_tbl_Model.Expr1 = qry_10a_Model_QI_PPCI.Expr1
WHERE (((qry_10a_Model_QI_PPCI.Expr1) Is Null))
GROUP BY 
tmp_tbl_Model.Age_Group_Rollup_Label, 
tmp_tbl_Model.NOC_Level, 
--'N/A', 
'99998', 
'Other', 
tmp_tbl_Model.Current_Region_PSSM_Code_Rollup, 
tmp_tbl_Model.Current_Region_PSSM_Name_Rollup
HAVING (((tmp_tbl_Model.NOC_Level)=5));"
# # dbExecute(decimal_con, qry_10a_Model_QI_PPCI_Suppressed_Total) # sum of the 5-digit NOCs that have been suppressed
# dbGetQuery(decimal_con, "SELECT * FROM qry_10a_Model_QI_PPCI_No_Supp") %>% 
#    write_csv(glue::glue("{lan}/reports-final/drafts/internal_only_static_no_ptib.csv"))
#  

# [SQL]

# ---- qry_10b_Quality_Indicator ----
qry_10b_Quality_Indicator <- 
"SELECT tmp_tbl_Model.Expr1, 
tmp_tbl_Model.Age_Group_Rollup_Label, 
tmp_tbl_Model.NOC_Level, 
tmp_tbl_Model.NOC, 
tmp_tbl_Model.ENGLISH_NAME, 
tmp_tbl_Model.Current_Region_PSSM_Code_Rollup, 
tmp_tbl_Model.Current_Region_PSSM_Name_Rollup, 
tmp_tbl_QI.[2023/2024]
FROM tmp_tbl_Model LEFT JOIN tmp_tbl_QI 
ON tmp_tbl_Model.Expr1 = tmp_tbl_QI.Expr1
ORDER BY tmp_tbl_Model.Expr1;"
# dbExecute(decimal_con, qry_10b_Quality_Indicator) 

# [SQL]


# ---- qry_10c_Coverage_Indicator ----
qry_10c_Coverage_Indicator <- 
"SELECT tmp_tbl_Model.Expr1, 
tmp_tbl_Model.Age_Group_Rollup_Label, 
tmp_tbl_Model.NOC_Level, 
tmp_tbl_Model.NOC, 
tmp_tbl_Model.ENGLISH_NAME, 
tmp_tbl_Model.Current_Region_PSSM_Code_Rollup, 
tmp_tbl_Model.Current_Region_PSSM_Name_Rollup, 
tmp_tbl_Model_Inc_Private_Inst.[2023/2024] AS Expr2
FROM tmp_tbl_Model 
LEFT JOIN tmp_tbl_Model_Inc_Private_Inst 
  ON tmp_tbl_Model.Expr1 = tmp_tbl_Model_Inc_Private_Inst.Expr1
ORDER BY tmp_tbl_Model.Expr1;"
# dbExecute(decimal_con, qry_10c_Coverage_Indicator) 

# [SQL]


# ---- qry_10d_tmp_No_Near_Completers ----
qry_10d_tmp_No_Near_Completers <- 
"SELECT [tmp_tbl_Model_2017-05-16].Expr1, [tmp_tbl_Model_2017-05-16].Age_Group_Rollup_Label, 
[tmp_tbl_Model_2017-05-16].PSSM_Skill_Level, [tmp_tbl_Model_2017-05-16].SKILL_LEVEL_CATEGORY_CODE, 
[tmp_tbl_Model_2017-05-16].NOC_Level, [tmp_tbl_Model_2017-05-16].NOC_SKILL_TYPE, 
[tmp_tbl_Model_2017-05-16].NOC, [tmp_tbl_Model_2017-05-16].ENGLISH_NAME, 
[tmp_tbl_Model_2017-05-16].Current_Region_PSSM_Code_Rollup, 
[tmp_tbl_Model_2017-05-16].Current_Region_PSSM_Name_Rollup, 
[tmp_tbl_Model_2017-05-16].[2015/2016], 
[tmp_tbl_Model_2017-06-08_No_Near_Completers].[2015/2016], 
[tmp_tbl_Model_2017-05-16].[2016/2017], 
[tmp_tbl_Model_2017-06-08_No_Near_Completers].[2016/2017], 
[tmp_tbl_Model_2017-05-16].[2017/2018], 
[tmp_tbl_Model_2017-06-08_No_Near_Completers].[2017/2018], 
[tmp_tbl_Model_2017-05-16].[2018/2019], 
[tmp_tbl_Model_2017-06-08_No_Near_Completers].[2018/2019], 
[tmp_tbl_Model_2017-05-16].[2019/2020], 
[tmp_tbl_Model_2017-06-08_No_Near_Completers].[2019/2020], 
[tmp_tbl_Model_2017-05-16].[2020/2021], 
[tmp_tbl_Model_2017-06-08_No_Near_Completers].[2020/2021], 
[tmp_tbl_Model_2017-05-16].[2021/2022], 
[tmp_tbl_Model_2017-06-08_No_Near_Completers].[2021/2022], 
[tmp_tbl_Model_2017-05-16].[2022/2023], 
[tmp_tbl_Model_2017-06-08_No_Near_Completers].[2022/2023], 
[tmp_tbl_Model_2017-05-16].[2023/2024], 
[tmp_tbl_Model_2017-06-08_No_Near_Completers].[2023/2024], 
[tmp_tbl_Model_2017-05-16].[2024/2025], 
[tmp_tbl_Model_2017-06-08_No_Near_Completers].[2024/2025], 
[tmp_tbl_Model_2017-05-16].[2025/2026], 
[tmp_tbl_Model_2017-06-08_No_Near_Completers].[2025/2026], 
[tmp_tbl_Model_2017-05-16].[2026/2027], 
[tmp_tbl_Model_2017-06-08_No_Near_Completers].[2026/2027]
FROM [tmp_tbl_Model_2017-05-16] LEFT JOIN [tmp_tbl_Model_2017-06-08_No_Near_Completers] 
ON [tmp_tbl_Model_2017-05-16].Expr1 = [tmp_tbl_Model_2017-06-08_No_Near_Completers].Expr1
ORDER BY 2, 7, 5, 3, 9;"
# # dbExecute(decimal_con, qry_10d_tmp_No_Near_Completers) 
# 
#  

# [SQL]


# ---- qry_LCIP4_CRED ----
qry_LCIP4_CRED <- 
"SELECT Occupation_Distributions.LCIP4_CRED
FROM Occupation_Distributions
GROUP BY Occupation_Distributions.LCIP4_CRED
ORDER BY Occupation_Distributions.LCIP4_CRED;"
# dbGetQuery(decimal_con, qry_LCIP4_CRED) 

# [SQL]


# ---- qry_LCIP4_CRED_Filtered_NOC ----
qry_LCIP4_CRED_Filtered_NOC <- 
"SELECT TOP 10 Occupation_Distributions.LCIP4_CRED, Occupation_Distributions.NOC, Occupation_Distributions.Count, Occupation_Distributions.[percent]
FROM Occupation_Distributions INNER JOIN qry_LCIP4_CRED ON Occupation_Distributions.LCIP4_CRED=qry_LCIP4_CRED.LCIP4_CRED
WHERE (((Occupation_Distributions.Count) In (Select Top 3 [Count] From Occupation_Distributions WHERE [Occupation_Distributions].[LCIP4_CRED]=[qry_LCIP4_CRED].[LCIP4_CRED] Order By [Count] Desc)))
ORDER BY Occupation_Distributions.[percent] DESC;"
# #dbGetQuery(decimal_con, qry_LCIP4_CRED_Filtered_NOC) 

# [SQL]


# ---- qry_LCIP4_CRED_NOC ----
qry_LCIP4_CRED_NOC <- 
"SELECT Occupation_Distributions.LCIP4_CRED, Occupation_Distributions.NOC
FROM Occupation_Distributions
GROUP BY Occupation_Distributions.LCIP4_CRED, Occupation_Distributions.NOC
ORDER BY Occupation_Distributions.LCIP4_CRED;"
# dbGetQuery(decimal_con, qry_LCIP4_CRED_NOC) 

# [SQL]


# ---- qry100_Grad_Skill_Level ----
qry100_Grad_Skill_Level <- 
"SELECT Q_4_NOC_4D_Totals_by_PSSM_CRED.PSSM_Skill_Level AS Expr1, Sum(Q_4_NOC_4D_Totals_by_PSSM_CRED.APPRAPPR) AS SumOfAPPRAPPR, Sum(Q_4_NOC_4D_Totals_by_PSSM_CRED.APPRCERT) AS SumOfAPPRCERT, Sum(Q_4_NOC_4D_Totals_by_PSSM_CRED.[1 - CERT]) AS [SumOf1 - CERT], Sum(Q_4_NOC_4D_Totals_by_PSSM_CRED.[1 - DIPL]) AS [SumOf1 - DIPL], Sum(Q_4_NOC_4D_Totals_by_PSSM_CRED.[1 - ADGR or UT]) AS [SumOf1 - ADGR or UT], Sum(Q_4_NOC_4D_Totals_by_PSSM_CRED.[1 - ADCT or ADIP]) AS [SumOf1 - ADCT or ADIP], Sum(Q_4_NOC_4D_Totals_by_PSSM_CRED.BACH) AS SumOfBACH, Sum(Q_4_NOC_4D_Totals_by_PSSM_CRED.[1 - PDCT or PDDP]) AS [SumOf1 - PDCT or PDDP], Sum(Q_4_NOC_4D_Totals_by_PSSM_CRED.MAST) AS SumOfMAST, Sum(Q_4_NOC_4D_Totals_by_PSSM_CRED.DOCT) AS SumOfDOCT, Sum(Q_4_NOC_4D_Totals_by_PSSM_CRED.[2 - CERT]) AS [SumOf2 - CERT], Sum(Q_4_NOC_4D_Totals_by_PSSM_CRED.[2 - DIPL]) AS [SumOf2 - DIPL], Sum(Q_4_NOC_4D_Totals_by_PSSM_CRED.[2 - ADGR or UT]) AS [SumOf2 - ADGR or UT], Sum(Q_4_NOC_4D_Totals_by_PSSM_CRED.[2 - ADCT or ADIP]) AS [SumOf2 - ADCT or ADIP], Sum(Q_4_NOC_4D_Totals_by_PSSM_CRED.[2 - PDCT or PDDP]) AS [SumOf2 - PDCT or PDDP]
FROM Q_4_NOC_4D_Totals_by_PSSM_CRED
GROUP BY Q_4_NOC_4D_Totals_by_PSSM_CRED.PSSM_Skill_Level;"
# # dbExecute(decimal_con, qry100_Grad_Skill_Level) 
# 
# # ---- public release ----

# [SQL]


# ---- qry99_Presentations_Graduates_Appendix ----
qry99_Presentations_Graduates_Appendix <- 
"SELECT Age_Group_Rollup_Label, PSSM_Credential_Name, 
[2023/2024], 
[2024/2025], 
[2025/2026], 
[2026/2027], 
[2027/2028], 
[2028/2029], 
[2029/2030], 
[2030/2031],
[2031/2032],
[2032/2033],
[2033/2034],
[2034/2035]
FROM (
SELECT Q_1c_Grad_Projections_by_Program.Age_Group_Rollup_Label, 
Q_1c_Grad_Projections_by_Program.Year as yr,
T_PSSM_Credential_Grouping_Appendix.PSSM_Credential_Name, 
Grads
FROM T_PSSM_Credential_Grouping_Appendix 
INNER JOIN Q_1c_Grad_Projections_by_Program 
	ON T_PSSM_Credential_Grouping_Appendix.PSSM_Credential = Q_1c_Grad_Projections_by_Program.PSSM_Credential
WHERE (((Q_1c_Grad_Projections_by_Program.PSSM_CRED) Not Like 'P - %'))
) AS SourceTable
PIVOT (
    Sum([Grads]) FOR Yr IN ([2023/2024], 
[2024/2025], 
[2025/2026], 
[2026/2027], 
[2027/2028], 
[2028/2029], 
[2029/2030], 
[2030/2031],
[2031/2032],
[2032/2033],
[2033/2034],
[2034/2035])
) AS PivotTable;"
# dbGetQuery(decimal_con, qry99_Presentations_Graduates_Appendix) %>% 
#   mutate(across(where(is.numeric), round)) %>%
#   write_csv(glue::glue("{lan}/reports-final/drafts/graduate_projections_noc_2021_static_incl_ptib.csv"))
# 
# 

# [SQL]

# ---- qry99_Presentations_Graduates_Appendix_by_Age_Group_Totals ----
qry99_Presentations_Graduates_Appendix_by_Age_Group_Totals <- 
  "SELECT Age_Group_Rollup_Label, 
[2023/2024], 
[2024/2025], 
[2025/2026], 
[2026/2027], 
[2027/2028], 
[2028/2029], 
[2029/2030], 
[2030/2031],
[2031/2032],
[2032/2033],
[2033/2034],
[2034/2035]
FROM (
SELECT Q_1c_Grad_Projections_by_Program.Age_Group_Rollup_Label, 
Q_1c_Grad_Projections_by_Program.Year as yr,
T_PSSM_Credential_Grouping_Appendix.PSSM_Credential_Name, 
Grads
FROM T_PSSM_Credential_Grouping_Appendix 
INNER JOIN Q_1c_Grad_Projections_by_Program 
	ON T_PSSM_Credential_Grouping_Appendix.PSSM_Credential = Q_1c_Grad_Projections_by_Program.PSSM_Credential
WHERE (((Q_1c_Grad_Projections_by_Program.PSSM_CRED) Not Like 'P - %'))
) AS SourceTable
PIVOT (
    Sum([Grads]) FOR Yr IN ([2023/2024], 
[2024/2025], 
[2025/2026], 
[2026/2027], 
[2027/2028], 
[2028/2029], 
[2029/2030], 
[2030/2031],
[2031/2032],
[2032/2033],
[2033/2034],
[2034/2035])
) AS PivotTable;"
# dbGetQuery(decimal_con, qry99_Presentations_Graduates_Appendix_by_Age_Group_Totals) 

# [SQL]


# ---- qry99_Presentations_Graduates_Appendix_Unrounded ----
qry99_Presentations_Graduates_Appendix_Unrounded <- 
"TRANSFORM Sum(Q_1c_Grad_Projections_by_Program.Grads) AS SumOfGrads
SELECT Q_1c_Grad_Projections_by_Program.Age_Group_Rollup_Label, T_PSSM_Credential_Grouping_Appendix.PSSM_Credential_Name
FROM T_PSSM_Credential_Grouping_Appendix INNER JOIN Q_1c_Grad_Projections_by_Program ON T_PSSM_Credential_Grouping_Appendix.PSSM_Credential = Q_1c_Grad_Projections_by_Program.PSSM_Credential
WHERE (((Q_1c_Grad_Projections_by_Program.PSSM_CRED) Not Like 'P - %'))
GROUP BY Q_1c_Grad_Projections_by_Program.Age_Group_Rollup_Label, T_PSSM_Credential_Grouping_Appendix.PSSM_Credential_Name, T_PSSM_Credential_Grouping_Appendix.ORDER
ORDER BY Q_1c_Grad_Projections_by_Program.Age_Group_Rollup_Label, T_PSSM_Credential_Grouping_Appendix.ORDER
PIVOT Q_1c_Grad_Projections_by_Program.Year;"
# # dbExecute(decimal_con, qry99_Presentations_Graduates_Appendix_Unrounded) 

# [SQL]


# ---- qry99_Presentations_Graduates_Including_those_not_projected ----
qry99_Presentations_Graduates_Including_those_not_projected <- 
"SELECT T_PSSM_CRED_RECODE.PSSM_CRED_Group, Sum(Graduate_Projections.Graduates) AS SumOfGraduates
FROM Graduate_Projections INNER JOIN T_PSSM_CRED_RECODE ON Graduate_Projections.PSSM_CRED = T_PSSM_CRED_RECODE.PSSM_CRED
WHERE ((((Graduate_Projections.Year)='2015/2016' Or (Graduate_Projections.Year)='2016/2017')=False) And (((Graduate_Projections.Age_Group)='15 to 16' Or (Graduate_Projections.Age_Group)='65 to 89')=False) And ((Graduate_Projections.PSSM_CRED) Not Like 'P - %'))
GROUP BY T_PSSM_CRED_RECODE.PSSM_CRED_Group, T_PSSM_CRED_RECODE.ORDER
ORDER BY T_PSSM_CRED_RECODE.ORDER DESC;"
# # dbExecute(decimal_con, qry99_Presentations_Graduates_Including_those_not_projected) 

# [SQL]


# ---- qry99_Presentations_Labour_Force ----
qry99_Presentations_Labour_Force <- 
"SELECT tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.PSSM_Credential, tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.PSSM_CRED, tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.Age_Group_Rollup, tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.Age_Group_Rollup_Label, tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.Year, tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.LCP4_CD, tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.LCIP4_CRED, Sum(tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.New_Labour_Supply) AS SumOfNew_Labour_Supply, T_Current_Region_PSSM_Rollup_Codes_BC.Current_Region_PSSM_Code_Rollup, Sum(tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.NLS) AS SumOfNLS
FROM tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union INNER JOIN T_Current_Region_PSSM_Rollup_Codes_BC ON tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.Current_Region_PSSM_Code_Rollup = T_Current_Region_PSSM_Rollup_Codes_BC.Current_Region_PSSM_Code_Rollup
WHERE (((tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.PSSM_CRED) Not Like 'P - %'))
GROUP BY tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.PSSM_Credential, tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.PSSM_CRED, tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.Age_Group_Rollup, tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.Age_Group_Rollup_Label, tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.Year, tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.LCP4_CD, tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.LCIP4_CRED, T_Current_Region_PSSM_Rollup_Codes_BC.Current_Region_PSSM_Code_Rollup;"
# # dbExecute(decimal_con, qry99_Presentations_Labour_Force) 

# [SQL]


# ---- qry99_Presentations_Labour_Force_BC ----
qry99_Presentations_Labour_Force_BC <- 
"SELECT T_PSSM_CRED_RECODE.PSSM_CRED_Group, Sum(tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.NLS) AS SumOfNLS
FROM ((tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union INNER JOIN T_PSSM_CRED_RECODE ON tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.PSSM_CRED = T_PSSM_CRED_RECODE.PSSM_CRED) INNER JOIN T_Current_Region_PSSM_Rollup_Codes_BC ON tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.Current_Region_PSSM_Code_Rollup = T_Current_Region_PSSM_Rollup_Codes_BC.Current_Region_PSSM_Code_Rollup) INNER JOIN T_Current_Region_PSSM_Rollup_Codes ON T_Current_Region_PSSM_Rollup_Codes_BC.Current_Region_PSSM_Code_Rollup_BC = T_Current_Region_PSSM_Rollup_Codes.Current_Region_PSSM_Code_Rollup
WHERE (((tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.PSSM_CRED) Not Like 'P - %'))
GROUP BY T_PSSM_CRED_RECODE.PSSM_CRED_Group, T_PSSM_CRED_RECODE.ORDER
ORDER BY T_PSSM_CRED_RECODE.ORDER DESC;"
# # dbExecute(decimal_con, qry99_Presentations_Labour_Force_BC) 

# [SQL]


# ---- qry99_Presentations_Labour_Force_Overall ----
qry99_Presentations_Labour_Force_Overall <- 
"SELECT T_PSSM_CRED_RECODE.PSSM_CRED_Group, Sum(tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.NLS) AS SumOfNLS
FROM tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union INNER JOIN T_PSSM_CRED_RECODE ON tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.PSSM_CRED = T_PSSM_CRED_RECODE.PSSM_CRED
WHERE (((tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.PSSM_CRED) Not Like 'P - %'))
GROUP BY T_PSSM_CRED_RECODE.PSSM_CRED_Group, T_PSSM_CRED_RECODE.ORDER
ORDER BY T_PSSM_CRED_RECODE.ORDER DESC;"
# # dbExecute(decimal_con, qry99_Presentations_Labour_Force_Overall) 

# [SQL]


# ---- qry99_Presentations_Occs ----
qry99_Presentations_Occs <- 
"SELECT tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.PSSM_Credential, tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.PSSM_CRED, tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.Age_Group_Rollup, tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.Age_Group_Rollup_Label, tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.Year, tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.LCP4_CD, tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.LCIP4_CRED, T_Current_Region_PSSM_Rollup_Codes.Current_Region_PSSM_Code_Rollup, T_Current_Region_PSSM_Rollup_Codes.Current_Region_PSSM_Name_Rollup, tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.NOC, Sum(tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.OccsN) AS SumOfOccsN
FROM (tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union INNER JOIN T_Current_Region_PSSM_Rollup_Codes_BC ON tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.Current_Region_PSSM_Code_Rollup = T_Current_Region_PSSM_Rollup_Codes_BC.Current_Region_PSSM_Code_Rollup) INNER JOIN T_Current_Region_PSSM_Rollup_Codes ON T_Current_Region_PSSM_Rollup_Codes_BC.Current_Region_PSSM_Code_Rollup_BC = T_Current_Region_PSSM_Rollup_Codes.Current_Region_PSSM_Code_Rollup
GROUP BY tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.PSSM_Credential, tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.PSSM_CRED, tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.Age_Group_Rollup, tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.Age_Group_Rollup_Label, tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.Year, tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.LCP4_CD, tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.LCIP4_CRED, T_Current_Region_PSSM_Rollup_Codes.Current_Region_PSSM_Code_Rollup, T_Current_Region_PSSM_Rollup_Codes.Current_Region_PSSM_Name_Rollup, tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.NOC
HAVING (((tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.PSSM_CRED) Not Like 'P - %'));"
# # dbExecute(decimal_con, qry99_Presentations_Occs) 

# [SQL]


# ---- qry99_Presentations_PPSCI_Graduates ----
qry99_Presentations_PPSCI_Graduates <- 
"SELECT T_PSSM_CRED_RECODE.PSSM_CRED_Group, Sum(Graduate_Projections.Graduates) AS SumOfGraduates
FROM Graduate_Projections INNER JOIN T_PSSM_CRED_RECODE ON Graduate_Projections.PSSM_CRED = T_PSSM_CRED_RECODE.PSSM_CRED
GROUP BY T_PSSM_CRED_RECODE.PSSM_CRED_Group, T_PSSM_CRED_RECODE.ORDER, Graduate_Projections.Year
HAVING (((Graduate_Projections.Year)='2019/2020'))
ORDER BY T_PSSM_CRED_RECODE.ORDER;"
# # dbExecute(decimal_con, qry99_Presentations_PPSCI_Graduates) 

# [SQL]

# ---- qry9999_NOC_4031_4032 ----
qry9999_NOC_4031_4032 <- 
"SELECT [tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union_2017-05-16].PSSM_Credential AS Expr1, [tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union_2017-05-16].PSSM_CRED AS Expr2, [tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union_2017-05-16].Age_Group_Rollup AS Expr3, [tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union_2017-05-16].Age_Group_Rollup_Label AS Expr4, [tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union_2017-05-16].Year AS Expr5, [tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union_2017-05-16].NOC AS Expr6, Sum([tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union_2017-05-16].OccsN) AS SumOfOccsN
FROM [tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union_2017-05-16]
WHERE ((([tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union_2017-05-16].[Current_Region_PSSM_Code_Rollup])<>9910 And ([tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union_2017-05-16].[Current_Region_PSSM_Code_Rollup])<>9911 And ([tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union_2017-05-16].[Current_Region_PSSM_Code_Rollup])<>9999))
GROUP BY [tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union_2017-05-16].PSSM_Credential, [tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union_2017-05-16].PSSM_CRED, [tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union_2017-05-16].Age_Group_Rollup, [tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union_2017-05-16].Age_Group_Rollup_Label, [tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union_2017-05-16].Year, [tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union_2017-05-16].NOC
HAVING ((([tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union_2017-05-16].Age_Group_Rollup)=3) And (([tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union_2017-05-16].Year)='2015/2016') And (([tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union_2017-05-16].NOC)='4031' Or ([tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union_2017-05-16].NOC)='4032'))
ORDER BY [tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union_2017-05-16].NOC;"
# # dbExecute(decimal_con, qry9999_NOC_4031_4032) 

# ---- Clean Up ----
dbExecute(decimal_con, "DROP TABLE tmp_tbl_Q3b12_Occupations_by_LCIP4_CRED_No_TT_Union_tmp")
dbExecute(decimal_con, "DROP TABLE tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union")
dbExecute(decimal_con, "DROP TABLE tmp_tbl_Q_2a4_Labour_Supply_by_LCIP4_CRED_No_TT_Union_tmp")
dbExecute(decimal_con, "DROP TABLE tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union_tmp")
dbExecute(decimal_con, "DROP TABLE tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union_tmp")
dbExecute(decimal_con, "DROP TABLE tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union")

dbExecute(decimal_con, "DROP TABLE Q_1_Grad_Projections_by_Age_by_Program")
dbExecute(decimal_con, "DROP TABLE Q_1_Grad_Projections_by_Age_by_Program_Static")
dbExecute(decimal_con, "DROP TABLE Q_1c_Grad_Projections_by_Program_LCP2")
dbExecute(decimal_con, "DROP TABLE Q_1c_Grad_Projections_by_Program")

# Lookups
dbExecute(decimal_con, "drop table INFOWARE_L_CIP_4DIGITS_CIP2016")
dbExecute(decimal_con, "drop table INFOWARE_L_CIP_6DIGITS_CIP2016")
#dbExecute(decimal_con, "DROP TABLE T_NOC_Skill_Type")
#dbExecute(decimal_con, "DROP TABLE tbl_NOC_Skill_Level_Aged_17_34")
dbExecute(decimal_con, "DROP TABLE T_Current_Region_PSSM_Rollup_Codes")
dbExecute(decimal_con, "DROP TABLE T_Current_Region_PSSM_Rollup_Codes_BC")
dbExecute(decimal_con, "DROP TABLE T_PSSM_CRED_RECODE")
dbExecute(decimal_con, "DROP TABLE T_Exclude_from_Projections_LCP4_CD")
dbExecute(decimal_con, "DROP TABLE T_Exclude_from_Projections_LCIP4_CRED")
dbExecute(decimal_con, "DROP TABLE T_Exclude_from_Projections_PSSM_Credential")
dbExecute(decimal_con, "DROP TABLE tbl_Age_Groups")
dbExecute(decimal_con, "DROP TABLE tbl_Age_Groups_Rollup")
dbExecute(decimal_con, "DROP TABLE T_Exclude_from_Labour_Supply_Unknown_LCP2_Proxy")
}
# Keep 
# dbExists(decimal_con, "")
# dbExists(decimal_con, "")