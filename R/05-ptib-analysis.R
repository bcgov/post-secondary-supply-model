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

# ******************************************************************************
# Private Training Institutions Branch (PTIB)
# 
# Required Tables
#   T_Private_Institutions_Credentials_Raw
#   T_PSSM_Credential_Grouping
#   INFOWARE_L_CIP_6DIGITS_CIP2016
#   T_PTIB_Y1_to_Y10
#
# Resulting Tables
#   qry_Private_Credentials_05i1_Grads_by_Year (PTIB data for Graduate_Projections)
#   qry_Private_Credentials_06d1_Cohort_Dist (PTIB data for Cohort_Program_Distributions_Projected & _Static)
#
# Part 1: Clean PTIB data
# * Update age groups, CIPs
# * Add and update exclude column
#
# Part 2: Domestic graduates
#
# Part 3: Cohort distributions
#
# ******************************************************************************

library(RODBC)
library(arrow)
library(tidyverse)
library(odbc)
library(RJDBC) ## loads DBI

# Setup
# ---- Configure LAN and file paths ----
lan <- config::get("lan")
my_schema <- config::get("myschema")

# ---- Connection to database ----
db_config <- config::get("decimal")
decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")

# import sql queries
## ** IMPORTANT - update queries with table years **
# ******************************************************************************

# ---- Required data tables and SQL ----
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_PSSM_Credential_Grouping"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_Private_Institutions_Credentials_Raw"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."T_PTIB_Y1_to_Y10"')))
dbExistsTable(decimal_con, SQL(glue::glue('"{my_schema}"."INFOWARE_L_CIP_6DIGITS_CIP2016"')))

# Part 1 ----
## ---- Add PSSM_Credential to PTIB data ----

# [SELECT INTO] Create T_Private_Institutions_Credentials from T_PSSM_Credential_Grouping
# ******************************************************************************
# Part 1 ----

## qry_Private_Credentials_00a_Append ----
# grab PSSM_Credential and add to PTIB data; drop other and none credential
qry_Private_Credentials_00a_Append <-
"SELECT intYear = T_Private_Institutions_Credentials_Raw.Year,
Credential = T_PSSM_Credential_Grouping.PSSM_Credential,
LCIP_CD = T_Private_Institutions_Credentials_Raw.CIP,
Age_Group = T_Private_Institutions_Credentials_Raw.Age_Group,
Immigration_Status = T_Private_Institutions_Credentials_Raw.Immigration_Status,
Graduates = T_Private_Institutions_Credentials_Raw.Sum_of_Graduates,
Enrolled_Not_Graduated = T_Private_Institutions_Credentials_Raw.Sum_of_Enrolments,
Enrolment = T_Private_Institutions_Credentials_Raw.Sum_of_Total_Enrolments
INTO T_Private_Institutions_Credentials
FROM T_PSSM_Credential_Grouping INNER JOIN T_Private_Institutions_Credentials_Raw
ON T_PSSM_Credential_Grouping.PRGM_Credential_Awarded_Name = T_Private_Institutions_Credentials_Raw.Credential
WHERE (((T_PSSM_Credential_Grouping.PSSM_Credential) Is Not Null)
AND ((T_Private_Institutions_Credentials_Raw.Credential)<>'None'))"
dbExecute(decimal_con, qry_Private_Credentials_00a_Append)

## ---- Check CIP length ----

# [SQL]

## qry_Private_Credentials_00b_Check_CIP_Length ----
# Check that all CIPs are 7 digits
qry_Private_Credentials_00b_Check_CIP_Length <-
  "SELECT T_Private_Institutions_Credentials.LCIP_CD, Len([LCIP_CD]) AS Expr1
FROM T_Private_Institutions_Credentials
WHERE (((Len([LCIP_CD]))<>7))"
dbGetQuery(decimal_con, qry_Private_Credentials_00b_Check_CIP_Length)

## ---- Remove periods from CIPs ----

# [UPDATE] T_Private_Institutions_Credentials

## qry_Private_Credentials_00c_Clean_CIP_Period ----
# remove periods from LCIP_CD
qry_Private_Credentials_00c_Clean_CIP_Period <-
"UPDATE T_Private_Institutions_Credentials
SET T_Private_Institutions_Credentials.LCIP_CD = Replace([LCIP_CD],'.','')"
dbExecute(decimal_con, qry_Private_Credentials_00c_Clean_CIP_Period)
dbGetQuery(decimal_con, qry_Private_Credentials_00b_Check_CIP_Length) %>% filter(Expr1!=6) # sanity check

## ---- Check CIPs against infoware 6digit CIPs ----

# [SQL]

## qry_Private_Credentials_00d_Check_CIPs ----
# Check CIPs valid; compared to the Infoware table
qry_Private_Credentials_00d_Check_CIPs <-
"SELECT T_Private_Institutions_Credentials.LCIP_CD,
INFOWARE_L_CIP_6DIGITS_CIP2016.LCIP_CD
FROM INFOWARE_L_CIP_6DIGITS_CIP2016 RIGHT JOIN T_Private_Institutions_Credentials
ON INFOWARE_L_CIP_6DIGITS_CIP2016.LCIP_CD = T_Private_Institutions_Credentials.LCIP_CD
WHERE (((INFOWARE_L_CIP_6DIGITS_CIP2016.LCIP_CD) Is Null));"
dbGetQuery(decimal_con, qry_Private_Credentials_00d_Check_CIPs)


## ---- Update Exclude column ----
# Flag not for credit and ESL programs and unclassified 99.9999
dbExecute(decimal_con, "ALTER TABLE T_Private_Institutions_Credentials 
                ADD  Exclude VARCHAR(255), LCIP_NAME VARCHAR(255)")

dbExecute(decimal_con, "UPDATE T_Private_Institutions_Credentials 
           SET T_Private_Institutions_Credentials.LCIP_NAME = INFOWARE_L_CIP_6DIGITS_CIP2016.LCIP_NAME
           FROM T_Private_Institutions_Credentials 
           INNER JOIN INFOWARE_L_CIP_6DIGITS_CIP2016
           ON T_Private_Institutions_Credentials.LCIP_CD = INFOWARE_L_CIP_6DIGITS_CIP2016.LCIP_CD")

dbExecute(decimal_con, "UPDATE T_Private_Institutions_Credentials 
           SET T_Private_Institutions_Credentials.Exclude = 1
           WHERE (((T_Private_Institutions_Credentials.LCIP_NAME) ='English as a second language') OR
          ((T_Private_Institutions_Credentials.LCIP_NAME) LIKE '%not for credit%') OR
          ((T_Private_Institutions_Credentials.LCIP_CD)='99999') );")

dbExecute(decimal_con, "ALTER TABLE T_Private_Institutions_Credentials
                DROP COLUMN LCIP_NAME;")


## ---- Update age groups ----

# [UPDATE] T_Private_Institutions_Credentials

## qry_Private_Credentials_00f_Recode_Age_Group ----
## recode dashes in age group
qry_Private_Credentials_00f_Recode_Age_Group <-
"UPDATE T_Private_Institutions_Credentials
SET T_Private_Institutions_Credentials.Age_Group = Replace([Age_Group],'-',' to ');"
dbExecute(decimal_con, qry_Private_Credentials_00f_Recode_Age_Group)

## ---- Fix immigration status ----
# what to do with unknowns? see documentation

## ---- Copy to Clean table ----
dbExecute(decimal_con, "SELECT *
                INTO T_Private_Institutions_Credentials_Clean
                FROM T_Private_Institutions_Credentials;")


## ---- Age averages ----
dbExecute(decimal_con, "ALTER TABLE T_Private_Institutions_Credentials
                ALTER COLUMN intYear VARCHAR(255);")

# check relevant years to update queries below
tbl(decimal_con, "T_Private_Institutions_Credentials") %>% collect() %>% 
  count(intYear)

## !! update DATA years in below queries


# [INSERT INTO] T_Private_Institutions_Credentials

## qry_Private_Credentials_00g_Avg ----
## Get averages of enrolments/graduates by credential/LCIP_CD/Age/Immigration by # years; remove excluded
qry_Private_Credentials_00g_Avg <-
"INSERT INTO T_Private_Institutions_Credentials ( intYear, Credential, LCIP_CD, Age_Group, Immigration_Status, Enrolment, Enrolled_Not_Graduated, Graduates, Exclude )
SELECT 'Avg 2021 & 2022' AS intYear,
T_Private_Institutions_Credentials_Clean.Credential,
T_Private_Institutions_Credentials_Clean.LCIP_CD,
T_Private_Institutions_Credentials_Clean.Age_Group,
T_Private_Institutions_Credentials_Clean.Immigration_Status,
Sum([Enrolment])/2 AS AvgOfEnrolment,
Sum([Enrolled_Not_Graduated])/2 AS AvgOfEnrolment_Not_Graduated,
Sum([Graduates])/2 AS AvgOfGraduates,
T_Private_Institutions_Credentials_Clean.Exclude
FROM T_Private_Institutions_Credentials_Clean
GROUP BY T_Private_Institutions_Credentials_Clean.Credential,
T_Private_Institutions_Credentials_Clean.LCIP_CD,
T_Private_Institutions_Credentials_Clean.Age_Group,
T_Private_Institutions_Credentials_Clean.Immigration_Status,
T_Private_Institutions_Credentials_Clean.Exclude
HAVING (((T_Private_Institutions_Credentials_Clean.Exclude) Is Null));"
dbExecute(decimal_con, qry_Private_Credentials_00g_Avg)
dbExecute(decimal_con, "DELETE FROM T_Private_Institutions_Credentials
                WHERE intYear <> 'Avg 2021 & 2022'")

# Part 2 ----
## STOP !!! Update MODEL year in queries ----

## ---- Count domestic grads ----

# [SELECT INTO] Create qry_Private_Credentials_01a_Domestic from T_Private_Institutions_Credentials
# ******************************************************************************

# Part 2 ----
## qry_Private_Credentials_01a_Domestic ----
# count domestic grads for each combination of age group, CIP and Credential
qry_Private_Credentials_01a_Domestic <-
"SELECT '2023/2024' AS [Year],
T_Private_Institutions_Credentials.Credential,
T_Private_Institutions_Credentials.LCIP_CD,
T_Private_Institutions_Credentials.Age_Group,
Sum(IIf([Immigration_Status]='Domestic',[Graduates],0)) AS Domestic
INTO qry_Private_Credentials_01a_Domestic
FROM T_Private_Institutions_Credentials
WHERE (((T_Private_Institutions_Credentials.Exclude) Is Null)
AND ((T_Private_Institutions_Credentials.Graduates) Is Not Null))
GROUP BY T_Private_Institutions_Credentials.Credential,
T_Private_Institutions_Credentials.LCIP_CD,
T_Private_Institutions_Credentials.Age_Group
HAVING (((T_Private_Institutions_Credentials.Credential)='CERT'
Or (T_Private_Institutions_Credentials.Credential)='DIPL'));"
dbExecute(decimal_con, qry_Private_Credentials_01a_Domestic)

## ---- Count domestic and international grads ----

# [SELECT INTO] Create qry_Private_Credentials_01b_Domestic_International from T_Private_Institutions_Credentials

## qry_Private_Credentials_01b_Domestic_International ----
# count domenstic and international grads for each combination of age group, CIP and Credential
qry_Private_Credentials_01b_Domestic_International <-
"SELECT '2023/2024' AS [Year],
T_Private_Institutions_Credentials.Credential,
T_Private_Institutions_Credentials.LCIP_CD,
T_Private_Institutions_Credentials.Age_Group,
Sum(T_Private_Institutions_Credentials.Graduates) AS Domestic_International
INTO qry_Private_Credentials_01b_Domestic_International
FROM T_Private_Institutions_Credentials
WHERE (((T_Private_Institutions_Credentials.Exclude) Is Null)
AND ((T_Private_Institutions_Credentials.Immigration_Status)='Domestic'
Or (T_Private_Institutions_Credentials.Immigration_Status)='International'
Or (T_Private_Institutions_Credentials.Immigration_Status)='#N/A')
AND ((T_Private_Institutions_Credentials.Graduates) Is Not Null))
GROUP BY T_Private_Institutions_Credentials.Credential,
T_Private_Institutions_Credentials.LCIP_CD, T_Private_Institutions_Credentials.Age_Group
HAVING (((T_Private_Institutions_Credentials.Credential)='CERT'
Or (T_Private_Institutions_Credentials.Credential)='DIPL'));"
dbExecute(decimal_con, qry_Private_Credentials_01b_Domestic_International)

## ---- Compute percent of domestic and international grads that are domestic ----

# [SELECT INTO] Create qry_Private_Credentials_01c_Percent_Domestic from qry_Private_Credentials_01a_Domestic

## qry_Private_Credentials_01c_Percent_Domestic ----
# Compute percent of domestic and international grads that are domestic
qry_Private_Credentials_01c_Percent_Domestic <-
"SELECT qry_Private_Credentials_01a_Domestic.Year,
qry_Private_Credentials_01a_Domestic.Credential,
qry_Private_Credentials_01a_Domestic.LCIP_CD,
qry_Private_Credentials_01a_Domestic.Age_Group,
qry_Private_Credentials_01a_Domestic.Domestic,
qry_Private_Credentials_01b_Domestic_International.Domestic_International,
IIf([Domestic]=0,0,[Domestic]/[Domestic_International]) AS [Percent_Domestic]
INTO qry_Private_Credentials_01c_Percent_Domestic
FROM qry_Private_Credentials_01a_Domestic INNER JOIN qry_Private_Credentials_01b_Domestic_International
ON (qry_Private_Credentials_01a_Domestic.Age_Group = qry_Private_Credentials_01b_Domestic_International.Age_Group)
AND (qry_Private_Credentials_01a_Domestic.LCIP_CD = qry_Private_Credentials_01b_Domestic_International.LCIP_CD)
AND (qry_Private_Credentials_01a_Domestic.Credential = qry_Private_Credentials_01b_Domestic_International.Credential)
AND (qry_Private_Credentials_01a_Domestic.Year = qry_Private_Credentials_01b_Domestic_International.Year);"
dbExecute(decimal_con, qry_Private_Credentials_01c_Percent_Domestic)

## ---- Compute unknown or blank immigration status ----

# [SELECT INTO] Create qry_Private_Credentials_01d_Grads_Blank from T_Private_Institutions_Credentials

## qry_Private_Credentials_01d_Grads_Blank ----
qry_Private_Credentials_01d_Grads_Blank <-
"SELECT [qry_Private_Credentials_01c_Percent_Domestic].Year,
T_Private_Institutions_Credentials.Credential,
T_Private_Institutions_Credentials.LCIP_CD,
T_Private_Institutions_Credentials.Age_Group,
[Graduates]*[Percent_Domestic] AS Graduates_Blank
INTO qry_Private_Credentials_01d_Grads_Blank
FROM T_Private_Institutions_Credentials INNER JOIN [qry_Private_Credentials_01c_Percent_Domestic]
ON (T_Private_Institutions_Credentials.Age_Group = [qry_Private_Credentials_01c_Percent_Domestic].Age_Group)
AND (T_Private_Institutions_Credentials.LCIP_CD = [qry_Private_Credentials_01c_Percent_Domestic].LCIP_CD)
AND (T_Private_Institutions_Credentials.Credential = [qry_Private_Credentials_01c_Percent_Domestic].Credential)
WHERE (((T_Private_Institutions_Credentials.Immigration_Status)='(blank)'
Or (T_Private_Institutions_Credentials.Immigration_Status)='Unknown')
AND ((T_Private_Institutions_Credentials.Exclude) Is Null));"
dbExecute(decimal_con, qry_Private_Credentials_01d_Grads_Blank)

## ---- Join domestic and blank ----

# [SELECT INTO] Create qry_Private_Credentials_01e_Grads_Union from qry_Private_Credentials_01a_Domestic

## qry_Private_Credentials_01e_Grads_Union ----
qry_Private_Credentials_01e_Grads_Union <- "
SELECT qry_Private_Credentials_01a_Domestic.Year,
qry_Private_Credentials_01a_Domestic.Credential,
qry_Private_Credentials_01a_Domestic.LCIP_CD,
qry_Private_Credentials_01a_Domestic.Age_Group,
qry_Private_Credentials_01a_Domestic.Domestic
INTO qry_Private_Credentials_01e_Grads_Union
FROM qry_Private_Credentials_01a_Domestic
UNION ALL SELECT qry_Private_Credentials_01d_Grads_Blank.Year,
qry_Private_Credentials_01d_Grads_Blank.Credential,
qry_Private_Credentials_01d_Grads_Blank.LCIP_CD,
qry_Private_Credentials_01d_Grads_Blank.Age_Group,
qry_Private_Credentials_01d_Grads_Blank.Graduates_Blank
FROM qry_Private_Credentials_01d_Grads_Blank;"
dbExecute(decimal_con, qry_Private_Credentials_01e_Grads_Union)

## ---- Sum of union query ----

# [SELECT INTO] Create qry_Private_Credentials_01f_Grads from qry_Private_Credentials_01e_Grads_Union

## qry_Private_Credentials_01f_Grads ----
# Sum of union query
qry_Private_Credentials_01f_Grads <- "
SELECT qry_Private_Credentials_01e_Grads_Union.Year,
qry_Private_Credentials_01e_Grads_Union.Credential,
qry_Private_Credentials_01e_Grads_Union.LCIP_CD,
qry_Private_Credentials_01e_Grads_Union.Age_Group,
Sum(qry_Private_Credentials_01e_Grads_Union.Domestic) AS Grads
INTO qry_Private_Credentials_01f_Grads
FROM qry_Private_Credentials_01e_Grads_Union
GROUP BY qry_Private_Credentials_01e_Grads_Union.Year,
qry_Private_Credentials_01e_Grads_Union.Credential,
qry_Private_Credentials_01e_Grads_Union.LCIP_CD,
qry_Private_Credentials_01e_Grads_Union.Age_Group;"
dbExecute(decimal_con, qry_Private_Credentials_01f_Grads)

## ---- Summarize the Grads by Credential/Age ----

# [SELECT INTO] Create qry_Private_Credentials_05i_Grads from qry_Private_Credentials_01f_Grads

## qry_Private_Credentials_05i_Grads ----
# Summarize the Grads by Credential/Age
qry_Private_Credentials_05i_Grads <-
  "SELECT qry_Private_Credentials_01f_Grads.Year,
qry_Private_Credentials_01f_Grads.Credential,
qry_Private_Credentials_01f_Grads.Age_Group,
Sum(qry_Private_Credentials_01f_Grads.Grads) AS SumOfGrads
INTO qry_Private_Credentials_05i_Grads
FROM qry_Private_Credentials_01f_Grads
GROUP BY qry_Private_Credentials_01f_Grads.Year,
qry_Private_Credentials_01f_Grads.Credential,
qry_Private_Credentials_01f_Grads.Age_Group;"
dbExecute(decimal_con, qry_Private_Credentials_05i_Grads)

## ---- Get Grads by all years, saved as table  ----

# [SELECT INTO] Create qry_Private_Credentials_05i1_Grads_by_Year from qry_Private_Credentials_05i_Grads

## qry_Private_Credentials_05i1_Grads_by_Year ----
# add Grads for all years to new table for import into Graduate_Projections later
qry_Private_Credentials_05i1_Grads_by_Year <-
  "SELECT 'PTIB' AS Survey,
'P - ' + [Credential] AS PSSM_CRED,
qry_Private_Credentials_05i_Grads.Age_Group,
T_PTIB_Y1_to_Y10.Y1_to_Y10 AS [Year],
qry_Private_Credentials_05i_Grads.SumOfGrads AS Graduates
INTO qry_Private_Credentials_05i1_Grads_by_Year
FROM qry_Private_Credentials_05i_Grads INNER JOIN T_PTIB_Y1_to_Y10
ON qry_Private_Credentials_05i_Grads.Year = T_PTIB_Y1_to_Y10.Y1;"
dbExecute(decimal_con, qry_Private_Credentials_05i1_Grads_by_Year)

## ---- Delete excess age groups ----

# [DELETE] qry_Private_Credentials_05i1_Grads_by_Year

## qry_Private_Credentials_05i2_Delete_AgeGrps ----
# remove excess age groups
qry_Private_Credentials_05i2_Delete_AgeGrps <-
  "DELETE FROM qry_Private_Credentials_05i1_Grads_by_Year
WHERE (((qry_Private_Credentials_05i1_Grads_by_Year.Survey)='PTIB') AND
((qry_Private_Credentials_05i1_Grads_by_Year.Age_Group)='(blank)') OR
((qry_Private_Credentials_05i1_Grads_by_Year.Age_Group)='Unknown') OR
((qry_Private_Credentials_05i1_Grads_by_Year.Age_Group)='65+') OR
((qry_Private_Credentials_05i1_Grads_by_Year.Age_Group)='16 or less'))"
dbExecute(decimal_con, qry_Private_Credentials_05i2_Delete_AgeGrps)

## ---- Update Graduate_Projections ----
dbExecute(decimal_con, "INSERT INTO Graduate_Projections ( Survey, PSSM_CRED, Age_Group, [Year], Graduates )
          SELECT Survey, PSSM_CRED, Age_Group, [Year], Graduates
          FROM qry_Private_Credentials_05i1_Grads_by_Year;")


## ---- Drop tmp part2 qry datasets ----
dbExecute(decimal_con, "DROP TABLE qry_Private_Credentials_01a_Domestic")
dbExecute(decimal_con, "DROP TABLE qry_Private_Credentials_01b_Domestic_International")
dbExecute(decimal_con, "DROP TABLE qry_Private_Credentials_01c_Percent_Domestic")
dbExecute(decimal_con, "DROP TABLE qry_Private_Credentials_01d_Grads_Blank")
dbExecute(decimal_con, "DROP TABLE qry_Private_Credentials_01e_Grads_Union")

# Part 3 ----
## ---- Counts grads by CIP ----

# [SELECT INTO] Create qry_Private_Credentials_06b_Cohort_Dist from qry_Private_Credentials_01f_Grads

# ******************************************************************************

# Part 3 ----
## qry_Private_Credentials_06b_Cohort_Dist ----
# count grads by CIP
qry_Private_Credentials_06b_Cohort_Dist <-
  "SELECT qry_Private_Credentials_01f_Grads.Year,
qry_Private_Credentials_01f_Grads.Credential,
'P - ' + [Credential] AS PSSM_CRED,
Left([LCIP_CD],4) AS LCP4_CD,
'P - ' + Left([LCIP_CD],4) + ' - ' + [Credential] AS LCIP4_CRED,
'P - ' + Left([LCIP_CD],2) + ' - ' + [Credential] AS LCIP2_CRED,
qry_Private_Credentials_01f_Grads.Age_Group,
Sum(qry_Private_Credentials_01f_Grads.Grads) AS [Count]
INTO qry_Private_Credentials_06b_Cohort_Dist
FROM qry_Private_Credentials_01f_Grads
GROUP BY qry_Private_Credentials_01f_Grads.Year,
qry_Private_Credentials_01f_Grads.Credential,
'P - ' + [Credential], Left([LCIP_CD],4),
'P - ' + Left([LCIP_CD],4) + ' - ' + [Credential],
'P - ' + Left([LCIP_CD],2) + ' - ' + [Credential],
qry_Private_Credentials_01f_Grads.Age_Group;"
dbExecute(decimal_con, qry_Private_Credentials_06b_Cohort_Dist)

## ---- Sums total by age ----

# [SELECT INTO] Create qry_Private_Credentials_06c_Cohort_Dist_Total from qry_Private_Credentials_06b_Cohort_Dist

## qry_Private_Credentials_06c_Cohort_Dist_Total ----
# sum totals by age group
qry_Private_Credentials_06c_Cohort_Dist_Total <-
  "SELECT qry_Private_Credentials_06b_Cohort_Dist.Year,
qry_Private_Credentials_06b_Cohort_Dist.Credential,
qry_Private_Credentials_06b_Cohort_Dist.PSSM_CRED,
qry_Private_Credentials_06b_Cohort_Dist.Age_Group,
Sum(qry_Private_Credentials_06b_Cohort_Dist.Count) AS Total
INTO qry_Private_Credentials_06c_Cohort_Dist_Total
FROM qry_Private_Credentials_06b_Cohort_Dist
GROUP BY qry_Private_Credentials_06b_Cohort_Dist.Year,
qry_Private_Credentials_06b_Cohort_Dist.Credential,
qry_Private_Credentials_06b_Cohort_Dist.PSSM_CRED,
qry_Private_Credentials_06b_Cohort_Dist.Age_Group;"
dbExecute(decimal_con, qry_Private_Credentials_06c_Cohort_Dist_Total)

## ---- Prepare and save as table to update necessary tables later ----
# Use the same table to update Cohort_Program_Distributions_Static and Cohort_Program_Distributions_Projected

# [SELECT INTO] Create qry_Private_Credentials_06d1_Cohort_Dist from qry_Private_Credentials_06c_Cohort_Dist_Total

## qry_Private_Credentials_06d1_Cohort_Dist ----
qry_Private_Credentials_06d1_Cohort_Dist <-
"SELECT 'PTIB' AS Survey,
qry_Private_Credentials_06b_Cohort_Dist.Credential,
qry_Private_Credentials_06b_Cohort_Dist.PSSM_CRED,
qry_Private_Credentials_06b_Cohort_Dist.LCP4_CD,
qry_Private_Credentials_06b_Cohort_Dist.LCIP4_CRED,
qry_Private_Credentials_06b_Cohort_Dist.LCIP2_CRED,
qry_Private_Credentials_06b_Cohort_Dist.Age_Group,
qry_Private_Credentials_06b_Cohort_Dist.Year,
qry_Private_Credentials_06b_Cohort_Dist.Count,
qry_Private_Credentials_06c_Cohort_Dist_Total.Total,
IIf(([Total]=0),0,[Count]/[Total]) AS [Percent]
INTO qry_Private_Credentials_06d1_Cohort_Dist
FROM qry_Private_Credentials_06c_Cohort_Dist_Total INNER JOIN qry_Private_Credentials_06b_Cohort_Dist
ON (qry_Private_Credentials_06c_Cohort_Dist_Total.PSSM_CRED = qry_Private_Credentials_06b_Cohort_Dist.PSSM_CRED)
AND (qry_Private_Credentials_06c_Cohort_Dist_Total.Age_Group = qry_Private_Credentials_06b_Cohort_Dist.Age_Group);"
dbExecute(decimal_con, qry_Private_Credentials_06d1_Cohort_Dist)

## ---- Delete excess age groups ----

# [DELETE] qry_Private_Credentials_06d1_Cohort_Dist


## qry_Private_Credentials_06d2_Delete_AgeGrps ----
# remove excess age groups
qry_Private_Credentials_06d2_Delete_AgeGrps <-
  "DELETE FROM qry_Private_Credentials_06d1_Cohort_Dist
WHERE (((qry_Private_Credentials_06d1_Cohort_Dist.Survey)='PTIB') AND
((qry_Private_Credentials_06d1_Cohort_Dist.Age_Group)='(blank)') OR
((qry_Private_Credentials_06d1_Cohort_Dist.Age_Group)='Unknown') OR
((qry_Private_Credentials_06d1_Cohort_Dist.Age_Group)='65+') OR
((qry_Private_Credentials_06d1_Cohort_Dist.Age_Group)='16 or less'))"
dbExecute(decimal_con, qry_Private_Credentials_06d2_Delete_AgeGrps)


# Clean up ----
## ---- Drop tmp qry datasets ----
dbExecute(decimal_con, "DROP TABLE qry_Private_Credentials_01f_Grads")
dbExecute(decimal_con, "DROP TABLE qry_Private_Credentials_05i_Grads")
dbExecute(decimal_con, "DROP TABLE qry_Private_Credentials_06b_Cohort_Dist")
dbExecute(decimal_con, "DROP TABLE qry_Private_Credentials_06c_Cohort_Dist_Total")

## ---- Drop Main Datasets
dbExecute(decimal_con, "DROP TABLE T_Private_Institutions_Credentials")
dbExecute(decimal_con, "DROP TABLE T_Private_Institutions_Credentials_Clean")

## ---- Drop Lookups
dbExecute(decimal_con, "DROP TABLE T_PSSM_Credential_Grouping")
dbExecute(decimal_con, "DROP TABLE T_PTIB_Y1_to_Y10")

## ---- disconnect_connect ----
dbDisconnect(decimal_con)
# rm(list=ls())
