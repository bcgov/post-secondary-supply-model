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

## qry_Private_Credentials_00b_Check_CIP_Length ----
# Check that all CIPs are 7 digits
qry_Private_Credentials_00b_Check_CIP_Length <- 
  "SELECT T_Private_Institutions_Credentials.LCIP_CD, Len([LCIP_CD]) AS Expr1
FROM T_Private_Institutions_Credentials
WHERE (((Len([LCIP_CD]))<>7))"

## qry_Private_Credentials_00c_Clean_CIP_Period ----
# remove periods from LCIP_CD
qry_Private_Credentials_00c_Clean_CIP_Period <- 
"UPDATE T_Private_Institutions_Credentials 
SET T_Private_Institutions_Credentials.LCIP_CD = Replace([LCIP_CD],'.','')"

## qry_Private_Credentials_00d_Check_CIPs ----
# Check CIPs valid; compared to the Infoware table
qry_Private_Credentials_00d_Check_CIPs <- 
"SELECT T_Private_Institutions_Credentials.LCIP_CD, 
INFOWARE_L_CIP_6DIGITS_CIP2016.LCIP_CD
FROM INFOWARE_L_CIP_6DIGITS_CIP2016 RIGHT JOIN T_Private_Institutions_Credentials 
ON INFOWARE_L_CIP_6DIGITS_CIP2016.LCIP_CD = T_Private_Institutions_Credentials.LCIP_CD
WHERE (((INFOWARE_L_CIP_6DIGITS_CIP2016.LCIP_CD) Is Null));"

## qry_Private_Credentials_00f_Recode_Age_Group ----
## recode dashes in age group
qry_Private_Credentials_00f_Recode_Age_Group <- 
"UPDATE T_Private_Institutions_Credentials 
SET T_Private_Institutions_Credentials.Age_Group = Replace([Age_Group],'-',' to ');"

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

## qry_Private_Credentials_05i2_Delete_AgeGrps ----
# remove excess age groups
qry_Private_Credentials_05i2_Delete_AgeGrps <- 
  "DELETE FROM qry_Private_Credentials_05i1_Grads_by_Year
WHERE (((qry_Private_Credentials_05i1_Grads_by_Year.Survey)='PTIB') AND
((qry_Private_Credentials_05i1_Grads_by_Year.Age_Group)='(blank)') OR
((qry_Private_Credentials_05i1_Grads_by_Year.Age_Group)='Unknown') OR
((qry_Private_Credentials_05i1_Grads_by_Year.Age_Group)='65+') OR
((qry_Private_Credentials_05i1_Grads_by_Year.Age_Group)='16 or less'))"

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

## qry_Private_Credentials_06d1_Cohort_Dist_Projected ----
qry_Private_Credentials_06d1_Cohort_Dist_Projected <- 
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
INTO qry_Private_Credentials_06d1_Cohort_Dist_Projected
FROM qry_Private_Credentials_06c_Cohort_Dist_Total INNER JOIN qry_Private_Credentials_06b_Cohort_Dist 
ON (qry_Private_Credentials_06c_Cohort_Dist_Total.PSSM_CRED = qry_Private_Credentials_06b_Cohort_Dist.PSSM_CRED) 
AND (qry_Private_Credentials_06c_Cohort_Dist_Total.Age_Group = qry_Private_Credentials_06b_Cohort_Dist.Age_Group);"


## qry_Private_Credentials_06d1_Cohort_Dist_Static ----
qry_Private_Credentials_06d1_Cohort_Dist_Static <- 
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
INTO qry_Private_Credentials_06d1_Cohort_Dist_Static
FROM qry_Private_Credentials_06c_Cohort_Dist_Total INNER JOIN qry_Private_Credentials_06b_Cohort_Dist 
ON (qry_Private_Credentials_06c_Cohort_Dist_Total.Age_Group = qry_Private_Credentials_06b_Cohort_Dist.Age_Group) 
AND (qry_Private_Credentials_06c_Cohort_Dist_Total.PSSM_CRED = qry_Private_Credentials_06b_Cohort_Dist.PSSM_CRED);"


## qry_Private_Credentials_06d2_Projected_Delete_AgeGrps ----
# remove excess age groups
qry_Private_Credentials_06d2_Projected_Delete_AgeGrps <- 
  "DELETE FROM qry_Private_Credentials_06d1_Cohort_Dist_Projected
WHERE (((qry_Private_Credentials_06d1_Cohort_Dist_Projected.Survey)='PTIB') AND
((qry_Private_Credentials_06d1_Cohort_Dist_Projected.Age_Group)='(blank)') OR
((qry_Private_Credentials_06d1_Cohort_Dist_Projected.Age_Group)='Unknown') OR
((qry_Private_Credentials_06d1_Cohort_Dist_Projected.Age_Group)='65+') OR
((qry_Private_Credentials_06d1_Cohort_Dist_Projected.Age_Group)='16 or less'))"

## qry_Private_Credentials_05i2_Delete_AgeGrps ----
# remove excess age groups
qry_Private_Credentials_06d2_Static_Delete_AgeGrps <- 
  "DELETE FROM qry_Private_Credentials_06d1_Cohort_Dist_Static
WHERE (((qry_Private_Credentials_06d1_Cohort_Dist_Static.Survey)='PTIB') AND
((qry_Private_Credentials_06d1_Cohort_Dist_Static.Age_Group)='(blank)') OR
((qry_Private_Credentials_06d1_Cohort_Dist_Static.Age_Group)='Unknown') OR
((qry_Private_Credentials_06d1_Cohort_Dist_Static.Age_Group)='65+') OR
((qry_Private_Credentials_06d1_Cohort_Dist_Static.Age_Group)='16 or less'))"
# ******************************************************************************
