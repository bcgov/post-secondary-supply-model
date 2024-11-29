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
