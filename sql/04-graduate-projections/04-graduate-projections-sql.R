# ---- qry20a_4Credential_By_Year_Gender_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs ---- 
qry20a_4Credential_By_Year_Gender_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs <- "SELECT        tblCredential_HighestRank.psi_gender_cleaned, AgeGroupLookup.AgeGroup, tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY, 
                         tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY + AgeGroupLookup.AgeGroup + tblCredential_HighestRank.psi_gender_cleaned AS Expr1, tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED, COUNT(*) 
                         AS Count
INTO Credential_By_Year_Gender_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs
FROM            tblCredential_HighestRank INNER JOIN
                         AgeGroupLookup ON tblCredential_HighestRank.AGE_GROUP_AT_GRAD = AgeGroupLookup.AgeIndex INNER JOIN
                         Credential_Non_Dup ON tblCredential_HighestRank.id = Credential_Non_Dup.id
WHERE        (tblCredential_HighestRank.PSI_VISA_STATUS = 'DOMESTIC') AND (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '09') AND (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '10') AND 
                         (Credential_Non_Dup.RESEARCH_UNIVERSITY = 1) AND (Credential_Non_Dup.OUTCOMES_CRED <> 'DACSO') OR
                         (tblCredential_HighestRank.PSI_VISA_STATUS IS NULL) AND (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '09') AND (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '10') AND 
                         (Credential_Non_Dup.RESEARCH_UNIVERSITY = 1) AND (Credential_Non_Dup.OUTCOMES_CRED <> 'DACSO') OR
                         (tblCredential_HighestRank.PSI_VISA_STATUS = 'DOMESTIC') AND (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '09') AND (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '10') AND 
                         (Credential_Non_Dup.RESEARCH_UNIVERSITY IS NULL) OR
                         (tblCredential_HighestRank.PSI_VISA_STATUS IS NULL) AND (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '09') AND (Credential_Non_Dup.FINAL_CIP_CLUSTER_CODE <> '10') AND 
                         (Credential_Non_Dup.RESEARCH_UNIVERSITY IS NULL)
GROUP BY AgeGroupLookup.AgeGroup, tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY, tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED, tblCredential_HighestRank.psi_gender_cleaned
HAVING        (tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY <> 'APPRENTICESHIP')
ORDER BY AgeGroupLookup.AgeGroup, tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY, tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED, tblCredential_HighestRank.psi_gender_cleaned DESC;"

# ---- qry09c_MinEnrolment ---- 
qry09c_MinEnrolment <- "
SELECT     MinEnrolment.PSI_GENDER, MinEnrolment.PSI_GENDER + AgeGroupLookup.AgeGroup As Groups, MinEnrolment.PSI_SCHOOL_YEAR, COUNT(*) AS Expr1
INTO    qry09c_MinEnrolment
FROM       MinEnrolment 
INNER JOIN  AgeGroupLookup 
ON  MinEnrolment.AGE_GROUP_ENROL_DATE = AgeGroupLookup.AgeIndex
GROUP BY MinEnrolment.PSI_GENDER, AgeGroupLookup.AgeGroup, MinEnrolment.PSI_SCHOOL_YEAR
HAVING      (MinEnrolment.PSI_SCHOOL_YEAR <> '2023/2024')
ORDER BY MinEnrolment.PSI_GENDER, AgeGroupLookup.AgeGroup, MinEnrolment.PSI_SCHOOL_YEAR;"

