# 04-graduate-projections

## Overview

This folder contains SQL queries for calculating graduation rates and generating graduate projections for the Post-Secondary Supply Model.

---

## WHAT: What Does This Code Do?

### Core Functions

| Function | Description |
|----------|-------------|
| **Enrolment Rate Calculation** | Calculate historical enrolment rates by demographics |
| **Graduation Rate Calculation** | Calculate graduation rates by year/gender/age group |
| **Credential Aggregation** | Aggregate credentials by award year, filtering domestic students |
| **Projection Modeling** | Generate future graduate forecasts using trend analysis |

### Source Tables
- `MinEnrolment` - Minimum enrolment records with demographic info
- `tblCredential_HighestRank` - Highest ranked credentials per student
- `Credential_Non_Dup` - Deduplicated credential records
- `AgeGroupLookup` - Age group mapping table

### Output Tables Created
- `Credential_By_Year_Gender_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs` - Credential counts
- `qry09c_MinEnrolment` - Minimum enrolment by demographics

---

## WHY: Why Is This Necessary?

### Projection Methodology
The graduate projection model builds historical rates to forecast future supply:

1. **Enrolment Base**: Count unique students by year/gender/age (MinEnrolment)
2. **Graduation Counts**: Count credentials by year/gender/age (excluding RU/DACSO)
3. **Rate Calculation**: `Graduation_Rate = Graduates / Enrolment`
4. **Forecasting**: Apply historical rates to projected populations

### Key Exclusions
- **Research Universities**: Cluster codes '09' and '10' excluded
- **DACSO**: Survey outcomes excluded from counts
- **Apprenticeship**: Different completion pathway, excluded from rates
- **International**: Visa status filtering for domestic-only rates

### Formula
```
Graduation_Rate = COUNT(credentials) / COUNT(min_enrolment)
Projected_Graduates = Population_Projection × Graduation_Rate
```

---

## HOW: How It Works

### Script: 04-graduate-projections-sql.R

#### Step 1: Credential Counts by Demographics
```sql
-- Count credentials by year/gender/age group (domestic only, exclude RU/DACSO)
SELECT psi_gender_cleaned, AgeGroup, PSI_CREDENTIAL_CATEGORY,
       PSI_AWARD_SCHOOL_YEAR_DELAYED,
       COUNT(*) AS Count
INTO Credential_By_Year_Gender_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs
FROM tblCredential_HighestRank
INNER JOIN AgeGroupLookup
  ON tblCredential_HighestRank.AGE_GROUP_AT_GRAD = AgeGroupLookup.AgeIndex
INNER JOIN Credential_Non_Dup
  ON tblCredential_HighestRank.id = Credential_Non_Dup.id
WHERE PSI_VISA_STATUS = 'DOMESTIC'
  AND FINAL_CIP_CLUSTER_CODE NOT IN ('09', '10')  -- Exclude research universities
  AND OUTCOMES_CRED <> 'DACSO'  -- Exclude DACSO outcomes
GROUP BY psi_gender_cleaned, AgeGroup, PSI_CREDENTIAL_CATEGORY,
         PSI_AWARD_SCHOOL_YEAR_DELAYED
HAVING PSI_CREDENTIAL_CATEGORY <> 'APPRENTICESHIP'  -- Exclude apprenticeships
```

#### Step 2: Minimum Enrolment by Demographics
```sql
-- Minimum enrolment counts by year/gender/age group
SELECT PSI_GENDER,
       PSI_GENDER + AgeGroup AS Groups,
       PSI_SCHOOL_YEAR,
       COUNT(*) AS Expr1
INTO qry09c_MinEnrolment
FROM MinEnrolment
INNER JOIN AgeGroupLookup
  ON MinEnrolment.AGE_GROUP_ENROL_DATE = AgeGroupLookup.AgeIndex
WHERE PSI_SCHOOL_YEAR <> '2023/2024'  -- Exclude current year
GROUP BY PSI_GENDER, AgeGroupLookup.AgeGroup, PSI_SCHOOL_YEAR
```

#### Step 3: Calculate Rates
```sql
-- Join enrolment and graduation data to calculate rates
SELECT e.PSI_GENDER, e.AgeGroup, e.PSI_SCHOOL_YEAR,
       e.Expr1 AS Enrolment_Count,
       g.Count AS Graduation_Count,
       CAST(g.Count AS FLOAT) / CAST(e.Expr1 AS FLOAT) AS Graduation_Rate
FROM qry09c_MinEnrolment e
INNER JOIN Credential_By_Year_Gender_AgeGroup_Domestic_Exclude_RU_DACSO_Exclude_CIPs g
  ON e.PSI_GENDER = g.psi_gender_cleaned
  AND e.PSI_SCHOOL_YEAR = g.PSI_AWARD_SCHOOL_YEAR_DELAYED
```

---

## Processing Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                    Source Data                                 │
│  - MinEnrolment (unique enrolment records)                    │
│  - tblCredential_HighestRank (credential data)                │
│  - Credential_Non_Dup (deduplicated)                          │
│  - AgeGroupLookup (age categorization)                       │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│              Step 1: Credential Aggregation                    │
│  - Filter domestic students only                              │
│  - Exclude research universities (CIP 09, 10)                 │
│  - Exclude DACSO outcomes                                     │
│  - Group by year/gender/age/credential                       │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│              Step 2: Enrolment Aggregation                    │
│  - Calculate minimum enrolment by demographics                │
│  - Exclude current year (incomplete data)                    │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│              Step 3: Rate Calculation                         │
│  - Graduation_Rate = Graduates / Enrolment                    │
│  - Average over 3-5 year rolling window                      │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│              Step 4: Projection Output                        │
│  - Apply rates to population projections                     │
│  - Generate forecasts by year/credential/age/gender         │
└───────────────────────────────────────────────────────────────┘
```

---

## Key Variables

| Variable | Description | Source |
|----------|-------------|--------|
| `PSI_GENDER` | Student gender (Male/Female) | MinEnrolment, tblCredential_HighestRank |
| `AgeGroup` | Age group at graduation/enrolment | AgeGroupLookup |
| `PSI_SCHOOL_YEAR` | Enrolment year (e.g., '2022/2023') | MinEnrolment |
| `PSI_AWARD_SCHOOL_YEAR_DELAYED` | Award year (1-year lag) | tblCredential_HighestRank |
| `PSI_CREDENTIAL_CATEGORY` | Credential type (CERT, DIPL, BACH, etc.) | tblCredential_HighestRank |
| `Graduation_Rate` | Calculated rate: Graduates / Enrolment | Computed |
| `FINAL_CIP_CLUSTER_CODE` | CIP cluster for exclusion | Credential_Non_Dup |

---

## Age Group Categories

| AgeGroup | Age Range |
|----------|-----------|
| 1 | 17-19 |
| 2 | 20-24 |
| 3 | 25-34 |
| 4 | 35-44 |
| 5 | 45-54 |
| 6 | 55-64 |
| 7 | 65+ |

---

## See Also

- **Main Script**: `R/04-graduate-projections.R`
- **Related Folders**: `sql/01-credential-analysis/`, `sql/01d-enrolment-analysis/`
- **Input Tables**: `MinEnrolment`, `tblCredential_HighestRank`, `Credential_Non_Dup`
- **Output Used By**: `R/06-program-projections.R` (program-level forecasts)
