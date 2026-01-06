# 06-program-projections

## Overview

This folder contains SQL queries for generating program-level supply projections. These queries build cohort distributions and apply graduation rates to create detailed forecasts by program (CIP), credential, and age group.

---

## WHAT: What Does This Code Do?

### Core Functions

| Function | Description |
|----------|-------------|
| **Program Projection Input** | Build base table from credential data |
| **Weight Calculations** | Apply survey weights to cohort data |
| **Distribution Building** | Create program-credential-age distributions |
| **Near-Completer Handling** | Process DACSO near-completer adjustments |
| **Year Expansion** | Expand single-year data to 10-year projections |

### Source Tables
- `tblCredential_HighestRank` - Highest ranked credentials
- `Credential_Non_Dup` - Deduplicated credential records
- `T_Cohorts_Recoded` - Recoded cohort data with weights
- `T_Weights_STP` - Survey weights by year
- `T_PSSM_Projection_Cred_Grp` - Credential grouping for projections

### Output Tables Created
- `tbl_Program_Projection_Input` - Base projection data
- `Q012b_Weight_Cohort_Dist` - Weighted cohort distributions (undergrad)
- `Q013b_Weight_Cohort_Dist_MAST_DOCT_Others` - Graduate distributions
- `Q014b_Weight_Cohort_Dist_APPR` - Apprenticeship distributions
- `Q012c4_Weighted_Cohort_Distribution_Projected` - Projected distributions

---

## WHY: Why Is This Necessary?

### Program-Level Projections
The model needs to forecast **where** graduates will work, not just **how many**:

1. **CIP-Based Distribution**: Map graduates to 4-digit CIP codes
2. **Credential Weighting**: Apply appropriate weights for each credential type
3. **Age Stratification**: Different age groups have different labour market behavior
4. **Near-Completer Adjustment**: DACSO respondents who later got credentials
5. **Multiple Credential Types**: Handle undergrad, graduate, and apprenticeship separately

### Key Challenges
- **Different Weighting**: Undergrad vs graduate vs apprenticeship use different weights
- **Missing Labour Supply**: Handle cases where no direct labour supply data exists
- **Near-Completers**: Adjust for students who responded "no credential" but later graduated
- **Projection Years**: Expand base year to 10 future years (Y1-Y10)

---

## HOW: How It Works

### Script: 06-program-projections.R

#### Part A: Build Projection Input Table

```sql
-- Create base projection input from credential data
SELECT A.AgeGroup,
       tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY,
       tblCredential_HighestRank.PSI_CREDENTIAL_CATEGORY + A.AgeGroup AS Expr1,
       Credential_Non_Dup.FINAL_CIP_CODE_4,
       tblCredential_HighestRank.PSI_AWARD_SCHOOL_YEAR_DELAYED,
       COUNT(*) AS Count
INTO tbl_Program_Projection_Input
FROM tblCredential_HighestRank
INNER JOIN AgeGroupLookup A
  ON tblCredential_HighestRank.AGE_GROUP_AT_GRAD = A.AgeIndex
INNER JOIN Credential_Non_Dup
  ON tblCredential_HighestRank.id = Credential_Non_Dup.id
WHERE (PSI_VISA_STATUS = 'DOMESTIC' OR PSI_VISA_STATUS IS NULL)
  AND RESEARCH_UNIVERSITY IN (1, NULL)
  AND OUTCOMES_CRED <> 'DACSO'
  AND FINAL_CIP_CLUSTER_CODE NOT IN ('09', '10')  -- Exclude research universities
GROUP BY A.AgeGroup, PSI_CREDENTIAL_CATEGORY,
         PSI_AWARD_SCHOOL_YEAR_DELAYED, FINAL_CIP_CODE_4
HAVING PSI_CREDENTIAL_CATEGORY <> 'APPRENTICESHIP'
```

#### Part B: Weight Calculations (Undergraduate)

```sql
-- Apply weights to undergraduate credentials
SELECT T_PSSM_Projection_Cred_Grp.PSSM_Credential,
       CONCAT(CASE WHEN COSC_GRAD_STATUS_LGDS_CD IS NULL THEN NULL
              ELSE CAST(COSC_GRAD_STATUS_LGDS_CD AS NVARCHAR(50)) + ' - ' END,
              PSSM_Credential) AS PSSM_CRED,
       tbl_Program_Projection_Input.FINAL_CIP_CODE_4 AS LCP4_CD,
       tbl_Program_Projection_Input.AgeGroup,
       SUM(tbl_Program_Projection_Input.Count) AS Counts,
       T_Weights_STP.Weight,
       SUM(Count) * Weight AS Weighted
INTO Q012b_Weight_Cohort_Dist
FROM T_PSSM_Projection_Cred_Grp
INNER JOIN tbl_Program_Projection_Input
  ON T_PSSM_Projection_Cred_Grp.PSSM_Projection_Credential = tbl_Program_Projection_Input.PSI_CREDENTIAL_CATEGORY
INNER JOIN T_Weights_STP
  ON tbl_Program_Projection_Input.PSI_AWARD_SCHOOL_YEAR_DELAYED = T_Weights_STP.Year_Code
WHERE T_Weights_STP.Model = '2023-2024'
  AND T_Weights_STP.Weight > 0
  AND PSSM_Credential NOT IN ('APPRAPPR', 'APPRCERT', 'GRCT or GRDP', 'PDEG', 'MAST', 'DOCT')
GROUP BY PSSM_Credential, COSC_GRAD_STATUS_LGDS_CD,
         FINAL_CIP_CODE_4, AgeGroup, Weight
```

#### Part C: Weight Calculations (Graduate - PDEG/MAST/DOCT)

```sql
-- Handle graduate credentials with different grouping
SELECT T_PSSM_Projection_Cred_Grp.PSSM_Credential,
       CONCAT(CASE WHEN COSC_GRAD_STATUS_LGDS_CD IS NULL THEN NULL
              ELSE CAST(COSC_GRAD_STATUS_LGDS_CD AS NVARCHAR(50)) + ' - ' END,
              PSSM_Credential) AS PSSM_CRED,
       qry_12_LCP4_LCIPPC_Recode_9999.LCIP_LCIPPC_CD AS LCIPPC_CD,
       qry_12_LCP4_LCIPPC_Recode_9999.LCIP_LCIPPC_CD + ' - ' + PSSM_Credential AS LCIPPC_CRED,
       tbl_Program_Projection_Input.AgeGroup,
       SUM(tbl_Program_Projection_Input.Count) AS Counts,
       T_Weights_STP.Weight,
       SUM(Count) * Weight AS Weighted
INTO Q013b_Weight_Cohort_Dist_MAST_DOCT_Others
FROM T_PSSM_Projection_Cred_Grp
INNER JOIN tbl_Program_Projection_Input
  ON T_PSSM_Projection_Cred_Grp.PSSM_Projection_Credential = tbl_Program_Projection_Input.PSI_CREDENTIAL_CATEGORY
INNER JOIN T_Weights_STP
  ON tbl_Program_Projection_Input.PSI_AWARD_SCHOOL_YEAR_DELAYED = T_Weights_STP.Year_Code
INNER JOIN qry_12_LCP4_LCIPPC_Recode_9999
  ON tbl_Program_Projection_Input.FINAL_CIP_CODE_4 = qry_12_LCP4_LCIPPC_Recode_9999.LCIP_LCP4_CD
WHERE T_Weights_STP.Model = '2023-2024'
  AND T_Weights_STP.Weight > 0
  AND PSSM_Credential IN ('GRCT or GRDP', 'PDEG', 'MAST', 'DOCT')
GROUP BY PSSM_Credential, COSC_GRAD_STATUS_LGDS_CD,
         LCIP_LCIPPC_CD, AgeGroup, Weight
```

#### Part D: Apprenticeship Weights

```sql
-- Apprenticeships use different source data (T_Cohorts_Recoded)
SELECT T_Cohorts_Recoded.PSSM_Credential,
       T_Cohorts_Recoded.PSSM_Credential AS PSSM_CRED,
       T_Cohorts_Recoded.LCP4_CD,
       T_Cohorts_Recoded.TTRAIN,
       T_Cohorts_Recoded.LCIP4_CRED,
       T_Cohorts_Recoded.LCIP2_CRED,
       tbl_Age_Groups.Age_Group_Label AS Age_Group,
       COUNT(*) AS Counts,
       T_Cohorts_Recoded.Weight,
       COUNT(*) * Weight AS Weighted
INTO Q014b_Weighted_Cohort_Dist_APPR
FROM T_Cohorts_Recoded
INNER JOIN tbl_Age_Groups
  ON T_Cohorts_Recoded.Age_Group = tbl_Age_Groups.Age_Group
WHERE PSSM_Credential IN ('APPRAPPR', 'APPRCERT')
  AND Weight > 0
GROUP BY PSSM_Credential, LCP4_CD, TTRAIN,
         LCIP4_CRED, LCIP2_CRED, Age_Group_Label, Weight
```

#### Part E: Aggregate Weighted Distributions

```sql
-- Sum weighted counts by cohort key
SELECT PSSM_Credential, PSSM_CRED, LCP4_CD, COSC_GRAD_STATUS_LGDS_CD,
       LCIP4_CRED, LCIP2_CRED, AgeGroup,
       SUM(Weighted) AS Count
INTO Q012c_Weighted_Cohort_Dist
FROM Q012b_Weight_Cohort_Dist
GROUP BY PSSM_Credential, PSSM_CRED, LCP4_CD, COSC_GRAD_STATUS_LGDS_CD,
         LCIP4_CRED, LCIP2_CRED, AgeGroup

-- Calculate totals for percentage
SELECT PSSM_Credential, PSSM_CRED, LCP4_CD, AgeGroup,
       SUM(Count) AS Totals
INTO Q012d_Weighted_Cohort_Dist_Total
FROM Q012c_Weighted_Cohort_Dist
GROUP BY PSSM_Credential, PSSM_CRED, LCP4_CD, AgeGroup
```

#### Part F: Calculate Percentages

```sql
-- Distribution percentage = count / total
INSERT INTO Cohort_Program_Distributions_Static
(Survey, PSSM_Credential, PSSM_CRED, LCP4_CD, GRAD_STATUS, TTRAIN,
 LCIP4_CRED, LCIP2_CRED, Age_Group, Year, Count, Total, Percent)
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
       CASE WHEN Totals = 0 THEN 0 ELSE CAST(Count_Distributed AS FLOAT)/CAST(Totals AS FLOAT) END AS Percent
FROM Q012c5_Weighted_Cohort_Dist_TTRAIN
INNER JOIN Q012d_Weighted_Cohort_Dist_Total
  ON Q012c5_Weighted_Cohort_Dist_TTRAIN.AgeGroup = Q012d_Weighted_Cohort_Dist_Total.AgeGroup
  AND Q012c5_Weighted_Cohort_Dist_TTRAIN.PSSM_CRED = Q012d_Weighted_Cohort_Dist_Total.PSSM_CRED
```

#### Part G: Year Expansion (Y1 to Y10)

```sql
-- Expand single-year data to 10 projection years
INSERT INTO Graduate_Projections (Survey, PSSM_Credential, PSSM_CRED, Age_Group, Year, Graduates)
SELECT Graduate_Projections.Survey,
       Graduate_Projections.PSSM_Credential,
       Graduate_Projections.PSSM_CRED,
       Graduate_Projections.Age_Group,
       T_APPR_Y2_to_Y10.Y2_to_Y10,
       Graduate_Projections.Graduates
FROM Graduate_Projections
INNER JOIN T_APPR_Y2_to_Y10
  ON Graduate_Projections.Year = T_APPR_Y2_to_Y10.Y1
WHERE Graduate_Projections.Survey = 'APPSO'
```

---

## Processing Flow

```
┌─────────────────────────────────────────────────────────────────┐
│              Step 1: Build Projection Input                    │
│  - Extract credentials with CIP codes                          │
│  - Join to age group lookup                                    │
│  - Filter domestic, exclude RU/DACSO                           │
│  - Group by credential/CIP/age/award year                     │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│              Step 2: Apply Weights                             │
│  - Undergraduate: Use T_Weights_STP                            │
│  - Graduate (PDEG/MAST/DOCT): Use different grouping           │
│  - Apprenticeship: Use T_Cohorts_Recoded with Weight          │
│  - Calculate: Count × Weight = Weighted                        │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│              Step 3: Aggregate by Cohort                       │
│  - Sum weighted counts by cohort key                           │
│  - Create LCIP4_CRED, LCIP2_CRED hierarchical keys            │
│  - Calculate totals for percentage denominator                │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│              Step 4: Calculate Percentages                     │
│  - Distribution = Count / Total                                │
│  - Handle near-completers (DACSO adjustment)                  │
│  - Insert into Cohort_Program_Distributions_* tables          │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│              Step 5: Year Expansion                            │
│  - Map Y1 to Y2-Y10 for projections                            │
│  - Join with T_*_Y2_to_Y10 lookup tables                      │
│  - Generate 10-year projection table                          │
└───────────────────────────────────────────────────────────────┘
```

---

## Cohort Key Hierarchy

| Level | Key Format | Description |
|-------|------------|-------------|
| 1 | `PSSM_Credential` | Credential type only |
| 2 | `PSSM_CRED` | Credential + Grad Status |
| 3 | `LCIP4_CRED` | CIP4 + Credential |
| 4 | `LCIP2_CRED` | CIP2 + Credential |

**Example**:
```
PSSM_Credential: "CERT"
PSSM_CRED: "1 - CERT"
LCIP4_CRED: "1 - 1234 - CERT"
LCIP2_CRED: "1 - 12 - CERT"
```

---

## Credential Processing Groups

| Group | Credentials | Source Table | Weight Type |
|-------|-------------|--------------|-------------|
| Undergraduate | 1 - CERT, 1 - DIPL, etc. | tblCredential_HighestRank | T_Weights_STP |
| Graduate | PDEG, MAST, DOCT | tblCredential_HighestRank | T_Weights_STP |
| Graduate Other | GRCT or GRDP | tblCredential_HighestRank | T_Weights_STP |
| Apprenticeship | APPRAPPR, APPRCERT | T_Cohorts_Recoded | Weight |

---

## Key Variables

| Variable | Description | Source |
|----------|-------------|--------|
| `FINAL_CIP_CODE_4` | 4-digit CIP code | Credential_Non_Dup |
| `PSI_CREDENTIAL_CATEGORY` | Credential type | tblCredential_HighestRank |
| `Weight` | Survey weight | T_Weights_STP |
| `AgeGroup` | Age at graduation | AgeGroupLookup |
| `TTRAIN` | Training type code | T_Cohorts_Recoded |
| `GRAD_STATUS` | Graduate status code | T_PSSM_Projection_Cred_Grp |

---

## See Also

- **Main Script**: `R/06-program-projections.R`
- **Related Folders**: `sql/04-graduate-projections/`, `sql/07-occupation-projections/`
- **Input Tables**: `tblCredential_HighestRank`, `T_Cohorts_Recoded`, `T_Weights_STP`
- **Output Tables**: `Cohort_Program_Distributions_Static`, `Cohort_Program_Distributions_Projected`
