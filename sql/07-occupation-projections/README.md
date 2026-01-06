# 07-occupation-projections

## Overview

This folder contains SQL queries for generating occupation-level supply projections. These queries apply occupation distributions to labour supply data to forecast which occupations graduates will enter, categorized by NOC (National Occupational Classification) codes.

---

## WHAT: What Does This Code Do?

### Core Functions

| Function | Description |
|----------|-------------|
| **Graduate Projections** | Apply program distributions to graduate counts |
| **Labour Supply Mapping** | Map graduates to labour supply by region/CIP/age |
| **Occupation Distribution** | Apply NOC occupation distributions to labour supply |
| **Unknown Handling** | Handle missing labour supply/occupation data with proxies |
| **NOC Aggregation** | Aggregate results by NOC skill level and type |
| **Model Validation** | Compare with Quality Indicator (QI) data |

### Source Tables
- `Graduate_Projections` - Graduate counts by credential/age/year
- `Cohort_Program_Distributions` - Program-credential-age distributions
- `Labour_Supply_Distribution` - Labour supply by CIP/region/age
- `Occupation_Distributions` - NOC occupation shares by CIP/region/age
- `tbl_NOC_Skill_Level_Aged_17_34` - NOC skill level mappings

### Output Tables Created
- `Q_1_Grad_Projections_by_Age_by_Program` - Graduates by program/age/year
- `Q_2_*_Labour_Supply_*` - Various labour supply intermediate tables
- `Q_3_*_Occupations_*` - Occupation-level projections
- `Q_4_NOC_*_Totals_*` - Aggregated NOC totals by various dimensions
- `tmp_tbl_Model` - Final model output table

---

## WHY: Why Is This Necessary?

### Occupation-Level Forecasting
The PSSM must forecast **which occupations** graduates will enter, not just which programs they complete:

1. **Occupation Mapping**: Programs (CIP codes) don't directly map to occupations
2. **NOC Codes**: Canadian occupational classification (4-digit)
3. **Skill Levels**: NOC 0-4 skill level categorization
4. **Regional Variation**: Occupation distributions vary by region
5. **Multiple Proxies**: When direct data is missing, use proxy methods

### Key Challenges
- **One-to-Many Mapping**: One CIP can lead to multiple occupations
- **Missing Data**: Not all CIP/region/age combinations have occupation data
- **Proxy Methods**: Use LCP2, credential-level, or nearest-neighbor proxies
- **Unknown Handling**: Assign to NOC 9999 (unknown) when no match found

---

## HOW: How It Works

### Script: occupation-projections.R

#### Step 1: Graduate Projections by Program

```sql
-- Apply program distributions to graduate counts
SELECT Cohort_Program_Distributions.PSSM_Credential,
       Graduate_Projections.PSSM_CRED,
       Graduate_Projections.Age_Group,
       Graduate_Projections.Year,
       Cohort_Program_Distributions.LCP4_CD,
       Cohort_Program_Distributions.GRAD_STATUS,
       Cohort_Program_Distributions.TTRAIN,
       Cohort_Program_Distributions.LCIP4_CRED,
       Graduate_Projections.Graduates * Cohort_Program_Distributions.Percent AS Grads
INTO Q_1_Grad_Projections_by_Age_by_Program
FROM Graduate_Projections
INNER JOIN Cohort_Program_Distributions
  ON Graduate_Projections.Year = Cohort_Program_Distributions.Year
  AND Graduate_Projections.Age_Group = Cohort_Program_Distributions.Age_Group
  AND Graduate_Projections.PSSM_CRED = Cohort_Program_Distributions.PSSM_CRED
LEFT JOIN T_Exclude_from_Projections_*  -- Various exclusion tables
  ON Cohort_Program_Distributions.LCP4_CD = T_Exclude_from_Projections_LCP4_CD.LCIP_LCP4_CD
WHERE exclusions ARE NULL
```

#### Step 2: Aggregate by Age Rollup

```sql
-- Roll up age groups for labour supply matching
SELECT Q_1_Grad_Projections_by_Age_by_Program.PSSM_Credential,
       Q_1_Grad_Projections_by_Age_by_Program.PSSM_CRED,
       tbl_Age_Groups_Rollup.Age_Group_Rollup,
       tbl_Age_Groups_Rollup.Age_Group_Rollup_Label,
       Q_1_Grad_Projections_by_Age_by_Program.Year,
       Q_1_Grad_Projections_by_Age_by_Program.TTRAIN,
       Q_1_Grad_Projections_by_Age_by_Program.LCP4_CD,
       Q_1_Grad_Projections_by_Age_by_Program.LCIP4_CRED,
       SUM(Q_1_Grad_Projections_by_Age_by_Program.Grads) AS Grads
INTO Q_1c_Grad_Projections_by_Program
FROM Q_1_Grad_Projections_by_Age_by_Program
INNER JOIN tbl_Age_Groups
  ON Q_1_Grad_Projections_by_Age_by_Program.Age_Group = tbl_Age_Groups.Age_Group_Label
INNER JOIN tbl_Age_Groups_Rollup
  ON tbl_Age_Groups.Age_Group_Rollup = tbl_Age_Groups_Rollup.Age_Group_Rollup
GROUP BY Q_1_Grad_Projections_by_Age_by_Program.PSSM_Credential,
         Q_1_Grad_Projections_by_Age_by_Program.PSSM_CRED,
         tbl_Age_Groups_Rollup.Age_Group_Rollup,
         tbl_Age_Groups_Rollup.Age_Group_Rollup_Label,
         Q_1_Grad_Projections_by_Age_by_Program.Year,
         Q_1_Grad_Projections_by_Age_by_Program.TTRAIN,
         Q_1_Grad_Projections_by_Age_by_Program.LCP4_CD,
         Q_1_Grad_Projections_by_Age_by_Program.LCIP4_CRED
```

#### Step 3: Calculate Labour Supply

```sql
-- Apply labour supply rates to graduates
SELECT Q_1c_Grad_Projections_by_Program.PSSM_Credential,
       Q_1c_Grad_Projections_by_Program.PSSM_CRED,
       Q_1c_Grad_Projections_by_Program.Age_Group_Rollup,
       Q_1c_Grad_Projections_by_Program.Age_Group_Rollup_Label,
       Q_1c_Grad_Projections_by_Program.Year,
       Q_1c_Grad_Projections_by_Program.TTRAIN,
       Q_1c_Grad_Projections_by_Program.LCP4_CD,
       Q_1c_Grad_Projections_by_Program.LCIP4_CRED,
       Labour_Supply_Distribution.Current_Region_PSSM_Code_Rollup,
       Labour_Supply_Distribution.New_Labour_Supply,
       Grads * New_Labour_Supply AS NLS  -- New Labour Supply
INTO Q_2_Labour_Supply_by_LCIP4_CRED
FROM Q_1c_Grad_Projections_by_Program
INNER JOIN Labour_Supply_Distribution
  ON Q_1c_Grad_Projections_by_Program.LCIP4_CRED = Labour_Supply_Distribution.LCIP4_CRED
  AND Q_1c_Grad_Projections_by_Program.Age_Group_Rollup = Labour_Supply_Distribution.Age_Group_Rollup
```

#### Step 4: Handle Unknown Labour Supply

When direct labour supply data is missing, use proxy methods:

##### Proxy Method 1: No-TT (Non-Trades) Proxy
```sql
-- Use Labour_Supply_Distribution_No_TT for missing TTRAIN data
SELECT Q_2a_Labour_Supply_Unknown.*,
       labour_supply_distribution_no_tt.current_region_pssm_code_rollup,
       labour_supply_distribution_no_tt.new_labour_supply,
       Grads * new_labour_supply AS NLS
INTO Q_2a2_Labour_Supply_Unknown_No_TT_Proxy
FROM Q_2a_Labour_Supply_Unknown
INNER JOIN labour_supply_distribution_no_tt
  ON Q_2a_Labour_Supply_Unknown.age_group_rollup = labour_supply_distribution_no_tt.age_group_rollup
  AND Q_2a_Labour_Supply_Unknown.lcip4_cred = labour_supply_distribution_no_tt.lcip4_cred
```

##### Proxy Method 2: LCP2 (2-digit CIP) Proxy
```sql
-- Aggregate to 2-digit CIP level for broader matching
SELECT Q_2b4_Labour_Supply_Unknown.PSSM_Credential,
       Q_2b4_Labour_Supply_Unknown.PSSM_CRED,
       Q_2b4_Labour_Supply_Unknown.Age_Group_Rollup,
       Q_2b4_Labour_Supply_Unknown.Year,
       Q_2b4_Labour_Supply_Unknown.TTRAIN,
       Q_2b4_Labour_Supply_Unknown.LCP4_CD,
       Q_2b4_Labour_Supply_Unknown.LCIP4_CRED,
       Labour_Supply_Distribution_LCP2.Current_Region_PSSM_Code_Rollup,
       Labour_Supply_Distribution_LCP2.New_Labour_Supply,
       Grads * New_Labour_Supply AS NLS
INTO Q_2c_Labour_Supply_Unknown_LCP2_Proxy
FROM Labour_Supply_Distribution_LCP2
INNER JOIN Q_2b4_Labour_Supply_Unknown
  ON Labour_Supply_Distribution_LCP2.Age_Group_Rollup = Q_2b4_Labour_Supply_Unknown.Age_Group_Rollup
  AND Labour_Supply_Distribution_LCP2.LCP2_CD = T_LCP2_LCP4.LCIP_LCP2_CD
  AND Labour_Supply_Distribution_LCP2.PSSM_CRED = Q_2b4_Labour_Supply_Unknown.PSSM_CRED
INNER JOIN T_LCP2_LCP4
  ON Q_2b4_Labour_Supply_Unknown.LCP4_CD = T_LCP2_LCP4.LCIP_LCP4_CD
```

#### Step 5: Calculate Occupation Distributions

```sql
-- Apply occupation shares to labour supply
SELECT tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.PSSM_Credential,
       tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.PSSM_CRED,
       tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.Age_Group_Rollup,
       tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.Age_Group_Rollup_Label,
       tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.Year,
       tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.TTRAIN,
       tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.LCP4_CD,
       tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.LCIP4_CRED,
       tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.Current_Region_PSSM_Code_Rollup,
       Occupation_Distributions.NOC,
       Occupation_Distributions.Percent,
       NLS * Percent AS OccsN  -- Occupations
INTO Q_3_Occupations_by_LCIP4_CRED
FROM tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union
INNER JOIN Occupation_Dributions
  ON tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.LCIP4_CRED = Occupation_Distributions.LCIP4_CRED
  AND tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.Current_Region_PSSM_Code_Rollup = Occupation_Distributions.Current_Region_PSSM_Code_Rollup
  AND tmp_tbl_Q_2d_Labour_Supply_by_LCIP4_CRED_LCP2_Union.Age_Group_Rollup = Occupation_Distributions.Age_Group_Rollup
```

#### Step 6: Handle Unknown Occupations

When occupation data is missing, use similar proxy methods:

```sql
-- Use LCP2-level occupation data as proxy
SELECT Q_3b4_Occupations_Unknown.PSSM_Credential,
       Q_3b4_Occupations_Unknown.PSSM_CRED,
       Q_3b4_Occupations_Unknown.Age_Group_Rollup,
       Q_3b4_Occupations_Unknown.Year,
       Q_3b4_Occupations_Unknown.TTRAIN,
       Q_3b4_Occupations_Unknown.LCP4_CD,
       Q_3b4_Occupations_Unknown.LCIP4_CRED,
       Q_3b4_Occupations_Unknown.Current_Region_PSSM_Code_Rollup,
       Occupation_Distributions_LCP2.NOC,
       Occupation_Distributions_LCP2.Percent,
       NLS * Percent AS OccsN
INTO Q_3c_Occupations_Unknown_LCP2_Proxy
FROM Occupation_Distributions_LCP2
INNER JOIN Q_3b4_Occupations_Unknown
  ON Occupation_Distributions_LCP2.Age_Group_Rollup = Q_3b4_Occupations_Unknown.Age_Group_Rollup
  AND Occupation_Distributions_LCP2.Current_Region_PSSM_Code_Rollup = Q_3b4_Occupations_Unknown.Current_Region_PSSM_Code_Rollup
  AND Occupation_Distributions_LCP2.PSSM_CRED = Q_3b4_Occupations_Unknown.PSSM_CRED
INNER JOIN T_LCP2_LCP4
  ON Q_3b4_Occupations_Unknown.LCP4_CD = T_LCP2_LCP4.LCIP_LCP4_CD
  AND Occupation_Distributions_LCP2.LCP2_CD = T_LCP2_LCP4.LCIP_LCP2_CD
WHERE exclusions ARE NULL OR LEFT(LCIP4_CRED, 1) = 'P - '
```

#### Step 7: Final Unknown Handling (NOC 9999)

```sql
-- Assign to unknown occupation (NOC 9999) when no data available
SELECT tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.pssm_credential,
       tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.pssm_cred,
       tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.age_group_rollup,
       tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.age_group_rollup_label,
       tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.year,
       tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.ttrain,
       tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.lcp4_cd,
       tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.lcip4_cred,
       tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.current_region_pssm_code_rollup,
       9999 AS NOC,
       1 AS Percent,
       Sum(tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union.nls) AS OccsN
INTO Q_3e2_Occupations_Unknown
FROM tmp_tbl_q_2d_labour_supply_by_lcip4_cred_lcp2_union
LEFT JOIN q_3d4_occupations_by_lcip4_cred_lcp2_lcp2_private_union
  ON matching keys
WHERE no match found AND Sum(nls) > 0
GROUP BY all non-aggregated fields
```

#### Step 8: NOC Aggregation by Skill Level

```sql
-- Aggregate to NOC 1D (skill type) level
SELECT PSSM_Skill_Level, SKILL_LEVEL_CATEGORY_CODE, NOC_Level, NOC_SKILL_TYPE, NOC, ENGLISH_NAME,
       [1 - ADCT or ADIP], [1 - ADGR or UT], [1 - CERT], [1 - DIPL], [1 - PDCT or PDDP],
       [3 - ADCT or ADIP], [3 - ADGR or UT], [3 - CERT], [3 - DIPL], [3 - PDCT or PDDP],
       [APPRAPPR], [APPRCERT], [BACH], [DOCT], [GRCT or GRDP], [MAST], [P - CERT], [P - DIPL], [PDEG]
INTO Q_4_NOC_1D_Totals_by_PSSM_CRED
FROM (
    SELECT T_NOC_Skill_Type.PSSM_Skill_Level,
           T_NOC_Skill_Type.SKILL_LEVEL_CATEGORY_CODE,
           tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.OccsN,
           Len([tbl_NOC_Skill_Level_Aged_17_34].[SKILL_TYPE_CODE]) AS NOC_Level,
           tbl_NOC_Skill_Level_Aged_17_34.NOC_SKILL_TYPE,
           tbl_NOC_Skill_Level_Aged_17_34.SKILL_TYPE_CODE AS NOC,
           tbl_NOC_Skill_Level_Aged_17_34.SKILL_TYPE_ENGLISH_NAME,
           PSSM_CRED
    FROM tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union
    INNER JOIN tbl_NOC_Skill_Level_Aged_17_34
      ON tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union.NOC = tbl_NOC_Skill_Level_Aged_17_34.UNIT_GROUP_CODE
    INNER JOIN T_NOC_Skill_Type
      ON tbl_NOC_Skill_Level_Aged_17_34.SKILL_TYPE_CODE = T_NOC_Skill_Type.SKILL_TYPE_CODE
) AS SourceTable
PIVOT (Sum(OccsN) FOR PSSM_CRED IN ([credential list]))
AS PivotTable
ORDER BY NOC_SKILL_TYPE
```

#### Step 9: Year-Based Pivot

```sql
-- Pivot to create year columns for time series
SELECT Expr1, Age_Group_Rollup_Label, PSSM_Skill_Level, SKILL_LEVEL_CATEGORY_CODE, NOC_Level,
       NOC_SKILL_TYPE, NOC, ENGLISH_NAME, Current_Region_PSSM_Code_Rollup, Current_Region_PSSM_Name_Rollup,
       [2023/2024], [2024/2025], [2025/2026], [2026/2027], [2027/2028], [2028/2029],
       [2029/2030], [2030/2031], [2031/2032], [2032/2033], [2033/2034], [2034/2035]
INTO Q_4_NOC_1D_Totals_by_Year
FROM (
    SELECT Year, OccsN,
           Age_Group_Rollup_Label + '-' + PSSM_Skill_Level + '-' + SKILL_TYPE_CODE + '-' +
           CAST(Current_Region_PSSM_Code_Rollup AS NVARCHAR(50)) AS Expr1,
           Age_Group_Rollup_Label, PSSM_Skill_Level, SKILL_LEVEL_CATEGORY_CODE,
           Len(SKILL_TYPE_CODE) AS NOC_Level, NOC_SKILL_TYPE, SKILL_TYPE_CODE AS NOC,
           SKILL_TYPE_ENGLISH_NAME, Current_Region_PSSM_Code_Rollup, Current_Region_PSSM_Name_Rollup
    FROM tmp_tbl_Q_3d_Occupations_by_LCIP4_CRED_LCP2_Union
    INNER JOIN tbl_NOC_Skill_Level_Aged_17_34
      ON NOC = UNIT_GROUP_CODE
    INNER JOIN T_Current_Region_PSSM_Rollup_Codes
      ON Current_Region_PSSM_Code_Rollup = T_Current_Region_PSSM_Rollup_Codes.Current_Region_PSSM_Code_Rollup
    INNER JOIN T_NOC_Skill_Type
      ON SKILL_TYPE_CODE = T_NOC_Skill_Type.SKILL_TYPE_CODE
) AS SourceTable
PIVOT (Sum(OccsN) FOR Year IN ([2023/2024], [2024/2025], ...))
AS PivotTable
ORDER BY Expr1, Age_Group_Rollup_Label, NOC_Level, NOC_SKILL_TYPE, NOC
```

---

## Processing Flow

```
┌─────────────────────────────────────────────────────────────────┐
│              Step 1: Graduate Projections                      │
│  - Join Graduate_Projections × Cohort_Program_Distributions   │
│  - Apply: Graduates × Percent = Grads_by_Program              │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│              Step 2: Age Rollup                                │
│  - Join with tbl_Age_Groups                                    │
│  - Aggregate to Age_Group_Rollup                               │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│              Step 3: Labour Supply Calculation                 │
│  - Join with Labour_Supply_Distribution                        │
│  - Apply: Grads × New_Labour_Supply = NLS                      │
│  - Handle missing with No-TT proxy                             │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│              Step 4: Labour Supply Proxies                     │
│  - Unknown labour supply: use LCP2-level data                  │
│  - Union direct + proxy data                                   │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│              Step 5: Occupation Distribution                   │
│  - Join with Occupation_Distributions                          │
│  - Apply: NLS × Percent = OccsN                                │
│  - Handle unknown with LCP2 proxy                              │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│              Step 6: NOC Aggregation                           │
│  - Aggregate by NOC skill level (1D, 2D, 3D, 4D)              │
│  - Pivot by credential                                         │
│  - Pivot by year for time series                               │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│              Step 7: Model Output                              │
│  - Union all NOC levels                                        │
│  - Create tmp_tbl_Model                                        │
│  - Validate against QI                                         │
└───────────────────────────────────────────────────────────────┘
```

---

## NOC Skill Levels

| Level | Description | NOC Range |
|-------|-------------|-----------|
| **1D** | Skill Type (broadest) | 2-digit (e.g., "11") |
| **2D** | Major Group | 2-digit (e.g., "21") |
| **3D** | Minor Group | 3-digit (e.g., "212") |
| **4D** | Unit Group (narrowest) | 4-digit (e.g., "2121") |

**NOC 0** = Management occupations (special handling)

---

## NOC Skill Type Categories

| NOC_SKILL_TYPE | Description |
|----------------|-------------|
| 0 | Management |
| 1 | Business, finance, administration |
| 2 | Natural sciences, applied sciences |
| 3 | Health |
| 4 | Education, law, social services |
| 5 | Art, culture, recreation, sport |
| 6 | Sales, service |
| 7 | Trades, transport, equipment operators |
| 8 | Natural resources, agriculture |
| 9 | Manufacturing, utilities |

---

## Labour Supply Proxy Hierarchy

| Priority | Method | When Used | Table Used |
|----------|--------|-----------|------------|
| 1 | Direct | Exact match exists | Labour_Supply_Distribution |
| 2 | No-TT | Missing TTRAIN | Labour_Supply_Distribution_No_TT |
| 3 | LCP2 | Missing CIP4 | Labour_Supply_Distribution_LCP2 |
| 4 | LCP2 No-TT | Missing both | Labour_Supply_Distribution_LCP2_No_TT |
| 5 | Private Cred | Private institution proxy | Private credential crosswalk |
| 6 | Unknown | No data at all | NLS assigned to NOC 9999 |

---

## Key Variables

| Variable | Description | Source |
|----------|-------------|--------|
| `NLS` | New Labour Supply | Grads × New_Labour_Supply |
| `OccsN` | Occupation count | NLS × Occupation_Percent |
| `NOC` | National Occupational Classification code | Occupation_Distributions |
| `PSSM_Skill_Level` | Skill level category | T_NOC_Skill_Type |
| `Current_Region_PSSM_Code_Rollup` | Aggregated region code | T_Current_Region_PSSM_Rollup_Codes |
| `Age_Group_Rollup` | Aggregated age group | tbl_Age_Groups_Rollup |

---

## Output Tables

### Final Outputs
| Table | Description |
|-------|-------------|
| `tmp_tbl_Model` | Complete model with all dimensions |
| `Q_4_NOC_*_Totals_by_Year` | NOC totals pivoted by year |
| `Q_4_NOC_*_Totals_by_PSSM_CRED` | NOC totals pivoted by credential |
| `qry_10a_Model` | Model output with QI comparison |

### Intermediate Tables
| Table | Description |
|-------|-------------|
| `Q_1c_Grad_Projections_by_Program` | Graduates by program/age/year |
| `Q_2_*_Labour_Supply_*` | Labour supply calculations |
| `Q_3_*_Occupations_*` | Occupation-level projections |
| `tmp_tbl_Q_2d_*` | Labour supply union results |
| `tmp_tbl_Q_3d_*` | Occupation union results |

---

## See Also

- **Main Script**: `R/07-occupation-projections.R`
- **Related Folders**: `sql/06-program-projections/`
- **Input Tables**: `Graduate_Projections`, `Cohort_Program_Distributions`, `Labour_Supply_Distribution`, `Occupation_Distributions`
- **Lookup Tables**: `tbl_NOC_Skill_Level_Aged_17_34`, `T_NOC_Skill_Type`, `T_Current_Region_PSSM_Rollup_Codes`
