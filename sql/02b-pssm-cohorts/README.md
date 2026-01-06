# 02b-pssm-cohorts

## Overview

This folder contains SQL queries for building cohort tables and calculating weights for the PSSM model.

---

## WHAT: What Does This Code Do?

### Core Functions

| Function | Description |
|----------|-------------|
| **Cohort Building** | Create unified cohort records from survey data |
| **Weight Calculation** | Calculate survey weights (Weight_NLS, Weight_OCC) |
| **Labour Supply** | Derive New Labour Supply (NLS) flags |
| **Occupation Distribution** | Build occupation distribution tables |
| **Regional Coding** | Map records to PSSM region codes |

### Source Tables
- `T_Cohorts_Recoded` - Unified cohort table (from survey loads)
- `t_weights` - Year/survey weights
- `tbl_age`, `tbl_age_groups` - Age lookups
- `t_current_region_pssm_codes` - Region mapping
- `t_dacso_data_part_1` - DACSO outcomes
- `T_BGS_Data_Final_for_OutcomesMatching` - BGS outcomes
- `T_TRD_DATA` - TRD outcomes
- `T_APPSO_DATA_Final` - APPSO outcomes

### Output Tables
- `Labour_Supply_Distribution` - NLS distributions
- `Occupation_Distributions` - Occupation distributions
- `Occupation_Distributions_LCP2` - 2-digit CIP occupation distributions
- Various `*_No_TT` tables (without training type dimension)

---

## WHY: Why Is This Necessary?

### Cohort Construction
Each survey (TRD, APPSO, BGS, DACSO) has different:
- Column names
- Survey structures
- Labour force questions
- Outcome classifications

The cohort scripts unify these into `T_Cohorts_Recoded` with consistent:
- Column names
- CIP codes (4D and 2D)
- Age groups
- Region codes
- Weight values

### Weight Calculations
Raw survey counts don't represent the full population. Weights account for:
- **Sampling**: Different samples by year
- **Response**: Not all sampled students respond
- **Demographics**: Different response rates by group

### New Labour Supply (NLS)
NLS represents graduates entering the workforce:
- **NLS-0**: Not in labour force
- **NLS-1**: Employed full-time
- **NLS-2**: Employed part-time
- **NLS-3**: Part-time without NLS-1 match

---

## HOW: How It Works

### Scripts in This Folder

| Script | Purpose | Survey |
|--------|---------|--------|
| `02b-pssm-cohorts-trd.R` | TRD cohort building | Trades |
| `02b-pssm-cohorts-appso.R` | APPSO cohort building | Apprenticeship |
| `02b-pssm-cohorts-bgs.R` | BGS cohort building | Baccalaureate Graduate |
| `02b-pssm-cohorts-dacso.R` | DACSO cohort building | Diploma/Certificate |
| `02b-pssm-cohorts-new-labour-supply.R` | NLS weights | All |
| `02b-pssm-cohorts-occupation-distributions.R` | Occupation weights | All |
| `geocoding.R` | Region code mapping | All |
| `trd-data.sql` | TRD data extraction | Trades |
| `bgs-data.sql` | BGS data extraction | Baccalaureate |
| `appso-data.sql` | APPSO data extraction | Apprenticeship |
| `dacso-data.sql` | DACSO data extraction | Diploma/Certificate |

### Survey-Specific Scripts

#### TRD (Trades)
```sql
-- Q003: Create TRD cohort with weights
SELECT t.ID, t.PEN, t.SUBM_CD,
       t.TRD_AGE_AT_SURVEY, a.age_group,
       t.TRD_LABR_EMPLOYED, t.TRD_LABR_IN_LABOUR_MARKET,
       CASE WHEN t.TRD_LABR_EMPLOYED = 1 THEN 1
            WHEN t.TRD_LABR_IN_LABOUR_MARKET = 1 THEN 1
            ELSE 0 END AS new_labour_supply,
       w.weight
INTO T_TRD_DATA_Updated
FROM T_TRD_DATA t
INNER JOIN tbl_age a ON t.TRD_AGE_AT_SURVEY = a.age
INNER JOIN t_weights w ON t.SUBM_CD = w.subm_cd
WHERE w.model = '2022-2023' AND w.survey = 'TRD'
```

#### DACSO (Diploma/Certificate)
```sql
-- Q003: Create DACSO cohort
SELECT d.coci_pen, d.coci_stqu_id, d.coci_subm_cd,
       d.cosc_grad_status_lgds_cd_group,
       d.lcp4_cd, d.ttrain,
       CASE WHEN d.pfst_current_activity = 3 THEN 1
            WHEN d.pfst_current_activity = 2 AND d.labr_employed_full_part_time = 1 THEN 1
            WHEN d.pfst_current_activity = 2 AND d.labr_employed_full_part_time = 0 THEN 2
            ELSE 0 END AS new_labour_supply,
       w.weight
INTO t_dacso_data_part_1
FROM t_dacso_data_part_1_stepa d
INNER JOIN t_weights w ON d.coci_subm_cd = w.subm_cd
WHERE w.model = '2022-2023' AND w.survey = 'DACSO'
```

### Weight Calculation (Q005 Series)

#### Z01-Z04: Weight Probability
```sql
-- Z01: Base counts
SELECT survey, survey_year, inst_cd, age_group_rollup,
       COUNT(*) AS Count
INTO DACSO_Q005_Z01_Base_NLS
FROM T_Cohorts_Recoded
WHERE new_labour_supply IN (0, 1, 2, 3)
GROUP BY survey, survey_year, inst_cd, age_group_rollup

-- Z02: Calculate weight probability
SELECT *,
       CAST(Count AS FLOAT) / CAST(Respondents AS FLOAT) AS Weight_Prob
INTO DACSO_Q005_Z02c_Weight
FROM DACSO_Q005_Z02b_Respondents

-- Z04: Adjustment factor
SELECT *,
       CAST(Base AS FLOAT) / CAST(Weighted AS FLOAT) AS Weight_Adj_Fac
INTO DACSO_Q005_Z04_Weight_Adj_Fac
FROM DACSO_Q005_Z03_Weight_Total
```

#### Z05-Z08: Final Weights
```sql
-- Z05: Calculate Weight_NLS
SELECT *,
       weight * Weight_Adj_Fac AS Weight_NLS
INTO tmp_tbl_Weights_NLS
FROM DACSO_Q005_Z02c_Weight

-- Z08: Update T_Cohorts_Recoded
UPDATE T_Cohorts_Recoded
SET Weight_NLS = z05.Weight_NLS
FROM T_Cohorts_Recoded tcr
INNER JOIN tmp_tbl_Weights_NLS z05
  ON tcr.lcip4_cred = z05.lcip4_cred
  AND tcr.grad_status = z05.grad_status
```

### Occupation Distribution (Q009-Q010)

```sql
-- Q009: Weighted occupation counts
SELECT survey, pssm_credential, pssm_cred,
       current_region_pssm_code_rollup, lcp4_cd, ttrain,
       lcip4_cred, lcip2_cred, noc_cd,
       SUM(Weight_OCC) AS Count
INTO DACSO_Q009_Weight_Occs
FROM T_Cohorts_Recoded
WHERE new_labour_supply IN (1, 3)
  AND noc_cd IS NOT NULL
GROUP BY survey, pssm_credential, pssm_cred,
         current_region_pssm_code_rollup, lcp4_cd, ttrain,
         lcip4_cred, lcip2_cred, noc_cd

-- Q010: Calculate percentages
SELECT q.*,
       q.Count / q.Total AS Percent
INTO DACSO_Q010_Weighted_Occs_Dist
FROM DACSO_Q009b_Weighted_Occs q
INNER JOIN DACSO_Q009b_Weighted_Occs_Total t
  ON q.survey = t.survey
  AND q.lcp4_cd = t.lcp4_cd
  AND q.ttrain = t.ttrain
```

---

## Processing Flow

```
┌─────────────────────────────────────────────────────────────────┐
│              Survey Data (TRD/APPSO/BGS/DACSO)                │  (Source)
└───────────────────────────┬─────────────────────────────────────┘
                            │
          ┌───────────────┬─┴───────────────┬───────────────┐
          │               │                 │               │
          ▼               ▼                 ▼               ▼
┌───────────────┐ ┌───────────────┐ ┌───────────────┐ ┌───────────────┐
│TRD Cohort     │ │APPSO Cohort  │ │BGS Cohort     │ │DACSO Cohort  │
│Building       │ │Building      │ │Building       │ │Building      │
└───────┬───────┘ └───────┬───────┘ └───────┬───────┘ └───────┬───────┘
        │                 │                 │                 │
        └─────────────────┴─────────────────┴─────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                   T_Cohorts_Recoded                             │  (Unified)
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│              Weight Calculations (Q005 Z-Series)                │
│  - Weight_NLS (New Labour Supply)                               │
│  - Weight_OCC (Occupation)                                     │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│              Distribution Tables                               │
│  - Labour_Supply_Distribution                                  │
│  - Occupation_Distributions                                   │
│  - *_LCP2 (2-digit CIP versions)                             │
│  - *_No_TT (without training type)                            │
└─────────────────────────────────────────────────────────────────┘
```

---

## Weight Formula Summary

```
Weight_Prob = Count / Respondents
Weighted = Respondents × Weight_Prob × Weight_Year
Weight_Adj_Fac = Base / Weighted
Weight_NLS = Weight × Weight_Adj_Fac
Weight_OCC = Weight_NLS_Base × Weight_Adj_Fac
```

---

## See Also

- **Main Scripts**: `R/02b-*.R` (Fully refactored to dplyr)
- **Related Folders**: `sql/load-cohort-*.R` (Data loading)
- **Lookup Tables**: `t_weights`, `tbl_age`, `t_current_region_pssm_codes`
- **Output Used By**: `R/03-07-*.R` (Projection scripts)
