# 05-ptib-analysis

## Overview

This folder contains SQL queries for analyzing Private Training Institutions (PTIB) data in British Columbia. PTIB stands for Private Training Institutions Branch, the regulatory body for private post-secondary institutions.

---

## WHAT: What Does This Code Do?

### Core Functions

| Function | Description |
|----------|-------------|
| **Credential Mapping** | Map raw PTIB credentials to PSSM credential categories |
| **CIP Validation** | Validate and clean 7-digit CIP codes |
| **Immigration Status Analysis** | Analyze domestic vs international student patterns |
| **Graduation Counts** | Calculate graduate counts by credential/age/CIP |
| **Cohort Distributions** | Build program-CIP-age cohort distributions |
| **Trend Analysis** | Calculate 2-year averages for smoothing |

### Source Tables
- `T_Private_Institutions_Credentials_Raw` - Raw PTIB credential data
- `T_PSSM_Credential_Grouping` - PSSM credential mapping lookup
- `INFOWARE_L_CIP_6DIGITS_CIP2016` - Valid CIP code lookup
- `T_PTIB_Y1_to_Y10` - Year mapping for projections

### Output Tables Created
- `T_Private_Institutions_Credentials` - Cleaned credential data
- `qry_Private_Credentials_01a-01f_*` - Various graduation count tables
- `qry_Private_Credentials_05i1_Grads_by_Year` - Year-mapped graduates
- `qry_Private_Credentials_06b-06d_*` - Cohort distributions

---

## WHY: Why Is This Necessary?

### Private Institution Context
- **PTIB-regulated institutions** differ from public institutions
- Different **credential naming conventions** (non-standardized)
- May offer **non-credential programs** not tracked elsewhere
- Important for **complete supply picture** in BC

### Key Analysis Needs
1. **Credential Standardization**: Map varied PTIB credential names to PSSM categories
2. **Data Quality**: Validate CIP codes (7-digit requirement)
3. **Immigration Breakdown**: Separate domestic from international for accurate supply modeling
4. **Missing Data Handling**: Handle blank/unknown immigration status with imputation
5. **Trend Smoothing**: Use 2-year averages to reduce year-to-year noise

---

## HOW: How It Works

### Script Structure

#### Part 1: Data Cleaning & Preparation

##### Step 1a: Credential Mapping
```sql
-- Map raw PTIB credentials to PSSM categories
SELECT Year, Credential, CIP, Age_Group, Immigration_Status,
       Graduates, Enrolled_Not_Graduated, Enrolment
INTO T_Private_Institutions_Credentials
FROM T_PSSM_Credential_Grouping
INNER JOIN T_Private_Institutions_Credentials_Raw
  ON T_PSSM_Credential_Grouping.PRGM_Credential_Awarded_Name = T_Private_Institutions_Credentials_Raw.Credential
WHERE PSSM_Credential IS NOT NULL  -- Exclude unmapped credentials
  AND Credential <> 'None'         -- Exclude "no credential" records
```

##### Step 1b: CIP Validation & Cleaning
```sql
-- Remove periods from CIP codes
UPDATE T_Private_Institutions_Credentials
SET LCIP_CD = Replace(LCIP_CD, '.', '')

-- Validate 7-digit CIPs against Infoware lookup
SELECT T_Private_Institutions_Credentials.LCIP_CD
FROM T_Private_Institutions_Credentials
LEFT JOIN INFOWARE_L_CIP_6DIGITS_CIP2016
  ON INFOWARE_L_CIP_6DIGITS_CIP2016.LCIP_CD = T_Private_Institutions_Credentials.LCIP_CD
WHERE INFOWARE_L_CIP_6DIGITS_CIP2016.LCIP_CD IS NULL  -- Invalid CIPs
```

##### Step 1c: Age Group Recoding
```sql
-- Convert dashes to "to" format for consistency
UPDATE T_Private_Institutions_Credentials
SET Age_Group = Replace(Age_Group, '-', ' to ')
```

##### Step 1d: 2-Year Averages
```sql
-- Calculate 2021-2022 averages for smoothing
INSERT INTO T_Private_Institutions_Credentials
SELECT 'Avg 2021 & 2022' AS Year, Credential, LCIP_CD, Age_Group,
       Immigration_Status,
       SUM(Enrolment)/2 AS AvgOfEnrolment,
       SUM(Enrolled_Not_Graduated)/2 AS AvgOfEnrolment_Not_Graduated,
       SUM(Graduates)/2 AS AvgOfGraduates,
       Exclude
FROM T_Private_Institutions_Credentials_Clean
GROUP BY Credential, LCIP_CD, Age_Group, Immigration_Status, Exclude
HAVING Exclude IS NULL
```

#### Part 2: Immigration Status Processing

##### Step 2a: Count by Immigration Status
```sql
-- Count domestic graduates by credential/age/CIP
SELECT '2023/2024' AS Year, Credential, LCIP_CD, Age_Group,
       SUM(IIF(Immigration_Status='Domestic', Graduates, 0)) AS Domestic
INTO qry_Private_Credentials_01a_Domestic
FROM T_Private_Institutions_Credentials
WHERE Exclude IS NULL AND Graduates IS NOT NULL
  AND Credential IN ('CERT', 'DIPL')
GROUP BY Credential, LCIP_CD, Age_Group
```

##### Step 2b: Calculate Domestic Percentage
```sql
-- Calculate percent domestic for handling blanks
SELECT Domestic.Year, Domestic.Credential, Domestic.LCIP_CD, Domestic.Age_Group,
       Domestic.Domestic,
       Domestic_International.Domestic_International,
       IIF(Domestic=0, 0, Domestic/Domestic_International) AS Percent_Domestic
INTO qry_Private_Credentials_01c_Percent_Domestic
FROM qry_Private_Credentials_01a_Domestic AS Domestic
INNER JOIN qry_Private_Credentials_01b_Domestic_International
  ON Domestic.Credential = Domestic_International.Credential
  AND Domestic.LCIP_CD = Domestic_International.LCIP_CD
  AND Domestic.Age_Group = Domestic_International.Age_Group
```

##### Step 2c: Handle Blank Immigration Status
```sql
-- Impute domestic count for blank/unknown immigration status
SELECT Year, Credential, LCIP_CD, Age_Group,
       Graduates * Percent_Domestic AS Graduates_Blank
INTO qry_Private_Credentials_01d_Grads_Blank
FROM T_Private_Institutions_Credentials
INNER JOIN qry_Private_Credentials_01c_Percent_Domestic
  ON T_Private_Institutions_Credentials.Credential = qry_Private_Credentials_01c_Percent_Domestic.Credential
  AND T_Private_Institutions_Credentials.LCIP_CD = qry_Private_Credentials_01c_Percent_Domestic.LCIP_CD
WHERE Immigration_Status IN ('(blank)', 'Unknown') AND Exclude IS NULL
```

##### Step 2d: Union and Sum
```sql
-- Combine domestic + imputed blank counts
SELECT Year, Credential, LCIP_CD, Age_Group, Domestic
INTO qry_Private_Credentials_01f_Grads
FROM qry_Private_Credentials_01a_Domestic
UNION ALL
SELECT Year, Credential, LCIP_CD, Age_Group, Graduates_Blank
FROM qry_Private_Credentials_01d_Grads_Blank
```

#### Part 3: Cohort Distribution Building

##### Step 3a: Build Cohort Keys
```sql
-- Create hierarchical cohort identifiers
SELECT Year, Credential,
       'P - ' + Credential AS PSSM_CRED,
       LEFT(LCIP_CD, 4) AS LCP4_CD,
       'P - ' + LEFT(LCIP_CD, 4) + ' - ' + Credential AS LCIP4_CRED,
       'P - ' + LEFT(LCIP_CD, 2) + ' - ' + Credential AS LCIP2_CRED,
       Age_Group,
       SUM(Grads) AS Count
INTO qry_Private_Credentials_06b_Cohort_Dist
FROM qry_Private_Credentials_01f_Grads
GROUP BY Year, Credential, LEFT(LCIP_CD, 4), Age_Group
```

##### Step 3b: Calculate Percentages
```sql
-- Calculate distribution percentages within age groups
SELECT 'PTIB' AS Survey, Credential, PSSM_CRED, LCP4_CD,
       LCIP4_CRED, LCIP2_CRED, Age_Group, Year, Count,
       Total,
       IIF(Total=0, 0, Count/Total) AS Percent
INTO qry_Private_Credentials_06d1_Cohort_Dist
FROM qry_Private_Credentials_06b_Cohort_Dist
INNER JOIN qry_Private_Credentials_06c_Cohort_Dist_Total
  ON qry_Private_Credentials_06b_Cohort_Dist.PSSM_CRED = qry_Private_Credentials_06c_Cohort_Dist_Total.PSSM_CRED
  AND qry_Private_Credentials_06b_Cohort_Dist.Age_Group = qry_Private_Credentials_06c_Cohort_Dist_Total.Age_Group
```

---

## Processing Flow

```
┌─────────────────────────────────────────────────────────────────┐
│              Part 1: Data Cleaning                             │
│  - Map credentials to PSSM categories                          │
│  - Clean CIP codes (remove periods)                            │
│  - Validate against Infoware lookup                            │
│  - Recode age group format                                     │
│  - Calculate 2-year averages                                   │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│              Part 2: Immigration Processing                    │
│  - Count domestic graduates                                    │
│  - Count total graduates (domestic + international + NA)       │
│  - Calculate domestic percentage                               │
│  - Impute domestic count for blank immigration status          │
│  - Union and sum all domestic counts                          │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│              Part 3: Cohort Distribution                       │
│  - Build hierarchical cohort keys (P-CIP-creds)               │
│  - Calculate counts by cohort                                  │
│  - Calculate distribution percentages                         │
│  - Remove excess age groups                                    │
└─────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Output Tables                               │
│  - Graduate_Projections (with year mapping)                   │
│  - Cohort_Program_Distributions_Static                        │
│  - Cohort_Program_Distributions_Projected                    │
└───────────────────────────────────────────────────────────────┘
```

---

## Key Variables

| Variable | Description | Notes |
|----------|-------------|-------|
| `Credential` | Raw PTIB credential name | e.g., "Certificate", "Diploma" |
| `PSSM_Credential` | Standardized credential | e.g., "CERT", "DIPL" |
| `LCIP_CD` | 7-digit CIP code | Cleaned of periods |
| `LCP4_CD` | First 4 digits of CIP | Program level grouping |
| `LCIP4_CRED` | Combined key | "P - [CIP4] - [Credential]" |
| `LCIP2_CRED` | Combined key | "P - [CIP2] - [Credential]" |
| `Immigration_Status` | Domestic/International/blank | |
| `Percent_Domestic` | Domestic proportion | Used for imputation |

---

## PSSM Credential Mapping

| PSSM Credential | Includes |
|-----------------|----------|
| P - CERT | Certificates, attestation of completion |
| P - DIPL | Diplomas, advanced diplomas |
| P - ADGR or UT | Associate degrees, university transfer |
| P - BACH | Bachelor's degrees (rare in PTIB) |
| P - MAST | Master's degrees (rare in PTIB) |
| P - DOCT | Doctoral degrees (rare in PTIB) |

---

## Excluded Records

- Records with `Exclude` flag set
- Records with credential = 'None'
- Invalid CIP codes (not in Infoware lookup)
- Age groups: '(blank)', 'Unknown', '65+', '16 or less'

---

## See Also

- **Main Script**: `R/05-ptib-analysis.R`
- **Related Folders**: `sql/04-graduate-projections/`, `sql/06-program-projections/`
- **Lookup Tables**: `T_PSSM_Credential_Grouping`, `INFOWARE_L_CIP_6DIGITS_CIP2016`
- **Output Tables**: `Graduate_Projections`, `Cohort_Program_Distributions_*`
