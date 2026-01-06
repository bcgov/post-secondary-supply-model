# 02a-program-matching

## Overview

This folder contains SQL queries for matching institutional programs to standardized CIP (Classification of Instructional Programs) codes.

---

## WHAT: What Does This Code Do?

### Core Functions

| Function | Description |
|----------|-------------|
| **Program Matching** | Match institutional programs to CIP codes |
| **CIP Code Normalization** | Standardize CIP codes across data sources |
| **XWALK Maintenance** | Maintain crosswalk between programs and CIPs |
| **Credential Updates** | Update credential tables with matched CIPs |
| **NULL Recovery** | Recover missing CIP codes using heuristics |

### Source Tables
- `Credential_Non_Dup` - Deduplicated credential records
- `INFOWARE_L_CIP_*` - CIP lookup tables (2D, 4D, 6D)
- `INFOWARE_PROGRAMS` - Program catalog
- `INFOWARE_BGS_*` - BGS outcomes tables

### Output Tables
- `T_BGS_Data_Final_for_OutcomesMatching` - BGS outcomes with CIP codes
- `Credential_Non_Dup_BGS_IDs` - BGS credentials with matched CIPs
- `Credential_Non_Dup_GRAD_IDs` - GRAD credentials with matched CIPs
- `DACSO_STP_ProgramsCIP4_XWALK_ALL_*` - Program-to-CIP crosswalk

---

## WHY: Why Is This Necessary?

### CIP Code Standardization
Different data sources use different coding systems:
- **STP**: Uses PSI_CREDENTIAL_CIP (6-digit with periods)
- **BGS**: Uses CIP4DIG, CIP2DIG (various formats)
- **Infoware**: Uses LCIP_* codes

### Matching Methods by Survey

| Survey | Method | Key Fields |
|--------|--------|------------|
| **APPSO** | Aggregation | Group by PSI_PROGRAM_CODE, aggregate CIPs |
| **BGS** | PEN-based Matching | Match on PSI_PEN, then use outcomes matching |
| **DACSO** | XWALK Lookup | Join via COCI_INST_CD + PRGM_LCPC_CD |

### Program Matching Goals
1. **Consistency**: Ensure all programs have valid CIP codes
2. **Accuracy**: Use best available match method per survey
3. **Auditability**: Track which records were matched vs. filled

---

## HOW: How It Works

### Scripts in This Folder

| Script | Purpose | Matching Method |
|--------|---------|-----------------|
| `02a-appso-programs.R` | APPSO program aggregation | Group/Aggregate |
| `02a-bgs-program-matching.R` | BGS credential matching | PEN-based |
| `02a-dacso-program-matching.R` | DACSO program matching | XWALK lookup |
| `02a-update-cred-non-dup.R` | Update credential tables | N/A - Consolidation |
| `02a-convert-leftover-nulls.R` | Recover NULL CIPs | Heuristic |

### BGS Matching (02a-bgs-program-matching.R)

#### Step 1: Build Outcomes Data
```sql
-- Create T_BGS_Data_Final_for_OutcomesMatching
SELECT INFOWARE_BGS_COHORT_INFO.PEN,
       INFOWARE_BGS_DIST_19_23.STQU_ID,
       INFOWARE_BGS_COHORT_INFO.CIP4DIG,
       INFOWARE_BGS_COHORT_INFO.CIP_4DIGIT_NO_PERIOD,
       ...
INTO T_BGS_Data_Final_for_OutcomesMatching
FROM INFOWARE_BGS_DIST_19_23
INNER JOIN INFOWARE_BGS_COHORT_INFO
  ON INFOWARE_BGS_DIST_19_23.STQU_ID = INFOWARE_BGS_COHORT_INFO.STQU_ID
```

#### Step 2: Clean STP CIP Codes
```sql
-- Normalize STP CIP codes (6-digit to 4-digit)
SELECT PSI_CREDENTIAL_CIP,
       CASE WHEN LEN(PSI_CREDENTIAL_CIP) = 6
            THEN LEFT(PSI_CREDENTIAL_CIP, 2) + '0' + RIGHT(PSI_CREDENTIAL_CIP, 3)
            ELSE PSI_CREDENTIAL_CIP END AS STP_CIP_CODE_4
FROM Credential_Non_Dup
```

#### Step 3: PEN-Based Matching
```sql
-- Match BGS records to STP credentials by PEN
SELECT BGS.PEN, BGS.STQU_ID, BGS.CIP_4DIGIT_NO_PERIOD,
       STP.FINAL_CIP_CODE_4, STP.ID
FROM T_BGS_Data_Final_for_OutcomesMatching BGS
INNER JOIN Credential_Non_Dup STP
  ON BGS.PEN = STP.PSI_PEN
WHERE BGS.PEN IS NOT NULL
```

### DACSO Matching (02a-dacso-program-matching.R)

#### XWALK-Based Matching
```sql
-- Match using crosswalk table
SELECT DACSO.COCI_INST_CD, DACSO.PRGM_LCPC_CD,
       XWALK.CIP_CODE_4, XWALK.LCP4_CIP_4DIGITS_NAME
FROM t_dacso_data_part_1_stepa DACSO
INNER JOIN DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23 XWALK
  ON DACSO.COCI_INST_CD = XWALK.COCI_INST_CD
  AND DACSO.PRGM_LCPC_CD = XWALK.PRGM_LCPC_CD
```

### NULL Recovery (02a-convert-leftover-nulls.R)

#### Heuristic Matching for Missing CIPs
```sql
-- Try 6-digit match first
SELECT c.*, l.LCIP_LCP4_CD
FROM Credential_Non_Dup c
LEFT JOIN INFOWARE_L_CIP_6DIGITS_CIP2016 l
  ON c.PSI_CREDENTIAL_CIP = l.LCIP_CD_WITH_PERIOD
WHERE c.FINAL_CIP_CODE_4 IS NULL

-- Fallback to 2-digit match
UPDATE c
SET FINAL_CIP_CODE_4 = l.LCIP_LCP4_CD
FROM Credential_Non_Dup c
INNER JOIN INFOWARE_L_CIP_2DIGITS_CIP2016 l
  ON LEFT(c.PSI_CREDENTIAL_CIP, 2) = l.LCP2_CD
WHERE c.FINAL_CIP_CODE_4 IS NULL
```

---

## Processing Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                    Credential_Non_Dup                          │  (Source)
└───────────────────────────┬─────────────────────────────────────┘
                            │
        ┌───────────────────┼───────────────────┐
        │                   │                   │
        ▼                   ▼                   ▼
┌───────────────┐   ┌───────────────┐   ┌───────────────┐
│     BGS       │   │    DACSO      │   │    APPSO      │
│ PEN Matching  │   │  XWALK Lookup │   │  Aggregation  │
└───────┬───────┘   └───────┬───────┘   └───────┬───────┘
        │                   │                   │
        └───────────────────┼───────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                 Credential_Non_Dup_Updated                      │  (Consolidated)
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                 NULL Recovery (02a-convert-leftover-nulls)       │
└─────────────────────────────────────────────────────────────────┘
```

---

## Key CIP Code Formats

| Format | Example | Source |
|--------|---------|--------|
| 6-digit with period | `51.0204` | STP (PSI_CREDENTIAL_CIP) |
| 4-digit no period | `5102` | BGS (CIP_4DIGIT_NO_PERIOD) |
| 4-digit with period | `51.02` | Infoware (LCIP_LCP4_CD) |
| 2-digit | `51` | Cluster level |

---

## See Also

- **Main Scripts**: `R/02a-*.R` (Refactored versions)
- **Related Folders**: `sql/01-credential-preprocessing/`
- **Lookup Tables**: `INFOWARE_L_CIP_*`, `INFOWARE_PROGRAMS`
- **Output Used By**: `R/02b-pssm-cohorts.R`
