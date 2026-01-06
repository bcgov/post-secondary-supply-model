# 03-near-completers

## Overview

This folder contains SQL queries for analyzing near-completers and calculating TTRAIN (training type) adjustments.

---

## WHAT: What Does This Code Do?

### Core Functions

| Function | Description |
|----------|-------------|
| **Near-Completer Matching** | Match near-completers to STP credentials |
| **Credential Validation** | Check if near-completers later received credentials |
| **TTRAIN Analysis** | Analyze training type distribution |
| **Ratio Calculation** | Calculate near-completer ratios by demographics |

### Source Tables
- `T_DACSO_DATA_Part_1` - DACSO outcomes data
- `Credential_Non_Dup` - Deduplicated credentials
- `STP_Credential` - Raw STP credentials
- `tbl_Age`, `tbl_Age_Groups` - Age lookups

### Output Tables
- `DACSO_Matching_STP_Credential_PEN` - PEN-based matches
- `T_DACSO_Near_Completers_RatioAgeAtGradCIP4` - Ratios by age/CIP
- `T_DACSO_Near_Completers_RatioByGender` - Ratios by gender

---

## WHY: Why Is This Necessary?

### Near-Completer Definition
A **near-completer** is a DACSO respondent who:
1. Responded to the survey
2. Did NOT have a credential at survey time
3. Later received a credential (found in STP_Credential)

### Impact on Projections
Near-completers affect supply projections:
- They should be counted as graduates (eventually)
- They need to be subtracted from "no credential" counts
- They may have different labour market outcomes

### TTRAIN (Training Type)
The model tracks training type:
- **TTRAIN = 1**: Trades/Apprenticeship
- **TTRAIN = 0**: Other credentials

Near-completer analysis helps adjust TTRAIN distributions.

---

## HOW: How It Works

### Scripts in This Folder

| Script | Purpose |
|--------|---------|
| `dacso-near-completers.R` | Main near-completer analysis |
| `near-completers-investigation-ttrain.R` | TTRAIN investigation |

### Main Script: dacso-near-completers.R

#### Step 1: PEN-Based Matching
```sql
-- Match DACSO respondents to STP credentials by PEN
SELECT d.COCI_STQU_ID, d.COCI_PEN, d.COCI_INST_CD,
       d.LCP4_CD, d.COSC_GRAD_STATUS_LGDS_CD_GROUP,
       c.ID, c.PSI_AWARD_SCHOOL_YEAR, c.FINAL_CIP_CODE_4
INTO DACSO_Matching_STP_Credential_PEN
FROM T_DACSO_DATA_Part_1 d
INNER JOIN Credential_Non_Dup c
  ON d.COCI_PEN = c.PSI_PEN
GROUP BY d.COCI_STQU_ID, d.COCI_PEN, d.COCI_INST_CD,
         d.LCP4_CD, d.COSC_GRAD_STATUS_LGDS_CD_GROUP,
         c.ID, c.PSI_AWARD_SCHOOL_YEAR, c.FINAL_CIP_CODE_4
HAVING d.COCI_PEN <> ' '
```

#### Step 2: Match Quality Flags
```sql
-- Flag matches on credential category
UPDATE DACSO_Matching_STP_Credential_PEN
SET match_credential = 'yes'
WHERE prgm_credential_awarded_name = psi_credential_category;

-- Flag matches on CIP4
UPDATE DACSO_Matching_STP_Credential_PEN
SET match_cip_code_4 = 'yes'
WHERE LCP4_CD = FINAL_CIP_CODE_4;

-- Flag matches on award year
UPDATE DACSO_Matching_STP_Credential_PEN
SET match_award_school_year = 'yes'
WHERE (COCI_SUBM_CD = 'C_Outc18' AND PSI_AWARD_SCHOOL_YEAR IN ('2015/2016', '2016/2017'))
   OR (COCI_SUBM_CD = 'C_Outc19' AND PSI_AWARD_SCHOOL_YEAR IN ('2016/2017', '2017/2018'))
   -- ... more year combinations
```

#### Step 3: Identify Near-Completers
```sql
-- Find near-completers who later received credentials
SELECT d.COCI_STQU_ID, d.COCI_PEN, d.LCP4_CD,
       CASE WHEN c.ID IS NOT NULL THEN 'Yes' ELSE 'No' END AS Has_STP_Credential,
       CASE WHEN c.PSI_AWARD_SCHOOL_YEAR < d.COSC_GRAD_STATUS_LGDS_CD_GROUP THEN 'Yes' ELSE 'No' END AS Before_DACSO
INTO NearCompleters_Analysis
FROM T_DACSO_DATA_Part_1 d
LEFT JOIN Credential_Non_Dup c ON d.COCI_PEN = c.PSI_PEN
WHERE d.COSC_GRAD_STATUS_LGDS_CD_GROUP = '3'  -- Near-completer status
```

#### Step 4: Calculate Ratios
```sql
-- Ratio by age group and CIP4
SELECT age_group, lcp4_cred, lcp4_cd,
       COUNT(*) AS Total,
       SUM(CASE WHEN Has_STP_Credential = 'Yes' THEN 1 ELSE 0 END) AS With_STP_Credential,
       COUNT(*) - SUM(CASE WHEN Has_STP_Credential = 'Yes' THEN 1 ELSE 0 END) AS Near_Completers_Only
INTO T_DACSO_Near_Completers_RatioAgeAtGradCIP4
FROM NearCompleters_Analysis
GROUP BY age_group, lcp4_cred, lcp4_cd
```

---

## Processing Flow

```
┌─────────────────────────────────────────────────────────────────┐
│              T_DACSO_DATA_Part_1 (Near-Completers)            │  (Source)
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│              PEN-Based Matching to STP Credentials             │
│  - Match DACSO respondents to STP by PEN                      │
│  - Flag match quality (credential, CIP, year, institution)   │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│              Identify Near-Completers with STP Credentials    │
│  - Has_STP_Credential: Found in STP after DACSO             │
│  - Before_DACSO: Received credential before survey           │
│  - After_DACSO: Received credential after survey            │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│              Calculate Ratios                                 │
│  - By Age Group and CIP4                                     │
│  - By Gender                                                │
│  - By Institution                                          │
└───────────────────────────────────────────────────────────────┘
```

---

## Key Concepts

### Near-Completer Status
| Status | Description |
|--------|-------------|
| Has credential in STP | Found in credential file |
| No credential in STP | Not found (true near-completer) |
| Credential before DACSO | Received credential before survey |
| Credential after DACSO | Received credential after survey |

### Match Quality Flags
| Flag | Meaning |
|------|---------|
| `match_credential` | STP credential matches DACSO credential category |
| `match_cip_code_4` | STP CIP matches DACSO CIP4 |
| `match_award_school_year` | Award years are consistent |
| `match_inst` | Institution codes match |

---

## See Also

- **Main Script**: `R/03-near-completers-ttrain.R`
- **Related Folders**: `sql/02b-pssm-cohorts/`
- **Output Used By**: `R/06-program-projections.R`
