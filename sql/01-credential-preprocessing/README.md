# 01-credential-preprocessing

## Overview

This folder contains SQL queries for preprocessing STP credential data.

---

## WHAT: What Does This Code Do?

### Core Functions

| Function | Description |
|----------|-------------|
| **Credential Cleaning** | Clean and standardize credential data |
| **Visa Status Processing** | Identify domestic vs international students |
| **Supplemental Variables** | Add calculated fields for analysis |
| **Data Validation** | Check for data quality issues |

### Source Tables
- `STP_Credential` - Raw credential awards from STP system

### Output Tables
- `Credential_Non_Dup` - Deduplicated credential records

---

## WHY: Why Is This Necessary?

### Data Quality Issues
The raw `STP_Credential` table may contain:
- Duplicate credential records
- Invalid visa status values
- Missing or inconsistent CIP codes
- Unmapped credential categories

### Processing Goals
1. **Deduplication**: Remove duplicate credential records
2. **Visa Classification**: Categorize students as domestic/international
3. **CIP Code Mapping**: Ensure valid CIP codes for all records
4. **Supplemental Variables**: Add calculated fields for downstream analysis

---

## HOW: How It Works

### Script: 01a-credential-preprocessing.R

#### Main Processing Steps

| Step | Description |
|------|-------------|
| 1 | Load raw credential data |
| 2 | Identify and handle duplicates |
| 3 | Clean visa status values |
| 4 | Map credential categories |
| 5 | Add supplemental variables |
| 6 | Create final deduplicated table |

### Key Processing Logic

```sql
-- Example: Clean visa status
UPDATE Credential_Non_Dup
SET PSI_VISA_STATUS = CASE
    WHEN PSI_VISA_STATUS IS NULL THEN 'UNKNOWN'
    WHEN PSI_VISA_STATUS = 'PR' THEN 'DOMESTIC'
    WHEN PSI_VISA_STATUS = 'WP' THEN 'INTERNATIONAL'
    ELSE PSI_VISA_STATUS
END
```

---

## Key Query Categories

### Deduplication Queries
- Identify duplicate credential records
- Select canonical record for each duplicate set
- Create deduplicated output table

### Visa Status Queries
- Clean NULL/missing values
- Standardize status codes
- Categorize as domestic/international

### Supplemental Variable Queries
- Add calculated fields
- Join with lookup tables
- Derive new variables from existing data

---

## Processing Flow

```
┌─────────────────────┐
│   STP_Credential    │  (Source)
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│  Identify Duplicates│
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│  Clean Visa Status  │
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│  Add Supplemental   │
│  Variables          │
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│  Credential_Non_Dup │  (Output)
└─────────────────────┘
```

---

## See Also

- **Main Script**: `R/01b-credential-preprocessing.R`
- **Related Folders**: `sql/01-enrolment-preprocessing/`, `sql/01-credential-analysis/`
- **Output Table**: `Credential_Non_Dup` - Used by program matching scripts
