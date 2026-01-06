# 01-credential-analysis

## Overview

This folder contains SQL queries for analyzing credential data and creating supplemental analysis variables.

---

## WHAT: What Does This Code Do?

### Core Functions

| Function | Description |
|----------|-------------|
| **Gender Cleaning** | Standardize gender values across data sources |
| **Visa Status Analysis** | Analyze domestic vs international student patterns |
| **Enrollment Variables** | Add enrollment-based calculated fields |
| **Credential Variables** | Create credential-specific analysis variables |
| **Aggregation** | Calculate counts and distributions by various dimensions |

### Source Tables
- `STP_Credential` - Raw credential awards
- `Credential_Non_Dup` - Deduplicated credentials
- `STP_Enrolment` - Enrolment records for cross-reference

### Output Tables/Views
- Analysis aggregations for graduation projections
- Cross-tabulations by credential, gender, age, year

---

## WHY: Why Is This Necessary?

### Data Integration Needs
The credential analysis layer:
1. **Standardizes Variables**: Creates consistent variable definitions across data sources
2. **Enriches Data**: Adds calculated fields from enrollment records
3. **Enables Analysis**: Prepares data for graduation rate calculations
4. **Validates Quality**: Checks data consistency

### Key Analysis Dimensions
- **Credential Category**: Type of credential (Bachelor, Diploma, Certificate, etc.)
- **Gender**: Student gender (standardized)
- **Age Group**: Age at graduation
- **Year**: Award year (with delayed year for lag analysis)
- **Visa Status**: Domestic vs International

---

## HOW: How It Works

### Scripts in This Folder

| Script | Purpose |
|--------|---------|
| `01b-credential-analysis.R` | Main credential analysis queries |
| `credential-sup-vars-additional-gender-cleaning.R` | Gender value standardization |
| `credential-sup-vars-from-enrolment.R` | Enrollment-based variables |
| `credential-non-dup-psi_visa_status.R` | Visa status analysis |

### Main Script: 01b-credential-analysis.R

#### Analysis Categories

| Category | Description |
|----------|-------------|
| Gender Analysis | Standardize and count by gender |
| Age Analysis | Age at graduation groupings |
| Credential Analysis | Counts by credential type |
| Year Analysis | Trends over time |
| Visa Status | Domestic/international breakdown |

### Supplemental Variable Scripts

#### credential-sup-vars-additional-gender-cleaning.R
```sql
-- Standardize gender values
UPDATE Credential_Non_Dup
SET psi_gender_cleaned = CASE
    WHEN psi_gender IN ('M', 'Male') THEN 'Male'
    WHEN psi_gender IN ('F', 'Female') THEN 'Female'
    ELSE 'Unknown'
END
```

#### credential-sup-vars-from-enrolment.R
```sql
-- Add enrollment-based variables
SELECT c.*, e.PSI_SCHOOL_YEAR,
       e.PSI_MIN_START_DATE
INTO Credential_With_Enrolment_Vars
FROM Credential_Non_Dup c
LEFT JOIN STP_Enrolment e ON c.ID = e.ID
```

#### credential-non-dup-psi_visa_status.R
```sql
-- Analyze visa status distribution
SELECT PSI_VISA_STATUS, COUNT(*) AS Count
FROM Credential_Non_Dup
GROUP BY PSI_VISA_STATUS
```

---

## Key Query Categories

### Gender Queries
- Clean raw gender values
- Standardize to Male/Female/Unknown
- Create clean gender variable

### Visa Status Queries
- Identify NULL values
- Standardize status codes
- Flag domestic vs international

### Enrollment Cross-Reference
- Join credentials to enrollments
- Add enrollment dates and sequence
- Calculate time-based variables

### Aggregation Queries
- Count by credential/year/gender/age
- Create pivot tables for analysis
- Generate summary statistics

---

## Processing Flow

```
┌─────────────────────┐
│  Credential_Non_Dup │  (Source)
└─────────┬───────────┘
          │
          ▼
┌─────────────────────────────────────┐
│  Gender Cleaning                    │  gender-cleaning.R
└─────────┬───────────────────────────┘
          │
          ▼
┌─────────────────────────────────────┐
│  Visa Status Analysis              │  visa-status.R
└─────────┬───────────────────────────┘
          │
          ▼
┌─────────────────────────────────────┐
│  Enrollment Cross-Reference         │  enrolment-vars.R
└─────────┬───────────────────────────┘
          │
          ▼
┌─────────────────────────────────────┐
│  Aggregation & Analysis             │  01b-credential-analysis.R
└─────────────────────────────────────┘
```

---

## Key Output Variables

| Variable | Description |
|----------|-------------|
| `psi_gender_cleaned` | Standardized gender (Male/Female/Unknown) |
| `PSI_VISA_STATUS` | Student visa category |
| `AGE_GROUP_AT_GRAD` | Age at graduation (grouped) |
| `PSI_AWARD_SCHOOL_YEAR_DELAYED` | Award year with 1-year lag |
| `Domestic_Flag` | Boolean for domestic students |

---

## See Also

- **Main Script**: `R/01c-credential-analysis.R`
- **Related Folders**: `sql/01-credential-preprocessing/`
- **Output Used By**: `R/04-graduate-projections.R`
