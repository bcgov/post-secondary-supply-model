# 01d-enrolment-analysis

## Overview

This folder contains SQL queries for analyzing enrolment data patterns.

---

## WHAT: What Does This Code Do?

### Core Functions

| Function | Description |
|----------|-------------|
| **Enrolment Analysis** | Analyze enrolment trends and patterns |
| **Age Calculation** | Calculate student age at key points |
| **Trend Analysis** | Identify enrolment trends over time |
| **Cross-Tabulation** | Enrolment by various dimensions |

### Source Tables
- `STP_Enrolment` - Raw enrolment data
- `STP_Enrolment_Valid` - Filtered enrolment records
- `tbl_age` - Age lookup table
- `tbl_age_groups` - Age group definitions

### Output
- Analysis aggregations for enrolment trends
- Age-based groupings for cohort analysis

---

## WHY: Why Is This Necessary?

### Planning and Projection Needs
The enrolment analysis layer:
1. **Understands Trends**: Identifies historical enrolment patterns
2. **Age Demographics**: Analyzes student age distribution
3. **Programs Analysis**: Studies enrolment by program type
4. **Informs Projections**: Provides baseline for graduation forecasts

### Key Analysis Dimensions
- **Year**: Enrolment over time
- **Age Group**: Student age demographics
- **Institution**: Enrolment by PSI
- **Program**: Enrolment by program type
- **Credential**: Enrolment by credential level

---

## HOW: How It Works

### Scripts in This Folder

| Script | Purpose |
|--------|---------|
| `01d-enrolment-analysis.R` | Main enrolment analysis queries |
| `enrolment_age_update.R` | Age calculation and update queries |

### Main Script: 01d-enrolment-analysis.R

#### Analysis Categories

| Category | Description |
|----------|-------------|
| Trend Analysis | Year-over-year enrolment changes |
| Age Analysis | Student age demographics |
| Institutional Analysis | Enrolment by institution |
| Program Analysis | Enrolment by program type |
| Credential Analysis | Enrolment by credential level |

### enrolment_age_update.R

#### Age Calculation Logic
```sql
-- Calculate age at a reference date
SELECT ID, PSI_BIRTHDATE,
       DATEDIFF(YEAR, PSI_BIRTHDATE, REFERENCE_DATE) AS Age_At_Reference
FROM STP_Enrolment_Valid
WHERE PSI_BIRTHDATE IS NOT NULL
```

---

## Key Query Categories

### Trend Queries
- Year-over-year enrolment counts
- Program-specific trends
- Institutional trends

### Age Queries
- Age at enrolment
- Age at graduation (cross-reference)
- Age group assignments

### Aggregation Queries
- Counts by year/institution/program
- Percentage distributions
- Comparative analyses

---

## Processing Flow

```
┌─────────────────────┐
│  STP_Enrolment_Valid│  (Source)
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│  Age Calculation    │  enrolment_age_update.R
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│  Trend Analysis     │  01d-enrolment-analysis.R
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│  Aggregations       │
│  - By Year          │
│  - By Age Group     │
│  - By Institution   │
│  - By Program       │
└─────────────────────┘
```

---

## See Also

- **Main Script**: `R/01d-enrolment-analysis.R`
- **Related Folders**: `sql/01-enrolment-preprocessing/`
- **Lookup Tables**: `tbl_age`, `tbl_age_groups`
