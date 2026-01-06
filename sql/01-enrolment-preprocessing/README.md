# 01-enrolment-preprocessing

## Overview

This folder contains SQL queries for cleaning, validating, and preprocessing STP (Student Transition Point) enrolment data.

---

## WHAT: What Does This Code Do?

### Core Functions

| Function | Description |
|----------|-------------|
| **Data Validation** | Check for null PENs, duplicate records, invalid dates |
| **Record Classification** | Assign RecordStatus flags (0-8) to each enrolment record |
| **Date Cleaning** | Convert date formats (yy-mm-dd to yyyy-mm-dd) |
| **Birthdate Validation** | Clean and validate student birthdates |
| **Enrolment Tracking** | Identify minimum and first enrolment records per student |
| **Quality Assurance** | Create filtered `STP_Enrolment_Valid` table |

### Source Table
- `STP_Enrolment` - Raw enrolment data from STP system

### Output Tables

| Table | Purpose |
|-------|---------|
| `STP_Enrolment_Record_Type` | Tracks RecordStatus, MinEnrolment, FirstEnrolment for each record |
| `STP_Enrolment_Valid` | Filtered records meeting quality criteria (RecordStatus = 0) |
| `MinEnrolment_ID_PEN` | Records with minimum enrolment by PEN |
| `MinEnrolment_ID_STUID` | Records with minimum enrolment by Student ID |
| `FirstEnrolment_ID_PEN` | Records with first enrolment by PEN |
| `FirstEnrolment_ID_STUID` | Records with first enrolment by Student ID |
| Various `Drop_*` tables | Records flagged for exclusion |

---

## WHY: Why Is This Necessary?

### Data Quality Issues
The raw `STP_Enrolment` table contains records that should not be included in the PSSM model:
- Students without valid identifiers (PEN, Student ID)
- Developmental education records
- Skills-based courses without credentials
- Students outside BC
- Continuing education without credentials

### RecordStatus System
The 9-level RecordStatus system ensures only valid, relevant records are processed:
- **0 = Good**: Record meets all quality criteria
- **1-5 = Excluded**: Records that don't meet basic criteria
- **6-8 = Review**: Records requiring additional review (Skills Based, Developmental CIP, Recommendations)

### Key Processing Steps
1. **Primary Key Setup**: Add ID column for tracking
2. **PEN Validation**: Exclude records without identifiers
3. **Program Type Filtering**: Exclude developmental/skills-based
4. **Geographic Filtering**: Exclude students outside BC
5. **Enrolment Tracking**: Identify first/minimum enrolment per student
6. **Birthdate Cleaning**: Validate and standardize dates

---

## HOW: How It Works

### Scripts in This Folder

| Script | Purpose |
|--------|---------|
| `01-enrolment-preprocessing-sql.R` | Main ETL queries (60+ queries) |
| `convert-date-scripts.R` | Date format conversion (11 steps) |
| `pssm-birthdate-cleaning.R` | Birthdate validation and cleaning (21 steps) |

### Main Script: 01-enrolment-preprocessing-sql.R

#### Section 1: Initial Checks
```r
qry00a_check_null_epens  # Count null PENs
qry00b_check_unique_epens  # Count unique PENs
```

#### Section 2: Primary Key Setup
```r
qry00c_CreateIDinSTPEnrolment  # Add IDENTITY column
qry00d_SetPKeyinSTPEnrolment  # Set primary key
```

#### Section 3: Create Record Type Table
```r
qry01_ExtractAllID_into_STP_Enrolment_Record_Type  # Initialize tracking table
```

#### Sections 4-8: Record Status Classification

| Status | Query Pattern | Meaning |
|--------|--------------|---------|
| 1 | qry02a-c | Missing PEN or Student ID |
| 2 | qry03a-b | Developmental study level |
| 6 | qry03c-j | Skills-based courses |
| 7 | qry03k-l | Developmental CIP codes |
| 3 | qry04a-b | No PSI transition |
| 4 | qry05a-b | Credential only (no enrolment) |
| 5 | qry06a-b | PSI outside BC |
| 0 | qry07 | All remaining records |

#### Section 9: Minimum Enrolment
```r
qry09a-c_MinEnrolmentPEN  # By PEN for students with valid PEN
qry10a-c_MinEnrolmentSTUID  # By Student ID for students without PEN
qry11a-c_Update_MinEnrolment*  # Update tracking table
```

#### Section 10: First Enrolment Date
```r
qry12a-c_FirstEnrolmentPEN  # First enrolment by PEN
qry13a-c_FirstEnrolmentSTUID  # First enrolment by Student ID
qry14a-c_Update_FirstEnrolment*  # Update tracking table
```

### Record Status Codes

| Code | Name | Description |
|------|------|-------------|
| 0 | Valid | Passes all quality checks |
| 1 | No PEN/ID | Missing student identifier |
| 2 | Developmental | Developmental education |
| 3 | No Transition | No PSI transition status |
| 4 | Credential Only | Has credential but no enrolment |
| 5 | Outside BC | Attending PSI outside BC |
| 6 | Skills Based | Skills-based course only |
| 7 | Developmental CIP | CIP code indicates development |
| 8 | Recommendation | Based on recommendation |

---

## Key Query Examples

### Example 1: Check Null PENs
```sql
SELECT COUNT(*) AS n_null_epens
FROM STP_Enrolment
WHERE ENCRYPTED_TRUE_PEN IN ('', ' ', '(Unspecified)')
   OR ENCRYPTED_TRUE_PEN IS NULL;
```

### Example 2: Flag Missing PEN Records
```sql
SELECT id, PSI_STUDENT_NUMBER, PSI_CODE, ENCRYPTED_TRUE_PEN
INTO tmp_tbl_qry02a_Record_With_PEN_Or_STUID
FROM STP_Enrolment
WHERE (PSI_STUDENT_NUMBER NOT IN('', ' ', '(Unspecified)')
   AND PSI_CODE NOT IN('', ' ', '(Unspecified)'))
   OR (ENCRYPTED_TRUE_PEN NOT IN('', ' ', '(Unspecified)'));
```

### Example 3: Update Record Status
```sql
UPDATE STP_Enrolment_Record_Type
SET RecordStatus = 1
FROM STP_Enrolment_Record_Type
INNER JOIN Drop_No_PEN_or_No_STUID
  ON STP_Enrolment_Record_Type.ID = Drop_No_PEN_or_No_STUID.ID;
```

### Example 4: Minimum Enrolment by PEN
```sql
SELECT ENCRYPTED_TRUE_PEN, PSI_SCHOOL_YEAR,
       MIN(PSI_ENROLMENT_SEQUENCE) AS MinPSIEnrolmentSequence
INTO tmp_tbl_qry09a_MinEnrolmentPEN
FROM STP_Enrolment_Valid
GROUP BY ENCRYPTED_TRUE_PEN, PSI_SCHOOL_YEAR
HAVING ENCRYPTED_TRUE_PEN NOT IN('', ' ', '(Unspecified)');
```

---

## Processing Flow

```
┌─────────────────────┐
│   STP_Enrolment     │  (Source)
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│  Add ID Column      │  qry00c, qry00d
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│ Create Record_Type  │  qry01
└─────────┬───────────┘
          │
          ▼
┌─────────────────────────────────────┐
│  Classify Records (qry02-qry06)     │
│  - PEN validation                   │
│  - Program type filtering           │
│  - Geographic filtering             │
└─────────┬───────────────────────────┘
          │
          ▼
┌─────────────────────────────────────┐
│  Identify Min/First Enrolment       │  qry09-qry14
└─────────┬───────────────────────────┘
          │
          ▼
┌─────────────────────────────────────┐
│  Create STP_Enrolment_Valid         │  qry08a
│  (RecordStatus = 0 only)            │
└─────────────────────────────────────┘
```

---

## Tables Created and Dropped

### Persistent Tables (Kept)
- `STP_Enrolment_Record_Type`
- `STP_Enrolment_Valid`

### Temporary Tables (Created and Dropped)
- `tmp_tbl_qry02a_Record_With_PEN_Or_STUID`
- `Drop_No_PEN_or_No_STUID`
- `Drop_Developmental`
- `Drop_Skills_Based`
- `Keep_Skills_Based`
- `Drop_ContinuingEd`
- `Drop_ContinuingEd_More`
- `tmp_tbl_SkillsBasedCourses`
- `Suspect_Skills_Based`
- `Drop_Developmental_CIPS`
- `MinEnrolment_ID_PEN`
- `MinEnrolment_ID_STUID`
- `FirstEnrolment_ID_PEN`
- `FirstEnrolment_ID_STUID`

---

## See Also

- **Main Script**: `R/01a-enrolment-preprocessing.R`
- **Related Folders**: `sql/01-credential-preprocessing/`, `sql/01-credential-analysis/`
- **Config**: `config.yml` for database connection settings
