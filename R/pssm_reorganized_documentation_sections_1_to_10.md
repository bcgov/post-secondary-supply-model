# Post-Secondary Supply Model (PSSM) Documentation

## Reorganized Technical Documentation Draft

**Document status:** Working draft  
**Prepared for:** PSSM documentation reorganization  
**Scope:** Sections 1–10  

---

## Documentation Structure

1. **Introduction and Model Overview**
2. **System Architecture and Data Flow**
3. **Data Sources — What goes into the model**
4. **Data Storage and Access — SQL Server, Oracle, LAN**
5. **Pipeline Overview — End-to-end flow**
6. **Data Loading and Preprocessing — 01a–01d**
7. **Cohort Construction and Program Matching — 02a–02b**
8. **Model Runs and Projections — 02b–07**
9. **Special Data Processes — Census, Imputation, PTIB**
10. **Outputs and Reporting — 08 and downstream use**

---

# 1. Introduction and Model Overview

## 1.1 What the model does

The **Post-Secondary Supply Model (PSSM)** estimates future supply of people with post-secondary education in British Columbia.

It answers questions such as:

- How many graduates will enter the labour market?
- What fields of study do they come from?
- What occupations are they likely to work in?
- How does supply vary by region, age group, credential, and field of study?

## 1.2 How the model works

The model combines several data sources to follow students through a simplified lifecycle:

```text
Education records
        |
        v
Credentials / graduates
        |
        v
Labour market outcomes
        |
        v
Program and occupation projections
```

The main source groups are:

- **STP data** — public post-secondary enrolment and credential records.
- **Student Outcomes data** — survey-based employment, occupation, region, and further education outcomes.
- **Statistics Canada Census data** — supplemental labour supply and occupation distributions, especially for advanced credentials and suppressed cells.
- **PTIB data** — private training institution enrolments and graduates.

## 1.3 Core dimensions

The model relies on a consistent set of dimensions across sources:

- **Credential type** — diploma, degree, certificate, apprenticeship, and related groups.
- **Field of study** — usually based on CIP codes.
- **Occupation** — usually based on NOC codes.
- **Region** — PSSM region and region rollup codes.
- **Age group** — standardized model age bands.
- **Survey / source** — STP, APPSO, BGS, DACSO, TRD, Census, PTIB.

## 1.4 Purpose of this document

This document is intended to:

- explain how the full PSSM pipeline works;
- document key data sources and transformations;
- help new analysts understand and maintain the model;
- serve as a single reference for both process and code logic.

---

# 2. System Architecture and Data Flow

## 2.1 Overview

The current PSSM architecture is centred on:

- **R** for orchestration, data loading, scripted transformations, and reporting automation.
- **SQL Server on `decimal.idir.bcgov`** for core storage, large joins, and persistent model tables.
- **LAN-based files** for raw source files, lookup tables, Census exports, PTIB files, and provisioned Student Outcomes extracts.

Historical workflows used Access, Excel, Citrix, and `\\alder\S52018$`. These should be labelled as **legacy** unless a specific old workflow still depends on them.

## 2.2 High-level architecture

```text
+----------------------+
| Raw LAN Files        |
| CSV / XLSX / IVT     |
+----------+-----------+
           |
           v
+----------------------+
| R Load Scripts       |
| DBI / odbc / config  |
+----------+-----------+
           |
           v
+----------------------+
| SQL Server           |
| decimal.idir.bcgov   |
+----------+-----------+
           |
           v
+----------------------+
| R + SQL Pipeline     |
| cleaning / joins     |
| projections          |
+----------+-----------+
           |
           v
+----------------------+
| Final Tables         |
| Excel / CSV / views  |
+----------------------+
```

## 2.3 Core architecture components

### R — execution layer

R is responsible for:

- orchestrating the workflow;
- loading raw data into SQL Server;
- applying transformations and business logic;
- executing SQL queries;
- writing intermediate and final outputs.

The model is not designed as one monolithic script. Scripts are sourced in sequence and depend on run flags and tables created earlier.

### SQL Server — data layer

SQL Server stores:

- raw loaded tables;
- cleaned source tables;
- intermediate working tables;
- lookup tables;
- distribution tables;
- projection outputs;
- final reporting tables.

### LAN — input and staging layer

LAN storage is used for:

- raw STP files;
- Student Outcomes extracts;
- PTIB data;
- Census exports;
- lookup CSVs;
- validation and manual review files.

## 2.4 End-to-end flow

```text
LAN Files  →  R Load Scripts  →  SQL Server Tables
                                   ↓
                           R Transformation Scripts
                                   ↓
                            SQL Derived Tables
                                   ↓
                           Projection Steps
                                   ↓
                          Final Output Tables
                                   ↓
                           Excel / CSV / dashboards
```

## 2.5 Key constraints

- Execution order matters.
- Table names and schemas must be consistent.
- Many scripts assume prior tables already exist.
- Code fields should remain character to preserve leading zeros.
- Legacy paths and Access references should be clearly marked.

---

# 3. Data Sources — What Goes Into the Model

## 3.1 Overview

The PSSM integrates several data sources. Each source has a different role.

```text
          +--------------------+
          | STP Data           |
          | education system   |
          +---------+----------+
                    |
                    v
          +------------------------+
          | Student Outcomes       |
          | labour transitions     |
          +---------+--------------+
                    |
                    v
      +-----------------------------+
      | Core Model Outputs          |
      | supply projections          |
      +-------------+---------------+
                    ^
                    |
      +-------------+--------------+
      |                            |
+-------------+             +-------------+
| Census Data |             | PTIB Data   |
| gap filling |             | private     |
+-------------+             +-------------+
```

## 3.2 Source summary

| Source | Purpose | Key output | Typical owner / provider |
|---|---|---|---|
| STP | Tracks public post-secondary enrolments and credentials | `STP_Enrolment`, `STP_Credential` | ECC / PSFS |
| Student Outcomes | Captures labour market outcomes after education | APPSO, BGS, DACSO, TRD cohort tables | Student Outcomes team |
| Statistics Canada Census | Fills suppressed or sparse occupation/labour supply cells | `STAT_CAN`, Census-derived distributions | Statistics Canada |
| PTIB | Adds private training institution estimates | PTIB staging and projection tables | PTIB |
| Lookup/crosswalk files | Standardize categories | age, region, credential, CIP, NOC lookups | PSSM team |

## 3.3 STP data

STP is the primary education source. It contributes:

- enrolment records;
- credential award records;
- institution and program information;
- CIP fields;
- student identifiers;
- school year and credential timing.

Main tables:

```text
STP source files
      |
      +--> STP_Enrolment
      |
      +--> STP_Credential
```

## 3.4 Student Outcomes data

Student Outcomes data provides survey-based information about what graduates do after leaving education.

Main survey groups:

- **APPSO** — apprenticeship outcomes.
- **BGS** — bachelor graduate outcomes.
- **DACSO** — diploma, associate certificate, and related outcomes.
- **TRD** — trades-related outcomes.

Flow:

```text
INFOWARE / Oracle
        |
        v
Student Outcomes provisioned extracts
        |
        v
R load scripts
        |
        v
SQL Server *_raw tables
        |
        v
standardized cohort tables
```

## 3.5 Census data

Custom Census data supports:

- advanced credentials;
- suppressed NOC cells;
- occupation distributions where Student Outcomes sample sizes are small;
- labour supply distributions where survey information is weak.

## 3.6 PTIB data

PTIB data contributes private training institution graduates and enrolments. It is self-reported, not a unique headcount, and only covers regulated programs, so it must be validated separately from STP.

## 3.7 Lookup and crosswalk tables

Key lookups include:

- age group lookups;
- region rollups;
- credential groupings;
- CIP lookups;
- NOC lookups;
- weights;
- exclusion and suppression tables.

---

# 4. Data Storage and Access — SQL Server, Oracle, LAN

## 4.1 Overview

PSSM uses a SQL Server-centred architecture. SQL Server on `decimal.idir.bcgov` is the main system of record. LAN folders hold raw files and lookups. R scripts connect to both.

```text
LAN files → R scripts → SQL Server → R/SQL transformations → final outputs
```

## 4.2 Main storage layers

### LAN file storage

Used for raw and supporting files:

- STP source files;
- Student Outcomes extracts;
- Census exports;
- PTIB files;
- lookup CSVs;
- validation workbooks.

### SQL Server

Used for:

- raw loaded tables;
- cleaned tables;
- intermediate tables;
- lookup tables;
- projection tables;
- final reporting tables.

### R session memory

Used for transformations, but not as the long-term source of truth. Important outputs should be written to SQL Server.

## 4.3 Database naming

Historically, model-cycle databases have followed names such as:

```text
PSSMYYYY
```

where `YYYY` refers to the model or first projection year.

## 4.4 Table naming conventions

| Pattern | Meaning | Example |
|---|---|---|
| `STP_*` | STP source or processed table | `STP_Enrolment` |
| `*_raw` | raw loaded extract | `APPSO_Data_01_Final_raw` |
| `T_*` | working or lookup table | `T_Cohorts_Recoded` |
| `tbl_*` | lookup/supporting table | `tbl_Age` |
| `tmp_*` | temporary/staging table | `tmp_tbl_Model` |
| `*_Distribution` | distribution table | `Occupation_Distributions` |
| `*_Projections` | projection output | `Graduate_Projections` |

## 4.5 How R connects to SQL Server

Typical connection pattern:

```r
db_config <- config::get("decimal")

con <- dbConnect(
  odbc(),
  Driver = db_config$driver,
  Server = db_config$server,
  Database = db_config$database,
  Trusted_Connection = "True"
)
```

Common packages:

- `DBI`
- `odbc`
- `config`
- `RODBC`
- `RJDBC` for some Oracle workflows

## 4.6 Oracle / INFOWARE access

Student Outcomes source data originates in Oracle / INFOWARE. Current preferred approach:

```text
PSSM team prepares SQL/data request
        |
        v
Student Outcomes team provisions extract
        |
        v
CSV files transferred securely
        |
        v
R loads extracts to SQL Server
```

Direct Oracle access is restricted and should be handled according to Student Outcomes protocols.

## 4.7 Major table groups

```text
Raw source tables:
  STP_Enrolment
  STP_Credential
  Student Outcomes *_raw tables
  STAT_CAN
  PTIB staging tables

Preprocessing tables:
  STP_Enrolment_Record_Type
  STP_Credential_Record_Type
  MinEnrolment
  Credential_Non_Dup

Cohort and distribution tables:
  T_Cohorts_Recoded
  Labour_Supply_Distribution
  Occupation_Distributions

Projection/output tables:
  Graduate_Projections
  Cohort_Program_Distributions_Static
  Cohort_Program_Distributions_Projected
  tmp_tbl_Model
  tmp_tbl_QI
```

## 4.8 Risks

| Risk | Mitigation |
|---|---|
| Wrong schema or environment | Use config profiles and log schema at runtime |
| Lost leading zeros in codes | Load code fields as character |
| PC sleep during large upload | Disable sleep during long loads |
| Legacy paths | Mark legacy infrastructure clearly |
| Table overwrite | Validate schema and row counts before writing |

## 4.9 Practical table inventory format

For every important table, document:

```text
Table:
Layer:
Source:
Created by:
Used by:
Key fields:
Refresh frequency:
Notes:
```

---

# 5. Pipeline Overview — End-to-End Flow

## 5.1 Purpose

This section explains how the full pipeline runs. PSSM is not a set of standalone scripts; it is a sequenced workflow controlled by orchestrator and prep scripts.

## 5.2 Orchestration structure

```text
+--------------------------------------------------+
| run_all_three_model_runs.r                       |
+-------------------------+------------------------+
                          |
                          v
+--------------------------------------------------+
| prep-for-fresh-run.R                             |
| prep-for-qi-run.R                                |
| prep-for-ptib-run.R                              |
+-------------------------+------------------------+
                          |
                          v
+--------------------------------------------------+
| numbered scripts 01a–08                          |
+--------------------------------------------------+
```

## 5.3 Main run types

```text
Regular run → QI run → PTIB run
```

- **Regular run:** full base model.
- **QI run:** quality-indicator variant, focused on labour and occupation stability.
- **PTIB run:** private training institution integration.

## 5.4 Numbered workflow

```text
01a–01d  STP preprocessing
02a      Program matching
02b      Cohort construction and distributions
03       Near-completers
04       Graduate projections
05       PTIB analysis
06       Program projections
07       Occupation projections
08       Reporting
```

## 5.5 Dependency map

```text
Load scripts
   |
   v
01a–01d STP prep
   |
   v
02a Program matching
   |
   v
02b Cohorts/distributions
   |
   v
03 Near-completers
   |
   v
04 Graduate projections
   |
   v
05 PTIB
   |
   v
06 Program projections
   |
   v
07 Occupation projections
   |
   v
08 Reporting
```

## 5.6 Major load scripts

- `load-stp-enrol.R`
- `load-stp-cred.R`
- `load-outcomes-data.R`
- `load-cohort-appso.R`
- `load-cohort-bgs.R`
- `load-cohort-dacso.R`
- `load-cohort-trd.R`
- `load-custom-stats-can.R`
- `load-graduate-projections.R`
- `load-near-completers-ttrain.R`
- `load-occupation-projections.R`
- `load-program-projections.R`
- `load-ptib.R`

## 5.7 Practical run checklist

Before running:

- Confirm database, schema, and config.
- Confirm LAN paths are accessible.
- Confirm source files exist.
- Confirm Student Outcomes extracts are loaded.
- Confirm run flags are correct.
- Confirm logging is enabled.

After each major stage:

- Check row counts.
- Check key tables exist.
- Check missing/null keys.
- Check age, region, credential, CIP, and NOC distributions.
- Check logs for warnings and errors.

---

# 6. Data Loading and Preprocessing — 01a–01d

## 6.1 Purpose

The `01a–01d` workflow prepares raw STP enrolment and credential data for downstream modelling.

Main outputs:

- `STP_Enrolment_Record_Type`
- `STP_Credential_Record_Type`
- `MinEnrolment`
- `Credential_Non_Dup`

## 6.2 STP enrolment loading

```text
LAN STP enrolment files
        |
        v
load-stp-enrol.R
        |
        v
STP_Enrolment
        |
        v
enrolment preprocessing
```

`STP_Enrolment` contains student identifiers, institution, school year, program, credential category, CIP code, birthdate, gender, and related fields.

## 6.3 STP credential loading

```text
LAN STP credential file
        |
        v
load-stp-cred.R
        |
        v
STP_Credential
        |
        v
credential preprocessing
```

`STP_Credential` contains credential award records, including award date, institution, student identifiers, credential category, credential level, and credential CIP.

## 6.4 Record status logic

Record status separates usable records from excluded records.

```text
+--------------+--------------------------------+
| RecordStatus | Meaning                        |
+--------------+--------------------------------+
| 0            | Good / usable record           |
| 1            | Missing PEN or student ID      |
| 2            | Developmental                  |
| 3            | No PSI transition              |
| 4            | Credential only                |
| 5            | Outside BC                     |
| 6            | Skills-based                   |
| 7            | Developmental CIP              |
| 8            | Recommendation for certification|
| 9            | Dropped credential category    |
+--------------+--------------------------------+
```

Flow:

```text
Raw table
   |
   v
Apply exclusion rules
   |
   v
Assign RecordStatus
   |
   +--> RecordStatus = 0 → keep
   |
   +--> RecordStatus != 0 → exclude or retain for audit
```

## 6.5 Age cleaning

Age cleaning creates:

- age at enrolment;
- age at credential award;
- age group;
- invalid/missing age flags.

```text
Birthdate fields
        |
        v
Clean dates
        |
        v
Calculate age at event
        |
        v
Join to age lookup
        |
        v
Assign age group
```

## 6.6 Gender cleaning

Gender cleaning handles:

- missing gender;
- unknown gender;
- multiple gender values for a student;
- imputation where required.

```text
Raw gender values
        |
        v
identify conflicts / missing values
        |
        v
apply resolution or imputation rule
        |
        v
validate counts
```

## 6.7 Credential deduplication

Credential deduplication creates `Credential_Non_Dup`.

```text
STP_Credential
      |
      v
record status filtering
      |
      v
age / gender cleaning
      |
      v
deduplicate credentials
      |
      v
Credential_Non_Dup
```

## 6.8 MinEnrolment

`MinEnrolment` is the cleaned enrolment table used to identify first or minimum enrolment records.

```text
STP_Enrolment
        |
        v
STP_Enrolment_Record_Type
        |
        v
MinEnrolmentSupVar
        |
        v
MinEnrolment
```

## 6.9 Row count checks

Track counts at these checkpoints:

```text
Raw STP_Enrolment
Raw STP_Credential
Enrolment RecordStatus counts
Credential RecordStatus counts
Valid records retained
Missing/invalid ages
Missing/imputed gender
MinEnrolment final count
Credential_Non_Dup final count
```

## 6.10 Validation checks

Required checks:

1. Tables exist.
2. Row counts match expected source counts.
3. Key identifiers are usable.
4. Code fields retain leading zeros.
5. RecordStatus values are valid.
6. Age values are in expected ranges.
7. Gender values are valid or imputed.
8. Duplicate credentials are removed.
9. Final outputs are reasonable against prior model cycles.

## 6.11 Known risks

- Some manual checks from Access/Excel workflows may not be fully replicated.
- Skills-based and developmental exclusions can differ from older cycles.
- Age and gender imputation can cause small differences from historical manual fixes.
- Deduplication rule changes can affect projections.

---

# 7. Cohort Construction and Program Matching — 02a–02b

## 7.1 Purpose

This section documents:

- `02a` — program matching;
- `02b` — cohort construction, labour supply distributions, and occupation distributions.

The key outputs are:

- updated `Credential_Non_Dup`;
- `T_Cohorts_Recoded`;
- `Labour_Supply_Distribution`;
- `Occupation_Distributions`.

## 7.2 02a — Program matching

Program matching assigns the best available final CIP classification to credential records.

Inputs include:

- `Credential_Non_Dup`;
- BGS matching inputs;
- DACSO matching inputs;
- APPSO matching inputs;
- GRAD matching inputs;
- INFOWARE CIP lookup tables;
- institution and program crosswalks.

Outputs include:

```text
Credential_Non_Dup_BGS_IDs
Credential_Non_Dup_GRAD_IDs
Credential_Non_Dup_Programs_DACSO_FinalCIPs
Credential_Non_DIP_APPSO_IDs
updated Credential_Non_Dup
```

Flow:

```text
Credential_Non_Dup
        |
        +--> BGS matching
        +--> DACSO matching
        +--> APPSO matching
        +--> GRAD matching
        |
        v
update final CIP fields
        |
        v
model-ready credential table
```

## 7.3 02b — Cohort construction

The cohort workflow standardizes APPSO, BGS, DACSO, and TRD into a common structure.

```text
APPSO ----+
BGS ------+
DACSO ----+----> T_Cohorts_Recoded
TRD ------+
```

Cohort load scripts:

- `load-cohort-appso.R`
- `load-cohort-bgs.R`
- `load-cohort-dacso.R`
- `load-cohort-trd.R`

## 7.4 Standardized cohort fields

Common fields include:

- survey source;
- survey year;
- respondent identifier;
- credential;
- CIP fields;
- NOC fields;
- age group;
- region code;
- `NEW_LABOUR_SUPPLY`;
- weights;
- respondent flags.

## 7.5 Labour supply distribution

```text
T_Cohorts_Recoded
        |
        v
classify labour supply
        |
        v
apply weights
        |
        v
aggregate by CIP / credential / age / region
        |
        v
Labour_Supply_Distribution
```

## 7.6 Occupation distributions

```text
T_Cohorts_Recoded
        |
        v
filter to labour supply / valid occupation records
        |
        v
apply weights
        |
        v
aggregate by CIP / credential / NOC / age / region
        |
        v
Occupation_Distributions
```

Missing labour/occupation combinations may be appended with zero counts to keep distribution tables structurally complete.

## 7.7 Key lookup tables

```text
T_Weights
T_BGS_INST_Recode
tbl_Age
tbl_Age_Groups
tbl_Age_Groups_Rollup
T_PSSM_Credential_Grouping
T_year_survey_Year
T_Current_Region_PSSM_Codes
T_Current_Region_PSSM_Rollup_Codes
T_NOC_Broad_Categories
INFOWARE_L_CIP_* tables
```

## 7.8 Validation checks

Program matching:

1. Count records before and after matching.
2. Count matches by source.
3. Count remaining null final CIP values.
4. Review CIP changes.
5. Review unmatched records by institution.

Cohort construction:

1. Confirm all source cohorts loaded.
2. Confirm age groups and region codes assigned.
3. Confirm credentials and CIPs valid.
4. Confirm labour supply flags populated.
5. Check weighted and unweighted counts.
6. Check extreme weights.

## 7.9 Known risks

- Manual matching may not be fully replicated.
- CIP values can differ by source.
- Extreme weights can distort distributions.
- Missing NOC or CIP values create gaps.
- Legacy Access/Excel logic should be marked clearly.

---

# 8. Model Runs and Projections — 02b–07

## 8.1 Purpose

This section documents how the model moves from cohort/distribution tables into projection outputs.

It covers:

- near-completers;
- graduate projections;
- PTIB integration;
- program projections;
- occupation projections;
- regular, QI, and PTIB run differences.

## 8.2 Run types

```text
Regular run → QI run → PTIB run
```

### Regular run

Base model run. Produces core graduate, program, labour supply, and occupation projections.

### QI run

Quality-indicator variant. Focuses on labour supply and occupation distribution stability.

### PTIB run

Adds private training institution data and updates selected projection outputs.

## 8.3 Stage 03 — Near-completers

Near-completers are students who nearly completed a program but did not receive a credential in the expected way.

```text
Student Outcomes near-completer records
        |
        v
match against STP credentials
        |
        +--> remove those who later/earlier received credentials
        |
        v
calculate ratios by age / gender / CIP / year
        |
        v
near-completer ratio tables
```

Main outputs:

- `NearCompleters_CIP4`
- `T_DACSO_Near_Completers_RatioAgeAtGradCIP4`
- `T_DACSO_Near_Completers_RatioByGender`

## 8.4 Stage 04 — Graduate projections

Graduate projections forecast future graduates by credential, age group, gender, field of study, region, and year.

```text
Credential_Non_Dup
        |
        v
historical graduate counts
        |
        v
population projections
        |
        v
enrolment / graduation rates
        |
        v
near-completer adjustments
        |
        v
Graduate_Projections
```

Main outputs:

- `Graduate_Projections`
- `Graduate_Projections_Include_Historical`

## 8.5 Stage 05 — PTIB integration

PTIB adds private training institution graduates and enrolments.

```text
PTIB raw file
     |
     v
clean credential / CIP / age / immigration fields
     |
     v
aggregate graduates and enrolments
     |
     v
create PTIB projection rows
     |
     v
add to graduate / program / occupation outputs
```

Main outputs:

- `T_Private_Institutions_Credentials_Raw`
- `T_PTIB_Y1_to_Y10`
- PTIB rows in graduate and program projection tables.

## 8.6 Stage 06 — Program projections

Program projections distribute projected graduates across program or CIP groups.

```text
Graduate_Projections
        |
        v
historical cohort/program distributions
        |
        v
near-completer ratios
        |
        v
apprenticeship logic
        |
        v
PTIB adjustments
        |
        v
Cohort_Program_Distributions_Static
Cohort_Program_Distributions_Projected
```

## 8.7 Stage 07 — Occupation projections

Occupation projections apply labour supply and occupation distributions to graduate/program projections.

```text
Graduate_Projections
        |
        v
Cohort_Program_Distributions
        |
        v
Labour_Supply_Distribution
        |
        v
Occupation_Distributions
        |
        v
occupation projection tables
```

Main outputs:

- `Q_4_NOC_1D_Totals_by_PSSM_CRED`
- `tmp_tbl_Model`
- `tmp_tbl_QI`
- `tmp_tbl_Model_Inc_Private_Inst`

## 8.8 TTRAIN and proxy logic

Some records lack detailed `TTRAIN` values. The model uses No_TT and proxy tables where detailed information is missing.

```text
Program with TTRAIN → use detailed distribution
Program without TTRAIN → use No_TT / proxy distribution
If CIP4 unavailable → aggregate or proxy at CIP2
```

## 8.9 Validation checks

Near-completers:

- count before and after credential matching;
- check removals for earlier/later credentials;
- validate ratios by age, gender, CIP, and year.

Graduate projections:

- check totals by year, credential, age, gender, and CIP;
- compare historical years to source counts;
- confirm exclusions and rounding.

PTIB:

- validate source row count;
- validate CIP and credential recoding;
- confirm exclusions;
- check domestic/international split.

Program projections:

- check distribution sums;
- check proxy use;
- check apprenticeship, near-completer, and PTIB rows.

Occupation projections:

- check labour supply totals;
- check occupation percentages;
- check NOC rollups and exclusions;
- compare regular, QI, and PTIB totals.

---

# 9. Special Data Processes — Census, Imputation, PTIB

## 9.1 Purpose

This section pulls together special workflows that do not fit cleanly into the main STP / Student Outcomes sequence:

- custom Census / Statistics Canada data;
- Census NOC imputation;
- Census occupation distributions;
- Census labour supply distributions;
- PTIB-specific cleaning and integration.

## 9.2 Census workflow

```text
Statistics Canada custom table
        |
        v
Beyond 20/20 .ivt file
        |
        v
manual export to CSV / XLS
        |
        v
load-custom-stats-can.R
        |
        v
STAT_CAN
        |
        +--> NOC imputation
        +--> occupation distribution process
        +--> labour supply distribution process
```

## 9.3 `STAT_CAN`

**Layer:** Raw Census source table.  
**Created by:** `load-custom-stats-can.R`.  
**Used by:** NOC imputation, Census occupation distributions, and Census labour supply distributions.  
**Key fields:** geography, age group, credential, labour force status, attendance at school, CIP grouping, NOC, count fields.  
**Notes:** Validate encoding, geography names, header names, and suppression behaviour.

## 9.4 Census NOC imputation

NOC imputation estimates suppressed detailed occupation counts using parent NOC totals and known child counts.

```text
Detailed NOC count present → keep observed count
Detailed NOC count suppressed → estimate using parent total and known distribution
```

Key scripts:

- `run-imputation-by-region.R`
- `noc-imputation.R`

Flow:

```text
STAT_CAN
   |
   v
clean geography / age / CIP / NOC fields
   |
   v
split by region
   |
   v
run NOC imputation
   |
   v
regional new-count files
```

## 9.5 Census occupation distributions

Key script:

```text
occ-dists-census-data.R
```

Flow:

```text
Regional imputed Census files
        |
        v
combine files
        |
        v
apply region lookup / rollup
        |
        v
reshape credential counts
        |
        v
calculate totals and percentages
        |
        v
Occupation_Distributions_Stat_Can
```

## 9.6 Census labour supply distributions

Key script:

```text
labour-supply-dists-census-data.R
```

The workflow uses filtered and unfiltered Census exports. Some credentials follow the standard calculation, while `DOCT` uses special handling due to small counts and suppression.

```text
Filtered / unfiltered Census exports
        |
        v
apply region crosswalk
        |
        v
calculate labour supply
        |
        v
combine credential-specific outputs
        |
        v
Labour_Supply_Distribution_Stat_Can
```

## 9.7 PTIB-specific cleaning and integration

PTIB is handled separately because it is self-reported, not a unique headcount, and outside STP.

```text
PTIB Excel / CSV file
        |
        v
load-ptib.R or PTIB workflow
        |
        v
T_Private_Institutions_Credentials_Raw
        |
        v
clean credential / CIP / age / immigration fields
        |
        v
T_PTIB_Y1_to_Y10
        |
        v
projection rows
```

Core cleaning steps:

- standardize credential labels;
- clean and validate CIP codes;
- exclude ESL, not-for-credit, unclassified, or otherwise excluded records;
- recode age groups;
- recode immigration status;
- aggregate graduates and enrolments;
- calculate domestic/international shares.

## 9.8 Validation checks

Census:

1. Confirm requested dimensions.
2. Confirm geography labels and region mapping.
3. Confirm age, credential, CIP, and NOC levels.
4. Confirm row counts after load.
5. Check encoding issues.

NOC imputation:

1. Confirm all expected regional files were created.
2. Check parent totals before and after imputation.
3. Check negative or impossible counts.
4. Review high-impact cells.

PTIB:

1. Confirm source row count and years.
2. Validate credential recoding.
3. Validate CIP formatting.
4. Confirm exclusions.
5. Check age and immigration recoding.
6. Confirm PTIB rows are added only where intended.

## 9.9 Known risks

- Census suppression and random rounding mean imputed values are approximations.
- Beyond 20/20 exports may require manual reshaping.
- Doctoral labour supply logic is credential-specific and should be reviewed each cycle.
- PTIB is not directly comparable to STP.
- PTIB data may have publication or business sensitivity restrictions.

---

# 10. Outputs and Reporting — 08 and Downstream Use

## 10.1 Purpose

This section documents the final reporting layer. It covers:

- Excel and CSV outputs;
- internal and public releases;
- suppression;
- quality indicators;
- coverage indicators;
- downstream dashboards and analytical products;
- output storage.

## 10.2 Reporting flow

```text
Final SQL model tables
        |
        v
reporting queries / scripts
        |
        +--> internal release
        +--> public release
        +--> appendix tables
        +--> CSV extracts
        +--> dashboards
        +--> AEST-facing CIP/NOC products
```

## 10.3 Main reporting inputs

```text
Graduate_Projections
Graduate_Projections_Include_Historical
Cohort_Program_Distributions_Static
Cohort_Program_Distributions_Projected
Labour_Supply_Distribution
Occupation_Distributions
tmp_tbl_Model
tmp_tbl_QI
tmp_tbl_Model_Inc_Private_Inst
suppression / exclusion tables
QI / CI support tables
```

## 10.4 Output formats

### Excel

Used for:

- public release workbooks;
- internal release workbooks;
- appendix tables;
- occupation projection tables;
- CIP-to-NOC and NOC-to-CIP graph workbooks;
- validation workbooks.

### CSV

Best for:

- Power BI and dashboards;
- reproducible downstream analysis;
- version comparison;
- flat data extracts.

### SQL tables / views

SQL Server should remain the source of truth. Excel and CSV files should be treated as extracts.

## 10.5 Internal vs public release

```text
Final model tables
        |
        v
validation checks
        |
        +-----------------------------+
        |                             |
        v                             v
Internal release                Public release
more detail                     suppressed / filtered
diagnostics included            diagnostics removed
all years if needed             release-safe detail
```

Internal outputs may include diagnostics and unsuppressed detail for approved users. Public outputs apply suppression, filtering, rounding, and removal of internal-only fields.

## 10.6 Suppression

Suppression protects against releasing small, sensitive, or unstable cells.

Suppression may apply to:

- low NOC counts;
- small cells;
- unstable QI values;
- poor coverage;
- invalid or unknown NOC values;
- excluded credentials, CIPs, or regions.

```text
Final projection cell
        |
        v
check count threshold
        |
        v
check QI
        |
        v
check coverage
        |
        v
check exclusions
        |
        +--> keep
        +--> suppress / aggregate / exclude
```

## 10.7 Quality indicators

Quality indicators help determine whether outputs are stable enough for reporting.

```text
Regular model output
        |
        v
QI run / QI tables
        |
        v
compare stability
        |
        v
calculate QI
        |
        v
flag or filter public outputs
```

QI values should be clearly labelled as quality/stability measures, not projected counts.

## 10.8 Coverage indicators

Coverage indicators compare public-only estimates with public + private estimates.

```text
Coverage Indicator =
public post-secondary NOC count
--------------------------------
public + private NOC count
```

They help explain how much PTIB/private institution data affects projected occupation supply.

## 10.9 Reporting queries and downstream outputs

Examples include:

- `Qry_10a_Model_Public_Release`
- `Qry_10a_Model_QI_PPCI`
- appendix queries for graduate projections
- `CIP_to_NOC`
- `CIP_Totals`
- `NOC_to_CIP`
- `NOC_Totals`

CIP-to-NOC and NOC-to-CIP outputs support occupational outcomes reporting and AEST-facing products.

## 10.10 Dashboard and analytical use

Final outputs can feed:

- Power BI dashboards;
- Excel workbooks;
- AEST briefing tables;
- occupational outcomes products;
- internal validation tools;
- appendix tables;
- future PSSM model runs.

For dashboards, use SQL views or versioned CSV extracts instead of manually edited Excel workbooks.

## 10.11 Output storage

### SQL Server

Retain final model tables in SQL Server as the authoritative source.

### LAN reporting folders

Store Excel, CSV, validation workbooks, and review files in model-cycle reporting folders.

### SharePoint / AEST delivery

Store delivery products such as Access databases, Excel Top 10 files, and documentation where AEST users can access them.

### GitHub

Store code and reproducible documentation in the project repository.

## 10.12 Reporting validation checklist

Before producing outputs:

1. Confirm all final SQL tables exist.
2. Confirm row counts.
3. Confirm graduate projection totals.
4. Confirm program distributions sum correctly.
5. Confirm labour supply and occupation distributions are valid.
6. Confirm PTIB rows are included only where intended.
7. Confirm QI and coverage indicators are calculated.
8. Confirm suppression and exclusion rules are current.

Before public release:

1. Confirm small cells are suppressed.
2. Confirm unstable QI cells are removed or flagged.
3. Confirm internal-only fields are removed.
4. Confirm rounded outputs match release rules.
5. Confirm public workbooks do not expose suppressed values.
6. Confirm release notes document limitations.

## 10.13 Known reporting risks

- Suppression after percentage calculation can confuse visible percentages.
- Internal data can be accidentally released if public filters are incomplete.
- QI can be misunderstood as a projection rather than a quality measure.
- PTIB coverage can be misunderstood if not documented.
- Dashboards can drift if built from manually edited Excel files.

---

# Final Documentation Rule

Every PSSM section should document:

```text
Purpose
Inputs
Scripts
Tables created or updated
Key fields
Validation checks
Known risks
Downstream use
```

This keeps the documentation useful for onboarding, maintenance, auditability, and future model cycles.
