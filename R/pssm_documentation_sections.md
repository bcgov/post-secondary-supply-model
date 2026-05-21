

# Post-Secondary Supply Model (PSSM) Documentation

## Sections 1–4 Draft

**Document status:** Working draft  
**Prepared for:** PSSM documentation reorganization  
**Scope:** Sections 1–4 only  

---

# 1. Introduction and Model Overview

## 1.1 What this model does

The **Post-Secondary Supply Model (PSSM)** estimates future supply of people with post-secondary education in British Columbia.

It answers questions like:

- How many graduates will enter the labour market?
- What fields of study do they come from?
- What occupations are they likely to work in?
- How does supply vary by region, age, and credential type?

---

## 1.2 How the model works

The model combines several data sources to follow students through a simplified lifecycle:

1. **Education system data**  
   Tracks enrolment and credentials from post-secondary institutions.

2. **Graduate outcomes data**  
   Shows whether graduates work, study more, or leave the labour force.

3. **Census data**  
   Fills gaps, especially for small or missing groups such as advanced credentials.

4. **Private training data**  
   Adds estimates for students outside the public post-secondary system.

Together, these sources produce:

- graduate projections
- labour supply estimates
- occupation distributions

---

## 1.3 Core concepts used throughout the model

The model relies on a small set of core dimensions that appear throughout the pipeline:

- **Credential type** — diploma, degree, apprenticeship, certificate, and related groupings
- **Field of study** — based on CIP codes
- **Occupation** — based on NOC codes
- **Region** — based on PSSM regional groupings
- **Age group** — standardized across sources

These dimensions are preserved across processing steps so results can be combined, compared, and projected consistently.

---

## 1.4 How the pipeline is structured

The model is implemented as a step-by-step pipeline in R, supported by SQL Server.

High-level flow:

```text
Load raw data into SQL Server
        ↓
Clean and standardize datasets
        ↓
Build cohorts of students and graduates
        ↓
Estimate transitions from study to work
        ↓
Project future graduates and labour supply
        ↓
Generate outputs for reporting
```

Each step depends on previous steps. The full process is orchestrated through a master R workflow rather than by running one large script manually.

---

## 1.5 Recent workflow changes

Recent versions of the PSSM moved away from a workflow based on Access, Excel, and SQL queries toward a more reproducible workflow based on:

- R scripts
- SQL Server tables
- LAN-based source files
- version-controlled code

This change reduces manual steps and makes the model easier to audit, rerun, and maintain.

---

## 1.6 Purpose of this document

This document is intended to:

- explain how the full PSSM pipeline works
- document key data sources and transformations
- help new analysts understand and maintain the model
- serve as a single reference for process logic and code logic

---

# 2. System Architecture and Data Flow

## 2.1 Overview

The PSSM system uses a hybrid architecture combining:

- **R** for processing and orchestration
- **SQL Server** for storage and large table operations
- **LAN-based files** for raw inputs, lookups, and staged extracts

The system is designed as a linear, dependency-driven pipeline. Each stage writes outputs back to SQL Server so the next stage can read stable inputs.

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
| Excel / reports      |
+----------------------+
```

---

## 2.2 Core architecture components

### 2.2.1 R — execution layer

R is the primary engine that runs the model pipeline.

R is responsible for:

- orchestrating the workflow
- loading raw data into SQL Server
- applying transformations and business logic
- executing SQL queries
- producing intermediate and final datasets

The general execution pattern is:

```r
time_execution("script_name.R")
```

Important behaviour:

- scripts are sourced in sequence
- shared run flags control workflow branches
- some scripts depend on objects or flags created earlier
- scripts should not be treated as fully standalone unless documented that way

Common run flags include:

- `regular_run`
- `qi_run`
- `ptib_run`

---

### 2.2.2 SQL Server — data layer

SQL Server on `decimal.idir.bcgov` is the central storage layer.

SQL Server stores:

- raw source tables
- cleaned tables
- intermediate working tables
- lookup tables
- distribution tables
- projection outputs
- final reporting tables

Typical table lifecycle:

```text
Raw table
   ↓
Cleaned table
   ↓
Derived table
   ↓
Projection table
   ↓
Final output table
```

---

### 2.2.3 LAN file system — input and staging layer

LAN storage is used for:

- raw STP files
- Student Outcomes extracts
- Census exports
- PTIB files
- lookup tables
- staged intermediate files

The general flow is:

```text
LAN file
   ↓
R load script
   ↓
SQL Server table
   ↓
Pipeline processing
```

---

## 2.3 End-to-end data flow

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
                           Excel / Reporting
```

---

## 2.4 Pipeline stages mapped to architecture

### Stage 1 — Data ingestion

- **Source:** LAN files
- **Tool:** R load scripts
- **Output:** SQL Server raw tables

Examples:

- `load-stp-enrol.R` → `STP_Enrolment`
- `load-stp-cred.R` → `STP_Credential`
- `load-outcomes-data.R` → Student Outcomes `*_raw` tables
- `load-custom-stats-can.R` → `STAT_CAN`

---

### Stage 2 — Preprocessing

- **Source:** SQL raw tables
- **Tool:** R and SQL
- **Output:** cleaned and validated tables

Typical transformations:

- clean identifiers
- recode dates
- assign record status
- remove excluded records
- standardize fields used downstream

---

### Stage 3 — Cohort construction

- **Source:** cleaned STP and Student Outcomes data
- **Tool:** R and SQL
- **Output:** standardized cohort tables

This stage standardizes:

- age groups
- region codes
- labour supply flags
- credential labels
- CIP and NOC fields

---

### Stage 4 — Model transformations

- **Source:** cohort tables and lookups
- **Tool:** R and SQL
- **Output:** intermediate model datasets

Examples include:

- near-completers
- graduation rates
- credential projections
- program distributions

---

### Stage 5 — Projections

- **Source:** model datasets and population projections
- **Tool:** R and SQL
- **Output:** projected graduates and projected labour supply

---

### Stage 6 — Occupation mapping

- **Source:** projections and occupation distributions
- **Tool:** R and SQL
- **Output:** occupation-level supply estimates

Inputs include:

- Student Outcomes distributions
- Census-derived supplement tables
- lookup and exclusion tables

---

### Stage 7 — Outputs and reporting

Final tables are stored in SQL Server and may be exported to Excel or reporting tools.

---

## 2.5 Orchestration logic

The full pipeline is controlled by a main entry point:

```text
run_all_three_model_runs.r
```

It coordinates three major runs:

1. **Regular run** — full standard model run
2. **QI run** — quality indicator variant
3. **PTIB run** — private training institution adjustment run

```text
run_all_three_model_runs.r
        |
        +--> prep-for-fresh-run.R
        |
        +--> prep-for-qi-run.R
        |
        +--> prep-for-ptib-run.R
```

Each run sets flags, runs scripts in order, and writes outputs back to SQL Server.

---

## 2.6 Key design principles

### SQL Server is the source of truth

Important intermediate and final outputs should be written to SQL Server.

### Scripts are modular but dependent

Scripts are separated by stage, but many depend on earlier outputs, shared flags, or expected SQL tables.

### Reproducibility is preferred over manual steps

Historical manual work in Access and Excel has been reduced where possible.

### Dimensions must be standardized early

The model depends on consistent definitions of:

- CIP
- NOC
- region
- age group
- credential

---

## 2.7 Known constraints and risks

| Risk | Why it matters |
|---|---|
| Tight coupling between steps | One failed table can affect later stages |
| Heavy reliance on table names | Scripts often expect exact names |
| Global run flags | Wrong flag values can run the wrong branch |
| External file dependencies | Missing LAN files break load scripts |
| Legacy references | Old Access/Citrix paths can confuse new analysts |

---

# 3. Data Sources

## 3.1 Overview

The PSSM integrates multiple data sources to estimate post-secondary supply. Each source plays a specific role.

```text
          +--------------------+
          |   STP Data         |
          |   education system |
          +---------+----------+
                    |
                    v
          +------------------------+
          |   Student Outcomes     |
          |   labour transitions   |
          +---------+--------------+
                    |
                    v
      +-----------------------------+
      |   Core Model Outputs        |
      |   supply projections        |
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

---

## 3.2 Summary table

| Source | Purpose | Key output | Frequency | Owner / provider |
|---|---|---|---|---|
| STP | Tracks students and credentials | enrolment and graduate records | annual/model cycle | ECC / PSFS |
| Student Outcomes | Labour market outcomes | employment, occupation, further study | survey release cycle | Student Outcomes team |
| Statistics Canada Census | Gap filling and advanced credential distributions | occupation and labour supply distributions | Census cycle | Statistics Canada |
| PTIB | Private institution data | enrolments and graduates | annual/model cycle | PTIB |
| INFOWARE / Oracle | Upstream source for Student Outcomes | survey extracts | ongoing | Student Outcomes team |

---

## 3.3 STP data

### What it is

STP data is the primary education data source for the model. It tracks public post-secondary enrolments and credentials.

### Main tables

```text
+----------------+
| STP files      |
+-------+--------+
        |
        v
+---------------------+      +---------------------+
| STP_Enrolment       |      | STP_Credential      |
+---------------------+      +---------------------+
```

### Main datasets

#### `STP_Enrolment`

Contains detailed student enrolment records.

Typical fields:

- `ENCRYPTED_TRUE_PEN`
- `PSI_CODE`
- `PSI_STUDENT_NUMBER`
- `PSI_SCHOOL_YEAR`
- `PSI_PROGRAM_CODE`
- `PSI_CIP_CODE`
- `PSI_CREDENTIAL_CATEGORY`

#### `STP_Credential`

Contains credential award records.

Typical fields:

- `ENCRYPTED_TRUE_PEN`
- `PSI_CODE`
- `PSI_STUDENT_NUMBER`
- `CREDENTIAL_AWARD_DATE`
- `PSI_CREDENTIAL_CIP`
- `PSI_CREDENTIAL_CATEGORY`
- `PSI_CREDENTIAL_LEVEL`

### Role in pipeline

```text
STP data
   ↓
Preprocessing
   ↓
Record status assignment
   ↓
Credential and enrolment analysis
   ↓
Graduate projections
```

### Important processing rules

Many records are excluded from model use, including:

- developmental programs
- skills-based courses
- records with missing identifiers
- records with invalid or unusable program information

A record status flag is used to separate usable and excluded records.

---

## 3.4 Student Outcomes data

### What it is

Student Outcomes data captures what graduates do after leaving education.

Main survey sources:

- DACSO
- BGS
- APPSO
- TRD

### Source-to-table flow

```text
INFOWARE / Oracle
        |
        | extracts provisioned by Student Outcomes team
        v
CSV files on secure transfer / LAN
        |
        v
load-outcomes-data.R
        |
        v
SQL Server *_raw tables
        |
        v
cohort load scripts
        |
        v
standardized cohort tables
```

### Main standardized tables

```text
+----------------------+
| T_APPSO_DATA_Final   |
+----------------------+
| T_BGS_Data_Final     |
+----------------------+
| T_DACSO_DATA_PART... |
+----------------------+
| T_TRD_DATA           |
+----------------------+
          |
          v
+----------------------+
| T_Cohorts_Recoded    |
+----------------------+
```

### Role in pipeline

Student Outcomes data supports:

- labour supply rates
- occupation distributions
- further education indicators
- region and labour force behaviour
- CIP-to-NOC and NOC-to-CIP outputs

---

## 3.5 Statistics Canada Census data

### What it is

Custom Census data fills gaps where Student Outcomes data is too sparse or suppressed.

It is especially important for:

- Master’s degrees
- Doctorates
- graduate certificates or diplomas
- professional degrees
- small occupation groups

### Flow

```text
Beyond 20/20 IVT file
        |
        v
CSV / XLS export
        |
        v
load-custom-stats-can.R
        |
        v
STAT_CAN
        |
        v
NOC imputation
        |
        v
Census distribution tables
```

### Main Census-derived tables

- `STAT_CAN`
- regional imputation output files
- `Occupation_Distributions_Stat_Can`
- `Labour_Supply_Distribution_Stat_Can`

---

## 3.6 PTIB data

### What it is

PTIB data provides information on private training institutions not covered by STP.

### Typical variables

- year
- institution operating name
- program title
- CIP code
- credential
- age group
- immigration status
- graduates
- enrolments
- total enrolments

### Role in pipeline

```text
PTIB file
   ↓
cleanup and aggregation
   ↓
PTIB run adjustment
   ↓
final projection outputs
```

### Key limitations

- self-reported data
- not a unique headcount
- only covers regulated programs
- requires careful quality review

---

## 3.7 INFOWARE / Oracle

INFOWARE is the upstream Oracle source for Student Outcomes data.

Current preferred process:

1. PSSM team prepares data requirements or SQL request.
2. Student Outcomes team provisions extracts.
3. Extracts are transferred securely.
4. R loads extracts into SQL Server.

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
SQL Server decimal
```

---

## 3.8 Lookup and crosswalk files

Lookup tables support consistent categories across sources.

Examples:

- age group lookups
- region rollups
- credential groupings
- CIP lookups
- NOC lookups
- weighting tables

```text
Lookup CSVs on LAN
        |
        v
R load scripts
        |
        v
SQL lookup tables
        |
        v
joins / recodes / projections
```

---

# 4. Data Storage and Access

## 4.1 Overview

PSSM currently uses a SQL Server-centred architecture. The main storage and processing environment is SQL Server on `decimal.idir.bcgov`, with R acting as the orchestration and transformation layer. LAN files are used for raw inputs, lookup files, and staged extracts.

Historical Access, Citrix, and `\\alder\S52018$` references should be treated as legacy unless a specific old workflow requires them.

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
| Excel / reports      |
+----------------------+
```

---

## 4.2 Main storage layers

### 4.2.1 LAN file storage

Used for:

- raw data files
- lookup CSVs
- Census exports
- PTIB files
- Student Outcomes extracts

R reads these files and writes them into SQL Server for processing.

### 4.2.2 SQL Server on decimal

SQL Server is the main system of record for the pipeline.

It stores:

- raw loaded tables
- cleaned tables
- intermediate working tables
- projection tables
- labour supply and occupation distribution tables
- final outputs

### 4.2.3 R session memory

R is used for processing, but important outputs should not live only in memory. Major outputs should be written back to SQL Server.

---

## 4.3 SQL Server database pattern

Historically, a new SQL Server database was created for each model cycle using a name like:

```text
PSSMYYYY
```

where `YYYY` refers to the first prediction year or model cycle.

```text
decimal.idir.bcgov
│
├── PSSM2015       legacy reference
├── PSSM2017       legacy reference
├── PSSM2019       legacy reference
├── PSSM2021       transition/reference
└── PSSM2023       current/recent model cycle
```

---

## 4.4 Table naming conventions

| Pattern | Meaning | Example |
|---|---|---|
| `STP_*` | STP source or processed table | `STP_Enrolment` |
| `*_raw` | raw extract loaded from files | `APPSO_Data_01_Final_raw` |
| `T_*` | working or lookup table | `T_Cohorts_Recoded` |
| `tbl_*` | lookup/supporting table | `tbl_Age` |
| `tmp_*` | temporary or staging table | `tmp_tbl_Age` |
| `qry_*` | legacy query-derived object | `qry_Northeast` |
| `*_Distribution` | distribution table | `Labour_Supply_Distribution` |
| `*_Projections` | projection output | `Graduate_Projections` |

---

## 4.5 Core table groups

### Raw source tables

```text
+-----------------------------+
| Raw Source Tables           |
+-----------------------------+
| STP_Enrolment               |
| STP_Credential              |
| APPSO_*_raw                 |
| BGS_*_raw                   |
| DACSO_*_raw                 |
| TRD_*_raw                   |
| STAT_CAN                    |
| PTIB raw/staged tables      |
+-----------------------------+
```

### Preprocessing tables

```text
+-------------------------------+
| Preprocessing Tables          |
+-------------------------------+
| STP_Enrolment_Record_Type     |
| STP_Credential_Record_Type    |
| MinEnrolmentSupVar            |
| MinEnrolment                  |
| CredentialSupVarsFromEnrolment|
+-------------------------------+
```

### Cohort and outcomes tables

```text
+---------------------------+
| Student Outcomes Tables   |
+---------------------------+
| T_APPSO_DATA_Final        |
| APPSO_Graduates           |
| T_BGS_Data_Final          |
| T_DACSO_DATA_PART_1_STEPA |
| infoware_c_outc_clean_*   |
| T_TRD_DATA                |
| TRD_Graduates             |
| T_Cohorts_Recoded         |
+---------------------------+
```

### Lookup and crosswalk tables

```text
+------------------------------------------+
| Lookup / Crosswalk Tables                |
+------------------------------------------+
| tbl_Age                                  |
| tbl_Age_Groups                           |
| tbl_Age_Groups_Rollup                    |
| AgeGroupLookup                           |
| T_Current_Region_PSSM_Codes              |
| T_Current_Region_PSSM_Rollup_Codes       |
| T_Current_Region_PSSM_Rollup_Codes_BC    |
| T_PSSM_Credential_Grouping               |
| T_PSSM_Projection_Cred_Grp               |
| T_NOC_Broad_Categories                   |
| T_Weights                                |
+------------------------------------------+
```

### Model output tables

```text
+--------------------------------+
| Model Output Tables            |
+--------------------------------+
| Graduate_Projections           |
| Cohort_Program_Distributions   |
| Labour_Supply_Distribution     |
| Occupation_Distributions       |
| CIP_to_NOC                     |
| NOC_to_CIP                     |
+--------------------------------+
```

---

## 4.6 Data flow by table layer

```text
+-----------------------------+
| 1. Raw LAN / Provisioned    |
|    files                    |
+-------------+---------------+
              |
              v
+-----------------------------+
| 2. Raw SQL Tables           |
| STP_Enrolment               |
| STP_Credential              |
| *_raw Student Outcomes      |
| STAT_CAN                    |
+-------------+---------------+
              |
              v
+-----------------------------+
| 3. Cleaning / Preprocessing |
| Record type tables          |
| MinEnrolment                |
| Credential cleanup          |
+-------------+---------------+
              |
              v
+-----------------------------+
| 4. Cohort / Matching Tables |
| Credential_Non_Dup          |
| T_Cohorts_Recoded           |
| Program matching outputs    |
+-------------+---------------+
              |
              v
+-----------------------------+
| 5. Projection Tables        |
| Graduate_Projections        |
| Program distributions       |
| Labour supply distributions |
| Occupation distributions    |
+-------------+---------------+
              |
              v
+-----------------------------+
| 6. Final Outputs            |
| Excel workbooks             |
| public/internal extracts    |
+-----------------------------+
```

---

## 4.7 How R connects to SQL Server

R connects to SQL Server using configuration-driven settings.

Common packages include:

- `DBI`
- `odbc`
- `config`
- `RODBC`
- `RJDBC` for some Oracle-related workflows

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

---

## 4.8 Config values used by the pipeline

Common config values include:

| Config key | Purpose |
|---|---|
| `decimal` | SQL Server connection settings |
| `lan` | LAN root path |
| `myschema` | SQL schema for testing or production |

Typical usage:

```r
lan <- config::get("lan")
my_schema <- config::get("myschema")
db_config <- config::get("decimal")
```

---

## 4.9 SQL operations performed from R

Common database operations:

| Operation | Purpose |
|---|---|
| `dbWriteTable` | write an R data frame to SQL Server |
| `dbWriteTableArrow` | write large Arrow-backed data to SQL Server |
| `dbReadTable` | read a full SQL table into R |
| `dbGetQuery` | run a `SELECT` query and return data |
| `dbExecute` | run a SQL command with no returned dataset |

---

## 4.10 Load script pattern

Most load scripts follow this pattern:

```text
+-----------------------------+
| 1. Load libraries           |
+-------------+---------------+
              |
              v
+-----------------------------+
| 2. Read config              |
| decimal / lan / schema      |
+-------------+---------------+
              |
              v
+-----------------------------+
| 3. Connect to SQL Server    |
+-------------+---------------+
              |
              v
+-----------------------------+
| 4. Read LAN file or SQL     |
+-------------+---------------+
              |
              v
+-----------------------------+
| 5. Clean / recode / type    |
+-------------+---------------+
              |
              v
+-----------------------------+
| 6. Write table to SQL       |
+-------------+---------------+
              |
              v
+-----------------------------+
| 7. Disconnect               |
+-----------------------------+
```

---

## 4.11 Large table handling

Large files require careful handling because upload and transformation steps can be slow.

Recommended practice:

- disable PC sleep during long uploads
- use explicit schemas where possible
- keep code fields as character
- validate row counts after load
- avoid partial append reruns without duplicate checks

```text
Large file risk points:

LAN file read
     |
     v
R memory
     |
     v
SQL upload  <--- risk: slow network / sleep timeout
     |
     v
SQL table validation
```

---

## 4.12 Character-safe loading

Many fields should be stored as character even if they look numeric.

Examples:

- CIP codes
- NOC codes
- region codes
- school years
- institution codes
- student identifiers

```text
Bad:
0103  → 103

Good:
"0103" remains "0103"
```

---

## 4.13 Oracle / INFOWARE access

Student Outcomes source data originates in Oracle INFOWARE tables.

Current preferred process:

```text
+----------------------+
| INFOWARE Oracle      |
| restricted access    |
+----------+-----------+
           |
           | Student Outcomes team provisions extracts
           v
+----------------------+
| Secure transfer      |
| CSV files            |
+----------+-----------+
           |
           v
+----------------------+
| R load script        |
+----------+-----------+
           |
           v
+----------------------+
| SQL Server decimal   |
+----------------------+
```

---

## 4.14 Access, Citrix, and legacy infrastructure

Older PSSM workflows used:

- Microsoft Access front ends
- linked SQL tables
- Citrix Workspace
- `\\alder\S52018$`
- manual Excel edits

These should be documented as historical unless still needed for a specific legacy workflow.

```text
Legacy pattern:

Access front end
      |
      v
Linked SQL tables
      |
      v
Manual Excel edits
      |
      v
SQL update/import

Current pattern:

R script
   |
   v
SQL Server
   |
   v
Version-controlled pipeline
```

---

## 4.15 Permissions and access

Access requirements typically include:

- SQL Server access to the relevant PSSM database on decimal
- LAN access to project folders
- VPN access where required
- access to provisioned Student Outcomes files
- appropriate data-sharing permissions under the relevant agreement

---

## 4.16 Schema and environment management

The pipeline appears to support both:

- personal/testing schemas
- production or `dbo` schema

Recommended documentation rule:

> Every script should clearly state whether it writes to a personal schema, shared test schema, or production schema.

```text
+---------------------+
| Analyst test schema |
+----------+----------+
           |
           | promote when validated
           v
+---------------------+
| Production schema   |
| dbo                 |
+---------------------+
```

---

## 4.17 Temporary and intermediate table cleanup

Many workflow steps create temporary or intermediate tables.

Typical lifecycle:

```text
CREATE / SELECT INTO
        |
        v
UPDATE / JOIN / AGGREGATE
        |
        v
WRITE FINAL TABLE
        |
        v
DROP TEMP TABLE
```

Recommended practice:

- keep intermediate tables only if needed for audit or debugging
- prefix temporary tables consistently with `tmp_`
- log row counts before dropping
- avoid dropping tables before final validation

---

## 4.18 Key risk points

| Risk | Why it matters | Mitigation |
|---|---|---|
| Table overwritten too early | downstream steps may use wrong data | use schemas and row-count checks |
| Wrong config environment | analyst may write to production accidentally | explicit config profile |
| Lost leading zeros | CIP/NOC codes become invalid | load code fields as character |
| PC sleep during upload | SQL connection may drop | disable sleep during long loads |
| Missing lookup table | joins fail or silently drop records | pre-run dependency check |
| Legacy path references | scripts may fail on retired infrastructure | mark legacy paths clearly |
| Global run flags wrong | wrong model branch runs | log run flags at start |

---

## 4.19 Recommended table inventory format

For each major table, document:

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

## 4.20 Section summary

The current PSSM infrastructure should be documented as:

```text
R = orchestration and transformation
SQL Server decimal = central data store
LAN = raw file and lookup storage
Oracle INFOWARE = restricted upstream Student Outcomes source
Access / Citrix / alder = legacy only
```

The key operating principle is:

> Load raw files into SQL Server, transform through R and SQL, write every important intermediate and final output back to SQL Server, and keep the workflow reproducible through scripted steps.

---

# 4.21 Major Table Inventory

This section documents the major SQL Server tables used by the PSSM pipeline. It focuses on tables that are most important for onboarding, troubleshooting, and understanding how data moves through the model.

The tables are grouped by layer:

```text
1. Raw source tables
2. STP preprocessing tables
3. Student Outcomes and cohort tables
4. Program matching tables
5. Census / StatCan tables
6. PTIB tables
7. Lookup and crosswalk tables
8. Projection and distribution tables
9. Final reporting / analytical output tables
```

---

## 4.21.1 Raw Source Tables

## Table: `STP_Enrolment`

**Layer:** Raw source table

**Source:** STP enrolment files loaded from LAN-based files into SQL Server.

**Created by:** `load-stp-enrol.R`

**Used by:**

- enrolment preprocessing
- record status assignment
- `MinEnrolment` construction
- age and gender cleaning
- near-completer and graduate projection workflows

**Key fields:**

- `ENCRYPTED_TRUE_PEN`
- `PSI_CODE`
- `PSI_STUDENT_NUMBER`
- `PSI_SCHOOL_YEAR`
- `PSI_PROGRAM_CODE`
- `PSI_CIP_CODE`
- `PSI_CREDENTIAL_CATEGORY`
- `PSI_STUDY_LEVEL`
- `PSI_ENTRY_STATUS`

**Refresh frequency:** Each model cycle.

**Notes:** This is one of the largest tables in the model. Code and identifier fields should be loaded as character.

---

## Table: `STP_Credential`

**Layer:** Raw source table

**Source:** STP credential file loaded from LAN-based files into SQL Server.

**Created by:** `load-stp-cred.R`

**Used by:**

- credential preprocessing
- credential record status assignment
- credential analysis
- graduate projections
- program matching

**Key fields:**

- `ENCRYPTED_TRUE_PEN`
- `STP_ALT_ID`
- `PSI_CODE`
- `PSI_STUDENT_NUMBER`
- `PSI_SCHOOL_YEAR`
- `CREDENTIAL_AWARD_DATE`
- `PSI_PROGRAM_CODE`
- `PSI_CREDENTIAL_CIP`
- `PSI_CREDENTIAL_CATEGORY`
- `PSI_CREDENTIAL_LEVEL`

**Refresh frequency:** Each model cycle.

**Notes:** Dates may require normalization. Non-standard characters may need to be removed before upload.

---

## Table: `STAT_CAN`

**Layer:** Raw source table

**Source:** Custom Statistics Canada Census export, usually prepared from Beyond 20/20 files and exported to CSV or Excel before loading.

**Created by:** `load-custom-stats-can.R`

**Used by:**

- graduate NOC imputation
- Census occupation distributions
- Census labour supply distributions

**Key fields:**

- `geography`
- `age_group`
- `major_field_cip`
- `occupation_NOC`
- credential count fields
- labour force / attendance fields, depending on the export

**Refresh frequency:** Each Census-based model update.

**Notes:** Encoding issues are common. Census data is important for advanced credentials and suppressed occupation counts.

---

## Table: Student Outcomes `*_raw` tables

**Layer:** Raw source tables

**Source:** CSV extracts provisioned by the Student Outcomes team from INFOWARE / Oracle.

**Created by:** `load-outcomes-data.R`

**Used by:**

- cohort loading scripts
- program matching
- labour supply estimation
- occupation distribution construction

**Examples:**

- `APPSO_Data_01_Final_raw`
- `APPSO_Graduates_raw`
- `BGS_Q001_BGS_Data_2019_2023_raw`
- `DACSO_Q003_DACSO_DATA_Part_1_stepA_raw`
- `Q000_TRD_DATA_01_raw`
- `Q000_TRD_Graduates_raw`
- `infoware_c_outc_clean_short_resp_raw`

**Key fields:** Varies by survey, but generally includes person identifiers, survey year, credential fields, CIP, NOC, labour force fields, region, and age.

**Refresh frequency:** Each model cycle or when a new Student Outcomes release is provisioned.

**Notes:** These tables are source-specific and must be standardized before being used together.

---

## 4.21.2 STP Preprocessing Tables

## Table: `STP_Enrolment_Record_Type`

**Layer:** Preprocessing table

**Source:** Derived from `STP_Enrolment`.

**Created by:** Enrolment preprocessing scripts / SQL queries in the `01` workflow.

**Used by:**

- filtering usable enrolment records
- building `MinEnrolment`
- identifying excluded records

**Key fields:**

- enrolment record identifier
- student identifier
- `RecordStatus`

**Refresh frequency:** Each model cycle after `STP_Enrolment` is loaded.

**Notes:** Record status values identify whether a record is usable or excluded.

---

## Table: `STP_Credential_Record_Type`

**Layer:** Preprocessing table

**Source:** Derived from `STP_Credential`.

**Created by:** Credential preprocessing scripts / SQL queries in the `01` workflow.

**Used by:**

- filtering usable credential records
- credential analysis
- graduate projection inputs

**Key fields:**

- credential record identifier
- student identifier
- `RecordStatus`

**Refresh frequency:** Each model cycle after `STP_Credential` is loaded.

**Notes:** Credential record statuses include usable records and exclusion categories.

---

## Table: `MinEnrolmentSupVar`

**Layer:** Intermediate preprocessing table

**Source:** Derived from `STP_Enrolment` through sequential SQL queries.

**Created by:** Enrolment preprocessing SQL steps.

**Used by:**

- `MinEnrolment`
- age and gender cleaning
- supplemental enrolment variables

**Key fields:**

- student identifier
- enrolment identifier
- minimum enrolment fields
- supplemental program / credential variables

**Refresh frequency:** Each model cycle.

**Notes:** Supports construction of `MinEnrolment`.

---

## Table: `MinEnrolment`

**Layer:** Cleaned / derived STP table

**Source:** Built from `STP_Enrolment`, `STP_Enrolment_Record_Type`, and `MinEnrolmentSupVar`.

**Created by:** Enrolment preprocessing workflow.

**Used by:**

- enrolment analysis
- near-completer logic
- graduate projection inputs
- age and gender distributions

**Key fields:**

- `ENCRYPTED_TRUE_PEN`
- `PSI_CODE`
- `PSI_STUDENT_NUMBER`
- enrolment date fields
- age at enrolment
- age group
- gender
- credential category
- CIP fields
- record status

**Refresh frequency:** Each model cycle.

**Notes:** Core cleaned enrolment table.

---

## Table: `CredentialSupVarsFromEnrolment`

**Layer:** Intermediate preprocessing table

**Source:** Derived from enrolment records and linked to credential records.

**Created by:** Credential preprocessing workflow.

**Used by:**

- credential analysis
- filling or supplementing credential variables from enrolment data

**Key fields:**

- credential identifier
- student identifier
- enrolment-derived program fields
- enrolment-derived CIP fields
- credential-support variables

**Refresh frequency:** Each model cycle.

**Notes:** Used when credential records need supporting variables from enrolment history.

---

## 4.21.3 Student Outcomes and Cohort Tables

## Table: `T_APPSO_DATA_Final`

**Layer:** Cleaned Student Outcomes cohort table

**Source:** APPSO raw table provisioned from Student Outcomes.

**Created by:** `load-cohort-appso.R`

**Used by:**

- cohort construction
- labour supply calculations
- occupation distributions
- `T_Cohorts_Recoded`

**Key fields:**

- respondent / student identifier
- survey year
- age at survey
- `AGE_GROUP`
- `AGE_GROUP_LABEL`
- `CURRENT_REGION_PSSM_CODE`
- `NEW_LABOUR_SUPPLY`
- NOC fields
- CIP / credential fields
- weight fields

**Refresh frequency:** Each model cycle or Student Outcomes update.

**Notes:** Standardizes apprenticeship outcomes into PSSM variables.

---

## Table: `APPSO_Graduates`

**Layer:** Student Outcomes graduate count table

**Source:** APPSO graduate raw extract.

**Created by:** `load-cohort-appso.R`

**Used by:**

- cohort construction
- graduate count support
- age group distribution logic

**Key fields:**

- survey year
- credential type
- age / age group
- graduate counts

**Refresh frequency:** Each model cycle or Student Outcomes update.

**Notes:** Used alongside `T_APPSO_DATA_Final`.

---

## Table: `T_BGS_Data_Final`

**Layer:** Cleaned Student Outcomes cohort table

**Source:** BGS raw data from Student Outcomes.

**Created by:** `load-cohort-bgs.R`

**Used by:**

- BGS cohort construction
- program matching
- labour supply calculations
- `T_Cohorts_Recoded`

**Key fields:**

- person / survey identifier
- institution
- age
- survey year
- labour force fields
- employment fields
- further education fields
- region fields
- `CURRENT_REGION_PSSM_CODE`

**Refresh frequency:** Each model cycle or Student Outcomes update.

**Notes:** BGS fields are renamed and standardized to match common PSSM cohort vocabulary.

---

## Table: `T_DACSO_DATA_PART_1_STEPA`

**Layer:** Cleaned Student Outcomes cohort table

**Source:** DACSO raw extract from Student Outcomes.

**Created by:** `load-cohort-dacso.R`

**Used by:**

- DACSO cohort construction
- program matching
- labour supply calculations
- occupation distributions
- `T_Cohorts_Recoded`

**Key fields:**

- respondent / student identifier
- program fields
- CIP fields
- age fields
- labour force fields
- employment fields
- region fields
- `CURRENT_REGION_PSSM_CODE`

**Refresh frequency:** Each model cycle or Student Outcomes update.

**Notes:** SQL column typing and region recoding are part of the load process.

---

## Table: `infoware_c_outc_clean_short_resp`

**Layer:** Student Outcomes supporting table

**Source:** Student Outcomes / INFOWARE extract.

**Created by:** `load-cohort-dacso.R` or `load-outcomes-data.R`, depending on stage.

**Used by:**

- DACSO cohort workflow
- labour supply logic
- occupation distribution logic

**Key fields:**

- survey response fields
- training / labour fields
- credential fields
- respondent indicators

**Refresh frequency:** Each Student Outcomes refresh.

**Notes:** Contains short-response outcome data used in DACSO-related processing.

---

## Table: `T_TRD_DATA`

**Layer:** Cleaned Student Outcomes cohort table

**Source:** TRD raw extract from Student Outcomes.

**Created by:** `load-cohort-trd.R`

**Used by:**

- trades cohort construction
- labour supply calculations
- occupation distributions
- `T_Cohorts_Recoded`

**Key fields:**

- respondent / student identifier
- survey year
- age at survey
- `AGE_GROUP_LABEL`
- `CURRENT_REGION_PSSM_CODE`
- NOC fields
- CIP fields
- credential fields

**Refresh frequency:** Each model cycle or Student Outcomes update.

**Notes:** Standardizes trades-related outcomes.

---

## Table: `TRD_Graduates`

**Layer:** Student Outcomes graduate count table

**Source:** TRD graduate raw extract.

**Created by:** `load-cohort-trd.R`

**Used by:**

- trades graduate calculations
- cohort construction
- age group support

**Key fields:**

- survey year
- age at survey
- age group label
- graduate counts

**Refresh frequency:** Each model cycle or Student Outcomes update.

**Notes:** Used alongside `T_TRD_DATA`.

---

## Table: `T_Cohorts_Recoded`

**Layer:** Core model cohort table

**Source:** Built from standardized Student Outcomes cohort tables.

**Created by:** Cohort construction scripts, especially the `02b` workflow.

**Used by:**

- labour supply distributions
- occupation distributions
- CIP-to-NOC outputs
- NOC-to-CIP outputs
- downstream projection logic

**Key fields:**

- survey
- survey year
- person / respondent identifier
- `AGE_GROUP`
- region code
- credential
- CIP fields
- NOC fields
- labour supply flag
- weights
- previous credential / further education fields where available

**Refresh frequency:** Each model cycle.

**Notes:** This is one of the most important tables in the model.

```text
APPSO ----+
BGS ------+
DACSO ----+----> T_Cohorts_Recoded ----> Labour_Supply_Distribution
TRD ------+                         \
                                      +--> Occupation_Distributions
```

---

## 4.21.4 Program Matching Tables

## Table: `Credential_Non_Dup`

**Layer:** Core program matching table

**Source:** Derived from STP credential records after deduplication and program matching.

**Created by:** Program matching workflow, including `02a` scripts.

**Used by:**

- final CIP assignment
- program matching
- credential analysis
- graduate projections

**Key fields:**

- credential record identifier
- student identifier
- institution
- credential award year
- STP CIP fields
- matched CIP fields
- `FINAL_CIP_CODE_4`
- match status fields

**Refresh frequency:** Each model cycle.

**Notes:** Central table for program matching.

---

## Table: `Credential_Non_Dup_BGS_IDs`

**Layer:** Program matching support table

**Source:** BGS program matching workflow.

**Created by:** `02a` program matching scripts.

**Used by:** Updating `Credential_Non_Dup`.

**Key fields:**

- credential identifier
- BGS match identifier
- matched program / CIP fields

**Refresh frequency:** Each model cycle.

**Notes:** Transfers BGS program matching results into `Credential_Non_Dup`.

---

## Table: `Credential_Non_Dup_GRAD_IDs`

**Layer:** Program matching support table

**Source:** Graduate program matching workflow.

**Created by:** `02a` program matching scripts.

**Used by:** Updating `Credential_Non_Dup`.

**Key fields:**

- credential identifier
- graduate match identifier
- matched CIP fields

**Refresh frequency:** Each model cycle.

**Notes:** Updates `Credential_Non_Dup` with graduate matching results.

---

## Table: `Credential_Non_Dup_Programs_DACSO_FinalCIPs`

**Layer:** Program matching support table

**Source:** DACSO program matching workflow.

**Created by:** `02a` program matching scripts.

**Used by:** Updating `Credential_Non_Dup`.

**Key fields:**

- credential identifier
- DACSO program fields
- final CIP fields

**Refresh frequency:** Each model cycle.

**Notes:** Applies DACSO-derived final CIP assignments.

---

## Table: `Credential_Non_DIP_APPSO_IDs`

**Layer:** Program matching support table

**Source:** APPSO program matching workflow.

**Created by:** `02a` program matching scripts.

**Used by:** Updating `Credential_Non_Dup`.

**Key fields:**

- credential identifier
- APPSO match identifier
- matched CIP fields

**Refresh frequency:** Each model cycle.

**Notes:** Applies APPSO matching results to `Credential_Non_Dup`.

---

## 4.21.5 Census / Statistics Canada Tables

## Table: `Occupation_Distributions_Stat_Can`

**Layer:** Census-derived distribution table

**Source:** Imputed Census / Statistics Canada data.

**Created by:** `occ-dists-census-data.R`

**Used by:**

- occupation distribution process
- advanced credential gap filling
- final `Occupation_Distributions`

**Key fields:**

- survey label
- region code
- age group
- credential
- CIP field / cluster
- NOC code
- count
- percent

**Refresh frequency:** When new Census custom data is loaded and processed.

**Notes:** Built from region-level imputed Census outputs.

---

## Table: `Labour_Supply_Distribution_Stat_Can`

**Layer:** Census-derived distribution table

**Source:** Custom Census data, including filtered and unfiltered exports.

**Created by:** `labour-supply-dists-census-data.R`

**Used by:**

- labour supply distribution process
- advanced credential gap filling
- final `Labour_Supply_Distribution`

**Key fields:**

- survey label
- region code
- age group
- credential
- CIP field / cluster
- labour supply
- new labour supply rate
- total

**Refresh frequency:** When new Census custom data is loaded and processed.

**Notes:** `DOCT` uses special logic because of small counts and suppression.

---

## Table: Census imputation regional CSV outputs

**Layer:** Intermediate Census imputation outputs

**Source:** Custom Census table after NOC imputation by region.

**Created by:** `run-imputation-by-region.R` and `noc-imputation.R`

**Used by:**

- `occ-dists-census-data.R`
- Census occupation distribution construction

**Key fields:**

- region
- age group
- field of study
- NOC code
- credential counts
- imputed counts

**Refresh frequency:** When Census imputation is rerun.

**Notes:** Creates “new counts” files by region.

---

## 4.21.6 PTIB Tables

## Table: PTIB raw / staged table

**Layer:** Raw or staged source table

**Source:** PTIB data file containing private training institution enrolments and graduates.

**Created by:** PTIB load / cleaning workflow.

**Used by:**

- PTIB model run
- private institution adjustment
- final projections

**Key fields:**

- year
- institution operating name
- program title
- CIP code
- credential
- age group
- immigration status
- graduates
- enrolments
- total enrolments

**Refresh frequency:** Each model cycle or when PTIB data is updated.

**Notes:** Self-reported, not a unique headcount, and limited to regulated programs.

---

## 4.21.7 Lookup and Crosswalk Tables

## Table: `tbl_Age`

**Layer:** Lookup table

**Source:** LAN lookup CSV.

**Created by:** Loaded by multiple scripts.

**Used by:**

- age grouping
- cohort construction
- projection joins

**Key fields:** age, age group, age group label

**Refresh frequency:** Rarely.

**Notes:** Standardizes individual ages into PSSM age groups.

---

## Table: `tbl_Age_Groups`

**Layer:** Lookup table

**Source:** LAN lookup CSV.

**Created by:** Loaded by cohort and occupation projection scripts.

**Used by:**

- age group labelling
- cohort aggregation
- distribution tables

**Key fields:** age group code, age group label

**Refresh frequency:** Rarely.

**Notes:** Supports common age group definitions.

---

## Table: `tbl_Age_Groups_Rollup`

**Layer:** Lookup table

**Source:** LAN lookup CSV.

**Created by:** Loaded by cohort and Census distribution scripts.

**Used by:**

- rolled-up age groups
- Census occupation and labour supply distributions
- projection joins

**Key fields:** detailed age group, rolled-up age group

**Refresh frequency:** Rarely.

**Notes:** Used when detailed age groups must be collapsed.

---

## Table: `AgeGroupLookup`

**Layer:** Lookup table

**Source:** LAN lookup CSV.

**Created by:** Loaded by near-completer and projection scripts.

**Used by:**

- age assignment
- age validation
- age imputation support

**Key fields:** age index, age group, lower bound, upper bound

**Refresh frequency:** Rarely.

**Notes:** Maps raw ages to model age group indices.

---

## Table: `T_Current_Region_PSSM_Codes`

**Layer:** Region lookup table

**Source:** LAN lookup CSV.

**Created by:** Loaded by cohort and projection scripts.

**Used by:**

- region standardization
- cohort construction
- distribution tables

**Key fields:** region code, region name, PSSM region mapping fields

**Refresh frequency:** Rarely.

**Notes:** Standardizes region values across sources.

---

## Table: `T_Current_Region_PSSM_Rollup_Codes`

**Layer:** Region rollup lookup table

**Source:** LAN lookup CSV.

**Created by:** Loaded by cohort, Census, and occupation projection scripts.

**Used by:**

- region rollups
- Census distributions
- labour supply distributions
- occupation distributions

**Key fields:**

- `Current_Region_PSSM_Code_Rollup`
- `Current_Region_PSSM_Name_Rollup_StatCan`

**Refresh frequency:** Rarely.

**Notes:** Includes BC regions, Northeast, and Rest of Canada rollups.

---

## Table: `T_Current_Region_PSSM_Rollup_Codes_BC`

**Layer:** Region rollup lookup table

**Source:** LAN lookup CSV.

**Created by:** Loaded by cohort and projection scripts.

**Used by:** BC-only region rollups.

**Key fields:** BC region code, BC region rollup name

**Refresh frequency:** Rarely.

**Notes:** Used when Canada or Rest of Canada rollups are not needed.

---

## Table: `T_PSSM_Credential_Grouping`

**Layer:** Credential lookup table

**Source:** LAN lookup CSV.

**Created by:** Loaded by cohort and DACSO scripts.

**Used by:**

- credential recoding
- cohort construction
- labour supply and occupation distributions

**Key fields:** source credential, PSSM credential, PSSM credential grouping

**Refresh frequency:** Rarely.

**Notes:** Standardizes credential labels across sources.

---

## Table: `T_PSSM_Projection_Cred_Grp`

**Layer:** Projection credential lookup table

**Source:** LAN lookup CSV, sometimes modified in R during load.

**Created by:** `load-near-completers-ttrain.R`

**Used by:**

- near-completer workflow
- projection credential grouping
- graduate projections

**Key fields:** projection credential, PSSM credential, credential name, graduation status code

**Refresh frequency:** Rarely.

**Notes:** Supports credential grouping used by projection steps.

---

## Table: `T_Weights`

**Layer:** Weight lookup table

**Source:** LAN lookup CSV or carried forward from previous model cycle.

**Created by:** Loaded by `load-cohort-bgs.R` and related scripts.

**Used by:**

- BGS weighting
- cohort weighting
- regular and QI model variants

**Key fields:** group, weight, QI weight, survey grouping fields

**Refresh frequency:** Each model cycle or when weighting changes.

**Notes:** Weights differ between regular and QI runs.

---

## Table: `T_NOC_Broad_Categories`

**Layer:** NOC lookup table

**Source:** LAN lookup CSV.

**Created by:** Loaded by cohort / DACSO scripts.

**Used by:**

- NOC grouping
- occupation distributions
- CIP-to-NOC and NOC-to-CIP outputs

**Key fields:** broad category, major group, sub-major group, minor group, unit group, descriptions

**Refresh frequency:** When NOC standards or requirements change.

**Notes:** NOC code columns should be stored as character.

---

## 4.21.8 Projection and Distribution Tables

## Table: `Graduate_Projections`

**Layer:** Model output table

**Source:** Graduate projection workflow.

**Created by:** Projection scripts in the `04` workflow.

**Used by:**

- program projections
- labour supply projections
- occupation projections
- reporting

**Key fields:** projection year, region, age group, credential, CIP / program group, projected graduate count

**Refresh frequency:** Each model run.

**Notes:** Connects historical education data to projected supply.

---

## Table: `Cohort_Program_Distributions`

**Layer:** Model distribution table

**Source:** Cohort construction and program distribution workflow.

**Created by:** Projection / cohort scripts.

**Used by:**

- program projections
- labour supply projections
- occupation projections

**Key fields:** cohort year, credential, CIP/program grouping, age group, region, distribution percentage

**Refresh frequency:** Each model run.

**Notes:** Used to distribute projected graduates across program or CIP categories.

---

## Table: `Labour_Supply_Distribution`

**Layer:** Core model distribution table

**Source:** Derived from `T_Cohorts_Recoded`, Student Outcomes, and Census supplementation where required.

**Created by:** `02b` cohort labour supply scripts and later distribution workflows.

**Used by:**

- labour supply projections
- occupation projections
- CIP-to-NOC outputs
- NOC-to-CIP outputs

**Key fields:** survey, region, age group, credential, CIP/LCIP fields, labour supply count, labour supply percentage, weighted fields

**Refresh frequency:** Each model run.

**Notes:** Estimates the share of graduates in labour supply.

---

## Table: `Occupation_Distributions`

**Layer:** Core model distribution table

**Source:** Derived from `T_Cohorts_Recoded`, Student Outcomes, and Census supplementation where required.

**Created by:** `02b` occupation distribution scripts and later occupation projection workflows.

**Used by:**

- occupation projections
- CIP-to-NOC outputs
- NOC-to-CIP outputs
- reporting

**Key fields:** survey, region, age group, credential, CIP/LCIP fields, NOC code, occupation count, occupation percentage, weighted fields

**Refresh frequency:** Each model run.

**Notes:** Estimates the occupation distribution of post-secondary supply.

---

## Table: `population_projections`

**Layer:** Projection input table

**Source:** Population projection CSV file.

**Created by:** `load-graduate-projections.R`

**Used by:**

- graduate projections
- enrolment forecasting
- projection steps downstream

**Key fields:** projection year, age, region, population count

**Refresh frequency:** Each model cycle or when new population projections are available.

**Notes:** Provides the demographic projection base.

---

## 4.21.9 Final Analytical Output Tables

## Table: `CIP_to_NOC`

**Layer:** Final analytical output table

**Source:** Derived from labour supply and occupation distribution tables.

**Created by:** CIP-to-NOC workflow.

**Used by:**

- AEST reporting
- occupational outcome products
- Excel graphs and tables

**Key fields:** CIP code, CIP name, credential, NOC code, NOC name, count, percentage, suppression fields

**Refresh frequency:** When CIP-to-NOC outputs are produced.

**Notes:** Shows occupations associated with a field of study.

---

## Table: `NOC_to_CIP`

**Layer:** Final analytical output table

**Source:** Derived from labour supply and occupation distribution tables.

**Created by:** NOC-to-CIP workflow.

**Used by:**

- AEST reporting
- occupational outcome products
- Excel graphs and tables

**Key fields:** NOC code, NOC name, CIP code, CIP name, credential, count, percentage, suppression fields

**Refresh frequency:** When NOC-to-CIP outputs are produced.

**Notes:** Shows fields of study associated with an occupation.

---

## Table: `CIP_Totals`

**Layer:** Supporting output table

**Source:** Derived from `CIP_to_NOC` workflow.

**Created by:** CIP-to-NOC workflow.

**Used by:**

- denominators for CIP-to-NOC percentages
- Excel output tables
- validation

**Key fields:** CIP code, credential, total count

**Refresh frequency:** When CIP-to-NOC outputs are produced.

**Notes:** Stores total counts used for CIP-to-NOC percentages.

---

## Table: `NOC_Totals`

**Layer:** Supporting output table

**Source:** Derived from `NOC_to_CIP` workflow.

**Created by:** NOC-to-CIP workflow.

**Used by:**

- denominators for NOC-to-CIP percentages
- Excel output tables
- validation

**Key fields:** NOC code, credential, total count

**Refresh frequency:** When NOC-to-CIP outputs are produced.

**Notes:** Stores total counts used for NOC-to-CIP percentages.

---

## 4.21.10 Table relationship summary

```text
RAW EDUCATION DATA
------------------
STP_Enrolment
STP_Credential
      |
      v
STP_Enrolment_Record_Type
STP_Credential_Record_Type
MinEnrolment
Credential_Non_Dup
      |
      v
Graduate_Projections


RAW STUDENT OUTCOMES DATA
-------------------------
APPSO_*_raw
BGS_*_raw
DACSO_*_raw
TRD_*_raw
      |
      v
T_APPSO_DATA_Final
T_BGS_Data_Final
T_DACSO_DATA_PART_1_STEPA
T_TRD_DATA
      |
      v
T_Cohorts_Recoded
      |
      +----------------------------+
      |                            |
      v                            v
Labour_Supply_Distribution   Occupation_Distributions
      |                            |
      +-------------+--------------+
                    |
                    v
              CIP_to_NOC
              NOC_to_CIP


CENSUS / STATCAN DATA
---------------------
STAT_CAN
   |
   v
NOC imputation outputs
   |
   +----------------------------+
   |                            |
   v                            v
Occupation_Distributions_Stat_Can
Labour_Supply_Distribution_Stat_Can
   |                            |
   +-------------+--------------+
                 v
       Final distribution tables


PTIB DATA
---------
PTIB raw/staged data
      |
      v
PTIB run adjustments
      |
      v
Graduate / program / occupation projections
```

---

## 4.21.11 Practical rule for future documentation

For every new or modified table, add a short inventory block with:

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

This will make future onboarding and troubleshooting easier because the pipeline depends heavily on exact table names, schemas, and refresh order.


---



# Post-Secondary Supply Model (PSSM) Documentation

## Sections 5–8 Draft

**Document status:** Working draft  
**Prepared for:** PSSM documentation reorganization  
**Scope:** Sections 5–8 only  

---

# 5. Pipeline Overview

## 5.1 Purpose of this section

This section explains how the PSSM pipeline runs from start to finish.

It maps:

- the main orchestrator: `run_all_three_model_runs.r`
- the three prep scripts:
  - `prep-for-fresh-run.R`
  - `prep-for-qi-run.R`
  - `prep-for-ptib-run.R`
- the numbered workflow scripts: `01a–08`
- the load scripts that prepare source tables
- the dependency order between stages
- the major tables created or updated at each stage

The key point: **the PSSM is not a collection of standalone scripts. It is a sequenced pipeline where each step depends on tables and settings created earlier.**

---

## 5.2 High-level pipeline structure

At the highest level, the pipeline has three layers:

```text
+--------------------------------------------------+
| 1. Orchestration Layer                           |
| run_all_three_model_runs.r                       |
+-------------------------+------------------------+
                          |
                          v
+--------------------------------------------------+
| 2. Run Preparation Layer                         |
| prep-for-fresh-run.R                             |
| prep-for-qi-run.R                                |
| prep-for-ptib-run.R                              |
+-------------------------+------------------------+
                          |
                          v
+--------------------------------------------------+
| 3. Numbered Pipeline Layer                       |
| 01a–08                                           |
| preprocessing → matching → cohorts → projections |
| → occupation outputs → reporting                 |
+--------------------------------------------------+
```

The orchestrator coordinates three model runs — regular, QI, and PTIB — by sourcing the relevant prep scripts and then running the required numbered scripts in sequence.

---

## 5.3 Main orchestrator

## Script: `run_all_three_model_runs.r`

**Role:**  
Top-level controller for the full PSSM workflow.

**What it does:**

- Runs the regular model workflow
- Runs the QI workflow
- Runs the PTIB workflow
- Sources prep scripts in the correct order
- Ensures the numbered scripts are executed as a controlled sequence

```text
run_all_three_model_runs.r
        |
        +--> prep-for-fresh-run.R
        |
        +--> prep-for-qi-run.R
        |
        +--> prep-for-ptib-run.R
        |
        v
  numbered scripts 01a–08
```

**Important note:**  
The numbered scripts are designed to be sourced through the orchestrator or prep scripts. They should not generally be run independently because they depend on run flags, existing tables, and previous-stage outputs.

---

## 5.4 The three model runs

The full pipeline includes three sequential model run types.

```text
+------------------+
| Regular run      |
| full model run   |
+--------+---------+
         |
         v
+------------------+
| QI run           |
| quality variant  |
+--------+---------+
         |
         v
+------------------+
| PTIB run         |
| private training |
| adjustment       |
+------------------+
```

---

### 5.4.1 Regular run

**Prep script:**  
`prep-for-fresh-run.R`

**Purpose:**  
Runs the full standard model pipeline.

**Typical scope:**

- source data loading
- STP preprocessing
- program matching
- cohort construction
- near-completers
- graduate projections
- program projections
- occupation projections
- reporting outputs

**Notes:**  
The regular run sets up many of the base tables later used by the QI and PTIB runs.

---

### 5.4.2 QI run

**Prep script:**  
`prep-for-qi-run.R`

**Purpose:**  
Runs the quality indicator variant of the model.

**Typical scope:**

- uses selected regular-run outputs
- applies QI-specific weighting or filtering logic
- updates selected labour supply and occupation distribution outputs

**Notes:**  
The QI run does not simply rerun everything from scratch. It depends on parts of the regular run and may skip some scripts.

---

### 5.4.3 PTIB run

**Prep script:**  
`prep-for-ptib-run.R`

**Purpose:**  
Adds private training institution estimates into the model.

**Typical scope:**

- loads PTIB data
- cleans and maps private training credentials
- integrates private institution rows into projection outputs
- updates final projection and occupation outputs where required

**Notes:**  
The PTIB run is a specialized extension of the regular pipeline. It uses PTIB-specific logic and private institution adjustment tables.

---

## 5.5 Prep script responsibilities

The prep scripts prepare the R session and database state before the numbered scripts run.

Common prep script tasks include:

- setting run flags
- cleaning or resetting the environment
- loading required R packages
- sourcing utility functions
- checking or copying required tables
- setting up database and LAN connections
- preparing logging

```text
prep script
   |
   +--> set run flags
   |
   +--> load libraries
   |
   +--> load utility functions
   |
   +--> check required inputs
   |
   +--> call numbered scripts
```

---

## 5.6 Load scripts

Load scripts prepare raw and lookup data before the main numbered workflow uses it.

```text
+-----------------------------+
| Source files / extracts     |
+-------------+---------------+
              |
              v
+-----------------------------+
| load-*.R scripts            |
+-------------+---------------+
              |
              v
+-----------------------------+
| SQL Server source tables    |
+-----------------------------+
```

Major load scripts include:

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

---

## 5.7 End-to-end numbered workflow

The numbered workflow runs from source preprocessing to final outputs.

```text
01a–01d
  |
  v
02a
  |
  v
02b
  |
  v
03
  |
  v
04
  |
  v
05
  |
  v
06
  |
  v
07
  |
  v
08
```

---

## 5.8 Pipeline stages by number

## Stage 01a–01d — STP preprocessing

**Purpose:**  
Clean and prepare enrolment and credential data.

**Main tasks:**

- load or use `STP_Enrolment`
- load or use `STP_Credential`
- assign record status
- clean birthdates and gender
- assign age groups
- deduplicate credential records
- rank credentials where needed

**Major outputs:**

- `STP_Enrolment_Record_Type`
- `STP_Enrolment_Valid`
- `STP_Credential_Record_Type`
- `CredentialSupVars`
- `Credential_Non_Dup`
- `MinEnrolment`
- `AgeGroupLookup`

```text
STP_Enrolment      STP_Credential
      |                  |
      v                  v
record status       record status
      |                  |
      +--------+---------+
               v
       cleaned STP tables
               |
               v
       MinEnrolment / Credential_Non_Dup
```

---

## Stage 02a — Program matching

**Purpose:**  
Assign and clean final program / CIP classifications.

**Main tasks:**

- match credentials to Student Outcomes program data
- clean CIP fields
- match APPSO and BGS records
- support manual review of unmatched programs
- update final CIP fields in `Credential_Non_Dup`

**Major outputs:**

- `Credential_Non_Dup_APPSO_IDs`
- `Credential_Non_Dup_BGS_IDs`
- `T_BGS_Data_Final_for_OutcomesMatching`
- updated `Credential_Non_Dup`

```text
Credential_Non_Dup
        ^
        |
+-------+------------------------------+
|                                      |
APPSO matching                   BGS matching
|                                      |
v                                      v
Credential_Non_Dup_APPSO_IDs    Credential_Non_Dup_BGS_IDs
        \                              /
         \                            /
          +----> update final CIP ----+
```

---

## Stage 02b — Cohort construction, labour supply, and occupation distributions

**Purpose:**  
Build standardized cohort tables from Student Outcomes data and derive labour supply and occupation distributions.

**Main tasks:**

- integrate TRD, APPSO, DACSO, and BGS data
- standardize age groups
- standardize region codes
- apply survey weights
- create labour supply flags
- build occupation distributions
- build labour supply distributions

**Major outputs:**

- `T_Cohorts_Recoded`
- `Labour_Supply_Distribution`
- `Occupation_Distributions`
- `tbl_Age_Groups`
- `T_PSSM_Credential_Grouping`

```text
TRD --------+
APPSO ------+
DACSO ------+----> T_Cohorts_Recoded
BGS --------+              |
                          +-----------------------------+
                          |                             |
                          v                             v
              Labour_Supply_Distribution     Occupation_Distributions
```

---

## Stage 03 — Near-completers

**Purpose:**  
Estimate near-completers and related transition ratios for projection use.

**Main tasks:**

- calculate near-completer ratios
- aggregate by age, gender, year, and CIP
- prepare near-completer inputs for projections

**Major outputs:**

- `NearCompleters_CIP4`
- `T_DACSO_Near_Completers_RatioAgeAtGradCIP4`
- `T_DACSO_Near_Completers_RatioByGender`

```text
cleaned enrolment / cohort data
          |
          v
near-completer calculations
          |
          v
near-completer ratio tables
          |
          v
projection inputs
```

---

## Stage 04 — Graduate projections

**Purpose:**  
Forecast future graduates by credential, age, gender, field, and year.

**Main tasks:**

- use historical credential and enrolment data
- apply population projections
- calculate projected graduate counts
- include historical and projected views where needed

**Major outputs:**

- `Graduate_Projections`
- `Graduate_Projections_Include_Historical`

```text
historical credentials
        |
population projections
        |
near-completer ratios
        |
        v
Graduate_Projections
```

---

## Stage 05 — PTIB analysis

**Purpose:**  
Clean, map, and integrate private training institution data.

**Main tasks:**

- load PTIB data
- clean private institution credentials
- map credentials into model categories
- split domestic and international students where needed
- aggregate PTIB data for projection use

**Major outputs:**

- `T_Private_Institutions_Credentials_Raw`
- `T_PTIB_Y1_to_Y10`
- `qry_Private_Credentials_05i1_Grads_by_Year`

```text
PTIB raw data
     |
     v
credential cleaning / mapping
     |
     v
PTIB projection tables
     |
     v
private institution adjustment
```

---

## Stage 06 — Program projections

**Purpose:**  
Create static and projected cohort/program distribution tables.

**Main tasks:**

- combine main cohorts
- incorporate apprenticeships
- incorporate near-completers
- incorporate PTIB where relevant
- build static and projected program distribution tables

**Major outputs:**

- `Cohort_Program_Distributions_Projected`
- `Cohort_Program_Distributions_Static`

```text
Graduate_Projections
        |
T_Cohorts_Recoded
        |
Near-completers
        |
PTIB adjustment
        |
        v
Cohort_Program_Distributions
```

---

## Stage 07 — Occupation projections

**Purpose:**  
Convert program and labour supply projections into occupation-level projections.

**Main tasks:**

- apply labour supply distributions
- apply occupation distributions
- roll up NOC categories
- apply exclusion and suppression rules
- aggregate by credential, age, region, and year

**Major outputs:**

- `Q_4_NOC_1D_Totals_by_PSSM_CRED`
- `tmp_tbl_Model`
- `tmp_tbl_QI`
- `tmp_tbl_Model_Inc_Private_Inst`

```text
Graduate / program projections
          |
          v
Labour_Supply_Distribution
          |
          v
Occupation_Distributions
          |
          v
occupation projection tables
          |
          v
suppression / exclusions / rollups
```

---

## Stage 08 — Reporting

**Purpose:**  
Generate final reporting outputs.

**Main tasks:**

- create Excel outputs
- apply suppression
- apply exclusions
- create internal and public versions
- prepare final tables for downstream use

**Major outputs:**

- internal Excel workbooks
- public Excel workbooks
- final reporting extracts
- suppression and exclusion outputs

```text
final projection tables
        |
        v
suppression / exclusion rules
        |
        v
internal outputs
        |
        v
public outputs
```

---

# 5.9 Dependency order

The dependency order is strict.

```text
Source data must be loaded first
        |
        v
STP preprocessing must finish
        |
        v
program matching can update Credential_Non_Dup
        |
        v
cohort tables can be built
        |
        v
near-completer and graduate projections can run
        |
        v
program projections can run
        |
        v
occupation projections can run
        |
        v
reporting can run
```

A simplified dependency map:

```text
+------------------+
| Load scripts     |
+--------+---------+
         |
         v
+------------------+
| 01a–01d          |
| STP prep         |
+--------+---------+
         |
         v
+------------------+
| 02a              |
| Program matching |
+--------+---------+
         |
         v
+------------------+
| 02b              |
| Cohorts          |
+--------+---------+
         |
         v
+------------------+
| 03               |
| Near-completers  |
+--------+---------+
         |
         v
+------------------+
| 04               |
| Graduate proj.   |
+--------+---------+
         |
         v
+------------------+
| 05               |
| PTIB             |
+--------+---------+
         |
         v
+------------------+
| 06               |
| Program proj.    |
+--------+---------+
         |
         v
+------------------+
| 07               |
| Occupation proj. |
+--------+---------+
         |
         v
+------------------+
| 08               |
| Reporting        |
+------------------+
```

---

# 5.10 Table inventory by pipeline stage

## Stage: Source data loading

**Primary scripts:**

- `load-stp-enrol.R`
- `load-stp-cred.R`
- `load-outcomes-data.R`
- `load-custom-stats-can.R`
- `load-ptib.R`

**Creates or updates:**

- `STP_Enrolment`
- `STP_Credential`
- Student Outcomes `*_raw` tables
- `STAT_CAN`
- PTIB raw/staged tables

**Used by:**

- `01a–01d`
- `02b`
- Census workflows
- PTIB workflow

---

## Stage: STP preprocessing — `01a–01d`

**Creates or updates:**

- `STP_Enrolment_Record_Type`
- `STP_Enrolment_Valid`
- `STP_Credential_Record_Type`
- `CredentialSupVars`
- `Credential_Non_Dup`
- `MinEnrolment`
- `AgeGroupLookup`

**Used by:**

- program matching
- near-completers
- graduate projections

---

## Stage: Program matching — `02a`

**Creates or updates:**

- `Credential_Non_Dup_APPSO_IDs`
- `Credential_Non_Dup_BGS_IDs`
- `T_BGS_Data_Final_for_OutcomesMatching`
- updated `Credential_Non_Dup`

**Used by:**

- credential analysis
- graduate projections
- program distributions

---

## Stage: Cohorts and distributions — `02b`

**Creates or updates:**

- `T_Cohorts_Recoded`
- `Labour_Supply_Distribution`
- `Occupation_Distributions`
- `tbl_Age_Groups`
- `T_PSSM_Credential_Grouping`

**Used by:**

- program projections
- occupation projections
- CIP-to-NOC outputs
- NOC-to-CIP outputs

---

## Stage: Near-completers — `03`

**Creates or updates:**

- `NearCompleters_CIP4`
- `T_DACSO_Near_Completers_RatioAgeAtGradCIP4`
- `T_DACSO_Near_Completers_RatioByGender`

**Used by:**

- graduate projections
- program projections

---

## Stage: Graduate projections — `04`

**Creates or updates:**

- `Graduate_Projections`
- `Graduate_Projections_Include_Historical`

**Used by:**

- PTIB integration
- program projections
- occupation projections
- reporting

---

## Stage: PTIB — `05`

**Creates or updates:**

- `T_Private_Institutions_Credentials_Raw`
- `T_PTIB_Y1_to_Y10`
- `qry_Private_Credentials_05i1_Grads_by_Year`

**Used by:**

- PTIB run
- program projections
- occupation projections

---

## Stage: Program projections — `06`

**Creates or updates:**

- `Cohort_Program_Distributions_Projected`
- `Cohort_Program_Distributions_Static`

**Used by:**

- occupation projections
- reporting

---

## Stage: Occupation projections — `07`

**Creates or updates:**

- `Q_4_NOC_1D_Totals_by_PSSM_CRED`
- `tmp_tbl_Model`
- `tmp_tbl_QI`
- `tmp_tbl_Model_Inc_Private_Inst`

**Used by:**

- reporting
- final public/internal outputs

---

## Stage: Reporting — `08`

**Creates or updates:**

- internal Excel workbooks
- public Excel workbooks
- suppression/exclusion outputs
- final reporting extracts

**Used by:**

- analysts
- reporting products
- downstream publication or dashboard work

---

# 5.11 Table lifecycle

Tables generally follow this lifecycle:

```text
created
   |
   v
populated
   |
   v
updated / joined / aggregated
   |
   v
used by downstream stage
   |
   +--------------------+
   |                    |
   v                    v
retained            dropped
if final/needed     if temporary
```

---

# 5.12 Run-specific table behaviour

## Regular run

```text
regular_run = TRUE
qi_run      = FALSE
ptib_run    = FALSE
```

**Typical behaviour:**

- full data pipeline runs
- base model outputs are created
- regular weights and tables are used
- outputs support QI and PTIB follow-up runs

---

## QI run

```text
regular_run = FALSE
qi_run      = TRUE
ptib_run    = FALSE
```

**Typical behaviour:**

- QI-specific logic is applied
- some scripts may be skipped
- selected tables are updated for QI outputs
- QI tables may depend on regular-run tables

---

## PTIB run

```text
regular_run = FALSE
qi_run      = FALSE
ptib_run    = TRUE
```

**Typical behaviour:**

- PTIB-specific data is included
- private institution rows are integrated
- model outputs include private training adjustments

---

# 5.13 Logging and error handling

The pipeline uses logging to track script execution.

The utility wrapper `time_execution()` logs script start, completion, elapsed time, and errors. Execution logs are written to a file such as:

```text
./R/execution_log.txt
```

```text
time_execution(script)
        |
        +--> log start time
        |
        +--> source script
        |
        +--> log completion time
        |
        +--> if error:
                log error message
                log traceback
```

---

# 5.14 Practical run checklist

Before running the pipeline:

- Confirm SQL Server database and schema
- Confirm LAN paths are accessible
- Confirm required source files are available
- Confirm Student Outcomes extracts have been loaded
- Confirm lookup tables are current
- Confirm run flags are correct
- Confirm previous intermediate tables are safe to overwrite
- Confirm logging is enabled
- Confirm PC sleep settings will not interrupt long uploads

After running each major stage:

- Check row counts
- Check key tables were created
- Check record status counts
- Check missing/null key fields
- Check age, region, credential, CIP, and NOC distributions
- Check logs for warnings or errors

---

# 5.15 Section 5 summary

The PSSM pipeline is best understood as a strict sequence:

```text
orchestrator
   ↓
prep scripts
   ↓
load scripts
   ↓
01a–01d preprocessing
   ↓
02a program matching
   ↓
02b cohort and distributions
   ↓
03 near-completers
   ↓
04 graduate projections
   ↓
05 PTIB
   ↓
06 program projections
   ↓
07 occupation projections
   ↓
08 reporting
```

The main documentation rule for this section is:

> Always describe each pipeline stage by its inputs, scripts, outputs, dependencies, and validation checks.

---

# 6. Data Loading and Preprocessing — 01a–01d

## 6.1 Purpose of this section

This section documents the first major processing block in the PSSM pipeline: **STP data loading and preprocessing**.

The `01a–01d` workflow prepares the two core STP source tables:

- `STP_Enrolment`
- `STP_Credential`

These steps produce the cleaned, validated, and deduplicated tables used by later stages, especially:

- `MinEnrolment`
- `Credential_Non_Dup`
- `STP_Enrolment_Record_Type`
- `STP_Credential_Record_Type`

The main goal is to turn large raw STP files into model-ready tables with usable identifiers, cleaned age and gender fields, valid credential records, and clear exclusion flags.

---

## 6.2 Where 01a–01d fits in the pipeline

```text
Raw STP files
     |
     v
load-stp-enrol.R
load-stp-cred.R
     |
     v
STP_Enrolment
STP_Credential
     |
     v
01a–01d preprocessing
     |
     +--> record status tables
     |
     +--> age / gender cleaning
     |
     +--> MinEnrolment
     |
     +--> Credential_Non_Dup
     |
     v
Downstream stages:
02a program matching
02b cohorts
03 near-completers
04 graduate projections
```

---

# 6.3 STP enrolment loading

## 6.3.1 Source

The STP enrolment file is loaded into SQL Server as:

```text
STP_Enrolment
```

This table contains detailed enrolment records, including student identifiers, institution, school year, program, credential category, CIP code, age-related fields, gender, and other student/program attributes.

---

## 6.3.2 Load script

Primary script:

```text
load-stp-enrol.R
```

The loader reads LAN-based STP enrolment files and writes them to SQL Server using explicit schemas where possible. Identifier and code fields are preserved as character fields to avoid losing leading zeros or special codes.

---

## 6.3.3 Main output table

## Table: `STP_Enrolment`

**Layer:**  
Raw source table

**Created by:**  
`load-stp-enrol.R`

**Used by:**

- enrolment preprocessing
- record status assignment
- `MinEnrolment`
- age and gender cleaning
- near-completer analysis
- graduate projections

**Key fields:**

- `ENCRYPTED_TRUE_PEN`
- `PSI_PEN`
- `TRUE_PEN`
- `PSI_CODE`
- `PSI_STUDENT_NUMBER`
- `PSI_SCHOOL_YEAR`
- `PSI_PROGRAM_CODE`
- `PSI_CIP_CODE`
- `PSI_CREDENTIAL_CATEGORY`
- `PSI_BIRTHDATE`
- `PSI_GENDER`

**Refresh frequency:**  
Each model cycle.

**Notes:**  
Treat student identifiers, institution codes, school years, program codes, and CIP fields as character values.

---

## 6.3.4 Enrolment loading flow

```text
LAN STP enrolment files
        |
        v
load-stp-enrol.R
        |
        v
schema-safe read
        |
        v
write to SQL Server
        |
        v
STP_Enrolment
        |
        v
01 enrolment preprocessing
```

---

# 6.4 STP credential loading

## 6.4.1 Source

The STP credential file is loaded into SQL Server as:

```text
STP_Credential
```

This table contains credential award records, including award date, institution, student identifiers, program codes, credential category, credential level, and credential CIP.

---

## 6.4.2 Load script

Primary script:

```text
load-stp-cred.R
```

The credential loader reads the STP credential file from LAN storage and writes it to SQL Server. Date fields may require normalization, and non-standard characters may need to be removed before upload or during preprocessing.

---

## 6.4.3 Main output table

## Table: `STP_Credential`

**Layer:**  
Raw source table

**Created by:**  
`load-stp-cred.R`

**Used by:**

- credential preprocessing
- credential record status assignment
- credential deduplication
- `Credential_Non_Dup`
- program matching
- graduate projections

**Key fields:**

- `ENCRYPTED_TRUE_PEN`
- `STP_ALT_ID`
- `PSI_CODE`
- `PSI_STUDENT_NUMBER`
- `PSI_SCHOOL_YEAR`
- `CREDENTIAL_AWARD_DATE`
- `PSI_PROGRAM_CODE`
- `PSI_CREDENTIAL_CIP`
- `PSI_CREDENTIAL_CATEGORY`
- `PSI_CREDENTIAL_LEVEL`

**Refresh frequency:**  
Each model cycle.

**Notes:**  
Credential dates and CIP fields need careful validation before downstream use.

---

## 6.4.4 Credential loading flow

```text
LAN STP credential file
        |
        v
load-stp-cred.R
        |
        v
schema-safe read
        |
        v
write to SQL Server
        |
        v
STP_Credential
        |
        v
01 credential preprocessing
```

---

# 6.5 Record status logic

## 6.5.1 Purpose

Record status logic separates records that can be used in the model from records that must be excluded.

This logic is applied separately to enrolment and credential data. The outputs are record-type tables such as:

```text
STP_Enrolment_Record_Type
STP_Credential_Record_Type
```

Only records with a usable status, typically `RecordStatus = 0`, flow into downstream analysis.

---

## 6.5.2 Record status codes

The documentation identifies the following record status codes across enrolment and credential preprocessing:

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

The exact codes used depend on whether the table is enrolment or credential data, but the principle is the same: **flag records early and exclude non-model records consistently.**

---

## 6.5.3 Enrolment exclusion examples

For enrolment preprocessing, exclusions include:

- developmental records
- skills-based records
- no PSI transition records
- developmental CIP records
- outside-BC records, if applicable
- missing identifier records, if applicable

---

## 6.5.4 Credential exclusion examples

For credential preprocessing, exclusions include:

- developmental credentials
- skills-based credentials
- developmental CIP records
- recommendation-for-certification records
- dropped credential categories
- missing identifier records, if applicable

---

## 6.5.5 Record status flow

```text
Raw table
   |
   v
Apply exclusion rules
   |
   v
Assign RecordStatus
   |
   +--> RecordStatus = 0
   |        |
   |        v
   |   keep for model
   |
   +--> RecordStatus != 0
            |
            v
       exclude or retain for audit
```

---

# 6.6 Age cleaning

## 6.6.1 Purpose

Age cleaning creates consistent age variables for enrolment and credential analysis.

Age is needed for:

- enrolment analysis
- credential analysis
- cohort distributions
- near-completers
- graduate projections
- labour supply and occupation distributions

Age is generally calculated from cleaned birthdate fields and relevant dates such as credential award date or enrolment date. Age groups are then assigned using lookup tables such as `AgeGroupLookup` or related age group tables.

---

## 6.6.2 Main age outputs

Typical outputs include:

- age at enrolment
- age at graduation or credential award
- age group
- age group label
- flags for invalid or missing ages

---

## 6.6.3 Age cleaning flow

```text
Birthdate fields
        |
        v
Clean / standardize dates
        |
        v
Calculate age at event
        |
        +--> enrolment date
        |
        +--> credential award date
        |
        v
Join to age lookup
        |
        v
Assign age group
        |
        v
Flag missing / invalid ages
```

---

## 6.6.4 Age imputation and manual fixes

Historical workflows included manual checks and fixes for invalid or out-of-bounds ages. The current workflow shifts more of this logic into R and SQL, including imputation methods and scripted assignment. Any remaining manual interventions should be clearly documented.

---

# 6.7 Gender cleaning

## 6.7.1 Purpose

Gender cleaning creates a consistent gender value for each student or record where possible.

Gender is used in:

- enrolment summaries
- credential summaries
- near-completer ratios
- graduate projections
- validation and distribution checks

The preprocessing workflow addresses missing gender, conflicting gender values across records, and imputation of unknown values where required.

---

## 6.7.2 Multiple gender values

Some students have more than one gender value across records. Workflows resolve these using rules such as:

- using credential file gender where prioritized
- using most recent credential information for credential records
- using first enrolment information for enrolment records
- using distribution-based imputation for missing values

---

## 6.7.3 Missing gender

Missing or unknown gender records are identified and updated where possible. Missing gender can be imputed using distributions by credential category or related groupings, with row counts tracked before and after updates.

---

## 6.7.4 Gender cleaning flow

```text
Raw gender values
        |
        v
Identify missing / unknown values
        |
        v
Identify students with conflicting gender values
        |
        v
Apply resolution rule
        |
        +--> credential-based value
        |
        +--> first enrolment value
        |
        +--> distribution-based imputation
        |
        v
Update cleaned table
        |
        v
Validate counts
```

---

# 6.8 Credential deduplication

## 6.8.1 Purpose

Credential deduplication removes duplicate credential records before analysis.

The final output of this process is:

```text
Credential_Non_Dup
```

This table is used for credential analysis, program matching, and graduate projections.

---

## 6.8.2 Deduplication logic

Duplicates are removed based on a set of key credential fields. The exact fields may vary by workflow version, but the deduplication goal is to avoid counting the same credential more than once for the same student/program/award combination.

---

## 6.8.3 Deduplication flow

```text
STP_Credential
      |
      v
Apply record status rules
      |
      v
Keep usable credential records
      |
      v
Identify duplicate credential records
      |
      v
Select retained record
      |
      v
Credential_Non_Dup
```

---

## 6.8.4 Why deduplication matters

Without deduplication:

- graduate counts may be overstated
- credential projections may be biased
- program matching may create duplicate matches
- downstream labour supply and occupation estimates may be inflated

`Credential_Non_Dup` is therefore a critical control point before program matching and graduate projections.

---

# 6.9 `MinEnrolment`

## 6.9.1 Purpose

`MinEnrolment` is the cleaned enrolment table used to identify students’ earliest or minimum enrolment records for model analysis.

It supports:

- enrolment summaries
- first enrolment logic
- age and gender distributions
- near-completer calculations
- graduate projection inputs

---

## 6.9.2 Main input tables

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

---

## 6.9.3 Table inventory

## Table: `MinEnrolment`

**Layer:**  
Cleaned / derived STP table

**Source:**  
Built from `STP_Enrolment`, `STP_Enrolment_Record_Type`, and `MinEnrolmentSupVar`.

**Created by:**  
Enrolment preprocessing workflow in `01a–01d`.

**Used by:**

- enrolment analysis
- near-completer workflow
- age and gender distributions
- projection inputs

**Key fields:**

- student identifier
- institution
- school year
- enrolment date
- age at enrolment
- age group
- gender
- program / credential fields
- CIP fields
- record status

**Refresh frequency:**  
Each model cycle.

**Notes:**  
Row-count and logic validation are important because differences in minimum-enrolment logic can affect downstream projections.

---

# 6.10 `Credential_Non_Dup`

## 6.10.1 Purpose

`Credential_Non_Dup` is the cleaned, deduplicated credential table used by downstream credential analysis and program matching.

It is one of the most important outputs from `01a–01d`.

---

## 6.10.2 Table inventory

## Table: `Credential_Non_Dup`

**Layer:**  
Cleaned / deduplicated STP credential table

**Source:**  
Derived from `STP_Credential` after record status filtering, cleaning, and deduplication.

**Created by:**  
Credential preprocessing workflow in `01a–01d`.

**Used by:**

- credential analysis
- program matching
- CIP assignment
- graduate projections
- downstream projection logic

**Key fields:**

- credential record identifier
- student identifier
- institution
- credential award date
- school year
- credential category
- credential level
- STP CIP fields
- cleaned age
- cleaned gender
- visa status
- final or matched CIP fields added later

**Refresh frequency:**  
Each model cycle.

**Notes:**  
This is the final deduplicated credential table for analysis.

---

## 6.10.3 `Credential_Non_Dup` flow

```text
STP_Credential
      |
      v
STP_Credential_Record_Type
      |
      v
valid credential records
      |
      v
age / gender cleaning
      |
      v
deduplication
      |
      v
Credential_Non_Dup
      |
      v
02a program matching
```

---

# 6.11 Row count checks

## 6.11.1 Purpose

Row count checks are mandatory because the preprocessing workflow is table-heavy and exclusion-heavy.

Counts should be checked:

- after loading raw tables
- after record status assignment
- after exclusions
- after age cleaning
- after gender cleaning
- after deduplication
- after creation of `MinEnrolment`
- after creation of `Credential_Non_Dup`

---

## 6.11.2 Suggested row count checkpoint table

```text
+--------------------------------+------------------------------+
| Checkpoint                     | What to verify               |
+--------------------------------+------------------------------+
| Raw STP_Enrolment              | total rows loaded            |
| Raw STP_Credential             | total rows loaded            |
| Enrolment record status        | counts by RecordStatus       |
| Credential record status       | counts by RecordStatus       |
| Valid enrolment records        | RecordStatus = 0 count       |
| Valid credential records       | RecordStatus = 0 count       |
| Age cleaning                   | missing / invalid age count  |
| Gender cleaning                | missing / imputed count      |
| MinEnrolment                   | final row count              |
| Credential_Non_Dup             | final deduplicated row count |
+--------------------------------+------------------------------+
```

---

## 6.11.3 Example count categories to track

Track at minimum:

```text
Raw loaded rows
Excluded developmental records
Excluded skills-based records
Excluded developmental CIP records
Excluded missing identifier records
Valid records retained
Records with missing age
Records with missing gender
Records imputed
Duplicate records removed
Final output rows
```

---

# 6.12 Validation checks

## 6.12.1 Required validation checks

Validation should happen after every major preprocessing step.

Recommended checks:

```text
1. Table exists
2. Row count matches expected load count
3. Key identifiers are not unexpectedly missing
4. Code fields retain leading zeros
5. RecordStatus values are valid
6. Age values fall within expected range
7. Age groups are assigned where expected
8. Gender values are valid or imputed
9. Duplicate credential records are removed
10. MinEnrolment and Credential_Non_Dup row counts are reasonable
```

---

## 6.12.2 Identifier validation

Check that student identifiers are usable.

Important identifier fields include:

- `ENCRYPTED_TRUE_PEN`
- `PSI_CODE`
- `PSI_STUDENT_NUMBER`
- `STP_ALT_ID`

Missing values such as `NULL`, empty strings, blanks, and `'(Unspecified)'` should be treated as missing in identifier logic.

---

## 6.12.3 Age validation

Check:

- age at enrolment
- age at credential award
- missing ages
- age group assignment
- age values outside valid ranges

Records outside expected ranges should be reviewed, excluded, or imputed according to the documented workflow.

---

## 6.12.4 Gender validation

Check:

- missing gender
- unknown gender
- students with multiple gender values
- imputed gender counts
- final gender distribution before and after imputation

---

## 6.12.5 Credential validation

Check:

- duplicate credential records
- invalid credential categories
- developmental credentials
- skills-based credentials
- valid CIP formatting
- credential award date logic
- final deduplicated counts

Credential validation is important because `Credential_Non_Dup` feeds program matching and graduate projections.

---

# 6.13 Known risks and legacy manual logic

## 6.13.1 Manual checks not fully carried forward

Earlier workflows included manual checks in Access, Excel, and SQL. Some of these checks were not fully incorporated into the newer R/SQL workflow, especially around skills-based courses, edge-case exclusions, age fixes, gender fixes, and program-level judgement calls.

---

## 6.13.2 Differences from older workflows

Testing against older model cycles showed some differences between old and new workflows. These differences are generally expected to have minimal final projection impact, but they should still be documented because they can affect row counts and comparisons with previous runs.

---

## 6.13.3 Skills-based and developmental records

Skills-based and developmental exclusions are a major source of differences between old and new workflows. Earlier analysts sometimes made manual decisions about which courses to include or exclude; the newer workflow relies more heavily on scripted rules.

---

## 6.13.4 Age and gender imputation

Age and gender imputation introduce controlled uncertainty. The workflow tracks missing values and applies rule-based or distribution-based imputation, but results may differ from historical manual corrections.

---

## 6.13.5 Credential deduplication risk

Deduplication rules can change final counts if the grouping keys or record-retention rules differ from earlier workflows. Because `Credential_Non_Dup` is used downstream, any change to deduplication logic should be documented and tested against prior model cycles.

---

# 6.14 Recommended preprocessing checklist

Before running `01a–01d`:

- Confirm `STP_Enrolment` loaded successfully.
- Confirm `STP_Credential` loaded successfully.
- Confirm raw row counts match source files.
- Confirm code fields were loaded as character.
- Confirm lookup tables are available.
- Confirm database and schema are correct.
- Confirm no unintended overwrite of production tables.

After running `01a–01d`:

- Check record status counts for enrolment.
- Check record status counts for credential.
- Check valid record counts.
- Check missing and invalid age counts.
- Check missing and imputed gender counts.
- Check duplicate credential counts removed.
- Check final `MinEnrolment` row count.
- Check final `Credential_Non_Dup` row count.
- Compare current counts to prior model cycle benchmarks.
- Review logs for warnings, errors, or unexpected nulls.

---

# 6.15 Section 6 summary

The `01a–01d` workflow turns raw STP enrolment and credential files into model-ready tables.

```text
STP_Enrolment
STP_Credential
      |
      v
record status logic
      |
      v
age and gender cleaning
      |
      v
credential deduplication
      |
      +-------------------+
      |                   |
      v                   v
MinEnrolment       Credential_Non_Dup
      |                   |
      v                   v
near-completers    program matching
graduate inputs    graduate projections
```

The most important outputs are:

- `STP_Enrolment_Record_Type`
- `STP_Credential_Record_Type`
- `MinEnrolment`
- `Credential_Non_Dup`

The key documentation rule for this section is:

> Every preprocessing step should document its input table, output table, exclusion logic, row count impact, and validation check.

---

# 7. Cohort Construction and Program Matching — 02a–02b

## 7.1 Purpose of this section

This section documents the two linked workflows that connect **cleaned STP credential data** with **Student Outcomes cohort data**:

- **`02a` — Program matching**
- **`02b` — Cohort construction, labour supply, and occupation distributions**

Together, these steps turn cleaned education records and survey records into model-ready cohort tables, labour supply distributions, and occupation distributions. The key outputs are `Credential_Non_Dup`, `T_Cohorts_Recoded`, `Labour_Supply_Distribution`, and `Occupation_Distributions`.

---

## 7.2 Where 02a–02b fits in the pipeline

```text
01a–01d preprocessing
        |
        v
Credential_Non_Dup
MinEnrolment
        |
        v
+-------------------------------+
| 02a Program Matching          |
| final CIP assignment          |
+---------------+---------------+
                |
                v
+-------------------------------+
| 02b Cohort Construction       |
| APPSO / BGS / DACSO / TRD     |
+---------------+---------------+
                |
                v
T_Cohorts_Recoded
        |
        +------------------------------+
        |                              |
        v                              v
Labour_Supply_Distribution     Occupation_Distributions
        |
        v
Downstream model runs and projections
```

---

# 7.3 02a — Program Matching

## 7.3.1 Purpose

The purpose of `02a` is to assign the best available **final CIP classification** to credential records. This is needed because STP, DACSO, BGS, APPSO, and other sources may carry different program or CIP information for the same student or credential.

The main table updated by this workflow is:

```text
Credential_Non_Dup
```

---

## 7.3.2 Main inputs

```text
Credential_Non_Dup
        |
        +--> STP credential CIP fields
        +--> student identifiers
        +--> institution
        +--> award year
        +--> credential category
        |
        v
Program matching workflow
```

Primary inputs include:

- `Credential_Non_Dup`
- BGS program matching inputs
- DACSO program matching inputs
- APPSO program matching inputs
- GRAD / credential matching inputs
- INFOWARE CIP lookup tables
- institution and program crosswalks
- manual review tables for unresolved matches

---

## 7.3.3 Main outputs

```text
+----------------------------------------------+
| Program Matching Output Tables               |
+----------------------------------------------+
| Credential_Non_Dup_BGS_IDs                   |
| Credential_Non_Dup_GRAD_IDs                  |
| Credential_Non_Dup_Programs_DACSO_FinalCIPs  |
| Credential_Non_DIP_APPSO_IDs                 |
| updated Credential_Non_Dup                   |
+----------------------------------------------+
```

These outputs are merged back into `Credential_Non_Dup` to populate or improve final CIP fields, including `FINAL_CIP_CODE_4` and related program classification fields.

---

## 7.3.4 Program matching logic

Program matching generally uses combinations of:

- student identifier
- institution
- credential award year
- program code
- CIP code
- CIP2 or CIP4 rollups
- source-specific program identifiers
- Student Outcomes program tables

The BGS matching process includes case-level matching between BGS and STP using fields such as PEN, institution, award year, and CIP, with manual review for ambiguous or unmatched records.

DACSO program matching uses historical and updated program linkages, including crosswalk logic for changed program codes and institution-specific issues. APPSO program matching contributes matched IDs and CIP information back into `Credential_Non_Dup` through APPSO-specific matching outputs.

---

## 7.3.5 Program matching flow

```text
Credential_Non_Dup
        |
        +----------------------+
        |                      |
        v                      v
BGS matching             DACSO matching
        |                      |
        v                      v
BGS matched IDs          DACSO final CIPs
        |                      |
        +----------+-----------+
                   |
                   v
           update Credential_Non_Dup
                   |
                   v
          fill FINAL_CIP_CODE_4
                   |
                   v
          convert leftover nulls
                   |
                   v
        model-ready credential table
```

After source-specific matching, remaining null `FINAL_CIP_CODE_4` values may be filled from STP CIP fields, and some cluster columns may require additional updates for GRAD or APPSO records.

---

## 7.3.6 Key scripts

The program matching workflow includes scripts such as:

```text
02a-appso-programs.R
02a-update-cred-non-dup.R
02a-convert-leftover-nulls.R
```

These scripts update matching tables, merge matched records into `Credential_Non_Dup`, and convert remaining unmatched or null CIP values where possible.

---

## 7.3.7 Table inventory — `Credential_Non_Dup`

## Table: `Credential_Non_Dup`

**Layer:**  
Core program matching table

**Source:**  
Derived from `STP_Credential` after preprocessing, filtering, cleaning, and deduplication.

**Created by:**  
`01a–01d` preprocessing workflow.

**Updated by:**  
`02a` program matching scripts.

**Used by:**

- program matching
- final CIP assignment
- graduate projections
- program projections
- downstream model tables

**Key fields:**

- credential record identifier
- student identifier
- institution
- credential award year
- STP CIP fields
- matched CIP fields
- `FINAL_CIP_CODE_4`
- credential category
- credential rank or grouping fields

**Refresh frequency:**  
Each model cycle.

**Notes:**  
This is the central table where matched CIP information is consolidated. Any change to matching rules can affect graduate projections and program distributions.

---

## 7.3.8 Program matching validation checks

Recommended validation checks:

```text
1. Count Credential_Non_Dup records before matching.
2. Count records matched by BGS.
3. Count records matched by DACSO.
4. Count records matched by APPSO.
5. Count records matched by GRAD workflow.
6. Count records with FINAL_CIP_CODE_4 still null.
7. Count records where matched CIP differs from STP CIP.
8. Review unmatched records by institution and credential.
9. Review large CIP2 or CIP4 shifts.
10. Confirm leftover null conversion logic was applied.
```

---

# 7.4 02b — Cohort Construction

## 7.4.1 Purpose

The purpose of `02b` is to build standardized Student Outcomes cohorts from APPSO, BGS, DACSO, and TRD survey data. These cohorts are combined into `T_Cohorts_Recoded`, which is then used to create labour supply and occupation distributions.

---

## 7.4.2 Main cohort sources

The four main cohort sources are:

```text
+--------+---------------------------------------------+
| Source | Description                                 |
+--------+---------------------------------------------+
| APPSO  | apprenticeship outcomes                      |
| BGS    | bachelor graduate outcomes                   |
| DACSO  | diploma, associate certificate, and related  |
| TRD    | trades-related outcomes                      |
+--------+---------------------------------------------+
```

These sources are loaded and standardized through cohort load scripts such as `load-cohort-appso.R`, `load-cohort-bgs.R`, `load-cohort-dacso.R`, and `load-cohort-trd.R`.

---

## 7.4.3 Cohort load scripts

```text
load-cohort-appso.R
        |
        v
T_APPSO_DATA_Final
APPSO_Graduates

load-cohort-bgs.R
        |
        v
T_BGS_Data_Final
T_Weights

load-cohort-dacso.R
        |
        v
T_DACSO_DATA_PART_1_STEPA
infoware_c_outc_clean_short_resp
lookup tables

load-cohort-trd.R
        |
        v
T_TRD_DATA
TRD_Graduates
```

The cohort load scripts standardize source-specific survey data by recoding fields, assigning age groups, assigning region codes, preparing graduate count tables, and writing cleaned tables back to SQL Server.

---

## 7.4.4 Standardized cohort fields

The cohort workflow standardizes source-specific survey fields into common PSSM variables.

Key standardized variables include:

- survey source
- survey year
- respondent or student identifier
- credential
- CIP fields
- NOC fields
- age group
- region code
- labour supply flag
- weights
- respondent status
- further education or previous credential fields where available

```text
APPSO fields
BGS fields
DACSO fields
TRD fields
      |
      v
standardized PSSM variables
      |
      v
T_Cohorts_Recoded
```

---

## 7.4.5 Age standardization

The cohort construction workflow standardizes age groups across survey sources. Common age groups include ranges such as `17–19`, `20–24`, `25–29`, `30–34`, and `35–64`, depending on the workflow and downstream model use.

Age lookup tables used in cohort processing include:

- `tbl_Age`
- `tbl_Age_Groups`
- `tbl_Age_Groups_Rollup`
- `AgeGroupLookup`

---

## 7.4.6 Region standardization

Survey-specific region fields are recoded into PSSM region fields such as `CURRENT_REGION_PSSM_CODE`. Region rollup tables support BC region groupings, broader rollups, and special regions such as Northeast or Rest of Canada where required.

Key region lookup tables include:

- `T_Current_Region_PSSM_Codes`
- `T_Current_Region_PSSM_Rollup_Codes`
- `T_Current_Region_PSSM_Rollup_Codes_BC`

---

## 7.4.7 Labour supply flag

A central output of cohort construction is the labour supply indicator, often represented as:

```text
NEW_LABOUR_SUPPLY
```

This variable identifies whether a respondent is treated as part of labour supply for model purposes. It is later used to build `Labour_Supply_Distribution`.

```text
employment / labour force survey fields
        |
        v
source-specific logic
        |
        v
NEW_LABOUR_SUPPLY
        |
        v
Labour_Supply_Distribution
```

---

## 7.4.8 Weights

The cohort workflow applies survey and year weights, including different weighting logic for regular and QI runs. Weight tables such as `T_Weights` are loaded and used in cohort construction and distribution calculations.

Validation should check for extreme weights, missing weights, and unexpected changes in weighted totals.

---

# 7.5 `T_Cohorts_Recoded`

## 7.5.1 Purpose

`T_Cohorts_Recoded` is the central unified cohort table. It combines APPSO, BGS, DACSO, and TRD into a common structure for labour supply and occupation distribution analysis.

```text
T_APPSO_DATA_Final
T_BGS_Data_Final
T_DACSO_DATA_PART_1_STEPA
T_TRD_DATA
        |
        v
T_Cohorts_Recoded
        |
        +------------------------------+
        |                              |
        v                              v
Labour_Supply_Distribution     Occupation_Distributions
```

---

## 7.5.2 Table inventory — `T_Cohorts_Recoded`

## Table: `T_Cohorts_Recoded`

**Layer:**  
Core cohort table

**Source:**  
Standardized APPSO, BGS, DACSO, and TRD Student Outcomes data.

**Created by:**  
`02b` cohort construction workflow.

**Used by:**

- labour supply distribution construction
- occupation distribution construction
- CIP-to-NOC outputs
- NOC-to-CIP outputs
- downstream model projections

**Key fields:**

- survey
- survey year
- respondent identifier
- credential
- CIP fields
- NOC fields
- age group
- region code
- `NEW_LABOUR_SUPPLY`
- weights
- respondent flags
- previous credential / further education fields where available

**Refresh frequency:**  
Each model cycle.

**Notes:**  
This table is one of the most important model inputs. Most downstream labour and occupation outputs depend on it.

---

# 7.6 Labour supply distribution

## 7.6.1 Purpose

`Labour_Supply_Distribution` estimates the share or count of graduates in labour supply by combinations such as credential, CIP, age group, region, and survey source. It is derived primarily from `T_Cohorts_Recoded` and may be supplemented by Census-derived tables where survey data is weak or suppressed.

---

## 7.6.2 Flow

```text
T_Cohorts_Recoded
        |
        v
filter / classify labour supply
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

---

## 7.6.3 Table inventory — `Labour_Supply_Distribution`

## Table: `Labour_Supply_Distribution`

**Layer:**  
Core model distribution table

**Source:**  
Derived from `T_Cohorts_Recoded`, Student Outcomes data, and Census supplementation where needed.

**Created by:**  
`02b` cohort labour supply scripts and downstream distribution logic.

**Used by:**

- labour supply projections
- occupation projections
- CIP-to-NOC outputs
- NOC-to-CIP outputs

**Key fields:**

- survey
- region
- age group
- credential
- CIP / LCIP fields
- labour supply count
- labour supply rate or percentage
- weight fields

**Refresh frequency:**  
Each model run.

**Notes:**  
Some versions include cleaned fields such as `LCIP4_CRED_Cleaned`, which removes source-specific prefixes from DACSO credential/CIP combinations.

---

# 7.7 Occupation distributions

## 7.7.1 Purpose

`Occupation_Distributions` estimates the occupation distribution of graduates by credential, CIP, age group, region, and NOC. It is derived from `T_Cohorts_Recoded`, using labour supply and occupation fields from the Student Outcomes sources.

---

## 7.7.2 Flow

```text
T_Cohorts_Recoded
        |
        v
filter to labour supply / occupation-valid records
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

---

## 7.7.3 Handling missing combinations

Some labour supply combinations may be missing from occupation distributions. These missing combinations can be identified and appended with zeroes so that labour and occupation distribution tables remain structurally complete.

```text
Labour_Supply_Distribution combinations
        |
        v
compare against Occupation_Distributions
        |
        +--> matched combinations
        |
        +--> missing combinations
                  |
                  v
             append with zero counts
```

---

## 7.7.4 Table inventory — `Occupation_Distributions`

## Table: `Occupation_Distributions`

**Layer:**  
Core model distribution table

**Source:**  
Derived from `T_Cohorts_Recoded`, Student Outcomes occupation fields, and Census supplementation where needed.

**Created by:**  
`02b` occupation distribution scripts and downstream occupation logic.

**Used by:**

- occupation projections
- CIP-to-NOC outputs
- NOC-to-CIP outputs
- final reporting

**Key fields:**

- survey
- region
- age group
- credential
- CIP / LCIP fields
- NOC code
- occupation count
- occupation percentage
- weight fields

**Refresh frequency:**  
Each model run.

**Notes:**  
Occupation distributions are later used for occupation projections and CIP-to-NOC / NOC-to-CIP products.

---

# 7.8 Key lookup tables used in 02a–02b

The `02a–02b` workflows depend on lookup and crosswalk tables for age, region, credential, weights, CIP, and NOC coding.

```text
+----------------------------------------+
| Key Lookup Tables                      |
+----------------------------------------+
| T_Weights                              |
| T_BGS_INST_Recode                      |
| tbl_Age                                |
| tbl_Age_Groups                         |
| tbl_Age_Groups_Rollup                  |
| T_PSSM_Credential_Grouping             |
| T_year_survey_Year                     |
| T_Current_Region_PSSM_Codes            |
| T_Current_Region_PSSM_Rollup_Codes     |
| T_Current_Region_PSSM_Rollup_Codes_BC  |
| T_NOC_Broad_Categories                 |
| INFOWARE_L_CIP_2DIGITS_CIP2016         |
| INFOWARE_L_CIP_4DIGITS_CIP2016         |
| INFOWARE_L_CIP_6DIGITS_CIP2016         |
+----------------------------------------+
```

These tables support category standardization, survey-year mapping, institution recoding, CIP validation, NOC grouping, and region rollups.

---

# 7.9 Validation checks

## 7.9.1 Program matching validation

Recommended checks:

```text
1. Count Credential_Non_Dup before and after program matching.
2. Count records matched by source: BGS, DACSO, APPSO, GRAD.
3. Count records still missing FINAL_CIP_CODE_4.
4. Count records where matched CIP differs from STP CIP.
5. Review large CIP2 or CIP4 changes.
6. Review unmatched records by institution.
7. Confirm fallback to STP CIP was applied only where intended.
8. Check that final CIP fields are valid against CIP lookup tables.
```

---

## 7.9.2 Cohort construction validation

Recommended checks:

```text
1. Confirm all four source cohorts loaded: APPSO, BGS, DACSO, TRD.
2. Confirm age groups are assigned.
3. Confirm region codes are assigned.
4. Confirm credential groupings are valid.
5. Confirm NEW_LABOUR_SUPPLY is populated.
6. Confirm NOC values are valid or explicitly unknown.
7. Confirm weights are populated.
8. Check weighted and unweighted counts.
9. Check extreme weights.
10. Compare cohort counts to previous model cycle.
```

---

## 7.9.3 Distribution validation

Recommended checks:

```text
1. Labour supply distributions sum as expected.
2. Occupation distributions sum as expected.
3. Missing labour/occupation combinations are identified.
4. Missing combinations are appended with zeroes where required.
5. Percentages use correct denominators.
6. Region exclusions are applied consistently.
7. Suppression rules are applied after percentage calculations where required.
8. Census-supplemented rows are clearly identifiable.
```

---

# 7.10 Known risks and legacy manual logic

## 7.10.1 Manual matching risk

Some program matching logic historically required manual review, especially for unmatched programs, institution-specific code issues, and ambiguous CIP assignments. These manual decisions may not be fully replicated in the automated R workflow.

---

## 7.10.2 CIP mismatch risk

CIP values can differ across STP, BGS, DACSO, and other sources. A change at the CIP4 or CIP2 level can affect program distributions and graduate projections, so large changes should be reviewed.

---

## 7.10.3 Weighting risk

Extreme or missing weights can distort labour supply and occupation distributions. Weight checks should be part of standard validation before using `Labour_Supply_Distribution` or `Occupation_Distributions`.

---

## 7.10.4 Missing NOC or CIP risk

Missing or invalid CIP and NOC values can create gaps in distribution tables. The workflow should identify missing combinations and decide whether to exclude, proxy, append with zero, or supplement using Census-derived data.

---

## 7.10.5 Legacy Access/Excel logic

Some earlier cohort and CIP-to-NOC workflows used Access queries, Excel workbooks, and manual macros. These historical steps should be documented as legacy unless still required, and any remaining manual logic should be clearly marked.

---

# 7.11 Section 7 summary

The `02a–02b` workflow is where cleaned education records become model-ready cohort and distribution tables.

```text
02a Program Matching
    |
    v
Credential_Non_Dup with final CIP fields
    |
    v
02b Cohort Construction
    |
    v
T_Cohorts_Recoded
    |
    +--> Labour_Supply_Distribution
    |
    +--> Occupation_Distributions
```

The most important outputs are:

- `Credential_Non_Dup`
- `Credential_Non_Dup_BGS_IDs`
- `Credential_Non_Dup_GRAD_IDs`
- `Credential_Non_Dup_Programs_DACSO_FinalCIPs`
- `Credential_Non_DIP_APPSO_IDs`
- `T_APPSO_DATA_Final`
- `T_BGS_Data_Final`
- `T_DACSO_DATA_PART_1_STEPA`
- `T_TRD_DATA`
- `T_Cohorts_Recoded`
- `Labour_Supply_Distribution`
- `Occupation_Distributions`

The key documentation rule for this section is:

> For every cohort or matching step, document the source table, matching keys, output table, row count impact, unmatched records, and validation checks.

---

# 8. Model Runs and Projections — 02b–07

## 8.1 Purpose of this section

This section documents how the model moves from **cohort/distribution tables** into the main projection outputs. It covers the major workflows from `02b–07`, including near-completers, graduate projections, PTIB integration, program projections, occupation projections, and the differences between the **regular**, **QI**, and **PTIB** runs.

At this point in the pipeline, the model already has cleaned STP tables, matched CIP fields, standardized Student Outcomes cohorts, and labour/occupation distributions. The `02b–07` workflows use those inputs to generate projected supply by credential, field of study, region, age group, and occupation.

---

## 8.2 Where 02b–07 fits in the pipeline

```text
01a–01d
STP loading and preprocessing
        |
        v
02a
program matching
        |
        v
02b
cohorts, labour supply, occupation distributions
        |
        v
03
near-completers
        |
        v
04
graduate projections
        |
        v
05
PTIB integration
        |
        v
06
program projections
        |
        v
07
occupation projections
        |
        v
08
reporting
```

The `02b–07` block is where the model shifts from **historical observed data** to **projected future supply**, using a mix of STP records, Student Outcomes distributions, population projections, PTIB data, and occupation mappings.

---

# 8.3 Run types: regular, QI, and PTIB

The model is organized around three run types:

```text
+------------------+
| Regular run      |
| base model       |
+--------+---------+
         |
         v
+------------------+
| QI run           |
| quality variant  |
+--------+---------+
         |
         v
+------------------+
| PTIB run         |
| private training |
| adjustment       |
+------------------+
```

The combined pipeline is controlled by flags for the regular, QI, and PTIB runs, and these flags affect which scripts run, which tables are updated, and which projection variants are produced.

---

## 8.3.1 Regular run

The **regular run** is the base model run. It produces the core graduate, program, labour supply, and occupation projection tables. It uses the full standard workflow and creates outputs that may later be used by the QI and PTIB runs.

```text
regular_run = TRUE
qi_run      = FALSE
ptib_run    = FALSE
```

Typical regular-run outputs include:

- `Graduate_Projections`
- `Graduate_Projections_Include_Historical`
- `Cohort_Program_Distributions_Static`
- `Cohort_Program_Distributions_Projected`
- `Labour_Supply_Distribution`
- `Occupation_Distributions`
- occupation projection tables used for reporting

---

## 8.3.2 QI run

The **QI run** is a quality-indicator variant of the regular model. It is focused mainly on the stability and quality of labour supply and occupation distribution outputs, rather than rebuilding every upstream table from scratch.

```text
regular_run = FALSE
qi_run      = TRUE
ptib_run    = FALSE
```

The QI workflow may link to or reuse regular-run tables, then calculate quality or coverage indicators used to assess whether output cells are stable enough for release.

---

## 8.3.3 PTIB run

The **PTIB run** adds private training institution data into the model. It uses PTIB-specific cleaning, credential mapping, age grouping, immigration status, and projection logic before integrating private training estimates into program and occupation outputs.

```text
regular_run = FALSE
qi_run      = FALSE
ptib_run    = TRUE
```

PTIB data is treated as an adjustment layer because it is self-reported, not a unique headcount, and has different coverage than STP data.

---

# 8.4 Stage 02b — Cohorts, labour supply, and occupation distributions

Although `02b` was introduced in Section 7, it also belongs in Section 8 because its outputs become the core inputs for the projection stages. It produces `T_Cohorts_Recoded`, `Labour_Supply_Distribution`, and `Occupation_Distributions`, which are reused in later model steps.

```text
T_Cohorts_Recoded
        |
        +------------------------------+
        |                              |
        v                              v
Labour_Supply_Distribution     Occupation_Distributions
        |                              |
        +--------------+---------------+
                       |
                       v
             program and occupation projections
```

The labour supply distribution estimates how many graduates are expected to be in labour supply, while occupation distributions estimate where those graduates are likely to work by NOC.

---

# 8.5 Stage 03 — Near-completers

## 8.5.1 Purpose

The near-completers workflow estimates students who nearly completed a program but did not receive a credential in the expected way. These estimates are used to adjust projections and avoid undercounting supply pathways that are visible in Student Outcomes but not always cleanly represented in credential data.

The workflow removes near-completers who later or earlier received credentials, using STP credential history, and calculates ratios by age, gender, CIP, and year.

---

## 8.5.2 Near-completer flow

```text
Student Outcomes near-completer records
        |
        v
match against STP credentials
        |
        +--> remove those who later/earlier received credentials
        |
        v
calculate near-completer ratios
        |
        +--> by gender
        +--> by age group
        +--> by CIP4
        +--> by year
        |
        v
near-completer ratio tables
        |
        v
graduate and program projection inputs
```

---

## 8.5.3 Main outputs

Key near-completer outputs include:

- `NearCompleters_CIP4`
- `T_DACSO_Near_Completers_RatioAgeAtGradCIP4`
- `T_DACSO_Near_Completers_RatioByGender`

---

## 8.5.4 Table inventory — `T_DACSO_Near_Completers_RatioAgeAtGradCIP4`

**Layer:**  
Near-completer projection input table.

**Source:**  
Derived from DACSO near-completer records, STP credential matching, age group assignment, and CIP4 groupings.

**Created by:**  
Stage `03` near-completer workflow.

**Used by:**

- graduate projections
- program projections
- near-completer adjustment logic

**Key fields:**

- year
- age group
- CIP4
- near-completer count
- completer count
- ratio fields

**Refresh frequency:**  
Each model cycle.

**Notes:**  
Validation should check whether near-completers who later or earlier received credentials were removed correctly.

---

# 8.6 Stage 04 — Graduate projections

## 8.6.1 Purpose

Graduate projections estimate future graduates by credential, age group, gender, field of study, and year. These projections are one of the core model outputs and feed into later program and occupation projection steps.

The workflow uses historical credential data, cleaned enrolment data, population projections, graduation rates, credential distributions, and near-completer adjustments.

---

## 8.6.2 Graduate projection flow

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
        |
        v
Graduate_Projections_Include_Historical
```

---

## 8.6.3 Main outputs

Key outputs include:

- `Graduate_Projections`
- `Graduate_Projections_Include_Historical`

Graduate projections are filtered or summarized by age, credential, region, year, and CIP, and future outputs may be rounded or prepared for internal/public reporting in later stages.

---

## 8.6.4 Table inventory — `Graduate_Projections`

**Layer:**  
Core projection table.

**Source:**  
Historical credential data, enrolment patterns, population projections, graduation rates, and near-completer adjustments.

**Created by:**  
Stage `04` graduate projection workflow.

**Used by:**

- program projections
- PTIB integration
- occupation projections
- reporting outputs

**Key fields:**

- projection year
- credential
- age group
- gender
- region
- CIP / program grouping
- projected graduate count

**Refresh frequency:**  
Each regular model run and updated where PTIB rows are added.

**Notes:**  
Graduate projections are a major control table; downstream outputs should not be built until this table passes row-count and distribution validation.

---

# 8.7 Stage 05 — PTIB integration

## 8.7.1 Purpose

PTIB integration adds private training institution data to the model. This step is needed because STP does not fully cover private training institutions. PTIB data contributes additional graduates and enrolments for private programs, using its own credential, age, CIP, and immigration fields.

The PTIB workflow cleans and aggregates PTIB records, validates CIP codes, removes excluded program types, maps credentials, counts domestic and international graduates, and prepares private-institution rows for the projection tables.

---

## 8.7.2 PTIB cleaning and projection flow

```text
PTIB raw file
     |
     v
clean credential and CIP fields
     |
     +--> exclude ESL / not-for-credit / unclassified records
     |
     v
recode age groups and immigration status
     |
     v
aggregate graduates and enrolments
     |
     v
calculate domestic / international shares
     |
     v
PTIB projection tables
     |
     v
add PTIB rows to graduate / program / occupation projections
```

---

## 8.7.3 Main outputs

PTIB-related outputs include:

- `T_Private_Institutions_Credentials_Raw`
- `T_PTIB_Y1_to_Y10`
- `qry_Private_Credentials_05i1_Grads_by_Year`
- PTIB rows in graduate projection and program distribution tables

---

## 8.7.4 Table inventory — `T_Private_Institutions_Credentials_Raw`

**Layer:**  
PTIB source/staging table.

**Source:**  
PTIB private training institution data, typically received as Excel or CSV data.

**Created by:**  
Stage `05` PTIB workflow or `load-ptib.R`.

**Used by:**

- PTIB credential cleaning
- PTIB graduate projections
- PTIB program projections
- private institution adjustment logic

**Key fields:**

- year
- institution
- program title
- CIP code
- credential
- age group
- immigration status
- graduates
- enrolments
- total enrolments

**Refresh frequency:**  
Each model cycle or whenever updated PTIB data is provided.

**Notes:**  
PTIB data is self-reported, not a unique headcount, and has coverage limitations; validation and exclusion logic are especially important.

---

# 8.8 Stage 06 — Program projections

## 8.8.1 Purpose

Program projections distribute projected graduates across program or CIP groupings. This stage combines graduate projections, cohort/program distributions, near-completer adjustments, apprenticeship logic, and PTIB rows where relevant.

The workflow creates both **static** and **projected** cohort/program distributions. Static tables are used where a fixed historical distribution is appropriate, while projected tables support future-year changes in program mix.

---

## 8.8.2 Program projection flow

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

---

## 8.8.3 Main outputs

Program projection outputs include:

- `Cohort_Program_Distributions_Static`
- `Cohort_Program_Distributions_Projected`
- related projected/static tables for apprenticeships, near-completers, PTIB, and credential/CIP combinations

---

## 8.8.4 Table inventory — `Cohort_Program_Distributions_Projected`

**Layer:**  
Projected program distribution table.

**Source:**  
Graduate projections, STP historical distributions, cohort distributions, near-completer logic, and PTIB adjustments.

**Created by:**  
Stage `06` program projection workflow.

**Used by:**

- occupation projections
- labour supply projections
- reporting outputs

**Key fields:**

- projection year
- credential
- age group
- CIP2 / CIP4
- region
- projected distribution percentage
- projected count fields where applicable

**Refresh frequency:**  
Each model run.

**Notes:**  
Validation should check that projected distributions sum correctly within each credential, age, region, and year grouping.

---

# 8.9 Stage 07 — Occupation projections

## 8.9.1 Purpose

Occupation projections convert graduate/program projections into projected labour supply by occupation. This stage applies labour supply distributions and occupation distributions to graduate and program projection outputs.

The occupation projection workflow handles NOC rollups, missing `TTRAIN` values, private institution proxies, 2-digit and 4-digit CIP proxies, occupation distribution proxies, and suppression/exclusion logic used later for reporting.

---

## 8.9.2 Occupation projection flow

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
        |
        v
NOC rollups, exclusions, suppression prep
```

---

## 8.9.3 TTRAIN and proxy logic

Some programs or private institution records may not have usable `TTRAIN` values. The workflow uses “No_TT” tables and proxy logic to support these cases. It may aggregate from CIP4 to CIP2 or substitute comparable credential/program distributions when detailed values are missing.

```text
Program with TTRAIN
        |
        v
use detailed distribution

Program without TTRAIN
        |
        v
use No_TT / proxy distribution
        |
        v
if CIP4 unavailable, aggregate or proxy at CIP2
```

---

## 8.9.4 Main outputs

Occupation projection outputs include:

- `Q_4_NOC_1D_Totals_by_PSSM_CRED`
- `tmp_tbl_Model`
- `tmp_tbl_QI`
- `tmp_tbl_Model_Inc_Private_Inst`
- final occupation projection tables used by reporting scripts

---

## 8.9.5 Table inventory — `tmp_tbl_Model`

**Layer:**  
Intermediate occupation projection table.

**Source:**  
Graduate projections, program distributions, labour supply distributions, and occupation distributions.

**Created by:**  
Stage `07` occupation projection workflow.

**Used by:**

- occupation projection aggregation
- QI comparison
- PTIB-inclusive model comparison
- reporting preparation

**Key fields:**

- projection year
- credential
- region
- age group
- CIP
- NOC
- projected counts
- labour supply fields
- occupation distribution fields

**Refresh frequency:**  
Each model run.

**Notes:**  
This is an intermediate table and may be dropped or replaced during processing; retain row-count checks before cleanup.

---

# 8.10 How the run types differ by stage

```text
+----------------------+----------------------+----------------------+----------------------+
| Stage                | Regular run          | QI run               | PTIB run             |
+----------------------+----------------------+----------------------+----------------------+
| 02b distributions    | builds base tables   | quality variant      | may reuse base logic |
| 03 near-completers   | included             | usually not focus    | may reuse outputs    |
| 04 grad projections  | core projections     | linked/reused        | PTIB rows added      |
| 05 PTIB              | not core focus       | excluded/not focus   | main PTIB workflow   |
| 06 program proj.     | base program output  | limited/linked       | private rows added   |
| 07 occupation proj.  | base occupation      | QI comparison        | includes private adj.|
+----------------------+----------------------+----------------------+----------------------+
```

The regular run builds the base model, the QI run focuses on quality/stability indicators for labour and occupation outputs, and the PTIB run integrates private institution records into selected projection outputs.

---

# 8.11 Key validation checks

## 8.11.1 Near-completers

Recommended checks:

```text
1. Count near-completers before STP credential matching.
2. Count near-completers removed because they later received credentials.
3. Count near-completers removed because they earlier received credentials.
4. Check ratios by gender, age group, CIP4, and year.
5. Compare ratios to prior model cycle.
```

---

## 8.11.2 Graduate projections

Recommended checks:

```text
1. Check graduate projection totals by year.
2. Check totals by credential.
3. Check totals by age group.
4. Check totals by gender.
5. Check totals by CIP.
6. Compare historical years against source credential counts.
7. Confirm exclusions were applied.
8. Confirm rounding rules, if any, were applied only at the intended stage.
```

---

## 8.11.3 PTIB integration

Recommended checks:

```text
1. Confirm PTIB source row count.
2. Check excluded ESL / not-for-credit / unclassified rows.
3. Check CIP formatting and validity.
4. Check credential recoding.
5. Check age group recoding.
6. Check domestic / international split.
7. Confirm PTIB rows were added only where expected.
8. Compare public-only vs public + private totals.
```

---

## 8.11.4 Program projections

Recommended checks:

```text
1. Check projected distribution sums within groups.
2. Check static distribution sums within groups.
3. Check missing CIP4 or CIP2 values.
4. Check proxy usage.
5. Check apprenticeship-specific rows.
6. Check near-completer-specific rows.
7. Check PTIB-specific rows.
```

---

## 8.11.5 Occupation projections

Recommended checks:

```text
1. Check labour supply totals before occupation allocation.
2. Check occupation distribution percentages.
3. Check NOC rollups.
4. Check No_TT proxy rows.
5. Check private institution proxy rows.
6. Check missing NOC values.
7. Check exclusions.
8. Check public suppression preparation.
9. Compare regular, QI, and PTIB model totals.
```

---

# 8.12 Known risks

## 8.12.1 Projection dependency risk

Graduate projections, program projections, and occupation projections are sequential. A problem in graduate projections can cascade into program and occupation outputs.

```text
Graduate projection error
        |
        v
program distribution error
        |
        v
occupation projection error
        |
        v
reporting error
```

---

## 8.12.2 Proxy risk

Proxy logic is necessary when detailed data is missing, especially for `TTRAIN`, private institutions, or missing CIP4 distributions. However, proxies can change the distribution of projected supply and should be clearly flagged in validation outputs.

---

## 8.12.3 PTIB comparability risk

PTIB data is not directly comparable to STP because it is self-reported, not a unique headcount, and may exclude or include categories differently.

---

## 8.12.4 QI interpretation risk

QI outputs are not a replacement for the regular model. They are a quality or stability layer used to help determine whether outputs are reliable enough for release or interpretation.

---

# 8.13 Section 8 summary

The `02b–07` workflow turns cleaned source data and cohort distributions into projection outputs.

```text
02b
cohorts, labour supply, occupation distributions
        |
        v
03
near-completers
        |
        v
04
graduate projections
        |
        v
05
PTIB integration
        |
        v
06
program projections
        |
        v
07
occupation projections
```

The main outputs are:

- `T_Cohorts_Recoded`
- `Labour_Supply_Distribution`
- `Occupation_Distributions`
- `NearCompleters_CIP4`
- `T_DACSO_Near_Completers_RatioAgeAtGradCIP4`
- `T_DACSO_Near_Completers_RatioByGender`
- `Graduate_Projections`
- `Graduate_Projections_Include_Historical`
- `T_Private_Institutions_Credentials_Raw`
- `T_PTIB_Y1_to_Y10`
- `Cohort_Program_Distributions_Static`
- `Cohort_Program_Distributions_Projected`
- `tmp_tbl_Model`
- `tmp_tbl_QI`
- `tmp_tbl_Model_Inc_Private_Inst`

The key documentation rule for this section is:

> For each projection stage, document the input tables, scripts, projection method, output tables, run-type behaviour, row count checks, and validation checks.

---

# PSSM Documentation — Sections 9–10

**Scope:** Sections 9 and 10 only  
**Sections included:**

9. **Special Data Processes — Census, Imputation, PTIB**  
10. **Outputs and Reporting — 08 and downstream use**

---

# 9. Special Data Processes — Census, Imputation, PTIB

## 9.1 Purpose

This section brings together the special workflows that sit beside the main STP and Student Outcomes sequence. These processes are important because they provide data or methods that the core STP / Student Outcomes pipeline cannot fully cover on its own.

The main special processes are:

- custom Statistics Canada Census data
- Census NOC imputation
- Census-derived occupation distributions
- Census-derived labour supply distributions
- PTIB-specific cleaning and integration

These workflows support the model when the standard data is incomplete, suppressed, sparse, or outside the public post-secondary system.

In particular:

- **Census data** helps fill gaps for advanced credentials and detailed occupations.
- **NOC imputation** helps recover usable distributions where Census cells are suppressed or rounded.
- **Census labour supply distributions** supplement Student Outcomes where survey data is weak.
- **PTIB data** adds private training institution estimates not captured in STP.

---

## 9.2 Where these special processes fit

```text
Main PSSM pipeline
------------------

STP + Student Outcomes
        |
        v
Cohorts / projections
        |
        v
Final outputs


Special supporting workflows
----------------------------

Statistics Canada Census
        |
        +--> NOC imputation
        +--> Census occupation distributions
        +--> Census labour supply distributions

PTIB
        |
        +--> private institution cleaning
        +--> private graduate estimates
        +--> private program / occupation adjustments
```

The Census and PTIB workflows do not follow the same path as the main STP / Student Outcomes sequence. They feed into selected downstream tables, especially:

- `Labour_Supply_Distribution`
- `Occupation_Distributions`
- `Graduate_Projections`
- `Cohort_Program_Distributions`
- occupation projection output tables

---

# 9.3 Statistics Canada / Census workflow

## 9.3.1 Purpose

The Census workflow provides supplemental data where Student Outcomes data is sparse, suppressed, or unavailable.

It is especially important for advanced credentials, including:

- Master’s degrees
- Doctorates
- graduate certificates or diplomas
- professional degrees
- small occupation groups

The custom Census data can include dimensions such as:

- age
- credential
- labour force status
- school attendance
- major field of study / CIP grouping
- occupation / NOC
- geography
- location of study
- Census count fields

---

## 9.3.2 Census source flow

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
        |
        +--> occupation distribution process
        |
        +--> labour supply distribution process
```

The Census source file may arrive in Beyond 20/20 format and usually needs to be exported to CSV or Excel before loading into SQL Server.

Expected cleanup issues include:

- non-standard headers
- encoding issues
- unusual dashes or special characters
- geography labels that need standardization
- credential labels that differ from model labels
- suppressed or rounded count cells

---

## 9.3.3 Key table inventory — `STAT_CAN`

## Table: `STAT_CAN`

**Layer:**  
Raw Census / Statistics Canada source table.

**Source:**  
Custom Statistics Canada Census export, usually prepared from Beyond 20/20 `.ivt` files and exported to CSV or Excel before loading.

**Created by:**  
`load-custom-stats-can.R`

**Used by:**

- NOC imputation
- Census occupation distribution workflow
- Census labour supply distribution workflow
- supplemental advanced credential distribution logic

**Key fields:**

- geography
- age group
- credential
- labour force status
- school attendance
- major field of study / CIP grouping
- NOC / occupation fields
- Census count fields

**Refresh frequency:**  
When a new custom Census table is ordered and processed.

**Notes:**  
Census suppression and random rounding mean this table should not be treated as a complete unsuppressed source. Imputation and validation are required before using it in downstream distributions.

---

# 9.4 Census NOC imputation

## 9.4.1 Purpose

Census NOC imputation estimates suppressed occupation counts using available rollup totals.

This is needed because detailed NOC counts can be suppressed or rounded, especially when data are split by:

- credential
- field of study
- age group
- region
- occupation

The imputation process uses higher-level NOC totals to estimate lower-level suppressed counts.

---

## 9.4.2 Imputation scripts

Key scripts include:

```text
run-imputation-by-region.R
noc-imputation.R
```

These scripts run imputation by region and produce regional output files that are later used by the occupation distribution workflow.

---

## 9.4.3 Imputation logic — plain-language version

The imputation logic asks:

> If a detailed NOC count is zero, missing, or suppressed, can we estimate it using the parent NOC total and the distribution of known lower-level NOC values?

Basic approach:

```text
Detailed NOC count is present
        |
        v
keep observed count


Detailed NOC count is zero / missing / suppressed
        |
        v
use parent NOC total
        |
        v
subtract known non-zero child counts
        |
        v
allocate remaining count proportionally
        |
        v
create imputed detailed count
```

---

## 9.4.4 Imputation flow

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
regional "new counts" files
   |
   v
occupation distribution processing
```

The output of the imputation step is usually a set of region-specific files containing imputed “new counts.” These files are then combined for the Census occupation distribution workflow.

---

## 9.4.5 Known limitations

Census imputation is constrained by:

- suppression
- random rounding
- small counts
- the level of detail available in the custom table
- changes in Census table design
- changes in NOC or CIP standards

The imputation method should be treated as a controlled approximation rather than a recovery of true unsuppressed values.

Any future change to Census table structure, credential detail, NOC version, or CIP grouping should trigger a review of the imputation method.

---

# 9.5 Census occupation distributions

## 9.5.1 Purpose

The Census occupation distribution workflow converts imputed Census counts into an occupation distribution table that can supplement the main PSSM occupation distribution process.

This is especially useful where Student Outcomes data is weak, sparse, or suppressed for advanced credentials.

The main output is:

```text
Occupation_Distributions_Stat_Can
```

This table can later be incorporated into the broader `Occupation_Distributions` workflow.

---

## 9.5.2 Script

Key script:

```text
occ-dists-census-data.R
```

This script:

- imports regional imputed NOC count files
- applies region lookups and rollups
- appends calculated geographies where needed
- reshapes credential columns
- calculates totals
- calculates occupation percentages
- saves the final table to SQL Server

---

## 9.5.3 Occupation distribution flow

```text
Regional imputed Census files
        |
        v
combine into one table
        |
        v
apply region lookup / rollup
        |
        +--> calculate Northeast
        |
        +--> calculate Rest of Canada
        |
        v
reshape credential counts
        |
        v
calculate total by CIP / region / age / credential
        |
        v
calculate occupation percentages
        |
        v
Occupation_Distributions_Stat_Can
```

---

## 9.5.4 Table inventory — `Occupation_Distributions_Stat_Can`

## Table: `Occupation_Distributions_Stat_Can`

**Layer:**  
Census-derived occupation distribution table.

**Source:**  
Imputed Census NOC counts by region, age group, credential, field of study, and occupation.

**Created by:**  
`occ-dists-census-data.R`

**Used by:**

- occupation distribution supplementation
- advanced credential occupation distributions
- downstream occupation projection logic where Census supplementation is required

**Key fields:**

- survey / Census label
- region code
- age group
- credential
- CIP / field of study grouping
- NOC code
- count
- total
- percent

**Refresh frequency:**  
When the Census custom table is updated and imputation is rerun.

**Notes:**  
Review region names and rollup logic each cycle because geography labels can change between exports.

---

# 9.6 Census labour supply distributions

## 9.6.1 Purpose

The Census labour supply workflow estimates labour supply rates for advanced credentials and other groups where Student Outcomes data may not be sufficient.

The main output is:

```text
Labour_Supply_Distribution_Stat_Can
```

This table supports downstream labour supply and occupation projection logic.

---

## 9.6.2 Script

Key script:

```text
labour-supply-dists-census-data.R
```

This script:

- processes filtered and unfiltered Census exports
- applies region crosswalks
- applies age group rollups
- calculates labour supply values
- combines credential-specific calculations
- saves the final table to SQL Server

---

## 9.6.3 Credential-specific handling

The Census labour supply workflow treats some credentials differently.

Typical grouping:

```text
GRCT / GRDP
PDEG
MAST
   |
   v
standard Census labour supply calculation


DOCT
   |
   v
special doctoral calculation
   |
   v
BC distribution and Rest of Canada logic
```

The doctoral workflow should be validated separately because doctoral counts are often small and more affected by suppression.

---

## 9.6.4 Labour supply calculation concept

The Census labour supply workflow calculates labour supply using employment and unemployment categories.

Conceptual formula:

```text
Labour Supply = Employed + Unemployed not attending school
```

Then:

```text
New_Labour_Supply = Labour Supply / Total
```

Flow:

```text
Employment / labour force counts
        |
        v
calculate labour supply
        |
        v
divide by total for age / CIP / region / credential
        |
        v
New_Labour_Supply rate
```

---

## 9.6.5 Labour supply distribution flow

```text
Filtered Census export
Unfiltered Census export
        |
        v
load and clean export sheets
        |
        v
apply region crosswalk
        |
        v
create Northeast and Rest of Canada rows
        |
        v
calculate labour supply by credential
        |
        v
combine credential-specific tables
        |
        v
Labour_Supply_Distribution_Stat_Can
```

---

## 9.6.6 Table inventory — `Labour_Supply_Distribution_Stat_Can`

## Table: `Labour_Supply_Distribution_Stat_Can`

**Layer:**  
Census-derived labour supply distribution table.

**Source:**  
Custom Census labour force / attendance / credential exports, including filtered and unfiltered data cuts.

**Created by:**  
`labour-supply-dists-census-data.R`

**Used by:**

- labour supply distribution supplementation
- advanced credential labour supply estimates
- downstream occupation projection logic

**Key fields:**

- survey / Census label
- region code
- age group
- credential
- CIP / field of study grouping
- labour supply
- `New_Labour_Supply`
- total

**Refresh frequency:**  
When the Census custom table is updated and labour distribution processing is rerun.

**Notes:**  
The doctoral workflow is different from the other credentials and should be validated separately.

---

# 9.7 PTIB-specific cleaning and integration

## 9.7.1 Purpose

PTIB data adds private training institution estimates to the model.

It is handled as a special process because it does not come through STP and has different:

- definitions
- coverage
- quality constraints
- reporting rules
- sensitivity considerations

PTIB data includes:

- institution
- program
- CIP
- credential
- age group
- immigration status
- graduates
- enrolments
- total enrolments

---

## 9.7.2 PTIB source flow

```text
PTIB Excel / CSV file
        |
        v
load-ptib.R or PTIB cleaning workflow
        |
        v
T_Private_Institutions_Credentials_Raw
        |
        v
credential / CIP / age / immigration cleaning
        |
        v
PTIB projection tables
        |
        v
Graduate_Projections
Cohort_Program_Distributions
Occupation projections
```

The PTIB workflow supports the PTIB model run and feeds selected projection tables, including graduate projections, program distributions, and occupation projection outputs.

---

## 9.7.3 PTIB cleaning steps

Core PTIB cleaning steps include:

- standardizing credential labels
- cleaning and validating CIP codes
- removing periods from CIP codes where needed
- checking CIP length and validity
- excluding ESL records
- excluding not-for-credit records
- excluding unclassified records
- excluding other out-of-scope program records
- recoding age groups
- recoding immigration status blanks or unknowns
- aggregating graduates and enrolments by credential, CIP, age, immigration, and year

---

## 9.7.4 PTIB integration flow

```text
Cleaned PTIB records
        |
        v
aggregate graduates by year / credential / CIP / age / immigration
        |
        v
calculate domestic and international shares
        |
        v
create PTIB projection rows
        |
        +--> Graduate_Projections
        |
        +--> Cohort_Program_Distributions
        |
        +--> occupation projection inputs
```

The PTIB run adds private institution rows to selected projection tables rather than replacing public-system projections.

---

## 9.7.5 Table inventory — `T_Private_Institutions_Credentials_Raw`

## Table: `T_Private_Institutions_Credentials_Raw`

**Layer:**  
PTIB raw / staging table.

**Source:**  
PTIB private training institution data file.

**Created by:**  
`load-ptib.R` or the PTIB cleaning workflow.

**Used by:**

- PTIB cleaning
- PTIB graduate estimates
- private institution program projections
- PTIB rows in occupation projections

**Key fields:**

- year
- institution
- program title
- CIP code
- credential
- age group
- immigration status
- graduates
- enrolments
- total enrolments

**Refresh frequency:**  
Each model cycle or whenever a new PTIB extract is provided.

**Notes:**  
PTIB is self-reported and not a unique headcount. It should be validated separately from STP and Student Outcomes data.

---

## 9.7.6 Table inventory — `T_PTIB_Y1_to_Y10`

## Table: `T_PTIB_Y1_to_Y10`

**Layer:**  
PTIB projection support table.

**Source:**  
Cleaned and aggregated PTIB records.

**Created by:**  
Stage `05` PTIB workflow.

**Used by:**

- PTIB graduate projection rows
- PTIB program distribution rows
- PTIB occupation projection adjustments

**Key fields:**

- projection year
- credential
- CIP
- age group
- graduate estimate
- domestic / international fields where applicable

**Refresh frequency:**  
Each PTIB run.

**Notes:**  
This table bridges cleaned PTIB source data into the projection-year model structure.

---

# 9.8 How special processes connect to main model tables

```text
Census / StatCan
----------------
STAT_CAN
   |
   +--> NOC imputation
   |
   +--> Occupation_Distributions_Stat_Can
   |
   +--> Labour_Supply_Distribution_Stat_Can
                    |
                    v
        main distribution workflows


PTIB
----
T_Private_Institutions_Credentials_Raw
   |
   +--> T_PTIB_Y1_to_Y10
   |
   +--> PTIB graduate rows
   |
   +--> PTIB program rows
   |
   +--> PTIB occupation projection rows
                    |
                    v
        main projection workflows
```

The Census workflows mainly supplement labour supply and occupation distributions, while PTIB workflows mainly add private institution records into graduate, program, and occupation projection outputs.

---

# 9.9 Validation checks

## 9.9.1 Census source validation

Recommended checks:

```text
1. Confirm Census export dimensions match the requested table.
2. Confirm geography labels are clean and mapped.
3. Confirm age groups are present and expected.
4. Confirm credential categories are present and expected.
5. Confirm CIP groupings align with model definitions.
6. Confirm NOC version and digit level are correct.
7. Confirm row counts after load to STAT_CAN.
8. Check for encoding issues in headers and geography values.
```

---

## 9.9.2 NOC imputation validation

Recommended checks:

```text
1. Check imputed files were created for each expected region.
2. Check parent NOC totals before and after imputation.
3. Check no negative imputed counts remain after rounding correction.
4. Check suppressed zero values were treated consistently.
5. Compare known non-zero counts before and after imputation.
6. Review high-impact NOC / CIP / credential combinations.
```

---

## 9.9.3 Census occupation distribution validation

Recommended checks:

```text
1. Confirm all expected regional imputation files were imported.
2. Confirm region rollups were applied correctly.
3. Confirm Northeast and Rest of Canada rows were calculated correctly.
4. Confirm credentials were reshaped into rows correctly.
5. Confirm totals by CIP / region / age / credential.
6. Confirm percentages equal count divided by total.
7. Confirm zero-count rows were removed or retained according to workflow rules.
8. Confirm final table was saved to SQL Server.
```

---

## 9.9.4 Census labour supply distribution validation

Recommended checks:

```text
1. Confirm filtered and unfiltered Census exports were loaded correctly.
2. Confirm credential-specific input choice is correct.
3. Confirm region crosswalks were applied.
4. Confirm Northeast and Rest of Canada rows were calculated correctly.
5. Confirm labour supply formula results do not exceed total population.
6. Confirm DOCT logic was validated separately.
7. Confirm final New_Labour_Supply rates are reasonable.
8. Confirm final table was saved to SQL Server.
```

---

## 9.9.5 PTIB validation

Recommended checks:

```text
1. Confirm source row count and source years.
2. Confirm credential recoding.
3. Confirm CIP format and validity.
4. Confirm ESL / not-for-credit / unclassified exclusions.
5. Confirm age group recoding.
6. Confirm immigration status recoding.
7. Confirm graduate and enrolment totals before and after aggregation.
8. Confirm domestic / international split.
9. Confirm PTIB rows were added only to intended projection tables.
10. Compare public-system totals versus public + private totals.
```

---

# 9.10 Known risks

## 9.10.1 Census suppression and rounding risk

Census counts are affected by suppression and random rounding. Imputation can estimate missing detailed counts, but it cannot recover the true unsuppressed data.

Census-derived distributions should therefore be treated as approximations and documented as such.

---

## 9.10.2 Census export structure risk

Beyond 20/20 exports can require manual reshaping before load, and header or geography text can change between exports.

Manual dimension selection, CSV/XLS export choices, encoding issues, and cleanup of special characters should be documented each cycle.

---

## 9.10.3 Credential-specific Census logic risk

The labour supply workflow uses different methods for different advanced credentials, especially doctoral credentials.

Any change to this logic could affect advanced credential labour supply projections and should be reviewed carefully.

---

## 9.10.4 PTIB comparability risk

PTIB data is not directly comparable to STP because it is self-reported, not a unique headcount, and may exclude program or enrolment categories differently.

---

## 9.10.5 PTIB publication / sensitivity risk

PTIB private training institution data can be business sensitive and may have restrictions on publication, sharing, or use beyond the original request.

This should be clearly flagged in the documentation and reporting workflow.

---

# 9.11 Section 9 summary

The special data workflows support parts of the model that cannot be handled fully by STP and Student Outcomes alone.

```text
Census / StatCan
        |
        +--> STAT_CAN
        +--> NOC imputation
        +--> Occupation_Distributions_Stat_Can
        +--> Labour_Supply_Distribution_Stat_Can


PTIB
        |
        +--> T_Private_Institutions_Credentials_Raw
        +--> T_PTIB_Y1_to_Y10
        +--> private graduate rows
        +--> private program rows
        +--> private occupation projection rows
```

The most important special-process outputs are:

- `STAT_CAN`
- regional Census imputation output files
- `Occupation_Distributions_Stat_Can`
- `Labour_Supply_Distribution_Stat_Can`
- `T_Private_Institutions_Credentials_Raw`
- `T_PTIB_Y1_to_Y10`
- PTIB rows in graduate, program, and occupation projection tables

The key documentation rule for this section is:

> Treat Census and PTIB workflows as special controlled inputs: document the source file, manual preparation, scripts, assumptions, output tables, validation checks, and known limitations every cycle.

---

# 10. Outputs and Reporting — 08 and Downstream Use

## 10.1 Purpose

This section documents the final reporting layer of the PSSM pipeline.

It covers:

- final Excel outputs
- CSV outputs
- SQL output tables
- internal and public releases
- suppression
- quality indicators
- coverage indicators
- downstream dashboards
- AEST-facing products
- output storage

The reporting stage uses final projection and distribution tables from earlier steps, especially:

- `Graduate_Projections`
- `Cohort_Program_Distributions`
- `Labour_Supply_Distribution`
- `Occupation_Distributions`
- occupation projection output tables

---

## 10.2 Where reporting fits in the pipeline

```text
01a–01d
STP preprocessing
        |
        v
02a–02b
program matching / cohorts / distributions
        |
        v
03–07
near-completers / graduate projections / PTIB /
program projections / occupation projections
        |
        v
08
outputs and reporting
        |
        +--> internal Excel outputs
        +--> public Excel outputs
        +--> CSV extracts
        +--> appendix tables
        +--> downstream dashboards / analysis
```

The reporting stage should only run after the main projection tables have passed validation.

---

# 10.3 Main reporting inputs

The final reporting stage draws from several major tables.

```text
+-----------------------------------------+
| Main Reporting Inputs                   |
+-----------------------------------------+
| Graduate_Projections                    |
| Graduate_Projections_Include_Historical |
| Cohort_Program_Distributions_Static     |
| Cohort_Program_Distributions_Projected  |
| Labour_Supply_Distribution              |
| Occupation_Distributions                |
| tmp_tbl_Model                           |
| tmp_tbl_QI                              |
| tmp_tbl_Model_Inc_Private_Inst          |
| suppression / exclusion tables          |
| QI / CI support tables                  |
+-----------------------------------------+
```

These tables support final occupation projections, graduate projection appendices, internal and public output files, and downstream analytical products.

---

# 10.4 Final output formats

## 10.4.1 Excel outputs

Excel is the main reporting format for analysts and business users.

Typical Excel outputs include:

- public release workbooks
- internal release workbooks
- graduate projection appendix tables
- occupation projection tables
- CIP-to-NOC and NOC-to-CIP graph workbooks
- validation and review workbooks

Flow:

```text
SQL final tables
        |
        v
reporting scripts / queries
        |
        v
Excel workbook outputs
        |
        +--> internal release
        +--> public release
        +--> appendix tables
        +--> graph workbooks
```

---

## 10.4.2 CSV outputs

CSV outputs are useful for downstream tools, dashboards, and reproducible analysis.

CSV outputs should be used when:

- the data will be consumed by Power BI or another dashboard tool
- the output needs to be versioned or compared across runs
- the user needs flat files instead of formatted workbooks
- large tables are easier to move as CSV than Excel

---

## 10.4.3 SQL output tables

SQL Server remains the source of truth for final model tables.

Excel and CSV outputs should be treated as extracts from final SQL tables, not as the authoritative model state.

---

# 10.5 Internal vs public release

## 10.5.1 Internal release

The internal release is intended for analysts and approved internal users.

It may include:

- all projection years
- unsuppressed or less-suppressed detail
- intermediate quality indicators
- coverage indicators
- diagnostic fields
- internal-only appendix tables
- tables useful for validation and review

---

## 10.5.2 Public release

The public release is the version intended for wider distribution.

It applies:

- suppression rules
- exclusion rules
- quality indicator filtering
- coverage indicator logic
- rounding
- removal of sensitive or unstable cells
- removal of internal-only diagnostic fields

The public release is focused on safe, interpretable outputs rather than maximum detail.

---

## 10.5.3 Internal vs public release flow

```text
Final model tables
        |
        v
apply validation checks
        |
        +-----------------------------+
        |                             |
        v                             v
Internal release                Public release
more detail                     suppressed / filtered
all years where needed          release-safe years/detail
diagnostics included            diagnostics removed
```

---

# 10.6 Suppression

## 10.6.1 Purpose

Suppression protects against releasing unstable, sensitive, or low-count results.

It is especially important for occupation-level outputs, where detailed NOC, CIP, credential, age, and region combinations can produce small counts.

---

## 10.6.2 Suppression logic

Suppression may apply to:

- low NOC counts
- small cell counts
- unstable QI values
- records with insufficient coverage
- unknown or invalid NOC values
- excluded credentials
- excluded CIPs
- excluded regions
- cells affected by extreme weights

Flow:

```text
Final projection cell
        |
        v
check count threshold
        |
        v
check quality indicator
        |
        v
check coverage indicator
        |
        v
check exclusion rules
        |
        +--> keep for public release
        |
        +--> suppress / aggregate / exclude
```

---

## 10.6.3 Suppression timing risk

If suppression is applied after percentages are calculated, the remaining displayed percentages may no longer sum intuitively for users.

Where possible, suppression should be integrated into the percentage calculation process or clearly documented in release notes.

---

# 10.7 Quality indicators

## 10.7.1 Purpose

Quality indicators help assess whether model outputs are stable enough for interpretation or release.

They are especially relevant for labour supply and occupation distribution outputs, where small sample sizes, weighting, suppression, and proxy logic can affect reliability.

---

## 10.7.2 QI workflow

The QI workflow is a separate run or linked model variant.

It reuses selected regular model outputs and focuses on quality and stability in labour supply and occupation distribution tables.

```text
Regular model outputs
        |
        v
QI run / linked QI tables
        |
        v
compare stability
        |
        v
calculate QI values
        |
        v
filter or flag public outputs
```

---

## 10.7.3 QI use in reporting

QI values should be used to:

- flag unstable outputs
- filter public-release records
- support internal review
- explain why some cells are suppressed or excluded
- compare regular and quality-adjusted model outputs

QI values should be clearly labelled as quality or stability indicators, not projected supply counts.

---

# 10.8 Coverage indicators

## 10.8.1 Purpose

Coverage indicators show how much of an occupation estimate is covered by public post-secondary data versus public plus private training data.

This is especially important when PTIB is integrated into the model.

---

## 10.8.2 Coverage indicator concept

The coverage indicator can be described as:

```text
Coverage Indicator =
public post-secondary NOC count
--------------------------------
public + private NOC count
```

This produces the share of an occupation estimate attributable to public post-secondary sources.

---

## 10.8.3 Coverage indicator flow

```text
public model output
        |
        v
public + private model output
        |
        v
compare by NOC / credential / region / age / year
        |
        v
coverage indicator
        |
        v
public/internal reporting
```

Coverage indicators help users understand where private institution estimates materially affect the final projected supply.

---

# 10.9 Reporting queries and output tables

Examples of reporting queries and output products include:

- `Qry_10a_Model_Public_Release`
- `Qry_10a_Model_QI_PPCI`
- appendix queries for graduate projections
- CIP-to-NOC output tables
- NOC-to-CIP output tables
- public Excel outputs
- internal Excel outputs

`Qry_10a_Model_Public_Release` is used for rounded and suppressed public-release outputs.

`Qry_10a_Model_QI_PPCI` supports QI and coverage indicators for internal use.

---

# 10.10 CIP-to-NOC and NOC-to-CIP downstream outputs

## 10.10.1 Purpose

CIP-to-NOC and NOC-to-CIP outputs support occupational outcomes reporting and external analytical products.

They show:

- which occupations are associated with a field of study
- which fields of study are associated with an occupation

These outputs are derived from labour supply and occupation distribution tables and may be delivered as Access tables, Excel workbooks, graph products, SQL tables, or CSV outputs.

---

## 10.10.2 Output tables

Key tables include:

- `CIP_to_NOC`
- `CIP_Totals`
- `NOC_to_CIP`
- `NOC_Totals`
- `CIP4D_Distributions_NOC`
- `CIP4D_Distributions_NOC_Credential`
- `Occupation_Distributions_CIP4D`
- `Occupation_Distributions_CIP4D_CRED`

---

## 10.10.3 Downstream flow

```text
Labour_Supply_Distribution
Occupation_Distributions
        |
        v
CIP-to-NOC / NOC-to-CIP calculations
        |
        v
Access / SQL output tables
        |
        v
Excel graph macros
        |
        v
AEST-facing workbooks and documentation
```

---

# 10.11 Downstream dashboards and analytical use

Final outputs can feed:

- Power BI dashboards
- Excel reporting workbooks
- AEST briefing tables
- occupational outcomes products
- internal validation tools
- appendix tables
- future PSSM model runs

For dashboard use, the best practice is to export clean, flat CSV or SQL views rather than use formatted Excel workbooks as the source.

Excel workbooks are useful for presentation; SQL tables or CSV extracts are better for reproducible reporting layers.

---

# 10.12 Output storage

## 10.12.1 SQL Server

Final model tables should be retained in SQL Server.

SQL Server is the authoritative storage layer for final tables and downstream queries.

Examples of final or near-final SQL tables include:

- `Graduate_Projections`
- `Graduate_Projections_Include_Historical`
- `Cohort_Program_Distributions_Static`
- `Cohort_Program_Distributions_Projected`
- `Labour_Supply_Distribution`
- `Occupation_Distributions`
- `tmp_tbl_Model`
- `tmp_tbl_QI`
- `tmp_tbl_Model_Inc_Private_Inst`

---

## 10.12.2 LAN reporting folders

Excel, CSV, and manual review outputs are stored in PSSM project folders on the LAN.

Reporting folders should be organized by:

- model cycle
- release type
- output type
- run type
- creation date

---

## 10.12.3 SharePoint / AEST delivery

Some downstream outputs, especially CIP-to-NOC and NOC-to-CIP products, are posted to SharePoint for AEST use.

These may include:

- Access databases
- Excel Top 10 files
- graph workbooks
- Word documentation
- methodology notes

---

## 10.12.4 GitHub

Code and reproducible documentation should remain in the project repository.

The repository should contain:

- scripts
- process notes
- methodology documentation
- supporting readme files
- reproducibility notes

---

# 10.13 Recommended reporting validation checklist

Before producing outputs:

```text
1. Confirm all required final SQL tables exist.
2. Confirm row counts for final projection tables.
3. Confirm graduate projection totals by year and credential.
4. Confirm program distributions sum correctly.
5. Confirm labour supply distributions sum correctly.
6. Confirm occupation distribution percentages are valid.
7. Confirm PTIB rows are included only where intended.
8. Confirm QI values were calculated.
9. Confirm coverage indicators were calculated.
10. Confirm suppression and exclusion rules are current.
```

Before public release:

```text
1. Confirm small cells are suppressed.
2. Confirm unstable QI cells are removed or flagged.
3. Confirm coverage indicators are included or used correctly.
4. Confirm excluded credentials, CIPs, regions, and NOCs are removed.
5. Confirm rounded outputs match release rules.
6. Confirm internal-only columns are removed.
7. Confirm public workbook does not expose suppressed values.
8. Confirm totals still make sense after suppression.
9. Confirm final files are saved to the correct release folder.
10. Confirm release notes document limitations.
```

---

# 10.14 Known reporting risks

## 10.14.1 Suppression after percentage calculation

If suppression is applied after percentages are calculated, visible percentages may not align intuitively with visible totals.

This should be documented and, where possible, improved in future workflows.

---

## 10.14.2 Internal data accidentally released

Internal outputs can include more detail than public outputs.

Public release scripts must remove internal-only fields, suppressed cells, and diagnostic columns.

---

## 10.14.3 QI misunderstood as model output

QI values should be interpreted as quality/stability indicators, not as projected supply counts.

Reports should clearly label QI and coverage fields.

---

## 10.14.4 PTIB coverage misunderstood

PTIB data is self-reported and not a unique headcount.

Coverage indicators should be used to explain how much private institution data affects occupation estimates.

---

## 10.14.5 Dashboard source drift

If dashboards are built from manually edited Excel files instead of controlled SQL tables or CSV extracts, numbers can drift from the official model outputs.

The preferred downstream source should be a final SQL table or versioned CSV extract.

---

# 10.15 Section 10 summary

The reporting stage converts final model tables into internal, public, and downstream analytical products.

```text
Final SQL model tables
        |
        v
reporting queries / scripts
        |
        +--> internal release
        |
        +--> public release
        |
        +--> appendix tables
        |
        +--> CSV extracts
        |
        +--> dashboards
        |
        +--> AEST-facing CIP/NOC products
```

The most important reporting concepts are:

- **Internal release:** more detail, diagnostics, and unsuppressed data where permitted.
- **Public release:** suppression, filtering, rounding, and quality controls applied.
- **Suppression:** protects small, unstable, or sensitive cells.
- **QI:** measures stability or quality of labour and occupation outputs.
- **Coverage indicator:** compares public-only output to public + private output.
- **Downstream use:** Excel, CSV, SQL views, dashboards, AEST products, and future model runs.

The key documentation rule for this section is:

> Every reporting output should document its source table, script or query, release type, suppression rules, QI and coverage logic, storage location, and intended downstream use.

