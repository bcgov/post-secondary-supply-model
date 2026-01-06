# SOP: Running the 02a Program Matching Module

**Last Updated:** December 22, 2025
**Module:** 02a - Program Matching (CIP Code Alignment)

## 1. Overview
This module aligns institutional program codes from the Student Transitions Project (STP) with standardized Classification of Instructional Programs (CIP) codes. It uses external data from **APPSO**, **BGS**, and **DACSO** to "solve" the correct CIP codes for credentials.

**Output:** The final output is the table `Credential_Non_Dup_Updated` in the SQL database.

## 2. Prerequisites
Before running these scripts, ensure:
1.  **`config.yml`** is set up in the project root with the correct:
    *   `decimal`: SQL Server connection details.
    *   `infoware`: Oracle/Infoware connection details (or ensure static Infoware tables are already loaded in your schema).
    *   `myschema`: The target schema for writing tables.
2.  **Base Tables:** The `Credential_Non_Dup` and `STP_Credential` tables must exist in the database (output of module 01).

## 3. Execution Order
The scripts **must** be run in the specific order below.

### Step 0: Load Infoware Lookups (Initialization)
*   **Script:** `R/load-infoware-lookups.R`
*   **Action:** Connects to the Infoware (Oracle) database and loads static CIP, Program, and BGS survey tables into the SQL Server.
*   **Output Tables:** `INFOWARE_L_CIP_*`, `INFOWARE_PROGRAMS`, `INFOWARE_BGS_*`.
*   *Note:* This only needs to be run once per update cycle or if lookup data changes.

### Step 1: APPSO Matching
*   **Script:** `R/02a-appso-programs.R`
*   **Action:** Cleans APPSO program codes and maps them to CIPs using Infoware lookups.
*   **Output Table:** `Credential_Non_Dup_APPSO_IDs`

### Step 2: BGS Matching
*   **Script:** `R/02a-bgs-program-matching.R`
*   **Action:** 
    *   Builds BGS outcomes data.
    *   Matches BGS survey respondents to STP records via PEN/Institution/Year.
    *   Applies manual matching logic (if configured).
*   **Output Tables:** 
    *   `Credential_Non_Dup_BGS_IDs`
    *   `Credential_Non_Dup_GRAD_IDs`
    *   `T_BGS_Data_Final_for_OutcomesMatching`

### Step 3: DACSO Matching
*   **Script:** `R/02a-dacso-program-matching.R`
*   **Action:**
    *   Updates the DACSO/STP Crosswalk (XWALK) with new programs.
    *   Matches STP programs to the XWALK using specific logic for BCIT, CAPU, and VIU.
*   **Output Tables:**
    *   `Credential_Non_Dup_Programs_DACSO_FinalCIPS`
    *   `DACSO_STP_ProgramsCIP4_XWALK_ALL_2021_23` (Updated XWALK)

### Step 4: Consolidation (CRITICAL)
*   **Script:** `R/02a-update-cred-non-dup.R`
*   **Action:**
    *   Loads the master `Credential_Non_Dup` table.
    *   Joins the results from Steps 1, 2, and 3.
    *   Applies a priority logic (e.g., if DACSO has a match, use it; otherwise check BGS, etc.).
    *   Cleans any remaining NULL CIPs using heuristic matching.
*   **Output Table:** `Credential_Non_Dup_Updated`

## 4. Post-Execution Verification
After running Step 4, analysts should verify the results in the SQL Database:

1.  **Check Row Count:** `Credential_Non_Dup_Updated` should have the same row count as `Credential_Non_Dup`.
2.  **Check Nulls:** Run the following query to ensure matching improved data quality:
    ```sql
    SELECT count(*) 
    FROM "YOUR_SCHEMA"."Credential_Non_Dup_Updated" 
    WHERE FINAL_CIP_CODE_4 IS NULL
    ```
3.  **Promotion:** Once verified, the `Credential_Non_Dup_Updated` table is intended to replace `Credential_Non_Dup` for subsequent modules (02b, 03, etc.). 
    *   *Note for Analysts:* The current scripts create a *new* table to avoid accidental data loss. You may need to rename tables or point downstream scripts to `_Updated`.

## 5. Troubleshooting
*   **Infoware Connection:** If JDBC fails, ensure the static Infoware tables (`INFOWARE_L_CIP...`) exist in your SQL schema. The scripts will use them if they are present.
*   **Manual Matching:** The BGS and DACSO scripts contain sections for "Manual Matching". In the automated pipeline, these default to using STP codes if a manual override file isn't found. If manual intervention is required, review the specific "Part 3" sections in those R scripts.
