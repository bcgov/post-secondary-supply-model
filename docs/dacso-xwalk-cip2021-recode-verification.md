# DACSO XWALK CIP2021 Recode - Verification Instructions for the Student Outcomes Survey Team

**Date:** 2026-08-15
**File to review:** `xwalk-seed-recode-review-2026-08-15.csv` (attached / alongside this
document; 229 rows)
**Contact:** Jon Duan, PSSM analyst
**Estimated effort:** most rows are quickly confirmed from the registry NOTES; the subset
needing real judgment is small (see review order below).

## What we are asking

The Post-Secondary Supply Model (PSSM) is migrating its CIP taxonomy from CIP2016 to
CIP2021 for the 2024/25 data refresh. Part of that migration rebuilt the DACSO
program-to-CIP4 crosswalk (XWALK) that the model uses to assign CIP codes to diploma,
associate, and certificate credentials.

The rebuild started from the last model run's XWALK (the 2021-23 window) and, wherever a
program's old CIP4 disagreed with the current INFOWARE registry coding, **we adopted the
registry's CIP2021 code**. The attached CSV lists every one of those changes: 229 rows,
about 165 distinct programs.

Please verify that the registry code we adopted is the correct CIP2021 coding for each
program. You maintain the INFOWARE program registry, so you are the authority on these
codes.

## What to do

1. Open the CSV (Excel or any spreadsheet tool).
2. For each row, judge whether `REG_CIP4` / `NEW_CIP4_NAME` is the right coding for the
   program named in `PRGM_INST_PROGRAM_NAME`.
3. Fill `REVIEW_VERDICT` with exactly one of:
   - `OK` - the registry code is correct; keep it.
   - `WRONG` - the registry code does not fit this program; record the correct CIP4 in
     `REVIEW_NOTES` (and, if you can, the reason).
   - `UNSURE` - needs discussion; add a note.
4. Return the completed CSV.

## How to read each row

Reading a row: "For institution `COCI_INST_CD`, program `PRGM_LCPC_CD`
(`PRGM_INST_PROGRAM_NAME`), the old XWALK said CIP4 `CIP_CODE_4`
(`OLD_CIP4_NAME`); the current registry codes it `REG_CIP4` (`NEW_CIP4_NAME`)."

| Column | Meaning |
|---|---|
| `COCI_INST_CD` | Institution code (PSI side). |
| `PRGM_LCPC_CD` | Program code (institution's own code). |
| `PRGM_INST_PROGRAM_NAME` | Program name as registered. |
| `CIP_CODE_4` | Old CIP4 from the 2021-23 XWALK (CIP2016 era). |
| `OLD_CIP4_NAME` | Name of that old code, from the CIP2016 4-digit lookup. |
| `REG_CIP4` | New CIP4 we adopted, from the current INFOWARE registry (CIP2021). |
| `NEW_CIP4_NAME` | Name of that new code, from the CIP2021 4-digit lookup. |
| `PRIOR_ANALYST_FLAG` | TRUE if a past PSSM cycle had manually updated this row's CIP - i.e. the recode overrode an earlier analyst decision. Only 2 rows. |
| `REG_PRGM_ID` | Registry program ID - use this when reporting a correction to INFOWARE. |
| `REG_NOTES` | The registry's own note for the program (often explains the change). |
| `REVIEW_VERDICT` | Yours to fill: OK / WRONG / UNSURE. |
| `REVIEW_NOTES` | Yours to fill. |

## Suggested review order

1. **The two flagged rows first** (`PRIOR_ANALYST_FLAG` = TRUE). These are the only rows
   where the recode overrode a previous manual decision:
   - TRU / `PCT` - Post-Baccalaureate Certificate, Tourism: 3101 (Parks & recreation)
     becomes 5209 (Hospitality administration/management).
   - SEL / `EACSW` - Education Assistant and Community Support Worker: 1907 (Human
     development & family studies) becomes 1315 (Teaching assistants/aides).
   Both carry registry NOTES recording a deliberate re-code during program review (2025
   and 2026 respectively), so we expect these to confirm, but they deserve a close look.
2. **Rows with an empty `REG_NOTES`** - no INFOWARE explanation on record; these need
   judgment against the program name.
3. **The rest** - most rows' NOTES read like "Changed CIP from 520301 ... in 2025 during
   program review", i.e. the registry re-coded the program itself. A quick check that the
   new name suits the program is enough.

Rows by institution, if you prefer to batch: BCIT 81, CAM 24, DOUG 22, TRU 17, LANG 16,
and smaller counts for 11 more institutions.

## Why this matters / what happens next

These codes flow directly into the model's CIP4-level credential keys for the 2024/25
refresh, so uncorrected mis-codes would propagate into the projections.

- For `WRONG` rows: the durable fix is an INFOWARE registry correction (please reference
  `REG_PRGM_ID`). We can also hold a local override in the model pipeline until the
  registry catches up - note that in `REVIEW_NOTES` and we will follow up.
- For `UNSURE` rows: we will arrange a short call.

## Provenance

- Old values: the 2021-23 XWALK export from the 2023 model run (LAN archive copy).
- New values: `INFOWARE_PROGRAMS` registry coding joined through the CIP2021 6-digit
  lookup (`LCIP_CD_CIP2021` -> `LCP6_CD`), restricted to unambiguous registry entries -
  where the registry itself gives conflicting codes for the same program key, the old
  value was kept and the row does not appear in this file.
- Both lookups (CIP2016 and CIP2021) are the INFOWARE L_CIP_4DIGITS tables.
