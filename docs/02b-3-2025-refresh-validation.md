# 02b-3 PSSM Cohorts Occupation Distributions — 2025 Refresh Validation Note

Branch `R/02b-3-pssm-cohorts-occupation-distributions`; validates the 2025 annual-refresh changes to
`R/02b-3-pssm-cohorts-occupation-distributions.R`. Wayfinder map: bcgov/post-secondary-supply-model#153
(tickets #154–#162; change list signed off at #156).

## What changed

Five commits on top of the 02b-2 branch tip (`fc8a6b8`):

- `4856419` **docs** — comment retrofit on the two census prep scripts (`labour-supply-dists-census-data.R`,
  `occ-dists-census-data.R`): why/what/how headers (OCCSN placement, no-survey-coverage rationale for
  graduate credentials, manual per-cycle prerequisite contract, occ → noc-imputation dependency,
  never-run-concurrently warning on the shared statcan lookup `_r` table, vintage-label note) plus
  per-section method comments.
- `86c2143` **docs** — comment-only retrofit on 02b-3 itself: OCCSN + OCC-weight header with the
  calibration identity, census-prerequisite provenance, weighting cross-reference map, six-table
  naming legend, append/write-back contracts.
- `4a66977` **docs** — 02b-2's provenance line states the run-by-hand census-prep contract explicitly
  (ticket #162 scope).
- `85c247c` **fix** — the signed-off change list (F1–F8): guarded `regular_run` default; the occ-StatCan
  read replaced with a guarded, schema-qualified `_r`-preferring read; census labels remapped to
  `2021 Census PSSM 2024-2025` (read-time remap + append hardcode); dead `library(RODBC)` and duplicate
  `db_config` dropped; `tables_to_keep` trimmed (02b-2's `labour_supply_distribution` removed);
  suppression-table write relocated into the `regular_run` block.
- `cb40ebd` **chore** — dead `library(RJDBC)` dropped from both census prep scripts (adjacent cleanup A1).

## The F2 bug, for the record

The occ-StatCan read was unqualified (`SQL('"Occupation_Distributions_Stat_Can"')`), resolving against
the connection's **default schema (dbo)** — a hard failure on PSSM2025 (no dbo copy exists) and never
the intended personal-schema table historically (the #154 probe observed a doubled 26-column result
set on PSSM2023). The fix (02b-2's guarded pattern) removes the class. The PSSM2023 **baseline
comparison is unaffected**: the baseline output tables were verified sound independently (explicit
schema-qualified reads), and the fresh census rows match them exactly (below).

## StatCan sourcing (#159, run by the user)

`R/occ-dists-census-data.R` was re-run by hand this cycle (with the labour-side twin). Verified
read-only: `Occupation_Distributions_Stat_Can_r` on PSSM2025 — **5,548 rows, identical to PSSM2023's
input on keys and COUNT/TOTAL/PERCENT**; label is the export vintage `2021 Census PSSM 2022-2023`,
remapped to the current model year at 02b-3 read time. Intermediates cleaned up by the script.

## Run

Standalone bootstrap (`.scratch/02b-3-2025-refresh/run-bootstrap.R`) loading dbo lookups + 02b-2's
personal-schema outputs (`t_cohorts_recoded_r` 313,036 rows with WEIGHT_NLS; `tmp_tbl_weights_nls_r`
19,080); the guarded `_r`-preferring read pulled the occ-StatCan table itself (fix exercised). Exit 0.
`WEIGHT_OCC` populated for 83,270 records. Written to the personal schema:
`occupation_distributions_r` 36,670 · `_no_tt_r` 27,020 · `_lcp2_r` 24,941 · `_lcp2_no_tt_r` 24,913 ·
`_lcp2_bc_r` 10,649 · `_lcp2_bc_no_tt_r` 10,610 · `t_suppression_public_release_noc_r` 346 (via the
relocated regular-run write).

## Verification vs PSSM2023

Baseline: the personal-schema `_r` tables on PSSM2023. As with 02b-2, exact cell matches are not
expected for Student Outcomes rows (the 5-year window slid 2019–2023 → 2021–2025); census rows have a
hard exact-match anchor.

| Check | Result |
|---|---|
| Structural parity (column sets, incl. `_bc_no_tt` mixed case + TTRAIN=NA shims) | identical ✓ |
| Census rows (5,548 = 5,548): keys, COUNT, TOTAL, PERCENT after label normalization | **exact match** ✓ |
| Internal identity: Σ Percent per stratum cell (5,567 cells) | max deviation 2.2e-16 ✓ |
| SO closeness, TOTAL ≥ 100 (n = 10,995) | median &#124;ΔPercent&#124; 0.008, p95 0.058, r 0.951 ✓ |
| SO closeness, all shared cells (n = 21,941) | r 0.891; drift concentrated in small cells |
| Suppression table overlap (AGE × NOC cells) | 259 of 346 / 341 shared (~75%) — expected small-cell churn ✓ |
| Row counts vs baseline (SO rows 31,122 vs 32,699) | tracks the slid window ✓ |

## Known quirks (documented, not fixed — pre-existing and symmetric across cycles)

- **q009 many-to-many join warning**: the `tmp_tbl_weights_occ` construction joins region *rollups*
  to the region *codes* they contain — a deliberate fan-out (one rollup covers several codes; the
  WEIGHT_OCC join-back then keys on code). dplyr ≥ 1.1 flags it; lookups verified free of duplicate
  codes/rollup values. Not a defect.
- The 20-char `LCIP4_CRED` filter in `_no_tt` (inherited SQL `nvarchar(20)` cast dropping three
  credential groups) and the PDEG BACH-22 double-count concern — BA notes carried forward.

## Breadcrumbs for the next efforts

- **03-near-completers / 06 / 07**: 07 reads the six `occupation_distributions*` tables (and the
  suppression table for public release) from the personal schema — all freshly written this cycle
  with census rows labeled `2021 Census PSSM 2024-2025` (02b-2's tables likewise). Any full-label
  census comparisons or filters written against "2023-2024" in 06/07 need the same one-line relabel.
- **Type-safety pass** (deferred): `RESPONDENT == "1"` and similar literal filters work via R
  coercion but are type-fragile across in-session vs DB-round-trip encodings — worth a consistent
  `as.numeric()`/`as.character()` guard pass across 02b-1/2/3 in a future effort.
- The 02b chain's census-prep prerequisites are now documented in all four script headers (02b-2,
  02b-3, and both prep scripts themselves) — ticket #162 closed.

## Assets (uncommitted, `.scratch/02b-3-2025-refresh/`)

`run-bootstrap.R`, `verify-vs-pssm2023.R`, `verify-refine2.R`, `verify-user-prep-runs.R`,
`parse-*.R`, `gh/` ticket bodies; research findings `research/input-provenance.md`,
`research/change-list.md`.
