# 02b-2 PSSM Cohorts New Labour Supply — 2025 Refresh Validation Note

Branch `R/02b-2-pssm-cohort-new-labour-supply`; validates the 2025 annual-refresh changes to
`R/02b-2-pssm-cohorts-new-labour-supply.R`. Wayfinder map: bcgov/post-secondary-supply-model#142
(tickets #143–#150; change list signed off at #145).

## What changed

Three commits on top of the 02b-1 branch tip (`c9ff41e`), `02b-2-pssm-cohorts-new-labour-supply.R`
plus root `CONTEXT.md`:

- `58ca4ae` **docs** — comment-only retrofit (OCCSN framing, input provenance, NLS 2→3 recode
  rationale, 4-stage weighting template + calibration identity, 12/8-table naming legend, StatCan
  append NA-padding contract, PDEG/law BACH-22 proxy rationale). Header FIXMEs resolved as
  documentation. Zero code change.
- `ed247f2` **fix** — signed-off change list: model window `years <- c(2021:2025)` now actually
  referenced by the invalid-NOC diagnostic (`as.numeric(SURVEY_YEAR) %in% years` — the old
  quoted-string list never matched the numeric column and silently returned nothing); `rm()`
  trimmed of four SQL-only intermediates that never exist in R (hard error every prior run);
  `tables_to_keep` trimmed from 12 to the 6 objects the script creates/modifies (standalone
  `base::get` error + stale loader copies); StatCan read guarded (02b-1 pattern) preferring the
  `_r` table the census prep writes with fallback to the prep-for-fresh-run no-suffix name; label
  remap target updated to the current model year `2021 Census PSSM 2024-2025`; unused
  `library(RODBC)` and the DEVELOPMENT-ONLY banner removed. CONTEXT.md gains a *StatCan census
  benchmark* entry (label vintage/remap contract, 2-digit keys, NA-padding).
- No changes to `R/labour-supply-dists-census-data.R` — its re-run needed only bootstrap-side
  staging (below).

## StatCan sourcing (#150, decision at #145)

The census benchmark table was absent from PSSM2025 entirely. The current-cycle LAN export
(`27-0001 .../data/statcan/stat-can-data-export-for-labour-supply-distributions.xlsx`) is
**byte-identical (MD5) to last cycle's**, so re-running the prep script was chosen (exercises the
pipeline) over carrying PSSM2023's table forward. Staging (`.scratch/02b-2-2025-refresh/census-prep-run.R`):
copied `dbo.tbl_age_groups_rollup_r` into my_schema (the standalone path never ran the loaders, so
the table the prep script joins was missing) and dropped two temp-table leftovers from an earlier
partial attempt. Result: `IDIR\JDUAN.Labour_Supply_Distribution_Stat_Can_r` — **864 rows,
identical to `PSSM2023.dbo.Labour_Supply_Distribution_Stat_Can` on every column (max abs diff 0)**,
as the identical input predicts.

## Run

Sourced via a read-only bootstrap (`.scratch/02b-2-2025-refresh/run-bootstrap.R`) loading the dbo
lookups + `my_schema.t_cohorts_recoded_r` (02b-1's output, 313,036 rows); the script's guarded
StatCan read pulled `Labour_Supply_Distribution_Stat_Can_r` itself. Exit 0. `WEIGHT_NLS` populated
for 194,052/313,036 records. Written to `my_schema`: `labour_supply_distribution_r` 6,631 ·
`_no_tt_r` 5,751 · `_lcp2_r` 2,617 · `_lcp2_no_tt_r` 2,577 · `tmp_tbl_weights_nls_r` 19,080 ·
`t_cohorts_recoded_r` 313,036 (with WEIGHT_NLS).

## Verification vs PSSM2023

Baseline: `IDIR\JDUAN.*_r` tables on PSSM2023 (last cycle's R-run write-backs). **Unlike 02b-1,
exact cell matches are not expected for Student Outcomes rows**: the 5-year window slid
2019–2023 → 2021–2025, so weighted TOTALs and New_Labour_Supply proportions legitimately shift.
Verification scripts: `.scratch/02b-2-2025-refresh/verify-vs-pssm2023.R`, `verify-refine.R`.

| Check | Result |
|---|---|
| Structural parity (column sets, incl. deliberate LCP2_CRED on `_lcp2` tables) | identical ✓ |
| Census rows (840 = 840): keys, COUNT, TOTAL, New_Labour_Supply after label normalization | **exact match** ✓ |
| PDEG/law filter (census PDEG-07 removal) | same 24 rows removed as baseline ✓ |
| Calibration identity Σ resp × WEIGHT_NLS = BASE_TOTAL (4,742 respondent strata) | max abs err 2.3e-13 ✓ |
| Zero-respondent strata (565; WEIGHTED_TOTAL = 0 → weight 0 by design) | 1,094 records, z09 surface ✓ |
| SO-row closeness on shared strata, TOTAL ≥ 100 (n = 2,578) | median &#124;ΔNLS&#124; 0.012, p95 0.102, r 0.964 ✓ |
| SO-row closeness, all shared strata (n = 4,678) | r 0.882; larger drift confined to small strata |
| Model window in `t_cohorts_recoded_r` | exactly 2021–2025, no stale years ✓ |
| Row-count growth vs baseline (6,631|6,478 etc.) | tracks the larger 2021–2025 cohort ✓ |

## CIP-version audit (02b-1 breadcrumb, closed)

Census rows carry 2-digit prefixes {01–08, 11} over graduate credentials (DOCT, GRCT/GRDP, MAST,
PDEG) — the same series set as PSSM2023 (byte-identical table). The SO side now carries 37
CIP2021 prefixes; {02, 06, 07, 08} appear census-only because those series have no
undergraduate/trades SO population — **unchanged vs the baseline**, and 02b-2 never joins these
keys. The genuine seam — census rows remain CIP2016-classified (per the StatCan export .msg
titles) while the STP/graduate side moved to CIP2021 — is inherited structural behavior that
becomes live where Module 07 joins the graduate-credential census rows against CIP2021-keyed
projections. **Breadcrumb for the 06/07 verification effort**, not an 02b-2 break.

## Known downstream impacts on 02b-3 (breadcrumbs for the next effort)

- **Occupation-side StatCan table missing on PSSM2025** — `Occupation_Distributions_Stat_Can`(_r)
  absent from `IDIR\JDUAN`, `dbo` (live-checked). 02b-3 will need its own prep re-run
  (`R/occ-dists-census-data.R`, line ~299 hardcodes the same `2021 Census PSSM 2022-2023` label);
  the LAN lookups + `tbl_age_groups_rollup_r` staging pattern from #150 applies directly.
- **Census label contract** — 02b-2 now emits `2021 Census PSSM 2024-2025`. 02b-3's duplicated
  remap (lines 68–69) and PDEG-append hardcode (line 1051, still `2023-2024`) must be updated to
  match (decision #145; consumers prefix-match, so the mismatch is cosmetic-but-confusing until
  aligned).
- 02b-3 consumes `t_cohorts_recoded` (with WEIGHT_NLS) and `tmp_tbl_weights_nls` in-session when
  chained, or from `my_schema.*_r` standalone — both freshly written this cycle.
- Join-type drift: none surfaced in 02b-2 (all keys verified type-compatible before the run), but
  the class remains live for 02b-3's own NOC joins.
- Deferred/ documented in-script: PDEG `distinct(TOTAL)` low-risk undercount note; no
  `dbDisconnect(con)` at end of 02b-2 (benign in the chained run).

## Assets (uncommitted, `.scratch/02b-2-2025-refresh/`)

`census-prep-run.R`, `run-bootstrap.R`, `verify-vs-pssm2023.R`, `verify-refine.R`,
`parse-check*.R`, `prep-precheck*.R`, `probe-*.R`; research findings
`research/input-provenance.md`, `research/change-list.md`.
