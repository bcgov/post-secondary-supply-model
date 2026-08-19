# 02b-1 PSSM Cohorts — 2025 Refresh Validation Note

Branch `R/02b-1-pssm-cohorts-2025`; validates the 2025 annual-refresh changes to
`R/02b-1-pssm-cohorts.R`. Wayfinder map: bcgov/post-secondary-supply-model#133.

## What changed

Three commits, `02b-1-pssm-cohorts.R` only (plus root `CONTEXT.md`):

- `6cbe34d` **docs** — comment-only retrofit (OCCSN framing, input provenance, rbind contract, per-branch pipelines, per-survey NLS semantics). Zero code change.
- `1449ce3` **fix** — signed-off change list: TRD+APPSO `LCIP_CD = LCP6_CD` (source column renamed to CIP2021 6-digit; canonical output name kept); guarded run-flag defaults; guarded `dbReadTable` of `t_bgs_data_final_for_outcomesmatching_r` from `my_schema`.
- `c5bf11d` **fix** — run-surfaced BGS join: `STQU_ID` was `<character>` on the BGS side, `<numeric>` on the outcomes-match side; coerced to character.

## Run

Sourced after a read-only bootstrap of the fresh `dbo` `*_r` tables (`.scratch/02b-1-2025-refresh/run-bootstrap.R`). Exit 0. Outputs written to `my_schema`:

- `t_cohorts_recoded_r` — **313,036 rows**
- `t_dacso_data_part_1_r` — 144,352 rows

## Row-count verification vs PSSM2023

Baseline: the personal-schema (`my_schema`) `t_cohorts_recoded_r` on PSSM2023 (last cycle's 02b-1 output). Counts by SURVEY × SURVEY_YEAR. Overlap years 2021–2023 are the test — they should be unchanged.

| Survey | 2021 | 2022 | 2023 | 2024 | 2025 |
|---|---|---|---|---|---|
| APPSO | 4982 ✓ | 5128 ✓ | 4187 ✓ | 4454 | 4415 |
| BGS | 23860 ✓ | 24752 ✓ | 25962 ✓ | 24572 | 24306 |
| DACSO | 26952 ✓ | 30575 ✓ | 28541 ✓ | 27949 | 30335 |
| TRD | 4574 ✓ | 3866 ✓ | 4359 ✓ | 4504 | 4763 |

✓ = identical to PSSM2023. Every overlap-year cell matches exactly; 2024/2025 newly populated; 2019/2020 retired (the 5-year window slid 2019–2023 → 2021–2025). This is precisely the expected shape — the refresh added two cycles and dropped two, touching nothing in the retained overlap.

## CIP2021 downstream-impact audit (light — catalogue only)

After the `LCIP_CD = LCP6_CD` fix, `LCIP_CD` in `T_Cohorts_Recoded` holds CIP2021 6-digit codes where it held CIP2016 before. Swept `LCIP_CD` / `LCP4_CD` / `LCP2_CD` / `CIP_CODE_4` consumers across 01c-, 02b-2, 02b-3, 04-, 06-.

- **`LCIP_CD` (6-digit) is NOT referenced by any downstream script.** Confirmed by grep across all consumers. So the column carrying the fixed value poses **no downstream join or comparison risk** — the content shift is inert for `LCIP_CD` specifically.
- Downstream keys on **`LCP4_CD`** (4-digit, from `LCIP_LCP4_CD` — name unchanged) and **`LCP2_CD`** (its 2-digit prefix). These are the stratum/join keys for `P(CIP|cred,age)`. Their *values* are CIP2021-derived this cycle across the whole pipeline (the upstream loaders and 02a already moved to CIP2021), so consumers see a consistent CIP2021 world, not a mix. No per-script recode is implied by 02b-1's change.
- **Risk sites to keep in view (opaque-key vs versioned-join)**: all catalogued consumers treat `LCP4_CD`/`LCP2_CD` as **opaque keys** (group-by, string-prefix, paste into `LCIP4_CRED`/`LCIP2_CRED`) — none join them against a CIP2016-versioned reference table within these scripts. Full verification that no *upstream* reference table lags at CIP2016 belongs to the 02b-2 effort, not here.

## Known downstream impacts on 02b-2 (breadcrumbs for the next effort)

Not fixed here — flagged for the 02b-2 branch:

- **Stale year window** — `02b-2:49` `years <- c(2019, 2020, 2021, 2022, 2023)` and `02b-2:145` `SURVEY_YEAR %in% c('2019','2020','2021','2022','2023')` still name the old window. 02b-1's output now spans 2021–2025; 02b-2 will silently drop 2024/2025 and filter to empty 2019/2020 until these are updated.
- **StatCan survey-label remap** — `02b-2:78-79` hardcodes `"2021 Census PSSM 2022-2023" → "2021 Census PSSM 2023-2024"`; confirm the label matches this cycle's `Labour_Supply_Distribution_Stat_Can`.
- **StatCan table schema** — `02b-2:71-73` reads `Labour_Supply_Distribution_Stat_Can` from `my_schema`; ensure that table is present/refreshed there for this run.
- **Join-type drift** — the BGS `STQU_ID` break (character vs numeric on DB round-trip) is a class the column-existence research doesn't catch. 02b-2 reloads `t_cohorts_recoded` from `my_schema` and does its own joins — expect and check for the same.

## Assets (uncommitted, `.scratch/02b-1-2025-refresh/`)

`run-bootstrap.R`, `baseline-counts.R`, `newrun-counts.R`, `stqu-type-check.R`; research findings `research/change-list.md`, `research/input-provenance.md`.
