# Post-Secondary Supply Model (PSSM)

Forecasts BC's supply of post-secondary-trained workers by credential, program (CIP), region, and occupation, from STP enrolment data and student-outcome survey data (TRD, APPSO, BGS, DACSO).

## Language

**T_Cohorts_Recoded**:
The master standardized cohort table — one row per survey respondent across all four surveys, on a shared column contract (the "skeleton"). Downstream stages key on its columns.
_Avoid_: cohorts table, survey union

**LCIP_CD**:
The canonical six-digit CIP program-code column in T_Cohorts_Recoded. The column *name* is version-stable even when a source survey renames its own column (the 2025 refresh renamed the CSV column to `LCP6_CD`); downstream work always joins on `LCIP_CD`. The CIP taxonomy version of its contents (CIP2016, CIP2021) is a property of the data refresh, tracked per refresh.
_Avoid_: LCP6_CD (source-side name only), CIP code (ambiguous — say 2-digit, 4-digit, or 6-digit)

**Survey cohorts**:
**TRD** — Trades Student Outcomes; **APPSO** — Apprenticeship Student Outcomes; **BGS** — Baccalaureate Graduates Survey; **DACSO** — Diploma/Associate/Certificate Student Outcomes.
_Avoid_: SO data (ambiguous about which survey)

**Model year**:
The hyphenated label (e.g. `2024-2025`) selecting the weight block in T_Weights for a model run. Fixing it is a decision of the annual refresh, not of individual scripts.
_Avoid_: survey year (calendar year of data collection — a different axis)

**New Labour Supply (NLS)**:
A respondent's labour-supply code: 1 = in the supply, 2 = in the supply while studying (kept in the cohort but excluded from 02b-3's NOC denominator), 3 = an NLS-2 record whose stratum has no NLS-1 records (recoded by 02b-2), 0 = not in the supply.
_Avoid_: labour supply (the aggregate distribution)

**StatCan census benchmark**:
The 2021-Census labour-supply distributions built by `R/labour-supply-dists-census-data.R` into the personal schema (`Labour_Supply_Distribution_Stat_Can_r`). Its SURVEY label carries the export vintage ("2021 Census PSSM 2022-2023"); 02b-2 remaps it to the current model year at read time. Downstream code always prefix-matches `2021 Census` on the label — never full-label equality. Census rows key on 2-digit LCP4_CD values and lack TTRAIN/LCIP2_CRED (NA-padded on append).
_Avoid_: StatCan table (ambiguous — same export feeds the occupation benchmark), census label (say export vintage or model-year label)
