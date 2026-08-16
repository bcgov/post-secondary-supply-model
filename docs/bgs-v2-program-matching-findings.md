# BGS v2 Program Matching — Findings (Ticket 14)

> Findings from the first full run of `R/02a-bgs-program-matching-v2.R`
> (2026-08-16), against the materialized CIP2021 / 2024–25-cycle data.
> Companion to `docs/technical-documentation-02a-bgs-program-matching.qmd`
> (the frozen original) and `docs/project-summary-for-new-analyst.md` (the
> model context). The comparison harness (wayfinder ticket 17) will restate
> these numbers formally; this doc records what the first run showed and what
> analysts need to do next.

---

## 1. What BGS v2 is — and why it exists beside the original

The BGS matcher decides, for every Baccalaureate Graduate Survey respondent
who PEN-matches an STP credential, whether the FINAL 4-digit CIP comes from
the survey or the credential record. Those decisions feed
P(CIP | cred, age) — the second term of the PSSM formula — from two
directions:

```
credential side → Credential_Non_Dup_BGS_IDs_v2_r → 02a-update-cred-non-dup
                   (priority step 2: DACSO > BGS > GRAD > APPSO > STP)
                 → credential_non_dup_r → 02b-1 LCIP4_CRED cohort key

survey side     → T_BGS_Data_Final_for_OutcomesMatching_v2_r
                 → 02b-1's BGS block (coalesced over the survey's own CIP)
```

The frozen original (`R/02a-bgs-program-matching.R`) works, but encodes three
kinds of analyst knowledge **as code**: institution aliases in a `MATCH_INST`
`case_when`, custom CIP choices inline (two rules + a 17-row override
tribble), and a manual-review CSV round-trip whose curated file dates from
the 2023 cycle. Each cycle, an analyst had to re-eyeball unmatched
institution pairs and re-apply stale manual decisions.

The v2 script (part of the parallel v2 family from the cip-matching effort)
keeps the frozen original as the baseline anchor and moves that knowledge
into **data**:

| Knowledge | Original (frozen) | v2 |
|---|---|---|
| Institution aliases | hardcoded `case_when` (~L942) | `dbo.T_BGS_STP_INST_ALIAS_v2_r` — joined, seeded verbatim (35 pairs) |
| Custom CIP choices | 2 inline rules + 16-row 4B.5 override tribble | `dbo.T_BGS_CUSTOM_CIP_CHOICES_v2_r` — 2 `cip_pair_rule` + 16 `program_override` rows |
| Borderline cases | 2023-era LAN CSV applied silently | exported for fresh curation; defaulted to STP meanwhile, with a `MATCH_RULE` trace |
| STP CIP cleaning | per-survey cascade (5-char partials promote to CIP4) | shared spine (`R/02a-cip-normalize.R`) — stricter: 5-char partials fill CIP2 only |
| PEN joining | raw string match | `normalize_pen()` canonicalization on both sides |
| Audit trail | none | `MATCH_RULE` decision trace on every assignment |

Both seed tables are written **only when absent** — after the first run they
are analyst-maintained data and the script never clobbers them.

---

## 2. The MATCH_RULE vocabulary (the decision trace)

Every assignment in v2 carries a `MATCH_RULE` column naming the rule that
made it. This is the audit trail the comparison harness (ticket 17)
consumes, and the fastest way to answer "why does this row have this CIP?".

| MATCH_RULE | Meaning |
|---|---|
| `exact_4digit_inst_year` | inst + award-year + 4-digit CIP all agree (BGS == STP) |
| `general_program_bgs_generic` | BGS used a "general" CIP → STP's specific code preferred |
| `general_program_stp_generic` | STP used a "general" CIP → BGS's specific code preferred |
| `xval_stp_4digit_evidence` | STP coding validated by a 4-digit agreement elsewhere |
| `xval_bgs_4digit_evidence` | BGS coding validated by a 4-digit agreement elsewhere |
| `custom_cip_choice` | override from `T_BGS_CUSTOM_CIP_CHOICES_v2_r` (`cip_pair_rule`) |
| `default_stp_double_majors` | residual 2-digit matches default to STP |
| `no_cip_match_default_stp` | inst + year matched but CIPs disagree → STP (the old manual-review caseload) |
| `no_inst_year_match` | PEN matched but inst/year flags failed (survey side stays NA) |
| `program_override_lookup` | credential-row override (`program_override` rows; 4B.5 semantics) |
| `stp_fallback_no_match` | credential never PEN-matched a survey row → its own spine-cleaned CIP |

---

## 3. First-run results (2026-08-16, CIP2021 / 2024–25 cycles)

Inputs: the ticket-11 materialized 02a tables + the ticket-13 spine cleaning
table (`Credential_Non_Dup_STP_CIP_Cleaning_v2_r`). One detail that matters
for reruns: the materialized survey table carries the **original's Part-5A
decision columns** (`STP_CIP_CODE_4*`, `FINAL_*`, `USE_STP_CIP`) — v2 drops
those on read so its decisions are derived fresh.

| Stage | Result |
|---|---|
| Credential side | 524,787 BGS credentials; 445,301 with a canonical PEN; **37 without a spine-resolved CIP4** (the stricter no-5-char-promotion rule — visible in the trace, not silent) |
| Survey side | 146,460 rows |
| PEN join | 137,421 case rows (STQU_ID × ID unique, as in the original) |
| Flags | MATCH_INST 131,268 · award-year 124,748 · ALL3_CIP4 108,361 · ALL3_CIP2 118,132 |
| Decision points (t1/t2) | 1,676 program combinations |
| Residual borderline | 6,074 rows / **299 program combinations** |

Decision-tree rule firing on the 1,676 points (rule order matters; later
rules fired zero times on the refreshed data):

| Rule | CIP_TO_USE | Points |
|---|---|---:|
| `general_program_bgs_generic` | STP | 1,107 |
| `general_program_stp_generic` | BGS | 136 |
| `xval_stp_4digit_evidence` | STP | 433 |
| `xval_bgs` / `custom_cip_choice` / `default_stp` | — | 0 |

Credential-side output MATCH_RULE distribution (one row per ID):

| MATCH_RULE | Rows |
|---|---:|
| `stp_fallback_no_match` | 398,176 |
| `exact_4digit_inst_year` | 108,140 |
| `general_program_bgs_generic` | 6,708 |
| `no_cip_match_default_stp` | 6,028 |
| `program_override_lookup` | 2,694 |
| `xval_stp_4digit_evidence` | 2,495 |
| `general_program_stp_generic` | 546 |

Outputs written (analyst personal schema, fresh `_v2_r` names coexisting
with the originals):

- `BGS_Matching_STP_Credential_PEN_v2_r` — 137,421 rows (the audit table)
- `Credential_Non_Dup_BGS_IDs_v2_r` — 524,787 rows (524,750 with FINAL CIP;
  the 37 without are the spine-unresolved rows above)
- `T_BGS_Data_Final_for_OutcomesMatching_v2_r` — 146,460 rows (123,722 with
  FINAL CIP)

---

## 4. v1-vs-v2: what the first run showed

The headline finding, stated plainly:

> **The deterministic engine is at parity with the original. The only
> differences are exactly the rows where the original silently applied
> 2023-era manual decisions.**

ID-level comparison over 524,789 paired credential IDs:

| Measure | Value |
|---|---|
| FINAL_CIP_CODE_4 identical | 522,593 (99.58%) |
| Differing | 2,159 (0.41%) |
| Differing rows' MATCH_RULE | **100% `no_cip_match_default_stp`** |

Zero flips in `exact_4digit`, `general_program`, `xval`, override, or
fallback rules. The engine ports faithfully; the difference is policy, not
logic.

Per-PEN CIP4 disagreement against the survey (the validation metric defined
in wayfinder ticket 04, modal-dedup denominator):

| Table | Pairs | Disagree | Rate |
|---|---:|---:|---:|
| v1 `Credential_Non_Dup_BGS_IDs_r` | 103,356 | 13,454 | **13.02%** |
| v2 `Credential_Non_Dup_BGS_IDs_v2_r` | 103,356 | 15,060 | **14.57%** |

v2 is currently 1.55pp **higher** — and the entire gap is the un-curated
borderline caseload: the original's Part 3C applied the analyst-curated 2023
LAN file (`BGS_Matching_STP_Cdtl_Check_MatchInstAwardYearOnly_ProgramCombos.csv`)
to those rows, choosing BGS CIPs in ~300 program combinations; v2 defaults
them to STP until those combinations are re-curated into
`T_BGS_CUSTOM_CIP_CHOICES_v2_r`. Once curated, v2 should reconverge to ≤ the
original's rate (the deterministic rules already agree everywhere else).

**Practical reading**: v2 trades 1.55pp of temporarily-borrowed 2023
judgement for a transparent, per-row-traced decision pipeline. The gap is a
work-list, not a regression.

---

## 5. Analyst work-list #1: the residual borderline caseload

`no_cip_match_default_stp` rows — 299 program combinations, 6,074 records —
are exported on every run to
`.scratch/cip-matching/diagnostics/bgs-v2-residual-manual-review.csv`
(regenerated by the script; `.scratch/` is local-only and gitignored).
Columns are the review essentials: institution, years, both program codes and
descriptions, both 4-digit CIPs and names.

Typical entries are genuine judgment calls, e.g.:

| BGS program / CIP | STP program / CIP |
|---|---|
| Bachelor of Arts (2401 Liberal arts) | Bachelor Of Arts Major In Sociology (4511) |
| BSc Mathematics (2701) | Bed, Stem (1301 Education, general) |

To act on them: add rows to `dbo.T_BGS_CUSTOM_CIP_CHOICES_v2_r` —
`CHOICE_TYPE = "cip_pair_rule"` (fires on BGS-CIP × STP-CIP at the decision
point level) or `"program_override"` (fires on the credential program
description) — then rerun the v2 script. Seeded examples show the exact
shape.

---

## 6. Analyst work-list #2: alias proposals (and a confound)

The alias-inference engine (ticket 05) counts PEN co-occurrence between
BGS `INSTITUTION_CODE` and STP `PSI_CODE` on rows where `MATCH_INST` is NA:
235 unmatched pairs, 82 with pair-count ≥ 10, exported to
`.scratch/cip-matching/diagnostics/bgs-v2-alias-proposals.csv`.
Proposals are **never auto-applied** — an analyst confirms pairs by adding
rows to `dbo.T_BGS_STP_INST_ALIAS_v2_r`.

**Confound discovered in the first run** (noted for the ticket-17 harness):
the highest-count proposals are `UBCV↔SFU`, `UBCV↔UVIC`, `UBCV↔BCIT`… — those
are **multi-institution students** (transfer/second-degree), not aliases.
A true alias is the *same institution* carrying two codes (e.g. `MALA→VIU`
after a rename). Reading the report: treat cross-institution pairs as noise
unless the pair pattern says otherwise; a refined proposal engine
(same-campus evidence, or frequency *relative to* each institution's overall
volume) is a harness-ticket refinement, not a blocker.

---

## 7. Conventions retained deliberately

- **Award-year lag table**: the explicit survey-year → award-year window
  (2000–2025) is ported verbatim, keeping the annual-manual-edit convention
  (an explicit out-of-scope decision on the cip-matching map — the pattern is
  `Y → (Y-3)/(Y-2) and (Y-2)/(Y-1)`, so extending it each cycle is trivial).
- **Priority chain unchanged**: v2's credential output slots into
  `02a-update-cred-non-dup`'s DACSO > BGS > GRAD > APPSO > STP order
  unchanged; the original's update script reads whichever BGS table it is
  pointed at.
- **Processing in-memory**: at ~525K rows the v2 script collects early and
  uses local dplyr throughout — local joins match NA-to-NA, which is exactly
  the semantics the original had to restore on SQL Server with
  `na_matches = "na"`.

---

## 8. Next steps

1. Curate the 299 borderline combinations into
   `T_BGS_CUSTOM_CIP_CHOICES_v2_r` and rerun — expect the per-PEN gap to
   close (§4).
2. Confirm genuine alias pairs into `T_BGS_STP_INST_ALIAS_v2_r` (§6).
3. Tickets 15 (DACSO v2) / 16 (TRD v2) / 17 (comparison harness) build the
   rest of the v2 family; the harness restates §4's comparison formally and
   tracks it across runs.
