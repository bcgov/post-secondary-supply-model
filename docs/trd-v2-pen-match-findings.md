# TRD v2 PEN-Match Bridge — Findings (Ticket 16)

> Findings from the first full run of `R/02a-trd-program-matching-v2.R`
> (2026-08-16), against the materialized CIP2021 / 2024–25-cycle data.
> Companion to `docs/bgs-v2-program-matching-findings.md` and
> `docs/dacso-v2-program-matching-findings.md`. Unlike those, this is the
> FIRST dedicated TRD matcher — there is no frozen original; the baseline is
> the frozen 02b-1 TRD block's survey-side-only behaviour, which this
> script's fallback path preserves exactly.

---

## 1. Why TRD needed a bridge at all

The TRD (Trades) cohort enters PSSM's P(CIP | cred, age) through 02b-1's TRD
block, which builds `LCIP4_CRED` cohort keys from the **survey-side
`LCIP_LCP4_CD` exclusively**. No STP-side reconciliation exists — and none
was possible under the original design assumption, because
`credential_non_dup_r` carries no 'TRD' bucket at all (upstream in 01c,
trades foundations/completion credentials are coded **DACSO**).

The ticket-02 fact-find reframed what a TRD matcher could be:

| Fact-find (2026-08-14) | Confirmed on refreshed data (this run) |
|---|---|
| 91.6% of TRD survey PENs match STP credentials; DACSO dominates (20,032 PENs) | 26,116 survey-credential pairs into the DACSO bucket; 21,879 of 22,066 survey rows carry a canonical PEN (0.85% null — matches) |
| Award-year lag peaks sharply at −2 (survey 2 school years after award) — same pattern as BGS | **Peak at −2, re-confirmed**; the window assertion passed |
| Raw matched-pair CIP4 agreement 85.4% (many-to-many, unfiltered) | 98.8% after the window + institution filter (see §4 — the jump is the design working) |
| 951 PENs (4.4%) have no STP credential at all | 1,092 distinct PENs (5.2%) with no DACSO-bucket credential — the fact-find counted across all buckets, so DACSO-only is expected slightly higher |

## 2. The bridge design (as built)

```
trd_data_r ──PEN (normalize_pen)──► DACSO-bucket credentials
                    │
                    ├─ award-year window: lag ∈ [−3, −1], centred on the −2 peak
                    │  (distribution regenerated + asserted every run)
                    ├─ institution agreement: INST == PSI_CODE (exact) or an
                    │  alias pair from dbo.T_BGS_STP_INST_ALIAS_v2_r
                    └─ preference: lag −2 first, then latest award year
                                       │
                    one credential per survey row ──► FINAL_CIP =
                        DACSO-v2 XWALK-reconciled CIP of the matched program
                        (falls back to the spine's cleaned STP CIP)
                    no survivor ──► survey-side LCIP_LCP4_CD as FINAL_CIP
                        (exactly the frozen 02b-1 behaviour)
```

Output: `Credential_Non_Dup_TRD_IDs_v2_r` — one row per TRD survey record
(KEY), with the matched credential's ID/PSI/award-year, the chosen FINAL CIP,
its source, and a `MATCH_RULE` trace. The frozen 02b-1 block is untouched;
reading this table there is future-cutover work.

## 3. First-run results (2026-08-16)

| Stage | Result |
|---|---|
| Survey rows / canonical PENs | 22,066 / 21,879 |
| DACSO credentials with canonical PEN | 613,502 |
| PEN pairs (many-to-many) | 26,116 |
| Lag peak (asserted in-window) | **−2** |
| Matched after window + institution filter | **20,589 (93.3%)** |
| — exact institution | 20,589 (100%; **0 alias matches needed**) |
| — at lag −2 | 16,243 |
| — at lag −3 / −1 | 4,346 |
| Survey-side fallback | 1,477 (no-PEN 187 · no-credential 1,118 · no-window-match 172) |
| FINAL CIP source among matched | 100% `dacso_v2_final` — the DACSO v2 XWALK reconciliation covered every matched credential's program |

MATCH_RULE distribution:

| MATCH_RULE | Rows |
|---|---:|
| `pen_match_lag2_exact_inst` | 16,243 |
| `pen_match_window_exact_inst` | 4,346 |
| `survey_fallback_no_credential` | 1,118 |
| `survey_fallback_no_pen` | 187 |
| `survey_fallback_no_window_match` | 172 |

## 4. The headline finding: 98.8% agreement — why it beats the fact-find's 85.4%

The fact-find's 85.4% CIP4 agreement was measured on **raw many-to-many
pairs** — every credential sharing the PEN, including prior awards at lags
≤ −4 (~1,500 pairs of noise) and anomalies. Those pairs compare the survey's
trades program against *the wrong credential* (an older diploma, say), so
they disagree.

The bridge's window (−3..−1) plus institution agreement isolates **the
credential being surveyed** — and on those pairs agreement is
**20,334/20,589 = 98.8%**. In other words: when you identify the RIGHT
credential, the STP side and the TRD survey almost never disagree on the
field of study. The trades cohort's survey-side CIP was never in conflict
with the credential side; it just had no credential side to be compared
against. This validates the reframed hybrid at a stroke — and suggests the
future 02b-1 cutover can draw TRD CIPs from either side with near-equal
confidence where the bridge matches.

## 5. Corroborating evidence: the survey's own PRGM_ID

All 22,066 survey rows carry `PRGM_ID` (the registry program id the survey
itself records) — it is kept on the output as evidence, though the bridge is
PEN-based per the ticket-09 design. A post-run probe joined matched
credentials to the DACSO v2 XWALK by business keys and compared PRGM_IDs:

- 3,483 of the fan-out-linked rows reach an XWALK PRGM_ID;
- **56.4% agree** with the survey's PRGM_ID.

Moderate agreement, and expected in shape: the survey records the *trades*
program while the credential may record a foundations or different-level
program at the same institution — same student, same institution, different
program identity. Not a gate (the design's validation was the fact-find
numbers, both reproduced), but a useful reference point for the harness
(ticket 17) and any future PRGM_ID-based tightening.

## 6. Analyst notes and next steps

- **Institution aliases turned out unnecessary**: TRD `INST` codes align
  with STP `PSI_CODE` directly (0 alias matches in 20,589). The alias table
  is still joined — if a future cycle introduces a mismatch, its pairs start
  applying with no code change.
- **The window is self-calibrating**: the lag distribution is recomputed and
  exported (`diagnostics/trd-v2-lag-distribution.csv`) and the script
  asserts its peak stays inside [−3, −1]. If a future survey cycle shifts
  cadence, the run stops loudly instead of matching on a stale window.
- **1,477 survey-side-fallback rows** are the trades respondents with no
  reconcilable credential — the same population the current 02b-1 handles
  for every row. The bridge changes nothing for them.
- Ticket 17's comparison harness consumes this table alongside the BGS/DACSO
  v2 outputs; the end-to-end gate (02b-1 `T_Cohorts_Recoded` within float
  tolerance) is defined in the ticket-04 metric.

## 7. The v2 family, complete but for the harness

With this build the v2 family covers every in-scope survey:

| Survey | v2 script | Per-PEN vs v1 (ticket-04 metric) |
|---|---|---|
| BGS | `02a-bgs-program-matching-v2.R` | 14.57% vs 13.02% — pending curation of 299 exported combos |
| DACSO | `02a-dacso-program-matching-v2.R` | **11.03% vs 12.89% (−1.86pp, immediate win)** |
| TRD | `02a-trd-program-matching-v2.R` | no v1 exists; 93.3% now credential-backed at 98.8% agreement |
| APPSO / GRAD | out of scope (STP-direct; row-identical by design) | — |

Ticket 17 (comparison harness) formalizes these comparisons and tracks them
across runs.
