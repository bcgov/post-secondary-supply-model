a Mermaid diagram illustrating the CIP matching pipeline:

```mermaid
flowchart TD
    %% ============================================================
    %% Source tables (upstream)
    %% ============================================================
    subgraph SRC["Upstream program-matching sources"]
        DACSO["Credential_Non_Dup_Programs_DACSO_FinalCIPs<br/>7-column composite key"]
        BGS["Credential_Non_Dup_BGS_IDs"]
        GRAD["Credential_Non_Dup_GRAD_IDs"]
        APPSO["Credential_Non_Dup_APPSO_IDs"]
    end

    %% ============================================================
    %% Target table
    %% ============================================================
    TARGET["credential_non_dup<br/>(12 CIP columns added, all empty)"]

    %% ============================================================
    %% Primary source priority chain
    %% ============================================================
    subgraph PRIMARY["Primary sources (priority-ordered, do not overwrite)"]
        S2["Step 2 — DACSO match<br/>7-column composite join<br/>fills all CIP fields"]
        S3["Step 3 — BGS match<br/>record-ID lookup (deduped)<br/>fills 4 final + cluster"]
        S4["Step 4 — GRAD match<br/>record-ID lookup (deduped)<br/>fills 4 final only"]
        S5["Step 5 — APPSO match<br/>record-ID lookup<br/>fills 4 final only"]
        S6["Step 6 — Cluster backfill (GRAD & APPSO)<br/>join 2-digit CIP -> INFOWARE 2-digit<br/>fills cluster code + name"]
    end

    %% ============================================================
    %% Fallback: STP institution CIP
    %% ============================================================
    subgraph FALLBACK["STP fallback (for records still missing 4-digit CIP)"]
        S7["Step 7 — Identify unresolved<br/>group by raw STP CIP + survey stream<br/>persist staging table"]
        S8["Step 8 — Standardise STP CIP<br/>6 chars, no dot -> append trailing 0<br/>still 6 chars -> prepend leading 0<br/>-> XX.XXXX format"]
        subgraph TIER["Step 9 — Progressive INFOWARE matching (4 tiers)"]
            T1["Tier 1: exact 6-digit match<br/>join to INFOWARE_L_CIP_6DIGITS"]
            T2["Tier 2: 5-digit partial match<br/>first 5 chars -> deduped taxonomy"]
            T3["Tier 3: general program mapping<br/>e.g. 52.00 -> 5201"]
            T4["Tier 4: 2-digit fallback<br/>first 2 chars of institution string"]
        end
        S10["Step 10 — Name + cluster enrichment<br/>join INFOWARE 4-digit & 2-digit names<br/>missing 4-digit name -> 'Invalid 4-digit CIP'"]
        S11["Step 11 — Propagate to credential records<br/>join on original CIP + survey stream<br/>update 6 final fields"]
    end

    %% ============================================================
    %% Edge cases + final write
    %% ============================================================
    EDGE["Step 12 — Edge cases<br/>- BGS CIP 99 -> 'Undeclared activity'<br/>- GRAD CIP 99 cluster -> 'Undeclared activity'<br/>- Last resort: copy raw STP CIPs directly"]
    FINAL["Step 13 — Write final credential_non_dup<br/>drop staging table"]

    %% ============================================================
    %% Flow
    %% ============================================================
    DACSO --> S2
    BGS   --> S3
    GRAD  --> S4
    APPSO --> S5

    TARGET --> S2
    S2 -- "unmatched" --> S3
    S3 -- "unmatched" --> S4
    S4 -- "unmatched" --> S5
    S5 --> S6

    S6 -- "still null 4-digit CIP" --> S7
    S7 --> S8
    S8 --> T1
    T1 -- "still missing" --> T2
    T2 -- "still missing" --> T3
    T3 -- "still missing" --> T4
    T4 --> S10
    S10 --> S11
    S11 --> EDGE
    EDGE --> FINAL
    FINAL -- "downstream" --> DOWN["Cohort creation<br/>Near-completer analysis<br/>PTIB analysis<br/>Program projections"]

    %% ============================================================
    %% Styling
    %% ============================================================
    classDef source fill:#e3f2fd,stroke:#1565c0,color:#0d47a1
    classDef primary fill:#fff3e0,stroke:#ef6c00,color:#4e342e
    classDef fallback fill:#f3e5f5,stroke:#6a1b9a,color:#4a148c
    classDef edge fill:#ffebee,stroke:#c62828,color:#b71c1c
    classDef target fill:#e8f5e9,stroke:#2e7d32,color:#1b5e20

    class DACSO,BGS,GRAD,APPSO source
    class S2,S3,S4,S5,S6 primary
    class S7,S8,T1,T2,T3,T4,S10,S11 fallback
    class EDGE,FINAL edge
    class TARGET,DOWN target
```

### How to read the diagram

**Left-to-right flow** mirrors the 13 steps in the doc:

1. **Sources (top-left)** — Four upstream tables enter the merge, ordered by precision: DACSO (7-column composite key) is most precise, then BGS / GRAD / APPSO (record-ID lookups).
2. **Primary sources (Steps 2–6)** — Each step only fills records still missing a 4-digit CIP. DACSO populates the entire CIP family in one shot; BGS, GRAD, APPSO each contribute progressively. Step 6 backfills the cluster code for GRAD/APPSO from the INFOWARE 2-digit table.
3. **STP fallback (Steps 7–11)** — For records still unresolved, the institution-reported CIP is cleaned (Step 8) and then matched against INFOWARE through a four-tier waterfall: 6-digit → 5-digit → general-program → 2-digit.
4. **Edge cases & final write (Steps 12–13)** — CIP `99` / "Undeclared activity" labels, a last-resort raw-STP copy, and the overwrite of `credential_non_dup`.

The arrows from each step to the next are labelled with the gate condition (e.g. "unmatched", "still null 4-digit CIP"), so reviewers can see where each step cedes control to the next lower-priority source.