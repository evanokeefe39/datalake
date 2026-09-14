# Enrichment v3 — session kickoff

Copy the block below into a new session.

---

We are **remediating a failed migration** in `C:/Users/evano/repos/datalake`, and I want you
**monitoring for drift from target state**, not just executing tasks.

The Enrichment v3 migration was built, passed its own tests, and never actually ran. All 36 of its
acceptance criteria were checkable without any other component existing, so per-node completeness
was easy and per-edge completeness was invisible. The code is largely correct but unwired, unrun
and unobserved. **A green suite here does not mean the pipeline works — that is the whole lesson.**

Branch `feat/enrichment-v3-phase-1-seam-and-landing`, HEAD `826d654`, tree clean.

## Read these first

1. `docs/postmortem/enrichment-v3/postmortem.md` — what failed and why.
2. `docs/postmortem/enrichment-v3/plans/remediation-plan.md` — **the plan and the authority**.
   Units W0/W-FREEZE/W1–W9, the DAG, §3 blast radius, **§3.0 backup gate**, §6 enforcement plane.
3. `docs/postmortem/enrichment-v3/panel/readmission/SYNTHESIS.md` — latest panel verdict.
4. `docs/architecture/pipelines/enrichment.md` — the **target** architecture (v3).
5. ADRs `docs/architecture/adr/0011-enrichment-layered-model.md`, `0012-dagster-native-orchestration.md`,
   `0013-seam-keeps-no-ledger.md`, `0014-orchestration-dynamics.md` — 0011 statics, 0012/0014 dynamics.
6. `WATCHDOG.md` — project traps; read before touching schema, serving or migrations.
7. `tasks/lessons.md` — mistakes already made this cycle.

## Locked decisions — do not relitigate

- **Gemini batch usage is retired permanently.** Code stays inert. The re-admission story was
  dropped after panel round 2 found it had no trigger, owner, or unit.
- **Cut over; no coexistence window.** The old write path is frozen.
- **`gold_analyses` is retired**, archived before drop, and stays live only until W6 migrates its
  rows and W7 proves parity.
- **Queue tables are cleared to drop** (`batch_jobs`, `batch_items`, `dead_letter`,
  `facets_batch_jobs`, `media_metadata`) — **per-table only, never database-level**.
  `media_cache`, `creators`, `profiles`, `creator_merges`, `prompt_registry` are NEVER dropped.
- **Scraped data is irreplaceable; enrichment is not.** Re-enriching everything is accepted.
  Losing Apify data (bronze Parquet + `data/media` bytes) is not.

## Current state

- **Suite: 57 red** (26 failed + 31 errors, 657 passed) across 16 files — measured. W2 must
  *partition* that by owning unit, not force it green; most is W3/W5 scope.
- **Backup gate §3.0 is PARTIAL.** `state.duckdb`, `ops.sqlite`, `bronze/` copied to
  `~/backups/datalake/2026-09-14-pre-migration/` and read-back verified. **`data/media` (55.36 GB)
  is UNVERIFIED** — believed to be in a GCS bucket, never confirmed. Do not call it satisfied.
- **Zero new-model objects exist** in `data/state.duckdb`. Nothing from the target has ever been
  materialized.

## Your mandate: watch for drift, don't just execute

Drift = the plan quietly ceasing to describe reality. That is what produced the post-mortem.

- **Re-measure before asserting.** Plan numbers are dated snapshots. Re-derive any count before
  using it as a gate; a frozen constant is a hypothesis wearing an assertion's clothes.
- **Characterize from full runs, never named samples.**
- **A guard firing is not a defect.** The hooks and `migration-ships-with-code` fire on legitimate
  work, including documents that merely mention destructive DDL. Confirm the ceremony and proceed;
  never silence a guard by weakening what it guards.
- **State the counterparty** — what each thing reads from, what reads it, and whether it is
  verifiable without a fake on either side. A boundary mock is a declared residual, never coverage.
- **Flag units that can't land alone.** W-FREEZE, for instance, creates the pipeline stall by
  construction unless paired with W4's harvested producer.
- **Report drift as a finding** with file:line or live-DB evidence. If plan and reality disagree,
  say so rather than silently following the plan.

## Open items needing me

1. **The fork decision** — finish vs revert. Plan recommends finish; not yet formally taken.
2. **W1 spike budget** — real paid API calls; needs approval before spend.
3. **Media backup verification** — confirm the GCS bucket holds `data/media`.

Read the seven sources, then tell me the fork decision is open and what you propose for W1.
Do not begin implementing before that.
