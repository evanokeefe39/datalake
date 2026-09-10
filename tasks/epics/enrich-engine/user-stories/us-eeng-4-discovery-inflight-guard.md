---
id: US-EENG-4
epic: E-ENRICH-ENGINE
persona: P1
status: Open
---
# US-EENG-4 — Discovery derives in-flight state from Dagster, never double-submits

- **Epic:** E-ENRICH-ENGINE
- **Status:** Open
- **Relates to:** US-EENG-1 (the submit path it guards), ADR-0012 (Dagster-native
  orchestration), ADR-0013 (seam keeps no ledger)
- **Source:** `tasks/plans/enrichment-v3-migration-master.md` §2b (gap table) and
  **Phase 2** (not Phase 5 — see sequencing below)
- **Migration phase:** **Phase 2 — Dagster-native orchestration.** The master
  plan is explicit: "⚠ The discovery replacement is part of THIS phase, not a
  later one." Deferring to Phase 5 would leave a window where the pipeline
  double-submits; this is a correctness ordering, not a tidiness preference.

## The defect (named code)

The discovery drain `ig_posts_gen_batches`
(`src/datalake/defs/instagram/assets.py:1041`) protects itself against
re-enqueueing in-flight work with a guard built on the retired queue:

- `assets.py:1140-1146` builds `open_ids` from
  `SELECT payload FROM batch_items WHERE status IN ('pending','processing')`
  (against `ops.sqlite`);
- `assets.py:1149` filters the candidate list against `open_ids`.

**When `batch_items` dies (ADR-0012 retires it in Phase 2), this guard dies
with it.** Without a replacement, the drain re-enqueues work already in flight
— a double-submit that re-pays the model for work already billed, exactly the
failure ADR-0011's economics exist to prevent.

## Story

**As a** pipeline operator, **I want** the discovery drain to derive its
in-flight set from the Dagster instance
(`get_materialized_partitions(submitted) − get_materialized_partitions(harvested)`,
per ADR-0013) instead of the retired `batch_items` table, **so that** retiring
the queue never creates a window where in-flight work is double-submitted and
double-billed.

## Settled contract (2026-09-10)

- **ADR-0013 — no ledger.** There is NO `external_jobs` table and no
  pipeline-owned ledger; the service owns its job store and Dagster polls it
  over HTTP. The in-flight set is instance-derived:
  `materialized(submitted) ∖ materialized(harvested)`.
- **ADR-0012 decision 2 — the sensor is an interval `@sensor`, never an
  `@asset_sensor`.** An asset sensor fires only on a NEW materialization
  event, so a job not terminal at that instant would never be re-checked and
  would hang forever. An interval sensor re-derives the FULL in-flight set on
  every tick (spike S1 proved two consecutive ticks re-checking in-flight
  work).
- The drain and the accounting identity MUST agree on the definition of
  "in flight" — both derive from the same instance source; a divergence is a
  silent double-submit.

## Acceptance criteria (binary)

- AC1 (no double-submit): With the queue tables gone
  (`batch_jobs`/`batch_items` dropped in Phase 2), two consecutive drain runs
  over the same corpus enqueue no post twice. A post in flight (submitted,
  not yet harvested) is excluded from candidates on the second run, proven by
  a test that submits a candidate, runs discovery again, and asserts
  zero re-submits of the in-flight post.
- AC2 (instance-derived in-flight set): The drain builds its in-flight set
  exclusively from the Dagster instance —
  `get_materialized_partitions(submitted) − get_materialized_partitions(harvested)`
  — and NO code path in the drain reads `batch_items` or any ledger/status
  table (grep-verified; ADR-0013 forbids `external_jobs`).
- AC3 (interval-sensor shape): The sensor that drives the drain/harvest cycle
  is an interval `@sensor` that re-derives the full in-flight set every tick
  and requests a run only for terminal partitions. It is NOT an
  `@asset_sensor`: a test proves that in-flight work present across
  consecutive ticks is re-checked on the second tick (an asset sensor would
  never fire again for it).
- AC4 (named skip message): When the tick finds in-flight work but nothing
  terminal, the skip message NAMES the in-flight work (e.g. "4 in flight,
  none terminal yet"), distinct from "nothing in flight". A bare "no run"
  skip is indistinguishable from "nothing to do" and fails this criterion.
- AC5 (accounting identity): On every read,
  `done + failed + in_flight + backlog == total_candidates` holds, where each
  term is derived from the same instance + lake sources the drain uses. A
  test deliberately breaks one term and asserts the identity check fails —
  the identity is what makes a silently-broken metric detectable.
- AC6 (consistency with the completion guard): The drain's completion dedupe
  reads `silver_content_classification`, not `gold_analyses` (master plan
  Phase 2, exit criterion 3); the in-flight guard and the completion guard
  never disagree about a candidate's state.

## Definition of done

- [ ] The drain rewrite lands WITH the Phase 2 queue retirement (same phase,
      same PR sequence) — never deferred to Phase 5.
- [ ] The double-submit test across two consecutive drain runs is an exit
      criterion in `dagster-native-orchestration-implementation.md` (master
      plan requires it be added there) and passes against real instance state.
- [ ] Scoped tests pass: interval-sensor re-check behavior, skip-message
      content, accounting identity incl. deliberate-break negative test.
- [ ] No ledger table exists anywhere (asserted by name per ADR-0013).

## Tests

- Two consecutive drain runs over the same corpus with one post in flight:
  the second run enqueues zero items for that post (AC1) — the guard is
  proven, not assumed.
- Instance restart between the two runs: the in-flight set is re-derived
  correctly with no in-memory state (AC2, spike S5).
- Two consecutive sensor ticks with the same in-flight job: the second tick
  re-checks it (interval shape) and its skip message names the in-flight
  count (AC3, AC4).
- Deliberately corrupt one identity term: the identity assertion fails loudly
  rather than reporting a green-but-wrong metric (AC5).
- Post-harvest: the post drops out of the in-flight set and re-running
  discovery over it submits nothing (completion guard consistency, AC6).
