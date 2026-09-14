# Enrichment v3 — Remediation Plan

Planning only. Branch `feat/enrichment-v3-phase-1-seam-and-landing`, HEAD `04ea11d`, 7/36 exit
criteria genuinely met. Inputs: [`../analysis/three-state-articulation.md`](../analysis/three-state-articulation.md) (target), [`../postmortem.md`](../postmortem.md)
§10, the three phase audits, [`../analysis/learnings-mece.md`](../analysis/learnings-mece.md) (C1–C5 control set), [`../panel/data.md`](../panel/data.md),
[`../panel/adversary.md`](../panel/adversary.md), `docs/architecture/pipelines/enrichment.md`, ADR-0011/0012/0013, master plan §3/§4/§8.

---

## 1. The fork in the road

### The three options, stated plainly

**(a) FINISH the migration as designed.** The code for all of ADR-0011's statics exists (seam,
landing, conform, six silver tables, four marts, silver-bound views) and is green under its own
tests. What is missing is *wiring and dynamics*: the drain→submit handoff, the
`enrichment_harvested` producer, the retry driver, the quarantine consumer, the seam call-site
adoption, and materialization of 12 objects against the live DB. Nothing audited says the
built code is wrong — it says it is unwired, unrun, and unobserved.

**(b) REVERT serving to `gold_analyses` and rebuild silver with process fixes.** The live DB is
already effectively state 1 (`gold_analyses` 9,576 rows, 22 views reading it, dashboard green),
so "revert" is really "abandon the branch". This discards: a correct prompt-identity scheme
(the entire legacy corpus is cleanly distinguishable by hash — audit P3-3), an idempotent
verbatim landing that is the one pattern implemented correctly (`test_harvest_landing.py:162`),
a conform layer that is provably API-free as coded, and the four mart definitions. The old
world's known limitations (hardcoded `domain`, `media_files = "[]"`, bespoke one-path queue)
are exactly what motivated the migration.

**(c) HYBRID — validation spike first, then decide on evidence.** One real bronze row driven
end-to-end (drain → submit → land verbatim → conform → silver → mart → view → drain run 2
suppression), on a subset (`--max-pages 1` / single post), before any further migration work.

### Assessment against the live data

- The live world is intact and serving: `gold_analyses` 9,576 rows, `gold_growth_facets` 205,
  22 transitive views, dashboard reads views only. There is no production outage to recover
  from — option (b)'s urgency is zero.
- Zero bronze rows means the migration has never met reality, but the *code-side* audits found
  no structural flaw in the built modules — the failures are all inter-connection and
  non-materialization (audits P2-1/P2-2, P4 #4/#5, P5/P6: "written but never materialized").
- The asymmetry the brief names is decisive: **ADR-0011 (statics: layering, seam, verbatim
  landing, deterministic conform) is sound and its code exists. ADR-0012 (dynamics: retry
  driver, quarantine consumer, harvested transition) is underspecified — those are design gaps
  that would have surfaced under any dispatch quality** ([`../panel/data.md`](../panel/data.md) §5). You cannot "revert
  your way out of" an underspecified dynamic; rebuilding silver would reproduce the same gaps
  unless ADR-0012 is completed first — which is exactly what option (a) must do anyway.

### Recommendation: **(c), converging to (a)**

Run the validation spike (W1) before any further work, with the ADR-0012 dynamic specs
(retry driver, quarantine disposition, harvested transition) written as part of the spike
preparation — the spike *is* the instrument that resolves the underspecification, because a
driver cannot be chosen on paper without seeing a real cycle. If the spike demonstrates the
round-trip (expected: the code is green and only unwired), proceed to FINISH (option a) with
the unit breakdown below. If the spike exposes structural breakage in the statics (not just
wiring), that is the first evidence ever obtained that would justify a (b)-style rebuild, and
the decision goes to the human with that evidence. Do **not** revert now: (b) destroys
reusable correct work to escape an orchestration problem whose fixes (§2) are independent of
the codebase choice.

**[HUMAN DECISION]**: adopt (c) with convergence to (a); spike budget (~1 real post, paid API
call) and abort criteria must be approved before any spend.

---

## 2. Ordered work breakdown

Every unit states its target files (≤5), declared dependencies, and an acceptance that is a
**demonstrated round-trip**, never an existence claim. Each unit names its MECE control (C1
specification / C2 accountability / C3 interface / C4 sequencing / C5 verification) per the
[`../analysis/learnings-mece.md`](../analysis/learnings-mece.md) §2 map, so every defect row of §2 there is covered exactly once.

### W0 — ADR-0012 dynamic specification (drives the fork decision)
- **What**: Write the three missing dynamic specs as an ADR-0012 addendum: (i) the *driver*
  that materializes `enrichment_harvested` (actor + trigger), (ii) the retry driver (who mints
  round N, when; partition-key round shape that both producer and consumer can derive), (iii)
  the quarantine disposition policy (consumer, triage cadence, retention, redrive rule). Also
  fix the partition-key shape requirement: the round dimension must survive derivation (no
  one-way digest fusing it).
- **Files**: `docs/architecture/adr/0012-dagster-native-orchestration.md`, `docs/architecture/adr/0014-orchestration-dynamics.md`, `tasks/plans/enrichment-v3-migration-master.md` (§3 sequencing note only).
- **Deps**: none. Blocks W1's fork decision.
- **Control**: C1 (three mechanisms existed with no driver/consumer).
- **Acceptance (round-trip)**: each spec answers "name the production consumer of everything
  you write and the production producer of everything you read" for retry, quarantine, and
  harvested — i.e., a worked cycle on paper: a failed submit produces a round-1 partition key,
  the retry driver consumes it, and the drain guard releases the post; a malformed response
  lands in quarantine, is read by its named consumer, and redrives. **[HUMAN APPROVAL: the
  spec sign-off gates the fork decision.]**

### W1 — Validation spike (the fork instrument)
- **What**: One real post (multi-image, one workload) driven end-to-end against the real
  providers: drain enqueue → submit through the seam → verbatim bronze landing → conform (zero
  API calls on re-run) → one silver table → mart/view → drain run 2 suppression. Subset only;
  no full corpus. Capture the real nested envelope as a test fixture (mocks are flat today).
- **Files**: `src/datalake/defs/enrichment/landing.py`, `src/datalake/defs/enrichment/conform.py`, `tests/fixtures/real_envelope_gemini.json` (new), [`../analysis/spike-evidence.md`](../analysis/spike-evidence.md) (new, evidence for the fork decision), `tasks/plans/enrichment-v3-migration-master.md` (result note).
- **Deps**: W0 (spec exists to check against). **Blocks everything else** — its verdict selects
  the path.
- **Control**: C5 (walking skeleton / tracer bullet — the most damning miss in the audits).
- **Acceptance (round-trip)**: `landing.read_responses()` height ≥ 1 with the real captured
  envelope; conform re-run over the same bronze row executes with a monkeypatched provider-SDK
  call counter reading **0**; the silver row exists keyed `(post_id, platform)`; a second drain
  run does not re-enqueue the post. Evidence file records each observed materialization.

### W2 — Branch stabilization: queue-retirement slices and the 25 red tests
- **What**: Land or discard the uncommitted changes; triage the 25 red tests. The zombie
  contract `_require_legacy_queue_tables` (`batch.py:51`) is deleted only when W3 has replaced
  its last reader — until then the red tests are fixed against the *current* topology, not
  deferred. Resolve the uncommitted work so the tree is reviewable.
- **Files**: `src/datalake/defs/enrichment/batch.py`, `src/datalake/defs/instagram/assets.py`, `tests/` (the 25 red tests), `tests/operational/test_state_compatibility.py` (unblock only; rewrite is W7).
- **Deps**: none (parallel with W0). Blocks W3.
- **Control**: C4 (retirement inverted: zombie kept alive by code that raises if removed).
- **Acceptance (round-trip)**: full suite green on the stabilized branch; then demonstrate the
  queue path: with `batch_jobs` present, the legacy read path executes; with it absent, the
  *replacement* discovery path (W3) serves submit — never a green test resting on deletion.

### W3 — Reconnect the halves: drain→submit handoff + retire the queue read-path
- **What**: `submit.py` discovers work from the Dagster instance's `enrichment_submitted`
  partitions (what the drain materializes at `assets.py:1289`), not from `batch_jobs`
  (`submit.py:77`). Delete `_require_legacy_queue_tables`. Multi-chunk handle round-trip test
  fixing the `"|"` vs `","` encoding divergence (submit.py:170 vs adapters.py:280) — one
  serialization, one test with a multi-chunk case.
- **Files**: `src/datalake/defs/enrichment/submit.py`, `src/datalake/defs/enrichment/batch.py` (delete), `src/datalake/defs/enrichment/partitions.py`, `src/datalake/defs/enrichment/adapters.py`, `tests/unit/enrichment/test_submit_discovery.py` (new).
- **Deps**: W1 verdict = finish (a); W2 (tree stable). Blocks W4, W5.
- **Control**: C2 (runtime producer/consumer pairing unowned) + C3 (handle encoding).
- **Acceptance (round-trip)**: on one shared instance: drain run materializes N partitions →
  submit discovers exactly N work items and submits them → the accounting identity
  `done + failed + in_flight + backlog == candidates` holds over the *real corpus*, not a
  constructed example. A test joins the two real halves and asserts a **nonempty handoff**.

### W4 — `enrichment_harvested` producer + retry driver
- **What**: Implement the driver specified in W0: an asset/job that materializes
  `enrichment_harvested` partitions when harvest reaches a terminal state (the missing writer
  that makes `in_flight = submitted ∖ harvested` shrink). Implement the retry driver: failed
  submits/terminal failures mint round-N partition keys per W0's shape; remove the round-0
  hardcode (`DRAIN_ATTEMPT_ROUND = 0`).
- **Files**: `src/datalake/defs/enrichment/partitions.py`, `src/datalake/defs/instagram/assets.py`, `src/datalake/defs/enrichment/harvest.py`, `tests/unit/enrichment/test_partitions_retry.py` (new).
- **Deps**: W3 (drain→submit exists to observe), W0 (spec).
- **Control**: C1 + C2 (mechanism with no actor; state transition with no named event).
- **Acceptance (round-trip)**: a real cycle: submit → harvest-terminal → `enrichment_harvested`
  partition observed materialized → drain run 2 re-enqueue eligible for that post while
  in-flight posts stay suppressed (the existing `test_two_consecutive_runs_enqueue_no_post_twice`
  rewritten so completion genuinely releases work — it must FAIL if the writer is removed).
  Retry: a forced terminal failure produces a round-1 key, distinct from round-0, and the guard
  admits it without double-submitting round 0.

### W5 — Seam call-site migration (the 12 bypasses) + one transport per service
- **What**: Route all 12 sites through the seam: `submit.py:152,170`;
  `harvest.py:311,316,319,334,446,454,459,462`; `facets_batch.py:318,322`. Fix the
  `max_tokens` interface defect by adding `JobSpec` to the seam's submit verb (the documented
  bypass reason). Delete `qwen_client`'s direct HTTP path (`qwen_client.py` submit/poll/retrieve
  usage) so `ServiceBackedAdapter` is the only qwen transport with one terminal predicate.
- **Files**: `src/datalake/defs/enrichment/submit.py`, `src/datalake/defs/enrichment/harvest.py`, `src/datalake/defs/enrichment/facets_batch.py`, `src/datalake/defs/enrichment/seam.py`, `src/datalake/defs/enrichment/qwen_client.py` (delete/retire).
- **Deps**: W3 (submit already rewired; avoids conflicting edits to `submit.py`). Blocks W1-grade
  real runs of the converged path; parallel with W4 only if W4's `harvest.py` edits are
  coordinated (both own `harvest.py` — see §5).
- **Control**: C2 + C3 (composition root never wired; two clients, divergent predicates;
  per-call parameter inexpressible).
- **Acceptance (round-trip)**: CI grep hook: provider names (`gemini_batch\.|qwen_client\.`)
  appear only inside adapter modules — zero matches elsewhere in `src/`; AND a seam-adoption
  test runs the real production entry point and records `run_lifecycle`/adapter calls on the
  stack trace. Existence of the registry test is explicitly not acceptance.

### W6 — Materialize the twelve objects (bronze→silver→marts) with the backfill
- **What**: Execute, for the first time, against `data/state.duckdb` and the real bronze root:
  the Phase-5 classification migration from `gold_analyses.result_json` (idempotent by natural
  key — `INSERT OR REPLACE` on `(post_id, platform)`; the backfill currently has **no**
  idempotency/reconciliation instrument — add one), the conform registration (give `conform`
  its production caller as a Dagster asset), and the four gold marts as views. Fix
  `gold_content_shape_performance` grain to include `platform` (and post-level signal) before
  materializing.
- **Files**: `scripts/migrate_classification_to_silver.py`, `src/datalake/defs/enrichment/conform.py`, `src/datalake/defs/instagram/assets.py` (conform caller + remove cross-domain DDL at :1170), `src/datalake/defs/serving/assets.py`, `tests/operational/test_backfill_idempotency.py` (new).
- **Deps**: W1 (real envelope shapes proven — conform has never met real data), W4 (terminal
  states exist to drive conform), W5 (all flows on the seam). Blocks W7.
- **Control**: C5 (zero real-run gate; twelve code-only objects; unproven replay guarantee).
- **Acceptance (round-trip)**: 9,576 `gold_analyses` rows accounted for —
  `count(silver_content_classification) + count(silver_enrichment_quarantine) == 9,576`, each
  migrated row carrying provenance (`provider, model, prompt_hash, schema_version, run_id`);
  re-running the backfill changes 0 rows (idempotency demonstrated by double-run + diff);
  delete a conformed row and re-conform from bronze with the SDK counter at **0** (replay,
  never re-bill — the keystone claim, now demonstrated on a real row). The 8 `model IS NULL`
  legacy rows are explicitly dispositioned (re-hashed via the documented procedure or
  quarantined with a reason code — not silently skipped).

### W7 — Serving rebind + schema-catalog/target reconciliation
- **What**: Rebind the 22 transitive views from `gold_analyses` to silver/marts (the silver-bound
  definitions already exist in `serving/assets.py` but have never executed). Reconcile the
  catalog to the TARGET world: `DUCKDB_TABLES` in `schemas.py` must expect
  `silver_content_classification` + the marts and stop listing `gold_analyses` as authoritative.
  Rewrite `test_state_compatibility.py` to assert view *definitions* against a baseline snapshot
  (not SELECT-ability) and to fail on the old world. **Additive-first**: new objects created and
  parity-verified before any view is rebound.
- **Files**: `src/datalake/defs/serving/assets.py`, `src/datalake/defs/common/schemas.py`, `tests/operational/test_state_compatibility.py`, `tests/operational/test_view_definition_baseline.py` (new), `dashboard/server.py` (read-only check only).
- **Deps**: W6 (silver populated — a rebind over empty silver serves nothing).
- **Control**: C4 (expand-contract: readers move before stores retire) + C5 (the vacuous gate).
- **Acceptance (round-trip)**: a parity job diffs the old and new worlds side-by-side for the
  migration window: for a sample of posts, `v_post_detail`-from-silver returns the same
  `result_json`-derived values as from `gold_analyses` (column-level equality on a real sample);
  view-definition snapshots of all 22 views captured BEFORE rebind, asserted unchanged-after
  except for their source table. Only after parity passes are the views rebound.

### W8 — Quarantine consumer + DQ/freshness gates (the last DQ layer)
- **What**: Give `silver_enrichment_quarantine` its named consumer: a triage view + a **blocking**
  Dagster asset check on quarantine growth and on `landed(bronze) ∖ conformed(silver)` (the
  anti-join ADR-0012 mandates). Repair the vacuous checks (asset checks that pass on unread
  parquet; `classify_error` defaulting unknown exceptions to TERMINAL; unbounded
  warn-and-continue poll loops). Declare freshness/volume expectations per asset.
- **Files**: `src/datalake/defs/enrichment/conform.py` (reason codes unchanged), `src/datalake/defs/enrichment/checks.py` (new), `src/datalake/defs/enrichment/harvest.py` (bound the poll loop), `src/datalake/defs/enrichment/classification.py` (`classify_error`), `tests/unit/enrichment/test_checks_fire.py` (new).
- **Deps**: W4 (terminal states), W6 (quarantine table real). Independent of W7.
- **Control**: C1 + C5 (control surface with no reader; gates that cannot fail).
- **Acceptance (round-trip)**: inject a deliberately malformed real-shaped response → the check
  fires (observed failure, not a log line); restore → check green. The anti-join check
  demonstrably fires when a bronze row has no conformed counterpart. Each check has a stated
  "what would make this fail" — the vacuity test of the audits.

### W9 — Retirement (Phase 7, last)
- **What**: Only after W3/W4/W7 prove the new world: drop `batch_jobs`, `batch_items`,
  `dead_letter`, `gold_growth_facets` from `ops.sqlite`/`state.duckdb`; reconcile
  `facets_batch_jobs`' 4 live ledger rows (1 JOB_FAILED, 2 RETRIEVABLE…) into the service's job
  store BEFORE the drop (audit P1-5a: currently nothing accounts for them); update docs.
- **Files**: `scripts/retire_queue_tables.py` (new), `data/ops.sqlite` (via script only), `src/datalake/defs/enrichment/facets.py` (docstring fix, P1-6), `docs/architecture/pipelines/enrichment.md`.
- **Deps**: W7 (readers moved — expand-contract contract satisfied), W3/W4 (new state sources proven).
- **Control**: C4 (retire only after readers move; starve, don't drop).
- **Acceptance (round-trip)**: reconciliation ledger for the 4 `facets_batch_jobs` rows first
  (each row mapped to a service job-store state or explicitly dispositioned), then the drop
  script runs, then a full drain→submit→harvest cycle succeeds **without** any legacy table —
  demonstrating the retirement, not asserting it.
  **[HUMAN APPROVAL required: any DROP against live data.]**

**MECE coverage check** against [`../analysis/learnings-mece.md`](../analysis/learnings-mece.md) §2: rows map to W0 (C1 rows: retry driver,
quarantine consumer, harvested producer — no duplication: spec in W0, implementation in W4/W8),
W2+W3 (drain/submit pairing, zombie queue), W5 (12 bypasses, dual clients, handle encoding),
W4 (partition-key shape, retry), W6 (mart grain, materialization, replay proof), W7 (catalog
reconciliation, vacuous gate, expand-contract), W8 (quarantine consumer, silent failures,
vacuous checks), W9 (sequencing/retirement). Every defect row covered at least once; no row
implemented twice (spec-once, build-once, verify-once per unit).

---

## 3. Blast radius & invalidation plan

| Change | Invalidates | Consumers to signal | Refresh class |
|---|---|---|---|
| W3/W4 discovery + in-flight switch (`batch_jobs` → Dagster partitions) | `ops.sqlite` `batch_jobs`/`batch_items` become read-dead; in-flight derivation changes | Drain, submit, harvest, accounting identity, any operator tooling reading the queue | Self-correcting for serving; **full** for pipeline state — first run under new discovery must be on a subset; 6 live `batch_jobs` rows reconciled before the read-path dies (W2/W9) |
| W4 retry/round keys | Partition keys minted before this change are round-0-shaped and unrecoverable by the new driver | Drain guard, accounting identity, in-flight dashboards | **Versioned backfill**: partition keys self-version by round suffix; legacy round-0 rows grandfathered and marked; identity re-checked on real corpus |
| W6 classification migration (9,576 rows) | Creates `silver_content_classification`; `gold_analyses` stays intact as read-only history — **9,576 rows must not be lost; nothing is dropped in this unit** | Serving views (via W7), dashboard, drain completion guard (now reads silver) | **Versioned backfill** — idempotent by natural key, proven by double-run; `gold_analyses` retained until W9 decision |
| W6 conform caller + marts | Creates 5 silver tables + quarantine + 4 marts (views); bronze becomes a live layer | Marts' consumers, asset checks | **Full** first materialization (nothing exists to refresh); thereafter self-correcting views |
| W7 serving rebind (22 transitive views) | Every view's source changes: `v_post_detail`, `v_overview`, and the 20 transitive views | **Dashboard (`dashboard/server.py`)** — reads views only, but the KPI values underneath change source; re-verify rendered KPI numbers post-rebind; also any notebook/ad-hoc consumer of `gold_analyses` by name | **Parity-gated cutover**: old and new coexist during migration window; views rebound atomically after sample parity passes; `gold_analyses` retained (never dropped without human approval) |
| W7 catalog reconciliation | `DUCKDB_TABLES`/`expected_schema.py` change meaning | `test_state_compatibility.py`, schema docs | Self-correcting (test suite re-reads catalog) |
| W8 checks/freshness | Adds blocking checks — new failure surface | On-call/operator workflows | Additive only |
| W9 drops (`batch_jobs`, `batch_items`, `dead_letter`, `gold_growth_facets`, `facets_batch_jobs`) | Irreversible removal of 776 `dead_letter` rows, 205 `gold_growth_facets` rows, queue history | Serving (must already be off gold by W7), dashboard | **Destructive — requires human approval + pre-drop archive** (export tables to Parquet under `data/lake/archive/` first). `gold_analyses` retention is a separate explicit decision — recommend retaining read-only through at least one full new-world cycle |

Nothing in this plan writes to `gold_analyses` or deletes any row before W9, and W9's drops are
gated, archived, and human-approved. All new DDL is additive (`CREATE OR REPLACE` for views,
new tables only).

---

## 4. Whole-remediation acceptance gate

**One end-to-end round-trip criterion:**

> One real, already-enriched post and one real never-enriched post, driven through the live
> pipeline: drain run 1 enqueues only the never-enriched post (completion guard reads silver,
> respects the 9,576 legacy rows); submit goes through the seam; the response lands verbatim in
> bronze (`ok` populated); conform materializes the silver row with zero additional API calls on
> re-run; the marts and serving views expose it; harvest reaches terminal state and the
> `enrichment_harvested` producer materializes; drain run 2 re-enqueues nothing in-flight and
> nothing already-conformed. Meanwhile `gold_analyses` still holds all 9,576 rows and the
> dashboard serves coherently throughout.

**Mechanical non-vacuity checks** (each can fail, each targets a specific vacuity found in the
audits):
1. **Reconciliation identity**: `count(silver_content_classification) + count(silver_enrichment_quarantine) ≥ 9,576` with every legacy row accounted (C5 — replaces the unmet C5.4).
2. **Provider-name grep in CI**: `gemini_batch\.|qwen_client\.` matches only adapter modules (C2/C3 — kills the 12-bypass class).
3. **Zero-row gate**: any new object referenced by a consumer with zero rows fails the merge (C5 — "every new object has rows").
4. **Catalog-vs-target reconciliation**: `DUCKDB_TABLES` names the TARGET world and the live DB matches — not the status quo (C5 — the vacuous-gate fix; reconciliation is against the *target* schema, per postmortem §10.2's correction).
5. **View-definition baseline**: all 22 transitive views' SQL snapshotted; rebind PRs must diff against it (C5 — replaces the never-written "asserted, not assumed").
6. **Zero-API-call replay counter**: a monkeypatched SDK-call counter reads 0 on a conform re-run over real bronze (C5 — replaces the AST scan).
7. **Falsifiability review**: every acceptance test in W2–W9 answers "what plausible bug makes it fail?" — a test that cannot fail is rejected at review (the audits' vacuity standard).

---

## 5. Sequencing & risk

### Declared DAG

```
W0 ──► W1 ──► [FORK DECISION — human] ──► W2 ──► W3 ──► W5 ──► W6 ──► W7 ──► W9
                                              │                ▲       ▲
                                              └──► W4 ─────────┘        │
                                                   W8 ──────────────────┘ (independent of W7)
```

- **W0, W2** start immediately and in parallel (no deps). W1 after W0.
- **W3 → W5 serial**: both edit `submit.py`; shared-file edits are one ownership boundary.
- **W3 → W4 serial**: the harvested producer and retry driver need the handoff to exist.
- **W4 and W5 both touch `harvest.py`** (W4 adds terminal-state materialization; W5 migrates 8
  call sites) — they MUST be either serialized or coordinated via `hub` with one owner named
  for `harvest.py`. Recommended: W4 first (dynamics before rewiring), then W5.
- **W6 → W7 serial**: rebind over empty silver serves nothing.
- **W8** parallelizable with W7 (different files; both depend on W6/W4 outputs).
- **W9** strictly last (expand-contract: nothing retires until its readers moved).

### Independent / parallelizable
W0 ∥ W2; W8 ∥ W7; W5 ∥ W8 if `harvest.py` ownership is settled first.

### Risky / destructive / human-gated
1. **Fork decision (after W1)** — human.
2. **W6 backfill** — first writes to `state.duckdb` under new logic; idempotency must be
   demonstrated (double-run) before it is trusted; mitigate by running on a copy of
   `state.duckdb` first (single-writer discipline).
3. **W7 view rebind** — the only step that can break the serving surface; parity-gated,
   snapshot-before, and the dashboard's rendered KPIs re-verified visually afterward.
4. **W9 drops** — irreversible; archive-first; explicit human approval per table; `gold_analyses`
   retention decided separately (recommend: keep read-only ≥ 1 full cycle).
5. **W1 spike spend** — real paid API calls; budget approved by human.
6. **The 25 red tests + uncommitted changes (W2)** — commit or discard decisions are human-visible
   in the PR; no history rewrite.

### Process controls binding every unit (from §10 of the postmortem — promotion, not prose)
- Each dispatch names the **owner** of the contract and the **production consumer** of
  everything it writes / producer of everything it reads (C2).
- Acceptance is a demonstrated round-trip with evidence of materialization, never an existence
  claim; the reviewer gate is run by a fresh-context `dlc-reviewer` per unit, verifying data
  coherence (rows exist? derived data stale? consumer contract changed?) not just code (C5).
- Mechanically checkable items ship as hooks/tests, not prose: the CI grep (W5), the
  reconciliation and definition-baseline tests (W6/W7), the replay counter (W6/W8).