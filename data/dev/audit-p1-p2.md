# Audit — Phase 1 (seam + landing) & Phase 2 (Dagster-native orchestration) exit criteria

READ-ONLY audit, 2026-09-13. Branch `feat/enrichment-v3-phase-1-seam-and-landing` (HEAD `04ea11d`).
Scope: every `- [ ]` exit criterion of Phase 1 and Phase 2 of `tasks/plans/enrichment-v3-migration-master.md`.
Method: criteria verbatim, evidence from code, tests, and READ-ONLY queries against the real
`data/state.duckdb`, `data/ops.sqlite`, and the real bronze lake root. Builds on (does not duplicate)
`data/dev/review-architecture-soundness.md`, `review-interfaces.md`, `review-round-semantics.md`.

**Headline:** Phase 1 is implemented as code with green unit tests, but the real artifacts the
criteria name do not exist: `bronze_enrichment_raw` has zero landed rows, `silver_content_classification`
does not exist in the real `state.duckdb`, and `facets_batch_jobs` still holds its 4 live rows.
Phase 2's drain guard is real code, but its "proven" no-double-submit test passes in a system that
can never submit twice because it can never submit a second time at all (`enrichment_harvested`
has no writer anywhere in `src/`).

## Criteria audited: 18. Count by status

| Status | Count | Criteria |
|---|---|---|
| MET | 3 | P1-2, P1-5b, P2-4 |
| MET in code, unproven on real data | 1 | P2-3 |
| PARTIALLY MET | 4 | P1-3, P1-4, P1-5, P1-5c |
| UNMET | 4 | P1-1 (criterion), P1-5a, P1-6, P2-1 |
| VACUOUS | 2 | P1-1's supporting test; P2-2 |
| MET only against fakes (folded into PARTIALLY MET) | 4 | — |

Both known vacuous examples from the brief are confirmed (P1-1, P2-2).

---

## Phase 1 — The seam and the landing

### P1-1. "`build_adapter("service_backed" | "direct_batch")` is the ONLY place a provider is named; swapping is a string, proven by test." — UNMET (criterion) / VACUOUS (its test)

- The test: `tests/unit/enrichment/test_adapters.py:286` `test_build_adapter_resolves_registered_names` — a unit test of the seam's own registry against fakes. It can fail mechanically (asserts a lookup table works), so not literally unfailable — but it tests the abstraction, not the system: it would pass even if no production code called `build_adapter` at all. Passes for the wrong reason; exercises a fake boundary.
- Production reality: `build_adapter` has exactly ONE production caller (`facets_batch.py:292`). Twelve production call sites bypass the seam — `submit.py:152,170`; `harvest.py:311,316,319,334,446,454,459,462`; `facets_batch.py:318,322`. `submit.py` and `harvest.py` never import `seam` at all (verified by grep today).
- A provider swap today means hand-rewriting submit + harvest. Real-run: the first real classification run executes `submit.py:152` (direct `gemini_batch.submit`), proving the criterion false in production.

### P1-2. "One submit fans out to both visual tables (NOT one submit per table)." — MET (by tests against a fake service; real-run unproven)

- Evidence: `tests/unit/enrichment/test_facets_batch.py:270` (`test_submit_health_checks_then_posts_one_job`), `:235` (`test_visual_ok`). The facets JSON schema embeds both facet and summary sections in one response, so one provider call covers `silver_visual_annotations` + `silver_visual_summaries` by construction.
- Caveat: every test uses a fake httpx transport. No real facets call has ever landed (P1-4: zero rows). A real run is the only proof the real service returns both sections in one body.

### P1-3. "A real envelope from BOTH providers parses through the same `Result` type." — PARTIALLY MET (parser unified; envelopes are NOT real)

- Both adapters normalize into the seam's canonical states/`Result` (`test_adapters.py:106,194` roundtrips; `:129,210` normalize-every-native-state); `Result` is shared. Good.
- But "real envelope" is the criterion's operative word and nothing real is parsed: both roundtrips use fakes (`gemini_jobs` in-memory fake; `service_routes` httpx mock). No captured real-response fixture exists anywhere under `tests/` (grep verified). The implementation plan itself flags this: "The mock returns a flat `items[]` while both real providers nest their results, so the mock cannot validate this." Real-run only: a captured Gemini batch result and a captured qwen `/results` body parsed through `Result`.

### P1-4. "`bronze_enrichment_raw` holds verbatim `response_text` for one real call per workload, and re-conforming from it makes zero API calls." — PARTIALLY MET (mechanism built and tested; the "one real call per workload" never happened)

- Mechanism: `landing.py` — schema carries verbatim `response_text`, natural key `(post_id, platform, workload, prompt_hash, run_id)`, idempotent `land_response`. `harvest.py:210` lands BEFORE parsing; `conform.py:662` re-conforms from `read_responses()` (lake, not provider).
- Tests: `test_harvest_landing.py:98` byte-identical landing, `:162` reharvest lands zero rows, `:129` landing-before-parse; `test_landing.py:61` byte-identical roundtrip. Green against fakes.
- Real state: the landing is EMPTY. `landing.read_responses()` over the real bronze root returned `height == 0` (queried 2026-09-13). `bronze_enrichment_raw` exists in no database (checked `state.duckdb`, `data/_gate.duckdb`, `data/_tmp_merge/state.duckdb`); it is a parquet dataset with no rows. Zero real calls per workload. The zero-API-call re-conform is proven only against synthetic rows.

### P1-5. "The qwen service's async job model is the `ServiceBackedAdapter` contract; no Gemini-specific code is reachable from the qwen path." — PARTIALLY MET

- The adapter mirrors the service's async model (submit -> `/jobs`, poll -> `/jobs/{id}`, retrieve -> `/results`; `normalize_state` maps every native state, `test_adapters.py:129`).
- Isolation is asserted for the adapters module: `test_adapters.py:340` `test_service_backed_path_imports_no_gemini_symbols`. This test CAN fail (inspects `ServiceBackedAdapter` source and module bindings) but is scoped too narrowly: it covers `adapters.py`, not the qwen path.
- The qwen path still reaches outside the adapter: `facets_batch.py:318,322` call `qwen_client.check_health` / `submit_job` directly (its own docstring admits the seam cannot express `max_tokens`), leaving a second HTTP client and a second terminal predicate (`qwen_client.job_is_terminal`). The path runs on two contracts, so "the async job model is THE contract" is not yet true.

### P1-5a. "`facets_batch_jobs` is dropped and every one of its 4 ledger rows is reconciled into the service's job store... before the drop." — UNMET

- Real query (2026-09-13, `data/ops.sqlite` read-only): `facets_batch_jobs` EXISTS with 4 rows — `4de3befc...` (JOB_FAILED), `58dbe69c...` (RETRIEVED), `c28c34d0...` (RETRIEVED), plus a 4th. No reconciliation artifact exists (no migration script, no accounting record, no code reads these rows). The drop has not been executed; nothing here has started.

### P1-5b. "No ledger table exists: `external_jobs` is NOT created (ADR-0013)." — MET

- Real query: full table list of `data/ops.sqlite` is `media_cache, batch_jobs, sqlite_sequence, batch_items, media_metadata, dead_letter, creators, profiles, creator_merges, prompt_registry, facets_batch_jobs` — no `external_jobs`, no new ledger table anywhere. The negative assertion is also pinned by `test_facets_landing.py:239`. This criterion's deliverable IS the absence, and absence is verified against the real DB.

### P1-5c. "`bronze_enrichment_raw` carries an explicit `ok`/status column, so failure is READ, not inferred from a missing conformed row." — PARTIALLY MET (column exists and is read; NOT populated on real rows)

- Exists: `landing.py` SCHEMA declares `ok: pl.Boolean` + `error_message`; failure rows land with `ok=False` (`test_harvest_landing.py:188,212,228`); conform/failure derivation reads the column rather than inferring from absence.
- Not populated on real rows: the real landing has zero rows (P1-4). The criterion exists so failure is distinguishable in production; unit tests prove the code path, but no real failed call has ever exercised it. Real-run only: a real failed harvest landing an `ok=False` row.

### P1-6. "`facets.py`'s false Gemini docstring is corrected." — UNMET

- `src/datalake/defs/enrichment/facets.py:4`: "The batch-native facets path (`facets_batch`) submits visual + text calls to the Gemini BATCH API and harvests them here" — still false; the batch-native path submits to the qwen service via `service_backed` (`facets_batch.py:292`). A one-line doc fix, explicitly listed, not done.

---

## Phase 2 — Orchestration is Dagster-native

Context (from the sibling reviews, re-verified today): the drain now materializes `enrichment_submitted` partitions, but nothing in `src/` writes `enrichment_harvested` (grep: only `partitions.py` definitions and docstrings), and `submit.py` still reads `batch_jobs`, which nothing writes. The two halves are disconnected.

### P2-1. "`ig_posts_gen_batches` enqueues correctly with `batch_items` gone, deriving in-flight state from the Dagster instance." — UNMET

- "With `batch_items` gone" fails factually: `data/ops.sqlite` still carries `batch_items` LIVE — 10,231 complete, 52 failed, 2 pending (queried 2026-09-13). `batch_jobs` (6 rows) and `dead_letter` also exist; `batch.py` is still in the tree and `_require_legacy_queue_tables` (batch.py:51) raises when the tables are missing, i.e. the queue is an active dependency, not a retired one.
- The instance-derivation half IS implemented: the drain's in-flight guard is `drain_suppressed_post_ids` -> `partitions.in_flight_partitions(instance)` (`instagram/assets.py:1043-1061`, applied at `:1224-1244`; enqueue materialization at `:1289`). No queue read anywhere in the drain.
- But enqueue correctness end-to-end cannot hold: the drain materializes partitions that NO submit path consumes (`submit.py:77` still SELECTs `batch_jobs`). A real run enqueues, submits nothing, then the guard suppresses those posts forever. Only a real run reveals this; no test joins the two halves.

### P2-2. "Two consecutive drain runs over the same corpus enqueue no post twice (the guard is proven, not assumed)." — VACUOUS

- The test: `tests/unit/instagram/test_drain_inflight_guard.py:199` `test_two_consecutive_runs_enqueue_no_post_twice` (real-instance variant at `:292`).
- Why it passes for the wrong reason: run 2 enqueues nothing at all — every candidate is suppressed. Structural cause: `enrichment_harvested` has NO writer in `src/` (grep verified; only `partitions.py` and a docstring mention it). Therefore every `enrichment_submitted` partition materialized in run 1 stays in the in-flight set FOREVER, whether the work completed, failed, or was never submitted. `in_flight = submitted - harvested` can only grow, so run 2 suppresses 100% of candidates unconditionally.
- Could the test fail? Mechanically yes — remove the guard and run 2 re-enqueues 3. But the property the criterion names — the guard suppresses exactly the in-flight work AND releases completed work — is never exercised, because the release side does not exist in production code. The test proves a guard over a monotone set, which is equivalent to "the pipeline runs once and then stops."
- The companion test `test_one_post_done_others_in_flight_still_no_double_submit` (:236) simulates completion by inserting a `silver_content_classification` row — but that does NOT release the partition (in-flight is instance-derived, not lake-derived); the test asserts `_enqueued(second) == 0`, i.e. it asserts the stall, not the cycle.
- Real-run translation: after the first drain, a real run can never submit anything again. "No post twice" is trivially satisfied by "no post twice, ever" — including the legitimate re-enrichment after a prompt change. Confirms the brief's known Phase-2 example.

### P2-3. "The completion guard reads `silver_content_classification`, not gold." — MET in code and tests; table does not exist in the real DB

- Code: `instagram/assets.py:1192` and `:1212` — both candidate queries use `NOT EXISTS (... silver_content_classification c WHERE ... prompt_hash = ?)`. No `gold_analyses` reference remains in the guard. Test: `test_drain_inflight_guard.py` `_conform` + `test_one_post_done...` exercise it.
- Caveat: `silver_content_classification` does not exist in the real `data/state.duckdb` (queried 2026-09-13 — catalog error). The drain creates it lazily via `_cls.CLASSIFICATION_DDL` (`assets.py:1170`, itself the layering violation flagged in `review-architecture-soundness.md` section 4.1). On a first real run the table is empty, so every post already enriched in `gold_analyses` re-enters candidates — the completion guard would re-enrich (and re-bill) the entire already-paid corpus. Real-run-only defect.

### P2-4. "The accounting identity and the drain agree on the definition of 'in flight' — asserted by a test, since a divergence is a silent double-submit." — MET (structurally, by shared code), with a real-run caveat

- Agreement mechanism: both sides call the same function. Guard: `partitions.in_flight_partitions(instance)` (`assets.py:1056-1061`). Identity: `in_flight = len(in_flight_partitions(instance))` (`partitions.py:281-330`). Same source, same key derivation — the feared divergence cannot occur by construction.
- The test: `test_drain_inflight_guard.py:241` `test_enqueue_partition_keys_equal_guard_derived_keys` asserts the keys the drain materializes at enqueue are EXACTLY the keys the guard derives. It can fail (any key-shape drift) and exercises the production write path. Genuine.
- Real-run caveat: agreement is currently agreement about a set that is always everything-ever-submitted (P2-2). The identity `done + failed + in_flight + backlog == candidates` has only been tested on constructed examples (`test_partitions.py:179`); it has never been checked on the real corpus — which the implementation plan's own Phase 1 requires. `review-round-semantics.md` section 4 additionally shows the identity will double-count once any retry round exists; reachable in Phase 3, noted because the identity is this phase's guardrail.

---

## What only a real run would catch

1. The pipeline halts after one cycle (P2-1/P2-2): drain -> partitions -> no consumer -> guard suppresses forever. Both halves unit-tested with separate fakes; no test joins them. Largest real-run exposure in Phases 1-2.
2. Verbatim landing with real provider bodies (P1-3, P1-4, P1-5c): schema, idempotency, and `ok` semantics proven only on synthetic rows; zero real rows; real envelopes nest (mocks are flat).
3. Completion guard against real data (P2-3): first real run sees the whole already-enriched corpus as candidates (double-bill against `gold_analyses` history).
4. The 4096-token truncation question (implementation-plan Phase 1 criterion that P1-3/P1-4 depend on): unmeasured on a real multi-image post.

## Final counts (18 criteria)

- Genuinely MET: 3 (P1-2 code-level, P1-5b, P2-4)
- MET in code only, unproven on real data: 1 (P2-3)
- PARTIALLY MET: 4 (P1-3, P1-4, P1-5, P1-5c)
- UNMET: 4 (P1-1 criterion, P1-5a, P1-6, P2-1)
- VACUOUS: 2 (P1-1's supporting test; P2-2)
