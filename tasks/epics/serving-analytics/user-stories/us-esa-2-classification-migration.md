---
id: US-ESA-2
epic: E-SERVING-ANALYTICS
persona: P1
status: Open
---
# US-ESA-2 — Migrate live classification to silver with no serving regression

- **Epic:** E-SERVING-ANALYTICS
- **Status:** Open
- **Relates to:** E-ENRICH-ENGINE (bronze landing `bronze_enrichment_raw` + the
  classification pass that will write `silver_content_classification`),
  US-ESA-1 (`silver_content_classification` is the gold marts' upstream
  input), E-DASHBOARD (insulated consumer — must stay insulated)
- **Source:** `tasks/plans/content-classification-migration.md` (the Phase 5
  sub-plan), `docs/architecture/pipelines/enrichment.md` §4 + §10
- **Migration phase:** Phase 5 of
  `tasks/plans/enrichment-v3-migration-master.md` ("content-classification
  migration — the live one"; the master's risk register names it "the one
  that can break production")
- **Settled contracts (do not re-litigate):** ADR-0011 (layered model; keys
  are `(post_id, platform)`; `domain` = content niche), ADR-0012
  (orchestration is Dagster-native), ADR-0013 (**no `external_jobs` ledger** —
  the service owns its job store, Dagster polls it over HTTP, in-flight state
  is instance-native; nothing in this migration creates a ledger or queue
  table). Migration strategy **RESOLVED — free** (master §6.1, sub-plan §4.1):
  backfill bronze from `result_json` and re-conform — zero re-billing, no
  re-enrichment.

## Verified evidence (2026-09-10, read-only against `data/state.duckdb`)

The free-migration claim was **re-verified directly** (not merely cited), with
one sharpening over the sub-plan's §4.1 wording:

- `gold_analyses` holds exactly **9,576** rows; **0** have NULL/empty
  `result_json`.
- **9,543** rows are JSON objects carrying all 10 target keys
  (`admiralty, content_type, domain, format, is_actionable, is_educational,
  style, subdomain, subtopic, topic`); **30** rows are JSON **arrays** whose
  element `[0]` carries the same 10 keys — the dual shape the serving views
  already handle via `COALESCE(result_json->>'$.X', result_json->>'$[0].X')`.
  So **9,573 / 9,576 rows conform after shape normalization.**
- **3 rows are key-deviant** — the reason the reconciliation gate below must
  be able to fail loudly: `3960884543625651437` (has `advisory`, no
  `admiralty`), `3917136874405145474` (has `subsubtopic`, no `subtopic`),
  `3863648947509006965` (no `subdomain`, no `topic`). Each is conformed via
  an explicit mapping rule or quarantined with a recorded reason — never a
  silent NULL.
- The duplicate-name bug is live: `SELECT DISTINCT domain FROM gold_analyses`
  returns exactly `['instagram']` — the key column carries the PLATFORM.
- Provenance caveats confirmed: `model` NULL on **8** rows;
  `distinct prompt_hash = 1`, `distinct model = 1` (no hash forking yet).
- `bronze_enrichment_raw` / `silver_content_classification` do not exist yet
  (consistent with the pre-Phase-1/4 state).

## Story

**As a** pipeline operator, **I want** the live `gold_analyses` table (9,576
rows, with two direct views and nineteen transitive views behind it) migrated
to `silver_content_classification` in strict Write → Audit → Publish order —
backfill bronze from `result_json`, conform deterministically, rebind the
direct views and readers additively, assert column/key/provenance parity,
reconcile every row, and only then retire the gold table — **so that** the
serving surface, the dashboard, and the owner's content-strategy questions
keep working with zero serving regression and zero re-billing.

## Contract

- **ADR-0013: no ledger anywhere.** The migration writes only
  `bronze_enrichment_raw` and `silver_content_classification`. No
  `external_jobs`, no queue table, no orchestration ledger is created;
  in-flight classification state stays Dagster-instance-native
  (`submitted ∖ harvested`).
- **The key rename is a correctness fix, not cosmetics.** Today's key
  `(post_id, domain)` carries the platform in `domain` while the body's
  `domain` field carries the niche — one name, two meanings, in the same
  table. Target key: `(post_id, platform)`;
  `domain`/`subdomain`/`topic`/`subtopic` are reserved for the niche.
- **The body becomes typed columns.** The opaque `result_json` blob that
  consumers JSON-extract becomes `domain, subdomain, topic, subtopic,
  is_educational, is_actionable, admiralty, content_type, style, format` +
  per-row provenance (`provider, model, prompt_hash, schema_version, run_id,
  analysed_at`). Dual-shape input (object or array-of-one) conforms
  deterministically — the `$[0]` fallback semantics the views use today.
- **Additive first, drop last — and the writer switches with the rebind.**
  New tables land before anything drops; `analysis.write_gold`
  (`enrichment/analysis.py:180`) stops writing `gold_analyses` when its
  readers move, so new enrichment lands bronze → conform → silver from then
  on; the drop happens only after every gate passes.
- **Negative space:** no serving view is renamed, restated, or dropped; the
  dashboard is never re-pointed at tables; the corpus is never re-enriched to
  migrate; the form-taxonomy overlap (`format`/`content_type`/`style` vs
  `silver_visual_annotations.value_medium`) stays an OPEN owner decision
  (`docs/architecture/pipelines/enrichment.md` §9.1) — this migration does
  not resolve it.

## Acceptance criteria (binary)

**Write (nothing consumer-visible changes):**

- AC1: `bronze_enrichment_raw` is backfilled from `gold_analyses.result_json`
  verbatim (the stored parsed body as `response_text`), one-time and
  idempotent on the natural key `(post_id, platform, workload, prompt_hash,
  run_id)`: a second run produces zero new rows and zero changed rows, and
  the backfill makes zero model/API calls (calls asserted at zero).
- AC2: `silver_content_classification` exists, created additively, keyed
  `(post_id, platform)`, with the 10 body columns + the provenance columns
  above; conform reads bronze ONLY with zero API calls, proven by a real run
  with calls asserted at zero.
- AC3 (row reconciliation — the loud gate): every one of the 9,576
  `gold_analyses` rows is either conformed or **explicitly quarantined with a
  machine-readable reason**; `conformed + quarantined = 9,576`, zero
  unexplained loss. The 3 key-deviant rows and the 30 array-form rows are
  individually accounted for by name (mapped or quarantined).

**Audit (before any consumer sees the new table):**

- AC4 (column parity): for every field `v_post_detail` extracts from
  `result_json` — `admiralty`, `gold_domain`, `gold_subdomain`, `gold_topic`,
  `gold_subtopic`, `content_type`, `style`, `format`, `is_educational`,
  `is_actionable` — the typed silver column carries an IDENTICAL value on the
  same `post_id`s, diffed row-for-row on a real corpus slice (not a fixture).
- AC5 (key + provenance parity): `platform` is populated on every row and
  equals the old key's value (`'instagram'`); every silver row carries
  non-null `prompt_hash`, `schema_version`, `run_id`; `model` is non-null or
  carries an explicit recorded gap marker — the 8 rows whose gold `model` was
  NULL are backfilled with provenance or dispositioned with a reason (the
  count reconciles to exactly 8; none silent).

**Publish (atomic, consumer-visible):**

- AC6: `v_post_detail` (`serving/assets.py:271`) and `v_overview`
  (`serving/assets.py:1144,1149`) read `silver_content_classification` and
  serve identical column names and identical rows before vs after, diffed on
  a real corpus slice. This includes the passthrough `result_json` column
  (`assets.py:221`, asserted on by `serving/asset_checks.py:172`): it must be
  served byte-identically — source it from the backfilled bronze
  `response_text`, which stores `result_json` verbatim. Consumers must not
  notice.
- AC7: all direct readers are rebound: `serving/asset_checks.py:163`,
  `instagram/asset_checks.py:262,299` (the rebind supersedes the misspelled
  `parsed.get('admirality')` lookup at `:271` with the typed `admiralty`
  column), `instagram/assets.py:1105,1125` (the enqueue completion guard now
  reads `silver_content_classification` with `platform`), `cli/pipeline.py:47`
  (the stale-batch scan reads `(post_id, platform)`); the test modules that
  read `gold_analyses` (the sub-plan §3 list) are updated with the rebind.
  After the rebind, grep finds zero remaining reads of `gold_analyses` in
  `src/`, and the migrations are updated in lockstep
  (`migrate_schema_drift.py` incl. its legacy `analytics_views` join,
  `migrate_to_v2.py`, `migrate_from_ig_pipeline.py`).
- AC8 (transitive surface unchanged): the 19 transitive views — `v_signal`,
  `v_quality_trend`, `v_creator_quality`, `v_rising_creators`,
  `v_domain_coverage`, `v_engagement_outliers`, `v_outlier_posts`,
  `v_creator_outlier_rate`, `v_underperformer_posts`,
  `v_creator_underperformer_rate`, `v_post_follower_context`,
  `v_post_baselines`, `v_post_metrics`, `v_creator_metrics`,
  `v_profile_metrics`, `v_creator_profile`, `v_creator_topics`,
  `v_standout_calendar`, `v_recent_hot_posts` — have byte-identical
  definitions before vs after, and `test_state_compatibility.py`
  (parametrized over `EXPECTED_DUCKDB_VIEWS` against the live DB) is green —
  the no-regression gate.
- AC9 (dashboard insulation preserved): the dashboard still reads ONLY the
  canonical serving views, never a table by name; its endpoints return the
  same response shapes as before the rebind (smoke-checked unchanged).
- AC10 (writer switched, then drop): the classification pass lands in bronze
  and conforms to silver (never writes gold directly); `instagram/assets.py:1182`
  `_ensure_state_tables` stops creating `gold_analyses`. Only after AC1–AC9
  pass: `gold_analyses` removed from the schema catalog
  (`common/schemas.py:90`) and dropped — never in the same PR that creates
  the replacement, and never before the rebind.

## Definition of done

- [ ] Backfill + conform exercised on a sample slice in a temp/isolated DB
      first (smoke), then production wiring reviewed before enablement.
- [ ] `test_state_compatibility.py` + the serving suite green after the
      rebind; schema catalog + readiness green for the bronze + silver rows.
- [ ] Reconciliation + parity evidence in the PR: counts, the quarantine list
      with reasons, before/after diffs of `v_post_detail` / `v_overview`.
- [ ] Single-writer discipline held — this phase does not run concurrently
      with Phases 4/6 (all write DuckDB; master §5).
- [ ] No ledger/queue table introduced anywhere (ADR-0013); no view renamed,
      restated, or dropped; `gold_analyses` dropped only after all gates.

## Tests

- Backfill idempotency: run the backfill twice over the same DB — identical
  bronze content, zero duplicate natural keys.
- Zero-call proof: conform from bronze with the model client mocked to fail
  on any call — the conform succeeds untouched.
- The reconciliation gate fires on fixtures of the 3 key-deviant shapes
  (missing `admiralty`/`subtopic`/`subdomain`; surrogate keys
  `advisory`/`subsubtopic`) — quarantined loudly with reasons, never a silent
  NULL or dropped row.
- Dual-shape conformance: an array-form `result_json` row and its
  object-form equivalent conform to identical silver rows.
- `v_post_detail` / `v_overview` before/after diff on a real corpus slice —
  identical rows, including the `result_json` passthrough column;
  `test_state_compatibility.py` parametrized view assertions green.
- Dashboard endpoints return the same response shapes pre/post rebind.
