# ROADMAP

Strategic direction and sequencing for the Duck Pond platform.

## Guiding principle

Stay Duck Pond. Adopt DuckLake later when ACID/time travel/concurrency matter.
The Dagster + Parquet + DuckDB medallion pattern is the right architecture for
this scale. DuckLake migrates cleanly from it because both store Parquet files —
the upgrade is a metadata migration, not a rewrite.

## Phase 1: Foundation ✅ Complete

Plan: `tasks/plans/phase-1-foundation.md`

### What shipped

- **Project structure** — `defs/common/`, `defs/instagram/`, `defs/serving/` packages
  with `PolarsIOManager`, `ApifyResource`, `GeminiResource`, `DuckDBResource`
- **Bronze asset** — `ig_posts_raw` (Apify → NDJSON → Polars → Parquet + `.meta`)
- **Migration** — `scripts/migrate_from_ig_pipeline.py` (10 bronze files, 2,768 rows migrated)
- **Tests** — 10 tests, all passing
- **Root cleanup** — old `resources.py`, `lake.py`, `schedules.py`, `config.py` deleted

### Key decision changes from original roadmap

- Polars replaces DuckDB for all Parquet I/O. DuckDB reserved for SQL transforms + state.
- No ParquetIOManager — replaced by PolarsIOManager.
- S3 backend deferred (out of scope for Phase 1).
- `_parquet_io.py` not created (Polars handles I/O directly).

---

## Phase 2: Silver Asset — Bronze Dedup

Plan: `tasks/plans/phase-2-silver-asset.md`

Read unprocessed bronze Parquet files, deduplicate by `post_id` via DuckDB DISTINCT ON,
write deduped silver Parquet via `PolarsIOManager`, populate `silver_posts` + `silver_progress`
state tables.

---

## Phase 3: Gold Asset — Gemini Enrichment

Plan: `tasks/plans/phase-3-gold-asset.md`

Read unenriched silver posts, send captions through Gemini (`gemini-3.1-flash-lite`)
for classification and enrichment, write gold Parquet, record `gold_analyses`.

---

## Phase 4: Serving Layer — Cross-Domain Dimensions and Views

Plan: `tasks/plans/phase-4-serving.md`

Build `dim_profile` (SCD2 with channel attribute), `dim_time`, and unified analytics views.
Serving is the pipeline output — what dashboarding tools query against.


## Phase 5: Watermark + Dead Letter Refactoring (2026-07-01)

Plan: `tasks/plans/watermark-deadletter-refactor.md`

### What shipped

- **Generic watermarks** — single `watermarks` table replaces per-pipeline progress tables.
  Any pipeline stamps its progress; reset by deleting a row.
- **Dead letter queue** — `dead_letter` table separates transient/permanent failures from
  main enrichment table. `gold_ig_analyses` contains only completed records.
- **Domain-scoped table names** — `silver_ig_posts`/`gold_ig_analyses`/`silver_ig_progress`
  support multi-source expansion (TikTok, YouTube, etc.)
- **Watermark-based discovery** — gold asset switched from LEFT JOIN gap detection to
  `processed_on > watermark_timestamp`. Cleaner, faster, naturally supports reset.
- **`processed_on` only on net-new posts** — silver asset no longer re-stamps every row
  every run. Existing posts keep their original processed_on.
- **Prompt hash stored in watermark** — `config_hash` column ready for Phase B auto-reset.
- **Migration script** — `scripts/migrate_to_v2.py` handles schema upgrade from Phase 1-4.
- **New tests** — coverage for processed_on stability, dead_letter routing, watermark reset,
  and generic watermark coexistence. 24 tests, all passing.

### Key decision changes from original roadmap

- Multi-source table naming (`silver_ig_posts` not `silver_posts`) to support expansion
  without name collisions. Deferred sources: TikTok, YouTube, LinkedIn.
---


## Phase 7: Batch-Based Enrichment Architecture ✅ Complete (2026-07-02)

Plan: `tasks/plans/enrichment-architecture-v2.md`

### What shipped

- **SQLite batch queue** — `batch_jobs` + `batch_items` in `ops.sqlite` (generic
  JSON payloads, consumer-tagged). `create_batch` / `claim_batch` /
  `claim_pending_items` coordinate work.
- **Standalone worker** — `scripts/enrichment_worker.py` claims the oldest
  pending batch and processes items with per-item retry (no sensor).
- **Per-item backpressure** — `scheduled_for` column; burst 429s reschedule
  with jittered exponential backoff.
- **Quota vs rate-limit distinction** — `insufficient_quota` halts the batch
  and reschedules remaining items without burning attempts; a burst
  `rate_limit_exceeded` retries per-item.
- **Media cache** — URL hash → File API URI cache in `media_metadata` table;
  scrape-time byte cache in `media_cache` (media end-to-end, 2026-08).
- **Asset changes** — `ig_posts_gen_batches` enqueues (no Gemini call);
  `gold_analyses` is an AssetSpec the standalone worker materializes.
- **Prompt hash** — `hashlib.sha256` for deterministic staleness detection across processes
- **Serving update** — `analytics_views` LEFT JOINs `gold_analyses` with domain filter
- **Daily schedule** — materialization every 3am (bronze→silver→enqueue is sub-second)

### Architecture

```
silver_ig_posts → ig_posts_gen_batches → ops.sqlite batch → worker → gold_analyses
```

Silver-to-gold materialization drops from 1+ hour to sub-second (just enqueuing work).
Materialization no longer blocks on API latency; one failure doesn't cascade.

## Phase 6: Hardening ✅ Complete (2026-07-01)

Plan: `tasks/plans/test-hardening.md`

### What shipped

- **Test architecture** — `tests/{unit,integration,e2e,fixtures,data}/` with domain subdirs
- **95 tests** (unit per-asset, integration cross-boundary, E2E full pipeline, operational readiness), 2 skipped (live API keys)
- **Domain-scoped factories** — `make_ig_bronze_row` / `write_ig_bronze` in `ig_bronze_factories.py`,
  schema loaded from real Parquet, 37-column exact match
- **Dagster asset checks** — 12 checks defined (instagram + serving), wired into Definitions, unit-tested
- **E2E coverage** — full pipeline happy path, watermark chain, dead_letter routing, cross-layer audit,
  data volume, schedule validation, ad-hoc runs, golden-dataset snapshot
- **Silver edge case** — empty bronze DataFrames with correct schema handled gracefully

### Validation Layer ✅

Plan: `tasks/plans/state-readiness-impl.md`

### What shipped

- **Schema contract catalog** — `tests/operational/expected_schema.py` with 6 tables + 1 view
- **State readiness tests** — `tests/operational/test_state_compatibility.py`: table existence, per-column type matching (extra columns tolerated), view queryability
- **Absent-DB handling** — `state_db` fixture skips all 8 tests cleanly when `data/state.duckdb` doesn't exist
- **Drift detection** — tested against missing column, type mismatch, and missing table scenarios; each produces a clear failure message
- **8 new tests** running at <0.5s
## Future: Creator 360 Analytics (DW metrics)

**Intent:** Move the creators list from a static directory to a rich per-creator
analytics profile in the DW. A creator (person/brand) owns 1..N profiles across
platforms; every metric must aggregate across all of them, tolerating multiple
profiles on a single platform.

**Engagement metrics (per creator, all profiles/platforms aggregated):**

- **Total posts** — sum across every profile (already surfaced in the creators
  list; promote to a first-class DW metric).
- **Posting frequency** — posts/week and posts/month, plus cadence (median gap
  between posts and recent trend). Drives "is this creator active/consistent?"
- **Standout vs Hot (decided 2026-09-02, shipped via PR #27)** — each post is
  judged against its OWN trailing point-in-time baseline (label-pass Tukey
  Q3/IQR at publish), never against the creator's all-time or rolling mean.
  A post is *standout* when `likes_zscore > 1.5` (Tukey fence); it is *Hot*
  when standout AND `likes_zscore >= 2` (2σ+). Canonical flags live in
  `v_post_metrics` (`is_standout`, `is_hot`); counts materialize per creator
  in `v_creator_metrics` and per profile in `v_profile_metrics`. Creator
  averages live only on creator-level surfaces (`v_creator_metrics`
  gate-free, `v_creator_quality` gated) — never on post rows.
- **Engagement trend (σ)** — time series of each post's `likes_zscore`
  relative to its own point-in-time baseline. Powers a line chart showing
  whether a creator is producing standouts lately.

**Content metrics:**

- **Topics + domain** — aggregate `gold_domain` / `gold_topic` / `gold_subtopic`
  per creator (top-N by post count), so a creator's coverage area is queryable
  without scanning posts.

**Commercial intelligence (new gold extraction):**

Extend the Gemini enrichment prompt + `result_json` schema to catalogue a
creator's monetization surface, then expose it via a dedicated serving view:

- **Products/services promoted** — sponsored or affiliate mentions.
- **Products/services founded/owned** — the creator's own products, courses,
  SaaS, agencies, etc.
- **Affiliations** — brands, partners, recurring sponsors.
- **Funnel strategy / CTAs** — link-in-bio, comment-to-DM, "comment X", watch
  next, etc.
- **Lead magnets** — freebies, checklists, ebooks, templates, webinars.

**Implementation notes:**

- Gold/serving-layer work only — no new source systems.
- Commercial extraction is additive JSON fields on `gold_analyses`; stale
  `prompt_hash` re-processing already handles backfilling existing posts when
  the prompt changes.
- Likely a `v_creator_profile` (or `v_creator_360`) view joining
  `creators`/`profiles` (ops) with silver/gold aggregates.

## Future: Evaluate DuckLake

**When:** After the pipeline has been running in production for 3+ months and
at least one of: multiple concurrent writers needed, time travel queries necessary,
or schema evolution causing friction.

**Migration path:** ATTACH DuckLake catalog → register existing Parquet files →
replace INSERT OR REPLACE with MERGE INTO → add ducklake extension.


## Future: Project Cost Tracking

**What:** Track the actual costs of building and running this project across three
dimensions:

- **API service costs** — Apify actor runs (Instagram scraping), Gemini/LLM API
  calls for enrichment. Log per-run costs in a `cost_log` table or spreadsheet.
- **Vibe coding token costs** — LLM tokens consumed by DeepSeek and GLM 5.2 models
  during development. Track sessions, model, prompt/response tokens, and estimated
  dollar cost. Useful for estimating whether agent-assisted development is cheaper
  than manual implementation.
- **Time investment** — Developer hours spent on architecture, implementation, and
  debugging. Track per phase to inform future project estimates.

**Why:** Understanding the real cost of a medallion pipeline built with agent-assisted
development provides concrete data for future project planning. Without this, it's
impossible to know whether Duck Lake is worth the cost, whether Apify is the right
scraper for production scale, or whether vibe coding saves time vs traditional dev.

**Suggested approach:** Log costs to a simple DuckDB table (`cost_log` with columns:
`phase`, `category` (api/tokens/time), `provider` (apify/gemini/deepseek/glm),
`units` (runs/tokens/hours), `amount`, `estimated_cost_usd`, `notes`, `recorded_at`).
Queryable alongside the pipeline data for a unified cost-per-insight metric.
## Negative space

**Out of scope for current phases:**
- Streaming/real-time ingestion (batch pipeline only)
- Multi-machine DuckDB (single machine)
- MotherDuck cloud integration
- Taxonomy management (self-mapped only)
- Profile scraping (separate pipeline path)

**Never:**
- Cloud data warehouse migration (Snowflake/BigQuery). Duck Pond is the platform.
- GitHub Issues (local `ISSUES.md` only)
- Abandoning medallion architecture

## Decision log

| Date | Decision | Rationale |
|---|---|---|
| 2026-06-30 | Stay Duck Pond, defer DuckLake | No data flowing yet; DuckLake solves problems we don't have. Migrates cleanly later. |
| 2026-06-30 | Polars for Parquet I/O, DuckDB for SQL/state | Polars handles NDJSON/Parquet edges; DuckDB handles transforms, state tables, views. |
| 2026-06-30 | S3/R2 deferred to hardening phase | No external infra needed for local dev; Paths and env vars are future-proofed. |
| 2026-06-30 | Rebuild state DB from Parquet for CI | Idempotent by design; cold start is correct and cheap. |
| 2026-06-30 | Bronze is manual trigger (not sensor) | Sensor-driven bronze is Phase 5; manual launchpad provides control during development. |
| 2026-06-30 | Domain-based structure, not layer-based | Dagster convention (dagster-open-platform). Scales to N data sources without giant files. |
| 2026-06-30 | One `assets.py` per domain | Dagster idiom; file-per-asset is not a Dagster convention. |
| 2026-06-30 | Migration as standalone script | Expert panel unanimous: migrations are one-shot ops, not ongoing data products. |
| 2026-06-30 | Serving layer cross-domain | Unified profile dim with channel attribute supports multi-source social media profiles. |
| 2026-07-01 | State readiness validation layer | Catches schema drift between code and running state DB. Explicit contract catalog prevents silent mismatches. |
