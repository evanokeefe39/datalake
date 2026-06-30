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

---

## Phase 5: Hardening (future)

Comprehensive test suite, CI/CD with S3 backend, sensor-driven bronze.
Not yet planned — implement after silver/gold/serving are stable.

**Phase 5 specifics (from original roadmap):**
- Comprehensive test suite: layer-by-layer coverage (schema, dedup, enrichment, SCD2, views)
- CI/CD: GitHub Actions workflow, S3/R2 backend for persistence
- Sensor-driven bronze: dynamic partition sensor from Apify `list_runs()`

## Future: Evaluate DuckLake

**When:** After the pipeline has been running in production for 3+ months and
at least one of: multiple concurrent writers needed, time travel queries necessary,
or schema evolution causing friction.

**Migration path:** ATTACH DuckLake catalog → register existing Parquet files →
replace INSERT OR REPLACE with MERGE INTO → add ducklake extension.

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
