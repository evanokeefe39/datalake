# datalake — Agent operating context

This repo is operated by Claude. Keep this file current — Claude reads it on every session.

## Key rules

- Never use `pip`. Always use `uv` for Python package management.
- Work on `feat/*`, `fix/*`, `chore/*` branches; squash-merge to `main` via PR.
- Conventional commits only: `type(scope): summary`.
- No direct pushes to `main`.
- Never use PowerShell.

## Architecture

**Medallion lakehouse:** bronze → silver → gold → serving.

| Layer | Storage | Writer | State tracking |
|-------|---------|--------|----------------|
| Bronze | Parquet (`data/lake/bronze/`) | Polars (direct write) | None — file-based |
| Silver | Parquet (`data/lake/silver/`) | PolarsIOManager | DuckDB `silver_ig_posts` + watermarks |
| Gold | Parquet (`data/lake/gold/`) | PolarsIOManager | DuckDB `gold_ig_analyses` + watermarks |
| Serving | DuckDB views + tables | DuckDB | `dim_profile` (SCD2), `analytics_views` (VIEW) |

**Domain-based structure, not layer-based:**

```
src/datalake/defs/
├── common/          # PolarsIOManager, ApifyResource, GeminiResource, lake.py, schedules.py
├── instagram/       # ig_posts_raw, ig_posts_slv, ig_posts_gld, ScrapeConfig
└── serving/         # dim_profile, analytics_views (cross-domain)
```

**Storage split:**
- **Parquet lake** — bulk data, lock-free parallel writes
- **DuckDB** (`data/state.duckdb`) — authoritative current state, watermarks, SCD2 dims, views

**Engine boundary:**
- Polars handles all Parquet I/O (read/write NDJSON and Parquet)
- DuckDB handles SQL transforms (DISTINCT ON dedup, watermark queries, SCD2, views)
- Arrow is the zero-copy interchange format between them (`to_arrow()` / `from_arrow()`)

## Table naming convention

Domain-scoped, not generic. Supports multi-source expansion (TikTok, YouTube, LinkedIn in future).

| DuckDB table | Purpose |
|---|---|
| `silver_ig_posts` | Deduped, normalized Instagram posts |
| `silver_ig_progress` | Per-dataset processing audit log |
| `gold_ig_analyses` | Completed Gemini enrichments only (no status column) |
| `dead_letter` | Failed enrichments — separate table, separate retry pipeline |
| `dim_profile` | SCD2 profile dimension (cross-domain, `channel` column) |
| `watermarks` | Generic progress tracking for any pipeline (`name`, `timestamp`) |

Parquet file names match asset keys, not table names — the PolarsIOManager uses `asset_key.path[-1]`.

## Watermarks pattern

A single `watermarks` table replaces per-pipeline progress tables. Any pipeline stamps its row:

```sql
CREATE TABLE watermarks (name TEXT PRIMARY KEY, timestamp TIMESTAMP NOT NULL);
```

- Silver reads/writes `watermarks WHERE name = 'silver_ig'`
- Gold reads/writes `watermarks WHERE name = 'gold_ig'`
- **Reset a pipeline:** `DELETE FROM watermarks WHERE name = '<pipeline>'` — next run reprocesses everything
- **Prompt hash** column deferred — will enable auto-reset when Gemini prompt changes

This pattern was adopted after panel review (2026-07-01) confirmed it's standard in Airflow (XCom), Prefect (blocks), and dbt (run_results).

## Dead letter pattern

Failures from Gemini enrichment go to a separate `dead_letter` table, not the main results table. This keeps `gold_ig_analyses` pure (only completed enrichments) and provides a clean retry surface:

```sql
CREATE TABLE dead_letter (
    post_id   TEXT NOT NULL,
    domain    TEXT NOT NULL DEFAULT 'instagram',
    error     TEXT,
    attempts  INTEGER NOT NULL DEFAULT 0,
    failed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    status    TEXT NOT NULL DEFAULT 'pending',
    PRIMARY KEY (post_id, domain)
);
```

A separate scheduled asset (`retry_dead_letter`, deferred) reads `WHERE status = 'pending'`, retries, and upserts successes. This mirrors ML feature store patterns (Feast, Tecton) where error queues are separate from serving data.

## processed_on semantics

`processed_on` in `silver_ig_posts` is set **only when a post first appears in silver**. It never changes on subsequent runs, even when engagement metrics update. This enables gold to do true incremental processing:

```sql
SELECT ... FROM silver_ig_posts WHERE processed_on > (SELECT timestamp FROM watermarks WHERE name = 'gold_ig')
```

If a post appears in a new bronze scrape with updated likes_count but the same caption, `processed_on` stays unchanged because the caption didn't change — re-enrichment would be wasteful.

**Historical note:** The previous column was called `silvered_at` and was re-stamped on every row every run. This made incremental gold processing impossible (every run saw all posts) and the column was effectively "last_touched_at" rather than "first_seen_at." Renamed and fixed in the watermark refactor (2026-07-01).

## Design process

Non-trivial architecture decisions go through a **panel of experts** review before planning. The panel typically includes:

- **Data Architect** — medallion patterns, normalization boundaries, naming conventions
- **ML Engineer** — feature table patterns, enrichment idempotency, reprocessing strategy
- **Dagster Expert** — asset conventions, I/O manager usage, scheduling patterns

The panel reviewed the watermark + dead_letter refactor (2026-07-01) and confirmed: domain-scoped silver, cross-domain gold, watermark-based incremental processing, dead_letter separation, and asset names unchanged (table names only). Plan at `tasks/plans/watermark-deadletter-refactor.md`.

## What didn't work (anti-patterns confirmed)

| Anti-pattern | Why it failed | What replaced it |
|---|---|---|
| Status columns on data tables (`gold_analyses.status`) | Mixed concerns: results and error tracking in one table. Required `WHERE status = 'completed'` on every query. | `dead_letter` table — results go to `gold_ig_analyses`, failures to `dead_letter` |
| Single-purpose watermark tables (`silver_watermark`) | Doesn't scale to N pipelines. Each new pipeline adds a new table. | Generic `watermarks(name, timestamp)` table — any pipeline uses it by name |
| LEFT JOIN gap detection for pending posts | Complex query, no clean reset mechanism. Resetting required mass UPDATE. | Watermark-based: `WHERE processed_on > watermark_timestamp`. Reset = DELETE row. |
| Re-stamping timestamps on every run (`silvered_at`) | Destroyed "first seen" semantics. Gold couldn't do incremental processing. | `processed_on` set on INSERT only, never updated |
| Layer-based directory structure (`defs/{bronze,silver,gold}/`) | Doesn't scale to multiple data sources. Forces unrelated code together. | Domain-based (`defs/instagram/`, `defs/serving/`) |
| Modeling against test data without verifying real data | Phase 2 silver was built against a 3-row test fixture when real data had 28 columns with nested types. | Gate: read ONE real input file and display schema before writing any asset that reads from disk |

## Dagster

- `dagster dev` → localhost:3000 (or `dg dev` if installed)
- Definitions module: `src/datalake/definitions.py`
- Assets: `defs/instagram/assets.py` (domain-scoped), `defs/serving/assets.py` (cross-domain)
- Resources: `defs/common/resources.py` (PolarsIOManager, ApifyResource, GeminiResource)
- Config schemas: `defs/instagram/config.py` (ScrapeConfig, GoldConfig)
- Path helpers: `defs/common/lake.py` (env-overridable, auto-creating directories)
- Schedules: `defs/common/schedules.py` (weekly_medallion)
- Telemetry disabled (`dagster.yaml`)
- `[tool.dagster]` in `pyproject.toml` enables auto-discovery

## Bronze asset (ig_posts_raw)

- **Manual trigger only** — not scheduled. User provides `ScrapeConfig` via Dagster launchpad.
- **Apify flow:** trigger_run → poll_run → stream_dataset (NDJSON) → Polars read_ndjson → write_parquet
- **Idempotent:** if Parquet already exists for dataset_id, re-reads and returns it
- **No DuckDB state** — bronze is pure Parquet
- **Bypasses PolarsIOManager** — dynamic dataset_id paths, writes directly via `df.write_parquet()`

## Silver asset (ig_posts_slv)

- **Trigger:** downstream of bronze (`deps=["ig_posts_raw"]`), plus weekly schedule
- **Incremental:** mtime-based bronze file discovery (files newer than `watermarks.silver_ig`)
- **Dedup:** DuckDB `DISTINCT ON(post_id)` — newest timestamp wins
- **Column mapping:** camelCase (Apify) → snake_case (silver) via `_BRONZE_TO_SILVER` dict
- **List serialization:** `hashtags` (Polars List → JSON string) for DuckDB TEXT compatibility
- **processed_on:** set only for net-new post_ids; existing posts keep their value
- **State:** `silver_ig_posts` (DuckDB table), `silver_ig_progress` (per-dataset log), `watermarks.silver_ig`

## Gold asset (ig_posts_gld)

- **Trigger:** downstream of silver (`deps=["ig_posts_slv"]`), plus weekly schedule
- **Discovery:** `WHERE processed_on > (SELECT timestamp FROM watermarks WHERE name = 'gold_ig')`
- **Model:** gemini-2.0-flash-lite, temperature=0.2, JSON response
- **Rate limit:** processes 10 posts per run (LIMIT 10)
- **Retries:** 3 attempts per post, exponential backoff (2^attempt seconds)
- **Failures → dead_letter** (not gold_ig_analyses)
- **Empty captions → dead_letter** (no API call)
- **Idempotent:** INSERT OR REPLACE by post_id
- **Reset:** `DELETE FROM watermarks WHERE name = 'gold_ig'` — next run reprocesses all silver
- **Gemini rate limits:** if 429/403 encountered during smoke tests, stop and request a new API key

## Serving layer

- `dim_profile`: SCD2 profile dimension. Reads DISTINCT profiles from `silver_ig_posts`. Closes old rows on username change, inserts new rows. `channel = 'instagram'`.
- `analytics_views`: CREATE OR REPLACE VIEW joining `silver_ig_posts` + `gold_ig_analyses` + `dim_profile` (current rows only). Query surface for dashboards.
- Both are in `defs/serving/assets.py`, group_name="serving"

## Smoke testing

Each implementation phase includes a targeted smoke test using a **temporary DuckDB database** (`data/smoke_test.duckdb`) with 2-3 test posts. Zero interference with production state. Smoke DB is deleted after verification.

## IG pipeline library

The old `ig_pipeline` library at `~/repos/ig-pipeline` is imported as a thin Apify client wrapper. Functions (`trigger_run`, `poll_run`, `stream_dataset`) are imported via `sys.path` in `defs/instagram/assets.py`. Extraction into the datalake package is deferred.

## Env vars

Set in `.env`:

| Variable | Default | Used by |
|----------|---------|---------|
| `APIFY_API_TOKEN` | — | `ApifyResource` |
| `GEMINI_API_KEY` | — | `GeminiResource` |
| `IG_DATA_DIR` | `data` | `lake.py` root path |
| `IG_BRONZE_DIR` | `data/lake/bronze` | Bronze asset |
| `IG_SILVER_DIR` | `data/lake/silver` | Silver asset |
| `IG_GOLD_DIR` | `data/lake/gold` | Gold asset |
| `IG_DB_PATH` | `data/state.duckdb` | DuckDB resource |

## Test conventions

- `uv run pytest tests/ -v`
- In-memory DuckDB (`:memory:`) via dependency injection
- Parquet tests use `tmp_path`
- One test per behavioral contract, one per edge case
- **Before writing any asset that reads from disk, read ONE real input file and display its schema.** Don't model against test data. Lesson from Phase 2 false start (2026-06-30).

## Decision log

| Date | Decision | Rationale |
|---|---|---|
| 2026-06-30 | Parquet for bulk, DuckDB for state | Lock-free parallel writes; DuckDB handles SQL transforms, SCD2, views |
| 2026-06-30 | Polars for I/O, DuckDB for SQL | Polars handles NDJSON/Parquet edges; DuckDB handles transforms and state |
| 2026-06-30 | Domain-based, not layer-based | Dagster convention. Scales to N data sources without giant files |
| 2026-06-30 | One `assets.py` per domain | Dagster idiom; file-per-asset is not a Dagster convention |
| 2026-06-30 | Bronze bypasses I/O manager | Dynamic dataset_id paths from Apify; I/O manager uses deterministic asset key paths |
| 2026-06-30 | Migration as standalone script | One-shot operations, not ongoing data products |
| 2026-06-30 | No GitHub Issues | Local `ISSUES.md` only |
| 2026-07-01 | Generic `watermarks` table | Replaces single-purpose `silver_watermark`. Panel review confirmed standard pattern |
| 2026-07-01 | `dead_letter` table for enrichment failures | Separates results from error tracking. Panel review confirmed ML feature store pattern |
| 2026-07-01 | Watermark-based gold discovery | Replaces LEFT JOIN gap detection. Reset = DELETE row, not mass UPDATE |
| 2026-07-01 | `processed_on` set only on net-new posts | Fixes re-stamping bug. Enables true incremental gold processing |
| 2026-07-01 | Domain-scoped table names (`silver_ig_posts`) | Supports multi-source expansion. Cross-domain normalization happens in gold, not silver |
| 2026-07-01 | Panel of experts for architecture review | Data Architect + ML Engineer + Dagster Expert review non-trivial design decisions |
| 2026-07-01 | Smoke tests between phases | Temp DB with subset of data, wiped after verification. Self-steering during implementation |
