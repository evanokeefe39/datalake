## 0.3.0 (2026-07-02) — Queue-Based Enrichment Architecture

### Architecture
- Queue-based work-queue architecture replaces synchronous Gemini enrichment
- SQLite (`ops.sqlite`) for operational state: `enrichment_queue`, `media_metadata`, `dead_letter`
- DuckDB (`state.duckdb`) remains analytical: silver tables, `gold_analyses`, watermarks, views
- Per-item backpressure via `scheduled_for` column; one 429 doesn't kill the batch
- Crash recovery via stale-item reaper (inline in `claim()` transaction)
- URL-based media dedup cache: same media URL → cached File API URI, no re-upload

### New Modules (`defs/enrichment/`)
- `queue.py` — five functions: `enqueue`, `claim` (with reaper), `complete`, `fail`, `reschedule` + `delete`, `depth`
- `worker.py` — `enrichment_worker` op + `enrichment_job`: reads silver, calls Gemini, writes `gold_analyses`
- `sensor.py` — `enrichment_sensor`: polls queue every 30s, claims up to 5 items, emits RunRequest
- `media_cache.py` — URL hash → Gemini File API URI cache in ops.sqlite
- `assets.py` — `gold_analyses` AssetSpec (not @asset), `check_enrichment_health`, `check_prompt_currency`
- `prompts.py` — `IG_GOLD_PROMPT` with `hashlib.sha256` prompt hash (deterministic across processes)

### Resource Changes
- New `SQLiteResource` in `defs/common/resources.py` (WAL mode, row factory, busy timeout)
- `ops` resource wired in `definitions.py`

### Asset Changes
- `ig_posts_gld` → `ig_posts_gld_enqueue`: depends on silver, writes to queue, advances watermark
- `ig_posts_gld_backfill` deleted (batch API deferred)
- Gold checks now target `gold_analyses` (instagram domain only); `schema_version` check removed
- `analytics_views` LEFT JOINs `gold_analyses` (not `gold_ig_analyses`) with domain filter

### Schedule Change
- `weekly_medallion` → `daily_medallion`: cron `0 3 * * *` (bronze→silver→enqueue is sub-second)

### Key Design Decisions
- `hashlib.sha256` for prompt_hash (Python `hash()` is non-deterministic across processes)
- `reschedule()` preserves attempts (quota exhaustion is global, not per-item)
- Worker owns dead_letter transition (checks `attempts >= MAX_ATTEMPTS` after `fail()`)
- `NOT EXISTS in gold_analyses` guard in enqueue query prevents re-processing
- All timestamps Python-generated ISO 8601 UTC — no `DEFAULT (datetime('now'))`
- No `priority` column (FIFO by `created_at`); no `domain` in silver (table name IS the domain)

### Tests
- 87 tests passing, 3 skipped (1 operational — needs pipeline run to create `gold_analyses` in real DB)
- New: queue operation tests (enqueue, claim, complete, fail, reschedule, depth, max attempts)
- New: enqueue asset tests (writes to queue, skips already-enriched, skips empty captions)
- Updated: integration, E2E, and snapshot tests for new architecture

## 0.2.2 (2026-07-01) — State Readiness Validation

### Operational Tests
- New `tests/operational/` package: schema contract catalog + three readiness tests
- `test_all_expected_tables_exist` — asserts every expected table exists in state DB
- `test_columns_match` — per-table column existence + type matching (extra columns tolerated)
- `test_view_is_queryable` — each expected view can be `SELECT *` queried without error
- `state_db` fixture in `conftest.py` — skips gracefully when `data/state.duckdb` absent
- Central schema catalog in `expected_schema.py` — contract for 6 tables, 1 view
- Drift detection proven: missing column, type mismatch, missing table all produce clear failures
- 95 tests total (87 + 8), 2 skipped (live API keys)

## 0.2.1 (2026-07-01) — Test Hardening

### Test Architecture
- Restructured: `tests/{unit,integration,e2e,fixtures,data}/` with domain subdirectories
- 87 tests across unit (per-asset), integration (cross-boundary), E2E (full pipeline)
- 2 skipped (require live API keys)
- DuckDB uses `:memory:` in all fixtures; no tempdir cleanup
- GeminiResource mocked at resource level via `patch.object`, not `google.genai.Client`
- All imports at module level; no `sys.path.insert` hacks

### Domain-Scoped Factories
- `make_ig_bronze_row` / `write_ig_bronze` produce rows matching real Apify schema (37 columns)
- Schema loaded from real bronze Parquet at import time; synthetic rows cast to match exactly
- Factories moved to `tests/fixtures/ig_bronze_factories.py` — ready for TikTok/YouTube expansion

### Bronze Schema Fix
- Factory now mirrors all 37 real Apify columns (was 26, missing `alt`, `audioUrl`, `childPosts`, etc.)
- `write_ig_bronze` casts output to production schema — no test can silently diverge from ingestion

### Dagster Asset Checks
- 12 asset checks defined across instagram and serving domains
- Wired into `Definitions` with unit tests per check

### Silver Edge Case
- Empty bronze DataFrames with correct schema now handled gracefully (was Arrow schema-less crash)

# Changelog

## 0.1.0 (2026-06-30) — Foundation

### Scaffold
- Dagster + Parquet + DuckDB medallion lakehouse platform
- `pyproject.toml` with dagster, dagster-duckdb, dagster-webserver, duckdb, httpx, google-genai
- `dagster.yaml`: telemetry off, `duckdb_state` concurrency pool (limit 1, op granularity)
- Skeleton package: `src/datalake/{definitions,resources,lake,config,schedules}.py`
- Asset stub directories: `defs/{bronze,silver,gold,serving}/__init__.py`

### Infrastructure
- Git repo at `github.com/evanokeefe39/datalake` (public)
- Protected `main` branch: linear history, squash merges only, CI must pass
- GitHub Actions: ruff, pytest, dagster definitions validate
- PR template with checklist
- `.env.example` for required tokens

### Config
- `ScrapeConfig`: Apify Instagram scrape parameters (urls, results_limit, results_type)
- `GoldConfig`: Gemini enrichment parameters (post_ids filter)
- `ApifyResource`, `GeminiResource`: env-token injection
- Parquet lake path helpers (`lake.py`): env-overridable, auto-creating directories

### Documentation
- README with architecture overview and git workflow
- AGENTS.md for Claude operating context
- ISSUES.md for local issue tracking
- CONTRIBUTING.md with setup and test instructions

## 0.2.0 (2026-07-01) — Watermark + Dead Letter Refactoring

### Schema Migration
- `scripts/migrate_to_v2.py`: standalone, idempotent migration from Phase 1-4 schema
- New `watermarks` table (name PK, timestamp, config_hash) replaces per-pipeline progress tables
- New `dead_letter` table (post_id+domain PK) separates failures from main enrichment table
- `silver_posts` → `silver_ig_posts` with `silvered_at` → `processed_on`
- `gold_analyses` → `gold_ig_analyses`, dropped `status`/`error`/`attempts` columns
- `silver_progress` → `silver_ig_progress`, `silver_watermark` removed
- All migration steps create new tables before dropping old ones (DuckDB FK-safe)

### Silver Asset Refactor
- `CREATE TABLE` uses `silver_ig_posts` and `silver_ig_progress` names
- Watermark reads from `watermarks WHERE name = 'silver_ig'` instead of `silver_watermark`
- `processed_on` only stamped on net-new `post_ids` (existing rows keep their timestamp)
- `_SILVER_COLUMNS` updated: `silvered_at` → `processed_on`

### Gold Asset Refactor
- Discovery switched from LEFT JOIN gap detection to watermark-based (`processed_on > watermark_timestamp`)
- Successful results → `gold_ig_analyses` (no status/error/attempts columns)
- Empty captions and API failures → `dead_letter` table
- Watermark advanced after each batch with `config_hash` for deferred auto-reset
- Return DataFrame schema: only `post_id`, `schema_version`, `result_json`, `analysed_at`

### Serving Layer Updates
- `dim_profile` reads from `silver_ig_posts`, `analytics_views` joins `silver_ig_posts` + `gold_ig_analyses`
- View uses `processed_on` (not `silvered_at`); `gold_status`/`gold_error` columns removed
- Comment added pointing consumers to `dead_letter` for failure visibility

### Test Updates
- All existing tests updated for new table/column names
- New: `test_gold_returns_only_completed`, `test_gold_reset_via_watermark_delete`,
  `test_watermarks_generic`
- 36 tests passing (bronze + silver + gold + serving + migration)
