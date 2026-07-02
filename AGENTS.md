# datalake — Agent operating context

This repo is operated by Claude. Keep this file current — Claude reads it on every session.

## Key rules

- Never use `pip`. Always use `uv` for Python package management.
- Work on `feat/*`, `fix/*`, `chore/*` branches; squash-merge to `main` via PR.
- Conventional commits only: `type(scope): summary`.
- No direct pushes to `main`.
- Never use PowerShell.

## Architecture
**Medallion lakehouse with async enrichment queue:**

| Layer | Storage | Writer | State tracking |
|-------|---------|--------|----------------|
| Bronze | Parquet (`data/lake/bronze/`) | Polars (direct write) | None — file-based |
| Silver | Parquet (`data/lake/silver/`) | PolarsIOManager | DuckDB `silver_ig_posts` + watermarks |
| Enqueue | SQLite (`data/ops.sqlite`) | `ig_posts_gld_enqueue` | `enrichment_queue` (pending/processing/complete) |
| Gold | DuckDB table | `enrichment_worker` op | `gold_analyses` (AssetSpec, partial materializations) |
| Serving | DuckDB views + tables | DuckDB | `dim_profile` (SCD2), `analytics_views` (VIEW) |

**SQLite for operational state, DuckDB for analytical state:**
- `ops.sqlite` — enrichment queue, media cache, dead_letter (OLTP: point lookups, frequent updates)
- `state.duckdb` — silver tables, gold_analyses, watermarks, serving dims/views (OLAP: scans, aggregations)

**Domain-based structure, not layer-based:**

```
src/datalake/defs/
├── common/          # PolarsIOManager, ApifyResource, GeminiResource, SQLiteResource, lake.py, schedules.py
├── enrichment/      # queue.py, worker.py, sensor.py, media_cache.py, assets.py, prompts.py
├── instagram/       # ig_posts_raw, ig_posts_slv, ig_posts_gld_enqueue, ScrapeConfig
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

| Database | Table | Purpose |
|---|---|---|
| DuckDB | `silver_ig_posts` | Deduped, normalized Instagram posts |
| DuckDB | `silver_ig_progress` | Per-dataset processing audit log |
| DuckDB | `gold_analyses` | Completed enrichments with domain PK (`post_id`, `domain`) and `prompt_hash` |
| DuckDB | `dim_profile` | SCD2 profile dimension (cross-domain, `channel` column) |
| DuckDB | `watermarks` | Generic progress tracking for any pipeline (`name`, `timestamp`) |
| SQLite | `enrichment_queue` | Work queue: post_id, domain, status, attempts, scheduled_for |
| SQLite | `media_metadata` | URL hash → Gemini File API URI cache |
| SQLite | `dead_letter` | Failed enrichments — moved from DuckDB, manual triage only |

Parquet file names match asset keys, not table names — the PolarsIOManager uses `asset_key.path[-1]`.

## Watermarks pattern

A single `watermarks` table replaces per-pipeline progress tables. Any pipeline stamps its row:

```sql
CREATE TABLE watermarks (name TEXT PRIMARY KEY, timestamp TIMESTAMP NOT NULL);
```

- Silver reads/writes `watermarks WHERE name = 'silver_ig'`
## Dead letter pattern

Failures from Gemini enrichment go to `ops.sqlite` (not DuckDB — moved with the queue architecture).
This keeps `gold_analyses` pure (only completed enrichments) and provides a clean triage surface:

```sql
CREATE TABLE dead_letter (
    post_id   TEXT NOT NULL,
    domain    TEXT NOT NULL DEFAULT 'instagram',
    error     TEXT,
    attempts  INTEGER NOT NULL DEFAULT 0,
    failed_at TEXT NOT NULL,
    PRIMARY KEY (post_id, domain)
);
```

Items arrive here when the worker exhausts retries (`attempts >= MAX_ATTEMPTS`). Manual triage only —
no automatic retry worker. The enrichment queue handles retries via `scheduled_for` and
exponential backoff; dead_letter is the terminal state.

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
| Status columns on data tables (`gold_analyses.status`) | Mixed concerns: results and error tracking in one table. Required `WHERE status = 'completed'` on every query. | `dead_letter` table — results go to `gold_analyses`, failures to `dead_letter` |
| Single-purpose watermark tables (`silver_watermark`) | Doesn't scale to N pipelines. Each new pipeline adds a new table. | Generic `watermarks(name, timestamp)` table — any pipeline uses it by name |
| LEFT JOIN gap detection for pending posts | Complex query, no clean reset mechanism. Resetting required mass UPDATE. | Watermark-based: `WHERE processed_on > watermark_timestamp`. Reset = DELETE row. |
| Re-stamping timestamps on every run (`silvered_at`) | Destroyed "first seen" semantics. Gold couldn't do incremental processing. | `processed_on` set on INSERT only, never updated |
| Layer-based directory structure (`defs/{bronze,silver,gold}/`) | Doesn't scale to multiple data sources. Forces unrelated code together. | Domain-based (`defs/instagram/`, `defs/serving/`) |
| Modeling against test data without verifying real data | Phase 2 silver was built against a 3-row test fixture when real data had 28 columns with nested types. | Gate: read ONE real input file and display schema before writing any asset that reads from disk |

## Dagster

- `dagster dev` → localhost:3000 (or `dg dev` if installed)
- Assets: `defs/instagram/assets.py` (domain-scoped), `defs/serving/assets.py` (cross-domain), `defs/enrichment/` (queue + worker + sensor)
- Resources: `defs/common/resources.py` (PolarsIOManager, ApifyResource, GeminiResource, SQLiteResource)
- Schedules: `defs/common/schedules.py` (daily_medallion, cron `0 3 * * *`)
- Config schemas: ``defs/instagram/config.py`` (ScrapeConfig, GoldConfig, GeminiTierConfig)
- Path helpers: `defs/common/lake.py` (env-overridable, auto-creating directories)
- Telemetry disabled (`dagster.yaml`)
- `[tool.dagster]` in `pyproject.toml` enables auto-discovery

## Bronze asset (ig_posts_raw)

- **Manual trigger only** — not scheduled. User provides `ScrapeConfig` via Dagster launchpad.
- **Apify flow:** trigger_run → poll_run → stream_dataset (NDJSON) → Polars read_ndjson → write_parquet
## Gold assets (queue-based, async)

### ig_posts_gld_enqueue (enqueue only, no Gemini)

- **Trigger:** downstream of silver (`deps=["ig_posts_slv"]`), plus daily schedule
- **Discovery:** `WHERE processed_on > watermark('gold_ig')` with `NOT EXISTS in gold_analyses` guard
- **Action:** writes to `enrichment_queue` in ops.sqlite (sub-millisecond per row, no API calls)
- **Watermark:** advances to `MAX(processed_on)` after batch enqueue
- **Empty captions:** skipped (worker handles them — completes without Gemini call)
- **Re-enrichment:** bypasses watermark; posts with stale `prompt_hash` re-enqueued directly

### enrichment_worker (async, via sensor)

- **Trigger:** `enrichment_sensor` polls queue every 30s, claims up to 5 items, emits RunRequest
- **Per-item processing:** reads silver → calls Gemini → writes `gold_analyses`
- **Rate limiting:** per-item backoff via `scheduled_for`; quota exhaustion reschedules all without burning attempts
- **Dead letter:** worker checks `attempts >= MAX_ATTEMPTS` after `fail()` and moves to dead_letter
- **Partial materialization:** emits `AssetMaterialization` events against `gold_analyses` AssetSpec
- **Model:** gemini-3.1-flash-lite, temperature=0.2, JSON response
- **429 classification:** `reschedule()` (global quota, preserves attempts) vs `fail()` (per-item burst, increments attempts)
- **Stale reaper:** inline in `claim()` transaction — no separate schedule; resets orphaned `processing` items after 10 minutes

### Why queue-based (not synchronous)

| Issue (old) | Fix (new) |
|---|---|
| Asset blocks on API latency (1+ hour materialization) | Queue decouples — materialization is sub-second |
| One 429 kills the batch | Per-item backpressure; worker continues past failures |
| No per-item rate limiting | `scheduled_for` column with exponential backoff |
| Crash → partial writes | Queue is durable; stale reaper recovers orphaned items |
| Domain coupling (TikTok = copy-paste) | Worker dispatches by `domain` column; same queue, same worker |
## Serving layer

- `dim_profile`: SCD2 profile dimension. Reads DISTINCT profiles from `silver_ig_posts`. Closes old rows on username change, inserts new rows. `channel = 'instagram'`.
- `analytics_views`: CREATE OR REPLACE VIEW joining `silver_ig_posts` + `gold_analyses` + `dim_profile` (current rows only). LEFT JOIN includes domain filter (`ga.domain = 'instagram'`). Query surface for dashboards.
- Both are in `defs/serving/assets.py`, group_name="serving"


## Gemini API rate limits

Google does not publish exact per-model RPM/TPM/RPD numbers — check your
live limits at `https://aistudio.google.com/rate-limit`. Approximate free-tier
limits for Flash/Flash-Lite models (mid-2026):

| Model | RPM | RPD | TPM |
|---|---|---|---|
| Gemini 2.5 Flash | ~10–15 | ~500–1,500 | up to 1,000,000 |
| Gemini 2.5 Flash-Lite | ~15–30 | ~500–1,500 | 250,000–1,000,000 |
| Gemini 3 Flash | ~10 | ~500–1,500 | ~250,000 |

Limits are **per Google Cloud project**, not per API key. RPD resets at
**midnight Pacific time** (08:00 UTC), not a rolling 24h window. Free tier
has no spend-based rate limit (that's Tier 1+ only). Pro models lost their
free tier in April 2026.

**RPD varies by account.** Google does not guarantee these numbers and
revises them without notice (50-80% cut in December 2025; further reductions
through mid-2026). Developers report anywhere from 500 to 1,500 RPD on
free tier. Check your live limits at ``https://aistudio.google.com/rate-limit``.

### Two kinds of 429

Both return HTTP 429 ``RESOURCE_EXHAUSTED``. The Gemini API response body
carries structured info (retry hints in ``error.details``) that can
distinguish subtypes:

| Subtype | Meaning | Fix |
|---|---|---|
| ``rate_limit_exceeded`` | RPM/TPM burst — too fast. Response carries ~10–20s retry hint. | **Jitter + exponential backoff.** Retry loop uses ``(2^N) + random(0,1)`` seconds. |
| ``insufficient_quota`` | Daily RPD spent — out of requests for the day. | **Stop retrying.** Wait until 08:00 UTC. Switch projects or upgrade tier. |

The ``_is_quota_exhausted()`` and ``_is_rate_limited()`` helpers in the
``enrichment_worker`` op inspect ``google.genai.errors.APIError`` attributes
(``code``, ``message``, ``details``) to distinguish subtypes. If the SDK
error isn't parseable it falls back to substring matching on quota-related keywords.

### Video: TPM is the bottleneck

Video is token-intensive. At default ``media_resolution``:

| Per second of video | Tokens |
|---|---|
| Frames (1 FPS, 258/frame) | 258 |
| Audio (32/sec) | 32 |
| **Total** | **~290/sec** |

At low resolution (``media_resolution='low'``): ~98 tokens/sec (66/frame).

| Video length | Default tokens | Low-res tokens |
|---|---|---|
| 1 minute | ~17,400 | ~5,880 |
| 10 minutes | ~174,000 | ~58,800 |
| 1 hour | ~1,044,000 | ~352,800 |

A single 10-minute video at default resolution eats ~70% of the low-end free
tier TPM (~250k). A 20-minute video exceeds it outright. To avoid this:

- **Estimate tokens before the call** — use file metadata (duration from
  ffprobe or container headers) rather than ``count_tokens()`` which
  requires an API round-trip. Formula: ``tokens ≈ duration_seconds * 290``
  (default) or ``duration_seconds * 98`` (low res).
- **Use ``media_resolution='low'``** when fine visual detail isn't needed —
  cuts token cost ~3×.
- **Send shorter clips** — trim to the relevant segment.
- **Use the File API** for videos >100MB or >1 minute. Free tier upload limit
  is 2GB; paid is 20GB.

### Billing trap

Enabling billing on a project **deletes the free tier entirely** — every call
becomes billable from the first token. This differs from most Google Cloud
services (BigQuery, Cloud Storage) where free tier persists alongside billing.
Workaround: use separate projects for free-tier evaluation and paid production.

## Gemini tier strategy

### Tier decision matrix

| | Free | Tier 1 | Tier 2 |
|---|---|---|---|
| **Cost** | $0 | $0 base, pay per token | $0 base, pay per token |
| **Entry** | Create project | Link billing account | $100 cumulative spend + 3 days |
| **RPD** | ~500 | Higher (check dashboard) | Higher still |
| **Batch tokens** | Limited | **10M** (flash-lite) | **500M** (flash-lite) |
| **File API upload** | 2 GB | 20 GB | 20 GB |
| **Spend cap** | None | $250/mo | $2,000/mo |
| **Spend rate limit** | None | $10/10 min | $200/10 min |
| **Data training** | Yes | **No** | **No** |
| **Pro models** | No | Yes | Yes |

### Approach per tier

**Free — Evaluation only.** ``enrichment_worker`` processes via queue with
per-item backpressure. Not for production volume.

**Tier 1 — Interactive via queue.** Queue-based enrichment handles routine
volume with per-item rate limiting. Batch API deferred; re-introduce as a
worker variant when cost savings justify the complexity.

**Tier 2 — Batch-first (future).** When batch API is re-introduced, all
enrichment via batch. Interactive asset for ad-hoc single-post only.

### Trigger points

| When you see... | Upgrade to... |
|---|---|
| Dead letter filling with 429s daily | **Tier 1** |
| Need to clear the backlog this week | **Tier 1** |
| Don't want Google training on your data | **Tier 1** |
| Batch job exceeds 10M token limit | **Tier 2** |
| Weekly volume >1,000 posts steady-state | **Tier 2** |
| Adding video enrichment | **Tier 2** (immediately) |
| $250/mo spend cap exhausted | **Tier 2** |
| $2,000/mo spend cap exhausted | **Tier 3** |

### Video scaling

Video adds two bottlenecks beyond text: upload time (File API, 5–15s per
video) and token volume (~17,400 tokens per minute of video at default
resolution). Sequential processing is impractical above ~100 videos/week.
Batch API with concurrent upload workers is required at any video scale.
Tier 2's 500M batch capacity and 20GB File API limit remove the practical
ceilings for video enrichment.


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
