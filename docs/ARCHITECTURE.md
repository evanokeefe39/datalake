# Architecture — Medallion Lakehouse with Async Enrichment

## Medallion layers

```
Bronze ──→ Silver ──→ Gold ──→ Serving
(Parquet)   (Parquet   (DuckDB)   (DuckDB views
            + DuckDB)              + tables)
```

| Layer | Storage | Writer | State tracking |
|---|---|---|---|
| Bronze | Parquet (`data/lake/bronze/`) | Polars (direct write) | None — file-based |
| Silver | Parquet (`data/lake/silver/`) | PolarsIOManager | DuckDB `silver_ig_posts` + watermarks |
| Enqueue | SQLite (`data/ops.sqlite`) | `ig_posts_gen_batches` | `batch_jobs` + `batch_items` |
| Gold | DuckDB table | Standalone worker | `gold_analyses` (AssetSpec, externally materialized) |
| Serving | DuckDB views + tables | DuckDB | `dim_profile` (SCD2), `dim_date`, analytics views |

## Storage split

Three storage backends, each chosen for its access pattern:

- **Parquet lake** (`data/lake/`) — bulk data, lock-free parallel writes, immutable files. Polars handles all I/O.
- **DuckDB** (`data/state.duckdb`) — authoritative current state, watermarks, SCD2 dims, views. OLAP: scans and aggregations.
- **SQLite** (`data/ops.sqlite`) — operational coordination: batch jobs, batch items, media metadata cache, dead letter. OLTP: point lookups, frequent updates.

## Engine boundary

| Engine | Responsibility |
|---|---|
| Polars | Parquet I/O (read/write NDJSON and Parquet), column mapping, dedup |
| DuckDB | SQL transforms (DISTINCT ON dedup, watermark queries, SCD2, views) |
| Arrow | Zero-copy interchange between Polars and DuckDB (`to_arrow()` / `from_arrow()`) |

## Domain-based structure

```
src/datalake/defs/
├── common/          # PolarsIOManager, ApifyResource, GeminiResource, SQLiteResource, lake.py, schedules.py
├── enrichment/      # batch.py (batch coordination), assets.py (gold_analyses AssetSpec + checks), prompts.py
├── instagram/       # ig_posts_raw, ig_posts_slv, ig_posts_gen_batches, config
└── serving/         # dim_profile, dim_date, analytics views (cross-domain)
```

Domains are independent data sources. `serving` is the only cross-domain module — it joins silver + gold + dims across all sources.

## Batch coordination

Batch processing replaces the old per-item enrichment queue. Operational state lives in `ops.sqlite`:

### `batch_jobs`
```sql
CREATE TABLE batch_jobs (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    status     TEXT NOT NULL DEFAULT 'pending',  -- pending | processing | complete
    domain     TEXT NOT NULL DEFAULT 'instagram',
    created_at TEXT NOT NULL,
    started_at TEXT,
    completed_at TEXT
);
```

### `batch_items`
```sql
CREATE TABLE batch_items (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id       INTEGER NOT NULL REFERENCES batch_jobs(id),
    post_id      TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'pending',  -- pending | processing | complete | failed | dead
    attempts     INTEGER NOT NULL DEFAULT 0,
    last_error   TEXT,
    scheduled_for TEXT,
    completed_at TEXT
);
```

### Lifecycle

1. `ig_posts_gen_batches` creates a batch via `create_batch()`, inserts post IDs as items.
2. Worker calls `claim_batch()` to claim the oldest pending batch (status → `processing`).
3. Worker calls `claim_pending_items()` to claim up to 5 items at a time.
4. Per-item: `complete_item()` on success, `fail_item()` on failure (retry with exponential backoff, `MAX_ATTEMPTS=5`). Terminal failures route to `dead_letter`.
5. Worker calls `mark_complete()` when all items are done or dead.

## REST materialization

The enrichment worker runs as a standalone process (not inside a Dagster run). It reports materializations to Dagster via the canonical REST endpoint:

```
POST http://localhost:3000/report_asset_materialization/
```

This decouples the worker lifecycle from Dagster runs:

| Advantage | Why it matters |
|---|---|
| Worker survives Dagster restarts | Data is persisted in DuckDB; materialization is reported when Dagster comes back |
| No run slot held open | Worker can run for hours processing batches without blocking a Dagster run |
| Automatic retry via re-claiming | Stale batches are re-claimed by the next worker invocation |
| Crash recovery | Items in `processing` state are re-claimed by the stale reaper |

### Why not PipesSubprocessClient?

Pipes ties enrichment to a Dagster run lifecycle — the run blocks until the subprocess exits. This is correct for short-lived jobs (dbt, Spark, model training) but wrong for long-running, rate-limited external API work. Gemini enrichment runs for hours, hits 429s with RPD resets at 08:00 UTC, and must survive crashes. REST materialization is the right pattern.

## Lineage

```
ig_posts_slv ─┬─→ gold_analyses ─┬─→ v_post_detail ─┬─→ v_signal
               │                   │                   ├─→ v_quality_trend
               ├─→ dim_profile ────┤                   ├─→ v_creator_quality
               │                   │                   ├─→ v_rising_creators
               │                   │                   ├─→ v_domain_coverage
               └───────────────────┤                   ├─→ v_engagement_outliers
                                   │                   ├─→ v_outlier_posts
                     dim_date ─────┘                   └─→ v_creator_outlier_rate
```

`ig_posts_gen_batches` is a coordination asset — it creates batches in SQLite. There is no formal Dagster data dependency from `gold_analyses` to `ig_posts_gen_batches` because the worker reads from SQLite (not from Dagster IOManager output). Both depend on `ig_posts_slv`.

## Watermarks

A single `watermarks` table in DuckDB tracks progress for every pipeline:

```sql
CREATE TABLE watermarks (name TEXT PRIMARY KEY, timestamp TIMESTAMP NOT NULL);
```

| Pipeline | Watermark name | What it tracks |
|---|---|---|
| Silver | `silver_ig` | Last `processed_on` processed |
| Enqueue | `gold_ig` | Last `processed_on` enqueued |

Reset a watermark by deleting its row: `DELETE FROM watermarks WHERE name = 'gold_ig'`. The next run reprocesses everything.

## `processed_on` semantics

Set only when a post first appears in silver. Never updated on subsequent runs, even when engagement metrics change. This enables true incremental gold processing — only new posts are enqueued each run.

## Dead letter

Terminal failures (items that exhausted `MAX_ATTEMPTS=5` retries) are written to `dead_letter` in `ops.sqlite`:

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

Manual triage only. No automatic retry worker. This keeps `gold_analyses` pure — only completed enrichments, never partial failures.
