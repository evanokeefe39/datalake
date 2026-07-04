# Operating Guide

## Prerequisites

- `.env` at repo root with `APIFY_API_TOKEN`, `GEMINI_API_KEY`, and optional `GEMINI_TIER` (`free`/`tier1`/`tier2`)
- `data/state.duckdb` and `data/ops.sqlite` exist (created automatically on first run)
- DAGSTER_HOME set to `data/dagster_home` (set in `.env`)

## Pipeline CLI

The pipeline is operated via ``python -m datalake.cli`` (or the thin wrapper
``scripts/run_pipeline.py``). Three subcommands:

```bash
uv run python -m datalake.cli run                      # incremental pipeline
uv run python -m datalake.cli run --dry-run             # state only
uv run python -m datalake.cli run --reset-watermarks    # full re-scan then run
uv run python -m datalake.cli run --reset-watermarks --date 2026-06-15
uv run python -m datalake.cli run --update-stale        # re-process stale analyses
uv run python -m datalake.cli batches                  # list batch state
uv run python -m datalake.cli batches --reset           # clear all batches
uv run python -m datalake.cli watermarks               # list watermarks
uv run python -m datalake.cli watermarks --reset        # reset to epoch-safe date
uv run python -m datalake.cli watermarks --reset --date 2026-06-15
```

Steps:
1. Silver — reads new bronze files, deduplicates, writes to ``data/lake/silver/`` and DuckDB
2. Batches — finds unenriched silver posts, creates a batch in ``ops.sqlite``
3. Serving — materializes ``dim_date``, ``dim_profile`` (SCD2), and ``v_post_detail`` (cascades to all downstream views)

## Enrichment worker

The worker runs independently — it reads from `batch_items` in SQLite, calls Gemini, and writes to `gold_analyses` in DuckDB.

### Process next pending batch

```bash
uv run python scripts/enrichment_worker.py
```

Claims the oldest pending batch, processes items with per-item retry (exponential backoff, `MAX_ATTEMPTS=5`), routes terminal failures to `dead_letter`, and POSTs materialization events to Dagster.

### Process a specific batch

```bash
uv run python scripts/enrichment_worker.py --batch-id 3
```

### Dry run (inspect batch state)

```bash
uv run python scripts/enrichment_worker.py --dry-run
```

### Rate limiting

The worker handles two kinds of Gemini 429:

| Type | Behavior |
|---|---|
| `rate_limit_exceeded` (RPM/TPM burst) | Exponential backoff: `2^attempt + random(0,1)` seconds |
| `insufficient_quota` (RPD exhausted) | Stops retrying, waits until 08:00 UTC (next daily quota reset) |

### Stale item recovery

If the worker crashes mid-batch, items stuck in `processing` state are reclaimed on the next run. The stale reaper looks for items where `status = 'processing'` and `started_at` is older than 30 minutes.

## Watermark resets

Inspect or reset watermarks via the ``watermarks`` subcommand:

```bash
uv run python -m datalake.cli watermarks               # list current watermarks
uv run python -m datalake.cli watermarks --reset        # reset to epoch-safe date
uv run python -m datalake.cli watermarks --reset --date 2026-06-15
```

Then run the pipeline normally:

```bash
uv run python -m datalake.cli run
```

To reset only batches (clear stale batch_jobs/batch_items):

```bash
uv run python -m datalake.cli batches --reset
```

## Dead letter triage

Check what's in dead letter:

```sql
-- In ops.sqlite
SELECT post_id, domain, error, attempts, failed_at
FROM dead_letter
ORDER BY failed_at DESC;
```

Common failure modes:

| Error pattern | Likely cause | Fix |
|---|---|---|
| `RESOURCE_EXHAUSTED` / quota | RPD exhausted | Wait until 08:00 UTC, re-run worker |
| `File API` / `upload` / `download` | Media URL expired or inaccessible | Check if media is still available; may be permanent |
| `SAFETY` | Content filtered by Gemini | Permanent — post cannot be enriched |
| `Invalid request` / `400` | Payload too large (long caption or video) | Trim caption or switch to `media_resolution='low'` |

To retry a dead letter item, re-enqueue it manually:

```python
# In Python
from datalake.defs.enrichment.batch import create_batch
from datalake.defs.common.resources import SQLiteResource
ops = SQLiteResource()
create_batch(ops, domain="instagram", post_ids=["POST_ID_HERE"])
```

Then run the worker normally.

## Stale analysis re-processing

When the enrichment prompt or model changes, existing `gold_analyses` rows have stale `prompt_hash`. To re-process them:

```bash
uv run python -m datalake.cli run --update-stale
```

This queries `gold_analyses WHERE prompt_hash IS NULL OR prompt_hash != CURRENT_PROMPT_HASH`, creates a batch, and the worker picks it up and UPSERTs fresh analyses.

To check how many stale rows exist without re-processing:

```sql
SELECT COUNT(*) FROM gold_analyses
WHERE prompt_hash IS NULL OR prompt_hash != '<CURRENT_PROMPT_HASH>';
```

Current prompt hash is available in `src/datalake/defs/enrichment/prompts.py` as `CURRENT_PROMPT_HASH`.

## Schema drift

The canonical schema catalog is ``src/datalake/defs/common/schemas.py``.
``tests/operational/expected_schema.py`` re-exports from it for test compatibility.
The readiness test checks it against the running databases:

```bash
uv run pytest tests/operational/test_state_compatibility.py -v
```

If the test fails with a "run the pipeline or migration" message, it means a table
or column exists in the catalog but not in the running database. Run the pipeline
or apply the relevant migration.

If it fails with a "stale table" message, a table in the database was renamed or
dropped in the catalog. Apply ``scripts/migrate_schema_drift.py``:

```bash
uv run python scripts/migrate_schema_drift.py
```

## Data migrations

| Script | Purpose |
|---|---|
| ``scripts/migrate_owner_username.py`` | Backfill null ``owner_username`` from bronze ``username`` fallback. Idempotent, ``--dry-run`` supported. |
| ``scripts/migrate_schema_drift.py`` | Apply schema migrations: rename tables, move data between DBs, drop vestigial tables. |
| ``scripts/migrate_to_v2.py`` | One-shot migration from Phase 1-4 schema to v2 domain-scoped tables. |
| ``scripts/migrate_from_ig_pipeline.py`` | Import bronze Parquet from legacy ig-pipeline repo. |

## Dagster UI

With `DAGSTER_HOME` set to `data/dagster_home`:

```bash
dagster dev
```

Opens at `http://localhost:3000`. Assets appear in the global asset graph. Materialization events from the standalone worker appear as external materializations on `gold_analyses`.

## Dagster CLI (headless)

Materialize a specific asset without the UI:

```bash
dagster asset materialize -m datalake --select ig_posts_slv
```

This runs the asset in-process and exits. Useful for debugging.

## Common troubleshooting

### "No pending batches" from worker

The worker claims the oldest pending batch. If none exist, run the pipeline:

```bash
uv run python scripts/run_pipeline.py
```

### Worker can't find silver posts

The worker reads `silver_ig_posts` from DuckDB. Make sure silver has been materialized:

```bash
dagster asset materialize -m datalake --select ig_posts_slv
```

### DuckDB lock errors

DuckDB allows one writer at a time. Stop `dagster dev` before running CLI commands that write to DuckDB, or vice versa.

### Bronze files not being picked up

Silver uses a watermark (`silver_ig`) to track which runs have been processed. If bronze files were written after the last silver run, just run the pipeline again. If the watermark is ahead (files were written, then watermark advanced past them without processing), reset it:

```sql
DELETE FROM watermarks WHERE name = 'silver_ig';
```

## Environment variables

| Variable | Default | Used by |
|---|---|---|
| `APIFY_API_TOKEN` | — | ApifyResource (bronze scraping) |
| `GEMINI_API_KEY` | — | GeminiResource (enrichment) |
| `GEMINI_TIER` | `free` | Rate-limit behavior and feature gates |
| `IG_DATA_DIR` | `data` | Root data directory |
| `IG_BRONZE_DIR` | `data/lake/bronze` | Bronze Parquet files |
| `IG_SILVER_DIR` | `data/lake/silver` | Silver Parquet files |
| `IG_DB_PATH` | `data/state.duckdb` | DuckDB path |
| `DAGSTER_HOME` | `data/dagster_home` | Dagster instance directory |
