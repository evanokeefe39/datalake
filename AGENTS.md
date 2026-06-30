# datalake — Agent operating context

This repo is designed to be operated by Claude. Keep this file current — Claude reads it on every session.

## Key rules

- Never use `pip`. Always use `uv` for Python package management.
- Work happens on `feat/*`, `fix/*`, `chore/*` branches; squash-merge to `main` via PR.
- Conventional commits only: `type(scope): summary`.
- No direct pushes to `main`.

## Architecture

**Medallion:** bronze → silver → gold → serving.

| Layer | Storage | Writer |
|-------|---------|--------|
| Bronze | Parquet (`data/lake/bronze/`) | Apify scraper asset (lock-free) |
| Silver | Parquet (`data/lake/silver/`) | Dedup asset (DuckDB state pool) |
| Gold | Parquet (`data/lake/gold/`) | Gemini enrichment asset (DuckDB state pool) |
| Serving | DuckDB views (`data/state.duckdb`) | View refresh asset (DuckDB state pool) |

**Concurrency:** `duckdb_state` pool (limit 1, op granularity) serializes all DuckDB writes. Parquet writes are lock-free.

**Env vars** (set in `.env`):

| Variable | Default | Used by |
|----------|---------|---------|
| `APIFY_API_TOKEN` | — | `ApifyResource` |
| `GEMINI_API_KEY` | — | `GeminiResource` |
| `IG_DATA_DIR` | `data` | `lake.py` root path |
| `IG_BRONZE_DIR` | `data/lake/bronze` | Bronze asset |
| `IG_SILVER_DIR` | `data/lake/silver` | Silver asset |
| `IG_GOLD_DIR` | `data/lake/gold` | Gold asset |
| `IG_DB_PATH` | `data/state.duckdb` | DuckDB resource |

## Dagster

- `dg dev` → localhost:3000
- Definitions module: `src/datalake/definitions.py`
- Assets register in `src/datalake/defs/{bronze,silver,gold,serving}/`
- Config schemas in `src/datalake/config.py`
- Resources in `src/datalake/resources.py`
- Schedules/sensors in `src/datalake/schedules.py`
- Telemetry disabled (`dagster.yaml`)

## IG pipeline library

The old `ig_pipeline` library at `~/repos/ig-pipeline` is imported as a thin wrapper. Ingestor wrappers live at `ingestors/ig_pipeline/` — they import functions from the old repo via `sys.path`.

## Test conventions

- `uv run pytest tests/ -v`
- In-memory DuckDB (`:memory:`) via dependency injection
- Parquet tests use `tmp_path`
- One test per behavioral contract, one per edge case

## Decision log

- **Parquet for bulk data:** lock-free parallel writes. DuckDB for state (SCD2, progress tracking). No single DB bottleneck.
- **DuckDB state pool:** op-granularity concurrency limit of 1 — DuckDB is single-writer. Parquet assets don't use this pool.
- **Dynamic partitions for bronze:** Apify run IDs as partition keys. Sensor auto-creates partitions from `list_runs()`.
- **No GitHub Issues:** local `ISSUES.md` is the issue tracker.
