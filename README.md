# Datum

Dagster + Parquet + DuckDB medallion lakehouse platform.

First workload: Instagram pipeline (migrated from `~/repos/ig-pipeline`).

## Quick start

```bash
uv sync
cp .env.example .env  # add APIFY_API_TOKEN, GEMINI_API_KEY
uv run dg dev
```

Open http://localhost:3000.

## Structure

```
src/datalake/defs/
├── bronze/     # Apify → raw .jsonl → typed .parquet
├── silver/     # Dedup + clean → lake/silver/*.parquet
├── gold/       # Gemini enrichment → lake/gold/*.parquet
└── serving/    # dim_time, dim_profile, analytics_views
```

## History

Built 2026-06-30 as the production platform for the Instagram pipeline,
superseding `~/repos/ig-pipeline`.
