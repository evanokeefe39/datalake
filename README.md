# Datalake

Dagster + Parquet + DuckDB medallion lakehouse platform.

First workload: Instagram pipeline (migrated from `~/repos/ig-pipeline`).

[![CI](https://github.com/evanokeefe39/datalake/actions/workflows/ci.yml/badge.svg)](https://github.com/evanokeefe39/datalake/actions/workflows/ci.yml)

## Quick start

```bash
git clone https://github.com/evanokeefe39/datalake.git
cd datalake
uv sync
cp .env.example .env  # add APIFY_API_TOKEN, GEMINI_API_KEY
uv run dg dev
```

Open http://localhost:3000.

## Architecture

Medallion lakehouse: bronze (raw ingest) → silver (dedup) → gold (enrichment) → serving (views).

```
src/datalake/defs/
├── bronze/     # Apify → raw .jsonl → typed .parquet
├── silver/     # Dedup + clean → lake/silver/*.parquet
├── gold/       # Gemini enrichment → lake/gold/*.parquet
└── serving/    # dim_time, dim_profile, analytics_views
```

**Storage split:**
- Parquet lake (`data/lake/{bronze,silver,gold}/*.parquet`) — bulk data, lock-free parallel writes
- DuckDB state (`data/state.duckdb`) — progress tracking, SCD2 dimensions, immutable raw dumps

## Git workflow

- Trunk-based: branch from `main`, squash-merge via PR
- Conventional commits (`feat(scope): …`)
- Branch prefixes: `feat/`, `fix/`, `chore/`, `refactor/`, `test/`, `docs/`
- Protected `main` — no direct pushes, linear history, CI must pass

## Issue tracking

Local file at `ISSUES.md`. No GitHub Issues — keeps noise off the repo.

## History

Built 2026-06-30 as the production platform for the Instagram pipeline,
superseding `~/repos/ig-pipeline`.
