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

Medallion lakehouse with async enrichment batches:
bronze (raw ingest) → silver (dedup) → batch creation → gold (async Gemini worker) → serving (views).

```
src/datalake/defs/
├── common/       # Resources, schedules, path helpers, lake paths
├── enrichment/   # batch, assets, prompts (standalone worker: scripts/enrichment_worker.py)
├── instagram/    # ig_posts_raw, ig_posts_slv, ig_posts_gen_batches, config
└── serving/      # dim_profile, dim_date, v_post_detail + 7 downstream views
```

**Storage split:**
- Parquet lake (`data/lake/{bronze,silver}/*.parquet`) — bulk data, lock-free parallel writes
- DuckDB state (`data/state.duckdb`) — silver tables, gold_analyses, watermarks, serving dims/views
- SQLite ops (`data/ops.sqlite`) — batch coordination, media cache, dead letter

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
