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

## Architecture (current state)

Medallion lakehouse with async enrichment batches:
bronze (raw ingest) → silver (dedup) → batch creation → gold (Dagster enrichment jobs) → serving (views).

Note: the accepted target design (ADR-0011 layered enrichment with
`bronze_enrichment_raw` → six `silver_*` tables → four gold marts, and
ADR-0012 Dagster-native orchestration replacing the ops.sqlite queue) is **not
yet implemented** — the code above is what runs today.

**Key documents:**
- Canonical enrichment spec: `docs/architecture/pipelines/enrichment.md` (v3 layered model —
  supersedes `docs/architecture/enrichment-design-v1-superseded.md`, kept as rationale only)
- Architecture decision records: `docs/architecture/adr/` (index: `docs/architecture/adr/README.md`;
  ADR-0011 = layered enrichment, ADR-0012 = Dagster-native orchestration)
- Reviewer traps and invariants: `WATCHDOG.md`; backlog: `ISSUES.md`

**The inference seam (target).** External model work rides one seam — a shared
job ledger plus three verbs (`submit`, `poll-to-terminal`, `retrieve`) — with a
swappable `ProviderAdapter` behind it, so replacing the qwen-batch service with
another provider (or Gemini batch) is a config change. Today the repo runs two
independent lifecycles instead; convergence is `tasks/plans/inference-service-seam.md`.

```
src/datalake/defs/
├── common/       # Resources, schedules, path helpers, lake paths
├── enrichment/   # batch, analysis, prompts (Dagster-native enrichment jobs)
├── instagram/    # ig_posts_raw, ig_posts_slv, ig_posts_gen_batches, config
└── serving/      # dim_profile, dim_date, v_post_detail + 13 downstream views (incl. 5 canonical metric views)
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
