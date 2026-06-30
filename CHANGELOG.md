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
