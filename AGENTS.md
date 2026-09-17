# datalake — Agent operating context

This repo is operated by Claude. Keep this file current — Claude reads it on every session.
This file is a router: it holds rules and pointers, not inventories. The pointed-to
documents own the detail.

## Key rules

- Never use `pip`. Always use `uv` for Python package management.
- Work on `feat/*`, `fix/*`, `docs/*`, `chore/*` branches; squash-merge to `main` via PR.
- Conventional commits only: `type(scope): summary`. No direct pushes to `main`.
- Never use PowerShell.

## Where Epics & User Stories live

Canonical store: **`tasks/epics/`** — one directory per epic
(`tasks/epics/<epic_slug>/epic.md` with user stories under `user-stories/`), registry at
`tasks/epics/README.md`. Only `tasks/epics/` is git-tracked; `tasks/plans/` and
`tasks/lessons.md` are untracked working notes, and `tasks/plans/*.md` are immutable
history. Author new stories in the store under their epic (As-a/I-want/So-that, binary AC,
DoD, tests). One story → one epic; use `relates-to` links, never copies. Check the
registry before spinning up a new workstream.

## Current direction

The enrichment backend is the standalone qwen batch service (ADR-0009) behind the
inference seam; the Gemini path is retired. Current focus is pipeline hardening: the
Apify ingestion path (SDK client, per-creator watermark sync, bounded fan-out via the
`core_refresh` schedule) and the enrichment replay path. See `docs/architecture/pipelines/enrichment.md`.

## Architecture in one screen

Medallion lakehouse: bronze lands every external response verbatim (Parquet, append-only);
silver conforms deterministically from bronze with zero API calls; gold holds analytic
marts; serving holds dims/metrics/marts/views. With bronze verbatim, a schema or mapping
change is a replay, never a re-bill.

| Layer | Storage | Notes |
|---|---|---|
| Bronze | Parquet (`data/lake/bronze/`) | Polars direct write; file-based, no state tracking |
| Silver | Parquet + DuckDB | PolarsIOManager; watermarks in DuckDB |
| Gold | DuckDB tables | Analytic marts |
| Serving | DuckDB dims/views | `dim_*` SCD2, `v_*` views |
| Ops | `data/ops.sqlite` | Dashboard app DB only — media_cache, creators, profiles, creator_merges |

Engine boundary: Polars handles all Parquet I/O; DuckDB handles SQL transforms (dedup,
watermarks, SCD2, views); Arrow is the zero-copy interchange between them.

Workspace layout (uv workspace; roles per ADR-0015):

```
datalake/
├── packages/opsdb/                # ops.sqlite contract
├── packages/storage/              # byte storage
├── services/orchestration/        # the Dagster code location
├── services/jobs/                 # the inference service
└── services/dashboard/            # FastAPI + vite

services/orchestration/src/orchestration/defs/
├── ig_core/{bnz,slv,gld}/         # Instagram: scrape → silver → labels
├── ig_enriched/{bnz,slv,gld}/     # Instagram enrichment payloads (prompts, schemas, mappings)
├── serving/{dims,metrics,marts,views,checks}.py
├── integration/                   # external API clients (transport only; apify_runs.py)
├── platform/                      # resources, paths, DuckDB catalog, schedules
└── {youtube,tiktok}_{core,enriched}/   # skeletons
```

Module names describe a role, never a provider. Every `__init__.py` is a docstring and
nothing else (enforced by `tests/operational/test_thin_init_files.py`).

## Canonical ground truth — read these before modeling anything

Do NOT take a table, view, asset, or schedule on faith — including from chat history or
older docs. Check the owning file:

| Question | Authoritative source |
|---|---|
| What tables/views exist? | `services/orchestration/src/orchestration/defs/platform/schemas.py` (DuckDB catalog); `packages/opsdb` schema (SQLite) |
| What assets/schedules/sensors run? | `services/orchestration/src/orchestration/definitions.py` |
| How does ingestion work (producers, payloads, contracts)? | `docs/architecture/bronze-schema.md` |
| Enrichment v3 spec (layered model)? | `docs/architecture/pipelines/enrichment.md` |
| Why was X decided? | `docs/architecture/adr/README.md` (ADR index — the decision log of record) |
| What invariants must hold? | `WATCHDOG.md` |
| What work is open? | `ISSUES.md` |

Live state (verified 2026-09-17): `data/ops.sqlite` holds exactly four tables —
`creator_merges`, `creators`, `media_cache`, `profiles`. (`prompt_registry` was retired the
same day; provenance now rides on the bronze/silver rows per ADR-0011.) Schedules:
`daily_medallion`, `core_refresh`, `details_sweep`. Sensors:
`enrichment_submit_sensor`, `enrichment_harvest_sensor`. There is no batch queue, no
Gemini job, and no `gold_analyses`/`gold_growth_facets` — all dropped 2026-09-15.

## Determinism / cost boundary (ADR-0018) — the one rule you must not break

No auto-materialization path from silver outputs or any mart may reach `submit`. Silver
must remain a pure deterministic replay of `bronze_enrichment_raw` — otherwise a
schema/mapping change stops being a replay and becomes a re-bill. Specifically:

- Do NOT add cost-limiting orchestration logic (budget arithmetic, cost gates, checks that
  fail because a run *could* cost money).
- Do NOT wire a policy that lets gold/silver transitively reach `submit`.
- `dry_run`/`limit`/workload selectors are operational controls, not the safety mechanism.
- A provider failure (403 credit limit / 429) is a loud, retryable failure at the seam —
  never pre-empted by graph logic.
- Cost is enforced at the API credential (OpenRouter key credit limit) as defense in depth.
- Schedules ship STOPPED; the owner enables them deliberately. Any auto-materialization
  policy an asset carries MUST be asserted by a test.

## Running the platform (Compose)

`docker compose up -d --build` from the repo root. Services: `orchestration` (Dagster,
host 7642 → container 3000, override `DAGSTER_HOST_PORT`), `jobs` (inference service,
8462), `dashboard` (3002). `DATALAKE_HOST_DATA_DIR` is required (host path of `data/`).
Never run `docker compose down -v` — it deletes the jobs service's job-store volume.
DuckDB is single-writer: a host `dagster dev` and the containers must not run at once.
A container whose host port is occupied comes up UNPUBLISHED (`docker ps` shows no
`0.0.0.0:N->N`) — check the mapping, not just `Up`.

## Roster boundary (ADR-0017)

The dashboard owns `creators`/`profiles`/`creator_merges` and serves them at
`GET /api/roster`. The pipeline lands that response as append-only bronze (`ig_roster_raw`)
and reads the roster FROM THE LAKE — never by opening `ops.sqlite`. `media_cache` stays
pipeline-owned; the dashboard writes through `opsdb.media_cache.record_media_cache_row`.
Persisted media paths translate through `IG_HOST_PATH_PREFIX` →
`IG_CONTAINER_PATH_PREFIX` (`platform.paths.runtime_path`).

## Core refresh (per-creator watermark sync)

The monthly `core_refresh` schedule (`defs/platform/core_refresh.py`) selects refreshable
tier1 profiles, bounds each chunk of URLs by the OLDEST member's watermark
(`onlyPostsNewerThan`, absolute `YYYY-MM-DD`), and fans out in bounded multi-URL runs.
Transport: `defs/integration/apify_runs.py` over the official `apify-client` SDK.
`max_charge_usd` scales with URL count (`CORE_REFRESH_CHARGE_CAP_USD` per URL).
`DEFAULT_MAX_PARALLEL_SCRAPE_RUNS = 16`, `MAX_PROFILES_PER_SCRAPE_RUN = 10`;
`validate_fanout` raises against the Apify account ceilings before any run is launched.
Never-scraped profiles get a full backfill (no date boundary); they are never dropped.

## Scrape run memory

Memory is a RUN option, not an actor input — `memoryMbytes` is absent from the Instagram
scraper actor's `input.properties`, so it must go to `start(memory_mbytes=...)`, never
into `run_input`. `SCRAPE_RUN_MEMORY_MB` in `core_refresh.py` pins it for the scheduled
core-refresh path only; the launchpad (`bronze_ig_posts`), `scrape_details_to_bronze` and
local ingest leave `memory_mbytes=None` and take the actor's own default (1024).

Apify does NOT echo run options in logs — confirm what a run actually used:

```python
ApifyClient(token).run(<run_id>).get().options.memory_mbytes
```

Monitor for FAILED with a memory message, or `item_count` below `results_limit` — both are
signs the pinned memory was too small.

## Verification plane (BINDING on all agents)

"Tests pass" is evidence a process ran, not that the system works. Confirmation is a
tracer shot: one real run of the changed path end-to-end with the destination verified.
(The v3 migration shipped ~700 green tests and a pipeline that could not run one cycle.)

Four controls:

1. **Read before dispatch** — grep tests/docs for the subsystem; an existing test importing
   a symbol encoding a design IS a specification.
2. **Story ACs in every brief** — acceptance includes the story's AC list, not just the unit's.
3. **Conformance over existence** — runtime conformance checks (Protocol `isinstance`,
   catalog-vs-producer column diff, fakes accept the full signature) run in CI.
4. **One real run as the acceptance gate** — the smoke slice (`scripts/make_smoke_slice.py`)
   makes a real e2e run cheap.

Done bar: green suite AND materialized destination AND one observed run through the slice.

## Test conventions

- `uv run pytest tests/ -v`; in-memory DuckDB via DI; Parquet tests use `tmp_path`.
- One test per behavioral contract, one per edge case.
- **Before writing any asset that reads from disk, read ONE real input file and display
  its schema.** Do not model against test data (Phase 2 false start, 2026-06-30).
- Full suite is the final gate, not the inner loop: measured 197s, 407s and ~200s on
  three runs over 2026-09-17 (779-782 tests). Those are the only timings recorded; use
  scoped runs while working and budget a few minutes for the full pass.

## Env vars (essentials)

Set in `.env`: `APIFY_API_TOKEN`, `OPENROUTER_API_KEY`, `JOBS_SERVICE_URL`,
`DAGSTER_HOME`, `IG_DATA_DIR`, `IG_BRONZE_DIR`, `IG_SILVER_DIR`, `IG_GOLD_DIR`,
`IG_DB_PATH`, `OPS_DB_PATH`, `IG_LOCAL_INGEST_DIR`, `IG_HOST_PATH_PREFIX`,
`IG_CONTAINER_PATH_PREFIX`. `platform/paths.py` calls `load_dotenv()` as its first
statement — the one sanctioned import-time side effect. Stale `GEMINI_API_KEY`/`GEMINI_TIER`
entries may remain in `.env`; only `scripts/archive/experiments/` reads them.

## Naming convention

Layer prefix, domain-scoped: `bronze_ig_posts`, `silver_ig_posts`, `gold_post_enrichment`;
serving keys are `dim_*`/`v_*`. Asset key == produced table for silver/gold/serving.
Bronze keys name Parquet datasets, not DuckDB tables, and two bronze names are on-disk
identities rather than graph keys: `bronze_enrichment_raw` (the landing filename) and
`ig_roster_raw` (a directory of append-only snapshots — a rename orphans existing
snapshots silently). The join key across domains is `platform`, never `domain`.

## DAGSTER_HOME

Set in `.env` to `C:/Users/evano/repos/datalake/data/dagster_home`. Both `dagster dev`
and CLI commands share this instance; without it, CLI runs land in a temp directory
invisible to the UI.

## Report / analysis HTML writing

When a piece is meant to be *read*, follow the house playbook: structure/voice per
`~/repos/dev-portfolio-2/docs/content-style.md`; standalone-HTML register per the
`~/repos/vibe-coding-analytics/docs/reports/2026-09-06-vibe-coding-analytics-retrospective.html`
template. Ground every number in lake/run output; unverifiable claims are dropped or
marked estimates; provenance and caveats go in a footer or honesty banner.
