# datalake — Agent operating context

This repo is operated by Claude. Keep this file current — Claude reads it on every session.

## Key rules

- Never use `pip`. Always use `uv` for Python package management.
- Work on `feat/*`, `fix/*`, `docs/*`, `chore/*` branches; squash-merge to `main` via PR.
- Conventional commits only: `type(scope): summary`.
- No direct pushes to `main`.
- Never use PowerShell.

## Where Epics & User Stories live

The canonical Epic/User-Story store is **`tasks/epics/`** — one directory per
epic (`tasks/epics/<epic_slug>/epic.md`), with that epic's user stories under
`tasks/epics/<epic_slug>/user-stories/`. The registry/status board is
`tasks/epics/README.md`.

- **Only `tasks/epics/` is git-tracked** (`.gitignore` ignores the rest of
  `tasks/`). Epics/stories are governance content and are versioned;
  `tasks/plans/` and `tasks/lessons.md` are untracked working notes.
- **Author new user stories in the store**, under their epic, with
  As-a/I-want/So-that + binary AC + DoD + tests. Link to the source plan for the
  "how"; the plan may point to its canonical epic.
- **Canonical ownership is one story → one epic (many-to-one).** A story can be
  *relevant* to other epics via a `relates-to` link — never a second copy.
- **`tasks/plans/*.md` are immutable history** — do not retroactively edit them
  from here. When an old plan needs aligning with a merged/canonical epic,
  publish a new version that links to it.
- Consolidating similar epics is a deliberate step: record the merge in the
  registry and in affected stories' `relates-to`, then (optionally) version the
  source plan.
- When starting feature work, check the registry first: attach new stories to
  existing epics (commonly the enrichment/serving seams) rather than spinning up
  a fresh plan-only workstream.

## Report / analysis HTML writing (datalake analysis outputs, READMEs, docs)

When a piece is meant to be *read* (an analysis report, a retrospective, a
case study) rather than just executed, follow the org house playbook:

- **Writing:** read `~/repos/dev-portfolio-2/docs/content-style.md` for the
  house structure (SCQA/BLUF spine, ≤6 sections, quantifiable verifiable
  numbers) and voice (grounded gonzo, anti-AI-tell rules — no "robust/
  leverage/delve" vocab, no announcing scaffolds, no reflexive em-dash stacks).
  Data reports keep a neutral register for findings but still follow the
  anti-tell + burstiness rules.
- **Visual style for standalone HTML:** copy the register/layout of
  `~/repos/vibe-coding-analytics/docs/reports/2026-09-06-vibe-coding-analytics-retrospective.html`
  (skin-F terminal: VT323 display + IBM Plex Sans/Mono, sticky TOC,
  scroll-reveal, reduced-motion). For Tailwind/Chart.js analysis outputs, keep
  the existing toolchain but apply the same typographic hierarchy and honest
  banner conventions.
- **Ground every number** in the lake/run output; if a claim can't be verified
  from state, drop it or mark it an estimate. Provenance and caveats go in a
  footer or honesty banner, never buried mid-argument.

## Current direction (2026-08-12)

The user's priority is a **robust pipeline that extracts rich signal from video
and image across multiple sources** — not hosting/infra. Hosting (S3/R2, DuckLake,
MotherDuck, cloud warehouse) is explicitly deferred and migrates cleanly later.

**Next branch = pipeline hardening.** The critical gap: `ig_posts_slv` hardcodes
`media_files = "[]"` and `media_count = 0`, so bronze media URLs (`videoUrl`,
`displayUrl`) never reach Gemini — every `gold_analyses` row is text-only. The
multimodal worker code is correct but starved of input. Work items, in order:

1. Wire media end-to-end (bronze → silver `media_files` → worker → Gemini),
   proven by an External Integration Gate smoke test: 1 real image + 1 real video.
2. Fix the media-expiry race: cache media bytes at scrape time (CDN URLs die in
   ~4-5 days). This is the load-bearing part of the media cache, distinct from
   the dashboard thumbnail serving.
3. Multi-source as additive work, not an architecture project: the worker already
   dispatches by `domain`, gold PK is `(post_id, domain)`. YouTube first (video +
   stable URLs + free transcripts), then TikTok.
4. Triage-first video processing: deep-pass only high-value posts; uniform deep
   video is ~17.4k tokens/min and pure waste.


### DIRECTION PIVOT (2026-09-09) — qwen batch service replaces gemini-batch (ADR-0009)

The enrichment backend is pivoting from Google's `gemini-batch` to a **standalone,
domain-agnostic qwen batch service** (`~/repos/qwen-batch-service`, dockerized) —
driven by the Gemini Developer-API File-API **20 GiB storage cap** that killed a
full-corpus video run and the absence of a Vertex vision batch discount. qwen
(`qwen/qwen3.7-flash` via OpenRouter, ~$0.03/$0.13 per 1M) is ~$3 for the corpus.
Media becomes **client-side ffmpeg frame-sampling** (no File-API upload, no GCS
mirror — the GCS durable-media work is dropped). See **ADR-0009** and
`tasks/plans/qwen-batch-enrichment.md`; governance landed on `main` (PR #62,
2026-09-09; US-EENG-1/US-EENG-2 under E-ENRICH-ENGINE).

**Dependent service (US-EENG-2):** enrichment treats the qwen-batch service as a
hard dependency. Spin up: `docker compose up` (or `uv run`) in
`~/repos/qwen-batch-service`. The enrichment submit path MUST ping `GET /health`
first and **fail loudly** (never a quiet "nothing to do") if the service is down.

**Code state note:** the growth-facets engine now runs on the qwen-batch
service (one-shot CLI driver, no Dagster op); the separate IG `gold_analyses`
gemini path (gemini-batch machinery) remains until its own migration — treat
the 2026-09-08 section below as applying to that path.

### Enrichment v2 — layered model and inference seam (2026-09-10, TARGET STATE)

Two accepted decisions reshape enrichment; **neither is implemented yet** — the
code still runs the model described in the sections below.

*109:- **Layered model (ADR-0011).** Canonical spec: `docs/architecture/pipelines/enrichment.md` (v3).
  Bronze lands every external model response verbatim in `bronze_enrichment_raw`
  (immutable, append-only); silver conforms + validates it deterministically
  into six `silver_*` tables keyed `(post_id, platform)` — zero API calls, so a
  schema/mapping change is a replay, never a re-bill; gold builds four analytic
  marts. See the Architecture section below. This supersedes the v1 spec
  `docs/architecture/enrichment-design-v1-superseded.md` (retained as rationale/experiment
  record only — not canonical).
- **Inference seam (ADR-0008/0009).** One seam, not two lifecycles: three
  verbs (`submit` / `poll-to-terminal` / `retrieve`) over an HTTP contract;
  harvest composes poll + retrieve + an idempotent verbatim landing. **No
  ledger: ADR-0013 — the service owns its job store and Dagster polls it.**
  A `ProviderAdapter` Protocol with one canonical state vocabulary
  (`pending`/`processing`/`completed`/`failed`) lets `ServiceBackedAdapter`
  (qwen-batch-service — the service is what makes the synchronous OpenRouter/qwen
  provider async) and `DirectBatchAdapter` (Gemini, natively async) swap by
  config string; `build_adapter(name)` is the only place a provider is named.
  Proven in `~/repos/enrichment-spike/spike_defs/adapter.py`; NOT wired into this
  repo yet — today the Gemini `submit.py`/`gemini_batch.py`/`harvest.py` and qwen
  `facets_batch.py`/`qwen_client.py` lifecycles share no submit/poll/harvest code.
- **Orchestration (ADR-0012).** Orchestration state moves into the Dagster
  instance + the lake; the `ops.sqlite` queue (`batch_jobs`, `batch_items`,
  `dead_letter`, `facets_batch_jobs`) is retired. `ops.sqlite` retains
  `media_cache`, `creators`, `profiles`, `creator_merges`, `prompt_registry`
  (`media_metadata` was dropped with the queue — it was a Gemini File-API
  cache for a retired provider). **EXECUTED 2026-09-15**: all seven tables
  archived then dropped; see `scripts/retire_queue_tables.py`.

### Multimodal status (2026-09-08) — BATCH-NATIVE ONLY; interactive removed (PRE-PIVOT)

Enrichment is **batch-native only** as of 2026-09-08: the synchronous
per-item interactive path (`process_item`, `scripts/enrich_interactive.py`,
`scripts/enrich_facets_full.py`, `scripts/enrich_facets_pilot.py`,
`GeminiResource.analyze/analyze_with_usage`) was removed entirely. The
`gemini-batch` path is multimodal and is the ONLY enrichment vehicle:

- **Gold analyses (text + multimodal):** `submit_gemini_batches_job` →
  `gemini_batch_harvest`; `build_requests_for_items` resolves media through
  `_resolve_media_for_post` (scrape-time byte cache → File API / inline
  images, FREE-tier video gate + per-item video-token cap) and serializes
  media Parts at `MEDIA_RESOLUTION_LOW`.
- **Growth facets (`gold_growth_facets`, visual + text layer):**
  `scripts/enrich_facets_batch.py` driver over `defs.enrichment.facets_batch`
  (submit/harvest; visual pass with media, text pass caption-only).

First live multimodal runs (2026-09-04, then interactive, Tier-1 flash-lite):
residue batch recovered 125/130 posts (5 dead-lettered), UI/UX media-bearing
slice 465/485 (20 dead-lettered). Media enrichment changed classification on
**93.6%** of media-bearing posts vs text-only (topic 72%, subtopic 91%) — the
model sees actual content, not just captions. Dead-letter modes seen:
per-item File-API "Unknown mime type" on a few image URLs (~3%) + transient
CDN connect timeouts; both route to `dead_letter` correctly. The former
interactive worker hard-crash on very long runs is moot with interactive gone.




## Architecture
**Medallion lakehouse with async enrichment batches:**

| Layer | Storage | Writer | State tracking |
|-------|---------|--------|----------------|
| Bronze | Parquet (`data/lake/bronze/`) | Polars (direct write) | None — file-based |
| Silver | Parquet (`data/lake/silver/`) | PolarsIOManager | DuckDB `silver_ig_posts` + watermarks |
| Batches | SQLite (`data/ops.sqlite`) | `ig_posts_gen_batches` | `batch_jobs` + `batch_items` (CURRENT; retired by ADR-0012 — target is Dagster-native state) |
| Gold | DuckDB table | Dagster enrichment jobs (submit → harvest) | `gold_analyses` (AssetSpec, externally materialized) — CURRENT; target is four ADR-0011 marts |
| Serving | DuckDB views + tables | DuckDB | `dim_profile` (SCD2), `dim_date`, 14 analytics views (incl. 5 canonical metric views) |

**Enrichment layered model (ADR-0011) — LIVE since 2026-09-15.**
Canonical spec: `docs/architecture/pipelines/enrichment.md` (v3; supersedes
`docs/architecture/enrichment-design-v1-superseded.md` v1, retained as rationale only). The table above described the pre-migration world; this is what RUNS
TODAY (verified 2026-09-15 against `data/state.duckdb`):

| Layer | Job | Contract |
|---|---|---|
| Bronze | Land ALL external model responses verbatim | `bronze_enrichment_raw` — immutable, append-only, Parquet, keyed `(post_id, platform, workload, prompt_hash, run_id)` |
| Silver | Conform + validate — deterministic from bronze, ZERO API calls | six `silver_*` tables, keyed `(post_id, platform)` |
| Gold | Analytic marts answering the owner's three questions | four marts |

- **Six silver tables** (each with its own provenance columns — `provider`,
  `model`, `prompt_hash`, `schema_version`, `run_id`, ...): `silver_visual_annotations`,
  `silver_visual_summaries`, `silver_audio_transcripts`, `silver_text_annotations`,
  `silver_text_summaries`, `silver_content_classification`.
- **Four gold marts:** `gold_post_enrichment`, `gold_creator_performance` (Q1:
  who performs well in X domain), `gold_content_shape_performance` (Q2: what
  content shape performs), `gold_top_posts` (Q3: what performs across domains).
- **Key rules:** the join key is `platform`, NEVER `domain` (fixes the live
  `gold_analyses.domain = 'instagram'` overload); provider lives in metadata
  columns, never in a table name; validation lives in silver with LOUD
  quarantine on terminal failures; marts compose the canonical metric views
  (`v_post_metrics`, `v_creator_metrics`, ...) and never re-derive metrics.
- **Why:** with the verbatim response landed in bronze, parsing/validating/
  splitting is a pure function of bronze — a schema or mapping change is a
  deterministic replay, not a re-bill. The paid/stochastic part ends at bronze.
- **Replacements:** `gold_analyses` → `silver_content_classification`;
  `gold_growth_facets` → the four visual/text silver tables;
  proposed-but-never-built `gold_transcripts` → `silver_audio_transcripts`.

**Inference seam (ADR-0008/0009, amended by ADR-0013):** one boundary where the
pipeline hands work to an outside system — three verbs
(`submit` / `poll-to-terminal` / `retrieve`) over an HTTP contract, with each
side owning its own state. **There is no shared `external_jobs` ledger.** The
service owns its job store and Dagster polls it. `ProviderAdapter` swaps by config
string via `build_adapter(name)`: `ServiceBackedAdapter` (qwen-batch-service —
the service is what makes the synchronous OpenRouter/qwen provider async) and
`DirectBatchAdapter` (Gemini, natively async). Not built in this repo; proven in
`~/repos/enrichment-spike/spike_defs/adapter.py`.

**Orchestration (ADR-0012):** orchestration state moves to the Dagster instance
+ the lake. The `ops.sqlite` queue (`batch_jobs`, `batch_items`, `dead_letter`,
`facets_batch_jobs`) is retired; `ops.sqlite` retains `media_cache`,
`media_metadata`, `creators`, `profiles`, `creator_merges`, `prompt_registry`.
Retry becomes a new partition key; failures surface via the anti-join
`landed(bronze) ∖ conformed(silver)` plus a BLOCKING asset check — the blocking
check is IMPLEMENTED (``orchestration.defs.engine/`checks.py::check_no_silent_loss`). The
queue-table DROP itself is still pending human approval (see `scripts/retire_queue_tables.py`).

**Current vs target, in one line:** today `gold_analyses` + `gold_growth_facets`
+ the `batch_*`/`dead_letter` queue in `ops.sqlite`; target the six silver
tables + four marts + Dagster-native orchestration. An agent reading this file
must not assume the gold-mirror model is the destination.

**A uv workspace, with role-based structure inside the code location (ADR-0015):**

```
datalake/                          # workspace root — the command home for dg/dagster/pytest/ruff
├── packages/opsdb/                # ops.sqlite contract (dashboard writes, pipeline reads)
├── packages/storage/              # byte storage (R2 under storage-migration)
├── services/orchestration/        # the Dagster code location
├── services/jobs/                 # the inference service (moved in by services-extraction)
└── services/dashboard/            # FastAPI + vite

services/orchestration/src/orchestration/defs/
├── ig_core/{bnz,slv,gld}/         # Instagram: scrape → silver → labels
├── ig_enriched/{bnz,slv,gld}/     # Instagram enrichment PAYLOADS (prompts, schemas, mappings)
├── engine/                        # cross-domain enrichment machinery — never duplicated per domain
├── serving/{dims,metrics,marts,views,checks}.py
├── integration/                   # external API clients (transport only)
├── platform/                      # resources, paths, DuckDB catalog, schedules
└── {youtube,tiktok}_{core,enriched}/   # skeletons
```

Module names describe a **role**, never a provider. `engine/` holds submit, harvest,
partitions, the seam, the landing writer, the media cache and the silver runtime;
a domain package holds only its payloads. Every `__init__.py` is a docstring and
nothing else (enforced by `tests/operational/test_thin_init_files.py`), so importing
one module never drags in its siblings' dependencies.

**Storage split:**
- **Parquet lake** — bulk data, lock-free parallel writes
- **DuckDB** (`data/state.duckdb`) — authoritative current state, watermarks, SCD2 dims, views
- **Lake layers (ADR-0011, LIVE)** — `bronze_enrichment_raw` (9,576 rows) + six `silver_*` tables + four gold marts, per the layered model above

**Engine boundary:**
- Polars handles all Parquet I/O (read/write NDJSON and Parquet)
- DuckDB handles SQL transforms (DISTINCT ON dedup, watermark queries, SCD2, views)
- Arrow is the zero-copy interchange format between them (`to_arrow()` / `from_arrow()`)

## Table naming convention

Domain-scoped, not generic. Supports multi-source expansion (TikTok, YouTube, LinkedIn in future).

| Database | Table | Purpose |
|---|---|---|
| DuckDB | `silver_ig_posts` | Deduped, normalized Instagram posts |
| DuckDB | `gold_analyses` | Completed enrichments with domain PK (`post_id`, `domain`) and `prompt_hash` |
| DuckDB | `dim_profile` | SCD2 profile dimension (cross-domain, `channel` column), carries `creator_id`/`creator_name` |
| DuckDB | `dim_date` | Generated date dimension — 1 year back, fiscal year (Jul–Jun) |
| DuckDB | `watermarks` | Generic progress tracking for any pipeline (`name`, `timestamp`) |
| SQLite | `batch_jobs` | Batch coordination: job-level status (`pending`/`processing`/`complete`) |
| SQLite | `batch_items` | Per-post items within a batch, with retry tracking (`attempts`, `scheduled_for`) |
| SQLite | `media_metadata` | URL hash → Gemini File API URI cache |
| SQLite | `media_cache` | Scrape-time byte cache: media URL hash → local file path (image/video bytes) |
| SQLite | `dead_letter` | Terminal failures after `MAX_ATTEMPTS` retries exhausted |
| SQLite | `creators` | A person/brand (`id`, `name`) — owns 1..N profiles across platforms |
| SQLite | `profiles` | One account per platform (`platform`, `handle` PK) linked to a creator; carries scrape config (depth, enabled, tier) |
| SQLite | `creator_merges` | Merge ledger for retired duplicate auto-creators (`merged_creator_id` PK → `surviving_creator_id`, `handle`, `merged_at`/`reversed_at` for `--undo`) |

**Current vs target (ADR-0011/0012):** the tables above are what exists today.
Under the accepted v3 model, `gold_analyses` is replaced by
`silver_content_classification`, `gold_growth_facets` by
`silver_visual_annotations` + `silver_visual_summaries` +
`silver_text_annotations` + `silver_text_summaries`, and `batch_jobs`/
`batch_items`/`dead_letter` are retired in favor of Dagster-native
orchestration state (`media_cache`, `creators`, `profiles`, `creator_merges`,
`prompt_registry` retained). **EXECUTED 2026-09-15** — the queue tables are
gone from the live DBs; Dagster-native state has been live since the W3/W4
units landed.

**DuckDB views:** `v_post_detail` (foundational), `v_signal`, `v_quality_trend`, `v_creator_quality`, `v_rising_creators`, `v_domain_coverage`, `v_engagement_outliers`, `v_outlier_posts`, `v_creator_outlier_rate`, `v_post_baselines` (serving-layer comments/views point-in-time baselines), `v_post_metrics` (canonical per-post metrics), `v_creator_metrics` (gate-free per-creator activity), `v_creator_profile` (per-creator canonical rollup: momentum + dominant domain), `v_creator_topics` (per-creator top-5 topics by count and performance), `v_recent_hot_posts` (recent 28-day hot feed), `v_profile_metrics` (per-profile counts), `v_overview` (single-row), `v_standout_calendar` (standouts per day-of-month)


## Watermarks pattern

A single `watermarks` table replaces per-pipeline progress tables. Any pipeline stamps its row:

```sql
CREATE TABLE watermarks (name TEXT PRIMARY KEY, timestamp TIMESTAMP NOT NULL);
```

- Silver reads/writes `watermarks WHERE name = 'silver_ig'`
## Dead letter pattern

> **ADR-0012 (accepted 2026-09-10; DROPPED 2026-09-15):** the `dead_letter` table
> is scheduled for retirement alongside the `ops.sqlite` queue — failures will
> surface via the anti-join `landed(bronze) ∖ conformed(silver)` plus a BLOCKING
> asset check. Everything below describes the CURRENT (pre-ADR-0012) behavior
> and stays accurate until that lands.

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
no automatic retry worker. The batch system handles retries via `scheduled_for` in
`batch_items` with exponential backoff; dead_letter is the terminal state.

A separate scheduled asset (`retry_dead_letter`, deferred) reads `WHERE status = 'pending'`, retries, and upserts successes. This mirrors ML feature store patterns (Feast, Tecton) where error queues are separate from serving data.

## processed_on semantics

`processed_on` in `silver_ig_posts` is set **only when a post first appears in silver**. It never changes on subsequent runs, even when engagement metrics update. This enables gold to do true incremental processing:

```sql
SELECT ... FROM ig_post_labels
WHERE enrich_decision IN ('standout', 'control', 'floor_filler')
  AND NOT EXISTS (SELECT 1 FROM gold_analyses g
                  WHERE g.post_id = ig_post_labels.post_id
                    AND g.prompt_hash = :current_prompt_hash)
  AND NOT EXISTS (SELECT 1 FROM batch_items b
                  WHERE json_extract(b.payload, '$.post_id') = ig_post_labels.post_id
                    AND b.status IN ('pending', 'processing'))
```

The old `gold_ig` watermark is RETIRED (Epic 3): `ig_post_labels` is the
discovery source; only a current-prompt gold analysis blocks re-enrichment
(stale-prompt rows re-enqueue, US-L5).

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


## Schema catalog and drift detection

``orchestration.defs.platform.schemas` (DuckDB) + `opsdb.schema` (SQLite)` is the canonical schema definition for both databases.
`tests/operational/expected_schema.py` re-exports it for backward compatibility.
Any table the pipeline reads or writes must be listed here. The readiness test
(`test_state_compatibility.py`) asserts the catalog matches the running databases.
**DuckDB tables:** `silver_ig_posts`, `watermarks`, `dim_profile`, `dim_date`,
plus the v3 layer: the six `silver_*` enrichment tables
(`silver_content_classification`, `silver_visual_annotations`,
`silver_visual_summaries`, `silver_text_annotations`, `silver_text_summaries`,
`silver_audio_transcripts`) and the four gold marts (`gold_post_enrichment`,
`gold_creator_performance`, `gold_content_shape_performance`, `gold_top_posts`).
**SQLite tables:** `media_cache`, `creators`, `profiles`, `creator_merges`,
`prompt_registry`

**Dropped by the W9 retirement (2026-09-15)** — these are NOT expected and their
absence is correct: `gold_analyses`, `gold_growth_facets` (DuckDB);
`batch_jobs`, `batch_items`, `dead_letter`, `facets_batch_jobs`,
`media_metadata` (SQLite). All archived to `data/lake/archive/<table>/<utc>/`
with export count == live count verified in the same run. `bronze_enrichment_raw`
is Parquet-backed and is never a registered DuckDB table.
**Views:** `v_post_detail`, `v_signal`, `v_quality_trend`, `v_creator_quality`, `v_rising_creators`, `v_domain_coverage`, `v_engagement_outliers`, `v_outlier_posts`, `v_creator_outlier_rate`, `v_post_metrics`, `v_creator_metrics`, `v_profile_metrics`, `v_overview`, `v_standout_calendar`

- **Missing tables/columns** — fails with "run the pipeline or migration"
- **Stale table names** — tables in the DB that were renamed/dropped (e.g. `gold_ig_analyses`). Fails with migration hint.
- **Extra tables** — tables in the DB not in the catalog. Warns, doesn't fail (may be legitimate).

**Current state (above) vs target (ADR-0011/0012):** when the v3 layered model
and Dagster-native orchestration land, the catalog changes — six `silver_*`
tables, `bronze_enrichment_raw`, four gold marts appear, and `batch_jobs`/
`batch_items`/`dead_letter` leave `ops.sqlite` (`media_cache`, `media_metadata`,
`creators`, `profiles`, `creator_merges`, `prompt_registry` retained). Update
`schemas.py` and this list together when that migration ships.

## Operational scripts

| Script | Purpose |
|---|---|
| `migrations/migrate_schema_drift.py` | Apply schema migrations: rename tables, move data between DBs, drop vestigial tables. Idempotent. |
| `migrations/migrate_to_v2.py` | One-shot migration from Phase 1-4 schema to v2 domain-scoped tables. |
| `migrations/migrate_from_ig_pipeline.py` | Import bronze Parquet from legacy ig-pipeline repo. |
| `migrations/migrate_owner_username.py` | Backfill null ``owner_username`` in silver from bronze ``username`` fallback. Idempotent. |
| `migrations/migrate_creators_profiles.py` | Split `scrape_targets` → `creators` + `profiles` (1:1 backfill), create `media_metadata`, drop `scrape_targets`. Idempotent. |
| `scripts/conform_silver.py` | Publish + register the six v3 silver tables from `bronze_enrichment_raw` — the live silver publisher (zero API calls; deterministic replay). |
| `scripts/make_smoke_slice.py` | Deterministic dev/smoke slice builder for the verification plane. |
| `scripts/reconcile_facets_jobs.py` | Reconcile `facets_batch_jobs` against the qwen service job store. Superseded by the reconciliation step built into `retire_queue_tables.py --apply`. |
| `scripts/archive/retire_queue_tables.py` | W9 retirement (executed 2026-09-15): reconcile handles → archive+verify → per-table drop → KEEP-set assertion. Archived — see `scripts/archive/README.md`. |
| `migrations/migrate_drop_prompt_registry.py` | Archive then retire `prompt_registry` (provenance now rides on the bronze/silver rows). `--plan` / `--apply`. |
## Stale analysis update

When the prompt or output schema changes, existing `silver_content_classification`
rows carry a stale `prompt_hash`, and `check_prompt_currency` fails while any
exist. `CURRENT_PROMPT_HASH` lives in
`orchestration.defs.ig_enriched.slv.prompts`.

Re-processing is a **replay, not a re-bill**: bronze holds every provider response
verbatim, so bumping `DERIVATION_VERSION` in `engine/silver_rt.py` and
re-materializing `silver_enrichment` republishes silver deterministically with
zero API calls. Only a genuinely new prompt needs new provider calls, and those go
out through the submit sensor.

## DAGSTER_HOME

Set in `.env` to `C:/Users/evano/repos/datalake/data/dagster_home`. Both `dagster dev`
and CLI commands (`dagster asset materialize`, `dagster job execute`) share this instance.
Without it, CLI runs go to a different temp directory and aren't visible in the UI.

## Running the platform (Compose)

`docker compose up -d --build` from the repo root brings up all three services:

| Service | Port | What it is |
|---|---|---|
| `orchestration` | 7642 | Dagster (`dagster dev` — webserver + daemon, so sensors tick). Host `7642` → container `3000`; override with `DAGSTER_HOST_PORT` |
| `jobs` | 8462 | the inference service (`services/jobs`) |
| `dashboard` | 3002 | FastAPI + the built Vite SPA |

**Ports are chosen to not collide.** Dagster's own default (3000) is one of the most
contended ports in development — it collides with Node/Next/CRA dev servers, langfuse, and
much else — so this Compose publishes it on **7642** instead and keeps the container
listening on 3000 internally. Change the host side with `DAGSTER_HOST_PORT`.

**`DATALAKE_HOST_DATA_DIR` is required** — the absolute host path of this checkout's
`data/` (e.g. `C:/Users/evano/repos/datalake/data`). It is the host side of the path map
that translates persisted media paths into the `/data` paths both containers see, and
compose refuses to start without it rather than handing the inference service paths it
cannot open.

**Never run `docker compose down -v`.** The `jobs` service keeps its job store on the
`/jobs-data` volume; `-v` deletes it.

DuckDB is single-writer, so a host `dagster dev` and the containers must not run at once.

Note that a container which starts while its declared host port is occupied comes up
**unpublished** (`docker ps` shows `3002/tcp` with no `0.0.0.0:N->N/tcp`) rather than
failing — so check the mapping, not just the `Up` status.

## The roster boundary (ADR-0017)

The dashboard OWNS `creators`/`profiles`/`creator_merges` and serves them at
`GET /api/roster`. The pipeline lands that response as append-only bronze
(`ig_roster_raw`, one snapshot per `fetched_at`), publishes `silver_ig_roster`, and reads
the roster FROM THE LAKE — never by opening `ops.sqlite`. Adding a profile is a
registration; the `details_sweep` schedule reconciles which profiles need a details
scrape (enabled `results_type='details'` past the sweep watermark), and the watermark
advances in `ig_profile_details_raw` after the scrape lands, so a failure retries.

`media_cache` stays pipeline-owned (it is written on the scrape hot path); the dashboard
is a writer to it through `opsdb.media_cache.record_media_cache_row`, which ensures the
table exists.

**Host↔container media paths.** Rows hold whatever path vocabulary the fetching process
had (Windows-absolute, for host writes). `platform.paths.runtime_path` translates them
through `IG_HOST_PATH_PREFIX` → `IG_CONTAINER_PATH_PREFIX`; a host run with the host
prefix unset is identity. A container-written row (`/data/media/...`) will not resolve on
the host without the reverse mapping — run the pipeline in Compose once it is the runtime
of record.

## Pipeline automation and the cost boundary (ADR-0018)

**The pipeline is job-driven.** Asking for a downstream asset runs its upstream work — an
operator asks for an outcome ("publish silver", "refresh the marts") and Dagster computes
the rest from declared lineage. There is no required sequence of hand-run stages, and no
stage whose normal operation is "someone remembers to trigger it."

**The submit edge is a DETERMINISM boundary, not a cost boundary.** No auto-materialization
path from `silver_enrichment` or any mart may reach `submit`. `silver_*` must stay a *pure
deterministic replay of `bronze_enrichment_raw`* (ADR-0011): if a `materialize` on silver
could transitively call the provider, silver would become a function of live provider state
and sampling — different rows on different runs — and "a schema change is a replay, not a
re-bill" would stop being true.

This does NOT become removable when cost is handled. Do not read the credit limit below as
"the wall is unnecessary":

- Do NOT add orchestration logic whose purpose is limiting spend. No budget arithmetic, no
  cost gate, no asset check that fails because a run *could* cost money.
- Do NOT wire an auto-materialization policy that lets gold/silver transitively reach
  `submit` — even if the API key makes it "safe".
- `dry_run` / `limit` / the workload selector are **operational controls** for scoping a run
  (debugging, narrow backfills) — NOT the safety mechanism, and neither removing nor relying
  on them as a guarantee is correct.
- A provider failure (credit limit 403 / 429) is a **loud, retryable failure at the seam**
  (`ProviderError`). It must never be pre-empted by graph logic or silently absorbed.

**Cost is enforced at the API credential, as DEFENSE IN DEPTH.** The OpenRouter key carries
its own credit limit (403 "key limit exceeded"), set and rotated by the owner per period.
It bounds the *submit* edge and a runaway fan-out. It does not protect the free replay path
and it is not the reason the submit boundary exists.

What stays human-triggered, for reasons that are NOT cost:

- **Schedules ship STOPPED** (`DefaultScheduleStatus.STOPPED`) — policy about surprise: a
  new schedule does not start spending effort on its own. The owner enables it deliberately.
- **Bounded fan-out ops keep an explicit cap** (e.g. `DEFAULT_MAX_PROFILES_PER_SWEEP`) so a
  first tick cannot fan out into an unbounded sweep. That cap bounds *blast radius*, chosen
  once and reviewable.

Any auto-materialization policy an asset carries MUST be asserted by a test, in the same
spirit as the asset-graph-integrity guard: a policy that exists only as a decorator
argument silently disappears in the next refactor, which is exactly how the bronze→silver
gap arose (bronze landed, silver sat still, because the lineage was correct but nothing
acted on it). The determinism wall needs its own assertion too — an unreachable-submit
check that FAILS when a path is introduced.


## Bronze asset (ig_posts_raw)

- **Manual trigger only** — not scheduled. User provides `ScrapeConfig` via Dagster launchpad.
- **Apify flow:** trigger_run → poll_run → stream_dataset (NDJSON) → Polars read_ndjson → write_parquet
## Gold assets (batch-based, async)

### ig_posts_gen_batches (batch creation, no Gemini)

- **Trigger:** downstream of the label pass (`deps=["ig_post_labels"]`), plus daily schedule
- **Discovery:** dumb drain over `ig_post_labels` (see processed_on semantics above for the query):
  `enrich_decision IN ('standout','control','floor_filler')` AND `label_version` current
  AND no current-prompt `gold_analyses` row AND no open `batch_items` row
- **Action:** creates a `batch_jobs` row and `batch_items` in ops.sqlite (sub-millisecond, no API calls)
- **Watermark:** none — the `gold_ig` watermark is retired; the drain is stateless over labels
- **Empty captions:** skipped at the label pass (`enrich_decision='skip'`, US-L6)
- **Re-enrichment:** explicit `post_ids` bypasses all guards; stale-prompt gold rows re-enqueue automatically

### Enrichment (Dagster-native, batch)

- **Trigger:** `submit_gemini_batches_job` consumes ONE pending, unsubmitted
  `gemini-batch` batch per run; the `gemini_batch_harvest_sensor` issues
  `gemini_batch_harvest` runs when chunks reach terminal state. Batch-native
  growth facets: `uv run python scripts/enrich_facets_batch.py` (interactive
  enrichment removed 2026-09-08).

- **Lifecycle:** gen_batches → media_upload → submit → harvest; the shared
  enrichment logic lives in ``orchestration.defs.engine/`analysis.py`.
- **Retry:** exponential backoff with jitter, `MAX_ATTEMPTS=5`, terminal failures → `dead_letter`

> **Driver sensor (2026-09-15):** `enrichment_harvest_sensor` (interval, ADR-0012 D2) is
> wired and ENABLED — it re-derives the full in-flight set each tick and requests harvest
> runs for terminal partitions only. The retired `gemini_batch_harvest_sensor` is deleted.
> Any schedule still ships stopped; the user enables those deliberately.

> **Target (ADR-0012; queue DROPPED 2026-09-15):** this section describes the pre-drop
> batch queue model. In the target state the lifecycle becomes
> submit → harvest-as-partition-landing into `bronze_enrichment_raw`, with
> orchestration state in the Dagster instance instead of `batch_jobs`/`batch_items`.

### Why batch-based (not synchronous)

| Issue (old) | Fix (new) |
|---|---|
| Asset blocks on API latency (1+ hour materialization) | Batch decouples — materialization is sub-second |
| One 429 kills the batch | Per-item backpressure; worker continues past failures |
| No per-item rate limiting | `scheduled_for` column with exponential backoff |
| Crash → partial writes | Batch is durable; stale items reclaimed on next run |
| Domain coupling (TikTok = copy-paste) | Worker dispatches by `domain` column; same batch system, same worker |

Under ADR-0012 the same anti-blocking rationale is served by Dagster-native
partitions and asset checks; this table describes the current batch queue.

## Serving layer

- `dim_profile`: SCD2 profile dimension. Reads DISTINCT profiles from `silver_ig_posts`. Closes old rows on username change, inserts new rows. `channel = 'instagram'`. Carries `creator_id`/`creator_name` linked from `profiles`/`creators` in ops.
- `v_post_detail`: Foundational flat view joining silver + gold (JSON-extracted) + dim_profile + dim_date. LEFT JOINs throughout — posts without enrichment or profiles still appear.
- Seventeen downstream views: `v_signal` (high-value filter), `v_quality_trend` (weekly aggregates), `v_creator_quality` (creator rankings, gated), `v_rising_creators` (momentum), `v_domain_coverage` (heatmap), `v_engagement_outliers` (label-backed per-post z-scores), `v_outlier_posts` (1σ+ outliers), `v_creator_outlier_rate` (outlier production rate), plus the canonical metric views: `v_post_metrics` (per-post label + point-in-time baseline + comments/views z-scores + `engagement_score` + `is_standout`/`is_hot` (2σ+)/`is_top3_in_owner`; NO creator-avg column), `v_post_baselines` (serving-layer trailing baselines + z-scores for `comments_count` and `video_view_count` — mirrors the likes estimator semantics without touching `ig_post_labels`; views baseline only where `video_view_count` > 0), `v_creator_metrics` (gate-free per-creator counts/avg/max), `v_creator_profile` (per-creator canonical rollup: counts, true avg, `avg_engagement_score`, `momentum_ratio`/`is_rising`, `dominant_domain`), `v_creator_topics` (long-form `(creator_id, topic)`: top-5 by post count and top-5 by baseline-normalized weighted performance), `v_recent_hot_posts` (recent 28-day hot feed), `v_profile_metrics` (per-owner_username counts), `v_overview` (single-row), `v_standout_calendar` (standouts per day-of-month).
- All in `defs/serving/assets.py`, group_name="serving". Dashboard analytics endpoints are thin projectors over these views — no aggregation in `dashboard/server.py` (guard: `tests/unit/dashboard/test_no_aggregation_in_server.py`).



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
```orchestration.defs.engine/`analysis.py`` module inspect ``google.genai.errors.APIError`` attributes
(``code``, ``message``, ``details``) to distinguish subtypes. If the SDK
error isn't parseable it falls back to substring matching on quota-related keywords.

### Video: TPM is the bottleneck

Video is token-intensive. At default ``media_resolution``:

| Per second of video | Tokens |
|---|---|
| Frames (1 FPS, 258/frame) | 258 |
| Audio (32/sec) | 32 |
| **Total** | **~290/sec** |

At low resolution (``media_resolution='MEDIA_RESOLUTION_LOW'``): ~98 tokens/sec (66/frame).

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
- **Use ``media_resolution='MEDIA_RESOLUTION_LOW'``** when fine visual detail isn't needed —
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
**Free — Evaluation only.** Batch API is paid-tier only, so free-tier work is
restricted to non-batch experimentation (prompt spikes, token counting). No
production enrichment.

**Tier 1 — Batch-native (current, 2026-09-08).** ALL enrichment runs through
the Gemini BATCH API: gold analyses via `submit_gemini_batches_job` →
`gemini_batch_harvest`, growth facets (visual + text) via
`scripts/enrich_facets_batch.py`. The interactive per-item path was removed
2026-09-08 — batch is the only execution vehicle at every paid tier.

**Tier 2 — Same batch-native model, higher caps.** Upgrade for in-flight
batch token ceilings (10M → 500M) and spend headroom, not for a different
execution model.

### Trigger points

| When you see... | Upgrade to... |
|---|---|
| Dead letter filling with 429s daily | **Tier 1** |
| Need to clear the backlog this week | **Tier 1** |
| Don't want Google training on your data | **Tier 1** |
| Weekly volume >1,000 posts steady-state | **Tier 2** |
| $2,000/mo spend cap exhausted | **Tier 3** |

### Tier 1 → Tier 2 escalation threshold (numeric)

Current decision (2026-08-12): operate on **Tier 1**. Escalate to Tier 2 when
any of these three numeric triggers fires (monitor via a metric query or the
``batches`` CLI):

|Metric|Threshold|Why|
|---|---|---|
|Weekly post volume|≥ 1,000 posts/week for 2 consecutive weeks|Sustained volume exceeds Tier 1's 10M in-flight batch token cap (chunk into more sequential waves, or escalate)|
|Batch token projection|Any batch job projected > 10M tokens|Tier 1 flash-lite batch cap is 10M; Tier 2 is 500M|
|Rolling 30-day Gemini spend|≥ $200 (80% of Tier 1's $250/mo cap)|Leaves 20% headroom to avoid a hard stop mid-cycle|

Video enrichment runs natively through the batch path (no interactive
processing penalty since 2026-09-08); video remains cost-relevant but is no
longer an execution-model trigger.

### Video scaling

Video adds two bottlenecks beyond text: upload time (File API, 5–15s per
video) and token volume (~17,400 tokens per minute of video at default
resolution). Sequential processing is impractical above ~100 videos/week.
Batch API with concurrent upload workers is required at any video scale.
Tier 2's 500M batch capacity and 20GB File API limit remove the practical
ceilings for video enrichment.


### Batch caps are model-specific (embedding ≠ generation) — addendum 2026-09-02

The 10M/500M batch-token figures above are **Flash-Lite (generation) caps**.
Embedding models carry **separate, much lower** enqueued-token caps. From the
KB embedding spikes (see `agent-knowledgebase/docs/research/spike-lessons.md`,
same Google Cloud billing account):

| Model family | Tier 1 | Tier 2 | Tier 3 |
|---|---|---|---|
| Flash-Lite (generation) | 10M | 500M | — |
| Embedding 2 | 500k | 5M | 10M |

Two facts govern whether these caps constrain you:

1. **The cap is IN-FLIGHT**, not cumulative lifetime volume. It bounds tokens
   enqueued across *active* batch jobs at once. **No corpus size ever hard-blocks
   a tier** — you chunk into sequential batch jobs and each wave fits the cap.
   Tiers select concurrency/embed speed, never feasibility.
2. **Batch API is paid-tier only** — the 50% embedding discount requires Tier 1
   (linked billing). Free-tier embeddings cost $0 but are quota-limited (~1000
   embed/day + RPM/RPD).

So for datalake: if you embed video/image at scale, size each batch job to the
model's in-flight cap and run sequential waves; upgrade a tier to get *fewer,
faster* waves, not because a corpus "won't fit." Cost stays cheap at corpus
scale (~$81 Gemini / ~$31 Voyage for ~25.9K media); quotas, not dollars, gate.

## Smoke testing

Each implementation phase includes a targeted smoke test using a **temporary DuckDB database** (`data/smoke_test.duckdb`) with 2-3 test posts. Zero interference with production state. Smoke DB is deleted after verification.

## IG pipeline library

The old `ig_pipeline` library at `~/repos/ig-pipeline` is imported as a thin Apify client wrapper. Functions (`trigger_run`, `poll_run`, `stream_dataset`) are imported via `sys.path` in `defs/instagram/assets.py`. Extraction into the datalake package is deferred.

## Env vars

Set in `.env`:

| Variable | Default | Used by |
|----------|---------|---------|
| `APIFY_API_TOKEN` | — | `ApifyResource` |
| `IG_DATA_DIR` | `data` | `platform/paths.py` root path |
| `IG_BRONZE_DIR` | `data/lake/bronze` | Bronze asset |
| `IG_SILVER_DIR` | `data/lake/silver` | Silver asset |
| `IG_GOLD_DIR` | `data/lake/gold` | Gold asset |
| `IG_DB_PATH` | `data/state.duckdb` | DuckDB resource |
| `OPS_DB_PATH` | `data/ops.sqlite` | `SQLiteResource` |
| `JOBS_SERVICE_URL` | `http://127.0.0.1:8462` | the inference-service client |
| `IG_LOCAL_INGEST_DIR` | a local checkout path | local ad-hoc ingest root |

`GEMINI_API_KEY` / `GEMINI_TIER` may still be present in `.env`; nothing reads
them since the Gemini path retired (ADR-0015). `platform/paths.py` calls
`load_dotenv()` as its first statement — the one sanctioned import-time side
effect, because it reads the environment into module constants and runs before
any other loader would.

## Test conventions

- `uv run pytest tests/ -v`
- In-memory DuckDB (`:memory:`) via dependency injection
- Parquet tests use `tmp_path`
- One test per behavioral contract, one per edge case
- **Before writing any asset that reads from disk, read ONE real input file and display its schema.** Don't model against test data. Lesson from Phase 2 false start (2026-06-30).
- **Full suite is slow (~15-20 min; exceeds a 600s timeout).** Use scoped runs (`uv run pytest <path>`) during work; run the full suite only as a final gate. Tracked as ISSUES.md #15 — not yet prioritized.

## Decision log

> Canonical decision records (with rationale, alternatives, and evolution) live
> in [`docs/architecture/adr/`](docs/architecture/adr/README.md) — ADRs are the single source of truth for
> *why* decisions were made and superseded. This table is a lightweight,
> chronological index of highlights.

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
| 2026-08-14 | `creators` + `profiles` split (replaces `scrape_targets`) | Multi-platform enabler: creator (person/brand) owns 1..N profiles (account per platform). `dim_profile` carries `creator_id`/`creator_name` for click-through without cross-DB joins. Depth is per-profile. Backfill is 1:1 (IG-only today). |
| 2026-09-10 | Enrichment layered model (ADR-0011) — bronze verbatim → six `silver_*` → four gold marts, keyed `(post_id, platform)` | Deterministic remap from `bronze_enrichment_raw` means schema/mapping changes are replays, not re-bills. LIVE 2026-09-15. Spec: `docs/architecture/pipelines/enrichment.md` (v3) |
| 2026-09-10 | Dagster-native orchestration (ADR-0012) | Retires the `ops.sqlite` queue (`batch_jobs`/`batch_items`/`dead_letter`/`facets_batch_jobs`); retains media/identity/prompt tables. Queue DROP EXECUTED 2026-09-15 (archived first) |
| 2026-09-10 | Inference seam (ADR-0008/0009): three verbs + `submit`/`poll-to-terminal`/`retrieve`, `ProviderAdapter` swap | One seam serves both the qwen-batch-service (async wrapper over a synchronous provider) and Gemini's native batch. Proven in the enrichment spike; not yet wired in |
| 2026-09-15 | Repository layout (ADR-0015): a uv workspace (`services/`, `packages/`), `datalake`→`orchestration`, role-based modules, providers named only in the adapter layer | The layout no longer matched what the code did; a retired provider was still selectable by config; two entry points fought over orchestration state. `src/` is gone, the Gemini path is deleted, `__init__.py` files are docstrings-only |
| 2026-09-16 | Compose + roster boundary (ADR-0017): one Compose brings up orchestration/jobs/dashboard; the dashboard owns the creator roster and serves it over HTTP; persisted media paths translate through a host↔container prefix map | The pipeline was hand-started from a separate checkout, and the roster had two writers on one `ops.sqlite`. Roster now crosses the boundary as a published bronze source; media paths are stored in the writer's vocabulary and translated at read |
| 2026-09-16 | Pipeline automation + cost boundary (ADR-0018): a downstream materialization runs its upstream work; cost is enforced at the API credential, never in the graph | A real run landed 4 bronze rows while `silver_visual_annotations` stayed at 0 — lineage was correct but nothing acted on it, so the chain stopped at bronze. Manual submit existed as a spend guard; with cost protected at the key, that guard has no purpose in the graph |

## Verification plane — defense in depth (2026-09-15, BINDING)

**The principle:** "unit tests green" is evidence that a process ran, not that the system
works. Unit tests raise the PROBABILITY that something works; they are not confirmation.
Confirmation is a tracer shot: one real run of the changed path, end-to-end, through a slice,
with the destination verified. Every agent and subagent in this repo is bound by this —
"tests pass" is never an acceptable completion claim on its own.

This is not theoretical. The Enrichment v3 migration shipped with ~700 green tests and a
pipeline that could not run one cycle. The remediation session's FIRST real enrichment run
surfaced three defects in minutes (empty adapter registry, a Protocol method neither adapter
implemented, a CLI argument iterated character-by-character) — every one invisible to a green
suite. Full ledger: `tasks/lessons.md` 2026-09-15.

### The four controls (binding on orchestrator and subagents alike)

1. **Read before dispatch.** Before any unit that touches an existing subsystem, the
   orchestrator greps the test tree and docs for that subsystem and attaches what it finds to
   the brief. An existing test that imports a symbol encoding a design IS a specification.
2. **Story ACs in every brief.** A worker's acceptance includes the user story's binary AC
   list, not just the unit's own criteria. "The unit is green" says nothing about the
   consumer the story names.
3. **Conformance over existence.** For any Protocol, schema catalog, or prompt schema: a
   runtime check (`isinstance` against a `runtime_checkable` Protocol, catalog-vs-producer
   column diff, fake-accepts-full-signature) runs in CI. "X exists" is not "X conforms".
4. **One real run as the acceptance gate.** The deterministic smoke slice
   (`scripts/make_smoke_slice.py`, ~100 posts with media bytes, own roots) makes a real
   end-to-end run cheap (~$0.03). "Done" = the changed path executed against it and the
   destination was verified — never the log line.

### The enforcement plane (layered so one miss does not sink the migration)

| Layer | Gate | Catches |
|---|---|---|
| 0 — author | runtime Protocol conformance; catalog-vs-producer column diff; fakes accept the FULL current signature | missing methods, schema drift, fake drift |
| 1 — unit | scoped pytest on the changed files | slice-local logic |
| 2 — load | `uv run dagster definitions validate -m datalake.definitions`; full suite on a SETTLED tree only | unloadable graphs, cross-module breakage |
| 3 — integration | smoke-slice e2e: real provider call -> verbatim bronze -> conform -> silver -> mart query returns rows | everything the unit layer structurally cannot see |
| 4 — retirement | archive count == live count in the SAME run; blocking anti-join check; per-table drops with the KEEP-set assertion | silent loss, destructive-DDL drift |

**The done bar, in one line:** green suite AND materialized destination AND one observed run
through the slice. Any one alone is not done.

### Enrichment v3 — verified state, 2026-09-15

Remediation units W0–W9 are discharged. Verified against the live store, not
against claims:

- **Layered model live.** `bronze_enrichment_raw` 9,576 rows (verbatim, Parquet);
  six `silver_*` tables published; four gold marts materialized; all 27 serving
  assets materialize.
- **Parity PASS.** Pre-migration `gold_analyses` vs live
  `silver_content_classification`: 9,576 rows both sides, joined on `post_id`,
  **0 both-non-null conflicts**. 30 rows differ only where the old value was NULL
  and silver populates it — better data, never divergent data.
- **Replay purity proven (the keystone claim).** Re-publishing silver from bronze
  changed ONLY the 8 sentinel rows; the other 9,568 were byte-identical across 15
  business columns. A schema/mapping change is a replay, not a re-bill.
- **Sentinel defect fixed.** Two sibling producers defined `MODEL_LEGACY_NULL`
  differently, so 8 live rows carried the ADR-REJECTED literal. Now a single
  definition in ``orchestration.defs.platform.schemas`` imported by both; the 8 rows read
  `unrecorded-legacy-null`.
- **DQ gates exist and FIRE.** `check_no_silent_loss` (blocking anti-join),
  `check_quarantine_growth`, `check_silver_snapshot_freshness`, plus
  `v_quarantine_triage`. Each is proven to fail on injected bad state — a check
  that only ever passes is the defect that unit exists to fix.
- **Queue retirement EXECUTED 2026-09-15** (owner-approved). All 7 tables archived
  (export count == live count, same run, 26,465 rows) then dropped per-table;
  KEEP set asserted intact and non-empty after. Every live count matched the
  2026-09-14 baseline — zero drift. **Proof the rebind held: all seven serving
  views still resolve with `gold_analyses` gone.** Pre-drop snapshots in
  `data/backups/*.pre-w9-drop`; log in `data/logs/w9-retirement-<utc>.json`.

**Still open (logged in ISSUES.md, not silently carried):** #27 (`conform_silver.py`
`--silver-root` defaults to the live lake), #28 (Gemini module removal blockers),
#29 (`--plan` has no Dagster equivalent), #30 (no test constructs the asset graph,
no pytest markers), #31 (full suite does not finish clean; cause UNVERIFIED), #32
(reconciliation is now gated; the open qwen job was accepted as a recorded
decision). The qwen facets pass is still driven by the hand-rolled CLI — promoting
it into Dagster ops is the remaining ADR-0012 work.
