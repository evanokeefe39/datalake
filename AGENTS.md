# datalake — Agent operating context

This repo is operated by Claude. Keep this file current — Claude reads it on every session.

## Key rules

- Never use `pip`. Always use `uv` for Python package management.
- Work on `feat/*`, `fix/*`, `chore/*` branches; squash-merge to `main` via PR.
- Conventional commits only: `type(scope): summary`.
- No direct pushes to `main`.
- Never use PowerShell.

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

**Pending user decision:** video at scale is a Tier 2 batch-API problem (free
tier skips video in the worker gate). Tier choice is a cost call, not infra.



## Architecture
**Medallion lakehouse with async enrichment batches:**

| Layer | Storage | Writer | State tracking |
|-------|---------|--------|----------------|
| Bronze | Parquet (`data/lake/bronze/`) | Polars (direct write) | None — file-based |
| Silver | Parquet (`data/lake/silver/`) | PolarsIOManager | DuckDB `silver_ig_posts` + watermarks |
| Batches | SQLite (`data/ops.sqlite`) | `ig_posts_gen_batches` | `batch_jobs` + `batch_items` |
| Gold | DuckDB table | `enrichment_worker` (standalone) | `gold_analyses` (AssetSpec, externally materialized) |
| Serving | DuckDB views + tables | DuckDB | `dim_profile` (SCD2), `dim_date`, 14 analytics views (incl. 5 canonical metric views) |

**SQLite for operational state, DuckDB for analytical state:**
- `ops.sqlite` — batch coordination, media cache, dead_letter (OLTP: point lookups, frequent updates)
- `state.duckdb` — silver tables, gold_analyses, watermarks, serving dims/views (OLAP: scans, aggregations)

**Domain-based structure, not layer-based:**

src/datalake/defs/
├── common/          # PolarsIOManager, ApifyResource, GeminiResource, SQLiteResource, lake.py, schedules.py
├── enrichment/      # batch.py, assets.py, prompts.py
├── instagram/       # ig_posts_raw, ig_posts_slv, ig_posts_gen_batches, config
└── serving/         # dim_profile, dim_date, v_post_detail + 13 downstream views (incl. 5 canonical metric views)

**Storage split:**
- **Parquet lake** — bulk data, lock-free parallel writes
- **DuckDB** (`data/state.duckdb`) — authoritative current state, watermarks, SCD2 dims, views

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

**DuckDB views:** `v_post_detail` (foundational), `v_signal`, `v_quality_trend`, `v_creator_quality`, `v_rising_creators`, `v_domain_coverage`, `v_engagement_outliers`, `v_outlier_posts`, `v_creator_outlier_rate`, `v_post_baselines` (serving-layer comments/views point-in-time baselines), `v_post_metrics` (canonical per-post metrics), `v_creator_metrics` (gate-free per-creator activity), `v_creator_profile` (per-creator canonical rollup: momentum + dominant domain), `v_creator_topics` (per-creator top-5 topics by count and performance), `v_profile_metrics` (per-profile counts), `v_overview` (single-row), `v_standout_calendar` (standouts per day-of-month)


## Watermarks pattern

A single `watermarks` table replaces per-pipeline progress tables. Any pipeline stamps its row:

```sql
CREATE TABLE watermarks (name TEXT PRIMARY KEY, timestamp TIMESTAMP NOT NULL);
```

- Silver reads/writes `watermarks WHERE name = 'silver_ig'`
## Dead letter pattern

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

`src/datalake/defs/common/schemas.py` is the canonical schema definition for both databases.
`tests/operational/expected_schema.py` re-exports it for backward compatibility.
Any table the pipeline reads or writes must be listed here. The readiness test
(`test_state_compatibility.py`) asserts the catalog matches the running databases.
**DuckDB tables:** `silver_ig_posts`, `gold_analyses`, `watermarks`, `dim_profile`, `dim_date`
**SQLite tables:** `batch_jobs`, `batch_items`, `media_metadata`, `media_cache`, `dead_letter`, `creators`, `profiles`, `creator_merges`
**Views:** `v_post_detail`, `v_signal`, `v_quality_trend`, `v_creator_quality`, `v_rising_creators`, `v_domain_coverage`, `v_engagement_outliers`, `v_outlier_posts`, `v_creator_outlier_rate`, `v_post_metrics`, `v_creator_metrics`, `v_profile_metrics`, `v_overview`, `v_standout_calendar`
- **Missing tables/columns** — fails with "run the pipeline or migration"
- **Stale table names** — tables in the DB that were renamed/dropped (e.g. `gold_ig_analyses`). Fails with migration hint.
- **Extra tables** — tables in the DB not in the catalog. Warns, doesn't fail (may be legitimate).

## Operational scripts

| Script | Purpose |
|---|---|
| `scripts/run_pipeline.py` | Thin entry point → delegates to ``python -m datalake.cli``. Subcommands: ``run`` (pipeline), ``batches`` (inspect/reset), ``watermarks`` (inspect/reset). |
| `scripts/migrate_schema_drift.py` | Apply schema migrations: rename tables, move data between DBs, drop vestigial tables. Idempotent. |
| `scripts/migrate_to_v2.py` | One-shot migration from Phase 1-4 schema to v2 domain-scoped tables. |
| `scripts/migrate_from_ig_pipeline.py` | Import bronze Parquet from legacy ig-pipeline repo. |
| `scripts/migrate_owner_username.py` | Backfill null ``owner_username`` in silver from bronze ``username`` fallback. Idempotent. |
| `scripts/migrate_creators_profiles.py` | Split `scrape_targets` → `creators` + `profiles` (1:1 backfill), recreate lost batch tables, drop `scrape_targets`. Idempotent. |
| `scripts/migrate_curated_creator_merge.py` | Consolidate duplicate auto-creators into curated identities (21→`bywaviboy`, 147→`vinny_creative`; 243/610 retired). Reassigns profiles, records `creator_merges`, refreshes `dim_profile`. Idempotent, `--undo` reverses. Replaces `migrate_owner_profiles.py`. |
## Stale analysis update

When the enrichment prompt or model changes, existing `gold_analyses` rows have stale `prompt_hash`.
`check_prompt_currency` detects these. To re-process:

```
uv run python scripts/run_pipeline.py --update-stale-analyses
```

This queries `gold_analyses WHERE prompt_hash IS NULL OR prompt_hash != CURRENT_PROMPT_HASH`,
batches them directly (bypassing the watermark + NOT EXISTS guard), and the enrichment
worker picks them up and UPSERTs fresh analyses with the current prompt.

## DAGSTER_HOME

Set in `.env` to `C:/Users/evano/repos/datalake/data/dagster_home`. Both `dagster dev`
and CLI commands (`dagster asset materialize`, `dagster job execute`) share this instance.
Without it, CLI runs go to a different temp directory and aren't visible in the UI.


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

### enrichment_worker (standalone CLI)

- **Trigger:** run directly: `uv run python scripts/enrichment_worker.py` (no sensor needed)
- **Lifecycle:** claims oldest pending batch → processes items with per-item retry → POSTs materialization to Dagster via REST
- **Retry:** exponential backoff with jitter, `MAX_ATTEMPTS=5`, terminal failures → `dead_letter`

### Why batch-based (not synchronous)

| Issue (old) | Fix (new) |
|---|---|
| Asset blocks on API latency (1+ hour materialization) | Batch decouples — materialization is sub-second |
| One 429 kills the batch | Per-item backpressure; worker continues past failures |
| No per-item rate limiting | `scheduled_for` column with exponential backoff |
| Crash → partial writes | Batch is durable; stale items reclaimed on next run |
| Domain coupling (TikTok = copy-paste) | Worker dispatches by `domain` column; same batch system, same worker |

## Serving layer

- `dim_profile`: SCD2 profile dimension. Reads DISTINCT profiles from `silver_ig_posts`. Closes old rows on username change, inserts new rows. `channel = 'instagram'`. Carries `creator_id`/`creator_name` linked from `profiles`/`creators` in ops.
- `v_post_detail`: Foundational flat view joining silver + gold (JSON-extracted) + dim_profile + dim_date. LEFT JOINs throughout — posts without enrichment or profiles still appear.
- Thirteen downstream views: `v_signal` (high-value filter), `v_quality_trend` (weekly aggregates), `v_creator_quality` (creator rankings, gated), `v_rising_creators` (momentum), `v_domain_coverage` (heatmap), `v_engagement_outliers` (label-backed per-post z-scores), `v_outlier_posts` (1σ+ outliers), `v_creator_outlier_rate` (outlier production rate), plus the canonical metric views: `v_post_metrics` (per-post label + point-in-time baseline + comments/views z-scores + `engagement_score` + `is_standout`/`is_hot` (2σ+)/`is_top3_in_owner`; NO creator-avg column), `v_post_baselines` (serving-layer trailing baselines + z-scores for `comments_count` and `video_view_count` — mirrors the likes estimator semantics without touching `ig_post_labels`; views baseline only where `video_view_count` > 0), `v_creator_metrics` (gate-free per-creator counts/avg/max), `v_creator_profile` (per-creator canonical rollup: counts, true avg, `avg_engagement_score`, `momentum_ratio`/`is_rising`, `dominant_domain`), `v_creator_topics` (long-form `(creator_id, topic)`: top-5 by post count and top-5 by baseline-normalized weighted performance), `v_profile_metrics` (per-owner_username counts), `v_overview` (single-row), `v_standout_calendar` (standouts per day-of-month).
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
``enrichment_worker`` op inspect ``google.genai.errors.APIError`` attributes
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

### Approach per tier

**Free — Evaluation only.** ``enrichment_worker`` processes via queue with
per-item backpressure. Not for production volume.

**Tier 1 — Interactive via queue.** Queue-based enrichment handles routine
volume with per-item rate limiting. Batch API deferred; re-introduce as a
worker variant when cost savings justify the complexity.

**Tier 2 — Batch-first (future).** When batch API is re-introduced, all
enrichment via batch. Interactive asset for ad-hoc single-post only.

### Trigger points

| When you see... | Upgrade to... |
|---|---|
| Dead letter filling with 429s daily | **Tier 1** |
| Need to clear the backlog this week | **Tier 1** |
| Don't want Google training on your data | **Tier 1** |
| Batch job exceeds 10M token limit | **Tier 2** |
| Weekly volume >1,000 posts steady-state | **Tier 2** |
| Adding video enrichment | **Tier 2** (immediately) |
| $250/mo spend cap exhausted | **Tier 2** |
| $2,000/mo spend cap exhausted | **Tier 3** |
### Tier 1 → Tier 2 escalation threshold (numeric)

Current decision (2026-08-12): operate on **Tier 1**. Escalate to Tier 2 when
any of these three numeric triggers fires (monitor via a metric query or the
``batches`` CLI):

|Metric|Threshold|Why|
|---|---|---|
|Weekly post volume|≥ 1,000 posts/week for 2 consecutive weeks|Interactive-only processing exceeds per-item rate limiting at sustained volume|
|Batch token projection|Any batch job projected > 10M tokens|Tier 1 flash-lite batch cap is 10M; Tier 2 is 500M|
|Rolling 30-day Gemini spend|≥ $200 (80% of Tier 1's $250/mo cap)|Leaves 20% headroom to avoid a hard stop mid-cycle|

Video enrichment remains an immediate Tier 2 trigger regardless of these
metrics (token volume + upload time make interactive processing impractical
above ~100 videos/week).


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
| `GEMINI_API_KEY` | — | `GeminiResource` |
| `IG_DATA_DIR` | `data` | `lake.py` root path |
| `IG_BRONZE_DIR` | `data/lake/bronze` | Bronze asset |
| `IG_SILVER_DIR` | `data/lake/silver` | Silver asset |
| `IG_GOLD_DIR` | `data/lake/gold` | Gold asset |
| `IG_DB_PATH` | `data/state.duckdb` | DuckDB resource |

## Test conventions

- `uv run pytest tests/ -v`
- In-memory DuckDB (`:memory:`) via dependency injection
- Parquet tests use `tmp_path`
- One test per behavioral contract, one per edge case
- **Before writing any asset that reads from disk, read ONE real input file and display its schema.** Don't model against test data. Lesson from Phase 2 false start (2026-06-30).
- **Full suite is slow (~15-20 min; exceeds a 600s timeout).** Use scoped runs (`uv run pytest <path>`) during work; run the full suite only as a final gate. Tracked as ISSUES.md #15 — not yet prioritized.

## Decision log

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
