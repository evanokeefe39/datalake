# Core pipeline — Instagram posts, end to end (CURRENT)

> **Entry point:** [`../README.md`](../README.md) — the architecture index.
> System-wide shape (medallion layers, storage split, lineage, dead letter) lives
> in [`../design.md`](../design.md); this doc does not duplicate it.
>
> This document describes the pipeline that **runs today**. It does not describe
> the enrichment re-layering (ADR-0011, not yet implemented) — that spec is
> [`enrichment.md`](enrichment.md). Decision records: ADR-0011
> ([`../adr/0011-enrichment-layered-model.md`](../adr/0011-enrichment-layered-model.md)),
> ADR-0012
> ([`../adr/0012-dagster-native-orchestration.md`](../adr/0012-dagster-native-orchestration.md)).

## What this pipeline is

This is the core (non-enrichment) Instagram posts pipeline: how a post travels
from an Apify scrape to a queryable serving view. It is a Dagster asset graph in
four stages — **ingest → bronze → silver → labels**, with a **serving** layer
built on top — implemented in `src/datalake/defs/instagram/assets.py`,
`src/datalake/defs/serving/assets.py`, and the shared resources in
`src/datalake/defs/common/`.

What it is **not**: this pipeline does no model work. The enrichment path that
consumes the labels (Gemini analysis of `enrich_decision`-approved posts) is a
separate pipeline documented in [`enrichment.md`](enrichment.md). The seam
between them is the drain asset `ig_posts_gen_batches`, described below — it is
the last asset on this side of the boundary.

The stages, with their real asset names:

```
ig_posts_raw ──→ ig_posts_slv ──→ ig_post_labels ──→ ig_posts_gen_batches ─ (enrichment, other pipeline)
      │                │                                     │
      └ ig_posts_local_raw (second bronze producer)          └ ops.sqlite: batch_jobs / batch_items
ig_posts_slv ──→ dim_profile (SCD2), dim_date ──→ v_post_detail ──→ downstream serving views
```

## Stage 1 — Ingest (Apify): `ig_posts_raw`

- **Runs it:** the Dagster asset `ig_posts_raw` (`defs/instagram/assets.py`),
  parameterized by `ScrapeConfig`, using the `ApifyResource` (APIFY_API_TOKEN)
  and `SQLiteResource` (ops.sqlite) resources.
- **What it does:** triggers the `apify~instagram-scraper` actor via
  `trigger_run`, polls to completion with `poll_run`, then streams the run's
  dataset as NDJSON via `stream_dataset` into a local `.jsonl`, loads it with
  Polars `read_ndjson`, and writes typed Parquet.
- **Writes:** `data/lake/bronze/{dataset_id}.parquet` (path helper
  `bronze_path()` in `defs/common/lake.py`), plus a `.meta` JSON sidecar next to
  it (run id, dataset id, actor, item count, requested URLs, estimated cost).
  Media bytes are cached at scrape time into `data/media/…` (via
  `cache_media_bytes`) while the CDN URLs are fresh — silver never does network
  I/O; producers own ingestion-time caching.
- **Idempotency:** if the Parquet file for a dataset_id already exists, the
  asset re-reads it and returns without re-downloading or re-caching.
- **State:** none beyond the files themselves (bronze is file-based; see the
  watermarks section for how silver discovers new files by mtime).

A second bronze producer, `ig_posts_local_raw`, ingests local ad-hoc scrape
dumps into the same bronze directory and schema — the silver asset reads both.

The bronze contract — what columns the files carry and who produces them — is
specified once in [`../bronze-schema.md`](../bronze-schema.md); this doc does
not restate it.

## Stage 2 — Silver: `ig_posts_slv`

- **Runs it:** the Dagster asset `ig_posts_slv` (`deps=["ig_posts_raw",
  "ig_posts_local_raw"]`), using the `DuckDBResource`.
- **Reads:** bronze Parquet files under `IG_BRONZE_DIR` (default
  `data/lake/bronze/`) whose file mtime is newer than the `silver_ig`
  watermark (see [Incrementality](#incrementality-watermarks-processed_on-and-the-drain)).
- **Normalizes:** renames camelCase Apify columns to snake_case silver schema
  (`_BRONZE_TO_SILVER` map), derives `media_files` (JSON) + `media_count`,
  coalesces `owner_username` from `username` when `ownerUsername` is missing,
  derives post URLs from shortcodes, parses the timestamp column, drops Apify
  extras, and filters out rows without a valid `post_id`.
- **Dedups:** the union of existing silver state plus new files is handed to
  DuckDB over Arrow and deduplicated with
  `SELECT DISTINCT ON(post_id) … ORDER BY post_id, scraped_at DESC NULLS LAST,
  source_dataset DESC` — newest scrape wins, with `scraped_at` derived from the
  bronze `.meta` `downloaded_at` (falling back to file mtime). `scraped_at` is
  transient: it drives dedup ordering and observation provenance but is dropped
  before persisting.
- **Writes:** the deduplicated result is upserted back into DuckDB
  (`INSERT OR REPLACE INTO silver_ig_posts`), and every bronze file also
  appends one raw observation per post per bronze file into
  `silver_ig_post_observations` (`INSERT OR IGNORE` on PK `(post_id,
  source_dataset)` — pre-dedup, so the point-in-time engagement history is
  never collapsed). The PolarsIOManager Parquet output for the asset's own
  return value uses the `IG_SILVER_DIR` convention via
  `defs/common/lake.py`.
- **State:** DuckDB tables `silver_ig_posts`, `silver_ig_post_observations`,
  and the `silver_ig` watermark row (DDL from `defs/common/schemas.py`).
- **Verified row counts (read-only query against `data/state.duckdb`,
  2026-09-10):** `silver_ig_posts` = 10,038 rows;
  `silver_ig_post_observations` = 12,701 rows. (A figure of ~2,200 posts floats
  around the repo and is stale — do not cite it.)

`ig_posts_slv` is a pure transform — no network I/O, no caching. Re-running
with no new bronze files is a no-op that returns existing state.

## Stage 3 — Labels: `ig_post_labels` (the pass between silver and enrichment)

- **Runs it:** the Dagster asset `ig_post_labels` (`deps=["ig_posts_slv"]`),
  daily per its schedule; the logic lives in
  `defs/instagram/labels.py` (`run_label_pass`).
- **What it does:** for every silver post it reads the latest non-sentinel
  observation from `silver_ig_post_observations`, judges it against the
  trailing **Tukey-fence baseline** (post Q3 + 1.5·IQR) of the creator's
  matured posts, and stamps a row into the `ig_post_labels` DuckDB table:
  `label` (standout / average / unjudgeable / insufficient_baseline),
  `method` (`day0_heuristic` provisional or `day7_matched` final),
  `enrich_decision` (standout / control / floor_filler / skip), baseline
  center/spread/n, and `label_version`.
- **Maturity semantics:** provisional day0 labels upgrade **exactly once** to
  day7 when a core post matures; day7 labels are immutable under re-judgment.
  Empty-caption posts get `enrich_decision='skip'` and never reach the drain.
- **Self-versioning:** `LABEL_VERSION` (currently 1) is bumped in the same
  commit as any estimator/rule change; the pass recomputes every row whose
  `label_version` differs.
- **State:** DuckDB table `ig_post_labels` (verified: 10,038 rows, matching
  silver — every silver post carries a label).

The serving-layer view `v_engagement_outliers` joins `ig_post_labels` onto
`v_post_detail`, so the labels surface directly in serving as label-backed
z-scores and outlier tiers (ADR-0006 point-in-time semantics).

## Stage 4 — The drain: `ig_posts_gen_batches` (the seam to enrichment)

- **Runs it:** the Dagster asset `ig_posts_gen_batches`
  (`deps=["ig_post_labels"]`), configured by `GoldConfig`.
- **What it does:** a deliberately dumb drain over the labels table. It selects
  posts whose label pass approved them for enrichment
  (`enrich_decision IN ('standout', 'control', 'floor_filler')` at the current
  `LABEL_VERSION`) that (a) have no `gold_analyses` row with the
  `CURRENT_PROMPT_HASH` — stale gold rows are re-eligible — and (b) have no
  open item in `ops.sqlite` `batch_items` (`status IN ('pending',
  'processing')`). It creates a batch via `create_batch()` with one JSON
  payload per post.
- **Mode:** batch mode (`gemini-batch`, the BATCH API, ~50% cheaper) is the
  default whenever the active Gemini tier supports it
  (`GeminiTierConfig.detect().supports_batch`); it falls back to `interactive`
  on the free tier or when `prefer_interactive` is set.
- **State:** `ops.sqlite` tables `batch_jobs` / `batch_items` (queue lifecycle
  described in [`../design.md`](../design.md), "Batch coordination"; failures
  dead-letter per [`../adr/0004-ops-sqlite-state-duckdb-deadletter.md`](../adr/0004-ops-sqlite-state-duckdb-deadletter.md)). The `gold_ig` watermark is **retired** — the
  labels table is the discovery source, no watermark is consulted here.
- **Beyond the seam:** from here the enrichment pipeline takes over (submit /
  harvest / `gold_analyses`, the gold DuckDB table — verified 9,576 rows).
  `gold_analyses` and `gold_growth_facets` are the CURRENT gold model; the
  bronze→six-silver→four-marts re-layering is TARGET (ADR-0011,
  [`enrichment.md`](enrichment.md)).

## Serving layer: dims and views

- **Runs it:** assets in `defs/serving/assets.py` (group `serving`), the only
  cross-domain module.
- **`dim_profile`:** SCD2 profile dimension. Reads distinct `owner_id` /
  `owner_username` from `silver_ig_posts`, links `creator_id` / `creator_name`
  from the `profiles` / `creators` tables in ops.sqlite, and maintains
  `effective_from` / `effective_to` / `is_current` in DuckDB — closing the old
  row and inserting a new one when an identity changes. Asset checks
  (`defs/serving/asset_checks.py`) assert no gaps between consecutive intervals.
- **`dim_date`:** a generated date dimension for consistent time-based
  aggregation.
- **`v_post_detail`:** the foundational flat view — silver posts LEFT JOINed
  with `gold_analyses` (JSON-extracted into typed columns), `dim_profile`, and
  `dim_date`. Posts without enrichment or profiles still appear. Downstream
  serving views (`v_signal`, `v_quality_trend`, `v_creator_quality`,
  `v_rising_creators`, `v_domain_coverage`, `v_engagement_outliers`,
  `v_outlier_posts`, `v_creator_outlier_rate`, and the canonical metric views
  `v_post_metrics`, `v_creator_metrics`, `v_profile_metrics`, `v_overview`,
  `v_standout_calendar`, among others) all read from it. The full enumeration
  lives in `AGENTS.md` (the "DuckDB views" list in its pipeline inventory) —
  it is not duplicated here, and there is no dedicated doc page for the view
  list outside `AGENTS.md`.
- **Contract:** per
  [ADR-0005](../adr/0005-thin-projector-serving.md) the dashboard (`dashboard/server.py`) is a thin
  projector over these views — view SELECT + WHERE/ORDER/LIMIT, no aggregation
  in the server (guarded by `tests/unit/dashboard/test_no_aggregation_in_server.py`).
  Per [ADR-0006](../adr/0006-point-in-time-metric-semantics.md) all metrics are point-in-time and baseline-normalized, computed
  in the warehouse, never in the client.

A reader can now trace a post end to end: Apify actor run →
`data/lake/bronze/{dataset_id}.parquet` → `silver_ig_posts` (DuckDB, deduped,
`processed_on` stamped) → `ig_post_labels` (Tukey judgment) →
`ig_posts_gen_batches` → (enrichment) → `gold_analyses` → joined into
`v_post_detail` → the dashboard.

## Storage and engine boundary

Three backends, each chosen for its access pattern (fuller treatment in
[`../design.md`](../design.md), "Storage split" / "Engine boundary"):

| Backend | Role in this pipeline |
|---|---|
| Parquet lake (`data/lake/`, env-overridable via `IG_DATA_DIR` / `IG_BRONZE_DIR` / `IG_SILVER_DIR` / `IG_GOLD_DIR`) | Bulk files: bronze scrapes, asset outputs. Polars does all file I/O (`read_ndjson`, `read_parquet`, `write_parquet`). |
| DuckDB (`data/state.duckdb`) | Authoritative state: `silver_ig_posts`, `silver_ig_post_observations`, `ig_post_labels`, `watermarks`, `dim_profile`/`dim_date`, all views. All SQL transforms run here. |
| SQLite (`data/ops.sqlite`) | Operational coordination only: scrape config (`profiles`), creator identity, the enrichment queue (`batch_jobs`/`batch_items`), media caches. |

Arrow is the zero-copy interchange between Polars and DuckDB
(`DataFrame.to_arrow()` registered as a DuckDB relation, results read back with
`pl.from_arrow(...)`). Bronze bypasses the `PolarsIOManager` (dynamic
dataset_id paths, direct `write_parquet`); other assets use the
`PolarsIOManager` (`defs/common/resources.py`), which persists a returned
Polars DataFrame to `data/lake/<asset_key>.parquet` and reloads it on input.

## Incrementality: watermarks, `processed_on`, and the drain

**Watermarks.** One DuckDB table, `watermarks (name, timestamp)`, tracks
progress per pipeline. Silver uses the `silver_ig` row: at materialization,
`ig_posts_slv` re-reads only bronze Parquet files whose mtime is newer than the
watermark, then advances the watermark to the newest file examined. No new
files → no-op. (Profiles use the analogous `profiles_ig` watermark; the
`gold_ig` watermark is retired — see the drain.)

**`processed_on`.** Set exactly once, when a post **first appears** in silver
(null → `now` after dedup), and **never re-stamped** on subsequent scrapes even
as engagement metrics change. The code enforces this structurally: before dedup,
each post's existing non-null `processed_on` is carried forward across all its
rows (`fill_null(processed_on.max().over(post_id))`), so the "newest scrape
wins" tie-break can never overwrite it with the current time. This is what
makes incremental processing sound — downstream consumers can treat
`processed_on` as the post's first-seen timestamp.

**The discovery drain.** `ig_posts_gen_batches` needs no watermark of its own:
it is a stateless anti-join over state that already exists —
`ig_post_labels` (approved, current version) minus `gold_analyses` (current
prompt hash) minus open `batch_items`. Re-running it can only ever enqueue what
is genuinely new.

**Why re-runs are safe, stage by stage:**

- Bronze: a materialization for an existing dataset_id re-reads the existing
  Parquet (idempotency check on the destination path).
- Silver: watermark-gated file discovery + `INSERT OR REPLACE` upsert + the
  carried-forward `processed_on` — re-running is a no-op or a pure re-dedup.
- Observations: `INSERT OR IGNORE` on the `(post_id, source_dataset)` PK.
- Labels: idempotent pass; day7 rows immutable, day0 upgrades exactly once,
  version-mismatched rows recompute.
- Drain: the anti-join guards (`gold_analyses` prompt hash, open batch items)
  mean a re-run never re-pays for work already submitted or done.

## Current vs target — risks in this pipeline

This doc describes what **runs today**. Two ratified-but-unbuilt decisions
touch it:

1. **[ADR-0012](../adr/0012-dagster-native-orchestration.md) (Dagster-native orchestration) — not yet implemented.** The
   drain's in-flight guard reads `batch_items` in `ops.sqlite`
   (`defs/instagram/assets.py`, the `SELECT payload FROM batch_items WHERE
   status IN ('pending', 'processing')` anti-join). ADR-0012 retires
   `batch_jobs` / `batch_items` and moves queue state into the Dagster
   instance — so **that guard must be re-derived from the Dagster instance
   when ADR-0012 ships, or the drain will double-submit** (the same
   double-submit hazard called out in the ADR-0012 section of
   [`../design.md`](../design.md)). Treat this as a Phase 2 dependency of the
   orchestration migration, not an optional refactor.

2. **[ADR-0011](../adr/0011-enrichment-layered-model.md) (layered enrichment) — not yet implemented.** The gold model this
   pipeline feeds today is `gold_analyses` + `gold_growth_facets` written by
   the batch jobs. The target model (bronze `bronze_enrichment_raw` → six
   `silver_*` conform tables → four gold marts) is specified in
   [`enrichment.md`](enrichment.md); nothing in this doc's stages changes
   under it except what sits beyond the `ig_posts_gen_batches` seam.

Also note: the external enrichment worker is **removed**, not running
([ADR-0002](../adr/0002-external-worker-rest-materialization.md) superseded by
[ADR-0007](../adr/0007-batch-native-enrichment-deprecate-worker.md)). The
submit/harvest roles it once held now live in Dagster jobs; there is no
standalone enrichment process in this repo.
