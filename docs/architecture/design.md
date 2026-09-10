# Design — Medallion Lakehouse with Async Enrichment

> **Entry point:** [`README.md`](README.md) — the architecture index, including
> the single current-vs-target table. Start there if you are new.
>
> This is the **main design document**: how the system is shaped, storage,
> engine boundaries, lineage. It describes the **as-built** design. For the
> **target** — two ratified decisions the code does not yet implement — see
> [`enrichment.md`](pipelines/enrichment.md) and
> [`inference-service.md`](services/inference.md), plus
> [ADR-0011](adr/0011-enrichment-layered-model.md) and
> [ADR-0012](adr/0012-dagster-native-orchestration.md).
>
> For **rationale and decision history** (why the design is shaped this way, and
> what it superseded), see the [Architecture Decision Records](adr/README.md).
> For pictorial onboarding, see [`docs/education/`](../education/).
>
> _Note: this document previously linked a rendered `enrichment-architecture.html`
> that was never committed; the link is removed rather than left dangling._

### Key decision records

| Decision | ADR | Status |
|---|---|---|
| Enrichment output is an ingested source, not a transform (LLM boundary) | [0001](adr/0001-enrichment-as-ingested-source.md) | Accepted |
| External worker + gold as AssetSpec (REST materialization, not Pipes) | [0002](adr/0002-external-worker-rest-materialization.md) | Accepted |
| No LLM/API calls in the transform layer | [0003](adr/0003-no-api-in-transform-layer.md) | Accepted |
| ops.sqlite vs state.duckdb split; dead-letter + queue | [0004](adr/0004-ops-sqlite-state-duckdb-deadletter.md) | Accepted |
| Metrics in warehouse views; thin-projector dashboard | [0005](adr/0005-thin-projector-serving.md) | Accepted |
| Point-in-time-only metric semantics | [0006](adr/0006-point-in-time-metric-semantics.md) | Accepted |
| Enrichment layered model: bronze raw landing → six silver conform tables → four gold marts | [0011](adr/0011-enrichment-layered-model.md) | Accepted (not yet implemented) |
| Dagster-native orchestration; retire the ops.sqlite queue | [0012](adr/0012-dagster-native-orchestration.md) | Accepted (not yet implemented) |

## Medallion layers

```
Bronze ──→ Silver ──→ Gold ──→ Serving
(Parquet)   (Parquet   (DuckDB)   (DuckDB views
            + DuckDB)              + tables)
```

| Layer | Storage | Writer | State tracking |
|---|---|---|---|
| Bronze | Parquet (`data/lake/bronze/`) | Polars (direct write) | None — file-based |
| Silver | Parquet (`data/lake/silver/`) | PolarsIOManager | DuckDB `silver_ig_posts` + watermarks |
| Enqueue | SQLite (`data/ops.sqlite`) | `ig_posts_gen_batches` | `batch_jobs` + `batch_items` |
| Gold | DuckDB table | Standalone worker | `gold_analyses` (AssetSpec, externally materialized) |

## Target architecture — where the detail lives

This document describes the **as-built** system. Three ratified decisions reshape
it — the layered enrichment model
([ADR-0011](adr/0011-enrichment-layered-model.md)), Dagster-native orchestration
([ADR-0012](adr/0012-dagster-native-orchestration.md)), and the inference seam
([ADR-0008](adr/0008-hermetic-with-explicit-api-seam.md) /
[ADR-0009](adr/0009-qwen-batch-service.md)). Their detail is owned by the docs
that will implement them, and is deliberately not restated here:

| Target concern | Owned by |
|---|---|
| The enrichment layer model — bronze landing, the six `silver_*` tables, the four gold marts | [`pipelines/enrichment.md`](pipelines/enrichment.md) |
| The seam, the adapters, and the external service | [`services/inference.md`](services/inference.md) |
| Orchestration state moving off the `ops.sqlite` queue, incl. every table's disposition | [ADR-0012](adr/0012-dagster-native-orchestration.md) + [`pipelines/enrichment.md`](pipelines/enrichment.md) |

**None of the three is built.** Everything below describes what actually runs:
`gold_analyses` and `gold_growth_facets` as DuckDB gold tables, the
`batch_jobs` / `batch_items` / `dead_letter` lifecycle, and two independent
provider lifecycles sharing no code.

## Storage split

Three storage backends, each chosen for its access pattern:

- **Parquet lake** (`data/lake/`) — bulk data, lock-free parallel writes, immutable files. Polars handles all I/O.
- **DuckDB** (`data/state.duckdb`) — authoritative current state, watermarks, SCD2 dims, views. OLAP: scans and aggregations.
- **SQLite** (`data/ops.sqlite`) — operational coordination: batch jobs, batch items, media metadata cache, dead letter. OLTP: point lookups, frequent updates.

## Engine boundary

| Engine | Responsibility |
|---|---|
| Polars | Parquet I/O (read/write NDJSON and Parquet), column mapping, dedup |
| DuckDB | SQL transforms (DISTINCT ON dedup, watermark queries, SCD2, views) |
| Arrow | Zero-copy interchange between Polars and DuckDB (`to_arrow()` / `from_arrow()`) |

## Domain-based structure

```
src/datalake/defs/
├── common/          # PolarsIOManager, ApifyResource, GeminiResource, SQLiteResource, lake.py, schedules.py
├── enrichment/      # batch.py (batch coordination), assets.py (gold_analyses AssetSpec + checks), prompts.py
├── instagram/       # ig_posts_raw, ig_posts_slv, ig_posts_gen_batches, config
└── serving/         # dim_profile, dim_date, analytics views (cross-domain)
```

Domains are independent data sources. `serving` is the only cross-domain module — it joins silver + gold + dims across all sources.

## Batch coordination

**Owned by [`pipelines/enrichment.md`](pipelines/enrichment.md)** — "Current
state — the batch queue". That doc carries the `batch_jobs` / `batch_items`
schemas, the claim/complete/fail lifecycle, the retry policy
(`MAX_ATTEMPTS=5`), and `dead_letter`. Not restated here.

The one system-wide fact worth keeping: the queue lives in `ops.sqlite`
(operational, OLTP) while the analytical state it feeds lives in DuckDB —
the storage split below. ADR-0012 retires the queue tables and keeps the
media/identity/prompt tables; each table's disposition is in
[ADR-0012](adr/0012-dagster-native-orchestration.md).

## REST materialization — SUPERSEDED, described for history only

> **The external enrichment worker no longer exists.** It was removed when
> enrichment became Dagster-native batch ([ADR-0007](adr/0007-batch-native-enrichment-deprecate-worker.md),
> superseding [ADR-0002](adr/0002-external-worker-rest-materialization.md)). The
> worker's claim/consume/harvest roles moved into Dagster; its async role moved
> into the batch path. There is no `enrichment_worker` in `src/` or `scripts/`.

The decision was: run enrichment as a standalone process and report
materializations to Dagster by REST, rather than tying it to a Dagster run.
The reasoning still informs today's submit/harvest jobs, and is recorded in
full in the two ADRs above — in short, Pipes would have tied a rate-limited,
hours-long, crash-prone external workload to a run lifecycle that blocks.

**Do not read this as a description of a running component.**

## Lineage

```
ig_posts_slv ─┬─→ gold_analyses ─┬─→ v_post_detail ─┬─→ v_signal
               │                   │                   ├─→ v_quality_trend
               ├─→ dim_profile ────┤                   ├─→ v_creator_quality
               │                   │                   ├─→ v_rising_creators
               │                   │                   ├─→ v_domain_coverage
               └───────────────────┤                   ├─→ v_engagement_outliers
                                   │                   │     └─→ v_post_metrics ─┬─→ v_creator_metrics
                                   │                   │                          ├─→ v_profile_metrics
                                   │                   │                          └─→ v_standout_calendar
                     dim_date ─────┘                   ├─→ v_outlier_posts
                                                       ├─→ v_creator_outlier_rate
                                                       └─→ v_overview
```

`ig_posts_gen_batches` is a coordination asset — it creates batches in SQLite. There is no formal Dagster data dependency from `gold_analyses` to `ig_posts_gen_batches` because the worker reads from SQLite (not from Dagster IOManager output). Both depend on `ig_posts_slv`.

The five **canonical metric views** (`v_post_metrics`, `v_creator_metrics`,
`v_profile_metrics`, `v_overview`, `v_standout_calendar`) are the single
source for every per-post/per-creator metric. `v_post_metrics` builds on
`v_engagement_outliers` (label pass + point-in-time Tukey baseline); the
others aggregate over it. `dashboard/server.py` is a **thin projector**:
view SELECT + WHERE/ORDER/LIMIT + row→JSON — no `AVG`/`SUM`/`GROUP BY`
aggregation in the server (guard:
`tests/unit/dashboard/test_no_aggregation_in_server.py`).


## Watermarks and `processed_on`

Both are **owned by [`pipelines/core.md`](pipelines/core.md)** — "Incrementality:
watermarks, `processed_on`, and the drain". That doc carries what this one would
only summarize: the `watermarks (name, timestamp)` table's actual definition,
which pipelines have a row (`silver_ig`, `profiles_ig`; `gold_ig` retired), how
silver gates file discovery on bronze mtime, and the structural
carry-forward (`fill_null(processed_on.max().over(post_id))`) that makes
`processed_on` a true first-seen timestamp.

## Dead letter

**Owned by [`pipelines/enrichment.md`](pipelines/enrichment.md)** — "Current
state — the batch queue" carries the `dead_letter` schema and its semantics
(manual triage only, no automatic retry worker, keeping `gold_analyses` free of
partial failures). Retired by [ADR-0012](adr/0012-dagster-native-orchestration.md),
where failures surface instead via the anti-join
`landed(bronze) ∖ conformed(silver)` plus a blocking asset check.
