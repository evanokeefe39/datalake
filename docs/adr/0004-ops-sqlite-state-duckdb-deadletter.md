# ADR-0004: SQLite (ops/coordination) vs DuckDB (analytical state) split; dead-letter + queue decoupling

- Status: Accepted
- Decided: 2026-07 (backfilled 2026-09-03)

## Context

The system needs two very different kinds of state that were initially conflated.
Coordination state is OLTP: point lookups, frequent small updates (claim an item,
increment attempts, route a failure). Analytical state is OLAP: scans and
aggregations over materialized facts (posts, enrichments, serving views).
Mixing error/queue tracking into the analytical tables also polluted query paths
(every read needed a `WHERE status = 'completed'`).

## Decision

Two stores with a clean split:
- **`ops.sqlite`** (operational/coordination, OLTP): the batch queue
  (`batch_jobs`/`batch_items` with `attempts`, `scheduled_for` backoff), media
  metadata + byte-cache (`media_metadata`, `media_cache`), **`dead_letter`**
  (terminal failures after `MAX_ATTEMPTS=5`), creators/profiles.
- **`state.duckdb`** (analytical, OLAP): silver, `gold_analyses`, watermarks,
  SCD2 dims, serving views.

A generic **`watermarks`** table (`name`, `timestamp`) replaces per-pipeline
progress tables; reset = DELETE row. `dead_letter` keeps `gold_analyses` pure
(only completed enrichments), with results to gold and failures to dead-letter.

## Alternatives considered

- Status/error columns on analytical tables: rejected — mixed concerns, required
  filtering on every query.
- Per-pipeline watermark tables: rejected — didn't scale to N pipelines.
- LEFT JOIN gap detection for pending posts: rejected — no clean reset; mass
  UPDATE needed. Replaced by watermark/stateless-drain approaches.

## Consequences

Positive: results and error tracking cleanly separated; queue decouples the DAG
from the worker; terminal failures are triageable without polluting gold. Neutral:
two stores to manage. Negative: coordination state is not analytically queryable
in DuckDB (acceptable — it's operational).

## Supersedes / Superseded by

Superseded by: none. Related: ADR-0002, ADR-0001 (feature-store mapping of these
roles).
