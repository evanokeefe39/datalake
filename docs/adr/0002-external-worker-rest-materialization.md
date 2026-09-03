# ADR-0002: External enrichment worker + gold as AssetSpec (REST materialization, not Pipes)

- Status: Accepted
- Decided: 2026-07 (backfilled 2026-09-03)

## Context

Enrichment calls an external, rate-limited LLM for hours at a time (429s, RPD
reset at 08:00 UTC) and must survive crashes and Dagster restarts. If it ran as
a Dagster Pipes subprocess, the run would block until the subprocess exits —
correct for short-lived jobs (dbt, Spark, model training) but wrong for
long-running, rate-limited external API work.

## Decision

Enrichment runs in a **standalone worker process** (not inside a Dagster run).
`gold_analyses` is an **AssetSpec, externally materialized**: the worker is its
sole writer, and it reports materialization to Dagster via the canonical REST
endpoint (`POST /report_asset_materialization/`). This decouples the worker
lifecycle from Dagster runs.

## Alternatives considered

- **Dagster PipesSubprocessClient**: rejected — ties enrichment to a run
  lifecycle that blocks until exit; wrong for hours-long, rate-limited work.

## Consequences

Positive: worker survives Dagster restarts; no run slot held open for hours;
automatic retry via re-claiming (stale batches re-claimed on next invocation);
crash recovery via the stale reaper (items in `processing` are re-claimed).
Negative: materialization events are reported out-of-band (lineage is eventual,
not run-derived); the worker is a non-hermetic producer that must be operated
separately.

## Supersedes / Superseded by

Supersedes: the old synchronous/graph-coupled enrichment. Superseded by: none.
