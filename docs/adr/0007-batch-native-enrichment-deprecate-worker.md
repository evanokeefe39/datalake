# ADR-0007: Enrichment is Dagster-native async batch (submit + harvest sensor); deprecate the external worker

- Status: Accepted
- Decided: 2026-09-06

## Context

Enrichment historically ran in a standalone external process (`enrichment_worker.py`),
the decision recorded in ADR-0002. That decision conflated two workloads and, as
the #24 incident showed, pushed coordination that Dagster already owns out of
Dagster:

- **Async batch** (`gemini-batch`) is an *async external job*: the hours-long work
  happens on Google's side, and a Dagster run only *submits* (fast) and later
  *harvests* (fast). A run never blocks for hours. The core reason ADR-0002
  rejected in-Dagster enrichment ("a run blocks for hours on rate-limited work")
  therefore does **not** apply to the batch path.
- **Interactive synchronous** enrichment (`generate_content`, File-API upload) is
  the genuinely blocking workload — but it is the *ad-hoc* path, not the
  primary one, and need not be orchestrated at all.

The claim/consume/harvest/schedule/keep-alive machinery the worker provides
(ops.sqlite `batch_items`, claim + re-claim, "who polls for a finished batch")
is Dagster's native domain: sensors, the run queue, per-run retries, the daemon.
Keeping it in a separate one-shot process produced exactly the #24 bug — a
one-shot CLI as the *only* poller, so a finished batch sat unharvested overnight.

## Decision

`gold_analyses` has **one orchestrated writer**: an async `gemini-batch` path
that lives inside Dagster.

1. **Pure assets produce candidates** (labels &rarr; a bounded, current batch), as today.
2. A **submit run** (short) files those candidates as a `gemini-batch` job and
   persists the `job_id` (ops.sqlite, status `pending`), then ends immediately.
   Media are pre-uploaded to the Gemini File API by a bounded upload step first.
3. A **harvest sensor** (cursor; reads pending job ids) polls Gemini; on a
   terminal batch it triggers a short **harvest run** that fetches and applies
   results to `gold_analyses`.

Interactive synchronous enrichment is **not orchestrated**: it becomes a manual,
out-of-band script (a developer tool) that a human runs deliberately to enrich a
small subset (a new creator, a spot-check), upserting `gold_analyses`
idempotently keyed on `(post_id, domain, prompt_hash)`. It is secondary and
low-priority by design. Because DuckDB is single-writer, the ad-hoc script must
not run concurrently with a harvest run.

The custom `enrichment_worker` process and its orchestration roles are
**deprecated and removed** (its claim/consume/harvest roles move to Dagster; its
async role moves to batch; its synchronous role moves to the ad-hoc script).

## Alternatives considered

- **Option A — keep the external worker, add sensors** (ADR-0002 preserved):
  still operates a separate process and keeps claim/queue coordination partly
  outside Dagster; only clearly justified if enrichment must progress while the
  Dagster daemon is down, or for interactive-at-scale. Rejected as the default —
  a bounded-run + batch architecture covers both without a persistent process.
- **Interactive enrichment as one long in-graph run**: rejected — holds a run
  slot for hours (the original ADR-0002 concern). Interactive work is either
  bounded per-run (interim) or the out-of-band script (steady state).

## Consequences

Positive: one orchestration model for the primary path; #24 is impossible by
construction (the harvest sensor is the poller); lineage, backfill, freshness
and retries are Dagster-native; DuckDB single-writer is respected (sole
orchestrated writer is the harvest run); one less process to operate.

Negative: `gemini-batch` must become the real default path — today it is deferred
and text-only, so this requires the migration in the plan; media must be
uploaded to the File API before batch submit (a bounded Dagster op); supersedes
ADR-0002; ad-hoc gold freshness depends on the manual script writing
deliberately.

## Supersedes / Superseded by

Supersedes: ADR-0002. Related: ADR-0001 (this resolves the "ingested source vs
graph-produced" framing toward a graph-native enrichment seam), ADR-0008
(reinterprets the API boundary), ADR-0004 (coordination store).
