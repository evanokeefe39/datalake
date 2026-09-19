# Architecture Decision Records

Canonical decision history for the datalake. Each ADR records a decision that
is load-bearing enough that re-treading it later would be a mistake — with its
context, the alternatives considered, and its consequences. ADRs are the
**single source of truth for *why* the design is the way it is**; they are
deliberately separate from [`design.md`](../design.md), which describes the
*as-built* design and links here for rationale.

Part of the [architecture documentation](../README.md).

## How to use this

- **Read** `docs/architecture/design.md` for how the system is shaped today. When it
  says "see ADR-xxxx," read that ADR for the reasoning and alternatives.
- **Add a new ADR** whenever a design decision is made that future work could
  otherwise second-guess or accidentally reverse. One ADR per decision.
- **Never edit an Accepted ADR to flip its decision.** Supersede it: write a
  new ADR with `Status: Superseded by ADR-xxxx` on the old one and a new ADR
  describing the change and why. This keeps the history linear and auditable.
- ADRs are numbered in order of adoption (`NNNN`), not by when the decision was
  first made. Backfilled records note the original decision date.

## Index

| ADR | Decision in one line | Status | Decided |
|-----|-------|--------|---------|
| [0001](0001-enrichment-as-ingested-source.md) | Treat enrichment output as an **ingested source**, not a transform — the basis for landing raw responses in bronze | Accepted | 2026-09-03 (ratified) |
| [0002](0002-external-worker-rest-materialization.md) | Run enrichment in an external worker reporting materializations by REST, not Pipes | Superseded by [0007](0007-batch-native-enrichment-deprecate-worker.md) | 2026-07 (backfilled 2026-09-03) |
| [0003](0003-no-api-in-transform-layer.md) | No LLM/API calls in the transform layer | Superseded by [0008](0008-hermetic-with-explicit-api-seam.md) | 2026-08 (backfilled 2026-09-03) |
| [0004](0004-ops-sqlite-state-duckdb-deadletter.md) | SQLite for ops/coordination, DuckDB for analytical state | Accepted — queue + `dead_letter` scope superseded by [0012](0012-dagster-native-orchestration.md) | 2026-07 (backfilled 2026-09-03) |
| [0005](0005-thin-projector-serving.md) | Metrics live in warehouse views; the dashboard is a thin projector | Accepted | 2026-08 (backfilled 2026-09-03) |
| [0006](0006-point-in-time-metric-semantics.md) | Metrics are point-in-time (at-post-time baselines), never all-time averages | Accepted | 2026-08 (backfilled 2026-09-03) |
| [0007](0007-batch-native-enrichment-deprecate-worker.md) | Enrichment is Dagster-native async batch (submit + harvest); the external worker is removed | Accepted — **ledger clause of Amendment 1 superseded by [0013](0013-seam-keeps-no-ledger.md)** | 2026-09-06 |
| [0008](0008-hermetic-with-explicit-api-seam.md) | Transforms stay hermetic behind exactly one explicit API seam | Accepted | 2026-09-06 |
| [0009](0009-qwen-batch-service.md) | The enrichment backend is a standalone, domain-agnostic qwen batch service (replaces gemini-batch) | Accepted | 2026-09-09 |
| [0010](0010-enrichment-naming-and-provenance.md) | Name enrichment tables `gold_<channel>_<artifact>` with per-pass provenance (structural split) | Superseded by [0011](0011-enrichment-layered-model.md) (naming + layer); **ledger clause superseded by [0013](0013-seam-keeps-no-ledger.md)** | 2026-09-09 |
| [0011](0011-enrichment-layered-model.md) | **The v3 layer model**: bronze verbatim → six `silver_*` conform tables → four gold marts; key `platform` not `domain` | Accepted (**not yet implemented**) | 2026-09-10 |
| [0012](0012-dagster-native-orchestration.md) | Orchestration state is Dagster-native; retire the `ops.sqlite` queue | Accepted (**not yet implemented**) | 2026-09-10 |
| [0013](0013-seam-keeps-no-ledger.md) | The seam keeps **no ledger** — the service owns its job store (Dagster polls it), Dagster owns orchestration state | Accepted (**not yet implemented**) | 2026-09-10 |
| [0014](0014-orchestration-dynamics.md) | The orchestration **dynamics**: the harvested driver, the retry driver, the quarantine disposition, and a partition key whose round survives derivation | Accepted — D1's writer amended by [0016](0016-discovery-and-guard-share-one-derivation.md) | 2026-09-14 |
| [0015](0015-repository-layout.md) | **The repository layout**: a uv workspace (`services/`, `packages/`), module names that describe a role and never a provider, and the eight conventions the tree encodes | Accepted | 2026-09-15 |
| [0016](0016-discovery-and-guard-share-one-derivation.md) | Discovery and the double-submit guard read **one** in-flight derivation; the submit stage materializes its own placeholder | Accepted | 2026-09-15 |
| [0017](0017-compose-and-roster-boundary.md) | **One Compose brings up orchestration + jobs + dashboard**; the creator roster crosses the service boundary over HTTP, and persisted media paths are translated by a host↔container prefix map | Accepted | 2026-09-16 |
| [0018](0018-pipeline-automation-and-the-cost-boundary.md) | **Auto-materialize the replay path; keep the submit edge a DETERMINISM boundary** (silver stays a pure replay of bronze). Cost lives on the API credential as defense-in-depth, not in the graph | Accepted | 2026-09-16 |
| [0019](0019-app-state-and-analytical-media.md) | **Split application state from analytical media**: app state → Postgres owned solely by the dashboard, reached only over its API; post media → object storage with a database holding keys, never 55 GB of bytes | Proposed | 2026-09-18 |

ADR-0011, 0012 and 0013 **landed 2026-09-15** (W0–W9): the layered model is live,
the `ops.sqlite` queue is retired, and no ledger exists. ADR-0013 reconciled
ADR-0012 with the seam by removing the one orchestration-shaped table it had left
unspecified. ADR-0014's dynamics are live except D1's writer, which ADR-0016
amends. Read [`../README.md`](../README.md) for the as-built shape.

## Template

```md
# ADR-NNNN: <short decision title>

- Status: Proposed | Accepted | Superseded by ADR-xxxx | Deprecated
- Decided: <date>

## Context
The problem and the forces at play. Why this decision needed to be made.

## Decision
The decision itself, stated crisply.

## Alternatives considered
What else was weighed and why it lost.

## Consequences
Positive, negative, and neutral effects. What it commits future work to.

## Supersedes / Superseded by
Links to the ADRs this replaces or that replace this one.
```
