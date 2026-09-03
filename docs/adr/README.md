# Architecture Decision Records

Canonical decision history for the datalake. Each ADR records a decision that
is load-bearing enough that re-treading it later would be a mistake — with its
context, the alternatives considered, and its consequences. ADRs are the
**single source of truth for *why* the design is the way it is**; they are
deliberately separate from `docs/ARCHITECTURE.md`, which describes the
*current* design (the as-built view) and links here for rationale.

## How to use this

- **Read** `docs/ARCHITECTURE.md` for how the system is shaped today. When it
  says "see ADR-xxxx," read that ADR for the reasoning and alternatives.
- **Add a new ADR** whenever a design decision is made that future work could
  otherwise second-guess or accidentally reverse. One ADR per decision.
- **Never edit an Accepted ADR to flip its decision.** Supersede it: write a
  new ADR with `Status: Superseded by ADR-xxxx` on the old one and a new ADR
  describing the change and why. This keeps the history linear and auditable.
- ADRs are numbered in order of adoption (`NNNN`), not by when the decision was
  first made. Backfilled records note the original decision date.

## Index

| ADR | Title | Status | Decided |
|-----|-------|--------|---------|
| [0001](0001-enrichment-as-ingested-source.md) | Enrichment output is an ingested source, not a transform (LLM/API boundary) | **Proposed** | 2026-09-03 (pending build-vs-buy + ratification) |
| [0002](0002-external-worker-rest-materialization.md) | External enrichment worker + gold as AssetSpec (REST materialization, not Pipes) | Accepted | 2026-07 (backfilled 2026-09-03) |
| [0003](0003-no-api-in-transform-layer.md) | No LLM/API calls in the transform layer; network I/O confined to ingestion + external worker | Accepted | 2026-08 (backfilled 2026-09-03) |
| [0004](0004-ops-sqlite-state-duckdb-deadletter.md) | SQLite (ops/coordination) vs DuckDB (analytical state) split; dead-letter + queue decoupling | Accepted | 2026-07 (backfilled 2026-09-03) |
| [0005](0005-thin-projector-serving.md) | Metrics computed in warehouse views; dashboard/API is a thin projector | Accepted | 2026-08 (backfilled 2026-09-03) |
| [0006](0006-point-in-time-metric-semantics.md) | Point-in-time-only metric semantics (per-post baselines, not all-time averages) | Accepted | 2026-08 (backfilled 2026-09-03) |

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
