# ADR-0008: Hermetic transforms with one explicit API seam (batch submit + harvest)

- Status: Accepted
- Decided: 2026-09-06

## Context

ADR-0003 confined network I/O to two boundaries: ingestion (bronze producers)
and **the external enrichment worker**. ADR-0007 moves enrichment into Dagster as
an async `gemini-batch` submit + harvest path, so the "external worker" boundary
no longer exists as a process. The principle ADR-0003 protected must survive the
change: pure, deterministic transforms stay hermetic and cheaply replayable;
stochastic, rate-limited, paid API calls happen only at a named seam.

## Decision

ADR-0003's boundary is reinterpreted from "ingestion + an external worker" to
"ingestion + **one explicit, tagged enrichment seam inside the graph**". Silver
onward remains hermetic. The only places the graph may call Gemini are:

1. The **File-API media upload** step (bounded, ahead of batch submit).
2. The **batch submit** step.
3. The **batch harvest** step (fetch + apply results to `gold_analyses`).

These are a single coherent seam: an enrichment boundary named and tagged as
such, never `generate_content`/upload buried inside a pure transform. The
hermetic invariant of everything above/below the seam is unchanged and guarded.

## Alternatives considered

- **Keep ADR-0003 strict ("no API in any asset")**: forces the enrichment call
  out of the graph and back into a worker, contradicting ADR-0007.
- **Allow API calls anywhere in transforms**: loses deterministic replayability
  and concentrates cost unpredictably — the original reason for the rule.

## Consequences

Positive: retains ADR-0003's actual value (replayable hermetic graph) while
letting enrichment be a Dagster citizen; one seam to reason about and tag; the
media-caching precedent (bytes cached at ingestion) still holds.

Negative: the seam must be guarded so API calls don't leak into pure assets —
via naming/tags and tests/review, and an asset check that the seam stays the
only API-touching boundary.

## Supersedes / Superseded by

Supersedes: ADR-0003. Related: ADR-0007 (the seam's home), ADR-0005.
