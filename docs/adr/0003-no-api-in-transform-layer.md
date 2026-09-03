# ADR-0003: No LLM/API calls in the transform layer; network I/O confined to ingestion + the external worker

- Status: Accepted
- Decided: 2026-08 (backfilled 2026-09-03)

## Context

The team's rule (established during the media-caching work) is that the
transformation layer must not make external API calls. A transform is
deterministic, idempotent, cheap to replay; an external call is stochastic,
rate-limited, and paid. Putting API calls inside Dagster transform assets makes
the graph non-hermetic, hard to replay deterministically, and able to block or
fail on external conditions. The media-cache precedent: media bytes are
downloaded **at ingestion** (bronze producers) into `media_cache`, never fetched
inside a silver/serving transform — `silver_ig_posts` is a **pure transform** with
"no network I/O."

## Decision

No LLM/API/Gemini calls anywhere in the Dagster transform graph. Network I/O is
confined to two boundaries:
1. **Ingestion** (bronze producers) — fetching the raw external source (scrape)
   and downloading media bytes into `media_cache`.
2. **The external enrichment worker** (see ADR-0002) — the sole caller of Gemini
   (`generate_content`, File-API upload); it writes `gold_analyses` and POSTs a
   materialization event.

Silver onward is pure transform. The DAG only enqueues candidates and consumes
gold; it never blocks on Gemini.

## Alternatives considered

- Fetching media / calling Gemini inside a silver or gold transform: rejected —
  violates the rule, makes transforms non-deterministic and non-replayable.

## Consequences

Positive: the graph stays hermetic/replayable; cost + nondeterminism concentrate
in one isolated seam; the worker owns retry/rate-limit/batch-size. Neutral: media
availability is an ingestion-time concern (CDN URLs expire ~4-5 days, mitigated
by the scrape-time byte cache). Negative: the "no external call" invariant must be
guarded (tests + review) so it isn't accidentally reintroduced.

## Supersedes / Superseded by

Superseded by: none. Related: ADR-0002, ADR-0005.
