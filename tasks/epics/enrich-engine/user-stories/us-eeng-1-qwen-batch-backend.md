---
id: US-EENG-1
epic: E-ENRICH-ENGINE
persona: P1
status: Open
---
# US-EENG-1 — Enrichment backend runs on the standalone qwen-batch service

- **Epic:** E-ENRICH-ENGINE
- **Status:** Open
- **Relates to:** qwen-batch-service (dependent), US-EFAC-3 (visual pass rides it)
- **Source:** `tasks/plans/qwen-batch-enrichment.md`, ADR-0009

## Story

**As a** pipeline operator, **I want** the async enrichment backend to be the
standalone qwen-batch service (media = client-side frame-sampling; images sent
in-request), **so that** I keep the submit → poll → harvest decoupling and
resume-safety without the Gemini File-API storage cap or a domain-owned queue.

## Acceptance criteria (binary)

- AC1: The Dagster submit path files candidates as a `qwen-batch-service` job
      (POST /jobs) and returns sub-second — no blocking, no Gemini client.
- AC2: A harvest sensor polls GET /jobs/{id}; on terminal state it reads
      /results and writes `gold_growth_facets` keyed `(post_id, domain)`.
- AC3: Media = client-side ffmpeg frame-sampling of reels + native images →
      image file paths; the service treats media as opaque images (no IG/GCS
      coupling in the service).
- AC4: Per-item results survive a service restart (its own durable store);
      failed items are service-side dead-lettered, never silently dropped.
- AC5: Incremental runs re-discover only posts lacking a current-schema gold row.

## Definition of done

- [ ] Submit → poll → harvest end-to-end against the service on a sample writes
      validated gold rows to a temp DB first (smoke), then production wiring is
      reviewed before enablement.
- [ ] Schema catalog + readiness green for the gold rows.
- [ ] Scoped tests pass; no regression to existing gold paths.

## Tests

- Service down during submit → loud failure (see US-EENG-2), not a silent no-op.
- A transient qwen failure on one item retries and does not fail the job.
- A full corpus is resume-safe: re-running after a mid-job crash enriches only
  the unfinished posts (no duplicate gold writes).
