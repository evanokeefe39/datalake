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

- AC1: The facets SUBMIT PATH (scripts/enrich_facets_batch.py →
      facets_batch.submit_facets_batch) files candidates as a
      `qwen-batch-service` job (POST /jobs); orchestration is the one-shot CLI
      driver, no Dagster op.
- AC2: facets_batch.harvest_facets_batches polls GET /jobs/{id}; on
      `completed` it reads /results and writes `gold_growth_facets` keyed
      `(post_id, domain)`.
- AC3: Media = cached local file paths; the SERVICE frame-samples reels with
      ffmpeg on its own host + native images (the client never runs ffmpeg).
- AC4: Per-item results survive a service restart (its own durable store);
      failed items are service-side dead-lettered, never silently dropped.
- AC5: Incremental runs re-discover only posts lacking a current-schema gold row.
- AC6 (service durability): A mid-run SERVICE restart, or an OpenRouter outage /
      credits exhaustion, does not lose completed work or wedge a job. On
      restart the service finishes only unfinished items — completed items are
      never reprocessed; transient outages retry with backoff and recover;
      a persistent outage terminal-fails only the affected items (never a stuck
      `processing` job). Proven by service-level tests.
- AC7 (client resume-without-reprocessing): A datalake re-run after a mid-run
      failure (service down, outage, credits) resubmits ONLY posts lacking a
      current gold row. Posts already written to gold are neither resubmitted
      nor double-written (UPSERT idempotent). Proven by a client-level test
      that runs the discovery → submit cycle twice and asserts zero re-submits
      of done posts.

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
- Service restart mid-job (AC6): the completed item is not reprocessed; the
  unfinished one resumes and the job completes.
- OpenRouter outage / credits exhaustion persistent (AC6): the affected item is
  terminal-failed (bounded retries), never a stuck `processing` job.
- Client resume-without-reprocessing (AC7): discovery → submit run twice over the
  same DB submits zero items for already-gold posts and never double-writes.
