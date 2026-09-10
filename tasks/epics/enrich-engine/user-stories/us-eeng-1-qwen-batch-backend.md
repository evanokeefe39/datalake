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
- **Settled contract (2026-09-10):** visual submits land their verbatim
  response in `bronze_enrichment_raw` and fan out at silver conform to
  `silver_visual_annotations` + `silver_visual_summaries`; text submits fan
  out to `silver_text_annotations` + `silver_text_summaries`;
  classification writes `silver_content_classification`. Provider is metadata,
  never a table name. Key is `(post_id, platform)`; `domain` means the
  CONTENT niche.

## Story

**As a** pipeline operator, **I want** the async enrichment backend to be the
standalone qwen-batch service (media = client-side frame-sampling; images sent
in-request), **so that** I keep the submit → poll → harvest decoupling and
resume-safety without the Gemini File-API storage cap or a pipeline-owned queue.

## Acceptance criteria (binary)

- AC1: The facets SUBMIT PATH (scripts/enrich_facets_batch.py →
      facets_batch.submit_facets_batch) files candidates as a
      `qwen-batch-service` job (POST /jobs); orchestration is the one-shot CLI
      driver, no Dagster op. ONE visual job — at harvest it lands verbatim in
      `bronze_enrichment_raw` and fans out at conform to
      `silver_visual_annotations` AND `silver_visual_summaries` (never one
      submit per table).
- AC2: facets_batch.harvest_facets_batches polls GET /jobs/{id}; on
      `completed` it reads /results and writes the verbatim response to
      `bronze_enrichment_raw` (immutable, idempotent on
      `(post_id, platform, workload, prompt_hash, run_id)`), then
      deterministically conforms it to the fan-out silver tables
      (`silver_visual_annotations`, `silver_visual_summaries`) keyed
      `(post_id, platform)` — ZERO API calls in the transform; validation
      (parse, required fields, enum conformance, carousel length) lives in
      silver, with unparseable output quarantined LOUDLY, never dropped.
- AC3: Media = cached local file paths; the SERVICE frame-samples reels with
      ffmpeg on its own host + native images (the client never runs ffmpeg).
- AC4: Per-item results survive a service restart (its own durable store);
      failed items are service-side dead-lettered, never silently dropped.
- AC5: Incremental runs re-discover only posts lacking a current-schema
      silver row.
- AC6 (service durability): A mid-run SERVICE restart, or an OpenRouter outage /
      credits exhaustion, does not lose completed work or wedge a job. On
      restart the service finishes only unfinished items — completed items are
      never reprocessed; transient outages retry with backoff and recover;
      a persistent outage terminal-fails only the affected items (never a stuck
      `processing` job). Proven by service-level tests.
- AC7 (client resume-without-reprocessing): A datalake re-run after a mid-run
      failure (service down, outage, credits) resubmits ONLY posts lacking a
      silver row. Posts already conformed to silver are neither resubmitted
      nor double-written (UPSERT idempotent on the natural key). Proven by a
      client-level test
      that runs the discovery → submit cycle twice and asserts zero re-submits
      of done posts.
- AC8 (workload pluggability): The service hosts the ≥3 enrichment workloads
      (qwen-vision, text-LLM, Whisper STT) as pluggable job types — a
      `workload` (executor) field on the job with an executor registry in the
      worker (`openrouter_vision`, `openrouter_text`, `whisper_local`); the
      store, lease/resume, dead-letter, and `/jobs` REST contract are
      unchanged. A transcript job submits an audio path and harvests
      transcript text via the same submit → poll → results flow. No second
      service is created.
- AC9 (one ingest seam): All workloads ingest via ONE shared async pattern —
      local ledger (`workload`-keyed) → submit → poll-to-terminal → retrieve →
      verbatim landing in `bronze_enrichment_raw` → deterministic silver
      conform + validate → idempotent silver upsert with model/prompt_hash
      provenance. Gemini-batch
      (now writing `silver_content_classification`, the retired
      `gold_analyses` replaced) converges on the same lifecycle seam
      opportunistically;

## Definition of done

- [ ] Submit → poll → harvest end-to-end against the service on a sample
      writes verbatim rows to `bronze_enrichment_raw` and validated silver
      rows to a temp DB first (smoke), then production wiring is
      reviewed before enablement.
- [ ] Schema catalog + readiness green for the bronze + silver rows.
- [ ] Scoped tests pass; no regression to existing silver paths.

## Tests

- Service down during submit → loud failure (see US-EENG-2), not a silent no-op.
- A transient qwen failure on one item retries and does not fail the job.
- A full corpus is resume-safe: re-running after a mid-job crash enriches only
  the unfinished posts (no duplicate silver writes).
- Service restart mid-job (AC6): the completed item is not reprocessed; the
  unfinished one resumes and the job completes.
- OpenRouter outage / credits exhaustion persistent (AC6): the affected item is
  terminal-failed (bounded retries), never a stuck `processing` job.
- Client resume-without-reprocessing (AC7): discovery → submit run twice over the
  same DB submits zero items for already-enriched posts and never double-writes.
