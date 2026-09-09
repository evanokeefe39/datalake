# Epic E-ENRICH-ENGINE — Async enrichment engine (submit → poll → harvest)

- **Theme:** Enrichment engine (core)
- **Owner:** dlc-worker
- **Status:** Active (backend pivoting: Gemini batch → standalone qwen-batch service)
- **Depends on:** E-MEDIA (byte cache as media source)
- **Feeds:** E-ENRICH-LABELS, E-ENRICH-FACETS, E-ENRICH-SUMMARIES,
  E-SERVING-ANALYTICS
- **Relates to:** `qwen-batch-service` (standalone dependent service, ADR-0009)

## Outcome

A durable, media-aware async enrichment engine that posts **no API calls in
transform layers** and never blocks a Dagster run. Dagster owns orchestration
(a sub-second submit + a harvest sensor); the hours-long work runs in an async
backend whose job state Dagster polls and harvests. Media is resolved client-side
to frames (images) and sent in-request, so no upload/storage cap binds the corpus.

## Backend pivot (ADR-0009)

The enrichment backend moves from Google's `gemini-batch` (Developer API, File-API
media) to a **standalone, domain-agnostic qwen batch service**
(`~/repos/qwen-batch-service`, dockerized) that mimics the batch contract:
`POST /jobs` → poll `GET /jobs/{id}` → `GET /jobs/{id}/results`. qwen
(`qwen/qwen3.7-flash`) is synchronous, so the service's own worker drains jobs and
holds durable per-item state (resume-safe). The engine's submit/poll/harvest
shape is unchanged — only the backend and the media transport change.

Why (recorded in ADR-0009):
- Gemini Developer-API batch discount is real but its File-API media path caps at
  20 GiB/project — a full-corpus video run hit that wall.
- Vertex has no Gemini vision batch discount; on-demand ~3–6× the Developer batch.
- qwen via OpenRouter (~$0.03/$0.13 per 1M) costs ~$3 for the corpus; client-side
  frame-sampling needs no GCS mirror and no File-API upload.
- The service is generic (owns no domain schema), so it is a dependent service,
  not a domain enrichment worker — Dagster stays the orchestrator (ADR-0007 shape).

## Scope highlights

- Submit/poll/harvest orchestration in Dagster; `qwen-batch-service` is the async
  backend (dependent service). Loud failure (health check) if it is down.
- Multimodal media: client-side ffmpeg frame-sampling for reels + native images →
  image parts; `max_tokens` ≥ 2500; retry-on-empty for reasoning-model flakes.
- Per-item retry/backoff + terminal dead-letter live inside the service (its own
  store), so the corpus run is resume-safe without an `ops.sqlite` queue.
- Incremental follow-on runs via stateless discovery (posts lacking a current
  gold row resubmit).

## Cross-cutting

- The service is reachable and healthy before any submit (loud failure).
- The gold table / prompt hashing / schema versioning are unchanged by the pivot.

## Source of truth

`tasks/plans/qwen-batch-enrichment.md`, `docs/adr/0009-qwen-batch-service.md`
(and ADR-0007 for the preserved orchestration shape),
`tasks/epics/enrich-engine/user-stories/us-eeng-*.md`.

## Epic DoD

- [x] Batch submit/poll/harvest orchestration shape live (Gemini era).
- [ ] Enrichment backend runs on the standalone qwen-batch service (US-EENG-1);
      media = client-side frame-sampling; gold rows written per post.
- [ ] Service is a declared dependent service; enrichment fails loudly if it is
      down (US-EENG-2).
- [ ] Full-corpus run resume-safe on the service; dead-letter is service-side.
- [ ] Durability verified by tests (US-EENG-1 AC6/AC7): a service restart and an
      OpenRouter/credits outage resume without reprocessing completed items or
      duplicating gold; transient outages recover; no job wedges in `processing`.
