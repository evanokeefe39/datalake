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

The qwen-batch service is the **general async external-workload runner** for
the ≥3 enrichment workloads (Whisper STT transcription →
`silver_audio_transcripts`, qwen-vision → `silver_visual_annotations` +
`silver_visual_summaries`, text-LLM → `silver_text_annotations` +
`silver_text_summaries` + `silver_content_classification`) as pluggable job
types, and all results ingest back into features via **ONE shared async
pattern**: local ledger → submit → poll-to-terminal → retrieve → verbatim
landing in `bronze_enrichment_raw` → deterministic conform + validation into
idempotent silver upsert with ordering guard (ZERO API calls in transform
layers — ADR-0003). Gemini-batch (now `silver_content_classification`,
replacing the retired `gold_analyses`) converges on the same lifecycle seam
opportunistically (its own executor + Dagster trigger may stay).

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
- Incremental follow-on runs via stateless discovery (posts lacking a current
  silver row resubmit).
- Multi-workload seam: the service gains a `workload` (executor) field —
  `openrouter_vision`, `openrouter_text`, `whisper_local` (faster-whisper
  subprocess over audio paths) — plus a generalized media list; the store,
  lease/resume, and REST contract are unchanged. Datalake side: one shared
  external-jobs ledger + submit/poll/retrieve seam across workloads
- **One-submit-fans-out (settled contract):** ONE visual submit lands its
  verbatim response in `bronze_enrichment_raw` and fans out at conform to
  `silver_visual_annotations` AND `silver_visual_summaries`; ONE text submit
  lands verbatim and fans out to `silver_text_annotations` AND
  `silver_text_summaries`; `silver_content_classification` is its own submit
  (it additionally consumes visual summaries + transcripts). Never one submit
  per table — that would double the bill.
- **Provider mapping (recorded, not in table names):** vision = qwen,
  audio = whisper, text = swappable (qwen default; Gemini batch a pluggable
  alternative). Provider/model live in per-row metadata
  (`provider`, `model`, `prompt_hash`, `schema_version`, …); there are NO
  `_bound` columns — enum definitions live in the versioned schema registry.
  DAG: `silver_text_annotations`/`silver_text_summaries` depend on
  `silver_audio_transcripts`; `silver_content_classification` depends on
  `silver_audio_transcripts` + `silver_visual_summaries`.
- **Layering (strict medallion):** the harvest writes the verbatim response
  body to `bronze_enrichment_raw` (immutable, append-only, idempotent on
  `(post_id, platform, workload, prompt_hash, run_id)`, Parquet); silver is a
  deterministic pure function of bronze — conform + validate (parse,
  required fields, enum conformance via `schema_version`, length bounds,
  cross-field checks like carousel `n == len(image_summaries)`); unparseable
  or terminal-invalid output is quarantined/dead-lettered LOUDLY, never a
  silent NULL, never a dropped row. Remapping bronze → silver is FREE (no
  re-calling the paid model). Gold is analytic marts only (E-SERVING-ANALYTICS).

## Cross-cutting
- The silver tables / prompt hashing / schema versioning are unchanged by the pivot.
  Key is `(post_id, platform)` — `platform` matches `profiles.platform`;
  `domain` means the CONTENT niche (dev/AI/…), never the platform.
- The service is reachable and healthy before any submit (loud failure).

## Source of truth

`tasks/plans/qwen-batch-enrichment.md`, `docs/adr/0009-qwen-batch-service.md`
(and ADR-0007 for the preserved orchestration shape),
`tasks/epics/enrich-engine/user-stories/us-eeng-*.md`.

## Epic DoD

- [x] Batch submit/poll/harvest orchestration shape live (Gemini era).
- [ ] Enrichment backend runs on the standalone qwen-batch service (US-EENG-1);
      media = client-side frame-sampling; bronze + silver rows written per post.
- [ ] Service is a declared dependent service; enrichment fails loudly if it is
      down (US-EENG-2).
- [ ] Full-corpus run resume-safe on the service; dead-letter is service-side.
- [ ] Durability verified by tests (US-EENG-1 AC6/AC7): a service restart and an
      OpenRouter/credits outage resume without reprocessing completed items or
      duplicating silver; transient outages recover; no job wedges in `processing`.
