# ADR-0009: Enrichment backend is a standalone qwen batch service (supersedes the Gemini File-API/Vertex calculus)

- Status: Accepted
- Decided: 2026-09-09
- Branch: `feat/qwen-enrichment`
- Supersedes the backend of: ADR-0007 (its orchestration shape is preserved; only
  the async backend + media transport change)

## Context

The enrichment engine (E-ENRICH-ENGINE) submitted media-bearing posts to
Google's `gemini-batch` (Developer API) for the ~50% batch discount, holding
results server-side until a harvest sensor collected them (ADR-0007 shape). Two
problems forced a rethink for video-at-scale:

1. **A 20 GiB File-API storage cap.** Media for a full-corpus video run is staged
   through the Gemini File API *upload* method, which caps at `file_storage_bytes`
   20 GiB per project. A full-corpus run hit that wall mid-upload (429
   `RESOURCE_EXHAUSTED`, quota `FileStorageBytesPerProject`); the quota does not
   free promptly even after deletion. The GCS-registration method (`files:register`)
   has no total cap but requires a Service Agent + per-bucket IAM and a service-
   account-authenticated client — real, but non-trivial.
2. **Cost on the alternatives.** Vertex Gemini has no verified batch discount on
   current flash-lite (the batch column is legacy Gemini 2.0 only), so a full
   corpus there is on-demand ~$130 (vs ~$12 Developer-API batch). Anthropic
   (~$56–276) and Claude have no native video; OpenAI is frame-based too.

Research across non-Google providers surfaced **qwen/qwen3.7-flash via OpenRouter**
at ~$0.03/$0.13 per 1M — ~$3 for the corpus — and, crucially, it is a
**synchronous** model: results return in-request, nothing is held server-side.

## Decision

Replace the Google `gemini-batch` backend with a **standalone, domain-agnostic
qwen batch service** (`~/repos/qwen-batch-service`) that mimics the batch
contract Dagster already orchestrates against:

- **Keep ADR-0007's shape**: Dagster owns orchestration (sub-second submit +
  harvest sensor); no domain enrichment worker, no per-item queue in `ops.sqlite`.
- **Move the async work to the service**: because qwen is synchronous, the
  service runs its own worker, holds durable per-item state in its own DB
  (resume-safe), and exposes `POST /jobs` → `GET /jobs/{id}` →
  `GET /jobs/{id}/results`. Dagster polls the service and harvests.
- **Media = client-side ffmpeg frame-sampling** of reels + native images, sent
  as image parts in-request. No File-API upload, no storage cap, and **no GCS
  media mirror** (dropped — frame-sampling reads the local byte cache).
- The service is **generic** (owns no domain schema), making it a dependent
  service, not the domain enrichment worker ADR-0002 rejected. It is declared a
  hard dependency: enrichment pings `/health` and **fails loudly** if the service
  is down.

## Why this is not a reversal of ADR-0007

ADR-0007's core decision — enrichment is orchestrated inside Dagster as async
submit/poll/harvest, not a one-shot external enrichment worker — is preserved.
Only the *backend* that holds the async job changes (Google's `gemini-batch` →
the standalone qwen service), and the service is deliberately outside the domain
(no `ops.sqlite`, no gold schema) so it is reusable infra, not the coupling
ADR-0002 removed.

## Alternatives considered

- **Gemini Dev batch via GCS-registration**: ~$12–18, keeps Google tooling, but
  needs the Service Agent + bucket IAM one-time setup and stays tied to Google.
- **Vertex on-demand**: ~$130, no vision batch discount.
- **Claude / OpenAI / frame models**: frame-sampling viable but ~$6 (GPT-4.1-nano
  batch) to ~$56+ (Haiku); qwen is cheaper and schema-faithful (pilot-validated).
- **Bring back the domain enrichment worker**: rejected — keeps the coupling
  ADR-0007 removed; the service holds state instead.

## Consequences

- The corpus (~8,649 media posts) runs on the qwen service at ~$3, ~15 h,
  resume-safe (per-item durable state), incremental on later runs.
- `max_tokens` ≥ 2500 (reasoning model truncates JSON otherwise); retry-on-empty
  for ~7% reasoning-model flakes.
- The GCS media mirror is unused and can be retired. The local byte cache remains
  the media source for frame-sampling.
