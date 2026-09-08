# Epic E-ENRICH-ENGINE — Batch-native Gemini enrichment engine

- **Theme:** Enrichment engine (core)
- **Owner:** dlc-worker
- **Status:** Active
- **Depends on:** E-MEDIA
- **Feeds:** E-ENRICH-LABELS, E-ENRICH-FACETS, E-ENRICH-SUMMARIES,
  E-SERVING-ANALYTICS

## Outcome
A single, hermetic Gemini batch engine (submit → harvest) that enriches posts
durably, media-aware, with per-item retry and a terminal dead-letter — no API
calls inside transform layers, no external worker.

## Scope highlights
- Batch-native architecture (ADR-0007) replacing the external REST worker
  (ADR-0002); two-world split resolved (ADR-0003 no-API-in-transform).
- `gemini_batch.submit/retrieve`; chunking; in-flight token caps per tier.
- Multimodal media pass: `lookup_or_upload_all` (scrape-time byte cache → File
  API), inline small-image optimization, low-res video (~98 tok/s), per-item
  media resolution dropped on dead CDN.
- Dead-letter (ops.sqlite) terminal after MAX_ATTEMPTS; `scheduled_for` backoff.
- Tier strategy: free / Tier-1 / Tier-2 escalation triggers.

## Cross-cutting
- Seam guard: `media_upload.seam_violations()==[]` must hold.
- Gemini 429 subtypes: rate-limit (backoff) vs quota-exhausted (stop until
  UTC midnight) must be distinguished.

## Source of truth
`tasks/plans/enrichment-architecture-v2.md`, `migration-enrichment-v2.md`,
`gold-enrichment-scaling.md`, `docs/refactor-research/batch-native-enrichment*`,
`enrichment-service-patterns.md`, ADR-0007/0008.

## Epic DoD
- [x] Batch submit/harvest path live; interactive multimodal smoke-proven.
- [ ] Batch-multimodal (gemini-batch with media) shipped at scale (deferred
      follow-up; interactive is text/media path today).
