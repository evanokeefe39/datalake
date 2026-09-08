# Epic E-MEDIA — Scrape-time media & avatar byte capture

- **Theme:** Media infrastructure (capture, not enrichment)
- **Owner:** dlc-worker
- **Status:** Active
- **Depends on:** E-INGEST
- **Feeds:** E-ENRICH-ENGINE, E-ENRICH-FACETS, E-ENRICH-SUMMARIES,
  E-ENRICH-TRANSCRIPTS, E-SERVING-ANALYTICS

## Outcome
All post media (videos, carousel images, thumbnails) and profile avatars are
byte-cached **at scrape time** so they survive CDN expiry (~4–5 days), and
`silver_ig_posts.media_files` carries the live URLs the enrichment passes read.

## Why this is load-bearing
CDN media URLs die in days. The enrichment passes (engine/facets/summaries) and
**audio/transcript extraction** all read the cached video bytes; without this
cache they starve (the `media_files = "[]"` bug) or re-upload every cycle.

## Source of truth
`tasks/plans/multimodal-processing.md`, `media-and-entity-routing.md`,
`19-20-batch-multimodal-mime.md` (media_cache → File API wiring + mime fixes).

## Epic DoD
- [x] `media_cache` (bytes) + `media_metadata` (File API URIs) in ops.sqlite.
- [x] 7,029 cached mp4s on disk, all with AAC audio, avg ~35 s.
- [ ] Avatar byte capture wired to `ig_profiles_slv` (downstream of profile
      bronze data, still open).
