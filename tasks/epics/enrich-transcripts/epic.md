# Epic E-ENRICH-TRANSCRIPTS — Local ASR transcript capture

- **Theme:** Richer media-grounded enrichment (new)
- **Owner:** dlc-worker
- **Status:** Open (design) — feasibility confirmed, no build
- **Depends on:** E-MEDIA (cached video bytes)
- **Feeds:** E-ENRICH-FACETS (text-layer), E-SERVING-ANALYTICS (search/embeddings)
- **Relates to:** (none — deliberately **independent of the Gemini batch engine**)

## Outcome
A `transcript` text column per video, produced **locally and offline** (ffmpeg
audio-extract from the cached video → faster-whisper) at ~$0/min — giving the
text-layer facets their spoken channel and enabling search, with **no Gemini
dependency and no scrape-time expiry race**.

## Why this is load-bearing and independent
- **No `audioUrl` dependency:** the scrape-time byte cache already persists the
  video, so we ffmpeg-extract audio from the cached mp4 (confirmed: all 7,029
  cached mp4s have AAC, avg ~35 s, ffmpeg 8.1.2 available). Transcription is a
  run-anytime job, not a scrape-time race, and never touches Gemini/batch.
- Full backfill ≈ ~10–35 h CPU (base/small.en int8 on laptop or cx33) — an
  unattended 1–2 night job at $0; incremental per scrape = minutes. GPU (if ever)
  ~1–2 h via Parakeet; not needed for a one-time backfill.

## Source of truth
`docs/enrichment-enhancement-design.md` §5, §7–§8; media facts measured from
`ops.sqlite` + ffprobe (2026-09-07).

## Epic DoD (draft)
- [ ] ffmpeg audio-extract → faster-whisper job; `transcript` additive column.
- [ ] Incremental-at-scrape + resumable overnight backfill; music-only clips yield
      near-empty transcripts (no pre-filter needed).
