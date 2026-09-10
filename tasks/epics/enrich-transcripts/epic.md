# Epic E-ENRICH-TRANSCRIPTS — ASR transcripts (local default + cloud burst)

- **Theme:** Richer media-grounded enrichment (new)
- **Owner:** dlc-worker
- **Status:** Open (design) — feasibility confirmed, no build
- **Feeds:** E-ENRICH-FACETS (text-layer), E-SERVING-ANALYTICS (search/embeddings)
- **Relates to:** E-ENRICH-FACETS (US-EFAC-4 text call consumes transcripts —
  DAG: `gold_text_annotations` depends on `gold_audio_transcripts`),
  E-ENRICH-SUMMARIES (US-ESUM-3 transcript summary; DAG:
  `gold_text_summaries` and `gold_content_classification` also depend on
  `gold_audio_transcripts`). Deliberately independent
  of the remote-async enrichment engine (no Gemini/qwen dependency).
## Outcome
A durable transcript per video in `gold_audio_transcripts`, produced **locally
and offline** (ffmpeg
audio-extract from the cached video → faster-whisper) at ~$0/min — giving the
text-layer facets their spoken channel and enabling search, with **no Gemini
dependency and no scrape-time expiry race**.
## Workload + storage contract (audit 2026-09-09; reconciled to the settled
## contract 2026-09-10)
- **Workload mapping:** audio → STT (whisper / faster-whisper). STT is a LOCAL
  resumable-job pattern, NOT the remote-async ingest seam (ledger → poll →
  retrieve): there is no remote terminal state to poll. Its pluggability sits
  at the EXECUTOR level (`local` | `gcp-spot`, US-ETR-4) — see
  `data/dev/dlc-enrichment-audit.md` §4.
- **Home table:** `gold_audio_transcripts` — keys `post_id`, `domain`;
  `transcript`, `transcript_status` (`no_audio_source` | `pending` | `done` |
  `empty_audio`), `audio_present`, `asr_model`, `language`, `transcribed_at` —
  own provenance (ASR model version, not a prompt hash). The transcript is a
  durable derived artifact, not a facet and not a summary; it does not ride
  `gold_visual_annotations` / `gold_text_annotations`.
- **Audio-absent explicitness (product intent):** images/carousels have no
  audio, so their rows carry `transcript_status = no_audio_source` — the
  schema must distinguish that from "not yet transcribed" (`pending`) and
  "transcribed but empty (music-only)" (`empty_audio`). Consumers
  (US-EFAC-4, US-ESUM-3, `gold_content_classification`) route on this status.

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
- [ ] ffmpeg audio-extract → faster-whisper job; `gold_audio_transcripts`
      additive table with transcript_status (audio-absent explicit per above).
- [ ] Incremental-at-scrape + resumable overnight backfill; music-only clips
      yield `empty_audio` (a status, not a bare empty NULL).
- [ ] **Pluggable transcript backend** (US-ETR-4): `local` faster-whisper is the
      default ($0; incremental + prod overnight); an on-demand **GCP spot GPU
      burst** in the SAME region as the media bucket runs the SAME model for
      fast dev/test slices (parity preserved, per-second billing). Cost-
      guarded + guaranteed tear-down.
