# Epic E-ENRICH-TRANSCRIPTS — ASR transcripts (local default + cloud burst)

- **Theme:** Richer media-grounded enrichment (new)
- **Owner:** dlc-worker
- **Status:** Open (design) — feasibility confirmed, no build
- **Feeds:** E-ENRICH-FACETS (text-layer), E-SERVING-ANALYTICS (search/embeddings)
- **Relates to:** E-ENRICH-FACETS (US-EFAC-4 text call consumes transcripts —
  DAG: `gold_text_annotations` depends on `gold_audio_transcripts`),
  E-ENRICH-SUMMARIES (US-ESUM-3 transcript summary; DAG:
  `gold_text_summaries` and `gold_content_classification` also depend on
  `gold_audio_transcripts`). Rides the shared submit/harvest seam as the local `whisper`
  executor plugin (ADR-0010 decision 5) — no remote API call.
## Outcome
A durable transcript per video in `gold_audio_transcripts`, produced **locally
and offline** (ffmpeg
audio-extract from the cached video → faster-whisper) at ~$0/min — giving the
text-layer facets their spoken channel and enabling search, with **no Gemini
dependency and no scrape-time expiry race**.
## Workload + storage contract (audit 2026-09-09; reconciled to the settled contract 2026-09-10)
- **Workload mapping:** audio → STT (whisper / faster-whisper). Per ADR-0010 decision 5, STT rides the SAME submit/harvest seam as
  the other workloads (ledger → poll-to-terminal → retrieve → upsert),
  hosted as the service's local `whisper` executor plugin — the earlier
  "local job outside the seam" carve-out is superseded. Execution stays
  local and offline ($0, no remote API call), but it is a seam job like any
  other. Pluggability sits at the EXECUTOR level (`local` | `gcp-spot`,
  US-ETR-4) — see `data/dev/dlc-enrichment-audit.md` §4.
- **Home table:** `gold_audio_transcripts` — keys `post_id`, `domain`;
  body `transcript`, `transcript_status` (`no_audio_source` | `pending` |
  `done` | `empty_audio`), `audio_present`, `asr_model`, `language`; plus the
  shared envelope metadata (ADR-0010 §4: `provider`, `model`, `prompt_hash`
  — NULL for ASR, there is no prompt — `schema_version`, `input_modality`,
  `content_mime_type`, `sampling_params_json`, `run_id`, `analysed_at`). The transcript is a
  durable derived artifact, not a facet and not a summary; it does not ride
  `gold_visual_annotations` / `gold_text_annotations`.
- **Audio-absent explicitness (product intent):** images/carousels have no
  audio, so their rows carry `transcript_status = no_audio_source` — the
  schema must distinguish that from "not yet transcribed" (`pending`) and
  "transcribed but empty (music-only)" (`empty_audio`). Consumers
  (US-EFAC-4, US-ESUM-3, `gold_content_classification`) route on this status.

## Why this is load-bearing
- **No `audioUrl` dependency:** the scrape-time byte cache already persists the
  video, so we ffmpeg-extract audio from the cached mp4 (confirmed: all 7,029
  cached mp4s have AAC, avg ~35 s, ffmpeg 8.1.2 available). Transcription is a
  run-anytime job, not a scrape-time race, and never touches a remote API (whisper runs locally).
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
