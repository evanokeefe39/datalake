---
id: US-ETR-2
epic: E-ENRICH-TRANSCRIPTS
persona: P1
status: Open
---
# US-ETR-2 — Local ASR transcription (faster-whisper, $0, no Gemini)

- **Epic:** E-ENRICH-TRANSCRIPTS
- **Status:** Open
- **Source:** `docs/enrichment-enhancement-design.md` §5

## Story
**As a** pipeline operator, **I want** the extracted audio transcribed locally
with faster-whisper (`small.en`/`base.en`, int8) **so that** a `transcript`
text is produced at ~$0/min with no Gemini dependency and no data leaving the
machine.

## Acceptance criteria (binary)
- AC1: Transcription executes locally via the seam's `whisper` executor
      (faster-whisper) — no remote API call, no upload. It submits/harvests on
      the shared seam like every other workload (ADR-0010 decision 5).
- AC2: Model choice configurable (`base.en` ≈ ~10–14 h, `small.en` ≈ ~25–35 h for
      full 7,029 backfill on CPU).
- AC3: Music-only / near-silent clips yield `transcript_status = empty_audio`
      with an empty/low-confidence transcript — a status, not a bare empty
      NULL, so consumers can distinguish "no speech" from "not yet done".
- AC4: Output persisted as an additive `gold_audio_transcripts` table (keys
      `post_id`, `domain`; `transcript`, `transcript_status`, `audio_present`,
      `asr_model`, `language`, plus the shared envelope metadata (`analysed_at`,
      `provider`, `model`, … — `prompt_hash` NULL for ASR; per ADR-0010 §4);
      provider mapping: audio = whisper). No Gemini/qwen schema keys
      touched; no `gold_visual_annotations` / `gold_text_annotations` change.
- AC5: Image/carousel posts get `transcript_status = no_audio_source` (no
      ffmpeg decode attempted) — audio-absence is explicit, never recorded as
      an empty result.

## Definition of done
- [ ] Micro-benchmark run (one ~35 s cached reel) to pin real RTF before the full
      backfill — replaces the ~2–3× RTF estimate.
- [ ] Transcript column + schema-catalog entry + readiness green.

## Tests
- A speech-bearing clip yields a non-empty transcript.
- A music-only clip yields empty/flagged transcript without error.
