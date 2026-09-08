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
- AC1: Transcription runs offline (faster-whisper); no API key, no batch, no
      upload.
- AC2: Model choice configurable (`base.en` ≈ ~10–14 h, `small.en` ≈ ~25–35 h for
      full 7,029 backfill on CPU).
- AC3: Music-only / near-silent clips yield empty/low-confidence transcripts
      quickly — no separate pre-filter required.
- AC4: Transcript stored as an additive `transcript` text column (no Gemini
      schema keys touched).

## Definition of done
- [ ] Micro-benchmark run (one ~35 s cached reel) to pin real RTF before the full
      backfill — replaces the ~2–3× RTF estimate.
- [ ] Transcript column + schema-catalog entry + readiness green.

## Tests
- A speech-bearing clip yields a non-empty transcript.
- A music-only clip yields empty/flagged transcript without error.
