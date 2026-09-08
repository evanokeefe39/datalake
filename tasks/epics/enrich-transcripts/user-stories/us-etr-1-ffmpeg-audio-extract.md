---
id: US-ETR-1
epic: E-ENRICH-TRANSCRIPTS
persona: P1
status: Open
---
# US-ETR-1 — Extract audio from the cached video (no audioUrl dependency)

- **Epic:** E-ENRICH-TRANSCRIPTS
- **Status:** Open
- **Source:** `docs/enrichment-enhancement-design.md` §5

## Story
**As a** pipeline operator, **I want** the audio track pulled from the already-
cached video bytes via ffmpeg (not a separate expiring `audioUrl`), **so that**
transcription can run at any time with no scrape-time capture race and no
re-download.

## Acceptance criteria (binary)
- AC1: `ffmpeg -i <cached.mp4> -vn` → 16 kHz mono WAV (or equivalent decode) works
      for all cached content videos (confirmed AAC); corrupt/unreadable file fails
      per-file, never aborts the job.
- AC2: Read from the local `media_cache` path (7,029 mp4s present); no CDN/HTTP
      fetch for audio extraction.
- AC3: Audio is transient — decode → transcribe → discard; only text persists.

## Definition of done
- [ ] ffmpeg extraction smoke-tested on a sample of cached mp4s (probe shows audio
      stream present before decode).
- [ ] A corrupt-file case returns a per-file error to a retry/skip surface, not a
      hard crash.

## Tests
- Each cached mp4 in a sample yields a decodable audio stream (ffprobe check).
- Missing/corrupt file → job continues, file marked skipped.
