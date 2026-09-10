---
id: US-ESUM-3
epic: E-ENRICH-SUMMARIES
persona: P3
status: Open
---
# US-ESUM-3 — Transcript summary for video (what is SAID), folded into the text call

- **Epic:** E-ENRICH-SUMMARIES
- **Status:** Open
- **Relates to:** E-ENRICH-TRANSCRIPTS (transcript source:
  `silver_audio_transcripts`), E-ENRICH-FACETS (US-EFAC-4 — same cheap text call;
  ONE text submit fans out at harvest to `silver_text_annotations` +
  `silver_text_summaries`)
- **Source:** audit `data/dev/dlc-enrichment-audit.md` §1.1 row 8, §3 P0-3

## Story
**As a** growth analyst, **I want** a short `transcript_summary` — a summary of
what the creator SAYS, distinct from the visual `content_summary` — produced in
the same cheap text call that already reads caption + transcript and stored in
`silver_text_summaries`, **so that** search/embeddings and qualitative analysis
cover the spoken channel with zero extra input cost.

## Acceptance criteria (binary)
- AC1: `transcript_summary` is a dedicated table — `silver_text_summaries`
  (transcript_summary), written by the TEXT pass with its own provenance
  (P0-4 per-pass provenance; `prompt_hash`, `model`, `schema_version`),
  validator-checked (bounded length). The text submit fans out to
  `silver_text_annotations` AND `silver_text_summaries` — one job, two silver
  writes, never two submits.
- AC2: Video posts WITH a transcript
  (`silver_audio_transcripts.transcript_status = done`) get a transcript
  summary. Posts with `empty_audio` get an explicit empty summary, not NULL.
- AC3: Image/carousel posts are **audio-absent, not skipped**: no
  transcript summary is requested; the row's absence is explained by
  `silver_audio_transcripts.transcript_status = no_audio_source` (US-ETR-2),
  never a bare NULL.
- AC4: Transcript-optional degradation (US-EFAC-4 AC4) holds: a video still
  awaiting transcription yields no transcript summary and is re-discovered once
  the transcript lands — surfaced in run output, never silent.

## Definition of done
- [ ] Text-call prompt + parser extended with `transcript_summary` (text pass
      only; visual sub-validator still rejects it — same split enforcement as
      `defs/enrichment/growth_facets_schema.py:146-155`).
- [ ] Schema-catalog + readiness green for `silver_text_summaries`.
- [ ] Status-based routing tested against real `silver_audio_transcripts` rows.

## Tests
- Video with `done` transcript → summary present; `no_audio_source` row → no
  summary requested and status explains absence.
- `empty_audio` → explicit empty summary (distinguishable from not-yet-done).
- Adding `transcript_summary` to the text call sends zero new media requests.

