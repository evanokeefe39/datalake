---
id: US-EFAC-4
epic: E-ENRICH-FACETS
persona: P2
status: Open
---
# US-EFAC-4 — Text-layer facets (caption + transcript), cheap and re-runnable

- **Epic:** E-ENRICH-FACETS
- **Status:** Open
- **Relates to:** E-ENRICH-TRANSCRIPTS (transcript channel:
  `gold_audio_transcripts`), E-ENRICH-ENGINE
- **Source:** `docs/enrichment-enhancement-design.md` §3, §5, §7
- **Settled contract (2026-09-10):** the text pass writes
  `gold_text_annotations`; ONE text submit fans out at harvest to
  `gold_text_annotations` AND `gold_text_summaries` (US-ESUM-3). The DAG
  declares `gold_text_annotations` as DEPENDENT on `gold_audio_transcripts`.

## Story
**As a** platform engineer, **I want** the text-derivable facets (hook_content /
hook_type, disclosed sponsorship, claims, cta_type, audience_named, profanity,
value_depth, topical brand-safety) derived from caption + transcript into
`gold_text_annotations` in a separate, cheap TEXT call — **so that** schema
evolution iterates freely by re-running text only, never re-paying the
expensive visual input.

## Acceptance criteria (binary)
- AC1: Text facet call is media-free (caption + transcript + on-screen OCR only);
      it is the fast, re-runnable iteration surface. Output lands in
      `gold_text_annotations` keyed `(post_id, domain)` with its own
      `prompt_hash`.
- AC2: The expensive universal visual call's output is a fixed, rarely-changing
      *visual contract* (`gold_visual_annotations`); new semantic facets go to
      the text call (regression guard: adding a text facet must not re-send
      media).
- AC4: Transcript-optional — degrades gracefully to caption-only when a post
      has no transcript yet; transcript availability is routed on
      `gold_audio_transcripts.transcript_status` (US-ETR-2): `done` → include
      transcript; `pending` → caption-only now, re-discovered when done;
      `empty_audio` → include (empty transcript is a fact);
      `no_audio_source` → caption-only, permanent for images/carousels.
      Every routing decision is surfaced in run output, never silent.
- AC5: The text pass records ITS OWN provenance (text `prompt_hash` + `model` +
      `schema_version` + `run_id`) on `gold_text_annotations` — per-pass
      provenance (epic audit P0-4); it must not touch the visual pass's
      provenance on `gold_visual_annotations`.

## Definition of done
- [ ] Text call separated from the visual call in code; seam guard green.
- [ ] A schema-change exercise (add one text facet, re-run) shows visual input
      unchanged.
- [ ] Caption-only fallback covered by a test.

## Tests
- With transcript present vs absent, the text call produces equivalent required
  fields (graceful degradation).
- Re-running after adding a text facet issues zero new media (visual) requests.
