---
id: US-EFAC-4
epic: E-ENRICH-FACETS
persona: P2
status: Open
---
# US-EFAC-4 — Text-layer facets (caption + transcript), cheap and re-runnable

- **Epic:** E-ENRICH-FACETS
- **Status:** Open
- **Relates to:** E-ENRICH-TRANSCRIPTS (transcript channel), E-ENRICH-ENGINE
- **Source:** `docs/enrichment-enhancement-design.md` §3, §5, §7

## Story
**As a** platform engineer, **I want** the text-derivable facets (hook_content /
hook_type, disclosed sponsorship, claims, cta_type, audience_named, profanity,
value_depth, topical brand-safety) derived from caption + transcript in a
separate, cheap TEXT call — **so that** schema evolution iterates freely by
re-running text only, never re-paying the expensive video input.

## Acceptance criteria (binary)
- AC1: Text facet call is media-free (caption + transcript + on-screen OCR only);
      it is the fast, re-runnable iteration surface.
- AC2: The expensive universal video call's output is a fixed, rarely-changing
      *visual contract*; new semantic facets go to the text call (regression guard:
      adding a text facet must not re-send video).
- AC3: Text call output also additive + prompt_versioned + validator-checked.
- AC4: Transcript-optional — degrades gracefully to caption-only when a post has
      no transcript yet.

## Definition of done
- [ ] Text call separated from the video call in code; seam guard green.
- [ ] A schema-change exercise (add one text facet, re-run) shows video input
      unchanged.
- [ ] Caption-only fallback covered by a test.

## Tests
- With transcript present vs absent, the text call produces equivalent required
  fields (graceful degradation).
- Re-running after adding a text facet issues zero new media (video) requests.
