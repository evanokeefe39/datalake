---
id: US-ESUM-1
epic: E-ENRICH-SUMMARIES
persona: P3
status: Open
---
# US-ESUM-1 — content_summary + per-image carousel summaries in the universal call

- **Epic:** E-ENRICH-SUMMARIES
- **Status:** Open
- **Relates to:** E-ENRICH-FACETS (US-EFAC-3 — same single visual call; ONE
  submit fans out at harvest to `silver_visual_annotations` +
  `silver_visual_summaries`),
  E-SERVING-ANALYTICS
- **Source:** `docs/enrichment-enhancement-design.md` §6; fold spike

## Story
**As a** growth analyst, **I want** a visual-first `content_summary` per video,
an **overall summary of the images** per carousel, and one short summary per
carousel image — emitted in the same single visual call as the visual facets
and written to `silver_visual_summaries` — **so that** I get a
searchable/embeddable visual narrative without a second (expensive) visual
round-trip. For a single image the overall summary IS the individual summary
(one output, not two).

## Acceptance criteria (binary)
- AC1: Folded output shape — video: `{visual_facets, content_summary}`;
      carousel: `{visual_facets, content_summary (OVERALL of the images),
      image_summaries:[{index, summary},…]}`; single image:
      `{visual_facets, content_summary}` — at `max_output_tokens=4096`.
      (Audit 2026-09-09: today the prompt asks per-image summaries only for
      carousels — `defs/enrichment/prompts.py:110-124` — so the overall is NEW.)
- AC2: Carousel `image_summaries` length == number of images sent (index
      alignment validated, mismatches surfaced not silent).
- AC3: Summaries are free-text — written to `silver_visual_summaries`
      (`content_summary`, `image_summaries_json`), NOT part of the
      reliability-gated facet JSON in `silver_visual_annotations`.
- AC4: No second visual call for summaries (regression guard on single-pass).

## Definition of done
- [ ] Universal-call output includes overall + per-image summaries; validator
      checks carousel length; `content_summary` populated for every content form.
- [ ] Prototype (~10 posts incl. a carousel) reviewed for quality + alignment
      before full wiring.
- [ ] Single-pass guard: summary does not trigger re-upload or a second media
      request.

## Tests
- Carousel with N images → validator asserts exactly N `image_summaries` entries.
- A malformed/truncated summary block is surfaced, never silently dropped.
- Full corpus would pay 1 visual input per post (assert via request counting).

