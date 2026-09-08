---
id: US-ESUM-1
epic: E-ENRICH-SUMMARIES
persona: P3
status: Open
---
# US-ESUM-1 — content_summary + per-image carousel summaries in the universal call

- **Epic:** E-ENRICH-SUMMARIES
- **Status:** Open
- **Relates to:** E-ENRICH-FACETS (US-EFAC-3 — same single video call),
  E-SERVING-ANALYTICS
- **Source:** `docs/enrichment-enhancement-design.md` §6; fold spike

## Story
**As a** growth analyst, **I want** a visual-first `content_summary`
per video and one short summary per carousel image, emitted in the same single
video call as the visual facets, **so that** I get a searchable/embeddable
visual narrative without a second (expensive) video round-trip.

## Acceptance criteria (binary)
- AC1: Folded output shape — video: `{visual_facets, content_summary}`; carousel:
      `{visual_facets, image_summaries:[{index, summary},…]}` — at
      `max_output_tokens=4096`.
- AC2: Carousel `image_summaries` length == number of images sent (index
      alignment validated, mismatches surfaced not silent).
- AC3: Summary is free-text — stored as its own additive field/column, NOT part
      of the reliability-gated facet JSON.
- AC4: No second video call for summaries (regression guard on single-pass).

## Definition of done
- [ ] Universal-call output includes summaries; validator checks carousel length.
- [ ] Prototype (~10 posts incl. a carousel) reviewed for quality + alignment
      before full wiring.
- [ ] Single-pass guard: summary does not trigger re-upload or a second media
      request.

## Tests
- Carousel with N images → validator asserts exactly N `image_summaries` entries.
- A malformed/truncated summary block is surfaced, never silently dropped.
- Full corpus would pay 1 video input per post (assert via request counting).
