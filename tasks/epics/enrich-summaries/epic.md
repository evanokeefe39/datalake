# Epic E-ENRICH-SUMMARIES — Video & carousel visual summaries

- **Theme:** Richer media-grounded enrichment (new)
- **Owner:** dlc-worker
- **Status:** Open (design) — fold decision spike-backed; build not started
- **Depends on:** E-ENRICH-ENGINE, E-MEDIA
- **Relates to:** E-ENRICH-FACETS (same universal video call), E-SERVING-ANALYTICS
  (embeddings/search/qualitative use)

## Outcome
Every media post carries a visual-first `content_summary` (video) and one
summary per carousel image, produced inside the same universal video call the
facets already pay for — so embeddings/search and qualitative "what do winning
posts show/say" get a visual narrative with no second video round-trip.

## Design decision (spike-backed, 2026-09-08)
Summaries are **folded into the single universal video call**, NOT a separate
pass. This deliberately reverses triage-first because the video input is already
spent on the facet core every media post — asking for a bounded summary is
near-free marginal output and eliminates a second video round-trip (the exact
scope the user is cutting). Output cap 4096; video summary 2–3 sentences
visual-first; one sentence per carousel image.

**Spike evidence** (`scripts/facet_summary_spike.py`, 22 posts × A/B_F/B_S):
facet fidelity under fold held (is_sponsored/text_overlay 100%, face_present/
sponsorship 95%, value_medium 86%); carousel index-alignment 11/11; A = 1
video call vs B = 2. **Boundary:** the A-vs-B_S judge is self-referential,
caption-context (rates caption-plausibility, not true visual grounding) — so
visual *faithfulness* of the folded summary is the residual unmeasured risk.

## Source of truth
`docs/enrichment-enhancement-design.md` §6, `scripts/facet_summary_spike.py`,
`data/facet_summary_spike.duckdb`.

## Epic DoD (draft)
- [ ] `content_summary` + `image_summaries` present in the universal call output,
      validated (parse + carousel length == n_media).
- [ ] Re-test visual faithfulness ONLY if a summary becomes decision-grade
      (then spot-check or summary-only on that subset); not for embeddings use.
