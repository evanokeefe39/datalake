# Epic E-ENRICH-FACETS — Cross-modal growth facets

- **Theme:** Richer media-grounded enrichment (new)
- **Owner:** dlc-worker
- **Status:** Open (design) — schema is V3-validated; build not started
- **Depends on:** E-ENRICH-ENGINE, E-MEDIA, E-SERVING-ANALYTICS (utility check)
- **Relates to:** E-ENRICH-SUMMARIES (same universal video call), E-ENRICH-TRANSCRIPTS
  (text-layer facets), E-ENRICH-LABELS (standout/underperformer labels reused as the
  engagement-utility criterion)

## Outcome
Posts carry a validated, additive `growth_facets_json`: cross-modal facets
(hooks, value/format, CTA, sponsorship, an explicit brand-safety set, claims)
judged across every channel that carries their evidence — caption, ASR
transcript, on-screen OCR, imagery — so creator-growth analytics can split
"what works" by observable content mechanics.

## Design state (evidence)
- Facets are **modality-agnostic**, not text-vs-visual assigned (user-corrected).
  Visual-necessary core: face_present, what's shown, brand_logos, value_medium,
  on-screen-only claims. Cross-modal: hooks, sponsorship, claims, CTA, profanity,
  brand-safety.
- **Brand-safety = explicit enumerable set** (brand-selectable, any channel):
  `brand_safety_profanity`, `_sexualized_content`, `_political`,
  `_medical_claims`, `_financial_guarantees`, `_violence_trauma`. The vague
  `sensitive_adjacency` is retired.
- **V3 prompt (enum categoricals + codebook decision-rules + exemplars)** is the
  winner (95 posts × 4 presentations × 2 runs): audience_named 1.00, face_present
  1.00, value_depth 0.95, cta_type 0.97, hook_type 0.84 (directional-grade), etc.
- Reserved gold keys (is_educational/actionable, admiralty, domain, format +
  *_json) structurally excluded; two-pass additive (own column/hash).
- **No human gold** (user decision). Validity = V3 agreement gate +
  **downstream engagement-utility** (do facets discriminate standout/hot vs
  underperformer, from lake data).

## Source of truth
`docs/enrichment-enhancement-design.md` §3–§4,
`tasks/plans/facet-list-experiment-design.md` (experiments + literature),
spikes `data/facet_menu.duckdb`, `data/facet_experiment.duckdb`,
`data/facet_caption.duckdb`.

## Epic DoD (draft)
- [ ] V3 facet schema locked; reserved-key + post-parse validator exists.
- [ ] Universal visual-facet pass additive (own hash); schema-catalog + readiness
      green; engagement-utility report shows which facets discriminate.
- [ ] Text-layer facet call (caption+transcript) cheap and re-runnable.
