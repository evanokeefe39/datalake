---
id: US-EFAC-1
epic: E-ENRICH-FACETS
persona: P3
status: Open
---
# US-EFAC-1 — Lock the V3 cross-modal facet schema

- **Epic:** E-ENRICH-FACETS
- **Status:** Open
- **Source:** `docs/architecture/enrichment-design-v1-superseded.md` §4, `facet-list-experiment-design.md`

## Story
**As a** growth analyst, **I want** the V3 facet schema (enum
categoricals + codebook decision-rules + boundary exemplars) finalized across
all evidence channels, **so that** facet output is agreement-gated, stable, and
reusable by downstream analytics and audit.

## Acceptance criteria (binary)
- AC1: Field set split is explicit: visual-necessary (face_present, brand_logos,
  value_medium, what's-shown, on-screen-only claims) vs cross-modal
  (hook, is_sponsored, claims, cta_type, profanity, brand-safety), each with
  enum + codebook.
- AC2: `value_medium` granularity resolved (weakest V3 field, ~0.82) — enums
  either tightened or demoted to open-with-`other`.
- AC3: `brand_logos` / `on_screen_claim` definitions firmed (the two softest in
  the fold spike, 68/73%).
- AC4: Brand-safety is the explicit 6-flag set (profanity, sexualized_content,
  political, medical_claims, financial_guarantees, violence_trauma); the vague
  `sensitive_adjacency` is gone.
- AC5: Reserved classification keys (the `silver_content_classification`
  body: is_educational/actionable, admiralty, domain — CONTENT niche,
  content_type, format, *_json) structurally excluded from facet output; a
  post-parse validator rejects any reserved key or missing required field.
- AC6: Prompt is versioned (own `prompt_hash`) so staleness is detectable.

## Definition of done
- [ ] Schema doc + JSON-Schema committed under `docs/`.
- [ ] Validator unit-tested (rejects reserved keys / missing fields).
- [ ] Agreement gate: a V3 re-run on a sample holds ≥0.8 on decision-grade
      fields (hook_type excluded — directional only).

## Tests
- Validator rejects a crafted JSON containing `domain` / `format` / a bogus
  `_json` key (reserved for `silver_content_classification`).
- Every enum field's allowed values match the schema; out-of-enum value → fail.
