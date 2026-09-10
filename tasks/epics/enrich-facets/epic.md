# Epic E-ENRICH-FACETS — Cross-modal growth facets

- **Theme:** Richer media-grounded enrichment (new)
- **Owner:** dlc-worker
- **Status:** Open (design) — schema is V3-validated; build not started
- **Depends on:** E-ENRICH-ENGINE (now the qwen batch service, ADR-0009), E-MEDIA,
  E-SERVING-ANALYTICS (utility check)
- **Relates to:** E-ENRICH-SUMMARIES (one visual submit fans out to both visual
  tables — annotations + summaries), E-ENRICH-TRANSCRIPTS
  (text-layer facets), E-ENRICH-LABELS (standout/underperformer labels reused as the
  engagement-utility criterion)

## Backend + workload mapping (2026-09-10, ADR-0009 + audit; reconciled to the
## settled contract `data/dev/enrichment-final-design.md`)
Workloads map by capability — pixels→vision, audio→STT (whisper), text→text LLM.
**Provider is NOT in the table name** — it lives in per-row metadata
(`provider`, `model`), so a provider swap never renames a table (vision is
locked to qwen and audio to whisper in practice, but names stay
provider-agnostic; text is swappable — qwen default, Gemini pluggable):
- **Visual facets** = qwen VISION workload (`silver_visual_annotations`) — the
  **visual** pass (images AND video frames — the pass is "visual", not "video";
  media is client-side ffmpeg frame-sampled, sent as image parts to the qwen
  batch service — not Gemini File-API URIs / `MEDIA_RESOLUTION_LOW`).
- **Text facets** = qwen TEXT workload (`silver_text_annotations`) over caption
  (+ transcript from `silver_audio_transcripts` once E-ENRICH-TRANSCRIPTS lands —
  the DAG declares text_* as DEPENDENT on `silver_audio_transcripts`).
The facets schema, prompt hashing, and additive silver-column semantics are
unchanged by backend choice; only the transport changed. Harvest lands the
verbatim response in `bronze_enrichment_raw` and conforms it to silver with
ZERO API calls (repo's ONE remote-async ingest pattern: ledger → submit →
poll → retrieve → idempotent MERGE with loud per-item failure) —
`data/dev/dlc-enrichment-audit.md` §2.

**One-submit-fans-out (settled contract):** the visual submit returns
annotations AND summaries in ONE request → ONE job fans out at harvest to
`silver_visual_annotations` AND `silver_visual_summaries` (E-ENRICH-SUMMARIES).
The text submit likewise fans out to `silver_text_annotations` AND
`silver_text_summaries` (US-ESUM-3). NOT one submit per table — that would
double the bill. Enum/bounded-field definitions live in the versioned schema
registry, referenced by `schema_version`; there are NO `_bound` columns.

**Provenance requirement (audit P0-4), reconciled:** the old shared
`gold_growth_facets` table with ONE `prompt_hash` for TWO writers is retired —
`gold_growth_facets` splits into four tables — `silver_visual_annotations` +
`silver_visual_summaries` (visual pass) and `silver_text_annotations` +
`silver_text_summaries` (text pass) — each with its OWN provenance
(`prompt_hash`, `model`, `schema_version`, `run_id`, …), so each pass's output
is attributable and stale-detectable independently; the `model`-as-done-marker
hack retires with it. Expressed here as a requirement; the DDL lands with the
implementing story.

Posts carry validated, additive enrichment outputs split per channel:
`silver_visual_annotations` (face_present, value_medium, brand_logos,
text_overlay_present, on_screen_claim) and `silver_text_annotations` (hooks,
sponsorship, claims, CTA, audience/value depth, hashtag strategy, an explicit
`brand_safety_json` field, evidence) — cross-modal facets judged across every channel
that carries their evidence (caption, ASR transcript from
`silver_audio_transcripts`, on-screen OCR, imagery) — so creator-growth
analytics can split "what works" by observable content mechanics.

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
- Reserved classification keys (the `silver_content_classification` body:
  is_educational/actionable, admiralty, domain — CONTENT niche, + *_json)
  structurally excluded from facet output; two-pass additive (own table + hash per pass).
- **No `_bound` columns** — enum/bounded-field definitions are versioned in the
  schema registry and referenced per-row by `schema_version`.
- **No human gold** (user decision). Validity = V3 agreement gate +
  **downstream engagement-utility** (do facets discriminate standout/hot vs
  underperformer, from lake data).

## Source of truth
`docs/enrichment-enhancement-design.md` §3–§4,
`tasks/plans/facet-list-experiment-design.md` (experiments + literature),
spikes `data/facet_menu.duckdb`, `data/facet_experiment.duckdb`,
`data/facet_caption.duckdb`.

## Epic DoD (draft)
- [ ] V3 facet schema locked; reserved-key + post-parse validator exists;
      enum definitions versioned in the schema registry (`schema_version`) —
      no `_bound` columns.
- [ ] Universal visual-facet pass additive into `silver_visual_annotations` (own
- [ ] Per-pass provenance split: `silver_visual_annotations` and
      `silver_text_annotations` each carry their own prompt_hash + model — no
      shared-hash overwrite, no `model`-as-done-marker.
- [ ] Per-pass provenance schema-catalog + readiness green; the
      engagement-utility report shows which facets discriminate.
- [ ] Text-layer facet call (caption + `silver_audio_transcripts` transcript,
      status-routed; DAG: `silver_text_annotations` depends on
      `silver_audio_transcripts`) cheap and re-runnable.
