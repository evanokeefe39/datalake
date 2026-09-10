# Enrichment design v2 — the definitive spec

Settled 2026-09-09. Source of truth: `data/dev/enrichment-final-design.md`;
naming and provenance decisions are recorded in
[ADR-0010](adr/0010-enrichment-naming-and-provenance.md). This document is the
canonical enrichment spec. `docs/enrichment-enhancement-design.md` (v1,
2026-09-07) is retained as the rationale/experiment record — where the two
differ, THIS document wins.

## 1. Product intent (per content form)

| Content form | Gold outputs |
|---|---|
| **Reels/video** (audio present) | visual annotations (`gold_visual_annotations`), text annotations (`gold_text_annotations`), transcript (`gold_audio_transcripts`), visual summary (`gold_visual_summaries.content_summary`), transcript summary (`gold_text_summaries.transcript_summary`) |
| **Image carousel / single** (no audio) | visual annotations (`gold_visual_annotations`), text annotations (`gold_text_annotations`), visual summaries — per-image (`image_summaries_json`) + overall (`content_summary`) |

Transcript outputs are N/A for image/carousel (no audio) — but the absence is
**explicit**: such posts get a `gold_audio_transcripts` row with
`transcript_status = no_audio_source`, never a silent NULL and never a missing
row. The same status enum separates `pending` / `done` / `empty_audio`.

## 2. Naming rules (final)

- Layer-first: `gold_`. Channel token: `visual` | `text` | `audio` — **NOT
  "video"** (the visual pass covers images AND video frames).
- Pattern: `gold_<channel>_<artifact>` — `gold_visual_annotations`,
  `gold_visual_summaries`, `gold_text_annotations`, `gold_text_summaries`,
  `gold_audio_transcripts`. The one cross-channel taxonomy artifact is
  content-scoped, not channel-scoped: `gold_content_classification`.
- **Provider is NOT in the table name.** Vision is locked to qwen and audio to
  whisper in practice, but names stay provider-agnostic so a provider swap
  never renames a table. Provider/model live in metadata columns (§4).
- **No `_bound` columns.** Enum/bounded-field definitions live in the versioned
  schema registry, referenced by `schema_version` (the locked V3 facet schema,
  `GROWTH_FACETS_SCHEMA_VERSION = "3"`, is the first entry of that pattern). No
  per-row conformance flags.
- Body column names match the table's schema keys 1:1 (JSON↔column mapping is
  trivial).

## 3. Tables

Six enrichment tables (external, paid, provenance-stamped). Deterministic
post/creator/comment metrics are intentionally NOT a new gold layer — the
serving views (`v_post_metrics`, `v_creator_metrics`, `v_profile_metrics`)
already own them.

| Table | Artifact | Producer/workload | Inputs |
|---|---|---|---|
| `gold_visual_annotations` | visual facets | qwen vision | images / video frames |
| `gold_visual_summaries` | visual summaries (overall + per-image) | qwen vision | images / video frames |
| `gold_audio_transcripts` | raw STT transcript | whisper | audio (reels) |
| `gold_text_annotations` | text-layer facets | text LLM (provider swappable; qwen default) | caption + transcript |
| `gold_text_summaries` | transcript summary | text LLM | transcript |
| `gold_content_classification` | taxonomy + educational/actionable + admiralty | text LLM | caption + transcript + visual summaries |

## 4. Columns

### Keys + metadata (all enrichment tables)

- **Keys:** `post_id`, `domain`.
- **Metadata (provenance, every enrichment table):** `provider`, `model`,
  `prompt_hash`, `schema_version`, `input_modality`, `content_mime_type`,
  `sampling_params_json`, `run_id`, `analysed_at`.

Per-pass provenance is structural: each pass owns its table, so its
`prompt_hash`/`model`/`run_id` can never be overwritten by another pass (the
defect that killed the single-table facet store — see §9).

### Body columns

| Table | Body columns |
|---|---|
| `gold_visual_annotations` | `face_present`, `value_medium`, `brand_logos`, `text_overlay_present`, `on_screen_claim` |
| `gold_visual_summaries` | `content_summary` (**overall** visual summary for ALL forms — video frames / single image / carousel overall; this resolves the carousel overall-summary gap), `image_summaries_json` (per-image, carousel only) |
| `gold_audio_transcripts` | `transcript`, `transcript_status` (`no_audio_source` \| `pending` \| `done` \| `empty_audio`), `audio_present`, `asr_model`, `language` |
| `gold_text_annotations` | `hook_content`, `hook_type`, `is_sponsored`, `sponsorship_signal`, `claimed_results`, `cta_type`, `audience_named`, `value_depth`, `replicable_tactic`, `hashtag_strategy`, `evidence`, `brand_safety_json` |
| `gold_text_summaries` | `transcript_summary` |
| `gold_content_classification` | `domain`, `subdomain`, `topic`, `subtopic`, `is_educational`, `is_actionable`, `admiralty`, `content_type`, `style`, `format` |

Facet field semantics (enums, codebook wording, agreement grades) are locked in
`docs/growth-facets-schema.md` (V3): cross-modal fields →
`gold_text_annotations`; visual-necessary fields → `gold_visual_annotations`.
The facet schema itself stays channel-blind; `evidence` records which channels
grounded each judgment — the table split is by producing pass, not by claim
that a field is single-channel.

**Overlap flag (open decision):** `format` / `content_type` / `style` on
`gold_content_classification` overlap `gold_visual_annotations.value_medium`.
"Form" should live once — the recommendation is to drop `format` from
classification. **OPEN** — do not resolve silently (§8).

## 5. Dependency DAG

```
media ─┬─► gold_visual_annotations
       ├─► gold_visual_summaries
       └─► gold_audio_transcripts
caption + transcript ─► gold_text_annotations        (dep: gold_audio_transcripts)
transcript ───────────► gold_text_summaries          (dep: gold_audio_transcripts)
caption + transcript + visual summaries ─► gold_content_classification
                                          (deps: gold_audio_transcripts, gold_visual_summaries)
```

Dependencies are **declared per asset**. Re-running an upstream invalidates
downstream — the staged/derived deps are declared, not emergent: text passes
wait on `gold_audio_transcripts`; classification waits on
`gold_audio_transcripts` + `gold_visual_summaries`.

## 6. Asset graph — one async seam, one submit per pass

**CRITICAL cost/payload correctness:** the visual call returns annotations AND
summaries in ONE request; the text call returns text annotations AND transcript
summary in ONE request. The graph therefore has **ONE visual submit fanning out
to both visual tables at harvest, and ONE text submit fanning out to both text
tables** — NOT one submit per table (that would double the bill).
`gold_content_classification` is its own submit (it additionally consumes
visual summaries/transcripts).

```
silver ─┬─(submit qwen-vision)─► visual job ─(harvest)─┬─► gold_visual_annotations
        │                                              └─► gold_visual_summaries
        ├─(submit whisper)────► audio job  ─(harvest)───► gold_audio_transcripts
        │
        │   after transcripts (+ summaries):
        ├─(submit text-LLM)───► text job   ─(harvest)─┬─► gold_text_annotations
        │                                              └─► gold_text_summaries
        └─(submit text-LLM)───► classif job ─(harvest)──► gold_content_classification
```

Pattern per enrichment asset (the ADR-0007 bridge over ONE shared ledger):

- **`submit`** — sub-second; workload executor = `qwen-vision` | `whisper` |
  `text-LLM`; records the job in the shared `external_jobs` ledger with the
  provider in metadata.
- **`harvest`** — poll to terminal → retrieve → idempotent upsert into the gold
  table(s), per-pass provenance, loud per-item failure (an item-level invalid
  result is surfaced and re-discoverable, never marked done, never silent).

Executors: **one seam, pluggable** — `qwen-vision`, `whisper`, `text-LLM`
(qwen default; Gemini batch a pluggable alternative — provider recorded in
metadata, decision per workload by quality/throughput benchmark). The two
orchestration triggers remain (CLI for the qwen path, Dagster sensor for the
gemini executor): **the seam is the lifecycle, not the scheduler.**

## 7. Provider / workload mapping

| Channel | Workload | Engine | Notes |
|---|---|---|---|
| visual | qwen-vision | qwen (standalone batch service, ADR-0009) | locked in practice; the table name stays provider-agnostic |
| audio | whisper | faster-whisper | local default; executor pluggable, rides the same submit/harvest lifecycle |
| text | text-LLM | qwen default; Gemini batch = pluggable alternative | a provider swap is a metadata change, never a rename |

## 8. Open decisions (flagged, not silently resolved)

1. **Form-taxonomy overlap** — `format` / `content_type` / `style`
   (`gold_content_classification`) vs `value_medium`
   (`gold_visual_annotations`). "Form" should live once; recommendation: drop
   `format` from classification. Pending decision.
2. **Parser enum violations are all-or-nothing** (yield loss, not false
   signal) — noted as a follow-up to relax recovery. Do NOT invent `_bound`
   flags to solve it (ADR-0010).

## 9. Replaces (history)

- `gold_analyses` → `gold_content_classification`.
- `gold_growth_facets` → split into `gold_visual_annotations` +
  `gold_visual_summaries` + `gold_text_annotations` + `gold_text_summaries`.
- `gold_transcripts` (proposed name, never built) → `gold_audio_transcripts`.
- Carousel overall-summary gap (v1 design) → `content_summary` is the overall
  visual summary for ALL forms.
