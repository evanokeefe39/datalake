# ADR-0010: Enrichment naming (`gold_<channel>_<artifact>`) and per-pass provenance

- Status: Accepted
- Decided: 2026-09-09 (settled enrichment design v2 contract,
  `data/dev/enrichment-final-design.md`)
- Related: ADR-0001 (enrichment output is an ingested source), ADR-0007
  (submit/harvest bridge), ADR-0009 (qwen batch service, executor seam)

## Context

Enrichment output in gold accumulated three naming/provenance problems while
the design evolved through the Gemini batch era (ADR-0007) into the qwen
service era (ADR-0009):

1. **Engine-era names.** The classification store was `gold_analyses` (built
   on the Gemini batch path); the facet store was `gold_growth_facets` (built
   on the qwen path); a transcript store had been drafted as
   `gold_transcripts`. None of the names carry a channel token, and the
   facet/table names describe *when the engine was wired*, not *what the
   artifact is*.
2. **Two passes share one row and one `prompt_hash`.** On
   `gold_growth_facets`, the visual and text passes MERGE into the same
   `(post_id, domain)` row and "the text pass overwrites prompt_hash on the
   same row" (`defs/enrichment/facets_batch.py:250-256`). The row's hash no
   longer identifies the prompt that produced the visual fields — a direct
   violation of ADR-0001's canon items (a)/(d) — and `model` was conscripted
   as a done-marker to compensate.
3. **Provider-coupled naming is a live risk.** The Gemini→qwen pivot
   (ADR-0009) happened weeks after the Gemini batch decision. Any provider
   name baked into a table name would have forced a rename (and every
   downstream consumer rebind) at each pivot.
4. **The `_bound` temptation.** A proposed fix for enum-violation handling was
   per-row `_bound` conformance flags — control-flow data smuggled into the
   schema, duplicating what a versioned schema definition already owns.

## Decision

1. **Name enrichment tables `gold_<channel>_<artifact>`**, channel token ∈
   `visual` | `text` | `audio` (never "video" — the visual pass covers images
   AND video frames). Final set: `gold_visual_annotations`,
   `gold_visual_summaries`, `gold_text_annotations`, `gold_text_summaries`,
   `gold_audio_transcripts`. The one cross-channel artifact (taxonomy +
   educational/actionable/admiralty, consuming caption + transcript + visual
   summaries) is content-scoped: `gold_content_classification`.
2. **Provider is NOT in table names.** It lives in the metadata columns
   (`provider`, `model`), so a provider swap (qwen ⇄ Gemini for the text
   workload) is a data change, never a rename/rebind.
3. **No `_bound` columns.** Enum/bounded-field definitions live in the
   versioned schema registry, referenced by each row's `schema_version`
   (first entry: the locked V3 facet schema, `GROWTH_FACETS_SCHEMA_VERSION =
   "3"`). Parser handling of enum violations stays all-or-nothing (yield
   loss, not false signal); relaxing it is a follow-up that must not invent
   per-row flags.
4. **One metadata set on every enrichment table** — keys `post_id`, `domain`;
   metadata `provider`, `model`, `prompt_hash`, `schema_version`,
   `input_modality`, `content_mime_type`, `sampling_params_json`, `run_id`,
   `analysed_at`. Per-pass provenance is achieved **structurally, by the
   table split** (one pass = one table = its own full metadata row), not by
   per-pass hash columns on a shared row. This retires the
   model-as-done-marker hack.
5. **One shared async ingest seam.** A single workload-keyed `external_jobs`
   ledger with placeholder-before-POST submit; `submit → poll-to-terminal →
   retrieve → idempotent upsert` verbs; per-workload executor plugins
   (`qwen-vision` | `whisper` | `text-LLM`; Gemini batch remains a pluggable
   executor for text); loud per-item failure semantics shared across
   workloads. **One submit per PASS, harvest fans out to every table the pass
   produced**: visual submit → `gold_visual_annotations` +
   `gold_visual_summaries`; text submit → `gold_text_annotations` +
   `gold_text_summaries`; audio submit → `gold_audio_transcripts`;
   classification its own submit → `gold_content_classification`. One submit
   per table would double the bill for passes whose call returns multiple
   artifacts. The two orchestration triggers remain (CLI for the qwen path,
   Dagster sensor for the gemini executor) — the seam is the lifecycle, not
   the scheduler.

The full table/column/DAG/asset-graph spec built on these decisions lives in
`docs/enrichment-design.md`.

## Alternatives considered

- **Provider in the name** (`gold_qwen_visual_annotations`, …): rejected — the
  ADR-0009 pivot proves providers change; a rename would churn every consumer,
  view, and dashboard binding.
- **Per-pass hash columns on one shared facet row**
  (`visual_prompt_hash`/`text_prompt_hash` on `gold_growth_facets`): rejected —
  keeps two writers on one row, keeps the merge/coalesce machinery, and leaves
  staleness detection per pass fiddly. The table split makes provenance
  impossible to overwrite by construction.
- **`_bound` per-row conformance flags**: rejected — duplicates the versioned
  schema registry's job, adds control-flow columns to analytical tables, and
  repeats the smell of `model` conscripted as a done-marker.
- **STT as a separate local-job pattern outside the ingest seam** (the
  2026-09-09 data-domain audit's carve-out): superseded — the settled v2 asset
  graph routes whisper through the same submit/harvest lifecycle as an
  executor plugin (ADR-0009 hosts `whisper_local` on the service's executor
  registry).
- **One submit per table**: rejected on cost — the visual call returns
  annotations AND summaries in one request, the text call returns text
  annotations AND transcript summary in one request; separate submits would
  re-pay the input (double the bill) for zero extra signal.

## Consequences

Positive: names describe artifact + channel and survive engine and provider
pivots; per-pass provenance is structural (each row attributable to exactly one
prompt/model/run); the enum-definition burden sits in one versioned registry;
one ingest seam to harden instead of a fork per workload; the fan-out shape
keeps the multi-artifact passes at one bill.

Negative: every existing reader of `gold_analyses` / `gold_growth_facets`
(views, dashboards, docs, tests) must be rebound to the new names; the four-way
facet split multiplies table count (accepted — provenance clarity beats table
count at this scale); the schema registry must actually be maintained
(`schema_version` referenced but with no registry entry is a dangling
guarantee).

## Supersedes / Superseded by

Supersedes (naming + provenance scope only; the underlying architecture
decisions of ADR-0001/0007/0009 stand):

- `gold_analyses` → `gold_content_classification`.
- `gold_growth_facets` → split into `gold_visual_annotations` +
  `gold_visual_summaries` + `gold_text_annotations` + `gold_text_summaries`.
- `gold_transcripts` (proposed, never built) → `gold_audio_transcripts`.
- The audit-era "per-pass hash columns" resolution of P0-4 → the structural
  table split.

Superseded by: none yet.
