# Enrichment design v3 — layered model (bronze → silver → gold)

Settled 2026-09-10. The layer model and the platform/domain naming fix are
recorded in [ADR-0011](adr/0011-enrichment-layered-model.md), which supersedes
[ADR-0010](adr/0010-enrichment-naming-and-provenance.md) in its naming scope
(ADR-0010's per-pass provenance split, no-`_bound` rule, schema registry, and
one-seam/one-submit-per-pass decisions stand unchanged). This document is the
canonical enrichment spec. `docs/enrichment-enhancement-design.md` (v1,
2026-09-07) is retained as the rationale/experiment record — where the two
differ, THIS document wins.

## 0. The layer model

Three strict-medallion layers, one contract each:

| Layer | Job | Contract |
|---|---|---|
| **Bronze** | Land ALL external model responses verbatim | `bronze_enrichment_raw` — immutable, append-only, no transformation. Parquet. |
| **Silver** | Conform + validate — deterministic from bronze, ZERO API calls | Six `silver_*` tables; the raw→conformed map is a pure function of bronze. Validation (parse, required fields, enums via the schema registry, length bounds, cross-field, completeness) lives here; failures quarantine/dead-letter LOUDLY. |
| **Gold** | Analytic marts answering the owner's three questions | Four marts (`gold_post_enrichment`, `gold_creator_performance`, `gold_content_shape_performance`, `gold_top_posts`) + serving views. |

The owner's three questions — the whole point of the warehouse — shape gold:

1. **Who is performing well in X domain?** → `gold_creator_performance`
   (filter by domain).
2. **What is the "shape" of content that performs well in X domain / X topic
   / for X follower count?** → `gold_content_shape_performance`.
3. **What posts are doing well across all domains, and what is the shape of
   their content?** → `gold_top_posts` joined to `gold_post_enrichment`.

**Why the raw→conformed map is silver (the layering fix):** silver is
hermetic and deterministic (ADR-0003). Once the verbatim response is landed
in bronze, parsing/validating/splitting it is a pure function of bronze —
re-running silver never re-calls (or re-pays for) the model; a schema/mapping
change is a deterministic replay, not a re-bill. The paid/stochastic part ends
at the bronze landing; the old objection to enrichment in silver was true
only when the response was captured nowhere. Enrichment output is an ingested
source (ADR-0001): verbatim landing = bronze, conformed form = silver,
analytics = gold.

## 1. Product intent (per content form)

| Content form | Silver outputs |
|---|---|
| **Reels/video** (audio present) | visual annotations (`silver_visual_annotations`), text annotations (`silver_text_annotations`), transcript (`silver_audio_transcripts`), visual summary (`silver_visual_summaries.content_summary`), transcript summary (`silver_text_summaries.transcript_summary`) |
| **Image carousel / single** (no audio) | visual annotations (`silver_visual_annotations`), text annotations (`silver_text_annotations`), visual summaries — per-image (`image_summaries_json`) + overall (`content_summary`) |

Transcript outputs are N/A for image/carousel (no audio) — but the absence is
**explicit**: such posts get a `silver_audio_transcripts` row with
`transcript_status = no_audio_source`, never a silent NULL and never a
missing row. The same status enum separates `pending` / `done` /
`empty_audio`.

## 2. Naming rules (final)

- Layer-first: `bronze_` | `silver_` | `gold_`. Enrichment conformance tables
  are `silver_<channel>_<artifact>` — `silver_visual_annotations`,
  `silver_visual_summaries`, `silver_audio_transcripts`,
  `silver_text_annotations`, `silver_text_summaries`. Channel token:
  `visual` | `text` | `audio` — **NOT "video"** (the visual pass covers
  images AND video frames). The one cross-channel taxonomy artifact is
  content-scoped, not channel-scoped: `silver_content_classification`.
- **The join key is `platform`, never `domain`.** `platform` matches the
  repo's existing `profiles.platform`. `domain`/`subdomain`/`topic`/
  `subtopic` mean ONLY the content niche (what the owner calls "X domain") —
  they are body columns of the classification table, never join keys. This
  resolves the duplicate-column-name bug where the key `domain` carried the
  PLATFORM while the classification body column `domain` carried the niche.
- **Provider is NOT in the table name.** Vision is locked to qwen and audio
  to whisper in practice, but names stay provider-agnostic so a provider swap
  never renames a table. Provider/model live in metadata columns (§5).
- **No `_bound` columns.** Enum/bounded-field definitions live in the
  versioned schema registry, referenced by `schema_version` (the locked V3
  facet schema, `GROWTH_FACETS_SCHEMA_VERSION = "3"`, is the first entry of
  that pattern). No per-row conformance flags.
- Body column names match the table's schema keys 1:1 (JSON↔column mapping
  is trivial).

## 3. Bronze — `bronze_enrichment_raw`

The landing zone for ALL external model responses (one general pattern, not
per-API). Verbatim response body, immutable, append-only, no transformation.

- Columns: `post_id`, `platform`, `workload` (`qwen-vision` | `whisper` |
  `text-LLM`), `provider`, `model`, `prompt_hash`, `schema_version`,
  `run_id`, `analysed_at`, `input_modality`, `sampling_params_json`,
  `response_text` (verbatim), `request_echo_json`.
- Idempotency key: `(post_id, platform, workload, prompt_hash, run_id)`.
- Parquet.

Payoff: a schema/mapping change re-derives silver WITHOUT re-calling the
paid model; the model's actual output stays auditable; true WAP is possible
(write raw → audit → publish conformed). This retires the half-version of
parsed JSON inside gold (`gold_analyses.result_json` /
`gold_growth_facets.growth_facets_json`).

## 4. Silver — conform + validate

Six tables, deterministic from `bronze_enrichment_raw` with ZERO API calls.

| Table | Artifact | Workload (source pass) | Inputs |
|---|---|---|---|
| `silver_visual_annotations` | visual facets | qwen vision | images / video frames |
| `silver_visual_summaries` | visual summaries (overall + per-image) | qwen vision | images / video frames |
| `silver_audio_transcripts` | raw STT transcript | whisper | audio (reels) |
| `silver_text_annotations` | text-layer facets | text LLM (provider swappable; qwen default) | caption + transcript |
| `silver_text_summaries` | transcript summary | text LLM | transcript |
| `silver_content_classification` | taxonomy + educational/actionable + admiralty | text LLM | caption + transcript + visual summaries |

Deterministic post/creator/comment metrics are intentionally NOT a new gold
layer — the serving views (`v_post_metrics`, `v_creator_metrics`,
`v_profile_metrics`) already own them.

### Keys + metadata (all silver enrichment tables)

- **Keys:** `(post_id, platform)`.
- **Metadata (provenance, every table):** `provider`, `model`, `prompt_hash`
  (NULL on `silver_audio_transcripts` — ASR has no prompt),
  `schema_version`, `input_modality`, `content_mime_type`,
  `sampling_params_json`, `run_id`, `analysed_at`.

Per-pass provenance is structural: each pass owns its table, so its
`prompt_hash`/`model`/`run_id` can never be overwritten by another pass (the
defect that killed the single-table facet store — see §10).

### Body columns

| Table | Body columns |
|---|---|
| `silver_visual_annotations` | `face_present`, `value_medium`, `brand_logos`, `text_overlay_present`, `on_screen_claim` |
| `silver_visual_summaries` | `content_summary` (**overall** visual summary for ALL forms — video frames / single image / carousel overall; this resolves the carousel overall-summary gap), `image_summaries_json` (per-image, carousel only) |
| `silver_audio_transcripts` | `transcript`, `transcript_status` (`no_audio_source` \| `pending` \| `done` \| `empty_audio`), `audio_present`, `asr_model`, `language` |
| `silver_text_annotations` | `hook_content`, `hook_type`, `is_sponsored`, `sponsorship_signal`, `claimed_results`, `cta_type`, `audience_named`, `value_depth`, `replicable_tactic`, `hashtag_strategy`, `evidence`, `brand_safety_json` |
| `silver_text_summaries` | `transcript_summary` |
| `silver_content_classification` | `domain`, `subdomain`, `topic`, `subtopic`, `is_educational`, `is_actionable`, `admiralty`, `content_type`, `style`, `format` |

Facet field semantics (enums, codebook wording, agreement grades) are locked
in `docs/growth-facets-schema.md` (V3): cross-modal fields →
`silver_text_annotations`; visual-necessary fields →
`silver_visual_annotations`. The facet schema itself stays channel-blind;
`evidence` records which channels grounded each judgment — the table split
is by producing pass, not by claim that a field is single-channel.

**Validation lives in silver** (deterministic, zero API calls):

- parse-ability of the bronze `response_text` against the pass's schema;
- required fields present;
- enum conformance against the versioned schema registry (via
  `schema_version`);
- length bounds;
- cross-field checks (e.g. carousel n == len(`image_summaries`));
- completeness.

Unparseable or terminal-invalid results are quarantined/dead-lettered
LOUDLY — never a silent NULL, never a dropped row.

**Overlap flag (open decision):** `format` / `content_type` / `style` on
`silver_content_classification` overlap
`silver_visual_annotations.value_medium`. "Form" should live once — the
recommendation is to drop `format` from classification. **OPEN** — do not
resolve silently (§9).

## 5. Gold — analytic marts

Gold is NOT a per-channel mirror of the passes. It joins the silver channel
outputs into marts that answer the owner's three questions.

**Dependency direction (metrics-centralization):** the canonical metric
views (`v_creator_profile`, `v_post_follower_context`, `v_post_metrics`,
`v_creator_metrics`) are the SINGLE definition of every metric. The gold
marts COMPOSE those views together with the silver enrichment outputs; they
never restate tier buckets, momentum constants/windows, or engagement
baselines. Only thin analytics projections derive from the marts — the marts
are NOT upstream of the metric views.

### 5.1 `gold_post_enrichment` — the wide per-post shape

All channel outputs joined + engagement metrics + provenance. PK
`(post_id, platform)`.

### 5.2 `gold_creator_performance` — Q1: who is performing well in X domain?

PK `(creator_id, platform)`. This mart COMPOSES the canonical views —
`v_creator_profile` (momentum_ratio, is_rising, avg_engagement_score,
dominant_domain, total_posts) and `v_post_follower_context`
(follower_tier, strictly at-post-time) — as its upstream, and adds ONLY the
enrichment-derived content profile (domain slicing over the silver
classification). It does NOT derive or restate `follower_tier`,
`momentum_ratio`, `is_rising`, `avg_engagement_score`, or `dominant_domain`:
the momentum windows/gates (28d/84d, ≥3 posts, ≥1.25, ≥5.0) and the tier
buckets each have exactly one definition, in the canonical views (WATCHDOG
metrics-centralization). Columns: `follower_count`, `follower_tier`,
`post_count`, `median_engagement_score`, `avg_engagement_score`,
`standout_rate`, `momentum_ratio`, `is_rising`, `dominant_domain`.

### 5.3 `gold_content_shape_performance` — Q2: what shape of content performs?

LONG table, PK `(domain, topic, follower_tier, facet_name, facet_value)`:
`n_posts`, `avg_engagement_z`, `standout_rate`, `lift_vs_slice_baseline`.
"Shape" = the enrichment facets + metadata; the long form survives
facet-schema evolution (a new facet is new rows, not a migration). It groups
the enrichment facets by `follower_tier` taken from `v_post_follower_context`
and measures performance via `v_post_metrics` (engagement z / standout) —
both canonical views, referenced as upstream; NO metric is re-derived here
and the tier buckets are never restated.

### 5.4 `gold_top_posts` — Q3: what posts are doing well across all domains?

PK `(post_id, platform)`: rank/percentile across all domains, joined to the
full shape + summary/transcript for qualitative reading.

## 6. The seam, end to end

**The seam is not infrastructure.** It is the BOUNDARY where our pipeline
hands work to an outside system and takes results back — an interface, not a
component. Nothing runs "in" the seam. Its contract is realized by exactly two
things: one shared `external_jobs` ledger (a table BOTH sides read and write)
and three verbs — `submit`, `poll-to-terminal`, `retrieve`. Those three are the
seam verbs, and they are the whole contract. `harvest` is Dagster's own step,
not a seam verb: it composes `poll-to-terminal` + `retrieve` + the idempotent
verbatim landing.

What sits on each side of that boundary:

- **Dagster (our orchestrator, INSIDE the boundary):** discovery, batching,
  `submit` (sub-second), `harvest` (idempotent bronze landing). It owns
  orchestration, lineage, quality. **It never calls a provider directly** —
  the ledger is the only thing that crosses the boundary.
- **External infra (OUTSIDE the boundary; separate processes/containers, own
  durable state):** `qwen-batch-service` (FastAPI + its own SQLite job store;
  drains jobs against OpenRouter/qwen; server-side ffmpeg frame-sampling for
  reels) and `whisper` (faster-whisper ASR, the service's `whisper_local`
  executor).
- **Providers (outside our infra entirely):** OpenRouter → qwen (vision +
  text). ASR is local (no vendor).

**Separation of concerns:** Dagster owns orchestration/lineage/quality; the
service owns durability/retry/backoff/dead-letter/media handling; the ledger
is the contract between them. Harvest lands verbatim in
`bronze_enrichment_raw`; silver derives from bronze deterministically (zero
API calls).

## 7. Dependency DAG

Within-silver derivation is deterministic; the paid passes feed bronze:

```
media ─┬─► (submit qwen-vision) ─► bronze_enrichment_raw ─► silver_visual_annotations
       │                                                 └─► silver_visual_summaries
       └─► (submit whisper) ─► bronze_enrichment_raw ─► silver_audio_transcripts

caption + bronze transcript ─► (submit text-LLM) ─► bronze_enrichment_raw ─► silver_text_annotations
                                                            └─► silver_text_summaries
caption + transcript + visual summaries ─► (submit text-LLM) ─► bronze_enrichment_raw
                                                            └─► silver_content_classification

silver_ig_posts + silver_* ─► v_post_detail            (the serving base)
v_post_detail ─► 21 canonical metric views + dims      (own every metric definition)
canonical views ─► gold_creator_performance            (composes them)
canonical views + silver_* ─► gold_content_shape_performance
canonical views + silver_* ─► gold_post_enrichment ─► gold_top_posts
```

Dependencies are **declared per asset**. Text passes wait on
`silver_audio_transcripts`; classification waits on
`silver_audio_transcripts` + `silver_visual_summaries`; all gold marts wait
on the silver tables they join. Re-running an upstream invalidates
downstream — the staged/derived deps are declared, not emergent.

> **Possible future enhancement — classification grounding (NOT a current
> dependency).** Classification today reads the caption + transcript +
> `silver_visual_summaries`. It could additionally read
> `silver_text_summaries.transcript_summary` (the "what is SAID"
> condensation), and/or the `silver_text_annotations` facets, to help ground
> the labelling. Deliberately deferred: the raw transcript is already an
> input, so the summary is largely redundant today; revisit if classification
> quality (or transcript-reading cost) warrants it.

**Layer order (enforced):** `bronze_enrichment_raw` → `silver_*` →
`v_post_detail` → canonical metric views (21) → gold marts (4). The canonical
views are UPSTREAM of the marts and are never re-pointed at them — no cycle.

## 8. Asset graph — one async seam, one submit per pass

**CRITICAL cost/payload correctness:** the visual call returns annotations
AND summaries in ONE request; the text call returns text annotations AND
transcript summary in ONE request. The graph therefore has **ONE visual
submit fanning out to both visual tables, and ONE text submit fanning out to
both text tables** — NOT one submit per table (that would double the bill).
`silver_content_classification` is its own submit (it additionally consumes
visual summaries/transcripts).

```
silver ─┬─(submit qwen-vision)─► visual job ─(harvest)─┬─► bronze_enrichment_raw ─(silver)─┬─► silver_visual_annotations
        │                                              │                                    └─► silver_visual_summaries
        ├─(submit whisper)────► audio job  ─(harvest)───► bronze_enrichment_raw ─(silver)────► silver_audio_transcripts
        │
        │   after transcripts (+ summaries):
        ├─(submit text-LLM)───► text job   ─(harvest)─┬─► bronze_enrichment_raw ─(silver)─┬─► silver_text_annotations
        │                                              │                                   └─► silver_text_summaries
        └─(submit text-LLM)───► classif job ─(harvest)──► bronze_enrichment_raw ─(silver)────► silver_content_classification
```

Pattern per enrichment asset (the ADR-0007 bridge over ONE shared ledger):

- **`submit`** (a seam verb, and Dagster's step that calls it) — sub-second;
  workload executor = `qwen-vision` | `whisper` | `text-LLM`; records the job
  in the shared `external_jobs` ledger with the provider in metadata.
- **`harvest`** (Dagster's step; composes the seam verbs `poll-to-terminal` and
  `retrieve`) — poll to terminal → retrieve → idempotent verbatim landing
  into `bronze_enrichment_raw`, per-pass provenance, loud per-item failure
  (an item-level invalid result is surfaced and re-discoverable, never marked
  done, never silent). Silver conform/validate runs downstream of the landing,
  deterministically.

Executors: **one seam, pluggable** — `qwen-vision`, `whisper`, `text-LLM`
(qwen default; Gemini batch a pluggable alternative — provider recorded in
metadata, decision per workload by quality/throughput benchmark). The two
orchestration triggers remain (CLI for the qwen path, Dagster sensor for the
gemini executor): **the lifecycle is the same, whatever the scheduler.**

## Preserved serving surface (no-regression)

The re-layer is ADDITIVE to the serving layer; it replaces nothing.
`v_post_detail` is the serving base — silver posts joined with the silver
enrichment tables and the dims. The 21 canonical metric views + dims are built
on it and are the SINGLE definition of every metric. The gold marts sit
downstream and compose them.

**Must not regress** — every surface below keeps its canonical definition,
stays built on `v_post_detail`, and is never dropped, renamed, or restated:

| Surface | Serves |
|---|---|
| `v_post_detail` | per-post detail (the serving base) |
| `v_recent_hot_posts` | hot posts (recent 28-day) |
| `v_outlier_posts`, `v_engagement_outliers`, `v_creator_outlier_rate` | standout / outlier posts |
| `v_underperformer_posts`, `v_creator_underperformer_rate` | underperformers |
| `v_rising_creators`, `v_creator_profile` | rising creators + momentum |
| `v_post_metrics`, `v_post_baselines` | per-post metrics + point-in-time baselines |
| `v_creator_metrics`, `v_creator_quality`, `v_creator_topics` | creator detail |
| `v_post_follower_context` | follower tier at post time |
| `v_signal`, `v_quality_trend`, `v_domain_coverage`, `v_profile_metrics`, `v_overview`, `v_standout_calendar` | signal / trend / coverage / overview |
| `dim_profile`, `dim_date` | dims |

**No cycle.** The canonical views are upstream of the gold marts and are never
re-pointed at them. A projection built on a mart is a NEW, additive surface,
not a redefinition of any view above.

**Authoritative list + enforcement.** The canonical list is `DUCKDB_VIEWS`
(`src/datalake/defs/common/schemas.py`). It is not a prose promise:
`tests/operational/test_state_compatibility.py` already parametrizes over
`EXPECTED_DUCKDB_VIEWS` and asserts every view against the live DB — so if the
migration drops or renames any view, that test fails. The no-regression
requirement is therefore a checkable invariant at code-migration time.

## 9. Open decisions (flagged, not silently resolved)

1. **Form-taxonomy overlap** — `format` / `content_type` / `style`
   (`silver_content_classification`) vs `value_medium`
   (`silver_visual_annotations`). "Form" should live once; recommendation:
   drop `format` from classification. Pending decision.
2. **Parser enum violations are all-or-nothing** (yield loss, not false
   signal) — noted as a follow-up to relax recovery. Do NOT invent `_bound`
   flags to solve it (ADR-0010). Silver quarantine/dead-letter must make the
   loss visible, never silent.
3. **`asr_model` vs the envelope `model`** on `silver_audio_transcripts` —
   the ASR model is nameable both as a body column (`asr_model`) and in the
   shared metadata (`model`). Candidate redundancy; recommendation: keep the
   envelope `model` and drop `asr_model`. Pending — do not resolve silently.
4. ~~Raw provider-response landing + deterministic remap~~ — **RESOLVED by
   ADR-0011**: the verbatim response lands in `bronze_enrichment_raw`
   (immutable, keyed `(post_id, platform, workload, prompt_hash, run_id)`),
   and the deterministic conform/validate/split into the six `silver_*`
   tables is silver. Payoff realized: mapping/schema changes re-derive silver
   without re-calling the paid model; the model's actual output stays
   auditable; true WAP (write raw → audit → publish conformed).

## 10. Replaces (history)

- `gold_analyses` → `silver_content_classification`.
- `gold_growth_facets` → split into `silver_visual_annotations` +
  `silver_visual_summaries` + `silver_text_annotations` +
  `silver_text_summaries`.
- `gold_transcripts` (proposed name, never built) →
  `silver_audio_transcripts`.
- Carousel overall-summary gap (v1 design) → `content_summary` is the overall
  visual summary for ALL forms.
- v2's six `gold_<channel>_<artifact>` tables (ADR-0010) → re-layered:
  verbatim landing in `bronze_enrichment_raw`, conformance in
  `silver_<channel>_<artifact>` (key `platform`), marts in the four
  `gold_*` tables (ADR-0011).
