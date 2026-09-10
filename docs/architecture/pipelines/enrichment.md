# Enrichment design v3 — layered model (bronze → silver → gold)

Settled 2026-09-10. The layer model and the platform/domain naming fix are
recorded in [ADR-0011](../adr/0011-enrichment-layered-model.md), which supersedes
[ADR-0010](../adr/0010-enrichment-naming-and-provenance.md) in its naming scope
(ADR-0010's per-pass provenance split, no-`_bound` rule, schema registry, and
one-seam/one-submit-per-pass decisions stand unchanged). This document is the
canonical enrichment spec. `docs/architecture/enrichment-design-v1-superseded.md` (v1,
2026-09-07) is retained as the rationale/experiment record — where the two
differ, THIS document wins.

### Version history — the authoritative numbering

The terms "v2" and "v3" name **different generations**. Use them precisely;
when a doc says "v2" it is describing a superseded layer, not the target.

| Term | Date | Recorded in | What it settled |
|---|---|---|---|
| **v1** | 2026-09-07 | `docs/architecture/enrichment-design-v1-superseded.md` | the facets / transcripts / summaries design (rationale + experiment record; superseded) |
| **v2** | 2026-09-09 | [ADR-0010](../adr/0010-enrichment-naming-and-provenance.md) | naming + per-pass provenance + one-seam/one-submit-per-pass; **six `gold_<channel>_<artifact>` tables** |
| **v3 — THE TARGET** | 2026-09-10 | [ADR-0011](../adr/0011-enrichment-layered-model.md) | the **layered model**: bronze `bronze_enrichment_raw` → six `silver_*` tables → four gold marts; join key `platform`, never `domain` |

**So: this document (v3) is the target; "v2" is the superseded ADR-0010
generation.** The v3 design keeps v2's per-pass provenance split, its
no-`_bound`-column rule, its versioned schema registry, and its
one-submit-per-pass fan-out — what changed is the **layer** (gold → silver) and
the **naming/key** (`gold_<channel>_<artifact>` on `domain` →
`silver_<channel>_<artifact>` on `platform`).

A doc that says "v2 layered model" or "the v2 target" is mislabelled: the
layered model is v3. `docs/education/enrichment-v2-onboarding.html` keeps "v2"
in its *filename* for link stability but teaches the v3 model — its title and
kicker say so.

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
in `docs/architecture/growth-facets-schema.md` (V3): cross-modal fields →
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

## 6. The seam

**Owned by [`inference-service.md`](../services/inference.md)** — the ledger, the
three verbs, the adapter swap, and the service contract. Not restated here: the
seam is the boundary *around* this layer model, not part of it, and a second
copy would only drift from the first.

The one fact this document needs from it: `harvest` lands provider responses
verbatim in `bronze_enrichment_raw` (§3), and everything in §4 is derived from
that deterministically — zero API calls, so a mapping change is a replay and
never a re-bill.

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

## 8. Asset graph — which pass produces which table

The **rule** (one submit per pass, never one per table, and why) is owned by
[`inference-service.md`](../services/inference.md) §4. What follows is the
enrichment-specific mapping only: which table each pass lands in.

The visual call returns annotations AND summaries in one request; the text call
returns text annotations AND a summary in one. `silver_content_classification`
is its own pass (it additionally consumes visual summaries/transcripts).

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


## Current state — the batch queue (retired by ADR-0012)

This is what runs today and what ADR-0012 replaces. Moved here from the
system-wide design doc: the queue is this pipeline's intake, not a property of
the whole lakehouse.

Operational state lives in `ops.sqlite`:

```sql
CREATE TABLE batch_jobs (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    status     TEXT NOT NULL DEFAULT 'pending',  -- pending | processing | complete
    domain     TEXT NOT NULL DEFAULT 'instagram',
    created_at TEXT NOT NULL,
    started_at TEXT,
    completed_at TEXT
);

CREATE TABLE batch_items (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id       INTEGER NOT NULL REFERENCES batch_jobs(id),
    post_id      TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'pending',  -- pending | processing | complete | failed | dead
    attempts     INTEGER NOT NULL DEFAULT 0,
    last_error   TEXT,
    scheduled_for TEXT,
    completed_at TEXT
);
```

**Lifecycle.** (1) `ig_posts_gen_batches` creates a batch via `create_batch()` and
inserts post IDs as items — see [`core.md`](core.md) Stage 4 for the drain's own
guards. (2) The submit job calls `claim_batch()` to claim the oldest pending
batch. (3) Submit/harvest call `claim_pending_items()` for up to 5 items at a
time. (4) Per item: `complete_item()` on success, `fail_item()` on failure —
retry with exponential backoff, `MAX_ATTEMPTS=5`. Terminal failures route to
`dead_letter`. (5) The harvest job calls `mark_complete()` once all items are
done or dead.

Until ADR-0007 these steps were driven by a standalone enrichment worker; that
process is removed and the roles are Dagster's now.

**`dead_letter`** — terminal failures, after retries are exhausted:

```sql
CREATE TABLE dead_letter (
    post_id   TEXT NOT NULL,
    domain    TEXT NOT NULL DEFAULT 'instagram',
    error     TEXT,
    attempts  INTEGER NOT NULL DEFAULT 0,
    failed_at TEXT NOT NULL,
    PRIMARY KEY (post_id, domain)
);
```

Manual triage only; there is no automatic retry worker. This keeps
`gold_analyses` pure — completed enrichments only, never partial failures. Note
the `domain` column here means *platform* (`'instagram'`), the same overload
ADR-0011 corrects by keying on `platform` instead.

**All of this is what ADR-0012 retires** — see the section below for each table's
disposition.

## Orchestration (ADR-0012)

Orchestration state is **Dagster-native**
([ADR-0012](../adr/0012-dagster-native-orchestration.md), ratified 2026-09-10 —
accepted, NOT YET IMPLEMENTED): orchestration state lives in the Dagster
instance + the lake, not a hand-rolled queue. The `ops.sqlite` enrichment queue
retires: `batch_jobs`, `batch_items`, `dead_letter`, and
`facets_batch_jobs` DROP. `ops.sqlite` RETAINS its operational tables —
`media_cache`, `media_metadata`, `creators`, `profiles`, `creator_merges`,
`prompt_registry`. ADR-0012 supersedes only the queue + dead_letter scope of
ADR-0004; ADR-0004's ops/analytical split (SQLite operational, DuckDB
analytical) otherwise stands.

What the swap buys:

- **Retry is a NEW partition key**: re-materializing a harvested partition
  is invisible to `submitted ∖ harvested`; retry round N targets posts that
  failed exactly N times.
- Failures surface via the anti-join `landed(bronze) ∖ conformed(silver)`
  plus a BLOCKING asset check.
- The accounting identity: `done + failed + in_flight + backlog ==
  candidates`.
- **The discovery drain's in-flight guard moves to the Dagster instance**:
  `ig_posts_gen_batches` derives in-flight state from the instance, not
  `batch_items` — otherwise the drain double-submits.

Until implemented, the `ops.sqlite` queue is live and its only poller
(`gemini_batch_harvest_sensor`) ships STOPPED — enabling it is a manual
deploy step. Evidence: `~/repos/enrichment-spike` (S1–S6 + a live gate) and
`FINDINGS.md`; plan:
`tasks/plans/dagster-native-orchestration-implementation.md`.

Pattern per enrichment asset (the ADR-0007 bridge):

- **`submit`** (a seam verb, and Dagster's step that calls it) — sub-second;
  workload executor = `qwen-vision` | `whisper` | `text-LLM`; the provider is
  recorded in metadata, never in a table name. **No ledger row is written** —
  the service owns its own job store and Dagster polls it
  ([ADR-0013](../adr/0013-seam-keeps-no-ledger.md)).
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
