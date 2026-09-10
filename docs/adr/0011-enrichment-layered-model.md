# ADR-0011: Enrichment layered model — bronze landing → silver conform → gold marts

- Status: Accepted
- Decided: 2026-09-10
- Related: ADR-0001 (enrichment output is an ingested source), ADR-0003
  (hermetic transform layer), ADR-0007 (submit/harvest bridge), ADR-0008
  (hermetic transforms with one explicit API seam), ADR-0009 (qwen batch
  service), ADR-0010 (naming + per-pass provenance; superseded in its
  **naming** scope)

## Context

The settled enrichment design (ADR-0010) placed all six enrichment tables in
gold. That decision was internally inconsistent with the record it cites:

1. **The raw→conformed map is deterministic, so it belongs in silver.**
   ADR-0003 makes silver hermetic and a pure function of bronze. Once the
   verbatim provider response is landed (the "raw capture" pattern ADR-0010
   endorsed as an open item), parsing/validating/splitting that response into
   conformed columns is exactly the deterministic, replayable transform
   silver exists for — zero API calls, free to re-run.
2. **The raw capture itself is the missing bronze landing.** ADR-0001 names
   enrichment "an ingested source": ingest the verbatim response first. A
   dedicated bronze landing table for ALL external model responses makes
   remapping FREE — a schema/mapping change never re-calls the paid model.
3. **A hard naming bug.** The enrichment tables were keyed on `domain`, but
   `domain` was overloaded: the join key carried the PLATFORM
   (`'instagram'`), while `gold_content_classification` ALSO had a body
   column `domain` meaning the CONTENT NICHE (`'dev/AI'`) — a duplicate
   column name with two meanings. The repo's existing `profiles` table
   already keys on `platform`.
4. **Gold was shaped as per-channel mirrors, not marts.** Six
   `gold_<channel>_<artifact>` tables answer none of the owner's three
   questions directly; they force every consumer to re-join and re-aggregate.

## Decision

**Bronze — the landing zone for ALL external model responses (one general
pattern, not per-API):**

- `bronze_enrichment_raw` — verbatim response body, immutable, append-only,
  no transformation. Columns: `post_id`, `platform`, `workload`
  (`qwen-vision` | `whisper` | `text-LLM`), `provider`, `model`,
  `prompt_hash`, `schema_version`, `run_id`, `analysed_at`,
  `input_modality`, `sampling_params_json`, `response_text` (verbatim),
  `request_echo_json`. Idempotent on
  `(post_id, platform, workload, prompt_hash, run_id)`. Parquet.

**Silver — conform + validate (deterministic from bronze, ZERO API calls):**

Six tables — `silver_visual_annotations`, `silver_visual_summaries`,
`silver_audio_transcripts`, `silver_text_annotations`,
`silver_text_summaries`, `silver_content_classification`.

- Keys: `(post_id, platform)` — **the join key is `platform`, never
  `domain`**. `domain`/`subdomain`/`topic`/`subtopic` mean ONLY the content
  niche (what the owner calls "X domain"), resolving the duplicate-name bug.
- Body columns unchanged from the prior spec (the ADR-0010 six-table split
  stands; only the layer and key change).
- Envelope metadata on each: `provider`, `model`, `prompt_hash`,
  `schema_version`, `input_modality`, `content_mime_type`,
  `sampling_params_json`, `run_id`, `analysed_at` (`prompt_hash` NULL on
  transcripts).
- **Validation lives HERE**: parse-ability, required fields, enum conformance
  against the versioned schema registry (via `schema_version`), length
  bounds, cross-field checks (carousel n == len(image_summaries)),
  completeness. Unparseable/terminal results → quarantine/dead-letter
  LOUDLY — never a silent NULL, never a dropped row.

**Gold — analytic marts (NOT per-channel mirrors):**

1. `gold_post_enrichment` — the wide per-post shape: all channel outputs
   joined + engagement metrics + provenance. PK `(post_id, platform)`.
2. `gold_creator_performance` — Q1 ("who is performing well in X domain?").
   PK `(creator_id, platform)`. COMPOSES the canonical views
   (`v_creator_profile` for momentum_ratio/is_rising/avg_engagement_score/
   dominant_domain/total_posts; `v_post_follower_context` for follower_tier)
   as its upstream and adds ONLY the enrichment-derived content profile —
   it does NOT derive or restate the tier buckets or the momentum
   windows/gates (each has exactly one definition, in the canonical views).
   Columns: `follower_count`, `follower_tier`, `post_count`,
   `median_engagement_score`, `avg_engagement_score`, `standout_rate`,
   `momentum_ratio`, `is_rising`, `dominant_domain`.
3. `gold_content_shape_performance` — Q2 ("what is the shape of content that
   performs well in X domain / X topic / for X follower count?"). LONG table,
   PK `(domain, topic, follower_tier, facet_name, facet_value)`:
   `n_posts`, `avg_engagement_z`, `standout_rate`, `lift_vs_slice_baseline`.
   Groups the enrichment facets by `follower_tier` from
   `v_post_follower_context` and measures performance via `v_post_metrics`
   (engagement z / standout); no metric is re-derived and tier buckets are
   never restated. ("Shape" = the enrichment facets + metadata; long form
   survives facet-schema evolution.)

**Dependency direction (metrics-centralization):** the canonical metric
views are the SINGLE metric definition; the gold marts COMPOSE them together
with the silver enrichment outputs; only thin analytics projections derive
from the marts. The marts MUST NOT restate tier buckets or momentum
constants — they reference the canonical views.

## Why the raw→conformed map is silver (supersedes ADR-0010's layering)

- Silver is hermetic and deterministic (ADR-0003). With the verbatim response
  landed in bronze, the raw→conformed map is a **pure function of bronze** —
  same input, same output, free to re-run. That is silver's definition.
- The paid/stochastic part (the model call) is confined to ingestion and the
  external seam (ADR-0008) — it ends at the bronze landing. Re-running silver
  never re-calls the paid model; a mapping/schema change is a deterministic
  replay of silver, not a re-bill.
- Enrichment output is an ingested source (ADR-0001) → its **verbatim
  landing is bronze** (as-observed, immutable); its conformed form is silver;
  the analytics-ready joins/aggregates are gold. The old objection ("AI
  output in silver means re-running silver re-pays") was true only when the
  response was captured nowhere — with a bronze landing it is false by
  construction.
- Gold stops being a mirror of the passes and becomes the join/aggregate
  layer that answers the owner's three questions: creators by domain, content
  shape by domain/topic/tier, top posts across all domains.

## Alternatives considered

- **Keep six gold tables + a separate bronze-of-enrichment only** (the
  ADR-0010 open item half-adopted): rejected — leaves validation in the write
  path of paid gold tables and keeps gold as per-channel mirrors; consumers
  still hand-join.
- **Keep everything in gold including raw JSON** (status quo v2): rejected —
  no replay without re-billing, merged per-row provenance, no validation
  layer between the provider and consumers.
- **Key silver/gold on `domain` and rename the classification body column**
  (e.g. `niche`): rejected — `profiles.platform` is the established key and
  the owner's language ("X domain") maps to the niche; a second name for the
  niche would fork the vocabulary. Instead the key becomes `platform` and the
  niche keeps the `domain`/`subdomain`/`topic`/`subtopic` names, with the
  join key carrying no `domain` at all.
- **Quarantine as silent NULLs/dropped rows in silver**: rejected — a green
  pipeline must not be able to silently drop whole sources; terminal/
  unparseable results dead-letter loudly.

## Consequences

Positive: remap is free and deterministic (no re-billing on schema change);
validation is codified in silver with loud quarantine; the platform/domain
duplicate-name bug is resolved at the contract level; gold answers the three
owner questions directly; provenance survives end-to-end (bronze verbatim →
silver envelope → gold marts).

Negative: every existing reader of the planned `gold_<channel>_<artifact>`
names must rebind to `silver_*`; the ingest seam's harvest target moves from
gold to the bronze landing (submit stays per PASS; harvest lands verbatim in
`bronze_enrichment_raw`, and silver derives from it deterministically); one
more table in the medallion chain per workload.

## Supersedes / Superseded by

Supersedes ADR-0010's **naming** decision (`gold_<channel>_<artifact>` →
`silver_<channel>_<artifact>`, key `platform` not `domain`) and its layer
placement of the six tables in gold. ADR-0010's per-pass provenance
(structural table split), the no-`_bound`-columns rule, the versioned schema
registry, and the one-seam/one-submit-per-pass fan-out all STAND, re-homed
into this layer model. ADR-0010's history is preserved; its Status records
the supersession. The layer model in `docs/enrichment-design.md` is the
canonical spec.
