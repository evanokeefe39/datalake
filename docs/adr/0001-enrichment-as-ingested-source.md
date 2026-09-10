# ADR-0001: Enrichment output is an ingested source, not a transform (LLM/API boundary)

- Status: **Accepted**
- Decided: 2026-09-03 (build-vs-buy comparison resolved; ratified by human)

## Context

Instagram posts are enriched by an external, stochastic, paid LLM (Gemini). The
output (`gold_analyses`: domain/topic/admiralty/educational/actionable JSON) is
a durable, versioned dataset. Two pressures drove an architecture reassessment
(industry-pattern review + repo-specific review):

1. **Enrichment must not live in the transformation layer.** A transform is
   deterministic, idempotent, cheap to replay, and keyed by re-run. An LLM call
   is the opposite — nondeterministic, costly, rate-limited, and re-paying to
   recompute. The team's rule (echoed from the media-cache work) is *no external
   API calls in transform assets*.
2. **Scale drivers** — whole-corpus enrichment (all ~9,137 posts, incl. 5,956
   currently `skip`-gated), Gemini **batch API** (submit/poll/retrieve, ~50%
   cheaper), and **multimodal** media — raised the question of whether the
   architecture must change.

### Canon (industry review, 2026-09-03)

Databricks (medallion), Feast/Vertex/Databricks Feature Store, and Tecton
converge: **"enriched output = a source you ingest,"** not a transform computed
per-read. External LLM output is materialized once into a durable, versioned
dataset and re-ingested on backfill/version bump.

This maps the stack onto the offline/online feature-store skeleton:

| Role | Here |
|---|---|
| Offline feature/enrichment store (validated, versioned, time-series) | DuckDB `gold_analyses` |
| Online/latest-value snapshot + state | SQLite (`media_metadata`, latest-value state, watermarks) |
| Registry + versioning (avoid training/serving skew; stale-prompt) | `prompt_hash` / `check_prompt_currency` |
| Ingestion-side error/retry quarantine | `dead_letter` / `batch_items` / `scheduled_for` |
| External-dependency cache (raw media) | `media_cache` byte cache + File-API URI cache |

Four light upgrades the canon recommends (all small, none a platform build-out):
(a) data-version/model-version columns on every enriched batch; (b) keep offline
backfill distinct from the latest-value fast path; (c) a freshness SLO (time
since each entity was last enriched); (d) key output by input media + prompt/model
version so dedupe is stable.

### Fit to hot/cold, Lambda/Kappa

Data is genuinely cold; silver/gold stay **batch** (accuracy over latency).
The fit is **Lambda's "batch layer is truth, refresh the serving view on the
enriched-dataset-landing event"** — not Kappa (no log to replay). The worker is
the one async producer / non-hermetic subgraph where all LLM nondeterminism +
cost concentrate; Dagster stays deterministic. For batch API the seam is clean:
expose **submit → poll → retrieve** as the worker's contract; Dagster assets
call only those verbs and never hold batching/retry knowledge.

### Repo review (dlc-worker, tasks/findings/enrichment-architecture-assessment.md)

**No architecture change is required for any of the three drivers** — all fit the
existing seams:
- Whole-corpus = an admission-clause change in `ig_posts_gen_batches` (a
  `GoldConfig` flag or the existing `post_ids` bypass); the worker reads silver
  directly and `skip` posts need no special execution path.
- Gemini batch API = a second **worker execution mode** (interactive | gemini-batch)
  reusing the same ops.sqlite queue, retry, dead_letter, and materialization POST.
  Submission/polling stays in the worker, never in the DAG (else the blocking-asset
  anti-pattern returns).
- Multimodal = the existing scrape-time byte cache + File-API URI cache already
  implement media-as-versioned-input; remaining items are a retention policy and
  a historical byte-cache gap (pre-fix posts), not design.
- Only genuinely new artifact: a **prompt/version registry** resolving
  `prompt_hash` → its definition.

## Decision

(PROPOSED — pending ratification.) Keep the existing enrichment seam: gold is an
AssetSpec written solely by an external worker, decoupled from the transform DAG
by an async SQLite queue. **Do not refactor the transformation architecture.**
To support whole-corpus + batch + multimodal: add worker execution modes
(interactive | gemini-batch), a whole-corpus admission flag, and a prompt/version
registry. Do NOT build a full feature-platform (offline/online stores, registry
service, etc.) at this scale.

Build-vs-buy resolved (2026-09-03): feature stores (Feast/Tecton/Hopsworks) own
registry/versioning/freshness/offline-vs-online but NOT the durable
queue/retry/dead-letter this system hand-rolled; there is no first-party Dagster
integration (Feast/Tecton) — all integrate via AssetSpec/ExternalAsset, the same
seam already in use; dbt would model gold as a `source` (freshness SLAs) but the
LLM call must still run outside dbt and it re-hosts the read-side SQL in a second
tool. **Verdict: keep the hand-rolled SQLite queue + AssetSpec-worker seam.** A
free Dagster-native lift: attach a `FreshnessPolicy` to the gold `AssetSpec` to
formalize freshness (verify it consumes the worker-POSTed events + OSS
`freshness.enabled` caveat); plus the canon light-upgrades (version columns,
distinct offline backfill).

## Alternatives considered

1. **Full feature-platform build-out** (formal offline/online stores + registry):
   rejected — over-engineering for a single-writer local lakehouse (repo + industry
   review agree).
2. **Streaming/kappa rewrite, Dagster sensor for batch submission, media as a
   Dagster asset, queue migration off SQLite**: explicitly NOT needed.
3. **Move Gemini calls into a transform asset**: rejected — recreates the
   blocking-asset anti-pattern and violates the no-API-in-transform rule.

## Consequences

Positive: the no-API-in-transform boundary holds; whole-corpus + batch + multimodal
fit existing seams (worker-mode + discovery changes); no rewrites. Negative: the
pattern is hand-rolled — coordination primitives (retry/dead-letter/versioning/
freshness) are bespoke and must be maintained; a build-vs-buy alternative remains
under evaluation. Commits future work to: keeping gold single-writer; keeping the
worker the sole API/Gold boundary; adding version columns + freshness SLO as
volume grows.

## Supersedes / Superseded by

Supersedes (for this scope): none (new direction). Superseded by: none yet.

## Amendment (2026-09-09 — enrichment data-domain audit)

The ingested-source canon stands. What changed since this ADR: the worker seam
became Dagster-native batch (ADR-0007) and the media/vision backend moved to
the standalone qwen batch service (ADR-0009). Consequence on the ground: the
ingest pattern (local ledger → submit → poll-to-terminal → retrieve →
idempotent gold upsert with ordering guard) is now implemented TWICE with no
shared code — the Gemini batch path (`defs/enrichment/{gemini_batch,submit,
harvest}.py`) and the qwen facets path with its own `facets_batch_jobs` ledger
(`defs/enrichment/facets_batch.py:92-126`, carrying a `gemini_batch_name →
job_id` rename as fossil evidence of the clone).

Position (data-domain): converge the remote-async workloads (qwen-vision,
qwen-text, gemini-batch-text) on ONE shared ingest seam — generic ledger +
submit/poll/retrieve verbs + per-workload request-builder/parser/validator/
gold-writer plugins + shared crash semantics (placeholder-before-POST) and
loud per-item failure. STT is a distinct LOCAL resumable-job pattern with a
pluggable executor and stays outside that seam. Gemini batch either becomes an
executor behind the seam or a frozen exception that receives no new workloads;
the ~50% batch discount is ~$5–20-scale noise against the cost of maintaining
a second poll/harvest path. Additionally, canon items (a)/(d) are violated on
`gold_growth_facets` (one shared prompt_hash for two writer passes; `model`
conscripted as a done-marker) — per-pass provenance is required (E-ENRICH-FACETS
epic, audit P0-4). Full analysis: `data/dev/dlc-enrichment-audit.md`.
Decision ownership for the seam module and the classification executor choice
(the `gold_analyses` workload — final table name `gold_content_classification`,
ADR-0010):
E-ENRICH-ENGINE (ADR-0009).

## Amendment 2 (2026-09-09 — settled enrichment design v2: naming + provenance)

The final naming for every table this ADR references is set by
[ADR-0010](0010-enrichment-naming-and-provenance.md)
(`gold_<channel>_<artifact>`, channel ∈ `visual`/`text`/`audio`; provider
never in table names):

- `gold_analyses` (this ADR's 2026-09-03 subject table; the name above is
  kept as then-current history) → **`gold_content_classification`**.
- `gold_growth_facets` (named in Amendment 1) → replaced by the four-way
  split `gold_visual_annotations` / `gold_visual_summaries` /
  `gold_text_annotations` / `gold_text_summaries`.
- Amendment 1's per-pass provenance requirement (P0-4) is SETTLED as the
  structural table split: each pass owns its table and its full metadata set
  (`provider`, `model`, `prompt_hash`, `schema_version`, `input_modality`,
  `content_mime_type`, `sampling_params_json`, `run_id`, `analysed_at`) — the
  shared-`prompt_hash` defect and the model-as-done-marker hack die with the
  single-table store. No `_bound` columns: enum definitions live in the
  versioned schema registry, referenced by `schema_version`.
- The one-ingest-seam position is codified in ADR-0010: one shared
  `external_jobs` ledger, per-workload executor plugins (qwen-vision |
  whisper | text-LLM), one submit per pass with harvest fanning out to every
  table that pass produced. Amendment 1's "STT outside the seam" carve-out is
  superseded — whisper rides the same lifecycle as an executor plugin.
