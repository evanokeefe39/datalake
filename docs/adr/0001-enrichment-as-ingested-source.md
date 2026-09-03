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
