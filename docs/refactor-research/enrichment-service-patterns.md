# Research: What should an enrichment service look like in Dagster?

Status: Iteration 0 — live research note (feeds `tasks/plans/refactor-architecture-investigation.md`)
Branch: `chore/refactor-investigation`
Date: 2026-09-06

> Scope: typical design patterns for **enrichment + external API calls** in a
> Dagster pipeline, Dagster-specific cases, and the specific antipatterns in this
> space. Grounded in the `dagster-expert` skill (vendor) + Dagster's canonical
> model + our ADR history. External web search was **unavailable this session**
> (all providers down/CAPTCHA/API errors); `[VERIFY]` marks claims to re-check via
> web when search recovers.

## 1. Framing: what "enrichment service" means here

Enrichment takes already-ingested candidate rows (facts / media) and adds derived
signal by calling an **external, stochastic, rate-limited, paid** API (Gemini:
`generate_content`, File-API upload, async batch jobs). Its shape in Dagster is not
a single artifact — it decomposes into five independent architectural decisions:

| # | Decision | The real options |
|---|----------|------------------|
| D1 | **Where the API call executes** | (a) in a normal run's asset/op; (b) in a Dagster-launched subprocess (Pipes); (c) **outside Dagster** in a standalone worker writing an AssetSpec + REST materialization (our ADR-0002); (d) external **batch API** submit + async poll |
| D2 | **Who keeps long-lived/async work running** | manual CLI (our #24 pain) · Sensor (polls → triggers runs) · declarative automation · a long-lived service |
| D3 | **Unit of retry / backpressure** | item (row) vs run. Per-item retry + backoff + dead-letter is almost always correct |
| D4 | **Enrichment scoping / incrementality** | per-fact (each post) vs **per-dimension** (each profile/creator) + nullability/incremental gates |
| D5 | **Determinism boundary (ADR-0003)** | keep pure transforms hermetic; confine stochastic calls to an explicit, tagged enrichment seam |

D1 + D2 are the ones that produced our two-world split and its observed pains.

## 2. Dagster-native building blocks available (grounded)

From the `dagster-expert` skill automation references + Dagster's model:

- **Assets** — a materialization = durable state. `gold_analyses` is naturally an asset, whatever writes it.
- **Resources** — inject the API client (a `GeminiResource`) into assets so code stays testable/configurable and the client owns rate-limit/retry config. Resource is the *right* home for "how to talk to the API," not module globals/op args.
- **Basic sensors with cursors** — poll an external condition and emit `RunRequest`s, tracking state in a JSON cursor (`[VERIFY]` canonical; see skill `basic-sensors.md`). **This is Dagster's native answer to #24**: an async external job (Gemini batch) is harvested by a sensor that polls "is the remote job done?" and triggers the harvest run — no manual re-run.
- **Asset sensors** — react to a materialization event (e.g., a labels/gold asset landing) to launch enrichment, with conditional logic on metadata.
- **Declarative automation** — asset-centric, composable `AutomationCondition`s (freshness/missing/updated). Dagster now recommends it over imperative sensors **for asset-to-asset automation inside one code location** (`[VERIFY]`); sensors remain right for triggering *outside* the graph / external side effects.
- **Asset checks** — attach invariants (freshness, prompt-currency — we already use `@asset_check`).
- **Partitions / backfills** — per-source or time-partitioned enrichment and replay.
- **Out-of-process**: Pipes (Dagster launches a subprocess and streams events) vs **external AssetSpec + REST** (external writer, eventual lineage). ADR-0002 rejected Pipes for enrichment: a run would block until the subprocess exits — wrong for hours-long, rate-limited work.

## 3. Typical design patterns (enrichment + API calls)

**P1 — Enrich as a queued, isolated external worker** (our current ADR-0002 shape).
Pure assets enqueue candidates into a durable queue (ops.sqlite `batch_items`);
a standalone worker claims items, calls Gemini with per-item retry/backoff +
dead-letter, writes `gold_analyses`, reports materialization via REST. Good for:
hours-long rate-limited work, crash isolation, not holding run slots. **Weakness it
must fix: the consume loop is a manual one-shot CLI.** The fix is not necessarily
"move to assets" — it's **make the consume loop a Sensor citizen** (D2), which
ADR-0002 never precluded.

**P2 — Enrichment as a native, incremental, dimension-gated asset group**
(job-search-toolkit's shape). Enrich assets call the LLM *in-process*, behind
column-nullability gates (only rows missing signal), scoped **per-dimension** (per
profile, not per fact post), bounded concurrency (bounded `ThreadPoolExecutor` via
a resource), idempotent upserts. Good for: moderate volume, one code path,
Dagster-native lineage/backfill/freshness. **Cost: relaxes ADR-0003** (a stochastic
call now lives in an asset — acceptable only if that asset is explicitly an
enrichment seam, not a "pure transform").

**P3 — External batch API submit + async sensor harvest.** Submit many items to a
remote batch job (Gemini batch), then a **sensor polls job status and triggers the
harvest asset** when done. This is the *direct* cure for #24 (a job "stuck"
overnight was done on Google's side, un-harvested) and pairs with `gemini-batch`
becoming our default (PR #49).

**P4 — Long-lived enrichment service via Pipes or external AssetSpec.** A durable
producer outside the core graph. Our ADR-0002 chose AssetSpec+REST over Pipes; a
Sensor can drive its "run when work exists" step the same way P1 can.

**The synthesis:** Dagster's native enrichment shape is **composition, not a single
giant asset** — a durable queue produced by pure assets; a **Sensor** (not a manual
CLI) that triggers consume/harvest when work exists or an external batch job is
done; **per-item** retry + backoff + dead-letter; enrichment expressed as external
assets/AssetSpec (or an explicitly gated enrichment asset group) with freshness +
asset checks; and **resource-injected clients**. Both Option A (sensor-driven
external worker) and Option B (enrichment-as-assets) are *legitimate* Dagster
shapes — the deciding factor is the ADR-0003 determinism boundary (D5) and whether
the work is hours-long/async (external + sensor) vs bounded/in-process (assets).

## 4. Cross-cutting best practices (enrichment + API calls)

- Inject the client via **resource**; the resource owns model/tier config + rate-limit knowledge (repo's two-429 taxonomy: `rate_limit_exceeded` → jitter+backoff; `insufficient_quota` → stop until quota reset).
- Call the API **at the leaf**; keep everything above/below pure (ADR-0003 spirit) so replay is deterministic and cost concentrates in one seam.
- **Per-item retry** (not per-run) with exponential backoff + jitter; dead-letter terminal failures; durable queue so the graph never blocks on Gemini.
- **Idempotent/upsert writes** keyed by `(post_id, domain, prompt_hash)`.
- **Backpressure**: cap concurrency, budget-gate expansion/enrichment, media token cap, triage-first (deep-pass only high-value items).
- **Media bytes cached at ingestion** (CDN expiry ~4-5d), never refetched in a transform (our #20 / media-cache precedent).
- **Freshness policy + asset checks** on gold; stale-prompt detection (`check_prompt_currency`).
- **Per-source trip-guard / circuit breaker** for scraping (job-search-toolkit does this; we have a single config-driven scrape asset).
- **Observability**: freshness, dead-letter triage, per-item status, budget spend.

## 5. Antipatterns in this space (specific)

- **A1. Stochastic API call inside a pure deterministic transform asset** → non-hermetic, non-replayable graph. (ADR-0003's exact rationale; the #1 thing our design rule forbids.)
- **A2. Blocking a Dagster run for hours** on rate-limited calls / holding run slots; **run-level retries for item-level 429s** → wastes quota, wedges runs. Retry must be per-item at the seam.
- **A3. `while True` poll loop inside an op/asset** → blocks a run forever. Polling external async state belongs in a **Sensor with a cursor**, not a run.
- **A4. Fire-and-forget batch submission with no automated harvest** → the #24 trap (job done on Google's side, looks stuck, un-harvested).
- **A5. Queue consumer as a manual one-shot CLI** (no sensor/schedule) → pipeline silently stops progressing when nobody re-runs it.
- **A6. Enriching per-fact instead of per-dimension** → repeated re-enrichment of the same profile; waste.
- **A7. Uniform deep processing of every item** (no triage/budget) → cost blowup at scale.
- **A8. No per-source isolation/trip-guard** → one flaky source aborts the whole run.
- **A9. No durable media cache at ingestion** → CDN URLs die mid-flight; enrichment starved or dead-lettered (#20).
- **A10. Secrets/config hardcoded or in ops**, not via resource/config.
- **A11. Re-running the whole pipeline** to process new rows (no incremental/nullability gate) → O(n) replay every run.
- **A12. Equating "Dagster citizen" only with "one asset that does everything in the graph"** → misses that Sensors + external AssetSpec are equally first-class. (This is the trap that would push us to a needless big-bang rewrite.)

## 6. Application to datalake (map to our option space)

| Our option | D1 | D2 | D5 (ADR-0003) | #24 fix |
|-----------|----|----|------|---------|
| **A** sensor-driven external worker | (c) external | **Sensor** | preserved | harvest Sensor |
| **B** enrichment-as-assets (JST) | (a) in-graph | decl. automation / asset sensor | **relax** | nullability-gated asset re-runs |
| **C** hybrid: profiles-as-`Source`s + worker citizen | (c)/(d) | Sensor + Source | preserved | Sensor |
| **D** on-demand single-run, no daemon | (a)/(d) | manual/materialize | relax | weakest |

`[VERIFY — web pending]`: declarative-automation recency + exact Dagster guidance
on external-asset vs Pipes for long-running API work; and whether Dagster
recommends sensors vs declarative automation for the async-harvest case.

## 7. Open / next research
- Re-run web search (providers were down) on: Dagster canonical enrichment/LLM
  examples; Dagster guidance on Pipes vs external AssetSpec for long-running work;
  declarative-automation vs sensor for async harvest.
- Read Dagster docs: declarative-automation, external-assets/AssetSpec, Pipes.
- Extract job-search-toolkit's `enrich_job`/`resources/llm_client.py` concretely.

## 8. Addendum — patterns-in-the-wild grounding (web recovered 2026-09-06)

Web search came back this session. Two sources materially ground the option
space and resolve several `[VERIFY]` gaps above.

**Community: long-running external API work in Dagster.** A widely-shared gist
`lukew-cogapp/dagster-long-running-operations` (2026-04) orchestrates bulk
embedding/LLM/vision API work (~150k records, hours-days) and codifies exactly
two patterns that mirror our space:
- **Pattern 1 — Bulk API: Submit + Sensor Poll.** A run filters records against a
  content-hash cache, submits only misses to the remote batch, persists the
  `job_id` + status "pending", and **ends immediately**. A lightweight sensor
  (on `minimum_interval_seconds`, ~300s for hour-long jobs) reads pending job ids,
  polls the remote job, and on terminal state issues a `RunRequest` to the harvest
  step. Stated properties map 1:1 to our #24 need: no worker sits open for the
  batch, job ids persisted externally so polling survives restarts, sensor ticks
  stay cheap/within timeout. **This is Option A's exact shape.**
- **Pattern 2 — No bulk API: cached resource + threaded fan-out.** A single asset
  fans work across a bounded `ThreadPoolExecutor`/async executor, memoizes on a
  content-hash key (input id + model version + prompt hash), flushes a Parquet
  cache periodically for crash safety, upserts idempotently. Optionally partitioned
  for checkpointing (sweet spot 500-2000 records/partition). **This is Option C's
  shape**, and the cache-key design is exactly our `(post_id, domain, prompt_hash)`.
- **Decision guide:** single-asset in-process polling fine when work is minutes;
  hours/days of external work → move to sensor-based polling (or webhook). This is
  the duration rule that keeps our bounded Option C separate from hours-long Option A.

**Dagster official / vendor.** The `dagster-expert` skill's automation references
(choosing-automation decision tree; basic/asset/run-status sensors; declarative
automation) confirm: basic sensors = "poll an external condition, request a run";
declarative automation (`AutomationCondition`, `eager/on_cron/on_missing`,
freshness) is Dagster's recommended idiom for asset-to-asset work *inside one code
location*; sensors remain right for triggering work outside the graph / external
side effects. Dagster 1.8 deprecated the experimental `external_assets_from_specs`
and `SourceAsset` naming in favor of **external assets** (`AssetSpec`) — so our
`gold_analyses` AssetSpec is the current canonical spelling, and Option B's "make
the roster an external trigger" is a cursor-based basic sensor reading
`ops.sqlite.profiles` (table-row polling is a textbook documented sensor idiom).
Dagster's own blog "When Sync Isn't Enough" endorses bounded async in-process
execution for inference/embeddings/enrichment — grounding Option C's in-process
shape for bounded work.

Net: **both** Option A (sensor + external worker) and Option C (in-graph gated
asset) are legitimate, in-the-wild Dagster shapes; the deciding factor is the
ADR-0003 determinism boundary (D5) and the duration/boundedness of the work —
the same conclusion the article now draws. Option B reduces to trigger
granularity (per-profile fan-out vs consolidated scrape), not whether the roster
drives ingestion.
