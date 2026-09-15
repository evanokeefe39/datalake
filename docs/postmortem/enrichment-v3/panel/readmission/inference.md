# EXPERT PANEL — ML / INFERENCE ENGINEERING SEAT (Round 2: Gemini bin & re-admission)

Reviewer: PanelInference (provider semantics, billing, idempotency, provenance). Read-only on `src/` and the plan.
Subject: [`../seam-readmission-brief.md`](../seam-readmission-brief.md) — the proposal to temporarily bin Gemini *usage* and re-admit it later through the ADR-0008/0009 seam.

---

## 1. Findings table — where this loses money, data, or correctness (Q5)

*(populated incrementally; see footer for evidence base)*

| # | Claim | Flaw | Correction |
|---|---|---|---|
| F1 | "Qwen-only keeps the corpus processable" | **Wrong workload granularity.** The seam is a *provider* seam, not a *workload* seam. The only live seam path today is the facets workload (`facets_batch.py:283-292` builds `service_backed`); the content-classification workload — the one that wrote `gold_analyses` (`analysis.py:171-188` is the upsert) — has **no qwen call site at all**. Binning Gemini therefore does not leave "qwen processing the corpus"; it leaves 9,576 of 10,038 posts' classification workload with **zero live providers**. | Quantified: live DB `silver_ig_posts` = 10,038 rows; `gold_analyses` = 9,576 rows, `count(distinct post_id)` = 9,576; **462 posts have no classification row** and cannot gain one while Gemini is binned. The seam *could* serve the workload through qwen (`Item` carries prompt + images, `seam.py:50-57`), but no adapter call site exists for it — enabling it is new code, not a config flip. |
| F2 | "A schema/mapping change is a deterministic silver replay with zero provider calls" | **The premise is currently unexercised, and the bin does not create it.** The zero-re-bill claim rests on verbatim bronze landing (`harvest.py:80-86`: "the paid/stochastic part ends at bronze"). But the bronze landing lives *inside the harvest path being retired* (`harvest.py:89-157`), and round 1 established bronze has zero rows — the 9,576 paid responses were never landed. The replay material for the existing corpus is `gold_analyses.result_json` (live DB: present for all 9,576 rows), **not** bronze. | The bin/re-admit cycle itself does not re-bill *existing* rows — re-admission replays from `result_json`, zero provider calls, provided the Phase-5 backfill runs before or during re-admission. What the bin *does* create: a window where NEW paid Gemini responses land nowhere — there is no bronze writer for them and (after the entry points are retired) no writer at all. Money is not lost on the old corpus; the *re-billing guarantee is lost for anything inferred during the bin* if that work happens through any non-landing path. |
| F3 | "Temporarily bin creates no orphaned state" | **One job is genuinely in flight.** Live DB `ops.sqlite`: `batch_jobs` has 6 rows — jobs 1–5 `status='complete'`; **job 6 is still `status='processing'` with 2 `pending` `batch_items` (attempts=2)**. Under ADR-0013 the provider job state is external (ADR-0013:44-49) — if the sensor and submit job are retired while job 6 is unresolved, those 2 items are stranded: Dagster never polls the provider job again, and Gemini batch jobs EXPIRE (`gemini_batch.py:34` — `EXPIRED` is a terminal-fail state). | Fix is cheap and must be in the bin procedure: **drain before binning** — poll job 6's `gemini_batch_name` to terminal, retrieve, land, and only then retire the entry points. Two items is trivial money; the defect is procedural (silent abandonment), not economic. |
| F4 | "Provenance survives the bin" | **Partly — and one number in the brief is wrong.** Live DB `media_cache` = **27,748** rows, not the brief's 2,768 (`ops.sqlite`). `media_metadata` = 5,613 rows, 5,419 `uploaded` with `file_api_uri` set — and their `expires_at` spans **2026-09-02 → 2026-09-11, i.e. every File API URI is already expired** (today 2026-09-14). 194 rows are stranded in `uploading`. Gemini provenance in `gold_analyses` is model-only (`model` column; 9,568 × `gemini-3.5-flash-lite`, 8 NULL) with `prompt_hash` — the table has **no provider column**. | Consequences for re-admission: (a) media re-admission MUST re-upload from `media_cache.local_path` (or re-fetch via `media_metadata.media_url`) — the stored `file_api_uri` values are dead provenance and must never be replayed; (b) `uploading`-state rows are orphaned mid-upload and need reset-and-requeue; (c) if the Phase-5 backfill promotes `result_json` into silver, provenance (`provider`, `model`, `prompt_hash`, landing timestamp) must be carried forward or the derived rows are untrustworthy under `provenance-on-derived` — binning does not orphan the *rows*, but re-admission through the seam writes to bronze/silver with a *different* provenance schema than `gold_analyses` carries, so the two eras will not join without an explicit backfill mapping. |

---

## 2. Missed patterns (inference-specific)

| Pattern | Verdict | Note |
|---|---|---|
| **Provider smoke test as a deploy gate** | **APPLIES — and the bin removes even the ability to run one** | Inference platform practice: a provider is never declared live until a *canary round-trip* (1-item submit → poll-to-terminal → retrieve → land) passes against the real API. The repo has this as an *ad hoc* property of every batch run, not as a gate. Once the two entry points are retired, there is no harness that can execute a Gemini round-trip at all until new call sites exist — so "bring it back later through the seam" has no verification instrument during the bin. Cheapest mitigation: keep one pytest-based live smoke (skipped without `GEMINI_API_KEY`) that exercises `build_adapter("direct_batch")` end-to-end. |
| **Credential/tier expiry as a lifecycle state** | **APPLIES** | The tier gate (`GeminiTierConfig.detect().supports_batch`, `adapters.py:337-341`) is the direct path's `health()`. It is checked at *submit time* (`gemini_batch.py:160-165`). During the bin, tier/payment state drifts silently; `DirectBatchAdapter.health()` at re-admission time is the first signal — and it can be true while the *model name* has drifted (the adapter default `gemini-2.5-flash-lite` at `adapters.py:51` vs the historical corpus model `gemini-3.5-flash-lite` in `gold_analyses`). Health must cover model liveness, not just tier. |
| **File-API media lifecycle vs batch job lifecycle** | **APPLIES** | Media upload state (`media_metadata.upload_state`, File API URIs with 48-hour-class expiry — live DB max `expires_at` = 2026-09-11) decays on a *shorter* clock than any plausible bin. Any re-admission design that assumes "media metadata is reusable" is wrong by construction; the re-upload step is not an optimization, it is mandatory. The brief's proposal does not mention media at all — for a multimodal corpus this is the largest silent correctness gap in the bin. |
| **Cost metering pinned to a landing row** | **PARTIALLY PRESENT** | `gemini_batch.py:65-67` states estimates are "chunk sizing only, not billing", and `facets_batch.py:240` has an estimator for facets. Nothing ties an actual provider charge to a bronze row. The bronze-verbatim landing is what makes *reconstruction* cheap, but the design has no place that records *what was paid* at submission time. Re-admission is the moment to add it (it is a `JobSpec` field), not another thing to defer. |

---

## 3. Q3 — the actual re-admission procedure: "constructs" vs "live"

**First, a naming correction that matters operationally:** the registry key is `"direct_batch"`, not `"gemini"` (`adapters.py:359-360` — `register_adapter("direct_batch", DirectBatchAdapter)`). The brief's phrase `build_adapter("gemini")` names no registered adapter; `build_adapter` raises `KeyError` on unknown names (`seam.py:130-139`). Every procedure step below uses the real key.

### What the seam gives you for FREE (constructs — true today, no work needed)

1. `DirectBatchAdapter` is implemented and registered: submit/poll/retrieve via the SDK verbs (`adapters.py:276-335`), composite-handle chunk aggregation (`adapters.py:287`, `,`-joined; `poll` splits on `,` at `adapters.py:289-297`), canonical-state normalization with unknown-state rejection (`adapters.py:299-316`).
2. The canonical vocabulary and the registry/`build_adapter` dispatch (`seam.py:26-39`, `:110-139`).
3. `run_lifecycle` as a correct loop *shape* (submit → poll-to-terminal → retrieve, `seam.py:145-179`).
4. The identity function family for analysis contracts (`seam.py:184-200`).

All of this stays alive during the bin *as code*, exercised only by unit tests. That is what "constructs" means: **`build_adapter("direct_batch")` returns an object, not a working provider.**

### What the seam does NOT give you (must be built or observed — this is where re-admission lives)

1. **A caller.** `run_lifecycle` has zero production callers (brief §3, confirmed: the name occurs only in `seam.py` and tests). Re-admission = writing the Dagster asset/job that calls it. The proposal's appeal to the seam hides this: the seam is a *library*; the re-admission is a *wiring project* of the same size the nine deletions avoided.
2. **`JobSpec` / per-call `max_tokens`.** The chunk cap cannot pass through the seam's submit verb today (`adapters.py:285` passes `self.max_tokens`, but a seam caller has no way to set it per workload). This must land *before* Gemini re-admission or the first re-admitted workload either silently uses the default cap or bypasses again.
3. **The workload driver + ADR-0014 partition keys.** Round-N retry keys and the harvested producer are specified (ADR-0014) but unimplemented; re-admission inherits them as prerequisites.
4. **A bronze landing writer for the seam path.** The existing `_land_verbatim` (`harvest.py:89-157`) is coupled to the legacy harvest. The seam path's harvest step must land verbatim with `provider`/`model`/`run_id` (the `Result` dataclass carries `provider`, `seam.py:61-78`; adapters set it, `adapters.py:332`).
5. **Media re-upload.** Every File API URI is expired (live DB, F4). Re-admission must re-drive `media_upload` from `media_cache.local_path` before any multimodal submit.
6. **Liveness evidence.** Tier gate true (`GeminiTierConfig.detect().supports_batch`), current SDK/API version, and a *real observed* round-trip.

### The procedure (executable, ordered, gated)

| Step | Action | Gate / evidence |
|---|---|---|
| 0 | **Drain job 6** before the bin: poll `batches/…` (job 6's handle in `batch_jobs`) to terminal, retrieve, land verbatim, close the 2 pending `batch_items`. | `batch_items` has zero non-terminal rows; job 6 status ∈ {RETRIEVED, JOB_FAILED}. |
| 1 | Land the legacy corpus: Phase-5 backfill of `gold_analyses.result_json` → bronze (verbatim envelopes + provenance columns). Run **before** deleting the legacy path so the mapping code is still importable. | bronze row count ≥ 9,576; re-run is idempotent on natural key. |
| 2 | Add `JobSpec` to the seam's submit verb (the named fix for the `max_tokens` defect). | Test: per-call `max_tokens` reaches `DirectBatchAdapter.submit` → `gemini_batch.submit(..., max_tokens=...)`. |
| 3 | Restore a **live smoke**, not the nine sites: one pytest marked `live` that builds `service_backed` *and* `direct_batch`, submits a 1-item synthetic prompt, polls to canonical terminal, retrieves, and asserts the `Result` shape. Skipped (not failed) without credentials. | Green with key; skipped without. This is the artifact that keeps "temporarily" honest. |
| 4 | **Re-admit**: write the seam-calling Dagster job/sensor for the classification workload (the only consumer of the 462 unenriched posts), ADR-0014 partition keys, bronze landing in the harvest step. | `run_lifecycle` has ≥1 production caller; asset graph edge exists. |
| 5 | **Declare the provider LIVE only after**: (a) `build_adapter("direct_batch").health()` is true (tier gate); (b) the smoke of step 3 passes against the current API/SDK; (c) one real workload item lands verbatim in bronze with provider/model/run_id; (d) a conform replay over that bronze row completes with **zero additional provider calls** (assert by poll-count or mocked-transport call log). | (a)–(d) each independently observable; the provider is "live" only when all four hold on the same day. |
| 6 | Reconcile the two eras: seam-path rows and backfilled `result_json` rows must join on `post_id` with provenance columns populated on both. | Join query returns 10,038 rows, zero NULL provenance. |

Steps 3 and 6 are the difference between re-admission and a rewrite: if the smoke harness (3) or the provenance mapping (6) does not survive the bin, re-admission rebuilds discovery, fixtures, and reconciliation from scratch.

---

## 4. Terminal-state semantics: divergent predicates, classified

The canonical contract is four states, terminal = `{completed, failed}` (`seam.py:26-32`). Against it:

- **qwen / service-backed**: 1:1 map, and an unknown state *raises* rather than guesses (`adapters.py:125-130`, `:179-186`). `qwen_client` independently defines terminal = `{completed, failed}` (`qwen_client.py:25`, `:148-150`). This is the reference implementation — the service's own vocabulary was *designed to be* the canonical one (ADR-0009: the service replaced `gemini-batch` as the async-job holder; ADR-0009:54-56).
- **Gemini / direct-batch**: 10 native states collapsed to 4 (`adapters.py:214-225`); module-level predicates live separately in `gemini_batch.py:33-35` (`_TERMINAL_OK`/`_TERMINAL_FAIL`/`_ACTIVE`, including a `"SUBMITTED"` active state that appears in no official enum list the comments cite).

**Three findings:**

1. **The lossy aggregate is the real divergence, not the vocabulary size.** `DirectBatchAdapter.normalize_state` folds a composite handle by priority `FAILED > all-COMPLETED > PENDING > PROCESSING` (`adapters.py:299-316`): one failed chunk marks the *whole submission* `FAILED`, while `retrieve()` still fetches all chunks (`adapters.py:318-324`) — so completed chunks are retrievable under a `failed` aggregate, a state/result contradiction a seam consumer must know about but cannot see in the canonical vocabulary. qwen has no composite handles, so it cannot express this defect. **This asymmetry is a contract decision**: the seam must either declare composite-handle aggregation semantics in `Capabilities` (it currently says only `supports_chunking=True`, `adapters.py:242`) or reject partial-FAILURE aggregates.
2. **`EXPIRED → failed` conflates "provider dropped the job" with "the work failed".** Gemini `EXPIRED` means the job never completed its lifecycle provider-side (also `CANCELLED`). Mapping all three to `failed` (`adapters.py:223-224`) is correct at the vocabulary level but loses the *retry disposition*: an EXPIRED job's items were never attempted, so retrying them is free and correct; a FAILED job's items were attempted, so retrying must respect attempt counts and backoff. The seam has no slot for this distinction (`classify_error` classifies exceptions, not terminal job states, `seam.py:107`). **Contract decision**: terminal-substate → retry-disposition mapping belongs in the interface (or in `Result`/`Capabilities.notes`), not buried in the adapter dict.
3. **The module-level duplication is an implementation detail — but binning does not remove it.** `gemini_batch.is_terminal` (`gemini_batch.py:252`) and `qwen_client.job_is_terminal` (`qwen_client.py:148`) coexist with the adapter-level predicates. The adapters already bend to canonical; the residual defect is two stale free-function predicates that can drift from the adapter mapping. The bin (retiring entry points, keeping the client) leaves all three vocabularies intact and **changes nothing about this defect** — which is worth saying plainly: the proposal does not fix any of the named seam defects; it only stops exercising them.

**Who bends:** Gemini has already bent (adapter normalizes into canonical; qwen's vocabulary is canonical by construction). The remaining bend required is *semantic*, not lexical: composite-aggregate and EXPIRED-vs-FAILED dispositions. Both are contract decisions to be recorded in ADR-0008/0009 (or a new ADR), because `run_lifecycle`'s callers will branch on them.

---

## 5. Verdict on the proposal, from this seat

The proposal is **directionally right but procedurally underspecified**. Right: keeping `gemini_batch.py`/`DirectBatchAdapter`/`build_adapter` (the constructs are genuinely reusable, `adapters.py:228-341`) and avoiding nine per-site migrations. Wrong in three load-bearing ways:

1. **It does not preserve the economic guarantee it advertises.** The zero-re-bill replay claim needs the Phase-5 backfill (F2) and a seam-path landing writer; neither is in the bin. The bin as described deletes the only writer (legacy harvest) before the replacement exists.
2. **"Qwen-only" is a misnomer for the classification corpus.** It is *no-provider* for 462 posts and a frozen corpus for the rest (F1).
3. **It skips the drain and the media reality** (F3, F4): job 6 must be drained before retirement, and every File API URI is already dead, so re-admission includes a mandatory media re-upload campaign the proposal never mentions.

**Bottom line:** bin the *usage*, but the bin must be an ordered procedure — drain → backfill → JobSpec → live smoke → seam wiring → liveness gates → reconcile — not two moves. As sketched in the brief, it loses the replay guarantee's only remaining implementation and strands one in-flight job.

---

*Evidence base — actually read: `seam-readmission-brief.md` (entire); `src/datalake/defs/enrichment/{seam,adapters,gemini_batch,submit,harvest}.py` (key ranges cited inline); `facets_batch.py` and `qwen_client.py` (structural survey + cited lines); `analysis.py:171-188`; ADR-0009 (grep-skimmed, lines cited); ADR-0013 (lines cited). Live DB observations (read-only): `data/state.duckdb` — `gold_analyses` 9,576 rows / 9,576 distinct post_ids / 0 NULL `result_json` / model histogram (9,568 gemini-3.5-flash-lite, 8 NULL) / 1 distinct domain; `silver_ig_posts` 10,038; 462 posts without gold_analyses row. `data/ops.sqlite` (RO URI) — `batch_jobs` 6 rows (jobs 1–5 `complete`; job 6 `processing` with 2 pending batch_items at attempts=2); `batch_items` 10,231 complete / 52 failed / 2 pending; `dead_letter` 776; `media_cache` 27,748 (brief said 2,768 — brief error); `media_metadata` 5,613 (5,419 uploaded with file_api_uri, expires 2026-09-02→09-11; 194 uploading); `facets_batch_jobs` 4 (1 JOB_FAILED, 2 RETRIEVED, 1 SUBMITTED); `prompt_registry` 1. Round-1 `panel/data.md` read for format and the bronze-zero-rows fact (cited, not re-executed — READ-ONLY constraint). Inferred, marked as such: Gemini EXPIRED/CANCELLED provider semantics beyond what the code's state sets encode (standard Gemini Batch API behavior, not verified against live docs in this session).*
