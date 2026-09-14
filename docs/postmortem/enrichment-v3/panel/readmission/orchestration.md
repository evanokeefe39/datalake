# EXPERT PANEL — READMISSION ROUND · ORCHESTRATION / DAGSTER SEAT

Reviewer: PanelOrchestration (Dagster asset/partition engineering). Read-only.
Subject: [`../seam-readmission-brief.md`](../seam-readmission-brief.md) — the "bin Gemini, re-admit through the seam" proposal.
Round 1 (this seat's previous findings): [`../process.md`](../process.md), [`../data.md`](../data.md).

---

## 1. Findings table — the handwaved, overstated, understated, conflated

| # | Claim (brief / W-FREEZE) | Flaw | Correction |
|---|---|---|---|
| F1 | W-FREEZE: "a manual attempt to write `gold_analyses` fails loudly rather than silently succeeding" (plan:157-158) | **The acceptance asserts a property its file list cannot deliver.** W-FREEZE touches `submit.py`, `harvest.py`, `enrichment/__init__.py`, `ops.sqlite` (plan:151-152). Removing the *entry points* (`submit_gemini_batches_job` `submit.py:227`, `gemini_batch_harvest_sensor` `harvest.py:419`, ops at `submit.py:215` / `harvest.py:361`) unregisters them from Dagster — but the module-level functions they wrap remain importable: `harvest_gemini_batches` (`harvest.py:271`) still calls `ensure_gold_analyses` (`harvest.py:288`) and `apply_retrieved` (`harvest.py:340-342`, which writes `gold_analyses`), and `submit_pending_gemini_batches` (`submit.py:178`) still calls the real provider. A `python -c` / notebook / ad-hoc script call succeeds silently after the freeze. The plan itself retires `analysis.py:179` (`INSERT INTO gold_analyses`) by name (plan:435) but that file is not in W-FREEZE's Files list. | The only real enforcement in the acceptance is the empirical one: "`gold_analyses` count unchanged after 24h" (plan:159-160). That is a convention probe, and the plan's own W9 text says it best: "the freeze is a convention enforced by removing entry points, and a gate that depends on a convention holding for weeks is a gate with a hidden assumption" (plan:437-438). To make the acceptance true as written, add an enforcement artifact — a runtime tripwire (a freeze flag `ensure_gold_analyses` refuses past), or an asset check on the frozen count — or reword the acceptance to the property actually delivered ("no registered Dagster entry point drives the path"). |
| F2 | Brief §2 / plan:185-187: "`build_adapter("gemini")` must still construct" as the W-FREEZE capability guard | **The name does not exist in the registry.** `adapters.py:359-360` registers exactly two names: `"service_backed"` and `"direct_batch"`. `build_adapter` raises `KeyError: unknown adapter 'gemini'; known adapters: ['direct_batch', 'service_backed']` for anything else (`seam.py:135-138`). As written, the W-FREEZE guard test would go red on day one — the one test meant to prove re-admissibility fails, and the obvious "fix" (registering a `"gemini"` alias) is an un-discussed naming decision on the seam's only naming point. | Either register `DirectBatchAdapter` under `"gemini"` (and keep `"direct_batch"` only if a deliberate alias policy exists) or correct the plan/test to `"direct_batch"`. This is a 1-line fix, but it is exactly the class of seam-owner decision the post-mortem said must be named up front (`data.md` §4), and it must be resolved BEFORE W-FREEZE bakes the wrong name into its acceptance. |
| F3 | Brief §2 / ADR-0014: "Partition key is `<workload>\x00r<N>\x00<post_id>`" | **That is the pre-hash payload, not the key.** `partition_key` returns `sha256(payload)[:16]` — a one-way digest (`partitions.py:155-156`). The docstring says it directly: "Keys are one-way digests; to map an in-flight key back to posts, re-derive `partition_key(workload, round, [pid])` per candidate and test membership" (`partitions.py:211-213`). This matters for every question about "admitting a dimension to the key": you cannot append a component post-hoc, and no consumer can parse a provider out of a materialized key. | See §2 below — the answer is that the key MUST NOT gain a provider dimension, and the codebase already argues why (F4). |
| F4 | Implicit proposal premise: two live providers would need the partition key to distinguish them | **Conflates provenance with identity.** The seam itself forbids fusing provider identity into keys: `prompt_identity` keeps the model "PROVENANCE — recorded beside the result, not fused into its key. Fusing it means swapping providers marks the entire corpus stale and re-enriches it for no semantic reason" (`seam.py:187-190`). The same argument holds with more force for the partition key: the drain's in-flight guard asks "is this post being enriched *by anyone*?" (`assets.py:1073-1074`) — a *provider-agnostic* question. A per-post provider axis in the key would let the guard approve the same post on provider B while it is in flight on provider A: a double-bill, the exact failure the guard exists to prevent (`assets.py:1037-1038`). | Provider is a **run-config dimension, not a partition dimension** (see §2). The key grammar admits no provider component and does not need one; per-post, per-round uniqueness is the invariant, and at most one provider may serve a post's round. |
| F5 | W-FREEZE: "This is a precondition, ordered BEFORE W1" and "formalizes an existing fact rather than stopping a running pipeline" (plan:135, 146-150) — implying the bin is operationally free | **True for the retired path; false for the corpus.** The freeze removes the only *submit* actor while the seam has **zero production callers** (`seam.py:145` `run_lifecycle` — brief §3) and `enrichment_harvested` has **no producer** (W4 not yet built; plan:316-322). Between W-FREEZE and W3/W4 the repo has a discovery actor (`ig_posts_gen_batches`, `assets.py:1114`) that still enqueues, and no actor that drains the queue it creates. That is a no-live-provider window by construction, and its consequence is the stall traced in §3 below. | The freeze is safe as a *code* operation and unsafe as an *operational* state. It needs one added acceptance: no drain run fires (or no drain run is allowed to enqueue) between W-FREEZE and W3, or the enqueue path is gated in the same unit. |

## 2. Q1 — What Gemini looks like as a Dagster object under the seam

### 2.1 One asset graph, provider as run config — not a second graph, not a partition axis

The two options the brief poses (second run/asset vs same run with a provider axis) share a
false premise: that the provider belongs somewhere in the *asset/partition* topology. Under
ADR-0008/0009 it does not. The Dagster-native shape:

**Assets (already exist, provider-agnostic, unchanged):**
- `enrichment_submitted` with `SUBMITTED_PARTITIONS` (`partitions.py:103-109`) — the partition
  space is named after the *asset*, not the provider; a second provider needs no second space.
- `enrichment_harvested` with `HARVESTED_PARTITIONS` (`partitions.py:113-115`) — same.
- The in-flight derivation `materialized(submitted) − materialized(harvested)`
  (`partitions.py:230-236`) is already provider-blind, which is the correct semantics: one
  post, one live enrichment, whichever provider served it.

**Jobs (new, per stage, provider chosen inside the run):**
- A **submit job** that discovers work from the instance's `enrichment_submitted` partitions
  (the W3 design, plan:302-305), resolves `build_adapter(config.provider)` at run time
  (`seam.py:130-139` — "Swapping providers is this string"), and drives the seam. Provider is
  a Dagster **run config field** (a `Config` class on the op, or a job-level `RunConfig`),
  not an asset, not a partition key, not a tag fused into derivations.
- A **harvest job** that per ADR-0014 (brief §2) reports the `enrichment_harvested`
  materialization AND mints the round-N+1 retry keys — the same run does both, per the plan's
  W4 contract (plan:741-743). It polls through the same adapter the submit run used; the
  provider string travels in run tags so a restart re-resolves it (the pattern the retired
  sensor already used for job names, `harvest.py:426-429`).
- If operational independence is wanted (a Gemini-only schedule, its own sensor cadence), that
  is **two jobs over the same two assets** (`define_asset_job` selection), not two asset
  graphs. Dagster schedules/sensors attach to jobs, so per-provider cadence is a job-level
  concern — the construct boundary where a provider axis legitimately lives.

### 2.2 The partition key: no provider dimension, and the codebase already says why

`partition_key(workload, attempt_round, [post_id])` digests to a 16-hex key
(`partitions.py:155-156`); no consumer can parse a dimension out of it, only re-derive it —
and the only deriver, the drain guard, derives with `DRAIN_WORKLOAD` and `DRAIN_ATTEMPT_ROUND`
alone (`assets.py:1023-1027, 1060`). Three consequences:

1. **Adding a provider component is a breaking key-shape change**, not an extension: every
   existing in-flight key becomes unreachable by the new derivation, and the drain would
   silently re-submit everything in flight — the round-0 double-submit defect (#7) repeating
   one dimension over. The plan's own blast-radius table flags exactly this class for W4's
   round keys: "partition keys minted before this change are round-0-shaped and unrecoverable"
   (plan:482).
2. **It is unnecessary**: `seam.py:187-190` states the governing rule — provider is provenance,
   "recorded beside the result, not fused into its key. Fusing it means swapping providers
   marks the entire corpus stale and re-enriches it for no semantic reason." `Result.provider`
   already travels on every result (`seam.py:78`); bronze rows are self-describing.
3. **It is harmful**: the guard's question is provider-agnostic ("is this post in flight on
   anyone?", `assets.py:1073-1074`). A provider axis would make the guard answer "in flight on
   *this* provider", licensing concurrent double-enrichment across providers — double billing,
   the exact failure the post-mortem identified as the identity's purpose
   (`partitions.py:243-253`).

**The concurrency rule the key enforces implicitly:** at most one provider serves a given
`(workload, round, post_id)` — i.e., per round, providers are serialized. That is correct for
the retry dynamics (round N+1 keys are minted fresh, `assets.py:1026-1028`, so a provider
*swap* happens between rounds, not within one), and ADR-0014's harvest-run-mints-retry-keys
design is what makes the swap point exist. **Answer: the grammar admits no provider dimension
and does not need one. If a genuine multi-provider-same-round need ever appears, the right
Dagster construct is a second partition *space* keyed by an explicit provider-bearing workload
string the caller passes in — a contract change owned by the seam owner, not a silent grammar
extension.**

### 2.3 Concrete re-admission checklist (Dagster constructs, not prose)

1. Job `enrichment_submit` over asset graph consuming `enrichment_submitted`; op `Config`
   field `provider: str` (default from `detect_provider()`, `adapters.py:347-352`).
2. Job `enrichment_harvest` (or the same run's second op, per ADR-0014's single-run design)
   that: polls through the adapter, lands verbatim bronze, calls
   `instance.report_runless_asset_event(AssetMaterialization("enrichment_harvested",
   partition=key))` — the mirror of the drain's enqueue pattern (`assets.py:1088-1092`) —
   and mints round-N+1 `enrichment_submitted` keys for failures.
3. Sensor *optional*, not structural: a cadence trigger over the harvest job (the retired
   `gemini_batch_harvest_sensor`'s job-sensor pairing, `harvest.py:414-419`, is the template,
   but its `batch_jobs` discovery query, `harvest.py:397-401`, dies with W3 — the new
   discovery surface is the instance's partitions, not sqlite).
4. Registry naming resolved (F2): the provider string in config must match a registered name
   (`adapters.py:359-360`).
5. The W-FREEZE constructibility test (plan:184-187) upgraded from "builds" to "builds AND one
   real spike round-trip through `run_lifecycle`" — construction is not liveness (brief Q3's
   own distinction); the seam has never driven a real call in this repo (brief §3).

## 3. Q5 — The no-live-provider window: the stall, traced to the mechanism

**Verdict: yes, the bin creates a window with no live submit path, and if a drain run fires
during it the entire label-approved candidate corpus enters permanent in-flight suppression —
with no live instrument that fires on it, and one dormant one prescribing the wrong remedy.**
Traced end to end:

1. **Enqueue with no submitter.** `ig_posts_gen_batches` enqueues by materializing
   `enrichment_submitted` partitions runlessly: `add_dynamic_partitions` +
   `report_runless_asset_event` per post (`assets.py:1083-1092`, called at
   `assets.py:1289`). This path has **no dependency on any provider existing** — it consults
   `ig_post_labels` and the instance only (`assets.py:1118, 1159-1170`). It runs happily
   forever with zero providers.
2. **Nothing consumes the queue.** The only consumer of `enrichment_submitted` was the retired
   `submit_gemini_batches_job`/op pair, which read `batch_jobs` anyway (`submit.py:77, 65-88`)
   and dies at W-FREEZE. The seam's discovery-from-partitions submit is W3 (plan:302-308), and
   W3 depends on W1 and W2 (plan:309) — weeks away, per the plan's own graph
   (W-FREEZE → W1 → W2 → W3, plan:529).
3. **In-flight grows.** `in_flight = materialized(submitted) − materialized(harvested)`
   (`partitions.py:230-236`). With no harvest producer (W4 unbuilt, plan:316-322), every
   enqueued key enters in-flight and **never leaves**. Not unbounded in the mathematical sense
   — bounded by the approved-candidate set (~2.2k-post corpus, label-approved subset) plus any
   retry rounds — but absorbing the *entire* approved corpus in one drain run.
4. **The guard suppresses the whole corpus.** `drain_suppressed_post_ids` filters every
   candidate whose derived key is in-flight (`assets.py:1043-1061`, applied at
   `assets.py:1228-1244`). After the absorbing run, every subsequent drain run sees all
   candidates suppressed and emits only a WARNING log: "enqueueing nothing — ALL %d
   candidate(s) in flight" (`assets.py:1256-1261`). **No failure, no check, no asset-materialization
   signal — a warning string is the only trace.** This is precisely the stall mechanism the
   post-mortem identified, reproduced not by a bug but by the *ordering* of the freeze.
5. **The accounting identity cannot catch it.** `done + failed + in_flight + backlog ==
   total_candidates` (`partitions.py:245`) — the identity **holds** during the stall: in_flight
   is a legitimate bucket and the books balance perfectly while zero progress is made
   (`partitions.py:243-253` states what the identity detects: instance/lake disagreement, not
   progress cessation). W8's freshness/zero-progress gates do not exist yet (plan:407-415).
6. **One existing check CAN fire on it — with the wrong remedy, and only if checks execute.**
   `check_enrichment_health` (registered in `ENRICHMENT_CHECKS`, `assets.py:211-215`) fails
   when `approved_unenriched > 20`, counting label-approved posts absent from `gold_analyses`
   (`assets.py:138-146`, query at :105-114). Under the stall that condition trips exactly as
   designed — so the stall is not fully silent — but its failure description prescribes
   "run ig_posts_gen_batches to drain the admission gate" (`assets.py:143-144`), the
   *opposite* of correct under a no-provider window: the drain cannot drain, and re-running
   it merely re-observes suppression. And it only fires when asset checks execute. Grounded:
   `common/schedules.py:17-30` registers `daily_medallion` — **whose target list INCLUDES
   `ig_posts_gen_batches`** (`schedules.py:22`) — and `core_refresh`; both are
   `default_status=STOPPED` (`schedules.py:28`, `:87`), and the only enrichment sensor is
   `DefaultSensorStatus.STOPPED` (`harvest.py:414-418`), so nothing ticks today is a fact.
   The sharper consequence: the stall is **one UI toggle away, not one manual run away** —
   enabling `daily_medallion` starts a 03:00-daily drain into a corpus with no submitter, and
   nothing in the schedule's description warns of the missing-consumer dependency.
7. **The "escape hatch" worsens it.** Explicit `post_ids` re-enrichment "bypasses all guards"
   (`assets.py:1172-1173`) but still enqueues through the same partition materialization
   (`assets.py:1283-1289`) — so a manual re-enrichment attempt during the window *permanently
   suppresses those posts too*, adding to the in-flight set with no submitter to ever clear them.

**Recoverability (why this is a stall, not a loss):** the in-flight state is event-log-derived,
not stored (`partitions.py:221` — "a pure function of the instance snapshot — no stored
state"). Once W3's submit consumes the queued partitions and W4's harvest reports
`enrichment_harvested`, in-flight drains and the suppressed posts become eligible again.
Damage is a *delay of unknown duration*, plus the operational blindness in 4-5.

**Required mitigations (minimum):**
- (a) Add a W-FREEZE acceptance: **no drain run is triggered between W-FREEZE and W3** (the
  drain is manual today — its only schedule target (`daily_medallion`, `schedules.py:22`) is
  `default_status=STOPPED` (`schedules.py:28`) — so this is one operator rule while stopped,
  and a standing hazard the moment that schedule is enabled), OR
- (b) Gate the enqueue in `ig_posts_gen_batches` behind a live-submit precondition (fail loudly
  if no submit job is registered) — one guard clause in the same file; and
- (c) At W3 entry, inventory the instance's materialized `enrichment_submitted` partitions —
  anything enqueued during the window is pre-existing W3 work, not new drain output.

## 4. Q2 — What must survive the bin for re-admission to be a *wiring* job

Named constructs; loss of any one makes re-admission a rewrite, not a re-admission:

| Construct | Where | Why it is load-bearing |
|---|---|---|
| `ProviderAdapter` Protocol + `ServiceBackedAdapter` + `DirectBatchAdapter` + `build_adapter` + `run_lifecycle` | `seam.py:94, 130, 145`; `adapters.py:133, 228` | The seam is the thing re-admission goes *through*. It has zero production callers (brief §3) — the freeze/W5 cleanup must treat it as **load-bearing dormant**, not dead code. `dormant-vs-broken`: a deliberately-retired *usage* over a live interface is healthy; deleting the interface is the one irreversible mistake in the whole proposal. |
| Canonical terminal vocabulary + per-adapter normalization | `seam.py:23-31`; `adapters.py:106, 125-130, 214-225` | Gemini's native states reach the seam only through `_GEMINI_STATES` → `normalize_state`; this mapping is the live, tested-in-spike-only translation of `gemini_batch.job_state/is_terminal` (`harvest.py:316-319`). Lose it, and re-admission re-derives the divergent-terminal-predicates defect (#5). |
| `partition_key` + `in_flight_partitions` + `Account` + the two partition definitions | `partitions.py:118, 205, 239, 107, 113` | The key grammar is the shared contract between drain, (future) submit, harvest, and the guard. Any W-FREEZE edit to `partitions.py` (it is in W4's Files, plan:322) must not touch this quartet. |
| `gemini_batch.py` client verbs (`submit/poll/job_state/is_terminal/retrieve`) | `harvest.py:311-334` callers; module per plan:184 | The only working implementation of Gemini's API contract. `DirectBatchAdapter` wraps *it* — the adapter is not self-sufficient. |
| The handle-serialization divergence as a *known defect* | `submit.py:170` (`"|"`) vs `adapters.py:280` (`,`), plan:336-340 | The freeze must not "resolve" this by deleting one side silently; the multi-chunk round-trip test (plan:306) is the artifact that survives to force the fix. |
| `detect_provider()` + registration names | `adapters.py:347-360` | The only existing provider-selection surface; the run-config default and the F2 naming decision live here. |
| `tests/fixtures/real_envelope_gemini.json` (W1 output) | plan:194 | The real Gemini response shape; without it, re-admission re-faces the flat-mock validation gap (round-1 F4). |
| The W-FREEZE deletion map (plan:167-172) + the constructibility test (plan:184-187) | plan | The record of *which nine sites bypassed and why* — re-admission's don't-do-this list, and the guard that keeps "unused but available" true (plan:515). |

## 5. The standard control (orchestration form)

The round-1 data seat's control (declared lineage + orphan check, `data.md` §3) has a precise
Dagster instantiation here: **the asset graph must make the drain's output a *typed input* of
the submit stage, and a check must assert the queue drains.** Concretely:

- W3's submit asset declares `deps=["enrichment_submitted"]` (the graph edge the round-1
  findings showed was missing — defect #1) — this is already the plan's design (plan:303-305,
  311-313); the freeze does not disturb it.
- The orphan direction is the one W-FREEZE creates: **a producer (`ig_posts_gen_batches`)
  whose consumer is retired.** The orchestration control for that is a single asset check on
  the in-flight set — `in_flight monotone non-decreasing across N runs ⇒ andon` — or, cheaper,
  the W-FREEZE precondition of §3(a)/(b) above. W8's freshness checks (plan:410) already name
  this class; the finding is that **W-FREEZE lands before W8**, so the window runs unguarded
  unless the freeze unit itself carries the control.

## 6. W-FREEZE acceptance, assessed honestly

"Disabling the entry points" is real and observable (registry listing, plan:156-157).
"A manual attempt to write `gold_analyses` fails loudly" is **not delivered by the unit's file
list** (F1) — the functions stay importable and write successfully. The honest acceptance is
either the empirical probe alone (count unchanged after 24h/`N` days — a convention probe the
plan already labels fragile at plan:437-438) or one added enforcement artifact. Cheap options,
in ascending strength: (1) delete rather than retire the row-writing path —
`harvest_gemini_batches`'s `ensure_gold_analyses` + `apply_retrieved`→`write_gold` chain
(`harvest.py:49, 288, 340-342`; the INSERT at `analysis.py:179`) — alongside the entry points;
the only other `ensure_gold_analyses` callers are the two asset checks
(`assets.py:82, 160`), whose use is DDL-only and harmless, so deleting the retired chain closes
every code path to a `gold_analyses` row write while breaking nothing registered; (2) a
`freeze` flag consulted by `ensure_gold_analyses` that raises; (3) a scheduled asset check
asserting the count is unchanged. (1) is free and closes the actual hole: the *only* remaining
production writer of `gold_analyses` reachable from registered code after the freeze would
otherwise be an import, not an entry point — the distinction the acceptance gestures at
without drawing.

---

*Evidence base — READ: `docs/postmortem/enrichment-v3/panel/seam-readmission-brief.md` (full);
`remediation-plan.md` W-FREEZE/W0/W3/W4/W5/W8/W9/§6/graph (:111-160, :302-349, :407-415,
:419-449, :465-548, :686-743); `src/datalake/defs/instagram/assets.py` :1022-1315 (drain,
guard, enqueue helper); `src/datalake/defs/enrichment/partitions.py` :1-360 (full);
`src/datalake/defs/enrichment/submit.py` :1-230 (full); `src/datalake/defs/enrichment/harvest.py`
:271-478 (harvest + sensor); `src/datalake/defs/enrichment/seam.py` :1-200; `adapters.py`
:1-360 (structure + registration + detect_provider). Round-1 house format matched against
`panel/data.md`. NOT executed: no live-DB reads (all DB-adjacent numbers cited from the plan's
own 2026-09-14 live reads, plan:701); no test runs (read-only constraint). INFERENCE, marked:
the stall in §3 is code-traced but *unobserved* — it requires a drain run during the window;
the "weeks away" W3 estimate is read from the plan's dependency graph (plan:529), not a
schedule. ADR-0012/0014 are cited as their content appears in the plan (W0 addendum, plan:27-28,
111-115) and the brief (§2); no standalone ADR files exist under `docs/postmortem/enrichment-v3/`
(verified by directory listing).*
