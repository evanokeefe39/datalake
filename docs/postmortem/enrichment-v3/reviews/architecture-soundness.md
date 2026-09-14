# Target-architecture soundness review — Enrichment v3

Read-only design review, 2026-09-13. Branch `feat/enrichment-v3-phase-1-seam-and-landing`.
Scope: the TARGET design (ADR-0011/0012/0013 + `docs/architecture/pipelines/enrichment.md`,
`docs/architecture/services/inference.md`, `tasks/plans/enrichment-v3-migration-master.md`),
judged independently of implementation progress. Implementation status is a sibling
reviewer's job and is deliberately out of scope except as citation material.

Sources cited: ADR sections, spec sections, and `file:line` in this repo at review time.
All greps/reads were run fresh today; nothing is cited from memory.

---

## 1. Producer/consumer completeness — **SOUND-WITH-CAVEATS**

Enumerated state spaces in the target design, classified on the DESIGN (not the code):

| State space | Producer (design) | Consumer (design) | Verdict |
|---|---|---|---|
| `bronze_enrichment_raw` | harvest = poll + retrieve + idempotent verbatim landing (`pipelines/enrichment.md` §8 "Orchestration" bullet; `inference.md` §5 "landing (target)") | six silver conforms; audit/WAP replay (`enrichment.md` §0, §3) | complete |
| `silver_visual_annotations` / `silver_visual_summaries` | conform of the qwen-vision pass's bronze rows (`enrichment.md` §8 diagram) | gold marts, `v_post_detail` | complete |
| `silver_audio_transcripts` | conform of whisper pass (incl. explicit `no_audio_source` rows, §1) | text/classification submits' inputs + gold | complete |
| `silver_text_annotations` / `silver_text_summaries` | conform of text-LLM pass | gold marts, classification input | complete |
| `silver_content_classification` | conform of classification pass; Phase 5 migration backfill from `gold_analyses` (`master.md` §6.1) | `v_post_detail`, `v_overview`, marts | complete |
| four `gold_*` marts | Phase 6 marts composing canonical views + silver (`enrichment.md` §5) | thin analytics projections, dashboard, owner's three questions | complete |
| 21 canonical views + dims | serving layer; authoritative list `DUCKDB_VIEWS` (`schemas.py`), asserted by `test_state_compatibility.py` (`enrichment.md` §"Preserved serving surface") | marts + dashboard | complete |
| `enrichment_submitted` partition space | submit step, materialized before the billed call (placeholder-before-POST) (ADR-0013 "Consequences" bullet 2) | `in_flight = submitted − harvested`, drain guard, accounting identity (ADR-0012 #1, `partitions.py:92`) | complete |
| `enrichment_harvested` partition space | harvest run, materialized when bronze rows land (`partitions.py:29-33`; `inference.md` §5 harvest composition) | same derivations | **complete in the design** — the code's missing producer is an implementation gap, not a design gap |
| `prompt_registry` (ops.sqlite, retained) | Phase 3 prompt-identity writer (`master.md` §3 Phase 3; `registry.py:35`) | prompt-hash provenance reads (`registry.py:56`) | complete |
| service job store (outside our schema) | the service (`POST /jobs`) | Dagster poll over `GET /jobs/{id}` (`inference.md` §3 contract) | complete |
| **silver quarantine** (`silver_enrichment_quarantine`, `conform.py:77`) | silver validation ("quarantine/dead-letter LOUDLY", ADR-0011 Silver section) | **NONE NAMED** — no ADR declares the table, its schema/key, its retention, or its triage consumer. The old `dead_letter` had at least a declared PK and "manual triage" owner (`pipelines/enrichment.md` "Current state" DDL). The replacement quarantine exists in code but is **unspecified in the design layer**. | **consumer only / underspecified** |

**Caveat (the one real gap):** ADR-0011 mandates loud quarantine and ADR-0012 retires
`dead_letter`, but no ADR names the quarantine's replacement: its table contract, its
key, whether failed items are also counted in the accounting identity's `failed`, or
who reads it. Minimal fix: one paragraph in ADR-0011 (or a short ADR-0014) declaring
`silver_enrichment_quarantine` — key, retention, and "consumer = the retry driver of
§2" — so the word "dead-letter" in ADR-0011/0012 prose has a defined referent.

---

## 2. Is `landed ∖ conformed` an adequate replacement for `dead_letter`? — **DEFECTIVE (as a *substitute*; sound as *detection*)**

What the design provides:

- **Detection: yes, soundly.** The anti-join is a blocking asset check with counts and
  stuck ids (ADR-0012 decision 7), backed by an accounting identity
  (`done + failed + in_flight + backlog == candidates`, ADR-0013) that makes silent
  under-detection detectable. `partitions.py:129` implements exactly this. Detection
  without a stored cache is strictly better than `dead_letter` (which the ADR correctly
  calls "a cache of a derivable query that can silently empty itself", ADR-0012
  Alternatives).
- **Retry mechanism: specified but ORPHANED in the plan.** ADR-0012 decisions 5–6 do
  define the retry semantics: "Retry is a NEW partition key … retry round N targets
  posts that have failed exactly N times", and "the retry budget needs no column …
  'give up' is the query `attempts >= MAX_ATTEMPTS`" (counted as bronze landing rows).
  That is a coherent bounded-retry design **on paper**. But:
  1. **No driver.** Nothing in ADR-0012/0013 or the master plan names the actor that
     creates retry-round partitions. The master plan contains ZERO matches for
     `retry`/`attempt`/`MAX_ATTEMPTS`/`backoff` (grep verified today over
     `tasks/plans/enrichment-v3-migration-master.md`). A mechanism with no driver is
     detection, not retry. The old queue at least had `fail_item()` →
     `scheduled_for` backoff → `dead_letter` (`pipelines/enrichment.md` lifecycle 4).
  2. **The attempt counter under-counts the most common failure.** Attempts = "row
     count in the append-only landing table" (ADR-0012 decision 6). A failed SUBMIT
     never lands a bronze row: `enrichment_submitted` is materialized before the
     billed call (ADR-0013), so if the POST raises, the partition is submitted with no
     handle, no landing row, and `attempts = 0` forever while `in_flight` grows
     monotonically. The `ok` column ADR-0013 mandates covers failed *harvests*, not
     failed *submits*. The design has no failed-submit story at all.
  3. `landed ∖ conformed` is also the **backlog** definition, per the identity — i.e.
     the failure set and the not-yet-harvested set are conflated in one query
     (`partitions.py:129` docstring says "failure/backlog set"). Under continuous
     ingestion those two are confounded: a slow-but-healthy backlog and a stuck
     failure are indistinguishable without the landing-row `ok` column, which ADR-0013
     only *recommends* (it is a Phase 1 exit criterion in the master plan, but not an
     ADR decision).

**Minimal design correction:** (a) name the retry driver in ADR-0012 — an interval
sensor (the same one that drives harvest) that reads `failure_set`, escalates
`attempt_round`, and stops at `attempts >= MAX_ATTEMPTS` by quarantining into the
§1 quarantine table; (b) define the submit-failure path: a failed POST must either
un-materialize `enrichment_submitted` or land an explicit failure row (the `ok`
column) so the attempt is counted; (c) promote the `ok`/status column from
"recommended fast-follow" (ADR-0012 consequences) to an ADR-0013 decision — it is
load-bearing for the accounting identity, not a nicety.

---

## 3. Does the no-ledger rule survive the Gemini path? — **SOUND-WITH-CAVEATS; blocks work until amended**

**Ownership level: yes, plain text survives.** ADR-0013's split table says the
*provider* owns its job store; for `DirectBatchAdapter` the provider is Google's Batch
API, and Google genuinely owns the job. "Dagster polls it" maps cleanly onto
`GET /jobs/{id}`-equivalent verbs. No ledger is created by having Gemini hold the job —
that is literally the "DirectBatchAdapter | a provider with its own job model | the
provider" row (`inference.md` §2).

**But the design never says where the HANDLE lives, and that is a real hole:**

- The handle is an opaque string that may be *composite* (comma-joined chunk names,
  `adapters.py:283-287`; `inference.md` §2 `ChunkedDirectBatchAdapter`).
- Today it persists in `batch_items.gemini_batch_name` (`harvest.py:74` reads the
  '|'-joined blob) — a table ADR-0012 **drops** (master.md §3 Phase 7 table).
- `partitions.py:36-40` states explicitly: "there is no stored mapping from a job
  handle to the post ids it covers" — the discovery drain derives membership from
  partition keys, which answers *in-flight*, not *poll-with-what-handle*.
- `gemini_batch.py` has no list verb (only `batches.create` / `batches.get` /
  `files.download`, `gemini_batch.py:177,238,306`), so the handle cannot be
  re-derived by listing today.
- The service path does not have this problem: `GET /jobs` lists jobs and results
  carry `custom_key` = our post ids (`inference.md` §3; `adapters.py:188-193`), so a
  partition can find its job by `custom_key` membership with zero storage on our side.

So: ADR-0013 is consistent for the *service-backed* path and **silent-to-broken for
the direct path**. The spike's S5 negative assertion proved no *ledger* is needed; it
did not prove handles are recoverable for a provider whose store cannot be queried by
our key.

**Concrete recommendation (unblocks Phase 1/2):**

Store the handle as **materialization metadata on the `enrichment_submitted`
partition** (run output metadata via `log_asset_materialization` / partition
materialization metadata). This is not a ledger and needs **no exception** to
ADR-0013: ADR-0012 decision 1 already rules "orchestration state lives in the Dagster
instance and the lake", and a per-partition metadata record is instance state, not a
bespoke table — the exact place ADR-0012 *wants* it. Concretely:

1. `submit` materializes `enrichment_submitted` with metadata `{handle: <opaque str>,
   workload, attempt_round}`. `harvest` reads the in-flight partitions' latest
   metadata to recover the handle. Restart-surviving (instance persistence), no table,
   no drift surface — the instance IS the ledger ADR-0012 endorses.
2. Add `handle_recoverable: bool` to `Capabilities` (`seam.py:82-90`): the
   service-backed adapter is recoverable via `GET /jobs` + `custom_key` even with no
   metadata; the direct adapter is not and must always rely on (1). Consumers branch
   on the capability, never the name (`inference.md` §2's own rule).
3. Belt-and-braces for the direct path: make `display_name` deterministic
   (`enrichment-<partition-key>`, already `-segN`-suffixed per chunk,
   `gemini_batch.py:176`) and add a `list` verb to `DirectBatchAdapter` so handles can
   be rebuilt by `client.batches.list` if metadata is ever lost.

Amend ADR-0013 with one paragraph: "the handle is Dagster instance state (materialization
metadata), not a ledger; providers whose store is listable are recoverable without it."
That keeps the no-ledger rule intact *in plain text* instead of by silence.

---

## 4. Layering — **DEFECTIVE** (design acyclic; violations concrete)

**The dependency DAG is sound.** `enrichment.md` §7 declares strict layer order
bronze → silver → `v_post_detail` → 21 canonical views → 4 marts, with the
metrics-centralization rule ("marts compose views, never restate") and an enforcement
mechanism (`DUCKDB_VIEWS` + `test_state_compatibility.py`). No cycle is possible by
construction. This part is genuinely well-designed.

**Violations (all verified today):**

1. `src/datalake/defs/instagram/assets.py:1170` executes
   `_cls.CLASSIFICATION_DDL` — an Instagram-domain asset creating another domain's
   silver schema (`classification.py:65` defines the `silver_content_classification`
   DDL). One domain defining and migrating another domain's table is exactly the
   sideways reach the medallion/ADR-0003 layering forbids.
2. `instagram/assets.py:1233,1288` — hidden `DagsterInstance.get()` fallbacks inside
   asset code: a global service-locator reach that bypasses the resource seam.
3. `adapters.py` (enrichment domain) imports `GeminiTierConfig` from
   `datalake.defs.instagram.config` (`adapters.py`, `DirectBatchAdapter.health()` and
   `detect_provider()`): enrichment→instagram is the wrong direction; tier gating is
   provider/billing config, not Instagram-domain knowledge.
4. `DirectBatchAdapter` (seam layer) reaches into `datalake.defs.enrichment.gemini_batch`,
   which itself builds provider SDK clients — acceptable (adapter owns its provider
   SDK) but note `gemini_batch` is also still the *legacy* direct path's module; the
   design should state that the adapter owns it exclusively post-migration.

**Minimal corrections:** move the classification DDL execution out of the Instagram
asset (it belongs to the silver conform/Phase 5 migration owner); delete the
`DagsterInstance.get()` fallbacks in favor of an injected resource; move
`GeminiTierConfig` to `defs/common/` (or make the tier gate a constructor argument to
`DirectBatchAdapter`, which also removes the enrichment→instagram import).

---

## 5. Interface adequacy: `ProviderAdapter` — **SOUND-WITH-CAVEATS**

**The three-verb Protocol is the right abstraction level.** `seam.py:94-107`
(`build_request/submit/poll/normalize_state/is_terminal/retrieve/classify_error`) plus
`Capabilities` (`seam.py:82-90`) genuinely covers both workloads: the service-backed
adapter absorbs the sync→async conversion (`inference.md` §3, "the adapter absorbs
exactly that difference"), the direct adapter owns the native job model, and the
composite-handle problem is solved inside the adapter (`adapters.py:16-19`), matching
ADR-0012 decision 9. `run_lifecycle` is provider-blind. This is not a
lowest-common-denominator trap — yet.

**But `max_tokens` proves the seam's job-level extension point is misplaced:**

- `max_tokens` is **job-level** (a property of one `submit` call: the service contract
  `POST /jobs` carries it at job level, `inference.md` §3; `gemini_batch.submit` takes
  it per submission, `gemini_batch.py:137`). `Item` (`seam.py:35-45`) is per-item and
  correctly does not carry it.
- The only extension point is `build_adapter(name, **kwargs)` → constructor
  (`seam.py:130-133`). The asymmetry is already visible in the two implementations:
  `DirectBatchAdapter.__init__` accepts `max_tokens` (`adapters.py:254`),
  `ServiceBackedAdapter.__init__` does not (`adapters.py:150-157`) and its `submit`
  body omits it (`adapters.py:178-183`) — while the service's own `POST /jobs`
  contract ACCEPTS `max_tokens`. The Protocol cannot express a parameter the
  transport already supports, and the two adapters have drifted on day one.
- Constructor injection is the wrong place for a per-PASS parameter: the visual pass
  needs 4096, the text pass may not (`master.md` §6 decision 4 measures per-workload
  budgets); one adapter instance would have to be rebuilt per pass, and `detect_provider()`
  (`adapters.py:344-348`) constructs a bare `DirectBatchAdapter()` with defaults.

**Minimal correction:** change the verb to
`submit(self, items: Sequence[Item], *, job: JobSpec) -> str` where `JobSpec` is a
frozen dataclass (`max_tokens: int | None, display_name: str = ""`). `max_tokens` moves
out of `DirectBatchAdapter.__init__`; `ServiceBackedAdapter` forwards it into the
`POST /jobs` body. Both adapters then serve every workload honestly, and the
constructor keeps only identity/transport config (base_url, model, timeout). Second
small fix: `health()` is semantically overloaded (HTTP ping vs tier gate,
`adapters.py:345-352`) — surface that as a `Capabilities` flag so consumers branch on
capability, consistent with `inference.md` §2's own rule.

One seam CAN honestly serve both providers — provided the job-level parameter moves
into the verb signature before Phase 1's "both adapters parse one Result" exit
criterion is trusted.

---

## 6. Schema/catalog coherence — **SOUND-WITH-CAVEATS**

- **Retained `ops.sqlite` set is coherent.** Every table is explicitly classified with
  fate and rationale (`master.md` §3 Phase 7 inventory: 4 DROPs, 6 RETAINs +
  `sqlite_sequence`); `prompt_registry` survives by design and is cataloged
  (`common/schemas.py:336`). No owner-less table. ADR-0012's retain list matches the
  plan's (the omission was fixed 2026-09-10 per `master.md` §6b).
- **Silver keys are consistent**: all six tables keyed `(post_id, platform)`
  (`enrichment.md` §4; `classification.py:65-68` DDL matches). The `domain`-overload
  bug is resolved at contract level (ADR-0011 decision 3).
- **Bronze idempotency is coherent with the retry design**: key
  `(post_id, platform, workload, prompt_hash, run_id)` (`enrichment.md` §3) — a retry
  round gets a new `run_id` → a new append-only row, which is exactly how
  "attempts = landing row count" (ADR-0012 decision 6) is countable. Good.
- **Gap 1 — `gold_content_shape_performance`'s PK omits `platform`.** ADR-0011 Gold §3:
  PK `(domain, topic, follower_tier, facet_name, facet_value)` — no `platform`. Silver
  is keyed `(post_id, platform)`; two platforms with the same niche/tier/facet value
  would collapse into one mart row, and the owner's second question is posed per
  platform corpus today. Add `platform` to the PK (or state explicitly that the lake
  is single-platform and the key is forward-hostile but accepted).
- **Gap 2 — the quarantine table is referenced by code** (`conform.py:77`
  `silver_enrichment_quarantine`) **but declared by no ADR** (see §1) — its schema/key
  is currently code-only, i.e. a contract that exists only in implementation.
- **Gap 3 — gold marts' provenance columns are unspecified** in ADR-0011 (silver
  envelopes carry `provider/model/prompt_hash/run_id`; the marts' spec lists only
  measures). Minor, but a mart that joins four silver tables should state which
  provenance it carries forward.

**Minimal corrections:** add `platform` to the shape-mart PK in ADR-0011; declare the
quarantine table's contract; one sentence on mart provenance.

---

## Verdict summary

| # | Question | Verdict | Minimal design fix |
|---|---|---|---|
| 1 | Producer/consumer completeness | **SOUND-WITH-CAVEATS** | Declare `silver_enrichment_quarantine`'s contract in an ADR. (`enrichment_harvested` is complete *in the design* — its missing producer is an implementation defect, not a design one.) |
| 2 | `landed ∖ conformed` vs `dead_letter` | **DEFECTIVE** as a full substitute | Name the retry driver (sensor over `failure_set` with round escalation), count failed submits as attempts (or un-materialize on failed POST), promote the `ok` column to an ADR decision, add retry to Phase 2 exit criteria. |
| 3 | No-ledger vs Gemini | **SOUND-WITH-CAVEATS — blocks work** | Amend ADR-0013: handles are Dagster **materialization metadata** on `enrichment_submitted` (instance state, not a ledger); add `handle_recoverable` to `Capabilities`; deterministic `display_name` + adapter `list` verb as fallback. |
| 4 | Layering | **DEFECTIVE** (design acyclic; violations concrete) | Move classification DDL out of `instagram/assets.py:1170`; remove `DagsterInstance.get()` fallbacks (`:1233,:1288`); relocate `GeminiTierConfig` out of `defs/instagram`. |
| 5 | `ProviderAdapter` adequacy | **SOUND-WITH-CAVEATS** | `submit(items, *, job: JobSpec)`; `max_tokens` leaves the constructor; `ServiceBackedAdapter` forwards it to `POST /jobs`. |
| 6 | Schema/catalog coherence | **SOUND-WITH-CAVEATS** | Add `platform` to `gold_content_shape_performance` PK; declare the quarantine table contract in an ADR; state mart provenance. |

**The one blocking item is #3** — until the Gemini handle's home is written down,
Phase 1's "both adapters through one lifecycle" exit criterion cannot be met without
an ad-hoc handle store re-appearing under another name, which is exactly the failure
mode ADR-0013 exists to prevent.
