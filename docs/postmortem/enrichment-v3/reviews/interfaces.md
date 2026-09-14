# Interface & Seam Adoption Review — Enrichment v3 Phase 1

READ-ONLY review of branch `feat/enrichment-v3-phase-1-seam-and-landing` (HEAD `04ea11d` + uncommitted work).
Question: does the code program to interfaces/seams/contracts, or bypass/duplicate them?

**Headline:** the seam exists, is well-built, and is almost entirely un-adopted. It is
wired into exactly one production function (poll/retrieve of the facets path). Three of
four production provider flows never touch `build_adapter`. Separately, the two halves
of the new pipeline (partition enqueue → submit) are **disconnected**: the drain
materializes `enrichment_submitted` partitions that nothing consumes, while the submit
job still reads a legacy `batch_jobs` table that nothing writes.

---

## 1. Exhaustive provider-call-site inventory (src/, excluding tests)

### A. Through the seam (legitimate — this IS the seam)
| Site | Call | Classification |
|---|---|---|
| `src/datalake/defs/enrichment/adapters.py:277-283` | `gemini_batch.submit` (DirectBatchAdapter.submit) | via seam adapter |
| `src/datalake/defs/enrichment/adapters.py:290-294` | `gemini_batch.poll` + `job_state` (DirectBatchAdapter.poll) | via seam adapter |
| `src/datalake/defs/enrichment/adapters.py:319-324` | `gemini_batch.retrieve` (DirectBatchAdapter.retrieve) | via seam adapter |
| `src/datalake/defs/enrichment/adapters.py` (ServiceBackedAdapter) | httpx to qwen service (`/jobs`, `/jobs/{id}`, `/results`, `/health`) | via seam adapter |
| `src/datalake/defs/enrichment/facets_batch.py:292` | `seam.build_adapter("service_backed", ...)` | THE one production seam adoption |
| `src/datalake/defs/common/resources.py:98` | `google.genai.Client` construction in GeminiResource | resource layer — acceptable, not a call site |

### B. Bypassing the seam (production, must migrate)
| # | Site | Call | Why it bypasses |
|---|---|---|---|
| 1 | `src/datalake/defs/enrichment/submit.py:152` | `gemini_batch.submit(gemini, _DEFAULT_GEMINI_MODEL, requests, ...)` | Direct provider call; never imports `seam`/`adapters` |
| 2 | `src/datalake/defs/enrichment/submit.py:170` | `"|".join(names)` — handle serialization | **Duplicates** the adapter's comma-join handle contract (adapters.py:280 `",".join`) — the two encodings have already diverged |
| 3 | `src/datalake/defs/enrichment/harvest.py:311` | `gemini_batch.poll` (harvest pass) | Direct |
| 4 | `src/datalake/defs/enrichment/harvest.py:316` | `gemini_batch.job_state` | Direct |
| 5 | `src/datalake/defs/enrichment/harvest.py:319` | `gemini_batch.is_terminal` | Direct |
| 6 | `src/datalake/defs/enrichment/harvest.py:334` | `gemini_batch.retrieve` | Direct |
| 7 | `src/datalake/defs/enrichment/harvest.py:446` | `gemini_batch.is_terminal` (sensor) | Direct |
| 8 | `src/datalake/defs/enrichment/harvest.py:454` | `gemini_batch.poll` (sensor) | Direct |
| 9 | `src/datalake/defs/enrichment/harvest.py:459` | `gemini_batch.job_state` (sensor) | Direct |
| 10 | `src/datalake/defs/enrichment/harvest.py:462` | `gemini_batch.is_terminal` (sensor) | Direct |
| 11 | `src/datalake/defs/enrichment/facets_batch.py:318` | `qwen_client.check_health(base_url)` | Uses the *parallel* client; adapter has `health()` which would raise-classify the same condition |
| 12 | `src/datalake/defs/enrichment/facets_batch.py:322` | `qwen_client.submit_job(...)` | Bypasses `ServiceBackedAdapter.submit` — admitted in the docstring: "max_tokens is per-mode, so submit stays on the HTTP client (**the seam adapter's contract is model-only**)" — i.e. a real interface gap: `ProviderAdapter.submit` cannot express `max_tokens` |

### C. Would need an interface (or scope) decision
| Site | Notes |
|---|---|
| `src/datalake/defs/enrichment/media_cache.py:403,543` | Direct `google.genai` SDK (`File`, `GeminiClient`) for File-API media upload. This is media, not inference, but it is a named provider outside the seam; either declare it out of the seam's scope in ADR-0008 or add a media seam. Currently undecided. |
| `src/datalake/defs/enrichment/adapters.py:264-267` | `DirectBatchAdapter._gemini()` constructs `GeminiResource()` from ambient env on every call — provider *configuration* reaches the adapter as a hidden global rather than injection (see §3). |

### Remaining-work count
**12 bypass sites across 3 modules** (submit.py: 2, harvest.py: 8, facets_batch.py: 2),
plus 1 scope decision (media_cache). By *flow*, **3 of 4 production provider flows
(submit, harvest+sensor, facets-submit) have never heard of the seam**; only the
facets poll/retrieve path uses it. `submit.py` and `harvest.py` do not import `seam`
at all; `harvest.py` never imports `adapters`. Phase 1's exit criterion
("build_adapter is the ONLY place a provider is named") is met by
`tests/unit/enrichment/test_adapters.py` against the seam's own registry — i.e. the
test proves the registry works, not that the system uses it.

---

## 2. Duplicated contract knowledge (the silent-desync inventory)

1. **Partition-key inputs (workload + attempt round).** `partitions.partition_key`
   (partitions.py:118) is one function — good — but its *inputs* are re-encoded by
   consumers: `DRAIN_WORKLOAD = landing.WORKLOAD_CONTENT_CLASSIFICATION`
   (instagram/assets.py:1023) and `DRAIN_ATTEMPT_ROUND = 0` (assets.py:1026), with
   `classification.WORKLOAD` (classification.py:60) a third alias of the same string.
   Three modules each decide which workload the classification pipeline uses and that
   round is 0. **Failure mode:** a retry round bump in one module but not another makes
   submit keys and drain keys disjoint → the guard never suppresses → silent
   double-submit (paid). No test pins the two sides to one constant. Minimal fix:
   one module owns `drain_workload()/current_attempt_round()`; consumers call it.
   (Related: nothing increments `DRAIN_ATTEMPT_ROUND` anywhere — retry is a
   docstring, not a mechanism.)

2. **Multi-chunk handle serialization.** submit.py:170 writes `names` joined with
   `"|"`; DirectBatchAdapter encodes the same concept as `",".join` (adapters.py:280)
   and harvest.py splits on `"|"` (harvest.py:301, 406) while the adapter splits on
   `","` (adapters.py:294). Two independent encodings of "one handle = several
   provider jobs". **Failure mode:** if submit ever routes through the adapter (the
   plan), a `"|"`-joined handle fed to `DirectBatchAdapter.retrieve` splits on commas
   and polls a name containing `|` → API 404s per chunk, surfaced only as repeated
   sensor warnings. Minimal fix: the adapter owns serialization; submit stores the
   adapter-returned handle verbatim.

3. **Claim transition.** submit.py:133-137 hand-rolls the `batch_jobs.status='processing'`
   update and the comment says so: "mirrors claim_batch's status update". Claim rule
   now lives in batch.py:175 *and* inline in submit.py. **Failure mode:** the claim
   invariant (e.g. status guard, attempts reset) changes in `claim_batch` and the
   inline copy silently diverges. Minimal fix: call `claim_batch` (or delete one).

4. **qwen service contract, whole.** `qwen_client.py` (health:54, submit:70, get_job:109,
   get_results:128, job_is_terminal:148) is a complete second HTTP client for the same
   service that `ServiceBackedAdapter` already wraps — including a second terminal-state
   rule (`job_is_terminal` vs adapter `is_terminal`/`normalize_state` maps). facets_batch
   uses client for submit/health and adapter for poll/retrieve: split-brain against one
   service. **Failure mode:** the service adds a state or changes a route; the adapter
   map is updated and `qwen_client.job_is_terminal` is not → submit works, the *other*
   terminal predicate disagrees, jobs hang or harvest early. Minimal fix: facets submit
   goes through `build_adapter("service_backed").submit(...)`; the seam interface gains
   `max_tokens` (e.g. an `Item`/request field), and `qwen_client` reduces to the
   adapter's transport or is deleted.

5. **`(post_id, platform)` join key.** Declared in conform.py:88 `KEY_COLUMNS`, but
   re-encoded as DDL prose in classification.py:85 (`PRIMARY KEY (post_id, platform)`),
   each facets/analyses DDL comment, and hardcoded `platform = 'instagram'` literals in
   serving SQL (serving/assets.py:261, 1338-1342 joins by string). Single implementation
   of the *columns*, but the *values* are literals sprinkled in queries. **Failure mode:**
   a second platform joins on literal `'instagram'` and reads the wrong rows silently —
   the exact `domain`-vs-`platform` bug class classification.py:10 warns about, recreated
   in SQL. Minimal fix: a `platform` constant per domain module; queries parameterize.

6. **`_TERMINAL_STATUS` private cross-import.** adapters.py imports seam's *private*
   `_TERMINAL_STATUS` (seam.py) — the classification rule has one home (good) but is
   consumed through a private name, so the seam can refactor it away with no signal
   until adapters breaks at import. Minimal fix: rename public (`TERMINAL_STATUS`).

7. **Clean, for contrast:** prompt hashing is single-sourced (prompts.py:39-66
   delegates to `seam.prompt_identity` / `_v1`) — the only contract that got the
   "one function" treatment fully.

---

## 3. Hidden globals and implicit state

| Site | What | Verdict |
|---|---|---|
| `src/datalake/defs/instagram/assets.py:1233` | `instance = DagsterInstance.get()` fallback inside the in-flight guard of the **production** asset `ig_posts_gen_batches` | **Production global.** If Dagster home is unset or the ephemeral instance differs from the one submit wrote to, the guard reads the *wrong* instance → suppression silently no-ops → double-submit. The asset signature already accepts `instance: PartitionSnapshot | None`; the fallback removes the guarantee. Minimal fix: require the injected instance (raise if None) or resolve it once in Definitions wiring. |
| `src/datalake/defs/instagram/assets.py:1288` | Second `DagsterInstance.get()` fallback before `_materialize_submitted_partitions` | Same defect, worse: the enqueue and a *later run's* guard must read the SAME instance for suppression to ever work; two independent `.get()` calls don't guarantee that. |
| `src/datalake/defs/enrichment/adapters.py:265-267` | `GeminiResource()` constructed from ambient env inside the adapter | Provider identity (API key/project) enters implicitly. A test or a second workspace silently uses whatever `GEMINI_API_KEY` is in the process env. Minimal fix: take the resource (or key) as an `__init__` arg like `base_url` already is. |
| `src/datalake/defs/enrichment/adapters.py:160` | `os.environ.get("QWEN_SERVICE_URL", DEFAULT_QWEN_SERVICE_URL)` in `ServiceBackedAdapter.__init__` | Acceptable pattern (config with explicit override), but inconsistent with `_gemini()` — one dependency is injectable, the other is env-read. Pick one. |
| `src/datalake/defs/instagram/assets.py:1026` | `DRAIN_ATTEMPT_ROUND: int = 0` module constant | Global retry policy frozen at 0 (see §2.1). |

---

## 4. Layering violations

1. **`src/datalake/defs/instagram/assets.py:1170`** — `conn.execute(_cls.CLASSIFICATION_DDL)`
   with `from datalake.defs.enrichment import classification as _cls` imported *inside the
   function*. An Instagram asset creates and depends on another domain's silver schema,
   invisibly to the asset's `deps`. **Failure mode:** the classification DDL changes
   (new column, new PK) and the Instagram asset's copy-of-truth assertion passes while
   conform fails at write time; or the import is "cleaned up" and the drain crashes
   against a missing table. **No test would catch it.** Minimal fix: classification.py
   owns an `ensure_tables(conn)` (it already owns the DDL); the asset calls that — or
   better, a dedicated `silver_content_classification` ensuring asset becomes a `deps`
   entry. (Note: a `silver_content_classification` asset exists — the asset should
   depend on it, not re-run its DDL.)

2. **`src/datalake/defs/enrichment/media_upload.py:44-66`** — `_API_MARKERS` /
   `_PURE_MODULES` encode the ADR-0008 seam rule inside production code as an
   honor-system lint that runs at op time. The rule's owner is the test suite, not a
   runtime module. Low harm, but it is contract knowledge with no consumer in
   production. Minimal fix: move the marker scan to a unit test.

3. **`src/datalake/defs/enrichment/__init__.py:8`** — re-exports `create_batch` while
   batch.py:126's own comment says its callers "migrate submit to partitions".
   The package's public surface advertises a retired path. Minimal fix: drop the export
   when the legacy queue retires (see §6).

---

## 5. Error-handling quality

1. **`_HttpAdapter.classify_error` (adapters.py ~100): silently defaults unknown
   exceptions to TERMINAL.** Any exception that isn't a `ProviderError` or an httpx
   transport error (e.g. a `KeyError` from a malformed service response, a `TypeError`
   from a bug) is classified terminal → **no retry, no diagnostic** distinguishing
   "caller's fault" from "our bug". A transient-looking `json.JSONDecodeError` from a
   proxy would permanently fail the item. Minimal fix: unknown types default
   RETRYABLE *and log the exception type*, or raise for unmapped types. (No test pins
   the unknown-exception branch.)
2. **`normalize_state` — both adapters raise `ProviderError` on unknown state
   (adapters.py ~196, ~290).** Correct: loud, no silent default. Pass.
3. **harvest.py:305-309, 336-340** — poll/retrieve exceptions caught, `logger.warning`,
   `all_terminal = False`, continue. A job whose name is permanently wrong (or whose
   chunk 404s) is re-polled every sensor tick forever: **no attempt counter, no
   escalation, no dead-letter**. The drain-side `MAX_ATTEMPTS` machinery exists but
   this loop never drives it. **Failure mode:** silent stall; you notice only via a
   dashboard that never advances. Minimal fix: count consecutive poll failures per
   name and route to `fail_item`/quarantine at a bound.
4. **gemini_batch.py `_parse_inlined` (~296):** missing `custom_key` becomes the
   literal key `"?"` — two such responses collide and `merged.update`
   (adapters.py:325 / harvest apply) **silently drops all but the last**. Minimal
   fix: synthesize a unique key (index) and log.
5. **`ServiceBackedAdapter.health` returns False on any HTTPError** (adapters.py ~227)
   while facets uses `qwen_client.check_health` which *raises*. Two behaviors for the
   same question; the adapter's False is a silent fallback (and unused in production).
   Consolidate with the adapter (see §2.4).
6. **`src/datalake/defs/instagram/asset_checks.py:53`** — `pl.read_parquet` failure →
   `None` → check treats as "nothing written": a corrupt bronze file makes a check
   *pass vacuously* rather than fail loudly. **:199** — per-file read failures
   silently skipped, skewing the bronze count. Minimal fix: catch narrow
   (`FileNotFoundError`), else fail the check with the path named.
7. **`src/datalake/defs/serving/assets.py:56`** and **media_cache.py:72,481** —
   `except Exception: pass` around ALTER/metadata reads. The ALTER cases are
   idempotent-migration patterns (acceptable, though `except sqlite3.OperationalError`
   would be honest); media_cache.py:481 swallows *any* error reading video duration —
   a silent `duration=None` feeding token-budget estimation (underestimates caps).
   Narrow the catch.

---

## 6. Dead or unreachable code left by the partial migration

1. **`seam.run_lifecycle` (seam.py:145-178)** — zero production callers
   (`grep run_lifecycle src/` → seam.py only; tests only). The "ONE lifecycle" exists
   solely in tests. Either wire submit/harvest through it (the actual point) or it's
   a shelf.
2. **`seam.prompt_identity_v1` (seam.py:195) + `prompts.legacy_prompt_hash`
   (prompts.py:56-66)** — self-described "kept ONLY for migration comparison". The
   comparison period needs a removal date; otherwise it is a second hash rule someone
   will call.
3. **The whole legacy queue read-path: `batch.py` `create_batch`/`claim_batch`/
   `complete_item`/`fail_item`/`dead_letter`/`_require_legacy_queue_tables`
   (batch.py:128-500, guard at :51).** Only production *readers* remain (submit.py,
   harvest.py, media_upload.py). The only writer left is
   `scripts/smoke_test_media_e2e.py:145`. **This is the load-bearing finding:** the
   drain's new enqueue (partition materialization, instagram/assets.py:1289) writes
   Dagster partitions; `submit_gemini_batches_job` (submit.py:77) still SELECTs from
   `batch_jobs`, which nothing in production creates. The two halves never meet:
   enqueued posts produce no submission; `batch_jobs` reads an empty table;
   `SUBMITTED_PARTITIONS`/`HARVESTED_PARTITIONS` (partitions.py) have no consumer
   outside the drain itself — so the drain's own guard suppresses everything after
   round 1 and the pipeline stops after one cycle. `_require_legacy_queue_tables`
   actively *preserves* the zombie contract by raising when the tables are missing.
   Minimal fix: submit's discovery reads the instance's `enrichment_submitted`
   partitions (the seam the drain already writes), or the drain enqueues to the queue
   — one direction, not both.
4. **`enrichment/__init__.py` legacy exports** (`create_batch`, `mark_complete`) —
   advertise retired paths (see §4.3).
5. **`media_upload.py:53 "gemini_batch."` in `_API_MARKERS`** — fine, but note
   `_PURE_MODULES` references `datalake.defs.enrichment.assets`, a module in which
   the marker set itself says no API call may appear; the guard data is otherwise
   unenforced (see §4.2).

---

## 7. Ranked list — what bites first

| # | Defect | Bites when | Minimal fix |
|---|---|---|---|
| 1 | **Disconnected pipeline halves:** drain enqueues partitions nobody consumes; submit reads `batch_jobs` nobody writes (instagram/assets.py:1289 vs submit.py:77; `_require_legacy_queue_tables` batch.py:51 keeps the zombie alive) | First real drain run after this branch: items enqueued, never submitted, guard then suppresses them forever. No test catches it because both halves are tested with their own fakes. | Make submit's discovery read `enrichment_submitted` partitions (the interface the drain already writes); delete or stop calling `_require_legacy_queue_tables`. |
| 2 | **`enrichment_harvested` has no writer** → in-flight = submitted − ∅ grows monotonically (partitions.py:103-104, 205-236) | After run 1, every candidate is "in flight" forever; discovery stops permanently; double-submit test passes vacuously. | Harvest (or conform) reports the `enrichment_harvested` materialization at the same key the drain derives. |
| 3 | **12 provider call sites bypass the seam** (§1); `submit.py`/`harvest.py` don't import seam at all | Provider swap/addition requires rewriting submit + harvest; the exit criterion is unmet though green tests suggest otherwise. | Port harvest first (it maps 1:1 onto adapter poll/normalize_state/retrieve), then submit; add a grep test asserting `gemini_batch.`/`qwen_client.` appear only in `adapters.py`/`gemini_batch.py`/`qwen_client.py`. |
| 4 | **Two HTTP clients for the qwen service** (qwen_client.py vs ServiceBackedAdapter) with two terminal predicates (§2.4) | Service adds a state or route → one client updated, one not → submit succeeds, harvest hangs or misclassifies. | Add `max_tokens` to the seam contract; facets submit via `build_adapter`; collapse qwen_client into the adapter's transport. |
| 5 | **`DagsterInstance.get()` fallbacks** (instagram/assets.py:1233, 1288) | Guard/enqueue silently use a different instance than the one jobs run on → suppression no-ops → double-submit (paid). | Make `instance` required; wire once in Definitions. |
| 6 | **Handle serialization duplicated and divergent** (`"|"` submit.py:170 vs `","` adapters.py:280) | The moment submit routes through the adapter, every composite handle breaks at poll. | Adapter owns serialization; submit persists the returned handle. |
| 7 | **Partition-key inputs duplicated** (DRAIN_WORKLOAD/DRAIN_ATTEMPT_ROUND vs classification.WORKLOAD; round hardcoded 0) (§2.1) | First retry round, or a workload rename, silently desynchronizes guard and submit → double-submit or permanent suppression. | One owner function for (workload, round); consumers call it. |
| 8 | **Layering violation:** instagram asset executes `classification.CLASSIFICATION_DDL` (assets.py:1170) | Schema evolution of classification breaks the drain asset or silently drifts from the owning asset. | classification.ensure_tables(conn) called by the owning asset; drain depends on that asset. |
| 9 | **`classify_error` silently defaults unknown exceptions to TERMINAL** (adapters.py ~100) | Any non-httpx bug exception permanently fails items with no retry and no distinguishing diagnostic. | Default RETRYABLE + log; pin with a unit test on the unknown-exception branch. |
| 10 | **Poll/retrieve failures in harvest are warn-and-continue with no bound** (harvest.py:305, 336) | A bad job name or quota failure loops every sensor tick forever. | Per-name failure counter → fail_item/dead-letter path. |
| 11 | **`"?"` custom_key collision in `_parse_inlined`** (gemini_batch.py:296) | Responses silently overwrite each other; conformed row count < landed count. | Unique synthetic key + log. |
| 12 | Dead code: `run_lifecycle`, `prompt_identity_v1`/`legacy_prompt_hash`, legacy queue functions + `_require_legacy_queue_tables`, `__init__` exports (§6) | Ambient confusion; someone calls the second hash rule or the retired queue. | Delete after #1/#3 land; date the v1-hash removal. |

**Test-coverage observation:** every green test in this area injects a fake on *one*
side of the seam (fake instance, fake service, fake registry) — none asserts the two
production halves connect (partition enqueue → submit discovery; submit → harvest).
That is why #1 and #2 are invisible. The cheapest guard is an integration-shaped unit
test that runs drain-enqueue and submit-discovery against the SAME fake instance and
asserts a nonempty handoff.
