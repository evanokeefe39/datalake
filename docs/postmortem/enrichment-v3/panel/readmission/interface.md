# EXPERT PANEL ROUND 2 — INTERFACE / CONTRACT ARCHITECTURE SEAT (re-admission)

Reviewer: PanelInterface (contract design, interface expressiveness, protocol adequacy). Read-only.
Subject: [`../seam-readmission-brief.md`](../seam-readmission-brief.md) — the proposal to bin Gemini usage
(retire 2 Dagster entry points, delete 9 direct call sites, keep the code) and re-admit it later through
the ADR-0008/0009 seam. Cross-checked against ADR-0008/0009/0013, `seam.py`, `adapters.py`,
`gemini_batch.py`, `submit.py`, `batch.py`, `harvest.py`, `facets_batch.py`, and the adapter/seam unit tests.

---

## 1. Findings table

| # | Claim (brief or its appealed-to interface) | Flaw | Correction |
|---|---|---|---|
| F1 | "Bring Gemini back through the seam" — the seam (three verbs + `run_lifecycle`) is the re-admission interface | **The verb set is nearly sufficient; the lifecycle shape is not.** `run_lifecycle` is a blocking, in-process loop: submit, poll ≤ `max_polls` (default 50) with an in-memory handle, retrieve, return (`seam.py:163-178`). Gemini batch jobs run for hours. The legacy path survived that fact by *persisting the handle* across processes — `set_gemini_batch_name` stores the `"\|"`-joined job names on the `batch_jobs` queue row (`submit.py:170`; `batch.py:235-269`), and the sensor/harvest read them back later (`harvest.py:300-347`). ADR-0013 deletes exactly that persistence ("no `external_jobs`", ADR-0013:42-56), and the seam offers **no verb that recovers a handle in a later process**. So "re-admit through the seam" as written means either holding one Dagster run open for hours or inventing handle persistence the ADR-0013 world forbids by name. The `max_tokens` defect is real but is the *second*-order gap; handle lifetime is the first. | The interface is sufficient for qwen (jobs live minutes; one process can block) and structurally insufficient for Gemini as specified. Re-admission requires the seam to name where the handle lives between the submit run and the harvest run — partition state (ADR-0013:48-49) carries *that a job exists*, not *its job name* — or a derivable-handle mechanism (e.g. server-side listing by `display_name` prefix; unverified, see §3). |
| F2 | The known `max_tokens` defect: "the submit verb cannot express a per-call parameter" | **Understated: there are two distinct `max_tokens`, and the seam cannot express either at call time — while the sole seam-adopting caller bypasses the seam for exactly this reason, in writing.** (a) The *output-token* cap is per-mode (`UNIVERSAL_MAX_OUTPUT_TOKENS` visual vs `1024` text) and is why `submit_facets_batch` calls `qwen_client.submit_job` directly: "``max_tokens`` is per-mode, so submit stays on the HTTP client (the seam adapter's contract is model-only)" (`facets_batch.py:310-311, 319-334`). (b) The Gemini *in-flight chunk cap* is a third parameter again (`gemini_batch.py:151-152, 167-168`), which the adapter smuggles in as a **constructor** argument (`DirectBatchAdapter.__init__(max_tokens=...)`, `adapters.py:253-257`) — a different placement than qwen's per-call body field, so "fix `JobSpec`" must express both meanings, not one. | A `JobSpec` that carries a single `max_tokens` re-creates the same defect with a new name. The seam's per-call parameter must be a structured spec with provider-declared semantics per `Capabilities`, or the second bypass is guaranteed. |
| F3 | The handle-encoding split: `"|"` (`submit.py:170`) vs `","` (`adapters.py:287` — the brief's `:280` is stale against the working tree) | **The contract permitted the split because the handle has no declared format and no owner.** The Protocol declares `submit(items) -> str` and nothing more (`seam.py:102`); no test or type constrains the string's content; "opaque to callers" (`adapters.py:16-19, 233-234`) is prose, not enforcement. With zero production callers of the seam (`seam.py` docstring; verified: the only `build_adapter` production call is `facets_batch.py:292`, for *job-state reads only*), no party had authority over the other, so both sides encoded freely. | Canonical side: `","` — in the seam world the handle lives wholly inside the adapter (submit returns it, poll/retrieve consume it) and its format is an adapter-internal invariant, already pinned by the composite round-trip test (`test_adapters.py:194-207`). `"|"` is the legacy *persistence* encoding of a larger blob — `batch.py:256-268` stores parallel `names[]` and `statuses[]` arrays joined by `"\|"`, i.e. the legacy handle carries state the seam deliberately does not (ADR-0013). Deleting the legacy path deletes the `"|"` side; the bin should keep the round-trip test as the format's owner. |
| F4 | The seam is "provider-neutral BY CONSTRUCTION" (`seam.py:1-8`) | **One provider-specific taxonomy is baked into the neutral layer: HTTP.** `_TERMINAL_STATUS` is a set of HTTP status codes (`seam.py:38`), and the shared `classify_error` policy is written in `httpx` terms (ProviderError status codes, `httpx.TimeoutException/TransportError`, `adapters.py:97-106`). `DirectBatchAdapter` has no HTTP transport — it sets `base_url = ""` explicitly "to satisfy the Protocol" (`adapters.py:259-261`) — yet inherits this classifier, under which **every** non-ProviderError, non-httpx exception (i.e. any Gemini SDK error) falls to `return TERMINAL` (`adapters.py:106`). That is the post-mortem's silent-failure polarity (#11: `classify_error`→TERMINAL) reproduced by construction at the re-admission site. | Error classification must move from the transport base class into the Protocol's per-adapter responsibility (it is already a Protocol verb, `seam.py:107`; the flaw is that the *default* lives on an HTTP base class a non-HTTP adapter must inherit). Gemini's classifier needs SDK-native exception mapping before re-admission, with a default of RETRYABLE, not TERMINAL. |
| F5 | "Two implementations" — `ServiceBackedAdapter` (qwen) and `DirectBatchAdapter` (Gemini) — so the seam already admits two providers | **The claim "admits Gemini" rests on an adapter that has never run over the wire.** `DirectBatchAdapter` is *partial*: structurally real (it delegates to `gemini_batch.submit/poll/job_state/retrieve`, the production-proven verbs used by `submit.py:152` and `harvest.py:311-334`; lazy imports verified `adapters.py:264-267, 277, 290, 319-321`), but it has **no production caller** — `detect_provider()` (`adapters.py:347-352`) has no caller outside its own module and tests, and every test instantiates it with a monkeypatched `_gemini` returning `object()` (`test_adapters.py:188-197`). Paper + fakes, never wire. | The bin's honest claim is "the seam admits Gemini *on paper and under fakes*". Re-admission acceptance must include one real submit→poll→retrieve round trip through `DirectBatchAdapter` (one chunk, one post), not merely "the adapter constructs". See §3. |
| F6 | Canonical-state vocabulary `pending/processing/completed/failed` covers both providers | **It erases a Gemini-native distinction the legacy path preserved.** `_GEMINI_STATES` maps `CANCELLED` and `EXPIRED` both to `FAILED` (`adapters.py:223-224`). Legacy semantics treated these as per-chunk facts with different dispositions (a chunk `EXPIRED` without running is not the same operator event as a model failure; the legacy harvest re-synced statuses per chunk name, `batch.py:300-309`). Worse, the composite aggregation is lossy at the logical level: **any** chunk `FAILED` ⇒ the whole handle returns `FAILED` (`adapters.py:310-311`) even if the other chunks `SUCCEEDED`, while `retrieve` still returns their per-item results. The seam's single aggregate state cannot express "3 of 4 chunks terminal — harvest those". Legacy did exactly that per chunk (`harvest.py:311-334`). | Either the canonical vocabulary gains a partial/composite concept, or the adapter's aggregation contract must be re-specified so a `FAILED` aggregate is defined as "harvest what succeeded, account the rest as failed" — today that behavior is accidental, not declared anywhere. |

### 1.1 Where the seam is genuinely provider-neutral (the demonstrated absence)

Credit where the contract holds — verified, not balanced for form:

- **`Item`** (`seam.py:49-57`): `custom_key / prompt / images / post_id / platform`. Both adapters build their native request from exactly these (`adapters.py:162-167` qwen, `adapters.py:269-274` Gemini) plus nothing provider-only. The media mapping is declared in one place (`_media_files`, `adapters.py:109-118`). No second provider-specific field is needed by either adapter today.
- **`Result`** (`seam.py:60-78`): verbatim `response_text` with explicit losslessness rationale; both adapters populate it uniformly (`adapters.py:190-200, 325-335`).
- **`Capabilities`** (`seam.py:81-90`): the one place consumers are told to branch on capability, not name — and both adapters declare honestly divergent values (tier gate, chunking: `adapters.py:140-147, 239-249`).
- **`normalize_state` + canonical vocabulary** (`seam.py:26-35, 104`): the normalization-into-one-vocabulary design is sound for *single* jobs; its flaw is only the composite aggregation (F6).

## 2. Q1 verdict — is the seam sufficient to admit a second provider cleanly?

**Verdict: the verb set is sufficient; the lifecycle contract is not. The seam was designed around
the qwen-service shape (short-lived jobs, one process can block from submit to retrieve), and Gemini
is admitted nominally: its adapter exists, is registered, is tested against fakes, and has never
carried a real job.**

The sufficiency question decomposes into three claims, each falsifiable:

1. **Verbs.** `submit / poll / normalize_state / is_terminal / retrieve / classify_error` +
   `Capabilities` (`seam.py:93-107`) express what both providers need at the *call* level — with two
   named exceptions: no per-call parameters (F2) and no composite/partial-state expression (F6).
   Both are fixable inside the existing verb signatures.
2. **Lifecycle.** `run_lifecycle` (`seam.py:145-178`) is a *synchronous convenience*, not the seam.
   It cannot express Gemini's operational model (submit in one run, harvest in another, hours apart)
   because (a) the handle has no declared home between runs (F1) and (b) ADR-0013 forbids the one
   artifact the legacy path used for that purpose. **This is the load-bearing insufficiency.** The
   brief's §3 lists handle divergence as an encoding defect; it is actually a *lifetime* defect —
   the legacy `"|"` blob existed to make the handle survive a process boundary the seam cannot cross.
3. **Neutrality.** The dataclasses are genuinely neutral (§1.1). The neutrality that is *fake* is
   error classification (F4: HTTP taxonomy on a non-HTTP adapter) and, in the aggregate-state layer,
   chunk granularity (F6).

**What would have to change in the interface** (the brief's Q1 asks this directly):

- A declared handle-lifetime mechanism: either a seam-level statement of where the handle lives
  between the submit run and the harvest run (Dagster partition metadata, a derivable server-side
  key, or an explicit amendment to ADR-0013 naming the exception), or — cheaper — a proof that the
  handle is *derivable* (e.g. Gemini job names recoverable by listing batches under the adapter's
  `display_name` namespace, `adapters.py:255`; unverified — marked inference).
- `JobSpec` on submit carrying per-call parameters with provider-declared semantics (F2: the
  qwen output cap and the Gemini chunk cap are different parameters and must not collide on one
  field name).
- Error classification promoted off the HTTP base class to a per-adapter responsibility with a
  RETRYABLE default for unknown exception classes (F4).
- A declared aggregation contract for composite handles (F6): what a `FAILED` aggregate *means*
  for harvest, stated where the test can pin it.

**Falsification:** I am wrong about insufficiency if (a) `run_lifecycle`'s handle can be recovered
in a later process through an existing mechanism I did not find, or (b) a real Gemini round trip
succeeds through `DirectBatchAdapter` with the seam's *current* shape — i.e. one Dagster run that
submits and retrieves within `max_polls` — and the team accepts holding runs open for batch
durations. Observation (b) with a documented per-run bound would make F1 moot.

---

## 3. `DirectBatchAdapter`: real, partial, or nominal?

**Classification: PARTIAL — structurally real, operationally unexercised.**

| Evidence for "real" | Evidence against "admits Gemini" |
|---|---|
| Delegates to the production-proven `gemini_batch` verbs (`adapters.py:276-335`), the same code the legacy path ran (`submit.py:152`, `harvest.py:311-334`) | No production caller anywhere: `detect_provider` (`adapters.py:347-352`) is referenced only by its own module and tests; the sole production `build_adapter` call hardcodes `"service_backed"` (`facets_batch.py:292`) |
| Correct lazy imports so the qwen path never pulls google-genai (`adapters.py:264-267, 277, 290, 319-321`; guarded by `test_adapters.py:336-340`) | Every test drives it with `monkeypatch.setattr(DirectBatchAdapter, "_gemini", lambda self: object())` (`test_adapters.py:188-197`) — the SDK boundary is a stub in every test that exists |
| The state map is complete and tested, including the unknown-state raise (`adapters.py:214-225`; `test_adapters.py:210-232`) | The tier-gate health check (`adapters.py:337-341`) is real, but nothing wires it into any run request |
| The composite-handle round trip is pinned by test (`test_adapters.py:194-207`) | Chunking under `max_batch_tokens` (`gemini_batch.py:105-128`) is production-proven, but the adapter's *aggregation* of multi-chunk handles (F6) has only ever run on two fake jobs |

**Consequence for the proposal:** the brief's plan "keep the Gemini code: `gemini_batch.py`,
`DirectBatchAdapter`, `build_adapter("gemini")"` preserves something that has never run. That is
acceptable for a *bin*, but the panel must record that the preserved artifact's readiness claim is
"unit tests pass against fakes" — so the re-admission gate in Q3 terms must be a live round trip,
not a construction check. (Registered name note: the registration is `"direct_batch"`, not
`build_adapter("gemini")` as the brief §1 states — `adapters.py:359-360`. The brief's name is
aspirational; the config string that exists today is `direct_batch`.)

## 4. Q2 — the minimum preservation set for re-admission (not a rewrite)

Concrete list. Items marked **LOAD-BEARING**: deleting them turns "bring it back through the seam"
into rebuilding the provider integration from the ADRs. Items marked **CONVENIENT**: safe to drop.

### LOAD-BEARING (loss = rewrite)

| # | Artifact | Why it is the contract, not code |
|---|---|---|
| P1 | `gemini_batch.py` in full — `submit` + `chunk_requests` + `request_estimate_tokens` (`gemini_batch.py:105-168`), `poll`/`job_state`/`is_terminal` (:235-253), `retrieve` (:259+) | The only wire-proven Gemini code in the repo; the adapter's entire substance delegates to it. The chunking cap logic encodes the tier economics (ADR-0009's cost calculus) as code. |
| P2 | `_GEMINI_STATES` map + `DirectBatchAdapter` + its registration string `"direct_batch"` (`adapters.py:214-360`) and its tests — composite-handle round trip (`test_adapters.py:194-207`), full native-state mapping + unknown-state raise (:210-232), terminal agreement (:301-310), import-isolation guard (:336-340) | This is the seam-side normalization contract, pinned by tests. The round-trip test is the handle format's *owner* after the `"|"` side is deleted (F3). |
| P3 | ADR-0009's two records: the **20 GiB File-API cap** (ADR-0009:16-22 — the reason Gemini left) and the **"executor exception, not a pattern exception" carve-out** (ADR-0009:100-104 — the architectural permission for Gemini to exist behind the seam at all) | Losing the cap record guarantees the re-admission re-hits the wall mid-corpus; losing the carve-out means re-admission has no ADR to cite and must re-litigate the seam's scope. |
| P4 | `GeminiTierConfig` + the tier gate as `DirectBatchAdapter.health` (`adapters.py:337-341`) and the loud pre-mutation gate (`submit.py:191-196`) | The capability declaration `requires_tier_gate=True` (`adapters.py:241`) is only meaningful if the gate survives. Batch is paid-tier only — this is an economic invariant, not a convenience. |
| P5 | The media contract: `Item.images` → `media_files` mapping semantics (`adapters.py:109-118`) and the media path it points at (`media_upload.py` / scrape-time byte cache) | Batch requests are media-bearing; ADR-0009's frame-sampling decision (client-side, no GCS mirror) is the reason the seam's `images` field suffices. If the cache path rots while Gemini is binned, re-admission re-opens media transport design. |
| P6 | The seam Protocol + dataclasses themselves (`seam.py:26-107`) and `test_seam.py`'s lifecycle test | The thing the proposal appeals to. Small, stable, callerless today — cheap to keep, fatal to lose (the re-admission would have no interface to re-admit *through*). |

### CONVENIENT (safe to drop — this is the point of the bin)

- The two Dagster entry points and the nine direct `gemini_batch` call sites in `submit.py`/`harvest.py` (`submit.py:152,170`; `harvest.py:300-347, 405-472`) — legacy orchestration the seam replaces. **Caveat:** harvest the *knowledge* first — the sensor's run-key signature logic (`harvest.py:462-472`) is a real design for resumable multi-chunk harvest that the seam currently lacks (F1/F6); save the idea, delete the code.
- The `ops.sqlite` `batch_jobs` persistence encoding — `"|"`-joined parallel name/status arrays (`batch.py:235-269`, `harvest.py:300-309`). Its *problem* (handle-as-blob with status smuggled in) is documented by F3; the encoding itself must not survive.
- The `gold_analyses` write path (legacy target; superseded by the silver landing under ADR-0011).
- `detect_provider()` as written (`adapters.py:347-352`) — callerless; its tier-based selection logic is worth a comment, not the function.

### The one thing that is neither

`run_lifecycle`'s current shape (F1) must be *preserved as code* (it is P6) but **must not be
treated as the re-admission vehicle**. If the return treats `run_lifecycle` as-is as "the seam
works", the re-admission becomes a bypass with extra steps. The return's first work item is
fixing the lifecycle/handle-lifetime gap (§2), using the preserved P2 tests as the regression net.

---

## 5. Bottom line

The proposal's instinct is right and the interface appeal is half right: the seam's *verbs* were
built for exactly this re-admission and `Item`/`Result`/`Capabilities` are genuinely neutral. But
the seam's *lifecycle* was built around the qwen service's job horizon, and every Gemini-specific
insufficiency (handle lifetime F1, per-call parameters F2, composite granularity F6, non-HTTP error
classification F4) sits precisely at the seam. Binning Gemini is cheap; the return is a re-admission
only if P1–P6 survive **and** the lifecycle gap is fixed as the first act of the return, with the
live round trip (one chunk, one post, real tier-1 key) as the acceptance gate — because today the
only honest readiness claim for `DirectBatchAdapter` is "passes tests against a stub".

---

*Evidence base — read in full or in targeted range: `seam-readmission-brief.md` (entire);
`src/datalake/defs/enrichment/seam.py` (entire, incl. elided ranges re-read);
`adapters.py` (entire, incl. `_HttpAdapter` 54-119); `gemini_batch.py` (submit/chunk/poll/
job_state/is_terminal/retrieve signatures + docstrings via structural + range reads);
`submit.py:94-209`; `batch.py` name/status persistence (:235-269, :300-312);
`harvest.py:300-347, 405-472` (via grep ranges); `facets_batch.py:295-343`;
ADR-0008/0009/0013 (entire); `tests/unit/enrichment/test_adapters.py` (Gemini sections,
:185-340); `test_seam.py` (lifecycle/registry tests). Grep-verified: zero production callers of
`run_lifecycle`, `detect_provider`, `DirectBatchAdapter` outside adapters.py + tests; nine direct
`gemini_batch.*` call sites outside adapters.py (submit.py:152; harvest.py:311,316,319,334,446,454,459,462).
NOT read: qwen-batch-service repo (its native state vocabulary taken on trust from
`adapters.py:121-130`'s claimed confirmation); `~/repos/enrichment-spike` (taken on trust from
seam.py docstring and ADR-0013). Inferred, marked as such: the display-name-derivable-handle
recovery option (§2) is untested speculation; the brief's stale line citations
(`adapters.py:280`, `submit.py:227`, `facets_batch.py:318,322`) read as revision drift, not
substantive error. No DB was opened (no data claims needed). Read-only throughout: no edits to
`src/` or the plan; this file is the only write.*
