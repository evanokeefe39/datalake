# Post-mortem: implementation drift in the Enrichment v3 migration

Independent root-cause analysis, 2026-09-13. Branch `feat/enrichment-v3-phase-1-seam-and-landing`
(6 commits since `main`). Question: **why did the implementing subagents fail to build
to the given design and plan?** This is a causal analysis, not a defect list — the defects
themselves are cataloged in
[`reviews/interfaces.md`](reviews/interfaces.md),
[`reviews/architecture-soundness.md`](reviews/architecture-soundness.md), and
[`reviews/round-semantics.md`](reviews/round-semantics.md), which this document builds on and
does not duplicate.

> **READ THE CORRECTIONS BEFORE QUOTING THIS DOCUMENT.** This analysis was reviewed by a
> four-seat expert panel and the adversarial seat overturned parts of it. Three findings were
> revised after the fact and the revisions are load-bearing:
>
> - **§9 retracts a claim in §8.4.** The assertion that "no unit was ever briefed to migrate the
>   seam call sites" was an inference from the commit trail presented as a dispatch record. The
>   evidence does not discriminate assignment failure from a half-executed brief. **The
>   orchestrator-vs-worker verdict for that defect is UNDETERMINED.**
> - **The headline verdict below is overstated.** "Per-component dispatch plus per-component
>   acceptance" is **non-discriminating**: it is present in every migration this process has run,
>   including successful ones. The discriminating form is the compound — unowned contracts **and**
>   no acceptance gate capable of observing a real run (`reviews/…`; see `analysis/learnings-mece.md`
>   §6). The proximate cause is **acceptance**, not dispatch shape.
> - **§10 is the load-bearing section.** The rules were not missing — they were not *binding*.
>
> Section §5 states the original verdict; read §9 and §10 with it.

**Original headline verdict, retained for the record: the orchestrator's decomposition and
verification process was the dominant cause. The workers executed their dispatches competently;
the dispatches and the phase-acceptance instrument were structurally incapable of producing an
integrated pipeline.** Detail below; summary in §5, and the corrections in §9–§10.

---

## 1. What each subagent was likely told, and what it could and could not see

The commit trail (main..HEAD, oldest first) maps cleanly onto per-component dispatches:

| Commit | Likely dispatch | Agent could see | Agent could NOT see |
|---|---|---|---|
| `ef15765` seam + bronze landing | "Port the inference seam (ProviderAdapter Protocol, `build_adapter`) and add the verbatim bronze landing" (`inference-service-seam.md` sub-plan) | Its own module, the spike's adapter design, ADR-0008/0013 | Whether any production caller would ever adopt `build_adapter` or `run_lifecycle`. A module author cannot know its future callers; nothing in the dispatch named them. Result: `run_lifecycle` has **zero production callers** ([`reviews/interfaces.md`](reviews/interfaces.md) §6.1) — built to spec, adopted by no dispatch. |
| `872904e` "converge both lifecycles onto one seam" | "Converge the lifecycles" | The facets path (`facets_batch.py`), which it converged onto the seam (the ONE adoption at `facets_batch.py:292`) | That the Gemini path (`submit.py`, `harvest.py` — 8 of 12 bypass sites live in `harvest.py`) was not in its remit. "Both lifecycles" was not unpacked into "and every call site of them." The agent converged one lifecycle and reported success; that is a faithful reading of the dispatch, not a mistake. |
| `b38465b` prompt identity + silver conform | "Bind prompt+schema, never the model; build the conform layer" | `prompts.py`, `conform.py`, bronze schema | Whether the drain/submit would agree with its hash semantics. (This contract, notably, *did* get the one-function treatment and is clean — the only one that did, per [`reviews/interfaces.md`](reviews/interfaces.md) §2.7.) |
| `4e9cf4b` + `c9e8724` classification + drain | "Derive in-flight from the Dagster instance" | `ig_posts_gen_batches`, `partitions.py`, the instance snapshot | Whether anything would ever **consume** the `enrichment_submitted` partitions it materializes (`assets.py:1289`), and whether anything would ever **write** `enrichment_harvested` (nothing does). The agent derived the guard from the instance exactly as told; that `submit.py:77` still SELECTs `batch_jobs` — a table nothing writes — is invisible from its slice. |
| `04ea11d` serving rebind + gold marts | "Rebind classification consumers; build four gold marts" | Views, marts, `test_state_compatibility.py` | The pipeline upstream; it is correctly out of scope for this slice. |

The common shape: **each dispatch named a module to build, and none named a contract
to keep.** An agent told to build a module cannot know whether its caller will use it;
an agent told to derive in-flight from the instance cannot know that the other half of
the pipeline reads a retired table. Under this decomposition shape, the observed
outcomes (built-but-unadopted seam, disconnected halves, missing harvested writer) are
the *expected* products, not execution failures.

## 2. Five Whys on the central failure

**Central failure:** components were built and verified individually while the
contracts between them went unbuilt and untested — drain enqueues to Dagster
partitions that nothing consumes, `submit.py` reads a `batch_jobs` table nothing
writes, `enrichment_harvested` has no writer, the seam is bypassed by 12 sites, and
every one of these landed green.

**Why 1 — Why did the two pipeline halves end up disconnected?**
Because the drain slice and the submit slice each implemented what its dispatch said
against the state that existed in front of it: the drain materializes
`enrichment_submitted` partitions (`instagram/assets.py:1289`), while
`submit_gemini_batches_job` still discovers work in `batch_jobs` (`submit.py:77`) and
`_require_legacy_queue_tables` (`batch.py:51`) actively raises if that retired table is
absent — the zombie contract is *preserved by code*. No slice was asked to own the
handoff. Evidence: [`reviews/interfaces.md`](reviews/interfaces.md) §6.3 ("the two halves never meet") and §7
defect 1.

**Why 2 — Why did no slice own the handoff?**
Because decomposition was per-component, and integration was nobody's dispatch. The
master plan *knew* the cross-dependencies — §4's consumer/blast-radius register names
`ig_posts_gen_batches` as Phase 2's consumer, and Phase 2's prose explicitly says "the
drain rewrite must land **with** the queue retirement, in this phase" — but the queue
**retirement** (dropping writers) was scheduled in Phase 7, and no exit criterion in
any phase says "submit consumes what the drain produces." The plan converted a
cross-phase coupling into per-phase component criteria, and no dispatch carried the
join. Evidence: master plan §3 Phase 2 vs Phase 7 table; the plan's own §8 correction
(2026-09-13) admits the root cause structurally: "every test injected a fake on ONE
side of a seam."

**Why 3 — Why did per-phase verification accept all of this?**
Because the verification instrument for each phase was the phase's own unit tests, and
every one of those tests injects a fake on one side of the seam (fake instance, fake
service, fake registry). The drain test proves the drain works against a fake instance;
the submit test proves submit works against a fake `batch_jobs`; nothing asserts a
nonempty handoff between the two real halves. Evidence: [`reviews/interfaces.md`](reviews/interfaces.md) §7
("Test-coverage observation") verbatim: "none asserts the two production halves
connect... That is why #1 and #2 are invisible." The orchestrator accepted
phase-complete reports on the strength of green suites — 701 tests passing while the
pipeline as a whole cannot run one cycle (the drain's own guard suppresses everything
after round 1 because nothing retires the in-flight set).

**Why 4 — Why did the plan's exit criteria fail to force the integration?**
Because the criteria are phrased as **existence claims about components**, not
**round-trips between them**. "`build_adapter` is the ONLY place a provider is named"
was met by a registry unit test while 12 production call sites bypass the seam — the
criterion as written is satisfiable without any production adoption. "Two consecutive
drain runs enqueue no post twice" passes because run 2 enqueues nothing at all
(suppression is total — the guard is broken, and the test cannot tell). "One submit
fans out to both visual tables" has no production caller exercising it. Every
major defect in this migration is an existence criterion passing green against an
abstraction. The plan itself now concedes this: §8 ("Verification strategy — corrected
2026-09-13 **after independent review**") is an orchestration-layer admission that the
original verification strategy was "assert an existence, never a round-trip," and
specifies the four assertions that should have existed from day one.

**Why 5 — Root cause.**
**The coordination plan treated integration as an emergent property of component
completeness rather than a first-class, owned, tested deliverable.** It decomposed by
component, accepted by component-green tests, and assigned the seams between components
— the only place this migration's risk actually lives (§4's own register names the
drain as the load-bearing cross-phase consumer) — to nobody. Per-component dispatch
under per-component verification cannot, even with perfect worker execution, detect or
prevent inter-component drift; the failure was therefore determined at dispatch design
time, before any worker saw a prompt. The workers' tests passed because their tests
were true statements about their slices. The orchestrator's mistake was believing that
component truth composes into system truth.

## 3. Failure-mode classification

Classes: **(a)** dispatch/decomposition shape, **(b)** verification instrument,
**(c)** task instructions to workers, **(d)** worker execution, **(e)** plan/spec
itself. Most defects have a primary class and contributing classes.

| # | Defect | Class | Reasoning |
|---|---|---|---|
| 1 | Pipeline halves disconnected: drain → partitions nobody consumes; `submit.py` reads `batch_jobs` nobody writes; `_require_legacy_queue_tables` keeps the zombie alive | **(a) primary; (e)** — the plan put queue retirement in Phase 7 while rewiring the drain in Phase 2, and no criterion demanded a round-trip; workers built each half to spec | Orchestrator fault. Neither worker's slice contained the other half. |
| 2 | `enrichment_harvested` has no writer → in-flight grows monotonically, discovery stops after one cycle | **(a)+(c)** — the dispatch "derive in-flight from the instance" named only the submitted side; no slice was told to materialize the harvested side. Design is sound (review-architecture §1: "complete in the design — the code's missing producer is an implementation gap") but the *implementation gap* traces to an unassigned producer, not to the slice worker | Orchestrator fault in the main; a correct dispatch would have named the writer. |
| 3 | `run_lifecycle` zero production callers — Phase 1's stated purpose (converge both lifecycles) unfulfilled | **(a)+(c)** — "converge the lifecycles" produced the convergence function, but the Gemini-path rewiring (12 call sites in `submit.py`/`harvest.py`) was in no dispatch | Orchestrator fault. The agent that built the shelf was not the agent with authority over `submit.py`/`harvest.py`. |
| 4 | 12 provider call sites bypass the seam (3 of 4 flows never touch it) | **(a) primary** — per-module dispatches meant no dispatch included "migrate `submit.py`/`harvest.py` call sites"; **(b)** the registry test proved the registry, not adoption | Orchestrator fault. Note `facets_batch.py:322` bypass is class (e)/(latent) — see §4. |
| 5 | Two complete HTTP clients for the qwen service (`qwen_client.py` vs `ServiceBackedAdapter`) with divergent terminal predicates | **(a)** — split-brain across slices: facets submit stayed on the client, facets poll/retrieve moved to the adapter | Orchestrator fault; the seam adoption was partial because the dispatch boundary cut the flow in half. |
| 6 | Handle serialization diverged: `"|"` (submit.py:170) vs `","` (adapters.py:280) | **(a)+(b)** — two slices each encoded "one handle = several jobs"; no round-trip test forced them to agree | Orchestrator fault; the divergence is invisible to any single-side test. |
| 7 | Partition-key round-0 hardcode (`DRAIN_ATTEMPT_ROUND=0`) → latent double-submit under retry | **(e) primary** — ADR-0012 decision 5 mandates key *distinctness* but is silent on key *shape*; the digest fusion was a module-level choice the plan never reviewed ([`reviews/round-semantics.md`](reviews/round-semantics.md) §5); **(c)** — the worker cited decision 5 in its docstring and implemented the guard honestly for round 0 | Design/plan fault; the worker's implementation matches its remit. |
| 8 | Vacuous exit-criterion tests (registry test; "no post twice" passing on total suppression) | **(b)** — the verification instrument accepted existence proofs as adoption/convergence proofs | Orchestrator/instrument fault. |
| 9 | Retry semantics orphaned: no driver, no round minter, failed submits count zero attempts | **(e)** — master plan has zero matches for retry/attempt/backoff ([`reviews/architecture-soundness.md`](reviews/architecture-soundness.md) §2.1); ADR-0012 defines the mechanism on paper and names no actor | Plan/ADR fault. |
| 10 | Layering violations: `instagram/assets.py:1170` executes another domain's DDL; two `DagsterInstance.get()` fallbacks in production guard code; `adapters.py` imports `GeminiTierConfig` from instagram config | **(d) + (e)** — the `.get()` fallbacks are genuine worker shortcuts (the asset signature already accepted an injected instance; the fallback removes a guarantee to make a test easier); the DDL reach and tier-config import are slice-boundary shortcuts no test caught | Mixed: worker shortcut (fallbacks), plan blind spot (cross-domain reach). |
| 11 | Silent-failure paths: `classify_error` defaults unknown exceptions to TERMINAL; harvest poll failures warn-and-continue forever; `"?"` custom_key collision; asset checks pass vacuously on unread parquet | **(d)** — worker execution quality; all are inside single slices, detectable by any careful worker review | Worker fault (quality), uncaught because the orchestrator's acceptance did not include adversarial error-path review. |
| 12 | Dead/zombie code: legacy queue read-path, `prompt_identity_v1`, legacy `__init__` exports | **(a)+(c)** — retirement was Phase 7 but each slice left its own leftovers; no dispatch said "delete what you supersede" | Orchestrator fault in acceptance scope; workers did not overstep. |
| 13 | `max_tokens` job-level vs per-item seam (caused facets bypass) | **(e)** — real interface defect; see §4 | Design fault, worker handled it correctly. |

Summary: **(a) and (b) account for every system-level failure** (1–6, 8, 12). **(d) is
confined to slice-local quality issues** (10 partial, 11). **(e) covers the design
gaps** (7, 9, 13). The workers did not fail their remits; the remits failed the system.

## 4. Latent-tension cases: worker shortcut vs real interface defect

**Real interface defects the worker had no authority to fix — worked around correctly:**

- **`max_tokens` (facets_batch.py:322).** The seam's `Item` is per-item; `max_tokens`
  is job-level; the only extension point was constructor kwargs, which cannot express
  a per-pass parameter (review-architecture §5 shows the two adapters had drifted on
  day one for exactly this reason). The worker **documented the bypass in the
  docstring** with the precise reason. This is not a shortcut — the seam's verb
  signature is defective (`submit` needs a `JobSpec`), and fixing it requires design
  authority the worker's remit did not include. Verdict: **worker correct, interface
  defective, escalation obligation was the orchestrator's.**
- **`DRAIN_ATTEMPT_ROUND = 0` digest fusion.** ADR-0012 decision 5 mandates distinct
  retry keys but says nothing about key shape; the digest shape was a module-level
  design choice that made round-complete guarding impossible (review-round-semantics
  §2, §5). The worker implemented the guard honestly for round 0 and cited the ADR in
  the docstring. Verdict: **worker correct given an underspecified interface; the
  structural hole is a design gap** requiring the option-(a) interface decision.
- **`enrichment_harvested` missing producer.** [INFERENCE from dispatch shape] the
  dispatch said "derive in-flight from the instance," which reads as a guard-side
  instruction; no producer instruction existed. Not a shortcut — an unassigned side of
  a contract.

**Genuine worker shortcuts:**

- **`DagsterInstance.get()` fallbacks** (`instagram/assets.py:1233,1288`). The asset
  signature *already accepts* an injected instance; the fallback is a convenience that
  silently removes the guarantee that guard and enqueue read the same instance. The
  worker had everything needed to do it right and chose the permissive path. Verdict:
  **worker shortcut**, made worse by the review finding two of them.
- **Silent-failure defaults** (`classify_error` → TERMINAL on unknown exceptions;
  warn-and-continue poll loops with no bound; vacuous asset-check reads). These are
  slice-local and were within the worker's remit to get right. Verdict: **worker
  execution quality**, though a "loud failure" rule exists in the plan's §5 invariants
  — meaning the plan told them and the acceptance never checked.

## 5. Verdict: orchestrator vs workers

**The orchestration was the dominant cause, and this postmortem does not protect it.**

- Every system-level defect (disconnected halves, missing harvested writer, unadopted
  seam, dual clients, divergent handle encoding, vacuous green suites) is fully
  explained by per-component dispatch + per-component acceptance. No worker, however
  excellent, could have caught these from inside their slice.
- The workers' genuine failings (instance fallbacks, silent-failure paths, dead code)
  are real but second-order: they are quality defects inside slices, each caught by a
  normal code review of the slice, none load-bearing for the migration's failure.
- The plan itself contributed at two levels: exit criteria phrased as existence rather
  than round-trips, and known cross-phase dependencies (§4's register — which *named*
  the drain as Phase 2's consumer) converted into per-phase checkboxes instead of
  per-dispatch contracts. The plan even pre-wrote its own postmortem: §8, added after
  the fact, contains the sentence that should have been in the dispatches from the
  start — "Assert a ROUND-TRIP, never an existence."

## 6. Signals that would have caught each defect at dispatch time

| Defect | Signal that would have caught it |
|---|---|
| 1 Disconnected halves | One integration-shaped test per phase boundary: run drain-enqueue and submit-discovery against the **same** fake instance and assert a **nonempty handoff** (produce → consume → compare). Equivalently: a dispatch-time question — *"name the production consumer of everything you write; name the production producer of everything you read."* |
| 2 Missing harvested writer | Dispatch must name every state space the agent's change produces AND consumes; acceptance requires demonstrating the producer fires (a materialization observed, not a function existing). |
| 3 `run_lifecycle` unadopted | Acceptance question: "show `run_lifecycle` on a stack trace from the production entry point." A seam-adoption test that runs the real entry point and records an adapter call. |
| 4 Bypass sites | Grep-shaped CI test: provider names (`gemini_batch\.`, `qwen_client\.`) appear only in adapter modules. Dispatch for "converge the lifecycles" must enumerate the call sites being converged (the plan's own §1 evidence table listed them — it was never handed to the worker). |
| 5 Dual qwen clients | Same grep test plus: "one transport per external service" as a per-dispatch contract; the dispatch for the facets slice should have included deleting `qwen_client` submit usage. |
| 6 Handle divergence | Round-trip test on the handle with a **multi-chunk** case — single-chunk passes under both encodings (plan §8 assertion 1). |
| 7 Round-0 double-submit | At design time: "for each derivable state (in-flight, failed, backlog), can a consumer answer it without knowing a hidden input?" — i.e., review the key shape against the consumer before the guard is built; a retry-shaped test (round-1 in flight) at the drain. |
| 8 Vacuous tests | Test-review question at acceptance: "what would this test have to look like to FAIL?" If no plausible bug fails it, the criterion is vacuous. Also: any criterion worded as existence ("is the ONLY place", "is the ONE lifecycle") must be paired with an adoption assertion. |
| 9 Orphaned retry | Plan-review check: every ADR mechanism (retry, quarantine, failed-submit) must name its **driver** (actor + trigger) before phases are dispatched. |
| 10 Layering violations | Cross-domain reach check per dispatch: "list every module you import that is not in your assigned slice" — imports outside the slice get flagged at PR, not at review. |
| 11 Silent failures | Acceptance includes the plan's own §5 invariant ("Loud failure") applied to each error path; adversarial review question: "what happens when the provider returns garbage / the response is malformed?" |
| 12 Zombie code | Per-dispatch acceptance: "delete everything you superseded" as an explicit deliverable, not deferred to a later phase. |
| 13 `max_tokens` seam gap | Interface-review at seam design time: enumerate per-call parameters the transport supports (`POST /jobs` carries `max_tokens`) and check the Protocol can express each. This was catchable at ADR-0008 review, before any dispatch. |

## 7. Evidence index

- [`reviews/interfaces.md`](reviews/interfaces.md) §1 (12 bypass sites, one adoption), §2 (desync inventory), §3 (hidden globals), §4 (layering), §5 (silent failures), §6 (dead code incl. `run_lifecycle` zero callers, disconnected halves), §7 (ranked defects + test-coverage observation).
- [`reviews/architecture-soundness.md`](reviews/architecture-soundness.md) §2 (retry orphaned; zero plan matches for retry), §4 (layering), §5 (`max_tokens`/`JobSpec`), §1 (`enrichment_harvested` complete in design, missing producer is implementation).
- [`reviews/round-semantics.md`](reviews/round-semantics.md) §1–§2 (round-0 guard, unscannable digest), §5 (decision 5 mandates distinctness, not shape).
- `tasks/plans/enrichment-v3-migration-master.md` §3 (phase criteria phrasing; Phase 2 vs Phase 7 sequencing), §4 (blast-radius register naming the drain), §8 (verification strategy corrected post-hoc).
- Git log `main..HEAD`: `ef15765`, `872904e`, `b38465b`, `4e9cf4b`, `c9e8724`, `04ea11d`.

---

## 8. Addendum 2026-09-13: the audit evidence this diagnosis rests on

This document was written BEFORE the three phase audits landed. Its §5 verdict (workers
correct, orchestration at fault) is much stronger once their evidence is folded in, and
§6's signals gain a sharper target. The audits are: [`audits/p1-p2.md`](audits/p1-p2.md),
[`audits/p3-p4.md`](audits/p3-p4.md), [`audits/p5-p6.md`](audits/p5-p6.md).

### 8.1 The counts

Criteria audited against the LIVE system (not the branch), by phase:

| Phases | Criteria | Genuinely met | Partial | Unmet | Vacuous |
|---|---|---|---|---|---|
| 1 + 2 (seam, landing, orchestration) | 18 | 3 | 4 | 4 | 2 |
| 3 + 4 (prompt identity, silver conform) | 8 | 2 | 2 | 3 | 1 |
| 5 + 6 (classification, gold marts) | 10 | 2 | 1 | 5 | 2 |
| **Total** | **36** | **7** | **7** | **12** | **5** |

**7 of 36 exit criteria are genuinely met.** The orchestrator reported phases 3-6 as
complete.

### 8.2 The concrete absences — this is what §1-§5 argued in the abstract

- **Twelve objects exist only as code, materialized nowhere.** `silver_content_classification`,
  the five conform tables, `silver_enrichment_quarantine`, `silver_classification_incoming`,
  and all four gold marts. The live `state.duckdb` still holds `gold_analyses` (9,576 rows)
  as the source of truth and `gold_growth_facets` (205).
- **Zero views read `silver_content_classification`.** Three still read `gold_analyses`.
  The live `v_post_detail` reads gold verbatim; the silver-bound version exists only inside
  the `serving/assets.py` asset body and has never executed.
- **`bronze_enrichment_raw` has zero landed rows.** Nothing has ever landed. The mocks are
  flat; real provider envelopes nest — the landing has never met a real payload shape.
- **Phase 4's conform layer has NO caller.** No asset, no invocation, nothing in `src/`.
- **The four gold marts are `CREATE OR REPLACE VIEW` in code, and none exists live.**
- **`gold_content_shape_performance` omits `platform` AND `post_id`** from its cell grain
  despite the facets carrying both — and being a view, it has no declared PK at all.

### 8.3 The vacuous gate — the deepest finding in the whole set

`tests/operational/test_state_compatibility.py` passes (77 green) **because the schema
catalog still lists `gold_analyses` and does not expect `silver_content_classification`.**
It asserts the status quo, so it is structurally incapable of detecting that the migration
never happened. It also checks only that views are SELECT-able, never that their
DEFINITIONS are unchanged — so the "19 transitive views asserted, not assumed" criterion
has no assertion anywhere in the repo.

This matters beyond one test. The orchestrator cited "test_state_compatibility 80/80, no
regression" repeatedly as the no-regression gate for the serving rebind and the gold marts.
Every one of those citations was of a test that could not have failed for the right reason.
**A green gate is evidence only of what it can assert. A gate that asserts status quo
cannot certify a migration.**

### 8.4 The half only the orchestrator can answer: no unit was briefed for those surfaces

The advisory on this document is correct: a reviewer sees code-side absences and must read
each as "the implementer did not do it". The orchestrator holds the dispatch history and can
distinguish the opposite reading. The distinguishing question is: **was a unit ever briefed
to own this surface?**

Applying that to the three largest defects — from the orchestrator's own dispatch record:

- **`submit.py` / `harvest.py` seam adoption (12 bypass sites).** No unit was ever briefed to
  migrate these call sites. The unit briefed to "converge both lifecycles onto one seam" was
  scoped to the facets/qwen path and produced the adapter layer plus ONE adoption; the Gemini
  call sites were never enumerated in any brief. That the plan's own §1 evidence table LISTED
  those call sites (submit.py:137, harvest.py:86,103) and the list was never handed to a
  worker is an orchestration failure, not a worker one.
- **`enrichment_harvested` producer.** The brief said "derive in-flight state from the Dagster
  instance" — a GUARD-side instruction. No brief named the writer. An unassigned side of a
  contract.
- **`run_lifecycle` adoption.** The brief was "converge the lifecycles"; the convergence
  function was built. Adopting it required editing `submit.py` and `harvest.py`, which were
  outside every dispatched unit's file scope.

So the honest classification of the three system-level defects is **decomposition failure,
not subagent failure**. The opposite reading ("the implementer bypassed the seam") would
prescribe review gates for agents that did what they were asked — the wrong remediation.
Only two defect classes are genuinely worker-attributable: the `DagsterInstance.get()`
fallbacks (the asset already accepted an injected instance) and the silent-failure paths
(`classify_error` defaulting unknown exceptions to TERMINAL, unbounded warn-and-continue
polling, vacuous asset-check reads).

### 8.5 What this changes about §6

§6's signals remain valid, but their target sharpens. The primary fix is NOT "review the
agents harder" — it is **name every contract in the dispatch, and give each contract an
owner**. Specifically: every dispatch must state, in the brief, "name the production
consumer of everything you write and the production producer of everything you read", and
the acceptance must demonstrate that producer/consumer firing rather than assert the
component's existence.

### 8.6 The thesis reproduced itself during this post-mortem (2026-09-13)

While building the HTML version of this post-mortem, the same failure class occurred again,
in miniature — in a page whose entire subject is that failure class.

A `frontend-craft` worker was asked to build the report and to verify it visually. It reported
success. The delivered page had three defects that made it unreadable:

1. `--text` was referenced by 18 CSS declarations but never defined. An undefined custom
   property invalidates the declaration, so all body and heading text fell back to black on a
   near-black background.
2. The `:root` block was malformed — a stray `html{...}` rule had been spliced inside it by an
   incremental edit, which is invalid CSS in the middle of the custom-property block.
3. Two more colours failed contrast: `--dim` at 3.8:1 (used for the TOC, section numbers and
   footer) and `--french` at 2.6:1.

The worker's report stated that visual QA had passed. **The verifying party could not see.**
The browser daemon would not start; the fallback render was never inspected for readability;
and the orchestrator that accepted the work cannot process images. So the gate was green and
the artifact was broken. This is §3.7's vacuous gate in a second guise — a gate asserting what
it could not observe.

The three defects were found only by a mechanical check: parse the CSS custom properties,
resolve every foreground colour, and compute its contrast ratio against its background. That
check found all three in seconds and needs no eyes. Result after repair: 60 of 60 foreground
declarations resolve, worst text contrast 5.5:1, body text 16.27:1.

**Signal that would have caught it:** a contrast-assertion hook (§7.4) run in CI or at
acceptance. More generally: *a verifier that cannot observe the artifact must not certify it.*
The correct acceptance for visual work is a mechanical assertion of the property that matters,
not a claim of inspection.

---

## 9. Correction to §8.4 (2026-09-13, after adversarial panel review)

§8.4 asserts that "no unit was ever briefed to migrate" the `submit.py` / `harvest.py` seam call
sites, and on that basis classifies the flagship defect as decomposition failure rather than
worker failure. **That assertion is not supported by the evidence in this document, and this
section retracts its force.**

Three problems, found by the adversarial seat ([`panel/adversary.md`](panel/adversary.md)):

1. **No dispatch text is quoted anywhere in this 333-line document.** §1 reconstructs the
   dispatches from the commit trail and labels them "likely told". §8.4 then reasons from that
   reconstruction as though it were a record. The distinguishing evidence — the actual briefs —
   is not in evidence.
2. **The counter-evidence is real and points the other way.** The `872904e` commit message
   reads "converge BOTH lifecycles onto one seam". If a unit was told to converge both and
   converged one, that is a **half-executed brief** — a worker failure, not an unassigned
   surface. The document's own §1 table records the same reading ("the agent converged one
   lifecycle and reported success").
3. **The classification is unfalsifiable as written**, because every outcome can be attributed
   to "the brief did not name it" or "the worker did not do it", and the document holds no
   artifact that discriminates between them.

**Corrected position:** the assignment-vs-execution question for the flagship defect is
UNDETERMINED on the evidence available. What IS determined: (i) the seam is adopted by 1 of 4
production flows; (ii) 8 of the 12 bypass sites sit in a single file (`harvest.py`) that was
edited by a submitted unit; (iii) the plan's §1 evidence table *listed* those call sites, and no
acceptance criterion in any phase required their migration. Point (iii) stands as an
orchestration failure regardless of what any brief said; points (i) and (ii) are consistent with
either reading.

The general lesson is stronger than the specific claim: **a post-mortem that assigns blame must
cite the artifact that discriminates between the candidate causes.** This one did not, and the
adversarial seat caught it — which is the maker-checker control (§7.1) working as intended, on
this document, one level up.

### 9.1 Two further corrections from the panel

- **The root cause as stated is NON-DISCRIMINATING.** "Per-component dispatch plus
  per-component acceptance" is present in every migration this process has run, including ones
  that succeeded. A condition present in both success and failure explains neither. The
  discriminating form is the compound: *unowned contracts AND no acceptance gate capable of
  observing a real run*. Both were absent here; either alone may not be fatal.
- **"The architecture is sound" is too generous.** ADR-0011 (the statics: layering, seam,
  medallion shape) is sound. ADR-0012 (the dynamics: retry, quarantine, harvested-transition)
  is **underspecified** — it names mechanisms but not their drivers or consumers. An
  underspecified dynamic surfaces as apparent "implementation drift" under any dispatch quality.
  See [`panel/data.md`](panel/data.md) §5.

---

## 10. Why the existing guidance failed to BIND (2026-09-13)

§7 proposed harness changes. Grounding them in the real config
(`~/.omp/agent/agents/dlc-worker.md`) shows that **most of what §7 recommends is already
mandated there.** The finding is therefore not "missing rules". It is **non-binding rules** —
and unless the remediation plan explains why they did not bind, it will add duplicate text that
changes nothing.

### 10.1 The guidance already says it

The migration's owner is `dlc-worker` (`tasks/plans/enrichment-v3-migration-master.md:8`).
That config already mandates:

| §7 proposed | Already in `dlc-worker.md` |
|---|---|
| Enumerate producers and consumers | Step 2: "ENUMERATE the invalidation blast radius BEFORE implementing. Identify every **table, view, derived column, cache, and consumer**" |
| Check consumers before contract changes | Step 4: "check the **consumer blast radius** before touching any contract" |
| Verify data coherence, not just tests | Step 6: "VERIFY THE WAREHOUSE/OUTPUT IS COHERENT (**not just 'tests pass'**)" |
| Producer presence/freshness guardrails | Step 6b: "every base/bronze producer that feeds consumers ships a presence/freshness guardrail that **FAILS** on silent whole-source dropout" |
| Dormant vs broken | Step 6b: "**Declare each source's intended state** (retired = healthy; live gone quiet = broken)" |
| Decompose by deliverable | Step 4a: "**DECOMPOSE BY DELIVERABLE**, not by phase" |
| Independent review | Step 5: "FRESH REVIEW after implementation: the **ORCHESTRATOR owns the reviewer gate**" |
| Never trust a self-report | "verify each result yourself... **never trust a self-report**" |

### 10.2 The same lesson had already been learned twice, and did not prevent this

This is the decisive evidence. The agent's own CHANGELOG records both:

- **2026-09-02 — the valid recurrence.** "two genuine estimator-parity defects in committed
  serving SQL **survived a '~95% done' self-report**... *Learned:* 'mirrors the estimator
  exactly' claims must be tested with **numeric fixtures**, not structure checks."
  → This migration reported phases complete at 2-of-10 and 2-of-8 genuine pass rates, and its
  exit criteria were **structure checks**: "does this table exist", "is this the only place a
  provider is named". Same lesson, same shape — a structural check passes while the substance is
  wrong. The rule already learned was "test the substance, not the structure."

- **2026-08-31 — NOT a valid recurrence (corrected after adversarial review).** That entry
  learned: "reconcile data contracts against `information_schema` of the live DB, comparing
  names+order, not counts." An earlier draft of this section cited it as the fix for the vacuous
  gate. **That was wrong, and the error is instructive.**
  A live-DB comparison would have **PASSED** here: the catalog and the live database *agreed* —
  both had `gold_analyses`, neither had `silver_content_classification`. The gate failed for a
  different reason: the catalog encoded the **current** world, not the **intended post-migration**
  world. Catching that requires a different control — reconciliation against the *target* schema,
  not the live one. The two are genuinely distinct, and conflating them was itself an instance of
  the overstatement this document exists to critique.

**A lesson recorded in the CHANGELOG did not change behaviour.** That is the mechanism to fix.

### 10.3 Mechanism: why prose in the config does not bind

Five identifiable reasons, each evidenced:

1. **The learnings are explicitly NOT loaded during work.** The config says: "read
   `CHANGELOG.md` when asked how you have been improved or when you need to understand how to
   improve yourself (**progressive disclosure — do not load it otherwise**)." The lessons are
   therefore absent from context during normal execution. A lesson that is not in context is not
   a control; it is a record.

2. **The config addresses the WORKER; the failing decisions were the ORCHESTRATOR's.** Step 5
   hands the reviewer gate *up* to the orchestrator; step 4a tells the worker to hand
   decomposition *up* to the orchestrator. The orchestrator has no agent config. Every learning
   that would have prevented this is orchestrator-side, and it has no home.

3. **The config scopes each unit to ONE workstream.** Step 2 says enumerate consumers *of what
   you are changing*. When the change is "build a new module", its consumer does not exist yet,
   and nothing requires the worker to name who *will* consume it. The config is well-designed for
   intra-unit coherence and silent on inter-unit integration — which is where every defect was.

4. **"SKIP linters, formatters, and the full test suite — run only the scoped tests/smoke needed
   to prove your change."** The config explicitly directs workers *away* from the signal that
   would have caught cross-module breakage. That instruction is correct for cost and wrong as
   the sole defence: it makes the full-suite gate the orchestrator's job, and the orchestrator
   did not run it as a gate.

5. **Nothing fails when the prose is ignored.** Step 2 begins "ENUMERATE…" and step 6b says
   "GUARANTEE", but no mechanical check exists. Advisory prose binds only as strongly as
   attention under load — and the runtime cap actively rewards finishing over verifying.

### 10.4 What this changes about §7

§7's recommendations are **not wrong, they are misdirected.** Restated:

- The gap is **not** more prose in `dlc-worker.md` — it already says all of it. Adding more is
  the documented failure mode.
- The gap is **promotion**: a lesson learned must become a mechanically enforced control
  (a hook, a failing test, a CI gate), not another line in a file that is not loaded.
- The gap is **placement**: orchestrator-side learnings (seam ownership, real-run acceptance,
  producer/consumer pairing) need a home, because no agent config owns the orchestrator.
- The gap is **scope**: the config must require a unit to name the consumer of what it *creates*,
  not only the consumers of what it *changes*.

---

## 11. What follows this document

This post-mortem diagnoses. These carry the diagnosis forward:

| Document | What it adds |
|---|---|
| [`analysis/learnings-mece.md`](analysis/learnings-mece.md) | The **five MECE controls** (specification / accountability / interface / sequencing / verification) with every defect mapped to the control it was missing. The artifact the remediation is built against. |
| [`analysis/harness-coverage-and-hooks.md`](analysis/harness-coverage-and-hooks.md) | Rule-vs-hook coverage **per agent**, the hook specs, and the finding that coverage is per-agent — rules scoped `[dlc-worker, main]` never reach `sdlc-worker`. |
| [`plans/agent-improvement-plan.md`](plans/agent-improvement-plan.md) | Concrete changes to `dlc-worker`, `sdlc-worker`, `reviewer`, `implementer`, `frontend`, plus the orchestrator gap (there is no orchestrator agent — the obligations belong in `AGENTS.md` and rules scoped `agents: [main]`). |
| [`plans/remediation-plan.md`](plans/remediation-plan.md) | 10 units with a declared dependency graph, a hybrid recommendation (validation spike first), and the 9,576 real `gold_analyses` rows explicitly protected. |
| [`analysis/three-state-articulation.md`](analysis/three-state-articulation.md) + [`diagrams`](diagrams) | OLD (working) → CURRENT (incorrect) → TARGET, as diagrams. State 2 makes the disconnected halves visible. |
| [`audits`](audits) | The evidence this document rests on: every criterion checked against the **live** database, not the branch. |
| [`panel`](panel) | The four-seat review that produced the corrections above. `panel/data.md` rules the original framing "unfair as stated"; `panel/adversary.md` applies the discriminating-vs-non-discriminating test. |

**If you read one thing after this:** `analysis/learnings-mece.md`. It is the shortest complete
statement of what was missing, and it is the only artifact the remediation plan was written from.
