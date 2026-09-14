# Expert Panel — PROCESS seat: how the work was organized

Seat: agent orchestration, dispatch shape, verification instrument, harness placement.
Document under review: `data/dev/postmortem-implementation-drift.md` (+ audits as evidence).
Read-only review; this file is the deliverable.

---

## 1. MECE test of the (a)–(e) classification

Classes: (a) dispatch/decomposition shape, (b) verification instrument,
(c) task instructions, (d) worker execution, (e) plan/spec.

### 1.1 The fatal ambiguity: (a) vs (c) vs (e) are one axis in disguise

(a), (c), and (e) are not three classes — they are the **same failure observed at three
stages of a pipeline**: the plan writes the dependency structure (e), the orchestrator
converts it into a dispatch (a), and the dispatch is the task instruction (c). Every
defect the post-mortem tags (a)+(c) or (a)+(e) has one root, one fix, and one owner:
the orchestrator. Splitting it into three classes manufactures MECE-looking coverage
from a single cause. Cases:

- #2 (`enrichment_harvested`): tagged (a)+(c). The reasoning text says "the design is
  sound… the *implementation gap* traces to an unassigned producer." If the design is
  sound, this is NOT (e); if no dispatch named the writer, it is one orchestration
  decision (decompose without naming contract sides), not two independent failures.
  (a) and (c) are the same event seen from two altitudes.
- #3 (`run_lifecycle` unadopted): tagged (a)+(c), same problem. One missing line in one
  brief. Two tags, one fix.
- #7 (round-0 hardcode): tagged (e) primary, (c) contributing — but the reasoning admits
  ADR-0012 was "silent on key shape." A silent ADR is a plan gap (e); "the worker cited
  decision 5 and implemented honestly" means the instruction (c) was *correct given the
  spec*. Class (c) adds nothing; it is double-counting the same silence.

**Verdict: the taxonomy is not MECE.** It is exhaustive (nothing escapes) but not
mutually exclusive — and the overlap is not cosmetic: it inflates the apparent number
of failure classes and blurs ownership.

### 1.2 The MECE repair

The correct cut is by **where the fix lives**, not where the text lives:

| Class | = | Owner of fix |
|---|---|---|
| Contract/design gap | old (e) | plan author / ADR |
| Decomposition & acceptance gap | old (a) **+** (b) **+** (c) merged | orchestrator |
| Worker execution | old (d) | worker + code review |

Under this cut: (a),(b),(c) collapse into "orchestration" (10 defects), (e) keeps 3
(7, 9, 13), (d) keeps ~2 (10-partial, 11). See §2 for why (a) and (b) also merge.

### 1.3 Gaps: defects that fit no class cleanly

- **#10's two halves don't belong in one row.** The `DagsterInstance.get()` fallbacks
  are (d); the cross-domain DDL reach and `GeminiTierConfig` import are **plan blind
  spots about module boundaries** — that is (e)-flavored (the plan never declared the
  slice boundary). The row is a hybrid that the taxonomy has no clean slot for.
- **#12 (dead code) is tagged (a)+(c)** but the real cause is (b): the acceptance never
  checked for deletion. "No dispatch said delete what you supersede" is an acceptance
  gap wearing a dispatch costume. The class assignment follows the *narrative*, not the
  fix.
- **Missing class entirely: the sequencing/integration owner.** #1's own reasoning
  ("queue retirement in Phase 7 while rewiring the drain in Phase 2… no dispatch carried
  the join") names **phase ordering** as a cause — which is neither dispatch shape (a)
  nor plan content (e) in the way the taxonomy uses them. It is a *scheduling of
  interdependent work* failure. The taxonomy has no class for it; it gets absorbed
  silently into (a). A MECE taxonomy needs a SEQUENCING class.

---

## 2. "Orchestration" is too broad — the four-way decomposition

The post-mortem uses "orchestration" to cover at least four distinct, independently
fixable decisions:

1. **DECOMPOSITION** — how the work was cut into units (per-module, not per-contract;
   integration assigned to nobody).
2. **DISPATCH** — what each unit was told (module named, contract not named; §1's table
   shows this is the dominant defect vector).
3. **ACCEPTANCE** — how completion was judged (phase-own unit tests, existence criteria,
   7-of-36 genuinely met, the vacuous `test_state_compatibility.py` gate, and §8.6's
   unverifiable "visual QA passed").
4. **SEQUENCING** — phase ordering (queue retirement in Phase 7 while Phase 2 rewires
   the drain; cross-phase coupling converted into per-phase checkboxes).

### 2.1 Verdict: the proximate cause is ACCEPTANCE

This is the finding the post-mortem conflates away. Decomposition and dispatch are
**necessary but not sufficient** causes: even with per-component dispatches, a
round-trip acceptance test at each phase boundary would have caught defects 1, 2, 4,
5, 6, and 8 immediately — §6 of the post-mortem itself says every one of them was
catchable "at acceptance." The post-mortem's Why-5 ("determined at dispatch design
time, before any worker saw a prompt") is therefore wrong as stated: a competent
acceptance instrument is a second, *post-dispatch* chance to catch the same defects,
and §6's own signal table is mostly acceptance-time checks. What dispatch-shape failure
guarantees is that defects are *created*; what acceptance failure guarantees is that
they are *not detected*. The headline says the failure was "determined at dispatch
design time" — but the audits show the defects survived to ship because the acceptance
gate was vacuous. **Root cause: acceptance. Contributing causes: decomposition,
dispatch. Sequencing: a plan-level cause (class (e) under the MECE repair) that made
the first two harder but did not by itself force any defect.**

Counter-evidence the post-mortem should have weighed: `facets_batch.py:292` is the ONE
place a worker adopted the seam without being told to at the contract level — showing
workers can exceed the dispatch; it was the acceptance that never rewarded or required
it. And the Phase 3–6 phases were reported "complete" against 2-of-10 and 2-of-8
genuine pass rates — a dispatch cannot lie; only an acceptance can accept a lie.

---

## 3. §6 signals, rewritten: checkable vs aspirational

| §6 signal (as written) | Verdict | Operational form |
|---|---|---|
| "name the production consumer of everything you write; producer of everything you read" | **Aspirational as stated** — a self-report; nothing checks the named consumer is real | Mechanical: a CI grep/lint gate — every new table/partition/topic written by the diff must appear in ≥1 read site outside the writing module (or in a registered consumer manifest); every read of a table must resolve to a live producer. Same shape as the plan's own blast-radius register, enforced not just written. |
| "demonstrate the producer fires" (defect 2) | **Checkable** | Asset-check / integration test: materialize, then query for ≥1 row (or the write path asserted via mock at minimum); acceptance reports the observed row, not "function exists". |
| "show `run_lifecycle` on a stack trace from the production entry point" | **Checkable** — this is the best signal in the document | Run the real entry point under `cProfile`/sys.settrace in CI; assert the symbol appears. Falsifiable, mechanical. |
| Grep-shaped "provider names only in adapter modules" (defect 4, 5) | **Checkable** | A 5-line CI rule; trivially mechanical. Also add the positive direction the signal misses: the adapter modules must have ≥1 non-test caller. |
| "round-trip test with multi-chunk case" (defect 6) | **Checkable** | Property test: `encode(decode(handle))` over a multi-chunk fixture; shared fixture file so both encodings cannot silently diverge again. |
| "can a consumer answer without knowing a hidden input?" (defect 7) | **Aspirational** — a design-review question, not a check | Convert: ADR review checklist item with a *named reviewer role*; plus the retry-shaped test (round-1 in flight) at the drain, which IS checkable. |
| "what would this test have to look like to FAIL?" (defect 8) | **Aspirational** as phrased (judgment) but checkable in the specific case | Mechanical mutation test: flip one assertion in each new test, confirm the suite reddens. Or the cheaper rule: any criterion containing "is the ONLY/ONE" must be paired with a named adoption test in the PR. |
| "every ADR mechanism must name its driver" (defect 9) | **Aspirational** — plan-review judgment | Semi-checkable: lint ADRs for mechanism nouns (retry, quarantine, backoff) and require a `driver:` field per mechanism; fail ADR review if absent. |
| "list every module you import outside your slice" (defect 10) | **Checkable** | Per-PR: `git diff --name-only` vs a declared slice manifest; imports crossing slices fail CI. Trivially mechanical. |
| "adversarial review question: what happens when the provider returns garbage?" (defect 11) | **Aspirational** — this is the one that legitimately needs judgment | Keep as reviewer-brief question (see §4 table), but pin it: every `except` clause added by the diff must map to a named error kind with a declared disposition (retryable/terminal/loud), checked by review checklist. |
| "delete everything you superseded" (defect 12) | **Checkable** | Acceptance checklist line + `grep` for the superseded symbol names in `src/` at merge time; the superseded symbol list is written by the dispatch. |
| §8.6 "verifier that cannot observe the artifact must not certify it" | **Checkable** | The contrast-assertion hook it names; more generally: every acceptance claim must cite a tool *output*, and the tool list is per-surface (browser-driven screenshot for UI, query row for data). |

**Duplication check:** defect 4 and defect 5 signals are the same check twice (the grep
rule) — collapse. Defects 1 and 2 signals are the same principle (producer/consumer
demonstration) at two surfaces — keep both instances but count one signal. Net: §6 has
~10 genuinely distinct signals, of which **7 are mechanically checkable and 3 are
judgment calls that need a checklist or reviewer-brief home** (§4).

---

## 4. The harness question: where do the learnings live?

Grounding: `~/.omp/agent/agents/dlc-worker.md` (417-line CHANGELOG) and
`sdlc-worker.md`. Current state: both agents already carry blast-radius enumeration,
fresh reviewer spawning, "never trust a self-report," lessons.md scanning, and
self-improvement changelogs. **What is missing is not worker behaviour — the worker
configs already say the right things. The gap is that nothing mechanical verifies any
of it, and the post-mortem's learnings are at risk of being added as *more prose* to
configs that already have the prose.** (dlc-worker CHANGELOG 2026 entry: briefs
"carried the OFFLOAD LONG RUNS prose … but did not follow it" — the documented
failure mode of prose learnings.)

Home criteria:
- **HOOK** (or CI gate): the check is mechanical, greppable, or a runnable assertion —
  judgment not required at check time.
- **RULE** (invariant, always loaded): a hard behavioral boundary that applies to every
  dispatch regardless of project.
- **SKILL** (knowledge, pulled by concern): domain technique (how to write a round-trip
  test, how to structure a walking skeleton) — pulled when the concern is active.
- **AGENT CONFIG** (behavioural, per-agent): disposition/sequencing habits specific to
  that agent's role in the loop.

| Learning | Best home | Why |
|---|---|---|
| "Assert a round-trip, never an existence" | **RULE** | Universal invariant for any verification, not data-specific, not mechanically checkable in the abstract — it governs how tests are written. A hook can *lint for "the ONLY/ONE" criterion wording* as a cheap enforcement of its most common violation. |
| Grep gate: provider names only in adapter modules (defects 4, 5) | **HOOK / CI** | Pure mechanical check; belongs in CI, not in any agent's attention. |
| Producer-fires demonstration (defect 2) | **HOOK** (asset-check in Dagster) + **RULE** ("acceptance reports observed rows, not symbol existence") | The check is mechanical; the reporting habit is behavioral. |
| Stack-trace adoption proof (defect 3) | **HOOK** | Fully mechanical (trace + assert in CI). |
| Import-outside-slice gate (defect 10) | **HOOK** | `diff` vs manifest; zero judgment. |
| "Delete what you supersede" (defect 12) | **AGENT CONFIG** (acceptance checklist in the worker return format) + grep hook for the named superseded symbols | The *listing* of superseded symbols is a worker duty (only the worker knows them); the *grep* is mechanical. |
| Driver named for every ADR mechanism (defect 9) | **SKILL** (a plan-review/ADR-authoring skill) | It is knowledge about how to write specs, exercised only by the agent writing plans; forcing it into every agent body wastes context on agents that never write ADRs. |
| Key-shape review before building guards (defect 7) | **SKILL** (dagster-expert-extra / orchestration skill: partition-key design) | Domain technique, pulled by concern. |
| Adversarial error-path questions ("what if the provider returns garbage?") | **AGENT CONFIG** (reviewer agent's brief template) | Judgment at review time; the reviewer agent is the correct bearer. Pinning each `except` to a declared disposition can later graduate to a lint. |
| "Verifier that cannot observe must not certify" (§8.6) | **RULE** | Universal acceptance invariant. Its concrete instances (contrast gate, query-a-row) are HOOKS. |
| Dispatches name contracts, not modules; enumerate consumers/producers per side | **AGENT CONFIG** — but of the **orchestrator**, which the post-mortem never names as an agent to improve | This is the deepest misplacement risk in the whole set: the post-mortem prescribes fixes for workers, but the defective actor was the orchestrator's brief-writing. There is no `orchestrator` agent config on this machine holding this learning; it currently lives only in prose rules. |
| Explicit read budget / context-handoff in briefs (CHANGELOG learning) | **AGENT CONFIG** (orchestrator brief template) | Already learned once at cap-abort cost; still prose. |

**Harness verdict:** the worker configs need almost nothing new — the dlcs' existing
prose already covers blast radius, fresh review, and self-report distrust. The
implementation gap the requester is pointing at is real but is concentrated in:
(a) no mechanical hooks exist for any of the checkable learnings above;
(b) the *orchestrator-side* agent (brief-writing, acceptance) has no config carrying
these learnings, so every fix lands on the wrong agent; (c) the acceptance-instrument
defect (vacuous gate) is a rule-without-hook — the rule was written in dlc-worker §6b
(guardrail-presence-gate) and was never invoked.

---

## 5. Seams-owner vs producer/consumer — one learning or two?

**They are distinct, on different axes. Do not merge; do not keep as two peer learnings —
keep as one learning plus one enforcement mechanism.**

- **"Seams need owners" is a DECOMPOSITION decision**: it happens at cut time, before
  any dispatch. It answers *who* — a named unit or named person owns the connection
  between two components. Its failure mode is defect 1 (no slice owned the handoff).
- **"Producer/consumer checks" is an ACCEPTANCE/verification instrument**: it happens at
  acceptance time, after work. It answers *is the connection real* — demonstrated
  firing, not existence. Its failure mode is defects 2 and 8 (guard passed on a fake;
  nothing demonstrated a handoff).

Axis of separation: **time in the pipeline (before work vs after work) and question
asked (who is responsible vs is it true)**. One is an org-chart property; the other is
an evidence property. They are complements: an owned seam with only existence tests
still ships vacuously (what happened here — Phase 2 *was* nominally the drain's
consumer per the plan's register, yet the handoff was never demonstrated); a
producer/consumer check with no owner is nobody's test to write.

Practical consequence: keep them as **one learning with two clauses** — "every seam has
a named owner, and the owner's acceptance is a demonstrated round-trip." As two separate
learnings they will be re-derived independently and drift, which is exactly the
divergence class (defect 6) this migration suffered.

---

## 6. Missed patterns for agent orchestration

1. **Maker-checker / four-eyes — APPLIES.** The migration had makers (workers) and a
   post-hoc audit, but no four-eyes at the *seams*: each acceptance was performed by
   the same context that wrote the dispatch. A fresh-context checker with the explicit
   question "which production path consumes this?" would have caught defects 1–5. The
   reviewer-spawning habit exists in both worker configs (step 4/5) — but the
   post-mortem's own dispatch record shows phases 3–6 were accepted on the
   orchestrator's reading of green suites, i.e. the checker either never ran with the
   contract question or its findings didn't gate the merge. The pattern isn't missing
   from the config; it was missing from the loop.
2. **Verification vs validation — APPLIES, and it is the post-mortem's sharpest unspoken
   distinction.** Verification = "did we build the thing right" (tests against spec);
   validation = "did we build the right thing" (does the whole run one cycle against
   real data). 701 green tests verified slices; nothing validated the pipeline — zero
   landed rows in `bronze_enrichment_raw`, no marts live. §8.3's vacuous-gate finding is
   precisely a confusion of the two. The walking-skeleton point below is validation's
   orchestration form.
3. **Walking skeleton as an orchestration primitive — APPLIES, with hindsight.** The
   migration should have started with Phase 0: a thin end-to-end path (one post → bronze
   row → conform → mart cell) walked before any component was thickened. Every defect
   the post-mortem calls system-level is invisible at slice level and obvious at
   skeleton level: "drain's partitions have a consumer" is a skeleton assertion;
   "submit consumes the drain" likewise. ADR-0012 mandates Dagster-native orchestration —
   which makes the skeleton *cheaper here than usual* (partitions + materialize-one) —
   making its absence the most actionable single miss. Mark HINDSIGHT rather than
   APPLIES only in the sense that no dispatch ever owned it; the plan's own §4 register
   knew the coupling.
4. **Interface-first dispatch — APPLIES.** Defects 5 (max_tokens), 6 (handle encoding),
   7 (key shape) are all two-slices-encode-one-contract-independently: the contract
   artifact (JobSpec, handle grammar, partition-key grammar) should have existed as a
   reviewed, typed artifact *before* the dispatches that consume it. The post-mortem
   says this for #13 only, as a §6 row; it should be a first-class dispatch rule:
   "two or more units touch a format → the format is its own deliverable, dispatched
   first."
5. **Definition of done as a contract — APPLIES.** The exit criteria were statements
   about artifacts; DoD-as-contract makes each criterion a *falsifiable assertion owned
   by a checker that did not write the code*. 7-of-36 genuinely met, 5 vacuous, and the
   orchestrator still reported phases complete — the criteria failed as contracts
   because nothing bound their evaluation to an independent observer. This is the
   acceptance-side twin of the seams-owner learning, and per §2 it is the proximate fix.

## 7. Summary verdict for Main

- Taxonomy (a)–(e): not MECE; (a)+(c)+(e-orchestration-share) collapse to one class;
  sequencing/integration-owner is a missing class; two rows (#10, #12) are hybrid.
- "Orchestration" conflates decomposition, dispatch, acceptance, sequencing. Proximate
  cause: **ACCEPTANCE** (vacuous instrument), not dispatch-shape as the headline claims;
  decomposition/dispatch are contributing, sequencing is plan-level.
- §6: ~7 of 10 distinct signals mechanically checkable (grep, trace, row-observation,
  mutation, slice-manifest diff, superseded-symbol grep); 3 need judgment and belong in
  a reviewer brief; several as-written are aspirational self-reports.
- Harness: the dlc/sdlc worker configs already carry most behavioural learnings; the
  missing implementation is hooks for the checkable signals, an orchestrator-side home
  for brief/acceptance learnings, and rules for the two universal invariants
  (round-trip, verifier-must-observe).
- Seams-owner (decomposition-time, "who") vs producer/consumer (acceptance-time, "is it
  true"): distinct axes; merge into one learning with two clauses.
- Missed patterns: maker-checker (APPLIES — config had it, loop didn't), verification
  vs validation (APPLIES — the §8.3 finding), walking skeleton (APPLIES/HINDSIGHT —
  cheapest possible in a Dagster-native design), interface-first dispatch (APPLIES),
  DoD-as-contract (APPLIES).
