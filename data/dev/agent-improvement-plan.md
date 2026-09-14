# Agent improvement plan

Derived from the Enrichment v3 post-mortem, the four-seat panel, and the harness coverage
analysis. **Additive throughout** — no existing rule is removed, and no agent's existing
guidance is deleted. Where a clause is rewritten, the obligation is preserved and the scope or
specificity is corrected.

Guiding principle, established in `postmortem-implementation-drift.md` §10: **the configs
already contain most of this prose and it did not bind.** So every change below is chosen for
one of three properties:

1. **Binding** — it is mechanically checkable, or it is an acceptance gate (not advice).
2. **Correctly scoped** — it reaches the agent that can actually violate it.
3. **Capability-aware** — it states what the agent must do when it *cannot* observe.

Adding obligation-prose that satisfies none of these is the documented failure mode.

---

## 0. The structural gap: there is no orchestrator agent

Eight agent configs exist: `dlc-worker`, `sdlc-worker`, `frontend-craft`, `frontend`,
`implementer`, `ux-worker`, `iac-worker`, `reviewer`. **None of them is the orchestrator.**

Both worker configs hand responsibility *upward*:
- `dlc-worker.md` step 5: "the ORCHESTRATOR owns the reviewer gate."
- `dlc-worker.md` step 4a: "tell the orchestrator to write it inline."
- `sdlc-worker.md` rule: "the orchestrator verifies and formats once at phase end."

Every learning that would have prevented this migration is orchestrator-side — decomposition,
dispatch contract, acceptance gate, integration ownership. The orchestrator has no config, so
none of it binds. **This is the highest-value change in the plan** (§6).

---

## 1. `dlc-worker` — light touch, three changes

It already mandates the blast-radius enumeration, consumer check, fresh review, coherence-not-
tests, producer guardrails, dormant-vs-broken, decompose-by-deliverable, WRITE-FIRST, offload,
and never-trust-a-self-report. It is the best-configured agent in the set. Add only:

**1.1 Step 2 — extend "consumers" to created artifacts.**
Current step 2 enumerates consumers of what you *change*. Add one clause:
> For a NEW component, name its production consumer, or state explicitly that it has none yet
> and who will assign one.

*Closes G2. This is where all six system-level defects lived.*

**1.2 Step 6 — require a real run, not a test.**
Step 6 says "not just tests pass". Sharpen to:
> Demonstrate the change on a REAL run or a real subset. A passing test proves a process ran;
> it does not prove the data is right. State which evidence is a real run and which is a fixture.

*Closes the gap that let 12 objects ship code-only.*

**1.3 Step 6b-adjacent — new clause: every mechanism names its driver.**
> Any retry, quarantine, reconciliation, or cleanup you build names the actor that drives it and
> the event that triggers it. A mechanism with no driver does not exist yet.

*Closes G1. Would have caught the retry with `MAX_ATTEMPTS` and no round-minter.*

**Not proposed:** any change to the existing blast-radius, guardrail, or review clauses. They are
correct and already present.

---

## 2. `sdlc-worker` — the largest gap, five changes

It is a thin config (5.3KB vs `dlc-worker`'s 12.2KB) and, more importantly, **six rules credited
in the coverage analysis never reach it** (§7.2 of `harness-coverage-and-hooks.md`). It is the
agent most likely to report "tests pass" on an unrun path, and it lacks both rules that forbid it.

**2.1 Add a dependency/contract step.**
It has no equivalent of `dlc-worker` step 2. Add, before implementation:
> List what this change depends on and what depends on it. Name the interfaces it touches and
> their owners. If it creates a component, name the consumer.

*`declared-dependencies` and `contract-first-traceability` are scoped `[dlc-worker, main]`; this
brings the obligation to the feature agent without changing those rules.*

**2.2 Add a real-run verification step.**
Feature work has the same failure mode as data work: a green suite over an unexercised path.
> Verify by running the changed path and observing the result — the actual endpoint, CLI, or
> rendered surface. A test that passes is not evidence that the feature works.

*Pairs with widening `validate-against-real-dev-run` (§5).*

**2.3 Add the vacuity question to its review step.**
Its step 4 spawns a fresh reviewer. Add the contract:
> The review must state, for each acceptance criterion, the bug that would make it fail. A
> criterion no plausible bug could fail is unproven, not met.

**2.4 Add capability-aware verification.**
Its step 5 says "run the relevant scoped tests". Add:
> If the acceptance requires observing a surface this environment cannot provide (a browser, a
> display, a rendered page), say so and do not claim it. Report the verification you could
> perform and the one you could not.

*This is the §8.6 lesson, and `sdlc-worker` has no such clause.*

**2.5 Add the non-binding-changelog rule.**
Both worker configs say read the CHANGELOG "when asked how you have been improved... do not load
it otherwise" — so lessons are absent from context during work, and the 2026-08-31/09-02 lessons
recurred. Add to both configs:
> Before starting, read the most recent 5 CHANGELOG entries for lessons that apply to this kind
> of work. Apply them or state why they do not apply.

*Cheap, bounded (5 entries), and it makes the improvement loop actually close.*

---

## 3. `reviewer` — the highest-leverage changes

The reviewer is where certification happens, and this session produced two certifications that
were false: 7-of-36 criteria reported met, and a page certified "visual QA passed" while
unreadable. Four additions:

**3.1 The vacuity question (mandatory per criterion).**
> For every acceptance criterion, state the bug that would make it fail. If no plausible bug
> fails it, the criterion is VACUOUS — report it as unmet, not met.

*The `test_state_compatibility.py` gate and the "build_adapter is the ONLY place" registry test
both fail this question instantly. This is the single highest-value addition in the plan.*

**3.2 Capability guard (blocks false certification).**
> If you cannot observe the artifact your verdict depends on — the browser did not start, the
> model cannot read images, the surface is headless — you MUST NOT certify it. Report
> `unobservable` for that criterion, state the capability gap, and require a mechanical
> assertion instead.

*Directly prevents §8.6. The reviewer has `browser` and `inspect_image` tools, but nothing tells
it what to do when they do not work — and it silently proceeded.*

**3.3 Round-trip over existence.**
> An adoption claim ("X is the only place", "Y now uses Z") is not verified by finding the
> abstraction. Verify at least one PRODUCTION call site, or report the claim unverified.

*The registry test passed while 12 sites bypassed the seam.*

**3.4 Real-run evidence.**
> "Tests pass" and "build is green" are not evidence of correctness. Require a real run, a real
> query over real data, or an observed execution of the changed path.

**Not proposed:** any change to the existing spec-fidelity, over-engineering, or visual-surface
clauses. They are good.

---

## 4. `implementer`, `frontend`, `frontend-craft` — capability and scope

**4.1 `implementer`** (1.4KB — the thinnest config).
Add a bounded self-check before reporting done:
> Before reporting done: the changed path was executed (not only imported), and the acceptance
> criterion was observed. If it cannot be observed here, say so.

**4.2 `frontend` and `frontend-craft` — the §8.6 failure was here.**
Both assume a working browser and a vision-capable model. `frontend.md`: "You run on a
vision-capable model, so read the screenshot image directly." `frontend-craft` did exactly that,
the browser daemon failed, and it reported visual QA passed. Add to both:
> If the browser tool does not start, do NOT claim visual verification. Fall back to mechanical
> assertions you CAN compute (contrast ratios, computed styles, DOM structure, no-overflow
> measurements), report the capability gap explicitly, and mark the visual claim unverified.

*This is the most concrete, evidenced fix in the plan — it happened in this session.*

---

## 5. Rule changes (additive; nothing deleted)

Per `harness-coverage-and-hooks.md` §7.4, with `agents:` frontmatter corrected:

| New rule | `agents:` | Closes |
|---|---|---|
| G1 — mechanisms name their driver | `[dlc-worker, sdlc-worker, main]` | ADR-0012 gap |
| G2 — created artifacts name their consumer | *(omit — all agents)* | the six system-level defects |
| G3 — every contract has a named owner | *(omit — all agents)* | unowned seams |
| G4 — one contract, one shape | *(omit — all agents)* | handle encoding, dual clients |
| G5 — gates declare blind spots; unobservant verifiers must not certify | *(omit — all agents)* | vacuous gate + §8.6 |
| G6 — orphan check (runtime producer/consumer) | `[dlc-worker, main]` | disconnected halves |

**Plus two WIDENINGS (additions to scope, not deletions):**

- `validate-against-real-dev-run` → widen `agents:` to include `sdlc-worker`; generalise the
  obligation with a data example rather than data-only wording.
- `qa-codified-every-layer` → same, plus a feature-analogue clause ("every behavioral contract
  has a test that fails on a plausible bug").

---

## 6. NEW: the orchestrator config — the missing agent

This is the change the analysis points to as highest-value. Proposed
`~/.omp/agent/agents/orchestrator.md`, covering the four things nothing currently owns:

**6.1 Dispatch contract.** Every unit brief states: the contract the unit owns; the consumer of
anything it creates; the acceptance as a *demonstrated round-trip*; and the files in scope.
A brief missing any of these is not dispatchable.

**6.2 Integration is an owned deliverable.** Before dispatching a multi-unit plan, enumerate
every seam between units. Each seam gets a named owning unit, or is explicitly declared unowned
with a reason. *This is the root cause from the post-mortem, made into a step.*

**6.3 The acceptance gate.** The orchestrator verifies by running the real path, not by reading
the unit's test results. It confirms the verifier could actually observe the artifact.

**6.4 Capability check on acceptance.** Before accepting "verified", confirm the verifier had the
capability to observe it. If not, the claim is unverified.

Also: the orchestrator is where the new hooks' failures route, and where the MECE control set is
applied per phase.

---

## 7. Sequencing

Ordered by (value ÷ risk), each step independently landable:

1. **`reviewer` 3.1 + 3.2** — vacuity question and capability guard. Highest value; the
   reviewer is where certification happens, and both failures this session came through it.
2. **`frontend`/`frontend-craft` 4.2** — capability fallback. Directly evidenced; small.
3. **`sdlc-worker` 2.1–2.5** — closes the widest coverage gap and fixes a mis-scoped rule set.
4. **New rules G1–G6 + the two widenings** (§5) — additive, no deletions.
5. **Hooks H1, H3, H4** (`harness-coverage-and-hooks.md` §3, §8.3/8.4) — the mechanical
   enforcement that makes the rules bind.
6. **`dlc-worker` 1.1–1.3 + the CHANGELOG clause** — light, and it already has the rest.
7. **`orchestrator.md`** (§6) — the structural gap. Largest benefit, largest design effort;
   build it last so it encodes what the earlier steps taught.

---

## 8. What this plan deliberately does NOT do

- **Does not delete or weaken any rule or agent clause.** All changes are additions, or
  rewrites that preserve the obligation.
- **Does not add prose about behaviour the configs already mandate** — that is the documented
  failure mode (`dlc-worker/CHANGELOG.md`, and §10.3 of the post-mortem).
- **Does not attempt to hook judgment.** Seam ownership and the correctness of a named consumer
  stay rules and human decisions; only their mechanically checkable part becomes a hook.
- **Does not treat the agent configs as the whole fix.** Per §10.3, guidance binds only when it
  is loaded, enforced, and scoped. Rules state obligations; hooks enforce what is decidable;
  acceptance gates catch the rest.

---

## 9. CORRECTIONS (2026-09-13, after review)

### 9.1 §6 was filed in the wrong place — the orchestrator is NOT an agent

**Verified against `omp://task-agent-discovery.md`.** `~/.omp/agent/agents/*.md` defines a
**dispatchable task agent**: "OMP discovers user agents from `~/.omp/agent/agents/*.md`... Task
agents normalize into `AgentDefinition`", merged by name and spawned via the `task` tool. A file
`agents/orchestrator.md` would therefore create *another subagent type* named "orchestrator" —
which Main could dispatch to — not configure the main loop. Main would still have none of the
obligations, and the plan's highest-value item would bind nothing.

**Where Main's behaviour actually comes from:**

1. **`~/.omp/agent/AGENTS.md`** — the factory operating instructions. This is Main's config. It
   already carries orchestrator-shaped sections ("Subagent strategy", "Delegation gates",
   "Stop boundary"), which is precisely where the new obligations belong.
2. **Rules scoped `agents: [main]`** — the existing rule frontmatter already uses `main` as a
   valid scope (`contract-first-traceability`, `declared-dependencies`, `qa-codified-every-layer`,
   `validate-against-real-dev-run`, `wap-audit-before-publish`, and others all list it).

**Corrected §6 — the orchestrator obligations are delivered as:**

| Obligation | Home |
|---|---|
| Dispatch contract (every brief names the contract, the consumer of what it creates, the round-trip acceptance) | `~/.omp/agent/AGENTS.md` — extend the existing **Delegation gates** section |
| Integration is an owned deliverable (every inter-unit seam gets a named owner or is declared unowned) | New rule, `agents: [main]`, plus a line in AGENTS.md's delegation section |
| Acceptance gate (verify by running the real path, not by reading the unit's tests) | `~/.omp/agent/AGENTS.md` — a new **Acceptance** section |
| Capability check on acceptance (confirm the verifier could observe the artifact) | New rule, `agents: [main, reviewer]` |

No new agent is created. The obligations land where Main actually reads them.

**Note on recursion:** `task.maxRecursionDepth` defaults to `2`, so a subagent *can* spawn
subagents. That is a real capability, but it does not make an "orchestrator agent" the right home
for Main's obligations — the ambiguity §6 created was precisely that.

### 9.2 §2.5 contradicted a standing convention — replaced

`~/.omp/agent/AGENTS.md` states: the changelog "is a reference, not part of the prompt", and each
agent `.md` carries one progressive-disclosure line ending **"do not load it otherwise."**
§2.5 proposed adding "before starting, read the most recent 5 CHANGELOG entries" to both worker
configs. That leaves two contradictory standing instructions, and the convention exists for
context economy. It was a silent config edit against a global rule.

**Replaced with a fix that respects the convention and is stronger anyway:**

> When a CHANGELOG entry describes a failure class that has now occurred **twice**, promote it out
> of the CHANGELOG into a rule or a hook — because the CHANGELOG is deliberately not loaded during
> work, so a lesson that stays there cannot prevent its own recurrence.

This is the same conclusion as `postmortem-implementation-drift.md` §10.4 (promotion, not prose),
applied to the changelog mechanism itself. It costs no per-run context, keeps the convention
intact, and directly addresses the observed failure: the 2026-08-31 and 2026-09-02 lessons were
written down and recurred.

**Concrete evidence for the escalation rule:** the 2026-09-02 entry ("defects survived a '~95%
done' self-report") and this migration's 2-of-10 phase acceptance are the same class, twice.

---

## 10. `dlc-reviewer`: yes — the config already asks for an agent that does not exist

The question was whether a `dlc-reviewer` class makes sense. **It does**, and the strongest
argument is not a design preference — it is that `dlc-worker.md` already requires one:

> Step 5: "spawn a NEW reviewer subagent with clean context... so it verifies **data coherence,
> not just code**."

No such agent exists. The available reviewers are:

- **`reviewer`** (user-shadowed, 3.4KB) — app-shaped: spec fidelity, over-engineering,
  over-simplification, code smells, re-inventing the wheel, industry standards, UI surfaces.
  It has **zero data-coherence discipline**. Nothing in it asks whether the rows exist, whether
  the derived data is stale, or whether a producer has a consumer.
- **`security-reviewer`**, **`designer`**, **`scout`**, **`librarian`** — unrelated.

So `dlc-worker` hands a task to a reviewer that cannot perform it. That is the same class of
defect as the missing orchestrator: **a config instructing a handoff to a capability that was
never built.**

### 10.1 Why the split is principled, not proliferation

The `dlc-worker` / `sdlc-worker` split exists because **data work is stateful and app work is
not** — changing a derivation formula does not self-correct existing rows. **The same asymmetry
applies to verification:**

| | App verification (behavior) | Data verification (state) |
|---|---|---|
| Question | Does the path work? | Is the stored state correct *now*? |
| Evidence | The endpoint responds, the UI renders | The rows exist, the grain is unique, the derived layer is not stale |
| Failure mode invisible to code review | — | code-only objects; orphan producers; stale derived data |

A code-shaped review certified a broken data system in this migration. A **state**-shaped review
is the direct remedy, and it is not something the generic reviewer can absorb by adding a
paragraph — the checks are different in kind.

### 10.2 Proposed definition

`~/.omp/agent/agents/dlc-reviewer.md`, mirroring `dlc-worker` the way `sdlc-worker` mirrors it:

```md
---
name: dlc-reviewer
description: Independent verifier for DATA changes — checks data coherence and warehouse state (not just code): materialization, orphaned producers/consumers, staleness/drift, grain uniqueness, provenance, and criterion vacuity.
model: "@task"
thinkingLevel: medium
tools: read, grep, glob, lsp, bash, eval, web_search
---
```

Read-only by construction: no `edit`, no `write`. It verifies; it does not fix.

### 10.3 Its checklist — the seven checks the generic reviewer cannot make

1. **Materialization.** Every object the change declares exists in the live store, and has rows —
   or carries a recorded justification for zero rows. A declared-but-absent object is a failure.
2. **Orphan check.** Every producer has a named consumer; every consumer has a named producer.
   Report any orphan as a blocker. *(This is the check that would have caught the disconnected
   halves in one pass.)*
3. **Staleness / drift.** Is stored derived data stale under the new logic? Count rows still
   carrying the old derivation's signature.
4. **Grain / uniqueness.** Is each key unique at the grain it claims? Check against every
   dimension the source carries — not just the columns the change mentions.
5. **Provenance.** Does every derived layer record what produced it, from what, with which logic,
   and when?
6. **Criterion vacuity.** For every acceptance criterion, state the bug that would make it fail.
   **If no plausible bug fails it, the criterion is VACUOUS — report it unmet, not met.**
7. **Capability guard.** If a check needs an observation this environment cannot make, report
   `unobservable` and refuse to certify that criterion.

Plus the two generic disciplines worth inheriting: round-trip over existence (verify a
**production** call site, never just the abstraction), and real-run evidence ("tests pass" is not
evidence that the data is right).

### 10.4 Do NOT also create `sdlc-reviewer`

The generic `reviewer` **is** the app reviewer. It already covers spec fidelity, UI surfaces, code
smells, and industry standards, and it drives the browser for rendered verification. Adding an
`sdlc-reviewer` would duplicate it and invite divergence. The split is:

- **`reviewer`** — default. App/feature/UI changes.
- **`dlc-reviewer`** — data/model/pipeline/schema changes.

Two agents, chosen by the change type. The orchestrator picks, which is the correct place for the
judgment (§9.1).

### 10.5 Where the existing `reviewer` still needs work

The §3 changes stand and are not superseded by this: the vacuity question (3.1) and the capability
guard (3.2) belong in **both** reviewers — they are not data-specific. `dlc-reviewer` inherits them
plus the seven state checks; `reviewer` gains them as proposed.
