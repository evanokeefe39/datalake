# MECE learnings from the Enrichment v3 failure

Synthesis of a four-seat expert panel (`panel-software.md`, `panel-data.md`,
`panel-process.md`, `panel-adversary.md`) over `postmortem-implementation-drift.md`.
This is the artifact the remediation plan should be built against.

---

## 1. The MECE control set

The partition is by **the question a control answers**, not by defect or by technology. Five
questions, five controls. Mutually exclusive because each answers a different question;
collectively exhaustive because any control you could add to a migration answers one of them.

| # | Control | Question it answers | Failure mode when absent |
|---|---|---|---|
| C1 | **Specification** | What must be true? | The mechanism exists on paper with no driver or consumer |
| C2 | **Accountability** | Who answers for it? | A contract has no owner; divergence is nobody's job |
| C3 | **Interface** | What shape does it take? | The same contract is expressed two ways; shape is undeclared |
| C4 | **Sequencing** | When does it happen? | A store is retired before its readers move; ordering inverts |
| C5 | **Verification** | How do we know? | The check asserts existence or status quo, and cannot fail |

**MECE applies to the control set, not to the defects.** A defect is a symptom and can be
produced by more than one missing control. That is why the post-mortem's class taxonomy
(a)-(e) failed its MECE test: it partitioned *defects*, so items landed in two classes or none.
Partition the controls; map the defects to them many-to-one.

The panel found the post-mortem's (a)-(e) taxonomy is **exhaustive but not mutually exclusive**:
(a) dispatch shape, (c) instructions, and part of (e) plan are one orchestration failure observed
at three altitudes; defect #7 double-counts one ADR silence as both (e) and (c); defects #10 and
#12 are hybrid rows whose class follows the narrative rather than the fix.

---

## 2. Defect → control map

| Defect | Missing control | Software / Data manifestation |
|---|---|---|
| `enrichment_harvested` has no writer → pipeline stalls after one cycle | **C1** + **C2** | Data: a state transition specified as a set expression with no named event |
| Retry: `MAX_ATTEMPTS` and backoff exist, no driver mints a round | **C1** | Data: a mechanism with no actor and no trigger |
| `silver_enrichment_quarantine` mandated, never declared, no consumer | **C1** | Data: a control surface with no reader and no redrive policy |
| Drain writes partitions nothing consumes; submit reads a table nothing writes | **C2** | Data: runtime data topology — producer/consumer pairing unowned |
| Seam adopted by 1 of 4 flows; 12 bypass sites | **C2** + **C3** | Software: static code topology — port built, composition root never wired |
| Two qwen HTTP clients (`qwen_client` vs `ServiceBackedAdapter`) | **C3** | Software: one boundary, two implementations, divergent terminal predicates |
| Handle encoding diverged: `'|'` (submit) vs `','` (adapters) | **C3** | Data: one contract, two serializations, no round-trip fixture |
| Partition key embeds round 0 → latent double-submit under retry | **C3** | Data: key shape chosen without checking the consumer's needs |
| `max_tokens` job-level vs `Item` per-item | **C3** | Software: the port cannot express a parameter the transport carries |
| Mart grain omits `platform` though the source carries it | **C3** | Data: key not unique at the grain claimed |
| Queue retirement in Phase 7; drain rewired in Phase 2 | **C4** | Data: retirement deferred past the rewiring; the zombie contract is kept alive by a raising guard |
| `test_state_compatibility.py` green because the catalog still asserts the old world | **C5** | Data: the gate proves status quo, so it cannot certify a migration |
| Exit criteria phrased as existence ("is the ONLY place a provider is named") | **C5** | Software: an adoption claim proved by a registry unit test |
| Every test injects a fake on one side of a seam | **C5** | Software: no test asserts a handoff between two real halves |
| Zero bronze rows; conform has no caller; 12 objects code-only | **C5** | Data: no real-run gate; nothing observed the path end to end |
| `classify_error` sends unknown exceptions to TERMINAL | **C1** + **C5** | Software: undefined behaviour for an unhandled case, unchecked |
| Layering breach: enrichment imports Instagram config; `DagsterInstance.get()` fallbacks | **C2** + **C3** | Software: dependency direction and injection guarantees unowned |

---

## 3. "Seams need owners" vs "producer/consumer checks" — DISTINCT, and the user was right to ask

All four seats independently reached the same verdict: **they are not the same, and collapsing
them is itself a defect.**

|  | Seam ownership | Producer/consumer check |
|---|---|---|
| Axis | **Static code topology** (software) / accountability | **Runtime data topology** (data) / observability |
| Question | Who answers for this boundary? | Is data actually moving across it? |
| When | Design time — at decomposition and dispatch | Acceptance time — on a real run |
| Failure mode | Divergence: two implementations of one boundary (#5, #6, #13) | Orphaning: a producer with no consumer, a consumer with no producer (#1, #2, callerless conform) |
| Addressee when red | The named owner | Nobody, unless a check also has an owner |

**They are independently fallible, and this project proves both directions:**

- **Owner without a check.** The HTML post-mortem (§8.6): one agent owned the page outright. No
  working check existed. It shipped unreadable. Ownership was never the missing piece.
- **Check without an owner.** When an orphan-lineage check goes red, there must be a name to
  route it to. The §4 blast-radius register *named* the drain as Phase 2's consumer — ownership
  existed on paper — and nothing enforced it.

`panel-software.md` puts the decisive evidence best: this project **had owned seams** (the §4
register named the coupling) and still shipped existence-shaped criteria. **Ownership alone is
provably insufficient.** The correct single learning, if you must merge them, is conjunctive:

> Every contract gets a named owner in the dispatch **AND** a round-trip assertion in
> acceptance. Neither clause alone would have caught this migration.

Keep them separate in the remediation plan. Merging them yields "write the contract down" where
an integration run was needed.

---

## 4. Industry patterns that were missed

Marked APPLIES (would genuinely have changed the outcome) or HINDSIGHT (invoking it would be
retrospective).

### C1 Specification
- **Every mechanism names its driver.** Retry, quarantine, reconciliation each name an actor and
  a trigger. *APPLIES — three mechanisms here had none.*
- **Data contracts / schema registry.** The consumer declares what it needs; the producer
  publishes what it guarantees. *APPLIES — the remediation is written as prose, never as a
  consumer-declared contract.*

### C2 Accountability
- **Lineage with an orphan check.** Every asset declares its inputs AND its consumers; the gate
  fails on an orphan producer or consumer. *APPLIES — and note: this, not a unit test, is the
  standard control for the read/write asymmetry. The project HAD the static manifest (plan §4)
  and used it three ways wrong: never converted to runtime lineage (no edge drain→submit), never
  converted to per-dispatch acceptance questions, never run as an orphan audit (which would have
  enumerated all 12 code-only objects in one pass).*
- **Composition root (hexagonal / ports-and-adapters).** The miss was not the hexagon — the port
  was designed and the adapters built. The miss was the **composition root**: the place where a
  port is bound to an adapter for production. *APPLIES — strongest framing of the seam defect.*
- **Maker-checker / four-eyes.** *APPLIES — the configs carried the reviewer habit; the loop
  never ran it against the contract question. This document's §9 correction is the control
  working, one level up.*

### C3 Interface
- **Consumer-driven contract testing.** The pattern, not the tool. *APPLIES — this is precisely
  the "fake on one side" problem the plan's own §8 admits.*
- **Interface-first dispatch.** When two units touch one format, the format is its own first
  unit. *APPLIES — two slices each encoded handle-to-jobs independently and diverged.*
- **Module vs component.** A component is only a component when it can be verified without
  another existing. *APPLIES as diagnostic vocabulary — all six "components" here passed that
  test individually.*
- **Interface segregation / abstract factory.** *HINDSIGHT — the `max_tokens` wrinkle caused one
  documented bypass, not the failure.*

### C4 Sequencing
- **Expand-contract (parallel change).** Add the new path, dual-write or dual-read, migrate
  readers, then remove the old. *APPLIES — the biggest data-side omission. The inverted
  sequencing created the zombie queue table "preserved by code", and the absence of a
  parallel-run window is the root of the vacuous compatibility gate.*
- **Strangler fig.** *APPLIES — same family; the legacy queue should have been starved, not
  dropped on a phase boundary.*

### C5 Verification
- **Walking skeleton / tracer bullet.** One real item through the whole path on day one.
  *APPLIES — the most damning miss. Bronze has zero rows: nothing ever ran live. A single tracer
  bullet would have exposed every disconnection in the same afternoon.*
- **Verification vs validation.** Verification asks "did we build it right"; validation asks "did
  we build the right thing". *APPLIES — §3.7's vacuous gate is exactly this confusion. The gate
  verified the status quo and validated nothing.*
- **Write-Audit-Publish.** *APPLIES — designed in the spec, never implemented. The post-mortem
  never checks whether it was.*
- **Data quality gates** (dbt tests, Great Expectations, Soda) and **freshness/SLA assertions**.
  *APPLIES — zero failing DQ gates existed at any level, so data defects were invisible by
  construction. Tool choice is hindsight-adjacent; the control is not.*
- **Backfill idempotency by natural key.** *APPLIES — the landing is idempotent and the
  post-mortem does not credit it; the Phase 5 classification backfill has no idempotency or
  reconciliation instrument, and the post-mortem does not flag it.*
- **Medallion replay guarantee.** The design claims "conform is a pure function of bronze".
  *APPLIES and is UNPROVEN, not partially met — it needs one real response through land→conform.
  The post-mortem buries this keystone claim as a single bullet.*
- **Lineage vs provenance.** Provenance columns are done well. Lineage control is absent.
  *APPLIES — and the read-side fix is lineage, not provenance.*
- **Definition of Done as a contract.** A criterion is a contract only if an independent
  observer is bound to its evaluation. *APPLIES — 7 of 36 met, yet phases reported complete.*

---

## 5. Where this belongs in the harness

> **SUPERSEDED IN PART — read `postmortem-implementation-drift.md` §10 before acting on this.**
> This table was drafted before the real config was read. `~/.omp/agent/agents/dlc-worker.md`
> **already mandates most of these items** (blast radius, consumer check, fresh review,
> coherence-not-tests, producer guardrails, dormant-vs-broken, decompose-by-deliverable,
> never-trust-a-self-report). The finding is therefore **non-binding rules, not missing rules** —
> and adding prose is the documented failure mode. The config's own CHANGELOG records this exact
> class of lesson being learned on 2026-08-31 and 2026-09-02, and recurring here anyway. The
> table below is still useful as a *placement* guide; the framing is corrected in §10.4.

The critical finding from `panel-process.md`: **the `dlc-worker` and `sdlc-worker` configs
already carry the behavioural prose** — blast radius, fresh-context reviewer, never trust
self-report, guardrail-presence. Adding more prose is the documented failure mode (the agents'
own CHANGELOG records briefs carrying the offload prose and not following it).

Two real gaps: **no hooks exist for any mechanically checkable signal**, and **orchestrator-side
learnings have no config home**, so they land on the wrong agent.

| Learning | Best home | Why |
|---|---|---|
| Every mechanism names its driver | **Skill** (`schema-migration` / new `migration-verification`) | Design-time knowledge, applied per mechanism |
| Every contract gets an owner + a round-trip assertion | **Rule** | Universal invariant; applies to every unit |
| An unobservant verifier must not certify | **Rule** | Universal invariant; the §8.6 CSS case |
| Assert round-trips, never existences | **Rule** | Universal; pairs with the above |
| Provider names appear only in adapter modules | **Hook** (CI grep) | Purely mechanical; catches all 12 bypasses |
| Every new object has rows in the live DB | **Hook** | Mechanical; zero rows blocks merge |
| Schema catalog matches the live database | **Hook** | Mechanical; would have caught the vacuous gate |
| Every text colour ≥4.5:1 on its background | **Hook** | Mechanical; caught all three CSS bugs |
| A gate declares what it cannot detect | **Skill** (reviewer) | Judgment, but teachable |
| Existence is never acceptance | **Skill** (reviewer) | Judgment, but teachable |
| Verify against real data before trusting a module | **Agent** (`dlc-worker`) | Behavioural — already present; needs a gate, not more prose |
| Producer/consumer naming in the brief | **Orchestrator** — currently no home | The gap. Needs a place to live. |

The last row is the structural gap: the learnings that would have prevented this are
**orchestrator-side**, and no agent config owns the orchestrator. That is the highest-value
harness change in this document.

---

## 6. What the panel corrected in the post-mortem itself

- **§8.4 retracted.** "No unit was ever briefed to migrate the seam call sites" is an inference
  from the commit trail presented as a dispatch record. The evidence does not discriminate
  assignment failure from half-executed brief. See §9 of the post-mortem.
- **Root cause is non-discriminating as stated.** "Per-component dispatch + per-component
  acceptance" is present in every migration this process has run, including successful ones. The
  discriminating form is the compound: unowned contracts AND no acceptance gate capable of
  observing a real run.
- **"The architecture is sound" is too generous.** ADR-0011 (statics) sound; ADR-0012 (dynamics)
  **underspecified**. An underspecified dynamic surfaces as apparent drift under any dispatch
  quality.
- **"7 of 36" is a valid falsifier of "phases complete", invalid as a health metric.** The
  post-mortem itself shows the criteria are unreliable instruments, then trusts their integer
  sum — committing the same sin it diagnoses. The MET/PARTIAL boundary is analyst discretion.
- **Proximate cause is ACCEPTANCE, not dispatch shape.** Per-component dispatch guarantees
  defects are created; the acceptance instrument is what let them land. Counter-evidence to the
  dispatch-first reading: `facets_batch.py:292` is the one unprompted seam adoption in the
  migration.
- **Evidence-chain caveat.** Live-DB audit facts are mechanical and stronger than the claims they
  refute. But several qualitative verdicts echo one source (`review-interfaces.md` §7) through
  five documents presented as triangulation. Subagents rendered a verdict on subagents with the
  conflict unregistered.

---

## 7. Corrected placement: promotion, not prose

The config already contains the guidance (see `postmortem-implementation-drift.md` §10). So the
action is not to author more instructions but to **promote each learning into a control that
cannot be skipped.** Four placement targets, in order of binding strength:

| Target | Binds? | What belongs there |
|---|---|---|
| **Hook / CI gate** | Yes — fails the build | Anything mechanically checkable: provider names outside adapters; zero rows in a new object; live-DB-vs-catalog reconciliation; contrast assertions |
| **Failing test** | Yes — fails the suite | Round-trip assertions; idempotency (run twice, compare); producer/consumer handoff on one shared fake |
| **Rule** | Partly — always in context | Universal invariants: every contract gets an owner AND a round-trip assertion; an unobservant verifier must not certify; assert round-trips, never existences |
| **Agent config prose** | Weakly — diluted by context, not enforced | Only genuinely behavioural things. **There is nothing left to add here for this failure.** |

### 7.1 The three structural gaps, restated

1. **Promotion gap.** A CHANGELOG lesson is not loaded during work (`do not load it otherwise`)
   and therefore binds nothing. Every lesson in §10.2 must become a hook or a failing test.
2. **Orchestrator gap.** The learnings that would have prevented this are orchestrator-side —
   seam ownership, real-run acceptance, producer/consumer pairing. Step 5 of the config hands
   the reviewer gate *up* to the orchestrator. **No agent config owns the orchestrator.** This is
   the highest-value harness change available.
3. **Scope gap.** Step 2 requires enumerating consumers of what you *change*. It must also
   require naming the consumer of what you *create* — a new module's consumer does not exist yet,
   and that is precisely where all six system-level defects lived.

### 7.2 The one-line version

The factory's rule — *a guarantee you cannot observe is not a guarantee* — applies to its own
agent configs. Guidance that is not loaded, not enforced, and not scoped to the failure is not
guidance. The remediation is promotion into hooks and tests, plus giving the orchestrator a home.
