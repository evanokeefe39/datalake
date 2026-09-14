# Enrichment v3 migration — post-mortem

What happened when a 7-phase lakehouse migration drifted from its design, why it was invisible
to every gate the team had, and what the harness should change so it cannot happen again.

**The headline: 7 of 36 exit criteria were genuinely met, the branch was green, and the pipeline
could not complete one cycle.**

---

## Start here

| If you have... | Read |
|---|---|
| 2 minutes | [`postmortem.html`](postmortem.html) — the visual version: KPI strip, the six failures, the Five Whys, four figures |
| 20 minutes | [`postmortem.md`](postmortem.md) — the full analysis, including why existing guidance did not bind |
| The architecture | [`diagrams`](diagrams) — three states: old (working) → current (incorrect) → target |

---

## The three states

| State | Diagram | What it shows |
|---|---|---|
| **OLD — working** | [`diagrams/state1-old.html`](diagrams/state1-old.html) | The queue-driven pipeline that actually ran and produced 9,576 enriched rows. The baseline. |
| **CURRENT — incorrect** | [`diagrams/state2-current.html`](diagrams/state2-current.html) | **Start here.** The branch today: 12 objects code-only, the two halves not meeting, the missing producer, the bypassed seam. |
| **TARGET** | [`diagrams/state3-target.html`](diagrams/state3-target.html) | The layered model per ADR-0011/0012/0013: verbatim bronze → deterministic conform → six silver tables → four gold marts, Dagster-native. |

The narrative behind the three states is
[`analysis/three-state-articulation.md`](analysis/three-state-articulation.md).

---

## Contents

### The analysis
- **[`postmortem.md`](postmortem.md)** — root cause, the Five Whys, fault allocation, and §10:
  why the guidance that already existed did not bind.
- **[`analysis/learnings-mece.md`](analysis/learnings-mece.md)** — the **five MECE controls**
  (specification / accountability / interface / sequencing / verification), every defect mapped
  to the control it was missing. This is the artifact the remediation is built against.
- **[`analysis/harness-coverage-and-hooks.md`](analysis/harness-coverage-and-hooks.md)** —
  rule-vs-hook coverage **per agent**, and the hook specs. Contains a correction: coverage is
  per-agent, and the rules scoped `[dlc-worker, main]` never reach `sdlc-worker`.
- **[`analysis/three-state-articulation.md`](analysis/three-state-articulation.md)** — the source
  for the diagrams.

### The evidence — audits against the LIVE database
Not against the branch. Every number here was queried.
- [`audits/p1-p2.md`](audits/p1-p2.md) — 18 criteria: 3 met, 4 partial, 4 unmet, 2 vacuous
- [`audits/p3-p4.md`](audits/p3-p4.md) — 8 criteria: Phase 4 was entirely test-world
- [`audits/p5-p6.md`](audits/p5-p6.md) — Phase 5 UNMET, Phase 6 0-of-4 marts materialized

### Independent reviews
- [`reviews/architecture-soundness.md`](reviews/architecture-soundness.md)
- [`reviews/interfaces.md`](reviews/interfaces.md) — the 12 seam bypass sites
- [`reviews/round-semantics.md`](reviews/round-semantics.md) — the retry-key defect

### The expert panel
Four seats with distinct lenses. They disagree in useful ways.
- [`panel/software.md`](panel/software.md) — overstatement and missed patterns
- [`panel/data.md`](panel/data.md) — **rules the central framing "unfair as stated"**: ADR-0011
  (statics) is sound, ADR-0012 (dynamics) is underspecified
- [`panel/process.md`](panel/process.md) — the proximate cause is acceptance, not dispatch
- [`panel/adversary.md`](panel/adversary.md) — **the discriminating-vs-non-discriminating test**,
  and the claim that would be most damaging if wrong

### The plans
- **[`plans/remediation-plan.md`](plans/remediation-plan.md)** — 10 units with a declared
  dependency graph, a hybrid recommendation (validation spike first), and the 9,576 real
  `gold_analyses` rows explicitly protected.
- [`plans/agent-improvement-plan.md`](plans/agent-improvement-plan.md) — changes to the
  `dlc-worker`, `sdlc-worker`, `reviewer`, `implementer` and `frontend` agents, plus the
  orchestrator gap.

---

## The three findings worth carrying away

**1. Integration is not an emergent property of component completeness.** Per-component dispatch
under per-component acceptance cannot detect inter-component drift, even with perfect execution.
The failure was set at dispatch-design time, before any worker saw a prompt. Every test injected
a fake on *one side* of a seam, so no test exercised a seam.

**2. A gate is evidence only of what it can assert.** `test_state_compatibility.py` passed 77
tests because the schema catalog still asserted the *old* world — so it could not detect that the
migration never happened. This pattern recurred twice more in the same session: a page certified
"visual QA passed" while unreadable, and a hook proposed to catch a vacuous gate that was itself
vacuous.

**3. The rules were not missing — they were not binding.** `dlc-worker` already mandates the
blast-radius enumeration, the consumer check, the fresh review, the producer guardrail and
"never trust a self-report". Its own CHANGELOG records the same lesson being learned twice, and
recurring anyway. Guidance that is not *loaded*, not *enforced*, and not *scoped to the failure*
is not guidance. The remediation is promotion into hooks and failing tests, not more prose.

---

## Honest caveats

- Several verdicts echo a single source (`reviews/interfaces.md` §7) through five documents
  presented as triangulation.
- The post-mortem retracts one of its own claims in §9: an assertion about what the workers were
  briefed to do was an inference from the commit trail, not a dispatch record.
- "7 of 36" is a valid falsifier of "phases complete", but **invalid as a health metric** — the
  document shows the criteria are unreliable instruments and then trusts their integer sum.
