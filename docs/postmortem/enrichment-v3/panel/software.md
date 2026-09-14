# Expert Panel — Software Design & Architecture seat

Reviewer: PanelSoftware. Document under review: `docs/postmortem/enrichment-v3/postmortem.md`
(hereafter "PM"). Evidence base: the three audits, three prior reviews, the master plan
(§8 read at line 439+), ADRs. All line references are to the PM unless noted.

---

## 1. Findings table: handwaved / overstated / understated / conflated

| # | Claim (quoted) | Flaw | Correction |
|---|---|---|---|
| 1 | "the failure was therefore determined at dispatch design time, before any worker saw a prompt" (Why 5) | **OVERSTATED — and contradicted by the PM's own defect table.** §3 assigns defects 10–11 to class (d) worker execution ("genuine worker shortcuts", "worker execution quality"). Those were NOT determined at dispatch time; they were committed by workers inside their remits. Also, (b)-class failures are acceptance-time, not dispatch-time. | The defensible claim: the *system-level* failures (1–6, 8, 12) were structurally guaranteed by the dispatch shape. Drop "determined" for "made likely"; drop the universal quantifier. |
| 2 | "The workers executed their dispatches competently" (headline verdict) | **OVERSTATED / CONFLATED.** Conflates *scope fidelity* with *engineering competence*. §3 defect 11 is class (d); §4 explicitly convicts workers of the `DagsterInstance.get()` fallbacks ("the worker had everything needed to do it right and chose the permissive path") and of silent-failure defaults. `classify_error` mapping unknown exceptions to TERMINAL is a data-loss-class defect in an enrichment pipeline — the PM calls these "second-order," which understates operational risk even if they didn't cause the migration failure. | Correct headline: "workers were scope-faithful; two localized quality-failure classes are worker-attributable (PM §3 classes d) and are not second-order for operations, only for the migration outcome." |
| 3 | "the agent converged one lifecycle and reported success; that is a faithful reading of the dispatch, not a mistake" (§1, `872904e` row) | **OVERSTATED / HANWAVED.** The dispatch said "converge BOTH lifecycles." One of two is not a faithful reading — it is a half-execution of an unambiguous instruction. The escape hatch ("both was not unpacked into call sites") rescues the *scope boundary* question, not the *reading* claim. A competent engineer converging 1 of 2 either escalates the ambiguity or states in the completion report "the Gemini path call sites remain; are they in scope?" The PM's own §6 signal for defect 4 ("dispatch must enumerate the call sites") concedes the enumeration was knowable from the plan's §1 table — the worker could also have enumerated them. | Verdict (see §4 below): faithful within file-scope, but failing the escalate-or-report obligation. "Faithful, not a mistake" claims too much. |
| 4 | Entire §1 table, including the "Agent could NOT see" column | **HANDWAVED.** §1 opens "What each subagent was *likely* told" and reconstructs dispatches from the commit trail — the PM admits inference ("likely dispatch") yet §1–§5 reason as if the dispatch texts were in evidence. Only one claim in the whole document carries an inference label (§4, `enrichment_harvested`); the rest of the mechanism narrative inherits §1's reconstructed ground truth silently. The PM itself notes the orchestrator is the only party who can answer §8.4's question — so until the orchestrator confirms, §1's mechanism is hypothesis, not diagnosis. | Relabel §1 as a hypothesis consistent with the commit trail; mark the §5 verdict as conditional on orchestrator confirmation of the dispatch texts. (The PM partially does this in §8.4 but does not downgrade §1–§5.) |
| 5 | "integration was treated as emergent rather than an owned deliverable" (headline / Why 5) | **CONFLATED** — bundles three distinct mechanisms with three distinct fixes: (i) no integration *dispatch* (assignment gap), (ii) a verification instrument incapable of round-trips (acceptance-instrument gap, PM class b), (iii) a *sequencing* error — queue retirement in Phase 7 vs drain rewiring in Phase 2 (plan gap, class e). "Emergent" names none of them; the PM's own Why 2 names (iii) precisely and buries it. | Split the headline into the three mechanisms; only (i) is an "ownership" failure. (ii) survives even perfect dispatches; (iii) survives even per-phase integration tests if the phases never coexist. |
| 6 | "The plan even pre-wrote its own postmortem: §8 … contains the sentence that should have been in the dispatches from the start" (Why 4) | **UNDERSTATED, in a revealing direction.** Plan §8 was added 2026-09-13 *after* the drift was discovered. Citing it as "the plan knowing" blurs evidence created before vs after the fact. The PM should rely only on pre-existing artifacts (§4's blast-radius register, plan §1 call-site table) for "the plan knew." | Distinguish: the plan *knew the coupling* (pre-existing §4 register) but never wrote the round-trip rule until after the failure. The knowledge-of-coupling claim stands; the "pre-wrote its own postmortem" phrasing overstates the plan's prescience. |
| 7 | §8.6 addendum ("the thesis reproduced itself during this post-mortem") | **CONFLATED (mild).** The CSS episode is a verifier-that-cannot-observe failure, not a decomposition/integration failure. It genuinely illustrates §3.8 (vacuous gate) but not §3.1–3.6 (no owned handoff). Presenting it as "the thesis reproducing itself" imports an unrelated failure mode as confirmation. | Keep it as corroborating §8's vacuous-gate finding only; say so explicitly. |
| 8 | "The seam is adopted by 1 of 4 production flows; 12 bypass sites" (PM §3 defect 4, consistent with audits) | **CORRECT** — and correctly traced to class (a). No correction. | — |

---

## 2. Root cause under strict scrutiny

**Test applied:** a root cause must be (A) actionable and (B) explain why rival
explanations are insufficient.

**Does "integration was treated as emergent rather than an owned deliverable" survive?**
Partially. It is actionable (assign integration as a dispatchable deliverable with an
owner) and it does explain defects 1–6, 8, 12. But it fails the *altitude* test: it is
the mechanism by which the failure happened, not the deepest reason. The PM's own Why 2
contains the stricter root cause and does not promote it:

> "The master plan *knew* the cross-dependencies — §4's consumer/blast-radius register
> names `ig_posts_gen_batches` as Phase 2's consumer … — but the queue retirement … was
> scheduled in Phase 7, and no exit criterion in any phase says 'submit consumes what
> the drain produces.'" (Why 2)

**Stricter root cause (named):** *Known cross-component couplings were recorded in the
plan but never converted into dispatch content or acceptance criteria — a translation
loss at the plan→dispatch boundary.* This is stricter because:

- It explains why the failure happened *despite* the orchestrator doing the hard
  analytical work (§4's register correctly identified the load-bearing consumer). The
  "treated as emergent" framing implies the orchestrator never thought about
  integration; the evidence shows the opposite — the analysis existed and was dropped
  in translation. That is a different (and more damning, and more fixable) failure:
  the knowledge existed and did not propagate.
- It is more precisely actionable: the fix is not "own integration" (vague, already
  everyone's job in some sense) but "no phase dispatch without converting every §4-style
  coupling into (a) a named contract owner and (b) a round-trip acceptance assertion."
- It subsumes the rival explanation "interface contracts were never written down":
  partially true, but the plan's register WAS a written (implicit) contract that got
  lost; the failure is contractual knowledge existing in one artifact and absent from
  the two artifacts that drive behavior (briefs, acceptance).

**Verdict:** the stated root cause survives strict scrutiny only as the proximate
mechanism. The stricter root cause — coupling knowledge lost in plan→dispatch
translation — should replace it, with "integration as emergent" demoted to the
consequence.

---

## 3. Missed industry-standard patterns

| Pattern | What it is | Would it have applied here? |
|---|---|---|
| **Consumer-driven contract testing** (Pact et al.) | The *consumer* of an interface writes the test that pins the producer's behavior; the producer's CI runs against consumer expectations. | **APPLIES** — the single most on-point miss. PM/plan §8: "every test injected a fake on ONE side of a seam… two implementations against two different encodings of a spec." That is precisely what CDC testing exists to prevent. Hindsight caveat: Pact the *tool* targets HTTP providers; the needed artifact here is a lightweight round-trip contract test (produce → consume → compare), which the plan now specifies. Pattern applies; tool does not. |
| **Walking skeleton / tracer bullets** (Cockburn; Hunt/Thomas) | A thin end-to-end slice through every layer, running, before breadth is added. | **APPLIES — the most damning miss.** The audits show `bronze_enrichment_raw` has ZERO landed rows, four gold marts exist only as code, the live DB still runs the legacy path. One day-one tracer bullet — land one real post, conform it, materialize one mart row, read it from `v_post_detail` — would have exposed every disconnected half immediately. Not hindsight: the pattern predates the project and its absence is visible in the commit order (six per-component commits, no end-to-end commit). |
| **Strangler fig** (Fowler) | Incrementally route traffic from old to new system behind a facade; retire old pieces as slices go live. | **APPLIES with caveats.** The migration was effectively big-bang on a branch: 6 commits, nothing live until "phases complete." A strangler approach (rebind one serving view to silver, verify live, proceed) would have made the vacuous `test_state_compatibility` gate fail loudly, because each rebind would be observable in the live DB. Caveat: a Dagster/DuckDB pipeline has less "traffic routing" surface than a web system, so the pattern applies in its *incremental-cutover* sense, not its proxy-routing sense. |
| **Hexagonal / ports-and-adapters** (Cockburn) | Domain logic sits behind ports; adapters implement ports; a **composition root** wires adapter to port at startup. | **APPLIES — but the miss is the composition root, not the hexagon.** The seam (ProviderAdapter Protocol) is a correctly designed port; the adapters exist; `run_lifecycle` is the application core. What was never built is the *wiring*: nothing chooses an adapter per production flow, so 3 of 4 flows never cross the port. The pattern's own rule ("an application is a plugin to the domain, assembled in main") was violated — `build_adapter` has zero production callers. The PM hints at this ("built to spec, adopted by no dispatch") but never names the composition root as the missing piece. |
| **Module vs component** (Kiczales; Szyperski) | A module is a unit of *code* (import/deploy unit); a component is independently *deployable and verifiable* with its own contract. | **APPLIES as the diagnostic vocabulary.** The dispatches were per-module but named and accepted as if per-component. None of the six "components" was independently verifiable against the system — every one of the 36 exit criteria was checkable without any other component existing. Naming this distinction at dispatch time ("is your deliverable verifiable without a fake on any side?") would have surfaced the gap mechanically. |
| **Expand–contract (parallel change)** | Add the new interface alongside the old (expand), migrate consumers, then remove the old (contract) — never both in one step. | **APPLIES.** The migration inverted it: drain rewired to Dagster partitions in Phase 2 while the queue's *readers* survived (`submit.py:77`, `_require_legacy_queue_tables` raising if the table is absent) and writers retired in Phase 7 — a contract phase executed before its expand phase completed, producing the zombie table "preserved by code." The PM describes this correctly (Why 1) but never names the pattern, which is the standard cure. |
| **Abstract factory / ISP analysis of the seam** | Interface-granularity questions: should `submit` take a `JobSpec` (job-level `max_tokens`)? | **HINDSIGHT.** The `max_tokens` gap (PM §4) is a real but minor seam-design wrinkle; it caused one documented bypass, not the migration failure. Flagging ISP as a missed pattern would be armchair design; the seam's verb signature is defensible for its 1 adopted flow. |
| **"Emergence in the large"** (systems view: composition produces properties components lack) | — | **APPLIES only as the refutation.** The PM's thesis is that emergence was trusted where composition was owed. The correct counter-pattern is the walking skeleton + round-trip acceptance; no additional pattern is needed. |

---

## 4. Verdict on "the workers did their briefs"

**Scope fidelity: mostly yes. Engineering competence: overstated by the headline.**

- **Faithful-within-scope holds for 5 of 6 commit slices.** The drain worker derived the
  guard from the instance as told; the prompt-identity slice "got the one-function
  treatment and is clean" (PM §1); the `max_tokens` bypass is documented in the
  docstring with the exact interface reason — that is *above* the bar, not merely
  competent.
- **But three things break the clean exoneration:**
  1. *Unescalated ambiguity.* "Converge both lifecycles" → converge one, report success.
     Regardless of how the brief was worded, a senior engineer who converges half of an
     unambiguous instruction must either escalate or report the residue. Neither
     happened (or the PM cannot show it did — see finding #4). That is a professional
     judgment failure, mitigated by the dispatch shape but not erased by it.
  2. *The PM's own class (d).* Two defect families (instance `.get()` fallbacks,
     silent-failure defaults) are convicted as worker faults in §3/§4. "Workers
     executed their dispatches competently" as a headline cannot coexist with a defect
     table assigning real defects to worker execution.
  3. *Understated severity of class (d).* "Second-order" (§5) is a claim about the
     migration outcome, not about operational fitness. A pipeline whose
     `classify_error` defaults unknown exceptions to TERMINAL is not
     production-competent code regardless of who is to blame for the migration.
- **Defensible verdict:** *the workers were scope-faithful; one brief was
  half-executed without escalation; two quality-failure classes are worker-attributable
  and non-trivial; the system-level failure is nonetheless orchestration's.* The PM's
  headline gets the last clause right and the first three wrong by overreach.

---

## 5. "Seams need owners" vs "producer/consumer checks" — decision

**They are not the same thing. They are the two halves of one control, split by the
time at which they act — but neither entails the other, and each failed independently
here.**

- **Axis separating them: accountability vs observability** (equivalently: plan-time
  ownership vs acceptance-time verification).
  - *Seams need owners* is an **assignment** property: some named dispatch/person is
    accountable for the boundary. Decided when dispatches are written.
  - *Producer/consumer checks* is a **verification** property: the handoff is exercised
    and observable (nonempty handoff demonstrated, not an existence asserted).
    Applied whenever acceptance runs.
- **Neither entails the other, and this project proves both directions:**
  - *Owned but unverified:* the plan's §4 blast-radius register *named* the coupling
    (drain as Phase 2's consumer) — the seam had an owner on paper — and the exit
    criteria were still existence-shaped, so the handoff was never exercised. Ownership
    without round-trip checks produced exactly the observed green-but-wrong state.
  - *Verified without formal owner (counterfactual):* any single reviewer adding one
    integration test across the drain/submit boundary would have caught defect 1
    without anyone "owning" the seam.
- **Decision for the learning set:** keep both learnings, but state the composition:
  **every contract gets (a) a named owner in the dispatch AND (b) a round-trip
  assertion in acceptance.** If the set must merge them into one learning, the correct
  merged form is that sentence — not "seams need owners" (insufficient: this project
  had paper ownership) and not "producer/consumer checks" (insufficient: checks without
  an owner decay into whichever slice feels like writing them).

---

## 6. Summary

The PM is structurally strong — its five-whys chain is evidence-backed and §8's
audit-folded addendum is honest. Its failures are rhetorical overreach, not analytical:
(a) "determined at dispatch design time" and "workers executed competently" claim more
than the PM's own defect table licenses; (b) the headline root cause is the proximate
mechanism while the true root — coupling knowledge lost in plan→dispatch translation —
sits fully evidenced in Why 2 and is never promoted; (c) §1's dispatch reconstruction
is inference wearing the clothes of evidence. The missed patterns that matter most are
the walking skeleton (its absence explains why nothing was ever live), consumer-driven
contract testing (its absence explains why every seam was fake-on-one-side), and
expand–contract sequencing (its inversion created the zombie queue table). The
seams-owner and producer/consumer learnings are distinct — accountability vs
observability — and both are necessary.
