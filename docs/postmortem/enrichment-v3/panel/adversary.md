# Adversarial / Red-Team Review of the Post-mortem

Panel seat: ADVERSARY · 2026-09-13 · Read-only; this file is the deliverable.
Target: [`../postmortem.md`](../postmortem.md) (and its §8 audit addendum).
Stance: find where it is WRONG, not where it is incomplete. Where an attack fails, that is said too.

**One-line verdict up front:** the post-mortem's *direction* (orchestration-dominant) survives,
but three of its load-bearing moves do not: (1) "workers executed their briefs competently" is
partly false and partly unprovable from the evidence given; (2) "7 of 36" is used as a metric the
document itself has discredited; (3) its headline root cause is stated in a form that does not
discriminate failure from success. Details below.

---

## 1. Steelman the opposite conclusion: the workers (or the plan) are substantially at fault

The post-mortem's case for worker innocence rests on one epistemic claim: *each worker's slice
made the inter-component defects invisible.* Attacked seriously, it has four holes.

**(a) The workers wrote the vacuous instrument.** The post-mortem classifies vacuous exit-criterion
tests as class (b) — "verification instrument" — orchestrator fault. But a test does not write
itself. `test_two_consecutive_runs_enqueue_no_post_twice` (audit-p1-p2 §P2-2) was authored by a
worker, in the worker's own slice, with full visibility of the guard's semantics. The post-mortem's
own §6 answer to defect 8 — "what would this test have to look like to FAIL?" — is a question a
competent worker asks at authoring time, inside the slice, with no cross-component knowledge
required. Same for the tautological prompt-hash test (audit-p3-p4 §P3-1) and the SELECT-ability-only
compatibility suite. The post-mortem treats the instrument as if it descended from the dispatch;
it was *produced* by the workers. "The remits failed the system" quietly converts worker-authored
weak tests into orchestration fault.

**(b) The exculpation of the flagship defect rests on an inference the document itself flags.**
§4 marks the `enrichment_harvested` exculpation "[INFERENCE from dispatch shape]" and §8.4 then
asserts, without any quoted dispatch text, that "the unit briefed to 'converge both lifecycles'
was scoped to the facets/qwen path." The commit message it cites says **both** lifecycles. A fair
reading of `872904e` is the opposite of the post-mortem's: the Gemini lifecycle was in remit and
the worker converged half of it. The post-mortem acknowledges the ambiguity ("'both lifecycles'
was not unpacked") and then resolves it in the worker's favor on the strength of an inference.
Zero dispatch text is quoted anywhere in 333 lines. The single most important factual claim in the
verdict is unquoted and unverifiable from the document.

**(c) Eight of twelve bypass sites are in ONE file, `harvest.py` (audit-p1-p2 §P1-1).** The
post-mortem's invisibility argument ("nothing in the dispatch named them") is weakest exactly
where the bypasses are densest: a worker editing `harvest.py` — the file containing 8 of the 12
bypass sites — had the seam on disk, one import away. Even if adoption of *every* site was out of
remit, a worker who lands responses at `harvest.py:210` and leaves 8 direct provider calls in the
same file did not need cross-slice knowledge to notice the seam existed. The post-mortem never
addresses the density argument; it argues from the general shape of dispatches, not from the
actual file layout its own evidence table describes.

**(d) Plan fault is not orchestrator fault.** The post-mortem lumps class (e) — plan/spec defects
(retry orphaned, key shape unspecified, `max_tokens` seam gap) — into "orchestration," but these
are *authoring* defects in ADRs and the master plan. If the plan is at fault, the remediation is
different from "name every contract in the dispatch" (§8.5's primary fix): it is interface review
of ADRs before any dispatch. The post-mortem's §5 verdict collapses three distinct loci —
dispatch design, plan authorship, acceptance process — into one actor, which makes its headline
root cause unfalsifiable: whatever fails, "orchestration" absorbs it.

**Countervailing evidence the steelman must concede.** The live-DB facts are mechanical and
reproducible: 0 bronze rows, no silver tables, 0 callers of `conform`, `submit.py:77` reading
`batch_jobs`. No worker, editing only their slice, could cause those facts to be *detected* —
that part of the post-mortem holds. And the two genuine worker-fault items it does own
(`DagsterInstance.get()` fallbacks, silent-failure paths) are correctly classified.

**Verdict: the steelman PARTIALLY SURVIVES.** "Workers blameless" is overstated: worker-authored
vacuous tests (the instrument class) are worker fault, the `both lifecycles` reading is resolved
in the worker's favor without evidence, and plan-authorship fault is conflated with orchestration
fault. What survives is the narrower claim: *the disconnected-halves and missing-writer defects
were not detectable from any single slice.* The headline verdict as written — workers competent,
orchestration dominant, full stop — is broader than its evidence.

---

## 2. Is "7 of 36" a valid metric or rhetoric?

**Verdict: valid as a falsifier, invalid as a measure — and the document leans on it as a measure.**

- The post-mortem itself demonstrates the criteria are unreliable instruments: audit-p3-p4 shows
  P4 #7 ("a real violation fires") is *untestable as written* until real runs exist, P4 #6
  ("loudly") has no operational definition, P3 #1 is met near-vacuously and still counted in the
  "MET" column of the post-mortem's own §8.1 table. A criterion can be unmet and the system fine
  (the criterion was wrong) — the post-mortem proves at least three criteria are wrong.
- The boundary between MET / PARTIALLY MET is analyst discretion: audit-p1-p2 counts P1-2 "MET
  (by tests against a fake service; real-run unproven)" — by the standard applied to P4 #4, that
  is a test-world-only MET. Move three such judgments and "7" becomes 4 or 10. **The integer is
  false precision.**
- So what does 7/36 actually measure? Not migration health, and not even code-vs-criteria
  agreement (that would need the criteria to be sound). It measures *how many criteria survive an
  adversarial audit*, with an auditor whose strictness the post-mortem cites approvingly precisely
  because the number came out low.
- Where the number IS legitimate: as refutation of the orchestrator's "phases 3-6 complete." For
  that, one unmet criterion suffices; 7/36 is rhetorical amplification of a binary fact. Using it
  in the headline ("**7 of 36 exit criteria are genuinely met**") borrows quantitative authority
  the underlying instrument does not have.
- Sharpest form of the objection: **the post-mortem commits, against the plan's exit criteria,
  the same sin it diagnoses in the orchestrator — treating a green/red count as evidence of a
  property the instrument cannot assert.** The orchestrator trusted green suites; the post-mortem
  trusts an audit tally. Both are instrument readings, and §8.3's own sentence — "a green gate is
  evidence only of what it can assert" — applies to the audit's tally too.

---

## 3. Discriminating vs non-discriminating: would this cause also appear in a SUCCESSFUL migration?

A factor present in both failed and successful migrations explains nothing. This is the sharpest
test available, and the post-mortem never applies it.

| Post-mortem's claimed cause | Present in successful migrations too? | Verdict |
|---|---|---|
| Per-component dispatch decomposition | Yes — many migrations succeed with per-component dispatches, especially with a final integration dispatch or a real-run gate | **NON-DISCRIMINATING as stated.** Only the sharper variant discriminates (below) |
| Integration not owned by any dispatch | A successful migration by definition got integrated somehow — but "unowned and it still worked" describes luck, not process; when integration is unowned and succeeds, it succeeds through some compensating mechanism | **DISCRIMINATING** (this is the real root cause; note it is not what the headline says) |
| Existence-shaped exit criteria | Yes — sloppy criteria are endemic; they go unpunished when a real run or human reviewer compensates | **NON-DISCRIMINATING alone**; discriminating only in combination with "no compensating gate" |
| Vacuous green tests | Yes — vacuous tests exist in countless working pipelines; they are latent, not causal | **NON-DISCRIMINATING** |
| Multi-subagent parallel execution | Yes — the standard operating model of this repo; many workstreams succeeded this way | **NON-DISCRIMINATING** |
| No real run ever required at any acceptance gate | A successful migration's artifacts exist in the live DB — i.e., something ran. The absence of any real-run requirement is specific to this failure | **DISCRIMINATING** (borderline tautological: "it failed because nothing was ever executed" — but as a *process* gap it is a genuine, fixable discriminator) |
| Underspecified ADRs (retry driver, key shape, max_tokens) | Successes exist with underspecified specs, resolved by workers asking or by review | **WEAKLY DISCRIMINATING** — raises probability of drift, not sufficient |
| Worker quality defects (silent failures, `.get()` fallbacks) | Yes — these quality defects exist in working pipelines everywhere | **NON-DISCRIMINATING** for the migration outcome (correctly demoted by the post-mortem, though by the wrong reasoning) |
| Zombie code left in tree | Yes — dead code coexists with success routinely | **NON-DISCRIMINATING** |

**Finding:** the post-mortem's headline root cause — "per-component dispatch + per-component
acceptance" — is a **universal condition** of this orchestration style, present in every
migration this harness has ever run, including ones that shipped. What actually discriminates
this failure from successes is the compound: *no contract had an owner AND no acceptance gate
ever executed the real path.* The post-mortem reaches this compound in §8.5 but sells §5's
headline in the non-discriminating form. A post-mortem whose root cause does not survive the
success/failure test will prescribe remedies already in place wherever the process succeeded.

---

## 4. The evidence chain: is it circular, and is it stronger than what it refutes?

**Where the chain is genuinely strong (the attack fails here).** The audits' central facts are
mechanical, reproducible, and independent of any model's judgment: table catalogs queried
read-only (`silver_content_classification` absent), row counts (bronze = 0, gold_analyses = 9,576),
view SQL text (3 views read `gold_analyses`, 0 read silver), greps (no writer for
`enrichment_harvested`; zero importers of `conform`). These are stronger than the orchestrator's
"phases 3-6 complete" claims, which cite no artifact at all. Live-DB verification is not a
subagent opinion; it is a reproducible check. No circularity there.

**Where circularity is real:**

1. **Echo chamber on the qualitative verdicts.** The three `review-*.md` documents are inputs to
   all three audits (each says "builds on, does not duplicate"), and the post-mortem builds on the
   audits. The single most-quoted load-bearing observation — review-interfaces §7 "none asserts
   the two production halves connect" — is one subagent's reading, echoed through five documents
   until it has the *appearance* of triangulation. It happens to be correct (it is checkable: grep
   the test suite for any test joining drain and submit), but the post-mortem cites its five-fold
   repetition as corroboration, and repetition of one source is not corroboration.
2. **Self-exculpation risk.** The post-mortem, the audits, and the workers are all the same class
   of actor: subagents of the orchestrator. The verdict "the workers executed their briefs
   competently" is a verdict by subagents about subagents. It may be true, but the document never
   registers the conflict, and §1's entire table of "what each subagent was likely told" is
   reconstruction presented with table-shaped confidence.
3. **The dispatch record is claimed, never shown.** §8.4 says "from the orchestrator's own
   dispatch record" three times and quotes it zero times. The audits verified the database; no
   document verifies the *dispatches* — the facts that carry the entire worker-innocence verdict.
   The evidence chain is strong at its base (live DB) and empty at its apex (dispatch text).

**Net:** the audits are stronger than the claims they refute on system-state facts; they are
equally suspect (same provenance, no independent check) on everything attributed to intent,
remit, and dispatch — which is exactly the territory where the post-mortem's verdict lives.

---

## 5. The claim that would be most embarrassing if wrong

**Named: §8.4's "no unit was ever briefed to migrate the `submit.py`/`harvest.py` call sites" (and
its sibling, "no brief named the `enrichment_harvested` writer").**

Why it is the most load-bearing: it is the basis for exculpating the flagship defect (12 bypass
sites, "8 of 12 in harvest.py"), it is the direct support for the headline verdict ("workers
executed their briefs competently"), and it determines the remediation — if true, fix the
dispatches; if false, the problem is worker execution or acceptance, and §8.5's "primary fix"
(name every contract in the dispatch) treats a symptom.

**Test of it:** it fails on its own document. §4 marks the parallel claim "[INFERENCE from
dispatch shape]"; §8.4 upgrades the same claim to fact by invoking a dispatch record it never
quotes. The commit message for `872904e` — "converge both lifecycles onto one seam" — is the only
dispatch-adjacent evidence in the document, and it reads *against* the post-mortem: "both" is not
"the facets one." The post-mortem notices this ("'both lifecycles' was not unpacked") and then
resolves the ambiguity in favor of the worker with no further evidence.

**Verdict: NOT ESTABLISHED.** It may well be true — a dispatch record showing a facets-scoped
brief would settle it in one quote — but as written, the most consequential claim in the
post-mortem is an inference dressed as a record citation. If it fell, the entire §5 verdict
inverts for the highest-profile defect and the post-mortem would have recommended the wrong fix
with high confidence.

---

## 6. The requester's question: are "seams need owners" and "producer/consumer checks" the same learning?

**They are distinct, on different axes, and the migration proves both directions of the difference.**

- **"Seams need owners" is an accountability invariant** — a *who*, fixed at design/dispatch
  time: some named actor is responsible for each inter-component contract.
- **"Producer/consumer checks" is a verification invariant** — a *how you know*, fixed at
  acceptance time: a demonstrated round-trip (a materialization observed, a handoff asserted
  nonempty), not an existence claim.

They can fail independently:

- *Owner without check* → silent drift. The frontend-craft case in the post-mortem's own §8.6 is
  exactly this: the worker owned the page, and the verification was a claim of inspection from a
  party that could not see. Ownership was fine; the check was vacuous.
- *Check without owner* → a contract enforced by a test nobody is responsible for maintaining;
  it rots or, worse, the missing side of the contract (the writer, the consumer) has no one to
  assign work to when the test goes red. A CI round-trip test on this migration would have gone
  red — and red against whom? With no owner, the finding has no addressee and gets waived.

They compose, and the composition is the actual learning: **ownership decides who acts; the check
decides whether anyone ever has to.** This migration lacked both (no owner for the drain→submit
handoff, no round-trip gate anywhere), which is why it drifted so completely. Collapsing the two
into one learning would predict that adding either alone fixes the failure class — the §8.6 case
demonstrates it does not.

---

## 7. Summary of findings (priority order)

1. **BLOCKER (verdict integrity):** The most load-bearing claim — "no unit was ever briefed" for
   the seam migration and harvested writer — is an inference presented as a record citation;
   no dispatch text is quoted in 333 lines, and the one commit message quoted reads against it.
   Fix: quote the dispatches, or downgrade §8.4 to inference and soften §5 accordingly.
2. **HIGH (analysis validity):** The headline root cause ("per-component dispatch + per-component
   acceptance") is non-discriminating — it is present in every migration this process has run,
   including successful ones. The discriminating cause is the compound: unowned contracts AND no
   real-run gate. §5 should say so; §8.5 nearly does.
3. **HIGH (metric misuse):** "7 of 36" is used as a quantitative measure after the document itself
   established the criteria are unreliable instruments. Valid as a falsifier of "complete";
   invalid as a health metric; the integer is false precision (MET/PARTIAL boundaries are
   discretionary — see P1-2 vs P4 #4).
4. **MEDIUM (worker verdict overstated):** Worker-authored vacuous tests are classified as
   instrument/orchestrator fault; the instrument was produced inside worker slices. "Workers
   competent" survives only in the narrowed form "slice-invisible defects were not detectable
   from any single slice."
5. **MEDIUM (taxonomy conflation):** Plan-authorship defects (class e) are folded into
   "orchestration," making the root cause unfalsifiable and the remediation ambiguous.
6. **CONFIRMED (no attack found):** The live-DB evidence base is sound and stronger than the
   claims it refutes; the two admitted worker-fault classifications are correctly assigned; the
   owners/checks distinction in §6 is real and correctly drawn. Manufactured ends here.
