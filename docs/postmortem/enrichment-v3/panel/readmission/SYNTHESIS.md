# SYNTHESIS — Panel round 2: bin Gemini, re-admit through the seam

Convened 2026-09-14. Four seats, read-only, against one artifact
([`seam-readmission-brief.md`](seam-readmission-brief.md)).
Seat reports: [`interface.md`](interface.md) · [`inference.md`](inference.md) ·
[`orchestration.md`](orchestration.md) · [`adversary.md`](adversary.md).

---

## Verdict

**The proposal splits cleanly in two, and the two halves have different verdicts.**

**The deletion half survives.** Binning Gemini usage is correct, and four independent seats
concur. It removes the 9-site migration into a queue W3 deletes anyway (adversary: *"work
genuinely avoided, not relocated"*), and `DirectBatchAdapter` remains registered, tested code —
*categorically different* from the round-1 zombie-queue failure. No seat found a reason not to
do it.

**The re-admission half does not survive as written.** Every seat that examined it returned the
same shape of answer: the seam is **half-proven**. Its *state vocabulary* and *adapter registry*
are genuinely provider-neutral (verified: `Item/Result/Capabilities`, `seam.py:49-90`;
`test_adapters.py:194-310`). Its *lifecycle* was designed around qwen's job horizon and admits
Gemini only on paper. Re-admission is therefore a real option, but not by the mechanism the
proposal appeals to — not without fixing the lifecycle first.

**The one-line version:** binning Gemini is sound; "bring it back through the seam" is a
promissory note until four named interface gaps are closed and someone writes down what would
trigger the return.

## 1. The highest-risk finding, and it is not about Gemini

The orchestration seat established a failure mode the proposal does not mention and the plan
does not guard:

> With **no live submitter**, one drain run materializes the **entire approved corpus** as
> `enrichment_submitted`. In-flight (`submitted − harvested`) can then never shrink. The guard
> suppresses every future drain run — with a **warning log only**
> (`assets.py:1256-1261`, trace: `1083-1092,1289` → `partitions.py:230-236` → `1228-1244`).

So the stall the post-mortem identified as the migration's signature failure is *also* the
signature failure of this proposal, reachable through it. Mitigating facts: it is not fully
silent (`check_enrichment_health` can fire on `approved_unenriched > 20`, though it prescribes
the wrong remedy and nothing ticks it), it is recoverable once W3/W4 land, and the schedule that
would trigger it (`daily_medallion` → the drain) ships stopped. It is **one UI toggle away.**

**Consequence:** W-FREEZE cannot land without either W4's harvested producer or an explicit
submitter-quiet guard. Freezing the writer without wiring the reader creates the stall by
construction.

## 2. Interface gaps that must close before re-admission is real

| # | Gap | Evidence | Why it blocks re-admission |
|---|---|---|---|
| F1 | **Handle lifetime has no home between runs** | `run_lifecycle` (`seam.py:145-178`) is blocking and in-process with an in-memory handle; the legacy path persisted the handle in `ops.sqlite` (`submit.py:170`, `batch.py:235-269`) — which **ADR-0013 forbids by name** | Gemini jobs run **hours**; qwen's are fast. The contract has no declared place for a handle to live across a run boundary. This is the load-bearing gap |
| F2 | **Two distinct `max_tokens` meanings** | qwen per-mode output cap (`facets_batch.py:310-334`, the explicit bypass comment) vs Gemini chunk cap smuggled via constructor (`adapters.py:253-257`) | The planned `JobSpec` fix must not collide them onto one field, or it fixes one provider by breaking the other |
| F3 | **Handle encoding split** `"|"` vs `","` | `submit.py:170` vs `adapters.py:287` | `<load-bearing below>` — permitted only because `submit→str` declares no format *and* the seam has never had a production caller |
| F4 | **HTTP taxonomy in the neutral layer** | `_TERMINAL_STATUS` (`seam.py:38`), `classify_error` on `_HttpAdapter`; `DirectBatchAdapter` sets `base_url=''` "to satisfy the Protocol", so **every Gemini SDK error falls to TERMINAL** (`adapters.py:106`) | A silent-failure path in the provider-neutral layer. Any Gemini failure becomes indistinguishable from a terminal success-path |
| F6 | **Composite aggregation loses granularity** | any chunk `FAILED` maps the whole to `FAILED` (`adapters.py:310-311`) | The legacy path had per-chunk harvest granularity; the seam's shape throws it away |

F3 deserves its own note: the encoding divergence was *permitted by the interface*. `submit`
returns an opaque `str` with no declared format, so two implementations invented two formats and
nothing failed — because there was no consumer to fail. This is what "half-proven" means
concretely, and it is the strongest argument that re-admission must be gated on a **real
round-trip**, not on an adapter constructing.

## 3. Where the seats agree

- **The interface is not sufficient as-is.** Interface and adversary independently reached this;
  interface named the mechanism (lifecycle vs verbs), adversary named the presumption
  (*"'bring it back according to the interface we defined' presumes the interface is adequate"*).
- **`DirectBatchAdapter` is partial, not real.** Structurally genuine (delegates to
  production-proven `gemini_batch` verbs) but **never run over the wire** — every test stubs
  `_gemini` with `object()` (`test_adapters.py:188-197`). Classified by interface seat as
  PARTIAL; the deletion half is honest partly because the code survives as tested code.
- **No provider dimension belongs in the partition key.** Orchestration is explicit: provider is
  **run config, not a partition axis** (`seam.py:187-190` argues this in-repo). The key must not
  gain one.
- **Terminal-state divergence is a contract decision**, not an implementation detail (inference).
- **Binning fixes none of the seam defects.** Inference seat, plainly: the four gaps survive the
  bin and must be closed for the return to be a re-admission.

## 4. Factual corrections the panel forced (all verified against the live repo)

| Claim | Status | Verified how |
|---|---|---|
| Adapter registry key is `"gemini"` | **WRONG** — names are `service_backed` and `direct_batch` | `seam.py:113,119` — the only `register_adapter` call |
| Partition key is the literal composite grammar | **WRONG for current code** — it is `sha256(workload \x00 attempt_round \x00 sorted([post_id]))[:16]` | `partitions.py:118,156`. ADR-0014 D1 specifies the composite as the **target**; W4 implements it |
| `media_cache` is 2,768 rows | **WRONG — off by 10×**; it is 27,748 | live `ops.sqlite` |
| `facets_batch.py:318,322` is the qwen bypass | **STALE** — it is `:292` via `build_adapter` | grep |
| `adapters.py:280` for the encoding split | **STALE** — it is `:287` | grep |
| File API URIs usable for re-admission | **FALSE** — 5,419 of 5,613 expired, max `expires_at` 2026-09-11T00:37Z | live `ops.sqlite` |

The last row is the one with teeth: **re-admitting Gemini requires a full media re-seed**, not a
credential check. Media bytes survive in `media_cache` (27,748 rows, keyed by URL hash), so the
re-seed is local rather than a re-scrape — but it is work the proposal did not account for.

## 5. Conditions for the proposal to be honest

The adversary's verdict is the operative one: the deletion half survives attack; the
re-admission half is **a promissory note with no scheduled trigger, owner, or unit**. It becomes
honest if and only if:

1. **A trigger is named, with an owner.** "Temporarily" with no observable condition is
   indistinguishable from permanent retirement wearing a re-admission story. Adversary's
   suggested concrete trigger: a **Tier-1 grant**, observable via `health()`
   (`adapters.py:349-352`) — pick a real one and write it down.
2. **The four gaps (F1/F2/F3/F4, plus F6) are closed in W5**, not deferred. Only one (`JobSpec`)
   is currently scheduled.
3. **Re-admission is gated on a live one-chunk round trip over the wire** — not on
   `build_adapter("direct_batch")` constructing.
4. **The preservation set survives the bin** (interface seat, load-bearing): `gemini_batch.py` in
   full; `DirectBatchAdapter` + `_GEMINI_STATES` + the `direct_batch` registration + its tests;
   ADR-0009's 20 GiB File-API cap record; `GeminiTierConfig` tier gate; the
   `Item.images → media_files` media contract + byte cache; the seam Protocol + dataclasses +
   `test_seam`.
5. **The non-recoverable rot is mitigated.** Adversary splits rot into recoverable (SDK drift →
   quarterly lock + rerun, minutes; credential/tier expiry; wiring recipe) and **not recoverable
   — knowledge rot**: fixture and test knowledge decays silently, and the interface drifts toward
   a single-consumer shape with no red test in CI. The cheap mitigation is the
   `real_envelope_gemini.json` fixture W1 already plans (plan:194) — keep it green so drift has
   something to break.

## 6. What changes in the plan

- **W-FREEZE must not land alone.** It needs W4's harvested producer (or an explicit
  submitter-quiet guard) or it creates the stall by construction (§1).
- **W-FREEZE Consequence 3** corrected: the real registry name, and the honest strength of the
  "constructs" guarantee (§4).
- **Add a re-admission trigger + owner** to W-FREEZE, or the unit should be renamed to
  acknowledge a permanent retirement. The honest framing matters more than which answer is
  chosen.
- **W5 absorbs the four interface gaps** as first-class work, not as incidental fixes.
- **W1's media assumption re-checked**: the spike cannot assume any surviving File API upload.
