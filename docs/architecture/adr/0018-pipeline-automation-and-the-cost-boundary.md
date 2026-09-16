# ADR-0018: Pipeline automation — auto-materialize the graph, externalize the cost boundary

- Status: Accepted
- Decided: 2026-09-16
- Supersedes: the manual-submit guidance in `tasks/plans/compose-services-and-roster-plan.md` S11 (submit STOPPED as the *only* spend guard)

## Context

After the Compose work landed (ADR-0017), the platform runs: orchestration, jobs and
dashboard come up together, the harvest sensor ticks, and a real enrichment cycle was
executed end to end. That run exposed a gap that is architectural rather than a defect.

**Observed.** A submit of two media-bearing items, followed by the harvest sensor's
automatic tick, landed 4 rows into `bronze_enrichment_raw` — all `provider='service_backed'`
— while `silver_visual_annotations` stayed at 0 rows. Nothing carried bronze into silver.
Materializing silver by hand worked. So the live chain was:

```
submit (manual)  ->  harvest (sensor)  ->  land bronze  ->  STOP
```

The assets have correct lineage: `silver_enrichment` reads bronze, the marts read silver.
Nothing *acts* on that lineage. The lineage survived the ADR-0012 migration; the triggers
did not, because the queue tables that used to drive each stage were retired without
re-deriving the triggers from the graph.

**Why this was not simply fixed at the time.** The obvious answer — "materialize the marts
and let Dagster pull upstream" — collides with a real consequence: upstream of bronze is a
paid external provider. With full auto-materialization, a read (or a backfill, or a retry)
can transitively cause a provider call. A system where **a read spends money** is one where
an accidental CLI invocation, a re-run, or a sensor misfire each re-bill. That is why the
plan shipped submit manual and left the harvest sensor running.

**The judgement this ADR settles.** The owner has decided that cost is protected
*externally*: the OpenRouter API key carries its own credit limit, the owner sets the spend
ceiling and rotates keys on the period they are comfortable with. Cost management is
therefore a property of the **credential**, not of the orchestration graph. Dagster must not
encode spend policy — no manual-submit gate *whose purpose is cost control*, no budget
arithmetic, no per-run cost ceiling as a correctness property.

That decision removes the tension. The paid edge is no longer a special case requiring an
operator, and the pipeline can be expressed the way Dagster intends: declarative lineage
with jobs that run the graph.

## Decision

**1. The pipeline is job-driven, not manually staged. A job that targets a downstream asset
runs its upstream work.**

An operator asks for an outcome — "publish silver", "refresh the marts" — and Dagster
computes the rest from the declared lineage. There is no required sequence of hand-run
stages, and no stage whose normal operation is "someone remembers to trigger it". The
default posture is: **materializing a downstream asset runs everything it depends on.**

**2. Cost is enforced at the credential, never in the graph.**

The spend boundary lives on the API key (provider-side credit limit, rotated by the owner).
Consequently:

- No orchestration logic exists *for the purpose of* limiting spend. `dry_run`, `limit` and
  the workload selector remain as **operational controls** — ways to scope a run for
  debugging or a narrow backfill — not as the safety mechanism. Removing them would be a
  mistake; treating them as the guarantee would be one too.
- A run that would call the provider is allowed to do so. It is not gated behind a human
  approval step, and no asset check fails because a run *could* cost money.
- When the key's credit limit is reached, the provider returns an error; that is handled as
  a **loud, retryable provider failure at the seam** (the existing `ProviderError` /
  429-class handling), not silently absorbed and not pre-empted by graph logic.

**3. What remains deliberately manual is scoped and named.**

Two things stay human-triggered, and for reasons that are *not* cost:

- **Schedules ship STOPPED by default** (`DefaultScheduleStatus.STOPPED`). This is the
  existing repo policy: a schedule appearing for the first time does not start spending
  effort on its own; the owner enables it deliberately. It is a *policy about surprise*,
  not a cost guard.
- **`op`s that mutate outside the lake** (e.g. a details sweep that pays Apify per profile)
  keep an explicit cap (`DEFAULT_MAX_PROFILES_PER_SWEEP`) so a first tick cannot fan out
  into an unbounded paid sweep. The cap is a **blast-radius bound on a fan-out**, chosen
  once and reviewable — not per-run cost accounting.

**4. Auto-materialization is declared per asset, with the free/paid edge made explicit.**

- Silver conformance and the gold marts are pure functions of already-landed bytes. They
  auto-materialize on new upstream data.
- The enrichment boundary (`submit` → provider → `harvest` → bronze) is where bytes enter
  from outside. Its automation is expressed explicitly (a sensor and/or the harvest
  partition machinery already in place), so the graph states — legibly — where the system
  reaches outside itself.
- Because cost is external, the *reason* to keep this edge explicit is **legibility and
  blast radius**, not spend. A reader must be able to see where external work happens
  without inferring it.

**5. Any auto-materialization policy is a tested contract.**

Whatever policy an asset carries is asserted by a test, in the same spirit as the
asset-graph-integrity guard (S3) that made dead keys impossible to reintroduce. A policy
that exists only as a decorator argument is a policy that silently disappears in the next
refactor — which is precisely how this gap arose.

## Consequences

**Positive.**

- The system behaves the way a Dagster user expects: ask for an outcome, get the upstream
  work. The "materialize gold and nothing happens" surprise is gone.
- Cost control has one owner (the credential) and one failure mode (a provider error),
  instead of being spread across graph shape, operator discipline, and hope.
- The graph states where external work occurs, so the boundary is reviewable rather than
  emergent.

**Negative / accepted.**

- A `materialize` on a downstream asset can now cause provider calls. This is intentional
  under the external-cost decision, and it is exactly the behaviour a naive reader would
  expect anyway. The mitigation is legibility (which assets cross the boundary), not
  gating.
- An unbounded fan-out (e.g. a first details sweep across every profile) can spend quickly
  if the credential's limit is generous. Bounded by the declared cap on fan-out ops and by
  the schedule policy above.
- Key rotation is now an operational dependency of the pipeline. If the key lapses, runs
  fail loudly at the seam. This is accepted: a loud failure at a known boundary is
  preferable to a silent spend guard of unclear authority.

**Not decided here.**

- **Whether `silver_enrichment` gains partitions or an incremental policy.** It currently
  re-derives from the whole bronze root, so auto-materializing it means a full conform per
  landing. That is a performance design question with its own tradeoffs, deliberately left
  open. The correctness contract does not depend on it; only the cost of recomputation does.
- **Whether the submit edge is driven by a schedule, a sensor on a discovery asset, or only
  by job invocation.** All three are consistent with this ADR. The scheduled variants ship
  STOPPED per decision 3.

## Verification

This decision is implemented when, and only when, all of the following are observed against
the running system:

1. Materializing a gold mart from an empty silver runs silver (and any upstream work it
   needs) with no manual intermediate step.
2. A new landed bronze row reaches `silver_*` without an operator materializing silver.
3. The asset-graph-integrity guard asserts each asset's automation policy, and FAILS when a
   policy is removed — proven by injecting the removal.
4. A provider-side failure (credit limit / 429) surfaces as a loud seam error and is
   retryable, never as a quiet no-op.
