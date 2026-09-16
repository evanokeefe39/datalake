# ADR-0018: Pipeline automation — auto-materialize the replay path, keep the submit edge a determinism boundary

- Status: Accepted
- Decided: 2026-09-16
- Supersedes: the manual-submit guidance in `tasks/plans/compose-services-and-roster-plan.md` S11 (submit STOPPED as the *only* spend guard)
- Relates to: [ADR-0011](0011-enrichment-layered-model.md) (replay purity — the reason the submit
  edge is a boundary at all), [ADR-0012](0012-dagster-native-orchestration.md) (the provider's
  credit limit), [ADR-0013](0013-seam-keeps-no-ledger.md) (the seam owns its own state)

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

**4. Auto-materialization is declared per asset, with the submit edge as a DETERMINISM boundary.**

- Silver conformance and the gold marts are pure functions of already-landed bytes. They
  auto-materialize on new upstream data.
- **`submit` is never reachable by transitive auto-materialization from silver or gold.**
  This is not a spend preference and it does not become removable when cost is handled —
  it is the replay-purity contract of ADR-0011. `silver_*` must remain a *deterministic
  function of `bronze_enrichment_raw`*. If a `materialize` on silver could transitively
  reach the provider, then silver becomes a function of **live provider state and sampling**
  rather than a pure replay of landed bytes: re-publishing silver would produce different
  rows on different runs, "a schema change is a replay, not a re-bill" would stop being
  true, and the parity/purity evidence from the v3 migration would be meaningless. The wall
  exists to keep that function total and deterministic.
- **The API credit limit is defense-in-depth, not the justification.** It bounds the *submit*
  edge (OpenRouter returns 403 "key limit exceeded", recorded in ADR-0012) and protects
  against a runaway fan-out. It does not protect the free replay path, and its presence must
  never be read as "the wall is now unnecessary".
- Therefore the enrichment boundary is expressed explicitly in the graph (a sensor and/or
  the harvest partition machinery already in place), so that where the system reaches
  outside itself is a *declared, legible* property — and so that no reader infers the wall
  from a cost argument that can later evaporate.
- A second, weaker reason to keep it legible: blast radius. An unbounded fan-out that
  reaches the provider spends quickly. That is a consequence of the boundary, not its
  purpose.

**5. Any auto-materialization policy is a tested contract.**

Whatever policy an asset carries is asserted by a test, in the same spirit as the
asset-graph-integrity guard (S3) that made dead keys impossible to reintroduce. A policy
that exists only as a decorator argument is a policy that silently disappears in the next
refactor — which is precisely how this gap arose.

## Consequences

**Positive.**

- The system behaves the way a Dagster user expects: ask for an outcome, get the upstream
  work — for everything upstream of the submit boundary. The "materialize gold and nothing
  happens" surprise is gone.
- **Silver stays a pure replay of bronze.** Because submit is unreachable transitively, a
  schema or mapping change remains a replay rather than a re-bill (ADR-0011), and the
  parity and replay-purity evidence from the v3 migration keeps its meaning.
- The submit boundary is stated where a reader looks for it (decision 4 and its assertion), so
  "can a `materialize` spend money?" has a single, checkable answer rather than one that
  depends on which paragraph was read first.
- The graph states where external work occurs, so the boundary is reviewable rather than
  emergent.

**Negative / accepted.**

- **A `materialize` on gold or silver does NOT reach the provider** — by design, and this is
  the whole point of decision 4. The accepted cost is that submitting new work is a
  *separate, explicit act*: the system does not discover-and-submit as a side effect of a
  read. Reaching the provider therefore requires choosing to (a job invocation, a sensor,
  or a schedule the owner enables).
- An unbounded fan-out (e.g. a first details sweep across every profile) can spend quickly.
  Bounded by the declared cap on fan-out ops and by the schedule policy above; the
  credential limit is the backstop.
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
5. **The determinism wall is enforced, not merely documented.** A graph assertion proves no
   auto-materialization path from `silver_enrichment` (or any mart) reaches `submit` — and
   the guard FAILS when such a path is introduced, proven by injecting one. If this
   assertion is absent, decision 4 rests on prose alone and the next refactor can quietly
   make silver provider-dependent.
6. **Replay purity still holds end to end**: materializing `silver_enrichment` twice against
   an unchanged bronze produces identical rows and issues zero provider calls (the property
   ADR-0011's "replay, not a re-bill" claim depends on).
