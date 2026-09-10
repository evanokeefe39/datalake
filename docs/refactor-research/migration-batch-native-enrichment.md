# Migration plan: batch-native enrichment (deprecate the external worker)

- Status: Proposed plan — output of the `chore/refactor-investigation` branch.
  Ratify by merging to `main`; then open a dedicated migration/refactor branch
  and execute the phases below.
- Date: 2026-09-06
- ADRs: [ADR-0007](../architecture/adr/0007-batch-native-enrichment-deprecate-worker.md)
  (batch-native enrichment; supersedes 0002),
  [ADR-0008](../architecture/adr/0008-hermetic-with-explicit-api-seam.md) (explicit API seam;
  supersedes 0003).

## Target state (the destination)

`gold_analyses` has exactly one **orchestrated** writer — an async `gemini-batch`
path that lives in Dagster:

```
pure assets (labels)  →  upload media (bounded op)  →  submit gemini-batch (short run,
                                                       persist job_id, ends)
   →  harvest sensor (cursor) polls terminal  →  harvest run (short) fetches + applies → gold_analyses
```

- **Primary:** orchestrated async `gemini-batch` (submit + harvest) — the hours-long
  work is external; runs never block.
- **Secondary, out-of-band:** interactive synchronous enrichment is a manual script
  (a developer tool, not orchestrated) that idempotently upserts `gold_analyses` for
  a chosen subset. Low priority by design.
- **Removed:** `scripts/enrichment_worker.py` and its coordination roles.

## Guiding principles

- **Strangler / expand-contract.** Land each phase green and reversible; nothing
  is deleted until the replacement is proven. The worker can stay behind a flag
  through Phase 5.
- **Keep gold correct.** Interactive and batch must write `gold_analyses` to the
  same contract (idempotent upsert on `(post_id, domain, prompt_hash)`); batch must
  reproduce interactive results on a sample before becoming the default.
- **DuckDB is single-writer.** The harvest run (orchestrated) and the ad-hoc script
  must never write `state.duckdb` concurrently. Ad-hoc = deliberate, not overlapping.
- **Seam guard (ADR-0008).** Silver onward stays hermetic; API calls live only in
  the tagged enrichment seam. Guard with naming/tags + an asset check + a test.

## Prerequisite gap to close

`gemini-batch` is currently deferred and text-only; **interactive** is the proven,
wired path. The migration's first obligation is making batch real (smoke-tested)
before it can be "primary."

## Phased plan

### Phase 0 — Ratify & baseline
- Merge this investigation branch to `main` (records ADR-0007/0008 + this plan).
- Snapshot current behavior: gold row/prompt counts, worker run shape, dead-letter
  state, the two 429 modes in the wild. This is the comparison baseline.
- Exit: baseline captured; branch clean.

### Phase 1 — Prove `gemini-batch` end-to-end + the harvest sensor (#24 cure)
- **External Integration Gate:** submit ONE real `gemini-batch` job and harvest it;
  confirm the applied gold matches interactive output for the same posts.
- Build the **harvest sensor** (cursor; reads pending `job_id`s, polls terminal) and
  a short **harvest run** (fetch + apply → gold). This closes #24 regardless of
  everything else — no more manual re-run.
- Verification: a submitted batch that finishes is harvested without a human; gold
  advances; the sensor survives a restart (job id persisted).
- Exit: batch harvest is proven and sensor-driven. Interactive worker still present
  and functional (unchanged) — dual-writer is acceptable during migration because
  both share the idempotent upsert contract.

### Phase 2 — Make batch the production default
- Wire **media File-API upload** as a bounded Dagster op (ahead of submit), since
  batch requires pre-uploaded files.
- Move volume enrichment onto batch; interactive shrinks to the residual/long-tail.
- Verification: real volume flows through submit + harvest; cost/token estimates
  printed at submit; per-item/429 handling where applicable.
- Exit: batch is the normal path; interactive is no longer the volume carrier.

### Phase 3 — Bound the interactive path (interim)
- While batch matures, ensure no interactive workload holds a Dagster run for hours:
  run interactive in bounded chunks (≤K items per run) with a sensor continuing
  while the queue is non-empty — or route it through the Phase-4 script.
- Verification: no run blocks > a bounded window; crashed runs reclaimable.
- Exit: interactive is bounded/deprioritized, not wedging runs.

### Phase 4 — Add the out-of-band interactive script
- A standalone manual tool that enriches a chosen subset synchronously and upserts
  `gold_analyses`. Not orchestrated, not scheduled. Records its writes (or reports a
  materialization) so gold freshness stays honest.
- Move residual interactive usage here.
- Exit: all interactive enrichment is manual/out-of-band; nothing interactive is
  orchestrated.

### Phase 5 — Remove the external worker
- Remove `enrichment_worker.py`, its REST materialization, and the
  `batch_items`/claim coordination Dagster now owns.
- Add the ADR-0008 seam guard (tag + asset check + test: no `generate_content`
  inside a pure asset). Enforce single-writer.
- Exit: gold's only orchestrated writer is the harvest run; no worker process;
  worker removed behind a proven replacement.

### Phase 6 — Docs, tests, as-built
- Update `OPERATING.md`, `ARCHITECTURE.md`, enrichment tests, dead-letter/reaper
  semantics, freshness + asset checks. Reconcile ADR-0007/0008 with as-built.
- Exit: full suite green; OPERATING reflects the new run path; merge gate met.

## Rollback
- Worker retained (flag/env) until Phase 5 exit is verified; each phase is
  independently reversible. Gold upserts are idempotent, so a partial phase can be
  re-run without corrupting state.

## Risks
| Risk | Mitigation |
|---|---|
| Batch (deferred) turns out not to cover a workload interactive handled | Phase 1 smoke before any defaulting; keep bounded interactive through Phase 3 |
| Media File-API upload ordering/latency | Upload as its own bounded op + cache (media_cache) ahead of submit |
| Two writers during migration corrupt gold | Shared idempotent upsert contract; single-writer rule; harvest/adhoc never concurrent |
| API call leaks into a pure transform after the worker goes | ADR-0008 seam guard (tag + asset check + test) |
| Runs re-wedge on interactive during transition | Phase 3 bounded chunks + continuing sensor |

## Definition of done (migration-branch merge gate)
- Batch is the sole orchestrated gold writer; worker removed.
- A batch that finishes is always harvested by the sensor (no #24 class).
- Interactive enrichment is out-of-band (manual script); nothing orchestrated blocks for hours.
- ADR-0008 seam guarded by a failing test; pure transforms hermetic.
- OPERATING/ARCHITECTURE/tests updated; full suite green.
