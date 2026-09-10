# ADR-0013: The seam keeps no ledger — provider job state is the service's, orchestration state is Dagster's

- Status: **Accepted**
- Decided: 2026-09-10

Part of the [architecture documentation](../README.md) · ADR index: [`README.md`](README.md)

## Context

ADR-0007 (Amendment 1) and ADR-0010 (decision 5) specified the seam as **one
shared `external_jobs` ledger that both sides read and write** — one
workload-keyed table, placeholder-before-POST, replacing `facets_batch_jobs`
and the Gemini `batch_jobs`/`batch_items` pair.

One day later, ADR-0012 moved orchestration state to the Dagster instance and
retired the `ops.sqlite` queue. **ADR-0012 never mentioned `external_jobs`**, so
it retired the queue without reconciling against the ledger the previous ADR had
just specified. That left the seam plan specifying a table that ADR-0012's
premise — and the spike's own evidence — said should not exist.

The tension surfaced as an unresolved question: *does `external_jobs` live in
`ops.sqlite` or the Dagster instance?* It is the wrong question. Pinning a
location would have reinstated, under a new name, exactly the queue table
ADR-0012 retires.

The spike had already answered it. S5 (`spike_defs/visibility.py`) proved that
backlog, in-flight, and permanently-failed are all recoverable from instance
state + the warehouse with no bespoke ledger table, and its negative assertion
tested for `external_jobs` **by name**:

```
[PASS] no ledger table in instance storage — []
[PASS] no ledger table in the warehouse either
```

with `get_materialized_partitions(submitted) - get_materialized_partitions(harvested)`
as the complete, restart-surviving in-flight set, and S4 proving idempotency
comes from natural keys rather than a status column.

## Decision

**The seam keeps no ledger table. There is no `external_jobs`.**

State ownership splits cleanly, and each side owns its own record:

| Side | Owns | Reached by |
|---|---|---|
| **Inference service** (separate container) | its job/item store — lease, retry, backoff, dead-letter, resume | Dagster polls it: `POST /jobs` (submit), `GET /jobs/{id}` (poll-to-terminal), `GET /jobs/{id}/results` (retrieve) |
| **Dagster** | which partitions are submitted vs harvested | instance state: `get_materialized_partitions(submitted) - get_materialized_partitions(harvested)` |
| **The lake** | what actually succeeded | `landed(bronze_enrichment_raw) ∖ conformed(silver)` |

ADR-0007's "both sides read and write" is satisfied **by the HTTP contract plus
each side's own store**, not by a shared table. The service's job store is the
service's internal state that we read; it is not our orchestration state, and it
does not belong in our warehouse.

**No location question remains, because there is no table to locate.**

## Alternatives considered

**A shared `ops.sqlite` table both sides reach.** Rejected — it is the queue
table ADR-0012 retires, reintroduced under a new name, and it re-creates the
coordination state the Dagster-native move exists to remove. The spike's
negative assertion falsifies its necessity.

**A Dagster-instance-native ledger table.** Rejected — the service is a separate
container and cannot reach the instance, so it does not satisfy "both sides read
and write" either. It would swap one contradiction for another.

**Amend ADR-0012 to carve the seam record out of "orchestration state."** 
Rejected as unnecessary. It was only needed to preserve a table the evidence
says shouldn't exist; with the split above there is nothing to carve out.

## Consequences

- **`external_jobs` leaves the plan.** Phase 1 does not create it, and its exit
  criterion "reconcile the 4 `facets_batch_jobs` rows into `external_jobs`"
  becomes: **the 4 rows are reconciled into the service's job store, or
  explicitly accounted for as in-flight work, before `facets_batch_jobs` drops.**
  A ledger row is in-flight work, not a cache entry — that reasoning survives;
  only the destination changes.
- **Placeholder-before-POST is a materialization, not a row.** The
  `enrichment_submitted` partition is materialized before the billed call, which
  is the inspectable record the amendment wanted.
- **An explicit `ok`/status column is required on `bronze_enrichment_raw`.** The
  spike's open recommendation: failure is currently *inferred* from a missing
  conformed row rather than *read*. The anti-join is correct only while
  `write_conformed` never conforms a failed item — an unremarked implementation
  detail that, if broken, yields a silently empty error queue behind a green
  pipeline. Make it read, and make the invariant explicit and tested.
- **The accounting identity is the guardrail:**
  `done + failed + in_flight + backlog == total_candidates` must hold on every
  read, so a metric that silently stopped detecting is detectable.

## Supersedes / Superseded by

- **Supersedes** the ledger clause of [ADR-0007](0007-batch-native-enrichment-deprecate-worker.md)
  Amendment 1 and of [ADR-0010](0010-enrichment-naming-and-provenance.md)
  decision 5. Their remaining content stands: the seam's verbs, per-workload
  executors, placeholder-before-POST *semantics*, one-submit-per-pass, and loud
  per-item failure.
- **Reconciles** [ADR-0012](0012-dagster-native-orchestration.md) with the seam
  by removing the one orchestration-shaped table it had left unspecified.
- **Evidence:** `~/repos/enrichment-spike` — S4 (ledger-free idempotency), S5
  (visibility without a ledger; the negative assertion naming `external_jobs`).
