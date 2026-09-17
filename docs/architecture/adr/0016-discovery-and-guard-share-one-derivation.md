# ADR-0016: Discovery and the in-flight guard share one derivation

Status: Accepted
Date: 2026-09-15
Amends: ADR-0014 (annotated, decision text unchanged)

## Context

ADR-0014 D1 made the Dagster instance authoritative for orchestration state: a
post's work is an `enrichment_submitted` partition, and the in-flight set is the
materialized-submitted set minus the materialized-harvested set. That decision is
unchanged and this record does not alter it.

What ADR-0014 left unspecified is *who runs discovery*. In the shipped code they
are two assets and two queries:

- `ig_posts_gen_batches` (the drain) runs the eligibility query against
  `ig_post_labels` and `silver_content_classification`, filters the result
  through `post_partition_state` for the in-flight guard, and materializes an
  `enrichment_submitted` partition per surviving post.
- `engine/submit.py` then reads back the materialized-but-not-harvested set,
  parses the keys, and submits.

So a post becomes work by being written to the instance by one asset and read
back out of it by another, in a separate run, with a separate code path for
each. The guard ("is this post already in flight?") and the discovery ("should
this post be enriched?") are two derivations of the same predicate, evaluated at
two different times against the same state.

The cost of that split is not theoretical. Because the two derivations are
separate code, they can disagree, and when they disagree the failure mode is
invisible:

- A post suppressed by the guard at drain time can be submitted anyway if the
  submit stage's `in_flight_partitions` read sees a different set — the two
  functions must be kept in lockstep by hand.
- The classification prompt literal `f"{IG_GOLD_PROMPT}\n{caption}"` and its
  workspace sibling in the facet path are currently duplicated. Two literals
  that must stay identical and are compared by nothing.
- The drain writes partitions and returns a DataFrame nobody consumes. Its
  `enqueued`/`suppressed`/`mode` columns are diagnostics that no downstream
  asset reads — a `@asset` used as a job, carrying a return type the graph
  ignores.

## Decision

**Discovery and the in-flight guard are one derivation, and it lives in the
submit stage.**

1. `engine/submit.py` becomes the sole discovery actor. One pass:
   candidates (per workload, from that workload's eligibility query) → **one**
   `in_flight_partitions` read → `MAX_ROUNDS` guard → materialize the
   `enrichment_submitted` placeholder → build items → **one** `adapter.submit`.
2. The `ig_posts_gen_batches` asset is deleted. There is no drain. `submit` is
   no longer reactive to a set someone else wrote; it *derives* the set and acts
   on it in the same run.
3. Per-workload eligibility and item construction move into a `Workload`
   registry (`ig_enriched/slv/workloads.py`). A `Workload` declares its name, its
   silver table, its `candidates(conn, cfg)` query, its `build_item(row)` →
   `Item`, and its `estimate(items)`. `submit` iterates the registry and knows
   nothing domain-specific.
4. `GoldConfig` (the drain's config, named for the retired `gold_analyses`
   table) is replaced by `SubmitConfig`, which is where enrichment admission is
   now configured — because submit is where admission is now decided.
5. An `enrichment_submit_sensor` requests a submit run when discovery finds
   pending work, so the retry/backfill cadence is preserved without an asset in
   the graph that exists only to be scheduled.

## Consequences

- **The guard and discovery cannot disagree** — they are one function reading
  one snapshot. This is the whole point: the failure mode "suppressed at drain
  time, submitted anyway" is no longer representable.
- **A submit run either does work or reports that there is none from the same
  read.** The operator-facing ambiguity ADR-0014 AC4 addressed (a bare "no run"
  for both "nothing to do" and "everything in flight") is resolved by the run's
  own metadata rather than by a warning log in a different asset.
- **The `MAX_ROUNDS` stuck-partition guard moves verbatim** out of
  `discover_pending` and into the pre-submit guard, where both the discovery
  result and the in-flight set are in scope at once.
- **Item construction is registry dispatch.** Adding a workload (see the
  growth-facets pass, which registers here) means adding a `Workload`, not
  editing submit. The engine never names a payload.
- **`ig_posts_gen_batches` disappears from the graph**, so anything selecting it
  breaks loudly at collection rather than silently selecting nothing. The
  `daily_medallion` schedule drops the asset.
- **One submit per run is preserved** (`DEFAULT_SUBMIT_LIMIT`), because the
  provider job is the dedup boundary and a bounded run is what keeps cost
  predictable. Discovery is now recomputed each run rather than remembered in a
  partition — the instance still holds the durable record, but it is read, not
  used as an inter-asset channel.

## Annotations

- **ADR-0014 D1** is unchanged: the instance remains authoritative, the in-flight
  set remains `submitted ∖ harvested`, and `post_partition_state` remains the
  round derivation. This ADR changes *who calls* that derivation, not what it is.
- **ADR-0012** is unchanged: orchestration state is Dagster-native. Removing the
  drain removes inter-asset state-passing, which strengthens rather than weakens
  that record.
