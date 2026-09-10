# ADR-0012: Orchestration state is Dagster-native — retire the ops.sqlite queue and dead_letter

- Status: Accepted
- Decided: 2026-09-10 (ratified by the owner 2026-09-10)
- Related: ADR-0007 (enrichment is Dagster-native async batch), ADR-0008
  (hermetic transforms with one explicit API seam), ADR-0009 (qwen batch
  service), ADR-0011 (enrichment layered model: bronze landing → silver
  conform → gold marts), ADR-0004 (ops/analytical split — **superseded in its
  queue + dead_letter scope only**), ADR-0013 (the seam keeps no ledger —
  reconciles this decision with the seam)
- Evidence: `~/repos/enrichment-spike/FINDINGS.md` (spikes S1–S6 + a live
  External Integration Gate)

## Context

ADR-0007 moved enrichment into Dagster (submit asset → harvest sensor), but the
`ops.sqlite` **queue survived the move**: `batch_jobs` / `batch_items` still
carry `status`, `attempts`, and `scheduled_for` backoff, and `dead_letter` still
stores terminal failures. That is a parallel ledger of things Dagster's instance
already knows, and it is the same data kept in two places — which is how the two
drift.

Spikes S1–S6 tested, against a real Dagster 1.13 instance and a demo warehouse
carrying the production schema, whether the instance plus the lake can carry that
state instead. Each claim was driven to a PASS/FAIL with raw evidence, and every
harness asserts a **negative** as well: that no bespoke ledger table exists.

The queue half of ADR-0004 was about *where coordination state lives* (SQLite,
not the analytical store). That question was answered correctly and is not in
dispute here. What is now answered — and what this ADR changes — is *whether we
need a bespoke queue at all*, given the orchestrator ships one.

## Decision

**Orchestration state lives in the Dagster instance and the lake. The ops.sqlite
queue and dead_letter are retired; the rest of ops.sqlite is untouched.**

1. **In-flight work is derived, not stored.**
   `get_materialized_partitions(submitted) − get_materialized_partitions(harvested)`
   is the complete in-flight set. It is re-derived from the instance on every
   sensor tick and survives a restart with no in-memory state (S1, S5).
2. **The sensor is an interval `@sensor`, never an `@asset_sensor`.** An asset
   sensor fires only on a NEW materialization event, so work that was not
   terminal at that instant would never be re-checked and would hang forever.
   S1 proves two consecutive ticks found in-flight work, requested no run, and
   *reported* it.
3. **One batched harvest run per tick.** N terminal partitions produce exactly
   ONE run request covering all of them (S1). DuckDB is single-writer; batching
   removes writer contention by construction rather than policing it.
4. **Completion is a conformed row, not a status column.** A post is done when
   its conformed silver row exists for the current contract. No `status` column
   anywhere (S5, S4).
5. **Retry is a NEW partition key.** Because in-flight is a set difference,
   re-materializing an already-harvested partition is invisible and would orphan
   provider work (S3). Retry round N targets posts that have failed **exactly N**
   times — idempotent per round and naturally bounded.
6. **The retry budget needs no column.** `attempts` is the row count in the
   append-only landing table, because every attempt lands its own row keyed by
   run/handle (S3). "Give up" is the query `attempts >= MAX_ATTEMPTS`: terminal
   state is derived, so it cannot drift from reality.
7. **Failures surface through a BLOCKING asset check.** The failure set is the
   anti-join `landed(bronze) ∖ conformed(silver)`, and a Dagster asset check
   fails loudly with counts and the stuck post ids (S3). A failed item never
   fails its job, so the check is the *only* thing making it visible.
8. **An op that writes DuckDB runs in-process.** DuckDB is single-writer, so under
   a subprocess executor the op contends with any other open handle on the file
   and fails outright (`Cannot open file … being used by another process`,
   observed). Pin `in_process_executor` wherever such a handle may be open. This
   is a **narrow, grounded** rule about the storage engine — it is NOT "the
   multiprocess executor is unsafe". An earlier reading of spike S1 blamed the
   executor for destroying instance run state; that attribution was **WRONG and
   is retracted** — the loss came from the spike harness wiping its own scratch
   `DAGSTER_HOME` from module scope, which `multiprocessing`'s spawn path
   re-executes in every child because under `python -m` the entry module *is*
   `__main__`. Windows compute-log tail overhead (~50 s/step) is a secondary,
   performance-only reason for the pin.
9. **Providers sit behind one `ProviderAdapter` Protocol; handles are opaque
   strings.** Provider choice is config; provider-native state vocabularies and
   result field names are normalized at the seam; a fan-out provider returns ONE
   opaque handle the orchestrator cannot see into, so multi-handle work needs no
   schema change (S2, S6).
10. **Prompt identity binds the prompt plus its output schema — never the
    model.** The model is provenance, recorded beside the result. Binding it into
    the staleness key means a provider swap re-enriches the whole corpus for no
    semantic change (S2).
11. **A direct provider is never proxied through the inference service.**
    Verified by endpoint tracing, not by reading source (S6).

**`ops.sqlite` retains** the state that is genuinely not orchestration:
`media_metadata`, `media_cache`, `creators`, `profiles`, `creator_merges`,
`prompt_registry` (the prompt-hash provenance store).

## Alternatives considered

- **Keep the queue alongside Dagster** (status quo): rejected — two ledgers for
  one truth. The drift is not hypothetical: the spike found `facets_batch_jobs`
  live in `ops.sqlite` but absent from the schema catalog.
- **Keep `dead_letter` and only drop the queue**: rejected — the failure set is
  already an anti-join over tables that must exist anyway, so `dead_letter` is a
  cache of a derivable query, and one that can silently empty itself (see the
  invariant risk below).
- **Status columns on the analytical tables**: already rejected in ADR-0004 and
  still rejected; this ADR removes the need for them rather than relocating them.
- **Re-materialize a partition to retry it**: rejected — invisible to the
  in-flight derivation; the provider job is orphaned and never harvested.
- **Key the conformed upsert on `run_id`**: rejected — idempotent per-handle
  only; a reprocess under the same contract appends a second row and silently
  doubles every downstream count.
- **Bind `prompt_hash` to the model** (current behaviour): rejected — a provider
  swap becomes a full-corpus re-enrichment. Must ship as a deliberate once-only
  migration or a versioned key, never silently.

## Consequences

Positive: one source of truth for orchestration state; no queue durability
protocol to maintain; in-flight, backlog, and failures all reconstruct from the
instance and the lake with an accounting identity
(`done + failed + in_flight + backlog == candidates`) that makes a silently wrong
metric detectable; retries are surgical rather than whole-unit; provider swaps
are config.

Negative / work this commits us to:

- **The in-process pin is required for DuckDB-writing ops, and only for that
  reason.** DuckDB is single-writer; a subprocess executor contends with any other
  handle on the file. State the rule that way — never as "the executor corrupts
  instance state", which was a misdiagnosis of a spike-harness bug (see decision
  8 and the S1 correction in the spike `FINDINGS.md`).
- **The failure check rests on an invariant that must be tested**: a failed item
  must NEVER be conformed. If that rule breaks, the anti-join empties and the
  andon passes while failures accumulate — the worst available failure mode. It
  needs a direct test.
- **`bronze_enrichment_raw` cannot distinguish success from failure directly**
  (no `ok`/status column; the output or the error both land in `response_text`).
  Failure is inferred from absence, so an explicit status column is recommended
  as a fast-follow schema change.
- **The prompt-identity change invalidates every existing hash once.** It must be
  a deliberate migration (once-only re-enrichment or a versioned key).
- **Observability moves**: "how many are in flight" becomes a query over the
  instance rather than a `SELECT` on a familiar table, so any existing operator
  habit or dashboard reading `batch_jobs` must be rebound.
- **Provider quota is a distinct failure from service-down.** ADR-0009's health
  gate must separate them in its message, or an operator chases a healthy
  service while every item fails. The live gate hit exactly this: OpenRouter
  returned `403 Key limit exceeded (daily limit)` while its model list still
  returned 200.

## Supersedes / Superseded by

Supersedes the **queue and dead_letter half** of ADR-0004 (`batch_jobs`,
`batch_items` with `attempts`/`scheduled_for`, and `dead_letter`). ADR-0004's
central decision — operational state in SQLite, analytical state in DuckDB, media
cache and creators/profiles where they are — STANDS and is not in dispute.
ADR-0004's Status records the partial supersession. Related: ADR-0007, ADR-0008,
ADR-0011.
