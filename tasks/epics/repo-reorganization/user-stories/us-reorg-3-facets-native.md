---
id: US-REORG-3
epic: E-REORG
persona: P1
status: Done
---
# US-REORG-3 — The growth-facets pass is a registered workload, not a hand-rolled CLI

- **Epic:** E-REORG
- **Status:** Done — branch `refactor/facets-native`
- **Depends on:** US-REORG-2 (the workload registry and submit-as-discovery-actor)
- **Relates to:** ADR-0012 (orchestration state), ADR-0016 (one derivation),
  E-ENRICH-FACETS (the pass itself), E-ENRICH-TRANSCRIPTS (the same shape, later)
- **Source:** `tasks/plans/repo-reorganization.md` §3

## Story

**As a** pipeline operator, **I want** the growth-facets passes to run through the same
submit/harvest machinery as every other enrichment workload, **so that** they share one
discovery path, one retry budget and one set of DQ gates — instead of living in a CLI
that bypasses all three.

## Why

The facet passes were the only enrichment driving its own lifecycle: a 243-line CLI
(`scripts/enrich_facets_batch.py`) with `--plan` / `--run` / `--harvest` arms, calling
`engine/facets_batch.py` directly. That CLI wrote to the lake outside any Dagster run, so
it produced no run record and no asset-check result — the orchestration state ADR-0012
makes authoritative. It also kept a second copy of the eligibility queries, the pricing
constants and the prompt-hash map, which is how the facet workloads came to be absent from
the anti-join check's workload map while their `silver_*` tables sat empty and unnoticed.

## Acceptance criteria (binary)

- AC1: `growth-facets-visual` and `growth-facets-text` are registered `Workload`s with
  their own `candidates`, `build_item`, `estimate`, `job_spec`, `prompt_hash`,
  `schema_version` and `parse`.
- AC2: `scripts/enrich_facets_batch.py`, `scripts/archive/poll_qwen_run.py` and
  `engine/facets_batch.py` are deleted; nothing imports them.
- AC3: The facet eligibility queries exist once, in the registry — `_facets_candidates`
  reads completion from that pass's typed silver table (never the retired
  `gold_growth_facets`, which marked conform-quarantined posts as done).
- AC4: `SubmitConfig` carries `dry_run: bool = False`; a dry run runs discovery, the guard
  and item building, projects tokens/USD with the same arithmetic a real run uses, and
  writes NOTHING to the instance.
- AC5: `scripts/make_smoke_slice.py`'s `enrichment_plan_check` calls the workload
  registry's `candidates`/`build_item` in-process rather than shelling out to a CLI.
- AC6: Submit issues ONE provider job per workload (each carries its own job-level
  options) and records that workload's handle on the partitions it covered.
- AC7: `WORKLOAD_SILVER_TABLES` in `ig_enriched/slv/checks.py` covers all three
  workloads, so an empty `silver_*` table is an anti-join loss rather than silence.
- AC8: `harvest.land_result` stamps each landing with the provenance of the workload that
  produced it — never the classification prompt's hash for a facet response (which would
  poison the bronze→silver replay), and never a guessed provenance for an unregistered
  workload.
- AC9: `SubmitConfig(workload=...)` naming an unregistered workload raises.

## Definition of done

- [x] Every AC above holds.
- [x] All three workloads discover and build against the smoke slice, each with a distinct
      prompt hash and job spec.
- [x] A `workload=`-scoped submit per workload issues three separate provider calls with
      three distinct job specs, and all three workloads appear in the in-flight set.
- [x] `uv run dagster definitions validate` passes; `uv run ruff check` is clean.
- [x] Squash-merged to `main` via PR.

## Verification performed

Against `data/smoke/` (real slice, real silver rows):

| Workload | candidates | built | unbuildable | est. tokens | est. USD |
|---|---|---|---|---|---|
| `content-classification` | 49 | 49 | 0 | 120,006 | $0.0269 |
| `growth-facets-visual` | 21 | 21 | 0 | 60,253 | $0.0130 |
| `growth-facets-text` | 100 | 100 | 0 | 51,573 | $0.0149 |

Submit (stub adapter, ephemeral instance) issued three calls —
`JobSpec(max_tokens=None, mode=None)` × 49, `JobSpec(max_tokens=4096, mode='visual')` × 7,
`JobSpec(max_tokens=1024, mode='text')` × 44 — with all three workloads in flight (100
partitions). A bogus `workload=` raised. An unregistered workload name is refused by
`land_result` rather than landed with guessed provenance.

## Tests

| AC | Test |
|---|---|
| AC1, AC3 | `tests/unit/enrichment/test_facets_workloads.py` |
| AC4 | dry-run assertions in the same module |
| AC8 | provenance assertions in the same module |
| AC2, AC5 | graph + import sweep; `scripts/make_smoke_slice.py` runs in-process |
