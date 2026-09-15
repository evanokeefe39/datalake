# Epic E-REORG — Repository reorganization into a uv workspace

- **Theme:** Repository structure & boundaries
- **Owner:** sdlc-worker
- **Status:** Active
- **Depends on:** E-ENRICH-ENGINE (ADR-0011/0012 must have landed)
- **Feeds:** services-extraction, storage-migration
- **Relates to:** ADR-0015 (this epic's decision record), ADR-0016 (discovery folds into
  submit)

## Outcome

The repository has boundaries that match its components. `services/orchestration` is the
Dagster code location, `services/jobs` is the inference service, `services/dashboard` is
the serving app, and `packages/` holds the contracts they share — with a declared
dependency direction (`orchestration → opsdb`, `dashboard → opsdb`, never
orchestration ↔ dashboard). Inside the code location, module placement states what a
module *is*: cross-domain machinery in `engine/`, a platform's payloads beside that
platform, transport in `integration/`, plumbing in `platform/`.

The epic also removes what the old layout kept alive past its usefulness: a hand-rolled
CLI that bypassed orchestration state, a module that silently routed paid work to a
retired provider, four ownerless modules, and a naming vocabulary that led with vocabulary
the pipeline no longer uses.

## Why now

Two problems made the old layout actively harmful rather than merely untidy.

**A retired provider was still reachable on the paid path.** `detect_provider()` returned
`"direct_batch"` whenever the Gemini tier gate opened, and `.env` set `GEMINI_TIER=tier1` —
so the live classification workload could run on the backend ADR-0009 replaced. Nothing in
the tree said which provider was authoritative, because both had equal standing.

**Orchestration had two entry points.** `src/datalake/cli/` called asset functions through
`build_asset_context`, outside any Dagster run: no run record, no event log, no asset
checks — invisible to the sensor and partition machinery that *is* the orchestration state
under ADR-0012. The tree presented that as a peer of the Dagster jobs.

## User stories

| Story | Scope | Branch |
|---|---|---|
| [US-REORG-1](user-stories/us-reorg-1-workspace-tree.md) | The tree move, the `datalake`→`orchestration` rename, the retirements | `refactor/workspace-tree` |
| [US-REORG-2](user-stories/us-reorg-2-submit-discovery.md) | The drain folds into submit; the submit sensor closes the trigger gap | `refactor/submit-discovery` |
| [US-REORG-3](user-stories/us-reorg-3-facets-native.md) | The growth-facets pass joins the engine | `refactor/facets-native` |

## Out of scope for this epic

Moving `services/jobs` and `services/dashboard` into the workspace, and the R2 storage
migration. The tree names their destinations; those moves are `services-extraction` and
`storage-migration`. This epic creates `packages/opsdb` and `packages/storage` so both are
mechanical when they land.

Testing and integration are deferred to the phase after this epic: acceptance here is
**structural** — the packages import, the Dagster graph validates, lint is clean. The
pytest suite's import paths are retargeted because the move forces it, but its assertions
are that later phase's work.

## Definition of done

- All three branches merged to `main` via squash-merge PRs.
- `uv sync --dev` resolves the workspace; every member installs.
- `uv run dagster definitions validate` passes on the final tree.
- `uv run ruff check services/ packages/ tests/ scripts/ migrations/` is clean.
- No module outside the adapter layer names a provider; no module is named after one.
- ADR-0015 and ADR-0016 are Accepted and indexed.
