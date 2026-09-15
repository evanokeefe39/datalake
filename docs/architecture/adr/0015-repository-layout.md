# ADR-0015: Repository layout — a uv workspace, three boundaries, and where code lives

- Status: Accepted
- Decided: 2026-09-15

## Context

The repository grew as a single-package Dagster monolith: everything importable lived
under `src/datalake/`, and `defs/` became a holding area for whatever had no better home.
By September 2026 that had produced four concrete problems.

*The package name lied about the scope.* `datalake` named the whole system while containing
only the Dagster code location. The inference service and the dashboard — both real
components — lived outside it, with no declared boundary between them. A reader asking
"what depends on what" had to infer it from imports.

*The layout did not match the domain.* `defs/` held enrichment engine machinery, Instagram
payloads, ops-database access, serving marts, and a `common/` drawer, all as flat siblings.
Nothing said which of those generalize across data sources and which do not, so the
distinction was re-derived on every change.

*Retired code stayed load-bearing.* Four modules with no owner after their features
retired, a dead 29-line module, a dead lifecycle function, and a 19-name facade with zero
importers were still on the import path — and one of them, `detect_provider`, silently
routed the live classification workload to a provider ADR-0009 had retired.

*Two entry points fought over orchestration.* A hand-rolled CLI called asset functions
directly through `build_asset_context`, outside any Dagster run — no run record, no event
log, no asset checks, invisible to the sensor and partition machinery that *is* the
orchestration state under ADR-0012.

## Decision

**The repository is a uv workspace. One package is not "the project"; the workspace is.**

Module names describe a role, never a provider or a vendor. The layout below is the
contract, and the conventions it encodes are enforced by two tests
(`tests/operational/test_thin_init_files.py`, `tests/unit/orchestration/test_paths_anchor.py`).

```
pyproject.toml              # workspace root: virtual project, members = services/*, packages/*
packages/opsdb/             # ops.sqlite contract — dashboard writes, pipeline reads
packages/storage/           # byte storage (local today, R2 under storage-migration)
services/orchestration/     # the Dagster code location (was src/datalake)
services/jobs/              # the inference service (services-extraction)
services/dashboard/         # FastAPI + vite (services-extraction)
```

Inside the code location:

```
orchestration/definitions.py        # the composition root — the ONLY module importing every subsystem
orchestration/defs/
  ig_core/{bnz,slv,gld}/            # one platform: scrape → silver → labels
  ig_enriched/{bnz,slv,gld}/        # that platform's enrichment PAYLOADS (prompts, schemas, mappings)
  engine/                           # cross-domain enrichment machinery — never duplicated per domain
  serving/{dims,metrics,marts,views,checks}.py
  integration/                      # external API clients — transport, no pipeline logic
  platform/                         # resources, paths, the DuckDB catalog, schedules
  youtube_core/ youtube_enriched/ tiktok_core/ tiktok_enriched/   # skeletons
```

**The eight conventions the tree encodes:**

1. **A platform is one package, staged inside it** — `<platform>_core/` for that source's
   own pipeline, `<platform>_enriched/` for what the enrichment passes produce from it.
   `{bnz,slv,gld}/` inside each keeps the medallion stages visible without duplicating the
   platform dimension.
2. **Enrichment machinery is cross-domain and lives once, in `engine/`.** A per-domain
   directory holds *payloads* — prompts, output schemas, table mappings, validation
   overlays — never a second copy of submit/harvest/partition logic.
3. **Providers are named in exactly one place.** `engine/service_backed.py` registers under
   a seam name; `build_adapter(name)` is the only lookup. No module outside the adapter
   layer names a vendor, and none is named after one.
4. **Serving is split by kind, not by size** — dimensions, canonical metric views, analytic
   marts, consumer views, checks. A metric has ONE definition and it lives in `metrics.py`;
   marts and dashboards compose it.
5. **External clients are transport-only.** `integration/` holds API clients with no
   pipeline logic; a client that starts deciding *what* to fetch has moved out of scope.
6. **`platform/` is the pipeline's own plumbing** — resources, paths, catalogs, schedules.
   It replaced `common/`, a name that invited anything to live there.
7. **`packages/` holds shared contracts, and is the only cross-service dependency path.**
   The dashboard imports `opsdb`; `orchestration` imports `opsdb`; neither imports the
   other. A contract that two services share belongs in a package, not in one of them.
8. **`__init__.py` files are docstrings and nothing else.** No imports, no assignments, no
   side effects — so importing one module never drags in its siblings' dependencies, and
   the import graph is exactly what a reader sees. Enforced by test.

**Two specific decisions inside this:**

*The data-lake root is found by walking to the `.git` marker*, replacing a fixed
`Path(__file__).resolve().parents[4]`. A parent count silently repoints `data/` — and the
whole media corpus with it — the moment the file moves, which this ADR does. The marker
walk either finds the root or raises.

*`platform/paths.py` calls `load_dotenv()` as its first executable statement.* This is the
one sanctioned import-time side effect in the codebase. It exists because `paths.py` reads
the environment into module constants at import, and it runs *before* either of the two
`load_dotenv()` calls that used to exist — so `.env`-provided `IG_*` paths were silently
ignored. Loading here closes that ordering trap at its source. `platform/resources.py`
correspondingly drops its own call: a second loader would race, and the loser would be
invisible.

**The retired vocabulary.** "Conform" was the only term in the pipeline outside the house
medallion words, and it described the silver build. The asset is `silver_enrichment`; the
runtime is `engine/silver_rt.py`; the per-table mappings are the domain payloads. The
Gemini path retires with this decision: `gemini_batch.py`, `DirectBatchAdapter`,
`GeminiResource`, `GeminiTier`/`GeminiTierConfig` and `google-genai` are deleted, and the
jobs service is the only provider on the paid path.

## Alternatives considered

*Minimal rename first.* Moving `src/datalake` → `services/orchestration` and stopping would
have been reviewable in isolation, but it would have left every naming problem intact and
made the next branch re-touch every file. The three branches here are each a full target
state for their scope, so no branch leaves a half-renamed tree.

*Keep `defs/` flat and rely on discipline.* The existing layout was not chosen — it
accumulated. A flat directory cannot express "this is cross-domain" versus "this is
Instagram's", and that distinction is the one every change has to get right.

*Put the engine under each platform.* Tempting because a platform's pipeline reads
top-to-bottom, but the enrichment machinery is genuinely shared: submit, harvest,
partitioning and the seam are identical for every platform, and duplicating them per
platform is how two implementations drift.

*A compatibility alias for `datalake`.* Cheap, and it would have let the old imports keep
working during the transition. It also makes the rename optional rather than done, and
leaves two names for one thing indefinitely. The prefix rewrite covered ~150 call sites
mechanically.

*Move `dagster.yaml` into `services/orchestration/`.* Rejected: Dagster reads
`$DAGSTER_HOME/dagster.yaml`, not the tracked file at the package root, so moving it changes
nothing about behavior while risking the concurrency pools activating on a machine where
they were not before.

*Split the SQLite schema guards (`_STALE_*`) into the catalog module.* They are assertions
about the *database's* state, not the catalog's contents, so they stay in
`tests/operational/`.

## Consequences

Positive: the dependency direction is explicit and machine-checked
(`orchestration → opsdb`, `dashboard → opsdb`, never orchestration ↔ dashboard); a new data
source is a new `<platform>_core/` package rather than a new set of files in a shared
directory; the import graph is inspectable because no `__init__` hides edges; and the
retired provider can no longer be selected by configuration.

Negative: this is a large, single-purpose diff, and it rewrites ~150 import sites and the
pytest suite's module paths. Branching, not gradual migration, was the tradeoff — the
alternative leaves the tree in a state where both layouts are half-true.

It also commits future work: a new module must be placed by *role* (engine? payload?
transport? plumbing?), and a thing that fits none of those is a signal the tree is missing
a category — not a reason to re-create `common/`.

Neutral: `services/jobs` and `services/dashboard` are named in the tree but land under
`services-extraction`; `packages/storage` is a stub until `storage-migration` wires R2. The
tree is the agreed end state, and the two later workstreams are mechanical moves because
their destinations already exist.

## Supersedes / Superseded by

Supersedes the layout described in `AGENTS.md` and `docs/OPERATING.md` prior to
2026-09-15. Does not supersede any ADR's *decision* — ADR-0008/0009/0011/0012/0013/0014 all
stand; this ADR fixes where their artifacts live. ADR-0009's tier gate
(`GeminiTierConfig`) is removed with the Gemini path, and its rate-limit table is superseded
by the jobs service's own concurrency control.
