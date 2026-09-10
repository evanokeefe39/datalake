# Architecture

Design documentation for the datalake. **Start here.**

This directory holds the design of the system — what it is, how it is shaped,
and why. It does not hold plans, backlog, or research (see "Where to go" below).

## Current vs target — the one thing to know first

Two ratified decisions describe a target the code does **not** yet implement.
Everything in this directory is explicit about which it is describing, but the
summary is here so you never have to guess.

| Area | Current (what runs) | Target (accepted, NOT implemented) |
|---|---|---|
| Enrichment model | `gold_analyses` + `gold_growth_facets` in DuckDB, written by the batch jobs | bronze `bronze_enrichment_raw` → six `silver_*` conform tables → four gold marts |
| Orchestration | `ops.sqlite` queue: `batch_jobs` / `batch_items` / `dead_letter` (+ `facets_batch_jobs`) | state in the Dagster instance + the lake; the queue is retired |
| Provider integration | two independent lifecycles (Gemini `submit`/`harvest`; qwen `facets_batch`/`qwen_client`) | one seam — a shared ledger + `submit` / `poll-to-terminal` / `retrieve` — behind a swappable `ProviderAdapter` |

The decisions behind the target are [ADR-0011](adr/0011-enrichment-layered-model.md)
(layering) and [ADR-0012](adr/0012-dagster-native-orchestration.md)
(orchestration). This table is the only part of this page that must be kept
current; everything below points elsewhere rather than restating it.

## The documents

Two axes. **Pipelines** explain how data flows and is transformed.
**Services** explain a thing that runs — a process, a server, a seam. Everything
else is the system-wide view and the schema references.

| Doc | What it covers |
|---|---|
| [`design.md`](design.md) | **The system-wide design.** Medallion layers, storage split, engine boundary, domain structure, lineage, watermarks, dead letter. Start here for how it fits together. |

**Pipelines — how data flows**

| Doc | What it covers |
|---|---|
| [`pipelines/core.md`](pipelines/core.md) | **The core posts pipeline**, end to end: Apify → bronze → silver → labels → serving. The non-enrichment path. |
| [`pipelines/enrichment.md`](pipelines/enrichment.md) | **The canonical enrichment spec** (v3). The layer model, the six `silver_*` tables, the four gold marts, naming rules, version history. |

**Services — what runs**

| Doc | What it covers |
|---|---|
| [`services/inference.md`](services/inference.md) | The inference seam and the external service: the ledger, the three verbs, the adapter swap, the `qwen-batch-service` contract. |
| [`services/dashboard.md`](services/dashboard.md) | The dashboard: a thin projector over the serving views, its guard test, and what it must never read. |

**References**

| Doc | What it covers |
|---|---|
| [`bronze-schema.md`](bronze-schema.md) | The bronze layer contract and its producers (Apify, local disk, and the target `bronze_enrichment_raw`). |
| [`growth-facets-schema.md`](growth-facets-schema.md) | The locked V3 facet schema (US-EFAC-1): fields, enums, validator behaviour. |
| [`enrichment-design-v1-superseded.md`](enrichment-design-v1-superseded.md) | **Superseded.** The v1 design, kept as the rationale and experiment record. See its header for the scheduled removal. |

## Decisions — `adr/`

The [Architecture Decision Records](adr/README.md) are the single source of truth
for *why* the design is shaped this way, including alternatives considered and
supersessions. One file per decision; the index carries status and summary.

Read them when a design looks arbitrary, or before reversing something. They are
deliberately separate from `design.md`, which describes the *as-built* shape.

## Where to go for X

Deliberately pointers, not copies — each list below already has one owner and
one place to update, and duplicating it here would create a second thing to rot.

| I want… | Go to | Tracked? |
|---|---|---|
| to know what the system does today | [`design.md`](design.md) | yes |
| how posts data flows, end to end | [`pipelines/core.md`](pipelines/core.md) | yes |
| the enrichment spec | [`pipelines/enrichment.md`](pipelines/enrichment.md) | yes |
| how the pipeline talks to a model | [`services/inference.md`](services/inference.md) | yes |
| the dashboard's rules | [`services/dashboard.md`](services/dashboard.md) | yes |
| why a decision was made | [`adr/`](adr/README.md) | yes |
| to know what the design *will* be | [`pipelines/enrichment.md`](pipelines/enrichment.md) (target sections) + [ADR-0011](adr/0011-enrichment-layered-model.md) / [ADR-0012](adr/0012-dagster-native-orchestration.md) / [`services/inference.md`](services/inference.md) | yes |
| **migration status** (what is done, what's next) | [`tasks/epics/ROADMAP.md`](../../tasks/epics/ROADMAP.md) — the build order — plus each epic's status in [`tasks/epics/README.md`](../../tasks/epics/README.md) | yes |
| which plan serves which epic | [`tasks/epics/PLANS-INDEX.md`](../../tasks/epics/PLANS-INDEX.md) | yes |
| how to operate and run the pipeline | [`docs/OPERATING.md`](../OPERATING.md) | yes |
| review traps and invariants | [`WATCHDOG.md`](../../WATCHDOG.md) | yes |
| deferred work and open issues | [`ISSUES.md`](../../ISSUES.md) | yes |
| onboarding explainers (pictorial) | [`docs/education/`](../education/) | yes |
| research and investigation history | [`docs/research/`](../research/), [`docs/refactor-research/`](../refactor-research/) | yes |
| the detailed per-phase implementation plans | `tasks/plans/*.md` — **local working notes** | **no** |

> **On the last row.** This repo's convention (`.gitignore`) is that `tasks/plans/`
> are working notes and only `tasks/epics/` is versioned. So on a fresh clone the
> plan files will not exist — that is expected, not a broken link. The **tracked**
> record of what is planned and how far along it is lives in `tasks/epics/`
> (`ROADMAP.md` for order, `README.md` for status, each epic's `user-stories/`
> for the acceptance criteria). Read those; treat any `tasks/plans/` reference
> elsewhere in these docs as an optional deeper dive that a local checkout may
> have and a fresh clone will not.

## Conventions

- **One fact, one place.** A contract lives in exactly one document; everything
  else links to it. If you find yourself copying a table, copy the link instead.
- **Label current vs target.** Never describe unbuilt work as if it runs.
- **Keep history honest.** Superseded docs keep their content and gain a
  supersession header; they are never silently rewritten.
