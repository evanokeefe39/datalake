# Plans ↔ Epics / User Stories index

Uniform reverse-lookup: every `tasks/plans/*.md` plan maps back to the epic(s)
it serves and, where applicable, the user stories it implements. The forward
direction lives in each `tasks/epics/<slug>/epic.md` under `## Source of truth`
(epic → plans). This index is the inverse (plan → epic). It validates the
cohesive-story claim: **every plan sits on the enrichment spine.**

Legend: mappings under each epic come from that epic's `## Source of truth`;
plans not yet cited by an epic are assigned by title and marked `[assign]`.

## Conventions
- New plans declare their epic + user stories at the top:
  `Relates to: <epic-slug> (E-<ID>) · US-<x>`.
- Plans are immutable history — when one needs aligning with a merged epic,
  publish a new version that links to it (never edit the original retroactively).

## E-INGEST — ingest-silver
- `phase-1-foundation.md`, `phase-2-silver-asset.md`
- `ig-local-ingestion.md`
- `watermark-deadletter-refactor.md` (watermark half)
- `state-readiness-impl.md`, `state-readiness-tests.md`
- `duckdb-lakehouse-evaluation.md` `[assign]` (early storage eval → foundation)

## E-MEDIA — media-capture
- `multimodal-processing.md` (scrape-time byte cache / media_files)
- `media-and-entity-routing.md`
- `19-20-batch-multimodal-mime.md` (media_cache + mime fixes)

## E-ENRICH-ENGINE — batch-native Gemini engine
- `phase-3-gold-asset.md` `[assign]` (gold enrichment)
- `enrichment-architecture-v2.md`, `migration-enrichment-v2.md`
- `gold-enrichment-scaling.md`, `enrichment-exec-upgrade.md`
- `multimodal-processing.md` (engine multimodal pass)
- `refactor-architecture-investigation.md`, `orchestration-investigation-opening.md`
- `pipeline-hardening-architecture-review.md`
- Supporting: `docs/refactor-research/migration-batch-native-enrichment*`,
  `docs/adr/0007` + `0008`, `docs/adr/0002/0003`, `tasks/plans/findings/enrichment-architecture-assessment.md`

## E-ENRICH-LABELS — triage labels
- `post-performance-observations-workstreams.md` (Epic 3 — the value driver)
- `post-performance-observations-implementation.md`

## E-ENRICH-FACETS — cross-modal facets (NEW)
- `facet-list-experiment-design.md` → US-EFAC-1/2/3/4
- `docs/enrichment-enhancement-design.md` (§3–§4)

## E-ENRICH-SUMMARIES — folded summaries (NEW)
- `docs/enrichment-enhancement-design.md` (§6) → US-ESUM-1/2

## E-ENRICH-TRANSCRIPTS — ASR capture (NEW)
- `docs/enrichment-enhancement-design.md` (§5) → US-ETR-1/2/3/4

## E-SERVING-ANALYTICS — dims/metrics/creator analytics
- `phase-4-serving.md`, `metrics-centralization.md`
- `creator-metrics.md`, `creator-ranking-rising-creators.md`
- `follower-observations-underperformer-eda.md`
- `serving-test-and-docs-repair.md` (serving half)
- `growth-research-report.md`, `docs/creator-growth-analysis.md` (research)
- `post-performance-observations-investigation.md`,
  `investigate-post-metrics-observations.md` (Epic 1–2 groundwork)
- `post-performance-observations-workstreams.md` (Epics 1, 2, 4) — shared with E-ENRICH-LABELS

## E-IDENTITY — creators/profiles
- `creators-and-profiles.md`, `profile-management.md`
- `creators-ui-redesign.md` (identity half; also E-DASHBOARD)
- `curated-creator-consolidation-hotposts-fix.md` (consolidation half; also E-SERVING-ANALYTICS)

## E-DASHBOARD — product UI
- `dashboard-tables-filtering.md`
- `creators-ui-redesign.md` (UI half)
- `post-performance-observations-workstreams.md` (Epic 5 — dashboard rewire)
- `rest-api-push-tweaks.md`, `rest-api-push-refactor.md` `[assign]`
- `serving-test-and-docs-repair.md` (dashboard/tests half)

## Meta / process (no product epic — governance)
- `expert-panel-review.md`, `test-hardening.md` — repo process, not a feature.

## Unmapped
None — every plan lands on the spine above. This index should stay in sync as
new plans land: add the plan to its epic's `## Source of truth` in `epic.md`
and it will surface here.
