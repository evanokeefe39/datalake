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

## E-ENRICH-ENGINE — qwen batch engine (ADR-0009; pre-pivot history kept)
- `enrichment-v3-migration-master.md` — **the migration coordinator** (all seven
  phases, current → target; spans every epic below)
- `inference-service-seam.md` → US-EENG-1/US-EENG-2 (Phase 1: the one seam, the
  bronze landing, the provider swap)
- `dagster-native-orchestration-implementation.md` → ADR-0012 (Phases 2-3:
  Dagster-native orchestration, prompt identity)
- `dagster-native-enrichment-spikes.md` → ADR-0012 evidence (spikes S1-S6)
- `facet-batch-native.md` (qwen facets lifecycle, pre-consolidation)
- `qwen-batch-enrichment.md` → US-EENG-1/US-EENG-2 (qwen-batch backend swap; pivot)
- Pre-pivot (Gemini era): `phase-3-gold-asset.md`, `enrichment-architecture-v2.md`,
  `migration-enrichment-v2.md`, `gold-enrichment-scaling.md`, `enrichment-exec-upgrade.md`,
  `multimodal-processing.md`, `pipeline-hardening-architecture-review.md`,
  `refactor-architecture-investigation.md`, `orchestration-investigation-opening.md`
- Supporting: `docs/architecture/adr/0009`, `docs/architecture/adr/0007` (shape) + `0008`, `docs/architecture/adr/0002/0003`

## E-ENRICH-LABELS — triage labels
- `post-performance-observations-workstreams.md` (Epic 3 — the value driver)
- `post-performance-observations-implementation.md`

## E-ENRICH-FACETS — cross-modal facets (NEW)
- `silver-conform-tables.md` → US-EFAC-1/3/4 (Phase 4: `silver_visual_annotations`,
  `silver_text_annotations`)
- `facet-list-experiment-design.md` → US-EFAC-1/2/3/4
- `docs/architecture/enrichment-design-v1-superseded.md` (§3–§4)

## E-ENRICH-SUMMARIES — folded summaries (NEW)
- `silver-conform-tables.md` → US-ESUM-1/3 (Phase 4: the summary tables conform
  from the same visual/text submit)
- `docs/architecture/enrichment-design-v1-superseded.md` (§6) → US-ESUM-1/2

## E-ENRICH-TRANSCRIPTS — ASR capture (NEW)
- `silver-conform-tables.md` → US-ETR-1/2/3 (Phase 4: the audio pass)
- `docs/architecture/enrichment-design-v1-superseded.md` (§5) → US-ETR-1/2/3/4

## E-SERVING-ANALYTICS — dims/metrics/creator analytics
- `content-classification-migration.md` → US-ESA-1 (Phase 5: migrate the live
  `gold_analyses` to `silver_content_classification`, no serving regression)
- `phase-4-serving.md`, `metrics-centralization.md`
- `creator-metrics.md`, `creator-ranking-rising-creators.md`
- `follower-observations-underperformer-eda.md`
- `serving-test-and-docs-repair.md` (serving half)
- `growth-research-report.md`, `docs/research/creator-growth-analysis.md` (research)
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
