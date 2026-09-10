# Epic E-ENRICH-LABELS — Triage labels & cost-first enrichment admission

- **Theme:** Enrichment value driver (labels)
- **Owner:** dlc-worker
- **Status:** Active
- **Depends on:** E-ENRICH-ENGINE
- **Feeds:** E-SERVING-ANALYTICS, E-DASHBOARD (consumer signal)

Every post carries an auditable, self-versioned performance label
(`standout` / `control` / `floor_filler` / `skip`), and the enrichment queue
drains **only** high-value posts for the expensive deep pass — so text-LLM
spend goes where it earns.

**Split admission (audit 2026-09-09; reconciled to the settled contract
2026-09-10):** triage-first now gates ONLY the deep classification pass
(`silver_content_classification`, the table that replaces the retired
`gold_analyses`). The universal qwen visual pass (facets + summaries →
`silver_visual_annotations` + `silver_visual_summaries`, E-ENRICH-FACETS/
E-ENRICH-SUMMARIES) is deliberately admission-free corpus-wide — its visual
input is paid for every media post anyway, so summaries/facets ride it as
marginal output (`docs/enrichment-enhancement-design.md` §6; the fold decision
reverses triage-first for that pass). Labels remain the engagement-utility
criterion for facets (US-EFAC-2) regardless of admission.

## Scope highlights
- `ig_post_labels` with `label_version`; `enrich_decision` (gates the deep
  classification pass into `silver_content_classification` only — see split
  admission above); empty captions → `skip` (US-L6).
- Self-versioning: any formula/schema change marks rows stale in the SAME
  commit (no silent-staleness trap).
- Enrichment discovery is stateless over labels (watermark retired); re-enrichment on
  stale prompt / explicit post_ids bypass.
- `v_engagement_outliers` rewire + negative-surface magnitude split (folds into
  E-SERVING-ANALYTICS delivery).
- Consumer signal → E-DASHBOARD (`v_engagement_outliers` future-leak rewire).

## Source of truth
`tasks/plans/post-performance-observations-workstreams.md` (Epic 3 — the value
driver), `post-performance-observations-implementation.md`.

## Epic DoD
- [ ] Enrichment queue drains standouts/control/floor-fillers only; zero uniform deep
      enrichment; staleness surfaced by `label_version` bump.
