# Epic E-ENRICH-LABELS — Triage labels & cost-first enrichment admission

- **Theme:** Enrichment value driver (labels)
- **Owner:** dlc-worker
- **Status:** Active
- **Depends on:** E-ENRICH-ENGINE
- **Feeds:** E-SERVING-ANALYTICS, E-DASHBOARD (consumer signal)

## Outcome
Every post carries an auditable, self-versioned performance label
(`standout` / `control` / `floor_filler` / `skip`), and the enrichment queue
drains **only** high-value posts (triage-first) so Gemini spend goes where it
earns (uniform → triage cost lever).

## Scope highlights
- `ig_post_labels` with `label_version`; `enrich_decision`; empty captions →
  `skip` (US-L6).
- Self-versioning: any formula/schema change marks rows stale in the SAME
  commit (no silent-staleness trap).
- Gold discovery is stateless over labels (watermark retired); re-enrichment on
  stale prompt / explicit post_ids bypass.
- `v_engagement_outliers` rewire + negative-surface magnitude split (folds into
  E-SERVING-ANALYTICS delivery).
- Consumer signal → E-DASHBOARD (`v_engagement_outliers` future-leak rewire).

## Source of truth
`tasks/plans/post-performance-observations-workstreams.md` (Epic 3 — the value
driver), `post-performance-observations-implementation.md`.

## Epic DoD
- [ ] Gold queue drains standouts/control/floor-fillers only; zero uniform deep
      enrichment; staleness surfaced by `label_version` bump.
