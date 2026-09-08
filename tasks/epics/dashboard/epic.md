# Epic E-DASHBOARD — Dashboard product surface

- **Theme:** Product UI (user-facing)
- **Owner:** sdlc-worker
- **Status:** Active
- **Depends on:** E-SERVING-ANALYTICS
- **Consumer signal:** E-ENRICH-LABELS (`v_engagement_outliers` future-leak
  rewire is OBLIGATORY alongside the label change)

## Outcome
A dashboard product that surfaces creators, posts, signals, and performance —
thin server projectors over serving views, no aggregation in the API layer, no
data-engine logic in the UI.

## Scope highlights
- Tables + filtering across Overview / Posts / Signals / Profiles; click-through
  to posts and creators.
- Creators page (roster + single-creator footprint + cross-platform socials).
- Standout/underperformer/outlier surfaces; rising-creator momentum view.
- Guard: `tests/unit/dashboard/test_no_aggregation_in_server.py`.

## Source of truth
`tasks/plans/dashboard-tables-filtering.md`, `creators-ui-redesign.md`,
`serving-test-and-docs-repair.md`, `post-performance-observations-workstreams.md`
(Epic 5).

## Epic DoD
- [ ] Overview shows Top/Rising creators with click-through (post-performance
      Epic 5 criteria).
- [ ] `v_engagement_outliers` future-leak rewire shipped with the label signal.
