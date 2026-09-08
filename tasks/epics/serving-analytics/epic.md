# Epic E-SERVING-ANALYTICS — Serving dims, metric views & creator analytics

- **Theme:** Serving / analytics
- **Owner:** dlc-worker
- **Status:** Active
- **Depends on:** E-ENRICH-ENGINE, E-ENRICH-LABELS, E-ENRICH-FACETS (feed),
  E-IDENTITY (dim_profile creator linkage)
- **Feeds:** E-DASHBOARD

## Outcome
A serving layer of dims + views that exposes per-post quality, per-creator
rollups, outliers, rising creators, and engagement baselines — thin projectors
the dashboard reads with zero aggregation.

## Scope highlights
- `dim_profile` (SCD2, creator-linked), `dim_date`, 17 views incl. canonical
  metric views (`v_post_metrics`, `v_creator_profile`, `v_post_baselines`,
  `v_engagement_outliers`, `v_recent_hot_posts`, `v_standout_calendar`, …).
- Point-in-time baseline semantics; NO creator-avg leak; outlier z-scores
  label-backed.
- Follower-observation serving (post→owner follower tier at post time),
  negative/underperformer magnitude split.
- Consumer of E-ENRICH-FACETS/SUMMARIES for embeddings/search once those land.

## Source of truth
`tasks/plans/phase-4-serving.md`, `metrics-centralization.md`,
`creator-metrics.md`, `creator-ranking-rising-creators.md`,
`follower-observations-underperformer-eda.md`, `serving-test-and-docs-repair.md`.

## Epic DoD
- [x] Views mirror the metric semantics; no aggregation in `server.py`
      (`tests/unit/dashboard/test_no_aggregation_in_server.py`).
- [ ] Serving views exposed for growth facets + summaries when they ship.
