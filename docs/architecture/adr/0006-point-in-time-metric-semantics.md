# ADR-0006: Point-in-time-only metric semantics (per-post baselines, never all-time averages)

- Status: Accepted
- Decided: 2026-08 (backfilled 2026-09-03)

## Context

A long design saga established the correct way to judge post performance. A
post's likes/comments/views must be judged against its **own trailing,
point-in-time baseline** (the creator's prior posts at the time the post
appeared) — never a creator all-time/current average, and never a live
recomputed baseline. Using an all-time average mislabels big-account viral
posts and ignores how a creator's baseline shifts over time. The label pass
(per creator, trailing N=20 prior posts or 90-day lookback, `baseline_center`=Q3,
`baseline_spread`=IQR) produces per-post z-scores; `standout` = Tukey label
(z > 1.5); **hot = standout AND z ≥ 2 (2σ+)**. Q3/IQR must not be mislabeled as
"average."

## Decision

All performance metrics are point-in-time and baseline-normalized, never
all-time averages. The serving views (`v_post_metrics` et al.) expose per-post
`likes_zscore`, `comments_zscore`, `views_zscore` and a baseline-normalized
weighted `engagement_score` (0.5·likes_z + 0.3·comments_z + 0.2·views_z, NULL-safe),
computed in the warehouse (ADR-0005), consuming per-post baselines for comments
and video views computed in the serving layer (label pass untouched).

## Alternatives considered

- Judging posts vs a creator all-time/current average: rejected (user).
- Judging vs a live/changing baseline: rejected — must be point-in-time.
- Extending the versioned `ig_post_labels` pass to comments/views baselines:
  rejected — avoids another full re-label / LABEL_VERSION churn; baselines
  computed in the serving layer instead.

## Consequences

Positive: size-independent, creator-relative, honest signal; incremental gold
works (only net-new posts re-enrich); no re-label churn for comments/views.
Negative: metrics are relative by design — absolute engagement still exposed
separately (counts); consumers must not render z-scores/engagement_score as
averages (guarded by labels/UI honesty rules).

## Supersedes / Superseded by

Superseded by: none. Related: ADR-0005.
