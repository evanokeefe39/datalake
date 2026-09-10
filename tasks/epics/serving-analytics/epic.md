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
the dashboard reads with zero aggregation — plus the FOUR GOLD MARTS shaped to
the owner's three questions (who performs well in X domain; what content shape
performs in X domain/topic/follower tier; which posts do well across all
domains and what is their shape).

## Scope highlights
- `dim_profile` (SCD2, creator-linked), `dim_date`, 17 views incl. canonical
  metric views (`v_post_metrics`, `v_creator_profile`, `v_post_baselines`,
  `v_engagement_outliers`, `v_recent_hot_posts`, `v_standout_calendar`, …).
- Point-in-time baseline semantics; NO creator-avg leak; outlier z-scores
  label-backed.
- Follower-observation serving (post→owner follower tier at post time),
  negative/underperformer magnitude split.
- Consumer of E-ENRICH-FACETS/SUMMARIES for embeddings/search once those land.
- **Gold enrichment marts (the owner's three questions; strict medallion —
  gold is analytic marts, NOT per-channel mirrors of silver):**
  1. `gold_post_enrichment` — the wide per-post shape: all six silver channel
     outputs joined + engagement metrics + provenance. PK `(post_id, platform)`.
  2. `gold_creator_performance` — Q1 "who is performing well in X domain?".
     PK `(creator_id, platform)`. COMPOSES the canonical metric views as its
     upstream — `v_creator_profile` (momentum_ratio, is_rising,
     avg_engagement_score, dominant_domain, total_posts) and
     `v_post_follower_context` (follower_tier, at-post-time) — and adds ONLY
     the enrichment-derived content profile (domain slicing over silver
     classification). Does NOT restate tier buckets or momentum
     windows/gates (single definition in the views); domain = CONTENT niche.
  3. `gold_content_shape_performance` — Q2 "what is the shape of the content
     that performs well in X domain / topic / follower tier?". LONG table, PK
     `(domain, topic, follower_tier, facet_name, facet_value)`: n_posts,
     avg_engagement_z, standout_rate, lift_vs_slice_baseline. Groups facets
     by `follower_tier` from `v_post_follower_context` and measures
     performance via `v_post_metrics` (engagement z / standout); NO metric
     re-derived. Long form survives facet-schema evolution.
  4. `gold_top_posts` — Q3 "what posts are doing well across all domains, and
     what is the shape of their content?". PK `(post_id, platform)`: rank/
     percentile across all domains, joined to the full shape +
     summary/transcript for qualitative reading.
  The canonical metric views are the SINGLE metric definition: the marts
  COMPOSE them; only thin analytics projections derive from the marts. Key
  fix: enrichment keys are `(post_id, platform)` — `platform` matches
  `profiles.platform`; `domain` means the CONTENT niche (dev/AI/…), never
  the platform.
- **Layer order (enforced):** `bronze_enrichment_raw` → `silver_*` →
  `v_post_detail` (serving base: silver posts ⋈ silver enrichment ⋈ dims) →
  21 canonical metric views + dims → the four gold marts. The canonical views
  are UPSTREAM of the marts and are never re-pointed at them (no cycle).
- **Preserved serving surface (no-regression):** the re-layer is additive —
  every existing serving surface keeps its canonical definition over
  `v_post_detail`: `v_recent_hot_posts` (hot posts); `v_outlier_posts` /
  `v_engagement_outliers` / `v_creator_outlier_rate` (standout, outlier);
  `v_underperformer_posts` / `v_creator_underperformer_rate` (underperformers);
  `v_rising_creators` + `v_creator_profile` (rising creators, momentum);
  `v_post_metrics` / `v_post_baselines`; `v_creator_metrics` /
  `v_creator_quality` / `v_creator_topics` (creator detail);
  `v_post_follower_context`; `v_signal` / `v_quality_trend` /
  `v_domain_coverage` / `v_profile_metrics` / `v_overview` /
  `v_standout_calendar`; and the dims `dim_profile` / `dim_date`. None may be
  dropped, renamed, or restated.

## Source of truth
`tasks/plans/phase-4-serving.md`, `metrics-centralization.md`,
`creator-metrics.md`, `creator-ranking-rising-creators.md`,
`follower-observations-underperformer-eda.md`, `serving-test-and-docs-repair.md`.

## Epic DoD
- [x] Views mirror the metric semantics; no aggregation in `server.py`
      (`tests/unit/dashboard/test_no_aggregation_in_server.py`).
- [ ] Serving views exposed for growth facets + summaries when they ship.
- [ ] The four gold marts (`gold_post_enrichment`, `gold_creator_performance`,
      `gold_content_shape_performance`, `gold_top_posts`) materialized and
      queryable for the owner's three questions (US-ESA-1).
- [ ] No serving-surface regression: all 21 canonical views + 2 dims still
      exist and still resolve over `v_post_detail`; the existing serving +
      readiness tests stay green.
