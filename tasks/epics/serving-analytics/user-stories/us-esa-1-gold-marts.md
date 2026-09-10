---
id: US-ESA-1
epic: E-SERVING-ANALYTICS
persona: P7
status: Open
---
# US-ESA-1 — Four gold marts shaped to the owner's three questions

- **Epic:** E-SERVING-ANALYTICS
- **Status:** Open
- **Relates to:** E-ENRICH-ENGINE (bronze landing + six silver channel tables),
  E-ENRICH-FACETS (facet facets), E-ENRICH-LABELS (standout labels),
  E-IDENTITY (creator linkage), E-DASHBOARD (consumer)
- **Source:** strict-medallion re-layering of enrichment (2026-09-10); owner's
  content-strategy goal

## Story

**As a** growth analyst (the owner), **I want** the enrichment outputs joined
and aggregated into four gold marts — `gold_post_enrichment`,
`gold_creator_performance`, `gold_content_shape_performance`,
`gold_top_posts` — **so that** I can directly answer: (Q1) who is performing
well in X domain, (Q2) what is the shape of the content that performs well in X
domain / X topic / for X follower count, and (Q3) what posts are doing well
across all domains and what is the shape of their content — without hand-rolling
joins over six channel tables.

## Contract

- Strict medallion: gold is analytic marts, NOT per-channel mirrors of silver.
  Dependency direction: the canonical metric views (`v_creator_profile`,
  `v_post_follower_context`, `v_post_metrics`, `v_creator_metrics`) are the
  SINGLE metric definition; the marts COMPOSE those views together with the
  silver enrichment outputs and MUST NOT restate tier buckets or momentum
  constants (single definition — WATCHDOG metrics-centralization); only thin
  analytics projections derive from the marts.
- Keys are `(post_id, platform)` / `(creator_id, platform)` — `platform`
  matches `profiles.platform`; `domain` means the CONTENT niche
  (dev/AI/tech/indie/data/AI-engineer), never the platform. This resolves the
  old duplicate-`domain` bug (enrichment tables keyed on platform 'instagram'
  vs `gold_content_classification.domain` = content niche).
- Gold reads SILVER only (deterministic, ZERO API calls); silver carries the
  validation/quality contract.

## Acceptance criteria (binary)

- AC1: `gold_post_enrichment` — the wide per-post shape: all six silver channel
      outputs (`silver_visual_annotations`, `silver_visual_summaries`,
      `silver_audio_transcripts`, `silver_text_annotations`,
      `silver_text_summaries`, `silver_content_classification`) joined +
      engagement metrics + provenance. PK `(post_id, platform)`.
- AC2: `gold_creator_performance` (Q1) — PK `(creator_id, platform)`;
      COMPOSES `v_creator_profile` (momentum_ratio, is_rising,
      avg_engagement_score, dominant_domain, total_posts) and
      `v_post_follower_context` (follower_tier) as upstream, adding ONLY the
      enrichment-derived content profile; columns: follower_count,
      follower_tier, post_count, median_engagement_score,
      avg_engagement_score, standout_rate, momentum_ratio, is_rising,
      dominant_domain; filterable by domain (content niche). Does NOT
      re-derive any of those metrics.
- AC3: `gold_content_shape_performance` (Q2) — LONG table, PK
      `(domain, topic, follower_tier, facet_name, facet_value)`: n_posts,
      avg_engagement_z, standout_rate, lift_vs_slice_baseline; groups the
      enrichment facets by `follower_tier` from `v_post_follower_context`
      and measures performance via `v_post_metrics` (engagement z /
      standout); NO metric re-derived. Long form so facet-schema evolution
      does not break the mart.
- AC4: `gold_top_posts` (Q3) — PK `(post_id, platform)`: rank/percentile
      across ALL domains, joined to the full content shape + summary/transcript
      for qualitative reading.
- AC5: Marts are additive-only, idempotent on their PKs (re-run produces
      identical output), and derived purely from silver + existing serving
      metrics (no API calls, no aggregation in `server.py`).
- AC6: No serving-surface regression — all 21 canonical metric views/dims
      (`v_post_detail`, `v_recent_hot_posts`, `v_outlier_posts`,
      `v_engagement_outliers`, `v_creator_outlier_rate`,
      `v_underperformer_posts`, `v_creator_underperformer_rate`,
      `v_rising_creators`, `v_creator_profile`, `v_post_metrics`,
      `v_post_baselines`, `v_creator_metrics`, `v_creator_quality`,
      `v_creator_topics`, `v_post_follower_context`, `v_signal`,
      `v_quality_trend`, `v_domain_coverage`, `v_profile_metrics`,
      `v_overview`, `v_standout_calendar`, `dim_profile`, `dim_date`) still
      exist and still resolve over `v_post_detail`. None is dropped, renamed,
      or re-pointed at a mart (no cycle). Authoritative list: `DUCKDB_VIEWS`
      (`src/datalake/defs/common/schemas.py`); enforced by
      `tests/operational/test_state_compatibility.py` (parametrized over
      `EXPECTED_DUCKDB_VIEWS` against the live DB).

## Definition of done

- [ ] All four marts materialized on a sample slice first (temp/isolated DB
      smoke), then production wiring reviewed.
- [ ] Every view in the preserved serving surface still exists and resolves
      over `v_post_detail`; the existing serving tests stay green.
- [ ] Schema catalog + readiness green for the four marts.
- [ ] A query per owner question runs against real rows and returns
      sensible output (spot-checked against the underlying silver tables).
- [ ] Dashboards/consumers that read silver channel tables directly are
      enumerated and re-pointed (or intentionally left), recorded in the PR.

## Tests

- Re-running the mart build twice yields identical rows (idempotency).
- Row-count reconciliation: every post with silver enrichment rows appears in
  `gold_post_enrichment` (no silent drops); counts match the silver sources.
- `gold_content_shape_performance` facet cells sum back to the slice's post
  count (no double-counting across facet values).
- Q1/Q2/Q3 example queries return rows grounded in the fixture dataset.
