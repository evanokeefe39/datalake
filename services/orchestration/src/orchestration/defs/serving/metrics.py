"""Canonical metric views — the single upstream definition of every metric.

These are what marts and dashboards compose. A metric must not be
re-derived downstream: the views below are its one definition."""


from dagster import AssetKey, asset
from dagster_duckdb import DuckDBResource


@asset(
    name="v_post_baselines",
    group_name="serving",
    description=(
        "Serving-layer point-in-time trailing baselines + z-scores for "
        "comments_count and video_view_count (mirrors the likes estimator "
        "semantics; label pass untouched)."
    ),
    deps=[AssetKey(["v_post_detail"])],
)
def v_post_baselines(duckdb: DuckDBResource) -> None:
    """Trailing per-post baselines for comments and video views.

    Mirrors the label-pass estimator exactly (N=20 trailing prior posts per
    creator, expanded to a 90-day lookback when fewer than 20 priors exist,
    min n=5, center=Q3, spread=IQR) but computed in the SERVING layer over
    ``v_post_detail`` — ``ig_post_labels`` is NOT touched.

    - Baseline key: ``COALESCE(creator_id, owner_username)`` (handle fallback
      so posts without a creator link still get baselines).
    - Strict priors: ``q.timestamp < p.timestamp`` — no future leak. Posts
      with NULL timestamps are excluded from windows entirely.
    - comments: window posts are priors with non-NULL ``comments_count``
      (0 stays in; absent is NULL and drops out).
    - Strict priors: ``q.timestamp < p.timestamp`` — no future leak. Posts
      with NULL timestamps are excluded from windows entirely. Timestamp
      ties are broken by ``post_id DESC`` so the N=20 window is
      deterministic (the label pass relies on stable-sort order instead).
    """
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE OR REPLACE VIEW v_post_baselines AS
            WITH posts AS (
                SELECT
                    post_id,
                    COALESCE(creator_id::VARCHAR, owner_username)
                        AS baseline_key,
                    timestamp,
                    comments_count,
                    video_view_count
                FROM v_post_detail
                WHERE COALESCE(creator_id::VARCHAR, owner_username)
                          IS NOT NULL
            ),
            comment_pairs AS (
                SELECT
                    p.post_id,
                    p.timestamp                                    AS post_ts,
                    q.comments_count                               AS val,
                    q.timestamp                                    AS prior_ts,
                    ROW_NUMBER() OVER (
                        PARTITION BY p.post_id
                        ORDER BY q.timestamp DESC, q.post_id DESC
                    )                                              AS recency,
                    COUNT(*) OVER (PARTITION BY p.post_id)         AS n_priors
                FROM posts p
                JOIN posts q
                  ON  q.baseline_key = p.baseline_key
                  AND q.timestamp    < p.timestamp
                  AND q.comments_count IS NOT NULL
                WHERE p.timestamp IS NOT NULL
            ),
            comment_windows AS (
                SELECT
                    post_id,
                    COUNT(*)                  AS comments_baseline_n,
                    -- min n=5 (_BASELINE_MIN_N): below it the baseline is NULL
                    CASE WHEN COUNT(*) >= 5
                         THEN quantile_cont(val, 0.25) END AS q1,
                    CASE WHEN COUNT(*) >= 5
                         THEN quantile_cont(val, 0.75) END AS q3
                FROM comment_pairs
                -- Estimator rule: >= 20 priors → the 20 most recent;
                -- < 20 priors → only priors within the 90-day lookback.
                WHERE CASE WHEN n_priors >= 20 THEN recency <= 20
                           ELSE prior_ts >= post_ts - INTERVAL 90 DAY END
                GROUP BY post_id
            ),
            view_pairs AS (
                SELECT
                    p.post_id,
                    p.timestamp                                    AS post_ts,
                    q.video_view_count                             AS val,
                    q.timestamp                                    AS prior_ts,
                    ROW_NUMBER() OVER (
                        PARTITION BY p.post_id
                        ORDER BY q.timestamp DESC, q.post_id DESC
                    )                                              AS recency,
                    COUNT(*) OVER (PARTITION BY p.post_id)         AS n_priors
                FROM posts p
                JOIN posts q
                  ON  q.baseline_key   = p.baseline_key
                  AND q.timestamp      < p.timestamp
                  AND q.video_view_count > 0
                WHERE p.timestamp IS NOT NULL
                  AND p.video_view_count > 0
                    -- image/carousel posts get NULL views_* columns
            ),
            view_windows AS (
                SELECT
                    post_id,
                    COUNT(*)                  AS views_baseline_n,
                    -- min n=5 (_BASELINE_MIN_N): below it the baseline is NULL
                    CASE WHEN COUNT(*) >= 5
                         THEN quantile_cont(val, 0.25) END AS q1,
                    CASE WHEN COUNT(*) >= 5
                         THEN quantile_cont(val, 0.75) END AS q3
                FROM view_pairs
                WHERE CASE WHEN n_priors >= 20 THEN recency <= 20
                           ELSE prior_ts >= post_ts - INTERVAL 90 DAY END
                GROUP BY post_id
            )
            SELECT
                p.post_id,
                cb.comments_baseline_n,
                cb.q3                    AS comments_baseline_q3,
                cb.q3 - cb.q1            AS comments_baseline_iqr,
                vb.views_baseline_n,
                vb.q3                    AS views_baseline_q3,
                vb.q3 - vb.q1            AS views_baseline_iqr
            FROM posts p
            LEFT JOIN comment_windows cb ON cb.post_id = p.post_id
            LEFT JOIN view_windows   vb ON vb.post_id = p.post_id
        """)


@asset(
    name="v_post_metrics",
    group_name="serving",
    description=(
        "Canonical per-post metrics: Tukey label + point-in-time baseline, "
        "comments/views z-scores, engagement_score, is_standout/is_hot/"
        "relative_performance, top-3-per-owner flag."
    ),
    deps=[AssetKey(["v_engagement_outliers"])],
)
def v_post_metrics(duckdb: DuckDBResource) -> None:
    """Single source for every per-post hot/standout/z-score/baseline field.

    Consumes ``v_engagement_outliers`` (likes z-score NOT re-derived) and
    ``ig_post_labels`` for the trailing Tukey baseline. Point-in-time
    contract: a post is judged against its OWN label-pass baseline
    (``baseline_q3``/``baseline_iqr``), never a creator all-time average.
    ``is_hot`` = standout AND ``likes_zscore >= 2`` (2σ+).
    ``is_top3_in_owner`` ranks standout posts per owner by ``likes_zscore``.
    Comments/views get their own serving-layer point-in-time baselines from
    ``v_post_baselines`` (same trailing semantics; ``views_zscore`` is NULL
    on image posts where ``video_view_count`` is NULL/0).
    ``engagement_score`` = 0.5*likes_zscore + 0.3*comments_zscore +
    0.2*views_zscore (NULL component z ⇒ 0 contribution — a missing baseline
    is not a mediocre post; NULL only when ALL three z-scores are NULL).
    No creator-avg column on post rows — creator averages live only on
    ``v_creator_metrics`` (activity, gate-free) / ``v_creator_profile``
    (rollup, gate-free) / ``v_creator_quality`` (quality, gated).
    """
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE OR REPLACE VIEW v_post_metrics AS
            WITH ranked AS (
                SELECT
                    eo.post_id, eo.owner_username, eo.creator_id, eo.channel,
                    eo.creator_name,
                    eo.likes_count, eo.comments_count, eo.video_view_count,
                    eo.timestamp, eo.shortcode, eo.caption,
                    eo.label, eo.method, eo.is_provisional,
                    eo.likes_zscore,
                    l.baseline_center AS baseline_q3,
                    l.baseline_spread AS baseline_iqr,
                    b.comments_baseline_n,
                    b.comments_baseline_q3,
                    b.comments_baseline_iqr,
                    CASE WHEN b.comments_baseline_iqr > 0
                          AND eo.comments_count IS NOT NULL
                         THEN ROUND(
                             (eo.comments_count - b.comments_baseline_q3)
                             / b.comments_baseline_iqr, 2)
                    END AS comments_zscore,
                    b.views_baseline_n,
                    b.views_baseline_q3,
                    b.views_baseline_iqr,
                    CASE WHEN b.views_baseline_iqr > 0
                          AND eo.video_view_count > 0
                         THEN ROUND(
                             (eo.video_view_count - b.views_baseline_q3)
                             / b.views_baseline_iqr, 2)
                    END AS views_zscore,
                    eo.sigma_tier,
                    CASE WHEN eo.label = 'standout' THEN 1 ELSE 0 END
                        AS is_standout,
                    CASE WHEN eo.label = 'standout'
                          AND eo.likes_zscore >= 2 THEN 1 ELSE 0 END
                        AS is_hot,
                    CASE WHEN eo.label = 'standout'
                          AND eo.likes_zscore >= 2 THEN 'hot'
                         WHEN eo.label = 'standout' THEN 'standout'
                    END AS relative_performance,
                    eo.likes_count / NULLIF(l.baseline_center, 0)
                        AS breakout_multiple,
                    ROW_NUMBER() OVER (
                        PARTITION BY eo.owner_username, eo.label
                        ORDER BY eo.likes_zscore DESC NULLS LAST,
                                 eo.likes_count DESC NULLS LAST
                    ) AS owner_rank
                FROM v_engagement_outliers eo
                LEFT JOIN ig_post_labels l ON eo.post_id = l.post_id
                LEFT JOIN v_post_baselines b ON eo.post_id = b.post_id
            )
            SELECT
                *,
                CASE WHEN likes_zscore IS NULL
                      AND comments_zscore IS NULL
                      AND views_zscore IS NULL
                     THEN NULL
                     ELSE ROUND(
                         0.5 * COALESCE(likes_zscore, 0)
                         + 0.3 * COALESCE(comments_zscore, 0)
                         + 0.2 * COALESCE(views_zscore, 0), 2)
                END AS engagement_score,
                CASE WHEN is_standout = 1 AND owner_rank <= 3
                     THEN 1 ELSE 0 END AS is_top3_in_owner
            FROM ranked
        """)


@asset(
    name="v_creator_metrics",
    group_name="serving",
    description="Gate-free per-creator activity metrics: counts, true avg, max.",
    deps=[AssetKey(["v_post_metrics"])],
)
def v_creator_metrics(duckdb: DuckDBResource) -> None:
    """Per-creator ACTIVITY metrics — deliberately gate-free.

    Unlike ``v_creator_quality`` (enriched_posts >= 3, quality rankings),
    every curated creator appears here with real counts and a true mean
    ``avg_likes`` over all their scraped posts. Consumers: /api/creators.
    """
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE OR REPLACE VIEW v_creator_metrics AS
            SELECT
                creator_id,
                COUNT(*)            AS total_posts,
                SUM(is_standout)    AS standout_count,
                SUM(is_hot)         AS hot_count,
                AVG(likes_count)    AS avg_likes,
                MAX(likes_count)    AS max_likes
            FROM v_post_metrics
            WHERE creator_id IS NOT NULL
            GROUP BY creator_id
        """)


@asset(
    name="v_profile_metrics",
    group_name="serving",
    description="Per-owner_username activity counts: post/standout/hot.",
    deps=[AssetKey(["v_post_metrics"])],
)
def v_profile_metrics(duckdb: DuckDBResource) -> None:
    """Per-PROFILE activity counts for creator-detail per-profile post_count."""
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE OR REPLACE VIEW v_profile_metrics AS
            SELECT
                owner_username,
                COUNT(*)         AS post_count,
                SUM(is_standout) AS standout_count,
                SUM(is_hot)      AS hot_count
            FROM v_post_metrics
            WHERE owner_username IS NOT NULL
            GROUP BY owner_username
        """)


@asset(
    name="v_creator_profile",
    group_name="serving",
    description=(
        "One row per creator: activity counts, true avg, momentum, dominant "
        "domain, avg engagement score. Canonical creators-page rollup."
    ),
    deps=[AssetKey(["v_post_metrics"]), AssetKey(["v_post_detail"])],
)
def v_creator_profile(duckdb: DuckDBResource) -> None:
    """Per-creator canonical rollup for the creators page / rising card.

    Grain: one row per ``creator_id``. Combines the gate-free activity
    rollup (same semantics as ``v_creator_metrics`` plus
    ``avg_engagement_score``), the dominant ``gold_domain`` (most frequent
    over the creator's enriched posts; ties broken alphabetically), and the
    momentum windows shared with ``v_rising_creators`` (defined here, gated
    there — single definition, see ``is_rising``).
    Momentum: 28-day recent avg likes vs 84→28-day baseline avg likes;
    ``is_rising`` applies the exact ``v_rising_creators`` gates.
    """
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE OR REPLACE VIEW v_creator_profile AS
            WITH activity AS (
                SELECT
                    creator_id,
                    MAX(creator_name)        AS creator_name,
                    COUNT(*)                 AS total_posts,
                    SUM(is_standout)         AS standout_count,
                    SUM(is_hot)              AS hot_count,
                    AVG(likes_count)         AS avg_likes,
                    MAX(likes_count)         AS max_likes,
                    AVG(engagement_score)    AS avg_engagement_score
                FROM v_post_metrics
                WHERE creator_id IS NOT NULL
                GROUP BY creator_id
            ),
            domains AS (
                SELECT
                    creator_id,
                    gold_domain,
                    COUNT(*) AS domain_posts,
                    ROW_NUMBER() OVER (
                        PARTITION BY creator_id
                        ORDER BY COUNT(*) DESC, gold_domain ASC
                    ) AS rn
                FROM v_post_detail
                WHERE creator_id IS NOT NULL
                  AND gold_domain IS NOT NULL
                GROUP BY creator_id, gold_domain
            ),
            windows AS (
                SELECT
                    creator_id,
                    AVG(likes_count) FILTER
                        (WHERE timestamp >= CURRENT_DATE - INTERVAL '28' DAY)
                        AS recent_avg,
                    COUNT(likes_count) FILTER
                        (WHERE timestamp >= CURRENT_DATE - INTERVAL '28' DAY)
                        AS recent_posts,
                    AVG(likes_count) FILTER (WHERE timestamp >= CURRENT_DATE - INTERVAL '84' DAY
                                             AND  timestamp <  CURRENT_DATE - INTERVAL '28' DAY)
                        AS baseline_avg,
                    COUNT(likes_count) FILTER (WHERE timestamp >= CURRENT_DATE - INTERVAL '84' DAY
                                               AND  timestamp <  CURRENT_DATE - INTERVAL '28' DAY)
                        AS baseline_posts
                FROM v_post_detail
                WHERE creator_id IS NOT NULL
                  AND timestamp IS NOT NULL
                GROUP BY creator_id
            )
            SELECT
                a.creator_id,
                a.creator_name,
                a.total_posts,
                a.standout_count,
                a.hot_count,
                a.avg_likes,
                a.max_likes,
                a.avg_engagement_score,
                d.gold_domain             AS dominant_domain,
                d.domain_posts            AS dominant_domain_posts,
                w.recent_avg,
                w.recent_posts,
                w.baseline_avg,
                w.baseline_posts,
                w.recent_avg / NULLIF(w.baseline_avg, 0) AS momentum_ratio,
                CASE WHEN COALESCE(w.recent_posts, 0) >= 3
                      AND COALESCE(w.baseline_posts, 0) >= 3
                      AND w.baseline_avg > 0
                      AND w.recent_avg >= 5.0
                      AND w.recent_avg / w.baseline_avg >= 1.25
                     THEN TRUE ELSE FALSE END AS is_rising
            FROM activity a
            LEFT JOIN domains d
                ON d.creator_id = a.creator_id AND d.rn = 1
            LEFT JOIN windows w ON w.creator_id = a.creator_id
        """)


@asset(
    name="v_creator_topics",
    group_name="serving",
    description=(
        "Long-form per-creator topics: top-5 by post count and top-5 by "
        "baseline-normalized weighted performance."
    ),
    deps=[AssetKey(["gold_post_enrichment"])],
)
def v_creator_topics(duckdb: DuckDBResource) -> None:
    """Top topics per creator for the creators-page topic chips.

    Grain: one row per ``(creator_id, gold_topic)`` over ENRICHED posts.
    Reads the wide per-post mart ``gold_post_enrichment`` — the correct
    upstream for topic + engagement: it already composes the canonical
    ``v_post_metrics`` (``engagement_score``, ``creator_id``) and
    ``v_post_detail`` (``gold_topic``) on the ``(post_id, platform)`` key,
    so no metric is re-derived here (WATCHDOG metrics-centralization).
    ``perf_score`` = mean of member posts' baseline-normalized weighted
    ``engagement_score`` (posts without a score drop out of the mean).
    ``perf_rank`` ranks topics by ``perf_score`` DESC within a creator;
    ``count_rank`` by ``post_count`` DESC (RANK — ties share a rank). Rows
    are kept when they are top-5 by EITHER rank, so the UI can slice by
    ``perf_rank <= 5`` / ``count_rank <= 5``.
    """
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE OR REPLACE VIEW v_creator_topics AS
            WITH topics AS (
                SELECT
                    gpe.creator_id,
                    gpe.gold_topic               AS topic,
                    COUNT(*)                     AS post_count,
                    AVG(gpe.engagement_score)    AS perf_score
                FROM gold_post_enrichment gpe
                WHERE gpe.creator_id IS NOT NULL
                  AND gpe.gold_topic IS NOT NULL
                GROUP BY gpe.creator_id, gpe.gold_topic
            ),
            ranked AS (
                SELECT
                    *,
                    RANK() OVER (PARTITION BY creator_id
                                 ORDER BY post_count DESC) AS count_rank,
                    RANK() OVER (PARTITION BY creator_id
                                 ORDER BY perf_score DESC NULLS LAST)
                        AS perf_rank
                FROM topics
            )
            SELECT
                creator_id,
                topic,
                post_count,
                perf_score,
                perf_rank,
                count_rank
            FROM ranked
            WHERE count_rank <= 5 OR perf_rank <= 5
        """)


ASSETS: list = [
    v_post_baselines,
    v_post_metrics,
    v_creator_metrics,
    v_profile_metrics,
    v_creator_profile,
    v_creator_topics,
]
