"""Cross-domain shared dimensions and views for the datalake.

``dim_profile`` tracks Instagram profiles with SCD2 (slowly changing dimension)
via ``effective_from``/``effective_to``/``is_current``.

``dim_date`` is a generated date dimension table for consistent time-based
aggregations across all serving views.

``v_post_detail`` is the foundational flat view joining silver posts, gold
analyses, profiles, and dates. All seven downstream analytics views read from it.
"""

from __future__ import annotations

from datetime import datetime, timezone

from dagster import AssetKey, asset
from dagster_duckdb import DuckDBResource

from datalake.defs.common.resources import SQLiteResource
from datalake.defs.common.schemas import duckdb_ddl

# ── Dimensions ──────────────────────────────────────────────────────────────


@asset(
    name="dim_profile",
    group_name="serving",
    description="SCD2 profile dimension tracking owner attributes over time.",
    deps=[AssetKey("ig_posts_slv")],
)
def profile_dimension(duckdb: DuckDBResource, ops: SQLiteResource) -> None:
    """Upsert profile dimension with SCD2 tracking.

    Reads distinct owner profiles from ``silver_ig_posts`` and maintains
    ``effective_from``/``effective_to``/``is_current`` in DuckDB. ``creator_id``
    and ``creator_name`` are linked from the ``profiles``/``creators`` tables in
    ops.sqlite so every serving view can expose the owning creator.
    """
    from datalake.defs.instagram.creators import creator_map

    db = duckdb
    with db.get_connection() as conn:
        conn.execute(duckdb_ddl("dim_profile"))

        # Migration: existing DBs predate profile_pic_path and the creator
        # columns. DuckDB ALTER has no IF NOT EXISTS, so tolerate the
        # duplicate-column error.
        for col, typ in (
            ("profile_pic_path", "TEXT"),
            ("creator_id", "INTEGER"),
            ("creator_name", "TEXT"),
        ):
            try:
                conn.execute(f"ALTER TABLE dim_profile ADD COLUMN {col} {typ}")
            except Exception:
                pass  # column already exists

        # Get distinct profiles from silver_ig_posts
        profiles = conn.execute("""
            SELECT DISTINCT owner_id, owner_username
            FROM silver_ig_posts
            WHERE owner_id IS NOT NULL
        """).fetchall()

        # Creator link: {handle: {creator_id, creator_name}} from ops.
        handle_map = creator_map(ops)

        if not profiles:
            return

        # Determine next profile_key
        max_key = conn.execute("SELECT COALESCE(MAX(profile_key), 0) FROM dim_profile").fetchone()[
            0
        ]

        now_ts = datetime.now(timezone.utc).isoformat()

        for owner_id, owner_username in profiles:
            creator = handle_map.get(owner_username, {})

            # Check existing current row
            existing = conn.execute(
                """
                SELECT profile_key, owner_username
                FROM dim_profile
                WHERE owner_id = ? AND is_current = TRUE
            """,
                [owner_id],
            ).fetchone()

            if existing:
                existing_key, existing_username = existing
                if existing_username == owner_username:
                    # No identity change — creator link refreshed below.
                    continue
                # Close the old row
                conn.execute(
                    """
                    UPDATE dim_profile
                    SET effective_to = ?, is_current = FALSE
                    WHERE profile_key = ?
                """,
                    [now_ts, existing_key],
                )

            # Insert new row
            max_key += 1
            conn.execute(
                """
                INSERT INTO dim_profile
                    (profile_key, owner_id, owner_username, channel,
                     effective_from, effective_to, is_current,
                     creator_id, creator_name)
                VALUES (?, ?, ?, 'instagram', ?, NULL, TRUE, ?, ?)
            """,
                [
                    max_key,
                    owner_id,
                    owner_username,
                    now_ts,
                    creator.get("creator_id"),
                    creator.get("creator_name"),
                ],
            )

        # Refresh the creator link on current rows. This is a mutable
        # relationship (a profile's owner), not a slowly-changing attribute,
        # so it updates in place rather than versioning a new SCD2 row.
        for owner_username, creator in handle_map.items():
            conn.execute(
                """
                UPDATE dim_profile
                SET creator_id = ?, creator_name = ?
                WHERE owner_username = ? AND is_current = TRUE
            """,
                [creator["creator_id"], creator["creator_name"], owner_username],
            )


@asset(
    name="dim_date",
    group_name="serving",
    description="Generated date dimension: 1 year back from today, with fiscal year (Jul–Jun).",
)
def dim_date(duckdb: DuckDBResource) -> None:
    """Generate a standard date dimension table.

    One row per day from (CURRENT_DATE - 1 year) through CURRENT_DATE.
    Financial year runs July–June (e.g. FY2026 = Jul 2025 – Jun 2026).
    """
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE OR REPLACE TABLE dim_date AS
            SELECT
                date_col::DATE                                        AS date,
                EXTRACT(YEAR FROM date_col)                           AS year,
                EXTRACT(QUARTER FROM date_col)                        AS quarter,
                EXTRACT(MONTH FROM date_col)                          AS month_number,
                MONTHNAME(date_col)                                   AS month_name,
                EXTRACT(WEEK FROM date_col)                           AS week_number,
                EXTRACT(DAY FROM date_col)                            AS day_number,
                DAYNAME(date_col)                                     AS day_of_week,
                CASE WHEN DAYOFWEEK(date_col) IN (0, 6)
                     THEN TRUE ELSE FALSE END                         AS is_weekend,
                CASE WHEN EXTRACT(MONTH FROM date_col) >= 7
                     THEN EXTRACT(YEAR FROM date_col)
                     ELSE EXTRACT(YEAR FROM date_col) - 1
                END                                                   AS financial_year
            FROM generate_series(
                CURRENT_DATE - INTERVAL 1 YEAR,
                CURRENT_DATE,
                INTERVAL 1 DAY
            ) AS t(date_col)
        """)


# ── Foundational view ───────────────────────────────────────────────────────


_GOLD_KEY = AssetKey(["gold_analyses"])


@asset(
    name="v_post_detail",
    group_name="serving",
    description="Foundational flat view: silver posts + gold analyses + profile + date.",
    deps=[_GOLD_KEY, AssetKey(["dim_profile"]), AssetKey(["dim_date"])],
)
def v_post_detail(duckdb: DuckDBResource) -> None:
    """Create the foundational analytics view.

    Extracts JSON fields from gold_analyses.result_json into typed columns.
    LEFT JOINs are used throughout — posts without enrichment still appear,
    and posts without profiles still appear.
    """
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE OR REPLACE VIEW v_post_detail AS
            SELECT
                sp.post_id,
                sp.shortcode,
                sp.url,
                sp.caption,
                sp.owner_id,
                sp.owner_username,
                sp.likes_count,
                sp.comments_count,
                sp.video_play_count,
                sp.video_view_count,
                sp.timestamp,
                sp.hashtags,
                sp.meta_data,
                sp.has_engagement_bait,
                sp.media_files,
                sp.media_count,
                sp.source_dataset,
                sp.processed_on,

                -- Gold enrichment fields (extracted from JSON)
                ga.result_json,
                ga.analysed_at                                 AS gold_analysed_at,
                ga.prompt_hash,
                ga.result_json->>'$.admiralty'                 AS admiralty,
                ga.result_json->>'$.domain'                    AS gold_domain,
                ga.result_json->>'$.subdomain'                 AS gold_subdomain,
                ga.result_json->>'$.topic'                     AS gold_topic,
                ga.result_json->>'$.subtopic'                  AS gold_subtopic,
                ga.result_json->>'$.content_type'              AS content_type,
                ga.result_json->>'$.style'                     AS style,
                ga.result_json->>'$.format'                    AS format,
                (ga.result_json->>'$.is_educational')::BOOLEAN AS is_educational,
                (ga.result_json->>'$.is_actionable')::BOOLEAN  AS is_actionable,

                -- Profile dimension (current row only)
                dp.profile_key,
                dp.channel,
                dp.effective_from,
                dp.effective_to,
                dp.is_current,
                dp.creator_id,
                dp.creator_name,

                -- Date dimension
                dd.date                                        AS dim_date,
                dd.year,
                dd.quarter,
                dd.month_number,
                dd.month_name,
                dd.week_number,
                dd.day_number,
                dd.day_of_week,
                dd.is_weekend,
                dd.financial_year

            FROM silver_ig_posts sp
            LEFT JOIN gold_analyses ga
                ON sp.post_id = ga.post_id AND ga.domain = 'instagram'
            LEFT JOIN dim_profile dp
                ON sp.owner_id = dp.owner_id AND dp.is_current = TRUE
            LEFT JOIN dim_date dd
                ON DATE(sp.timestamp) = dd.date
        """)


# ── Signal view ─────────────────────────────────────────────────────────────


@asset(
    name="v_signal",
    group_name="serving",
    description="High-value posts: educational content or A/B-tier admiralty only.",
    deps=[AssetKey(["v_post_detail"])],
)
def v_signal(duckdb: DuckDBResource) -> None:
    """Filter to high-signal posts — educational or authoritative sources."""
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE OR REPLACE VIEW v_signal AS
            SELECT *
            FROM v_post_detail
            WHERE is_educational = TRUE
               OR admiralty LIKE 'A%'
               OR admiralty LIKE 'B%'
        """)


# ── Analytics views ─────────────────────────────────────────────────────────


@asset(
    name="v_quality_trend",
    group_name="serving",
    description="Weekly aggregate: admiralty tiers, educational rate, avg engagement.",
    deps=[AssetKey(["v_post_detail"])],
)
def v_quality_trend(duckdb: DuckDBResource) -> None:
    """Weekly quality trends — tier distribution, educational percentage, engagement."""
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE OR REPLACE VIEW v_quality_trend AS
            SELECT
                year,
                week_number,
                COUNT(*)                                                 AS post_count,
                COUNT(CASE WHEN result_json IS NOT NULL THEN 1 END)      AS enriched_count,
                AVG(CASE WHEN is_educational THEN 1.0 ELSE 0.0 END)      AS educational_pct,
                AVG(likes_count)                                         AS avg_likes,
                AVG(comments_count)                                      AS avg_comments,
                AVG(video_view_count)                                    AS avg_video_views,
                SUM(CASE WHEN admiralty LIKE 'A%' THEN 1 ELSE 0 END)     AS tier_a,
                SUM(CASE WHEN admiralty LIKE 'B%' THEN 1 ELSE 0 END)     AS tier_b,
                SUM(CASE WHEN admiralty LIKE 'C%' THEN 1 ELSE 0 END)     AS tier_c,
                SUM(CASE WHEN admiralty IS NULL THEN 1 ELSE 0 END)       AS tier_unknown
            FROM v_post_detail
            WHERE result_json IS NOT NULL
            GROUP BY year, week_number
            ORDER BY year, week_number
        """)


@asset(
    name="v_creator_quality",
    group_name="serving",
    description="Absolute creator quality: admiralty, rates, and engagement.",
    deps=[AssetKey(["v_post_detail"])],
)
def v_creator_quality(duckdb: DuckDBResource) -> None:
    """Per-creator quality and engagement metrics pooled across all profiles."""
    with duckdb.get_connection() as conn:
        conn.execute("DROP VIEW IF EXISTS v_profile_quality")
        conn.execute("""
            CREATE OR REPLACE VIEW v_creator_quality AS
            WITH base AS (
                SELECT
                    creator_id,
                    MAX(creator_name) AS creator_name,
                    COUNT(*) AS total_posts,
                    COUNT(result_json) AS enriched_posts,
                    AVG(CASE WHEN admiralty LIKE 'A%' THEN 3.0
                             WHEN admiralty LIKE 'B%' THEN 2.0
                             WHEN admiralty LIKE 'C%' THEN 1.0
                             WHEN admiralty LIKE 'D%' THEN 0.0 END) AS admiralty_score,
                    AVG(CASE WHEN is_educational THEN 1.0
                             WHEN NOT is_educational THEN 0.0 END) AS educational_rate,
                    AVG(CASE WHEN is_actionable THEN 1.0
                             WHEN NOT is_actionable THEN 0.0 END) AS actionable_rate,
                    AVG(likes_count) AS avg_likes,
                    MAX(likes_count) AS max_likes
                FROM v_post_detail
                WHERE creator_id IS NOT NULL
                GROUP BY creator_id
            )
            SELECT b.*,
                   ROUND(0.4 * PERCENT_RANK() OVER (ORDER BY COALESCE(b.admiralty_score, 0))
                       + 0.4 * PERCENT_RANK() OVER (ORDER BY COALESCE(LN(1 + b.avg_likes), 0))
                       + 0.2 * PERCENT_RANK() OVER (
                           ORDER BY b.enriched_posts::DOUBLE / NULLIF(b.total_posts, 0))
                       , 4) AS composite_score
            FROM base b
            WHERE b.enriched_posts >= 3
        """)


@asset(
    name="v_rising_creators",
    group_name="serving",
    description="Rising creators: momentum of recent vs baseline engagement.",
    deps=[AssetKey(["v_creator_profile"])],
)
def v_rising_creators(duckdb: DuckDBResource) -> None:
    """Creators whose recent engagement outpaces their baseline by >= 1.25x.

    A gated projection of ``v_creator_profile`` — the momentum windows and
    gates are defined ONCE there (28d recent vs 84→28d baseline avg likes,
    >=3 posts per window, baseline_avg > 0, recent_avg >= 5.0, ratio >= 1.25)
    so creator-profile cards and this feed can never drift apart. Output
    contract (columns + grain) unchanged.
    """
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE OR REPLACE VIEW v_rising_creators AS
            SELECT
                creator_id,
                creator_name,
                recent_avg,
                recent_posts,
                baseline_avg,
                baseline_posts,
                momentum_ratio
            FROM v_creator_profile
            WHERE is_rising
        """)


@asset(
    name="v_domain_coverage",
    group_name="serving",
    description="Domain x admiralty heatmap: post counts by category and tier.",
    deps=[AssetKey(["v_post_detail"])],
)
def v_domain_coverage(duckdb: DuckDBResource) -> None:
    """Long-format heatmap of domain vs admiralty tier coverage."""
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE OR REPLACE VIEW v_domain_coverage AS
            SELECT
                gold_domain,
                admiralty,
                COUNT(*) AS post_count
            FROM v_post_detail
            WHERE gold_domain IS NOT NULL
              AND admiralty IS NOT NULL
              AND result_json IS NOT NULL
            GROUP BY gold_domain, admiralty
            ORDER BY gold_domain, admiralty
        """)


# ── Engagement outlier views ────────────────────────────────────────────────


@asset(
    name="v_engagement_outliers",
    group_name="serving",
    description="Label-backed outlier tiers from ig_post_labels (no lifetime z-score).",
    deps=[AssetKey(["v_post_detail"]), AssetKey(["ig_post_labels"])],
)
def v_engagement_outliers(duckdb: DuckDBResource) -> None:
    """Per-post outlier tiers from ``ig_post_labels`` — no future-leak.

    The lifetime z-score computation was retired (US-D2): a post's tier now
    comes from its materialized Tukey-fence label. ``likes_zscore`` is the
    post's likes against its own trailing baseline (center/spread from the
    label pass), so outstanding posts are ranked without leaking future data.
    Posts without a label row (pending / not yet judged) fall to 'normal'.
    """
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE OR REPLACE VIEW v_engagement_outliers AS
            WITH labeled AS (
                SELECT
                    p.*,
                    l.label,
                    l.method,
                    l.is_provisional,
                    CASE WHEN l.baseline_spread > 0
                         THEN ROUND(
                             (p.likes_count - l.baseline_center)
                             / l.baseline_spread,
                             2
                         )
                    END AS likes_zscore
                FROM v_post_detail p
                LEFT JOIN ig_post_labels l ON p.post_id = l.post_id
            )
            SELECT
                *,
                CASE
                    WHEN label = 'standout' AND likes_zscore >= 3 THEN '3σ+'
                    WHEN label = 'standout' AND likes_zscore >= 2 THEN '2σ'
                    WHEN label = 'standout' THEN '1σ'
                    WHEN likes_zscore <= -1 THEN '-1σ'
                    ELSE 'normal'
                END AS sigma_tier
            FROM labeled
        """)


@asset(
    name="v_outlier_posts",
    group_name="serving",
    description="Posts 1σ+ outliers per their ``ig_post_labels`` tier.",
    deps=[AssetKey(["v_post_detail"]), AssetKey(["v_engagement_outliers"])],
)
def v_outlier_posts(duckdb: DuckDBResource) -> None:
    """Filter to outlier posts only — label-backed tier of 1σ or more."""
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE OR REPLACE VIEW v_outlier_posts AS
            SELECT *
            FROM v_engagement_outliers
            WHERE sigma_tier IN ('1σ', '2σ', '3σ+')
        """)


@asset(
    name="v_creator_outlier_rate",
    group_name="serving",
    description="Which creators produce the most label-backed outliers.",
    deps=[AssetKey(["v_post_detail"]), AssetKey(["v_engagement_outliers"])],
)
def v_creator_outlier_rate(duckdb: DuckDBResource) -> None:
    """Per-creator outlier stats from ``ig_post_labels`` — rate, avg z, max z."""
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE OR REPLACE VIEW v_creator_outlier_rate AS
            SELECT
                COALESCE(MAX(owner_id) FILTER (WHERE owner_id IS NOT NULL), 'unknown') AS owner_id,
                owner_username,
                MAX(creator_id)                                                       AS creator_id,
                COUNT(*)                                                       AS total_posts,
                SUM(CASE WHEN sigma_tier IN ('1σ', '2σ', '3σ+')
                         THEN 1 ELSE 0 END)                                    AS outlier_posts,
                AVG(CASE WHEN sigma_tier IN ('1σ', '2σ', '3σ+')
                         THEN 1.0 ELSE 0.0 END)                                AS outlier_rate,
                AVG(likes_zscore)                                              AS avg_zscore,
                MAX(likes_zscore)                                              AS max_zscore
            FROM v_engagement_outliers
            GROUP BY owner_username
            ORDER BY outlier_rate DESC
        """)

# ── Canonical metric views (metrics centralization) ─────────────────────────



# ── Point-in-time baselines (serving layer, comments + video views) ────────


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
    - views: meaningful only where the post's own ``video_view_count`` is
      non-NULL and > 0; window posts are priors with ``video_view_count > 0``.
      Image/carousel posts get NULL views_* columns.
    - Below min n=5 the baseline is NULL → z-score NULL (not 0).
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
                        ORDER BY q.timestamp DESC
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
                        ORDER BY q.timestamp DESC
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
    deps=[AssetKey(["v_post_metrics"]), AssetKey(["v_post_detail"])],
)
def v_creator_topics(duckdb: DuckDBResource) -> None:
    """Top topics per creator for the creators-page topic chips.

    Grain: one row per ``(creator_id, gold_topic)`` over ENRICHED posts.
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
                    pm.creator_id,
                    pd.gold_topic                AS topic,
                    COUNT(*)                     AS post_count,
                    AVG(pm.engagement_score)     AS perf_score
                FROM v_post_metrics pm
                JOIN v_post_detail pd ON pd.post_id = pm.post_id
                WHERE pm.creator_id IS NOT NULL
                  AND pd.gold_topic IS NOT NULL
                GROUP BY pm.creator_id, pd.gold_topic
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


@asset(
    name="v_overview",
    group_name="serving",
    description="Single-row overview: totals, enrichment pct, avg admiralty, signal count.",
    deps=[
        AssetKey(["v_post_detail"]),
        AssetKey(["v_creator_quality"]),
        AssetKey(["v_signal"]),
    ],
)
def v_overview(duckdb: DuckDBResource) -> None:
    """Exactly one row — moves /api/overview aggregation into the warehouse."""
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE OR REPLACE VIEW v_overview AS
            SELECT
                (SELECT COUNT(*) FROM silver_ig_posts) AS total_posts,
                (SELECT COUNT(*) FROM gold_analyses
                 WHERE domain = 'instagram')           AS total_enriched,
                (SELECT COUNT(DISTINCT owner_username)
                 FROM silver_ig_posts)                 AS total_profiles,
                ROUND(
                    (SELECT COUNT(*) FROM gold_analyses WHERE domain = 'instagram')
                    / NULLIF((SELECT COUNT(*) FROM silver_ig_posts), 0) * 100,
                    1
                )                                      AS enrichment_pct,
                (SELECT COALESCE(ROUND(AVG(admiralty_score), 2), 0)
                 FROM v_creator_quality
                 WHERE enriched_posts > 0)             AS avg_admiralty_score,
                (SELECT COUNT(*) FROM v_signal)        AS high_signal_count
        """)


@asset(
    name="v_standout_calendar",
    group_name="serving",
    description="Standout posts per day-of-month for the weekly-summary chart.",
    deps=[AssetKey(["v_post_metrics"])],
)
def v_standout_calendar(duckdb: DuckDBResource) -> None:
    """Day-of-month standout counts — moves the /api/weekly-summary GROUP BY
    out of the server."""
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE OR REPLACE VIEW v_standout_calendar AS
            SELECT
                EXTRACT(DAY FROM timestamp) AS day_of_month,
                SUM(is_standout)            AS standout_count
            FROM v_post_metrics
            WHERE is_standout = 1
            GROUP BY day_of_month
        """)


@asset(
    name="v_recent_hot_posts",
    group_name="serving",
    description=(
        "Recent (28-day) hot posts — 2σ+ standouts from the last 28 days, "
        "top-3 per owner. The Overview 'Recent Hot Posts' feed."
    ),
    deps=[AssetKey(["v_post_metrics"])],
)
def v_recent_hot_posts(duckdb: DuckDBResource) -> None:
    """Recency-weighted view of the ``hot`` (2σ+) metric for the Overview card.

    Distinct from the all-time 2σ 'hot' counts (``v_creator_metrics.hot_count``
    / a creator's relative_performance): restricts to posts published in the
    last 28 days so an old breakout does not dominate a 'recent' feed.
    Ranking (top-3 per owner, by ``likes_zscore``) is computed here so the
    dashboard stays a thin projector. Recency is evaluated at query time
    (views are live), so the window is always 'as of now'.
    """
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE OR REPLACE VIEW v_recent_hot_posts AS
            WITH recent AS (
                SELECT *
                FROM v_post_metrics
                WHERE is_hot = 1
                  AND timestamp >= CURRENT_DATE - INTERVAL '28' DAY
            ),
            ranked AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY owner_username
                        ORDER BY likes_zscore DESC NULLS LAST,
                                 likes_count DESC NULLS LAST
                    ) AS recent_rank
                FROM recent
            )
            SELECT * FROM ranked WHERE recent_rank <= 3
        """)


# ── Exported for definitions.py ─────────────────────────────────────────────

assets: list = [
    profile_dimension,
    dim_date,
    v_post_detail,
    v_post_baselines,
    v_signal,
    v_quality_trend,
    v_creator_quality,
    v_rising_creators,
    v_domain_coverage,
    v_engagement_outliers,
    v_outlier_posts,
    v_creator_outlier_rate,
    v_post_metrics,
    v_creator_metrics,
    v_creator_profile,
    v_creator_topics,
    v_profile_metrics,
    v_overview,
    v_standout_calendar,
    v_recent_hot_posts,
]
