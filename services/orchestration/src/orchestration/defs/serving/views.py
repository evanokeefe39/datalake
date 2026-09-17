"""Consumer views — the flat, filterable surfaces the dashboard reads.

v_post_detail is foundational: the remaining views read from it rather
than re-joining silver and gold themselves."""


from dagster import AssetKey, asset
from dagster_duckdb import DuckDBResource

_CLASSIFICATION_KEY = AssetKey(["silver_content_classification"])


@asset(
    name="v_post_detail",
    group_name="serving",
    description="Foundational flat view: silver posts + content classification + profile + date.",
    deps=[_CLASSIFICATION_KEY, AssetKey(["dim_profile"]), AssetKey(["dim_date"])],
)
def v_post_detail(duckdb: DuckDBResource) -> None:
    """Create the foundational analytics view.

    Reads the typed columns from ``silver_content_classification`` (keyed
    ``(post_id, platform)``) — no JSON extraction. LEFT JOINs are used
    throughout — posts without enrichment still appear, and posts without
    profiles still appear.
    """
    with duckdb.get_connection() as conn:
        # Clean cutover (US-ESA-2): ``analytics_views`` was retired with the
        # serving refactor to per-view assets, but it still exists in state
        # DBs created before the refactor — still reading ``gold_analyses``.
        # It is not in the catalog (DUCKDB_VIEWS) and nothing selects it;
        # drop it so no serving view can keep referencing the retired table.
        conn.execute("DROP VIEW IF EXISTS analytics_views")
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

                -- Content classification fields (typed silver columns).
                -- ``result_json`` is the verbatim bronze payload, byte-identical
                -- to what ``gold_analyses`` served.
                scc.result_json,
                scc.analysed_at                                AS gold_analysed_at,
                scc.prompt_hash,
                scc.admiralty                                  AS admiralty,
                scc.domain                                     AS gold_domain,
                scc.subdomain                                  AS gold_subdomain,
                scc.topic                                      AS gold_topic,
                scc.subtopic                                   AS gold_subtopic,
                scc.content_type                               AS content_type,
                scc.style                                      AS style,
                scc.format                                     AS format,
                scc.is_educational                             AS is_educational,
                scc.is_actionable                              AS is_actionable,

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
            LEFT JOIN silver_content_classification scc
                ON sp.post_id = scc.post_id AND scc.platform = 'instagram'
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
                    WHEN likes_zscore <= -3 THEN '-3σ'
                    WHEN likes_zscore <= -2 THEN '-2σ'
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



@asset(
    name="v_underperformer_posts",
    group_name="serving",
    description=(
        "Posts that UNDERPERFORM their creator baseline (negative sigma tiers), "
        "with enrichment attributes."
    ),
    deps=[AssetKey(["v_post_detail"]), AssetKey(["v_engagement_outliers"])],
)
def v_underperformer_posts(duckdb: DuckDBResource) -> None:
    """First-class underperformer surface — the inverse of ``v_outlier_posts``.

    Mirrors the positive surface: one row per post whose ``ig_post_labels``
    tier is negative (``-1σ``/``-2σ``/``-3σ``), joined with the enrichment
    and content attributes needed to study WHAT underperforms (topic,
    content_type, format, admiralty). Additive — no positive semantics
    touched.
    """
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE OR REPLACE VIEW v_underperformer_posts AS
            SELECT
                post_id,
                owner_id,
                owner_username,
                creator_id,
                timestamp,
                likes_count,
                comments_count,
                video_view_count,
                label,
                method,
                is_provisional,
                likes_zscore,
                sigma_tier,
                gold_topic,
                gold_subtopic,
                gold_domain,
                gold_subdomain,
                content_type,
                format,
                style,
                admiralty,
                is_educational,
                is_actionable,
                result_json
            FROM v_engagement_outliers
            WHERE sigma_tier IN ('-1σ', '-2σ', '-3σ')
        """)


@asset(
    name="v_creator_underperformer_rate",
    group_name="serving",
    description="Which creators produce the most label-backed underperformers.",
    deps=[AssetKey(["v_post_detail"]), AssetKey(["v_engagement_outliers"])],
)
def v_creator_underperformer_rate(duckdb: DuckDBResource) -> None:
    """Per-creator underperformer stats — mirror of ``v_creator_outlier_rate``."""
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE OR REPLACE VIEW v_creator_underperformer_rate AS
            SELECT
                COALESCE(MAX(owner_id) FILTER (WHERE owner_id IS NOT NULL), 'unknown') AS owner_id,
                owner_username,
                MAX(creator_id)                                             AS creator_id,
                COUNT(*)                                                    AS total_posts,
                SUM(CASE WHEN sigma_tier IN ('-1σ', '-2σ', '-3σ')
                         THEN 1 ELSE 0 END)                                 AS underperformer_posts,
                AVG(CASE WHEN sigma_tier IN ('-1σ', '-2σ', '-3σ')
                         THEN 1.0 ELSE 0.0 END)                             AS underperformer_rate,
                AVG(likes_zscore)                                           AS avg_zscore,
                MIN(likes_zscore)                                              AS min_zscore
            FROM v_engagement_outliers
            GROUP BY owner_username
            ORDER BY underperformer_rate DESC
        """)


@asset(
    name="v_post_follower_context",
    group_name="serving",
    description=(
        "Per-post owner follower level at post time (nearest at-or-after "
        "observation), with growth-tier bucketing."
    ),
    deps=[
        AssetKey(["v_post_detail"]),
        AssetKey(["silver_ig_profiles"]),
    ],
)
def v_post_follower_context(duckdb: DuckDBResource) -> None:
    """Map each silver post to its owner's follower level AT POST TIME.

    Attribution picks the owner's ``silver_ig_profile_observations`` row
    nearest at-or-after the post timestamp — preferring an observation from
    the post's own ``source_dataset`` when one exists, else the nearest by
    ``observed_at``. ``owner_id`` is the join key, with an
    ``owner_username`` fallback only for posts that lack an ``owner_id``.

    CAVEAT (honesty): the backfill yields mostly ONE observation per owner,
    so the "at-post-time" level is really just the nearest observation we
    have — it may postdate the post by a long margin and does NOT imply the
    follower count was ever at that level when the post was published. No
    growth is fabricated: posts with no observation at all carry NULL
    follower context. Growth-over-time analysis becomes sound only as
    future scrapes accumulate multiple observations per owner.

    Growth tiers: 0-100 / 100-1k / 1k-10k / 10k+ (lower bound inclusive).
    """
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE OR REPLACE VIEW v_post_follower_context AS
            WITH candidates AS (
                SELECT
                    p.post_id,
                    o.followers_count,
                    o.observed_at,
                    o.source_dataset,
                    ROW_NUMBER() OVER (
                        PARTITION BY p.post_id
                        ORDER BY
                            CASE WHEN o.source_dataset = p.source_dataset
                                 THEN 0 ELSE 1 END,
                            o.observed_at ASC
                    ) AS rn
                FROM v_post_detail p
                JOIN silver_ig_profile_observations o
                    ON (o.owner_id = p.owner_id)
                    OR (p.owner_id IS NULL
                        AND o.owner_username = p.owner_username)
                WHERE o.observed_at >= p.timestamp
            )
            SELECT
                p.post_id,
                p.owner_id,
                p.owner_username,
                p.timestamp,
                c.followers_count,
                c.observed_at                   AS follower_observed_at,
                c.source_dataset                AS follower_source_dataset,
                CASE
                    WHEN c.followers_count IS NULL     THEN NULL
                    WHEN c.followers_count < 100       THEN '0-100'
                    WHEN c.followers_count < 1000      THEN '100-1k'
                    WHEN c.followers_count < 10000     THEN '1k-10k'
                    ELSE '10k+'
                END                             AS follower_tier
            FROM v_post_detail p
            LEFT JOIN candidates c
                ON c.post_id = p.post_id AND c.rn = 1
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
                (SELECT COUNT(*) FROM silver_content_classification
                 WHERE platform = 'instagram')         AS total_enriched,
                (SELECT COUNT(DISTINCT owner_username)
                 FROM silver_ig_posts)                 AS total_profiles,
                ROUND(
                    (SELECT COUNT(*) FROM silver_content_classification
                     WHERE platform = 'instagram')
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


@asset(
    name="v_quarantine_triage",
    group_name="serving",
    description=(
        "Operator triage surface for the enrichment quarantine (W8): each "
        "quarantined row joined to its offending bronze excerpt and post "
        "context."
    ),
    deps=[AssetKey(["silver_enrichment_quarantine"])],
)
def v_quarantine_triage(duckdb: DuckDBResource) -> None:
    """Create the quarantine triage view.

    ``silver_enrichment_quarantine``'s named consumer (ADR-0012/W8): one row
    per quarantined key with the reason, the verbatim bronze
    ``response_text``/``error_message`` (latest landing per key, read from
    the bronze Parquet — quarantined rows exist precisely because silver
    does NOT have them), and the owning post's context from
    ``silver_ig_posts``. LEFT JOINs throughout — a triage row must never
    disappear because its bronze file was pruned or the post left silver.
    """
    from orchestration.defs.engine import landing
    from orchestration.defs.platform import paths as lake

    bronze_file = landing.response_path(lake.BRONZE_LAKE)
    with duckdb.get_connection() as conn:
        conn.execute(f"""
            CREATE OR REPLACE VIEW v_quarantine_triage AS
            WITH bronze_latest AS (
                SELECT
                    post_id, platform, workload, provider, model,
                    ok, error_message, response_text, landing_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY post_id, platform, workload
                        ORDER BY landing_at DESC
                    ) AS rn
                FROM read_parquet('{bronze_file.as_posix()}')
            )
            SELECT
                q.post_id,
                q.platform,
                q.workload,
                q.reason_code,
                q.reason_detail,
                q.response_excerpt,
                q.quarantined_at,
                q.derivation_version,
                q.schema_version,
                q.prompt_hash,
                q.run_id,
                q.provider,
                q.model,
                b.ok                              AS bronze_ok,
                b.error_message                   AS bronze_error_message,
                b.response_text                   AS bronze_response_text,
                b.landing_at                      AS bronze_landing_at,
                sp.owner_username,
                sp.caption,
                sp.timestamp                      AS posted_at
            FROM silver_enrichment_quarantine q
            LEFT JOIN bronze_latest b
                ON b.post_id = q.post_id
               AND b.platform = q.platform
               AND b.workload = q.workload
               AND b.rn = 1
            LEFT JOIN silver_ig_posts sp
                ON sp.post_id = q.post_id
        """)


ASSETS: list = [
    v_post_detail,
    v_signal,
    v_quality_trend,
    v_creator_quality,
    v_rising_creators,
    v_domain_coverage,
    v_engagement_outliers,
    v_outlier_posts,
    v_creator_outlier_rate,
    v_underperformer_posts,
    v_creator_underperformer_rate,
    v_post_follower_context,
    v_overview,
    v_standout_calendar,
    v_recent_hot_posts,
    v_quarantine_triage,
]
