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
        conn.execute("""
            CREATE TABLE IF NOT EXISTS dim_profile (
                profile_key      INTEGER PRIMARY KEY,
                owner_id         TEXT NOT NULL,
                owner_username   TEXT,
                channel          TEXT NOT NULL DEFAULT 'instagram',
                effective_from   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                effective_to     TIMESTAMP,
                is_current       BOOLEAN NOT NULL DEFAULT TRUE,
                profile_pic_path TEXT,
                creator_id       INTEGER,
                creator_name     TEXT
            )
        """)

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
    name="v_profile_quality",
    group_name="serving",
    description="Creator rankings: admiralty score, educational rate, outlier rate.",
    deps=[AssetKey(["v_post_detail"])],
)
def v_profile_quality(duckdb: DuckDBResource) -> None:
    """Per-creator quality metrics — weighted admiralty, educational rate, engagement."""
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE OR REPLACE VIEW v_profile_quality AS
            SELECT
                COALESCE(MAX(owner_id) FILTER (WHERE owner_id IS NOT NULL), 'unknown') AS owner_id,
                owner_username,
                MAX(creator_id)                                              AS creator_id,
                COUNT(*)                                                     AS total_posts,
                COUNT(CASE WHEN result_json IS NOT NULL THEN 1 END)          AS enriched_posts,
                AVG(CASE
                    WHEN admiralty LIKE 'A%' THEN 3.0
                    WHEN admiralty LIKE 'B%' THEN 2.0
                    WHEN admiralty LIKE 'C%' THEN 1.0
                    ELSE 0.0
                END)                                                         AS admiralty_score,
                AVG(CASE WHEN is_educational THEN 1.0 ELSE 0.0 END)          AS educational_rate,
                AVG(likes_count)                                             AS avg_likes,
                AVG(comments_count)                                          AS avg_comments,
                AVG(video_view_count)                                        AS avg_video_views,
                MAX(likes_count)                                             AS max_likes
            FROM v_post_detail
            WHERE result_json IS NOT NULL
            GROUP BY owner_username
            ORDER BY admiralty_score DESC
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
    description="Per-creator z-scores for likes with sigma-tier flags.",
    deps=[AssetKey(["v_post_detail"])],
)
def v_engagement_outliers(duckdb: DuckDBResource) -> None:
    """Compute per-creator z-scores on likes_count, flagging sigma tiers."""
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE OR REPLACE VIEW v_engagement_outliers AS
            WITH zscored AS (
                SELECT
                    *,
                    (likes_count - AVG(likes_count) OVER (PARTITION BY owner_id))
                        / NULLIF(STDDEV(likes_count) OVER (PARTITION BY owner_id), 0)
                        AS likes_zscore
                FROM v_post_detail
            )
            SELECT
                *,
                CASE
                    WHEN likes_zscore >= 3 THEN '3σ+'
                    WHEN likes_zscore >= 2 THEN '2σ'
                    WHEN likes_zscore >= 1 THEN '1σ'
                    WHEN likes_zscore <= -1 THEN '-1σ'
                    ELSE 'normal'
                END AS sigma_tier
            FROM zscored
        """)


@asset(
    name="v_outlier_posts",
    group_name="serving",
    description="Posts 1σ+ above their creator's mean likes.",
    deps=[AssetKey(["v_post_detail"]), AssetKey(["v_engagement_outliers"])],
)
def v_outlier_posts(duckdb: DuckDBResource) -> None:
    """Filter to outlier posts only — positive z-score of 1σ or more."""
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
    description="Which creators produce the most engagement outliers.",
    deps=[AssetKey(["v_post_detail"]), AssetKey(["v_engagement_outliers"])],
)
def v_creator_outlier_rate(duckdb: DuckDBResource) -> None:
    """Per-creator outlier stats — rate, avg z-score, max z-score."""
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


# ── Exported for definitions.py ─────────────────────────────────────────────

assets: list = [
    profile_dimension,
    dim_date,
    v_post_detail,
    v_signal,
    v_quality_trend,
    v_profile_quality,
    v_domain_coverage,
    v_engagement_outliers,
    v_outlier_posts,
    v_creator_outlier_rate,
]
