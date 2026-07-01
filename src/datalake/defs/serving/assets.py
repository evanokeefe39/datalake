"""Cross-domain shared dimensions and views for the datalake.

``dim_profile`` tracks Instagram profiles with SCD2 (slowly changing dimension)
via ``effective_from``/``effective_to``/``is_current``.

``analytics_views`` is a unified view joining silver posts, gold analyses,
and profile dimensions.
"""

from __future__ import annotations

from datetime import datetime, timezone

from dagster import asset
from dagster_duckdb import DuckDBResource


@asset(
    name="dim_profile",
    group_name="serving",
    description="SCD2 profile dimension tracking owner attributes over time.",
)
def profile_dimension(duckdb: DuckDBResource) -> None:
    """Upsert profile dimension with SCD2 tracking.

    Reads distinct owner profiles from ``silver_ig_posts`` and maintains
    ``effective_from``/``effective_to``/``is_current`` in DuckDB.
    """
    db = duckdb
    with db.get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS dim_profile (
                profile_key     INTEGER PRIMARY KEY,
                owner_id        TEXT NOT NULL,
                owner_username  TEXT,
                channel         TEXT NOT NULL DEFAULT 'instagram',
                effective_from  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                effective_to    TIMESTAMP,
                is_current      BOOLEAN NOT NULL DEFAULT TRUE
            )
        """)

        # Get distinct profiles from silver_ig_posts
        profiles = conn.execute("""
            SELECT DISTINCT owner_id, owner_username
            FROM silver_ig_posts
            WHERE owner_id IS NOT NULL
        """).fetchall()

        if not profiles:
            return

        # Determine next profile_key
        max_key = conn.execute(
            "SELECT COALESCE(MAX(profile_key), 0) FROM dim_profile"
        ).fetchone()[0]

        now_ts = datetime.now(timezone.utc).isoformat()

        for owner_id, owner_username in profiles:
            # Check existing current row
            existing = conn.execute("""
                SELECT profile_key, owner_username
                FROM dim_profile
                WHERE owner_id = ? AND is_current = TRUE
            """, [owner_id]).fetchone()

            if existing:
                existing_key, existing_username = existing
                if existing_username == owner_username:
                    # No change — skip
                    continue
                # Close the old row
                conn.execute("""
                    UPDATE dim_profile
                    SET effective_to = ?, is_current = FALSE
                    WHERE profile_key = ?
                """, [now_ts, existing_key])

            # Insert new row
            max_key += 1
            conn.execute("""
                INSERT INTO dim_profile
                    (profile_key, owner_id, owner_username, channel,
                     effective_from, effective_to, is_current)
                VALUES (?, ?, ?, 'instagram', ?, NULL, TRUE)
            """, [max_key, owner_id, owner_username, now_ts])


@asset(
    name="analytics_views",
    group_name="serving",
    description="Unified view joining silver posts, gold analyses, and profiles.",
    deps=["profile_dimension"],
)
def analytics_views(duckdb: DuckDBResource) -> None:
    """Create or replace the unified analytics view.

    Consumers looking for enrichment failures should query ``dead_letter``
    directly (``WHERE domain = 'instagram'``).
    """
    with duckdb.get_connection() as conn:
        # Ensure gold_ig_analyses exists for LEFT JOIN (even if gold hasn't run)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS gold_ig_analyses (
                post_id         TEXT PRIMARY KEY,
                schema_version  INTEGER NOT NULL DEFAULT 3,
                result_json     TEXT,
                analysed_at     TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE OR REPLACE VIEW analytics_views AS
            SELECT
                sp.post_id,
                sp.shortcode,
                sp.url,
                sp.caption,
                sp.owner_id,
                sp.owner_username,
                sp.likes_count,
                sp.comments_count,
                sp.video_view_count,
                sp.timestamp,
                sp.hashtags,
                sp.source_dataset,
                sp.processed_on,
                ga.result_json,
                ga.analysed_at AS gold_analysed_at,
                dp.profile_key,
                dp.channel,
                dp.effective_from,
                dp.effective_to,
                dp.is_current
            FROM "silver_ig_posts" sp
            LEFT JOIN "gold_ig_analyses" ga ON sp.post_id = ga.post_id
            LEFT JOIN "dim_profile" dp
                ON sp.owner_id = dp.owner_id AND dp.is_current = TRUE
        """)

# ── Exported for definitions.py ───────────────────────────────────────────

assets: list = [profile_dimension, analytics_views]
