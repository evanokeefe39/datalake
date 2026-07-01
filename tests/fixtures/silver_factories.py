"""Silver-layer factory helpers for tests.

Consolidates the ``_seed_silver_posts`` helpers previously duplicated in
``test_gold.py`` and ``test_serving.py``.
"""

from __future__ import annotations


def seed_silver_posts(
    duckdb,
    rows: list[tuple],
    *,
    caption_idx: int = 1,
    owner_id_idx: int | None = None,
    owner_username_idx: int | None = None,
) -> None:
    """Insert test rows into ``silver_ig_posts`` (creates table if needed).

    Parameters
    ----------
    duckdb:
        A ``DuckDBResource`` instance.
    rows:
        Each tuple provides field values. The minimum is ``(post_id, caption)``.
    caption_idx:
        Index of the caption field within each row (default 1).
    owner_id_idx, owner_username_idx:
        If provided, extras are inserted as owner identity columns.
    """
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS silver_ig_posts (
                post_id TEXT PRIMARY KEY, shortcode TEXT, url TEXT,
                caption TEXT, owner_id TEXT, owner_username TEXT,
                likes_count INTEGER, comments_count INTEGER,
                video_play_count INTEGER, video_view_count INTEGER,
                timestamp TIMESTAMP, hashtags TEXT, meta_data TEXT,
                has_engagement_bait BOOLEAN, media_files TEXT,
                media_count INTEGER, source_dataset TEXT,
                processed_on TIMESTAMP
            )
        """)
        for row in rows:
            post_id = row[0]
            caption = row[caption_idx]
            owner_id = row[owner_id_idx] if owner_id_idx is not None else "default_owner"
            owner_username = row[owner_username_idx] if owner_username_idx is not None else "default_user"
            conn.execute(
                """INSERT OR REPLACE INTO silver_ig_posts
                   (post_id, caption, owner_id, owner_username,
                    likes_count, comments_count, video_play_count,
                    video_view_count, timestamp, hashtags,
                    has_engagement_bait, media_files, media_count,
                    source_dataset, processed_on)
                   VALUES (?, ?, ?, ?, 0, 0, 0, 0, NOW(),
                    '[]', FALSE, '[]', 0, 'test', NOW())""",
                [post_id, caption, owner_id, owner_username],
            )
