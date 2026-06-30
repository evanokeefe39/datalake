"""Tests for the serving layer assets (profile_dimension, analytics_views)."""

from __future__ import annotations

import pytest
from dagster import build_asset_context
from dagster_duckdb import DuckDBResource


def _seed_silver_posts(duckdb, rows: list[tuple]) -> None:
    """Insert test rows into silver_posts."""
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS silver_posts (
                post_id TEXT PRIMARY KEY, shortcode TEXT, url TEXT,
                caption TEXT, owner_id TEXT, owner_username TEXT,
                likes_count INTEGER, comments_count INTEGER,
                video_play_count INTEGER, video_view_count INTEGER,
                timestamp TIMESTAMP, hashtags TEXT, meta_data TEXT,
                has_engagement_bait BOOLEAN, media_files TEXT,
                media_count INTEGER, source_dataset TEXT,
                silvered_at TIMESTAMP
            )
        """)
        for row in rows:
            conn.execute(
                """INSERT OR REPLACE INTO silver_posts
                   (post_id, owner_id, owner_username, caption,
                    likes_count, comments_count, timestamp, hashtags,
                    has_engagement_bait, media_files, media_count,
                    source_dataset)
                   VALUES (?, ?, ?, ?, 0, 0, NOW(),
                    '[]', FALSE, '[]', 0, 'test')""",
                row,
            )


@pytest.fixture
def db(request) -> DuckDBResource:
    """Create a DuckDB resource backed by a tmp_path file."""
    import os
    import tempfile

    db_path = os.path.join(tempfile.mkdtemp(), "test.duckdb")

    def cleanup():
        try:
            os.unlink(db_path)
        except FileNotFoundError:
            pass

    request.addfinalizer(cleanup)
    return DuckDBResource(database=db_path)


# ── profile_dimension tests ────────────────────────────────────────────────────


def test_profile_dimension_creates_rows(db):
    """Distinct owner_ids from silver_posts → rows in profile_dimension."""
    _seed_silver_posts(db, [
        ("1", "owner_a", "user_a", "Post 1"),
        ("2", "owner_b", "user_b", "Post 2"),
        ("3", "owner_a", "user_a", "Post 3"),
    ])
    from datalake.defs.serving.assets import profile_dimension

    context = build_asset_context(resources={"duckdb": db})
    profile_dimension(context)

    with db.get_connection() as conn:
        rows = conn.execute(
            "SELECT owner_id, owner_username, is_current "
            "FROM dim_profile ORDER BY owner_id"
        ).fetchall()

    assert rows == [
        ("owner_a", "user_a", True),
        ("owner_b", "user_b", True),
    ]


def test_profile_dimension_scd2_username_change(db):
    """Same owner with new username → closes old row, inserts new."""
    _seed_silver_posts(db, [
        ("1", "owner_a", "old_name", "Post"),
    ])
    from datalake.defs.serving.assets import profile_dimension
    context = build_asset_context(resources={"duckdb": db})

    profile_dimension(context)
    with db.get_connection() as conn:
        rows = conn.execute(
            "SELECT owner_username, is_current, effective_to "
            "FROM dim_profile WHERE owner_id = 'owner_a' "
            "ORDER BY effective_from"
        ).fetchall()
    assert len(rows) == 1
    assert rows[0] == ("old_name", True, None)

    # Update username in place, then re-run
    with db.get_connection() as conn:
        conn.execute(
            "UPDATE silver_posts SET owner_username = ? WHERE owner_id = ?",
            ["new_name", "owner_a"],
        )
    profile_dimension(context)

    with db.get_connection() as conn:
        rows = conn.execute(
            "SELECT owner_username, is_current, effective_to IS NOT NULL "
            "FROM dim_profile WHERE owner_id = 'owner_a' "
            "ORDER BY effective_from"
        ).fetchall()
    assert len(rows) == 2
    assert rows[0] == ("old_name", False, True)  # closed
    assert rows[1] == ("new_name", True, False)   # open

def test_profile_dimension_no_change_idempotent(db):
    """Same owner, same username → no new rows added."""
    _seed_silver_posts(db, [("1", "owner_a", "user_a", "Post")])
    from datalake.defs.serving.assets import profile_dimension

    context = build_asset_context(resources={"duckdb": db})

    profile_dimension(context)
    profile_dimension(context)  # run twice

    with db.get_connection() as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM dim_profile"
        ).fetchone()[0]
    assert count == 1  # no duplicate row


# ── analytics_views tests ─────────────────────────────────────────────────


def test_analytics_views_joins_correctly(db):
    """analytics_views joins silver_posts with profile_dimension."""
    _seed_silver_posts(db, [("1", "owner_a", "user_a", "Test post")])
    from datalake.defs.serving.assets import analytics_views, profile_dimension

    ctx = build_asset_context(resources={"duckdb": db})
    profile_dimension(ctx)
    analytics_views(ctx)

    with db.get_connection() as conn:
        row = conn.execute(
            "SELECT post_id, owner_username, owner_id, is_current "
            "FROM analytics_views"
        ).fetchone()

    assert row == ("1", "user_a", "owner_a", True)


def test_analytics_views_empty_data(db):
    """analytics_views runs cleanly with empty silver_posts."""
    _seed_silver_posts(db, [])
    from datalake.defs.serving.assets import analytics_views, profile_dimension

    context = build_asset_context(resources={"duckdb": db})
    profile_dimension(context)
    analytics_views(context)

    with db.get_connection() as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM analytics_views"
        ).fetchone()[0]
    assert count == 0
