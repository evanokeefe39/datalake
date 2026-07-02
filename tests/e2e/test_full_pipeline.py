"""E2E tests: full bronze → silver → enqueue → serving pipeline.

All assets run against tmp_path. Enqueue replaces the old synchronous gold step.
"""

from __future__ import annotations

from unittest.mock import patch

from dagster import build_asset_context
from dagster_duckdb import DuckDBResource

from datalake.defs.common.resources import SQLiteResource
from datalake.defs.enrichment.queue import claim
from datalake.defs.instagram.assets import ig_posts_gld_enqueue, ig_posts_slv
from datalake.defs.serving.assets import analytics_views, profile_dimension
from tests.fixtures.ig_bronze_factories import make_ig_bronze_row, write_ig_bronze


def _run_silver(duckdb, bronze_dir):
    with patch("datalake.defs.instagram.assets.BRONZE_LAKE", bronze_dir):
        ctx = build_asset_context(resources={"duckdb": duckdb})
        return ig_posts_slv(ctx)


def _run_enqueue(duckdb, ops):
    return ig_posts_gld_enqueue(duckdb=duckdb, ops=ops)


def _run_serving(duckdb):
    ctx = build_asset_context(resources={"duckdb": duckdb})
    profile_dimension(ctx)
    analytics_views(ctx)


def test_full_pipeline_happy_path(tmp_path):
    """GIVEN bronze Parquet files with multiple posts
    WHEN silver → enqueue → serving are run
    THEN all three layers produce the correct outputs.
    """
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))
    ops = SQLiteResource(database=str(tmp_path / "ops.sqlite"))

    # Setup serving schema
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS gold_analyses (
                post_id TEXT NOT NULL, domain TEXT NOT NULL DEFAULT 'instagram',
                prompt_hash TEXT, result_json TEXT, analysed_at TEXT NOT NULL,
                PRIMARY KEY (post_id, domain)
            )
        """)

    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir()
    rows = []
    for i, post_id in enumerate(["p1", "p2", "p3"]):
        rows.append(make_ig_bronze_row(
            post_id=post_id,
            shortcode=f"sc{i}",
            caption=f"Post {post_id} caption",
            username=f"user{i}",
            owner_id=f"owner{i}",
        ))
    write_ig_bronze(bronze_dir / "test.parquet", rows)

    # Silver
    silver_result = _run_silver(duckdb, bronze_dir)
    assert len(silver_result) == 3

    # Enqueue
    enqueue_result = _run_enqueue(duckdb, ops)
    assert enqueue_result["enqueued"][0] == 3

    # Verify queue
    claimed = claim(ops, limit=10)
    assert len(claimed) == 3

    # Serving (should run even with empty gold_analyses)
    _run_serving(duckdb)

    with duckdb.get_connection() as conn:
        rows = conn.execute("SELECT COUNT(*) FROM analytics_views").fetchone()
        assert rows[0] == 3


def test_empty_gold_does_not_block_serving(tmp_path):
    """GIVEN silver data but no gold enrichments
    WHEN serving assets run
    THEN views are created with NULL gold columns (LEFT JOIN).
    """
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))

    # Setup serving schema
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS gold_analyses (
                post_id TEXT NOT NULL, domain TEXT NOT NULL DEFAULT 'instagram',
                prompt_hash TEXT, result_json TEXT, analysed_at TEXT NOT NULL,
                PRIMARY KEY (post_id, domain)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS silver_ig_posts (
                post_id TEXT PRIMARY KEY, caption TEXT, processed_on TIMESTAMP,
                owner_id TEXT DEFAULT 'test', owner_username TEXT DEFAULT 'test',
                likes_count INTEGER DEFAULT 0, comments_count INTEGER DEFAULT 0,
                video_play_count INTEGER DEFAULT 0, video_view_count INTEGER DEFAULT 0,
                timestamp TIMESTAMP DEFAULT NOW(), hashtags TEXT DEFAULT '[]',
                has_engagement_bait BOOLEAN DEFAULT FALSE, media_files TEXT DEFAULT '[]',
                media_count INTEGER DEFAULT 0, source_dataset TEXT DEFAULT 'test',
                shortcode TEXT DEFAULT '', url TEXT DEFAULT '', meta_data TEXT DEFAULT '{}'
            )
        """)
        conn.execute(
            "INSERT INTO silver_ig_posts (post_id, caption, processed_on) VALUES (?, ?, NOW())",
            ["p1", "Test"],
        )

    _run_serving(duckdb)

    with duckdb.get_connection() as conn:
        row = conn.execute("SELECT COUNT(*) FROM analytics_views").fetchone()
        assert row[0] == 1
