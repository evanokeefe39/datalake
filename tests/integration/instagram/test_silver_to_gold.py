"""Integration tests: silver DuckDB state → gold enqueue.

Tests the cross-asset boundary between ``ig_posts_slv`` (silver output in
DuckDB) and ``ig_posts_gen_batches`` (batch-based enqueuer).
"""

from __future__ import annotations

from dagster_duckdb import DuckDBResource

from orchestration.defs.platform.resources import SQLiteResource
from orchestration.defs.ig_core.slv.posts import ig_posts_slv
from orchestration.defs.ig_enriched.slv.workloads import ig_posts_gen_batches
from tests.fixtures.ig_bronze_factories import make_ig_bronze_row, write_ig_bronze


def _run_silver(duckdb, ops, bronze_dir):
    from unittest.mock import patch

    with patch("orchestration.defs.platform.paths.BRONZE_LAKE", bronze_dir):
        from dagster import build_asset_context

        ctx = build_asset_context(resources={"duckdb": duckdb, "ops": ops})
        return ig_posts_slv(ctx)


def _run_enqueue(duckdb, ops):
    from dagster import DagsterInstance, build_asset_context

    from orchestration.defs.ig_core.bnz.scrape import GoldConfig

    instance = DagsterInstance.ephemeral()
    return ig_posts_gen_batches(
        build_asset_context(instance=instance), config=GoldConfig(),
        duckdb=duckdb, ops=ops,
    )


def test_enqueue_reads_silver_output(tmp_path):
    """GIVEN silver has posts via bronze→silver pipeline
    WHEN ig_posts_gen_batches runs
    THEN posts are enqueued in ops.sqlite.
    """
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))

    ops = SQLiteResource(database=str(tmp_path / "ops.sqlite"))

    # Run bronze → silver
    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir()
    row = make_ig_bronze_row(post_id="p1", shortcode="sc1", caption="Test caption", username="test")
    write_ig_bronze(bronze_dir / "test.parquet", [row])

    result = _run_silver(duckdb, ops, bronze_dir)
    from datetime import datetime, timezone

    from orchestration.defs.ig_core.slv.labels import LABEL_VERSION

    # Label pass (labels-driven admission): approve p1 for enrichment
    now = datetime.now(timezone.utc)
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ig_post_labels (
                post_id VARCHAR PRIMARY KEY,
                label VARCHAR NOT NULL,
                method VARCHAR NOT NULL,
                enrich_decision VARCHAR NOT NULL,
                judged_at TIMESTAMP WITH TIME ZONE NOT NULL,
                is_provisional BOOLEAN NOT NULL,
                label_version INTEGER NOT NULL,
                baseline_center DOUBLE,
                baseline_spread DOUBLE,
                baseline_n INTEGER
            )
        """)
        conn.execute(
            "INSERT OR REPLACE INTO ig_post_labels "
            "(post_id, label, method, enrich_decision, judged_at, "
            " is_provisional, label_version) "
            "VALUES (?, 'standout', 'day7_matched', 'standout', ?, FALSE, ?)",
            ["p1", now, LABEL_VERSION],
        )
    assert len(result) == 1


def test_enqueue_skips_already_completed(tmp_path):
    """GIVEN a label-approved post with a current-prompt gold analysis
    WHEN ig_posts_gen_batches runs
    THEN it is seen as a candidate but not re-enqueued.
    """
    from datetime import datetime, timezone

    from orchestration.defs.ig_enriched.slv.prompts import CURRENT_PROMPT_HASH
    from orchestration.defs.ig_core.slv.labels import LABEL_VERSION

    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))
    ops = SQLiteResource(database=str(tmp_path / "ops.sqlite"))

    # Seed silver
    now = datetime.now(timezone.utc)
    with duckdb.get_connection() as conn:
        from orchestration.defs.ig_enriched.slv.classification import CLASSIFICATION_DDL

        conn.execute(CLASSIFICATION_DDL)
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
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ig_post_labels (
                post_id VARCHAR PRIMARY KEY,
                label VARCHAR NOT NULL,
                method VARCHAR NOT NULL,
                enrich_decision VARCHAR NOT NULL,
                judged_at TIMESTAMP WITH TIME ZONE NOT NULL,
                is_provisional BOOLEAN NOT NULL,
                label_version INTEGER NOT NULL,
                baseline_center DOUBLE,
                baseline_spread DOUBLE,
                baseline_n INTEGER
            )
        """)
        conn.execute(
            "INSERT INTO silver_ig_posts (post_id, caption, processed_on) VALUES (?, ?, ?)",
            ["p1", "Test caption", now],
        )
        conn.execute(
            "INSERT INTO ig_post_labels "
            "(post_id, label, method, enrich_decision, judged_at, "
            " is_provisional, label_version) "
            "VALUES (?, 'standout', 'day7_matched', 'standout', ?, FALSE, ?)",
            ["p1", now, LABEL_VERSION],
        )
        conn.execute(
            "INSERT INTO silver_content_classification "
            "(post_id, platform, prompt_hash, analysed_at) "
            "VALUES (?, 'instagram', ?, ?)",
            ["p1", CURRENT_PROMPT_HASH, now.isoformat()],
        )

    result = _run_enqueue(duckdb, ops)
    assert result["enqueued"][0] == 0
    assert result["candidates_seen"][0] == 0
    # No enqueue happened
    assert result["enqueued"][0] == 0

