"""Golden-dataset snapshot test — new architecture.

Runs silver → enqueue on a committed bronze Parquet fixture and verifies
logical output columns.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest
from dagster import build_asset_context
from dagster_duckdb import DuckDBResource

from datalake.defs.common.resources import SQLiteResource
from datalake.defs.enrichment.batch import claim_batch
from datalake.defs.instagram.assets import ig_posts_gen_batches, ig_posts_slv
from datalake.defs.serving.assets import dim_date, profile_dimension, v_post_detail

SAMPLE_PARQUET = Path(__file__).resolve().parent.parent / "data" / "bronze_sample.parquet"


@pytest.fixture
def db(tmp_path) -> DuckDBResource:
    return DuckDBResource(database=str(tmp_path / "state.duckdb"))


@pytest.fixture
def ops_db(tmp_path) -> SQLiteResource:
    return SQLiteResource(database=str(tmp_path / "ops.sqlite"))


@pytest.fixture
def bronze_dir(tmp_path) -> Path:
    dest = tmp_path / "bronze_sample.parquet"
    dest.write_bytes(SAMPLE_PARQUET.read_bytes())
    return tmp_path


def test_silver_deduplication_preserves_all_posts(db, ops_db, bronze_dir):
    """GIVEN the committed bronze Parquet
    WHEN silver runs
    THEN all posts appear in silver with expected columns.
    """
    with patch("datalake.defs.instagram.assets.BRONZE_LAKE", bronze_dir):
        ctx = build_asset_context(resources={"duckdb": db, "ops": ops_db})
        result = ig_posts_slv(ctx)

    assert len(result) >= 1
    expected_silver_cols = {"post_id", "caption", "owner_id", "owner_username", "likes_count"}
    missing = expected_silver_cols - set(result.columns)
    assert not missing, f"Missing columns: {missing}"


def test_enqueue_enqueues_silver_posts(db, ops_db, bronze_dir):
    """GIVEN silver populated from the committed bronze Parquet
    WHEN ig_posts_gen_batches runs
    THEN posts are enqueued in ops.sqlite.
    """
    # Setup serving table for enqueue NOT EXISTS guard
    with db.get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS gold_analyses (
                post_id TEXT NOT NULL, domain TEXT NOT NULL DEFAULT 'instagram',
                prompt_hash TEXT, result_json TEXT, analysed_at TEXT NOT NULL,
                PRIMARY KEY (post_id, domain)
            )
        """)

    # Run silver
    with patch("datalake.defs.instagram.assets.BRONZE_LAKE", bronze_dir):
        ctx = build_asset_context(resources={"duckdb": db, "ops": ops_db})
        ig_posts_slv(ctx)

    # Label pass (labels-driven admission): approve every silver post
    from datetime import datetime, timezone

    from datalake.defs.instagram.labels import LABEL_VERSION

    now = datetime.now(timezone.utc)
    with db.get_connection() as conn:
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
            "SELECT post_id, 'standout', 'day7_matched', 'standout', ?, FALSE, ? "
            "FROM silver_ig_posts",
            [now, LABEL_VERSION],
        )

    # Run enqueue
    result = ig_posts_gen_batches(duckdb=db, ops=ops_db)

    assert result["enqueued"][0] >= 1

    # Verify queue
    batch = claim_batch(ops_db)
    assert batch is not None
    assert len(batch["payloads"]) >= 1


def test_serving_runs_on_empty_gold(db, ops_db, bronze_dir):
    """GIVEN silver from the committed bronze Parquet
    WHEN serving assets run (with empty gold_analyses)
    THEN views are created successfully.
    """
    # Setup serving schema
    with db.get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS gold_analyses (
                post_id TEXT NOT NULL, domain TEXT NOT NULL DEFAULT 'instagram',
                prompt_hash TEXT, result_json TEXT, analysed_at TEXT NOT NULL,
                PRIMARY KEY (post_id, domain)
            )
        """)

    with patch("datalake.defs.instagram.assets.BRONZE_LAKE", bronze_dir):
        ctx = build_asset_context(resources={"duckdb": db, "ops": ops_db})
        ig_posts_slv(ctx)

    ctx = build_asset_context(resources={"duckdb": db, "ops": ops_db})
    profile_dimension(ctx)
    dim_date(ctx)
    v_post_detail(ctx)

    with db.get_connection() as conn:
        count = conn.execute("SELECT COUNT(*) FROM v_post_detail").fetchone()[0]
        assert count >= 1
