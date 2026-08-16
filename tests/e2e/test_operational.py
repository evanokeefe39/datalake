"""E2E tests: operational concerns — schedules and ad-hoc run sequences.

Per test-hardening plan Phase 3:
- Schedule ``daily_medallion`` loads without error
- Schedule target list matches actual asset keys
- Ad-hoc run sequence: ``ig_posts_slv`` → ``ig_posts_gen_batches`` → serving, verify each step
"""

from __future__ import annotations

import json
from unittest.mock import patch

from dagster import build_asset_context, build_schedule_context

from datalake.defs.common.resources import SQLiteResource
from datalake.defs.common.schedules import daily_medallion
from datalake.defs.instagram.assets import ig_posts_gen_batches, ig_posts_slv
from datalake.defs.serving.assets import profile_dimension, v_post_detail
from tests.fixtures.ig_bronze_factories import make_ig_bronze_row, write_ig_bronze

# ── Helpers ────────────────────────────────────────────────────────────────


def _run_silver(duckdb, ops, bronze_dir):
    with patch("datalake.defs.instagram.assets.BRONZE_LAKE", bronze_dir):
        ctx = build_asset_context(resources={"duckdb": duckdb, "ops": ops})
        return ig_posts_slv(ctx)


def _run_enqueue(duckdb, ops_db):
    from datalake.defs.instagram.config import GoldConfig

    return ig_posts_gen_batches(config=GoldConfig(), duckdb=duckdb, ops=ops_db)


def _run_profile_dimension(duckdb, ops):
    ctx = build_asset_context(resources={"duckdb": duckdb, "ops": ops})
    profile_dimension(ctx)


def _run_serving_views(duckdb):
    ctx = build_asset_context(resources={"duckdb": duckdb})
    v_post_detail(ctx)



def test_daily_medallion_schedule_loads(tmp_path):
    """GIVEN a schedule context
    WHEN the daily_medallion schedule is evaluated
    THEN it resolves without error and produces a tick with run request(s).
    """
    ctx = build_schedule_context()
    result = daily_medallion.evaluate_tick(ctx)

    assert result is not None
    # The schedule should produce at least one run request
    assert len(result) > 0


# ── Test: schedule target list matches actual asset keys ───────────────────


def test_schedule_target_matches_asset_keys():
    """GIVEN the daily_medallion schedule definition
    WHEN its target is inspected
    THEN it targets exactly the expected downstream assets
         (slv, gld_enqueue, serving — not bronze).
    """
    target_repr = repr(daily_medallion.target)
    # Bronze is on-demand; schedule drives silver → enqueue → serving
    assert "ig_posts_slv" in target_repr
    assert "ig_posts_gen_batches" in target_repr
    assert "dim_profile" in target_repr
    assert "v_post_detail" in target_repr
    assert "ig_posts_raw" not in target_repr


# ── Test: ad-hoc run sequence ──────────────────────────────────────────────


def test_ad_hoc_run_sequence(tmp_path):
    """GIVEN a fresh DuckDB with no data
    WHEN bronze → silver → enqueue are run with a single post
    THEN silver deduplicates and enqueue writes to the queue (no Gemini call).
    """
    # Setup ops SQLite
    ops_path = tmp_path / "ops.sqlite"
    ops_db = SQLiteResource(database=str(ops_path))

    # Setup DuckDB
    from dagster_duckdb import DuckDBResource

    db_path = tmp_path / "test.duckdb"
    duckdb_res = DuckDBResource(database=str(db_path))
    # Create gold_analyses table for the enqueue NOT EXISTS guard
    with duckdb_res.get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS gold_analyses (
                post_id TEXT NOT NULL,
                domain TEXT NOT NULL DEFAULT 'instagram',
                prompt_hash TEXT,
                result_json TEXT,
                analysed_at TEXT NOT NULL,
                PRIMARY KEY (post_id, domain)
            )
        """)

    # Step 1: Write bronze Parquet
    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir()
    row = make_ig_bronze_row(post_id="p1", shortcode="sc1", caption="Test caption", username="test")
    write_ig_bronze(bronze_dir / "test.parquet", [row])
    # Step 2: Silver deduplication
    result = _run_silver(duckdb_res, ops_db, bronze_dir)
    assert len(result) == 1

    # Step 3: Enqueue
    result = _run_enqueue(duckdb_res, ops_db)
    assert result["enqueued"][0] == 1

    # Step 4: Verify queue has the item
    from datalake.defs.enrichment.batch import claim_batch

    batch = claim_batch(ops_db)
    assert batch is not None
    assert len(batch["payloads"]) == 1
    assert json.loads(batch["payloads"][0])["post_id"] == "p1"
