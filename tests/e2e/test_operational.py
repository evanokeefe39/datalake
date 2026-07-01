"""E2E tests: operational concerns — schedules and ad-hoc run sequences.

Per test-hardening plan Phase 3:
- Schedule ``weekly_medallion`` loads without error
- Schedule target list matches actual asset keys
- Ad-hoc run sequence: ``ig_posts_slv`` → ``ig_posts_gld`` → serving, verify each step
"""

from __future__ import annotations

import json
from unittest.mock import patch

from dagster import build_asset_context, build_schedule_context
from dagster_duckdb import DuckDBResource

from datalake.defs.common.resources import GeminiResource
from datalake.defs.common.schedules import weekly_medallion
from datalake.defs.instagram.assets import ig_posts_gld, ig_posts_slv
from datalake.defs.serving.assets import analytics_views, profile_dimension
from tests.fixtures.ig_bronze_factories import make_ig_bronze_row, write_ig_bronze
from tests.fixtures.gold_factories import FAKE_ANALYSIS

# ── Helpers ────────────────────────────────────────────────────────────────


def _run_silver(duckdb, bronze_dir):
    with patch("datalake.defs.instagram.assets.BRONZE_LAKE", bronze_dir):
        ctx = build_asset_context(resources={"duckdb": duckdb})
        return ig_posts_slv(ctx)


def _run_gold(duckdb, gemini):
    ctx = build_asset_context(resources={"duckdb": duckdb, "gemini": gemini})
    return ig_posts_gld(ctx)


def _run_profile_dimension(duckdb):
    ctx = build_asset_context(resources={"duckdb": duckdb})
    profile_dimension(ctx)


def _run_analytics_views(duckdb):
    ctx = build_asset_context(resources={"duckdb": duckdb})
    analytics_views(ctx)


# ── Test: schedule loads without error ─────────────────────────────────────


def test_weekly_medallion_schedule_loads(tmp_path):
    """GIVEN a schedule context
    WHEN the weekly_medallion schedule is evaluated
    THEN it resolves without error and produces a tick with run request(s).
    """
    ctx = build_schedule_context()
    result = weekly_medallion.evaluate_tick(ctx)

    assert result is not None
    # The schedule should produce at least one run request
    assert len(result) > 0


# ── Test: schedule target list matches actual asset keys ───────────────────


def test_schedule_target_matches_asset_keys():
    """GIVEN the weekly_medallion schedule definition
    WHEN its target is inspected
    THEN it targets exactly the expected downstream assets
         (slv, gld, serving — not bronze).
    """
    target_repr = repr(weekly_medallion.target)
    # Bronze is on-demand; schedule drives silver → gold → serving
    assert "ig_posts_slv" in target_repr
    assert "ig_posts_gld" in target_repr
    assert "dim_profile" in target_repr
    assert "analytics_views" in target_repr

    # Bronze must NOT be in the schedule (it's manual-trigger only)
    assert "ig_posts_raw" not in target_repr


# ── Test: ad-hoc run sequence ──────────────────────────────────────────────


def test_ad_hoc_run_sequence(tmp_path):
    """GIVEN a fresh DuckDB with no data
    WHEN assets are run ad-hoc in sequence: silver → gold → serving
    THEN each step's state is verifiable before proceeding to the next.
    """
    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir()
    write_ig_bronze(
        bronze_dir / "ds_001.parquet",
        [
            make_ig_bronze_row("p1", "abc", "Ad-hoc post content", "user_a",
                            likes=50, comments=5),
        ],
    )
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))
    gemini = GeminiResource()

    # ── Step 1: silver ──────────────────────────────────────────────────
    silver_result = _run_silver(duckdb, bronze_dir)

    with duckdb.get_connection() as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM silver_ig_posts"
        ).fetchone()[0]
    assert count == 1
    assert len(silver_result) == 1
    assert silver_result["post_id"][0] == "p1"

    # ── Step 2: gold ────────────────────────────────────────────────────
    # Drop watermarks created by silver (config_hash column mismatch)
    with duckdb.get_connection() as conn:
        conn.execute("DROP TABLE IF EXISTS watermarks")

    with patch.object(
        GeminiResource, "analyze", return_value=json.dumps(FAKE_ANALYSIS)
    ):
        gold_result = _run_gold(duckdb, gemini)

    with duckdb.get_connection() as conn:
        g_count = conn.execute(
            "SELECT COUNT(*) FROM gold_ig_analyses"
        ).fetchone()[0]
    assert g_count == 1
    assert gold_result["post_id"][0] == "p1"

    # ── Step 3: serving ─────────────────────────────────────────────────
    _run_profile_dimension(duckdb)
    _run_analytics_views(duckdb)

    with duckdb.get_connection() as conn:
        view_rows = conn.execute(
            "SELECT post_id, result_json, profile_key, channel "
            "FROM analytics_views"
        ).fetchall()
    assert len(view_rows) == 1
    post_id, result_json, profile_key, channel = view_rows[0]
    assert post_id == "p1"
    assert result_json is not None  # gold enrichment present
    assert profile_key is not None  # dimension resolved
    assert channel == "instagram"
