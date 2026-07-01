"""Golden-dataset snapshot test.

Runs the full medallion pipeline on a committed bronze Parquet fixture and
compares logical output columns against expected values. Volatile columns
(analysed_at, processed_on, timestamps) are excluded from the diff.

Per test-hardening plan Phase 5:
- Fixed bronze input → deterministic silver/gold/serving output
- Diff on logical (business-logic) columns only
- Catch regressions in field extraction, enrichment, or SCD2 logic
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest
from dagster import build_asset_context
from dagster_duckdb import DuckDBResource

from datalake.defs.common.resources import GeminiResource
from datalake.defs.instagram.assets import ig_posts_gld, ig_posts_slv
from datalake.defs.serving.assets import analytics_views, profile_dimension

from tests.fixtures.gold_factories import FAKE_ANALYSIS

# ── Fixture: the committed bronze Parquet ──────────────────────────────────

SAMPLE_PARQUET = Path(__file__).resolve().parent.parent / "data" / "bronze_sample.parquet"


@pytest.fixture
def db(tmp_path) -> DuckDBResource:
    """File-backed DuckDB so connections persist across ``get_connection()`` calls."""
    return DuckDBResource(database=str(tmp_path / "state.duckdb"))


@pytest.fixture
def bronze_dir(tmp_path) -> Path:
    """Isolate the sample Parquet in a temp directory."""
    dest = tmp_path / "bronze_sample.parquet"
    dest.write_bytes(SAMPLE_PARQUET.read_bytes())
    return tmp_path


@pytest.fixture
def gemini() -> GeminiResource:
    return GeminiResource(api_key="test-key")


# ── Helpers ────────────────────────────────────────────────────────────────

_VOLATILE_COLUMNS = {"analysed_at", "processed_on"}


def _assert_logical_match(df: pl.DataFrame, *, expected: list[dict]):
    """Check that logical columns match expected values.
    Volatile columns (analysed_at, processed_on) are excluded.
    """
    actual = df.to_dicts()
    assert len(actual) == len(expected), f"Row count: {len(actual)} != {len(expected)}"

    for a, e in zip(actual, expected):
        for key, value in e.items():
            if key in _VOLATILE_COLUMNS:
                continue
            assert a.get(key) == value, (
                f"Column '{key}': expected {value!r}, got {a.get(key)!r}"
            )


# ── Snapshot: silver ───────────────────────────────────────────────────────


def test_silver_snapshot(db, bronze_dir):
    """Silver output on frozen bronze input matches expected values."""
    with patch("datalake.defs.instagram.assets.BRONZE_LAKE", bronze_dir):
        ctx = build_asset_context(resources={"duckdb": db})
        result = ig_posts_slv(ctx)

    expected = [
        {"post_id": "post_001", "shortcode": "abc",
         "caption": "Great post about AI marketing",
         "owner_id": "owner_1", "owner_username": "user1",
         "likes_count": 120, "comments_count": 15,
         "has_engagement_bait": False, "source_dataset": "bronze_sample"},
        {"post_id": "post_002", "shortcode": "def", "caption": "",
         "owner_id": "owner_1", "owner_username": "user1",
         "likes_count": 45, "comments_count": 3,
         "has_engagement_bait": False, "source_dataset": "bronze_sample"},
        {"post_id": "post_003", "shortcode": "ghi",
         "caption": "Interesting take on startups",
         "owner_id": "owner_2", "owner_username": "user2",
         "likes_count": 300, "comments_count": 42,
         "has_engagement_bait": False, "source_dataset": "bronze_sample"},
        {"post_id": "post_004", "shortcode": "jkl",
         "caption": "AI tools for productivity",
         "owner_id": "owner_3", "owner_username": "user3",
         "likes_count": 89, "comments_count": 7,
         "has_engagement_bait": False, "source_dataset": "bronze_sample"},
        {"post_id": "post_005", "shortcode": "mno",
         "caption": "Marketing tips 2024",
         "owner_id": "owner_2", "owner_username": "user2",
         "likes_count": 210, "comments_count": 28,
         "has_engagement_bait": False, "source_dataset": "bronze_sample"},
    ]
    _assert_logical_match(result, expected=expected)


# ── Snapshot: gold ─────────────────────────────────────────────────────────


def test_gold_snapshot(db, bronze_dir, gemini):
    """Gold output on frozen silver input matches expected values.

    ``post_002`` has an empty caption → routed to dead_letter (not gold).
    The other 4 posts are enriched successfully.
    """
    with patch("datalake.defs.instagram.assets.BRONZE_LAKE", bronze_dir):
        ig_posts_slv(build_asset_context(resources={"duckdb": db}))

    with patch.object(GeminiResource, "analyze",
                      return_value=json.dumps(FAKE_ANALYSIS)):
        ctx = build_asset_context(resources={"duckdb": db, "gemini": gemini})
        result = ig_posts_gld(ctx)

    assert len(result) == 4  # post_002 (empty caption) → dead_letter
    gold_ids = set(result["post_id"].to_list())
    assert gold_ids == {"post_001", "post_003", "post_004", "post_005"}

    for row in result.to_dicts():
        parsed = json.loads(row["result_json"])
        assert parsed["is_educational"] is True
        assert parsed["is_actionable"] is True
        assert parsed["admirality"] == "B1"
        assert parsed["domain"] == "Business"

    with db.get_connection() as conn:
        dead = conn.execute(
            "SELECT post_id, error FROM dead_letter ORDER BY post_id"
        ).fetchall()
    assert len(dead) == 1
    assert dead[0][0] == "post_002"
    assert dead[0][1] == "Empty caption"

# ── Snapshot: serving ──────────────────────────────────────────────────────


def test_serving_snapshot(db, bronze_dir, gemini):
    """Serving output on frozen gold input matches expected values."""
    with patch("datalake.defs.instagram.assets.BRONZE_LAKE", bronze_dir):
        ig_posts_slv(build_asset_context(resources={"duckdb": db}))

    with patch.object(GeminiResource, "analyze",
                      return_value=json.dumps(FAKE_ANALYSIS)):
        ctx = build_asset_context(resources={"duckdb": db, "gemini": gemini})
        ig_posts_gld(ctx)

    serving_ctx = build_asset_context(resources={"duckdb": db})
    profile_dimension(serving_ctx)
    analytics_views(serving_ctx)

    with db.get_connection() as conn:
        profiles = conn.execute(
            "SELECT owner_id, owner_username, is_current "
            "FROM dim_profile ORDER BY owner_id, effective_from"
        ).fetchall()

        # 3 distinct owners — each gets 1 SCD2 row (no username changes here)
        assert len(profiles) == 3
        expected_profiles = {
            ("owner_1", "user1", True),
            ("owner_2", "user2", True),
            ("owner_3", "user3", True),
        }
        for row in profiles:
            assert (row[0], row[1], row[2]) in expected_profiles, f"Unexpected profile: {row}"

        views = conn.execute(
            "SELECT post_id, owner_username, owner_id, is_current "
            "FROM analytics_views ORDER BY post_id"
        ).fetchall()

    assert len(views) == 5
    assert views[0] == ("post_001", "user1", "owner_1", True)
    assert views[1] == ("post_002", "user1", "owner_1", True)
    assert views[2] == ("post_003", "user2", "owner_2", True)
