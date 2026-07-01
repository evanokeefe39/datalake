"""Tests for the ``ig_posts_slv`` silver asset.

Gap-fills per test-hardening plan:
- Dedup correctness, latest-dataset-wins, row count ≤ bronze, type coercion,
  processed_on stability, engagement updates, extra columns, watermark boundary
"""

from __future__ import annotations

import json
from unittest.mock import patch

import polars as pl
import pytest
from dagster import build_asset_context
from dagster_duckdb import DuckDBResource

from datalake.defs.instagram.assets import ig_posts_slv

from tests.fixtures.ig_bronze_factories import make_ig_bronze_row, write_ig_bronze


# ── Parametrized dedup scenarios ───────────────────────────────────────────


DEDUP_SCENARIOS = [
    pytest.param(
        [make_ig_bronze_row("1", "abc", "A", "u1")],
        ["1"],
        id="single_row",
    ),
    pytest.param(
        [
            make_ig_bronze_row("1", "abc", "Old", "u1", timestamp="2024-01-01T00:00:00.000Z"),
            make_ig_bronze_row("1", "abc", "New", "u1", timestamp="2024-06-01T00:00:00.000Z"),
        ],
        ["1"],
        id="same_file_dedup_latest_wins",
    ),
    pytest.param(
        [make_ig_bronze_row("1", "abc", "A", "u1"), make_ig_bronze_row("2", "def", "B", "u2")],
        ["1", "2"],
        id="unique_posts",
    ),
    pytest.param(
        [
            make_ig_bronze_row("1", "abc", "Null ts post", "u1", timestamp=None),
            make_ig_bronze_row("1", "abc", "Has ts", "u1"),
        ],
        ["1"],
        id="null_timestamp_dedup",
    ),
]

NO_DEDUP_SCENARIOS = [
    pytest.param(
        [make_ig_bronze_row("1", "abc", "Same", "u1"),
         make_ig_bronze_row("1", "abc", "Same", "u1"),
         make_ig_bronze_row("1", "abc", "Same", "u1")],
        ["1"],
        id="triplicate_same_file",
    ),
    pytest.param(
        [make_ig_bronze_row(None, "abc", "Null id", "u1"),
         make_ig_bronze_row("1", "abc", "Valid", "u1")],
        ["1"],
        id="null_id_filtered",
    ),
]


# ── Tests ──────────────────────────────────────────────────────────────────


def test_no_bronze_files(tmp_path):
    """Edge case: zero bronze files → returns empty DataFrame."""
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))

    with patch("datalake.defs.instagram.assets.BRONZE_LAKE", tmp_path):
        context = build_asset_context(resources={"duckdb": duckdb})
        result = ig_posts_slv(context)

    assert result.is_empty()


def test_empty_bronze_file(tmp_path):
    """Edge case: 0-row bronze file → logged and skipped."""
    write_ig_bronze(tmp_path / "ds_empty.parquet", [])
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))

    with patch("datalake.defs.instagram.assets.BRONZE_LAKE", tmp_path):
        context = build_asset_context(resources={"duckdb": duckdb})
        result = ig_posts_slv(context)

    assert result.is_empty()


@pytest.mark.parametrize("bronze_rows,expected_ids", DEDUP_SCENARIOS)
def test_dedup(tmp_path, bronze_rows, expected_ids):
    """Unique post_ids → all land in silver. Duplicates → latest wins."""
    write_ig_bronze(tmp_path / "ds_001.parquet", bronze_rows)
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))

    with patch("datalake.defs.instagram.assets.BRONZE_LAKE", tmp_path):
        context = build_asset_context(resources={"duckdb": duckdb})
        result = ig_posts_slv(context)

    assert set(result["post_id"].to_list()) == set(expected_ids)
    # Row count invariant: silver ≤ bronze
    assert len(result) <= len(bronze_rows)


@pytest.mark.parametrize("bronze_rows,expected_ids", NO_DEDUP_SCENARIOS)
def test_dedup_edge_cases(tmp_path, bronze_rows, expected_ids):
    """Edge cases: triplicate rows, null IDs filtered."""
    write_ig_bronze(tmp_path / "ds_001.parquet", bronze_rows)
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))

    with patch("datalake.defs.instagram.assets.BRONZE_LAKE", tmp_path):
        context = build_asset_context(resources={"duckdb": duckdb})
        result = ig_posts_slv(context)

    assert set(result["post_id"].to_list()) == set(expected_ids)


def test_dedup_across_datasets(tmp_path):
    """Same post_id in multiple bronze files → latest timestamp wins."""
    ds1 = [
        make_ig_bronze_row("1", "abc", "Old caption", "user1",
                        timestamp="2024-01-01T00:00:00.000Z"),
    ]
    ds2 = [
        make_ig_bronze_row("1", "abc", "New caption", "user1",
                        timestamp="2024-06-01T00:00:00.000Z"),
    ]
    write_ig_bronze(tmp_path / "ds_001.parquet", ds1)
    write_ig_bronze(tmp_path / "ds_002.parquet", ds2)
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))

    with patch("datalake.defs.instagram.assets.BRONZE_LAKE", tmp_path):
        context = build_asset_context(resources={"duckdb": duckdb})
        result = ig_posts_slv(context)

    assert len(result) == 1
    assert result["caption"][0] == "New caption"
    assert result["source_dataset"][0] == "ds_002"


def test_idempotent_no_new_files(tmp_path):
    """Second run with no new bronze → returns existing silver."""
    rows = [make_ig_bronze_row("1", "abc", "Post", "user1")]
    write_ig_bronze(tmp_path / "ds_001.parquet", rows)
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))

    with patch("datalake.defs.instagram.assets.BRONZE_LAKE", tmp_path):
        context = build_asset_context(resources={"duckdb": duckdb})
        r1 = ig_posts_slv(context)
        assert len(r1) == 1

        # Second run — no new bronze files
        r2 = ig_posts_slv(context)
        assert len(r2) == 1


def test_incremental_new_file(tmp_path):
    """New bronze file after first run → merged with existing silver."""
    write_ig_bronze(
        tmp_path / "ds_001.parquet",
        [make_ig_bronze_row("1", "abc", "First", "user1")],
    )
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))

    with patch("datalake.defs.instagram.assets.BRONZE_LAKE", tmp_path):
        context = build_asset_context(resources={"duckdb": duckdb})
        r1 = ig_posts_slv(context)
        assert len(r1) == 1

    # Add a second file
    write_ig_bronze(
        tmp_path / "ds_002.parquet",
        [make_ig_bronze_row("2", "def", "Second", "user2")],
    )
    with patch("datalake.defs.instagram.assets.BRONZE_LAKE", tmp_path):
        context = build_asset_context(resources={"duckdb": duckdb})
        r2 = ig_posts_slv(context)

    assert len(r2) == 2
    assert set(r2["post_id"].to_list()) == {"1", "2"}


def test_hashtags_serialized_to_json(tmp_path):
    """hashtags List(String) → serialized to JSON string in DuckDB."""
    rows = [
        make_ig_bronze_row("1", "abc", "Post", "user1",
                        hashtags=["ai", "startup", "marketing"]),
    ]
    write_ig_bronze(tmp_path / "ds_001.parquet", rows)
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))

    with patch("datalake.defs.instagram.assets.BRONZE_LAKE", tmp_path):
        context = build_asset_context(resources={"duckdb": duckdb})
        result = ig_posts_slv(context)

    db_val = result["hashtags"][0]
    assert json.loads(db_val) == ["ai", "startup", "marketing"]


def test_columns_renamed_and_derived(tmp_path):
    """Bronze camelCase → silver snake_case with derived fields."""
    rows = [make_ig_bronze_row("1", "abc123", "Check this", "test_user",
                            owner_id="test_owner", likes=42, comments=7,
                            hashtags=["ai"])]
    write_ig_bronze(tmp_path / "ds_001.parquet", rows)
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))

    with patch("datalake.defs.instagram.assets.BRONZE_LAKE", tmp_path):
        context = build_asset_context(resources={"duckdb": duckdb})
        result = ig_posts_slv(context)

    row = result.row(0, named=True)
    assert row["post_id"] == "1"
    assert row["shortcode"] == "abc123"
    assert row["owner_username"] == "test_user"
    assert row["owner_id"] == "test_owner"
    assert row["likes_count"] == 42
    assert row["comments_count"] == 7
    assert row["hashtags"] == '["ai"]'
    assert row["timestamp"] is not None
    assert row["processed_on"] is not None


def test_silver_ig_posts_upserted(tmp_path):
    """Bronze rows end up in silver_ig_posts DuckDB table."""
    rows = [make_ig_bronze_row("1", "abc", "Post", "user1")]
    write_ig_bronze(tmp_path / "ds_001.parquet", rows)
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))

    with patch("datalake.defs.instagram.assets.BRONZE_LAKE", tmp_path):
        context = build_asset_context(resources={"duckdb": duckdb})
        ig_posts_slv(context)

    with duckdb.get_connection() as conn:
        post = conn.execute(
            "SELECT post_id, caption, source_dataset FROM silver_ig_posts"
        ).fetchone()
    assert post == ("1", "Post", "ds_001")
