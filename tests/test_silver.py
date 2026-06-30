"""Tests for the ``ig_posts_slv`` silver asset.

Uses the real bronze Parquet schema (50+ cols, List(String) for hashtags)
to verify dedup, hashtags serialization, state tracking, and idempotency.
"""

from __future__ import annotations

import json
from unittest.mock import patch

import polars as pl
from dagster import build_asset_context
from dagster_duckdb import DuckDBResource

from datalake.defs.instagram.assets import ig_posts_slv

# ── Helpers ───────────────────────────────────────────────────────────────


def _write_bronze(path, rows: list[dict]) -> None:
    """Write a bronze Parquet file with real Apify-like schema."""
    df = pl.DataFrame(rows)
    df.write_parquet(path)


def _make_bronze_row(
    post_id: str,
    shortcode: str,
    caption: str,
    username: str,
    owner_id: str = "12345",
    likes: int = 10,
    comments: int = 2,
    hashtags: list[str] | None = None,
    timestamp: str = "2024-01-01T00:00:00.000Z",
) -> dict:
    """Create a row with real Apify bronze columns (camelCase, List fields)."""
    return {
        "id": post_id,
        "shortCode": shortcode,
        "caption": caption,
        "ownerUsername": username,
        "ownerId": owner_id,
        "likesCount": likes,
        "commentsCount": comments,
        "timestamp": timestamp,
        "hashtags": hashtags or [],
        "url": f"https://www.instagram.com/p/{shortcode}/",
        "mentions": [],
        "type": "Video",
        "latestComments": [],
        "taggedUsers": [],
        "videoViewCount": 0,
        "videoPlayCount": 0,
        "ownerFullName": f"{username} Full Name",
        "displayUrl": f"https://example.com/{shortcode}.jpg",
        "dimensionsHeight": 1080,
        "dimensionsWidth": 1080,
        "inputUrl": "https://www.instagram.com/testprofile/",
        "firstComment": "",
        "commentsDisabled": False,
        "productType": "feed",
        "isPinned": False,
        "isSponsored": False,
    }


# ── Tests ─────────────────────────────────────────────────────────────────


def test_no_bronze_files(tmp_path):
    """Edge case: zero bronze files → returns empty DataFrame."""
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))

    with patch("datalake.defs.instagram.assets.BRONZE_LAKE", tmp_path):
        context = build_asset_context(resources={"duckdb": duckdb})
        result = ig_posts_slv(context)

    assert len(result) == 0


def test_empty_bronze_file(tmp_path):
    """Edge case: 0-row bronze file → logged and skipped."""
    _write_bronze(tmp_path / "ds_empty.parquet", [])
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))

    with patch("datalake.defs.instagram.assets.BRONZE_LAKE", tmp_path):
        context = build_asset_context(resources={"duckdb": duckdb})
        result = ig_posts_slv(context)

    assert len(result) == 0


def test_simple_dedup(tmp_path):
    """Unique post_ids → all land in silver."""
    rows = [
        _make_bronze_row("1", "abc", "Post A", "user1"),
        _make_bronze_row("2", "def", "Post B", "user2"),
        _make_bronze_row("3", "ghi", "Post C", "user3"),
    ]
    _write_bronze(tmp_path / "ds_001.parquet", rows)
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))

    with patch("datalake.defs.instagram.assets.BRONZE_LAKE", tmp_path):
        context = build_asset_context(resources={"duckdb": duckdb})
        result = ig_posts_slv(context)

    assert len(result) == 3
    assert set(result["post_id"].to_list()) == {"1", "2", "3"}

    with duckdb.get_connection() as conn:
        silver_count = conn.execute(
            "SELECT COUNT(*) FROM silver_posts"
        ).fetchone()[0]
        progress = conn.execute(
            "SELECT source_dataset, post_count FROM silver_progress"
        ).fetchall()

    assert silver_count == 3
    assert ("ds_001", 3) in progress


def test_hashtags_serialized_to_json(tmp_path):
    """hashtags List(String) → serialized to JSON string in DuckDB."""
    rows = [
        _make_bronze_row("1", "abc", "Post", "user1",
                         hashtags=["ai", "startup", "marketing"]),
        _make_bronze_row("2", "def", "Post 2", "user1", hashtags=[]),
    ]
    _write_bronze(tmp_path / "ds_001.parquet", rows)
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))

    with patch("datalake.defs.instagram.assets.BRONZE_LAKE", tmp_path):
        context = build_asset_context(resources={"duckdb": duckdb})
        result = ig_posts_slv(context)

    assert len(result) == 2
    assert result["hashtags"].dtype == pl.Utf8

    hashtag_list = result["hashtags"].to_list()
    assert json.loads(hashtag_list[0]) == ["ai", "startup", "marketing"]
    assert json.loads(hashtag_list[1]) == []

    # Verify DuckDB stored them as TEXT
    with duckdb.get_connection() as conn:
        db_val = conn.execute(
            "SELECT hashtags FROM silver_posts WHERE post_id = '1'"
        ).fetchone()[0]
    assert isinstance(db_val, str)
    assert json.loads(db_val) == ["ai", "startup", "marketing"]


def test_dedup_across_datasets(tmp_path):
    """Same post_id in multiple bronze files → latest timestamp wins."""
    ds1 = [_make_bronze_row("1", "abc", "Old caption", "user1",
                            timestamp="2024-01-01T00:00:00.000Z")]
    ds2 = [_make_bronze_row("1", "abc", "New caption", "user1",
                            timestamp="2024-02-01T00:00:00.000Z")]
    _write_bronze(tmp_path / "ds_001.parquet", ds1)
    _write_bronze(tmp_path / "ds_002.parquet", ds2)
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))

    with patch("datalake.defs.instagram.assets.BRONZE_LAKE", tmp_path):
        context = build_asset_context(resources={"duckdb": duckdb})
        result = ig_posts_slv(context)

    assert len(result) == 1
    assert result["caption"][0] == "New caption"
    assert result["source_dataset"][0] == "ds_002"


def test_idempotent_no_new_files(tmp_path):
    """Second run with no new bronze → returns existing silver."""
    rows = [_make_bronze_row("1", "abc", "Post", "user1")]
    _write_bronze(tmp_path / "ds_001.parquet", rows)
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))

    with patch("datalake.defs.instagram.assets.BRONZE_LAKE", tmp_path):
        context = build_asset_context(resources={"duckdb": duckdb})
        result1 = ig_posts_slv(context)
        assert len(result1) == 1

        result2 = ig_posts_slv(context)
        assert len(result2) == 1
        assert result2["post_id"][0] == "1"

    with duckdb.get_connection() as conn:
        progress = conn.execute(
            "SELECT COUNT(*) FROM silver_progress"
        ).fetchone()[0]
    assert progress == 1


def test_incremental_new_file(tmp_path):
    """New bronze file after first run → merged with existing silver."""
    _write_bronze(
        tmp_path / "ds_001.parquet",
        [_make_bronze_row("1", "abc", "Post A", "user1")],
    )
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))

    with patch("datalake.defs.instagram.assets.BRONZE_LAKE", tmp_path):
        context = build_asset_context(resources={"duckdb": duckdb})
        result1 = ig_posts_slv(context)
        assert len(result1) == 1

        _write_bronze(
            tmp_path / "ds_002.parquet",
            [_make_bronze_row("2", "def", "Post B", "user2")],
        )
        result2 = ig_posts_slv(context)

    assert len(result2) == 2
    assert set(result2["post_id"].to_list()) == {"1", "2"}

    with duckdb.get_connection() as conn:
        tracked = {
            row[0]
            for row in conn.execute(
                "SELECT source_dataset FROM silver_progress"
            ).fetchall()
        }
    assert tracked == {"ds_001", "ds_002"}


def test_dedup_within_same_dataset(tmp_path):
    """Duplicate post_id in one file → DISTINCT ON picks the latest."""
    rows = [
        _make_bronze_row("1", "abc", "First", "user1",
                         timestamp="2024-01-01T00:00:00.000Z"),
        _make_bronze_row("1", "abc", "Second", "user1",
                         timestamp="2024-01-01T00:00:00.000Z"),
    ]
    _write_bronze(tmp_path / "ds_001.parquet", rows)
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))

    with patch("datalake.defs.instagram.assets.BRONZE_LAKE", tmp_path):
        context = build_asset_context(resources={"duckdb": duckdb})
        result = ig_posts_slv(context)

    assert len(result) == 1


def test_null_id_filtered(tmp_path):
    """Rows with null id (failed Apify requests) are filtered out."""
    rows = [
        _make_bronze_row("1", "abc", "Valid", "user1"),
        {"id": None, "shortCode": None, "caption": None,
         "ownerUsername": None, "likesCount": 0, "commentsCount": 0,
         "timestamp": None, "hashtags": None, "url": None,
         "mentions": [], "type": None, "error": "Request failed"},
    ]
    _write_bronze(tmp_path / "ds_001.parquet", rows)
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))

    with patch("datalake.defs.instagram.assets.BRONZE_LAKE", tmp_path):
        context = build_asset_context(resources={"duckdb": duckdb})
        result = ig_posts_slv(context)

    assert len(result) == 1
    assert "1" in result["post_id"].to_list()


def test_columns_renamed_and_derived(tmp_path):
    """Bronze camelCase → silver snake_case with derived fields."""
    rows = [_make_bronze_row("1", "abc123", "Check this", "test_user",
                              owner_id="test_owner", likes=42, comments=7,
                              timestamp="2024-06-15T12:00:00.000Z")]
    _write_bronze(tmp_path / "ds_001.parquet", rows)
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))

    with patch("datalake.defs.instagram.assets.BRONZE_LAKE", tmp_path):
        context = build_asset_context(resources={"duckdb": duckdb})
        result = ig_posts_slv(context)

    assert len(result) == 1
    assert result["post_id"][0] == "1"
    assert result["shortcode"][0] == "abc123"
    assert result["owner_username"][0] == "test_user"
    assert result["owner_id"][0] == "test_owner"
    assert result["likes_count"][0] == 42
    assert result["comments_count"][0] == 7
    assert result["source_dataset"][0] == "ds_001"
    assert result["silvered_at"][0] is not None


def test_silver_posts_upserted(tmp_path):
    """Bronze rows end up in silver_posts DuckDB table."""
    rows = [_make_bronze_row("1", "abc", "Post", "user1")]
    _write_bronze(tmp_path / "ds_001.parquet", rows)
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))

    with patch("datalake.defs.instagram.assets.BRONZE_LAKE", tmp_path):
        context = build_asset_context(resources={"duckdb": duckdb})
        ig_posts_slv(context)

    with duckdb.get_connection() as conn:
        post = conn.execute(
            "SELECT post_id, caption, source_dataset FROM silver_posts"
        ).fetchone()

    assert post == ("1", "Post", "ds_001")


def test_null_timestamp(tmp_path):
    """Null timestamps → NULLS LAST ordering still dedups."""
    rows = [_make_bronze_row("1", "abc", "Post", "user1", timestamp=None)]
    _write_bronze(tmp_path / "ds_001.parquet", rows)
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))

    with patch("datalake.defs.instagram.assets.BRONZE_LAKE", tmp_path):
        context = build_asset_context(resources={"duckdb": duckdb})
        result = ig_posts_slv(context)

    assert len(result) == 1
    assert result["post_id"][0] == "1"
