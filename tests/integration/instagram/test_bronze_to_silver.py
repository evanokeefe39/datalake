"""Integration tests: bronze Parquet files → silver DuckDB state.

Tests the cross-asset boundary between ``ig_posts_raw`` (bronze output) and
``ig_posts_slv`` (silver reader). Uses real DuckDB persistence and real Parquet
I/O — the only patch is ``BRONZE_LAKE`` pointing at ``tmp_path`` so we control
which files the silver asset discovers.

Per test-hardening plan Phase 2:
- Full schema match on round-trip
- Partial file read: one corrupt bronze, others valid → corrupt skipped
- Schema evolution: new Apify fields silently dropped by silver select
- Row count invariant: silver rows ≤ bronze rows
"""
import json
from unittest.mock import patch

import polars as pl
from dagster import build_asset_context
from dagster_duckdb import DuckDBResource

from datalake.defs.instagram.assets import ig_posts_slv

from tests.fixtures.ig_bronze_factories import make_ig_bronze_row, write_ig_bronze


# ── Test: full schema round-trip ──────────────────────────────────────────


def test_full_schema_round_trip(tmp_path):
    """GIVEN a bronze file with all Apify columns populated
    WHEN silver is processed
    THEN every silver column exists and has correct types.
    """
    row = make_ig_bronze_row(
        post_id="p1",
        shortcode="abc123",
        caption="Full schema test",
        username="test_user",
        owner_id="owner_1",
        likes=42,
        comments=7,
        hashtags=["ai", "ml"],
        timestamp="2024-06-01T12:00:00.000Z",
    )
    write_ig_bronze(tmp_path / "ds_001.parquet", [row])
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))

    with patch("datalake.defs.instagram.assets.BRONZE_LAKE", tmp_path):
        context = build_asset_context(resources={"duckdb": duckdb})
        result = ig_posts_slv(context)

    assert len(result) == 1
    row = result.row(0, named=True)
    # Core identity
    assert row["post_id"] == "p1"
    assert row["shortcode"] == "abc123"
    assert row["caption"] == "Full schema test"
    assert row["owner_username"] == "test_user"
    assert row["owner_id"] == "owner_1"
    # Numeric fields
    assert row["likes_count"] == 42
    assert row["comments_count"] == 7
    # JSON-serialized list
    assert json.loads(row["hashtags"]) == ["ai", "ml"]
    # Derived / schema columns
    assert row["source_dataset"] == "ds_001"
    assert row["processed_on"] is not None


# ── Test: corrupt bronze file is skipped ──────────────────────────────────


def test_corrupt_file_skipped(tmp_path, caplog):
    """GIVEN one corrupt bronze Parquet and one valid file
    WHEN silver is processed
    THEN corrupt file is logged and skipped; valid data still lands in silver.
    """
    import logging

    caplog.set_level(logging.WARNING)

    # Valid file
    write_ig_bronze(
        tmp_path / "good.parquet",
        [make_ig_bronze_row("p1", "abc", "Good post", "user1")],
    )
    # Corrupt file — write garbage bytes
    corrupt_path = tmp_path / "corrupt.parquet"
    corrupt_path.write_bytes(b"\x00\x00\x00CORRUPT PARQUET\xFF\xFF\xFF")

    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))

    with patch("datalake.defs.instagram.assets.BRONZE_LAKE", tmp_path):
        context = build_asset_context(resources={"duckdb": duckdb})
        result = ig_posts_slv(context)

    # Only the valid post landed in silver
    assert len(result) == 1
    assert result["post_id"][0] == "p1"

    # Corrupt file was logged as a warning
    warning_messages = [r.message for r in caplog.records if r.levelno == logging.WARNING]
    assert any("corrupt" in msg.lower() for msg in warning_messages)


# ── Test: extra Apify fields are silently dropped ─────────────────────────


def test_extra_bronze_columns_dropped(tmp_path):
    """GIVEN a bronze file with extra Apify columns not in the silver schema
    WHEN silver is processed
    THEN the extra columns are silently dropped — only silver columns survive.
    """
    rows = [make_ig_bronze_row("p1", "abc", "Post", "user1")]
    df = pl.DataFrame(rows)
    # Add extra columns that Apify might send but silver doesn't consume
    df = df.with_columns([
        pl.lit("extra").alias("apifyExtraField"),
        pl.lit(99.9).alias("someScore"),
        pl.lit(True).alias("isVerified"),
    ])
    df.write_parquet(tmp_path / "ds_extra.parquet")

    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))

    with patch("datalake.defs.instagram.assets.BRONZE_LAKE", tmp_path):
        context = build_asset_context(resources={"duckdb": duckdb})
        result = ig_posts_slv(context)

    assert len(result) == 1
    # Extra columns must not appear in output
    assert "apifyExtraField" not in result.columns
    assert "someScore" not in result.columns
    assert "isVerified" not in result.columns
    # Core columns are intact
    assert "post_id" in result.columns
    assert result["post_id"][0] == "p1"


# ── Test: row count invariant ─────────────────────────────────────────────


def test_silver_rows_leq_bronze_rows(tmp_path):
    """GIVEN multiple bronze files with overlapping post_ids
    WHEN silver is processed
    THEN silver row count ≤ total bronze row count (dedup guarantee).
    """
    # Three bronze files: 2 rows each, with 2 overlapping post_ids
    ds1 = [
        make_ig_bronze_row("1", "a", "Post 1 v1", "user1",
                        timestamp="2024-01-01T00:00:00.000Z"),
        make_ig_bronze_row("2", "b", "Post 2", "user1"),
    ]
    ds2 = [
        make_ig_bronze_row("1", "a", "Post 1 v2", "user1",
                        timestamp="2024-06-01T00:00:00.000Z"),
        make_ig_bronze_row("3", "c", "Post 3", "user2"),
    ]
    ds3 = [
        make_ig_bronze_row("2", "b", "Post 2 v2", "user1",
                        timestamp="2024-03-01T00:00:00.000Z"),
        make_ig_bronze_row("4", "d", "Post 4", "user3"),
    ]
    write_ig_bronze(tmp_path / "ds_001.parquet", ds1)
    write_ig_bronze(tmp_path / "ds_002.parquet", ds2)
    write_ig_bronze(tmp_path / "ds_003.parquet", ds3)

    total_bronze = len(ds1) + len(ds2) + len(ds3)  # 6

    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))

    with patch("datalake.defs.instagram.assets.BRONZE_LAKE", tmp_path):
        context = build_asset_context(resources={"duckdb": duckdb})
        result = ig_posts_slv(context)

    assert len(result) <= total_bronze
    # 6 bronze rows with 2 duplicates → 4 unique post_ids expected
    assert len(result) == 4
    assert set(result["post_id"].to_list()) == {"1", "2", "3", "4"}
