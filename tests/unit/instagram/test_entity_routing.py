"""Entity classification, multi-entity bronze routing, and config validation.

Covers ISSUES #10 (entity-aware routing): the classifier (US-01), the silver
guard that skips non-post files (US-02), the profiles/comments assets
(US-03/US-04), and the ScrapeConfig results_type enum (US-09).
"""

from __future__ import annotations

import json
from unittest.mock import patch

import polars as pl
import pytest
from dagster import build_asset_context
from dagster_duckdb import DuckDBResource
from pydantic import ValidationError

from datalake.defs.instagram.assets import (
    _classify_bronze,
    ig_comments_slv,
    ig_posts_slv,
    ig_profiles_slv,
)
from datalake.defs.instagram.config import ResultsType, ScrapeConfig
from tests.fixtures.ig_bronze_factories import make_ig_bronze_row, write_ig_bronze

# ── Classifier (US-01) ───────────────────────────────────────────────────


def _df(columns: list[str], rows: int = 1) -> pl.DataFrame:
    return pl.DataFrame({c: [f"{c}_{i}" for i in range(rows)] for c in columns})


def _details_df(rows: int = 1) -> pl.DataFrame:
    """A details-shaped bronze DataFrame (profile scrape), no post ids."""
    return pl.DataFrame(
        {
            "ownerId": [f"own_{i}" for i in range(rows)],
            "username": [f"user_{i}" for i in range(rows)],
            "fullName": [f"Full {i}" for i in range(rows)],
            "biography": [f"bio {i}" for i in range(rows)],
            "followersCount": [100 + i for i in range(rows)],
            "followsCount": [50 + i for i in range(rows)],
            "postsCount": [10 + i for i in range(rows)],
            "isBusinessAccount": [False] * rows,
            "isVerified": [False] * rows,
            "profilePicUrlHD": [None] * rows,
            "externalUrl": [None] * rows,
        }
    )


def test_classifier_posts():
    df = _df(["id", "shortCode", "caption"])
    assert _classify_bronze(df, None) == "posts"


def test_classifier_details():
    df = _df(["biography", "followersCount", "profilePicUrlHD"])
    assert _classify_bronze(df, None) == "details"


def test_classifier_comments():
    df = _df(["commentId", "text", "ownerUsername"])
    assert _classify_bronze(df, None) == "comments"


def test_classifier_unknown():
    df = _df(["someCol", "otherCol"])
    assert _classify_bronze(df, None) == "unknown"


def test_classifier_empty_file():
    df = pl.DataFrame({"id": [], "shortCode": []})
    assert _classify_bronze(df, None) == "unknown"


def test_classifier_meta_priority(tmp_path):
    """Meta sidecar results_type wins over schema sniffing."""
    meta_path = tmp_path / "ds.parquet.meta"
    meta_path.write_text(json.dumps({"input": {"results_type": "details"}}))
    df = _df(["id", "shortCode"])  # post-shaped, but meta says details
    assert _classify_bronze(df, meta_path) == "details"


def test_classifier_meta_fallback_when_field_absent(tmp_path):
    """Meta without results_type falls back to schema sniffing."""
    meta_path = tmp_path / "ds.parquet.meta"
    meta_path.write_text(json.dumps({"input": {"urls": ["https://x"]}}))
    df = _df(["id", "shortCode"])
    assert _classify_bronze(df, meta_path) == "posts"


# ── ScrapeConfig enum (US-09) ────────────────────────────────────────────


def test_scrape_config_valid_types():
    cfg = ScrapeConfig(
        urls=["https://instagram.com/x"], results_type=ResultsType.DETAILS
    )
    assert cfg.results_type == ResultsType.DETAILS


def test_scrape_config_default_posts():
    cfg = ScrapeConfig(urls=["https://instagram.com/x"])
    assert cfg.results_type == ResultsType.POSTS


def test_scrape_config_invalid_type_rejected():
    with pytest.raises(ValidationError):
        ScrapeConfig(urls=["https://instagram.com/x"], results_type="storks")


# ── Silver guard: skip non-post bronze (US-02) ───────────────────────────


def test_slv_skips_profile_bronze(tmp_path):
    """Details-shaped bronze (the o44 failure mode) is skipped, not loaded."""
    _details_df(rows=3).write_parquet(tmp_path / "ds_profile.parquet")
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))

    with patch("datalake.defs.instagram.assets.BRONZE_LAKE", tmp_path):
        context = build_asset_context(resources={"duckdb": duckdb})
        result = ig_posts_slv(context)

    assert result.is_empty()


def test_slv_skips_comment_bronze(tmp_path):
    """Comment-shaped bronze is skipped."""
    _df(["commentId", "text", "ownerUsername"], rows=2).write_parquet(
        tmp_path / "ds_comments.parquet"
    )
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))

    with patch("datalake.defs.instagram.assets.BRONZE_LAKE", tmp_path):
        context = build_asset_context(resources={"duckdb": duckdb})
        result = ig_posts_slv(context)

    assert result.is_empty()


# ── Profiles asset (US-03) ───────────────────────────────────────────────


def test_profiles_slv_upsert(tmp_path):
    """Details-type bronze upserts rows into silver_ig_profiles."""
    _details_df(rows=2).write_parquet(tmp_path / "ds_profile.parquet")
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))

    with patch("datalake.defs.instagram.assets.BRONZE_LAKE", tmp_path):
        context = build_asset_context(resources={"duckdb": duckdb})
        result = ig_profiles_slv(context)

    assert len(result) == 2
    assert set(result["owner_id"].to_list()) == {"own_0", "own_1"}

    with duckdb.get_connection() as conn:
        count = conn.execute("SELECT COUNT(*) FROM silver_ig_profiles").fetchone()[0]
    assert count == 2


def test_profiles_slv_no_bronze(tmp_path):
    """Zero bronze files → empty DataFrame."""
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))

    with patch("datalake.defs.instagram.assets.BRONZE_LAKE", tmp_path):
        context = build_asset_context(resources={"duckdb": duckdb})
        result = ig_profiles_slv(context)

    assert result.is_empty()


def test_profiles_slv_extracts_from_post_bronze(tmp_path):
    """Post-shaped bronze also yields profiles (author fields on every row)."""
    rows = [
        make_ig_bronze_row("1", "abc", "Post A", "user1", owner_id="own_1"),
        make_ig_bronze_row("2", "def", "Post B", "user1", owner_id="own_1"),
        make_ig_bronze_row("3", "ghi", "Post C", "user2", owner_id="own_2"),
    ]
    write_ig_bronze(tmp_path / "ds_posts.parquet", rows)
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))

    with patch("datalake.defs.instagram.assets.BRONZE_LAKE", tmp_path):
        context = build_asset_context(resources={"duckdb": duckdb})
        result = ig_profiles_slv(context)

    # 3 posts, 2 distinct authors → 2 profiles (deduped by owner_id)
    assert set(result["owner_id"].to_list()) == {"own_1", "own_2"}
    assert set(result["owner_username"].to_list()) == {"user1", "user2"}

# ── Comments stub (US-04) ────────────────────────────────────────────────


def test_comments_slv_returns_empty_and_creates_table(tmp_path):
    """Stub returns empty DataFrame and ensures the table exists."""
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))
    context = build_asset_context(resources={"duckdb": duckdb})
    result = ig_comments_slv(context)

    assert result.is_empty()

    with duckdb.get_connection() as conn:
        count = conn.execute("SELECT COUNT(*) FROM silver_ig_comments").fetchone()[0]
    assert count == 0
