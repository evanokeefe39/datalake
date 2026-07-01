"""Unit tests for Instagram-layer asset checks.

Each test exercises a single ``@asset_check`` function, validating both
pass and fail (where applicable) paths.
"""

from __future__ import annotations

import json
from unittest.mock import patch

import polars as pl
import pytest
from dagster import build_asset_check_context
from dagster_duckdb import DuckDBResource

from datalake.defs.instagram.asset_checks import (
    ig_checks,
)
from tests.fixtures.ig_bronze_factories import make_ig_bronze_row, write_ig_bronze
from tests.fixtures.gold_factories import FAKE_ANALYSIS

# ── Resolve individual check functions by name ─────────────────────────────

_CHECKS_BY_NAME = {list(c.check_keys)[0].name: c for c in ig_checks}


@pytest.fixture
def duckdb(tmp_path):
    return DuckDBResource(database=str(tmp_path / "state.duckdb"))


# ===== Bronze checks =======================================================


class TestBronzeChecks:
    """Tests for ``ig_posts_raw_*`` checks."""

    def test_has_rows_passes(self, tmp_path):
        """GIVEN a bronze Parquet with 2 rows
        WHEN the check runs
        THEN it passes with row_count metadata.
        """
        write_ig_bronze(tmp_path / "ds_001.parquet", [
            make_ig_bronze_row("p1", "abc", "Post 1", "u1"),
            make_ig_bronze_row("p2", "def", "Post 2", "u2"),
        ])
        with patch("datalake.defs.instagram.asset_checks.BRONZE_LAKE", tmp_path):
            check = _CHECKS_BY_NAME["ig_posts_raw_has_rows"]
            result = check()
        assert result.passed is True
        assert result.metadata["row_count"].value == 2

    def test_has_rows_fails_empty(self, tmp_path):
        """GIVEN no bronze Parquet files
        WHEN the check runs
        THEN it fails.
        """
        with patch("datalake.defs.instagram.asset_checks.BRONZE_LAKE", tmp_path):
            check = _CHECKS_BY_NAME["ig_posts_raw_has_rows"]
            result = check()
        assert result.passed is False

    def test_has_meta_passes(self, tmp_path):
        """GIVEN a bronze Parquet with valid .meta sidecar
        WHEN the check runs
        THEN it passes.
        """
        p = tmp_path / "ds_001.parquet"
        write_ig_bronze(p, [make_ig_bronze_row("p1", "abc", "Post", "u1")])
        meta_path = p.with_suffix(".parquet.meta")
        meta_path.write_text(json.dumps({
            "run_id": "run_1", "actor": "test", "item_count": 1,
            "downloaded_at": "2024-01-01T00:00:00Z",
        }))
        with patch("datalake.defs.instagram.asset_checks.BRONZE_LAKE", tmp_path):
            check = _CHECKS_BY_NAME["ig_posts_raw_has_meta"]
            result = check()
        assert result.passed is True

    def test_has_meta_fails_missing(self, tmp_path):
        """GIVEN bronze Parquet without .meta sidecar
        WHEN the check runs
        THEN it fails.
        """
        write_ig_bronze(tmp_path / "ds_001.parquet", [
            make_ig_bronze_row("p1", "abc", "Post", "u1"),
        ])
        with patch("datalake.defs.instagram.asset_checks.BRONZE_LAKE", tmp_path):
            check = _CHECKS_BY_NAME["ig_posts_raw_has_meta"]
            result = check()
        assert result.passed is False

    def test_run_id_not_null_passes(self, tmp_path):
        """GIVEN bronze Parquet with all rows having non-null 'id'
        WHEN the check runs
        THEN it passes.
        """
        write_ig_bronze(tmp_path / "ds_001.parquet", [
            make_ig_bronze_row("p1", "abc", "Post", "u1"),
        ])
        with patch("datalake.defs.instagram.asset_checks.BRONZE_LAKE", tmp_path):
            check = _CHECKS_BY_NAME["ig_posts_raw_run_id_not_null"]
            result = check()
        assert result.passed is True

    def test_run_id_not_null_fails_with_null(self, tmp_path):
        """GIVEN bronze Parquet with a row having null 'id'
        WHEN the check runs
        THEN it fails.
        """
        df = pl.DataFrame({
            "id": [None],
            "shortCode": ["abc"],
        })
        df.write_parquet(tmp_path / "ds_bad.parquet")
        with patch("datalake.defs.instagram.asset_checks.BRONZE_LAKE", tmp_path):
            check = _CHECKS_BY_NAME["ig_posts_raw_run_id_not_null"]
            result = check()
        assert result.passed is False


# ===== Silver checks =======================================================


class TestSilverChecks:
    """Tests for ``ig_posts_slv_*`` checks."""

    def test_no_duplicates_passes(self, duckdb):
        """GIVEN silver_ig_posts with unique post_ids
        WHEN the check runs
        THEN it passes.
        """
        with duckdb.get_connection() as conn:
            conn.execute("""
                CREATE TABLE silver_ig_posts (
                    post_id TEXT PRIMARY KEY, caption TEXT
                )
            """)
            conn.execute("INSERT INTO silver_ig_posts VALUES ('p1', 'Post 1')")
            conn.execute("INSERT INTO silver_ig_posts VALUES ('p2', 'Post 2')")

        ctx = build_asset_check_context(resources={"duckdb": duckdb})
        check = _CHECKS_BY_NAME["ig_posts_slv_no_duplicates"]
        result = check(ctx)
        assert result.passed is True

    def test_no_duplicates_fails(self, duckdb):
        """GIVEN silver_ig_posts with duplicate post_ids
        WHEN the check runs
        THEN it fails.
        """
        with duckdb.get_connection() as conn:
            conn.execute("""
                CREATE TABLE silver_ig_posts (
                    post_id TEXT, caption TEXT
                )
            """)
            conn.execute("INSERT INTO silver_ig_posts VALUES ('p1', 'Post 1')")
            conn.execute("INSERT INTO silver_ig_posts VALUES ('p1', 'Post 1 dup')")

        ctx = build_asset_check_context(resources={"duckdb": duckdb})
        check = _CHECKS_BY_NAME["ig_posts_slv_no_duplicates"]
        result = check(ctx)
        assert result.passed is False

    def test_row_count_bounded_passes(self, duckdb, tmp_path):
        """GIVEN silver rows ≤ bronze rows
        WHEN the check runs
        THEN it passes.
        """
        write_ig_bronze(tmp_path / "ds_001.parquet", [
            make_ig_bronze_row("p1", "abc", "Post", "u1"),
            make_ig_bronze_row("p2", "def", "Post", "u2"),
        ])
        with duckdb.get_connection() as conn:
            conn.execute("""
                CREATE TABLE silver_ig_posts (
                    post_id TEXT PRIMARY KEY, caption TEXT
                )
            """)
            conn.execute("INSERT INTO silver_ig_posts VALUES ('p1', 'Post 1')")

        with patch("datalake.defs.instagram.asset_checks.BRONZE_LAKE", tmp_path):
            ctx = build_asset_check_context(resources={"duckdb": duckdb})
            check = _CHECKS_BY_NAME["ig_posts_slv_row_count_bounded"]
            result = check(ctx)
        assert result.passed is True
        assert result.metadata["silver_rows"].value == 1
        assert result.metadata["bronze_rows"].value == 2


# ===== Gold checks =========================================================


class TestGoldChecks:
    """Tests for ``ig_posts_gld_*`` checks."""

    @pytest.fixture(autouse=True)
    def _setup_gold_table(self, duckdb):
        with duckdb.get_connection() as conn:
            conn.execute("""
                CREATE TABLE gold_ig_analyses (
                    post_id TEXT PRIMARY KEY,
                    schema_version INTEGER NOT NULL DEFAULT 3,
                    result_json TEXT,
                    analysed_at TIMESTAMP
                )
            """)
        yield

    def test_valid_admiralty_passes(self, duckdb):
        """GIVEN all gold rows have valid admiralty codes
        WHEN the check runs
        THEN it passes.
        """
        with duckdb.get_connection() as conn:
            conn.execute(
                "INSERT INTO gold_ig_analyses (post_id, result_json) VALUES (?, ?)",
                ["p1", json.dumps(FAKE_ANALYSIS)],
            )

        ctx = build_asset_check_context(resources={"duckdb": duckdb})
        check = _CHECKS_BY_NAME["ig_posts_gld_valid_admiralty"]
        result = check(ctx)
        assert result.passed is True

    def test_valid_admiralty_fails(self, duckdb):
        """GIVEN a gold row with invalid admiralty code
        WHEN the check runs
        THEN it fails.
        """
        bad = dict(FAKE_ANALYSIS)
        bad["admirality"] = "Z9"
        with duckdb.get_connection() as conn:
            conn.execute(
                "INSERT INTO gold_ig_analyses (post_id, result_json) VALUES (?, ?)",
                ["p1", json.dumps(bad)],
            )

        ctx = build_asset_check_context(resources={"duckdb": duckdb})
        check = _CHECKS_BY_NAME["ig_posts_gld_valid_admiralty"]
        result = check(ctx)
        assert result.passed is False

    def test_valid_json_passes(self, duckdb):
        """GIVEN gold rows with valid educational_json and actionable_json
        WHEN the check runs
        THEN it passes.
        """
        with duckdb.get_connection() as conn:
            conn.execute(
                "INSERT INTO gold_ig_analyses (post_id, result_json) VALUES (?, ?)",
                ["p1", json.dumps(FAKE_ANALYSIS)],
            )

        ctx = build_asset_check_context(resources={"duckdb": duckdb})
        check = _CHECKS_BY_NAME["ig_posts_gld_valid_json"]
        result = check(ctx)
        assert result.passed is True

    def test_valid_json_fails_missing_educational(self, duckdb):
        """GIVEN a gold row without educational_json
        WHEN the check runs
        THEN it fails.
        """
        bad = dict(FAKE_ANALYSIS)
        del bad["educational_json"]
        with duckdb.get_connection() as conn:
            conn.execute(
                "INSERT INTO gold_ig_analyses (post_id, result_json) VALUES (?, ?)",
                ["p1", json.dumps(bad)],
            )

        ctx = build_asset_check_context(resources={"duckdb": duckdb})
        check = _CHECKS_BY_NAME["ig_posts_gld_valid_json"]
        result = check(ctx)
        assert result.passed is False

    def test_schema_version_current_passes(self, duckdb):
        """GIVEN all gold rows have schema_version = 3
        WHEN the check runs
        THEN it passes.
        """
        with duckdb.get_connection() as conn:
            conn.execute(
                "INSERT INTO gold_ig_analyses (post_id, result_json) VALUES (?, ?)",
                ["p1", json.dumps(FAKE_ANALYSIS)],
            )

        ctx = build_asset_check_context(resources={"duckdb": duckdb})
        check = _CHECKS_BY_NAME["ig_posts_gld_schema_version_current"]
        result = check(ctx)
        assert result.passed is True

    def test_schema_version_current_fails(self, duckdb):
        """GIVEN a gold row with stale schema_version
        WHEN the check runs
        THEN it fails.
        """
        with duckdb.get_connection() as conn:
            conn.execute(
                "INSERT INTO gold_ig_analyses "
                "(post_id, schema_version, result_json) VALUES (?, ?, ?)",
                ["p1", 2, json.dumps(FAKE_ANALYSIS)],
            )

        ctx = build_asset_check_context(resources={"duckdb": duckdb})
        check = _CHECKS_BY_NAME["ig_posts_gld_schema_version_current"]
        result = check(ctx)
        assert result.passed is False
