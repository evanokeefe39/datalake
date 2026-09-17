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
from orchestration.defs.ig_core.slv.checks import (
    ig_checks,
)

from tests.fixtures.ig_bronze_factories import make_ig_bronze_row, write_ig_bronze

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
        with patch("orchestration.defs.ig_core.slv.checks.BRONZE_LAKE", tmp_path):
            check = _CHECKS_BY_NAME["ig_posts_raw_has_rows"]
            result = check()
        assert result.passed is True
        assert result.metadata["row_count"].value == 2

    def test_has_rows_fails_empty(self, tmp_path):
        """GIVEN no bronze Parquet files
        WHEN the check runs
        THEN it fails.
        """
        with patch("orchestration.defs.ig_core.slv.checks.BRONZE_LAKE", tmp_path):
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
            "dataset_id": "ds_001",
            "input": {
                "urls": ["https://instagram.com/test"],
                "results_limit": 12,
                "results_type": "posts",
            },
            "downloaded_at": "2024-01-01T00:00:00Z",
        }))
        with patch("orchestration.defs.ig_core.slv.checks.BRONZE_LAKE", tmp_path):
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
        with patch("orchestration.defs.ig_core.slv.checks.BRONZE_LAKE", tmp_path):
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
        with patch("orchestration.defs.ig_core.slv.checks.BRONZE_LAKE", tmp_path):
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
        with patch("orchestration.defs.ig_core.slv.checks.BRONZE_LAKE", tmp_path):
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

        with patch("orchestration.defs.ig_core.slv.checks.BRONZE_LAKE", tmp_path):
            ctx = build_asset_check_context(resources={"duckdb": duckdb})
            check = _CHECKS_BY_NAME["ig_posts_slv_row_count_bounded"]
            result = check(ctx)
        assert result.passed is True
        assert result.metadata["silver_rows"].value == 1
        assert result.metadata["bronze_rows"].value == 2


# ===== Classification checks ===============================================


class TestClassificationChecks:
    """Tests for the silver-backed admiralty check (W9: the retired
    ``gold_analyses`` checks — ``ig_posts_gld_valid_admiralty`` /
    ``ig_posts_gld_valid_json`` — were replaced / retired respectively;
    the JSON-shape guard is enforced by conform's typed columns)."""

    @pytest.fixture(autouse=True)
    def _setup_classification_table(self, duckdb):
        from orchestration.defs.platform.schemas import duckdb_ddl

        with duckdb.get_connection() as conn:
            conn.execute(duckdb_ddl("silver_content_classification"))
        yield

    def test_valid_admiralty_passes(self, duckdb):
        """GIVEN a classification row with a valid admiralty code
        WHEN the check runs
        THEN it passes.
        """
        with duckdb.get_connection() as conn:
            conn.execute(
                "INSERT INTO silver_content_classification "
                "(post_id, platform, admiralty) VALUES (?, 'instagram', ?)",
                ["p1", "A1"],
            )

        ctx = build_asset_check_context(resources={"duckdb": duckdb})
        check = _CHECKS_BY_NAME["ig_classification_valid_admiralty"]
        result = check(ctx)
        assert result.passed is True

    def test_invalid_admiralty_fails(self, duckdb):
        """GIVEN a classification row with an invalid admiralty code
        WHEN the check runs
        THEN it fails.
        """
        with duckdb.get_connection() as conn:
            conn.execute(
                "INSERT INTO silver_content_classification "
                "(post_id, platform, admiralty) VALUES (?, 'instagram', ?)",
                ["p1", "Z9"],
            )

        ctx = build_asset_check_context(resources={"duckdb": duckdb})
        check = _CHECKS_BY_NAME["ig_classification_valid_admiralty"]
        result = check(ctx)
        assert result.passed is False

    def test_null_admiralty_reported_not_failed(self, duckdb):
        """GIVEN a row with NULL admiralty (not yet classified)
        WHEN the check runs
        THEN it passes but reports the null count.
        """
        with duckdb.get_connection() as conn:
            conn.execute(
                "INSERT INTO silver_content_classification "
                "(post_id, platform) VALUES (?, 'instagram')", ["p1"],
            )

        ctx = build_asset_check_context(resources={"duckdb": duckdb})
        check = _CHECKS_BY_NAME["ig_classification_valid_admiralty"]
        result = check(ctx)
        assert result.passed is True
        assert result.metadata["null_admiralty"].value == 1


# ===== Observation freshness (US-DISC-7 AC 12) ==============================


class TestObservationFreshness:
    """Tests for ``ig_observation_freshness``.

    Freshness is counted as DISTINCT datasets, never as rows: a dataset assigns
    one observation and a replay appends nothing (INSERT OR IGNORE), so a row
    count would report freshness that never happened. These tests also exercise
    the check's interval arithmetic, which has no other coverage.
    """

    def _obs(self, duckdb, dataset: str, age_days: int, post: str | None = None) -> None:
        from orchestration.defs.platform.schemas import duckdb_ddl

        with duckdb.get_connection() as conn:
            conn.execute(duckdb_ddl("silver_ig_post_observations"))
            conn.execute(
                "INSERT INTO silver_ig_post_observations "
                "(post_id, observed_at, source_dataset) "
                "VALUES (?, now() - INTERVAL (?) DAY, ?)",
                [post or f"p_{dataset}_{age_days}", age_days, dataset],
            )

    def _run(self, duckdb):
        ctx = build_asset_check_context(resources={"duckdb": duckdb})
        return _CHECKS_BY_NAME["ig_observation_freshness"](ctx)

    def test_fresh_dataset_passes(self, duckdb):
        """GIVEN an observation dataset landed today
        WHEN the check runs
        THEN it passes and reports one fresh dataset.
        """
        self._obs(duckdb, "ds_today", 0)
        result = self._run(duckdb)
        assert result.passed is True
        assert result.metadata["fresh_datasets"].value == 1

    def test_stale_dataset_fails(self, duckdb):
        """GIVEN every dataset older than the window
        WHEN the check runs
        THEN it fails.
        """
        self._obs(duckdb, "ds_old", 400)
        result = self._run(duckdb)
        assert result.passed is False
        assert result.metadata["fresh_datasets"].value == 0

    def test_replay_does_not_inflate_freshness(self, duckdb):
        """GIVEN one dataset carrying three observations of different posts
        WHEN the check runs
        THEN freshness is 1, not 3 — rows are not datasets.

        This is the failure the check exists to catch: a dataset assigns one
        observation the first time, and a later replay of the same dataset adds
        posts but no new dataset, so a row count would report freshness that
        did not happen.
        """
        self._obs(duckdb, "ds_replay", 0, post="p1")
        self._obs(duckdb, "ds_replay", 0, post="p2")
        self._obs(duckdb, "ds_replay", 1, post="p3")
        result = self._run(duckdb)
        assert result.passed is True
        assert result.metadata["fresh_datasets"].value == 1

    def test_no_observations_at_all_fails(self, duckdb):
        """GIVEN an empty observations table
        WHEN the check runs
        THEN it fails rather than crashing on the empty query.
        """
        from orchestration.defs.platform.schemas import duckdb_ddl

        with duckdb.get_connection() as conn:
            conn.execute(duckdb_ddl("silver_ig_post_observations"))
        result = self._run(duckdb)
        assert result.passed is False
        assert result.metadata["fresh_datasets"].value == 0
