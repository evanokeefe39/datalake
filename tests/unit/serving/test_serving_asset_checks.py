"""Unit tests for serving-layer asset checks.

Each test exercises a single ``@asset_check`` function, validating both
pass and fail paths.
"""

from __future__ import annotations

import pytest
from dagster import build_asset_check_context
from dagster_duckdb import DuckDBResource

from datalake.defs.serving.asset_checks import serving_checks

_CHECKS_BY_NAME = {list(c.check_keys)[0].name: c for c in serving_checks}


@pytest.fixture
def duckdb(tmp_path):
    return DuckDBResource(database=str(tmp_path / "state.duckdb"))


@pytest.fixture
def _seed_dim_profile(duckdb):
    """Seed ``dim_profile`` with non-overlapping, well-formed rows."""
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE TABLE dim_profile (
                profile_key INTEGER PRIMARY KEY,
                owner_id TEXT NOT NULL,
                owner_username TEXT,
                channel TEXT NOT NULL DEFAULT 'instagram',
                effective_from TIMESTAMP NOT NULL,
                effective_to TIMESTAMP,
                is_current BOOLEAN NOT NULL DEFAULT TRUE
            )
        """)
        conn.execute("""
            INSERT INTO dim_profile
                (profile_key, owner_id, owner_username, effective_from, effective_to, is_current)
            VALUES
                (1, 'owner_a', 'user_a', '2024-01-01', '2024-06-01', FALSE),
                (2, 'owner_a', 'user_a_v2', '2024-06-01', NULL, TRUE),
                (3, 'owner_b', 'user_b', '2024-01-01', NULL, TRUE)
        """)


# ===== dim_profile checks ==================================================


class TestProfileChecks:
    """Tests for ``dim_profile_*`` checks."""

    def test_no_overlapping_intervals_passes(self, duckdb, _seed_dim_profile):
        """GIVEN dim_profile with non-overlapping intervals per owner
        WHEN the check runs
        THEN it passes.
        """
        ctx = build_asset_check_context(resources={"duckdb": duckdb})
        result = _CHECKS_BY_NAME["dim_profile_no_overlapping_intervals"](ctx)
        assert result.passed is True

    def test_no_overlapping_intervals_fails(self, duckdb):
        """GIVEN dim_profile with overlapping intervals
        WHEN the check runs
        THEN it fails.
        """
        with duckdb.get_connection() as conn:
            conn.execute("""
                CREATE TABLE dim_profile (
                    profile_key INTEGER PRIMARY KEY,
                    owner_id TEXT NOT NULL,
                    owner_username TEXT,
                    channel TEXT NOT NULL DEFAULT 'instagram',
                    effective_from TIMESTAMP NOT NULL,
                    effective_to TIMESTAMP,
                    is_current BOOLEAN NOT NULL DEFAULT TRUE
                )
            """)
            # Overlapping: row 2 starts before row 1 ends
            conn.execute("""
                INSERT INTO dim_profile
                    (profile_key, owner_id, owner_username,
                     effective_from, effective_to, is_current)
                VALUES
                    (1, 'owner_a', 'user_a', '2024-01-01', '2024-12-31', FALSE),
                    (2, 'owner_a', 'user_a_v2', '2024-06-01', NULL, TRUE)
            """)

        ctx = build_asset_check_context(resources={"duckdb": duckdb})
        result = _CHECKS_BY_NAME["dim_profile_no_overlapping_intervals"](ctx)
        assert result.passed is False

    def test_effective_range_passes(self, duckdb, _seed_dim_profile):
        """GIVEN dim_profile with effective_from ≤ effective_to
        WHEN the check runs
        THEN it passes.
        """
        ctx = build_asset_check_context(resources={"duckdb": duckdb})
        result = _CHECKS_BY_NAME["dim_profile_effective_range_valid"](ctx)
        assert result.passed is True

    def test_effective_range_fails(self, duckdb):
        """GIVEN dim_profile with effective_from > effective_to
        WHEN the check runs
        THEN it fails.
        """
        with duckdb.get_connection() as conn:
            conn.execute("""
                CREATE TABLE dim_profile (
                    profile_key INTEGER PRIMARY KEY,
                    owner_id TEXT NOT NULL,
                    owner_username TEXT,
                    channel TEXT NOT NULL DEFAULT 'instagram',
                    effective_from TIMESTAMP NOT NULL,
                    effective_to TIMESTAMP,
                    is_current BOOLEAN NOT NULL DEFAULT TRUE
                )
            """)
            conn.execute("""
                INSERT INTO dim_profile
                    (profile_key, owner_id, owner_username, effective_from, effective_to)
                VALUES (1, 'owner_a', 'user_a', '2024-06-01', '2024-01-01')
            """)

        ctx = build_asset_check_context(resources={"duckdb": duckdb})
        result = _CHECKS_BY_NAME["dim_profile_effective_range_valid"](ctx)
        assert result.passed is False

    def test_no_gaps_passes(self, duckdb, _seed_dim_profile):
        """GIVEN dim_profile with no gaps between consecutive intervals
        WHEN the check runs
        THEN it passes.
        """
        ctx = build_asset_check_context(resources={"duckdb": duckdb})
        result = _CHECKS_BY_NAME["dim_profile_no_gaps"](ctx)
        assert result.passed is True

    def test_no_gaps_fails(self, duckdb):
        """GIVEN dim_profile with a gap between intervals
        WHEN the check runs
        THEN it fails.
        """
        with duckdb.get_connection() as conn:
            conn.execute("""
                CREATE TABLE dim_profile (
                    profile_key INTEGER PRIMARY KEY,
                    owner_id TEXT NOT NULL,
                    owner_username TEXT,
                    channel TEXT NOT NULL DEFAULT 'instagram',
                    effective_from TIMESTAMP NOT NULL,
                    effective_to TIMESTAMP,
                    is_current BOOLEAN NOT NULL DEFAULT TRUE
                )
            """)
            # Gap: second row starts at 2024-07-01, first ended at 2024-06-01
            conn.execute("""
                INSERT INTO dim_profile
                    (profile_key, owner_id, owner_username,
                     effective_from, effective_to, is_current)
                VALUES
                    (1, 'owner_a', 'user_a', '2024-01-01', '2024-06-01', FALSE),
                    (2, 'owner_a', 'user_a_v2', '2024-07-01', NULL, TRUE)
            """)

        ctx = build_asset_check_context(resources={"duckdb": duckdb})
        result = _CHECKS_BY_NAME["dim_profile_no_gaps"](ctx)
        assert result.passed is False


# ===== analytics_views check ===============================================


class TestAnalyticsViewsCheck:
    """Tests for ``analytics_views_row_count_positive``."""

    def test_row_count_positive_passes(self, duckdb):
        """GIVEN analytics_views has rows
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
            conn.execute("""
                CREATE TABLE gold_ig_analyses (
                    post_id TEXT PRIMARY KEY, result_json TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE dim_profile (
                    profile_key INTEGER PRIMARY KEY,
                    owner_id TEXT NOT NULL,
                    owner_username TEXT,
                    channel TEXT NOT NULL DEFAULT 'instagram',
                    effective_from TIMESTAMP NOT NULL,
                    effective_to TIMESTAMP,
                    is_current BOOLEAN NOT NULL DEFAULT TRUE
                )
            """)
            conn.execute("""
                CREATE OR REPLACE VIEW analytics_views AS
                SELECT sp.post_id
                FROM silver_ig_posts sp
                LEFT JOIN gold_ig_analyses ga ON sp.post_id = ga.post_id
                LEFT JOIN dim_profile dp ON 1=0
            """)

        ctx = build_asset_check_context(resources={"duckdb": duckdb})
        result = _CHECKS_BY_NAME["analytics_views_row_count_positive"](ctx)
        assert result.passed is True
        assert result.metadata["row_count"].value == 1

    def test_row_count_positive_fails_no_rows(self, duckdb):
        """GIVEN analytics_views returns 0 rows
        WHEN the check runs
        THEN it fails.
        """
        with duckdb.get_connection() as conn:
            conn.execute("""
                CREATE TABLE silver_ig_posts (
                    post_id TEXT PRIMARY KEY, caption TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE gold_ig_analyses (
                    post_id TEXT PRIMARY KEY, result_json TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE dim_profile (
                    profile_key INTEGER PRIMARY KEY,
                    owner_id TEXT NOT NULL,
                    owner_username TEXT,
                    channel TEXT NOT NULL DEFAULT 'instagram',
                    effective_from TIMESTAMP NOT NULL,
                    effective_to TIMESTAMP,
                    is_current BOOLEAN NOT NULL DEFAULT TRUE
                )
            """)
            conn.execute("""
                CREATE OR REPLACE VIEW analytics_views AS
                SELECT sp.post_id
                FROM silver_ig_posts sp
                LEFT JOIN dim_profile dp ON 1=0
            """)

        ctx = build_asset_check_context(resources={"duckdb": duckdb})
        result = _CHECKS_BY_NAME["analytics_views_row_count_positive"](ctx)
        assert result.passed is False
