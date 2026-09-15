"""Shared fixtures for the test suite.

* ``db`` — in-memory ``DuckDBResource`` backed by ``tmp_path``.
* ``apify_mock`` — ``ApifyResource`` with a dummy token.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from dagster_duckdb import DuckDBResource
from orchestration.defs.platform.resources import ApifyResource, SQLiteResource


@pytest.fixture
def db(tmp_path) -> DuckDBResource:
    """Create a DuckDB resource backed by tmp_path (auto-cleaned by pytest)."""
    return DuckDBResource(database=str(tmp_path / "test.duckdb"))


@pytest.fixture
def ops(tmp_path) -> SQLiteResource:
    """A SQLiteResource backed by tmp_path (auto-cleaned by pytest)."""
    return SQLiteResource(database=str(tmp_path / "ops.sqlite"))




@pytest.fixture
def apify_mock() -> ApifyResource:
    """ApifyResource with a dummy token for testing."""
    return ApifyResource(token="test-token")


@pytest.fixture
def state_db() -> Path:
    """Return path to the real state database, skipping if absent.

    Opens read-only. On a fresh clone the file won't exist — skip cleanly
    rather than failing, because a cold checkout is valid pipeline state.
    """
    path = Path("data/state.duckdb")
    if not path.exists():
        pytest.skip("data/state.duckdb not found — run the pipeline first")
    return path
