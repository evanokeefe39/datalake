"""Shared fixtures for the test suite.

* ``db`` — in-memory ``DuckDBResource`` backed by ``tmp_path``.
* ``gemini_mock`` — ``GeminiResource`` with a dummy API key.
* ``apify_mock`` — ``ApifyResource`` with a dummy token.
"""

from __future__ import annotations



import pytest
from dagster_duckdb import DuckDBResource

from datalake.defs.common.resources import ApifyResource, GeminiResource
from pathlib import Path



@pytest.fixture
def db(tmp_path) -> DuckDBResource:
    """Create a DuckDB resource backed by tmp_path (auto-cleaned by pytest)."""
    return DuckDBResource(database=str(tmp_path / "test.duckdb"))


@pytest.fixture
def gemini_mock() -> GeminiResource:
    """GeminiResource pre-configured for testing.

    Returns a bare resource with a dummy API key. Tests apply behavior
    by patching ``GeminiResource.analyze`` at the class level via
    ``patch.object(GeminiResource, "analyze", ...)``.
    """
    return GeminiResource(api_key="test-key")


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
