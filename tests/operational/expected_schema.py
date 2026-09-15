"""Schema catalog — re-exports from the canonical catalog modules.

This file exists for backward compatibility and test imports. The single
sources of truth are ``orchestration.defs.platform.schemas`` (DuckDB half)
and ``opsdb.schema`` (SQLite half).
"""

from __future__ import annotations

from opsdb.schema import SQLITE_TABLES  # noqa: F401
from orchestration.defs.platform.schemas import (  # noqa: F401
    DUCKDB_TABLES,
    DUCKDB_VIEWS,
    SILVER_COLUMNS,
)

# ── Backward-compat aliases ───────────────────────────────────────────────

EXPECTED_DUCKDB = DUCKDB_TABLES
EXPECTED_DUCKDB_VIEWS = DUCKDB_VIEWS
EXPECTED_SQLITE = SQLITE_TABLES
EXPECTED_SCHEMA = DUCKDB_TABLES
EXPECTED_VIEWS = DUCKDB_VIEWS
