"""Schema catalog — re-exports from the canonical ``datalake.defs.common.schemas``.

This file exists for backward compatibility and test imports. The single
source of truth is ``src/datalake/defs/common/schemas.py``.
"""

from __future__ import annotations

from datalake.defs.common.schemas import (  # noqa: F401
    DUCKDB_TABLES,
    DUCKDB_VIEWS,
    SILVER_COLUMNS,
    SQLITE_TABLES,
)

# ── Backward-compat aliases ───────────────────────────────────────────────

EXPECTED_DUCKDB = DUCKDB_TABLES
EXPECTED_DUCKDB_VIEWS = DUCKDB_VIEWS
EXPECTED_SQLITE = SQLITE_TABLES
EXPECTED_SCHEMA = DUCKDB_TABLES
EXPECTED_VIEWS = DUCKDB_VIEWS
