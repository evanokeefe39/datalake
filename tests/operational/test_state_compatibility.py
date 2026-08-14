"""Readiness tests: validate code expectations against live databases.

Opens ``data/state.duckdb`` and ``data/ops.sqlite`` read-only and asserts:
- Table existence (both expected tables present, and no unexpected extra tables)
- Column types (extra columns tolerated, missing/mismatched columns fail)
- View queryability
- Stale table name detection (e.g. gold_ig_analyses when gold_analyses is expected)

On a fresh clone (no DB files) every test is skipped — a cold checkout is
valid pipeline state, not a defect.
"""


import sqlite3
import warnings
from pathlib import Path

import duckdb
import pytest

from tests.operational.expected_schema import (
    EXPECTED_DUCKDB,
    EXPECTED_DUCKDB_VIEWS,
    EXPECTED_SQLITE,
)

# ── Known stale table names ──────────────────────────────────────────────
# Table names that existed in older schema versions but have been
# renamed or dropped. If any of these appear in the DB, the test fails
# with a migration hint.

_STALE_DUCKDB_TABLES: dict[str, str] = {
    "gold_ig_analyses": "Rename to 'gold_analyses' — run scripts/migrate_schema_drift.py",
    "silver_ig_progress": (
        "Drop — vestigial table replaced by watermarks. "
        "Run scripts/migrate_schema_drift.py"
    ),
}

_STALE_SQLITE_TABLES: dict[str, str] = {
    "instagram_media_cache": (
        "Drop — replaced by 'media_cache' (byte cache). "
        "Run scripts/migrate_schema_drift.py"
    ),
    "scrape_targets": (
        "Replace with 'creators' + 'profiles'. "
        "Run scripts/migrate_creators_profiles.py"
    ),
}

# ── Tables that exist in the DB but are not in the catalog ───────────────
# Extra tables are detected and reported as warnings. They might be
# legitimate (user-created) or forgotten migrations. Either way, surface them.

# ---------------------------------------------------------------------------
# DuckDB helpers
# ---------------------------------------------------------------------------


def _duckdb_connect(path: str) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(path, read_only=True)


def _duckdb_list_tables(con: duckdb.DuckDBPyConnection) -> set[str]:
    rows = con.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'main'
          AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """
    ).fetchall()
    return {r[0] for r in rows}


def _duckdb_list_views(con: duckdb.DuckDBPyConnection) -> set[str]:
    rows = con.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'main'
          AND table_type = 'VIEW'
        ORDER BY table_name
        """
    ).fetchall()
    return {r[0] for r in rows}


def _duckdb_get_columns(con: duckdb.DuckDBPyConnection, table: str) -> dict[str, str]:
    rows = con.execute(
        f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'main'
          AND table_name = '{table}'
        ORDER BY ordinal_position
        """
    ).fetchall()
    return {r[0]: r[1] for r in rows}


# ---------------------------------------------------------------------------
# SQLite helpers
# ---------------------------------------------------------------------------


def _sqlite_connect(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _sqlite_list_tables(con: sqlite3.Connection) -> set[str]:
    rows = con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    return {r["name"] for r in rows}


def _sqlite_get_columns(con: sqlite3.Connection, table: str) -> dict[str, str]:
    rows = con.execute(f"PRAGMA table_info('{table}')").fetchall()
    return {r["name"]: r["type"].upper() for r in rows}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def state_db():
    """Skip all DuckDB tests when the file does not exist."""
    path = Path("data/state.duckdb")
    if not path.exists():
        pytest.skip("data/state.duckdb does not exist")
    return path


@pytest.fixture
def ops_db():
    """Skip all SQLite tests when the file does not exist."""
    path = Path("data/ops.sqlite")
    if not path.exists():
        pytest.skip("data/ops.sqlite does not exist")
    return path


# ---------------------------------------------------------------------------
# DuckDB: table existence
# ---------------------------------------------------------------------------


class TestDuckDBTablesExist:
    """Every expected table must exist; no stale table names; warn on extras."""

    def test_all_expected_tables_exist(self, state_db):
        con = _duckdb_connect(str(state_db))
        actual = _duckdb_list_tables(con)
        con.close()

        expected = set(EXPECTED_DUCKDB)
        missing = expected - actual
        assert not missing, (
            f"State DB is missing expected table(s): "
            f"{', '.join(sorted(missing))}\n"
            f"Run the pipeline or migration to create them."
        )

    def test_no_stale_table_names(self, state_db):
        con = _duckdb_connect(str(state_db))
        actual = _duckdb_list_tables(con)
        con.close()

        stale = {t for t in actual if t in _STALE_DUCKDB_TABLES}
        if stale:
            lines = [f"  {t} — {_STALE_DUCKDB_TABLES[t]}" for t in sorted(stale)]
            pytest.fail(
                "State DB has stale table(s) from a previous schema:\n"
                + "\n".join(lines)
            )

    def test_no_unexpected_tables(self, state_db):
        con = _duckdb_connect(str(state_db))
        actual = _duckdb_list_tables(con)
        con.close()

        expected = set(EXPECTED_DUCKDB) | set(_STALE_DUCKDB_TABLES)
        extra = actual - expected
        if extra:
            # Warn but do not fail — extra tables may be legitimate.

            warnings.warn(
                f"State DB has unexpected table(s) not in the catalog: "
                f"{', '.join(sorted(extra))}. "
                f"Add them to expected_schema.py if they are intentional."
            )


# ---------------------------------------------------------------------------
# DuckDB: column types
# ---------------------------------------------------------------------------


class TestDuckDBColumnsMatch:
    """Every expected column must exist with the correct type.

    Extra columns in the DB are tolerated (forward-compatible).
    """

    @pytest.mark.parametrize("table", sorted(EXPECTED_DUCKDB))
    def test_columns_match(self, state_db, table):
        con = _duckdb_connect(str(state_db))
        actual_tables = _duckdb_list_tables(con)

        if table not in actual_tables:
            pytest.skip(f"Table '{table}' does not exist — cannot check columns")

        actual_cols = _duckdb_get_columns(con, table)
        expected_cols = EXPECTED_DUCKDB[table]
        con.close()

        missing: list[str] = []
        type_mismatches: list[str] = []

        for col, dtype in expected_cols.items():
            if col not in actual_cols:
                missing.append(f"  {col} ({dtype})")
            elif actual_cols[col].upper() != dtype.upper():
                type_mismatches.append(
                    f"  {col}: expected {dtype}, got {actual_cols[col]}"
                )

        msg_parts: list[str] = []
        if missing:
            msg_parts.append(
                f"Missing column(s) in '{table}':\n" + "\n".join(missing)
            )
        if type_mismatches:
            msg_parts.append(
                f"Type mismatch(es) in '{table}':\n" + "\n".join(type_mismatches)
            )

        assert not msg_parts, "\n\n".join(msg_parts)


# ---------------------------------------------------------------------------
# DuckDB: views
# ---------------------------------------------------------------------------


class TestDuckDBViewsQueryable:
    """Every expected view must be SELECT-able without error."""

    @pytest.mark.parametrize("view", sorted(EXPECTED_DUCKDB_VIEWS))
    def test_view_is_queryable(self, state_db, view):
        con = _duckdb_connect(str(state_db))
        actual_views = _duckdb_list_views(con)

        if view not in actual_views:
            pytest.skip(f"View '{view}' does not exist — cannot query")

        try:
            con.execute(f"SELECT * FROM {view} LIMIT 1")
        except Exception as exc:
            pytest.fail(
                f"Failed to query view '{view}': {exc}\n"
                f"This may indicate a broken view definition or "
                f"missing underlying table."
            )
        finally:
            con.close()


# ---------------------------------------------------------------------------
# SQLite: table existence
# ---------------------------------------------------------------------------


class TestSQLiteTablesExist:
    """Every expected ops.sqlite table must exist; no stale names; warn on extras."""

    def test_all_expected_tables_exist(self, ops_db):
        con = _sqlite_connect(str(ops_db))
        actual = _sqlite_list_tables(con)
        con.close()

        expected = set(EXPECTED_SQLITE)
        missing = expected - actual
        assert not missing, (
            f"Ops DB is missing expected table(s): "
            f"{', '.join(sorted(missing))}\n"
            f"Run the pipeline or migration to create them."
        )

    def test_no_stale_table_names(self, ops_db):
        con = _sqlite_connect(str(ops_db))
        actual = _sqlite_list_tables(con)
        con.close()

        stale = {t for t in actual if t in _STALE_SQLITE_TABLES}
        if stale:
            lines = [f"  {t} — {_STALE_SQLITE_TABLES[t]}" for t in sorted(stale)]
            pytest.fail(
                "Ops DB has stale table(s) from a previous schema:\n"
                + "\n".join(lines)
            )

    def test_no_unexpected_tables(self, ops_db):
        con = _sqlite_connect(str(ops_db))
        actual = _sqlite_list_tables(con)
        con.close()

        expected = set(EXPECTED_SQLITE) | set(_STALE_SQLITE_TABLES)
        extra = actual - expected
        if extra:

            warnings.warn(
                f"Ops DB has unexpected table(s) not in the catalog: "
                f"{', '.join(sorted(extra))}. "
                f"Add them to expected_schema.py if they are intentional."
            )


# ---------------------------------------------------------------------------
# SQLite: column types
# ---------------------------------------------------------------------------


class TestSQLiteColumnsMatch:
    """Every expected column in ops.sqlite must exist with the correct type."""

    @pytest.mark.parametrize("table", sorted(EXPECTED_SQLITE))
    def test_columns_match(self, ops_db, table):
        con = _sqlite_connect(str(ops_db))
        actual_tables = _sqlite_list_tables(con)

        if table not in actual_tables:
            pytest.skip(f"Table '{table}' does not exist — cannot check columns")

        actual_cols = _sqlite_get_columns(con, table)
        expected_cols = EXPECTED_SQLITE[table]
        con.close()

        missing: list[str] = []
        type_mismatches: list[str] = []

        for col, dtype in expected_cols.items():
            if col not in actual_cols:
                missing.append(f"  {col} ({dtype})")
            elif actual_cols[col].upper() != dtype.upper():
                type_mismatches.append(
                    f"  {col}: expected {dtype}, got {actual_cols[col]}"
                )

        msg_parts: list[str] = []
        if missing:
            msg_parts.append(
                f"Missing column(s) in ops.sqlite '{table}':\n" + "\n".join(missing)
            )
        if type_mismatches:
            msg_parts.append(
                f"Type mismatch(es) in ops.sqlite '{table}':\n"
                + "\n".join(type_mismatches)
            )

        assert not msg_parts, "\n\n".join(msg_parts)


# ── Backward-compat aliases (old test code) ──────────────────────────────

# These exist so existing test classes bound to the old names still resolve.
TestStateTablesExist = TestDuckDBTablesExist
TestStateColumnsMatch = TestDuckDBColumnsMatch
TestViewsQueryable = TestDuckDBViewsQueryable
