"""Readiness tests: validate code expectations against the real state database.

Opens ``data/state.duckdb`` read-only and asserts schema contracts:
table existence, column types, view queryability.

On a fresh clone (no state DB) every test is skipped — a cold checkout is
valid pipeline state, not a defect.
"""

from __future__ import annotations

import duckdb
import pytest

from tests.operational.expected_schema import EXPECTED_SCHEMA, EXPECTED_VIEWS

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _connect(path: str) -> duckdb.DuckDBPyConnection:
    """Open the state DB read-only, raising on corruption or lock."""
    return duckdb.connect(path, read_only=True)


def _list_tables(con: duckdb.DuckDBPyConnection) -> set[str]:
    """Return the set of user table names in the DB."""
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


def _list_views(con: duckdb.DuckDBPyConnection) -> set[str]:
    """Return the set of user view names in the DB."""
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


def _get_columns(con: duckdb.DuckDBPyConnection, table: str) -> dict[str, str]:
    """Return {column_name: data_type} for the given table/view."""
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
# Tests
# ---------------------------------------------------------------------------


class TestStateTablesExist:
    """Every expected table must exist in the real state DB."""

    def test_all_expected_tables_exist(self, state_db):
        con = _connect(str(state_db))
        actual = _list_tables(con)
        con.close()

        expected = set(EXPECTED_SCHEMA)
        missing = expected - actual
        assert not missing, (
            f"State DB is missing expected table(s): "
            f"{', '.join(sorted(missing))}\n"
            f"Run the pipeline or migration to create them."
        )


class TestStateColumnsMatch:
    """Every expected column must exist with the correct type.

    Extra columns in the DB are tolerated (forward-compatible). Missing
    columns or type mismatches fail with detail.
    """

    @pytest.mark.parametrize("table", sorted(EXPECTED_SCHEMA))
    def test_columns_match(self, state_db, table):
        con = _connect(str(state_db))
        actual_tables = _list_tables(con)

        if table not in actual_tables:
            pytest.skip(f"Table '{table}' does not exist — cannot check columns")

        actual_cols = _get_columns(con, table)
        expected_cols = EXPECTED_SCHEMA[table]
        con.close()

        missing: list[str] = []
        type_mismatches: list[str] = []

        for col, dtype in expected_cols.items():
            if col not in actual_cols:
                missing.append(f"  {col} ({dtype})")
            elif actual_cols[col].upper() != dtype.upper():
                type_mismatches.append(f"  {col}: expected {dtype}, got {actual_cols[col]}")

        msg_parts: list[str] = []
        if missing:
            msg_parts.append(f"Missing column(s) in '{table}':\n" + "\n".join(missing))
        if type_mismatches:
            msg_parts.append(f"Type mismatch(es) in '{table}':\n" + "\n".join(type_mismatches))

        assert not msg_parts, "\n\n".join(msg_parts)


class TestViewsQueryable:
    """Every expected view must be SELECT-able without error."""

    @pytest.mark.parametrize("view", sorted(EXPECTED_VIEWS))
    def test_view_is_queryable(self, state_db, view):
        con = _connect(str(state_db))
        actual_views = _list_views(con)

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
