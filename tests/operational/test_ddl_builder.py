"""DDL builder tests — the catalog and generated DDL can never drift.

The schema catalog (``schemas.py``) is the single source of truth. Its DDL
builder must produce ``CREATE TABLE`` statements that, when executed against a
fresh database, yield exactly the columns and types the catalog declares. This
guarantees that a column added to the catalog shows up in the DDL (and vice
versa) — the drift class that produced the ``dead_letter.failed_at`` and
``media_metadata.video_duration_seconds`` mismatches.
"""

from __future__ import annotations

import sqlite3

import duckdb
import pytest

from datalake.defs.common.schemas import (
    DUCKDB_TABLES,
    SQLITE_TABLES,
    duckdb_all_ddl,
    duckdb_ddl,
    sqlite_all_ddl,
)


@pytest.mark.parametrize("table", sorted(DUCKDB_TABLES))
def test_duckdb_ddl_matches_catalog(table: str):
    """Executing the generated DuckDB DDL yields the catalog's columns/types."""
    con = duckdb.connect(":memory:")
    try:
        con.execute(duckdb_ddl(table))
        actual = con.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_name = ? ORDER BY ordinal_position",
            [table],
        ).fetchall()
    finally:
        con.close()

    actual_map = {r[0]: r[1] for r in actual}
    expected = DUCKDB_TABLES[table]
    assert list(actual_map) == list(expected), (
        f"{table}: column order drifted — {list(actual_map)} != {list(expected)}"
    )
    for col, dtype in expected.items():
        assert actual_map[col].upper() == dtype.upper(), (
            f"{table}.{col}: expected {dtype}, got {actual_map[col]}"
        )


@pytest.mark.parametrize("table", sorted(SQLITE_TABLES))
def test_sqlite_ddl_matches_catalog(table: str):
    """Executing the generated SQLite DDL yields the catalog's columns/types."""
    con = sqlite3.connect(":memory:")
    try:
        con.executescript(sqlite_all_ddl())
        actual = con.execute(f"PRAGMA table_info({table})").fetchall()
    finally:
        con.close()

    actual_map = {r[1]: r[2].upper() for r in actual}
    expected = SQLITE_TABLES[table]
    assert list(actual_map) == list(expected), (
        f"{table}: column order drifted — {list(actual_map)} != {list(expected)}"
    )
    for col, dtype in expected.items():
        assert actual_map[col] == dtype.upper(), (
            f"{table}.{col}: expected {dtype}, got {actual_map[col]}"
        )


def test_duckdb_all_ddl_creates_every_table():
    """``duckdb_all_ddl`` creates every DuckDB table in one pass."""
    con = duckdb.connect(":memory:")
    try:
        con.execute(duckdb_all_ddl())
        actual = {
            r[0]
            for r in con.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main' AND table_type = 'BASE TABLE'"
            ).fetchall()
        }
    finally:
        con.close()

    assert set(DUCKDB_TABLES) == actual


def test_sqlite_all_ddl_creates_indexes():
    """``sqlite_all_ddl`` creates the declared batch_items index."""
    con = sqlite3.connect(":memory:")
    try:
        con.executescript(sqlite_all_ddl())
        indexes = {
            r[1]
            for r in con.execute(
                "SELECT type, name FROM sqlite_master WHERE type = 'index'"
            ).fetchall()
        }
    finally:
        con.close()

    assert "idx_batch_items_job_status" in indexes
