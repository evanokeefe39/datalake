"""Tests for the owner_id backfill migration (scripts/migrate_backfill_owner_id.py).

Root-cause regression: legacy bronze datasets carry the author's ``ownerId``
on every post row, but rows ingested before the ``ownerId`` → ``owner_id``
mapping fix entered silver with null ``owner_id``. Those posts can't join
``dim_profile`` (keyed on ``owner_id``), so creator_id-based views exclude
them while owner-username-keyed surfaces include them — the "ACT=0% vs
ACT=YES" mismatch. The migration recovers ``owner_id`` from bronze.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import duckdb
import polars as pl
import pytest

SCRIPT = (
    Path(__file__).resolve().parents[3]
    / "scripts"
    / "migrate_backfill_owner_id.py"
)


@pytest.fixture
def migrate():
    spec = importlib.util.spec_from_file_location(
        "migrate_backfill_owner_id", SCRIPT
    )
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def test_collect_owner_ids_uses_owner_id_with_row_fallback(tmp_path, migrate):
    """ownerId wins per row; a null ownerId falls back to the owner_id column."""
    df = pl.DataFrame(
        {
            "id": ["p1", "p2", "p3"],
            "ownerId": ["123", None, "456"],
            "owner_id": ["999", "888", None],
        }
    )
    df.write_parquet(tmp_path / "ds.parquet")
    migrate.BRONZE_DIR = tmp_path
    assert migrate.collect_owner_ids() == {"p1": "123", "p2": "888", "p3": "456"}


def test_collect_owner_ids_owner_id_column_only(tmp_path, migrate):
    df = pl.DataFrame({"id": ["p1"], "owner_id": ["777"]})
    df.write_parquet(tmp_path / "ds.parquet")
    migrate.BRONZE_DIR = tmp_path
    assert migrate.collect_owner_ids() == {"p1": "777"}


def test_collect_owner_ids_skips_rows_without_any_owner(tmp_path, migrate):
    df = pl.DataFrame({"id": ["p1", "p2"], "ownerId": [None, None]})
    df.write_parquet(tmp_path / "ds.parquet")
    migrate.BRONZE_DIR = tmp_path
    assert migrate.collect_owner_ids() == {}


def test_apply_fixes_backfills_only_null_owner_ids(tmp_path, migrate):
    db = duckdb.connect(str(tmp_path / "state.duckdb"))
    db.execute("CREATE TABLE silver_ig_posts (post_id VARCHAR, owner_id VARCHAR)")
    db.execute(
        "INSERT INTO silver_ig_posts VALUES ('p1', NULL), ('p2', 'keep'), ('p3', NULL)"
    )
    fixed = migrate.apply_fixes(db, {"p1": "123", "p3": "456"})
    rows = dict(db.execute("SELECT post_id, owner_id FROM silver_ig_posts").fetchall())
    db.close()
    assert fixed == 2
    assert rows == {"p1": "123", "p2": "keep", "p3": "456"}
