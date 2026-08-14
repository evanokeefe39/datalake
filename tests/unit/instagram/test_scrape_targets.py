"""Tests for the ``scrape_targets`` control table and CRUD helpers."""

from __future__ import annotations

import pytest

from datalake.defs.common.resources import SQLiteResource
from datalake.defs.common.schemas import SQLITE_TABLES
from datalake.defs.instagram.scrape_targets import (
    delete_target,
    enabled_targets,
    ensure_schema,
    list_targets,
    upsert_target,
)


@pytest.fixture
def ops(tmp_path) -> SQLiteResource:
    return SQLiteResource(database=str(tmp_path / "ops.sqlite"))


def test_ensure_schema_creates_table(ops):
    ensure_schema(ops)
    conn = ops.get_connection()
    try:
        names = {
            r[0]
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
    finally:
        conn.close()
    assert "scrape_targets" in names


def test_upsert_and_list_roundtrip(ops):
    upsert_target(ops, username="alice", profile_url="https://www.instagram.com/alice/")
    upsert_target(
        ops,
        username="bob",
        profile_url="https://www.instagram.com/bob/",
        enabled=False,
    )

    usernames = {r["username"] for r in list_targets(ops)}
    assert usernames == {"alice", "bob"}


def test_upsert_is_idempotent(ops):
    url = "https://www.instagram.com/alice/"
    upsert_target(ops, username="alice", profile_url=url, results_limit=3)
    upsert_target(ops, username="alice", profile_url=url, results_limit=5)

    rows = list_targets(ops)
    assert len(rows) == 1
    assert rows[0]["results_limit"] == 5


def test_delete_target(ops):
    upsert_target(ops, username="alice", profile_url="https://www.instagram.com/alice/")
    delete_target(ops, "alice")
    assert list_targets(ops) == []


def test_enabled_targets_filters_disabled(ops):
    upsert_target(ops, username="alice", profile_url="https://www.instagram.com/alice/")
    upsert_target(ops, username="bob", profile_url="https://www.instagram.com/bob/", enabled=False)

    assert [t["username"] for t in enabled_targets(ops)] == ["alice"]


def test_schema_catalog_includes_scrape_targets():
    assert "scrape_targets" in SQLITE_TABLES
    cols = set(SQLITE_TABLES["scrape_targets"])
    assert cols >= {
        "username",
        "profile_url",
        "results_type",
        "results_limit",
        "enabled",
        "tier",
        "updated_at",
    }
