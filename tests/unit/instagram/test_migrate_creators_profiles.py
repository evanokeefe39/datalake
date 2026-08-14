"""Tests for the scrape_targets → creators/profiles migration script."""

from __future__ import annotations

import importlib.util
import sqlite3
from pathlib import Path

import duckdb

_SCRIPT = (
    Path(__file__).resolve().parents[3] / "scripts" / "migrate_creators_profiles.py"
)
_spec = importlib.util.spec_from_file_location("migrate_creators_profiles", _SCRIPT)
assert _spec and _spec.loader, "migration script not found"
migrate_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(migrate_mod)


def _seed_scrape_targets(ops_path: Path) -> None:
    con = sqlite3.connect(str(ops_path))
    try:
        con.execute(
            """
            CREATE TABLE scrape_targets (
                username      TEXT PRIMARY KEY,
                profile_url   TEXT NOT NULL,
                results_type  TEXT NOT NULL DEFAULT 'details',
                results_limit INTEGER NOT NULL DEFAULT 1,
                enabled       INTEGER NOT NULL DEFAULT 1,
                tier          TEXT NOT NULL DEFAULT 'tier1',
                updated_at    TEXT NOT NULL
            )
            """
        )
        con.executemany(
            "INSERT INTO scrape_targets "
            "(username, profile_url, results_type, results_limit, enabled, tier, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    "alice", "https://www.instagram.com/alice/",
                    "details", 10, 1, "tier1", "2026-01-01T00:00:00+00:00",
                ),
                (
                    "bob", "https://www.instagram.com/bob/",
                    "details", 5, 1, "tier1", "2026-01-01T00:00:00+00:00",
                ),
            ],
        )
        con.commit()
    finally:
        con.close()


def _seed_silver_profiles(duckdb_path: Path) -> None:
    con = duckdb.connect(str(duckdb_path))
    try:
        con.execute(
            "CREATE TABLE silver_ig_profiles (owner_username TEXT, full_name TEXT)"
        )
        con.execute(
            "INSERT INTO silver_ig_profiles VALUES ('alice', 'Alice Adams'), ('bob', NULL)"
        )
    finally:
        con.close()


def _tables(ops_path: Path) -> set[str]:
    con = sqlite3.connect(str(ops_path))
    try:
        return {
            r[0]
            for r in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    finally:
        con.close()


def test_migration_backfills_and_drops(tmp_path):
    ops_path = tmp_path / "ops.sqlite"
    duckdb_path = tmp_path / "state.duckdb"
    _seed_scrape_targets(ops_path)
    _seed_silver_profiles(duckdb_path)

    migrate_mod.migrate(ops_path, duckdb_path)

    con = sqlite3.connect(str(ops_path))
    con.row_factory = sqlite3.Row
    try:
        creators = {
            r["name"]: r["id"]
            for r in con.execute("SELECT id, name FROM creators").fetchall()
        }
        # full_name when known, else handle
        assert set(creators) == {"Alice Adams", "bob"}
        profiles = con.execute(
            "SELECT handle, creator_id FROM profiles ORDER BY handle"
        ).fetchall()
        assert {p["handle"] for p in profiles} == {"alice", "bob"}
        for p in profiles:
            assert p["creator_id"] in creators.values()
    finally:
        con.close()

    tables = _tables(ops_path)
    assert "scrape_targets" not in tables
    assert {"batch_jobs", "batch_items", "media_metadata", "dead_letter"} <= tables


def test_migration_idempotent(tmp_path):
    ops_path = tmp_path / "ops.sqlite"
    duckdb_path = tmp_path / "state.duckdb"
    _seed_scrape_targets(ops_path)
    _seed_silver_profiles(duckdb_path)

    migrate_mod.migrate(ops_path, duckdb_path)
    migrate_mod.migrate(ops_path, duckdb_path)  # re-run must be a no-op

    con = sqlite3.connect(str(ops_path))
    try:
        n_creators = con.execute("SELECT COUNT(*) FROM creators").fetchone()[0]
        n_profiles = con.execute("SELECT COUNT(*) FROM profiles").fetchone()[0]
    finally:
        con.close()
    assert n_creators == 2
    assert n_profiles == 2


def test_migration_without_scrape_targets_is_noop(tmp_path):
    ops_path = tmp_path / "ops.sqlite"
    duckdb_path = tmp_path / "state.duckdb"

    migrate_mod.migrate(ops_path, duckdb_path)

    tables = _tables(ops_path)
    assert {
        "creators", "profiles", "batch_jobs", "batch_items",
        "media_metadata", "dead_letter",
    } <= tables
    con = sqlite3.connect(str(ops_path))
    try:
        n_creators = con.execute("SELECT COUNT(*) FROM creators").fetchone()[0]
    finally:
        con.close()
    assert n_creators == 0
