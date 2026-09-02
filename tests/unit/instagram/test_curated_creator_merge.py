"""Tests for curated-creator consolidation (scripts/migrate_curated_creator_merge.py).

Contract: a curated creator (creators.id) owns the profile for its real
in-lake handle; duplicate auto-creators keyed by owner_username are retired;
the merge is idempotent and reversible; no profile or dim_profile row ever
references a retired creator.

Regression guard for the WAVIBOY (21 ← 243) / Nick Vinny (147 ← 610)
consolidation: tests assert identity semantics, not just row shape.
"""

from __future__ import annotations

import importlib.util
import sqlite3
import sys
from pathlib import Path

import duckdb
import pytest

_SCRIPT = (
    Path(__file__).resolve().parents[3]
    / "scripts"
    / "migrate_curated_creator_merge.py"
)
_spec = importlib.util.spec_from_file_location("migrate_curated_creator_merge", _SCRIPT)
assert _spec and _spec.loader
mig = importlib.util.module_from_spec(_spec)
sys.modules["migrate_curated_creator_merge"] = mig
_spec.loader.exec_module(mig)

MERGES = mig.DEFAULT_MERGES
WAVIBOY, NICK = 21, 147
DUP_WAVIBOY, DUP_NICK = 243, 610


@pytest.fixture
def env(tmp_path):
    """ops.sqlite + state.duckdb seeded with the pre-merge duplicate state."""
    ops_path = tmp_path / "ops.sqlite"
    duck_path = tmp_path / "state.duckdb"

    ops = sqlite3.connect(str(ops_path))
    ops.executescript(
        """
        CREATE TABLE creators (
            id INTEGER PRIMARY KEY, name TEXT NOT NULL,
            created_at TEXT NOT NULL, updated_at TEXT NOT NULL
        );
        CREATE TABLE profiles (
            platform TEXT NOT NULL, handle TEXT NOT NULL,
            profile_url TEXT NOT NULL, results_type TEXT NOT NULL,
            results_limit INTEGER NOT NULL, enabled INTEGER NOT NULL,
            tier TEXT NOT NULL, creator_id INTEGER NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (platform, handle)
        );
        INSERT INTO creators VALUES
            (21, 'WAVIBOY | AI Creative Direction', 't', 't'),
            (147, 'Nick Vinny · Brand Designer', 't', 't'),
            (243, 'bywaviboy', 't', 't'),
            (610, 'vinny_creative', 't', 't');
        INSERT INTO profiles VALUES
            ('instagram', 'bywaviboy', 'u', 'posts', -1, 1, 'tier1', 243, 't'),
            ('instagram', 'vinny_creative', 'u', 'posts', -1, 1, 'tier1', 610, 't');
        """
    )
    ops.commit()
    ops.close()

    con = duckdb.connect(str(duck_path))
    con.execute(
        "CREATE TABLE dim_profile (profile_key INTEGER, owner_id TEXT, "
        "owner_username TEXT, channel TEXT, is_current BOOLEAN, "
        "creator_id INTEGER, creator_name TEXT)"
    )
    con.execute(
        "INSERT INTO dim_profile VALUES (1, 'w1', 'bywaviboy', 'instagram', TRUE, 243, 'bywaviboy'),"
        "       (2, 'n1', 'vinny_creative', 'instagram', TRUE, 610, 'vinny_creative')"
    )
    con.close()
    return ops_path, duck_path


def _creator_ids(ops_path: Path) -> set[int]:
    ops = sqlite3.connect(str(ops_path))
    try:
        return {r[0] for r in ops.execute("SELECT id FROM creators")}
    finally:
        ops.close()


def _profile_owners(ops_path: Path) -> dict[str, int]:
    ops = sqlite3.connect(str(ops_path))
    try:
        return {
            r[0]: r[1]
            for r in ops.execute("SELECT handle, creator_id FROM profiles")
        }
    finally:
        ops.close()


def _dim_current(duck_path: Path) -> dict[str, tuple[int, str]]:
    con = duckdb.connect(str(duck_path), read_only=True)
    try:
        return {
            r[0]: (r[1], r[2])
            for r in con.execute(
                "SELECT owner_username, creator_id, creator_name "
                "FROM dim_profile WHERE is_current"
            ).fetchall()
        }
    finally:
        con.close()


def test_merge_reassigns_profile_and_retires_duplicate(env):
    ops_path, duck_path = env
    mig.merge(ops_path, duck_path, MERGES)

    assert _profile_owners(ops_path)["bywaviboy"] == WAVIBOY
    assert _profile_owners(ops_path)["vinny_creative"] == NICK
    assert DUP_WAVIBOY not in _creator_ids(ops_path)
    assert DUP_NICK not in _creator_ids(ops_path)
    # The ad-hoc sentinel (results_limit = -1) is preserved on the profile.
    ops = sqlite3.connect(str(ops_path))
    assert ops.execute(
        "SELECT results_limit FROM profiles WHERE handle='bywaviboy'"
    ).fetchone()[0] == -1
    ops.close()


def test_dim_profile_current_rows_carry_curated_identity(env):
    ops_path, duck_path = env
    mig.merge(ops_path, duck_path, MERGES)

    dim = _dim_current(duck_path)
    assert dim["bywaviboy"] == (WAVIBOY, "WAVIBOY | AI Creative Direction")
    assert dim["vinny_creative"] == (NICK, "Nick Vinny · Brand Designer")


def test_no_profile_or_dim_row_references_retired_creator(env):
    """The invariant the original defect violated."""
    ops_path, duck_path = env
    mig.merge(ops_path, duck_path, MERGES)
    retired = {DUP_WAVIBOY, DUP_NICK}

    owners = _profile_owners(ops_path)
    assert not any(v in retired for v in owners.values())
    dim = _dim_current(duck_path)
    assert not any(v[0] in retired for v in dim.values())


def test_merge_is_idempotent(env):
    ops_path, duck_path = env
    mig.merge(ops_path, duck_path, MERGES)
    snap = (_creator_ids(ops_path), _profile_owners(ops_path))
    mig.merge(ops_path, duck_path, MERGES)
    assert (_creator_ids(ops_path), _profile_owners(ops_path)) == snap
    assert dim_after(env) == dim_after(env)


def dim_after(env):
    return _dim_current(env[1])


def test_undo_restores_original_state(env):
    ops_path, duck_path = env
    mig.merge(ops_path, duck_path, MERGES)
    mig.undo(ops_path, duck_path, MERGES)

    assert _profile_owners(ops_path)["bywaviboy"] == DUP_WAVIBOY
    assert _profile_owners(ops_path)["vinny_creative"] == DUP_NICK
    ids = _creator_ids(ops_path)
    assert {DUP_WAVIBOY, DUP_NICK, WAVIBOY, NICK} <= ids
    # Ledger stamps the reversal.
    ops = sqlite3.connect(str(ops_path))
    assert ops.execute(
        "SELECT COUNT(*) FROM creator_merges WHERE reversed_at IS NULL"
    ).fetchone()[0] == 0
    ops.close()
    assert _dim_current(duck_path)["bywaviboy"] == (DUP_WAVIBOY, "bywaviboy")


def test_remerge_after_undo_reapplies(env):
    ops_path, duck_path = env
    mig.merge(ops_path, duck_path, MERGES)
    mig.undo(ops_path, duck_path, MERGES)
    mig.merge(ops_path, duck_path, MERGES)
    assert _profile_owners(ops_path)["bywaviboy"] == WAVIBOY
    assert _dim_current(duck_path)["vinny_creative"][0] == NICK
