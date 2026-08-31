"""Tests for the Tukey-fence label pass (Epic 3, US-L1/L2/L3/L6).

Covers the rule table, self-versioning (single day0→day7 upgrade, day7
immutability, version-mismatch recompute), the bootstrap guard (0 day7),
control determinism, and the floor-filler top-up.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from datalake.defs.common.resources import DuckDBResource, SQLiteResource
from datalake.defs.common.schemas import duckdb_ddl
from datalake.defs.instagram.labels import LABEL_VERSION, run_label_pass

NOW = datetime(2026, 8, 31, tzinfo=timezone.utc)


@pytest.fixture()
def conn(tmp_path):
    db = DuckDBResource(database=str(tmp_path / "state.duckdb"))
    with db.get_connection() as c:
        for t in ("silver_ig_posts", "silver_ig_post_observations",
                  "ig_post_labels", "gold_analyses"):
            c.execute(duckdb_ddl(t))
        yield c


@pytest.fixture()
def ops(tmp_path):
    return SQLiteResource(database=str(tmp_path / "ops.sqlite"))


def _core_ops(ops, handle="alice"):
    """Register a tier1 enabled profile so the handle counts as core."""
    from datalake.defs.instagram.creators import add_profile, create_creator

    creator = create_creator(ops, "Alice")
    add_profile(ops, creator_id=creator["id"], platform="instagram", handle=handle)
    return ops


def _post(conn, post_id, owner_id, likes, ts, caption="Caption", username=None):
    conn.execute(
        "INSERT INTO silver_ig_posts (post_id, owner_id, owner_username, caption, "
        "likes_count, timestamp, processed_on, source_dataset) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, 'test')",
        [post_id, owner_id, username or owner_id, caption, likes, ts, ts],
    )
    conn.execute(
        "INSERT INTO silver_ig_post_observations VALUES (?, ?, ?, NULL, NULL, NULL, 'test')",
        [post_id, ts, likes],
    )


def _history(conn, owner, n, likes=100, end=NOW, username=None):
    """n flat-baseline posts for owner, spaced 30d apart, ending at `end`."""
    for i in range(n):
        _post(conn, f"{owner}{i}", owner, likes,
              end - timedelta(days=30 * (n - i)), username=username)


def _labels(conn):
    rows = conn.execute(
        "SELECT post_id, label, method, enrich_decision, is_provisional, "
        "label_version, baseline_center, baseline_spread, baseline_n "
        "FROM ig_post_labels"
    ).fetchall()
    return {r[0]: r for r in rows}


# ── Rule table ───────────────────────────────────────────────────────────────


def test_sentinel_likes_unjudgeable(conn):
    _post(conn, "p1", "u1", -1, NOW - timedelta(days=30))
    run_label_pass(conn, now=NOW)
    r = _labels(conn)["p1"]
    assert r[1] == "unjudgeable" and r[2] == "day0_heuristic"
    assert r[3] == "control" and r[4] is True


def test_null_likes_unjudgeable(conn):
    _post(conn, "p1", "u1", None, NOW - timedelta(days=30))
    run_label_pass(conn, now=NOW)
    assert _labels(conn)["p1"][1] == "unjudgeable"


def test_insufficient_baseline(conn):
    # Newest of 4 posts has only 3 prior known posts < min n of 5.
    _history(conn, "u1", 4)
    run_label_pass(conn, now=NOW)
    r = _labels(conn)["u13"]
    assert r[1] == "insufficient_baseline" and r[3] == "control"


def test_core_mature_day7_standout(conn):
    _history(conn, "u1", 20)
    _post(conn, "px", "u1", 1000, NOW - timedelta(days=10))
    run_label_pass(conn, core_handles={"u1"}, now=NOW)
    r = _labels(conn)["px"]
    assert r[1] == "standout" and r[2] == "day7_matched"
    assert r[3] == "standout" and r[4] is False
    assert r[5] == LABEL_VERSION
    assert r[6] is not None and r[7] is not None and r[8] >= 5


def test_core_recent_pending(conn):
    _history(conn, "u1", 20)
    _post(conn, "px", "u1", 1000, NOW - timedelta(days=2))
    run_label_pass(conn, core_handles={"u1"}, now=NOW)
    r = _labels(conn)["px"]
    assert r[1] == "pending" and r[2] == "pending" and r[3] == "skip"


def test_tail_post_is_day0_provisional(conn):
    _history(conn, "u1", 20)
    _post(conn, "px", "u1", 1000, NOW - timedelta(days=10))
    run_label_pass(conn, core_handles=set(), now=NOW)  # no core handles
    r = _labels(conn)["px"]
    assert r[1] == "standout" and r[2] == "day0_heuristic" and r[4] is True


def test_control_membership_deterministic(conn):
    _history(conn, "u1", 20)
    _post(conn, "px", "u1", 100, NOW - timedelta(days=10))  # non-standout
    run_label_pass(conn, core_handles={"u1"}, now=NOW)
    first = _labels(conn)["px"][3]
    conn.execute("DELETE FROM ig_post_labels")
    run_label_pass(conn, core_handles={"u1"}, now=NOW)
    second = _labels(conn)["px"][3]
    assert first == second
    assert first in ("control", "skip")


def test_empty_caption_skip(conn):
    _history(conn, "u1", 20)
    _post(conn, "px", "u1", 1000, NOW - timedelta(days=10), caption="   ")
    run_label_pass(conn, core_handles={"u1"}, now=NOW)
    r = _labels(conn)["px"]
    assert r[3] == "skip" and r[2] == "day0_heuristic" and r[4] is True


# ── Self-versioning (US-L2) ─────────────────────────────────────────────────


def test_day0_upgrades_once_and_day7_immutable(conn):
    _history(conn, "u1", 20)
    _post(conn, "px", "u1", 1000, NOW - timedelta(days=10))
    # Bootstrap-style first stamp: day0/provisional.
    run_label_pass(conn, core_handles={"u1"}, now=NOW, bootstrap=True)
    r = _labels(conn)["px"]
    assert r[2] == "day0_heuristic" and r[4] is True
    # Daily pass: single upgrade to day7.
    stats = run_label_pass(conn, core_handles={"u1"}, now=NOW)
    assert stats["upgraded_day7"] == 1
    assert _labels(conn)["px"][2] == "day7_matched"

    # Re-run: no further upgrade; day7 kept untouched.
    stats = run_label_pass(conn, core_handles={"u1"}, now=NOW)
    assert stats["upgraded_day7"] == 0
    assert stats["kept_day7"] == 1
    assert _labels(conn)["px"][2] == "day7_matched"


def test_version_bump_recomputes(conn):
    _history(conn, "u1", 20)
    _post(conn, "px", "u1", 1000, NOW - timedelta(days=10))
    run_label_pass(conn, core_handles={"u1"}, now=NOW, bootstrap=True)
    # Simulate a stale-version row (estimator bumped without a re-run).
    conn.execute(
        "UPDATE ig_post_labels SET label_version = ? WHERE post_id = 'px'",
        [LABEL_VERSION + 1],
    )
    stats = run_label_pass(conn, core_handles={"u1"}, now=NOW)
    assert stats["stamped"] == 21  # every provisional row recomputed
    assert _labels(conn)["px"][5] == LABEL_VERSION


# ── Bootstrap guard (US-L3) ─────────────────────────────────────────────────


def test_bootstrap_never_writes_day7(conn):
    _history(conn, "u1", 20)
    _post(conn, "px", "u1", 1000, NOW - timedelta(days=365))
    run_label_pass(conn, core_handles={"u1"}, now=NOW, bootstrap=True)
    day7 = conn.execute(
        "SELECT COUNT(*) FROM ig_post_labels WHERE method = 'day7_matched'"
    ).fetchone()[0]
    assert day7 == 0
    r = _labels(conn)["px"]
    assert r[2] == "day0_heuristic" and r[4] is True


def test_bootstrap_idempotent(conn):
    _history(conn, "u1", 20)
    _post(conn, "px", "u1", 1000, NOW - timedelta(days=365))
    run_label_pass(conn, core_handles={"u1"}, now=NOW, bootstrap=True)
    n1 = conn.execute("SELECT COUNT(*) FROM ig_post_labels").fetchone()[0]
    run_label_pass(conn, core_handles={"u1"}, now=NOW, bootstrap=True)
    n2 = conn.execute("SELECT COUNT(*) FROM ig_post_labels").fetchone()[0]
    assert n1 == n2


# ── Floor-filler top-up ──────────────────────────────────────────────────────


def test_floor_filler_top_up(conn):
    # Creator with one standout; promotion tops up to the 3-post floor minus
    # the deterministic controls already approved.
    _history(conn, "u1", 20)
    _post(conn, "px", "u1", 5000, NOW - timedelta(days=10))
    run_label_pass(conn, core_handles={"u1"}, now=NOW)
    decisions = [r[3] for r in _labels(conn).values()]
    assert decisions.count("standout") == 1
    controls = decisions.count("control")
    assert decisions.count("floor_filler") == max(0, 2 - controls)


def test_floor_filler_requires_standout(conn):
    _history(conn, "u1", 20)
    run_label_pass(conn, core_handles={"u1"}, now=NOW)
    decisions = [r[3] for r in _labels(conn).values()]
    assert "floor_filler" not in decisions


# ── Integration: asset wiring ────────────────────────────────────────────────


def test_label_pass_asset_and_schedule(tmp_path, ops):
    from datalake.defs.common.schedules import daily_medallion
    from datalake.defs.instagram.assets import ig_post_labels

    _core_ops(ops, "alice")
    db = DuckDBResource(database=str(tmp_path / "state.duckdb"))
    with db.get_connection() as conn:
        for t in ("silver_ig_posts", "silver_ig_post_observations",
                  "ig_post_labels", "gold_analyses"):
            conn.execute(duckdb_ddl(t))
        _history(conn, "alice", 20, username="alice")
        _post(conn, "px", "alice", 1000, NOW - timedelta(days=10),
              username="alice")

    ig_post_labels(duckdb=db, ops=ops)

    with db.get_connection() as conn:
        rows = {
            r[0]: r
            for r in conn.execute(
                "SELECT post_id, label, method, enrich_decision FROM ig_post_labels"
            ).fetchall()
        }
    assert len(rows) == 21
    # alice is a core handle and px is 10d old → day7 matched
    assert rows["px"][2] == "day7_matched"
    # Schedule now drives the label pass
    assert "ig_post_labels" in repr(daily_medallion.target)
