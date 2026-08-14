"""Tests for the serving layer assets (profile_dimension, v_post_detail).

Gap-fills per test-hardening plan:
- SCD2 integrity, no overlapping intervals, no gaps, multi-owner→profile_key
"""

from __future__ import annotations

import pytest
from dagster import build_asset_context

from datalake.defs.serving.assets import (
    dim_date as _dim_date_asset,
)
from datalake.defs.serving.assets import (
    profile_dimension as _profile_dimension_asset,
)
from datalake.defs.serving.assets import (
    v_post_detail as _v_post_detail_asset,
)
from tests.fixtures.silver_factories import seed_silver_posts


def _ensure_gold_table(db):
    """Create an empty gold_analyses table so v_post_detail can LEFT JOIN it."""
    with db.get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS gold_analyses (
                post_id TEXT NOT NULL,
                domain TEXT NOT NULL DEFAULT 'instagram',
                prompt_hash TEXT,
                result_json TEXT,
                analysed_at TEXT NOT NULL,
                PRIMARY KEY (post_id, domain)
            )
        """)


def _run_profile_dimension(db, ops):
    _profile_dimension_asset(build_asset_context(resources={"duckdb": db, "ops": ops}))


def _run_v_post_detail(ctx):
    _dim_date_asset(ctx)
    _v_post_detail_asset(ctx)


# ── Profile dimension tests ────────────────────────────────────────────────


def test_profile_dimension_creates_rows(db, ops):
    """Distinct owner_ids from silver_ig_posts → rows in profile_dimension."""
    seed_silver_posts(
        db,
        [("1", "owner_a", "user_a", "Post 1"),
         ("2", "owner_b", "user_b", "Post 2")],
        caption_idx=3,
        owner_id_idx=1,
        owner_username_idx=2,
    )
    _run_profile_dimension(db, ops)

    with db.get_connection() as conn:
        rows = conn.execute(
            "SELECT owner_id, owner_username, is_current "
            "FROM dim_profile ORDER BY owner_id"
        ).fetchall()
    assert rows == [
        ("owner_a", "user_a", True),
        ("owner_b", "user_b", True),
    ]


def test_profile_dimension_scd2_username_change(db, ops):
    """Same owner with new username → closes old row, inserts new."""
    seed_silver_posts(
        db,
        [("1", "owner_a", "old_name", "Post"),
         ("2", "owner_a", "new_name", "Another post")],
        caption_idx=3,
        owner_id_idx=1,
        owner_username_idx=2,
    )
    _run_profile_dimension(db, ops)

    with db.get_connection() as conn:
        rows = conn.execute(
            "SELECT owner_username, is_current, effective_to IS NOT NULL "
            "FROM dim_profile WHERE owner_id = 'owner_a' "
            "ORDER BY effective_from"
        ).fetchall()
    assert len(rows) == 2
    usernames = {r[0] for r in rows}
    assert usernames == {"old_name", "new_name"}
    # One row is current (is_current=True, effective_to IS NULL)
    assert any(r[1] and not r[2] for r in rows)
    # One row is closed (is_current=False, effective_to IS NOT NULL)
    assert any(not r[1] and r[2] for r in rows)


def test_profile_dimension_no_change_idempotent(db, ops):
    """Same owner, same username → no new rows added."""
    seed_silver_posts(
        db,
        [("1", "owner_a", "user_a", "Post")],
        caption_idx=3,
        owner_id_idx=1,
        owner_username_idx=2,
    )
    _run_profile_dimension(db, ops)

    with db.get_connection() as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM dim_profile"
        ).fetchone()[0]
    assert count == 1


# ── SCD2 integrity parametrized scenarios ──────────────────────────────────


@pytest.mark.parametrize("rows,expected_ranges", [
    pytest.param(
        [("1", "owner_a", "user_a", "Post 1"),
         ("2", "owner_b", "user_b", "Post 2")],
        2,
        id="multiple_owners",
    ),
    pytest.param(
        [("1", "owner_a", "user_a", "Post 1"),
         ("2", "owner_a", "user_b", "Post 2")],
        2,
        id="same_owner_scd2",
    ),
])
def test_scd2_integrity(db, ops, rows, expected_ranges):
    """SCD2 invariants: effective_from ≤ effective_to, no overlaps, no gaps."""
    seed_silver_posts(
        db, rows,
        caption_idx=3,
        owner_id_idx=1,
        owner_username_idx=2,
    )
    _run_profile_dimension(db, ops)

    with db.get_connection() as conn:
        data = conn.execute(
            "SELECT owner_id, effective_from, effective_to "
            "FROM dim_profile ORDER BY owner_id, effective_from"
        ).fetchall()

    assert len(data) == expected_ranges

    by_key: dict[str, list] = {}
    for key, eff_from, eff_to in data:
        assert eff_from <= (eff_to or eff_from), "effective_from ≤ effective_to"
        by_key.setdefault(key, []).append((eff_from, eff_to))

    for key, intervals in by_key.items():
        for i in range(1, len(intervals)):
            prev_to = intervals[i - 1][1]
            curr_from = intervals[i][0]
            assert prev_to == curr_from, (
                f"No gap or overlap for {key}: "
                f"prev_to={prev_to}, curr_from={curr_from}"
            )


# ── v_post_detail tests ────────────────────────────────────────────────────


def test_v_post_detail_joins_correctly(db, ops):
    """v_post_detail joins silver_ig_posts with profile_dimension."""
    seed_silver_posts(
        db,
        [("1", "owner_a", "user_a", "Test post")],
        caption_idx=3,
        owner_id_idx=1,
        owner_username_idx=2,
    )
    _ensure_gold_table(db)
    _run_profile_dimension(db, ops)
    _run_v_post_detail(build_asset_context(resources={"duckdb": db}))

    with db.get_connection() as conn:
        row = conn.execute(
            "SELECT post_id, owner_username, owner_id, is_current "
            "FROM v_post_detail"
        ).fetchone()
    assert row == ("1", "user_a", "owner_a", True)


def test_v_post_detail_empty_data(db, ops):
    """v_post_detail runs cleanly with empty silver_ig_posts."""
    seed_silver_posts(db, [])
    _ensure_gold_table(db)
    _run_profile_dimension(db, ops)
    _run_v_post_detail(build_asset_context(resources={"duckdb": db}))

    with db.get_connection() as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM v_post_detail"
        ).fetchone()[0]
    assert count == 0


def test_profile_dimension_links_creator(db, ops):
    """dim_profile gains creator_id/creator_name from ops profiles/creators."""
    from datalake.defs.instagram.creators import add_profile, create_creator

    creator = create_creator(ops, "Jane Doe")
    add_profile(ops, creator_id=creator["id"], platform="instagram", handle="user_a")

    seed_silver_posts(
        db,
        [("1", "owner_a", "user_a", "Post")],
        caption_idx=3,
        owner_id_idx=1,
        owner_username_idx=2,
    )
    _run_profile_dimension(db, ops)

    with db.get_connection() as conn:
        row = conn.execute(
            "SELECT creator_id, creator_name FROM dim_profile WHERE owner_id = 'owner_a'"
        ).fetchone()
    assert row == (creator["id"], "Jane Doe")


def test_profile_dimension_unlinked_creator_null(db, ops):
    """A handle with no creator yields NULL creator_id/creator_name."""
    seed_silver_posts(
        db,
        [("1", "owner_a", "user_a", "Post")],
        caption_idx=3,
        owner_id_idx=1,
        owner_username_idx=2,
    )
    _run_profile_dimension(db, ops)

    with db.get_connection() as conn:
        row = conn.execute(
            "SELECT creator_id, creator_name FROM dim_profile WHERE owner_id = 'owner_a'"
        ).fetchone()
    assert row == (None, None)
