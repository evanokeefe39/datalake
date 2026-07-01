"""Tests for the serving layer assets (profile_dimension, analytics_views).

Gap-fills per test-hardening plan:
- SCD2 integrity, no overlapping intervals, no gaps, multi-owner→profile_key
"""

from __future__ import annotations

import pytest
from dagster import build_asset_context

from datalake.defs.serving.assets import (
    analytics_views as _analytics_views_asset,
    profile_dimension as _profile_dimension_asset,
)

from tests.fixtures.silver_factories import seed_silver_posts


def _run_profile_dimension(ctx):
    _profile_dimension_asset(ctx)


def _run_analytics_views(ctx):
    _analytics_views_asset(ctx)


# ── Profile dimension tests ────────────────────────────────────────────────


def test_profile_dimension_creates_rows(db):
    """Distinct owner_ids from silver_ig_posts → rows in profile_dimension."""
    seed_silver_posts(
        db,
        [("1", "owner_a", "user_a", "Post 1"),
         ("2", "owner_b", "user_b", "Post 2")],
        caption_idx=3,
        owner_id_idx=1,
        owner_username_idx=2,
    )
    _run_profile_dimension(build_asset_context(resources={"duckdb": db}))

    with db.get_connection() as conn:
        rows = conn.execute(
            "SELECT owner_id, owner_username, is_current "
            "FROM dim_profile ORDER BY owner_id"
        ).fetchall()
    assert rows == [
        ("owner_a", "user_a", True),
        ("owner_b", "user_b", True),
    ]


def test_profile_dimension_scd2_username_change(db):
    """Same owner with new username → closes old row, inserts new."""
    seed_silver_posts(
        db,
        [("1", "owner_a", "old_name", "Post"),
         ("2", "owner_a", "new_name", "Another post")],
        caption_idx=3,
        owner_id_idx=1,
        owner_username_idx=2,
    )
    _run_profile_dimension(build_asset_context(resources={"duckdb": db}))

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


def test_profile_dimension_no_change_idempotent(db):
    """Same owner, same username → no new rows added."""
    seed_silver_posts(
        db,
        [("1", "owner_a", "user_a", "Post")],
        caption_idx=3,
        owner_id_idx=1,
        owner_username_idx=2,
    )
    _run_profile_dimension(build_asset_context(resources={"duckdb": db}))

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
def test_scd2_integrity(db, rows, expected_ranges):
    """SCD2 invariants: effective_from ≤ effective_to, no overlaps, no gaps."""
    seed_silver_posts(
        db, rows,
        caption_idx=3,
        owner_id_idx=1,
        owner_username_idx=2,
    )
    _run_profile_dimension(build_asset_context(resources={"duckdb": db}))

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


# ── Analytics views tests ──────────────────────────────────────────────────


def test_analytics_views_joins_correctly(db):
    """analytics_views joins silver_ig_posts with profile_dimension."""
    seed_silver_posts(
        db,
        [("1", "owner_a", "user_a", "Test post")],
        caption_idx=3,
        owner_id_idx=1,
        owner_username_idx=2,
    )
    ctx = build_asset_context(resources={"duckdb": db})
    _run_profile_dimension(ctx)
    _run_analytics_views(ctx)

    with db.get_connection() as conn:
        row = conn.execute(
            "SELECT post_id, owner_username, owner_id, is_current "
            "FROM analytics_views"
        ).fetchone()
    assert row == ("1", "user_a", "owner_a", True)


def test_analytics_views_empty_data(db):
    """analytics_views runs cleanly with empty silver_ig_posts."""
    seed_silver_posts(db, [])
    context = build_asset_context(resources={"duckdb": db})
    _run_profile_dimension(context)
    _run_analytics_views(context)

    with db.get_connection() as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM analytics_views"
        ).fetchone()[0]
    assert count == 0
