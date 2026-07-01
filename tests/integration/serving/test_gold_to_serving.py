"""Integration tests: gold DuckDB state → serving layer.

Tests the cross-asset boundary between ``ig_posts_gld`` (gold output in DuckDB)
and serving assets (``profile_dimension``, ``analytics_views``). Uses shared
DuckDB persistence and real asset execution.

Per test-hardening plan Phase 2:
- analytics_views with NULL gold_ig_analyses join (all columns NULL)
- SCD2 effective_to precision: synchronous with next row's effective_from
- Cross-domain channel attribute: instagram rows have channel='instagram'
"""

import pytest
from dagster import build_asset_context
from dagster_duckdb import DuckDBResource

from datalake.defs.serving.assets import analytics_views, profile_dimension

from tests.fixtures.silver_factories import seed_silver_posts


def _run_profile_dimension(duckdb):
    profile_dimension(build_asset_context(resources={"duckdb": duckdb}))


def _run_analytics_views(duckdb):
    analytics_views(build_asset_context(resources={"duckdb": duckdb}))


# ── Test: NULL gold_ig_analyses join ──────────────────────────────────────


def test_analytics_views_null_join(tmp_path):
    """GIVEN silver posts exist but gold has NOT been run
    WHEN analytics_views is created
    THEN the view returns rows with NULL gold columns (LEFT JOIN).
    """
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))
    seed_silver_posts(
        duckdb,
        [("1", "owner_a", "user_a", "Post without gold analysis")],
        caption_idx=3,
        owner_id_idx=1,
        owner_username_idx=2,
    )

    _run_profile_dimension(duckdb)
    _run_analytics_views(duckdb)

    with duckdb.get_connection() as conn:
        rows = conn.execute(
            "SELECT post_id, result_json, gold_analysed_at, profile_key, channel "
            "FROM analytics_views"
        ).fetchall()

    assert len(rows) == 1
    post_id, result_json, gold_analysed_at, profile_key, channel = rows[0]
    assert post_id == "1"
    # Gold columns are NULL since gold_ig_analyses was never populated
    assert result_json is None
    assert gold_analysed_at is None
    # Profile dimension should exist
    assert profile_key is not None
    assert channel == "instagram"


# ── Test: SCD2 effective_to precision ─────────────────────────────────────


def test_scd2_effective_to_precision(tmp_path):
    """GIVEN a profile with one username, then updated to a new one
    WHEN profile_dimension runs after each state
    THEN effective_to of the closed row equals effective_from of the new row.
    """
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))

    # ── Pass 1: seed with old username, run profile dimension ─────────────
    seed_silver_posts(
        duckdb,
        [("1", "owner_a", "old_name", "First post")],
        caption_idx=3,
        owner_id_idx=1,
        owner_username_idx=2,
    )
    _run_profile_dimension(duckdb)

    with duckdb.get_connection() as conn:
        c1 = conn.execute(
            "SELECT COUNT(*) FROM dim_profile WHERE owner_id = 'owner_a'"
        ).fetchone()[0]
    assert c1 == 1, f"Expected 1 row after pass 1, got {c1}"

    # ── Pass 2: update existing post's username, re-run profile ──────────
    # Update in-place so DISTINCT only returns the new tuple
    with duckdb.get_connection() as conn:
        conn.execute(
            "UPDATE silver_ig_posts SET owner_username = 'new_name' "
            "WHERE post_id = '1'"
        )
    _run_profile_dimension(duckdb)
    with duckdb.get_connection() as conn:
        c2 = conn.execute(
            "SELECT COUNT(*) FROM dim_profile WHERE owner_id = 'owner_a'"
        ).fetchone()[0]
        assert c2 == 2, f"Expected 2 rows after pass 2, got {c2}"

        rows = conn.execute(
            "SELECT owner_username, effective_from, effective_to, is_current "
            "FROM dim_profile WHERE owner_id = 'owner_a' "
            "ORDER BY effective_from"
        ).fetchall()

    # Row 0: old_name (closed), Row 1: new_name (current)
    old_username, old_from, old_to, old_current = rows[0]
    new_username, new_from, new_to, new_current = rows[1]

    assert old_username == "old_name", f"Expected old_name, got {old_username}"
    assert not old_current, f"Expected old row closed, got is_current={old_current}"
    assert old_to is not None, "Old row should have effective_to set"

    assert new_username == "new_name", f"Expected new_name, got {new_username}"
    assert new_current, f"Expected new row current, got is_current={new_current}"
    assert new_to is None, "New row should not have effective_to"

    # effective_to of old row == effective_from of new row (same transaction)
    assert old_to == new_from, (
        "SCD2 gap/precision violation: effective_to of closed row "
        f"({old_to}) != effective_from of new row ({new_from})"
    )

# ── Test: cross-domain channel attribute ──────────────────────────────────


def test_channel_attribute_on_instagram_rows(tmp_path):
    """GIVEN silver posts have instagram data
    WHEN profile_dimension runs
    THEN dim_profile has channel='instagram' for those rows.
    """
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))

    seed_silver_posts(
        duckdb,
        [("1", "owner_a", "user_a", "Post 1"),
         ("2", "owner_b", "user_b", "Post 2")],
        caption_idx=3,
        owner_id_idx=1,
        owner_username_idx=2,
    )

    _run_profile_dimension(duckdb)

    with duckdb.get_connection() as conn:
        rows = conn.execute(
            "SELECT owner_id, channel FROM dim_profile ORDER BY owner_id"
        ).fetchall()

    assert len(rows) == 2
    for owner_id, channel in rows:
        assert channel == "instagram", (
            f"Expected channel='instagram' for {owner_id}, got '{channel}'"
        )
