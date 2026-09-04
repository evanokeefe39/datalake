"""Tests for v_post_follower_context (Epic A2.2, US-A2.2).

Covers the follower-at-post-time attribution contract:
nearest at-or-after observation, same-source_dataset preference,
owner_id -> owner_username fallback, no-fabrication on missing
observations, and growth-tier bucketing boundaries.
"""

from __future__ import annotations

import pytest
from dagster import build_asset_context
from dagster_duckdb import DuckDBResource

from datalake.defs.serving.assets import (
    v_post_follower_context as _v_post_follower_context,
)


@pytest.fixture
def db(tmp_path) -> DuckDBResource:
    """DuckDB resource seeded with minimal v_post_detail + observations."""
    resource = DuckDBResource(database=str(tmp_path / "follower_ctx.duckdb"))
    with resource.get_connection() as con:
        con.execute(
            """
            CREATE TABLE v_post_detail (
                post_id TEXT, owner_id TEXT, owner_username TEXT,
                timestamp TIMESTAMPTZ, source_dataset TEXT
            )
            """
        )
        con.execute(
            """
            CREATE TABLE silver_ig_profile_observations (
                owner_id TEXT NOT NULL, owner_username TEXT,
                observed_at TIMESTAMPTZ NOT NULL,
                followers_count INTEGER, source_dataset TEXT NOT NULL,
                PRIMARY KEY (owner_id, observed_at, source_dataset)
            )
            """
        )
        con.execute(
            """
            INSERT INTO v_post_detail VALUES
            -- multiple observations after the post
            ('p_multi', 'o1', 'alice', '2026-01-10T00:00:00Z', 'ds_a'),
            -- only observations BEFORE the post -> NULL (no fabrication)
            ('p_stale', 'o2', 'bob',   '2026-06-01T00:00:00Z', 'ds_a'),
            -- no observation at all -> NULL
            ('p_none',  'o3', 'carol', '2026-01-10T00:00:00Z', 'ds_a'),
            -- no owner_id -> username fallback
            ('p_uname', NULL, 'dave',  '2026-01-10T00:00:00Z', 'ds_a'),
            -- same-dataset vs cross-dataset tie on observed_at
            ('p_pref',  'o5', 'erin',  '2026-01-10T00:00:00Z', 'ds_b'),
            -- exact boundary: obs == post timestamp qualifies (at-or-after)
            ('p_exact', 'o6', 'frank', '2026-01-10T12:00:00Z', 'ds_a')
            """
        )
        con.execute(
            """
            INSERT INTO silver_ig_profile_observations VALUES
            -- alice: two future observations; nearest (Jan 15) must win
            ('o1', 'alice', '2026-01-15T00:00:00Z',  500, 'ds_a'),
            ('o1', 'alice', '2026-03-01T00:00:00Z', 5000, 'ds_a'),
            -- bob: observation only BEFORE the post
            ('o2', 'bob',   '2026-01-01T00:00:00Z',  123, 'ds_a'),
            -- dave: only username matches
            ('other', 'dave', '2026-01-11T00:00:00Z', 750, 'ds_a'),
            -- erin: same-dataset (ds_b) and cross-dataset (ds_a) at equal ts
            ('o5', 'erin',  '2026-01-11T00:00:00Z', 1500, 'ds_a'),
            ('o5', 'erin',  '2026-01-11T00:00:00Z', 2500, 'ds_b'),
            -- frank: observation exactly at the post timestamp
            ('o6', 'frank', '2026-01-10T12:00:00Z',  12000, 'ds_a')
            """
        )
    return resource


def _run(db: DuckDBResource) -> None:
    _v_post_follower_context(build_asset_context(resources={"duckdb": db}))


def _rows(db: DuckDBResource) -> dict:
    with db.get_connection() as con:
        return {
            r[0]: r[1:]
            for r in con.execute(
                "SELECT post_id, followers_count, follower_observed_at, "
                "follower_source_dataset, follower_tier "
                "FROM v_post_follower_context"
            ).fetchall()
        }


def test_nearest_at_or_after_observation_wins(db):
    """Nearest at-or-after observation is chosen, not the latest one."""
    _run(db)
    row = _rows(db)["p_multi"]
    assert row[0] == 500  # Jan-15 (500), not Mar-01 (5000)
    assert row[2] == "ds_a"


def test_stale_and_missing_observations_are_null(db):
    """No fabricated growth: no at-or-after observation -> NULL context."""
    _run(db)
    rows = _rows(db)
    assert rows["p_stale"] == (None, None, None, None)
    assert rows["p_none"] == (None, None, None, None)


def test_owner_username_fallback(db):
    """Posts without owner_id attribute via owner_username."""
    _run(db)
    row = _rows(db)["p_uname"]
    assert row[0] == 750
    assert row[2] == "ds_a"


def test_same_source_dataset_preferred(db):
    """At an observed_at tie, the post's own source_dataset wins."""
    _run(db)
    row = _rows(db)["p_pref"]
    assert row[0] == 2500  # ds_b row, not the ds_a row
    assert row[2] == "ds_b"


def test_observation_exactly_at_post_timestamp_qualifies(db):
    """At-or-after is inclusive: obs == post timestamp attributes."""
    _run(db)
    row = _rows(db)["p_exact"]
    assert row[0] == 12000
    assert row[3] == "10k+"


def test_growth_tier_bucket_boundaries(tmp_path):
    """Bucket edges: 99/100, 999/1k, 9999/10k map to adjacent tiers."""
    resource = DuckDBResource(database=str(tmp_path / "tiers.duckdb"))
    with resource.get_connection() as con:
        con.execute(
            "CREATE TABLE v_post_detail (post_id TEXT, owner_id TEXT, "
            "owner_username TEXT, timestamp TIMESTAMPTZ, source_dataset TEXT)"
        )
        con.execute(
            "CREATE TABLE silver_ig_profile_observations ("
            "owner_id TEXT NOT NULL, owner_username TEXT, "
            "observed_at TIMESTAMPTZ NOT NULL, followers_count INTEGER, "
            "source_dataset TEXT NOT NULL, "
            "PRIMARY KEY (owner_id, observed_at, source_dataset))"
        )
        owners = ["a", "b", "c", "d", "e", "f"]
        con.execute(
            "INSERT INTO v_post_detail VALUES "
            + ", ".join(
                f"('{o}p', '{o}', 'u_{o}', '2026-01-01T00:00:00Z', 'ds')"
                for o in owners
            )
        )
        con.execute(
            "INSERT INTO silver_ig_profile_observations VALUES "
            + ", ".join(
                f"('{o}', 'u_{o}', '2026-01-02T00:00:00Z', {n}, 'ds')"
                for o, n in zip(owners, [99, 100, 999, 1000, 9999, 10000])
            )
        )
    _v_post_follower_context(build_asset_context(resources={"duckdb": resource}))
    with resource.get_connection() as con:
        tiers = dict(
            con.execute(
                "SELECT followers_count, follower_tier FROM "
                "v_post_follower_context"
            ).fetchall()
        )
    assert tiers[99] == "0-100"
    assert tiers[100] == "100-1k"
    assert tiers[999] == "100-1k"
    assert tiers[1000] == "1k-10k"
    assert tiers[9999] == "1k-10k"
    assert tiers[10000] == "10k+"
