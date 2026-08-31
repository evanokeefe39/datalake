"""Tests for the label-backed outlier serving views (US-D2).

``v_engagement_outliers`` tiers come from ``ig_post_labels`` (no lifetime
z-score, no future-leak); ``v_outlier_posts`` and ``v_creator_outlier_rate``
consume that view.
"""

from __future__ import annotations

import pytest
from dagster import build_asset_context
from dagster_duckdb import DuckDBResource

from datalake.defs.serving.assets import (
    v_creator_outlier_rate as _v_creator_outlier_rate,
)
from datalake.defs.serving.assets import (
    v_engagement_outliers as _v_engagement_outliers,
)
from datalake.defs.serving.assets import (
    v_outlier_posts as _v_outlier_posts,
)


@pytest.fixture
def db(tmp_path) -> DuckDBResource:
    """DuckDB resource seeded with minimal v_post_detail + ig_post_labels."""
    resource = DuckDBResource(database=str(tmp_path / "outliers.duckdb"))
    with resource.get_connection() as con:
        con.execute(
            """
            CREATE TABLE v_post_detail (
                post_id TEXT, owner_id TEXT, owner_username TEXT,
                creator_id INTEGER, likes_count BIGINT
            )
            """
        )
        con.execute(
            """
            CREATE TABLE ig_post_labels (
                post_id TEXT PRIMARY KEY, label TEXT, method TEXT,
                is_provisional BOOLEAN, baseline_center DOUBLE,
                baseline_spread DOUBLE
            )
            """
        )
        con.execute(
            """
            INSERT INTO v_post_detail VALUES
            ('p1', 'o1', 'jane', 1, 900),
            ('p2', 'o1', 'jane', 1, 800),
            ('p3', 'o1', 'jane', 1, 100),
            ('p4', 'o2', 'bob',  2, 50)
            """
        )
        con.execute(
            """
            INSERT INTO ig_post_labels VALUES
            ('p1', 'standout', 'day7_matched', FALSE, 100, 50),
            ('p2', 'standout', 'day0_heuristic', TRUE, 100, 50),
            ('p3', 'average', 'day0_heuristic', TRUE, 100, 50),
            ('p4', 'pending', 'pending', TRUE, NULL, NULL)
            """
        )
    return resource


def _run_view_assets(db: DuckDBResource) -> None:
    ctx = build_asset_context(resources={"duckdb": db})
    _v_engagement_outliers(ctx)
    _v_outlier_posts(ctx)
    _v_creator_outlier_rate(ctx)


def test_tiers_are_label_backed(db):
    """Standout-labeled posts get positive tiers; unlabeled fall to normal."""
    _run_view_assets(db)
    with db.get_connection() as con:
        tiers = dict(con.execute(
            "SELECT post_id, sigma_tier FROM v_engagement_outliers"
        ).fetchall())
    assert tiers["p1"] == "3σ+"   # z = (900-100)/50 = 16
    assert tiers["p2"] == "3σ+"
    assert tiers["p3"] == "normal"  # average label, z = 0
    assert tiers["p4"] == "normal"  # pending / no usable baseline


def test_no_lifetime_zscore_view(db):
    """The view must not rank posts without a standout label as outliers."""
    _run_view_assets(db)
    with db.get_connection() as con:
        n = con.execute(
            "SELECT COUNT(*) FROM v_engagement_outliers "
            "WHERE sigma_tier IN ('1σ', '2σ', '3σ+')"
        ).fetchone()[0]
    assert n == 2  # exactly the two standout-labeled posts


def test_v_outlier_posts_and_creator_rate_consistent(db):
    """v_outlier_posts and v_creator_outlier_rate agree with the tiers."""
    _run_view_assets(db)
    with db.get_connection() as con:
        assert con.execute(
            "SELECT COUNT(*) FROM v_outlier_posts"
        ).fetchone()[0] == 2
        rows = con.execute(
            "SELECT owner_username, outlier_posts, outlier_rate, total_posts "
            "FROM v_creator_outlier_rate"
        ).fetchall()
    by_owner = {r[0]: (r[1], r[2], r[3]) for r in rows}
    assert by_owner["jane"] == (2, 2 / 3, 3)
    assert by_owner["bob"][0] == 0


def test_views_expose_label_metadata(db):
    """The rewired view surfaces method/provisional for downstream triage."""
    _run_view_assets(db)
    with db.get_connection() as con:
        row = con.execute(
            "SELECT method, is_provisional FROM v_engagement_outliers "
            "WHERE post_id = 'p1'"
        ).fetchone()
    assert row == ("day7_matched", False)
