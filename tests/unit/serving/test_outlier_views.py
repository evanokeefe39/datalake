"""Tests for the label-backed outlier serving views (US-D2, US-A4.1).

``v_engagement_outliers`` tiers come from ``ig_post_labels`` (no lifetime
z-score, no future-leak); ``v_outlier_posts`` and ``v_creator_outlier_rate``
consume that view. US-A4.1: the collapsed negative bucket is split by
magnitude ('-1σ'/'-2σ'/'-3σ') and exposed via ``v_underperformer_posts`` +
``v_creator_underperformer_rate``.
"""

from __future__ import annotations

import pytest
from dagster import build_asset_context
from dagster_duckdb import DuckDBResource

from datalake.defs.serving.assets import (
    v_creator_outlier_rate as _v_creator_outlier_rate,
)
from datalake.defs.serving.assets import (
    v_creator_underperformer_rate as _v_creator_underperformer_rate,
)
from datalake.defs.serving.assets import (
    v_engagement_outliers as _v_engagement_outliers,
)
from datalake.defs.serving.assets import (
    v_outlier_posts as _v_outlier_posts,
)
from datalake.defs.serving.assets import (
    v_underperformer_posts as _v_underperformer_posts,
)


@pytest.fixture
def db(tmp_path) -> DuckDBResource:
    """DuckDB resource seeded with minimal v_post_detail + ig_post_labels.

    Baselines are chosen so ``likes_zscore`` is a controlled quantity:
    z = (likes - baseline_center) / baseline_spread, rounded to 2dp.
    """
    resource = DuckDBResource(database=str(tmp_path / "outliers.duckdb"))
    with resource.get_connection() as con:
        con.execute(
            """
            CREATE TABLE v_post_detail (
                post_id TEXT, owner_id TEXT, owner_username TEXT,
                creator_id INTEGER, likes_count BIGINT,
                timestamp TIMESTAMP, comments_count BIGINT,
                video_view_count BIGINT,
                gold_topic TEXT, gold_subtopic TEXT, gold_domain TEXT,
                gold_subdomain TEXT, content_type TEXT, format TEXT,
                style TEXT, admiralty TEXT, is_educational BOOLEAN,
                is_actionable BOOLEAN, result_json TEXT
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
            INSERT INTO v_post_detail
                (post_id, owner_id, owner_username, creator_id,
                 likes_count, timestamp, comments_count, video_view_count,
                 gold_topic, gold_subtopic, gold_domain, gold_subdomain,
                 content_type, format, style, admiralty,
                 is_educational, is_actionable, result_json)
            VALUES
            -- positives / control rows (US-D2 semantics)
            ('p1', 'o1', 'jane', 1, 900, NULL, NULL, NULL,
             NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
            ('p2', 'o1', 'jane', 1, 800, NULL, NULL, NULL,
             NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
            ('p3', 'o1', 'jane', 1, 100, NULL, NULL, NULL,
             NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
            ('p4', 'o2', 'bob',  2, 50,  NULL, NULL, NULL,
             NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
            -- negative boundary matrix (carol): center 0 unless noted
            ('p5',  'o3', 'carol', 3, -3, '2026-01-01', 1, 10,
             'tooling', 'cli', 'devtools', 'tooling', 'tutorial', 'carousel',
             'terse', 'B', TRUE, TRUE, '{"admiralty": "B"}'),
            ('p6',  'o3', 'carol', 3, -7, NULL, NULL, NULL,
             NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
            ('p7',  'o3', 'carol', 3, -5, NULL, NULL, NULL,
             NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
            ('p8',  'o3', 'carol', 3, -4, NULL, NULL, NULL,
             NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
            ('p9',  'o3', 'carol', 3, -3, NULL, NULL, NULL,
             NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
            ('p10', 'o3', 'carol', 3, -2, NULL, NULL, NULL,
             NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
            ('p11', 'o3', 'carol', 3, -1, NULL, NULL, NULL,
             NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
            """
        )
        con.execute(
            """
            INSERT INTO ig_post_labels VALUES
            ('p1',  'standout', 'day7_matched',   FALSE, 100, 50),
            ('p2',  'standout', 'day0_heuristic', TRUE,  100, 50),
            ('p3',  'average',  'day0_heuristic', TRUE,  100, 50),
            ('p4',  'pending',  'pending',        TRUE,  NULL, NULL),
            ('p5',  'average',  'day7_matched',   FALSE, 0,   1),
            ('p6',  'average',  'day7_matched',   FALSE, 0,   2),
            ('p7',  'average',  'day7_matched',   FALSE, 0,   2),
            ('p8',  'average',  'day7_matched',   FALSE, 0,   2),
            ('p9',  'average',  'day7_matched',   FALSE, 0,   2),
            ('p10', 'average',  'day7_matched',   FALSE, 0,   2),
            ('p11', 'average',  'day7_matched',   FALSE, 0,   2)
            """
        )
    return resource


def _run_view_assets(db: DuckDBResource) -> None:
    ctx = build_asset_context(resources={"duckdb": db})
    _v_engagement_outliers(ctx)
    _v_outlier_posts(ctx)
    _v_creator_outlier_rate(ctx)
    _v_underperformer_posts(ctx)
    _v_creator_underperformer_rate(ctx)


def test_tiers_are_label_backed(db):
    """Standout-labeled posts get positive tiers; unlabeled fall to normal."""
    _run_view_assets(db)
    with db.get_connection() as con:
        tiers = dict(con.execute(
            "SELECT post_id, sigma_tier FROM v_engagement_outliers"
        ).fetchall())
    assert tiers["p1"] == "3σ+"   # z = (900-100)/50 = 16
    assert tiers["p2"] == "3σ+"
    assert tiers["p3"] == "normal"
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


def test_negative_magnitude_split_boundaries(db):
    """Negatives split by magnitude: -3.5→-3σ, -2.5→-2σ, -1.5→-1σ, -0.5→normal."""
    _run_view_assets(db)
    with db.get_connection() as con:
        tiers = dict(con.execute(
            "SELECT post_id, sigma_tier FROM v_engagement_outliers"
        ).fetchall())
    assert tiers["p5"] == "-3σ"    # z = -3.0 exactly (inclusive boundary)
    assert tiers["p6"] == "-3σ"    # z = -3.5
    assert tiers["p7"] == "-2σ"    # z = -2.5 lands in -2σ
    assert tiers["p8"] == "-2σ"    # z = -2.0 exactly
    assert tiers["p9"] == "-1σ"    # z = -1.5 lands in -1σ
    assert tiers["p10"] == "-1σ"   # z = -1.0 exactly (inclusive boundary)
    assert tiers["p11"] == "normal"  # z = -0.5, not an outlier


def test_positive_semantics_unchanged(db):
    """Positive tiers and standout-only logic are byte-identical to pre-A4.1."""
    _run_view_assets(db)
    with db.get_connection() as con:
        rows = con.execute(
            "SELECT post_id, sigma_tier, label FROM v_engagement_outliers "
            "WHERE sigma_tier IN ('1σ', '2σ', '3σ+')"
        ).fetchall()
    assert {r[0]: r[1] for r in rows} == {"p1": "3σ+", "p2": "3σ+"}
    assert all(r[2] == "standout" for r in rows)


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
    # negatives never leak into the positive surface
    assert by_owner["carol"][0] == 0


def test_v_underperformer_posts_filters_negatives_only(db):
    """The negative surface returns exactly the negative-tier posts."""
    _run_view_assets(db)
    with db.get_connection() as con:
        rows = con.execute(
            "SELECT post_id, sigma_tier, owner_username, likes_count "
            "FROM v_underperformer_posts"
        ).fetchall()
    tiers = {r[0]: r[1] for r in rows}
    assert set(tiers.values()) <= {"-1σ", "-2σ", "-3σ"}
    assert set(tiers) == {"p5", "p6", "p7", "p8", "p9", "p10"}
    assert all(r[2] == "carol" for r in rows)


def test_v_underperformer_posts_carries_enrichment(db):
    """Underperformer rows carry enrichment/format attributes for EDA."""
    _run_view_assets(db)
    with db.get_connection() as con:
        row = con.execute(
            "SELECT gold_topic, gold_subtopic, gold_domain, content_type, "
            "format, admiralty, is_educational, result_json, timestamp, "
            "comments_count, video_view_count "
            "FROM v_underperformer_posts WHERE post_id = 'p5'"
        ).fetchone()
    assert row == (
        "tooling", "cli", "devtools", "tutorial", "carousel", "B",
        True, '{"admiralty": "B"}',
        __import__("datetime").datetime(2026, 1, 1), 1, 10,
    )


def test_v_creator_underperformer_rate(db):
    """Creator-level underperformer rate mirrors the positive rate view."""
    _run_view_assets(db)
    with db.get_connection() as con:
        rows = con.execute(
            "SELECT owner_username, underperformer_posts, "
            "underperformer_rate, total_posts, min_zscore "
            "FROM v_creator_underperformer_rate"
        ).fetchall()
    by_owner = {r[0]: r[1:] for r in rows}
    assert by_owner["carol"] == (6, 6 / 7, 7, -3.5)
    assert by_owner["jane"] == (0, 0.0, 3, 0.0)  # min z incl. average post p3
    assert by_owner["bob"] == (0, 0.0, 1, None)


def test_views_expose_label_metadata(db):
    """The rewired view surfaces method/provisional for downstream triage."""
    _run_view_assets(db)
    with db.get_connection() as con:
        row = con.execute(
            "SELECT method, is_provisional FROM v_engagement_outliers "
            "WHERE post_id = 'p1'"
        ).fetchone()
    assert row == ("day7_matched", False)
