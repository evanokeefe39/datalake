"""Tests for the label-backed standout/hot/weekly-summary endpoints (US-D1).

The endpoints read ``ig_post_labels`` (``label='standout'``) instead of
computing lifetime z-scores; day7_matched rows are preferred over
provisional day0_heuristic rows in ranking.
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pytest
from fastapi.testclient import TestClient

_SERVER_PATH = Path(__file__).resolve().parents[3] / "dashboard" / "server.py"
_spec = importlib.util.spec_from_file_location("dashboard_server", _SERVER_PATH)
assert _spec and _spec.loader, "dashboard/server.py not found"
server = importlib.util.module_from_spec(_spec)
sys.modules["dashboard_server"] = server
_spec.loader.exec_module(server)


@pytest.fixture
def label_db(tmp_path, monkeypatch):
    """DuckDB seeded with posts + Tukey labels for the standout endpoints."""
    db_path = tmp_path / "state.duckdb"
    monkeypatch.setattr(server, "DB_PATH", db_path)
    con = duckdb.connect(str(db_path))
    con.execute(
        """
        CREATE TABLE silver_ig_posts (
            post_id TEXT, owner_id TEXT, owner_username TEXT, shortcode TEXT,
            caption TEXT, likes_count BIGINT, comments_count BIGINT,
            video_view_count BIGINT, timestamp TIMESTAMP
        )
        """
    )
    con.execute(
        """
        CREATE TABLE ig_post_labels (
            post_id TEXT PRIMARY KEY, label TEXT, method TEXT,
            enrich_decision TEXT, is_provisional BOOLEAN,
            baseline_center DOUBLE, baseline_spread DOUBLE
        )
        """
    )
    con.execute(
        """
        CREATE TABLE dim_profile (
            owner_id TEXT, is_current BOOLEAN, creator_id INTEGER, channel TEXT
        )
        """
    )
    # jane: two standouts — day7-ranked above day0; one average post.
    con.execute(
        """
        INSERT INTO silver_ig_posts VALUES
        ('s7', 'o1', 'jane', 'sc7', 'day7 hit', 900, 5, 0,
         CURRENT_DATE - INTERVAL '26' DAY),
        ('s0', 'o1', 'jane', 'sc0', 'day0 hit', 800, 4, 0,
         CURRENT_DATE - INTERVAL '25' DAY),
        ('avg', 'o1', 'jane', 'sca', 'normal', 100, 1, 0,
         CURRENT_DATE - INTERVAL '24' DAY)
        """
    )
    con.execute(
        """
        INSERT INTO ig_post_labels VALUES
        ('s7', 'standout', 'day7_matched', 'standout', FALSE, 100, 50),
        ('s0', 'standout', 'day0_heuristic', 'standout', TRUE, 100, 50),
        ('avg', 'average', 'day0_heuristic', 'floor_filler', TRUE, 100, 50)
        """
    )
    con.execute("INSERT INTO dim_profile VALUES ('o1', TRUE, 7, 'instagram')")
    # Minimal mirror of v_post_metrics + v_standout_calendar (canonical views
    # live in defs/serving/assets.py; real-DB coherence is covered elsewhere).
    con.execute("""
        CREATE VIEW v_post_metrics_base AS
        SELECT
            sp.post_id, sp.owner_username, dp.creator_id, dp.channel,
            sp.likes_count, sp.comments_count, sp.video_view_count,
            sp.timestamp, sp.shortcode, sp.caption,
            l.label, l.method, l.is_provisional,
            ROUND((sp.likes_count - l.baseline_center)
                  / NULLIF(l.baseline_spread, 0), 2)         AS likes_zscore,
            l.baseline_center                               AS baseline_q3,
            l.baseline_spread                               AS baseline_iqr,
            CASE WHEN l.label = 'standout' THEN 1 ELSE 0 END AS is_standout,
            CASE WHEN l.label = 'standout'
                  AND (sp.likes_count - l.baseline_center)
                      / NULLIF(l.baseline_spread, 0) >= 2
                 THEN 1 ELSE 0 END                          AS is_hot,
            CASE WHEN l.label = 'standout'
                  AND (sp.likes_count - l.baseline_center)
                      / NULLIF(l.baseline_spread, 0) >= 2 THEN 'hot'
                 WHEN l.label = 'standout' THEN 'standout'
            END                                             AS relative_performance,
            ROUND(sp.likes_count / NULLIF(l.baseline_center, 0), 1)
                                                            AS breakout_multiple,
            ROW_NUMBER() OVER (
                PARTITION BY sp.owner_username
                ORDER BY (sp.likes_count - l.baseline_center)
                         / NULLIF(l.baseline_spread, 0) DESC
            )                                               AS owner_rank
        FROM silver_ig_posts sp
        JOIN ig_post_labels l ON sp.post_id = l.post_id
        LEFT JOIN dim_profile dp
            ON sp.owner_id = dp.owner_id AND dp.is_current = TRUE
    """)
    con.execute("""
        CREATE VIEW v_post_metrics AS
        SELECT base.*,
               CASE WHEN base.is_standout = 1 AND base.owner_rank <= 3
                    THEN 1 ELSE 0 END AS is_top3_in_owner
        FROM v_post_metrics_base base
    """)
    con.execute("""
        CREATE VIEW v_recent_hot_posts AS
        WITH recent AS (
            SELECT *
            FROM v_post_metrics
            WHERE is_hot = 1
              AND timestamp >= CURRENT_DATE - INTERVAL '28' DAY
        ),
        ranked AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY owner_username
                       ORDER BY likes_zscore DESC NULLS LAST,
                                likes_count DESC NULLS LAST
                   ) AS recent_rank
            FROM recent
        )
        SELECT * FROM ranked WHERE recent_rank <= 3
    """)
    con.execute("""
        CREATE VIEW v_standout_calendar AS
        SELECT EXTRACT(DAY FROM timestamp) AS day_of_month,
               SUM(is_standout)            AS standout_count
        FROM v_post_metrics
        WHERE is_standout = 1
        GROUP BY day_of_month
    """)
    con.close()
    return db_path


def test_standout_posts_returns_label_backed_posts(label_db):
    resp = TestClient(server.app).get("/api/standout-posts")
    assert resp.status_code == 200
    rows = resp.json()
    # Only standout-labeled posts; day7_matched ranked ahead of day0_heuristic.
    assert [r["post_id"] for r in rows] == ["s7", "s0"]
    assert rows[0]["method"] == "day7_matched"
    assert rows[1]["provisional"] is True
    # Baseline stats come from the label pass, not a lifetime aggregate —
    # exposed with honest names (they are per-post trailing Tukey stats).
    assert rows[0]["baseline_q3"] == 100
    assert rows[0]["baseline_iqr"] == 50
    # Point-in-time: NO all-time creator-avg key on post rows (PR #26 defect
    # removed); context is the trailing baseline only.
    assert "creator_avg_likes" not in rows[0]


def test_standout_posts_excludes_non_standout_labels(label_db):
    resp = TestClient(server.app).get("/api/standout-posts?limit=100")
    assert resp.status_code == 200
    assert "avg" not in {r["post_id"] for r in resp.json()}


def test_hot_posts_ranks_per_creator_from_labels(label_db):
    resp = TestClient(server.app).get("/api/hot-posts")
    assert resp.status_code == 200
    rows = resp.json()
    assert [r["post_id"] for r in rows] == ["s7", "s0"]
    # Trailing Tukey stats with honest names; NO all-time creator-avg key on
    # post rows (point-in-time contract, PR #26 defect removed).
    assert rows[0]["baseline_q3"] == 100
    assert rows[0]["baseline_iqr"] == 50
    assert "creator_avg_likes" not in rows[0]
    assert "mean_likes" not in rows[0]


def test_weekly_summary_counts_standout_labels_by_day(label_db):
    resp = TestClient(server.app).get("/api/weekly-summary")
    assert resp.status_code == 200
    days = {r["day"]: r["standout_count"] for r in resp.json()}
    # Expected day-of-month derived from the fixture's relative timestamps
    # (CURRENT_DATE - 26/25 days) — hardcoded day numbers break across a
    # month boundary.
    d7 = (date.today() - timedelta(days=26)).day
    d0 = (date.today() - timedelta(days=25)).day
    assert days == {d7: 1, d0: 1}
