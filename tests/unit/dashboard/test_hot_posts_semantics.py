"""Semantic-contract tests for the Hot Posts / standout surfaces.

Regression guards, post metrics-centralization (2026-09-02):

1. POINT-IN-TIME: a post's context is its OWN trailing label-pass Tukey
   baseline (``baseline_q3``/``baseline_iqr``), never a creator all-time or
   current average. The Issue-B defect (baseline under ``mean_likes``) and the
   PR #26 defect (all-time ``creator_avg_likes`` on post rows) must both stay
   dead.
2. HONEST NAMES: the trailing baseline is exposed only under
   ``baseline_q3``/``baseline_iqr``; no "avg"-implying key on post rows.
3. HOT TIER: ``hot`` = standout AND z >= 2 (2σ+).
4. CROSS-SURFACE: creator ``avg_likes`` on the creators surface is the same
   true mean as computed from the posts.

The fixtures materialize minimal mirror views of ``v_post_metrics`` /
``v_creator_metrics`` / ``v_standout_calendar`` (same grain and semantics as
``defs/serving/assets.py``) over a tiny label-backed dataset. Real-DB
coherence of the actual views is covered by the live smoke +
``tests/operational/test_state_compatibility.py``.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import duckdb
import pytest
from fastapi.testclient import TestClient

_SERVER_PATH = Path(__file__).resolve().parents[3] / "dashboard" / "server.py"
_spec = importlib.util.spec_from_file_location("dashboard_server_semantics", _SERVER_PATH)
assert _spec and _spec.loader
server = importlib.util.module_from_spec(_spec)
sys.modules["dashboard_server_semantics"] = server
_spec.loader.exec_module(server)

_V_POST_METRICS = """
CREATE VIEW v_post_metrics_base AS
SELECT
    sp.post_id, sp.owner_username, dp.creator_id, dp.channel,
    sp.likes_count, sp.comments_count, sp.video_view_count,
    sp.timestamp, sp.shortcode, sp.caption,
    l.label, l.method, l.is_provisional,
    ROUND((sp.likes_count - l.baseline_center)
          / NULLIF(l.baseline_spread, 0), 2)               AS likes_zscore,
    l.baseline_center                                      AS baseline_q3,
    l.baseline_spread                                      AS baseline_iqr,
    CASE WHEN l.label = 'standout' THEN 1 ELSE 0 END       AS is_standout,
    CASE WHEN l.label = 'standout'
          AND (sp.likes_count - l.baseline_center)
              / NULLIF(l.baseline_spread, 0) >= 2 THEN 1 ELSE 0 END AS is_hot,
    CASE WHEN l.label = 'standout'
          AND (sp.likes_count - l.baseline_center)
              / NULLIF(l.baseline_spread, 0) >= 2 THEN 'hot'
         WHEN l.label = 'standout' THEN 'standout'
    END                                                    AS relative_performance,
    ROUND(sp.likes_count / NULLIF(l.baseline_center, 0), 1) AS breakout_multiple,
    ROW_NUMBER() OVER (
        PARTITION BY sp.owner_username
        ORDER BY (sp.likes_count - l.baseline_center)
                 / NULLIF(l.baseline_spread, 0) DESC
    )                                                      AS owner_rank
FROM silver_ig_posts sp
JOIN ig_post_labels l ON sp.post_id = l.post_id
LEFT JOIN dim_profile dp
    ON sp.owner_id = dp.owner_id AND dp.is_current = TRUE
"""

_V_POST_METRICS_FINAL = """
CREATE VIEW v_post_metrics AS
SELECT
    base.*,
    CASE WHEN base.is_standout = 1 AND base.owner_rank <= 3
         THEN 1 ELSE 0 END AS is_top3_in_owner
FROM v_post_metrics_base base
"""

_V_CREATOR_METRICS = """
CREATE VIEW v_creator_metrics AS
SELECT creator_id,
       COUNT(*)         AS total_posts,
       SUM(is_standout) AS standout_count,
       SUM(is_hot)      AS hot_count,
       AVG(likes_count) AS avg_likes,
       MAX(likes_count) AS max_likes
FROM v_post_metrics
WHERE creator_id IS NOT NULL
GROUP BY creator_id
"""
_V_STANDOUT_CALENDAR = """
CREATE VIEW v_standout_calendar AS
SELECT EXTRACT(DAY FROM timestamp) AS day_of_month,
       SUM(is_standout)            AS standout_count
FROM v_post_metrics
WHERE is_standout = 1
GROUP BY day_of_month
"""
_V_RECENT_HOT_POSTS = """
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
"""




def _seed_label_db(db_path, monkeypatch):
    """Creator whose early-trailing baseline diverges from their real mean.

    The creator's first post is an early breakout (trailing Tukey Q3 = 40) but
    their all-time mean is pulled up by later high-performers — the exact shape
    that made "10,844 vs 40 avg" misleading (Issue B) and that PR #26's
    all-time creator_avg on post rows would misrepresent again.
    """
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
    # Early breakout (trailing Q3=40, kept OLD deliberately: it must stay out
    # of the recent hot feed) + later posts that set the real mean + a RECENT
    # 2σ+ breakout that the 28-day v_recent_hot_posts window picks up.
    con.execute(
        """
        INSERT INTO silver_ig_posts VALUES
        ('early', 'o1', 'jane', 'sce', 'breakout', 10844, 5, 0,
         TIMESTAMP '2025-11-29 10:00:00'),
        ('late1', 'o1', 'jane', 'scl1', 'later', 900, 4, 0,
         TIMESTAMP '2026-01-01 10:00:00'),
        ('late2', 'o1', 'jane', 'scl2', 'later', 1100, 4, 0,
        TIMESTAMP '2026-01-02 10:00:00'),
        ('recent', 'o1', 'jane', 'scr', 'recent breakout', 2080, 6, 0,
         CURRENT_DATE - INTERVAL '3' DAY)
        """
    )
    con.execute(
        """
        INSERT INTO ig_post_labels VALUES
        ('early', 'standout', 'day7_matched', 'standout', FALSE, 40, 10),
        ('late1', 'average', 'day7_matched', 'control', FALSE, 90, 30),
        ('late2', 'average', 'day7_matched', 'control', FALSE, 90, 30),
        ('recent', 'standout', 'day7_matched', 'standout', FALSE, 80, 40)
        """
    )
    con.execute("INSERT INTO dim_profile VALUES ('o1', TRUE, 7, 'instagram')")
    # Minimal mirror views with the same grain/semantics as the canonical ones.
    con.execute(_V_POST_METRICS)
    con.execute(_V_POST_METRICS_FINAL)
    con.execute(_V_CREATOR_METRICS)
    con.execute(_V_STANDOUT_CALENDAR)
    con.execute(_V_RECENT_HOT_POSTS)
    con.close()
    return db_path


@pytest.fixture
def label_db(tmp_path, monkeypatch):
    return _seed_label_db(tmp_path / "state.duckdb", monkeypatch)


def test_no_creator_avg_on_post_rows(label_db):
    """Post rows carry point-in-time context only — no all-time creator-avg."""
    for path in ("/api/hot-posts", "/api/standout-posts"):
        rows = TestClient(server.app).get(path).json()
        assert rows, f"expected at least one post on {path}"
        for row in rows:
            assert "creator_avg_likes" not in row, (
                f"{path} leaked an all-time creator avg onto a post row — "
                "misleading for early breakouts (PR #26 defect)"
            )
            assert "mean_likes" not in row
            assert {"baseline_q3", "baseline_iqr", "z_score"} <= row.keys()


def test_point_in_time_zscore_vs_own_baseline(label_db):
    """An old post's z-score is vs its OWN trailing baseline, not a later avg."""
    con = duckdb.connect(str(label_db), read_only=True)
    z, q3 = con.execute(
        "SELECT likes_zscore, baseline_q3 FROM v_post_metrics WHERE post_id = 'early'"
    ).fetchone()
    con.close()
    assert q3 == 40  # its own trailing Tukey Q3, not the all-time mean (4281)
    assert z == pytest.approx((10844 - 40) / 10, abs=0.01)  # (likes - Q3)/IQR


def test_hot_tier_is_z_at_least_2(label_db):
    """hot = standout AND likes_zscore >= 2 (2σ+); relative_performance matches."""
    con = duckdb.connect(str(label_db), read_only=True)
    rows = con.execute(
        "SELECT is_standout, likes_zscore, is_hot, relative_performance "
        "FROM v_post_metrics"
    ).fetchall()
    con.close()
    for is_standout, z, is_hot, rel in rows:
        assert is_hot == (1 if (is_standout and z >= 2) else 0)
        expected_rel = (
            "hot" if (is_standout and z >= 2) else ("standout" if is_standout else None)
        )
        assert rel == expected_rel


def test_cross_surface_creator_avg_is_true_mean(label_db):
    """Creators-surface avg_likes is the true all-time mean (gate-free)."""
    con = duckdb.connect(str(label_db), read_only=True)
    avg, n = con.execute(
        "SELECT avg_likes, total_posts FROM v_creator_metrics WHERE creator_id = 7"
    ).fetchone()
    con.close()
    assert n == 4
    assert avg == pytest.approx((10844 + 900 + 1100 + 2080) / 4, abs=1e-6)


def test_hot_posts_ranks_by_zscore_within_top3(label_db):
    """Recent Hot Posts: 28-day window, 2σ+ only, ranked by z-score."""
    resp = TestClient(server.app).get("/api/hot-posts")
    rows = resp.json()
    assert resp.status_code == 200
    assert rows, "expected at least one hot post"
    # The old Nov-2025 breakout is excluded by the 28-day recency window;
    # only the recent 2σ+ post surfaces here.
    assert [r["post_id"] for r in rows] == ["recent"]
    zs = [r["z_score"] for r in rows]
    assert zs == sorted(zs, reverse=True)
    assert rows[0]["baseline_q3"] == 80  # its OWN trailing Tukey Q3


def test_standout_posts_expose_point_in_time_baseline(label_db):
    rows = TestClient(server.app).get("/api/standout-posts").json()
    assert rows
    for row in rows:
        if row["post_id"] == "early":
            assert row["baseline_q3"] == 40 and row["baseline_iqr"] == 10
