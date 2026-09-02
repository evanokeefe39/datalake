"""Semantic-contract tests for the Hot Posts / standout surfaces.

Regression guard for the Issue-B defect: the API used to expose the per-post
TRAILING Tukey Q3 under the key ``mean_likes``, which the overview UI rendered
as "… vs 40 avg" — misrepresenting the creator's real average ~20x for
early-breakout posts.

Contracts tested here:
1. The value a surface renders as an "average" (``creator_avg_likes``) is a
   true all-time mean of the creator's posts — same definition as the
   creators surface (``v_creator_quality.avg_likes``).
2. The per-post trailing baseline is exposed under honest names
   (``baseline_q3``/``baseline_iqr``), never under an "avg"-implying key.
3. Cross-surface consistency: hot-posts ``creator_avg_likes`` equals the
   creators-surface average for the same creator.
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


@pytest.fixture
def label_db(tmp_path, monkeypatch):
    """Creator whose early-trailing baseline diverges from their real mean.

    The creator's first posts score low (trailing Tukey Q3 = 40) but their
    all-time mean is pulled up by later high-performers — the exact shape that
    made "10,844 vs 40 avg" misleading.
    """
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
    # Early breakout (trailing Q3=40) + later posts that set the real mean.
    con.execute(
        """
        INSERT INTO silver_ig_posts VALUES
        ('early', 'o1', 'jane', 'sce', 'breakout', 10844, 5, 0,
         TIMESTAMP '2025-11-29 10:00:00'),
        ('late1', 'o1', 'jane', 'scl1', 'later', 900, 4, 0,
         TIMESTAMP '2026-01-01 10:00:00'),
        ('late2', 'o1', 'jane', 'scl2', 'later', 1100, 4, 0,
         TIMESTAMP '2026-01-02 10:00:00')
        """
    )
    con.execute(
        """
        INSERT INTO ig_post_labels VALUES
        ('early', 'standout', 'day7_matched', 'standout', FALSE, 40, 10),
        ('late1', 'average', 'day7_matched', 'control', FALSE, 90, 30),
        ('late2', 'average', 'day7_matched', 'control', FALSE, 90, 30)
        """
    )
    con.execute("INSERT INTO dim_profile VALUES ('o1', TRUE, 7, 'instagram')")
    con.close()
    return db_path


def test_avg_key_carries_true_creator_mean(label_db):
    rows = TestClient(server.app).get("/api/hot-posts").json()
    assert rows, "expected at least one hot post"
    assert "mean_likes" not in rows[0]
    # (10844 + 900 + 1100) / 3 = 4281.33 — the real all-time mean.
    assert rows[0]["creator_avg_likes"] == pytest.approx(4281, abs=1)
    assert rows[0]["baseline_q3"] == 40  # trailing Tukey Q3, labeled as such


def test_cross_surface_avg_consistency(label_db):
    """hot-posts 'avg' must equal the creators-surface average definition."""
    hot = TestClient(server.app).get("/api/hot-posts").json()[0]
    con = duckdb.connect(str(label_db), read_only=True)
    try:
        true_avg = con.execute(
            "SELECT AVG(likes_count) FROM silver_ig_posts "
            "WHERE owner_username = 'jane'"
        ).fetchone()[0]
    finally:
        con.close()
    assert hot["creator_avg_likes"] == pytest.approx(round(true_avg, 0), abs=1)


def test_baseline_exposed_under_honest_names(label_db):
    for path in ("/api/hot-posts", "/api/standout-posts"):
        rows = TestClient(server.app).get(path).json()
        for row in rows:
            assert "mean_likes" not in row
            assert "std_likes" not in row
            assert {"baseline_q3", "baseline_iqr", "z_score"} <= row.keys()
