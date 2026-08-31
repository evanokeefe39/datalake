"""Tests for the label-backed standout/hot/weekly-summary endpoints (US-D1).

The endpoints read ``ig_post_labels`` (``label='standout'``) instead of
computing lifetime z-scores; day7_matched rows are preferred over
provisional day0_heuristic rows in ranking.
"""

from __future__ import annotations

import importlib.util
import sys
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
         TIMESTAMP '2026-03-07 10:00:00'),
        ('s0', 'o1', 'jane', 'sc0', 'day0 hit', 800, 4, 0,
         TIMESTAMP '2026-03-08 10:00:00'),
        ('avg', 'o1', 'jane', 'sca', 'normal', 100, 1, 0,
         TIMESTAMP '2026-03-09 10:00:00')
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
    # Baseline stats come from the label pass, not a lifetime aggregate.
    assert rows[0]["mean_likes"] == 100
    assert rows[0]["std_likes"] == 50


def test_standout_posts_excludes_non_standout_labels(label_db):
    resp = TestClient(server.app).get("/api/standout-posts?limit=100")
    assert resp.status_code == 200
    assert "avg" not in {r["post_id"] for r in resp.json()}


def test_hot_posts_ranks_per_creator_from_labels(label_db):
    resp = TestClient(server.app).get("/api/hot-posts")
    assert resp.status_code == 200
    rows = resp.json()
    assert [r["post_id"] for r in rows] == ["s7", "s0"]
    assert rows[0]["mean_likes"] == 100


def test_weekly_summary_counts_standout_labels_by_day(label_db):
    resp = TestClient(server.app).get("/api/weekly-summary")
    assert resp.status_code == 200
    days = {r["day"]: r["standout_count"] for r in resp.json()}
    assert days == {7: 1, 8: 1}
