"""Tests for the creator-scoped posts endpoint (creators detail page).

``dashboard/server.py`` lives outside the installed ``datalake`` package, so
it is loaded by file path here (same pattern as ``test_media_endpoints``).
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
def tmp_db(tmp_path, monkeypatch):
    """A DuckDB with a ``v_post_detail`` table and three posts."""
    db_path = tmp_path / "state.duckdb"
    monkeypatch.setattr(server, "DB_PATH", db_path)
    con = duckdb.connect(str(db_path))
    con.execute(
        """
        CREATE TABLE v_post_detail (
            post_id TEXT, owner_username TEXT, creator_id INTEGER, caption TEXT,
            likes_count BIGINT, comments_count BIGINT, video_view_count BIGINT,
            is_educational BOOLEAN, is_actionable BOOLEAN,
            admiralty TEXT, gold_domain TEXT, gold_topic TEXT, gold_subtopic TEXT,
            content_type TEXT, style TEXT, format TEXT,
            gold_analysed_at TIMESTAMP, timestamp TIMESTAMP, shortcode TEXT, channel TEXT
        )
        """
    )
    con.execute(
        """
        INSERT INTO v_post_detail VALUES
        ('p1', 'jane', 1, 'hello', 10, 2, 100, TRUE, FALSE, 'A1', 'Tech', 't', 's',
         'reel', 's', 'f', NULL, TIMESTAMP '2026-01-01 00:00:00', 'sc1', 'instagram'),
        ('p2', 'jane', 1, 'world', 20, 3, 200, NULL, NULL, NULL, NULL, NULL, NULL,
         NULL, NULL, NULL, NULL, TIMESTAMP '2026-01-02 00:00:00', 'sc2', 'instagram'),
        ('p3', 'other', 2, 'x', 5, 0, 50, NULL, NULL, NULL, NULL, NULL, NULL,
         NULL, NULL, NULL, NULL, TIMESTAMP '2026-01-03 00:00:00', 'sc3', 'instagram')
        """
    )
    con.execute(
        "CREATE TABLE silver_ig_posts (post_id TEXT, owner_username TEXT, likes_count BIGINT)"
    )
    con.execute(
        "INSERT INTO silver_ig_posts VALUES "
        "('p1', 'jane', 10), ('p2', 'jane', 20), ('p3', 'other', 5)"
    )
    con.execute(
        """
        CREATE TABLE ig_post_labels (
            post_id TEXT, label TEXT, method TEXT, enrich_decision TEXT,
            is_provisional BOOLEAN, baseline_center DOUBLE, baseline_spread DOUBLE
        )
        """
    )
    con.execute(
        """
        INSERT INTO ig_post_labels VALUES
        ('p1', 'standout', 'day7_matched', 'standout', FALSE, 10, 5)
        """
    )
    # Minimal mirror of v_post_metrics (canonical view in serving assets):
    # exposes the point-in-time metric columns the dashboard's _POST_SELECT
    # joins against (relative_performance / baseline_q3 / likes_zscore) plus
    # the tier flags and breakout_multiple the canonical view carries.
    con.execute(
        """
        CREATE TABLE v_post_metrics (
            post_id TEXT, owner_username TEXT, relative_performance TEXT,
            baseline_q3 DOUBLE, baseline_iqr DOUBLE, likes_zscore DOUBLE,
            is_standout BOOLEAN, is_hot BOOLEAN, breakout_multiple DOUBLE
        )
        """
    )
    con.execute(
        """
        INSERT INTO v_post_metrics VALUES
        ('p1', 'jane', 'standout', 10, 5, 0.0, TRUE, FALSE, 1.0),
        ('p2', 'jane', NULL, NULL, NULL, NULL, FALSE, FALSE, NULL),
        ('p3', 'other', 'hot', 1, 2, 2.0, TRUE, TRUE, 5.0)
        """
    )
    con.close()
    return db_path


@pytest.fixture
def jane_id(tmp_path, monkeypatch):
    """A creator with one Instagram + one TikTok profile; returns its id."""
    monkeypatch.setattr(server, "OPS_PATH", tmp_path / "ops.sqlite")
    ops = server._ops_resource()
    creator = server.create_creator(ops, "Jane Doe")
    server.add_profile(ops, creator_id=creator["id"], platform="instagram", handle="jane")
    server.add_profile(ops, creator_id=creator["id"], platform="tiktok", handle="jane")
    return creator["id"]


def test_creator_posts_returns_instagram_posts_desc(tmp_db, jane_id):
    resp = TestClient(server.app).get(f"/api/creators/{jane_id}/posts")
    assert resp.status_code == 200
    rows = resp.json()
    assert [r["post_id"] for r in rows] == ["p2", "p1"]
    assert rows[0]["shortcode"] == "sc2"
    assert rows[0]["analysed_at"] is None
    assert rows[0]["is_educational"] is None
    assert rows[1]["likes_count"] == 10
    assert rows[1]["relative_performance"] == "standout"


def test_creator_posts_without_instagram_profiles(tmp_db, tmp_path, monkeypatch):
    monkeypatch.setattr(server, "OPS_PATH", tmp_path / "ops.sqlite")
    ops = server._ops_resource()
    creator = server.create_creator(ops, "TikTok Only")
    server.add_profile(ops, creator_id=creator["id"], platform="tiktok", handle="tt")
    resp = TestClient(server.app).get(f"/api/creators/{creator['id']}/posts")
    assert resp.status_code == 200
    assert resp.json() == []


def test_creator_posts_unknown_creator(tmp_db, jane_id):
    resp = TestClient(server.app).get("/api/creators/999/posts")
    assert resp.status_code == 404


def test_posts_endpoint_still_shapes_rows(tmp_db, jane_id):
    resp = TestClient(server.app).get("/api/posts")
    assert resp.status_code == 200
    rows = resp.json()
    assert len(rows) == 3
    assert {r["post_id"] for r in rows} == {"p1", "p2", "p3"}
