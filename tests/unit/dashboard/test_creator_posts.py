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
        "CREATE TABLE silver_ig_posts (owner_username TEXT, likes_count BIGINT)"
    )
    con.execute(
        "INSERT INTO silver_ig_posts VALUES "
        "('jane', 10), ('jane', 20), ('jane', 30)"
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
    assert rows[0]["relative_performance"] is None


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
    assert all(r["platform"] == "instagram" for r in rows)


def test_attach_relative_performance_classifies_tiers():
    """Hot (>2σ), standout (>1σ), and normal posts are tagged correctly."""
    con = duckdb.connect(":memory:")
    con.execute(
        "CREATE TABLE silver_ig_posts (owner_username TEXT, likes_count BIGINT)"
    )
    con.execute(
        "INSERT INTO silver_ig_posts VALUES "
        "('jane', 5), ('jane', 20), ('jane', 20), ('jane', 20)"
    )
    posts = [
        {"post_id": "p1", "likes_count": 5},
        {"post_id": "p2", "likes_count": 20},
        {"post_id": "p3", "likes_count": 25},
        {"post_id": "p4", "likes_count": 50},
    ]
    server._attach_relative_performance(con, posts, ["jane"])
    con.close()
    assert [p["relative_performance"] for p in posts] == [
        None,
        None,
        "standout",
        "hot",
    ]


def test_attach_relative_performance_insufficient_baseline():
    """Fewer than 3 positive-likes posts → every post is untagged (None)."""
    con = duckdb.connect(":memory:")
    con.execute(
        "CREATE TABLE silver_ig_posts (owner_username TEXT, likes_count BIGINT)"
    )
    con.execute("INSERT INTO silver_ig_posts VALUES ('jane', 5), ('jane', 20)")
    posts = [
        {"post_id": "p1", "likes_count": 5},
        {"post_id": "p2", "likes_count": 20},
    ]
    server._attach_relative_performance(con, posts, ["jane"])
    con.close()
    assert [p["relative_performance"] for p in posts] == [None, None]
