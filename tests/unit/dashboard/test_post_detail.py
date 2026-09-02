"""Tests for the read-only post detail endpoint (GET /api/posts/{post_id}).

The endpoint is a thin projector over the canonical serving views
(v_post_detail + v_post_metrics): full metadata, gold enrichment summary,
point-in-time engagement context (z vs the post's own trailing Tukey
baseline — never a creator all-time average), and a clean 404 for
unknown posts. Transcript is surfaced as not-yet-available (null).
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
    """A DuckDB with minimal mirrors of v_post_detail + v_post_metrics."""
    db_path = tmp_path / "state.duckdb"
    monkeypatch.setattr(server, "DB_PATH", db_path)
    con = duckdb.connect(str(db_path))
    con.execute(
        """
        CREATE TABLE v_post_detail (
            post_id TEXT, shortcode TEXT, url TEXT, owner_username TEXT,
            creator_id INTEGER, creator_name TEXT, caption TEXT,
            timestamp TIMESTAMP, likes_count BIGINT, comments_count BIGINT,
            video_view_count BIGINT, media_count BIGINT, hashtags TEXT,
            admiralty TEXT, gold_domain TEXT, gold_subdomain TEXT,
            gold_topic TEXT, gold_subtopic TEXT, content_type TEXT,
            style TEXT, format TEXT, is_educational BOOLEAN,
            is_actionable BOOLEAN, gold_analysed_at TIMESTAMP, channel TEXT
        )
        """
    )
    con.execute(
        """
        INSERT INTO v_post_detail VALUES
        ('p1', 'sc1', 'https://www.instagram.com/p/sc1/', 'jane', 1, 'Jane Doe',
         'hello', TIMESTAMP '2026-01-01 00:00:00', 100, 5, 900, 2, '#drone',
         'A1', 'Tech', 'Media', 'Drone videography', 'Sports', 'reel', 'casual',
         'broll', TRUE, FALSE, TIMESTAMP '2026-01-02 00:00:00', 'instagram'),
        ('p2', 'sc2', 'https://www.instagram.com/p/sc2/', 'jane', 1, 'Jane Doe',
         'world', TIMESTAMP '2026-01-02 00:00:00', 10, 1, NULL, 1, '',
         NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
         'instagram')
        """
    )
    con.execute(
        """
        CREATE TABLE v_post_metrics (
            post_id TEXT, label TEXT, is_provisional BOOLEAN,
            likes_zscore DOUBLE, baseline_q3 DOUBLE, baseline_iqr DOUBLE,
            breakout_multiple DOUBLE, sigma_tier TEXT, is_standout BOOLEAN,
            is_hot BOOLEAN, relative_performance TEXT, owner_rank BIGINT,
            is_top3_in_owner BOOLEAN
        )
        """
    )
    con.execute(
        """
        INSERT INTO v_post_metrics VALUES
        ('p1', 'standout', FALSE, 3.25, 20, 10, 5.0, '2sigma', TRUE, TRUE,
         'standout', 1, TRUE),
        ('p2', NULL, NULL, NULL, NULL, NULL, NULL, NULL, FALSE, FALSE,
         NULL, NULL, FALSE)
        """
    )
    con.close()
    return db_path


def test_post_detail_returns_full_payload(tmp_db):
    resp = TestClient(server.app).get("/api/posts/p1")
    assert resp.status_code == 200
    p = resp.json()
    # Metadata + source link
    assert p["post_id"] == "p1"
    assert p["shortcode"] == "sc1"
    assert p["url"] == "https://www.instagram.com/p/sc1/"
    assert p["owner_username"] == "jane"
    assert p["creator_id"] == 1
    assert p["creator_name"] == "Jane Doe"
    assert p["platform"] == "instagram"
    # Engagement counts
    assert p["likes_count"] == 100
    assert p["comments_count"] == 5
    assert p["video_view_count"] == 900
    # Gold enrichment summary
    assert p["enrichment"]["admiralty"] == "A1"
    assert p["enrichment"]["gold_domain"] == "Tech"
    assert p["enrichment"]["gold_topic"] == "Drone videography"
    assert p["enrichment"]["is_educational"] is True
    assert p["enrichment"]["is_actionable"] is False
    # Point-in-time context: z vs the post's OWN trailing baseline
    pit = p["point_in_time"]
    assert pit["likes_zscore"] == pytest.approx(3.25)
    assert pit["baseline_q3"] == 20
    assert pit["baseline_iqr"] == 10
    assert pit["breakout_multiple"] == 5.0
    assert pit["is_hot"] is True
    assert pit["is_standout"] is True
    assert pit["is_top3_in_owner"] is True
    assert "avg" not in pit  # no creator-avg key mislabeled on posts


def test_post_detail_without_metrics_row(tmp_db):
    """A post with no label-pass metrics still renders (nulls, zero counts)."""
    resp = TestClient(server.app).get("/api/posts/p2")
    assert resp.status_code == 200
    p = resp.json()
    assert p["likes_count"] == 10
    assert p["video_view_count"] == 0
    assert p["enrichment"]["gold_domain"] is None
    pit = p["point_in_time"]
    assert pit["likes_zscore"] is None
    assert pit["baseline_q3"] is None
    assert pit["label"] is None


def test_post_detail_transcript_not_yet_available(tmp_db):
    resp = TestClient(server.app).get("/api/posts/p1")
    assert resp.status_code == 200
    assert resp.json()["transcript"] is None


def test_post_detail_unknown_post_is_404(tmp_db):
    resp = TestClient(server.app).get("/api/posts/does-not-exist")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "post not found"
