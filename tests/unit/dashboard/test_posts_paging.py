"""Tests for the paged, server-side-filtered ``/api/posts`` endpoint.

The /posts table is server-paged (no full-set JSON dump) with SQL filters
equivalent to the previous client-side predicates. These tests pin the paging
contract (``{rows, total, limit, offset}``) and the filter→SQL mapping.
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
    """DuckDB with the serving views /api/posts selects from, plus 6 posts."""
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
        ('p1', 'jane', 1, 'build a design system', 10, 2, 100, TRUE, FALSE, 'A1',
         'Tech', 't1', 's1', 'reel', 's', 'f', NULL, TIMESTAMP '2026-01-01', 'sc1', 'instagram'),
        ('p2', 'jane', 1, 'ux tips for onboarding', 20, 3, 200, FALSE, TRUE, 'A2',
         'Creative', 't2', 's2', 'reel', 's', 'f', NULL, TIMESTAMP '2026-01-02', 'sc2', 'instagram'),
        ('p3', 'other', 2, 'business growth', 5, 0, 50, NULL, NULL, 'B1',
         'Business', 't3', 's3', 'carousel', 's', 'f', NULL, TIMESTAMP '2026-01-03', 'sc3', 'instagram'),
        ('p4', 'jane', 1, 'app prototype demo', 40, 5, 400, TRUE, TRUE, 'A1',
         'Tech', 't4', 's4', 'reel', 's', 'f', NULL, TIMESTAMP '2026-01-04', 'sc4', 'instagram'),
        ('p5', 'tiktoker', 3, 'design tools roundup', 3, 1, 30, NULL, NULL, 'C1',
         'Creative', 't5', 's5', 'image', 's', 'f', NULL, TIMESTAMP '2026-01-05', 'sc5', 'tiktok'),
        ('p6', 'jane', 1, 'revenue model notes', 60, 8, 0, TRUE, FALSE, 'A1',
         'Business', 't6', 's6', 'carousel', 's', 'f', NULL, TIMESTAMP '2026-01-06', 'sc6', 'instagram')
        """
    )
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
        ('p3', 'other', NULL, NULL, NULL, NULL, FALSE, FALSE, NULL),
        ('p4', 'jane', 'hot', 10, 5, 2.0, TRUE, TRUE, 5.0),
        ('p5', 'tiktoker', NULL, NULL, NULL, NULL, FALSE, FALSE, NULL),
        ('p6', 'jane', 'standout', 10, 5, 1.0, TRUE, FALSE, 1.0)
        """
    )
    con.close()
    return db_path


def _get(**params):
    return TestClient(server.app).get("/api/posts", params=params)


def _ids(body):
    return [r["post_id"] for r in body["rows"]]


def test_posts_returns_all_when_under_default_limit(tmp_db):
    body = _get().json()
    assert set(body) == {"rows", "total", "limit", "offset"}
    assert body["total"] == 6
    assert body["limit"] == 50
    assert body["offset"] == 0
    assert sorted(_ids(body)) == ["p1", "p2", "p3", "p4", "p5", "p6"]


def test_posts_paginates_with_offset_and_total(tmp_db):
    body = _get(limit=2).json()
    assert body["total"] == 6
    assert len(body["rows"]) == 2
    first_page = _ids(body)
    # default order is timestamp DESC, so page 1 = newest two.
    assert first_page == ["p6", "p5"]
    page2 = _get(limit=2, offset=2).json()
    assert _ids(page2) == ["p4", "p3"]
    assert page2["total"] == 6


def test_posts_filter_platforms(tmp_db):
    body = _get(platforms="instagram").json()
    ids = set(_ids(body))
    assert "p5" not in ids  # tiktok excluded
    assert len(body["rows"]) == 5
    body2 = _get(platforms="instagram,tiktok").json()
    assert len(body2["rows"]) == 6


def test_posts_filter_domains_and_ranks(tmp_db):
    body = _get(domains="Tech").json()
    assert set(_ids(body)) == {"p1", "p4"}
    body2 = _get(ranks="A").json()
    # admiralty first char in (A)
    assert set(_ids(body2)) == {"p1", "p2", "p4", "p6"}


def test_posts_filter_educational_and_min_likes(tmp_db):
    body = _get(educational="true").json()
    assert set(_ids(body)) == {"p1", "p4", "p6"}
    body2 = _get(min_likes=20).json()
    assert set(_ids(body2)) == {"p2", "p4", "p6"}
    body3 = _get(min_likes=20, max_likes=40).json()
    assert set(_ids(body3)) == {"p2", "p4"}


def test_posts_quick_search_q(tmp_db):
    body = _get(q="design").json()
    # matches captions containing "design": p1, p5; also topic/domain matches.
    assert "p1" in _ids(body) and "p5" in _ids(body)
    assert body["total"] == len(body["rows"])


def test_posts_date_range(tmp_db):
    body = _get(date_from="2026-01-03", date_to="2026-01-04").json()
    # inclusive end date
    assert set(_ids(body)) == {"p4", "p3"}


def test_posts_username_scope(tmp_db):
    body = _get(username="jane").json()
    assert set(_ids(body)) == {"p1", "p2", "p4", "p6"}
