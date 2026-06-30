"""Tests for the ``ig_posts_gld`` gold asset.

Mocks the Gemini client to avoid real API calls. Verifies enrichment logic,
error handling, rate-limit retries, and idempotency.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from dagster import build_asset_context
from dagster_duckdb import DuckDBResource

from datalake.defs.common.resources import GeminiResource

# ── Fake analysis payload ─────────────────────────────────────────────────

_FAKE_ANALYSIS = {
    "is_educational": True,
    "is_actionable": True,
    "admirality": "B1",
    "domain": "Business",
    "subdomain": "Marketing",
    "topic": "AI Content",
    "subtopic": "Prompt Engineering",
    "content_type": "tutorial",
    "style": "casual",
    "format": "talking head",
    "educational_json": {
        "summary": "How to write better AI prompts.",
        "workflow": [
            {"step": "Define goal", "tool": "None", "detail": "Know what you want"},
        ],
        "concepts": [{"term": "Prompt Engineering", "explanation": "Crafting inputs"}],
        "principles": ["Be specific"],
        "techniques": ["Iterative refinement"],
    },
    "actionable_json": {
        "summary": "You can improve your prompts.",
        "resources": [],
        "tools": ["ChatGPT"],
        "guides": ["Start simple, add context"],
        "downloads": [],
    },
}


# ── Helpers ───────────────────────────────────────────────────────────────


def _seed_silver_posts(duckdb, rows: list[tuple[str, str]]) -> None:
    """Insert test rows into silver_posts (creates table if needed)."""
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS silver_posts (
                post_id TEXT PRIMARY KEY, shortcode TEXT, url TEXT,
                caption TEXT, owner_id TEXT, owner_username TEXT,
                likes_count INTEGER, comments_count INTEGER,
                video_play_count INTEGER, video_view_count INTEGER,
                timestamp TIMESTAMP, hashtags TEXT, meta_data TEXT,
                has_engagement_bait BOOLEAN, media_files TEXT,
                media_count INTEGER, source_dataset TEXT,
                silvered_at TIMESTAMP
            )
        """)
        for post_id, caption in rows:
            conn.execute(
                """INSERT OR REPLACE INTO silver_posts
                   (post_id, caption, likes_count, comments_count,
                    video_play_count, video_view_count, timestamp,
                    hashtags, has_engagement_bait, media_files,
                    media_count, source_dataset)
                   VALUES (?, ?, 0, 0, 0, 0, NOW(),
                    '[]', FALSE, '[]', 0, 'test')""",
                [post_id, caption],
            )


def _mock_gemini_client(text: str = "fake response") -> MagicMock:
    """Create a mocked Gemini client that returns the given text as JSON."""
    response = MagicMock()
    response.text = text
    model_mock = MagicMock()
    model_mock.generate_content.return_value = response
    client = MagicMock()
    client.models = model_mock
    return client


# ── Fixtures ──────────────────────────────────────────────────────────────


@pytest.fixture
def db(request) -> DuckDBResource:
    """Create a DuckDB resource backed by a tmp_path file."""
    import os
    import tempfile

    db_path = os.path.join(tempfile.mkdtemp(), "test.duckdb")

    def cleanup():
        try:
            os.unlink(db_path)
        except FileNotFoundError:
            pass

    request.addfinalizer(cleanup)
    return DuckDBResource(database=db_path)


# ── Tests ─────────────────────────────────────────────────────────────────


def test_enriches_posts(db):
    """Unenriched posts are sent to Gemini and recorded as completed."""
    _seed_silver_posts(db, [("1", "Great post about AI marketing")])
    gemini = GeminiResource()

    with patch(
        "google.genai.Client",
        return_value=_mock_gemini_client(json.dumps(_FAKE_ANALYSIS)),
    ):
        context = build_asset_context(resources={"duckdb": db, "gemini": gemini})
        from datalake.defs.instagram.assets import ig_posts_gld

        result = ig_posts_gld(context)

    assert len(result) == 1
    assert result["status"][0] == "completed"
    parsed = json.loads(result["result_json"][0])
    assert parsed["domain"] == "Business"


def test_skips_empty_caption(db):
    """Posts with empty caption are skipped, not sent to Gemini."""
    _seed_silver_posts(db, [("1", ""), ("2", "  "), ("3", "Real caption")])
    gemini = GeminiResource()

    with patch(
        "google.genai.Client",
        return_value=_mock_gemini_client(json.dumps(_FAKE_ANALYSIS)),
    ):
        context = build_asset_context(resources={"duckdb": db, "gemini": gemini})
        from datalake.defs.instagram.assets import ig_posts_gld

        result = ig_posts_gld(context)

    assert len(result) == 1
    assert result["post_id"][0] == "3"

    with db.get_connection() as conn:
        records = conn.execute(
            "SELECT post_id, status FROM gold_analyses ORDER BY post_id"
        ).fetchall()
    assert records == [
        ("1", "skipped"),
        ("2", "skipped"),
        ("3", "completed"),
    ]


def test_handles_api_error(db):
    """Gemini failure after retries → recorded as failed."""
    _seed_silver_posts(db, [("1", "First post"), ("2", "Second post")])
    gemini = GeminiResource()

    mock_models = MagicMock()
    mock_models.generate_content.side_effect = RuntimeError("API down")
    failing_client = MagicMock()
    failing_client.models = mock_models

    with patch("google.genai.Client", return_value=failing_client):
        context = build_asset_context(resources={"duckdb": db, "gemini": gemini})
        from datalake.defs.instagram.assets import ig_posts_gld

        result = ig_posts_gld(context)

    assert len(result) == 0

    with db.get_connection() as conn:
        records = conn.execute(
            "SELECT post_id, status, attempts FROM gold_analyses ORDER BY post_id"
        ).fetchall()
    assert len(records) == 2
    for _, status, attempts in records:
        assert status == "failed"
        assert attempts == 3


def test_idempotent_completed(db):
    """Already completed posts are not re-processed."""
    _seed_silver_posts(db, [("1", "Post")])
    gemini = GeminiResource()

    with patch(
        "google.genai.Client",
        return_value=_mock_gemini_client(json.dumps(_FAKE_ANALYSIS)),
    ):
        context = build_asset_context(resources={"duckdb": db, "gemini": gemini})
        from datalake.defs.instagram.assets import ig_posts_gld

        r1 = ig_posts_gld(context)
        assert len(r1) == 1

        r2 = ig_posts_gld(context)
        assert len(r2) == 1

    with db.get_connection() as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM gold_analyses WHERE attempts > 0"
        ).fetchone()[0]
    assert count == 1
def test_no_pending_posts(db):
    """No unenriched posts → returns empty result."""
    # Need silver_posts to exist for FK constraint on gold_analyses
    _seed_silver_posts(db, [])
    gemini = GeminiResource()
    context = build_asset_context(resources={"duckdb": db, "gemini": gemini})
    from datalake.defs.instagram.assets import ig_posts_gld

    result = ig_posts_gld(context)
    assert len(result) == 0


def test_rate_limit_retry(db):
    """429 rate limit triggers retry with backoff, then succeeds."""
    _seed_silver_posts(db, [("1", "Post text")])
    gemini = GeminiResource()

    call_log = []
    client = MagicMock()

    def side_effect(*args, **kwargs):
        call_log.append("call")
        if len(call_log) == 1:
            raise RuntimeError("429 Rate limited")
        response = MagicMock()
        response.text = json.dumps(_FAKE_ANALYSIS)
        return response

    client.models.generate_content = side_effect

    with patch("google.genai.Client", return_value=client):
        context = build_asset_context(resources={"duckdb": db, "gemini": gemini})
        from datalake.defs.instagram.assets import ig_posts_gld

        result = ig_posts_gld(context)

    assert len(result) == 1
    assert result["status"][0] == "completed"
    assert len(call_log) == 2  # first failed, second succeeded
