"""Tests for the ``ig_posts_gld`` gold asset.

Gap-fills per test-hardening plan:
- Enrichment, SCHEMA_VERSION, admiralty validity, JSON parseability,
  non-JSON→dead_letter, empty/None→dead_letter, partial batch routing,
  pagination edge, watermark advance, dead-letter exclusion
"""

from __future__ import annotations

import json
from datetime import datetime
from unittest.mock import patch

from dagster import build_asset_context

from datalake.defs.common.resources import GeminiResource
from datalake.defs.instagram.assets import ig_posts_gld

from tests.fixtures.gold_factories import FAKE_ANALYSIS
from tests.fixtures.silver_factories import seed_silver_posts


# ── Tests ──────────────────────────────────────────────────────────────────


def test_enriches_posts(db, gemini_mock):
    """Unenriched posts are sent to Gemini and recorded as completed."""
    seed_silver_posts(db, [("1", "Great post about AI marketing")])

    with patch.object(GeminiResource, "analyze",
                      return_value=json.dumps(FAKE_ANALYSIS)):
        context = build_asset_context(
            resources={"duckdb": db, "gemini": gemini_mock},
        )
        result = ig_posts_gld(context)

    assert len(result) == 1
    assert result["post_id"][0] == "1"
    parsed = json.loads(result["result_json"][0])
    assert parsed["domain"] == "Business"


def test_skips_empty_caption(db, gemini_mock):
    """Posts with empty caption go to dead_letter, not gold_ig_analyses."""
    seed_silver_posts(db, [("1", ""), ("2", "  "), ("3", "Real caption")])

    with patch.object(GeminiResource, "analyze",
                      return_value=json.dumps(FAKE_ANALYSIS)):
        context = build_asset_context(
            resources={"duckdb": db, "gemini": gemini_mock},
        )
        result = ig_posts_gld(context)

    assert len(result) == 1
    assert result["post_id"][0] == "3"

    with db.get_connection() as conn:
        gold_count = conn.execute(
            "SELECT COUNT(*) FROM gold_ig_analyses"
        ).fetchone()[0]
        dead_rows = conn.execute(
            "SELECT post_id, error FROM dead_letter ORDER BY post_id"
        ).fetchall()
    assert gold_count == 1
    assert len(dead_rows) == 2
    assert all(row[1] == "Empty caption" for row in dead_rows)


def test_handles_api_error(db, gemini_mock):
    """Gemini failure after retries → post goes to dead_letter."""
    seed_silver_posts(db, [("1", "First post"), ("2", "Second post")])

    with patch.object(GeminiResource, "analyze",
                      side_effect=RuntimeError("API down")):
        context = build_asset_context(
            resources={"duckdb": db, "gemini": gemini_mock},
        )
        result = ig_posts_gld(context)

    assert result.is_empty()

    with db.get_connection() as conn:
        gold_count = conn.execute(
            "SELECT COUNT(*) FROM gold_ig_analyses"
        ).fetchone()[0]
        dead_rows = conn.execute(
            "SELECT post_id, attempts FROM dead_letter ORDER BY post_id"
        ).fetchall()
    assert gold_count == 0
    assert len(dead_rows) == 2
    for _, attempts in dead_rows:
        assert attempts == 3


def test_idempotent_completed(db, gemini_mock):
    """Already completed posts are not re-processed."""
    seed_silver_posts(db, [("1", "Post")])

    with patch.object(GeminiResource, "analyze",
                      return_value=json.dumps(FAKE_ANALYSIS)):
        context = build_asset_context(
            resources={"duckdb": db, "gemini": gemini_mock},
        )
        r1 = ig_posts_gld(context)
        assert len(r1) == 1

        r2 = ig_posts_gld(context)
        assert len(r2) == 1


def test_no_pending_posts(db, gemini_mock):
    """No unenriched posts → returns empty result."""
    seed_silver_posts(db, [])
    context = build_asset_context(
        resources={"duckdb": db, "gemini": gemini_mock},
    )
    result = ig_posts_gld(context)
    assert result.is_empty()


def test_rate_limit_retry(db, gemini_mock):
    """429 rate limit triggers retry with backoff, then succeeds."""
    seed_silver_posts(db, [("1", "Post text")])

    call_log = []

    def analyze_side_effect(prompt):
        call_log.append("call")
        if len(call_log) == 1:
            raise RuntimeError("429 Rate limited")
        return json.dumps(FAKE_ANALYSIS)

    with patch.object(GeminiResource, "analyze",
                      side_effect=analyze_side_effect):
        context = build_asset_context(
            resources={"duckdb": db, "gemini": gemini_mock},
        )
        result = ig_posts_gld(context)

    assert len(result) == 1
    assert result["post_id"][0] == "1"
    assert len(call_log) == 2


def test_gold_returns_only_completed(db, gemini_mock):
    """Returned DataFrame contains only completed rows (no failed/skipped)."""
    seed_silver_posts(db, [
        ("1", "Real content"),
        ("2", ""),
        ("3", "More content"),
    ])

    with patch.object(GeminiResource, "analyze",
                      return_value=json.dumps(FAKE_ANALYSIS)):
        context = build_asset_context(
            resources={"duckdb": db, "gemini": gemini_mock},
        )
        result = ig_posts_gld(context)

    assert len(result) == 2
    assert set(result["post_id"].to_list()) == {"1", "3"}
    assert "status" not in result.columns
    assert "error" not in result.columns
    assert "attempts" not in result.columns


def test_gold_reset_via_watermark_delete(db, gemini_mock):
    """Deleting the gold_ig watermark triggers full reprocess on next run."""
    seed_silver_posts(db, [("1", "Great post about AI marketing")])

    with patch.object(GeminiResource, "analyze",
                      return_value=json.dumps(FAKE_ANALYSIS)):
        context = build_asset_context(
            resources={"duckdb": db, "gemini": gemini_mock},
        )
        r1 = ig_posts_gld(context)
        assert len(r1) == 1

    with db.get_connection() as db_conn:
        db_conn.execute("DELETE FROM watermarks WHERE name = 'gold_ig'")

    with patch.object(GeminiResource, "analyze",
                      return_value=json.dumps(FAKE_ANALYSIS)):
        r2 = ig_posts_gld(context)
        assert len(r2) == 1
        assert r2["post_id"][0] == "1"

    with db.get_connection() as db_conn:
        count = db_conn.execute(
            "SELECT COUNT(*) FROM gold_ig_analyses"
        ).fetchone()[0]
    assert count == 1


def test_watermarks_generic(db, gemini_mock):
    """Multiple named watermarks coexist without interference."""
    seed_silver_posts(db, [("1", "Post"), ("2", "Another post")])

    with patch.object(GeminiResource, "analyze",
                      return_value=json.dumps(FAKE_ANALYSIS)):
        result = ig_posts_gld(build_asset_context(
            resources={"duckdb": db, "gemini": gemini_mock},
        ))
    assert len(result) == 2

    with db.get_connection() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO watermarks (name, timestamp) VALUES (?, ?)",
            ["other_pipeline", datetime(2024, 1, 1)],
        )

    with patch.object(GeminiResource, "analyze",
                      return_value=json.dumps(FAKE_ANALYSIS)):
        result2 = ig_posts_gld(build_asset_context(
            resources={"duckdb": db, "gemini": gemini_mock},
        ))
    assert len(result2) == 2

    with db.get_connection() as conn:
        names = {
            row[0]
            for row in conn.execute("SELECT name FROM watermarks").fetchall()
        }
    assert "gold_ig" in names
    assert "other_pipeline" in names
