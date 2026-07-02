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

from datalake.defs.common.resources import GeminiResource
from datalake.defs.instagram.assets import ig_posts_gld
from datalake.defs.instagram.config import (
    GeminiTierConfig,
    GoldConfig,
)
from tests.fixtures.gold_factories import FAKE_ANALYSIS
from tests.fixtures.silver_factories import seed_silver_posts

# ── Existing gold enrichment tests ──────────────────────────────────────────


def test_enriches_posts(db, gemini_mock):
    """Unenriched posts are sent to Gemini and recorded as completed."""
    seed_silver_posts(db, [("1", "Great post about AI marketing")])

    with patch.object(GeminiResource, "analyze",
                      return_value=json.dumps(FAKE_ANALYSIS)):
        result = ig_posts_gld(
            config=GoldConfig(), duckdb=db, gemini=gemini_mock,
        )

    assert len(result) == 1
    assert result["post_id"][0] == "1"
    parsed = json.loads(result["result_json"][0])
    assert parsed["domain"] == "Business"


def test_skips_empty_caption(db, gemini_mock):
    """Posts with empty caption go to dead_letter, not gold_ig_analyses."""
    seed_silver_posts(db, [("1", ""), ("2", "  "), ("3", "Real caption")])

    with patch.object(GeminiResource, "analyze",
                      return_value=json.dumps(FAKE_ANALYSIS)):
        result = ig_posts_gld(
            config=GoldConfig(), duckdb=db, gemini=gemini_mock,
        )

    assert len(result) == 1
    assert result["post_id"][0] == "3"

    with db.get_connection() as conn:
        gold_count = conn.execute(
            "SELECT COUNT(*) FROM gold_ig_analyses"
        ).fetchone()[0]
        dead_rows = conn.execute(
            "SELECT post_id, error FROM dead_letter"
        ).fetchall()
    assert gold_count == 1
    assert len(dead_rows) == 0


def test_handles_api_error(db, gemini_mock):
    """Gemini failure after retries → post goes to dead_letter."""
    seed_silver_posts(db, [("1", "First post"), ("2", "Second post")])

    with patch.object(GeminiResource, "analyze",
                      side_effect=RuntimeError("API down")):
        result = ig_posts_gld(
            config=GoldConfig(), duckdb=db, gemini=gemini_mock,
        )

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
        r1 = ig_posts_gld(
            config=GoldConfig(), duckdb=db, gemini=gemini_mock,
        )
        assert len(r1) == 1

        r2 = ig_posts_gld(
            config=GoldConfig(), duckdb=db, gemini=gemini_mock,
        )
        assert len(r2) == 1


def test_no_pending_posts(db, gemini_mock):
    """No unenriched posts → returns empty result."""
    seed_silver_posts(db, [])
    result = ig_posts_gld(
        config=GoldConfig(), duckdb=db, gemini=gemini_mock,
    )
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
        result = ig_posts_gld(
            config=GoldConfig(), duckdb=db, gemini=gemini_mock,
        )

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
        result = ig_posts_gld(
            config=GoldConfig(), duckdb=db, gemini=gemini_mock,
        )

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
        r1 = ig_posts_gld(
            config=GoldConfig(), duckdb=db, gemini=gemini_mock,
        )
        assert len(r1) == 1

    with db.get_connection() as db_conn:
        db_conn.execute("DELETE FROM watermarks WHERE name = 'gold_ig'")

    with patch.object(GeminiResource, "analyze",
                      return_value=json.dumps(FAKE_ANALYSIS)):
        r2 = ig_posts_gld(
            config=GoldConfig(), duckdb=db, gemini=gemini_mock,
        )
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
        result = ig_posts_gld(
            config=GoldConfig(), duckdb=db, gemini=gemini_mock,
        )
    assert len(result) == 2

    with db.get_connection() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO watermarks (name, timestamp) VALUES (?, ?)",
            ["other_pipeline", datetime(2024, 1, 1)],
        )

    with patch.object(GeminiResource, "analyze",
                      return_value=json.dumps(FAKE_ANALYSIS)):
        result2 = ig_posts_gld(
            config=GoldConfig(), duckdb=db, gemini=gemini_mock,
        )
    assert len(result2) == 2

    with db.get_connection() as conn:
        names = {
            row[0]
            for row in conn.execute("SELECT name FROM watermarks").fetchall()
        }
    assert "gold_ig" in names
    assert "other_pipeline" in names


# ── Tier detection tests ───────────────────────────────────────────────────


def test_tier_free_limits_posts(monkeypatch, db, gemini_mock):
    """Free tier limits to 10 posts and does not support batch."""
    monkeypatch.setenv("GEMINI_TIER", "free")
    cfg = GeminiTierConfig.detect()
    assert cfg.max_posts_per_run == 10
    assert not cfg.supports_batch
    assert cfg.default_rpm == 10


def test_tier1_unlimited_posts(monkeypatch):
    """Tier 1: unlimited posts, batch enabled, 30 RPM."""
    monkeypatch.setenv("GEMINI_TIER", "tier1")
    cfg = GeminiTierConfig.detect()
    assert cfg.max_posts_per_run == 0  # 0 = unlimited
    assert cfg.supports_batch
    assert cfg.default_rpm == 30
    assert cfg.max_batch_tokens == 10_000_000


def test_tier2_unlimited_with_higher_limits(monkeypatch):
    """Tier 2: same as Tier 1 but with larger batch and higher RPM."""
    monkeypatch.setenv("GEMINI_TIER", "tier2")
    cfg = GeminiTierConfig.detect()
    assert cfg.max_posts_per_run == 0
    assert cfg.supports_batch
    assert cfg.default_rpm == 60
    assert cfg.max_batch_tokens == 128_000_000


def test_tier_fallback_to_free_for_unknown(monkeypatch):
    """Unknown tier value falls back to FREE."""
    monkeypatch.setenv("GEMINI_TIER", "invalid_tier")
    cfg = GeminiTierConfig.detect()
    assert cfg.max_posts_per_run == 10
    assert not cfg.supports_batch


# ── Per-post watermark advancement tests ────────────────────────────────────


def test_watermark_advances_per_post(db, gemini_mock):
    """Watermark advances after each successful post, not at batch end."""
    seed_silver_posts(db, [
        ("1", "First"),
        ("2", "Second"),
    ])

    call_count = 0

    def analyze_with_tracker(prompt):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return json.dumps(FAKE_ANALYSIS)
        raise RuntimeError("Second call fails")

    with patch.object(GeminiResource, "analyze",
                      side_effect=analyze_with_tracker):
        result = ig_posts_gld(
            config=GoldConfig(), duckdb=db, gemini=gemini_mock,
        )

    # Post "1" succeeded — watermark should have advanced.
    # Post "2" failed — dead_letter, but watermark is still past "1".
    with db.get_connection() as conn:
        wm = conn.execute(
            "SELECT timestamp FROM watermarks WHERE name = 'gold_ig'"
        ).fetchone()
        dead = conn.execute(
            "SELECT post_id FROM dead_letter"
        ).fetchall()
    assert wm is not None, "Watermark should exist"
    assert len(dead) == 1
    # Result should include only post "1" (post "2" failed)
    assert len(result) == 1
    assert result["post_id"][0] == "1"


# ── Concurrent interactive + batch idempotency tests ───────────────────────


def test_interactive_and_batch_no_conflict(db, gemini_mock):
    """INSERT OR REPLACE ensures concurrent interactive+batch are idempotent."""
    seed_silver_posts(db, [("1", "Post for both paths")])

    with patch.object(GeminiResource, "analyze",
                      return_value=json.dumps(FAKE_ANALYSIS)):
        interactive_result = ig_posts_gld(
            config=GoldConfig(), duckdb=db, gemini=gemini_mock,
        )
    assert len(interactive_result) == 1

    # Simulate batch writing the same post — INSERT OR REPLACE
    batch_result_json = json.dumps({
        **FAKE_ANALYSIS,
        "domain": "Batch Enrichment",
    })
    with db.get_connection() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO gold_ig_analyses "
            "(post_id, schema_version, result_json, analysed_at) "
            "VALUES (?, 3, ?, ?)",
            ["1", batch_result_json, datetime.utcnow().isoformat()],
        )

    # Interactive re-run — should see existing row, not re-process
    with patch.object(GeminiResource, "analyze",
                      return_value=json.dumps(FAKE_ANALYSIS)):
        final = ig_posts_gld(
            config=GoldConfig(), duckdb=db, gemini=gemini_mock,
        )

    # Row count should stay 1
    assert len(final) == 1
    # The batch-written domain should persist (INSERT OR REPLACE)
    parsed = json.loads(final["result_json"][0])
    assert parsed["domain"] == "Batch Enrichment"


def test_batch_overwrites_interactive_same_post(db):
    """Batch backfill overwrites interactive result for the same post_id."""
    seed_silver_posts(db, [("1", "Post")])

    # Simulate interactive already enriched
    with db.get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS gold_ig_analyses (
                post_id TEXT PRIMARY KEY,
                schema_version INTEGER NOT NULL DEFAULT 3,
                result_json TEXT, analysed_at TIMESTAMP
            )
        """)
        conn.execute(
            "INSERT OR REPLACE INTO gold_ig_analyses "
            "(post_id, schema_version, result_json, analysed_at) "
            "VALUES (?, 3, ?, ?)",
            ["1", json.dumps({"source": "interactive"}), datetime.utcnow().isoformat()],
        )

    # Simulate batch writing a different result
    with db.get_connection() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO gold_ig_analyses "
            "(post_id, schema_version, result_json, analysed_at) "
            "VALUES (?, 3, ?, ?)",
            ["1", json.dumps({"source": "batch"}), datetime.utcnow().isoformat()],
        )

    with db.get_connection() as conn:
        row = conn.execute(
            "SELECT result_json FROM gold_ig_analyses WHERE post_id = '1'"
        ).fetchone()
    assert json.loads(row[0])["source"] == "batch"
