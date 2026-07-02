"""Integration test: video/audio processing via Gemini multimodal API.

This test FAILS until the enrichment worker passes uploaded media file URIs
to Gemini for multimodal analysis. Currently, ``lookup_or_upload`` uploads
media to Gemini File API but the returned URI is discarded — the prompt
sent to Gemini is text-only (caption).

Success criterion on PR #7: this test must pass.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from dagster_duckdb import DuckDBResource

from datalake.defs.common.resources import SQLiteResource


def _seed_silver_with_video(db: DuckDBResource, post_id: str, caption: str) -> None:
    """Create a silver_ig_posts row with a video URL in media_files."""
    import json
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    with db.get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS silver_ig_posts (
                post_id TEXT PRIMARY KEY, caption TEXT, processed_on TIMESTAMP,
                owner_id TEXT DEFAULT 'test', owner_username TEXT DEFAULT 'test',
                likes_count INTEGER DEFAULT 0, comments_count INTEGER DEFAULT 0,
                video_play_count INTEGER DEFAULT 0, video_view_count INTEGER DEFAULT 0,
                timestamp TIMESTAMP DEFAULT NOW(), hashtags TEXT DEFAULT '[]',
                has_engagement_bait BOOLEAN DEFAULT FALSE, media_files TEXT DEFAULT '[]',
                media_count INTEGER DEFAULT 0, source_dataset TEXT DEFAULT 'test',
                shortcode TEXT DEFAULT '', url TEXT DEFAULT '', meta_data TEXT DEFAULT '{}'
            )
        """)
        conn.execute(
            """INSERT OR REPLACE INTO silver_ig_posts
               (post_id, caption, media_files, media_count, processed_on)
               VALUES (?, ?, ?, ?, ?)""",
            [
                post_id, caption,
                json.dumps(["https://example.com/video.mp4"]),
                1, now,
            ],
        )


def test_worker_passes_media_uri_to_gemini(tmp_path):
    """GIVEN a silver post with a video URL
    WHEN the enrichment worker processes it
    THEN the Gemini prompt includes the uploaded file URI for multimodal analysis.

    FAILING: The worker calls ``lookup_or_upload`` to get a Gemini File API URI
    but never passes it to ``gemini.analyze()``. The prompt is text-only.
    """
    from scripts.enrichment_worker import process_item

    ops = SQLiteResource(database=str(tmp_path / "ops.sqlite"))
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))

    # Setup gold_analyses table (normally done by ensure_gold_analyses)
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS gold_analyses (
                post_id TEXT NOT NULL, domain TEXT NOT NULL DEFAULT 'instagram',
                prompt_hash TEXT, result_json TEXT, analysed_at TEXT NOT NULL,
                PRIMARY KEY (post_id, domain)
            )
        """)

    # Seed: a post WITH a video URL
    _seed_silver_with_video(duckdb, "vid1", "Check out this video tutorial")

    # Create a batch with this item
    from datalake.defs.enrichment.batch import (
        _ensure_schema,
        claim_batch,
        claim_pending_items,
        create_batch,
    )

    _ensure_schema(ops)
    job_id = create_batch(ops, ["vid1"], ["instagram"])
    claim_batch(ops)
    items = claim_pending_items(ops, job_id, limit=1)
    item = items[0]

    # Mock Gemini
    mock_gemini = MagicMock()
    mock_gemini.analyze.return_value = '{"is_educational": true}'

    # Capture what prompt was sent to analyze()
    captured_prompts: list[str] = []
    mock_gemini.analyze.side_effect = lambda prompt, **kwargs: (
        captured_prompts.append(prompt),
        '{"is_educational": true}',
    )[1]

    # Mock lookup_or_upload to return a fake File API URI
    fake_file_uri = "https://generativelanguage.googleapis.com/v1beta/files/fake-abc123"
    with patch(
        "scripts.enrichment_worker.lookup_or_upload",
        return_value=fake_file_uri,
    ):
        process_item(ops, duckdb, mock_gemini, item)

    # ── Assertions ──────────────────────────────────────────────────────
    assert len(captured_prompts) == 1, (
        f"Expected 1 Gemini call, got {len(captured_prompts)}"
    )

    prompt = captured_prompts[0]

    # FAILING: The file URI should appear in the prompt sent to Gemini.
    # Currently the worker does text-only: prompt_text = IG_GOLD_PROMPT + "\n" + caption
    assert fake_file_uri in prompt, (
        f"Gemini prompt does not include uploaded file URI.\n"
        f"Expected to find: {fake_file_uri}\n"
        f"Actual prompt (first 300 chars): {prompt[:300]}\n"
        f"\nFix: pass the lookup_or_upload return value to gemini.analyze() "
        f"as multimodal content so video/audio posts get visual analysis."
    )

    # Verify gold_analyses was written
    with duckdb.get_connection() as conn:
        row = conn.execute(
            "SELECT post_id, result_json FROM gold_analyses WHERE post_id = ?",
            ["vid1"],
        ).fetchone()

    assert row is not None, "gold_analyses row not written for vid1"
    assert row[0] == "vid1"


def test_video_post_without_media_files_still_works(tmp_path):
    """GIVEN a silver post WITHOUT media files (text-only post)
    WHEN the enrichment worker processes it
    THEN Gemini is called with the caption (text-only mode works).

    This test should PASS — it verifies the text-only path isn't broken.
    """
    from scripts.enrichment_worker import process_item

    ops = SQLiteResource(database=str(tmp_path / "ops.sqlite"))
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))

    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS gold_analyses (
                post_id TEXT NOT NULL, domain TEXT NOT NULL DEFAULT 'instagram',
                prompt_hash TEXT, result_json TEXT, analysed_at TEXT NOT NULL,
                PRIMARY KEY (post_id, domain)
            )
        """)

    # Seed: a post WITHOUT media
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS silver_ig_posts (
                post_id TEXT PRIMARY KEY, caption TEXT, processed_on TIMESTAMP,
                owner_id TEXT DEFAULT 'test', owner_username TEXT DEFAULT 'test',
                likes_count INTEGER DEFAULT 0, comments_count INTEGER DEFAULT 0,
                video_play_count INTEGER DEFAULT 0, video_view_count INTEGER DEFAULT 0,
                timestamp TIMESTAMP DEFAULT NOW(), hashtags TEXT DEFAULT '[]',
                has_engagement_bait BOOLEAN DEFAULT FALSE, media_files TEXT DEFAULT '[]',
                media_count INTEGER DEFAULT 0, source_dataset TEXT DEFAULT 'test',
                shortcode TEXT DEFAULT '', url TEXT DEFAULT '', meta_data TEXT DEFAULT '{}'
            )
        """)
        conn.execute(
            "INSERT INTO silver_ig_posts (post_id, caption, processed_on) VALUES (?, ?, ?)",
            ["txt1", "A text-only post", now],
        )

    from datalake.defs.enrichment.batch import (
        _ensure_schema,
        claim_batch,
        claim_pending_items,
        create_batch,
    )

    _ensure_schema(ops)
    job_id = create_batch(ops, ["txt1"], ["instagram"])
    claim_batch(ops)
    items = claim_pending_items(ops, job_id, limit=1)
    item = items[0]

    mock_gemini = MagicMock()
    mock_gemini.analyze.return_value = '{"is_educational": false}'

    with patch("scripts.enrichment_worker.lookup_or_upload", return_value=None):
        result = process_item(ops, duckdb, mock_gemini, item)

    assert result is True

    # Verify gold was written
    with duckdb.get_connection() as conn:
        row = conn.execute(
            "SELECT post_id, result_json FROM gold_analyses WHERE post_id = ?",
            ["txt1"],
        ).fetchone()
    assert row is not None
    assert row[0] == "txt1"
