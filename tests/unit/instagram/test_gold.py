"""Tests for the enrichment queue and enqueue asset.

Verifies the queue-based enrichment architecture:
- Queue operations (enqueue, claim, complete, fail, reschedule)
- ig_posts_gld_enqueue asset behaviour
- SQLiteResource integration
"""

from __future__ import annotations

from dagster_duckdb import DuckDBResource
from datalake.defs.common.resources import SQLiteResource
from datalake.defs.enrichment.queue import (
    MAX_ATTEMPTS,
    claim,
    complete,
    delete,
    depth,
    enqueue,
    fail,
    reschedule,
)
from datalake.defs.instagram.assets import ig_posts_gld_enqueue

# ── Fixtures ────────────────────────────────────────────────────────────────
def _make_ops_db(tmp_path) -> SQLiteResource:
    return SQLiteResource(database=str(tmp_path / "ops.sqlite"))


def _make_duckdb(tmp_path, create_gold_analyses: bool = True) -> DuckDBResource:
    db = DuckDBResource(database=str(tmp_path / "state.duckdb"))
    with db.get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS watermarks (
                name TEXT PRIMARY KEY,
                timestamp TIMESTAMP NOT NULL,
                config_hash TEXT
            )
        """)
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
        if create_gold_analyses:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS gold_analyses (
                    post_id TEXT NOT NULL, domain TEXT NOT NULL DEFAULT 'instagram',
                    prompt_hash TEXT, result_json TEXT, analysed_at TEXT NOT NULL,
                    PRIMARY KEY (post_id, domain)
                )
            """)
    return db


def _seed_silver(db: DuckDBResource, rows: list[tuple]) -> None:
    """Seed silver_ig_posts table for enqueue testing."""
    with db.get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS silver_ig_posts (
                post_id TEXT PRIMARY KEY,
                caption TEXT,
                processed_on TIMESTAMP,
                owner_id TEXT DEFAULT 'test',
                owner_username TEXT DEFAULT 'test',
                likes_count INTEGER DEFAULT 0,
                comments_count INTEGER DEFAULT 0,
                video_play_count INTEGER DEFAULT 0,
                video_view_count INTEGER DEFAULT 0,
                timestamp TIMESTAMP DEFAULT NOW(),
                hashtags TEXT DEFAULT '[]',
                has_engagement_bait BOOLEAN DEFAULT FALSE,
                media_files TEXT DEFAULT '[]',
                media_count INTEGER DEFAULT 0,
                source_dataset TEXT DEFAULT 'test',
                shortcode TEXT DEFAULT '',
                url TEXT DEFAULT '',
                meta_data TEXT DEFAULT '{}'
            )
        """)
        for post_id, caption, processed_on in rows:
            conn.execute(
                """INSERT OR REPLACE INTO silver_ig_posts
                   (post_id, caption, processed_on)
                   VALUES (?, ?, ?)""",
                [post_id, caption, processed_on],
            )


# ── Queue operation tests ───────────────────────────────────────────────────


def test_enqueue_and_claim(tmp_path):
    """GIVEN an empty queue
    WHEN a post is enqueued and then claimed
    THEN claim returns the post and sets status to processing.
    """
    ops = _make_ops_db(tmp_path)
    enqueue(ops, "p1", "instagram")

    rows = claim(ops, limit=5)
    assert len(rows) == 1
    assert rows[0]["post_id"] == "p1"
    assert rows[0]["domain"] == "instagram"


def test_enqueue_idempotent(tmp_path):
    """GIVEN a post already in the queue
    WHEN the same post is enqueued again
    THEN the row is reset to pending with attempts=0.
    """
    ops = _make_ops_db(tmp_path)
    enqueue(ops, "p1", "instagram")
    claim(ops, limit=5)  # Claim it → status becomes 'processing'

    # Re-enqueue (simulates watermark reset)
    enqueue(ops, "p1", "instagram")

    rows = claim(ops, limit=5)
    assert len(rows) == 1


def test_claim_empty_queue(tmp_path):
    """GIVEN an empty queue
    WHEN claim is called
    THEN it returns an empty list.
    """
    ops = _make_ops_db(tmp_path)
    rows = claim(ops, limit=5)
    assert rows == []


def test_claim_respects_scheduled_for(tmp_path):
    """GIVEN a post scheduled for the future
    WHEN claim is called
    THEN the post is not claimed.
    """
    ops = _make_ops_db(tmp_path)
    enqueue(ops, "p1", "instagram")

    # Manually set scheduled_for to far future
    conn = ops.get_connection()
    conn.execute(
        "UPDATE enrichment_queue SET scheduled_for = '2099-01-01T00:00:00' WHERE post_id = 'p1'"
    )
    conn.commit()
    conn.close()

    rows = claim(ops, limit=5)
    assert len(rows) == 0


def test_complete_removes_from_pending(tmp_path):
    """GIVEN a claimed post
    WHEN complete is called
    THEN it's no longer claimable.
    """
    ops = _make_ops_db(tmp_path)
    enqueue(ops, "p1", "instagram")
    claim(ops, limit=5)
    complete(ops, "p1", "instagram")

    rows = claim(ops, limit=5)
    assert len(rows) == 0


def test_fail_reschedules_with_backoff(tmp_path):
    """GIVEN a claimed post
    WHEN fail is called with a backoff
    THEN attempts is incremented and the post is rescheduled.
    """
    ops = _make_ops_db(tmp_path)
    enqueue(ops, "p1", "instagram")
    claim(ops, limit=5)

    attempts = fail(ops, "p1", "instagram", "test error", backoff_seconds=60)
    assert attempts == 2

    # Should not be claimable yet (scheduled for 60s from now)
    rows = claim(ops, limit=5)
    assert len(rows) == 0


def test_reschedule_preserves_attempts(tmp_path):
    """GIVEN a claimed post
    WHEN reschedule is called (quota exhaustion)
    THEN attempts is preserved (not incremented).
    """
    ops = _make_ops_db(tmp_path)
    enqueue(ops, "p1", "instagram")
    claim(ops, limit=5)  # increments attempts to 1

    reschedule(ops, "p1", "instagram", "quota exhausted", backoff_seconds=3600)

    conn = ops.get_connection()
    row = conn.execute(
        "SELECT attempts FROM enrichment_queue WHERE post_id = 'p1'"
    ).fetchone()
    conn.close()
    assert row["attempts"] == 1  # Preserved, not incremented


def test_depth_tracks_pending_and_processing(tmp_path):
    """GIVEN enqueued and claimed posts
    WHEN depth is called
    THEN it returns the count of non-complete items.
    """
    ops = _make_ops_db(tmp_path)
    enqueue(ops, "p1", "instagram")
    enqueue(ops, "p2", "instagram")
    claim(ops, limit=1)  # p1 → processing

    d = depth(ops)
    assert d == 2  # p1 in processing + p2 in pending


def test_max_attempts_then_delete(tmp_path):
    """GIVEN a post that fails repeatedly
    WHEN attempts reaches MAX_ATTEMPTS
    THEN the post should be moved to dead_letter and deleted from queue.
    """
    ops = _make_ops_db(tmp_path)
    enqueue(ops, "p1", "instagram")
    claim(ops, limit=5)  # attempts = 1

    for _ in range(MAX_ATTEMPTS - 1):  # Already at 1, need MAX_ATTEMPTS - 1 more
            fail(ops, "p1", "instagram", "test error", backoff_seconds=0)

    # Now delete from queue
    delete(ops, "p1", "instagram")

    conn = ops.get_connection()
    row = conn.execute(
        "SELECT COUNT(*) as cnt FROM enrichment_queue WHERE post_id = 'p1'"
    ).fetchone()
    conn.close()
    assert row["cnt"] == 0


# ── Enqueue asset tests ─────────────────────────────────────────────────────


def test_enqueue_asset_writes_to_queue(tmp_path):
    """GIVEN silver has unenriched posts
    WHEN ig_posts_gld_enqueue runs
    THEN posts are enqueued and watermark advances.
    """
    db = _make_duckdb(tmp_path)
    ops = _make_ops_db(tmp_path)

    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    _seed_silver(db, [("p1", "Test caption", now), ("p2", "Another caption", now)])

    result = ig_posts_gld_enqueue(duckdb=db, ops=ops)

    assert result["enqueued"][0] == 2

    # Verify queue has the items
    claimed = claim(ops, limit=10)
    assert len(claimed) == 2
    post_ids = {r["post_id"] for r in claimed}
    assert post_ids == {"p1", "p2"}


def test_enqueue_skips_already_enriched(tmp_path):
    """GIVEN silver has posts that already exist in gold_analyses
    WHEN ig_posts_gld_enqueue runs
    THEN those posts are not enqueued.
    """
    db = _make_duckdb(tmp_path)
    ops = _make_ops_db(tmp_path)

    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    _seed_silver(db, [("p1", "Test caption", now), ("p2", "Already done", now)])

    # Mark p2 as already enriched
    with db.get_connection() as conn:
        conn.execute(
            "INSERT INTO gold_analyses (post_id, domain, analysed_at) "
            "VALUES (?, 'instagram', ?)",
            ["p2", now.isoformat()],
        )

    result = ig_posts_gld_enqueue(duckdb=db, ops=ops)

    assert result["enqueued"][0] == 1

    claimed = claim(ops, limit=10)
    assert len(claimed) == 1
    assert claimed[0]["post_id"] == "p1"


def test_enqueue_skips_empty_caption(tmp_path):
    """GIVEN silver has posts with empty captions
    WHEN ig_posts_gld_enqueue runs
    THEN empty-caption posts are not enqueued.
    """
    db = _make_duckdb(tmp_path)
    ops = _make_ops_db(tmp_path)

    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    _seed_silver(db, [("p1", "", now), ("p2", None, now)])

    result = ig_posts_gld_enqueue(duckdb=db, ops=ops)

    assert result["enqueued"][0] == 0

    claimed = claim(ops, limit=10)
    assert len(claimed) == 0


def test_enqueue_no_pending_posts(tmp_path):
    """GIVEN no unenriched silver posts
    WHEN ig_posts_gld_enqueue runs
    THEN it returns an empty DataFrame.
    """
    db = _make_duckdb(tmp_path)
    ops = _make_ops_db(tmp_path)

    result = ig_posts_gld_enqueue(duckdb=db, ops=ops)

    assert result.is_empty()
