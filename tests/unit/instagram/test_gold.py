"""Tests for the batch-based enrichment architecture.

Verifies:
- Batch operations (create_batch, claim_batch, complete_item, fail_item, reschedule)
- ig_posts_gld_enqueue asset behaviour
- SQLiteResource integration
"""

from __future__ import annotations

from dagster_duckdb import DuckDBResource

from datalake.defs.common.resources import SQLiteResource
from datalake.defs.enrichment.batch import (
    MAX_ATTEMPTS,
    claim_batch,
    claim_pending_items,
    complete_item,
    create_batch,
    fail_item,
    mark_complete,
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


# ── Batch operation tests ───────────────────────────────────────────────────


def test_create_batch_and_claim(tmp_path):
    """GIVEN an empty ops.sqlite
    WHEN a batch is created and then claimed
    THEN claim_batch returns the batch with all items.
    """
    ops = _make_ops_db(tmp_path)
    create_batch(ops, ["p1", "p2"], ["instagram", "instagram"])

    batch = claim_batch(ops)
    assert batch is not None
    assert len(batch["post_ids"]) == 2
    assert "p1" in batch["post_ids"]
    assert "p2" in batch["post_ids"]


def test_create_batch_empty_raises(tmp_path):
    """GIVEN an empty post_ids list
    WHEN create_batch is called
    THEN ValueError is raised.
    """
    ops = _make_ops_db(tmp_path)
    try:
        create_batch(ops, [])
        assert False, "Expected ValueError"
    except ValueError:
        pass


def test_claim_batch_empty(tmp_path):
    """GIVEN an empty ops.sqlite
    WHEN claim_batch is called
    THEN it returns None.
    """
    ops = _make_ops_db(tmp_path)
    batch = claim_batch(ops)
    assert batch is None


def test_claim_pending_items(tmp_path):
    """GIVEN a batch with items
    WHEN claim_pending_items is called with a limit
    THEN only that many items are claimed.
    """
    ops = _make_ops_db(tmp_path)
    create_batch(ops, ["p1", "p2", "p3"], ["instagram"] * 3)
    batch = claim_batch(ops)

    items = claim_pending_items(ops, batch["id"], limit=2)
    assert len(items) == 2

    # Remaining item should still be claimable
    items2 = claim_pending_items(ops, batch["id"], limit=5)
    assert len(items2) == 1


def test_complete_item_marks_done(tmp_path):
    """GIVEN a claimed item
    WHEN complete_item is called
    THEN the item is no longer claimable.
    """
    ops = _make_ops_db(tmp_path)
    create_batch(ops, ["p1"], ["instagram"])
    batch = claim_batch(ops)
    items = claim_pending_items(ops, batch["id"], limit=5)
    assert len(items) == 1

    complete_item(ops, items[0]["id"])

    # No more pending items
    items2 = claim_pending_items(ops, batch["id"], limit=5)
    assert len(items2) == 0


def test_fail_item_increments_attempts(tmp_path):
    """GIVEN a claimed item
    WHEN fail_item is called
    THEN attempts is incremented and item is rescheduled as pending.
    """
    ops = _make_ops_db(tmp_path)
    create_batch(ops, ["p1"], ["instagram"])
    batch = claim_batch(ops)
    items = claim_pending_items(ops, batch["id"], limit=5)
    assert len(items) == 1

    attempts = fail_item(ops, items[0]["id"], "test error", backoff=0)
    assert attempts == 1

    # Item should be claimable again (status reset to pending)
    items2 = claim_pending_items(ops, batch["id"], limit=5)
    assert len(items2) == 1


def test_fail_item_max_attempts(tmp_path):
    """GIVEN an item that fails repeatedly
    WHEN attempts reaches MAX_ATTEMPTS
    THEN the item stays failed and is counted in batch failed_items.
    """
    ops = _make_ops_db(tmp_path)
    create_batch(ops, ["p1"], ["instagram"])
    batch = claim_batch(ops)
    items = claim_pending_items(ops, batch["id"], limit=5)
    item_id = items[0]["id"]

    for i in range(MAX_ATTEMPTS):
        attempts = fail_item(ops, item_id, f"error {i}", backoff=0)

    assert attempts == MAX_ATTEMPTS

    # Item should not be claimable (status = 'failed')
    items2 = claim_pending_items(ops, batch["id"], limit=5)
    assert len(items2) == 0

    # Verify failed_items count
    conn = ops.get_connection()
    row = conn.execute(
        "SELECT failed_items FROM batch_jobs WHERE id = ?",
        [batch["id"]],
    ).fetchone()
    conn.close()
    assert row[0] == 1


def test_mark_complete(tmp_path):
    """GIVEN a processing batch with all items done
    WHEN mark_complete is called
    THEN the batch status is set to 'complete'.
    """
    ops = _make_ops_db(tmp_path)
    create_batch(ops, ["p1"], ["instagram"])
    batch = claim_batch(ops)

    mark_complete(ops, batch["id"])

    conn = ops.get_connection()
    row = conn.execute(
        "SELECT status FROM batch_jobs WHERE id = ?",
        [batch["id"]],
    ).fetchone()
    conn.close()
    assert row[0] == "complete"


# ── Enqueue asset tests ─────────────────────────────────────────────────────


def test_enqueue_asset_writes_batch(tmp_path):
    """GIVEN silver has unenriched posts
    WHEN ig_posts_gld_enqueue runs
    THEN a batch is created and watermark advances.
    """
    db = _make_duckdb(tmp_path)
    ops = _make_ops_db(tmp_path)

    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    _seed_silver(db, [("p1", "Test caption", now), ("p2", "Another caption", now)])

    result = ig_posts_gld_enqueue(duckdb=db, ops=ops)

    assert result["enqueued"][0] == 2

    # Verify batch has the items
    batch = claim_batch(ops)
    assert batch is not None
    assert len(batch["post_ids"]) == 2
    assert set(batch["post_ids"]) == {"p1", "p2"}


def test_enqueue_skips_already_enriched(tmp_path):
    """GIVEN silver has posts that already exist in gold_analyses
    WHEN ig_posts_gld_enqueue runs
    THEN those posts are not batched.
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

    batch = claim_batch(ops)
    assert batch is not None
    assert batch["post_ids"] == ["p1"]


def test_enqueue_skips_empty_caption(tmp_path):
    """GIVEN silver has posts with empty captions
    WHEN ig_posts_gld_enqueue runs
    THEN empty-caption posts are not batched.
    """
    db = _make_duckdb(tmp_path)
    ops = _make_ops_db(tmp_path)

    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    _seed_silver(db, [("p1", "", now), ("p2", None, now)])

    result = ig_posts_gld_enqueue(duckdb=db, ops=ops)

    assert result["enqueued"][0] == 0

    batch = claim_batch(ops)
    assert batch is None


def test_enqueue_no_pending_posts(tmp_path):
    """GIVEN no unenriched silver posts
    WHEN ig_posts_gld_enqueue runs
    THEN it returns an empty DataFrame.
    """
    db = _make_duckdb(tmp_path)
    ops = _make_ops_db(tmp_path)

    result = ig_posts_gld_enqueue(duckdb=db, ops=ops)

    assert result.is_empty()
