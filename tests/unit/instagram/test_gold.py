"""Tests for the batch-based enrichment architecture.

Verifies:
- Batch operations (create_batch, claim_batch, complete_item, fail_item, reschedule)
- ig_posts_gen_batches asset behaviour
- SQLiteResource integration
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from datalake.defs.common.resources import DuckDBResource, SQLiteResource
from datalake.defs.enrichment.batch import (
    MAX_ATTEMPTS,
    claim_batch,
    claim_pending_items,
    complete_item,
    create_batch,
    fail_item,
    mark_complete,
)
from datalake.defs.instagram.assets import ig_posts_gen_batches

# ── Helpers ──────────────────────────────────────────────────────────────────

def _pd(post_id: str, domain: str = "instagram") -> str:
    """Build a Gemini-consumer payload string."""
    return json.dumps({"post_id": post_id, "domain": domain})


def _make_ops_db(tmp_path):
    return SQLiteResource(database=str(tmp_path / "ops.sqlite"))


def _make_duckdb(tmp_path):
    return DuckDBResource(database=str(tmp_path / "state.duckdb"))


def _seed_silver(db, rows):
    """Seed silver_ig_posts with (post_id, caption, processed_on) tuples."""
    with db.get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS silver_ig_posts (
                post_id TEXT PRIMARY KEY, caption TEXT,
                processed_on TIMESTAMP, timestamp TIMESTAMP,
                source_dataset TEXT NOT NULL DEFAULT '',
                url TEXT, shortcode TEXT, owner_id TEXT, owner_username TEXT,
                likes_count INTEGER, comments_count INTEGER,
                video_play_count INTEGER, video_view_count INTEGER,
                hashtags TEXT, meta_data TEXT,
                has_engagement_bait BOOLEAN, media_files TEXT, media_count INTEGER
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS gold_analyses (
                post_id TEXT NOT NULL, domain TEXT NOT NULL DEFAULT 'instagram',
                prompt_hash TEXT, result_json TEXT, analysed_at TEXT NOT NULL,
                PRIMARY KEY (post_id, domain)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS watermarks (
                name TEXT PRIMARY KEY, timestamp TIMESTAMP NOT NULL, config_hash TEXT
            )
        """)
    for post_id, caption, ts in rows:
        with db.get_connection() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO silver_ig_posts "
                "(post_id, caption, processed_on, timestamp, source_dataset) "
                "VALUES (?, ?, ?, ?, 'test')",
                [post_id, caption, ts, ts],
            )


# ── Batch operation tests ───────────────────────────────────────────────────


def test_create_batch_and_claim(tmp_path):
    """GIVEN an empty ops.sqlite
    WHEN a batch is created and then claimed
    THEN claim_batch returns the batch with all items.
    """
    ops = _make_ops_db(tmp_path)
    create_batch(ops, [_pd("p1"), _pd("p2")])

    batch = claim_batch(ops)
    assert batch is not None
    assert len(batch["payloads"]) == 2
    post_ids = [json.loads(p)["post_id"] for p in batch["payloads"]]
    assert "p1" in post_ids
    assert "p2" in post_ids


def test_create_batch_empty_raises(tmp_path):
    """GIVEN an empty payloads list
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
    create_batch(ops, [_pd("p1"), _pd("p2"), _pd("p3")])
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
    create_batch(ops, [_pd("p1")])
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
    create_batch(ops, [_pd("p1")])
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
    create_batch(ops, [_pd("p1")])
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
    create_batch(ops, [_pd("p1")])
    batch = claim_batch(ops)

    mark_complete(ops, batch["id"])

    conn = ops.get_connection()
    row = conn.execute(
        "SELECT status FROM batch_jobs WHERE id = ?",
        [batch["id"]],
    ).fetchone()
    conn.close()
    assert row[0] == "complete"


def test_fail_item_honors_backoff(tmp_path):
    """GIVEN a claimed item
    WHEN fail_item is called with a positive backoff
    THEN the item is rescheduled with a future scheduled_for and is not
    claimable until that time passes.
    """
    ops = _make_ops_db(tmp_path)
    create_batch(ops, [_pd("p1")])
    batch = claim_batch(ops)
    items = claim_pending_items(ops, batch["id"], limit=5)
    assert len(items) == 1

    attempts = fail_item(ops, items[0]["id"], "rate limit", backoff=30)
    assert attempts == 1

    # scheduled_for is 30s in the future — not claimable yet
    assert claim_pending_items(ops, batch["id"], limit=5) == []

    # Back-date scheduled_for so it becomes due
    past = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
    conn = ops.get_connection()
    conn.execute(
        "UPDATE batch_items SET scheduled_for = ? WHERE id = ?",
        [past, items[0]["id"]],
    )
    conn.commit()
    conn.close()

    items2 = claim_pending_items(ops, batch["id"], limit=5)
    assert len(items2) == 1


def test_fail_item_preserve_attempts(tmp_path):
    """GIVEN a claimed item
    WHEN fail_item is called with preserve_attempts=True
    THEN attempts is unchanged and the item is rescheduled (not failed).
    """
    ops = _make_ops_db(tmp_path)
    create_batch(ops, [_pd("p1")])
    batch = claim_batch(ops)
    items = claim_pending_items(ops, batch["id"], limit=5)
    item_id = items[0]["id"]

    attempts = fail_item(
        ops, item_id, "quota exhausted", backoff=3600, preserve_attempts=True
    )
    assert attempts == 0

    conn = ops.get_connection()
    row = conn.execute(
        "SELECT attempts, status, scheduled_for FROM batch_items WHERE id = ?",
        [item_id],
    ).fetchone()
    conn.close()
    assert row[0] == 0
    assert row[1] == "pending"
    assert row[2] is not None


def test_claim_batch_reclaims_processing_with_pending(tmp_path):
    """GIVEN a 'processing' batch that still has pending items
    WHEN claim_batch is called
    THEN the batch is reclaimed so retries work across worker runs.
    """
    ops = _make_ops_db(tmp_path)
    create_batch(ops, [_pd("p1"), _pd("p2")])
    batch = claim_batch(ops)

    # Complete p1, leaving p2 pending in a 'processing' batch.
    items = claim_pending_items(ops, batch["id"], limit=1)
    complete_item(ops, items[0]["id"])

    batch2 = claim_batch(ops)
    assert batch2 is not None
    assert batch2["id"] == batch["id"]


# ── Enqueue asset tests ─────────────────────────────────────────────────────


def test_enqueue_asset_writes_batch(tmp_path):
    """GIVEN silver has unenriched posts
    WHEN ig_posts_gen_batches runs
    THEN a batch is created and watermark advances.
    """
    db = _make_duckdb(tmp_path)
    ops = _make_ops_db(tmp_path)

    now = datetime.now(timezone.utc)
    _seed_silver(db, [("p1", "Test caption", now), ("p2", "Another caption", now)])

    result = ig_posts_gen_batches(duckdb=db, ops=ops)

    assert result["enqueued"][0] == 2

    # Verify batch has the items
    batch = claim_batch(ops)
    assert batch is not None
    assert len(batch["payloads"]) == 2
    post_ids = {json.loads(p)["post_id"] for p in batch["payloads"]}
    assert post_ids == {"p1", "p2"}


def test_enqueue_skips_already_enriched(tmp_path):
    """GIVEN silver has posts that already exist in gold_analyses
    WHEN ig_posts_gen_batches runs
    THEN those posts are not batched.
    """
    db = _make_duckdb(tmp_path)
    ops = _make_ops_db(tmp_path)

    now = datetime.now(timezone.utc)
    _seed_silver(db, [("p1", "Test caption", now), ("p2", "Already done", now)])

    # Mark p2 as already enriched
    with db.get_connection() as conn:
        conn.execute(
            "INSERT INTO gold_analyses (post_id, domain, analysed_at) "
            "VALUES (?, 'instagram', ?)",
            ["p2", now.isoformat()],
        )

    result = ig_posts_gen_batches(duckdb=db, ops=ops)

    assert result["enqueued"][0] == 1

    batch = claim_batch(ops)
    assert batch is not None
    post_ids = [json.loads(p)["post_id"] for p in batch["payloads"]]
    assert post_ids == ["p1"]


def test_enqueue_skips_empty_caption(tmp_path):
    """GIVEN silver has posts with empty captions
    WHEN ig_posts_gen_batches runs
    THEN empty-caption posts are not batched.
    """
    db = _make_duckdb(tmp_path)
    ops = _make_ops_db(tmp_path)

    now = datetime.now(timezone.utc)
    _seed_silver(db, [("p1", "   ", now)])

    result = ig_posts_gen_batches(duckdb=db, ops=ops)

    assert result["enqueued"][0] == 0

    batch = claim_batch(ops)
    assert batch is None


def test_enqueue_no_pending_posts(tmp_path):
    """GIVEN no unenriched silver posts
    WHEN ig_posts_gen_batches runs
    THEN it returns an empty DataFrame.
    """
    db = _make_duckdb(tmp_path)
    ops = _make_ops_db(tmp_path)

    # Seed schema so the SELECT doesn't fail on missing table
    _seed_silver(db, [])

    result = ig_posts_gen_batches(duckdb=db, ops=ops)
    assert len(result) == 0



def test_enqueue_post_ids_restricts_batch(tmp_path):
    """GIVEN posts already past the gold watermark
    WHEN ig_posts_gen_batches runs with post_ids
    THEN only the requested posts are batched (watermark bypassed) and the
         gold watermark is left unchanged.
    """
    from datalake.defs.instagram.config import GoldConfig

    db = _make_duckdb(tmp_path)
    ops = _make_ops_db(tmp_path)

    now = datetime.now(timezone.utc)
    _seed_silver(
        db,
        [
            ("p1", "Caption one", now),
            ("p2", "Caption two", now),
            ("p3", "Caption three", now),
        ],
    )

    # Advance the gold watermark past all three posts — the normal path skips them.
    with db.get_connection() as conn:
        conn.execute(
            "INSERT INTO watermarks (name, timestamp) VALUES ('gold_ig', ?)",
            [now + timedelta(days=1)],
        )
        before = conn.execute(
            "SELECT timestamp FROM watermarks WHERE name = 'gold_ig'"
        ).fetchone()[0]

    result = ig_posts_gen_batches(
        config=GoldConfig(post_ids=["p2", "p3"]), duckdb=db, ops=ops
    )

    assert result["enqueued"][0] == 2
    batch = claim_batch(ops)
    assert batch is not None
    post_ids = {json.loads(p)["post_id"] for p in batch["payloads"]}
    assert post_ids == {"p2", "p3"}

    # A targeted run must not advance the gold watermark.
    with db.get_connection() as conn:
        after = conn.execute(
            "SELECT timestamp FROM watermarks WHERE name = 'gold_ig'"
        ).fetchone()[0]
    assert after == before
