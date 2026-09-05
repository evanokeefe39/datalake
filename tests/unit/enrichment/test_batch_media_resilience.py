"""Unit tests for gemini-batch submit-path media resilience.

Verifies that a single dead-media item (e.g. a genuine cache miss whose CDN
URL returns HTTP 403) cannot abort the whole batch submission:

- all-cached media → every media-bearing post submits multimodally;
- one item's media resolution raising → that item is failed (attempts++,
  backoff) and EXCLUDED from the returned requests while the rest still
  submit — mirroring the interactive per-item failure routing;
- at MAX_ATTEMPTS the dead item lands in dead_letter;
- text-only posts stay text-only.

No network: ``_resolve_media_for_post`` is monkeypatched; a real
``urllib.error.HTTPError`` is raised to mirror the live 403 crash.
"""

from __future__ import annotations

import json
import sqlite3
import urllib.error

import pytest

from datalake.defs.common.resources import DuckDBResource, SQLiteResource
from datalake.defs.enrichment.batch import (
    MAX_ATTEMPTS,
    _ensure_schema,
    claim_pending_items,
    create_batch,
)
from scripts.enrichment_worker import (
    _item_attempts,
    build_requests_for_items,
)


@pytest.fixture()
def env(tmp_path):
    """Ops db with a claimed gemini-batch + DuckDB silver rows for p1/p2/p3."""
    ops = SQLiteResource(database=str(tmp_path / "ops.sqlite"))
    _ensure_schema(ops)
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))
    with duckdb.get_connection() as conn:
        conn.execute(
            "CREATE TABLE silver_ig_posts "
            "(post_id VARCHAR, caption VARCHAR, media_files VARCHAR)"
        )
        conn.execute(
            "INSERT INTO silver_ig_posts VALUES "
            "('p1', 'caption one', '[\"https://cdn/img1.jpg\"]'), "
            "('p2', 'caption two', '[\"https://cdn/img2.jpg\"]'), "
            "('p3', 'caption three', NULL)"
        )
    payloads = [
        json.dumps({"post_id": pid, "domain": "instagram"})
        for pid in ("p1", "p2", "p3")
    ]
    job_id = create_batch(ops, payloads, mode="gemini-batch")
    items = claim_pending_items(ops, job_id, limit=100)
    assert len(items) == 3
    by_post = {json.loads(it["payload"])["post_id"]: it for it in items}
    return ops, duckdb, items, by_post


def _patch(monkeypatch, behavior):
    """Patch _resolve_media_for_post: behavior(post_id) called per item."""
    import scripts.enrichment_worker as worker

    monkeypatch.setattr(
        worker,
        "_resolve_media_for_post",
        lambda ops, gemini, post_id, media_json: behavior(post_id),
    )


def _set_attempts(ops, item_id, attempts):
    conn = sqlite3.connect(str(ops.database))
    conn.execute(
        "UPDATE batch_items SET attempts = ? WHERE id = ?", [attempts, item_id]
    )
    conn.commit()
    conn.close()


class TestBatchMediaResilience:
    def test_all_cached_media_submits_multimodal(self, env, monkeypatch):
        ops, duckdb, items, _ = env
        media = [{"uri": "files/abc", "mime_type": "image/jpeg"}]
        _patch(monkeypatch, lambda pid: media)

        reqs = build_requests_for_items(ops, duckdb, None, items)

        # p1/p2 submit multimodal from cache; p3 has no media at all.
        assert [r["post_id"] for r in reqs] == ["p1", "p2", "p3"]
        for req in reqs[:2]:
            assert req["media_files"] == media
        assert "media_files" not in reqs[2]


    def test_dead_media_item_failed_and_excluded(self, env, monkeypatch):
        """One HTTP-403 cache miss must not abort the batch submission."""
        ops, duckdb, items, by_post = env
        media = [{"uri": "files/abc", "mime_type": "image/jpeg"}]

        def behavior(pid):
            if pid == "p2":  # genuine cache miss → CDN 403
                raise urllib.error.HTTPError(
                    "https://cdn/img2.jpg", 403, "Forbidden", None, None
                )
            return media

        _patch(monkeypatch, behavior)

        reqs = build_requests_for_items(ops, duckdb, None, items)

        # p1 (cached) and p3 (text-only) still submit; p2 is excluded.
        assert [r["post_id"] for r in reqs] == ["p1", "p3"]
        assert reqs[0]["media_files"] == media
        assert "media_files" not in reqs[1]
        # p2 was failed with backoff (attempts incremented), not completed.
        assert _item_attempts(ops, by_post["p2"]["id"]) == 1
        assert _item_attempts(ops, by_post["p1"]["id"]) == 0

    def test_dead_media_item_dead_letters_at_max_attempts(
        self, env, monkeypatch
    ):
        ops, duckdb, items, by_post = env
        # Pre-exhaust the item so the next media failure crosses MAX_ATTEMPTS.
        _set_attempts(ops, by_post["p2"]["id"], MAX_ATTEMPTS - 1)

        def behavior(pid):
            if pid == "p2":
                raise RuntimeError("HTTP Error 403: Forbidden")
            return [{"uri": "files/abc", "mime_type": "image/jpeg"}]

        _patch(monkeypatch, behavior)
        build_requests_for_items(ops, duckdb, None, items)

        conn = sqlite3.connect(str(ops.database))
        row = conn.execute(
            "SELECT post_id, domain, attempts FROM dead_letter WHERE post_id = 'p2'"
        ).fetchone()
        conn.close()
        assert row == ("p2", "instagram", MAX_ATTEMPTS)

    def test_text_only_item_untouched(self, env, monkeypatch):
        ops, duckdb, items, _ = env
        _patch(
            monkeypatch,
            lambda pid: (
                []
                if pid == "p3"
                else [{"uri": "files/abc", "mime_type": "image/jpeg"}]
            ),
        )

        reqs = build_requests_for_items(ops, duckdb, None, items)

        p3 = next(r for r in reqs if r["post_id"] == "p3")
        assert "media_files" not in p3
        assert p3["prompt"].endswith("caption three")
