"""Unit tests for the bounded media pre-upload op (Phase 2a).

Verifies, with no network:

- pending candidates on unsubmitted batch jobs are discovered (bounded);
- media resolution is idempotent — a URL already recorded ``uploaded`` in
  ``media_metadata`` counts as a cache hit and never re-uploads;
- text-only posts are skipped without any File API interaction;
- a per-post failure is isolated (counted, run continues);
- the seam-tagged op invokes the same core (ADR-0008 seam op).

``lookup_or_upload_all`` is monkeypatched with a fake that behaves like the
real thing on the cache contract: first call records the File API URI in
``media_metadata`` (upload_state='uploaded'), later calls are cache hits.
"""

from __future__ import annotations

import json
from importlib import import_module

import pytest
from dagster import build_op_context

from datalake.defs.common.resources import DuckDBResource, SQLiteResource
from datalake.defs.enrichment.batch import _ensure_schema, create_batch
from datalake.defs.enrichment.media_upload import (
    media_upload_pending_batches_op,
    upload_media_for_pending_batches,
)

media_upload_module = import_module("datalake.defs.enrichment.media_upload")

MEDIA = [{"uri": "files/abc", "mime_type": "image/jpeg"}]


def _record_upload(ops: SQLiteResource, url: str) -> None:
    """Mirror _upload_one's persistence: record the URL as uploaded."""
    from datalake.defs.enrichment.media_cache import url_hash

    conn = ops.get_connection()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO media_metadata "
            "(media_url_hash, media_url, file_api_uri, mime_type, upload_state, "
            " expires_at, created_at, uploaded_at) "
            "VALUES (?, ?, 'files/abc', 'image/jpeg', 'uploaded', "
            "'9999-12-31T00:00:00+00:00', '2026-01-01', '2026-01-01')",
            [url_hash(url), url],
        )
        conn.commit()
    finally:
        conn.close()


def _fake_lookup(ops: SQLiteResource, calls: list[str]):
    """Cache-contract-faithful fake of lookup_or_upload_all."""

    def fake(ops_, gemini, media_files_json, inline_images=False):
        urls = json.loads(media_files_json)
        calls.extend(urls)
        for url in urls:
            conn = ops_.get_connection()
            try:
                from datalake.defs.enrichment.media_cache import url_hash

                row = conn.execute(
                    "SELECT upload_state FROM media_metadata "
                    "WHERE media_url_hash = ?",
                    [url_hash(url)],
                ).fetchone()
            finally:
                conn.close()
            if not row or row[0] != "uploaded":
                _record_upload(ops_, url)
        return [dict(m) for m in MEDIA]

    return fake


@pytest.fixture()
def env(tmp_path):
    """Ops db with one unsubmitted batch job + DuckDB silver rows p1/p2/p3."""
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
    create_batch(ops, payloads, mode="gemini-batch")
    return ops, duckdb


class TestMediaUploadPendingBatches:
    def test_uploads_pending_media_once_then_cache_hits(
        self, env, monkeypatch
    ):
        """Second run over the same candidates must not re-upload."""
        ops, duckdb = env
        calls: list[str] = []
        monkeypatch.setattr(
            media_upload_module, "lookup_or_upload_all",
            _fake_lookup(ops, calls),
        )

        first = upload_media_for_pending_batches(ops, duckdb, None)
        assert first["posts"] == 2  # p3 is text-only
        assert first["uploads"] == 2
        assert first["failed"] == 0

        second = upload_media_for_pending_batches(ops, duckdb, None)
        assert second["posts"] == 2
        assert second["uploads"] == 0  # idempotent — no duplicate uploads
        assert second["cache_hits"] == 2

    def test_bounded_by_limit(self, env, monkeypatch):
        ops, duckdb = env
        calls: list[str] = []
        monkeypatch.setattr(
            media_upload_module, "lookup_or_upload_all",
            _fake_lookup(ops, calls),
        )
        result = upload_media_for_pending_batches(ops, duckdb, None, limit=1)
        assert result["posts"] == 1
        assert len(calls) == 1

    def test_text_only_post_skipped(self, env, monkeypatch):
        ops, duckdb = env
        calls: list[str] = []
        monkeypatch.setattr(
            media_upload_module, "lookup_or_upload_all",
            _fake_lookup(ops, calls),
        )
        upload_media_for_pending_batches(ops, duckdb, None)
        assert "https://cdn/img1.jpg" in calls
        assert "https://cdn/img2.jpg" in calls
        # p3 has NULL media_files — never touches the File API path.

    def test_per_post_failure_isolated(self, env, monkeypatch):
        """One dead post's media failure must not abort the bounded run."""
        ops, duckdb = env

        def raising_fake(ops_, gemini, media_files_json, inline_images=False):
            if "img1" in media_files_json:
                raise RuntimeError("simulated File API outage")
            return [dict(m) for m in MEDIA]

        monkeypatch.setattr(
            media_upload_module, "lookup_or_upload_all",
            raising_fake,
        )
        result = upload_media_for_pending_batches(ops, duckdb, None)
        assert result["failed"] == 1
        assert result["posts"] == 1  # p2 still resolved

    def test_op_invocation_with_resources(self, env, monkeypatch):
        """The seam-tagged op drives the same core under a real op context."""
        ops, duckdb = env
        monkeypatch.setattr(
            media_upload_module, "lookup_or_upload_all",
            _fake_lookup(ops, []),
        )
        context = build_op_context(
            resources={"ops": ops, "duckdb": duckdb, "gemini": None}
        )
        result = media_upload_pending_batches_op(context, ops, duckdb, None)
        assert result["posts"] == 2
