"""Unit tests for the scrape-time media byte cache.

The load-bearing property: media bytes are downloaded at scrape time and the
enrichment worker uploads from local bytes, never depending on CDN URLs that
expire in ~4-5 days.

RETIRED: the Gemini File-API upload tests (``lookup_or_upload_all`` and the
``media_metadata`` table) were removed with that cluster — retired per
ADR-0009, dropped by the W9 retirement.
"""

from __future__ import annotations

from unittest.mock import patch

from datalake.defs.common.resources import SQLiteResource
from datalake.defs.enrichment.media_cache import (
    cache_media_bytes,
    cached_local_path,
    url_hash,
)


def test_cache_media_bytes_downloads_and_records(tmp_path):
    ops = SQLiteResource(database=str(tmp_path / "ops.sqlite"))

    with patch(
        "datalake.defs.enrichment.media_cache._download_bytes",
        return_value=(b"fake-image-bytes", "image/jpeg"),
    ):
        path = cache_media_bytes(ops, "https://cdn.example.com/a.jpg", media_dir=tmp_path)

    assert path is not None
    assert open(path, "rb").read() == b"fake-image-bytes"

    conn = ops.get_connection()
    try:
        row = conn.execute(
            "SELECT local_path, content_type, source_url FROM media_cache WHERE cache_key = ?",
            [url_hash("https://cdn.example.com/a.jpg")],
        ).fetchone()
    finally:
        conn.close()

    assert row is not None
    assert row["content_type"] == "image/jpeg"
    assert row["source_url"] == "https://cdn.example.com/a.jpg"


def test_cache_media_bytes_skips_already_cached(tmp_path):
    ops = SQLiteResource(database=str(tmp_path / "ops.sqlite"))

    with patch(
        "datalake.defs.enrichment.media_cache._download_bytes",
        return_value=(b"once", "image/jpeg"),
    ) as dl:
        cache_media_bytes(ops, "https://cdn.example.com/a.jpg", media_dir=tmp_path)
        cache_media_bytes(ops, "https://cdn.example.com/a.jpg", media_dir=tmp_path)

    # Second call must be a cache hit — no re-download.
    assert dl.call_count == 1


def test_cached_local_path_returns_none_when_missing(tmp_path):
    ops = SQLiteResource(database=str(tmp_path / "ops.sqlite"))
    assert cached_local_path(ops, "https://cdn.example.com/unknown.jpg") is None
