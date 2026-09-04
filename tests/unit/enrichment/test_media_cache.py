"""Unit tests for the scrape-time media byte cache.

The load-bearing property: media bytes are downloaded at scrape time and the
enrichment worker uploads from local bytes, never depending on CDN URLs that
expire in ~4-5 days.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from datalake.defs.common.resources import GeminiResource, SQLiteResource
from datalake.defs.enrichment.media_cache import (
    cache_media_bytes,
    cached_local_path,
    lookup_or_upload_all,
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


def test_lookup_or_upload_all_prefers_cached_bytes(tmp_path):
    """A cached byte file is uploaded directly — the CDN is never hit."""
    ops = SQLiteResource(database=str(tmp_path / "ops.sqlite"))
    gemini = GeminiResource(api_key="test-key")

    url = "https://cdn.example.com/expired-after-5-days.jpg"
    local = tmp_path / "cached.jpg"
    local.write_bytes(b"cached-bytes")

    # Seed the byte cache directly.
    from datalake.defs.enrichment.media_cache import _ensure_media_cache_table

    _ensure_media_cache_table(ops)
    conn = ops.get_connection()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO media_cache "
            "(cache_key, local_path, content_type, size_bytes, fetched_at, source_url) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            [
                url_hash(url),
                str(local),
                "image/jpeg",
                local.stat().st_size,
                "2026-01-01T00:00:00+00:00",
                url,
            ],
        )
        conn.commit()
    finally:
        conn.close()

    # Mock the Gemini client: upload returns a File with a URI + ACTIVE state.
    fake_file = MagicMock()
    fake_file.name = "files/fake-abc123"
    fake_file.uri = "https://generativelanguage.googleapis.com/v1beta/files/fake-abc123"
    fake_file.mime_type = "image/jpeg"
    fake_file.size_bytes = 12
    fake_file.video_metadata = None
    fake_file.state.name = "ACTIVE"

    fake_files = MagicMock()
    fake_files.upload.return_value = fake_file
    fake_files.get.return_value = fake_file

    fake_client = MagicMock()
    fake_client.files = fake_files

    with patch("google.genai.Client", return_value=fake_client), patch(
        "datalake.defs.enrichment.media_cache._download_to_file",
        side_effect=AssertionError("CDN download must not be hit on a cache hit"),
    ) as download:
        result = lookup_or_upload_all(ops, gemini, json.dumps([url]))

    assert len(result) == 1
    assert result[0]["uri"] == fake_file.uri
    assert result[0]["mime_type"] == "image/jpeg"

    # The upload must have used the cached local path, not a temp download.
    upload_arg = fake_files.upload.call_args.kwargs.get("file")
    assert upload_arg == str(local)
    download.assert_not_called()


def test_lookup_upload_passes_resolved_mime_when_content_type_is_generic(tmp_path):
    """#20: an octet-stream content type must fall back to the URL extension so
    the upload sends a real mime_type instead of triggering the File API
    "Unknown mime type" rejection that dead-lettered ~3% of media posts."""
    ops = SQLiteResource(database=str(tmp_path / "ops.sqlite"))
    gemini = GeminiResource(api_key="test-key")

    url = "https://cdn.example.com/path/pic.jpg"
    local = tmp_path / "pic.jpg"
    local.write_bytes(b"fake-image-bytes")

    from datalake.defs.enrichment.media_cache import _ensure_media_cache_table

    _ensure_media_cache_table(ops)
    conn = ops.get_connection()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO media_cache "
            "(cache_key, local_path, content_type, size_bytes, fetched_at, source_url) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            [
                url_hash(url),
                str(local),
                "application/octet-stream",  # generic — Gemini could not sniff this
                local.stat().st_size,
                "2026-01-01T00:00:00+00:00",
                url,
            ],
        )
        conn.commit()
    finally:
        conn.close()

    fake_file = MagicMock()
    fake_file.name = "files/fake-img1"
    fake_file.uri = "https://generativelanguage.googleapis.com/v1beta/files/fake-img1"
    fake_file.mime_type = "image/jpeg"
    fake_file.size_bytes = 12
    fake_file.video_metadata = None
    fake_file.state.name = "ACTIVE"

    fake_files = MagicMock()
    fake_files.upload.return_value = fake_file
    fake_files.get.return_value = fake_file

    with patch("google.genai.Client", return_value=MagicMock(files=fake_files)):
        result = lookup_or_upload_all(ops, gemini, json.dumps([url]))

    assert len(result) == 1
    # The upload must have carried a real mime (extension fallback from .jpg).
    sent = fake_files.upload.call_args.kwargs
    assert sent.get("mime_type") == "image/jpeg", sent
    assert sent.get("file") == str(local)
