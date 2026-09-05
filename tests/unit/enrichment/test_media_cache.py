"""Unit tests for the scrape-time media byte cache.

The load-bearing property: media bytes are downloaded at scrape time and the
enrichment worker uploads from local bytes, never depending on CDN URLs that
expire in ~4-5 days.
"""

from __future__ import annotations

import json
import os
import threading
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
    # The upload must have carried a real mime (extension fallback from .jpg),
    # passed via the UploadFileConfig (Files.upload has no top-level mime_type).
    sent = fake_files.upload.call_args.kwargs
    assert sent.get("config", {}).get("mime_type") == "image/jpeg", sent
    assert sent.get("file") == str(local)


class FakeNotFoundError(Exception):
    """Mimics google.genai.errors.APIError for a deleted file."""

    code = 404


def _seed_byte_cache(ops, url, local_path, content_type="image/jpeg"):
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
                str(local_path),
                content_type,
                os.path.getsize(local_path),
                "2026-01-01T00:00:00+00:00",
                url,
            ],
        )
        conn.commit()
    finally:
        conn.close()


def _make_fake_client(uris: list[str], get_state: str = "ACTIVE"):
    """Fake genai Client: upload returns a distinct ACTIVE file per call;
    files.get echoes the matching (or last uploaded) file."""
    from unittest.mock import MagicMock


    lock = threading.Lock()
    made: list = []

    def _upload(*args, **kwargs):
        with lock:
            i = len(made)
            f = MagicMock()
            f.name = f"files/fake-{i}"
            f.uri = uris[i] if i < len(uris) else f"https://x/files/fake-{i}"
            f.mime_type = "image/jpeg"
            f.size_bytes = 12
            f.video_metadata = None
            f.state.name = get_state
            made.append(f)
        return f

    def _get(name=None):
        for f in made:
            if f.name == name:
                return f
        return made[-1] if made else MagicMock()

    files = MagicMock()
    files.upload.side_effect = _upload
    files.get.side_effect = _get
    client = MagicMock()
    client.files = files
    return client, files


def test_lookup_or_upload_all_concurrent_misses_keep_input_order(tmp_path):
    """(A) A multi-URL cache miss uploads concurrently yet returns the exact
    input-ordered list of per-media dicts."""
    import threading

    ops = SQLiteResource(database=str(tmp_path / "ops.sqlite"))
    gemini = GeminiResource(api_key="test-key")
    urls = [
        "https://cdn.example.com/a.jpg",
        "https://cdn.example.com/b.jpg",
        "https://cdn.example.com/c.jpg",
    ]
    uris = [f"https://generativelanguage.googleapis.com/v1beta/files/f{i}" for i in range(3)]
    client, files = _make_fake_client(uris)
    # Prove concurrency: all three uploads must be in flight simultaneously.
    barrier = threading.Barrier(3, timeout=15)
    real_upload = files.upload.side_effect
    dest_to_url: dict = {}
    uri_by_file: dict = {}

    def _upload(*args, **kwargs):
        barrier.wait()
        f = real_upload(*args, **kwargs)
        uri_by_file[kwargs.get("file")] = f.uri
        return f

    files.upload.side_effect = _upload

    def _dl(url, dest):
        dest_to_url[dest] = url
        with open(dest, "wb") as fh:
            fh.write(b"x")
        return "image/jpeg"

    with patch("google.genai.Client", return_value=client), patch(
        "datalake.defs.enrichment.media_cache._download_to_file", side_effect=_dl
    ):
        result = lookup_or_upload_all(ops, gemini, json.dumps(urls))

    assert files.upload.call_count == 3
    # Which thread got which File API URI is nondeterministic under
    # concurrency, but the OUTPUT list must be in INPUT URL order.
    uri_by_url = {dest_to_url[f]: uri for f, uri in uri_by_file.items()}
    assert [uri_by_url[u] for u in urls] == [r["uri"] for r in result]
    assert {r["uri"] for r in result} == set(uris)
    for r in result:
        assert r["mime_type"] == "image/jpeg"
        assert "duration_seconds" in r


def test_dead_cached_uri_is_reuploaded(tmp_path):
    """(C) A served-but-dead File API URI (files.get 404) triggers a
    transparent re-upload instead of failing the request."""
    from datetime import datetime, timedelta, timezone

    ops = SQLiteResource(database=str(tmp_path / "ops.sqlite"))
    gemini = GeminiResource(api_key="test-key")
    from datalake.defs.enrichment.media_cache import _ensure_schema
    _ensure_schema(ops)
    url = "https://cdn.example.com/pic.jpg"
    local = tmp_path / "pic.jpg"
    local.write_bytes(b"fresh-bytes")
    _seed_byte_cache(ops, url, local)
    now = datetime.now(timezone.utc)
    conn = ops.get_connection()
    try:
        conn.execute(
            "INSERT INTO media_metadata (media_url_hash, media_url, file_api_uri, "
            "mime_type, upload_state, expires_at, created_at) VALUES (?,?,?,?,?,?,?)",
            [
                url_hash(url),
                url,
                "https://generativelanguage.googleapis.com/v1beta/files/dead",
                "image/jpeg",
                "uploaded",
                (now + timedelta(hours=40)).isoformat(),
                now.isoformat(),
            ],
        )
        conn.commit()
    finally:
        conn.close()

    new_uri = "https://generativelanguage.googleapis.com/v1beta/files/fresh"
    client, files = _make_fake_client([new_uri])
    files.get.side_effect = None
    # Liveness probe for the dead URI raises 404; the post-upload poll would
    # also use get — make get succeed for the freshly uploaded file only.
    def _get(name=None):
        if name == "files/dead":
            raise FakeNotFoundError("NOT_FOUND")
        f = MagicMock()
        f.uri = new_uri
        f.state.name = "ACTIVE"
        f.mime_type = "image/jpeg"
        f.video_metadata = None
        f.size_bytes = 11
        return f

    files.get.side_effect = _get

    with patch("google.genai.Client", return_value=client), patch(
        "datalake.defs.enrichment.media_cache._download_to_file",
        side_effect=AssertionError("byte cache should serve the re-upload"),
    ):
        result = lookup_or_upload_all(ops, gemini, json.dumps([url]))

    assert files.upload.call_count == 1  # re-upload happened
    assert result[0]["uri"] == new_uri


def test_cache_window_matches_verified_file_api_retention():
    """(C) Reuse window is the documented 48h File API retention minus a
    conservative margin — not the old 24h that re-uploaded every day."""
    from datalake.defs.enrichment.media_cache import (
        CACHE_REUSE_WINDOW_HOURS,
        FILE_RETENTION_MARGIN_HOURS,
        GEMINI_FILE_RETENTION_HOURS,
    )

    assert GEMINI_FILE_RETENTION_HOURS == 48  # ai.google.dev/gemini-api/docs/files
    assert FILE_RETENTION_MARGIN_HOURS == 4
    assert CACHE_REUSE_WINDOW_HOURS == 44
    assert CACHE_REUSE_WINDOW_HOURS > 24



def _seed_uploaded_metadata(ops, url, uri, expires_at):
    from datalake.defs.enrichment.media_cache import _ensure_schema

    _ensure_schema(ops)
    conn = ops.get_connection()
    try:
        conn.execute(
            "INSERT INTO media_metadata (media_url_hash, media_url, file_api_uri, "
            "mime_type, upload_state, expires_at, created_at) VALUES (?,?,?,?,?,?,?)",
            [
                url_hash(url),
                url,
                uri,
                "image/jpeg",
                "uploaded",
                expires_at,
                expires_at,
            ],
        )
        conn.commit()
    finally:
        conn.close()


def test_expired_row_conflict_never_serves_stale_uri(tmp_path):
    """An expired uploaded row is re-uploaded even though the TOCTOU recheck
    (INSERT OR IGNORE conflict) finds it 'uploaded' — no stale URI service."""
    from datetime import datetime, timedelta, timezone

    ops = SQLiteResource(database=str(tmp_path / "ops.sqlite"))
    gemini = GeminiResource(api_key="test-key")
    url = "https://cdn.example.com/old.jpg"
    local = tmp_path / "old.jpg"
    local.write_bytes(b"bytes")
    _seed_byte_cache(ops, url, local)
    past = (datetime.now(timezone.utc) - timedelta(hours=50)).isoformat()
    _seed_uploaded_metadata(
        ops, url, "https://generativelanguage.googleapis.com/v1beta/files/stale", past
    )

    new_uri = "https://generativelanguage.googleapis.com/v1beta/files/new"
    client, files = _make_fake_client([new_uri])
    with patch("google.genai.Client", return_value=client):
        lookup_or_upload_all(ops, gemini, json.dumps([url]))

    files.upload.assert_called_once()


def test_inline_aggregate_budget_downgrades_to_file_api(tmp_path, monkeypatch):
    """Once the per-request inline byte budget is exhausted, remaining small
    images fall back to the File API instead of blowing the request limit."""
    from datalake.defs.enrichment import media_cache

    monkeypatch.setattr(media_cache, "INLINE_TOTAL_BUDGET_BYTES", 10)
    ops = SQLiteResource(database=str(tmp_path / "ops.sqlite"))
    gemini = GeminiResource(api_key="test-key")

    urls = [f"https://cdn.example.com/pic{i}.jpg" for i in range(3)]
    for i, url in enumerate(urls):
        local = tmp_path / f"pic{i}.jpg"
        local.write_bytes(b"0123456789")  # 10 bytes each
        _seed_byte_cache(ops, url, local)

    uris = [f"https://generativelanguage.googleapis.com/v1beta/files/g{i}" for i in range(2)]
    client, files = _make_fake_client(uris)
    with patch("google.genai.Client", return_value=client):
        result = lookup_or_upload_all(ops, gemini, json.dumps(urls), inline_images=True)

    assert files.upload.call_count == 2  # budget 10 bytes fits exactly ONE image
    kinds = ["inline" if "inline_data" in r else "uri" for r in result]
    assert kinds == ["inline", "uri", "uri"]
