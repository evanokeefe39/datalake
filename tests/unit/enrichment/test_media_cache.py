"""Unit tests for the scrape-time media byte cache.

The load-bearing property: media bytes are downloaded at scrape time and the
enrichment worker uploads from local bytes, never depending on CDN URLs that
expire in ~4-5 days.

RETIRED: the Gemini File-API upload tests (``lookup_or_upload_all`` and the
``media_metadata`` table) were removed with that cluster — retired per
ADR-0009, dropped by the W9 retirement.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from orchestration.defs.engine.media import (
    MEDIA_CACHE_ATTEMPTS,
    cache_media_bytes,
    cache_media_urls,
    local_media_path,
    url_hash,
)
from orchestration.defs.platform.resources import SQLiteResource


def test_cache_media_bytes_downloads_and_records(tmp_path):
    ops = SQLiteResource(database=str(tmp_path / "ops.sqlite"))

    with patch(
        "orchestration.defs.engine.media._download_bytes",
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
        "orchestration.defs.engine.media._download_bytes",
        return_value=(b"once", "image/jpeg"),
    ) as dl:
        cache_media_bytes(ops, "https://cdn.example.com/a.jpg", media_dir=tmp_path)
        cache_media_bytes(ops, "https://cdn.example.com/a.jpg", media_dir=tmp_path)

    # Second call must be a cache hit — no re-download.
    assert dl.call_count == 1


def test_cache_media_bytes_retries_a_transient_failure(tmp_path):
    """A flaky CDN must not permanently lose the media.

    The cache is filled ONLY at scrape time while the signed URL is alive, so a
    single transient failure loses those bytes forever and the post can never be
    enriched. Retrying converts a lost post into a slightly slower scrape.
    """
    ops = SQLiteResource(database=str(tmp_path / "ops.sqlite"))

    with patch(
        "orchestration.defs.engine.media._download_bytes",
        side_effect=[None, None, (b"bytes", "image/jpeg")],
    ) as dl:
        with patch("orchestration.defs.engine.media.time.sleep"):
            path = cache_media_bytes(
                ops, "https://cdn.example.com/flaky.jpg", media_dir=tmp_path
            )

    assert path is not None, "third attempt succeeded — must not give up early"
    assert dl.call_count == 3


def test_cache_media_bytes_gives_up_after_the_attempt_budget(tmp_path):
    """A permanently dead URL stops after MEDIA_CACHE_ATTEMPTS and says so."""
    ops = SQLiteResource(database=str(tmp_path / "ops.sqlite"))

    with patch(
        "orchestration.defs.engine.media._download_bytes", return_value=None
    ) as dl:
        with patch("orchestration.defs.engine.media.time.sleep"):
            path = cache_media_bytes(
                ops, "https://cdn.example.com/dead.jpg", media_dir=tmp_path
            )

    assert path is None
    assert dl.call_count == MEDIA_CACHE_ATTEMPTS


def test_cache_media_bytes_does_not_retry_a_cache_hit(tmp_path):
    """Idempotence: an already-cached URL costs zero downloads, not one."""
    ops = SQLiteResource(database=str(tmp_path / "ops.sqlite"))

    with patch(
        "orchestration.defs.engine.media._download_bytes",
        return_value=(b"once", "image/jpeg"),
    ) as dl:
        cache_media_bytes(ops, "https://cdn.example.com/a.jpg", media_dir=tmp_path)
        cache_media_bytes(ops, "https://cdn.example.com/a.jpg", media_dir=tmp_path)

    assert dl.call_count == 1


def test_cache_media_urls_accounts_for_every_url(tmp_path):
    """The accounting is the point: a run's media loss must be visible.

    Discarding the per-URL result is what let a whole scrape run cache nothing
    and report success (ISSUES.md #25 — 790 posts, no media, no error).
    """
    ops = SQLiteResource(database=str(tmp_path / "ops.sqlite"))
    good = {"https://cdn.example.com/ok1.jpg", "https://cdn.example.com/ok2.jpg"}

    def fake_download(url):
        return (b"bytes", "image/jpeg") if url in good else None

    with patch("orchestration.defs.engine.media._download_bytes", side_effect=fake_download):
        with patch("orchestration.defs.engine.media.time.sleep"):
            report = cache_media_urls(
                ops,
                [
                    "https://cdn.example.com/ok1.jpg",
                    "https://cdn.example.com/dead.jpg",
                    "https://cdn.example.com/ok2.jpg",
                    # duplicate + blank must not inflate the counts
                    "https://cdn.example.com/ok1.jpg",
                    "",
                ],
                media_dir=tmp_path,
            )

    assert report.attempted == 3
    assert report.cached == 2
    assert report.failed == 1
    assert report.failed_urls == ["https://cdn.example.com/dead.jpg"]
    assert report.ok is False
    assert "2/3 cached" in report.summary() and "1 FAILED" in report.summary()


def test_cache_media_urls_reports_ok_when_nothing_fails(tmp_path):
    ops = SQLiteResource(database=str(tmp_path / "ops.sqlite"))

    with patch(
        "orchestration.defs.engine.media._download_bytes",
        return_value=(b"bytes", "image/jpeg"),
    ):
        report = cache_media_urls(
            ops, ["https://cdn.example.com/a.jpg"], media_dir=tmp_path
        )

    assert report.ok is True
    assert report.failed == 0
    assert report.summary() == "media cache: 1/1 cached"


def test_local_media_path_returns_none_when_missing(tmp_path):
    ops = SQLiteResource(database=str(tmp_path / "ops.sqlite"))
    assert local_media_path(ops, "https://cdn.example.com/unknown.jpg") is None


def test_local_media_path_returns_none_when_row_exists_but_file_deleted(tmp_path):
    """A recorded row whose bytes are gone is a MISS, not a dead path."""
    from opsdb.media_cache import _ensure_media_cache_table, record_media_cache_row

    ops = SQLiteResource(database=str(tmp_path / "ops.sqlite"))
    _ensure_media_cache_table(ops)
    url = "https://cdn.example.com/vanished.jpg"
    record_media_cache_row(
        ops, url_hash(url), str(tmp_path / "gone.jpg"), "image/jpeg", 12, url
    )
    assert local_media_path(ops, url) is None


def test_local_media_path_translates_host_prefix_to_container(tmp_path, monkeypatch):
    """A stored host path resolves to the SAME bytes under the container root.

    This is the property the Compose path map exists for: media_cache rows hold
    the fetching process's path vocabulary, and a container must translate
    before testing existence — otherwise every row reads as a miss even though
    the bytes are mounted at the translated location.
    """
    from opsdb.media_cache import _ensure_media_cache_table, record_media_cache_row
    from orchestration.defs.platform import paths

    ops = SQLiteResource(database=str(tmp_path / "ops.sqlite"))
    _ensure_media_cache_table(ops)

    # The bytes live at <tmp>/media/posts/x.jpg; pretend that whole tree is
    # mounted at /data inside the container.
    container_root = tmp_path / "container"
    media = container_root / "media" / "posts" / "x.jpg"
    media.parent.mkdir(parents=True, exist_ok=True)
    media.write_bytes(b"bytes")

    host_prefix = str(tmp_path / "host")
    stored = f"{host_prefix}/media/posts/x.jpg"

    monkeypatch.setattr(paths, "_HOST_PATH_PREFIX", host_prefix)
    monkeypatch.setattr(paths, "_CONTAINER_PATH_PREFIX", str(container_root))

    url = "https://cdn.example.com/x.jpg"
    record_media_cache_row(ops, url_hash(url), stored, "image/jpeg", 5, url)

    resolved = local_media_path(ops, url)
    assert resolved is not None
    assert Path(resolved) == media.resolve()
