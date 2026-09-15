"""Scrape-time media byte cache — URL hash → local file path.

Instagram CDN URLs expire in ~4-5 days while the enrichment worker may run
days later, so media bytes are downloaded at scrape time and stored under
``data/media/posts/`` keyed by ``sha256(url)``. The cache table
(``media_cache`` in ops.sqlite) records URL hash → local path + content type.

RETIRED: the Gemini File-API upload cluster (``lookup_or_upload_all`` and
helpers, the ``media_metadata`` table) was removed here — retired per
ADR-0009, its ``media_metadata`` table dropped by the W9 retirement and
archived at ``data/lake/archive/media_metadata/``. The live enrichment path
uploads from these local bytes and falls back to the live CDN only on a
cache miss.
"""

import hashlib
import logging
import os
import shutil
import sqlite3
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from datalake.defs.common.lake import POST_MEDIA_DIR
from datalake.defs.common.resources import SQLiteResource
from datalake.defs.common.schemas import sqlite_ddl

logger = logging.getLogger("media_cache")


def url_hash(media_url: str) -> str:
    """SHA256 hash of a media URL (not content)."""
    return hashlib.sha256(media_url.encode()).hexdigest()


# ── Scrape-time byte cache ─────────────────────────────────────────────────
#
# Instagram CDN URLs expire in ~4-5 days. The enrichment worker can run days
# after a post is scraped (quota backoff, backlog), so it must not depend on
# those URLs. This byte cache downloads media at scrape time (silver write)
# and records a ``media_cache`` row (URL hash → local path). The worker uploads
# from the local bytes, falling back to the live CDN only on a cache miss.

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

_EXT_BY_MIME = {
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
    "image/gif": ".gif",
    "video/mp4": ".mp4",
    "video/webm": ".webm",
    "video/quicktime": ".mov",
}

_CONTENT_TYPE_BY_EXT = {v: k for k, v in _EXT_BY_MIME.items()}


def _ensure_media_cache_table(ops: SQLiteResource) -> None:
    """Create ``media_cache`` if it doesn't exist (idempotent)."""
    conn = ops.get_connection()
    try:
        conn.execute(sqlite_ddl("media_cache"))
        conn.commit()
    finally:
        conn.close()


def _download_bytes(url: str) -> tuple[bytes, str] | None:
    """Download ``url``; return ``(bytes, content_type)`` or None on failure."""
    req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            content_type = resp.headers.get("Content-Type", "")
            return resp.read(), content_type
    except Exception as exc:  # network errors are non-fatal — cache is best-effort
        logger.warning("media download failed for %s: %s", url[:80], exc)
        return None


def _atomic_write(path: Path, data: bytes) -> None:
    """Write bytes atomically (temp file + rename) to avoid partial reads."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(data)
    os.replace(tmp, path)


def cached_local_path(ops: SQLiteResource, media_url: str) -> str | None:
    """Return the local byte path for a URL if cached and the file exists."""
    conn = ops.get_connection()
    try:
        row = conn.execute(
            "SELECT local_path FROM media_cache WHERE cache_key = ?",
            [url_hash(media_url)],
        ).fetchone()
    except sqlite3.OperationalError:
        # media_cache table not yet created — nothing can be cached.
        return None
    finally:
        conn.close()
    if not row:
        return None
    path = row["local_path"]
    return path if path and os.path.exists(path) else None


def cache_media_bytes(
    ops: SQLiteResource,
    media_url: str,
    *,
    media_dir: Path | None = None,
) -> str | None:
    """Download media bytes and record them in ``media_cache`` (best-effort).

    Returns the local file path, or None if the download failed or the URL was
    already cached. Failure is non-fatal — the worker falls back to the CDN.
    """
    _ensure_media_cache_table(ops)
    existing = cached_local_path(ops, media_url)
    if existing:
        return existing

    downloaded = _download_bytes(media_url)
    if downloaded is None:
        return None
    data, content_type = downloaded
    base_type = content_type.split(";")[0].strip().lower()
    ext = _EXT_BY_MIME.get(base_type, ".bin")

    cache_key = url_hash(media_url)
    dest = (media_dir or POST_MEDIA_DIR) / f"{cache_key}{ext}"
    _atomic_write(dest, data)

    conn = ops.get_connection()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO media_cache "
            "(cache_key, local_path, content_type, size_bytes, fetched_at, source_url) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            [
                cache_key,
                str(dest),
                content_type,
                dest.stat().st_size,
                datetime.now(timezone.utc).isoformat(),
                media_url,
            ],
        )
        conn.commit()
    finally:
        conn.close()

    logger.info("cached %s → %s", media_url[:80], dest.name)
    return str(dest)


def seed_media_from_file(
    ops: SQLiteResource,
    media_url: str,
    src_path: Path,
    *,
    media_dir: Path | None = None,
) -> str | None:
    """Seed ``media_cache`` from an existing local file — no download.

    A SEED, not a fetch: copies already-downloaded bytes into the
    ``POST_MEDIA_DIR`` cache keyed by sha256(url) so the cache is
    self-contained (the source checkout may move or disappear). Use for
    local-disk ingestion where the CDN URLs in the metadata are already
    stale/expiring and re-downloading is both wasteful and lossy.

    Idempotent: returns the cached path when the URL is already cached.
    Returns None when the source file is missing (caller decides severity).
    """
    _ensure_media_cache_table(ops)
    existing = cached_local_path(ops, media_url)
    if existing:
        return existing
    if not src_path.exists():
        logger.warning("seed: source missing for %s: %s", media_url[:80], src_path)
        return None

    cache_key = url_hash(media_url)
    ext = src_path.suffix.lower()
    content_type = _CONTENT_TYPE_BY_EXT.get(ext, "application/octet-stream")
    dest = (media_dir or POST_MEDIA_DIR) / f"{cache_key}{ext}"
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".tmp")
    shutil.copyfile(src_path, tmp)
    os.replace(tmp, dest)

    conn = ops.get_connection()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO media_cache "
            "(cache_key, local_path, content_type, size_bytes, fetched_at, source_url) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            [
                cache_key,
                str(dest),
                content_type,
                dest.stat().st_size,
                datetime.now(timezone.utc).isoformat(),
                media_url,
            ],
        )
        conn.commit()
    finally:
        conn.close()

    logger.info("seeded %s → %s", media_url[:80], dest.name)
    return str(dest)
