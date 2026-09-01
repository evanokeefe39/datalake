"""Gemini File API media cache — URL hash → File API URI.

Lives in ops.sqlite because it shares the same OLTP access pattern as the
queue: point lookups by hash, state-machine column, frequent updates.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
import sqlite3
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from datalake.defs.common.lake import POST_MEDIA_DIR
from datalake.defs.common.resources import GeminiResource, SQLiteResource
from datalake.defs.common.schemas import sqlite_ddl

logger = logging.getLogger("media_cache")


def _ensure_schema(ops: SQLiteResource) -> None:
    """Create media_metadata table if it doesn't exist (idempotent).

    Called by queue._ensure_schema, but we re-ensure here for safety
    when media_cache is called in isolation (e.g. tests).
    """
    conn = ops.get_connection()
    try:
        conn.execute(sqlite_ddl("media_metadata"))
        # Migrations: add columns that may not exist in pre-existing DBs
        for col in ("expires_at", "video_duration_seconds"):
            try:
                conn.execute(
                    f"ALTER TABLE media_metadata ADD COLUMN {col} TEXT"
                    if col == "expires_at" else
                    f"ALTER TABLE media_metadata ADD COLUMN {col} REAL"
                )
            except Exception:
                pass  # Column already exists
    finally:
        conn.close()


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


_CONTENT_TYPE_BY_EXT = {v: k for k, v in _EXT_BY_MIME.items()}


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


def lookup_or_upload_all(
    ops: SQLiteResource,
    gemini: GeminiResource,
    media_files_json: str | None,
) -> list[dict]:
    """Look up cached File API URIs for media URLs, or download + upload to Gemini.

    Args:
        ops: SQLite resource for the media_metadata cache.
        gemini: Gemini resource for File API uploads.
        media_files_json: JSON array of media URLs, or None/empty.

    Returns:
        List of MediaFile dicts (``{"uri": …, "mime_type": …}``).
        Empty list if no media URLs or all are invalid.
    """
    import tempfile
    import time
    from datetime import timedelta

    from google.genai import Client as GeminiClient
    from google.genai.types import File

    if not media_files_json:
        return []

    try:
        urls = json.loads(media_files_json)
    except (json.JSONDecodeError, TypeError):
        return []

    if not urls:
        return []

    # Deduplicate while preserving order
    seen: set[str] = set()
    unique_urls: list[str] = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            unique_urls.append(u)

    _ensure_schema(ops)
    _ensure_media_cache_table(ops)
    now = datetime.now(timezone.utc)
    expires_at = (now + timedelta(hours=24)).isoformat()
    now_iso = now.isoformat()
    client = GeminiClient(api_key=gemini.api_key)

    result: list[dict] = []
    conn = ops.get_connection()
    try:
        for url in unique_urls:
            h = url_hash(url)

            # Check cache — must have a URI, be uploaded, and not expired
            row = conn.execute(
                """SELECT file_api_uri, mime_type, upload_state, expires_at,
                          video_duration_seconds
                   FROM media_metadata WHERE media_url_hash = ?""",
                [h],
            ).fetchone()

            if row and row["file_api_uri"] and row["upload_state"] == "uploaded":
                if row["expires_at"] and row["expires_at"] > now_iso:
                    result.append({
                        "uri": row["file_api_uri"],
                        "mime_type": row["mime_type"] or "application/octet-stream",
                        "duration_seconds": row["video_duration_seconds"],
                    })
                    continue
                elif row["expires_at"]:
                    logger.info("Cache expired for %s — re-uploading", url[:80])

            # TOCTOU guard: INSERT OR IGNORE placeholder to prevent duplicate uploads
            conn.execute(
                """INSERT OR IGNORE INTO media_metadata
                   (media_url_hash, media_url, upload_state, created_at)
                   VALUES (?, ?, 'uploading', ?)""",
                [h, url, now_iso],
            )
            # If the INSERT was ignored, another process claimed it — check again
            if conn.total_changes == 0:
                row2 = conn.execute(
                    """SELECT file_api_uri, mime_type, upload_state,
                              video_duration_seconds
                       FROM media_metadata WHERE media_url_hash = ?""",
                    [h],
                ).fetchone()
                if row2 and row2["file_api_uri"] and row2["upload_state"] == "uploaded":
                    result.append({
                        "uri": row2["file_api_uri"],
                        "mime_type": row2["mime_type"] or "application/octet-stream",
                        "duration_seconds": row2["video_duration_seconds"],
                    })
                    continue

            logger.info("Uploading %s to Gemini File API...", url[:80])

            # Resolve bytes: prefer the scrape-time byte cache (CDN URLs die in
            # ~4-5 days); fall back to a live download only on a cache miss.
            tmp_path = None
            try:
                cached = conn.execute(
                    "SELECT local_path FROM media_cache WHERE cache_key = ?", [h]
                ).fetchone()
                if cached and cached["local_path"] and os.path.exists(cached["local_path"]):
                    upload_path = cached["local_path"]
                else:
                    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".media")
                    os.close(tmp_fd)
                    urllib.request.urlretrieve(url, tmp_path)
                    upload_path = tmp_path

                uploaded: File = client.files.upload(file=upload_path)

                # Poll until ACTIVE or timeout
                deadline = time.monotonic() + 30
                while time.monotonic() < deadline:
                    uploaded = client.files.get(name=uploaded.name)
                    if uploaded.state.name == "ACTIVE":
                        break
                    if uploaded.state.name == "FAILED":
                        raise RuntimeError(
                            f"Gemini File API upload failed for {url[:80]}: "
                            f"state={uploaded.state.name}"
                        )
                    time.sleep(2)
                else:
                    raise TimeoutError(
                        f"Gemini File API upload timed out waiting for ACTIVE state "
                        f"for {url[:80]}"
                    )

                uri = uploaded.uri
                if not uri:
                    raise ValueError(
                        f"Gemini File API returned no URI for {url[:80]} "
                        f"(name={uploaded.name}). Do not use uploaded.name as URI."
                    )

                mime_type = getattr(uploaded, "mime_type", None) or "application/octet-stream"

                # Extract video duration for token budget estimation
                duration = None
                try:
                    vm = getattr(uploaded, "video_metadata", None)
                    if vm is not None:
                        duration = getattr(vm, "duration_seconds", None)
                except Exception:
                    pass

                conn.execute(
                    """UPDATE media_metadata SET
                       file_api_uri = ?, mime_type = ?, file_size = ?,
                       video_duration_seconds = ?,
                       upload_state = 'uploaded', expires_at = ?,
                       uploaded_at = ?
                       WHERE media_url_hash = ?""",
                    [
                        uri,
                        mime_type,
                        getattr(uploaded, "size_bytes", None),
                        duration,
                        expires_at,
                        now_iso,
                        h,
                    ],
                )
                conn.commit()

                result.append({"uri": uri, "mime_type": mime_type, "duration_seconds": duration})
                logger.info("Upload complete: %s → %s", url[:80], uri[:80])

            finally:
                if tmp_path and os.path.exists(tmp_path):
                    os.unlink(tmp_path)

        return result
    finally:
        conn.close()
