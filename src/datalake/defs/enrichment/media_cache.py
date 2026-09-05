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
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from datalake.defs.common.lake import POST_MEDIA_DIR
from datalake.defs.common.resources import GeminiResource, SQLiteResource
from datalake.defs.common.schemas import sqlite_ddl


# ── Gemini File API retention & reuse window ────────────────────────────────
#
# VERIFIED against https://ai.google.dev/gemini-api/docs/files: "Files are
# automatically deleted after 48 hours." The previous 24h reuse window made
# multi-day drains re-upload everything each new day even though the served
# URIs were still live. We reuse until the real retention minus a conservative
# margin so a served URI is never already deleted server-side; and if a served
# URI is dead anyway (404 on files.get), the caller gets a transparent
# re-upload instead of a failed request.
GEMINI_FILE_RETENTION_HOURS = 48
FILE_RETENTION_MARGIN_HOURS = 4
CACHE_REUSE_WINDOW_HOURS = GEMINI_FILE_RETENTION_HOURS - FILE_RETENTION_MARGIN_HOURS  # 44h

# Inline-media cap for batch requests. The GenerateContent request body must
# stay under the Gemini request-size limit (~20MB with inline bytes); we cap
# per-file well below it. Images only — video always goes through the File API.

# Aggregate cap on inline bytes PER REQUEST (one post = one batch request):
# the GenerateContent body must stay under the ~20MB request limit, so even
# individually-small images get downgraded to the File API once the running
# total would exceed this budget.
INLINE_TOTAL_BUDGET_BYTES = 18 * 1024 * 1024
INLINE_MEDIA_LIMIT_BYTES = 15 * 1024 * 1024

# Modest File API upload concurrency: the upload + ACTIVE-state poll phase is
# I/O-bound, and this stays well under per-project File API request rate.
UPLOAD_WORKERS = 6

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


def _download_to_file(url: str, dest: str) -> str:
    """Download ``url`` to ``dest``; return the served Content-Type.

    Raises on network failure (the caller classifies it as a File API error),
    so a mis-cached/live URL never silently uploads the wrong bytes.
    """
    req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            content_type = resp.headers.get("Content-Type", "")
            dest_path = Path(dest)
            _atomic_write(dest_path, resp.read())
            return content_type
    except Exception as exc:
        logger.warning("media download failed for %s: %s", url[:80], exc)
        raise


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


def _normalize_mime(content_type: str | None) -> str | None:
    """Normalize a Content-Type header: strip parameters, lowercase."""
    if not content_type:
        return None
    mime = content_type.split(";")[0].strip().lower()
    return mime or None


def _mime_from_url(url: str) -> str | None:
    """Extension-based mime fallback from the URL path (no query/fragment)."""
    ext = os.path.splitext(urllib.parse.urlparse(url).path)[1].lower()
    return _CONTENT_TYPE_BY_EXT.get(ext)


def _resolve_mime_type(content_type: str | None, url: str) -> str | None:
    """Best-effort mime for an upload: header first, then URL extension.

    A generic application/octet-stream header is treated as "no useful mime
    signal" so the URL-extension fallback (or Gemini's own sniffer) can still
    classify the bytes — passing octet-stream explicitly would reproduce the
    "Unknown mime type" File API rejection.
    """
    mime = _normalize_mime(content_type)
    if mime and mime != "application/octet-stream":
        return mime
    return _mime_from_url(url)


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


def _file_name_from_uri(uri: str) -> str | None:
    """Extract the File API resource name (``files/<id>``) from a served URI."""
    idx = uri.rfind("/files/")
    return uri[idx + 1 :] if idx >= 0 else None


def _uri_is_alive(client, uri: str) -> bool:
    """Check a cached File API URI is still servable (C fallback).

    A 404 / NOT_FOUND from ``files.get`` means the server has already deleted
    the file — the caller must re-upload. Any other error (transient network,
    quota) is conservatively treated as alive: serving a possibly-fine cached
    URI is better than re-uploading the world on every hiccup.
    """
    name = _file_name_from_uri(uri)
    if not name:
        return True  # not a recognizable File API URI — don't second-guess it
    try:
        f = client.files.get(name=name)
    except Exception as exc:
        code = getattr(exc, "code", None)
        text = str(exc).upper()
        if code == 404 or "NOT_FOUND" in text or "NOT FOUND" in text:
            logger.info("Cached File API URI is dead (404): %s", uri[:80])
            return False
        logger.warning("URI liveness probe failed (treating as alive): %s: %s", uri[:80], exc)
        return True
    state = getattr(f, "state", None)
    return state is None or getattr(state, "name", None) == "ACTIVE"


def _try_inline_payload(conn, url: str, h: str) -> dict | None:
    """Return an inline_data part payload for a small image, or None.

    Batch-path optimization (B): image media under the inline limit are served
    as raw bytes (``{"inline_data": …, "mime_type": …}``) so no File API upload
    round-trip happens at all. Video, unknown mime, and oversize files return
    None and fall through to the File API upload path.

    Raises for that file when the bytes cannot be obtained (cache miss + CDN
    failure) — mirroring the upload path's failure semantics.
    """
    cached = conn.execute(
        "SELECT local_path, content_type FROM media_cache WHERE cache_key = ?",
        [h],
    ).fetchone()
    mime: str | None = None
    path: str | None = None
    if cached and cached["local_path"] and os.path.exists(cached["local_path"]):
        mime = _resolve_mime_type(cached["content_type"], url)
        path = cached["local_path"]
    else:
        mime = _mime_from_url(url)

    if not mime or not mime.startswith("image/"):
        return None

    if path:
        with open(path, "rb") as fh:
            data = fh.read()
    else:
        got = _download_bytes(url)
        if got is None:
            raise RuntimeError(f"CDN download failed for {url[:80]} (inline path)")
        data, served_ct = got
        mime = _normalize_mime(served_ct) or mime

    if len(data) > INLINE_MEDIA_LIMIT_BYTES:
        logger.info("Image too large for inline (%d bytes): %s", len(data), url[:80])
        return None
    return {"inline_data": data, "mime_type": mime, "duration_seconds": None}


def _upload_one(
    ops: SQLiteResource,
    client,
    url: str,
    expires_at: str,
    now_iso: str,
) -> dict:
    """Upload one media URL to the File API and record it (thread worker for A).

    Opens its OWN sqlite connection — media_metadata/media_cache writes are
    serialized by SQLite's write lock (WAL + busy_timeout), so concurrent
    workers can never corrupt the DB. Raises on failure: the caller surfaces
    the error for that file without losing the other uploads' results.
    """
    import tempfile
    import time

    from google.genai.types import File

    h = url_hash(url)
    conn = ops.get_connection()
    tmp_path: str | None = None
    try:
        logger.info("Uploading %s to Gemini File API...", url[:80])

        # Resolve bytes: prefer the scrape-time byte cache (CDN URLs die in
        # ~4-5 days); fall back to a live download only on a cache miss.
        cached = conn.execute(
            "SELECT local_path, content_type FROM media_cache WHERE cache_key = ?",
            [h],
        ).fetchone()
        if cached and cached["local_path"] and os.path.exists(cached["local_path"]):
            upload_path = cached["local_path"]
            # Scrape-time cache recorded the served Content-Type.
            upload_mime = _resolve_mime_type(cached["content_type"], url)
        else:
            tmp_fd, tmp_path = tempfile.mkstemp(suffix=".media")
            os.close(tmp_fd)
            served_ct = _download_to_file(url, tmp_path)
            upload_path = tmp_path
            upload_mime = _resolve_mime_type(served_ct, url)
        if not upload_mime:
            logger.warning(
                "No mime_type resolvable for %s — letting the API sniff", url[:80]
            )

        # Files.upload takes mime_type via config, not a top-level kwarg.
        upload_config = {"mime_type": upload_mime} if upload_mime else None

        # Minimal 429 awareness for concurrent uploads: one bounded retry.
        deadline429 = time.monotonic() + 30
        while True:
            try:
                uploaded: File = client.files.upload(file=upload_path, config=upload_config)
                break
            except Exception as exc:
                if getattr(exc, "code", None) == 429 and time.monotonic() < deadline429:
                    logger.warning("File API 429 for %s — backing off 5s", url[:80])
                    time.sleep(5)
                    continue
                raise

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

        logger.info("Upload complete: %s → %s", url[:80], uri[:80])
        return {"uri": uri, "mime_type": mime_type, "duration_seconds": duration}
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)
        conn.close()


def lookup_or_upload_all(
    ops: SQLiteResource,
    gemini: GeminiResource,
    media_files_json: str | None,
    inline_images: bool = False,
) -> list[dict]:
    """Look up cached File API URIs for media URLs, or download + upload to Gemini.

    Args:
        ops: SQLite resource for the media_metadata cache.
        gemini: Gemini resource for File API uploads.
        media_files_json: JSON array of media URLs, or None/empty.
        inline_images: batch-path option (B). When True, small images are
            served as ``{"inline_data": bytes, ...}`` payloads with NO File API
            upload; video/oversize still go through the File API (concurrently).

    Returns:
        List of media dicts in INPUT URL ORDER — ``{"uri", "mime_type",
        "duration_seconds"}`` for File API media, or ``{"inline_data",
        "mime_type", "duration_seconds"}`` for inline images.
        Empty list if no media URLs or all are invalid.

    Concurrency (A): cache lookups + TOCTOU claims run on the caller's thread;
    the upload + ACTIVE-poll phase runs on a small ThreadPoolExecutor with
    per-thread SQLite connections. One failing file raises for that file —
    after all other uploads have completed and been persisted — so a bad
    upload never wastes the batch's other work. Existing 429 handling was
    absent; a bounded single-retry backoff covers concurrent-upload bursts.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from datetime import timedelta

    from google.genai import Client as GeminiClient

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
    expires_at = (now + timedelta(hours=CACHE_REUSE_WINDOW_HOURS)).isoformat()
    now_iso = now.isoformat()
    client = GeminiClient(api_key=gemini.api_key)

    results: dict[str, dict] = {}
    errors: dict[str, Exception] = {}
    pending: list[str] = []
    conn = ops.get_connection()
    try:
        inline_bytes_used = 0
        for url in unique_urls:
            h = url_hash(url)

            # Check cache — must have a URI, be uploaded, not expired, and
            # still be alive server-side (dead URIs transparently re-upload).
            row = conn.execute(
                """SELECT file_api_uri, mime_type, upload_state, expires_at,
                          video_duration_seconds
                   FROM media_metadata WHERE media_url_hash = ?""",
                [h],
            ).fetchone()
            force = False
            if row and row["file_api_uri"] and row["upload_state"] == "uploaded":
                if row["expires_at"] and row["expires_at"] > now_iso:
                    if _uri_is_alive(client, row["file_api_uri"]):
                        results[url] = {
                            "uri": row["file_api_uri"],
                            "mime_type": row["mime_type"] or "application/octet-stream",
                            "duration_seconds": row["video_duration_seconds"],
                        }
                        continue
                    # Dead server-side: re-upload no matter what the row says.
                    force = True
                    logger.info("Dead cached URI for %s — re-uploading", url[:80])
                elif row["expires_at"]:
                    logger.info("Cache expired for %s — re-uploading", url[:80])

            # Batch inline path (B): small images become bytes, no upload.
            # A running budget caps the per-request aggregate inline size.
            if inline_images:
                try:
                    payload = _try_inline_payload(conn, url, h)
                except Exception as exc:
                    errors[url] = exc
                    continue
                if payload is not None:
                    size = len(payload["inline_data"])
                    if inline_bytes_used + size <= INLINE_TOTAL_BUDGET_BYTES:
                        inline_bytes_used += size
                        results[url] = payload
                        continue
                    logger.info(
                        "Inline budget exhausted (%d bytes) — File API for %s",
                        inline_bytes_used, url[:80],
                    )

            # TOCTOU guard: INSERT OR IGNORE placeholder to prevent duplicate
            # uploads across processes (single-writer DB).
            cur = conn.execute(
                """INSERT OR IGNORE INTO media_metadata
                   (media_url_hash, media_url, upload_state, created_at)
                   VALUES (?, ?, 'uploading', ?)""",
                [h, url, now_iso],
            )
            if cur.rowcount == 0 and not force:
                # Another writer claimed it — check whether it already finished.
                row2 = conn.execute(
                    """SELECT file_api_uri, mime_type, upload_state, expires_at,
                              video_duration_seconds
                       FROM media_metadata WHERE media_url_hash = ?""",
                    [h],
                ).fetchone()
                # Same validity bar as the primary cache check: uploaded,
                # unexpired, and still alive server-side — never serve a
                # stale/expired URI another writer recorded earlier.
                if (
                    row2
                    and row2["file_api_uri"]
                    and row2["upload_state"] == "uploaded"
                    and row2["expires_at"]
                    and row2["expires_at"] > now_iso
                    and _uri_is_alive(client, row2["file_api_uri"])
                ):
                    results[url] = {
                        "uri": row2["file_api_uri"],
                        "mime_type": row2["mime_type"] or "application/octet-stream",
                        "duration_seconds": row2["video_duration_seconds"],
                    }
                    continue

            pending.append(url)
            if force:
                logger.info("Forcing re-upload of %s (dead cached URI)", url[:80])
            # Release the write lock before worker threads open their own
            # connections — an uncommitted INSERT would block them.
            conn.commit()

        # Concurrent upload + ACTIVE-poll phase (A).
        if pending:
            with ThreadPoolExecutor(max_workers=UPLOAD_WORKERS) as pool:
                futures = {
                    pool.submit(_upload_one, ops, client, u, expires_at, now_iso): u
                    for u in pending
                }
                for fut in as_completed(futures):
                    u = futures[fut]
                    try:
                        results[u] = fut.result()
                    except Exception as exc:
                        # Per-file failure isolation: remember, keep collecting.
                        errors[u] = exc
                        logger.error("Media upload failed for %s: %s", u[:80], exc)
    finally:
        conn.close()

    if errors:
        # Raise for the FIRST failing file in input order — matches the
        # sequential semantics callers were built on; all successful uploads
        # above were already persisted and will be cache hits next time.
        first = next(u for u in unique_urls if u in errors)
        raise errors[first]

    return [results[u] for u in unique_urls]
