"""Gemini File API media cache — URL hash → File API URI.

Lives in ops.sqlite because it shares the same OLTP access pattern as the
queue: point lookups by hash, state-machine column, frequent updates.
"""

from __future__ import annotations

import hashlib
import json

from datalake.defs.common.resources import GeminiResource, SQLiteResource


def _ensure_schema(ops: SQLiteResource) -> None:
    """Create media_metadata table if it doesn't exist (idempotent).

    Called by queue._ensure_schema, but we re-ensure here for safety
    when media_cache is called in isolation (e.g. tests).
    """
    conn = ops.get_connection()
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS media_metadata (
                media_url_hash         TEXT PRIMARY KEY,
                media_url              TEXT NOT NULL,
                file_api_uri           TEXT,
                mime_type              TEXT,
                file_size              INTEGER,
                video_duration_seconds REAL,
                upload_state           TEXT DEFAULT 'pending',
                expires_at             TEXT,
                created_at             TEXT NOT NULL,
                uploaded_at            TEXT
            )
        """)
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
    import logging
    import os as _os
    import tempfile
    import time
    import urllib.request
    from datetime import datetime, timedelta, timezone

    from google.genai import Client as GeminiClient
    from google.genai.types import File

    logger = logging.getLogger("media_cache")

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

            # Download URL to temp file (SDK requires local path, not URL)
            tmp_path = None
            try:
                tmp_fd, tmp_path = tempfile.mkstemp(suffix=".media")
                _os.close(tmp_fd)
                urllib.request.urlretrieve(url, tmp_path)

                uploaded: File = client.files.upload(file=tmp_path)

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
                if tmp_path and _os.path.exists(tmp_path):
                    _os.unlink(tmp_path)

        return result
    finally:
        conn.close()
