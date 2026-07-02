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
                media_url_hash TEXT PRIMARY KEY,
                media_url      TEXT NOT NULL,
                file_api_uri   TEXT,
                mime_type      TEXT,
                file_size      INTEGER,
                upload_state   TEXT DEFAULT 'pending',
                created_at     TEXT NOT NULL,
                uploaded_at    TEXT
            )
        """)
    finally:
        conn.close()


def url_hash(media_url: str) -> str:
    """SHA256 hash of a media URL (not content)."""
    return hashlib.sha256(media_url.encode()).hexdigest()


def lookup_or_upload(
    ops: SQLiteResource,
    gemini: GeminiResource,
    media_files_json: str | None,
) -> str | None:
    """Look up cached File API URI for media URLs, or upload to Gemini.

    Args:
        ops: SQLite resource for the media_metadata cache.
        gemini: Gemini resource for File API uploads.
        media_files_json: JSON array of media URLs, or None/empty.

    Returns:
        The first File API URI found/uploaded, or None if no media.
    """
    if not media_files_json:
        return None

    try:
        urls = json.loads(media_files_json)
    except (json.JSONDecodeError, TypeError):
        return None

    if not urls:
        return None

    _ensure_schema(ops)
    first_url = urls[0]
    h = url_hash(first_url)

    conn = ops.get_connection()
    try:
        row = conn.execute(
            "SELECT file_api_uri, upload_state FROM media_metadata WHERE media_url_hash = ?",
            [h],
        ).fetchone()

        if row and row["file_api_uri"]:
            return row["file_api_uri"]

        # Not cached — upload to Gemini File API
        from datetime import datetime, timezone

        from google.genai import Client as GeminiClient
        from google.genai.types import File

        now = datetime.now(timezone.utc).isoformat()

        client = GeminiClient(api_key=gemini.api_key)
        uploaded: File = client.files.upload(file=first_url)  # type: ignore[arg-type]
        uri = uploaded.uri or uploaded.name

        conn.execute(
            """INSERT OR REPLACE INTO media_metadata
               (media_url_hash, media_url, file_api_uri, mime_type,
                file_size, upload_state, created_at, uploaded_at)
               VALUES (?, ?, ?, ?, ?, 'uploaded', ?, ?)""",
            [
                h,
                first_url,
                uri,
                getattr(uploaded, "mime_type", None),
                getattr(uploaded, "size_bytes", None),
                now,
                now,
            ],
        )
        conn.commit()
        return uri
    finally:
        conn.close()
