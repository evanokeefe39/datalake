"""The ``media_cache`` row contract — URL hash, path lookup, row recording.

The table is ops.sqlite's record of *where* cached media bytes live; the bytes
themselves are written by whichever producer fetched them (the scrape-time byte
cache in `orchestration.defs.engine.media`, or the dashboard's thumbnail
endpoint). This module owns the table's DDL, the hash scheme the key is derived
from, and the single INSERT that both writers share — three copies of that
statement is how a column addition becomes a silent partial write.

Rows are keyed by ``sha256(source_url)``, not by content, because the producers
decide *what* to fetch by URL and must agree on the key before any bytes exist.
"""

from __future__ import annotations

import hashlib
import os
import sqlite3
from datetime import datetime, timezone

from .schema import ConnectionFactory, sqlite_ddl


def url_hash(media_url: str) -> str:
    """SHA256 hash of a media URL (not content).

    Precondition: `media_url` is a non-empty string.
    Postcondition: returns a 64-character lowercase hex digest.
    """
    return hashlib.sha256(media_url.encode()).hexdigest()


def _ensure_media_cache_table(ops: ConnectionFactory) -> None:
    """Create ``media_cache`` if it doesn't exist (idempotent).

    Precondition: `ops` exposes ``get_connection()`` returning a row-factory
    connection.
    Postcondition: the table exists; the connection is closed either way.
    """
    conn = ops.get_connection()
    try:
        conn.execute(sqlite_ddl("media_cache"))
        conn.commit()
    finally:
        conn.close()


def record_media_cache_row(
    ops: ConnectionFactory,
    cache_key: str,
    local_path: str,
    content_type: str | None,
    size_bytes: int,
    source_url: str,
) -> None:
    """Upsert one ``media_cache`` row.

    Precondition: `cache_key` is `url_hash(source_url)` and `local_path` names
    bytes that have already been written — this function records, it does not
    fetch or copy.
    Postcondition: exactly one row exists for `cache_key`, carrying `local_path`,
    `content_type`, `size_bytes`, the URL, and a fresh UTC `fetched_at`. A prior
    row for the same key is replaced.
    """
    conn = ops.get_connection()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO media_cache "
            "(cache_key, local_path, content_type, size_bytes, fetched_at, source_url) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            [
                cache_key,
                local_path,
                content_type,
                size_bytes,
                datetime.now(timezone.utc).isoformat(),
                source_url,
            ],
        )
        conn.commit()
    finally:
        conn.close()


def cached_local_path(ops: ConnectionFactory, media_url: str) -> str | None:
    """Return the local byte path for a URL if recorded and the file exists.

    Precondition: none.
    Postcondition: returns the recorded path when a row exists AND the file is
    present on disk; otherwise None. A row whose file has been deleted is
    treated as a miss, so a caller re-fetches rather than opening a dead path.
    The absent-table case is a miss, not an error: before the first write there
    is nothing to look up.
    """
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
