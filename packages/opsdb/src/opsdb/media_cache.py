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
import re
import sqlite3
from datetime import UTC, datetime

from .schema import ConnectionFactory, sqlite_ddl


def url_hash(media_url: str) -> str:
    """SHA256 hash of a media URL (not content) — the LEGACY key.

    Precondition: `media_url` is a non-empty string.
    Postcondition: returns a 64-character lowercase hex digest.

    DEPRECATED for new writes: a CDN URL is a *transient* identity. Instagram
    signs it with an ``oe`` expiry of ~4.5 days and re-signs the same media under
    a different URL, so this key changes while the content does not — a cache
    entry keyed here becomes unreachable the moment the signature rotates, and
    cannot be recomputed because the signature is minted server-side.

    Prefer :func:`media_key`, which derives from the stable Instagram media id.
    Retained because existing rows are keyed this way and both keys must resolve
    during the transition.
    """
    return hashlib.sha256(media_url.encode()).hexdigest()


#: Instagram's CDN filenames embed a stable per-file identity:
#: ``/<bucket>/<created>_<MEDIA_ID>_<user>_n.jpg``. The middle group is the media
#: id and it SURVIVES re-signing (verified: a 10-item carousel re-fetched from
#: Apify returned byte-identical ids in the same order), unlike the signature
#: around it. Measured across 600 posts: extractable from every URL, unique per
#: file (no carousel collision), zero collisions corpus-wide.
_MEDIA_ID_RE = re.compile(r"/(\d+)_(\d+)_(\d+)_n\.")


def media_id(media_url: str) -> str | None:
    """The stable Instagram media id embedded in a CDN URL, or None.

    Precondition: `media_url` is a string (may be empty).
    Postcondition: the media id when the URL carries one, else None. Video URLs
    on some hosts omit the ``_n.`` filename form; those return None and fall back
    to :func:`url_hash`.
    """
    match = _MEDIA_ID_RE.search(media_url or "")
    return match.group(2) if match else None


def media_key(media_url: str) -> str:
    """The key for a media URL: the STABLE id when available, else the URL hash.

    Precondition: `media_url` is a non-empty string.
    Postcondition: ``"mid:<media_id>"`` when the URL carries a stable id, else the
    legacy ``sha256(url)``. Both forms are opaque 64-char-ish strings and both
    resolve through the same lookup.

    Why this is the better key: the same media re-signed by Instagram produces the
    same ``media_key`` but a different ``url_hash``. So a cache entry keyed here
    stays reachable across re-signs, and — the point for recovery — a freshly
    fetched URL can be keyed directly, with no need to remember which expired URL
    it once corresponded to.

    The ``mid:`` prefix keeps the two key spaces disjoint in one column (a legacy
    sha256 digest can never start with ``mid:``), so a row cannot be read as the
    wrong kind of key.
    """
    mid = media_id(media_url)
    return f"mid:{mid}" if mid else url_hash(media_url)


def cache_keys_for(media_url: str) -> tuple[str, ...]:
    """Every key a media URL may be stored under, most-preferred first.

    A lookup must try both: rows written before the stable key exist under
    ``url_hash``, and rewriting them is a migration this change does not perform.
    Writes use the first entry.
    """
    stable = media_key(media_url)
    legacy = url_hash(media_url)
    return (stable, legacy) if stable != legacy else (stable,)


def _ensure_media_cache_table(
    ops: ConnectionFactory, conn: sqlite3.Connection | None = None
) -> None:
    """Create ``media_cache`` if it doesn't exist (idempotent).

    Precondition: `ops` exposes ``get_connection()`` returning a row-factory
    connection. `conn`, when given, is an already-open connection to reuse.
    Postcondition: the table exists. A connection the caller supplied is left
    OPEN (the caller owns it); one opened here is closed.

    The DDL is idempotent, but re-issuing it is not free: opening a connection
    costs ~23 ms on Windows, so a batch caller that seeds thousands of URLs
    should pass its own `conn` rather than pay that per row.
    """
    owned = conn is None
    if conn is None:
        conn = ops.get_connection()
    try:
        conn.execute(sqlite_ddl("media_cache"))
        conn.commit()
    finally:
        if owned:
            conn.close()


def record_media_cache_row(
    ops: ConnectionFactory,
    cache_key: str,
    local_path: str,
    content_type: str | None,
    size_bytes: int,
    source_url: str,
    *,
    conn: sqlite3.Connection | None = None,
) -> None:
    """Upsert one ``media_cache`` row.

    Precondition: `cache_key` is `url_hash(source_url)` and `local_path` names
    bytes that have already been written — this function records, it does not
    fetch or copy. `conn`, when given, is an already-open connection to reuse.
    Postcondition: exactly one row exists for `cache_key`, carrying `local_path`,
    `content_type`, `size_bytes`, the URL, and a fresh UTC `fetched_at`. A prior
    row for the same key is replaced. A connection the caller supplied is left
    open; one opened here is closed.

    Ensures the table exists first: every writer of this table must be able to
    write to a fresh ops.sqlite without knowing a separate initialisation step.
    """
    _ensure_media_cache_table(ops, conn)
    owned = conn is None
    if conn is None:
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
                datetime.now(UTC).isoformat(),
                source_url,
            ],
        )
        conn.commit()
    finally:
        if owned:
            conn.close()


def stored_local_path(
    ops: ConnectionFactory,
    media_url: str,
    *,
    conn: sqlite3.Connection | None = None,
) -> str | None:
    """Return the PERSISTED byte path for a URL, with no filesystem check.

    Precondition: none. `conn`, when given, is an already-open connection to
    reuse — a batch caller pays ~23 ms per open otherwise.
    Postcondition: the recorded ``local_path`` when a row exists, else None. The
    absent-table case is a miss, not an error: before the first write there is
    nothing to look up. A supplied connection is left open.

    This is the raw accessor and deliberately does NOT test the path: the path
    was written by whichever process fetched the bytes, so it is expressed in
    THAT filesystem's vocabulary (a Windows-absolute path, today). Whether it is
    openable here is the caller's question, answered by translating it with
    ``platform.paths.runtime_path`` first. Checking existence before translating
    is what made the media cache look empty inside a container.
    """
    owned = conn is None
    if conn is None:
        conn = ops.get_connection()
    try:
        # Try the stable key first, then the legacy URL hash: rows written before
        # the stable key exist under url_hash and must keep resolving.
        for candidate in cache_keys_for(media_url):
            row = conn.execute(
                "SELECT local_path FROM media_cache WHERE cache_key = ?",
                [candidate],
            ).fetchone()
            if row is not None:
                break
        else:
            row = None
    except sqlite3.OperationalError:
        # media_cache table not yet created — nothing can be cached.
        return None
    finally:
        if owned:
            conn.close()
    if not row:
        return None
    path = row["local_path"]
    return path or None
