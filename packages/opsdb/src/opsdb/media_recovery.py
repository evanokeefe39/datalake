"""Durable record of posts a paid media re-fetch will never recover.

The standing media-recovery mechanism exists because a scrape-time media
download can fail permanently (the CDN URL is signed and expires in ~4.5 days),
leaving a post un-enrichable with nothing that retries. Recovery re-fetches by
permalink, which is paid.

Some posts are unrecoverable in a way that never changes: the post was deleted or
made private, so Instagram (via Apify) returns an item carrying no media at all.
Re-fetching those costs the same as a successful one and returns the same
nothing — measured at roughly a third of candidates in the pilot.

Without a durable verdict the scan is amnesiac: it re-selects and RE-PAYS for
those posts on every single run, forever. This module is that memory. A post is
recorded ONCE, keyed by ``post_id``, and the candidate scan excludes it.

The verdict is deliberately one-way. A re-appearance of the media is not
detectable from here (the post is gone), and a post that reappears would be
re-scraped by the core-refresh path — which lands new silver rows with their own
media URLs and a fresh opportunity to cache. Recording exhaustion never blocks
that path, because it only suppresses the PAID re-fetch.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime

from .schema import ConnectionFactory, sqlite_ddl

#: Reasons a post is permanently unrecoverable through the paid path. A CLOSED
#: set, enforced rather than merely documented: recording a transient failure
#: (a rate limit, a network blip, a count mismatch on a post that could still
#: pair correctly next time) as exhaustion would condemn a post the mechanism
#: could still fix, which is unrecoverable-by-bug rather than by fact.
UNRECOVERABLE_NO_MEDIA = "apify_item_carried_no_media_urls"

UNRECOVERABLE_REASONS = frozenset({UNRECOVERABLE_NO_MEDIA})


def _ensure_table(ops: ConnectionFactory, conn: sqlite3.Connection | None = None) -> None:
    """Create ``media_recovery_exhausted`` if absent (idempotent).

    A caller-supplied connection is left OPEN (the caller owns it); one opened
    here is closed.
    """
    owned = conn is None
    if conn is None:
        conn = ops.get_connection()
    try:
        conn.execute(sqlite_ddl("media_recovery_exhausted"))
    finally:
        if owned:
            conn.close()


def record_exhausted(
    ops: ConnectionFactory,
    post_id: str,
    reason: str,
    *,
    conn: sqlite3.Connection | None = None,
) -> None:
    """Record that ``post_id`` cannot be recovered by a paid re-fetch.

    Precondition: `post_id` is non-empty; `reason` is one of the module's
    ``UNRECOVERABLE_*`` constants. An unknown reason RAISES rather than being
    stored, because the verdict is one-way: a transient failure recorded here
    would permanently condemn a post the mechanism could still recover.
    Postcondition: the row exists. Idempotent — re-recording updates the reason
    and timestamp rather than raising, so a re-run is safe.
    """
    if reason not in UNRECOVERABLE_REASONS:
        raise ValueError(
            f"unknown exhaustion reason {reason!r}; expected one of "
            f"{sorted(UNRECOVERABLE_REASONS)}. A transient failure must stay "
            f"retryable, not be recorded as a permanent verdict."
        )
    _ensure_table(ops, conn)
    owned = conn is None
    if conn is None:
        conn = ops.get_connection()
    try:
        conn.execute(
            """INSERT INTO media_recovery_exhausted (post_id, reason, recorded_at)
               VALUES (?, ?, ?)
               ON CONFLICT(post_id) DO UPDATE SET
                   reason = excluded.reason,
                   recorded_at = excluded.recorded_at""",
            [post_id, reason, datetime.now(UTC).isoformat()],
        )
        conn.commit()
    finally:
        if owned:
            conn.close()


def exhausted_post_ids(
    ops: ConnectionFactory, *, conn: sqlite3.Connection | None = None
) -> set[str]:
    """Every post_id recorded as permanently unrecoverable.

    Returns an empty set when the table does not exist yet, so a scan on a fresh
    database behaves as it did before this table was introduced.
    """
    owned = conn is None
    if conn is None:
        conn = ops.get_connection()
    try:
        rows = conn.execute("SELECT post_id FROM media_recovery_exhausted").fetchall()
    except sqlite3.OperationalError:
        # Table not created yet — nothing has been recorded.
        return set()
    finally:
        if owned:
            conn.close()
    return {r[0] for r in rows}
