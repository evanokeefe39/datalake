"""Durable record of posts a paid media re-fetch will never recover.

The standing media-recovery mechanism exists because a scrape-time media
download can fail permanently (the CDN URL is signed and expires in ~4.5 days),
leaving a post un-enrichable with nothing that retries. Recovery re-fetches by
permalink, which is paid.

Some posts can never be recovered: the post was DELETED, so Apify returns an error
item with ``error="not_found"`` instead of media. Re-fetching those costs the same
as a successful one and returns the same nothing. Measured rate: 2 of 232 posts in
a full pass (~1%) — an earlier hand-picked pilot suggested ~33%, which was sample
bias and must not be used to size this table.

WHAT IS *NOT* RECORDED HERE MATTERS AS MUCH AS WHAT IS. Apify also fails with
``restricted_page`` ("Restricted access, only partial data available"), which is
TEMPORARY — a later run can succeed, and one such post recovered mid-session after
being restricted. Recording a temporary failure here permanently excludes a
recoverable post, which is worse than a failed attempt because nothing ever
revisits it. So the reason is a closed set (see ``UNRECOVERABLE_REASONS``) and the
caller must classify the Apify error before recording.

Without a durable verdict the scan is amnesiac: it re-selects and RE-PAYS for a
deleted post on every single run, forever. This module is that memory. A post is
recorded ONCE, keyed by ``post_id``, and the candidate scan excludes it.

The verdict is one-way by design, and that is only safe because the reason set is
narrow. A deleted post does not come back; a restricted one does, so it stays
retryable and stays in the scan.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime

from .schema import ConnectionFactory, sqlite_ddl

#: Reasons a post is permanently unrecoverable through the paid path. A CLOSED
#: set, enforced rather than merely documented: recording a transient failure
#: (a rate limit, a network blip, a `restricted_page` error, a count mismatch on a
#: post that could still pair correctly next time) as exhaustion would condemn a
#: post the mechanism could still fix, which is unrecoverable-by-bug rather than
#: by fact.
#:
#: Named for the CONDITION, not the symptom: Apify returns an error item with
#: `error="not_found"` / `"Post does not exist"`, which is what makes the verdict
#: permanent. An earlier name (`apify_item_carried_no_media_urls`) described the
#: symptom and would have accepted an error item of ANY cause, including ones that
#: resolve on a later run.
UNRECOVERABLE_POST_GONE = "apify_reports_post_does_not_exist"

UNRECOVERABLE_REASONS = frozenset({UNRECOVERABLE_POST_GONE})

#: Apify item `error` values that mean the post is permanently gone. Closed, and
#: deliberately narrow: `restricted_page` ("Restricted access, only partial data
#: available") is NOT here, because restriction is a temporary state and a later
#: run can succeed — measured on real posts that returned it.
UNRECOVERABLE_APIFY_ERRORS = frozenset({"not_found"})


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
