"""The details-scrape reconciliation sweep (A-D3).

The dashboard used to fire a details scrape from the HTTP request that added a
profile. That is gone: a roster row IS the scrape intent, and this schedule is
what acts on it. The difference is not cosmetic —

* a scrape fired from a request dies with the request (the caller sees a
  background task, the pipeline sees nothing), while a sweep retries;
* a request-triggered scrape cannot be bounded, while a sweep can cap how many
  paid scrapes one tick starts;
* the roster is the owner's declaration of intent, and it is readable at any
  time — so "which profiles need a details refresh?" is a query, not an event.

Reconciliation key: a profile needs a details scrape when its `results_type` is
`details` AND its `updated_at` is newer than the sweep watermark. An idle tick
returns a SkipReason, so the schedule is silent rather than noisy when there is
nothing to do.

**The watermark is load-bearing.** Without it, the first tick after this lands
would see every historical profile as "newer than the epoch" and fire one paid
scrape per row — the roster is ~675 profiles. The watermark advances only after
a tick has emitted its requests, and `DEFAULT_MAX_PROFILES_PER_SWEEP` caps a
single tick so even a genuine bulk roster change cannot fan out unbounded.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from dagster import (
    AssetKey,
    DefaultScheduleStatus,
    RunRequest,
    ScheduleDefinition,
    SkipReason,
)

from orchestration.defs.platform.resources import DuckDBResource

logger = logging.getLogger(__name__)

#: The schedule's watermarks row. One name, advanced per successful sweep.
WATERMARK_NAME = "ig_profile_details_sweep"

#: Cap on paid scrapes started by ONE tick. A roster change that adds fifty
#: profiles must not become fifty simultaneous Apify runs; the remainder is
#: picked up by the following ticks, since the watermark only advances past
#: what was actually emitted.
DEFAULT_MAX_PROFILES_PER_SWEEP = 10

#: Per-profile Apify charge cap (details scrapes are small and single-profile).
DETAILS_CHARGE_CAP_USD = 0.05


def _watermark(conn) -> datetime:
    """The last swept `updated_at`, or the epoch when the sweep never ran."""
    from orchestration.defs.platform.schemas import duckdb_ddl

    conn.execute(duckdb_ddl("watermarks"))
    row = conn.execute(
        "SELECT timestamp FROM watermarks WHERE name = ?", [WATERMARK_NAME]
    ).fetchone()
    if row is None or row[0] is None:
        return datetime(1970, 1, 1, tzinfo=UTC)
    stamp = row[0]
    return stamp.replace(tzinfo=UTC) if stamp.tzinfo is None else stamp


def profiles_needing_details(
    conn, *, max_profiles: int = DEFAULT_MAX_PROFILES_PER_SWEEP
) -> list[dict]:
    """Details-typed profiles whose `updated_at` is past the sweep watermark.

    Ordered by `updated_at` so a capped tick takes the OLDEST unswept rows
    first — a backlog drains in order instead of starving whichever profile
    happens to sort last.
    """
    since = _watermark(conn)
    cursor = conn.execute(
        """
        SELECT platform, handle, profile_url, results_limit, updated_at
        FROM silver_ig_roster
        WHERE enabled = TRUE
          AND results_type = 'details'
          AND updated_at IS NOT NULL
          AND CAST(updated_at AS TIMESTAMP) > ?
        ORDER BY CAST(updated_at AS TIMESTAMP) ASC
        LIMIT ?
        """,
        [since.replace(tzinfo=None), max_profiles],
    )
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, r)) for r in cursor.fetchall()]


def advance_watermark(conn, *, through: str) -> None:
    """Advance the sweep watermark to `through`, never backwards.

    Called by `ig_profile_details_raw` after a SUCCESSFUL scrape — not by the
    schedule. Advancing at evaluation time would move the watermark past runs
    that had not executed yet: an Apify error or a cap would then never be
    retried, and the profile would be silently skipped forever. Recording
    completion is the only point at which "this profile is done" is true.
    """
    current = _watermark(conn)
    if current.tzinfo is not None:
        current = current.replace(tzinfo=None)
    incoming = str(through).replace("T", " ")
    if incoming <= str(current):
        return
    conn.execute(
        "INSERT OR REPLACE INTO watermarks (name, timestamp) VALUES (?, ?)",
        [WATERMARK_NAME, through],
    )


def details_sweep_run_requests(
    duckdb: DuckDBResource,
    *,
    max_profiles: int = DEFAULT_MAX_PROFILES_PER_SWEEP,
) -> list[RunRequest] | SkipReason:
    """One RunRequest per profile due a details scrape, or a SkipReason.

    Reads only — it does NOT advance the watermark. The emitted run advances it
    on success, so a failed scrape stays due and is retried on the next tick.

    The run_key carries the profile's `updated_at`, so the same still-unswept
    profile requested on consecutive ticks deduplicates while a genuine roster
    change produces a new key.
    """
    with duckdb.get_connection() as conn:
        due = profiles_needing_details(conn, max_profiles=max_profiles)

    if not due:
        return SkipReason("No profiles due a details scrape.")

    return [
        RunRequest(
            run_key=f"details:{p['platform']}:{p['handle']}:{p['updated_at']}",
            asset_selection=[AssetKey("ig_profile_details_raw")],
            run_config={
                "ops": {
                    "ig_profile_details_raw": {
                        "config": {
                            "profile_url": p["profile_url"],
                            "results_limit": p["results_limit"] or 1,
                            "max_charge_usd": DETAILS_CHARGE_CAP_USD,
                            "platform": p["platform"],
                            "handle": p["handle"],
                            "roster_updated_at": str(p["updated_at"]),
                        }
                    }
                }
            },
        )
        for p in due
    ]


def _details_sweep_evaluation(context) -> list[RunRequest] | SkipReason:
    return details_sweep_run_requests(context.resources.duckdb)


# Hourly reconciliation. STOPPED by default like every other schedule: the
# owner enables it deliberately, because it spends money.
details_sweep = ScheduleDefinition(
    name="details_sweep",
    target=["ig_profile_details_raw"],
    cron_schedule="0 * * * *",
    default_status=DefaultScheduleStatus.STOPPED,
    description=(
        "Roster-driven details-scrape reconciliation: one run per enabled "
        "details-typed profile whose roster row changed since the last sweep."
    ),
)
