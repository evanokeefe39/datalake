"""The monthly core-scrape refresh, reconciled per profile (US-DISC-7).

`core_refresh` re-scrapes the roster so maturity (time-since-post) data
accumulates. Two things make it more than "scrape every profile":

* **The date boundary is per profile.** Each profile's boundary is its own
  `MAX(timestamp)` in silver — not a flat window. A flat window like
  `onlyPostsNewerThan: "10 days"` re-evaluates against the current clock, so a
  corpus whose newest post is older than the window returns NOTHING while
  reporting success. A per-profile boundary has no such window to fall outside
  of: a profile dormant for a year is a capacity question (how many results to
  ask for), never a silent zero.

* **The fan-out is bounded and named.** A run carries many profiles
  (`directUrls` is an array, and `resultsLimit` applies per URL), so the paid
  unit is a RUN, not a profile. `DEFAULT_MAX_PARALLEL_SCRAPE_RUNS` caps how many
  one tick may start, and `validate_fanout` refuses a fan-out the Apify account
  cannot run rather than letting the platform reject it halfway.

The watermark is derived, not stored: `MAX(timestamp)` per profile is a query
over silver, so there is no second place for "how fresh is this profile?" to
drift out of sync with the data it describes.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
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

#: Sort sentinel for a profile with no posts in silver. It sorts before every
#: real timestamp, so never-scraped profiles lead the queue — they are the
#: profiles whose next scrape is guaranteed to return something.
NEVER_SCRAPED = datetime.min.replace(tzinfo=UTC)

#: Per-profile Apify charge cap. Scaled by the run's URL count (see
#: `plan_refresh_runs`): the original 0.50 was sized for a single-profile run,
#: and a 10-URL run at depth 100 costs about $2.30 — capping it at 0.50 would
#: cut the run short mid-flight.
CORE_REFRESH_CHARGE_CAP_USD = 0.50

#: Cap on paid scrape runs started by ONE tick (US-DISC-7 AC 6). Owner decision
#: 2026-09-17. Bounds blast radius: a roster change that adds fifty profiles
#: must not become fifty simultaneous Apify runs.
DEFAULT_MAX_PARALLEL_SCRAPE_RUNS = 16

#: Profiles per run. Mirrors `DEFAULT_MAX_PROFILES_PER_SWEEP`: enough to
#: amortise run overhead, few enough that one profile's failure is a tenth of a
#: run rather than the whole thing.
MAX_PROFILES_PER_SCRAPE_RUN = 10

#: Apify STARTER account ceilings. Exceeding either makes the platform reject
#: runs rather than queue them, which is why `validate_fanout` fails loudly
#: before anything is launched.
ACCOUNT_MAX_CONCURRENT_RUNS = 32
ACCOUNT_MAX_MEMORY_MB = 65_536  # 64 GB combined across concurrent runs

#: Memory per scrape run, in MB.
#:
#: 256 is the smallest allocation Apify offers, chosen to minimise compute cost
#: (billing is GB-seconds, so memory multiplies wall-clock spend). The actor's
#: own ``defaultRunOptions.memory_mbytes`` is 1024, so this is deliberately
#: BELOW the platform default: if runs start failing on OOM, raise this rather
#: than debugging the actor.
#:
#: **NOT YET MEASURED** — the 1024-vs-4096 GB-seconds comparison (US-DISC-7
#: AC 13) has not been run, and 256 has not been load-tested. Watch the first
#: scheduled cycles for runs failing with a memory error (a FAILED status whose
#: status_message mentions memory) and for bronze files landing with item_count
#: below the requested results_limit. See AGENTS.md § Scrape run memory.
#:
#: Note: Apify does NOT log run options, so the granted memory is not visible in
#: the run log. To confirm what a run actually used:
#:     ApifyClient(token).run(<run_id>).get().options.memory_mbytes
#: A bronze sidecar's ``input.memory_mbytes`` records what we REQUESTED; the run
#: record is the only source for what was GRANTED.
SCRAPE_RUN_MEMORY_MB = 256


@dataclass(frozen=True)
class RefreshRun:
    """One Apify run's worth of work: a batch of profiles sharing an input."""

    run_key: str
    urls: list[str]
    results_limit: int
    results_type: str
    only_posts_newer_than: str | None  # "YYYY-MM-DD"; None = full backfill
    max_charge_usd: float


def refreshable_profiles(conn, *, platform: str = "instagram") -> list[dict]:
    """Enabled tier1 profiles whose `results_limit` is not the ad-hoc sentinel.

    Reads the PUBLISHED roster (`silver_ig_roster`) through the shared reader,
    so the selection cannot drift from every other roster consumer. The
    sentinel (`AD_HOC_LIMIT`, -1) means "this profile was never given a depth",
    deliberately not "scrape me with depth -1".
    """
    from orchestration.defs.ig_core.slv.roster import enabled_profiles

    return [
        p
        for p in enabled_profiles(conn, platform=platform)
        if p["tier"] == "tier1"
    ]


def profile_watermarks(conn) -> dict[str, datetime]:
    """`handle` → the profile's newest post timestamp in silver.

    Handles are normalised exactly as `roster.creator_map` does (lowercased,
    leading `@` stripped) so a lookup cannot miss on formatting alone. A handle
    absent from the result has never been scraped — the caller must treat that
    as "full backfill", never as "nothing to do".

    Timestamps are returned UTC-AWARE. The column is ``TIMESTAMP`` (no zone), so
    DuckDB hands back naive datetimes; attaching UTC here keeps every comparison
    against ``NEVER_SCRAPED`` total, and the boundary a run transmits is a date
    in the same frame.
    """
    cursor = conn.execute(
        """
        SELECT LOWER(LTRIM(owner_username, '@')) AS h, MAX(timestamp) AS mx
        FROM silver_ig_posts
        WHERE owner_username IS NOT NULL
        GROUP BY 1
        """
    )
    return {
        h: (mx.replace(tzinfo=UTC) if mx.tzinfo is None else mx)
        for h, mx in cursor.fetchall()
        if mx is not None
    }


def validate_fanout(
    requested_runs: int, *, memory_mb: int, max_parallel_runs: int
) -> None:
    """Raise if the planned fan-out exceeds what the Apify account can run.

    Called before any RunRequest is built: the account rejects runs past its
    concurrency and combined-memory ceilings rather than queueing them, so a
    fan-out that fits on paper but not in Apify would fail opaquely mid-tick.
    Failing here names the limit that was hit.
    """
    if max_parallel_runs > ACCOUNT_MAX_CONCURRENT_RUNS:
        raise RuntimeError(
            f"max_parallel_runs={max_parallel_runs} exceeds the Apify account "
            f"ceiling of {ACCOUNT_MAX_CONCURRENT_RUNS} concurrent runs"
        )
    total_mb = requested_runs * memory_mb
    if total_mb > ACCOUNT_MAX_MEMORY_MB:
        raise RuntimeError(
            f"{requested_runs} runs x {memory_mb} MB exceeds the account's "
            f"{ACCOUNT_MAX_MEMORY_MB} MB combined-memory ceiling"
        )


def plan_refresh_runs(
    profiles: list[dict],
    watermarks: dict[str, datetime],
    *,
    max_parallel_runs: int = DEFAULT_MAX_PARALLEL_SCRAPE_RUNS,
    max_profiles_per_run: int = MAX_PROFILES_PER_SCRAPE_RUN,
) -> list[RefreshRun]:
    """Group refreshable profiles into bounded, depth-homogeneous runs.

    Four rules, in order, each load-bearing:

    1. **Group by `results_limit` first.** `resultsLimit` is ONE input field
       applied to every URL in the body, so a run cannot carry profiles of
       differing depths. Skipping this grouping does not spread the depths — it
       silently applies one profile's depth to all of them.
    2. **Order each group oldest-boundary-first**, never-scraped profiles
       leading. The stalest profiles are the ones most likely to have new posts,
       so the queue drains in the order that recovers the most data.
    3. **Chunk, then take the chunk's OLDEST DATED member's boundary** as the
       run's `onlyPostsNewerThan`. The input is top-level — one date per run —
       so a chunk's boundary must be the earliest member's, or a newer member's
       date would exclude an older member's posts. Never-scraped members have no
       date to contribute and are excluded from that minimum; a chunk with no
       dated member at all gets None (full backfill).
    4. **Scale the charge cap by URL count**, so the cap means "this run may not
       exceed its fair share" rather than a fixed budget that a deep run busts.
    """
    groups: dict[int, list[dict]] = {}
    for p in profiles:
        groups.setdefault(p["results_limit"], []).append(p)

    def _boundary(profile: dict) -> datetime:
        """Sort key: never-scraped profiles lead, then oldest first."""
        key = profile["handle"].lower().lstrip("@")
        return watermarks.get(key, NEVER_SCRAPED)

    planned: list[RefreshRun] = []
    for results_limit in sorted(groups):
        group = groups[results_limit]
        # Never-scraped profiles are partitioned out FIRST, so fixed-size
        # chunking of the dated remainder cannot sweep them into a dated run.
        # A never-scraped profile needs the full history, and a dated chunk
        # would hand it that chunk's boundary instead.
        unscraped = [p for p in group if _boundary(p) == NEVER_SCRAPED]
        dated = sorted(
            (p for p in group if _boundary(p) != NEVER_SCRAPED), key=_boundary
        )

        for start in range(0, len(unscraped), max_profiles_per_run):
            chunk = unscraped[start : start + max_profiles_per_run]
            planned.append(
                RefreshRun(
                    run_key="",  # keyed below, once the order is final
                    urls=[p["profile_url"] for p in chunk],
                    results_limit=results_limit,
                    results_type=chunk[0]["results_type"],
                    only_posts_newer_than=None,  # full backfill
                    max_charge_usd=CORE_REFRESH_CHARGE_CAP_USD * len(chunk),
                )
            )

        for start in range(0, len(dated), max_profiles_per_run):
            chunk = dated[start : start + max_profiles_per_run]
            # The run's boundary is its oldest member — the input is top-level,
            # so one date per run, and the earliest is the only one that cannot
            # exclude a member's posts.
            boundary = min(_boundary(p) for p in chunk).date().isoformat()
            planned.append(
                RefreshRun(
                    run_key="",  # keyed below, once the order is final
                    urls=[p["profile_url"] for p in chunk],
                    results_limit=results_limit,
                    results_type=chunk[0]["results_type"],
                    only_posts_newer_than=boundary,
                    max_charge_usd=CORE_REFRESH_CHARGE_CAP_USD * len(chunk),
                )
            )

    # Depth ascending, then oldest boundary first — a deterministic total order,
    # so the same data always yields the same keys.
    planned.sort(
        key=lambda r: (r.results_limit, r.only_posts_newer_than or "0000-00-00")
    )
    keyed = [
        RefreshRun(
            run_key=(
                f"core_refresh:instagram:{i:02d}-"
                f"{r.only_posts_newer_than or 'backfill'}"
            ),
            urls=r.urls,
            results_limit=r.results_limit,
            results_type=r.results_type,
            only_posts_newer_than=r.only_posts_newer_than,
            max_charge_usd=r.max_charge_usd,
        )
        for i, r in enumerate(planned)
    ]

    backfill_runs = [r for r in keyed if r.only_posts_newer_than is None]
    logger.info(
        "core_refresh: %d runs for %d profiles (%d full-backfill run(s) covering "
        "%d never-scraped profile(s))",
        len(keyed),
        sum(len(r.urls) for r in keyed),
        len(backfill_runs),
        sum(len(r.urls) for r in backfill_runs),
    )

    if len(keyed) > max_parallel_runs:
        logger.warning(
            "core_refresh: %d runs planned, deferring %d to the next tick: %s",
            len(keyed),
            len(keyed) - max_parallel_runs,
            [r.run_key for r in keyed[max_parallel_runs:]],
        )
        return keyed[:max_parallel_runs]
    return keyed


def core_refresh_run_requests(
    duckdb: DuckDBResource,
    *,
    max_parallel_runs: int = DEFAULT_MAX_PARALLEL_SCRAPE_RUNS,
) -> list[RunRequest] | SkipReason:
    """One RunRequest per planned scrape run, or a SkipReason.

    Reads the published roster and silver's per-profile watermarks on one
    connection, plans the runs, and refuses a fan-out the account cannot run
    before emitting anything.
    """
    with duckdb.get_connection() as conn:
        profiles = refreshable_profiles(conn)
        if not profiles:
            return SkipReason("No enabled tier1 instagram profiles to refresh.")
        watermarks = profile_watermarks(conn)

    runs = plan_refresh_runs(
        profiles, watermarks, max_parallel_runs=max_parallel_runs
    )
    validate_fanout(
        len(runs),
        memory_mb=SCRAPE_RUN_MEMORY_MB,
        max_parallel_runs=max_parallel_runs,
    )

    return [
        RunRequest(
            run_key=run.run_key,
            asset_selection=[AssetKey("bronze_ig_posts")],
            run_config={
                "ops": {
                    "bronze_ig_posts": {
                        "config": {
                            "urls": run.urls,
                            "results_limit": run.results_limit,
                            "results_type": run.results_type,
                            "max_charge_usd": run.max_charge_usd,
                            "only_posts_newer_than": run.only_posts_newer_than,
                            "memory_mbytes": SCRAPE_RUN_MEMORY_MB,
                        }
                    }
                }
            },
        )
        for run in runs
    ]


def _core_refresh_evaluation(context) -> list[RunRequest] | SkipReason:
    return core_refresh_run_requests(context.resources.duckdb)


# Monthly core refresh — re-scrape the refreshable roster so maturity
# (time-since-post) data accumulates. STOPPED by default: enablement is a
# decision gate (owner + core-set composition).
core_refresh = ScheduleDefinition(
    name="core_refresh",
    target=["bronze_ig_posts"],
    cron_schedule="0 4 2 * *",  # 4am on the 2nd of each month
    default_status=DefaultScheduleStatus.STOPPED,
    description=(
        "Monthly roster refresh: depth-homogeneous batches bounded by "
        "DEFAULT_MAX_PARALLEL_SCRAPE_RUNS, each bounded by its own profile "
        "watermark. STOPPED pending enablement owner."
    ),
    execution_fn=_core_refresh_evaluation,
)
