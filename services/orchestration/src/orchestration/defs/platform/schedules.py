"""Schedules and sensors for the datalake platform."""

from __future__ import annotations

from dagster import (
    AssetKey,
    DefaultScheduleStatus,
    RunRequest,
    ScheduleDefinition,
    SkipReason,
)
from dagster_duckdb import DuckDBResource

# Daily medallion processing — materialize silver→gold→serving downstreams.
# Bronze is on-demand (user launches from UI with ScrapeConfig).
daily_medallion = ScheduleDefinition(
    name="daily_medallion",
    target=[
        # Roster first: the label pass and dim_profile read the published
        # roster, so it must be fresh before they run.
        "ig_roster_raw",
        "silver_ig_roster",
        "silver_ig_posts",
        "ig_post_labels",
        "dim_profile",
        "dim_date",
        "v_post_detail",
    ],
    cron_schedule="0 3 * * *",  # 3am daily
    default_status=DefaultScheduleStatus.STOPPED,
    description="Roster ingest + silver dedup + labels + dims + views. Bronze is on-demand.",
)


CORE_REFRESH_CHARGE_CAP_USD = 0.50
"""Per-run Apify charge cap for a single profile refresh."""


def core_refresh_run_requests(
    duckdb: DuckDBResource,
) -> list[RunRequest] | SkipReason:
    """Build one RunRequest per enabled tier1 instagram profile.

    Reads the PUBLISHED roster (`silver_ig_roster`) at schedule-evaluation time,
    so a roster change reaches the schedule once it is published — the dashboard
    stays the owner, and the schedule never opens the dashboard's database.
    Returns a SkipReason when the roster is empty.
    """
    from orchestration.defs.ig_core.slv.roster import enabled_profiles

    with duckdb.get_connection() as conn:
        tier1 = [p for p in enabled_profiles(conn) if p["tier"] == "tier1"]
    if not tier1:
        return SkipReason("No enabled tier1 instagram profiles to refresh.")
    return [
        RunRequest(
            run_key=f"core_refresh:instagram:{p['handle']}",
            asset_selection=[AssetKey("bronze_ig_posts")],
            run_config={
                "ops": {
                    "bronze_ig_posts": {
                        "config": {
                            "urls": [p["profile_url"]],
                            "results_limit": p["results_limit"],
                            "results_type": p["results_type"],
                            "max_charge_usd": CORE_REFRESH_CHARGE_CAP_USD,
                        }
                    }
                }
            },
        )
        for p in tier1
    ]


def _core_refresh_evaluation(context) -> list[RunRequest] | SkipReason:
    return core_refresh_run_requests(context.resources.duckdb)


# Monthly core refresh — re-scrape every enabled tier1 profile so maturity
# (time-since-post) data accumulates. STOPPED by default: enablement is a
# decision gate (owner + core-set composition + Gemini tier).
core_refresh = ScheduleDefinition(
    name="core_refresh",
    target=["bronze_ig_posts"],
    cron_schedule="0 4 2 * *",  # 4am on the 2nd of each month
    default_status=DefaultScheduleStatus.STOPPED,
    description=(
        "Monthly per-profile bronze refresh (max_charge_usd capped). "
        "STOPPED pending enablement owner."
    ),
    execution_fn=_core_refresh_evaluation,
)
