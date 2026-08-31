"""Schedules and sensors for the datalake platform."""

from __future__ import annotations

from dagster import (
    AssetKey,
    DefaultScheduleStatus,
    RunRequest,
    ScheduleDefinition,
    SkipReason,
)

from .resources import SQLiteResource

# Daily medallion processing — materialize silver→gold→serving downstreams.
# Bronze is on-demand (user launches from UI with ScrapeConfig).
daily_medallion = ScheduleDefinition(
    name="daily_medallion",
    target=["ig_posts_slv", "ig_post_labels", "ig_posts_gen_batches", "dim_profile", "dim_date", "v_post_detail"],
    cron_schedule="0 3 * * *",  # 3am daily
    default_status=DefaultScheduleStatus.STOPPED,
    description="Silver dedup + gold enrich + dims + views. Bronze is on-demand.",
)


CORE_REFRESH_CHARGE_CAP_USD = 0.50
"""Per-run Apify charge cap for a single profile refresh."""


def core_refresh_run_requests(ops: SQLiteResource) -> list[RunRequest] | SkipReason:
    """Build one RunRequest per enabled tier1 instagram profile.

    Reads the ``profiles`` control table at schedule-evaluation time so roster
    changes take effect without a code deploy. Returns a SkipReason when the
    roster is empty.
    """
    from ..instagram.creators import enabled_profiles  # deferred: avoids common↔instagram import cycle

    tier1 = [
        p
        for p in enabled_profiles(ops)
        if p["platform"] == "instagram" and p["tier"] == "tier1"
    ]
    if not tier1:
        return SkipReason("No enabled tier1 instagram profiles to refresh.")
    return [
        RunRequest(
            run_key=f"core_refresh:instagram:{p['handle']}",
            asset_selection=[AssetKey("ig_posts_raw")],
            run_config={
                "ops": {
                    "ig_posts_raw": {
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
    return core_refresh_run_requests(SQLiteResource())


# Monthly core refresh — re-scrape every enabled tier1 profile so maturity
# (time-since-post) data accumulates. STOPPED by default: enablement is a
# decision gate (owner + core-set composition + Gemini tier).
core_refresh = ScheduleDefinition(
    name="core_refresh",
    target=["ig_posts_raw"],
    cron_schedule="0 4 2 * *",  # 4am on the 2nd of each month
    default_status=DefaultScheduleStatus.STOPPED,
    description="Monthly per-profile bronze refresh (max_charge_usd capped). STOPPED pending enablement owner.",
    execution_fn=_core_refresh_evaluation,
)
