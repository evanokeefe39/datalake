"""Schedules and sensors for the datalake platform."""

from __future__ import annotations

from dagster import DefaultScheduleStatus, ScheduleDefinition

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
