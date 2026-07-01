"""Schedules and sensors for the datalake platform."""

from dagster import DefaultScheduleStatus, ScheduleDefinition

# Weekly medallion processing — materialize silver→gold→serving downstreams.
# Bronze is on-demand (user launches from UI with ScrapeConfig).
weekly_medallion = ScheduleDefinition(
    name="weekly_medallion",
    target=["ig_posts_slv", "ig_posts_gld", "dim_profile", "analytics_views"],
    cron_schedule="0 2 * * 1",  # 2am Monday
    default_status=DefaultScheduleStatus.STOPPED,
    description="Silver dedup + gold enrich + dims + views. Bronze is on-demand.",
)
