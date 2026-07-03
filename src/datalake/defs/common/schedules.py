"""Schedules and sensors for the datalake platform."""

from dagster import DefaultScheduleStatus, ScheduleDefinition

# Daily medallion processing — materialize silver→gold→serving downstreams.
# Bronze is on-demand (user launches from UI with ScrapeConfig).
daily_medallion = ScheduleDefinition(
    name="daily_medallion",
    target=["ig_posts_slv", "ig_posts_gld_batches", "dim_profile", "dim_date", "v_post_detail"],
    cron_schedule="0 3 * * *",  # 3am daily
    default_status=DefaultScheduleStatus.STOPPED,
    description="Silver dedup + gold enrich + dims + views. Bronze is on-demand.",
)
