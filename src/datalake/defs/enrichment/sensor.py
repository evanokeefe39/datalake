"""Enrichment sensor — polls the queue and launches worker runs."""

from __future__ import annotations

from dagster import RunRequest, SkipReason, sensor

from datalake.defs.common.resources import SQLiteResource
from datalake.defs.enrichment.queue import claim


@sensor(
    job_name="enrichment_job",
    minimum_interval_seconds=30,
    description="Polls enrichment_queue for pending items, claims up to 5 per tick.",
)
def enrichment_sensor(context) -> RunRequest | SkipReason:
    """Claim pending items and emit a RunRequest with post_ids + domains."""
    ops: SQLiteResource = context.resources.ops

    rows = claim(ops, limit=5)
    if not rows:
        return SkipReason("no pending items in enrichment queue")

    post_ids = [r["post_id"] for r in rows]
    domains = [r["domain"] for r in rows]

    batch_key = "|".join(post_ids)

    return RunRequest(
        run_key=batch_key,
        run_config={
            "post_ids": post_ids,
            "domains": domains,
        },
    )
