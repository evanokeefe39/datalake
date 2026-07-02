"""Enrichment architecture — queue-based external API processing.

Operational state lives in ops.sqlite (SQLiteResource).
Analytical results live in gold_analyses (DuckDB).
"""

from .assets import ENRICHMENT_CHECKS, ensure_gold_analyses, gold_analyses
from .queue import claim, complete, delete, depth, enqueue, fail, reschedule
from .sensor import enrichment_sensor
from .worker import enrichment_job

__all__ = [
    # Queue operations
    "claim",
    "complete",
    "delete",
    "depth",
    "enqueue",
    "fail",
    "reschedule",
    # Worker
    "enrichment_job",
    # Sensor
    "enrichment_sensor",
    # Assets
    "ensure_gold_analyses",
    "gold_analyses",
    "ENRICHMENT_CHECKS",
]
