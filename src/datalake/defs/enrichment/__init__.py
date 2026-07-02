"""Enrichment architecture — batch-based external API processing.

Operational state lives in ops.sqlite (SQLiteResource).
Analytical results live in gold_analyses (DuckDB).
"""

from .assets import ENRICHMENT_CHECKS, ensure_gold_analyses, gold_analyses
from .batch import create_batch, mark_complete

__all__ = [
    # Batch operations
    "create_batch",
    "mark_complete",
    # Assets
    "ensure_gold_analyses",
    "gold_analyses",
    "ENRICHMENT_CHECKS",
]
