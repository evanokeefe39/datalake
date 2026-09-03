"""Enrichment architecture — batch-based external API processing.

Operational state lives in ops.sqlite (SQLiteResource).
Analytical results live in gold_analyses (DuckDB).
"""

from .assets import ENRICHMENT_CHECKS, ensure_gold_analyses, gold_analyses
from .batch import create_batch, mark_complete
from .registry import register_current_prompt, resolve_prompt

__all__ = [
    # Batch operations
    "create_batch",
    "mark_complete",
    # Prompt/version registry
    "register_current_prompt",
    "resolve_prompt",
    # Assets
    "ensure_gold_analyses",
    "gold_analyses",
    "ENRICHMENT_CHECKS",
]
