"""Enrichment architecture — batch-based external API processing.

Operational state lives in ops.sqlite (SQLiteResource).
Analytical results live in gold_analyses (DuckDB).
"""

from .assets import ENRICHMENT_CHECKS, ensure_gold_analyses, gold_analyses
from .batch import create_batch, mark_complete
from .harvest import (
    gemini_batch_harvest,
    gemini_batch_harvest_sensor,
    harvest_gemini_batches,
)
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
    # Harvest (Phase 1, ADR-0007)
    "gemini_batch_harvest",
    "gemini_batch_harvest_sensor",
    "harvest_gemini_batches",
]

