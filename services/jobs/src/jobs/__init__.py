"""qwen-batch-service — a standalone async batch service over qwen via OpenRouter.

Domain-agnostic: it submits arbitrary (prompt, images) items as a job, runs them
against qwen asynchronously in the background, and lets a client poll + harvest.
It knows nothing about any consumer's schema, queue, or database — it owns only
its own job/item store.
"""

__version__ = "0.1.0"
