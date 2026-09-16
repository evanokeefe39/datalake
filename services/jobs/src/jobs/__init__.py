"""The inference service — a standalone async batch executor.

Domain-agnostic: it submits arbitrary (prompt, images) items as a job, runs them
against qwen asynchronously in the background, and lets a client poll + harvest.
It knows nothing about any consumer's schema, queue, or database — it owns only
its own job/item store.

The version lives in `pyproject.toml` and is read at runtime via importlib
metadata, so this module stays a docstring and nothing else (ADR-0015).
"""
