"""Prompt/version registry — resolves prompt_hash → (prompt, model, recorded_at).

The one genuinely-new artifact from the ADR-0001 architecture review. The
registry makes every gold row self-describing: given the ``prompt_hash``
recorded on a ``gold_analyses``/``batch_jobs`` row, the exact prompt text and
model that produced it are recoverable.
"""

from __future__ import annotations

from datalake.defs.common.resources import SQLiteResource
from datalake.defs.enrichment.batch import _ensure_schema, _now_iso
from datalake.defs.enrichment.prompts import (
    _DEFAULT_GEMINI_MODEL,
    CURRENT_PROMPT_HASH,
    IG_GOLD_PROMPT,
    compute_prompt_hash,
)


def register_prompt(
    ops: SQLiteResource,
    prompt: str,
    model: str,
    recorded_at: str,
) -> str:
    """Upsert a prompt into the registry (idempotent on prompt_hash).

    Returns the prompt_hash.
    """
    prompt_hash = compute_prompt_hash(prompt, model)
    conn = ops.get_connection()
    try:
        conn.execute(
            "INSERT OR IGNORE INTO prompt_registry "
            "(prompt_hash, prompt, model, recorded_at) VALUES (?, ?, ?, ?)",
            [prompt_hash, prompt, model, recorded_at],
        )
        conn.commit()
    finally:
        conn.close()
    return prompt_hash


def register_current_prompt(ops: SQLiteResource) -> str:
    """Register the current prompt + default model (idempotent)."""
    _ensure_schema(ops)
    return register_prompt(ops, IG_GOLD_PROMPT, _DEFAULT_GEMINI_MODEL, _now_iso())


def resolve_prompt(ops: SQLiteResource, prompt_hash: str) -> dict | None:
    """Resolve a prompt_hash to its (prompt, model, recorded_at) definition."""
    conn = ops.get_connection()
    try:
        row = conn.execute(
            "SELECT prompt, model, recorded_at FROM prompt_registry "
            "WHERE prompt_hash = ?",
            [prompt_hash],
        ).fetchone()
        if not row:
            return None
        return {"prompt": row[0], "model": row[1], "recorded_at": row[2]}
    finally:
        conn.close()


def is_current_prompt_registered(ops: SQLiteResource) -> bool:
    """True if CURRENT_PROMPT_HASH resolves in the registry."""
    return resolve_prompt(ops, CURRENT_PROMPT_HASH) is not None
