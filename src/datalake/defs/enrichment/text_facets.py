"""Text-layer →Gemini call — media-free text/cross-modal facets (US-EFAC-4).

The cheap companion to the universal video call (``facets.py``): one
TEXT-ONLY flash-lite call per post over the caption (+ transcript when a
transcript channel exists) extracting the text-layer facet subset
(hook_*, sponsorship, claims, cta_type, audience_named, value_depth,
replicable_tactic, hashtag_strategy, evidence) plus the brand_safety
6-flag object, validated by ``validate_text_facets``. Transcript is
optional — the call degrades gracefully to caption-only (US-EFAC-4 AC4).
This pass must NOT extract the visual-core facets or summaries (US-EFAC-3
owns those; the sub-validator rejects them).

Cost accounting: per-response usage_metadata + Tier-1 flash-lite list-price
ESTIMATES (configurable via env); reported per item and per run.

## Merge / validation contract (per-pass partial storage)

The locked full V3 schema requires visual AND text fields together, but
each pass alone is partial. Contract:

- Each pass stores ONLY its own validated sub-fields, merged over whatever
  ``growth_facets_json`` already holds for the row (union of passes). A row
  after both passes satisfies ``validate_growth_facets``; intermediate rows
  are intentionally partial and are NOT validated against the full schema —
  assembly/completeness is a later concern (``validate_growth_facets``
  errors at serving/assembly time are the completeness signal).
- The merged JSON never contains reserved gold keys, ``content_summary`` or
  ``image_summaries`` (each pass's sub-validator rejects those).
- ``gold_growth_facets.prompt_hash`` reflects the LAST pass that wrote the
  row (single column); each driver resumes off its own hash. A text write
  after a visual write therefore makes the row "not yet done" from the
  visual driver's perspective — acceptable, since re-runs of a pass only
  re-pay that cheap pass, never the media.
- Upsert ordering guard identical to ``write_gold_facets_conn`` so a stale
  concurrent write never clobbers a newer one.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any

from datalake.defs.enrichment.growth_facets_schema import (
    GROWTH_FACETS_SCHEMA_VERSION,
    validate_text_facets,
)
from datalake.defs.enrichment.prompts import (
    _DEFAULT_GEMINI_MODEL,
    CURRENT_TEXT_FACETS_PROMPT_HASH,
    build_text_facets_prompt,
)

logger = logging.getLogger("enrichment.text_facets")

# Text-only output budget: facets + brand_safety fit comfortably in 2048.
TEXT_MAX_OUTPUT_TOKENS = 2048

# Tier-1 flash-lite list-price ESTIMATES, USD per 1M tokens (env-overridable).
# Text-only input tokens are tiny (caption+transcript ≪ media tokens), so the
# per-post cost is dominated by output tokens (~2 orders cheaper than the
# universal media call).
_TEXT_INPUT_PRICE_PER_M = float(
    os.environ.get("TEXT_FACETS_INPUT_PRICE_PER_M", "0.10")
)
_TEXT_OUTPUT_PRICE_PER_M = float(
    os.environ.get("TEXT_FACETS_OUTPUT_PRICE_PER_M", "0.40")
)


# ── Response parsing + validation ────────────────────────────────────────────


def parse_text_response(text: str | None) -> dict[str, Any]:
    """Parse + validate ONE text-call response.

    Returns ``{"text_facets", "errors"}``. ``text_facets`` is populated only
    when ``validate_text_facets`` passes clean. Never raises — callers route
    errors per item (same contract as ``parse_universal_response``).
    """
    if not text or not text.strip():
        return {"text_facets": None, "errors": ["empty response"]}
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        return {"text_facets": None, "errors": [f"invalid JSON: {exc}"]}
    if not isinstance(payload, dict):
        return {
            "text_facets": None,
            "errors": [f"expected object, got {type(payload).__name__}"],
        }

    facets = payload.get("text_facets")
    if not isinstance(facets, dict):
        return {"text_facets": None,
                "errors": ["text_facets: missing or not an object"]}
    errors = validate_text_facets(facets)
    if errors:
        return {"text_facets": None, "errors": [f"text_facets.{e}" for e in errors]}
    return {"text_facets": facets, "errors": []}


# ── Merge + storage (partial-per-pass contract, see module docstring) ────────


def merge_text_into_row(existing_json: str | None, text_facets: dict) -> str:
    """Merge validated text facets into an existing per-pass partial JSON.

    Existing visual fields are preserved untouched; text fields overwrite any
    prior text fields; the merge never injects summary/reserved keys.
    Returns the assembled JSON string.
    """
    merged: dict = {}
    if existing_json:
        try:
            existing = json.loads(existing_json)
            if isinstance(existing, dict):
                merged = existing
        except json.JSONDecodeError:
            logger.warning("existing growth_facets_json unparseable — replacing")
    merged.update(text_facets)
    return json.dumps(merged, ensure_ascii=False)


def _analysed_at_now() -> str:
    """Current UTC ISO timestamp (same convention as batch._now_iso).

    Full microsecond precision matters: the upsert ordering guard compares
    analysed_at strings, and a second-truncated timestamp loses to a
    same-second visual write (lexicographically shorter = smaller).
    """
    return datetime.now(timezone.utc).isoformat()


_TEXT_FACETS_UPSERT = """INSERT INTO gold_growth_facets
   (post_id, domain, prompt_hash, schema_version,
    growth_facets_json, content_summary, image_summaries_json,
    model, analysed_at)
   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
   ON CONFLICT (post_id, domain) DO UPDATE SET
       prompt_hash = excluded.prompt_hash,
       schema_version = excluded.schema_version,
       growth_facets_json = excluded.growth_facets_json,
       model = excluded.model,
       analysed_at = excluded.analysed_at
   WHERE gold_growth_facets.analysed_at IS NULL
      OR excluded.analysed_at > gold_growth_facets.analysed_at"""




def write_text_facets_conn(
    conn,
    post_id: str,
    domain: str,
    text_facets: dict,
    model: str = _DEFAULT_GEMINI_MODEL,
    prompt_hash: str | None = None,
) -> None:
    """Read-merge-upsert text facets into ``gold_growth_facets``.

    Preserves existing per-pass columns (content_summary, image_summaries_json)
    on the conflicting row; merges text fields into growth_facets_json per the
    partial-per-pass contract. Ordering-guarded like ``write_gold_facets_conn``.
    """
    row = conn.execute(
        "SELECT growth_facets_json, content_summary, image_summaries_json "
        "FROM gold_growth_facets WHERE post_id = ? AND domain = ?",
        [post_id, domain],
    ).fetchone()
    existing_json = row[0] if row else None
    content_summary = row[1] if row else None
    image_summaries_json = row[2] if row else None
    merged_json = merge_text_into_row(existing_json, text_facets)
    conn.execute(
        _TEXT_FACETS_UPSERT,
        (
            post_id,
            domain,
            prompt_hash or CURRENT_TEXT_FACETS_PROMPT_HASH,
            GROWTH_FACETS_SCHEMA_VERSION,
            merged_json,
            content_summary,
            image_summaries_json,
            model,
            _analysed_at_now(),
        ),
    )


# ── The text call (media-free, one per post) ─────────────────────────────────


def run_text_call(
    gemini,
    post_id: str,
    caption: str,
    transcript: str | None = None,
) -> dict[str, Any]:
    """Run the ONE media-free text call for a post (US-EFAC-4).

    Sends ONLY caption (+ transcript when present) — no media is uploaded,
    resolved, or billed. Transcript-optional: degrades to caption-only.
    Returns ``{"ok", "text_facets", "usage", "cost_usd", "elapsed_s",
    "errors", "raw"}``. Raises on transport-level failure only (429
    taxonomy handled by the driver's retry loop).
    """
    started = time.monotonic()
    prompt = build_text_facets_prompt(caption, transcript)
    text, usage = gemini.analyze_with_usage(
        prompt,
        max_output_tokens=TEXT_MAX_OUTPUT_TOKENS,
    )
    parsed = parse_text_response(text)
    elapsed = time.monotonic() - started
    return {
        "ok": not parsed["errors"],
        "text_facets": parsed["text_facets"],
        "usage": usage,
        "cost_usd": _estimate_cost(usage),
        "elapsed_s": elapsed,
        "errors": parsed["errors"],
        "raw": text,
    }


def _estimate_cost(usage: dict | None) -> float:
    """Tier-1 flash-lite cost estimate from response usage (design §7)."""
    if not usage:
        return 0.0
    prompt_t = usage.get("prompt_token_count") or 0
    cand_t = usage.get("candidates_token_count") or 0
    return (
        prompt_t / 1_000_000 * _TEXT_INPUT_PRICE_PER_M
        + cand_t / 1_000_000 * _TEXT_OUTPUT_PRICE_PER_M
    )
