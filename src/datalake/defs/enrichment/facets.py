"""Universal video→Gemini call — visual facets + folded summaries (US-EFAC-3/1).

One media call per post at MEDIA_RESOLUTION_LOW on flash-lite:
- ``visual_facets``  — the V3 visual-core sub-schema (face_present,
  value_medium, brand_logos, text_overlay_present, on_screen_claim), validated
  by ``validate_visual_facets``; text-layer facets are NOT extracted here
  (US-EFAC-4 deferral).
- ``content_summary`` / ``image_summaries`` — separate additive columns, never
  inside the facet JSON (locked schema rejects them as unknown fields).

Reuses the proven interactive media path (scrape-time byte cache → File API →
tier + per-item token gate) via ``analysis._resolve_media_for_post`` — no
parallel pipeline (AGENTS.md multimodal status). Storage is additive: a
``gold_growth_facets`` table keyed ``(post_id, domain)`` with its own
``prompt_hash``; the gold table and reserved keys are untouched (ADR-0008).

Cost accounting: per-response usage_metadata + Tier-1 flash-lite list-price
ESTIMATES (configurable via env); reported per item and per run.
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

from datalake.defs.common.schemas import duckdb_ddl
from datalake.defs.enrichment import analysis as ew
from datalake.defs.enrichment.growth_facets_schema import (
    GROWTH_FACETS_SCHEMA_VERSION,
    validate_visual_facets,
)
from datalake.defs.enrichment.prompts import (
    _DEFAULT_GEMINI_MODEL,
    CURRENT_FACETS_PROMPT_HASH,
    build_growth_facets_prompt,
)

logger = logging.getLogger("enrichment.facets")

# Universal-call output budget (design §6): facets + bounded summaries fit 4096.
UNIVERSAL_MAX_OUTPUT_TOKENS = 4096

# Tier-1 flash-lite list-price ESTIMATES, USD per 1M tokens (env-overridable).
# Grounded against the repo's own ~$0.002/media-call estimate (design §7).
_INPUT_PRICE_PER_M = float(os.environ.get("FACETS_INPUT_PRICE_PER_M", "0.10"))
_OUTPUT_PRICE_PER_M = float(os.environ.get("FACETS_OUTPUT_PRICE_PER_M", "0.40"))


# ── Response parsing + validation ────────────────────────────────────────────


def parse_universal_response(text: str | None, n_media: int) -> dict[str, Any]:
    """Parse + validate ONE universal-call response.

    Returns ``{"visual_facets", "content_summary", "image_summaries", "errors"}``.
    ``visual_facets`` is populated only when ``validate_visual_facets`` passes
    clean. ``image_summaries`` is required (and length-checked against
    ``n_media``) only for carousels (``n_media > 1``). Never raises — callers
    route errors per item.
    """
    errors: list[str] = []
    facets: dict | None = None
    summary: str | None = None
    image_summaries: list | None = None

    if not text or not text.strip():
        return {"visual_facets": None, "content_summary": None,
                "image_summaries": None, "errors": ["empty response"]}
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        return {"visual_facets": None, "content_summary": None,
                "image_summaries": None, "errors": [f"invalid JSON: {exc}"]}
    if not isinstance(payload, dict):
        return {"visual_facets": None, "content_summary": None,
                "image_summaries": None,
                "errors": [f"expected object, got {type(payload).__name__}"]}

    facets = payload.get("visual_facets")
    if not isinstance(facets, dict):
        errors.append("visual_facets: missing or not an object")
        facets = None
    else:
        facet_errors = validate_visual_facets(facets)
        if facet_errors:
            errors.extend(f"visual_facets.{e}" for e in facet_errors)
            facets = None

    summary = payload.get("content_summary")
    if summary is not None and not isinstance(summary, str):
        errors.append(
            f"content_summary: expected string, got {type(summary).__name__}"
        )
        summary = None

    if n_media > 1:
        image_summaries = payload.get("image_summaries")
        if not isinstance(image_summaries, list):
            errors.append(
                "image_summaries: missing or not an array (carousel)"
            )
            image_summaries = None
        elif len(image_summaries) != n_media:
            errors.append(
                f"image_summaries: expected {n_media} entries, "
                f"got {len(image_summaries)} (index mis-alignment)"
            )
            image_summaries = None
        else:
            for i, entry in enumerate(image_summaries):
                if not (
                    isinstance(entry, dict)
                    and set(entry) == {"index", "summary"}
                    and isinstance(entry["index"], int)
                    and isinstance(entry["summary"], str)
                ):
                    errors.append(
                        f"image_summaries[{i}]: expected "
                        '{"index": int, "summary": string}, got '
                        f"{entry!r}"
                    )
                    image_summaries = None
                    break

    return {"visual_facets": facets, "content_summary": summary,
            "image_summaries": image_summaries, "errors": errors}


_GOLD_FACETS_DDL = duckdb_ddl("gold_growth_facets")

_GOLD_FACETS_UPSERT = """INSERT INTO gold_growth_facets
   (post_id, domain, prompt_hash, schema_version,
    growth_facets_json, content_summary, image_summaries_json,
    model, analysed_at)
   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
   ON CONFLICT (post_id, domain) DO UPDATE SET
       prompt_hash = excluded.prompt_hash,
       schema_version = excluded.schema_version,
       growth_facets_json = excluded.growth_facets_json,
       content_summary = excluded.content_summary,
       image_summaries_json = excluded.image_summaries_json,
       model = excluded.model,
       analysed_at = excluded.analysed_at
   WHERE gold_growth_facets.analysed_at IS NULL
      OR excluded.analysed_at > gold_growth_facets.analysed_at"""


def write_gold_facets_conn(
    conn,
    post_id: str,
    domain: str,
    visual_facets: dict,
    content_summary: str | None,
    image_summaries: list | None,
    model: str = _DEFAULT_GEMINI_MODEL,
    prompt_hash: str | None = None,
) -> None:
    """``write_gold_facets`` over an EXISTING duckdb connection (pilot path)."""
    conn.execute(_GOLD_FACETS_DDL)
    conn.execute(
        _GOLD_FACETS_UPSERT,
        [
            post_id,
            domain,
            prompt_hash or CURRENT_FACETS_PROMPT_HASH,
            GROWTH_FACETS_SCHEMA_VERSION,
            json.dumps(visual_facets, sort_keys=True),
            content_summary,
            json.dumps(image_summaries) if image_summaries is not None else None,
            model,
            ew._now_iso(),
        ],
    )


def write_gold_facets(
    duckdb: DuckDBResource,
    post_id: str,
    domain: str,
    visual_facets: dict,
    content_summary: str | None,
    image_summaries: list | None,
    model: str = _DEFAULT_GEMINI_MODEL,
    prompt_hash: str | None = None,
) -> None:
    """Upsert validated universal-call output into ``gold_growth_facets``.

    Additive table (ADR-0008): own PK ``(post_id, domain)``, own ``prompt_hash``
    (schema version folded in), ordering guard identical to ``write_gold`` so a
    stale concurrent write never clobbers a newer one. Never touches
    ``gold_analyses`` or its reserved keys.
    """
    with duckdb.get_connection() as conn:
        write_gold_facets_conn(
            conn, post_id, domain, visual_facets, content_summary,
            image_summaries, model, prompt_hash,
        )


# ── The universal call (one media call per post) ─────────────────────────────


def run_universal_call(
    ops: SQLiteResource,
    gemini: GeminiResource,
    post_id: str,
    caption: str,
    media_files_json: str | None,
) -> dict[str, Any]:
    """Run the ONE universal media call for a post (US-EFAC-3 + US-ESUM-1).

    Reuses the proven interactive media path: byte cache → File API via
    ``_resolve_media_for_post`` (FREE-tier video gate + per-item token cap),
    then a single flash-lite call at MEDIA_RESOLUTION_LOW, 4096 output.
    Returns ``{"ok", "visual_facets", "content_summary", "image_summaries",
    "usage", "cost_usd", "elapsed_s", "n_media", "errors", "text_only"}``.
    Raises on transport-level failure only (429 taxonomy handled by caller's
    retry loop, same as ``process_item``).
    """
    started = time.monotonic()
    media_files = ew._resolve_media_for_post(ops, gemini, post_id, media_files_json)
    if not media_files:
        # Media unresolvable / gated out: terminal per-item condition.
        return {"ok": False, "visual_facets": None, "content_summary": None,
                "image_summaries": None, "usage": None, "cost_usd": 0.0,
                "elapsed_s": 0.0, "n_media": 0, "text_only": True,
                "errors": ["media unresolvable or gated out (no File API media)"]}

    n_media = len(media_files)
    prompt = build_growth_facets_prompt(caption, n_media)
    text, usage = gemini.analyze_with_usage(
        prompt,
        media_files=media_files,
        media_resolution="MEDIA_RESOLUTION_LOW",
        max_output_tokens=UNIVERSAL_MAX_OUTPUT_TOKENS,
    )
    parsed = parse_universal_response(text, n_media)
    elapsed = time.monotonic() - started
    cost = _estimate_cost(usage)
    return {
        "ok": not parsed["errors"],
        "visual_facets": parsed["visual_facets"],
        "content_summary": parsed["content_summary"],
        "image_summaries": parsed["image_summaries"],
        "usage": usage,
        "cost_usd": cost,
        "elapsed_s": elapsed,
        "text_only": False,
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
        prompt_t / 1_000_000 * _INPUT_PRICE_PER_M
        + cand_t / 1_000_000 * _OUTPUT_PRICE_PER_M
    )
