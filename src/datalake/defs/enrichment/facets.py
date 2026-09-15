"""Facets response parsing + validation (US-EFAC-3/4/1).

The batch-native facets path (``facets_batch``) submits visual + text calls
to the Gemini BATCH API and harvests them here:
- ``parse_universal_response`` / ``parse_text_response`` — validate each
  response against the V3 schema / text-layer sub-schema, so invalid
  responses are counted loudly at harvest time (never silently landed).

The former ``gold_growth_facets`` write helpers (``write_gold_facets*``,
``_GOLD_FACETS_DDL`` / ``_GOLD_FACETS_UPSERT``) were RETIRED 2026-09-15
(W9): the harvest now lands responses VERBATIM in bronze
(``bronze_enrichment_raw``) and conform publishes the typed
``silver_visual_annotations`` / ``silver_text_annotations`` tables. No live
code may create or write ``gold_growth_facets``.

The synchronous interactive call (``run_universal_call``) was removed
2026-09-08 — enrichment is BATCH-NATIVE ONLY.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from datalake.defs.enrichment.growth_facets_schema import (
    validate_text_facets,
    validate_visual_facets,
)

logger = logging.getLogger("enrichment.facets")

# Universal-call output budget (design §6): facets + bounded summaries fit 4096.
UNIVERSAL_MAX_OUTPUT_TOKENS = 4096



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



def parse_text_response(text: str | None) -> dict[str, Any]:
    """Parse + validate ONE text-layer-call response (US-EFAC-4).

    Returns ``{"text_facets", "errors"}`` — ``text_facets`` is populated only
    when ``validate_text_facets`` passes clean (all required fields present,
    correct types, no unknown keys, no visual/summary bleed). Never raises.
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
    errors = validate_text_facets(payload)
    return {"text_facets": None if errors else payload, "errors": errors}





