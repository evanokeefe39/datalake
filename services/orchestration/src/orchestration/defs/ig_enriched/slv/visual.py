"""Visual payload mapping — `silver_visual_annotations` + `silver_visual_summaries`.

The per-image facet layer of a post: what is in each frame, and the overall
summary the visual pass produces alongside it.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime

from orchestration.defs.ig_enriched.slv.quarantine import _length_errors

_RUNTIME = None


def _rt():
    """The shared silver-build runtime, imported lazily.

    The runtime imports these payload modules for their table mappings, so a
    module-level import back would be a cycle. The helpers here (row assembly,
    provenance, JSON encoding, table ids) are the runtime's, and are what make
    every table's rows structurally identical.
    """
    global _RUNTIME
    if _RUNTIME is None:
        from orchestration.defs.engine import silver_rt

        _RUNTIME = silver_rt
    return _RUNTIME


def _conform_visual(
    bronze_row: Mapping[str, object],
    payload: dict,
    n_media: int | None,
    *,
    now: datetime,
) -> tuple[dict[str, object], dict[str, object], list[str]]:
    """Map one visual-pass payload onto the two visual tables.

    Returns ``(annotation_row, summary_row, errors)`` — rows are populated
    only when ``errors`` is empty (a failed row NEVER produces a silently-
    null conformed row).

    Cross-field check: for a carousel of ``n`` images, ``n ==
    len(image_summaries)`` must hold. ``n`` comes from ``n_media_by_post``
    (deterministic silver metadata — no API); when it is unknown, the
    per-image ``index`` sequence must still be exactly ``0..len-1``
    (contiguous, zero-based) — mis-alignment fires either way.
    """
    errors: list[str] = _length_errors(payload)

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
    if summary is None:
        errors.append("completeness: content_summary required for every visual response")
    elif not isinstance(summary, str):
        errors.append(f"content_summary: expected string, got {type(summary).__name__}")
        summary = None

    image_summaries = payload.get("image_summaries")
    if image_summaries is not None or (n_media is not None and n_media > 1):
        # Carousel path: entries must be {index: int, summary: str}.
        if not isinstance(image_summaries, list):
            errors.append("cross-field: image_summaries missing or not an array (carousel)")
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
                        f"image_summaries[{i}]: expected {{'index': int, "
                        f"'summary': string}}, got {entry!r}"
                    )
                    image_summaries = None
                    break
            if image_summaries is not None:
                # THE carousel cross-field check: n == len(image_summaries).
                if n_media is not None and len(image_summaries) != n_media:
                    errors.append(
                        f"cross-field: carousel of {n_media} images but "
                        f"image_summaries has {len(image_summaries)} entries "
                        "(index mis-alignment)"
                    )
                    image_summaries = None
                else:
                    indices = [e["index"] for e in image_summaries]
                    if sorted(indices) != list(range(len(image_summaries))):
                        errors.append(
                            "cross-field: image_summaries indices must be "
                            f"exactly 0..{len(image_summaries) - 1}, got {indices} "
                            "(index mis-alignment)"
                        )
                        image_summaries = None

    annotation_row: dict[str, object] | None = None
    summary_row: dict[str, object] | None = None
    if not errors and facets is not None and summary is not None:
        annotation_row = {
            "face_present": facets["face_present"],
            "value_medium": facets["value_medium"],
            "brand_logos_json": _rt()._json(facets["brand_logos"]),
            "text_overlay_present": facets["text_overlay_present"],
            "on_screen_claim": facets["on_screen_claim"],
        }
        summary_row = {
            "content_summary": summary,
            "image_summaries_json": (
                _rt()._json(image_summaries) if image_summaries is not None else None
            ),
        }
    prov = _rt()._provenance(bronze_row, now=now)
    ann_out = (
        _rt()._assemble(_rt().SILVER_VISUAL_ANNOTATIONS, bronze_row, annotation_row, prov)
        if annotation_row
        else None
    )
    sum_out = (
        _rt()._assemble(_rt().SILVER_VISUAL_SUMMARIES, bronze_row, summary_row, prov)
        if summary_row
        else None
    )
    return ann_out, sum_out, errors


# ── Response parsing ────────────────────────────────────────────────────────


import json  # noqa: E402
import logging  # noqa: E402
from typing import Any  # noqa: E402

from orchestration.defs.ig_enriched.slv.schemas import (  # noqa: E402
    validate_visual_facets,
)

logger = logging.getLogger("ig_enriched.visual")

UNIVERSAL_MAX_OUTPUT_TOKENS = 4096
"""Output-token ceiling for a universal visual response."""


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
