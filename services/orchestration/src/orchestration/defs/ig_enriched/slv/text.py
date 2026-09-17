"""Text payload mapping — `silver_text_annotations` + `silver_text_summaries`.

The caption-layer facets. The text pass does not emit a summary yet, so
`silver_text_summaries` is explicitly empty rather than fabricated — the same
discipline the transcript table follows.
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


def _conform_text(
    bronze_row: Mapping[str, object],
    payload: dict,
    *,
    now: datetime,
) -> tuple[dict[str, object], list[str]]:
    """Map one text-pass payload onto ``silver_text_annotations``.

    The text pass emits text-layer facets + brand_safety only — it does not
    yet emit a transcript summary, so ``silver_text_summaries`` stays empty
    (explicit, per its docstring). Returns ``(row, errors)``; row is None-
    equivalent (empty dict + errors) on failure.
    """
    errors = validate_text_facets(payload) + _length_errors(payload)
    if errors:
        return {}, errors

    row = {
        "hook_content": payload["hook_content"],
        "hook_type": payload["hook_type"],
        "is_sponsored": payload["is_sponsored"],
        "sponsorship_signal": payload["sponsorship_signal"],
        "claimed_results": payload["claimed_results"],
        "cta_type": payload["cta_type"],
        "audience_named": payload["audience_named"],
        "value_depth": payload["value_depth"],
        "replicable_tactic": payload["replicable_tactic"],
        "hashtag_strategy": payload.get("hashtag_strategy", ""),
        "evidence": payload["evidence"],
        "brand_safety_json": _rt()._json(payload["brand_safety"]),
    }
    prov = _rt()._provenance(bronze_row, now=now)
    return _rt()._assemble(_rt().SILVER_TEXT_ANNOTATIONS, bronze_row, row, prov), []


# ── Response parsing ────────────────────────────────────────────────────────


import json  # noqa: E402
import logging  # noqa: E402
from typing import Any  # noqa: E402

from orchestration.defs.ig_enriched.slv.schemas import (  # noqa: E402
    validate_text_facets,
)

logger = logging.getLogger("ig_enriched.text")


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
