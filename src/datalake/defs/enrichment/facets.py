"""Facets response parsing + gold_growth_facets writes (US-EFAC-3/4/1).

The batch-native facets path (``facets_batch``) submits visual + text calls
to the Gemini BATCH API and harvests them here:
- ``parse_universal_response`` / ``parse_text_response`` — validate each
  response against the V3 schema / text-layer sub-schema,
- ``write_gold_facets_conn`` / ``write_gold_facets_pass_conn`` — additive,
  MERGE-semantics upserts into ``gold_growth_facets`` keyed
  ``(post_id, domain)`` with its own ``prompt_hash``; the gold table and
  reserved keys are untouched (ADR-0008).

The synchronous interactive call (``run_universal_call``) was removed
2026-09-08 — enrichment is BATCH-NATIVE ONLY.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from datalake.defs.common.resources import DuckDBResource
from datalake.defs.common.schemas import duckdb_ddl
from datalake.defs.enrichment import analysis as ew
from datalake.defs.enrichment.growth_facets_schema import (
    GROWTH_FACETS_SCHEMA_VERSION,
    validate_text_facets,
    validate_visual_facets,
)
from datalake.defs.enrichment.prompts import (
    _DEFAULT_QWEN_MODEL,
    CURRENT_FACETS_PROMPT_HASH,
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
       content_summary = coalesce(excluded.content_summary,
                                  gold_growth_facets.content_summary),
       image_summaries_json = coalesce(excluded.image_summaries_json,
                                       gold_growth_facets.image_summaries_json),
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
    model: str = _DEFAULT_QWEN_MODEL,
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


def write_gold_facets_pass_conn(
    conn,
    post_id: str,
    domain: str,
    facet_fields: dict,
    model: str = _DEFAULT_QWEN_MODEL,
    prompt_hash: str | None = None,
    content_summary: str | None = None,
    image_summaries: list | None = None,
) -> None:
    """Upsert ONE pass's sub-fields into ``gold_growth_facets`` (MERGE).

    Partial-storage contract (batch path): the visual pass writes the visual
    sub-fields + ``content_summary`` / ``image_summaries_json``; the text pass
    merges the text sub-fields. Each write MERGES into the row's existing
    ``growth_facets_json`` (never clobbers the other pass's fields), so the
    stored row is the union and satisfies the full V3 schema once both passes
    have landed. Ordering guard identical to ``write_gold``: a stale write
    (older ``analysed_at``) never clobbers a newer one.
    """
    conn.execute(_GOLD_FACETS_DDL)
    existing = conn.execute(
        "SELECT growth_facets_json, analysed_at FROM gold_growth_facets "
        "WHERE post_id = ? AND domain = ?",
        [post_id, domain],
    ).fetchone()
    merged: dict = {}
    if existing and existing[0]:
        try:
            prior = json.loads(existing[0])
            if isinstance(prior, dict):
                merged = prior
        except json.JSONDecodeError:
            pass
    merged.update(facet_fields)
    conn.execute(
        _GOLD_FACETS_UPSERT,
        [
            post_id,
            domain,
            prompt_hash or CURRENT_FACETS_PROMPT_HASH,
            GROWTH_FACETS_SCHEMA_VERSION,
            json.dumps(merged, sort_keys=True),
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
    model: str = _DEFAULT_QWEN_MODEL,
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


