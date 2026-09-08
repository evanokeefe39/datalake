"""Canonical V3 growth-facet JSON-Schema + post-parse validator (US-EFAC-1).

This module is the single source of truth for the ``growth_facets_json``
column produced by the universal video→Gemini call (design doc §4). It is
PURE/hermetic under the ADR-0008 seam: no media, API, or DB imports, no
Dagster ops — only the schema dict, constants, and a stdlib validator.

Design notes (documented in ``docs/growth-facets-schema.md``):

- Keep-bias: the field set is BROAD. Only structurally excluded things are
  the reserved gold keys (``RESERVED_GOLD_KEYS``) — the gold pass owns them.
- Facets are modality-agnostic: each facet reads whatever channel carries
  its evidence (caption, transcript, on-screen text, imagery). The schema
  therefore does not encode per-facet channels.
- ``content_summary`` / ``image_summaries`` are deliberately NOT here — they
  are separate additive columns, not facets.
- No ``jsonschema`` dependency: validation is hand-rolled with clear,
  human-readable error strings (checked pyproject 2026-09-08).
- ``value_medium`` is **open-with-other** free text (V3 was <0.85 agreement;
  demoted at lock 2026-09-08 rather than gated on a later v4 bump).
  ``VALUE_MEDIUM_EXAMPLES`` is model-facing vocabulary only, never enforced.
"""

from __future__ import annotations

# ── Versioning ───────────────────────────────────────────────────────────────
# Bump on any breaking change to the field set, enums, or required split.
# Passes owning extraction must fold this into their prompt_hash so stale
# facet JSON written under an older schema is detectable (US-EFAC-1 AC6).
GROWTH_FACETS_SCHEMA_VERSION = "3"

# ── Reserved gold keys (structurally excluded) ──────────────────────────────
# Owned by the existing gold pass; a facet payload carrying any of these is a
# contract violation (cross-pass key bleed) and MUST be rejected.
RESERVED_GOLD_KEYS: frozenset[str] = frozenset(
    {
        "is_educational",
        "is_actionable",
        "admiralty",
        "domain",
        "subdomain",
        "content_type",
        "format",
        "educational_json",
        "actionable_json",
    }
)

# ── Enums ────────────────────────────────────────────────────────────────────
HOOK_TYPES = (
    "pattern_interrupt",
    "bold_claim",
    "question",
    "benefit_promise",
    "curiosity_gap",
    "story_open",
    "visual_hook",
    "demonstration",
    "none_clear",
    "other",
)
CTA_TYPES = (
    "comment",
    "save",
    "share",
    "follow",
    "like",
    "link_click",
    "dm",
    "none",
    "other",
)
VALUE_DEPTHS = ("shallow", "practical", "deep")
VALUE_MEDIUM_EXAMPLES = (
    # Open-with-other: shown to the model as descriptive vocabulary, NOT
    # validated. The model writes the actual presentation form as free text.
    "demo",
    "talking_head",
    "screenshare",
    "broll_voiceover",
    "slideshow_carousel",
    "text_graphic",
    "other",
)
BRAND_SAFETY_FLAGS = (
    "profanity",
    "sexualized_content",
    "political",
    "medical_claims",
    "financial_guarantees",
    "violence_trauma",
)

# ── Field sets ───────────────────────────────────────────────────────────────
# Required = every decision-grade field (bools, enums, brand_safety) plus the
# free-text fields, which must be PRESENT but MAY be empty ("") when the
# evidence channel carries nothing — absence vs "nothing found" must be
# distinguishable from a malformed payload. ``hashtag_strategy`` is the only
# optional field (frequently absent by construction; see docs).
FREE_TEXT_FIELDS = (
    "hook_content",
    "sponsorship_signal",
    "replicable_tactic",
    "evidence",
    "value_medium",
)
BOOL_FIELDS = (
    "is_sponsored",
    "claimed_results",
    "audience_named",
    "face_present",
    "text_overlay_present",
    "on_screen_claim",
)
ENUM_FIELDS = {
    "hook_type": HOOK_TYPES,
    "cta_type": CTA_TYPES,
    "value_depth": VALUE_DEPTHS,
}
ARRAY_FIELDS = ("brand_logos",)
OPTIONAL_FIELDS = ("hashtag_strategy",)

# ── JSON-Schema (canonical, draft-agnostic) ─────────────────────────────────
GROWTH_FACETS_JSON_SCHEMA: dict = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "datalake/schemas/growth_facets_json/v3",
    "title": "growth_facets_json (V3 cross-modal growth facets)",
    "type": "object",
    "additionalProperties": False,
    "required": [
        # text-layer / cross-modal
        "hook_content",
        "hook_type",
        "is_sponsored",
        "sponsorship_signal",
        "claimed_results",
        "cta_type",
        "audience_named",
        "value_depth",
        "replicable_tactic",
        "evidence",
        # visual core
        "face_present",
        "value_medium",
        "brand_logos",
        "text_overlay_present",
        "on_screen_claim",
        # brand safety
        "brand_safety",
    ],
    "properties": {
        "hook_content": {"type": "string"},
        "hook_type": {"type": "string", "enum": list(HOOK_TYPES)},
        "is_sponsored": {"type": "boolean"},
        "sponsorship_signal": {"type": "string"},
        "claimed_results": {"type": "boolean"},
        "cta_type": {"type": "string", "enum": list(CTA_TYPES)},
        "audience_named": {"type": "boolean"},
        "value_depth": {"type": "string", "enum": list(VALUE_DEPTHS)},
        "replicable_tactic": {"type": "string"},
        "hashtag_strategy": {"type": "string"},
        "evidence": {"type": "string"},
        "face_present": {"type": "boolean"},
        "value_medium": {"type": "string"},
        "brand_logos": {
            "type": "array",
            "items": {"type": "string"},
        },
        "text_overlay_present": {"type": "boolean"},
        "on_screen_claim": {"type": "boolean"},
        "brand_safety": {
            "type": "object",
            "additionalProperties": False,
            "required": list(BRAND_SAFETY_FLAGS),
            "properties": {
                flag: {"type": "boolean"} for flag in BRAND_SAFETY_FLAGS
            },
        },
    },
}

# ── Validator ────────────────────────────────────────────────────────────────


def _err_type(field: str, expected: str, value: object) -> str:
    return (
        f"{field}: expected {expected}, got "
        f"{type(value).__name__} ({value!r})"
    )


def validate_growth_facets(obj: object) -> list[str]:
    """Validate a parsed ``growth_facets_json`` payload.

    Returns a list of human-readable error strings; empty list = valid.
    Checks, in order: top-level object shape, reserved gold keys, required
    fields, field types/enums, ``brand_safety`` exact 6-flag shape, unknown
    keys, and ``brand_logos`` element types.
    """
    errors: list[str] = []
    if not isinstance(obj, dict):
        return [f"payload: expected object, got {type(obj).__name__}"]

    # Reserved gold keys — specific error before the generic unknown-key check.
    for key in sorted(RESERVED_GOLD_KEYS & obj.keys()):
        errors.append(
            f"{key}: reserved gold key is structurally excluded from "
            "growth_facets_json"
        )

    for field in GROWTH_FACETS_JSON_SCHEMA["required"]:
        if field not in obj:
            errors.append(f"{field}: missing required field")


    # Free-text fields: present (checked above) and string; "" allowed.
    for field in FREE_TEXT_FIELDS:
        if field in obj and not isinstance(obj[field], str):
            errors.append(_err_type(field, "string", obj[field]))

    for field in BOOL_FIELDS:
        if field in obj and not isinstance(obj[field], bool):
            errors.append(_err_type(field, "boolean", obj[field]))

    for field, allowed in ENUM_FIELDS.items():
        if field in obj:
            value = obj[field]
            if not isinstance(value, str):
                errors.append(_err_type(field, "string enum", value))
            elif value not in allowed:
                errors.append(
                    f"{field}: {value!r} not in enum {list(allowed)}"
                )

    for field in ARRAY_FIELDS:
        if field in obj:
            value = obj[field]
            if not isinstance(value, list):
                errors.append(_err_type(field, "array of strings", value))
            else:
                for i, item in enumerate(value):
                    if not isinstance(item, str):
                        errors.append(
                            f"{field}[{i}]: expected string, got "
                            f"{type(item).__name__} ({item!r})"
                        )

    if "hashtag_strategy" in obj and not isinstance(
        obj["hashtag_strategy"], str
    ):
        errors.append(_err_type("hashtag_strategy", "string", obj["hashtag_strategy"]))

    # brand_safety: exact 6-flag bool object.
    if "brand_safety" in obj:
        bs = obj["brand_safety"]
        if not isinstance(bs, dict):
            errors.append(_err_type("brand_safety", "object", bs))
        else:
            for flag in BRAND_SAFETY_FLAGS:
                if flag not in bs:
                    errors.append(f"brand_safety.{flag}: missing required flag")
            for key, value in sorted(bs.items()):
                if key not in BRAND_SAFETY_FLAGS:
                    errors.append(
                        f"brand_safety.{key}: unknown brand-safety flag "
                        f"(allowed: {list(BRAND_SAFETY_FLAGS)})"
                    )
                elif not isinstance(value, bool):
                    errors.append(
                        _err_type(f"brand_safety.{key}", "boolean", value)
                    )

    # Unknown keys beyond the reserved set (schema is locked; catch typos).
    for key in sorted(obj.keys() - GROWTH_FACETS_JSON_SCHEMA["properties"].keys()):
        if key not in RESERVED_GOLD_KEYS:
            errors.append(f"{key}: unknown field (not in V3 schema)")

    return errors
