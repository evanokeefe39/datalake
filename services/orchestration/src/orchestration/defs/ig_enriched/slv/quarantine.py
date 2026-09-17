"""Validation overlays for enrichment payloads — the quarantine vocabulary.

Every reason a payload can be rejected lives here, together with the checks that
produce them. The blocking asset checks match on these `REASON_*` codes, so the
vocabulary is a contract: adding a reason is safe, renaming or dropping one
silently changes what a check calls a failure.

The carousel rule is the cross-field case worth naming: a carousel payload whose
item count disagrees with its image-summary count is a `cross_field_violation`,
not a truncation.
"""

from __future__ import annotations

# ── Quarantine reason codes (distinguishable per violation class) ──────────
REASON_PROVIDER_ERROR = "provider_error"          # bronze ok=False (read, never inferred)
REASON_PARSE_ERROR = "parse_error"                # invalid JSON / not an object / empty body
REASON_MISSING_REQUIRED = "missing_required_field"
REASON_ENUM_VIOLATION = "enum_violation"
REASON_TYPE_VIOLATION = "type_violation"
REASON_LENGTH_VIOLATION = "length_violation"
REASON_UNKNOWN_FIELD = "unknown_field"
REASON_CROSS_FIELD = "cross_field_violation"      # e.g. carousel n != len(image_summaries)
REASON_COMPLETENESS = "completeness_violation"    # required summary/facet object absent
REASON_UNSUPPORTED_WORKLOAD = "unsupported_workload"


# Priority order: the FIRST matching class becomes the row's reason_code, so
# every quarantine row is attributable to exactly one violation class.
_REASON_PRIORITY: tuple[tuple[str, tuple[str, ...]], ...] = (
    (REASON_PARSE_ERROR, ("invalid JSON", "expected object", "empty response")),
    (REASON_MISSING_REQUIRED, ("missing required",)),
    (REASON_ENUM_VIOLATION, ("not in enum",)),
    (REASON_TYPE_VIOLATION, ("expected",)),
    (REASON_LENGTH_VIOLATION, ("exceeds length bound",)),
    (REASON_UNKNOWN_FIELD, ("unknown field", "unknown brand-safety flag")),
    (REASON_CROSS_FIELD, ("cross-field",)),
    (REASON_COMPLETENESS, ("completeness:",)),
)


def classify_reason(errors: list[str]) -> str:
    """Map validator error strings to the highest-priority reason code.

    Postcondition: returns one of the ``REASON_*`` codes (never raises);
    an unclassifiable error falls back to ``REASON_COMPLETENESS`` so a
    quarantine row is never emitted without an attributable class.
    """
    for code, markers in _REASON_PRIORITY:
        if any(marker in e for e in errors for marker in markers):
            return code
    return REASON_COMPLETENESS


# ── Length bounds (conform-layer contract; deterministic, versioned) ───────
MAX_SUMMARY_CHARS = 4000
"""Bound for content_summary / per-image summaries / transcript_summary."""
MAX_ANNOTATION_TEXT_CHARS = 2000


_LENGTH_BOUNDS: dict[str, int] = {
    "content_summary": MAX_SUMMARY_CHARS,
    "hook_content": MAX_ANNOTATION_TEXT_CHARS,
    "sponsorship_signal": MAX_ANNOTATION_TEXT_CHARS,
    "replicable_tactic": MAX_ANNOTATION_TEXT_CHARS,
    "evidence": MAX_ANNOTATION_TEXT_CHARS,
    "hashtag_strategy": MAX_ANNOTATION_TEXT_CHARS,
    "value_medium": MAX_ANNOTATION_TEXT_CHARS,
}


_LENGTH_FIELD_KEYS = {
    "content_summary": "content_summary",
    "hook_content": "hook_content",
    "sponsorship_signal": "sponsorship_signal",
    "replicable_tactic": "replicable_tactic",
    "evidence": "evidence",
    "hashtag_strategy": "hashtag_strategy",
    "value_medium": "visual_facets.value_medium",
}


def _length_errors(payload: dict) -> list[str]:
    """Length-bound violations, reason-coded via the ``exceeds length bound``
    marker (→ ``length_violation``). Free-text "" is allowed; only *bounds*."""
    errors: list[str] = []
    summary = payload.get("content_summary")
    if isinstance(summary, str) and len(summary) > MAX_SUMMARY_CHARS:
        errors.append(
            f"content_summary: exceeds length bound {MAX_SUMMARY_CHARS} "
            f"(got {len(summary)} chars)"
        )
    facets = payload.get("visual_facets")
    if isinstance(facets, dict):
        value = facets.get("value_medium")
        if isinstance(value, str) and len(value) > MAX_ANNOTATION_TEXT_CHARS:
            errors.append(
                f"visual_facets.value_medium: exceeds length bound "
                f"{MAX_ANNOTATION_TEXT_CHARS} (got {len(value)} chars)"
            )
    for field_name, bound in _LENGTH_BOUNDS.items():
        if field_name == "content_summary" or field_name == "value_medium":
            continue
        value = payload.get(field_name)
        if isinstance(value, str) and len(value) > bound:
            errors.append(
                f"{field_name}: exceeds length bound {bound} "
                f"(got {len(value)} chars)"
            )
    return errors


def _coerce_bool(value: object) -> bool | None:
    """Legacy-cast semantics: real JSON booleans pass, "true"/"1" conform,
    NULL stays NULL — never silently coerced to False."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.strip().lower() in {"true", "1"}:
        return True
    if isinstance(value, str) and value.strip().lower() in {"false", "0"}:
        return False
    return bool(value)


CLASSIFICATION_BODY_KEYS: tuple[str, ...] = (
    "domain",
    "subdomain",
    "topic",
    "subtopic",
    "is_educational",
    "is_actionable",
    "admiralty",
    "content_type",
    "style",
    "format",
)
