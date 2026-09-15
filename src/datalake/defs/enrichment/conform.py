"""Silver conform + validation for enrichment responses (ADR-0011 Phase 4).

Turns ``bronze_enrichment_raw`` (verbatim, already landed) into six validated
silver tables — **a pure function of bronze with ZERO network/API calls**.

Enforced structurally: this module's import graph contains only the standard
library, ``polars``, the lake path helpers, the bronze landing reader, and the
facet schema registry. No provider client, adapter, seam module, or HTTP
library is imported — a conform run cannot reach the network because no code
path that knows how to is even loaded. Tests additionally assert the import
graph (see ``tests/unit/enrichment/test_conform.py``).

Tables (each keyed ``(post_id, platform)``, each carrying its OWN provenance
columns ``provider / model / prompt_hash / schema_version / run_id`` plus
``conformed_at`` and a ``derivation_version`` so stale state under old logic
is detectable — self-versioning):

- ``silver_visual_annotations``   — visual-core facets   (growth-facets-visual)
- ``silver_visual_summaries``     — overall + per-image summaries (same pass)
- ``silver_audio_transcripts``    — STT transcript       (NO landing workload
  yet — the table and its conform path exist, but ``TRANSCRIPT_WORKLOADS`` is
  deliberately empty, so correct emptiness is explicit, never fabricated)
- ``silver_content_classification`` — taxonomy + educational/actionable +
  admiralty (content-classification; also the target of the legacy
  ``gold_analyses`` backfill, ``migrations/migrate_classification_to_silver.py``)
- ``silver_text_annotations``     — text-layer facets    (growth-facets-text)
- ``silver_text_summaries``       — transcript summary   (the text pass does
  not emit a summary yet; same explicit-emptiness rule as transcripts)

Validation lives HERE (never inferred downstream). A bronze row that fails
terminally — unparseable JSON, missing required fields, enum violation, type
violation, length violation, unknown field, carousel cross-field mismatch,
completeness, or a failed provider landing — lands LOUDLY in
``silver_enrichment_quarantine`` with a machine-readable ``reason_code`` and
the full error detail. A malformed row NEVER produces a silently-NULL conformed
row and is NEVER swallowed. This quarantine surface replaces the retired
``ops.sqlite dead_letter`` table within conform (ADR-0012/0013: the general
failure view is ``landed ∖ conformed``; the quarantine makes each reason
identifiable per row).

Deterministic replay: ``conform`` over the same bronze produces byte-identical
tables (given the same ``now``). A schema/mapping change bumps
``DERIVATION_VERSION`` and is a replay of bronze — never a re-bill.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from datalake.defs.common import lake
from datalake.defs.enrichment import landing
from datalake.defs.enrichment.growth_facets_schema import (
    validate_text_facets,
    validate_visual_facets,
)

logger = logging.getLogger("enrichment.conform")

DERIVATION_VERSION = "1"
"""Bump on any change to conform logic, field mapping, bounds, or reason
codes. Each silver row records it; a version mismatch on read = stale derived
state under old logic."""

# ── Table identity (layer-first naming, platform key, provider-agnostic) ───
SILVER_VISUAL_ANNOTATIONS = "silver_visual_annotations"
SILVER_VISUAL_SUMMARIES = "silver_visual_summaries"
SILVER_AUDIO_TRANSCRIPTS = "silver_audio_transcripts"
SILVER_TEXT_ANNOTATIONS = "silver_text_annotations"
SILVER_TEXT_SUMMARIES = "silver_text_summaries"
SILVER_CONTENT_CLASSIFICATION = "silver_content_classification"
"""The classification pass's table — ``gold_analyses``'s replacement. The
join key is ``platform``; ``domain`` here means ONLY the content niche
(the ``gold_analyses.domain = 'instagram'`` overload is the bug this table
fixes, ADR-0011)."""
SILVER_QUARANTINE = "silver_enrichment_quarantine"

SILVER_TABLES: tuple[str, ...] = (
    SILVER_VISUAL_ANNOTATIONS,
    SILVER_VISUAL_SUMMARIES,
    SILVER_AUDIO_TRANSCRIPTS,
    SILVER_TEXT_ANNOTATIONS,
    SILVER_TEXT_SUMMARIES,
    SILVER_CONTENT_CLASSIFICATION,
)

# ── Key ────────────────────────────────────────────────────────────────────
KEY_COLUMNS: tuple[str, ...] = ("post_id", "platform")
"""Natural key of every silver enrichment table (platform — NEVER domain)."""

# ── Shared provenance block ────────────────────────────────────────────────
_PROVENANCE: dict[str, pl.DataType] = {
    "provider": pl.String,
    "model": pl.String,
    "prompt_hash": pl.String,
    "schema_version": pl.String,
    "run_id": pl.String,
    "input_modality": pl.String,
    "sampling_params_json": pl.String,
    "analysed_at": pl.Datetime("us", "UTC"),
    "conformed_at": pl.Datetime("us", "UTC"),
    "content_mime_type": pl.String,
    "derivation_version": pl.String,
}


def _schema(body: dict[str, pl.DataType]) -> dict[str, pl.DataType]:
    """Full table schema: key + provenance + body columns, in that order."""
    out: dict[str, pl.DataType] = {k: pl.String for k in KEY_COLUMNS}
    out.update(_PROVENANCE)
    out.update(body)
    return out


# ── Table schemas (JSON↔column mapping is 1:1; enums live in the registry) ─
TABLE_SCHEMAS: dict[str, dict[str, pl.DataType]] = {
    SILVER_VISUAL_ANNOTATIONS: _schema(
        {
            "face_present": pl.Boolean,
            "value_medium": pl.String,
            "brand_logos_json": pl.String,
            "text_overlay_present": pl.Boolean,
            "on_screen_claim": pl.Boolean,
        }
    ),
    SILVER_VISUAL_SUMMARIES: _schema(
        {
            "content_summary": pl.String,
            "image_summaries_json": pl.String,
        }
    ),
    SILVER_AUDIO_TRANSCRIPTS: _schema(
        {
            "transcript": pl.String,
            "transcript_status": pl.String,
            "audio_present": pl.Boolean,
            "asr_model": pl.String,
            "language": pl.String,
        }
    ),
    SILVER_TEXT_ANNOTATIONS: _schema(
        {
            "hook_content": pl.String,
            "hook_type": pl.String,
            "is_sponsored": pl.Boolean,
            "sponsorship_signal": pl.String,
            "claimed_results": pl.Boolean,
            "cta_type": pl.String,
            "audience_named": pl.Boolean,
            "value_depth": pl.String,
            "replicable_tactic": pl.String,
            "hashtag_strategy": pl.String,
            "evidence": pl.String,
            "brand_safety_json": pl.String,
        }
    ),
    SILVER_TEXT_SUMMARIES: _schema(
        {
            "transcript_summary": pl.String,
        }
    ),
    SILVER_CONTENT_CLASSIFICATION: _schema(
        {
            "domain": pl.String,
            "subdomain": pl.String,
            "topic": pl.String,
            "subtopic": pl.String,
            "is_educational": pl.Boolean,
            "is_actionable": pl.Boolean,
            "admiralty": pl.String,
            "content_type": pl.String,
            "style": pl.String,
            "format": pl.String,
            # Verbatim passthrough (bronze response_text == legacy result_json,
            # US-ESA-2 AC6 byte parity) — carried ON the silver table because
            # serving reads silver, never bronze.
            "result_json": pl.String,
        }
    ),
    SILVER_QUARANTINE: {
        "post_id": pl.String,
        "platform": pl.String,
        "workload": pl.String,
        "provider": pl.String,
        "model": pl.String,
        "prompt_hash": pl.String,
        "schema_version": pl.String,
        "run_id": pl.String,
        "reason_code": pl.String,
        "reason_detail": pl.String,
        "response_excerpt": pl.String,
        "quarantined_at": pl.Datetime("us", "UTC"),
        "derivation_version": pl.String,
    },
}

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
"""Bound for free-text annotation fields (hook_content, evidence, ...)."""

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


# ── Transcripts: no landing workload produces them yet ─────────────────────
TRANSCRIPT_WORKLOADS: frozenset[str] = frozenset()
"""Deliberately EMPTY: no bronze workload (visual / text / classification)
carries an ASR transcript today. ``silver_audio_transcripts`` is created and
typed, and its conform path exists (``_transcript_row``), but it is populated
only when a transcript workload is added to ``landing.WORKLOADS`` and
registered here — correct, explicit emptiness, never fabricated rows.
``transcript_status`` enum: no_audio_source | pending | done | empty_audio."""

SUPPORTED_CONFORM_WORKLOADS = frozenset(
    {
        landing.WORKLOAD_GROWTH_FACETS_VISUAL,
        landing.WORKLOAD_GROWTH_FACETS_TEXT,
        landing.WORKLOAD_CONTENT_CLASSIFICATION,
    }
)
"""Workloads this conform layer maps."""

_EXCERPT_CHARS = 500
"""How much of the verbatim response a quarantine row retains for triage."""


# ── Results ────────────────────────────────────────────────────────────────


@dataclass
class ConformResult:
    """Outcome of one deterministic conform run.

    ``tables`` maps each of the five silver table ids to its DataFrame
    (typed per TABLE_SCHEMAS, empty-but-typed when no source rows exist).
    ``quarantine`` carries every terminally-failed bronze row with its
    reason. ``counts`` summarizes: conformed / quarantined / skipped per
    workload — loud, queryable, never silently dropped.
    """

    tables: dict[str, pl.DataFrame] = field(default_factory=dict)
    quarantine: pl.DataFrame = field(
        default_factory=lambda: pl.DataFrame(schema=TABLE_SCHEMAS[SILVER_QUARANTINE])
    )
    counts: dict[str, int] = field(default_factory=dict)


# ── Helpers ────────────────────────────────────────────────────────────────


def table_path(table_id: str, root: str | os.PathLike[str] | None = None) -> Path:
    """Path to a conformed table's Parquet file (house ``lake.py`` pattern).

    Precondition: ``table_id`` is one of the SILVER_TABLES/QUARANTINE ids.
    Postcondition: ``<root>/<table_id>.parquet`` (root defaults to the silver
    lake root), parent directory exists.
    """
    if table_id not in TABLE_SCHEMAS:
        raise ValueError(f"unknown silver table id: {table_id!r}")
    if root is None:
        path = lake.silver_path(table_id)
    else:
        path = Path(os.fspath(root)) / f"{table_id}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _write_atomic(df: pl.DataFrame, path: Path) -> None:
    """Publish a table snapshot atomically (temp + rename), landing-style.

    A crash never leaves a half-written Parquet file; a write failure
    propagates (no silent partial publish — write-audit-publish).
    """
    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".parquet.tmp", dir=str(path.parent)
    )
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        df.write_parquet(tmp_path)
        os.replace(tmp_path, path)
    except BaseException:
        if tmp_path.exists():
            tmp_path.unlink()
        raise


def _empty_tables() -> dict[str, pl.DataFrame]:
    return {
        tid: pl.DataFrame(schema=TABLE_SCHEMAS[tid]) for tid in SILVER_TABLES
    }


def _latest_per_key(df: pl.DataFrame) -> pl.DataFrame:
    """Deterministically pick the latest row per (post_id, platform, workload).

    Bronze is append-only, so one (post_id, platform, workload) may carry
    several landed rows (re-runs under new prompt hashes / run ids). The
    selection is a pure function of the data: latest ``analysed_at``, then
    lexicographically-greatest ``run_id``, then ``prompt_hash`` — so a replay
    over the same bronze always yields the same row.
    """
    if df.is_empty():
        return df
    return (
        df.sort(["analysed_at", "run_id", "prompt_hash"])
        .unique(subset=["post_id", "platform", "workload"], keep="last")
    )


def _quarantine_row(
    bronze_row: Mapping[str, object],
    *,
    reason_code: str,
    reason_detail: str,
    now: datetime,
) -> dict[str, object]:
    """One loud quarantine record with bronze provenance retained verbatim."""
    text = bronze_row.get("response_text") or ""
    return {
        "post_id": bronze_row["post_id"],
        "platform": bronze_row["platform"],
        "workload": bronze_row["workload"],
        "provider": bronze_row["provider"],
        "model": bronze_row["model"],
        "prompt_hash": bronze_row["prompt_hash"],
        "schema_version": bronze_row["schema_version"],
        "run_id": bronze_row["run_id"],
        "reason_code": reason_code,
        "reason_detail": reason_detail,
        "response_excerpt": text[:_EXCERPT_CHARS],
        "quarantined_at": now,
        "derivation_version": DERIVATION_VERSION,
    }


def _provenance(bronze_row: Mapping[str, object], *, now: datetime) -> dict[str, object]:
    """Per-table provenance copied from the bronze row + conform stamps.

    Each pass owns its table, so its provenance can never be overwritten by
    another pass (per-pass provenance is structural, ADR-0011 §4).
    """
    return {
        "provider": bronze_row["provider"],
        "model": bronze_row["model"],
        "prompt_hash": bronze_row["prompt_hash"],
        "schema_version": bronze_row["schema_version"],
        "run_id": bronze_row["run_id"],
        "input_modality": bronze_row.get("input_modality"),
        "content_mime_type": bronze_row.get("content_mime_type"),
        "sampling_params_json": bronze_row.get("sampling_params_json"),
        "analysed_at": bronze_row["analysed_at"],
        "conformed_at": now,
        "derivation_version": DERIVATION_VERSION,
    }


def _json(value: object) -> str:
    """Deterministic JSON encoding for JSON-typed columns."""
    return json.dumps(value, sort_keys=True, ensure_ascii=False)


# ── Visual workload → annotations + summaries ──────────────────────────────


def _assemble(
    table_id: str, bronze_row: Mapping[str, object],
    body: dict[str, object], prov: dict[str, object],
) -> dict[str, object]:
    """Full row: key + provenance + body, in schema order."""
    out = {k: bronze_row[k] for k in KEY_COLUMNS}
    out.update(body)
    out.update(prov)
    return out

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
            "brand_logos_json": _json(facets["brand_logos"]),
            "text_overlay_present": facets["text_overlay_present"],
            "on_screen_claim": facets["on_screen_claim"],
        }
        summary_row = {
            "content_summary": summary,
            "image_summaries_json": (
                _json(image_summaries) if image_summaries is not None else None
            ),
        }
    prov = _provenance(bronze_row, now=now)
    ann_out = (
        _assemble(SILVER_VISUAL_ANNOTATIONS, bronze_row, annotation_row, prov)
        if annotation_row
        else None
    )
    sum_out = (
        _assemble(SILVER_VISUAL_SUMMARIES, bronze_row, summary_row, prov)
        if summary_row
        else None
    )
    return ann_out, sum_out, errors





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
        "brand_safety_json": _json(payload["brand_safety"]),
    }
    prov = _provenance(bronze_row, now=now)
    return _assemble(SILVER_TEXT_ANNOTATIONS, bronze_row, row, prov), []



def _transcript_row(
    bronze_row: Mapping[str, object],
    payload: dict,
    *,
    now: datetime,
) -> tuple[dict[str, object], list[str]]:
    """Conform path for a future transcript workload.

    Preconditions: ``payload`` is ``{"transcript": str, "language": str,
    "audio_present": bool}``; ``transcript_status`` must be one of the spec's
    enum values. NOT REACHABLE today (``TRANSCRIPT_WORKLOADS`` is empty) —
    kept here so adding a whisper-style workload is a one-line registration,
    and so the table's emptiness is a *declared* state, not an accident.
    """
    statuses = ("no_audio_source", "pending", "done", "empty_audio")
    errors: list[str] = []
    status = payload.get("transcript_status")
    transcript = payload.get("transcript")
    if not isinstance(transcript, str):
        errors.append(f"transcript: expected string, got {type(transcript).__name__}")
    if status not in statuses:
        errors.append(f"transcript_status: {status!r} not in enum {list(statuses)}")
    if errors:
        return {}, errors
    row = {
        "transcript": transcript,
        "transcript_status": status,
        "audio_present": bool(payload.get("audio_present", False)),
        "asr_model": bronze_row.get("model"),
        "language": payload.get("language", ""),
    }
    prov = _provenance(bronze_row, now=now)
    return _assemble(SILVER_AUDIO_TRANSCRIPTS, bronze_row, row, prov), []


# ── Classification workload → silver_content_classification ───────────────

MODEL_LEGACY_NULL = "legacy-unknown"
"""Sentinel for rows whose producing model was never recorded (ADR-0014 D5):
an honest "this result is verified, but the model name was never written" —
NOT a NULL and NOT a fabricated model name."""

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
"""The 10 classification body fields (contract: exactly these columns)."""


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


def _classification_body(payload: object) -> tuple[dict | None, list[str]]:
    """Validate a classification payload into the 10-column body.

    Accepts the dual-shape input (object or single-element array) the legacy
    serving views' ``$[0]`` fallback tolerated. Returns ``(body, errors)``;
    ``body is None`` means terminal failure (quarantine, never silent NULL).
    """
    if isinstance(payload, list):
        if len(payload) == 1 and isinstance(payload[0], dict):
            payload = payload[0]
        else:
            return None, [
                f"expected object, got array of {len(payload)}"
                if isinstance(payload, list)
                else "expected object"
            ]
    if not isinstance(payload, dict):
        return None, [f"expected object, got {type(payload).__name__}"]
    if not any(key in payload for key in CLASSIFICATION_BODY_KEYS):
        return None, [
            "missing required classification body keys "
            f"(none of {len(CLASSIFICATION_BODY_KEYS)} present)"
        ]
    return {key: payload.get(key) for key in CLASSIFICATION_BODY_KEYS}, []


def _conform_classification(
    bronze_row: Mapping[str, object],
    payload: object,
    *,
    now: datetime,
) -> tuple[dict[str, object], list[str]]:
    """Map one classification payload onto ``silver_content_classification``.

    The platform is the bronze row's ``platform`` KEY column — never the
    niche ``domain`` body column. ``model IS NULL`` carries the
    ``legacy-unknown`` sentinel (ADR-0014 D5), never a silent NULL.
    """

    body, errors = _classification_body(payload)
    if errors:
        return {}, errors
    model = bronze_row.get("model")
    if model is None:
        model = MODEL_LEGACY_NULL
    prov = _provenance(bronze_row, now=now)
    prov["model"] = model
    # Verbatim passthrough: the bronze response_text IS the legacy
    # result_json (US-ESA-2 AC6) — carried through byte-identically,
    # never re-serialized.
    body = {**body, "result_json": bronze_row["response_text"]}
    return _assemble(SILVER_CONTENT_CLASSIFICATION, bronze_row, body, prov), []



def conform(
    *,
    root: str | os.PathLike[str] | None = None,
    silver_root: str | os.PathLike[str] | None = None,
    now: datetime | None = None,
    n_media_by_post: Mapping[str, int] | None = None,
    conn: object | None = None,
) -> ConformResult:
    """Read bronze, conform + validate, publish the six silver tables.

    Preconditions:
    - ``root`` (bronze) is a lake root dir or None (→ bronze lake root);
      the SILVER root defaults to the silver lake root. ``silver_root`` is
      the destination root for the six tables + quarantine (defaults to
      ``root`` when given, else the silver lake root) — kept separate so a
      test/consumer never mixes bronze and silver directories.
    - ``n_media_by_post`` optionally maps post_id → media count (e.g. from
      ``silver_ig_posts.media_files``) for the carousel cross-field check.
      Deterministic metadata only — NEVER a network lookup.
    - ``conn`` (optional duckdb connection) receives the published tables
      via :func:`register_conformed`.

    Postconditions:
    - ZERO network/API calls — structurally impossible (import graph).
    - Each of the six tables is keyed ``(post_id, platform)`` and written
      atomically as a deterministic snapshot of bronze; re-running over the
      same bronze (same ``now``) is byte-identical (idempotent replay).
    - Every terminally-failed row lands in ``silver_enrichment_quarantine``
      with an attributable ``reason_code``; no silently-NULL conformed row,
      no swallowed exception. Failed landings (``ok=False``) are quarantined
      via ``provider_error`` — failure is READ from bronze, never inferred.
    - ``ConformResult.counts`` accounts for every bronze row
      (conformed + quarantined = total).

    Raises: any Polars/OS/JSON-encoding error propagates.
    """
    now = now or datetime.now(UTC)
    n_media_by_post = n_media_by_post or {}
    out_root = silver_root if silver_root is not None else root

    bronze = landing.read_responses(root)
    tables = _empty_tables()
    quarantine_rows: list[dict[str, object]] = []
    counts: dict[str, int] = dict.fromkeys(("conformed", "quarantined"), 0)

    candidates = _latest_per_key(bronze)
    for row in candidates.iter_rows(named=True):
        workload = row["workload"]

        if not row["ok"]:
            # Failed provider landing: READ from bronze's explicit ok column.
            quarantine_rows.append(
                _quarantine_row(
                    row,
                    reason_code=REASON_PROVIDER_ERROR,
                    reason_detail=row["error_message"] or "provider landing failed",
                    now=now,
                )
            )
            counts["quarantined"] += 1
            continue

        if workload not in SUPPORTED_CONFORM_WORKLOADS:
            quarantine_rows.append(
                _quarantine_row(
                    row,
                    reason_code=REASON_UNSUPPORTED_WORKLOAD,
                    reason_detail=f"workload {workload!r} has no conform mapping",
                    now=now,
                )
            )
            counts["quarantined"] += 1
            continue

        text = row["response_text"] or ""
        if not text.strip():
            quarantine_rows.append(
                _quarantine_row(
                    row,
                    reason_code=REASON_PARSE_ERROR,
                    reason_detail="empty response",
                    now=now,
                )
            )
            counts["quarantined"] += 1
            continue
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            quarantine_rows.append(
                _quarantine_row(
                    row,
                    reason_code=REASON_PARSE_ERROR,
                    reason_detail=f"invalid JSON: {exc}",
                    now=now,
                )
            )
            counts["quarantined"] += 1
            continue
        if not isinstance(payload, dict) and (
            workload != landing.WORKLOAD_CONTENT_CLASSIFICATION
        ):
            quarantine_rows.append(
                _quarantine_row(
                    row,
                    reason_code=REASON_PARSE_ERROR,
                    reason_detail=f"expected object, got {type(payload).__name__}",
                    now=now,
                )
            )
            counts["quarantined"] += 1
            continue

        if workload == landing.WORKLOAD_GROWTH_FACETS_VISUAL:
            ann_row, sum_row, errors = _conform_visual(
                row,
                payload,
                n_media_by_post.get(row["post_id"]),
                now=now,
            )
            if errors:
                quarantine_rows.append(
                    _quarantine_row(
                        row,
                        reason_code=classify_reason(errors),
                        reason_detail="; ".join(errors),
                        now=now,
                    )
                )
                counts["quarantined"] += 1
                continue
            tables[SILVER_VISUAL_ANNOTATIONS] = tables[
                SILVER_VISUAL_ANNOTATIONS
            ].vstack(pl.DataFrame([ann_row], schema=TABLE_SCHEMAS[SILVER_VISUAL_ANNOTATIONS]))
            tables[SILVER_VISUAL_SUMMARIES] = tables[
                SILVER_VISUAL_SUMMARIES
            ].vstack(pl.DataFrame([sum_row], schema=TABLE_SCHEMAS[SILVER_VISUAL_SUMMARIES]))
        elif workload == landing.WORKLOAD_GROWTH_FACETS_TEXT:
            text_row, errors = _conform_text(row, payload, now=now)
            if errors:
                quarantine_rows.append(
                    _quarantine_row(
                        row,
                        reason_code=classify_reason(errors),
                        reason_detail="; ".join(errors),
                        now=now,
                    )
                )
                counts["quarantined"] += 1
                continue
            tables[SILVER_TEXT_ANNOTATIONS] = tables[
                SILVER_TEXT_ANNOTATIONS
            ].vstack(pl.DataFrame([text_row], schema=TABLE_SCHEMAS[SILVER_TEXT_ANNOTATIONS]))
        elif workload == landing.WORKLOAD_CONTENT_CLASSIFICATION:
            cls_row, errors = _conform_classification(row, payload, now=now)
            if errors:
                quarantine_rows.append(
                    _quarantine_row(
                        row,
                        reason_code=classify_reason(errors),
                        reason_detail="; ".join(errors),
                        now=now,
                    )
                )
                counts["quarantined"] += 1
                continue
            tables[SILVER_CONTENT_CLASSIFICATION] = tables[
                SILVER_CONTENT_CLASSIFICATION
            ].vstack(
                pl.DataFrame(
                    [cls_row], schema=TABLE_SCHEMAS[SILVER_CONTENT_CLASSIFICATION]
                )
            )
        counts["conformed"] += 1

    quarantine = (
        pl.DataFrame(quarantine_rows, schema=TABLE_SCHEMAS[SILVER_QUARANTINE])
        if quarantine_rows
        else pl.DataFrame(schema=TABLE_SCHEMAS[SILVER_QUARANTINE])
    )

    # Publish: each table atomically, plus the quarantine surface.
    for tid in SILVER_TABLES:
        _write_atomic(tables[tid], table_path(tid, out_root))
    _write_atomic(quarantine, table_path(SILVER_QUARANTINE, out_root))

    if conn is not None:
        register_conformed(conn, out_root)

    if quarantine.height:
        logger.warning(
            "Conform quarantined %d bronze row(s): %s",
            quarantine.height,
            quarantine.group_by("reason_code")
            .len()
            .sort("reason_code")
            .select(pl.concat_str(["reason_code", pl.lit("×"), pl.col("len")]))
            .to_series()
            .to_list(),
        )
    logger.info(
        "Conform done: conformed=%d quarantined=%d",
        counts["conformed"],
        counts["quarantined"],
    )
    return ConformResult(tables=tables, quarantine=quarantine, counts=counts)


def register_conformed(conn, root: str | os.PathLike[str] | None = None) -> None:
    """Register the published Parquet tables in DuckDB for SQL queryability.

    House pattern: Parquet is the lake storage, DuckDB the authoritative
    queryable state (``CREATE OR REPLACE TABLE ... AS SELECT * FROM
    read_parquet(...)``) — consistent with how silver/gold assets are
    handled. Reads only files this run published.
    """
    for tid in (*SILVER_TABLES, SILVER_QUARANTINE):
        path = table_path(tid, root)
        conn.execute(
            f"CREATE OR REPLACE TABLE {tid} AS "
            f"SELECT * FROM read_parquet('{path.as_posix()}')"
        )


def read_table(
    table_id: str, root: str | os.PathLike[str] | None = None
) -> pl.DataFrame:
    """Read one conformed table (typed; empty-but-typed when absent)."""
    path = table_path(table_id, root)
    if not path.exists():
        return pl.DataFrame(schema=TABLE_SCHEMAS[table_id])
    return pl.read_parquet(path)
