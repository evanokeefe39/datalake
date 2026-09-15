"""Transcript payload mapping — `silver_audio_transcripts`.

No landing workload produces transcripts yet, so `TRANSCRIPT_WORKLOADS` is
deliberately empty: correct emptiness is explicit, never fabricated. The table
and its mapping exist so the path is ready when a transcription workload lands.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime

from orchestration.defs.ig_enriched.slv.schemas import validate_text_facets  # noqa: F401

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
    prov = _rt()._provenance(bronze_row, now=now)
    return _rt()._assemble(_rt().SILVER_AUDIO_TRANSCRIPTS, bronze_row, row, prov), []


# ── Transcripts: no landing workload produces them yet ─────────────────────
TRANSCRIPT_WORKLOADS: frozenset[str] = frozenset()
"""Deliberately EMPTY: no bronze workload (visual / text / classification)
carries an ASR transcript today. ``silver_audio_transcripts`` is created and
typed, and its conform path exists (``_transcript_row``), but it is populated
only when a transcript workload is added to ``landing.WORKLOADS`` and
registered here — correct, explicit emptiness, never fabricated rows.
``transcript_status`` enum: no_audio_source | pending | done | empty_audio."""
