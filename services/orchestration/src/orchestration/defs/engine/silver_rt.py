"""Shared silver-build runtime — generic over every domain's payload.

`conform` is a pure function of bronze: given the landed verbatim responses it
produces the six validated silver tables. Nothing here knows what a payload
*means* — the per-table mappings live in the domain packages
(`ig_enriched/slv/{classification,visual,text,audio}.py`) and the validation
vocabulary in `ig_enriched/slv/quarantine.py`. That split is what makes a
schema or mapping change a deterministic replay of bronze rather than a re-bill.

The publisher (`silver_enrichment`, registered here next to the runtime it
drives) reads every landed bronze row, derives each silver table, writes it
atomically, and registers it in DuckDB. Tables are keyed `(post_id, platform)`
and carry their OWN provenance columns — `provider / model / prompt_hash /
schema_version / run_id` plus `conformed_at` and `derivation_version` — so state
produced under old logic is detectable rather than silently trusted.

Terminal failures land LOUDLY in `silver_enrichment_quarantine` with a
machine-readable `reason_code` and the full error detail. A malformed row never
produces a silently-NULL conformed row.
"""

import json
import logging
import os
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from orchestration.defs.engine import landing
from orchestration.defs.ig_enriched.slv.classification import _conform_classification
from orchestration.defs.ig_enriched.slv.prompts import CURRENT_PROMPT_HASH  # noqa: F401
from orchestration.defs.ig_enriched.slv.quarantine import (
    REASON_PARSE_ERROR,
    REASON_PROVIDER_ERROR,
    REASON_UNSUPPORTED_WORKLOAD,
    classify_reason,
)
from orchestration.defs.ig_enriched.slv.text import _conform_text
from orchestration.defs.ig_enriched.slv.visual import _conform_visual
from orchestration.defs.platform import paths as lake

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


def _schema(body: dict[str, pl.DataType]) -> dict[str, pl.DataType]:
    """Full table schema: key + provenance + body columns, in that order."""
    out: dict[str, pl.DataType] = {k: pl.String for k in KEY_COLUMNS}
    out.update(_PROVENANCE)
    out.update(body)
    return out


SUPPORTED_CONFORM_WORKLOADS = frozenset(
    {
        landing.WORKLOAD_GROWTH_FACETS_VISUAL,
        landing.WORKLOAD_GROWTH_FACETS_TEXT,
        landing.WORKLOAD_CONTENT_CLASSIFICATION,
    }
)
"""Workloads this conform layer maps."""

_EXCERPT_CHARS = 500


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


def _assemble(
    table_id: str, bronze_row: Mapping[str, object],
    body: dict[str, object], prov: dict[str, object],
) -> dict[str, object]:
    """Full row: key + provenance + body, in schema order."""
    out = {k: bronze_row[k] for k in KEY_COLUMNS}
    out.update(body)
    out.update(prov)
    return out


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

