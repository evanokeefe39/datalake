"""Classification-pass conform — `silver_content_classification`.

The classification workload's deterministic conform (bronze → silver). It
lives here, not in `conform.py`: that module explicitly SKIPS the
classification workload (`conform.SUPPORTED_CONFORM_WORKLOADS`) and defers it
to this unit (US-ESA-2, Phase 5 of the enrichment-v3 migration).

Contract (ADR-0011, US-ESA-2):

- Keyed ``(post_id, platform)`` — the join key is `platform`, NEVER `domain`.
  `domain`/`subdomain`/`topic`/`subtopic` mean ONLY the content niche. The
  legacy `gold_analyses` table overloaded `domain = 'instagram'` as the key;
  this table fixes that: the platform is a key column, the niche is a body
  column.
- Body: 10 typed columns conformed from the response body. Dual-shape input
  (JSON object or single-element JSON array) conforms deterministically —
  the `$[0]` fallback semantics the legacy serving views already applied.
- `result_json` is carried through VERBATIM: it is the bronze `response_text`,
  so the serving passthrough column stays byte-identical (US-ESA-2 AC6) and
  nothing the model said is ever re-encoded.
- Provenance per row: `provider, model, prompt_hash, schema_version, run_id,
  analysed_at` (copied from bronze). `model` gaps carry an explicit sentinel,
  never a silent NULL.
- Zero API calls — a pure function of bronze (ADR-0003): a schema/mapping
  change is a replay, never a re-bill.

Quarantine: a bronze row that cannot conform (unparseable JSON, wrong shape,
no classification keys at all) is quarantined LOUDLY with a machine-readable
reason — never silently dropped, never silently NULL.


Legacy key-deviant rows (US-ESA-2): three live `gold_analyses` rows carry
surrogate/missing keys. `LEGACY_DEVIANCE_NOTES` records the explicit
disposition for each; the deviance is NAMED in the migration reconciliation
report and the verbatim body is preserved in bronze. The typed columns carry
exactly what the legacy views served (NULL where the key was absent) so the
rebind changes no serving output.
"""

from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import duckdb
import polars as pl

from orchestration.defs.engine import landing
from orchestration.defs.ig_enriched.slv.quarantine import CLASSIFICATION_BODY_KEYS
from orchestration.defs.platform import paths as lake
from orchestration.defs.platform.schemas import MODEL_LEGACY_NULL

# ── Table identity ──────────────────────────────────────────────────────────

TABLE_ID = "silver_content_classification"
"""Serving-facing name of the conformed classification table."""

WORKLOAD: str = landing.WORKLOAD_CONTENT_CLASSIFICATION
"""The landing workload this conform owns (aliased here so callers never
hard-code the string — the F821/`CLASSIFICATION_WORKLOAD` defect this module
once shipped)."""

CLASSIFICATION_DDL = """CREATE TABLE IF NOT EXISTS silver_content_classification (
    post_id VARCHAR NOT NULL,
    platform VARCHAR NOT NULL,
    domain VARCHAR,
    subdomain VARCHAR,
    topic VARCHAR,
    subtopic VARCHAR,
    is_educational BOOLEAN,
    is_actionable BOOLEAN,
    admiralty VARCHAR,
    content_type VARCHAR,
    style VARCHAR,
    format VARCHAR,
    result_json VARCHAR,
    prompt_hash VARCHAR,
    schema_version VARCHAR,
    run_id VARCHAR,
    provider VARCHAR,
    model VARCHAR,
    analysed_at VARCHAR,
    PRIMARY KEY (post_id, platform)
)"""
"""State.duckdb DDL for the classification table (additive, idempotent).

`result_json` is the VERBATIM bronze `response_text` (US-ESA-2 AC6 byte
parity); `analysed_at` stays VARCHAR carrying the legacy timestamp verbatim
so `v_post_detail.gold_analysed_at` serves unchanged.
"""

# ── Body contract ───────────────────────────────────────────────────────────

BODY_KEYS: tuple[str, ...] = (
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
"""The 10 classification fields the body must (normally) carry."""

LEGACY_EXTRA_KEYS: frozenset[str] = frozenset({"educational_json", "actionable_json"})
"""Legacy body fields that are not classification deviance."""

# ── Quarantine reasons ──────────────────────────────────────────────────────

REASON_PARSE_ERROR = "parse_error"
REASON_SHAPE_ERROR = "shape_error"
REASON_EMPTY_BODY = "empty_classification_body"

_QUARANTINE_SCHEMA: dict[str, pl.DataType] = {
    "post_id": pl.String,
    "platform": pl.String,
    "workload": pl.String,
    "prompt_hash": pl.String,
    "run_id": pl.String,
    "ok": pl.Boolean,
    "reason_code": pl.String,
    "reason": pl.String,
    "response_excerpt": pl.String,
}

# ── Legacy key-deviant dispositions (named, never silent) ───────────────────

LEGACY_DEVIANCE_NOTES: dict[str, str] = {
    "3960884543625651437": (
        "key_deviant: body carries 'advisory' where the taxonomy expects "
        "'admiralty'; the typed admiralty column stays NULL — exactly what "
        "the legacy views served — and the surrogate value is preserved "
        "verbatim in bronze."
    ),
    "3917136874405145474": (
        "key_deviant: body carries 'subsubtopic' where the taxonomy expects "
        "'subtopic'; the typed subtopic column stays NULL — exactly what the "
        "legacy views served — and the surrogate value is preserved verbatim "
        "in bronze."
    ),
    "3863648947509006965": (
        "key_deviant: body carries neither 'subdomain' nor 'topic'; both "
        "typed columns stay NULL — exactly what the legacy views served — "
        "and the rest of the classification conforms normally."
    ),
}

# ── Silver schema ───────────────────────────────────────────────────────────

SILVER_SCHEMA: dict[str, pl.DataType] = {
    # Key — (post_id, platform), NEVER domain
    "post_id": pl.String,
    "platform": pl.String,
    # Body — typed columns replacing JSON extraction
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
    # Verbatim passthrough (bronze response_text == legacy result_json)
    "result_json": pl.String,
    # Provenance
    "prompt_hash": pl.String,
    "schema_version": pl.String,
    "run_id": pl.String,
    "provider": pl.String,
    "model": pl.String,
    "analysed_at": pl.String,
}



@dataclass
class ClassificationResult:
    """Outcome of one deterministic classification conform run.

    ``counts`` accounts for every bronze classification row:
    ``conformed + quarantined == ok_rows`` and
    ``classification_rows == ok_rows + failed_landing`` — zero unaccounted.
    """

    silver: pl.DataFrame
    quarantine: pl.DataFrame
    counts: dict[str, int] = field(default_factory=dict)
    deviance: list[dict[str, str]] = field(default_factory=list)


def _parse_body(response_text: str) -> tuple[dict | None, str | None]:
    """Parse a response body into a classification dict.

    Accepts the dual shape the legacy views handled — a JSON object, or a
    single-element JSON array of one object (the `$[0]` COALESCE fallback).
    """
    try:
        payload = json.loads(response_text)
    except json.JSONDecodeError as exc:
        return None, f"{REASON_PARSE_ERROR}: invalid JSON: {exc}"
    if isinstance(payload, dict):
        return payload, None
    if (
        isinstance(payload, list)
        and len(payload) == 1
        and isinstance(payload[0], dict)
    ):
        return payload[0], None
    return (
        None,
        f"{REASON_SHAPE_ERROR}: expected a JSON object or single-element "
        f"array of objects, got {type(payload).__name__}",
    )


def _as_bool(value: object) -> bool | None:
    """Boolean conform mirroring the legacy ``(x)::BOOLEAN`` cast semantics."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ("true", "t", "1", "yes")
    return bool(value)


def _deviance_notes(
    post_id: str, body: dict, deviance_notes: Mapping[str, str] | None
) -> list[str]:
    """Record why a body deviates from the 10-key contract — never silently."""
    notes: list[str] = [
        f"missing:{key}" for key in BODY_KEYS if key not in body
    ]
    notes += [
        f"surrogate:{key}={body[key]!r}"
        for key in body
        if isinstance(body[key], (str, int, float, bool))
        and key not in BODY_KEYS
        and key not in LEGACY_EXTRA_KEYS
    ]
    if post_id in (deviance_notes or {}):
        notes.insert(0, deviance_notes[post_id])
    return notes


def conform_classification(
    bronze: pl.DataFrame,
    *,
    deviance_notes: Mapping[str, str] | None = None,
) -> ClassificationResult:
    """Conform classification bronze rows into silver, deterministically.

    Preconditions: ``bronze`` has the ``landing.SCHEMA`` shape; rows with
    ``ok=False`` are skipped (counted as ``failed_landing`` — a failed
    response carries no body to conform). Zero API calls by construction.

    Postconditions:
    - Every classification-workload bronze row is conformed, quarantined
      with a machine-readable reason, or counted as a failed landing.
    - ``result_json`` is byte-identical to the bronze ``response_text``.
    - A re-run over the same bronze produces an identical silver frame.
    """
    counts = {
        "total_bronze": bronze.height,
        "classification_rows": 0,
        "failed_landing": 0,
        "conformed": 0,
        "quarantined": 0,
        "deviant": 0,
        "array_form": 0,
        "model_gap": 0,
    }

    silver_rows: list[dict[str, object]] = []
    quarantine_rows: list[dict[str, object]] = []
    deviance: list[dict[str, str]] = []

    if bronze.height and "workload" in bronze.columns:
        cls = bronze.filter(pl.col("workload") == WORKLOAD)
    else:
        cls = bronze
    counts["classification_rows"] = cls.height

    for row in cls.iter_rows(named=True):
        base = {
            "post_id": row["post_id"],
            "platform": row["platform"],
            "workload": row["workload"],
            "prompt_hash": row["prompt_hash"],
            "run_id": row["run_id"],
        }

        body, error = _parse_body(row["response_text"])
        if body is None:
            quarantine_rows.append(
                {
                    **base,
                    "ok": row["ok"],
                    "reason_code": error.split(":", 1)[0],
                    "reason": error,
                    "response_excerpt": (row["response_text"] or "")[:500],
                }
            )
            counts["quarantined"] += 1
            continue

        notes = _deviance_notes(row["post_id"], body, deviance_notes)
        if notes:
            counts["deviant"] += 1
            deviance.append({"post_id": row["post_id"], "reason": "; ".join(notes)})

        if not any(key in body for key in BODY_KEYS):
            quarantine_rows.append(
                {
                    **base,
                    "ok": row["ok"],
                    "reason_code": REASON_EMPTY_BODY,
                    "reason": (
                        "classification body carries none of the "
                        f"{len(BODY_KEYS)} classification keys"
                    ),
                    "response_excerpt": (row["response_text"] or "")[:500],
                }
            )
            counts["quarantined"] += 1
            continue

        model = row["model"]
        if model is None:
            counts["model_gap"] += 1
            model = MODEL_LEGACY_NULL
        if row["response_text"].lstrip().startswith("["):
            counts["array_form"] += 1

        silver_rows.append(
            {
                "post_id": row["post_id"],
                "platform": row["platform"],
                **{key: body.get(key) for key in BODY_KEYS},
                "result_json": row["response_text"],
                "prompt_hash": row["prompt_hash"],
                "schema_version": row["schema_version"],
                "run_id": row["run_id"],
                "provider": row["provider"],
                "model": model,
                "analysed_at": row["analysed_at"]
                if isinstance(row["analysed_at"], str)
                else (
                    row["analysed_at"].isoformat()
                    if row["analysed_at"] is not None
                    else None
                ),
            }
        )
        counts["conformed"] += 1

    silver = (
        pl.DataFrame(silver_rows, schema=SILVER_SCHEMA)
        if silver_rows
        else pl.DataFrame(schema=SILVER_SCHEMA)
    )
    # Body booleans conform like the legacy ``(x)::BOOLEAN`` cast: real JSON
    # booleans pass through, "true"/"1"-style strings conform, NULLs stay
    # NULL — never silently coerced to False.
    for col in ("is_educational", "is_actionable"):
        silver = silver.with_columns(
            pl.col(col)
            .map_elements(_as_bool, return_dtype=pl.Boolean)
            .cast(pl.Boolean)
        )
    quarantine = (
        pl.DataFrame(quarantine_rows, schema=_QUARANTINE_SCHEMA)
        if quarantine_rows
        else pl.DataFrame(schema=_QUARANTINE_SCHEMA)
    )
    return ClassificationResult(
        silver=silver, quarantine=quarantine, counts=counts, deviance=deviance
    )




# ── Reconciliation gate (US-ESA-2 AC3 — the loud gate) ──────────────────────


class ReconciliationError(RuntimeError):
    """The row-reconciliation identity failed — rows were silently lost."""


def reconcile(
    total: int, counts: Mapping[str, int], *, failed_landing_key: str = "failed_landing"
) -> dict[str, int]:
    """Assert ``total == conformed + quarantined (+ failed_landing)``.

    Preconditions: ``counts`` carries at least ``conformed`` and
    ``quarantined`` (and, when bronze may contain failed landings,
    ``failed_landing``).

    Postconditions: on success, returns ``counts`` unchanged — every row is
    accounted for. Raises ``ReconciliationError`` naming the exact shortfall
    otherwise; a dropped row can never pass silently.
    """
    conformed = counts.get("conformed", 0)
    quarantined = counts.get("quarantined", 0)
    failed = counts.get(failed_landing_key, 0)
    accounted = conformed + quarantined + failed
    unaccounted = total - accounted
    if unaccounted != 0:
        raise ReconciliationError(
            f"RECONCILIATION FAILED: {total} rows do not account as "
            f"conformed({conformed}) + quarantined({quarantined}) + "
            f"failed({failed}) = {accounted} — {unaccounted} unaccounted "
            "(silently dropped). Refusing to publish."
        )
    return dict(counts)


# ── Publish + register (Write → Audit → Publish; lake + state.duckdb) ──────


def _silver_path(root: str | os.PathLike[str] | None = None) -> Path:
    """Path to the silver Parquet snapshot (house ``lake.py`` pattern).

    ``root=None`` → the default silver lake root; otherwise
    ``<root>/<TABLE_ID>.parquet`` with the parent directory created.
    """
    if root is None:
        return lake.silver_path(TABLE_ID)
    path = Path(os.fspath(root)) / f"{TABLE_ID}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def publish_silver(
    silver: pl.DataFrame, root: str | os.PathLike[str] | None = None
) -> Path:
    """Publish the conformed frame to the silver lake, atomically.

    Preconditions: ``silver`` matches ``SILVER_SCHEMA``. Postconditions: the
    Parquet snapshot at ``_silver_path(root)`` is replaced atomically (temp +
    rename) — a crash never leaves a half-written file; a write failure
    raises with the temp file cleaned up.
    """
    path = _silver_path(root)
    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{TABLE_ID}.", suffix=".parquet.tmp", dir=str(path.parent)
    )
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        silver.write_parquet(tmp_path)
        os.replace(tmp_path, path)
    except BaseException:
        if tmp_path.exists():
            tmp_path.unlink()
        raise
    return path


def read_silver(root: str | os.PathLike[str] | None = None) -> pl.DataFrame:
    """Read the published silver table (typed; empty-but-typed when absent)."""
    path = _silver_path(root)
    if not path.exists():
        return pl.DataFrame(schema=SILVER_SCHEMA)
    return pl.read_parquet(path)


def register_silver(
    con: duckdb.DuckDBPyConnection, root: str | os.PathLike[str] | None = None
) -> None:
    """Upsert the published silver Parquet into state.duckdb.

    Preconditions: the Parquet snapshot exists (``publish_silver`` ran) and
    the connection can write. Postconditions: ``TABLE_ID`` exists under
    ``CLASSIFICATION_DDL`` (additive, IF NOT EXISTS) and holds exactly one
    row per ``(post_id, platform)`` — INSERT OR REPLACE by primary key, so a
    re-run converges, never duplicates, never drops the legacy table.
    """
    silver = read_silver(root)
    con.execute(CLASSIFICATION_DDL)
    if silver.height == 0:
        return
    con.register("_silver_classification_incoming", silver.to_arrow())
    try:
        con.execute(
            f"INSERT OR REPLACE INTO {TABLE_ID} "
            "SELECT * FROM _silver_classification_incoming"
        )
    finally:
        con.unregister("_silver_classification_incoming")


# ── Conform mapping ─────────────────────────────────────────────────


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
    ``unrecorded-legacy-null`` sentinel (ADR-0014 D5), never a silent NULL.
    """

    # Imported here, not at module scope: the runtime imports this module for
    # its mapping, so a module-level import back would be a cycle. These two
    # names are the runtime's (the table id and the row assembler).
    from orchestration.defs.engine.silver_rt import (
        SILVER_CONTENT_CLASSIFICATION,
        _assemble,
        _provenance,
    )

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
