"""Bronze landing zone for enrichment responses — `bronze_enrichment_raw`.

Verbatim, immutable, append-only capture of every external model response
(ADR-0011 §3; `docs/architecture/bronze-schema.md` producer 3). Parsing,
validation, and conform are the silver layer's job (Phase 4) — this module
only lands `response_text` as-observed, so a schema or mapping change is a
deterministic silver replay, never a re-call of the paid model.

Contract:
- Storage: one Parquet file per lake root under `data/lake/bronze/`, written
  with Polars (bronze never touches DuckDB or the I/O manager).
- Natural key: `(post_id, platform, workload, prompt_hash, run_id)`.
- Immutable + append-only: rows are never updated or overwritten.
- Idempotent: landing a key that already exists is a no-op (0 rows), not a
  duplicate.
- Explicit `ok` column: failure is READ from the landed record, never
  inferred from the absence of a conformed row (spike finding S5).
- A write failure raises — no silent partial landing.
"""

from __future__ import annotations

import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from orchestration.defs.platform import paths as lake

# ── Table identity ─────────────────────────────────────────────────────────

DATASET_ID = "bronze_enrichment_raw"
"""Lake dataset id — file name follows the `lake.py` convention:
`<dataset_id>.parquet` under the bronze root."""

# ── Workloads (named constants, never loose strings) ───────────────────────

WORKLOAD_GROWTH_FACETS_VISUAL = "growth-facets-visual"
"""Visual pass: media (images / video frames) → visual facets + summaries."""

WORKLOAD_GROWTH_FACETS_TEXT = "growth-facets-text"
"""Text pass: caption + transcript → text-layer facets + summary."""

WORKLOAD_CONTENT_CLASSIFICATION = "content-classification"
"""Classification pass: taxonomy + educational/actionable + admiralty."""

WORKLOADS: frozenset[str] = frozenset(
    {
        WORKLOAD_GROWTH_FACETS_VISUAL,
        WORKLOAD_GROWTH_FACETS_TEXT,
        WORKLOAD_CONTENT_CLASSIFICATION,
    }
)

KNOWN_PROVIDERS: frozenset[str] = frozenset({"service_backed", "qwen", "none"})
"""Providers that may land into the LIVE default lake root. Any other value
(`fake`, a typo, an experimental adapter) is refused with root=None — test
fixtures leaking provider='fake' rows into the real bronze lake is exactly
the defect this guard makes structurally impossible.

A provider value is the ADAPTER's registered name (`ProviderAdapter.name`),
which since the Gemini retirement names the SEAM, not the vendor: the qwen
service reaches the lake as ``service_backed``, not ``qwen``. ``qwen`` is kept
only for rows already landed under that name.

A new real provider is added here explicitly, alongside its producer. This
list drifting from the adapter registry is not hypothetical: `service_backed`
was absent for the whole life of the post-retirement adapter, so every harvest
was refused at landing while submit, poll and the entire test suite stayed
green. `test_landing.py`'s
``test_every_registered_adapter_may_land_in_the_live_root`` now pins the two
together so the next rename fails in CI instead of in production."""

# ── Natural key ────────────────────────────────────────────────────────────

KEY_COLUMNS: tuple[str, ...] = (
    "post_id",
    "platform",
    "workload",
    "prompt_hash",
    "run_id",
)
"""Immutable natural key — idempotency and append-only dedup are by this
five-column tuple; rows are never updated or overwritten."""

# ── Schema ─────────────────────────────────────────────────────────────────

SCHEMA: dict[str, pl.DataType] = {
    # Key
    "post_id": pl.String,
    "platform": pl.String,
    "workload": pl.String,
    "prompt_hash": pl.String,
    "run_id": pl.String,
    # Provenance
    "provider": pl.String,
    "model": pl.String,
    "schema_version": pl.String,
    "landing_at": pl.Datetime("us", "UTC"),
    "analysed_at": pl.Datetime("us", "UTC"),
    # Status — read explicitly, never inferred (S5)
    "ok": pl.Boolean,
    "error_message": pl.String,
    # Verbatim payload + request capture (no parsing at this layer)
    "response_text": pl.String,
    "request_echo_json": pl.String,
    "input_modality": pl.String,
    "sampling_params_json": pl.String,
}
"""Landed record shape. `response_text` is the verbatim provider body;
`landing_at` is the landing timestamp written by this module."""


def response_path(root: str | os.PathLike[str] | None = None) -> Path:
    """Path to the landing Parquet file.

    Precondition: `root` (if given) is a lake root directory path or None.
    Postcondition: returned path is `<root>/<DATASET_ID>.parquet` (root
    defaults to the bronze lake root per `lake.py`) and the parent
    directory exists.
    """
    if root is None:
        return lake.bronze_path(DATASET_ID)
    path = Path(os.fspath(root)) / f"{DATASET_ID}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def read_responses(root: str | os.PathLike[str] | None = None) -> pl.DataFrame:
    """Read all landed records.

    Preconditions: `root` None or a lake root directory. No lake invariants
    are checked — this is a plain typed read.

    Postconditions: returns a DataFrame with the SCHEMA shape; empty (but
    correctly typed) when nothing has landed. Raises on a corrupt file —
    a read failure is never swallowed.
    """
    path = response_path(root)
    if not path.exists():
        return pl.DataFrame(schema=SCHEMA)
    return pl.read_parquet(path)


def land_response(
    *,
    post_id: str,
    platform: str,
    workload: str,
    provider: str,
    model: str,
    prompt_hash: str,
    schema_version: str,
    run_id: str,
    response_text: str,
    ok: bool,
    error_message: str | None = None,
    analysed_at: datetime | None = None,
    request_echo_json: str | None = None,
    input_modality: str | None = None,
    sampling_params_json: str | None = None,
    root: str | os.PathLike[str] | None = None,
) -> int:
    """Land ONE provider response verbatim; idempotent by the natural key.

    Preconditions:
    - `workload` is one of the WORKLOAD constants.
    - `response_text` is the verbatim provider body — no parsing, no field
      extraction, no normalization at this layer.
    - `ok=False` carries a non-null `error_message`.

    Postconditions:
    - Returns the number of rows landed (1 = appended, 0 = this key already
      landed — a no-op, never a duplicate).
    - Existing rows are unchanged; the write is atomic (temp + rename) so a
      crash never leaves a half-written file. A write failure raises.

    Raises: ValueError on an unknown workload or a failed landing without
    `error_message`; any Polars/OS error propagates.
    """
    if workload not in WORKLOADS:
        raise ValueError(
            f"unknown workload {workload!r}; expected one of {sorted(WORKLOADS)}"
        )
    if root is None and provider not in KNOWN_PROVIDERS:
        raise ValueError(
            f"refusing to land provider {provider!r} into the LIVE default "
            "lake root: not in KNOWN_PROVIDERS "
            f"{sorted(KNOWN_PROVIDERS)} — pass an explicit tmp/test root"
        )
    if not ok and error_message is None:
        raise ValueError(
            "landing a failed response (ok=False) requires error_message"
        )

    existing = read_responses(root)

    # Idempotency by natural key: an already-landed key is a no-op.
    key = {
        "post_id": post_id,
        "platform": platform,
        "workload": workload,
        "prompt_hash": prompt_hash,
        "run_id": run_id,
    }
    dup_mask = pl.lit(True)
    for col in KEY_COLUMNS:
        dup_mask = dup_mask & (pl.col(col) == key[col])
    if existing.filter(dup_mask).height > 0:
        return 0

    landing_at = datetime.now(UTC)
    analysed = analysed_at if analysed_at is not None else landing_at
    row = pl.DataFrame(
        {
            "post_id": [post_id],
            "platform": [platform],
            "workload": [workload],
            "prompt_hash": [prompt_hash],
            "run_id": [run_id],
            "provider": [provider],
            "model": [model],
            "schema_version": [schema_version],
            "landing_at": [landing_at],
            "analysed_at": [analysed],
            "ok": [ok],
            "error_message": [error_message],
            "response_text": [response_text],
            "request_echo_json": [request_echo_json],
            "input_modality": [input_modality],
            "sampling_params_json": [sampling_params_json],
        },
        schema=SCHEMA,
    )
    combined = existing.vstack(row)

    path = response_path(root)
    # Atomic publish: temp file in the same directory, then rename, so a
    # consumer's read never sees a half-written Parquet file.
    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{DATASET_ID}.", suffix=".parquet.tmp", dir=str(path.parent)
    )
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        combined.write_parquet(tmp_path)
        os.replace(tmp_path, path)
    except BaseException:
        # Clean the temp file on any failure; the landed file is untouched
        # and the error propagates (no silent partial landing).
        if tmp_path.exists():
            tmp_path.unlink()
        raise
    return 1
