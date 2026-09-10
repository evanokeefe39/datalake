"""Dagster-native gemini-batch harvest — the #24 fix.

Phase 1 of the batch-native-enrichment migration (ADR-0007): a cursor-based
**sensor** discovers batch jobs with persisted ``gemini_batch_name`` chunks,
polls their remote state via the batch verbs
(``gemini_batch.poll``/``job_state``/``is_terminal``), and issues a RunRequest
for the short **harvest job** when a chunk reaches a terminal state. The
harvest run retrieves terminal chunks, applies results to ``gold_analyses``
with the idempotent ``write_gold`` upsert contract
(``ON CONFLICT (post_id, domain)``, ordering guard on ``analysed_at``), and
closes the queue bookkeeping.

Design notes:
- Job names persist in ``ops.sqlite`` (``batch_jobs.gemini_batch_name``), so
  the sensor survives daemon restarts — the cursor only caches poll results
  to keep ticks cheap; re-discovery comes from the SQLite row, never memory.
- The harvest op owns the retrieval semantics (per-item fail/dead-letter
  routing, attempts-preserving resubmit on job failure, mark_complete when
  nothing remains) — after Phase 5 it is the only consumer of terminal
  chunks.
- ADR-0008 seam: the ONLY API calls (poll/retrieve) happen inside this
  tagged enrichment boundary. Silver onward stays hermetic.

Sensor default status is STOPPED (not RUNNING): every tick polls the live
Gemini API for each in-flight chunk, so enabling it is a deliberate,
quota-bearing decision until the Phase 1 External Integration Gate smoke
passes. Flip to RUNNING via the UI once batch is proven.
"""
from __future__ import annotations

import hashlib
import json
import logging

from dagster import (
    AssetMaterialization,
    DefaultSensorStatus,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    job,
    op,
    sensor,
)

from datalake.defs.common.resources import DuckDBResource, GeminiResource, SQLiteResource
from datalake.defs.enrichment import gemini_batch
from datalake.defs.enrichment.analysis import write_gold
from datalake.defs.enrichment.assets import ensure_gold_analyses
from datalake.defs.enrichment.batch import (
    MAX_ATTEMPTS,
    _ensure_schema,
    _now_iso,
    batch_progress,
    complete_item,
    fail_item,
    mark_complete,
    set_gemini_batch_status,
)

logger = logging.getLogger("enrichment.harvest")

# Per-chunk statuses that mean "the harvest run has already handled this chunk"
# — aligned to the '|'-joined gemini_batch_name blob.
_HANDLED = {"RETRIEVED", "JOB_FAILED"}

# Cheap-tick bound: at most this many remote polls per sensor tick; the cursor
# caches each polled state so a chunk is polled once per state transition.
_MAX_POLLS_PER_TICK = 25


# ── Gold upsert ──────────────────────────────────────────────────────────────
# Consolidated (batch-native migration): ``write_gold`` lives in
# ``analysis.py`` as the single implementation shared with the submit
# path — the former byte-identical duplicate here is gone.


def _dead_letter_insert(
    ops: SQLiteResource, post_id: str, domain: str, error: str, attempts: int
) -> None:
    """Insert a failed item into dead_letter."""
    conn = ops.get_connection()
    try:
        conn.execute(
            """INSERT OR REPLACE INTO dead_letter
               (post_id, domain, error, attempts, failed_at)
               VALUES (?, ?, ?, ?, ?)""",
            [post_id, domain, error, attempts, _now_iso()],
        )
        conn.commit()
    finally:
        conn.close()


def _resubmit_items_preserve_attempts(
    ops: SQLiteResource, job_id: int, error: str
) -> None:
    """Return a job's processing items to pending (attempts preserved)."""
    conn = ops.get_connection()
    try:
        rows = conn.execute(
            "SELECT id FROM batch_items WHERE job_id = ? AND status = 'processing'",
            [job_id],
        ).fetchall()
    finally:
        conn.close()
    for (item_id,) in rows:
        fail_item(ops, item_id, f"job failed: {error}", backoff=0, preserve_attempts=True)


def apply_retrieved(
    ops: SQLiteResource,
    duckdb: DuckDBResource,
    results: dict[str, dict],
) -> tuple[int, int]:
    """Write retrieved responses to gold and close their batch items.

    Custom key is
    ``batch_items.id``; per-item errors route through ``fail_item`` (retry
    with backoff) and ``dead_letter`` at ``MAX_ATTEMPTS``.
    """
    ensure_gold_analyses(duckdb)
    processed = failed = 0
    for custom_key, res in results.items():
        conn = ops.get_connection()
        try:
            row = conn.execute(
                "SELECT payload FROM batch_items WHERE id = ?", [custom_key]
            ).fetchone()
        finally:
            conn.close()
        if not row:
            logger.warning("Response for unknown item %s — skipped", custom_key)
            continue
        payload = json.loads(row[0])
        post_id = payload["post_id"]
        domain = payload["domain"]
        if not res.get("ok"):
            attempts = fail_item(ops, custom_key, res.get("error") or "unknown error")
            if attempts >= MAX_ATTEMPTS:
                _dead_letter_insert(ops, post_id, domain, res.get("error") or "", attempts)
            failed += 1
            continue
        text = res["text"]
        try:
            json.loads(text)
        except json.JSONDecodeError:
            attempts = fail_item(ops, custom_key, "Gemini batch returned invalid JSON")
            if attempts >= MAX_ATTEMPTS:
                _dead_letter_insert(ops, post_id, domain, "invalid JSON", attempts)
            failed += 1
            continue
        write_gold(duckdb, post_id, domain, text)
        complete_item(ops, custom_key)
        processed += 1
    return processed, failed


# ── Harvest run core (one pass) ──────────────────────────────────────────────


def harvest_gemini_batches(
    ops: SQLiteResource,
    duckdb: DuckDBResource,
    gemini: GeminiResource,
) -> dict:
    """Poll + retrieve every submitted Gemini chunk that reached a terminal
    state, and apply results to gold.

    One pass over the submitted chunks: retrieve terminal state, apply
    results to gold (the op emits an ``AssetMaterialization`` for
    ``gold_analyses`` inside Dagster), and close the bookkeeping. Bounded
    and short; the sensor re-triggers whenever more chunks terminate.
    """
    _ensure_schema(ops)
    ensure_gold_analyses(duckdb)
    conn = ops.get_connection()
    try:
        rows = conn.execute(
            "SELECT id, gemini_batch_name, gemini_batch_status FROM batch_jobs "
            "WHERE gemini_batch_name IS NOT NULL ORDER BY id"
        ).fetchall()
    finally:
        conn.close()

    total_completed = 0
    total_failed = 0
    for job_id, names_blob, statuses_blob in rows:
        names = [n for n in names_blob.split("|") if n]
        statuses = (statuses_blob or "").split("|") if statuses_blob else []
        if len(statuses) != len(names):
            statuses = ["SUBMITTED"] * len(names)
        all_terminal = True
        job_error: str | None = None
        for i, name in enumerate(names):
            if statuses[i] in _HANDLED:
                continue
            try:
                job = gemini_batch.poll(gemini, name)
            except Exception as exc:
                logger.warning("Poll failed for %s: %s", name, exc)
                all_terminal = False
                continue
            state = gemini_batch.job_state(job)
            statuses[i] = state

            if not gemini_batch.is_terminal(state):
                all_terminal = False
                continue

            if state != "SUCCEEDED":
                error = str(getattr(job, "error", "") or "")
                logger.error("Gemini batch job %s %s: %s", name, state, error[:200])
                statuses[i] = "JOB_FAILED"
                job_error = error[:500]
                _resubmit_items_preserve_attempts(ops, job_id, error)
                continue

            # SUCCEEDED — retrieve responses for this chunk only.
            try:
                results = gemini_batch.retrieve(gemini, name)
            except Exception as exc:
                logger.warning("Retrieve failed for %s: %s", name, exc)
                all_terminal = False
                continue

            processed, failed = apply_retrieved(ops, duckdb, results)
            total_completed += processed
            total_failed += failed
            statuses[i] = "RETRIEVED"

        set_gemini_batch_status(ops, job_id, "|".join(statuses), job_error)

        if all_terminal:
            progress = batch_progress(ops, job_id)
            remaining = progress["pending"] + progress["processing"]
            if remaining == 0:
                mark_complete(ops, job_id)
    return {"completed": total_completed, "failed": total_failed}


# ── Dagster op + job ─────────────────────────────────────────────────────────


@op(tags={"adr": "0008", "seam": "enrichment-api"})
def harvest_gemini_batches_op(context, ops, duckdb, gemini) -> dict:
    """Apply terminal gemini-batch chunks to gold_analyses (short, bounded)."""
    result = harvest_gemini_batches(ops, duckdb, gemini)
    if result["completed"]:
        context.log_event(
            AssetMaterialization(
                asset_key="gold_analyses",
                metadata={
                    "completed": result["completed"],
                    "failed": result["failed"],
                    "writer": "gemini_batch_harvest",
                },
            )
        )
    context.log.info(
        "Harvest pass: %d completed, %d failed",
        result["completed"],
        result["failed"],
    )
    return result


@job(name="gemini_batch_harvest")
def gemini_batch_harvest():
    """Retrieve terminal gemini-batch chunks → gold_analyses."""
    harvest_gemini_batches_op()


# ── Harvest sensor ───────────────────────────────────────────────────────────


def _harvestable_jobs(ops: SQLiteResource) -> list[tuple[int, list[str], list[str]]]:
    """Jobs with persisted Gemini chunk names that are not yet complete."""
    _ensure_schema(ops)
    conn = ops.get_connection()
    try:
        rows = conn.execute(
            "SELECT id, gemini_batch_name, gemini_batch_status FROM batch_jobs "
            "WHERE gemini_batch_name IS NOT NULL AND status != 'complete' "
            "ORDER BY id"
        ).fetchall()
    finally:
        conn.close()
    out = []
    for job_id, names_blob, statuses_blob in rows:
        names = [n for n in names_blob.split("|") if n]
        statuses = (statuses_blob or "").split("|") if statuses_blob else []
        if len(statuses) != len(names):
            statuses = ["SUBMITTED"] * len(names)
        out.append((job_id, names, statuses))
    return out


@sensor(
    job=gemini_batch_harvest,
    default_status=DefaultSensorStatus.STOPPED,
    minimum_interval_seconds=300,
)
def gemini_batch_harvest_sensor(
    context: SensorEvaluationContext,
    ops: SQLiteResource,
    gemini: GeminiResource,
) -> SensorResult:
    """Discover terminal gemini-batch jobs and request a harvest run.

    Cursor: ``{chunk_name: last_polled_state}`` — makes each tick cheap
    (already-terminal chunks are not re-polled) and resumable across daemon
    restarts (job names themselves persist in ops.sqlite, so a restart
    re-discovers in-flight jobs even with a fresh cursor).
    """
    try:
        cursor: dict = json.loads(context.cursor) if context.cursor else {}
    except (json.JSONDecodeError, TypeError):
        cursor = {}
    new_cursor = dict(cursor)
    polls = 0
    requests: list[RunRequest] = []

    for job_id, names, statuses in _harvestable_jobs(ops):
        signature = list(statuses)
        needs_harvest = False
        for i, name in enumerate(names):
            if statuses[i] in _HANDLED:
                continue
            cached = cursor.get(name)
            if cached and gemini_batch.is_terminal(cached):
                # Known-terminal from a previous tick; request without polling.
                signature[i] = cached
                needs_harvest = True
                continue
            if polls >= _MAX_POLLS_PER_TICK:
                continue
            try:
                job = gemini_batch.poll(gemini, name)
                polls += 1
            except Exception as exc:
                logger.warning("Sensor poll failed for %s: %s", name, exc)
                continue
            state = gemini_batch.job_state(job)
            new_cursor[name] = state
            signature[i] = state
            if gemini_batch.is_terminal(state):
                needs_harvest = True
        if needs_harvest:
            # run_key changes whenever the per-chunk status signature changes;
            # Dagster dedups identical keys, so a terminal chunk requests
            # exactly one harvest run (until its status advances — e.g. the
            # run marks it RETRIEVED or a failed chunk triggers resubmission).
            sig = hashlib.md5("|".join(signature).encode()).hexdigest()[:8]
            requests.append(
                RunRequest(
                    run_key=f"harvest-{job_id}-{sig}",
                    tags={"batch_job_id": str(job_id)},
                )
            )

    context.update_cursor(json.dumps(new_cursor, sort_keys=True))
    return SensorResult(run_requests=requests)
