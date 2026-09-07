"""Dagster-native gemini-batch submit — Phase 2 of the batch-native migration.

ADR-0007/0008: consumes ONE pending, unsubmitted ``gemini-batch``-mode batch
per run (as created by ``ig_posts_gen_batches``), pre-uploads media with the
Phase-2a core (``upload_media_for_pending_batches``), builds requests with the
relocated builder (``analysis.build_requests_for_items``), submits via
``gemini_batch.submit`` and persists the returned chunk names on the queue
batch job. The existing harvest sensor picks terminal chunks up from there —
the orchestrated loop is::

    gen_batches → media_upload → submit → harvest

Semantics:
- **First-submission only**: batches that already carry a ``gemini_batch_name``
  are skipped (idempotent re-runs never re-submit in-flight chunks).
  Resubmission of failed/retrieved chunks remains the standalone worker's
  submit path on this branch (``scripts/enrichment_worker.py``).
- **Tier gate**: raises ``RuntimeError`` on tiers without BATCH API before any
  queue mutation (same contract as the worker's submit + ``gemini_batch.submit``).
- **Failure isolation**: a submission failure reschedules the claimed items
  with attempts preserved (backoff 300s) — identical to the worker's path.
- **Bounded**: one batch per run (``DEFAULT_SUBMIT_LIMIT``), media pre-warm
  bounded by ``media_upload.DEFAULT_UPLOAD_LIMIT``.
- ADR-0008 seam: every API call (File API upload, ``batches.create``) happens
  inside the seam-tagged op below or the shared seam modules it calls.
"""

from __future__ import annotations

import logging

from dagster import job, op

from datalake.defs.common.resources import DuckDBResource, GeminiResource, SQLiteResource
from datalake.defs.enrichment import gemini_batch
from datalake.defs.enrichment.analysis import build_requests_for_items
from datalake.defs.enrichment.batch import (
    _ensure_schema,
    claim_pending_items,
    fail_item,
    set_gemini_batch_name,
)
from datalake.defs.enrichment.media_upload import (
    DEFAULT_UPLOAD_LIMIT,
    SEAM_TAGS,
    upload_media_for_pending_batches,
)
from datalake.defs.enrichment.prompts import _DEFAULT_GEMINI_MODEL
from datalake.defs.instagram.config import GeminiTierConfig

logger = logging.getLogger("enrichment.submit")

# Bounded runs: at most this many batches submit per run (the worker also does
# one submit per cycle — keeps chunk bookkeeping simple on both writers).
DEFAULT_SUBMIT_LIMIT = 1

# Backoff applied to claimed items when the submission itself fails (mirrors
# the worker's submit_gemini_batches).
_SUBMIT_FAILURE_BACKOFF_SECONDS = 300


# ── Discovery ────────────────────────────────────────────────────────────────


def unsubmitted_batch_ids(ops: SQLiteResource, limit: int) -> list[int]:
    """Pending, unsubmitted gemini-batch jobs with claimable items.

    Oldest first. Excludes already-submitted jobs (``gemini_batch_name IS
    NULL``), completed jobs, and jobs whose items are all claimed/terminal —
    so a re-run after a crashed submit picks the batch back up, and a re-run
    after a successful submit finds nothing.
    """
    _ensure_schema(ops)
    conn = ops.get_connection()
    try:
        rows = conn.execute(
            "SELECT id FROM batch_jobs "
            "WHERE mode = 'gemini-batch' "
            "AND status IN ('pending', 'processing') "
            "AND gemini_batch_name IS NULL "
            "AND EXISTS (SELECT 1 FROM batch_items i "
            "            WHERE i.job_id = batch_jobs.id AND i.status = 'pending') "
            "ORDER BY created_at ASC LIMIT ?",
            [limit],
        ).fetchall()
    finally:
        conn.close()
    return [r[0] for r in rows]


# ── Core (mock-testable) ─────────────────────────────────────────────────────


def submit_batch(
    ops: SQLiteResource,
    duckdb: DuckDBResource,
    gemini: GeminiResource,
    job_id: int,
) -> dict:
    """Claim + build + submit + persist for ONE batch job.

    Returns ``{"submitted": <chunk count>}`` (plus ``"skipped": True`` when
    the batch already carried a name). Assumes a batch-capable tier — the
    tier gate lives in :func:`submit_pending_gemini_batches` and inside
    ``gemini_batch.submit``.
    """
    _ensure_schema(ops)

    # Defensive idempotency re-read: the batch may have been submitted by the
    # worker (dual-writer migration window) after discovery picked it.
    conn = ops.get_connection()
    try:
        row = conn.execute(
            "SELECT gemini_batch_name FROM batch_jobs WHERE id = ?", [job_id]
        ).fetchone()
    finally:
        conn.close()
    if row and row[0]:
        logger.info("Batch %d already submitted (%s) — skipping", job_id, row[0])
        return {"submitted": 0, "skipped": True}

    # Claim every claimable pending item — they are in-flight from now on.
    items: list[dict] = []
    while True:
        chunk = claim_pending_items(ops, job_id, limit=1000)
        if not chunk:
            break
        items.extend(chunk)
    if not items:
        logger.info("Batch %d: no claimable pending items — skipping", job_id)
        return {"submitted": 0}

    # Claimed transition (mirrors claim_batch's status update).
    conn = ops.get_connection()
    try:
        conn.execute(
            "UPDATE batch_jobs SET status = 'processing' WHERE id = ?", [job_id]
        )
        conn.commit()
    finally:
        conn.close()

    requests = build_requests_for_items(ops, duckdb, gemini, items)
    if not requests:
        # Everything was completed (empty captions / unknown domains) or
        # per-item failed with backoff by the builder — nothing to submit
        # this cycle; backoff items rejoin discovery once scheduled_for passes.
        logger.info("Batch %d: nothing submittable this cycle", job_id)
        return {"submitted": 0}

    try:
        names = gemini_batch.submit(
            gemini,
            _DEFAULT_GEMINI_MODEL,
            requests,
            display_name=f"enrich-job{job_id}",
        )
    except Exception as exc:
        # Submission failed (API error, quota, precondition): reschedule the
        # claimed items so they are retried on a later cycle instead of
        # stranding in 'processing'. Attempts preserved — not their fault.
        logger.error("Batch %d: submit failed — rescheduling items: %s", job_id, exc)
        for item in items:
            fail_item(
                ops, item["id"], f"submit failed: {exc}",
                backoff=_SUBMIT_FAILURE_BACKOFF_SECONDS, preserve_attempts=True,
            )
        return {"submitted": 0}

    set_gemini_batch_name(ops, job_id, "|".join(names))
    logger.info(
        "Batch %d: submitted %d request(s) in %d Gemini batch job(s)",
        job_id, len(requests), len(names),
    )
    return {"submitted": len(names), "requests": len(requests)}


def submit_pending_gemini_batches(
    ops: SQLiteResource,
    duckdb: DuckDBResource,
    gemini: GeminiResource,
    limit: int = DEFAULT_SUBMIT_LIMIT,
) -> dict:
    """One submit pass: media pre-warm, then submit up to ``limit`` batches.

    Idempotent: unsubmitted-batch discovery + the per-batch name re-read make
    re-runs no-ops. Raises ``RuntimeError`` on tiers without BATCH API —
    before any queue mutation or API call.
    """
    _ensure_schema(ops)
    tier_cfg = GeminiTierConfig.detect()
    if not tier_cfg.supports_batch:
        raise RuntimeError(
            f"Gemini batch API requires Tier 1+ (active tier: {tier_cfg.tier.value}). "
            "Set GEMINI_TIER=tier1 with a paid key."
        )

    # Best-effort media pre-warm (cache-first, bounded): keeps build time low
    # and makes the submit job self-sufficient when the media_upload op has
    # not run. Per-post failures are tolerated — the builder re-resolves and
    # routes failures per item.
    upload_media_for_pending_batches(ops, duckdb, gemini, limit=DEFAULT_UPLOAD_LIMIT)

    submitted = 0
    batch_ids = unsubmitted_batch_ids(ops, limit)
    for job_id in batch_ids:
        result = submit_batch(ops, duckdb, gemini, job_id)
        submitted += result["submitted"]
    return {"submitted": submitted, "batches": batch_ids}


# ── Dagster op + job ─────────────────────────────────────────────────────────


@op(tags=SEAM_TAGS)
def submit_gemini_batches_op(context, ops, duckdb, gemini) -> dict:
    """Submit one pending unsubmitted gemini-batch batch (short, bounded)."""
    result = submit_pending_gemini_batches(ops, duckdb, gemini)
    context.log.info(
        "Submit pass: %d chunk(s) submitted across %d batch(es)",
        result["submitted"],
        len(result["batches"]),
    )
    return result


@job(name="submit_gemini_batches_job")
def submit_gemini_batches_job():
    """Submit pending unsubmitted gemini-batch batches to the Gemini BATCH API."""
    submit_gemini_batches_op()
