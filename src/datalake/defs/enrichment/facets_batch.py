"""Batch-native growth-facets enrichment — facets → ``gold_growth_facets``.

Runs on the standalone **qwen-batch service** (``qwen_client.py``), NOT the
Gemini BATCH API. The IG-gold enrichment flow (``analysis.py`` /
``gemini_batch.py``) is a separate target and still uses Gemini.

Flow (one-shot CLI driver, ``scripts/enrich_facets_batch.py``):

    enumerate_targets        (posts lacking a current gold row under the
                              qwen-scoped CURRENT_*_PROMPT_HASH)
    → build_facets_batch_requests
        visual: media URLs resolved to scrape-time cached local paths
          (media_paths.media_urls_to_local_paths — the SERVICE frame-samples
          video files with ffmpeg on its own host; the client only hands it
          absolute paths). Text mode sends images=[] (caption only).
    → submit_facets_batch    (check_health LOUDLY first — US-EENG-2 — then
          ONE qwen service job via qwen_client.submit_job; job_id persisted
          in the ops.sqlite ``facets_batch_jobs`` resume ledger)
    → wait_for_facets_batches (poll qwen_client.get_job to terminal, bounded)
    → harvest_facets_batches (qwen_client.get_results → parse → validate →
          facets.write_gold_facets_pass_conn MERGE upsert)

Two passes, partial-storage contract (US-EFAC-3/4):

- **visual** — ``build_growth_facets_prompt(caption, n_media)`` + image
  paths; parse with ``parse_universal_response``; writes visual sub-fields +
  content_summary / image_summaries.
- **text** — ``build_text_facets_prompt`` + NO media; parse with
  ``parse_text_response``; merges text sub-fields.

Each write merges into the row's existing ``growth_facets_json`` so the
stored row is the union of both passes and satisfies the full V3 schema.

Deterministic zero-media decision (documented, never a silent hole): a
VISUAL target whose media URLs resolve to NO cached local path is SKIPPED
with a logged reason (``logger.warning``); the driver re-discovers it on the
next run once the media cache is filled. A caption-only visual call cannot
satisfy the visual contract, so submitting it would only produce invalid
rows. TEXT targets never involve media and always submit.
"""

from __future__ import annotations

import json
import logging
import os
import time
import uuid

from datalake.defs.common.resources import SQLiteResource
from datalake.defs.enrichment import analysis as ew
from datalake.defs.enrichment import facets
from datalake.defs.enrichment.growth_facets_schema import (
    GROWTH_FACETS_SCHEMA_VERSION,
    VISUAL_FACET_FIELDS,
)
from datalake.defs.enrichment.media_paths import (
    is_video_path,
    media_urls_to_local_paths,
)
from datalake.defs.enrichment.prompts import (
    CURRENT_FACETS_PROMPT_HASH,
    CURRENT_TEXT_FACETS_PROMPT_HASH,
    _DEFAULT_QWEN_MODEL,
    build_growth_facets_prompt,
    build_text_facets_prompt,
)
from datalake.defs.enrichment import qwen_client

logger = logging.getLogger("enrichment.facets_batch")

# Qwen list prices (USD per 1M tokens) — advisory cost projection only,
# not billing. Env-overridable for price drift.
QWEN_INPUT_PRICE_PER_M = float(os.environ.get("QWEN_INPUT_PRICE_PER_M", "0.03"))
QWEN_OUTPUT_PRICE_PER_M = float(os.environ.get("QWEN_OUTPUT_PRICE_PER_M", "0.13"))

# Estimated output tokens per response — cost projection only (not billing).
_EST_OUTPUT_TOKENS_VISUAL = facets.UNIVERSAL_MAX_OUTPUT_TOKENS
_EST_OUTPUT_TOKENS_TEXT = 1024

# Advisory per-media input-token estimates for the qwen service: images are
# vision tokens; videos are frame-sampled server-side into 8 frames.
_TOKENS_PER_IMAGE = 258
_VIDEO_FRAMES = 8

_TEXT_REQUIRED = (
    "hook_content", "hook_type", "is_sponsored", "sponsorship_signal",
    "claimed_results", "cta_type", "audience_named", "value_depth",
    "replicable_tactic", "evidence", "brand_safety",
)

# ── Resume ledger (ops.sqlite) ───────────────────────────────────────────────

_LEDGER_DDL = """
CREATE TABLE IF NOT EXISTS facets_batch_jobs (
    id INTEGER PRIMARY KEY,
    mode VARCHAR NOT NULL,
    model VARCHAR NOT NULL,
    job_id VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    n_requests INTEGER NOT NULL,
    est_tokens INTEGER NOT NULL,
    meta_json VARCHAR NOT NULL DEFAULT '{}',
    created_at VARCHAR NOT NULL,
    updated_at VARCHAR NOT NULL
)
"""


def _ensure_ledger(ops: SQLiteResource) -> None:
    conn = ops.get_connection()
    try:
        conn.execute(_LEDGER_DDL)
        # Backward-compatible migration from the Gemini-era schema where the
        # job column was named ``gemini_batch_name``.
        cols = [
            r[1] for r in conn.execute(
                "PRAGMA table_info(facets_batch_jobs)"
            ).fetchall()
        ]
        if "job_id" not in cols and "gemini_batch_name" in cols:
            conn.execute(
                "ALTER TABLE facets_batch_jobs RENAME COLUMN "
                "gemini_batch_name TO job_id"
            )
            conn.commit()
    finally:
        conn.close()

def _record_jobs(
    ops: SQLiteResource,
    mode: str,
    model: str,
    job_ids: list[str],
    n_requests: int,
    est_tokens: int,
    meta: dict,
    status: str = "SUBMITTED",
) -> None:
    _ensure_ledger(ops)
    now = ew._now_iso()
    conn = ops.get_connection()
    try:
        for job_id in job_ids:
            conn.execute(
                "INSERT INTO facets_batch_jobs "
                "(mode, model, job_id, status, n_requests, "
                " est_tokens, meta_json, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [mode, model, job_id, status, n_requests, est_tokens,
                 json.dumps(meta), now, now],
            )
        conn.commit()
    finally:
        conn.close()


def _mark_submitted(ops: SQLiteResource, placeholder: str, job_id: str) -> None:
    """Flip a SUBMITTING placeholder row to SUBMITTED with the real job id."""
    conn = ops.get_connection()
    try:
        conn.execute(
            "UPDATE facets_batch_jobs SET job_id = ?, "
            "status = 'SUBMITTED', updated_at = ? WHERE job_id = ?",
            [job_id, ew._now_iso(), placeholder],
        )
        conn.commit()
    finally:
        conn.close()


def _set_ledger_status(ops: SQLiteResource, job_id: str, status: str) -> None:
    conn = ops.get_connection()
    try:
        conn.execute(
            "UPDATE facets_batch_jobs SET status = ?, updated_at = ? "
            "WHERE job_id = ?",
            [status, ew._now_iso(), job_id],
        )
        conn.commit()
    finally:
        conn.close()


def pending_ledger_jobs(ops: SQLiteResource) -> list[dict]:
    """Submitted-but-not-yet-harvested facet batch jobs (resume-safe)."""
    _ensure_ledger(ops)
    conn = ops.get_connection()
    try:
        rows = conn.execute(
            "SELECT job_id, mode, model, status, meta_json FROM "
            "facets_batch_jobs WHERE status = 'SUBMITTED' ORDER BY id"
        ).fetchall()
    finally:
        conn.close()
    return [
        {"job_id": r[0], "mode": r[1], "model": r[2], "status": r[3],
         "meta": json.loads(r[4]) if r[4] else {}}
        for r in rows
    ]


# ── Target enumeration ───────────────────────────────────────────────────────


def enumerate_targets(
    conn,
    mode: str,
    limit: int | None = None,
    post_ids: list[str] | None = None,
) -> list[dict]:
    """Posts eligible for the given pass, not yet done under its prompt hash.

    visual: media-bearing, non-empty caption, no gold row under
    ``CURRENT_FACETS_PROMPT_HASH``.
    text: non-empty caption, no stored row carrying the full text-layer
    sub-schema (row-union detection — robust across the visual pass's
    prompt_hash column ownership).
    """
    if mode not in ("visual", "text"):
        raise ValueError(f"unknown mode: {mode}")
    where = "TRIM(caption) <> ''"
    params: list = []
    if mode == "visual":
        where += " AND media_files IS NOT NULL AND media_files <> '[]'"
    if post_ids:
        where += " AND post_id IN (" + ",".join("?" * len(post_ids)) + ")"
        params.extend(post_ids)
    # Idempotent ensure: plan mode may run against a state db that has never
    # materialized gold_growth_facets.
    conn.execute(facets._GOLD_FACETS_DDL)
    rows = conn.execute(
        f"SELECT post_id, caption, media_files FROM silver_ig_posts "
        f"WHERE {where} ORDER BY post_id",
        params,
    ).fetchall()
    done = _done_post_ids(conn, mode)
    targets = [
        {"post_id": r[0], "caption": r[1] or "", "media_files": r[2]}
        for r in rows
        if r[0] not in done
    ]
    return targets[:limit] if limit else targets


def _done_post_ids(conn, mode: str) -> set[str]:
    """Post_ids already fully enriched for ``mode`` under the current engine
    (model) + schema — content-based, so a pass-owned prompt_hash overwrite
    cannot double-spend, while rows from a superseded engine (e.g. gemini-era)
    are re-enqueued for the current (qwen) engine."""
    if mode == "visual":
        # Done iff schema_version is current, the stored facet JSON carries
        # every required visual field, AND the row was produced by the current
        # engine (model). Must NOT gate on prompt_hash: the text pass overwrites
        # prompt_hash on the same row, so a hash-only check would re-enqueue
        # (and re-pay for) text-done posts every run. model is stable across
        # both passes (each stamps the current qwen model), so it stays a valid
        # discriminator that also re-enqueues gemini-era rows under qwen.
        rows = conn.execute(
            "SELECT post_id, growth_facets_json, schema_version, model "
            "FROM gold_growth_facets"
        ).fetchall()
        done = set()
        for post_id, blob, schema_version, model in rows:
            if model != _DEFAULT_QWEN_MODEL:
                continue
            if schema_version != GROWTH_FACETS_SCHEMA_VERSION:
                continue
            try:
                payload = json.loads(blob) if blob else None
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict) and all(
                f in payload for f in VISUAL_FACET_FIELDS
            ):
                done.add(post_id)
        return done
    # text: done iff the stored JSON carries every required text field AND the
    # row was produced by the current engine (model) — mirroring visual.
    rows = conn.execute(
        "SELECT post_id, growth_facets_json, model FROM gold_growth_facets "
        "WHERE model = ?",
        [_DEFAULT_QWEN_MODEL],
    ).fetchall()
    done = set()
    for post_id, blob, model in rows:
        try:
            payload = json.loads(blob) if blob else None
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict) and all(f in payload for f in _TEXT_REQUIRED):
            done.add(post_id)
    return done

# ── Request building ─────────────────────────────────────────────────────────


def build_facets_batch_requests(
    ops: SQLiteResource,
    conn,
    targets: list[dict],
    mode: str,
) -> list[dict]:
    """Build qwen service job items for facet targets.

    Visual targets resolve media URLs to scrape-time cached LOCAL file paths
    (``media_urls_to_local_paths``) at submit time — the service reads files
    from its own host disk and frame-samples videos with ffmpeg. Text targets
    send ``images=[]`` (caption only). ``custom_key`` is the post_id so
    harvest maps responses directly.

    A visual target whose media resolves to NO cached path is skipped with a
    logged reason (deterministic, re-discovered on the next run) — see the
    module docstring.
    """
    items: list[dict] = []
    for t in targets:
        post_id = t["post_id"]
        caption = t["caption"]
        if mode == "visual":
            images = media_urls_to_local_paths(
                ops, t["media_files"], include_video=True
            )
            if not images:
                logger.warning(
                    "Post %s: no cached media paths resolvable — skipped "
                    "(re-discovered on the next run after media cache fill)",
                    post_id,
                )
                continue
            req = {
                "custom_key": post_id,
                "prompt": build_growth_facets_prompt(caption, len(images)),
                "images": images,
            }
        else:
            req = {
                "custom_key": post_id,
                "prompt": build_text_facets_prompt(caption),
                "images": [],
            }
        items.append(req)
    return items


# ── Cost projection ──────────────────────────────────────────────────────────


def estimate_facets_cost(items: list[dict]) -> tuple[int, float]:
    """(estimated input tokens, projected USD) for a qwen item list.

    Advisory only (not billing): prompt chars/4 + vision tokens per image
    (videos counted as their server-side frame sample), priced at the qwen
    list rates.
    """
    input_tokens = 0
    out_tokens = 0
    for r in items:
        n_video = sum(1 for p in r.get("images") or [] if is_video_path(p))
        n_img = len(r.get("images") or []) - n_video
        input_tokens += (
            len(r.get("prompt") or "") // 4
            + n_img * _TOKENS_PER_IMAGE
            + n_video * _VIDEO_FRAMES * _TOKENS_PER_IMAGE
        )
        out_tokens += (
            _EST_OUTPUT_TOKENS_VISUAL if r.get("images") else _EST_OUTPUT_TOKENS_TEXT
        )
    cost = (
        input_tokens / 1_000_000 * QWEN_INPUT_PRICE_PER_M
        + out_tokens / 1_000_000 * QWEN_OUTPUT_PRICE_PER_M
    )
    return input_tokens, cost


# ── Submit / wait / harvest ──────────────────────────────────────────────────


def submit_facets_batch(
    ops: SQLiteResource,
    items: list[dict],
    mode: str,
    model: str = _DEFAULT_QWEN_MODEL,
    base_url: str | None = None,
) -> str:
    if not items:
        raise ValueError("items must not be empty")
    qwen_client.check_health(base_url)
    _ensure_ledger(ops)
    input_tokens, _ = estimate_facets_cost(items)
    meta = {
        "n_media": {
            r["custom_key"]: len(r.get("images") or []) for r in items
        }
    }
    max_tokens = (
        facets.UNIVERSAL_MAX_OUTPUT_TOKENS if mode == "visual" else 1024
    )
    # Ledger row BEFORE the POST: a SUBMITTING placeholder means a crash in
    # the submit window leaves an inspectable row instead of an orphaned
    # submitted (and billed) service job.
    placeholder = f"submitting-{uuid.uuid4().hex}"
    _record_jobs(
        ops, mode, model, [placeholder], len(items), input_tokens, meta,
        status="SUBMITTING",
    )
    job_id = qwen_client.submit_job(
        base_url,
        [
            {
                "custom_key": r["custom_key"],
                "prompt": r["prompt"],
                "images": r.get("images") or [],
            }
            for r in items
        ],
        model=model,
        max_tokens=max_tokens,
    )
    _mark_submitted(ops, placeholder, job_id)
    logger.info(
        "Facets batch (%s): %d items → qwen job %s", mode, len(items), job_id
    )
    return job_id


def wait_for_facets_batches(
    job_ids: list[str],
    poll_seconds: int = 60,
    timeout_seconds: int = 24 * 3600,
    base_url: str | None = None,
) -> dict[str, str]:
    """Poll service job ids until each reaches a terminal state (bounded).

    Returns ``{job_id: terminal_state}``. Raises ``RuntimeError`` on timeout.
    """
    results: dict[str, str] = {}
    remaining = set(job_ids)
    deadline = time.monotonic() + timeout_seconds
    while remaining:
        for job_id in list(remaining):
            doc = qwen_client.get_job(base_url, job_id=job_id)
            state = str(doc.get("state", ""))
            if qwen_client.job_is_terminal(state):
                results[job_id] = state
                remaining.discard(job_id)
        if remaining and time.monotonic() > deadline:
            raise RuntimeError(
                f"Timed out waiting for facet jobs: {sorted(remaining)}"
            )
        if remaining:
            time.sleep(poll_seconds)
    return results


def harvest_facets_batches(
    ops: SQLiteResource,
    conn,
    job_ids: list[str] | None = None,
    model: str = _DEFAULT_QWEN_MODEL,
    base_url: str | None = None,
) -> dict:
    """Retrieve terminal facet jobs, parse, validate, write gold rows.

    For each SUBMITTED ledger job (or the explicit ``job_ids``): poll the
    service; on ``completed``, ``get_results`` returns one item per
    ``custom_key`` (the post_id); visual responses go through
    ``parse_universal_response`` (n_media from ledger ``meta_json['n_media']``
    else re-derived from silver) → validated visual sub-fields + summaries;
    text responses through ``parse_text_response``. Validated facets merge
    into ``gold_growth_facets`` via ``write_gold_facets_pass_conn``.
    Job-level failure flips the ledger row to JOB_FAILED; per-item failures
    are surfaced in the returned dict and never silently dropped (their
    targets are NOT recorded as done — the driver re-discovers them).
    """
    _ensure_ledger(ops)
    pending = pending_ledger_jobs(ops)
    if job_ids:
        wanted = set(job_ids)
        known = {j["job_id"] for j in pending}
        missing = sorted(wanted - known)
        if missing:
            raise ValueError(
                "Job ids not in the facets ledger (submit via "
                "submit_facets_batch first): " + ", ".join(missing)
            )
        pending = [j for j in pending if j["job_id"] in wanted]
    written = invalid = failed_items = 0
    for job in pending:
        job_id = job["job_id"]
        mode = job.get("mode")
        # Gold rows are stamped with the model the job was SUBMITTED with,
        # not the CLI default the harvest call may carry.
        model = job.get("model") or model
        meta = job.get("meta") or {}
        doc = qwen_client.get_job(base_url, job_id=job_id)
        state = str(doc.get("state", ""))
        if not qwen_client.job_is_terminal(state):
            continue
        if state != "completed":
            logger.error("Facets job %s terminal state %s", job_id, state)
            _set_ledger_status(ops, job_id, "JOB_FAILED")
            continue
        for res in qwen_client.get_results(base_url, job_id=job_id):
            post_id = res.get("custom_key")
            if not res.get("ok"):
                logger.warning(
                    "Post %s service item error: %s",
                    post_id, res.get("error"),
                )
                failed_items += 1
                continue
            text = res.get("output")
            if mode == "text":
                parsed = facets.parse_text_response(text)
                if parsed["errors"] or parsed["text_facets"] is None:
                    logger.warning(
                        "Post %s text facets invalid: %s",
                        post_id, parsed["errors"],
                    )
                    invalid += 1
                    continue
                facets.write_gold_facets_pass_conn(
                    conn, post_id, "instagram", parsed["text_facets"],
                    model=model, prompt_hash=CURRENT_TEXT_FACETS_PROMPT_HASH,
                )
            else:
                sent = (meta.get("n_media") or {}).get(post_id)
                n_media = int(sent) if sent is not None else _n_media_for(
                    conn, post_id
                )
                parsed = facets.parse_universal_response(text, n_media)
                if parsed["errors"] or parsed["visual_facets"] is None:
                    logger.warning(
                        "Post %s visual facets invalid: %s",
                        post_id, parsed["errors"],
                    )
                    invalid += 1
                    continue
                facets.write_gold_facets_pass_conn(
                    conn, post_id, "instagram", parsed["visual_facets"],
                    model=model,
                    content_summary=parsed["content_summary"],
                    image_summaries=parsed["image_summaries"],
                )
            written += 1
        _set_ledger_status(ops, job_id, "RETRIEVED")
    return {
        "written": written,
        "invalid": invalid,
        "failed_items": failed_items,
        "jobs": len(pending),
    }


def _n_media_for(conn, post_id: str) -> int:
    row = conn.execute(
        "SELECT media_files FROM silver_ig_posts WHERE post_id = ?", [post_id]
    ).fetchone()
    if not row or not row[0]:
        return 0
    try:
        urls = json.loads(row[0])
    except json.JSONDecodeError:
        return 0
    return len(urls) if isinstance(urls, list) else 0
